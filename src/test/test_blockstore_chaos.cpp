// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// Seed-driven crash chaos test for the real blockstore running on simulated disks.
//
// Everything below the blockstore is simulated: io_uring completions arrive out of
// submission order after a random delay, the volatile write cache loses a random
// subset of itself on a power outage, and writes that were in flight at that moment
// land fully, partially (torn at a sector boundary) or not at all.
//
// The blockstore itself is the real thing - journal, metadata, flusher, compaction,
// checksums. A model of what was written is kept alongside, and after every simulated
// power outage the recovered state is checked against it.
//
// The workload is a single loop of independent steps: every step either submits one more
// operation, lets the simulation run for one event, restarts the store or pulls the plug.
// Nothing waits for anything, so an outage lands wherever it lands - in the middle of a
// write, of a sync, of a stabilize or of a rollback alike. What has to follow an operation
// follows it asynchronously, from its completion: with capacitors a two-phase write becomes
// stabilizable as soon as it completes, without them a sync makes everything it covered
// stabilizable when the sync itself completes.
//
// The seed picks the configuration too, so a failing run is replayed exactly by passing
// the seed it printed. Run with -h for the options.

#include <malloc.h>
#include <sys/wait.h>
#include <unistd.h>
#include <errno.h>
#include <map>
#include <random>
#include <set>
#include <time.h>
#include <vector>

#include "str_util.h"
#include "ringloop_mock.h"
#include "blockstore_impl.h"
#include "v1/impl.h"

#define OBJ_COUNT 4
#define OBJ_SIZE (128*1024)
#define GRAN 4096
#define MAX_STEPS 1000000
// Size of the external bitmap the store keeps per object version: one bit per granule
#define BMP_SIZE (OBJ_SIZE/GRAN/8)

// What one step of the test does. The first group is the relative weight of each possible
// action - they're weights, not percentages, and only their ratio matters. Setting one to
// zero turns the corresponding action off entirely, which is how a workload without
// deletions or without rollbacks is tested
struct chaos_probs_t
{
    // Submit nothing, just let the simulation run for one event
    uint32_t wait = 500;
    uint32_t write = 60, del = 6, read = 40, sync = 30, stabilize = 50, rollback = 6;
    // Restart the store cleanly - everything in flight is allowed to finish first
    uint32_t restart = 1;
    // Pull the plug right now, whatever happens to be in flight
    uint32_t crash = 2;
    // Read the whole object instead of a random range, in percent
    uint32_t full_read = 25;
    // How many operations may be in flight at once. When the limit is reached, a step
    // waits for one of them to complete instead of submitting anything
    uint32_t max_inflight = 8;
};

static chaos_probs_t probs;

// What a single step decided to do
enum chaos_action_t
{
    ACT_CRASH = 0,
    ACT_RESTART,
    ACT_WRITE,
    ACT_DELETE,
    ACT_READ,
    ACT_SYNC,
    ACT_STABILIZE,
    ACT_ROLLBACK,
    ACT_WAIT,
    ACT_COUNT,
};

// One point of the configuration matrix. The two implementations are supposed to have
// identical semantics, so the very same workload and the very same checks run against both.
struct chaos_cfg_t
{
    const char *impl;
    // false: immediate_commit=all with all fsyncs disabled
    // true:  immediate_commit=none, data fsync enabled, so the disk has a volatile cache
    bool use_fsync;
    // Checksum block size, or 0 for no checksums at all - a rather different code path.
    // Deliberately includes sizes larger than the write granularity, where a write only
    // covers part of a checksummed block
    uint32_t csum_block;
    // false: BS_OP_WRITE followed by BS_OP_STABLE, the way an EC pool writes
    // true:  BS_OP_WRITE_STABLE, the way a replicated pool writes
    //
    // This can't be mixed within one object: writing a stable version on top of an
    // unstable one is rejected with EINVAL, and a pool is always one or the other
    bool instant_writes;
};

// Extra diagnostics for a deadlock, only available in the new implementation
static void dump_extra(blockstore_impl_t *bs)
{
    printf("completed_lsn=%ju fsynced_lsn=%ju compacted_count=%ju compact_queue=%u to_compact=%u\n",
        bs->heap->get_completed_lsn(), bs->heap->get_fsynced_lsn(), bs->heap->get_compacted_count(),
        bs->heap->get_compact_queue_size(), bs->heap->get_to_compact_count());
}

static void dump_extra(v1::blockstore_impl_t *bs)
{
}

// What we believe about one object.
//
// `submitted` is the highest version we ever enqueued a write for - not necessarily
// acknowledged, because a write may have been in flight when the power went away.
// `durable` is the highest version we know must survive a crash: its write, a sync
// after it and its stabilization all completed successfully.
// Versions restart from 1 after a deletion - the store explicitly allows writing low
// version numbers over a delete, because the object is gone. So the model can't use the
// version as its own key: `submitted`/`acked`/`synced`/`durable` below are a sequence
// number that only ever grows, and `ver` maps it to the version the store actually sees.
struct obj_model_t
{
    uint64_t submitted = 0;
    uint64_t acked = 0;
    uint64_t synced = 0;
    uint64_t durable = 0;
    // Next version number to hand to the store
    uint64_t next_version = 1;
    // Sequence number of the last acknowledged deletion. Everything below it is gone from
    // the store, so a sync completing with an older snapshot doesn't make it stabilizable
    uint64_t deleted_upto = 0;
    // Whether `durable` names a version that this store instance actually stabilized, and
    // therefore still holds as a distinct version we can roll back to. Recovery resets it:
    // the version we called durable may have been flushed into a single clean entry by then.
    bool can_rollback = false;
    // Full object content as of every sequence number we still care about.
    // An empty vector means the object was deleted at that point.
    std::map<uint64_t, std::vector<uint8_t>> content;
    // Sequence number -> version number as seen by the store
    std::map<uint64_t, uint64_t> ver;
    // Sequence number -> the byte the external bitmap of that write was filled with.
    // The bitmap is opaque to the blockstore but it travels with the entry through
    // compaction, recovery and rollbacks just like the data does
    std::map<uint64_t, uint8_t> bmp;
};

struct pending_t
{
    uint64_t seq_id = 0;
    blockstore_op_t op = {};
    bool done = false;
    int obj = -1;
    // Objects this operation refers to. While it's in flight no other operation may touch
    // them, which is what keeps the model unambiguous: the base content of a write and the
    // expected result of a read can't change under it
    std::vector<int> touched;
    // Sequence number in the model, and the version number handed to the store
    uint64_t version = 0;
    uint64_t bs_version = 0;
    std::vector<uint8_t> content;
    // External bitmap handed to the store with a write, or filled in by a read
    std::vector<uint8_t> bitmap;
    // For a stabilize op: every (object, version) pair it carries
    std::vector<std::pair<int, uint64_t>> stabilized;
    // Model update to run if - and only if - the operation completed successfully
    std::function<void()> on_success;
};

template<class BS>
struct chaos_t
{
    blockstore_config_t config;
    ring_loop_mock_t *ringloop = NULL;
    timerfd_manager_t *tfd = NULL;
    io_sim_t *sim = NULL;
    disk_mock_t *data_disk = NULL, *meta_disk = NULL;
    BS *bs = NULL;
    obj_model_t objs[OBJ_COUNT];
    uint64_t crashes = 0, restarts = 0, writes = 0, deletes = 0, syncs = 0, stabilizes = 0;
    uint64_t rollbacks = 0, refused_writes = 0, checked_reads = 0, step_no = 0;
    bool use_fsync = false, instant_writes = false;
    uint64_t seq_id = 0;
    bool trace = false;

    // Operations submitted but not yet accounted for
    std::vector<pending_t*> inflight;
    // Number of in-flight operations referring to each object
    int busy[OBJ_COUNT] = {};
    int sync_inflight = 0, rollback_inflight = 0;
    // Object -> the highest sequence number which is written and synced, but not stable yet.
    // Filled in from operation completions, drained by ACT_STABILIZE
    std::map<int, uint64_t> to_stabilize;

    void configure(const chaos_cfg_t & cfg, int meta_format)
    {
        use_fsync = cfg.use_fsync;
        instant_writes = cfg.instant_writes;
        config["data_device"] = "./test_data.bin";
        config["data_device_size"] = "268435456";
        config["data_device_sect"] = "4096";
        config["meta_offset"] = "0";
        config["journal_offset"] = "16777216";
        config["data_offset"] = "33554432";
        config["disable_data_fsync"] = cfg.use_fsync ? "0" : "1";
        config["immediate_commit"] = cfg.use_fsync ? "none" : "all";
        config["log_level"] = "0";
        if (cfg.csum_block)
        {
            config["data_csum_type"] = "crc32c";
            config["csum_block_size"] = std::to_string(cfg.csum_block);
        }
        config["meta_format"] = std::to_string(meta_format);
    }

    void create(uint32_t seed)
    {
        ringloop = new ring_loop_mock_t(RINGLOOP_DEFAULT_SIZE, [this](io_uring_sqe *sqe)
        {
            if (sqe->fd == MOCK_DATA_FD)
            {
                bool ok = data_disk->submit(sqe);
                assert(ok);
            }
            else if (sqe->fd == MOCK_META_FD)
            {
                assert(meta_disk);
                bool ok = meta_disk->submit(sqe);
                assert(ok);
            }
            else
                assert(0);
        });
        sim = new io_sim_t(ringloop, seed);
        data_disk = new disk_mock_t("data disk", parse_size(config["data_device_size"]), config["disable_data_fsync"] != "1");
        data_disk->clear(0, parse_size(config["data_offset"]));
        config["log_level"] = trace ? "11" : "0";
        data_disk->faults.forbid_overlapping_writes = true;
        data_disk->trace = trace;
        data_disk->atomic_write_size = config.find("atomic_write_size") != config.end()
            ? parse_size(config["atomic_write_size"]) : 4096;
        sim->add_disk(data_disk);
        start_bs();
    }

    // (Re)create the blockstore on top of the disks, as if the process had been restarted
    void start_bs()
    {
        assert(!bs);
        // The blockstore sets one-shot timers capturing `this`, so the timer manager
        // can't outlive it across a simulated crash
        tfd = new timerfd_manager_t(NULL);
        sim->set_tfd(tfd);
        bs = new BS(config, ringloop, tfd, true);
        if (!sim->run_until([this]() { return bs->is_started(); }, MAX_STEPS))
        {
            fprintf(stderr, "blockstore failed to start\n");
            abort();
        }
    }

    void destroy()
    {
        if (bs)
        {
            delete bs;
            bs = NULL;
        }
        ringloop->reset();
        if (tfd)
        {
            delete tfd;
            tfd = NULL;
        }
    }

    ~chaos_t()
    {
        destroy();
        for (auto *p: inflight)
            free_op(p);
        inflight.clear();
        if (sim)
        {
            delete sim;
            sim = NULL;
        }
        if (data_disk)
        {
            delete data_disk;
            data_disk = NULL;
        }
        if (ringloop)
        {
            delete ringloop;
            ringloop = NULL;
        }
    }

    // Every write gets its own recognisable external bitmap
    uint8_t bitmap_byte(int obj, uint64_t bs_version)
    {
        return (uint8_t)(0x40 + obj*29 + bs_version*7);
    }

    uint64_t rnd(uint64_t min, uint64_t max)
    {
        return sim->random(min, max);
    }

    // Draw unconditionally so that the sequence doesn't depend on the probability itself
    bool chance(uint32_t percent)
    {
        return rnd(0, 99) < percent;
    }

    object_id oid(int i)
    {
        return (object_id){ .inode = 1, .stripe = (uint64_t)i * OBJ_SIZE };
    }

    bool exists(int obj)
    {
        auto & m = objs[obj];
        return m.acked && !m.content.at(m.acked).empty();
    }

    // Self-describing payload: every record says which object, which version and which
    // offset wrote it, so a mismatch tells exactly where the stale data came from. Its
    // size divides the write granularity, so records never straddle a write or a read.
    struct payload_t
    {
        uint64_t seq;      // the model's own sequence number
        uint64_t version;  // version number as the store sees it
        uint64_t obj;
        uint64_t offset;   // byte offset of this record within the object
    };

    static void fill_range(std::vector<uint8_t> & c, int obj, uint64_t seq, uint64_t version,
        uint64_t offset, uint64_t len)
    {
        for (uint64_t i = 0; i < len; i += sizeof(payload_t))
            *(payload_t*)(c.data() + offset + i) = (payload_t){
                .seq = seq,
                .version = version,
                .obj = (uint64_t)obj,
                .offset = offset + i,
            };
    }

    const std::vector<uint8_t> & base_content(int obj)
    {
        static std::vector<uint8_t> zero;
        auto & m = objs[obj];
        if (!m.acked || m.content.at(m.acked).empty())
        {
            // Never written, or deleted - a write recreates it from scratch
            zero.assign(OBJ_SIZE, 0);
            return zero;
        }
        return m.content.at(m.acked);
    }

    // Hand an operation to the store. Only the callback and the trace line - see submit()
    // for the tracked variant, which is what the workload uses
    void enqueue(pending_t *p)
    {
        p->seq_id = ++seq_id;
        if (trace)
            printf(" seq %ju\n", p->seq_id);
        p->op.callback = [this, p](blockstore_op_t *op)
        {
            if (trace)
                printf("done %ju\n", p->seq_id);
            p->done = true;
        };
        bs->enqueue_op(&p->op);
    }

    pending_t *submit(pending_t *p)
    {
        for (int o: p->touched)
            busy[o]++;
        if (p->op.opcode == BS_OP_SYNC)
            sync_inflight++;
        else if (p->op.opcode == BS_OP_ROLLBACK)
            rollback_inflight++;
        enqueue(p);
        inflight.push_back(p);
        return p;
    }

    void free_op(pending_t *p)
    {
        if (p->op.buf)
            free(p->op.buf);
        delete p;
    }

    pending_t *submit_write(int obj)
    {
        auto & m = objs[obj];
        uint64_t offset = rnd(0, OBJ_SIZE/GRAN - 1) * GRAN;
        uint64_t len = rnd(1, (OBJ_SIZE - offset)/GRAN) * GRAN;
        uint64_t version = ++m.submitted;
        auto *p = new pending_t();
        p->obj = obj;
        p->touched.push_back(obj);
        p->version = version;
        p->bs_version = m.next_version++;
        p->content = base_content(obj);
        fill_range(p->content, obj, version, p->bs_version, offset, len);
        p->op.opcode = instant_writes ? BS_OP_WRITE_STABLE : BS_OP_WRITE;
        p->op.oid = oid(obj);
        p->op.version = p->bs_version;
        p->op.offset = offset;
        p->op.len = len;
        p->op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, len);
        memcpy(p->op.buf, p->content.data() + offset, len);
        p->bitmap.assign(BMP_SIZE, bitmap_byte(obj, p->bs_version));
        p->op.bitmap = p->bitmap.data();
        p->on_success = [this, obj]() { written(obj); };
        writes++;
        if (trace)
            printf("%s %jx:%jx v%ju %ju +%ju", instant_writes ? "write_stable" : "write", p->op.oid.inode, p->op.oid.stripe, p->bs_version, offset, len);
        return submit(p);
    }

    // Deletions carry no data and are stable as soon as they are written, in both
    // implementations. The model records them as an empty content vector.
    pending_t *submit_delete(int obj)
    {
        auto & m = objs[obj];
        auto *p = new pending_t();
        p->obj = obj;
        p->touched.push_back(obj);
        p->version = ++m.submitted;
        p->bs_version = m.next_version++;
        p->op.opcode = BS_OP_DELETE;
        p->op.oid = oid(obj);
        p->op.version = p->bs_version;
        p->op.offset = 0;
        p->op.len = 0;
        p->op.buf = NULL;
        p->on_success = [this, obj]() { written(obj); };
        deletes++;
        if (trace)
            printf("delete %jx:%jx v%ju", p->op.oid.inode, p->op.oid.stripe, p->bs_version);
        return submit(p);
    }

    pending_t *submit_sync()
    {
        auto *p = new pending_t();
        p->op.opcode = BS_OP_SYNC;
        // What the sync is going to cover: everything acknowledged by the time it was
        // submitted. Anything acknowledged later may or may not make it in, so it isn't
        // counted - the snapshot is a lower bound, which is all the model needs
        std::vector<uint64_t> covered(OBJ_COUNT);
        for (int obj = 0; obj < OBJ_COUNT; obj++)
            covered[obj] = objs[obj].acked;
        p->on_success = [this, covered]() { synced(covered); };
        syncs++;
        if (trace)
            printf("sync");
        return submit(p);
    }

    // Stabilize several versions in a single operation, the way an OSD does. With one
    // object touched this degenerates to a single entry, so both shapes get exercised.
    pending_t *submit_stable(const std::vector<std::pair<int, uint64_t>> & entries, bool rollback = false)
    {
        auto *p = new pending_t();
        p->stabilized = entries;
        p->op.opcode = rollback ? BS_OP_ROLLBACK : BS_OP_STABLE;
        p->op.len = entries.size();
        p->op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, entries.size()*sizeof(obj_ver_id));
        if (trace)
            printf(rollback ? "rollback " : "stable ");
        for (size_t i = 0; i < entries.size(); i++)
        {
            auto & ov = ((obj_ver_id*)p->op.buf)[i];
            ov = (obj_ver_id){
                .oid = oid(entries[i].first),
                .version = objs[entries[i].first].ver.at(entries[i].second),
            };
            p->touched.push_back(entries[i].first);
            if (trace)
                printf("%s%jx:%jx v%ju", i > 0 ? ", " : "", ov.oid.inode, ov.oid.stripe, ov.version);
        }
        return submit(p);
    }

    // ---- model updates driven by completions ----

    // A write or a deletion of `obj` completed. Whether that already makes it durable, and
    // whether it now needs a stabilize, is exactly the difference between the configurations
    void written(int obj)
    {
        auto & m = objs[obj];
        if (m.content.at(m.acked).empty())
        {
            // The object is gone, so the next write starts numbering over again
            m.next_version = 1;
            m.can_rollback = false;
            m.deleted_upto = m.acked;
            to_stabilize.erase(obj);
        }
        if (use_fsync)
        {
            // The disk has a volatile cache: nothing is guaranteed until a sync
            return;
        }
        m.synced = m.acked;
        promote(obj);
    }

    // A sync covering `covered` completed
    void synced(const std::vector<uint64_t> & covered)
    {
        for (int obj = 0; obj < OBJ_COUNT; obj++)
        {
            auto & m = objs[obj];
            // A deletion may have wiped out part of what the sync covered while it was in
            // flight, and then the versions below it don't exist any more - stabilizing
            // them would be as wrong here as it would be in an OSD
            if (!covered[obj] || covered[obj] > m.acked || covered[obj] <= m.synced ||
                covered[obj] < m.deleted_upto || !m.content.count(covered[obj]))
                continue;
            m.synced = covered[obj];
            promote(obj);
        }
    }

    // Everything up to `synced` is on the disk now. A deletion and a BS_OP_WRITE_STABLE
    // write are stable by themselves, so for them that already means durable; a two-phase
    // write still has to be stabilized, which happens asynchronously from ACT_STABILIZE
    void promote(int obj)
    {
        auto & m = objs[obj];
        if (m.synced <= m.durable)
            return;
        if (instant_writes || m.content.at(m.synced).empty())
        {
            m.durable = m.synced;
            prune(obj);
        }
        else
            to_stabilize[obj] = m.synced;
    }

    // ---- the steps themselves ----

    chaos_action_t pick_action()
    {
        const uint32_t w[ACT_COUNT] = {
            probs.crash, probs.restart, probs.write, probs.del,
            probs.read, probs.sync, probs.stabilize, probs.rollback, probs.wait,
        };
        uint64_t total = 0;
        for (int i = 0; i < ACT_COUNT; i++)
            total += w[i];
        if (!total)
            return ACT_WAIT;
        uint64_t dice = rnd(0, total-1), acc = 0;
        for (int i = 0; i < ACT_COUNT; i++)
        {
            acc += w[i];
            if (dice < acc)
                return (chaos_action_t)i;
        }
        return ACT_WAIT;
    }

    void step()
    {
        reap();
        auto action = pick_action();
        if (action == ACT_CRASH)
        {
            do_crash();
            return;
        }
        if (action == ACT_RESTART)
        {
            do_restart();
            return;
        }
        bool submitted = false;
        if (inflight.size() < probs.max_inflight)
        {
            switch (action)
            {
            case ACT_WRITE:     submitted = try_write(false); break;
            case ACT_DELETE:    submitted = try_write(true); break;
            case ACT_READ:      submitted = try_read(); break;
            case ACT_SYNC:      submitted = try_sync(); break;
            case ACT_STABILIZE: submitted = try_stabilize(); break;
            case ACT_ROLLBACK:  submitted = try_rollback(); break;
            default:            break;
            }
        }
        if (submitted)
            return;
        if (inflight.size() >= probs.max_inflight)
        {
            // The queue is full - wait for room. Nothing completing at all within the step
            // budget is the only way a hang shows up, so this is where it's caught
            if (!sim->run_until([this]()
                {
                    for (auto *p: inflight)
                        if (p->done)
                            return true;
                    return false;
                }, MAX_STEPS))
                report_deadlock();
            return;
        }
        sim->step();
    }

    // Fold every completed operation into the model and free it
    void reap()
    {
        for (size_t i = 0; i < inflight.size(); )
        {
            auto *p = inflight[i];
            if (!p->done)
            {
                i++;
                continue;
            }
            inflight.erase(inflight.begin()+i);
            account(p);
            free_op(p);
        }
    }

    // Only one operation at a time may refer to an object: that way the base content of a
    // write and the expected result of a read can't change while they're in flight
    std::vector<int> free_objects(bool must_exist)
    {
        std::vector<int> res;
        for (int obj = 0; obj < OBJ_COUNT; obj++)
            if (!busy[obj] && (!must_exist || exists(obj)))
                res.push_back(obj);
        return res;
    }

    // Deleting a non-existent object is a no-op that the store answers right away without
    // creating an entry, so only delete what is actually there
    bool try_write(bool del)
    {
        auto cand = free_objects(del);
        if (!cand.size())
            return false;
        int obj = cand[rnd(0, cand.size()-1)];
        if (del)
            submit_delete(obj);
        else
            submit_write(obj);
        return true;
    }

    // Read a random part of a random object. Ranges are aligned to the write granularity
    // but not to the checksum block size, so they cut across checksum blocks and across
    // the boundaries of the extents that were actually written.
    bool try_read()
    {
        auto cand = free_objects(false);
        if (!cand.size())
            return false;
        int obj = cand[rnd(0, cand.size()-1)];
        uint64_t offset = 0, len = OBJ_SIZE;
        if (!chance(probs.full_read))
        {
            offset = rnd(0, OBJ_SIZE/GRAN - 1) * GRAN;
            len = rnd(1, (OBJ_SIZE - offset)/GRAN) * GRAN;
        }
        auto *p = new pending_t();
        p->obj = obj;
        p->touched.push_back(obj);
        fill_read(p, obj, offset, len);
        submit(p);
        return true;
    }

    bool try_sync()
    {
        // A rollback rewrites the model, so don't let a snapshot of it fly across one
        if (rollback_inflight)
            return false;
        submit_sync();
        return true;
    }

    // Commit whatever completions have made stabilizable so far. Objects are picked from
    // to_stabilize, so a stabilize covers several of them exactly when several of them
    // happened to become ready - which is what an OSD does too
    bool try_stabilize()
    {
        std::vector<std::pair<int, uint64_t>> entries;
        for (auto & s: to_stabilize)
        {
            auto & m = objs[s.first];
            if (!busy[s.first] && s.second > m.durable && s.second <= m.synced &&
                m.ver.count(s.second) && m.content.count(s.second))
                entries.push_back(s);
        }
        if (!entries.size())
            return false;
        entries.resize(rnd(1, entries.size()));
        for (auto & e: entries)
            to_stabilize.erase(e.first);
        auto *p = submit_stable(entries);
        p->on_success = [this, entries]()
        {
            for (auto & e: entries)
            {
                objs[e.first].durable = e.second;
                objs[e.first].can_rollback = true;
                prune(e.first);
            }
        };
        stabilizes++;
        return true;
    }

    // Throw the unstable versions away instead of committing them. Only the two-phase mode
    // has anything to roll back - an instant write is stable at once. Rolling back to "no
    // version at all" isn't expressible here, and a deletion is stable by itself, so only
    // objects that have a stable version below the newest one can be rolled back
    bool try_rollback()
    {
        if (instant_writes || rollback_inflight || sync_inflight)
            return false;
        std::vector<std::pair<int, uint64_t>> rolled;
        for (int obj = 0; obj < OBJ_COUNT; obj++)
        {
            auto & m = objs[obj];
            if (!busy[obj] && m.can_rollback && m.acked > m.durable &&
                m.content.count(m.durable) && !m.content.at(m.durable).empty())
                rolled.push_back(std::make_pair(obj, m.durable));
        }
        if (!rolled.size())
            return false;
        rolled.resize(rnd(1, rolled.size()));
        for (auto & r: rolled)
            to_stabilize.erase(r.first);
        auto *p = submit_stable(rolled, true);
        p->on_success = [this, rolled]()
        {
            for (auto & r: rolled)
            {
                auto & m = objs[r.first];
                // Everything above the stable version is gone, and the store's version counter
                // for the object goes back to it as well
                while (m.content.size() && m.content.rbegin()->first > m.durable)
                {
                    m.ver.erase(m.content.rbegin()->first);
                    m.bmp.erase(m.content.rbegin()->first);
                    m.content.erase(std::prev(m.content.end()));
                }
                m.acked = m.synced = m.durable;
                m.next_version = m.ver.at(m.durable) + 1;
                rollbacks++;
            }
        };
        return true;
    }

    // Pull the plug right now. Whatever completed before the outage definitely happened;
    // whatever was still in flight may or may not have, so its content is only recorded as
    // a legal outcome. Every object then has to come back holding at least its durable
    // version - or something newer that was actually written
    void do_crash()
    {
        sim->power_loss();
        crashes++;
        for (auto *p: inflight)
            account(p);
        destroy();
        for (auto *p: inflight)
            free_op(p);
        forget_inflight();
        start_bs();
        for (int obj = 0; obj < OBJ_COUNT; obj++)
            check_recovered(obj, objs[obj].durable, "a power outage");
    }

    void forget_inflight()
    {
        inflight.clear();
        for (int obj = 0; obj < OBJ_COUNT; obj++)
            busy[obj] = 0;
        sync_inflight = rollback_inflight = 0;
        // The versions it named are either gone or renumbered by the recovery check
        to_stabilize.clear();
    }

    // Wait for everything in flight to complete and fold it all into the model
    void drain()
    {
        if (inflight.size())
        {
            if (!sim->run_until([this]()
                {
                    for (auto *p: inflight)
                        if (!p->done)
                            return false;
                    return true;
                }, MAX_STEPS))
                report_deadlock();
        }
        reap();
    }

    void report_deadlock()
    {
        fprintf(stderr, "step %ju: operations did not complete in %d steps at t=%juus - deadlock?\n",
            step_no, MAX_STEPS, sim->now_us);
        for (auto *p: inflight)
            if (!p->done)
                fprintf(stderr, "  stuck: %s %jx:%jx v%ju %u+%u\n", op_name(p),
                    p->op.oid.inode, p->op.oid.stripe, p->op.version, p->op.offset, p->op.len);
        bs->dump_diagnostics();
        dump_extra(bs);
        fflush(stdout);
        abort();
    }

    // Fold one finished - or interrupted - operation into the model
    void account(pending_t *p)
    {
        for (int o: p->touched)
            busy[o]--;
        if (p->op.opcode == BS_OP_SYNC)
            sync_inflight--;
        else if (p->op.opcode == BS_OP_ROLLBACK)
            rollback_inflight--;
        if (p->op.opcode == BS_OP_READ)
        {
            // A read interrupted by an outage tells us nothing
            if (p->done)
                check_read_result(p);
            return;
        }
        bool is_write = p->op.opcode == BS_OP_WRITE || p->op.opcode == BS_OP_WRITE_STABLE ||
            p->op.opcode == BS_OP_DELETE;
        bool ok = p->done && (p->op.opcode == BS_OP_WRITE || p->op.opcode == BS_OP_WRITE_STABLE
            ? p->op.retval == (int)p->op.len : p->op.retval == 0);
        if (p->done && !ok)
        {
            // Journal is full and the flusher can't keep up - expected under pressure.
            // A sync may fail for the same reason, and then it simply didn't sync anything
            if (is_write && p->op.retval == -EAGAIN)
            {
                refused_writes++;
                return;
            }
            if (p->op.opcode == BS_OP_SYNC)
                return;
            if (p->stabilized.size())
            {
                // The entries of a stabilize live in the buffer, not in op.oid/op.version
                fprintf(stderr, "%s failed with retval=%jd:", op_name(p), (int64_t)p->op.retval);
                for (size_t i = 0; i < p->stabilized.size(); i++)
                {
                    auto & ov = ((obj_ver_id*)p->op.buf)[i];
                    fprintf(stderr, " %jx:%jx v%ju", ov.oid.inode, ov.oid.stripe, ov.version);
                }
                fprintf(stderr, "\n");
                abort();
            }
            fprintf(stderr, "%s of %jx:%jx v%ju failed with retval=%jd\n", op_name(p),
                p->op.oid.inode, p->op.oid.stripe, p->op.version, (int64_t)p->op.retval);
            abort();
        }
        if (is_write)
        {
            // The content of a write which was in flight during an outage is just as legal an
            // outcome as the content of one which completed. An empty vector spells "deleted"
            auto & m = objs[p->obj];
            m.content[p->version] = p->content;
            m.ver[p->version] = p->bs_version;
            if (p->op.opcode != BS_OP_DELETE)
                m.bmp[p->version] = bitmap_byte(p->obj, p->bs_version);
            if (ok)
                m.acked = p->version;
        }
        if (ok && p->on_success)
            p->on_success();
    }

    static const char *op_name(pending_t *p)
    {
        return p->op.opcode == BS_OP_DELETE ? "delete"
            : p->op.opcode == BS_OP_SYNC ? "sync"
            : p->op.opcode == BS_OP_STABLE ? "stabilize"
            : p->op.opcode == BS_OP_ROLLBACK ? "rollback"
            : p->op.opcode == BS_OP_READ ? "read" : "write";
    }

    // Versions below the durable one can never be observed again
    void prune(int obj)
    {
        auto & m = objs[obj];
        while (m.content.size() && m.content.begin()->first < m.durable)
        {
            m.ver.erase(m.content.begin()->first);
            m.bmp.erase(m.content.begin()->first);
            m.content.erase(m.content.begin());
        }
    }

    void fill_read(pending_t *p, int obj, uint64_t offset, uint64_t len)
    {
        p->op.opcode = BS_OP_READ;
        p->op.oid = oid(obj);
        p->op.version = UINT64_MAX;
        p->op.offset = offset;
        p->op.len = len;
        p->op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, len);
        memset(p->op.buf, 0, len);
        p->bitmap.assign(BMP_SIZE, 0);
        p->op.bitmap = p->bitmap.data();
        if (trace)
            printf("read %jx:%jx %u +%u", p->op.oid.inode, p->op.oid.stripe, p->op.offset, p->op.len);
    }

    // Read the whole object synchronously and return its content, or empty if it doesn't
    // exist. Used by the recovery checks, where nothing else is in flight anyway
    uint64_t last_read_version = 0;
    std::vector<uint8_t> last_read_bitmap;
    std::vector<uint8_t> read_object(int obj, int *retval)
    {
        auto *p = new pending_t();
        fill_read(p, obj, 0, OBJ_SIZE);
        enqueue(p);
        if (!sim->run_until([p]() { return p->done; }, MAX_STEPS))
        {
            fprintf(stderr, "step %ju: a read of object %d did not complete in %d steps "
                "at t=%juus - deadlock?\n", step_no, obj, MAX_STEPS, sim->now_us);
            bs->dump_diagnostics();
            dump_extra(bs);
            fflush(stdout);
            abort();
        }
        *retval = p->op.retval;
        last_read_version = p->op.version;
        last_read_bitmap = p->bitmap;
        std::vector<uint8_t> res;
        if (p->op.retval == (int)OBJ_SIZE)
            res.assign(p->op.buf, p->op.buf + OBJ_SIZE);
        free_op(p);
        return res;
    }

    // The store must return the external bitmap of exactly the version it returned
    void check_bitmap(int obj, uint64_t version, const std::vector<uint8_t> & got, const char *what)
    {
        auto & m = objs[obj];
        auto it = m.bmp.find(version);
        if (it == m.bmp.end())
            return;
        std::vector<uint8_t> want(BMP_SIZE, it->second);
        if (got != want)
        {
            fprintf(stderr, "object %d %s returned the bitmap of another version: got %02x, expected %02x for v%ju\n",
                obj, what, got.size() ? got[0] : 0, it->second, version);
            abort();
        }
    }

    // No other operation may touch the object while a read of it is in flight, so a read
    // of the newest version must return exactly what the last acknowledged write put there -
    // stable or not
    void check_read_result(pending_t *p)
    {
        int obj = p->obj;
        auto & m = objs[obj];
        uint64_t offset = p->op.offset, len = p->op.len;
        if (!exists(obj))
        {
            if (p->op.retval != -ENOENT)
            {
                fprintf(stderr, "read of %s object %d returned retval=%jd\n",
                    m.acked ? "deleted" : "never-written", obj, (int64_t)p->op.retval);
                abort();
            }
            return;
        }
        std::vector<uint8_t> want(m.content.at(m.acked).begin() + offset,
            m.content.at(m.acked).begin() + offset + len);
        std::vector<uint8_t> got;
        if (p->op.retval == (int)len)
            got.assign(p->op.buf, p->op.buf + len);
        if (p->op.retval != (int)len || got != want)
        {
            fprintf(stderr, "read of object %d v%ju at %ju+%ju returned retval=%jd, content %s\n",
                obj, m.acked, offset, len, (int64_t)p->op.retval,
                p->op.retval == (int)len ? "mismatch" : "n/a");
            report_mismatch(obj, got, want, offset);
            abort();
        }
        check_bitmap(obj, m.acked, p->bitmap, "read");
        checked_reads++;
    }

    // Synchronous whole-object check, for the very end of the run
    void check_object(int obj)
    {
        auto *p = new pending_t();
        p->obj = obj;
        fill_read(p, obj, 0, OBJ_SIZE);
        enqueue(p);
        if (!sim->run_until([p]() { return p->done; }, MAX_STEPS))
        {
            fprintf(stderr, "the final read of object %d did not complete\n", obj);
            abort();
        }
        check_read_result(p);
        free_op(p);
    }

    void report_mismatch(int obj, const std::vector<uint8_t> & got, const std::vector<uint8_t> & want, uint64_t base = 0)
    {
        if (got.size() != want.size())
            return;
        for (uint64_t i = 0; i < got.size(); i += sizeof(payload_t))
        {
            auto & g = *(payload_t*)(got.data()+i);
            auto & w = *(payload_t*)(want.data()+i);
            if (memcmp(&g, &w, sizeof(payload_t)) != 0)
            {
                fprintf(stderr, "  object %d offset %ju: got obj=%ju seq=%ju v%ju off=%ju,"
                    " expected obj=%ju seq=%ju v%ju off=%ju\n", obj, base+i,
                    g.obj, g.seq, g.version, g.offset, w.obj, w.seq, w.version, w.offset);
                return;
            }
        }
    }

    // Restart without any power loss at all: everything in flight is allowed to finish
    // first, so nothing may be lost and every object must come back holding exactly what
    // its last acknowledged write put there
    void do_restart()
    {
        drain();
        restarts++;
        std::vector<std::vector<uint8_t>> before(OBJ_COUNT);
        std::vector<uint64_t> before_ver(OBJ_COUNT, 0);
        std::vector<int> before_rv(OBJ_COUNT, 0);
        for (int obj = 0; obj < OBJ_COUNT; obj++)
        {
            before[obj] = read_object(obj, &before_rv[obj]);
            before_ver[obj] = last_read_version;
        }
        // Let everything in flight finish right before pulling the blockstore out from
        // under it, or an unfinished write would later execute against freed buffers
        if (!sim->run_until([this]() { return !sim->has_inflight(); }, MAX_STEPS))
        {
            fprintf(stderr, "step %ju: disk did not go idle before a clean restart\n", step_no);
            abort();
        }
        destroy();
        forget_inflight();
        start_bs();
        for (int obj = 0; obj < OBJ_COUNT; obj++)
        {
            int rv = 0;
            auto after = read_object(obj, &rv);
            // With immediate_commit=all every acknowledged write is already durable, so a
            // restart that loses nothing must return exactly the same state. With
            // immediate_commit=none unsynced writes may legitimately be dropped.
            if (!use_fsync && (rv != before_rv[obj] || after != before[obj]))
            {
                uint64_t jstart = parse_size(config["journal_offset"]);
                uint64_t jend = parse_size(config["data_offset"]);
                uint64_t hole = data_disk->first_unwritten(jstart, jend);
                fprintf(stderr, "object %d changed across a clean restart that lost nothing: "
                    "before retval=%d v%ju, after retval=%d v%ju; first never-written journal "
                    "sector at %jx (journal %jx..%jx)\n",
                    obj, before_rv[obj], before_ver[obj], rv, last_read_version, hole, jstart, jend);
                abort();
            }
        }
        for (int obj = 0; obj < OBJ_COUNT; obj++)
        {
            // With immediate_commit=all every acknowledged write is already durable.
            // With immediate_commit=none only synced writes are guaranteed - the rest
            // may legitimately be dropped while replaying the journal.
            auto & m = objs[obj];
            check_recovered(obj, use_fsync ? m.synced : m.acked, "a clean restart");
        }
    }

    // Verify that an object came back from a restart holding the content of *some* version
    // at or above `min_version`, and take whatever came back as the new baseline for the
    // writes that follow. Which version is the lower bound depends on what was guaranteed:
    // the last durable one after a power outage, the last acknowledged or synced one after
    // a restart that lost nothing.
    void check_recovered(int obj, uint64_t min_version, const char *what)
    {
        auto & m = objs[obj];
        int retval = 0;
        auto got = read_object(obj, &retval);
        bool exists = retval == OBJ_SIZE;
        if (!exists && retval != -ENOENT)
        {
            fprintf(stderr, "read of object %d after %s failed with retval=%d "
                "(torn data committed?)\n", obj, what, retval);
            abort();
        }
        for (auto & v: m.content)
        {
            // An empty candidate is a deletion, and it matches an object that is gone
            if (v.first >= min_version && v.second.empty() != exists && (!exists || v.second == got))
            {
                // Whatever came back is the current state and the base for the writes that
                // follow, but it is NOT necessarily durable itself: if it came from a version
                // that was never stabilized, the next outage may still roll it back to the
                // last durable one. So keep the durable content around as a legal outcome
                // and only move the "current" pointer. Version numbers stay monotonic
                // because the blockstore may still hold unstable versions above the match.
                check_bitmap(obj, v.first, last_read_bitmap, what);
                std::vector<uint8_t> durable_content;
                bool has_durable = m.durable && m.content.count(m.durable);
                uint64_t durable_ver = has_durable ? m.ver.at(m.durable) : 0;
                bool has_durable_bmp = has_durable && m.bmp.count(m.durable);
                uint8_t durable_bmp = has_durable_bmp ? m.bmp.at(m.durable) : 0;
                if (has_durable)
                    durable_content = m.content.at(m.durable);
                bool got_bmp = exists && m.bmp.count(v.first);
                uint8_t got_bmp_byte = got_bmp ? m.bmp.at(v.first) : 0;
                m.content.clear();
                m.ver.clear();
                m.bmp.clear();
                if (has_durable)
                {
                    m.content[m.durable] = durable_content;
                    m.ver[m.durable] = durable_ver;
                    if (has_durable_bmp)
                        m.bmp[m.durable] = durable_bmp;
                }
                m.content[m.submitted] = got;
                m.ver[m.submitted] = exists ? last_read_version : 0;
                if (got_bmp)
                    m.bmp[m.submitted] = got_bmp_byte;
                m.acked = m.synced = m.submitted;
                m.can_rollback = false;
                m.deleted_upto = 0;
                // Continue numbering above whatever the store came back with, or start
                // over from 1 if the object is gone
                m.next_version = exists ? last_read_version+1 : 1;
                return;
            }
        }
        if (!exists && !min_version)
        {
            // Nothing was guaranteed, so losing the object entirely is a legal outcome
            m.acked = m.synced = 0;
            m.content.clear();
            m.ver.clear();
            m.bmp.clear();
            m.next_version = 1;
            m.can_rollback = false;
            m.deleted_upto = 0;
            return;
        }
        fprintf(stderr, "object %d came back from %s %s, which is neither a version at or above "
            "the guaranteed one nor anything that was ever written (guaranteed=v%ju, durable=v%ju, "
            "acked=v%ju, submitted=v%ju, blockstore reports v%ju)\n", obj, what,
            exists ? "holding unexpected content" : "gone", min_version,
            m.durable, m.acked, m.submitted, last_read_version);
        fprintf(stderr, "  guaranteed version %ju is %s; candidates:", min_version,
            m.content.count(min_version) ? (m.content.at(min_version).empty() ? "a deletion" : "content") : "not tracked");
        for (auto & v: m.content)
            fprintf(stderr, " v%ju=%s", v.first, v.second.empty() ? "deleted" : "content");
        fprintf(stderr, "\n");
        // Show how far off every candidate is - the one that almost matches usually
        // tells which write was lost or applied out of order
        for (auto & v: m.content)
        {
            if (v.second.empty() || v.second.size() != got.size())
                continue;
            fprintf(stderr, "  vs v%ju (store v%ju):\n", v.first, m.ver.count(v.first) ? m.ver.at(v.first) : 0);
            report_mismatch(obj, got, v.second);
        }
        abort();
    }

    void run(uint64_t steps)
    {
        for (step_no = 0; step_no < steps; step_no++)
            step();
        // Everything still in flight must complete, and everything durable must still be there
        drain();
        for (int obj = 0; obj < OBJ_COUNT; obj++)
            check_object(obj);
    }
};

template<class BS>
static void run_impl(const chaos_cfg_t & cfg, int meta_format, uint32_t seed, uint64_t steps, bool trace)
{
    printf("\n-- chaos %s seed=%u steps=%ju fsync=%d csum_block=%s writes=%s\n",
        cfg.impl, seed, steps, cfg.use_fsync ? 1 : 0,
        cfg.csum_block ? std::to_string(cfg.csum_block).c_str() : "none",
        cfg.instant_writes ? "instant" : "two-phase");
    chaos_t<BS> t;
    t.configure(cfg, meta_format);
    t.trace = trace;
    t.create(seed);
    t.run(steps);
    printf("OK: %ju writes (%ju refused with EAGAIN), %ju deletes, %ju syncs, %ju stabilizes, "
        "%ju rollbacks, %ju crashes, %ju clean restarts, %ju verified reads, %ju disk ops\n",
        t.writes, t.refused_writes, t.deletes, t.syncs, t.stabilizes, t.rollbacks, t.crashes,
        t.restarts, t.checked_reads, t.data_disk->completed_ops);
}

// Both implementations, both commit modes, and checksum blocks equal to and larger than
// the write granularity - partial checksum block updates are a rich source of bugs - plus
// the checksumless mode
static const chaos_cfg_t configs[] = {
    { .impl = "v2", .use_fsync = false, .csum_block = 0    , .instant_writes = false },
    { .impl = "v2", .use_fsync = false, .csum_block = 4096 , .instant_writes = false },
    { .impl = "v2", .use_fsync = false, .csum_block = 16384, .instant_writes = false },
    { .impl = "v2", .use_fsync = true , .csum_block = 0    , .instant_writes = false },
    { .impl = "v2", .use_fsync = true , .csum_block = 4096 , .instant_writes = false },
    { .impl = "v2", .use_fsync = true , .csum_block = 16384, .instant_writes = false },
    { .impl = "v2", .use_fsync = false, .csum_block = 0    , .instant_writes = true },
    { .impl = "v2", .use_fsync = false, .csum_block = 4096 , .instant_writes = true },
    { .impl = "v2", .use_fsync = false, .csum_block = 16384, .instant_writes = true },
    { .impl = "v2", .use_fsync = true , .csum_block = 0    , .instant_writes = true },
    { .impl = "v2", .use_fsync = true , .csum_block = 4096 , .instant_writes = true },
    { .impl = "v2", .use_fsync = true , .csum_block = 16384, .instant_writes = true },
    { .impl = "v1", .use_fsync = false, .csum_block = 0    , .instant_writes = false },
    { .impl = "v1", .use_fsync = false, .csum_block = 4096 , .instant_writes = false },
    { .impl = "v1", .use_fsync = false, .csum_block = 16384, .instant_writes = false },
    { .impl = "v1", .use_fsync = true , .csum_block = 0    , .instant_writes = false },
    { .impl = "v1", .use_fsync = true , .csum_block = 4096 , .instant_writes = false },
    { .impl = "v1", .use_fsync = true , .csum_block = 16384, .instant_writes = false },
    { .impl = "v1", .use_fsync = false, .csum_block = 0    , .instant_writes = true },
    { .impl = "v1", .use_fsync = false, .csum_block = 4096 , .instant_writes = true },
    { .impl = "v1", .use_fsync = false, .csum_block = 16384, .instant_writes = true },
    { .impl = "v1", .use_fsync = true , .csum_block = 0    , .instant_writes = true },
    { .impl = "v1", .use_fsync = true , .csum_block = 4096 , .instant_writes = true },
    { .impl = "v1", .use_fsync = true , .csum_block = 16384, .instant_writes = true },
};

static void run_one(const chaos_cfg_t & cfg, uint32_t seed, uint64_t steps, bool trace)
{
    if (!strcmp(cfg.impl, "v1"))
        run_impl<v1::blockstore_impl_t>(cfg, 2, seed, steps, trace);
    else
        run_impl<blockstore_impl_t>(cfg, 3, seed, steps, trace);
}

static const char *help_text =
    "Seed-driven crash chaos test for the blockstore, (c) Vitaliy Filippov, 2019+ (VNPL-1.1)\n"
    "\n"
    "USAGE:\n"
    "  test_blockstore_chaos [OPTIONS]\n"
    "\n"
    "Without options it runs a fixed range of seeds covering every configuration twice, and\n"
    "then fills the run up to %d seeds with random ones, so that every build tries a new set\n"
    "of situations instead of the same ones over and over. Every seed is printed, and the\n"
    "failing ones are listed at the end, so a failure is reproduced by passing its seed back.\n"
    "Seeds are run one per child process, so a failure doesn't stop the sweep. A single seed\n"
    "runs in-process, which is what you want under a debugger.\n"
    "\n"
    "  --seed <n>          Run just this seed, in-process\n"
    "  --seeds <a>-<b>     Run seeds <a> through <b> (default 1-%d). Turns the random seeds\n"
    "                      off unless --random is given as well\n"
    "  --random <n>        Run <n> random seeds after the range above (default %d). They are\n"
    "                      spread over the configurations evenly\n"
    "  --steps <n>         Steps per seed (default %ju)\n"
    "  --config <n>        Force configuration <n> instead of deriving it from the seed\n"
    "  --max-inflight <n>  Operations in flight at once (default %u)\n"
    "  -v, --trace         Trace every operation and every simulated disk request\n"
    "  -h, --help          Show this help\n"
    "\n"
    "Every step picks one action below at random, by weight. The weights are relative to\n"
    "each other, and setting one to 0 turns the action off, e.g. --p-delete 0 --p-rollback 0\n"
    "gives a workload without deletions and rollbacks:\n"
    "  --p-wait <n>        Submit nothing, just let the simulation run (default %u)\n"
    "  --p-write <n>       Write a random range of a random object (default %u)\n"
    "  --p-delete <n>      Delete an existing object (default %u)\n"
    "  --p-read <n>        Read back and verify a random object (default %u)\n"
    "  --p-sync <n>        Sync (default %u)\n"
    "  --p-stabilize <n>   Commit whatever became stabilizable (default %u)\n"
    "  --p-rollback <n>    Roll back to the last stable version, two-phase mode only (default %u)\n"
    "  --p-restart <n>     Clean restart, letting everything in flight finish (default %u)\n"
    "  --p-crash <n>       Pull the plug right now, whatever is in flight (default %u)\n"
    "\n"
    "  --p-full-read <n>   Percentage of reads covering the whole object (default %u)\n"
    "\n"
    "Configurations (%d):\n";

static void print_help(int cfg_count, int default_seeds, int default_fixed, int default_random, uint64_t default_steps)
{
    chaos_probs_t d;
    printf(help_text, default_seeds, default_fixed, default_random, default_steps, d.max_inflight,
        d.wait, d.write, d.del, d.read, d.sync, d.stabilize, d.rollback, d.restart, d.crash,
        d.full_read, cfg_count);
    for (int i = 0; i < cfg_count; i++)
        printf("  %2d: %s, fsync=%d, csum_block=%s, writes=%s\n", i, configs[i].impl,
            configs[i].use_fsync ? 1 : 0,
            configs[i].csum_block ? std::to_string(configs[i].csum_block).c_str() : "none",
            configs[i].instant_writes ? "instant" : "two-phase");
}

// Run one seed in a child process so that an aborted run doesn't take the whole sweep
// with it. Returns false if the child died or exited non-zero.
static bool run_seed_isolated(const chaos_cfg_t & cfg, uint32_t seed, uint64_t steps, bool trace)
{
    fflush(stdout);
    fflush(stderr);
    pid_t pid = fork();
    if (pid < 0)
    {
        fprintf(stderr, "fork: %s\n", strerror(errno));
        exit(1);
    }
    if (!pid)
    {
        run_one(cfg, seed, steps, trace);
        fflush(stdout);
        fflush(stderr);
        _exit(0);
    }
    int status = 0;
    while (waitpid(pid, &status, 0) < 0 && errno == EINTR) {}
    if (WIFSIGNALED(status))
    {
        printf("seed %u killed by signal %d\n", seed, WTERMSIG(status));
        return false;
    }
    return WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

int main(int narg, char *args[])
{
    // Failures are reported on stderr while progress goes to stdout - keep them in order
    setvbuf(stdout, NULL, _IONBF, 0);
    setvbuf(stderr, NULL, _IONBF, 0);
    const int cfg_count = sizeof(configs)/sizeof(configs[0]);
    const int default_seeds = 200;
    // Every configuration twice as a fixed regression floor, and the rest of the run filled up
    // with random seeds so that every build tries situations the previous ones never reached
    const int default_fixed = 2*cfg_count;
    const int default_random = default_seeds > default_fixed ? default_seeds-default_fixed : 0;
    bool trace = false, single = false, seeds_given = false, random_given = false;
    uint32_t seed_from = 1, seed_to = default_fixed, seed = 0;
    int random_count = default_random;
    uint64_t steps = 10000;
    int force_cfg = -1;
    for (int i = 1; i < narg; i++)
    {
        const char *opt = args[i];
        const char *val = i < narg-1 ? args[i+1] : NULL;
        auto take = [&](uint64_t & into)
        {
            if (!val)
            {
                fprintf(stderr, "%s requires a value\n", opt);
                exit(1);
            }
            into = strtoull(val, NULL, 10);
            i++;
        };
        uint64_t v = 0;
        if (!strcmp(opt, "-h") || !strcmp(opt, "--help"))
        {
            print_help(cfg_count, default_seeds, default_fixed, default_random, steps);
            return 0;
        }
        else if (!strcmp(opt, "-v") || !strcmp(opt, "--trace"))
            trace = true;
        else if (!strcmp(opt, "--seed"))
        {
            take(v);
            seed = (uint32_t)v;
            single = true;
        }
        else if (!strcmp(opt, "--seeds"))
        {
            if (!val)
            {
                fprintf(stderr, "--seeds requires a value\n");
                exit(1);
            }
            char *end = NULL;
            seed_from = seed_to = (uint32_t)strtoul(val, &end, 10);
            if (end && *end == '-')
                seed_to = (uint32_t)strtoul(end+1, NULL, 10);
            single = false;
            seeds_given = true;
            i++;
        }
        else if (!strcmp(opt, "--random"))
        {
            take(v);
            random_count = (int)v;
            random_given = true;
        }
        else if (!strcmp(opt, "--steps"))
            take(steps);
        else if (!strcmp(opt, "--config"))
        {
            take(v);
            force_cfg = (int)(v % cfg_count);
        }
        else if (!strcmp(opt, "--max-inflight")) { take(v); probs.max_inflight = (uint32_t)v; }
        else if (!strcmp(opt, "--p-wait"))       { take(v); probs.wait = (uint32_t)v; }
        else if (!strcmp(opt, "--p-write"))      { take(v); probs.write = (uint32_t)v; }
        else if (!strcmp(opt, "--p-delete"))     { take(v); probs.del = (uint32_t)v; }
        else if (!strcmp(opt, "--p-read"))       { take(v); probs.read = (uint32_t)v; }
        else if (!strcmp(opt, "--p-sync"))       { take(v); probs.sync = (uint32_t)v; }
        else if (!strcmp(opt, "--p-stabilize"))  { take(v); probs.stabilize = (uint32_t)v; }
        else if (!strcmp(opt, "--p-rollback"))   { take(v); probs.rollback = (uint32_t)v; }
        else if (!strcmp(opt, "--p-restart"))    { take(v); probs.restart = (uint32_t)v; }
        else if (!strcmp(opt, "--p-crash"))      { take(v); probs.crash = (uint32_t)v; }
        else if (!strcmp(opt, "--p-full-read"))  { take(v); probs.full_read = (uint32_t)v; }
        else
        {
            fprintf(stderr, "Unknown option %s, use -h for help\n", opt);
            exit(1);
        }
    }
    if (!probs.max_inflight)
    {
        fprintf(stderr, "--max-inflight must be at least 1\n");
        exit(1);
    }
    // The seed picks the configuration as well, so reproducing a failure is a single short run
    if (single)
    {
        run_one(configs[force_cfg >= 0 ? force_cfg : (int)(seed % cfg_count)], seed, steps, trace);
        printf("\nall ok\n");
        return 0;
    }
    // An explicit range means "run exactly these", not "these plus a pile of random ones"
    if (seeds_given && !random_given)
        random_count = 0;
    std::vector<uint32_t> seeds;
    for (uint32_t s = seed_from; s <= seed_to; s++)
        seeds.push_back(s);
    size_t fixed_count = seeds.size();
    if (random_count > 0)
    {
        std::set<uint32_t> used(seeds.begin(), seeds.end());
        std::mt19937 rnd(time(NULL) ^ getpid());
        for (int i = 0; i < random_count; i++)
        {
            uint32_t s;
            do
            {
                // The seed picks the configuration too, and some configurations are far more
                // interesting than others - so spread the random seeds over them evenly
                s = (rnd() % (1u<<30)) / cfg_count * cfg_count + (i % cfg_count);
            } while (!s || !used.insert(s).second);
            seeds.push_back(s);
        }
        printf("Running %zu fixed seed(s) and %d random one(s)\n", fixed_count, random_count);
    }
    std::vector<uint32_t> failed;
    for (auto s: seeds)
    {
        if (!run_seed_isolated(configs[force_cfg >= 0 ? force_cfg : (int)(s % cfg_count)], s, steps, trace))
            failed.push_back(s);
    }
    if (failed.size())
    {
        printf("\nFailing seeds:");
        for (auto s: failed)
            printf(" %u", s);
        printf("\n%zu of %zu seed(s) failed\n", failed.size(), seeds.size());
        return 1;
    }
    printf("\nall ok (%zu seed(s))\n", seeds.size());
    return 0;
}
