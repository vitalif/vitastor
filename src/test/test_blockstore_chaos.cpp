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
// Usage: test_blockstore_chaos [seed] [rounds] [config]
// The seed picks the configuration too, so a failing run is replayed exactly by passing
// the seed it printed. Without arguments it sweeps seeds over every configuration.

#include <malloc.h>
#include <map>
#include <set>
#include <vector>

#include "str_util.h"
#include "ringloop_mock.h"
#include "blockstore_impl.h"
#include "v1/impl.h"

#define OBJ_COUNT 4
#define OBJ_SIZE (128*1024)
#define GRAN 4096
#define MAX_STEPS 1000000

// One point of the configuration matrix. The two implementations are supposed to have
// identical semantics, so the very same workload and the very same checks run against both.
struct chaos_cfg_t
{
    const char *impl;
    // false: immediate_commit=all with all fsyncs disabled
    // true:  immediate_commit=none, data fsync enabled, so the disk has a volatile cache
    bool use_fsync;
    // Checksum block size. Deliberately includes sizes larger than the write granularity,
    // where a write only covers part of a checksummed block
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
    // Full object content as of every sequence number we still care about.
    // An empty vector means the object was deleted at that point.
    std::map<uint64_t, std::vector<uint8_t>> content;
    // Sequence number -> version number as seen by the store
    std::map<uint64_t, uint64_t> ver;
};

struct pending_t
{
    blockstore_op_t op = {};
    bool done = false;
    int obj = -1;
    // Sequence number in the model, and the version number handed to the store
    uint64_t version = 0;
    uint64_t bs_version = 0;
    std::vector<uint8_t> content;
    // For a stabilize op: every (object, version) pair it carries
    std::vector<std::pair<int, uint64_t>> stabilized;
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
    uint64_t crashes = 0, writes = 0, deletes = 0, refused_writes = 0, checked_reads = 0, round = 0;
    bool use_fsync = false, instant_writes = false;

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
        config["data_csum_type"] = "crc32c";
        config["csum_block_size"] = std::to_string(cfg.csum_block);
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
        data_disk->faults.forbid_overlapping_writes = true;
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

    uint64_t rnd(uint64_t min, uint64_t max)
    {
        return sim->random(min, max);
    }

    object_id oid(int i)
    {
        return (object_id){ .inode = 1, .stripe = (uint64_t)i * OBJ_SIZE };
    }

    // Self-describing payload: every 8 bytes tell which object, version and offset
    // they were written by, so a mismatch says exactly where the stale data came from
    static void fill_range(std::vector<uint8_t> & c, int obj, uint64_t version, uint64_t offset, uint64_t len)
    {
        for (uint64_t i = 0; i < len; i += 8)
            *(uint64_t*)(c.data() + offset + i) = (version << 40) | ((uint64_t)(obj & 0xff) << 32) | (offset + i);
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

    pending_t *submit(pending_t *p)
    {
        p->op.callback = [p](blockstore_op_t *op) { p->done = true; };
        bs->enqueue_op(&p->op);
        return p;
    }

    pending_t *submit_write(int obj)
    {
        auto & m = objs[obj];
        uint64_t offset = rnd(0, OBJ_SIZE/GRAN - 1) * GRAN;
        uint64_t len = rnd(1, (OBJ_SIZE - offset)/GRAN) * GRAN;
        uint64_t version = ++m.submitted;
        auto *p = new pending_t();
        p->obj = obj;
        p->version = version;
        p->bs_version = m.next_version++;
        p->content = base_content(obj);
        fill_range(p->content, obj, version, offset, len);
        p->op.opcode = instant_writes ? BS_OP_WRITE_STABLE : BS_OP_WRITE;
        p->op.oid = oid(obj);
        p->op.version = p->bs_version;
        p->op.offset = offset;
        p->op.len = len;
        p->op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, len);
        memcpy(p->op.buf, p->content.data() + offset, len);
        writes++;
        return submit(p);
    }

    // Deletions carry no data and are stable as soon as they are written, in both
    // implementations. The model records them as an empty content vector.
    pending_t *submit_delete(int obj)
    {
        auto & m = objs[obj];
        auto *p = new pending_t();
        p->obj = obj;
        p->version = ++m.submitted;
        p->bs_version = m.next_version++;
        p->op.opcode = BS_OP_DELETE;
        p->op.oid = oid(obj);
        p->op.version = p->bs_version;
        p->op.offset = 0;
        p->op.len = 0;
        p->op.buf = NULL;
        deletes++;
        return submit(p);
    }

    pending_t *submit_sync()
    {
        auto *p = new pending_t();
        p->op.opcode = BS_OP_SYNC;
        return submit(p);
    }

    // Stabilize several versions in a single operation, the way an OSD does. With one
    // object touched this degenerates to a single entry, so both shapes get exercised.
    pending_t *submit_stable(const std::vector<std::pair<int, uint64_t>> & entries)
    {
        auto *p = new pending_t();
        p->stabilized = entries;
        p->op.opcode = BS_OP_STABLE;
        p->op.len = entries.size();
        p->op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, entries.size()*sizeof(obj_ver_id));
        for (size_t i = 0; i < entries.size(); i++)
            ((obj_ver_id*)p->op.buf)[i] = (obj_ver_id){
                .oid = oid(entries[i].first),
                .version = objs[entries[i].first].ver.at(entries[i].second),
            };
        return submit(p);
    }

    void wait_all(std::vector<pending_t*> & batch)
    {
        bool ok = sim->run_until([&batch]()
        {
            for (auto *p: batch)
                if (!p->done)
                    return false;
            return true;
        }, MAX_STEPS);
        if (!ok)
        {
            fprintf(stderr, "round %ju: operations did not complete in %d steps at t=%juus - deadlock?\n",
                round, MAX_STEPS, sim->now_us);
            for (auto *p: batch)
                if (!p->done)
                    fprintf(stderr, "  stuck: opcode=%ju %jx:%jx v%ju %u+%u\n", p->op.opcode,
                        p->op.oid.inode, p->op.oid.stripe, p->op.version, p->op.offset, p->op.len);
            bs->dump_diagnostics();
            dump_extra(bs);
            fflush(stdout);
            abort();
        }
    }

    void free_batch(std::vector<pending_t*> & batch)
    {
        for (auto *p: batch)
        {
            if (p->op.buf)
                free(p->op.buf);
            delete p;
        }
        batch.clear();
    }

    // Write to a few objects at once, then optionally sync and stabilize. A version
    // counts as durable once its write was synced and then stabilized, because
    // stabilizing fsyncs the journal itself in both implementations. Writes made with
    // BS_OP_WRITE_STABLE are stable already, so for them the sync alone is enough.
    void do_write_round()
    {
        std::vector<pending_t*> batch;
        std::set<int> touched;
        int n = (int)rnd(1, OBJ_COUNT);
        for (int i = 0; i < n; i++)
        {
            int obj = (int)rnd(0, OBJ_COUNT-1);
            if (touched.find(obj) != touched.end())
                continue;
            touched.insert(obj);
            auto & m = objs[obj];
            bool exists = m.acked && !m.content.at(m.acked).empty();
            // Deleting a non-existent object is a no-op that the store answers right away
            // without creating an entry, so only delete what is actually there
            if (exists && rnd(0, 7) == 0)
                batch.push_back(submit_delete(obj));
            else
                batch.push_back(submit_write(obj));
        }
        wait_all(batch);
        for (auto *p: batch)
        {
            auto & m = objs[p->obj];
            if (p->op.opcode == BS_OP_DELETE ? p->op.retval == 0 : p->op.retval == (int)p->op.len)
            {
                m.acked = p->version;
                // p->content stays empty for a deletion, which is how the model spells "gone"
                m.content[p->version] = p->content;
                m.ver[p->version] = p->bs_version;
                if (p->op.opcode == BS_OP_DELETE)
                {
                    // The object is gone, so the next write starts numbering over again
                    m.next_version = 1;
                }
            }
            else if (p->op.retval == -EAGAIN)
            {
                // Journal is full and the flusher can't keep up - expected under pressure
                refused_writes++;
            }
            else
            {
                fprintf(stderr, "write of %jx:%jx v%ju failed with retval=%jd\n",
                    p->op.oid.inode, p->op.oid.stripe, p->version, (int64_t)p->op.retval);
                abort();
            }
        }
        free_batch(batch);
        if (rnd(0, 3) == 0)
            return;
        batch.push_back(submit_sync());
        wait_all(batch);
        bool synced = batch[0]->op.retval == 0;
        free_batch(batch);
        if (!synced)
            return;
        for (int obj: touched)
        {
            auto & m = objs[obj];
            m.synced = m.acked;
            // A version written with BS_OP_WRITE_STABLE is stable as soon as it's written,
            // and so is a deletion. For those the sync we just did already made it durable,
            // and stabilizing them would be wrong - there is nothing to commit.
            bool already_stable = instant_writes || m.synced && m.content.at(m.synced).empty();
            if (already_stable && m.synced > m.durable)
            {
                m.durable = m.synced;
                prune(obj);
            }
        }
        if (rnd(0, 3) == 0)
            return;
        std::vector<std::pair<int, uint64_t>> stabilized;
        for (int obj: touched)
            if (objs[obj].synced > objs[obj].durable)
                stabilized.push_back(std::make_pair(obj, objs[obj].synced));
        if (!stabilized.size())
            return;
        batch.push_back(submit_stable(stabilized));
        wait_all(batch);
        if (batch[0]->op.retval != 0)
        {
            fprintf(stderr, "stabilize of %zu version(s) starting at %jx:%jx v%ju failed with retval=%jd\n",
                stabilized.size(), oid(stabilized[0].first).inode, oid(stabilized[0].first).stripe,
                stabilized[0].second, (int64_t)batch[0]->op.retval);
            abort();
        }
        free_batch(batch);
        for (auto & s: stabilized)
        {
            objs[s.first].durable = s.second;
            prune(s.first);
        }
    }

    // Versions below the durable one can never be observed again
    void prune(int obj)
    {
        auto & m = objs[obj];
        while (m.content.size() && m.content.begin()->first < m.durable)
        {
            m.ver.erase(m.content.begin()->first);
            m.content.erase(m.content.begin());
        }
    }

    // Read the whole object and return its content, or empty if it doesn't exist
    uint64_t last_read_version = 0;
    std::vector<uint8_t> read_object(int obj, int *retval)
    {
        std::vector<pending_t*> batch;
        auto *p = new pending_t();
        p->op.opcode = BS_OP_READ;
        p->op.oid = oid(obj);
        p->op.version = UINT64_MAX;
        p->op.offset = 0;
        p->op.len = OBJ_SIZE;
        p->op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, OBJ_SIZE);
        memset(p->op.buf, 0, OBJ_SIZE);
        batch.push_back(submit(p));
        wait_all(batch);
        *retval = p->op.retval;
        last_read_version = p->op.version;
        std::vector<uint8_t> res;
        if (p->op.retval == OBJ_SIZE)
            res.assign(p->op.buf, p->op.buf + OBJ_SIZE);
        free_batch(batch);
        return res;
    }

    // With nothing in flight, a read of the newest version must return exactly what
    // the last acknowledged write put there - stable or not
    void check_read(int obj)
    {
        auto & m = objs[obj];
        int retval = 0;
        auto got = read_object(obj, &retval);
        if (!m.acked || m.content.at(m.acked).empty())
        {
            if (retval != -ENOENT)
            {
                fprintf(stderr, "read of %s object %d returned retval=%d\n",
                    m.acked ? "deleted" : "never-written", obj, retval);
                abort();
            }
            return;
        }
        auto & want = m.content.at(m.acked);
        if (retval != OBJ_SIZE || got != want)
        {
            fprintf(stderr, "read of object %d v%ju returned retval=%d, content %s\n",
                obj, m.acked, retval, retval == OBJ_SIZE ? "mismatch" : "n/a");
            report_mismatch(obj, got, want);
            abort();
        }
        checked_reads++;
    }

    void report_mismatch(int obj, const std::vector<uint8_t> & got, const std::vector<uint8_t> & want)
    {
        if (got.size() != want.size())
            return;
        for (uint64_t i = 0; i < got.size(); i += 8)
        {
            uint64_t g = *(uint64_t*)(got.data()+i), w = *(uint64_t*)(want.data()+i);
            if (g != w)
            {
                fprintf(stderr, "  object %d offset %ju: got obj=%ju v%ju off=%ju, expected obj=%ju v%ju off=%ju\n",
                    obj, i, (g >> 32) & 0xff, g >> 40, g & 0xffffffff,
                    (w >> 32) & 0xff, w >> 40, w & 0xffffffff);
                return;
            }
        }
    }

    // Cut the power in the middle of a batch of operations, restart, and check that
    // every object came back holding the content of *some* version between the last
    // one we know is durable and the last one we ever submitted a write for.
    void do_crash_round()
    {
        std::vector<pending_t*> batch;
        int n = (int)rnd(1, OBJ_COUNT);
        std::set<int> touched;
        for (int i = 0; i < n; i++)
        {
            int obj = (int)rnd(0, OBJ_COUNT-1);
            if (touched.find(obj) != touched.end())
                continue;
            touched.insert(obj);
            batch.push_back(submit_write(obj));
        }
        if (rnd(0, 1))
            batch.push_back(submit_sync());
        // Let the operations get partway through, then pull the plug
        uint64_t steps = rnd(0, 40);
        for (uint64_t i = 0; i < steps; i++)
            sim->step();
        for (auto *p: batch)
        {
            if (p->obj < 0)
                continue;
            auto & m = objs[p->obj];
            if (p->done && p->op.retval == (int)p->op.len)
            {
                // Completed before the outage, so it definitely happened
                m.acked = p->version;
                m.content[p->version] = p->content;
            }
            else if (!p->done)
            {
                // Still in flight when the power went away: the blockstore may or may not
                // have committed it, so record the content as a legal outcome only
                m.content[p->version] = p->content;
            }
        }
        sim->power_loss();
        crashes++;
        destroy();
        free_batch(batch);
        start_bs();
        for (int obj = 0; obj < OBJ_COUNT; obj++)
            check_recovered(obj, objs[obj].durable, "a power outage");
    }

    // Restart without any power loss at all: everything in flight is allowed to finish
    // first, so nothing may be lost and every object must come back holding exactly what
    // its last acknowledged write put there
    void do_restart_round()
    {
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
            fprintf(stderr, "round %ju: disk did not go idle before a clean restart\n", round);
            abort();
        }
        destroy();
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
                std::vector<uint8_t> durable_content;
                bool has_durable = m.durable && m.content.count(m.durable);
                uint64_t durable_ver = has_durable ? m.ver.at(m.durable) : 0;
                if (has_durable)
                    durable_content = m.content.at(m.durable);
                m.content.clear();
                m.ver.clear();
                if (has_durable)
                {
                    m.content[m.durable] = durable_content;
                    m.ver[m.durable] = durable_ver;
                }
                m.content[m.submitted] = got;
                m.ver[m.submitted] = exists ? last_read_version : 0;
                m.acked = m.synced = m.submitted;
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
            m.next_version = 1;
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
        report_mismatch(obj, got, m.content.count(min_version) ? m.content.at(min_version) : got);
        abort();
    }

    void run(uint64_t rounds)
    {
        for (uint64_t i = 0; i < rounds; i++)
        {
            round = i;
            uint64_t dice = rnd(0, 9);
            if (dice < 6)
                do_write_round();
            else if (dice < 7)
                check_read((int)rnd(0, OBJ_COUNT-1));
            else if (dice < 8)
                do_restart_round();
            else
                do_crash_round();
        }
        // Everything durable must still be there at the end
        for (int obj = 0; obj < OBJ_COUNT; obj++)
            check_read(obj);
    }
};

template<class BS>
static void run_impl(const chaos_cfg_t & cfg, int meta_format, uint32_t seed, uint64_t rounds)
{
    printf("\n-- chaos %s seed=%u rounds=%ju fsync=%d csum_block=%u writes=%s\n",
        cfg.impl, seed, rounds, cfg.use_fsync ? 1 : 0, cfg.csum_block,
        cfg.instant_writes ? "instant" : "two-phase");
    chaos_t<BS> t;
    t.configure(cfg, meta_format);
    t.create(seed);
    t.run(rounds);
    printf("OK: %ju writes (%ju refused with EAGAIN), %ju deletes, %ju crashes, "
        "%ju verified reads, %ju disk ops\n", t.writes, t.refused_writes, t.deletes,
        t.crashes, t.checked_reads, t.data_disk->completed_ops);
}

// Both implementations, both commit modes, and checksum blocks equal to and larger than
// the write granularity - partial checksum block updates are a rich source of bugs
static const chaos_cfg_t configs[] = {
    { .impl = "v2", .use_fsync = false, .csum_block = 4096,  .instant_writes = false },
    { .impl = "v2", .use_fsync = false, .csum_block = 16384, .instant_writes = false },
    { .impl = "v2", .use_fsync = true,  .csum_block = 4096,  .instant_writes = false },
    { .impl = "v2", .use_fsync = true,  .csum_block = 16384, .instant_writes = false },
    { .impl = "v2", .use_fsync = false, .csum_block = 4096,  .instant_writes = true },
    { .impl = "v2", .use_fsync = false, .csum_block = 16384, .instant_writes = true },
    { .impl = "v2", .use_fsync = true,  .csum_block = 4096,  .instant_writes = true },
    { .impl = "v2", .use_fsync = true,  .csum_block = 16384, .instant_writes = true },
    { .impl = "v1", .use_fsync = false, .csum_block = 4096,  .instant_writes = false },
    { .impl = "v1", .use_fsync = false, .csum_block = 16384, .instant_writes = false },
    { .impl = "v1", .use_fsync = true,  .csum_block = 4096,  .instant_writes = false },
    { .impl = "v1", .use_fsync = true,  .csum_block = 16384, .instant_writes = false },
    { .impl = "v1", .use_fsync = false, .csum_block = 4096,  .instant_writes = true },
    { .impl = "v1", .use_fsync = false, .csum_block = 16384, .instant_writes = true },
    { .impl = "v1", .use_fsync = true,  .csum_block = 4096,  .instant_writes = true },
    { .impl = "v1", .use_fsync = true,  .csum_block = 16384, .instant_writes = true },
};

static void run_one(const chaos_cfg_t & cfg, uint32_t seed, uint64_t rounds)
{
    if (!strcmp(cfg.impl, "v1"))
        run_impl<v1::blockstore_impl_t>(cfg, 2, seed, rounds);
    else
        run_impl<blockstore_impl_t>(cfg, 3, seed, rounds);
}

int main(int narg, char *args[])
{
    // Failures are reported on stderr while progress goes to stdout - keep them in order
    setvbuf(stdout, NULL, _IONBF, 0);
    const int cfg_count = sizeof(configs)/sizeof(configs[0]);
    if (narg > 1)
    {
        // The seed picks the configuration as well, so reproducing a failure is a single
        // short run. Pass a third argument to force a specific configuration instead.
        uint32_t seed = (uint32_t)strtoul(args[1], NULL, 10);
        uint64_t rounds = narg > 2 ? strtoull(args[2], NULL, 10) : 300;
        int cfg = narg > 3 ? (int)strtoul(args[3], NULL, 10) % cfg_count : (int)(seed % cfg_count);
        run_one(configs[cfg], seed, rounds);
    }
    else
    {
        // One run takes about a second. Sweep whole multiples of the configuration
        // count so that every configuration gets the same number of seeds.
        for (uint32_t seed = 1; seed <= 2*cfg_count; seed++)
            run_one(configs[seed % cfg_count], seed, 300);
    }
    printf("\nall ok\n");
    return 0;
}
