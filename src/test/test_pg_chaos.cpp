// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// Seed-driven chaos test for PG behavior: one real OSD (the primary, with a real
// blockstore on simulated disks) owning a single PG in a replicated x3 pool or an EC
// pool with 2 data + 2 parity chunks, the other PG members being FAKE peers
// implemented in the test itself.
//
// The whole point is to exercise the PG machinery - not the storage, which is assumed
// correct and is covered by test_blockstore_chaos. The fake peers store object versions
// like a disk would (fsynced ones survive their crashes, non-fsynced ones don't), answer
// every secondary op the primary sends (LIST/WRITE/READ/DELETE/SYNC/STABILIZE/ROLLBACK),
// and nothing more: the responder never inspects or judges what the OSD sends.
//
// Events: client read/write/delete/sync as a SELF client, peer crashes and recoveries,
// PG set changes (the PG "moves" onto a different OSD, recovery optional), OSD restarts
// and OSD power outages. A model of what was written is kept alongside: every version
// is recorded as the delta exactly as issued. Verification only ever reads through the
// primary OSD and asks one question: can the client-visible answer be explained as the
// merge of one consistent subset of the recorded versions, with everything durable
// present and bytes never lying about which version wrote them?
//
// The seed picks configuration and schedule; a failing run replays exactly by passing
// the seed it printed. Run with -h for the options.

#include <malloc.h>
#include <sys/wait.h>
#include <unistd.h>
#include <errno.h>
#include <algorithm>
#include <functional>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <string.h>
#include <time.h>
#include <vector>

#include "str_util.h"
#include "malloc_or_die.h"
#include "allocator.h"
#include "ringloop_mock.h"
#include "blockstore_impl.h"
#include "osd_primary.h"
#include "osd_test_fixture.h"
#include "etcd_state_client_mock.h"

#define POOL_ID 1
#define INODE 1
#define OBJ_COUNT 4
#define OBJ_SIZE (128*1024)
#define GRAN 4096
#define PG_NUM 1
// OSDs 2 and 3 start as fake replicas of the replicated pool; OSD 4 is the spare there
// and a member for the EC pool, OSD 5 is the EC spare. EC 2+2 puts the primary and three
// members into one set, so the pool can still move onto a different set
#define MAX_OSDS 5
// EC profile of the EC variant: 2 data + 2 parity chunks. One model object then spans
// exactly one parity stripe (2 blocks of OBJ_SIZE/2), so a write never crosses stripes
#define EC_DATA 2
#define EC_PARITY 2
#define MAX_STEPS 1000000
// The replicated pool makes every object cover exactly one block, and the model treats
// one model object = one (inode, stripe) pair = one block
#define OBJ_OFFSET(i) ((uint64_t)(i) * OBJ_SIZE)
// Object bitmap the OSD keeps per version: one bit per granule
#define BMP_SIZE (OBJ_SIZE/GRAN/8)

// What one step of the test does. The first group is the relative weight of each possible
// action - they're weights, not percentages, and only their ratio matters. Setting one to
// zero turns the corresponding action off entirely
struct chaos_probs_t
{
    // Submit nothing, just let the simulation run for one event
    uint32_t wait = 500;
    uint32_t write = 60, del = 6, read = 40, sync = 30;
    // Restart the OSD cleanly - everything in flight is allowed to finish first
    uint32_t restart = 1;
    // Command the running OSD to hand the PG over: the primary moves off it,
    // it releases the state key, and a fresh instance captures the PG
    uint32_t takeover = 1;
    // Pull the plug right now, whatever happens to be in flight
    uint32_t crash = 2;
    // Crash one fake peer (its unstable versions die with it), bring it back, or move
    // the PG onto a different set of OSDs through the etcd mock
    uint32_t peer_down = 3, peer_up = 3, set_change = 1;
    // Percent of fake peer crashes that drop the never-fsynced journal tail: a
    // process crash always takes RAM, this models whether the volatile disk cache
    // goes with it. The default 50 alternates the two peer kinds: one crash
    // buries the tail, the next lets the peer re-join with its un-synced copies,
    // get trusted through LIST, and only lose them on a later crash. 100 loses
    // the tail every time; fsynced copies survive every crash whatever this says
    uint32_t peer_cache_loss = 50;
    // Read the whole object instead of a random range, in percent
    uint32_t full_read = 25;
    // Chance per round to deliver an already-applied subop reply, in percent.
    // Applies land with their own request-path delay (service_peers), so the
    // two halves of a subop round trip are independent
    uint32_t p_reply = 30;
    // Chance per round to apply a delayed request-path subop (and to deliver a
    // harvested wire record), in percent
    uint32_t p_wire = 70;
    // Percent of cluster events (crash, restart, takeover, peer changes, set
    // change) after which the test waits for the PG to settle and runs its
    // full post-event verification; otherwise it keeps operating against the
    // still-settling PG. The unconditional wait serialized the timeline: ops
    // were never in the write_queue behind flush_actions when a repeer or a
    // stop began, so a forgotten write_queue cancellation was invisible.
    // 100 = always wait (the old behavior)
    uint32_t p_post = 50;
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
    ACT_TAKEOVER,
    ACT_PEER_DOWN,
    ACT_PEER_UP,
    ACT_SET_CHANGE,
    ACT_WRITE,
    ACT_DELETE,
    ACT_READ,
    ACT_SYNC,
    ACT_WAIT,
    ACT_COUNT,
};

// One point of the configuration matrix.
struct chaos_cfg_t
{
    // false: immediate_commit=all with fsyncs disabled, so the OSD answers every write
    //        as already durable and the disk has no volatile cache
    // true:  immediate_commit=none with a volatile write cache, so an acknowledged write
    //        is only durable once a completed SYNC (or an untracked autosync) committed it
    bool use_fsync;
    // Checksum block size, or 0 for no checksums at all - a rather different code path.
    // Deliberately includes sizes larger than the write granularity, where a write only
    // covers part of a checksummed block
    uint32_t csum_block;
    // Let the OSD actually recover misplaced/degraded copies (rebalance on). Off by
    // default so a set change leaves real recovery for dedicated seeds to exercise
    bool recovery = false;
    // true: EC pool with 2 data + 2 parity chunks (4 members, blocks of OBJ_SIZE/2);
    // false: replicated x3 with one full-size block per object
    bool ec = false;
};

// What we believe about one object.
//
// `submitted` is the highest sequence number we ever enqueued a write for - not
// necessarily acknowledged, because a write may have been in flight when the power went
// away. `acked` is the highest version whose write or delete completed successfully; the
// OSD assigns real version numbers itself, so the model works with its own sequence
// numbers that only ever grow.
// `durable` (== `synced` at this level) is the highest sequence number we know must
// survive: a SYNC whose reply completed covered it. With immediate_commit=all every
// acknowledged write is durable at once.
// Versions restart after a deletion - the object is gone and is created anew, so the
// bitmap and the "previous content" start empty.
// One issued version: what the client wrote, exactly. len == 0 spells a deletion.
// Deltas are cheap enough to never forget a version: a refused or interrupted
// write, or a copy that outlived a deletion, can surface at any later time and
// must stay explainable
// One fact per version, observed against the reads and events of the session.
// This is the model's entire memory for a version: everything else (the
// highest acked, the last delete, the presence floor...) is a scan over these
// states, never a stored copy.
//  SUBMITTED - issued, no terminal answer yet (in flight when the session
//              died): the reply makes it ACKED (ack) or OPTIONAL (refusal)
//  ACKED     - acknowledged (or its bytes established by observation in
//              immediate_commit=all): the OSD merged read answers with the
//              newest version any chain has, so an acked version must show
//              in every read that reaches the copies holding it, no matter
//              who joined or left since - an older stale chain can only
//              lose the merge
//  SYNCED    - a SYNC fsynced it: durable forever; conclusive absence from a
//              read that had quorum is committed data lost
//  PARTIAL   - deletions only: acked, but a copy the deletion couldn't reach
//              may have survived it. The world from before the deletion is a
//              legal answer until a repeated deletion comes back clean
//  OPTIONAL  - no standing ack: never acked (refused, interrupted), or a
//              crash or repeer rollback broke the ack's promise. Its bytes
//              may already sit on some chains and may still show, or be gone
//              for good; conclusive absence over its range is DEAD
//  DEAD      - its absence was fully observed over its whole range: it may
//              never show again (a rolled-back fragment recommitted)
enum ver_state_t { V_SUBMITTED, V_ACKED, V_SYNCED, V_PARTIAL, V_OPTIONAL, V_DEAD };

struct delta_t
{
    uint64_t off = 0, len = 0;
    std::vector<uint8_t> bytes;
    // The whole truth about this version (ver_state_t). Deletions carry
    // OPTIONAL/ACKED/SYNCED/PARTIAL
    ver_state_t state = V_SUBMITTED;
    // The live member set this version was acknowledged against: where its
    // fragments actually landed. The event gates walk each object's chain
    // top-down and defend every still-servable version from this record, so
    // a rollback can't strand a placement claim on a version the disk lost
    std::vector<osd_num_t> ack_live;
};

struct obj_model_t
{
    // Every version ever submitted for this object, forever, with its state.
    // This map is the model's entire memory: the watermarks below are scans
    // over it, recomputed on demand (O(n) is fine here; index it later if a
    // check ever gets hot)
    std::map<uint64_t, delta_t> vers;
    // Highest submitted version (0 = none)
    uint64_t last_submitted() const
    {
        return vers.empty() ? 0 : vers.rbegin()->first;
    }
    // Highest version whose ack arrived and still stands (PARTIAL: an acked
    // deletion with survivors); lower ones may be OPTIONAL (never acked or
    // demoted) - their bytes only partially landed and showing them is a
    // legal race. "v <= last_acked()" is how the checks say "at or below the
    // acknowledged chain"
    uint64_t last_acked() const
    {
        for (auto it = vers.rbegin(); it != vers.rend(); ++it)
            if (it->second.state != V_OPTIONAL && it->second.state != V_DEAD &&
                it->second.state != V_SUBMITTED)
                return it->first;
        return 0;
    }
    // Highest acknowledged deletion: everything below it is gone, so a sync
    // completing with an older snapshot or an absence verdict is meaningless
    uint64_t last_delete() const
    {
        for (auto it = vers.rbegin(); it != vers.rend(); ++it)
            // last acknowledged (or still-owed, see redel_pending) delete;
            // an in-flight one is not a floor yet
            if (it->second.state != V_DEAD && it->second.state != V_SUBMITTED &&
                it->second.len == 0)
                return it->first;
        return 0;
    }
    // The presence floor: the highest committed whole-object write. Every
    // granule of every serving chain must show through it
    uint64_t floor_now() const
    {
        for (auto it = vers.rbegin(); it != vers.rend(); ++it)
            if (it->second.state == V_SYNCED && it->second.off == 0 &&
                it->second.len == OBJ_SIZE)
                return it->first;
        return 0;
    }
    // Highest deletion pinned as partially applied (0 = none): its surviving
    // copy may keep serving the world from before it
    uint64_t partial_del() const
    {
        for (auto it = vers.rbegin(); it != vers.rend(); ++it)
            if (it->second.len == 0 && it->second.state == V_PARTIAL)
                return it->first;
        return 0;
    }
    // The newest deletion whose effect every chain is known to have: nothing
    // beneath it may be demanded
    uint64_t del_floor_now() const
    {
        uint64_t d = last_delete();
        return d > partial_del() ? d : partial_del();
    }
    // The version number the OSD last reported for this object - diagnostics only
    uint64_t osd_version = 0;
    // Cache for the merged acknowledged content, recomputed on demand
    std::vector<uint8_t> full_cache;
    std::vector<uint8_t> bmp_cache;
    uint64_t full_cache_ver = (uint64_t)-1;
};

struct pending_t
{
    uint64_t seq_id = 0;
    osd_op_t *op = NULL;
    bool done = false;
    int obj = -1;
    // Objects this operation refers to. While it's in flight no other operation may touch
    // them, which is what keeps the model unambiguous: the base content of a write and the
    // expected result of a read can't change under it
    std::vector<int> touched;
    // Sequence number in the model
    uint64_t version = 0;
    // Model update to run if - and only if - the operation completed successfully
    std::function<void()> on_success;
};

// One stored version on a fake peer. `fsynced` means the copy went through a
// journal sync (or was written with the journal durable right away, which is
// what immediate_commit=all does), so it survives a crash; `stable` means it
// was written stable or stabilized, so peering won't roll it back. A real
// secondary loses only what was never fsynced - an unstable but synced copy
// stays on its disk through a crash
struct peer_ver_t
{
    std::vector<uint8_t> data;
    std::vector<uint8_t> bmp;
    // The bitmap attribute as it arrived on the wire. The blockstore keeps it apart
    // from its own internal bits and answers read attr / read_bitmap with it - EC
    // parity blocks carry the RS-encoded data bitmaps, which the primary decodes
    std::vector<uint8_t> ext;
    bool stable = false;
    bool fsynced = false;
};

struct chaos_t;

// A fake secondary OSD: a versioned object store, an on/off switch and the
// responder that applies every secondary op the way a correct one would; the
// reply travels back on its own delay
struct fake_peer_t
{
    osd_num_t num = 0;
    // true = the messenger has a connection and the responder replies
    bool up = false;
    // object stripe (= our object index for a replicated pool) -> version -> copy
    std::map<uint64_t, std::map<uint64_t, peer_ver_t>> store;
    // Subops applied here whose replies are still in flight to the primary.
    // A reply belongs to the connection it was produced on: when the
    // connection dies the reply is lost with it - the applied-but-not-yet-
    // acked world. see fake_cluster_t::service_peers()
    std::vector<osd_op_t*> replying;
    // The simulation: geometry (blk, blk_bmp), config (use_fsync) and tracing.
    // Set at construction; the peer answers subops with it
    chaos_t *ch = NULL;

    // A process crash: the RAM and the unflushed journal go away, so every
    // version that was never fsynced vanishes and synced ones survive
    // Returns true if the never-fsynced journal tail was lost with it
    bool crash();
    uint64_t latest(uint64_t stripe);
    // The blockstore's version resolution: 0 = newest STABLE, UINT64_MAX = newest
    // of any kind, anything else = newest <= v
    uint64_t lookup(uint64_t stripe, uint64_t version);
    // Apply one queued subop the way a correct secondary would; the reply is
    // filled in but delivered later, see fake_cluster_t::service_peers()
    void apply(osd_op_t *op);
    // The chain merge a blockstore read would answer for (stripe, have)
    std::vector<uint8_t> merge_chain(uint64_t stripe, uint64_t have,
        std::vector<uint8_t> *bmp_out);
};

// Everything the model believes about the objects: the geometry they live in, the
// per-version state bookkeeping and the pure checks derived from it
// and the pure checks that need only those. chaos_t drives it
struct chaos_model_t
{
    obj_model_t objs[OBJ_COUNT];

    bool use_fsync = false;

    // PG geometry from the configuration. For EC the primary's blockstore blocks are
    // half-object: an object is one parity stripe of EC_DATA data blocks + EC_PARITY
    // parity blocks, each block held by exactly one set member (its role). For the
    // replicated pool every member holds the whole object and blk stays OBJ_SIZE
    bool is_ec = false;

    uint64_t pg_size = 3, pg_minsize = 2, parity = 0;

    uint64_t blk = OBJ_SIZE;

    // Bitmap of one block (per role), vs BMP_SIZE for a whole object
    uint64_t blk_bmp = BMP_SIZE;

    // Objects whose post-outage verification couldn't run because the PG came back
    // without quorum; retried once some peer returns
    std::set<int> recover_pending;

    // True between a power outage and the recovery reads that reconcile it; SYNC
    // completions may not mark versions SYNCED while it's set (see synced())
    bool in_crisis = false;


    // True only in the quiescent final phase of run(): the resurrection tolerance is
    // turned off there, so a genuinely diverged object still trips a hard failure
    bool final_checks = false;

    bool exists(int obj);
    // Self-describing payload: every record says which object, which version and which
    // offset wrote it, so a mismatch tells exactly where the stale data came from. Its
    // size divides the write granularity, so records never straddle a write or a read.
    struct payload_t
    {
        uint64_t seq;      // the model's own sequence number
        uint64_t pad0;       // keeps the record at 32 bytes, dividing the granularity
        uint64_t obj;
        uint64_t offset;   // byte offset of this record within the object
    };

    static void fill_range(std::vector<uint8_t> & c, int obj, uint64_t seq,
        uint64_t offset, uint64_t len);
    const std::vector<uint8_t> & base_content(int obj);
    // The object's acknowledged content is a deletion: the highest version the
    // model acknowledged covers the object with absence
    bool deleted(int obj);
    bool unfinished_delete(int obj);
    // Does the client still owe this object a clean complete delete: the
    // newest delete above the last clean one is pinned as partially applied
    // (V_PARTIAL) or submitted and never acked. Derived from the version
    // list - no extra bookkeeping
    bool redel_pending(int obj);
    // While a deletion window is open the chain may legitimately serve the
    // world from before the deletion
    bool delete_window(int obj);
    // Pin the newest acknowledged deletion as partially applied
    void mark_partial(int obj);
    // A clean delete ack or a read that really showed the object gone: every
    // copy is proven deleted, the surviving-copy worlds are over
    void close_partials(int obj);
    // A write or a deletion of `obj` completed. With immediate_commit=all that's already
    // durable; with a volatile cache a sync still has to commit it.
    void written(int obj);
    // A sync covering `covered` completed. An OSD sync fsyncs the journal *and*
    // stabilizes everything it snapshoted, so synced is durable right away.
    // A power outage leaves the model's pre-crash acks unverified: a SYNC on the fresh
    // OSD session covers nothing of them (its dirty sets start empty), so it must not
    // promote durable either. Promotions stay frozen until the recovery reads have
    // reconciled every object; acked-unsynced writes that the outage swallowed then
    // correctly stop being part of any future SYNC snapshot
    void synced(const std::vector<uint64_t> & covered);
    // Mark every recorded version <= n that's ACKED as SYNCED (durable)
    void commit_upto(int obj, uint64_t n);
    // A granule that carries data in the returned content must be marked as written in
    // the OSD's bitmap: data can't come out of a hole. The reverse is legal - a set bit
    // over zero content is fine, because a set change materializes holes as written
    // zeros in a merged version, and rolling that version back afterwards legitimately
    // turns the bits back into holes. Expectation-side bitmap tracking can't tell the
    // two chain states apart (they carry equal content), so the check is content-based
    void check_bitmap(int obj, const std::vector<uint8_t> & content, uint64_t base,
        const std::vector<uint8_t> & bmp, const char *what);
    // Print everything the model believes about an object, for failure reports
    void dump_model(int obj);
    // A bitmap makes the report skip granules the OSD marks holes: bytes behind
    // them are physical leftovers of whatever owned the data block before
    void report_mismatch(int obj, const std::vector<uint8_t> & got, const std::vector<uint8_t> & want,
        uint64_t base = 0, const std::vector<uint8_t> *bmp = NULL);
    // Can a read of [offset, offset+len) returning `got` be explained as the merge
    // of one consistent subset of the recorded versions? Every version up to
    // `durable` must be in the subset (it was committed); every higher one may or
    // may not be. A record claims its own version by its sequence number, so the
    // bytes of each covered granule must equal the delta exactly as issued, and
    // the winner per granule must be the highest version of the subset that
    // covers it - a lower one showing through a higher applied one, a durable
    // version missing behind a hole, or bytes no version ever wrote all fail.
    // When a deletion is pinned as partially applied (V_PARTIAL), a world
    // without it (the copy it
    // missed still serving) is legal too, so it's tried both ways. With
    // allow_partial (EC) an optional winner missing from a granule is also
    // legal - per-role chains can apply an optional version only in part.
    // Returns -1 (no world), 0 (exactly the durable state), 1 (world used
    // optional versions); *max_seen gets the highest version seen applied
    int world_check(int obj, const std::vector<uint8_t> & got, uint64_t offset,
        uint64_t len, const char *what, bool allow_partial = false,
        uint64_t *max_seen = NULL);
};

// Bytes on the wire: a subop still queued when its sending instance died.
// The kernel delivers the bytes after the sender is gone, so a peer may
// apply the request long after the connection died - but nothing can answer
// it: the callback belonged to the dead instance. op is a detached osd_op_t
// owning its own copies of the request, the payload and the attr bitmap
struct wire_op_t
{
    osd_num_t dst;
    osd_op_t *op;
    void *payload;
    uint8_t *attr;
};

// The fleet of fake secondary OSDs: who exists, who has a live messenger
// connection and which queued op gets answered on this round. The answering
// itself is the peers' own behavior, see fake_peer_t::answer(); ch gives the
// fleet access to the simulation and the messenger
struct fake_cluster_t
{
    chaos_t & ch;
    explicit fake_cluster_t(chaos_t & owner) : ch(owner) {}
    // Fake replicas including the spare, by OSD number. Survive OSD restarts -
    // an OSD crash doesn't touch the peers
    std::map<osd_num_t, fake_peer_t> peers;
    // Peers with a live messenger connection to the current osd_t instance. Only the
    // fixture may touch osd_t::msgr, so we remember which ones it connected
    std::set<osd_num_t> connected;
    // Subops that were on the wire when their instance died; they land on
    // the peer they were addressed to in the peer's own time, without reply
    std::vector<wire_op_t> wire;
    uint64_t wire_harvested = 0, wire_delivered = 0, wire_lost = 0;
    // Copy out mutating subops queued on live connections; called by
    // chaos_t::destroy() just before the instance disappears
    void harvest_wire();
    // Apply a random subset of the queued ops of every up fake peer and deliver
    // a random subset of its in-flight replies, reconnecting peers the OSD
    // dropped itself; see fake_peer_t::apply()
    void service_peers();
    osd_client_t *peer_cl(osd_num_t num);
    // EC puts the member's role in the low bits of the blockstore stripe
    // (oid.stripe = parity-stripe start | role), and an object is exactly one stripe
    int obj_of(uint64_t stripe);
    uint64_t role_of(uint64_t stripe);
};

// The driver: the real OSD and its disks, the "monitor" through the etcd mock,
// PG events, the step scheduler and in-flight op tracking. The object model is
// inherited, the fake peers live in .cluster
struct chaos_t : chaos_model_t
{
    chaos_t();
    // Fake secondary OSDs with their stores and connections
    fake_cluster_t cluster;
    json11::Json::object osd_config;

    // The fixture is a friend of osd_t; we use it for exec()/pg()/has_pg() and replace
    // its mock instances with the ones driven by the simulator
    osd_test_fixture_t f;

    io_sim_t *sim = NULL;

    disk_mock_t *data_disk = NULL;

    blockstore_impl_t *bs = NULL;

    uint64_t crashes = 0, restarts = 0, takeovers = 0, writes = 0, deletes = 0, syncs = 0;
    // How many peer crashes kept their never-fsynced journal tail (only drawn
    // when --peer-cache-loss < 100). A run where nobody ever keeps the journal
    // tail would report "ok" while the interesting scenario never happened
    uint64_t peer_cache_kept = 0;

    uint64_t checked_reads = 0, step_no = 0;

    uint64_t seq_id = 0, op_id = 0;

    bool trace = false;

    // Membership of the single PG as currently set in etcd. cur_set[0] is the primary
    // and is always the real OSD 1
    std::vector<osd_num_t> cur_set = { 1, 2, 3 };

    std::vector<osd_num_t> live_set();
    // Members of `grp` that are live members of `pool` (OSD 1 is always live)
    uint32_t live_overlap(const std::vector<osd_num_t> &grp, const std::vector<osd_num_t> &pool);
    // Would `pool` of live PG members still keep every object's newest write
    // reconstructible?
    bool quorum_kept(const std::vector<osd_num_t> &pool);
    uint64_t peer_downs = 0, peer_ups = 0, set_changes = 0, refused_ops = 0;

    // Operations submitted but not yet accounted for
    std::vector<pending_t*> inflight;

    // Number of in-flight operations referring to each object
    int busy[OBJ_COUNT] = {};

    void configure(const chaos_cfg_t & cfg);
    void create(uint32_t seed);
    bool cfg_buffered();
    // Cluster-side abort diagnostics: the set with down markers and the peer
    // copies of the object (diagnostics only - verification never reads them)
    void dump_peers(int obj);
    // Fold a conclusive whole-object observation into the per-version states
    void observe(int obj, const std::vector<uint8_t> & got, uint64_t max_seen);
    // Every PG has finished peering and flush work - possibly degraded or even
    // incomplete (a restart onto dead peers), which the event loop resolves later.
    // Nothing may be in flight, including the rollback and stabilize batch a
    // freshly peered PG may still be writing
    bool pgs_quiescent();
    // The same, but client operations may still be in flight: what an event (peer
    // crash, set change) has to leave behind is a settled PG that finished its
    // rollback / recovery / stabilize work
    bool pg_settled();
    // The PG can serve reads and writes: active, not peering, no pending flush work
    bool pg_healthy();
    // The PG takes operations right now: active, no peering - possibly with
    // flush work still queued (a repeated delete doesn't need settled chains)
    bool pg_active();
    // The OSD may still need a peer that's gone from the current set: PG peering
    // refuses to start while any member of a historical set with committed data is
    // unreachable, and it legitimately connects to such peers
    bool pg_needs_osd(osd_num_t num);
    // The mock must never know pg/history revisions older than the ones the OSD
    // read at load time. PG state report txns compare pg/history's mod_revision
    // against them, and once the OSD itself writes history that compare would
    // fail forever. A mock with an up-to-date watch is what the OSD talks to in
    // production, so advance the revisions to the mock's current one before
    // every simulated step
    void sync_etcd_revisions();
    // run_until with the fake peers serviced between simulator steps, so a subop
    // always finds somebody to answer it
    bool run_until(std::function<bool()> cond, int max_steps = MAX_STEPS);
    // The monitor's role: the set being replaced stays in the PG history until the OSD
    // cleans it on active+clean. Peering then still accounts for copies left on members
    // which the set change shuffled out: LIST reaches them, so later deletes cover them,
    // and a delete that can't reach them reports left_on_dead
    void write_pg_history(const std::vector<osd_num_t> & old_set);
    // Let repeering, rollback and recovery finish after an event
    void settle();
    // settle() only with probability p_post; see the definition
    void maybe_settle();
    // The model client: a registered msgr session (client_id != 0), fake but real.
    // dirty_pgs registration, drop_dirty_pg_connections and the sync-entry checks
    // work against it unmodified; when a repeer kills the session, the model sees
    // exactly what a real client sees: refused in-flight ops and a broken
    // session guarantee
    uint64_t client_id = 0;
    void connect_session();
    void drain_replies();
    void session_died();
    void check_session_death();
    // Crash one live peer: its unstable versions die with it, in-flight subops die
    // with -EPIPE and the PG repeers without it
    void do_peer_down();
    void do_peer_up();
    // An event proved a chain exists that fresh acks never reached (a member
    // invited or returning with an unknown chain, a copy a delete couldn't
    // reach, a read served from below the floor): every state carrying a
    // "all chains have it" promise falls back - ACKED can't claim every chain,
    // SYNCED can't be demanded of the chain that missed it
    // A repeer rolls back versions the OSD still held unstable (volatile cache,
    // or acked on a degraded PG). Their promise breaks like in a power loss:
    // demote them to OPTIONAL for the model; committed (SYNCED) data survives
    void demote_unstable();
    // Copies a clean delete left physically behind on a peer the primary's
    // peering couldn't see - the OSD's INCOMPLETE verdicts are about them
    // True when every member the PG remembers (current, previous or historical
    // sets) is in the current set - nobody holds copies the ops can't reach
    bool in_set(osd_num_t n);
    // Publish the current membership through the etcd mock; the OSD notices it in
    // apply_pg_config and repeers the PG
    void write_pg_config(osd_num_t primary = 1);
    // Move the PG onto a different OSD set. Only live OSDs join, the primary stays 1,
    // and the new set always keeps one member of the old one so that peering can't
    // legitimately lose the quorum
    void do_set_change();
    // (Re)create the OSD and its blockstore on top of the disks, as if the process had
    // been restarted. Peering reads the real blockstore and the fake peers' lists and
    // rolls unstable versions back.
    void start_osd();
    void destroy();
    ~chaos_t();
    uint64_t rnd(uint64_t min, uint64_t max);
    // Draw unconditionally so that the sequence doesn't depend on the probability itself
    bool chance(uint32_t percent);
    // Hand an operation to the OSD
    void enqueue(pending_t *p);
    pending_t *submit(pending_t *p);
    void free_op(pending_t *p);
    void reap_op(osd_op_t *op);
    // Build an op for the model client's session; the reply is delivered through
    // op->callback when the messenger's buffering is drained
    osd_op_t *make_rw_op(uint64_t opcode, uint64_t offset, uint64_t len);
    pending_t *submit_write(int obj);
    // Deletions carry no data. The model records them as an empty content vector; like a
    // write, they're only durable once a sync committed them.
    pending_t *submit_delete(int obj);
    pending_t *submit_sync();
    chaos_action_t pick_action();
    void step();
    // Fold every completed operation into the model and free it
    void reap();
    // Only one operation at a time may refer to an object: that way the base content of a
    // write and the expected result of a read can't change while they're in flight
    std::vector<int> free_objects(bool must_exist);
    // Deleting a non-existent object isn't expressible at OSD level as a no-op we can
    // model cleanly, so only delete what is actually there
    bool try_write(bool del);
    // Read a random part of a random object. Ranges are aligned to the write granularity
    // but not to the checksum block size, so they cut across checksum blocks and across
    // the boundaries of the extents that were actually written.
    bool try_read();
    // Pull the plug right now. Whatever completed before the outage definitely happened;
    // whatever was still in flight may or may not have, so its content is only recorded as
    // a legal outcome. Every object then has to come back holding at least its durable
    // version - or something newer that was actually written and autosynced.
    void do_crash();
    void forget_inflight(bool reap_unfinished = false);
    // Wait for everything in flight to complete and fold it all into the model
    void drain();
    void report_deadlock();
    // Fold one finished - or interrupted - operation into the model
    void account(pending_t *p);
    static const char *op_name(pending_t *p);
    // Read the whole object synchronously and return its content, or empty if it doesn't
    // exist. Used by the recovery checks, where nothing else is in flight anyway
    uint64_t last_read_version = 0;

    std::vector<uint8_t> last_read_bitmap;

    std::vector<uint8_t> read_object(int obj, int *retval);
    // No other operation may touch the object while a read of it is in flight, so a read
    // of the newest version must return exactly what the last acknowledged write put
    // there - stable or not
    void check_read_result(pending_t *p);
    // Synchronous whole-object check, for the very end of the run
    void check_object(int obj);
    // Restart without any power loss at all: everything in flight is allowed to finish
    // first, so nothing may be lost. With immediate_commit=all every acknowledged write
    // is already stable, so the OSD must come back holding exactly what it put down.
    // With immediate_commit=none peering rolls every not-yet-synced version back even
    // though nothing was actually lost, so only the synced ones are guaranteed there.
    void do_restart();
    void do_takeover();
    // Verify that an object came back from a restart with an answer explainable
    // as the merge of one consistent subset of the recorded versions: forced
    // (durable, not crash-demoted) versions must show their bytes, optional ones
    // either show them or not, deletions refused or in flight may or may not
    // have applied. The model keeps its own chain as the base for
    // following writes - each of those is pinned against reality as issued
    bool check_recovered(int obj, const char *what);
    // Run the deferred post-outage checks for the objects whose PG now has quorum.
    // Objects whose PG is still incomplete simply stay on the pending list
    void flush_recover_pending();
    // Commit everything the OSD still holds dirty so that a recovery read sees
    // the settled state instead of a half-flushed PG
    void force_sync();
    void run(uint64_t steps);
};

// fake_peer_t: version store mechanics

bool fake_peer_t::crash()
{
    up = false;
    // A process crash loses RAM. Whether the never-fsynced journal tail also
    // dies with it is a disk-cache property, not a process one - let the test
    // choose (see chaos_probs_t::peer_cache_loss). The journal keeps every
    // fsynced version, stable or not: stability is a commit flag inside the same
    // journal, so losing it would mean losing what fsync promised
    const bool journal_lost = probs.peer_cache_loss >= 100 ||
        ch->chance(probs.peer_cache_loss);
    if (!journal_lost)
        ch->peer_cache_kept++;
    if (!journal_lost)
        return false;
    for (auto o = store.begin(); o != store.end(); )
    {
        for (auto it = o->second.begin(); it != o->second.end(); )
        {
            if (!it->second.fsynced)
                it = o->second.erase(it);
            else
                ++it;
        }
        // A stripe with no versions left is no stripe at all - leaving an empty
        // map behind would make every rbegin() on it undefined
        if (o->second.empty())
            o = store.erase(o);
        else
            ++o;
    }
    return true;
}

uint64_t fake_peer_t::latest(uint64_t stripe)
{
    auto it = store.find(stripe);
    return it == store.end() || it->second.empty() ? 0 : it->second.rbegin()->first;
}

uint64_t fake_peer_t::lookup(uint64_t stripe, uint64_t version)
{
    auto it = store.find(stripe);
    if (it == store.end())
        return 0;
    for (auto vit = it->second.rbegin(); vit != it->second.rend(); ++vit)
    {
        if (version == UINT64_MAX)
            return vit->first;
        else if (!version)
        {
            if (vit->second.stable)
                return vit->first;
        }
        else if (vit->first <= version)
            return vit->first;
    }
    return 0;
}

// The bytes a blockstore would answer for (stripe, have): every version is a
// delta, so each granule comes from the newest version <= have covering it,
// and the effective bitmap is the union of theirs. In EC a peer's item is one
// block (blk), not the whole object; the role bits in the stripe already pick
// the right store entry
std::vector<uint8_t> fake_peer_t::merge_chain(uint64_t stripe, uint64_t have,
    std::vector<uint8_t> *bmp_out)
{
    std::vector<uint8_t> merged(ch->blk, 0);
    if (bmp_out)
        bmp_out->assign(ch->blk_bmp, 0);
    for (auto & vv : store[stripe])
    {
        if (vv.first > have)
            break;
        if (bmp_out)
        {
            // The answer's attr bitmap belongs to the version the item's data
            // comes from for each granule (blockstore_read.cpp answers the
            // merged read with the ext bitmap of the newest covering version),
            // and that ext bitmap always contains the granules the version
            // physically carries: the primary computes it as old-merged | new
            // bits and never drops a bit a chain member covers. The merged
            // bitmap is therefore the union of the covering versions' own bits
            for (uint64_t g = 0; g < ch->blk/GRAN; g++)
            {
                if (vv.second.bmp[g/8] & (1 << (g%8)))
                    (*bmp_out)[g/8] |= 1 << (g%8);
            }
        }
        for (uint64_t g = 0; g < ch->blk/GRAN; g++)
        {
            if (vv.second.bmp[g/8] & (1 << (g%8)))
                memcpy(merged.data() + g*GRAN, vv.second.data.data() + g*GRAN, GRAN);
        }
    }
    return merged;
}

// chaos_model_t: pure bookkeeping over the model state

bool chaos_model_t::deleted(int obj)
{
    auto & m = objs[obj];
    uint64_t la = m.last_acked();
    return la && m.vers.at(la).len == 0;
}

// Does the client still owe this object a clean complete delete? Derived
// from the version list: above the newest clean delete (acknowledged with
// the whole set and never rolled back, or sealed by SYNC) stands a delete
// pinned as partially applied (V_PARTIAL: a copy on a member the deletion
// couldn't reach survived it - the only way a copy can outlive a deletion),
// submitted and never acked (in flight, refused, or a crash orphan - its
// subops may have applied on any subset), or demoted to OPTIONAL by a
// journal-loss rollback that ate its un-fsynced entry on a holder. A later
// clean delete closes the window: every older refusal is history
bool chaos_model_t::redel_pending(int obj)
{
    auto & m = objs[obj];
    uint64_t clean = 0;
    for (auto & kv: m.vers)
        if (kv.second.len == 0 &&
            (kv.second.state == V_ACKED || kv.second.state == V_SYNCED))
            clean = kv.first;
    for (auto & kv: m.vers)
        if (kv.first > clean && kv.second.len == 0 &&
            (kv.second.state == V_PARTIAL || kv.second.state == V_OPTIONAL ||
             kv.second.state == V_SUBMITTED))
            return true;
    return false;
}

// An open deletion window: a clean deletion whose copies may dodge it
// (deleted), or a delete still owing its clean ack (redel_pending). While
// the window is open the chain may legitimately serve the world from before
// the deletion
bool chaos_model_t::delete_window(int obj)
{
    return deleted(obj) || redel_pending(obj);
}

// The only legitimate cause of -EIO: the OSD deems the object INCOMPLETE.
// Only an EC object can legitimately get there in this test: a role the
// delete couldn't reach keeps old data and resurrects it as an incomplete
// item. A replicated one must never read incomplete - if it does, it's an
// OSD bug, not something the model should excuse. The same predicate stands
// at all three places an -EIO can arrive (account, check_read_result,
// check_recovered)
bool chaos_model_t::unfinished_delete(int obj)
{
    return is_ec && delete_window(obj);
}

void chaos_model_t::mark_partial(int obj)
{
    uint64_t d = objs[obj].last_delete();
    if (d)
        objs[obj].vers[d].state = V_PARTIAL;
}

void chaos_model_t::close_partials(int obj)
{
    for (auto & kv: objs[obj].vers)
        if (kv.second.state == V_PARTIAL)
            kv.second.state = V_ACKED;
}

bool chaos_model_t::exists(int obj)
{
    auto & m = objs[obj];
    uint64_t la = m.last_acked();
    return la && m.vers.at(la).len > 0;
}

void chaos_model_t::fill_range(std::vector<uint8_t> & c, int obj, uint64_t seq,
    uint64_t offset, uint64_t len)
{
    // c covers just the range [offset, offset+len); the payload still carries
    // the absolute object offset
    for (uint64_t i = 0; i < len; i += sizeof(payload_t))
    {
        *(payload_t*)(c.data() + i) = (payload_t){
            .seq = seq,
            .pad0 = 0,
            .obj = (uint64_t)obj,
            .offset = offset + i,
        };
    }
}

// Merge the model's own acknowledged chain: versions up to `acked` applied in
// order, a deletion covering the whole object. The fold depends on `acked`
// and on which versions below it still count, so the cache is reset whenever
// either moves (see the demote and promotion paths)
const std::vector<uint8_t> & chaos_model_t::base_content(int obj)
{
    auto & m = objs[obj];
    uint64_t la = m.last_acked();
    if (m.full_cache_ver == la)
        return m.full_cache;
    // Folding is sequential: the fold of versions <= la is the cached fold of
    // versions <= full_cache_ver with the window (full_cache_ver, la] applied
    // on top. Below the watermark two changes would matter: a demotion
    // (ACKED -> OPTIONAL) also moves la back, which cold-starts the fold in
    // the branch below, and an observation promotion (OPTIONAL -> ACKED)
    // doesn't move la - its sites in observe() reset full_cache_ver
    if (m.full_cache_ver > la)
    {
        // Cold start
        m.full_cache.assign(OBJ_SIZE, 0);
        m.bmp_cache.assign(BMP_SIZE, 0);
        m.full_cache_ver = 0;
    }
    for (auto it = m.vers.lower_bound(m.full_cache_ver + 1);
         it != m.vers.end() && it->first <= la; it++)
    {
        // An OPTIONAL version (never acked or demoted) isn't part of the
        // acknowledged chain - skipping it must not reset the cached fold the
        // way an acked delete does
        if (it->second.state == V_OPTIONAL || it->second.state == V_SUBMITTED)
            continue;
        if (it->second.len == 0)
        {
            m.full_cache.assign(OBJ_SIZE, 0);
            m.bmp_cache.assign(BMP_SIZE, 0);
            continue;
        }
        memcpy(m.full_cache.data() + it->second.off, it->second.bytes.data(), it->second.len);
        bitmap_set(m.bmp_cache.data(), it->second.off, it->second.len, GRAN);
    }
    m.full_cache_ver = la;
    return m.full_cache;
}

// A SYNC (or, with immediate_commit=all, the ack itself) committed every
// recorded version up to n: the ACKED ones become durable (SYNCED). SYNC
// doesn't replicate - it only fsyncs what each member already holds
void chaos_model_t::commit_upto(int obj, uint64_t n)
{
    for (auto & kv: objs[obj].vers)
    {
        if (kv.first > n)
            break;
        if (kv.second.state == V_ACKED)
            kv.second.state = V_SYNCED;
    }
}

void chaos_model_t::written(int obj)
{
    if (use_fsync)
        // The disk has a volatile cache: nothing is guaranteed until a sync
        return;
    commit_upto(obj, objs[obj].last_acked());
}

void chaos_model_t::synced(const std::vector<uint64_t> & covered)
{
    for (int obj = 0; obj < OBJ_COUNT; obj++)
    {
        auto & m = objs[obj];
        // A deletion may have wiped out part of what the sync covered while it
        // was in flight, and then the versions below it don't exist any more
        if (in_crisis || recover_pending.count(obj) ||
            !covered[obj] || covered[obj] > m.last_acked() ||
            covered[obj] < m.last_delete() || !m.vers.count(covered[obj]))
            continue;
        commit_upto(obj, covered[obj]);
    }
}

void chaos_model_t::check_bitmap(int obj, const std::vector<uint8_t> & content, uint64_t base,
    const std::vector<uint8_t> & bmp, const char *what)
{
    // On EC the OSD answers data and bitmap by RS-decoding them independently
    // from whatever per-role versions the surviving sources held (osd_rmw.cpp
    // reconstruct): the per-version relation "data implies the bit" only holds
    // inside one version's chain, so a granule carrying reconstructed data can
    // come back under a decoded bitmap that marks it a hole, with nothing
    // actually wrong - the soak seeds showed exactly that on an active PG,
    // model and OSD bitmaps agreeing bit for bit. So the bitmap cross-check is
    // skipped on EC; what still guards the data there is the self-describing
    // invariant (every 32-byte record names its object, its offset and a
    // sequence number we handed out) and durability by the candidate path, so
    // no corruption can hide behind the skip
    if (is_ec)
        return;
    for (uint64_t pos = base; pos + GRAN <= base + content.size(); pos += GRAN)
    {
        const uint8_t *g = content.data() + (pos - base);
        bool nonzero = false;
        for (uint64_t i = 0; i < GRAN; i++)
        {
            if (g[i])
            {
                nonzero = true;
                break;
            }
        }
        if (!nonzero)
            continue;
        uint64_t gran = pos / GRAN;
        if (gran/8 >= bmp.size() || !(bmp[gran/8] & (1 << (gran%8))))
        {
            fprintf(stderr, "object %d %s: granule %ju (offset %ju) carries data "
                "but the OSD bitmap marks it a hole\n", obj, what, gran, pos);
            fprintf(stderr, "  got bmp");
            for (uint64_t j = 0; j < bmp.size(); j++)
                fprintf(stderr, " %02x", bmp[j]);
            fprintf(stderr, "\n  model bmp");
            {
                auto & mm = objs[obj];
                base_content(obj);
                for (uint64_t j = 0; j < mm.bmp_cache.size(); j++)
                    fprintf(stderr, " %02x", mm.bmp_cache[j]);
            }
            fprintf(stderr, "\n");
            dump_model(obj);
            abort();
        }
    }
}

void chaos_model_t::dump_model(int obj)
{
    auto & m = objs[obj];
    fprintf(stderr, "model obj%d: submitted=%ju acked=%ju deleted_upto=%ju "
        "floor=%ju del_floor=%ju osd_version=%ju partial=%ju\n",
        obj, m.last_submitted(), m.last_acked(), m.last_delete(),
        m.floor_now(), m.del_floor_now(), m.osd_version, m.partial_del());
    fprintf(stderr, "  recorded versions:");
    for (auto & v: m.vers)
    {
        static const char *sn[] = {"(sub)", "", "", "(par)", "(opt)", "(dead)"};
        fprintf(stderr, " %ju%s%s", v.first,
            v.second.len == 0 ? "(del)" : "", sn[v.second.state]);
    }
    fprintf(stderr, "\n");
}

void chaos_model_t::report_mismatch(int obj, const std::vector<uint8_t> & got, const std::vector<uint8_t> & want,
    uint64_t base, const std::vector<uint8_t> *bmp)
{
    if (got.size() != want.size())
        return;
    for (uint64_t i = 0; i < got.size(); i += sizeof(payload_t))
    {
        if (bmp)
        {
            uint64_t g = (base + i) / GRAN;
            if (g/8 >= bmp->size() || !((*bmp)[g/8] & (1 << (g%8))))
                continue;
        }
        auto & g = *(payload_t*)(got.data()+i);
        auto & w = *(payload_t*)(want.data()+i);
        if (memcmp(&g, &w, sizeof(payload_t)) != 0)
        {
            fprintf(stderr, "  object %d offset %ju: got obj=%ju seq=%ju off=%ju,"
                " expected obj=%ju seq=%ju off=%ju\n", obj, base+i,
                g.obj, g.seq, g.offset, w.obj, w.seq, w.offset);
            return;
        }
    }
}

// Returns -1 when no world explains the answer, 0 when it's exactly the forced
// state with no optional version involved, 1 when the answer is explained with
// optionals. A granule is forced at `pos` when a durable version covering it
// was acknowledged over a complete placement - a full-placement full-object
// write lifted that granule's floor to it - so every member able to serve the
// read holds its bytes. A granule under the floor of an older durable version
// must still show that version. Everything above the floor is optional:
// refused, interrupted, crash-demoted, or acked while some member the set
// still routes reads to couldn't have received it (it was down, or it joined
// empty). allow_partial tolerates an optional winner showing on one granule
// and not on another - an EC read merges per-role chains - as long as every
// shown byte
// is a byte-exact copy of a version the model issued for this very place
int chaos_model_t::world_check(int obj, const std::vector<uint8_t> & got, uint64_t offset,
    uint64_t len, const char *what, bool allow_partial, uint64_t *max_seen)
{
    auto & m = objs[obj];
    // Which deletion (if any) may be absent from the chain serving this read:
    // the one pinned as partially applied - a copy it couldn't reach may
    // still serve the world from before it
    uint64_t opt_del = m.partial_del();

    // Versions some record of this read claims. A claimed version is applied in
    // the serving chain (or, for EC, in at least one role's chain), so it must
    // win every granule of its range that a higher applied version doesn't cover
    std::set<uint64_t> claimset;
    static const payload_t zero = {};
    for (uint64_t pos = offset; pos + sizeof(payload_t) <= offset + len; pos += sizeof(payload_t))
    {
        const payload_t *rec = (const payload_t*)(got.data() + (pos - offset));
        if (!memcmp(rec, &zero, sizeof(payload_t)))
            continue;
        auto vit = m.vers.find(rec->seq);
        if (rec->obj != (uint64_t)obj || rec->offset != pos || vit == m.vers.end() ||
            vit->second.len == 0 || pos < vit->second.off ||
            pos + sizeof(payload_t) > vit->second.off + vit->second.len ||
            memcmp(vit->second.bytes.data() + (pos - vit->second.off), rec, sizeof(payload_t)))
        {
            fprintf(stderr, "  object %d %s: offset %ju: record obj=%ju seq=%ju off=%ju "
                "is not a byte-exact copy of a version the model issued\n",
                obj, what, pos, rec->obj, rec->seq, rec->offset);
            return -1;
        }
        claimset.insert(rec->seq);
    }
    // A version is forced at granule pos when it's durable and a
    // full-object write lifted pos over whatever an older chain holds
    const uint64_t la = m.last_acked(), fl = m.floor_now();
    uint64_t dfl = m.del_floor_now();
    // A deletion the client still owes a clean ack for may already have
    // applied on some members: the world below its version is legal too
    uint64_t ls_ = m.last_submitted();
    if (redel_pending(obj))
    {
        // Not just the very last submitted op: a refused delete may be followed
        // by another refused op, and it still may have applied
        for (uint64_t d = ls_; d > la; d--)
        {
            auto dit = m.vers.find(d);
            if (dit != m.vers.end() && dit->second.len == 0 &&
                dit->second.state == V_OPTIONAL)
            {
                if (d > dfl)
                    dfl = d;
                break;
            }
        }
    }
    auto forced_at = [&m, fl, dfl](uint64_t v, uint64_t pos)
    {
        // SYNCED is durable with every chain known to hold it; the floor says a
        // later whole-object write lifted every granule over whatever a stale
        // or new chain may miss; below the newest applied deletion nothing can
        // be demanded
        return v > 0 && m.vers.at(v).state == V_SYNCED &&
            fl >= v && v > dfl;
    };
    // Worlds for deletions: the strict chain first; then the one where the
    // deletions acknowledged before a power outage applied on the members
    // landed everywhere and the deletion's effect survives the outage even
    // though the primary lost the record - demoted deletes aren't here, the
    // floor raising above covers them; and, in EC, worlds where a copy
    // missed a deletion
    for (int dsel = 0; dsel <= 1; dsel++)
    for (int pass = 0; pass <= (opt_del ? 1 : 0); pass++)
    {
        // The applied set: every forced version plus the claimed ones. Walk
        // down from the top for each granule: the first applied version
        // covering it is the winner, and a deletion above it ends the walk
        // with "absent"
        bool ok = true, tolerated = pass != 0 || dsel != 0;
        uint64_t maxw = 0;
        // The applied set doesn't depend on pos (forced_at ignores it), so
        // collect it once per (dsel, pass) as a dense top-down list instead of
        // re-testing every recorded version for every payload. The walk below
        // is then the same walk-down-from-top, over a far smaller array that
        // almost always stops on the first entry
        std::vector<std::pair<uint64_t, const delta_t*>> ap;
        for (auto it = m.vers.end(); it != m.vers.begin(); )
        {
            --it;
            // A clean acknowledged delete is part of the applied set of
            // every chain that received it; in EC, per-role chains may
            // have received it only in part, so its absence world must be
            // available. For replicated reads the primary's own chain is
            // the only one served and deletes there are judged by the
            // gone paths, not mid-merge
            if (!forced_at(it->first, 0) && !claimset.count(it->first) &&
                !(dsel && it->second.len == 0 && it->first <= la) &&
                !(allow_partial && it->second.len == 0 && it->first <= la &&
                  it->second.state != V_OPTIONAL &&
                  it->second.state != V_SUBMITTED))
                continue;
            if (pass && it->first == opt_del)
                continue;
            ap.push_back(std::make_pair(it->first, (const delta_t*)&it->second));
        }
        for (uint64_t pos = offset; pos + sizeof(payload_t) <= offset + len;
             pos += sizeof(payload_t))
        {
            const payload_t *rec = (const payload_t*)(got.data() + (pos - offset));
            uint64_t winner = 0;   // 0 = absent
            for (auto & a: ap)
            {
                if (a.second->len == 0)
                {
                    winner = 0;    // a deletion covers everything
                    break;
                }
                if (pos >= a.second->off && pos + sizeof(payload_t) <=
                    a.second->off + a.second->len)
                {
                    winner = a.first;
                    break;
                }
            }
            bool hole = !memcmp(rec, &zero, sizeof(payload_t));
            uint64_t shown = hole ? 0 : rec->seq;
            if (hole ? winner == 0 : winner == rec->seq)
            {
                if (winner > maxw)
                    maxw = winner;
                if (!forced_at(winner, pos))
                    tolerated = true;
                continue;
            }
            // A non-forced winner may show on one granule and not on another
            // (per-role EC chains, or a stale serving member), so an older
            // claimed version or a hole beneath it is legal - as are older
            // claimed bytes beneath a deletion a copy hasn't seen. A forced
            // winner missing from its own granule is not
            if (allow_partial && !forced_at(winner, pos) &&
                (hole || (claimset.count(shown) && (shown < winner || winner == 0))))
            {
                tolerated = true;
                if (winner > maxw)
                    maxw = winner;
                continue;
            }
            ok = false;
            fprintf(stderr, "  object %d %s: offset %ju shows %s but the highest "
                "applicable version covering it is seq %ju%s\n", obj, what, pos,
                hole ? "a hole" : "a lower version's bytes", winner,
                winner ? "" : " (absent)");
            break;
        }
        if (ok)
        {
            if (max_seen)
                *max_seen = maxw;
            return tolerated ? 1 : 0;
        }
    }
    return -1;
}

// chaos_t: the driver

chaos_t::chaos_t() : cluster(*this) {}

std::vector<osd_num_t> chaos_t::live_set()
{
    std::vector<osd_num_t> r;
    for (auto n: cur_set)
    {
        if (n == 1 || cluster.peers[n].up)
            r.push_back(n);
    }
    return r;
}

uint32_t chaos_t::live_overlap(const std::vector<osd_num_t> &grp, const std::vector<osd_num_t> &pool)
{
    uint32_t c = 0;
    for (auto g: grp)
    {
        for (auto p: pool)
        {
            if (g == p)
            {
                c++;
                break;
            }
        }
    }
    return c;
}

bool chaos_t::quorum_kept(const std::vector<osd_num_t> &pool)
{
    // The PG itself must stay peerable: cursize < pg_minsize makes it
    // INCOMPLETE, which for EC is the write-hang territory (osd_peering.cpp
    // FIXME). The per-object check below can't catch this when every object
    // happens to be deleted
    if (pool.size() < (size_t)pg_minsize)
        return false;
    for (int obj = 0; obj < OBJ_COUNT; obj++)
    {
        // Walk the versions newest-first and defend every one a read may
        // still resolve to, stopping at the first SYNCED: stable is always
        // the peering target, so a serving chain never rolls below the last
        // synced version, and everything under it (or under an acknowledged
        // deletion) is gone or covered. The unstable versions above it get
        // defended too: an event that re-peers the PG rolls them back, but
        // a read may catch one before that, and an -EIO from a placement
        // this event cut under pg_minsize isn't a legal answer. OPTIONAL
        // holes carry no placement claim (a refused write may have applied
        // on any subset and its bytes may legitimately vanish); DEAD ones
        // can't show again
        for (auto it = objs[obj].vers.rbegin(); it != objs[obj].vers.rend(); ++it)
        {
            const delta_t & d = it->second;
            if (d.state == V_SYNCED)
            {
                if (d.len > 0 && live_overlap(d.ack_live, pool) < (uint32_t)pg_minsize)
                    return false;
                break;
            }
            if ((d.state == V_OPTIONAL || d.state == V_SUBMITTED ||
                 d.state == V_ACKED) && d.len > 0 &&
                live_overlap(d.ack_live, pool) < (uint32_t)pg_minsize)
                return false;
        }
    }
    return true;
}

void chaos_t::configure(const chaos_cfg_t & cfg)
{
    use_fsync = cfg.use_fsync;
    is_ec = cfg.ec;
    if (is_ec)
    {
        pg_size = EC_DATA + EC_PARITY;
        pg_minsize = EC_DATA;
        parity = EC_PARITY;
        blk = OBJ_SIZE / EC_DATA;
        blk_bmp = blk / GRAN / 8;
        // The primary is role 0 of a 4-member set; the spare OSD waits outside
        cur_set = { 1, 2, 3, 4 };
    }
    else
    {
        pg_size = 3;
        pg_minsize = 2;
        parity = 0;
        blk = OBJ_SIZE;
        blk_bmp = BMP_SIZE;
        cur_set = { 1, 2, 3 };
    }
    osd_config = json11::Json::object {
        { "osd_num", (uint64_t)1 },
        { "etcd_address", "127.0.0.1:2379" },
        { "etcd_prefix", "/vitastor" },
        { "immediate_commit", cfg.use_fsync ? "none" : "all" },
        { "block_size", blk },
        { "bitmap_granularity", (uint64_t)GRAN },
        { "data_device", "./test_pg_chaos_data.bin" },
        { "data_device_size", "268435456" },
        { "data_device_sect", "4096" },
        { "meta_offset", "0" },
        { "journal_offset", "16777216" },
        // The mock etcd doesn't implement lease keepalive, and the OSD renews its
        // lease on the report-interval timer - with virtual time running fast that
        // would kill the OSD. Make both timers effectively never fire
        { "etcd_report_interval", (uint64_t)100000000 },
        { "etcd_stats_interval", (uint64_t)100000000 },
        { "data_offset", "33554432" },
        { "disable_data_fsync", cfg.use_fsync ? "0" : "1" },
        { "meta_format", (uint64_t)3 },
        { "log_level", (uint64_t)(trace ? 5 : 0) },
        // These two print to stdout on a real-time timer; the simulated clock would
        // trigger them constantly and only produce noise
        { "print_stats_interval", (uint64_t)1000000000 },
        { "slow_log_interval", (uint64_t)1000000000 },
    };
    if (cfg.csum_block)
    {
        osd_config["data_csum_type"] = "crc32c";
        osd_config["csum_block_size"] = (uint64_t)cfg.csum_block;
    }
    if (!cfg.recovery)
    {
        // Leave misplaced and degraded copies where they are unless asked; the
        // recovery machinery itself is real code the fake peers can also serve,
        // so specific seeds can turn it on
        osd_config["no_recovery"] = true;
        osd_config["no_rebalance"] = true;
    }
}

void chaos_t::create(uint32_t seed)
{
    // The fixture's ringloop is inert; the simulator needs one that feeds SQEs to it
    delete f.ringloop;
    f.ringloop = new ring_loop_mock_t(RINGLOOP_DEFAULT_SIZE, [this](io_uring_sqe *sqe)
    {
        if (sqe->fd == MOCK_DATA_FD)
        {
            assert(data_disk);
            bool ok = data_disk->submit(sqe);
            assert(ok);
        }
        else if (sqe->fd == MOCK_META_FD)
        {
            // A single-device configuration: the journal and metadata live on the
            // data disk, so a separate meta fd would mean the blockstore changed
            fprintf(stderr, "unexpected SQE on a separate meta device\n");
            abort();
        }
        else
        {
            // The messenger is mocked out and the blockstore only uses mock fds, so
            // anything else would be a request nobody would ever complete
            fprintf(stderr, "unexpected SQE: fd=%d opcode=%d\n", (int)sqe->fd, (int)sqe->opcode);
            abort();
        }
    });
    delete f.tfd;
    f.tfd = NULL;
    // Drop the fixture's initial client; start_osd creates a fresh one over the store
    delete f.st_cli;
    f.st_cli = NULL;
    sim = new io_sim_t(f.ringloop, seed);
    // Buffered messenger replies are delivered with every sim step, so any pump
    // (run_until, force_sync, per-step heartbeat) can complete a waiting op
    sim->after_step = [this]()
    {
        drain_replies();
    };
    data_disk = new disk_mock_t("data disk", parse_size(osd_config["data_device_size"].string_value()), cfg_buffered());
    data_disk->clear(0, parse_size(osd_config["data_offset"].string_value()));
    data_disk->faults.forbid_overlapping_writes = true;
    data_disk->trace = trace;
    data_disk->atomic_write_size = 4096;
    sim->add_disk(data_disk);
    // Every current set member starts up; the spare (OSD 4 for the replicated pool,
    // OSD 5 for EC) only joins via events
    cluster.peers.clear();
    for (uint64_t n = 2; n <= pg_size; n++)
        cluster.peers[n] = (fake_peer_t){ .num = (osd_num_t)n, .up = true, .ch = this };
    cluster.peers[pg_size+1] = (fake_peer_t){ .num = (osd_num_t)(pg_size+1), .up = false, .ch = this };
    start_osd();
}

bool chaos_t::cfg_buffered()
{
    return osd_config["disable_data_fsync"].string_value() != "1";
}

bool chaos_t::pgs_quiescent()
{
    static const uint64_t BUSY = PG_REPEERING | PG_PEERING | PG_STARTING;
    if (!f.has_pg(POOL_ID, PG_NUM))
        return false;
    pg_t & pg = f.pg(POOL_ID, PG_NUM);
    if ((pg.state & BUSY) != 0 || !(pg.state & (PG_ACTIVE | PG_INCOMPLETE)) ||
        pg.inflight != 0 || pg.flush_batch != NULL || !pg.flush_actions.empty())
        return false;
    return true;
}

bool chaos_t::pg_settled()
{
    if (!f.has_pg(POOL_ID, PG_NUM))
        return false;
    pg_t & pg = f.pg(POOL_ID, PG_NUM);
    // A PG without quorum is settled too: it sits INCOMPLETE until the next event
    // revives a peer, and that's not a deadlock. What must not remain is peering
    // work or an unfinished rollback/recovery batch
    static const uint64_t BUSY = PG_REPEERING | PG_PEERING | PG_STARTING;
    return !(pg.state & BUSY) && (pg.state & (PG_ACTIVE | PG_INCOMPLETE)) &&
        pg.flush_batch == NULL && pg.flush_actions.empty();
}

bool chaos_t::pg_healthy()
{
    if (!f.osd || !f.has_pg(POOL_ID, PG_NUM))
        return false;
    pg_t & pg = f.pg(POOL_ID, PG_NUM);
    static const uint64_t BUSY = PG_REPEERING | PG_PEERING | PG_STARTING | PG_INCOMPLETE;
    return !(pg.state & BUSY) && (pg.state & PG_ACTIVE) &&
        pg.flush_batch == NULL && pg.flush_actions.empty();
}

bool chaos_t::pg_active()
{
    if (!f.osd || !f.has_pg(POOL_ID, PG_NUM))
        return false;
    pg_t & pg = f.pg(POOL_ID, PG_NUM);
    return (pg.state & PG_ACTIVE);
}

void chaos_t::dump_peers(int obj)
{
    fprintf(stderr, "  set:");
    for (auto s: cur_set)
    {
            fprintf(stderr, " %ju%s", (uint64_t)s,
                s == 1 || cluster.peers[s].up ? "" : "(down)");
    }
    fprintf(stderr, "\n");
    // EC keeps one role per stripe, so scan all stripes of the object
    for (auto & kv: cluster.peers)
    {
        fprintf(stderr, "  peer %ju%s:", (uint64_t)kv.first,
            kv.second.up ? "" : "(down)");
        for (auto & sv: kv.second.store)
        {
            if (cluster.obj_of(sv.first) != (uint64_t)obj)
                continue;
            fprintf(stderr, " r%ju:", cluster.role_of(sv.first));
            for (auto & vv: sv.second)
            {
                fprintf(stderr, " v%ju%s%s", vv.first,
                    vv.second.stable ? "S" : "u",
                    vv.second.fsynced ? "" : "!f");
            }
        }
        fprintf(stderr, "\n");
    }
}

bool chaos_t::pg_needs_osd(osd_num_t num)
{
    if (!f.has_pg(POOL_ID, PG_NUM))
        return false;
    pg_t & pg = f.pg(POOL_ID, PG_NUM);
    for (osd_num_t o: pg.all_peers)
    {
        if (o == num)
            return true;
    }
    for (auto & hs: pg.target_history)
    {
        for (osd_num_t o: hs)
        {
            if (o == num)
                return true;
        }
    }
    return false;
}


void chaos_t::sync_etcd_revisions()
{
    for (auto *cli: f.etcd_store.clients)
    {
        cli->etcd_watch_revision_config = cli->etcd_watch_revision_osd =
            cli->etcd_watch_revision_pg = (int)f.etcd_store.mod_revision + 1;
    }
}

bool chaos_t::run_until(std::function<bool()> cond, int max_steps)
{
    return sim->run_until([this, &cond]() {
        sync_etcd_revisions();
        cluster.service_peers();
        drain_replies();
        // The OSD drops connections of clients holding dirty (unsynced) writes
        // to a PG it repeers - that's the session guarantee breaking
        check_session_death();
        return cond();
    }, max_steps);
}

void chaos_t::write_pg_history(const std::vector<osd_num_t> & old_set)
{
    std::vector<osd_num_t> old_sorted = old_set;
    std::sort(old_sorted.begin(), old_sorted.end());
    auto & pg_cfg = f.st_cli->pool_config[POOL_ID].pg_config[PG_NUM];
    // Mirror what a real OSD would report first: the PG is taken by us, so
    // our own last report (with cur_primary set) is already in the store
    pg_cfg.cur_primary = 1;
    auto & hist = pg_cfg.target_history;
    auto it = std::lower_bound(hist.begin(), hist.end(), old_sorted);
    if (it == hist.end() || *it != old_sorted)
        hist.insert(it, old_sorted);
    std::set<osd_num_t> all_peers(cur_set.begin(), cur_set.end());
    for (auto & hs: hist)
    {
        for (osd_num_t o: hs)
            all_peers.insert(o);
    }
    json11::Json::array sets_arr;
    for (auto & hs: hist)
        sets_arr.push_back(json11::Json(hs));
    json11::Json::object value = {
        { "epoch", (double)(pg_cfg.epoch + 1) },
        { "all_peers", json11::Json(std::vector<osd_num_t>(all_peers.begin(), all_peers.end())) },
        { "osd_sets", sets_arr },
    };
    if (pg_cfg.next_scrub)
        value["next_scrub"] = (double)pg_cfg.next_scrub;
    f.st_cli->set("/vitastor/pg/history/"+std::to_string(POOL_ID)+"/"+std::to_string(PG_NUM),
        json11::Json(value));
}

void chaos_t::connect_session()
{
    client_id = f.connect_client();
}

void chaos_t::drain_replies()
{
    auto *cl = client_id ? f.client(client_id) : NULL;
    if (!cl || cl->peer_state == PEER_STOPPED || cl->sent_ops.empty())
        return;
    // In-process delivery of what the mock messenger buffered like a socket
    // would; the ops stay owned by the model and die in free_op as usual.
    // Only ops that carry a real reply may complete: a client id can be reused
    // by a peer's client after a restart, and finishing a peer op (a peering
    // LIST the OSD hasn't answered yet) with a zeroed reply would hand the
    // OSD a fabricated empty world
    auto sent = std::move(cl->sent_ops);
    for (auto & kv: sent)
    {
        auto *op = kv.second;
        if (op->reply.hdr.magic != SECONDARY_OSD_REPLY_MAGIC)
        {
            // Still queued to the server - keep waiting for the real reply
            cl->sent_ops[kv.first] = op;
            continue;
        }
        // Copy the lambda to be unaffected by whatever the callback does
        std::function<void(osd_op_t*)>(op->callback)(op);
    }
}

void chaos_t::session_died()
{
    if (trace)
        printf("client session killed\n");
    // The connection is dead: every op that isn't in the client's hands yet is
    // lost with it. Finished replies still queued in the dead session never
    // reach a real socket's reader - cancel() them: -EPIPE through the callback,
    // refused by account(), not acked. Ops still running inside the OSD get
    // EPIPE'd by the mock's outbox_push when they finish
    auto *cl = client_id ? f.client(client_id) : NULL;
    if (cl)
    {
        auto sent = std::move(cl->sent_ops);
        for (auto & kv: sent)
            kv.second->cancel();
    }
    // Fold the acks the client really received before breaking the promise:
    // reap accounts them as ACKED and demote_unstable() then turns exactly
    // those into OPTIONAL - production order (the ack reached the client, THEN
    // the connection died and the repeer rolls the version back)
    reap();
    if (use_fsync)
        demote_unstable();
    connect_session();
}

// Poll whether the messenger still has our session. The OSD that repeers a PG
// kills the sessions holding dirty (un-synced) writes to it, and only those
// sessions can own acked-unstable versions - so a session that's still alive
// means nothing of ours was rolled back, and the model keeps its promises
void chaos_t::check_session_death()
{
    // No session (yet): nothing can have died - connect_session() in
    // start_osd establishes it after the loading pumps
    if (client_id)
    {
        auto *cl = f.client(client_id);
        if (!cl || cl->peer_state == PEER_STOPPED)
            session_died();
    }
}

void chaos_t::settle()
{
    if (!run_until([this]() { return pg_settled(); }, MAX_STEPS))
    {
        fprintf(stderr, "step %ju: the PG did not settle in %d steps at t=%juus - deadlock?\n",
            step_no, MAX_STEPS, sim->now_us);
        report_deadlock();
    }
    flush_recover_pending();
}

// Wait for the PG to settle and flush the deferred checks only with probability
// p_post; the rest of the time keep operating against a still-settling PG. The
// unconditional wait used to serialize the timeline: ops never overlapped a
// settling repeer, so a write sitting in the write_queue behind flush_actions
// at the very moment a repeer or stop began - the state a forgotten
// write_queue cancellation hangs on - never arose. A lost cancellation now
// shows up as ops that never complete (the step loop's deadlock detector) or
// as a settle() timeout at the next event that does wait
void chaos_t::maybe_settle()
{
    if (chance(probs.p_post))
        settle();
}

void chaos_t::demote_unstable()
{
    for (int obj = 0; obj < OBJ_COUNT; obj++)
    {
        for (auto & kv: objs[obj].vers)
        {
            if (kv.second.state == V_ACKED)
                kv.second.state = V_OPTIONAL;
            // an in-flight version isn't demoted by an unrelated crash - its
            // op hasn't landed yet; whatever lands it will say
        }
    }
}

void chaos_t::do_peer_down()
{
    // A crash must not take any object's serving chain below the
    // reconstructible number of live copies. quorum_kept() walks every
    // object's versions top-down - defending each version a read may still
    // be served from, down to and including the last SYNCED one, below
    // which stable rules and the delete floor leave nothing to defend - and
    // rejects victims that would drop any of them under pg_minsize live
    // placement members. Several members may already be down: their synced
    // copies survive and come back with them, so nothing forbids stacking
    // failures as long as that holds
    std::vector<osd_num_t> live;
    for (auto & kv: cluster.peers)
    {
        if (kv.second.up)
            live.push_back(kv.first);
    }
    if (!live.size())
        return;
    // Pick a victim whose loss still leaves every still-servable version
    // pg_minsize live placement members - below that the data would become
    // legitimately unreadable, which isn't corruption we can check for
    for (size_t i = 0; i < live.size(); i++)
        std::swap(live[i], live[rnd(i, live.size()-1)]);
    osd_num_t n = 0;
    for (auto cand: live)
    {
        std::vector<osd_num_t> pool;
        for (auto m: cur_set)
        {
            if (m == 1 || (cluster.peers[m].up && m != cand))
                pool.push_back(m);
        }
        if (quorum_kept(pool))
        {
            n = cand;
            break;
        }
    }
    if (!n)
        return;
    if (trace)
        printf("peer %ju down\n", n);
    cluster.peers[n].crash();
    if (f.osd && cluster.connected.count(n))
    {
        f.disconnect_peer(n);
        cluster.connected.erase(n);
    }
    peer_downs++;
    maybe_settle();
    // Demotion happens only where the client can OBSERVE it: a peer crash
    // repeers the PG, the repeer kills the dirty client sessions (V_ACKED in
    // volatile mode means exactly a dirty client), and session_died() demotes.
    // The model never demotes from server-side knowledge of who crashed or
    // what their journal lost - if a crash rolls something back while the
    // session lives, that's a non-repeatable read and check 5 catches it
}

void chaos_t::do_peer_up()
{
    std::vector<osd_num_t> down;
    for (auto & kv: cluster.peers)
    {
        if (!kv.second.up)
            down.push_back(kv.first);
    }
    if (!down.size())
        return;
    osd_num_t n = down[rnd(0, down.size()-1)];
    if (trace)
        printf("peer %ju up\n", n);
    cluster.peers[n].up = true;
    // Connect the peer if the PG can talk to it: a current member or one of the
    // historical sets the PG's history still holds (peering accounts for its copies
    // and deletes reach them; the old "only current set" rule left history members
    // permanently dead and deletions incomplete)
    if (f.osd && (in_set(n) || pg_needs_osd(n)) && !cluster.connected.count(n))
    {
        f.connect_peer(n);
        cluster.connected.insert(n);
    }
    peer_ups++;
    maybe_settle();
    // same as in do_peer_down: the repeer a returning member starts kills any
    // dirty session it rolls back over, and session_died() handles the demotion
}

bool chaos_t::in_set(osd_num_t n)
{
    return std::find(cur_set.begin(), cur_set.end(), n) != cur_set.end();
}

void chaos_t::write_pg_config(osd_num_t primary)
{
    json11::Json::object items;
    json11::Json::array osd_set;
    for (auto n: cur_set)
        osd_set.push_back((double)n);
    items[std::to_string(PG_NUM)] = json11::Json::object {
        { "osd_set", osd_set },
        { "primary", (double)primary },
    };
    f.st_cli->set("/vitastor/pg/config", json11::Json::object {
        { "items", json11::Json::object { { std::to_string(POOL_ID), items } } },
    });
}

void chaos_t::do_set_change()
{
    std::vector<osd_num_t> old_set = cur_set;
    std::vector<osd_num_t> members, spare;
    for (auto & kv: cluster.peers)
    {
        if (!kv.second.up)
            continue;
        if (in_set(kv.first))
            members.push_back(kv.first);
        else
            spare.push_back(kv.first);
    }
    if (!members.size() || !spare.size())
        return;
    // Shuffle both lists so any member can be kicked and any spare invited
    for (size_t i = 0; i < members.size(); i++)
        std::swap(members[i], members[rnd(i, members.size()-1)]);
    for (size_t i = 0; i < spare.size(); i++)
        std::swap(spare[i], spare[rnd(i, spare.size()-1)]);
    // Keep at least one old member so peering keeps the quorum across the change
    uint32_t keep_count = rnd(1, pg_size-2);
    while (keep_count >= 1 &&
        (members.size() < keep_count || spare.size() < pg_size-1-keep_count))
        keep_count--;
    if (keep_count < 1)
        return;
    std::vector<osd_num_t> new_set = { 1 };
    if (is_ec)
    {
        // EC roles are positions and recovery is off, so copies can't move:
        // content is keyed by stripe|role, and any set change other than
        // replacing a DEAD member in place strands chains - a kicked live
        // member takes its chains out of the set, a newcomer arrives empty,
        // and the acknowledged data becomes legitimately unreconstructible
        // (the class the soak caught: invited-empty + stale-parity INCOMPLETE
        // reads and -EIO writes). So EC changes may only put a spare into a
        // role whose member is currently down; every live member keeps its
        // exact role, and the change is the whole point of the replacement
        new_set = cur_set;
        uint32_t si = 0, holes = 0;
        for (uint32_t p = 1; p < new_set.size(); p++)
            holes += (new_set[p] != 1 && !cluster.peers[new_set[p]].up);
        if (!holes)
            return;
        for (uint32_t p = 1; p < new_set.size(); p++)
        {
            if (new_set[p] != 1 && !cluster.peers[new_set[p]].up)
            {
                if (si >= spare.size())
                    return;   // can't replace every dead role
                new_set[p] = spare[si++];
            }
        }
    }
    else
    {
        for (uint32_t i = 0; i < keep_count; i++)
            new_set.push_back(members[i]);
        for (uint32_t i = 0; i + keep_count < pg_size-1; i++)
            new_set.push_back(spare[i]);
    }
    if (new_set == cur_set)
        return;
    // The invited members arrive empty, so only the primary and the kept old
    // members still hold any object's newest EC version. A member that is alive
    // now but was down at the last write isn't in the version's placement
    // either, so this is the check that really protects the acknowledged EC data
    if (!quorum_kept(new_set))
        return;
    if (trace)
    {
        printf("set change: {");
        for (auto n: cur_set)
            printf("%ju,", (uint64_t)n);
        printf("} -> {");
        for (auto n: new_set)
            printf("%ju,", (uint64_t)n);
        printf("}\n");
    }
    cur_set = new_set;
    // Membership needs no model bookkeeping: merged reads always answer with
    // the newest version any chain in quorum holds, so newcomers carrying
    // older copies simply lose every merge and nothing acknowledged has to be
    // re-promised
    // Members shuffled out of the set stay connected: they remain in the PG history
    // (the etcd data survives restarts), peering LISTs their copies so that later
    // deletes cover them, and while one of them can't be reached a delete reports
    // left_on_dead. Connect them before the config put so that the repeer triggered
    // by it starts with the full peer set: dead_peers stays empty while they're up
    for (auto & kv: cluster.peers)
    {
        if (kv.second.up && !cluster.connected.count(kv.first))
        {
            f.connect_peer(kv.first);
            cluster.connected.insert(kv.first);
        }
    }
    write_pg_config();
    write_pg_history(old_set);
    set_changes++;
    maybe_settle();
}

void chaos_t::start_osd()
{
    assert(!f.osd);
    f.tfd = new timerfd_manager_t(NULL);
    // Virtual time mode requires timerfd==-1, which a NULL ctor handler guarantees.
    // bind_socket() still registers its listen fd through this member, so install a
    // sink for it: those events would only ever reach the mocked messenger anyway.
    f.tfd->set_fd_handler = [](int fd, bool enable, std::function<void(int, int)>) {};
    sim->set_tfd(f.tfd);
    // A fresh client over the surviving store: a restart recreates the client,
    // not the etcd data (keys, revisions and leases carry over)
    etcd_state_client_mock_t *st_cli = new etcd_state_client_mock_t(f.etcd_store);
    f.st_cli = st_cli;
    sync_etcd_revisions();
    // The replicated pool is x3 with a quorum of 2, so losing one peer degrades
    // the PG instead of killing it; losing two makes it incomplete. The EC pool is
    // 2+2 with a quorum of 2: one block per member, any two members can rebuild
    std::vector<std::vector<osd_num_t>> pgs;
    pgs.push_back(cur_set);
    f.configure_replicated_pool(POOL_ID, cur_set.size(), 2, PG_NUM, pgs);
    // The fixture helper hardcodes a replicated immediate_commit=none pool; the
    // pool must advertise the scheme and commit mode this run is configured for
    f.st_cli->set("/vitastor/config/pools", json11::Json::object {
        { std::to_string(POOL_ID), json11::Json::object {
            { "name", "pool_" + std::to_string(POOL_ID) },
            { "scheme", is_ec ? "ec" : "replicated" },
            { "pg_size", (uint64_t)pg_size },
            { "pg_minsize", (uint64_t)pg_minsize },
            { "parity_chunks", (uint64_t)parity },
            { "block_size", (uint64_t)blk },
            { "pg_count", (uint64_t)PG_NUM },
            { "failure_domain", "osd" },
            { "immediate_commit", use_fsync ? "none" : "all" },
        } },
    });
    bs = NULL;
    try
    {
        f.osd = new osd_t(osd_config, f.ringloop, f.tfd,
            std::unique_ptr<etcd_state_client_t>(st_cli),
            [this](blockstore_config_t & cfg) -> blockstore_i* {
                bs = new blockstore_impl_t(cfg, f.ringloop, f.tfd, true);
                return bs;
            });
    }
    catch(std::exception & ex)
    {
        fprintf(stderr, "failed to create OSD: %s\n", ex.what());
        abort();
    }
    f.osd->start();
    // (Re)connect every fake peer that is up; LIST subops get answered by
    // service_peers inside the wait below
    cluster.connected.clear();
    for (auto & kv: cluster.peers)
    {
        if (kv.second.up)
        {
            f.connect_peer(kv.first);
            cluster.connected.insert(kv.first);
        }
    }
    // The mock has no watch: the lease grant -> state put -> load_pgs chain
    // completes in txn responses, but the blockstore initial load only
    // advances when the simulator steps; drive it until the load finished
    if (!run_until([this]()
    {
        auto *mc = dynamic_cast<etcd_state_client_mock_t*>(f.st_cli);
        return f.osd->peering_state != OSD_LOADING_PGS && mc && mc->queue.empty();
    }, MAX_STEPS))
    {
        fprintf(stderr, "step %ju: OSD %ju did not load PGs from etcd in %d steps at t=%juus\n",
            step_no, f.osd->osd_num, MAX_STEPS, sim->now_us);
        report_deadlock();
    }
    connect_session();
    if (!run_until([this]() { return pgs_quiescent(); }, MAX_STEPS))
    {
        fprintf(stderr, "step %ju: OSD did not finish peering in %d steps at t=%juus\n",
            step_no, MAX_STEPS, sim->now_us);
        report_deadlock();
    }
}

void chaos_t::destroy()
{
    // The OSD owns its blockstore and its etcd client. The mock etcd can't
    // expire a lease on its own, so emulate the crash: revoke the lease and
    // drop its keys, like the real etcd would do after the TTL
    if (f.osd)
    {
        uint64_t lease_id = strtoull(f.osd->etcd_lease_id.c_str(), NULL, 10);
        // revoke would notify; the OSD is going away, so drop the keys silently
        for (auto it = f.etcd_store.data.begin(); it != f.etcd_store.data.end(); )
        {
            if (lease_id && it->second.lease_id == lease_id)
                it = f.etcd_store.data.erase(it);
            else
                it++;
        }
        f.etcd_store.leases.erase(lease_id);
        // Replies a peer still holds belong to ops of the OSD being deleted;
        // delivering them would run callbacks on freed state. Drop them the
        // same way forget_inflight drops ops (bounded by max_inflight)
        for (auto & pkv: cluster.peers)
            pkv.second.replying.clear();
        // Bytes still sitting on the messenger's connections do not die with
        // the process: the kernel delivers them. Catch the mutating subops
        // aside; the peers will apply them in their own time
        cluster.harvest_wire();
        delete f.osd;
        f.osd = NULL;
    }
    bs = NULL;
    f.st_cli = NULL;
    f.ringloop->reset();
    // The blockstore and the OSD set one-shot timers capturing `this`, so the timer
    // manager can't outlive them across a simulated crash
    if (f.tfd)
    {
        delete f.tfd;
        f.tfd = NULL;
    }
}

chaos_t::~chaos_t()
{
    destroy();
    for (auto *p: inflight)
    {
        if (p->done)
            free_op(p);
        else
        {
            // Ops interrupted by a power outage were never completed by
            // anyone and the instance that owned them is gone - the model
            // still owns their memory
            reap_op(p->op);
            p->op = NULL;
        }
        delete p;
    }
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
}

uint64_t chaos_t::rnd(uint64_t min, uint64_t max)
{
    return sim->random(min, max);
}

bool chaos_t::chance(uint32_t percent)
{
    return rnd(0, 99) < percent;
}

void chaos_t::enqueue(pending_t *p)
{
    p->seq_id = ++seq_id;
    if (trace)
        printf(" seq %ju\n", p->seq_id);
    p->op->callback = [this, p](osd_op_t *op)
    {
        if (trace)
            printf("done %ju\n", p->seq_id);
        // The OSD answers degraded reads with a multi-segment reply in op->iov
        // ("send buffer in parts to avoid copying"); the real messenger copies
        // those segments into the client buffer. This test hands ops to the OSD
        // directly, so materialize the reply into op->buf the same way, otherwise
        // a reconstructed read would appear as buffer leftovers in op->buf
        if (op->req.hdr.opcode == OSD_OP_READ && op->reply.hdr.retval > 0 && op->iov.count > 0 &&
            !(op->iov.count == 1 && op->iov.buf[0].iov_base == op->buf))
        {
            // The segments of a degraded answer live in the OSD's own read
            // buffer and op->buf is the same allocation, so the segments can't
            // be compacted down in place - they overlap and clobber each other
            // (the real messenger sends the segments over a socket and the
            // real client reassembles them into its own buffer), so stage
            // them through a temporary first
            size_t acc = 0;
            for (size_t s = 0; s < op->iov.count; s++)
                acc += op->iov.buf[s].iov_len;
            assert(acc == (size_t)op->reply.hdr.retval);
            std::vector<uint8_t> got(acc);
            size_t acc2 = 0;
            for (size_t s = 0; s < op->iov.count; s++)
            {
                memcpy(got.data()+acc2, op->iov.buf[s].iov_base, op->iov.buf[s].iov_len);
                acc2 += op->iov.buf[s].iov_len;
            }
            memcpy((uint8_t*)op->buf, got.data(), acc);
        }
        p->done = true;
    };
    f.exec(p->op);
}

pending_t *chaos_t::submit(pending_t *p)
{
    for (int o: p->touched)
        busy[o]++;
    enqueue(p);
    inflight.push_back(p);
    return p;
}

void chaos_t::free_op(pending_t *p)
{
    // ~osd_op_t frees op->buf, including the one we attached to a write
    delete p->op;
    delete p;
}

// Free an op no one will ever complete: its OSD instance was destroyed under
// it (a crash). Mirrors the ownership of the production completion path -
// the op_data blob owns the stripes and the chain (one calloc), the subop
// array owns per-subop bs_ops while its rmw_buf/bitmap/iov alias into op_data
// and the op's buffers, the sync snapshot hangs off one more malloc and two
// news, and ~osd_op_t frees buf/bitmap_buf/rmw_buf. No finish_op(): the model
// has already accounted these ops the way a crash lands them
void chaos_t::reap_op(osd_op_t *op)
{
    if (op->op_data)
    {
        auto od = op->op_data;
        if (od->subops)
        {
            for (int i = 0; i < od->n_subops; i++)
            {
                delete od->subops[i].bs_op;
                od->subops[i].bs_op = NULL;
                // alias into the op_data blob - never owned by the subop
                od->subops[i].rmw_buf = NULL;
            }
            delete[] od->subops;
            od->subops = NULL;
        }
        // unstable_* / dirty_pgs and chain_reads share one anonymous union:
        // which arm is live depends on the opcode, freeing both double-frees
        if (op->req.hdr.opcode == OSD_OP_READ)
        {
            if (od->chain_reads)
                free(od->chain_reads);
        }
        else
        {
            if (od->unstable_writes)
            {
                delete od->unstable_write_osds;
                delete[] od->unstable_writes;
            }
            if (od->dirty_pgs)
                free(od->dirty_pgs);
        }
        free(od);
        op->op_data = NULL;
    }
    delete op->bs_op;
    op->bs_op = NULL;
    delete op;
}

osd_op_t *chaos_t::make_rw_op(uint64_t opcode, uint64_t offset, uint64_t len)
{
    auto *op = new osd_op_t();
    memset(&op->req, 0, sizeof(op->req));
    memset(&op->reply, 0, sizeof(op->reply));
    // Client-owned (OSD_OP_OUT): the session may be killed while the op is on
    // the wire; the messenger's cancel path completes such ops with -EPIPE
    // through the callback instead of destroying them
    op->op_type = OSD_OP_OUT;
    op->client_id = client_id;
    op->req.rw.header.magic = SECONDARY_OSD_OP_MAGIC;
    op->req.rw.header.id = ++op_id;
    op->req.rw.header.opcode = opcode;
    op->req.rw.inode = INODE_WITH_POOL(POOL_ID, INODE);
    op->req.rw.offset = offset;
    op->req.rw.len = len;
    op->req.rw.version = 0; // let the OSD pick the version
    return op;
}

pending_t *chaos_t::submit_write(int obj)
{
    auto & m = objs[obj];
    uint64_t offset = rnd(0, OBJ_SIZE/GRAN - 1) * GRAN;
    uint64_t len = rnd(1, (OBJ_SIZE - offset)/GRAN) * GRAN;
    uint64_t version = m.last_submitted() + 1;
    auto *p = new pending_t();
    p->obj = obj;
    p->touched.push_back(obj);
    p->version = version;
    // The bytes this version will issue: the acknowledged state of the range
    // with new content stamped over it. Only the range is needed - the write
    // carries nothing outside it - so the buffer is range-sized
    const std::vector<uint8_t> & base = base_content(obj);
    std::vector<uint8_t> buf(base.begin()+offset, base.begin()+offset+len);
    fill_range(buf, obj, version, offset, len);
    // A submitted write is a legal outcome of a crash or outage the moment it's
    // in flight - its entries may already live on the members' chains - so the
    // model records its delta right away, not only on ack
    delta_t d;
    d.off = offset;
    d.len = len;
    d.bytes = std::move(buf);
    p->op = make_rw_op(OSD_OP_WRITE, OBJ_OFFSET(obj) + offset, len);
    p->op->buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, len);
    memcpy(p->op->buf, d.bytes.data(), len);
    m.vers[version] = std::move(d);
    p->on_success = [this, obj]() { written(obj); };
    writes++;
    if (trace)
        printf("write obj%d %ju+%ju v%ju", obj, offset, len, version);
    return submit(p);
}

pending_t *chaos_t::submit_delete(int obj)
{
    auto & m = objs[obj];
    auto *p = new pending_t();
    p->obj = obj;
    p->touched.push_back(obj);
    p->version = m.last_submitted() + 1;
    // Like a write, a submitted delete is a legal post-outcome state from the
    // moment it goes in: its markers may already sit on some of the copies
    m.vers[p->version] = delta_t();
    p->op = make_rw_op(OSD_OP_DELETE, OBJ_OFFSET(obj), 0);
    p->on_success = [this, obj]() { written(obj); };
    deletes++;
    if (trace)
        printf("delete obj%d v%ju", obj, p->version);
    return submit(p);
}

pending_t *chaos_t::submit_sync()
{
    auto *p = new pending_t();
    p->op = make_rw_op(OSD_OP_SYNC, 0, 0);
    // What the sync is going to cover: everything acknowledged by the time it was
    // submitted. Anything acknowledged later may or may not make it in, so it isn't
    // counted - the snapshot is a lower bound, which is all the model needs
    std::vector<uint64_t> covered(OBJ_COUNT);
    for (int obj = 0; obj < OBJ_COUNT; obj++)
        covered[obj] = objs[obj].last_acked();
    p->on_success = [this, covered]() { synced(covered); };
    syncs++;
    if (trace)
        printf("sync");
    return submit(p);
}

chaos_action_t chaos_t::pick_action()
{
    const uint32_t w[ACT_COUNT] = {
        probs.crash, probs.restart, probs.takeover, probs.peer_down, probs.peer_up, probs.set_change,
        probs.write, probs.del, probs.read, probs.sync, probs.wait,
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

void chaos_t::step()
{
    reap();
    // A delete the model client knows it issued but which may not have been
    // complete (redel_pending) is repeated before anything else
    if (inflight.size() < probs.max_inflight)
    {
        for (int obj = 0; obj < OBJ_COUNT; obj++)
        {
            if (busy[obj] || !pg_active() || !redel_pending(obj))
                continue;
            submit_delete(obj);
            // No early return: events must keep firing while a deletion is
            // repeated - closing the window often needs a dead member back
            break;
        }
    }
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
    if (action == ACT_TAKEOVER)
    {
        do_takeover();
        return;
    }
    if (action == ACT_PEER_DOWN)
    {
        do_peer_down();
        return;
    }
    if (action == ACT_PEER_UP)
    {
        do_peer_up();
        return;
    }
    if (action == ACT_SET_CHANGE)
    {
        do_set_change();
        return;
    }
    bool submitted = false;
    if (inflight.size() < probs.max_inflight)
    {
        switch(action)
        {
            case ACT_WRITE:  submitted = try_write(false); break;
            case ACT_DELETE: submitted = try_write(true); break;
            case ACT_READ:   submitted = try_read(); break;
            case ACT_SYNC:   submitted = true; submit_sync(); break;
            default:         break;
        }
    }
    if (submitted)
        return;
    if (inflight.size() >= probs.max_inflight)
    {
        // The queue is full - wait for room. Nothing completing at all within the step
        // budget is the only way a hang shows up, so this is where it's caught
        if (!run_until([this]()
            {
                for (auto *p: inflight)
                    if (p->done)
                        return true;
                return false;
            }, MAX_STEPS))
            report_deadlock();
        return;
    }
    cluster.service_peers();
    sim->step();
}

void chaos_t::reap()
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

std::vector<int> chaos_t::free_objects(bool must_exist)
{
    std::vector<int> res;
    for (int obj = 0; obj < OBJ_COUNT; obj++)
    {
        if (!busy[obj] && (!must_exist || exists(obj)))
            res.push_back(obj);
    }
    return res;
}

bool chaos_t::try_write(bool del)
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

bool chaos_t::try_read()
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
    p->op = make_rw_op(OSD_OP_READ, OBJ_OFFSET(obj) + offset, len);
    if (trace)
        printf("read obj%d %ju+%ju", obj, offset, len);
    submit(p);
    return true;
}

void chaos_t::do_crash()
{
    sim->power_loss();
    crashes++;
    for (auto *p: inflight)
        account(p);
    // A delete that was in flight at the crash may have applied on some roles
    // and on none - the object sits INCOMPLETE in the OSD until a complete
    // delete lands. Its OPTIONAL version above the last clean delete is
    // itself the open window that arms the repeat (redel_pending) - there is
    // nothing to carry over
    // Acked-uncommitted versions are at the replay's mercy only with a volatile
    // cache: with immediate_commit=all every ack is journaled durable, a crash
    // takes nothing that was acknowledged, and those versions keep their
    // promise through the outage. Demote them once and for all - a later SYNC
    // must not make a lost version forceful. If some chain still carries their
    // bytes, world_check explains that world through the claim path; if the
    // bytes are gone, nothing may demand them
    if (use_fsync)
    {
        for (int obj = 0; obj < OBJ_COUNT; obj++)
        {
            for (auto & kv: objs[obj].vers)
            {
                if (kv.second.state == V_ACKED)
                    kv.second.state = V_OPTIONAL;
                // (SUBMITTED is skipped: its op hasn't landed; account()
                // resolves it from the reply)
            }
        }
    }
    // From here on the pre-crash acks are unverifiable: a SYNC on the fresh OSD
    // session proves nothing about them, so promotions freeze until the recovery
    // reads below have reconciled every object
    in_crisis = true;
    destroy();
    forget_inflight(true);
    start_osd();
    // Commit everything the journal replay left dirty before reconciling, so the
    // recovery reads below see the settled chain instead of a half-flushed PG
    force_sync();
    if (chance(probs.p_post))
    {
        settle();
        for (int obj = 0; obj < OBJ_COUNT; obj++)
        {
            if (!check_recovered(obj, "a power outage"))
            {
                // The PG came back without quorum; the guarantee can only be
                // verified once some peer is revived
                recover_pending.insert(obj);
            }
        }
    }
    else
    {
        // Keep operating against the settling PG: ops submitted now queue
        // behind its flush_actions and race the repeer. The recovery reads
        // ride over to the next settle as deferred checks
        for (int obj = 0; obj < OBJ_COUNT; obj++)
            recover_pending.insert(obj);
    }
    // Every object is reconciled (or parked in recover_pending with its own gate)
    in_crisis = false;
}

void chaos_t::forget_inflight(bool reap_unfinished)
{
    for (auto *p: inflight)
    {
        // Never let a done-but-unaccounted op cross a restart boundary
        if (p->done)
            free_op(p);
        else if (reap_unfinished)
        {
            // The instance that owned them is already destroyed; nobody
            // will ever complete them, but their memory is still ours
            reap_op(p->op);
            p->op = NULL;
            delete p;
        }
    }
    inflight.clear();
    for (int obj = 0; obj < OBJ_COUNT; obj++)
        busy[obj] = 0;
}

void chaos_t::drain()
{
    if (inflight.size())
    {
        if (!run_until([this]()
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

void chaos_t::report_deadlock()
{
    fprintf(stderr, "step %ju: operations did not complete in %d steps at t=%juus - deadlock?\n",
        step_no, MAX_STEPS, sim->now_us);
    for (auto *p: inflight)
    {
        if (!p->done)
        {
            fprintf(stderr, "  stuck: %s obj%d %ju+%ju retval=%jd\n", op_name(p),
                p->obj, (uint64_t)p->op->req.rw.offset, (uint64_t)p->op->req.rw.len,
                (int64_t)p->op->reply.hdr.retval);
        }
    }
    if (f.has_pg(POOL_ID, PG_NUM))
    {
        pg_t & pg = f.pg(POOL_ID, PG_NUM);
        fprintf(stderr, "  pg %u: state=%x inflight=%d flush_batch=%d flush_actions=%zu\n",
            PG_NUM, pg.state, pg.inflight, pg.flush_batch != NULL, pg.flush_actions.size());
    }
    if (bs)
        bs->dump_diagnostics();
    fflush(stdout);
    abort();
}

void chaos_t::account(pending_t *p)
{
    for (int o: p->touched)
        busy[o]--;
    if (p->op->req.hdr.opcode == OSD_OP_SYNC)
    {
        // A sync that completed normally has synced its snapshot. One that was
        // refused (journal pressure) or hit an inactive PG synced nothing
        if (p->done && p->op->reply.hdr.retval >= 0 && p->on_success)
            p->on_success();
        else if (p->done)
            refused_ops++;
        return;
    }
    if (p->op->req.hdr.opcode == OSD_OP_READ)
    {
        // A read interrupted by an outage tells us nothing
        if (p->done)
            check_read_result(p);
        return;
    }
    int64_t want = p->op->req.hdr.opcode == OSD_OP_WRITE ? (int64_t)p->op->req.rw.len : 0;
    bool ok = p->done && p->op->reply.hdr.retval == want;
    if (p->done && !ok)
    {
        int64_t rv = p->op->reply.hdr.retval;
        // -EAGAIN: an admission refusal (journal pressure) - the op never
        // fanned out, its bytes exist nowhere: DEAD. Only the first storage
        // ever answers this; the current one doesn't. -EPIPE: the PG is
        // repeering, without quorum or this OSD isn't its primary now: no
        // acknowledged effect, but the write may have partially reached the
        // replicas, so its content stays a legal outcome below. -EIO is not
        // a refusal: the OSD only gives it for an object it deems INCOMPLETE,
        // which has exactly one legal explanation - see below
        if (rv == -EAGAIN)
        {
            refused_ops++;
            objs[p->obj].vers[p->version].state = V_DEAD;
            return;   // never fanned out - born SUBMITTED, terminal DEAD
        }
        if (rv == -EPIPE)
        {
            refused_ops++;
            if (objs[p->obj].vers[p->version].state == V_SUBMITTED)
                objs[p->obj].vers[p->version].state = V_OPTIONAL;
            // The op's fate is now known: SUBMITTED -> OPTIONAL.
            // A refusal isn't an ack, but the write's subops may already have
            // applied on some or all of the set (a replicated one even through
            // mark_partial_write). The recorded delta stays in the model's
            // version list, where world_check keeps it a legal outcome forever
        }
        else if (rv == -EIO && unfinished_delete(p->obj))
        {
            // The OSD deems the object INCOMPLETE - legal exactly while a
            // delete the model owes is unfinished: the open window arms the
            // repeat by itself (redel_pending). Rewriting over an unfinished
            // delete would paper over the resurrection, so nothing else here
            refused_ops++;
            if (objs[p->obj].vers[p->version].state == V_SUBMITTED)
                objs[p->obj].vers[p->version].state = V_OPTIONAL;
        }
        else
        {
            fprintf(stderr, "%s of obj%d %ju+%ju failed with retval=%jd\n", op_name(p),
                p->obj, (uint64_t)p->op->req.rw.offset, (uint64_t)p->op->req.rw.len, rv);
            dump_model(p->obj);
            dump_peers(p->obj);
            abort();
        }
    }
    auto & m = objs[p->obj];
    if (ok)
    {
        m.vers[p->version].state = V_ACKED;   // also resolves SUBMITTED
        // An acknowledged write or delete reached every live member: the
        // primary fans it out to the whole set and any failed subop fails
        // the operation, so the placement of the acknowledged version is
        // exactly the live set it was committed against. The event gates
        // defend a chain from these records, walking each object's versions
        // top-down; below an acknowledged deletion there is nothing left to
        // defend, and quorum_kept() stops there
        m.vers[p->version].ack_live = live_set();
        // Commit the operation into the model: an acknowledged write is durable
        // at once with immediate_commit=all, an acknowledged delete records the
        // floor below which everything is gone
        if (p->on_success)
            p->on_success();
    }
    // The OSD spells out an incomplete deletion with OSD_DEL_LEFT_ON_DEAD: a copy
    // on a member it couldn't reach (dead, or a history member that doesn't
    // answer) was cleared by nobody, and the object may come back as that copy.
    // Same story when the delete is acked while the set is smaller than the PG's
    // history: copies outside the set survived it. Then the world where the last
    // deletion is absent from the serving chain stays legal (world_check's second
    // pass), and the client's remedy is to repeat the deletion until one comes
    // back complete over a healthy set covering the whole history. A refused
    // delete is no ack at all: its subops may have applied on some members, and
    // while a peer is down its unreachable copy may be exactly what survived -
    // arm the repeats; a later clean ack of a rewritten object says nothing
    // about what the dead peer kept
    if (p->op->req.hdr.opcode == OSD_OP_DELETE && p->done)
    {
        bool lod = ok && (p->op->reply.del.flags & OSD_DEL_LEFT_ON_DEAD) != 0;
        if (lod)
        {
            // A copy the delete couldn't reach may keep serving the world from
            // before it - nothing above it can be demanded of a read. The flag
            // is the OSD's own confession of incompleteness: for EC (where a
            // survivor makes the item INCOMPLETE) the window stays open on the
            // flag alone. For replicated a stale survivor is harmless old
            // history and the flag is PG-wide - nothing to pin
            if (is_ec)
                mark_partial(p->obj);
        }
        else if (ok)
        {
            // A delete acked without the flag is complete as far as the OSD
            // can know - and the model knows no more than the client does:
            // no inspecting peer stores behind the OSD's back. The window
            // closes; a later INCOMPLETE over a copy the OSD claims doesn't
            // exist is a finding, not something to paper over
            close_partials(p->obj);
        }
        // A refused delete (EPIPE) needs no bookkeeping: its version stays
        // OPTIONAL above the last clean delete - the open window that keeps
        // the old world legal (delete_window) and arms the repeat
        // (redel_pending). Refusals arm unconditionally - the set the OSD
        // failed against can differ from the model's view (it drops peer
        // connections on its own, and a refused delete against a member the
        // model still considers up is applied on the rest exactly like a
        // refused one against a known-dead peer)
    }
}

const char *chaos_t::op_name(pending_t *p)
{
    switch(p->op->req.hdr.opcode)
    {
        case OSD_OP_DELETE: return "delete";
        case OSD_OP_SYNC: return "sync";
        case OSD_OP_READ: return "read";
        default: return "write";
    }
}

std::vector<uint8_t> chaos_t::read_object(int obj, int *retval)
{
    auto *p = new pending_t();
    p->obj = obj;
    p->op = make_rw_op(OSD_OP_READ, OBJ_OFFSET(obj), OBJ_SIZE);
    enqueue(p);
    if (!run_until([p]() { return p->done; }, MAX_STEPS))
    {
        fprintf(stderr, "step %ju: a read of object %d did not complete in %d steps "
            "at t=%juus - deadlock?\n", step_no, obj, MAX_STEPS, sim->now_us);
        if (bs)
            bs->dump_diagnostics();
        fflush(stdout);
        abort();
    }
    *retval = p->op->reply.hdr.retval;
    last_read_version = p->op->reply.rw.version;
    std::vector<uint8_t> res;
    if (p->op->reply.hdr.retval == OBJ_SIZE)
    {
        auto buf = (uint8_t*)p->op->buf;
        if (!last_read_version)
        {
            // A read of a vanished object comes back as zeros with version 0.
            // Nonzero bytes under version 0 are a "shadow": the primary rolled
            // its whole chain back in a crash and resolves the version as gone
            // while peers still serve content - or a resurrection of a deleted
            // object. Data for an object the model still thinks exists is
            // judged by check_recovered against the retained versions; a model
            // that saw the object deleted (acked, or the delete in flight) must
            // instead run the client's remedy, so open the resurrection window
            // and requeue the delete. This is often the first sighting of a
            // copy the model never predicted (its delete had looked clean, or
            // peering re-merged stale versions since)
            auto & m = objs[obj];
            bool nonzero_data = false;
            for (int i = 0; !nonzero_data && i < OBJ_SIZE; i++)
                nonzero_data = buf[i] != 0;
            bool resurrected = nonzero_data && !final_checks &&
                (delete_window(obj) || (m.last_delete() > 0 && !exists(obj)));
            if (resurrected)
            {
                // Pin the deletion as partial: the open window both makes the
                // resurrected world legal and arms the repeat (redel_pending)
                mark_partial(obj);
            }
            last_read_bitmap.assign(BMP_SIZE, 0);
        }
        else
        {
            if (p->op->reply.rw.bitmap_len != BMP_SIZE)
            {
                fprintf(stderr, "read of object %d returned bitmap_len=%u, expected %d\n",
                    obj, p->op->reply.rw.bitmap_len, BMP_SIZE);
                abort();
            }
            last_read_bitmap.assign((uint8_t*)p->op->bitmap, (uint8_t*)p->op->bitmap + BMP_SIZE);
        }
        res.assign(buf, buf + OBJ_SIZE);
    }
    else
        last_read_bitmap.assign(BMP_SIZE, 0);
    delete p->op;
    delete p;
    return res;
}

// Fold a conclusive whole-object read into the version states. The read only
// speaks for one chain (replicated reads are served by a single member): a
// version whose range the answer fully covers, with a newer version answering,
// and no sign of it anywhere in the range, is DEAD - a repeer rolls unstable
// chains back in whole ranges, so it may never show again. A version seen with
// its own bytes is alive: OPTIONAL comes back under the session guarantee.
// Absence is conclusive only over the version's whole range with no holes, no
// lag (a chain that never got the write may serve the world from before it),
// no pending deletion world, and only when the answer actually showed a newer
// version somewhere (max_seen)
void chaos_t::observe(int obj, const std::vector<uint8_t> & got, uint64_t max_seen)
{
    auto & m = objs[obj];
    // Positive fold first, for every scheme: bytes of an OPTIONAL version seen
    // anywhere resolve its uncertainty - the write IS applied, as a whole, not
    // just at the granule that showed. An event may legitimately undo it:
    // every crash over a volatile cache and every repeer rollback rolls
    // unstable versions back, and demote_unstable() returns such a version to
    // OPTIONAL - where it can show again and be resolved again. With
    // immediate_commit=all nothing rolls an acknowledged version back, so the
    // promotion is final there. Presence needs no freshness gate (a stale
    // block shows bytes a chain really carries), but an unreconciled object
    // after a crash is still skipped - the same conservative carve-out the
    // negative fold below applies
    // With a volatile cache observation is never re-establishment: a repeer
    // over a dead peer rolls unstable versions back even when the reading
    // session wrote nothing and survives untouched (drop_dirty_pg_connections
    // only kills sessions that are dirty on this PG), so unsynced bytes that
    // show carry no promise - re-promoting from them would let an "established"
    // version vanish with no demoting event. Only an ack delivered on a live
    // session, or a SYNC, establishes. With immediate_commit=all nothing rolls
    // back and the promotion stands
    if (!use_fsync && got.size() == OBJ_SIZE)
    {
        for (auto & kv: m.vers)
        {
            if ((kv.second.state != V_OPTIONAL && kv.second.state != V_SUBMITTED) ||
                kv.second.len == 0)
                continue;
            for (uint64_t pos = kv.second.off;
                 pos + sizeof(payload_t) <= kv.second.off + kv.second.len;
                 pos += sizeof(payload_t))
            {
                const payload_t *rec = (const payload_t*)(got.data() + pos);
                if (rec->obj == (uint64_t)obj && rec->seq == kv.first)
                {
                    kv.second.state = V_ACKED;
                    // A version promoted below the fold watermark changes
                    // the cached fold - it skipped it when folding
                    if (kv.first <= m.full_cache_ver)
                        m.full_cache_ver = (uint64_t)-1;
                    break;
                }
            }
        }
    }
    // EC answers are RS-reconstructed across per-role chains that legitimately
    // differ: which member's chain answered which granule can't be attributed,
    // so presence/absence proves nothing about a single chain
    if (is_ec)
        return;
    if (delete_window(obj))
        return;   // the surviving-copy world makes presence/absence unprovable
    // With a volatile write cache a crash can take a version's data while its
    // journal stamp survives: the read then serves the block's stale bytes -
    // an older world of this object, explained by the claim path and legal to
    // show until something rewrites the block. So while any rollback-broken
    // (OPTIONAL) version stands above the last delete, or the object isn't
    // reconciled yet, the model can't tell a stale-block answer from
    // committed loss - no fold
    if (use_fsync && recover_pending.count(obj))
        return;
    if (use_fsync)
    {
        uint64_t ld = m.last_delete();
        for (auto & kv: m.vers)
            if ((kv.second.state == V_OPTIONAL || kv.second.state == V_SUBMITTED) &&
                kv.first > ld)
                return;
    }
    static const payload_t zero = {};
    // The strict verdict: the answer is entirely ACKED/SYNCED content. A chain
    // that legitimately lags behind the acknowledged one was rolled back by a
    // crash or a repeer, and every such event demotes the ACKED versions to
    // OPTIONAL - so an ACKED/SYNCED la with only ACKED/SYNCED bytes shown can
    // only come from the acknowledged chain itself: every granule la wrote
    // must show it, anything else is committed data lost. A delete
    // acknowledged after the last write excuses absence; a delete still in
    // flight may already have applied on the primary and legitimately answers
    // zeros (linearizability), so it's not judged either
    const uint64_t la = m.last_acked();
    bool del_inflight = false;
    for (auto & kv: m.vers)
        if (kv.first > la && (kv.second.state == V_OPTIONAL ||
            kv.second.state == V_SUBMITTED) && kv.second.len == 0)
            del_inflight = true;
    if (got.size() == OBJ_SIZE && la &&
        !del_inflight &&
        // ACKED or SYNCED: the ack reached every chain (or SYNC sealed it
        // after the last membership event); LAGGY/STALE chains may miss it,
        // OPTIONAL/DEAD lost the promise
        (m.vers.at(la).state == V_ACKED || m.vers.at(la).state == V_SYNCED) &&
        m.vers.at(la).len > 0)
    {
        bool all_strict = true;
        for (uint64_t pos = 0; pos + sizeof(payload_t) <= got.size();
             pos += sizeof(payload_t))
        {
            const payload_t *rec = (const payload_t*)(got.data() + pos);
            if (!memcmp(rec, &zero, sizeof(payload_t)))
                continue;
            auto vit = m.vers.find(rec->seq);
            if (vit == m.vers.end() ||
                vit->second.state == V_OPTIONAL || vit->second.state == V_DEAD ||
                vit->second.state == V_SUBMITTED)
            {
                all_strict = false;
                break;
            }
        }
        if (all_strict)
        {
            const delta_t & ad = m.vers.at(la);
            for (uint64_t pos = ad.off; pos + sizeof(payload_t) <= ad.off + ad.len;
                 pos += sizeof(payload_t))
            {
                const payload_t *rec = (const payload_t*)(got.data() + pos);
                // A newer in-flight version legitimately overwrites the acked
                // bytes: linearizability, not loss, and so do bytes of a version
                // that never acked (a partially applied failed write can race a
                // read). Anything else means acknowledged bytes vanished from
                // the strict chain
                if (rec->seq >= la)
                    continue;
                auto vit = m.vers.find(rec->seq);
                if (vit == m.vers.end() || vit->second.state == V_OPTIONAL ||
                    vit->second.state == V_SUBMITTED)
                    continue;
                fprintf(stderr, "obj%d read shows seq=%ju over acked v%ju's granule "
                    "%ju - committed data lost\n", obj, rec->seq, la, pos);
                dump_model(obj);
                dump_peers(obj);
                abort();
            }
        }
    }
    // A DEAD version showing again would be a rolled-back fragment
    // recommitting - an OSD bug, not a legal world
    for (uint64_t pos = 0; pos + sizeof(payload_t) <= got.size(); pos += sizeof(payload_t))
    {
        const payload_t *rec = (const payload_t*)(got.data() + pos);
        if (!memcmp(rec, &zero, sizeof(payload_t)))
            continue;
        auto vit = m.vers.find(rec->seq);
        if (vit != m.vers.end() && vit->second.state == V_DEAD)
        {
            fprintf(stderr, "obj%d DEAD version %ju showed again at offset %ju - a "
                "rolled-back fragment recommitted\n", obj, rec->seq, pos);
            dump_model(obj);
            dump_peers(obj);
            abort();
        }
    }
    for (auto & kv: m.vers)
    {
        // Fold a recorded version against this answer. Judgment is RANGE-based,
        // not global: a version is judged by what its own range shows, even if
        // newer bytes appear elsewhere in the object (a 0-4K write fully shown
        // as old bytes is DEAD even when a 4-8K write answered - max_seen
        // proves nothing about this range). Only versions that MUST be seen are
        // judged: a never-acked hole (empty ack_live) owes nothing to any read
        // - its absence from an old world proves nothing; below the last acked
        // delete there's nothing to demand
        auto & d = kv.second;
        // V_SUBMITTED: the op hasn't landed - reads racing it may show or hide
        // its bytes by linearizability, so its absence proves nothing. Only a
        // terminal answer (ack -> ACKED, refusal -> OPTIONAL) makes its bytes
        // demanded or disclaimed; the fold judges OPTIONAL from then on
        if (d.len == 0 || kv.first <= m.last_delete() ||
            d.state == V_SUBMITTED)
            continue;
        bool present = false;
        bool covered = true;
        for (uint64_t pos = d.off; pos + sizeof(payload_t) <= d.off + d.len;
             pos += sizeof(payload_t))
        {
            const payload_t *rec = (const payload_t*)(got.data() + pos);
            if (rec->seq == kv.first)
            {
                present = true;
                break;
            }
            // Bytes of a NEWER version: its range is superseded, its absence
            // can't be judged from this read. A hole means the answer may be
            // partial - neither is conclusive
            if (rec->seq > kv.first || !memcmp(rec, &zero, sizeof(payload_t)))
            {
                covered = false;
                if (rec->seq > kv.first)
                    break;
            }
        }
        if (present)
        {
            if ((d.state == V_OPTIONAL || d.state == V_SUBMITTED) && !use_fsync)
            {
                d.state = V_ACKED;
                // A version promoted below the fold watermark changes the
                // cached fold: it skipped this version when folding
                if (kv.first <= m.full_cache_ver)
                    m.full_cache_ver = (uint64_t)-1;
            }
        }
        else if (covered)
        {
            // covered: the range shows ONLY bytes of older versions - the
            // answer claims a world strictly below this version. The chain
            // rule then judges top-down: every version whose range shows an
            // older world and whose bytes must be seen is gone - established
            // ones (SYNCED/ACKED) abort as committed loss, demoted ones
            // (OPTIONAL with ack history) are proven rolled back: DEAD
            // A committed version read fully absent over its whole range, with
            // a newer version answering, is committed data lost
            if (d.state == V_SYNCED)
            {
                fprintf(stderr, "obj%d committed version %ju read fully absent over "
                    "its range %ju+%ju while seq %ju showed - committed data lost\n",
                    obj, kv.first, d.off, d.len, max_seen);
                dump_model(obj);
                dump_peers(obj);
                abort();
            }
            // An ACKED version was established by an ack or a positive
            // observation, and only an event can undo it - and every event
            // (a crash over the volatile cache, a repeer rollback, a session
            // kill) demotes ACKED versions to OPTIONAL before anything reads
            // from the re-settled chain. Covered absent over its range while
            // still ACKED means established bytes vanished with no event in
            // between: a non-repeatable read
            if (d.state == V_ACKED)
            {
                fprintf(stderr, "obj%d established version %ju read absent over its "
                    "range %ju+%ju while seq %ju showed, with no demoting event "
                    "since - repeatable read broken\n",
                    obj, kv.first, d.off, d.len, max_seen);
                dump_model(obj);
                dump_peers(obj);
                abort();
            }
            if (d.state != V_DEAD)
                d.state = V_DEAD;
        }
    }
}

void chaos_t::check_read_result(pending_t *p)
{
    int obj = p->obj;
    auto & m = objs[obj];

    // Reads that arrived while the PG was repeering or without quorum simply
    // bounce. -EIO means the OSD deems the EC object INCOMPLETE: legal only
    // while an unfinished delete stands behind it (the resurrected copy of
    // an incomplete deletion) - the remedy is to finish deleting, never to
    // rewrite. No other INCOMPLETE is excused - neither a thin pool nor a PG
    // missing a member: outside the delete window it's an OSD bug
    int64_t rv = p->op->reply.hdr.retval;
    if (rv < 0 && rv != -ENOENT)
    {
        if (rv == -EAGAIN || rv == -EPIPE || rv == -ERANGE)
        {
            // -ERANGE: shards answer under diverged fact_versions (crash
            // rolled chains back unevenly); the merge refuses and the client
            // retries - legal while the object is not yet re-agreed
            refused_ops++;
            return;
        }
        if (rv == -EIO)
        {
            if (unfinished_delete(obj) && !final_checks)
            {
                refused_ops++;
                mark_partial(obj);
                return;
            }
            fprintf(stderr, "read of object %d failed with -EIO: the OSD deems "
                "it INCOMPLETE, but no unfinished delete stands behind it\n"
                "  unfinished=%d final=%d\n", obj,
                (int)unfinished_delete(obj), (int)final_checks);
            dump_model(obj);
            dump_peers(obj);
            abort();
        }
        fprintf(stderr, "read of object %d failed with retval=%jd\n", obj, rv);
        dump_model(obj);
        dump_peers(obj);
        abort();
    }
    uint64_t offset = p->op->req.rw.offset % OBJ_SIZE, len = p->op->req.rw.len;
    if (!exists(obj))
    {
        // The OSD answers reads of a clean vanished object with zeros and version 0
        // (ENOENT is converted to a hole); a bare -ENOENT would be fine too
        int64_t retval = p->op->reply.hdr.retval;
        bool zeroed = retval == (int64_t)len && !p->op->reply.rw.version;
        if (zeroed)
        {
            for (uint64_t i = 0; i < len; i++)
            {
                if (((uint8_t*)p->op->buf)[i])
                {
                    zeroed = false;
                    break;
                }
            }
        }
        if (retval == -ENOENT || zeroed)
        {
            // The served chain is deleted - that's this quorum's answer, not
            // a completeness verdict: an earlier LOD ack said copies exist
            // that this read didn't reach. Only a delete acked WITHOUT the
            // flag closes the window (account()); an absent read just agrees
            // with the served world and keeps the repeat armed
            return;
        }
        // Content under a deletion. It's either a copy the delete couldn't
        // reach (its world lacks the deletion - legal while it's explainable
        // as recorded versions) or corruption. Either way it's evidence for
        // the client: arm the repeated deletion and mark the surviving-copy
        // world as legal, then demand that the bytes are a consistent merge
        // of versions the model actually issued
        if (retval > 0)
        {
            if (!final_checks)
            {
                mark_partial(obj);
            }
            std::vector<uint8_t> got((uint8_t*)p->op->buf, (uint8_t*)p->op->buf + retval);
            if (world_check(obj, got, offset, retval, "a read under a deletion", is_ec) >= 0)
            {
                checked_reads++;
                return;
            }
            // EC reads RS-reconstruct across members whose chains diverged, so
            // the answer can mix granules of chains no single member has
            if (is_ec && delete_window(obj) && !final_checks)
            {
                checked_reads++;
                return;
            }
        }
        dump_model(obj);
        if (retval > 0)
        {
            auto & g = *(payload_t*)p->op->buf;
            fprintf(stderr, "  read returned seq=%ju obj=%ju offset=%ju\n",
                g.seq, g.obj, g.offset);
        }
        fprintf(stderr, "read of %s object %d returned retval=%jd v%ju\n",
            m.last_acked() ? "deleted" : "never-written", obj, retval,
            p->op->reply.rw.version);
        abort();
    }
    std::vector<uint8_t> got;
    std::vector<uint8_t> got_bmp(BMP_SIZE, 0);
    if (p->op->reply.hdr.retval == (int64_t)len)
    {
        got.assign((uint8_t*)p->op->buf, (uint8_t*)p->op->buf + len);
        if (p->op->reply.rw.bitmap_len != BMP_SIZE)
        {
            fprintf(stderr, "async read of object %d returned bitmap_len=%u, expected %d\n",
                obj, p->op->reply.rw.bitmap_len, BMP_SIZE);
            abort();
        }
        got_bmp.assign((uint8_t*)p->op->bitmap, (uint8_t*)p->op->bitmap + BMP_SIZE);
    }
    // The answer must be explainable as the merge of one consistent subset of
    // the recorded versions: everything up to durable applied (committed data
    // can't vanish behind a hole or under older bytes), optional versions
    // applied or not, and each granule showing the highest applied version
    // that covers it - byte-identical to the delta exactly as it was issued
    uint64_t max_seen = 0;
    if (p->op->reply.hdr.retval == (int64_t)len &&
        world_check(obj, got, offset, len, "read", is_ec, &max_seen) >= 0)
    {
        // Data can't come out of a hole; the reverse (a set bit over zeros) is
        // legal. Content-based, so it holds in every world
        check_bitmap(obj, got, offset, got_bmp, "read");
        checked_reads++;
        // A whole-object read covers every version's range: its observation is
        // conclusive. Partial reads can't say "absent over the whole range"
        if (offset == 0 && len == OBJ_SIZE)
            observe(obj, got, max_seen);
        return;
    }
    const std::vector<uint8_t> & acked_state = base_content(obj);
    std::vector<uint8_t> want(acked_state.begin() + offset,
        acked_state.begin() + offset + len);
    fprintf(stderr, "read of object %d v%ju at %ju+%ju returned retval=%jd, content %s\n",
        obj, m.last_acked(), offset, len, (int64_t)p->op->reply.hdr.retval,
        p->op->reply.hdr.retval == (int64_t)len ? "mismatch" : "n/a");
    dump_model(obj);
    report_mismatch(obj, got, want, offset, &got_bmp);
    abort();
}

void chaos_t::check_object(int obj)
{
    auto *p = new pending_t();
    p->obj = obj;
    p->op = make_rw_op(OSD_OP_READ, OBJ_OFFSET(obj), OBJ_SIZE);
    enqueue(p);
    if (!run_until([p]() { return p->done; }, MAX_STEPS))
    {
        fprintf(stderr, "the final read of object %d did not complete\n", obj);
        abort();
    }
    check_read_result(p);
    free_op(p);
}

// Hand the PG over: command the running instance to give it up, wait for the
// exact moment the state key disappears, and let a fresh instance capture it.
// The production stop_pg drains or cancels everything in flight and the state
// report deletes the /pg/state key (PG_OFFLINE with us still recorded as the
// primary). The fake target never claims the PG, so it sits unclaimed until
// the config points the primary back at us and the new instance takes it with
// a fresh lease - the full release->claim sequencing a real takeover runs
void chaos_t::do_takeover()
{
    if (!f.osd || !f.has_pg(POOL_ID, PG_NUM) || cur_set.size() < 2)
        return;
    osd_num_t to = cur_set[1 + rnd(0, cur_set.size()-2)];
    if (trace)
        printf("handover: primary -> %ju\n", (uint64_t)to);
    write_pg_config(to);
    std::string state_key = f.st_cli->etcd_prefix + "/pg/state/" +
        std::to_string(POOL_ID) + "/" + std::to_string(PG_NUM);
    // Wait exactly for the release: the state key goes away with the report
    // transaction completing the stop. Until then the world keeps turning -
    // queued subops answer, in-flight ops complete or die with the dropped
    // session (the session-death hook demotes the versions the new instance's
    // peering is about to roll back)
    if (!run_until([this, &state_key]()
        { return !f.etcd_store.data.count(state_key); }, MAX_STEPS))
    {
        fprintf(stderr, "step %ju: the OSD did not release the PG in %d steps\n",
            step_no, MAX_STEPS);
        report_deadlock();
    }
    // Land everything stop_pg completed or cancelled, exactly like a crash
    // landing does: an acked write that finished before the stop is recorded
    // as optional below (volatile); an unfinished delete stays OPTIONAL -
    // the open window itself arms the repeat (redel_pending)
    for (auto *p: inflight)
        account(p);
    forget_inflight();
    // stop_pg dropped the session whose unsynced writes the new instance's
    // peering is about to roll back - if it did. Polling here (the key-wait
    // stopped pumping the instant the key vanished) demotes exactly the
    // versions the session death covers; rolling back under a live session
    // stays a bug the later reads will catch
    check_session_death();
    takeovers++;
    // The disk may still be chewing on the journal flushes of a stop that just
    // finished; destroying the blockstore under a live SQE reads freed buffers
    // on the next disk step - let it go idle first, like a clean restart does
    if (!run_until([this]() { return !sim->has_inflight(); }, MAX_STEPS))
    {
        fprintf(stderr, "step %ju: the disk did not go idle before a handover\n", step_no);
        abort();
    }
    destroy();
    start_osd();
    // The config still points the primary at the fake member, which never
    // claims: the new instance ignored the PG. Point it back - the new
    // instance captures the unclaimed PG, peers and rolls back what peers
    // report as unstable
    write_pg_config();
    maybe_settle();
}

void chaos_t::do_restart()
{
    drain();
    restarts++;
    std::vector<std::vector<uint8_t>> before(OBJ_COUNT);
    std::vector<uint64_t> before_ver(OBJ_COUNT, 0);
    std::vector<int> before_rv(OBJ_COUNT, 0);
    bool verify = chance(probs.p_post);
    if (!use_fsync && verify)
    {
        for (int obj = 0; obj < OBJ_COUNT; obj++)
        {
            before[obj] = read_object(obj, &before_rv[obj]);
            before_ver[obj] = last_read_version;
        }
    }
    // Let everything in flight finish right before pulling the OSD out from under
    // it, or an unfinished write would later execute against freed buffers
    if (!run_until([this]() { return !sim->has_inflight(); }, MAX_STEPS))
    {
        fprintf(stderr, "step %ju: disk did not go idle before a clean restart\n", step_no);
        abort();
    }
    destroy();
    forget_inflight();
    start_osd();
    if (!use_fsync && verify && pg_healthy())
    {
        for (int obj = 0; obj < OBJ_COUNT; obj++)
        {
            int rv = 0;
            auto after = read_object(obj, &rv);
            // A deleted object whose deletion raced a peer outage, or whose old
            // copy sat on a member the delete never reached, can read back as its
            // resurrected copy after restart peering re-merges the stale versions -
            // not a byte-for-byte loss. Treat a flip to resurrected-looking content
            // as the window opening (the delete itself may have looked clean) and
            // run the client's remedy. A lingering -EIO (INCOMPLETE) is not
            // carved out here: check_recovered below judges it
            if ((is_ec || delete_window(obj)) &&
                (!final_checks && objs[obj].last_delete() > 0 &&
                    (rv > 0 || before_rv[obj] == -ENOENT)))
            {
                if (rv != before_rv[obj] || after != before[obj])
                {
                    mark_partial(obj);
                }
                continue;
            }
            if (rv != before_rv[obj] || after != before[obj])
            {
                // Content or holes under version 0 aren't attributable to a
                // version: a rolled-back chain exposes leftovers, and a repeering
                // or incomplete EC PG answers holes for data that lives on the
                // absent member. Neither is a byte-exact baseline; -ENOENT is a
                // real "gone" answer and stays strict. Without a baseline the
                // honest invariant is the model one, checked unconditionally below
                if (before_rv[obj] > 0 && !before_ver[obj])
                    before_rv[obj] = -EAGAIN;
                // A pre-restart read that didn't complete (EPIPE from a repeer or an
                // outage at that moment) observed no state, so there's no "before" to
                // be byte-exact against.  A read that completed with -ENOENT is a real
                // observation and stays strict.  Without an observed baseline the
                // honest invariant is the model one, checked unconditionally below
                if (before_rv[obj] >= 0 || before_rv[obj] == -ENOENT)
                {
                    fprintf(stderr, "object %d changed across a clean restart that lost nothing: "
                        "before retval=%d v%ju, after retval=%d v%ju\n",
                        obj, before_rv[obj], before_ver[obj], rv, last_read_version);
                    report_mismatch(obj, after, before[obj]);
                    abort();
                }
            }
        }
    }
    if (verify)
    {
        for (int obj = 0; obj < OBJ_COUNT; obj++)
        {
            if (!check_recovered(obj, "a clean restart"))
                recover_pending.insert(obj);
        }
    }
    else
    {
        // No wait after the restart: the model checks ride over to the next settle
        for (int obj = 0; obj < OBJ_COUNT; obj++)
            recover_pending.insert(obj);
    }
}

bool chaos_t::check_recovered(int obj, const char *what)
{
    auto & m = objs[obj];
    int retval = 0;
    auto got = read_object(obj, &retval);
    if (retval == -EPIPE || retval == -EAGAIN)
        return false;   // no quorum right now - the caller will retry later
    if (retval == -ERANGE)
    {
        // The OSD refuses to merge shards answering under different
        // fact_versions (crash rolled some chains back further than others).
        // A new write over the whole set re-agrees them; the caller retries
        // the reconciliation until then
        return false;
    }
    // -EIO: the OSD deems this object INCOMPLETE. The only legal source is an
    // unfinished delete whose resurrected copy left an incomplete item behind
    // - pin it and defer the verdict until the repeat lands. Nothing else is
    // excused: no thin pool, no incomplete set - an INCOMPLETE verdict
    // outside the delete window is an OSD bug
    if (retval == -EIO)
    {
        if (unfinished_delete(obj))
        {
            mark_partial(obj);
            return false;
        }
        fprintf(stderr, "object %d came back from %s INCOMPLETE (read -EIO) "
            "with no unfinished delete behind it\n", obj, what);
        dump_model(obj);
        dump_peers(obj);
        abort();
    }
    // A vanished object reads back as zeros with version 0, or as -ENOENT.
    // Data under version 0 (the primary rolled back its own chain, peers
    // answer) is judged like any content: it must be explained by versions
    // the model recorded
    bool nonzero = false;
    for (uint64_t i = 0; !nonzero && i < got.size(); i++)
        nonzero = got[i] != 0;
    bool present = retval == OBJ_SIZE && (last_read_version != 0 || nonzero);
    if (retval != OBJ_SIZE && retval != -ENOENT)
    {
        fprintf(stderr, "read of object %d after %s failed with retval=%d "
            "(torn data committed?)\n", obj, what, retval);
        abort();
    }
    if (present)
    {
        // Content coming back over a deletion the model acknowledged (or over
        // one still in flight) is a copy that survived it. That's the world
        // without the deletion - legal to observe, and the client's remedy is
        // to repeat the deletion. Defer the verdict; the repeat will land
        if (deleted(obj) || redel_pending(obj))
        {
            if (deleted(obj))
                mark_partial(obj);
            return false;
        }
        if (last_read_version)
            check_bitmap(obj, got, 0, last_read_bitmap, what);
        uint64_t max_seen = 0;
        int w = world_check(obj, got, 0, got.size(), what, is_ec, &max_seen);
        if (w >= 0)
        {
            // A recovery read of a settled cluster is a conclusive observation.
            // The pending flag must not gate this call: flush_recover_pending
            // checks the object while it's still on the list and erases it
            // only after, so gating here would skip exactly the first
            // conclusive read. The defer that IS justified - a volatile-cache
            // answer of an object not reconciled yet may show a stale block -
            // lives in observe() itself, scoped to use_fsync
            if (!in_crisis)
                observe(obj, got, max_seen);
            m.osd_version = last_read_version;
            // Reality showed more than the plain durable state: an optional
            // version applied (in the whole chain or on some roles only).
            // Nothing to latch: the chain above the last SYNCED is unstable,
            // so the next membership event re-peers the PG and rolls it back
            // to the durable floor, and until then quorum_kept() defends
            // every still-servable version by its own ack placement
            return true;
        }
    }
    else
    {
        // Gone is a legal world when nothing forces content. With no floor
        // lifted (or none surviving the last membership change or outage),
        // no version has ever been acknowledged over a complete placement in
        // a way every serving chain must hold - an empty or stale chain may
        // serve and it reads as gone. With a floor, the highest version
        // forced at some granule decides: absent or a deletion means gone is
        // explainable. Versions above the durable floor are optional by
        // definition - a crash-demoted one may be gone for good, and a delete
        // that never got its ack may still have applied on the members, in
        // which case reality simply chose the deleted world
        if (!m.floor_now())
        {
            m.osd_version = 0;
            return true;
        }
        // Above the highest forced version a delete may sit unforced - not
        // acknowledged, crash-demoted, or acknowledged but lost from the
        // primary's cache in the outage - while its subops applied on every
        // member. That world is gone
        bool top_del = true;
        uint64_t top_v = 0;
        for (auto & kv: m.vers)
        {
            // Only committed (SYNCED) or established-absent (DEAD) versions
            // decide what the settled chain must show; anything else is
            // optional by definition
            if (kv.second.state != V_SYNCED && kv.second.state != V_DEAD)
                continue;
            top_del = kv.second.len == 0;
            top_v = kv.first;
        }
        if (top_del)
        {
            m.osd_version = 0;
            return true;
        }
        for (auto & kv: m.vers)
        {
            if (kv.first > top_v && kv.second.len == 0)
            {
                m.osd_version = 0;
                return true;
            }
        }
    }
    // On a PG that still lacks quorum (or is repeering) the read answer is
    // not the final state of the object - it answers holes for data that
    // lives on the absent member. Defer the verdict
    if (!pg_active())
        return false;
    fprintf(stderr, "object %d came back from %s %s, which no world of the "
        "recorded versions explains (floor=seq %ju, acked=seq %ju, "
        "submitted=seq %ju, OSD reports v%ju)\n", obj, what,
        present ? "holding content" : "gone",
        m.floor_now(), m.last_acked(), m.last_submitted(), last_read_version);
    dump_model(obj);
    if (present)
    {
        const std::vector<uint8_t> & acked_state = base_content(obj);
        report_mismatch(obj, got, acked_state, 0, &last_read_bitmap);
    }
    // Peer stores are diagnostics only - verification never reads them
    for (auto & kv: cluster.peers)
    {
        if (!in_set(kv.first))
            continue;
        fprintf(stderr, "  peer %ju%s chain:", (uint64_t)kv.first,
            kv.second.up ? "" : "(down)");
        for (auto & vv : kv.second.store[OBJ_OFFSET(obj)])
        {
            fprintf(stderr, " v%ju%s bmp=", vv.first, vv.second.stable ? "S" : "u");
            for (auto & c: vv.second.bmp)
                fprintf(stderr, "%02x", c);
        }
        fprintf(stderr, "\n");
    }
    fprintf(stderr, "  recover_pending=%zu\n", recover_pending.size());
    abort();
}

void chaos_t::flush_recover_pending()
{
    for (auto it = recover_pending.begin(); it != recover_pending.end(); )
    {
        if (!pg_active())
            return;
        int obj = *it;
        if (check_recovered(obj, "an outage"))
            it = recover_pending.erase(it);
        else
            ++it;
    }
}

void chaos_t::force_sync()
{
    submit_sync();
    for (int guard = 0; guard < MAX_STEPS; guard++)
    {
        bool waiting = false;
        for (auto *p: inflight)
        {
            if (!p->done)
                waiting = true;
        }
        if (!waiting)
            break;
        cluster.service_peers();
        sim->step();
        reap();
    }
    reap();
}

void chaos_t::run(uint64_t steps)
{
    for (step_no = 0; step_no < steps; step_no++)
        step();
    // Bring the cluster back to full health so the final checks run against a PG
    // with quorum: revive every dead peer and wait for repeering and rollback
    for (auto & kv: cluster.peers)
    {
        kv.second.up = true;
        // only members the OSD may peer with may be reconnected: the current
        // set plus anything the PG's sets (current, prev or historical) still
        // hold. Peers which are up but weren't connected during the run (a
        // history member that came back outside the set) are connected here too
        if (f.osd && (in_set(kv.first) || pg_needs_osd(kv.first)) && !cluster.connected.count(kv.first))
        {
            f.connect_peer(kv.first);
            cluster.connected.insert(kv.first);
        }
    }
    if (!run_until([this]() { return pg_healthy(); }, MAX_STEPS))
    {
        fprintf(stderr, "the PG did not become healthy after reviving every peer\n");
        report_deadlock();
    }
    // Convergence phase: everything the model still owes has to complete here,
    // with the cluster fully healthy and nothing else moving - deferred recovery
    // checks, re-verification of refused writes and repeated deletions for
    // objects in the resurrection window. Only then do the strict checks below
    // mean anything
    int owed = 0;
    for (int guard = 0; guard++ < 200; )
    {
        flush_recover_pending();
        drain();
        owed = 0;
        for (int obj = 0; obj < OBJ_COUNT; obj++)
        {
            if (!redel_pending(obj))
                continue;
            owed++;
            if (!busy[obj])
                submit_delete(obj);
        }
        drain();
        settle();
        if (recover_pending.empty() && owed == 0)
            break;
    }
    if (recover_pending.size() || owed)
    {
        fprintf(stderr, "pending recovery or repeated deletion could not"
            " complete on a healthy PG (recover=%zu redel=%d)\n",
            recover_pending.size(), owed);
        for (int obj = 0; obj < OBJ_COUNT; obj++)
            if (redel_pending(obj))
                fprintf(stderr, "  obj %d: redel=%d deleted=%d\n",
                    obj, (int)redel_pending(obj), (int)deleted(obj));
        abort();
    }
    // From here the surviving-copy worlds count as settled: the repeated
    // deletions have all come back clean, so what the strict checks see
    // through the primary is the state the cluster agreed on
    final_checks = true;
    for (int obj = 0; obj < OBJ_COUNT; obj++)
        check_object(obj);
}

// fake_cluster_t: the responder runs over the driver's simulation (ch), the
// inherited model state and the messenger, so the bodies live after chaos_t
osd_client_t *fake_cluster_t::peer_cl(osd_num_t num)
{
    // The fixture keeps msgr private; track our own connections instead. The OSD
    // may also drop a peer on its own after failed subops, so the fixture's .at
    // can throw - a peer we can't look up is simply not connected anymore
    if (!ch.f.osd || !connected.count(num))
        return NULL;
    try
    {
        return ch.f.peer(num);
    }
    catch(std::out_of_range &)
    {
        connected.erase(num);
        return NULL;
    }
}

static void subop_reply(osd_op_t *op, int64_t retval);

// A wire record owns its payload and attr as separate mallocs; the op's
// own destructor doesn't know about them
static void free_wire_op(wire_op_t & w)
{
    free(w.payload);
    free(w.attr);
    delete w.op;
}

// The instance owning the connections is gone; the kernel still delivers
// bytes sitting on them. Deep-copy every mutating subop - the originals
// live in the memory of the instance about to be freed. Reads and lists
// mutate nothing and would answer to nobody, so they are skipped; a peer
// that's already dead RSTs its sockets and never sees them
void fake_cluster_t::harvest_wire()
{
    for (auto & kv: peers)
    {
        if (!kv.second.up || !connected.count(kv.first))
            continue;
        osd_client_t *cl = peer_cl(kv.first);
        if (!cl)
            continue;
        for (auto & okv: cl->sent_ops)
        {
            osd_op_t *op = okv.second;
            uint64_t opcode = op->req.hdr.opcode;
            uint64_t payload = 0, attr = 0;
            switch (opcode)
            {
                case OSD_OP_SEC_WRITE:
                case OSD_OP_SEC_WRITE_STABLE:
                    payload = op->req.sec_rw.len;
                    attr = op->req.sec_rw.attr_len;
                    break;
                case OSD_OP_SEC_STABILIZE:
                case OSD_OP_SEC_ROLLBACK:
                    payload = op->req.sec_stab.len;
                    break;
                case OSD_OP_SEC_DELETE:
                case OSD_OP_SEC_SYNC:
                    break;
                default:
                    continue;   // a late read or list answers nobody
            }
            if (wire.size() >= 64)
                return; // keep the straggler backlog bounded
            auto *copy = new osd_op_t();
            copy->op_type = OSD_OP_IN;
            copy->osd_num = kv.first;
            copy->req = op->req;
            void *payload_copy = NULL;
            uint8_t *attr_copy = NULL;
            if (payload)
            {
                payload_copy = malloc_or_die(payload);
                memcpy(payload_copy, op->iov.buf[0].iov_base, payload);
                copy->iov.buf[0].iov_base = payload_copy;
                copy->iov.buf[0].iov_len = payload;
            }
            if (attr && op->bitmap)
            {
                attr_copy = (uint8_t*)malloc_or_die(attr);
                memcpy(attr_copy, op->bitmap, attr);
            }
            if (ch.trace)
                printf("wire: peer %ju keeps op %ju after the sender died\n",
                    (uint64_t)kv.first, opcode);
            wire_harvested++;
            wire.push_back((wire_op_t){ .dst = kv.first, .op = copy,
                .payload = payload_copy, .attr = attr_copy });
        }
    }
}

// Two passes per up peer: first apply a random subset of the queued ops (the
// request-path delay), then deliver a random subset of the replies already
// produced (the response-path delay). Unapplied ops and undelivered replies are
// retried on later rounds, so applies and replies arrive in a random order
// after random delays of their own. A reply belongs to the connection it was
// produced on: when that connection dies - a peer crash, a peer the OSD
// dropped - undelivered replies are lost with it and the primary learns about
// the ops with -EPIPE, like from a closed socket. The applies themselves stay:
// crash() only drops what was never fsynced, so the primary can legitimately
// end up with a failed write whose bytes stuck on the peer. After the peers,
// the wire queue of stragglers is served too
void fake_cluster_t::service_peers()
{
    for (auto & kv: peers)
    {
        fake_peer_t & fp = kv.second;
        if (!fp.replying.empty() && (!fp.up || !peer_cl(kv.first)))
        {
            for (osd_op_t *op: fp.replying)
                subop_reply(op, -EPIPE);
            fp.replying.clear();
        }
        if (!fp.up)
            continue;
        osd_client_t *cl = peer_cl(kv.first);
        if (!cl)
        {
            // A peer the OSD dropped itself (failed subops, peering error) is
            // reconnected by the messenger in the background, and the new
            // connection re-peers the PG. There's no TCP here, so retry it the
            // same way: the fixture's connect triggers repeer_pgs for us
            if (ch.f.osd && !connected.count(kv.first) &&
                (ch.in_set(kv.first) || ch.pg_needs_osd(kv.first)))
            {
                ch.f.connect_peer(kv.first);
                connected.insert(kv.first);
            }
            continue;
        }
        std::vector<uint64_t> ids;
        for (auto & op_kv: cl->sent_ops)
            ids.push_back(op_kv.first);
        for (uint64_t id: ids)
        {
            auto it = cl->sent_ops.find(id);
            if (it == cl->sent_ops.end())
                continue;   // an earlier round has already reshuffled the queue
            if (ch.rnd(0, 99) >= (int)probs.p_wire)
                continue;   // apply on a later round
            osd_op_t *op = it->second;
            cl->sent_ops.erase(it);
            fp.apply(op);
            fp.replying.push_back(op);
        }
        // Response pass. A reply callback may free the op and reshuffle
        // anything, so erase it from the queue before delivering
        for (size_t i = 0; i < fp.replying.size(); )
        {
            if (ch.rnd(0, 99) >= (int)probs.p_reply)
            {
                i++;
                continue;   // deliver on a later round
            }
            osd_op_t *op = fp.replying[i];
            fp.replying.erase(fp.replying.begin()+i);
            subop_reply(op, op->reply.hdr.retval);
        }
    }
    // Stragglers: subops that were on the wire when their instance died
    // arrive at the peer in their own time and apply there without an
    // answer - the callback belonged to the dead instance. A peer that died
    // before delivery RSTs the socket instead of getting the bytes, so
    // those records die too
    for (size_t i = 0; i < wire.size(); )
    {
        auto pit = peers.find(wire[i].dst);
        if (pit == peers.end() || !pit->second.up)
        {
            wire_lost++;
            free_wire_op(wire[i]);
            wire.erase(wire.begin()+i);
            continue;
        }
        if (ch.rnd(0, 99) >= (int)probs.p_wire)
        {
            i++;
            continue;   // deliver on a later round
        }
        wire_op_t late = wire[i];
        wire.erase(wire.begin()+i);
        wire_delivered++;
        if (ch.trace)
            fprintf(stderr, "WIRE-DEL peer %ju opcode %ju inode %jx stripe %jx off %ju len %ju\n",
                late.dst, late.op->req.hdr.opcode, late.op->req.sec_rw.oid.inode,
                late.op->req.sec_rw.oid.stripe, (uint64_t)late.op->req.sec_rw.offset,
                (uint64_t)late.op->req.sec_rw.len);
        pit->second.apply(late.op);   // the reply is computed and dropped
        free_wire_op(late);
    }
}

// EC puts the member's role in the low bits of the blockstore stripe
// (oid.stripe = parity-stripe start | role), and an object is exactly one stripe
int fake_cluster_t::obj_of(uint64_t stripe)
{
    return stripe / OBJ_SIZE;
}

uint64_t fake_cluster_t::role_of(uint64_t stripe)
{
    return stripe % OBJ_SIZE;
}

// Complete a subop reply and run the OSD's callback. A copy, because a
// callback that frees the subop must not pull the ground from under us
static void subop_reply(osd_op_t *op, int64_t retval)
{
    op->reply.hdr.retval = retval;
    std::function<void(osd_op_t*)>(op->callback)(op);
}

// fake_peer_t: the secondary protocol

// Apply one queued subop the way a correct secondary would: mutate the store
// and fill the reply fields. The callback runs later, when service_peers
// delivers the reply (or -EPIPEs it when the connection died)
void fake_peer_t::apply(osd_op_t *op)
{
    uint64_t opcode = op->req.hdr.opcode;
    op->reply.hdr.magic = SECONDARY_OSD_REPLY_MAGIC;
    op->reply.hdr.id = op->req.hdr.id;
    op->reply.hdr.opcode = opcode;
    if (ch->trace)
    {
        if (opcode == OSD_OP_SEC_READ || opcode == OSD_OP_SEC_WRITE ||
            opcode == OSD_OP_SEC_WRITE_STABLE)
        {
            printf("peer %ju: sec op %ju stripe %jx v%jx off=%ju len=%ju attr=%ju\n",
                num, opcode, op->req.sec_rw.oid.stripe, op->req.sec_rw.version,
                (uint64_t)op->req.sec_rw.offset, (uint64_t)op->req.sec_rw.len,
                (uint64_t)op->req.sec_rw.attr_len);
        }
        else if (opcode == OSD_OP_SEC_DELETE)
            printf("peer %ju: sec del stripe %jx v%jx\n",
                num, op->req.sec_del.oid.stripe, op->req.sec_del.version);
        else
            printf("peer %ju: sec op %ju\n", num, opcode);
    }
    if (opcode == OSD_OP_SEC_READ || opcode == OSD_OP_SEC_WRITE ||
        opcode == OSD_OP_SEC_WRITE_STABLE)
    {
        uint64_t stripe = op->req.sec_rw.oid.stripe, version = op->req.sec_rw.version;
        uint64_t offset = op->req.sec_rw.offset, len = op->req.sec_rw.len;
        if (opcode == OSD_OP_SEC_READ)
        {
            uint64_t have = lookup(stripe, version);
            if (!have)
            {
                op->reply.hdr.retval = -ENOENT;
                return;
            }
            // The blockstore merges the version chain on read
            std::vector<uint8_t> merged_bmp;
            std::vector<uint8_t> merged = merge_chain(stripe, have, &merged_bmp);
            op->reply.sec_rw.version = have;
            if (ch->trace)
            {
                printf("peer %ju: sec read reply v%jx bmp=", num, have);
                for (auto & c: merged_bmp)
                    printf("%02x", c);
                printf("\n");
            }
            if (len > 0)
            {
                memcpy(op->iov.buf[0].iov_base, merged.data() + offset, len);
                if (op->req.sec_rw.attr_len)
                {
                    memcpy(op->bitmap, merged_bmp.data(), op->req.sec_rw.attr_len);
                    op->reply.sec_rw.attr_len = op->req.sec_rw.attr_len;
                }
            }
            op->reply.hdr.retval = (int64_t)len;
        }
        else
        {
            auto & vers = store[stripe];
            if (vers.count(version))
            {
                // A real blockstore refuses to create one version twice
                op->reply.hdr.retval = -EEXIST;
                return;
            }
            peer_ver_t pv;
            pv.data.assign(ch->blk, 0);
            if (len)
                memcpy(pv.data.data() + offset, op->iov.buf[0].iov_base, len);
            // The blockstore's internal version bitmap covers exactly the granules
            // the write touched. The bitmap on the wire is the primary's merged
            // view (for EC parity, the RS-encoded data bitmaps) and belongs to the
            // attr side, where the blockstore keeps it as the external bitmap
            pv.bmp.assign(ch->blk_bmp, 0);
            for (uint64_t g = offset/GRAN; g < (offset+len+GRAN-1)/GRAN; g++)
                pv.bmp[g/8] |= 1 << (g%8);
            pv.ext.assign(ch->blk_bmp, 0);
            if (op->req.sec_rw.attr_len && op->bitmap)
            {
                memcpy(pv.ext.data(), op->bitmap,
                    std::min((uint64_t)op->req.sec_rw.attr_len, ch->blk_bmp));
            }
            pv.stable = opcode == OSD_OP_SEC_WRITE_STABLE;
            // immediate_commit=all lands every write durable at once; without
            // it the copy sits in the journal until a SYNC fsyncs it
            pv.fsynced = !ch->use_fsync;
            vers[version] = std::move(pv);
            op->reply.sec_rw.version = version;
            op->reply.hdr.retval = (int64_t)len;
        }
    }
    else if (opcode == OSD_OP_SEC_LIST)
    {
        // Like blockstore_heap_t::list_objects: for every object the newest
        // stable version (if any) plus every unstable version above it, the
        // stable entries first and the unstable ones at the tail, split by
        // stable_count. Hiding a stable version under a newer unstable one
        // would make peering roll back what it can't see and deem recoverable
        // objects INCOMPLETE
        std::vector<obj_ver_id> stable_items, unstable_items;
        for (auto & o: store)
        {
            if (o.second.empty())
                continue;
            uint64_t stable_ver = 0;
            for (auto & vv: o.second)
            {
                if (vv.second.stable)
                    stable_ver = vv.first;
            }
            if (stable_ver)
            {
                stable_items.push_back((obj_ver_id){
                    .oid = { .inode = INODE_WITH_POOL(POOL_ID, INODE), .stripe = o.first },
                    .version = stable_ver,
                });
            }
            for (auto & vv: o.second)
            {
                if (vv.first > stable_ver)
                {
                    unstable_items.push_back((obj_ver_id){
                        .oid = { .inode = INODE_WITH_POOL(POOL_ID, INODE), .stripe = o.first },
                        .version = vv.first,
                    });
                }
            }
        }
        uint64_t stable_count = stable_items.size();
        std::vector<obj_ver_id> items = stable_items;
        items.insert(items.end(), unstable_items.begin(), unstable_items.end());
        op->reply.sec_list.stable_count = stable_count;
        op->buf = NULL;
        if (items.size())
        {
            op->buf = malloc(items.size() * sizeof(obj_ver_id));
            memcpy(op->buf, items.data(), items.size() * sizeof(obj_ver_id));
        }
        op->reply.hdr.retval = (int64_t)items.size();
    }
    else if (opcode == OSD_OP_SEC_DELETE)
    {
        // A blockstore delete at version X removes the item with all versions up
        // to X; a newer item survives. The primary deletes at fact_ver+1, so a
        // peer lagging with an older version must lose the object entirely
        uint64_t stripe = op->req.sec_del.oid.stripe, version = op->req.sec_del.version;
        auto sit = store.find(stripe);
        if (sit != store.end() &&
            (!version || sit->second.rbegin()->first <= version))
            store.erase(sit);
        op->reply.sec_del.version = version;
        op->reply.hdr.retval = 0;
    }
    else if (opcode == OSD_OP_SEC_SYNC)
    {
        // The journal sync: every copy written before it became durable and
        // now survives a crash of this peer
        for (auto & o: store)
        {
            for (auto & vv: o.second)
                vv.second.fsynced = true;
        }
        op->reply.hdr.retval = 0;
    }
    else if (opcode == OSD_OP_SEC_STABILIZE || opcode == OSD_OP_SEC_ROLLBACK)
    {
        int n = op->req.sec_stab.len / sizeof(obj_ver_id);
        auto *ovs = (obj_ver_id*)op->iov.buf[0].iov_base;
        for (int i = 0; i < n; i++)
        {
            auto sit = store.find(ovs[i].oid.stripe);
            if (sit == store.end())
                continue;
            if (opcode == OSD_OP_SEC_ROLLBACK)
            {
                // A real blockstore rolls the piece back TO the requested
                // version: everything above it is removed, everything up to
                // and including it survives (version 0 removes the piece)
                for (auto vit = sit->second.upper_bound(ovs[i].version);
                    vit != sit->second.end(); )
                    vit = sit->second.erase(vit);
                if (sit->second.empty())
                    store.erase(sit);
            }
            else
            {
                // The real blockstore marks all versions up to the requested
                // one of each listed piece as stable
                for (auto & vv: sit->second)
                {
                    if (vv.first <= ovs[i].version)
                    {
                        vv.second.stable = true;
                        // A stabilize follows a sync for every listed piece,
                        // so these versions are durable as well
                        vv.second.fsynced = true;
                    }
                }
            }
        }
        op->reply.hdr.retval = 0;
    }
    else if (opcode == OSD_OP_SEC_READ_BMP)
    {
        int n = op->req.sec_read_bmp.len / sizeof(obj_ver_id);
        auto *req_ovs = (obj_ver_id*)op->buf;
        uint64_t item_size = sizeof(uint64_t) + ch->blk_bmp;
        void *reply_buf = malloc_or_die(n * item_size);
        for (int i = 0; i < n; i++)
        {
            uint8_t *dst = (uint8_t*)reply_buf + i * item_size;
            memset(dst, 0, item_size);
            uint64_t have = lookup(req_ovs[i].oid.stripe, req_ovs[i].version);
            if (have)
            {
                *(uint64_t*)dst = have;
                for (auto & vv : store[req_ovs[i].oid.stripe])
                {
                    if (vv.first > have)
                        break;
                    for (uint64_t b = 0; b < ch->blk_bmp; b++)
                        dst[sizeof(uint64_t)+b] |= vv.second.ext[b];
                }
            }
        }
        free(op->buf);
        op->buf = reply_buf;
        op->reply.hdr.retval = (int64_t)(n * item_size);
    }
    else
    {
        fprintf(stderr, "peer %ju: unexpected secondary opcode %ju\n", num, opcode);
        fflush(stdout);
        abort();
    }
}

static void run_one(const chaos_cfg_t & cfg, uint32_t seed, uint64_t steps, bool trace)
{
    printf("\n-- chaos pg seed=%u steps=%ju fsync=%d csum_block=%s scheme=%s recovery=%d\n",
        seed, steps, cfg.use_fsync ? 1 : 0,
        cfg.csum_block ? std::to_string(cfg.csum_block).c_str() : "none",
        cfg.ec ? "ec2+2" : "replicated",
        cfg.recovery ? 1 : 0);
    chaos_t t;
    t.trace = trace;
    t.configure(cfg);
    t.create(seed);
    t.run(steps);
    printf("OK: %ju writes (%ju refused), %ju deletes, %ju syncs, "
        "%ju crashes, %ju clean restarts, %ju takeovers, "
        "%ju peer deaths (%ju kept journal), %ju peer recoveries, "
        "%ju PG set changes, %ju verified reads, %ju stragglers applied "
        "(%ju captured, %ju lost), %ju disk ops\n",
        t.writes, t.refused_ops, t.deletes, t.syncs, t.crashes,
        t.restarts, t.takeovers, t.peer_downs, t.peer_cache_kept, t.peer_ups, t.set_changes,
        t.checked_reads, t.cluster.wire_delivered, t.cluster.wire_harvested,
        t.cluster.wire_lost, t.data_disk->completed_ops);
    // With --peer-cache-loss < 100 and a volatile-cache config the keep scenario
    // MUST have been exercised; silently covering nothing is how a knob that
    // "looks enabled" tests nothing. immediate_commit=all configs ack every write
    // as durable, so peer journals hold nothing to keep. Below ~8 deaths a
    // keep==0 result is indistinguishable from chance (0.5^n), so don't claim
    if (probs.peer_cache_loss < 100 && cfg.use_fsync && t.peer_downs >= 8 && t.peer_cache_kept == 0)
        printf("NOTE: peer-cache-loss=%u was set but no peer crash ever kept its "
            "journal (%ju peer deaths) - the scenario was not covered\n",
            probs.peer_cache_loss, t.peer_downs);
    // Bytes on the wire are part of the modelled world: with this many dirty
    // deaths some queued subops MUST have landed after the sender died.
    // Captured but never delivered would mean the delivery never runs
    if (t.crashes + t.takeovers >= 10 && t.cluster.wire_harvested > 0 &&
        t.cluster.wire_delivered == 0)
    {
        fprintf(stderr, "%ju subops were caught on the wire but none ever "
            "applied - the straggler delivery does not work\n",
            t.cluster.wire_harvested);
        abort();
    }
}

// Both commit modes for a 3-replica pool, with every checksum choice: none, a
// block equal to the write granularity, and a block larger than it (partial
// checksum block updates are a rich source of bugs). The EC pool with 2 data
// and 2 parity chunks runs a sampled subset of the same matrix
static chaos_cfg_t configs[] = {
    { .use_fsync = false, .csum_block = 0,     .ec = false },
    { .use_fsync = false, .csum_block = 4096,  .ec = false },
    { .use_fsync = false, .csum_block = 16384, .ec = false },
    { .use_fsync = true , .csum_block = 0,     .ec = false },
    { .use_fsync = true , .csum_block = 4096,  .ec = false },
    { .use_fsync = true , .csum_block = 16384, .ec = false },
    { .use_fsync = false, .csum_block = 0,     .ec = true  },
    { .use_fsync = false, .csum_block = 16384, .ec = true  },
    { .use_fsync = true , .csum_block = 4096,  .ec = true  },
    { .use_fsync = true , .csum_block = 0,     .ec = true  },
};

static const char *help_text =
    "Seed-driven PG chaos test: one real OSD, its peers simulated, (c) Vitaliy Filippov, 2019+ (VNPL-1.1)\n"
    "\n"
    "USAGE:\n"
    "  test_pg_chaos [OPTIONS]\n"
    "\n"
    "Runs one real OSD with a real blockstore owning one PG, replicated x3 or EC 2+2\n"
    "depending on the configuration, with the other members answered by a fake-peer\n"
    "simulator that stores exactly what the blockstore would store. The test then\n"
    "kills and revives peers, moves the PG to\n"
    "other OSDs, crashes the primary and keeps reading and writing through it all,\n"
    "checking that acknowledged data is never lost, never appears at wrong offsets,\n"
    "and that a fully healthy set holds it on every replica. Without options it runs\n"
    "a fixed range of seeds covering every configuration twice, then fills up to %d\n"
    "seeds with random ones. Every seed is printed, failing ones are listed at the\n"
    "end, so a failure is reproduced by passing its seed back. Seeds run one per\n"
    "child process. A single seed runs in-process, which is what you want under a\n"
    "debugger.\n"
    "\n"
    "  --seed <n>          Run just this seed, in-process\n"
    "  --seeds <a>-<b>     Run seeds <a> through <b> (default 1-%d). Turns the random seeds\n"
    "                      off unless --random is given as well\n"
    "  --random <n>        Run <n> random seeds after the range above (default %d). They are\n"
    "                      spread over the configurations evenly\n"
    "  --steps <n>         Steps per seed (default %ju)\n"
    "  --config <n>        Force configuration <n> instead of deriving it from the seed\n"
    "  --max-inflight <n>  Operations in flight at once (default %u)\n"
    "  --recovery          Enable recovery/rebalance after events (off by default, so\n"
    "                      that degraded states and data on old OSDs are exercised)\n"
    "  --ec                Run only the EC (2 data + 2 parity chunks) configurations\n"
    "  --peer-cache-loss <p>  Percent of peer crashes that drop the never-fsynced\n"
    "                        journal tail (default 50; 100 to always drop it)\n"
    "  --replicated        Run only the replicated x3 configurations\n"
    "  -v, --trace         Trace every operation and every simulated disk request\n"
    "  -h, --help          Show this help\n"
    "\n"
    "Every step picks one action below at random, by weight. The weights are relative\n"
    "to each other, and setting one to 0 turns the action off, e.g. --p-crash 0 gives\n"
    "a run without primary crashes:\n"
    "  --p-wait <n>        Submit nothing, just let the simulation run (default %u)\n"
    "  --p-write <n>       Write a random range of a random object (default %u)\n"
    "  --p-delete <n>      Delete an existing object (default %u)\n"
    "  --p-read <n>        Read back and verify a random object (default %u)\n"
    "  --p-sync <n>        Sync (default %u)\n"
    "  --p-restart <n>     Clean restart of the primary (default %u)\n"
    "  --p-takeover <n>    Hand the PG over to a fresh OSD instance (default %u)\n"
    "  --p-crash <n>       Kill the primary, whatever is in flight (default %u)\n"
    "  --p-peer-down <n>   Kill a random live peer (default %u)\n"
    "  --p-peer-up <n>     Revive a dead peer or the spare (default %u)\n"
    "  --p-set-change <n>  Move the PG to another OSD set (default %u)\n"
    "\n"
    "  --p-full-read <n>   Percentage of reads covering the whole object (default %u)\n"
    "  --p-reply <n>       Chance per round to deliver an already-applied peer\n"
    "                      reply, the response-path delay (default %u)\n"
    "  --p-wire <n>        Chance per round to apply a delayed peer request or\n"
    "                      deliver a harvested wire record (default %u)\n"
    "  --p-post <n>        Percent of cluster events after which the test waits\n"
    "                      for the PG to settle and verifies everything (default %u)\n"
    "\n"
    "Configurations (%d):\n";

static void print_help(int cfg_count, int default_seeds, int default_fixed, int default_random, uint64_t default_steps)
{
    chaos_probs_t d;
    printf(help_text, default_seeds, default_fixed, default_random, default_steps, d.max_inflight,
        d.wait, d.write, d.del, d.read, d.sync, d.restart, d.takeover, d.crash,
        d.peer_down, d.peer_up, d.set_change, d.full_read, d.p_reply, d.p_wire, d.p_post, cfg_count);
    for (int i = 0; i < cfg_count; i++)
    {
        printf("  %2d: fsync=%d, csum_block=%s, scheme=%s\n", i, configs[i].use_fsync ? 1 : 0,
            configs[i].csum_block ? std::to_string(configs[i].csum_block).c_str() : "none",
            configs[i].ec ? "ec 2+2" : "replicated x3");
    }
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
    const int default_seeds = 100;
    // Every configuration twice as a fixed regression floor, and the rest of the run filled up
    // with random seeds so that every build tries situations the previous ones never reached
    const int default_fixed = 2*cfg_count;
    const int default_random = default_seeds > default_fixed ? default_seeds-default_fixed : 0;
    bool recovery = false;
    bool ec_only = false, rep_only = false;
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
            if (v >= (uint64_t)cfg_count)
            {
                fprintf(stderr, "--config: no configuration %llu (valid: 0..%d, see --help)\n",
                    (unsigned long long)v, cfg_count - 1);
                exit(1);
            }
            force_cfg = (int)v;
        }
        else if (!strcmp(opt, "--max-inflight")) { take(v); probs.max_inflight = (uint32_t)v; }
        else if (!strcmp(opt, "--p-wait"))       { take(v); probs.wait = (uint32_t)v; }
        else if (!strcmp(opt, "--p-write"))      { take(v); probs.write = (uint32_t)v; }
        else if (!strcmp(opt, "--p-delete"))     { take(v); probs.del = (uint32_t)v; }
        else if (!strcmp(opt, "--p-read"))       { take(v); probs.read = (uint32_t)v; }
        else if (!strcmp(opt, "--p-sync"))       { take(v); probs.sync = (uint32_t)v; }
        else if (!strcmp(opt, "--p-crash"))      { take(v); probs.crash = (uint32_t)v; }
        else if (!strcmp(opt, "--p-peer-down"))  { take(v); probs.peer_down = (uint32_t)v; }
        else if (!strcmp(opt, "--p-peer-up"))    { take(v); probs.peer_up = (uint32_t)v; }
        else if (!strcmp(opt, "--p-set-change")) { take(v); probs.set_change = (uint32_t)v; }
        else if (!strcmp(opt, "--recovery"))     recovery = true;
        else if (!strcmp(opt, "--ec"))           ec_only = true;
        else if (!strcmp(opt, "--replicated"))   rep_only = true;
        else if (!strcmp(opt, "--p-restart"))    { take(v); probs.restart = (uint32_t)v; }
        else if (!strcmp(opt, "--p-takeover"))   { take(v); probs.takeover = (uint32_t)v; }
        else if (!strcmp(opt, "--p-full-read"))  { take(v); probs.full_read = (uint32_t)v; }
        else if (!strcmp(opt, "--p-reply"))      { take(v); probs.p_reply = (uint32_t)v; }
        else if (!strcmp(opt, "--p-wire"))       { take(v); probs.p_wire = (uint32_t)v; }
        else if (!strcmp(opt, "--peer-cache-loss")) { take(v); probs.peer_cache_loss = (uint32_t)v; }
        else if (!strcmp(opt, "--p-post"))     { take(v); probs.p_post = (uint32_t)v; }
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
    if (!probs.p_reply)
    {
        fprintf(stderr, "--p-reply must be at least 1\n");
        exit(1);
    }
    if (!probs.p_wire || probs.p_wire > 100)
    {
        fprintf(stderr, "--p-wire must be between 1 and 100\n");
        exit(1);
    }
    if (probs.p_post > 100)
    {
        fprintf(stderr, "--p-post must be between 0 and 100\n");
        exit(1);
    }
    for (auto & c: configs)
        c.recovery = recovery;
    // --ec / --replicated restrict the seed-to-configuration mapping to one scheme
    std::vector<int> cfg_sel;
    for (int i = 0; i < cfg_count; i++)
    {
        if ((!ec_only || configs[i].ec) && (!rep_only || !configs[i].ec))
            cfg_sel.push_back(i);
    }
    if (!cfg_sel.size())
    {
        fprintf(stderr, "no configurations match the --ec/--replicated filter\n");
        exit(1);
    }
    // The seed picks the configuration as well, so reproducing a failure is a single short run
    if (single)
    {
        run_one(configs[force_cfg >= 0 ? force_cfg : cfg_sel[seed % cfg_sel.size()]], seed, steps, trace);
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
                s = (rnd() % (1u<<30)) / cfg_sel.size() * cfg_sel.size() + (i % cfg_sel.size());
            } while (!s || !used.insert(s).second);
            seeds.push_back(s);
        }
        printf("Running %zu fixed seed(s) and %d random one(s)\n", fixed_count, random_count);
    }
    std::vector<uint32_t> failed;
    for (auto s: seeds)
    {
        if (!run_seed_isolated(configs[force_cfg >= 0 ? force_cfg : cfg_sel[s % cfg_sel.size()]], s, steps, trace))
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
