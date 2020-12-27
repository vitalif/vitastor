// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.0 (see README.md for details)

#include <map>
#include <vector>
#include <algorithm>

#include "cpp-btree/btree_map.h"

#include "object_id.h"
#include "osd_ops.h"
#include "pg_states.h"

#define PG_EPOCH_BITS 48

struct pg_obj_loc_t
{
    uint64_t role;
    osd_num_t osd_num;
    bool outdated;
};

typedef std::vector<pg_obj_loc_t> pg_osd_set_t;

struct pg_osd_set_state_t
{
    // (role -> osd_num_t) map, as in pg.target_set and pg.cur_set
    std::vector<osd_num_t> read_target;
    // full OSD set including additional OSDs where the object is misplaced
    pg_osd_set_t osd_set;
    uint64_t state = 0;
    uint64_t object_count = 0;
};

struct pg_list_result_t
{
    obj_ver_id *buf = NULL;
    uint64_t total_count;
    uint64_t stable_count;
};

struct osd_op_t;

struct pg_peering_state_t
{
    // osd_num -> list result
    std::map<osd_num_t, osd_op_t*> list_ops;
    std::map<osd_num_t, pg_list_result_t> list_results;
    pool_id_t pool_id = 0;
    pg_num_t pg_num = 0;
};

struct obj_piece_id_t
{
    object_id oid;
    uint64_t osd_num;
};

struct flush_action_t
{
    bool rollback = false, make_stable = false;
    uint64_t stable_to = 0, rollback_to = 0;
    bool submitted = false;
};

struct pg_flush_batch_t
{
    std::map<osd_num_t, std::vector<obj_ver_id>> rollback_lists;
    std::map<osd_num_t, std::vector<obj_ver_id>> stable_lists;
    int flush_ops = 0, flush_done = 0;
    int flush_objects = 0;
};

struct pg_t
{
    int state = 0;
    uint64_t scheme = 0;
    uint64_t pg_cursize = 0, pg_size = 0, pg_minsize = 0, parity_chunks = 0;
    pool_id_t pool_id = 0;
    pg_num_t pg_num = 0;
    uint64_t clean_count = 0, total_count = 0;
    // epoch number - should increase with each non-clean activation of the PG
    uint64_t epoch = 0, reported_epoch = 0;
    // target history and all potential peers
    std::vector<std::vector<osd_num_t>> target_history;
    std::vector<osd_num_t> all_peers;
    bool history_changed = false;
    // peer list from the last peering event
    std::vector<osd_num_t> cur_peers;
    // target_set is the "correct" peer OSD set for this PG
    std::vector<osd_num_t> target_set;
    // cur_set is the current set of connected peer OSDs for this PG
    // cur_set = (role => osd_num or UINT64_MAX if missing). role numbers begin with zero
    std::vector<osd_num_t> cur_set;
    // same thing in state_dict-like format
    pg_osd_set_t cur_loc_set;
    // moved object map. by default, each object is considered to reside on cur_set.
    // this map stores all objects that differ.
    // it may consume up to ~ (raw storage / object size) * 24 bytes in the worst case scenario
    // which is up to ~192 MB per 1 TB in the worst case scenario
    std::map<pg_osd_set_t, pg_osd_set_state_t> state_dict;
    btree::btree_map<object_id, pg_osd_set_state_t*> incomplete_objects, misplaced_objects, degraded_objects;
    std::map<obj_piece_id_t, flush_action_t> flush_actions;
    btree::btree_map<object_id, uint64_t> ver_override;
    pg_peering_state_t *peering_state = NULL;
    pg_flush_batch_t *flush_batch = NULL;

    int inflight = 0; // including write_queue
    std::multimap<object_id, osd_op_t*> write_queue;

    void calc_object_states(int log_level);
    void print_state();
};

inline bool operator < (const pg_obj_loc_t &a, const pg_obj_loc_t &b)
{
    return a.outdated < b.outdated ||
        a.outdated == b.outdated && a.role < b.role ||
        a.outdated == b.outdated && a.role == b.role && a.osd_num < b.osd_num;
}

inline bool operator == (const obj_piece_id_t & a, const obj_piece_id_t & b)
{
    return a.oid == b.oid && a.osd_num == b.osd_num;
}

inline bool operator < (const obj_piece_id_t & a, const obj_piece_id_t & b)
{
    return a.oid < b.oid || a.oid == b.oid && a.osd_num < b.osd_num;
}

namespace std
{
    template<> struct hash<pg_osd_set_t>
    {
        inline size_t operator()(const pg_osd_set_t &s) const
        {
            size_t seed = 0;
            for (auto e: s)
            {
                // Copy-pasted from spp::hash_combine()
                seed ^= (e.role + 0xc6a4a7935bd1e995 + (seed << 6) + (seed >> 2));
                seed ^= (e.osd_num + 0xc6a4a7935bd1e995 + (seed << 6) + (seed >> 2));
            }
            return seed;
        }
    };

    template<> struct hash<obj_piece_id_t>
    {
        inline size_t operator()(const obj_piece_id_t &s) const
        {
            size_t seed = std::hash<object_id>()(s.oid);
            // Copy-pasted from spp::hash_combine()
            seed ^= (s.osd_num + 0xc6a4a7935bd1e995 + (seed << 6) + (seed >> 2));
            return seed;
        }
    };
}
