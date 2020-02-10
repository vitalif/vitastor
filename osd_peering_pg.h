#include <map>
#include <vector>
#include <algorithm>

#include "object_id.h"
#include "osd_ops.h"

#include "sparsepp/sparsepp/spp.h"

// Placement group states
// Exactly one of these:
#define PG_OFFLINE (1<<0)
#define PG_PEERING (1<<1)
#define PG_INCOMPLETE (1<<2)
#define PG_ACTIVE (1<<3)
// Plus any of these:
#define PG_DEGRADED (1<<4)
#define PG_HAS_UNFOUND (1<<5)
#define PG_HAS_DEGRADED (1<<6)
#define PG_HAS_MISPLACED (1<<7)
#define PG_HAS_UNCLEAN (1<<8)

// OSD object states
#define OBJ_CLEAN 0x01
#define OBJ_MISPLACED 0x02
#define OBJ_DEGRADED 0x03
#define OBJ_INCOMPLETE 0x04
#define OBJ_NEEDS_STABLE 0x10000
#define OBJ_NEEDS_ROLLBACK 0x20000
#define OBJ_OVERCOPIED 0x40000
#define OBJ_BUGGY 0x80000

struct pg_obj_loc_t
{
    uint64_t role;
    osd_num_t osd_num;
    bool stable;
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
    obj_ver_id *buf;
    uint64_t total_count;
    uint64_t stable_count;
};

struct osd_op_t;

struct pg_peering_state_t
{
    // osd_num -> list result
    spp::sparse_hash_map<osd_num_t, osd_op_t*> list_ops;
    spp::sparse_hash_map<osd_num_t, pg_list_result_t> list_results;
    int list_done = 0;
};

struct pg_obj_state_check_t
{
    int obj_start = 0, obj_end = 0, ver_start = 0, ver_end = 0;
    object_id oid = { 0 };
    uint64_t max_ver = 0;
    uint64_t target_ver = 0;
    uint64_t n_copies = 0, has_roles = 0, n_roles = 0, n_stable = 0, n_matched = 0;
    bool is_buggy = false, has_old_unstable = false;
    pg_osd_set_t osd_set;
};

struct obj_ver_role
{
    object_id oid;
    uint64_t version;
    uint64_t osd_num;
    bool is_stable;
};

struct obj_piece_id_t
{
    object_id oid;
    uint64_t osd_num;
};

struct obj_piece_ver_t
{
    uint64_t max_ver = 0;
    uint64_t stable_ver = 0;
};

struct obj_stab_action_t
{
    bool rollback = false, make_stable = false;
    uint64_t stable_to = 0, rollback_to = 0;
};

struct pg_t
{
    int state;
    uint64_t pg_cursize = 3, pg_size = 3, pg_minsize = 2;
    pg_num_t pg_num;
    uint64_t clean_count = 0;
    // target_set is the "correct" peer OSD set for this PG
    std::vector<osd_num_t> target_set;
    // cur_set is the current set of connected peer OSDs for this PG
    // cur_set = (role => osd_num or UINT64_MAX if missing). role numbers begin with zero
    std::vector<osd_num_t> cur_set;
    // moved object map. by default, each object is considered to reside on the cur_set.
    // this map stores all objects that differ.
    // it may consume up to ~ (raw storage / object size) * 24 bytes in the worst case scenario
    // which is up to ~192 MB per 1 TB in the worst case scenario
    std::map<pg_osd_set_t, pg_osd_set_state_t> state_dict;
    spp::sparse_hash_map<object_id, pg_osd_set_state_t*> obj_states;
    std::map<obj_piece_id_t, obj_stab_action_t> obj_stab_actions;
    spp::sparse_hash_map<object_id, uint64_t> ver_override;
    pg_peering_state_t *peering_state = NULL;

    void calc_object_states();
    void remember_object(pg_obj_state_check_t &st, std::vector<obj_ver_role> &all);
};

inline bool operator < (const pg_obj_loc_t &a, const pg_obj_loc_t &b)
{
    return a.role < b.role || a.role == b.role && a.osd_num < b.osd_num ||
        a.role == b.role && a.osd_num == b.osd_num && a.stable < b.stable;
}

inline bool operator < (const obj_ver_role & a, const obj_ver_role & b)
{
    return a.oid < b.oid ||
        // object versions come in descending order
        a.oid == b.oid && a.version > b.version ||
        a.oid == b.oid && a.version == b.version ||
        a.oid == b.oid && a.version == b.version && a.osd_num < b.osd_num;
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
                seed ^= ((e.stable ? 1 : 0) + 0xc6a4a7935bd1e995 + (seed << 6) + (seed >> 2));
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
