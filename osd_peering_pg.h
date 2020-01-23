#include <map>
#include <vector>
#include <algorithm>

#include "object_id.h"

#include "sparsepp/sparsepp/spp.h"

// Placement group states
// Exactly one of these:
#define PG_OFFLINE (1<<0)
#define PG_PEERING (1<<1)
#define PG_INCOMPLETE (1<<2)
#define PG_ACTIVE (1<<3)
// Plus any of these:
#define PG_HAS_UNFOUND (1<<4)
#define PG_HAS_DEGRADED (1<<5)
#define PG_HAS_MISPLACED (1<<6)

// OSD object states
#define OBJ_CLEAN 0x01
#define OBJ_MISPLACED 0x02
#define OBJ_DEGRADED 0x03
#define OBJ_INCOMPLETE 0x04
#define OBJ_NONSTABILIZED 0x10000
#define OBJ_UNDERWRITTEN 0x20000
#define OBJ_OVERCOPIED 0x40000
#define OBJ_BUGGY 0x80000

// Max 64 replicas
#define STRIPE_MASK 0x3F
#define STRIPE_SHIFT 6

struct pg_obj_loc_t
{
    uint64_t role;
    uint64_t osd_num;
    bool stable;
};

typedef std::vector<pg_obj_loc_t> pg_osd_set_t;

struct pg_osd_set_state_t
{
    pg_osd_set_t osd_set;
    uint64_t state = 0;
    uint64_t object_count = 0;
};

struct pg_ver_override_t
{
    uint64_t max_ver;
    uint64_t target_ver;
};

struct pg_list_result_t
{
    obj_ver_id *buf;
    uint64_t total_count;
    uint64_t stable_count;
};

struct pg_peering_state_t
{
    // osd_num -> list result
    spp::sparse_hash_map<uint64_t, pg_list_result_t> list_results;
    int list_done = 0;
};

struct pg_obj_state_check_t
{
    int start = 0;
    object_id oid = { 0 };
    uint64_t max_ver = 0;
    uint64_t target_ver = 0;
    uint64_t n_copies = 0, has_roles = 0, n_roles = 0, n_stable = 0, n_matched = 0;
    bool is_buggy = false;
    pg_osd_set_t osd_set;
};

struct obj_ver_role
{
    object_id oid;
    uint64_t version;
    uint64_t osd_num;
    bool is_stable;
};

struct pg_t
{
    int state;
    uint64_t pg_size = 3, pg_minsize = 2;
    uint64_t pg_num;
    uint64_t clean_count = 0;
    // target_set = (role => osd_num). role starts from zero
    std::vector<uint64_t> target_set;
    // moved object map. by default, each object is considered to reside on the target_set.
    // this map stores all objects that differ.
    // this map may consume up to ~ (raw storage / object size) * 24 bytes in the worst case scenario
    // which is up to ~192 MB per 1 TB in the worst case scenario
    std::map<pg_osd_set_t, pg_osd_set_state_t> state_dict;
    spp::sparse_hash_map<object_id, pg_osd_set_state_t*> obj_states;
    spp::sparse_hash_map<object_id, pg_ver_override_t> ver_override;
    pg_peering_state_t *peering_state = NULL;

    void calc_object_states();
    void remember_object(pg_obj_state_check_t &st, std::vector<obj_ver_role> &all, int end);
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
}
