#include "osd_peering_pg.h"

struct obj_ver_role
{
    object_id oid;
    uint64_t version;
    uint64_t osd_num;
    bool is_stable;
};

inline bool operator < (const obj_ver_role & a, const obj_ver_role & b)
{
    // ORDER BY inode ASC, stripe & ~STRIPE_MASK ASC, version DESC, role ASC, osd_num ASC
    return a.oid.inode < b.oid.inode || a.oid.inode == b.oid.inode && (
        (a.oid.stripe & ~STRIPE_MASK) < (b.oid.stripe & ~STRIPE_MASK) ||
        (a.oid.stripe & ~STRIPE_MASK) == (b.oid.stripe & ~STRIPE_MASK) && (
            a.version > b.version ||
            a.version == b.version && (
                a.oid.stripe < b.oid.stripe ||
                a.oid.stripe == b.oid.stripe && a.osd_num < b.osd_num
            )
        )
    );
}

struct obj_piece_ver_t
{
    uint64_t max_ver = 0;
    uint64_t stable_ver = 0;
    uint64_t max_target = 0;
};

struct pg_obj_state_check_t
{
    pg_t *pg;
    std::vector<obj_ver_role> list;
    int list_pos;
    int obj_start = 0, obj_end = 0, ver_start = 0, ver_end = 0;
    object_id oid = { 0 };
    uint64_t max_ver = 0;
    uint64_t last_ver = 0;
    uint64_t target_ver = 0;
    uint64_t n_copies = 0, has_roles = 0, n_roles = 0, n_stable = 0, n_mismatched = 0;
    uint64_t n_unstable = 0, n_buggy = 0;
    pg_osd_set_t osd_set;

    void walk();
    void start_object();
    void handle_version();
    void finish_object();
};

void pg_obj_state_check_t::walk()
{
    pg->clean_count = 0;
    pg->total_count = 0;
    pg->state = 0;
    for (list_pos = 0; list_pos < list.size(); list_pos++)
    {
        if (oid.inode != list[list_pos].oid.inode ||
            oid.stripe != (list[list_pos].oid.stripe & ~STRIPE_MASK))
        {
            if (oid.inode != 0)
            {
                finish_object();
            }
            start_object();
        }
        handle_version();
    }
    if (oid.inode != 0)
    {
        finish_object();
    }
    if (pg->pg_cursize < pg->pg_size)
    {
        pg->state = pg->state | PG_DEGRADED;
    }
    pg->state = pg->state | PG_ACTIVE;
}

void pg_obj_state_check_t::start_object()
{
    obj_start = list_pos;
    oid = { .inode = list[list_pos].oid.inode, .stripe = list[list_pos].oid.stripe & ~STRIPE_MASK };
    last_ver = max_ver = list[list_pos].version;
    target_ver = 0;
    ver_start = list_pos;
    has_roles = n_copies = n_roles = n_stable = n_mismatched = 0;
    n_unstable = n_buggy = 0;
}

void pg_obj_state_check_t::handle_version()
{
    if (!target_ver && last_ver != list[list_pos].version && (n_stable > 0 || n_roles >= pg->pg_minsize))
    {
        // Version is either stable or recoverable
        target_ver = last_ver;
        ver_end = list_pos;
    }
    if (!target_ver)
    {
        if (last_ver != list[list_pos].version)
        {
            ver_start = list_pos;
            has_roles = n_copies = n_roles = n_stable = n_mismatched = 0;
            last_ver = list[list_pos].version;
        }
        int replica = (list[list_pos].oid.stripe & STRIPE_MASK);
        n_copies++;
        if (replica >= pg->pg_size)
        {
            // FIXME In the future, check it against the PG epoch number to handle replication factor/scheme changes
            n_buggy++;
        }
        else
        {
            if (list[list_pos].is_stable)
            {
                n_stable++;
            }
            if (pg->cur_set[replica] != list[list_pos].osd_num)
            {
                n_mismatched++;
            }
            if (!(has_roles & (1 << replica)))
            {
                has_roles = has_roles | (1 << replica);
                n_roles++;
            }
        }
    }
    if (!list[list_pos].is_stable)
    {
        n_unstable++;
    }
}

void pg_obj_state_check_t::finish_object()
{
    if (!target_ver && (n_stable > 0 || n_roles >= pg->pg_minsize))
    {
        // Version is either stable or recoverable
        target_ver = last_ver;
        ver_end = list_pos;
    }
    obj_end = list_pos;
    // Remember the decision
    uint64_t state = 0;
    if (n_buggy > 0)
    {
        state = OBJ_BUGGY;
        // FIXME: bring pg offline
        throw std::runtime_error("buggy object state");
    }
    if (n_unstable > 0)
    {
        pg->state |= PG_HAS_UNCLEAN;
        std::unordered_map<obj_piece_id_t, obj_piece_ver_t> pieces;
        for (int i = obj_start; i < obj_end; i++)
        {
            auto & pcs = pieces[(obj_piece_id_t){ .oid = list[i].oid, .osd_num = list[i].osd_num }];
            if (!pcs.max_ver)
            {
                pcs.max_ver = list[i].version;
            }
            if (list[i].is_stable && !pcs.stable_ver)
            {
                pcs.stable_ver = list[i].version;
            }
            if (list[i].version <= target_ver)
            {
                pcs.max_target = list[i].version;
            }
        }
        for (auto pp: pieces)
        {
            auto & pcs = pp.second;
            if (pcs.stable_ver < pcs.max_ver)
            {
                auto & act = pg->flush_actions[pp.first];
                // osd_set doesn't include rollback/stable states, so don't include them in the state code either
                if (pcs.max_ver > target_ver)
                {
                    act.rollback = true;
                    act.rollback_to = pcs.max_target;
                }
                if (pcs.stable_ver < (pcs.max_ver > target_ver ? pcs.max_target : pcs.max_ver))
                {
                    act.make_stable = true;
                    act.stable_to = pcs.max_ver > target_ver ? pcs.max_target : pcs.max_ver;
                }
            }
        }
    }
    if (!target_ver)
    {
        return;
    }
    if (n_roles < pg->pg_minsize)
    {
        printf("Object is incomplete: inode=%lu stripe=%lu version=%lu/%lu\n", oid.inode, oid.stripe, target_ver, max_ver);
        for (int i = ver_start; i < ver_end; i++)
        {
            printf("Present on: osd %lu, role %ld%s\n", list[i].osd_num, (list[i].oid.stripe & STRIPE_MASK), list[i].is_stable ? " (stable)" : "");
        }
        if (0)
        {
            // For future debug level
            for (int i = obj_start; i < obj_end; i++)
            {
                printf("v%lu present on: osd %lu, role %ld%s\n", list[i].version, list[i].osd_num, (list[i].oid.stripe & STRIPE_MASK), list[i].is_stable ? " (stable)" : "");
            }
        }
        state = OBJ_INCOMPLETE;
        pg->state = pg->state | PG_HAS_INCOMPLETE;
    }
    else if (n_roles < pg->pg_cursize)
    {
        printf("Object is degraded: inode=%lu stripe=%lu version=%lu/%lu\n", oid.inode, oid.stripe, target_ver, max_ver);
        for (int i = ver_start; i < ver_end; i++)
        {
            printf("Present on: osd %lu, role %ld%s\n", list[i].osd_num, (list[i].oid.stripe & STRIPE_MASK), list[i].is_stable ? " (stable)" : "");
        }
        state = OBJ_DEGRADED;
        pg->state = pg->state | PG_HAS_DEGRADED;
    }
    if (n_mismatched > 0)
    {
        state |= OBJ_MISPLACED;
        pg->state = pg->state | PG_HAS_MISPLACED;
    }
    pg->total_count++;
    if (state != 0 || ver_end < obj_end)
    {
        osd_set.clear();
        for (int i = ver_start; i < ver_end; i++)
        {
            osd_set.push_back((pg_obj_loc_t){
                .role = (list[i].oid.stripe & STRIPE_MASK),
                .osd_num = list[i].osd_num,
                .outdated = false,
            });
        }
    }
    if (ver_end < obj_end)
    {
        // Check for outdated versions not present in the current target OSD set
        for (int i = ver_end; i < obj_end; i++)
        {
            int j;
            for (j = 0; j < osd_set.size(); j++)
            {
                if (osd_set[j].osd_num == list[i].osd_num)
                {
                    break;
                }
            }
            if (j >= osd_set.size() && pg->cur_set[list[i].oid.stripe & STRIPE_MASK] != list[i].osd_num)
            {
                osd_set.push_back((pg_obj_loc_t){
                    .role = (list[i].oid.stripe & STRIPE_MASK),
                    .osd_num = list[i].osd_num,
                    .outdated = true,
                });
                state |= OBJ_MISPLACED;
                pg->state = pg->state | PG_HAS_MISPLACED;
            }
        }
    }
    if (target_ver < max_ver)
    {
        pg->ver_override[oid] = target_ver;
    }
    if (state == 0)
    {
        pg->clean_count++;
    }
    else
    {
        auto it = pg->state_dict.find(osd_set);
        if (it == pg->state_dict.end())
        {
            std::vector<uint64_t> read_target;
            read_target.resize(pg->pg_size);
            for (int i = 0; i < pg->pg_size; i++)
            {
                read_target[i] = 0;
            }
            for (auto & o: osd_set)
            {
                if (!o.outdated)
                {
                    read_target[o.role] = o.osd_num;
                }
            }
            pg->state_dict[osd_set] = {
                .read_target = read_target,
                .osd_set = osd_set,
                .state = state,
                .object_count = 1,
            };
            it = pg->state_dict.find(osd_set);
        }
        else
        {
            it->second.object_count++;
        }
        if (state & OBJ_INCOMPLETE)
        {
            pg->incomplete_objects[oid] = &it->second;
        }
        else if (state & OBJ_DEGRADED)
        {
            pg->degraded_objects[oid] = &it->second;
        }
        else
        {
            pg->misplaced_objects[oid] = &it->second;
        }
    }
}

// FIXME: Write at least some tests for this function
void pg_t::calc_object_states()
{
    // Copy all object lists into one array
    pg_obj_state_check_t st;
    st.pg = this;
    auto ps = peering_state;
    for (auto it: ps->list_results)
    {
        auto nstab = it.second.stable_count;
        auto n = it.second.total_count;
        auto osd_num = it.first;
        uint64_t start = st.list.size();
        st.list.resize(start + n);
        obj_ver_id *ov = it.second.buf;
        for (uint64_t i = 0; i < n; i++, ov++)
        {
            st.list[start+i] = {
                .oid = ov->oid,
                .version = ov->version,
                .osd_num = osd_num,
                .is_stable = i < nstab,
            };
        }
        free(it.second.buf);
        it.second.buf = NULL;
    }
    ps->list_results.clear();
    // Sort
    std::sort(st.list.begin(), st.list.end());
    // Walk over it and check object states
    st.walk();
    print_state();
}

void pg_t::print_state()
{
    printf(
        "PG %u is %s%s%s%s%s%s%s%s%s (%lu objects)\n", pg_num,
        (state & PG_OFFLINE) ? "offline" : "",
        (state & PG_PEERING) ? "peering" : "",
        (state & PG_INCOMPLETE) ? "incomplete" : "",
        (state & PG_ACTIVE) ? "active" : "",
        (state & PG_DEGRADED) ? " + degraded" : "",
        (state & PG_HAS_INCOMPLETE) ? " + has_incomplete" : "",
        (state & PG_HAS_DEGRADED) ? " + has_degraded" : "",
        (state & PG_HAS_MISPLACED) ? " + has_misplaced" : "",
        (state & PG_HAS_UNCLEAN) ? " + has_unclean" : "",
        total_count
    );
}

const int pg_state_bit_count = 10;

const int pg_state_bits[10] = {
    PG_OFFLINE,
    PG_PEERING,
    PG_INCOMPLETE,
    PG_ACTIVE,
    PG_DEGRADED,
    PG_HAS_INCOMPLETE,
    PG_HAS_DEGRADED,
    PG_HAS_MISPLACED,
    PG_HAS_UNCLEAN,
};

const char *pg_state_names[10] = {
    "offline",
    "peering",
    "incomplete",
    "active",
    "degraded",
    "has_incomplete",
    "has_degraded",
    "has_misplaced",
    "has_unclean",
};
