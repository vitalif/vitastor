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
    // ORDER BY inode ASC, stripe & ~STRIPE_MASK ASC, version DESC, osd_num ASC
    return a.oid.inode < b.oid.inode || a.oid.inode == b.oid.inode && (
        (a.oid.stripe & ~STRIPE_MASK) < (b.oid.stripe & ~STRIPE_MASK) ||
        (a.oid.stripe & ~STRIPE_MASK) == (b.oid.stripe & ~STRIPE_MASK) && (
            a.version > b.version || a.version == b.version && a.osd_num < b.osd_num
        )
    );
}

struct obj_piece_ver_t
{
    uint64_t max_ver = 0;
    uint64_t stable_ver = 0;
};

struct pg_obj_state_check_t
{
    pg_t *pg;
    int i;
    std::vector<obj_ver_role> list;
    int obj_start = 0, obj_end = 0, ver_start = 0, ver_end = 0;
    object_id oid = { 0 };
    uint64_t max_ver = 0;
    uint64_t last_ver = 0;
    uint64_t target_ver = 0;
    uint64_t n_copies = 0, has_roles = 0, n_roles = 0, n_stable = 0, n_mismatched = 0;
    bool is_buggy = false, has_old_unstable = false;
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
    for (i = 0; i < list.size(); i++)
    {
        if (oid.inode != list[i].oid.inode ||
            oid.stripe != (list[i].oid.stripe & ~STRIPE_MASK))
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
    obj_start = i;
    oid = { .inode = list[i].oid.inode, .stripe = list[i].oid.stripe & ~STRIPE_MASK };
    last_ver = max_ver = list[i].version;
    target_ver = 0;
    ver_start = i;
    has_roles = n_copies = n_roles = n_stable = n_mismatched = 0;
    is_buggy = false;
}

void pg_obj_state_check_t::handle_version()
{
    if (!target_ver && last_ver != list[i].version && (n_stable > 0 || n_roles >= pg->pg_minsize))
    {
        // Version is either stable or recoverable
        target_ver = last_ver;
        ver_end = i;
    }
    if (!target_ver)
    {
        if (last_ver != list[i].version)
        {
            ver_start = i;
            has_roles = n_copies = n_roles = n_stable = n_mismatched = 0;
            last_ver = list[i].version;
        }
        int replica = (list[i].oid.stripe & STRIPE_MASK);
        n_copies++;
        if (replica >= pg->pg_size)
        {
            // FIXME In the future, check it against the PG epoch number to handle replication factor/scheme changes
            is_buggy = true;
        }
        else
        {
            if (list[i].is_stable)
            {
                n_stable++;
            }
            if (pg->cur_set[replica] != list[i].osd_num)
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
    else if (!list[i].is_stable)
    {
        has_old_unstable = true;
    }
}

void pg_obj_state_check_t::finish_object()
{
    if (!target_ver && (n_stable > 0 || n_roles >= pg->pg_minsize))
    {
        // Version is either stable or recoverable
        target_ver = last_ver;
        ver_end = i;
    }
    obj_end = i;
    // Remember the decision
    uint64_t state = OBJ_CLEAN;
    if (target_ver > 0)
    {
        if (n_roles < pg->pg_minsize)
        {
            printf("Object is incomplete: inode=%lu stripe=%lu version=%lu/%lu\n", oid.inode, oid.stripe, target_ver, max_ver);
            for (int i = ver_start; i < ver_end; i++)
            {
                printf("Present on: osd %lu, role %ld%s\n", list[i].osd_num, (list[i].oid.stripe & STRIPE_MASK), list[i].is_stable ? " (stable)" : "");
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
        if (n_stable < n_copies)
        {
            state |= OBJ_NEEDS_STABLE;
            pg->state = pg->state | PG_HAS_UNCLEAN;
        }
    }
    if (target_ver < max_ver || has_old_unstable)
    {
        state |= OBJ_NEEDS_ROLLBACK;
        pg->state = pg->state | PG_HAS_UNCLEAN;
    }
    if (is_buggy)
    {
        state |= OBJ_BUGGY;
        // FIXME: bring pg offline
        throw std::runtime_error("buggy object state");
    }
    pg->total_count++;
    if (state == OBJ_CLEAN)
    {
        pg->clean_count++;
    }
    else
    {
        osd_set.clear();
        for (int i = ver_start; i < ver_end; i++)
        {
            osd_set.push_back((pg_obj_loc_t){
                .role = (list[i].oid.stripe & STRIPE_MASK),
                .osd_num = list[i].osd_num,
                .stable = list[i].is_stable,
            });
        }
        std::sort(osd_set.begin(), osd_set.end());
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
                read_target[o.role] = o.osd_num;
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
        pg->obj_states[oid] = &it->second;
        if (target_ver < max_ver)
        {
            pg->ver_override[oid] = target_ver;
        }
        if (state & (OBJ_NEEDS_ROLLBACK | OBJ_NEEDS_STABLE))
        {
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
            }
            for (auto pp: pieces)
            {
                auto & pcs = pp.second;
                if (pcs.stable_ver < pcs.max_ver)
                {
                    auto & act = pg->flush_actions[pp.first];
                    if (pcs.max_ver > target_ver)
                    {
                        act.rollback = true;
                        act.rollback_to = target_ver;
                    }
                    else if (pcs.max_ver < target_ver && pcs.stable_ver < pcs.max_ver)
                    {
                        act.rollback = true;
                        act.rollback_to = pcs.stable_ver;
                    }
                    if (pcs.max_ver >= target_ver && pcs.stable_ver < target_ver)
                    {
                        act.make_stable = true;
                        act.stable_to = target_ver;
                    }
                }
            }
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
