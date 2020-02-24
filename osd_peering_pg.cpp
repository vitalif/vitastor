#include "osd_peering_pg.h"

void pg_t::remember_object(pg_obj_state_check_t &st, std::vector<obj_ver_role> &all)
{
    auto & pg = *this;
    // Remember the decision
    uint64_t state = 0;
    if (st.n_roles == pg.pg_cursize)
    {
        if (st.n_matched == pg.pg_cursize)
            state = OBJ_CLEAN;
        else
        {
            state = OBJ_MISPLACED;
            pg.state = pg.state | PG_HAS_MISPLACED;
        }
    }
    else if (st.n_roles < pg.pg_minsize)
    {
        printf("Object is unfound: inode=%lu stripe=%lu version=%lu/%lu\n", st.oid.inode, st.oid.stripe, st.target_ver, st.max_ver);
        state = OBJ_INCOMPLETE;
        pg.state = pg.state | PG_HAS_UNFOUND;
    }
    else
    {
        printf("Object is degraded: inode=%lu stripe=%lu version=%lu/%lu\n", st.oid.inode, st.oid.stripe, st.target_ver, st.max_ver);
        state = OBJ_DEGRADED;
        pg.state = pg.state | PG_HAS_DEGRADED;
    }
    if (st.n_copies > pg.pg_size)
    {
        state |= OBJ_OVERCOPIED;
        pg.state = pg.state | PG_HAS_UNCLEAN;
    }
    if (st.n_stable < st.n_copies)
    {
        state |= OBJ_NEEDS_STABLE;
        pg.state = pg.state | PG_HAS_UNCLEAN;
    }
    if (st.target_ver < st.max_ver || st.has_old_unstable)
    {
        state |= OBJ_NEEDS_ROLLBACK;
        pg.state = pg.state | PG_HAS_UNCLEAN;
        pg.ver_override[st.oid] = st.target_ver;
    }
    if (st.is_buggy)
    {
        state |= OBJ_BUGGY;
        // FIXME: bring pg offline
        throw std::runtime_error("buggy object state");
    }
    if (state != OBJ_CLEAN)
    {
        st.osd_set.clear();
        for (int i = st.ver_start; i < st.ver_end; i++)
        {
            st.osd_set.push_back((pg_obj_loc_t){
                .role = (all[i].oid.stripe & STRIPE_MASK),
                .osd_num = all[i].osd_num,
                .stable = all[i].is_stable,
            });
        }
        std::sort(st.osd_set.begin(), st.osd_set.end());
        auto it = pg.state_dict.find(st.osd_set);
        if (it == pg.state_dict.end())
        {
            std::vector<uint64_t> read_target;
            read_target.resize(pg.pg_size);
            for (int i = 0; i < pg.pg_size; i++)
            {
                read_target[i] = 0;
            }
            for (auto & o: st.osd_set)
            {
                read_target[o.role] = o.osd_num;
            }
            pg.state_dict[st.osd_set] = {
                .read_target = read_target,
                .osd_set = st.osd_set,
                .state = state,
                .object_count = 1,
            };
            it = pg.state_dict.find(st.osd_set);
        }
        else
        {
            it->second.object_count++;
        }
        pg.obj_states[st.oid] = &it->second;
        if (st.target_ver < st.max_ver)
        {
            pg.ver_override[st.oid] = st.target_ver;
        }
        if (state & (OBJ_NEEDS_ROLLBACK | OBJ_NEEDS_STABLE))
        {
            spp::sparse_hash_map<obj_piece_id_t, obj_piece_ver_t> pieces;
            for (int i = st.obj_start; i < st.obj_end; i++)
            {
                auto & pcs = pieces[(obj_piece_id_t){ .oid = all[i].oid, .osd_num = all[i].osd_num }];
                if (!pcs.max_ver)
                {
                    pcs.max_ver = all[i].version;
                }
                if (all[i].is_stable && !pcs.stable_ver)
                {
                    pcs.stable_ver = all[i].version;
                }
            }
            for (auto pp: pieces)
            {
                auto & pcs = pp.second;
                if (pcs.stable_ver < pcs.max_ver)
                {
                    auto & act = obj_stab_actions[pp.first];
                    if (pcs.max_ver > st.target_ver)
                    {
                        act.rollback = true;
                        act.rollback_to = st.target_ver;
                    }
                    else if (pcs.max_ver < st.target_ver && pcs.stable_ver < pcs.max_ver)
                    {
                        act.rollback = true;
                        act.rollback_to = pcs.stable_ver;
                    }
                    if (pcs.max_ver >= st.target_ver && pcs.stable_ver < st.target_ver)
                    {
                        act.make_stable = true;
                        act.stable_to = st.target_ver;
                    }
                }
            }
        }
    }
    else
        pg.clean_count++;
}

// FIXME: Write at least some tests for this function
void pg_t::calc_object_states()
{
    auto & pg = *this;
    // Copy all object lists into one array
    std::vector<obj_ver_role> all;
    auto ps = pg.peering_state;
    for (auto it: ps->list_results)
    {
        auto nstab = it.second.stable_count;
        auto n = it.second.total_count;
        auto osd_num = it.first;
        uint64_t start = all.size();
        all.resize(start + n);
        obj_ver_id *ov = it.second.buf;
        for (uint64_t i = 0; i < n; i++, ov++)
        {
            all[start+i] = {
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
    std::sort(all.begin(), all.end());
    // Walk over it and check object states
    pg.clean_count = 0;
    pg.state = 0;
    int replica = 0;
    pg_obj_state_check_t st;
    for (int i = 0; i < all.size(); i++)
    {
        if (st.oid.inode != all[i].oid.inode ||
            st.oid.stripe != (all[i].oid.stripe & ~STRIPE_MASK))
        {
            if (st.oid.inode != 0)
            {
                // Remember object state
                st.obj_end = st.ver_end = i;
                remember_object(st, all);
            }
            st.obj_start = st.ver_start = i;
            st.oid = { .inode = all[i].oid.inode, .stripe = all[i].oid.stripe & ~STRIPE_MASK };
            st.max_ver = st.target_ver = all[i].version;
            st.has_roles = st.n_copies = st.n_roles = st.n_stable = st.n_matched = 0;
            st.is_buggy = st.has_old_unstable = false;
        }
        else if (st.target_ver != all[i].version)
        {
            if (st.n_stable > 0 || st.n_roles >= pg.pg_minsize)
            {
                // Last processed version is either recoverable or stable, choose it as target and skip previous versions
                st.ver_end = i;
                i++;
                while (i < all.size() && st.oid.inode == all[i].oid.inode &&
                    st.oid.stripe == (all[i].oid.stripe & ~STRIPE_MASK))
                {
                    if (!all[i].is_stable)
                    {
                        st.has_old_unstable = true;
                    }
                    i++;
                }
                st.obj_end = i;
                i--;
                continue;
            }
            else
            {
                // Last processed version is unstable and unrecoverable
                // We'll know that because target_ver < max_ver
                st.ver_start = i;
                st.target_ver = all[i].version;
                st.has_roles = st.n_copies = st.n_roles = st.n_stable = st.n_matched = 0;
            }
        }
        replica = (all[i].oid.stripe & STRIPE_MASK);
        st.n_copies++;
        if (replica >= pg.pg_size)
        {
            // FIXME In the future, check it against the PG epoch number to handle replication factor/scheme changes
            st.is_buggy = true;
        }
        else
        {
            if (all[i].is_stable)
            {
                st.n_stable++;
            }
            if (pg.cur_set[replica] == all[i].osd_num)
            {
                st.n_matched++;
            }
            if (!(st.has_roles & (1 << replica)))
            {
                st.has_roles = st.has_roles | (1 << replica);
                st.n_roles++;
            }
        }
    }
    if (st.oid.inode != 0)
    {
        // Remember object state
        st.obj_end = st.ver_end = all.size();
        remember_object(st, all);
    }
    if (pg.pg_cursize < pg.pg_size)
    {
        pg.state = pg.state | PG_DEGRADED;
    }
    printf(
        "PG %u is active%s%s%s%s%s\n", pg.pg_num,
        (pg.state & PG_DEGRADED) ? " + degraded" : "",
        (pg.state & PG_HAS_UNFOUND) ? " + has_unfound" : "",
        (pg.state & PG_HAS_DEGRADED) ? " + has_degraded" : "",
        (pg.state & PG_HAS_MISPLACED) ? " + has_misplaced" : "",
        (pg.state & PG_HAS_UNCLEAN) ? " + has_unclean" : ""
    );
    pg.state = pg.state | PG_ACTIVE;
}
