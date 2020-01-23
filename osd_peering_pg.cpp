#include "osd_peering_pg.h"

void pg_t::remember_object(pg_obj_state_check_t &st, std::vector<obj_ver_role> &all, int end)
{
    auto & pg = *this;
    // Remember the decision
    uint64_t state = 0;
    if (st.n_roles == pg.pg_size)
    {
        if (st.n_matched == pg.pg_size)
            state = OBJ_CLEAN;
        else
            state = OBJ_MISPLACED;
    }
    else if (st.n_roles < pg.pg_minsize)
        state = OBJ_INCOMPLETE;
    else
        state = OBJ_DEGRADED;
    if (st.n_copies > pg.pg_size)
        state |= OBJ_OVERCOPIED;
    if (st.n_stable < st.n_copies)
        state |= OBJ_NONSTABILIZED;
    if (st.target_ver < st.max_ver)
        state |= OBJ_UNDERWRITTEN;
    if (st.is_buggy)
        state |= OBJ_BUGGY;
    if (state != OBJ_CLEAN)
    {
        st.osd_set.clear();
        for (int i = st.start; i < end; i++)
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
            pg.state_dict[st.osd_set] = {
                .osd_set = st.osd_set,
                .state = state,
                .object_count = 1,
            };
            it = pg.state_dict.find(st.osd_set);
        }
        else
            it->second.object_count++;
        pg.obj_states[st.oid] = &it->second;
        if (state & OBJ_UNDERWRITTEN)
        {
            pg.ver_override[st.oid] = {
                .max_ver = st.max_ver,
                .target_ver = st.target_ver,
            };
        }
    }
    else
        pg.clean_count++;
}

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
    int replica = 0;
    pg_obj_state_check_t st;
    for (int i = 0; i < all.size(); i++)
    {
        if (st.oid.inode != all[i].oid.inode ||
            st.oid.stripe != (all[i].oid.stripe >> STRIPE_SHIFT))
        {
            if (st.oid.inode != 0)
            {
                // Remember object state
                remember_object(st, all, i);
            }
            st.start = i;
            st.oid = { .inode = all[i].oid.inode, .stripe = all[i].oid.stripe >> STRIPE_SHIFT };
            st.max_ver = st.target_ver = all[i].version;
            st.has_roles = st.n_copies = st.n_roles = st.n_stable = st.n_matched = 0;
            st.is_buggy = false;
        }
        if (st.target_ver != all[i].version)
        {
            if (st.n_stable > 0 || st.n_roles >= pg.pg_minsize)
            {
                // Version is either recoverable or stable, choose it as target and skip previous versions
                remember_object(st, all, i);
                while (i < all.size() && st.oid.inode == all[i].oid.inode &&
                    st.oid.stripe == (all[i].oid.stripe >> STRIPE_SHIFT))
                {
                    i++;
                }
                continue;
            }
            else
            {
                // Remember that there are newer unrecoverable versions
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
            if (pg.target_set[replica] == all[i].osd_num)
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
        remember_object(st, all, all.size());
    }
}
