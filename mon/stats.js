// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

function derive_osd_stats(st, prev, prev_diff)
{
    const diff = prev_diff || { op_stats: {}, subop_stats: {}, recovery_stats: {}, inode_stats: {} };
    if (!st || !st.time || !prev || !prev.time || prev.time >= st.time)
    {
        return diff;
    }
    const timediff = BigInt(st.time*1000 - prev.time*1000);
    for (const op in st.op_stats||{})
    {
        const pr = prev && prev.op_stats && prev.op_stats[op];
        let c = st.op_stats[op];
        c = { bytes: BigInt(c.bytes||0), usec: BigInt(c.usec||0), count: BigInt(c.count||0) };
        const b = c.bytes - BigInt(pr && pr.bytes||0);
        const us = c.usec - BigInt(pr && pr.usec||0);
        const n = c.count - BigInt(pr && pr.count||0);
        diff.op_stats[op] = { ...c, bps: n > 0 ? b*1000n/timediff : 0n, iops: n > 0 ? n*1000n/timediff : 0n, lat: n > 0 ? us/n : 0n };
    }
    for (const op in st.subop_stats||{})
    {
        const pr = prev && prev.subop_stats && prev.subop_stats[op];
        let c = st.subop_stats[op];
        c = { usec: BigInt(c.usec||0), count: BigInt(c.count||0) };
        const us = c.usec - BigInt(pr && pr.usec||0);
        const n = c.count - BigInt(pr && pr.count||0);
        diff.subop_stats[op] = { ...c, iops: n > 0 ? n*1000n/timediff : 0n, lat: n > 0 ? us/n : 0n };
    }
    for (const op in st.recovery_stats||{})
    {
        const pr = prev && prev.recovery_stats && prev.recovery_stats[op];
        let c = st.recovery_stats[op];
        c = { bytes: BigInt(c.bytes||0), count: BigInt(c.count||0) };
        const b = c.bytes - BigInt(pr && pr.bytes||0);
        const n = c.count - BigInt(pr && pr.count||0);
        diff.recovery_stats[op] = { ...c, bps: n > 0 ? b*1000n/timediff : 0n, iops: n > 0 ? n*1000n/timediff : 0n };
    }
    for (const pool_id in st.inode_stats||{})
    {
        diff.inode_stats[pool_id] = {};
        for (const inode_num in st.inode_stats[pool_id])
        {
            const inode_diff = diff.inode_stats[pool_id][inode_num] = {};
            for (const op of [ 'read', 'write', 'delete' ])
            {
                const c = st.inode_stats[pool_id][inode_num][op];
                const pr = prev && prev.inode_stats && prev.inode_stats[pool_id] &&
                    prev.inode_stats[pool_id][inode_num] && prev.inode_stats[pool_id][inode_num][op];
                const n = BigInt(c.count||0) - BigInt(pr && pr.count||0);
                inode_diff[op] = {
                    bps: n > 0 ? (BigInt(c.bytes||0) - BigInt(pr && pr.bytes||0))*1000n/timediff : 0n,
                    iops: n > 0 ? n*1000n/timediff : 0n,
                    lat: n > 0 ? (BigInt(c.usec||0) - BigInt(pr && pr.usec||0))/n : 0n,
                };
            }
        }
    }
    return diff;
}

// sum_op_stats(this.state.osd, this.prev_stats)
function sum_op_stats(all_osd, prev_stats)
{
    for (const osd in all_osd.stats)
    {
        const cur = { ...all_osd.stats[osd], inode_stats: all_osd.inodestats[osd]||{} };
        prev_stats.osd_diff[osd] = derive_osd_stats(
            cur, prev_stats.osd_stats[osd], prev_stats.osd_diff[osd]
        );
        prev_stats.osd_stats[osd] = cur;
    }
    const sum_diff = { op_stats: {}, subop_stats: {}, recovery_stats: { degraded: {}, misplaced: {} } };
    // Sum derived values instead of deriving summed
    for (const osd in all_osd.state)
    {
        const derived = prev_stats.osd_diff[osd];
        if (!all_osd.state[osd] || !derived)
        {
            continue;
        }
        for (const type in sum_diff)
        {
            for (const op in derived[type]||{})
            {
                for (const k in derived[type][op])
                {
                    sum_diff[type][op] = sum_diff[type][op] || {};
                    sum_diff[type][op][k] = (sum_diff[type][op][k] || 0n) + derived[type][op][k];
                }
            }
        }
    }
    return sum_diff;
}

// sum_object_counts(this.state, this.config)
function sum_object_counts(state, global_config)
{
    const object_counts = { object: 0n, clean: 0n, misplaced: 0n, degraded: 0n, incomplete: 0n };
    const object_bytes = { object: 0n, clean: 0n, misplaced: 0n, degraded: 0n, incomplete: 0n };
    let pgstats = state.pgstats;
    if (state.pg.stats)
    {
        // Merge with old stats for seamless transition to new stats
        for (const pool_id in state.pg.stats)
        {
            pgstats[pool_id] = { ...(state.pg.stats[pool_id] || {}), ...(pgstats[pool_id] || {}) };
        }
    }
    const pool_per_osd = {};
    const clean_per_osd = {};
    for (const pool_id in pgstats)
    {
        let object_size = 0;
        for (const osd_num of pgstats[pool_id].write_osd_set||[])
        {
            if (osd_num && state.osd.stats[osd_num] && state.osd.stats[osd_num].block_size)
            {
                object_size = state.osd.stats[osd_num].block_size;
                break;
            }
        }
        const pool_cfg = (state.config.pools[pool_id]||{});
        if (!object_size)
        {
            object_size = pool_cfg.block_size || global_config.block_size || 131072;
        }
        if (pool_cfg.scheme !== 'replicated')
        {
            object_size *= ((pool_cfg.pg_size||0) - (pool_cfg.parity_chunks||0));
        }
        object_size = BigInt(object_size);
        for (const pg_num in pgstats[pool_id])
        {
            const st = pgstats[pool_id][pg_num];
            if (st)
            {
                for (const k in object_counts)
                {
                    if (st[k+'_count'])
                    {
                        object_counts[k] += BigInt(st[k+'_count']);
                        object_bytes[k] += BigInt(st[k+'_count']) * object_size;
                    }
                }
                if (st.object_count)
                {
                    for (const pg_osd of (((state.pg.config.items||{})[pool_id]||{})[pg_num]||{}).osd_set||[])
                    {
                        if (!(pg_osd in clean_per_osd))
                        {
                            clean_per_osd[pg_osd] = 0n;
                        }
                        clean_per_osd[pg_osd] += BigInt(st.object_count);
                        pool_per_osd[pg_osd] = pool_per_osd[pg_osd]||{};
                        pool_per_osd[pg_osd][pool_id] = true;
                    }
                }
            }
        }
    }
    // If clean_per_osd[osd] is larger than osd capacity then it will fill up during rebalance
    let backfillfull_pools = {};
    let backfillfull_osds = {};
    for (const osd in clean_per_osd)
    {
        const st = state.osd.stats[osd];
        if (!st || !st.size || !st.data_block_size)
        {
            continue;
        }
        let cap = BigInt(st.size)/BigInt(st.data_block_size);
        cap = cap * BigInt((global_config.osd_backfillfull_ratio||0.99)*1000000) / 1000000n;
        if (cap < clean_per_osd[osd])
        {
            backfillfull_osds[osd] = { cap: BigInt(st.size), clean: clean_per_osd[osd]*BigInt(st.data_block_size) };
            for (const pool_id in pool_per_osd[osd])
            {
                backfillfull_pools[pool_id] = true;
            }
        }
    }
    backfillfull_pools = Object.keys(backfillfull_pools).sort();
    return { object_counts, object_bytes, backfillfull_pools, backfillfull_osds };
}

// sum_inode_stats(this.state, this.prev_stats)
function sum_inode_stats(state, prev_stats)
{
    const inode_stats = {};
    const inode_stub = () => ({
        raw_used: 0n,
        read: { count: 0n, usec: 0n, bytes: 0n, bps: 0n, iops: 0n, lat: 0n },
        write: { count: 0n, usec: 0n, bytes: 0n, bps: 0n, iops: 0n, lat: 0n },
        delete: { count: 0n, usec: 0n, bytes: 0n, bps: 0n, iops: 0n, lat: 0n },
    });
    const seen_pools = {};
    for (const pool_id in state.config.pools)
    {
        seen_pools[pool_id] = true;
        state.pool.stats[pool_id] = state.pool.stats[pool_id] || {};
        state.pool.stats[pool_id].used_raw_tb = 0n;
    }
    for (const osd_num in state.osd.space)
    {
        for (const pool_id in state.osd.space[osd_num])
        {
            state.pool.stats[pool_id] = state.pool.stats[pool_id] || {};
            if (!seen_pools[pool_id])
            {
                state.pool.stats[pool_id].used_raw_tb = 0n;
                seen_pools[pool_id] = true;
            }
            inode_stats[pool_id] = inode_stats[pool_id] || {};
            for (const inode_num in state.osd.space[osd_num][pool_id])
            {
                const u = BigInt(state.osd.space[osd_num][pool_id][inode_num]||0);
                if (inode_num)
                {
                    inode_stats[pool_id][inode_num] = inode_stats[pool_id][inode_num] || inode_stub();
                    inode_stats[pool_id][inode_num].raw_used += u;
                }
                state.pool.stats[pool_id].used_raw_tb += u;
            }
        }
    }
    for (const pool_id in seen_pools)
    {
        const used = state.pool.stats[pool_id].used_raw_tb;
        state.pool.stats[pool_id].used_raw_tb = Number(used)/1024/1024/1024/1024;
    }
    for (const osd_num in state.osd.state)
    {
        const ist = state.osd.inodestats[osd_num];
        if (!ist || !state.osd.state[osd_num])
        {
            continue;
        }
        for (const pool_id in ist)
        {
            inode_stats[pool_id] = inode_stats[pool_id] || {};
            for (const inode_num in ist[pool_id])
            {
                inode_stats[pool_id][inode_num] = inode_stats[pool_id][inode_num] || inode_stub();
                for (const op of [ 'read', 'write', 'delete' ])
                {
                    inode_stats[pool_id][inode_num][op].count += BigInt(ist[pool_id][inode_num][op].count||0);
                    inode_stats[pool_id][inode_num][op].usec += BigInt(ist[pool_id][inode_num][op].usec||0);
                    inode_stats[pool_id][inode_num][op].bytes += BigInt(ist[pool_id][inode_num][op].bytes||0);
                }
            }
        }
    }
    for (const osd in state.osd.state)
    {
        const osd_diff = prev_stats.osd_diff[osd];
        if (!osd_diff || !state.osd.state[osd])
        {
            continue;
        }
        for (const pool_id in osd_diff.inode_stats)
        {
            for (const inode_num in prev_stats.osd_diff[osd].inode_stats[pool_id])
            {
                inode_stats[pool_id][inode_num] = inode_stats[pool_id][inode_num] || inode_stub();
                for (const op of [ 'read', 'write', 'delete' ])
                {
                    const op_diff = prev_stats.osd_diff[osd].inode_stats[pool_id][inode_num][op] || {};
                    const op_st = inode_stats[pool_id][inode_num][op];
                    op_st.bps += op_diff.bps;
                    op_st.iops += op_diff.iops;
                    op_st.lat += op_diff.lat;
                    op_st.n_osd = (op_st.n_osd || 0) + 1;
                }
            }
        }
    }
    for (const pool_id in inode_stats)
    {
        for (const inode_num in inode_stats[pool_id])
        {
            let nonzero = inode_stats[pool_id][inode_num].raw_used > 0;
            for (const op of [ 'read', 'write', 'delete' ])
            {
                const op_st = inode_stats[pool_id][inode_num][op];
                if (op_st.n_osd)
                {
                    op_st.lat /= BigInt(op_st.n_osd);
                    delete op_st.n_osd;
                }
                if (op_st.bps > 0 || op_st.iops > 0)
                    nonzero = true;
            }
            if (!nonzero && (!state.config.inode[pool_id] || !state.config.inode[pool_id][inode_num]))
            {
                // Deleted inode (no data, no I/O, no config)
                delete inode_stats[pool_id][inode_num];
            }
        }
    }
    return { inode_stats, seen_pools };
}

function serialize_bigints(obj)
{
    obj = { ...obj };
    for (const k in obj)
    {
        if (typeof obj[k] == 'bigint')
        {
            obj[k] = ''+obj[k];
        }
        else if (typeof obj[k] == 'object')
        {
            obj[k] = serialize_bigints(obj[k]);
        }
    }
    return obj;
}

module.exports = {
    derive_osd_stats,
    sum_op_stats,
    sum_object_counts,
    sum_inode_stats,
    serialize_bigints,
};
