// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

function derive_osd_stats(st, prev, prev_diff)
{
    const diff = { op_stats: {}, subop_stats: {}, recovery_stats: {}, inode_stats: {} };
    if (!st || !st.time || !prev || !prev.time || prev.time >= st.time)
    {
        return prev_diff || diff;
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
        if (n > 0)
            diff.op_stats[op] = { ...c, bps: b*1000n/timediff, iops: n*1000n/timediff, lat: us/n };
    }
    for (const op in st.subop_stats||{})
    {
        const pr = prev && prev.subop_stats && prev.subop_stats[op];
        let c = st.subop_stats[op];
        c = { usec: BigInt(c.usec||0), count: BigInt(c.count||0) };
        const us = c.usec - BigInt(pr && pr.usec||0);
        const n = c.count - BigInt(pr && pr.count||0);
        if (n > 0)
            diff.subop_stats[op] = { ...c, iops: n*1000n/timediff, lat: us/n };
    }
    for (const op in st.recovery_stats||{})
    {
        const pr = prev && prev.recovery_stats && prev.recovery_stats[op];
        let c = st.recovery_stats[op];
        c = { bytes: BigInt(c.bytes||0), count: BigInt(c.count||0) };
        const b = c.bytes - BigInt(pr && pr.bytes||0);
        const n = c.count - BigInt(pr && pr.count||0);
        if (n > 0)
            diff.recovery_stats[op] = { ...c, bps: b*1000n/timediff, iops: n*1000n/timediff };
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
                    bps: (BigInt(c.bytes||0) - BigInt(pr && pr.bytes||0))*1000n/timediff,
                    iops: n*1000n/timediff,
                    lat: (BigInt(c.usec||0) - BigInt(pr && pr.usec||0))/(n || 1n),
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
    const sum_diff = { op_stats: {}, subop_stats: {}, recovery_stats: {} };
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
    for (const pool_id in state.pg.stats)
    {
        let object_size = 0;
        for (const osd_num of state.pg.stats[pool_id].write_osd_set||[])
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
        for (const pg_num in state.pg.stats[pool_id])
        {
            const st = state.pg.stats[pool_id][pg_num];
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
            }
        }
    }
    return { object_counts, object_bytes };
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
