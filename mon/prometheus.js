// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

const metric_help =
`# HELP vitastor_object_bytes Total size of objects in cluster in bytes
# TYPE vitastor_object_bytes gauge
# HELP vitastor_object_count Total number of objects in cluster
# TYPE vitastor_object_count gauge
# HELP vitastor_stat_count Total operation count
# TYPE vitastor_stat_count counter
# HELP vitastor_stat_usec Total operation latency in usec
# TYPE vitastor_stat_usec counter
# HELP vitastor_stat_bytes Total operation size in bytes
# HELP vitastor_stat_bytes counter

# HELP vitastor_image_raw_used Image raw used size in bytes
# TYPE vitastor_image_raw_used counter
# HELP vitastor_image_stat_count Per-image total operation count
# TYPE vitastor_image_stat_count counter
# HELP vitastor_image_stat_usec Per-image total operation latency
# TYPE vitastor_image_stat_usec counter
# HELP vitastor_image_stat_bytes Per-image total operation size in bytes
# TYPE vitastor_image_stat_bytes counter

# HELP vitastor_osd_status OSD up/down status
# TYPE vitastor_osd_status gauge
# HELP vitastor_osd_size_bytes OSD total space in bytes
# TYPE vitastor_osd_size_bytes gauge
# HELP vitastor_osd_free_bytes OSD free space in bytes
# TYPE vitastor_osd_free_bytes gauge
# HELP vitastor_osd_stat_count Per-image total operation count
# TYPE vitastor_osd_stat_count counter
# HELP vitastor_osd_stat_usec Per-image total operation latency
# TYPE vitastor_osd_stat_usec counter
# HELP vitastor_osd_stat_bytes Per-image total operation size in bytes
# TYPE vitastor_osd_stat_bytes counter

# HELP vitastor_monitor_info Monitor info, 1 is master, 0 is standby
# TYPE vitastor_monitor_info gauge

# HELP vitastor_pool_info Pool configuration (in labels)
# TYPE vitastor_pool_info gauge
# HELP vitastor_pool_status Pool up/down status
# TYPE vitastor_pool_status gauge
# HELP vitastor_pool_raw_to_usable Raw to usable space ratio
# TYPE vitastor_pool_raw_to_usable gauge
# HELP vitastor_pool_space_efficiency Pool space usage efficiency
# TYPE vitastor_pool_space_efficiency gauge
# HELP vitastor_pool_total_raw_tb Total raw space in pool in TB
# TYPE vitastor_pool_total_raw_tb gauge
# HELP vitastor_pool_used_raw_tb Used raw space in pool in TB
# TYPE vitastor_pool_used_raw_tb gauge
# HELP vitastor_pg_count PG counts by state
# HELP vitastor_pg_count gauge

`;

function export_prometheus_metrics(st)
{
    let res = metric_help;

    // Global statistics

    for (const k in st.stats.object_bytes)
    {
        res += `vitastor_object_bytes{object_type="${k}"} ${st.stats.object_bytes[k]}\n`;
    }

    for (const k in st.stats.object_counts)
    {
        res += `vitastor_object_count{object_type="${k}"} ${st.stats.object_counts[k]}\n`;
    }

    for (const typ of [ 'op', 'subop', 'recovery' ])
    {
        for (const op in st.stats[typ+"_stats"]||{})
        {
            const op_stat = st.stats[typ+"_stats"][op];
            for (const key of [ 'count', 'usec', 'bytes' ])
            {
                res += `vitastor_stat_${key}{op="${op}",op_type="${typ}"} ${op_stat[key]||0}\n`;
            }
        }
    }

    // Per-image statistics

    for (const pool in st.inode.stats)
    {
        for (const inode in st.inode.stats[pool])
        {
            const ist = st.inode.stats[pool][inode];
            const inode_name = ((st.config.inode[pool]||{})[inode]||{}).name||'';
            const inode_label = `image_name="${addslashes(inode_name)}",inode_num="${inode}",pool_id="${pool}"`;
            res += `vitastor_image_raw_used{${inode_label}} ${ist.raw_used||0}\n`;
            for (const op of [ 'read', 'write', 'delete' ])
            {
                for (const k of [ 'count', 'usec', 'bytes' ])
                {
                    if (ist[op])
                    {
                        res += `vitastor_image_stat_${k}{${inode_label},op="${op}"} ${ist[op][k]||0}\n`;
                    }
                }
            }
        }
    }

    // Per-OSD statistics

    for (const osd in st.osd.stats)
    {
        const osd_stat = st.osd.stats[osd];
        const up = st.osd.state[osd] && st.osd.state[osd].state == 'up' ? 1 : 0;
        res += `vitastor_osd_status{host="${addslashes(osd_stat.host)}",osd_num="${osd}"} ${up}\n`;
        res += `vitastor_osd_size_bytes{osd_num="${osd}"} ${osd_stat.size||0}\n`;
        res += `vitastor_osd_free_bytes{osd_num="${osd}"} ${osd_stat.free||0}\n`;
        for (const op in osd_stat.op_stats)
        {
            const ist = osd_stat.op_stats[op];
            for (const k of [ 'count', 'usec', 'bytes' ])
            {
                res += `vitastor_osd_stat_${k}{osd_num="${osd}",op="${op}",op_type="op"} ${ist[k]||0}\n`;
            }
        }
        for (const op in osd_stat.subop_stats)
        {
            const ist = osd_stat.subop_stats[op];
            for (const k of [ 'count', 'usec', 'bytes' ])
            {
                res += `vitastor_osd_stat_${k}{osd_num="${osd}",op="${op}",op_type="subop"} ${ist[k]||0}\n`;
            }
        }
    }

    // Monitor statistics

    for (const mon_id in st.mon.member)
    {
        const mon = st.mon.member[mon_id];
        const master = st.mon.master && st.mon.master.id == mon_id ? 1 : 0;
        const ip = (mon.ip instanceof Array ? mon.ip[0] : mon.ip) || '';
        res += `vitastor_monitor_info{monitor_hostname="${addslashes(mon.hostname)}",monitor_id="${mon_id}",monitor_ip="${addslashes(ip)}"} ${master}\n`;
    }

    // Per-pool statistics

    for (const pool_id in st.config.pools)
    {
        const pool_cfg = st.config.pools[pool_id];
        const pool_label = `pool_id="${pool_id}",pool_name="${addslashes(pool_cfg.name)}"`;
        const pool_stat = st.pool.stats[pool_id];
        res += `vitastor_pool_info{${pool_label}`+
            `,pool_scheme="${addslashes(pool_cfg.scheme)}"`+
            `,pg_size="${pool_cfg.pg_size||0}",pg_minsize="${pool_cfg.pg_minsize||0}"`+
            `,parity_chunks="${pool_cfg.parity_chunks||0}",pg_count="${pool_cfg.pg_count||0}"`+
            `,failure_domain="${addslashes(pool_cfg.failure_domain)}"`+
            `} 1\n`;
        if (!pool_stat)
        {
            continue;
        }
        res += `vitastor_pool_raw_to_usable{${pool_label}} ${pool_stat.raw_to_usable||0}\n`;
        res += `vitastor_pool_space_efficiency{${pool_label}} ${pool_stat.space_efficiency||0}\n`;
        res += `vitastor_pool_total_raw_tb{${pool_label}} ${pool_stat.total_raw_tb||0}\n`;
        res += `vitastor_pool_used_raw_tb{${pool_label}} ${pool_stat.used_raw_tb||0}\n`;

        // PG states and pool up/down status
        const real_pg_count = (Object.keys(((st.config.pgs||{}).items||{})[pool_id]||{}).length) || (0|pool_cfg.pg_count);
        const per_state = {
            active: 0,
            starting: 0,
            peering: 0,
            incomplete: 0,
            repeering: 0,
            stopping: 0,
            offline: 0,
            degraded: 0,
            has_inconsistent: 0,
            has_corrupted: 0,
            has_incomplete: 0,
            has_degraded: 0,
            has_misplaced: 0,
            has_unclean: 0,
            has_invalid: 0,
            left_on_dead: 0,
            scrubbing: 0,
        };
        const pool_pg_states = st.pg.state[pool_id] || {};
        for (let i = 1; i <= real_pg_count; i++)
        {
            if (!pool_pg_states[i])
            {
                per_state['offline'] = 1 + (per_state['offline']|0);
            }
            else
            {
                for (const st_name of pool_pg_states[i].state)
                {
                    per_state[st_name] = 1 + (per_state[st_name]|0);
                }
            }
        }
        for (const st_name in per_state)
        {
            res += `vitastor_pg_count{pg_state="${st_name}",${pool_label}} ${per_state[st_name]}\n`;
        }
        const pool_active = per_state['active'] >= real_pg_count ? 1 : 0;
        res += `vitastor_pool_status{${pool_label}} ${pool_active}\n`;
    }

    return res;
}

function addslashes(str)
{
    return ((str||'')+'').replace(/(["\n\\])/g, "\\$1"); // escape " \n \
}

module.exports = { export_prometheus_metrics };
