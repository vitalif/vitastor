// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

const http = require('http');
const crypto = require('crypto');
const os = require('os');
const WebSocket = require('ws');
const LPOptimizer = require('./lp-optimizer.js');
const stableStringify = require('./stable-stringify.js');
const PGUtil = require('./PGUtil.js');

// FIXME document all etcd keys and config variables in the form of JSON schema or similar
const etcd_nonempty_keys = {
    'config/global': 1,
    'config/node_placement': 1,
    'config/pools': 1,
    'config/pgs': 1,
    'history/last_clean_pgs': 1,
    'stats': 1,
};
const etcd_allow = new RegExp('^'+[
    'config/global',
    'config/node_placement',
    'config/pools',
    'config/osd/[1-9]\\d*',
    'config/pgs',
    'config/inode/[1-9]\\d*/[1-9]\\d*',
    'osd/state/[1-9]\\d*',
    'osd/stats/[1-9]\\d*',
    'osd/inodestats/[1-9]\\d*',
    'osd/space/[1-9]\\d*',
    'mon/master',
    'pg/state/[1-9]\\d*/[1-9]\\d*',
    'pg/stats/[1-9]\\d*/[1-9]\\d*',
    'pg/history/[1-9]\\d*/[1-9]\\d*',
    'history/last_clean_pgs',
    'inode/stats/[1-9]\\d*/[1-9]\\d*',
    'stats',
    'index/image/.*',
    'index/maxid/[1-9]\\d*',
].join('$|^')+'$');

const etcd_tree = {
    config: {
        /* global: {
            // WARNING: NOT ALL OF THESE ARE ACTUALLY CONFIGURABLE HERE
            // THIS IS JUST A POOR MAN'S CONFIG DOCUMENTATION
            // etcd connection
            config_path: "/etc/vitastor/vitastor.conf",
            etcd_address: "10.0.115.10:2379/v3",
            etcd_prefix: "/vitastor",
            // mon
            etcd_mon_ttl: 30, // min: 10
            etcd_mon_timeout: 1000, // ms. min: 0
            etcd_mon_retries: 5, // min: 0
            mon_change_timeout: 1000, // ms. min: 100
            mon_stats_timeout: 1000, // ms. min: 100
            osd_out_time: 600, // seconds. min: 0
            placement_levels: { datacenter: 1, rack: 2, host: 3, osd: 4, ... },
            // client and osd
            tcp_header_buffer_size: 65536,
            use_sync_send_recv: false,
            use_rdma: true,
            rdma_device: null, // for example, "rocep5s0f0"
            rdma_port_num: 1,
            rdma_gid_index: 0,
            rdma_mtu: 4096,
            rdma_max_sge: 128,
            rdma_max_send: 32,
            rdma_max_recv: 8,
            rdma_max_msg: 1048576,
            log_level: 0,
            block_size: 131072,
            disk_alignment: 4096,
            bitmap_granularity: 4096,
            immediate_commit: false, // 'all' or 'small'
            client_dirty_limit: 33554432,
            peer_connect_interval: 5, // seconds. min: 1
            peer_connect_timeout: 5, // seconds. min: 1
            osd_idle_timeout: 5, // seconds. min: 1
            osd_ping_timeout: 5, // seconds. min: 1
            up_wait_retry_interval: 500, // ms. min: 50
            // osd
            etcd_report_interval: 30, // min: 10
            run_primary: true,
            bind_address: "0.0.0.0",
            bind_port: 0,
            autosync_interval: 5,
            client_queue_depth: 128, // unused
            recovery_queue_depth: 4,
            recovery_sync_batch: 16,
            readonly: false,
            no_recovery: false,
            no_rebalance: false,
            print_stats_interval: 3,
            slow_log_interval: 10,
            // blockstore - fixed in superblock
            block_size,
            disk_alignment,
            journal_block_size,
            meta_block_size,
            bitmap_granularity,
            journal_device,
            journal_offset,
            journal_size,
            disable_journal_fsync,
            data_device,
            data_offset,
            data_size,
            disable_data_fsync,
            meta_device,
            meta_offset,
            disable_meta_fsync,
            disable_device_lock,
            // blockstore - configurable
            max_write_iodepth,
            min_flusher_count: 1,
            max_flusher_count: 256,
            inmemory_metadata,
            inmemory_journal,
            journal_sector_buffer_count,
            journal_no_same_sector_overwrites,
        }, */
        global: {},
        /* node_placement: {
            host1: { level: 'host', parent: 'rack1' },
            ...
        }, */
        node_placement: {},
        /* pools: {
            <id>: {
                name: 'testpool',
                // jerasure uses Reed-Solomon-Vandermonde codes
                scheme: 'replicated' | 'xor' | 'jerasure',
                pg_size: 3,
                pg_minsize: 2,
                // number of parity chunks, required for jerasure
                parity_chunks?: 1,
                pg_count: 100,
                failure_domain: 'host',
                max_osd_combinations: 10000,
                pg_stripe_size: 4194304,
                root_node?: 'rack1',
                // restrict pool to OSDs having all of these tags
                osd_tags?: 'nvme' | [ 'nvme', ... ],
            },
            ...
        }, */
        pools: {},
        osd: {
            /* <id>: { reweight?: 1, tags?: [ 'nvme', ... ] }, ... */
        },
        /* pgs: {
            hash: string,
            items: {
                <pool_id>: {
                    <pg_id>: {
                        osd_set: [ 1, 2, 3 ],
                        primary: 1,
                        pause: false,
                    }
                }
            }
        }, */
        pgs: {},
        /* inode: {
            <pool_id>: {
                <inode_t>: {
                    name: string,
                    size?: uint64_t, // bytes
                    parent_pool?: <pool_id>,
                    parent_id?: <inode_t>,
                    readonly?: boolean,
                }
            }
        }, */
        inode: {},
    },
    osd: {
        state: {
            /* <osd_num_t>: {
                state: "up",
                addresses: string[],
                host: string,
                port: uint16_t,
                primary_enabled: boolean,
                blockstore_enabled: boolean,
            }, */
        },
        stats: {
            /* <osd_num_t>: {
                time: number, // unix time
                blockstore_ready: boolean,
                size: uint64_t, // bytes
                free: uint64_t, // bytes
                host: string,
                op_stats: {
                    <string>: { count: uint64_t, usec: uint64_t, bytes: uint64_t },
                },
                subop_stats: {
                    <string>: { count: uint64_t, usec: uint64_t },
                },
                recovery_stats: {
                    degraded: { count: uint64_t, bytes: uint64_t },
                    misplaced: { count: uint64_t, bytes: uint64_t },
                },
            }, */
        },
        inodestats: {
            /* <inode_t>: {
                read: { count: uint64_t, usec: uint64_t, bytes: uint64_t },
                write: { count: uint64_t, usec: uint64_t, bytes: uint64_t },
                delete: { count: uint64_t, usec: uint64_t, bytes: uint64_t },
            }, */
        },
        space: {
            /* <osd_num_t>: {
                <inode_t>: uint64_t, // bytes
            }, */
        },
    },
    mon: {
        master: {
            /* ip: [ string ], */
        },
    },
    pg: {
        state: {
            /* <pool_id>: {
                <pg_id>: {
                    primary: osd_num_t,
                    state: ("starting"|"peering"|"incomplete"|"active"|"repeering"|"stopping"|"offline"|
                        "degraded"|"has_incomplete"|"has_degraded"|"has_misplaced"|"has_unclean"|
                        "has_invalid"|"left_on_dead")[],
                }
            }, */
        },
        stats: {
            /* <pool_id>: {
                <pg_id>: {
                    object_count: uint64_t,
                    clean_count: uint64_t,
                    misplaced_count: uint64_t,
                    degraded_count: uint64_t,
                    incomplete_count: uint64_t,
                    write_osd_set: osd_num_t[],
                },
            }, */
        },
        history: {
            /* <pool_id>: {
                <pg_id>: {
                    osd_sets: osd_num_t[][],
                    all_peers: osd_num_t[],
                    epoch: uint32_t,
                },
            }, */
        },
    },
    inode: {
        stats: {
            /* <pool_id>: {
                <inode_t>: {
                    raw_used: uint64_t, // raw used bytes on OSDs
                    read: { count: uint64_t, usec: uint64_t, bytes: uint64_t },
                    write: { count: uint64_t, usec: uint64_t, bytes: uint64_t },
                    delete: { count: uint64_t, usec: uint64_t, bytes: uint64_t },
                },
            }, */
        },
    },
    pool: {
        stats: {
            /* <pool_id>: {
                used_raw_tb: float, // used raw space in the pool
                total_raw_tb: float, // maximum amount of space in the pool
                raw_to_usable: float, // raw to usable ratio
                space_efficiency: float, // 0..1
            } */
        },
    },
    stats: {
        /* op_stats: {
            <string>: { count: uint64_t, usec: uint64_t, bytes: uint64_t },
        },
        subop_stats: {
            <string>: { count: uint64_t, usec: uint64_t },
        },
        recovery_stats: {
            degraded: { count: uint64_t, bytes: uint64_t },
            misplaced: { count: uint64_t, bytes: uint64_t },
        },
        object_counts: {
            object: uint64_t,
            clean: uint64_t,
            misplaced: uint64_t,
            degraded: uint64_t,
            incomplete: uint64_t,
        }, */
    },
    history: {
        last_clean_pgs: {},
    },
    index: {
        image: {
            /* <name>: {
                id: uint64_t,
                pool_id: uint64_t,
            }, */
        },
        maxid: {
            /* <pool_id>: uint64_t, */
        },
    },
};

// FIXME Split into several files
class Mon
{
    constructor(config)
    {
        // FIXME: Maybe prefer local etcd
        this.etcd_urls = [];
        for (let url of config.etcd_url.split(/,/))
        {
            let scheme = 'http';
            url = url.trim().replace(/^(https?):\/\//, (m, m1) => { scheme = m1; return ''; });
            if (!/\/[^\/]/.exec(url))
                url += '/v3';
            this.etcd_urls.push(scheme+'://'+url);
        }
        this.verbose = config.verbose || 0;
        this.config = {};
        this.etcd_prefix = config.etcd_prefix || '/vitastor';
        this.etcd_prefix = this.etcd_prefix.replace(/\/\/+/g, '/').replace(/^\/?(.*[^\/])\/?$/, '/$1');
        this.etcd_start_timeout = (config.etcd_start_timeout || 5) * 1000;
        this.state = JSON.parse(JSON.stringify(this.constructor.etcd_tree));
    }

    async start()
    {
        await this.load_config();
        await this.get_lease();
        await this.become_master();
        await this.load_cluster_state();
        await this.start_watcher(this.config.etcd_mon_retries);
        await this.recheck_pgs();
    }

    async load_config()
    {
        const res = await this.etcd_call('/kv/txn', { success: [
            { requestRange: { key: b64(this.etcd_prefix+'/config/global') } }
        ] }, this.etcd_start_timeout, -1);
        if (res.responses[0].response_range.kvs)
        {
            this.parse_kv(res.responses[0].response_range.kvs[0]);
        }
        this.check_config();
    }

    check_config()
    {
        this.config.etcd_mon_ttl = Number(this.config.etcd_mon_ttl) || 30;
        if (this.config.etcd_mon_ttl < 10)
        {
            this.config.etcd_mon_ttl = 10;
        }
        this.config.etcd_mon_timeout = Number(this.config.etcd_mon_timeout) || 0;
        if (this.config.etcd_mon_timeout <= 0)
        {
            this.config.etcd_mon_timeout = 1000;
        }
        this.config.etcd_mon_retries = Number(this.config.etcd_mon_retries) || 5;
        if (this.config.etcd_mon_retries < 0)
        {
            this.config.etcd_mon_retries = 0;
        }
        this.config.mon_change_timeout = Number(this.config.mon_change_timeout) || 1000;
        if (this.config.mon_change_timeout < 100)
        {
            this.config.mon_change_timeout = 100;
        }
        this.config.mon_stats_timeout = Number(this.config.mon_stats_timeout) || 1000;
        if (this.config.mon_stats_timeout < 100)
        {
            this.config.mon_stats_timeout = 100;
        }
        this.config.mon_stats_interval = Number(this.config.mon_stats_interval) || 5000;
        if (this.config.mon_stats_interval < 100)
        {
            this.config.mon_stats_interval = 100;
        }
        // After this number of seconds, a dead OSD will be removed from PG distribution
        this.config.osd_out_time = Number(this.config.osd_out_time) || 0;
        if (!this.config.osd_out_time)
        {
            this.config.osd_out_time = 600; // 10 minutes by default
        }
    }

    async start_watcher(retries)
    {
        let retry = 0;
        if (!retries || retries < 1)
        {
            retries = 1;
        }
        while (retries < 0 || retry < retries)
        {
            const base = 'ws'+this.etcd_urls[Math.floor(Math.random()*this.etcd_urls.length)].substr(4);
            const ok = await new Promise((ok, no) =>
            {
                const timer_id = setTimeout(() =>
                {
                    this.ws.close();
                    ok(false);
                }, this.config.etcd_mon_timeout);
                this.ws = new WebSocket(base+'/watch');
                const fail = () =>
                {
                    ok(false);
                };
                this.ws.on('error', fail);
                this.ws.on('open', () =>
                {
                    this.ws.removeListener('error', fail);
                    if (timer_id)
                        clearTimeout(timer_id);
                    ok(true);
                });
            });
            if (ok)
            {
                break;
            }
            this.ws = null;
            retry++;
        }
        if (!this.ws)
        {
            this.die('Failed to open etcd watch websocket');
        }
        this.ws.send(JSON.stringify({
            create_request: {
                key: b64(this.etcd_prefix+'/'),
                range_end: b64(this.etcd_prefix+'0'),
                start_revision: ''+this.etcd_watch_revision,
                watch_id: 1,
                progress_notify: true,
            },
        }));
        this.ws.on('message', (msg) =>
        {
            let data;
            try
            {
                data = JSON.parse(msg);
            }
            catch (e)
            {
            }
            if (!data || !data.result || !data.result.events)
            {
                if (!data || !data.result || !data.result.watch_id)
                {
                    console.error('Garbage received from watch websocket: '+msg);
                }
            }
            else
            {
                let stats_changed = false, changed = false, pg_states_changed = false;
                if (this.verbose)
                {
                    console.log('Revision '+data.result.header.revision+' events: ');
                }
                this.etcd_watch_revision = BigInt(data.result.header.revision)+BigInt(1);
                for (const e of data.result.events)
                {
                    this.parse_kv(e.kv);
                    const key = e.kv.key.substr(this.etcd_prefix.length);
                    if (key.substr(0, 11) == '/osd/stats/' || key.substr(0, 10) == '/pg/stats/' || key.substr(0, 16) == '/osd/inodestats/')
                    {
                        stats_changed = true;
                    }
                    else if (key.substr(0, 10) == '/pg/state/')
                    {
                        pg_states_changed = true;
                    }
                    else if (key != '/stats' && key.substr(0, 13) != '/inode/stats/')
                    {
                        changed = true;
                    }
                    if (this.verbose)
                    {
                        console.log(JSON.stringify(e));
                    }
                }
                if (pg_states_changed)
                {
                    this.save_last_clean().catch(console.error);
                }
                if (stats_changed)
                {
                    this.schedule_update_stats();
                }
                if (changed)
                {
                    this.schedule_recheck();
                }
            }
        });
    }

    async save_last_clean()
    {
        // last_clean_pgs is used to avoid extra data move when observing a series of changes in the cluster
        for (const pool_id in this.state.config.pools)
        {
            const pool_cfg = this.state.config.pools[pool_id];
            if (!this.validate_pool_cfg(pool_id, pool_cfg, false))
            {
                continue;
            }
            for (let pg_num = 1; pg_num <= pool_cfg.pg_count; pg_num++)
            {
                if (!this.state.pg.state[pool_id] ||
                    !this.state.pg.state[pool_id][pg_num] ||
                    !(this.state.pg.state[pool_id][pg_num].state instanceof Array))
                {
                    // Unclean
                    return;
                }
                let st = this.state.pg.state[pool_id][pg_num].state.join(',');
                if (st != 'active' && st != 'active,left_on_dead' && st != 'left_on_dead,active')
                {
                    // Unclean
                    return;
                }
            }
        }
        this.state.history.last_clean_pgs = JSON.parse(JSON.stringify(this.state.config.pgs));
        await this.etcd_call('/kv/txn', {
            success: [ { requestPut: {
                key: b64(this.etcd_prefix+'/history/last_clean_pgs'),
                value: b64(JSON.stringify(this.state.history.last_clean_pgs))
            } } ],
        }, this.etcd_start_timeout, 0);
    }

    async get_lease()
    {
        const max_ttl = this.config.etcd_mon_ttl + this.config.etcd_mon_timeout/1000*this.config.etcd_mon_retries;
        const res = await this.etcd_call('/lease/grant', { TTL: max_ttl }, this.config.etcd_mon_timeout, -1);
        this.etcd_lease_id = res.ID;
        setInterval(async () =>
        {
            const res = await this.etcd_call('/lease/keepalive', { ID: this.etcd_lease_id }, this.config.etcd_mon_timeout, this.config.etcd_mon_retries);
            if (!res.result.TTL)
            {
                this.die('Lease expired');
            }
        }, this.config.etcd_mon_timeout);
    }

    async become_master()
    {
        const state = { ip: this.local_ips() };
        while (1)
        {
            const res = await this.etcd_call('/kv/txn', {
                compare: [ { target: 'CREATE', create_revision: 0, key: b64(this.etcd_prefix+'/mon/master') } ],
                success: [ { requestPut: { key: b64(this.etcd_prefix+'/mon/master'), value: b64(JSON.stringify(state)), lease: ''+this.etcd_lease_id } } ],
            }, this.etcd_start_timeout, 0);
            if (res.succeeded)
            {
                break;
            }
            console.log('Waiting to become master');
            await new Promise(ok => setTimeout(ok, this.etcd_start_timeout));
        }
        console.log('Became master');
    }

    async load_cluster_state()
    {
        const res = await this.etcd_call('/kv/txn', { success: [
            { requestRange: { key: b64(this.etcd_prefix+'/'), range_end: b64(this.etcd_prefix+'0') } },
        ] }, this.etcd_start_timeout, -1);
        this.etcd_watch_revision = BigInt(res.header.revision)+BigInt(1);
        this.state = JSON.parse(JSON.stringify(this.constructor.etcd_tree));
        for (const response of res.responses)
        {
            for (const kv of response.response_range.kvs)
            {
                this.parse_kv(kv);
            }
        }
    }

    all_osds()
    {
        return Object.keys(this.state.osd.stats);
    }

    get_osd_tree()
    {
        const levels = this.config.placement_levels||{};
        levels.host = levels.host || 100;
        levels.osd = levels.osd || 101;
        const tree = { '': { children: [] } };
        let up_osds = {};
        for (const node_id in this.state.config.node_placement||{})
        {
            const node_cfg = this.state.config.node_placement[node_id];
            if (!node_id || /^\d/.exec(node_id) ||
                !node_cfg.level || !levels[node_cfg.level])
            {
                // All nodes must have non-empty non-numeric IDs and valid levels
                continue;
            }
            tree[node_id] = { id: node_id, level: node_cfg.level, parent: node_cfg.parent, children: [] };
        }
        // This requires monitor system time to be in sync with OSD system times (at least to some extent)
        const down_time = Date.now()/1000 - this.config.osd_out_time;
        for (const osd_num of this.all_osds().sort((a, b) => a - b))
        {
            const stat = this.state.osd.stats[osd_num];
            if (stat && stat.size && (this.state.osd.state[osd_num] || Number(stat.time) >= down_time))
            {
                // Numeric IDs are reserved for OSDs
                const osd_cfg = this.state.config.osd[osd_num];
                let reweight = osd_cfg && Number(osd_cfg.reweight);
                if (reweight < 0 || isNaN(reweight))
                    reweight = 1;
                if (this.state.osd.state[osd_num] && reweight > 0)
                {
                    // React to down OSDs immediately
                    up_osds[osd_num] = true;
                }
                tree[osd_num] = tree[osd_num] || {};
                tree[osd_num].id = osd_num;
                tree[osd_num].parent = tree[osd_num].parent || stat.host;
                tree[osd_num].level = 'osd';
                tree[osd_num].size = reweight * stat.size / 1024 / 1024 / 1024 / 1024; // terabytes
                if (osd_cfg && osd_cfg.tags)
                {
                    tree[osd_num].tags = (osd_cfg.tags instanceof Array ? [ ...osd_cfg.tags ] : [ osd_cfg.tags ])
                        .reduce((a, c) => { a[c] = true; return a; }, {});
                }
                delete tree[osd_num].children;
                if (!tree[tree[osd_num].parent])
                {
                    tree[tree[osd_num].parent] = {
                        id: tree[osd_num].parent,
                        level: 'host',
                        parent: null,
                        children: [],
                    };
                }
            }
        }
        for (const node_id in tree)
        {
            if (node_id === '')
            {
                continue;
            }
            const node_cfg = tree[node_id];
            const node_level = levels[node_cfg.level] || node_cfg.level;
            let parent_level = node_cfg.parent && tree[node_cfg.parent] && tree[node_cfg.parent].children
                && tree[node_cfg.parent].level;
            parent_level = parent_level ? (levels[parent_level] || parent_level) : null;
            // Parent's level must be less than child's; OSDs must be leaves
            const parent = parent_level && parent_level < node_level ? node_cfg.parent : '';
            tree[parent].children.push(tree[node_id]);
            delete node_cfg.parent;
        }
        return { up_osds, levels, osd_tree: tree };
    }

    async stop_all_pgs(pool_id)
    {
        let has_online = false, paused = true;
        for (const pg in this.state.config.pgs.items[pool_id]||{})
        {
            // FIXME: Change all (||{}) to ?. (optional chaining) at some point
            const cur_state = (((this.state.pg.state[pool_id]||{})[pg]||{}).state||[]).join(',');
            if (cur_state != '' && cur_state != 'offline')
            {
                has_online = true;
            }
            if (!this.state.config.pgs.items[pool_id][pg].pause)
            {
                paused = false;
            }
        }
        if (!paused)
        {
            console.log('Stopping all PGs for pool '+pool_id+' before changing PG count');
            const new_cfg = JSON.parse(JSON.stringify(this.state.config.pgs));
            for (const pg in new_cfg.items[pool_id])
            {
                new_cfg.items[pool_id][pg].pause = true;
            }
            // Check that no OSDs change their state before we pause PGs
            // Doing this we make sure that OSDs don't wake up in the middle of our "transaction"
            // and can't see the old PG configuration
            const checks = [];
            for (const osd_num of this.all_osds())
            {
                const key = b64(this.etcd_prefix+'/osd/state/'+osd_num);
                checks.push({ key, target: 'MOD', result: 'LESS', mod_revision: ''+this.etcd_watch_revision });
            }
            const res = await this.etcd_call('/kv/txn', {
                compare: [
                    { key: b64(this.etcd_prefix+'/mon/master'), target: 'LEASE', lease: ''+this.etcd_lease_id },
                    { key: b64(this.etcd_prefix+'/config/pgs'), target: 'MOD', mod_revision: ''+this.etcd_watch_revision, result: 'LESS' },
                    ...checks,
                ],
                success: [
                    { requestPut: { key: b64(this.etcd_prefix+'/config/pgs'), value: b64(JSON.stringify(new_cfg)) } },
                ],
            }, this.config.etcd_mon_timeout, 0);
            return false;
        }
        return !has_online;
    }

    reset_rng()
    {
        this.seed = 0x5f020e43;
    }

    rng()
    {
        this.seed ^= this.seed << 13;
        this.seed ^= this.seed >> 17;
        this.seed ^= this.seed << 5;
        return this.seed + 2147483648;
    }

    pick_primary(pool_id, osd_set, up_osds)
    {
        let alive_set;
        if (this.state.config.pools[pool_id].scheme === 'replicated')
            alive_set = osd_set.filter(osd_num => osd_num && up_osds[osd_num]);
        else
        {
            // Prefer data OSDs for EC because they can actually read something without an additional network hop
            const pg_data_size = (this.state.config.pools[pool_id].pg_size||0) -
                (this.state.config.pools[pool_id].parity_chunks||0);
            alive_set = osd_set.slice(0, pg_data_size).filter(osd_num => osd_num && up_osds[osd_num]);
            if (!alive_set.length)
                alive_set = osd_set.filter(osd_num => osd_num && up_osds[osd_num]);
        }
        if (!alive_set.length)
            return 0;
        return alive_set[this.rng() % alive_set.length];
    }

    save_new_pgs_txn(request, pool_id, up_osds, prev_pgs, new_pgs, pg_history)
    {
        const pg_items = {};
        this.reset_rng();
        new_pgs.map((osd_set, i) =>
        {
            osd_set = osd_set.map(osd_num => osd_num === LPOptimizer.NO_OSD ? 0 : osd_num);
            pg_items[i+1] = {
                osd_set,
                primary: this.pick_primary(pool_id, osd_set, up_osds),
            };
            if (prev_pgs[i] && prev_pgs[i].join(' ') != osd_set.join(' ') &&
                prev_pgs[i].filter(osd_num => osd_num).length > 0)
            {
                pg_history[i] = pg_history[i] || {};
                pg_history[i].osd_sets = pg_history[i].osd_sets || [];
                pg_history[i].osd_sets.push(prev_pgs[i]);
            }
            if (pg_history[i] && pg_history[i].osd_sets)
            {
                pg_history[i].osd_sets = Object.values(pg_history[i].osd_sets
                    .reduce((a, c) => { a[c.join(' ')] = c; return a; }, {}));
            }
        });
        for (let i = 0; i < new_pgs.length || i < prev_pgs.length; i++)
        {
            // FIXME: etcd has max_txn_ops limit, and it's 128 by default
            // Sooo we probably want to change our storage scheme for PG histories...
            request.compare.push({
                key: b64(this.etcd_prefix+'/pg/history/'+pool_id+'/'+(i+1)),
                target: 'MOD',
                mod_revision: ''+this.etcd_watch_revision,
                result: 'LESS',
            });
            if (pg_history[i])
            {
                request.success.push({
                    requestPut: {
                        key: b64(this.etcd_prefix+'/pg/history/'+pool_id+'/'+(i+1)),
                        value: b64(JSON.stringify(pg_history[i])),
                    },
                });
            }
            else
            {
                request.success.push({
                    requestDeleteRange: {
                        key: b64(this.etcd_prefix+'/pg/history/'+pool_id+'/'+(i+1)),
                    },
                });
            }
        }
        this.state.config.pgs.items = this.state.config.pgs.items || {};
        if (!new_pgs.length)
        {
            delete this.state.config.pgs.items[pool_id];
        }
        else
        {
            this.state.config.pgs.items[pool_id] = pg_items;
        }
    }

    validate_pool_cfg(pool_id, pool_cfg, warn)
    {
        pool_cfg.pg_size = Math.floor(pool_cfg.pg_size);
        pool_cfg.pg_minsize = Math.floor(pool_cfg.pg_minsize);
        pool_cfg.parity_chunks = Math.floor(pool_cfg.parity_chunks) || undefined;
        pool_cfg.pg_count = Math.floor(pool_cfg.pg_count);
        pool_cfg.failure_domain = pool_cfg.failure_domain || 'host';
        pool_cfg.max_osd_combinations = Math.floor(pool_cfg.max_osd_combinations) || 10000;
        if (!/^[1-9]\d*$/.exec(''+pool_id))
        {
            if (warn)
                console.log('Pool ID '+pool_id+' is invalid');
            return false;
        }
        if (pool_cfg.scheme !== 'xor' && pool_cfg.scheme !== 'replicated' && pool_cfg.scheme !== 'jerasure')
        {
            if (warn)
                console.log('Pool '+pool_id+' has invalid coding scheme (one of "xor", "replicated" and "jerasure" required)');
            return false;
        }
        if (!pool_cfg.pg_size || pool_cfg.pg_size < 1 || pool_cfg.pg_size > 256 ||
            (pool_cfg.scheme === 'xor' || pool_cfg.scheme == 'jerasure') && pool_cfg.pg_size < 3)
        {
            if (warn)
                console.log('Pool '+pool_id+' has invalid pg_size');
            return false;
        }
        if (!pool_cfg.pg_minsize || pool_cfg.pg_minsize < 1 || pool_cfg.pg_minsize > pool_cfg.pg_size ||
            pool_cfg.scheme === 'xor' && pool_cfg.pg_minsize < (pool_cfg.pg_size - 1))
        {
            if (warn)
                console.log('Pool '+pool_id+' has invalid pg_minsize');
            return false;
        }
        if (pool_cfg.scheme === 'xor' && pool_cfg.parity_chunks != 0 && pool_cfg.parity_chunks != 1)
        {
            if (warn)
                console.log('Pool '+pool_id+' has invalid parity_chunks (must be 1)');
            return false;
        }
        if (pool_cfg.scheme === 'jerasure' && (pool_cfg.parity_chunks < 1 || pool_cfg.parity_chunks > pool_cfg.pg_size-2))
        {
            if (warn)
                console.log('Pool '+pool_id+' has invalid parity_chunks (must be between 1 and pg_size-2)');
            return false;
        }
        if (!pool_cfg.pg_count || pool_cfg.pg_count < 1)
        {
            if (warn)
                console.log('Pool '+pool_id+' has invalid pg_count');
            return false;
        }
        if (!pool_cfg.name)
        {
            if (warn)
                console.log('Pool '+pool_id+' has empty name');
            return false;
        }
        if (pool_cfg.max_osd_combinations < 100)
        {
            if (warn)
                console.log('Pool '+pool_id+' has invalid max_osd_combinations (must be at least 100)');
            return false;
        }
        if (pool_cfg.root_node && typeof(pool_cfg.root_node) != 'string')
        {
            if (warn)
                console.log('Pool '+pool_id+' has invalid root_node (must be a string)');
            return false;
        }
        if (pool_cfg.osd_tags && typeof(pool_cfg.osd_tags) != 'string' &&
            (!(pool_cfg.osd_tags instanceof Array) || pool_cfg.osd_tags.filter(t => typeof t != 'string').length > 0))
        {
            if (warn)
                console.log('Pool '+pool_id+' has invalid osd_tags (must be a string or array of strings)');
            return false;
        }
        return true;
    }

    filter_osds_by_tags(orig_tree, flat_tree, tags)
    {
        if (!tags)
        {
            return;
        }
        for (const tag of (tags instanceof Array ? tags : [ tags ]))
        {
            for (const host in flat_tree)
            {
                let found = 0;
                for (const osd in flat_tree[host])
                {
                    if (!orig_tree[osd].tags || !orig_tree[osd].tags[tag])
                        delete flat_tree[host][osd];
                    else
                        found++;
                }
                if (!found)
                {
                    delete flat_tree[host];
                }
            }
        }
    }

    async recheck_pgs()
    {
        // Take configuration and state, check it against the stored configuration hash
        // Recalculate PGs and save them to etcd if the configuration is changed
        // FIXME: Do not change anything if the distribution is good and random enough and no PGs are degraded
        const { up_osds, levels, osd_tree } = this.get_osd_tree();
        const tree_cfg = {
            osd_tree,
            pools: this.state.config.pools,
        };
        const tree_hash = sha1hex(stableStringify(tree_cfg));
        if (this.state.config.pgs.hash != tree_hash)
        {
            // Something has changed
            const etcd_request = { compare: [], success: [] };
            for (const pool_id in (this.state.config.pgs||{}).items||{})
            {
                if (!this.state.config.pools[pool_id])
                {
                    // Pool deleted. Delete all PGs, but first stop them.
                    if (!await this.stop_all_pgs(pool_id))
                    {
                        this.schedule_recheck();
                        return;
                    }
                    const prev_pgs = [];
                    for (const pg in this.state.config.pgs.items[pool_id]||{})
                    {
                        prev_pgs[pg-1] = this.state.config.pgs.items[pool_id][pg].osd_set;
                    }
                    this.save_new_pgs_txn(etcd_request, pool_id, up_osds, prev_pgs, [], []);
                }
            }
            for (const pool_id in this.state.config.pools)
            {
                const pool_cfg = this.state.config.pools[pool_id];
                if (!this.validate_pool_cfg(pool_id, pool_cfg, false))
                {
                    continue;
                }
                let pool_tree = osd_tree[pool_cfg.root_node || ''];
                pool_tree = pool_tree ? pool_tree.children : [];
                pool_tree = LPOptimizer.flatten_tree(pool_tree, levels, pool_cfg.failure_domain, 'osd');
                this.filter_osds_by_tags(osd_tree, pool_tree, pool_cfg.osd_tags);
                // These are for the purpose of building history.osd_sets
                const real_prev_pgs = [];
                let pg_history = [];
                for (const pg in ((this.state.config.pgs.items||{})[pool_id]||{}))
                {
                    real_prev_pgs[pg-1] = this.state.config.pgs.items[pool_id][pg].osd_set;
                    if (this.state.pg.history[pool_id] &&
                        this.state.pg.history[pool_id][pg])
                    {
                        pg_history[pg-1] = this.state.pg.history[pool_id][pg];
                    }
                }
                // And these are for the purpose of minimizing data movement
                let prev_pgs = [];
                for (const pg in ((this.state.history.last_clean_pgs.items||{})[pool_id]||{}))
                {
                    prev_pgs[pg-1] = this.state.history.last_clean_pgs.items[pool_id][pg].osd_set;
                }
                prev_pgs = JSON.parse(JSON.stringify(prev_pgs.length ? prev_pgs : real_prev_pgs));
                const old_pg_count = real_prev_pgs.length;
                const optimize_cfg = {
                    osd_tree: pool_tree,
                    pg_count: pool_cfg.pg_count,
                    pg_size: pool_cfg.pg_size,
                    pg_minsize: pool_cfg.pg_minsize,
                    max_combinations: pool_cfg.max_osd_combinations,
                };
                let optimize_result;
                if (old_pg_count > 0)
                {
                    if (old_pg_count != pool_cfg.pg_count)
                    {
                        // PG count changed. Need to bring all PGs down.
                        if (!await this.stop_all_pgs(pool_id))
                        {
                            this.schedule_recheck();
                            return;
                        }
                        const new_pg_history = [];
                        PGUtil.scale_pg_count(prev_pgs, pg_history, new_pg_history, pool_cfg.pg_count);
                        pg_history = new_pg_history;
                    }
                    for (const pg of prev_pgs)
                    {
                        while (pg.length < pool_cfg.pg_size)
                        {
                            pg.push(0);
                        }
                        while (pg.length > pool_cfg.pg_size)
                        {
                            pg.pop();
                        }
                    }
                    if (!this.state.config.pgs.hash)
                    {
                        // Re-shuffle PGs
                        optimize_result = await LPOptimizer.optimize_initial(optimize_cfg);
                    }
                    else
                    {
                        optimize_result = await LPOptimizer.optimize_change({
                            prev_pgs,
                            ...optimize_cfg,
                        });
                    }
                }
                else
                {
                    optimize_result = await LPOptimizer.optimize_initial(optimize_cfg);
                }
                if (old_pg_count != optimize_result.int_pgs.length)
                {
                    console.log(
                        `PG count for pool ${pool_id} (${pool_cfg.name || 'unnamed'})`+
                        ` changed from: ${old_pg_count} to ${optimize_result.int_pgs.length}`
                    );
                    // Drop stats
                    etcd_request.success.push({ requestDeleteRange: {
                        key: b64(this.etcd_prefix+'/pg/stats/'+pool_id+'/'),
                        range_end: b64(this.etcd_prefix+'/pg/stats/'+pool_id+'0'),
                    } });
                }
                LPOptimizer.print_change_stats(optimize_result);
                const pg_effsize = Math.min(pool_cfg.pg_size, Object.keys(pool_tree).length);
                this.state.pool.stats[pool_id] = {
                    used_raw_tb: (this.state.pool.stats[pool_id]||{}).used_raw_tb || 0,
                    total_raw_tb: optimize_result.space,
                    raw_to_usable: pg_effsize / (pool_cfg.pg_size - (pool_cfg.parity_chunks||0)),
                    space_efficiency: optimize_result.space/(optimize_result.total_space||1),
                };
                etcd_request.success.push({ requestPut: {
                    key: b64(this.etcd_prefix+'/pool/stats/'+pool_id),
                    value: b64(JSON.stringify(this.state.pool.stats[pool_id])),
                } });
                this.save_new_pgs_txn(etcd_request, pool_id, up_osds, real_prev_pgs, optimize_result.int_pgs, pg_history);
            }
            this.state.config.pgs.hash = tree_hash;
            await this.save_pg_config(etcd_request);
        }
        else
        {
            // Nothing changed, but we still want to recheck the distribution of primaries
            let changed = false;
            for (const pool_id in this.state.config.pools)
            {
                const pool_cfg = this.state.config.pools[pool_id];
                if (!this.validate_pool_cfg(pool_id, pool_cfg, false))
                {
                    continue;
                }
                const replicated = pool_cfg.scheme === 'replicated';
                this.reset_rng();
                for (let pg_num = 1; pg_num <= pool_cfg.pg_count; pg_num++)
                {
                    const pg_cfg = this.state.config.pgs.items[pool_id][pg_num];
                    if (pg_cfg)
                    {
                        const new_primary = this.pick_primary(pool_id, pg_cfg.osd_set, up_osds);
                        if (pg_cfg.primary != new_primary)
                        {
                            console.log(
                                `Moving pool ${pool_id} (${pool_cfg.name || 'unnamed'}) PG ${pg_num}`+
                                ` primary OSD from ${pg_cfg.primary} to ${new_primary}`
                            );
                            changed = true;
                            pg_cfg.primary = new_primary;
                        }
                    }
                }
            }
            if (changed)
            {
                await this.save_pg_config();
            }
        }
    }

    async save_pg_config(etcd_request = { compare: [], success: [] })
    {
        etcd_request.compare.push(
            { key: b64(this.etcd_prefix+'/mon/master'), target: 'LEASE', lease: ''+this.etcd_lease_id },
            { key: b64(this.etcd_prefix+'/config/pgs'), target: 'MOD', mod_revision: ''+this.etcd_watch_revision, result: 'LESS' },
        );
        etcd_request.success.push(
            { requestPut: { key: b64(this.etcd_prefix+'/config/pgs'), value: b64(JSON.stringify(this.state.config.pgs)) } },
        );
        const res = await this.etcd_call('/kv/txn', etcd_request, this.config.etcd_mon_timeout, 0);
        if (!res.succeeded)
        {
            console.log('Someone changed PG configuration while we also tried to change it. Retrying in '+this.config.mon_change_timeout+' ms');
            this.schedule_recheck();
            return;
        }
        console.log('PG configuration successfully changed');
    }

    // Schedule next recheck at least at <unixtime>
    schedule_next_recheck_at(unixtime)
    {
        this.next_recheck_at = !this.next_recheck_at || this.next_recheck_at > unixtime
            ? unixtime : this.next_recheck_at;
        const now = Date.now()/1000;
        if (this.next_recheck_timer)
        {
            clearTimeout(this.next_recheck_timer);
            this.next_recheck_timer = null;
        }
        if (this.next_recheck_at < now)
        {
            this.next_recheck_at = 0;
            this.schedule_recheck();
        }
        else
        {
            this.next_recheck_timer = setTimeout(() =>
            {
                this.next_recheck_timer = null;
                this.next_recheck_at = 0;
                this.schedule_recheck();
            }, now-this.next_recheck_at);
        }
    }

    // Schedule a recheck to run after a small timeout (1s)
    // If already scheduled, cancel previous timer and schedule it again
    // This is required for multiple change events to trigger at most 1 recheck in 1s
    schedule_recheck()
    {
        if (this.recheck_timer)
        {
            clearTimeout(this.recheck_timer);
            this.recheck_timer = null;
        }
        this.recheck_timer = setTimeout(() =>
        {
            this.recheck_timer = null;
            this.recheck_pgs().catch(console.error);
        }, this.config.mon_change_timeout || 1000);
    }

    sum_op_stats()
    {
        const op_stats = {}, subop_stats = {}, recovery_stats = {};
        for (const osd in this.state.osd.stats)
        {
            const st = this.state.osd.stats[osd]||{};
            for (const op in st.op_stats||{})
            {
                op_stats[op] = op_stats[op] || { count: 0n, usec: 0n, bytes: 0n };
                op_stats[op].count += BigInt(st.op_stats[op].count||0);
                op_stats[op].usec += BigInt(st.op_stats[op].usec||0);
                op_stats[op].bytes += BigInt(st.op_stats[op].bytes||0);
            }
            for (const op in st.subop_stats||{})
            {
                subop_stats[op] = subop_stats[op] || { count: 0n, usec: 0n };
                subop_stats[op].count += BigInt(st.subop_stats[op].count||0);
                subop_stats[op].usec += BigInt(st.subop_stats[op].usec||0);
            }
            for (const op in st.recovery_stats||{})
            {
                recovery_stats[op] = recovery_stats[op] || { count: 0n, bytes: 0n };
                recovery_stats[op].count += BigInt(st.recovery_stats[op].count||0);
                recovery_stats[op].bytes += BigInt(st.recovery_stats[op].bytes||0);
            }
        }
        return { op_stats, subop_stats, recovery_stats };
    }

    sum_object_counts()
    {
        const object_counts = { object: 0n, clean: 0n, misplaced: 0n, degraded: 0n, incomplete: 0n };
        for (const pool_id in this.state.pg.stats)
        {
            for (const pg_num in this.state.pg.stats[pool_id])
            {
                const st = this.state.pg.stats[pool_id][pg_num];
                if (st)
                {
                    for (const k in object_counts)
                    {
                        if (st[k+'_count'])
                        {
                            object_counts[k] += BigInt(st[k+'_count']);
                        }
                    }
                }
            }
        }
        return object_counts;
    }

    sum_inode_stats()
    {
        const inode_stats = {};
        const inode_stub = () => ({
            raw_used: 0n,
            read: { count: 0n, usec: 0n, bytes: 0n },
            write: { count: 0n, usec: 0n, bytes: 0n },
            delete: { count: 0n, usec: 0n, bytes: 0n },
        });
        for (const pool_id in this.state.config.pools)
        {
            this.state.pool.stats[pool_id] = this.state.pool.stats[pool_id] || {};
            this.state.pool.stats[pool_id].used_raw_tb = 0n;
        }
        for (const osd_num in this.state.osd.space)
        {
            for (const pool_id in this.state.osd.space[osd_num])
            {
                this.state.pool.stats[pool_id] = this.state.pool.stats[pool_id] || { used_raw_tb: 0n };
                inode_stats[pool_id] = inode_stats[pool_id] || {};
                for (const inode_num in this.state.osd.space[osd_num][pool_id])
                {
                    const u = BigInt(this.state.osd.space[osd_num][pool_id][inode_num]||0);
                    inode_stats[pool_id][inode_num] = inode_stats[pool_id][inode_num] || inode_stub();
                    inode_stats[pool_id][inode_num].raw_used += u;
                    this.state.pool.stats[pool_id].used_raw_tb += u;
                }
            }
        }
        for (const pool_id in this.state.config.pools)
        {
            const used = this.state.pool.stats[pool_id].used_raw_tb;
            this.state.pool.stats[pool_id].used_raw_tb = Number(used)/1024/1024/1024/1024;
        }
        for (const osd_num in this.state.osd.inodestats)
        {
            const ist = this.state.osd.inodestats[osd_num];
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
        return inode_stats;
    }

    fix_stat_overflows(obj, scratch)
    {
        for (const k in obj)
        {
            if (typeof obj[k] == 'bigint')
            {
                if (obj[k] >= 0x10000000000000000n)
                {
                    if (scratch[k])
                    {
                        for (const k2 in scratch)
                        {
                            obj[k2] -= scratch[k2];
                            scratch[k2] = 0n;
                        }
                    }
                    else
                    {
                        for (const k2 in obj)
                        {
                            scratch[k2] = obj[k2];
                        }
                    }
                }
            }
            else if (typeof obj[k] == 'object')
            {
                this.fix_stat_overflows(obj[k], scratch[k] = (scratch[k] || {}));
            }
        }
    }

    serialize_bigints(obj)
    {
        for (const k in obj)
        {
            if (typeof obj[k] == 'bigint')
            {
                obj[k] = ''+obj[k];
            }
            else if (typeof obj[k] == 'object')
            {
                this.serialize_bigints(obj[k]);
            }
        }
    }

    async update_total_stats()
    {
        const txn = [];
        const stats = this.sum_op_stats();
        const object_counts = this.sum_object_counts();
        const inode_stats = this.sum_inode_stats();
        this.fix_stat_overflows(stats, (this.prev_stats = this.prev_stats || {}));
        this.fix_stat_overflows(inode_stats, (this.prev_inode_stats = this.prev_inode_stats || {}));
        stats.object_counts = object_counts;
        this.serialize_bigints(stats);
        this.serialize_bigints(inode_stats);
        txn.push({ requestPut: { key: b64(this.etcd_prefix+'/stats'), value: b64(JSON.stringify(stats)) } });
        for (const pool_id in inode_stats)
        {
            for (const inode_num in inode_stats[pool_id])
            {
                txn.push({ requestPut: {
                    key: b64(this.etcd_prefix+'/inode/stats/'+pool_id+'/'+inode_num),
                    value: b64(JSON.stringify(inode_stats[pool_id][inode_num])),
                } });
            }
        }
        for (const pool_id in this.state.pool.stats)
        {
            txn.push({ requestPut: {
                key: b64(this.etcd_prefix+'/pool/stats/'+pool_id),
                value: b64(JSON.stringify(this.state.pool.stats[pool_id])),
            } });
        }
        if (txn.length)
        {
            await this.etcd_call('/kv/txn', { success: txn }, this.config.etcd_mon_timeout, 0);
        }
    }

    schedule_update_stats()
    {
        if (this.stats_timer)
        {
            clearTimeout(this.stats_timer);
            this.stats_timer = null;
        }
        let sleep = (this.stats_update_next||0) - Date.now();
        if (sleep < this.config.mon_stats_timeout)
        {
            sleep = this.config.mon_stats_timeout;
        }
        this.stats_timer = setTimeout(() =>
        {
            this.stats_timer = null;
            this.stats_update_next = Date.now() + this.config.mon_stats_interval;
            this.update_total_stats().catch(console.error);
        }, sleep);
    }

    parse_kv(kv)
    {
        if (!kv || !kv.key)
        {
            return;
        }
        kv.key = de64(kv.key);
        kv.value = kv.value ? de64(kv.value) : null;
        let key = kv.key.substr(this.etcd_prefix.length+1);
        if (!this.constructor.etcd_allow.exec(key))
        {
            console.log('Bad key in etcd: '+kv.key+' = '+kv.value);
            return;
        }
        try
        {
            kv.value = kv.value ? JSON.parse(kv.value) : null;
        }
        catch (e)
        {
            console.log('Bad value in etcd: '+kv.key+' = '+kv.value);
            return;
        }
        let key_parts = key.split('/');
        let cur = this.state;
        for (let i = 0; i < key_parts.length-1; i++)
        {
            cur = (cur[key_parts[i]] = cur[key_parts[i]] || {});
        }
        if (etcd_nonempty_keys[key])
        {
            // Do not clear these to null
            kv.value = kv.value || {};
        }
        cur[key_parts[key_parts.length-1]] = kv.value;
        if (key === 'config/global')
        {
            this.config = this.state.config.global;
            this.check_config();
            for (const osd_num in this.state.osd.stats)
            {
                // Recheck PGs <osd_out_time> later
                this.schedule_next_recheck_at(
                    !this.state.osd.stats[osd_num] ? 0 : this.state.osd.stats[osd_num].time+this.config.osd_out_time
                );
            }
        }
        else if (key === 'config/pools')
        {
            for (const pool_id in this.state.config.pools)
            {
                // Adjust pool configuration so PG distribution hash doesn't change on recheck()
                const pool_cfg = this.state.config.pools[pool_id];
                this.validate_pool_cfg(pool_id, pool_cfg, true);
            }
        }
        else if (key_parts[0] === 'osd' && key_parts[1] === 'stats')
        {
            // Recheck PGs <osd_out_time> later
            this.schedule_next_recheck_at(
                !this.state.osd.stats[key[2]] ? 0 : this.state.osd.stats[key[2]].time+this.config.osd_out_time
            );
        }
    }

    async etcd_call(path, body, timeout, retries)
    {
        let retry = 0;
        if (retries >= 0 && retries < 1)
        {
            retries = 1;
        }
        while (retries < 0 || retry < retries)
        {
            const base = this.etcd_urls[Math.floor(Math.random()*this.etcd_urls.length)];
            const res = await POST(base+path, body, timeout);
            if (res.error)
            {
                console.error('etcd returned error: '+res.error);
                break;
            }
            if (res.json)
            {
                if (res.json.error)
                {
                    console.error('etcd returned error: '+res.json.error);
                    break;
                }
                if (this.etcd_urls.length > 1)
                {
                    // Stick to the same etcd for the rest of calls
                    this.etcd_urls = [ base ];
                }
                return res.json;
            }
            retry++;
        }
        this.die();
    }

    die(err)
    {
        // In fact we can just try to rejoin
        console.error(new Error(err || 'Cluster connection failed'));
        process.exit(1);
    }

    local_ips()
    {
        const ips = [];
        const ifaces = os.networkInterfaces();
        for (const ifname in ifaces)
        {
            for (const iface of ifaces[ifname])
            {
                if (iface.family == 'IPv4' && !iface.internal)
                {
                    ips.push(iface.address);
                }
            }
        }
        return ips;
    }
}

function POST(url, body, timeout)
{
    return new Promise((ok, no) =>
    {
        const body_text = Buffer.from(JSON.stringify(body));
        let timer_id = timeout > 0 ? setTimeout(() =>
        {
            if (req)
                req.abort();
            req = null;
            ok({ error: 'timeout' });
        }, timeout) : null;
        let req = http.request(url, { method: 'POST', headers: {
            'Content-Type': 'application/json',
            'Content-Length': body_text.length,
        } }, (res) =>
        {
            if (!req)
            {
                return;
            }
            clearTimeout(timer_id);
            let res_body = '';
            res.setEncoding('utf8');
            res.on('data', chunk => { res_body += chunk; });
            res.on('end', () =>
            {
                if (res.statusCode != 200)
                {
                    ok({ error: res_body, code: res.statusCode });
                    return;
                }
                try
                {
                    res_body = JSON.parse(res_body);
                    ok({ response: res, json: res_body });
                }
                catch (e)
                {
                    ok({ error: e, response: res, body: res_body });
                }
            });
        });
        req.write(body_text);
        req.end();
    });
}

function b64(str)
{
    return Buffer.from(str).toString('base64');
}

function de64(str)
{
    return Buffer.from(str, 'base64').toString();
}

function sha1hex(str)
{
    const hash = crypto.createHash('sha1');
    hash.update(str);
    return hash.digest('hex');
}

Mon.etcd_allow = etcd_allow;
Mon.etcd_tree = etcd_tree;

module.exports = Mon;
