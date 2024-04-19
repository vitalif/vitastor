// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

const fs = require('fs');
const http = require('http');
const crypto = require('crypto');
const os = require('os');
const WebSocket = require('ws');
const { RuleCombinator, parse_level_indexes, parse_pg_dsl } = require('./dsl_pgs.js');
const { SimpleCombinator, flatten_tree } = require('./simple_pgs.js');
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
    'mon/member/[a-f0-9]+',
    'pg/state/[1-9]\\d*/[1-9]\\d*',
    'pg/stats/[1-9]\\d*/[1-9]\\d*',
    'pg/history/[1-9]\\d*/[1-9]\\d*',
    'pool/stats/[1-9]\\d*',
    'history/last_clean_pgs',
    'inode/stats/[1-9]\\d*/\\d+',
    'pool/stats/[1-9]\\d*',
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
            etcd_prefix: "/vitastor",
            // etcd connection - configurable online
            etcd_address: "10.0.115.10:2379/v3",
            // mon
            etcd_mon_ttl: 5, // min: 1
            etcd_mon_timeout: 1000, // ms. min: 0
            etcd_mon_retries: 5, // min: 0
            mon_change_timeout: 1000, // ms. min: 100
            mon_retry_change_timeout: 50, // ms. min: 10
            mon_stats_timeout: 1000, // ms. min: 100
            osd_out_time: 600, // seconds. min: 0
            placement_levels: { datacenter: 1, rack: 2, host: 3, osd: 4, ... },
            use_old_pg_combinator: false,
            // client and osd
            tcp_header_buffer_size: 65536,
            use_sync_send_recv: false,
            use_rdma: true,
            rdma_device: null, // for example, "rocep5s0f0"
            rdma_port_num: 1,
            rdma_gid_index: 0,
            rdma_mtu: 4096,
            rdma_max_sge: 128,
            rdma_max_send: 8,
            rdma_max_recv: 16,
            rdma_max_msg: 132096,
            block_size: 131072,
            disk_alignment: 4096,
            bitmap_granularity: 4096,
            immediate_commit: false, // 'all' or 'small'
            // client - configurable online
            client_max_dirty_bytes: 33554432,
            client_max_dirty_ops: 1024,
            client_enable_writeback: false,
            client_max_buffered_bytes: 33554432,
            client_max_buffered_ops: 1024,
            client_max_writeback_iodepth: 256,
            client_retry_interval: 50, // ms. min: 10
            client_eio_retry_interval: 1000, // ms
            client_retry_enospc: true,
            osd_nearfull_ratio: 0.95,
            // client and osd - configurable online
            log_level: 0,
            peer_connect_interval: 5, // seconds. min: 1
            peer_connect_timeout: 5, // seconds. min: 1
            osd_idle_timeout: 5, // seconds. min: 1
            osd_ping_timeout: 5, // seconds. min: 1
            max_etcd_attempts: 5,
            etcd_quick_timeout: 1000, // ms
            etcd_slow_timeout: 5000, // ms
            etcd_keepalive_timeout: 30, // seconds, default is max(30, etcd_report_interval*2)
            etcd_ws_keepalive_interval: 30, // seconds
            // osd
            etcd_report_interval: 5, // seconds
            etcd_stats_interval: 30, // seconds
            run_primary: true,
            osd_network: null, // "192.168.7.0/24" or an array of masks
            bind_address: "0.0.0.0",
            bind_port: 0,
            readonly: false,
            osd_memlock: false,
            // osd - configurable online
            autosync_interval: 5,
            autosync_writes: 128,
            client_queue_depth: 128, // unused
            recovery_queue_depth: 1,
            recovery_sleep_us: 0,
            recovery_tune_util_low: 0.1,
            recovery_tune_client_util_low: 0,
            recovery_tune_util_high: 1.0,
            recovery_tune_client_util_high: 0.5,
            recovery_tune_interval: 1,
            recovery_tune_agg_interval: 10, // 10 times recovery_tune_interval
            recovery_tune_sleep_min_us: 10, // 10 microseconds
            recovery_pg_switch: 128,
            recovery_sync_batch: 16,
            no_recovery: false,
            no_rebalance: false,
            print_stats_interval: 3,
            slow_log_interval: 10,
            inode_vanish_time: 60,
            auto_scrub: false,
            no_scrub: false,
            scrub_interval: '30d', // 1s/1m/1h/1d
            scrub_queue_depth: 1,
            scrub_sleep: 0, // milliseconds
            scrub_list_limit: 1000, // objects to list on one scrub iteration
            scrub_find_best: true,
            scrub_ec_max_bruteforce: 100, // maximum EC error locator brute-force iterators
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
            // blockstore - configurable offline
            inmemory_metadata,
            inmemory_journal,
            journal_sector_buffer_count,
            journal_no_same_sector_overwrites,
            // blockstore - configurable online
            max_write_iodepth,
            min_flusher_count: 1,
            max_flusher_count: 256,
            throttle_small_writes: false,
            throttle_target_iops: 100,
            throttle_target_mbs: 100,
            throttle_target_parallelism: 1,
            throttle_threshold_us: 50,
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
                // 'ec' uses Reed-Solomon-Vandermonde codes, 'jerasure' is an alias for 'ec'
                scheme: 'replicated' | 'xor' | 'ec' | 'jerasure',
                pg_size: 3,
                pg_minsize: 2,
                // number of parity chunks, required for EC
                parity_chunks?: 1,
                pg_count: 100,
                // default is failure_domain=host
                failure_domain?: 'host',
                // additional failure domain rules; failure_domain=x is equivalent to x=123..N
                level_placement?: 'dc=112233 host=123456',
                raw_placement?: 'any, dc=1 host!=1, dc=1 host!=(1,2)',
                old_combinator: false,
                max_osd_combinations: 10000,
                // block_size, bitmap_granularity, immediate_commit must match all OSDs used in that pool
                block_size: 131072,
                bitmap_granularity: 4096,
                // 'all'/'small'/'none', same as in OSD options
                immediate_commit: 'none',
                pg_stripe_size: 0,
                root_node?: 'rack1',
                // restrict pool to OSDs having all of these tags
                osd_tags?: 'nvme' | [ 'nvme', ... ],
                // prefer to put primary on OSD with these tags
                primary_affinity_tags?: 'nvme' | [ 'nvme', ... ],
                // scrub interval
                scrub_interval?: '30d',
            },
            ...
        }, */
        pools: {},
        osd: {
            /* <id>: { reweight?: 1, tags?: [ 'nvme', ... ], noout?: true }, ... */
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
            /* <pool_id>: {
                <inode_t>: {
                    read: { count: uint64_t, usec: uint64_t, bytes: uint64_t },
                    write: { count: uint64_t, usec: uint64_t, bytes: uint64_t },
                    delete: { count: uint64_t, usec: uint64_t, bytes: uint64_t },
                },
            }, */
        },
        space: {
            /* <osd_num_t>: {
                <pool_id>: {
                    <inode_t>: uint64_t, // bytes
                },
            }, */
        },
    },
    mon: {
        master: {
            /* ip: [ string ], id: uint64_t */
        },
        standby: {
            /* <uint64_t>: { ip: [ string ] }, */
        },
    },
    pg: {
        state: {
            /* <pool_id>: {
                <pg_id>: {
                    primary: osd_num_t,
                    state: ("starting"|"peering"|"incomplete"|"active"|"repeering"|"stopping"|"offline"|
                        "degraded"|"has_incomplete"|"has_degraded"|"has_misplaced"|"has_unclean"|
                        "has_invalid"|"has_inconsistent"|"has_corrupted"|"left_on_dead"|"scrubbing")[],
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
                    epoch: uint64_t,
                    next_scrub: uint64_t,
                },
            }, */
        },
    },
    inode: {
        stats: {
            /* <pool_id>: {
                <inode_t>: {
                    raw_used: uint64_t, // raw used bytes on OSDs
                    read: { count: uint64_t, usec: uint64_t, bytes: uint64_t, bps: uint64_t, iops: uint64_t, lat: uint64_t },
                    write: { count: uint64_t, usec: uint64_t, bytes: uint64_t, bps: uint64_t, iops: uint64_t, lat: uint64_t },
                    delete: { count: uint64_t, usec: uint64_t, bytes: uint64_t, bps: uint64_t, iops: uint64_t, lat: uint64_t },
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
            <string>: { count: uint64_t, usec: uint64_t, bytes: uint64_t, bps: uint64_t, iops: uint64_t, lat: uint64_t },
        },
        subop_stats: {
            <string>: { count: uint64_t, usec: uint64_t, iops: uint64_t, lat: uint64_t },
        },
        recovery_stats: {
            degraded: { count: uint64_t, bytes: uint64_t, bps: uint64_t, iops: uint64_t },
            misplaced: { count: uint64_t, bytes: uint64_t, bps: uint64_t, iops: uint64_t },
        },
        object_counts: {
            object: uint64_t,
            clean: uint64_t,
            misplaced: uint64_t,
            degraded: uint64_t,
            incomplete: uint64_t,
        },
        object_bytes: {
            total: uint64_t,
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
        this.failconnect = (e) => this._die(e, 2);
        this.die = (e) => this._die(e, 1);
        if (fs.existsSync(config.config_path||'/etc/vitastor/vitastor.conf'))
        {
            config = {
                ...JSON.parse(fs.readFileSync(config.config_path||'/etc/vitastor/vitastor.conf', { encoding: 'utf-8' })),
                ...config,
            };
        }
        this.parse_etcd_addresses(config.etcd_address||config.etcd_url);
        this.verbose = config.verbose || 0;
        this.initConfig = config;
        this.config = { ...config };
        this.etcd_prefix = config.etcd_prefix || '/vitastor';
        this.etcd_prefix = this.etcd_prefix.replace(/\/\/+/g, '/').replace(/^\/?(.*[^\/])\/?$/, '/$1');
        this.etcd_start_timeout = (config.etcd_start_timeout || 5) * 1000;
        this.state = JSON.parse(JSON.stringify(this.constructor.etcd_tree));
        this.prev_stats = { osd_stats: {}, osd_diff: {} };
        this.signals_set = false;
        this.ws = null;
        this.ws_alive = false;
        this.ws_keepalive_timer = null;
        this.on_stop_cb = () => this.on_stop(0).catch(console.error);
        this.recheck_pgs_active = false;
    }

    parse_etcd_addresses(addrs)
    {
        const is_local_ip = this.local_ips(true).reduce((a, c) => { a[c] = true; return a; }, {});
        this.etcd_local = [];
        this.etcd_urls = [];
        this.selected_etcd_url = null;
        this.etcd_urls_to_try = [];
        if (!(addrs instanceof Array))
            addrs = addrs ? (''+(addrs||'')).split(/,/) : [];
        if (!addrs.length)
        {
            console.error('Vitastor etcd address(es) not specified. Please set on the command line or in the config file');
            process.exit(1);
        }
        for (let url of addrs)
        {
            let scheme = 'http';
            url = url.trim().replace(/^(https?):\/\//, (m, m1) => { scheme = m1; return ''; });
            const slash = url.indexOf('/');
            const colon = url.indexOf(':');
            const is_local = is_local_ip[colon >= 0 ? url.substr(0, colon) : (slash >= 0 ? url.substr(0, slash) : url)];
            url = scheme+'://'+(slash >= 0 ? url : url+'/v3');
            if (is_local)
                this.etcd_local.push(url);
            else
                this.etcd_urls.push(url);
        }
    }

    async start()
    {
        await this.load_config();
        await this.get_lease();
        await this.become_master();
        await this.load_cluster_state();
        await this.start_watcher(this.config.etcd_mon_retries);
        for (const pool_id in this.state.config.pools)
        {
            if (!this.state.pool.stats[pool_id] ||
                !Number(this.state.pool.stats[pool_id].pg_real_size))
            {
                // Generate missing data in etcd
                this.state.config.pgs.hash = null;
                break;
            }
        }
        await this.recheck_pgs();
        this.schedule_update_stats();
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
        this.config.etcd_mon_ttl = Number(this.config.etcd_mon_ttl) || 5;
        if (this.config.etcd_mon_ttl < 1)
        {
            this.config.etcd_mon_ttl = 1;
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
        this.config.mon_retry_change_timeout = Number(this.config.mon_retry_change_timeout) || 50;
        if (this.config.mon_retry_change_timeout < 50)
        {
            this.config.mon_retry_change_timeout = 50;
        }
        this.config.mon_stats_timeout = Number(this.config.mon_stats_timeout) || 1000;
        if (this.config.mon_stats_timeout < 100)
        {
            this.config.mon_stats_timeout = 100;
        }
        // After this number of seconds, a dead OSD will be removed from PG distribution
        this.config.osd_out_time = Number(this.config.osd_out_time) || 0;
        if (!this.config.osd_out_time)
        {
            this.config.osd_out_time = 600; // 10 minutes by default
        }
    }

    pick_next_etcd()
    {
        if (this.selected_etcd_url)
            return this.selected_etcd_url;
        if (!this.etcd_urls_to_try || !this.etcd_urls_to_try.length)
        {
            this.etcd_urls_to_try = [ ...this.etcd_local ];
            const others = [ ...this.etcd_urls ];
            while (others.length)
            {
                const url = others.splice(0|(others.length*Math.random()), 1);
                this.etcd_urls_to_try.push(url[0]);
            }
        }
        this.selected_etcd_url = this.etcd_urls_to_try.shift();
        return this.selected_etcd_url;
    }

    restart_watcher(cur_addr)
    {
        if (this.ws)
        {
            this.ws.close();
            this.ws = null;
        }
        if (this.ws_keepalive_timer)
        {
            clearInterval(this.ws_keepalive_timer);
            this.ws_keepalive_timer = null;
        }
        if (this.selected_etcd_url == cur_addr)
        {
            this.selected_etcd_url = null;
        }
        this.start_watcher(this.config.etcd_mon_retries).catch(this.die);
    }

    async start_watcher(retries)
    {
        let retry = 0;
        if (!retries || retries < 1)
        {
            retries = 1;
        }
        const tried = {};
        while (retries < 0 || retry < retries)
        {
            const cur_addr = this.pick_next_etcd();
            const base = 'ws'+cur_addr.substr(4);
            let now = Date.now();
            if (tried[base] && now-tried[base] < this.etcd_start_timeout)
            {
                await new Promise(ok => setTimeout(ok, this.etcd_start_timeout-(now-tried[base])));
                now = Date.now();
            }
            tried[base] = now;
            const ok = await new Promise(ok =>
            {
                const timer_id = setTimeout(() =>
                {
                    this.ws.close();
                    this.ws = null;
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
                break;
            if (this.selected_etcd_url == cur_addr)
                this.selected_etcd_url = null;
            this.ws = null;
            retry++;
        }
        if (!this.ws)
        {
            this.failconnect('Failed to open etcd watch websocket');
        }
        const cur_addr = this.selected_etcd_url;
        this.ws_alive = true;
        this.ws_keepalive_timer = setInterval(() =>
        {
            if (this.ws_alive)
            {
                this.ws_alive = false;
                this.ws.send(JSON.stringify({ progress_request: {} }));
            }
            else
            {
                console.log('etcd websocket timed out, restarting it');
                this.restart_watcher(cur_addr);
            }
        }, (Number(this.config.etcd_ws_keepalive_interval) || 30)*1000);
        this.ws.on('error', () => this.restart_watcher(cur_addr));
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
            this.ws_alive = true;
            let data;
            try
            {
                data = JSON.parse(msg);
            }
            catch (e)
            {
            }
            if (!data || !data.result)
            {
                console.error('Unknown message received from watch websocket: '+msg);
            }
            else if (data.result.canceled)
            {
                // etcd watch canceled
                if (data.result.compact_revision)
                {
                    // we may miss events if we proceed
                    console.error('Revisions before '+data.result.compact_revision+' were compacted by etcd, exiting');
                    this.on_stop(1);
                }
                console.error('Watch canceled by etcd, reason: '+data.result.cancel_reason+', exiting');
                this.on_stop(1);
            }
            else if (data.result.created)
            {
                // etcd watch created
            }
            else
            {
                let stats_changed = false, changed = false, pg_states_changed = false;
                if (this.verbose)
                {
                    console.log('Revision '+data.result.header.revision+' events: ');
                }
                this.etcd_watch_revision = BigInt(data.result.header.revision)+BigInt(1);
                for (const e of data.result.events||[])
                {
                    this.parse_kv(e.kv);
                    const key = e.kv.key.substr(this.etcd_prefix.length);
                    if (key.substr(0, 11) == '/osd/state/')
                    {
                        stats_changed = true;
                        changed = true;
                    }
                    else if (key.substr(0, 11) == '/osd/stats/' || key.substr(0, 10) == '/pg/stats/' || key.substr(0, 16) == '/osd/inodestats/')
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
                    this.save_last_clean().catch(this.die);
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

    // Schedule save_last_clean() to to run after a small timeout (1s) (to not spam etcd)
    schedule_save_last_clean()
    {
        if (!this.save_last_clean_timer)
        {
            this.save_last_clean_timer = setTimeout(() =>
            {
                this.save_last_clean_timer = null;
                this.save_last_clean().catch(this.die);
            }, this.config.mon_change_timeout || 1000);
        }
    }

    async save_last_clean()
    {
        if (this.save_last_clean_running)
        {
            this.schedule_save_last_clean();
            return;
        }
        this.save_last_clean_running = true;
        // last_clean_pgs is used to avoid extra data move when observing a series of changes in the cluster
        const new_clean_pgs = { items: {} };
        // eslint-disable-next-line indent
    next_pool:
        for (const pool_id in this.state.config.pools)
        {
            new_clean_pgs.items[pool_id] = (this.state.history.last_clean_pgs.items||{})[pool_id];
            const pool_cfg = this.state.config.pools[pool_id];
            if (!this.validate_pool_cfg(pool_id, pool_cfg, false))
            {
                continue next_pool;
            }
            for (let pg_num = 1; pg_num <= pool_cfg.pg_count; pg_num++)
            {
                if (!this.state.pg.state[pool_id] ||
                    !this.state.pg.state[pool_id][pg_num] ||
                    !(this.state.pg.state[pool_id][pg_num].state instanceof Array))
                {
                    // Unclean
                    continue next_pool;
                }
                let st = this.state.pg.state[pool_id][pg_num].state.join(',');
                if (st != 'active' && st != 'active,left_on_dead' && st != 'left_on_dead,active')
                {
                    // Unclean
                    continue next_pool;
                }
            }
            new_clean_pgs.items[pool_id] = this.state.config.pgs.items[pool_id];
        }
        this.state.history.last_clean_pgs = new_clean_pgs;
        await this.etcd_call('/kv/txn', {
            success: [ { requestPut: {
                key: b64(this.etcd_prefix+'/history/last_clean_pgs'),
                value: b64(JSON.stringify(this.state.history.last_clean_pgs))
            } } ],
        }, this.etcd_start_timeout, 0);
        this.save_last_clean_running = false;
    }

    get_mon_state()
    {
        return { ip: this.local_ips(), hostname: os.hostname() };
    }

    async get_lease()
    {
        const max_ttl = this.config.etcd_mon_ttl + this.config.etcd_mon_timeout/1000*this.config.etcd_mon_retries;
        // Get lease
        let res = await this.etcd_call('/lease/grant', { TTL: max_ttl }, this.config.etcd_mon_timeout, -1);
        this.etcd_lease_id = res.ID;
        // Register in /mon/member, just for the information
        const state = this.get_mon_state();
        res = await this.etcd_call('/kv/put', {
            key: b64(this.etcd_prefix+'/mon/member/'+this.etcd_lease_id),
            value: b64(JSON.stringify(state)),
            lease: ''+this.etcd_lease_id
        }, this.etcd_start_timeout, 0);
        // Set refresh timer
        this.lease_timer = setInterval(async () =>
        {
            const res = await this.etcd_call('/lease/keepalive', { ID: this.etcd_lease_id }, this.config.etcd_mon_timeout, this.config.etcd_mon_retries);
            if (!res.result.TTL)
            {
                this.failconnect('Lease expired');
            }
        }, this.config.etcd_mon_ttl*1000);
        if (!this.signals_set)
        {
            process.on('SIGINT', this.on_stop_cb);
            process.on('SIGTERM', this.on_stop_cb);
            this.signals_set = true;
        }
    }

    async on_stop(status)
    {
        clearInterval(this.lease_timer);
        await this.etcd_call('/lease/revoke', { ID: this.etcd_lease_id }, this.config.etcd_mon_timeout, this.config.etcd_mon_retries);
        process.exit(status);
    }

    async become_master()
    {
        const state = { ...this.get_mon_state(), id: ''+this.etcd_lease_id };
        // eslint-disable-next-line no-constant-condition
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
        const tree = {};
        let up_osds = {};
        for (const node_id in this.state.config.node_placement||{})
        {
            const node_cfg = this.state.config.node_placement[node_id];
            if (/^\d+$/.exec(node_id))
            {
                node_cfg.level = 'osd';
            }
            if (!node_id || !node_cfg.level || !levels[node_cfg.level])
            {
                // All nodes must have non-empty IDs and valid levels
                continue;
            }
            tree[node_id] = { id: node_id, level: node_cfg.level, parent: node_cfg.parent, children: [] };
        }
        // This requires monitor system time to be in sync with OSD system times (at least to some extent)
        const down_time = Date.now()/1000 - this.config.osd_out_time;
        for (const osd_num of this.all_osds().sort((a, b) => a - b))
        {
            const stat = this.state.osd.stats[osd_num];
            const osd_cfg = this.state.config.osd[osd_num];
            if (stat && stat.size && (this.state.osd.state[osd_num] || Number(stat.time) >= down_time ||
                osd_cfg && osd_cfg.noout))
            {
                // Numeric IDs are reserved for OSDs
                let reweight = osd_cfg == null ? 1 : Number(osd_cfg.reweight);
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
                if (!tree[stat.host])
                {
                    tree[stat.host] = {
                        id: stat.host,
                        level: 'host',
                        parent: null,
                        children: [],
                    };
                }
            }
        }
        return { up_osds, levels, osd_tree: tree };
    }

    make_hier_tree(tree)
    {
        const levels = this.config.placement_levels||{};
        levels.host = levels.host || 100;
        levels.osd = levels.osd || 101;
        tree = { ...tree };
        for (const node_id in tree)
        {
            tree[node_id] = { ...tree[node_id], children: [] };
        }
        tree[''] = { children: [] };
        for (const node_id in tree)
        {
            if (node_id === '' || tree[node_id].level === 'osd' && (!tree[node_id].size || tree[node_id].size <= 0))
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
        }
        // Delete empty nodes
        let deleted = 0;
        do
        {
            deleted = 0;
            for (const node_id in tree)
            {
                if (tree[node_id].level !== 'osd' && (!tree[node_id].children || !tree[node_id].children.length))
                {
                    const parent = tree[node_id].parent;
                    if (parent)
                    {
                        tree[parent].children = tree[parent].children.filter(c => c != tree[node_id]);
                    }
                    deleted++;
                    delete tree[node_id];
                }
            }
        } while (deleted > 0);
        return tree;
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
            await this.etcd_call('/kv/txn', {
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

    pick_primary(pool_id, osd_set, up_osds, aff_osds)
    {
        let alive_set;
        if (this.state.config.pools[pool_id].scheme === 'replicated')
        {
            // Prefer "affinity" OSDs
            alive_set = osd_set.filter(osd_num => osd_num && aff_osds[osd_num]);
            if (!alive_set.length)
                alive_set = osd_set.filter(osd_num => osd_num && up_osds[osd_num]);
        }
        else
        {
            // Prefer data OSDs for EC because they can actually read something without an additional network hop
            const pg_data_size = (this.state.config.pools[pool_id].pg_size||0) -
                (this.state.config.pools[pool_id].parity_chunks||0);
            alive_set = osd_set.slice(0, pg_data_size).filter(osd_num => osd_num && aff_osds[osd_num]);
            if (!alive_set.length)
                alive_set = osd_set.filter(osd_num => osd_num && aff_osds[osd_num]);
            if (!alive_set.length)
            {
                alive_set = osd_set.slice(0, pg_data_size).filter(osd_num => osd_num && up_osds[osd_num]);
                if (!alive_set.length)
                    alive_set = osd_set.filter(osd_num => osd_num && up_osds[osd_num]);
            }
        }
        if (!alive_set.length)
            return 0;
        return alive_set[this.rng() % alive_set.length];
    }

    save_new_pgs_txn(save_to, request, pool_id, up_osds, osd_tree, prev_pgs, new_pgs, pg_history)
    {
        const aff_osds = this.get_affinity_osds(this.state.config.pools[pool_id] || {}, up_osds, osd_tree);
        const pg_items = {};
        this.reset_rng();
        new_pgs.map((osd_set, i) =>
        {
            osd_set = osd_set.map(osd_num => osd_num === LPOptimizer.NO_OSD ? 0 : osd_num);
            pg_items[i+1] = {
                osd_set,
                primary: this.pick_primary(pool_id, osd_set, up_osds, aff_osds),
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
        save_to.items = save_to.items || {};
        if (!new_pgs.length)
        {
            delete save_to.items[pool_id];
        }
        else
        {
            save_to.items[pool_id] = pg_items;
        }
    }

    validate_pool_cfg(pool_id, pool_cfg, warn)
    {
        pool_cfg.pg_size = Math.floor(pool_cfg.pg_size);
        pool_cfg.pg_minsize = Math.floor(pool_cfg.pg_minsize);
        pool_cfg.parity_chunks = Math.floor(pool_cfg.parity_chunks) || undefined;
        pool_cfg.pg_count = Math.floor(pool_cfg.pg_count);
        pool_cfg.max_osd_combinations = Math.floor(pool_cfg.max_osd_combinations) || 10000;
        if (!/^[1-9]\d*$/.exec(''+pool_id))
        {
            if (warn)
                console.log('Pool ID '+pool_id+' is invalid');
            return false;
        }
        if (pool_cfg.scheme !== 'xor' && pool_cfg.scheme !== 'replicated' &&
            pool_cfg.scheme !== 'ec' && pool_cfg.scheme !== 'jerasure')
        {
            if (warn)
                console.log('Pool '+pool_id+' has invalid coding scheme (one of "xor", "replicated", "ec" and "jerasure" required)');
            return false;
        }
        if (!pool_cfg.pg_size || pool_cfg.pg_size < 1 || pool_cfg.pg_size > 256 ||
            pool_cfg.scheme !== 'replicated' && pool_cfg.pg_size < 3)
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
        if ((pool_cfg.scheme === 'ec' || pool_cfg.scheme === 'jerasure') &&
            (pool_cfg.parity_chunks < 1 || pool_cfg.parity_chunks > pool_cfg.pg_size-2))
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
        if (pool_cfg.primary_affinity_tags && typeof(pool_cfg.primary_affinity_tags) != 'string' &&
            (!(pool_cfg.primary_affinity_tags instanceof Array) || pool_cfg.primary_affinity_tags.filter(t => typeof t != 'string').length > 0))
        {
            if (warn)
                console.log('Pool '+pool_id+' has invalid primary_affinity_tags (must be a string or array of strings)');
            return false;
        }
        if (!this.get_pg_rules(pool_id, pool_cfg, true))
        {
            return false;
        }
        return true;
    }

    filter_osds_by_root_node(pool_tree, root_node)
    {
        if (!root_node)
        {
            return;
        }
        let hier_tree = this.make_hier_tree(pool_tree);
        let included = [ ...(hier_tree[root_node] || {}).children||[] ];
        for (let i = 0; i < included.length; i++)
        {
            if (included[i].children)
            {
                included.splice(i+1, 0, ...included[i].children);
            }
        }
        let cur = pool_tree[root_node] || {};
        while (cur && cur.id)
        {
            included.unshift(cur);
            cur = pool_tree[cur.parent||''];
        }
        included = included.reduce((a, c) => { a[c.id||''] = true; return a; }, {});
        for (const item in pool_tree)
        {
            if (!included[item])
            {
                delete pool_tree[item];
            }
        }
    }

    filter_osds_by_tags(orig_tree, tags)
    {
        if (!tags)
        {
            return;
        }
        for (const tag of (tags instanceof Array ? tags : [ tags ]))
        {
            for (const osd in orig_tree)
            {
                if (orig_tree[osd].level === 'osd' &&
                    (!orig_tree[osd].tags || !orig_tree[osd].tags[tag]))
                {
                    delete orig_tree[osd];
                }
            }
        }
    }

    filter_osds_by_block_layout(orig_tree, block_size, bitmap_granularity, immediate_commit)
    {
        for (const osd in orig_tree)
        {
            if (orig_tree[osd].level === 'osd')
            {
                const osd_stat = this.state.osd.stats[osd];
                if (osd_stat && (osd_stat.bs_block_size && osd_stat.bs_block_size != block_size ||
                    osd_stat.bitmap_granularity && osd_stat.bitmap_granularity != bitmap_granularity ||
                    osd_stat.immediate_commit == 'small' && immediate_commit == 'all' ||
                    osd_stat.immediate_commit == 'none' && immediate_commit != 'none'))
                {
                    delete orig_tree[osd];
                }
            }
        }
    }

    get_affinity_osds(pool_cfg, up_osds, osd_tree)
    {
        let aff_osds = up_osds;
        if (pool_cfg.primary_affinity_tags)
        {
            aff_osds = Object.keys(up_osds).reduce((a, c) => { a[c] = osd_tree[c]; return a; }, {});
            this.filter_osds_by_tags(aff_osds, pool_cfg.primary_affinity_tags);
            for (const osd in aff_osds)
            {
                aff_osds[osd] = true;
            }
        }
        return aff_osds;
    }

    get_pg_rules(pool_id, pool_cfg, warn)
    {
        if (pool_cfg.level_placement)
        {
            const pg_size = (0|pool_cfg.pg_size);
            let rules = pool_cfg.level_placement;
            if (typeof rules === 'string')
            {
                rules = rules.split(/\s+/).map(s => s.split(/=/, 2)).reduce((a, c) => { a[c[0]] = c[1]; return a; }, {});
            }
            else
            {
                rules = { ...rules };
            }
            // Always add failure_domain to prevent rules from being totally incorrect
            const all_diff = [];
            for (let i = 1; i <= pg_size; i++)
            {
                all_diff.push(i);
            }
            rules[pool_cfg.failure_domain || 'host'] = all_diff;
            const levels = this.config.placement_levels||{};
            levels.host = levels.host || 100;
            levels.osd = levels.osd || 101;
            for (const k in rules)
            {
                if (!levels[k] || typeof rules[k] !== 'string' &&
                    (!(rules[k] instanceof Array) ||
                    rules[k].filter(s => typeof s !== 'string' && typeof s !== 'number').length > 0))
                {
                    if (warn)
                        console.log('Pool '+pool_id+' configuration is invalid: level_placement should be { [level]: string | (string|number)[] }');
                    return null;
                }
                else if (rules[k].length != pg_size)
                {
                    if (warn)
                        console.log('Pool '+pool_id+' configuration is invalid: values in level_placement should contain exactly pg_size ('+pg_size+') items');
                    return null;
                }
            }
            return parse_level_indexes(rules);
        }
        else if (typeof pool_cfg.raw_placement === 'string')
        {
            try
            {
                return parse_pg_dsl(pool_cfg.raw_placement);
            }
            catch (e)
            {
                if (warn)
                    console.log('Pool '+pool_id+' configuration is invalid: invalid raw_placement: '+e.message);
            }
        }
        else
        {
            let rules = [ [] ];
            let prev = [ 1 ];
            for (let i = 1; i < pool_cfg.pg_size; i++)
            {
                rules.push([ [ pool_cfg.failure_domain||'host', '!=', prev ] ]);
                prev = [ ...prev, i+1 ];
            }
            return rules;
        }
    }

    async generate_pool_pgs(pool_id, osd_tree, levels)
    {
        const pool_cfg = this.state.config.pools[pool_id];
        if (!this.validate_pool_cfg(pool_id, pool_cfg, false))
        {
            return null;
        }
        let pool_tree = { ...osd_tree };
        this.filter_osds_by_root_node(pool_tree, pool_cfg.root_node);
        this.filter_osds_by_tags(pool_tree, pool_cfg.osd_tags);
        this.filter_osds_by_block_layout(
            pool_tree,
            pool_cfg.block_size || this.config.block_size || 131072,
            pool_cfg.bitmap_granularity || this.config.bitmap_granularity || 4096,
            pool_cfg.immediate_commit || this.config.immediate_commit || 'none'
        );
        pool_tree = this.make_hier_tree(pool_tree);
        // First try last_clean_pgs to minimize data movement
        let prev_pgs = [];
        for (const pg in ((this.state.history.last_clean_pgs.items||{})[pool_id]||{}))
        {
            prev_pgs[pg-1] = [ ...this.state.history.last_clean_pgs.items[pool_id][pg].osd_set ];
        }
        if (!prev_pgs.length)
        {
            // Fall back to config/pgs if it's empty
            for (const pg in ((this.state.config.pgs.items||{})[pool_id]||{}))
            {
                prev_pgs[pg-1] = [ ...this.state.config.pgs.items[pool_id][pg].osd_set ];
            }
        }
        const old_pg_count = prev_pgs.length;
        const optimize_cfg = {
            osd_weights: Object.values(pool_tree).filter(item => item.level === 'osd').reduce((a, c) => { a[c.id] = c.size; return a; }, {}),
            combinator: !this.config.use_old_pg_combinator || pool_cfg.level_placement || pool_cfg.raw_placement
                // new algorithm:
                ? new RuleCombinator(pool_tree, this.get_pg_rules(pool_id, pool_cfg), pool_cfg.max_osd_combinations)
                // old algorithm:
                : new SimpleCombinator(flatten_tree(pool_tree[''].children, levels, pool_cfg.failure_domain, 'osd'), pool_cfg.pg_size, pool_cfg.max_osd_combinations),
            pg_count: pool_cfg.pg_count,
            pg_size: pool_cfg.pg_size,
            pg_minsize: pool_cfg.pg_minsize,
            ordered: pool_cfg.scheme != 'replicated',
        };
        let optimize_result;
        // Re-shuffle PGs if config/pgs.hash is empty
        if (old_pg_count > 0 && this.state.config.pgs.hash)
        {
            if (prev_pgs.length != pool_cfg.pg_count)
            {
                // Scale PG count
                // Do it even if old_pg_count is already equal to pool_cfg.pg_count,
                // because last_clean_pgs may still contain the old number of PGs
                PGUtil.scale_pg_count(prev_pgs, pool_cfg.pg_count);
            }
            for (const pg of prev_pgs)
            {
                while (pg.length < pool_cfg.pg_size)
                {
                    pg.push(0);
                }
            }
            optimize_result = await LPOptimizer.optimize_change({
                prev_pgs,
                ...optimize_cfg,
            });
        }
        else
        {
            optimize_result = await LPOptimizer.optimize_initial(optimize_cfg);
        }
        console.log(`Pool ${pool_id} (${pool_cfg.name || 'unnamed'}):`);
        LPOptimizer.print_change_stats(optimize_result);
        let pg_effsize = pool_cfg.pg_size;
        for (const pg of optimize_result.int_pgs)
        {
            const this_pg_size = pg.filter(osd => osd != LPOptimizer.NO_OSD).length;
            if (this_pg_size && this_pg_size < pg_effsize)
            {
                pg_effsize = this_pg_size;
            }
        }
        return {
            pool_id,
            pgs: optimize_result.int_pgs,
            stats: {
                total_raw_tb: optimize_result.space,
                pg_real_size: pg_effsize || pool_cfg.pg_size,
                raw_to_usable: (pg_effsize || pool_cfg.pg_size) / (pool_cfg.scheme === 'replicated'
                    ? 1 : (pool_cfg.pg_size - (pool_cfg.parity_chunks||0))),
                space_efficiency: optimize_result.space/(optimize_result.total_space||1),
            },
        };
    }

    async recheck_pgs()
    {
        if (this.recheck_pgs_active)
        {
            this.schedule_recheck();
            return;
        }
        this.recheck_pgs_active = true;
        // Take configuration and state, check it against the stored configuration hash
        // Recalculate PGs and save them to etcd if the configuration is changed
        // FIXME: Do not change anything if the distribution is good and random enough and no PGs are degraded
        const { up_osds, levels, osd_tree } = this.get_osd_tree();
        const tree_cfg = {
            osd_tree,
            levels,
            pools: this.state.config.pools,
        };
        const tree_hash = sha1hex(stableStringify(tree_cfg));
        if (this.state.config.pgs.hash != tree_hash)
        {
            // Something has changed
            console.log('Pool configuration or OSD tree changed, re-optimizing');
            // First re-optimize PGs, but don't look at history yet
            const optimize_results = (await Promise.all(Object.keys(this.state.config.pools)
                .map(pool_id => this.generate_pool_pgs(pool_id, osd_tree, levels)))).filter(r => r);
            // Then apply the modification in the form of an optimistic transaction,
            // each time considering new pg/history modifications (OSDs modify it during rebalance)
            while (!await this.apply_pool_pgs(optimize_results, up_osds, osd_tree, tree_hash))
            {
                console.log(
                    'Someone changed PG configuration while we also tried to change it.'+
                    ' Retrying in '+this.config.mon_retry_change_timeout+' ms'
                );
                // Failed to apply - parallel change detected. Wait a bit and retry
                const old_rev = this.etcd_watch_revision;
                while (this.etcd_watch_revision === old_rev)
                {
                    await new Promise(ok => setTimeout(ok, this.config.mon_retry_change_timeout));
                }
                const new_ot = this.get_osd_tree();
                const new_tcfg = {
                    osd_tree: new_ot.osd_tree,
                    levels: new_ot.levels,
                    pools: this.state.config.pools,
                };
                if (sha1hex(stableStringify(new_tcfg)) !== tree_hash)
                {
                    // Configuration actually changed, restart from the beginning
                    this.recheck_pgs_active = false;
                    setImmediate(() => this.recheck_pgs().catch(this.die));
                    return;
                }
                // Configuration didn't change, PG history probably changed, so just retry
            }
            console.log('PG configuration successfully changed');
        }
        else
        {
            // Nothing changed, but we still want to recheck the distribution of primaries
            let new_config_pgs;
            let changed = false;
            for (const pool_id in this.state.config.pools)
            {
                const pool_cfg = this.state.config.pools[pool_id];
                if (!this.validate_pool_cfg(pool_id, pool_cfg, false))
                {
                    continue;
                }
                const aff_osds = this.get_affinity_osds(pool_cfg, up_osds, osd_tree);
                this.reset_rng();
                for (let pg_num = 1; pg_num <= pool_cfg.pg_count; pg_num++)
                {
                    if (!this.state.config.pgs.items[pool_id])
                    {
                        continue;
                    }
                    const pg_cfg = this.state.config.pgs.items[pool_id][pg_num];
                    if (pg_cfg)
                    {
                        const new_primary = this.pick_primary(pool_id, pg_cfg.osd_set, up_osds, aff_osds);
                        if (pg_cfg.primary != new_primary)
                        {
                            if (!new_config_pgs)
                            {
                                new_config_pgs = JSON.parse(JSON.stringify(this.state.config.pgs));
                            }
                            console.log(
                                `Moving pool ${pool_id} (${pool_cfg.name || 'unnamed'}) PG ${pg_num}`+
                                ` primary OSD from ${pg_cfg.primary} to ${new_primary}`
                            );
                            changed = true;
                            new_config_pgs.items[pool_id][pg_num].primary = new_primary;
                        }
                    }
                }
            }
            if (changed)
            {
                const ok = await this.save_pg_config(new_config_pgs);
                if (ok)
                    console.log('PG configuration successfully changed');
                else
                {
                    console.log('Someone changed PG configuration while we also tried to change it. Retrying in '+this.config.mon_change_timeout+' ms');
                    this.schedule_recheck();
                }
            }
        }
        this.recheck_pgs_active = false;
    }

    async apply_pool_pgs(results, up_osds, osd_tree, tree_hash)
    {
        for (const pool_id in (this.state.config.pgs||{}).items||{})
        {
            // We should stop all PGs when deleting a pool or changing its PG count
            if (!this.state.config.pools[pool_id] ||
                this.state.config.pgs.items[pool_id] && this.state.config.pools[pool_id].pg_count !=
                Object.keys(this.state.config.pgs.items[pool_id]).reduce((a, c) => (a < (0|c) ? (0|c) : a), 0))
            {
                if (!await this.stop_all_pgs(pool_id))
                {
                    return false;
                }
            }
        }
        const new_config_pgs = JSON.parse(JSON.stringify(this.state.config.pgs));
        const etcd_request = { compare: [], success: [] };
        for (const pool_id in (new_config_pgs||{}).items||{})
        {
            if (!this.state.config.pools[pool_id])
            {
                const prev_pgs = [];
                for (const pg in new_config_pgs.items[pool_id]||{})
                {
                    prev_pgs[pg-1] = new_config_pgs.items[pool_id][pg].osd_set;
                }
                // Also delete pool statistics
                etcd_request.success.push({ requestDeleteRange: {
                    key: b64(this.etcd_prefix+'/pool/stats/'+pool_id),
                } });
                this.save_new_pgs_txn(new_config_pgs, etcd_request, pool_id, up_osds, osd_tree, prev_pgs, [], []);
            }
        }
        for (const pool_res of results)
        {
            const pool_id = pool_res.pool_id;
            const pool_cfg = this.state.config.pools[pool_id];
            let pg_history = [];
            for (const pg in ((this.state.config.pgs.items||{})[pool_id]||{}))
            {
                if (this.state.pg.history[pool_id] &&
                    this.state.pg.history[pool_id][pg])
                {
                    pg_history[pg-1] = this.state.pg.history[pool_id][pg];
                }
            }
            const real_prev_pgs = [];
            for (const pg in ((this.state.config.pgs.items||{})[pool_id]||{}))
            {
                real_prev_pgs[pg-1] = [ ...this.state.config.pgs.items[pool_id][pg].osd_set ];
            }
            if (real_prev_pgs.length > 0 && real_prev_pgs.length != pool_res.pgs.length)
            {
                console.log(
                    `Changing PG count for pool ${pool_id} (${pool_cfg.name || 'unnamed'})`+
                    ` from: ${real_prev_pgs.length} to ${pool_res.pgs.length}`
                );
                pg_history = PGUtil.scale_pg_history(pg_history, real_prev_pgs, pool_res.pgs);
                // Drop stats
                etcd_request.success.push({ requestDeleteRange: {
                    key: b64(this.etcd_prefix+'/pg/stats/'+pool_id+'/'),
                    range_end: b64(this.etcd_prefix+'/pg/stats/'+pool_id+'0'),
                } });
            }
            const stats = {
                used_raw_tb: (this.state.pool.stats[pool_id]||{}).used_raw_tb || 0,
                ...pool_res.stats,
            };
            etcd_request.success.push({ requestPut: {
                key: b64(this.etcd_prefix+'/pool/stats/'+pool_id),
                value: b64(JSON.stringify(stats)),
            } });
            this.save_new_pgs_txn(new_config_pgs, etcd_request, pool_id, up_osds, osd_tree, real_prev_pgs, pool_res.pgs, pg_history);
        }
        new_config_pgs.hash = tree_hash;
        return await this.save_pg_config(new_config_pgs, etcd_request);
    }

    async save_pg_config(new_config_pgs, etcd_request = { compare: [], success: [] })
    {
        etcd_request.compare.push(
            { key: b64(this.etcd_prefix+'/mon/master'), target: 'LEASE', lease: ''+this.etcd_lease_id },
            { key: b64(this.etcd_prefix+'/config/pgs'), target: 'MOD', mod_revision: ''+this.etcd_watch_revision, result: 'LESS' },
        );
        etcd_request.success.push(
            { requestPut: { key: b64(this.etcd_prefix+'/config/pgs'), value: b64(JSON.stringify(new_config_pgs)) } },
        );
        const txn_res = await this.etcd_call('/kv/txn', etcd_request, this.config.etcd_mon_timeout, 0);
        return txn_res.succeeded;
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
    // This is required for multiple change events to trigger at most 1 recheck in 1s
    schedule_recheck()
    {
        if (!this.recheck_timer)
        {
            this.recheck_timer = setTimeout(() =>
            {
                this.recheck_timer = null;
                this.recheck_pgs().catch(this.die);
            }, this.config.mon_change_timeout || 1000);
        }
    }

    derive_osd_stats(st, prev, prev_diff)
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

    sum_op_stats()
    {
        for (const osd in this.state.osd.stats)
        {
            const cur = { ...this.state.osd.stats[osd], inode_stats: this.state.osd.inodestats[osd]||{} };
            this.prev_stats.osd_diff[osd] = this.derive_osd_stats(
                cur, this.prev_stats.osd_stats[osd], this.prev_stats.osd_diff[osd]
            );
            this.prev_stats.osd_stats[osd] = cur;
        }
        const sum_diff = { op_stats: {}, subop_stats: {}, recovery_stats: {} };
        // Sum derived values instead of deriving summed
        for (const osd in this.state.osd.state)
        {
            const derived = this.prev_stats.osd_diff[osd];
            if (!this.state.osd.state[osd] || !derived)
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

    sum_object_counts()
    {
        const object_counts = { object: 0n, clean: 0n, misplaced: 0n, degraded: 0n, incomplete: 0n };
        const object_bytes = { object: 0n, clean: 0n, misplaced: 0n, degraded: 0n, incomplete: 0n };
        for (const pool_id in this.state.pg.stats)
        {
            let object_size = 0;
            for (const osd_num of this.state.pg.stats[pool_id].write_osd_set||[])
            {
                if (osd_num && this.state.osd.stats[osd_num] && this.state.osd.stats[osd_num].block_size)
                {
                    object_size = this.state.osd.stats[osd_num].block_size;
                    break;
                }
            }
            const pool_cfg = (this.state.config.pools[pool_id]||{});
            if (!object_size)
            {
                object_size = pool_cfg.block_size || this.config.block_size || 131072;
            }
            if (pool_cfg.scheme !== 'replicated')
            {
                object_size *= ((pool_cfg.pg_size||0) - (pool_cfg.parity_chunks||0));
            }
            object_size = BigInt(object_size);
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
                            object_bytes[k] += BigInt(st[k+'_count']) * object_size;
                        }
                    }
                }
            }
        }
        return { object_counts, object_bytes };
    }

    sum_inode_stats()
    {
        const inode_stats = {};
        const inode_stub = () => ({
            raw_used: 0n,
            read: { count: 0n, usec: 0n, bytes: 0n, bps: 0n, iops: 0n, lat: 0n },
            write: { count: 0n, usec: 0n, bytes: 0n, bps: 0n, iops: 0n, lat: 0n },
            delete: { count: 0n, usec: 0n, bytes: 0n, bps: 0n, iops: 0n, lat: 0n },
        });
        const seen_pools = {};
        for (const pool_id in this.state.config.pools)
        {
            seen_pools[pool_id] = true;
            this.state.pool.stats[pool_id] = this.state.pool.stats[pool_id] || {};
            this.state.pool.stats[pool_id].used_raw_tb = 0n;
        }
        for (const osd_num in this.state.osd.space)
        {
            for (const pool_id in this.state.osd.space[osd_num])
            {
                this.state.pool.stats[pool_id] = this.state.pool.stats[pool_id] || {};
                if (!seen_pools[pool_id])
                {
                    this.state.pool.stats[pool_id].used_raw_tb = 0n;
                    seen_pools[pool_id] = true;
                }
                inode_stats[pool_id] = inode_stats[pool_id] || {};
                for (const inode_num in this.state.osd.space[osd_num][pool_id])
                {
                    const u = BigInt(this.state.osd.space[osd_num][pool_id][inode_num]||0);
                    if (inode_num)
                    {
                        inode_stats[pool_id][inode_num] = inode_stats[pool_id][inode_num] || inode_stub();
                        inode_stats[pool_id][inode_num].raw_used += u;
                    }
                    this.state.pool.stats[pool_id].used_raw_tb += u;
                }
            }
        }
        for (const pool_id in seen_pools)
        {
            const used = this.state.pool.stats[pool_id].used_raw_tb;
            this.state.pool.stats[pool_id].used_raw_tb = Number(used)/1024/1024/1024/1024;
        }
        for (const osd_num in this.state.osd.state)
        {
            const ist = this.state.osd.inodestats[osd_num];
            if (!ist || !this.state.osd.state[osd_num])
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
        for (const osd in this.state.osd.state)
        {
            const osd_diff = this.prev_stats.osd_diff[osd];
            if (!osd_diff || !this.state.osd.state[osd])
            {
                continue;
            }
            for (const pool_id in osd_diff.inode_stats)
            {
                for (const inode_num in this.prev_stats.osd_diff[osd].inode_stats[pool_id])
                {
                    inode_stats[pool_id][inode_num] = inode_stats[pool_id][inode_num] || inode_stub();
                    for (const op of [ 'read', 'write', 'delete' ])
                    {
                        const op_diff = this.prev_stats.osd_diff[osd].inode_stats[pool_id][inode_num][op] || {};
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
                if (!nonzero && (!this.state.config.inode[pool_id] || !this.state.config.inode[pool_id][inode_num]))
                {
                    // Deleted inode (no data, no I/O, no config)
                    delete inode_stats[pool_id][inode_num];
                }
            }
        }
        return { inode_stats, seen_pools };
    }

    serialize_bigints(obj)
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
                obj[k] = this.serialize_bigints(obj[k]);
            }
        }
        return obj;
    }

    async update_total_stats()
    {
        const txn = [];
        const { object_counts, object_bytes } = this.sum_object_counts();
        let stats = this.sum_op_stats();
        let { inode_stats, seen_pools } = this.sum_inode_stats();
        stats.object_counts = object_counts;
        stats.object_bytes = object_bytes;
        stats = this.serialize_bigints(stats);
        inode_stats = this.serialize_bigints(inode_stats);
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
        for (const pool_id in this.state.inode.stats)
        {
            for (const inode_num in this.state.inode.stats[pool_id])
            {
                if (!inode_stats[pool_id] || !inode_stats[pool_id][inode_num])
                {
                    txn.push({ requestDeleteRange: {
                        key: b64(this.etcd_prefix+'/inode/stats/'+pool_id+'/'+inode_num),
                    } });
                }
            }
        }
        for (const pool_id in this.state.pool.stats)
        {
            if (!seen_pools[pool_id])
            {
                txn.push({ requestDeleteRange: {
                    key: b64(this.etcd_prefix+'/pool/stats/'+pool_id),
                } });
                delete this.state.pool.stats[pool_id];
            }
            else
            {
                const pool_stats = { ...this.state.pool.stats[pool_id] };
                this.serialize_bigints(pool_stats);
                txn.push({ requestPut: {
                    key: b64(this.etcd_prefix+'/pool/stats/'+pool_id),
                    value: b64(JSON.stringify(pool_stats)),
                } });
            }
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
            return;
        }
        this.stats_timer = setTimeout(() =>
        {
            this.stats_timer = null;
            this.update_total_stats().catch(console.error);
        }, this.config.mon_stats_timeout);
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
        const old = cur[key_parts[key_parts.length-1]];
        cur[key_parts[key_parts.length-1]] = kv.value;
        if (key === 'config/global')
        {
            this.config = { ...this.initConfig, ...this.state.config.global };
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
            // Recheck OSD tree on OSD addition/deletion
            const osd_num = key_parts[2];
            if ((!old) != (!kv.value) || old && kv.value && old.size != kv.value.size)
            {
                this.schedule_recheck();
            }
            // Recheck PGs <osd_out_time> after last OSD statistics report
            this.schedule_next_recheck_at(
                !this.state.osd.stats[osd_num] ? 0 : this.state.osd.stats[osd_num].time+this.config.osd_out_time
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
        const tried = {};
        while (retries < 0 || retry < retries)
        {
            retry++;
            const base = this.pick_next_etcd();
            let now = Date.now();
            if (tried[base] && now-tried[base] < timeout)
            {
                await new Promise(ok => setTimeout(ok, timeout-(now-tried[base])));
                now = Date.now();
            }
            tried[base] = now;
            const res = await POST(base+path, body, timeout);
            if (res.error)
            {
                if (this.selected_etcd_url == base)
                    this.selected_etcd_url = null;
                console.error('failed to query etcd: '+res.error);
                continue;
            }
            if (res.json)
            {
                if (res.json.error)
                {
                    console.error('etcd returned error: '+res.json.error);
                    break;
                }
                return res.json;
            }
        }
        this.failconnect();
    }

    _die(err, code)
    {
        // In fact we can just try to rejoin
        console.error(err instanceof Error ? err : new Error(err || 'Cluster connection failed'));
        process.exit(code || 2);
    }

    local_ips(all)
    {
        const ips = [];
        const ifaces = os.networkInterfaces();
        for (const ifname in ifaces)
        {
            for (const iface of ifaces[ifname])
            {
                if (iface.family == 'IPv4' && !iface.internal || all)
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
    return new Promise(ok =>
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
            res.on('error', (error) => ok({ error }));
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
        req.on('error', (error) => ok({ error }));
        req.on('close', () => ok({ error: new Error('Connection closed prematurely') }));
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
