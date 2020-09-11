const http = require('http');
const crypto = require('crypto');
const os = require('os');
const WebSocket = require('ws');
const LPOptimizer = require('./lp-optimizer.js');
const stableStringify = require('./stable-stringify.js');
const PGUtil = require('./PGUtil.js');

// FIXME Split into several files
class Mon
{
    // FIXME document all etcd keys and config variables in the form of JSON schema or similar
    static etcd_allow = new RegExp('^'+[
        'config/global',
        'config/node_placement',
        'config/pools',
        'config/osd/[1-9]\\d*',
        'config/pgs',
        'osd/state/[1-9]\\d*',
        'osd/stats/[1-9]\\d*',
        'mon/master',
        'pg/state/[1-9]\\d*/[1-9]\\d*',
        'pg/stats/[1-9]\\d*/[1-9]\\d*',
        'pg/history/[1-9]\\d*/[1-9]\\d*',
        'stats',
    ].join('$|^')+'$')

    static etcd_tree = {
        config: {
            /* global: {
                // mon
                etcd_mon_ttl: 30, // min: 10
                etcd_mon_timeout: 1000, // ms. min: 0
                etcd_mon_retries: 5, // min: 0
                mon_change_timeout: 1000, // ms. min: 100
                mon_stats_timeout: 1000, // ms. min: 100
                osd_out_time: 1800, // seconds. min: 0
                placement_levels: { datacenter: 1, rack: 2, host: 3, osd: 4, ... },
                // client and osd
                use_sync_send_recv: false,
                log_level: 0,
                block_size: 131072,
                disk_alignment: 4096,
                bitmap_granularity: 4096,
                pg_stripe_size: 4194304,
                immediate_commit: false, // 'all' or 'small'
                client_dirty_limit: 33554432,
                peer_connect_interval: 5, // seconds. min: 1
                peer_connect_timeout: 5, // seconds. min: 1
                up_wait_retry_interval: 500, // ms. min: 50
                // osd
                etcd_report_interval: 30, // min: 10
                run_primary: true,
                bind_address: "0.0.0.0",
                bind_port: 0,
                autosync_interval: 5,
                client_queue_depth: 128, // unused
                recovery_queue_depth: 4,
                readonly: false,
                print_stats_interval: 3,
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
                flusher_count,
                inmemory_metadata,
                inmemory_journal,
                journal_sector_buffer_count,
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
                    scheme: 'xor',
                    pg_size: 3,
                    pg_minsize: 2,
                    pg_count: 100,
                    failure_domain: 'host',
                    max_osd_combinations: 10000,
                    // FIXME add device classes/tags
                },
                ...
            }, */
            pools: {},
            osd: {
                /* <id>: { reweight: 1 }, ... */
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
                        state: ("starting"|"peering"|"incomplete"|"active"|"stopping"|"offline"|
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
    }

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
        this.parse_kv(res.responses[0].response_range.kvs[0]);
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
        // After this number of seconds, a dead OSD will be removed from PG distribution
        this.config.osd_out_time = Number(this.config.osd_out_time) || 0;
        if (!this.config.osd_out_time)
        {
            this.config.osd_out_time = 30*60; // 30 minutes by default
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
                this.ws.on('open', () =>
                {
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
                let stats_changed = false, changed = false;
                if (this.verbose)
                {
                    console.log('Revision '+data.result.header.revision+' events: ');
                }
                this.etcd_watch_revision = BigInt(data.result.header.revision)+BigInt(1);
                for (const e of data.result.events)
                {
                    this.parse_kv(e.kv);
                    const key = e.kv.key.substr(this.etcd_prefix.length);
                    if (key.substr(0, 11) == '/osd/stats/' || key.substr(0, 10) == '/pg/stats/')
                    {
                        stats_changed = true;
                    }
                    else if (key != '/stats')
                    {
                        changed = true;
                    }
                    if (this.verbose)
                    {
                        console.log(e);
                    }
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

    async get_lease()
    {
        const max_ttl = this.config.etcd_mon_ttl + this.config.etcd_mon_timeout/1000*this.config.etcd_mon_retries;
        const res = await this.etcd_call('/lease/grant', { TTL: max_ttl }, this.config.etcd_mon_timeout, this.config.etcd_mon_retries);
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
            if (stat.size && (this.state.osd.state[osd_num] || Number(stat.time) >= down_time))
            {
                // Numeric IDs are reserved for OSDs
                const reweight = this.state.config.osd[osd_num] && Number(this.state.config.osd[osd_num].reweight) || 1;
                tree[osd_num] = tree[osd_num] || { id: osd_num, parent: stat.host };
                tree[osd_num].level = 'osd';
                tree[osd_num].size = reweight * stat.size / 1024 / 1024 / 1024 / 1024; // terabytes
                delete tree[osd_num].children;
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
            const parent = parent_level && parent_level < node_level ? tree[node_cfg.parent] : '';
            tree[parent].children.push(tree[node_id]);
            delete node_cfg.parent;
        }
        return LPOptimizer.flatten_tree(tree[''].children, levels, this.config.failure_domain, 'osd');
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
            if (!res.succeeded)
            {
                return false;
            }
            this.state.config.pgs = new_cfg;
        }
        return !has_online;
    }

    save_new_pgs_txn(request, pool_id, prev_pgs, new_pgs, pg_history)
    {
        const pg_items = {};
        new_pgs.map((osd_set, i) =>
        {
            osd_set = osd_set.map(osd_num => osd_num === LPOptimizer.NO_OSD ? 0 : osd_num);
            const alive_set = osd_set.filter(osd_num => osd_num);
            pg_items[i+1] = {
                osd_set,
                primary: alive_set.length ? alive_set[Math.floor(Math.random()*alive_set.length)] : 0,
            };
            if (prev_pgs[i] && prev_pgs[i].join(' ') != osd_set.join(' '))
            {
                pg_history[i] = pg_history[i] || {};
                pg_history[i].osd_sets = pg_history[i].osd_sets || [];
                pg_history[i].osd_sets.push(prev_pgs[i]);
            }
        });
        for (let i = 0; i < new_pgs.length || i < prev_pgs.length; i++)
        {
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
        this.state.config.pgs.items[pool_id] = pg_items;
    }

    validate_pool_cfg(pool_id, pool_cfg, warn)
    {
        pool_cfg.pg_size = Math.floor(pool_cfg.pg_size);
        pool_cfg.pg_minsize = Math.floor(pool_cfg.pg_minsize);
        pool_cfg.pg_count = Math.floor(pool_cfg.pg_count);
        pool_cfg.failure_domain = pool_cfg.failure_domain || 'host';
        pool_cfg.max_osd_combinations = Math.floor(pool_cfg.max_osd_combinations) || 10000;
        if (!/^[1-9]\d*$/.exec(''+pool_id))
        {
            if (warn)
                console.log('Pool ID '+pool_id+' is invalid');
            return false;
        }
        if (!pool_cfg.pg_size || pool_cfg.pg_size < 1 ||
            pool_cfg.scheme === 'xor' && pool_cfg.pg_size < 3)
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
        if (pool_cfg.scheme !== 'xor' && pool_cfg.scheme !== 'replicated')
        {
            if (warn)
                console.log('Pool '+pool_id+' has invalid coding scheme (only "xor" and "replicated" are allowed)');
            return false;
        }
        if (pool_cfg.max_osd_combinations < 100)
        {
            if (warn)
                console.log('Pool '+pool_id+' has invalid max_osd_combinations (must be at least 100)');
            return false;
        }
        return true;
    }

    async recheck_pgs()
    {
        // Take configuration and state, check it against the stored configuration hash
        // Recalculate PGs and save them to etcd if the configuration is changed
        // FIXME: Also do not change anything if the distribution is good enough and no PGs are degraded
        const tree_cfg = {
            osd_tree: this.get_osd_tree(),
            pools: this.state.config.pools,
        };
        const tree_hash = sha1hex(stableStringify(tree_cfg));
        if (this.state.config.pgs.hash != tree_hash)
        {
            // Something has changed
            const etcd_request = { compare: [], success: [] };
            for (const pool_id in this.state.config.pools)
            {
                const pool_cfg = this.state.config.pools[pool_id];
                if (!this.validate_pool_cfg(pool_id, pool_cfg, false))
                {
                    continue;
                }
                const prev_pgs = [];
                for (const pg in ((this.state.config.pgs.items||{})[pool_id]||{}).items||{})
                {
                    prev_pgs[pg-1] = this.state.config.pgs.items[pool_id][pg].osd_set;
                }
                const pg_history = [];
                const old_pg_count = prev_pgs.length;
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
                        PGUtil.scale_pg_count(prev_pgs, this.state.pg.history[pool_id]||{}, pg_history, pool_cfg.pg_count);
                    }
                    optimize_result = await LPOptimizer.optimize_change({
                        prev_pgs,
                        osd_tree: tree_cfg.osd_tree,
                        pg_size: pool_cfg.pg_size,
                        pg_minsize: pool_cfg.pg_minsize,
                        max_combinations: pool_cfg.max_osd_combinations,
                    });
                }
                else
                {
                    optimize_result = await LPOptimizer.optimize_initial({
                        osd_tree: tree_cfg.osd_tree,
                        pg_count: pool_cfg.pg_count,
                        pg_size: pool_cfg.pg_size,
                        pg_minsize: pool_cfg.pg_minsize,
                        max_combinations: pool_cfg.max_osd_combinations,
                    });
                }
                if (old_pg_count != optimize_result.int_pgs.length)
                {
                    console.log(
                        `PG count for pool ${pool_id} (${pool_cfg.name || 'unnamed'}) `+
                        `changed from: ${old_pg_count} to ${optimize_result.int_pgs.length}`
                    );
                }
                LPOptimizer.print_change_stats(optimize_result);
                this.save_new_pgs_txn(etcd_request, pool_id, prev_pgs, optimize_result.int_pgs, pg_history);
            }
            this.state.config.pgs.hash = tree_hash;
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
    }

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

    sum_stats()
    {
        let overflow = false;
        this.prev_stats = this.prev_stats || { op_stats: {}, subop_stats: {}, recovery_stats: {} };
        const op_stats = {}, subop_stats = {}, recovery_stats = {};
        for (const osd in this.state.osd.stats)
        {
            const st = this.state.osd.stats[osd];
            for (const op in st.op_stats||{})
            {
                op_stats[op] = op_stats[op] || { count: 0n, usec: 0n, bytes: 0n };
                op_stats[op].count += BigInt(st.op_stats.count||0);
                op_stats[op].usec += BigInt(st.op_stats.usec||0);
                op_stats[op].bytes += BigInt(st.op_stats.bytes||0);
            }
            for (const op in st.subop_stats||{})
            {
                subop_stats[op] = subop_stats[op] || { count: 0n, usec: 0n };
                subop_stats[op].count += BigInt(st.subop_stats.count||0);
                subop_stats[op].usec += BigInt(st.subop_stats.usec||0);
            }
            for (const op in st.recovery_stats||{})
            {
                recovery_stats[op] = recovery_stats[op] || { count: 0n, bytes: 0n };
                recovery_stats[op].count += BigInt(st.recovery_stats.count||0);
                recovery_stats[op].bytes += BigInt(st.recovery_stats.bytes||0);
            }
        }
        for (const op in op_stats)
        {
            if (op_stats[op].count >= 0x10000000000000000n)
            {
                if (!this.prev_stats.op_stats[op])
                {
                    overflow = true;
                }
                else
                {
                    op_stats[op].count -= this.prev_stats.op_stats[op].count;
                    op_stats[op].usec -= this.prev_stats.op_stats[op].usec;
                    op_stats[op].bytes -= this.prev_stats.op_stats[op].bytes;
                }
            }
        }
        for (const op in subop_stats)
        {
            if (subop_stats[op].count >= 0x10000000000000000n)
            {
                if (!this.prev_stats.subop_stats[op])
                {
                    overflow = true;
                }
                else
                {
                    subop_stats[op].count -= this.prev_stats.subop_stats[op].count;
                    subop_stats[op].usec -= this.prev_stats.subop_stats[op].usec;
                }
            }
        }
        for (const op in recovery_stats)
        {
            if (recovery_stats[op].count >= 0x10000000000000000n)
            {
                if (!this.prev_stats.recovery_stats[op])
                {
                    overflow = true;
                }
                else
                {
                    recovery_stats[op].count -= this.prev_stats.recovery_stats[op].count;
                    recovery_stats[op].bytes -= this.prev_stats.recovery_stats[op].bytes;
                }
            }
        }
        const object_counts = { object: 0n, clean: 0n, misplaced: 0n, degraded: 0n, incomplete: 0n };
        for (const pool_id in this.state.pg.stats)
        {
            for (const pg_num in this.state.pg.stats[pool_id])
            {
                const st = this.state.pg.stats[pool_id][pg_num];
                for (const k in object_counts)
                {
                    if (st[k+'_count'])
                    {
                        object_counts[k] += BigInt(st[k+'_count']);
                    }
                }
            }
        }
        return (this.prev_stats = { overflow, op_stats, subop_stats, recovery_stats, object_counts });
    }

    async update_total_stats()
    {
        const stats = this.sum_stats();
        if (!stats.overflow)
        {
            // Convert to strings, serialize and save
            const ser = {};
            for (const st of [ 'op_stats', 'subop_stats', 'recovery_stats' ])
            {
                ser[st] = {};
                for (const op in stats[st])
                {
                    ser[st][op] = {};
                    for (const k in stats[st][op])
                    {
                        ser[st][op][k] = ''+stats[st][op][k];
                    }
                }
            }
            ser.object_counts = {};
            for (const k in stats.object_counts)
            {
                ser.object_counts[k] = ''+stats.object_counts[k];
            }
            await this.etcd_call('/kv/txn', {
                success: [ { requestPut: { key: b64(this.etcd_prefix+'/stats'), value: b64(JSON.stringify(ser)) } } ],
            }, this.config.etcd_mon_timeout, 0);
        }
    }

    schedule_update_stats()
    {
        if (this.stats_timer)
        {
            clearTimeout(this.stats_timer);
            this.stats_timer = null;
        }
        this.stats_timer = setTimeout(() =>
        {
            this.stats_timer = null;
            this.update_total_stats().catch(console.error);
        }, this.config.mon_stats_timeout || 1000);
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
        kv.value = kv.value ? JSON.parse(kv.value) : null;
        key = key.split('/');
        let cur = this.state;
        for (let i = 0; i < key.length-1; i++)
        {
            cur = (cur[key[i]] = cur[key[i]] || {});
        }
        cur[key[key.length-1]] = kv.value;
        if (key.join('/') === 'config/global')
        {
            this.state.config.global = this.state.config.global || {};
            this.config = this.state.config.global;
            this.check_config();
        }
        else if (key.join('/') === 'config/pools')
        {
            for (const pool_id in this.state.config.pools)
            {
                const pool_cfg = this.state.config.pools[pool_id];
                this.validate_pool_cfg(pool_id, pool_cfg, true);
            }
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

module.exports = Mon;
