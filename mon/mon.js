// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

const { URL } = require('url');
const fs = require('fs');
const crypto = require('crypto');
const os = require('os');
const AntiEtcdAdapter = require('./antietcd_adapter.js');
const EtcdAdapter = require('./etcd_adapter.js');
const { create_http_server } = require('./http_server.js');
const { export_prometheus_metrics } = require('./prometheus.js');
const { etcd_tree, etcd_allow, etcd_nonempty_keys } = require('./etcd_schema.js');
const { validate_pool_cfg } = require('./pool_config.js');
const { sum_op_stats, sum_object_counts, sum_inode_stats, serialize_bigints } = require('./stats.js');
const stableStringify = require('./stable-stringify.js');
const { scale_pg_history } = require('./pg_utils.js');
const { get_osd_tree } = require('./osd_tree.js');
const { b64, de64, local_ips } = require('./utils.js');
const { recheck_primary, save_new_pgs_txn, generate_pool_pgs } = require('./pg_gen.js');

class Mon
{
    static async run_forever(config)
    {
        let mergedConfig = config;
        if (fs.existsSync(config.config_path||'/etc/vitastor/vitastor.conf'))
        {
            const fileConfig = JSON.parse(fs.readFileSync(config.config_path||'/etc/vitastor/vitastor.conf', { encoding: 'utf-8' }));
            mergedConfig = { ...fileConfig, ...config };
        }
        let antietcd = await AntiEtcdAdapter.start_antietcd(mergedConfig);
        let mon;
        const run = () =>
        {
            console.log('Starting Monitor');
            const my_mon = new Mon(config);
            my_mon.etcd = antietcd
                ? new AntiEtcdAdapter(my_mon, antietcd)
                : new EtcdAdapter(my_mon);
            my_mon.etcd.parse_config(my_mon.config);
            mon = my_mon;
            my_mon.on_die = () =>
            {
                if (mon == my_mon)
                {
                    // Start a new instance
                    run();
                }
            };
            my_mon.start().catch(my_mon.die);
        };
        run();
        const on_stop_cb = () => mon.on_stop().then(() => process.exit(0)).catch(err =>
        {
            console.error(err);
            process.exit(0);
        });
        process.on('SIGINT', on_stop_cb);
        process.on('SIGTERM', on_stop_cb);
    }

    constructor(config)
    {
        this.stopped = false;
        this.die = (e) => this._die(e);
        this.fileConfig = {};
        if (fs.existsSync(config.config_path||'/etc/vitastor/vitastor.conf'))
        {
            this.fileConfig = JSON.parse(fs.readFileSync(config.config_path||'/etc/vitastor/vitastor.conf', { encoding: 'utf-8' }));
        }
        this.cliConfig = config;
        this.config = { ...this.fileConfig, ...this.cliConfig };
        this.check_config();
        this.state = JSON.parse(JSON.stringify(etcd_tree));
        this.prev_stats = { osd_stats: {}, osd_diff: {} };
        this.recheck_pgs_active = false;
        this.updating_total_stats = false;
        this.watcher_active = false;
        this.old_pg_config = false;
        this.old_pg_stats_seen = false;
    }

    async start()
    {
        if (this.config.enable_prometheus || !('enable_prometheus' in this.config))
        {
            this.http = await create_http_server(this.config, (req, res) =>
            {
                const u = new URL(req.url, 'http://'+(req.headers.host || 'localhost'));
                if (u.pathname.replace(/\/+$/, '') == (this.config.prometheus_path||'/metrics'))
                {
                    if (!this.watcher_active)
                    {
                        res.writeHead(503);
                        res.write('Monitor is in standby mode. Please retrieve metrics from master monitor instance\n');
                    }
                    else
                    {
                        res.write(export_prometheus_metrics(this.state));
                    }
                }
                else
                {
                    res.writeHead(404);
                    res.write('Not found. Metrics path: '+(this.config.prometheus_path||'/metrics\n'));
                }
                res.end();
            });
            this.http_connections = new Set();
            this.http.on('connection', conn =>
            {
                this.http_connections.add(conn);
                conn.once('close', () => this.http_connections.delete(conn));
            });
        }
        await this.load_config();
        await this.get_lease();
        await this.etcd.become_master();
        await this.load_cluster_state();
        await this.etcd.start_watcher(this.config.etcd_mon_retries);
        this.watcher_active = true;
        for (const pool_id in this.state.config.pools)
        {
            if (!this.state.pool.stats[pool_id] ||
                !Number(this.state.pool.stats[pool_id].pg_real_size))
            {
                // Generate missing data in etcd
                this.state.pg.config.hash = null;
                break;
            }
        }
        await this.recheck_pgs();
        this.schedule_update_stats();
    }

    async load_config()
    {
        const res = await this.etcd.etcd_call('/kv/txn', { success: [
            { requestRange: { key: b64(this.config.etcd_prefix+'/config/global') } }
        ] }, this.config.etcd_start_timeout, -1);
        if (res.responses[0].response_range.kvs)
        {
            this.parse_kv(res.responses[0].response_range.kvs[0]);
        }
    }

    check_config()
    {
        this.config.etcd_prefix = this.config.etcd_prefix || '/vitastor';
        this.config.etcd_prefix = this.config.etcd_prefix.replace(/\/\/+/g, '/').replace(/^\/?(.*[^\/])\/?$/, '/$1');
        this.config.etcd_start_timeout = (this.config.etcd_start_timeout || 5) * 1000;
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

    on_message(msg)
    {
        let stats_changed = false, changed = false, pg_states_changed = false;
        if (this.config.verbose)
        {
            console.log('Revision '+msg.header.revision+' events: ');
        }
        this.etcd_watch_revision = BigInt(msg.header.revision)+BigInt(1);
        for (const e of msg.events||[])
        {
            const kv = this.parse_kv(e.kv);
            const key = kv.key.substr(this.config.etcd_prefix.length);
            if (key.substr(0, 11) == '/osd/state/')
            {
                stats_changed = true;
                changed = true;
            }
            else if (key.substr(0, 11) == '/osd/stats/' || key.substr(0, 9) == '/pgstats/' || key.substr(0, 16) == '/osd/inodestats/')
            {
                stats_changed = true;
            }
            else if (key.substr(0, 10) == '/pg/stats/')
            {
                this.old_pg_stats_seen = true;
                stats_changed = true;
            }
            else if (key.substr(0, 10) == '/pg/state/')
            {
                pg_states_changed = true;
            }
            else if (key != '/stats' && key.substr(0, 13) != '/inode/stats/')
            {
                if (key == '/config/pgs' && !kv.value)
                {
                    this.old_pg_config = false;
                }
                changed = true;
            }
            if (this.config.verbose)
            {
                console.log(JSON.stringify({ ...e, kv: kv || undefined }));
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

    // Schedule save_last_clean() to to run after a small timeout (1s) (to not spam etcd)
    schedule_save_last_clean()
    {
        if (this.stopped)
        {
            return;
        }
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
            if (!validate_pool_cfg(pool_id, pool_cfg, this.config.placement_levels, false))
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
            new_clean_pgs.items[pool_id] = this.state.pg.config.items[pool_id];
        }
        this.state.history.last_clean_pgs = new_clean_pgs;
        await this.etcd.etcd_call('/kv/txn', {
            success: [ { requestPut: {
                key: b64(this.config.etcd_prefix+'/history/last_clean_pgs'),
                value: b64(JSON.stringify(this.state.history.last_clean_pgs))
            } } ],
        }, this.config.etcd_start_timeout, 0);
        this.save_last_clean_running = false;
    }

    get_mon_state()
    {
        return { ip: local_ips(), hostname: os.hostname() };
    }

    async get_lease()
    {
        const max_ttl = this.config.etcd_mon_ttl + this.config.etcd_mon_timeout/1000*this.config.etcd_mon_retries;
        // Get lease
        let res = await this.etcd.etcd_call('/lease/grant', { TTL: max_ttl }, this.config.etcd_mon_timeout, -1);
        this.etcd_lease_id = res.ID;
        // Register in /mon/member, just for the information
        const state = this.get_mon_state();
        res = await this.etcd.etcd_call('/kv/put', {
            key: b64(this.config.etcd_prefix+'/mon/member/'+this.etcd_lease_id),
            value: b64(JSON.stringify(state)),
            lease: ''+this.etcd_lease_id
        }, this.config.etcd_start_timeout, 0);
        // Set refresh timer
        this.lease_timer = setInterval(() =>
        {
            this.etcd.etcd_call('/lease/keepalive', { ID: this.etcd_lease_id }, this.config.etcd_mon_timeout, this.config.etcd_mon_retries)
                .then(res =>
                {
                    if (!res.result.TTL)
                        this.die('Lease expired');
                })
                .catch(this.die);
        }, this.config.etcd_mon_ttl*1000);
    }

    async on_stop()
    {
        console.log('Stopping Monitor');
        if (this.http)
        {
            await new Promise(ok =>
            {
                this.http.close(ok);
                for (const conn of this.http_connections)
                    conn.destroy();
            });
            this.http = null;
        }
        this.etcd.stop_watcher();
        if (this.save_last_clean_timer)
        {
            clearTimeout(this.save_last_clean_timer);
            this.save_last_clean_timer = null;
        }
        if (this.next_recheck_timer)
        {
            clearTimeout(this.next_recheck_timer);
            this.next_recheck_timer = null;
        }
        if (this.recheck_timer)
        {
            clearTimeout(this.recheck_timer);
            this.recheck_timer = null;
        }
        if (this.stats_timer)
        {
            clearTimeout(this.stats_timer);
            this.stats_timer = null;
        }
        if (this.lease_timer)
        {
            clearInterval(this.lease_timer);
            this.lease_timer = null;
        }
        let p = null;
        if (this.etcd_lease_id)
        {
            const lease_id = this.etcd_lease_id;
            this.etcd_lease_id = null;
            p = this.etcd.etcd_call('/lease/revoke', { ID: lease_id }, this.config.etcd_mon_timeout, this.config.etcd_mon_retries);
        }
        // 'stopped' flag prevents all further etcd communications of this instance
        this.stopped = true;
        if (p)
        {
            await p;
        }
    }

    async load_cluster_state()
    {
        const res = await this.etcd.etcd_call('/kv/txn', { success: [
            { requestRange: { key: b64(this.config.etcd_prefix+'/'), range_end: b64(this.config.etcd_prefix+'0') } },
        ] }, this.config.etcd_start_timeout, -1);
        this.etcd_watch_revision = BigInt(res.header.revision)+BigInt(1);
        this.state = JSON.parse(JSON.stringify(etcd_tree));
        for (const response of res.responses)
        {
            for (const kv of response.response_range.kvs)
            {
                this.parse_kv(kv);
            }
        }
        if (Object.keys((this.state.config.pgs||{}).items||{}).length)
        {
            // Support seamless upgrade to new OSDs
            if (!Object.keys((this.state.pg.config||{}).items||{}).length)
            {
                const pgs = JSON.stringify(this.state.config.pgs);
                this.state.pg.config = JSON.parse(pgs);
                const res = await this.etcd.etcd_call('/kv/txn', {
                    success: [
                        { requestPut: { key: b64(this.config.etcd_prefix+'/pg/config'), value: b64(pgs) } },
                    ],
                    compare: [
                        { key: b64(this.config.etcd_prefix+'/pg/config'), target: 'MOD', mod_revision: ''+this.etcd_watch_revision, result: 'LESS' },
                    ],
                }, this.config.etcd_mon_timeout, this.config.etcd_mon_retries);
                if (!res.succeeded)
                    throw new Error('Failed to duplicate old PG config to new PG config');
            }
            this.old_pg_config = true;
        }
    }

    all_osds()
    {
        return Object.keys(this.state.osd.stats);
    }

    async stop_all_pgs(pool_id)
    {
        let has_online = false, paused = true;
        for (const pg in this.state.pg.config.items[pool_id]||{})
        {
            // FIXME: Change all (||{}) to ?. (optional chaining) at some point
            const cur_state = (((this.state.pg.state[pool_id]||{})[pg]||{}).state||[]).join(',');
            if (cur_state != '' && cur_state != 'offline')
            {
                has_online = true;
            }
            if (!this.state.pg.config.items[pool_id][pg].pause)
            {
                paused = false;
            }
        }
        if (!paused)
        {
            console.log('Stopping all PGs for pool '+pool_id+' before changing PG count');
            const new_cfg = JSON.parse(JSON.stringify(this.state.pg.config));
            for (const pg in new_cfg.items[pool_id])
            {
                new_cfg.items[pool_id][pg].pause = true;
            }
            // Check that no OSDs change their state before we pause PGs
            // Doing this we make sure that OSDs don't wake up in the middle of our "transaction"
            // and can't see the old PG configuration
            const checks = [
                { key: b64(this.config.etcd_prefix+'/mon/master'), target: 'LEASE', lease: ''+this.etcd_lease_id },
                { key: b64(this.config.etcd_prefix+'/pg/config'), target: 'MOD', mod_revision: ''+this.etcd_watch_revision, result: 'LESS' },
            ];
            for (const osd_num of this.all_osds())
            {
                const key = b64(this.config.etcd_prefix+'/osd/state/'+osd_num);
                checks.push({ key, target: 'MOD', result: 'LESS', mod_revision: ''+this.etcd_watch_revision });
            }
            const txn = {
                compare: checks,
                success: [
                    { requestPut: { key: b64(this.config.etcd_prefix+'/pg/config'), value: b64(JSON.stringify(new_cfg)) } },
                ],
            };
            if (this.old_pg_config)
            {
                txn.success.push({ requestPut: { key: b64(this.config.etcd_prefix+'/config/pgs'), value: b64(JSON.stringify(new_cfg)) } });
            }
            await this.etcd.etcd_call('/kv/txn', txn, this.config.etcd_mon_timeout, 0);
            return false;
        }
        return !has_online;
    }

    async recheck_pgs()
    {
        if (this.stopped)
        {
            return;
        }
        if (this.recheck_pgs_active)
        {
            this.schedule_recheck();
            return;
        }
        this.recheck_pgs_active = true;
        // Take configuration and state, check it against the stored configuration hash
        // Recalculate PGs and save them to etcd if the configuration is changed
        // FIXME: Do not change anything if the distribution is good and random enough and no PGs are degraded
        const { up_osds, levels, osd_tree } = get_osd_tree(this.config, this.state);
        const tree_cfg = {
            osd_tree,
            levels,
            pools: this.state.config.pools,
        };
        const tree_hash = sha1hex(stableStringify(tree_cfg));
        if (this.state.pg.config.hash != tree_hash)
        {
            // Something has changed
            console.log('Pool configuration or OSD tree changed, re-optimizing');
            // First re-optimize PGs, but don't look at history yet
            const optimize_results = (await Promise.all(Object.keys(this.state.config.pools)
                .map(pool_id => generate_pool_pgs(this.state, this.config, pool_id, osd_tree, levels)))).filter(r => r);
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
                const new_ot = get_osd_tree(this.config, this.state);
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
            let new_pg_config = recheck_primary(this.state, this.config, up_osds, osd_tree);
            if (new_pg_config)
            {
                const ok = await this.save_pg_config(new_pg_config);
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
        const etcd_request = { compare: [], success: [] };
        for (const pool_id in (this.state.pg.config||{}).items||{})
        {
            // We should stop all PGs when deleting a pool or changing its PG count
            if (!this.state.config.pools[pool_id] ||
                this.state.pg.config.items[pool_id] && this.state.config.pools[pool_id].pg_count !=
                Object.keys(this.state.pg.config.items[pool_id]).reduce((a, c) => (a < (0|c) ? (0|c) : a), 0))
            {
                if (!await this.stop_all_pgs(pool_id))
                {
                    return false;
                }
            }
            if (!this.state.config.pools[pool_id])
            {
                // Delete PG history and stats of the deleted pool
                etcd_request.success.push({ requestDeleteRange: {
                    key: b64(this.config.etcd_prefix+'/pg/history/'+pool_id+'/'),
                    range_end: b64(this.config.etcd_prefix+'/pg/history/'+pool_id+'0'),
                } });
                etcd_request.success.push({ requestDeleteRange: {
                    key: b64(this.config.etcd_prefix+'/pg/stats/'+pool_id+'/'),
                    range_end: b64(this.config.etcd_prefix+'/pg/stats/'+pool_id+'0'),
                } });
                etcd_request.success.push({ requestDeleteRange: {
                    key: b64(this.config.etcd_prefix+'/pgstats/'+pool_id+'/'),
                    range_end: b64(this.config.etcd_prefix+'/pgstats/'+pool_id+'0'),
                } });
            }
        }
        const new_pg_config = JSON.parse(JSON.stringify(this.state.pg.config));
        for (const pool_id in (new_pg_config||{}).items||{})
        {
            if (!this.state.config.pools[pool_id])
            {
                const prev_pgs = [];
                for (const pg in new_pg_config.items[pool_id]||{})
                {
                    prev_pgs[pg-1] = new_pg_config.items[pool_id][pg].osd_set;
                }
                // Also delete pool statistics
                etcd_request.success.push({ requestDeleteRange: {
                    key: b64(this.config.etcd_prefix+'/pool/stats/'+pool_id),
                } });
                save_new_pgs_txn(new_pg_config, etcd_request, this.state, this.config.etcd_prefix,
                    this.etcd_watch_revision, pool_id, up_osds, osd_tree, prev_pgs, [], []);
            }
        }
        for (const pool_res of results)
        {
            const pool_id = pool_res.pool_id;
            const pool_cfg = this.state.config.pools[pool_id];
            let pg_history = [];
            for (const pg in ((this.state.pg.config.items||{})[pool_id]||{}))
            {
                if (this.state.pg.history[pool_id] &&
                    this.state.pg.history[pool_id][pg])
                {
                    pg_history[pg-1] = this.state.pg.history[pool_id][pg];
                }
            }
            const real_prev_pgs = [];
            for (const pg in ((this.state.pg.config.items||{})[pool_id]||{}))
            {
                real_prev_pgs[pg-1] = [ ...this.state.pg.config.items[pool_id][pg].osd_set ];
            }
            if (real_prev_pgs.length > 0 && real_prev_pgs.length != pool_res.pgs.length)
            {
                console.log(
                    `Changing PG count for pool ${pool_id} (${pool_cfg.name || 'unnamed'})`+
                    ` from: ${real_prev_pgs.length} to ${pool_res.pgs.length}`
                );
                pg_history = scale_pg_history(pg_history, real_prev_pgs, pool_res.pgs);
                // Drop stats
                etcd_request.success.push({ requestDeleteRange: {
                    key: b64(this.config.etcd_prefix+'/pgstats/'+pool_id+'/'),
                    range_end: b64(this.config.etcd_prefix+'/pgstats/'+pool_id+'0'),
                } });
            }
            const stats = {
                used_raw_tb: (this.state.pool.stats[pool_id]||{}).used_raw_tb || 0,
                ...pool_res.stats,
            };
            etcd_request.success.push({ requestPut: {
                key: b64(this.config.etcd_prefix+'/pool/stats/'+pool_id),
                value: b64(JSON.stringify(stats)),
            } });
            save_new_pgs_txn(new_pg_config, etcd_request, this.state, this.config.etcd_prefix,
                this.etcd_watch_revision, pool_id, up_osds, osd_tree, real_prev_pgs, pool_res.pgs, pg_history);
        }
        new_pg_config.hash = tree_hash;
        const { backfillfull_pools, backfillfull_osds } = sum_object_counts(
            { ...this.state, pg: { ...this.state.pg, config: new_pg_config } }, this.config
        );
        if (backfillfull_pools.join(',') != ((this.state.pg.config||{}).backfillfull_pools||[]).join(','))
        {
            this.log_backfillfull(backfillfull_osds, backfillfull_pools);
        }
        new_pg_config.backfillfull_pools = backfillfull_pools.length ? backfillfull_pools : undefined;
        if (!await this.save_pg_config(new_pg_config, etcd_request))
        {
            return false;
        }
        return true;
    }

    async save_pg_config(new_pg_config, etcd_request = { compare: [], success: [] })
    {
        etcd_request.compare.push(
            { key: b64(this.config.etcd_prefix+'/mon/master'), target: 'LEASE', lease: ''+this.etcd_lease_id },
            { key: b64(this.config.etcd_prefix+'/pg/config'), target: 'MOD', mod_revision: ''+this.etcd_watch_revision, result: 'LESS' },
        );
        etcd_request.success.push(
            { requestPut: { key: b64(this.config.etcd_prefix+'/pg/config'), value: b64(JSON.stringify(new_pg_config)) } },
        );
        if (this.old_pg_config)
        {
            etcd_request.success.push({ requestPut: { key: b64(this.config.etcd_prefix+'/config/pgs'), value: b64(JSON.stringify(new_pg_config)) } });
        }
        const txn_res = await this.etcd.etcd_call('/kv/txn', etcd_request, this.config.etcd_mon_timeout, 0);
        return txn_res.succeeded;
    }

    // Schedule next recheck at least at <unixtime>
    schedule_next_recheck_at(unixtime)
    {
        if (this.stopped)
        {
            return;
        }
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
        if (this.stopped)
        {
            return;
        }
        if (!this.recheck_timer)
        {
            this.recheck_timer = setTimeout(() =>
            {
                this.recheck_timer = null;
                this.recheck_pgs().catch(this.die);
            }, this.config.mon_change_timeout || 1000);
        }
    }

    async update_total_stats()
    {
        const txn = [];
        const { object_counts, object_bytes, backfillfull_pools, backfillfull_osds } = sum_object_counts(this.state, this.config);
        let stats = sum_op_stats(this.state.osd, this.prev_stats);
        let { inode_stats, seen_pools } = sum_inode_stats(this.state, this.prev_stats);
        stats.object_counts = object_counts;
        stats.object_bytes = object_bytes;
        stats = serialize_bigints(stats);
        inode_stats = serialize_bigints(inode_stats);
        txn.push({ requestPut: { key: b64(this.config.etcd_prefix+'/stats'), value: b64(JSON.stringify(stats)) } });
        for (const pool_id in inode_stats)
        {
            for (const inode_num in inode_stats[pool_id])
            {
                txn.push({ requestPut: {
                    key: b64(this.config.etcd_prefix+'/inode/stats/'+pool_id+'/'+inode_num),
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
                        key: b64(this.config.etcd_prefix+'/inode/stats/'+pool_id+'/'+inode_num),
                    } });
                }
            }
        }
        if (!this.recheck_pgs_active)
        {
            // PG recheck also modifies /pool/stats, so don't touch it here if it's active
            for (const pool_id in this.state.pool.stats)
            {
                if (!seen_pools[pool_id])
                {
                    txn.push({ requestDeleteRange: {
                        key: b64(this.config.etcd_prefix+'/pool/stats/'+pool_id),
                    } });
                    delete this.state.pool.stats[pool_id];
                }
                else
                {
                    const pool_stats = { ...this.state.pool.stats[pool_id] };
                    serialize_bigints(pool_stats);
                    txn.push({ requestPut: {
                        key: b64(this.config.etcd_prefix+'/pool/stats/'+pool_id),
                        value: b64(JSON.stringify(pool_stats)),
                    } });
                }
            }
        }
        if (txn.length)
        {
            await this.etcd.etcd_call('/kv/txn', { success: txn }, this.config.etcd_mon_timeout, 0);
        }
        if (!this.recheck_pgs_active &&
            backfillfull_pools.join(',') != ((this.state.pg.config||{}).backfillfull_pools||[]).join(','))
        {
            this.log_backfillfull(backfillfull_osds, backfillfull_pools);
            const new_pg_config = { ...this.state.pg.config, backfillfull_pools: backfillfull_pools.length ? backfillfull_pools : undefined };
            await this.save_pg_config(new_pg_config);
        }
    }

    log_backfillfull(osds, pools)
    {
        for (const osd in osds)
        {
            const bf = osds[osd];
            console.log('OSD '+osd+' may fill up during rebalance: capacity '+(bf.cap/1024n/1024n)+
                ' MB, target user data '+(bf.clean/1024n/1024n)+' MB');
        }
        console.log(
            (pools.length ? 'Pool(s) '+pools.join(', ') : 'No pools')+
            ' are backfillfull now, applying rebalance configuration'
        );
    }

    schedule_update_stats()
    {
        if (this.stopped || this.stats_timer)
        {
            return;
        }
        this.stats_timer = setTimeout(() =>
        {
            this.stats_timer = null;
            if (this.updating_total_stats)
            {
                this.schedule_update_stats();
                return;
            }
            this.updating_total_stats = true;
            try
            {
                this.update_total_stats().catch(console.error);
            }
            catch (e)
            {
                console.error(e);
            }
            this.updating_total_stats = false;
        }, this.config.mon_stats_timeout);
    }

    parse_kv(kv)
    {
        if (!kv || !kv.key)
        {
            return kv;
        }
        kv = { ...kv };
        kv.key = de64(kv.key);
        kv.value = kv.value ? de64(kv.value) : null;
        let key = kv.key.substr(this.config.etcd_prefix.length+1);
        if (!etcd_allow.exec(key))
        {
            console.log('Bad key in etcd: '+kv.key+' = '+kv.value);
            return kv;
        }
        try
        {
            kv.value = kv.value ? JSON.parse(kv.value) : null;
        }
        catch (e)
        {
            console.log('Bad value in etcd: '+kv.key+' = '+kv.value);
            return kv;
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
        if (kv.value == null)
        {
            delete cur[key_parts[key_parts.length-1]];
        }
        else
        {
            cur[key_parts[key_parts.length-1]] = kv.value;
        }
        if (key === 'config/global')
        {
            this.config = { ...this.fileConfig, ...this.state.config.global, ...this.cliConfig };
            this.check_config();
            this.etcd.parse_config(this.config);
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
                validate_pool_cfg(pool_id, pool_cfg, this.config.placement_levels, true);
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
        return kv;
    }

    _die(err)
    {
        // Stop this instance of Monitor so we can restart
        console.error(err instanceof Error ? err : new Error(err || 'Cluster connection failed'));
        this.on_stop().catch(console.error);
        this.on_die();
    }
}

function sha1hex(str)
{
    const hash = crypto.createHash('sha1');
    hash.update(str);
    return hash.digest('hex');
}

module.exports = Mon;
