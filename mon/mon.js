// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

const fs = require('fs');
const crypto = require('crypto');
const os = require('os');
const EtcdAdapter = require('./etcd_adapter.js');
const { RuleCombinator } = require('./lp_optimizer/dsl_pgs.js');
const { SimpleCombinator, flatten_tree } = require('./lp_optimizer/simple_pgs.js');
const { etcd_tree, etcd_allow, etcd_nonempty_keys } = require('./etcd_schema.js');
const { validate_pool_cfg, get_pg_rules } = require('./pool_config.js');
const { sum_op_stats, sum_object_counts, sum_inode_stats, serialize_bigints } = require('./stats.js');
const LPOptimizer = require('./lp_optimizer/lp_optimizer.js');
const stableStringify = require('./stable-stringify.js');
const { scale_pg_count, scale_pg_history } = require('./pg_utils.js');
const { get_osd_tree, make_hier_tree, filter_osds_by_root_node,
    filter_osds_by_tags, filter_osds_by_block_layout, get_affinity_osds } = require('./osd_tree.js');

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
        this.verbose = config.verbose || 0;
        this.initConfig = config;
        this.config = { ...config };
        this.etcd_prefix = config.etcd_prefix || '/vitastor';
        this.etcd_prefix = this.etcd_prefix.replace(/\/\/+/g, '/').replace(/^\/?(.*[^\/])\/?$/, '/$1');
        this.etcd_start_timeout = (config.etcd_start_timeout || 5) * 1000;
        this.state = JSON.parse(JSON.stringify(etcd_tree));
        this.prev_stats = { osd_stats: {}, osd_diff: {} };
        this.signals_set = false;
        this.on_stop_cb = () => this.on_stop(0).catch(console.error);
        this.recheck_pgs_active = false;
        this.etcd = new EtcdAdapter(this);
        this.etcd.parse_config(this.config);
    }

    async start()
    {
        await this.load_config();
        await this.get_lease();
        await this.etcd.become_master();
        await this.load_cluster_state();
        await this.etcd.start_watcher(this.config.etcd_mon_retries);
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
        const res = await this.etcd.etcd_call('/kv/txn', { success: [
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

    on_message(msg)
    {
        let stats_changed = false, changed = false, pg_states_changed = false;
        if (this.verbose)
        {
            console.log('Revision '+msg.header.revision+' events: ');
        }
        this.etcd_watch_revision = BigInt(msg.header.revision)+BigInt(1);
        for (const e of msg.events||[])
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
            new_clean_pgs.items[pool_id] = this.state.config.pgs.items[pool_id];
        }
        this.state.history.last_clean_pgs = new_clean_pgs;
        await this.etcd.etcd_call('/kv/txn', {
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
        let res = await this.etcd.etcd_call('/lease/grant', { TTL: max_ttl }, this.config.etcd_mon_timeout, -1);
        this.etcd_lease_id = res.ID;
        // Register in /mon/member, just for the information
        const state = this.get_mon_state();
        res = await this.etcd.etcd_call('/kv/put', {
            key: b64(this.etcd_prefix+'/mon/member/'+this.etcd_lease_id),
            value: b64(JSON.stringify(state)),
            lease: ''+this.etcd_lease_id
        }, this.etcd_start_timeout, 0);
        // Set refresh timer
        this.lease_timer = setInterval(async () =>
        {
            const res = await this.etcd.etcd_call('/lease/keepalive', { ID: this.etcd_lease_id }, this.config.etcd_mon_timeout, this.config.etcd_mon_retries);
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
        await this.etcd.etcd_call('/lease/revoke', { ID: this.etcd_lease_id }, this.config.etcd_mon_timeout, this.config.etcd_mon_retries);
        process.exit(status);
    }

    async load_cluster_state()
    {
        const res = await this.etcd.etcd_call('/kv/txn', { success: [
            { requestRange: { key: b64(this.etcd_prefix+'/'), range_end: b64(this.etcd_prefix+'0') } },
        ] }, this.etcd_start_timeout, -1);
        this.etcd_watch_revision = BigInt(res.header.revision)+BigInt(1);
        this.state = JSON.parse(JSON.stringify(etcd_tree));
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
            await this.etcd.etcd_call('/kv/txn', {
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
        const aff_osds = get_affinity_osds(this.state.config.pools[pool_id] || {}, up_osds, osd_tree);
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

    async generate_pool_pgs(pool_id, osd_tree, levels)
    {
        const pool_cfg = this.state.config.pools[pool_id];
        if (!validate_pool_cfg(pool_id, pool_cfg, this.config.placement_levels, false))
        {
            return null;
        }
        let pool_tree = { ...osd_tree };
        filter_osds_by_root_node(this.config, pool_tree, pool_cfg.root_node);
        filter_osds_by_tags(pool_tree, pool_cfg.osd_tags);
        filter_osds_by_block_layout(
            pool_tree,
            this.state.osd.stats,
            pool_cfg.block_size || this.config.block_size || 131072,
            pool_cfg.bitmap_granularity || this.config.bitmap_granularity || 4096,
            pool_cfg.immediate_commit || this.config.immediate_commit || 'none'
        );
        pool_tree = make_hier_tree(this.config, pool_tree);
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
                ? new RuleCombinator(pool_tree, get_pg_rules(pool_id, pool_cfg, this.config.placement_levels), pool_cfg.max_osd_combinations)
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
                scale_pg_count(prev_pgs, pool_cfg.pg_count);
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
        const { up_osds, levels, osd_tree } = get_osd_tree(this.config, this.state);
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
            let new_config_pgs;
            let changed = false;
            for (const pool_id in this.state.config.pools)
            {
                const pool_cfg = this.state.config.pools[pool_id];
                if (!validate_pool_cfg(pool_id, pool_cfg, this.config.placement_levels, false))
                {
                    continue;
                }
                const aff_osds = get_affinity_osds(pool_cfg, up_osds, osd_tree);
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
                pg_history = scale_pg_history(pg_history, real_prev_pgs, pool_res.pgs);
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
        const txn_res = await this.etcd.etcd_call('/kv/txn', etcd_request, this.config.etcd_mon_timeout, 0);
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

    async update_total_stats()
    {
        const txn = [];
        const { object_counts, object_bytes } = sum_object_counts(this.state, this.config);
        let stats = sum_op_stats(this.state.osd, this.prev_stats);
        let { inode_stats, seen_pools } = sum_inode_stats(this.state, this.prev_stats);
        stats.object_counts = object_counts;
        stats.object_bytes = object_bytes;
        stats = serialize_bigints(stats);
        inode_stats = serialize_bigints(inode_stats);
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
                serialize_bigints(pool_stats);
                txn.push({ requestPut: {
                    key: b64(this.etcd_prefix+'/pool/stats/'+pool_id),
                    value: b64(JSON.stringify(pool_stats)),
                } });
            }
        }
        if (txn.length)
        {
            await this.etcd.etcd_call('/kv/txn', { success: txn }, this.config.etcd_mon_timeout, 0);
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
        if (!etcd_allow.exec(key))
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
