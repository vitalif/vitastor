const http = require('http');
const os = require('os');
const WebSocket = require('ws');
const LPOptimizer = require('./lp-optimizer.js');
const stableStringify = require('./stable-stringify.js');

class Mon
{
    static etcd_tree = {
        config: {
            global: null,
            /* placement_tree = {
                levels: { datacenter: 1, rack: 2, host: 3, osd: 4, ... },
                nodes: { host1: { level: 'host', parent: 'rack1' }, ... },
                failure_domain: 'host',
            } */
            placement_tree: null,
            osd: {},
            pgs: {},
        },
        osd: {
            state: {},
            stats: {},
        },
        mon: {
            master: null,
        },
        pg: {
            change_stamp: null,
            state: {},
            stats: {},
            history: {},
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
        this.etcd_prefix = config.etcd_prefix || '/rage';
        this.etcd_prefix = this.etcd_prefix.replace(/\/\/+/g, '/').replace(/^\/?(.*[^\/])\/?$/, '/$1');
        this.etcd_start_timeout = (config.etcd_start_timeout || 5) * 1000;
        this.state = JSON.parse(JSON.stringify(Mon.etcd_tree));
    }

    async start()
    {
        await this.load_config();
        await this.get_lease();
        await this.become_master();
        await this.load_cluster_state();
        await this.start_watcher();
        await this.recheck_pgs();
    }

    async load_config()
    {
        const res = await this.etcd_call('/txn', { success: [
            { requestRange: { key: b64(this.etcd_prefix+'/config/global') } }
        ] }, this.etcd_start_timeout, -1);
        this.parse_kv(res.responses[0].response_range.kvs[0]);
        this.check_config();
    }

    check_config()
    {
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
        this.config.max_osd_combinations = Number(this.config.max_osd_combinations) || 10000;
        if (this.config.max_osd_combinations < 100)
        {
            this.config.max_osd_combinations = 100;
        }
    }

    async start_watcher(retries)
    {
        let retry = 0;
        if (retries >= 0 && retries < 1)
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
                }, timeout);
                this.ws = new WebSocket(base+'/watch');
                this.ws.on('open', () =>
                {
                    if (timer_id)
                        clearTimeout(timer_id);
                    ok(true);
                });
            });
            if (!ok)
            {
                this.ws = null;
            }
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
                console.error('Garbage received from watch websocket: '+msg);
            }
            else
            {
                let stats_changed = false, changed = false;
                console.log('Revision '+data.result.header.revision+' events: ');
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
                    console.log(e);
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
        }, config.etcd_mon_timeout);
    }

    async become_master()
    {
        const state = { ip: this.local_ips() };
        while (1)
        {
            const res = await this.etcd_call('/txn', {
                compare: [ { target: 'CREATE', create_revision: 0, key: b64(this.etcd_prefix+'/mon/master') } ],
                success: [ { key: b64(this.etcd_prefix+'/mon/master'), value: b64(JSON.stringify(state)), lease: ''+this.etcd_lease_id } ],
            }, this.etcd_start_timeout, 0);
            if (!res.succeeded)
            {
                await new Promise(ok => setTimeout(ok, this.etcd_start_timeout));
            }
        }
    }

    async load_cluster_state()
    {
        const res = await this.etcd_call('/txn', { success: [
            { requestRange: { key: b64(this.etcd_prefix+'/'), range_end: b64(this.etcd_prefix+'0') } },
        ] }, this.etcd_start_timeout, -1);
        this.etcd_watch_revision = BigInt(res.header.revision)+BigInt(1);
        const data = JSON.parse(JSON.stringify(Mon.etcd_tree));
        for (const response of res.responses)
        {
            for (const kv of response.response_range.kvs)
            {
                this.parse_kv(kv);
            }
        }
        this.state = data;
    }

    all_osds()
    {
        return Object.keys(this.state.osd.stats);
    }

    get_osd_tree()
    {
        this.state.config.placement_tree = this.state.config.placement_tree||{};
        const levels = this.state.config.placement_tree.levels||{};
        levels.host = levels.host || 100;
        levels.osd = levels.osd || 101;
        const tree = { '': { children: [] } };
        for (const node_id in this.state.config.placement_tree.nodes||{})
        {
            const node_cfg = this.state.config.placement_tree.nodes[node_id];
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
        return LPOptimizer.flatten_tree(tree[''].children, levels, this.state.config.failure_domain, 'osd');
    }

    async stop_all_pgs()
    {
        let has_online = false, paused = true;
        for (const pg in this.state.config.pgs.items||{})
        {
            const cur_state = ((this.state.pg.state[pg]||{}).state||[]).join(',');
            if (cur_state != '' && cur_state != 'offline')
            {
                has_online = true;
            }
            if (!this.state.config.pgs.items[pg].pause)
            {
                paused = false;
            }
        }
        if (!paused)
        {
            console.log('Stopping all PGs before changing PG count');
            const new_cfg = JSON.parse(JSON.stringify(this.state.config.pgs));
            for (const pg in new_cfg.items)
            {
                new_cfg.items[pg].pause = true;
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
            const res = await this.etcd_call('/txn', {
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

    scale_pg_count(prev_pgs, pg_history, new_pg_count)
    {
        const old_pg_count = prev_pgs.length;
        // Add all possibly intersecting PGs into the history of new PGs
        if (!(new_pg_count % old_pg_count))
        {
            // New PG count is a multiple of the old PG count
            const mul = (new_pg_count / old_pg_count);
            for (let i = 0; i < new_pg_count; i++)
            {
                const old_i = Math.floor(new_pg_count / mul);
                pg_history[i] = JSON.parse(JSON.stringify(this.state.pg.history[1+old_i]));
            }
        }
        else if (!(old_pg_count % new_pg_count))
        {
            // Old PG count is a multiple of the new PG count
            const mul = (old_pg_count / new_pg_count);
            for (let i = 0; i < new_pg_count; i++)
            {
                pg_history[i] = {
                    osd_sets: [],
                    all_peers: [],
                    epoch: 0,
                };
                for (let j = 0; j < mul; j++)
                {
                    pg_history[i].osd_sets.push(prev_pgs[i*mul]);
                    const hist = this.state.pg.history[1+i*mul+j];
                    if (hist && hist.osd_sets && hist.osd_sets.length)
                    {
                        Array.prototype.push.apply(pg_history[i].osd_sets, hist.osd_sets);
                    }
                    if (hist && hist.all_peers && hist.all_peers.length)
                    {
                        Array.prototype.push.apply(pg_history[i].all_peers, hist.all_peers);
                    }
                    if (hist && hist.epoch)
                    {
                        pg_history[i].epoch = pg_history[i].epoch < hist.epoch ? hist.epoch : pg_history[i].epoch;
                    }
                }
            }
        }
        else
        {
            // Any PG may intersect with any PG after non-multiple PG count change
            // So, merge ALL PGs history
            let all_sets = {};
            let all_peers = {};
            let max_epoch = 0;
            for (const pg of prev_pgs)
            {
                all_sets[pg.join(' ')] = pg;
            }
            for (const pg in this.state.pg.history)
            {
                const hist = this.state.pg.history[pg];
                if (hist && hist.osd_sets)
                {
                    for (const pg of hist.osd_sets)
                    {
                        all_sets[pg.join(' ')] = pg;
                    }
                }
                if (hist && hist.all_peers)
                {
                    for (const osd_num of hist.all_peers)
                    {
                        all_peers[osd_num] = Number(osd_num);
                    }
                }
                if (hist && hist.epoch)
                {
                    max_epoch = max_epoch < hist.epoch ? hist.epoch : max_epoch;
                }
            }
            all_sets = Object.values(all_sets);
            all_peers = Object.values(all_peers);
            for (let i = 0; i < new_pg_count; i++)
            {
                pg_history[i] = { osd_sets: all_sets, all_peers, epoch: max_epoch };
            }
        }
        // Mark history keys for removed PGs as removed
        for (let i = new_pg_count; i < old_pg_count; i++)
        {
            pg_history[i] = null;
        }
        if (old_pg_count < new_pg_count)
        {
            for (let i = new_pg_count-1; i >= 0; i--)
            {
                prev_pgs[i] = prev_pgs[Math.floor(i/new_pg_count*old_pg_count)];
            }
        }
        else if (old_pg_count > new_pg_count)
        {
            for (let i = 0; i < new_pg_count; i++)
            {
                prev_pgs[i] = prev_pgs[Math.round(i/new_pg_count*old_pg_count)];
            }
            prev_pgs.splice(new_pg_count, old_pg_count-new_pg_count);
        }
    }

    async save_new_pgs(prev_pgs, new_pgs, pg_history, tree_hash)
    {
        const txn = [], checks = [];
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
            checks.push({
                key: b64(this.etcd_prefix+'/pg/history/'+(i+1)),
                target: 'MOD',
                mod_revision: ''+this.etcd_watch_revision,
                result: 'LESS',
            });
            if (pg_history[i])
            {
                txn.push({
                    requestPut: {
                        key: b64(this.etcd_prefix+'/pg/history/'+(i+1)),
                        value: b64(JSON.stringify(pg_history[i])),
                    },
                });
            }
            else
            {
                txn.push({
                    requestDeleteRange: {
                        key: b64(this.etcd_prefix+'/pg/history/'+(i+1)),
                    },
                });
            }
        }
        this.state.config.pgs = {
            hash: tree_hash,
            items: pg_items,
        };
        const res = await this.etcd_call('/txn', {
            compare: [
                { key: b64(this.etcd_prefix+'/mon/master'), target: 'LEASE', lease: ''+this.etcd_lease_id },
                { key: b64(this.etcd_prefix+'/config/pgs'), target: 'MOD', mod_revision: ''+this.etcd_watch_revision, result: 'LESS' },
                ...checks,
            ],
            success: [
                { requestPut: { key: b64(this.etcd_prefix+'/config/pgs'), value: b64(JSON.stringify(this.state.config.pgs)) } },
                ...txn,
            ],
        }, this.config.etcd_mon_timeout, 0);
        return res.succeeded;
    }

    async recheck_pgs()
    {
        // Take configuration and state, check it against the stored configuration hash
        // Recalculate PGs and save them to etcd if the configuration is changed
        const tree_cfg = {
            osd_tree: this.get_osd_tree(),
            pg_count: this.config.pg_count || Object.keys(this.state.config.pgs.items||{}).length || 128,
            max_osd_combinations: this.config.max_osd_combinations,
        };
        const tree_hash = sha1hex(stableStringify(tree_cfg));
        if (this.state.config.pgs.hash != tree_hash)
        {
            // Something has changed
            const prev_pgs = [];
            for (const pg in this.state.config.pgs.items||{})
            {
                prev_pgs[pg-1] = this.state.config.pgs.items[pg].osd_set;
            }
            const pg_history = [];
            const old_pg_count = prev_pgs.length;
            let optimize_result;
            if (old_pg_count > 0)
            {
                if (old_pg_count != tree_cfg.pg_count)
                {
                    // PG count changed. Need to bring all PGs down.
                    if (!await this.stop_all_pgs())
                    {
                        this.schedule_recheck();
                        return;
                    }
                    this.scale_pg_count(prev_pgs, pg_history, new_pg_count);
                }
                optimize_result = await LPOptimizer.optimize_change(prev_pgs, tree_cfg.osd_tree, 3, tree_cfg.max_osd_combinations);
            }
            else
            {
                optimize_result = await LPOptimizer.optimize_initial(tree_cfg.osd_tree, 3, tree_cfg.pg_count, tree_cfg.max_osd_combinations);
            }
            if (!await this.save_new_pgs(prev_pgs, optimize_result.int_pgs, pg_history, tree_hash))
            {
                console.log('Someone changed PG configuration while we also tried to change it. Retrying in '+this.config.mon_change_timeout+' ms');
                this.schedule_recheck();
                return;
            }
            console.log('PG configuration successfully changed');
            if (old_pg_count != optimize_result.int_pgs.length)
            {
                console.log(`PG count changed from: ${old_pg_count} to ${optimize_result.int_pgs.length}`);
            }
            LPOptimizer.print_change_stats(optimize_result);
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
        for (const pg_num in this.state.pg.stats)
        {
            const st = this.state.pg.stats[pg_num];
            for (const k in object_counts)
            {
                if (st[k+'_count'])
                {
                    object_counts[k] += BigInt(st[k+'_count']);
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
            await this.etcd_call('/txn', {
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
        kv.value = kv.value ? JSON.parse(de64(kv.value)) : null;
        const key = kv.key.substr(this.etcd_prefix.length).replace(/^\/+/, '').split('/');
        const cur = this.state, orig = Mon.etcd_tree;
        for (let i = 0; i < key.length-1; i++)
        {
            if (!orig[key[i]])
            {
                console.log('Bad key in etcd: '+kv.key+' = '+kv.value);
                return;
            }
            orig = orig[key[i]];
            cur = (cur[key[i]] = cur[key[i]] || {});
        }
        if (orig[key.length-1])
        {
            console.log('Bad key in etcd: '+kv.key+' = '+kv.value);
            return;
        }
        cur[key[key.length-1]] = kv.value;
        if (key.join('/') === 'config/global')
        {
            this.state.config.global = this.state.config.global || {};
            this.config = this.state.config.global;
            this.check_config();
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
            if (res.json)
            {
                if (res.json.error)
                {
                    console.log('etcd returned error: '+res.json.error);
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
        console.fatal(err || 'Cluster connection failed');
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
            'Content-Length': body_text,
        } }, (res) =>
        {
            if (!req)
            {
                return;
            }
            clearTimeout(timer_id);
            if (res.statusCode != 200)
            {
                ok({ error: res.statusCode, response: res });
                return;
            }
            let res_body = '';
            res.setEncoding('utf8');
            res.on('data', chunk => { res_body += chunk });
            res.on('end', () =>
            {
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
