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
        if (!/^\/+/.exec(this.etcd_prefix))
        {
            this.etcd_prefix = '/' + this.etcd_prefix;
        }
        this.etcd_start_timeout = (config.etcd_start_timeout || 5) * 1000;
        this.data = JSON.parse(JSON.stringify(Mon.etcd_tree));
    }

    async start()
    {
        await this.load_cluster_state();
        return;
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
        this.config = this.parse_kv(res.responses[0].response_range.kvs[0]).value || {};
        if (!(this.config instanceof Object) || this.config instanceof Array)
        {
            throw new Error(this.etcd_prefix+'/config/global is not a JSON object');
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
                start_revision: this.etcd_watch_revision,
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
                console.log('Revision '+data.result.header.revision+' events: ');
                for (const e of data.result.events)
                {
                    this.parse_kv(e.kv);
                    console.log(e);
                }
                if (this.changeTimer)
                {
                    clearTimeout(this.changeTimer);
                    this.changeTimer = null;
                }
                this.changeTimer = setTimeout(() =>
                {
                    this.changeTimer = null;
                    this.recheck_pgs().catch(console.error);
                }, this.config.mon_change_timeout || 1000);
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
        this.data = data;
        this.data.config.placement_tree = this.data.config.placement_tree || {};
    }

    get_osd_tree()
    {
        const levels = this.data.config.placement_tree.levels || {};
        levels.host = levels.host || 100;
        levels.osd = levels.osd || 101;
        const tree = { '': { children: [] } };
        for (const node_id in this.data.config.placement_tree)
        {
            const node_cfg = this.data.config.placement_tree[node_id];
            if (!node_id || /^\d/.exec(node_id) ||
                !node_cfg.level || !levels[node_cfg.level])
            {
                // All nodes must have non-empty non-numeric IDs and valid levels
                continue;
            }
            tree[node_id] = { id: node_id, level: node_cfg.level, parent: node_cfg.parent, children: [] };
        }
        const down_time = Date.now()/1000 - this.config.osd_out_time;
        for (const osd_num of Object.keys(this.data.osd.stats).sort((a, b) => a - b))
        {
            const stat = this.data.osd.stats[osd_num];
            if (stat.size && (this.data.osd.state[osd_num] || Number(stat.time) >= down_time))
            {
                // Numeric IDs are reserved for OSDs
                tree[osd_num] = tree[osd_num] || { id: osd_num, parent: stat.host };
                tree[osd_num].level = 'osd';
                tree[osd_num].size = stat.size / 1024 / 1024 / 1024 / 1024; // terabytes
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
        return LPOptimizer.flatten_tree(tree[''].children, levels, this.data.config.failure_domain, 'osd');
    }

    async recheck_pgs()
    {
        // Take configuration and state, check it against the stored configuration hash
        // Recalculate PGs and save them to etcd if the configuration is changed
        const tree_cfg = {
            osd_tree: this.get_osd_tree(),
            pg_count: this.data.config.global.pg_count || Object.keys(this.data.config.pgs.items||{}).length || 128,
            max_osd_combinations: this.data.config.global.max_osd_combinations,
        };
        const tree_hash = sha1hex(stableStringify(tree_cfg));
        if (this.data.config.pgs.hash != tree_hash)
        {
            // Something has changed
            const pg_history = [];
            const prev_pgs = [];
            for (const pg in this.data.config.pgs.items||{})
            {
                prev_pgs[pg-1] = this.data.config.pgs.items[pg].osd_set;
            }
            let pgs;
            if (prev_pgs.length > 0)
            {
                if (prev_pgs.length != tree_cfg.pg_count)
                {
                    // PG count changed. Need to bring all PGs down.
                    for (const pg in this.data.config.pgs.items)
                    {
                        const cur_state = ((this.data.pg.state[pg]||{}).state||[]).join(',');
                        if (cur_state != '' && cur_state != 'offline')
                        {
                            await this.stop_all_pgs();
                            return;
                        }
                    }
                    all_osds = Object.keys(all_osds);
                    // ...and add all possibly intersecting PGs into the history of new PGs
                    if (!(tree_cfg.pg_count % prev_pgs.length))
                    {
                        // New PG count is a multiple of the old PG count
                        const mul = (tree_cfg.pg_count / prev_pgs.length);
                        for (let i = 0; i < tree_cfg.pg_count; i++)
                        {
                            const old_i = Math.floor(tree_cfg.pg_count / mul);
                            pg_history[i] = JSON.parse(JSON.stringify(this.data.pg.history[1+old_i]));
                        }
                    }
                    else if (!(prev_pgs.length % tree_cfg.pg_count))
                    {
                        // Old PG count is a multiple of the new PG count
                        const mul = (prev_pgs.length / tree_cfg.pg_count);
                        for (let i = 0; i < tree_cfg.pg_count; i++)
                        {
                            pg_history[i] = {
                                osd_sets: [],
                                all_peers: [],
                            };
                            for (let j = 0; j < mul; j++)
                            {
                                pg_history[i].osd_sets.push(prev_pgs[i*mul]);
                                const hist = this.data.pg.history[1+i*mul+j];
                                if (hist && hist.osd_sets && hist.osd_sets.length)
                                {
                                    Array.prototype.push.apply(pg_history[i].osd_sets, hist.osd_sets);
                                }
                                if (hist && hist.all_peers && hist.all_peers.length)
                                {
                                    Array.prototype.push.apply(pg_history[i].all_peers, hist.all_peers);
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
                        for (const pg of prev_pgs)
                        {
                            all_sets[pg.join(' ')] = pg;
                        }
                        for (const pg in this.data.pg.history)
                        {
                            const hist = this.data.pg.history[pg];
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
                        }
                        all_sets = Object.values(all_sets);
                        all_peers = Object.values(all_peers);
                        for (let i = 0; i < tree_cfg.pg_count; i++)
                        {
                            pg_history[i] = { osd_sets: all_sets, all_peers };
                        }
                    }
                    // Mark history keys for removed PGs as removed
                    for (let i = tree_cfg.pg_count; i < prev_pgs.length; i++)
                    {
                        pg_history[i] = null;
                    }
                }
                if (prev_pgs.length < tree_cfg.pg_count)
                {
                    for (let i = tree_cfg.pg_count-1; i >= 0; i--)
                    {
                        prev_pgs[i] = prev_pgs[Math.floor(i/tree_cfg.pg_count*prev_pgs.length)];
                    }
                }
                else if (prev_pgs.length > tree_cfg.pg_count)
                {
                    for (let i = 0; i < tree_cfg.pg_count; i++)
                    {
                        prev_pgs[i] = prev_pgs[Math.round(i/tree_cfg.pg_count*prev_pgs.length)];
                    }
                    prev_pgs.splice(tree_cfg.pg_count, prev_pgs.length-tree_cfg.pg_count);
                }
                pgs = LPOptimizer.optimize_change(prev_pgs, tree_cfg.osd_tree, tree_cfg.max_osd_combinations);
            }
            else
            {
                pgs = LPOptimizer.optimize_initial(tree_cfg.osd_tree, tree_cfg.pg_count, tree_cfg.max_osd_combinations);
            }
            // FIXME: Handle insufficient failure domain count
            const txn = [];
            const pg_items = {};
            pgs.map((osd_set, i) =>
            {
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
            for (let i = 0; i < tree_cfg.pg_count || i < prev_pgs.length; i++)
            {
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
            this.data.config.pgs = {
                hash: tree_hash,
                count: tree_cfg.pg_count,
                items: pg_items,
            };
            const res = await this.etcd_call('/txn', {
                compare: [
                    { key: b64(this.etcd_prefix+'/mon/master'), target: 'LEASE', lease: ''+this.etcd_lease_id },
                    { key: b64(this.etcd_prefix+'/config/pgs'), target: 'MOD', mod_revision: this.etcd_watch_revision, result: 'LESS' },
                    { key: b64(this.etcd_prefix+'/pg/change_stamp'), target: 'MOD', mod_revision: this.etcd_watch_revision, result: 'LESS' },
                ],
                success: [
                    { requestPut: { key: b64(this.etcd_prefix+'/config/pgs'), value: b64(JSON.stringify(this.data.config.pgs)) } },
                    ...txn,
                ],
            }, this.config.etcd_mon_timeout, 0);
            
        }
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
        const cur = this.data, orig = Mon.etcd_tree;
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
            this.data.config.global = this.data.config.global || {};
            this.data.config.global.max_osd_combinations = Number(this.data.config.global.max_osd_combinations) || 10000;
            if (this.data.config.global.max_osd_combinations < 100)
            {
                this.data.config.global.max_osd_combinations = 100;
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
