// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

const AntiEtcd = require('antietcd');

const vitastor_auth_filter = require('./vitastor_auth_filter.js');
const vitastor_persist_filter = require('./vitastor_persist_filter.js');
const { b64, local_ips } = require('./utils.js');

class AntiEtcdAdapter
{
    static async start_antietcd(config)
    {
        let antietcd;
        if (config.use_antietcd)
        {
            let cluster = config.etcd_address;
            if (!(cluster instanceof Array))
                cluster = cluster ? (''+(cluster||'')).split(/,+/) : [];
            cluster = Object.keys(cluster.reduce((a, url) =>
            {
                a[url.toLowerCase().replace(/^(https?:\/\/)?(.*?)(\/.*)?$/, (m, m1, m2) => (m1||'http://')+m2)] = true;
                return a;
            }, {}));
            const cfg_port = config.antietcd_port;
            const is_local = local_ips(true).reduce((a, c) => { a[c] = true; return a; }, {});
            is_local['0.0.0.0'] = true;
            is_local['::'] = true;
            is_local[''] = true;
            // split :, 3 -> <schema>:<//ip>:<port>
            const selected = [];
            for (let i = 0; i < cluster.length; i++)
            {
                const m = /^(https?:\/\/)?(?:\[(.*)\]|([^\[\:]+))(?::(\d+))?$/.exec(cluster[i]);
                if (!m)
                    continue;
                const ip = m[3] || m[2];
                const port = m[4] || 2379;
                if (is_local[ip] && (!cfg_port || port == cfg_port))
                    selected.push({ idx: i, ip, port });
            }
            if (selected.length > 1)
            {
                console.error('More than 1 etcd_address matches local IPs, please specify port');
                process.exit(1);
            }
            else if (selected.length == 1)
            {
                const antietcd_config = {
                    ip: selected[0].ip,
                    port: selected[0].port,
                    cert: config.antietcd_cert,
                    key: config.antietcd_key,
                    ca: config.antietcd_ca,
                    data: config.antietcd_data_file || ((config.antietcd_data_dir || '/var/lib/vitastor') + '/mon_'+selected[0].port+'.json.gz'),
                    persist_filter: vitastor_persist_filter({ vitastor_prefix: config.etcd_prefix || '/vitastor' }),
                    node_id: cluster[selected[0].idx].replace(/^(https?:\/\/)/, ''), // same as in <cluster> below
                    cluster: (cluster.length == 1 ? null : cluster.reduce((a, c) => { a[c.replace(/^(https?:\/\/)/, '')] = c; return a; }, {})),
                    cluster_key: (config.etcd_prefix || '/vitastor'),
                    stale_read: 1,
                    log_level: 1,
                    logs: { cluster: true },
                };
                if (config.etcd_proxy)
                {
                    // Monitor may use the builtin etcd_proxy mode
                    if (!config.etcd_proxy.urls)
                    {
                        console.error('etcd_proxy.urls are empty');
                        process.exit(1);
                    }
                    antietcd_config.etcd_proxy = config.etcd_proxy.urls;
                    antietcd_config.etcd_cert = config.etcd_proxy.cert;
                    antietcd_config.etcd_key = config.etcd_proxy.key;
                    antietcd_config.etcd_ca = config.etcd_proxy.ca;
                    delete antietcd_config.data;
                    delete antietcd_config.persist_filter;
                    delete antietcd_config.cluster;
                    delete antietcd_config.cluster_key;
                }
                if (config.use_auth)
                {
                    antietcd_config.client_cert_auth = true;
                    antietcd_config.auth_filter = vitastor_auth_filter;
                    antietcd_config.ca = config.client_ca;
                    antietcd_config.osd_ca = config.osd_ca;
                    antietcd_config.mon_ca = config.mon_ca;
                    if (!config.etcd_proxy)
                    {
                        antietcd_config.peer_ca = config.antietcd_server_ca;
                        if (!config.antietcd_server_ca || config.antietcd_server_ca == config.client_ca)
                        {
                            console.error('Secure setup requires separate antietcd_server_ca (for signing antietcd server certificates) and client_ca (for signing client certificates)');
                            process.exit(1);
                        }
                    }
                }
                for (const key in config)
                {
                    if (key.substr(0, 9) === 'antietcd_')
                    {
                        const noprefix = key.substr(9);
                        if (!(noprefix in antietcd_config) || noprefix == 'ip' || noprefix == 'cluster_key')
                        {
                            antietcd_config[noprefix] = config[key];
                        }
                    }
                }
                console.log('Starting Antietcd node '+antietcd_config.node_id);
                antietcd = new AntiEtcd(antietcd_config);
                await antietcd.start();
            }
            else
            {
                console.log('Antietcd is enabled, but etcd_address does not contain local IPs, proceeding without it');
            }
        }
        return antietcd;
    }

    constructor(mon, antietcd)
    {
        this.mon = mon;
        this.antietcd = antietcd;
        this.on_leader = [];
        this.on_change = (st) =>
        {
            if (st.state === 'leader')
            {
                for (const cb of this.on_leader)
                {
                    cb();
                }
                this.on_leader = [];
            }
        };
        this.antietcd.on('raftchange', this.on_change);
    }

    parse_config(/*config*/)
    {
    }

    stop_watcher()
    {
        this.antietcd.off('raftchange', this.on_change);
        const watch_id = this.watch_id;
        if (watch_id)
        {
            this.watch_id = null;
            this.antietcd.cancel_watch(watch_id).catch(console.error);
        }
    }

    async start_watcher()
    {
        if (this.watch_id)
        {
            await this.antietcd.cancel_watch(this.watch_id);
            this.watch_id = null;
        }
        const watch_id = await this.antietcd.create_watch({
            key: b64(this.mon.config.etcd_prefix+'/'),
            range_end: b64(this.mon.config.etcd_prefix+'0'),
            start_revision: ''+this.mon.etcd_watch_revision,
            watch_id: 1,
            progress_notify: true,
        }, (message) =>
        {
            setImmediate(() => this.mon.on_message(message.result));
        });
        console.log('Successfully subscribed to antietcd revision '+this.antietcd.etctree.mod_revision);
        this.watch_id = watch_id;
    }

    async become_master()
    {
        if (!this.antietcd.cluster)
        {
            console.log('Running in non-clustered mode');
        }
        else
        {
            console.log('Waiting to become master');
            if (this.antietcd.cluster.raft.state !== 'leader')
            {
                await new Promise(ok => this.on_leader.push(ok));
            }
        }
        const state = { ...this.mon.get_mon_state(), id: ''+this.mon.etcd_lease_id };
        await this.etcd_call('/kv/txn', {
            success: [ { requestPut: { key: b64(this.mon.config.etcd_prefix+'/mon/master'), value: b64(JSON.stringify(state)), lease: ''+this.mon.etcd_lease_id } } ],
        }, this.mon.config.etcd_start_timeout, 0);
        if (this.antietcd.cluster)
        {
            console.log('Became master');
        }
    }

    async etcd_call(path, body, timeout, retries)
    {
        let retry = 0;
        if (retries >= 0 && retries < 1)
        {
            retries = 1;
        }
        let prev = 0;
        while (retries < 0 || retry < retries)
        {
            retry++;
            if (this.mon.stopped)
            {
                throw new Error('Monitor instance is stopped');
            }
            try
            {
                if (Date.now()-prev < timeout)
                {
                    await new Promise(ok => setTimeout(ok, timeout-(Date.now()-prev)));
                }
                prev = Date.now();
                const res = await this.antietcd.api(path.replace(/^\/+/, '').replace(/\/+$/, '').replace(/\/+/g, '_'), body, { user_type: 'mon' });
                if (res.error)
                {
                    console.error('Failed to query antietcd '+path+' (retry '+retry+'/'+retries+'): '+res.error);
                }
                else
                {
                    return res;
                }
            }
            catch (e)
            {
                console.error('Failed to query antietcd '+path+' (retry '+retry+'/'+retries+'): '+e.stack);
            }
        }
        throw new Error('Failed to query antietcd ('+retries+' retries)');
    }
}

module.exports = AntiEtcdAdapter;
