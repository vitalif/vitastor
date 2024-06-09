// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

const fs = require('fs');

const AntiEtcd = require('antietcd');

const vitastor_persist_filter = require('./vitastor_persist_filter.js');
const { b64, local_ips } = require('./utils.js');

class AntiEtcdAdapter
{
    static async start_antietcd(config)
    {
        let antietcd;
        if (config.use_antietcd)
        {
            let fileConfig = {};
            if (fs.existsSync(config.config_path||'/etc/vitastor/vitastor.conf'))
            {
                fileConfig = JSON.parse(fs.readFileSync(config.config_path||'/etc/vitastor/vitastor.conf', { encoding: 'utf-8' }));
            }
            let mergedConfig = { ...fileConfig, ...config };
            let cluster = mergedConfig.etcd_address;
            if (!(cluster instanceof Array))
                cluster = cluster ? (''+(cluster||'')).split(/,+/) : [];
            cluster = Object.keys(cluster.reduce((a, url) =>
            {
                a[url.toLowerCase().replace(/^https?:\/\//, '').replace(/\/.*$/, '')] = true;
                return a;
            }, {}));
            const cfg_port = mergedConfig.antietcd_port;
            const is_local = local_ips(true).reduce((a, c) => { a[c] = true; return a; }, {});
            const selected = cluster.map(s => s.split(':', 2)).filter(ip => is_local[ip[0]] && (!cfg_port || ip[1] == cfg_port));
            if (selected.length > 1)
            {
                console.error('More than 1 etcd_address matches local IPs, please specify port');
                process.exit(1);
            }
            else if (selected.length == 1)
            {
                const antietcd_config = {
                    ip: selected[0][0],
                    port: selected[0][1],
                    data: mergedConfig.antietcd_data_file || ((mergedConfig.antietcd_data_dir || '/var/lib/vitastor') + '/mon_'+selected[0][1]+'.json.gz'),
                    persist_filter: vitastor_persist_filter(mergedConfig.etcd_prefix || '/vitastor'),
                    node_id: selected[0][0]+':'+selected[0][1], // node_id = ip:port
                    cluster: (cluster.length == 1 ? null : cluster),
                    cluster_key: (mergedConfig.etcd_prefix || '/vitastor'),
                    stale_read: 1,
                };
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
        if (!this.antietcd.raft)
        {
            console.log('Running in non-clustered mode');
        }
        else
        {
            console.log('Waiting to become master');
            await new Promise(ok => this.on_leader.push(ok));
        }
        const state = { ...this.mon.get_mon_state(), id: ''+this.mon.etcd_lease_id };
        await this.etcd_call('/kv/txn', {
            success: [ { requestPut: { key: b64(this.mon.config.etcd_prefix+'/mon/master'), value: b64(JSON.stringify(state)), lease: ''+this.mon.etcd_lease_id } } ],
        }, this.mon.config.etcd_start_timeout, 0);
        if (this.antietcd.raft)
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
                const res = await this.antietcd.api(path.replace(/^\/+/, '').replace(/\/+$/, '').replace(/\/+/g, '_'), body);
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
