// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

const http = require('http');
const WebSocket = require('ws');
const { b64, local_ips } = require('./utils.js');

const MON_STOPPED = 'Monitor instance is stopped';

class EtcdAdapter
{
    constructor(mon)
    {
        this.mon = mon;
        this.ws = null;
        this.ws_alive = false;
        this.ws_keepalive_timer = null;
    }

    parse_config(config)
    {
        this.parse_etcd_addresses(config.etcd_address||config.etcd_url);
    }

    parse_etcd_addresses(addrs)
    {
        const is_local_ip = local_ips(true).reduce((a, c) => { a[c] = true; return a; }, {});
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

    stop_watcher(cur_addr)
    {
        cur_addr = cur_addr || this.selected_etcd_url;
        if (this.ws)
        {
            console.log('Disconnected from etcd at '+this.ws_used_url);
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
    }

    restart_watcher(cur_addr)
    {
        this.stop_watcher(cur_addr);
        this.start_watcher(this.mon.config.etcd_mon_retries).catch(this.mon.die);
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
            if (tried[base] && now-tried[base] < this.mon.config.etcd_start_timeout)
            {
                await new Promise(ok => setTimeout(ok, this.mon.config.etcd_start_timeout-(now-tried[base])));
                now = Date.now();
            }
            tried[base] = now;
            if (this.mon.stopped)
            {
                return;
            }
            const ok = await new Promise(ok =>
            {
                const timer_id = setTimeout(() =>
                {
                    if (this.ws)
                    {
                        console.log('Disconnected from etcd at '+this.ws_used_url);
                        this.ws.close();
                        this.ws = null;
                    }
                    ok(false);
                }, this.mon.config.etcd_mon_timeout);
                this.ws = new WebSocket(base+'/watch');
                this.ws_used_url = cur_addr;
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
            this.mon.die('Failed to open etcd watch websocket');
            return;
        }
        if (this.mon.stopped)
        {
            this.stop_watcher();
            return;
        }
        const cur_addr = this.selected_etcd_url;
        this.ws_alive = true;
        this.ws_keepalive_timer = setInterval(() =>
        {
            if (this.ws_alive && this.ws)
            {
                this.ws_alive = false;
                this.ws.send(JSON.stringify({ progress_request: {} }));
            }
            else
            {
                console.log('etcd websocket timed out, restarting it');
                this.restart_watcher(cur_addr);
            }
        }, (Number(this.mon.config.etcd_ws_keepalive_interval) || 5)*1000);
        this.ws.on('error', () => this.restart_watcher(cur_addr));
        this.ws.send(JSON.stringify({
            create_request: {
                key: b64(this.mon.config.etcd_prefix+'/'),
                range_end: b64(this.mon.config.etcd_prefix+'0'),
                start_revision: ''+this.mon.etcd_watch_revision,
                watch_id: 1,
                progress_notify: true,
            },
        }));
        this.ws.on('message', (msg) =>
        {
            if (this.mon.stopped)
            {
                this.stop_watcher();
                return;
            }
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
                    this.mon.die('Revisions before '+data.result.compact_revision+' were compacted by etcd, exiting');
                }
                this.mon.die('Watch canceled by etcd, reason: '+data.result.cancel_reason+', exiting');
            }
            else if (data.result.created)
            {
                // etcd watch created
                console.log('Successfully subscribed to etcd at '+this.selected_etcd_url+', revision '+data.result.header.revision);
            }
            else
            {
                this.mon.on_message(data.result);
            }
        });
    }

    async become_master()
    {
        const state = { ...this.mon.get_mon_state(), id: ''+this.mon.etcd_lease_id };
        console.log('Waiting to become master');
        // eslint-disable-next-line no-constant-condition
        while (1)
        {
            const res = await this.etcd_call('/kv/txn', {
                compare: [ { target: 'CREATE', create_revision: 0, key: b64(this.mon.config.etcd_prefix+'/mon/master') } ],
                success: [ { requestPut: { key: b64(this.mon.config.etcd_prefix+'/mon/master'), value: b64(JSON.stringify(state)), lease: ''+this.mon.etcd_lease_id } } ],
            }, this.mon.config.etcd_start_timeout, 0);
            if (res.succeeded)
            {
                break;
            }
            await new Promise(ok => setTimeout(ok, this.mon.config.etcd_start_timeout));
        }
        console.log('Became master');
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
            if (this.mon.stopped)
            {
                throw new Error(MON_STOPPED);
            }
            const res = await POST(base+path, body, timeout);
            if (this.mon.stopped)
            {
                throw new Error(MON_STOPPED);
            }
            if (res.error)
            {
                if (this.selected_etcd_url == base)
                    this.selected_etcd_url = null;
                console.error('Failed to query etcd '+path+' (retry '+retry+'/'+retries+'): '+res.error);
                continue;
            }
            if (res.json)
            {
                if (res.json.error)
                {
                    console.error(path+': etcd returned error: '+res.json.error);
                    break;
                }
                return res.json;
            }
        }
        throw new Error('Failed to query etcd ('+retries+' retries)');
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

module.exports = EtcdAdapter;
