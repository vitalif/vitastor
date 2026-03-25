// AntiEtcd authentication filter for Vitastor
// (c) Vitaliy Filippov, 2026
// License: Mozilla Public License 2.0 or Vitastor Network Public License 1.1

// Permissions are based on:
// 1. Users.
//    Stored in /vitastor/config/user/<username>.
//    Has 2 properties:
//    - type, one of: osd, mon, admin, client.
//      osd, mon types should be used by OSDs/monitors.
//      admin should be used for administrative access from vitastor-cli.
//      client should be used for regular clients.
//    - groups, a list of group names the user is included in.
// 2. Images.
//    Stored in /vitastor/config/inode/<pool>/<inode>. Has the following properties:
//    - owner (user name)
//    - owner_group (group name)
//    - reader_group

const static_perms = {
    invalid: {
        keys: {},
        prefixes: {},
    },
    osd: {
        keys: { '/pg/config': false },
        prefixes: { '/osd/': true, '/pg/state/': true, '/pg/history/': true, '/pgstats/': true },
    },
    mon: {
        keys: { '/pg/config': true, '/stats': true, '/history/last_clean_pgs': true },
        prefixes: {
            '/config/': false, '/osd/': false, '/mon/': true, '/pg/history/': true,
            '/pgstats/': false, '/inode/stats/': true, '/pool/stats/': true,
        },
    },
    admin: {
        keys: { '/stats': false },
        prefixes: {
            '/config/': true, '/osd/': true, '/index/': true, '/pg/history/': true,
            '/mon/': false, '/pg/': false, '/pgstats/': false, '/inode/stats/': false, '/pool/stats/': false,
        },
    },
    client: {
        keys: { '/config/global': false, '/config/node_placement': false, '/config/pools': false, '/pg/config': false },
        prefixes: { '/osd/stats/': false, '/pg/state/': false, '/index/maxid/': false },
    },
};

const api_perms = {
    osd: { lease_grant: true, lease_revoke: true, lease_keepalive: true },
    mon: { lease_grant: true, lease_revoke: true, lease_keepalive: true },
    admin: { maintenance_status: true },
    client: {},
};

class VitastorAuthFilter
{
    constructor(antietcd)
    {
        this.cfg = antietcd.cfg;
        this.antietcd = antietcd;
        this.prefix = this.cfg.vitastor_prefix || '/vitastor';
        this.prefix_parts = this.prefix.split('/');
    }

    _get(path, decode)
    {
        let cur = this.antietcd.etctree.state;
        path = path instanceof Array ? path : path.split('/');
        for (const p of path)
        {
            if (!cur.children)
            {
                return null;
            }
            cur = cur.children[p];
            if (!cur)
            {
                return null;
            }
        }
        if (decode)
        {
            return this._decode(path, cur.value);
        }
        return cur;
    }

    _decode(path, cur)
    {
        if (!cur)
        {
            return null;
        }
        if (cur)
        {
            try
            {
                cur = JSON.parse(cur);
            }
            catch (e)
            {
                console.warn('Invalid JSON in '+(path instanceof Array ? path.join('/') : path)+': '+e);
            }
        }
        return cur;
    }

    // userInfo: { name: string, type: string, perms: static_perms[type], groups: { [string]: true } }
    _check_compare(check, userInfo, checked)
    {
        let key = String(check.key);
        if (key.substr(0, this.prefix.length) !== this.prefix)
        {
            return false;
        }
        key = key.substr(this.prefix.length);
        if (key in userInfo.perms.keys)
        {
            return true;
        }
        for (const pfx in userInfo.perms.prefixes)
        {
            if (key.substr(0, pfx.length) == pfx)
            {
                return true;
            }
        }
        if (userInfo.type == 'client')
        {
            // Image permissions
            if (key.substr(0, 14) == '/config/inode/')
            {
                // Allowed to check that a key does not exist
                if (check.target == 'VERSION' && check.version == 0)
                {
                    checked['M'+key] = true;
                    return true;
                }
                else if (check.target == 'MOD')
                {
                    const data = this._get(check.key);
                    if (!data || data.mod_revision != check.mod_revision)
                    {
                        // Break check to trigger CAS failure
                        check.mod_revision = '18446744073709551615'; // UINT64_MAX
                        return true;
                    }
                    const inode = this._decode(check.key, data.value);
                    if (inode && (inode.owner_group && userInfo.groups[inode.owner_group] ||
                        inode.owner === userInfo.name))
                    {
                        checked['M'+key] = true;
                        return true;
                    }
                }
                return false;
            }
            if (key.substr(0, 13) == '/index/image/')
            {
                // Allowed to check that a key does not exist
                if (check.target == 'VERSION' && check.version == 0)
                {
                    checked['M'+key] = true;
                    return true;
                }
                else if (check.target == 'MOD')
                {
                    let data = this._get(check.key);
                    if (!data || data.mod_revision != check.mod_revision)
                    {
                        // Break check to trigger CAS failure
                        check.mod_revision = '18446744073709551615'; // UINT64_MAX
                        return true;
                    }
                    data = this._decode(check.key, data.value);
                    if (data)
                    {
                        const inode = this._get([ ...this.prefix_parts, 'config', 'inode', data.pool_id, data.id ], true);
                        if (inode && (inode.owner_group && userInfo.groups[inode.owner_group] ||
                            inode.owner === userInfo.name))
                        {
                            checked['M'+key] = true;
                            return true;
                        }
                    }
                }
                return false;
            }
            if (key.substr(0, 13) == '/index/maxid/')
            {
                const pool_id = key.substr(13);
                const pool_cfg = this._get([ ...this.prefix_parts, 'config', 'pools' ], true);
                if (!pool_cfg || !pool_cfg[pool_id] || !pool_cfg[pool_id].creator_group || !userInfo.groups[pool_cfg[pool_id].creator_group])
                {
                    return false;
                }
                if (check.target == 'VERSION' && check.version == 0)
                {
                    checked['I'+parseInt(key.substr(13))+'_0'] = true;
                    return true;
                }
                else if (check.target == 'MOD')
                {
                    const data = this._get(check.key);
                    if (!data || data.mod_revision != check.mod_revision)
                    {
                        // Break check to trigger CAS failure
                        check.mod_revision = '18446744073709551615'; // UINT64_MAX
                        return true;
                    }
                    checked['I'+parseInt(key.substr(13))+'_'+data.value] = true;
                    return true;
                }
                return false;
            }
        }
        return false;
    }

    _check_read(kv, userInfo)
    {
        let key = String(kv.key);
        if (key.substr(0, this.prefix.length) !== this.prefix)
        {
            return false;
        }
        key = key.substr(this.prefix.length);
        if (key in userInfo.perms.keys)
        {
            return true;
        }
        for (const pfx in userInfo.perms.prefixes)
        {
            if (key.substr(0, pfx.length) == pfx)
            {
                return true;
            }
        }
        if (userInfo.type == 'client')
        {
            // Image permissions
            if (key.substr(0, 14) == '/config/inode/')
            {
                const inode = this._decode(kv.key, kv.value);
                if (inode && (inode.reader_group && userInfo.groups[inode.reader_group] ||
                    inode.owner_group && userInfo.groups[inode.owner_group] ||
                    inode.owner === userInfo.name))
                {
                    return true;
                }
                return false;
            }
            if (key.substr(0, 13) == '/index/image/')
            {
                const data = this._decode(kv.key, kv.value);
                const inode = this._get([ ...this.prefix_parts, 'config', 'inode', data.pool_id, data.id ], true);
                if (inode && (inode.reader_group && userInfo.groups[inode.reader_group] ||
                    inode.owner_group && userInfo.groups[inode.owner_group] ||
                    inode.owner === userInfo.name))
                {
                    return true;
                }
                return false;
            }
        }
        return false;
    }

    _check_write(put, userInfo, checked)
    {
        let key = String(put.key);
        if (key.substr(0, this.prefix.length) !== this.prefix)
        {
            return false;
        }
        key = key.substr(this.prefix.length);
        if (userInfo.perms.keys[key])
        {
            return true;
        }
        for (const pfx in userInfo.perms.prefixes)
        {
            if (userInfo.perms.prefixes[pfx] && key.substr(0, pfx.length) == pfx)
            {
                return true;
            }
        }
        if (checked && userInfo.type == 'client')
        {
            if (key.substr(0, 13) == '/index/maxid/' &&
                checked['I'+parseInt(key.substr(13))+'_'+(put.value-1)])
            {
                // Allowed to increment maxid
                return true;
            }
            if (checked['M'+key])
            {
                // Allowed to modify known images with CAS checks
                return true;
            }
        }
        return false;
    }

    _check_req(req, userInfo, checked)
    {
        let r;
        if ((r = (req.request_range || req.requestRange)))
        {
            // All range queries are allowed, but responses are filtered - it's simpler
        }
        else if ((r = (req.request_put || req.requestPut)))
        {
            if (!this._check_write(r, userInfo, checked))
                return false;
        }
        else if ((r = (req.request_delete_range || req.requestDeleteRange)))
        {
            if (!r.range_end || r.range_end === r.key)
            {
                if (!this._check_write({ key: r.key }, userInfo))
                    return false;
            }
            else
            {
                // All keys in range must satisfy prefix
                r.range_end = String(r.range_end);
                if (r.key.length != r.range_end.length ||
                    r.key[r.key.length-1] != '/' ||
                    r.range_end[r.range_end.length-1] != '0')
                {
                    return false;
                }
                let key = r.key.substr(this.prefix.length);
                let found = false;
                for (const pfx in userInfo.perms.prefixes)
                {
                    if (userInfo.perms.prefixes[pfx] && key.substr(0, pfx.length) == pfx)
                    {
                        found = true;
                        break;
                    }
                }
                if (!found)
                    return false;
            }
        }
        return true;
    }

    _get_user(username)
    {
        if (!username)
        {
            return null;
        }
        let userInfo = this._get([ ...this.prefix_parts, 'config', 'user', username ], true);
        if (!userInfo)
        {
            userInfo = { type: 'client' };
        }
        userInfo.perms = static_perms[userInfo.type] || static_perms['invalid'];
        userInfo.name = username;
        if (userInfo.groups instanceof Array)
        {
            userInfo.groups = userInfo.groups.reduce((a, c) => { a[c] = true; return a; }, {});
        }
        else
        {
            userInfo.groups = {};
        }
        return userInfo;
    }

    filter_api(username, api/*, data*/)
    {
        if (username === 'root')
        {
            return true;
        }
        const userInfo = this._get([ ...this.prefix_parts, 'config', 'user', username ], true);
        return userInfo && api_perms[userInfo.type] && api_perms[userInfo.type][api];
    }

    filter_txn(username, txn)
    {
        if (username === 'root')
        {
            return true;
        }
        const userInfo = this._get_user(username);
        if (!userInfo)
        {
            return null;
        }
        const checked = {};
        if (txn.compare)
        {
            for (const check of txn.compare)
            {
                if (!this._check_compare(check, userInfo, checked))
                    return null;
            }
        }
        // Special transactions:
        // 1. create image: create config/inode and index/image, increment index/maxid/<pool> (with CAS)
        // 2. create snapshot: same as create image but also rename previous to @snap
        if (txn.success)
        {
            for (const req of txn.success)
            {
                if (!this._check_req(req, userInfo, checked))
                    return null;
            }
        }
        if (txn.failure)
        {
            for (const req of txn.failure)
            {
                if (!this._check_req(req, userInfo, null))
                    return null;
            }
        }
        return txn;
    }

    filter_txn_response(username, txn, res)
    {
        if (!res.responses || username === 'root')
        {
            return;
        }
        const userInfo = this._get_user(username);
        if (!userInfo)
        {
            for (const resp of res.responses)
            {
                if (resp.response_range && resp.response_range.kvs)
                {
                    resp.response_range.kvs = [];
                }
            }
            return;
        }
        for (const resp of res.responses)
        {
            if (resp.response_range && resp.response_range.kvs)
            {
                resp.response_range.kvs = resp.response_range.kvs.filter(kv => this._check_read(kv, userInfo));
            }
        }
    }

    filter_watch_message(username, msg)
    {
        if (!msg.result || !msg.result.events || username === 'root')
        {
            return;
        }
        const userInfo = this._get_user(username);
        if (!userInfo)
        {
            msg.result.events = [];
            return;
        }
        msg.result.events = msg.result.events.filter(ev => this._check_read(ev.kv, userInfo));
    }
}

module.exports = VitastorAuthFilter;
