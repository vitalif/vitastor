const { select_murmur3 } = require('./murmur3.js');

const NO_OSD = 'Z';

class SimpleCombinator
{
    constructor(flat_tree, pg_size, max_combinations, ordered)
    {
        this.osd_tree = flat_tree;
        this.pg_size = pg_size;
        this.max_combinations = max_combinations;
        this.ordered = ordered;
    }

    random_combinations()
    {
        return random_combinations(this.osd_tree, this.pg_size, this.max_combinations, this.ordered);
    }

    check_combinations(pgs)
    {
        return check_combinations(this.osd_tree, pgs);
    }
}

// Convert multi-level osd_tree = { level: number|string, id?: string, size?: number, children?: osd_tree }[]
// levels = { string: number }
// to a two-level osd_tree suitable for all_combinations()
function flatten_tree(osd_tree, levels, failure_domain_level, osd_level, domains = {}, i = { i: 1 })
{
    osd_level = levels[osd_level] || osd_level;
    failure_domain_level = levels[failure_domain_level] || failure_domain_level;
    for (const node of osd_tree)
    {
        if ((levels[node.level] || node.level) < failure_domain_level)
        {
            flatten_tree(node.children||[], levels, failure_domain_level, osd_level, domains, i);
        }
        else
        {
            domains['dom'+(i.i++)] = extract_osds([ node ], levels, osd_level);
        }
    }
    return domains;
}

function extract_osds(osd_tree, levels, osd_level, osds = {})
{
    for (const node of osd_tree)
    {
        if ((levels[node.level] || node.level) >= osd_level)
        {
            osds[node.id] = node.size;
        }
        else
        {
            extract_osds(node.children||[], levels, osd_level, osds);
        }
    }
    return osds;
}

// ordered = don't treat (x,y) and (y,x) as equal
function random_combinations(osd_tree, pg_size, count, ordered)
{
    const osds = Object.keys(osd_tree).reduce((a, c) => { a[c] = Object.keys(osd_tree[c]).sort(); return a; }, {});
    const hosts = Object.keys(osd_tree).sort().filter(h => osds[h].length > 0);
    const r = {};
    // Generate random combinations including each OSD at least once
    for (let h = 0; h < hosts.length; h++)
    {
        for (let o = 0; o < osds[hosts[h]].length; o++)
        {
            const pg = [ osds[hosts[h]][o] ];
            const cur_hosts = [ ...hosts ];
            cur_hosts.splice(h, 1);
            for (let i = 1; i < pg_size && i < hosts.length; i++)
            {
                const next_host = select_murmur3(cur_hosts.length, i => pg[0]+':i:'+cur_hosts[i]);
                const next_osd = select_murmur3(osds[cur_hosts[next_host]].length, i => pg[0]+':i:'+osds[cur_hosts[next_host]][i]);
                pg.push(osds[cur_hosts[next_host]][next_osd]);
                cur_hosts.splice(next_host, 1);
            }
            while (pg.length < pg_size)
            {
                pg.push(NO_OSD);
            }
            r['pg_'+pg.join('_')] = pg;
        }
    }
    // Generate purely random combinations
    while (count > 0)
    {
        let host_idx = [];
        const cur_hosts = [ ...hosts.map((h, i) => i) ];
        const max_hosts = pg_size < hosts.length ? pg_size : hosts.length;
        if (ordered)
        {
            for (let i = 0; i < max_hosts; i++)
            {
                const r = select_murmur3(cur_hosts.length, i => count+':h:'+cur_hosts[i]);
                host_idx[i] = cur_hosts[r];
                cur_hosts.splice(r, 1);
            }
        }
        else
        {
            for (let i = 0; i < max_hosts; i++)
            {
                const r = select_murmur3(cur_hosts.length - (max_hosts - i - 1), i => count+':h:'+cur_hosts[i]);
                host_idx[i] = cur_hosts[r];
                cur_hosts.splice(0, r+1);
            }
        }
        let pg = host_idx.map(h => osds[hosts[h]][select_murmur3(osds[hosts[h]].length, i => count+':o:'+osds[hosts[h]][i])]);
        while (pg.length < pg_size)
        {
            pg.push(NO_OSD);
        }
        r['pg_'+pg.join('_')] = pg;
        count--;
    }
    return r;
}

// Super-stupid algorithm. Given the current OSD tree, generate all possible OSD combinations
// osd_tree = { failure_domain1: { osd1: size1, ... }, ... }
// ordered = return combinations without duplicates having different order
function all_combinations(osd_tree, pg_size, ordered, count)
{
    const hosts = Object.keys(osd_tree).sort();
    const osds = Object.keys(osd_tree).reduce((a, c) => { a[c] = Object.keys(osd_tree[c]).sort(); return a; }, {});
    while (hosts.length < pg_size)
    {
        osds[NO_OSD] = [ NO_OSD ];
        hosts.push(NO_OSD);
    }
    let host_idx = [];
    let osd_idx = [];
    for (let i = 0; i < pg_size; i++)
    {
        host_idx.push(i);
        osd_idx.push(0);
    }
    const r = [];
    while (!count || count < 0 || r.length < count)
    {
        r.push(host_idx.map((hi, i) => osds[hosts[hi]][osd_idx[i]]));
        let inc = pg_size-1;
        while (inc >= 0)
        {
            osd_idx[inc]++;
            if (osd_idx[inc] >= osds[hosts[host_idx[inc]]].length)
            {
                osd_idx[inc] = 0;
                inc--;
            }
            else
            {
                break;
            }
        }
        if (inc < 0)
        {
            // no osds left in the current host combination, select the next one
            inc = pg_size-1;
            same_again: while (inc >= 0)
            {
                host_idx[inc]++;
                for (let prev_host = 0; prev_host < inc; prev_host++)
                {
                    if (host_idx[prev_host] == host_idx[inc])
                    {
                        continue same_again;
                    }
                }
                if (host_idx[inc] < (ordered ? hosts.length-(pg_size-1-inc) : hosts.length))
                {
                    while ((++inc) < pg_size)
                    {
                        host_idx[inc] = (ordered ? host_idx[inc-1]+1 : 0);
                    }
                    break;
                }
                else
                {
                    inc--;
                }
            }
            if (inc < 0)
            {
                break;
            }
        }
    }
    return r;
}

function check_combinations(osd_tree, pgs)
{
    const hosts = Object.keys(osd_tree).sort();
    const host_per_osd = {};
    for (const host in osd_tree)
    {
        for (const osd in osd_tree[host])
        {
            host_per_osd[osd] = host;
        }
    }
    const res = [];
    skip_pg: for (const pg of pgs)
    {
        const seen_hosts = {};
        for (const osd of pg)
        {
            if (!host_per_osd[osd] || seen_hosts[host_per_osd[osd]])
            {
                continue skip_pg;
            }
            seen_hosts[host_per_osd[osd]] = true;
        }
        res.push(pg);
    }
    return res;
}

function compat(params)
{
    return {
        ...params,
        osd_weights: Object.assign({}, ...Object.values(params.osd_tree)),
        combinator: new SimpleCombinator(params.osd_tree, params.pg_size, params.max_combinations||10000),
    };
}

module.exports = {
    flatten_tree,
    SimpleCombinator,
    compat,
    NO_OSD,
};
