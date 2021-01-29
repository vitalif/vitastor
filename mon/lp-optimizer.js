// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.0 (see README.md for details)

// Data distribution optimizer using linear programming (lp_solve)

const child_process = require('child_process');

const NO_OSD = 'Z';

async function lp_solve(text)
{
    const cp = child_process.spawn('lp_solve');
    let stdout = '', stderr = '', finish_cb;
    cp.stdout.on('data', buf => stdout += buf.toString());
    cp.stderr.on('data', buf => stderr += buf.toString());
    cp.on('exit', () => finish_cb && finish_cb());
    cp.stdin.write(text);
    cp.stdin.end();
    if (cp.exitCode == null)
    {
        await new Promise(ok => finish_cb = ok);
    }
    if (!stdout.trim())
    {
        return null;
    }
    let score = 0;
    let vars = {};
    for (const line of stdout.split(/\n/))
    {
        let m = /^(^Value of objective function: (-?[\d\.]+)|Actual values of the variables:)\s*$/.exec(line);
        if (m)
        {
            if (m[2])
            {
                score = m[2];
            }
            continue;
        }
        else if (/This problem is (infeasible|unbounded)/.exec(line))
        {
            return null;
        }
        let [ k, v ] = line.trim().split(/\s+/, 2);
        if (v)
        {
            vars[k] = v;
        }
    }
    return { score, vars };
}

async function optimize_initial({ osd_tree, pg_count, pg_size = 3, pg_minsize = 2, max_combinations = 10000, parity_space = 1 })
{
    if (!pg_count || !osd_tree)
    {
        return null;
    }
    const all_weights = Object.assign({}, ...Object.values(osd_tree));
    const total_weight = Object.values(all_weights).reduce((a, c) => Number(a) + Number(c), 0);
    const all_pgs = Object.values(random_combinations(osd_tree, pg_size, max_combinations, parity_space > 1));
    const pg_per_osd = {};
    for (const pg of all_pgs)
    {
        for (let i = 0; i < pg.length; i++)
        {
            const osd = pg[i];
            pg_per_osd[osd] = pg_per_osd[osd] || [];
            pg_per_osd[osd].push((i >= pg_minsize ? parity_space+'*' : '')+"pg_"+pg.join("_"));
        }
    }
    const pg_effsize = Math.min(pg_minsize, Object.keys(osd_tree).length)
        + Math.max(0, Math.min(pg_size, Object.keys(osd_tree).length) - pg_minsize) * parity_space;
    let lp = '';
    lp += "max: "+all_pgs.map(pg => 'pg_'+pg.join('_')).join(' + ')+";\n";
    for (const osd in pg_per_osd)
    {
        if (osd !== NO_OSD)
        {
            let osd_pg_count = all_weights[osd]/total_weight*pg_effsize*pg_count;
            lp += pg_per_osd[osd].join(' + ')+' <= '+osd_pg_count+';\n';
        }
    }
    for (const pg of all_pgs)
    {
        lp += 'pg_'+pg.join('_')+" >= 0;\n";
    }
    lp += "sec "+all_pgs.map(pg => 'pg_'+pg.join('_')).join(', ')+";\n";
    const lp_result = await lp_solve(lp);
    if (!lp_result)
    {
        console.log(lp);
        throw new Error('Problem is infeasible or unbounded - is it a bug?');
    }
    const int_pgs = make_int_pgs(lp_result.vars, pg_count);
    const eff = pg_list_space_efficiency(int_pgs, all_weights, pg_minsize, parity_space);
    const res = {
        score: lp_result.score,
        weights: lp_result.vars,
        int_pgs,
        space: eff * pg_effsize,
        total_space: total_weight,
    };
    return res;
}

function make_int_pgs(weights, pg_count)
{
    const total_weight = Object.values(weights).reduce((a, c) => Number(a) + Number(c), 0);
    let int_pgs = [];
    let pg_left = pg_count;
    let weight_left = total_weight;
    for (const pg_name in weights)
    {
        let n = Math.round(weights[pg_name] / weight_left * pg_left);
        for (let i = 0; i < n; i++)
        {
            int_pgs.push(pg_name.substr(3).split('_'));
        }
        weight_left -= weights[pg_name];
        pg_left -= n;
    }
    return int_pgs;
}

function calc_intersect_weights(pg_size, pg_count, prev_weights, all_pgs)
{
    const move_weights = {};
    if ((1 << pg_size) < pg_count)
    {
        const intersect = {};
        for (const pg_name in prev_weights)
        {
            const pg = pg_name.substr(3).split(/_/);
            for (let omit = 1; omit < (1 << pg_size); omit++)
            {
                let pg_omit = [ ...pg ];
                let intersect_count = pg_size;
                for (let i = 0; i < pg_size; i++)
                {
                    if (omit & (1 << i))
                    {
                        pg_omit[i] = '';
                        intersect_count--;
                    }
                }
                pg_omit = pg_omit.join(':');
                intersect[pg_omit] = Math.max(intersect[pg_omit] || 0, intersect_count);
            }
        }
        for (const pg of all_pgs)
        {
            let max_int = 0;
            for (let omit = 1; omit < (1 << pg_size); omit++)
            {
                let pg_omit = [ ...pg ];
                for (let i = 0; i < pg_size; i++)
                {
                    if (omit & (1 << i))
                    {
                        pg_omit[i] = '';
                    }
                }
                pg_omit = pg_omit.join(':');
                max_int = Math.max(max_int, intersect[pg_omit] || 0);
            }
            move_weights['pg_'+pg.join('_')] = pg_size-max_int;
        }
    }
    else
    {
        const prev_pg_hashed = Object.keys(prev_weights).map(pg_name => pg_name.substr(3).split(/_/).reduce((a, c) => { a[c] = 1; return a; }, {}));
        for (const pg of all_pgs)
        {
            if (!prev_weights['pg_'+pg.join('_')])
            {
                let max_int = 0;
                for (const prev_hash in prev_pg_hashed)
                {
                    const intersect_count = pg.reduce((a, osd) => a + (prev_hash[osd] ? 1 : 0), 0);
                    if (max_int < intersect_count)
                    {
                        max_int = intersect_count;
                        if (max_int >= pg_size)
                        {
                            break;
                        }
                    }
                }
                move_weights['pg_'+pg.join('_')] = pg_size-max_int;
            }
        }
    }
    return move_weights;
}

function add_valid_previous(osd_tree, prev_weights, all_pgs)
{
    // Add previous combinations that are still valid
    const hosts = Object.keys(osd_tree).sort();
    const host_per_osd = {};
    for (const host in osd_tree)
    {
        for (const osd in osd_tree[host])
        {
            host_per_osd[osd] = host;
        }
    }
    skip_pg: for (const pg_name in prev_weights)
    {
        const seen_hosts = {};
        const pg = pg_name.substr(3).split(/_/);
        for (const osd of pg)
        {
            if (!host_per_osd[osd] || seen_hosts[host_per_osd[osd]])
            {
                continue skip_pg;
            }
            seen_hosts[host_per_osd[osd]] = true;
        }
        if (!all_pgs[pg_name])
        {
            all_pgs[pg_name] = pg;
        }
    }
}

// Try to minimize data movement
async function optimize_change({ prev_pgs: prev_int_pgs, osd_tree, pg_size = 3, pg_minsize = 2, max_combinations = 10000, parity_space = 1 })
{
    if (!osd_tree)
    {
        return null;
    }
    const pg_effsize = Math.min(pg_minsize, Object.keys(osd_tree).length)
        + Math.max(0, Math.min(pg_size, Object.keys(osd_tree).length) - pg_minsize) * parity_space;
    const pg_count = prev_int_pgs.length;
    const prev_weights = {};
    const prev_pg_per_osd = {};
    for (const pg of prev_int_pgs)
    {
        const pg_name = 'pg_'+pg.join('_');
        prev_weights[pg_name] = (prev_weights[pg_name]||0) + 1;
        for (let i = 0; i < pg.length; i++)
        {
            const osd = pg[i];
            prev_pg_per_osd[osd] = prev_pg_per_osd[osd] || [];
            prev_pg_per_osd[osd].push([ pg_name, (i >= pg_minsize ? parity_space : 1) ]);
        }
    }
    // Get all combinations
    let all_pgs = random_combinations(osd_tree, pg_size, max_combinations, parity_space > 1);
    add_valid_previous(osd_tree, prev_weights, all_pgs);
    all_pgs = Object.values(all_pgs);
    const pg_per_osd = {};
    for (const pg of all_pgs)
    {
        const pg_name = 'pg_'+pg.join('_');
        for (let i = 0; i < pg.length; i++)
        {
            const osd = pg[i];
            pg_per_osd[osd] = pg_per_osd[osd] || [];
            pg_per_osd[osd].push([ pg_name, (i >= pg_minsize ? parity_space : 1) ]);
        }
    }
    // Penalize PGs based on their similarity to old PGs
    const move_weights = calc_intersect_weights(pg_size, pg_count, prev_weights, all_pgs);
    // Calculate total weight - old PG weights
    const all_pg_names = all_pgs.map(pg => 'pg_'+pg.join('_'));
    const all_pgs_hash = all_pg_names.reduce((a, c) => { a[c] = true; return a; }, {});
    const all_weights = Object.assign({}, ...Object.values(osd_tree));
    const total_weight = Object.values(all_weights).reduce((a, c) => Number(a) + Number(c), 0);
    // Generate the LP problem
    let lp = '';
    lp += 'max: '+all_pg_names.map(pg_name => (
        prev_weights[pg_name] ? `${pg_size+1}*add_${pg_name} - ${pg_size+1}*del_${pg_name}` : `${pg_size+1-move_weights[pg_name]}*${pg_name}`
    )).join(' + ')+';\n';
    for (const osd in pg_per_osd)
    {
        if (osd !== NO_OSD)
        {
            const osd_sum = (pg_per_osd[osd]||[]).map(([ pg_name, space ]) => (
                prev_weights[pg_name] ? `${space} * add_${pg_name} - ${space} * del_${pg_name}` : `${space} * ${pg_name}`
            )).join(' + ');
            const rm_osd_pg_count = (prev_pg_per_osd[osd]||[])
                .reduce((a, [ old_pg_name, space ]) => (a + (all_pgs_hash[old_pg_name] ? space : 0)), 0);
            const osd_pg_count = all_weights[osd]*pg_effsize/total_weight*pg_count - rm_osd_pg_count;
            lp += osd_sum + ' <= ' + osd_pg_count + ';\n';
        }
    }
    let pg_vars = [];
    for (const pg_name of all_pg_names)
    {
        if (prev_weights[pg_name])
        {
            pg_vars.push(`add_${pg_name}`, `del_${pg_name}`);
            // Can't add or remove less than zero
            lp += `add_${pg_name} >= 0;\n`;
            lp += `del_${pg_name} >= 0;\n`;
            // Can't remove more than the PG already has
            lp += `add_${pg_name} - del_${pg_name} >= -${prev_weights[pg_name]};\n`;
        }
        else
        {
            pg_vars.push(pg_name);
            lp += `${pg_name} >= 0;\n`;
        }
    }
    lp += 'sec '+pg_vars.join(', ')+';\n';
    // Solve it
    const lp_result = await lp_solve(lp);
    if (!lp_result)
    {
        console.log(lp);
        throw new Error('Problem is infeasible or unbounded - is it a bug?');
    }
    // Generate the new distribution
    const weights = { ...prev_weights };
    for (const k in prev_weights)
    {
        if (!all_pgs_hash[k])
        {
            delete weights[k];
        }
    }
    for (const k in lp_result.vars)
    {
        if (k.substr(0, 4) === 'add_')
        {
            weights[k.substr(4)] = (weights[k.substr(4)] || 0) + Number(lp_result.vars[k]);
        }
        else if (k.substr(0, 4) === 'del_')
        {
            weights[k.substr(4)] = (weights[k.substr(4)] || 0) - Number(lp_result.vars[k]);
        }
        else if (k.substr(0, 3) === 'pg_')
        {
            weights[k] = Number(lp_result.vars[k]);
        }
    }
    for (const k in weights)
    {
        if (!weights[k])
        {
            delete weights[k];
        }
    }
    const int_pgs = make_int_pgs(weights, pg_count);
    // Align them with most similar previous PGs
    const new_pgs = align_pgs(prev_int_pgs, int_pgs);
    let differs = 0, osd_differs = 0;
    for (let i = 0; i < pg_count; i++)
    {
        if (new_pgs[i].join('_') != prev_int_pgs[i].join('_'))
        {
            differs++;
        }
        for (let j = 0; j < pg_size; j++)
        {
            if (new_pgs[i][j] != prev_int_pgs[i][j])
            {
                osd_differs++;
            }
        }
    }
    return {
        prev_pgs: prev_int_pgs,
        score: lp_result.score,
        weights,
        int_pgs: new_pgs,
        differs,
        osd_differs,
        space: pg_effsize * pg_list_space_efficiency(new_pgs, all_weights, pg_minsize, parity_space),
        total_space: total_weight,
    };
}

function print_change_stats(retval, detailed)
{
    const new_pgs = retval.int_pgs;
    const prev_int_pgs = retval.prev_pgs;
    if (prev_int_pgs)
    {
        if (detailed)
        {
            for (let i = 0; i < new_pgs.length; i++)
            {
                if (new_pgs[i].join('_') != prev_int_pgs[i].join('_'))
                {
                    console.log("pg "+i+": "+prev_int_pgs[i].join(' ')+" -> "+new_pgs[i].join(' '));
                }
            }
        }
        console.log(
            "Data movement: "+retval.differs+" pgs, "+
            retval.osd_differs+" pg*osds = "+Math.round(retval.osd_differs / prev_int_pgs.length / 3 * 10000)/100+" %"
        );
    }
    console.log(
        "Total space (raw): "+Math.round(retval.space*100)/100+" TB, space efficiency: "+
        Math.round(retval.space/(retval.total_space||1)*10000)/100+" %"
    );
}

function align_pgs(prev_int_pgs, int_pgs)
{
    const aligned_pgs = [];
    put_aligned_pgs(aligned_pgs, int_pgs, prev_int_pgs, (pg) => [ pg.join(':') ]);
    put_aligned_pgs(aligned_pgs, int_pgs, prev_int_pgs, (pg) => [ pg[0]+'::'+pg[2], ':'+pg[1]+':'+pg[2], pg[0]+':'+pg[1]+':' ]);
    put_aligned_pgs(aligned_pgs, int_pgs, prev_int_pgs, (pg) => [ pg[0]+'::', ':'+pg[1]+':', '::'+pg[2] ]);
    const free_slots = prev_int_pgs.map((pg, i) => !aligned_pgs[i] ? i : null).filter(i => i != null);
    for (const pg of int_pgs)
    {
        if (!free_slots.length)
        {
            throw new Error("Can't place unaligned PG");
        }
        aligned_pgs[free_slots.shift()] = pg;
    }
    return aligned_pgs;
}

function put_aligned_pgs(aligned_pgs, int_pgs, prev_int_pgs, keygen)
{
    let prev_indexes = {};
    for (let i = 0; i < prev_int_pgs.length; i++)
    {
        for (let k of keygen(prev_int_pgs[i]))
        {
            prev_indexes[k] = prev_indexes[k] || [];
            prev_indexes[k].push(i);
        }
    }
    PG: for (let i = int_pgs.length-1; i >= 0; i--)
    {
        let pg = int_pgs[i];
        let keys = keygen(int_pgs[i]);
        for (let k of keys)
        {
            while (prev_indexes[k] && prev_indexes[k].length)
            {
                let idx = prev_indexes[k].shift();
                if (!aligned_pgs[idx])
                {
                    aligned_pgs[idx] = pg;
                    int_pgs.splice(i, 1);
                    continue PG;
                }
            }
        }
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

// unordered = don't treat (x,y) and (y,x) as equal
function random_combinations(osd_tree, pg_size, count, unordered)
{
    let seed = 0x5f020e43;
    let rng = () =>
    {
        seed ^= seed << 13;
        seed ^= seed >> 17;
        seed ^= seed << 5;
        return seed + 2147483648;
    };
    const hosts = Object.keys(osd_tree).sort();
    const osds = Object.keys(osd_tree).reduce((a, c) => { a[c] = Object.keys(osd_tree[c]).sort(); return a; }, {});
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
                const next_host = rng() % cur_hosts.length;
                const next_osd = rng() % osds[cur_hosts[next_host]].length;
                pg.push(osds[cur_hosts[next_host]][next_osd]);
                cur_hosts.splice(next_host, 1);
            }
            const cyclic_pgs = [ pg ];
            if (unordered)
            {
                for (let i = 1; i < pg.size; i++)
                {
                    cyclic_pgs.push([ ...pg.slice(i), ...pg.slice(0, i) ]);
                }
            }
            for (const pg of cyclic_pgs)
            {
                while (pg.length < pg_size)
                {
                    pg.push(NO_OSD);
                }
                r['pg_'+pg.join('_')] = pg;
            }
        }
    }
    // Generate purely random combinations
    while (count > 0)
    {
        let host_idx = [];
        const cur_hosts = [ ...hosts.map((h, i) => i) ];
        const max_hosts = pg_size < hosts.length ? pg_size : hosts.length;
        if (unordered)
        {
            for (let i = 0; i < max_hosts; i++)
            {
                const r = rng() % cur_hosts.length;
                host_idx[i] = cur_hosts[r];
                cur_hosts.splice(r, 1);
            }
        }
        else
        {
            for (let i = 0; i < max_hosts; i++)
            {
                const r = rng() % (cur_hosts.length - (max_hosts - i - 1));
                host_idx[i] = cur_hosts[r];
                cur_hosts.splice(0, r+1);
            }
        }
        let pg = host_idx.map(h => osds[hosts[h]][rng() % osds[hosts[h]].length]);
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

function pg_weights_space_efficiency(weights, pg_count, osd_sizes)
{
    const per_osd = {};
    for (const pg_name in weights)
    {
        for (const osd of pg_name.substr(3).split(/_/))
        {
            per_osd[osd] = (per_osd[osd]||0) + weights[pg_name];
        }
    }
    return pg_per_osd_space_efficiency(per_osd, pg_count, osd_sizes);
}

function pg_list_space_efficiency(pgs, osd_sizes, pg_minsize, parity_space)
{
    const per_osd = {};
    for (const pg of pgs)
    {
        for (let i = 0; i < pg.length; i++)
        {
            const osd = pg[i];
            per_osd[osd] = (per_osd[osd]||0) + (i >= pg_minsize ? (parity_space||1) : 1);
        }
    }
    return pg_per_osd_space_efficiency(per_osd, pgs.length, osd_sizes);
}

function pg_per_osd_space_efficiency(per_osd, pg_count, osd_sizes)
{
    // each PG gets randomly selected in 1/N cases
    // & there are x PGs per OSD
    // => an OSD is selected in x/N cases
    // => total space * x/N <= OSD size
    // => total space <= OSD size * N/x
    let space;
    for (let osd in per_osd)
    {
        if (osd in osd_sizes)
        {
            const space_estimate = osd_sizes[osd] * pg_count / per_osd[osd];
            if (space == null || space > space_estimate)
            {
                space = space_estimate;
            }
        }
    }
    return space == null ? 0 : space;
}

module.exports = {
    NO_OSD,

    optimize_initial,
    optimize_change,
    print_change_stats,
    pg_weights_space_efficiency,
    pg_list_space_efficiency,
    pg_per_osd_space_efficiency,
    flatten_tree,

    lp_solve,
    make_int_pgs,
    align_pgs,
    random_combinations,
    all_combinations,
};
