// Data distribution optimizer using linear programming (lp_solve)

const child_process = require('child_process');

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
    let score = 0;
    let vars = {};
    for (const line of stdout.split(/\n/))
    {
        let m = /^(^Value of objective function: ([\d\.]+)|Actual values of the variables:)\s*$/.exec(line);
        if (m)
        {
            if (m[2])
            {
                score = m[2];
            }
            continue;
        }
        else if (/This problem is infeasible/.exec(line))
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

function make_single(osd_tree)
{
    const initial = all_combinations(osd_tree, 1)[0];
    const all_weights = Object.assign.apply({}, Object.values(osd_tree));
    let weight;
    for (const osd of initial)
    {
        if (weight == null || all_weights[osd] < weight)
        {
            weight = all_weights[osd];
        }
    }
    return [
        { set: initial, weight: weight },
    ];
}

async function optimize_initial(osd_tree, pg_count)
{
    const all_weights = Object.assign.apply({}, Object.values(osd_tree));
    const all_pgs = all_combinations($osd_tree);
    const pg_per_osd = {};
    for (const pg in all_pgs)
    {
        for (const osd of pg)
        {
            pg_per_osd[osd] = pg_per_osd[osd] || [];
            pg_per_osd[osd].push("pg_"+pg.join("_"));
        }
    }
    let lp = '';
    lp += "max: "+all_pgs.map(pg => 'pg_'+pg.join('_')).join(' + ')+";\n";
    for (const osd in pg_per_osd)
    {
        lp += pg_per_osd[osd].join(' + ')+' <= '+all_weights[osd]+';\n';
    }
    for (const pg of all_pgs)
    {
        lp += 'pg_'+pg.join('_')+" >= 0;\n";
    }
    lp += "int "+all_pgs.map(pg => 'pg_'+pg.join('_')).join(', ')+";\n";
    const [ score, weights ] = await lp_solve(lp);
    const int_pgs = make_int_pgs(weights, pg_count);
    const eff = pg_list_space_efficiency(int_pgs, osd_tree);
    const total_weight = Object.values(all_weights).reduce((a, c) => Number(a) + Number(c));
    return { score, weights, int_pgs, total_space: eff*3, space_eff: eff*3/total_weight };
}

function make_int_pgs(weights, pg_count)
{
    const total_weight = Object.values(weights).reduce((a, c) => Number(a) + Number(c));
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

function get_int_pg_weights(prev_int_pgs, osd_tree)
{
    const space = pg_list_space_efficiency(prev_int_pgs, osd_tree);
    const prev_weights = {};
    let count = 0;
    for (const pg of prev_int_pgs)
    {
        const pg_name = 'pg_'+pg.join('_');
        prev_weights[pg_name] = (prev_weights[pg_name]||0) + 1;
        count++;
    }
    for (const pg_name in prev_weights)
    {
        prev_weights[pg_name] *= space / count;
    }
    return prev_weights;
}

// Try to minimize data movement
async function optimize_change(prev_int_pgs, osd_tree)
{
    const pg_count = prev_int_pgs.length;
    const prev_weights = {};
    const prev_pg_per_osd = {};
    for (const pg of prev_int_pgs)
    {
        const pg_name = 'pg_'+pg.join('_');
        prev_weights[pg_name] = (prev_weights[pg_name]||0) + 1;
        for (const osd of pg)
        {
            prev_pg_per_osd[osd] = prev_pg_per_osd[osd] || [];
            prev_pg_per_osd[osd].push(pg_name);
        }
    }
    // Get all combinations
    const all_pgs = all_combinations(osd_tree);
    const pg_per_osd = {};
    for (const pg of all_pgs)
    {
        const pg_name = 'pg_'+pg.join('_');
        for (const osd of pg)
        {
            pg_per_osd[osd] = pg_per_osd[osd] || [];
            pg_per_osd[osd].push(pg_name);
        }
    }
    // Penalize PGs based on their similarity to old PGs
    const intersect = {};
    for (const pg_name in prev_weights)
    {
        const pg = pg_name.substr(3).split(/_/);
        intersect[pg[0]+'::'] = intersect[':'+pg[1]+':'] = intersect['::'+pg[2]] = 2;
        intersect[pg[0]+'::'+pg[2]] = intersect[':'+pg[1]+':'+pg[2]] = intersect[pg[0]+':'+pg[1]+':'] = 1;
    }
    const move_weights = {};
    for (const pg of all_pgs)
    {
        move_weights['pg_'+pg.join('_')] =
            intersect[pg[0]+'::'+pg[2]] || intersect[':'+pg[1]+':'+pg[2]] || intersect[pg[0]+':'+pg[1]+':'] ||
            intersect[pg[0]+'::'] || intersect[':'+pg[1]+':'] || intersect['::'+pg[2]] ||
            3;
    }
    // Calculate total weight - old PG weights
    const all_pg_names = all_pgs.map(pg => 'pg_'+pg.join('_'));
    const all_weights = Object.assign.apply({}, Object.values(osd_tree));
    const total_weight = Object.values(all_weights).reduce((a, c) => Number(a) + Number(c));
    // Generate the LP problem
    let lp = '';
    lp += 'min: '+all_pg_names.map(pg_name => move_weights[pg_name] + ' * ' + (prev_weights[pg_name] ? 'add_' : '') + pg_name).join(' + ')+';\n';
    lp += all_pg_names.map(pg_name => prev_weights[pg_name] ? `add_${pg_name} - del_${pg_name}` : pg_name).join(' + ')+' = 0;\n';
    for (const osd in pg_per_osd)
    {
        const osd_sum = (pg_per_osd[osd]||[]).map(pg_name => prev_weights[pg_name] ? `add_${pg_name} - del_${pg_name}` : pg_name).join(' + ');
        const osd_weight = all_weights[osd]*3/total_weight*pg_count - (prev_pg_per_osd[osd]||[]).length;
        lp += osd_sum + ' <= ' + Math.round(osd_weight) + ';\n';
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
    lp += 'int '+pg_vars.join(', ')+';\n';
    // Solve it
    const lp_result = await lp_solve(lp);
    if (!lp_result)
    {
        console.log(lp);
        throw new Error('Problem is infeasible - is it a bug?');
    }
    // Generate the new distribution
    const weights = { ...prev_weights };
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
        else
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
        for (let j = 0; j < 3; j++)
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
        space: pg_list_space_efficiency(new_pgs, osd_tree),
        total_space: total_weight,
    };
}

function print_change_stats(retval)
{
    const new_pgs = retval.int_pgs;
    const prev_int_pgs = retval.prev_pgs;
    for (let i = 0; i < new_pgs.length; i++)
    {
        if (new_pgs[i].join('_') != prev_int_pgs[i].join('_'))
        {
            console.log("pg "+i+": "+prev_int_pgs[i].join(' ')+" -> "+new_pgs[i].join(' '));
        }
    }
    console.log(
        "Data movement: "+retval.differs+" pgs, "+
        retval.osd_differs+" pg*osds = "+Math.round(retval.osd_differs / prev_int_pgs.length / 3 * 10000)/100+" %"
    );
    console.log(
        "Total space: "+Math.round(retval.space*3*100)/100+" TB, space efficiency: "+
        Math.round(retval.space*3/retval.total_space*10000)/100+" %"
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

function all_combinations(osd_tree, count, ordered)
{
    const hosts = Object.keys(osd_tree).sort();
    const osds = Object.keys(osd_tree).reduce((a, c) => { a[c] = Object.keys(osd_tree[c]).sort(); return a; }, {});
    let h = [ 0, 1, 2 ];
    let o = [ 0, 0, 0 ];
    const r = [];
    while (!count || count < 0 || r.length < count)
    {
        let inc;
        if (h[2] != h[1] && h[2] != h[0] && h[1] != h[0])
        {
            r.push(h.map((host_idx, i) => osds[hosts[host_idx]][o[i]]));
            inc = 2;
            while (inc >= 0)
            {
                o[inc]++;
                if (o[inc] >= osds[hosts[h[inc]]].length)
                {
                    o[inc] = 0;
                    inc--;
                }
                else
                {
                    break;
                }
            }
        }
        else
        {
            inc = -1;
        }
        if (inc < 0)
        {
            // no osds left in current host combination, select the next one
            o = [ 0, 0, 0 ];
            h[2]++;
            if (h[2] >= hosts.length)
            {
                h[1]++;
                h[2] = ordered ? h[1]+1 : 0;
                if ((ordered ? h[2] : h[1]) >= hosts.length)
                {
                    h[0]++;
                    h[1] = ordered ? h[0]+1 : 0;
                    h[2] = ordered ? h[1]+1 : 0;
                    if ((ordered ? h[2] : h[0]) >= hosts.length)
                    {
                        break;
                    }
                }
            }
        }
    }
    return r;
}

function pg_weights_space_efficiency(weights, pg_count, osd_tree)
{
    const per_osd = {};
    for (const pg_name in weights)
    {
        for (const osd of pg_name.substr(3).split(/_/))
        {
            per_osd[osd] = (per_osd[osd]||0) + weights[pg_name];
        }
    }
    return pg_per_osd_space_efficiency(per_osd, pg_count, osd_tree);
}

function pg_list_space_efficiency(pgs, osd_tree)
{
    const per_osd = {};
    for (const pg of pgs)
    {
        for (const osd of pg)
        {
            per_osd[osd] = (per_osd[osd]||0) + 1;
        }
    }
    return pg_per_osd_space_efficiency(per_osd, pgs.length, osd_tree);
}

function pg_per_osd_space_efficiency(per_osd, pg_count, osd_tree)
{
    const all_weights = Object.assign.apply({}, Object.values(osd_tree));
    // each PG gets randomly selected in 1/N cases
    // => there are x PGs per OSD
    // => an OSD is selected in x/N cases
    // => total space * x/N <= OSD weight
    // => total space <= OSD weight * N/x
    let space;
    for (let osd in per_osd)
    {
        const space_estimate = all_weights[osd] * pg_count / per_osd[osd];
        if (space == null || space > space_estimate)
        {
            space = space_estimate;
        }
    }
    return space;
}

module.exports = {
    optimize_initial,
    optimize_change,
    print_change_stats,
    pg_weights_space_efficiency,
    pg_list_space_efficiency,
    pg_per_osd_space_efficiency,

    lp_solve,
    make_single,
    make_int_pgs,
    get_int_pg_weights,
    align_pgs,
    all_combinations,
};
