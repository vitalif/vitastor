// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

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

// osd_weights = { [id]: weight }
async function optimize_initial({ osd_weights, combinator, pg_count, pg_size = 3, pg_minsize = 2, parity_space = 1, ordered = false })
{
    if (!pg_count || !osd_weights)
    {
        return null;
    }
    const total_weight = Object.values(osd_weights).reduce((a, c) => Number(a) + Number(c), 0);
    const all_pgs = Object.values(make_cyclic(combinator.random_combinations(), parity_space));
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
    let pg_effsize = all_pgs.reduce((a, c) => Math.max(a, c.filter(e => e != NO_OSD).length), 0);
    pg_effsize = Math.min(pg_minsize, pg_effsize) + Math.max(0, Math.min(pg_size, pg_effsize) - pg_minsize) * parity_space;
    let lp = '';
    lp += "max: "+all_pgs.map(pg => 'pg_'+pg.join('_')).join(' + ')+";\n";
    for (const osd in pg_per_osd)
    {
        if (osd !== NO_OSD)
        {
            let osd_pg_count = osd_weights[osd]/total_weight*pg_effsize*pg_count;
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
    const int_pgs = make_int_pgs(lp_result.vars, pg_count, ordered);
    const eff = pg_list_space_efficiency(int_pgs, osd_weights, pg_minsize, parity_space);
    const res = {
        score: lp_result.score,
        weights: lp_result.vars,
        int_pgs,
        space: eff * pg_effsize,
        total_space: total_weight,
    };
    return res;
}

function make_cyclic(pgs, parity_space)
{
    if (parity_space > 1)
    {
        for (const pg in pgs)
        {
            for (let i = 1; i < pg.size; i++)
            {
                const cyclic = [ ...pg.slice(i), ...pg.slice(0, i) ];
                pgs['pg_'+cyclic.join('_')] = cyclic;
            }
        }
    }
    return pgs;
}

function shuffle(array)
{
    for (let i = array.length - 1, j, x; i > 0; i--)
    {
        j = Math.floor(Math.random() * (i + 1));
        x = array[i];
        array[i] = array[j];
        array[j] = x;
    }
}

function make_int_pgs(weights, pg_count, round_robin)
{
    const total_weight = Object.values(weights).reduce((a, c) => Number(a) + Number(c), 0);
    let int_pgs = [];
    let pg_left = pg_count;
    let weight_left = total_weight;
    for (const pg_name in weights)
    {
        let cur_pg = pg_name.substr(3).split('_');
        let n = Math.round(weights[pg_name] / weight_left * pg_left);
        for (let i = 0; i < n; i++)
        {
            int_pgs.push([ ...cur_pg ]);
            if (round_robin)
            {
                cur_pg.push(cur_pg.shift());
            }
        }
        weight_left -= weights[pg_name];
        pg_left -= n;
    }
    shuffle(int_pgs);
    return int_pgs;
}

function calc_intersect_weights(old_pg_size, pg_size, pg_count, prev_weights, all_pgs, ordered)
{
    const move_weights = {};
    if ((1 << old_pg_size) < pg_count)
    {
        const intersect = {};
        for (const pg_name in prev_weights)
        {
            const pg = pg_name.substr(3).split(/_/);
            for (let omit = 1; omit < (1 << old_pg_size); omit++)
            {
                let pg_omit = [ ...pg ];
                let intersect_count = old_pg_size;
                for (let i = 0; i < old_pg_size; i++)
                {
                    if (omit & (1 << i))
                    {
                        pg_omit[i] = '';
                        intersect_count--;
                    }
                }
                if (!ordered)
                    pg_omit = pg_omit.filter(n => n).sort();
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
                        pg_omit[i] = '';
                }
                if (!ordered)
                    pg_omit = pg_omit.filter(n => n).sort();
                pg_omit = pg_omit.join(':');
                max_int = Math.max(max_int, intersect[pg_omit] || 0);
            }
            move_weights['pg_'+pg.join('_')] = pg_size-max_int;
        }
    }
    else
    {
        const prev_pg_hashed = Object.keys(prev_weights).map(pg_name => pg_name
            .substr(3).split(/_/).reduce((a, c, i) => { a[c] = i+1; return a; }, {}));
        for (const pg of all_pgs)
        {
            if (!prev_weights['pg_'+pg.join('_')])
            {
                let max_int = 0;
                for (const prev_hash of prev_pg_hashed)
                {
                    const intersect_count = ordered
                        ? pg.reduce((a, osd, i) => a + (prev_hash[osd] == 1+i ? 1 : 0), 0)
                        : pg.reduce((a, osd) => a + (prev_hash[osd] ? 1 : 0), 0);
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

// Try to minimize data movement
async function optimize_change({ prev_pgs: prev_int_pgs, osd_weights, combinator, pg_size = 3, pg_minsize = 2, parity_space = 1, ordered = false })
{
    if (!osd_weights)
    {
        return null;
    }
    // FIXME: use parity_chunks with parity_space instead of pg_minsize
    let all_pgs = make_cyclic(combinator.random_combinations(), parity_space);
    let pg_effsize = Object.values(all_pgs).reduce((a, c) => Math.max(a, c.filter(e => e != NO_OSD).length), 0);
    pg_effsize = Math.min(pg_minsize, pg_effsize) + Math.max(0, Math.min(pg_size, pg_effsize) - pg_minsize) * parity_space;
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
    const old_pg_size = prev_int_pgs[0].length;
    // Get all combinations
    if (old_pg_size == pg_size)
    {
        const still_valid = combinator.check_combinations(Object.keys(prev_weights).map(pg_name => pg_name.substr(3).split('_')));
        for (const pg of still_valid)
        {
            all_pgs['pg_'+pg.join('_')] = pg;
        }
    }
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
    const move_weights = calc_intersect_weights(old_pg_size, pg_size, pg_count, prev_weights, all_pgs, ordered);
    // Calculate total weight - old PG weights
    const all_pg_names = all_pgs.map(pg => 'pg_'+pg.join('_'));
    const all_pgs_hash = all_pg_names.reduce((a, c) => { a[c] = true; return a; }, {});
    const total_weight = Object.values(osd_weights).reduce((a, c) => Number(a) + Number(c), 0);
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
            const osd_pg_count = osd_weights[osd]*pg_effsize/total_weight*pg_count - rm_osd_pg_count;
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
    }
    if (ordered)
    {
        for (let i = 0; i < pg_count; i++)
        {
            for (let j = 0; j < pg_size; j++)
            {
                if (new_pgs[i][j] != prev_int_pgs[i][j])
                {
                    osd_differs++;
                }
            }
        }
    }
    else
    {
        for (let i = 0; i < pg_count; i++)
        {
            const old_map = prev_int_pgs[i].reduce((a, c) => { a[c] = (a[c]|0) + 1; return a; }, {});
            for (let j = 0; j < pg_size; j++)
            {
                if ((0|old_map[new_pgs[i][j]]) > 0)
                {
                    old_map[new_pgs[i][j]]--;
                }
                else
                {
                    osd_differs++;
                }
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
        space: pg_effsize * pg_list_space_efficiency(new_pgs, osd_weights, pg_minsize, parity_space),
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

    lp_solve,
    make_int_pgs,
    align_pgs,
};
