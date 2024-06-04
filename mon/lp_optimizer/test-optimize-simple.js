// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

const { compat } = require('./simple_pgs.js');
const LPOptimizer = require('./lp_optimizer.js');

async function run()
{
    const osd_tree = {
        100: { 1: 1 },
        200: { 2: 1 },
        300: { 3: 1 },
    };

    let res;

    console.log('16 PGs, size=3');
    res = await LPOptimizer.optimize_initial(compat({ osd_tree, pg_size: 3, pg_count: 16, ordered: false }));
    LPOptimizer.print_change_stats(res, false);
    assert(res.space == 3, 'Initial distribution');
    console.log('\nChange size to 2');
    res = await LPOptimizer.optimize_change(compat({ prev_pgs: res.int_pgs, osd_tree, pg_size: 2, ordered: false }));
    LPOptimizer.print_change_stats(res, false);
    assert(res.space >= 3*14/16 && res.osd_differs == 0, 'Redistribution');
    console.log('\nRemove OSD 3');
    const no3_tree = { ...osd_tree };
    delete no3_tree['300'];
    res = await LPOptimizer.optimize_change(compat({ prev_pgs: res.int_pgs, osd_tree: no3_tree, pg_size: 2, ordered: false }));
    LPOptimizer.print_change_stats(res, false);
    assert(res.space == 2, 'Redistribution after OSD removal');

    console.log('\n16 PGs, size=3, ordered');
    res = await LPOptimizer.optimize_initial(compat({ osd_tree, pg_size: 3, pg_count: 16, ordered: true }));
    LPOptimizer.print_change_stats(res, false);
    assert(res.space == 3, 'Initial distribution');
    console.log('\nChange size to 2, ordered');
    res = await LPOptimizer.optimize_change(compat({ prev_pgs: res.int_pgs, osd_tree, pg_size: 2, ordered: true }));
    LPOptimizer.print_change_stats(res, false);
    assert(res.space >= 3*14/16 && res.osd_differs < 8, 'Redistribution');
}

function assert(cond, txt)
{
    if (!cond)
    {
        throw new Error((txt||'test')+' failed');
    }
}

run().catch(console.error);
