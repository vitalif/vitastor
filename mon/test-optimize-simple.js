// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

const LPOptimizer = require('./lp-optimizer.js');

async function run()
{
    const osd_tree = { a: { 1: 1 }, b: { 2: 1 }, c: { 3: 1 } };
    let res;

    console.log('16 PGs, size=3');
    res = await LPOptimizer.optimize_initial({ osd_tree, pg_size: 3, pg_count: 16 });
    LPOptimizer.print_change_stats(res, false);

    console.log('\nReduce PG size to 2');
    res = await LPOptimizer.optimize_change({ prev_pgs: res.int_pgs.map(pg => pg.slice(0, 2)), osd_tree, pg_size: 2 });
    LPOptimizer.print_change_stats(res, false);

    console.log('\nRemove OSD 3');
    delete osd_tree['c'];
    res = await LPOptimizer.optimize_change({ prev_pgs: res.int_pgs, osd_tree, pg_size: 2 });
    LPOptimizer.print_change_stats(res, false);
}

run().catch(console.error);
