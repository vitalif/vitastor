// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

const LPOptimizer = require('./lp-optimizer.js');

const osd_tree = {
    100: { 1: 1 },
    200: { 2: 1 },
    300: { 3: 1 },
};

async function run()
{
    let res;
    console.log('16 PGs, size=3');
    res = await LPOptimizer.optimize_initial({ osd_tree, pg_size: 3, pg_count: 16 });
    LPOptimizer.print_change_stats(res, false);
    console.log('\nChanging size to 2');
    res = await LPOptimizer.optimize_change({ prev_pgs: res.int_pgs, osd_tree, pg_size: 2 });
    LPOptimizer.print_change_stats(res, false);
    if (res.space < 3*14/16)
    {
        throw new Error('Redistribution failed');
    }
}

run().catch(console.error);
