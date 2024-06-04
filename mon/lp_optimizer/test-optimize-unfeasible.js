// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

const { compat } = require('./simple_pgs.js');
const LPOptimizer = require('./lp_optimizer.js');

const osd_tree = {
    100: {
        1: 0.1,
        2: 0.1,
        3: 0.1,
    },
    200: {
        4: 0.1,
        5: 0.1,
        6: 0.1,
    },
};

async function run()
{
    let res;
    console.log('256 PGs, 3+3 OSDs, size=2');
    res = await LPOptimizer.optimize_initial(compat({ osd_tree, pg_size: 2, pg_count: 256 }));
    LPOptimizer.print_change_stats(res, false);

    // Should NOT fail with the "unfeasible or unbounded" exception
    console.log('\nRemoving osd.2');
    delete osd_tree[100][2];
    res = await LPOptimizer.optimize_change(compat({ prev_pgs: res.int_pgs, osd_tree, pg_size: 2 }));
    LPOptimizer.print_change_stats(res, false);
}

run().catch(console.error);
