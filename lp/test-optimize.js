const LPOptimizer = require('./lp-optimizer.js');

const osd_tree = {
    100: {
        7: 3.63869,
    },
    300: {
        10: 3.46089,
        11: 3.46089,
        12: 3.46089,
    },
    400: {
        1: 3.49309,
        2: 3.49309,
        3: 3.49309,
    },
    500: {
        4: 3.58498,
//        8: 3.58589,
        9: 3.63869,
    },
    600: {
        5: 3.63869,
        6: 3.63869,
    },
};

async function run()
{
    // Test: add 1 OSD of almost the same size. Ideal data movement could be 1/12 = 8.33%. Actual is ~11.72%
    // Space efficiency is ~99.5% in both cases.
    let prev = await LPOptimizer.optimize_initial(osd_tree, 256);
    LPOptimizer.print_change_stats(prev);
    osd_tree[500][8] = 3.58589;
    let next = await LPOptimizer.optimize_change(prev.int_pgs, osd_tree);
    LPOptimizer.print_change_stats(next);
}

run().catch(console.error);
