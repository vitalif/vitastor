// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

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
/*    100: {
        1: 2.72800,
    },
    200: {
        2: 2.72900,
    },
    300: {
        3: 1.87000,
    },
    400: {
        4: 1.87000,
    },
    500: {
        5: 3.63869,
    },*/
};

const crush_tree = [
    { level: 1, children: [
        { level: 2, children: [
            { level: 3, id: 1, size: 3 },
            { level: 3, id: 2, size: 2 },
        ] },
        { level: 2, children: [
            { level: 3, id: 3, size: 4 },
            { level: 3, id: 4, size: 4 },
        ] },
    ] },
    { level: 1, children: [
        { level: 2, children: [
            { level: 3, id: 5, size: 4 },
            { level: 3, id: 6, size: 1 },
        ] },
        { level: 2, children: [
            { level: 3, id: 7, size: 3 },
            { level: 3, id: 8, size: 5 },
        ] },
    ] },
    { level: 1, children: [
        { level: 2, children: [
            { level: 3, id: 9, size: 5 },
            { level: 3, id: 10, size: 2 },
        ] },
        { level: 2, children: [
            { level: 3, id: 11, size: 3 },
            { level: 3, id: 12, size: 3 },
        ] },
    ] },
];

async function run()
{
    let res;

    // Test: add 1 OSD of almost the same size. Ideal data movement could be 1/12 = 8.33%. Actual is ~13%
    // Space efficiency is ~99% in all cases.

    console.log('256 PGs, size=2');
    res = await LPOptimizer.optimize_initial({ osd_tree, pg_size: 2, pg_count: 256 });
    LPOptimizer.print_change_stats(res, false);
    console.log('\nAdding osd.8');
    osd_tree[500][8] = 3.58589;
    res = await LPOptimizer.optimize_change({ prev_pgs: res.int_pgs, osd_tree, pg_size: 2 });
    LPOptimizer.print_change_stats(res, false);
    console.log('\nRemoving osd.8');
    delete osd_tree[500][8];
    res = await LPOptimizer.optimize_change({ prev_pgs: res.int_pgs, osd_tree, pg_size: 2 });
    LPOptimizer.print_change_stats(res, false);

    console.log('\n256 PGs, size=3');
    res = await LPOptimizer.optimize_initial({ osd_tree, pg_size: 3, pg_count: 256 });
    LPOptimizer.print_change_stats(res, false);
    console.log('\nAdding osd.8');
    osd_tree[500][8] = 3.58589;
    res = await LPOptimizer.optimize_change({ prev_pgs: res.int_pgs, osd_tree, pg_size: 3 });
    LPOptimizer.print_change_stats(res, false);
    console.log('\nRemoving osd.8');
    delete osd_tree[500][8];
    res = await LPOptimizer.optimize_change({ prev_pgs: res.int_pgs, osd_tree, pg_size: 3 });
    LPOptimizer.print_change_stats(res, false);

    console.log('\n256 PGs, size=3, failure domain=rack');
    res = await LPOptimizer.optimize_initial({ osd_tree: LPOptimizer.flatten_tree(crush_tree, {}, 1, 3), pg_size: 3, pg_count: 256 });
    LPOptimizer.print_change_stats(res, false);
}

run().catch(console.error);
