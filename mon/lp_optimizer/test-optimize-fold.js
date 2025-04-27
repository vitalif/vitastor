// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

const assert = require('assert');
const { fold_failure_domains, unfold_failure_domains, adjust_distribution } = require('./fold.js');
const DSL = require('./dsl_pgs.js');
const LPOptimizer = require('./lp_optimizer.js');
const stableStringify = require('../stable-stringify.js');

async function run()
{
    // Test run adjust_distribution
    console.log('adjust_distribution');
    const rand = [];
    for (let i = 0; i < 100; i++)
    {
        rand.push(1 + Math.floor(10*Math.random()));
        // or rand.push(0);
    }
    const adj = adjust_distribution({ 1: 1, 2: 1, 3: 1, 4: 1, 5: 1, 6: 1, 7: 1, 8: 1, 9: 1, 10: 1 }, rand);
    //console.log(rand.join(' '));
    console.log(rand.reduce((a, c) => { a[c] = (a[c]||0)+1; return a; }, {}));
    //console.log(adj.join(' '));
    console.log(adj.reduce((a, c) => { a[c] = (a[c]||0)+1; return a; }, {}));
    console.log('Movement: '+rand.reduce((a, c, i) => a+(rand[i] != adj[i] ? 1 : 0), 0)+'/'+rand.length);

    console.log('\nfold_failure_domains');
    console.log(JSON.stringify(fold_failure_domains(
        [
            { id: 1,       level: 'osd',  size: 1, parent: 'disk1' },
            { id: 2,       level: 'osd',  size: 2, parent: 'disk1' },
            { id: 'disk1', level: 'disk', parent: 'host1' },
            { id: 'host1', level: 'host', parent: 'dc1' },
            { id: 'dc1',   level: 'dc' },
        ],
        [ [ [ 'dc' ], [ 'host' ] ] ]
    ), 0, 2));

    console.log('\nfold_failure_domains empty rules');
    console.log(JSON.stringify(fold_failure_domains(
        [
            { id: 1,       level: 'osd',  size: 1, parent: 'disk1' },
            { id: 2,       level: 'osd',  size: 2, parent: 'disk1' },
            { id: 'disk1', level: 'disk', parent: 'host1' },
            { id: 'host1', level: 'host', parent: 'dc1' },
            { id: 'dc1',   level: 'dc' },
        ],
        []
    ), 0, 2));

    console.log('\noptimize_folded');
    // 5 DCs, 2 hosts per DC, 10 OSD per host
    const nodes = [];
    for (let i = 1; i <= 100; i++)
    {
        nodes.push({ id: i, level: 'osd', size: 1, parent: 'host'+(1+(0|((i-1)/10))) });
    }
    for (let i = 1; i <= 10; i++)
    {
        nodes.push({ id: 'host'+i, level: 'host', parent: 'dc'+(1+(0|((i-1)/2))) });
    }
    for (let i = 1; i <= 5; i++)
    {
        nodes.push({ id: 'dc'+i, level: 'dc' });
    }

    // Check rules
    const rules = DSL.parse_level_indexes({ dc: '112233', host: '123456' }, [ 'dc', 'host', 'osd' ]);
    assert.deepEqual(rules, [[],[["dc","=",1],["host","!=",[1]]],[["dc","!=",[1]]],[["dc","=",3],["host","!=",[3]]],[["dc","!=",[1,3]]],[["dc","=",5],["host","!=",[5]]]]);

    // Check tree folding
    const { nodes: folded_nodes, leaves: folded_leaves } = fold_failure_domains(nodes, rules);
    const expected_folded = [];
    const expected_leaves = {};
    for (let i = 1; i <= 10; i++)
    {
        expected_folded.push({ id: 100+i, name: 'host'+i, level: 'host', size: 10, parent: 'dc'+(1+(0|((i-1)/2))) });
        expected_leaves[100+i] = [ ...new Array(10).keys() ].map(k => ({ id: 10*(i-1)+k+1, level: 'osd', size: 1, parent: 'host'+i }));
    }
    for (let i = 1; i <= 5; i++)
    {
        expected_folded.push({ id: 'dc'+i, level: 'dc' });
    }
    assert.equal(stableStringify(folded_nodes), stableStringify(expected_folded));
    assert.equal(stableStringify(folded_leaves), stableStringify(expected_leaves));

    // Now optimise it
    console.log('1000 PGs, EC 112233');
    const leaf_weights = folded_nodes.reduce((a, c) => { if (Number(c.id)) { a[c.id] = c.size; } return a; }, {});
    let res = await LPOptimizer.optimize_initial({
        osd_weights: leaf_weights,
        combinator: new DSL.RuleCombinator(folded_nodes, rules, 10000, false),
        pg_size: 6,
        pg_count: 1000,
        ordered: false,
    });
    LPOptimizer.print_change_stats(res, false);
    assert.equal(res.space, 100, 'Initial distribution');

    const unfolded_res = { ...res };
    unfolded_res.int_pgs = unfold_failure_domains(res.int_pgs, null, folded_leaves);
    const osd_weights = nodes.reduce((a, c) => { if (Number(c.id)) { a[c.id] = c.size; } return a; }, {});
    unfolded_res.space = unfolded_res.pg_effsize * LPOptimizer.pg_list_space_efficiency(unfolded_res.int_pgs, osd_weights, 0, 1);
    LPOptimizer.print_change_stats(unfolded_res, false);
    assert.equal(res.space, 100, 'Initial distribution');
}

run().catch(console.error);
