// Interesting real-world example coming from Ceph with EC and compression enabled.
// EC parity chunks can't be compressed as efficiently as data chunks,
// thus they occupy more space (2.26x more space) in OSD object stores.
// This leads to really uneven OSD fill ratio in Ceph even when PGs are perfectly balanced.
// But we support this case with the "parity_space" parameter in optimize_initial()/optimize_change().

const LPOptimizer = require('./lp-optimizer.js');

const osd_tree = {
    ripper5: {
        osd0: 3.493144989013672,
        osd1: 3.493144989013672,
        osd2: 3.454082489013672,
        osd12: 3.461894989013672,
    },
    ripper7: {
        osd4: 3.638690948486328,
        osd5: 3.638690948486328,
        osd6: 3.638690948486328,
    },
    ripper4: {
        osd9: 3.4609375,
        osd10: 3.4609375,
        osd11: 3.4609375,
    },
    ripper6: {
        osd3: 3.5849609375,
        osd7: 3.5859336853027344,
        osd8: 3.638690948486328,
        osd13: 3.461894989013672
    },
};

const prev_pgs = [[12,7,5],[6,11,12],[3,6,9],[10,0,5],[2,5,13],[9,8,6],[3,4,12],[7,4,12],[12,11,13],[13,6,0],[4,13,10],[9,7,6],[7,10,0],[10,8,0],[3,10,2],[3,0,4],[6,13,0],[13,10,0],[13,10,5],[8,11,6],[3,9,2],[2,8,5],[8,9,5],[3,12,11],[0,7,4],[13,11,1],[11,3,12],[12,8,10],[7,5,12],[2,13,5],[7,11,0],[13,2,6],[0,6,8],[13,1,6],[0,13,4],[0,8,10],[4,10,0],[8,12,4],[8,12,9],[12,7,4],[13,9,5],[3,2,11],[1,9,7],[1,8,5],[5,12,9],[3,5,12],[2,8,10],[0,8,4],[1,4,11],[7,10,2],[12,13,5],[3,1,11],[7,1,4],[4,12,8],[7,0,9],[11,1,8],[3,0,5],[11,13,0],[1,13,5],[12,7,10],[12,8,4],[11,13,5],[0,11,6],[2,11,3],[13,1,11],[2,7,10],[7,10,12],[7,12,10],[12,11,5],[13,12,10],[2,3,9],[4,3,9],[13,2,5],[7,12,6],[12,10,13],[9,8,1],[13,1,5],[9,5,12],[5,11,7],[6,2,9],[8,11,6],[12,5,8],[6,13,1],[7,6,11],[2,3,6],[8,5,9],[1,13,6],[9,3,2],[7,11,1],[3,10,1],[0,11,7],[3,0,5],[1,3,6],[6,0,9],[3,11,4],[8,10,2],[13,1,9],[12,6,9],[3,12,9],[12,8,9],[7,5,0],[8,12,5],[0,11,3],[12,11,13],[0,7,11],[0,3,10],[1,3,11],[2,7,11],[13,2,6],[9,12,13],[8,2,4],[0,7,4],[5,13,0],[13,12,9],[1,9,8],[0,10,3],[3,5,10],[7,12,9],[2,13,4],[12,7,5],[9,2,7],[3,2,9],[6,2,7],[3,1,9],[4,3,2],[5,3,11],[0,7,6],[1,6,13],[7,10,2],[12,4,8],[13,12,6],[7,5,11],[6,2,3],[2,7,6],[2,3,10],[2,7,10],[11,12,6],[0,13,5],[10,2,4],[13,0,11],[7,0,6],[8,9,4],[8,4,11],[7,11,2],[3,4,2],[6,1,3],[7,2,11],[8,9,4],[11,4,8],[10,3,1],[2,10,13],[1,7,11],[13,11,12],[2,6,9],[10,0,13],[7,10,4],[0,11,13],[13,10,1],[7,5,0],[7,12,10],[3,1,4],[7,1,5],[3,11,5],[7,5,0],[1,3,5],[10,5,12],[0,3,9],[7,1,11],[11,8,12],[3,6,2],[7,12,9],[7,11,12],[4,11,3],[0,11,13],[13,2,5],[1,5,8],[0,11,8],[3,5,1],[11,0,6],[3,11,2],[11,8,12],[4,1,3],[10,13,4],[13,9,6],[2,3,10],[12,7,9],[10,0,4],[10,13,2],[3,11,1],[7,2,9],[1,7,4],[13,1,4],[7,0,6],[5,3,9],[10,0,7],[0,7,10],[3,6,10],[13,0,5],[8,4,1],[3,1,10],[2,10,13],[13,0,5],[13,10,2],[12,7,9],[6,8,10],[6,1,8],[10,8,1],[13,5,0],[5,11,3],[7,6,1],[8,5,9],[2,13,11],[10,12,4],[13,4,1],[2,13,4],[11,7,0],[2,9,7],[1,7,6],[8,0,4],[8,1,9],[7,10,12],[13,9,6],[7,6,11],[13,0,4],[1,8,4],[3,12,5],[10,3,1],[10,2,13],[2,4,8],[6,2,3],[3,0,10],[6,7,12],[8,12,5],[3,0,6],[13,12,10],[11,3,6],[9,0,13],[10,0,6],[7,5,2],[1,3,11],[7,10,2],[2,9,8],[11,13,12],[0,8,4],[8,12,11],[6,0,3],[1,13,4],[11,8,2],[12,3,6],[4,7,1],[7,6,12],[3,10,6],[0,10,7],[8,9,1],[0,10,6],[8,10,1]]
    .map(pg => pg.map(n => 'osd'+n));

const by_osd = {};

for (let i = 0; i < prev_pgs.length; i++)
{
    for (let j = 0; j < prev_pgs[i].length; j++)
    {
        by_osd[prev_pgs[i][j]] = by_osd[prev_pgs[i][j]] || [];
        by_osd[prev_pgs[i][j]][j] = (by_osd[prev_pgs[i][j]][j] || 0) + 1;
    }
}

/*

This set of PGs was balanced by hand, by heavily tuning OSD weights in Ceph:

{
  osd0: 4.2,
  osd1: 3.5,
  osd2: 3.45409,
  osd3: 4.5,
  osd4: 1.4,
  osd5: 1.4,
  osd6: 1.75,
  osd7: 4.5,
  osd8: 4.4,
  osd9: 2.2,
  osd10: 2.7,
  osd11: 2,
  osd12: 3.4,
  osd13: 3.4,
}

EC+compression is a nightmare in Ceph, yeah :))

To calculate the average ratio between data chunks and parity chunks we
calculate the number of PG chunks for each chunk role for each OSD:

{
  osd12: [ 18, 22, 17 ],
  osd7: [ 35, 22, 8 ],
  osd5: [ 6, 17, 27 ],
  osd6: [ 13, 12, 28 ],
  osd11: [ 13, 26, 20 ],
  osd3: [ 30, 20, 10 ],
  osd9: [ 8, 12, 26 ],
  osd10: [ 15, 23, 20 ],
  osd0: [ 22, 22, 14 ],
  osd2: [ 22, 16, 16 ],
  osd13: [ 29, 19, 13 ],
  osd8: [ 20, 18, 12 ],
  osd4: [ 8, 10, 28 ],
  osd1: [ 17, 17, 17 ]
}

And now we can pick a pair of OSDs and determine the ratio by solving the following:

osd5 = 23*X + 27*Y = 3249728140
osd13 = 48*X + 13*Y = 2991675992

=>

osd5 - 27/13*osd13 = 23*X - 27/13*48*X = -76.6923076923077*X = -2963752766.46154

=>

X = 38644720.1243731
Y = (osd5-23*X)/27 = 87440725.0792377
Y/X = 2.26268232239284 ~= 2.26

Which means that parity chunks are compressed ~2.26 times worse than data chunks.

Fine, let's try to optimize for it.

*/

async function run()
{
    const all_weights = Object.assign({}, ...Object.values(osd_tree));
    const total_weight = Object.values(all_weights).reduce((a, c) => Number(a) + Number(c), 0);
    const eff = LPOptimizer.pg_list_space_efficiency(prev_pgs, all_weights, 2, 2.26);
    const orig = eff*4.26 / total_weight;
    console.log('Original efficiency was: '+Math.round(orig*10000)/100+' %');

    let prev = await LPOptimizer.optimize_initial({ osd_tree, pg_size: 3, pg_count: 256, parity_space: 2.26 });
    LPOptimizer.print_change_stats(prev);

    let next = await LPOptimizer.optimize_change({ prev_pgs, osd_tree, pg_size: 3, max_combinations: 10000, parity_space: 2.26 });
    LPOptimizer.print_change_stats(next);
}

run().catch(console.error);
