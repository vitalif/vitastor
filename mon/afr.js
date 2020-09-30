// Functions to calculate Annualized Failure Rate of your cluster
// if you know AFR of your drives, number of drives, expected rebalance time
// and replication factor
// License: VNPL-1.0 (see README.md for details)

const { sprintf } = require('sprintf-js');

module.exports = {
    cluster_afr_fullmesh,
    failure_rate_fullmesh,
    cluster_afr_pgs,
    cluster_afr_pgs_ec,
    c_n_k,
};

console.log('4 nodes with 3 4TB drives, capable to backfill at 100 MB/s, drive AFR 3%, 2 replicas, 1 PG per OSD');
console.log(sprintf("%.7f%%", 100*cluster_afr_pgs(4, 3, 0.03, 4000, 0.1, 2, 1)));
console.log('4 nodes with 3 4TB drives, capable to backfill at 100 MB/s, drive AFR 3%, node AFR 5%, 2 replicas, 1 PG per OSD');
console.log(sprintf("%.7f%%", 100*cluster_afr_pgs_hosts(4, 3, 0.03, 0.05, 4000, 0.1, 2, 1)));
console.log('4 nodes with 3 4TB drives, capable to backfill at 100 MB/s, drive AFR 3%, EC 2+1, 1 PG per OSD');
console.log(sprintf("%.7f%%", 100*cluster_afr_pgs_ec(4, 3, 0.03, 4000, 0.1, 3, 2, 1)));
console.log('10 nodes with 10 8TB drives, capable to backfill at 20 MB/s, drive AFR 10%, 2 replicas, 1 PG per OSD');
console.log(sprintf("%.7f%%", 100*cluster_afr_pgs(10, 10, 0.1, 8000, 0.02, 2, 1)));
console.log('10 nodes with 10 8TB drives, capable to backfill at 20 MB/s, drive AFR 10%, node AFR 5%, 2 replicas, 1 PG per OSD');
console.log(sprintf("%.7f%%", 100*cluster_afr_pgs_hosts(10, 10, 0.1, 0.05, 8000, 0.02, 2, 1)));
console.log('10 nodes with 10 8TB drives, capable to backfill at 20 MB/s, drive AFR 10%, 3 replicas, 1 PG per OSD');
console.log(sprintf("%.7f%%", 100*cluster_afr_pgs(10, 10, 0.1, 8000, 0.02, 3, 1)));
console.log('10 nodes with 10 8TB drives, capable to backfill at 20 MB/s, drive AFR 10%, node AFR 5%, 3 replicas, 1 PG per OSD');
console.log(sprintf("%.7f%%", 100*cluster_afr_pgs_hosts(10, 10, 0.1, 0.05, 8000, 0.02, 3, 1)));
console.log('10 nodes with 10 8TB drives, capable to backfill at 20 MB/s, drive AFR 10%, 3 replicas, 100 PG per OSD');
console.log(sprintf("%.7f%%", 100*cluster_afr_pgs(10, 10, 0.1, 8000, 0.02, 3, 100)));
console.log('10 nodes with 10 8TB drives, capable to backfill at 20 MB/s, drive AFR 10%, node AFR 5%, 3 replicas, 100 PG per OSD');
console.log(sprintf("%.7f%%", 100*cluster_afr_pgs_hosts(10, 10, 0.1, 0.05, 8000, 0.02, 3, 100)));

/******** "FULL MESH": ASSUME EACH OSD COMMUNICATES WITH ALL OTHER OSDS ********/

// Estimate AFR of the cluster
// n - number of drives
// afr - annualized failure rate of a single drive
// l - expected rebalance time in days after a single drive failure
// k - replication factor / number of drives that must fail at the same time for the cluster to fail
function cluster_afr_fullmesh(n, afr, l, k)
{
    return 1 - (1 - afr * failure_rate_fullmesh(n-(k-1), afr*l/365, k-1)) ** (n-(k-1));
}

// Probability of at least <f> failures in a cluster with <n> drives with AFR=<a>
function failure_rate_fullmesh(n, a, f)
{
    if (f <= 0)
    {
        return (1-a)**n;
    }
    let p = 1;
    for (let i = 0; i < f; i++)
    {
        p -= c_n_k(n, i) * (1-a)**(n-i) * a**i;
    }
    return p;
}

/******** PGS: EACH OSD ONLY COMMUNICATES WITH <pgs> OTHER OSDs ********/

// <n> hosts of <m> drives of <capacity> GB, each able to backfill at <speed> GB/s,
// <k> replicas, <pgs> unique peer PGs per OSD
//
// For each of n*m drives: P(drive fails in a year) * P(any of its peers fail in <l*365> next days).
// More peers per OSD increase rebalance speed (more drives work together to resilver).
// At the same time, more peers per OSD increase probability of any of them to fail!
//
// Probability of all except one drives in a replica group to fail is (AFR^(k-1)).
// So with <x> PGs it becomes ~ (x * (AFR*L/365)^(k-1)). Interesting but reasonable consequence
// is that, with k=2, total failure rate doesn't depend on number of peers per OSD,
// because it gets increased linearly by increased number of peers to fail
// and decreased linearly by reduced rebalance time.
function cluster_afr_pgs(n, m, afr, capacity, speed, k, pgs)
{
    pgs = Math.min(pgs, (n-1)*m/(k-1));
    const l = capacity/pgs/speed/86400/365;
    return 1 - (1 - afr * (1-(1-(afr*l)**(k-1))**pgs)) ** (n*m);
}

function cluster_afr_pgs_ec(n, m, afr, capacity, speed, ec_total, ec_data, pgs)
{
    pgs = Math.min(pgs, (n-1)*m/(ec_total-1));
    const l = capacity/pgs/speed/86400/365;
    return 1 - (1 - afr * (1-(1-failure_rate_fullmesh(ec_total-1, afr*l, ec_total-ec_data))**pgs)) ** (n*m);
}

// Same as above, but also take server failures into account
function cluster_afr_pgs_hosts(n, m, afr_drive, afr_host, capacity, speed, k, pgs)
{
    let otherhosts = Math.min(pgs, (n-1)/(k-1));
    pgs = Math.min(pgs, (n-1)*m/(k-1));
    let pgh = Math.min(pgs*m, (n-1)*m/(k-1));
    const ld = capacity/pgs/speed/86400/365;
    const lh = m*capacity/pgs/speed/86400/365;
    const p1 = ((afr_drive+afr_host*pgs/otherhosts)*lh);
    const p2 = ((afr_drive+afr_host*pgs/otherhosts)*ld);
    return 1 - ((1 - afr_host * (1-(1-p1**(k-1))**pgh)) ** n) *
        ((1 - afr_drive * (1-(1-p2**(k-1))**pgs)) ** (n*m));
}

function cluster_afr_pgs_ec_hosts(n, m, afr_drive, afr_host, capacity, speed, ec_total, ec_data, pgs)
{
    let otherhosts = Math.min(pgs, (n-1)/(ec_total-1));
    pgs = Math.min(pgs, (n-1)*m/(ec_total-1));
    let pgh = Math.min(pgs*m, (n-1)*m/(ec_total-1));
    const ld = capacity/pgs/speed/86400/365;
    const lh = m*capacity/pgs/speed/86400/365;
    const p1 = ((afr_drive+afr_host*pgs/otherhosts)*lh);
    const p2 = ((afr_drive+afr_host*pgs/otherhosts)*ld);
    return 1 - ((1 - afr_host * (1-(1-failure_rate_fullmesh(ec_total-1, p1, ec_total-ec_data))**pgh)) ** n) *
        ((1 - afr_drive * (1-(1-failure_rate_fullmesh(ec_total-1, p2, ec_total-ec_data))**pgs)) ** (n*m));
}

/******** UTILITY ********/

// Combination count
function c_n_k(n, k)
{
    let r = 1;
    for (let i = 0; i < k; i++)
    {
        r *= (n-i) / (i+1);
    }
    return r;
}
