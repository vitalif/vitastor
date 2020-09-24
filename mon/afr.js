// Functions to calculate Annualized Failure Rate of your cluster
// if you know AFR of your drives, number of drives, expected rebalance time
// and replication factor
// License: VNPL-1.0 (see README.md for details)

module.exports = {
    cluster_afr,
    failure_rate,
    c_n_k,
};

console.log(100*cluster_afr(100, 0.03, 10, 3), '%');
console.log(100*cluster_afr(1000, 0.03, 1, 3), '%');
console.log(100*cluster_afr(5, 0.1, 1, 2), '%');
console.log(100*cluster_afr(14, 0.01, 1, 2), '%');
console.log(100*cluster_afr(100, 0.03, 1, 2), '%');

// Estimate AFR of the cluster (not taking failure domains into account)
// n - number of drives
// afr - annualized failure rate of a single drive
// l - expected rebalance time in days after a single drive failure
// k - replication factor / number of drives that must fail at the same time for the cluster to fail
function cluster_afr(n, afr, l, k)
{
    let p = 0;
    for (let i = 0; i < n-(k-1); i++)
    {
        p += afr * (1-afr)**i * failure_rate(n-i-1, afr*l/365, k-1);
    }
    return p;
}

// Probability of at least <f> failures in a cluster with <n> drives with AFR=<a>
function failure_rate(n, a, f)
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
