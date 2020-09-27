// Functions to calculate Annualized Failure Rate of your cluster
// if you know AFR of your drives, number of drives, expected rebalance time
// and replication factor
// License: VNPL-1.0 (see README.md for details)

module.exports = {
    cluster_afr_domains,
    cluster_afr,
    failure_rate,
    c_n_k,
};

console.log(100*cluster_afr(100, 0.03, 10, 3), '%');
console.log(100*cluster_afr(1000, 0.03, 1, 3), '%');
console.log(100*cluster_afr(5, 0.1, 1, 2), '%');
console.log(100*cluster_afr(14, 0.01, 1, 2), '%');
console.log(100*cluster_afr(100, 0.03, 1, 2), '%');
console.log(100*cluster_afr_domains(10, 10, 0.03, 0.08, 8000, 0.02, 10/8, 2, 70), '%');
console.log(100*cluster_afr_domains(10, 100, 0.03, 0.08, 8000, 0.02, 10/8, 2, 70), '%');

// Estimate AFR of the cluster, taking failure domains and server failures into account
// n_nodes - number of failure domains
// drives_per_node - number of drives in a failure domain
// afr_drive - AFR of a single drive
// afr_node - AFR of a single failure domain (server) not counting its drives' AFR
// osd_size - drive size in GB
// rebalance_speed_per_drive - rebalance speed per drive in GB/s
// network_speed - network speed in GB/s
// k - replication factor
// pg_per_osd - average PG per osd
function cluster_afr_domains(n_nodes, drives_per_node, afr_drive, afr_node, osd_size, rebalance_speed_per_drive, network_speed, k, pg_per_osd = 100)
{
    const any_node_drive_afr = failure_rate(drives_per_node, afr_drive, 1);
    // Logic behind this: one failed drive is expected to backfill to (pg_per_osd / 2) peers if we expect 50% collisions
    // Which in fact is more correct for Ceph than for Vitastor because Vitastor's PG distribution isn't usually random!
    const drive_rebalance_speed = Math.min(rebalance_speed_per_drive * Math.min(drives_per_node, pg_per_osd / 2 / n_nodes), network_speed);
    const drive_rebalance_days = osd_size / (drive_rebalance_speed*n_nodes) / 86400;
    const node_rebalance_speed = Math.min(rebalance_speed_per_drive * Math.min(drives_per_node, pg_per_osd / 2 * drives_per_node / n_nodes), network_speed);
    const node_rebalance_days = osd_size*drives_per_node / (node_rebalance_speed*(n_nodes-1)) / 86400;
    let p = 0;
    for (let i = 0; i < n_nodes-(k-1); i++)
    {
        p += any_node_drive_afr * (1-any_node_drive_afr-afr_node)**i
            * failure_rate(n_nodes-i-1, (any_node_drive_afr+afr_node)*drive_rebalance_days/365, k-1);
        p += afr_node * (1-any_node_drive_afr-afr_node)**i
            * failure_rate(n_nodes-i-1, (any_node_drive_afr+afr_node)*node_rebalance_days/365, k-1);
    }
    return p;
}

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
