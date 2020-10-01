// Functions to calculate Annualized Failure Rate of your cluster
// if you know AFR of your drives, number of drives, expected rebalance time
// and replication factor
// License: VNPL-1.0 (see README.md for details)

const { sprintf } = require('sprintf-js');

module.exports = {
    cluster_afr_fullmesh,
    failure_rate_fullmesh,
    cluster_afr,
    print_cluster_afr,
    c_n_k,
};

print_cluster_afr({ n_hosts: 4, n_drives: 6, afr_drive: 0.03, afr_host: 0.05, capacity: 4000, speed: 0.1, replicas: 2 });
print_cluster_afr({ n_hosts: 4, n_drives: 3, afr_drive: 0.03, capacity: 4000, speed: 0.1, replicas: 2 });
print_cluster_afr({ n_hosts: 4, n_drives: 3, afr_drive: 0.03, afr_host: 0.05, capacity: 4000, speed: 0.1, replicas: 2 });
print_cluster_afr({ n_hosts: 4, n_drives: 3, afr_drive: 0.03, capacity: 4000, speed: 0.1, ec: [ 2, 1 ] });
print_cluster_afr({ n_hosts: 4, n_drives: 3, afr_drive: 0.03, afr_host: 0.05, capacity: 4000, speed: 0.1, ec: [ 2, 1 ] });
print_cluster_afr({ n_hosts: 10, n_drives: 10, afr_drive: 0.1, capacity: 8000, speed: 0.02, replicas: 2 });
print_cluster_afr({ n_hosts: 10, n_drives: 10, afr_drive: 0.1, afr_host: 0.05, capacity: 8000, speed: 0.02, replicas: 2 });
print_cluster_afr({ n_hosts: 10, n_drives: 10, afr_drive: 0.1, capacity: 8000, speed: 0.02, replicas: 3 });
print_cluster_afr({ n_hosts: 10, n_drives: 10, afr_drive: 0.1, afr_host: 0.05, capacity: 8000, speed: 0.02, replicas: 3 });
print_cluster_afr({ n_hosts: 10, n_drives: 10, afr_drive: 0.1, capacity: 8000, speed: 0.02, replicas: 3, pgs: 100 });
print_cluster_afr({ n_hosts: 10, n_drives: 10, afr_drive: 0.1, afr_host: 0.05, capacity: 8000, speed: 0.02, replicas: 3, pgs: 100 });
print_cluster_afr({ n_hosts: 10, n_drives: 10, afr_drive: 0.1, afr_host: 0.05, capacity: 8000, speed: 0.02, replicas: 3, pgs: 100, degraded_replacement: 1 });

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
// More peers per OSD increase rebalance speed (more drives work together to resilver) if you
// let them finish rebalance BEFORE replacing the failed drive.
// At the same time, more peers per OSD increase probability of any of them to fail!
//
// Probability of all except one drives in a replica group to fail is (AFR^(k-1)).
// So with <x> PGs it becomes ~ (x * (AFR*L/365)^(k-1)). Interesting but reasonable consequence
// is that, with k=2, total failure rate doesn't depend on number of peers per OSD,
// because it gets increased linearly by increased number of peers to fail
// and decreased linearly by reduced rebalance time.
function cluster_afr_pgs({ n_hosts, n_drives, afr_drive, capacity, speed, replicas, pgs = 1, degraded_replacement })
{
    pgs = Math.min(pgs, (n_hosts-1)*n_drives/(replicas-1));
    const l = capacity/(degraded_replacement ? 1 : pgs)/speed/86400/365;
    return 1 - (1 - afr_drive * (1-(1-(afr_drive*l)**(replicas-1))**pgs)) ** (n_hosts*n_drives);
}

function cluster_afr_pgs_ec({ n_hosts, n_drives, afr_drive, capacity, speed, ec: [ ec_data, ec_parity ], pgs = 1, degraded_replacement })
{
    const ec_total = ec_data+ec_parity;
    pgs = Math.min(pgs, (n_hosts-1)*n_drives/(ec_total-1));
    const l = capacity/(degraded_replacement ? 1 : pgs)/speed/86400/365;
    return 1 - (1 - afr_drive * (1-(1-failure_rate_fullmesh(ec_total-1, afr_drive*l, ec_parity))**pgs)) ** (n_hosts*n_drives);
}

// Same as above, but also take server failures into account
function cluster_afr_pgs_hosts({ n_hosts, n_drives, afr_drive, afr_host, capacity, speed, replicas, pgs = 1, degraded_replacement })
{
    let otherhosts = Math.min(pgs, (n_hosts-1)/(replicas-1));
    pgs = Math.min(pgs, (n_hosts-1)*n_drives/(replicas-1));
    let pgh = Math.min(pgs*n_drives, (n_hosts-1)*n_drives/(replicas-1));
    const ld = capacity/(degraded_replacement ? 1 : pgs)/speed/86400/365;
    const lh = n_drives*capacity/pgs/speed/86400/365;
    const p1 = ((afr_drive+afr_host*pgs/otherhosts)*lh);
    const p2 = ((afr_drive+afr_host*pgs/otherhosts)*ld);
    return 1 - ((1 - afr_host * (1-(1-p1**(replicas-1))**pgh)) ** n_hosts) *
        ((1 - afr_drive * (1-(1-p2**(replicas-1))**pgs)) ** (n_hosts*n_drives));
}

function cluster_afr_pgs_ec_hosts({ n_hosts, n_drives, afr_drive, afr_host, capacity, speed, ec: [ ec_data, ec_parity ], pgs = 1, degraded_replacement })
{
    const ec_total = ec_data+ec_parity;
    const otherhosts = Math.min(pgs, (n_hosts-1)/(ec_total-1));
    pgs = Math.min(pgs, (n_hosts-1)*n_drives/(ec_total-1));
    const pgh = Math.min(pgs*n_drives, (n_hosts-1)*n_drives/(ec_total-1));
    const ld = capacity/(degraded_replacement ? 1 : pgs)/speed/86400/365;
    const lh = n_drives*capacity/pgs/speed/86400/365;
    const p1 = ((afr_drive+afr_host*pgs/otherhosts)*lh);
    const p2 = ((afr_drive+afr_host*pgs/otherhosts)*ld);
    return 1 - ((1 - afr_host * (1-(1-failure_rate_fullmesh(ec_total-1, p1, ec_parity))**pgh)) ** n_hosts) *
        ((1 - afr_drive * (1-(1-failure_rate_fullmesh(ec_total-1, p2, ec_parity))**pgs)) ** (n_hosts*n_drives));
}

// Wrapper for 4 above functions
function cluster_afr(config)
{
    if (config.ec && config.afr_host)
    {
        return cluster_afr_pgs_ec_hosts(config);
    }
    else if (config.ec)
    {
        return cluster_afr_pgs_ec(config);
    }
    else if (config.afr_host)
    {
        return cluster_afr_pgs_hosts(config);
    }
    else
    {
        return cluster_afr_pgs(config);
    }
}

function print_cluster_afr(config)
{
    console.log(
        `${config.n_hosts} nodes with ${config.n_drives} ${sprintf("%.1f", config.capacity/1000)}TB drives`+
        `, capable to backfill at ${sprintf("%.1f", config.speed*1000)} MB/s, drive AFR ${sprintf("%.1f", config.afr_drive*100)}%`+
        (config.afr_host ? `, host AFR ${sprintf("%.1f", config.afr_host*100)}%` : '')+
        (config.ec ? `, EC ${config.ec[0]}+${config.ec[1]}` : `, ${config.replicas} replicas`)+
        `, ${config.pgs||1} PG per OSD`+
        (config.degraded_replacement ? `\n...and you don't let the rebalance finish before replacing drives` : '')
    );
    console.log('-> '+sprintf("%.7f%%", 100*cluster_afr(config))+'\n');
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
