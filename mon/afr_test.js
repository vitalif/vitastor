const { sprintf } = require('sprintf-js');
const { cluster_afr } = require('./afr.js');

print_cluster_afr({ n_hosts: 4, n_drives: 6, afr_drive: 0.03, afr_host: 0.05, capacity: 4000, speed: 0.1, replicas: 2 });
print_cluster_afr({ n_hosts: 4, n_drives: 3, afr_drive: 0.03, afr_host: 0, capacity: 4000, speed: 0.1, replicas: 2 });
print_cluster_afr({ n_hosts: 4, n_drives: 3, afr_drive: 0.03, afr_host: 0.05, capacity: 4000, speed: 0.1, replicas: 2 });
print_cluster_afr({ n_hosts: 4, n_drives: 3, afr_drive: 0.03, afr_host: 0, capacity: 4000, speed: 0.1, ec: true, ec_data: 2, ec_parity: 1 });
print_cluster_afr({ n_hosts: 4, n_drives: 3, afr_drive: 0.03, afr_host: 0.05, capacity: 4000, speed: 0.1, ec: true, ec_data: 2, ec_parity: 1 });
print_cluster_afr({ n_hosts: 10, n_drives: 10, afr_drive: 0.1, afr_host: 0, capacity: 8000, speed: 0.02, replicas: 2 });
print_cluster_afr({ n_hosts: 10, n_drives: 10, afr_drive: 0.1, afr_host: 0.05, capacity: 8000, speed: 0.02, replicas: 2 });
print_cluster_afr({ n_hosts: 10, n_drives: 10, afr_drive: 0.1, afr_host: 0, capacity: 8000, speed: 0.02, replicas: 3 });
print_cluster_afr({ n_hosts: 10, n_drives: 10, afr_drive: 0.1, afr_host: 0.05, capacity: 8000, speed: 0.02, replicas: 3 });
print_cluster_afr({ n_hosts: 10, n_drives: 10, afr_drive: 0.1, afr_host: 0, capacity: 8000, speed: 0.02, replicas: 3, pgs: 100 });
print_cluster_afr({ n_hosts: 10, n_drives: 10, afr_drive: 0.1, afr_host: 0.05, capacity: 8000, speed: 0.02, replicas: 3, pgs: 100 });
print_cluster_afr({ n_hosts: 10, n_drives: 10, afr_drive: 0.1, afr_host: 0.05, capacity: 8000, speed: 0.02, replicas: 3, pgs: 100, degraded_replacement: 1 });

function print_cluster_afr(config)
{
    console.log(
        `${config.n_hosts} nodes with ${config.n_drives} ${sprintf("%.1f", config.capacity/1000)}TB drives`+
        `, capable to backfill at ${sprintf("%.1f", config.speed*1000)} MB/s, drive AFR ${sprintf("%.1f", config.afr_drive*100)}%`+
        (config.afr_host ? `, host AFR ${sprintf("%.1f", config.afr_host*100)}%` : '')+
        (config.ec ? `, EC ${config.ec_data}+${config.ec_parity}` : `, ${config.replicas} replicas`)+
        `, ${config.pgs||1} PG per OSD`+
        (config.degraded_replacement ? `\n...and you don't let the rebalance finish before replacing drives` : '')
    );
    console.log('-> '+sprintf("%.7f%%", 100*cluster_afr(config))+'\n');
}
