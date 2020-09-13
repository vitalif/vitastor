#!/bin/bash
# Example startup script generator
# Of course this isn't a production solution yet, this is just for tests

IP=`ip -json a s | jq -r '.[].addr_info[] | select(.broadcast == "10.115.0.255") | .local'`

[ "$IP" != "" ] || exit 1

useradd vitastor
chmod 755 /root

BASE=${IP/*./}
BASE=$(((BASE-10)*12))

cat >/etc/systemd/system/vitastor.target <<EOF
[Unit]
Description=vitastor target
[Install]
WantedBy=multi-user.target
EOF

i=1
for DEV in `ls /dev/disk/by-id/ | grep ata-INTEL_SSDSC2KB`; do
    dd if=/dev/zero of=/dev/disk/by-id/$DEV bs=1048576 count=$(((427814912+1048575)/1048576+2))
    dd if=/dev/zero of=/dev/disk/by-id/$DEV bs=1048576 count=$(((427814912+1048575)/1048576+2)) seek=$((1920377991168/1048576))
cat >/etc/systemd/system/vitastor-osd$((BASE+i)).service <<EOF
[Unit]
Description=Vitastor object storage daemon osd.$((BASE+i))
After=network-online.target local-fs.target time-sync.target
Wants=network-online.target local-fs.target time-sync.target
PartOf=vitastor.target

[Service]
LimitNOFILE=1048576
LimitNPROC=1048576
LimitMEMLOCK=infinity
ExecStart=/root/vitastor/osd \\
    --etcd_address $IP:2379/v3 \\
    --bind_address $IP \\
    --osd_num $((BASE+i)) \\
    --disable_data_fsync 1 \\
    --disable_device_lock 1 \\
    --immediate_commit all \\
    --flusher_count 8 \\
    --disk_alignment 4096 --journal_block_size 4096 --meta_block_size 4096 \\
    --journal_no_same_sector_overwrites true \\
    --journal_sector_buffer_count 1024 \\
    --journal_offset 0 \\
    --meta_offset 16777216 \\
    --data_offset 427814912 \\
    --data_size $((1920377991168-427814912)) \\
    --data_device /dev/disk/by-id/$DEV
WorkingDirectory=/root/vitastor
ExecStartPre=+chown vitastor:vitastor /dev/disk/by-id/$DEV
User=vitastor
PrivateTmp=false
TasksMax=infinity
Restart=always
StartLimitInterval=0
StartLimitIntervalSec=0
RestartSec=10

[Install]
WantedBy=vitastor.target
EOF
    systemctl enable vitastor-osd$((BASE+i))
    i=$((i+1))
cat >/etc/systemd/system/vitastor-osd$((BASE+i)).service <<EOF
[Unit]
Description=Vitastor object storage daemon osd.$((BASE+i))
After=network-online.target local-fs.target time-sync.target
Wants=network-online.target local-fs.target time-sync.target
PartOf=vitastor.target

[Service]
LimitNOFILE=1048576
LimitNPROC=1048576
LimitMEMLOCK=infinity
ExecStart=/root/vitastor/osd \\
    --etcd_address $IP:2379/v3 \\
    --bind_address $IP \\
    --osd_num $((BASE+i)) \\
    --disable_data_fsync 1 \\
    --immediate_commit all \\
    --flusher_count 8 \\
    --disk_alignment 4096 --journal_block_size 4096 --meta_block_size 4096 \\
    --journal_no_same_sector_overwrites true \\
    --journal_sector_buffer_count 1024 \\
    --journal_offset 1920377991168 \\
    --meta_offset $((1920377991168+16777216)) \\
    --data_offset $((1920377991168+427814912)) \\
    --data_size $((1920377991168-427814912)) \\
    --data_device /dev/disk/by-id/$DEV
WorkingDirectory=/root/vitastor
ExecStartPre=+chown vitastor:vitastor /dev/disk/by-id/$DEV
User=vitastor
PrivateTmp=false
TasksMax=infinity
Restart=always
StartLimitInterval=0
StartLimitIntervalSec=0
RestartSec=10

[Install]
WantedBy=vitastor.target
EOF
    systemctl enable vitastor-osd$((BASE+i))
    i=$((i+1))
done

exit

node mon-main.js --etcd_url 'http://10.115.0.10:2379,http://10.115.0.11:2379,http://10.115.0.12:2379,http://10.115.0.13:2379' --etcd_prefix '/vitastor' --etcd_start_timeout 5

podman run -d --network host --restart always -v /var/lib/etcd0.etcd:/etcd0.etcd --name etcd quay.io/coreos/etcd:v3.4.13 etcd -name etcd0 \
    -advertise-client-urls http://10.115.0.10:2379 -listen-client-urls http://10.115.0.10:2379 \
    -initial-advertise-peer-urls http://10.115.0.10:2380 -listen-peer-urls http://10.115.0.10:2380 \
    -initial-cluster-token vitastor-etcd-1 -initial-cluster etcd0=http://10.115.0.10:2380,etcd1=http://10.115.0.11:2380,etcd2=http://10.115.0.12:2380,etcd3=http://10.115.0.13:2380 \
    -initial-cluster-state new --max-txn-ops=100000 --auto-compaction-retention=10 --auto-compaction-mode=revision

etcdctl --endpoints http://10.115.0.10:2379 put /vitastor/config/global '{"immediate_commit":"all"}'

etcdctl --endpoints http://10.115.0.10:2379 put /vitastor/config/pools '{"1":{"name":"testpool","scheme":"replicated","pg_size":2,"pg_minsize":1,"pg_count":48,"failure_domain":"host"}}'

#let pgs = {};
#for (let n = 0; n < 48; n++) { let i = n/2 | 0; pgs[1+n] = { osd_set: [ (1+i%12+(i/12 | 0)*24), (1+12+i%12+(i/12 | 0)*24) ], primary: (1+(n%2)*12+i%12+(i/12 | 0)*24) }; };
#console.log(JSON.stringify({ items: { 1: pgs } }));
#etcdctl --endpoints http://10.115.0.10:2379 put /vitastor/config/pgs ...

#    --disk_alignment 4096 --journal_block_size 4096 --meta_block_size 4096 \\
#    --data_offset 427814912 \\

#    --disk_alignment 4096 --journal_block_size 512 --meta_block_size 512 \\
#    --data_offset 433434624 \\
