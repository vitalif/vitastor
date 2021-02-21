#!/bin/bash
# Very simple systemd unit generator for vitastor-osd services
# Not the final solution yet, mostly for tests
# Copyright (c) Vitaliy Filippov, 2019+
# License: MIT

# USAGE: ./make-osd.sh /dev/disk/by-partuuid/xxx [ /dev/disk/by-partuuid/yyy]...

IP_SUBSTR="10.200.1."
ETCD_HOSTS="etcd0=http://10.200.1.10:2380,etcd1=http://10.200.1.11:2380,etcd2=http://10.200.1.12:2380"

set -e -x

IP=`ip -json a s | jq -r '.[].addr_info[] | select(.local | startswith("'$IP_SUBSTR'")) | .local'`
[ "$IP" != "" ] || exit 1
ETCD_MON=$(echo $ETCD_HOSTS | perl -pe 's/:2380/:2379/g; s/etcd\d*=//g;')
D=`dirname $0`

# Create OSDs on all passed devices
OSD_NUM=1
for DEV in $*; do

# Ugly :) -> node.js rework pending
while true; do
    ST=$(etcdctl --endpoints="$ETCD_MON" get --print-value-only /vitastor/osd/stats/$OSD_NUM)
    if [ "$ST" = "" ]; then
        break
    fi
    OSD_NUM=$((OSD_NUM+1))
done
etcdctl --endpoints="$ETCD_MON" put /vitastor/osd/stats/$OSD_NUM '{}'

echo Creating OSD $OSD_NUM on $DEV

OPT=`node $D/simple-offsets.js --device $DEV --format options | tr '\n' ' '`
META=`echo $OPT | grep -Po '(?<=data_offset )\d+'`
dd if=/dev/zero of=$DEV bs=1048576 count=$(((META+1048575)/1048576)) oflag=direct

cat >/etc/systemd/system/vitastor-osd$OSD_NUM.service <<EOF
[Unit]
Description=Vitastor object storage daemon osd.$OSD_NUM
After=network-online.target local-fs.target time-sync.target
Wants=network-online.target local-fs.target time-sync.target
PartOf=vitastor.target

[Service]
LimitNOFILE=1048576
LimitNPROC=1048576
LimitMEMLOCK=infinity
ExecStart=/usr/bin/vitastor-osd \\
    --etcd_address $IP:2379/v3 \\
    --bind_address $IP \\
    --osd_num $OSD_NUM \\
    --disable_data_fsync 1 \\
    --immediate_commit all \\
    --flusher_count 8 \\
    --disk_alignment 4096 --journal_block_size 4096 --meta_block_size 4096 \\
    --journal_no_same_sector_overwrites true \\
    --journal_sector_buffer_count 1024 \\
    $OPT
WorkingDirectory=/
ExecStartPre=+chown vitastor:vitastor $DEV
User=vitastor
PrivateTmp=false
TasksMax=infinity
Restart=always
StartLimitInterval=0
RestartSec=10

[Install]
WantedBy=vitastor.target
EOF

systemctl enable vitastor-osd$OSD_NUM

done
