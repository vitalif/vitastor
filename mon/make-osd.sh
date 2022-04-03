#!/bin/bash
# Very simple systemd unit generator for vitastor-osd services
# Not the final solution yet, mostly for tests
# Copyright (c) Vitaliy Filippov, 2019+
# License: MIT

# USAGE:
# 1) Put etcd_address and osd_network into /etc/vitastor/vitastor.conf. Example:
#    {
#        "etcd_address":["http://10.200.1.10:2379/v3","http://10.200.1.11:2379/v3","http://10.200.1.12:2379/v3"],
#        "osd_network":"10.200.1.0/24"
#    }
# 2) Run ./make-osd.sh /dev/disk/by-partuuid/xxx [ /dev/disk/by-partuuid/yyy]...

set -e -x

# Create OSDs on all passed devices
for DEV in $*; do

OSD_NUM=$(vitastor-cli alloc-osd)

echo Creating OSD $OSD_NUM on $DEV

OPT=$(vitastor-cli simple-offsets --format options $DEV | tr '\n' ' ')
META=$(vitastor-cli simple-offsets --format json $DEV | jq .data_offset)
dd if=/dev/zero of=$DEV bs=1048576 count=$(((META+1048575)/1048576)) oflag=direct

mkdir -p /var/log/vitastor
id vitastor &>/dev/null || useradd vitastor
chown vitastor /var/log/vitastor

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
ExecStart=bash -c '/usr/bin/vitastor-osd \\
    --osd_num $OSD_NUM \\
    --disable_data_fsync 1 \\
    --immediate_commit all \\
    --disk_alignment 4096 --journal_block_size 4096 --meta_block_size 4096 \\
    --journal_no_same_sector_overwrites true \\
    --journal_sector_buffer_count 1024 \\
    $OPT >>/var/log/vitastor/osd$OSD_NUM.log 2>&1'
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
