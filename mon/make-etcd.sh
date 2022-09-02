#!/bin/bash
# Simple systemd unit generator for etcd
# Copyright (c) Vitaliy Filippov, 2019+
# License: MIT

# USAGE: [IP_SUBSTR=10.200.1.] [ETCD_HOSTS=etcd0=http://10.200.1.10:2380,...] ./make-etcd.sh

IP_SUBSTR=${IP_SUBSTR:-10.200.1.}
ETCD_HOSTS=${ETCD_HOSTS:-etcd0=http://10.200.1.10:2380,etcd1=http://10.200.1.11:2380,etcd2=http://10.200.1.12:2380}

# determine IP
IP=`ip -json a s | jq -r '.[].addr_info[] | select(.local | startswith("'$IP_SUBSTR'")) | .local'`
[ "$IP" != "" ] || exit 1
ETCD_NUM=${ETCD_HOSTS/$IP*/}
[ "$ETCD_NUM" != "$ETCD_HOSTS" ] || exit 1
ETCD_NUM=$(echo $ETCD_NUM | tr -d -c , | wc -c)

useradd etcd

mkdir -p /var/lib/etcd$ETCD_NUM.etcd
cat >/etc/systemd/system/etcd.service <<EOF
[Unit]
Description=etcd for vitastor
After=network-online.target local-fs.target time-sync.target
Wants=network-online.target local-fs.target time-sync.target

[Service]
Restart=always
ExecStart=/usr/local/bin/etcd -name etcd$ETCD_NUM --data-dir /var/lib/etcd$ETCD_NUM.etcd \\
    --advertise-client-urls http://$IP:2379 --listen-client-urls http://$IP:2379 \\
    --initial-advertise-peer-urls http://$IP:2380 --listen-peer-urls http://$IP:2380 \\
    --initial-cluster-token vitastor-etcd-1 --initial-cluster $ETCD_HOSTS \\
    --initial-cluster-state new --max-txn-ops=100000 --max-request-bytes=104857600 \\
    --auto-compaction-retention=10 --auto-compaction-mode=revision
WorkingDirectory=/var/lib/etcd$ETCD_NUM.etcd
ExecStartPre=+chown -R etcd /var/lib/etcd$ETCD_NUM.etcd
User=etcd
PrivateTmp=false
TasksMax=infinity
Restart=always
StartLimitInterval=0
RestartSec=10

[Install]
WantedBy=local.target
EOF

systemctl daemon-reload
systemctl enable etcd
systemctl start etcd
