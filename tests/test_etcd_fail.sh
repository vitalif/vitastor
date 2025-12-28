#!/bin/bash -ex

# Run 5 etcds and kill them while operating
ETCD_COUNT=5

. `dirname $0`/run_3osds.sh

$VITASTOR_FIO -bs=4M -direct=1 -iodepth=1 -fsync=1 -rw=randwrite \
    -pool=1 -inode=1 -size=128M -cluster_log_level=10

kill_etcds()
{
    sleep 5
    kill $ETCD1_PID $ETCD2_PID
    sleep 5
    kill $ETCD3_PID
    start_etcd 1
    sleep 5
    kill $ETCD4_PID
    start_etcd 2
    sleep 5
    kill $ETCD5_PID
    start_etcd 3
}

kill_etcds &

$VITASTOR_FIO -bs=4k -direct=1 -iodepth=1 -fsync=1 -rw=randwrite \
    -pool=1 -inode=1 -size=128M -cluster_log_level=10 -runtime=30

format_green OK
