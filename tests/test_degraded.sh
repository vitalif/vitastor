#!/bin/bash -ex

OSD_COUNT=0
PG_COUNT=32
PG_SIZE=3
PG_MINSIZE=2
POOLCFG='"failure_domain":"host",'
. `dirname $0`/run_3osds.sh

$ETCDCTL put /vitastor/config/node_placement '{"1":{"parent":"host1"},"2":{"parent":"host2"},"3":{"parent":"host3"},"4":{"parent":"host1"},"5":{"parent":"host2"},"6":{"parent":"host3"},"host1":{"level":"host"},"host2":{"level":"host"}}'

OSD_COUNT=6
for i in $(seq 1 $OSD_COUNT); do
    start_osd $i
done
wait_up 120

$VITASTOR_FIO -bs=4k -direct=1 -iodepth=4 -rw=randwrite -loops=1000 \
    -pool=1 -inode=2 -size=256M -cluster_log_level=10 -runtime=5 &
FIO_PID=$!

sleep 15
kill -9 $OSD1_PID $OSD4_PID

wait $FIO_PID

format_green OK
