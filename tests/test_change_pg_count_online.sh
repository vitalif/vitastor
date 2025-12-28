#!/bin/bash -ex

OSD_COUNT=${OSD_COUNT:-6}
PG_COUNT=16
GLOBAL_CONFIG=',"client_retry_interval":1000'

. `dirname $0`/run_3osds.sh

$VITASTOR_FIO -bs=4M -direct=1 -iodepth=1 -fsync=1 -rw=write -pool=1 -inode=2 -size=128M

$VITASTOR_FIO -bs=4k -direct=1 -iodepth=4 -rw=randrw -pool=1 -inode=2 -size=128M -loops=100 \
    -cluster_log_level=3 -runtime=60 &>./testdata/fio.log &
FIO_PID=$!

try_change()
{
    n=$1

    for i in {1..6}; do
        echo --- Change PG count to $n --- >>testdata/osd$i.log
    done
    echo --- Change PG count to $n --- >>testdata/mon.log

    $ETCDCTL put /vitastor/config/pools '{"1":{'$POOLCFG',"pg_size":'$PG_SIZE',"pg_minsize":'$PG_MINSIZE',"pg_count":'$n'}}'
    echo "Pool 1 (testpool) PG count changed from $PG_COUNT to $n" >> testdata/pgr.log
    PG_COUNT=$n

    sleep 10
}

sleep 5

# 16 -> 32

try_change 32

# 32 -> 16

try_change 16

# 16 -> 25

try_change 25

# 25 -> 17

try_change 17

# 17 -> 16

try_change 16

wait $FIO_PID

grep -P 'Pool 1 \(testpool\) PG count changed from \d+ to \d+' testdata/fio.log > testdata/pgc.log
diff testdata/pgr.log testdata/pgc.log

format_green OK
