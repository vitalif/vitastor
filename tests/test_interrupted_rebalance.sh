#!/bin/bash -ex

OSD_COUNT=7
PG_COUNT=32
. `dirname $0`/run_3osds.sh

IMG_SIZE=960

$VITASTOR_FIO -bs=4M -direct=1 -iodepth=16 -fsync=16 -rw=write \
    -pool=1 -inode=2 -size=${IMG_SIZE}M -cluster_log_level=10

try_reweight 1 0

try_reweight 2 0

try_reweight 3 0

try_reweight 4 0

try_reweight 5 0

try_reweight 1 1

try_reweight 2 1

try_reweight 3 1

try_reweight 4 1

try_reweight 5 1

# Allow rebalance to start
sleep 5

# Wait for the rebalance to finish
wait_finish_rebalance 300

# Check that PGs never had degraded objects !
# FIXME: In fact, the test doesn't guarantee it because PGs aren't always peered only with full prior OSD sets :-(
#if grep has_degraded ./testdata/mon.log; then
#    format_error "Some copies of objects were lost during interrupted rebalancings"
#fi

# Check that no objects are lost !
nobj=`$ETCDCTL get --prefix '/vitastor/pgstats' --print-value-only | jq -s '[ .[].object_count ] | reduce .[] as $num (0; .+$num)'`
if [ "$nobj" -ne $((IMG_SIZE*8/PG_DATA_SIZE)) ]; then
    format_error "Data lost after multiple interrupted rebalancings"
fi

format_green OK
