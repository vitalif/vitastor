#!/bin/bash -ex

SCHEME=xor
PG_COUNT=16
PG_MINSIZE=2
. `dirname $0`/run_3osds.sh

$VITASTOR_CLI create -s 128M testimg

$VITASTOR_FIO -bs=4M -direct=1 -iodepth=1 \
    -end_fsync=1 -fsync=1 -rw=write -image=testimg -size=128M -cluster_log_level=10

kill -9 $OSD3_PID
$ETCDCTL del /vitastor/osd/state/3

if $VITASTOR_CLI rm testimg --log_level 10 ; then
    format_error "Delete should not be successful with inactive OSDs"
fi

if ! ( $VITASTOR_CLI ls | grep testimg | grep DEL ) ; then
    format_error "Image should be marked as partially deleted"
fi

start_osd 3
sleep 5

# Now do the same but without del /vitastor/osd/state

$VITASTOR_FIO -bs=4M -direct=1 -iodepth=1 \
    -end_fsync=1 -fsync=1 -rw=write -image=testimg -size=128M -cluster_log_level=10

kill -9 $OSD3_PID

if $VITASTOR_CLI rm testimg --log_level 10 ; then
    format_error "Delete should not be successful with inactive OSDs"
fi

if ! ( $VITASTOR_CLI ls | grep testimg | grep DEL ) ; then
    format_error "Image should be marked as partially deleted"
fi

format_green OK
