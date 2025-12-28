#!/bin/bash -ex

PG_COUNT=16
. `dirname $0`/run_3osds.sh

$VITASTOR_FIO -bs=4M -direct=1 -iodepth=1 \
    -end_fsync=1 -fsync=1 -rw=write -pool=1 -inode=1 -size=128M -cluster_log_level=10

$ETCDCTL get --prefix '/vitastor/pg/state'

$VITASTOR_CLI rm-data --pool 1 --inode 1

format_green OK
