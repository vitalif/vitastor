#!/bin/bash -ex

PG_COUNT=16
. `dirname $0`/run_3osds.sh

$VITASTOR_FIO -bs=4M -direct=1 -iodepth=1 \
    -end_fsync=1 -fsync=1 -rw=write -pool=1 -inode=1 -size=128M -cluster_log_level=10

$ETCDCTL get --prefix '/vitastor/pg/state'

$VITASTOR_CLI rm-data --pool 1 --inode 1

# test resize with partial data removal

$VITASTOR_CLI create -s 128M testimg

$VITASTOR_FIO -bs=4M -direct=1 -iodepth=1 -end_fsync=1 -fsync=1 -rw=write -image=testimg

$VITASTOR_CLI modify --resize 64M testimg

$VITASTOR_CLI dd iimg=testimg of=./testdata/bin/testimg.bin

sz=$(stat -c %s ./testdata/bin/testimg.bin)

[[ "$sz" = 67108864 ]] || format_error "File size is $sz bytes after resize to 64 M"

format_green OK
