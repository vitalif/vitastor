#!/bin/bash -ex

OSD_ARGS="--data_csum_type crc32c"
OFFSET_ARGS=$OSD_ARGS
GLOBAL_CONFIG=',"client_eio_retry_interval":0'
. `dirname $0`/run_3osds.sh

$VITASTOR_CLI create -s 32M testimg

$VITASTOR_FIO -bs=4k -blockalign=128k -direct=1 -iodepth=16 -end_fsync=1 -rw=write -image=testimg

$VITASTOR_CLI dd iimg=testimg of=/dev/null

if grep -q 'Checksum mismatch' ./testdata/osd*.log; then
    format_error Checksum mismatches detected during test
fi

format_green OK
