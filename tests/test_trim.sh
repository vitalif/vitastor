#!/bin/bash -ex

# Test client-level TRIM (discard): deletes whole objects covered by the range

PG_COUNT=16
. `dirname $0`/run_3osds.sh

$VITASTOR_CLI create -s 128M testimg

# Fill the whole image
$VITASTOR_FIO -bs=4M -direct=1 -iodepth=4 -end_fsync=1 -rw=write -image=testimg

get_used()
{
    $VITASTOR_CLI ls -l --json | jq -r '[ .[] | select(.name == "testimg") | .used_size ] [0] // 0'
}

# Used size is reported by OSDs and aggregated by the monitor with a delay
wait_condition 60 'u=$(get_used); [[ "$u" -ge '$((127*1024*1024))' ]]' "Initial used size reporting"

# Trim [32M, 96M) - 64M must be freed
$VITASTOR_FIO -bs=4M -direct=1 -iodepth=4 -end_fsync=1 -rw=trim -image=testimg -offset=32M -size=64M

wait_condition 60 'u=$(get_used); [[ "$u" -le '$((65*1024*1024))' ]]' "Used size reduction after trim"

# Check the data: the trimmed area must read as zeros, the rest must be intact
$VITASTOR_CLI dd iimg=testimg of=./testdata/bin/trimmed.bin
cmp <(dd if=/dev/zero bs=1M count=64 status=none) <(dd if=./testdata/bin/trimmed.bin bs=1M skip=32 count=64 status=none)
! cmp -s <(dd if=/dev/zero bs=1M count=32 status=none) <(dd if=./testdata/bin/trimmed.bin bs=1M count=32 status=none)
! cmp -s <(dd if=/dev/zero bs=1M count=32 status=none) <(dd if=./testdata/bin/trimmed.bin bs=1M skip=96 count=32 status=none)

# A trim smaller than the object size must be ignored and must not touch any data
$VITASTOR_FIO -bs=64k -direct=1 -iodepth=1 -end_fsync=1 -rw=trim -image=testimg -offset=1M -size=64k
$VITASTOR_CLI dd iimg=testimg of=./testdata/bin/trimmed2.bin
cmp ./testdata/bin/trimmed.bin ./testdata/bin/trimmed2.bin

format_green OK
