#!/bin/bash -ex
# Test listing of large directories completely in the K/V cache previously leading
# to stack overflows because of list_next() recursion via get_block() synchronous path.
# The server died with SIGSEGV (stack overflow) at ~7800 direntries.
#
# max_list_cookies=0 turns the READDIR cookie cache off, so every request has to
# rescan the directory from its very beginning - which is exactly what made the
# recursion deep in production. A 2 MB stack lowers the old limit to ~1900
# entries, so ENTRY_COUNT below is enough to reproduce it.

GLOBAL_CONFIG=',"client_enable_writeback":false'
IMMEDIATE_COMMIT=1
PG_COUNT=16
. `dirname $0`/run_3osds.sh

ENTRY_COUNT=6000
# Large K/V blocks hold thousands of direntries each, and entries of one cached
# block are returned to the caller synchronously - which is what used to make the
# recursion deep enough to overflow the (deliberately small) stack
KV_BLOCK_SIZE=131072

$VITASTOR_CLI create -s 10G fsmeta
$VITASTOR_CLI modify-pool --used-for-app fs:fsmeta testpool
( ulimit -s 1024; exec build/src/nfs/vitastor-nfs --config_path $VITASTOR_CFG start --fs fsmeta \
    --portmap 0 --port 2050 --foreground 1 --max_list_cookies 0 --kv_block_size $KV_BLOCK_SIZE >>./testdata/nfs.log 2>&1 ) &
NFS_PID=$!

mkdir -p testdata/nfs
sudo mount localhost:/ ./testdata/nfs -o port=2050,mountport=2050,nfsvers=3,soft,nolock,tcp
MNT=$(pwd)/testdata/nfs
trap "sudo umount -f $MNT"' || true; kill -9 $(jobs -p)' EXIT

mkdir $MNT/bigdir
seq 1 $ENTRY_COUNT | xargs -P 16 -I{} touch $MNT/bigdir/file_{}

# Drop the client-side directory cache so the listings really hit the server
sudo umount ./testdata/nfs/
sudo mount localhost:/ ./testdata/nfs -o port=2050,mountport=2050,nfsvers=3,soft,nolock,tcp

# READDIR
COUNT=$(ls -f -1 $MNT/bigdir | wc -l)
if ! kill -0 $NFS_PID; then
    format_error 'vitastor-nfs died while listing a large directory'
fi
if [ "$COUNT" -ne $((ENTRY_COUNT+2)) ]; then
    format_error "READDIR returned $COUNT entries instead of $((ENTRY_COUNT+2)) (with . and ..)"
fi
# Every name exactly once
DUPS=$(ls -f -1 $MNT/bigdir | sort | uniq -d | wc -l)
if [ "$DUPS" -ne 0 ]; then
    format_error "READDIR returned $DUPS duplicate entries"
fi
format_green "READDIR of $ENTRY_COUNT entries ok"

sudo umount ./testdata/nfs/
sudo mount localhost:/ ./testdata/nfs -o port=2050,mountport=2050,nfsvers=3,soft,nolock,tcp

# READDIRPLUS
COUNT=$(ls -lU $MNT/bigdir | grep -c '^-')
if [ "$COUNT" -ne $ENTRY_COUNT ]; then
    format_error "READDIRPLUS returned $COUNT files instead of $ENTRY_COUNT"
fi
format_green "READDIRPLUS of $ENTRY_COUNT entries ok"

# The same listing path is used by the K/V dump
build/src/kv/vitastor-kv --config_path $VITASTOR_CFG --kv_block_size $KV_BLOCK_SIZE fsmeta dump > ./testdata/kv_dump.txt
COUNT=$(grep -c '^set ' ./testdata/kv_dump.txt)
if [ "$COUNT" -lt $ENTRY_COUNT ]; then
    format_error "vitastor-kv dump returned $COUNT keys, expected at least $ENTRY_COUNT"
fi
format_green "vitastor-kv dump of $COUNT keys ok"

format_green OK
