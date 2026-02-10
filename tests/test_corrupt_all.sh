#!/bin/bash -ex

SCHEME=replicated
PG_SIZE=2
OSD_COUNT=2
IMG_SIZE=128
OSD_ARGS="--data_csum_type crc32c --csum_block_size 4k --inmemory_journal false --journal_trim_interval $((IMG_SIZE*8)) $OSD_ARGS"
OFFSET_ARGS="--data_csum_type crc32c --csum_block_size 4k $OFFSET_ARGS"
GLOBAL_CONFIG=',"client_eio_retry_interval":0'
. `dirname $0`/run_3osds.sh
check_qemu

$ETCDCTL put /vitastor/config/inode/1/1 '{"name":"testimg","size":'$((IMG_SIZE*1024*1024))'}'

# Write
$VITASTOR_FIO -bs=1M -direct=1 -iodepth=4 -end_fsync=1 -rw=write -image=testimg -runtime=10
#$VITASTOR_FIO -bs=4k -direct=1 -iodepth=16 -end_fsync=1 -rw=randwrite -image=testimg -number_ios=10000

# Intentionally corrupt OSD data and restart both of them
kill $OSD1_PID $OSD2_PID
data_offset=$(build/src/disk_tool/vitastor-disk simple-offsets ./testdata/bin/test_osd1.bin $OFFSET_ARGS | grep data_offset | awk '{print $2}')
truncate -s $data_offset ./testdata/bin/test_osd1.bin
dd if=/dev/zero of=./testdata/bin/test_osd1.bin bs=1024 count=1 seek=$((OSD_SIZE*1024-1))
truncate -s $data_offset ./testdata/bin/test_osd2.bin
dd if=/dev/zero of=./testdata/bin/test_osd2.bin bs=1024 count=1 seek=$((OSD_SIZE*1024-1))
start_osd 1
start_osd 2

# Wait until start
wait_up 10

# Trigger scrub
$ETCDCTL put /vitastor/pg/history/1/1 `$ETCDCTL get --print-value-only /vitastor/pg/history/1/1 | jq -s -c '(.[0] // {}) + {"next_scrub":1}'`

# Wait for scrub to finish
wait_condition 300 "$ETCDCTL get --prefix /vitastor/pg/history/ --print-value-only | jq -s -e '([ .[] | select(.next_scrub == 0 or .next_scrub == null) ] | length) == $PG_COUNT'" Scrubbing

# Verify that ALL objects are now corrupted+incomplete
$VITASTOR_CLI describe --json &>./testdata/describe.json
$VITASTOR_CLI describe --json | jq -e '[ .[] | select(.corrupted) ] | length == '$((IMG_SIZE * 8 * PG_SIZE))

# Check that we can remove or overwrite them
$VITASTOR_FIO -bs=4M -direct=1 -iodepth=1 -end_fsync=1 -rw=write -offset=$((IMG_SIZE/2))M -image=testimg -runtime=10
$VITASTOR_CLI rm-data --pool 1 --inode 1

format_green OK
