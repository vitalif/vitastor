#!/bin/bash -ex

PG_COUNT=${PG_COUNT:-32}

. `dirname $0`/run_3osds.sh

$VITASTOR_FIO -bs=4M -direct=1 -iodepth=4 \
    -rw=write -end_fsync=1 -pool=1 -inode=1 -size=256M -runtime=10

$VITASTOR_FIO -bs=4k -direct=1 -iodepth=32 \
    -rw=randwrite -end_fsync=1 -pool=1 -inode=1 -size=256M -runtime=10 -number_ios=1024

for i in $(seq 1 $OSD_COUNT); do
    pid=OSD${i}_PID
    pid=${!pid}
    kill -9 $pid
done

offsets=$(build/src/disk_tool/vitastor-disk simple-offsets --format json ./testdata/bin/test_osd1.bin $OFFSET_ARGS)
opts=$(build/src/disk_tool/vitastor-disk simple-offsets --format options ./testdata/bin/test_osd1.bin $OFFSET_ARGS)
meta_format=$(echo $offsets | jq -r .meta_format)
journal_offset=$(echo $offsets | jq -r .journal_offset)
meta_offset=$(echo $offsets | jq -r .meta_offset)
data_offset=$(echo $offsets | jq -r .data_offset)
if [[ "$meta_format" = "3" ]]; then
    build/src/disk_tool/vitastor-disk dump-meta --io cached $opts | jq '. + { "entries": .entries | sort_by(.stripe) }' >./testdata/meta.json
    build/src/disk_tool/vitastor-disk write-meta --io cached $opts <./testdata/meta.json
    build/src/disk_tool/vitastor-disk dump-meta --io cached $opts | jq '. + { "entries": .entries | sort_by(.stripe) }' >./testdata/meta2.json
else
    build/src/disk_tool/vitastor-disk dump-journal --io cached --json ./testdata/bin/test_osd1.bin 4096 $journal_offset $((meta_offset-journal_offset)) | jq 'map(del(.crc32, .crc32_prev)) | map(if .type == "small_write" or .type == "small_write_instant" then del(.loc) elif .type == "start" then del(.start) else . end)' >./testdata/journal.json
    build/src/disk_tool/vitastor-disk write-journal --io cached --json ./testdata/bin/test_osd1.bin 4096 4 $journal_offset $((meta_offset-journal_offset)) <./testdata/journal.json
    build/src/disk_tool/vitastor-disk dump-journal --io cached --json ./testdata/bin/test_osd1.bin 4096 $journal_offset $((meta_offset-journal_offset)) | jq 'map(del(.crc32, .crc32_prev)) | map(if .type == "small_write" or .type == "small_write_instant" then del(.loc) elif .type == "start" then del(.start) else . end)' >./testdata/journal2.json
    diff ./testdata/journal.json ./testdata/journal2.json
    build/src/disk_tool/vitastor-disk dump-meta --io cached ./testdata/bin/test_osd1.bin 4096 $meta_offset $((data_offset-meta_offset)) >./testdata/meta.json
    build/src/disk_tool/vitastor-disk write-meta --io cached $opts <./testdata/meta.json
    build/src/disk_tool/vitastor-disk dump-meta --io cached ./testdata/bin/test_osd1.bin 4096 $meta_offset $((data_offset-meta_offset)) >./testdata/meta2.json
fi
diff ./testdata/meta.json ./testdata/meta2.json

format_green OK
