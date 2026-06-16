#!/bin/bash -ex

PG_COUNT=1
PG_SIZE=1
PG_MINSIZE=1
SCHEME=replicated
OSD_COUNT=1
. `dirname $0`/run_3osds.sh

kill -9 $OSD1_PID
offsets=$(build/src/disk_tool/vitastor-disk simple-offsets --format json ./testdata/bin/test_osd$i.bin $OFFSET_ARGS)
opts=$(build/src/disk_tool/vitastor-disk simple-offsets --format options ./testdata/bin/test_osd$i.bin $OFFSET_ARGS)
meta_format=$(echo $offsets | jq -r .meta_format)
meta_offset=$(echo $offsets | jq -r .meta_offset)
data_offset=$(echo $offsets | jq -r .data_offset)
data_len=$((1024*1024*1024 - data_offset))
last_block=$((data_len/128/1024 - 1))

if [[ "$meta_format" = "3" ]]; then
    cat >>./testdata/meta.json <<EOF
{"version":"3.0","meta_block_size":4096,"data_block_size":131072,"bitmap_granularity":4096,"data_csum_type":"none","csum_block_size":0,"entries":[
{"pool":1,"inode":"0x1","stripe":"0x0","writes":[{"lsn":1,"version":1,"type":"big","stable":true,"location":$((last_block*128*1024)),"offset":0,"len":131072,"bitmap":"ffffffff","ext_bitmap":"ffffffff"}]}
]}
EOF
    build/src/disk_tool/vitastor-disk write-meta --io cached $opts <./testdata/meta.json
    build/src/disk_tool/vitastor-disk raw-resize --io cached \
        $opts \
        --new_data_len $((data_len - data_len%131072 - 4096))
    build/src/disk_tool/vitastor-disk dump-meta --io cached $opts >./testdata/meta1.json
    if ! jq -s -e '(.[0].entries | length) == 1 and .[0].entries[0].writes[0].location < '$((last_block*128*1024)) <./testdata/meta1.json; then
        format_error "Last block not moved"
    fi
else
    cat >>./testdata/meta.json <<EOF
{"version":"0.9","meta_block_size":4096,"data_block_size":131072,"bitmap_granularity":4096,"data_csum_type":"none","csum_block_size":0,"entries":[
{"block":$last_block,"pool":1,"inode":"0x1","stripe":"0x0","version":1,"bitmap":"ffffffff","ext_bitmap":"ffffffff"}
]}
EOF
    build/src/disk_tool/vitastor-disk write-meta --io cached $opts <./testdata/meta.json
    build/src/disk_tool/vitastor-disk raw-resize --io cached \
        $opts \
        --new_data_len $((data_len - data_len%131072 - 4096))
    build/src/disk_tool/vitastor-disk dump-meta --io cached \
        ./testdata/bin/test_osd1.bin 4096 $meta_offset $((data_offset-meta_offset)) >./testdata/meta1.json
    if ! jq -s -e '(.[0].entries | length) == 1 and .[0].entries[0].block < '$last_block <./testdata/meta1.json; then
        format_error "Last block not moved"
    fi
fi

format_green OK
