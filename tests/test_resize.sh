#!/bin/bash -ex

PG_COUNT=${PG_COUNT:-32}

. `dirname $0`/run_3osds.sh
check_qemu

LD_PRELOAD="build/src/client/libfio_vitastor.so" \
    fio -thread -name=test -ioengine=build/src/client/libfio_vitastor.so -bs=4M -direct=1 -iodepth=4 \
        -rw=write -etcd=$ETCD_URL -end_fsync=1 -pool=1 -inode=1 -size=256M -runtime=10

LD_PRELOAD="build/src/client/libfio_vitastor.so" \
    fio -thread -name=test -ioengine=build/src/client/libfio_vitastor.so -bs=4k -direct=1 -iodepth=32 \
        -rw=randwrite -etcd=$ETCD_URL -end_fsync=1 -pool=1 -inode=1 -size=256M -runtime=10 -number_ios=1024

qemu-img convert -S 4096 -p \
    -f raw "vitastor:etcd_host=127.0.0.1\:$ETCD_PORT/v3:pool=1:inode=1:size=$((256*1024*1024))" \
    -O raw ./testdata/bin/before.bin

for i in $(seq 1 $OSD_COUNT); do
    pid=OSD${i}_PID
    pid=${!pid}
    kill -9 $pid
done

for i in $(seq 1 $OSD_COUNT); do
    offsets=$(build/src/disk_tool/vitastor-disk simple-offsets --format json ./testdata/bin/test_osd$i.bin $OFFSET_ARGS)
    opts=$(build/src/disk_tool/vitastor-disk simple-offsets --format options ./testdata/bin/test_osd$i.bin $OFFSET_ARGS)
    meta_format=$(echo $offsets | jq -r .meta_format)
    meta_offset=$(echo $offsets | jq -r .meta_offset)
    data_offset=$(echo $offsets | jq -r .data_offset)
    if [[ "$meta_format" = "3" ]]; then
        build/src/disk_tool/vitastor-disk dump-meta --io cached $opts >./testdata/meta_before_resize.json
        new_data_offset=$((128*1024*1024+data_offset%131072))
    else
        build/src/disk_tool/vitastor-disk dump-journal --io cached --json ./testdata/bin/test_osd$i.bin 4096 0 $meta_offset >./testdata/journal_before_resize.json
        build/src/disk_tool/vitastor-disk dump-meta --io cached ./testdata/bin/test_osd$i.bin 4096 $meta_offset $((data_offset-meta_offset)) >./testdata/meta_before_resize.json
        new_data_offset=$((128*1024*1024+32768))
    fi
    build/src/disk_tool/vitastor-disk raw-resize --io cached \
        $opts \
        --new_meta_offset 0 \
        --new_meta_len $((1024*1024)) \
        --new_journal_offset $((1024*1024)) \
        --new_data_offset $new_data_offset
    if [[ "$meta_format" = "3" ]]; then
        build/src/disk_tool/vitastor-disk dump-meta --io cached $opts \
            --meta_offset 0 \
            --meta_len $((1024*1024)) \
            --journal_offset $((1024*1024)) \
            --data_offset $new_data_offset >./testdata/meta_after_resize.json
    else
        build/src/disk_tool/vitastor-disk dump-journal --io cached --json ./testdata/bin/test_osd$i.bin 4096 $((1024*1024)) $((127*1024*1024)) >./testdata/journal_after_resize.json
        build/src/disk_tool/vitastor-disk dump-meta --io cached ./testdata/bin/test_osd$i.bin 4096 0 $((1024*1024)) >./testdata/meta_after_resize.json
    fi
    RMBLOCK=".block"
    if [[ "$meta_format" = "3" ]]; then
        RMBLOCK=".writes[].location"
    fi
    if ! (cat ./testdata/meta_before_resize.json ./testdata/meta_after_resize.json | \
        jq -e -s 'map([ .entries[] | del('"$RMBLOCK"') ] | sort_by(.pool, .inode, .stripe)) | .[0] == .[1] and (.[0] | length) > 1000'); then
        format_error "OSD $i metadata corrupted after resizing"
    fi
    if [[ "$meta_format" != "3" ]]; then
        if ! (cat ./testdata/journal_before_resize.json ./testdata/journal_after_resize.json | \
            jq -e -s 'map([ .[] | del(.crc32, .crc32_prev, .valid, .loc, .start) ]) | .[0] == .[1] and (.[0] | length) > 1'); then
            format_error "OSD $i journal corrupted after resizing"
        fi
    fi
done

$ETCDCTL del --prefix /vitastor/osd/state/

for i in $(seq 1 $OSD_COUNT); do
    build/src/osd/vitastor-osd --osd_num $i --bind_address 127.0.0.1 $NO_SAME $OSD_ARGS --etcd_address $ETCD_URL \
        --meta_format $meta_format \
        --data_device ./testdata/bin/test_osd$i.bin \
        --meta_offset 0 \
        --journal_offset $((1024*1024)) \
        --data_offset $new_data_offset >>./testdata/osd$i.log 2>&1 &
    eval OSD${i}_PID=$!
done

qemu-img convert -S 4096 -p \
    -f raw "vitastor:etcd_host=127.0.0.1\:$ETCD_PORT/v3:pool=1:inode=1:size=$((256*1024*1024))" \
    -O raw ./testdata/bin/after.bin

if ! cmp ./testdata/bin/before.bin ./testdata/bin/after.bin; then
    format_error "Data differs after resizing"
fi

format_green OK
