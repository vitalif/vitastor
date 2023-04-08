#!/bin/bash -ex

PG_COUNT=${PG_COUNT:-32}

. `dirname $0`/run_3osds.sh

LD_PRELOAD="build/src/libfio_vitastor.so" \
    fio -thread -name=test -ioengine=build/src/libfio_vitastor.so -bs=4M -direct=1 -iodepth=4 \
        -rw=write -etcd=$ETCD_URL -end_fsync=1 -pool=1 -inode=1 -size=256M -runtime=10

LD_PRELOAD="build/src/libfio_vitastor.so" \
    fio -thread -name=test -ioengine=build/src/libfio_vitastor.so -bs=4k -direct=1 -iodepth=32 \
        -rw=randwrite -etcd=$ETCD_URL -end_fsync=1 -pool=1 -inode=1 -size=256M -runtime=10 -number_ios=1024

qemu-img convert -S 4096 -p \
    -f raw "vitastor:etcd_host=127.0.0.1\:$ETCD_PORT/v3:pool=1:inode=1:size=$((256*1024*1024))" \
    -O raw ./testdata/before.bin

for i in $(seq 1 $OSD_COUNT); do
    pid=OSD${i}_PID
    pid=${!pid}
    kill -9 $pid
done

for i in $(seq 1 $OSD_COUNT); do
    offsets=$(build/src/vitastor-disk simple-offsets --format json ./testdata/test_osd$i.bin)
    meta_offset=$(echo $offsets | jq -r .meta_offset)
    data_offset=$(echo $offsets | jq -r .data_offset)
    build/src/vitastor-disk dump-journal --json ./testdata/test_osd$i.bin 4096 0 $meta_offset >./testdata/journal_before_resize.json
    build/src/vitastor-disk dump-meta ./testdata/test_osd$i.bin 4096 $meta_offset $((data_offset-meta_offset)) >./testdata/meta_before_resize.json
    build/src/vitastor-disk resize \
        $(build/src/vitastor-disk simple-offsets --format options ./testdata/test_osd$i.bin 2>/dev/null) \
        --new_meta_offset 0 \
        --new_meta_len $((1024*1024)) \
        --new_journal_offset $((1024*1024)) \
        --new_data_offset $((128*1024*1024))
    build/src/vitastor-disk dump-journal --json ./testdata/test_osd$i.bin 4096 $((1024*1024)) $((127*1024*1024)) >./testdata/journal_after_resize.json
    build/src/vitastor-disk dump-meta ./testdata/test_osd$i.bin 4096 0 $((1024*1024)) >./testdata/meta_after_resize.json
    if ! (cat ./testdata/meta_before_resize.json ./testdata/meta_after_resize.json | \
        jq -e -s 'map([ .entries[] | del(.block) ] | sort_by(.pool, .inode, .stripe)) | .[0] == .[1] and (.[0] | length) > 1000'); then
        format_error "OSD $i metadata corrupted after resizing"
    fi
    if ! (cat ./testdata/journal_before_resize.json ./testdata/journal_after_resize.json | \
        jq -e -s 'map([ .[].entries[] | del(.crc32, .crc32_prev, .valid, .loc, .start) ]) | .[0] == .[1] and (.[0] | length) > 1'); then
        format_error "OSD $i journal corrupted after resizing"
    fi
done

$ETCDCTL del --prefix /vitastor/osd/state/

for i in $(seq 1 $OSD_COUNT); do
    build/src/vitastor-osd --osd_num $i --bind_address 127.0.0.1 $NO_SAME $OSD_ARGS --etcd_address $ETCD_URL \
        --data_device ./testdata/test_osd$i.bin \
        --meta_offset 0 \
        --journal_offset $((1024*1024)) \
        --data_offset $((128*1024*1024)) >>./testdata/osd$i.log 2>&1 &
    eval OSD${i}_PID=$!
done

qemu-img convert -S 4096 -p \
    -f raw "vitastor:etcd_host=127.0.0.1\:$ETCD_PORT/v3:pool=1:inode=1:size=$((256*1024*1024))" \
    -O raw ./testdata/after.bin

if ! cmp ./testdata/before.bin ./testdata/after.bin; then
    format_error "Data differs after resizing"
fi

format_green OK
