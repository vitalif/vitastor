#!/bin/bash -ex

ANTIETCD=1
. `dirname $0`/common.sh

[[ -e build/src/disk_tool/vitastor-disk-test ]] || ln -s vitastor-disk build/src/disk_tool/vitastor-disk-test

dd if=/dev/zero of=./testdata/bin/test_osd1.bin bs=1 count=1 seek=$((100*1024*1024*1024-1))
LOOP1=$(sudo losetup --show -f ./testdata/bin/test_osd1.bin)
trap "kill -9 $(jobs -p) || true; sudo losetup -d $LOOP1"' || true' EXIT
dd if=/dev/zero of=./testdata/bin/test_meta.bin bs=1 count=1 seek=$((1024*1024*1024-1))
LOOP2=$(sudo losetup --show -f ./testdata/bin/test_meta.bin)
trap "kill -9 $(jobs -p) || true; sudo losetup -d $LOOP1 $LOOP2"' || true' EXIT

# also test prepare --hybrid :)
# non-vitastor random type UUID to prevent udev activation
mount | grep '/dev type devtmpfs' || sudo mount udev /dev/ -t devtmpfs
sudo build/src/disk_tool/vitastor-disk-test prepare --meta_format 2 --no_init 1 --meta_reserve 1x,1M \
    --block_size 131072 --osd_num 987654 --part_type_uuid 0df42ae0-3695-4395-a957-7d5ff3645c56 \
    --hybrid --fast-devices $LOOP2 $LOOP1

# write almost empty journal
node <<EOF > ./testdata/journal.json
console.log(JSON.stringify([
    {"type":"start","start":"0x1000"},
    {"type":"big_write_instant","inode":"0x1000000000001","stripe":"0xc60000","ver":"10","offset":0,"len":131072,"loc":"0x18ffdc0000","bitmap":"ffffffff"}
]));
EOF
sudo build/src/disk_tool/vitastor-disk write-journal ${LOOP1}p1 < ./testdata/journal.json
sudo build/src/disk_tool/vitastor-disk dump-journal --json --format data ${LOOP1}p1 | jq -S '[ .[] | del(.crc32, .crc32_prev) ]' > ./testdata/j2.json
jq -S '[ .[] + {"valid":true} ]' < ./testdata/journal.json > ./testdata/j1.json
diff ./testdata/j1.json ./testdata/j2.json

# write fake metadata items in the end
DATA_DEV_SIZE=$(sudo blockdev --getsize64 ${LOOP1}p1)
BLOCK_COUNT=$(((DATA_DEV_SIZE-4096)/128/1024))
node <<EOF > ./testdata/meta.json
console.log(JSON.stringify({
    version: "0.9",
    meta_block_size: 4096,
    data_block_size: 131072,
    bitmap_granularity: 4096,
    data_csum_type: "none",
    csum_block_size: 0,
    entries: [ ...new Array(100).keys() ].map(i => ({
        block: ($BLOCK_COUNT-100)+i,
        pool: 1,
        inode: "0x1",
        stripe: "0x"+Number(i*0x20000).toString(16),
        version: 10,
        bitmap: "ffffffff",
        ext_bitmap: "ffffffff",
    })),
}));
EOF

# also test write & dump
sudo build/src/disk_tool/vitastor-disk write-meta ${LOOP1}p1 < ./testdata/meta.json
sudo build/src/disk_tool/vitastor-disk dump-meta ${LOOP1}p1 > ./testdata/compare.json
jq -S < ./testdata/meta.json > ./testdata/1.json
jq -S < ./testdata/compare.json > ./testdata/2.json
diff ./testdata/1.json ./testdata/2.json

# move journal & meta back, data will become smaller; end indexes should be shifted by -1251
sudo build/src/disk_tool/vitastor-disk-test resize --move-journal '' --move-meta '' ${LOOP1}p1
sudo build/src/disk_tool/vitastor-disk dump-meta ${LOOP1}p1 | jq -S > ./testdata/2.json
jq -S '. + {"entries": [ .entries[] | (. + { "block": (.block-1251) }) ]}' < ./testdata/meta.json > ./testdata/1.json
diff ./testdata/1.json ./testdata/2.json
sudo build/src/disk_tool/vitastor-disk dump-journal --json --format data ${LOOP1}p1 | jq -S '[ .[] | del(.crc32, .crc32_prev) ]' > ./testdata/j2.json
jq -S '[ (.[] + {"valid":true}) | (if .type == "big_write_instant" then . + {"loc":"0x18f6160000"} else . end) ]' < ./testdata/journal.json > ./testdata/j1.json
diff ./testdata/j1.json ./testdata/j2.json

# move journal & meta out, data will become larger; end indexes should be shifted back by +1251
sudo build/src/disk_tool/vitastor-disk-test resize --move-journal ${LOOP2}p1 --move-meta ${LOOP2}p2 ${LOOP1}p1
sudo build/src/disk_tool/vitastor-disk dump-meta ${LOOP1}p1 | jq -S > ./testdata/2.json
jq -S < ./testdata/meta.json > ./testdata/1.json
diff ./testdata/1.json ./testdata/2.json
jq -S '[ .[] + {"valid":true} ]' < ./testdata/journal.json > ./testdata/j1.json
sudo build/src/disk_tool/vitastor-disk dump-journal --json --format data ${LOOP1}p1 | jq -S '[ .[] | del(.crc32, .crc32_prev) ]' > ./testdata/j2.json

# reduce data device size by exactly 128k * 99 (occupied blocks); exactly 1 should be left in place :)
sudo build/src/disk_tool/vitastor-disk-test resize --data-size $((DATA_DEV_SIZE-128*1024*99)) ${LOOP1}p1
sudo build/src/disk_tool/vitastor-disk dump-meta ${LOOP1}p1 | jq -S > ./testdata/2.json
jq -S '. + {"entries": ([ .entries[] | (. + { "block": (.block | if . > '$BLOCK_COUNT'-100 then .-('$BLOCK_COUNT'-100+1) else '$BLOCK_COUNT'-100 end) }) ] | .[1:] + [ .[0] ])}' < ./testdata/meta.json > ./testdata/1.json
diff ./testdata/1.json ./testdata/2.json
jq -S '[ .[] + {"valid":true} ]' < ./testdata/journal.json > ./testdata/j1.json
sudo build/src/disk_tool/vitastor-disk dump-journal --json --format data ${LOOP1}p1 | jq -S '[ .[] | del(.crc32, .crc32_prev) ]' > ./testdata/j2.json

# extend data device size to maximum
sudo build/src/disk_tool/vitastor-disk-test resize --data-size max ${LOOP1}p1
sudo build/src/disk_tool/vitastor-disk dump-meta ${LOOP1}p1 | jq -S > ./testdata/2.json
diff ./testdata/1.json ./testdata/2.json

format_green OK
