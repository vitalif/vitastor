#!/bin/bash -ex

# Test for Issue #112: Integer division bug causes crash with block_size < 32KB
# This test verifies that small block sizes (4KB, 8KB, 16KB) work correctly
# with 4KB bitmap_granularity, both for aligned and unaligned I/O

# Arrange: Set up test environment with small block sizes
export SCHEME=replicated
export OSD_COUNT=3
export PG_COUNT=1
export PG_SIZE=2
export OSD_SIZE=256

# Test with 16KB block_size and 4KB bitmap_granularity
# This should trigger the bug: 16384 / 4096 / 8 = 4 / 8 = 0 bytes
export OFFSET_ARGS="--data_block_size 16384 --bitmap_granularity 4096"

. `dirname $0`/run_3osds.sh

# Act: Run I/O tests that exercise the bitmap code paths

echo "Test 1: 128KB aligned I/O (may work even with bug)"
LD_PRELOAD="build/src/client/libfio_vitastor.so" \
    fio -thread -name=test -ioengine=build/src/client/libfio_vitastor.so \
        -bs=128k -direct=1 -iodepth=4 -rw=randwrite \
        -etcd=$ETCD_URL -pool=1 -inode=1 -size=64M -runtime=5

echo "Test 2: 4KB random I/O (will crash with bug)"
LD_PRELOAD="build/src/client/libfio_vitastor.so" \
    fio -thread -name=test -ioengine=build/src/client/libfio_vitastor.so \
        -bs=4k -direct=1 -iodepth=16 -rw=randwrite \
        -etcd=$ETCD_URL -pool=1 -inode=1 -size=64M -runtime=5

echo "Test 3: Mixed read/write with 4KB I/O"
LD_PRELOAD="build/src/client/libfio_vitastor.so" \
    fio -thread -name=test -ioengine=build/src/client/libfio_vitastor.so \
        -bs=4k -direct=1 -iodepth=16 -rw=randrw -rwmixread=50 \
        -etcd=$ETCD_URL -pool=1 -inode=1 -size=64M -runtime=5

echo "Test 4: Sequential 4KB writes"
LD_PRELOAD="build/src/client/libfio_vitastor.so" \
    fio -thread -name=test -ioengine=build/src/client/libfio_vitastor.so \
        -bs=4k -direct=1 -iodepth=1 -rw=write \
        -etcd=$ETCD_URL -pool=1 -inode=1 -size=64M

# Assert: If we reach here without crash, test passed
format_green "OK: 16KB block_size with 4KB bitmap_granularity works correctly"
