#!/bin/bash -ex

OSD_SIZE=200
GLOBAL_CONFIG=',"client_retry_enospc":false'

. `dirname $0`/run_3osds.sh

export LD_PRELOAD="build/src/client/libfio_vitastor.so"

# Should fail with ENOSPC
if fio -thread -name=test -ioengine=build/src/client/libfio_vitastor.so -bs=1M -direct=1 -iodepth=4 \
    -rw=write -etcd=$ETCD_URL -pool=1 -inode=1 -size=500M -cluster_log_level=10; then
    format_error "Should get ENOSPC, but didn't"
fi

# Should fail with ENOSPC too (the idea is to try to overwrite first objects to check their rollback)
if fio -thread -name=test -ioengine=build/src/client/libfio_vitastor.so -bs=1M -direct=1 -iodepth=32 \
    -rw=write -etcd=$ETCD_URL -pool=1 -inode=1 -size=500M -cluster_log_level=10; then
    format_error "Should get ENOSPC, but didn't"
fi

# Should complete OK
if ! fio -thread -name=test -ioengine=build/src/client/libfio_vitastor.so -bs=4k -direct=1 -iodepth=4 \
    -rw=randwrite -etcd=$ETCD_URL -pool=1 -inode=1 -size=100M -cluster_log_level=10 -number_ios=4096; then
    format_error "Should do random writes over ENOSPC correctly, but got an error"
fi

export -n LD_PRELOAD

format_green OK
