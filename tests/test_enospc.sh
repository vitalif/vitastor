#!/bin/bash -ex

OSD_SIZE=200
GLOBAL_CONFIG=',"client_retry_enospc":false'

. `dirname $0`/run_3osds.sh

# Should fail with ENOSPC
if $VITASTOR_FIO -bs=1M -direct=1 -iodepth=4 \
    -rw=write -pool=1 -inode=1 -size=500M -cluster_log_level=10; then
    format_error "Should get ENOSPC, but didn't"
fi

# Should fail with ENOSPC too (the idea is to try to overwrite first objects to check their rollback)
if $VITASTOR_FIO -bs=1M -direct=1 -iodepth=32 \
    -rw=write -pool=1 -inode=1 -size=500M -cluster_log_level=10; then
    format_error "Should get ENOSPC, but didn't"
fi

# Should complete OK
if ! $VITASTOR_FIO -bs=4k -direct=1 -iodepth=4 \
    -rw=randwrite -pool=1 -inode=1 -size=100M -cluster_log_level=10 -number_ios=4096; then
    format_error "Should do random writes over ENOSPC correctly, but got an error"
fi

export -n LD_PRELOAD

format_green OK
