#!/bin/bash -ex

. `dirname $0`/run_3osds.sh

$VITASTOR_FIO -bs=128k -direct=1 -numjobs=1 -iodepth=4 \
    -rw=write -pool=1 -inode=1 -size=128M -runtime=10 -no_io_uring=1

format_green OK
