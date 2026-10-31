#!/bin/bash -ex

. `dirname $0`/run_3osds.sh
check_qemu

# Test parallel rm of two snapshots

$VITASTOR_CLI create -s 32M repro-golden
$VITASTOR_FIO -bs=1M -direct=1 -iodepth=4 -end_fsync=1 -rw=write \
    -image=repro-golden -mirror_file=./testdata/bin/mirror.bin
qemu-img convert -p \
    -f raw "vitastor:config_path=$VITASTOR_CFG:image=repro-golden" \
    -O raw ./testdata/bin/res.bin
cmp ./testdata/bin/mirror.bin ./testdata/bin/res.bin

$VITASTOR_CLI snap-create repro-golden@init

for i in {1..10}; do
    $VITASTOR_CLI ls --tree
    $VITASTOR_CLI snap-create repro-golden@a$i
    $VITASTOR_CLI snap-create repro-golden@b$i
    $VITASTOR_CLI create --parent repro-golden@a$i -s 32M repro-cl-a$i
    $VITASTOR_CLI create --parent repro-golden@b$i -s 32M repro-cl-b$i
    ($VITASTOR_CLI rm repro-golden@a$i &>./testdata/rm_a$i.log || true) & # two concurrent rm's
    CLI1=$!
    ($VITASTOR_CLI rm repro-golden@b$i &>./testdata/rm_b$i.log || true) &
    CLI2=$!
    wait $CLI1 $CLI2
    qemu-img convert -p \
        -f raw "vitastor:config_path=$VITASTOR_CFG:image=repro-golden" \
        -O raw ./testdata/bin/res.bin
    if ! cmp ./testdata/bin/mirror.bin ./testdata/bin/res.bin; then
        sleep 100000
    fi
done

$VITASTOR_CLI ls --tree

$VITASTOR_CLI rm --matching repro-golden@a* repro-golden@b*

$VITASTOR_CLI ls --tree

format_green OK
