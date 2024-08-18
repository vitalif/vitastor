#!/bin/bash -ex

. `dirname $0`/run_3osds.sh

# pipe in - pipe out
dd if=/dev/urandom of=./testdata/testfile bs=1M count=128
build/src/cmd/vitastor-cli --etcd_address $ETCD_URL dd oimg=testimg iodepth=4 bs=1M count=128 < ./testdata/testfile
build/src/cmd/vitastor-cli --etcd_address $ETCD_URL dd iimg=testimg iodepth=4 bs=1M count=128 > ./testdata/testfile1
diff ./testdata/testfile ./testdata/testfile1
rm ./testdata/testfile1

# snapshot
dd if=/dev/urandom of=./testdata/over bs=1M count=4
dd if=./testdata/over of=./testdata/testfile bs=1M seek=17 conv=notrunc
build/src/cmd/vitastor-cli --etcd_address $ETCD_URL snap-create testimg@snap1
build/src/cmd/vitastor-cli --etcd_address $ETCD_URL dd iodepth=4 if=./testdata/over oimg=testimg bs=1M seek=17
build/src/cmd/vitastor-cli --etcd_address $ETCD_URL dd iodepth=4 iimg=testimg of=./testdata/testfile1
diff ./testdata/testfile ./testdata/testfile1

format_green OK
