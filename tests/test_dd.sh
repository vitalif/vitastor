#!/bin/bash -ex

. `dirname $0`/run_3osds.sh

# pipe in - pipe out
dd if=/dev/urandom of=./testdata/testfile bs=1M count=128
$VITASTOR_CLI dd oimg=testimg iodepth=4 bs=1M count=128 < ./testdata/testfile
$VITASTOR_CLI dd iimg=testimg iodepth=4 bs=1M count=128 > ./testdata/testfile1
diff ./testdata/testfile ./testdata/testfile1
rm ./testdata/testfile1

# snapshot
dd if=/dev/urandom of=./testdata/over bs=1M count=4
dd if=./testdata/over of=./testdata/testfile bs=1M seek=17 conv=notrunc
$VITASTOR_CLI snap-create testimg@snap1
$VITASTOR_CLI dd iodepth=4 if=./testdata/over oimg=testimg bs=1M seek=17
$VITASTOR_CLI dd iodepth=4 iimg=testimg of=./testdata/testfile1
diff ./testdata/testfile ./testdata/testfile1

format_green OK
