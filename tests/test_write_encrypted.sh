#!/bin/bash -ex

. `dirname $0`/run_3osds.sh

# Basic AES-XTS encryption test

$ETCDCTL put /vitastor/config/inode/1/1 '{"name":"testimg","size":'$((128*1024*1024))',"enc_key":"'$(openssl rand -hex 64)'"}'

$VITASTOR_FIO -bs=1M -direct=1 -iodepth=4 -mirror_file=./testdata/bin/mirror.bin -end_fsync=1 -rw=write -image=testimg

$VITASTOR_FIO -bs=4k -direct=1 -iodepth=16 -serialize_overlap=1 -mirror_file=./testdata/bin/mirror.bin -verify=md5 -end_fsync=1 -rw=randwrite -image=testimg -runtime=10

$VITASTOR_CLI dd iimg=testimg of=./testdata/bin/read.bin
diff ./testdata/bin/read.bin ./testdata/bin/mirror.bin

$VITASTOR_CLI dd iimg=testimg of=./testdata/bin/read.bin bs=4k iodepth=32
diff ./testdata/bin/read.bin ./testdata/bin/mirror.bin

format_green OK
