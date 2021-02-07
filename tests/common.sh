#!/bin/bash -ex

if [ ! "$BASH_VERSION" ] ; then
    echo "Use bash to run this script ($0)" 1>&2
    exit 1
fi

format_error()
{
    echo $(echo -n -e "\033[1;31m")$1$(echo -n -e "\033[m")
    $ETCDCTL get --prefix /vitastor > ./testdata/etcd-dump.txt
    exit 1
}
format_green()
{
    echo $(echo -n -e "\033[1;32m")$1$(echo -n -e "\033[m")
}

cd `dirname $0`/..

trap 'kill -9 $(jobs -p)' EXIT

ETCD=${ETCD:-etcd}
ETCD_PORT=${ETCD_PORT:-12379}

rm -rf ./testdata
mkdir -p ./testdata

$ETCD -name etcd_test --data-dir ./testdata/etcd \
    --advertise-client-urls http://127.0.0.1:$ETCD_PORT --listen-client-urls http://127.0.0.1:$ETCD_PORT \
    --initial-advertise-peer-urls http://127.0.0.1:$((ETCD_PORT+1)) --listen-peer-urls http://127.0.0.1:$((ETCD_PORT+1)) \
    --max-txn-ops=100000 --auto-compaction-retention=10 --auto-compaction-mode=revision &>./testdata/etcd.log &
ETCD_PID=$!
ETCD_URL=127.0.0.1:$ETCD_PORT/v3
ETCDCTL="${ETCD}ctl --endpoints=http://$ETCD_URL"
