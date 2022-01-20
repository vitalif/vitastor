#!/bin/bash -ex

if [ ! "$BASH_VERSION" ] ; then
    echo "Use bash to run this script ($0)" 1>&2
    exit 1
fi

format_error()
{
    echo $(echo -n -e "\033[1;31m")"$1"$(echo -n -e "\033[m")
    $ETCDCTL get --prefix /vitastor > ./testdata/etcd-dump.txt
    exit 1
}
format_green()
{
    echo $(echo -n -e "\033[1;32m")"$1"$(echo -n -e "\033[m")
}

cd `dirname $0`/..

trap 'kill -9 $(jobs -p)' EXIT

ETCD=${ETCD:-etcd}
ETCD_IP=${ETCD_IP:-127.0.0.1}
ETCD_PORT=${ETCD_PORT:-12379}
ETCD_COUNT=${ETCD_COUNT:-1}

if [ "$KEEP_DATA" = "" ]; then
    rm -rf ./testdata
    mkdir -p ./testdata
fi

ETCD_URL="http://$ETCD_IP:$ETCD_PORT"
ETCD_CLUSTER="etcd1=http://$ETCD_IP:$((ETCD_PORT+1))"
for i in $(seq 2 $ETCD_COUNT); do
    ETCD_URL="$ETCD_URL,http://$ETCD_IP:$((ETCD_PORT+2*i-2))"
    ETCD_CLUSTER="$ETCD_CLUSTER,etcd$i=http://$ETCD_IP:$((ETCD_PORT+2*i-1))"
done
ETCDCTL="${ETCD}ctl --endpoints=$ETCD_URL"

start_etcd()
{
    local i=$1
    $ETCD -name etcd$i --data-dir ./testdata/etcd$i \
        --advertise-client-urls http://$ETCD_IP:$((ETCD_PORT+2*i-2)) --listen-client-urls http://$ETCD_IP:$((ETCD_PORT+2*i-2)) \
        --initial-advertise-peer-urls http://$ETCD_IP:$((ETCD_PORT+2*i-1)) --listen-peer-urls http://$ETCD_IP:$((ETCD_PORT+2*i-1)) \
        --initial-cluster-token vitastor-tests-etcd --initial-cluster-state new \
        --initial-cluster "$ETCD_CLUSTER" \
        --max-txn-ops=100000 --auto-compaction-retention=10 --auto-compaction-mode=revision &>./testdata/etcd$i.log &
    eval ETCD${i}_PID=$!
}

for i in $(seq 1 $ETCD_COUNT); do
    start_etcd $i
done
if [ $ETCD_COUNT -gt 1 ]; then
    sleep 1
fi

echo leak:fio >> testdata/lsan-suppress.txt
echo leak:tcmalloc >> testdata/lsan-suppress.txt
echo leak:ceph >> testdata/lsan-suppress.txt
echo leak:librbd >> testdata/lsan-suppress.txt
echo leak:_M_mutate >> testdata/lsan-suppress.txt
echo leak:_M_assign >> testdata/lsan-suppress.txt
export LSAN_OPTIONS=report_objects=true:suppressions=`pwd`/testdata/lsan-suppress.txt
export ASAN_OPTIONS=verify_asan_link_order=false
