#!/bin/bash -ex

if [ ! "$BASH_VERSION" ] ; then
    echo "Use bash to run this script ($0)" 1>&2
    exit 1
fi

format_error()
{
    echo $(echo -n -e "\033[1;31m")"$0 $1"$(echo -n -e "\033[m")
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
ANTIETCD=${ANTIETCD}
USE_RAMDISK=${USE_RAMDISK}

RAMDISK=/run/user/$(id -u)
findmnt $RAMDISK >/dev/null || (sudo mkdir -p $RAMDISK && sudo mount -t tmpfs tmpfs $RAMDISK)

if [[ -z "$KEEP_DATA" ]]; then
    rm -rf ./testdata
    rm -rf /run/user/$(id -u)/testdata_etcd* /run/user/$(id -u)/testdata_bin
    mkdir -p ./testdata
    if [[ -n "$USE_RAMDISK" ]]; then
        OSD_ARGS="$OSD_ARGS --data_io cached"
        mkdir -p /run/user/$(id -u)/testdata_bin
        ln -s /run/user/$(id -u)/testdata_bin ./testdata/bin
    else
        mkdir -p ./testdata/bin
    fi
fi

ETCD_URL="http://$ETCD_IP:$ETCD_PORT"
for i in $(seq 2 $ETCD_COUNT); do
    ETCD_URL="$ETCD_URL,http://$ETCD_IP:$((ETCD_PORT+2*i-2))"
done

start_etcd()
{
    local i=$1
    if [[ -z "$ANTIETCD" ]]; then
        ionice -c2 -n0 $ETCD -name etcd$i --data-dir $RAMDISK/testdata_etcd$i \
            --advertise-client-urls http://$ETCD_IP:$((ETCD_PORT+2*i-2)) --listen-client-urls http://$ETCD_IP:$((ETCD_PORT+2*i-2)) \
            --initial-advertise-peer-urls http://$ETCD_IP:$((ETCD_PORT+2*i-1)) --listen-peer-urls http://$ETCD_IP:$((ETCD_PORT+2*i-1)) \
            --initial-cluster-token vitastor-tests-etcd --initial-cluster-state new \
            --initial-cluster "$ETCD_CLUSTER" --max-request-bytes=104857600 \
            --max-txn-ops=100000 --auto-compaction-retention=10 --auto-compaction-mode=revision &>./testdata/etcd$i.log &
        eval ETCD${i}_PID=$!
    else
        node mon/mon-main.js $MON_PARAMS --antietcd_port $((ETCD_PORT+2*i-2)) --etcd_address $ETCD_URL --etcd_prefix "/vitastor" --verbose 1 >>./testdata/mon$i.log 2>&1 &
        eval ETCD${i}_PID=$!
    fi
}

start_etcd_cluster()
{
    ETCD_CLUSTER="etcd1=http://$ETCD_IP:$((ETCD_PORT+1))"
    for i in $(seq 2 $ETCD_COUNT); do
        ETCD_CLUSTER="$ETCD_CLUSTER,etcd$i=http://$ETCD_IP:$((ETCD_PORT+2*i-1))"
    done
    for i in $(seq 1 $ETCD_COUNT); do
        start_etcd $i
    done
}

wait_etcd()
{
    for i in {1..30}; do
        $ETCDCTL --dial-timeout=1s --command-timeout=1s get --prefix / && break
        if [[ $i = 30 ]]; then
            format_error "Failed to start etcd"
        fi
        sleep 1
    done
}

if [[ -n "$ANTIETCD" ]]; then
    ETCDCTL="node mon/node_modules/.bin/anticli -e $ETCD_URL"
    MON_PARAMS="--use_antietcd 1 --antietcd_data_dir ./testdata --antietcd_persist_interval 500 $MON_PARAMS"
else
    ETCDCTL="${ETCD}ctl --endpoints=$ETCD_URL --dial-timeout=5s --command-timeout=10s"
    MON_PARAMS="$MON_PARAMS"
    start_etcd_cluster
fi

echo leak:fio >> testdata/lsan-suppress.txt
echo leak:tcmalloc >> testdata/lsan-suppress.txt
echo leak:ceph >> testdata/lsan-suppress.txt
echo leak:librbd >> testdata/lsan-suppress.txt
echo leak:_M_mutate >> testdata/lsan-suppress.txt
echo leak:_M_assign >> testdata/lsan-suppress.txt
export LSAN_OPTIONS=report_objects=true:suppressions=`pwd`/testdata/lsan-suppress.txt
export ASAN_OPTIONS=verify_asan_link_order=false:abort_on_error=1
