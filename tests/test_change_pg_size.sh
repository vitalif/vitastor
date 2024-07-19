#!/bin/bash -ex

PG_COUNT=16
SCHEME=${SCHEME:-replicated}

. `dirname $0`/run_3osds.sh

try_change()
{
    n=$1
    s=$2

    $ETCDCTL put /vitastor/config/pools '{"1":{'$POOLCFG',"pg_size":'$s',"pg_minsize":'$PG_MINSIZE',"pg_count":'$n'}}'

    for i in {1..10}; do
        ($ETCDCTL get /vitastor/pg/config --print-value-only |\
            jq -s -e '(.[0].items["1"] | map(  ([ .osd_set[] | select(. != 0) ] | length) == '$s'  ) | length == '$n')
                and ([ .[0].items["1"] | map(.osd_set)[][] ] | sort | unique == ["1","2","3"])') && \
            ($ETCDCTL get --prefix /vitastor/pg/state/ --print-value-only | jq -s -e '([ .[] | select(.state == ["active"]) ] | length) == '$n'') && \
            break
        sleep 1
    done

    if ! ($ETCDCTL get /vitastor/pg/config --print-value-only |\
        jq -s -e '(.[0].items["1"] | map(  ([ .osd_set[] | select(. != 0) ] | length) == '$s'  ) | length == '$n')
            and ([ .[0].items["1"] | map(.osd_set)[][] ] | sort | unique == ["1","2","3"])'); then
        $ETCDCTL get /vitastor/pg/config
        $ETCDCTL get --prefix /vitastor/pg/state/
        format_error "FAILED: PG SIZE NOT CHANGED OR SOME OSDS DO NOT HAVE PGS"
    fi

    if ! ($ETCDCTL get --prefix /vitastor/pg/state/ --print-value-only | jq -s -e '([ .[] | select(.state == ["active"]) ] | length) == '$n); then
        $ETCDCTL get /vitastor/pg/config
        $ETCDCTL get --prefix /vitastor/pg/state/
        format_error "FAILED: PGS NOT UP AFTER PG SIZE CHANGE"
    fi
}

try_change 16 2

format_green OK
