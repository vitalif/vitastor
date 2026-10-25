#!/bin/bash -ex

# Test vitastor-cli ls --tree with filters which leave out parent layers

. `dirname $0`/run_3osds.sh

$VITASTOR_CLI create -s 128M testimg
$VITASTOR_CLI snap-create testimg@snap1
$VITASTOR_CLI create --parent testimg@snap1 testclone

$VITASTOR_CLI create-pool testpool2 -s 3 -n 4 --failure_domain osd
wait_pool_up 30 2 3 4
$VITASTOR_CLI create --parent testimg@snap1 -p testpool2 testclone2

# First column of each row without the header
check_tree()
{
    local expected=$1
    shift
    local rows=$($VITASTOR_CLI ls --tree "$@" | tail -n +2 | sed 's/  .*//' | tr '\n' ';')
    if [[ "$rows" != "$expected" ]]; then
        format_error "ls --tree $* shows '$rows', expected '$expected'"
    fi
}

$VITASTOR_CLI ls --tree

check_tree 'testimg@snap1;├─ testclone;├─ testclone2;└─ testimg;'
check_tree 'testimg@snap1;└─ testimg;' 'testimg*'
# Layers whose parent is filtered out are shown as roots
check_tree 'testimg;' testimg
check_tree 'testclone;testimg;' testclone testimg
check_tree 'testclone;' -n 1
check_tree 'testclone2;' -p testpool2

if ! $VITASTOR_CLI ls --tree testimg | grep -q ' testimg@snap1 *$'; then
    format_error "ls --tree testimg should show testimg@snap1 as the parent"
fi

format_green OK
