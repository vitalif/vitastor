#!/bin/bash -ex
GLOBAL_CONFIG=',"client_enable_writeback":false'
IMMEDIATE_COMMIT=1
PG_COUNT=16
. "$(dirname "$0")/run_3osds.sh"
$VITASTOR_CLI create -s 10G fsmeta
$VITASTOR_CLI modify-pool --used-for-app fs:fsmeta testpool

start_nfs() {
    local port=$1 var=$2
    build/src/nfs/vitastor-nfs --config_path "$VITASTOR_CFG" start --fs fsmeta --portmap 0 --port "$port" --foreground 1 --trace 1 >>"./testdata/nfs-$port.log" 2>&1 &
    printf -v "$var" '%s' "$!"
}

mount_nfs() {
    local port=$1 mnt=$2
    sudo mount localhost:/ "$mnt" -o port=$port,mountport=$port,nfsvers=3,sync,soft,timeo=50,retrans=2,nolock,tcp
}

dir_list() { LC_ALL=C ls -1U --color=never -A "$1"; }
prefill_dir() {
    local mnt=$1 count=$2 i
    for i in $(seq 1 "$count"); do : > "$mnt/$(printf 'prefill-%08d' "$i")"; done
}

NFS1_PID=""; NFS2_PID=""
MNT_A="$(pwd)/testdata/nfs-a"; MNT_B="$(pwd)/testdata/nfs-b"
cleanup_all() {
    sudo umount -f "$MNT_A" 2>/dev/null || true
    sudo umount -f "$MNT_B" 2>/dev/null || true
    [ -n "$NFS1_PID" ] && kill "$NFS1_PID" 2>/dev/null || true
    [ -n "$NFS2_PID" ] && kill "$NFS2_PID" 2>/dev/null || true
    kill -9 $(jobs -p) 2>/dev/null || true
}
trap cleanup_all EXIT

start_nfs 2050 NFS1_PID
start_nfs 2051 NFS2_PID
mkdir -p "$MNT_A" "$MNT_B"
mount_nfs 2050 "$MNT_A"
mkdir -p "$MNT_A/dir"

POLL_COUNT=${POLL_COUNT:-30}
POLL_INTERVAL=${POLL_INTERVAL:-1}
PREFILL_COUNT=${PREFILL_COUNT:-1000}

echo "Preparing $PREFILL_COUNT prefills"
prefill_dir "$MNT_A/dir" "$PREFILL_COUNT"
sync

# Mount B after prefill so baseline is established from already-populated dir.
mount_nfs 2051 "$MNT_B"
BASE_A_COUNT=$(dir_list "$MNT_A/dir" | wc -l)
BASE_B_COUNT=$(dir_list "$MNT_B/dir" | wc -l)
[ "$BASE_A_COUNT" -ge "$PREFILL_COUNT" ] || format_error "A baseline too small ($BASE_A_COUNT < $PREFILL_COUNT)"
[ "$BASE_B_COUNT" -ge "$PREFILL_COUNT" ] || format_error "B baseline too small ($BASE_B_COUNT < $PREFILL_COUNT)"

echo "Baseline ok: A=$BASE_A_COUNT B=$BASE_B_COUNT"
BUG_FILE=$(printf '000-new-%s' "$(date +%s%N)")
# Warm B READDIR cache first, then write on A.
# This intentionally checks cross-instance visibility after B has an old view.
dir_list "$MNT_B/dir" >/dev/null
touch "$MNT_A/dir/$BUG_FILE"
sync
dir_list "$MNT_A/dir" | grep -Fxq "$BUG_FILE" || format_error "A READDIR misses $BUG_FILE"
seen=0
for i in $(seq 1 "$POLL_COUNT"); do
    if dir_list "$MNT_B/dir" | grep -Fxq "$BUG_FILE"; then seen=1; break; fi
    sleep "$POLL_INTERVAL"
done
[ "$seen" -eq 1 ] || format_error "Detected READDIR inconsistency: A lists $BUG_FILE but B misses it after $POLL_COUNT polls"
format_green "No cross-instance READDIR inconsistency in single-run check"
