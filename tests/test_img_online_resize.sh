#!/bin/bash -ex

PG_COUNT=16
. `dirname $0`/run_3osds.sh

$VITASTOR_CLI create -s 100M testimg

if ! (mount | grep '/dev type devtmpfs'); then
    sudo bash -c 'mount udev /dev/ -t devtmpfs && mount devpts /dev/pts -t devpts'
fi

NBD_DEV=$(sudo build/src/client/vitastor-nbd --config_path $VITASTOR_CFG map --image testimg)
trap "sudo build/src/client/vitastor-nbd unmap $NBD_DEV"' || true; kill -9 $(jobs -p)' EXIT
UBLK_DEV=$(sudo build/src/client/vitastor-ublk --config_path $VITASTOR_CFG map --image testimg)
trap "sudo build/src/client/vitastor-ublk unmap $UBLK_DEV || true; sudo build/src/client/vitastor-nbd unmap $NBD_DEV || true; "'kill -9 $(jobs -p)' EXIT

VDUSE_DEV=
if sudo vdpa dev list; then
    # VDUSE doesn't work in Docker without host network at all
    check_qemu
    [[ -e /sys/module/vduse ]] || sudo modprobe vduse
    [[ -e /sys/module/virtio_vdpa ]] || sudo modprobe virtio-vdpa
    VDPA_ID=vpda$$
    sudo -E qemu-storage-daemon --pidfile ./testdata/qsd.pid --daemonize --blockdev '{"node-name":"'$VDPA_ID'","driver":"vitastor",
        "config-path":"'$VITASTOR_CFG'","image":"testimg","cache":{"direct":true,"no-flush":false},"discard":"unmap"}' \
        --export vduse-blk,id=$VDPA_ID,node-name=$VDPA_ID,name=$VDPA_ID,num-queues=16,queue-size=128,writable=true
    QSD_PID=$(sudo cat ./testdata/qsd.pid)
    trap "sudo kill -9 $QSD_PID; sudo build/src/client/vitastor-ublk unmap $UBLK_DEV || true; sudo build/src/client/vitastor-nbd unmap $NBD_DEV || true; "'kill -9 $(jobs -p)' EXIT
    sudo vdpa dev add name $VDPA_ID mgmtdev vduse
    trap "sudo vdpa dev del $VDPA_ID; sudo kill -9 $QSD_PID; sudo build/src/client/vitastor-ublk unmap $UBLK_DEV || true; sudo build/src/client/vitastor-nbd unmap $NBD_DEV || true; "'kill -9 $(jobs -p)' EXIT
    VDUSE_DEV=/dev/$(ls /sys/bus/vdpa/devices/$VDPA_ID/virtio*/block/)
fi

check_bdev_size() {
    if [[ $(sudo blockdev --getsize64 $1) -ne $2 ]]; then
        format_error "$1 device size is not $2"
    fi
}

check_bdev_size $NBD_DEV 104857600
check_bdev_size $UBLK_DEV 104857600
if [[ -n "$VDUSE_DEV" ]]; then
    check_bdev_size $VDUSE_DEV 104857600
fi

$VITASTOR_CLI modify --resize 200M testimg

for i in {1..10}; do
    if [[ "$(sudo blockdev --getsize64 $NBD_DEV)" -ne 209715200 || "$(sudo blockdev --getsize64 $UBLK_DEV)" -ne 209715200 || -n "$VDUSE_DEV" && "$(sudo blockdev --getsize64 $VDUSE_DEV)" -ne 209715200 ]]; then
        sleep 1
    else
        break
    fi
done

check_bdev_size $NBD_DEV 209715200
check_bdev_size $UBLK_DEV 209715200
if [[ -n "$VDUSE_DEV" ]]; then
    check_bdev_size $VDUSE_DEV 209715200
fi

format_green OK
