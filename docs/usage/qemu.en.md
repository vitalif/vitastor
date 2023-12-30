[Documentation](../../README.md#documentation) → Usage → QEMU and qemu-img

-----

[Читать на русском](qemu.ru.md)

# QEMU and qemu-img

## QEMU

You need patched QEMU version to use Vitastor driver. Pre-built [packages](../installation/packages.en.md) are available.

To start a VM using plain QEMU command-line with Vitastor disk, use the following commands:

Old syntax (-drive):

```
qemu-system-x86_64 -enable-kvm -m 1024 \
    -drive 'file=vitastor:image=debian9',
        format=raw,if=none,id=drive-virtio-disk0,cache=none \
    -device 'virtio-blk-pci,scsi=off,bus=pci.0,addr=0x5,drive=drive-virtio-disk0,
        id=virtio-disk0,bootindex=1,write-cache=off' \
    -vnc 0.0.0.0:0
```

Etcd address may be specified explicitly by adding `:etcd_host=192.168.7.2\:2379/v3` to `file=`.
Configuration file path may be overriden by adding `:config_path=/etc/vitastor/vitastor.conf`.

New syntax (-blockdev):

```
qemu-system-x86_64 -enable-kvm -m 1024 \
    -blockdev '{"node-name":"drive-virtio-disk0","driver":"vitastor","image":"debian9",
        "cache":{"direct":true,"no-flush":false},"auto-read-only":true,"discard":"unmap"}' \
    -device 'virtio-blk-pci,scsi=off,bus=pci.0,addr=0x5,drive=drive-virtio-disk0,
        id=virtio-disk0,bootindex=1,write-cache=off' \
    -vnc 0.0.0.0:0
```

With a separate I/O thread:

```
qemu-system-x86_64 -enable-kvm -m 1024 \
    -object iothread,id=vitastor1 \
    -blockdev '{"node-name":"drive-virtio-disk0","driver":"vitastor","image":"debian9",
        "cache":{"direct":true,"no-flush":false},"auto-read-only":true,"discard":"unmap"}' \
    -device 'virtio-blk-pci,iothread=vitastor1,scsi=off,bus=pci.0,addr=0x5,drive=drive-virtio-disk0,
        id=virtio-disk0,bootindex=1,write-cache=off' \
    -vnc 0.0.0.0:0
```

You can also specify inode ID, pool and size manually instead of `:image=<IMAGE>` option: `:pool=<POOL>:inode=<INODE>:size=<SIZE>`.

## qemu-img

For qemu-img, you should use `vitastor:image=<IMAGE>[:etcd_host=<HOST>]` as filename.

For example, to upload a VM image into Vitastor, run:

```
qemu-img convert -f qcow2 debian10.qcow2 -p -O raw 'vitastor:image=debian10'
```

You can also specify `:pool=<POOL>:inode=<INODE>:size=<SIZE>` instead of `:image=<IMAGE>`
if you don't want to use inode metadata.

### Exporting snapshots

Starting with 0.8.4, you can also export individual layers (snapshot diffs) using `qemu-img`.

Suppose you have an image `testimg` and a snapshot `testimg@0` created with `vitastor-cli snap-create testimg@0`.

Then you can export the `testimg@0` snapshot and the data written to `testimg` after creating
the snapshot separately using the following commands (key points are using `skip-parents=1` and
`-B backing_file` option):

```
qemu-img convert -f raw 'vitastor:image=testimg@0' \
    -O qcow2 testimg_0.qcow2

qemu-img convert -f raw 'vitastor:image=testimg:skip-parents=1' \
    -O qcow2 -o 'cluster_size=4k' -B testimg_0.qcow2 testimg.qcow2
```

In fact, with `cluster_size=4k` any QCOW2 file can be used instead `-B testimg_0.qcow2`, even an empty one.

QCOW2 `cluster_size=4k` option is required if you want `testimg.qcow2` to contain only the data
overwritten  **exactly** in the child layer. With the default 64 KB QCOW2 cluster size you'll
get a bit of extra data from parent layers, i.e. a 4 KB overwrite will result in `testimg.qcow2`
containing 64 KB of data. And this extra data will be taken by `qemu-img` from the file passed
in `-B` option, so you really need 4 KB cluster if you use an empty image in `-B`.

After this procedure you'll get two chained QCOW2 images. To detach `testimg.qcow2` from
its parent, run:

```
qemu-img rebase -u -b '' testimg.qcow2
```

This can be used for backups. Just note that exporting an image that is currently being written to
is of course unsafe and doesn't produce a consistent result, so only export snapshots if you do this
on a live VM.

## vhost-user-blk

QEMU, starting with 6.0, includes support for attaching disks via a separate
userspace worker process, called `vhost-user-blk`. It usually has slightly (20-30 us)
lower latency.

Example commands to use it with Vitastor:

```
qemu-storage-daemon \
    --daemonize \
    --blockdev '{"node-name":"drive-virtio-disk1","driver":"vitastor","image":"testosd1","cache":{"direct":true,"no-flush":false},"auto-read-only":true,"discard":"unmap"}' \
    --export type=vhost-user-blk,id=vitastor1,node-name=drive-virtio-disk1,addr.type=unix,addr.path=/run/vitastor1-user-blk.sock,writable=on,num-queues=1

qemu-system-x86_64 -enable-kvm -m 2048 -M accel=kvm,memory-backend=mem \
    -object memory-backend-memfd,id=mem,size=2G,share=on \
    -chardev socket,id=vitastor1,reconnect=1,path=/run/vitastor1-user-blk.sock \
    -device vhost-user-blk-pci,chardev=vitastor1,num-queues=1,config-wce=off \
    -vnc 0.0.0.0:0
```

memfd memory-backend is crucial, vhost-user-blk does not work without it.

## VDUSE

Linux kernel, starting with version 5.15, supports a new interface for attaching virtual disks
to the host - VDUSE (vDPA Device in Userspace). QEMU, starting with 7.2, has support for
exporting QEMU block devices over this protocol using qemu-storage-daemon.

VDUSE is currently the best interface to attach Vitastor disks as kernel devices because:
- It avoids data copies and thus achieves much better performance than [NBD](nbd.en.md)
- It doesn't have NBD timeout problem - the device doesn't die if an operation executes for too long
- It doesn't have hung device problem - if the userspace process dies it can be restarted (!)
  and block device will continue operation
- It doesn't seem to have the device number limit

Example performance comparison:

|                      | direct fio  | NBD         | VDUSE       |
|----------------------|-------------|-------------|-------------|
| linear write         | 3.85 GB/s   | 1.12 GB/s   | 3.85 GB/s   |
| 4k random write Q128 | 240000 iops | 120000 iops | 178000 iops |
| 4k random write Q1   | 9500 iops   | 7620 iops   | 7640 iops   |
| linear read          | 4.3 GB/s    | 1.8 GB/s    | 2.85 GB/s   |
| 4k random read Q128  | 287000 iops | 140000 iops | 189000 iops |
| 4k random read Q1    | 9600 iops   | 7640 iops   | 7780 iops   |

To try VDUSE you need at least Linux 5.15, built with VDUSE support
(CONFIG_VDPA=m, CONFIG_VDPA_USER=m, CONFIG_VIRTIO_VDPA=m).

Debian Linux kernels have these options disabled by now, so if you want to try it on Debian,
use a kernel from Ubuntu [kernel-ppa/mainline](https://kernel.ubuntu.com/~kernel-ppa/mainline/), Proxmox,
or build modules for Debian kernel manually:

```
mkdir build
cd build
apt-get install linux-headers-`uname -r`
apt-get build-dep linux-image-`uname -r`-unsigned
apt-get source linux-image-`uname -r`-unsigned
cd linux*/drivers/vdpa
make -C /lib/modules/`uname -r`/build M=$PWD CONFIG_VDPA=m CONFIG_VDPA_USER=m CONFIG_VIRTIO_VDPA=m -j8 modules modules_install
cat Module.symvers >> /lib/modules/`uname -r`/build/Module.symvers
cd ../virtio
make -C /lib/modules/`uname -r`/build M=$PWD CONFIG_VDPA=m CONFIG_VDPA_USER=m CONFIG_VIRTIO_VDPA=m -j8 modules modules_install
depmod -a
```

You also need `vdpa` tool from the `iproute2` package.

Commands to attach Vitastor image as a VDUSE device:

```
modprobe vduse
modprobe virtio-vdpa
qemu-storage-daemon --daemonize --blockdev '{"node-name":"test1","driver":"vitastor",\
  "etcd-host":"192.168.7.2:2379/v3","image":"testosd1","cache":{"direct":true,"no-flush":false},"discard":"unmap"}' \
  --export vduse-blk,id=test1,node-name=test1,name=test1,num-queues=16,queue-size=128,writable=true
vdpa dev add name test1 mgmtdev vduse
```

After running these commands, `/dev/vda` device will appear in the system and you'll be able to
use it as a normal disk.

To remove the device:

```
vdpa dev del test1
kill <qemu-storage-daemon_process_PID>
```
