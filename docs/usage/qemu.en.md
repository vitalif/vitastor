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
    -drive 'file=vitastor:etcd_host=192.168.7.2\:2379/v3:image=debian9',
        format=raw,if=none,id=drive-virtio-disk0,cache=none \
    -device 'virtio-blk-pci,scsi=off,bus=pci.0,addr=0x5,drive=drive-virtio-disk0,
        id=virtio-disk0,bootindex=1,write-cache=off' \
    -vnc 0.0.0.0:0
```

New syntax (-blockdev):

```
qemu-system-x86_64 -enable-kvm -m 1024 \
    -blockdev '{"node-name":"drive-virtio-disk0","driver":"vitastor","image":"debian9",
        "cache":{"direct":true,"no-flush":false},"auto-read-only":true,"discard":"unmap"}' \
    -device 'virtio-blk-pci,scsi=off,bus=pci.0,addr=0x5,drive=drive-virtio-disk0,
        id=virtio-disk0,bootindex=1,write-cache=off' \
    -vnc 0.0.0.0:0
```

## qemu-img

For qemu-img, you should use `vitastor:etcd_host=<HOST>:image=<IMAGE>` as filename.

For example, to upload a VM image into Vitastor, run:

```
qemu-img convert -f qcow2 debian10.qcow2 -p -O raw 'vitastor:etcd_host=192.168.7.2\:2379/v3:image=debian10'
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
qemu-img convert -f raw 'vitastor:etcd_host=192.168.7.2\:2379/v3:image=testimg@0' \
    -O qcow2 testimg_0.qcow2

qemu-img convert -f raw 'vitastor:etcd_host=192.168.7.2\:2379/v3:image=testimg:skip-parents=1' \
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
