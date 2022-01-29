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
