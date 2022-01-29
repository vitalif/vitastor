[Документация](../../README-ru.md#документация) → Использование → QEMU и qemu-img

-----

[Read in English](qemu.en.md)

# QEMU и qemu-img

## QEMU

Чтобы использовать Vitastor-диски в QEMU, вам нужна доработанная версия QEMU.
Её можно установить [из пакетов](../installation/packages.ru.md).

Для ручного запуска виртуальной машины QEMU из командной строки с Vitastor-диском
используйте один из следующих вариантов команд:

Старый синтаксис (-drive):

```
qemu-system-x86_64 -enable-kvm -m 1024 \
    -drive 'file=vitastor:etcd_host=192.168.7.2\:2379/v3:image=debian9',
        format=raw,if=none,id=drive-virtio-disk0,cache=none \
    -device 'virtio-blk-pci,scsi=off,bus=pci.0,addr=0x5,drive=drive-virtio-disk0,
        id=virtio-disk0,bootindex=1,write-cache=off' \
    -vnc 0.0.0.0:0
```

Новый синтаксис (-blockdev):

```
qemu-system-x86_64 -enable-kvm -m 1024 \
    -blockdev '{"node-name":"drive-virtio-disk0","driver":"vitastor","image":"debian9",
        "cache":{"direct":true,"no-flush":false},"auto-read-only":true,"discard":"unmap"}' \
    -device 'virtio-blk-pci,scsi=off,bus=pci.0,addr=0x5,drive=drive-virtio-disk0,
        id=virtio-disk0,bootindex=1,write-cache=off' \
    -vnc 0.0.0.0:0
```

Вместо `:image=<IMAGE>` также можно указывать номер инода, пул и размер: `:pool=<POOL>:inode=<INODE>:size=<SIZE>`.

## qemu-img

Для qemu-img используйте строку `vitastor:etcd_host=<HOST>:image=<IMAGE>` в качестве имени файла диска.

Например, чтобы загрузить образ диска в Vitastor:

```
qemu-img convert -f qcow2 debian10.qcow2 -p -O raw 'vitastor:etcd_host=10.115.0.10\:2379/v3:image=testimg'
```

Если вы не хотите обращаться к образу по имени, вместо `:image=<IMAGE>` можно указать номер пула, номер инода и размер:
`:pool=<POOL>:inode=<INODE>:size=<SIZE>`.
