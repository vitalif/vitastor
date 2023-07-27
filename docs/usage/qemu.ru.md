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

### Экспорт снимков

Начиная с 0.8.4 вы можете экспортировать отдельные слои (изменения в снимках) с помощью `qemu-img`.

Допустим, что у вас есть образ `testimg` и его снимок `testimg@0`, созданный с помощью `vitastor-cli snap-create testimg@0`.

Тогда вы можете выгрузить снимок `testimg@0` и данные, изменённые в `testimg` после создания снимка, отдельно,
с помощью следующих команд (ключевые моменты - использование `skip-parents=1` и опции `-B backing_file.qcow2`):

```
qemu-img convert -f raw 'vitastor:etcd_host=192.168.7.2\:2379/v3:image=testimg@0' \
    -O qcow2 testimg_0.qcow2

qemu-img convert -f raw 'vitastor:etcd_host=192.168.7.2\:2379/v3:image=testimg:skip-parents=1' \
    -O qcow2 -o 'cluster_size=4k' -B testimg_0.qcow2 testimg.qcow2
```

На самом деле, с `cluster_size=4k` вместо `-B testimg_0.qcow2` можно использовать любой qcow2-файл,
даже пустой.

Опция QCOW2 `cluster_size=4k` нужна, если вы хотите, чтобы `testimg.qcow2` содержал **в точности**
данные, перезаписанные в дочернем слое. С размером кластера QCOW2 по умолчанию, составляющим 64 КБ,
вы получите немного "лишних" данных из родительских слоёв - перезапись 4 КБ будет приводить к тому,
что в `testimg.qcow2` будет появляться 64 КБ данных. Причём "лишние" данные qemu-img будет брать
как раз из файла, указанного в опции `-B`, так что если там указан пустой образ, кластер обязан быть 4 КБ.

После данной процедуры вы получите два QCOW2-образа, связанных в цепочку. Чтобы "отцепить" образ
`testimg.qcow2` от базового, выполните:

```
qemu-img rebase -u -b '' testimg.qcow2
```

Это можно использовать для резервного копирования. Только помните, что экспортировать образ, в который
в то же время идёт запись, небезопасно - результат чтения не будет целостным. Так что если вы работаете
с активными виртуальными машинами, экспортируйте только их снимки, но не сам образ.

## VDUSE

В Linux, начиная с версии ядра 5.15, доступен новый интерфейс для подключения виртуальных дисков
к системе - VDUSE (vDPA Device in Userspace), а в QEMU, начиная с версии 7.2, есть поддержка
экспорта блочных устройств QEMU по этому протоколу через qemu-storage-daemon.

VDUSE страдает общей проблемой FUSE-подобных интерфейсов в Linux: если пользовательский процесс
подвиснет, например, если будет потеряна связь с кластером Vitastor - читающие/пишущие в кластер
процессы могут "залипнуть" в состоянии D (непрерываемый сон) и их будет невозможно убить даже
через kill -9. В этом случае удалить из системы устройство можно только перезагрузившись.

С другой стороны, VDUSE быстрее по сравнению с [NBD](nbd.ru.md), поэтому его может
быть предпочтительно использовать там, где производительность важнее. Порядок показателей:
прямое тестирование через fio - 115000 iops, NBD - 60000 iops, VDUSE - 90000 iops.

Чтобы использовать VDUSE, вам нужно ядро Linux версии хотя бы 5.15, собранное с поддержкой
VDUSE (CONFIG_VIRTIO_VDPA=m и CONFIG_VDPA_USER=m). В ядрах в Debian Linux поддержка пока
отключена - если хотите попробовать эту функцию на Debian, поставьте ядро из Ubuntu
[kernel-ppa/mainline](https://kernel.ubuntu.com/~kernel-ppa/mainline/) или из Proxmox.

Команды для подключения виртуального диска через VDUSE:

```
modprobe vduse
modprobe virtio-vdpa
qemu-storage-daemon --daemonize --blockdev '{"node-name":"test1","driver":"vitastor",\
  "etcd-host":"192.168.7.2:2379/v3","image":"testosd1","cache":{"direct":true,"no-flush":false},"discard":"unmap"}' \
  --export vduse-blk,id=test1,node-name=test1,name=test1,num-queues=16,queue-size=128,writable=true
vdpa dev add name test1 mgmtdev vduse
```

После этого в системе появится устройство /dev/vda, которое можно будет использовать как
обычный диск.

Для удаления устройства из системы:

```
vdpa dev del test1
kill <PID_процесса_qemu-storage-daemon>
```
