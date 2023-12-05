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

С отдельным потоком ввода-вывода:

```
qemu-system-x86_64 -enable-kvm -m 1024 \
    -object iothread,id=vitastor1 \
    -blockdev '{"node-name":"drive-virtio-disk0","driver":"vitastor","image":"debian9",
        "cache":{"direct":true,"no-flush":false},"auto-read-only":true,"discard":"unmap"}' \
    -device 'virtio-blk-pci,iothread=vitastor1,scsi=off,bus=pci.0,addr=0x5,drive=drive-virtio-disk0,
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

## vhost-user-blk

QEMU, начиная с 6.0, позволяет подключать диски через отдельный рабочий процесс.
Этот метод подключения называется `vhost-user-blk` и обычно имеет чуть меньшую
задержку (ниже на 20-30 микросекунд, чем при обычном методе).

Пример команд для использования vhost-user-blk с Vitastor:

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

Здесь критична опция memory-backend-memfd, vhost-user-blk без неё не работает.

## VDUSE

В Linux, начиная с версии ядра 5.15, доступен новый интерфейс для подключения виртуальных дисков
к системе - VDUSE (vDPA Device in Userspace), а в QEMU, начиная с версии 7.2, есть поддержка
экспорта блочных устройств QEMU по этому протоколу через qemu-storage-daemon.

VDUSE - на данный момент лучший интерфейс для подключения дисков Vitastor в виде блочных
устройств на уровне ядра, ибо:
- VDUSE не копирует данные и поэтому достигает значительно лучшей производительности, чем [NBD](nbd.ru.md)
- Также оно не имеет проблемы NBD-таймаута - устройство не умирает, если операция выполняется слишком долго
- Также оно не имеет проблемы подвисающих устройств - если процесс-обработчик умирает, его можно
  перезапустить (!) и блочное устройство продолжит работать
- По-видимому, у него нет предела числа подключаемых в систему устройств

Пример сравнения производительности:

|                          | Прямой fio  | NBD         | VDUSE       |
|--------------------------|-------------|-------------|-------------|
| линейная запись          | 3.85 GB/s   | 1.12 GB/s   | 3.85 GB/s   |
| 4k случайная запись Q128 | 240000 iops | 120000 iops | 178000 iops |
| 4k случайная запись Q1   | 9500 iops   | 7620 iops   | 7640 iops   |
| линейное чтение          | 4.3 GB/s    | 1.8 GB/s    | 2.85 GB/s   |
| 4k случайное чтение Q128 | 287000 iops | 140000 iops | 189000 iops |
| 4k случайное чтение Q1   | 9600 iops   | 7640 iops   | 7780 iops   |

Чтобы попробовать VDUSE, вам нужно ядро Linux как минимум версии 5.15, собранное с поддержкой
VDUSE (CONFIG_VDPA=m, CONFIG_VDPA_USER=m, CONFIG_VIRTIO_VDPA=m).

В ядрах в Debian Linux поддержка пока отключена по умолчанию, так что чтобы попробовать VDUSE
на Debian, поставьте ядро из Ubuntu [kernel-ppa/mainline](https://kernel.ubuntu.com/~kernel-ppa/mainline/),
из Proxmox или соберите модули для ядра Debian вручную:

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

Также вам понадобится консольная утилита `vdpa` из пакета `iproute2`.

Команды для подключения виртуального диска через VDUSE:

```
modprobe vduse
modprobe virtio-vdpa
qemu-storage-daemon --daemonize --blockdev '{"node-name":"test1","driver":"vitastor",\
  "etcd-host":"192.168.7.2:2379/v3","image":"testosd1","cache":{"direct":true,"no-flush":false},"discard":"unmap"}' \
  --export vduse-blk,id=test1,node-name=test1,name=test1,num-queues=16,queue-size=128,writable=true
vdpa dev add name test1 mgmtdev vduse
```

После этого в системе появится устройство `/dev/vda`, которое можно будет использовать как
обычный диск.

Для удаления устройства из системы:

```
vdpa dev del test1
kill <PID_процесса_qemu-storage-daemon>
```
