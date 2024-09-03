[Документация](../../README-ru.md#документация) → Установка → OpenNebula

-----

[Read in English](opennebula.en.md)

## Автоматическая установка

Плагин OpenNebula Vitastor распространяется как Debian и RPM пакет `opennebula-vitastor`, начиная с версии Vitastor 1.9.0. Так что:

- Запустите `apt-get install opennebula-vitastor` или `yum install vitastor` после установки OpenNebula на всех серверах
- Проверьте, что он выводит "OK, Vitastor OpenNebula patches successfully applied" или "OK, Vitastor OpenNebula patches are already applied" в процессе установки
- Если сообщение не выведено, пройдите по шагам инструкцию [Ручная установка](#ручная-установка) и примените правки файлов конфигурации вручную
- [Заблокируйте доступ виртуальных машин в Vitastor](#блокировка-доступа-вм-в-vitastor)

## Ручная установка

Сначала установите саму OpenNebula. После этого, на каждом сервере:

- Скопируйте директорию [opennebula/remotes](../../opennebula/remotes) в `/var/lib/one`: `cp -r opennebula/remotes /var/lib/one/`
- Скопируйте директорию [opennebula/sudoers.d](../../opennebula/sudoers.d) в `/etc`: `cp -r opennebula/sudoers.d /etc/`
- Примените патч [downloader-vitastor.sh.diff](../../opennebula/remotes/datastore/vitastor/downloader-vitastor.sh.diff) к `/var/lib/one/remotes/datastore/downloader.sh`:
  `patch /var/lib/one/remotes/datastore/downloader.sh < opennebula/remotes/datastore/vitastor/downloader-vitastor.sh.diff` - либо прочитайте патч и примените изменение вручную
- Добавьте `kvm-vitastor` в список `LIVE_DISK_SNAPSHOTS` в файле `/etc/one/vmm_exec/vmm_execrc`
- Если вы используете Debian или Ubuntu (и AppArmor), добавьте пути к файлу(ам) конфигурации Vitastor в файл `/etc/apparmor.d/local/abstractions/libvirt-qemu`: например,
  `echo '  "/etc/vitastor/vitastor.conf" r,' >> /etc/apparmor.d/local/abstractions/libvirt-qemu`
- Примените изменения `/etc/one/oned.conf`

### Изменения oned.conf

1. Добавьте переопределение скрипта deploy в VM_MAD kvm, добавив `-l deploy.vitastor` в `ARGUMENTS`:

```diff
 VM_MAD = [
     NAME           = "kvm",
     SUNSTONE_NAME  = "KVM",
     EXECUTABLE     = "one_vmm_exec",
-    ARGUMENTS      = "-t 15 -r 0 kvm -p",
+    ARGUMENTS      = "-t 15 -r 0 kvm -p -l deploy=deploy.vitastor",
     DEFAULT        = "vmm_exec/vmm_exec_kvm.conf",
     TYPE           = "kvm",
     KEEP_SNAPSHOTS = "yes",
     LIVE_RESIZE    = "yes",
     SUPPORT_SHAREABLE    = "yes",
     IMPORTED_VMS_ACTIONS = "terminate, terminate-hard, hold, release, suspend,
         resume, delete, reboot, reboot-hard, resched, unresched, disk-attach,
         disk-detach, nic-attach, nic-detach, snapshot-create, snapshot-delete,
         resize, updateconf, update"
 ]
```

Опционально: если вы хотите также сохранять снимки памяти ВМ в Vitastor, добавьте
`-l deploy=deploy.vitastor,save=save.vitastor,restore=restore.vitastor`
вместо просто `-l deploy=deploy.vitastor`.

2. Добавьте `vitastor` в значения TM_MAD.ARGUMENTS и DATASTORE_MAD.ARGUMENTS:

```diff
 TM_MAD = [
     EXECUTABLE = "one_tm",
-    ARGUMENTS = "-t 15 -d dummy,lvm,shared,fs_lvm,fs_lvm_ssh,qcow2,ssh,ceph,dev,vcenter,iscsi_libvirt"
+    ARGUMENTS = "-t 15 -d dummy,lvm,shared,fs_lvm,fs_lvm_ssh,qcow2,ssh,ceph,vitastor,dev,vcenter,iscsi_libvirt"
 ]

 DATASTORE_MAD = [
     EXECUTABLE = "one_datastore",
-    ARGUMENTS  = "-t 15 -d dummy,fs,lvm,ceph,dev,iscsi_libvirt,vcenter,restic,rsync -s shared,ssh,ceph,fs_lvm,fs_lvm_ssh,qcow2,vcenter"
+    ARGUMENTS  = "-t 15 -d dummy,fs,lvm,ceph,vitastor,dev,iscsi_libvirt,vcenter,restic,rsync -s shared,ssh,ceph,vitastor,fs_lvm,fs_lvm_ssh,qcow2,vcenter"
 ]
```

3. Добавьте строчки с INHERIT_DATASTORE_ATTR для двух атрибутов Vitastor-хранилищ:

```
INHERIT_DATASTORE_ATTR = "VITASTOR_CONF"
INHERIT_DATASTORE_ATTR = "IMAGE_PREFIX"
```

4. Добавьте TM_MAD_CONF и DS_MAD_CONF для Vitastor:

```
TM_MAD_CONF = [
    NAME = "vitastor", LN_TARGET = "NONE", CLONE_TARGET = "SELF", SHARED = "YES",
    DS_MIGRATE = "NO", DRIVER = "raw", ALLOW_ORPHANS="format",
    TM_MAD_SYSTEM = "ssh,shared", LN_TARGET_SSH = "SYSTEM", CLONE_TARGET_SSH = "SYSTEM",
    DISK_TYPE_SSH = "FILE", LN_TARGET_SHARED = "NONE",
    CLONE_TARGET_SHARED = "SELF", DISK_TYPE_SHARED = "FILE"
]

DS_MAD_CONF = [
    NAME = "vitastor",
    REQUIRED_ATTRS = "DISK_TYPE,BRIDGE_LIST",
    PERSISTENT_ONLY = "NO",
    MARKETPLACE_ACTIONS = "export"
]
```

## Создайте хранилища

Примеры настроек хранилищ образов (image) и дисков ВМ (system):
[opennebula/vitastor-imageds.conf](../../opennebula/vitastor-imageds.conf) и
[opennebula/vitastor-systemds.conf](../../opennebula/vitastor-systemds.conf).

Скопируйте настройки и поменяйте следующие параметры так, как вам необходимо:

- POOL_NAME - имя пула Vitastor для сохранения образов дисков.
- IMAGE_PREFIX - строка, добавляемая в начало имён образов дисков.
- BRIDGE_LIST - список серверов с доступом к кластеру Vitastor, используемых для операций с хранилищем образов (image, не system).
- VITASTOR_CONF - путь к конфигурации Vitastor. Имейте в виду, что этот путь также надо добавить в `/etc/apparmor.d/local/abstractions/libvirt-qemu`, если вы используете AppArmor.
- STAGING_DIR - путь к временному каталогу, используемому при импорте внешних образов. Должен иметь достаточно свободного места, чтобы вмещать скачанные образы.

После этого создайте хранилища с помощью команд `onedatastore create vitastor-imageds.conf` и `onedatastore create vitastor-systemds.conf` (либо через UI).

## Блокировка доступа ВМ в Vitastor

Vitastor пока не поддерживает никакую аутентификацию, так что вы ДОЛЖНЫ заблокировать доступ гостевых ВМ
в кластер Vitastor на сетевом уровне.

Если вы используете VLAN-сети для ВМ - удостоверьтесь, что ВМ и гипервизор/сеть хранения помещены в разные
изолированные друг от друга VLAN-ы.

Если вы используете что-то более примитивное, например, мосты (bridge), вам, скорее всего, придётся вручную
настроить iptables / межсетевой экран, чтобы разрешить доступ к Vitastor только с IP гипервизоров.

Также в этом случае нужно будет переключить обычные мосты на "Bridged & Security Groups" и включить фильтр
спуфинга IP в OpenNebula. Правда, реализация этого фильтра пока не полная, и она не блокирует доступ к
локальным интерфейсам гипервизора. То есть, включённый фильтр спуфинга IP запрещает ВМ отправлять трафик
с чужими IP к другим ВМ или во внешний мир, но не запрещает отправлять его напрямую гипервизору. Чтобы
исправить это, тоже нужны дополнительные правила iptables.

Таким образом, более-менее полная блокировка при использовании простой сети на сетевых мостах может
выглядеть так (здесь `10.0.3.0/24` - подсеть ВМ, `10.0.2.0/24` - подсеть гипервизора):

```
# Разрешаем входящий трафик с физического устройства
iptables -A INPUT -m physdev --physdev-in eth0 -j ACCEPT
# Запрещаем трафик со всех ВМ, но с IP не из подсети ВМ
iptables -A INPUT ! -s 10.0.3.0/24 -i onebr0 -j DROP
# Запрещаем трафик от ВМ к сети гипервизора
iptables -I FORWARD 1 -s 10.0.3.0/24 -d 10.0.2.0/24 -j DROP
```

## Тестирование

Плагин OpenNebula по большей части состоит из bash-скриптов, и чтобы было понятнее, что они
вообще делают - ниже приведены описания процедур, которыми можно протестировать каждый из них.

| Скрипт                  | Описание                                      | Как протестировать                                                                   |
| ----------------------- | --------------------------------------------- | ------------------------------------------------------------------------------------ |
| vmm/kvm/deploy.vitastor | Запустить виртуальную машину                  | Создайте и запустите виртуальную машину с дисками Vitastor: постоянным / непостоянным / волатильным (временным). |
| vmm/kvm/save.vitastor   | Сохранить снимок памяти ВМ                    | Остановите виртуальную машину командой "Остановить".                                 |
| vmm/kvm/restore.vitastor| Восстановить снимок памяти ВМ                 | Запустите ВМ после остановки обратно.                                                |
| datastore/clone         | Скопировать образ как "постоянный"            | Создайте шаблон ВМ и создайте из него постоянную ВМ.                                 |
| datastore/cp            | Импортировать внешний образ                   | Импортируйте шаблон ВМ с образами дисков из Магазина OpenNebula.                     |
| datastore/export        | Экспортировать образ как URL                  | Вероятно: экспортируйте шаблон ВМ с образами в Магазин.                              |
| datastore/mkfs          | Создать образ с файловой системой             | Хранилище → Образы → Создать → Тип: базовый блок данных, Расположение: пустой образ диска, Файловая система: любая непустая. |
| datastore/monitor       | Вывод статистики места в хранилище образов    | Проверьте статистику свободного/занятого места в списке хранилищ образов.            |
| datastore/rm            | Удалить "постоянный" образ                    | Хранилище → Образы → Выберите образ → Удалить.                                       |
| datastore/snap_delete   | Удалить снимок "постоянного" образа           | Хранилище → Образы → Выберите образ → Выберите снимок → Удалить; <br> Чтобы создать образ со снимком: подключите постоянный образ к ВМ, создайте снимок, отключите образ. |
| datastore/snap_flatten  | Откатить образ к снимку, удалив другие снимки | Хранилище → Образы → Выберите образ → Выберите снимок → "Выровнять" (flatten).       |
| datastore/snap_revert   | Откатить образ к снимку                       | Хранилище → Образы → Выберите образ → Выберите снимок → Откатить.                    |
| datastore/stat          | Показать виртуальный размер образа в МБ       | Неизвестно. По-видимому, в плагинах Vitastor и Ceph не используется.                 |
| tm/clone                | Клонировать "непостоянный" образ в диск ВМ    | Подключите "непостоянный" образ к ВМ.                                                |
| tm/context              | Создать диск контекстуализации ВМ             | Создайте ВМ с контекстуализацией, как обычно. Но тестировать особенно нечего: в плагинах Vitastor и Ceph образ контекста хранится в локальной ФС гипервизора. |
| tm/cpds                 | Копировать диск ВМ/его снимок в новый образ   | Выберите ВМ → Выберите диск → Опционально выберите снимок → "Сохранить как".         |
| tm/delete               | Удалить диск-клон или волатильный диск ВМ     | Отключите волатильный или не-постоянный диск от ВМ.                                  |
| tm/failmigrate          | Обработать неудачную миграцию                 | Тестировать нечего. Скрипт пуст в плагинах Vitastor и Ceph. В других плагинах скрипт должен откатывать действия tm/premigrate. |
| tm/ln                   | Подключить "постоянный" образ к ВМ            | Тестировать нечего. Скрипт пуст в плагинах Vitastor и Ceph.                          |
| tm/mkimage              | Создать волатильный диск, без или с ФС        | Подключите волатильный диск к ВМ, с или без файловой системы.                        |
| tm/mkswap               | Создать волатильный диск подкачки             | Подключите волатильный диск к ВМ, форматированный как диск подкачки (swap).          |
| tm/monitor              | Вывод статистики места в хранилище дисков ВМ  | Проверьте статистику свободного/занятого места в списке хранилищ дисков ВМ.          |
| tm/mv                   | Мигрировать диск ВМ между хостами             | Мигрируйте ВМ между серверами. Правда, с точки зрения хранилища в плагинах Vitastor и Ceph этот скрипт ничего не делает. |
| tm/mvds                 | Отключить "постоянный" образ от ВМ            | Тестировать нечего. Скрипт пуст в плагинах Vitastor и Ceph. В целом же скрипт обратный к tm/ln и в других хранилищах он может, например, копировать образ ВМ с диска гипервизора обратно в хранилище. |
| tm/postbackup           | Выполняется после бэкапа                      | По-видимому, скрипт просто удаляет временные файлы после резервного копирования. Так что можно провести его и проверить, что на серверах не осталось временных файлов. |
| tm/postbackup_live      | Выполняется после бэкапа запущенной ВМ        | То же, что tm/postbackup, но для запущенной ВМ.                                      |
| tm/postmigrate          | Выполняется после миграции ВМ                 | Тестировать нечего. Однако, OpenNebula запускает скрипт только для системного хранилища, поэтому он вызывает аналогичные скрипты для хранилищ других дисков той же ВМ. Помимо этого в плагинах Vitastor и Ceph скрипт ничего не делает. |
| tm/prebackup            | Выполнить резервное копирование дисков ВМ     | Создайте хранилище резервных копий типа "rsync" → Забэкапьте в него ВМ.              |
| tm/prebackup_live       | То же самое для запущенной ВМ                 | То же, что tm/prebackup, но запускает fsfreeze/thaw (остановку доступа к дискам). Так что смысл теста - проведите резервное копирование и проверьте, что данные скопировались консистентно. |
| tm/premigrate           | Выполняется перед миграцией ВМ                | Тестировать нечего. Аналогично tm/postmigrate запускается только для системного хранилища. |
| tm/resize               | Изменить размер диска ВМ                      | Выберите ВМ → Выберите непостоянный диск → Измените его размер.                      |
| tm/restore              | Восстановить диски ВМ из бэкапа               | Создайте хранилище резервных копий → Забэкапьте в него ВМ → Восстановите её обратно. |
| tm/snap_create          | Создать снимок диска ВМ                       | Выберите ВМ → Выберите диск → Создайте снимок.                                       |
| tm/snap_create_live     | Создать снимок диска запущенной ВМ            | Выберите запущенную ВМ → Выберите диск → Создайте снимок.                            |
| tm/snap_delete          | Удалить снимок диска ВМ                       | Выберите ВМ → Выберите диск → Выберите снимок → Удалить.                             |
| tm/snap_revert          | Откатить диск ВМ к снимку                     | Выберите ВМ → Выберите диск → Выберите снимок → Откатить.                            |
