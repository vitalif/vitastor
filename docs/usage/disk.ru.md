[Документация](../../README-ru.md#документация) → Использование → Управление дисками

-----

[Read in English](disk.en.md)

# Инструмент управления дисками

vitastor-disk - инструмент командной строки для управления дисками Vitastor OSD.

Поддерживаются следующие команды:

- [prepare](#prepare)
- [upgrade-simple](#upgrade-simple)
- [resize](#resize)
- [start/stop/restart/enable/disable](#start/stop/restart/enable/disable)
- [read-sb](#read-sb)
- [write-sb](#write-sb)
- [udev](#udev)
- [exec-osd](#exec-osd)
- [pre-exec](#pre-exec)
- Для отладки:
  - [dump-journal](#dump-journal)
  - [write-journal](#write-journal)
  - [dump-meta](#dump-meta)
  - [write-meta](#write-meta)
- [simple-offsets](#simple-offsets)

## prepare

`vitastor-disk prepare [OPTIONS] [devices...]`

Подготовить диск(и) для OSD Vitastor.

У команды есть 2 режима. В первом режиме вы указываете список устройств `<devices>`,
которые должны быть целыми дисками (не разделами). На них автоматически создаются
разделы и инициализируются OSD.

Во втором режиме вместо списка устройств вы указываете пути к отдельным устройствам
`--data_device`, `--journal_device` и/или `--meta_device`, которые должны быть
уже существующими GPT-разделами. В этом случае инициализируется ровно один OSD.

Команде требуются утилиты `vitastor-cli`, `wipefs`, `sfdisk` и `partprobe` (из состава parted).

Опции для автоматического режима:

```
--osd_per_disk <N>
  Создавать по несколько (<N>) OSD на каждом диске (по умолчанию 1)
--hybrid
  Инициализировать гибридные (HDD+SSD) OSD на указанных дисках. SSD будут
  использованы для журналов и метаданных, а HDD - для данных. Разделы для журналов
  и метаданных будут созданы автоматически. Является ли диск SSD или HDD, определяется
  по флагу `/sys/block/.../queue/rotational`. В гибридном режиме по умолчанию
  используется размер объекта 1 МБ вместо 128 КБ, размер журнала 1 ГБ вместо 32 МБ
  и включённый throttle_small_writes.
--disable_data_fsync 1
  Отключать кэш и fsync-и для устройств данных (1/yes/true = да, по умолчанию да)
--disable_meta_fsync 1
  Отключать кэш и fsync-и для журналов и метаданных (по умолчанию да)
--meta_reserve 2x,1G
  В гибридном режиме для метаданных выделяется больше места, чем нужно на самом
  деле, чтобы оставить запас под будущее расширение. По умолчанию выделяется
  в 2 раза больше места, и не менее 1 ГБ. Чтобы изменить это поведение,
  воспользуйтесь данной опцией.
--max_other 10%
  Использовать диски под данные OSD, даже если на них уже есть не-Vitastor-овые
  разделы, но только в случае, если они занимают не более данного процента диска.
```

Опции для режима одного OSD:

```
--data_device <DEV>        Использовать раздел <DEV> для данных
--meta_device <DEV>        Использовать раздел <DEV> для метаданных (опционально)
--journal_device <DEV>     Использовать раздел <DEV> для журнала (опционально)
--disable_data_fsync 0     Отключить кэш и fsync устройства данных (по умолчанию нет)
--disable_meta_fsync 0     Отключить кэш и fsync метаданных (по умолчанию нет)
--disable_journal_fsync 0  Отключить кэш и fsync журнала (по умолчанию нет)
--force                    Пропустить проверки разделов (на пустоту и т.п.)
```

Опции для обоих режимов:

```
--journal_size 1G/32M      Задать размер журнала (области или раздела журнала)
--object_size 1M/128k      Задать размер объекта
--bitmap_granularity 4k    Задать гранулярность битовых карт
--data_device_block 4k     Задать размер блока устройства данных
--meta_device_block 4k     Задать размер блока метаданных
--journal_device_block 4k  Задать размер блока журнала
```

Настройка [immediate_commit](../config/layout-cluster.ru.md#immediate_commit)
автоматически выводится из опций отключения кэша - она устанавливается в "all", если кэш
отключён на всех устройствах, и в "small", если он отключён только на устройстве журнала.

Когда fsync данных/метаданных/журнала отключён, скрипты запуска OSD автоматически
проверяют состояние кэша диска и стараются его отключить для SATA/SAS дисков. Если
это не удаётся, в системный журнал выводится предупреждение.

Вы можете передать данной команде и некоторые другие опции OSD в качестве аргументов
и они тоже будут сохранены в суперблок: max_write_iodepth, max_write_iodepth, min_flusher_count,
max_flusher_count, inmemory_metadata, inmemory_journal, journal_sector_buffer_count,
journal_no_same_sector_overwrites, throttle_small_writes, throttle_target_iops,
throttle_target_mbs, throttle_target_parallelism, throttle_threshold_us.
Читайте об этих параметрах подробнее в разделе [Изменяемые параметры OSD](../config/osd.ru.md).

## upgrade-simple

`vitastor-disk upgrade-simple <UNIT_FILE|OSD_NUMBER>`

Обновить OSD, созданный старыми (0.7.1 и старее) скриптами `make-osd.sh` и `make-osd-hybrid.js`.

Добавляет суперблок на разделы OSD, отключает старый сервис `vitastor-osdN` и заменяет его на `vitastor-osd@N`.

Можно вызывать, указывая либо номер OSD, либо путь к файлу сервиса `UNIT_FILE`, но он обязан
иметь вид `/etc/systemd/system/vitastor-osd<OSD_NUMBER>.service`.

Имейте в виду, что процедура обновления не атомарна и при прерывании может уничтожить данные OSD,
так что обновляйте ваши OSD по очереди.

Команде требуется утилита `sfdisk`.

## resize

`vitastor-disk resize <ALL_OSD_PARAMETERS> <NEW_LAYOUT> [--iodepth 32]`

Изменить размер области данных и/или переместить журнал и метаданные.

В `ALL_OSD_PARAMETERS` нужно указать все относящиеся к диску параметры OSD
из суперблока OSD или из файла сервиса systemd (в старых версиях).

В `NEW_LAYOUT` нужно указать новые параметры расположения данных:

```
--new_data_offset РАЗМЕР     сдвинуть начало области данных на РАЗМЕР байт
--new_data_len РАЗМЕР        изменить размер области данных до РАЗМЕР байт
--new_meta_device ПУТЬ       использовать ПУТЬ как новое устройство метаданных
--new_meta_offset РАЗМЕР     разместить новые метаданные по смещению РАЗМЕР байт
--new_meta_len РАЗМЕР        сделать новые метаданные размером РАЗМЕР байт
--new_journal_device ПУТЬ    использовать ПУТЬ как новое устройство журнала
--new_journal_offset РАЗМЕР  разместить новый журнал по смещению РАЗМЕР байт
--new_journal_len РАЗМЕР     сделать новый журнал размером РАЗМЕР байт
```

РАЗМЕР может быть указан с суффиксами k/m/g/t. Если любой из новых параметров
расположения не указан, он принимается равным старому значению.

## start/stop/restart/enable/disable

`vitastor-disk start|stop|restart|enable|disable [--now] <device> [device2 device3 ...]`

Команды управления OSD по путям дисков через systemd.

Команды транслируются `systemctl` с сервисами `vitastor-osd@<num>` в виде аргументов.

Когда к командам включения/выключения добавляется параметр `--now`, OSD также сразу
запускаются/останавливаются.

## read-sb

`vitastor-disk read-sb <device>`

Прочитать суперблок OSD с диска `<device>` и вывести его в формате JSON.

## write-sb

`vitastor-disk write-sb <device>`

Прочитать JSON со стандартного ввода и записать его в суперблок OSD на диск `<device>`.

## udev

`vitastor-disk udev <device>`

Прочитать суперблок OSD с диска `<device>` и вывести переменные для udev.

## exec-osd

`vitastor-disk exec-osd <device>`

Прочитать суперблок OSD с диска `<device>` и запустить исполняемый файл OSD с параметрами оттуда.

Команда предназначена для использования из скриптов запуска (например, из сервисов systemd).

## pre-exec

`vitastor-disk pre-exec <device>`

Прочитать суперблок OSD с диска `<device>` и провести проверки OSD перед запуском.

На данный момент только отключает кэш диска или проверяет, что он отключён, если в параметрах
OSD отключены fsync-и.

Команда предназначена для использования из скриптов запуска (например, из сервисов systemd).

## dump-journal

`vitastor-disk dump-journal [OPTIONS] <journal_file> <journal_block_size> <offset> <size>`

Вывести журнал в человекочитаемом или в JSON (с опцией `--json`) виде.

Опции:

```
--all             Просканировать всю область журнала и вывести даже старые записи
--json            Вывести журнал в формате JSON
--format entries  (По умолчанию) Вывести только актуальные записи журнала без данных
--format data     Вывести только актуальные записи журнала с данными
--format blocks   Вывести массив блоков журнала, а в каждом массив актуальных записей без данных
```

## write-journal

`vitastor-disk write-journal <journal_file> <journal_block_size> <bitmap_size> <offset> <size>`

Записать журнал из JSON со стандартного ввода в формате, аналогичном `dump-journal --json --format data`.

## dump-meta

`vitastor-disk dump-meta <meta_file> <meta_block_size> <offset> <size>`

Вывести метаданные в формате JSON.

## write-meta

`vitastor-disk write-meta <meta_file> <offset> <size>`

Записать метаданные из JSON со стандартного ввода в формате, аналогичном `dump-meta`.

## simple-offsets

`vitastor-disk simple-offsets <device>`

Рассчитать смещения для старого ("простого и тупого") создания OSD на диске (без суперблока).

Опции (см. также [Дисковые параметры уровня кластера](../config/layout-cluster.ru.md)):

```
--object_size 128k       Размер блока хранилища
--bitmap_granularity 4k  Гранулярность битовых карт
--journal_size 32M       Размер журнала
--device_block_size 4k   Размер блока устройства
--journal_offset 0       Смещение журнала
--device_size 0          Размер устройства
--format text            Формат результата: json, options, env или text
```
