[Документация](../../README-ru.md#документация) → [Конфигурация](../config.ru.md) → Дисковые параметры OSD

-----

[Read in English](layout-osd.en.md)

# Дисковые параметры OSD

Данные параметры используются только OSD и, также как и общекластерные
дисковые параметры, задаются в момент инициализации дисков OSD и не могут быть
изменены после этого без потери данных.

- [data_device](#data_device)
- [meta_device](#meta_device)
- [journal_device](#journal_device)
- [journal_offset](#journal_offset)
- [journal_size](#journal_size)
- [meta_offset](#meta_offset)
- [data_offset](#data_offset)
- [data_size](#data_size)
- [meta_block_size](#meta_block_size)
- [journal_block_size](#journal_block_size)
- [disable_data_fsync](#disable_data_fsync)
- [disable_meta_fsync](#disable_meta_fsync)
- [disable_journal_fsync](#disable_journal_fsync)
- [disable_device_lock](#disable_device_lock)
- [disk_alignment](#disk_alignment)
- [data_csum_type](#data_csum_type)
- [csum_block_size](#csum_block_size)

## data_device

- Тип: строка

Путь к диску (блочному устройству) для хранения данных. Крайне рекомендуется
использовать стабильные пути: `/dev/disk/by-partuuid/xxx...` вместо простых
`/dev/sda` или `/dev/nvme0n1`, чтобы пути не могли спутаться после
перезагрузки сервера. Также вместо блочных устройств можно указывать файлы,
но это реализовано только для тестирования, а не для боевой среды.

## meta_device

- Тип: строка

Путь к диску метаданных. Метаданные должны располагаться на быстром
SSD-диске, иначе производительность пострадает. Если эта опция не указана,
для метаданных используется `data_device`.

## journal_device

- Тип: строка

Путь к диску журнала. Журнал должен располагаться на быстром SSD-диске,
иначе производительность пострадает. Если эта опция не указана,
для журнала используется `meta_device`, если же пуста и она, журнал
располагается на `data_device`. Нормально располагать журнал и метаданные
на одном устройстве, в этом случае достаточно указать только `meta_device`.

## journal_offset

- Тип: целое число
- Значение по умолчанию: 0

Смещение на устройстве в байтах, по которому располагается журнал.

## journal_size

- Тип: целое число

Размер журнала в байтах. По умолчанию для журнала используется всё доступное
место между journal_offset и data_offset, meta_offset или концом диска.
В SSD-кластерах большие журналы не нужны, достаточно 32 МБ. В гибридных
(SSD+HDD) кластерах осмысленно использовать больший размер журнал (например, 1 ГБ)
и включить [throttle_small_writes](osd.ru.md#throttle_small_writes).

## meta_offset

- Тип: целое число
- Значение по умолчанию: 0

Смещение на устройстве в байтах, по которому располагаются метаданные.
Эту опцию нужно задать, если метаданные у вас хранятся на том же
устройстве, что данные или журнал.

## data_offset

- Тип: целое число
- Значение по умолчанию: 0

Смещение на устройстве в байтах, по которому располагаются данные.
Эту опцию нужно задать, если данные у вас хранятся на том же
устройстве, что метаданные или журнал.

## data_size

- Тип: целое число

Размер области данных в байтах. По умолчанию под данные будет использована
вся доступная область устройства данных до конца устройства, но вы можете
использовать эту опцию, чтобы ограничить её меньшим размером. Заметьте, что
опции размера области метаданных нет - она вычисляется из размера области
данных автоматически.

## meta_block_size

- Тип: целое число
- Значение по умолчанию: 4096

Размер физического блока устройства метаданных. 4096 для большинства
современных SSD и HDD.

## journal_block_size

- Тип: целое число
- Значение по умолчанию: 4096

Размер физического блока устройства журнала. Должен быть кратен
`disk_alignment`. 4096 для большинства современных SSD и HDD.

## disable_data_fsync

- Тип: булево (да/нет)
- Значение по умолчанию: false

Не отправлять fsync-и устройству данных, т.е. не заставлять его сбрасывать кэш.
Безопасно, ТОЛЬКО если ваше устройство данных имеет кэш со сквозной
записью (write-through) или если кэш с отложенной записью (write-back) отключён.
Если вы отключаете кэш вручную через `hdparm` или запись в `/sys/.../scsi_disk/cache_type`,
то удостоверьтесь, что вы делаете это каждый раз перед запуском Vitastor OSD
(vitastor-disk делает это автоматически). Смотрите также опцию
[immediate_commit](layout-cluster.ru.md#immediate_commit) для информации о том,
как извлечь выгоду из отключённого кэша.

## disable_meta_fsync

- Тип: булево (да/нет)
- Значение по умолчанию: false

То же, что disable_data_fsync, но для устройства метаданных. Если устройство
метаданных не задано или если оно равно устройству данных, значение опции
игнорируется и вместо него используется значение опции disable_data_fsync.

## disable_journal_fsync

- Тип: булево (да/нет)
- Значение по умолчанию: false

То же, что disable_data_fsync, но для устройства журнала. Если устройство
журнала не задано или если оно равно устройству метаданных, значение опции
игнорируется и вместо него используется значение опции disable_meta_fsync.
Если одно и то же устройство используется и под данные, и под журнал, и под
метаданные - значение опции также игнорируется и вместо него используется
значение опции disable_data_fsync.

## disable_device_lock

- Тип: булево (да/нет)
- Значение по умолчанию: false

Не блокировать устройства данных, метаданных и журнала от открытия их
другими OSD с помощью flock(). Так делать не рекомендуется, но теоретически
вы можете это использовать, чтобы запускать несколько OSD на одном
устройстве с разными смещениями и без использования разделов.

## disk_alignment

- Тип: целое число
- Значение по умолчанию: 4096

Требуемое выравнивание записи на физические диски. Почти все современные
SSD и HDD диски используют 4 КБ физические секторы, даже если показывают
логический размер сектора 512 байт, поэтому 4 КБ - хорошее значение по
умолчанию.

Однако стоит понимать, что физический размер сектора тоже влияет на
избыточную запись (WA), потому что ничего меньше блока (сектора) на блочное
устройство записать невозможно. Таким образом, когда Vitastor-у нужно
записать на диск всего лишь одну 32-байтную запись метаданных, фактически
приходится перезаписывать 4 КБ сектор целиком.

Поэтому, на самом деле, может быть выгодно найти SSD, хорошо работающие с
меньшими, 512-байтными, блоками и использовать 512-байтные disk_alignment,
journal_block_size и meta_block_size. Однако на данный момент такие SSD
не известны...

Клиентам не обязательно знать про disk_alignment, так что помещать значение
этого параметра в etcd в /vitastor/config/global не нужно.

## data_csum_type

- Тип: строка
- Значение по умолчанию: none

Тип используемых OSD контрольных сумм данных. Может быть "crc32c" или "none".
Установите в "crc32c", чтобы включить расчёт и проверку контрольных сумм данных.

Следует понимать, что контрольные суммы в зависимости от размера блока их
расчёта либо увеличивают потребление памяти, либо снижают производительность.
Подробнее смотрите в описании параметра [csum_block_size](#csum_block_size).

## csum_block_size

- Тип: целое число
- Значение по умолчанию: 4096

Размер блока расчёта контрольных сумм.

Должен быть равен или кратен [bitmap_granularity](layout-cluster.ru.md#bitmap_granularity)
(который обычно равен 4 КБ).

Контрольные суммы увеличивают размер метаданных на 4 байта на каждые
csum_block_size данных.

Контрольные суммы - это всегда компромисс:
1. Вы либо жертвуете потреблением +1 ГБ памяти на 1 ТБ дискового пространства
2. Либо вы повышаете csum_block_size до, скажем, 32k и жертвуете 50%
   скорости случайной записи из-за цикла чтения-изменения-записи для расчёта
   новых контрольных сумм
3. Либо вы отключаете [inmemory_metadata](osd.ru.md#inmemory_metadata) и
   жертвуете 50% скорости случайного чтения из-за чтения контрольных сумм
   с диска

Таким образом, рекомендуются следующие варианты настроек:
1. All-flash, 1 ГБ памяти на 1 ТБ данных: по умолчанию (csum_block_size=4k)
2. All-flash, меньше памяти: csum_block_size=4k + inmemory_metadata=false
3. Гибридные HDD+SSD: csum_block_size=4k + inmemory_metadata=false
4. Только HDD, быстрее случайное чтение: csum_block_size=32k
5. Только HDD, быстрее случайная запись: csum_block_size=4k +
   inmemory_metadata=false + meta_io=cached

Смотрите также [meta_io](osd.ru.md#meta_io).
