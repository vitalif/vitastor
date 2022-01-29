[Documentation](../../README.md#documentation) → Usage → Vitastor CLI

-----

[Читать на русском](cli.ru.md)

# Vitastor CLI

vitastor-cli is a command-line tool for administrative tasks like image management.

It supports the following commands:

- [status](#status)
- [df](#df)
- [ls](#ls)
- [create](#create)
- [modify](#modify)
- [rm](#rm)
- [flatten](#flatten)
- [rm-data](#rm-data)
- [merge-data](#merge-data)
- [alloc-osd](#alloc-osd)
- [simple-offsets](#simple-offsets)

Global options:

```
--etcd_address ADDR  Etcd connection address
--iodepth N          Send N operations in parallel to each OSD when possible (default 32)
--parallel_osds M    Work with M osds in parallel when possible (default 4)
--progress 1|0       Report progress (default 1)
--cas 1|0            Use CAS writes for flatten, merge, rm (default is decide automatically)
--no-color           Disable colored output
--json               JSON output
```

## status

`vitastor-cli status`

Показать состояние кластера.

Пример вывода:

```
  cluster:
    etcd: 1 / 1 up, 1.8 M database size
    mon:  1 up, master stump
    osd:  8 / 12 up

  data:
    raw:   498.5 G used, 301.2 G / 799.7 G available, 399.8 G down
    state: 156.6 G clean, 97.6 G misplaced
    pools: 2 / 3 active
    pgs:   30 active
           34 active+has_misplaced
           32 offline

  io:
    client:    0 B/s rd, 0 op/s rd, 0 B/s wr, 0 op/s wr
    rebalance: 989.8 M/s, 7.9 K op/s
```

## df

`vitastor-cli df`

Показать список пулов и занятое место.

Пример вывода:

```
NAME      SCHEME  PGS  TOTAL    USED    AVAILABLE  USED%   EFFICIENCY
testpool  2/1     32   100 G    34.2 G  60.7 G     39.23%  100%
size1     1/1     32   199.9 G  10 G    121.5 G    39.23%  100%
kaveri    2/1     32   0 B      10 G    0 B        100%    0%
```

В примере у пула "kaveri" эффективность равна нулю, так как все OSD выключены.

## ls

`vitastor-cli ls [-l] [-p POOL] [--sort FIELD] [-r] [-n N] [<glob> ...]`

Показать список образов, если переданы шаблоны `<glob>`, то только с именами,
соответствующими этим шаблонам (стандартные ФС-шаблоны с * и ?).

Опции:

```
-p|--pool POOL  Фильтровать образы по пулу (ID или имени)
-l|--long       Также выводить статистику занятого места и ввода-вывода
--del           Также выводить статистику операций удаления
--sort FIELD    Сортировать по заданному полю (name, size, used_size, <read|write|delete>_<iops|bps|lat|queue>)
-r|--reverse    Сортировать в обратном порядке
-n|--count N    Показывать только первые N записей
```

Пример вывода:

```
NAME                 POOL      SIZE  USED    READ   IOPS  QUEUE  LAT   WRITE  IOPS  QUEUE  LAT   FLAGS  PARENT
debian9              testpool  20 G  12.3 G  0 B/s  0     0      0 us  0 B/s  0     0      0 us     RO
pve/vm-100-disk-0    testpool  20 G  0 B     0 B/s  0     0      0 us  0 B/s  0     0      0 us      -  debian9
pve/base-101-disk-0  testpool  20 G  0 B     0 B/s  0     0      0 us  0 B/s  0     0      0 us     RO  debian9
pve/vm-102-disk-0    testpool  32 G  36.4 M  0 B/s  0     0      0 us  0 B/s  0     0      0 us      -  pve/base-101-disk-0
debian9-test         testpool  20 G  36.6 M  0 B/s  0     0      0 us  0 B/s  0     0      0 us      -  debian9
bench                testpool  10 G  10 G    0 B/s  0     0      0 us  0 B/s  0     0      0 us      -
bench-kaveri         kaveri    10 G  10 G    0 B/s  0     0      0 us  0 B/s  0     0      0 us      -
```

## create

`vitastor-cli create -s|--size <size> [-p|--pool <id|name>] [--parent <parent_name>[@<snapshot>]] <name>`

Создать образ. Для размера `<size>` можно использовать суффиксы K/M/G/T (килобайт-мегабайт-гигабайт-терабайт).
Если указана опция `--parent`, создаётся клон образа. Родитель `<parent_name>[@<snapshot>]` должен быть
снимком (или просто немодифицируемым образом). Пул обязательно указывать, если в кластере больше одного пула.

```
vitastor-cli create --snapshot <snapshot> [-p|--pool <id|name>] <image>
vitastor-cli snap-create [-p|--pool <id|name>] <image>@<snapshot>
```

Создать снимок образа `<name>` (можно использовать любую форму команды). Снимок можно создавать без остановки
клиентов, если пишущий клиент максимум 1.

## modify

`vitastor-cli modify <name> [--rename <new-name>] [--resize <size>] [--readonly | --readwrite] [-f|--force]`

Изменить размер, имя образа или флаг "только для чтения". Снимать флаг "только для чтения"
и уменьшать размер образов, у которых есть дочерние клоны, без `--force` нельзя.

Если новый размер меньше старого, "лишние" данные будут удалены, поэтому перед уменьшением
образа сначала уменьшите файловую систему в нём.

```
-f|--force  Разрешить уменьшение или перевод в чтение-запись образа, у которого есть клоны.
```

## rm

`vitastor-cli rm <from> [<to>] [--writers-stopped]`

Удалить образ `<from>` или все слои от `<from>` до `<to>` (`<to>` должен быть дочерним
образом `<from>`), одновременно меняя родительские образы их клонов (если таковые есть).

`--writers-stopped` позволяет чуть более эффективно удалять образы в частом случае, когда
у удаляемой цепочки есть только один дочерний образ, содержащий небольшой объём данных.
В этом случае дочерний образ вливается в родительский и удаляется, а родительский
переименовывается в дочерний.

В других случаях родительские слои вливаются в дочерние.

## flatten

`vitastor-cli flatten <layer>`

Сделай образ `<layer>` плоским, то есть, скопировать в него данные и разорвать его
соединение с родительскими.

## rm-data

`vitastor-cli rm-data --pool <pool> --inode <inode> [--wait-list] [--min-offset <offset>]`

Удалить данные инода, не меняя метаданные образов.

```
--wait-list   Сначала запросить полный листинг объектов, а потом начать удалять.
              Требует больше памяти, но позволяет правильно печатать прогресс удаления.
--min-offset  Удалять только данные, начиная с заданного смещения.
```

## merge-data

`vitastor-cli merge-data <from> <to> [--target <target>]`

Слить данные слоёв, не меняя метаданные. Вливает данные из слоёв от `<from>` до `<to>`
в целевой образ `<target>`. `<to>` должен быть дочерним образом `<from>`, а `<target>`
должен быть одним из слоёв между `<from>` и `<to>`, включая сами `<from>` и `<to>`.

## alloc-osd

`vitastor-cli alloc-osd`

Атомарно выделить новый номер OSD и зарезервировать его, создав в etcd пустой
ключ `/osd/stats/<n>`.

## simple-offsets

`vitastor-cli simple-offsets <device>`

Рассчитать смещения для простого и тупого создания OSD на диске (без суперблока).

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
