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
- [rm-osd](#rm-osd)

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

Show cluster status.

Example output:

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

Show pool space statistics.

Example output:

```
NAME      SCHEME  PGS  TOTAL    USED    AVAILABLE  USED%   EFFICIENCY
testpool  2/1     32   100 G    34.2 G  60.7 G     39.23%  100%
size1     1/1     32   199.9 G  10 G    121.5 G    39.23%  100%
kaveri    2/1     32   0 B      10 G    0 B        100%    0%
```

In the example above, "kaveri" pool has "zero" efficiency because all its OSD are down.

## ls

`vitastor-cli ls [-l] [-p POOL] [--sort FIELD] [-r] [-n N] [<glob> ...]`

List images (only matching `<glob>` pattern(s) if passed).

Options:

```
-p|--pool POOL  Filter images by pool ID or name
-l|--long       Also report allocated size and I/O statistics
--del           Also include delete operation statistics
--sort FIELD    Sort by specified field (name, size, used_size, <read|write|delete>_<iops|bps|lat|queue>)
-r|--reverse    Sort in descending order
-n|--count N    Only list first N items
```

Example output:

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

Create an image. You may use K/M/G/T suffixes for `<size>`. If `--parent` is specified,
a copy-on-write image clone is created. Parent must be a snapshot (readonly image).
Pool must be specified if there is more than one pool.

```
vitastor-cli create --snapshot <snapshot> [-p|--pool <id|name>] <image>
vitastor-cli snap-create [-p|--pool <id|name>] <image>@<snapshot>
```

Create a snapshot of image `<name>` (either form can be used). May be used live if only a single writer is active.

## modify

`vitastor-cli modify <name> [--rename <new-name>] [--resize <size>] [--readonly | --readwrite] [-f|--force]`

Rename, resize image or change its readonly status. Images with children can't be made read-write.
If the new size is smaller than the old size, extra data will be purged.
You should resize file system in the image, if present, before shrinking it.

```
-f|--force  Proceed with shrinking or setting readwrite flag even if the image has children.
```

## rm

`vitastor-cli rm <from> [<to>] [--writers-stopped]`

Remove `<from>` or all layers between `<from>` and `<to>` (`<to>` must be a child of `<from>`),
rebasing all their children accordingly. --writers-stopped allows merging to be a bit
more effective in case of a single 'slim' read-write child and 'fat' removed parent:
the child is merged into parent and parent is renamed to child in that case.
In other cases parent layers are always merged into children.

## flatten

`vitastor-cli flatten <layer>`

Flatten a layer, i.e. merge data and detach it from parents.

## rm-data

`vitastor-cli rm-data --pool <pool> --inode <inode> [--wait-list] [--min-offset <offset>]`

Remove inode data without changing metadata.

```
--wait-list   Retrieve full objects listings before starting to remove objects.
              Requires more memory, but allows to show correct removal progress.
--min-offset  Purge only data starting with specified offset.
```

## merge-data

`vitastor-cli merge-data <from> <to> [--target <target>]`

Merge layer data without changing metadata. Merge `<from>`..`<to>` to `<target>`.
`<to>` must be a child of `<from>` and `<target>` may be one of the layers between
`<from>` and `<to>`, including `<from>` and `<to>`.

## alloc-osd

`vitastor-cli alloc-osd`

Allocate a new OSD number and reserve it by creating empty `/osd/stats/<n>` key.

## rm-osd

`vitastor-cli rm-osd [--force] [--allow-data-loss] [--dry-run] <osd_id> [osd_id...]`

Remove metadata and configuration for specified OSD(s) from etcd.

Refuses to remove OSDs with data without `--force` and `--allow-data-loss`.

With `--dry-run` only checks if deletion is possible without data loss and
redundancy degradation.
