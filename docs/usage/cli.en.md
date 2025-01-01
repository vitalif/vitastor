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
- [snap-create](#create)
- [modify](#modify)
- [dd](#dd)
- [rm](#rm)
- [flatten](#flatten)
- [rm-data](#rm-data)
- [merge-data](#merge-data)
- [describe](#describe)
- [fix](#fix)
- [alloc-osd](#alloc-osd)
- [rm-osd](#rm-osd)
- [osd-tree](#osd-tree)
- [ls-osd](#ls-osd)
- [modify-osd](#modify-osd)
- [pg-list](#pg-list)
- [create-pool](#create-pool)
- [modify-pool](#modify-pool)
- [ls-pools](#ls-pools)
- [rm-pool](#rm-pool)

Global options:

```
--config_file FILE   Path to Vitastor configuration file
--etcd_address URL   Etcd connection address
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

See also about [how to export snapshots](qemu.en.md#exporting-snapshots).

## modify

`vitastor-cli modify <name> [--rename <new-name>] [--resize <size>] [--readonly | --readwrite] [-f|--force] [--down-ok]`

Rename, resize image or change its readonly status. Images with children can't be made read-write.
If the new size is smaller than the old size, extra data will be purged.
You should resize file system in the image, if present, before shrinking it.

* `--deleted 1|0` - Set/clear 'deleted image' flag (set automatically during unfinished deletes).
* `-f|--force` - Proceed with shrinking or setting readwrite flag even if the image has children.
* `--down-ok` - Proceed with shrinking even if some data will be left on unavailable OSDs.

## dd

```
vitastor-cli dd [iimg=<image> | if=<file>] [oimg=<image> | of=<file>] [bs=1M] \
    [count=N] [seek/oseek=N] [skip/iseek=M] [iodepth=N] [status=progress] \
    [conv=nocreat,noerror,nofsync,trunc,nosparse] [iflag=direct] [oflag=direct,append]
```

Copy data between Vitastor images, files and pipes.

Options can be specified in classic dd style (`key=value`) or like usual (`--key value`).

| <!-- -->        | <!-- -->                                                                |
|-----------------|-------------------------------------------------------------------------|
| `iimg=<image>`  | Copy from Vitastor image `<image>`                                      |
| `if=<file>`     | Copy from file `<file>`                                                 |
| `oimg=<image>`  | Copy to Vitastor image `<image>`                                        |
| `of=<file>`     | Copy to file `<file>`                                                   |
| `bs=1M`         | Set copy block size                                                     |
| `count=N`       | Copy only N input blocks. If N ends in B it counts bytes, not blocks    |
| `seek/oseek=N`  | Skip N output blocks. If N ends in B it counts bytes, not blocks        |
| `skip/iseek=N`  | Skip N input blocks. If N ends in B it counts bytes, not blocks         |
| `iodepth=N`     | Send N reads or writes in parallel (default 4)                          |
| `status=LEVEL`  | The LEVEL of information to print to stderr: none/noxfer/progress       |
| `size=N`        | Specify size for the created output file/image (defaults to input size) |
| `iflag=direct`  | For input files only: use direct I/O                                    |
| `oflag=direct`  | For output files only: use direct I/O                                   |
| `oflag=append`  | For files only: append to output file                                   |
| `conv=nocreat`  | Do not create output file/image                                         |
| `conv=trunc`    | Truncate output file/image                                              |
| `conv=noerror`  | Continue copying after errors                                           |
| `conv=nofsync`  | Do not call fsync before finishing (default behaviour is fsync)         |
| `conv=nosparse` | Write all output blocks including all-zero blocks                       |

## rm

`vitastor-cli rm <from> [<to>] [--writers-stopped] [--down-ok]`

`vitastor-cli rm (--exact|--matching) <glob> ...`

Remove layer(s) and rebase all their children accordingly.

In the first form, remove `<from>` or layers between `<from>` and its child `<to>`.

In the second form, remove all images with exact or pattern-matched names.

Options:

* `--writers-stopped` allows optimised removal in case of a single 'slim' read-write
  child and 'fat' removed parent: the child is merged into parent and parent is renamed
  to child in that case. In other cases parent layers are always merged into children.
* `--exact` - remove multiple images with names matching given glob patterns.
* `--matching` - remove multiple images with given names
* `--down-ok` - continue deletion/merging even if some data will be left on unavailable OSDs.

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
--max-offset  Purge only data before specified offset.
--client_wait_up_timeout 16  Timeout for waiting until PGs are up in seconds.
```

## merge-data

`vitastor-cli merge-data <from> <to> [--target <target>]`

Merge layer data without changing metadata. Merge `<from>`..`<to>` to `<target>`.
`<to>` must be a child of `<from>` and `<target>` may be one of the layers between
`<from>` and `<to>`, including `<from>` and `<to>`.

## describe

`vitastor-cli describe [OPTIONS]`

Describe unclean object locations in the cluster. Options:

```
--osds <osds>
    Only list objects from primary OSD(s) <osds>.
--object-state <states>
    Only list objects in given state(s). State(s) may include:
    degraded, misplaced, incomplete, corrupted, inconsistent.
--pool <pool name or number>
    Only list objects in the given pool.
--pg <pg number>
    Only list objects in the given PG of the pool.
--inode, --min-inode, --max-inode
    Restrict listing to specific inode numbers.
--min-offset, --max-offset
    Restrict listing to specific offsets inside inodes.
```

## fix

`vitastor-cli fix [--objects <objects>] [--bad-osds <osds>] [--part <part>] [--check no]`

Fix inconsistent objects in the cluster by deleting some copies.

```
--objects <objects>
    Objects to fix, either in plain text or JSON format. If not specified,
    object list will be read from STDIN in one of the same formats.
    Plain text format: 0x<inode>:0x<stripe> <any delimiter> 0x<inode>:0x<stripe> ...
    JSON format: [{"inode":"0x...","stripe":"0x..."},...]
--bad-osds <osds>
    Remove inconsistent copies/parts of objects from these OSDs, effectively
    marking them bad and allowing Vitastor to recover objects from other copies.
--part <number>
    Only remove EC part <number> (from 0 to pg_size-1), required for extreme
    edge cases where one OSD has multiple parts of a EC object.
--check no
    Do not recheck that requested objects are actually inconsistent,
    delete requested copies/parts anyway.
```

## alloc-osd

`vitastor-cli alloc-osd`

Allocate a new OSD number and reserve it by creating empty `/osd/stats/<n>` key.

## rm-osd

`vitastor-cli rm-osd [--force] [--allow-data-loss] [--dry-run] <osd_id> [osd_id...]`

Remove metadata and configuration for specified OSD(s) from etcd.

Refuses to remove OSDs with data without `--force` and `--allow-data-loss`.

With `--dry-run` only checks if deletion is possible without data loss and
redundancy degradation.

## osd-tree

`vitastor-cli osd-tree [-l|--long]`

Show current OSD tree, optionally with I/O statistics if -l is specified.

Example output:

```
TYPE     NAME       UP    SIZE  USED%    TAGS          WEIGHT  BLOCK  BITMAP  IMM   NOOUT
host     kaveri
  disk   nvme0n1p1
    osd  3          down  100G  0 %      abc,kaveri    1       128k   4k      none  -
    osd  4          down  100G  0 %                    1       128k   4k      none  -
  disk   nvme1n1p1
    osd  5          down  100G  0 %      abc,kaveri    1       128k   4k      none  -
    osd  6          down  100G  0 %                    1       128k   4k      none  -
host     stump
  osd    1          up    100G  37.29 %  osdone        1       128k   4k      all   -
  osd    2          up    100G  26.8 %   abc           1       128k   4k      all   -
  osd    7          up    100G  21.84 %                1       128k   4k      all   -
  osd    8          up    100G  21.63 %                1       128k   4k      all   -
  osd    9          up    100G  20.69 %                1       128k   4k      all   -
  osd    10         up    100G  21.61 %                1       128k   4k      all   -
  osd    11         up    100G  21.53 %                1       128k   4k      all   -
  osd    12         up    100G  22.4 %                 1       128k   4k      all   -
```

## ls-osd

`vitastor-cli osds|ls-osd|osd-ls [-l|--long]`

Show current OSDs as list, optionally with I/O statistics if -l is specified.

Example output:

```
OSD  PARENT            UP    SIZE  USED%    TAGS          WEIGHT  BLOCK  BITMAP  IMM   NOOUT
3    kaveri/nvme0n1p1  down  100G  0 %      globl,kaveri  1       128k   4k      none  -
4    kaveri/nvme0n1p1  down  100G  0 %                    1       128k   4k      none  -
5    kaveri/nvme1n1p1  down  100G  0 %      globl,kaveri  1       128k   4k      none  -
6    kaveri/nvme1n1p1  down  100G  0 %                    1       128k   4k      none  -
1    stump             up    100G  37.29 %  osdone        1       128k   4k      all   -
2    stump             up    100G  26.8 %   globl         1       128k   4k      all   -
7    stump             up    100G  21.84 %                1       128k   4k      all   -
8    stump             up    100G  21.63 %                1       128k   4k      all   -
9    stump             up    100G  20.69 %                1       128k   4k      all   -
10   stump             up    100G  21.61 %                1       128k   4k      all   -
11   stump             up    100G  21.53 %                1       128k   4k      all   -
12   stump             up    100G  22.4 %                 1       128k   4k      all   -
```

## modify-osd

`vitastor-cli modify-osd [--tags tag1,tag2,...] [--reweight <number>] [--noout true/false] <osd_number>`

Set OSD reweight, tags or noout flag. See detail description in [OSD config documentation](../config/pool.en.md#osd-settings).

## pg-list

`vitastor-cli pg-list|pg-ls|list-pg|ls-pg|ls-pgs [OPTIONS] [state1+state2] [^state3] [...]`

List PGs with any of listed state filters (^ or ! in the beginning is negation). Options:

```
--pool <pool name or number>  Only list PGs of the given pool.
--min <min pg number>         Only list PGs with number >= min.
--max <max pg number>         Only list PGs with number <= max.
```

Examples:

`vitastor-cli pg-list active+degraded`

`vitastor-cli pg-list ^active`

## create-pool

`vitastor-cli create-pool|pool-create <name> (-s <pg_size>|--ec <N>+<K>) -n <pg_count> [OPTIONS]`

Create a pool. Required parameters:

| <!-- -->                 | <!-- -->                                                                              |
|--------------------------|---------------------------------------------------------------------------------------|
| `-s R` or `--pg_size R`  | Number of replicas for replicated pools                                               |
| `--ec N+K`               | Number of data (N) and parity (K) chunks for erasure-coded pools                      |
| `-n N` or `--pg_count N` | PG count for the new pool (start with 10*<OSD count>/pg_size rounded to a power of 2) |

Optional parameters:

| <!-- -->                       | <!-- -->                                                                   |
|--------------------------------|----------------------------------------------------------------------------|
| `--pg_minsize <number>`        | R or N+K minus number of failures to tolerate without downtime ([details](../config/pool.en.md#pg_minsize)) |
| `--failure_domain host`        | Failure domain: host, osd or a level from placement_levels. Default: host  |
| `--root_node <node>`           | Put pool only on child OSDs of this placement tree node                    |
| `--osd_tags <tag>[,<tag>]...`  | Put pool only on OSDs tagged with all specified tags                       |
| `--block_size 128k`            | Put pool only on OSDs with this data block size                            |
| `--bitmap_granularity 4k`      | Put pool only on OSDs with this logical sector size                        |
| `--immediate_commit none`      | Put pool only on OSDs with this or larger immediate_commit (none < small < all) |
| `--level_placement <rules>`    | Use additional failure domain rules (example: "dc=112233")                 |
| `--raw_placement <rules>`      | Specify raw PG generation rules ([details](../config/pool.en.md#raw_placement)) |
| `--primary_affinity_tags tags` | Prefer to put primary copies on OSDs with all specified tags               |
| `--scrub_interval <time>`      | Enable regular scrubbing for this pool. Format: number + unit s/m/h/d/M/y  |
| `--used_for_fs <name>`         | Mark pool as used for VitastorFS with metadata in image <name>             |
| `--pg_stripe_size <number>`    | Increase object grouping stripe                                            |
| `--max_osd_combinations 10000` | Maximum number of random combinations for LP solver input                  |
| `--wait`                       | Wait for the new pool to come online                                       |
| `-f` or `--force`              | Do not check that cluster has enough OSDs to create the pool               |

See also [Pool configuration](../config/pool.en.md) for detailed parameter descriptions.

Examples:

`vitastor-cli create-pool test_x4 -s 4 -n 32`

`vitastor-cli create-pool test_ec42 --ec 4+2 -n 32`

## modify-pool

`vitastor-cli modify-pool|pool-modify <id|name> [--name <new_name>] [PARAMETERS...]`

Modify an existing pool. Modifiable parameters:

```
[-s|--pg_size <number>] [--pg_minsize <number>] [-n|--pg_count <count>]
[--failure_domain <level>] [--root_node <node>] [--osd_tags <tags>] [--no_inode_stats 0|1]
[--max_osd_combinations <number>] [--primary_affinity_tags <tags>] [--scrub_interval <time>]
```

Non-modifiable parameters (changing them WILL lead to data loss):

```
[--block_size <size>] [--bitmap_granularity <size>]
[--immediate_commit <all|small|none>] [--pg_stripe_size <size>]
```

These, however, can still be modified with -f|--force.

See [create-pool](#create-pool) for parameter descriptions.

Examples:

`vitastor-cli modify-pool pool_A --name pool_B`

`vitastor-cli modify-pool 2 --pg_size 4 -n 128`

## rm-pool

`vitastor-cli rm-pool|pool-rm [--force] <id|name>`

Remove a pool. Refuses to remove pools with images without `--force`.

## ls-pools

`vitastor-cli ls-pools|pool-ls|ls-pool|pools [-l] [--detail] [--sort FIELD] [-r] [-n N] [--stats] [<glob> ...]`

List pools (only matching <glob> patterns if passed).

| <!-- -->             | <!-- -->                                              |
|----------------------|-------------------------------------------------------|
| `-l` or `--long`     | Also report I/O statistics                            |
| `--detail`           | Use list format (not table), show all details         |
| `--sort FIELD`       | Sort by specified field (see fields in --json output) |
| `-r` or `--reverse`  | Sort in descending order                              |
| `-n` or `--count N`  | Only list first N items                               |
