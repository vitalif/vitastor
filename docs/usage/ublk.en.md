[Documentation](../../README.md#documentation) → Usage → UBLK

-----

[Читать на русском](ublk.ru.md)

# UBLK

[ublk](https://docs.kernel.org/block/ublk.html) is a new io_uring-based Linux interface
for user-space block device drivers, available since Linux 6.0.

It's not zero-copy, but it's still a fast implementation, outperforming both [NBD](nbd.en.md)
and [VDUSE](qemu.en.md#vduse) iops-wise and may or may not outperform VDUSE in linear I/O MB/s.
ublk also allows to recover devices even if the server (vitastor-ublk process) dies.

## Example performance comparison

TCP (100G), 3 hosts each with 6 NVMe OSDs, 3 replicas, single client

|                      | direct fio  | NBD         | VDUSE      | UBLK        |
|----------------------|-------------|-------------|------------|-------------|
| linear write         | 3807 MB/s   | 1832 MB/s   | 3226 MB/s  | 3027 MB/s   |
| linear read          | 3067 MB/s   | 1885 MB/s   | 1800 MB/s  | 2076 MB/s   |
| 4k random write Q128 | 128624 iops | 91060 iops  | 94621 iops | 149450 iops |
| 4k random read Q128  | 117769 iops | 153408 iops | 93157 iops | 171987 iops |
| 4k random write Q1   | 8090 iops   | 6442 iops   | 6316 iops  | 7272 iops   |
| 4k random read Q1    | 9474 iops   | 7200 iops   | 6840 iops  | 8038 iops   |

RDMA (100G), 3 hosts each with 6 NVMe OSDs, 3 replicas, single client

|                      | direct fio  | NBD         | VDUSE       | UBLK        |
|----------------------|-------------|-------------|-------------|-------------|
| linear write         | 6998 MB/s   | 1878 MB/s   | 4249 MB/s   | 3140 MB/s   |
| linear read          | 8628 MB/s   | 3389 MB/s   | 5062 MB/s   | 3674 MB/s   |
| 4k random write Q128 | 222541 iops | 181589 iops | 138281 iops | 218222 iops |
| 4k random read Q128  | 412647 iops | 239987 iops | 151663 iops | 269583 iops |
| 4k random write Q1   | 11601 iops  | 8592 iops   | 9111 iops   | 10000 iops  |
| 4k random read Q1    | 10102 iops  | 7788 iops   | 8111 iops   | 8965 iops   |

## Commands

vitastor-ublk supports the following commands:

- [map](#map)
- [unmap](#unmap)
- [ls](#ls)

## map

To create a local block device for a Vitastor image run:

```
vitastor-ublk map [/dev/ublkbN] --image testimg
```

It will output a block device name like /dev/ublkb0 which you can then use as a normal disk.

You can also use `--pool <POOL> --inode <INODE> --size <SIZE>` instead of `--image <IMAGE>` if you want.

vitastor-ublk supports all usual Vitastor configuration options like `--config_path <path_to_config>` plus ublk-specific:

* `--recover` \
  Recover a mapped device if the previous ublk server is dead.
* `--queue_depth 256` \
  Maximum queue size for the device.
* `--max_io_size 1M` \
  Maximum single I/O size for the device. Default: `max(1 MB, pool block size * EC part count)`.
* `--readonly` \
  Make the device read-only.
* `--hdd` \
  Mark the device as rotational.
* `--logfile /path/to/log/file.txt` \
  Write log messages to the specified file instead of dropping them (in background mode)
  or printing them to the standard output (in foreground mode).
* `--dev_num N` \
  Use the specified device /dev/ublkbN instead of automatic selection (alternative syntax
  to /dev/ublkbN positional parameter).
* `--foreground 1` \
  Stay in foreground, do not daemonize.

Note that `ublk_queue_depth` and `ublk_max_io_size` may also be specified
in `/etc/vitastor/vitastor.conf` or in other configuration file specified with `--config_path`.

## unmap

To unmap the device run:

```
vitastor-ublk unmap /dev/ublkb0
```

## ls

```
vitastor-ublk ls [--json]
```

List mapped images.

Example output (normal format):

```
/dev/ublkb0
image: bench
pid: 584536

/dev/ublkb1
image: bench1
pid: 584546
```

Example output (JSON format):

```
{"/dev/ublkb0": {"image": "bench", "pid": 584536}, "/dev/ublkb1": {"image": "bench1", "pid": 584546}}
```
