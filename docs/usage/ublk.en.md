[Documentation](../../README.md#documentation) → Usage → ublk

-----

[Читать на русском](ublk.ru.md)

# UBLK

[ublk](https://docs.kernel.org/block/ublk.html) is a new io_uring-based Linux interface
for user-space block device drivers, available since Linux 6.0.

It's still not zero-copy, but so far it's the fastest userspace block device interface,
outperforming both [NBD](nbd.en.md) and [VDUSE](qemu.en.md#vduse). It also allows to recover
devices even if the server (vitastor-ublk process) dies.

Supports the following commands:

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
