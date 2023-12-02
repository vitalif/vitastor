[Documentation](../../README.md#documentation) → Usage → NBD

-----

[Читать на русском](nbd.ru.md)

# NBD

NBD stands for "Network Block Device", but in fact it also functions as "BUSE"
(Block Device in UserSpace). NBD is currently required to mount Vitastor via kernel.
NBD slighly lowers the performance due to additional overhead, but performance still
remains decent (see an example [here](../performance/comparison1.en.md#vitastor-0-4-0-nbd)).

Vitastor Kubernetes CSI driver is based on NBD.

See also [VDUSE](qemu.en.md#vduse).

## Map image

To create a local block device for a Vitastor image run:

```
vitastor-nbd map --image testimg
```

It will output a block device name like /dev/nbd0 which you can then use as a normal disk.

You can also use `--pool <POOL> --inode <INODE> --size <SIZE>` instead of `--image <IMAGE>` if you want.

vitastor-nbd supports all usual Vitastor configuration options like `--config_file <path_to_config>` plus NBD-specific:

* `--nbd_timeout 300` \
  Timeout for I/O operations in seconds after exceeding which the kernel stops
  the device. You can set it to 0 to disable the timeout, but beware that you
  won't be able to stop the device at all if vitastor-nbd process dies.
* `--nbd_max_devices 64 --nbd_max_part 3` \
  Options for the `nbd` kernel module when modprobing it (`nbds_max` and `max_part`).
  note that maximum allowed (nbds_max)*(1+max_part) is 256.
* `--logfile /path/to/log/file.txt` \
  Write log messages to the specified file instead of dropping them (in background mode)
  or printing them to the standard output (in foreground mode).
* `--dev_num N` \
  Use the specified device /dev/nbdN instead of automatic selection.
* `--foreground 1` \
  Stay in foreground, do not daemonize.

Note that `nbd_timeout`, `nbd_max_devices` and `nbd_max_part` options may also be specified
in `/etc/vitastor/vitastor.conf` or in other configuration file specified with `--config_file`.

## Unmap image

To unmap the device run:

```
vitastor-nbd unmap /dev/nbd0
```

## List mapped images

```
vitastor-nbd ls [--json]
```

Example output (normal format):

```
/dev/nbd0
image: bench
pid: 584536

/dev/nbd1
image: bench1
pid: 584546
```

Example output (JSON format):

```
{"/dev/nbd0": {"image": "bench", "pid": 584536}, "/dev/nbd1": {"image": "bench1", "pid": 584546}}
```
