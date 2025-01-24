[Documentation](../../README.md#documentation) → Usage → NBD

-----

[Читать на русском](nbd.ru.md)

# NBD

NBD stands for "Network Block Device", but in fact it also functions as "BUSE"
(Block Device in UserSpace). NBD is currently required to mount Vitastor via kernel.
NBD slighly lowers the performance due to additional overhead, but performance still
remains decent (see an example [here](../performance/comparison1.en.md#vitastor-0-4-0-nbd)).

See also [VDUSE](qemu.en.md#vduse) as a better alternative to NBD.

Vitastor Kubernetes CSI driver uses NBD when VDUSE is unavailable.

Supports the following commands:

- [map](#map)
- [unmap](#unmap)
- [ls](#ls)
- [netlink-map](#netlink-map)
- [netlink-unmap](#netlink-unmap)
- [netlink-revive](#netlink-revive)

## map

To create a local block device for a Vitastor image run:

```
vitastor-nbd map [/dev/nbdN] --image testimg
```

It will output a block device name like /dev/nbd0 which you can then use as a normal disk.

You can also use `--pool <POOL> --inode <INODE> --size <SIZE>` instead of `--image <IMAGE>` if you want.

vitastor-nbd supports all usual Vitastor configuration options like `--config_path <path_to_config>` plus NBD-specific:

* `--nbd_timeout 0` \
  Timeout for I/O operations in seconds after exceeding which the kernel stops the device.
  Before Linux 5.19, if nbd_timeout is 0, a dead NBD device can't be removed from
  the system at all without rebooting.
* `--nbd_max_devices 64 --nbd_max_part 3` \
  Options for the `nbd` kernel module when modprobing it (`nbds_max` and `max_part`).
* `--logfile /path/to/log/file.txt` \
  Write log messages to the specified file instead of dropping them (in background mode)
  or printing them to the standard output (in foreground mode).
* `--dev_num N` \
  Use the specified device /dev/nbdN instead of automatic selection (alternative syntax
  to /dev/nbdN positional parameter).
* `--foreground 1` \
  Stay in foreground, do not daemonize.

Note that `nbd_timeout`, `nbd_max_devices` and `nbd_max_part` options may also be specified
in `/etc/vitastor/vitastor.conf` or in other configuration file specified with `--config_path`.

## unmap

To unmap the device run:

```
vitastor-nbd unmap /dev/nbd0
```

## ls

```
vitastor-nbd ls [--json]
```

List mapped images.

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

## netlink-map

```
vitastor-nbd netlink-map [/dev/nbdN] (--image <image> | --pool <pool> --inode <inode> --size <size in bytes>)
```

On recent kernel versions it's also possible to map NBD devices using netlink interface.

This is an experimental feature because it doesn't solve all issues of NBD. Differences from regular ioctl-based 'map':

1. netlink-map can create new `/dev/nbdN` devices (those not present in /dev/).
2. netlink-mapped devices can be unmapped only using `netlink-unmap` command.
3. netlink-mapped devices don't show up `ls` output (yet).
4. Dead netlink-mapped devices can be 'revived' using `netlink-revive`.
   However, old I/O requests will hang forever if `nbd_timeout` is not specified.
5. netlink-map supports additional options:

* `--nbd_conn_timeout 0` \
  Disconnect a dead device automatically after this number of seconds.
* `--nbd_destroy_on_disconnect 1` \
  Delete the nbd device on disconnect.
* `--nbd_disconnect_on_close 1` \
  Disconnect the nbd device on close by last opener.
* `--nbd_ro 1` \
  Set device into read only mode.

## netlink-unmap

```
vitastor-nbd netlink-unmap /dev/nbdN
```

Unmap a device using netlink interface. Works with both netlink and ioctl mapped devices.

## netlink-revive

```
vitastor-nbd netlink-revive /dev/nbdX (--image <image> | --pool <pool> --inode <inode> --size <size in bytes>)
```

Restart a dead NBD netlink-mapped device without removing it. Supports the same options as `netlink-map`.
