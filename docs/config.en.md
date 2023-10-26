[Documentation](../README.md#documentation) → Configuration Reference

-----

[Читать на русском](config.ru.md)

# Configuration Reference

Vitastor configuration consists of:
- [Configuration parameters (key-value)](#parameter-reference)
- [Pool configuration](config/pool.en.md)
- [OSD placement tree configuration](config/pool.en.md#placement-tree)
- [Separate OSD settings](config/pool.en.md#osd-settings)
- [Inode configuration](config/inode.en.md) i.e. image metadata like name, size and parent reference

Configuration parameters can be set in 3 places:
- Configuration file (`/etc/vitastor/vitastor.conf` or other path)
- etcd key `/vitastor/config/global`. Most variables can be set there, but etcd
  connection parameters should obviously be set in the configuration file.
- Command line of Vitastor components: OSD (when you run it without vitastor-disk),
  mon, fio and QEMU options, OpenStack/Proxmox/etc configuration. The latter
  doesn't allow to set all variables directly, but it allows to override the
  configuration file and set everything you need inside it.
- OSD superblocks created by [vitastor-disk](usage/disk.en.md) contain
  primarily disk layout parameters of specific OSDs. In fact, these parameters
  are automatically passed into the command line of vitastor-osd process, so
  they have the same "status" as command-line parameters.

In the future, additional configuration methods may be added:
- OSD-specific keys in etcd like `/vitastor/config/osd/<number>`.

## Parameter Reference

- [Common](config/common.en.md)
- [Network](config/network.en.md)
- [Client](config/client.en.md)
- [Global Disk Layout](config/layout-cluster.en.md)
- [OSD Disk Layout](config/layout-osd.en.md)
- [OSD Runtime Parameters](config/osd.en.md)
- [Monitor](config/monitor.en.md)
