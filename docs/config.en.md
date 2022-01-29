[Documentation](../README.md#documentation) → Configuration Reference

-----

[Читать на русском](config.ru.md)

# Configuration Reference

Vitastor configuration consists of:
- Configuration parameters (key-value), described here
- [Pool configuration](config/pool.en.md)
- OSD placement tree configuration
- [Inode configuration](config/inode.en.md) i.e. image metadata like name, size and parent reference

Configuration parameters can be set in 3 places:
- Configuration file (`/etc/vitastor/vitastor.conf` or other path)
- etcd key `/vitastor/config/global`. Most variables can be set there, but etcd
  connection parameters should obviously be set in the configuration file.
- Command line of Vitastor components: OSD, mon, fio and QEMU options,
  OpenStack/Proxmox/etc configuration. The latter doesn't allow to set all
  variables directly, but it allows to override the configuration file and
  set everything you need inside it.

In the future, additional configuration methods may be added:
- OSD superblock which will, by design, contain parameters related to the disk
  layout and to one specific OSD.
- OSD-specific keys in etcd like `/vitastor/config/osd/<number>`.

## Parameter Reference

- [Common](config/common.en.md)
- [Network](config/network.en.md)
- [Global Disk Layout](config/layout-cluster.en.md)
- [OSD Disk Layout](config/layout-osd.en.md)
- [OSD Runtime Parameters](config/osd.en.md)
- [Monitor](config/monitor.en.md)
- [Pool configuration](config/pool.en.md)
- [Inode metadata in etcd](docs/config/inode.en.md)
