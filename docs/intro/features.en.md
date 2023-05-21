[Documentation](../../README.md#documentation) → Introduction → Features

-----

[Читать на русском](features.ru.md)

# Features

- [Server-side features](#server-side-features)
- [Plugins and tools](#plugins-and-tools)
- [Roadmap](#roadmap)

## Server-side features

- Basic part: highly-available block storage with symmetric clustering and no SPOF
- [Performance](../performance/comparison1.en.md) ;-D
- [Multiple redundancy schemes](../config/pool.en.md#scheme): Replication, XOR n+1, Reed-Solomon erasure codes
  based on jerasure and ISA-L libraries with any number of data and parity drives in a group
- Configuration via simple JSON data structures in etcd (parameters, pools and images)
- Automatic data distribution over OSDs, with support for:
  - Mathematical optimization for better uniformity and less data movement
  - Multiple pools
  - Placement tree, OSD selection by tags (device classes) and placement root
  - Configurable failure domains
- Recovery of degraded blocks
- Rebalancing (data movement between OSDs)
- [Lazy fsync support](../config/layout-cluster.en.md#immediate_commit)
- Per-OSD and per-image I/O and space usage statistics in etcd
- Snapshots and copy-on-write image clones
- [Write throttling to smooth random write workloads in SSD+HDD configurations](../config/osd.en.md#throttle_small_writes)
- [RDMA/RoCEv2 support via libibverbs](../config/network.en.md#rdma_device)
- [Scrubbing without checksums](../config/osd.en.md#auto_scrub) (verification of copies)

## Plugins and tools

- [Debian and CentOS packages](../installation/packages.en.md)
- [Image management CLI (vitastor-cli)](../usage/cli.en.md)
- [Disk management CLI (vitastor-disk)](docs/usage/disk.en.md)
- Generic user-space client library
- [Native QEMU driver](../usage/qemu.en.md)
- [Loadable fio engine for benchmarks](../usage/fio.en.md)
- [NBD proxy for kernel mounts](../usage/nbd.en.md)
- [CSI plugin for Kubernetes](../installation/kubernetes.en.md)
- [OpenStack support: Cinder driver, Nova and libvirt patches](../installation/openstack.en.md)
- [Proxmox storage plugin and packages](../installation/proxmox.en.md)
- [Simplified NFS proxy for file-based image access emulation (suitable for VMWare)](../usage/nfs.en.md)

## Roadmap

The following features are planned for the future:

- Other administrative tools
- Web GUI
- OpenNebula plugin
- iSCSI proxy
- Multi-threaded client
- Faster failover
- Checksums
- Tiered storage (SSD caching)
- NVDIMM support
- Compression (possibly)
- Read caching using system page cache (possibly)
