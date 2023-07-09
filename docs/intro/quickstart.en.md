[Documentation](../../README.md#documentation) → Introduction → Quick Start

-----

[Читать на русском](quickstart.ru.md)

# Quick Start

- [Preparation](#preparation)
- [Configure monitors](#configure-monitors)
- [Configure OSDs](#configure-osds)
- [Create a pool](#create-a-pool)
- [Check cluster status](#check-cluster-status)
- [Create an image](#create-an-image)
- [Install plugins](#install-plugins)

## Preparation

- Get some SATA or NVMe SSDs with capacitors (server-grade drives). You can use desktop SSDs
  with lazy fsync, but prepare for inferior single-thread latency. Read more about capacitors
  [here](../config/layout-cluster.en.md#immediate_commit).
- If you want to use HDDs, get modern HDDs with Media Cache or SSD Cache: HGST Ultrastar,
  Toshiba MG08, Seagate EXOS or something similar. If your drives don't have such cache then
  you also need small SSDs for journal and metadata (even 2 GB per 1 TB of HDD space is enough).
- Get a fast network (at least 10 Gbit/s). Something like Mellanox ConnectX-4 with RoCEv2 is ideal.
- Disable CPU powersaving: `cpupower idle-set -D 0 && cpupower frequency-set -g performance`.
- [Install Vitastor packages](../installation/packages.en.md).

## Configure monitors

On the monitor hosts:
- Put identical etcd_address into `/etc/vitastor/vitastor.conf`. Example:
  ```
  {
    "etcd_address": ["10.200.1.10:2379","10.200.1.11:2379","10.200.1.12:2379"]
  }
  ```
- Create systemd units for etcd by running: `/usr/lib/vitastor/mon/make-etcd`
- Start etcd and monitors: `systemctl enable --now etcd vitastor-mon`

## Configure OSDs

- Put etcd_address and osd_network into `/etc/vitastor/vitastor.conf`. Example:
  ```
  {
    "etcd_address": ["10.200.1.10:2379","10.200.1.11:2379","10.200.1.12:2379"],
    "osd_network": "10.200.1.0/24"
  }
  ```
- Initialize OSDs:
  - SSD-only or HDD-only: `vitastor-disk prepare /dev/sdXXX [/dev/sdYYY ...]`.
    Add `--disable_data_fsync off` to leave disk write cache enabled if you use
    desktop SSDs without capacitors. Do NOT add `--disable_data_fsync off` if you
    use HDDs or SSD+HDD.
  - Hybrid, SSD+HDD: `vitastor-disk prepare --hybrid /dev/sdXXX [/dev/sdYYY ...]`.
    Pass all your devices (HDD and SSD) to this script &mdash; it will partition disks and initialize journals on its own.
    This script skips HDDs which are already partitioned so if you want to use non-empty disks for
    Vitastor you should first wipe them with `wipefs -a`. SSDs with GPT partition table are not skipped,
    but some free unpartitioned space must be available because the script creates new partitions for journals.
- You can change OSD configuration in units or in `vitastor.conf`.
  Check [Configuration Reference](../config.en.md) for parameter descriptions.
- If all your drives have capacitors, and even if not, but if you ran `vitastor-disk`
  without `--disable_data_fsync off` at the first step, then put the following
  setting into etcd: \
  `etcdctl --endpoints=... put /vitastor/config/global '{"immediate_commit":"all"}'`
- Start all OSDs: `systemctl start vitastor.target`

## Create a pool

Create pool configuration in etcd:

```
etcdctl --endpoints=... put /vitastor/config/pools '{"1":{"name":"testpool",
  "scheme":"replicated","pg_size":2,"pg_minsize":1,"pg_count":256,"failure_domain":"host"}}'
```

For EC pools the configuration should look like the following:

```
etcdctl --endpoints=... put /vitastor/config/pools '{"2":{"name":"ecpool",
  "scheme":"ec","pg_size":4,"parity_chunks":2,"pg_minsize":2,"pg_count":256,"failure_domain":"host"}}'
```

After you do this, one of the monitors will configure PGs and OSDs will start them.

If you use HDDs you should also add `"block_size": 1048576` to pool configuration.
The other option is to add it into /vitastor/config/global, in this case it will
apply to all pools by default.

## Check cluster status

`vitastor-cli status`

Or you can check PG states with `etcdctl --endpoints=... get --prefix /vitastor/pg/state`. All PGs should become 'active'.

## Create an image

Use vitastor-cli ([read CLI documentation here](../usage/cli.en.md)):

```
vitastor-cli create -s 10G testimg
```

After that, you can [run benchmarks](../usage/fio.en.md) or [start QEMU manually](../usage/qemu.en.md) with this image.

## Install plugins

- [Proxmox](../installation/proxmox.en.md)
- [OpenStack](../installation/openstack.en.md)
- [Kubernetes CSI](../installation/kubernetes.en.md)
