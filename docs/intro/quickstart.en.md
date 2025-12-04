[Documentation](../../README.md#documentation) → Introduction → Quick Start

-----

[Читать на русском](quickstart.ru.md)

# Quick Start

- [Preparation](#preparation)
- [Recommended drives](#recommended-drives)
- [Configure monitors](#configure-monitors)
- [Configure OSDs](#configure-osds)
- [Create a pool](#create-a-pool)
- [Check cluster status](#check-cluster-status)
- [Create an image](#create-an-image)
- [Install plugins](#install-plugins)
- [Create VitastorFS](#create-vitastorfs)

## Preparation

- Get some SATA or NVMe SSDs with capacitors (server-grade drives). The best performance
  is achieved with Micron or Kioxia NVMes with atomic write support (see below). You can use desktop
  SSDs with lazy fsync, but prepare for inferior single-thread latency. Read more about
  capacitors [here](../config/layout-cluster.en.md#immediate_commit).
- If you want to use HDDs, get modern HDDs with Media Cache or SSD Cache: HGST Ultrastar,
  Toshiba MG, Seagate EXOS or something similar. If your drives don't have such cache then
  you also need small SSDs for journal and metadata (even 2 GB per 1 TB of HDD space is enough).
- Get a fast network (at least 10 Gbit/s). Something like Mellanox ConnectX-4 with RoCEv2 is ideal.
- Disable CPU powersaving: `cpupower idle-set -D 0 && cpupower frequency-set -g performance`.
- Either [install Vitastor packages](../installation/packages.en.md) or [install Vitastor in Docker](../installation/docker.en.md).

## Recommended drives

- NVMe with atomic write support (ideal!): Micron 7450/7500/7550, Kioxia CD6/CD7/CD8/CD9
- Other NVMe: Micron 9100/9200/9300/9400/9550, Micron 7300, Samsung PM983/PM9A3, Samsung PM1723/1735/1743,
  Intel DC-P3700/P4500/P4600, Intel/Solidigm D5-P4320/P5530, Intel/Solidigm D7-P5500/P5600, Solidigm D7-PS1010/PS1030/P5810,
  Intel Optane, Kingston DC1000B/DC1500M, Kioxia CD6/CD7/CD8/CD9
- SATA SSD: Micron 5100/5200/5300/5400, Samsung PM863/PM883/PM893, Intel/Solidigm D3-S4510/4520/4610/4620, Kingston DC500M
- HDD: HGST Ultrastar, Toshiba MG, Seagate EXOS

## Configure monitors

On the monitor hosts:
- Put identical etcd_address into `/etc/vitastor/vitastor.conf`. Example:
  ```
  {
    "etcd_address": ["10.200.1.10:2379","10.200.1.11:2379","10.200.1.12:2379"]
  }
  ```
- Create systemd units for etcd by running: `/usr/lib/vitastor/mon/make-etcd`
  Or, if you installed Vitastor in Docker, run `systemctl start vitastor-host; docker exec vitastor make-etcd`.
- Start etcd and monitors: `systemctl enable --now vitastor-etcd vitastor-mon`

## Configure OSDs

- Put etcd_address and [osd_network](../config/network.en.md#osd_network) into `/etc/vitastor/vitastor.conf`. Example:
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
- Start all OSDs: `systemctl start vitastor.target`

## Create a pool

Create a pool using vitastor-cli:

```
vitastor-cli create-pool testpool --pg_size 2 --pg_count 256
```

For EC pools the configuration should look like the following:

```
vitastor-cli create-pool testpool --ec 2+2 --pg_count 256
```

Add `--immediate_commit none` if you added `--disable_data_fsync off` at the OSD
initialization step, or if `vitastor-disk` complained about impossibility to
disable drive cache.

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

## Create VitastorFS

If you want to use clustered file system in addition to VM or container images:

- [Follow the instructions here](../usage/nfs.en.md#vitastorfs)
