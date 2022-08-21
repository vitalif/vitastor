# Vitastor

[Читать на русском](README-ru.md)

## The Idea

Make Clustered Block Storage Fast Again.

Vitastor is a distributed block SDS, direct replacement of Ceph RBD and internal SDS's
of public clouds. However, in contrast to them, Vitastor is fast and simple at the same time.
The only thing is it's slightly young :-).

Vitastor is architecturally similar to Ceph which means strong consistency,
primary-replication, symmetric clustering and automatic data distribution over any
number of drives of any size with configurable redundancy (replication or erasure codes/XOR).

Vitastor targets SSD and SSD+HDD clusters with at least 10 Gbit/s network, supports
TCP and RDMA and may achieve 4 KB read and write latency as low as ~0.1 ms
with proper hardware which is ~10 times faster than other popular SDS's like Ceph
or internal systems of public clouds.

Vitastor supports QEMU, NBD, NFS protocols, OpenStack, Proxmox, Kubernetes drivers.
More drivers may be created easily.

Read more details below in the documentation.

## Talks and presentations

- DevOpsConf'2021: presentation ([in Russian](https://vitastor.io/presentation/devopsconf/devopsconf.html),
  [in English](https://vitastor.io/presentation/devopsconf/devopsconf_en.html)),
  [video](https://vitastor.io/presentation/devopsconf/talk.webm)
- Highload'2022: presentation ([in Russian](https://vitastor.io/presentation/highload/highload.html)),
  [video](https://vitastor.io/presentation/highload/talk.webm)

## Documentation

- Introduction
  - [Quick Start](docs/intro/quickstart.en.md)
  - [Features](docs/intro/features.en.md)
  - [Architecture](docs/intro/architecture.en.md)
  - [Author and license](docs/intro/author.en.md)
- Installation
  - [Packages](docs/installation/packages.en.md)
  - [Proxmox](docs/installation/proxmox.en.md)
  - [OpenStack](docs/installation/openstack.en.md)
  - [Kubernetes CSI](docs/installation/kubernetes.en.md)
  - [Building from Source](docs/installation/source.en.md)
- Configuration
  - [Overview](docs/config.en.md)
  - Parameter Reference
    - [Common](docs/config/common.en.md)
    - [Network](docs/config/network.en.md)
    - [Global Disk Layout](docs/config/layout-cluster.en.md)
    - [OSD Disk Layout](docs/config/layout-osd.en.md)
    - [OSD Runtime Parameters](docs/config/osd.en.md)
    - [Monitor](docs/config/monitor.en.md)
  - [Pool configuration](docs/config/pool.en.md)
  - [Image metadata in etcd](docs/config/inode.en.md)
- Usage
  - [vitastor-cli](docs/usage/cli.en.md) (command-line interface)
  - [vitastor-disk](docs/usage/disk.en.md) (disk management tool)
  - [fio](docs/usage/fio.en.md) for benchmarks
  - [NBD](docs/usage/nbd.en.md) for kernel mounts
  - [QEMU and qemu-img](docs/usage/qemu.en.md)
  - [NFS](docs/usage/nfs.en.md) emulator for VMWare and similar
- Performance
  - [Understanding storage performance](docs/performance/understanding.en.md)
  - [Theoretical performance](docs/performance/theoretical.en.md)
  - [Example comparison with Ceph](docs/performance/comparison1.en.md)

## Author and License

Copyright (c) Vitaliy Filippov (vitalif [at] yourcmc.ru), 2019+

Join Vitastor Telegram Chat: https://t.me/vitastor

All server-side code (OSD, Monitor and so on) is licensed under the terms of
Vitastor Network Public License 1.1 (VNPL 1.1), a copyleft license based on
GNU GPLv3.0 with the additional "Network Interaction" clause which requires
opensourcing all programs directly or indirectly interacting with Vitastor
through a computer network and expressly designed to be used in conjunction
with it ("Proxy Programs"). Proxy Programs may be made public not only under
the terms of the same license, but also under the terms of any GPL-Compatible
Free Software License, as listed by the Free Software Foundation.
This is a stricter copyleft license than the Affero GPL.

Please note that VNPL doesn't require you to open the code of proprietary
software running inside a VM if it's not specially designed to be used with
Vitastor.

Basically, you can't use the software in a proprietary environment to provide
its functionality to users without opensourcing all intermediary components
standing between the user and Vitastor or purchasing a commercial license
from the author 😀.

Client libraries (cluster_client and so on) are dual-licensed under the same
VNPL 1.1 and also GNU GPL 2.0 or later to allow for compatibility with GPLed
software like QEMU and fio.

You can find the full text of VNPL-1.1 in the file [VNPL-1.1.txt](VNPL-1.1.txt).
GPL 2.0 is also included in this repository as [GPL-2.0.txt](GPL-2.0.txt).
