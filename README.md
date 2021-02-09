## Vitastor

## The Idea

Make Software-Defined Block Storage Great Again.

Vitastor is a small, simple and fast clustered block storage (storage for VM drives),
architecturally similar to Ceph which means strong consistency, primary-replication, symmetric
clustering and automatic data distribution over any number of drives of any size
with configurable redundancy (replication or erasure codes/XOR).

## Features

Vitastor is currently a pre-release, a lot of features are missing and you can still expect
breaking changes in the future. However, the following is implemented:

- Basic part: highly-available block storage with symmetric clustering and no SPOF
- Performance ;-D
- Multiple redundancy schemes: Replication, XOR n+1, Reed-Solomon erasure codes
  based on jerasure library with any number of data and parity drives in a group
- Configuration via simple JSON data structures in etcd
- Automatic data distribution over OSDs, with support for:
  - Mathematical optimization for better uniformity and less data movement
  - Multiple pools
  - Placement tree, OSD selection by tags (device classes) and placement root
  - Configurable failure domains
- Recovery of degraded blocks
- Rebalancing (data movement between OSDs)
- Lazy fsync support
- I/O statistics reporting to etcd
- Generic user-space client library
- QEMU driver (built out-of-tree)
- Loadable fio engine for benchmarks (also built out-of-tree)
- NBD proxy for kernel mounts
- Inode removal tool (vitastor-rm)
- Packaging for Debian and CentOS
- Per-inode I/O and space usage statistics
- Inode metadata storage in etcd
- Snapshots and copy-on-write image clones

## Roadmap

- OSD creation tool (OSDs currently have to be created by hand)
- Other administrative tools
- Proxmox and OpenNebula plugins
- iSCSI proxy
- Operation timeouts and better failure detection
- Scrubbing without checksums (verification of replicas)
- Checksums
- SSD+HDD optimizations, possibly including tiered storage and soft journal flushes
- RDMA and NVDIMM support
- Web GUI
- Compression (possibly)
- Read caching using system page cache (possibly)

## Architecture

Similarities:

- Just like Ceph, Vitastor has Pools, PGs, OSDs, Monitors, Failure Domains, Placement Tree.
- Just like Ceph, Vitastor is transactional (even though there's a "lazy fsync mode" which
  doesn't implicitly flush every operation to disks).
- OSDs also have journal and metadata and they can also be put on separate drives.
- Just like in Ceph, client library attempts to recover from any cluster failure so
  you can basically reboot the whole cluster and only pause, but not crash, your clients
  (I consider this a bug if the client crashes in that case).

Some basic terms for people not familiar with Ceph:

- OSD (Object Storage Daemon) is a process that stores data and serves read/write requests.
- PG (Placement Group) is a container for data that (normally) shares the same replicas.
- Pool is a container for data that has the same redundancy scheme and placement rules.
- Monitor is a separate daemon that watches cluster state and handles failures.
- Failure Domain is a group of OSDs that you allow to fail. It's "host" by default.
- Placement Tree groups OSDs in a hierarchy to later split them into Failure Domains.

Architectural differences from Ceph:

- Vitastor's primary focus is on SSDs. Proper SSD+HDD optimizations may be added in the future, though.
- Vitastor OSD is (and will always be) single-threaded. If you want to dedicate more than 1 core
  per drive you should run multiple OSDs each on a different partition of the drive.
  Vitastor isn't CPU-hungry though (as opposed to Ceph), so 1 core is sufficient in a lot of cases.
- Metadata and journal are always kept in memory. Metadata size depends linearly on drive capacity
  and data store block size which is 128 KB by default. With 128 KB blocks metadata should occupy
  around 512 MB per 1 TB (which is still less than Ceph wants). Journal doesn't have to be big,
  the example test below was conducted with only 16 MB journal. A big journal is probably even
  harmful as dirty write metadata also take some memory.
- Vitastor storage layer doesn't have internal copy-on-write or redirect-write. I know that maybe
  it's possible to create a good copy-on-write storage, but it's much harder and makes performance
  less deterministic, so CoW isn't used in Vitastor.
- The basic layer of Vitastor is block storage with fixed-size blocks, not object storage with
  rich semantics like in Ceph (RADOS).
- There's a "lazy fsync" mode which allows to batch writes before flushing them to the disk.
  This allows to use Vitastor with desktop SSDs, but still lowers performance due to additional
  network roundtrips, so use server SSDs with capacitor-based power loss protection
  ("Advanced Power Loss Protection") for best performance.
- PGs are ephemeral. This means that they aren't stored on data disks and only exist in memory
  while OSDs are running.
- Recovery process is per-object (per-block), not per-PG. Also there are no PGLOGs.
- Monitors don't store data. Cluster configuration and state is stored in etcd in simple human-readable
  JSON structures. Monitors only watch cluster state and handle data movement.
  Thus Vitastor's Monitor isn't a critical component of the system and is more similar to Ceph's Manager.
  Vitastor's Monitor is implemented in node.js.
- PG distribution isn't based on consistent hashes. All PG mappings are stored in etcd.
  Rebalancing PGs between OSDs is done by mathematical optimization - data distribution problem
  is reduced to a linear programming problem and solved by lp_solve. This allows for almost
  perfect (96-99% uniformity compared to Ceph's 80-90%) data distribution in most cases, ability
  to map PGs by hand without breaking rebalancing logic, reduced OSD peer-to-peer communication
  (on average, OSDs have fewer peers) and less data movement. It also probably has a drawback -
  this method may fail in very large clusters, but up to several hundreds of OSDs it's perfectly fine.
  It's also easy to add consistent hashes in the future if something proves their necessity.
- There's no separate CRUSH layer. You select pool redundancy scheme, placement root, failure domain
  and so on directly in pool configuration.

## Understanding Storage Performance

The most important thing for fast storage is latency, not parallel iops.

The best possible latency is achieved with one thread and queue depth of 1 which basically means
"client load as low as possible". In this case IOPS = 1/latency, and this number doesn't
scale with number of servers, drives, server processes or threads and so on.
Single-threaded IOPS and latency numbers only depend on *how fast a single daemon is*.

Why is it important? It's important because some of the applications *can't* use
queue depth greater than 1 because their task isn't parallelizable. A notable example
is any ACID DBMS because all of them write their WALs sequentially with fsync()s.

fsync, by the way, is another important thing often missing in benchmarks. The point is
that drives have cache buffers and don't guarantee that your data is actually persisted
until you call fsync() which is translated to a FLUSH CACHE command by the OS.

Desktop SSDs are very fast without fsync - NVMes, for example, can process ~80000 write
operations per second with queue depth of 1 without fsync - but they're really slow with
fsync because they have to actually write data to flash chips when you call fsync. Typical
number is around 1000-2000 iops with fsync.

Server SSDs often have supercapacitors that act as a built-in UPS and allow the drive
to flush its DRAM cache to the persistent flash storage when a power loss occurs.
This makes them perform equally well with and without fsync. This feature is called
"Advanced Power Loss Protection" by Intel; other vendors either call it similarly
or directly as "Full Capacitor-Based Power Loss Protection".

All software-defined storages that I currently know are slow in terms of latency.
Notable examples are Ceph and internal SDSes used by cloud providers like Amazon, Google,
Yandex and so on. They're all slow and can only reach ~0.3ms read and ~0.6ms 4 KB write latency
with best-in-slot hardware.

And that's in the SSD era when you can buy an SSD that has ~0.04ms latency for 100 $.

I use the following 6 commands with small variations to benchmark any storage:

- Linear write:
  `fio -ioengine=libaio -direct=1 -invalidate=1 -name=test -bs=4M -iodepth=32 -rw=write -runtime=60 -filename=/dev/sdX`
- Linear read:
  `fio -ioengine=libaio -direct=1 -invalidate=1 -name=test -bs=4M -iodepth=32 -rw=read -runtime=60 -filename=/dev/sdX`
- Random write latency (T1Q1, this hurts storages the most):
  `fio -ioengine=libaio -direct=1 -invalidate=1 -name=test -bs=4k -iodepth=1 -fsync=1 -rw=randwrite -runtime=60 -filename=/dev/sdX`
- Random read latency (T1Q1):
  `fio -ioengine=libaio -direct=1 -invalidate=1 -name=test -bs=4k -iodepth=1 -rw=randread -runtime=60 -filename=/dev/sdX`
- Parallel write iops (use numjobs if a single CPU core is insufficient to saturate the load):
  `fio -ioengine=libaio -direct=1 -invalidate=1 -name=test -bs=4k -iodepth=128 [-numjobs=4 -group_reporting] -rw=randwrite -runtime=60 -filename=/dev/sdX`
- Parallel read iops (use numjobs if a single CPU core is insufficient to saturate the load):
  `fio -ioengine=libaio -direct=1 -invalidate=1 -name=test -bs=4k -iodepth=128 [-numjobs=4 -group_reporting] -rw=randread -runtime=60 -filename=/dev/sdX`

## Vitastor's Theoretical Maximum Random Access Performance

Replicated setups:
- Single-threaded (T1Q1) read latency: 1 network roundtrip + 1 disk read.
- Single-threaded write+fsync latency:
  - With immediate commit: 2 network roundtrips + 1 disk write.
  - With lazy commit: 4 network roundtrips + 1 disk write + 1 disk flush.
- Saturated parallel read iops: min(network bandwidth, sum(disk read iops)).
- Saturated parallel write iops: min(network bandwidth, sum(disk write iops / number of replicas / write amplification)).

EC/XOR setups:
- Single-threaded (T1Q1) read latency: 1.5 network roundtrips + 1 disk read.
- Single-threaded write+fsync latency:
  - With immediate commit: 3.5 network roundtrips + 1 disk read + 2 disk writes.
  - With lazy commit: 5.5 network roundtrips + 1 disk read + 2 disk writes + 2 disk fsyncs.
  - 0.5 in actually (k-1)/k which means that an additional roundtrip doesn't happen when
    the read sub-operation can be served locally.
- Saturated parallel read iops: min(network bandwidth, sum(disk read iops)).
- Saturated parallel write iops: min(network bandwidth, sum(disk write iops * number of data drives / (number of data + parity drives) / write amplification)).
  In fact, you should put disk write iops under the condition of ~10% reads / ~90% writes in this formula.

Write amplification for 4 KB blocks is usually 3-5 in Vitastor:
1. Journal block write
2. Journal data write
3. Metadata block write
4. Another journal block write for EC/XOR setups
5. Data block write

If you manage to get an SSD which handles 512 byte blocks well (Optane?) you may
lower 1, 3 and 4 to 512 bytes (1/8 of data size) and get WA as low as 2.375.

Lazy fsync also reduces WA for parallel workloads because journal blocks are only
written when they fill up or fsync is requested.

## Example Comparison with Ceph

Hardware configuration: 4 nodes, each with:
- 6x SATA SSD Intel D3-4510 3.84 TB
- 2x Xeon Gold 6242 (16 cores @ 2.8 GHz)
- 384 GB RAM
- 1x 25 GbE network interface (Mellanox ConnectX-4 LX), connected to a Juniper QFX5200 switch

CPU powersaving was disabled. Both Vitastor and Ceph were configured with 2 OSDs per 1 SSD.

All of the results below apply to 4 KB blocks and random access (unless indicated otherwise).

Raw drive performance:
- T1Q1 write ~27000 iops (~0.037ms latency)
- T1Q1 read ~9800 iops (~0.101ms latency)
- T1Q32 write ~60000 iops
- T1Q32 read ~81700 iops

Ceph 15.2.4 (Bluestore):
- T1Q1 write ~1000 iops (~1ms latency)
- T1Q1 read ~1750 iops (~0.57ms latency)
- T8Q64 write ~100000 iops, total CPU usage by OSDs about 40 virtual cores on each node
- T8Q64 read ~480000 iops, total CPU usage by OSDs about 40 virtual cores on each node

T8Q64 tests were conducted over 8 400GB RBD images from all hosts (every host was running 2 instances of fio).
This is because Ceph has performance penalties related to running multiple clients over a single RBD image.

cephx_sign_messages was set to false during tests, RocksDB and Bluestore settings were left at defaults.

In fact, not that bad for Ceph. These servers are an example of well-balanced Ceph nodes.
However, CPU usage and I/O latency were through the roof, as usual.

Vitastor:
- T1Q1 write: 7087 iops (0.14ms latency)
- T1Q1 read: 6838 iops (0.145ms latency)
- T2Q64 write: 162000 iops, total CPU usage by OSDs about 3 virtual cores on each node
- T8Q64 read: 895000 iops, total CPU usage by OSDs about 4 virtual cores on each node
- Linear write (4M T1Q32): 2800 MB/s
- Linear read (4M T1Q32): 1500 MB/s

T8Q64 read test was conducted over 1 larger inode (3.2T) from all hosts (every host was running 2 instances of fio).
Vitastor has no performance penalties related to running multiple clients over a single inode.
If conducted from one node with all primary OSDs moved to other nodes the result was slightly lower (689000 iops),
this is because all operations resulted in network roundtrips between the client and the primary OSD.
When fio was colocated with OSDs (like in Ceph benchmarks above), 1/4 of the read workload actually
used the loopback network.

Vitastor was configured with: `--disable_data_fsync true --immediate_commit all --flusher_count 8
  --disk_alignment 4096 --journal_block_size 4096 --meta_block_size 4096
  --journal_no_same_sector_overwrites true --journal_sector_buffer_count 1024
  --journal_size 16777216`.

### EC/XOR 2+1

Vitastor:
- T1Q1 write: 2808 iops (~0.355ms latency)
- T1Q1 read: 6190 iops (~0.16ms latency)
- T2Q64 write: 85500 iops, total CPU usage by OSDs about 3.4 virtual cores on each node
- T8Q64 read: 812000 iops, total CPU usage by OSDs about 4.7 virtual cores on each node
- Linear write (4M T1Q32): 3200 MB/s
- Linear read (4M T1Q32): 1800 MB/s

Ceph:
- T1Q1 write: 730 iops (~1.37ms latency)
- T1Q1 read: 1500 iops with cold cache (~0.66ms latency), 2300 iops after 2 minute metadata cache warmup (~0.435ms latency)
- T4Q128 write (4 RBD images): 45300 iops, total CPU usage by OSDs about 30 virtual cores on each node
- T8Q64 read (4 RBD images): 278600 iops, total CPU usage by OSDs about 40 virtual cores on each node
- Linear write (4M T1Q32): 1950 MB/s before preallocation, 2500 MB/s after preallocation
- Linear read (4M T1Q32): 2400 MB/s

### NBD

NBD is currently required to mount Vitastor via kernel, but it imposes additional overhead
due to additional copying between the kernel and userspace. This mostly hurts linear
bandwidth, not iops.

Vitastor with single-thread NBD on the same hardware:
- T1Q1 write: 6000 iops (0.166ms latency)
- T1Q1 read: 5518 iops (0.18ms latency)
- T1Q128 write: 94400 iops
- T1Q128 read: 103000 iops
- Linear write (4M T1Q128): 1266 MB/s (compared to 2800 MB/s via fio)
- Linear read (4M T1Q128): 975 MB/s (compared to 1500 MB/s via fio)

## Installation

### Debian

- Trust Vitastor package signing key:
  `wget -q -O - https://vitastor.io/debian/pubkey | sudo apt-key add -`
- Add Vitastor package repository to your /etc/apt/sources.list:
  - Debian 11 (Bullseye/Sid): `deb https://vitastor.io/debian bullseye main`
  - Debian 10 (Buster): `deb https://vitastor.io/debian buster main`
- For Debian 10 (Buster) also enable backports repository:
  `deb http://deb.debian.org/debian buster-backports main`
- Install packages: `apt update; apt install vitastor lp-solve etcd linux-image-amd64`

### CentOS

- Add Vitastor package repository:
  - CentOS 7: `yum install https://vitastor.io/rpms/centos/7/vitastor-release-1.0-1.el7.noarch.rpm`
  - CentOS 8: `dnf install https://vitastor.io/rpms/centos/8/vitastor-release-1.0-1.el8.noarch.rpm`
- Enable EPEL: `yum/dnf install epel-release`
- Enable additional CentOS repositories:
  - CentOS 7: `yum install centos-release-scl`
  - CentOS 8: `dnf install centos-release-advanced-virtualization`
- Enable elrepo-kernel:
  - CentOS 7: `yum install https://www.elrepo.org/elrepo-release-7.el7.elrepo.noarch.rpm`
  - CentOS 8: `dnf install https://www.elrepo.org/elrepo-release-8.el8.elrepo.noarch.rpm`
- Install packages: `yum/dnf install vitastor lpsolve etcd kernel-ml qemu-kvm`

### Building from Source

- Install Linux kernel 5.4 or newer, for io_uring support. 5.8 or later is highly recommended because
  there is at least one known io_uring hang with 5.4 and an HP SmartArray controller.
- Install liburing 0.4 or newer and its headers.
- Install lp_solve.
- Install etcd. Attention: you need a fixed version from here: https://github.com/vitalif/etcd/,
  branch release-3.4, because there is a bug in upstream etcd which makes Vitastor OSDs fail to
  move PGs out of "starting" state if you have at least around ~500 PGs or so. The custom build
  will be unnecessary when etcd merges the fix: https://github.com/etcd-io/etcd/pull/12402.
- Install node.js 10 or newer.
- Install gcc and g++ 8.x or newer.
- Clone https://yourcmc.ru/git/vitalif/vitastor/ with submodules.
- Install QEMU 3.0+, get its source, begin to build it, stop the build and copy headers:
   - `<qemu>/include` &rarr; `<vitastor>/qemu/include`
   - Debian:
      * Use qemu packages from the main repository
      * `<qemu>/b/qemu/config-host.h` &rarr; `<vitastor>/qemu/b/qemu/config-host.h`
      * `<qemu>/b/qemu/qapi` &rarr; `<vitastor>/qemu/b/qemu/qapi`
   - CentOS 8:
      * Use qemu packages from the Advanced-Virtualization repository. To enable it, run
        `yum install centos-release-advanced-virtualization.noarch` and then `yum install qemu`
      * `<qemu>/config-host.h` &rarr; `<vitastor>/qemu/b/qemu/config-host.h`
      * For QEMU 3.0+: `<qemu>/qapi` &rarr; `<vitastor>/qemu/b/qemu/qapi`
      * For QEMU 2.0+: `<qemu>/qapi-types.h` &rarr; `<vitastor>/qemu/b/qemu/qapi-types.h`
   - `config-host.h` and `qapi` are required because they contain generated headers
- You can also rebuild QEMU with a patch that makes LD_PRELOAD unnecessary to load vitastor driver.
  See `qemu-*.*-vitastor.patch`.
- Install fio 3.7 or later, get its source and symlink it into `<vitastor>/fio`.
- Build & install Vitastor with `mkdir build && cd build && cmake .. && make -j8 && make install`.
  Pay attention to the `QEMU_PLUGINDIR` cmake option - it must be set to `qemu-kvm` on RHEL.

## Running

Please note that startup procedure isn't currently simple - you specify configuration
and calculate disk offsets almost by hand. This will be fixed in near future.

- Get some SATA or NVMe SSDs with capacitors (server-grade drives). You can use desktop SSDs
  with lazy fsync, but prepare for inferior single-thread latency.
- Get a fast network (at least 10 Gbit/s).
- Disable CPU powersaving: `cpupower idle-set -D 0 && cpupower frequency-set -g performance`.
- Check `/usr/lib/vitastor/mon/make-units.sh` and `/usr/lib/vitastor/mon/make-osd.sh` and
  put desired values into the variables at the top of these files.
- Create systemd units for the monitor and etcd: `/usr/lib/vitastor/mon/make-units.sh`
- Create systemd units for your OSDs: `/usr/lib/vitastor/mon/make-osd.sh /dev/disk/by-partuuid/XXX [/dev/disk/by-partuuid/YYY ...]`
- You can edit the units and change OSD configuration. Notable configuration variables:
  - `disable_data_fsync 1` - only safe with server-grade drives with capacitors.
  - `immediate_commit all` - use this if all your drives are server-grade.
  - `disable_device_lock 1` - only required if you run multiple OSDs on one block device.
  - `flusher_count 256` - flusher is a micro-thread that removes old data from the journal.
    You don't have to worry about this parameter anymore, 256 is enough.
  - `disk_alignment`, `journal_block_size`, `meta_block_size` should be set to the internal
    block size of your SSDs which is 4096 on most drives.
  - `journal_no_same_sector_overwrites true` prevents multiple overwrites of the same journal sector.
    Most (99%) SSDs don't need this option. But Intel D3-4510 does because it doesn't like when you
    overwrite the same sector twice in a short period of time. The setting forces Vitastor to never
    overwrite the same journal sector twice in a row which makes D3-4510 almost happy. Not totally
    happy, because overwrites of the same block can still happen in the metadata area... When this
    setting is set, it is also required to raise `journal_sector_buffer_count` setting, which is the
    number of dirty journal sectors that may be written to at the same time.
- `systemctl start vitastor.target` everywhere.
- Create global configuration in etcd: `etcdctl --endpoints=... put /vitastor/config/global '{"immediate_commit":"all"}'`
  (if all your drives have capacitors).
- Create pool configuration in etcd: `etcdctl --endpoints=... put /vitastor/config/pools '{"1":{"name":"testpool","scheme":"replicated","pg_size":2,"pg_minsize":1,"pg_count":256,"failure_domain":"host"}}'`.
  For jerasure pools the configuration should look like the following: `2:{"name":"ecpool","scheme":"jerasure","pg_size":4,"parity_chunks":2,"pg_minsize":2,"pg_count":256,"failure_domain":"host"}`.
- At this point, one of the monitors will configure PGs and OSDs will start them.
- You can check PG states with `etcdctl --endpoints=... get --prefix /vitastor/pg/state`. All PGs should become 'active'.
- Run tests with (for example): `fio -thread -ioengine=libfio_vitastor.so -name=test -bs=4M -direct=1 -iodepth=16 -rw=write -etcd=10.115.0.10:2379/v3 -pool=1 -inode=1 -size=400G`.
- Upload VM disk image with qemu-img (for example):
  ```
  qemu-img convert -f qcow2 debian10.qcow2 -p -O raw 'vitastor:etcd_host=10.115.0.10\:2379/v3:pool=1:inode=1:size=2147483648'
  ```
  Note that the command requires to be run with `LD_PRELOAD=/usr/lib/x86_64-linux-gnu/qemu/block-vitastor.so qemu-img ...`
  if you use unmodified QEMU.
- Run QEMU with (for example):
  ```
  qemu-system-x86_64 -enable-kvm -m 1024
    -drive 'file=vitastor:etcd_host=10.115.0.10\:2379/v3:pool=1:inode=1:size=2147483648',format=raw,if=none,id=drive-virtio-disk0,cache=none
    -device virtio-blk-pci,scsi=off,bus=pci.0,addr=0x5,drive=drive-virtio-disk0,id=virtio-disk0,bootindex=1,write-cache=off,physical_block_size=4096,logical_block_size=512
    -vnc 0.0.0.0:0
  ```
- Remove inode with (for example):
  ```
  vitastor-rm --etcd_address 10.115.0.10:2379/v3 --pool 1 --inode 1 --parallel_osds 16 --iodepth 32
  ```

## Known Problems

- Object deletion requests may currently lead to 'incomplete' objects if your OSDs crash during
  deletion because proper handling of object cleanup in a cluster should be "three-phase"
  and it's currently not implemented. Just to repeat the removal again in this case.

## Implementation Principles

- I like simple and stupid solutions, so expect Vitastor to stay simple.
- I also like reinventing the wheel to some extent, like writing my own HTTP client
  for etcd interaction instead of using prebuilt libraries, because in this case
  I'm confident about what my code does and what it doesn't do.
- I don't care about C++ "best practices" like RAII or proper inheritance or usage of
  smart pointers or whatever and I don't intend to change my mind, so if you're here
  looking for ideal reference C++ code, this probably isn't the right place.
- I like node.js better than any other dynamically-typed language interpreter
  because it's faster than any other interpreter in the world, has neutral C-like
  syntax and built-in event loop. That's why Monitor is implemented in node.js.

## Author and License

Copyright (c) Vitaliy Filippov (vitalif [at] yourcmc.ru), 2019+

You can also find me in the Russian Telegram Ceph chat: https://t.me/ceph_ru

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
