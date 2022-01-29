[Documentation](../../README.md#documentation) → Performance → Example Comparison with Ceph

-----

[Читать на русском](comparison1.ru.md)

# Example Comparison with Ceph

- [Test environment](#test-environment)
- [Raw drive performance](#raw-drive-performance)
- [2 replicas](#2-replicas)
  - [Ceph 15.2.4 (Bluestore)](#ceph-15-2-4-bluestore)
  - [Vitastor 0.4.0 (native)](#vitastor-0-4-0-native)
  - [Vitastor 0.4.0 (NBD)](#vitastor-0-4-0-nbd)
- [EC/XOR 2+1](#ec/xor-2-1)
  - [Ceph 15.2.4](#ceph-15-2-4)
  - [Vitastor 0.4.0](#vitastor-0-4-0)

## Test environment

Hardware configuration: 4 nodes, each with:
- 6x SATA SSD Intel D3-S4510 3.84 TB
- 2x Xeon Gold 6242 (16 cores @ 2.8 GHz)
- 384 GB RAM
- 1x 25 GbE network interface (Mellanox ConnectX-4 LX), connected to a Juniper QFX5200 switch

CPU powersaving was disabled. Both Vitastor and Ceph were configured with 2 OSDs per 1 SSD.

All of the results below apply to 4 KB blocks and random access (unless indicated otherwise).

T8Q64 tests were conducted over 8 400GB RBD images from all hosts (every host was running 2 instances of fio).
This is because Ceph has performance penalties related to running multiple clients over a single RBD image.

cephx_sign_messages was set to false during tests, RocksDB and Bluestore settings were left at defaults.

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

## Raw drive performance

- T1Q1 write ~27000 iops (~0.037ms latency)
- T1Q1 read ~9800 iops (~0.101ms latency)
- T1Q32 write ~60000 iops
- T1Q32 read ~81700 iops

## 2 replicas

### Ceph 15.2.4 (Bluestore)

- T1Q1 write ~1000 iops (~1ms latency)
- T1Q1 read ~1750 iops (~0.57ms latency)
- T8Q64 write ~100000 iops, total CPU usage by OSDs about 40 virtual cores on each node
- T8Q64 read ~480000 iops, total CPU usage by OSDs about 40 virtual cores on each node

In fact, not that bad for Ceph. These servers are an example of well-balanced Ceph nodes.
However, CPU usage and I/O latency were through the roof, as usual.

### Vitastor 0.4.0 (native)

- T1Q1 write: 7087 iops (0.14ms latency)
- T1Q1 read: 6838 iops (0.145ms latency)
- T2Q64 write: 162000 iops, total CPU usage by OSDs about 3 virtual cores on each node
- T8Q64 read: 895000 iops, total CPU usage by OSDs about 4 virtual cores on each node
- Linear write (4M T1Q32): 2800 MB/s
- Linear read (4M T1Q32): 1500 MB/s

### Vitastor 0.4.0 (NBD)

NBD is currently required to mount Vitastor via kernel, but it imposes additional overhead
due to additional copying between the kernel and userspace. This mostly hurts linear
bandwidth, not iops.

Vitastor with single-threaded NBD on the same hardware:
- T1Q1 write: 6000 iops (0.166ms latency)
- T1Q1 read: 5518 iops (0.18ms latency)
- T1Q128 write: 94400 iops
- T1Q128 read: 103000 iops
- Linear write (4M T1Q128): 1266 MB/s (compared to 2800 MB/s via fio)
- Linear read (4M T1Q128): 975 MB/s (compared to 1500 MB/s via fio)

## EC/XOR 2+1

### Ceph 15.2.4

- T1Q1 write: 730 iops (~1.37ms latency)
- T1Q1 read: 1500 iops with cold cache (~0.66ms latency), 2300 iops after 2 minute metadata cache warmup (~0.435ms latency)
- T4Q128 write (4 RBD images): 45300 iops, total CPU usage by OSDs about 30 virtual cores on each node
- T8Q64 read (4 RBD images): 278600 iops, total CPU usage by OSDs about 40 virtual cores on each node
- Linear write (4M T1Q32): 1950 MB/s before preallocation, 2500 MB/s after preallocation
- Linear read (4M T1Q32): 2400 MB/s

### Vitastor 0.4.0

- T1Q1 write: 2808 iops (~0.355ms latency)
- T1Q1 read: 6190 iops (~0.16ms latency)
- T2Q64 write: 85500 iops, total CPU usage by OSDs about 3.4 virtual cores on each node
- T8Q64 read: 812000 iops, total CPU usage by OSDs about 4.7 virtual cores on each node
- Linear write (4M T1Q32): 3200 MB/s
- Linear read (4M T1Q32): 1800 MB/s
