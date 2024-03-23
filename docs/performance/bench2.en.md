[Documentation](../../README.md#documentation) → Performance → Newer benchmark of Vitastor 1.3.1

-----

[Читать на русском](bench2.ru.md)

# Newer benchmark of Vitastor 1.3.1

- [Test environment](#test-environment)
- [Notes](#notes)
- [Raw drive performance](#raw-drive-performance)
- [2 replicas](#2-replicas)
- [3 replicas](#3-replicas)
- [EC 2+1](#ec-2-1)

## Test environment

Hardware configuration: 3 nodes, each with:
- 8x NVMe Samsung PM9A3 1.92 TB
- 2x Xeon Gold 6342 (24 cores @ 2.8 GHz)
- 256 GB RAM
- Dual-port 25 GbE Mellanox ConnectX-4 LX network card with RoCEv2
- Connected to 2 Mellanox SN2010 switches with MLAG

## Notes

Vitastor version was 1.3.1.

Tests were ran from the storage nodes - 4 fio clients per each of 3 nodes.

The same large 3 TB image was tested from all hosts because Vitastor has no
performance penalties related to running multiple clients over a single inode.

CPU power saving was disabled. 4 OSDs were created per each NVMe.
Checksums were not enabled. Tests with checksums will be conducted later,
along with the newer version of Vitastor, and results will be updated.

CPU configuration was not optimal because of NUMA. It's better to avoid 2-socket
platforms. It was especially noticeable in RDMA tests - in the form of ksoftirqd
processes (usually 1 per server) eating 100 % of one CPU core and actual bandwidth
of one network port reduced to 3-5 Gbit/s instead of 25 Gbit/s - probably because
of RFS (Receive Flow Steering) misses. Many network configurations were tried during
tests, but nothing helped to solve this problem, so final tests were conducted with
the default settings.

# Raw drive performance

- Linear write ~1000-2000 MB/s, depending on current state of the drive's garbage collector
- Linear read ~3300 MB/s
- T1Q1 random write ~60000 iops (latency ~0.015ms)
- T1Q1 random read ~14700 iops (latency ~0.066ms)
- T1Q16 random write ~180000 iops
- T1Q16 random read ~120000 iops
- T1Q32 random write ~180000 iops
- T1Q32 random read ~195000 iops
- T1Q128 random write ~180000 iops
- T1Q128 random read ~195000 iops
- T4Q128 random write ~525000 iops
- T4Q128 random read ~750000 iops

These numbers make obvious that results could be much better if a faster network
was available, because NVMe drives obviously weren't a bottleneck. For example,
theoretical maximum linear read performance for 24 drives could be 79.2 GByte/s,
which is 633 Gbit/s. Real Vitastor read speed (both linear and random) was around
16 Gbyte/s, which is 130 Gbit/s. It's important to note that it was still much
larger than the network bandwidth of one server (50 Gbit/s). This is also correct
because tests were conducted from all 3 nodes.

## 2 replicas

|                              | TCP          | RDMA         |
|------------------------------|--------------|--------------|
| Linear read (4M T6 Q16)      | 13.13 GB/s   | 16.25 GB/s   |
| Linear write (4M T6 Q16)     | 8.16 GB/s    | 7.88 GB/s    |
| Read 4k T1 Q1                | 8745 iops    | 10252 iops   |
| Write 4k T1 Q1               | 8097 iops    | 11488 iops   |
| Read 4k T12 Q128             | 1305936 iops | 4265861 iops |
| Write 4k T12 Q128            | 660490 iops  | 1384033 iops |

CPU consumption OSD per 1 disk:

|                              | TCP     | RDMA    |
|------------------------------|---------|---------|
| Linear read (4M T6 Q16)      | 29.7 %  | 29.8 %  |
| Linear write (4M T6 Q16)     | 84.4 %  | 33.2 %  |
| Read 4k T12 Q128             | 98.4 %  | 119.1 % |
| Write 4k T12 Q128            | 173.4 % | 175.9 % |

CPU consumption per 1 client (fio):

|                              | TCP    | RDMA   |
|------------------------------|--------|--------|
| Linear read (4M T6 Q16)      | 100 %  | 85.2 % |
| Linear write (4M T6 Q16)     | 55.8 % | 48.8 % |
| Read 4k T12 Q128             | 99.9 % | 96 %   |
| Write 4k T12 Q128            | 71.6 % | 48.5 % |

## 3 replicas

|                              | TCP          | RDMA         |
|------------------------------|--------------|--------------|
| Linear read (4M T6 Q16)      | 13.98 GB/s   | 16.54 GB/s   |
| Linear write (4M T6 Q16)     | 5.38 GB/s    | 5.7 GB/s     |
| Read 4k T1 Q1                | 8969 iops    | 9980 iops    |
| Write 4k T1 Q1               | 8126 iops    | 11672 iops   |
| Read 4k T12 Q128             | 1358818 iops | 4279088 iops |
| Write 4k T12 Q128            | 433890 iops  | 993506 iops  |

CPU consumption OSD per 1 disk:

|                              | TCP    | RDMA    |
|------------------------------|--------|---------|
| Linear read (4M T6 Q16)      | 24.9 % | 25.4 %  |
| Linear write (4M T6 Q16)     | 99.3 % | 38.4 %  |
| Read 4k T12 Q128             | 95.3 % | 111.7 % |
| Write 4k T12 Q128            | 173 %  | 194 %   |

CPU consumption per 1 client (fio):

|                              | TCP    | RDMA   |
|------------------------------|--------|--------|
| Linear read (4M T6 Q16)      | 99.9 % | 85.8 % |
| Linear write (4M T6 Q16)     | 38.9 % | 38.1 % |
| Read 4k T12 Q128             | 100 %  | 96.1 % |
| Write 4k T12 Q128            | 51.6 % | 41.9 % |

## EC 2+1

|                              | TCP          | RDMA         |
|------------------------------|--------------|--------------|
| Linear read (4M T6 Q16)      | 10.07 GB/s   | 11.43 GB/s   |
| Linear write (4M T6 Q16)     | 7.74 GB/s    | 8.32 GB/s    |
| Read 4k T1 Q1                | 7408 iops    | 8891 iops    |
| Write 4k T1 Q1               | 3525 iops    | 4903 iops    |
| Read 4k T12 Q128             | 1216496 iops | 2552765 iops |
| Write 4k T12 Q128            | 278110 iops  | 821261 iops  |

CPU consumption OSD per 1 disk:

|                              | TCP     | RDMA    |
|------------------------------|---------|---------|
| Linear read (4M T6 Q16)      | 68.6 %  | 33.6 %  |
| Linear write (4M T6 Q16)     | 108.3 % | 50.2 %  |
| Read 4k T12 Q128             | 138.1 % | 97.9 %  |
| Write 4k T12 Q128            | 168.7 % | 188.5 % |

CPU consumption per 1 client (fio):

|                              | TCP    | RDMA   |
|------------------------------|--------|--------|
| Linear read (4M T6 Q16)      | 88.2 % | 52.4 % |
| Linear write (4M T6 Q16)     | 51.8 % | 46.8 % |
| Read 4k T12 Q128             | 99.7 % | 61.3 % |
| Write 4k T12 Q128            | 35.1 % | 31.3 % |
