[Documentation](../../README.md#documentation) → Performance → Vitastor's Theoretical Maximum Performance

-----

[Читать на русском](theoretical.ru.md)

# Vitastor's Theoretical Maximum Performance

Replicated setups:
- Single-threaded (T1Q1) read latency: 1 network roundtrip + 1 disk read.
- Single-threaded write+fsync latency:
  - With immediate commit: 2 network roundtrips + 1 disk write.
  - With lazy commit: 4 network roundtrips + 1 disk write + 1 disk flush.
- Linear read: `min(total network bandwidth, sum(disk read MB/s))`.
- Linear write: `min(total network bandwidth, sum(disk write MB/s / number of replicas))`.
- Saturated parallel read iops: `min(total network bandwidth, sum(disk read iops))`.
- Saturated parallel write iops: `min(total network bandwidth / number of replicas, sum(disk write iops / number of replicas / (write amplification = 4)))`.

EC/XOR setups (EC N+K):
- Single-threaded (T1Q1) read latency: 1.5 network roundtrips + 1 disk read.
- Single-threaded write+fsync latency:
  - With immediate commit: 3.5 network roundtrips + 1 disk read + 2 disk writes.
  - With lazy commit: 5.5 network roundtrips + 1 disk read + 2 disk writes + 2 disk fsyncs.
  - 0.5 in actually `(N-1)/N` which means that an additional roundtrip doesn't happen when
    the read sub-operation can be served locally.
- Linear read: `min(total network bandwidth, sum(disk read MB/s))`.
- Linear write: `min(total network bandwidth, sum(disk write MB/s * N/(N+K)))`.
- Saturated parallel read iops: `min(total network bandwidth, sum(disk read iops))`.
- Saturated parallel write iops: roughly `total iops / (N+K) / WA`. More exactly,
  `min(total network bandwidth * N/(N+K), sum(disk randrw iops / (N*4 + K*5 + 1)))` with
  random read/write mix corresponding to `(N-1)/(N*4 + K*5 + 1)*100 % reads`.
  - For example, with EC 2+1 it is: `(7% randrw iops) / 14`.
  - With EC 6+3 it is: `(12.5% randrw iops) / 40`.

Write amplification for 4 KB blocks is usually 3-5 in Vitastor:
1. Journal block write
2. Journal data write
3. Metadata block write
4. Another journal block write for EC/XOR setups
5. Data block write

If you manage to get an SSD which handles 512 byte blocks well (Optane?) you may
lower 1, 3 and 4 to 512 bytes (1/8 of data size) and get WA as low as 2.375.

Implemented NVDIMM support can basically eliminate WA at all - all extra writes will
go to DRAM memory. But this requires a test cluster with NVDIMM - please contact me
if you want to provide me with such cluster for tests.

Lazy fsync also reduces WA for parallel workloads because journal blocks are only
written when they fill up or fsync is requested.

## In Practice

In practice, using tests from [Understanding Performance](understanding.en.md), decent TCP network,
good server-grade SSD/NVMe drives and disabled CPU power saving, you should head for:
- At least 5000 T1Q1 replicated read and write iops (maximum 0.2ms latency)
- At least 5000 T1Q1 EC read IOPS and at least 2200 EC write IOPS (maximum 0.45ms latency)
- At least ~80k parallel read iops or ~30k write iops per 1 core (1 OSD)
- Disk-speed or wire-speed linear reads and writes, whichever is the bottleneck in your case

Lower results may mean that you have bad drives, bad network or some kind of misconfiguration.

Current latency records:
- 9668 T1Q1 replicated write iops (0.103 ms latency) with TCP and NVMe
- 9143 T1Q1 replicated read iops (0.109 ms latency) with TCP and NVMe
