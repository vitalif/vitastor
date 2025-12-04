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
- Saturated parallel write iops: `min(total network bandwidth / number of replicas, sum(disk write iops / number of replicas / write amplification))`.

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
- Saturated parallel write iops: roughly `total iops / (N+K) / WA`. More exactly:
  - With the new store: `min(total network bandwidth * N/(N+K), sum(disk randrw iops / (2 + N-1 + K*2)))`,
    with random read/write mix corresponding to `(N-1)/(2 + N-1 + K*2)*100 % reads`.
    - For example, with EC 2+1 it is: `(20% randrw iops) / 5`.
    - With EC 6+3 it is: `(38% randrw iops) / 13`.
  - With the old store: `min(total network bandwidth * N/(N+K), sum(disk randrw iops / (3 + N-1 + K*3)))`,
    with random read/write mix corresponding to `(N-1)/(3 + N-1 + K*3)*100 % reads`.
    - For example, with EC 2+1 it is: `(14% randrw iops) / 7`.
    - With EC 6+3 it is: `(30% randrw iops) / 17`.

Write Amplification factor:
- For the new store and for 4 KB writes: WA is always 1 unless you set [atomic_write_size](../config/osd.en.md#atomic_write_size) to 0 manually.
- For the new store and for 8-124 KB writes: WA is 1 if you use NVMe drives with atomic write support, or roughly 2 if you use other drives.
- For the old store, WA is roughly `(2 * write size + 4 KB) / (write size)`. So, for 4 KB writes it's 3, and for 8-124 KB writes it's closer to 2.
- For both the new and the old store and for writes of [block_size](../config/layout-cluster.en.md#block_size): WA is almost 1.

Write Amplification consists of:
- For the new store:
  - Buffer block write if non-atomic
  - Data block write
  - Metadata write(s) (amortized)
- For the old store:
  - Journal block write (amortized)
  - Journal data write
  - Metadata block write
  - Another journal block write for EC/XOR setups (amortized)
  - Data block write

Other possibilities to reduce WA would be to use SSDs with internal 512-byte blocks
or NVDIMM, but both options seem unavailable on the market at the moment.

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
