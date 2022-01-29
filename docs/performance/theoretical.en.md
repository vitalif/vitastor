[Documentation](../../README.md#documentation) → Performance → Vitastor's Theoretical Maximum Performance

-----

[Читать на русском](theoretical.ru.md)

# Vitastor's Theoretical Maximum Performance

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

## In Practice

In practice, using tests from [Understanding Performance](understanding.en.md)
and good server-grade SSD/NVMe drives, you should head for:
- At least 5000 T1Q1 replicated read and write iops (maximum 0.2ms latency)
- At least ~80k parallel read iops or ~30k write iops per 1 core (1 OSD)
- Disk-speed or wire-speed linear reads and writes, whichever is the bottleneck in your case

Lower results may mean that you have bad drives, bad network or some kind of misconfiguration.
