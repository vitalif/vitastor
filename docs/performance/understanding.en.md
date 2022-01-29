[Documentation](../../README.md#documentation) → Performance → Understanding Storage Performance

-----

[Читать на русском](understanding.ru.md)

# Understanding Storage Performance

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

## fio commands

I use the following 6 commands with small variations to benchmark block storage:

- Linear write (results in MB/s):
  `fio -ioengine=libaio -direct=1 -invalidate=1 -name=test -bs=4M -iodepth=32 -rw=write -runtime=60 -filename=/dev/sdX`
- Linear read (results in MB/s):
  `fio -ioengine=libaio -direct=1 -invalidate=1 -name=test -bs=4M -iodepth=32 -rw=read -runtime=60 -filename=/dev/sdX`
- Random write latency (T1Q1, this hurts storages the most) (results in iops or milliseconds of latency):
  `fio -ioengine=libaio -direct=1 -invalidate=1 -name=test -bs=4k -iodepth=1 -fsync=1 -rw=randwrite -runtime=60 -filename=/dev/sdX`
- Random read latency (T1Q1) (results in iops or milliseconds of latency):
  `fio -ioengine=libaio -direct=1 -invalidate=1 -name=test -bs=4k -iodepth=1 -rw=randread -runtime=60 -filename=/dev/sdX`
- Parallel write iops (use numjobs=4 if a single CPU core is insufficient to saturate the load) (results only in iops):
  `fio -ioengine=libaio -direct=1 -invalidate=1 -name=test -bs=4k -iodepth=128 [-numjobs=4 -group_reporting] -rw=randwrite -runtime=60 -filename=/dev/sdX`
- Parallel read iops (use numjobs if a single CPU core is insufficient to saturate the load) (results only in iops):
  `fio -ioengine=libaio -direct=1 -invalidate=1 -name=test -bs=4k -iodepth=128 [-numjobs=4 -group_reporting] -rw=randread -runtime=60 -filename=/dev/sdX`
