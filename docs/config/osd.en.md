[Documentation](../../README.md#documentation) → [Configuration](../config.en.md) → Runtime OSD Parameters

-----

[Читать на русском](osd.ru.md)

# Runtime OSD Parameters

These parameters only apply to OSDs, are not fixed at the moment of OSD drive
initialization and can be changed - in /etc/vitastor/vitastor.conf or [vitastor-disk update-sb](../usage/disk.en.md#update-sb)
with an OSD restart or, for some of them, even without restarting by updating configuration in etcd.

- [bind_address](#bind_address)
- [bind_port](#bind_port)
- [osd_iothread_count](#osd_iothread_count)
- [etcd_report_interval](#etcd_report_interval)
- [etcd_stats_interval](#etcd_stats_interval)
- [run_primary](#run_primary)
- [autosync_interval](#autosync_interval)
- [autosync_writes](#autosync_writes)
- [recovery_queue_depth](#recovery_queue_depth)
- [recovery_sleep_us](#recovery_sleep_us)
- [recovery_pg_switch](#recovery_pg_switch)
- [recovery_sync_batch](#recovery_sync_batch)
- [readonly](#readonly)
- [no_recovery](#no_recovery)
- [no_rebalance](#no_rebalance)
- [print_stats_interval](#print_stats_interval)
- [slow_log_interval](#slow_log_interval)
- [inode_vanish_time](#inode_vanish_time)
- [max_write_iodepth](#max_write_iodepth)
- [min_flusher_count](#min_flusher_count)
- [max_flusher_count](#max_flusher_count)
- [inmemory_metadata](#inmemory_metadata)
- [inmemory_journal](#inmemory_journal)
- [data_io](#data_io)
- [meta_io](#meta_io)
- [journal_io](#journal_io)
- [journal_sector_buffer_count](#journal_sector_buffer_count)
- [journal_no_same_sector_overwrites](#journal_no_same_sector_overwrites)
- [throttle_small_writes](#throttle_small_writes)
- [throttle_target_iops](#throttle_target_iops)
- [throttle_target_mbs](#throttle_target_mbs)
- [throttle_target_parallelism](#throttle_target_parallelism)
- [throttle_threshold_us](#throttle_threshold_us)
- [osd_memlock](#osd_memlock)
- [auto_scrub](#auto_scrub)
- [no_scrub](#no_scrub)
- [scrub_interval](#scrub_interval)
- [scrub_queue_depth](#scrub_queue_depth)
- [scrub_sleep](#scrub_sleep)
- [scrub_list_limit](#scrub_list_limit)
- [scrub_find_best](#scrub_find_best)
- [scrub_ec_max_bruteforce](#scrub_ec_max_bruteforce)
- [recovery_tune_interval](#recovery_tune_interval)
- [recovery_tune_util_low](#recovery_tune_util_low)
- [recovery_tune_util_high](#recovery_tune_util_high)
- [recovery_tune_client_util_low](#recovery_tune_client_util_low)
- [recovery_tune_client_util_high](#recovery_tune_client_util_high)
- [recovery_tune_agg_interval](#recovery_tune_agg_interval)
- [recovery_tune_sleep_min_us](#recovery_tune_sleep_min_us)
- [recovery_tune_sleep_cutoff_us](#recovery_tune_sleep_cutoff_us)
- [discard_on_start](#discard_on_start)
- [min_discard_size](#min_discard_size)
- [allow_net_split](#allow_net_split)

## bind_address

- Type: string or array of strings

Instead of the network masks ([osd_network](network.en.md#osd_network) and
[osd_cluster_network](network.en.md#osd_cluster_network)), you can also set
OSD listen addresses explicitly using this parameter. May be useful if you
want to start OSDs on interfaces that are not UP + RUNNING.

## bind_port

- Type: integer

By default, OSDs pick random ports to use for incoming connections
automatically. With this option you can set a specific port for a specific
OSD by hand.

## osd_iothread_count

- Type: integer
- Default: 0

TCP network I/O thread count for OSD. When non-zero, a single OSD process
may handle more TCP I/O, but at a cost of increased latency because thread
switching overhead occurs. RDMA isn't affected by this option.

Because of latency, instead of enabling OSD I/O threads it's recommended to
just create multiple OSDs per disk, or use RDMA.

## etcd_report_interval

- Type: seconds
- Default: 5

Interval at which OSDs report their liveness to etcd. Affects OSD lease time
and thus the failover speed. Lease time is equal to this parameter value
plus max_etcd_attempts * etcd_quick_timeout because it should be guaranteed
that every OSD always refreshes its lease in time.

## etcd_stats_interval

- Type: seconds
- Default: 30

Interval at which OSDs report their statistics to etcd. Highly affects the
imposed load on etcd, because statistics include a key for every OSD and
for every PG. At the same time, low statistic intervals make `vitastor-cli`
statistics more responsive.

## run_primary

- Type: boolean
- Default: true

Start primary OSD logic on this OSD. As of now, can be turned off only for
debugging purposes. It's possible to implement additional feature for the
monitor which may allow to separate primary and secondary OSDs, but it's
unclear why anyone could need it, so it's not implemented.

## autosync_interval

- Type: seconds
- Default: 5
- Can be changed online: yes

Time interval at which automatic fsyncs/flushes are issued by each OSD when
the immediate_commit mode if disabled. fsyncs are required because without
them OSDs quickly fill their journals, become unable to clear them and
stall. Also this option limits the amount of recent uncommitted changes
which OSDs may lose in case of a power outage in case when clients don't
issue fsyncs at all.

## autosync_writes

- Type: integer
- Default: 128
- Can be changed online: yes

Same as autosync_interval, but sets the maximum number of uncommitted write
operations before issuing an fsync operation internally.

## recovery_queue_depth

- Type: integer
- Default: 1
- Can be changed online: yes

Maximum recovery and rebalance operations initiated by each OSD in parallel.
Note that each OSD talks to a lot of other OSDs so actual number of parallel
recovery operations per each OSD is greater than just recovery_queue_depth.
Increasing this parameter can speedup recovery if [auto-tuning](#recovery_tune_interval)
allows it or if it is disabled.

## recovery_sleep_us

- Type: microseconds
- Default: 0
- Can be changed online: yes

Delay for all recovery- and rebalance- related operations. If non-zero,
such operations are artificially slowed down to reduce the impact on
client I/O.

## recovery_pg_switch

- Type: integer
- Default: 128
- Can be changed online: yes

Number of recovery operations before switching to recovery of the next PG.
The idea is to mix all PGs during recovery for more even space and load
distribution but still benefit from recovery queue depth greater than 1.
Degraded PGs are anyway scanned first.

## recovery_sync_batch

- Type: integer
- Default: 16
- Can be changed online: yes

Maximum number of recovery operations before issuing an additional fsync.

## readonly

- Type: boolean
- Default: false

Read-only mode. If this is enabled, an OSD will never issue any writes to
the underlying device. This may be useful for recovery purposes.

## no_recovery

- Type: boolean
- Default: false
- Can be changed online: yes

Disable automatic background recovery of objects. Note that it doesn't
affect implicit recovery of objects happening during writes - a write is
always made to a full set of at least pg_minsize OSDs.

## no_rebalance

- Type: boolean
- Default: false
- Can be changed online: yes

Disable background movement of data between different OSDs. Disabling it
means that PGs in the `has_misplaced` state will be left in it indefinitely.

## print_stats_interval

- Type: seconds
- Default: 3
- Can be changed online: yes

Time interval at which OSDs print simple human-readable operation
statistics on stdout.

## slow_log_interval

- Type: seconds
- Default: 10
- Can be changed online: yes

Time interval at which OSDs dump slow or stuck operations on stdout, if
they're any. Also it's the time after which an operation is considered
"slow".

## inode_vanish_time

- Type: seconds
- Default: 60
- Can be changed online: yes

Number of seconds after which a deleted inode is removed from OSD statistics.

## max_write_iodepth

- Type: integer
- Default: 128
- Can be changed online: yes

Parallel client write operation limit per one OSD. Operations that exceed
this limit are pushed to a temporary queue instead of being executed
immediately.

## min_flusher_count

- Type: integer
- Default: 1
- Can be changed online: yes

Flusher is a micro-thread that moves data from the journal to the data
area of the device. Their number is auto-tuned between minimum and maximum.
Minimum number is set by this parameter.

## max_flusher_count

- Type: integer
- Default: 256
- Can be changed online: yes

Maximum number of journal flushers (see above min_flusher_count).

## inmemory_metadata

- Type: boolean
- Default: true

This parameter makes Vitastor always keep metadata area of the block device
in memory. It's required for good performance because it allows to avoid
additional read-modify-write cycles during metadata modifications. Metadata
area size is currently roughly 224 MB per 1 TB of data. You can turn it off
to reduce memory usage by this value, but it will hurt performance. This
restriction is likely to be removed in the future along with the upgrade
of the metadata storage scheme.

## inmemory_journal

- Type: boolean
- Default: true

This parameter make Vitastor always keep journal area of the block
device in memory. Turning it off will, again, reduce memory usage, but
hurt performance because flusher coroutines will have to read data from
the disk back before copying it into the main area. The memory usage benefit
is typically very small because it's sufficient to have 16-32 MB journal
for SSD OSDs. However, in theory it's possible that you'll want to turn it
off for hybrid (HDD+SSD) OSDs with large journals on quick devices.

## data_io

- Type: string
- Default: direct

I/O mode for *data*. One of "direct", "cached" or "directsync". Corresponds
to O_DIRECT, O_SYNC and O_DIRECT|O_SYNC, respectively.

Choose "cached" to use Linux page cache. This may improve read performance
for hot data and slower disks - HDDs and maybe SATA SSDs - but will slightly
decrease write performance for fast disks because page cache is an overhead
itself.

Choose "directsync" to use [immediate_commit](layout-cluster.en.md#immediate_commit)
(which requires disable_data_fsync) with drives having write-back cache
which can't be turned off, for example, Intel Optane. Also note that *some*
desktop SSDs (for example, HP EX950) may ignore O_SYNC thus making
disable_data_fsync unsafe even with "directsync".

## meta_io

- Type: string
- Default: direct

I/O mode for *metadata*. One of "direct", "cached" or "directsync".

"cached" may improve read performance, but only under the following conditions:
1. your drives are relatively slow (HDD, SATA SSD), and
2. checksums are enabled, and
3. [inmemory_metadata](#inmemory_metadata) is disabled.
Under all these conditions, metadata blocks are read from disk on every
read request to verify checksums and caching them may reduce this extra
read load. Without (3) metadata is never read from the disk after starting,
and without (2) metadata blocks are read from disk only during journal
flushing.

"directsync" is the same as above.

If the same device is used for data and metadata, meta_io by default is set
to the same value as [data_io](#data_io).

## journal_io

- Type: string
- Default: direct

I/O mode for *journal*. One of "direct", "cached" or "directsync".

Here, "cached" may only improve read performance for recent writes and
only if [inmemory_journal](#inmemory_journal) is turned off.

If the same device is used for metadata and journal, journal_io by default
is set to the same value as [meta_io](#meta_io).

## journal_sector_buffer_count

- Type: integer
- Default: 32

Maximum number of buffers that can be used for writing journal metadata
blocks. The only situation when you should increase it to a larger value
is when you enable journal_no_same_sector_overwrites. In this case set
it to, for example, 1024.

## journal_no_same_sector_overwrites

- Type: boolean
- Default: false

Enable this option for SSDs like Intel D3-S4510 and D3-S4610 which REALLY
don't like when a program overwrites the same sector multiple times in a
row and slow down significantly (from 25000+ iops to ~3000 iops). When
this option is set, Vitastor will always move to the next sector of the
journal after writing it instead of possibly overwriting it the second time.

Most (99%) other SSDs don't need this option.

## throttle_small_writes

- Type: boolean
- Default: false
- Can be changed online: yes

Enable soft throttling of small journaled writes. Useful for hybrid OSDs
with fast journal/metadata devices and slow data devices. The idea is that
small writes complete very quickly because they're first written to the
journal device, but moving them to the main device is slow. So if an OSD
allows clients to issue a lot of small writes it will perform very good
for several seconds and then the journal will fill up and the performance
will drop to almost zero. Throttling is meant to prevent this problem by
artifically slowing quick writes down based on the amount of free space in
the journal. When throttling is used, the performance of small writes will
decrease smoothly instead of abrupt drop at the moment when the journal
fills up.

## throttle_target_iops

- Type: integer
- Default: 100
- Can be changed online: yes

Target maximum number of throttled operations per second under the condition
of full journal. Set it to approximate random write iops of your data devices
(HDDs).

## throttle_target_mbs

- Type: integer
- Default: 100
- Can be changed online: yes

Target maximum bandwidth in MB/s of throttled operations per second under
the condition of full journal. Set it to approximate linear write
performance of your data devices (HDDs).

## throttle_target_parallelism

- Type: integer
- Default: 1
- Can be changed online: yes

Target maximum parallelism of throttled operations under the condition of
full journal. Set it to approximate internal parallelism of your data
devices (1 for HDDs, 4-8 for SSDs).

## throttle_threshold_us

- Type: microseconds
- Default: 50
- Can be changed online: yes

Minimal computed delay to be applied to throttled operations. Usually
doesn't need to be changed.

## osd_memlock

- Type: boolean
- Default: false

Lock all OSD memory to prevent it from being unloaded into swap with
mlockall(). Requires sufficient ulimit -l (max locked memory).

## auto_scrub

- Type: boolean
- Default: false
- Can be changed online: yes

Data scrubbing is the process of background verification of copies to find
and repair corrupted blocks. It's not run automatically by default since
it's a new feature. Set this parameter to true to enable automatic scrubs.

This parameter makes OSDs automatically schedule data scrubbing of clean PGs
every `scrub_interval` (see below). You can also start/schedule scrubbing
manually by setting `next_scrub` JSON key to the desired UNIX time of the
next scrub in `/pg/history/...` values in etcd.

## no_scrub

- Type: boolean
- Default: false
- Can be changed online: yes

Temporarily disable scrubbing and stop running scrubs.

## scrub_interval

- Type: string
- Default: 30d
- Can be changed online: yes

Default automatic scrubbing interval for all pools. Numbers without suffix
are treated as seconds, possible unit suffixes include 's' (seconds),
'm' (minutes), 'h' (hours), 'd' (days), 'M' (months) and 'y' (years).

## scrub_queue_depth

- Type: integer
- Default: 1
- Can be changed online: yes

Number of parallel scrubbing operations per one OSD.

## scrub_sleep

- Type: milliseconds
- Default: 0
- Can be changed online: yes

Additional interval between two consecutive scrubbing operations on one OSD.
Can be used to slow down scrubbing if it affects user load too much.

## scrub_list_limit

- Type: integer
- Default: 1000
- Can be changed online: yes

Number of objects to list in one listing operation during scrub.

## scrub_find_best

- Type: boolean
- Default: true
- Can be changed online: yes

Find and automatically restore best versions of objects with unmatched
copies. In replicated setups, the best version is the version with most
matching replicas. In EC setups, the best version is the subset of data
and parity chunks without mismatches.

The hypothetical situation where you might want to disable it is when
you have 3 replicas and you are paranoid that 2 HDDs out of 3 may silently
corrupt an object in the same way (for example, zero it out) and only
1 HDD will remain good. In this case disabling scrub_find_best may help
you to recover the data! See also scrub_ec_max_bruteforce below.

## scrub_ec_max_bruteforce

- Type: integer
- Default: 100
- Can be changed online: yes

Vitastor can locate corrupted chunks in EC setups with more than 1 parity
chunk by brute-forcing all possible error locations. This configuration
value limits the maximum number of checked combinations. You can try to
increase it if you have EC N+K setup with N and K large enough for
combination count `C(N+K-1, K-1) = (N+K-1)! / (K-1)! / N!` to be greater
than the default 100.

If there are too many possible combinations or if multiple combinations give
correct results then objects are marked inconsistent and aren't recovered
automatically.

In replicated setups bruteforcing isn't needed, Vitastor just assumes that
the variant with most available equal copies is correct. For example, if
you have 3 replicas and 1 of them differs, this one is considered to be
corrupted. But if there is no "best" version with more copies than all
others have then the object is also marked as inconsistent.

## recovery_tune_interval

- Type: seconds
- Default: 1
- Can be changed online: yes

Interval at which OSD re-considers client and recovery load and automatically
adjusts [recovery_sleep_us](#recovery_sleep_us). Recovery auto-tuning is
disabled if recovery_tune_interval is set to 0.

Auto-tuning targets utilization. Utilization is a measure of load and is
equal to the product of iops and average latency (so it may be greater
than 1). You set "low" and "high" client utilization thresholds and two
corresponding target recovery utilization levels. OSD calculates desired
recovery utilization from client utilization using linear interpolation
and auto-tunes recovery operation delay to make actual recovery utilization
match desired.

This allows to reduce recovery/rebalance impact on client operations. It is
of course impossible to remove it completely, but it should become adequate.
In some tests rebalance could earlier drop client write speed from 1.5 GB/s
to 50-100 MB/s, with default auto-tuning settings it now only reduces
to ~1 GB/s.

## recovery_tune_util_low

- Type: number
- Default: 0.1
- Can be changed online: yes

Desired recovery/rebalance utilization when client load is high, i.e. when
it is at or above recovery_tune_client_util_high.

## recovery_tune_util_high

- Type: number
- Default: 1
- Can be changed online: yes

Desired recovery/rebalance utilization when client load is low, i.e. when
it is at or below recovery_tune_client_util_low.

## recovery_tune_client_util_low

- Type: number
- Default: 0
- Can be changed online: yes

Client utilization considered "low".

## recovery_tune_client_util_high

- Type: number
- Default: 0.5
- Can be changed online: yes

Client utilization considered "high".

## recovery_tune_agg_interval

- Type: integer
- Default: 10
- Can be changed online: yes

The number of last auto-tuning iterations to use for calculating the
delay as average. Lower values result in quicker response to client
load change, higher values result in more stable delay. Default value of 10
is usually fine.

## recovery_tune_sleep_min_us

- Type: microseconds
- Default: 10
- Can be changed online: yes

Minimum possible value for auto-tuned recovery_sleep_us. Lower values
are changed to 0.

## recovery_tune_sleep_cutoff_us

- Type: microseconds
- Default: 10000000
- Can be changed online: yes

Maximum possible value for auto-tuned recovery_sleep_us. Higher values
are treated as outliers and ignored in aggregation.

## discard_on_start

- Type: boolean

Discard (SSD TRIM) unused data device blocks on every OSD startup.

## min_discard_size

- Type: integer
- Default: 1048576

Minimum consecutive block size to TRIM it.

## allow_net_split

- Type: boolean
- Default: false

Allow "safe" cases of network splits/partitions - allow to start PGs without
connections to some OSDs currently registered as alive in etcd, if the number
of actually connected PG OSDs is at least pg_minsize. That is, allow some OSDs to lose
connectivity with some other OSDs as long as it doesn't break pg_minsize guarantees.
The downside is that it increases the probability of writing data into just pg_minsize
OSDs during failover which can lead to PGs becoming incomplete after additional outages.

The old behaviour in versions up to 2.0.0 was equal to enabled allow_net_split.
