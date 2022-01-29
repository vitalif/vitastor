[Documentation](../../README.md#documentation) → [Configuration](../config.en.md) → Runtime OSD Parameters

-----

[Читать на русском](osd.ru.md)

# Runtime OSD Parameters

These parameters only apply to OSDs, are not fixed at the moment of OSD drive
initialization and can be changed with an OSD restart.

- [etcd_report_interval](#etcd_report_interval)
- [run_primary](#run_primary)
- [osd_network](#osd_network)
- [bind_address](#bind_address)
- [bind_port](#bind_port)
- [autosync_interval](#autosync_interval)
- [autosync_writes](#autosync_writes)
- [recovery_queue_depth](#recovery_queue_depth)
- [recovery_sync_batch](#recovery_sync_batch)
- [readonly](#readonly)
- [no_recovery](#no_recovery)
- [no_rebalance](#no_rebalance)
- [print_stats_interval](#print_stats_interval)
- [slow_log_interval](#slow_log_interval)
- [max_write_iodepth](#max_write_iodepth)
- [min_flusher_count](#min_flusher_count)
- [max_flusher_count](#max_flusher_count)
- [inmemory_metadata](#inmemory_metadata)
- [inmemory_journal](#inmemory_journal)
- [journal_sector_buffer_count](#journal_sector_buffer_count)
- [journal_no_same_sector_overwrites](#journal_no_same_sector_overwrites)
- [throttle_small_writes](#throttle_small_writes)
- [throttle_target_iops](#throttle_target_iops)
- [throttle_target_mbs](#throttle_target_mbs)
- [throttle_target_parallelism](#throttle_target_parallelism)
- [throttle_threshold_us](#throttle_threshold_us)
- [osd_memlock](#osd_memlock)

## etcd_report_interval

- Type: seconds
- Default: 5

Interval at which OSDs report their state to etcd. Affects OSD lease time
and thus the failover speed. Lease time is equal to this parameter value
plus max_etcd_attempts * etcd_quick_timeout because it should be guaranteed
that every OSD always refreshes its lease in time.

## run_primary

- Type: boolean
- Default: true

Start primary OSD logic on this OSD. As of now, can be turned off only for
debugging purposes. It's possible to implement additional feature for the
monitor which may allow to separate primary and secondary OSDs, but it's
unclear why anyone could need it, so it's not implemented.

## osd_network

- Type: string or array of strings

Network mask of the network (IPv4 or IPv6) to use for OSDs. Note that
although it's possible to specify multiple networks here, this does not
mean that OSDs will create multiple listening sockets - they'll only
pick the first matching address of an UP + RUNNING interface. Separate
networks for cluster and client connections are also not implemented, but
they are mostly useless anyway, so it's not a big deal.

## bind_address

- Type: string
- Default: 0.0.0.0

Instead of the network mask, you can also set OSD listen address explicitly
using this parameter. May be useful if you want to start OSDs on interfaces
that are not UP + RUNNING.

## bind_port

- Type: integer

By default, OSDs pick random ports to use for incoming connections
automatically. With this option you can set a specific port for a specific
OSD by hand.

## autosync_interval

- Type: seconds
- Default: 5

Time interval at which automatic fsyncs/flushes are issued by each OSD when
the immediate_commit mode if disabled. fsyncs are required because without
them OSDs quickly fill their journals, become unable to clear them and
stall. Also this option limits the amount of recent uncommitted changes
which OSDs may lose in case of a power outage in case when clients don't
issue fsyncs at all.

## autosync_writes

- Type: integer
- Default: 128

Same as autosync_interval, but sets the maximum number of uncommitted write
operations before issuing an fsync operation internally.

## recovery_queue_depth

- Type: integer
- Default: 4

Maximum recovery operations per one primary OSD at any given moment of time.
Currently it's the only parameter available to tune the speed or recovery
and rebalancing, but it's planned to implement more.

## recovery_sync_batch

- Type: integer
- Default: 16

Maximum number of recovery operations before issuing an additional fsync.

## readonly

- Type: boolean
- Default: false

Read-only mode. If this is enabled, an OSD will never issue any writes to
the underlying device. This may be useful for recovery purposes.

## no_recovery

- Type: boolean
- Default: false

Disable automatic background recovery of objects. Note that it doesn't
affect implicit recovery of objects happening during writes - a write is
always made to a full set of at least pg_minsize OSDs.

## no_rebalance

- Type: boolean
- Default: false

Disable background movement of data between different OSDs. Disabling it
means that PGs in the `has_misplaced` state will be left in it indefinitely.

## print_stats_interval

- Type: seconds
- Default: 3

Time interval at which OSDs print simple human-readable operation
statistics on stdout.

## slow_log_interval

- Type: seconds
- Default: 10

Time interval at which OSDs dump slow or stuck operations on stdout, if
they're any. Also it's the time after which an operation is considered
"slow".

## max_write_iodepth

- Type: integer
- Default: 128

Parallel client write operation limit per one OSD. Operations that exceed
this limit are pushed to a temporary queue instead of being executed
immediately.

## min_flusher_count

- Type: integer
- Default: 1

Flusher is a micro-thread that moves data from the journal to the data
area of the device. Their number is auto-tuned between minimum and maximum.
Minimum number is set by this parameter.

## max_flusher_count

- Type: integer
- Default: 256

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

Target maximum number of throttled operations per second under the condition
of full journal. Set it to approximate random write iops of your data devices
(HDDs).

## throttle_target_mbs

- Type: integer
- Default: 100

Target maximum bandwidth in MB/s of throttled operations per second under
the condition of full journal. Set it to approximate linear write
performance of your data devices (HDDs).

## throttle_target_parallelism

- Type: integer
- Default: 1

Target maximum parallelism of throttled operations under the condition of
full journal. Set it to approximate internal parallelism of your data
devices (1 for HDDs, 4-8 for SSDs).

## throttle_threshold_us

- Type: microseconds
- Default: 50

Minimal computed delay to be applied to throttled operations. Usually
doesn't need to be changed.

## osd_memlock

- Type: boolean
- Default: false

Lock all OSD memory to prevent it from being unloaded into swap with mlockall(). Requires sufficient ulimit -l (max locked memory).
