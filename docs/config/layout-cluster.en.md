[Documentation](../../README.md#documentation) → [Configuration](../config.en.md) → Cluster-Wide Disk Layout Parameters

-----

[Читать на русском](layout-cluster.ru.md)

# Cluster-Wide Disk Layout Parameters

These parameters apply to clients and OSDs, are fixed at the moment of OSD drive
initialization and can't be changed after it without losing data.

- [block_size](#block_size)
- [bitmap_granularity](#bitmap_granularity)
- [immediate_commit](#immediate_commit)
- [client_dirty_limit](#client_dirty_limit)

## block_size

- Type: integer
- Default: 131072

Size of objects (data blocks) into which all physical and virtual drives are
subdivided in Vitastor. One of current main settings in Vitastor, affects
memory usage, write amplification and I/O load distribution effectiveness.

Recommended default block size is 128 KB for SSD and 4 MB for HDD. In fact,
it's possible to use 4 MB for SSD too - it will lower memory usage, but
may increase average WA and reduce linear performance.

OSDs with different block sizes (for example, SSD and SSD+HDD OSDs) can
currently coexist in one etcd instance only within separate Vitastor
clusters with different etcd_prefix'es.

Also block size can't be changed after OSD initialization without losing
data.

You must always specify block_size in etcd in /vitastor/config/global if
you change it so all clients can know about it.

OSD memory usage is roughly (SIZE / BLOCK * 68 bytes) which is roughly
544 MB per 1 TB of used disk space with the default 128 KB block size.

## bitmap_granularity

- Type: integer
- Default: 4096

Required virtual disk write alignment ("sector size"). Must be a multiple
of disk_alignment. It's called bitmap granularity because Vitastor tracks
an allocation bitmap for each object containing 2 bits per each
(bitmap_granularity) bytes.

This parameter can't be changed after OSD initialization without losing
data. Also it's fixed for the whole Vitastor cluster i.e. two different
values can't be used in a single Vitastor cluster.

Clients MUST be aware of this parameter value, so put it into etcd key
/vitastor/config/global if you change it for any reason.

## immediate_commit

- Type: string
- Default: false

Another parameter which is really important for performance.

Desktop SSDs are very fast (100000+ iops) for simple random writes
without cache flush. However, they are really slow (only around 1000 iops)
if you try to fsync() each write, that is, when you want to guarantee that
each change gets immediately persisted to the physical media.

Server-grade SSDs with "Advanced/Enhanced Power Loss Protection" or with
"Supercapacitor-based Power Loss Protection", on the other hand, are equally
fast with and without fsync because their cache is protected from sudden
power loss by a built-in supercapacitor-based "UPS".

Some software-defined storage systems always fsync each write and thus are
really slow when used with desktop SSDs. Vitastor, however, can also
efficiently utilize desktop SSDs by postponing fsync until the client calls
it explicitly.

This is what this parameter regulates. When it's set to "all" the whole
Vitastor cluster commits each change to disks immediately and clients just
ignore fsyncs because they know for sure that they're unneeded. This reduces
the amount of network roundtrips performed by clients and improves
performance. So it's always better to use server grade SSDs with
supercapacitors even with Vitastor, especially given that they cost only
a bit more than desktop models.

There is also a common SATA SSD (and HDD too!) firmware bug (or feature)
that makes server SSDs which have supercapacitors slow with fsync. To check
if your SSDs are affected, compare benchmark results from `fio -name=test
-ioengine=libaio -direct=1 -bs=4k -rw=randwrite -iodepth=1` with and without
`-fsync=1`. Results should be the same. If fsync=1 result is worse you can
try to work around this bug by "disabling" drive write-back cache by running
`hdparm -W 0 /dev/sdXX` or `echo write through > /sys/block/sdXX/device/scsi_disk/*/cache_type`
(IMPORTANT: don't mistake it with `/sys/block/sdXX/queue/write_cache` - it's
unsafe to change by hand). The same may apply to newer HDDs with internal
SSD cache or "media-cache" - for example, a lot of Seagate EXOS drives have
it (they have internal SSD cache even though it's not stated in datasheets).

This parameter must be set both in etcd in /vitastor/config/global and in
OSD command line or configuration. Setting it to "all" or "small" requires
enabling disable_journal_fsync and disable_meta_fsync, setting it to "all"
also requires enabling disable_data_fsync.

TLDR: For optimal performance, set immediate_commit to "all" if you only use
SSDs with supercapacitor-based power loss protection (nonvolatile
write-through cache) for both data and journals in the whole Vitastor
cluster. Set it to "small" if you only use such SSDs for journals. Leave
empty if your drives have write-back cache.

## client_dirty_limit

- Type: integer
- Default: 33554432

Without immediate_commit=all this parameter sets the limit of "dirty"
(not committed by fsync) data allowed by the client before forcing an
additional fsync and committing the data. Also note that the client always
holds a copy of uncommitted data in memory so this setting also affects
RAM usage of clients.

This parameter doesn't affect OSDs themselves.
