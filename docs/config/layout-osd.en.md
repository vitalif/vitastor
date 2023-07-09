[Documentation](../../README.md#documentation) → [Configuration](../config.en.md) → OSD Disk Layout Parameters

-----

[Читать на русском](layout-osd.ru.md)

# OSD Disk Layout Parameters

These parameters apply to OSDs, are fixed at the moment of OSD drive
initialization and can't be changed after it without losing data.

- [data_device](#data_device)
- [meta_device](#meta_device)
- [journal_device](#journal_device)
- [journal_offset](#journal_offset)
- [journal_size](#journal_size)
- [meta_offset](#meta_offset)
- [data_offset](#data_offset)
- [data_size](#data_size)
- [meta_block_size](#meta_block_size)
- [journal_block_size](#journal_block_size)
- [disable_data_fsync](#disable_data_fsync)
- [disable_meta_fsync](#disable_meta_fsync)
- [disable_journal_fsync](#disable_journal_fsync)
- [disable_device_lock](#disable_device_lock)
- [disk_alignment](#disk_alignment)
- [data_csum_type](#data_csum_type)
- [csum_block_size](#csum_block_size)

## data_device

- Type: string

Path to the block device to use for data. It's highly recommendded to use
stable paths for all device names: `/dev/disk/by-partuuid/xxx...` instead
of just `/dev/sda` or `/dev/nvme0n1` to not mess up after server restart.
Files can also be used instead of block devices, but this is implemented
only for testing purposes and not for production.

## meta_device

- Type: string

Path to the block device to use for the metadata. Metadata must be on a fast
SSD or performance will suffer. If this option is skipped, `data_device` is
used for the metadata.

## journal_device

- Type: string

Path to the block device to use for the journal. Journal must be on a fast
SSD or performance will suffer. If this option is skipped, `meta_device` is
used for the journal, and if it's also empty, journal is put on
`data_device`. It's almost always fine to put metadata and journal on the
same device, in this case you only need to set `meta_device`.

## journal_offset

- Type: integer
- Default: 0

Offset on the device in bytes where the journal is stored.

## journal_size

- Type: integer

Journal size in bytes. By default, all available space between journal_offset
and data_offset, meta_offset or the end of the journal device is used.
Large journals aren't needed in SSD-only setups, 32 MB is always enough.
In SSD+HDD setups it is beneficial to use larger journals (for example, 1 GB)
and enable [throttle_small_writes](osd.en.md#throttle_small_writes).

## meta_offset

- Type: integer
- Default: 0

Offset on the device in bytes where the metadata area is stored.
Again, set it to something if you colocate metadata with journal or data.

## data_offset

- Type: integer
- Default: 0

Offset on the device in bytes where the data area is stored.
Again, set it to something if you colocate data with journal or metadata.

## data_size

- Type: integer

Data area size in bytes. By default, the whole data device up to the end
will be used for the data area, but you can restrict it if you want to use
a smaller part. Note that there is no option to set metadata area size -
it's derived from the data area size.

## meta_block_size

- Type: integer
- Default: 4096

Physical block size of the metadata device. 4096 for most current
HDDs and SSDs.

## journal_block_size

- Type: integer
- Default: 4096

Physical block size of the journal device. Must be a multiple of
`disk_alignment`. 4096 for most current HDDs and SSDs.

## disable_data_fsync

- Type: boolean
- Default: false

Do not issue fsyncs to the data device, i.e. do not flush its cache.
Safe ONLY if your data device has write-through cache. If you disable
the cache yourself using `hdparm` or `scsi_disk/cache_type` then make sure
that the cache disable command is run every time before starting Vitastor
OSD, for example, in the systemd unit. See also `immediate_commit` option
for the instructions to disable cache and how to benefit from it.

## disable_meta_fsync

- Type: boolean
- Default: false

Same as disable_data_fsync, but for the metadata device. If the metadata
device is not set or if the data device is used for the metadata the option
is ignored and disable_data_fsync value is used instead of it.

## disable_journal_fsync

- Type: boolean
- Default: false

Same as disable_data_fsync, but for the journal device. If the journal
device is not set or if the metadata device is used for the journal the
option is ignored and disable_meta_fsync value is used instead of it. If
the same device is used for data, metadata and journal the option is also
ignored and disable_data_fsync value is used instead of it.

## disable_device_lock

- Type: boolean
- Default: false

Do not lock data, metadata and journal block devices exclusively with
flock(). Though it's not recommended, but you can use it you want to run
multiple OSD with a single device and different offsets, without using
partitions.

## disk_alignment

- Type: integer
- Default: 4096

Required physical disk write alignment. Most current SSD and HDD drives
use 4 KB physical sectors even if they report 512 byte logical sector
size, so 4 KB is a good default setting.

Note, however, that physical sector size also affects WA, because with block
devices it's impossible to write anything smaller than a block. So, when
Vitastor has to write a single metadata entry that's only about 32 bytes in
size, it actually has to write the whole 4 KB sector.

Because of this it can actually be beneficial to use SSDs which work well
with 512 byte sectors and use 512 byte disk_alignment, journal_block_size
and meta_block_size. But the only SSD that may fit into this category is
Intel Optane (probably, not tested yet).

Clients don't need to be aware of disk_alignment, so it's not required to
put a modified value into etcd key /vitastor/config/global.

## data_csum_type

- Type: string
- Default: none

Data checksum type to use. May be "crc32c" or "none". Set to "crc32c" to
enable data checksums.

## csum_block_size

- Type: integer
- Default: 4096

Checksum calculation block size.

Must be equal or a multiple of [bitmap_granularity](layout-cluster.en.md#bitmap_granularity)
(which is usually 4 KB).

Checksums increase metadata size by 4 bytes per each csum_block_size of data.

Checksums are always a compromise:
1. You either sacrifice +1 GB RAM per 1 TB of data
2. Or you raise csum_block_size, for example, to 32k and sacrifice
   50% random write iops due to checksum read-modify-write
3. Or you turn off [inmemory_metadata](osd.en.md#inmemory_metadata) and
   sacrifice 50% random read iops due to checksum reads

Option 1 (default) is recommended for all-flash setups because these usually
have enough RAM.

Option 2 is recommended for HDD-only setups. HDD-only setups usually do NOT
have enough RAM for the default 4 KB csum_block_size.

Option 3 is recommended for SSD+HDD setups (because metadata SSDs will handle
extra reads without any performance drop) and also *maybe* for NVMe all-flash
setups when you don't have enough RAM (because NVMe drives have plenty
of read iops to spare).
