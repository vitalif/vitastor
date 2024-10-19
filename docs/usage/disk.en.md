[Documentation](../../README.md#documentation) → Usage → Disk management tool

-----

[Читать на русском](disk.ru.md)

# Disk management tool

vitastor-disk is a command-line tool for physical Vitastor disk management.

It supports the following commands:

- [prepare](#prepare)
- [upgrade-simple](#upgrade-simple)
- [resize](#resize)
- [raw-resize](#raw-resize)
- [start/stop/restart/enable/disable](#start/stop/restart/enable/disable)
- [purge](#purge)
- [read-sb](#read-sb)
- [write-sb](#write-sb)
- [update-sb](#update-sb)
- [udev](#udev)
- [exec-osd](#exec-osd)
- [pre-exec](#pre-exec)
- Debugging:
  - [dump-journal](#dump-journal)
  - [write-journal](#write-journal)
  - [dump-meta](#dump-meta)
  - [write-meta](#write-meta)
- [simple-offsets](#simple-offsets)

## prepare

`vitastor-disk prepare [OPTIONS] [devices...]`

Initialize disk(s) for Vitastor OSD(s).

There are two modes of this command. In the first mode, you pass `<devices>` which
must be raw disks (not partitions). They are partitioned automatically and OSDs
are initialized on all of them.

In the second mode, you omit `<devices>` and pass `--data_device`, `--journal_device`
and/or `--meta_device` which must be already existing partitions identified by their
GPT partition UUIDs. In this case a single OSD is created.

Requires `vitastor-cli`, `wipefs`, `sfdisk` and `partprobe` (from parted) utilities.

Options (automatic mode):

```
--osd_per_disk <N>
  Create <N> OSDs on each disk (default 1)
--hybrid
  Prepare hybrid (HDD+SSD) OSDs using provided devices. SSDs will be used for
  journals and metadata, HDDs will be used for data. Partitions for journals and
  metadata will be created automatically. Whether disks are SSD or HDD is decided
  by the `/sys/block/.../queue/rotational` flag. In hybrid mode, default object
  size is 1 MB instead of 128 KB, default journal size is 1 GB instead of 32 MB,
  and throttle_small_writes is enabled by default.
--disable_data_fsync auto
  Disable data device cache and fsync (1/yes/true = on, default auto)
--disable_meta_fsync auto
  Disable metadata/journal device cache and fsync (default auto)
--meta_reserve 2x,1G
  New metadata partitions in --hybrid mode are created larger than actual
  metadata size to ease possible future extension. The default is to allocate
  2 times more space and at least 1G. Use this option to override.
--max_other 10%
  Use disks for OSD data even if they already have non-Vitastor partitions,
  but only if these take up no more than this percent of disk space.
```

Options (single-device mode):

```
--data_device <DEV>        Use partition <DEV> for data
--meta_device <DEV>        Use partition <DEV> for metadata (optional)
--journal_device <DEV>     Use partition <DEV> for journal (optional)
--disable_data_fsync 0     Disable data device cache and fsync (default off)
--disable_meta_fsync 0     Disable metadata device cache and fsync (default off)
--disable_journal_fsync 0  Disable journal device cache and fsync (default off)
--force                    Bypass partition safety checks (for emptiness and so on)
```

Options (both modes):

```
--journal_size 1G/32M      Set journal size (area or partition size)
--block_size 1M/128k       Set blockstore object size
--bitmap_granularity 4k    Set bitmap granularity
--data_csum_type none      Set data checksum type (crc32c or none)
--csum_block_size 4k/32k   Set data checksum block size (SSD/HDD default)
--data_device_block 4k     Override data device block size
--meta_device_block 4k     Override metadata device block size
--journal_device_block 4k  Override journal device block size
```

[immediate_commit](../config/layout-cluster.en.md#immediate_commit) setting is
automatically derived from "disable fsync" options. It's set to "all" when fsync
is disabled on all devices, and to "small" if fsync is only disabled on journal device.

When data/meta/journal fsyncs are disabled, the OSD startup script automatically
checks the device cache status on start and tries to disable cache for SATA/SAS disks.
If it doesn't succeed it issues a warning in the system log.

You can also pass other OSD options here as arguments and they'll be persisted
in the superblock: cached_io_data, cached_io_meta, cached_io_journal,
inmemory_metadata, inmemory_journal, max_write_iodepth,
min_flusher_count, max_flusher_count, journal_sector_buffer_count,
journal_no_same_sector_overwrites, throttle_small_writes, throttle_target_iops,
throttle_target_mbs, throttle_target_parallelism, throttle_threshold_us.
See [Runtime OSD Parameters](../config/osd.en.md) for details.

## upgrade-simple

`vitastor-disk upgrade-simple <UNIT_FILE|OSD_NUMBER>`

Upgrade an OSD created by old (0.7.1 and older) `make-osd.sh` or `make-osd-hybrid.js` scripts.

Adds superblocks to OSD devices, disables old `vitastor-osdN` unit and replaces it with `vitastor-osd@N`.
Can be invoked with an osd number of with a path to systemd service file `UNIT_FILE` which
must be `/etc/systemd/system/vitastor-osd<OSD_NUMBER>.service`.

Note that the procedure isn't atomic and may ruin OSD data in case of an interrupt,
so don't upgrade all your OSDs in parallel.

Requires the `sfdisk` utility.

## resize

`vitastor-disk resize <osd_num>|<osd_device> [OPTIONS]`

Resize data area and/or move journal and metadata:

| <!-- -->                  | <!-- -->                               |
|---------------------------|----------------------------------------|
| `--move-journal TARGET`   | move journal to `TARGET`               |
| `--move-meta TARGET`      | move metadata to `TARGET`              |
| `--journal-size NEW_SIZE` | resize journal to `NEW_SIZE`           |
| `--data-size NEW_SIZE`    | resize data device to `NEW_SIZE`       |
| `--dry-run`               | only show new layout, do not apply it  |

`NEW_SIZE` may include k/m/g/t suffixes.

`TARGET` may be one of:

| <!-- -->       | <!-- -->                                                                 |
|----------------|--------------------------------------------------------------------------|
| `<partition>`  | move journal/metadata to an existing GPT partition                       |
| `<raw_device>` | create a GPT partition on `<raw_device>` and move journal/metadata to it |
| `""`           | (empty string) move journal/metadata back to the data device             |

## raw-resize

`vitastor-disk raw-resize <ALL_OSD_PARAMETERS> <NEW_LAYOUT> [--iodepth 32]`

Resize data area and/or rewrite/move journal and metadata (manual format).

`ALL_OSD_PARAMETERS` must include all (at least all disk-related)
parameters from OSD command line (i.e. from systemd unit or superblock).

`NEW_LAYOUT` may include new disk layout parameters:

| <!-- -->                    | <!-- -->                                  |
|-----------------------------|-------------------------------------------|
| `--new_data_offset SIZE`    | resize data area so it starts at `SIZE`   |
| `--new_data_len SIZE`       | resize data area to `SIZE` bytes          |
| `--new_meta_device PATH`    | use `PATH` for new metadata               |
| `--new_meta_offset SIZE`    | make new metadata area start at `SIZE`    |
| `--new_meta_len SIZE`       | make new metadata area `SIZE` bytes long  |
| `--new_journal_device PATH` | use `PATH` for new journal                |
| `--new_journal_offset SIZE` | make new journal area start at `SIZE`     |
| `--new_journal_len SIZE`    | make new journal area `SIZE` bytes long   |

SIZE may include k/m/g/t suffixes. If any of the new layout parameter
options are not specified, old values will be used.

## start/stop/restart/enable/disable

`vitastor-disk start|stop|restart|enable|disable [--now] <device> [device2 device3 ...]`

Manipulate Vitastor OSDs using systemd by their device paths.

Commands are passed to `systemctl` with `vitastor-osd@<num>` units as arguments.

When `--now` is added to enable/disable, OSDs are also immediately started/stopped.

## purge

`vitastor-disk purge [--force] [--allow-data-loss] <device> [device2 device3 ...]`

Purge Vitastor OSD(s) on specified device(s). Uses `vitastor-cli rm-osd` to check
if deletion is possible without data loss and to actually remove metadata from etcd.
`--force` and `--allow-data-loss` options may be used to ignore safety check results.

Requires `vitastor-cli`, `sfdisk` and `partprobe` (from parted) utilities.

## read-sb

`vitastor-disk read-sb [--force] <device>`

Try to read Vitastor OSD superblock from `<device>` and print it in JSON format.
`--force` allows to ignore validation errors.

## write-sb

`vitastor-disk write-sb <device>`

Read JSON from STDIN and write it into Vitastor OSD superblock on `<device>`.

## update-sb

`vitastor-disk update-sb <device> [--force] [--<parameter> <value>] [...]`

Read Vitastor OSD superblock from <device>, update parameters in it and write it back.

`--force` allows to ignore validation errors.

## udev

`vitastor-disk udev <device>`

Try to read Vitastor OSD superblock from `<device>` and print variables for udev.

## exec-osd

`vitastor-disk exec-osd <device>`

Read Vitastor OSD superblock from `<device>` and start the OSD with parameters from it.

Intended for use from startup scripts (i.e. from systemd units).

## pre-exec

`vitastor-disk pre-exec <device>`

Read Vitastor OSD superblock from `<device>` and perform pre-start checks for the OSD.

For now, this only checks that device cache is in write-through mode if fsync is disabled.

Intended for use from startup scripts (i.e. from systemd units).

## dump-journal

`vitastor-disk dump-journal [OPTIONS] <osd_device>`

`vitastor-disk dump-journal [OPTIONS] <journal_file> <journal_block_size> <offset> <size>`

Dump journal in human-readable or JSON (if `--json` is specified) format.

You can specify any OSD device (data, metadata or journal), or the layout manually.

Options:

```
--all             Scan the whole journal area for entries and dump them, even outdated ones
--json            Dump journal in JSON format
--format entries  (Default) Dump actual journal entries as an array, without data
--format data     Same as "entries", but also include small write data
--format blocks   Dump as an array of journal blocks each containing array of entries
```

## write-journal

`vitastor-disk write-journal <osd_device>`

`vitastor-disk write-journal <journal_file> <journal_block_size> <bitmap_size> <offset> <size>`

Write journal from JSON taken from standard input in the same format as produced by
`dump-journal --json --format data`.

You can specify any OSD device (data, metadata or journal), or the layout manually.

## dump-meta

`vitastor-disk dump-meta <osd_device>`

`vitastor-disk dump-meta <meta_file> <meta_block_size> <offset> <size>`

Dump metadata in JSON format.

You can specify any OSD device (data, metadata or journal), or the layout manually.

## write-meta

`vitastor-disk write-meta <osd_device>`

`vitastor-disk write-meta <meta_file> <offset> <size>`

Write metadata from JSON taken from standard input in the same format as produced by `dump-meta`.

You can specify any OSD device (data, metadata or journal), or the layout manually.

## simple-offsets

`vitastor-disk simple-offsets <device>`

Calculate offsets for old simple&stupid (no superblock) OSD deployment.

Options (see also [Cluster-Wide Disk Layout Parameters](../config/layout-cluster.en.md)):

```
--object_size 128k       Set blockstore block size
--bitmap_granularity 4k  Set bitmap granularity
--journal_size 32M       Set journal size
--data_csum_type none    Set data checksum type (crc32c or none)
--csum_block_size 4k     Set data checksum block size
--device_block_size 4k   Set device block size
--journal_offset 0       Set journal offset
--device_size 0          Set device size
--format text            Result format: json, options, env, or text
```
