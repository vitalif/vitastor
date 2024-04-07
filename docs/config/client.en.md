[Documentation](../../README.md#documentation) → [Configuration](../config.en.md) → Client Parameters

-----

[Читать на русском](client.ru.md)

# Client Parameters

These parameters apply only to Vitastor clients (QEMU, fio, NBD and so on) and
affect their interaction with the cluster.

- [client_retry_interval](#client_retry_interval)
- [client_eio_retry_interval](#client_eio_retry_interval)
- [client_retry_enospc](#client_retry_enospc)
- [client_max_dirty_bytes](#client_max_dirty_bytes)
- [client_max_dirty_ops](#client_max_dirty_ops)
- [client_enable_writeback](#client_enable_writeback)
- [client_max_buffered_bytes](#client_max_buffered_bytes)
- [client_max_buffered_ops](#client_max_buffered_ops)
- [client_max_writeback_iodepth](#client_max_writeback_iodepth)
- [nbd_timeout](#nbd_timeout)
- [nbd_max_devices](#nbd_max_devices)
- [nbd_max_part](#nbd_max_part)
- [osd_nearfull_ratio](#osd_nearfull_ratio)

## client_retry_interval

- Type: milliseconds
- Default: 50
- Minimum: 10
- Can be changed online: yes

Retry time for I/O requests failed due to inactive PGs or network
connectivity errors.

## client_eio_retry_interval

- Type: milliseconds
- Default: 1000
- Can be changed online: yes

Retry time for I/O requests failed due to data corruption or unfinished
EC object deletions (has_incomplete PG state). 0 disables such retries
and clients are not blocked and just get EIO error code instead.

## client_retry_enospc

- Type: boolean
- Default: true
- Can be changed online: yes

Retry writes on out of space errors to wait until some space is freed on
OSDs.

## client_max_dirty_bytes

- Type: integer
- Default: 33554432
- Can be changed online: yes

Without [immediate_commit](layout-cluster.en.md#immediate_commit)=all this parameter sets the limit of "dirty"
(not committed by fsync) data allowed by the client before forcing an
additional fsync and committing the data. Also note that the client always
holds a copy of uncommitted data in memory so this setting also affects
RAM usage of clients.

## client_max_dirty_ops

- Type: integer
- Default: 1024
- Can be changed online: yes

Same as client_max_dirty_bytes, but instead of total size, limits the number
of uncommitted write operations.

## client_enable_writeback

- Type: boolean
- Default: false
- Can be changed online: yes

This parameter enables client-side write buffering. This means that write
requests are accumulated in memory for a short time before being sent to
a Vitastor cluster which allows to send them in parallel and increase
performance of some applications. Writes are buffered until client forces
a flush with fsync() or until the amount of buffered writes exceeds the
limit.

Write buffering significantly increases performance of some applications,
for example, CrystalDiskMark under Windows (LOL :-D), but also any other
applications if they do writes in one of two non-optimal ways: either if
they do a lot of small (4 kb or so) sequential writes, or if they do a lot
of small random writes, but without any parallelism or asynchrony, and also
without calling fsync().

With write buffering enabled, you can expect around 22000 T1Q1 random write
iops in QEMU more or less regardless of the quality of your SSDs, and this
number is in fact bound by QEMU itself rather than Vitastor (check it
yourself by adding a "driver=null-co" disk in QEMU). Without write
buffering, the current record is 9900 iops, but the number is usually
even lower with non-ideal hardware, for example, it may be 5000 iops.

Even when this parameter is enabled, write buffering isn't enabled until
the client explicitly allows it, because enabling it without the client
being aware of the fact that his writes may be buffered may lead to data
loss. Because of this, older versions of clients don't support write
buffering at all, newer versions of the QEMU driver allow write buffering
only if it's enabled in disk settings with `-blockdev cache.direct=false`,
and newer versions of FIO only allow write buffering if you don't specify
`-direct=1`. NBD and NFS drivers allow write buffering by default.

You can overcome this restriction too with the `client_writeback_allowed`
parameter, but you shouldn't do that unless you **really** know what you
are doing.

## client_max_buffered_bytes

- Type: integer
- Default: 33554432
- Can be changed online: yes

Maximum total size of buffered writes which triggers write-back when reached.

## client_max_buffered_ops

- Type: integer
- Default: 1024
- Can be changed online: yes

Maximum number of buffered writes which triggers write-back when reached.
Multiple consecutive modified data regions are counted as 1 write here.

## client_max_writeback_iodepth

- Type: integer
- Default: 256
- Can be changed online: yes

Maximum number of parallel writes when flushing buffered data to the server.

## nbd_timeout

- Type: seconds
- Default: 300

Timeout for I/O operations for [NBD](../usage/nbd.en.md). If an operation
executes for longer than this timeout, including when your cluster is just
temporarily down for more than timeout, the NBD device will detach by itself
(and possibly break the mounted file system).

You can set timeout to 0 to never detach, but in that case you won't be
able to remove the kernel device at all if the NBD process dies - you'll have
to reboot the host.

## nbd_max_devices

- Type: integer
- Default: 64

Maximum number of NBD devices in the system. This value is passed as
`nbds_max` parameter for the nbd kernel module when vitastor-nbd autoloads it.

## nbd_max_part

- Type: integer
- Default: 3

Maximum number of partitions per NBD device. This value is passed as
`max_part` parameter for the nbd kernel module when vitastor-nbd autoloads it.
Note that (nbds_max)*(1+max_part) usually can't exceed 256.

## osd_nearfull_ratio

- Type: number
- Default: 0.95
- Can be changed online: yes

Ratio of used space on OSD to treat it as "almost full" in vitastor-cli status output.

Remember that some client writes may hang or complete with an error if even
just one OSD becomes 100 % full!

However, unlike in Ceph, 100 % full Vitastor OSDs don't crash (in Ceph they're
unable to start at all), so you'll be able to recover from "out of space" errors
without destroying and recreating OSDs.
