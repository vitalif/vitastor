[Documentation](../../README.md#documentation) → [Configuration](../config.en.md) → Network Protocol Parameters

-----

[Читать на русском](network.ru.md)

# Network Protocol Parameters

These parameters apply to clients and OSDs and affect network connection logic
between clients, OSDs and etcd.

- [tcp_header_buffer_size](#tcp_header_buffer_size)
- [use_sync_send_recv](#use_sync_send_recv)
- [use_rdma](#use_rdma)
- [rdma_device](#rdma_device)
- [rdma_port_num](#rdma_port_num)
- [rdma_gid_index](#rdma_gid_index)
- [rdma_mtu](#rdma_mtu)
- [rdma_max_sge](#rdma_max_sge)
- [rdma_max_msg](#rdma_max_msg)
- [rdma_max_recv](#rdma_max_recv)
- [rdma_max_send](#rdma_max_send)
- [peer_connect_interval](#peer_connect_interval)
- [peer_connect_timeout](#peer_connect_timeout)
- [osd_idle_timeout](#osd_idle_timeout)
- [osd_ping_timeout](#osd_ping_timeout)
- [up_wait_retry_interval](#up_wait_retry_interval)
- [max_etcd_attempts](#max_etcd_attempts)
- [etcd_quick_timeout](#etcd_quick_timeout)
- [etcd_slow_timeout](#etcd_slow_timeout)
- [etcd_keepalive_timeout](#etcd_keepalive_timeout)
- [etcd_ws_keepalive_timeout](#etcd_ws_keepalive_timeout)
- [client_dirty_limit](#client_dirty_limit)

## tcp_header_buffer_size

- Type: integer
- Default: 65536

Size of the buffer used to read data using an additional copy. Vitastor
packet headers are 128 bytes, payload is always at least 4 KB, so it is
usually beneficial to try to read multiple packets at once even though
it requires to copy the data an additional time. The rest of each packet
is received without an additional copy. You can try to play with this
parameter and see how it affects random iops and linear bandwidth if you
want.

## use_sync_send_recv

- Type: boolean
- Default: false

If true, synchronous send/recv syscalls are used instead of io_uring for
socket communication. Useless for OSDs because they require io_uring anyway,
but may be required for clients with old kernel versions.

## use_rdma

- Type: boolean
- Default: true

Try to use RDMA for communication if it's available. Disable if you don't
want Vitastor to use RDMA. TCP-only clients can also talk to an RDMA-enabled
cluster, so disabling RDMA may be needed if clients have RDMA devices,
but they are not connected to the cluster.

## rdma_device

- Type: string

RDMA device name to use for Vitastor OSD communications (for example,
"rocep5s0f0"). Please note that Vitastor RDMA requires Implicit On-Demand
Paging (Implicit ODP) and Scatter/Gather (SG) support from the RDMA device
to work. For example, Mellanox ConnectX-3 and older adapters don't have
Implicit ODP, so they're unsupported by Vitastor. Run `ibv_devinfo -v` as
root to list available RDMA devices and their features.

Remember that you also have to configure your network switches if you use
RoCE/RoCEv2, otherwise you may experience unstable performance. Refer to
the manual of your network vendor for details about setting up the switch
for RoCEv2 correctly. Usually it means setting up Lossless Ethernet with
PFC (Priority Flow Control) and ECN (Explicit Congestion Notification).

## rdma_port_num

- Type: integer
- Default: 1

RDMA device port number to use. Only for devices that have more than 1 port.
See `phys_port_cnt` in `ibv_devinfo -v` output to determine how many ports
your device has.

## rdma_gid_index

- Type: integer
- Default: 0

Global address identifier index of the RDMA device to use. Different GID
indexes may correspond to different protocols like RoCEv1, RoCEv2 and iWARP.
Search for "GID" in `ibv_devinfo -v` output to determine which GID index
you need.

**IMPORTANT:** If you want to use RoCEv2 (as recommended) then the correct
rdma_gid_index is usually 1 (IPv6) or 3 (IPv4).

## rdma_mtu

- Type: integer
- Default: 4096

RDMA Path MTU to use. Must be 1024, 2048 or 4096. There is usually no
sense to change it from the default 4096.

## rdma_max_sge

- Type: integer
- Default: 128

Maximum number of scatter/gather entries to use for RDMA. OSDs negotiate
the actual value when establishing connection anyway, so it's usually not
required to change this parameter.

## rdma_max_msg

- Type: integer
- Default: 132096

Maximum size of a single RDMA send or receive operation in bytes.

## rdma_max_recv

- Type: integer
- Default: 16

Maximum number of RDMA receive buffers per connection (RDMA requires
preallocated buffers to receive data). Each buffer is `rdma_max_msg` bytes
in size. So this setting directly affects memory usage: a single Vitastor
RDMA client uses `rdma_max_recv * rdma_max_msg * OSD_COUNT` bytes of memory.
Default is roughly 2 MB * number of OSDs.

## rdma_max_send

- Type: integer
- Default: 8

Maximum number of outstanding RDMA send operations per connection. Should be
less than `rdma_max_recv` so the receiving side doesn't run out of buffers.
Doesn't affect memory usage - additional memory isn't allocated for send
operations.

## peer_connect_interval

- Type: seconds
- Default: 5
- Minimum: 1

Interval before attempting to reconnect to an unavailable OSD.

## peer_connect_timeout

- Type: seconds
- Default: 5
- Minimum: 1

Timeout for OSD connection attempts.

## osd_idle_timeout

- Type: seconds
- Default: 5
- Minimum: 1

OSD connection inactivity time after which clients and other OSDs send
keepalive requests to check state of the connection.

## osd_ping_timeout

- Type: seconds
- Default: 5
- Minimum: 1

Maximum time to wait for OSD keepalive responses. If an OSD doesn't respond
within this time, the connection to it is dropped and a reconnection attempt
is scheduled.

## up_wait_retry_interval

- Type: milliseconds
- Default: 500
- Minimum: 50

OSDs respond to clients with a special error code when they receive I/O
requests for a PG that's not synchronized and started. This parameter sets
the time for the clients to wait before re-attempting such I/O requests.

## max_etcd_attempts

- Type: integer
- Default: 5

Maximum number of attempts for etcd requests which can't be retried
indefinitely.

## etcd_quick_timeout

- Type: milliseconds
- Default: 1000

Timeout for etcd requests which should complete quickly, like lease refresh.

## etcd_slow_timeout

- Type: milliseconds
- Default: 5000

Timeout for etcd requests which are allowed to wait for some time.

## etcd_keepalive_timeout

- Type: seconds
- Default: max(30, etcd_report_interval*2)

Timeout for etcd connection HTTP Keep-Alive. Should be higher than
etcd_report_interval to guarantee that keepalive actually works.

## etcd_ws_keepalive_timeout

- Type: seconds
- Default: 30

etcd websocket ping interval required to keep the connection alive and
detect disconnections quickly.

## client_dirty_limit

- Type: integer
- Default: 33554432

Without immediate_commit=all this parameter sets the limit of "dirty"
(not committed by fsync) data allowed by the client before forcing an
additional fsync and committing the data. Also note that the client always
holds a copy of uncommitted data in memory so this setting also affects
RAM usage of clients.

This parameter doesn't affect OSDs themselves.
