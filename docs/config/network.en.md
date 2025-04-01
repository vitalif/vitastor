[Documentation](../../README.md#documentation) → [Configuration](../config.en.md) → Network Protocol Parameters

-----

[Читать на русском](network.ru.md)

# Network Protocol Parameters

These parameters apply to clients and OSDs and affect network connection logic
between clients, OSDs and etcd.

- [osd_network](#osd_network)
- [osd_cluster_network](#osd_cluster_network)
- [use_rdma](#use_rdma)
- [use_rdmacm](#use_rdmacm)
- [disable_tcp](#disable_tcp)
- [rdma_device](#rdma_device)
- [rdma_port_num](#rdma_port_num)
- [rdma_gid_index](#rdma_gid_index)
- [rdma_mtu](#rdma_mtu)
- [rdma_max_sge](#rdma_max_sge)
- [rdma_max_msg](#rdma_max_msg)
- [rdma_max_recv](#rdma_max_recv)
- [rdma_max_send](#rdma_max_send)
- [rdma_odp](#rdma_odp)
- [peer_connect_interval](#peer_connect_interval)
- [peer_connect_timeout](#peer_connect_timeout)
- [osd_idle_timeout](#osd_idle_timeout)
- [osd_ping_timeout](#osd_ping_timeout)
- [max_etcd_attempts](#max_etcd_attempts)
- [etcd_quick_timeout](#etcd_quick_timeout)
- [etcd_slow_timeout](#etcd_slow_timeout)
- [etcd_keepalive_timeout](#etcd_keepalive_timeout)
- [etcd_ws_keepalive_interval](#etcd_ws_keepalive_interval)
- [etcd_min_reload_interval](#etcd_min_reload_interval)
- [tcp_header_buffer_size](#tcp_header_buffer_size)
- [use_sync_send_recv](#use_sync_send_recv)

## osd_network

- Type: string or array of strings

Network mask of public OSD network(s) (IPv4 or IPv6). Each OSD listens to all
addresses of UP + RUNNING interfaces matching one of these networks, on the
same port. Port is auto-selected except if [bind_port](osd.en.md#bind_port) is
explicitly specified. Bind address(es) may also be overridden manually by
specifying [bind_address](osd.en.md#bind_address). If OSD networks are not specified
at all, OSD just listens to a wildcard address (0.0.0.0).

## osd_cluster_network

- Type: string or array of strings

Network mask of separate network(s) (IPv4 or IPv6) to use for OSD
cluster connections. I.e. OSDs will always attempt to use these networks
to connect to other OSDs, while clients will attempt to use networks from
[osd_network](#osd_network).

## use_rdma

- Type: boolean
- Default: true

Try to use RDMA through libibverbs for communication if it's available.
Disable if you don't want Vitastor to use RDMA. TCP-only clients can also
talk to an RDMA-enabled cluster, so disabling RDMA may be needed if clients
have RDMA devices, but they are not connected to the cluster.

`use_rdma` works with RoCEv1/RoCEv2 networks, but not with iWARP and,
maybe, with some Infiniband configurations which require RDMA-CM.
Consider `use_rdmacm` for such networks.

## use_rdmacm

- Type: boolean
- Default: true

Use an alternative implementation of RDMA through RDMA-CM (Connection
Manager). Works with all RDMA networks: Infiniband, iWARP and
RoCEv1/RoCEv2, and even allows to disable TCP and run only with RDMA.
When enabled, OSDs listen to the same address(es) and port(s) using
TCP and RDMA-CM. `use_rdma` is automatically disabled when `use_rdmacm`
is enabled.

## disable_tcp

- Type: boolean
- Default: true

Fully disable TCP and only use RDMA-CM for OSD communication.

## rdma_device

- Type: string

RDMA device name to use for Vitastor OSD communications (for example,
"rocep5s0f0"). If not specified, Vitastor will try to find an RoCE
device matching [osd_network](osd.en.md#osd_network), preferring RoCEv2,
or choose the first available RDMA device if no RoCE devices are
found or if `osd_network` is not specified. Auto-selection is also
unsupported with old libibverbs < v32, like in Debian 10 Buster or
CentOS 7.

Vitastor supports all adapters, even ones without ODP support, like
Mellanox ConnectX-3 and non-Mellanox cards. Versions up to Vitastor
1.2.0 required ODP which is only present in Mellanox ConnectX >= 4.
See also [rdma_odp](#rdma_odp).

Run `ibv_devinfo -v` as root to list available RDMA devices and their
features.

Remember that you also have to configure your network switches if you use
RoCE/RoCEv2, otherwise you may experience unstable performance. Refer to
the manual of your network vendor for details about setting up the switch
for RoCEv2 correctly. Usually it means setting up Lossless Ethernet with
PFC (Priority Flow Control) and ECN (Explicit Congestion Notification).

## rdma_port_num

- Type: integer

RDMA device port number to use. Only for devices that have more than 1 port.
See `phys_port_cnt` in `ibv_devinfo -v` output to determine how many ports
your device has.

Not relevant for RDMA-CM (use_rdmacm).

## rdma_gid_index

- Type: integer

Global address identifier index of the RDMA device to use. Different GID
indexes may correspond to different protocols like RoCEv1, RoCEv2 and iWARP.
Search for "GID" in `ibv_devinfo -v` output to determine which GID index
you need.

If not specified, Vitastor will try to auto-select a RoCEv2 IPv4 GID, then
RoCEv2 IPv6 GID, then RoCEv1 IPv4 GID, then RoCEv1 IPv6 GID, then IB GID.
GID auto-selection is unsupported with libibverbs < v32.

A correct rdma_gid_index for RoCEv2 is usually 1 (IPv6) or 3 (IPv4).

Not relevant for RDMA-CM (use_rdmacm).

## rdma_mtu

- Type: integer

RDMA Path MTU to use. Must be 1024, 2048 or 4096. Default is to use the
RDMA device's MTU.

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

## rdma_odp

- Type: boolean
- Default: false

Use RDMA with On-Demand Paging. ODP is currently only available on Mellanox
ConnectX-4 and newer adapters. ODP allows to not register memory explicitly
for RDMA adapter to be able to use it. This, in turn, allows to skip memory
copying during sending. One would think this should improve performance, but
**in reality** RDMA performance with ODP is **drastically** worse. Example
3-node cluster with 8 NVMe in each node and 2*25 GBit/s ConnectX-6 RDMA network
without ODP pushes 3950000 read iops, but only 239000 iops with ODP...

This happens because Mellanox ODP implementation seems to be based on
message retransmissions when the adapter doesn't know about the buffer yet -
it likely uses standard "RNR retransmissions" (RNR = receiver not ready)
which is generally slow in RDMA/RoCE networks. Here's a presentation about
it from ISPASS-2021 conference: https://tkygtr6.github.io/pub/ISPASS21_slides.pdf

ODP support is retained in the code just in case a good ODP implementation
appears one day.

## peer_connect_interval

- Type: seconds
- Default: 5
- Minimum: 1
- Can be changed online: yes

Interval before attempting to reconnect to an unavailable OSD.

## peer_connect_timeout

- Type: seconds
- Default: 5
- Minimum: 1
- Can be changed online: yes

Timeout for OSD connection attempts.

## osd_idle_timeout

- Type: seconds
- Default: 5
- Minimum: 1
- Can be changed online: yes

OSD connection inactivity time after which clients and other OSDs send
keepalive requests to check state of the connection.

## osd_ping_timeout

- Type: seconds
- Default: 5
- Minimum: 1
- Can be changed online: yes

Maximum time to wait for OSD keepalive responses. If an OSD doesn't respond
within this time, the connection to it is dropped and a reconnection attempt
is scheduled.

## max_etcd_attempts

- Type: integer
- Default: 5
- Can be changed online: yes

Maximum number of attempts for etcd requests which can't be retried
indefinitely.

## etcd_quick_timeout

- Type: milliseconds
- Default: 1000
- Can be changed online: yes

Timeout for etcd requests which should complete quickly, like lease refresh.

## etcd_slow_timeout

- Type: milliseconds
- Default: 5000
- Can be changed online: yes

Timeout for etcd requests which are allowed to wait for some time.

## etcd_keepalive_timeout

- Type: seconds
- Default: max(30, etcd_report_interval*2)
- Can be changed online: yes

Timeout for etcd connection HTTP Keep-Alive. Should be higher than
etcd_report_interval to guarantee that keepalive actually works.

## etcd_ws_keepalive_interval

- Type: seconds
- Default: 5
- Can be changed online: yes

etcd websocket ping interval required to keep the connection alive and
detect disconnections quickly.

## etcd_min_reload_interval

- Type: milliseconds
- Default: 1000
- Can be changed online: yes

Minimum interval for full etcd state reload. Introduced to prevent
excessive load on etcd during outages when etcd can't keep up with event
streams and cancels them.

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
