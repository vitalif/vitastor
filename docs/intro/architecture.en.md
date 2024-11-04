[Documentation](../../README.md#documentation) → Introduction → Architecture

-----

[Читать на русском](architecture.ru.md)

# Architecture

- [Basic concepts](#basic-concepts)
- [Similarities to Ceph](#similarities-to-ceph)
- [Differences from Ceph](#differences-from-ceph)
- [Implementation Principles](#implementation-principles)

## Server-side components

- **OSD** (Object Storage Daemon) is a process that directly works with the disk, stores data
  and serves read/write requests. One OSD serves one disk (or one partition). OSDs talk to etcd
  and to each other — they receive cluster state from etcd, and send read/write requests for
  secondary copies of data to other OSDs.
- **etcd** — clustered key/value database, used as a reliable storage for configuration
  and high-level cluster state. Etcd is the component that prevents splitbrain in the cluster.
  Data blocks are not stored in etcd, etcd doesn't participate in data write or read path.
- **Монитор** — a separate node.js based daemon which monitors the cluster, calculates
  required configuration changes and saves them to etcd, thus commanding OSDs to apply these
  changes. Monitor also aggregates cluster statistics. OSD don't talk to monitor, monitor
  only sends and receives data from etcd.

## Basic concepts

- **Pool** is a container for data that has equal redundancy scheme and disk placement rules.
- **PG (Placement Group)** is a "shard" of the cluster, subdivision unit that has its own
  set of OSDs for data storage.
- **Failure Domain** is a group of OSDs, from the simultaneous failure of which you are
  protected by Vitastor. Default failure domain is "host" (server), but you choose a
  larger (for example, a rack of servers) or smaller (a single drive) failure domain
  for every pool.
- **Placement Tree** (similar to Ceph CRUSH Tree) groups OSDs in a hierarchy to later
  split them into Failure Domains.

## Client-side components

- **Client library** incapsulates client I/O logic. Client library connects to etcd and to all OSDs,
  receives cluster state from etcd, sends read and write requests directly to all OSDs. Due
  to the symmetric distributed architecture, all data blocks (each 128 KB by default) are placed
  to different OSDs, but clients always knows where each data block is stored and connects directly
  to the right OSD.

All other client-side components are based on the client library:

- **[vitastor-cli](../usage/cli.en.md)** — command-line utility for cluster management.
  Allows to view cluster state, manage pools and images, i.e. create, modify and remove
  virtual disks, their snapshots and clones.
- **[QEMU driver](../usage/qemu.en.md)** — pluggable QEMU module allowing QEMU/KVM virtual
  machines work with virtual Vitastor disks directly from userspace through the client library,
  without the need to attach disks as kernel block devices. However, if you want to attach
  disks, you can also do that with the same driver and [VDUSE](../usage/qemu.en.md#vduse).
- **[vitastor-nbd](../usage/nbd.en.md)** — utility that allows to attach Vitastor disks as
  kernel block devices using NBD (Network Block Device), which works more like "BUSE"
  (Block Device In Userspace). Vitastor doesn't have Linux kernel modules for the same task
  (at least by now). NBD is an older, non-recommended way to attach disks — you should use
  VDUSE whenever you can.
- **[CSI driver](../installation/kubernetes.en.md)** — driver for attaching Vitastor images
  as Kubernetes persistent volumes. Works through VDUSE (when available) or NBD — images are
  attached as kernel block devices and mounted into containers.
- **Drivers for Proxmox, OpenStack and so on** — pluggable modules for corresponding systems,
  allowing to use Vitastor as storage in them.
- **[vitastor-nfs](../usage/nfs.en.md)** — NFS 3.0 server allowing export of two file system variants:
  the first is a simplified pseudo-FS for file-based access to Vitastor block images (for non-QEMU
  hypervisors with NFS support), the second is **VitastorFS**, full-featured clustered POSIX FS.
  Both variants support parallel access from multiple vitastor-nfs servers. In fact, you are
  not required to setup separate NFS servers at all and use vitastor-nfs mount command on every
  client node — it starts the NFS server and mounts the FS locally.
- **[fio driver](../usage/fio.en.md)** — pluggable module for fio disk benchmarking tool for
  running performance tests on your Vitastor cluster.
- **vitastor-kv** — client for a key-value DB working over shared block volumes (usual
  vitastor images). VitastorFS metadata is stored in vitastor-kv.

## Additional utilities

- **vitastor-disk** — утилита для разметки дисков под Vitastor OSD. С её помощью можно
  создавать, удалять, менять размеры или перемещать разделы OSD.

## Overall read/write process

- Vitastor stores virtual disks, also named "images" or "inodes".
- Each image is stored in some pool. Pool specifies storage parameters such as redundancy
  scheme (replication or EC — erasure codes, i.e. error correction codes), failure domain
  and restrictions on OSD selection for image data placement. See [Pool configuration](../config/pool.en.md) for details.
- Each image is split into objects/blocks of fixed size, equal to [block_size](../config/layout-cluster.en.md#block_size)
  (128 KB by default), multiplied by data part count for EC or 1 for replicas. That is,
  if a pool uses EC 4+2 coding scheme (4 data parts + 2 parity parts), then, with the
  default block_size, images are split into 512 KB objects.
- Client read/write requests are split into parts at object boundaries.
- Each object is mapped to a PG number it belongs to, by simply taking a remainder of
  division of its offset by PG count of the image's pool.
- Client reads primary OSD for all PGs from etcd. Primary OSD for each PG is assigned
  by the monitor during cluster operation, along with the full PG OSD set.
- If not already connected, client connects to primary OSDs of all PGs involved in a
  read/write request and sends parts of the request to them.
- If a primary OSD is unavailable, client retries connection attempts indefinitely
  either until it becomes available or until the monitor assigns another OSD as primary
  for that PG.
- Client also retries requests if the primary OSD replies with error code EPIPE, meaning
  that the PG is inactive at this OSD at the moment - for example, when the primary OSD
  is switched, or if the primary OSD itself loses connection to replicas during request
  handling.
- Primary OSD determines where the parts of the object are stored. By default, all objects
  are assumed to be stored at the target OSD set of a PG, but some of them may be present
  at a different OSD set if they are degraded or moved, or if the data rebalancing process
  is active. OSDs doesn't do any network requests, if calculates locations of all objects
  during PG activation and stores it in memory.
- Primary OSD handles the request locally when it can - for example, when it's a read
  from a replicated pool or when it's a read from a EC pool involving only one data part
  stored on the OSD's local disk.
- When a request requires reads or writes to additional OSDs, primary OSD uses already
  established connections to secondary OSDs of the PG to execute these requests. This happens
  in parallel to local disk operations. All such connections are guaranteed to be already
  established when the PG is active, and if any of them is dropped, PG is restarted and
  all current read/write operations to it fail with EPIPE error and are retried by clients.
- After completing all secondary read/write requests, primary OSD sends the response to
  the client.

### Nuances of request handling

- If a pool uses erasure codes and some of the OSDs are unavailable, primary OSDs recover
  data from the remaining parts during read.
- Each object has a version number. During write, primary OSD first determines the current
  version of the object. As primary OSD usually stores the object or its part itself, most
  of the time version is read from the memory of the OSD itself. However, if primary OSD
  doesn't contain parts of the object, it requests the version number from a secondary OSD
  which has that part. Such request still doesn't involve reading from the disk though,
  because object metadata, including version number, is always stored in OSD memory.
- If a pool uses erasure codes, partial writes of an object require reading other parts of
  it from secondary OSDs or from the local disk of the primary OSD itself. This is called
  "read-modify-write" process.
- If a pool uses erasure codes, two-phase write process is used to get rid of the Write Hole
  problem: first a new version of object parts is written to all secondary OSDs without
  removing the previous version, and then, after receiving successful write confirmations
  from all OSDs, new version is committed and the old one is allowed to be removed.
- In a pool doesn't use immediate_commit mode, then write requests sent by clients aren't
  treated as committed to physical media instantly. Clients have to send separate type of
  requests (SYNC) to commit changes, and before it isn't sent, new versions of data are
  allowed to be lost if some OSDs die. Thus, when immediate_commit is disabled, clients
  store copies of all write requests in memory and repeat them from there when the
  connection to primary OSD is lost. This in-memory copy is removed after a successful
  SYNC, and to prevent excessive memory usage, clients also do an automatic SYNC
  every [client_dirty_limit](../config/network.en.md#client_dirty_limit) written bytes.

## Similarities to Ceph

- Vitastor also has Pools, PGs, OSDs, Monitors, Failure Domains, Placement Tree:
- Vitastor also distributes every image data across the whole cluster.
- Vitastor is also transactional. Even though there's a "lazy fsync mode" which
  doesn't implicitly flush every operation to disks, every write to the cluster is atomic.
- OSDs also have journal and metadata and they can also be put on separate drives.
- Just like in Ceph, client library attempts to recover from any cluster failure so
  you can basically reboot the whole cluster and only pause, but not crash, your clients.

## Differences from Ceph

- Vitastor's primary focus is on SSDs: SSD-only and SSD+HDD clusters.
- The basic layer of Vitastor is block storage with fixed-size blocks, not object storage with
  rich semantics like in Ceph (RADOS).
- PGs are ephemeral in Vitastor. This means that they aren't stored on data disks and only exist
  in memory while OSDs are running.
- Vitastor OSD is (and will always be) single-threaded. If you want to dedicate more than 1 core
  per drive you should run multiple OSDs each on a different partition of the drive.
  Vitastor isn't CPU-hungry though (as opposed to Ceph), so 1 core is sufficient in a lot of cases.
- Metadata is always kept in memory which removes the need for extra disk reads. Metadata size
  depends linearly on drive capacity and data store block size which is 128 KB by default.
  With 128 KB blocks metadata takes around 512 MB per 1 TB (which is still less than Ceph wants).
  Journal is also kept in memory by default, but in SSD-only clusters it's only 32 MB, and in SSD+HDD
  clusters, where it's beneficial to increase it, [inmemory_journal](../config/osd.en.md#inmemory_journal) can be disabled.
- Vitastor storage layer doesn't have internal copy-on-write or redirect-write. I know that maybe
  it's possible to create a good copy-on-write storage, but it's much harder and makes performance
  less deterministic, so CoW isn't used in Vitastor.
- There's a "lazy fsync" mode which allows to batch writes before flushing them to the disk.
  This allows to use Vitastor with desktop SSDs, but still lowers performance due to additional
  network roundtrips, so use server SSDs with capacitor-based power loss protection
  ("Advanced Power Loss Protection") for best performance.
- Recovery process is per-object (per-block), not per-PG. Also there are no PGLOGs.
- Monitors don't store data. Cluster configuration and state is stored in etcd in simple human-readable
  JSON structures. Monitors only watch cluster state and handle data movement.
  Thus Vitastor's Monitor isn't a critical component of the system and is more similar to Ceph's Manager.
  Vitastor's Monitor is implemented in node.js.
- PG distribution isn't based on consistent hashes. All PG mappings are stored in etcd.
  Rebalancing PGs between OSDs is done by mathematical optimization - data distribution problem
  is reduced to a linear programming problem and solved by lp_solve. This allows for almost
  perfect (96-99% uniformity compared to Ceph's 80-90%) data distribution in most cases, ability
  to map PGs by hand without breaking rebalancing logic, reduced OSD peer-to-peer communication
  (on average, OSDs have fewer peers) and less data movement. It also probably has a drawback -
  this method may fail in very large clusters, but up to several hundreds of OSDs it's perfectly fine.
  It's also easy to add consistent hashes in the future if something proves their necessity.
- There's no separate CRUSH layer. You select pool redundancy scheme, placement root, failure domain
  and so on directly in pool configuration.

## Implementation Principles

- I like architecturally simple solutions. Vitastor is and will always be designed
  exactly like that.
- I also like reinventing the wheel to some extent, like writing my own HTTP client
  for etcd interaction instead of using prebuilt libraries, because in this case
  I'm confident about what my code does and what it doesn't do.
- I don't care about C++ "best practices" like RAII or proper inheritance or usage of
  smart pointers or whatever and I don't intend to change my mind, so if you're here
  looking for ideal reference C++ code, this probably isn't the right place.
- I like node.js better than any other dynamically-typed language interpreter
  because it's faster than any other interpreter in the world, has neutral C-like
  syntax and built-in event loop. That's why Monitor is implemented in node.js.

## Known Problems

- Deleting images in a degraded cluster may currently lead to objects reappearing
  after dead OSDs come back, and in case of erasure-coded pools, they may even
  reappear as incomplete. Just repeat the removal request again in this case.
  This problem will be fixed in the nearest future, the fix is already implemented
  in the "epoch-deletions" branch.
