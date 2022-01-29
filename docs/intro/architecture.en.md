[Documentation](../../README.md#documentation) → Introduction → Architecture

-----

[Читать на русском](architecture.ru.md)

# Architecture

- [Basic concepts](#basic-concepts)
- [Similarities to Ceph](#similarities-to-ceph)
- [Differences from Ceph](#differences-from-ceph)
- [Implementation Principles](#implementation-principles)

## Basic concepts

- OSD (Object Storage Daemon) is a process that stores data and serves read/write requests.
- PG (Placement Group) is a "shard" of the cluster, group of data stored on one set of replicas.
- Pool is a container for data that has equal redundancy scheme and placement rules.
- Monitor is a separate daemon that watches cluster state and handles failures.
- Failure Domain is a group of OSDs that you allow to fail. It's "host" by default.
- Placement Tree groups OSDs in a hierarchy to later split them into Failure Domains.

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
  clusters, where it's beneficial to increase it, [inmemory_journal](docs/config/osd.en.md#inmemory_journal) can be disabled.
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
