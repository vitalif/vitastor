[Documentation](../../README.md#documentation) → Tests → The blockstore chaos-test model

-----

[Читать на русском](blockstore-chaos.ru.md)

# The blockstore chaos-test model

A seed-driven crash test of the blockstore (`src/test/test_blockstore_chaos.cpp`):
the real blockstore - journal, metadata, flusher, compaction, checksums, recovery -
running on a mock disk that lives entirely in RAM. Nothing above it exists: no OSD,
no PG, no network, no etcd. The test talks to the blockstore directly and checks
only what the storage itself promises. It is the counterpart of the
[PG chaos test](pg-chaos.en.md), which assumes the storage is correct.

Everything BELOW the blockstore is simulated: io_uring completions come back out of
submission order after a random delay, a volatile write cache holds part of every
write, and a power outage makes the writes that were in flight land fully, torn at a
sector boundary, or not at all, while a random half of the cache disappears. The
model of what was written is kept alongside, and after every outage the recovered
state is checked against it.

The same workload and the same checks run against both implementations - v1
(meta_format 2) and v2 (heap, meta_format 3) - which are supposed to have identical
semantics. The seed also picks the configuration, so a failing run is replayed by
passing back the seed it printed; `-h` lists the options.

The data is four 128 K objects (inode 1, four stripes) on one 256 MB mock disk that
carries metadata at offset 0, the journal from 16 M and the data from 32 M; write
granularity is 4 K.

## Configurations (24)

- v1 / v2 blockstore (meta_format 2 / 3) ×
- immediate_commit none/all ×
- `csum_block` none / 4096 / 16384 - 16384 is deliberately larger than the 4 K write
  granularity, so a write covers only part of a checksummed block ×
- instant/two-phase writes, like in replicated/EC pools (BS_OP_WRITE_STABLE / BS_OP_WRITE+BS_OP_STABLE)

2 × 2 × 3 × 2 = 24; the configuration is `seed % 24`, `--config` overrides it. The
two write styles can't be mixed inside one object: a stable version over an unstable
one is rejected with EINVAL, and a pool is always one or the other.

## The world model

Per object the model keeps `submitted` (the highest sequence ever enqueued), `acked`,
`synced` and `durable`, plus maps from the sequence number to the full 128 K content
(an empty vector means "deleted"), to the version the store actually sees, and to
the bitmap byte. Sequence numbers only ever grow and key the model, because version
numbers restart from 1 after a deletion - the store explicitly allows writing low
versions over a delete. `deleted_upto` stops an old sync snapshot from making a
wiped-out version stabilizable, and `can_rollback` records that this instance really
did stabilize the durable version (recovery may have merged it into one clean entry).

Durability is derived purely from completions - there are no extra flags:

- with `immediate_commit=all` a completed write is immediately synced; with a
  volatile disk cache nothing is synced until a SYNC that covers it completes;
- a synced deletion and a synced `BS_OP_WRITE_STABLE` write are stable by themselves
  and so are durable at once; a two-phase write still has to be stabilized, and that
  happens asynchronously, from a later step.

Object contents are self-identifying: 32-byte records `{seq, version, object,
offset}` whose size divides the 4 K granularity, so a record never straddles a write
or a read, and any mismatch decodes into "which write put these bytes here".

External bitmaps are self-identifying too: every write fills its 4-byte bitmap (one
bit per 4 K granule) with one recognisable byte. The blockstore treats the bitmap as
opaque, but it travels with the entry through compaction, recovery and rollbacks
just like the data does.

Invariant: only one operation at a time may touch an object. While its write, read,
stabilize or rollback is in flight nothing else may go near it, so the base content
of a write and the expected answer of a read cannot change underneath them.

## The simulated reality

- **Event loop**: one step is the completion of the earliest in-flight disk request,
  and the virtual clock jumps straight to it, so the configured latencies don't make
  the test slow. A single seeded PRNG drives the latencies, the workload and the
  power-outage dice alike.
- **Disk simulator**: per-request delays are drawn uniformly (10-200 us for reads
  and writes, 100-2000 us for fsync) - that is what makes completions arrive out of
  order; 4 K sector; writes up to 4 K are atomic and land whole or not at
  all. In buffered mode (`immediate_commit=none`) writes go into a volatile cache
  and only reach the media on fsync. On `power_loss()` every in-flight write
  lands fully, torn at a sector boundary, or not at all, and every cached write
  survives with probability 50% - the survivors are committed to the disk and the
  cache is emptied, so nothing from it disappears at the next outage. Error injection
  for read/write/fsync exists but is off by default: the blockstore treats most write
  errors as fatal, by design.
- **Ringloop stub**: an unknown fd aborts the test at once; `reset()` drops queued
  and pending completions as if the process had died, so no stale callback can fire
  into a destroyed blockstore. A power outage empties the disk's in-flight queue by
  definition, and a clean restart waits for it to go idle explicitly.

## Operations (one per step)

Each step picks an action by weight (`--p-*`; 0 disables it):

- **wait (500)** - submit nothing, just let the simulation run one event.
- **write (60)** - write a random 4 K-aligned range of a random free object: a new
  version (numbering restarts after a deletion), self-describing payload, its own
  bitmap byte.
- **delete (6)** - delete an existing object; deleting a missing one is a no-op the
  store answers without creating an entry, so it isn't exercised. A deletion is
  stable by itself.
- **read (40)** - read a random 4 K-aligned range - deliberately NOT aligned to the
  checksum block, so reads cut across checksum blocks and across the boundaries of
  the extents that were actually written - or the whole object with `full_read`%
  (25) chance; the answer is verified against the model when it completes.
- **sync (30)** - flush; what it covers is the set of acknowledged versions as of
  submit time, treated as a lower bound. Not submitted while a rollback is in
  flight: a rollback rewinds the test model - `acked` and `synced` drop back to the
  stable version, everything above it disappears from it - so the snapshot would
  name versions that no longer exist. This is a limit of the test model, not of the
  blockstore API.
- **stabilize (50)** - commit a random non-empty prefix of everything that has
  become stabilizable, as a single op carrying several (object, version) entries -
  the shape an OSD uses; single-entry ops happen just as naturally.
- **rollback (6)** - throw the unstable versions away instead of committing them.
  Two-phase mode only (an instant write is stable at once), and only for objects
  that have a stable non-deleted version below the newest one. On success everything
  above the stable version is gone and the store's version counter for the object
  goes back to it. Not submitted while a sync is in flight, for the same reason,
  nor while another rollback is in flight.
- **restart (1)** - a CLEAN restart: drain everything in flight, read all objects,
  wait for the disk to go idle, destroy the blockstore, create it again, read
  everything again.
- **crash (2)** - pull the plug right now: power loss, whatever was in flight is
  accounted as a legal-but-unconfirmed outcome, the blockstore is destroyed and
  recreated, every object is checked against the recovery rule.

At most `max_inflight` (8) operations are in flight at once; a step that finds the
queue full waits for one completion instead of submitting anything - that is where
hangs are checked.

At the end of the run everything still in flight is drained, and every object is
read whole and verified.

## Op outcome accounting

- Completed with the expected retval - the content is recorded at its sequence
  number, `acked` moves to it, and the follow-up (synced / durable / needs-stabilize)
  is derived from the configuration.
- A write or delete with `-EAGAIN` - a refusal, not a loss (the journal is full and
  the flusher can't keep up under pressure); the model is unchanged.
- A failed sync - it simply didn't sync anything; also legal under pressure.
- A read interrupted by an outage tells nothing and is dropped.
- Anything else - including a failed stabilize or rollback - aborts the test with
  the affected (object, version) entries printed.
- A write that was in flight at the moment of the outage: its content is recorded as
  a legal outcome all the same, but `acked` doesn't move, so it may still be lost.

## Checks (every abort point)

**Data consistency:**

1. a read of a deleted or never-written object returns `-ENOENT`;
2. a read returns exactly the acknowledged content: `retval == len` and byte-for-byte
   equality over the whole range; the first differing record is printed with both
   obj/seq/version/offset values, so stale data names its own author;
3. the returned external bitmap must belong to the returned version ("returned the
   bitmap of another version");
4. after a crash or a restart a read answers either the whole object or `-ENOENT` -
   any other retval means torn data was committed;
5. whatever came back must be the content of a version at or above the guaranteed
   floor: the last durable version after a power outage, the last acknowledged one
   (the last synced one with a volatile cache) after a restart that lost nothing.
   Anything else aborts the test with "came back ... holding unexpected content"
   plus a diff against every candidate version - the one that almost matches usually
   tells which write was lost or applied out of order;
6. the object may legitimately be gone only when nothing was guaranteed at all; then
   the model resets and numbering starts over from 1;
7. with `immediate_commit=all` a clean restart must return exactly the same state -
   same retval, same version, same bytes; the failure also prints the first
   never-written journal sector, which points straight at the lost record;
8. what came back becomes the new baseline but not the new durable point: the
   durable content stays a legal outcome, `can_rollback` is cleared, and the next
   outage may still roll the object back to the durable version;
9. a rollback may only remove versions above the stable one, and the version counter
   is rewound accordingly - later writes and reads would notice otherwise;
10. the final whole-object reads must match the acknowledged content.

**Liveness:**

11. "operations did not complete in 1000000 steps - deadlock?" - the stuck
    operations are printed together with the store's diagnostics
    (completed/fsynced/compacted LSNs, compact queue) in the v2 implementation;
12. "blockstore failed to start";
13. "a read of object N did not complete in MAX_STEPS - deadlock?" and "the final
    read of object N did not complete";
14. "disk did not go idle before a clean restart" - a use-after-free guard: no
    request may outlive the blockstore that submitted it.

**Failure accounting:**

15. only `-EAGAIN` is accepted as a refusal for a write/delete, a failed sync is
    treated as "synced nothing", and any other error, including a failed stabilize
    or rollback, aborts the test.

**Test-stub guards:**

16. an unexpected fd on the ringloop stub, a rejected SQE, `--max-inflight < 1`;
17. overlapping concurrent writes: a write submitted while another in-flight write
    still covers part of its range aborts the test - a device may apply such a pair
    in either order, so relying on the newer one winning is a bug, and catching it
    at submission time is deterministic instead of waiting for an unlucky completion
    order.

## Reporting and reproducibility

By default the test runs 48 fixed seeds (every configuration twice) plus 152 random
ones spread evenly over the configurations - 200 seeds × 10000 steps in total. Every
seed runs in its own child process, so one abort doesn't take the whole sweep down,
and the failing seeds are listed at the end. `--seed N` runs a single seed
in-process, which is what you want under a debugger; `--seeds a-b`, `--random n`,
`--steps n`, `--config n` and the `--p-*` weights shape the run (e.g.
`--p-delete 0 --p-rollback 0` gives a workload without deletions and rollbacks).

The OK line: writes (refused with EAGAIN) / deletes / syncs / stabilizes / rollbacks
/ crashes / clean restarts / verified reads / disk ops.

Determinism: one seed -> the same configuration and the same schedule; `-v` traces
every operation and every simulated disk request, including its simulated latency,
the power-loss decision taken for every in-flight write and every cached write
dropped from the volatile cache.

Built by `make build_tests` and run by ctest as `test_blockstore_chaos`.
