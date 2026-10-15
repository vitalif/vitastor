# The PG chaos-test model (`src/test/test_pg_chaos.cpp`)

A seed-driven stress test of the PG machinery: ONE real `osd_t` (the primary) with a
real blockstore over a mock disk (entirely in RAM, `ringloop_mock`) owns a single PG
(number 1, pool 1, four 128 K objects, 4 K granularity). The other PG members are
fake secondary OSDs inside the test. The pool is either replicated x3 (members 2,3,
spare 4) or EC 2+2 (members 2..4, spare 5, one object = one parity stripe). The seed
uniquely determines the configuration and the whole event sequence; a failing run is
reproduced with the same `--seed`.

What it mainly verifies is the PG machinery (the subop fan-out,
peering/LIST/rollback/stabilize, repeer daemons, stop_pg, INCOMPLETE, recovery) and
partly the primary OSD's storage; checking the storage is not the test's main goal -
test_blockstore_chaos covers it.

## Configurations (10)

- replicated/EC ×
- immediate_commit none/all ×
- csum_block none/4096/16384

`--recovery` enables recovery/rebalance after a set change (off by default so that
degraded states get exercised).

## The world model

Each object has an ordered version history; a version enters the model at SUBMIT
time (the delta = the request's exact bytes/length/granule bitmap), with a state:
- `V_SUBMITTED` - the op was sent and has no answer yet (in flight). It behaves
  like V_OPTIONAL in every respect but one: concurrent reads are entitled not to
  show this write's result, and that must not mark the version DEAD.
- `V_OPTIONAL` - the write was sent but not acknowledged: it either applied (then
  as a whole) or did not apply at all. Versions reach this state from V_SUBMITTED
  and, in the explicit-fsync modes, from V_ACKED when the client session dies
  before a SYNC request (that counts as a marker that the write is unacknowledged).
  Observing an OPTIONAL write's effect in a later read restores it to ACKED;
  observing its absence (not visible over its own range) makes it DEAD.
- `V_ACKED` - the write was acknowledged; the model records `ack_live` (the live set
  at ack time - used to check that the failure model wasn't exceeded). With
  immediate_commit=all it is immediately considered on-disk durable. An ACKED write
  must not roll back while the client's session is alive.
- `V_SYNCED` - acknowledged and SYNCed: durable on disk, and it may never roll back
  again at all.
- `V_PARTIAL` - deleted, but possibly not from every OSD (the PG stayed LEFT_ON_DEAD).
- `V_DEAD` - the version was either definitely refused (-EAGAIN, possibly -ENOSPC in
  the future), or it was OPTIONAL and it later became certain that it was refused
  because a subsequent read did NOT observe it. A DEAD version showing again fails
  the test.

Object contents are self-identifying: the pattern encodes the object, the version and
the offset, so a read result decodes back into "which version wrote these bytes".

Verification reads ONLY through the primary OSD and checks that the answer is
explained by a merge of ONE consistent subset of the written versions, where every
durable version is definitely present, every refused one is definitely absent, and
the contents never lie about their authorship. With an incomplete deletion, EC may
legitimately answer INCOMPLETE, and replicated pools may resurrect the object.

Everything derives from version states and events, with no extra flags: an ACKED
version cannot vanish except through an event that first breaks the session's
guarantee - a crash (taking the session down with the process) or a
repeer/stop_pg (which stop dirty sessions BEFORE peering starts).

The test's checks look ONLY at what the write path exposes to the client; the model
deliberately ignores things the client cannot know about, such as paired OSD
deaths per se.

## The simulated reality

- **Disk simulator**: requests carry delays; `power_loss()` = power outage -
  uncommitted writes are lost, committed ones survive. An unknown FD/opcode aborts
  the test (a stub guard).
- **etcd simulator**: keys/values + leases; revoke/TTL are emulated on a crash (the
  OSD's keys are removed, its lease revoked).
- **Peer OSD simulator**: answers per the secondary-OSD/blockstore contract -
  read/write/delete, SYNC, LIST. When a peer OSD dies, non-SYNCed writes die with it
  with probability `peer_cache_loss`% (default 50 - modelling a volatile disk cache);
  SYNCed ones always survive. A peer crash by itself does NOT change the world model:
  the model only knows what the client sees. The crash repeers the PG, the repeer
  kills dirty client sessions (volatile ACKED writes are exactly what makes a client
  dirty), the session dies - and THAT is what the client observes
  (PEER_STOPPED/EPIPE), and only session_died() demotes. If a crash rolled something
  back under a live session, that would be a rollback invisible to the client -
  check 5 catches it.
- **Network simulator**: service_peers runs every round. The request-phase delay:
  a random `--p-wire`% (default 70) of each peer's queued subops gets applied; the
  response-phase delay: a random `p_reply`% of the already-applied replies gets
  delivered. A reply belongs to its connection: the connection died - undelivered
  replies die with `-EPIPE` (the apply stays). When an OSD instance dies, however,
  mutating ops still in `sent_ops` are copied into a separate "wire" queue; they are
  applied later at the `--p-wire` rate WITHOUT an answer (the connection fell).
  Non-mutating ops (READ/LIST) are dropped (nobody left to answer), and the harvested
  queue is capped at 64 entries.

## Operations (one per step)

Each step picks an action by weight (`--p-*`; 0 disables it):

- **wait (500)** - send nothing, just run the simulation (event windows).
- **write (60)** - write a random range of a random object; bump the version; the
  delta is remembered as OPTIONAL; at most max_inflight (8) in flight.
- **delete (6)** - delete a random object; the version is remembered as OPTIONAL with
  zero length (a full delete); LEFT_ON_DEAD/ack are tracked in account().
- **read (40)** - read an object: whole with probability `full_read`%, otherwise an
  arbitrary range; a whole read verifies the object.
- **sync (30)** - flush the cache (fsync); on confirmation the versions are marked
  SYNCED (durable).
- **restart (1)** - a CLEAN OSD restart: drain the queue (all inflight finish), wait
  for the disk to go idle, destroy, recreate, wait for the OSD to come up, then,
  with `--p-post` (50%) chance, read and verify all object contents ("a clean restart
  that lost nothing").
- **takeover (1)** - a PG handover: first stop the PG by pointing the primary at a
  fake member in the etcd stub -> stop_pg stops the PG; right after the
  `/pg/state/1/1` key disappears (meaning the OSD released the PG) -> recreate the OSD
  and point the primary back at it -> re-verify the objects as in restart.
- **crash (2)** - the primary OSD crashes: a disk power loss (un-synced writes lost),
  lease cancel + removal of the OSD's keys without cancelling the ops in flight to
  peer OSDs, a new instance starts, objects re-verified.
- **peer_down (3)** - a peer OSD crashes: a victim is chosen that breaks no object's
  quorum (all latest OPTIONAL versions down to the first SYNCED are checked), the peer
  OSD stops and its connection drops.
- **peer_up (3)** - a peer OSD comes back (connects - if the PG or its history still
  involves it); then settle(): wait for the PG to come up (ACTIVE|INCOMPLETE +
  quiescence), then run recover_pending - deferred checks of objects whose chains may
  have re-settled.
- **set_change (1)** - the PG moves to another OSD set through etcd; with
  `--recovery` - recovery reads; without it, degradation is legal and the
  "surviving copies" world is protected by the checks.

At the end - wait for completion: clean up until "the PG is healthy and every op has
finished", at most MAX_STEPS=1e6 steps -> exceeding that aborts with a "did not
settle" error.

## Op outcome accounting (account)

- Completed with the expected retval -> V_ACKED. A delete without LEFT_ON_DEAD closes
  the partial-deletion window. A delete with LEFT_ON_DEAD must be repeated after the
  peers come back, and in EC it allows a future INCOMPLETE unless the object is
  rewritten in the meantime.
- Completed with EAGAIN -> V_DEAD: the version was definitely not applied; currently
  unreachable in the test, kept as a placeholder for the future - it would also apply
  to e.g. ENOSPC.
- Completed with EPIPE -> the version stays OPTIONAL: a refusal is not an ack, but the
  op may have applied (or not). An optional delete must also be repeated after the
  peers come back and likewise allows EC INCOMPLETE until the next rewrite.
- Completed with any other error -> the test aborts ("failed with retval").

## Checks (every abort point)

**Data consistency (world_check):**
1. world_check - the answer must be explained by a subset of the versions: every
   required (durable) one is present; bytes are answered by exactly their own version
   ("record obj/seq/off mismatch", a bitmap granule "carries data from wrong
   version").
2. "shows %s but the highest..." - an old version of the object was read while later
   writes are lost (monotonicity broken).
3. "read shows seq over acked v's granule" - a granule's contents (obj/seq/off in the
   record) contradict the version that owns the granule in the model: stitched or
   torn contents instead of the acknowledged bytes.
4. "DEAD version showed again" - refused versions must not come back.
5. "committed version read fully absent over floor" / "established version read
   absent over its granule" - acknowledged ops must not disappear.
6. A read -EIO "OSD deems unrecoverable" is legal in exactly one window: an
   unfinished EC deletion. Outside it, -EIO aborts the test - including a "clean" ack
   delete + INCOMPLETE after a revival (the OSD lied about completeness) and a chain
   rollback under a live session with no deletion window; any other retval != len
   aborts the test.
7. bitmap_len != the expected read answer length -> abort the test.
8. "came back INCOMPLETE" after set_change/crash - same criterion: an unfinished
   deletion window makes INCOMPLETE legal and the deletion is repeated; anything
   else aborts the test.
9. "came back ... which no world of the recorded versions explains" - the final
   check of object contents after the test settles.
10. "changed across a clean restart that lost nothing" - object immutability across
    a clean restart.

**Liveness/timing:**

11. "did not settle in MAX_STEPS" - the ops never completed within a huge step count
    (a possible self-deadlock);
12. "operations did not complete" - stuck ops, printed in the dump; "a read of
    object N did not complete in MAX_STEPS ... deadlock?"; "OSD did not load PGs
    from etcd in MAX_STEPS" / "OSD did not finish peering in MAX_STEPS" - start-up
    checks.
13. "did not release the PG" - the OSD didn't give the PG up when commanded;
14. "disk did not go idle" - a use-after-free guard: every disk op must finish
    before the OSD is destroyed;
15. "PG did not become healthy after reviving every peer" - the PG stayed down after
    all its OSDs came back;
16. "pending recovery/repeated deletion could not complete on a healthy PG" - a
    deletion/recovery that never finished on a live PG.

**Test-stub guards:**

17. An unexpected SQE (fd/opcode) on the disk stub; "unexpected secondary opcode" on
    a peer; arguments like `--p-reply >= 1`, `--max-inflight >= 1`, etc.

**Coverage checks:**

18. `peer_cache_loss<100` + a volatile config + >=8 peer deaths -> verify that the
    variant where a peer OSD kept its journal ran at least once.
19. >=10 dirty OSD deaths/PG handovers -> wire_harvested > 0: at least one op of the
    primary OSD that was in flight must have been harvested into the "wire" on the
    way down - the "delivery from a dead OSD" path was exercised.

## Reporting and reproducibility

The OK line: writes(refused)/deletes/syncs/crashes/restarts/takeovers/peer
deaths(kept journal)/recoveries/set changes/verified reads/stragglers
(applied/captured/lost)/disk ops.

Determinism: one seed -> the same configuration and the same schedule; `-v` traces
disk and peer-OSD operations, op harvesting on peer deaths, and etcd changes.
