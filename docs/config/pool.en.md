[Documentation](../../README.md#documentation) → [Configuration](../config.en.md) → Pool configuration

-----

[Читать на русском](pool.ru.md)

# Pool configuration

Pool configuration is set in etcd key `/vitastor/config/pools` in the following
JSON format:

```
{
  "<Numeric ID>": {
    "name": "<name>",
    ...other parameters...
  }
}
```

- [name](#name)
- [scheme](#scheme)
- [pg_size](#pg_size)
- [parity_chunks](#parity_chunks)
- [pg_minsize](#pg_minsize)
- [pg_count](#pg_count)
- [failure_domain](#failure_domain)
- [max_osd_combinations](#max_osd_combinations)
- [pg_stripe_size](#pg_stripe_size)
- [root_node](#root_node)
- [osd_tags](#osd_tags)
- [primary_affinity_tags](#primary_affinity_tags)

Examples:

- [Replicated Pool](#replicated-pool)
- [Erasure-coded Pool](#erasure-coded-pool)

# Parameters

## name

- Type: string
- Required

Pool name.

## scheme

- Type: string
- Required
- One of: "replicated", "xor" or "jerasure"

Redundancy scheme used for data in this pool.

## pg_size

- Type: integer
- Required

Total number of disks for PGs of this pool - i.e., number of replicas for
replicated pools and number of data plus parity disks for EC/XOR pools.

## parity_chunks

- Type: integer

Number of parity chunks for EC/XOR pools. For such pools, data will be lost
if you lose more than parity_chunks disks at once, so this parameter can be
equally described as FTT (number of failures to tolerate).

Required for EC/XOR pools, ignored for replicated pools.

## pg_minsize

- Type: integer
- Required

Number of available live disks for PGs of this pool to remain active.
That is, if it becomes impossible to place PG data on at least (pg_minsize)
OSDs, PG is deactivated for both read and write. So you know that a fresh
write always goes to at least (pg_minsize) OSDs (disks).

FIXME: pg_minsize behaviour may be changed in the future to only make PGs
read-only instead of deactivating them.

## pg_count

- Type: integer
- Required

Number of PGs for this pool. The value should be big enough for the monitor /
LP solver to be able to optimize data placement.

"Enough" is usually around 64-128 PGs per OSD, i.e. you set pg_count for pool
to (total OSD count * 100 / pg_size). You can round it to the closest power of 2,
because it makes it easier to reduce or increase PG count later by dividing or
multiplying it by 2.

In Vitastor, PGs are ephemeral, so you can change pool PG count anytime just
by overwriting pool configuration in etcd. Amount of the data affected by
rebalance will be smaller if the new PG count is a multiple of the old PG count
or vice versa.

## failure_domain

- Type: string
- Default: host

Failure domain specification. Must be "host" or "osd" or refer to one of the
placement tree levels, defined in [placement_levels](monitor.en.md#placement_levels).

Two replicas, or two parts in case of EC/XOR, of the same block of data are
never put on OSDs in the same failure domain (for example, on the same host).
So failure domain specifies the unit which failure you are protecting yourself
from.

## max_osd_combinations

- Type: integer
- Default: 10000

Vitastor data placement algorithm is based on the LP solver and OSD combinations
which are fed to it are generated ramdonly. This parameter specifies the maximum
number of combinations to generate when optimising PG placement.

This parameter usually doesn't require to be changed.

## pg_stripe_size

- Type: integer
- Default: 0

Specifies the stripe size for this pool according to which images are split into
different PGs. Stripe size can't be smaller than [block_size](layout-cluster.en.md#block_size)
multiplied by (pg_size - parity_chunks) for EC/XOR pools, or 1 for replicated pools,
and the same value is used by default.

This means first `pg_stripe_size = (block_size * (pg_size-parity_chunks))` bytes
of an image go to one PG, next `pg_stripe_size` bytes go to another PG and so on.

Usually doesn't require to be changed separately from the block size.

## root_node

- Type: string

Specifies the root node of the OSD tree to restrict this pool OSDs to.
Referenced root node must exist in /vitastor/config/node_placement.

## osd_tags

- Type: string or array of strings

Specifies OSD tags to restrict this pool to. If multiple tags are specified,
only OSDs having all of these tags will be used for this pool.

## primary_affinity_tags

- Type: string or array of strings

Specifies OSD tags to prefer putting primary OSDs in this pool to.
Note that for EC/XOR pools Vitastor always prefers to put primary OSD on one
of the OSDs containing a data chunk for a PG.

# Examples

## Replicated pool

```
{
  "1": {
    "name":"testpool",
    "scheme":"replicated",
    "pg_size":2,
    "pg_minsize":1,
    "pg_count":256,
    "failure_domain":"host"
  }
}
```

## Erasure-coded pool

```
{
  "2": {
    "name":"ecpool",
    "scheme":"jerasure",
    "pg_size":3,
    "parity_chunks":1,
    "pg_minsize":2,
    "pg_count":256,
    "failure_domain":"host"
  }
}
```
