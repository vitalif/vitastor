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

Pool configuration is also affected by:

- [OSD Placement Tree](#placement-tree)
- [Separate OSD settings](#osd-settings)

Parameters:

- [name](#name)
- [scheme](#scheme)
- [pg_size](#pg_size)
- [parity_chunks](#parity_chunks)
- [pg_minsize](#pg_minsize)
- [pg_count](#pg_count)
- [failure_domain](#failure_domain)
- [max_osd_combinations](#max_osd_combinations)
- [block_size](#block_size)
- [bitmap_granularity](#bitmap_granularity)
- [immediate_commit](#immediate_commit)
- [pg_stripe_size](#pg_stripe_size)
- [root_node](#root_node)
- [osd_tags](#osd_tags)
- [primary_affinity_tags](#primary_affinity_tags)
- [scrub_interval](#scrub_interval)

Examples:

- [Replicated Pool](#replicated-pool)
- [Erasure-coded Pool](#erasure-coded-pool)

# Placement Tree

OSD placement tree is set in a separate etcd key `/vitastor/config/node_placement`
in the following JSON format:

`
{
  "<node name or OSD number>": {
    "level": "<level>",
    "parent": "<parent node name, if any>"
  },
  ...
}
`

Here, if a node name is a number then it is assumed to refer to an OSD.
Level of the OSD is always "osd" and cannot be overriden. You may only
override parent node of the OSD which is its host by default.

Non-numeric node names refer to other placement tree nodes like hosts, racks,
datacenters and so on.

Hosts of all OSDs are auto-created in the tree with level "host" and name
equal to the host name reported by a corresponding OSD. You can refer to them
without adding them to this JSON tree manually.

Level may be "host", "osd" or refer to some other placement tree level
from [placement_levels](monitor.en.md#placement_levels).

Parent node reference is required for intermediate tree nodes.

# OSD settings

Separate OSD settings are set in etc keys `/vitastor/config/osd/<number>`
in JSON format `{"<key>":<value>}`.

As of now, two settings are supported:

## reweight

- Type: number, between 0 and 1
- Default: 1

Every OSD receives PGs proportional to its size. Reweight is a multiplier for
OSD size used during PG distribution.

This means an OSD configured with reweight lower than 1 receives less PGs than
it normally would. An OSD with reweight = 0 won't store any data. You can set
reweight to 0 to trigger rebalance and remove all data from an OSD.

## tags

- Type: string or array of strings

Sets tag or multiple tags for this OSD. Tags can be used to group OSDs into
subsets and then use a specific subset for pool instead of all OSDs.
For example you can mark SSD OSDs with tag "ssd" and HDD OSDs with "hdd" and
such tags will work as device classes.

# Pool parameters

## name

- Type: string
- Required

Pool name.

## scheme

- Type: string
- Required
- One of: "replicated", "xor", "ec" or "jerasure"

Redundancy scheme used for data in this pool. "jerasure" is an alias for "ec",
both use Reed-Solomon-Vandermonde codes based on ISA-L or jerasure libraries.
Fast ISA-L based implementation is used automatically when it's available,
slower jerasure version is used otherwise.

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

For example, the difference between pg_minsize 2 and 1 in a 3-way replicated
pool (pg_size=3) is:
- If 2 hosts go down with pg_minsize=2, the pool becomes inactive and remains
  inactive for [osd_out_time](monitor.en.md#osd_out_time) (10 minutes). After
  this timeout, the monitor selects replacement hosts/OSDs and the pool comes
  up and starts to heal. Therefore, if you don't have replacement OSDs, i.e.
  if you only have 3 hosts with OSDs and 2 of them are down, the pool remains
  inactive until you add or return at least 1 host (or change failure_domain
  to "osd").
- If 2 hosts go down with pg_minsize=1, the pool only experiences a short
  I/O pause until the monitor notices that OSDs are down (5-10 seconds with
  the default [etcd_report_interval](osd.en.md#etcd_report_interval)). After
  this pause, I/O resumes, but new data is temporarily written in only 1 copy.
  Then, after osd_out_time, the monitor also selects replacement OSDs and the
  pool starts to heal.

So, pg_minsize regulates the number of failures that a pool can tolerate
without temporary downtime for [osd_out_time](monitor.en.md#osd_out_time),
but at a cost of slightly reduced storage reliability.

FIXME: pg_minsize behaviour may be changed in the future to only make PGs
read-only instead of deactivating them.

## pg_count

- Type: integer
- Required

Number of PGs for this pool. The value should be big enough for the monitor /
LP solver to be able to optimize data placement.

"Enough" is usually around 10-100 PGs per OSD, i.e. you set pg_count for pool
to (total OSD count * 10 / pg_size). You can round it to the closest power of 2,
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

## block_size

- Type: integer
- Default: 131072

Block size for this pool. The value from /vitastor/config/global is used when
unspecified. Only OSDs with matching block_size are used for each pool. If you
want to further restrict OSDs for the pool, use [osd_tags](#osd_tags).

Read more about this parameter in [Cluster-Wide Disk Layout Parameters](layout-cluster.en.md#block_size).

## bitmap_granularity

- Type: integer
- Default: 4096

"Sector" size of virtual disks in this pool. The value from /vitastor/config/global
is used when unspecified. Similarly to block_size, only OSDs with matching
bitmap_granularity are used for each pool.

Read more about this parameter in [Cluster-Wide Disk Layout Parameters](layout-cluster.en.md#bitmap_granularity).

## immediate_commit

- Type: string, one of "all", "small" and "none"
- Default: none

Immediate commit setting for this pool. The value from /vitastor/config/global
is used when unspecified. Similarly to block_size, only OSDs with compatible
bitmap_granularity are used for each pool. "Compatible" means that a pool with
non-immediate commit will use OSDs with immediate commit enabled, but not vice
versa. I.e., pools with "none" use all OSDs, pools with "small" only use OSDs
with "all" or "small", and pools with "all" only use OSDs with "all".

Read more about this parameter in [Cluster-Wide Disk Layout Parameters](layout-cluster.en.md#immediate_commit).

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

## scrub_interval

- Type: time interval (number + unit s/m/h/d/M/y)

Automatic scrubbing interval for this pool. Overrides
[global scrub_interval setting](osd.en.md#scrub_interval).

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
    "scheme":"ec",
    "pg_size":3,
    "parity_chunks":1,
    "pg_minsize":2,
    "pg_count":256,
    "failure_domain":"host"
  }
}
```
