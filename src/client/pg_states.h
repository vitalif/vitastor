// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#pragma once

#pragma GCC visibility push(default)

// Placement group states
// STARTING -> [acquire lock] -> PEERING -> INCOMPLETE|ACTIVE
// ACTIVE -> REPEERING -> PEERING
// ACTIVE -> STOPPING -> OFFLINE -> [release lock]
// Exactly one of these:
#define PG_STARTING (1<<0)
#define PG_PEERING (1<<1)
#define PG_INCOMPLETE (1<<2)
#define PG_ACTIVE (1<<3)
#define PG_REPEERING (1<<4)
#define PG_STOPPING (1<<5)
#define PG_OFFLINE (1<<6)
// Plus any of these:
#define PG_DEGRADED (1<<7)
#define PG_HAS_INCOMPLETE (1<<8)
#define PG_HAS_DEGRADED (1<<9)
#define PG_HAS_MISPLACED (1<<10)
#define PG_HAS_UNCLEAN (1<<11)
#define PG_HAS_INVALID (1<<12)
#define PG_HAS_CORRUPTED (1<<13)
#define PG_HAS_INCONSISTENT (1<<14)
#define PG_LEFT_ON_DEAD (1<<15)
#define PG_SCRUBBING (1<<16)

// Lower bits that represent object role (EC 0/1/2... or always 0 with replication)
// 12 bits is a safe default that doesn't depend on pg_stripe_size or pg_block_size
#define STRIPE_MASK ((uint64_t)4096 - 1)

// OSD object states
#define OBJ_DEGRADED 0x02
#define OBJ_INCOMPLETE 0x04
#define OBJ_MISPLACED 0x08
// OBJ_CORRUPTED is always set with one of OBJ_INCOMPLETE/OBJ_DEGRADED/OBJ_MISPLACED
#define OBJ_CORRUPTED 0x10
// OBJ_INCONSISTENT is when its replicas don't match, but it's unclear which one is correct
// OBJ_INCONSISTENT may be set with CORRUPTED, but never with other states
#define OBJ_INCONSISTENT 0x20
#define OBJ_NEEDS_STABLE 0x10000
#define OBJ_NEEDS_ROLLBACK 0x20000

extern const int pg_state_bits[];
extern const char *pg_state_names[];
extern const int pg_state_bit_count;

extern const int object_state_bits[];
extern const char *object_state_names[];
extern const int object_state_bit_count;

#pragma GCC visibility pop
