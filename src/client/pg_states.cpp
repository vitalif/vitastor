// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include "pg_states.h"

const int pg_state_bit_count = 17;

const int pg_state_bits[17] = {
    PG_STARTING,
    PG_PEERING,
    PG_INCOMPLETE,
    PG_ACTIVE,
    PG_REPEERING,
    PG_STOPPING,
    PG_OFFLINE,
    PG_DEGRADED,
    PG_HAS_INCONSISTENT,
    PG_HAS_CORRUPTED,
    PG_HAS_INCOMPLETE,
    PG_HAS_DEGRADED,
    PG_HAS_MISPLACED,
    PG_HAS_UNCLEAN,
    PG_HAS_INVALID,
    PG_LEFT_ON_DEAD,
    PG_SCRUBBING,
};

const char *pg_state_names[17] = {
    "starting",
    "peering",
    "incomplete",
    "active",
    "repeering",
    "stopping",
    "offline",
    "degraded",
    "has_inconsistent",
    "has_corrupted",
    "has_incomplete",
    "has_degraded",
    "has_misplaced",
    "has_unclean",
    "has_invalid",
    "left_on_dead",
    "scrubbing",
};

const int object_state_bit_count = 8;

const int object_state_bits[8] = {
    OBJ_DEGRADED,
    OBJ_INCOMPLETE,
    OBJ_MISPLACED,
    OBJ_CORRUPTED,
    OBJ_INCONSISTENT,
    OBJ_NEEDS_STABLE,
    OBJ_NEEDS_ROLLBACK,
    0,
};

const char *object_state_names[8] = {
    "degraded",
    "incomplete",
    "misplaced",
    "corrupted",
    "inconsistent",
    "needs_stable",
    "needs_rollback",
    "clean",
};
