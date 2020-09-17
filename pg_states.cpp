// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.0 or GNU GPL-2.0+ (see README.md for details)

#include "pg_states.h"

const int pg_state_bit_count = 14;

const int pg_state_bits[14] = {
    PG_STARTING,
    PG_PEERING,
    PG_INCOMPLETE,
    PG_ACTIVE,
    PG_STOPPING,
    PG_OFFLINE,
    PG_DEGRADED,
    PG_HAS_INCOMPLETE,
    PG_HAS_DEGRADED,
    PG_HAS_MISPLACED,
    PG_HAS_UNCLEAN,
    PG_HAS_INVALID,
    PG_LEFT_ON_DEAD,
};

const char *pg_state_names[14] = {
    "starting",
    "peering",
    "incomplete",
    "active",
    "stopping",
    "offline",
    "degraded",
    "has_incomplete",
    "has_degraded",
    "has_misplaced",
    "has_unclean",
    "has_invalid",
    "left_on_dead",
};
