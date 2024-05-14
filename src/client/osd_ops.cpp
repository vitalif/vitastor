// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include "osd_ops.h"

const char* osd_op_names[] = {
    "",
    "read",
    "write",
    "write_stable",
    "sync",
    "stabilize",
    "rollback",
    "delete",
    "sync_stab_all",
    "list",
    "show_config",
    "primary_read",
    "primary_write",
    "primary_sync",
    "primary_delete",
    "ping",
    "sec_read_bmp",
    "scrub",
    "describe",
};
