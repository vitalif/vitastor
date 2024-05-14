// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include <assert.h>

#include "msgr_op.h"

osd_op_t::~osd_op_t()
{
    assert(!bs_op);
    assert(!op_data);
    if (bitmap_buf)
    {
        free(bitmap_buf);
    }
    if (rmw_buf)
    {
        free(rmw_buf);
    }
    if (buf)
    {
        // Note: reusing osd_op_t WILL currently lead to memory leaks
        // So we don't reuse it, but free it every time
        free(buf);
    }
}

bool osd_op_t::is_recovery_related()
{
    return (req.hdr.opcode == OSD_OP_SEC_READ ||
        req.hdr.opcode == OSD_OP_SEC_WRITE ||
        req.hdr.opcode == OSD_OP_SEC_WRITE_STABLE) &&
        (req.sec_rw.flags & OSD_OP_RECOVERY_RELATED) ||
        req.hdr.opcode == OSD_OP_SEC_DELETE &&
        (req.sec_del.flags & OSD_OP_RECOVERY_RELATED) ||
        req.hdr.opcode == OSD_OP_SEC_STABILIZE &&
        (req.sec_stab.flags & OSD_OP_RECOVERY_RELATED) ||
        req.hdr.opcode == OSD_OP_SEC_SYNC &&
        (req.sec_sync.flags & OSD_OP_RECOVERY_RELATED);
}
