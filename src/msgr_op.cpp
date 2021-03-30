// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include <assert.h>

#include "msgr_op.h"

osd_op_t::~osd_op_t()
{
    assert(!bs_op);
    assert(!op_data);
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
