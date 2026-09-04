// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#undef NDEBUG
#include <assert.h>

#include "messenger.h"
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

void osd_messenger_t::measure_exec(osd_op_t *cur_op)
{
    // Measure execution latency
    if (cur_op->req.hdr.opcode > OSD_OP_MAX)
    {
        return;
    }
    if (!cur_op->tv_end.tv_sec)
    {
        clock_gettime(CLOCK_REALTIME, &cur_op->tv_end);
    }
    uint64_t len = 0;
    if (cur_op->req.hdr.opcode == OSD_OP_READ ||
        cur_op->req.hdr.opcode == OSD_OP_WRITE ||
        cur_op->req.hdr.opcode == OSD_OP_SCRUB)
    {
        // req.rw.len is internally set to the full object size for scrubs
        len = cur_op->req.rw.len;
    }
    else if (cur_op->req.hdr.opcode == OSD_OP_SEC_READ ||
        cur_op->req.hdr.opcode == OSD_OP_SEC_WRITE ||
        cur_op->req.hdr.opcode == OSD_OP_SEC_WRITE_STABLE)
    {
        len = cur_op->req.sec_rw.len;
    }
    inc_op_stats(stats, cur_op->req.hdr.opcode, cur_op->tv_begin, cur_op->tv_end, len);
    if (cur_op->is_recovery_related())
    {
        inc_op_stats(recovery_stats, cur_op->req.hdr.opcode, cur_op->tv_begin, cur_op->tv_end, len);
    }
}

void osd_messenger_t::inc_op_stats(osd_op_stats_t & stats, uint64_t opcode, timespec & tv_begin, timespec & tv_end, uint64_t len)
{
    uint64_t usecs = (
        (tv_end.tv_sec - tv_begin.tv_sec)*1000000 +
        (tv_end.tv_nsec - tv_begin.tv_nsec)/1000
    );
    stats.op_stat_count[opcode]++;
    if (!stats.op_stat_count[opcode])
    {
        stats.op_stat_count[opcode] = 1;
        stats.op_stat_sum[opcode] = 0;
        stats.op_stat_bytes[opcode] = 0;
    }
    stats.op_stat_sum[opcode] += usecs;
    stats.op_stat_bytes[opcode] += len;
}
