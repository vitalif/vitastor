// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "osd_primary.h"

#define SELF_FD -1

void osd_t::autosync()
{
    if (immediate_commit != IMMEDIATE_ALL && !autosync_op)
    {
        if (autosync_copies_to_delete > 0)
        {
            autosync_copies_to_delete--;
        }
        autosync_op = new osd_op_t();
        autosync_op->op_type = OSD_OP_IN;
        autosync_op->peer_fd = SELF_FD;
        autosync_op->req = (osd_any_op_t){
            .sync = {
                .header = {
                    .magic = SECONDARY_OSD_OP_MAGIC,
                    .id = 1,
                    .opcode = OSD_OP_SYNC,
                },
            },
        };
        autosync_op->callback = [this](osd_op_t *op)
        {
            if (op->reply.hdr.retval < 0)
            {
                printf("Warning: automatic sync resulted in an error: %jd (%s)\n", -op->reply.hdr.retval, strerror(-op->reply.hdr.retval));
            }
            delete autosync_op;
            autosync_op = NULL;
            if (autosync_copies_to_delete > 0)
            {
                // Trigger the second "copies_to_delete" autosync
                autosync();
            }
        };
        exec_op(autosync_op);
    }
}

void osd_t::finish_op(osd_op_t *cur_op, int retval)
{
    inflight_ops--;
    if (cur_op->req.hdr.opcode == OSD_OP_READ ||
        cur_op->req.hdr.opcode == OSD_OP_WRITE ||
        cur_op->req.hdr.opcode == OSD_OP_DELETE)
    {
        // Track inode statistics
        if (!cur_op->tv_end.tv_sec)
        {
            clock_gettime(CLOCK_REALTIME, &cur_op->tv_end);
        }
        uint64_t usec = (
            (cur_op->tv_end.tv_sec - cur_op->tv_begin.tv_sec)*1000000 +
            (cur_op->tv_end.tv_nsec - cur_op->tv_begin.tv_nsec)/1000
        );
        int inode_st_op = cur_op->req.hdr.opcode == OSD_OP_DELETE
            ? INODE_STATS_DELETE
            : (cur_op->req.hdr.opcode == OSD_OP_READ ? INODE_STATS_READ : INODE_STATS_WRITE);
        inode_stats[cur_op->req.rw.inode].op_count[inode_st_op]++;
        inode_stats[cur_op->req.rw.inode].op_sum[inode_st_op] += usec;
        if (cur_op->req.hdr.opcode == OSD_OP_DELETE)
        {
            if (cur_op->op_data)
            {
                inode_stats[cur_op->req.rw.inode].op_bytes[inode_st_op] += (cur_op->op_data->pg
                    ? cur_op->op_data->pg->pg_data_size : 1) * bs_block_size;
            }
        }
        else
            inode_stats[cur_op->req.rw.inode].op_bytes[inode_st_op] += cur_op->req.rw.len;
    }
    if (cur_op->op_data)
    {
        if (cur_op->op_data->pg)
        {
            auto & pg = *cur_op->op_data->pg;
            rm_inflight(pg);
        }
        assert(!cur_op->op_data->subops);
        free(cur_op->op_data);
        cur_op->op_data = NULL;
    }
    cur_op->reply.hdr.magic = SECONDARY_OSD_REPLY_MAGIC;
    cur_op->reply.hdr.id = cur_op->req.hdr.id;
    cur_op->reply.hdr.opcode = cur_op->req.hdr.opcode;
    cur_op->reply.hdr.retval = retval;
    if (cur_op->peer_fd == SELF_FD)
    {
        // Do not include internal primary writes (recovery/rebalance) into client op statistics
        if (cur_op->req.hdr.opcode != OSD_OP_WRITE)
        {
            msgr.measure_exec(cur_op);
        }
        // Copy lambda to be unaffected by `delete op`
        std::function<void(osd_op_t*)>(cur_op->callback)(cur_op);
    }
    else
    {
        // FIXME add separate magic number for primary ops
        auto cl_it = msgr.clients.find(cur_op->peer_fd);
        if (cl_it != msgr.clients.end())
        {
            msgr.outbox_push(cur_op);
        }
        else
        {
            delete cur_op;
        }
    }
}

void osd_t::submit_primary_subops(int submit_type, uint64_t op_version, const uint64_t* osd_set, osd_op_t *cur_op)
{
    bool wr = submit_type == SUBMIT_WRITE;
    osd_primary_op_data_t *op_data = cur_op->op_data;
    osd_rmw_stripe_t *stripes = op_data->stripes;
    bool rep = !op_data->pg || op_data->pg->scheme == POOL_SCHEME_REPLICATED;
    // Allocate subops
    int n_subops = 0, zero_read = -1;
    for (int role = 0; role < (op_data->pg ? op_data->pg->pg_size : 1); role++)
    {
        if (osd_set[role] == this->osd_num || osd_set[role] != 0 && zero_read == -1)
            zero_read = role;
        if (osd_set[role] != 0 && (wr || !rep && stripes[role].read_end != 0))
            n_subops++;
    }
    if (!n_subops && (submit_type == SUBMIT_RMW_READ || rep))
        n_subops = 1;
    else
        zero_read = -1;
    osd_op_t *subops = new osd_op_t[n_subops];
    op_data->fact_ver = 0;
    op_data->done = op_data->errors = op_data->drops = op_data->errcode = 0;
    op_data->n_subops = n_subops;
    op_data->subops = subops;
    int sent = submit_primary_subop_batch(submit_type, op_data->oid.inode, op_version, op_data->stripes, osd_set, cur_op, 0, zero_read);
    assert(sent == n_subops);
}

int osd_t::submit_primary_subop_batch(int submit_type, inode_t inode, uint64_t op_version,
    osd_rmw_stripe_t *stripes, const uint64_t* osd_set, osd_op_t *cur_op, int subop_idx, int zero_read)
{
    bool rep = !cur_op->op_data->pg || cur_op->op_data->pg->scheme == POOL_SCHEME_REPLICATED;
    bool wr = submit_type == SUBMIT_WRITE;
    osd_primary_op_data_t *op_data = cur_op->op_data;
    int i = subop_idx;
    for (int role = 0; role < (op_data->pg ? op_data->pg->pg_size : 1); role++)
    {
        // We always submit zero-length writes to all replicas, even if the stripe is not modified
        if (!(wr || !rep && stripes[role].read_end != 0 || zero_read == role || submit_type == SUBMIT_SCRUB_READ))
        {
            continue;
        }
        osd_num_t role_osd_num = osd_set[role];
        int stripe_num = rep ? 0 : role;
        osd_rmw_stripe_t *si = stripes + (submit_type == SUBMIT_SCRUB_READ ? role : stripe_num);
        if (role_osd_num != 0)
        {
            si->osd_num = role_osd_num;
            si->role = stripe_num;
            submit_primary_subop(cur_op, &op_data->subops[i], si, wr, inode, op_version);
            i++;
        }
        else
        {
            si->osd_num = 0;
        }
    }
    return i-subop_idx;
}

void osd_t::submit_primary_subop(osd_op_t *cur_op, osd_op_t *subop,
    osd_rmw_stripe_t *si, bool wr, inode_t inode, uint64_t op_version)
{
    uint32_t subop_len = wr
        ? si->write_end - si->write_start
        : si->read_end - si->read_start;
    if (!wr && si->read_end == UINT32_MAX)
    {
        subop_len = 0;
    }
    si->read_error = false;
    subop->bitmap = si->bmp_buf;
    subop->bitmap_len = clean_entry_bitmap_size;
    // Using rmw_buf to pass pointer to stripes. Dirty but works
    subop->rmw_buf = si;
    if (si->osd_num == this->osd_num)
    {
        clock_gettime(CLOCK_REALTIME, &subop->tv_begin);
        subop->op_type = (uint64_t)cur_op; // also dirty
        subop->bs_op = new blockstore_op_t((blockstore_op_t){
            .opcode = (uint64_t)(wr ? (cur_op->op_data->pg->scheme == POOL_SCHEME_REPLICATED ? BS_OP_WRITE_STABLE : BS_OP_WRITE) : BS_OP_READ),
            .callback = [subop, this](blockstore_op_t *bs_subop)
            {
                handle_primary_bs_subop(subop);
            },
            { {
                .oid = (object_id){
                    .inode = inode,
                    .stripe = cur_op->op_data->oid.stripe | si->role,
                },
                .version = op_version,
                .offset = wr ? si->write_start : si->read_start,
                .len = subop_len,
            } },
            .buf = (uint8_t*)(wr ? si->write_buf : si->read_buf),
            .bitmap = (uint8_t*)si->bmp_buf,
        });
#ifdef OSD_DEBUG
         printf(
             "Submit %s to local: %jx:%jx v%ju %u-%u\n", wr ? "write" : "read",
             inode, op_data->oid.stripe | si->role, op_version,
             subop->bs_op->offset, subop->bs_op->len
         );
#endif
        bs->enqueue_op(subop->bs_op);
    }
    else
    {
        subop->op_type = OSD_OP_OUT;
        subop->req.sec_rw = (osd_op_sec_rw_t){
            .header = {
                .magic = SECONDARY_OSD_OP_MAGIC,
                .opcode = (uint64_t)(wr ? (cur_op->op_data->pg->scheme == POOL_SCHEME_REPLICATED ? OSD_OP_SEC_WRITE_STABLE : OSD_OP_SEC_WRITE) : OSD_OP_SEC_READ),
            },
            .oid = {
                .inode = inode,
                .stripe = cur_op->op_data->oid.stripe | si->role,
            },
            .version = op_version,
            .offset = wr ? si->write_start : si->read_start,
            .len = subop_len,
            .attr_len = wr ? clean_entry_bitmap_size : 0,
            .flags = cur_op->peer_fd == SELF_FD && cur_op->req.hdr.opcode != OSD_OP_SCRUB ? OSD_OP_RECOVERY_RELATED : 0,
        };
#ifdef OSD_DEBUG
        printf(
            "Submit %s to osd %ju: %jx:%jx v%ju %u-%u\n", wr ? "write" : "read", si->osd_num,
            inode, op_data->oid.stripe | si->role, op_version,
            subop->req.sec_rw.offset, subop->req.sec_rw.len
        );
#endif
        if (wr)
        {
            if (si->write_end > si->write_start)
            {
                subop->iov.push_back(si->write_buf, si->write_end - si->write_start);
            }
        }
        else
        {
            if (subop_len > 0)
            {
                subop->iov.push_back(si->read_buf, subop_len);
            }
        }
        subop->callback = [cur_op, this](osd_op_t *subop)
        {
            handle_primary_subop(subop, cur_op);
        };
        auto peer_fd_it = msgr.osd_peer_fds.find(si->osd_num);
        if (peer_fd_it != msgr.osd_peer_fds.end())
        {
            subop->peer_fd = peer_fd_it->second;
            msgr.outbox_push(subop);
        }
        else
        {
            // Fail it immediately
            subop->peer_fd = -1;
            subop->reply.hdr.retval = -EPIPE;
            ringloop->set_immediate([subop]() { std::function<void(osd_op_t*)>(subop->callback)(subop); });
        }
    }
}

static uint64_t bs_op_to_osd_op[] = {
    0,
    OSD_OP_SEC_READ,            // BS_OP_READ = 1
    OSD_OP_SEC_WRITE,           // BS_OP_WRITE = 2
    OSD_OP_SEC_WRITE_STABLE,    // BS_OP_WRITE_STABLE = 3
    OSD_OP_SEC_SYNC,            // BS_OP_SYNC = 4
    OSD_OP_SEC_STABILIZE,       // BS_OP_STABLE = 5
    OSD_OP_SEC_DELETE,          // BS_OP_DELETE = 6
    OSD_OP_SEC_LIST,            // BS_OP_LIST = 7
    OSD_OP_SEC_ROLLBACK,        // BS_OP_ROLLBACK = 8
};

void osd_t::handle_primary_bs_subop(osd_op_t *subop)
{
    osd_op_t *cur_op = (osd_op_t*)subop->op_type;
    blockstore_op_t *bs_op = subop->bs_op;
    int expected = bs_op->opcode == BS_OP_READ || bs_op->opcode == BS_OP_WRITE
        || bs_op->opcode == BS_OP_WRITE_STABLE ? bs_op->len : 0;
    if (bs_op->retval != expected && bs_op->opcode != BS_OP_READ &&
        (bs_op->opcode != BS_OP_WRITE && bs_op->opcode != BS_OP_WRITE_STABLE ||
        bs_op->retval != -ENOSPC))
    {
        // die on any error except ENOSPC during write
        if (bs_op->opcode == BS_OP_WRITE || bs_op->opcode == BS_OP_WRITE_STABLE)
        {
            printf(
                "%s subop to %jx:%jx v%ju failed locally: retval = %d (expected %d)\n",
                osd_op_names[bs_op_to_osd_op[bs_op->opcode]],
                bs_op->oid.inode, bs_op->oid.stripe, bs_op->version, bs_op->retval, expected
            );
        }
        else
        {
            printf(
                "%s subop failed locally: retval = %d (expected %d)\n",
                osd_op_names[bs_op_to_osd_op[bs_op->opcode]], bs_op->retval, expected
            );
        }
        throw std::runtime_error("local blockstore modification failed");
    }
    bool recovery_related = cur_op->peer_fd == SELF_FD && cur_op->req.hdr.opcode != OSD_OP_SCRUB;
    add_bs_subop_stats(subop, recovery_related);
    subop->req.hdr.opcode = bs_op_to_osd_op[bs_op->opcode];
    subop->reply.hdr.retval = bs_op->retval;
    if (bs_op->opcode == BS_OP_READ || bs_op->opcode == BS_OP_WRITE || bs_op->opcode == BS_OP_WRITE_STABLE)
    {
        subop->req.sec_rw.oid = bs_op->oid;
        subop->req.sec_rw.version = bs_op->version;
        subop->req.sec_rw.len = bs_op->len;
        subop->reply.sec_rw.version = bs_op->version;
    }
    delete bs_op;
    subop->bs_op = NULL;
    subop->peer_fd = SELF_FD;
    if (recovery_related && recovery_target_sleep_us)
    {
        tfd->set_timer_us(recovery_target_sleep_us, false, [=](int timer_id)
        {
            handle_primary_subop(subop, cur_op);
        });
    }
    else
    {
        handle_primary_subop(subop, cur_op);
    }
}

void osd_t::add_bs_subop_stats(osd_op_t *subop, bool recovery_related)
{
    // Include local blockstore ops in statistics
    uint64_t opcode = bs_op_to_osd_op[subop->bs_op->opcode];
    timespec tv_end;
    clock_gettime(CLOCK_REALTIME, &tv_end);
    uint64_t len = (opcode == OSD_OP_SEC_READ || opcode == OSD_OP_SEC_WRITE)
        ? subop->bs_op->len : 0;
    msgr.inc_op_stats(msgr.stats, opcode, subop->tv_begin, tv_end, len);
    if (recovery_related)
    {
        // It is OSD_OP_RECOVERY_RELATED
        msgr.inc_op_stats(msgr.recovery_stats, opcode, subop->tv_begin, tv_end, len);
    }
}

void osd_t::handle_primary_subop(osd_op_t *subop, osd_op_t *cur_op)
{
    uint64_t opcode = subop->req.hdr.opcode;
    int retval = subop->reply.hdr.retval;
    int expected;
    if (opcode == OSD_OP_SEC_READ || opcode == OSD_OP_SEC_WRITE || opcode == OSD_OP_SEC_WRITE_STABLE)
        expected = subop->req.sec_rw.len;
    else if (opcode == OSD_OP_SEC_READ_BMP)
        expected = subop->req.sec_read_bmp.len / sizeof(obj_ver_id) * (8 + clean_entry_bitmap_size);
    else
        expected = 0;
    osd_primary_op_data_t *op_data = cur_op->op_data;
    if (retval == -ENOENT && opcode == OSD_OP_SEC_READ)
    {
        // ENOENT is not an error for almost all reads, except scrub
        retval = expected;
        memset(((osd_rmw_stripe_t*)subop->rmw_buf)->read_buf, 0, expected);
        ((osd_rmw_stripe_t*)subop->rmw_buf)->not_exists = true;
    }
    if (opcode == OSD_OP_SEC_READ && (retval == -EIO || retval == -EDOM) ||
        opcode == OSD_OP_SEC_WRITE && retval != expected)
    {
        // We'll retry reads from other replica(s) on EIO/EDOM and mark object as corrupted
        // And we'll mark write as failed
        ((osd_rmw_stripe_t*)subop->rmw_buf)->read_error = true;
    }
    if (retval == expected && (opcode == OSD_OP_SEC_READ || opcode == OSD_OP_SEC_WRITE || opcode == OSD_OP_SEC_WRITE_STABLE))
    {
        uint64_t version = subop->reply.sec_rw.version;
#ifdef OSD_DEBUG
        int64_t peer_osd = subop->peer_fd == SELF_FD ? osd_num :
            (msgr.clients.find(subop->peer_fd) != msgr.clients.end()
                ? msgr.clients[subop->peer_fd]->osd_num : -subop->peer_fd);
        printf("subop %s %jx:%jx from osd %jd: version = %ju\n", osd_op_names[opcode],
            subop->req.sec_rw.oid.inode, subop->req.sec_rw.oid.stripe, peer_osd, version);
#endif
        if (version != 0 && op_data->fact_ver != UINT64_MAX)
        {
            if (op_data->fact_ver != 0 && op_data->fact_ver != version)
            {
                fprintf(
                    stderr, "different fact_versions returned from %s subops: %ju vs %ju\n",
                    osd_op_names[opcode], version, op_data->fact_ver
                );
                retval = -ERANGE;
            }
            else
                op_data->fact_ver = version;
        }
    }
    if (retval != expected)
    {
        int64_t peer_osd = (msgr.clients.find(subop->peer_fd) != msgr.clients.end()
            ? msgr.clients[subop->peer_fd]->osd_num : 0);
        if (opcode == OSD_OP_SEC_READ || opcode == OSD_OP_SEC_WRITE || opcode == OSD_OP_SEC_WRITE_STABLE)
        {
            printf("%s subop to %jx:%jx v%ju failed ", osd_op_names[opcode],
                subop->req.sec_rw.oid.inode, subop->req.sec_rw.oid.stripe, subop->req.sec_rw.version);
            if (subop->peer_fd >= 0 && peer_osd > 0)
                printf("on osd %ju: retval = %d (expected %d)\n", peer_osd, retval, expected);
            else if (peer_osd > 0)
                printf("on peer %d: retval = %d (expected %d)\n", subop->peer_fd, retval, expected);
            else
                printf("locally: retval = %d (expected %d)\n", retval, expected);
        }
        else if (opcode == OSD_OP_SEC_DELETE)
        {
            printf(
                "delete subop to %jx:%jx v%ju failed on osd %jd: retval = %d (expected %d)\n",
                subop->req.sec_del.oid.inode, subop->req.sec_del.oid.stripe, subop->req.sec_del.version,
                peer_osd, retval, expected
            );
        }
        else
        {
            printf(
                "%s subop failed on osd %jd: retval = %d (expected %d)\n",
                osd_op_names[opcode], peer_osd, retval, expected
            );
        }
        subop->rmw_buf = NULL;
        // Error priority: ENOSPC > others > EIO > EDOM > EPIPE
        if (op_data->errcode == 0 ||
            retval == -ENOSPC && op_data->errcode != -ENOSPC ||
            retval == -EIO && (op_data->errcode == -EDOM || op_data->errcode == -EPIPE) ||
            retval == -EDOM && (op_data->errcode == -EPIPE) ||
            retval != -EIO && retval != -EDOM && retval != -EPIPE)
        {
            op_data->errcode = retval;
        }
        if (subop->peer_fd >= 0 && retval != -EDOM && retval != -ERANGE &&
            (retval != -ENOSPC || opcode != OSD_OP_SEC_WRITE && opcode != OSD_OP_SEC_WRITE_STABLE) &&
            (retval != -EIO || opcode != OSD_OP_SEC_READ))
        {
            // Drop connection on unexpected errors
            msgr.stop_client(subop->peer_fd);
            op_data->drops++;
        }
        // Increase op_data->errors after stop_client to prevent >= n_subops running twice
        op_data->errors++;
    }
    else
    {
        subop->rmw_buf = NULL;
        op_data->done++;
    }
    if ((op_data->errors + op_data->done) >= op_data->n_subops)
    {
        delete[] op_data->subops;
        op_data->subops = NULL;
        op_data->st++;
        if (cur_op->req.hdr.opcode == OSD_OP_READ)
        {
            continue_primary_read(cur_op);
        }
        else if (cur_op->req.hdr.opcode == OSD_OP_WRITE)
        {
            continue_primary_write(cur_op);
        }
        else if (cur_op->req.hdr.opcode == OSD_OP_SYNC)
        {
            continue_primary_sync(cur_op);
        }
        else if (cur_op->req.hdr.opcode == OSD_OP_DELETE)
        {
            continue_primary_del(cur_op);
        }
        else if (cur_op->req.hdr.opcode == OSD_OP_SCRUB)
        {
            continue_primary_scrub(cur_op);
        }
        else
        {
            throw std::runtime_error("BUG: unknown opcode");
        }
    }
}

void osd_t::cancel_primary_write(osd_op_t *cur_op)
{
    if (cur_op->op_data && cur_op->op_data->subops)
    {
        // Primary-write operation is waiting for subops, subops
        // are sent to peer OSDs, so we can't just throw them away.
        // Mark them with an extra EPIPE.
        cur_op->op_data->errors++;
        if (cur_op->op_data->errcode == 0)
            cur_op->op_data->errcode = -EPIPE;
        cur_op->op_data->done--; // Caution: `done` must be signed because may become -1 here
    }
    else
    {
        finish_op(cur_op, -EPIPE);
    }
}

bool contains_osd(osd_num_t *osd_set, uint64_t size, osd_num_t osd_num)
{
    for (uint64_t i = 0; i < size; i++)
    {
        if (osd_set[i] == osd_num)
        {
            return true;
        }
    }
    return false;
}

void osd_t::submit_primary_del_subops(osd_op_t *cur_op, osd_num_t *cur_set, uint64_t set_size, pg_osd_set_t & loc_set)
{
    osd_primary_op_data_t *op_data = cur_op->op_data;
    bool rep = op_data->pg->scheme == POOL_SCHEME_REPLICATED;
    obj_ver_osd_t extra_chunks[loc_set.size()];
    int chunks_to_del = 0;
    for (auto & chunk: loc_set)
    {
        // ordered comparison for EC/XOR, unordered for replicated pools
        if (!cur_set || (rep
            ? !contains_osd(cur_set, set_size, chunk.osd_num)
            : (chunk.osd_num != cur_set[chunk.role])))
        {
            extra_chunks[chunks_to_del++] = (obj_ver_osd_t){
                .osd_num = chunk.osd_num,
                .oid = {
                    .inode = op_data->oid.inode,
                    .stripe = op_data->oid.stripe | (rep ? 0 : chunk.role),
                },
                // Same version as write
                .version = op_data->fact_ver,
            };
        }
    }
    submit_primary_del_batch(cur_op, extra_chunks, chunks_to_del);
}

void osd_t::submit_primary_del_batch(osd_op_t *cur_op, obj_ver_osd_t *chunks_to_delete, int chunks_to_delete_count)
{
    osd_primary_op_data_t *op_data = cur_op->op_data;
    op_data->n_subops = chunks_to_delete_count;
    op_data->done = op_data->errors = op_data->errcode = 0;
    if (op_data->n_subops <= 0)
    {
        return;
    }
    osd_op_t *subops = new osd_op_t[chunks_to_delete_count];
    op_data->subops = subops;
    for (int i = 0; i < chunks_to_delete_count; i++)
    {
        auto & chunk = chunks_to_delete[i];
        if (chunk.osd_num == this->osd_num)
        {
            clock_gettime(CLOCK_REALTIME, &subops[i].tv_begin);
            subops[i].op_type = (uint64_t)cur_op;
            subops[i].bs_op = new blockstore_op_t({
                .opcode = BS_OP_DELETE,
                .callback = [subop = &subops[i], this](blockstore_op_t *bs_subop)
                {
                    handle_primary_bs_subop(subop);
                },
                { {
                    .oid = chunk.oid,
                    .version = chunk.version,
                } },
            });
            bs->enqueue_op(subops[i].bs_op);
        }
        else
        {
            subops[i].op_type = OSD_OP_OUT;
            subops[i].req = (osd_any_op_t){ .sec_del = {
                .header = {
                    .magic = SECONDARY_OSD_OP_MAGIC,
                    .opcode = OSD_OP_SEC_DELETE,
                },
                .oid = chunk.oid,
                .version = chunk.version,
                .flags = cur_op->peer_fd == SELF_FD && cur_op->req.hdr.opcode != OSD_OP_SCRUB ? OSD_OP_RECOVERY_RELATED : 0,
            } };
            subops[i].callback = [cur_op, this](osd_op_t *subop)
            {
                handle_primary_subop(subop, cur_op);
            };
            auto peer_fd_it = msgr.osd_peer_fds.find(chunk.osd_num);
            if (peer_fd_it != msgr.osd_peer_fds.end())
            {
                subops[i].peer_fd = peer_fd_it->second;
                msgr.outbox_push(&subops[i]);
            }
            else
            {
                // Fail it immediately
                subops[i].peer_fd = -1;
                subops[i].reply.hdr.retval = -EPIPE;
                ringloop->set_immediate([subop = &subops[i]]() { std::function<void(osd_op_t*)>(subop->callback)(subop); });
            }
        }
    }
}

int osd_t::submit_primary_sync_subops(osd_op_t *cur_op)
{
    osd_primary_op_data_t *op_data = cur_op->op_data;
    int n_osds = op_data->dirty_osd_count;
    osd_op_t *subops = new osd_op_t[n_osds];
    op_data->done = op_data->errors = op_data->errcode = 0;
    op_data->n_subops = n_osds;
    op_data->subops = subops;
    std::map<uint64_t, int>::iterator peer_it;
    for (int i = 0; i < n_osds; i++)
    {
        osd_num_t sync_osd = op_data->dirty_osds[i];
        if (sync_osd == this->osd_num)
        {
            clock_gettime(CLOCK_REALTIME, &subops[i].tv_begin);
            subops[i].op_type = (uint64_t)cur_op;
            subops[i].bs_op = new blockstore_op_t({
                .opcode = BS_OP_SYNC,
                .callback = [subop = &subops[i], this](blockstore_op_t *bs_subop)
                {
                    handle_primary_bs_subop(subop);
                },
            });
            bs->enqueue_op(subops[i].bs_op);
        }
        else if ((peer_it = msgr.osd_peer_fds.find(sync_osd)) != msgr.osd_peer_fds.end())
        {
            subops[i].op_type = OSD_OP_OUT;
            subops[i].peer_fd = peer_it->second;
            subops[i].req = (osd_any_op_t){ .sec_sync = {
                .header = {
                    .magic = SECONDARY_OSD_OP_MAGIC,
                    .opcode = OSD_OP_SEC_SYNC,
                },
                .flags = cur_op->peer_fd == SELF_FD && cur_op->req.hdr.opcode != OSD_OP_SCRUB ? OSD_OP_RECOVERY_RELATED : 0,
            } };
            subops[i].callback = [cur_op, this](osd_op_t *subop)
            {
                handle_primary_subop(subop, cur_op);
            };
            msgr.outbox_push(&subops[i]);
        }
        else
        {
            op_data->done++;
        }
    }
    if (op_data->done >= op_data->n_subops)
    {
        delete[] op_data->subops;
        op_data->subops = NULL;
        return 0;
    }
    return 1;
}

void osd_t::submit_primary_stab_subops(osd_op_t *cur_op)
{
    osd_primary_op_data_t *op_data = cur_op->op_data;
    int n_osds = op_data->unstable_write_osds->size();
    osd_op_t *subops = new osd_op_t[n_osds];
    op_data->done = op_data->errors = op_data->errcode = 0;
    op_data->n_subops = n_osds;
    op_data->subops = subops;
    for (int i = 0; i < n_osds; i++)
    {
        auto & stab_osd = (*(op_data->unstable_write_osds))[i];
        if (stab_osd.osd_num == this->osd_num)
        {
            clock_gettime(CLOCK_REALTIME, &subops[i].tv_begin);
            subops[i].op_type = (uint64_t)cur_op;
            subops[i].bs_op = new blockstore_op_t((blockstore_op_t){
                .opcode = BS_OP_STABLE,
                .callback = [subop = &subops[i], this](blockstore_op_t *bs_subop)
                {
                    handle_primary_bs_subop(subop);
                },
                {
                    .len = (uint32_t)stab_osd.len,
                },
                .buf = (uint8_t*)(op_data->unstable_writes + stab_osd.start),
            });
            bs->enqueue_op(subops[i].bs_op);
        }
        else
        {
            subops[i].op_type = OSD_OP_OUT;
            subops[i].req = (osd_any_op_t){ .sec_stab = {
                .header = {
                    .magic = SECONDARY_OSD_OP_MAGIC,
                    .opcode = OSD_OP_SEC_STABILIZE,
                },
                .len = (uint64_t)(stab_osd.len * sizeof(obj_ver_id)),
                .flags = cur_op->peer_fd == SELF_FD && cur_op->req.hdr.opcode != OSD_OP_SCRUB ? OSD_OP_RECOVERY_RELATED : 0,
            } };
            subops[i].iov.push_back(op_data->unstable_writes + stab_osd.start, stab_osd.len * sizeof(obj_ver_id));
            subops[i].callback = [cur_op, this](osd_op_t *subop)
            {
                handle_primary_subop(subop, cur_op);
            };
            auto peer_fd_it = msgr.osd_peer_fds.find(stab_osd.osd_num);
            if (peer_fd_it != msgr.osd_peer_fds.end())
            {
                subops[i].peer_fd = peer_fd_it->second;
                msgr.outbox_push(&subops[i]);
            }
            else
            {
                // Fail it immediately
                subops[i].peer_fd = -1;
                subops[i].reply.hdr.retval = -EPIPE;
                ringloop->set_immediate([subop = &subops[i]]() { std::function<void(osd_op_t*)>(subop->callback)(subop); });
            }
        }
    }
}

void osd_t::submit_primary_rollback_subops(osd_op_t *cur_op, const uint64_t* osd_set)
{
    osd_primary_op_data_t *op_data = cur_op->op_data;
    osd_rmw_stripe_t *stripes = op_data->stripes;
    assert(op_data->pg->scheme != POOL_SCHEME_REPLICATED);
    // Allocate subops
    int n_subops = 0;
    for (int role = 0; role < op_data->pg->pg_size; role++)
    {
        if (osd_set[role] != 0 && !stripes[role].read_error &&
            (osd_set[role] == this->osd_num || msgr.osd_peer_fds.find(osd_set[role]) != msgr.osd_peer_fds.end()))
        {
            n_subops++;
        }
    }
    op_data->n_subops = n_subops;
    op_data->done = op_data->errors = 0;
    if (!op_data->n_subops)
    {
        return;
    }
    op_data->subops = new osd_op_t[n_subops];
    op_data->unstable_writes = new obj_ver_id[n_subops];
    int i = 0;
    for (int role = 0; role < op_data->pg->pg_size; role++)
    {
        if (osd_set[role] != 0 && !stripes[role].read_error &&
            (osd_set[role] == this->osd_num || msgr.osd_peer_fds.find(osd_set[role]) != msgr.osd_peer_fds.end()))
        {
            osd_op_t *subop = &op_data->subops[i];
            op_data->unstable_writes[i] = (obj_ver_id){
                .oid = {
                    .inode = op_data->oid.inode,
                    .stripe = op_data->oid.stripe | role,
                },
                .version = op_data->target_ver-1,
            };
            if (osd_set[role] == this->osd_num)
            {
                clock_gettime(CLOCK_REALTIME, &subop->tv_begin);
                subop->op_type = (uint64_t)cur_op;
                subop->bs_op = new blockstore_op_t((blockstore_op_t){
                    .opcode = BS_OP_ROLLBACK,
                    .callback = [subop, this](blockstore_op_t *bs_subop)
                    {
                        handle_primary_bs_subop(subop);
                    },
                    {
                        .len = 1,
                    },
                    .buf = (uint8_t*)(op_data->unstable_writes + i),
                });
#ifdef OSD_DEBUG
                printf(
                    "Submit rollback to local: %jx:%jx v%ju\n",
                    op_data->oid.inode, op_data->oid.stripe | role, op_data->target_ver-1
                );
#endif
                bs->enqueue_op(subop->bs_op);
            }
            else
            {
                subop->op_type = OSD_OP_OUT;
                subop->req = (osd_any_op_t){ .sec_stab = {
                    .header = {
                        .magic = SECONDARY_OSD_OP_MAGIC,
                        .opcode = OSD_OP_SEC_ROLLBACK,
                    },
                    .len = sizeof(obj_ver_id),
                } };
                subop->iov.push_back(op_data->unstable_writes + i, sizeof(obj_ver_id));
                subop->callback = [cur_op, this](osd_op_t *subop)
                {
                    handle_primary_subop(subop, cur_op);
                };
#ifdef OSD_DEBUG
                printf(
                    "Submit rollback to osd %ju: %jx:%jx v%ju\n", osd_set[role],
                    op_data->oid.inode, op_data->oid.stripe | role, op_data->target_ver-1
                );
#endif
                subop->peer_fd = msgr.osd_peer_fds.at(osd_set[role]);
                msgr.outbox_push(subop);
            }
            i++;
        }
    }
}

void osd_t::pg_cancel_write_queue(pg_t & pg, osd_op_t *first_op, object_id oid, int retval)
{
    auto st_it = pg.write_queue.find(oid), it = st_it;
    if (it == pg.write_queue.end() || it->second != first_op)
    {
        // Write queue doesn't match the first operation.
        // first_op is a leftover operation from the previous peering of the same PG.
        finish_op(first_op, retval);
        return;
    }
    std::vector<osd_op_t*> cancel_ops;
    while (it != pg.write_queue.end() && it->first == oid)
    {
        cancel_ops.push_back(it->second);
        it++;
    }
    if (st_it != it)
    {
        // First erase them and then run finish_op() for the sake of reenterability
        // Calling finish_op() on a live iterator previously triggered a bug where some
        // of the OSDs were looping infinitely if you stopped all of them with kill -INT during recovery
        pg.write_queue.erase(st_it, it);
        for (auto op: cancel_ops)
        {
            finish_op(op, retval);
        }
    }
}
