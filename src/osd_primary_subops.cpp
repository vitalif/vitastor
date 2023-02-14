// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "osd_primary.h"

void osd_t::autosync()
{
    if (immediate_commit != IMMEDIATE_ALL && !autosync_op)
    {
        autosync_op = new osd_op_t();
        autosync_op->op_type = OSD_OP_IN;
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
                printf("Warning: automatic sync resulted in an error: %ld (%s)\n", -op->reply.hdr.retval, strerror(-op->reply.hdr.retval));
            }
            delete autosync_op;
            autosync_op = NULL;
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
                inode_stats[cur_op->req.rw.inode].op_bytes[inode_st_op] += cur_op->op_data->pg_data_size * bs_block_size;
        }
        else
            inode_stats[cur_op->req.rw.inode].op_bytes[inode_st_op] += cur_op->req.rw.len;
    }
    if (cur_op->op_data)
    {
        if (cur_op->op_data->pg_num > 0)
        {
            auto & pg = pgs.at({ .pool_id = INODE_POOL(cur_op->op_data->oid.inode), .pg_num = cur_op->op_data->pg_num });
            pg.inflight--;
            assert(pg.inflight >= 0);
            if ((pg.state & PG_STOPPING) && pg.inflight == 0 && !pg.flush_batch)
            {
                finish_stop_pg(pg);
            }
            else if ((pg.state & PG_REPEERING) && pg.inflight == 0 && !pg.flush_batch)
            {
                start_pg_peering(pg);
            }
        }
        assert(!cur_op->op_data->subops);
        free(cur_op->op_data);
        cur_op->op_data = NULL;
    }
    if (!cur_op->peer_fd)
    {
        // Copy lambda to be unaffected by `delete op`
        std::function<void(osd_op_t*)>(cur_op->callback)(cur_op);
    }
    else
    {
        // FIXME add separate magic number for primary ops
        auto cl_it = msgr.clients.find(cur_op->peer_fd);
        if (cl_it != msgr.clients.end())
        {
            cur_op->reply.hdr.magic = SECONDARY_OSD_REPLY_MAGIC;
            cur_op->reply.hdr.id = cur_op->req.hdr.id;
            cur_op->reply.hdr.opcode = cur_op->req.hdr.opcode;
            cur_op->reply.hdr.retval = retval;
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
    bool rep = op_data->scheme == POOL_SCHEME_REPLICATED;
    // Allocate subops
    int n_subops = 0, zero_read = -1;
    for (int role = 0; role < op_data->pg_size; role++)
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
    op_data->done = op_data->errors = op_data->errcode = 0;
    op_data->n_subops = n_subops;
    op_data->subops = subops;
    int sent = submit_primary_subop_batch(submit_type, op_data->oid.inode, op_version, op_data->stripes, osd_set, cur_op, 0, zero_read);
    assert(sent == n_subops);
}

int osd_t::submit_primary_subop_batch(int submit_type, inode_t inode, uint64_t op_version,
    osd_rmw_stripe_t *stripes, const uint64_t* osd_set, osd_op_t *cur_op, int subop_idx, int zero_read)
{
    bool wr = submit_type == SUBMIT_WRITE;
    osd_primary_op_data_t *op_data = cur_op->op_data;
    bool rep = op_data->scheme == POOL_SCHEME_REPLICATED;
    int i = subop_idx;
    for (int role = 0; role < op_data->pg_size; role++)
    {
        // We always submit zero-length writes to all replicas, even if the stripe is not modified
        if (!(wr || !rep && stripes[role].read_end != 0 || zero_read == role))
        {
            continue;
        }
        osd_num_t role_osd_num = osd_set[role];
        int stripe_num = rep ? 0 : role;
        if (role_osd_num != 0)
        {
            osd_op_t *subop = op_data->subops + i;
            uint32_t subop_len = wr
                ? stripes[stripe_num].write_end - stripes[stripe_num].write_start
                : stripes[stripe_num].read_end - stripes[stripe_num].read_start;
            if (!wr && stripes[stripe_num].read_end == UINT32_MAX)
            {
                subop_len = 0;
            }
            stripes[stripe_num].osd_num = role_osd_num;
            stripes[stripe_num].read_error = false;
            subop->bitmap = stripes[stripe_num].bmp_buf;
            subop->bitmap_len = clean_entry_bitmap_size;
            // Using rmw_buf to pass pointer to stripes. Dirty but should work
            subop->rmw_buf = stripes+stripe_num;
            if (role_osd_num == this->osd_num)
            {
                clock_gettime(CLOCK_REALTIME, &subop->tv_begin);
                subop->op_type = (uint64_t)cur_op;
                subop->bs_op = new blockstore_op_t((blockstore_op_t){
                    .opcode = (uint64_t)(wr ? (rep ? BS_OP_WRITE_STABLE : BS_OP_WRITE) : BS_OP_READ),
                    .callback = [subop, this](blockstore_op_t *bs_subop)
                    {
                        handle_primary_bs_subop(subop);
                    },
                    {
                        .oid = (object_id){
                            .inode = inode,
                            .stripe = op_data->oid.stripe | stripe_num,
                        },
                        .version = op_version,
                        .offset = wr ? stripes[stripe_num].write_start : stripes[stripe_num].read_start,
                        .len = subop_len,
                    },
                    .buf = wr ? stripes[stripe_num].write_buf : stripes[stripe_num].read_buf,
                    .bitmap = stripes[stripe_num].bmp_buf,
                });
#ifdef OSD_DEBUG
                printf(
                    "Submit %s to local: %lx:%lx v%lu %u-%u\n", wr ? "write" : "read",
                    inode, op_data->oid.stripe | stripe_num, op_version,
                    subop->bs_op->offset, subop->bs_op->len
                );
#endif
                bs->enqueue_op(subop->bs_op);
            }
            else
            {
                subop->op_type = OSD_OP_OUT;
                subop->req.sec_rw = {
                    .header = {
                        .magic = SECONDARY_OSD_OP_MAGIC,
                        .id = msgr.next_subop_id++,
                        .opcode = (uint64_t)(wr ? (rep ? OSD_OP_SEC_WRITE_STABLE : OSD_OP_SEC_WRITE) : OSD_OP_SEC_READ),
                    },
                    .oid = {
                        .inode = inode,
                        .stripe = op_data->oid.stripe | stripe_num,
                    },
                    .version = op_version,
                    .offset = wr ? stripes[stripe_num].write_start : stripes[stripe_num].read_start,
                    .len = subop_len,
                    .attr_len = wr ? clean_entry_bitmap_size : 0,
                };
#ifdef OSD_DEBUG
                printf(
                    "Submit %s to osd %lu: %lx:%lx v%lu %u-%u\n", wr ? "write" : "read", role_osd_num,
                    inode, op_data->oid.stripe | stripe_num, op_version,
                    subop->req.sec_rw.offset, subop->req.sec_rw.len
                );
#endif
                if (wr)
                {
                    if (stripes[stripe_num].write_end > stripes[stripe_num].write_start)
                    {
                        subop->iov.push_back(stripes[stripe_num].write_buf, stripes[stripe_num].write_end - stripes[stripe_num].write_start);
                    }
                }
                else
                {
                    if (subop_len > 0)
                    {
                        subop->iov.push_back(stripes[stripe_num].read_buf, subop_len);
                    }
                }
                subop->callback = [cur_op, this](osd_op_t *subop)
                {
                    handle_primary_subop(subop, cur_op);
                };
                auto peer_fd_it = msgr.osd_peer_fds.find(role_osd_num);
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
            i++;
        }
        else
        {
            stripes[stripe_num].osd_num = 0;
        }
    }
    return i-subop_idx;
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
    OSD_OP_TEST_SYNC_STAB_ALL,  // BS_OP_SYNC_STAB_ALL = 9
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
        // die on any error except ENOSPC
        throw std::runtime_error(
            "local blockstore modification failed (opcode = "+std::to_string(bs_op->opcode)+
            " retval = "+std::to_string(bs_op->retval)+")"
        );
    }
    add_bs_subop_stats(subop);
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
    subop->peer_fd = -1;
    handle_primary_subop(subop, cur_op);
}

void osd_t::add_bs_subop_stats(osd_op_t *subop)
{
    // Include local blockstore ops in statistics
    uint64_t opcode = bs_op_to_osd_op[subop->bs_op->opcode];
    timespec tv_end;
    clock_gettime(CLOCK_REALTIME, &tv_end);
    msgr.stats.op_stat_count[opcode]++;
    if (!msgr.stats.op_stat_count[opcode])
    {
        msgr.stats.op_stat_count[opcode] = 1;
        msgr.stats.op_stat_sum[opcode] = 0;
        msgr.stats.op_stat_bytes[opcode] = 0;
    }
    msgr.stats.op_stat_sum[opcode] += (
        (tv_end.tv_sec - subop->tv_begin.tv_sec)*1000000 +
        (tv_end.tv_nsec - subop->tv_begin.tv_nsec)/1000
    );
    if (opcode == OSD_OP_SEC_READ || opcode == OSD_OP_SEC_WRITE)
    {
        msgr.stats.op_stat_bytes[opcode] += subop->bs_op->len;
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
    if (retval != expected)
    {
        if (opcode == OSD_OP_SEC_READ || opcode == OSD_OP_SEC_WRITE || opcode == OSD_OP_SEC_WRITE_STABLE)
        {
            printf(
                subop->peer_fd >= 0
                    ? "%1$s subop to %2$lx:%3$lx v%4$lu failed on peer %7$d: retval = %5$d (expected %6$d)\n"
                    : "%1$s subop to %2$lx:%3$lx v%4$lu failed locally: retval = %5$d (expected %6$d)\n",
                osd_op_names[opcode], subop->req.sec_rw.oid.inode, subop->req.sec_rw.oid.stripe, subop->req.sec_rw.version,
                retval, expected, subop->peer_fd
            );
        }
        else
        {
            printf(
                "%s subop failed on peer %d: retval = %d (expected %d)\n",
                osd_op_names[opcode], subop->peer_fd, retval, expected
            );
        }
        if (opcode == OSD_OP_SEC_READ && (retval == -EIO || retval == -EDOM))
        {
            // We'll retry reads from other replica(s) on EIO/EDOM and mark object as corrupted
            ((osd_rmw_stripe_t*)subop->rmw_buf)->read_error = true;
        }
        subop->rmw_buf = NULL;
        // Error priority: EIO > EDOM > ENOSPC > EPIPE
        if (op_data->errcode == 0 ||
            retval == -EIO ||
            retval == -EDOM && (op_data->errcode == -ENOSPC || op_data->errcode == -EPIPE) ||
            retval == -ENOSPC && op_data->errcode == -EPIPE)
        {
            op_data->errcode = retval;
        }
        op_data->errors++;
        if (subop->peer_fd >= 0 && retval != -EDOM &&
            (retval != -ENOSPC || opcode != OSD_OP_SEC_WRITE && opcode != OSD_OP_SEC_WRITE_STABLE) &&
            (retval != -EIO || opcode != OSD_OP_SEC_READ))
        {
            // Drop connection on unexpected errors
            msgr.stop_client(subop->peer_fd);
        }
    }
    else
    {
        subop->rmw_buf = NULL;
        op_data->done++;
        if (opcode == OSD_OP_SEC_READ || opcode == OSD_OP_SEC_WRITE || opcode == OSD_OP_SEC_WRITE_STABLE)
        {
            uint64_t version = subop->reply.sec_rw.version;
#ifdef OSD_DEBUG
            uint64_t peer_osd = msgr.clients.find(subop->peer_fd) != msgr.clients.end()
                ? msgr.clients[subop->peer_fd]->osd_num : osd_num;
            printf("subop %lu from osd %lu: version = %lu\n", opcode, peer_osd, version);
#endif
            if (op_data->fact_ver != UINT64_MAX)
            {
                if (op_data->fact_ver != 0 && op_data->fact_ver != version)
                {
                    throw std::runtime_error(
                        "different fact_versions returned from "+std::string(osd_op_names[opcode])+
                        " subops: "+std::to_string(version)+" vs "+std::to_string(op_data->fact_ver)
                    );
                }
                op_data->fact_ver = version;
            }
        }
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
    bool rep = op_data->scheme == POOL_SCHEME_REPLICATED;
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
                .oid = chunk.oid,
                .version = chunk.version,
            });
            bs->enqueue_op(subops[i].bs_op);
        }
        else
        {
            subops[i].op_type = OSD_OP_OUT;
            subops[i].req = (osd_any_op_t){ .sec_del = {
                .header = {
                    .magic = SECONDARY_OSD_OP_MAGIC,
                    .id = msgr.next_subop_id++,
                    .opcode = OSD_OP_SEC_DELETE,
                },
                .oid = chunk.oid,
                .version = chunk.version,
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
                    .id = msgr.next_subop_id++,
                    .opcode = OSD_OP_SEC_SYNC,
                },
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
                .buf = (void*)(op_data->unstable_writes + stab_osd.start),
            });
            bs->enqueue_op(subops[i].bs_op);
        }
        else
        {
            subops[i].op_type = OSD_OP_OUT;
            subops[i].req = (osd_any_op_t){ .sec_stab = {
                .header = {
                    .magic = SECONDARY_OSD_OP_MAGIC,
                    .id = msgr.next_subop_id++,
                    .opcode = OSD_OP_SEC_STABILIZE,
                },
                .len = (uint64_t)(stab_osd.len * sizeof(obj_ver_id)),
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
