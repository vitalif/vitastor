#include "osd.h"

#define FLUSH_BATCH 512

void osd_t::submit_pg_flush_ops(pg_num_t pg_num)
{
    pg_t & pg = pgs[pg_num];
    pg_flush_batch_t *fb = new pg_flush_batch_t();
    pg.flush_batch = fb;
    auto it = pg.flush_actions.begin(), prev_it = pg.flush_actions.begin();
    bool first = true;
    while (it != pg.flush_actions.end())
    {
        if (!first && (it->first.oid.inode != prev_it->first.oid.inode ||
            (it->first.oid.stripe & ~STRIPE_MASK) != (prev_it->first.oid.stripe & ~STRIPE_MASK)) &&
            fb->rollback_lists[it->first.osd_num].size() >= FLUSH_BATCH ||
            fb->stable_lists[it->first.osd_num].size() >= FLUSH_BATCH)
        {
            // Stop only at the object boundary
            break;
        }
        it->second.submitted = true;
        if (it->second.rollback)
        {
            fb->flush_objects++;
            fb->rollback_lists[it->first.osd_num].push_back((obj_ver_id){
                .oid = it->first.oid,
                .version = it->second.rollback_to,
            });
        }
        if (it->second.make_stable)
        {
            fb->flush_objects++;
            fb->stable_lists[it->first.osd_num].push_back((obj_ver_id){
                .oid = it->first.oid,
                .version = it->second.stable_to,
            });
        }
        prev_it = it;
        first = false;
        it++;
    }
    for (auto & l: fb->rollback_lists)
    {
        if (l.second.size() > 0)
        {
            fb->flush_ops++;
            submit_flush_op(pg.pg_num, fb, true, l.first, l.second.size(), l.second.data());
        }
    }
    for (auto & l: fb->stable_lists)
    {
        if (l.second.size() > 0)
        {
            fb->flush_ops++;
            submit_flush_op(pg.pg_num, fb, false, l.first, l.second.size(), l.second.data());
        }
    }
}

void osd_t::handle_flush_op(bool rollback, pg_num_t pg_num, pg_flush_batch_t *fb, osd_num_t peer_osd, int retval)
{
    if (pgs.find(pg_num) == pgs.end() || pgs[pg_num].flush_batch != fb)
    {
        // Throw the result away
        return;
    }
    if (retval != 0)
    {
        if (peer_osd == this->osd_num)
        {
            throw std::runtime_error(
                std::string(rollback
                    ? "Error while doing local rollback operation: "
                    : "Error while doing local stabilize operation: "
                ) + strerror(-retval)
            );
        }
        else
        {
            printf("Error while doing flush on OSD %lu: %d (%s)\n", osd_num, retval, strerror(-retval));
            auto fd_it = c_cli.osd_peer_fds.find(peer_osd);
            if (fd_it != c_cli.osd_peer_fds.end())
            {
                c_cli.stop_client(fd_it->second);
            }
            return;
        }
    }
    fb->flush_done++;
    if (fb->flush_done == fb->flush_ops)
    {
        // This flush batch is done
        std::vector<osd_op_t*> continue_ops;
        auto & pg = pgs[pg_num];
        auto it = pg.flush_actions.begin(), prev_it = it;
        auto erase_start = it;
        while (1)
        {
            if (it == pg.flush_actions.end() ||
                it->first.oid.inode != prev_it->first.oid.inode ||
                (it->first.oid.stripe & ~STRIPE_MASK) != (prev_it->first.oid.stripe & ~STRIPE_MASK))
            {
                pg.ver_override.erase((object_id){
                    .inode = prev_it->first.oid.inode,
                    .stripe = (prev_it->first.oid.stripe & ~STRIPE_MASK),
                });
                auto wr_it = pg.write_queue.find((object_id){
                    .inode = prev_it->first.oid.inode,
                    .stripe = (prev_it->first.oid.stripe & ~STRIPE_MASK),
                });
                if (wr_it != pg.write_queue.end())
                {
                    continue_ops.push_back(wr_it->second);
                    pg.write_queue.erase(wr_it);
                }
            }
            if ((it == pg.flush_actions.end() || !it->second.submitted) &&
                erase_start != it)
            {
                pg.flush_actions.erase(erase_start, it);
            }
            if (it == pg.flush_actions.end())
            {
                break;
            }
            prev_it = it;
            if (!it->second.submitted)
            {
                it++;
                erase_start = it;
            }
            else
            {
                it++;
            }
        }
        delete fb;
        pg.flush_batch = NULL;
        if (!pg.flush_actions.size())
        {
            pg.state = pg.state & ~PG_HAS_UNCLEAN;
            report_pg_state(pg);
        }
        for (osd_op_t *op: continue_ops)
        {
            continue_primary_write(op);
        }
        if (pg.inflight == 0 && (pg.state & PG_STOPPING))
        {
            finish_stop_pg(pg);
        }
    }
}

void osd_t::submit_flush_op(pg_num_t pg_num, pg_flush_batch_t *fb, bool rollback, osd_num_t peer_osd, int count, obj_ver_id *data)
{
    osd_op_t *op = new osd_op_t();
    // Copy buffer so it gets freed along with the operation
    op->buf = malloc(sizeof(obj_ver_id) * count);
    memcpy(op->buf, data, sizeof(obj_ver_id) * count);
    if (peer_osd == this->osd_num)
    {
        // local
        clock_gettime(CLOCK_REALTIME, &op->tv_begin);
        op->bs_op = new blockstore_op_t({
            .opcode = (uint64_t)(rollback ? BS_OP_ROLLBACK : BS_OP_STABLE),
            .callback = [this, op, pg_num, fb](blockstore_op_t *bs_op)
            {
                add_bs_subop_stats(op);
                handle_flush_op(bs_op->opcode == BS_OP_ROLLBACK, pg_num, fb, this->osd_num, bs_op->retval);
                delete op->bs_op;
                op->bs_op = NULL;
                delete op;
            },
            .len = (uint32_t)count,
            .buf = op->buf,
        });
        bs->enqueue_op(op->bs_op);
    }
    else
    {
        // Peer
        int peer_fd = c_cli.osd_peer_fds[peer_osd];
        op->op_type = OSD_OP_OUT;
        op->iov.push_back(op->buf, count * sizeof(obj_ver_id));
        op->peer_fd = peer_fd;
        op->req = {
            .sec_stab = {
                .header = {
                    .magic = SECONDARY_OSD_OP_MAGIC,
                    .id = c_cli.next_subop_id++,
                    .opcode = (uint64_t)(rollback ? OSD_OP_SEC_ROLLBACK : OSD_OP_SEC_STABILIZE),
                },
                .len = count * sizeof(obj_ver_id),
            },
        };
        op->callback = [this, pg_num, fb, peer_osd](osd_op_t *op)
        {
            handle_flush_op(op->req.hdr.opcode == OSD_OP_SEC_ROLLBACK, pg_num, fb, peer_osd, op->reply.hdr.retval);
            delete op;
        };
        c_cli.outbox_push(op);
    }
}

bool osd_t::pick_next_recovery(osd_recovery_op_t &op)
{
    for (auto pg_it = pgs.begin(); pg_it != pgs.end(); pg_it++)
    {
        if ((pg_it->second.state & (PG_ACTIVE | PG_HAS_DEGRADED)) == (PG_ACTIVE | PG_HAS_DEGRADED))
        {
            for (auto obj_it = pg_it->second.degraded_objects.begin(); obj_it != pg_it->second.degraded_objects.end(); obj_it++)
            {
                if (recovery_ops.find(obj_it->first) == recovery_ops.end())
                {
                    op.degraded = true;
                    op.pg_num = pg_it->first;
                    op.oid = obj_it->first;
                    return true;
                }
            }
        }
    }
    for (auto pg_it = pgs.begin(); pg_it != pgs.end(); pg_it++)
    {
        if ((pg_it->second.state & (PG_ACTIVE | PG_HAS_MISPLACED)) == (PG_ACTIVE | PG_HAS_MISPLACED))
        {
            for (auto obj_it = pg_it->second.misplaced_objects.begin(); obj_it != pg_it->second.misplaced_objects.end(); obj_it++)
            {
                if (recovery_ops.find(obj_it->first) == recovery_ops.end())
                {
                    op.degraded = false;
                    op.pg_num = pg_it->first;
                    op.oid = obj_it->first;
                    return true;
                }
            }
        }
    }
    return false;
}

void osd_t::submit_recovery_op(osd_recovery_op_t *op)
{
    op->osd_op = new osd_op_t();
    op->osd_op->op_type = OSD_OP_OUT;
    op->osd_op->req = {
        .rw = {
            .header = {
                .magic = SECONDARY_OSD_OP_MAGIC,
                .id = 1,
                .opcode = OSD_OP_WRITE,
            },
            .inode = op->oid.inode,
            .offset = op->oid.stripe,
            .len = 0,
        },
    };
    op->osd_op->callback = [this, op](osd_op_t *osd_op)
    {
        // Don't sync the write, it will be synced by our regular sync coroutine
        if (osd_op->reply.hdr.retval < 0)
        {
            // Error recovering object
            if (osd_op->reply.hdr.retval == -EPIPE)
            {
                // PG is stopped or one of the OSDs is gone, error is harmless
            }
            else
            {
                throw std::runtime_error("Failed to recover an object");
            }
        }
        // CAREFUL! op = &recovery_ops[op->oid]. Don't access op->* after recovery_ops.erase()
        op->osd_op = NULL;
        recovery_ops.erase(op->oid);
        delete osd_op;
        continue_recovery();
    };
    exec_op(op->osd_op);
}

// Just trigger write requests for degraded objects. They'll be recovered during writing
bool osd_t::continue_recovery()
{
    while (recovery_ops.size() < recovery_queue_depth)
    {
        osd_recovery_op_t op;
        if (pick_next_recovery(op))
        {
            recovery_ops[op.oid] = op;
            submit_recovery_op(&recovery_ops[op.oid]);
        }
        else
            return false;
    }
    return true;
}
