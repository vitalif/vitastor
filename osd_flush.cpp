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

void osd_t::handle_flush_op(pg_num_t pg_num, pg_flush_batch_t *fb, osd_num_t osd_num, bool ok)
{
    if (pgs.find(pg_num) == pgs.end() || pgs[pg_num].flush_batch != fb)
    {
        // Throw the result away
        return;
    }
    if (!ok)
    {
        if (osd_num == this->osd_num)
            throw std::runtime_error("Error while doing local flush operation");
        else
        {
            assert(osd_peer_fds.find(osd_num) != osd_peer_fds.end());
            stop_client(osd_peer_fds[osd_num]);
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
            pg.print_state();
        }
        for (osd_op_t *op: continue_ops)
        {
            continue_primary_write(op);
        }
    }
}

void osd_t::submit_flush_op(pg_num_t pg_num, pg_flush_batch_t *fb, bool rollback, osd_num_t osd_num, int count, obj_ver_id *data)
{
    osd_op_t *op = new osd_op_t();
    // Copy buffer so it gets freed along with the operation
    op->buf = malloc(sizeof(obj_ver_id) * count);
    memcpy(op->buf, data, sizeof(obj_ver_id) * count);
    if (osd_num == this->osd_num)
    {
        // local
        op->bs_op = new blockstore_op_t({
            .opcode = (uint64_t)(rollback ? BS_OP_ROLLBACK : BS_OP_STABLE),
            .callback = [this, op, pg_num, fb](blockstore_op_t *bs_op)
            {
                handle_flush_op(pg_num, fb, this->osd_num, bs_op->retval == 0);
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
        int peer_fd = osd_peer_fds[osd_num];
        op->op_type = OSD_OP_OUT;
        op->send_list.push_back(op->req.buf, OSD_PACKET_SIZE);
        op->send_list.push_back(op->buf, count * sizeof(obj_ver_id));
        op->peer_fd = peer_fd;
        op->req = {
            .sec_stab = {
                .header = {
                    .magic = SECONDARY_OSD_OP_MAGIC,
                    .id = this->next_subop_id++,
                    .opcode = (uint64_t)(rollback ? OSD_OP_SECONDARY_ROLLBACK : OSD_OP_SECONDARY_STABILIZE),
                },
                .len = count * sizeof(obj_ver_id),
            },
        };
        op->callback = [this, pg_num, fb](osd_op_t *op)
        {
            handle_flush_op(pg_num, fb, clients[op->peer_fd].osd_num, op->reply.hdr.retval == 0);
            delete op;
        };
        outbox_push(clients[peer_fd], op);
    }
}
