#include "osd.h"
#include "osd_rmw.h"

#define SUBMIT_READ 0
#define SUBMIT_RMW_READ 1
#define SUBMIT_WRITE 2

// read: read directly or read paired stripe(s), reconstruct, return
// write: read paired stripe(s), modify, write
//
// nuance: take care to read the same version from paired stripes!
// to do so, we remember "last readable" version until a write request completes
// and we postpone other write requests to the same stripe until completion of previous ones
//
// sync: sync peers, get unstable versions from somewhere, stabilize them

struct osd_primary_op_data_t
{
    int st = 0;
    pg_num_t pg_num;
    object_id oid;
    uint64_t target_ver;
    uint64_t fact_ver = 0;
    int n_subops = 0, done = 0, errors = 0;
    int degraded = 0, pg_size, pg_minsize;
    osd_rmw_stripe_t *stripes;
    osd_op_t *subops = NULL;
};

void osd_t::finish_primary_op(osd_op_t *cur_op, int retval)
{
    // FIXME add separate magics
    cur_op->reply.hdr.magic = SECONDARY_OSD_REPLY_MAGIC;
    cur_op->reply.hdr.id = cur_op->req.hdr.id;
    cur_op->reply.hdr.opcode = cur_op->req.hdr.opcode;
    cur_op->reply.hdr.retval = retval;
    outbox_push(this->clients[cur_op->peer_fd], cur_op);
}

bool osd_t::prepare_primary_rw(osd_op_t *cur_op)
{
    // PG number is calculated from the offset
    // Our EC scheme stores data in fixed chunks equal to (K*block size)
    // But we must not use K in the process of calculating the PG number
    // So we calculate the PG number using a separate setting which should be per-inode (FIXME)
    // FIXME Real pg_num should equal the below expression + 1
    pg_num_t pg_num = (cur_op->req.rw.inode + cur_op->req.rw.offset / parity_block_size) % pg_count;
    // FIXME: Postpone operations in inactive PGs
    if (pg_num > pgs.size() || !(pgs[pg_num].state & PG_ACTIVE))
    {
        finish_primary_op(cur_op, -EINVAL);
        return false;
    }
    uint64_t pg_parity_size = bs_block_size * pgs[pg_num].pg_minsize;
    object_id oid = {
        .inode = cur_op->req.rw.inode,
        // oid.stripe = starting offset of the parity stripe, so it can be mapped back to the PG
        .stripe = (cur_op->req.rw.offset / parity_block_size) * parity_block_size +
            ((cur_op->req.rw.offset % parity_block_size) / pg_parity_size) * pg_parity_size
    };
    if ((cur_op->req.rw.offset + cur_op->req.rw.len) > (oid.stripe + pg_parity_size) ||
        (cur_op->req.rw.offset % bs_disk_alignment) != 0 ||
        (cur_op->req.rw.len % bs_disk_alignment) != 0)
    {
        finish_primary_op(cur_op, -EINVAL);
        return false;
    }
    osd_primary_op_data_t *op_data = (osd_primary_op_data_t*)calloc(
        sizeof(osd_primary_op_data_t) + sizeof(osd_rmw_stripe_t) * pgs[pg_num].pg_size, 1
    );
    op_data->pg_num = pg_num;
    op_data->oid = oid;
    op_data->stripes = ((osd_rmw_stripe_t*)(op_data+1));
    cur_op->op_data = op_data;
    split_stripes(pgs[pg_num].pg_minsize, bs_block_size, (uint32_t)(cur_op->req.rw.offset - oid.stripe), cur_op->req.rw.len, op_data->stripes);
    return true;
}

void osd_t::continue_primary_read(osd_op_t *cur_op)
{
    if (!cur_op->op_data && !prepare_primary_rw(cur_op))
    {
        return;
    }
    osd_primary_op_data_t *op_data = cur_op->op_data;
    if (op_data->st == 1)      goto resume_1;
    else if (op_data->st == 2) goto resume_2;
    {
        auto & pg = pgs[op_data->pg_num];
        for (int role = 0; role < pg.pg_minsize; role++)
        {
            op_data->stripes[role].read_start = op_data->stripes[role].req_start;
            op_data->stripes[role].read_end = op_data->stripes[role].req_end;
        }
        // Determine version
        auto vo_it = pg.ver_override.find(op_data->oid);
        op_data->target_ver = vo_it != pg.ver_override.end() ? vo_it->second : UINT64_MAX;
        if (pg.state == PG_ACTIVE)
        {
            // Fast happy-path
            cur_op->buf = alloc_read_buffer(op_data->stripes, pg.pg_minsize, 0);
            submit_primary_subops(SUBMIT_READ, pg.pg_minsize, pg.cur_set.data(), cur_op);
            cur_op->send_list.push_back(cur_op->buf, cur_op->req.rw.len);
            op_data->st = 1;
        }
        else
        {
            // PG may be degraded or have misplaced objects
            auto st_it = pg.obj_states.find(op_data->oid);
            uint64_t* cur_set = (st_it != pg.obj_states.end()
                ? st_it->second->read_target.data()
                : pg.cur_set.data());
            if (extend_missing_stripes(op_data->stripes, cur_set, pg.pg_minsize, pg.pg_size) < 0)
            {
                free(op_data);
                finish_primary_op(cur_op, -EIO);
                return;
            }
            // Submit reads
            op_data->pg_minsize = pg.pg_minsize;
            op_data->pg_size = pg.pg_size;
            op_data->degraded = 1;
            cur_op->buf = alloc_read_buffer(op_data->stripes, pg.pg_size, 0);
            submit_primary_subops(SUBMIT_READ, pg.pg_size, cur_set, cur_op);
            op_data->st = 1;
        }
    }
resume_1:
    return;
resume_2:
    if (op_data->errors > 0)
    {
        free(op_data);
        cur_op->op_data = NULL;
        finish_primary_op(cur_op, -EIO);
        return;
    }
    if (op_data->degraded)
    {
        // Reconstruct missing stripes
        // FIXME: Always EC(k+1) by now. Add different coding schemes
        osd_rmw_stripe_t *stripes = op_data->stripes;
        for (int role = 0; role < op_data->pg_minsize; role++)
        {
            if (stripes[role].read_end != 0 && stripes[role].missing)
            {
                reconstruct_stripe(stripes, op_data->pg_size, role);
            }
            if (stripes[role].req_end != 0)
            {
                // Send buffer in parts to avoid copying
                cur_op->send_list.push_back(
                    stripes[role].read_buf + (stripes[role].req_start - stripes[role].read_start),
                    stripes[role].req_end - stripes[role].req_start
                );
            }
        }
    }
    free(op_data);
    cur_op->op_data = NULL;
    finish_primary_op(cur_op, cur_op->req.rw.len);
}

void osd_t::submit_primary_subops(int submit_type, int pg_size, const uint64_t* osd_set, osd_op_t *cur_op)
{
    bool w = submit_type == SUBMIT_WRITE;
    osd_primary_op_data_t *op_data = cur_op->op_data;
    osd_rmw_stripe_t *stripes = op_data->stripes;
    // Allocate subops
    int n_subops = 0, zero_read = -1;
    for (int role = 0; role < pg_size; role++)
    {
        if (osd_set[role] == this->osd_num || osd_set[role] != 0 && zero_read == -1)
        {
            zero_read = role;
        }
        if (osd_set[role] != 0 && (w || stripes[role].read_end != 0))
        {
            n_subops++;
        }
    }
    if (!n_subops && submit_type == SUBMIT_RMW_READ)
    {
        n_subops = 1;
    }
    else
    {
        zero_read = -1;
    }
    osd_op_t *subops = new osd_op_t[n_subops];
    op_data->done = op_data->errors = 0;
    op_data->n_subops = n_subops;
    op_data->subops = subops;
    int subop = 0;
    for (int role = 0; role < pg_size; role++)
    {
        // We always submit zero-length writes to all replicas, even if the stripe is not modified
        if (!(w || stripes[role].read_end != 0 || zero_read == role))
        {
            continue;
        }
        osd_num_t role_osd_num = osd_set[role];
        if (role_osd_num != 0)
        {
            if (role_osd_num == this->osd_num)
            {
                subops[subop].bs_op = new blockstore_op_t({
                    .opcode = (uint64_t)(w ? BS_OP_WRITE : BS_OP_READ),
                    .callback = [cur_op, this](blockstore_op_t *subop)
                    {
                        handle_primary_subop(cur_op, subop->retval == subop->len, subop->version);
                    },
                    .oid = {
                        .inode = op_data->oid.inode,
                        .stripe = op_data->oid.stripe | role,
                    },
                    .version = w ? 0 : (submit_type == SUBMIT_RMW_READ ? UINT64_MAX : op_data->target_ver),
                    .offset = w ? stripes[role].write_start : stripes[role].read_start,
                    .len = w ? stripes[role].write_end - stripes[role].write_start : stripes[role].read_end - stripes[role].read_start,
                    .buf = w ? stripes[role].write_buf : stripes[role].read_buf,
                });
                bs->enqueue_op(subops[subop].bs_op);
            }
            else
            {
                subops[subop].op_type = OSD_OP_OUT;
                subops[subop].peer_fd = this->osd_peer_fds.at(role_osd_num);
                subops[subop].req.sec_rw = {
                    .header = {
                        .magic = SECONDARY_OSD_OP_MAGIC,
                        .id = this->next_subop_id++,
                        .opcode = (uint64_t)(w ? OSD_OP_SECONDARY_WRITE : OSD_OP_SECONDARY_READ),
                    },
                    .oid = {
                        .inode = op_data->oid.inode,
                        .stripe = op_data->oid.stripe | role,
                    },
                    .version = w ? 0 : (submit_type == SUBMIT_RMW_READ ? UINT64_MAX : op_data->target_ver),
                    .offset = w ? stripes[role].write_start : stripes[role].read_start,
                    .len = w ? stripes[role].write_end - stripes[role].write_start : stripes[role].read_end - stripes[role].read_start,
                };
                subops[subop].buf = w ? stripes[role].write_buf : stripes[role].read_buf;
                if (w && stripes[role].write_end > 0)
                {
                    subops[subop].send_list.push_back(stripes[role].write_buf, stripes[role].write_end - stripes[role].write_start);
                }
                subops[subop].callback = [cur_op, this](osd_op_t *subop)
                {
                    // so it doesn't get freed
                    subop->buf = NULL;
                    handle_primary_subop(cur_op, subop->reply.hdr.retval == subop->req.sec_rw.len, subop->reply.sec_rw.version);
                };
                outbox_push(clients[subops[subop].peer_fd], &subops[subop]);
            }
            subop++;
        }
    }
}

void osd_t::handle_primary_subop(osd_op_t *cur_op, int ok, uint64_t version)
{
    osd_primary_op_data_t *op_data = cur_op->op_data;
    op_data->fact_ver = version;
    if (!ok)
    {
        op_data->errors++;
    }
    else
    {
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
        else
        {
            continue_primary_write(cur_op);
        }
    }
}

void osd_t::continue_primary_write(osd_op_t *cur_op)
{
    if (!cur_op->op_data && !prepare_primary_rw(cur_op))
    {
        return;
    }
    osd_primary_op_data_t *op_data = cur_op->op_data;
    // FIXME: Handle operation cancel
    auto & pg = pgs[op_data->pg_num];
    if (op_data->st == 1)      goto resume_1;
    else if (op_data->st == 2) goto resume_2;
    else if (op_data->st == 3) goto resume_3;
    else if (op_data->st == 4) goto resume_4;
    else if (op_data->st == 5) goto resume_5;
    // Check if actions are pending for this object
    {
        auto act_it = pg.obj_stab_actions.lower_bound((obj_piece_id_t){
            .oid = op_data->oid,
            .osd_num = 0,
        });
        if (act_it != pg.obj_stab_actions.end() &&
            act_it->first.oid.inode == op_data->oid.inode &&
            (act_it->first.oid.stripe & ~STRIPE_MASK) == op_data->oid.stripe)
        {
            // FIXME postpone the request until actions are done
            free(op_data);
            finish_primary_op(cur_op, -EIO);
            return;
        }
    }
resume_1:
    // Check if there are other write requests to the same object
    {
        auto vo_it = pg.ver_override.find(op_data->oid);
        if (vo_it != pg.ver_override.end())
        {
            op_data->st = 1;
            pg.write_queue.emplace(op_data->oid, cur_op);
            return;
        }
    }
    // Determine blocks to read
    cur_op->rmw_buf = calc_rmw_reads(cur_op->buf, op_data->stripes, pg.cur_set.data(), pg.pg_size, pg.pg_minsize, pg.pg_cursize);
    // Read required blocks
    submit_primary_subops(SUBMIT_RMW_READ, pg.pg_size, pg.cur_set.data(), cur_op);
resume_2:
    op_data->st = 2;
    return;
resume_3:
    // Save version override for parallel reads
    pg.ver_override[op_data->oid] = op_data->fact_ver;
    // Calculate parity
    calc_rmw_parity(op_data->stripes, pg.pg_size);
    // Send writes
    submit_primary_subops(SUBMIT_WRITE, pg.pg_size, pg.cur_set.data(), cur_op);
resume_4:
    op_data->st = 4;
    return;
resume_5:
    // Remember version as unstable
    osd_num_t *osd_set = pg.cur_set.data();
    for (int role = 0; role < pg.pg_size; role++)
    {
        if (osd_set[role] != 0)
        {
            this->unstable_writes[osd_set[role]][(object_id){
                .inode = op_data->oid.inode,
                .stripe = op_data->oid.stripe | role,
            }] = op_data->fact_ver;
        }
    }
    // Remember PG as dirty to drop the connection when PG goes offline
    // (this is required because of the "lazy sync")
    this->clients[cur_op->peer_fd].dirty_pgs.insert(op_data->pg_num);
    // Remove version override
    pg.ver_override.erase(op_data->oid);
    finish_primary_op(cur_op, cur_op->req.rw.len);
    // Continue other write operations to the same object
    {
        auto next_it = pg.write_queue.find(op_data->oid);
        if (next_it != pg.write_queue.end())
        {
            osd_op_t *next_op = next_it->second;
            pg.write_queue.erase(next_it);
            continue_primary_write(next_op);
        }
    }
}

void osd_t::exec_primary_sync(osd_op_t *cur_op)
{
    
}
