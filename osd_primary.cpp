#include "osd.h"
#include "xor.h"

// read: read directly or read paired stripe(s), reconstruct, return
// write: read paired stripe(s), modify, write
//
// nuance: take care to read the same version from paired stripes!
// to do so, we remember "last readable" version until a write request completes
// and we postpone other write requests to the same stripe until completion of previous ones
//
// sync: sync peers, get unstable versions from somewhere, stabilize them

struct off_len_t
{
    uint64_t offset, len;
};

struct osd_read_stripe_t
{
    uint64_t pos;
    uint32_t start, end;
    uint32_t real_start, real_end;
};

struct osd_primary_read_t
{
    pg_num_t pg_num;
    object_id oid;
    uint64_t target_ver;
    int n_subops = 0, done = 0, errors = 0;
    int degraded = 0, pg_size, pg_minsize;
    osd_read_stripe_t *stripes;
    osd_op_t *subops = NULL;
};

void osd_t::finish_primary_op(osd_op_t *cur_op, int retval)
{
    // FIXME add separate magics
    cur_op->reply.hdr.magic = SECONDARY_OSD_REPLY_MAGIC;
    cur_op->reply.hdr.id = cur_op->op.hdr.id;
    cur_op->reply.hdr.opcode = cur_op->op.hdr.opcode;
    cur_op->reply.hdr.retval = retval;
    outbox_push(this->clients[cur_op->peer_fd], cur_op);
}

void osd_t::exec_primary_read(osd_op_t *cur_op)
{
    object_id oid = {
        .inode = cur_op->op.rw.inode,
        .stripe = (cur_op->op.rw.offset / (bs_block_size*2)) << STRIPE_SHIFT,
    };
    uint64_t start = cur_op->op.rw.offset;
    uint64_t end = cur_op->op.rw.offset + cur_op->op.rw.len;
    pg_num_t pg_num = (oid % pg_count); // FIXME +1
    if (((end - 1) / (bs_block_size*2)) != oid.stripe ||
        (start % bs_disk_alignment) || (end % bs_disk_alignment) ||
        pg_num > pgs.size())
    {
        finish_primary_op(cur_op, -EINVAL);
        return;
    }
    osd_primary_read_t *op_data = (osd_primary_read_t*)calloc(
        sizeof(osd_primary_read_t) + sizeof(osd_read_stripe_t) * pgs[pg_num].pg_size, 1
    );
    op_data->oid = oid;
    osd_read_stripe_t *stripes = (op_data->stripes = ((osd_read_stripe_t*)(op_data+1)));
    cur_op->op_data = op_data;
    for (int role = 0; role < pgs[pg_num].pg_minsize; role++)
    {
        if (start < (1+role)*bs_block_size && end > role*bs_block_size)
        {
            stripes[role].real_start = stripes[role].start
                = start < role*bs_block_size ? 0 : start-role*bs_block_size;
            stripes[role].end = stripes[role].real_end
                = end > (role+1)*bs_block_size ? bs_block_size : end-role*bs_block_size;
        }
    }
    {
        auto vo_it = pgs[pg_num].ver_override.find(oid);
        op_data->target_ver = vo_it != pgs[pg_num].ver_override.end() ? vo_it->second : UINT64_MAX;
    }
    if (pgs[pg_num].pg_cursize == pgs[pg_num].pg_size)
    {
        // Fast happy-path
        submit_read_subops(pgs[pg_num].pg_minsize, pgs[pg_num].target_set.data(), cur_op);
        cur_op->send_list.push_back(cur_op->buf, cur_op->op.rw.len);
    }
    else
    {
        // PG is degraded
        uint64_t* target_set;
        {
            auto it = pgs[pg_num].obj_states.find(oid);
            target_set = (it != pgs[pg_num].obj_states.end()
                ? it->second->read_target.data()
                : pgs[pg_num].target_set.data());
        }
        if (extend_missing_stripes(stripes, target_set, pgs[pg_num].pg_minsize, pgs[pg_num].pg_size) < 0)
        {
            free(op_data);
            finish_primary_op(cur_op, -EIO);
            return;
        }
        // Submit reads
        submit_read_subops(pgs[pg_num].pg_size, target_set, cur_op);
        op_data->pg_minsize = pgs[pg_num].pg_minsize;
        op_data->pg_size = pgs[pg_num].pg_size;
        op_data->degraded = 1;
    }
}

void osd_t::handle_primary_read_subop(osd_op_t *cur_op, int ok)
{
    osd_primary_read_t *op_data = (osd_primary_read_t*)cur_op->op_data;
    if (!ok)
        op_data->errors++;
    else
        op_data->done++;
    if ((op_data->errors + op_data->done) >= op_data->n_subops)
    {
        delete[] op_data->subops;
        op_data->subops = NULL;
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
            osd_read_stripe_t *stripes = op_data->stripes;
            for (int role = 0; role < op_data->pg_minsize; role++)
            {
                if (stripes[role].end != 0 && stripes[role].real_end == 0)
                {
                    int other = role == 0 ? 1 : 0;
                    int parity = op_data->pg_size-1;
                    memxor(
                        cur_op->buf + stripes[other].pos + (stripes[other].real_start - stripes[role].start),
                        cur_op->buf + stripes[parity].pos + (stripes[parity].real_start - stripes[role].start),
                        cur_op->buf + stripes[role].pos, stripes[role].end - stripes[role].start
                    );
                }
                if (stripes[role].end != 0)
                {
                    // Send buffer in parts to avoid copying
                    cur_op->send_list.push_back(
                        cur_op->buf + stripes[role].pos + (stripes[role].real_start - stripes[role].start), stripes[role].end
                    );
                }
            }
        }
        free(op_data);
        cur_op->op_data = NULL;
        finish_primary_op(cur_op, cur_op->op.rw.len);
    }
}

int osd_t::extend_missing_stripes(osd_read_stripe_t *stripes, osd_num_t *target_set, int minsize, int size)
{
    for (int role = 0; role < minsize; role++)
    {
        if (stripes[role].end != 0 && target_set[role] == 0)
        {
            stripes[role].real_start = stripes[role].real_end = 0;
            // Stripe is missing. Extend read to other stripes.
            // We need at least pg_minsize stripes to recover the lost part.
            int exist = 0;
            for (int j = 0; j < size; j++)
            {
                if (target_set[j] != 0)
                {
                    if (stripes[j].real_end == 0 || j >= minsize)
                    {
                        stripes[j].real_start = stripes[role].start;
                        stripes[j].real_end = stripes[role].end;
                    }
                    else
                    {
                        stripes[j].real_start = stripes[j].start < stripes[role].start ? stripes[j].start : stripes[role].start;
                        stripes[j].real_end = stripes[j].end > stripes[role].end ? stripes[j].end : stripes[role].end;
                    }
                    exist++;
                    if (exist >= minsize)
                    {
                        break;
                    }
                }
            }
            if (exist < minsize)
            {
                // Less than minsize stripes are available for this object
                return -1;
            }
        }
    }
    return 0;
}

void osd_t::submit_read_subops(int read_pg_size, const uint64_t* target_set, osd_op_t *cur_op)
{
    osd_primary_read_t *op_data = (osd_primary_read_t*)cur_op->op_data;
    osd_read_stripe_t *stripes = op_data->stripes;
    uint64_t buf_size = 0;
    int n_subops = 0;
    for (int role = 0; role < read_pg_size; role++)
    {
        if (stripes[role].real_end != 0)
        {
            n_subops++;
            stripes[role].pos = buf_size;
            buf_size += stripes[role].real_end - stripes[role].real_start;
        }
        else if (stripes[role].end != 0)
        {
            stripes[role].pos = buf_size;
            buf_size += stripes[role].end - stripes[role].start;
        }
    }
    osd_op_t *subops = new osd_op_t[n_subops];
    cur_op->buf = memalign(MEM_ALIGNMENT, buf_size);
    op_data->n_subops = n_subops;
    op_data->subops = subops;
    int subop = 0;
    for (int role = 0; role < read_pg_size; role++)
    {
        if (stripes[role].real_end == 0)
        {
            continue;
        }
        auto role_osd_num = target_set[role];
        if (role_osd_num != 0)
        {
            if (role_osd_num == this->osd_num)
            {
                subops[subop].bs_op = {
                    .opcode = BS_OP_READ,
                    .callback = [this, cur_op](blockstore_op_t *subop)
                    {
                        handle_primary_read_subop(cur_op, subop->retval == subop->len);
                    },
                    .oid = {
                        .inode = op_data->oid.inode,
                        .stripe = op_data->oid.stripe | role,
                    },
                    .version = op_data->target_ver,
                    .offset = stripes[role].real_start,
                    .len = stripes[role].real_end - stripes[role].real_start,
                    .buf = cur_op->buf + stripes[role].pos,
                };
                bs->enqueue_op(&subops[subop].bs_op);
            }
            else
            {
                subops[subop].op_type = OSD_OP_OUT;
                subops[subop].peer_fd = this->osd_peer_fds.at(role_osd_num);
                subops[subop].op.sec_rw = {
                    .header = {
                        .magic = SECONDARY_OSD_OP_MAGIC,
                        .id = this->next_subop_id++,
                        .opcode = OSD_OP_SECONDARY_READ,
                    },
                    .oid = {
                        .inode = op_data->oid.inode,
                        .stripe = op_data->oid.stripe | role,
                    },
                    .version = op_data->target_ver,
                    .offset = stripes[role].real_start,
                    .len = stripes[role].real_end - stripes[role].real_start,
                };
                subops[subop].buf = cur_op->buf + stripes[role].pos;
                subops[subop].callback = [this, cur_op](osd_op_t *subop)
                {
                    // so it doesn't get freed. FIXME: do it better
                    subop->buf = NULL;
                    handle_primary_read_subop(cur_op, subop->reply.hdr.retval == subop->op.sec_rw.len);
                };
                outbox_push(clients[subops[subop].peer_fd], &subops[subop]);
            }
            subop++;
        }
    }
}

void osd_t::exec_primary_write(osd_op_t *cur_op)
{
    
}

void osd_t::exec_primary_sync(osd_op_t *cur_op)
{
    
}

void osd_t::make_primary_reply(osd_op_t *op)
{
    op->reply.hdr.magic = SECONDARY_OSD_REPLY_MAGIC;
    op->reply.hdr.id = op->op.hdr.id;
    op->reply.hdr.opcode = op->op.hdr.opcode;
}
