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

inline void split_stripes(uint64_t pg_minsize, uint32_t bs_block_size, uint64_t start, uint64_t end, osd_read_stripe_t *stripes)
{
    for (int role = 0; role < pg_minsize; role++)
    {
        if (start < (1+role)*bs_block_size && end > role*bs_block_size)
        {
            stripes[role].real_start = stripes[role].start
                = start < role*bs_block_size ? 0 : start-role*bs_block_size;
            stripes[role].real_end = stripes[role].end
                = end > (role+1)*bs_block_size ? bs_block_size : end-role*bs_block_size;
        }
    }
}

void osd_t::exec_primary_read(osd_op_t *cur_op)
{
    // PG number is calculated from the offset
    // Our EC scheme stores data in fixed chunks equal to (K*block size)
    // But we must not use K in the process of calculating the PG number
    // So we calculate the PG number using a separate setting which should be per-inode (FIXME)
    uint64_t start = cur_op->op.rw.offset;
    uint64_t end = cur_op->op.rw.offset + cur_op->op.rw.len;
    // FIXME Real pg_num should equal the below expression + 1
    pg_num_t pg_num = (cur_op->op.rw.inode + cur_op->op.rw.offset / parity_block_size) % pg_count;
    // FIXME: Postpone operations in inactive PGs
    if (pg_num > pgs.size() || !(pgs[pg_num].state & PG_ACTIVE))
    {
        finish_primary_op(cur_op, -EINVAL);
        return;
    }
    uint64_t pg_parity_size = bs_block_size * pgs[pg_num].pg_minsize;
    object_id oid = {
        .inode = cur_op->op.rw.inode,
        // oid.stripe = starting offset of the parity stripe, so it can be mapped back to the PG
        .stripe = (cur_op->op.rw.offset / parity_block_size) * parity_block_size +
            ((cur_op->op.rw.offset % parity_block_size) / pg_parity_size) * pg_parity_size
    };
    if (end > (oid.stripe + pg_parity_size) ||
        (start % bs_disk_alignment) != 0 ||
        (end % bs_disk_alignment) != 0)
    {
        finish_primary_op(cur_op, -EINVAL);
        return;
    }
    osd_primary_read_t *op_data = (osd_primary_read_t*)calloc(
        sizeof(osd_primary_read_t) + sizeof(osd_read_stripe_t) * pgs[pg_num].pg_size, 1
    );
    op_data->oid = oid;
    op_data->stripes = ((osd_read_stripe_t*)(op_data+1));
    cur_op->op_data = op_data;
    split_stripes(pgs[pg_num].pg_minsize, bs_block_size, start, end, op_data->stripes);
    // Determine version
    {
        auto vo_it = pgs[pg_num].ver_override.find(oid);
        op_data->target_ver = vo_it != pgs[pg_num].ver_override.end() ? vo_it->second : UINT64_MAX;
    }
    if (pgs[pg_num].state == PG_ACTIVE)
    {
        // Fast happy-path
        submit_read_subops(pgs[pg_num].pg_minsize, pgs[pg_num].cur_set.data(), cur_op);
        cur_op->send_list.push_back(cur_op->buf, cur_op->op.rw.len);
    }
    else
    {
        // PG may be degraded or have misplaced objects
        spp::sparse_hash_map<object_id, pg_osd_set_state_t*> obj_states;
        auto st_it = pgs[pg_num].obj_states.find(oid);
        uint64_t* cur_set = (st_it != pgs[pg_num].obj_states.end()
            ? st_it->second->read_target.data()
            : pgs[pg_num].cur_set.data());
        if (extend_missing_stripes(op_data->stripes, cur_set, pgs[pg_num].pg_minsize, pgs[pg_num].pg_size) < 0)
        {
            free(op_data);
            finish_primary_op(cur_op, -EIO);
            return;
        }
        // Submit reads
        submit_read_subops(pgs[pg_num].pg_size, cur_set, cur_op);
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
            // FIXME: Always EC(k+1) by now. Add different coding schemes
            osd_read_stripe_t *stripes = op_data->stripes;
            for (int role = 0; role < op_data->pg_minsize; role++)
            {
                if (stripes[role].end != 0 && stripes[role].real_end == 0)
                {
                    int prev = -2;
                    for (int other = 0; other < op_data->pg_size; other++)
                    {
                        if (other != role)
                        {
                            if (prev == -2)
                            {
                                prev = other;
                            }
                            else if (prev >= 0)
                            {
                                memxor(
                                    cur_op->buf + stripes[prev].pos + (stripes[prev].real_start - stripes[role].start),
                                    cur_op->buf + stripes[other].pos + (stripes[other].real_start - stripes[other].start),
                                    cur_op->buf + stripes[role].pos, stripes[role].end - stripes[role].start
                                );
                                prev = -1;
                            }
                            else
                            {
                                memxor(
                                    cur_op->buf + stripes[role].pos,
                                    cur_op->buf + stripes[other].pos + (stripes[other].real_start - stripes[role].start),
                                    cur_op->buf + stripes[role].pos, stripes[role].end - stripes[role].start
                                );
                            }
                        }
                    }
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

int osd_t::extend_missing_stripes(osd_read_stripe_t *stripes, osd_num_t *osd_set, int minsize, int size)
{
    for (int role = 0; role < minsize; role++)
    {
        if (stripes[role].end != 0 && osd_set[role] == 0)
        {
            stripes[role].real_start = stripes[role].real_end = 0;
            // Stripe is missing. Extend read to other stripes.
            // We need at least pg_minsize stripes to recover the lost part.
            int exist = 0;
            for (int j = 0; j < size; j++)
            {
                if (osd_set[j] != 0)
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

void osd_t::submit_read_subops(int read_pg_size, const uint64_t* osd_set, osd_op_t *cur_op)
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
        auto role_osd_num = osd_set[role];
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
    // "RAID5" EC(k+1) parity modification variants (Px = previous, Nx = new):
    // 1,2,3 write N1 -> read P2 -> write N3 = N1^P2
    // _,2,3 write N1 -> read P2 -> write N3 = N1^P2
    // 1,_,3 write N1 -> read P1,P3 -> write N3 = N1^P3^P1
    // 1,2,_ write N1 -> read nothing
    // 1,2,3,4 write N1 -> read P2,P3 -> write N4 = N1^P2^P3
    //                 (or read P1,P4 -> write N4 = N1^P4^P1)
    // 1,_,3,4 write N1 -> read P1,P4 -> write N4 = N1^P4^P1
    // _,2,3,4 write N1 -> read P2,P3 -> write N4 = N1^P3^P2
    // 1,2,3,4,5 write N1 -> read P1,P5 -> write N5 = N1^P5^P1
    // 1,_,3,4,5 write N1 -> read P1,P5 -> write N5 = N1^P5^P1
    // _,2,3,4,5 write N1 -> read P2,P3,P4 -> write N5 = N1^P2^P3^P4
    //
    // I.e, when we write a part:
    // 1) If parity is missing and all other parts are available:
    //    just overwrite the part
    // 2) If the modified part is missing and all other parts are available:
    //    read all other parts except parity, xor them all with the new data
    // 3) If all parts are available and size=3:
    //    read the paired data stripe, xor it with the new data
    // 4) Otherwise:
    //    read old parity and old data of the modified part, xor them both with the new data
    // Ouсh. Scary. But faster than the generic variant.
    //
    // Generic variant for jerasure is a simple RMW process: read all -> decode -> modify -> encode -> write
    
}

void osd_t::exec_primary_sync(osd_op_t *cur_op)
{
    
}
