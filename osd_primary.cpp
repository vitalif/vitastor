#include "osd.h"
#include "xor.h"

void osd_t::exec_primary_read(osd_op_t *cur_op)
{
    // read: read directly or read paired stripe(s), reconstruct, return
    // write: read paired stripe(s), modify, write
    // nuance: take care to read the same version from paired stripes!
    // if there are no write requests in progress we're good (stripes must be in sync)
    // and... remember the last readable version during a write request
    // and... postpone other write requests to the same stripe until the completion of previous ones
    //
    // sync: sync peers, get unstable versions from somewhere, stabilize them
    object_id oid = {
        .inode = cur_op->op.rw.inode,
        .stripe = (cur_op->op.rw.offset / (bs_block_size*2)) << STRIPE_SHIFT,
    };
    uint64_t start = cur_op->op.rw.offset, end = cur_op->op.rw.offset + cur_op->op.rw.len;
    unsigned pg_num = (oid % pg_count); // FIXME +1
    if (((end - 1) / (bs_block_size*2)) != oid.stripe ||
        (start % bs_disk_alignment) || (end % bs_disk_alignment) ||
        pg_num > pgs.size())
    {
        // FIXME add separate magics
        cur_op->reply.hdr.magic = SECONDARY_OSD_REPLY_MAGIC;
        cur_op->reply.hdr.id = cur_op->op.hdr.id;
        cur_op->reply.hdr.opcode = cur_op->op.hdr.opcode;
        cur_op->reply.hdr.retval = -EINVAL;
        outbox_push(clients[cur_op->peer_fd], cur_op);
        return;
    }
    // role -> start, end
    uint64_t reads[pgs[pg_num].pg_minsize*2] = { 0 };
    for (int role = 0; role < pgs[pg_num].pg_minsize; role++)
    {
        if (start < (1+role)*bs_block_size && end > role*bs_block_size)
        {
            reads[role*2] = start < role*bs_block_size ? 0 : start-role*bs_block_size;
            reads[role*2+1] = end > (role+1)*bs_block_size ? bs_block_size : end-role*bs_block_size;
        }
    }
    if (pgs[pg_num].pg_cursize == 3)
    {
        // Fast happy-path
        void *buf = memalign(MEM_ALIGNMENT, cur_op->op.rw.len);
        
    }
    else
    {
        // PG is degraded
        uint64_t real_reads[pgs[pg_num].pg_size*2] = { 0 };
        memcpy(real_reads, reads, sizeof(uint64_t)*pgs[pg_num].pg_minsize*2);
        for (int role = 0; role < pgs[pg_num].pg_minsize; role++)
        {
            if (reads[role*2+1] != 0 && pgs[pg_num].target_set[role] == UINT64_MAX)
            {
                // Stripe is missing. Extend read to other stripes.
                // We need at least pg_minsize stripes to recover the lost part.
                int exist = 0;
                for (int j = 0; j < pgs[pg_num].pg_size; j++)
                {
                    if (pgs[pg_num].target_set[j] != UINT64_MAX)
                    {
                        if (real_reads[j*2+1] == 0 || j >= pgs[pg_num].pg_minsize)
                        {
                            real_reads[j*2] = reads[role*2];
                            real_reads[j*2+1] = reads[role*2+1];
                        }
                        else
                        {
                            real_reads[j*2] = reads[j*2] < reads[role*2] ? reads[j*2] : reads[role*2];
                            real_reads[j*2+1] = reads[j*2+1] > reads[role*2+1] ? reads[j*2+1] : reads[role*2+1];
                        }
                        exist++;
                        if (exist >= pgs[pg_num].pg_minsize)
                        {
                            break;
                        }
                    }
                }
            }
        }
        uint64_t pos[pgs[pg_num].pg_size];
        uint64_t buf_size = 0;
        for (int role = 0; role < pgs[pg_num].pg_size; role++)
        {
            pos[role] = buf_size;
            buf_size += real_reads[role*2+1];
        }
        void *buf = memalign(MEM_ALIGNMENT, buf_size);
        // ...<SUBMIT READS AND GET REPLIES>...
        
        // Reconstruct missing stripes
        for (int role = 0; role < pgs[pg_num].pg_minsize; role++)
        {
            if (reads[role*2+1] != 0 && pgs[pg_num].target_set[role] == UINT64_MAX)
            {
                int other = role == 0 ? 1 : 0;
                int parity = pgs[pg_num].pg_size-1;
                memxor(
                    buf + pos[other] + (real_reads[other*2]-reads[role*2]),
                    buf + pos[parity] + (real_reads[parity*2]-reads[role*2]),
                    buf + pos[role], reads[role*2+1]-reads[role*2]
                );
            }
        }
        // Send buffer in parts to avoid copying
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
