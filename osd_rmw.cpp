#include <malloc.h>
#include <assert.h>
#include "xor.h"
#include "osd_rmw.h"

static inline void extend_read(uint32_t start, uint32_t end, osd_rmw_stripe_t & stripe)
{
    if (stripe.read_end == 0)
    {
        stripe.read_start = start;
        stripe.read_end = end;
    }
    else
    {
        if (stripe.read_end < end)
            stripe.read_end = end;
        if (stripe.read_start > start)
            stripe.read_start = start;
    }
}

static inline void cover_read(uint32_t start, uint32_t end, osd_rmw_stripe_t & stripe)
{
    // Subtract <to> write request from <from> request
    if (start >= stripe.req_start &&
        end <= stripe.req_end)
    {
        return;
    }
    if (start <= stripe.req_start &&
        end >= stripe.req_start &&
        end <= stripe.req_end)
    {
        end = stripe.req_start;
    }
    else if (start >= stripe.req_start &&
        start <= stripe.req_end &&
        end >= stripe.req_end)
    {
        start = stripe.req_end;
    }
    if (stripe.read_end == 0)
    {
        stripe.read_start = start;
        stripe.read_end = end;
    }
    else
    {
        if (stripe.read_end < end)
            stripe.read_end = end;
        if (stripe.read_start > start)
            stripe.read_start = start;
    }
}

void split_stripes(uint64_t pg_minsize, uint32_t bs_block_size, uint32_t start, uint32_t end, osd_rmw_stripe_t *stripes)
{
    end = start+end;
    for (int role = 0; role < pg_minsize; role++)
    {
        if (start < (1+role)*bs_block_size && end > role*bs_block_size)
        {
            stripes[role].req_start = start < role*bs_block_size ? 0 : start-role*bs_block_size;
            stripes[role].req_end = end > (role+1)*bs_block_size ? bs_block_size : end-role*bs_block_size;
        }
    }
}

void reconstruct_stripe(osd_rmw_stripe_t *stripes, int pg_size, int role)
{
    int prev = -2;
    for (int other = 0; other < pg_size; other++)
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
                    stripes[prev].read_buf + (stripes[prev].read_start - stripes[role].read_start),
                    stripes[other].read_buf + (stripes[other].read_start - stripes[other].read_start),
                    stripes[role].read_buf, stripes[role].read_end - stripes[role].read_start
                );
                prev = -1;
            }
            else
            {
                memxor(
                    stripes[role].read_buf,
                    stripes[other].read_buf + (stripes[other].read_start - stripes[role].read_start),
                    stripes[role].read_buf, stripes[role].read_end - stripes[role].read_start
                );
            }
        }
    }
}

int extend_missing_stripes(osd_rmw_stripe_t *stripes, osd_num_t *osd_set, int minsize, int size)
{
    for (int role = 0; role < minsize; role++)
    {
        if (stripes[role].read_end != 0 && osd_set[role] == 0)
        {
            stripes[role].missing = true;
            // Stripe is missing. Extend read to other stripes.
            // We need at least pg_minsize stripes to recover the lost part.
            // FIXME: LRC EC and similar don't require to read all other stripes.
            int exist = 0;
            for (int j = 0; j < size; j++)
            {
                if (osd_set[j] != 0)
                {
                    extend_read(stripes[role].read_start, stripes[role].read_end, stripes[j]);
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

void* alloc_read_buffer(osd_rmw_stripe_t *stripes, int read_pg_size, uint64_t add_size)
{
    // Calculate buffer size
    uint64_t buf_size = add_size;
    for (int role = 0; role < read_pg_size; role++)
    {
        if (stripes[role].read_end != 0)
        {
            buf_size += stripes[role].read_end - stripes[role].read_start;
        }
    }
    // Allocate buffer
    void *buf = memalign(MEM_ALIGNMENT, buf_size);
    uint64_t buf_pos = add_size;
    for (int role = 0; role < read_pg_size; role++)
    {
        if (stripes[role].read_end != 0)
        {
            stripes[role].read_buf = buf + buf_pos;
            buf_pos += stripes[role].read_end - stripes[role].read_start;
        }
    }
    return buf;
}

void* calc_rmw_reads(void *write_buf, osd_rmw_stripe_t *stripes, uint64_t *osd_set, uint64_t pg_size, uint64_t pg_minsize, uint64_t pg_cursize)
{
    // Generic parity modification (read-modify-write) algorithm
    // Reconstruct -> Read -> Calc parity -> Write
    // Now we always read continuous ranges. This means that an update of the beginning
    // of one data stripe and the end of another will lead to a read of full paired stripes.
    // FIXME: (Maybe) read small individual ranges in that case instead.
    uint32_t start = 0, end = 0;
    for (int role = 0; role < pg_minsize; role++)
    {
        if (stripes[role].req_end != 0)
        {
            start = !end || stripes[role].req_start < start ? stripes[role].req_start : start;
            end = std::max(stripes[role].req_end, end);
        }
    }
    for (int role = 0; role < pg_minsize; role++)
    {
        cover_read(start, end, stripes[role]);
    }
    int has_parity = 0;
    for (int role = pg_minsize; role < pg_size; role++)
    {
        if (osd_set[role] != 0)
        {
            has_parity++;
            stripes[role].write_start = start;
            stripes[role].write_end = end;
        }
        else
            stripes[role].missing = true;
    }
    if (pg_cursize < pg_size)
    {
        if (has_parity == 0)
        {
            // Parity is missing, we don't need to read anything
            for (int role = 0; role < pg_minsize; role++)
            {
                stripes[role].read_end = 0;
            }
        }
        else
        {
            // Other stripe(s) are missing
            for (int role = 0; role < pg_minsize; role++)
            {
                if (osd_set[role] == 0 && stripes[role].read_end != 0)
                {
                    stripes[role].missing = true;
                    for (int r2 = 0; r2 < pg_size; r2++)
                    {
                        // Read the non-covered range of <role> from all other stripes to reconstruct it
                        if (r2 != role && osd_set[r2] != 0)
                        {
                            extend_read(stripes[role].read_start, stripes[role].read_end, stripes[r2]);
                        }
                    }
                }
            }
        }
    }
    // Allocate read buffers
    void *rmw_buf = alloc_read_buffer(stripes, pg_size, has_parity * (end - start));
    // Position parity & write buffers
    uint64_t buf_pos = 0, in_pos = 0;
    for (int role = 0; role < pg_size; role++)
    {
        if (stripes[role].req_end != 0)
        {
            stripes[role].write_buf = write_buf + in_pos;
            in_pos += stripes[role].req_end - stripes[role].req_start;
        }
        else if (role >= pg_minsize && osd_set[role] != 0)
        {
            stripes[role].write_buf = rmw_buf + buf_pos;
            buf_pos += end - start;
        }
    }
    return rmw_buf;
}

static void get_old_new_buffers(osd_rmw_stripe_t & stripe, uint32_t wr_start, uint32_t wr_end, buf_len_t *bufs, int & nbufs)
{
    uint32_t ns = 0, ne = 0, os = 0, oe = 0;
    if (stripe.req_end > wr_start &&
        stripe.req_start < wr_end)
    {
        ns = std::max(stripe.req_start, wr_start);
        ne = std::min(stripe.req_end, wr_end);
    }
    if (stripe.read_end > wr_start &&
        stripe.read_start < wr_end)
    {
        os = std::max(stripe.read_start, wr_start);
        oe = std::min(stripe.req_end, wr_end);
    }
    if (ne && (!oe || ns <= os))
    {
        // NEW or NEW->OLD
        bufs[nbufs++] = { .buf = stripe.write_buf + ns - stripe.req_start, .len = ne-ns };
        if (os < ne)
            os = ne;
        if (oe > os)
        {
            // NEW->OLD
            bufs[nbufs++] = { .buf = stripe.read_buf + os - stripe.read_start, .len = oe-os };
        }
    }
    else if (oe)
    {
        // OLD or OLD->NEW or OLD->NEW->OLD
        if (ne)
        {
            // OLD->NEW or OLD->NEW->OLD
            bufs[nbufs++] = { .buf = stripe.read_buf + os - stripe.read_start, .len = ns-os };
            bufs[nbufs++] = { .buf = stripe.write_buf + ns - stripe.req_start, .len = ne-ns };
            if (oe > ne)
            {
                // OLD->NEW->OLD
                bufs[nbufs++] = { .buf = stripe.read_buf + ne - stripe.read_start, .len = oe-ne };
            }
        }
        else
        {
            // OLD
            bufs[nbufs++] = { .buf = stripe.read_buf + os - stripe.read_start, .len = oe-os };
        }
    }
}

static void xor_multiple_buffers(buf_len_t *xor1, int n1, buf_len_t *xor2, int n2, void *dest, uint32_t len)
{
    assert(n1 > 0 && n2 > 0);
    int i1 = 0, i2 = 0;
    uint32_t start1 = 0, start2 = 0, end1 = xor1[0].len, end2 = xor2[0].len;
    uint32_t pos = 0;
    while (pos < len)
    {
        // We know for sure that ranges overlap
        uint32_t end = std::min(end1, end2);
        memxor(xor1[i1].buf + pos-start1, xor2[i2].buf + pos-start2, dest+pos, end-pos);
        pos = end;
        if (pos >= end1)
        {
            i1++;
            if (i1 >= n1)
            {
                assert(pos >= end2);
                return;
            }
            start1 = end1;
            end1 += xor1[i1].len;
        }
        if (pos >= end2)
        {
            i2++;
            start2 = end2;
            end2 += xor2[i2].len;
        }
    }
}

void calc_rmw_parity(osd_rmw_stripe_t *stripes, int pg_size)
{
    if (stripes[pg_size-1].missing)
    {
        // Parity OSD is unavailable
        return;
    }
    for (int role = 0; role < pg_size; role++)
    {
        if (stripes[role].read_end != 0 && stripes[role].missing)
        {
            // Reconstruct missing stripe (EC k+1)
            reconstruct_stripe(stripes, pg_size, role);
            break;
        }
    }
    // Calculate new parity (EC k+1)
    int parity = pg_size-1, prev = -2;
    auto wr_end = stripes[parity].write_end;
    auto wr_start = stripes[parity].write_start;
    for (int other = 0; other < pg_size-1; other++)
    {
        if (prev == -2)
        {
            prev = other;
        }
        else
        {
            int n1 = 0, n2 = 0;
            buf_len_t xor1[3], xor2[3];
            if (prev == -1)
            {
                xor1[n1++] = { .buf = stripes[parity].write_buf, .len = wr_end-wr_start };
            }
            else
            {
                get_old_new_buffers(stripes[prev], wr_start, wr_end, xor1, n1);
                prev = -1;
            }
            get_old_new_buffers(stripes[other], wr_start, wr_end, xor2, n2);
            xor_multiple_buffers(xor1, n1, xor2, n2, stripes[parity].write_buf, wr_end-wr_start);
        }
    }
}
