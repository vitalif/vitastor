// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.0 (see README.md for details)

#include <stdexcept>
#include <string.h>
#include <assert.h>
#include <jerasure/reed_sol.h>
#include <jerasure.h>
#include <map>
#include "xor.h"
#include "osd_rmw.h"
#include "malloc_or_die.h"

#define OSD_JERASURE_W 32

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
    if (end == 0)
    {
        // Zero length request - offset doesn't matter
        return;
    }
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

void reconstruct_stripes_xor(osd_rmw_stripe_t *stripes, int pg_size)
{
    for (int role = 0; role < pg_size; role++)
    {
        if (stripes[role].read_end != 0 && stripes[role].missing)
        {
            // Reconstruct missing stripe (XOR k+1)
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
                        assert(stripes[role].read_start >= stripes[prev].read_start &&
                            stripes[role].read_start >= stripes[other].read_start);
                        memxor(
                            stripes[prev].read_buf + (stripes[role].read_start - stripes[prev].read_start),
                            stripes[other].read_buf + (stripes[role].read_start - stripes[other].read_start),
                            stripes[role].read_buf, stripes[role].read_end - stripes[role].read_start
                        );
                        prev = -1;
                    }
                    else
                    {
                        assert(stripes[role].read_start >= stripes[other].read_start);
                        memxor(
                            stripes[role].read_buf,
                            stripes[other].read_buf + (stripes[role].read_start - stripes[other].read_start),
                            stripes[role].read_buf, stripes[role].read_end - stripes[role].read_start
                        );
                    }
                }
            }
        }
    }
}

struct reed_sol_erased_t
{
    int *data;
    int size;
};

inline bool operator < (const reed_sol_erased_t &a, const reed_sol_erased_t &b)
{
    for (int i = 0; i < a.size && i < b.size; i++)
    {
        if (a.data[i] < b.data[i])
            return -1;
        else if (a.data[i] > b.data[i])
            return 1;
    }
    return 0;
}

struct reed_sol_matrix_t
{
    int refs = 0;
    int *data;
    std::map<reed_sol_erased_t, int*> decodings;
};

std::map<uint64_t, reed_sol_matrix_t> matrices;

void use_jerasure(int pg_size, int pg_minsize, bool use)
{
    uint64_t key = (uint64_t)pg_size | ((uint64_t)pg_minsize) << 32;
    auto rs_it = matrices.find(key);
    if (rs_it == matrices.end())
    {
        if (!use)
        {
            return;
        }
        int *matrix = reed_sol_vandermonde_coding_matrix(pg_minsize, pg_size-pg_minsize, OSD_JERASURE_W);
        matrices[key] = (reed_sol_matrix_t){
            .refs = 0,
            .data = matrix,
        };
        rs_it = matrices.find(key);
    }
    rs_it->second.refs += (!use ? -1 : 1);
    if (rs_it->second.refs <= 0)
    {
        free(rs_it->second.data);
        for (auto dec_it = rs_it->second.decodings.begin(); dec_it != rs_it->second.decodings.end();)
        {
            int *data = dec_it->second;
            rs_it->second.decodings.erase(dec_it++);
            free(data);
        }
        matrices.erase(rs_it);
    }
}

reed_sol_matrix_t* get_jerasure_matrix(int pg_size, int pg_minsize)
{
    uint64_t key = (uint64_t)pg_size | ((uint64_t)pg_minsize) << 32;
    auto rs_it = matrices.find(key);
    if (rs_it == matrices.end())
    {
        throw std::runtime_error("jerasure matrix not initialized");
    }
    return &rs_it->second;
}

// jerasure_matrix_decode() decodes all chunks at once and tries to reencode all missing coding chunks.
// we don't need it. also it makes an extra allocation of int *erased on every call and doesn't cache
// the decoding matrix.
// all these flaws are fixed in this function:
int* get_jerasure_decoding_matrix(osd_rmw_stripe_t *stripes, int pg_size, int pg_minsize)
{
    int edd = 0;
    int erased[pg_size] = { 0 };
    for (int i = 0; i < pg_size; i++)
        if (stripes[i].read_end == 0 || stripes[i].missing)
            erased[i] = 1;
    for (int i = 0; i < pg_minsize; i++)
        if (stripes[i].read_end != 0 && stripes[i].missing)
            edd++;
    if (edd == 0)
        return NULL;
    reed_sol_matrix_t *matrix = get_jerasure_matrix(pg_size, pg_minsize);
    auto dec_it = matrix->decodings.find((reed_sol_erased_t){ .data = erased, .size = pg_size });
    if (dec_it == matrix->decodings.end())
    {
        int *dm_ids = (int*)malloc(sizeof(int)*(pg_minsize + pg_minsize*pg_minsize + pg_size));
        int *decoding_matrix = dm_ids + pg_minsize;
        if (!dm_ids)
            throw std::bad_alloc();
        // we always use row_k_ones=1 and w=8 (OSD_JERASURE_W)
        if (jerasure_make_decoding_matrix(pg_minsize, pg_size-pg_minsize, OSD_JERASURE_W, matrix->data, erased, decoding_matrix, dm_ids) < 0)
        {
            free(dm_ids);
            throw std::runtime_error("jerasure_make_decoding_matrix() failed");
        }
        int *erased_copy = dm_ids + pg_minsize + pg_minsize*pg_minsize;
        memcpy(erased_copy, erased, pg_size*sizeof(int));
        matrix->decodings.emplace((reed_sol_erased_t){ .data = erased_copy, .size = pg_size }, dm_ids);
        return dm_ids;
    }
    return dec_it->second;
}

void reconstruct_stripes_jerasure(osd_rmw_stripe_t *stripes, int pg_size, int pg_minsize)
{
    int *dm_ids = get_jerasure_decoding_matrix(stripes, pg_size, pg_minsize);
    if (!dm_ids)
    {
        return;
    }
    int *decoding_matrix = dm_ids + pg_minsize;
    char *data_ptrs[pg_size] = { 0 };
    for (int role = 0; role < pg_minsize; role++)
    {
        if (stripes[role].read_end != 0 && stripes[role].missing)
        {
            for (int other = 0; other < pg_size; other++)
            {
                if (stripes[other].read_end != 0 && !stripes[other].missing)
                {
                    assert(stripes[other].read_start <= stripes[role].read_start);
                    assert(stripes[other].read_end >= stripes[role].read_end);
                    data_ptrs[other] = (char*)(stripes[other].read_buf + (stripes[role].read_start - stripes[other].read_start));
                }
            }
            data_ptrs[role] = (char*)stripes[role].read_buf;
            jerasure_matrix_dotprod(
                pg_minsize, OSD_JERASURE_W, decoding_matrix+(role*pg_minsize), dm_ids, role,
                data_ptrs, data_ptrs+pg_minsize, stripes[role].read_end - stripes[role].read_start
            );
        }
    }
}

int extend_missing_stripes(osd_rmw_stripe_t *stripes, osd_num_t *osd_set, int pg_minsize, int pg_size)
{
    for (int role = 0; role < pg_minsize; role++)
    {
        if (stripes[role].read_end != 0 && osd_set[role] == 0)
        {
            stripes[role].missing = true;
            // Stripe is missing. Extend read to other stripes.
            // We need at least pg_minsize stripes to recover the lost part.
            // FIXME: LRC EC and similar don't require to read all other stripes.
            int exist = 0;
            for (int j = 0; j < pg_size; j++)
            {
                if (osd_set[j] != 0)
                {
                    extend_read(stripes[role].read_start, stripes[role].read_end, stripes[j]);
                    exist++;
                    if (exist >= pg_minsize)
                    {
                        break;
                    }
                }
            }
            if (exist < pg_minsize)
            {
                // Less than pg_minsize stripes are available for this object
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
    void *buf = memalign_or_die(MEM_ALIGNMENT, buf_size);
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

void* calc_rmw(void *request_buf, osd_rmw_stripe_t *stripes, uint64_t *read_osd_set,
    uint64_t pg_size, uint64_t pg_minsize, uint64_t pg_cursize, uint64_t *write_osd_set, uint64_t chunk_size)
{
    // Generic parity modification (read-modify-write) algorithm
    // Read -> Reconstruct missing chunks -> Calc parity chunks -> Write
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
            stripes[role].write_start = stripes[role].req_start;
            stripes[role].write_end = stripes[role].req_end;
        }
    }
    int write_parity = 0;
    for (int role = pg_minsize; role < pg_size; role++)
    {
        if (write_osd_set[role] != 0)
        {
            write_parity = 1;
            if (write_osd_set[role] != read_osd_set[role])
            {
                start = 0;
                end = chunk_size;
                for (int r2 = pg_minsize; r2 < role; r2++)
                {
                    stripes[r2].write_start = start;
                    stripes[r2].write_end = end;
                }
            }
            stripes[role].write_start = start;
            stripes[role].write_end = end;
        }
    }
    if (write_parity)
    {
        for (int role = 0; role < pg_minsize; role++)
        {
            cover_read(start, end, stripes[role]);
        }
    }
    if (write_osd_set != read_osd_set)
    {
        pg_cursize = 0;
        // Object is degraded/misplaced and will be moved to <write_osd_set>
        for (int role = 0; role < pg_size; role++)
        {
            if (role < pg_minsize && write_osd_set[role] != read_osd_set[role] && write_osd_set[role] != 0)
            {
                // We need to get data for any moved / recovered chunk
                // And we need a continuous write buffer so we'll only optimize
                // for the case when the whole chunk is ovewritten in the request
                if (stripes[role].req_start != 0 ||
                    stripes[role].req_end != chunk_size)
                {
                    stripes[role].read_start = 0;
                    stripes[role].read_end = chunk_size;
                    // Warning: We don't modify write_start/write_end here, we do it in calc_rmw_parity()
                }
            }
            if (read_osd_set[role] != 0)
            {
                pg_cursize++;
            }
        }
    }
    if (pg_cursize < pg_size)
    {
        // Some stripe(s) are missing, so we need to read parity
        for (int role = 0; role < pg_size; role++)
        {
            if (read_osd_set[role] == 0)
            {
                stripes[role].missing = true;
                if (stripes[role].read_end != 0)
                {
                    int found = 0;
                    for (int r2 = 0; r2 < pg_size && found < pg_minsize; r2++)
                    {
                        // Read the non-covered range of <role> from at least <minsize> other stripes to reconstruct it
                        if (read_osd_set[r2] != 0)
                        {
                            extend_read(stripes[role].read_start, stripes[role].read_end, stripes[r2]);
                            found++;
                        }
                    }
                    if (found < pg_minsize)
                    {
                        // Object is incomplete - refuse partial overwrite
                        return NULL;
                    }
                }
            }
        }
    }
    // Allocate read buffers
    void *rmw_buf = alloc_read_buffer(stripes, pg_size, (write_parity ? pg_size-pg_minsize : 0) * (end - start));
    // Position write buffers
    uint64_t buf_pos = 0, in_pos = 0;
    for (int role = 0; role < pg_size; role++)
    {
        if (stripes[role].req_end != 0)
        {
            stripes[role].write_buf = request_buf + in_pos;
            in_pos += stripes[role].req_end - stripes[role].req_start;
        }
        else if (role >= pg_minsize && write_osd_set[role] != 0 && end != 0)
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
        oe = std::min(stripe.read_end, wr_end);
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

static void calc_rmw_parity_copy_mod(osd_rmw_stripe_t *stripes, int pg_size, int pg_minsize,
    uint64_t *read_osd_set, uint64_t *write_osd_set, uint32_t chunk_size, uint32_t &start, uint32_t &end)
{
    if (write_osd_set[pg_minsize] != 0 || write_osd_set != read_osd_set)
    {
        // Required for the next two if()s
        for (int role = 0; role < pg_minsize; role++)
        {
            if (stripes[role].req_end != 0)
            {
                start = !end || stripes[role].req_start < start ? stripes[role].req_start : start;
                end = std::max(stripes[role].req_end, end);
            }
        }
        for (int role = pg_minsize; role < pg_size; role++)
        {
            if (write_osd_set[role] != 0 && write_osd_set[role] != read_osd_set[role])
            {
                start = 0;
                end = chunk_size;
            }
        }
    }
    if (write_osd_set != read_osd_set)
    {
        for (int role = 0; role < pg_minsize; role++)
        {
            if (write_osd_set[role] != read_osd_set[role] && write_osd_set[role] != 0 &&
                (stripes[role].req_start != 0 || stripes[role].req_end != chunk_size))
            {
                // Copy modified chunk into the read buffer to write it back
                memcpy(
                    stripes[role].read_buf + stripes[role].req_start,
                    stripes[role].write_buf,
                    stripes[role].req_end - stripes[role].req_start
                );
                stripes[role].write_buf = stripes[role].read_buf;
                stripes[role].write_start = 0;
                stripes[role].write_end = chunk_size;
            }
        }
    }
}

static void calc_rmw_parity_copy_parity(osd_rmw_stripe_t *stripes, int pg_size, int pg_minsize,
    uint64_t *read_osd_set, uint64_t *write_osd_set, uint32_t chunk_size, uint32_t start, uint32_t end)
{
    if (write_osd_set != read_osd_set)
    {
        for (int role = pg_minsize; role < pg_size; role++)
        {
            if (write_osd_set[role] != read_osd_set[role] && (start != 0 || end != chunk_size))
            {
                // Copy new parity into the read buffer to write it back
                memcpy(
                    stripes[role].read_buf + start,
                    stripes[role].write_buf,
                    end - start
                );
                stripes[role].write_buf = stripes[role].read_buf;
                stripes[role].write_start = 0;
                stripes[role].write_end = chunk_size;
            }
        }
    }
#ifdef RMW_DEBUG
    printf("calc_rmw_parity:\n");
    for (int role = 0; role < pg_size; role++)
    {
        auto & s = stripes[role];
        printf(
            "Tr=%lu Tw=%lu Q=%x-%x R=%x-%x W=%x-%x Rb=%lx Wb=%lx\n",
            read_osd_set[role], write_osd_set[role],
            s.req_start, s.req_end,
            s.read_start, s.read_end,
            s.write_start, s.write_end,
            (uint64_t)s.read_buf,
            (uint64_t)s.write_buf
        );
    }
#endif
}

void calc_rmw_parity_xor(osd_rmw_stripe_t *stripes, int pg_size, uint64_t *read_osd_set, uint64_t *write_osd_set, uint32_t chunk_size)
{
    int pg_minsize = pg_size-1;
    reconstruct_stripes_xor(stripes, pg_size);
    uint32_t start = 0, end = 0;
    calc_rmw_parity_copy_mod(stripes, pg_size, pg_minsize, read_osd_set, write_osd_set, chunk_size, start, end);
    if (write_osd_set[pg_minsize] != 0 && end != 0)
    {
        // Calculate new parity (XOR k+1)
        int parity = pg_minsize, prev = -2;
        for (int other = 0; other < pg_minsize; other++)
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
                    xor1[n1++] = { .buf = stripes[parity].write_buf, .len = end-start };
                }
                else
                {
                    get_old_new_buffers(stripes[prev], start, end, xor1, n1);
                    prev = -1;
                }
                get_old_new_buffers(stripes[other], start, end, xor2, n2);
                xor_multiple_buffers(xor1, n1, xor2, n2, stripes[parity].write_buf, end-start);
            }
        }
    }
    calc_rmw_parity_copy_parity(stripes, pg_size, pg_minsize, read_osd_set, write_osd_set, chunk_size, start, end);
}

void calc_rmw_parity_jerasure(osd_rmw_stripe_t *stripes, int pg_size, int pg_minsize,
    uint64_t *read_osd_set, uint64_t *write_osd_set, uint32_t chunk_size)
{
    reed_sol_matrix_t *matrix = get_jerasure_matrix(pg_size, pg_minsize);
    reconstruct_stripes_jerasure(stripes, pg_size, pg_minsize);
    uint32_t start = 0, end = 0;
    calc_rmw_parity_copy_mod(stripes, pg_size, pg_minsize, read_osd_set, write_osd_set, chunk_size, start, end);
    if (end != 0)
    {
        int i;
        for (i = pg_minsize; i < pg_size; i++)
        {
            if (write_osd_set[i] != 0)
                break;
        }
        if (i < pg_size)
        {
            // Calculate new coding chunks
            buf_len_t bufs[pg_size][3];
            int nbuf[pg_size] = { 0 }, curbuf[pg_size] = { 0 };
            uint32_t positions[pg_size];
            void *data_ptrs[pg_size] = { 0 };
            for (int i = 0; i < pg_minsize; i++)
            {
                get_old_new_buffers(stripes[i], start, end, bufs[i], nbuf[i]);
                positions[i] = start;
            }
            for (int i = pg_minsize; i < pg_size; i++)
            {
                bufs[i][nbuf[i]++] = { .buf = stripes[i].write_buf, .len = end-start };
                positions[i] = start;
            }
            uint32_t pos = start;
            while (pos < end)
            {
                uint32_t next_end = end;
                for (int i = 0; i < pg_size; i++)
                {
                    assert(curbuf[i] < nbuf[i]);
                    assert(bufs[i][curbuf[i]].buf);
                    data_ptrs[i] = bufs[i][curbuf[i]].buf + pos-positions[i];
                    uint32_t this_end = bufs[i][curbuf[i]].len + positions[i];
                    if (next_end > this_end)
                        next_end = this_end;
                }
                assert(next_end > pos);
                for (int i = 0; i < pg_size; i++)
                {
                    uint32_t this_end = bufs[i][curbuf[i]].len + positions[i];
                    if (next_end >= this_end)
                    {
                        positions[i] += bufs[i][curbuf[i]].len;
                        curbuf[i]++;
                    }
                }
                jerasure_matrix_encode(
                    pg_minsize, pg_size-pg_minsize, OSD_JERASURE_W, matrix->data,
                    (char**)data_ptrs, (char**)data_ptrs+pg_minsize, next_end-pos
                );
                pos = next_end;
            }
        }
    }
    calc_rmw_parity_copy_parity(stripes, pg_size, pg_minsize, read_osd_set, write_osd_set, chunk_size, start, end);
}
