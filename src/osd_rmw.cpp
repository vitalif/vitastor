// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <stdexcept>
#include <string.h>
#include <assert.h>
extern "C" {
#include <reed_sol.h>
#include <jerasure.h>
#ifdef WITH_ISAL
#include <isa-l/erasure_code.h>
#endif
}
#include <map>
#include "allocator.h"
#include "xor.h"
#include "osd_rmw.h"
#include "malloc_or_die.h"

#define OSD_JERASURE_W 8

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

void reconstruct_stripes_xor(osd_rmw_stripe_t *stripes, int pg_size, uint32_t bitmap_size)
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
                            (uint8_t*)stripes[prev].read_buf + (stripes[role].read_start - stripes[prev].read_start),
                            (uint8_t*)stripes[other].read_buf + (stripes[role].read_start - stripes[other].read_start),
                            stripes[role].read_buf, stripes[role].read_end - stripes[role].read_start
                        );
                        memxor(stripes[prev].bmp_buf, stripes[other].bmp_buf, stripes[role].bmp_buf, bitmap_size);
                        prev = -1;
                    }
                    else
                    {
                        assert(stripes[role].read_start >= stripes[other].read_start);
                        memxor(
                            stripes[role].read_buf,
                            (uint8_t*)stripes[other].read_buf + (stripes[role].read_start - stripes[other].read_start),
                            stripes[role].read_buf, stripes[role].read_end - stripes[role].read_start
                        );
                        memxor(stripes[role].bmp_buf, stripes[other].bmp_buf, stripes[role].bmp_buf, bitmap_size);
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
            return true;
        else if (a.data[i] > b.data[i])
            return false;
    }
    return false;
}

struct reed_sol_matrix_t
{
    int refs = 0;
    int *je_data;
    uint8_t *isal_data;
    // 32 bytes = 256/8 = max pg_size/8
    std::map<std::array<uint8_t, 32>, void*> subdata;
    std::map<reed_sol_erased_t, void*> decodings;
};

static std::map<uint64_t, reed_sol_matrix_t> matrices;

void use_ec(int pg_size, int pg_minsize, bool use)
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
        uint8_t *isal_table = NULL;
#ifdef WITH_ISAL
        uint8_t *isal_matrix = (uint8_t*)malloc_or_die(pg_minsize*(pg_size-pg_minsize));
        for (int i = 0; i < pg_minsize*(pg_size-pg_minsize); i++)
        {
            isal_matrix[i] = matrix[i];
        }
        isal_table = (uint8_t*)malloc_or_die(pg_minsize*(pg_size-pg_minsize)*32);
        ec_init_tables(pg_minsize, pg_size-pg_minsize, isal_matrix, isal_table);
        free(isal_matrix);
#endif
        matrices[key] = (reed_sol_matrix_t){
            .refs = 0,
            .je_data = matrix,
            .isal_data = isal_table,
        };
        rs_it = matrices.find(key);
    }
    rs_it->second.refs += (!use ? -1 : 1);
    if (rs_it->second.refs <= 0)
    {
        free(rs_it->second.je_data);
        if (rs_it->second.isal_data)
            free(rs_it->second.isal_data);
        for (auto sub_it = rs_it->second.subdata.begin(); sub_it != rs_it->second.subdata.end();)
        {
            void *data = sub_it->second;
            rs_it->second.subdata.erase(sub_it++);
            free(data);
        }
        for (auto dec_it = rs_it->second.decodings.begin(); dec_it != rs_it->second.decodings.end();)
        {
            void *data = dec_it->second;
            rs_it->second.decodings.erase(dec_it++);
            free(data);
        }
        matrices.erase(rs_it);
    }
}

static reed_sol_matrix_t* get_ec_matrix(int pg_size, int pg_minsize)
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
static void* get_jerasure_decoding_matrix(osd_rmw_stripe_t *stripes, int pg_size, int pg_minsize)
{
    int edd = 0;
    int erased[pg_size];
    for (int i = 0; i < pg_size; i++)
        erased[i] = (stripes[i].read_end == 0 || stripes[i].missing ? 1 : 0);
    for (int i = 0; i < pg_minsize; i++)
        if (stripes[i].read_end != 0 && stripes[i].missing)
            edd++;
    if (edd == 0)
        return NULL;
    reed_sol_matrix_t *matrix = get_ec_matrix(pg_size, pg_minsize);
    auto dec_it = matrix->decodings.find((reed_sol_erased_t){ .data = erased, .size = pg_size });
    if (dec_it == matrix->decodings.end())
    {
#ifdef WITH_ISAL
        int smrow = 0;
        uint8_t *submatrix = (uint8_t*)malloc_or_die(pg_minsize*pg_minsize*2);
        for (int i = 0; i < pg_size; i++)
        {
            if (!erased[i])
            {
                if (i < pg_minsize)
                {
                    for (int j = 0; j < pg_minsize; j++)
                        submatrix[smrow*pg_minsize + j] = j == i;
                }
                else
                {
                    for (int j = 0; j < pg_minsize; j++)
                        submatrix[smrow*pg_minsize + j] = (uint8_t)matrix->je_data[(i-pg_minsize)*pg_minsize + j];
                }
                smrow++;
            }
        }
        if (smrow < pg_minsize)
        {
            free(submatrix);
            throw std::runtime_error("failed to make an invertible submatrix");
        }
        gf_invert_matrix(submatrix, submatrix + pg_minsize*pg_minsize, pg_minsize);
        smrow = 0;
        for (int i = 0; i < pg_minsize; i++)
        {
            if (erased[i])
            {
                memcpy(submatrix + pg_minsize*smrow, submatrix + (pg_minsize+i)*pg_minsize, pg_minsize);
                smrow++;
            }
        }
        uint8_t *rectable = (uint8_t*)malloc_or_die(32*smrow*pg_minsize + pg_size*sizeof(int));
        ec_init_tables(pg_minsize, smrow, submatrix, rectable);
        free(submatrix);
        int *erased_copy = (int*)(rectable + 32*smrow*pg_minsize);
        memcpy(erased_copy, erased, pg_size*sizeof(int));
        matrix->decodings.emplace((reed_sol_erased_t){ .data = erased_copy, .size = pg_size }, rectable);
        return rectable;
#else
        int *dm_ids = (int*)malloc_or_die(sizeof(int)*(pg_minsize + pg_minsize*pg_minsize + pg_size));
        int *decoding_matrix = dm_ids + pg_minsize;
        // we always use row_k_ones=1 and w=8 (OSD_JERASURE_W)
        if (jerasure_make_decoding_matrix(pg_minsize, pg_size-pg_minsize, OSD_JERASURE_W, matrix->je_data, erased, decoding_matrix, dm_ids) < 0)
        {
            free(dm_ids);
            throw std::runtime_error("jerasure_make_decoding_matrix() failed");
        }
        int *erased_copy = dm_ids + pg_minsize + pg_minsize*pg_minsize;
        memcpy(erased_copy, erased, pg_size*sizeof(int));
        matrix->decodings.emplace((reed_sol_erased_t){ .data = erased_copy, .size = pg_size }, dm_ids);
        return dm_ids;
#endif
    }
    return dec_it->second;
}

#ifndef WITH_ISAL
#define JERASURE_ALIGNMENT 16

// jerasure requires 16-byte alignment for SSE...
// FIXME: jerasure/gf-complete should probably be patched to automatically choose non-sse version for unaligned buffers
static void jerasure_matrix_encode_unaligned(int k, int m, int w, int *matrix, char **data_ptrs, char **coding_ptrs, int size)
{
    bool unaligned = false;
    for (int i = 0; i < k; i++)
        if (((unsigned long)data_ptrs[i]) % JERASURE_ALIGNMENT)
            unaligned = true;
    for (int i = 0; i < m; i++)
        if (((unsigned long)coding_ptrs[i]) % JERASURE_ALIGNMENT)
            unaligned = true;
    if (!unaligned)
    {
        jerasure_matrix_encode(k, m, w, matrix, data_ptrs, coding_ptrs, size);
        return;
    }
    int aligned_size = ((size+JERASURE_ALIGNMENT-1)/JERASURE_ALIGNMENT)*JERASURE_ALIGNMENT;
    int copy_size = aligned_size*(k+m);
    char local_data[copy_size > 4096 ? 0 : copy_size];
    char *data_copy = copy_size > 4096 || (unsigned long)local_data % JERASURE_ALIGNMENT
        ? (char*)memalign_or_die(JERASURE_ALIGNMENT, aligned_size*(k+m))
        : local_data;
    char *aligned_ptrs[k+m];
    for (int i = 0; i < k; i++)
    {
        memcpy(data_copy + i*aligned_size, data_ptrs[i], size);
        aligned_ptrs[i] = data_copy + i*aligned_size;
    }
    for (int i = 0; i < m; i++)
        aligned_ptrs[k+i] = data_copy + (k+i)*aligned_size;
    jerasure_matrix_encode(k, m, w, matrix, aligned_ptrs, aligned_ptrs+k, size);
    for (int i = 0; i < m; i++)
        memcpy(coding_ptrs[i], aligned_ptrs[k+i], size);
    if (copy_size > 4096 || (unsigned long)local_data % JERASURE_ALIGNMENT)
        free(data_copy);
}
#endif

#ifdef WITH_ISAL
void reconstruct_stripes_ec(osd_rmw_stripe_t *stripes, int pg_size, int pg_minsize, uint32_t bitmap_size)
{
    uint8_t *dectable = (uint8_t*)get_jerasure_decoding_matrix(stripes, pg_size, pg_minsize);
    if (!dectable)
    {
        return;
    }
    uint8_t *data_ptrs[pg_size];
    int wanted_base = 0, wanted = 0;
    uint64_t read_start = 0, read_end = 0;
    auto recover_seq = [&]()
    {
        int orig = 0;
        for (int other = 0; other < pg_size; other++)
        {
            if (stripes[other].read_end != 0 && !stripes[other].missing)
            {
                assert(stripes[other].read_start <= read_start);
                assert(stripes[other].read_end >= read_end);
                data_ptrs[orig++] = (uint8_t*)stripes[other].read_buf + (read_start - stripes[other].read_start);
            }
        }
        ec_encode_data(
            read_end-read_start, pg_minsize, wanted, dectable + wanted_base*32*pg_minsize,
            data_ptrs, data_ptrs + pg_minsize
        );
        wanted_base += wanted;
        wanted = 0;
    };
    for (int role = 0; role < pg_minsize; role++)
    {
        if (stripes[role].read_end != 0 && stripes[role].missing)
        {
            if (read_end && (stripes[role].read_start != read_start ||
                stripes[role].read_end != read_end))
            {
                recover_seq();
            }
            read_start = stripes[role].read_start;
            read_end = stripes[role].read_end;
            data_ptrs[pg_minsize + (wanted++)] = (uint8_t*)stripes[role].read_buf;
        }
    }
    if (wanted > 0)
    {
        recover_seq();
    }
}
#else
void reconstruct_stripes_ec(osd_rmw_stripe_t *stripes, int pg_size, int pg_minsize, uint32_t bitmap_size)
{
    int *dm_ids = (int*)get_jerasure_decoding_matrix(stripes, pg_size, pg_minsize);
    if (!dm_ids)
    {
        return;
    }
    int *decoding_matrix = dm_ids + pg_minsize;
    char *data_ptrs[pg_size];
    for (int role = 0; role < pg_size; role++)
    {
        data_ptrs[role] = NULL;
    }
    bool recovered = false;
    for (int role = 0; role < pg_minsize; role++)
    {
        if (stripes[role].read_end != 0 && stripes[role].missing)
        {
            recovered = true;
            if (stripes[role].read_end > stripes[role].read_start)
            {
                for (int other = 0; other < pg_size; other++)
                {
                    if (stripes[other].read_end != 0 && !stripes[other].missing)
                    {
                        assert(stripes[other].read_start <= stripes[role].read_start);
                        assert(stripes[other].read_end >= stripes[role].read_end);
                        data_ptrs[other] = (char*)stripes[other].read_buf + (stripes[role].read_start - stripes[other].read_start);
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
    if (recovered && bitmap_size > 0)
    {
        bool unaligned = false;
        for (int role = 0; role < pg_size; role++)
        {
            if (stripes[role].read_end != 0)
            {
                data_ptrs[role] = (char*)stripes[role].bmp_buf;
                if (((unsigned long)stripes[role].bmp_buf) % JERASURE_ALIGNMENT)
                    unaligned = true;
            }
        }
        if (!unaligned)
        {
            for (int role = 0; role < pg_minsize; role++)
            {
                if (stripes[role].read_end != 0 && stripes[role].missing)
                {
                    jerasure_matrix_dotprod(
                        pg_minsize, OSD_JERASURE_W, decoding_matrix+(role*pg_minsize), dm_ids, role,
                        data_ptrs, data_ptrs+pg_minsize, bitmap_size
                    );
                }
            }
        }
        else
        {
            // jerasure_matrix_dotprod requires 16-byte alignment for SSE...
            int aligned_size = ((bitmap_size+JERASURE_ALIGNMENT-1)/JERASURE_ALIGNMENT)*JERASURE_ALIGNMENT;
            int copy_size = aligned_size*pg_size;
            char local_data[copy_size > 4096 ? 0 : copy_size];
            bool alloc_copy = copy_size > 4096 || (unsigned long)local_data % JERASURE_ALIGNMENT;
            char *data_copy = alloc_copy
                ? (char*)memalign_or_die(JERASURE_ALIGNMENT, copy_size)
                : local_data;
            for (int role = 0; role < pg_size; role++)
            {
                if (stripes[role].read_end != 0)
                {
                    data_ptrs[role] = data_copy + role*aligned_size;
                    memcpy(data_ptrs[role], stripes[role].bmp_buf, bitmap_size);
                }
            }
            for (int role = 0; role < pg_size; role++)
            {
                if (stripes[role].read_end != 0 && stripes[role].missing)
                {
                    jerasure_matrix_dotprod(
                        pg_minsize, OSD_JERASURE_W, decoding_matrix+(role*pg_minsize), dm_ids, role,
                        data_ptrs, data_ptrs+pg_minsize, bitmap_size
                    );
                    memcpy(stripes[role].bmp_buf, data_ptrs[role], bitmap_size);
                }
            }
            if (alloc_copy)
                free(data_copy);
        }
    }
}
#endif

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
            stripes[role].read_buf = (uint8_t*)buf + buf_pos;
            buf_pos += stripes[role].read_end - stripes[role].read_start;
        }
    }
    return buf;
}

void* calc_rmw(void *request_buf, osd_rmw_stripe_t *stripes, uint64_t *read_osd_set,
    uint64_t pg_size, uint64_t pg_minsize, uint64_t pg_cursize, uint64_t *write_osd_set,
    uint64_t chunk_size, uint32_t bitmap_size)
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
            write_parity++;
            if (write_osd_set[role] != read_osd_set[role])
            {
                start = 0;
                end = chunk_size;
                for (int r2 = pg_minsize; r2 < role; r2++)
                {
                    if (write_osd_set[r2] != 0)
                    {
                        stripes[r2].write_start = start;
                        stripes[r2].write_end = end;
                    }
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
    void *rmw_buf = alloc_read_buffer(stripes, pg_size, write_parity * (end - start));
    // Position write buffers
    uint64_t buf_pos = 0, in_pos = 0;
    for (int role = 0; role < pg_size; role++)
    {
        if (stripes[role].req_end != 0)
        {
            stripes[role].write_buf = (uint8_t*)request_buf + in_pos;
            in_pos += stripes[role].req_end - stripes[role].req_start;
        }
        else if (role >= pg_minsize && write_osd_set[role] != 0 && end != 0)
        {
            stripes[role].write_buf = (uint8_t*)rmw_buf + buf_pos;
            buf_pos += end - start;
        }
    }
    return rmw_buf;
}

static void get_old_new_buffers(osd_rmw_stripe_t & stripe, uint32_t wr_start, uint32_t wr_end, buf_len_t *bufs, int & nbufs)
{
    uint32_t ns = 0, ne = 0, os = 0, oe = 0;
    if (stripe.write_end > wr_start &&
        stripe.write_start < wr_end)
    {
        ns = std::max(stripe.write_start, wr_start);
        ne = std::min(stripe.write_end, wr_end);
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
        bufs[nbufs++] = { .buf = (uint8_t*)stripe.write_buf + ns - stripe.write_start, .len = ne-ns };
        if (os < ne)
            os = ne;
        if (oe > os)
        {
            // NEW->OLD
            bufs[nbufs++] = { .buf = (uint8_t*)stripe.read_buf + os - stripe.read_start, .len = oe-os };
        }
    }
    else if (oe)
    {
        // OLD or OLD->NEW or OLD->NEW->OLD
        if (ne)
        {
            // OLD->NEW or OLD->NEW->OLD
            bufs[nbufs++] = { .buf = (uint8_t*)stripe.read_buf + os - stripe.read_start, .len = ns-os };
            bufs[nbufs++] = { .buf = (uint8_t*)stripe.write_buf + ns - stripe.write_start, .len = ne-ns };
            if (oe > ne)
            {
                // OLD->NEW->OLD
                bufs[nbufs++] = { .buf = (uint8_t*)stripe.read_buf + ne - stripe.read_start, .len = oe-ne };
            }
        }
        else
        {
            // OLD
            bufs[nbufs++] = { .buf = (uint8_t*)stripe.read_buf + os - stripe.read_start, .len = oe-os };
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
        memxor((uint8_t*)xor1[i1].buf + pos-start1, (uint8_t*)xor2[i2].buf + pos-start2, (uint8_t*)dest+pos, end-pos);
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
    uint64_t *read_osd_set, uint64_t *write_osd_set, uint32_t chunk_size, uint32_t bitmap_granularity,
    uint32_t &start, uint32_t &end)
{
    bool required = false;
    for (int role = pg_minsize; role < pg_size; role++)
    {
        if (write_osd_set[role] != 0)
        {
            // Whole parity chunk is needed when we move the object
            if (write_osd_set[role] != read_osd_set[role])
                end = chunk_size;
            required = true;
        }
    }
    if (required && end != chunk_size)
    {
        // start & end are required for calc_rmw_parity
        for (int role = 0; role < pg_minsize; role++)
        {
            if (stripes[role].req_end != 0)
            {
                start = !end || stripes[role].req_start < start ? stripes[role].req_start : start;
                end = std::max(stripes[role].req_end, end);
            }
        }
    }
    // Set bitmap bits accordingly
    if (bitmap_granularity > 0)
    {
        for (int role = 0; role < pg_minsize; role++)
        {
            if (stripes[role].req_end != 0)
            {
                bitmap_set(
                    stripes[role].bmp_buf, stripes[role].req_start,
                    stripes[role].req_end-stripes[role].req_start, bitmap_granularity
                );
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
                    (uint8_t*)stripes[role].read_buf + stripes[role].req_start,
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
                    (uint8_t*)stripes[role].read_buf + start,
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

void calc_rmw_parity_xor(osd_rmw_stripe_t *stripes, int pg_size, uint64_t *read_osd_set, uint64_t *write_osd_set,
    uint32_t chunk_size, uint32_t bitmap_size)
{
    uint32_t bitmap_granularity = bitmap_size > 0 ? chunk_size / bitmap_size / 8 : 0;
    int pg_minsize = pg_size-1;
    reconstruct_stripes_xor(stripes, pg_size, bitmap_size);
    uint32_t start = 0, end = 0;
    calc_rmw_parity_copy_mod(stripes, pg_size, pg_minsize, read_osd_set, write_osd_set, chunk_size, bitmap_granularity, start, end);
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
                    memxor(stripes[parity].bmp_buf, stripes[other].bmp_buf, stripes[parity].bmp_buf, bitmap_size);
                }
                else
                {
                    memxor(stripes[prev].bmp_buf, stripes[other].bmp_buf, stripes[parity].bmp_buf, bitmap_size);
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

void calc_rmw_parity_ec(osd_rmw_stripe_t *stripes, int pg_size, int pg_minsize,
    uint64_t *read_osd_set, uint64_t *write_osd_set, uint32_t chunk_size, uint32_t bitmap_size)
{
    uint32_t bitmap_granularity = bitmap_size > 0 ? chunk_size / bitmap_size / 8 : 0;
    reed_sol_matrix_t *matrix = get_ec_matrix(pg_size, pg_minsize);
    reconstruct_stripes_ec(stripes, pg_size, pg_minsize, bitmap_size);
    uint32_t start = 0, end = 0;
    calc_rmw_parity_copy_mod(stripes, pg_size, pg_minsize, read_osd_set, write_osd_set, chunk_size, bitmap_granularity, start, end);
    if (end != 0)
    {
        int write_parity = 0;
        bool is_seq = true;
        for (int i = pg_size-1; i >= pg_minsize; i--)
        {
            if (write_osd_set[i] != 0)
                write_parity++;
            else if (write_parity != 0)
                is_seq = false;
        }
        if (write_parity > 0)
        {
            // First get the coding matrix or sub-matrix
            void *matrix_data =
#ifdef WITH_ISAL
                matrix->isal_data;
#else
                matrix->je_data;
#endif
            if (!is_seq)
            {
                // We need a coding sub-matrix
                std::array<uint8_t, 32> missing_parity = {};
                for (int i = pg_minsize; i < pg_size; i++)
                {
                    if (!write_osd_set[i])
                        missing_parity[(i-pg_minsize) >> 3] |= (1 << ((i-pg_minsize) & 0x7));
                }
                auto sub_it = matrix->subdata.find(missing_parity);
                if (sub_it == matrix->subdata.end())
                {
                    int item_size =
#ifdef WITH_ISAL
                        32;
#else
                        sizeof(int);
#endif
                    void *subm = malloc_or_die(item_size * write_parity * pg_minsize);
                    for (int i = pg_minsize, j = 0; i < pg_size; i++)
                    {
                        if (write_osd_set[i])
                        {
                            memcpy((uint8_t*)subm + item_size*pg_minsize*j, (uint8_t*)matrix_data + item_size*pg_minsize*(i-pg_minsize), item_size*pg_minsize);
                            j++;
                        }
                    }
                    matrix->subdata[missing_parity] = subm;
                    matrix_data = subm;
                }
                else
                    matrix_data = sub_it->second;
            }
            // Calculate new coding chunks
            buf_len_t bufs[pg_size][3];
            int nbuf[pg_size], curbuf[pg_size];
            uint32_t positions[pg_size];
            void *data_ptrs[pg_size];
            for (int i = 0; i < pg_size; i++)
            {
                data_ptrs[i] = NULL;
                nbuf[i] = 0;
                curbuf[i] = 0;
            }
            for (int i = 0; i < pg_minsize; i++)
            {
                get_old_new_buffers(stripes[i], start, end, bufs[i], nbuf[i]);
                positions[i] = start;
            }
            for (int i = pg_minsize; i < pg_size; i++)
            {
                if (write_osd_set[i] != 0)
                {
                    bufs[i][nbuf[i]++] = { .buf = stripes[i].write_buf, .len = end-start };
                    positions[i] = start;
                }
            }
            uint32_t pos = start;
            while (pos < end)
            {
                uint32_t next_end = end;
                for (int i = 0, j = 0; i < pg_size; i++)
                {
                    if (i < pg_minsize || write_osd_set[i] != 0)
                    {
                        assert(curbuf[i] < nbuf[i]);
                        assert(bufs[i][curbuf[i]].buf);
                        data_ptrs[j++] = (uint8_t*)bufs[i][curbuf[i]].buf + pos-positions[i];
                        uint32_t this_end = bufs[i][curbuf[i]].len + positions[i];
                        if (next_end > this_end)
                            next_end = this_end;
                    }
                }
                assert(next_end > pos);
                for (int i = 0; i < pg_size; i++)
                {
                    if (i < pg_minsize || write_osd_set[i] != 0)
                    {
                        uint32_t this_end = bufs[i][curbuf[i]].len + positions[i];
                        if (next_end >= this_end)
                        {
                            positions[i] += bufs[i][curbuf[i]].len;
                            curbuf[i]++;
                        }
                    }
                }
#ifdef WITH_ISAL
                ec_encode_data(
                    next_end-pos, pg_minsize, write_parity, (uint8_t*)matrix_data,
                    (uint8_t**)data_ptrs, (uint8_t**)data_ptrs+pg_minsize
                );
#else
                jerasure_matrix_encode(
                    pg_minsize, write_parity, OSD_JERASURE_W, (int*)matrix_data,
                    (char**)data_ptrs, (char**)data_ptrs+pg_minsize, next_end-pos
                );
#endif
                pos = next_end;
            }
            for (int i = 0, j = 0; i < pg_size; i++)
            {
                if (i < pg_minsize || write_osd_set[i] != 0)
                    data_ptrs[j++] = stripes[i].bmp_buf;
            }
#ifdef WITH_ISAL
            ec_encode_data(
                bitmap_size, pg_minsize, write_parity, (uint8_t*)matrix_data,
                (uint8_t**)data_ptrs, (uint8_t**)data_ptrs+pg_minsize
            );
#else
            jerasure_matrix_encode_unaligned(
                pg_minsize, write_parity, OSD_JERASURE_W, (int*)matrix_data,
                (char**)data_ptrs, (char**)data_ptrs+pg_minsize, bitmap_size
            );
#endif
        }
    }
    calc_rmw_parity_copy_parity(stripes, pg_size, pg_minsize, read_osd_set, write_osd_set, chunk_size, start, end);
}
