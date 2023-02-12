// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "osd_primary.h"

void osd_t::continue_primary_scrub(osd_op_t *cur_op)
{
    if (!cur_op->op_data && !prepare_primary_rw(cur_op))
        return;
    osd_primary_op_data_t *op_data = cur_op->op_data;
    if (op_data->st == 1)
        goto resume_1;
    else if (op_data->st == 2)
        goto resume_2;
    {
        auto & pg = pgs.at({ .pool_id = INODE_POOL(op_data->oid.inode), .pg_num = op_data->pg_num });
        // Determine version
        auto vo_it = pg.ver_override.find(op_data->oid);
        op_data->target_ver = vo_it != pg.ver_override.end() ? vo_it->second : UINT64_MAX;
        // PG may have degraded or misplaced objects
        op_data->prev_set = get_object_osd_set(pg, op_data->oid, &op_data->object_state);
        // Read all available chunks
        int n_copies = 0;
        op_data->degraded = false;
        for (int role = 0; role < op_data->pg_size; role++)
        {
            op_data->stripes[role].read_start = 0;
            op_data->stripes[role].read_end = bs_block_size;
            if (op_data->prev_set[role] != 0)
            {
                n_copies++;
            }
            else if (op_data->scheme != POOL_SCHEME_REPLICATED && role < op_data->pg_data_size)
            {
                op_data->degraded = true;
            }
        }
        if (n_copies <= op_data->pg_data_size)
        {
            // Nothing to compare, even if we'd like to
            finish_op(cur_op, 0);
            return;
        }
        cur_op->buf = alloc_read_buffer(op_data->stripes, op_data->pg_size,
            op_data->scheme != POOL_SCHEME_REPLICATED ? bs_block_size*(op_data->pg_size-op_data->pg_data_size) : 0);
        // Submit reads
        osd_op_t *subops = new osd_op_t[n_copies];
        op_data->fact_ver = 0;
        op_data->done = op_data->errors = op_data->errcode = 0;
        op_data->n_subops = n_copies;
        op_data->subops = subops;
        int sent = submit_primary_subop_batch(SUBMIT_SCRUB_READ, op_data->oid.inode, op_data->target_ver,
            op_data->stripes, op_data->prev_set, cur_op, 0, -1);
        assert(sent == n_copies);
        op_data->st = 1;
    }
resume_1:
    return;
resume_2:
    if (op_data->errors > 0)
    {
        if (op_data->errcode == -EIO || op_data->errcode == -EDOM)
        {
            // I/O or checksum error
            int n_copies = 0;
            for (int role = 0; role < op_data->pg_size; role++)
            {
                if (op_data->stripes[role].read_end != 0 &&
                    !op_data->stripes[role].read_error)
                {
                    n_copies++;
                }
            }
            if (n_copies <= op_data->pg_data_size)
            {
                // Nothing to compare, just mark the object as corrupted
                auto & pg = pgs.at({ .pool_id = INODE_POOL(op_data->oid.inode), .pg_num = op_data->pg_num });
                // FIXME: ref = true ideally... because new_state != state is not necessarily true if it's freed and recreated
                op_data->object_state = mark_object_corrupted(pg, op_data->oid, op_data->object_state, op_data->stripes, false);
                // Operation is treated as unsuccessful only if the object becomes unreadable
                finish_op(cur_op, n_copies < op_data->pg_data_size ? op_data->errcode : 0);
                return;
            }
            // Proceed, we can still compare chunks that were successfully read
        }
        else
        {
            finish_op(cur_op, op_data->errcode);
            return;
        }
    }
    if (op_data->scheme == POOL_SCHEME_REPLICATED)
    {
        // Check that all chunks have returned the same data
        int total = 0;
        int eq_to[op_data->pg_size];
        for (int role = 0; role < op_data->pg_size; role++)
        {
            eq_to[role] = -1;
            if (op_data->stripes[role].read_end != 0 && !op_data->stripes[role].read_error)
            {
                total++;
                eq_to[role] = role;
                for (int other = 0; other < role; other++)
                {
                    // Only compare with unique chunks (eq_to[other] == other)
                    if (eq_to[other] == other && memcmp(op_data->stripes[role].read_buf, op_data->stripes[other].read_buf, bs_block_size) == 0)
                    {
                        eq_to[role] = eq_to[other];
                        break;
                    }
                }
            }
        }
        int votes[op_data->pg_size];
        for (int role = 0; role < op_data->pg_size; role++)
            votes[role] = 0;
        for (int role = 0; role < op_data->pg_size; role++)
        {
            if (eq_to[role] != -1)
                votes[eq_to[role]]++;
        }
        int best = -1;
        for (int role = 0; role < op_data->pg_size; role++)
        {
            if (best < 0 && votes[role] > 0 || votes[role] > votes[best])
                best = role;
        }
        if (best > 0 && votes[best] < total)
        {
            // FIXME Add a flag to allow to skip such objects and not recover them automatically
            bool unknown = false;
            for (int role = 0; role < op_data->pg_size; role++)
            {
                if (role != best && votes[role] == votes[best])
                    unknown = true;
                if (votes[role] > 0 && votes[role] < votes[best])
                {
                    printf(
                        "[PG %u/%u] Object %lx:%lx copy on OSD %lu doesn't match %d other copies, marking it as corrupted\n",
                        INODE_POOL(op_data->oid.inode), op_data->pg_num,
                        op_data->oid.inode, op_data->oid.stripe, op_data->stripes[role].osd_num, votes[best]
                    );
                    op_data->stripes[role].read_error = true;
                }
            }
            if (unknown)
            {
                // It's unknown which replica is good. There are multiple versions with no majority
                best = -1;
            }
        }
    }
    else
    {
        assert(op_data->scheme == POOL_SCHEME_EC || op_data->scheme == POOL_SCHEME_XOR);
        if (op_data->degraded)
        {
            // Reconstruct missing stripes
            // XOR shouldn't come here as it only has 1 parity chunk
            assert(op_data->scheme == POOL_SCHEME_EC);
            reconstruct_stripes_ec(op_data->stripes, op_data->pg_size, op_data->pg_data_size, clean_entry_bitmap_size);
        }
        // Generate parity chunks and compare them with actual data
        osd_num_t fake_osd_set[op_data->pg_size];
        for (int i = 0; i < op_data->pg_size; i++)
        {
            fake_osd_set[i] = 1;
            op_data->stripes[i].write_buf = i >= op_data->pg_data_size
                ? ((uint8_t*)cur_op->buf + (i-op_data->pg_data_size)*bs_block_size)
                : op_data->stripes[i].read_buf;
        }
        if (op_data->scheme == POOL_SCHEME_XOR)
        {
            calc_rmw_parity_xor(op_data->stripes, op_data->pg_size, fake_osd_set, fake_osd_set, bs_block_size, clean_entry_bitmap_size);
        }
        else if (op_data->scheme == POOL_SCHEME_EC)
        {
            calc_rmw_parity_ec(op_data->stripes, op_data->pg_size, op_data->pg_data_size, fake_osd_set, fake_osd_set, bs_block_size, clean_entry_bitmap_size);
        }
        // Now compare that write_buf == read_buf
        for (int role = op_data->pg_data_size; role < op_data->pg_size; role++)
        {
            if (op_data->stripes[role].osd_num != 0 && !op_data->stripes[role].read_error &&
                memcmp(op_data->stripes[role].read_buf, op_data->stripes[role].write_buf, bs_block_size) != 0)
            {
                // Chunks don't match - something's wrong... but we don't know what :D
                // FIXME: Try to locate errors (may be possible with >= 2 parity chunks)
                printf(
                    "[PG %u/%u] Object %lx:%lx parity chunk %d on OSD %lu doesn't match data, marking it as corrupted\n",
                    INODE_POOL(op_data->oid.inode), op_data->pg_num,
                    op_data->oid.inode, op_data->oid.stripe,
                    role-op_data->pg_data_size, op_data->stripes[role].osd_num
                );
                op_data->stripes[role].read_error = true;
            }
        }
    }
    for (int role = 0; role < op_data->pg_size; role++)
    {
        if (op_data->stripes[role].osd_num != 0 && !op_data->stripes[role].read_error)
        {
            // Got at least 1 read error or mismatch, mark the object as corrupted
            auto & pg = pgs.at({ .pool_id = INODE_POOL(op_data->oid.inode), .pg_num = op_data->pg_num });
            // FIXME: ref = true ideally... because new_state != state is not necessarily true if it's freed and recreated
            op_data->object_state = mark_object_corrupted(pg, op_data->oid, op_data->object_state, op_data->stripes, false);
            break;
        }
    }
    finish_op(cur_op, 0);
}
