// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "osd_primary.h"
#include "allocator.h"

void osd_t::continue_chained_read(osd_op_t *cur_op)
{
    osd_primary_op_data_t *op_data = cur_op->op_data;
    auto & pg = pgs.at({ .pool_id = INODE_POOL(op_data->oid.inode), .pg_num = op_data->pg_num });
    if (op_data->st == 1)
        goto resume_1;
    else if (op_data->st == 2)
        goto resume_2;
    else if (op_data->st == 3)
        goto resume_3;
    else if (op_data->st == 4)
        goto resume_4;
    cur_op->reply.rw.bitmap_len = 0;
    for (int role = 0; role < op_data->pg_data_size; role++)
    {
        op_data->stripes[role].read_start = op_data->stripes[role].req_start;
        op_data->stripes[role].read_end = op_data->stripes[role].req_end;
    }
resume_1:
resume_2:
    // Read bitmaps
    if (read_bitmaps(cur_op, pg, 1) != 0)
        return;
    // Prepare & submit reads
    if (submit_chained_read_requests(pg, cur_op) != 0)
        return;
    if (op_data->n_subops > 0)
    {
        // Wait for reads
        op_data->st = 3;
resume_3:
        return;
    }
resume_4:
    if (op_data->errors > 0)
    {
        if (op_data->errcode == -EIO || op_data->errcode == -EDOM)
        {
            // Handle corrupted reads and retry...
            check_corrupted_chained(pg, cur_op);
            free(cur_op->buf);
            cur_op->buf = NULL;
            free(op_data->chain_reads);
            op_data->chain_reads = NULL;
            // FIXME: We can in theory retry only specific parts instead of the whole operation
            goto resume_1;
        }
        else
        {
            free(op_data->chain_reads);
            op_data->chain_reads = NULL;
            finish_op(cur_op, op_data->errcode);
            return;
        }
    }
    send_chained_read_results(pg, cur_op);
    finish_op(cur_op, cur_op->req.rw.len);
}

int osd_t::read_bitmaps(osd_op_t *cur_op, pg_t & pg, int base_state)
{
    osd_primary_op_data_t *op_data = cur_op->op_data;
    if (op_data->st == base_state)
        goto resume_0;
    else if (op_data->st == base_state+1)
        goto resume_1;
    if (pg.state == PG_ACTIVE && pg.scheme == POOL_SCHEME_REPLICATED)
    {
        // Happy path for clean replicated PGs (all bitmaps are available locally)
        for (int chain_num = 0; chain_num < op_data->chain_size; chain_num++)
        {
            object_id cur_oid = { .inode = op_data->read_chain[chain_num], .stripe = op_data->oid.stripe };
            auto vo_it = pg.ver_override.find(cur_oid);
            auto read_version = (vo_it != pg.ver_override.end() ? vo_it->second : UINT64_MAX);
            // Read bitmap synchronously from the local database
            bs->read_bitmap(
                cur_oid, read_version, (uint8_t*)op_data->snapshot_bitmaps + chain_num*clean_entry_bitmap_size,
                !chain_num ? &cur_op->reply.rw.version : NULL
            );
        }
    }
    else
    {
        if (submit_bitmap_subops(cur_op, pg) < 0)
        {
            // Failure
            finish_op(cur_op, -EIO);
            return -1;
        }
resume_0:
        if (op_data->n_subops > 0)
        {
            // Wait for subops
            op_data->st = base_state;
            return 1;
        }
resume_1:
        if (pg.scheme != POOL_SCHEME_REPLICATED)
        {
            for (int chain_num = 0; chain_num < op_data->chain_size; chain_num++)
            {
                // Check if we need to reconstruct any bitmaps
                for (int i = 0; i < pg.pg_size; i++)
                {
                    if (op_data->missing_flags[chain_num*pg.pg_size + i])
                    {
                        osd_rmw_stripe_t local_stripes[pg.pg_size];
                        for (i = 0; i < pg.pg_size; i++)
                        {
                            local_stripes[i] = (osd_rmw_stripe_t){
                                .bmp_buf = (uint8_t*)op_data->snapshot_bitmaps + (chain_num*pg.pg_size + i)*clean_entry_bitmap_size,
                                .read_start = 1,
                                .read_end = 1,
                                .missing = op_data->missing_flags[chain_num*pg.pg_size + i] && true,
                            };
                        }
                        if (pg.scheme == POOL_SCHEME_XOR)
                        {
                            reconstruct_stripes_xor(local_stripes, pg.pg_size, clean_entry_bitmap_size);
                        }
                        else if (pg.scheme == POOL_SCHEME_EC)
                        {
                            reconstruct_stripes_ec(local_stripes, pg.pg_size, pg.pg_data_size, clean_entry_bitmap_size);
                        }
                        break;
                    }
                }
            }
        }
    }
    return 0;
}

int osd_t::collect_bitmap_requests(osd_op_t *cur_op, pg_t & pg, std::vector<bitmap_request_t> & bitmap_requests)
{
    osd_primary_op_data_t *op_data = cur_op->op_data;
    for (int chain_num = 0; chain_num < op_data->chain_size; chain_num++)
    {
        object_id cur_oid = { .inode = op_data->read_chain[chain_num], .stripe = op_data->oid.stripe };
        auto vo_it = pg.ver_override.find(cur_oid);
        uint64_t target_version = vo_it != pg.ver_override.end() ? vo_it->second : UINT64_MAX;
        uint64_t* cur_set = get_object_osd_set(pg, cur_oid, &op_data->chain_states[chain_num]);
        if (pg.scheme == POOL_SCHEME_REPLICATED)
        {
            osd_num_t read_target = 0;
            for (int i = 0; i < pg.pg_size; i++)
            {
                if (cur_set[i] == this->osd_num || cur_set[i] != 0 && read_target == 0)
                {
                    // Select local or any other available OSD for reading
                    read_target = cur_set[i];
                }
            }
            assert(read_target != 0);
            bitmap_requests.push_back((bitmap_request_t){
                .osd_num = read_target,
                .oid = cur_oid,
                .version = target_version,
                .bmp_buf = (uint8_t*)op_data->snapshot_bitmaps + chain_num*clean_entry_bitmap_size,
            });
        }
        else
        {
            osd_rmw_stripe_t local_stripes[pg.pg_size];
            memcpy(local_stripes, op_data->stripes, sizeof(osd_rmw_stripe_t) * pg.pg_size);
            if (extend_missing_stripes(local_stripes, cur_set, pg.pg_data_size, pg.pg_size) < 0)
            {
                free(op_data->snapshot_bitmaps);
                return -1;
            }
            int need_at_least = 0;
            for (int i = 0; i < pg.pg_size; i++)
            {
                if (local_stripes[i].read_end != 0 && cur_set[i] == 0)
                {
                    // We need this part of the bitmap, but it's unavailable
                    need_at_least = pg.pg_data_size;
                    op_data->missing_flags[chain_num*pg.pg_size + i] = 1;
                }
                else
                {
                    op_data->missing_flags[chain_num*pg.pg_size + i] = 0;
                }
            }
            int found = 0;
            for (int i = 0; i < pg.pg_size; i++)
            {
                if (cur_set[i] != 0 && (local_stripes[i].read_end != 0 || found < need_at_least))
                {
                    // Read part of the bitmap
                    bitmap_requests.push_back((bitmap_request_t){
                        .osd_num = cur_set[i],
                        .oid = {
                            .inode = cur_oid.inode,
                            .stripe = cur_oid.stripe | i,
                        },
                        .version = target_version,
                        .bmp_buf = (uint8_t*)op_data->snapshot_bitmaps + (chain_num*pg.pg_size + i)*clean_entry_bitmap_size,
                    });
                    found++;
                }
            }
            // Already checked by extend_missing_stripes, so it's fine to use assert
            assert(found >= need_at_least);
        }
    }
    std::sort(bitmap_requests.begin(), bitmap_requests.end());
    return 0;
}

int osd_t::submit_bitmap_subops(osd_op_t *cur_op, pg_t & pg)
{
    osd_primary_op_data_t *op_data = cur_op->op_data;
    std::vector<bitmap_request_t> *bitmap_requests = new std::vector<bitmap_request_t>();
    if (collect_bitmap_requests(cur_op, pg, *bitmap_requests) < 0)
    {
        delete bitmap_requests;
        return -1;
    }
    op_data->n_subops = 0;
    for (int i = 0; i < bitmap_requests->size(); i++)
    {
        if ((i == bitmap_requests->size()-1 || (*bitmap_requests)[i+1].osd_num != (*bitmap_requests)[i].osd_num) &&
            (*bitmap_requests)[i].osd_num != this->osd_num)
        {
            op_data->n_subops++;
        }
    }
    if (op_data->n_subops > 0)
    {
        op_data->fact_ver = 0;
        op_data->done = op_data->errors = 0;
        op_data->subops = new osd_op_t[op_data->n_subops];
    }
    for (int i = 0, subop_idx = 0, prev = 0; i < bitmap_requests->size(); i++)
    {
        if (i == bitmap_requests->size()-1 || (*bitmap_requests)[i+1].osd_num != (*bitmap_requests)[i].osd_num)
        {
            osd_num_t subop_osd_num = (*bitmap_requests)[i].osd_num;
            if (subop_osd_num == this->osd_num)
            {
                // Read bitmap synchronously from the local database
                for (int j = prev; j <= i; j++)
                {
                    bs->read_bitmap(
                        (*bitmap_requests)[j].oid, (*bitmap_requests)[j].version, (*bitmap_requests)[j].bmp_buf,
                        (*bitmap_requests)[j].oid.inode == cur_op->req.rw.inode ? &cur_op->reply.rw.version : NULL
                    );
                }
            }
            else
            {
                // Send to a remote OSD
                osd_op_t *subop = op_data->subops+subop_idx;
                subop->op_type = OSD_OP_OUT;
                // FIXME: Use the pre-allocated buffer
                assert(!subop->buf);
                subop->buf = malloc_or_die(sizeof(obj_ver_id)*(i+1-prev));
                subop->req = (osd_any_op_t){
                    .sec_read_bmp = {
                        .header = {
                            .magic = SECONDARY_OSD_OP_MAGIC,
                            .id = msgr.next_subop_id++,
                            .opcode = OSD_OP_SEC_READ_BMP,
                        },
                        .len = sizeof(obj_ver_id)*(i+1-prev),
                    }
                };
                obj_ver_id *ov = (obj_ver_id*)subop->buf;
                for (int j = prev; j <= i; j++, ov++)
                {
                    ov->oid = (*bitmap_requests)[j].oid;
                    ov->version = (*bitmap_requests)[j].version;
                }
                subop->callback = [cur_op, bitmap_requests, prev, i, this](osd_op_t *subop)
                {
                    int requested_count = subop->req.sec_read_bmp.len / sizeof(obj_ver_id);
                    if (subop->reply.hdr.retval == requested_count * (8 + clean_entry_bitmap_size))
                    {
                        void *cur_buf = (uint8_t*)subop->buf + 8;
                        for (int j = prev; j <= i; j++)
                        {
                            memcpy((*bitmap_requests)[j].bmp_buf, cur_buf, clean_entry_bitmap_size);
                            if ((*bitmap_requests)[j].oid.inode == cur_op->req.rw.inode)
                            {
                                memcpy(&cur_op->reply.rw.version, (uint8_t*)cur_buf-8, 8);
                            }
                            cur_buf = (uint8_t*)cur_buf + 8 + clean_entry_bitmap_size;
                        }
                    }
                    if ((cur_op->op_data->errors + cur_op->op_data->done + 1) >= cur_op->op_data->n_subops)
                    {
                        delete bitmap_requests;
                    }
                    handle_primary_subop(subop, cur_op);
                };
                auto peer_fd_it = msgr.osd_peer_fds.find(subop_osd_num);
                if (peer_fd_it != msgr.osd_peer_fds.end())
                {
                    subop->peer_fd = peer_fd_it->second;
                    msgr.outbox_push(subop);
                }
                else
                {
                    // Fail it immediately
                    subop->peer_fd = -1;
                    subop->reply.hdr.retval = -EPIPE;
                    ringloop->set_immediate([subop]() { std::function<void(osd_op_t*)>(subop->callback)(subop); });
                }
                subop_idx++;
            }
            prev = i+1;
        }
    }
    if (!op_data->n_subops)
    {
        delete bitmap_requests;
    }
    return 0;
}

std::vector<osd_chain_read_t> osd_t::collect_chained_read_requests(osd_op_t *cur_op)
{
    osd_primary_op_data_t *op_data = cur_op->op_data;
    std::vector<osd_chain_read_t> chain_reads;
    int stripe_count = (op_data->scheme == POOL_SCHEME_REPLICATED ? 1 : op_data->pg_size);
    memset(op_data->stripes[0].bmp_buf, 0, stripe_count * clean_entry_bitmap_size);
    uint8_t *global_bitmap = (uint8_t*)op_data->stripes[0].bmp_buf;
    // We always use at most 1 read request per layer
    for (int chain_pos = 0; chain_pos < op_data->chain_size; chain_pos++)
    {
        uint8_t *part_bitmap = ((uint8_t*)op_data->snapshot_bitmaps) + chain_pos*stripe_count*clean_entry_bitmap_size;
        int start = !cur_op->req.rw.len ? 0 : (cur_op->req.rw.offset - op_data->oid.stripe)/bs_bitmap_granularity;
        int end = !cur_op->req.rw.len ? op_data->pg_data_size*clean_entry_bitmap_size*8 : start + cur_op->req.rw.len/bs_bitmap_granularity;
        // Skip unneeded part in the beginning
        while (start < end && (
            ((global_bitmap[start>>3] >> (start&7)) & 1) ||
            !((part_bitmap[start>>3] >> (start&7)) & 1)))
        {
            start++;
        }
        // Skip unneeded part in the end
        while (start < end && (
            ((global_bitmap[(end-1)>>3] >> ((end-1)&7)) & 1) ||
            !((part_bitmap[(end-1)>>3] >> ((end-1)&7)) & 1)))
        {
            end--;
        }
        if (start < end)
        {
            // Copy (OR) bits in between
            int cur = start;
            for (; cur < end && (cur & 0x7); cur++)
            {
                global_bitmap[cur>>3] = global_bitmap[cur>>3] | (part_bitmap[cur>>3] & (1 << (cur&7)));
            }
            for (; cur <= end-8; cur += 8)
            {
                global_bitmap[cur>>3] = global_bitmap[cur>>3] | part_bitmap[cur>>3];
            }
            for (; cur < end; cur++)
            {
                global_bitmap[cur>>3] = global_bitmap[cur>>3] | (part_bitmap[cur>>3] & (1 << (cur&7)));
            }
            // Add request
            chain_reads.push_back((osd_chain_read_t){
                .chain_pos = chain_pos,
                .inode = op_data->read_chain[chain_pos],
                .offset = start*bs_bitmap_granularity,
                .len = (end-start)*bs_bitmap_granularity,
            });
        }
    }
    return chain_reads;
}

int osd_t::submit_chained_read_requests(pg_t & pg, osd_op_t *cur_op)
{
    // Decide which parts of which objects we need to read based on bitmaps
    osd_primary_op_data_t *op_data = cur_op->op_data;
    auto chain_reads = collect_chained_read_requests(cur_op);
    int stripe_count = (pg.scheme == POOL_SCHEME_REPLICATED ? 1 : pg.pg_size);
    op_data->chain_read_count = chain_reads.size();
    op_data->chain_reads = (osd_chain_read_t*)calloc_or_die(
        1, sizeof(osd_chain_read_t) * chain_reads.size()
        // FIXME: Allocate only <chain_reads.size()> instead of <chain_size> stripes
        // (but it's slightly harder to handle in send_chained_read_results())
        + sizeof(osd_rmw_stripe_t) * stripe_count * op_data->chain_size
    );
    osd_rmw_stripe_t *chain_stripes = (osd_rmw_stripe_t*)(
        (uint8_t*)op_data->chain_reads + sizeof(osd_chain_read_t) * op_data->chain_read_count
    );
    // Now process each subrequest as a separate read, including reconstruction if needed
    // Prepare reads
    int n_subops = 0;
    uint64_t read_buffer_size = 0;
    for (int cri = 0; cri < chain_reads.size(); cri++)
    {
        op_data->chain_reads[cri] = chain_reads[cri];
        object_id cur_oid = { .inode = chain_reads[cri].inode, .stripe = op_data->oid.stripe };
        // FIXME: maybe introduce split_read_stripes to shorten these lines and to remove read_start=req_start
        osd_rmw_stripe_t *stripes = chain_stripes + chain_reads[cri].chain_pos*stripe_count;
        split_stripes(pg.pg_data_size, bs_block_size, chain_reads[cri].offset, chain_reads[cri].len, stripes);
        if (op_data->scheme == POOL_SCHEME_REPLICATED && !stripes[0].req_end)
        {
            continue;
        }
        for (int role = 0; role < op_data->pg_data_size; role++)
        {
            stripes[role].read_start = stripes[role].req_start;
            stripes[role].read_end = stripes[role].req_end;
        }
        uint64_t *cur_set = pg.cur_set.data();
        if (pg.state != PG_ACTIVE)
        {
            cur_set = get_object_osd_set(pg, cur_oid, &op_data->chain_states[chain_reads[cri].chain_pos]);
            if (op_data->scheme != POOL_SCHEME_REPLICATED)
            {
                if (extend_missing_stripes(stripes, cur_set, pg.pg_data_size, pg.pg_size) < 0)
                {
                    free(op_data->chain_reads);
                    op_data->chain_reads = NULL;
                    finish_op(cur_op, -EIO);
                    return -1;
                }
                op_data->degraded = 1;
            }
            else
            {
                auto cur_state = op_data->chain_states[chain_reads[cri].chain_pos];
                if (cur_state && (cur_state->state & OBJ_INCOMPLETE))
                {
                    free(op_data->chain_reads);
                    op_data->chain_reads = NULL;
                    finish_op(cur_op, -EIO);
                    return -1;
                }
            }
        }
        if (op_data->scheme == POOL_SCHEME_REPLICATED)
        {
            n_subops++;
            read_buffer_size += stripes[0].read_end - stripes[0].read_start;
        }
        else
        {
            for (int role = 0; role < pg.pg_size; role++)
            {
                if (stripes[role].read_end > 0 && cur_set[role] != 0)
                    n_subops++;
                if (stripes[role].read_end > 0)
                    read_buffer_size += stripes[role].read_end - stripes[role].read_start;
            }
        }
    }
    assert(!cur_op->buf);
    cur_op->buf = memalign_or_die(MEM_ALIGNMENT, read_buffer_size);
    void *cur_buf = cur_op->buf;
    for (int cri = 0; cri < chain_reads.size(); cri++)
    {
        osd_rmw_stripe_t *stripes = chain_stripes + chain_reads[cri].chain_pos*stripe_count;
        for (int role = 0; role < stripe_count; role++)
        {
            if (stripes[role].read_end > 0)
            {
                stripes[role].read_buf = cur_buf;
                stripes[role].bmp_buf = (uint8_t*)op_data->snapshot_bitmaps + (chain_reads[cri].chain_pos*stripe_count + role)*clean_entry_bitmap_size;
                cur_buf = (uint8_t*)cur_buf + stripes[role].read_end - stripes[role].read_start;
            }
        }
    }
    // Submit all reads
    op_data->fact_ver = UINT64_MAX;
    op_data->done = op_data->errors = 0;
    op_data->n_subops = n_subops;
    if (!n_subops)
    {
        return 0;
    }
    op_data->subops = new osd_op_t[n_subops];
    int cur_subops = 0;
    for (int cri = 0; cri < chain_reads.size(); cri++)
    {
        osd_rmw_stripe_t *stripes = chain_stripes + chain_reads[cri].chain_pos*stripe_count;
        if (op_data->scheme == POOL_SCHEME_REPLICATED && !stripes[0].req_end)
        {
            continue;
        }
        object_id cur_oid = { .inode = chain_reads[cri].inode, .stripe = op_data->oid.stripe };
        auto vo_it = pg.ver_override.find(cur_oid);
        uint64_t target_ver = vo_it != pg.ver_override.end() ? vo_it->second : UINT64_MAX;
        auto cur_state = op_data->chain_states[chain_reads[cri].chain_pos];
        uint64_t *cur_set = (pg.state != PG_ACTIVE && cur_state ? cur_state->read_target.data() : pg.cur_set.data());
        int zero_read = -1;
        if (op_data->scheme == POOL_SCHEME_REPLICATED)
        {
            for (int role = 0; role < op_data->pg_size; role++)
                if (cur_set[role] == this->osd_num || zero_read == -1)
                    zero_read = role;
        }
        cur_subops += submit_primary_subop_batch(SUBMIT_READ, chain_reads[cri].inode, target_ver, stripes, cur_set, cur_op, cur_subops, zero_read);
    }
    assert(cur_subops == n_subops);
    return 0;
}

void osd_t::check_corrupted_chained(pg_t & pg, osd_op_t *cur_op)
{
    osd_primary_op_data_t *op_data = cur_op->op_data;
    int stripe_count = (pg.scheme == POOL_SCHEME_REPLICATED ? 1 : pg.pg_size);
    osd_rmw_stripe_t *chain_stripes = (osd_rmw_stripe_t*)(
        (uint8_t*)op_data->chain_reads + sizeof(osd_chain_read_t) * op_data->chain_read_count
    );
    for (int cri = 0; cri < op_data->chain_read_count; cri++)
    {
        object_id cur_oid = { .inode = op_data->chain_reads[cri].inode, .stripe = op_data->oid.stripe };
        osd_rmw_stripe_t *stripes = chain_stripes + op_data->chain_reads[cri].chain_pos*stripe_count;
        bool corrupted = false;
        for (int i = 0; i < stripe_count; i++)
        {
            if (stripes[i].read_error)
            {
                corrupted = true;
                break;
            }
        }
        if (corrupted)
        {
            mark_object_corrupted(pg, cur_oid, op_data->chain_states[op_data->chain_reads[cri].chain_pos], stripes, false, false);
        }
    }
}

void osd_t::send_chained_read_results(pg_t & pg, osd_op_t *cur_op)
{
    osd_primary_op_data_t *op_data = cur_op->op_data;
    int stripe_count = (pg.scheme == POOL_SCHEME_REPLICATED ? 1 : pg.pg_size);
    osd_rmw_stripe_t *chain_stripes = (osd_rmw_stripe_t*)(
        (uint8_t*)op_data->chain_reads + sizeof(osd_chain_read_t) * op_data->chain_read_count
    );
    // Reconstruct parts if needed
    if (op_data->degraded)
    {
        int stripe_count = (pg.scheme == POOL_SCHEME_REPLICATED ? 1 : pg.pg_size);
        for (int cri = 0; cri < op_data->chain_read_count; cri++)
        {
            // Reconstruct missing stripes
            osd_rmw_stripe_t *stripes = chain_stripes + op_data->chain_reads[cri].chain_pos*stripe_count;
            if (op_data->scheme == POOL_SCHEME_XOR)
            {
                reconstruct_stripes_xor(stripes, pg.pg_size, clean_entry_bitmap_size);
            }
            else if (op_data->scheme == POOL_SCHEME_EC)
            {
                reconstruct_stripes_ec(stripes, pg.pg_size, pg.pg_data_size, clean_entry_bitmap_size);
            }
        }
    }
    // Send bitmap
    cur_op->reply.rw.bitmap_len = op_data->pg_data_size * clean_entry_bitmap_size;
    cur_op->iov.push_back(op_data->stripes[0].bmp_buf, cur_op->reply.rw.bitmap_len);
    // And finally compose the result
    uint64_t sent = 0;
    int prev_pos = 0, pos = 0;
    bool prev_set = false;
    int prev = (cur_op->req.rw.offset - op_data->oid.stripe) / bs_bitmap_granularity;
    int end = prev + cur_op->req.rw.len/bs_bitmap_granularity;
    int cur = prev;
    while (cur <= end)
    {
        bool has_bit = false;
        if (cur < end)
        {
            for (pos = 0; pos < op_data->chain_size; pos++)
            {
                has_bit = (((uint8_t*)op_data->snapshot_bitmaps)[pos*stripe_count*clean_entry_bitmap_size + cur/8] >> (cur%8)) & 1;
                if (has_bit)
                    break;
            }
        }
        if (has_bit != prev_set || pos != prev_pos || cur == end)
        {
            if (cur > prev)
            {
                // Send buffer in parts to avoid copying
                if (!prev_set)
                {
                    while ((cur-prev) > zero_buffer_size/bs_bitmap_granularity)
                    {
                        cur_op->iov.push_back(zero_buffer, zero_buffer_size);
                        sent += zero_buffer_size;
                        prev += zero_buffer_size/bs_bitmap_granularity;
                    }
                    cur_op->iov.push_back(zero_buffer, (cur-prev)*bs_bitmap_granularity);
                    sent += (cur-prev)*bs_bitmap_granularity;
                }
                else
                {
                    osd_rmw_stripe_t *stripes = chain_stripes + prev_pos*stripe_count;
                    while (cur > prev)
                    {
                        int role = prev*bs_bitmap_granularity/bs_block_size;
                        int role_start = prev*bs_bitmap_granularity - role*bs_block_size;
                        int role_end = cur*bs_bitmap_granularity - role*bs_block_size;
                        if (role_end > bs_block_size)
                            role_end = bs_block_size;
                        assert(stripes[role].read_buf);
                        cur_op->iov.push_back(
                            (uint8_t*)stripes[role].read_buf + (role_start - stripes[role].read_start),
                            role_end - role_start
                        );
                        sent += role_end - role_start;
                        prev += (role_end - role_start)/bs_bitmap_granularity;
                    }
                }
            }
            prev = cur;
            prev_pos = pos;
            prev_set = has_bit;
        }
        cur++;
    }
    assert(sent == cur_op->req.rw.len);
    free(op_data->chain_reads);
    op_data->chain_reads = NULL;
}
