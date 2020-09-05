#include "osd_primary.h"

void osd_t::autosync()
{
    // FIXME Autosync based on the number of unstable writes to prevent
    // "journal_sector_buffer_count is too low for this batch" errors
    if (immediate_commit != IMMEDIATE_ALL && !autosync_op)
    {
        autosync_op = new osd_op_t();
        autosync_op->op_type = OSD_OP_IN;
        autosync_op->req = {
            .sync = {
                .header = {
                    .magic = SECONDARY_OSD_OP_MAGIC,
                    .id = 1,
                    .opcode = OSD_OP_SYNC,
                },
            },
        };
        autosync_op->callback = [this](osd_op_t *op)
        {
            if (op->reply.hdr.retval < 0)
            {
                printf("Warning: automatic sync resulted in an error: %ld (%s)\n", -op->reply.hdr.retval, strerror(-op->reply.hdr.retval));
            }
            delete autosync_op;
            autosync_op = NULL;
        };
        exec_op(autosync_op);
    }
}

void osd_t::finish_op(osd_op_t *cur_op, int retval)
{
    inflight_ops--;
    if (cur_op->op_data)
    {
        if (cur_op->op_data->pg_num > 0)
        {
            auto & pg = pgs[{ .pool_id = INODE_POOL(cur_op->op_data->oid.inode), .pg_num = cur_op->op_data->pg_num }];
            pg.inflight--;
            assert(pg.inflight >= 0);
            if ((pg.state & PG_STOPPING) && pg.inflight == 0 && !pg.flush_batch)
            {
                finish_stop_pg(pg);
            }
        }
        assert(!cur_op->op_data->subops);
        assert(!cur_op->op_data->unstable_write_osds);
        assert(!cur_op->op_data->unstable_writes);
        assert(!cur_op->op_data->dirty_pgs);
        free(cur_op->op_data);
        cur_op->op_data = NULL;
    }
    if (!cur_op->peer_fd)
    {
        // Copy lambda to be unaffected by `delete op`
        std::function<void(osd_op_t*)>(cur_op->callback)(cur_op);
    }
    else
    {
        // FIXME add separate magic number
        auto cl_it = c_cli.clients.find(cur_op->peer_fd);
        if (cl_it != c_cli.clients.end())
        {
            cur_op->reply.hdr.magic = SECONDARY_OSD_REPLY_MAGIC;
            cur_op->reply.hdr.id = cur_op->req.hdr.id;
            cur_op->reply.hdr.opcode = cur_op->req.hdr.opcode;
            cur_op->reply.hdr.retval = retval;
            c_cli.outbox_push(cur_op);
        }
        else
        {
            delete cur_op;
        }
    }
}

void osd_t::submit_primary_subops(int submit_type, uint64_t op_version, int pg_size, const uint64_t* osd_set, osd_op_t *cur_op)
{
    bool wr = submit_type == SUBMIT_WRITE;
    osd_primary_op_data_t *op_data = cur_op->op_data;
    osd_rmw_stripe_t *stripes = op_data->stripes;
    bool rep = op_data->scheme == POOL_SCHEME_REPLICATED;
    // Allocate subops
    int n_subops = 0, zero_read = -1;
    for (int role = 0; role < pg_size; role++)
    {
        if (osd_set[role] == this->osd_num || osd_set[role] != 0 && zero_read == -1)
        {
            zero_read = role;
        }
        if (osd_set[role] != 0 && (wr || !rep && stripes[role].read_end != 0))
        {
            n_subops++;
        }
    }
    if (!n_subops && (submit_type == SUBMIT_RMW_READ || rep))
    {
        n_subops = 1;
    }
    else
    {
        zero_read = -1;
    }
    osd_op_t *subops = new osd_op_t[n_subops];
    op_data->fact_ver = 0;
    op_data->done = op_data->errors = 0;
    op_data->n_subops = n_subops;
    op_data->subops = subops;
    int i = 0;
    for (int role = 0; role < pg_size; role++)
    {
        // We always submit zero-length writes to all replicas, even if the stripe is not modified
        if (!(wr || !rep && stripes[role].read_end != 0 || zero_read == role))
        {
            continue;
        }
        osd_num_t role_osd_num = osd_set[role];
        if (role_osd_num != 0)
        {
            int stripe_num = rep ? 0 : role;
            if (role_osd_num == this->osd_num)
            {
                clock_gettime(CLOCK_REALTIME, &subops[i].tv_begin);
                subops[i].op_type = (uint64_t)cur_op;
                subops[i].bs_op = new blockstore_op_t({
                    .opcode = (uint64_t)(wr ? (rep ? BS_OP_WRITE_STABLE : BS_OP_WRITE) : BS_OP_READ),
                    .callback = [subop = &subops[i], this](blockstore_op_t *bs_subop)
                    {
                        handle_primary_bs_subop(subop);
                    },
                    .oid = {
                        .inode = op_data->oid.inode,
                        .stripe = op_data->oid.stripe | stripe_num,
                    },
                    .version = op_version,
                    .offset = wr ? stripes[stripe_num].write_start : stripes[stripe_num].read_start,
                    .len = wr ? stripes[stripe_num].write_end - stripes[stripe_num].write_start : stripes[stripe_num].read_end - stripes[stripe_num].read_start,
                    .buf = wr ? stripes[stripe_num].write_buf : stripes[stripe_num].read_buf,
                });
#ifdef OSD_DEBUG
                printf(
                    "Submit %s to local: %lx:%lx v%lu %u-%u\n", wr ? "write" : "read",
                    op_data->oid.inode, op_data->oid.stripe | stripe_num, op_version,
                    subops[i].bs_op->offset, subops[i].bs_op->len
                );
#endif
                bs->enqueue_op(subops[i].bs_op);
            }
            else
            {
                subops[i].op_type = OSD_OP_OUT;
                subops[i].peer_fd = c_cli.osd_peer_fds.at(role_osd_num);
                subops[i].req.sec_rw = {
                    .header = {
                        .magic = SECONDARY_OSD_OP_MAGIC,
                        .id = c_cli.next_subop_id++,
                        .opcode = (uint64_t)(wr ? (rep ? OSD_OP_SEC_WRITE_STABLE : OSD_OP_SEC_WRITE) : OSD_OP_SEC_READ),
                    },
                    .oid = {
                        .inode = op_data->oid.inode,
                        .stripe = op_data->oid.stripe | stripe_num,
                    },
                    .version = op_version,
                    .offset = wr ? stripes[stripe_num].write_start : stripes[stripe_num].read_start,
                    .len = wr ? stripes[stripe_num].write_end - stripes[stripe_num].write_start : stripes[stripe_num].read_end - stripes[stripe_num].read_start,
                };
#ifdef OSD_DEBUG
                printf(
                    "Submit %s to osd %lu: %lx:%lx v%lu %u-%u\n", wr ? "write" : "read", role_osd_num,
                    op_data->oid.inode, op_data->oid.stripe | stripe_num, op_version,
                    subops[i].req.sec_rw.offset, subops[i].req.sec_rw.len
                );
#endif
                if (wr)
                {
                    if (stripes[stripe_num].write_end > stripes[stripe_num].write_start)
                    {
                        subops[i].iov.push_back(stripes[stripe_num].write_buf, stripes[stripe_num].write_end - stripes[stripe_num].write_start);
                    }
                }
                else
                {
                    if (stripes[stripe_num].read_end > stripes[stripe_num].read_start)
                    {
                        subops[i].iov.push_back(stripes[stripe_num].read_buf, stripes[stripe_num].read_end - stripes[stripe_num].read_start);
                    }
                }
                subops[i].callback = [cur_op, this](osd_op_t *subop)
                {
                    int fail_fd = subop->req.hdr.opcode == OSD_OP_SEC_WRITE &&
                        subop->reply.hdr.retval != subop->req.sec_rw.len ? subop->peer_fd : -1;
                    handle_primary_subop(subop, cur_op);
                    if (fail_fd >= 0)
                    {
                        // write operation failed, drop the connection
                        c_cli.stop_client(fail_fd);
                    }
                };
                c_cli.outbox_push(&subops[i]);
            }
            i++;
        }
    }
}

static uint64_t bs_op_to_osd_op[] = {
    0,
    OSD_OP_SEC_READ,            // BS_OP_READ = 1
    OSD_OP_SEC_WRITE,           // BS_OP_WRITE = 2
    OSD_OP_SEC_WRITE_STABLE,    // BS_OP_WRITE_STABLE = 3
    OSD_OP_SEC_SYNC,            // BS_OP_SYNC = 4
    OSD_OP_SEC_STABILIZE,       // BS_OP_STABLE = 5
    OSD_OP_SEC_DELETE,          // BS_OP_DELETE = 6
    OSD_OP_SEC_LIST,            // BS_OP_LIST = 7
    OSD_OP_SEC_ROLLBACK,        // BS_OP_ROLLBACK = 8
    OSD_OP_TEST_SYNC_STAB_ALL,  // BS_OP_SYNC_STAB_ALL = 9
};

void osd_t::handle_primary_bs_subop(osd_op_t *subop)
{
    osd_op_t *cur_op = (osd_op_t*)subop->op_type;
    blockstore_op_t *bs_op = subop->bs_op;
    int expected = bs_op->opcode == BS_OP_READ || bs_op->opcode == BS_OP_WRITE
        || bs_op->opcode == BS_OP_WRITE_STABLE ? bs_op->len : 0;
    if (bs_op->retval != expected && bs_op->opcode != BS_OP_READ)
    {
        // die
        throw std::runtime_error(
            "local blockstore modification failed (opcode = "+std::to_string(bs_op->opcode)+
            " retval = "+std::to_string(bs_op->retval)+")"
        );
    }
    add_bs_subop_stats(subop);
    subop->req.hdr.opcode = bs_op_to_osd_op[bs_op->opcode];
    subop->reply.hdr.retval = bs_op->retval;
    if (bs_op->opcode == BS_OP_READ || bs_op->opcode == BS_OP_WRITE || bs_op->opcode == BS_OP_WRITE_STABLE)
    {
        subop->req.sec_rw.len = bs_op->len;
        subop->reply.sec_rw.version = bs_op->version;
    }
    delete bs_op;
    subop->bs_op = NULL;
    handle_primary_subop(subop, cur_op);
}

void osd_t::add_bs_subop_stats(osd_op_t *subop)
{
    // Include local blockstore ops in statistics
    uint64_t opcode = bs_op_to_osd_op[subop->bs_op->opcode];
    timespec tv_end;
    clock_gettime(CLOCK_REALTIME, &tv_end);
    c_cli.stats.op_stat_count[opcode]++;
    if (!c_cli.stats.op_stat_count[opcode])
    {
        c_cli.stats.op_stat_count[opcode] = 1;
        c_cli.stats.op_stat_sum[opcode] = 0;
        c_cli.stats.op_stat_bytes[opcode] = 0;
    }
    c_cli.stats.op_stat_sum[opcode] += (
        (tv_end.tv_sec - subop->tv_begin.tv_sec)*1000000 +
        (tv_end.tv_nsec - subop->tv_begin.tv_nsec)/1000
    );
    if (opcode == OSD_OP_SEC_READ || opcode == OSD_OP_SEC_WRITE)
    {
        c_cli.stats.op_stat_bytes[opcode] += subop->bs_op->len;
    }
}

void osd_t::handle_primary_subop(osd_op_t *subop, osd_op_t *cur_op)
{
    uint64_t opcode = subop->req.hdr.opcode;
    int retval = subop->reply.hdr.retval;
    int expected = opcode == OSD_OP_SEC_READ || opcode == OSD_OP_SEC_WRITE
        || opcode == OSD_OP_SEC_WRITE_STABLE ? subop->req.sec_rw.len : 0;
    osd_primary_op_data_t *op_data = cur_op->op_data;
    if (retval != expected)
    {
        printf("%s subop failed: retval = %d (expected %d)\n", osd_op_names[opcode], retval, expected);
        if (retval == -EPIPE)
        {
            op_data->epipe++;
        }
        op_data->errors++;
    }
    else
    {
        op_data->done++;
        if (opcode == OSD_OP_SEC_READ || opcode == OSD_OP_SEC_WRITE || opcode == OSD_OP_SEC_WRITE_STABLE)
        {
            uint64_t version = subop->reply.sec_rw.version;
#ifdef OSD_DEBUG
            uint64_t peer_osd = c_cli.clients.find(subop->peer_fd) != c_cli.clients.end()
                ? c_cli.clients[subop->peer_fd].osd_num : osd_num;
            printf("subop %lu from osd %lu: version = %lu\n", opcode, peer_osd, version);
#endif
            if (op_data->fact_ver != 0 && op_data->fact_ver != version)
            {
                throw std::runtime_error(
                    "different fact_versions returned from "+std::string(osd_op_names[opcode])+
                    " subops: "+std::to_string(version)+" vs "+std::to_string(op_data->fact_ver)
                );
            }
            op_data->fact_ver = version;
        }
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
        else if (cur_op->req.hdr.opcode == OSD_OP_WRITE)
        {
            continue_primary_write(cur_op);
        }
        else if (cur_op->req.hdr.opcode == OSD_OP_SYNC)
        {
            continue_primary_sync(cur_op);
        }
        else if (cur_op->req.hdr.opcode == OSD_OP_DELETE)
        {
            continue_primary_del(cur_op);
        }
        else
        {
            throw std::runtime_error("BUG: unknown opcode");
        }
    }
}

void osd_t::cancel_primary_write(osd_op_t *cur_op)
{
    if (cur_op->op_data && cur_op->op_data->subops)
    {
        // Primary-write operation is waiting for subops, subops
        // are sent to peer OSDs, so we can't just throw them away.
        // Mark them with an extra EPIPE.
        cur_op->op_data->errors++;
        cur_op->op_data->epipe++;
        cur_op->op_data->done--; // Caution: `done` must be signed because may become -1 here
    }
    else
    {
        finish_op(cur_op, -EPIPE);
    }
}

static bool contains_osd(osd_num_t *osd_set, uint64_t size, osd_num_t osd_num)
{
    for (uint64_t i = 0; i < size; i++)
    {
        if (osd_set[i] == osd_num)
        {
            return true;
        }
    }
    return false;
}

void osd_t::submit_primary_del_subops(osd_op_t *cur_op, osd_num_t *cur_set, uint64_t set_size, pg_osd_set_t & loc_set)
{
    osd_primary_op_data_t *op_data = cur_op->op_data;
    bool rep = op_data->scheme == POOL_SCHEME_REPLICATED;
    int extra_chunks = 0;
    // ordered comparison for EC/XOR, unordered for replicated pools
    for (auto & chunk: loc_set)
    {
        if (!cur_set || (rep ? !contains_osd(cur_set, set_size, chunk.osd_num) : chunk.osd_num != cur_set[chunk.role]))
        {
            extra_chunks++;
        }
    }
    op_data->n_subops = extra_chunks;
    op_data->done = op_data->errors = 0;
    if (!extra_chunks)
    {
        return;
    }
    osd_op_t *subops = new osd_op_t[extra_chunks];
    op_data->subops = subops;
    int i = 0;
    for (auto & chunk: loc_set)
    {
        if (!cur_set || (rep ? !contains_osd(cur_set, set_size, chunk.osd_num) : chunk.osd_num != cur_set[chunk.role]))
        {
            int stripe_num = op_data->scheme == POOL_SCHEME_REPLICATED ? 0 : chunk.role;
            if (chunk.osd_num == this->osd_num)
            {
                clock_gettime(CLOCK_REALTIME, &subops[i].tv_begin);
                subops[i].op_type = (uint64_t)cur_op;
                subops[i].bs_op = new blockstore_op_t({
                    .opcode = BS_OP_DELETE,
                    .callback = [subop = &subops[i], this](blockstore_op_t *bs_subop)
                    {
                        handle_primary_bs_subop(subop);
                    },
                    .oid = {
                        .inode = op_data->oid.inode,
                        .stripe = op_data->oid.stripe | stripe_num,
                    },
                    // Same version as write
                    .version = op_data->fact_ver,
                });
                bs->enqueue_op(subops[i].bs_op);
            }
            else
            {
                subops[i].op_type = OSD_OP_OUT;
                subops[i].peer_fd = c_cli.osd_peer_fds.at(chunk.osd_num);
                subops[i].req.sec_del = {
                    .header = {
                        .magic = SECONDARY_OSD_OP_MAGIC,
                        .id = c_cli.next_subop_id++,
                        .opcode = OSD_OP_SEC_DELETE,
                    },
                    .oid = {
                        .inode = op_data->oid.inode,
                        .stripe = op_data->oid.stripe | stripe_num,
                    },
                    // Same version as write
                    .version = op_data->fact_ver,
                };
                subops[i].callback = [cur_op, this](osd_op_t *subop)
                {
                    int fail_fd = subop->reply.hdr.retval != 0 ? subop->peer_fd : -1;
                    handle_primary_subop(subop, cur_op);
                    if (fail_fd >= 0)
                    {
                        // delete operation failed, drop the connection
                        c_cli.stop_client(fail_fd);
                    }
                };
                c_cli.outbox_push(&subops[i]);
            }
            i++;
        }
    }
}

void osd_t::submit_primary_sync_subops(osd_op_t *cur_op)
{
    osd_primary_op_data_t *op_data = cur_op->op_data;
    int n_osds = op_data->unstable_write_osds->size();
    osd_op_t *subops = new osd_op_t[n_osds];
    op_data->done = op_data->errors = 0;
    op_data->n_subops = n_osds;
    op_data->subops = subops;
    for (int i = 0; i < n_osds; i++)
    {
        osd_num_t sync_osd = (*(op_data->unstable_write_osds))[i].osd_num;
        if (sync_osd == this->osd_num)
        {
            clock_gettime(CLOCK_REALTIME, &subops[i].tv_begin);
            subops[i].op_type = (uint64_t)cur_op;
            subops[i].bs_op = new blockstore_op_t({
                .opcode = BS_OP_SYNC,
                .callback = [subop = &subops[i], this](blockstore_op_t *bs_subop)
                {
                    handle_primary_bs_subop(subop);
                },
            });
            bs->enqueue_op(subops[i].bs_op);
        }
        else
        {
            subops[i].op_type = OSD_OP_OUT;
            subops[i].peer_fd = c_cli.osd_peer_fds.at(sync_osd);
            subops[i].req.sec_sync = {
                .header = {
                    .magic = SECONDARY_OSD_OP_MAGIC,
                    .id = c_cli.next_subop_id++,
                    .opcode = OSD_OP_SEC_SYNC,
                },
            };
            subops[i].callback = [cur_op, this](osd_op_t *subop)
            {
                int fail_fd = subop->reply.hdr.retval != 0 ? subop->peer_fd : -1;
                handle_primary_subop(subop, cur_op);
                if (fail_fd >= 0)
                {
                    // sync operation failed, drop the connection
                    c_cli.stop_client(fail_fd);
                }
            };
            c_cli.outbox_push(&subops[i]);
        }
    }
}

void osd_t::submit_primary_stab_subops(osd_op_t *cur_op)
{
    osd_primary_op_data_t *op_data = cur_op->op_data;
    int n_osds = op_data->unstable_write_osds->size();
    osd_op_t *subops = new osd_op_t[n_osds];
    op_data->done = op_data->errors = 0;
    op_data->n_subops = n_osds;
    op_data->subops = subops;
    for (int i = 0; i < n_osds; i++)
    {
        auto & stab_osd = (*(op_data->unstable_write_osds))[i];
        if (stab_osd.osd_num == this->osd_num)
        {
            clock_gettime(CLOCK_REALTIME, &subops[i].tv_begin);
            subops[i].op_type = (uint64_t)cur_op;
            subops[i].bs_op = new blockstore_op_t({
                .opcode = BS_OP_STABLE,
                .callback = [subop = &subops[i], this](blockstore_op_t *bs_subop)
                {
                    handle_primary_bs_subop(subop);
                },
                .len = (uint32_t)stab_osd.len,
                .buf = (void*)(op_data->unstable_writes + stab_osd.start),
            });
            bs->enqueue_op(subops[i].bs_op);
        }
        else
        {
            subops[i].op_type = OSD_OP_OUT;
            subops[i].peer_fd = c_cli.osd_peer_fds.at(stab_osd.osd_num);
            subops[i].req.sec_stab = {
                .header = {
                    .magic = SECONDARY_OSD_OP_MAGIC,
                    .id = c_cli.next_subop_id++,
                    .opcode = OSD_OP_SEC_STABILIZE,
                },
                .len = (uint64_t)(stab_osd.len * sizeof(obj_ver_id)),
            };
            subops[i].iov.push_back(op_data->unstable_writes + stab_osd.start, stab_osd.len * sizeof(obj_ver_id));
            subops[i].callback = [cur_op, this](osd_op_t *subop)
            {
                int fail_fd = subop->reply.hdr.retval != 0 ? subop->peer_fd : -1;
                handle_primary_subop(subop, cur_op);
                if (fail_fd >= 0)
                {
                    // sync operation failed, drop the connection
                    c_cli.stop_client(fail_fd);
                }
            };
            c_cli.outbox_push(&subops[i]);
        }
    }
}

void osd_t::pg_cancel_write_queue(pg_t & pg, osd_op_t *first_op, object_id oid, int retval)
{
    auto st_it = pg.write_queue.find(oid), it = st_it;
    finish_op(first_op, retval);
    if (it != pg.write_queue.end() && it->second == first_op)
    {
        it++;
    }
    else
    {
        // Write queue doesn't match the first operation.
        // first_op is a leftover operation from the previous peering of the same PG.
        return;
    }
    while (it != pg.write_queue.end() && it->first == oid)
    {
        finish_op(it->second, retval);
        it++;
    }
    if (st_it != it)
    {
        pg.write_queue.erase(st_it, it);
    }
}
