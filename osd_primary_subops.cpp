#include "osd_primary.h"

void osd_t::autosync()
{
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
    if (cur_op->op_data && cur_op->op_data->pg_num > 0)
    {
        auto & pg = pgs[cur_op->op_data->pg_num];
        pg.inflight--;
        assert(pg.inflight >= 0);
        if ((pg.state & PG_STOPPING) && pg.inflight == 0 && !pg.flush_batch)
        {
            finish_stop_pg(pg);
        }
    }
    if (!cur_op->peer_fd)
    {
        // Copy lambda to be unaffected by `delete op`
        std::function<void(osd_op_t*)>(cur_op->callback)(cur_op);
    }
    else
    {
        // FIXME add separate magic number
        auto cl_it = clients.find(cur_op->peer_fd);
        if (cl_it != clients.end())
        {
            cur_op->reply.hdr.magic = SECONDARY_OSD_REPLY_MAGIC;
            cur_op->reply.hdr.id = cur_op->req.hdr.id;
            cur_op->reply.hdr.opcode = cur_op->req.hdr.opcode;
            cur_op->reply.hdr.retval = retval;
            outbox_push(cl_it->second, cur_op);
        }
        else
        {
            delete cur_op;
        }
    }
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
    uint64_t op_version = w ? op_data->fact_ver+1 : (submit_type == SUBMIT_RMW_READ ? UINT64_MAX : op_data->target_ver);
    osd_op_t *subops = new osd_op_t[n_subops];
    op_data->fact_ver = 0;
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
                        if (subop->opcode == BS_OP_WRITE && subop->retval != subop->len)
                        {
                            // die
                            throw std::runtime_error("local write operation failed (retval = "+std::to_string(subop->retval)+")");
                        }
                        handle_primary_subop(
                            subop->opcode == BS_OP_WRITE ? OSD_OP_SECONDARY_WRITE : OSD_OP_SECONDARY_READ,
                            cur_op, subop->retval, subop->len, subop->version
                        );
                    },
                    .oid = {
                        .inode = op_data->oid.inode,
                        .stripe = op_data->oid.stripe | role,
                    },
                    .version = op_version,
                    .offset = w ? stripes[role].write_start : stripes[role].read_start,
                    .len = w ? stripes[role].write_end - stripes[role].write_start : stripes[role].read_end - stripes[role].read_start,
                    .buf = w ? stripes[role].write_buf : stripes[role].read_buf,
                });
                bs->enqueue_op(subops[subop].bs_op);
            }
            else
            {
                subops[subop].op_type = OSD_OP_OUT;
                subops[subop].send_list.push_back(subops[subop].req.buf, OSD_PACKET_SIZE);
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
                    .version = op_version,
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
                    int fail_fd = subop->req.hdr.opcode == OSD_OP_SECONDARY_WRITE &&
                        subop->reply.hdr.retval != subop->req.sec_rw.len ? subop->peer_fd : -1;
                    // so it doesn't get freed
                    subop->buf = NULL;
                    handle_primary_subop(
                        subop->req.hdr.opcode, cur_op, subop->reply.hdr.retval,
                        subop->req.sec_rw.len, subop->reply.sec_rw.version
                    );
                    if (fail_fd >= 0)
                    {
                        // write operation failed, drop the connection
                        stop_client(fail_fd);
                    }
                };
                outbox_push(clients[subops[subop].peer_fd], &subops[subop]);
            }
            subop++;
        }
    }
}

void osd_t::handle_primary_subop(uint64_t opcode, osd_op_t *cur_op, int retval, int expected, uint64_t version)
{
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
        if (opcode == OSD_OP_SECONDARY_READ || opcode == OSD_OP_SECONDARY_WRITE)
        {
            if (op_data->fact_ver != 0 && op_data->fact_ver != version)
            {
                throw std::runtime_error("different fact_versions returned from subops: "+std::to_string(version)+" vs "+std::to_string(op_data->fact_ver));
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

void osd_t::submit_primary_del_subops(osd_op_t *cur_op, uint64_t *cur_set, pg_osd_set_t & loc_set)
{
    osd_primary_op_data_t *op_data = cur_op->op_data;
    int extra_chunks = 0;
    for (auto & chunk: loc_set)
    {
        if (!cur_set || chunk.osd_num != cur_set[chunk.role])
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
        if (!cur_set || chunk.osd_num != cur_set[chunk.role])
        {
            if (chunk.osd_num == this->osd_num)
            {
                subops[i].bs_op = new blockstore_op_t({
                    .opcode = BS_OP_DELETE,
                    .callback = [cur_op, this](blockstore_op_t *subop)
                    {
                        if (subop->retval != 0)
                        {
                            // die
                            throw std::runtime_error("local delete operation failed");
                        }
                        handle_primary_subop(OSD_OP_SECONDARY_DELETE, cur_op, subop->retval, 0, 0);
                    },
                    .oid = {
                        .inode = op_data->oid.inode,
                        .stripe = op_data->oid.stripe | chunk.role,
                    },
                    // Same version as write
                    .version = op_data->fact_ver,
                });
                bs->enqueue_op(subops[i].bs_op);
            }
            else
            {
                subops[i].op_type = OSD_OP_OUT;
                subops[i].send_list.push_back(subops[i].req.buf, OSD_PACKET_SIZE);
                subops[i].peer_fd = osd_peer_fds.at(chunk.osd_num);
                subops[i].req.sec_del = {
                    .header = {
                        .magic = SECONDARY_OSD_OP_MAGIC,
                        .id = this->next_subop_id++,
                        .opcode = OSD_OP_SECONDARY_DELETE,
                    },
                    .oid = {
                        .inode = op_data->oid.inode,
                        .stripe = op_data->oid.stripe | chunk.role,
                    },
                    // Same version as write
                    .version = op_data->fact_ver,
                };
                subops[i].callback = [cur_op, this](osd_op_t *subop)
                {
                    int fail_fd = subop->reply.hdr.retval != 0 ? subop->peer_fd : 0;
                    handle_primary_subop(OSD_OP_SECONDARY_DELETE, cur_op, subop->reply.hdr.retval, 0, 0);
                    if (fail_fd >= 0)
                    {
                        // delete operation failed, drop the connection
                        stop_client(fail_fd);
                    }
                };
                outbox_push(clients[subops[i].peer_fd], &subops[i]);
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
            subops[i].bs_op = new blockstore_op_t({
                .opcode = BS_OP_SYNC,
                .callback = [cur_op, this](blockstore_op_t *subop)
                {
                    if (subop->retval != 0)
                    {
                        // die
                        throw std::runtime_error("local sync operation failed");
                    }
                    handle_primary_subop(OSD_OP_SECONDARY_SYNC, cur_op, subop->retval, 0, 0);
                },
            });
            bs->enqueue_op(subops[i].bs_op);
        }
        else
        {
            subops[i].op_type = OSD_OP_OUT;
            subops[i].send_list.push_back(subops[i].req.buf, OSD_PACKET_SIZE);
            subops[i].peer_fd = osd_peer_fds.at(sync_osd);
            subops[i].req.sec_sync = {
                .header = {
                    .magic = SECONDARY_OSD_OP_MAGIC,
                    .id = this->next_subop_id++,
                    .opcode = OSD_OP_SECONDARY_SYNC,
                },
            };
            subops[i].callback = [cur_op, this](osd_op_t *subop)
            {
                int fail_fd = subop->reply.hdr.retval != 0 ? subop->peer_fd : 0;
                handle_primary_subop(OSD_OP_SECONDARY_SYNC, cur_op, subop->reply.hdr.retval, 0, 0);
                if (fail_fd >= 0)
                {
                    // sync operation failed, drop the connection
                    stop_client(fail_fd);
                }
            };
            outbox_push(clients[subops[i].peer_fd], &subops[i]);
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
            subops[i].bs_op = new blockstore_op_t({
                .opcode = BS_OP_STABLE,
                .callback = [cur_op, this](blockstore_op_t *subop)
                {
                    if (subop->retval != 0)
                    {
                        // die
                        throw std::runtime_error("local stabilize operation failed");
                    }
                    handle_primary_subop(OSD_OP_SECONDARY_STABILIZE, cur_op, subop->retval, 0, 0);
                },
                .len = (uint32_t)stab_osd.len,
                .buf = (void*)(op_data->unstable_writes + stab_osd.start),
            });
            bs->enqueue_op(subops[i].bs_op);
        }
        else
        {
            subops[i].op_type = OSD_OP_OUT;
            subops[i].send_list.push_back(subops[i].req.buf, OSD_PACKET_SIZE);
            subops[i].peer_fd = osd_peer_fds.at(stab_osd.osd_num);
            subops[i].req.sec_stab = {
                .header = {
                    .magic = SECONDARY_OSD_OP_MAGIC,
                    .id = this->next_subop_id++,
                    .opcode = OSD_OP_SECONDARY_STABILIZE,
                },
                .len = (uint64_t)(stab_osd.len * sizeof(obj_ver_id)),
            };
            subops[i].send_list.push_back(op_data->unstable_writes + stab_osd.start, stab_osd.len * sizeof(obj_ver_id));
            subops[i].callback = [cur_op, this](osd_op_t *subop)
            {
                int fail_fd = subop->reply.hdr.retval != 0 ? subop->peer_fd : 0;
                handle_primary_subop(OSD_OP_SECONDARY_STABILIZE, cur_op, subop->reply.hdr.retval, 0, 0);
                if (fail_fd >= 0)
                {
                    // sync operation failed, drop the connection
                    stop_client(fail_fd);
                }
            };
            outbox_push(clients[subops[i].peer_fd], &subops[i]);
        }
    }
}

void osd_t::pg_cancel_write_queue(pg_t & pg, object_id oid, int retval)
{
    auto st_it = pg.write_queue.find(oid), it = st_it;
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
