#include <netinet/tcp.h>
#include <sys/epoll.h>

#include <algorithm>

#include "base64.h"
#include "osd.h"

void osd_t::connect_peer(osd_num_t peer_osd, const char *peer_host, int peer_port, std::function<void(osd_num_t, int)> callback)
{
    struct sockaddr_in addr;
    int r;
    if ((r = inet_pton(AF_INET, peer_host, &addr.sin_addr)) != 1)
    {
        callback(peer_osd, -EINVAL);
        return;
    }
    addr.sin_family = AF_INET;
    addr.sin_port = htons(peer_port ? peer_port : 11203);
    int peer_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (peer_fd < 0)
    {
        callback(peer_osd, -errno);
        return;
    }
    fcntl(peer_fd, F_SETFL, fcntl(peer_fd, F_GETFL, 0) | O_NONBLOCK);
    int timeout_id = -1;
    if (peer_connect_timeout > 0)
    {
        timeout_id = tfd->set_timer(1000*peer_connect_timeout, false, [this, peer_fd](int timer_id)
        {
            auto callback = clients[peer_fd].connect_callback;
            osd_num_t peer_osd = clients[peer_fd].osd_num;
            stop_client(peer_fd);
            callback(peer_osd, -EIO);
            return;
        });
    }
    r = connect(peer_fd, (sockaddr*)&addr, sizeof(addr));
    if (r < 0 && errno != EINPROGRESS)
    {
        close(peer_fd);
        callback(peer_osd, -errno);
        return;
    }
    assert(peer_osd != osd_num);
    clients[peer_fd] = (osd_client_t){
        .peer_addr = addr,
        .peer_port = peer_port,
        .peer_fd = peer_fd,
        .peer_state = PEER_CONNECTING,
        .connect_callback = callback,
        .connect_timeout_id = timeout_id,
        .osd_num = peer_osd,
        .in_buf = malloc(receive_buffer_size),
    };
    // Add FD to epoll (EPOLLOUT for tracking connect() result)
    epoll_event ev;
    ev.data.fd = peer_fd;
    ev.events = EPOLLOUT | EPOLLIN | EPOLLRDHUP | EPOLLET;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, peer_fd, &ev) < 0)
    {
        throw std::runtime_error(std::string("epoll_ctl: ") + strerror(errno));
    }
}

void osd_t::handle_connect_result(int peer_fd)
{
    auto & cl = clients[peer_fd];
    if (cl.connect_timeout_id >= 0)
    {
        tfd->clear_timer(cl.connect_timeout_id);
        cl.connect_timeout_id = -1;
    }
    osd_num_t peer_osd = cl.osd_num;
    int result = 0;
    socklen_t result_len = sizeof(result);
    if (getsockopt(peer_fd, SOL_SOCKET, SO_ERROR, &result, &result_len) < 0)
    {
        result = errno;
    }
    if (result != 0)
    {
        auto callback = cl.connect_callback;
        stop_client(peer_fd);
        callback(peer_osd, -result);
        return;
    }
    int one = 1;
    setsockopt(peer_fd, SOL_TCP, TCP_NODELAY, &one, sizeof(one));
    // Disable EPOLLOUT on this fd
    cl.peer_state = PEER_CONNECTED;
    epoll_event ev;
    ev.data.fd = peer_fd;
    ev.events = EPOLLIN | EPOLLRDHUP | EPOLLET;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_MOD, peer_fd, &ev) < 0)
    {
        throw std::runtime_error(std::string("epoll_ctl: ") + strerror(errno));
    }
    // Check OSD number
    check_peer_config(cl);
}

void osd_t::check_peer_config(osd_client_t & cl)
{
    osd_op_t *op = new osd_op_t();
    op->op_type = OSD_OP_OUT;
    op->send_list.push_back(op->req.buf, OSD_PACKET_SIZE);
    op->peer_fd = cl.peer_fd;
    op->req = {
        .show_conf = {
            .header = {
                .magic = SECONDARY_OSD_OP_MAGIC,
                .id = this->next_subop_id++,
                .opcode = OSD_OP_SHOW_CONFIG,
            },
        },
    };
    op->callback = [this](osd_op_t *op)
    {
        std::string json_err;
        json11::Json config = json11::Json::parse(std::string((char*)op->buf), json_err);
        osd_client_t & cl = clients[op->peer_fd];
        bool err = false;
        if (op->reply.hdr.retval < 0)
        {
            err = true;
            printf("Failed to get config from OSD %lu (retval=%ld), disconnecting peer\n", cl.osd_num, op->reply.hdr.retval);
        }
        else if (json_err != "")
        {
            err = true;
            printf("Failed to get config from OSD %lu: bad JSON: %s, disconnecting peer\n", cl.osd_num, json_err.c_str());
        }
        else if (config["osd_num"].uint64_value() != cl.osd_num)
        {
            err = true;
            printf("Connected to OSD %lu instead of OSD %lu, peer state is outdated, disconnecting peer\n", config["osd_num"].uint64_value(), cl.osd_num);
        }
        if (err)
        {
            stop_client(op->peer_fd);
            delete op;
            return;
        }
        osd_peer_fds[cl.osd_num] = cl.peer_fd;
        auto callback = cl.connect_callback;
        cl.connect_callback = NULL;
        callback(cl.osd_num, cl.peer_fd);
        delete op;
    };
    outbox_push(cl, op);
}

// Peering loop
void osd_t::handle_peers()
{
    if (peering_state & OSD_CONNECTING_PEERS)
    {
        load_and_connect_peers();
    }
    if (peering_state & OSD_PEERING_PGS)
    {
        bool still = false;
        for (auto & p: pgs)
        {
            if (p.second.state == PG_PEERING)
            {
                if (!p.second.peering_state->list_ops.size())
                {
                    p.second.calc_object_states(log_level);
                    report_pg_state(p.second);
                    incomplete_objects += p.second.incomplete_objects.size();
                    misplaced_objects += p.second.misplaced_objects.size();
                    // FIXME: degraded objects may currently include misplaced, too! Report them separately?
                    degraded_objects += p.second.degraded_objects.size();
                    if ((p.second.state & (PG_ACTIVE | PG_HAS_UNCLEAN)) == (PG_ACTIVE | PG_HAS_UNCLEAN))
                        peering_state = peering_state | OSD_FLUSHING_PGS;
                    else
                        peering_state = peering_state | OSD_RECOVERING;
                }
                else
                {
                    still = true;
                }
            }
        }
        if (!still)
        {
            // Done all PGs
            peering_state = peering_state & ~OSD_PEERING_PGS;
        }
    }
    if ((peering_state & OSD_FLUSHING_PGS) && !readonly)
    {
        bool still = false;
        for (auto & p: pgs)
        {
            if ((p.second.state & (PG_ACTIVE | PG_HAS_UNCLEAN)) == (PG_ACTIVE | PG_HAS_UNCLEAN))
            {
                if (!p.second.flush_batch)
                {
                    submit_pg_flush_ops(p.first);
                }
                still = true;
            }
        }
        if (!still)
        {
            peering_state = peering_state & ~OSD_FLUSHING_PGS | OSD_RECOVERING;
        }
    }
    if ((peering_state & OSD_RECOVERING) && !readonly)
    {
        if (!continue_recovery())
        {
            peering_state = peering_state & ~OSD_RECOVERING;
        }
    }
}

void osd_t::repeer_pgs(osd_num_t peer_osd)
{
    // Re-peer affected PGs
    for (auto & p: pgs)
    {
        bool repeer = false;
        if (p.second.state & (PG_PEERING | PG_ACTIVE | PG_INCOMPLETE))
        {
            for (osd_num_t pg_osd: p.second.all_peers)
            {
                if (pg_osd == peer_osd)
                {
                    repeer = true;
                    break;
                }
            }
            if (repeer)
            {
                // Repeer this pg
                printf("[PG %u] Repeer because of OSD %lu\n", p.second.pg_num, peer_osd);
                start_pg_peering(p.second.pg_num);
            }
        }
    }
}

// Repeer on each connect/disconnect peer event
void osd_t::start_pg_peering(pg_num_t pg_num)
{
    auto & pg = pgs[pg_num];
    pg.state = PG_PEERING;
    this->peering_state |= OSD_PEERING_PGS;
    report_pg_state(pg);
    // Reset PG state
    pg.state_dict.clear();
    incomplete_objects -= pg.incomplete_objects.size();
    misplaced_objects -= pg.misplaced_objects.size();
    degraded_objects -= pg.degraded_objects.size();
    pg.incomplete_objects.clear();
    pg.misplaced_objects.clear();
    pg.degraded_objects.clear();
    pg.flush_actions.clear();
    pg.ver_override.clear();
    if (pg.flush_batch)
    {
        delete pg.flush_batch;
    }
    pg.flush_batch = NULL;
    for (auto p: pg.write_queue)
    {
        cancel_op(p.second);
    }
    pg.write_queue.clear();
    for (auto it = unstable_writes.begin(); it != unstable_writes.end(); )
    {
        // Forget this PG's unstable writes
        pg_num_t n = (it->first.oid.inode + it->first.oid.stripe / pg_stripe_size) % pg_count + 1;
        if (n == pg.pg_num)
            unstable_writes.erase(it++);
        else
            it++;
    }
    pg.inflight = 0;
    dirty_pgs.erase(pg.pg_num);
    // Calculate current write OSD set
    pg.pg_cursize = 0;
    pg.cur_set.resize(pg.target_set.size());
    for (int role = 0; role < pg.target_set.size(); role++)
    {
        pg.cur_set[role] = pg.target_set[role] == this->osd_num ||
            osd_peer_fds.find(pg.target_set[role]) != osd_peer_fds.end() ? pg.target_set[role] : 0;
        if (pg.cur_set[role] != 0)
        {
            pg.pg_cursize++;
        }
    }
    if (pg.target_history.size())
    {
        // Refuse to start PG if no peers are available from any of the historical OSD sets
        // (PG history is kept up to the latest active+clean state)
        for (auto & history_set: pg.target_history)
        {
            bool found = false;
            for (auto history_osd: history_set)
            {
                if (history_osd != 0 && osd_peer_fds.find(history_osd) != osd_peer_fds.end())
                {
                    found = true;
                    break;
                }
            }
            if (!found)
            {
                pg.state = PG_INCOMPLETE;
                report_pg_state(pg);
            }
        }
    }
    if (pg.pg_cursize < pg.pg_minsize)
    {
        pg.state = PG_INCOMPLETE;
        report_pg_state(pg);
    }
    std::set<osd_num_t> cur_peers;
    for (auto peer_osd: pg.all_peers)
    {
        if (peer_osd == this->osd_num || osd_peer_fds.find(peer_osd) != osd_peer_fds.end())
        {
            cur_peers.insert(peer_osd);
        }
        else if (wanted_peers.find(peer_osd) == wanted_peers.end())
        {
            wanted_peers[peer_osd] = { 0 };
            peering_state |= OSD_CONNECTING_PEERS;
        }
    }
    if (pg.peering_state)
    {
        // Adjust the peering operation that's still in progress - discard unneeded results
        for (auto it = pg.peering_state->list_ops.begin(); it != pg.peering_state->list_ops.end();)
        {
            if (pg.state == PG_INCOMPLETE || cur_peers.find(it->first) == cur_peers.end())
            {
                // Discard the result after completion, which, chances are, will be unsuccessful
                discard_list_subop(it->second);
                pg.peering_state->list_ops.erase(it);
                it = pg.peering_state->list_ops.begin();
            }
            else
                it++;
        }
        for (auto it = pg.peering_state->list_results.begin(); it != pg.peering_state->list_results.end();)
        {
            if (pg.state == PG_INCOMPLETE || cur_peers.find(it->first) == cur_peers.end())
            {
                if (it->second.buf)
                {
                    free(it->second.buf);
                }
                pg.peering_state->list_results.erase(it);
                it = pg.peering_state->list_results.begin();
            }
            else
                it++;
        }
    }
    if (pg.state == PG_INCOMPLETE)
    {
        if (pg.peering_state)
        {
            delete pg.peering_state;
            pg.peering_state = NULL;
        }
        return;
    }
    if (!pg.peering_state)
    {
        pg.peering_state = new pg_peering_state_t();
        pg.peering_state->pg_num = pg.pg_num;
    }
    for (osd_num_t peer_osd: cur_peers)
    {
        if (pg.peering_state->list_ops.find(peer_osd) != pg.peering_state->list_ops.end() ||
            pg.peering_state->list_results.find(peer_osd) != pg.peering_state->list_results.end())
        {
            continue;
        }
        submit_sync_and_list_subop(peer_osd, pg.peering_state);
    }
    ringloop->wakeup();
}

void osd_t::submit_sync_and_list_subop(osd_num_t role_osd, pg_peering_state_t *ps)
{
    // Sync before listing, if not readonly
    if (readonly)
    {
        submit_list_subop(role_osd, ps);
    }
    else if (role_osd == this->osd_num)
    {
        // Self
        osd_op_t *op = new osd_op_t();
        op->op_type = 0;
        op->peer_fd = 0;
        op->bs_op = new blockstore_op_t();
        op->bs_op->opcode = BS_OP_SYNC;
        op->bs_op->callback = [this, ps, op, role_osd](blockstore_op_t *bs_op)
        {
            if (bs_op->retval < 0)
            {
                printf("Local OP_SYNC failed: %d (%s)\n", bs_op->retval, strerror(-bs_op->retval));
                force_stop(1);
                return;
            }
            delete op;
            ps->list_ops.erase(role_osd);
            submit_list_subop(role_osd, ps);
        };
        bs->enqueue_op(op->bs_op);
        ps->list_ops[role_osd] = op;
    }
    else
    {
        // Peer
        auto & cl = clients[osd_peer_fds[role_osd]];
        osd_op_t *op = new osd_op_t();
        op->op_type = OSD_OP_OUT;
        op->send_list.push_back(op->req.buf, OSD_PACKET_SIZE);
        op->peer_fd = cl.peer_fd;
        op->req = {
            .sec_sync = {
                .header = {
                    .magic = SECONDARY_OSD_OP_MAGIC,
                    .id = this->next_subop_id++,
                    .opcode = OSD_OP_SECONDARY_SYNC,
                },
            },
        };
        op->callback = [this, ps, role_osd](osd_op_t *op)
        {
            if (op->reply.hdr.retval < 0)
            {
                // FIXME: Mark peer as failed and don't reconnect immediately after dropping the connection
                printf("Failed to sync OSD %lu: %ld (%s), disconnecting peer\n", role_osd, op->reply.hdr.retval, strerror(-op->reply.hdr.retval));
                ps->list_ops.erase(role_osd);
                stop_client(op->peer_fd);
                delete op;
                return;
            }
            delete op;
            ps->list_ops.erase(role_osd);
            submit_list_subop(role_osd, ps);
        };
        outbox_push(cl, op);
        ps->list_ops[role_osd] = op;
    }
}

void osd_t::submit_list_subop(osd_num_t role_osd, pg_peering_state_t *ps)
{
    if (role_osd == this->osd_num)
    {
        // Self
        osd_op_t *op = new osd_op_t();
        op->op_type = 0;
        op->peer_fd = 0;
        op->bs_op = new blockstore_op_t();
        op->bs_op->opcode = BS_OP_LIST;
        op->bs_op->oid.stripe = pg_stripe_size;
        op->bs_op->len = pg_count;
        op->bs_op->offset = ps->pg_num-1;
        op->bs_op->callback = [ps, op, role_osd](blockstore_op_t *bs_op)
        {
            if (op->bs_op->retval < 0)
            {
                throw std::runtime_error("local OP_LIST failed");
            }
            printf(
                "[PG %u] Got object list from OSD %lu (local): %d object versions (%lu of them stable)\n",
                ps->pg_num, role_osd, bs_op->retval, bs_op->version
            );
            ps->list_results[role_osd] = {
                .buf = (obj_ver_id*)op->bs_op->buf,
                .total_count = (uint64_t)op->bs_op->retval,
                .stable_count = op->bs_op->version,
            };
            ps->list_ops.erase(role_osd);
            delete op;
        };
        bs->enqueue_op(op->bs_op);
        ps->list_ops[role_osd] = op;
    }
    else
    {
        // Peer
        auto & cl = clients[osd_peer_fds[role_osd]];
        osd_op_t *op = new osd_op_t();
        op->op_type = OSD_OP_OUT;
        op->send_list.push_back(op->req.buf, OSD_PACKET_SIZE);
        op->peer_fd = cl.peer_fd;
        op->req = {
            .sec_list = {
                .header = {
                    .magic = SECONDARY_OSD_OP_MAGIC,
                    .id = this->next_subop_id++,
                    .opcode = OSD_OP_SECONDARY_LIST,
                },
                .list_pg = ps->pg_num,
                .pg_count = pg_count,
                .pg_stripe_size = pg_stripe_size,
            },
        };
        op->callback = [this, ps, role_osd](osd_op_t *op)
        {
            if (op->reply.hdr.retval < 0)
            {
                printf("Failed to get object list from OSD %lu (retval=%ld), disconnecting peer\n", role_osd, op->reply.hdr.retval);
                ps->list_ops.erase(role_osd);
                stop_client(op->peer_fd);
                delete op;
                return;
            }
            printf(
                "[PG %u] Got object list from OSD %lu: %ld object versions (%lu of them stable)\n",
                ps->pg_num, role_osd, op->reply.hdr.retval, op->reply.sec_list.stable_count
            );
            ps->list_results[role_osd] = {
                .buf = (obj_ver_id*)op->buf,
                .total_count = (uint64_t)op->reply.hdr.retval,
                .stable_count = op->reply.sec_list.stable_count,
            };
            // set op->buf to NULL so it doesn't get freed
            op->buf = NULL;
            ps->list_ops.erase(role_osd);
            delete op;
        };
        outbox_push(cl, op);
        ps->list_ops[role_osd] = op;
    }
}

void osd_t::discard_list_subop(osd_op_t *list_op)
{
    if (list_op->peer_fd == 0)
    {
        // Self
        list_op->bs_op->callback = [list_op](blockstore_op_t *bs_op)
        {
            if (list_op->bs_op->buf)
                free(list_op->bs_op->buf);
            delete list_op;
        };
    }
    else
    {
        // Peer
        list_op->callback = [](osd_op_t *list_op)
        {
            delete list_op;
        };
    }
}

bool osd_t::stop_pg(pg_num_t pg_num)
{
    auto pg_it = pgs.find(pg_num);
    if (pg_it == pgs.end())
    {
        return false;
    }
    auto & pg = pg_it->second;
    if (pg.peering_state)
    {
        // Stop peering
        for (auto it = pg.peering_state->list_ops.begin(); it != pg.peering_state->list_ops.end();)
        {
            discard_list_subop(it->second);
        }
        for (auto it = pg.peering_state->list_results.begin(); it != pg.peering_state->list_results.end();)
        {
            if (it->second.buf)
            {
                free(it->second.buf);
            }
        }
        delete pg.peering_state;
        pg.peering_state = NULL;
    }
    if (!(pg.state & PG_ACTIVE))
    {
        return false;
    }
    pg.state = pg.state & ~PG_ACTIVE | PG_STOPPING;
    if (pg.inflight == 0 && !pg.flush_batch)
    {
        finish_stop_pg(pg);
    }
    else
    {
        report_pg_state(pg);
    }
    return true;
}

void osd_t::finish_stop_pg(pg_t & pg)
{
    pg.state = PG_OFFLINE;
    report_pg_state(pg);
}

void osd_t::report_pg_state(pg_t & pg)
{
    pg.print_state();
    this->pg_state_dirty.insert(pg.pg_num);
    if (pg.state == PG_OFFLINE && !this->pg_config_applied)
    {
        apply_pg_config();
    }
    report_pg_states();
}
