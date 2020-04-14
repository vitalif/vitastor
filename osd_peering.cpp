#include <netinet/tcp.h>
#include <sys/epoll.h>

#include <algorithm>

#include "osd.h"

void osd_t::init_primary()
{
    // Initial test version of clustering code requires exactly 2 peers
    // FIXME Hardcode
    std::string peerstr = config["peers"];
    while (peerstr.size())
    {
        int pos = peerstr.find(',');
        peers.push_back(parse_peer(pos < 0 ? peerstr : peerstr.substr(0, pos)));
        peerstr = pos < 0 ? std::string("") : peerstr.substr(pos+1);
        for (int i = 0; i < peers.size()-1; i++)
            if (peers[i].osd_num == peers[peers.size()-1].osd_num)
                throw std::runtime_error("same osd number "+std::to_string(peers[i].osd_num)+" specified twice in peers");
    }
    if (peers.size() < 2)
        throw std::runtime_error("run_primary requires at least 2 peers");
    pgs[1] = (pg_t){
        .state = PG_PEERING,
        .pg_cursize = 0,
        .pg_num = 1,
        .target_set = { 1, 2, 3 },
        .cur_set = { 1, 0, 0 },
    };
    pgs[1].print_state();
    pg_count = 1;
    peering_state = OSD_CONNECTING_PEERS;
    if (consul_address != "")
    {
        this->consul_tfd = new timerfd_interval(ringloop, consul_report_interval, [this]()
        {
            report_status();
        });
    }
    if (autosync_interval > 0)
    {
        this->sync_tfd = new timerfd_interval(ringloop, 3, [this]()
        {
            autosync();
        });
    }
}

osd_peer_def_t osd_t::parse_peer(std::string peer)
{
    // OSD_NUM:IP:PORT
    int pos1 = peer.find(':');
    int pos2 = peer.find(':', pos1+1);
    if (pos1 < 0 || pos2 < 0)
        throw new std::runtime_error("OSD peer string must be in the form OSD_NUM:IP:PORT");
    osd_peer_def_t r;
    r.addr = peer.substr(pos1+1, pos2-pos1-1);
    std::string osd_num_str = peer.substr(0, pos1);
    std::string port_str = peer.substr(pos2+1);
    r.osd_num = strtoull(osd_num_str.c_str(), NULL, 10);
    if (!r.osd_num)
        throw new std::runtime_error("Could not parse OSD peer osd_num");
    r.port = strtoull(port_str.c_str(), NULL, 10);
    if (!r.port)
        throw new std::runtime_error("Could not parse OSD peer port");
    return r;
}

void osd_t::connect_peer(osd_num_t osd_num, const char *peer_host, int peer_port, std::function<void(osd_num_t, int)> callback)
{
    struct sockaddr_in addr;
    int r;
    if ((r = inet_pton(AF_INET, peer_host, &addr.sin_addr)) != 1)
    {
        callback(osd_num, -EINVAL);
        return;
    }
    addr.sin_family = AF_INET;
    addr.sin_port = htons(peer_port ? peer_port : 11203);
    int peer_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (peer_fd < 0)
    {
        callback(osd_num, -errno);
        return;
    }
    fcntl(peer_fd, F_SETFL, fcntl(peer_fd, F_GETFL, 0) | O_NONBLOCK);
    r = connect(peer_fd, (sockaddr*)&addr, sizeof(addr));
    if (r < 0 && errno != EINPROGRESS)
    {
        close(peer_fd);
        callback(osd_num, -errno);
        return;
    }
    clients[peer_fd] = (osd_client_t){
        .peer_addr = addr,
        .peer_port = peer_port,
        .peer_fd = peer_fd,
        .peer_state = PEER_CONNECTING,
        .connect_callback = callback,
        .osd_num = osd_num,
        .in_buf = malloc(receive_buffer_size),
    };
    osd_peer_fds[osd_num] = peer_fd;
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
    osd_num_t osd_num = cl.osd_num;
    auto callback = cl.connect_callback;
    int result = 0;
    socklen_t result_len = sizeof(result);
    if (getsockopt(peer_fd, SOL_SOCKET, SO_ERROR, &result, &result_len) < 0)
    {
        result = errno;
    }
    if (result != 0)
    {
        stop_client(peer_fd);
        callback(osd_num, -result);
        return;
    }
    int one = 1;
    setsockopt(peer_fd, SOL_TCP, TCP_NODELAY, &one, sizeof(one));
    // Disable EPOLLOUT on this fd
    cl.connect_callback = NULL;
    cl.peer_state = PEER_CONNECTED;
    epoll_event ev;
    ev.data.fd = peer_fd;
    ev.events = EPOLLIN | EPOLLRDHUP | EPOLLET;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_MOD, peer_fd, &ev) < 0)
    {
        throw std::runtime_error(std::string("epoll_ctl: ") + strerror(errno));
    }
    callback(osd_num, peer_fd);
}

// Peering loop
void osd_t::handle_peers()
{
    if (peering_state & OSD_CONNECTING_PEERS)
    {
        for (int i = 0; i < peers.size(); i++)
        {
            if (osd_peer_fds.find(peers[i].osd_num) == osd_peer_fds.end() &&
                time(NULL) - peers[i].last_connect_attempt > 5) // FIXME hardcode 5
            {
                peers[i].last_connect_attempt = time(NULL);
                connect_peer(peers[i].osd_num, peers[i].addr.c_str(), peers[i].port, [this](osd_num_t osd_num, int peer_fd)
                {
                    // FIXME: Check peer config after connecting
                    if (peer_fd < 0)
                    {
                        printf("Failed to connect to peer OSD %lu: %s\n", osd_num, strerror(-peer_fd));
                        return;
                    }
                    printf("Connected with peer OSD %lu (fd %d)\n", clients[peer_fd].osd_num, peer_fd);
                    int i;
                    for (i = 0; i < peers.size(); i++)
                    {
                        if (osd_peer_fds.find(peers[i].osd_num) == osd_peer_fds.end())
                            break;
                    }
                    if (i >= peers.size())
                    {
                        // Connected to all peers
                        peering_state = peering_state & ~OSD_CONNECTING_PEERS;
                    }
                    repeer_pgs(osd_num, true);
                });
            }
        }
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
                    p.second.calc_object_states();
                    incomplete_objects += p.second.incomplete_objects.size();
                    misplaced_objects += p.second.misplaced_objects.size();
                    // FIXME: degraded objects may currently include misplaced, too! Report them separately?
                    degraded_objects += p.second.degraded_objects.size();
                    if (p.second.state & PG_HAS_UNCLEAN)
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
            if (p.second.state & PG_HAS_UNCLEAN)
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

void osd_t::repeer_pgs(osd_num_t osd_num, bool is_connected)
{
    // Re-peer affected PGs
    // FIXME: We shouldn't rely just on target_set. Other OSDs may also contain PG data.
    osd_num_t real_osd = (is_connected ? osd_num : 0);
    for (auto & p: pgs)
    {
        bool repeer = false;
        if (p.second.state != PG_OFFLINE)
        {
            for (int r = 0; r < p.second.target_set.size(); r++)
            {
                if (p.second.target_set[r] == osd_num &&
                    p.second.cur_set[r] != real_osd)
                {
                    p.second.cur_set[r] = real_osd;
                    repeer = true;
                    break;
                }
            }
            if (repeer)
            {
                // Repeer this pg
                printf("Repeer PG %d because of OSD %lu\n", p.second.pg_num, osd_num);
                start_pg_peering(p.second.pg_num);
                peering_state |= OSD_PEERING_PGS;
            }
        }
    }
}

// Repeer on each connect/disconnect peer event
void osd_t::start_pg_peering(pg_num_t pg_num)
{
    auto & pg = pgs[pg_num];
    pg.state = PG_PEERING;
    pg.print_state();
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
    // Start peering
    pg.pg_cursize = 0;
    for (int role = 0; role < pg.cur_set.size(); role++)
    {
        if (pg.cur_set[role] != 0)
        {
            pg.pg_cursize++;
        }
    }
    if (pg.pg_cursize < pg.pg_minsize)
    {
        pg.state = PG_INCOMPLETE;
        pg.print_state();
    }
    if (pg.peering_state)
    {
        // Adjust the peering operation that's still in progress
        for (auto it = pg.peering_state->list_ops.begin(); it != pg.peering_state->list_ops.end(); it++)
        {
            int role;
            for (role = 0; role < pg.cur_set.size(); role++)
            {
                if (pg.cur_set[role] == it->first)
                    break;
            }
            if (pg.state == PG_INCOMPLETE || role >= pg.cur_set.size())
            {
                // Discard the result after completion, which, chances are, will be unsuccessful
                auto list_op = it->second;
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
                pg.peering_state->list_ops.erase(it);
                it = pg.peering_state->list_ops.begin();
            }
        }
        for (auto it = pg.peering_state->list_results.begin(); it != pg.peering_state->list_results.end(); it++)
        {
            int role;
            for (role = 0; role < pg.cur_set.size(); role++)
            {
                if (pg.cur_set[role] == it->first)
                    break;
            }
            if (pg.state == PG_INCOMPLETE || role >= pg.cur_set.size())
            {
                if (it->second.buf)
                {
                    free(it->second.buf);
                }
                pg.peering_state->list_results.erase(it);
                it = pg.peering_state->list_results.begin();
            }
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
    }
    auto ps = pg.peering_state;
    for (int role = 0; role < pg.cur_set.size(); role++)
    {
        osd_num_t role_osd = pg.cur_set[role];
        if (!role_osd)
        {
            continue;
        }
        if (ps->list_ops.find(role_osd) != ps->list_ops.end() ||
            ps->list_results.find(role_osd) != ps->list_results.end())
        {
            continue;
        }
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
            op->bs_op->offset = pg.pg_num-1;
            op->bs_op->callback = [ps, op, role_osd](blockstore_op_t *bs_op)
            {
                if (op->bs_op->retval < 0)
                {
                    throw std::runtime_error("local OP_LIST failed");
                }
                printf(
                    "Got object list from OSD %lu (local): %d object versions (%lu of them stable)\n",
                    role_osd, bs_op->retval, bs_op->version
                );
                ps->list_results[role_osd] = {
                    .buf = (obj_ver_id*)op->bs_op->buf,
                    .total_count = (uint64_t)op->bs_op->retval,
                    .stable_count = op->bs_op->version,
                };
                ps->list_done++;
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
                    .list_pg = pg.pg_num,
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
                    "Got object list from OSD %lu: %ld object versions (%lu of them stable)\n",
                    role_osd, op->reply.hdr.retval, op->reply.sec_list.stable_count
                );
                ps->list_results[role_osd] = {
                    .buf = (obj_ver_id*)op->buf,
                    .total_count = (uint64_t)op->reply.hdr.retval,
                    .stable_count = op->reply.sec_list.stable_count,
                };
                // set op->buf to NULL so it doesn't get freed
                op->buf = NULL;
                ps->list_done++;
                ps->list_ops.erase(role_osd);
                delete op;
            };
            outbox_push(cl, op);
            ps->list_ops[role_osd] = op;
        }
    }
    ringloop->wakeup();
}

bool osd_t::stop_pg(pg_num_t pg_num)
{
    auto pg_it = pgs.find(pg_num);
    if (pg_it == pgs.end())
    {
        return false;
    }
    auto & pg = pg_it->second;
    if (!(pg.state & PG_ACTIVE))
    {
        return false;
    }
    pg.state = pg.state & ~PG_ACTIVE | PG_STOPPING;
    if (pg.inflight == 0)
    {
        finish_stop_pg(pg);
    }
    return true;
}

void osd_t::finish_stop_pg(pg_t & pg)
{
    pg.state = PG_OFFLINE;
}
