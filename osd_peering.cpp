#include <netinet/tcp.h>
#include <sys/epoll.h>

#include <algorithm>

#include "osd.h"

void osd_t::init_primary()
{
    // Initial test version of clustering code requires exactly 2 peers
    // FIXME Hardcode
    if (config["peer1"] == "" || config["peer2"] == "")
        throw std::runtime_error("run_primary requires two peers");
    peers.push_back(parse_peer(config["peer1"]));
    peers.push_back(parse_peer(config["peer2"]));
    if (peers[1].osd_num == peers[0].osd_num)
        throw std::runtime_error("peer1 and peer2 osd numbers are the same");
    pgs.push_back((pg_t){
        .state = PG_OFFLINE,
        .pg_cursize = 2, // or 3
        .pg_num = 1,
        .target_set = { 1, 0, 3 }, // or { 1, 2, 3 }
    });
    pg_count = 1;
    peering_state = 1;
}

osd_peer_def_t osd_t::parse_peer(std::string peer)
{
    // OSD_NUM:IP:PORT
    size_t pos1 = peer.find(':');
    size_t pos2 = peer.find(':', pos1+1);
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
    };
    osd_peer_fds[osd_num] = peer_fd;
    // Add FD to epoll (EPOLLOUT for tracking connect() result)
    epoll_event ev;
    ev.data.fd = peer_fd;
    ev.events = EPOLLOUT | EPOLLIN | EPOLLRDHUP;
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
    ev.events = EPOLLIN | EPOLLRDHUP;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_MOD, peer_fd, &ev) < 0)
    {
        throw std::runtime_error(std::string("epoll_ctl: ") + strerror(errno));
    }
    callback(osd_num, peer_fd);
}

// Peering loop
// Ideally: Connect -> Ask & check config -> Start PG peering
void osd_t::handle_peers()
{
    if (peering_state & 1)
    {
        for (int i = 0; i < peers.size(); i++)
        {
            if (osd_peer_fds.find(peers[i].osd_num) == osd_peer_fds.end() &&
                time(NULL) - peers[i].last_connect_attempt > 5)
            {
                peers[i].last_connect_attempt = time(NULL);
                connect_peer(peers[i].osd_num, peers[i].addr.c_str(), peers[i].port, [this](osd_num_t osd_num, int peer_fd)
                {
                    if (peer_fd < 0)
                    {
                        printf("Failed to connect to peer OSD %lu: %s\n", osd_num, strerror(-peer_fd));
                        return;
                    }
                    printf("Connected with peer OSD %lu (fd %d)\n", clients[peer_fd].osd_num, peer_fd);
                    int i;
                    for (i = 0; i < peers.size(); i++)
                    {
                        auto it = osd_peer_fds.find(peers[i].osd_num);
                        if (it == osd_peer_fds.end() || clients[it->second].peer_state != PEER_CONNECTED)
                        {
                            break;
                        }
                    }
                    if (i >= peers.size())
                    {
                        // Start PG peering
                        pgs[0].state = PG_PEERING;
                        pgs[0].state_dict.clear();
                        pgs[0].obj_states.clear();
                        pgs[0].ver_override.clear();
                        if (pgs[0].peering_state)
                            delete pgs[0].peering_state;
                        peering_state = 2;
                        ringloop->wakeup();
                    }
                });
            }
        }
    }
    if (peering_state & 2)
    {
        for (int i = 0; i < pgs.size(); i++)
        {
            if (pgs[i].state == PG_PEERING)
            {
                if (!pgs[i].peering_state)
                {
                    start_pg_peering(i);
                }
                else if (pgs[i].peering_state->list_done >= 3)
                {
                    pgs[i].calc_object_states();
                    peering_state = 0;
                }
            }
        }
    }
}

void osd_t::start_pg_peering(int pg_idx)
{
    // FIXME: Set PG_INCOMPLETE if incomplete
    auto & pg = pgs[pg_idx];
    auto ps = pg.peering_state = new pg_peering_state_t();
    {
        osd_num_t osd_num = this->osd_num;
        osd_op_t *op = new osd_op_t();
        op->op_type = 0;
        op->peer_fd = 0;
        op->bs_op.opcode = BS_OP_LIST;
        op->bs_op.callback = [ps, op, osd_num](blockstore_op_t *bs_op)
        {
            if (op->bs_op.retval < 0)
            {
                throw std::runtime_error("OP_LIST failed");
            }
            printf(
                "Got object list from OSD %lu (local): %d objects (%lu of them stable)\n",
                osd_num, bs_op->retval, bs_op->version
            );
            ps->list_results[osd_num] = {
                .buf = (obj_ver_id*)op->bs_op.buf,
                .total_count = (uint64_t)op->bs_op.retval,
                .stable_count = op->bs_op.version,
            };
            ps->list_done++;
            delete op;
        };
        bs->enqueue_op(&op->bs_op);
    }
    for (int i = 0; i < peers.size(); i++)
    {
        osd_num_t osd_num = peers[i].osd_num;
        auto & cl = clients[osd_peer_fds[peers[i].osd_num]];
        osd_op_t *op = new osd_op_t();
        op->op_type = OSD_OP_OUT;
        op->peer_fd = cl.peer_fd;
        op->op = {
            .sec_list = {
                .header = {
                    .magic = SECONDARY_OSD_OP_MAGIC,
                    .id = 1,
                    .opcode = OSD_OP_SECONDARY_LIST,
                },
                .pgnum = pg.pg_num,
                .pgtotal = pg_count,
            },
        };
        op->callback = [ps, osd_num](osd_op_t *op)
        {
            if (op->reply.hdr.retval < 0)
            {
                throw std::runtime_error("OP_LIST failed");
            }
            printf(
                "Got object list from OSD %lu: %ld objects (%lu of them stable)\n",
                osd_num, op->reply.hdr.retval, op->reply.sec_list.stable_count
            );
            ps->list_results[osd_num] = {
                .buf = (obj_ver_id*)op->buf,
                .total_count = (uint64_t)op->reply.hdr.retval,
                .stable_count = op->reply.sec_list.stable_count,
            };
            op->buf = NULL;
            ps->list_done++;
            delete op;
        };
        outbox_push(cl, op);
    }
}
