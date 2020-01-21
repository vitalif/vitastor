#include <netinet/tcp.h>
#include <sys/epoll.h>
#include "osd.h"

void osd_t::init_primary()
{
    // Initial test version of clustering code requires exactly 2 peers
    if (config["peer1"] == "" || config["peer2"] == "")
        throw std::runtime_error("run_primary requires two peers");
    peers.push_back(parse_peer(config["peer1"]));
    peers.push_back(parse_peer(config["peer2"]));
    if (peers[1].osd_num == peers[0].osd_num)
        throw std::runtime_error("peer1 and peer2 osd numbers are the same");
    pgs.push_back((osd_pg_t){
        .state = PG_OFFLINE,
        .pg_num = 1,
        .target_set = { { .role = 1, .osd_num = 1 }, { .role = 2, .osd_num = 2 }, { .role = 3, .osd_num = 3 } },
        .object_map = spp::sparse_hash_map<object_id, int>(),
    });
    pg_count = 1;
    needs_peering = true;
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

void osd_t::connect_peer(unsigned osd_num, const char *peer_host, int peer_port, std::function<void(int)> callback)
{
    struct sockaddr_in addr;
    int r;
    if ((r = inet_pton(AF_INET, peer_host, &addr.sin_addr)) != 1)
    {
        callback(-EINVAL);
        return;
    }
    addr.sin_family = AF_INET;
    addr.sin_port = htons(peer_port ? peer_port : 11203);
    int peer_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (peer_fd < 0)
    {
        callback(-errno);
        return;
    }
    fcntl(peer_fd, F_SETFL, fcntl(peer_fd, F_GETFL, 0) | O_NONBLOCK);
    r = connect(peer_fd, (sockaddr*)&addr, sizeof(addr));
    if (r < 0 && errno != EINPROGRESS)
    {
        close(peer_fd);
        callback(-errno);
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
    std::function<void(int)> callback = cl.connect_callback;
    int result = 0;
    socklen_t result_len = sizeof(result);
    if (getsockopt(peer_fd, SOL_SOCKET, SO_ERROR, &result, &result_len) < 0)
    {
        result = errno;
    }
    if (result != 0)
    {
        stop_client(peer_fd);
        callback(-result);
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
    callback(peer_fd);
}

// Peering loop
// Ideally: Connect -> Ask & check config -> Start PG peering
void osd_t::handle_peers()
{
    if (needs_peering)
    {
        for (int i = 0; i < peers.size(); i++)
        {
            if (osd_peer_fds.find(peers[i].osd_num) == osd_peer_fds.end() &&
                time(NULL) - peers[i].last_connect_attempt > 5)
            {
                peers[i].last_connect_attempt = time(NULL);
                connect_peer(peers[i].osd_num, peers[i].addr.c_str(), peers[i].port, [this](int peer_fd)
                {
                    printf("Connected with peer OSD %lu (fd %d)\n", clients[peer_fd].osd_num, peer_fd);
                    // Restart PG peering
                    pgs[0].state = PG_PEERING;
                    pgs[0].acting_set_ids.clear();
                    pgs[0].acting_sets.clear();
                    pgs[0].object_map.clear();
                    if (pgs[0].peering_state)
                        delete pgs[0].peering_state;
                    ringloop->wakeup();
                });
            }
        }
    }
    for (int i = 0; i < pgs.size(); i++)
    {
        if (pgs[i].state == PG_PEERING)
        {
            if (!pgs[i].peering_state)
            {
                pgs[i].peering_state = new osd_pg_peering_state_t();
                
            }
        }
    }
}
