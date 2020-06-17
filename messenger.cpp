#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/tcp.h>

#include "messenger.h"

osd_op_t::~osd_op_t()
{
    assert(!bs_op);
    assert(!op_data);
    if (rmw_buf)
    {
        free(rmw_buf);
    }
    if (buf)
    {
        // Note: reusing osd_op_t WILL currently lead to memory leaks
        // So we don't reuse it, but free it every time
        free(buf);
    }
}

void osd_messenger_t::connect_peer(uint64_t peer_osd, json11::Json peer_state)
{
    if (wanted_peers.find(peer_osd) == wanted_peers.end())
    {
        wanted_peers[peer_osd] = (osd_wanted_peer_t){
            .address_list = peer_state["addresses"],
            .port = (int)peer_state["port"].int64_value(),
        };
    }
    else
    {
        wanted_peers[peer_osd].address_list = peer_state["addresses"];
        wanted_peers[peer_osd].port = (int)peer_state["port"].int64_value();
    }
    wanted_peers[peer_osd].address_changed = true;
    if (!wanted_peers[peer_osd].connecting &&
        (time(NULL) - wanted_peers[peer_osd].last_connect_attempt) >= peer_connect_interval)
    {
        try_connect_peer(peer_osd);
    }
}

void osd_messenger_t::try_connect_peer(uint64_t peer_osd)
{
    auto wp_it = wanted_peers.find(peer_osd);
    if (wp_it == wanted_peers.end())
    {
        return;
    }
    if (osd_peer_fds.find(peer_osd) != osd_peer_fds.end())
    {
        wanted_peers.erase(peer_osd);
        return;
    }
    auto & wp = wp_it->second;
    if (wp.address_index >= wp.address_list.array_items().size())
    {
        return;
    }
    wp.cur_addr = wp.address_list[wp.address_index].string_value();
    wp.cur_port = wp.port;
    try_connect_peer_addr(peer_osd, wp.cur_addr.c_str(), wp.cur_port);
}

void osd_messenger_t::try_connect_peer_addr(osd_num_t peer_osd, const char *peer_host, int peer_port)
{
    struct sockaddr_in addr;
    int r;
    if ((r = inet_pton(AF_INET, peer_host, &addr.sin_addr)) != 1)
    {
        on_connect_peer(peer_osd, -EINVAL);
        return;
    }
    addr.sin_family = AF_INET;
    addr.sin_port = htons(peer_port ? peer_port : 11203);
    int peer_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (peer_fd < 0)
    {
        on_connect_peer(peer_osd, -errno);
        return;
    }
    fcntl(peer_fd, F_SETFL, fcntl(peer_fd, F_GETFL, 0) | O_NONBLOCK);
    int timeout_id = -1;
    if (peer_connect_timeout > 0)
    {
        timeout_id = tfd->set_timer(1000*peer_connect_timeout, false, [this, peer_fd](int timer_id)
        {
            osd_num_t peer_osd = clients[peer_fd].osd_num;
            stop_client(peer_fd);
            on_connect_peer(peer_osd, -EIO);
            return;
        });
    }
    r = connect(peer_fd, (sockaddr*)&addr, sizeof(addr));
    if (r < 0 && errno != EINPROGRESS)
    {
        close(peer_fd);
        on_connect_peer(peer_osd, -errno);
        return;
    }
    assert(peer_osd != this->osd_num);
    clients[peer_fd] = (osd_client_t){
        .peer_addr = addr,
        .peer_port = peer_port,
        .peer_fd = peer_fd,
        .peer_state = PEER_CONNECTING,
        .connect_timeout_id = timeout_id,
        .osd_num = peer_osd,
        .in_buf = malloc(receive_buffer_size),
    };
    tfd->set_fd_handler(peer_fd, [this](int peer_fd, int epoll_events)
    {
        // Either OUT (connected) or HUP
        handle_connect_epoll(peer_fd);
    });
}

void osd_messenger_t::handle_connect_epoll(int peer_fd)
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
        stop_client(peer_fd);
        on_connect_peer(peer_osd, -result);
        return;
    }
    int one = 1;
    setsockopt(peer_fd, SOL_TCP, TCP_NODELAY, &one, sizeof(one));
    cl.peer_state = PEER_CONNECTED;
    // FIXME Disable EPOLLOUT on this fd
    tfd->set_fd_handler(peer_fd, [this](int peer_fd, int epoll_events)
    {
        handle_peer_epoll(peer_fd, epoll_events);
    });
    // Check OSD number
    check_peer_config(cl);
}

void osd_messenger_t::handle_peer_epoll(int peer_fd, int epoll_events)
{
    // Mark client as ready (i.e. some data is available)
    if (epoll_events & EPOLLRDHUP)
    {
        // Stop client
        printf("[OSD %lu] client %d disconnected\n", this->osd_num, peer_fd);
        stop_client(peer_fd);
    }
    else if (epoll_events & EPOLLIN)
    {
        // Mark client as ready (i.e. some data is available)
        auto & cl = clients[peer_fd];
        cl.read_ready++;
        if (cl.read_ready == 1)
        {
            read_ready_clients.push_back(cl.peer_fd);
            ringloop->wakeup();
        }
    }
}

void osd_messenger_t::on_connect_peer(osd_num_t peer_osd, int peer_fd)
{
    auto & wp = wanted_peers.at(peer_osd);
    wp.connecting = false;
    if (peer_fd < 0)
    {
        printf("Failed to connect to peer OSD %lu address %s port %d: %s\n", peer_osd, wp.cur_addr.c_str(), wp.cur_port, strerror(-peer_fd));
        if (wp.address_changed)
        {
            wp.address_changed = false;
            wp.address_index = 0;
            try_connect_peer(peer_osd);
        }
        else if (wp.address_index < wp.address_list.array_items().size()-1)
        {
            // Try other addresses
            wp.address_index++;
            try_connect_peer(peer_osd);
        }
        else
        {
            // Retry again in <peer_connect_interval> seconds
            wp.last_connect_attempt = time(NULL);
            wp.address_index = 0;
            tfd->set_timer(1000*peer_connect_interval, false, [this, peer_osd](int)
            {
                try_connect_peer(peer_osd);
            });
        }
        return;
    }
    printf("Connected with peer OSD %lu (fd %d)\n", peer_osd, peer_fd);
    wanted_peers.erase(peer_osd);
    repeer_pgs(peer_osd);
}

void osd_messenger_t::check_peer_config(osd_client_t & cl)
{
    osd_op_t *op = new osd_op_t();
    op->op_type = OSD_OP_OUT;
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
        osd_client_t & cl = clients[op->peer_fd];
        std::string json_err;
        json11::Json config;
        bool err = false;
        if (op->reply.hdr.retval < 0)
        {
            err = true;
            printf("Failed to get config from OSD %lu (retval=%ld), disconnecting peer\n", cl.osd_num, op->reply.hdr.retval);
        }
        else
        {
            config = json11::Json::parse(std::string((char*)op->buf), json_err);
            if (json_err != "")
            {
                err = true;
                printf("Failed to get config from OSD %lu: bad JSON: %s, disconnecting peer\n", cl.osd_num, json_err.c_str());
            }
            else if (config["osd_num"].uint64_value() != cl.osd_num)
            {
                err = true;
                printf("Connected to OSD %lu instead of OSD %lu, peer state is outdated, disconnecting peer\n", config["osd_num"].uint64_value(), cl.osd_num);
                on_connect_peer(cl.osd_num, -1);
            }
        }
        if (err)
        {
            stop_client(op->peer_fd);
            delete op;
            return;
        }
        osd_peer_fds[cl.osd_num] = cl.peer_fd;
        on_connect_peer(cl.osd_num, cl.peer_fd);
        delete op;
    };
    outbox_push(op);
}

void osd_messenger_t::cancel_osd_ops(osd_client_t & cl)
{
    for (auto p: cl.sent_ops)
    {
        cancel_op(p.second);
    }
    cl.sent_ops.clear();
    for (auto op: cl.outbox)
    {
        cancel_op(op);
    }
    cl.outbox.clear();
    if (cl.write_op)
    {
        cancel_op(cl.write_op);
        cl.write_op = NULL;
    }
}

void osd_messenger_t::cancel_op(osd_op_t *op)
{
    if (op->op_type == OSD_OP_OUT)
    {
        op->reply.hdr.magic = SECONDARY_OSD_REPLY_MAGIC;
        op->reply.hdr.id = op->req.hdr.id;
        op->reply.hdr.opcode = op->req.hdr.opcode;
        op->reply.hdr.retval = -EPIPE;
        // Copy lambda to be unaffected by `delete op`
        std::function<void(osd_op_t*)>(op->callback)(op);
    }
    else
    {
        // This function is only called in stop_client(), so it's fine to destroy the operation
        delete op;
    }
}

void osd_messenger_t::stop_client(int peer_fd)
{
    assert(peer_fd != 0);
    auto it = clients.find(peer_fd);
    if (it == clients.end())
    {
        return;
    }
    uint64_t repeer_osd = 0;
    osd_client_t cl = it->second;
    if (cl.peer_state == PEER_CONNECTED)
    {
        if (cl.osd_num)
        {
            // Reload configuration from etcd when the connection is dropped
            printf("[OSD %lu] Stopping client %d (OSD peer %lu)\n", osd_num, peer_fd, cl.osd_num);
            repeer_osd = cl.osd_num;
        }
        else
        {
            printf("[OSD %lu] Stopping client %d (regular client)\n", osd_num, peer_fd);
        }
    }
    clients.erase(it);
    tfd->set_fd_handler(peer_fd, NULL);
    if (cl.osd_num)
    {
        osd_peer_fds.erase(cl.osd_num);
        // Cancel outbound operations
        cancel_osd_ops(cl);
    }
    if (cl.read_op)
    {
        delete cl.read_op;
        cl.read_op = NULL;
    }
    for (auto rit = read_ready_clients.begin(); rit != read_ready_clients.end(); rit++)
    {
        if (*rit == peer_fd)
        {
            read_ready_clients.erase(rit);
            break;
        }
    }
    for (auto wit = write_ready_clients.begin(); wit != write_ready_clients.end(); wit++)
    {
        if (*wit == peer_fd)
        {
            write_ready_clients.erase(wit);
            break;
        }
    }
    free(cl.in_buf);
    close(peer_fd);
    if (repeer_osd)
    {
        repeer_pgs(repeer_osd);
    }
}

void osd_messenger_t::accept_connections(int listen_fd)
{
    // Accept new connections
    sockaddr_in addr;
    socklen_t peer_addr_size = sizeof(addr);
    int peer_fd;
    while ((peer_fd = accept(listen_fd, (sockaddr*)&addr, &peer_addr_size)) >= 0)
    {
        assert(peer_fd != 0);
        char peer_str[256];
        printf("[OSD %lu] new client %d: connection from %s port %d\n", this->osd_num, peer_fd,
            inet_ntop(AF_INET, &addr.sin_addr, peer_str, 256), ntohs(addr.sin_port));
        fcntl(peer_fd, F_SETFL, fcntl(peer_fd, F_GETFL, 0) | O_NONBLOCK);
        int one = 1;
        setsockopt(peer_fd, SOL_TCP, TCP_NODELAY, &one, sizeof(one));
        clients[peer_fd] = {
            .peer_addr = addr,
            .peer_port = ntohs(addr.sin_port),
            .peer_fd = peer_fd,
            .peer_state = PEER_CONNECTED,
            .in_buf = malloc(receive_buffer_size),
        };
        // Add FD to epoll
        tfd->set_fd_handler(peer_fd, [this](int peer_fd, int epoll_events)
        {
            handle_peer_epoll(peer_fd, epoll_events);
        });
        // Try to accept next connection
        peer_addr_size = sizeof(addr);
    }
    if (peer_fd == -1 && errno != EAGAIN)
    {
        throw std::runtime_error(std::string("accept: ") + strerror(errno));
    }
}
