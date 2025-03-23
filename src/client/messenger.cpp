// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/epoll.h>
#include <netinet/tcp.h>
#include <stdexcept>

#include "addr_util.h"
#include "messenger.h"
#ifdef WITH_RDMA
#include "msgr_rdma.h"
#endif

#include <sys/poll.h>

msgr_iothread_t::msgr_iothread_t():
    ring(RINGLOOP_DEFAULT_SIZE, true),
    thread(&msgr_iothread_t::run, this)
{
    eventfd = ring.register_eventfd();
    if (eventfd < 0)
    {
        throw std::runtime_error(std::string("failed to register eventfd: ") + strerror(-eventfd));
    }
}

msgr_iothread_t::~msgr_iothread_t()
{
    stop();
}

void msgr_iothread_t::add_sqe(io_uring_sqe & sqe)
{
    mu.lock();
    queue.push_back((iothread_sqe_t){ .sqe = sqe, .data = std::move(*(ring_data_t*)sqe.user_data) });
    if (queue.size() == 1)
    {
        cond.notify_all();
    }
    mu.unlock();
}

void msgr_iothread_t::stop()
{
    mu.lock();
    if (stopped)
    {
        mu.unlock();
        return;
    }
    stopped = true;
    if (outer_loop_data)
    {
        outer_loop_data->callback = [](ring_data_t*){};
    }
    cond.notify_all();
    close(eventfd);
    mu.unlock();
    thread.join();
}

void msgr_iothread_t::add_to_ringloop(ring_loop_t *outer_loop)
{
    assert(!this->outer_loop || this->outer_loop == outer_loop);
    io_uring_sqe *sqe = outer_loop->get_sqe();
    assert(sqe != NULL);
    this->outer_loop = outer_loop;
    this->outer_loop_data = ((ring_data_t*)sqe->user_data);
    my_uring_prep_poll_add(sqe, eventfd, POLLIN);
    outer_loop_data->callback = [this](ring_data_t *data)
    {
        if (data->res < 0)
        {
            throw std::runtime_error(std::string("eventfd poll failed: ") + strerror(-data->res));
        }
        outer_loop_data = NULL;
        if (stopped)
        {
            return;
        }
        add_to_ringloop(this->outer_loop);
        ring.loop();
    };
}

void msgr_iothread_t::run()
{
    while (true)
    {
        {
            std::unique_lock<std::mutex> lk(mu);
            while (!stopped && !queue.size())
                cond.wait(lk);
            if (stopped)
                return;
            int i = 0;
            for (; i < queue.size(); i++)
            {
                io_uring_sqe *sqe = ring.get_sqe();
                if (!sqe)
                    break;
                ring_data_t *data = ((ring_data_t*)sqe->user_data);
                *data = std::move(queue[i].data);
                *sqe = queue[i].sqe;
                sqe->user_data = (uint64_t)data;
            }
            queue.erase(queue.begin(), queue.begin()+i);
        }
        // We only want to offload sendmsg/recvmsg. Callbacks will be called in main thread
        ring.submit();
    }
}

void osd_messenger_t::init()
{
#ifdef WITH_RDMA
    if (use_rdma)
    {
        rdma_context = msgr_rdma_context_t::create(
            osd_networks, rdma_device != "" ? rdma_device.c_str() : NULL,
            rdma_port_num, rdma_gid_index, rdma_mtu, rdma_odp, log_level
        );
        if (!rdma_context)
        {
            if (log_level > 0)
                fprintf(stderr, "[OSD %ju] Couldn't initialize RDMA, proceeding with TCP only\n", osd_num);
        }
        else
        {
            rdma_max_sge = rdma_max_sge < rdma_context->attrx.orig_attr.max_sge
                ? rdma_max_sge : rdma_context->attrx.orig_attr.max_sge;
            fprintf(stderr, "[OSD %ju] RDMA initialized successfully\n", osd_num);
            fcntl(rdma_context->channel->fd, F_SETFL, fcntl(rdma_context->channel->fd, F_GETFL, 0) | O_NONBLOCK);
            tfd->set_fd_handler(rdma_context->channel->fd, false, [this](int notify_fd, int epoll_events)
            {
                handle_rdma_events();
            });
            handle_rdma_events();
        }
    }
#endif
    if (ringloop && iothread_count > 0)
    {
        for (int i = 0; i < iothread_count; i++)
        {
            auto iot = new msgr_iothread_t();
            iothreads.push_back(iot);
            iot->add_to_ringloop(ringloop);
        }
    }
    keepalive_timer_id = tfd->set_timer(1000, true, [this](int)
    {
        auto cl_it = clients.begin();
        while (cl_it != clients.end())
        {
            auto cl = cl_it->second;
            cl_it++;
            auto peer_fd = cl->peer_fd;
            if (!cl->osd_num || cl->peer_state != PEER_CONNECTED && cl->peer_state != PEER_RDMA)
            {
                // Do not run keepalive on regular clients
                continue;
            }
            if (cl->ping_time_remaining > 0)
            {
                cl->ping_time_remaining--;
                if (!cl->ping_time_remaining)
                {
                    // Ping timed out, stop the client
                    fprintf(stderr, "Ping timed out for OSD %ju (client %d), disconnecting peer\n", cl->osd_num, cl->peer_fd);
                    stop_client(peer_fd, true);
                    // Restart iterator because it may be invalidated
                    cl_it = clients.upper_bound(peer_fd);
                }
            }
            else if (cl->idle_time_remaining > 0)
            {
                cl->idle_time_remaining--;
                if (!cl->idle_time_remaining)
                {
                    // Connection is idle for <osd_idle_time>, send ping
                    osd_op_t *op = new osd_op_t();
                    op->op_type = OSD_OP_OUT;
                    op->peer_fd = cl->peer_fd;
                    op->req = (osd_any_op_t){
                        .hdr = {
                            .magic = SECONDARY_OSD_OP_MAGIC,
                            .id = this->next_subop_id++,
                            .opcode = OSD_OP_PING,
                        },
                    };
                    op->callback = [this, cl](osd_op_t *op)
                    {
                        auto cl_it = clients.find(op->peer_fd);
                        if (cl_it == clients.end() || cl_it->second != cl)
                        {
                            // client is already dropped
                            delete op;
                            return;
                        }
                        int fail_fd = (op->reply.hdr.retval != 0 ? op->peer_fd : -1);
                        auto fail_osd_num = cl->osd_num;
                        cl->ping_time_remaining = 0;
                        delete op;
                        if (fail_fd >= 0)
                        {
                            fprintf(stderr, "Ping failed for OSD %ju (client %d), disconnecting peer\n", fail_osd_num, fail_fd);
                            stop_client(fail_fd, true);
                        }
                    };
                    cl->ping_time_remaining = osd_ping_timeout;
                    cl->idle_time_remaining = osd_idle_timeout;
                    outbox_push(op);
                    // Restart iterator because it may be invalidated
                    cl_it = clients.upper_bound(peer_fd);
                }
            }
            else
            {
                cl->idle_time_remaining = osd_idle_timeout;
            }
        }
    });
}

osd_messenger_t::~osd_messenger_t()
{
    if (keepalive_timer_id >= 0)
    {
        tfd->clear_timer(keepalive_timer_id);
        keepalive_timer_id = -1;
    }
    while (clients.size() > 0)
    {
        stop_client(clients.begin()->first, true, true);
    }
    if (iothreads.size())
    {
        for (auto iot: iothreads)
        {
            delete iot;
        }
        iothreads.clear();
    }
#ifdef WITH_RDMA
    if (rdma_context)
    {
        delete rdma_context;
    }
#endif
}

void osd_messenger_t::parse_config(const json11::Json & config)
{
#ifdef WITH_RDMA
    if (!config["use_rdma"].is_null())
    {
        // RDMA is on by default in RDMA-enabled builds
        this->use_rdma = config["use_rdma"].bool_value() || config["use_rdma"].uint64_value() != 0;
    }
    this->rdma_device = config["rdma_device"].string_value();
    this->rdma_port_num = (uint8_t)config["rdma_port_num"].uint64_value();
    if (!this->rdma_port_num)
        this->rdma_port_num = 1;
    if (!config["rdma_gid_index"].is_null())
        this->rdma_gid_index = (uint8_t)config["rdma_gid_index"].uint64_value();
    this->rdma_mtu = (uint32_t)config["rdma_mtu"].uint64_value();
    this->rdma_max_sge = config["rdma_max_sge"].uint64_value();
    if (!this->rdma_max_sge)
        this->rdma_max_sge = 128;
    this->rdma_max_send = config["rdma_max_send"].uint64_value();
    if (!this->rdma_max_send)
        this->rdma_max_send = 8;
    this->rdma_max_recv = config["rdma_max_recv"].uint64_value();
    if (!this->rdma_max_recv)
        this->rdma_max_recv = 16;
    this->rdma_max_msg = config["rdma_max_msg"].uint64_value();
    if (!this->rdma_max_msg || this->rdma_max_msg > 128*1024*1024)
        this->rdma_max_msg = 129*1024;
    this->rdma_odp = config["rdma_odp"].bool_value();
    std::vector<std::string> mask;
    if (config["bind_address"].is_string())
        mask.push_back(config["bind_address"].string_value());
    else if (config["osd_network"].is_string())
        mask.push_back(config["osd_network"].string_value());
    else
        for (auto v: config["osd_network"].array_items())
            mask.push_back(v.string_value());
    this->osd_networks = mask;
#endif
    if (!osd_num)
        this->iothread_count = (uint32_t)config["client_iothread_count"].uint64_value();
    else
        this->iothread_count = (uint32_t)config["osd_iothread_count"].uint64_value();
    this->receive_buffer_size = (uint32_t)config["tcp_header_buffer_size"].uint64_value();
    if (!this->receive_buffer_size || this->receive_buffer_size > 1024*1024*1024)
        this->receive_buffer_size = 65536;
    this->use_sync_send_recv = config["use_sync_send_recv"].bool_value() ||
        config["use_sync_send_recv"].uint64_value();
    this->peer_connect_interval = config["peer_connect_interval"].uint64_value();
    if (!this->peer_connect_interval)
        this->peer_connect_interval = 5;
    this->peer_connect_timeout = config["peer_connect_timeout"].uint64_value();
    if (!this->peer_connect_timeout)
        this->peer_connect_timeout = 5;
    this->osd_idle_timeout = config["osd_idle_timeout"].uint64_value();
    if (!this->osd_idle_timeout)
        this->osd_idle_timeout = 5;
    this->osd_ping_timeout = config["osd_ping_timeout"].uint64_value();
    if (!this->osd_ping_timeout)
        this->osd_ping_timeout = 5;
    this->log_level = config["log_level"].uint64_value();
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
    try_connect_peer(peer_osd);
}

void osd_messenger_t::try_connect_peer(uint64_t peer_osd)
{
    auto wp_it = wanted_peers.find(peer_osd);
    if (wp_it == wanted_peers.end() || wp_it->second.connecting ||
        (time(NULL) - wp_it->second.last_connect_attempt) < peer_connect_interval)
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
    wp.connecting = true;
    try_connect_peer_addr(peer_osd, wp.cur_addr.c_str(), wp.cur_port);
}

void osd_messenger_t::try_connect_peer_addr(osd_num_t peer_osd, const char *peer_host, int peer_port)
{
    assert(peer_osd != this->osd_num);
    struct sockaddr_storage addr;
    if (!string_to_addr(peer_host, 0, peer_port, &addr))
    {
        on_connect_peer(peer_osd, -EINVAL);
        return;
    }
    int peer_fd = socket(addr.ss_family, SOCK_STREAM, 0);
    if (peer_fd < 0)
    {
        on_connect_peer(peer_osd, -errno);
        return;
    }
    fcntl(peer_fd, F_SETFL, fcntl(peer_fd, F_GETFL, 0) | O_NONBLOCK);
    int r = connect(peer_fd, (sockaddr*)&addr, sizeof(addr));
    if (r < 0 && errno != EINPROGRESS)
    {
        close(peer_fd);
        on_connect_peer(peer_osd, -errno);
        return;
    }
    clients[peer_fd] = new osd_client_t();
    if (log_level > 0)
    {
        fprintf(stderr, "Connecting to OSD %ju at %s:%d (client %d)\n", peer_osd, peer_host, peer_port, peer_fd);
    }
    clients[peer_fd]->peer_addr = addr;
    clients[peer_fd]->peer_port = peer_port;
    clients[peer_fd]->peer_fd = peer_fd;
    clients[peer_fd]->peer_state = PEER_CONNECTING;
    clients[peer_fd]->connect_timeout_id = -1;
    clients[peer_fd]->osd_num = peer_osd;
    clients[peer_fd]->in_buf = malloc_or_die(receive_buffer_size);
    tfd->set_fd_handler(peer_fd, true, [this](int peer_fd, int epoll_events)
    {
        // Either OUT (connected) or HUP
        handle_connect_epoll(peer_fd);
    });
    if (peer_connect_timeout > 0)
    {
        clients[peer_fd]->connect_timeout_id = tfd->set_timer(1000*peer_connect_timeout, false, [this, peer_fd](int timer_id)
        {
            osd_num_t peer_osd = clients.at(peer_fd)->osd_num;
            stop_client(peer_fd, true);
            on_connect_peer(peer_osd, -EPIPE);
            return;
        });
    }
}

void osd_messenger_t::handle_connect_epoll(int peer_fd)
{
    auto cl = clients[peer_fd];
    if (cl->connect_timeout_id >= 0)
    {
        tfd->clear_timer(cl->connect_timeout_id);
        cl->connect_timeout_id = -1;
    }
    osd_num_t peer_osd = cl->osd_num;
    int result = 0;
    socklen_t result_len = sizeof(result);
    if (getsockopt(peer_fd, SOL_SOCKET, SO_ERROR, &result, &result_len) < 0)
    {
        result = errno;
    }
    if (result != 0)
    {
        stop_client(peer_fd, true);
        on_connect_peer(peer_osd, -result);
        return;
    }
    int one = 1;
    setsockopt(peer_fd, SOL_TCP, TCP_NODELAY, &one, sizeof(one));
    cl->peer_state = PEER_CONNECTED;
    tfd->set_fd_handler(peer_fd, false, [this](int peer_fd, int epoll_events)
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
        if (log_level > 0)
        {
            fprintf(stderr, "[OSD %ju] client %d disconnected\n", this->osd_num, peer_fd);
        }
        stop_client(peer_fd, true);
    }
    else if (epoll_events & EPOLLIN)
    {
        // Mark client as ready (i.e. some data is available)
        auto cl = clients[peer_fd];
        cl->read_ready++;
        if (cl->read_ready == 1)
        {
            read_ready_clients.push_back(cl->peer_fd);
            if (ringloop)
                ringloop->wakeup();
            else
                read_requests();
        }
    }
}

void osd_messenger_t::on_connect_peer(osd_num_t peer_osd, int peer_fd)
{
    auto & wp = wanted_peers.at(peer_osd);
    wp.connecting = false;
    if (peer_fd < 0)
    {
        fprintf(stderr, "Failed to connect to peer OSD %ju address %s port %d: %s\n", peer_osd, wp.cur_addr.c_str(), wp.cur_port, strerror(-peer_fd));
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
    if (log_level > 0)
    {
        fprintf(stderr, "[OSD %ju] Connected with peer OSD %ju (client %d)\n", osd_num, peer_osd, peer_fd);
    }
    wanted_peers.erase(peer_osd);
    repeer_pgs(peer_osd);
}

void osd_messenger_t::check_peer_config(osd_client_t *cl)
{
    osd_op_t *op = new osd_op_t();
    op->op_type = OSD_OP_OUT;
    op->peer_fd = cl->peer_fd;
    op->req = (osd_any_op_t){
        .show_conf = {
            .header = {
                .magic = SECONDARY_OSD_OP_MAGIC,
                .id = this->next_subop_id++,
                .opcode = OSD_OP_SHOW_CONFIG,
            },
        },
    };
#ifdef WITH_RDMA
    if (rdma_context)
    {
        cl->rdma_conn = msgr_rdma_connection_t::create(rdma_context, rdma_max_send, rdma_max_recv, rdma_max_sge, rdma_max_msg);
        if (cl->rdma_conn)
        {
            json11::Json payload = json11::Json::object {
                { "connect_rdma", cl->rdma_conn->addr.to_string() },
                { "rdma_max_msg", cl->rdma_conn->max_msg },
            };
            std::string payload_str = payload.dump();
            op->req.show_conf.json_len = payload_str.size();
            op->buf = malloc_or_die(payload_str.size());
            op->iov.push_back(op->buf, payload_str.size());
            memcpy(op->buf, payload_str.c_str(), payload_str.size());
        }
    }
#endif
    op->callback = [this, cl](osd_op_t *op)
    {
        std::string json_err;
        json11::Json config;
        bool err = false;
        if (op->reply.hdr.retval < 0)
        {
            err = true;
            fprintf(stderr, "Failed to get config from OSD %ju (retval=%jd), disconnecting peer\n", cl->osd_num, op->reply.hdr.retval);
        }
        else
        {
            config = json11::Json::parse(std::string((char*)op->buf), json_err);
            if (json_err != "")
            {
                err = true;
                fprintf(stderr, "Failed to get config from OSD %ju: bad JSON: %s, disconnecting peer\n", cl->osd_num, json_err.c_str());
            }
            else if (config["osd_num"].uint64_value() != cl->osd_num)
            {
                err = true;
                fprintf(stderr, "Connected to OSD %ju instead of OSD %ju, peer state is outdated, disconnecting peer\n", config["osd_num"].uint64_value(), cl->osd_num);
            }
            else if (config["protocol_version"].uint64_value() != OSD_PROTOCOL_VERSION)
            {
                err = true;
                fprintf(
                    stderr, "OSD %ju protocol version is %ju, but only version %u is supported.\n"
                    " If you need to upgrade from 0.5.x please request it via the issue tracker.\n",
                    cl->osd_num, config["protocol_version"].uint64_value(), OSD_PROTOCOL_VERSION
                );
            }
            if (check_config_hook)
            {
                err = !check_config_hook(cl, config);
            }
        }
        if (err)
        {
            osd_num_t peer_osd = cl->osd_num;
            stop_client(op->peer_fd);
            on_connect_peer(peer_osd, -EINVAL);
            delete op;
            return;
        }
#ifdef WITH_RDMA
        if (cl->rdma_conn && config["rdma_address"].is_string())
        {
            msgr_rdma_address_t addr;
            if (!msgr_rdma_address_t::from_string(config["rdma_address"].string_value().c_str(), &addr) ||
                cl->rdma_conn->connect(&addr) != 0)
            {
                fprintf(
                    stderr, "Failed to connect to OSD %ju (address %s) using RDMA, switching back to TCP\n",
                    cl->osd_num, config["rdma_address"].string_value().c_str()
                );
                delete cl->rdma_conn;
                cl->rdma_conn = NULL;
            }
            else
            {
                uint64_t server_max_msg = config["rdma_max_msg"].uint64_value();
                if (cl->rdma_conn->max_msg > server_max_msg)
                {
                    cl->rdma_conn->max_msg = server_max_msg;
                }
                if (log_level > 0)
                {
                    fprintf(stderr, "Connected to OSD %ju using RDMA\n", cl->osd_num);
                }
                cl->peer_state = PEER_RDMA;
                tfd->set_fd_handler(cl->peer_fd, false, [this](int peer_fd, int epoll_events)
                {
                    // Do not miss the disconnection!
                    if (epoll_events & EPOLLRDHUP)
                    {
                        handle_peer_epoll(peer_fd, epoll_events);
                    }
                });
                // Add the initial receive request
                try_recv_rdma(cl);
            }
        }
#endif
        osd_peer_fds[cl->osd_num] = cl->peer_fd;
        on_connect_peer(cl->osd_num, cl->peer_fd);
        delete op;
    };
    outbox_push(op);
}

void osd_messenger_t::accept_connections(int listen_fd)
{
    // Accept new connections
    sockaddr_storage addr;
    socklen_t peer_addr_size = sizeof(addr);
    int peer_fd;
    while ((peer_fd = accept(listen_fd, (sockaddr*)&addr, &peer_addr_size)) >= 0)
    {
        assert(peer_fd != 0);
        fprintf(stderr, "[OSD %ju] new client %d: connection from %s\n", this->osd_num, peer_fd,
            addr_to_string(addr).c_str());
        fcntl(peer_fd, F_SETFL, fcntl(peer_fd, F_GETFL, 0) | O_NONBLOCK);
        int one = 1;
        setsockopt(peer_fd, SOL_TCP, TCP_NODELAY, &one, sizeof(one));
        clients[peer_fd] = new osd_client_t();
        clients[peer_fd]->peer_addr = addr;
        clients[peer_fd]->peer_port = ntohs(((sockaddr_in*)&addr)->sin_port);
        clients[peer_fd]->peer_fd = peer_fd;
        clients[peer_fd]->peer_state = PEER_CONNECTED;
        clients[peer_fd]->in_buf = malloc_or_die(receive_buffer_size);
        // Add FD to epoll
        tfd->set_fd_handler(peer_fd, false, [this](int peer_fd, int epoll_events)
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

#ifdef WITH_RDMA
bool osd_messenger_t::is_rdma_enabled()
{
    return rdma_context != NULL;
}
#endif

json11::Json::object osd_messenger_t::read_config(const json11::Json & config)
{
    json11::Json::object file_config;
    const char *config_path = config["config_path"].string_value() != ""
        ? config["config_path"].string_value().c_str() : VITASTOR_CONFIG_PATH;
    int fd = open(config_path, O_RDONLY);
    if (fd < 0)
    {
        if (errno != ENOENT)
            fprintf(stderr, "Error reading %s: %s\n", config_path, strerror(errno));
        return file_config;
    }
    struct stat st;
    if (fstat(fd, &st) != 0)
    {
        fprintf(stderr, "Error reading %s: %s\n", config_path, strerror(errno));
        close(fd);
        return file_config;
    }
    std::string buf;
    buf.resize(st.st_size);
    int done = 0;
    while (done < st.st_size)
    {
        int r = read(fd, (uint8_t*)buf.data()+done, st.st_size-done);
        if (r < 0)
        {
            fprintf(stderr, "Error reading %s: %s\n", config_path, strerror(errno));
            close(fd);
            return file_config;
        }
        done += r;
    }
    close(fd);
    std::string json_err;
    file_config = json11::Json::parse(buf, json_err).object_items();
    if (json_err != "")
    {
        fprintf(stderr, "Invalid JSON in %s: %s\n", config_path, json_err.c_str());
    }
    return file_config;
}

static const char* cli_only_params[] = {
    // The list has to be sorted
    "bitmap_granularity",
    "block_size",
    "data_device",
    "data_offset",
    "data_size",
    "disable_data_fsync",
    "disable_device_lock",
    "disable_journal_fsync",
    "disable_meta_fsync",
    "disk_alignment",
    "flush_journal",
    "immediate_commit",
    "inmemory_journal",
    "inmemory_metadata",
    "journal_block_size",
    "journal_device",
    "journal_no_same_sector_overwrites",
    "journal_offset",
    "journal_sector_buffer_count",
    "journal_size",
    "meta_block_size",
    "meta_buf_size",
    "meta_device",
    "meta_offset",
    "osd_num",
    "readonly",
};

static const char **cli_only_end = cli_only_params + (sizeof(cli_only_params)/sizeof(cli_only_params[0]));

static const char* local_only_params[] = {
    // The list has to be sorted
    "config_path",
    "rdma_device",
    "rdma_gid_index",
    "rdma_max_msg",
    "rdma_max_recv",
    "rdma_max_send",
    "rdma_max_sge",
    "rdma_mtu",
    "rdma_port_num",
    "tcp_header_buffer_size",
    "use_rdma",
    "use_sync_send_recv",
};

static const char **local_only_end = local_only_params + (sizeof(local_only_params)/sizeof(local_only_params[0]));

// Basically could be replaced by std::lower_bound()...
static int find_str_array(const char **start, const char **end, const std::string & s)
{
    int min = 0, max = end-start;
    while (max-min >= 2)
    {
        int mid = (min+max)/2;
        int r = strcmp(s.c_str(), start[mid]);
        if (r < 0)
            max = mid;
        else if (r > 0)
            min = mid;
        else
            return mid;
    }
    if (min < end-start && !strcmp(s.c_str(), start[min]))
        return min;
    return -1;
}

json11::Json::object osd_messenger_t::merge_configs(const json11::Json::object & cli_config,
    const json11::Json::object & file_config,
    const json11::Json::object & etcd_global_config,
    const json11::Json::object & etcd_osd_config)
{
    // Priority: most important -> less important:
    // etcd_osd_config -> cli_config -> etcd_global_config -> file_config
    json11::Json::object res = file_config;
    for (auto & kv: file_config)
    {
        int cli_only = find_str_array(cli_only_params, cli_only_end, kv.first);
        if (cli_only < 0)
        {
            res[kv.first] = kv.second;
        }
    }
    for (auto & kv: etcd_global_config)
    {
        int local_only = find_str_array(local_only_params, local_only_end, kv.first);
        if (local_only < 0)
        {
            res[kv.first] = kv.second;
        }
    }
    for (auto & kv: cli_config)
    {
        res[kv.first] = kv.second;
    }
    for (auto & kv: etcd_osd_config)
    {
        int local_only = find_str_array(local_only_params, local_only_end, kv.first);
        if (local_only < 0)
        {
            res[kv.first] = kv.second;
        }
    }
    return res;
}
