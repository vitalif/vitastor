#include <sys/socket.h>
#include <sys/epoll.h>
#include <sys/poll.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

#include "osd.h"

static const char* osd_op_names[] = {
    "",
    "read",
    "write",
    "sync",
    "stabilize",
    "rollback",
    "delete",
    "sync_stab_all",
    "list",
    "show_config",
    "primary_read",
    "primary_write",
    "primary_sync",
};

osd_t::osd_t(blockstore_config_t & config, blockstore_t *bs, ring_loop_t *ringloop)
{
    this->config = config;
    this->bs = bs;
    this->ringloop = ringloop;
    this->tick_tfd = new timerfd_interval(ringloop, 3, [this]()
    {
        for (int i = 0; i <= OSD_OP_MAX; i++)
        {
            if (op_stat_count[i] != 0)
            {
                printf("avg latency for op %d (%s): %ld us\n", i, osd_op_names[i], op_stat_sum[i]/op_stat_count[i]);
                op_stat_count[i] = 0;
                op_stat_sum[i] = 0;
            }
        }
        for (int i = 0; i <= OSD_OP_MAX; i++)
        {
            if (subop_stat_count[i] != 0)
            {
                printf("avg latency for subop %d (%s): %ld us\n", i, osd_op_names[i], subop_stat_sum[i]/subop_stat_count[i]);
                subop_stat_count[i] = 0;
                subop_stat_sum[i] = 0;
            }
        }
        if (send_stat_count != 0)
        {
            printf("avg latency to send stabilize subop: %ld us\n", send_stat_sum/send_stat_count);
            send_stat_count = 0;
            send_stat_sum = 0;
        }
    });
    this->bs_block_size = bs->get_block_size();
    // FIXME: use bitmap granularity instead
    this->bs_disk_alignment = bs->get_disk_alignment();

    bind_address = config["bind_address"];
    if (bind_address == "")
        bind_address = "0.0.0.0";
    bind_port = strtoull(config["bind_port"].c_str(), NULL, 10);
    if (!bind_port || bind_port > 65535)
        bind_port = 11203;
    osd_num = strtoull(config["osd_num"].c_str(), NULL, 10);
    if (!osd_num)
        throw std::runtime_error("osd_num is required in the configuration");
    if (config["immediate_commit"] == "all")
        immediate_commit = IMMEDIATE_ALL;
    else if (config["immediate_commit"] == "small")
        immediate_commit = IMMEDIATE_SMALL;
    run_primary = config["run_primary"] == "true" || config["run_primary"] == "1" || config["run_primary"] == "yes";
    if (run_primary)
        init_primary();

    listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0)
    {
        throw std::runtime_error(std::string("socket: ") + strerror(errno));
    }
    int enable = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable));

    sockaddr_in addr;
    int r;
    if ((r = inet_pton(AF_INET, bind_address.c_str(), &addr.sin_addr)) != 1)
    {
        close(listen_fd);
        throw std::runtime_error("bind address "+bind_address+(r == 0 ? " is not valid" : ": no ipv4 support"));
    }
    addr.sin_family = AF_INET;
    addr.sin_port = htons(bind_port);

    if (bind(listen_fd, (sockaddr*)&addr, sizeof(addr)) < 0)
    {
        close(listen_fd);
        throw std::runtime_error(std::string("bind: ") + strerror(errno));
    }

    if (listen(listen_fd, listen_backlog) < 0)
    {
        close(listen_fd);
        throw std::runtime_error(std::string("listen: ") + strerror(errno));
    }

    fcntl(listen_fd, F_SETFL, fcntl(listen_fd, F_GETFL, 0) | O_NONBLOCK);

    epoll_fd = epoll_create(1);
    if (epoll_fd < 0)
    {
        close(listen_fd);
        throw std::runtime_error(std::string("epoll_create: ") + strerror(errno));
    }

    epoll_event ev;
    ev.data.fd = listen_fd;
    ev.events = EPOLLIN | EPOLLET;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &ev) < 0)
    {
        close(listen_fd);
        close(epoll_fd);
        throw std::runtime_error(std::string("epoll_ctl: ") + strerror(errno));
    }

    consumer.loop = [this]() { loop(); };
    ringloop->register_consumer(&consumer);
}

osd_t::~osd_t()
{
    delete tick_tfd;
    ringloop->unregister_consumer(&consumer);
    close(epoll_fd);
    close(listen_fd);
}

osd_op_t::~osd_op_t()
{
    if (bs_op)
    {
        delete bs_op;
    }
    if (op_data)
    {
        free(op_data);
    }
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

bool osd_t::shutdown()
{
    stopping = true;
    if (inflight_ops > 0)
    {
        return false;
    }
    return bs->is_safe_to_stop();
}

void osd_t::loop()
{
    if (!wait_state)
    {
        handle_epoll_events();
        wait_state = 1;
    }
    handle_peers();
    read_requests();
    send_replies();
    ringloop->submit();
}

void osd_t::handle_epoll_events()
{
    io_uring_sqe *sqe = ringloop->get_sqe();
    if (!sqe)
    {
        throw std::runtime_error("can't get SQE, will fall out of sync with EPOLLET");
    }
    ring_data_t *data = ((ring_data_t*)sqe->user_data);
    my_uring_prep_poll_add(sqe, epoll_fd, POLLIN);
    data->callback = [this](ring_data_t *data)
    {
        if (data->res < 0)
        {
            throw std::runtime_error(std::string("epoll failed: ") + strerror(-data->res));
        }
        handle_epoll_events();
    };
    ringloop->submit();
    int nfds;
    epoll_event events[MAX_EPOLL_EVENTS];
restart:
    nfds = epoll_wait(epoll_fd, events, MAX_EPOLL_EVENTS, 0);
    for (int i = 0; i < nfds; i++)
    {
        if (events[i].data.fd == listen_fd)
        {
            // Accept new connections
            sockaddr_in addr;
            socklen_t peer_addr_size = sizeof(addr);
            int peer_fd;
            while ((peer_fd = accept(listen_fd, (sockaddr*)&addr, &peer_addr_size)) >= 0)
            {
                char peer_str[256];
                printf("osd: new client %d: connection from %s port %d\n", peer_fd, inet_ntop(AF_INET, &addr.sin_addr, peer_str, 256), ntohs(addr.sin_port));
                fcntl(peer_fd, F_SETFL, fcntl(listen_fd, F_GETFL, 0) | O_NONBLOCK);
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
                epoll_event ev;
                ev.data.fd = peer_fd;
                ev.events = EPOLLIN | EPOLLET | EPOLLRDHUP;
                if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, peer_fd, &ev) < 0)
                {
                    throw std::runtime_error(std::string("epoll_ctl: ") + strerror(errno));
                }
                // Try to accept next connection
                peer_addr_size = sizeof(addr);
            }
            if (peer_fd == -1 && errno != EAGAIN)
            {
                throw std::runtime_error(std::string("accept: ") + strerror(errno));
            }
        }
        else
        {
            auto & cl = clients[events[i].data.fd];
            if (cl.peer_state == PEER_CONNECTING)
            {
                // Either OUT (connected) or HUP
                handle_connect_result(cl.peer_fd);
            }
            else if (events[i].events & EPOLLRDHUP)
            {
                // Stop client
                printf("osd: client %d disconnected\n", cl.peer_fd);
                stop_client(cl.peer_fd);
            }
            else
            {
                // Mark client as ready (i.e. some data is available)
                cl.read_ready++;
                if (cl.read_ready == 1)
                {
                    read_ready_clients.push_back(cl.peer_fd);
                    ringloop->wakeup();
                }
            }
        }
    }
    if (nfds == MAX_EPOLL_EVENTS)
    {
        goto restart;
    }
}

void osd_t::cancel_osd_ops(osd_client_t & cl)
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

void osd_t::cancel_op(osd_op_t *op)
{
    if (op->op_type == OSD_OP_OUT)
    {
        op->reply.hdr.magic = SECONDARY_OSD_REPLY_MAGIC;
        op->reply.hdr.id = op->req.hdr.id;
        op->reply.hdr.opcode = op->req.hdr.opcode;
        op->reply.hdr.retval = -EPIPE;
        op->callback(op);
    }
    else
    {
        delete op;
    }
}

void osd_t::stop_client(int peer_fd)
{
    auto it = clients.find(peer_fd);
    if (it == clients.end())
    {
        return;
    }
    auto & cl = it->second;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_DEL, peer_fd, NULL) < 0)
    {
        throw std::runtime_error(std::string("epoll_ctl: ") + strerror(errno));
    }
    if (cl.osd_num)
    {
        // Cancel outbound operations
        cancel_osd_ops(cl);
        osd_peer_fds.erase(cl.osd_num);
        repeer_pgs(cl.osd_num, false);
        peering_state |= OSD_CONNECTING_PEERS;
    }
    if (cl.read_op)
    {
        delete cl.read_op;
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
    clients.erase(it);
    close(peer_fd);
}

void osd_t::exec_op(osd_op_t *cur_op)
{
    clock_gettime(CLOCK_REALTIME, &cur_op->tv_begin);
    if (stopping)
    {
        // Throw operation away
        delete cur_op;
        return;
    }
    cur_op->send_list.push_back(cur_op->reply.buf, OSD_PACKET_SIZE);
    if (cur_op->req.hdr.magic != SECONDARY_OSD_OP_MAGIC ||
        cur_op->req.hdr.opcode < OSD_OP_MIN || cur_op->req.hdr.opcode > OSD_OP_MAX ||
        (cur_op->req.hdr.opcode == OSD_OP_SECONDARY_READ || cur_op->req.hdr.opcode == OSD_OP_SECONDARY_WRITE) &&
        (cur_op->req.sec_rw.len > OSD_RW_MAX || cur_op->req.sec_rw.len % OSD_RW_ALIGN || cur_op->req.sec_rw.offset % OSD_RW_ALIGN) ||
        (cur_op->req.hdr.opcode == OSD_OP_READ || cur_op->req.hdr.opcode == OSD_OP_WRITE) &&
        (cur_op->req.rw.len > OSD_RW_MAX || cur_op->req.rw.len % OSD_RW_ALIGN || cur_op->req.rw.offset % OSD_RW_ALIGN))
    {
        // Bad command
        cur_op->reply.hdr.magic = SECONDARY_OSD_REPLY_MAGIC;
        cur_op->reply.hdr.id = cur_op->req.hdr.id;
        cur_op->reply.hdr.opcode = cur_op->req.hdr.opcode;
        cur_op->reply.hdr.retval = -EINVAL;
        outbox_push(this->clients[cur_op->peer_fd], cur_op);
        return;
    }
    inflight_ops++;
    if (cur_op->req.hdr.opcode == OSD_OP_TEST_SYNC_STAB_ALL)
    {
        exec_sync_stab_all(cur_op);
    }
    else if (cur_op->req.hdr.opcode == OSD_OP_SHOW_CONFIG)
    {
        exec_show_config(cur_op);
    }
    // FIXME: Do not handle operations immediately, manage some sort of a queue instead
    else if (cur_op->req.hdr.opcode == OSD_OP_READ)
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
    else
    {
        exec_secondary(cur_op);
    }
}
