#include <sys/socket.h>
#include <sys/epoll.h>
#include <sys/poll.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "osd.h"

#define CL_READ_OP 1
#define CL_READ_DATA 2
#define SQE_SENT 0x100l
#define CL_WRITE_READY 1
#define CL_WRITE_REPLY 2
#define CL_WRITE_DATA 3

osd_t::osd_t(blockstore_config_t & config, blockstore_t *bs, ring_loop_t *ringloop)
{
    bind_address = config["bind_address"];
    if (bind_address == "")
        bind_address = "0.0.0.0";
    bind_port = strtoull(config["bind_port"].c_str(), NULL, 10);
    if (!bind_port || bind_port > 65535)
        bind_port = 11203;

    this->bs = bs;
    this->ringloop = ringloop;

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
    ev.events = EPOLLIN;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &ev) < 0)
    {
        throw std::runtime_error(std::string("epoll_ctl: ") + strerror(errno));
    }

    consumer.loop = [this]() { loop(); };
    ringloop->register_consumer(consumer);
}

osd_t::~osd_t()
{
    ringloop->unregister_consumer(consumer);
    close(epoll_fd);
    close(listen_fd);
}

bool osd_t::shutdown()
{
    // TODO
}

void osd_t::loop()
{
    if (wait_state == 0)
    {
        io_uring_sqe *sqe = ringloop->get_sqe();
        if (!sqe)
        {
            wait_state = 0;
            return;
        }
        ring_data_t *data = ((ring_data_t*)sqe->user_data);
        my_uring_prep_poll_add(sqe, epoll_fd, POLLIN);
        data->callback = [&](ring_data_t *data)
        {
            if (data->res < 0)
            {
                throw std::runtime_error(std::string("epoll failed: ") + strerror(-data->res));
            }
            handle_epoll_events();
            wait_state = 0;
        };
        wait_state = 1;
    }
    send_replies();
    read_requests();
    ringloop->submit();
}

#define MAX_EPOLL_EVENTS 16

int osd_t::handle_epoll_events()
{
    epoll_event events[MAX_EPOLL_EVENTS];
    int count = 0;
    int nfds;
    // FIXME: We shouldn't probably handle ALL available events, we should sometimes
    // yield control to Blockstore and possibly other consumers
    while ((nfds = epoll_wait(epoll_fd, events, MAX_EPOLL_EVENTS, 0)) > 0)
    {
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
                    clients[peer_fd] = {
                        .peer_addr = addr,
                        .peer_addr_size = peer_addr_size,
                        .peer_fd = peer_fd,
                    };
                    // Add FD to epoll
                    epoll_event ev;
                    ev.data.fd = peer_fd;
                    ev.events = EPOLLIN | EPOLLRDHUP;
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
                if (events[i].events & EPOLLRDHUP)
                {
                    // Stop client
                    printf("osd: client %d disconnected\n", cl.peer_fd);
                    stop_client(cl.peer_fd);
                }
                else if (!cl.read_ready)
                {
                    // Mark client as ready (i.e. some data is available)
                    cl.read_ready = true;
                    if (!cl.reading)
                        read_ready_clients.push_back(cl.peer_fd);
                }
            }
            count++;
        }
    }
    return count;
}

void osd_t::stop_client(int peer_fd)
{
    epoll_event ev;
    ev.data.fd = peer_fd;
    ev.events = EPOLLIN | EPOLLHUP;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_DEL, peer_fd, &ev) < 0)
    {
        throw std::runtime_error(std::string("epoll_ctl: ") + strerror(errno));
    }
    auto it = clients.find(peer_fd);
    if (it->second.read_ready)
    {
        for (auto rit = read_ready_clients.begin(); rit != read_ready_clients.end(); rit++)
        {
            if (*rit == peer_fd)
            {
                read_ready_clients.erase(rit);
                break;
            }
        }
    }
    clients.erase(it);
    close(peer_fd);
}

void osd_t::read_requests()
{
    for (int i = 0; i < read_ready_clients.size(); i++)
    {
        int peer_fd = read_ready_clients[i];
        auto & cl = clients[peer_fd];
        io_uring_sqe* sqe = ringloop->get_sqe();
        if (!sqe)
        {
            read_ready_clients.erase(read_ready_clients.begin(), read_ready_clients.begin() + i);
            return;
        }
        ring_data_t* data = ((ring_data_t*)sqe->user_data);
        if (!cl.read_buf)
        {
            // no reads in progress, so this is probably a new command
            cl.read_op = new osd_op_t;
            cl.read_buf = &cl.read_op->op_buf;
            cl.read_remaining = OSD_OP_PACKET_SIZE;
            cl.read_state = CL_READ_OP;
        }
        cl.read_iov.iov_base = cl.read_buf;
        cl.read_iov.iov_len = cl.read_remaining;
        cl.read_msg.msg_iov = &cl.read_iov;
        cl.read_msg.msg_iovlen = 1;
        data->callback = [this, peer_fd](ring_data_t *data) { handle_read(data, peer_fd); };
        my_uring_prep_recvmsg(sqe, peer_fd, &cl.read_msg, 0);
        cl.reading = true;
        cl.read_ready = false;
    }
    read_ready_clients.clear();
}

void osd_t::handle_read(ring_data_t *data, int peer_fd)
{
    auto cl_it = clients.find(peer_fd);
    if (cl_it != clients.end())
    {
        auto & cl = cl_it->second;
        if (data->res < 0 && data->res != -EAGAIN)
        {
            // this is a client socket, so don't panic. just disconnect it
            printf("Client %d socket read error: %d (%s). Disconnecting client\n", peer_fd, -data->res, strerror(-data->res));
            stop_client(peer_fd);
            return;
        }
        cl.reading = false;
        if (cl.read_ready)
        {
            read_ready_clients.push_back(peer_fd);
        }
        if (data->res > 0)
        {
            cl.read_remaining -= data->res;
            cl.read_buf += data->res;
            if (cl.read_remaining <= 0)
            {
                osd_op_t *cur_op = cl.read_op;
                cl.read_buf = NULL;
                if (cl.read_state == CL_READ_OP)
                {
                    if (cur_op->op.hdr.opcode == OSD_OP_SECONDARY_READ ||
                        cur_op->op.hdr.opcode == OSD_OP_SECONDARY_WRITE ||
                        cur_op->op.hdr.opcode == OSD_OP_SECONDARY_STABILIZE)
                    {
                        // Allocate a buffer
                        cur_op->buf = memalign(512, cur_op->op.sec_rw.len);
                    }
                    if (cur_op->op.hdr.opcode == OSD_OP_SECONDARY_WRITE ||
                        cur_op->op.hdr.opcode == OSD_OP_SECONDARY_STABILIZE)
                    {
                        // Read data
                        cl.read_buf = cur_op->buf;
                        cl.read_remaining = cur_op->op.sec_rw.len;
                        cl.read_state = CL_READ_DATA;
                    }
                    else
                    {
                        // Command is ready
                        cur_op->peer_fd = peer_fd;
                        enqueue_op(cur_op);
                        cl.read_op = NULL;
                        cl.read_state = 0;
                    }
                }
                else if (cl.read_state == CL_READ_DATA)
                {
                    // Command is ready
                    cur_op->peer_fd = peer_fd;
                    enqueue_op(cur_op);
                    cl.read_op = NULL;
                    cl.read_state = 0;
                }
            }
        }
    }
}

void osd_t::enqueue_op(osd_op_t *cur_op)
{
    cur_op->bs_op.callback = [this, cur_op](blockstore_op_t* bs_op)
    {
        auto cl_it = clients.find(cur_op->peer_fd);
        if (cl_it != clients.end())
        {
            auto & cl = cl_it->second;
            if (cl.write_state == 0)
            {
                cl.write_state = CL_WRITE_READY;
                write_ready_clients.push_back(cur_op->peer_fd);
            }
            cl.completions.push_back(cur_op);
            ringloop->wakeup();
        }
        else
        {
            delete cur_op;
        }
    };
    if (cur_op->op.hdr.magic != SECONDARY_OSD_OP_MAGIC ||
        cur_op->op.hdr.opcode < OSD_OP_MIN || cur_op->op.hdr.opcode > OSD_OP_MAX ||
        (cur_op->op.hdr.opcode == OSD_OP_SECONDARY_READ || cur_op->op.hdr.opcode == OSD_OP_SECONDARY_WRITE) &&
        (cur_op->op.sec_rw.len > OSD_RW_MAX || cur_op->op.sec_rw.len % OSD_RW_ALIGN || cur_op->op.sec_rw.offset % OSD_RW_ALIGN))
    {
        // Bad command
        cur_op->bs_op.retval = -EINVAL;
        cur_op->bs_op.callback(&cur_op->bs_op);
        return;
    }
    // FIXME: LIST is not a blockstore op yet
    cur_op->bs_op.flags = (cur_op->op.hdr.opcode == OSD_OP_SECONDARY_READ ? OP_READ
        : (cur_op->op.hdr.opcode == OSD_OP_SECONDARY_WRITE ? OP_WRITE
        : (cur_op->op.hdr.opcode == OSD_OP_SECONDARY_SYNC ? OP_SYNC
        : (cur_op->op.hdr.opcode == OSD_OP_SECONDARY_STABILIZE ? OP_STABLE
        : (cur_op->op.hdr.opcode == OSD_OP_SECONDARY_DELETE ? OP_DELETE
        : -1)))));
    if (cur_op->op.hdr.opcode == OSD_OP_SECONDARY_READ ||
        cur_op->op.hdr.opcode == OSD_OP_SECONDARY_WRITE)
    {
        cur_op->bs_op.oid = cur_op->op.sec_rw.oid;
        cur_op->bs_op.version = cur_op->op.sec_rw.version;
        cur_op->bs_op.offset = cur_op->op.sec_rw.offset;
        cur_op->bs_op.len = cur_op->op.sec_rw.len;
        cur_op->bs_op.buf = cur_op->buf;
    }
    else if (cur_op->op.hdr.opcode == OSD_OP_SECONDARY_DELETE)
    {
        cur_op->bs_op.oid = cur_op->op.sec_del.oid;
        cur_op->bs_op.version = cur_op->op.sec_del.version;
    }
    else if (cur_op->op.hdr.opcode == OSD_OP_SECONDARY_STABILIZE)
    {
        cur_op->bs_op.len = cur_op->op.sec_stabilize.len/sizeof(obj_ver_id);
        cur_op->bs_op.buf = cur_op->buf;
    }
    bs->enqueue_op(&cur_op->bs_op);
}

void osd_t::send_replies()
{
    for (int i = 0; i < write_ready_clients.size(); i++)
    {
        int peer_fd = write_ready_clients[i];
        auto & cl = clients[peer_fd];
        io_uring_sqe* sqe = ringloop->get_sqe();
        if (!sqe)
        {
            write_ready_clients.erase(write_ready_clients.begin(), write_ready_clients.begin() + i);
            return;
        }
        ring_data_t* data = ((ring_data_t*)sqe->user_data);
        if (!cl.write_buf)
        {
            // pick next command
            cl.write_op = cl.completions.front();
            cl.completions.pop_front();
            make_reply(cl.write_op);
            cl.write_buf = &cl.write_op->reply_buf;
            cl.write_remaining = OSD_REPLY_PACKET_SIZE;
            cl.write_state = CL_WRITE_REPLY;
        }
        cl.write_iov.iov_base = cl.write_buf;
        cl.write_iov.iov_len = cl.write_remaining;
        cl.write_msg.msg_iov = &cl.write_iov;
        cl.write_msg.msg_iovlen = 1;
        data->callback = [this, peer_fd](ring_data_t *data) { handle_send(data, peer_fd); };
        my_uring_prep_sendmsg(sqe, peer_fd, &cl.write_msg, 0);
        cl.write_state = cl.write_state | SQE_SENT;
    }
    write_ready_clients.clear();
}

void osd_t::make_reply(osd_op_t *op)
{
    op->reply.hdr.magic = SECONDARY_OSD_REPLY_MAGIC;
    op->reply.hdr.id = op->op.hdr.id;
    op->reply.hdr.retval = op->bs_op.retval;
}

void osd_t::handle_send(ring_data_t *data, int peer_fd)
{
    auto cl_it = clients.find(peer_fd);
    if (cl_it != clients.end())
    {
        auto & cl = cl_it->second;
        if (data->res < 0 && data->res != -EAGAIN)
        {
            // this is a client socket, so don't panic. just disconnect it
            printf("Client %d socket write error: %d (%s). Disconnecting client\n", peer_fd, -data->res, strerror(-data->res));
            stop_client(peer_fd);
            return;
        }
        cl.write_state = cl.write_state & ~SQE_SENT;
        if (data->res > 0)
        {
            cl.write_remaining -= data->res;
            cl.write_buf += data->res;
            if (cl.write_remaining <= 0)
            {
                cl.write_buf = NULL;
                osd_op_t *cur_op = cl.write_op;
                if (cl.write_state == CL_WRITE_REPLY)
                {
                    if (cur_op->op.hdr.opcode == OSD_OP_SECONDARY_READ &&
                        cur_op->reply.hdr.retval > 0)
                    {
                        // Send data
                        cl.write_buf = cur_op->buf;
                        cl.write_remaining = cur_op->reply.hdr.retval;
                        cl.write_state = CL_WRITE_DATA;
                    }
                    else if (cur_op->op.hdr.opcode == OSD_OP_SECONDARY_LIST)
                    {
                        // FIXME
                    }
                    else
                    {
                        goto op_done;
                    }
                }
                else if (cl.write_state == CL_WRITE_DATA)
                {
                op_done:
                    // Done
                    delete cur_op;
                    cl.write_op = NULL;
                    cl.write_state = cl.completions.size() > 0 ? CL_WRITE_READY : 0;
                }
            }
        }
        if (cl.write_state != 0)
        {
            write_ready_clients.push_back(peer_fd);
        }
    }
}
