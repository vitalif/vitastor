#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "osd_ops.h"
#include "ringloop.h"

struct osd_client_t
{
    sockaddr_in peer_addr;
    socklen_t peer_addr_size;
    int peer_fd;
    bool ready;
    bool reading;

    iovec iov;
    msghdr msg;
    void *cur_buf = NULL;
    int cur_len = 0;
};

class osd_t
{
    int wait_state = 0;
    int epoll_fd = 0;
    int listen_fd = 0;
    ring_consumer_t consumer;

    std::string bind_address;
    int bind_port, listen_backlog;
    ring_loop_t *ringloop;

    std::unordered_map<int,osd_client_t> clients;
    std::deque<int> ready_clients;

    void handle_epoll_events();
public:
    osd_t(ring_loop_t *ringloop);
    ~osd_t();
    void loop();
};

osd_t::osd_t(ring_loop_t *ringloop)
{
    this->ringloop = ringloop;

    listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0)
    {
        throw std::runtime_error(std::string("socket: ") + strerror(errno));
    }
    int enable = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable));

    sockaddr_in addr;
    if ((int r = inet_pton(AF_INET, bind_address.c_str(), &addr.sin_addr)) != 1)
    {
        close(listen_fd);
        throw std::runtime_error("bind address "+bind_address+(r == 0 ? " is not valid" : ": no ipv4 support"));
    }
    addr.sin_family = AF_INET;
    addr.sin_port = htons(bind_port);

    if (bind(listen_fd, &addr, sizeof(addr)) < 0)
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

    struct epoll_event ev;
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

void osd_t::loop()
{
    if (wait_state == 1)
    {
        return;
    }
    struct io_uring_sqe *sqe = ringloop->get_sqe();
    if (!sqe)
    {
        wait_state = 0;
        return;
    }
    struct ring_data_t *data = ((ring_data_t*)sqe->user_data);
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
                struct sockaddr_in addr;
                socklen_t peer_addr_size = sizeof(addr);
                int peer_fd;
                while ((peer_fd = accept(listen_fd, &addr, &peer_addr_size)) >= 0)
                {
                    fcntl(peer_fd, F_SETFL, fcntl(listen_fd, F_GETFL, 0) | O_NONBLOCK);
                    clients[peer_fd] = {
                        .peer_addr = addr,
                        .peer_addr_size = peer_addr_size,
                        .peer_fd = peer_fd,
                        .ready = false,
                    };
                    // Add FD to epoll
                    struct epoll_event ev;
                    ev.data.fd = peer_fd;
                    ev.events = EPOLLIN | EPOLLHUP;
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
                if (events[i].events & EPOLLHUP)
                {
                    // Stop client
                    stop_client(cl.peer_fd);
                }
                else if (!cl.ready)
                {
                    // Mark client as ready (i.e. some data is available)
                    cl.ready = true;
                    if (!cl.reading)
                        ready_clients.push_back(cl.peer_fd);
                }
            }
            count++;
        }
    }
    return count;
}

void osd_t::stop_client(int peer_fd)
{
    struct epoll_event ev;
    ev.data.fd = peer_fd;
    ev.events = EPOLLIN | EPOLLHUP;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_DEL, peer_fd, &ev) < 0)
    {
        throw std::runtime_error(std::string("epoll_ctl: ") + strerror(errno));
    }
    auto it = clients.find(peer_fd);
    clients.erase(it);
    if (cl.ready)
    {
        for (auto it = ready_clients.begin(); it != ready_clients.end(); it++)
        {
            if (*it == peer_fd)
            {
                ready_clients.erase(it);
                break;
            }
        }
    }
    close(peer_fd);
}

void osd_t::read_commands()
{
    for (int i = 0; i < ready_clients.size(); i++)
    {
        int peer_fd = ready_clients[i];
        auto & cl = clients[peer_fd];
        if (!cl.cur_buf)
        {
            // no reads in progress, this is probably a new command
            cl.cur_buf = cl.command_buffer;
            cl.cur_len = OSD_OP_PACKET_SIZE;
        }
        struct io_uring_sqe* sqe = ringloop->get_sqe();
        if (!sqe)
        {
            ready_clients.erase(ready_clients.begin(), ready_clients().begin() + i);
            return;
        }
        struct ring_data_t* data = ((ring_data_t*)sqe->user_data);
        cl.iov.iov_base = cl.cur_buf;
        cl.iov.iov_len = cl.cur_len;
        cl.msg.msg_iov = &cl.iov;
        cl.msg.msg_iovlen = 1;
        data->callback = [this, peer_fd](ring_data_t *data) { handle_read(data, peer_fd); };
        my_uring_prep_recvmsg(sqe, peer_fd, &cl.msg, 0);
        ringloop->submit();
        cl.reading = true;
    }
    ready_clients.clear();
}

void osd_t::handle_read(ring_data_t *data, int peer_fd)
{
    auto cl = clients.find(peer_fd);
    if (cl != clients.end())
    {
        if (data->res < 0 && data->res != -EAGAIN)
        {
            // this is a client socket, so don't panic. just disconnect it
            printf("Client %d socket read error: %d (%s). Disconnecting client\n", peer_fd, -data->res, strerror(-data->res));
            stop_client(peer_fd);
            return;
        }
        cl->reading = false;
        if (cl->ready)
            ready_clients.push_back(peer_fd);
        
    }
}
