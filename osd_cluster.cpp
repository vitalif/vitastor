#include <netinet/tcp.h>
#include <sys/epoll.h>

#include "osd.h"

int get_port(std::string & host)
{
    int port = 0;
    int pos = 0;
    if ((pos = host.find(':')) >= 0)
    {
        port = strtoull(host.c_str() + pos + 1, NULL, 10);
        if (port >= 0x10000)
        {
            port = 0;
        }
        host = host.substr(0, pos);
    }
    return port;
}

struct http_co_t
{
    osd_t *osd;
    std::string host;
    std::string request;
    std::vector<char> response;

    int st = 0;
    int peer_fd = -1;
    int epoll_events = 0;
    int code = 0;
    int sent = 0, received = 0;
    iovec iov;
    msghdr msg = { 0 };
    int cqe_res = 0;

    std::function<void(int, std::string)> callback;
    std::function<void(int, int)> epoll_handler;

    ~http_co_t();
    void resume();
};

void osd_t::http_request(std::string host, std::string request, std::function<void(int, std::string)> callback)
{
    http_co_t *handler = new http_co_t();
    handler->osd = this;
    handler->host = host;
    handler->request = request;
    handler->callback = callback;
    handler->epoll_handler = [this, handler](int peer_fd, int epoll_events)
    {
        handler->epoll_events |= epoll_events;
        handler->resume();
    };
    handler->resume();
}

http_co_t::~http_co_t()
{
    response.resize(response.size()+1);
    response[response.size()-1] = 0;
    callback(code, std::string(response.data(), response.size()));
    if (peer_fd >= 0)
    {
        osd->epoll_handlers.erase(peer_fd);
        epoll_ctl(osd->epoll_fd, EPOLL_CTL_DEL, peer_fd, NULL);
        close(peer_fd);
        peer_fd = -1;
    }
}

void http_co_t::resume()
{
    if (st == 0)
    {
        int port = get_port(host);
        struct sockaddr_in addr;
        int r;
        if ((r = inet_pton(AF_INET, host.c_str(), &addr.sin_addr)) != 1)
        {
            delete this;
            return;
        }
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port ? port : 80);
        peer_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (peer_fd < 0)
        {
            delete this;
            return;
        }
        fcntl(peer_fd, F_SETFL, fcntl(peer_fd, F_GETFL, 0) | O_NONBLOCK);
        r = connect(peer_fd, (sockaddr*)&addr, sizeof(addr));
        if (r < 0 && errno != EINPROGRESS)
        {
            delete this;
            return;
        }
        osd->epoll_handlers[peer_fd] = epoll_handler;
        // Add FD to epoll (EPOLLOUT for tracking connect() result)
        epoll_event ev;
        ev.data.fd = peer_fd;
        ev.events = EPOLLOUT | EPOLLIN | EPOLLRDHUP | EPOLLET;
        if (epoll_ctl(osd->epoll_fd, EPOLL_CTL_ADD, peer_fd, &ev) < 0)
        {
            throw std::runtime_error(std::string("epoll_ctl: ") + strerror(errno));
        }
        st = 1;
        return;
    }
    if (st == 1)
    {
        if (epoll_events & EPOLLOUT)
        {
            int result = 0;
            socklen_t result_len = sizeof(result);
            if (getsockopt(peer_fd, SOL_SOCKET, SO_ERROR, &result, &result_len) < 0)
            {
                result = errno;
            }
            if (result != 0)
            {
                delete this;
                return;
            }
            int one = 1;
            setsockopt(peer_fd, SOL_TCP, TCP_NODELAY, &one, sizeof(one));
            // Disable EPOLLOUT on this fd
            epoll_event ev;
            ev.data.fd = peer_fd;
            ev.events = EPOLLIN | EPOLLRDHUP | EPOLLET;
            if (epoll_ctl(osd->epoll_fd, EPOLL_CTL_MOD, peer_fd, &ev) < 0)
            {
                throw std::runtime_error(std::string("epoll_ctl: ") + strerror(errno));
            }
            st = 2;
            epoll_events = 0;
            resume();
            return;
        }
        else if (epoll_events & EPOLLRDHUP)
        {
            delete this;
            return;
        }
        else
        {
            return;
        }
    }
    // Write data
    if (st == 2)
    {
        io_uring_sqe *sqe = osd->ringloop->get_sqe();
        if (!sqe)
            return;
        ring_data_t* data = ((ring_data_t*)sqe->user_data);
        iov = (iovec){ .iov_base = (void*)(request.c_str()+sent), .iov_len = request.size()-sent };
        msg.msg_iov = &iov;
        msg.msg_iovlen = 1;
        data->callback = [this](ring_data_t *data)
        {
            st = 4;
            cqe_res = data->res;
            resume();
        };
        my_uring_prep_sendmsg(sqe, peer_fd, &msg, 0);
        st = 3;
        return;
    }
    if (st == 3)
    {
        return;
    }
    if (st == 4)
    {
        if (cqe_res < 0 && cqe_res != -EAGAIN)
        {
            delete this;
            return;
        }
        sent += cqe_res;
        if (sent < request.size())
            st = 2;
        else
            st = 5;
        resume();
        return;
    }
    // Read response
    if (st == 5)
    {
        if (epoll_events & EPOLLIN)
        {
            response.resize(received + 9000);
            io_uring_sqe *sqe = osd->ringloop->get_sqe();
            if (!sqe)
                return;
            ring_data_t* data = ((ring_data_t*)sqe->user_data);
            iov = { .iov_base = response.data()+received, .iov_len = 9000 };
            msg.msg_iov = &iov;
            msg.msg_iovlen = 1;
            data->callback = [this](ring_data_t *data)
            {
                st = 7;
                cqe_res = data->res;
                resume();
            };
            my_uring_prep_recvmsg(sqe, peer_fd, &msg, 0);
            st = 6;
            epoll_events = 0;
        }
        else if (epoll_events & EPOLLRDHUP)
        {
            delete this;
            return;
        }
    }
    if (st == 6)
    {
        return;
    }
    if (st == 7)
    {
        if (cqe_res < 0 && cqe_res != -EAGAIN)
        {
            delete this;
            return;
        }
        received += cqe_res;
        st = 5;
        resume();
        return;
    }
}

/*void osd_t::get_pgs()
{
    //consul_address
}*/
