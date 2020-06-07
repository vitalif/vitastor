#include <sys/epoll.h>
#include <sys/poll.h>
#include <unistd.h>

#include "epoll_manager.h"

#define MAX_EPOLL_EVENTS 64

epoll_manager_t::epoll_manager_t(ring_loop_t *ringloop)
{
    this->ringloop = ringloop;

    epoll_fd = epoll_create(1);
    if (epoll_fd < 0)
    {
        throw std::runtime_error(std::string("epoll_create: ") + strerror(errno));
    }

    tfd = new timerfd_manager_t([this](int fd, std::function<void(int, int)> handler) { set_fd_handler(fd, handler); });

    handle_epoll_events();
}

epoll_manager_t::~epoll_manager_t()
{
    if (tfd)
    {
        delete tfd;
        tfd = NULL;
    }
    close(epoll_fd);
}

void epoll_manager_t::set_fd_handler(int fd, std::function<void(int, int)> handler)
{
    if (handler != NULL)
    {
        bool exists = epoll_handlers.find(fd) != epoll_handlers.end();
        epoll_event ev;
        ev.data.fd = fd;
        ev.events = EPOLLOUT | EPOLLIN | EPOLLRDHUP | EPOLLET;
        if (epoll_ctl(epoll_fd, exists ? EPOLL_CTL_MOD : EPOLL_CTL_ADD, fd, &ev) < 0)
        {
            throw std::runtime_error(std::string("epoll_ctl: ") + strerror(errno));
        }
        epoll_handlers[fd] = handler;
    }
    else
    {
        if (epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, NULL) < 0 && errno != ENOENT)
        {
            throw std::runtime_error(std::string("epoll_ctl: ") + strerror(errno));
        }
        epoll_handlers.erase(fd);
    }
}

void epoll_manager_t::handle_epoll_events()
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
    do
    {
        nfds = epoll_wait(epoll_fd, events, MAX_EPOLL_EVENTS, 0);
        for (int i = 0; i < nfds; i++)
        {
            auto & cb = epoll_handlers[events[i].data.fd];
            cb(events[i].data.fd, events[i].events);
        }
    } while (nfds == MAX_EPOLL_EVENTS);
}
