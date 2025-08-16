// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#pragma once

#include <map>

#include "ringloop.h"
#include "timerfd_manager.h"

class __attribute__((visibility("default"))) epoll_manager_t
{
    int epoll_fd;
    bool pending;
    ring_consumer_t consumer;
    ring_loop_t *ringloop;
    std::map<int, std::function<void(int, int)>> epoll_handlers;

    void handle_uring_event();
public:
    epoll_manager_t(ring_loop_t *ringloop);
    ~epoll_manager_t();
    int get_fd();
    void set_fd_handler(int fd, bool wr, std::function<void(int, int)> handler);
    void handle_events(int timeout);

    timerfd_manager_t *tfd;
};
