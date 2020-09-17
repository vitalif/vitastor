// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.0 or GNU GPL-2.0+ (see README.md for details)

#pragma once

#include <map>

#include "ringloop.h"
#include "timerfd_manager.h"

class epoll_manager_t
{
    int epoll_fd;
    ring_loop_t *ringloop;
    std::map<int, std::function<void(int, int)>> epoll_handlers;
public:
    epoll_manager_t(ring_loop_t *ringloop);
    ~epoll_manager_t();
    void set_fd_handler(int fd, bool wr, std::function<void(int, int)> handler);
    void handle_epoll_events();

    timerfd_manager_t *tfd;
};
