// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#pragma once

#include "ringloop.h"

class timerfd_interval
{
    int wait_state;
    int timerfd;
    ring_loop_t *ringloop;
    ring_consumer_t consumer;
    std::function<void(void)> callback;
public:
    timerfd_interval(ring_loop_t *ringloop, int seconds, std::function<void(void)> cb);
    ~timerfd_interval();
    void loop();
};
