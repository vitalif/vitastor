// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#pragma once

#include <time.h>
#include <vector>
#include <functional>

struct timerfd_timer_t
{
    int id;
    uint64_t micros;
    timespec start, next;
    bool repeat;
    std::function<void(int)> callback;
};

class __attribute__((visibility("default"))) timerfd_manager_t
{
    int wait_state = 0;
    int timerfd = -1;
    int nearest = -1;
    int id = 1;
    int onstack = 0;
    std::vector<timerfd_timer_t> timers;
    timespec cur = {};

    void inc_timer(timerfd_timer_t & t);
    void set_nearest(bool trigger_inline);
    void trigger_nearest();
    void handle_readable();
public:
    std::function<void(int, bool, std::function<void(int, int)>)> set_fd_handler;

    timerfd_manager_t(std::function<void(int, bool, std::function<void(int, int)>)> set_fd_handler);
    ~timerfd_manager_t();
    int set_timer(uint64_t millis, bool repeat, std::function<void(int)> callback);
    int set_timer_us(uint64_t micros, bool repeat, std::function<void(int)> callback);
    void clear_timer(int timer_id);
    void tick(timespec passed);
};
