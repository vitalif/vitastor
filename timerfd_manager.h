#pragma once

#include <time.h>
#include "ringloop.h"

struct timerfd_timer_t
{
    int id;
    uint64_t millis;
    timespec start, next;
    bool repeat;
    std::function<void(int)> callback;
};

class timerfd_manager_t
{
    int wait_state = 0;
    int timerfd;
    int nearest = -1;
    int id = 1;
    std::vector<timerfd_timer_t> timers;
    ring_loop_t *ringloop;
    ring_consumer_t consumer;

    void inc_timer(timerfd_timer_t & t);
    void set_nearest();
    void set_wait();
    void loop();
public:
    timerfd_manager_t(ring_loop_t *ringloop);
    ~timerfd_manager_t();
    int set_timer(uint64_t millis, bool repeat, std::function<void(int)> callback);
    void clear_timer(int timer_id);
};
