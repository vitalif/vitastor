#pragma once

#include <time.h>
#include <vector>
#include <functional>

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

    void inc_timer(timerfd_timer_t & t);
    void set_nearest();
    void trigger_nearest();
    void handle_readable();
public:
    std::function<void(int, bool, std::function<void(int, int)>)> set_fd_handler;

    timerfd_manager_t(std::function<void(int, bool, std::function<void(int, int)>)> set_fd_handler);
    ~timerfd_manager_t();
    int set_timer(uint64_t millis, bool repeat, std::function<void(int)> callback);
    void clear_timer(int timer_id);
};
