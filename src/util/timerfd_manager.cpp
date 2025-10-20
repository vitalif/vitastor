// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include <sys/timerfd.h>
#include <sys/poll.h>
#include <sys/epoll.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <string>
#include <stdexcept>
#include "timerfd_manager.h"

timerfd_manager_t::timerfd_manager_t(std::function<void(int, bool, std::function<void(int, int)>)> set_fd_handler)
{
    this->set_fd_handler = set_fd_handler;
    wait_state = 0;
    timerfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    if (timerfd < 0)
    {
        throw std::runtime_error(std::string("timerfd_create: ") + strerror(errno));
    }
    set_fd_handler(timerfd, false, [this](int fd, int events)
    {
        handle_readable();
    });
}

timerfd_manager_t::~timerfd_manager_t()
{
    set_fd_handler(timerfd, false, NULL);
    close(timerfd);
}

void timerfd_manager_t::inc_timer(timerfd_timer_t & t)
{
    t.next.tv_sec += t.micros/1000000;
    t.next.tv_nsec += (t.micros%1000000)*1000;
    if (t.next.tv_nsec >= 1000000000)
    {
        t.next.tv_sec++;
        t.next.tv_nsec -= 1000000000;
    }
}

int timerfd_manager_t::set_timer(uint64_t millis, bool repeat, std::function<void(int)> callback)
{
    return set_timer_us(millis*1000, repeat, callback);
}

int timerfd_manager_t::set_timer_us(uint64_t micros, bool repeat, std::function<void(int)> callback)
{
    int timer_id = id++;
    timespec start;
    clock_gettime(CLOCK_MONOTONIC, &start);
    timers.push_back({
        .id = timer_id,
        .micros = micros,
        .start = start,
        .next = start,
        .repeat = repeat,
        .callback = callback,
    });
    inc_timer(timers[timers.size()-1]);
    set_nearest(false);
    return timer_id;
}

void timerfd_manager_t::clear_timer(int timer_id)
{
    for (int i = 0; i < timers.size(); i++)
    {
        if (timers[i].id == timer_id)
        {
            timers.erase(timers.begin()+i, timers.begin()+i+1);
            if (nearest == i)
            {
                nearest = -1;
                wait_state = wait_state & ~1;
            }
            else if (nearest > i)
            {
                nearest--;
            }
            set_nearest(false);
            break;
        }
    }
}

void timerfd_manager_t::set_nearest(bool trigger_inline)
{
    if (onstack > 0)
    {
        // Prevent re-entry
        return;
    }
    onstack++;
again:
    if (!timers.size())
    {
        nearest = -1;
        itimerspec exp = {};
        if (timerfd_settime(timerfd, 0, &exp, NULL))
        {
            throw std::runtime_error(std::string("timerfd_settime: ") + strerror(errno));
        }
        wait_state = wait_state & ~1;
    }
    else
    {
        nearest = 0;
        for (int i = 1; i < timers.size(); i++)
        {
            if (timers[i].next.tv_sec < timers[nearest].next.tv_sec ||
                timers[i].next.tv_sec == timers[nearest].next.tv_sec &&
                timers[i].next.tv_nsec < timers[nearest].next.tv_nsec)
            {
                nearest = i;
            }
        }
        timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);
        itimerspec exp = {
            .it_interval = { 0 },
            .it_value = timers[nearest].next,
        };
        exp.it_value.tv_sec -= now.tv_sec;
        exp.it_value.tv_nsec -= now.tv_nsec;
        if (exp.it_value.tv_nsec < 0)
        {
            exp.it_value.tv_sec--;
            exp.it_value.tv_nsec += 1000000000;
        }
        if (exp.it_value.tv_sec < 0 || exp.it_value.tv_sec == 0 && exp.it_value.tv_nsec <= 0)
        {
            // It already happened - set minimal timeout
            if (trigger_inline)
            {
                trigger_nearest();
                goto again;
            }
            exp.it_value = { .tv_sec = 0, .tv_nsec = 1 };
        }
        if (timerfd_settime(timerfd, 0, &exp, NULL))
        {
            throw std::runtime_error(std::string("timerfd_settime: ") + strerror(errno));
        }
        wait_state = wait_state | 1;
    }
    onstack--;
}

void timerfd_manager_t::handle_readable()
{
    uint64_t n;
    size_t res = read(timerfd, &n, 8);
    if (res == 8 && nearest >= 0)
    {
        trigger_nearest();
    }
    wait_state = 0;
    set_nearest(true);
}

void timerfd_manager_t::trigger_nearest()
{
    int nearest_id = timers[nearest].id;
    auto cb = timers[nearest].callback;
    if (timers[nearest].repeat)
    {
        inc_timer(timers[nearest]);
    }
    else
    {
        timers.erase(timers.begin()+nearest, timers.begin()+nearest+1);
    }
    nearest = -1;
    cb(nearest_id);
}
