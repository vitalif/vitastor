#include <sys/timerfd.h>
#include <sys/poll.h>
#include <unistd.h>
#include "timerfd_manager.h"

timerfd_manager_t::timerfd_manager_t(ring_loop_t *ringloop)
{
    wait_state = 0;
    timerfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    if (timerfd < 0)
    {
        throw std::runtime_error(std::string("timerfd_create: ") + strerror(errno));
    }
    consumer.loop = [this]() { loop(); };
    ringloop->register_consumer(&consumer);
    this->ringloop = ringloop;
}

timerfd_manager_t::~timerfd_manager_t()
{
    ringloop->unregister_consumer(&consumer);
    close(timerfd);
}

void timerfd_manager_t::inc_timer(timerfd_timer_t & t)
{
    t.next.tv_sec += t.millis/1000;
    t.next.tv_nsec += (t.millis%1000)*1000000;
    if (t.next.tv_nsec > 1000000000)
    {
        t.next.tv_sec++;
        t.next.tv_nsec -= 1000000000;
    }
}

int timerfd_manager_t::set_timer(uint64_t millis, bool repeat, std::function<void(int)> callback)
{
    int timer_id = id++;
    timespec start;
    clock_gettime(CLOCK_MONOTONIC, &start);
    timers.push_back({
        .id = timer_id,
        .millis = millis,
        .start = start,
        .next = start,
        .repeat = repeat,
        .callback = callback,
    });
    inc_timer(timers[timers.size()-1]);
    set_nearest();
    set_wait();
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
            set_nearest();
            set_wait();
            break;
        }
    }
}

void timerfd_manager_t::set_nearest()
{
    if (!timers.size())
    {
        nearest = -1;
        itimerspec exp = { 0 };
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
        if (timerfd_settime(timerfd, 0, &exp, NULL))
        {
            throw std::runtime_error(std::string("timerfd_settime: ") + strerror(errno));
        }
        wait_state = wait_state | 1;
    }
}

void timerfd_manager_t::loop()
{
    if (!(wait_state & 1) && timers.size())
    {
        set_nearest();
    }
    set_wait();
}

void timerfd_manager_t::set_wait()
{
    if ((wait_state & 3) == 1)
    {
        io_uring_sqe *sqe = ringloop->get_sqe();
        if (!sqe)
        {
            return;
        }
        ring_data_t *data = ((ring_data_t*)sqe->user_data);
        my_uring_prep_poll_add(sqe, timerfd, POLLIN);
        data->callback = [this](ring_data_t *data)
        {
            if (data->res < 0)
            {
                throw std::runtime_error(std::string("waiting for timer failed: ") + strerror(-data->res));
            }
            uint64_t n;
            read(timerfd, &n, 8);
            if (nearest >= 0)
            {
                timers[nearest].callback(timers[nearest].id);
                if (!timers[nearest].repeat)
                {
                    timers.erase(timers.begin()+nearest, timers.begin()+nearest+1);
                }
                else
                {
                    inc_timer(timers[nearest]);
                }
                nearest = -1;
            }
            wait_state = 0;
            set_nearest();
            set_wait();
        };
        wait_state = 3;
    }
}
