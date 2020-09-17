// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.0 or GNU GPL-2.0+ (see README.md for details)

#include <sys/timerfd.h>
#include <sys/poll.h>
#include <unistd.h>
#include "timerfd_interval.h"

timerfd_interval::timerfd_interval(ring_loop_t *ringloop, int seconds, std::function<void(void)> cb)
{
    wait_state = 0;
    timerfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    if (timerfd < 0)
    {
        throw std::runtime_error(std::string("timerfd_create: ") + strerror(errno));
    }
    struct itimerspec exp = {
        .it_interval = { seconds, 0 },
        .it_value = { seconds, 0 },
    };
    if (timerfd_settime(timerfd, 0, &exp, NULL))
    {
        throw std::runtime_error(std::string("timerfd_settime: ") + strerror(errno));
    }
    consumer.loop = [this]() { loop(); };
    ringloop->register_consumer(&consumer);
    this->ringloop = ringloop;
    this->callback = cb;
}

timerfd_interval::~timerfd_interval()
{
    ringloop->unregister_consumer(&consumer);
    close(timerfd);
}

void timerfd_interval::loop()
{
    if (wait_state == 1)
    {
        return;
    }
    struct io_uring_sqe *sqe = ringloop->get_sqe();
    if (!sqe)
    {
        wait_state = 0;
        return;
    }
    struct ring_data_t *data = ((ring_data_t*)sqe->user_data);
    my_uring_prep_poll_add(sqe, timerfd, POLLIN);
    data->callback = [&](ring_data_t *data)
    {
        if (data->res < 0)
        {
            throw std::runtime_error(std::string("waiting for timer failed: ") + strerror(-data->res));
        }
        uint64_t n;
        read(timerfd, &n, 8);
        wait_state = 0;
        callback();
    };
    wait_state = 1;
    ringloop->submit();
}
