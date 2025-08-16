// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#pragma once

#ifndef _LARGEFILE64_SOURCE
#define _LARGEFILE64_SOURCE
#endif

#include <string.h>
#include <assert.h>
#include <liburing.h>

#include <string>
#include <functional>
#include <vector>
#include <mutex>

#define RINGLOOP_DEFAULT_SIZE 1024

struct ring_data_t
{
    struct iovec iov; // for single-entry read/write operations
    int res;
    bool prev: 1;
    bool more: 1;
    std::function<void(ring_data_t*)> callback;
};

struct ring_consumer_t
{
    std::function<void(void)> loop;
};

class __attribute__((visibility("default"))) ring_loop_t
{
    std::vector<std::function<void()>> immediate_queue, immediate_queue2;
    std::vector<ring_consumer_t*> consumers;
    struct ring_data_t *ring_datas;
    std::mutex mu;
    bool mt;
    int *free_ring_data;
    unsigned free_ring_data_ptr;
    bool in_loop;
    bool loop_again;
    struct io_uring ring;
    int ring_eventfd = -1;
    bool support_zc = false;
public:
    ring_loop_t(int qd, bool multithreaded = false);
    ~ring_loop_t();
    void register_consumer(ring_consumer_t *consumer);
    void unregister_consumer(ring_consumer_t *consumer);
    int register_eventfd();

    io_uring_sqe* get_sqe();
    inline void set_immediate(const std::function<void()> cb)
    {
        immediate_queue.push_back(cb);
        wakeup();
    }
    inline int submit()
    {
        return io_uring_submit(&ring);
    }
    inline int wait()
    {
        struct io_uring_cqe *cqe;
        return io_uring_wait_cqe(&ring, &cqe);
    }
    int sqes_left();
    inline unsigned space_left()
    {
        return free_ring_data_ptr;
    }
    inline bool has_work()
    {
        return loop_again;
    }
    inline bool has_sendmsg_zc()
    {
        return support_zc;
    }

    void loop();
    void wakeup();

    unsigned save();
    void restore(unsigned sqe_tail);
};
