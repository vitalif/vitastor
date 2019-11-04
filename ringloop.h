#pragma once

#ifndef _LARGEFILE64_SOURCE
#define _LARGEFILE64_SOURCE
#endif

#include <liburing.h>
#include <string.h>

#include <functional>
#include <vector>

struct ring_data_t
{
    uint64_t source;
    struct iovec iov; // for single-entry read/write operations
    int res;
    void *op;
};

struct ring_consumer_t
{
    int number;
    std::function<void (ring_data_t*)> handle_event;
    std::function<void ()> loop;
};

class ring_loop_t
{
    std::vector<ring_consumer_t> consumers;
    struct ring_data_t *ring_data;
public:
    struct io_uring *ring;
    ring_loop_t(int qd);
    ~ring_loop_t();
    struct io_uring_sqe* get_sqe();
    int register_consumer(ring_consumer_t & consumer);
    void unregister_consumer(int number);
    void loop(bool sleep);
};
