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

class __attribute__((visibility("default"))) ring_loop_i
{
public:
    virtual ~ring_loop_i() = default;
    virtual void register_consumer(ring_consumer_t *consumer) = 0;
    virtual void unregister_consumer(ring_consumer_t *consumer) = 0;
    virtual int register_eventfd() = 0;
    virtual io_uring_sqe* get_sqe() = 0;
    virtual void set_immediate(const std::function<void()> & cb) = 0;
    virtual int submit() = 0;
    virtual int wait() = 0;
    virtual unsigned space_left() = 0;
    virtual bool has_work() = 0;
    virtual bool has_sendmsg_zc() = 0;
    virtual void loop() = 0;
    virtual void wakeup() = 0;
    virtual unsigned save() = 0;
    virtual void restore(unsigned sqe_tail) = 0;
};

class __attribute__((visibility("default"))) ring_loop_t: public ring_loop_i
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
    ring_loop_t(int qd, bool multithreaded = false, bool sqe128 = false);
    ~ring_loop_t();
    void register_consumer(ring_consumer_t *consumer);
    void unregister_consumer(ring_consumer_t *consumer);
    int register_eventfd();

    io_uring_sqe* get_sqe();
    inline void set_immediate(const std::function<void()> & cb)
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
    unsigned space_left();
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

class ring_loop_mock_t: public ring_loop_i
{
    std::vector<std::function<void()>> immediate_queue, immediate_queue2;
    std::vector<ring_consumer_t*> consumers;
    std::vector<io_uring_sqe> sqes;
    std::vector<ring_data_t> ring_datas;
    std::vector<ring_data_t *> free_ring_datas;
    std::vector<ring_data_t *> submit_ring_datas;
    std::vector<ring_data_t *> completed_ring_datas;
    std::function<void(io_uring_sqe *)> submit_cb;
    bool in_loop;
    bool loop_again;
    bool support_zc = false;

public:
    ring_loop_mock_t(int qd, std::function<void(io_uring_sqe *)> submit_cb);

    void register_consumer(ring_consumer_t *consumer);
    void unregister_consumer(ring_consumer_t *consumer);
    void wakeup();
    void set_immediate(const std::function<void()> & cb);
    unsigned space_left();
    bool has_work();
    bool has_sendmsg_zc();

    int register_eventfd();
    io_uring_sqe* get_sqe();
    int submit();
    int wait();
    void loop();
    unsigned save();
    void restore(unsigned sqe_tail);

    void mark_completed(ring_data_t *data);
};

class disk_mock_t
{
    uint8_t *data = NULL;
    size_t size = 0;
    ring_loop_mock_t *loop = NULL;
public:
    bool trace = false;
    disk_mock_t(ring_loop_mock_t *loop, size_t size);
    ~disk_mock_t();
    bool submit(io_uring_sqe *sqe);
};
