// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#pragma once

#include "ringloop.h"

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
    std::map<uint64_t, iovec> buffers;
    size_t size = 0;
    bool buffered = false;

    void erase_buffers(uint64_t begin, uint64_t end);
    ssize_t copy_from_sqe(io_uring_sqe *sqe, uint8_t *to, uint64_t base_offset);
    void read_item(uint8_t *to, uint64_t offset, uint64_t len);
public:
    bool trace = false;
    disk_mock_t(size_t size, bool buffered);
    ~disk_mock_t();
    void clear(size_t offset, size_t len);
    void discard_buffers(bool all, uint32_t seed);
    bool submit(io_uring_sqe *sqe);
};
