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

#define RINGLOOP_DEFAULT_SIZE 1024

static inline void my_uring_prep_rw(int op, struct io_uring_sqe *sqe, int fd, const void *addr, unsigned len, off_t offset)
{
    // Prepare a read/write operation without clearing user_data
    // Very recently, 22 Dec 2021, liburing finally got this change too (8ecd3fd959634df81d66af8b3a69c16202a014e8)
    // But all versions prior to it (sadly) clear user_data
    __u64 user_data = sqe->user_data;
    io_uring_prep_rw(op, sqe, fd, addr, len, offset);
    sqe->user_data = user_data;
}

static inline void my_uring_prep_readv(struct io_uring_sqe *sqe, int fd, const struct iovec *iovecs, unsigned nr_vecs, off_t offset)
{
    my_uring_prep_rw(IORING_OP_READV, sqe, fd, iovecs, nr_vecs, offset);
}

static inline void my_uring_prep_read_fixed(struct io_uring_sqe *sqe, int fd, void *buf, unsigned nbytes, off_t offset, int buf_index)
{
    my_uring_prep_rw(IORING_OP_READ_FIXED, sqe, fd, buf, nbytes, offset);
    sqe->buf_index = buf_index;
}

static inline void my_uring_prep_writev(struct io_uring_sqe *sqe, int fd, const struct iovec *iovecs, unsigned nr_vecs, off_t offset)
{
    my_uring_prep_rw(IORING_OP_WRITEV, sqe, fd, iovecs, nr_vecs, offset);
}

static inline void my_uring_prep_write_fixed(struct io_uring_sqe *sqe, int fd, const void *buf, unsigned nbytes, off_t offset, int buf_index)
{
    my_uring_prep_rw(IORING_OP_WRITE_FIXED, sqe, fd, buf, nbytes, offset);
    sqe->buf_index = buf_index;
}

static inline void my_uring_prep_recvmsg(struct io_uring_sqe *sqe, int fd, struct msghdr *msg, unsigned flags)
{
    my_uring_prep_rw(IORING_OP_RECVMSG, sqe, fd, msg, 1, 0);
    sqe->msg_flags = flags;
}

static inline void my_uring_prep_sendmsg(struct io_uring_sqe *sqe, int fd, const struct msghdr *msg, unsigned flags)
{
    my_uring_prep_rw(IORING_OP_SENDMSG, sqe, fd, msg, 1, 0);
    sqe->msg_flags = flags;
}

static inline void my_uring_prep_poll_add(struct io_uring_sqe *sqe, int fd, short poll_mask)
{
    my_uring_prep_rw(IORING_OP_POLL_ADD, sqe, fd, NULL, 0, 0);
    sqe->poll_events = poll_mask;
}

static inline void my_uring_prep_poll_remove(struct io_uring_sqe *sqe, void *user_data)
{
    my_uring_prep_rw(IORING_OP_POLL_REMOVE, sqe, 0, user_data, 0, 0);
}

static inline void my_uring_prep_fsync(struct io_uring_sqe *sqe, int fd, unsigned fsync_flags)
{
    my_uring_prep_rw(IORING_OP_FSYNC, sqe, fd, NULL, 0, 0);
    sqe->fsync_flags = fsync_flags;
}

static inline void my_uring_prep_nop(struct io_uring_sqe *sqe)
{
    my_uring_prep_rw(IORING_OP_NOP, sqe, 0, NULL, 0, 0);
}

static inline void my_uring_prep_timeout(struct io_uring_sqe *sqe, struct __kernel_timespec *ts, unsigned count, unsigned flags)
{
    my_uring_prep_rw(IORING_OP_TIMEOUT, sqe, 0, ts, 1, count);
    sqe->timeout_flags = flags;
}

static inline void my_uring_prep_timeout_remove(struct io_uring_sqe *sqe, __u64 user_data, unsigned flags)
{
    my_uring_prep_rw(IORING_OP_TIMEOUT_REMOVE, sqe, 0, (void *)user_data, 0, 0);
    sqe->timeout_flags = flags;
}

static inline void my_uring_prep_accept(struct io_uring_sqe *sqe, int fd, struct sockaddr *addr, socklen_t *addrlen, int flags)
{
    my_uring_prep_rw(IORING_OP_ACCEPT, sqe, fd, addr, 0, (__u64) addrlen);
    sqe->accept_flags = flags;
}

static inline void my_uring_prep_cancel(struct io_uring_sqe *sqe, void *user_data, int flags)
{
    my_uring_prep_rw(IORING_OP_ASYNC_CANCEL, sqe, 0, user_data, 0, 0);
    sqe->cancel_flags = flags;
}

struct ring_data_t
{
    struct iovec iov; // for single-entry read/write operations
    int res;
    std::function<void(ring_data_t*)> callback;
};

struct ring_consumer_t
{
    std::function<void(void)> loop;
};

class ring_loop_t
{
    std::vector<std::function<void()>> immediate_queue, immediate_queue2;
    std::vector<ring_consumer_t*> consumers;
    struct ring_data_t *ring_datas;
    int *free_ring_data;
    unsigned free_ring_data_ptr;
    bool loop_again;
    struct io_uring ring;
    int ring_eventfd = -1;
public:
    ring_loop_t(int qd);
    ~ring_loop_t();
    void register_consumer(ring_consumer_t *consumer);
    void unregister_consumer(ring_consumer_t *consumer);
    int register_eventfd();

    inline struct io_uring_sqe* get_sqe()
    {
        if (free_ring_data_ptr == 0)
            return NULL;
        struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
        assert(sqe);
        *sqe = { 0 };
        io_uring_sqe_set_data(sqe, ring_datas + free_ring_data[--free_ring_data_ptr]);
        return sqe;
    }
    inline void set_immediate(const std::function<void()> cb)
    {
        immediate_queue.push_back(cb);
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

    void loop();
    void wakeup();

    unsigned save();
    void restore(unsigned sqe_tail);
};
