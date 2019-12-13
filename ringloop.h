#pragma once

#ifndef _LARGEFILE64_SOURCE
#define _LARGEFILE64_SOURCE
#endif

#include <liburing.h>
#include <string.h>

#include <functional>
#include <vector>

static inline void my_uring_prep_rw(int op, struct io_uring_sqe *sqe, int fd, const void *addr, unsigned len, off_t offset)
{
    sqe->opcode = op;
    sqe->flags = 0;
    sqe->ioprio = 0;
    sqe->fd = fd;
    sqe->off = offset;
    sqe->addr = (unsigned long) addr;
    sqe->len = len;
    sqe->rw_flags = 0;
    sqe->__pad2[0] = sqe->__pad2[1] = sqe->__pad2[2] = 0;
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
    int number;
    std::function<void(void)> loop;
};

class ring_loop_t
{
    std::vector<ring_consumer_t> consumers;
    struct ring_data_t *ring_data;
public:
    bool loop_again;
    struct io_uring ring;
    ring_loop_t(int qd);
    ~ring_loop_t();
    inline struct io_uring_sqe* get_sqe()
    {
        // FIXME: Limit inflight ops count to not overflow the completion ring
        struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
        if (sqe)
        {
            io_uring_sqe_set_data(sqe, ring_data + (sqe - ring.sq.sqes));
        }
        return sqe;
    }
    int register_consumer(ring_consumer_t & consumer);
    void wakeup(ring_consumer_t & consumer);
    void unregister_consumer(ring_consumer_t & consumer);
    void loop();
    inline int submit()
    {
        return io_uring_submit(&ring);
    }
    inline int wait()
    {
        struct io_uring_cqe *cqe;
        return io_uring_wait_cqe(&ring, &cqe);
    }
};
