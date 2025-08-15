// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include <stdlib.h>
#include <unistd.h>

#include <stdexcept>

#include <sys/eventfd.h>

#include "ringloop.h"

ring_loop_t::ring_loop_t(int qd, bool multithreaded)
{
    mt = multithreaded;
    int ret = io_uring_queue_init(qd, &ring, 0);
    if (ret < 0)
    {
        throw std::runtime_error(std::string("io_uring_queue_init: ") + strerror(-ret));
    }
    free_ring_data_ptr = *ring.sq.kring_entries;
    ring_datas = (struct ring_data_t*)calloc(free_ring_data_ptr, sizeof(ring_data_t));
    free_ring_data = (int*)malloc(sizeof(int) * free_ring_data_ptr);
    if (!ring_datas || !free_ring_data)
    {
        throw std::bad_alloc();
    }
    for (int i = 0; i < free_ring_data_ptr; i++)
    {
        free_ring_data[i] = i;
    }
    in_loop = false;
    auto probe = io_uring_get_probe();
    if (probe)
    {
        support_zc = io_uring_opcode_supported(probe, IORING_OP_SENDMSG_ZC);
        io_uring_free_probe(probe);
    }
}

ring_loop_t::~ring_loop_t()
{
    free(free_ring_data);
    free(ring_datas);
    io_uring_queue_exit(&ring);
    if (ring_eventfd)
    {
        close(ring_eventfd);
    }
}

void ring_loop_t::register_consumer(ring_consumer_t *consumer)
{
    unregister_consumer(consumer);
    consumers.push_back(consumer);
}

void ring_loop_t::wakeup()
{
    loop_again = true;
}

void ring_loop_t::unregister_consumer(ring_consumer_t *consumer)
{
    for (int i = 0; i < consumers.size(); i++)
    {
        if (consumers[i] == consumer)
        {
            consumers.erase(consumers.begin()+i, consumers.begin()+i+1);
            break;
        }
    }
}

io_uring_sqe* ring_loop_t::get_sqe()
{
    if (mt)
        mu.lock();
    if (free_ring_data_ptr == 0)
    {
        if (mt)
            mu.unlock();
        return NULL;
    }
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
    assert(sqe);
    *sqe = { 0 };
    io_uring_sqe_set_data(sqe, ring_datas + free_ring_data[--free_ring_data_ptr]);
    if (mt)
        mu.unlock();
    return sqe;
}

void ring_loop_t::loop()
{
    if (in_loop)
    {
        return;
    }
    in_loop = true;
    if (ring_eventfd >= 0)
    {
        // Reset eventfd counter
        uint64_t ctr = 0;
        int r = read(ring_eventfd, &ctr, 8);
        if (r < 0 && errno != EAGAIN && errno != EINTR)
        {
            fprintf(stderr, "Error resetting eventfd: %s\n", strerror(errno));
        }
    }
    struct io_uring_cqe *cqe;
    while (!io_uring_peek_cqe(&ring, &cqe))
    {
        if (mt)
            mu.lock();
        struct ring_data_t *d = (struct ring_data_t*)cqe->user_data;
        if (cqe->flags & IORING_CQE_F_MORE)
        {
            // There will be a second notification
            if (mt)
                mu.unlock();
            d->res = cqe->res;
            d->more = true;
            if (d->callback)
                d->callback(d);
            d->prev = true;
            d->more = false;
        }
        else if (d->callback)
        {
            // First free ring_data item, then call the callback
            // so it has at least 1 free slot for the next event
            // which is required for EPOLLET to function properly
            struct ring_data_t dl;
            dl.iov = d->iov;
            dl.res = cqe->res;
            dl.more = false;
            dl.prev = d->prev;
            dl.callback.swap(d->callback);
            d->prev = d->more = false;
            free_ring_data[free_ring_data_ptr++] = d - ring_datas;
            if (mt)
                mu.unlock();
            dl.callback(&dl);
        }
        else
        {
            fprintf(stderr, "Warning: empty callback in SQE\n");
            free_ring_data[free_ring_data_ptr++] = d - ring_datas;
            if (mt)
                mu.unlock();
        }
        io_uring_cqe_seen(&ring, cqe);
    }
    do
    {
        loop_again = false;
        for (int i = 0; i < consumers.size(); i++)
        {
            consumers[i]->loop();
            if (immediate_queue.size())
            {
                immediate_queue2.swap(immediate_queue);
                for (auto & cb: immediate_queue2)
                    cb();
                immediate_queue2.clear();
            }
        }
    } while (loop_again);
    in_loop = false;
}

unsigned ring_loop_t::save()
{
    return ring.sq.sqe_tail;
}

void ring_loop_t::restore(unsigned sqe_tail)
{
    assert(ring.sq.sqe_tail >= sqe_tail);
    for (unsigned i = sqe_tail; i < ring.sq.sqe_tail; i++)
    {
        free_ring_data[free_ring_data_ptr++] = ((ring_data_t*)ring.sq.sqes[i & *ring.sq.kring_mask].user_data) - ring_datas;
    }
    ring.sq.sqe_tail = sqe_tail;
}

int ring_loop_t::sqes_left()
{
    struct io_uring_sq *sq = &ring.sq;
    unsigned int head = io_uring_smp_load_acquire(sq->khead);
    unsigned int next = sq->sqe_tail + 1;
    int left = *sq->kring_entries - (next - head);
    if (left > free_ring_data_ptr)
    {
        // return min(sqes left, ring_datas left)
        return free_ring_data_ptr;
    }
    return left;
}

int ring_loop_t::register_eventfd()
{
    if (ring_eventfd >= 0)
    {
        return ring_eventfd;
    }
    ring_eventfd = eventfd(0, EFD_CLOEXEC|EFD_NONBLOCK);
    if (ring_eventfd < 0)
    {
        return -errno;
    }
    int r = io_uring_register_eventfd(&ring, ring_eventfd);
    if (r < 0)
    {
        close(ring_eventfd);
        ring_eventfd = -1;
        return r;
    }
    // Loop once to prevent skipping events happened before eventfd was registered
    loop();
    return ring_eventfd;
}
