// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include <stdlib.h>

#include <stdexcept>

#include "ringloop.h"

ring_loop_t::ring_loop_t(int qd)
{
    int ret = io_uring_queue_init(qd, &ring, 0);
    if (ret < 0)
    {
        throw std::runtime_error(std::string("io_uring_queue_init: ") + strerror(-ret));
    }
    free_ring_data_ptr = *ring.cq.kring_entries;
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
}

ring_loop_t::~ring_loop_t()
{
    free(free_ring_data);
    free(ring_datas);
    io_uring_queue_exit(&ring);
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

void ring_loop_t::loop()
{
    struct io_uring_cqe *cqe;
    while (!io_uring_peek_cqe(&ring, &cqe))
    {
        struct ring_data_t *d = (struct ring_data_t*)cqe->user_data;
        if (d->callback)
        {
            // First free ring_data item, then call the callback
            // so it has at least 1 free slot for the next event
            // which is required for EPOLLET to function properly
            struct ring_data_t dl;
            dl.iov = d->iov;
            dl.res = cqe->res;
            dl.callback.swap(d->callback);
            free_ring_data[free_ring_data_ptr++] = d - ring_datas;
            dl.callback(&dl);
        }
        else
        {
            printf("Warning: empty callback in SQE\n");
            free_ring_data[free_ring_data_ptr++] = d - ring_datas;
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
