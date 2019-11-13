#include "ringloop.h"

ring_loop_t::ring_loop_t(int qd)
{
    int ret = io_uring_queue_init(qd, ring, 0);
    if (ret < 0)
    {
        throw new std::runtime_error(std::string("io_uring_queue_init: ") + strerror(-ret));
    }
    ring_data = (struct ring_data_t*)malloc(sizeof(ring_data_t) * ring->sq.ring_sz);
    if (!ring_data)
    {
        throw new std::bad_alloc();
    }
}

ring_loop_t::~ring_loop_t()
{
    free(ring_data);
}

struct io_uring_sqe* ring_loop_t::get_sqe()
{
    struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
    if (sqe)
    {
        io_uring_sqe_set_data(sqe, ring_data + (sqe - ring->sq.sqes));
    }
    return sqe;
}

int ring_loop_t::register_consumer(ring_consumer_t & consumer)
{
    consumer.number = consumers.size();
    consumers.push_back(consumer);
    return consumer.number;
}

void ring_loop_t::unregister_consumer(int number)
{
    if (number < consumers.size())
    {
        consumers[number].loop = NULL;
    }
}

void ring_loop_t::loop(bool sleep)
{
    struct io_uring_cqe *cqe;
    if (sleep)
    {
        io_uring_wait_cqe(ring, &cqe);
    }
    while ((io_uring_peek_cqe(ring, &cqe), cqe))
    {
        struct ring_data_t *d = (struct ring_data_t*)cqe->user_data;
        if (d->callback)
        {
            d->callback(d);
        }
        io_uring_cqe_seen(ring, cqe);
    }
    for (int i = 0; i < consumers.size(); i++)
    {
        consumers[i].loop();
    }
}
