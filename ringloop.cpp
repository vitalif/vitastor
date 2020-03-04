#include "ringloop.h"

ring_loop_t::ring_loop_t(int qd)
{
    int ret = io_uring_queue_init(qd, &ring, 0);
    if (ret < 0)
    {
        throw std::runtime_error(std::string("io_uring_queue_init: ") + strerror(-ret));
    }
    free_ring_data_ptr = *ring.cq.kring_entries;
    ring_datas = (struct ring_data_t*)malloc(sizeof(ring_data_t) * free_ring_data_ptr);
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
            d->res = cqe->res;
            d->callback(d);
        }
        free_ring_data[free_ring_data_ptr++] = d - ring_datas;
        io_uring_cqe_seen(&ring, cqe);
    }
    do
    {
        loop_again = false;
        for (int i = 0; i < consumers.size(); i++)
        {
            consumers[i]->loop();
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
