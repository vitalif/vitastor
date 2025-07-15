// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include <stdlib.h>
#include <unistd.h>

#include <stdexcept>

#include <sys/eventfd.h>

#include "malloc_or_die.h"
#include "ringloop.h"

ring_loop_t::ring_loop_t(int qd, bool multithreaded, bool sqe128)
{
    mt = multithreaded;
    io_uring_params params = {};
    if (sqe128)
    {
        params.flags = IORING_SETUP_SQE128;
    }
    int ret = io_uring_queue_init_params(qd, &ring, &params);
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
    io_uring_set_iowait(&ring, false);
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
    unsigned inc = (1 << io_uring_sqe_shift(&ring));
    for (unsigned i = sqe_tail; i < ring.sq.sqe_tail; i += inc)
    {
        free_ring_data[free_ring_data_ptr++] = ((ring_data_t*)ring.sq.sqes[i & *ring.sq.kring_mask].user_data) - ring_datas;
    }
    ring.sq.sqe_tail = sqe_tail;
}

unsigned ring_loop_t::space_left()
{
    struct io_uring_sq *sq = &ring.sq;
    unsigned int head = io_uring_smp_load_acquire(sq->khead);
    unsigned int next = sq->sqe_tail + 1;
    int left = (*sq->kring_entries - (next - head)) >> io_uring_sqe_shift(&ring);
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

ring_loop_mock_t::ring_loop_mock_t(int qd, std::function<void(io_uring_sqe *)> submit_cb)
{
    this->submit_cb = std::move(submit_cb);
    sqes.resize(qd);
    ring_datas.resize(qd);
    free_ring_datas.reserve(qd);
    submit_ring_datas.reserve(qd);
    completed_ring_datas.reserve(qd);
    for (size_t i = 0; i < ring_datas.size(); i++)
    {
        free_ring_datas.push_back(ring_datas.data() + i);
    }
    in_loop = false;
}

void ring_loop_mock_t::register_consumer(ring_consumer_t *consumer)
{
    unregister_consumer(consumer);
    consumers.push_back(consumer);
}

void ring_loop_mock_t::unregister_consumer(ring_consumer_t *consumer)
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

void ring_loop_mock_t::wakeup()
{
    loop_again = true;
}

void ring_loop_mock_t::set_immediate(const std::function<void()> & cb)
{
    immediate_queue.push_back(cb);
    wakeup();
}

unsigned ring_loop_mock_t::space_left()
{
    return free_ring_datas.size();
}

bool ring_loop_mock_t::has_work()
{
    return loop_again;
}

bool ring_loop_mock_t::has_sendmsg_zc()
{
    return false;
}

int ring_loop_mock_t::register_eventfd()
{
    return -1;
}

io_uring_sqe* ring_loop_mock_t::get_sqe()
{
    if (free_ring_datas.size() == 0)
    {
        return NULL;
    }
    ring_data_t *d = free_ring_datas.back();
    free_ring_datas.pop_back();
    submit_ring_datas.push_back(d);
    io_uring_sqe *sqe = &sqes[d - ring_datas.data()];
    *sqe = { 0 };
    io_uring_sqe_set_data(sqe, d);
    return sqe;
}

int ring_loop_mock_t::submit()
{
    for (size_t i = 0; i < submit_ring_datas.size(); i++)
    {
        submit_cb(&sqes[submit_ring_datas[i] - ring_datas.data()]);
    }
    submit_ring_datas.clear();
    return 0;
}

int ring_loop_mock_t::wait()
{
    return 0;
}

unsigned ring_loop_mock_t::save()
{
    return submit_ring_datas.size();
}

void ring_loop_mock_t::restore(unsigned sqe_tail)
{
    while (submit_ring_datas.size() > sqe_tail)
    {
        free_ring_datas.push_back(submit_ring_datas.back());
        submit_ring_datas.pop_back();
    }
}

void ring_loop_mock_t::loop()
{
    if (in_loop)
    {
        return;
    }
    in_loop = true;
    submit();
    while (completed_ring_datas.size())
    {
        ring_data_t *d = completed_ring_datas.back();
        completed_ring_datas.pop_back();
        if (d->callback)
        {
            struct ring_data_t dl;
            dl.iov = d->iov;
            dl.res = d->res;
            dl.more = dl.prev = false;
            dl.callback.swap(d->callback);
            free_ring_datas.push_back(d);
            dl.callback(&dl);
        }
        else
        {
            fprintf(stderr, "Warning: empty callback in SQE\n");
            free_ring_datas.push_back(d);
        }
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

void ring_loop_mock_t::mark_completed(ring_data_t *data)
{
    completed_ring_datas.push_back(data);
    wakeup();
}

disk_mock_t::disk_mock_t(ring_loop_mock_t *loop, size_t size)
{
    this->loop = loop;
    this->size = size;
    this->data = (uint8_t*)malloc_or_die(size);
}

disk_mock_t::~disk_mock_t()
{
    free(data);
}

bool disk_mock_t::submit(io_uring_sqe *sqe)
{
    ring_data_t *userdata = (ring_data_t*)sqe->user_data;
    if (sqe->opcode == IORING_OP_READV)
    {
        size_t off = sqe->off;
        iovec *v = (iovec*)sqe->addr;
        size_t n = sqe->len;
        for (size_t i = 0; i < n; i++)
        {
            size_t cur = (off + v[i].iov_len > size ? size-off : v[i].iov_len);
            if (trace)
                printf("read %zu+%zu to %jx\n", off, cur, (uint64_t)v[i].iov_base);
            memcpy(v[i].iov_base, data + off, cur);
            off += v[i].iov_len;
        }
        userdata->res = off - sqe->off;
    }
    else if (sqe->opcode == IORING_OP_WRITEV)
    {
        // Simple "immediate" mode. We should also implement "buffered" mode
        size_t off = sqe->off;
        iovec *v = (iovec*)sqe->addr;
        size_t n = sqe->len;
        for (size_t i = 0; i < n; i++)
        {
            if (off >= size)
            {
                off = sqe->off - EINVAL; // :D
                break;
            }
            size_t cur = (off + v[i].iov_len > size ? size-off : v[i].iov_len);
            if (trace)
                printf("write %zu+%zu from %jx\n", off, cur, (uint64_t)v[i].iov_base);
            memcpy(data + off, v[i].iov_base, cur);
            off += v[i].iov_len;
        }
        userdata->res = off - sqe->off;
    }
    else if (sqe->opcode == IORING_OP_FSYNC)
    {
        userdata->res = 0;
    }
    else
    {
        return false;
    }
    // Execution variability should also be introduced:
    // 1) reads submitted in parallel to writes (not after completing the write) should return old or new data randomly
    // 2) parallel operation completions should be delivered in random order
    // 3) when fsync is enabled, write cache should be sometimes lost during a simulated power outage
    loop->mark_completed(userdata);
    return true;
}
