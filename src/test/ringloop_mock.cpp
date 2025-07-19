// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include <random>

#include "ringloop_mock.h"
#include "malloc_or_die.h"

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

disk_mock_t::disk_mock_t(size_t size, bool buffered)
{
    this->size = size;
    this->data = (uint8_t*)malloc_or_die(size);
    this->buffered = buffered;
}

disk_mock_t::~disk_mock_t()
{
    discard_buffers(true, 0);
    free(data);
}

void disk_mock_t::erase_buffers(uint64_t begin, uint64_t end)
{
    for (auto it = buffers.upper_bound(begin); it != buffers.end(); )
    {
        const uint64_t bs = it->first - it->second.iov_len;
        const uint64_t be = it->first;
        if (bs >= end)
        {
            break;
        }
        if (bs >= begin && be <= end)
        {
            // Remove the whole buffer
            buffers.erase(it++);
        }
        else if (bs < begin && be > end)
        {
            // Cut beginning & end & stop
            uint8_t *ce = (uint8_t*)malloc_or_die(be-end);
            memcpy(ce, it->second.iov_base + (end-bs), be-end);
            uint8_t *cs = (uint8_t*)realloc(it->second.iov_base, begin-bs);
            if (!cs)
                throw std::bad_alloc();
            buffers[begin] = (iovec){ .iov_base = cs, .iov_len = begin-bs };
            buffers[be] = (iovec){ .iov_base = ce, .iov_len = be-end };
            break;
        }
        else if (bs < begin)
        {
            // Cut beginning
            uint8_t *cs = (uint8_t*)realloc(it->second.iov_base, begin-bs);
            if (!cs)
                throw std::bad_alloc();
            buffers[begin] = (iovec){ .iov_base = cs, .iov_len = begin-bs };
            buffers.erase(it++);
        }
        else
        {
            // Cut end & stop
            assert(be > end);
            uint8_t *ce = (uint8_t*)malloc_or_die(be-end);
            memcpy(ce, it->second.iov_base + (end-bs), be-end);
            buffers[be] = (iovec){ .iov_base = ce, .iov_len = be-end };
            buffers.erase(it);
            break;
        }
    }
}

void disk_mock_t::clear(size_t offset, size_t len)
{
    if (offset < size)
    {
        memset(data+offset, 0, len < size-offset ? len : size-offset);
    }
}

void disk_mock_t::discard_buffers(bool all, uint32_t seed)
{
    if (trace)
        printf("disk: discard buffers all=%d seed=%u\n", all, seed);
    if (all)
    {
        for (auto & b: buffers)
            free(b.second.iov_base);
        buffers.clear();
    }
    else
    {
        std::mt19937 rnd(seed);
        for (auto it = buffers.begin(); it != buffers.end(); )
        {
            if (rnd() < 0x80000000)
            {
                free(it->second.iov_base);
                buffers.erase(it++);
            }
            else
                it++;
        }
    }
}

ssize_t disk_mock_t::copy_from_sqe(io_uring_sqe *sqe, uint8_t *to, uint64_t base_offset)
{
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
            printf("disk: write %zu+%zu from %jx\n", off, cur, (uint64_t)v[i].iov_base);
        memcpy(to + off - base_offset, v[i].iov_base, cur);
        off += v[i].iov_len;
    }
    return off - sqe->off;
}

void disk_mock_t::read_item(uint8_t *to, uint64_t offset, uint64_t len)
{
    uint64_t last = offset;
    for (auto it = buffers.upper_bound(offset); it != buffers.end(); it++)
    {
        const uint64_t bs = it->first - it->second.iov_len;
        const uint64_t be = it->first;
        if (bs >= offset+len)
        {
            break;
        }
        if (last < bs)
        {
            // Fill the gap between buffers
            memcpy(to+last-offset, data+last, bs-last);
            last = bs;
        }
        if (last < offset)
        {
            last = offset;
        }
        uint64_t cur_end = be < offset+len ? be : offset+len;
        memcpy(to+last-offset, it->second.iov_base+last-bs, cur_end-last);
        last = be;
    }
    if (last < offset+len)
    {
        // Fill the gap in the end
        memcpy(to+last-offset, data+last, offset+len-last);
    }
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
            if (off < size)
            {
                size_t cur = (off + v[i].iov_len > size ? size-off : v[i].iov_len);
                if (trace)
                    printf("disk: read %zu+%zu to %jx\n", off, cur, (uint64_t)v[i].iov_base);
                if (buffers.size())
                    read_item((uint8_t*)v[i].iov_base, off, cur);
                else
                    memcpy(v[i].iov_base, data + off, cur);
            }
            off += v[i].iov_len;
        }
        userdata->res = off - sqe->off;
    }
    else if (sqe->opcode == IORING_OP_WRITEV)
    {
        uint64_t end = 0;
        if (buffered)
        {
            // Remove overwritten parts of buffers
            end = sqe->off;
            for (uint32_t i = 0; i < sqe->len; i++)
            {
                end += ((iovec*)sqe->addr)[i].iov_len;
            }
            erase_buffers(sqe->off, end);
        }
        if (!buffered || (sqe->rw_flags & RWF_DSYNC))
        {
            // Simple "immediate" mode
            userdata->res = copy_from_sqe(sqe, data, 0);
        }
        else
        {
            // Buffered mode
            uint8_t *buf = (uint8_t*)malloc_or_die(end - sqe->off);
            userdata->res = copy_from_sqe(sqe, buf, sqe->off);
            if (userdata->res == -EINVAL)
                free(buf);
            else
                buffers[end] = (iovec){ .iov_base = buf, .iov_len = end-sqe->off };
        }
    }
    else if (sqe->opcode == IORING_OP_FSYNC)
    {
        if (trace)
            printf("disk: fsync\n");
        if (buffers.size())
        {
            for (auto & b: buffers)
            {
                memcpy(data + b.first - b.second.iov_len, b.second.iov_base, b.second.iov_len);
                free(b.second.iov_base);
            }
            buffers.clear();
        }
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
    return true;
}
