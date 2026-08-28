// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include <assert.h>
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
    if (free_ring_datas.size() == 0 || is_full && is_full())
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

void ring_loop_mock_t::reset()
{
    assert(!in_loop);
    submit_ring_datas.clear();
    completed_ring_datas.clear();
    immediate_queue.clear();
    immediate_queue2.clear();
    free_ring_datas.clear();
    for (size_t i = 0; i < ring_datas.size(); i++)
    {
        ring_datas[i].callback = NULL;
        free_ring_datas.push_back(ring_datas.data() + i);
    }
    loop_again = false;
}

disk_mock_t::disk_mock_t(const std::string & name, size_t size, bool buffered)
{
    this->name = name;
    this->size = size;
    this->data = (uint8_t*)malloc_or_die(size);
    this->buffered = buffered;
    memset(this->data, 0, size);
}

disk_mock_t::~disk_mock_t()
{
    discard_buffers(true, 0);
    free(data);
}

// Store a buffer covering [end-len, end). Frees whatever was registered under the same
// key before, so that callers can't silently drop a buffer by overwriting the entry
void disk_mock_t::set_buffer(uint64_t end, uint8_t *buf, uint64_t len)
{
    auto it = buffers.find(end);
    if (it != buffers.end())
    {
        free(it->second.iov_base);
        it->second = (iovec){ .iov_base = buf, .iov_len = len };
    }
    else
        buffers[end] = (iovec){ .iov_base = buf, .iov_len = len };
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
            free(it->second.iov_base);
            buffers.erase(it++);
        }
        else if (bs < begin && be > end)
        {
            // Cut beginning & end & stop
            uint8_t *ce = (uint8_t*)malloc_or_die(be-end);
            memcpy(ce, (uint8_t*)it->second.iov_base + (end-bs), be-end);
            uint8_t *cs = (uint8_t*)realloc(it->second.iov_base, begin-bs);
            if (!cs)
                throw std::bad_alloc();
            // realloc has consumed the old pointer, so overwrite the entry (which keeps
            // the key `be`) before touching the map, or set_buffer would free it again
            it->second = (iovec){ .iov_base = ce, .iov_len = be-end };
            set_buffer(begin, cs, begin-bs);
            break;
        }
        else if (bs < begin)
        {
            // Cut beginning
            uint8_t *cs = (uint8_t*)realloc(it->second.iov_base, begin-bs);
            if (!cs)
                throw std::bad_alloc();
            // Same here - the old pointer is gone, drop it before erasing the entry
            it->second.iov_base = NULL;
            buffers.erase(it++);
            set_buffer(begin, cs, begin-bs);
        }
        else
        {
            // Cut end & stop
            assert(be > end);
            uint8_t *ce = (uint8_t*)malloc_or_die(be-end);
            memcpy(ce, (uint8_t*)it->second.iov_base + (end-bs), be-end);
            // The key stays the same, so replace the entry in place. Storing it and then
            // erasing `it` would drop the tail and leak it, because `it` points at key `be`
            set_buffer(be, ce, be-end);
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
    if (all)
    {
        if (trace)
            printf("%s: discard all buffers (%zu)\n", name.c_str(), buffers.size());
        for (auto & b: buffers)
            free(b.second.iov_base);
        buffers.clear();
    }
    else
    {
        if (trace)
            printf("%s: discard random buffers seed=%u\n", name.c_str(), seed);
        std::mt19937 rnd(seed);
        for (auto it = buffers.begin(); it != buffers.end(); )
        {
            if (rnd() < 0x80000000)
            {
                if (trace)
                    // the map is keyed by the end offset, print the usual offset+length instead
                    printf("%s:   dropping cached write %ju+%zu\n", name.c_str(), it->first - it->second.iov_len, it->second.iov_len);
                free(it->second.iov_base);
                buffers.erase(it++);
            }
            else
                it++;
        }
    }
}

ssize_t disk_mock_t::copy_from_sqe(io_uring_sqe *sqe, uint8_t *to, uint64_t base_offset, uint64_t limit)
{
    size_t off = sqe->off;
    iovec *v = (iovec*)sqe->addr;
    size_t n = sqe->len;
    size_t done = 0;
    for (size_t i = 0; i < n && done < limit; i++)
    {
        if (off >= size)
        {
            off = sqe->off - EINVAL; // :D
            break;
        }
        size_t cur = (off + v[i].iov_len > size ? size-off : v[i].iov_len);
        if (cur > limit-done)
            cur = limit-done;
        if (trace)
            printf("%s: write %zu+%zu from %jx\n", name.c_str(), off, cur, (uint64_t)v[i].iov_base);
        memcpy(to + off - base_offset, v[i].iov_base, cur);
        off += v[i].iov_len;
        done += cur;
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
        memcpy(to+last-offset, (uint8_t*)it->second.iov_base+last-bs, cur_end-last);
        last = be;
    }
    if (last < offset+len)
    {
        // Fill the gap in the end
        memcpy(to+last-offset, data+last, offset+len-last);
    }
}

bool disk_mock_t::execute(io_uring_sqe *sqe)
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
                    printf("%s: read %zu+%zu to %jx\n", name.c_str(), off, cur, (uint64_t)v[i].iov_base);
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
        {
            uint64_t wlen = 0;
            for (uint32_t i = 0; i < sqe->len; i++)
                wlen += ((iovec*)sqe->addr)[i].iov_len;
            if (sector_written.size() < size/sector_size)
                sector_written.resize(size/sector_size, false);
            for (uint64_t o = sqe->off/sector_size; o < (sqe->off+wlen+sector_size-1)/sector_size && o < sector_written.size(); o++)
                sector_written[o] = true;
        }
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
                set_buffer(end, buf, end-sqe->off);
        }
    }
    else if (sqe->opcode == IORING_OP_FSYNC)
    {
        if (trace)
            printf("%s: fsync\n", name.c_str());
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
    return true;
}

// In async mode the operation is executed at completion time, not at submission time.
// That single fact gives us most of the execution variability we want for free: a read
// overlapping an in-flight write returns old or new data depending on which of the two
// the simulator happens to complete first, and completions arrive out of submission order.
bool disk_mock_t::submit(io_uring_sqe *sqe)
{
    if (!sim)
    {
        return execute(sqe);
    }
    if (sqe->opcode != IORING_OP_READV && sqe->opcode != IORING_OP_WRITEV && sqe->opcode != IORING_OP_FSYNC)
    {
        return false;
    }
    if (faults.forbid_overlapping_writes && sqe->opcode == IORING_OP_WRITEV)
    {
        uint64_t len = 0;
        for (uint32_t i = 0; i < sqe->len; i++)
            len += ((iovec*)sqe->addr)[i].iov_len;
        for (auto & p: inflight)
        {
            auto & other = p.second.sqe;
            if (other.opcode != IORING_OP_WRITEV)
                continue;
            uint64_t olen = 0;
            for (uint32_t i = 0; i < other.len; i++)
                olen += ((iovec*)other.addr)[i].iov_len;
            if (sqe->off < other.off+olen && other.off < sqe->off+len)
            {
                overlapping_writes++;
                fprintf(stderr, "%s: overlapping concurrent writes: %ju+%ju submitted while %ju+%ju"
                    " is still in flight. The device may apply them in either order, so the older"
                    " content can win and silently revert the newer one\n",
                    name.c_str(), (uint64_t)sqe->off, len, (uint64_t)other.off, olen);
                abort();
            }
        }
    }
    uint64_t lat = sqe->opcode == IORING_OP_FSYNC
        ? sim->random(faults.fsync_min_latency, faults.fsync_max_latency)
        : sim->random(faults.min_latency, faults.max_latency);
    if (trace)
        printf("%s: submit opcode=%u off=%ju, completing in %juus\n", name.c_str(), sqe->opcode, (uint64_t)sqe->off, lat);
    inflight.insert(std::make_pair(sim->now_us + lat, (disk_mock_op_t){ .seq = op_seq++, .sqe = *sqe }));
    return true;
}

void disk_mock_t::set_sim(io_sim_t *sim)
{
    this->sim = sim;
}

bool disk_mock_t::has_inflight()
{
    return inflight.size() > 0;
}

uint64_t disk_mock_t::next_completion()
{
    return inflight.size() ? inflight.begin()->first : UINT64_MAX;
}

void disk_mock_t::complete_due(uint64_t now_us)
{
    while (inflight.size() && inflight.begin()->first <= now_us)
    {
        auto sqe = inflight.begin()->second.sqe;
        inflight.erase(inflight.begin());
        ring_data_t *userdata = (ring_data_t*)sqe.user_data;
        uint32_t err_ppm = sqe.opcode == IORING_OP_READV ? faults.read_error_ppm
            : (sqe.opcode == IORING_OP_WRITEV ? faults.write_error_ppm : faults.fsync_error_ppm);
        if (err_ppm && sim->chance_ppm(err_ppm))
        {
            if (trace)
                printf("%s: injecting error %d into opcode=%u off=%ju\n", name.c_str(), faults.error_code, sqe.opcode, (uint64_t)sqe.off);
            userdata->res = -faults.error_code;
            injected_errors++;
        }
        else
        {
            bool ok = execute(&sqe);
            assert(ok);
        }
        completed_ops++;
        sim->mark_completed(userdata);
    }
}

void disk_mock_t::power_loss(std::mt19937 & rnd)
{
    for (auto & p: inflight)
    {
        auto & sqe = p.second.sqe;
        if (sqe.opcode != IORING_OP_WRITEV)
            continue;
        uint64_t len = 0;
        for (uint32_t i = 0; i < sqe.len; i++)
            len += ((iovec*)sqe.addr)[i].iov_len;
        // A write in flight during a power outage may have reached the device fully,
        // partially (torn at a sector boundary), or not at all
        uint64_t limit = 0;
        switch (rnd() % (faults.tear_inflight_writes ? 3 : 2))
        {
        case 0:
            limit = 0;
            break;
        case 1:
            limit = len;
            break;
        default:
            limit = (uint64_t)(rnd() % (len/sector_size + 1)) * sector_size;
            break;
        }
        if (!limit)
            continue;
        if (trace)
            printf("%s: power loss: applying %ju/%ju bytes of in-flight write at %ju\n", name.c_str(), limit, len, (uint64_t)sqe.off);
        if (buffered && !(sqe.rw_flags & RWF_DSYNC))
        {
            uint8_t *buf = (uint8_t*)malloc_or_die(limit);
            copy_from_sqe(&sqe, buf, sqe.off, limit);
            erase_buffers(sqe.off, sqe.off+limit);
            set_buffer(sqe.off+limit, buf, limit);
        }
        else
            copy_from_sqe(&sqe, data, 0, limit);
    }
    inflight.clear();
    // Whatever was still in the volatile write cache is lost, except the part
    // the device happened to flush on its own before the power went away
    discard_buffers(false, (uint32_t)rnd());
}

uint64_t disk_mock_t::first_unwritten(uint64_t from, uint64_t to)
{
    for (uint64_t o = from/sector_size; o < (to+sector_size-1)/sector_size; o++)
        if (o >= sector_written.size() || !sector_written[o])
            return o*sector_size;
    return UINT64_MAX;
}

void ring_loop_mock_t::set_fake_full(std::function<bool()> is_full)
{
    this->is_full = is_full;
}

io_sim_t::io_sim_t(ring_loop_mock_t *ringloop, uint32_t seed)
{
    this->ringloop = ringloop;
    this->rnd.seed(seed);
}

void io_sim_t::set_tfd(timerfd_manager_t *tfd)
{
    this->tfd = tfd;
}

void io_sim_t::add_disk(disk_mock_t *disk)
{
    remove_disk(disk);
    disks.push_back(disk);
    disk->set_sim(this);
}

void io_sim_t::remove_disk(disk_mock_t *disk)
{
    for (size_t i = 0; i < disks.size(); i++)
    {
        if (disks[i] == disk)
        {
            disks.erase(disks.begin()+i, disks.begin()+i+1);
            break;
        }
    }
}

uint64_t io_sim_t::random(uint64_t min, uint64_t max)
{
    return max <= min ? min : min + rnd() % (max-min+1);
}

bool io_sim_t::chance_ppm(uint32_t ppm)
{
    return (rnd() % 1000000) < ppm;
}

void io_sim_t::mark_completed(ring_data_t *data)
{
    ringloop->mark_completed(data);
}

void io_sim_t::advance(uint64_t micros)
{
    if (!micros)
        return;
    now_us += micros;
    if (tfd)
        tfd->tick((timespec){ .tv_sec = (time_t)(micros/1000000), .tv_nsec = (long)((micros%1000000)*1000) });
}

bool io_sim_t::has_inflight()
{
    for (auto disk: disks)
        if (disk->has_inflight())
            return true;
    return false;
}

bool io_sim_t::step()
{
    ringloop->loop();
    uint64_t next = UINT64_MAX;
    for (auto disk: disks)
    {
        uint64_t t = disk->next_completion();
        if (t < next)
            next = t;
    }
    if (next == UINT64_MAX)
    {
        // Nothing in flight, only timers can move things forward
        advance(idle_tick);
        return false;
    }
    advance(next > now_us ? next - now_us : 0);
    for (auto disk: disks)
        disk->complete_due(now_us);
    return true;
}

bool io_sim_t::run_until(const std::function<bool()> & cond, uint64_t max_steps)
{
    for (uint64_t i = 0; i < max_steps; i++)
    {
        if (cond())
            return true;
        step();
    }
    ringloop->loop();
    return cond();
}

void io_sim_t::power_loss()
{
    for (auto disk: disks)
        disk->power_loss(rnd);
}
