// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#pragma once

#include "ringloop.h"
#include "timerfd_manager.h"
#include <errno.h>
#include <map>
#include <random>
#include <vector>

class io_sim_t;

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
    bool fake_full = false;
    bool in_loop = false;
    bool loop_again = false;
    bool support_zc = false;
    std::function<bool()> is_full;

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
    void set_fake_full(std::function<bool()> is_full);
    // Drop all queued and completed SQEs without running their callbacks and
    // return every ring_data_t to the free list, as if the process had died.
    // Callbacks point into the blockstore, so delete it before calling this.
    void reset();
};

// Fault injection settings for disk_mock_t. Only used in async mode, see disk_mock_t::set_sim().
struct disk_fault_opts_t
{
    // Completion delay is drawn uniformly from [min_latency, max_latency] microseconds.
    // A non-zero range is what makes completions arrive out of submission order.
    uint64_t min_latency = 10, max_latency = 200;
    uint64_t fsync_min_latency = 100, fsync_max_latency = 2000;
    // Probability of failing an operation instead of executing it, in parts per million.
    // Note that the blockstore treats most write errors as fatal, by design.
    uint32_t read_error_ppm = 0, write_error_ppm = 0, fsync_error_ppm = 0;
    int error_code = EIO;
    // Whether a write in flight during a power outage may land partially, torn at a
    // sector boundary. Turn it off to tell torn writes apart from lost cache contents
    bool tear_inflight_writes = true;
    // Abort if a write is submitted while another in-flight write overlaps its byte range.
    // A device may apply such a pair in either order, so relying on the newer one winning
    // is a bug - the older content can silently come back. Catching it at submission time
    // makes it deterministic instead of waiting for an unlucky completion order
    bool forbid_overlapping_writes = false;
};

struct disk_mock_op_t
{
    uint64_t seq;
    io_uring_sqe sqe;
};

class disk_mock_t
{
    uint8_t *data = NULL;
    std::map<uint64_t, iovec> buffers;
    std::string name;
    size_t size = 0;
    bool buffered = false;
    io_sim_t *sim = NULL;
    // Submitted, but not yet completed operations, keyed by completion time
    std::multimap<uint64_t, disk_mock_op_t> inflight;
    uint64_t op_seq = 0;
    // Which sectors were ever written, to find holes left by writes that never happened
    std::vector<bool> sector_written;

    void set_buffer(uint64_t end, uint8_t *buf, uint64_t len);
    void commit_buffers();
    void erase_buffers(uint64_t begin, uint64_t end);
    ssize_t copy_from_sqe(io_uring_sqe *sqe, uint8_t *to, uint64_t base_offset, uint64_t limit = UINT64_MAX);
    void read_item(uint8_t *to, uint64_t offset, uint64_t len);
    bool execute(io_uring_sqe *sqe);
public:
    bool trace = false;
    // Granularity of torn writes during a simulated power loss
    uint32_t sector_size = 4096;
    // Writes of at most this size are atomic: at a power outage they land whole or not at
    // all, never half. This is what NVMe reports as AWUPF, and the store relies on it for
    // in-place intent writes - see atomic_write_size in blockstore_disk
    uint32_t atomic_write_size = 4096;
    disk_fault_opts_t faults;
    uint64_t injected_errors = 0, completed_ops = 0, overlapping_writes = 0;

    disk_mock_t(const std::string & name, size_t size, bool buffered);
    ~disk_mock_t();
    void clear(size_t offset, size_t len);
    void discard_buffers(bool all, uint32_t seed);
    bool submit(io_uring_sqe *sqe);

    // Switch to async mode: submit() only queues the operation, and it's executed
    // and completed later by io_sim_t. In sync mode (the default) submit() executes
    // the operation immediately and the caller is responsible for mark_completed().
    void set_sim(io_sim_t *sim);
    bool has_inflight();
    // Completion time of the earliest in-flight operation, or UINT64_MAX if idle
    uint64_t next_completion();
    void complete_due(uint64_t now_us);
    // Simulate a power outage: in-flight writes land fully, partially or not at all,
    // and most of the volatile write cache is lost.
    void power_loss(std::mt19937 & rnd);
    // Offset of the first sector in [from, to) that was never written, or UINT64_MAX
    uint64_t first_unwritten(uint64_t from, uint64_t to);
};

// Discrete event simulator driving a ring_loop_mock_t, its disks and a virtual clock.
// One "event" is the completion of the earliest in-flight disk operation; time jumps
// straight to it, so a test runs as fast as the CPU allows no matter which latencies
// the disks are configured with. Everything is driven by a single seeded PRNG, so a
// failing run is replayed exactly by passing the same seed.
class io_sim_t
{
    ring_loop_mock_t *ringloop = NULL;
    timerfd_manager_t *tfd = NULL;
    std::vector<disk_mock_t*> disks;
public:
    uint64_t now_us = 0;
    std::mt19937 rnd;
    // How far to move the clock when nothing is in flight and only timers can make progress
    uint64_t idle_tick = 1000;

    io_sim_t(ring_loop_mock_t *ringloop, uint32_t seed);
    // The tfd must be constructed with a NULL set_fd_handler, i.e. be in virtual time mode
    void set_tfd(timerfd_manager_t *tfd);
    void add_disk(disk_mock_t *disk);
    void remove_disk(disk_mock_t *disk);
    uint64_t random(uint64_t min, uint64_t max);
    bool chance_ppm(uint32_t ppm);
    void mark_completed(ring_data_t *data);
    void advance(uint64_t micros);
    // Run one event. Returns false if nothing was in flight and only the clock moved
    bool has_inflight();
    bool step();
    // Step until cond() or until the step budget runs out. Returns cond()
    bool run_until(const std::function<bool()> & cond, uint64_t max_steps = 1000000);
    void power_loss();
};
