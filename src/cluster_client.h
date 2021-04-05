// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#pragma once

#include "messenger.h"
#include "etcd_state_client.h"

#define MIN_BLOCK_SIZE 4*1024
#define MAX_BLOCK_SIZE 128*1024*1024
#define DEFAULT_DISK_ALIGNMENT 4096
#define DEFAULT_BITMAP_GRANULARITY 4096
#define DEFAULT_CLIENT_MAX_DIRTY_BYTES 32*1024*1024
#define DEFAULT_CLIENT_MAX_DIRTY_OPS 1024

struct cluster_op_t;

struct cluster_op_part_t
{
    cluster_op_t *parent;
    uint64_t offset;
    uint32_t len;
    pg_num_t pg_num;
    osd_num_t osd_num;
    osd_op_buf_list_t iov;
    unsigned flags;
    osd_op_t op;
};

struct cluster_op_t
{
    uint64_t opcode; // OSD_OP_READ, OSD_OP_WRITE, OSD_OP_SYNC
    uint64_t inode;
    uint64_t offset;
    uint64_t len;
    int retval;
    osd_op_buf_list_t iov;
    std::function<void(cluster_op_t*)> callback;
protected:
    int flags = 0;
    int state = 0;
    void *buf = NULL;
    cluster_op_t *orig_op = NULL;
    bool needs_reslice = false;
    bool up_wait = false;
    int inflight_count = 0, done_count = 0;
    std::vector<cluster_op_part_t> parts;
    friend class cluster_client_t;
};

struct cluster_buffer_t
{
    void *buf;
    uint64_t len;
    int state;
};

// FIXME: Split into public and private interfaces
class cluster_client_t
{
    timerfd_manager_t *tfd;
    ring_loop_t *ringloop;

    uint64_t bs_block_size = 0;
    uint64_t bs_bitmap_granularity = 0;
    std::map<pool_id_t, uint64_t> pg_counts;
    bool immediate_commit = false;
    // FIXME: Implement inmemory_commit mode. Note that it requires to return overlapping reads from memory.
    uint64_t client_max_dirty_bytes = 0;
    uint64_t client_max_dirty_ops = 0;
    int log_level;
    int up_wait_retry_interval = 500; // ms

    int retry_timeout_id = 0;
    uint64_t op_id = 1;
    std::vector<cluster_op_t*> offline_ops;
    std::deque<cluster_op_t*> op_queue;
    std::map<object_id, cluster_buffer_t> dirty_buffers;
    std::set<osd_num_t> dirty_osds;
    uint64_t dirty_bytes = 0, dirty_ops = 0;

    bool pgs_loaded = false;
    ring_consumer_t consumer;
    std::vector<std::function<void(void)>> on_ready_hooks;

public:
    etcd_state_client_t st_cli;
    osd_messenger_t msgr;
    json11::Json config;

    cluster_client_t(ring_loop_t *ringloop, timerfd_manager_t *tfd, json11::Json & config);
    ~cluster_client_t();
    void execute(cluster_op_t *op);
    bool is_ready();
    void on_ready(std::function<void(void)> fn);

    static void copy_write(cluster_op_t *op, std::map<object_id, cluster_buffer_t> & dirty_buffers);
    void continue_ops(bool up_retry = false);
protected:
    bool affects_osd(uint64_t inode, uint64_t offset, uint64_t len, osd_num_t osd);
    void flush_buffer(const object_id & oid, cluster_buffer_t & wr);
    void on_load_config_hook(json11::Json::object & config);
    void on_load_pgs_hook(bool success);
    void on_change_hook(json11::Json::object & changes);
    void on_change_osd_state_hook(uint64_t peer_osd);
    int continue_rw(cluster_op_t *op);
    void slice_rw(cluster_op_t *op);
    bool try_send(cluster_op_t *op, int i);
    int continue_sync(cluster_op_t *op);
    void send_sync(cluster_op_t *op, cluster_op_part_t *part);
    void handle_op_part(cluster_op_part_t *part);
};
