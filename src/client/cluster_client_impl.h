// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#pragma once

#include "cluster_client.h"

#define SCRAP_BUFFER_SIZE 4*1024*1024
#define PART_SENT 1
#define PART_DONE 2
#define PART_ERROR 4
#define PART_RETRY 8
#define CACHE_DIRTY 1
#define CACHE_WRITTEN 2
#define CACHE_FLUSHING 3
#define CACHE_REPEATING 4
#define OP_FLUSH_BUFFER 0x02
#define OP_IMMEDIATE_COMMIT 0x04

struct cluster_buffer_t
{
    uint8_t *buf;
    uint64_t len;
    int state;
    uint64_t flush_id;
    uint64_t *refcnt;
};

typedef std::map<object_id, cluster_buffer_t>::iterator dirty_buf_it_t;

class writeback_cache_t
{
public:
    uint64_t writeback_bytes = 0;
    int writeback_queue_size = 0;
    int writebacks_active = 0;
    uint64_t last_flush_id = 0;

    std::map<object_id, cluster_buffer_t> dirty_buffers;
    std::vector<cluster_op_t*> writeback_overflow;
    std::vector<object_id> writeback_queue;
    std::multimap<uint64_t, uint64_t*> flushed_buffers; // flush_id => refcnt

    ~writeback_cache_t();
    dirty_buf_it_t find_dirty(uint64_t inode, uint64_t offset);
    bool is_left_merged(dirty_buf_it_t dirty_it);
    bool is_right_merged(dirty_buf_it_t dirty_it);
    void copy_write(cluster_op_t *op, int state, uint64_t new_flush_id = 0);
    int repeat_ops_for(cluster_client_t *cli, osd_num_t peer_osd, pool_id_t pool_id, pg_num_t pg_num);
    void start_writebacks(cluster_client_t *cli, int count);
    bool read_from_cache(cluster_op_t *op, uint32_t bitmap_granularity);
    void flush_buffers(cluster_client_t *cli, dirty_buf_it_t from_it, dirty_buf_it_t to_it);
    void mark_flush_written(uint64_t inode, uint64_t offset, uint64_t len, uint64_t flush_id);
    void fsync_start();
    void fsync_error();
    void fsync_ok();
};
