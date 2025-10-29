// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#pragma once

struct blockstore_init_meta_buf
{
    uint8_t *buf = NULL;
    uint64_t size = 0;
    uint64_t offset = 0;
    int state = 0;
};

class blockstore_init_meta
{
    blockstore_impl_t *bs;
    int wait_state = 0;
    int wait_count = 0;
    bool zero_on_init = false;
    void *metadata_buffer = NULL;
    blockstore_init_meta_buf bufs[2] = {};
    int submitted = 0;
    struct io_uring_sqe *sqe;
    struct ring_data_t *data;
    uint64_t md_offset = 0;
    uint64_t next_offset = 0;
    uint64_t last_read_offset = 0;
    uint64_t entries_loaded = 0;
    std::vector<uint32_t> recheck_mod;
    int i = 0, j = 0;
    bool handle_meta_block(uint8_t *buf, uint64_t count, uint64_t done_cnt);
    void handle_event(ring_data_t *data, int buf_num);
public:
    blockstore_init_meta(blockstore_impl_t *bs);
    int loop();
};
