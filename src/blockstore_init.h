// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#pragma once

class blockstore_init_meta
{
    blockstore_impl_t *bs;
    int wait_state = 0;
    bool zero_on_init = false;
    void *metadata_buffer = NULL;
    uint64_t metadata_read = 0, md_offset = 0;
    int prev = 0, prev_done = 0, done_len = 0, submitted = 0;
    uint64_t done_cnt = 0, done_pos = 0;
    uint64_t entries_loaded = 0;
    struct io_uring_sqe *sqe;
    struct ring_data_t *data;
    bool handle_entries(void *entries, unsigned count, int block_order);
    void handle_event(ring_data_t *data);
public:
    blockstore_init_meta(blockstore_impl_t *bs);
    int loop();
};

struct bs_init_journal_done
{
    void *buf;
    uint64_t pos, len;
};

class blockstore_init_journal
{
    blockstore_impl_t *bs;
    int wait_state = 0, wait_count = 0, handle_res = 0;
    uint64_t entries_loaded = 0;
    uint32_t crc32_last = 0;
    bool started = false;
    uint64_t next_free;
    std::vector<bs_init_journal_done> done;
    std::vector<obj_ver_id> double_allocs;
    uint64_t journal_pos = 0;
    uint64_t continue_pos = 0;
    void *init_write_buf = NULL;
    uint64_t init_write_sector = 0;
    bool wrapped = false;
    void *submitted_buf;
    struct io_uring_sqe *sqe;
    struct ring_data_t *data;
    journal_entry_start *je_start;
    std::function<void(ring_data_t*)> simple_callback;
    int handle_journal_part(void *buf, uint64_t done_pos, uint64_t len);
    void handle_event(ring_data_t *data);
    void erase_dirty_object(blockstore_dirty_db_t::iterator dirty_it);
public:
    blockstore_init_journal(blockstore_impl_t* bs);
    int loop();
};
