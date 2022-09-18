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
    bool zero_on_init = false;
    void *metadata_buffer = NULL;
    blockstore_init_meta_buf bufs[2] = {};
    int submitted = 0;
    struct io_uring_sqe *sqe;
    struct ring_data_t *data;
    uint64_t md_offset = 0;
    uint64_t next_offset = 0;
    uint64_t entries_loaded = 0;
    unsigned entries_per_block = 0;
    int i = 0, j = 0;
    std::vector<uint64_t> entries_to_zero;
    bool handle_meta_block(uint8_t *buf, uint64_t count, uint64_t done_cnt);
    void handle_event(ring_data_t *data, int buf_num);
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
    std::vector<iovec> small_write_data;
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
