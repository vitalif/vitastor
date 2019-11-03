#pragma once

class blockstore_init_meta
{
    blockstore *bs;
    uint8_t *metadata_buffer;
    uint64_t metadata_read = 0;
    struct iovec submit_iov;
    int prev = 0, prev_done = 0, done_len = 0, submitted = 0, done_cnt = 0;
public:
    blockstore_init_meta(blockstore* bs);
    int read_loop();
    void handle_entries(struct clean_disk_entry* entries, int count);
};

class blockstore_init_journal
{
    blockstore *bs;
    uint8_t *journal_buffer;
public:
    blockstore_init_journal(blockstore* bs);
    int read_loop();
};
