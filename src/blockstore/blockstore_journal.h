// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#pragma once

class blockstore_impl_t;

struct journal_sector_info_t
{
    uint64_t offset;
    uint64_t flush_count;
    bool written;
    bool dirty;
    uint64_t submit_id;
};

struct pending_journaling_t
{
    int pending;
    int sector;
    blockstore_op_t *op;
};

struct journal_t
{
    int fd;
    bool inmemory = false;
    bool flush_journal = false;
    void *buffer = NULL;

    uint64_t block_size;
    uint64_t offset, len;
    // Next free block offset
    uint64_t next_free = 0;
    // First occupied block offset
    uint64_t used_start = 0;
    // End of the last block not used for writing anymore
    uint64_t dirty_start = 0;
    uint32_t crc32_last = 0;

    // Current sector(s) used for writing
    void *sector_buf = NULL;
    journal_sector_info_t *sector_info = NULL;
    uint64_t sector_count;
    bool no_same_sector_overwrites = false;
    int cur_sector = 0;
    int in_sector_pos = 0;
    std::vector<int> submitting_sectors;
    std::multimap<uint64_t, pending_journaling_t> flushing_ops;
    uint64_t submit_id = 0;

    // Used sector map
    // May use ~ 80 MB per 1 GB of used journal space in the worst case
    std::map<uint64_t, uint64_t> used_sectors;

    ~journal_t();
    bool trim();
    uint64_t get_trim_pos();
    void dump_diagnostics();
    inline bool entry_fits(int size)
    {
        return !(block_size - in_sector_pos < size ||
            no_same_sector_overwrites && sector_info[cur_sector].written);
    }
};

struct blockstore_journal_check_t
{
    blockstore_impl_t *bs;
    uint64_t next_pos, next_sector, next_in_pos;
    int sectors_to_write, first_sector;
    bool right_dir; // writing to the end or the beginning of the ring buffer

    blockstore_journal_check_t(blockstore_impl_t *bs);
    int check_available(blockstore_op_t *op, int required, int size, int data_after);
};

journal_entry* prefill_single_journal_entry(journal_t & journal, uint16_t type, uint32_t size);
