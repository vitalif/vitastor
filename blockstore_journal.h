// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.0 (see README.md for details)

#pragma once

#include "crc32c.h"

#define MIN_JOURNAL_SIZE 4*1024*1024
#define JOURNAL_MAGIC 0x4A33
#define JOURNAL_BUFFER_SIZE 4*1024*1024

// We reserve some extra space for future stabilize requests during writes
// FIXME: This value should be dynamic i.e. Blockstore ideally shouldn't allow
// writing more than can be stabilized afterwards
#define JOURNAL_STABILIZE_RESERVATION 65536

// Journal entries
// Journal entries are linked to each other by their crc32 value
// The journal is almost a blockchain, because object versions constantly increase
#define JE_MIN         0x01
#define JE_START       0x01
#define JE_SMALL_WRITE 0x02
#define JE_BIG_WRITE   0x03
#define JE_STABLE      0x04
#define JE_DELETE      0x05
#define JE_ROLLBACK    0x06
#define JE_SMALL_WRITE_INSTANT 0x07
#define JE_BIG_WRITE_INSTANT   0x08
#define JE_MAX         0x08

// crc32c comes first to ease calculation and is equal to crc32()
struct __attribute__((__packed__)) journal_entry_start
{
    uint32_t crc32;
    uint16_t magic;
    uint16_t type;
    uint32_t size;
    uint32_t reserved;
    uint64_t journal_start;
};

struct __attribute__((__packed__)) journal_entry_small_write
{
    uint32_t crc32;
    uint16_t magic;
    uint16_t type;
    uint32_t size;
    uint32_t crc32_prev;
    object_id oid;
    uint64_t version;
    uint32_t offset;
    uint32_t len;
    // small_write entries contain <len> bytes of data which is stored in next sectors
    // data_offset is its offset within journal
    uint64_t data_offset;
    uint32_t crc32_data;
};

struct __attribute__((__packed__)) journal_entry_big_write
{
    uint32_t crc32;
    uint16_t magic;
    uint16_t type;
    uint32_t size;
    uint32_t crc32_prev;
    object_id oid;
    uint64_t version;
    uint32_t offset;
    uint32_t len;
    uint64_t location;
};

struct __attribute__((__packed__)) journal_entry_stable
{
    uint32_t crc32;
    uint16_t magic;
    uint16_t type;
    uint32_t size;
    uint32_t crc32_prev;
    object_id oid;
    uint64_t version;
};

struct __attribute__((__packed__)) journal_entry_rollback
{
    uint32_t crc32;
    uint16_t magic;
    uint16_t type;
    uint32_t size;
    uint32_t crc32_prev;
    object_id oid;
    uint64_t version;
};

struct __attribute__((__packed__)) journal_entry_del
{
    uint32_t crc32;
    uint16_t magic;
    uint16_t type;
    uint32_t size;
    uint32_t crc32_prev;
    object_id oid;
    uint64_t version;
};

struct __attribute__((__packed__)) journal_entry
{
    union
    {
        struct __attribute__((__packed__))
        {
            uint32_t crc32;
            uint16_t magic;
            uint16_t type;
            uint32_t size;
            uint32_t crc32_prev;
        };
        journal_entry_start start;
        journal_entry_small_write small_write;
        journal_entry_big_write big_write;
        journal_entry_stable stable;
        journal_entry_rollback rollback;
        journal_entry_del del;
    };
};

inline uint32_t je_crc32(journal_entry *je)
{
    // 0x48674bc7 = crc32(4 zero bytes)
    return crc32c(0x48674bc7, ((uint8_t*)je)+4, je->size-4);
}

struct journal_sector_info_t
{
    uint64_t offset;
    uint64_t usage_count;
    bool written;
    bool dirty;
};

struct journal_t
{
    int fd;
    uint64_t device_size;
    bool inmemory = false;
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

    // Used sector map
    // May use ~ 80 MB per 1 GB of used journal space in the worst case
    std::map<uint64_t, uint64_t> used_sectors;

    ~journal_t();
    bool trim();
};

struct blockstore_journal_check_t
{
    blockstore_impl_t *bs;
    uint64_t next_pos, next_sector, next_in_pos;
    int sectors_required, first_sector;
    bool right_dir; // writing to the end or the beginning of the ring buffer

    blockstore_journal_check_t(blockstore_impl_t *bs);
    int check_available(blockstore_op_t *op, int required, int size, int data_after);
};

journal_entry* prefill_single_journal_entry(journal_t & journal, uint16_t type, uint32_t size);

void prepare_journal_sector_write(journal_t & journal, int sector, io_uring_sqe *sqe, std::function<void(ring_data_t*)> cb);
