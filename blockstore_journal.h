#pragma once

#include "crc32c.h"

#define MIN_JOURNAL_SIZE 4*1024*1024
#define JOURNAL_MAGIC 0x4A33
#define JOURNAL_BUFFER_SIZE 4*1024*1024

// Journal entries
// Journal entries are linked to each other by their crc32 value
// The journal is almost a blockchain, because object versions constantly increase
#define JE_START       0x01
#define JE_SMALL_WRITE 0x02
#define JE_BIG_WRITE   0x03
#define JE_STABLE      0x04
#define JE_DELETE      0x05

// crc32c comes first to ease calculation and is equal to crc32()
struct __attribute__((__packed__)) journal_entry_start
{
    uint32_t crc32;
    uint16_t magic;
    uint16_t type;
    uint32_t size;
    uint32_t crc32_replaced;
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
    // small_write entries contain <len> bytes of data, but data is stored in the next journal sector
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
        journal_entry_del del;
    };
};

inline uint32_t je_crc32(journal_entry *je)
{
    return crc32c_zero4(((uint8_t*)je)+4, je->size-4);
}

struct journal_sector_info_t
{
    uint64_t offset;
    uint64_t usage_count;
};

struct journal_t
{
    int fd;
    uint64_t device_size;

    uint64_t offset, len;
    uint64_t next_free = 512;
    uint64_t used_start = 512;
    uint32_t crc32_last = 0;

    // Current sector(s) used for writing
    uint8_t *sector_buf;
    journal_sector_info_t *sector_info;
    uint64_t sector_count;
    int cur_sector = 0;
    int in_sector_pos = 0;
};

struct blockstore_journal_check_t
{
    blockstore *bs;
    uint64_t next_pos, next_sector, next_in_pos;
    int sectors_required;

    blockstore_journal_check_t(blockstore *bs);
    int check_available(blockstore_operation *op, int required, int size, int data_after);
};

inline journal_entry* prefill_single_journal_entry(journal_t & journal, uint16_t type, uint32_t size)
{
    if (512 - journal.in_sector_pos < size)
    {
        // Move to the next journal sector
        // Also select next sector buffer in memory
        journal.cur_sector = ((journal.cur_sector + 1) % journal.sector_count);
        journal.sector_info[journal.cur_sector].offset = journal.next_free;
        journal.in_sector_pos = 0;
        journal.next_free = (journal.next_free+512) < journal.len ? journal.next_free + 512 : 512;
        memset(journal.sector_buf + 512*journal.cur_sector, 0, 512);
    }
    journal_entry *je = (struct journal_entry*)(
        journal.sector_buf + 512*journal.cur_sector + journal.in_sector_pos
    );
    journal.in_sector_pos += size;
    je->magic = JOURNAL_MAGIC;
    je->type = type;
    je->size = size;
    je->crc32_prev = journal.crc32_last;
    return je;
}

// FIXME: make inline
void prepare_journal_sector_write(journal_t & journal, io_uring_sqe *sqe, std::function<void(ring_data_t*)> cb);
