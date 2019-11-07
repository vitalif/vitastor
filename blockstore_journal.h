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
    uint64_t block;
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
    uint64_t cur_sector = 0;
    uint64_t in_sector_pos = 0;
};
