// Metadata on-disk structures
// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#pragma once

#include "crc32c.h"

#define JOURNAL_MAGIC 0x4A33
#define JOURNAL_VERSION_V1 1
#define JOURNAL_VERSION_V2 2
#define JOURNAL_BUFFER_SIZE 4*1024*1024
#define JOURNAL_ENTRY_HEADER_SIZE 16

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

// crc32c comes first to ease calculation
struct __attribute__((__packed__)) journal_entry_start
{
    uint32_t crc32;
    uint16_t magic;
    uint16_t type;
    uint32_t size;
    uint32_t reserved;
    uint64_t journal_start;
    uint64_t version;
    uint32_t data_csum_type;
    uint32_t csum_block_size;
};
#define JE_START_V0_SIZE 24
#define JE_START_V1_SIZE 32
#define JE_START_V2_SIZE 40

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
    uint32_t crc32_data; // zero when data_csum_type != 0
    // small_write and big_write entries are followed by the "external" bitmap
    // its size is dynamic and included in journal entry's <size> field
    uint8_t bitmap[];
    // and then data checksums if data_csum_type != 0
    // uint32_t data_crc32c[];
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
    // small_write and big_write entries are followed by the "external" bitmap
    // its size is dynamic and included in journal entry's <size> field
    uint8_t bitmap[];
    // and then data checksums if data_csum_type != 0
    // uint32_t data_crc32c[];
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

// "VITAstor"
#define BLOCKSTORE_META_MAGIC_V1 0x726F747341544956l
#define BLOCKSTORE_META_FORMAT_V1 1
#define BLOCKSTORE_META_FORMAT_V2 2
#define BLOCKSTORE_META_FORMAT_HEAP 3

// metadata header (superblock)
struct __attribute__((__packed__)) blockstore_meta_header_v1_t
{
    uint64_t zero;
    uint64_t magic;
    uint64_t version;
    uint32_t meta_block_size;
    uint32_t data_block_size;
    uint32_t bitmap_granularity;
};

struct __attribute__((__packed__)) blockstore_meta_header_v2_t
{
    uint64_t zero;
    uint64_t magic;
    uint64_t version;
    uint32_t meta_block_size;
    uint32_t data_block_size;
    uint32_t bitmap_granularity;
    uint32_t data_csum_type;
    uint32_t csum_block_size;
    uint32_t header_csum;
};

struct __attribute__((__packed__)) blockstore_meta_header_v3_t
{
    uint64_t zero;
    uint64_t magic;
    uint64_t version;
    uint32_t meta_block_size;
    uint32_t data_block_size;
    uint32_t bitmap_granularity;
    uint32_t data_csum_type;
    uint32_t csum_block_size;
    uint32_t header_csum;
    uint64_t meta_area_size;
    uint64_t completed_lsn;

    void set_crc32c();
};

// 32 bytes = 24 bytes + block bitmap (4 bytes by default) + external attributes (also bitmap, 4 bytes by default)
// per "clean" entry on disk with fixed metadata tables
struct __attribute__((__packed__)) clean_disk_entry
{
    object_id oid;
    uint64_t version;
    uint8_t bitmap[];
    // Two more fields come after bitmap in metadata version 2:
    // uint32_t data_csum[];
    // uint32_t entry_csum;
};
