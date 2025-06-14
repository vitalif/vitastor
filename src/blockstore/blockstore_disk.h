// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#pragma once

#include <stdint.h>

#include <string>
#include <map>

// Memory alignment for direct I/O (usually 512 bytes)
#ifndef DIRECT_IO_ALIGNMENT
#define DIRECT_IO_ALIGNMENT 512
#endif

#define BLOCKSTORE_CSUM_NONE 0
// Lower byte of checksum type is its length
#define BLOCKSTORE_CSUM_CRC32C 0x104

class allocator_t;

struct blockstore_disk_t
{
    std::string data_device, meta_device, journal_device;
    uint32_t data_block_size;
    uint64_t cfg_journal_size, cfg_data_size;
    // Required write alignment and journal/metadata/data areas' location alignment
    uint32_t disk_alignment = 4096;
    // Journal block size - minimum_io_size of the journal device is the best choice
    uint64_t journal_block_size = 4096;
    // Metadata block size - minimum_io_size of the metadata device is the best choice
    uint64_t meta_block_size = 4096;
    // Target free space in metadata blocks
    uint32_t meta_block_target_free_space = 800;
    // Sparse write tracking granularity. 4 KB is a good choice. Must be a multiple of disk_alignment
    uint64_t bitmap_granularity = 4096;
    // Data checksum type, BLOCKSTORE_CSUM_NONE or BLOCKSTORE_CSUM_CRC32C
    uint32_t data_csum_type = BLOCKSTORE_CSUM_NONE;
    // Checksum block size, must be a multiple of bitmap_granularity
    uint32_t csum_block_size = 4096;
    // By default, Blockstore locks all opened devices exclusively. This option can be used to disable locking
    bool disable_flock = false;
    // I/O modes for data, metadata and journal: direct or "" = O_DIRECT, cached = O_SYNC, directsync = O_DIRECT|O_SYNC
    // O_SYNC without O_DIRECT = use Linux page cache for reads and writes
    std::string data_io, meta_io, journal_io;
    // Keep journal (buffered data) in memory?
    bool inmemory_meta = true;
    // Keep metadata in memory?
    bool inmemory_journal = true;
    // Data discard granularity and minimum size (for the sake of performance)
    bool discard_on_start = false;
    uint64_t min_discard_size = 1024*1024;
    uint64_t discard_granularity = 0;

    int meta_fd = -1, data_fd = -1, journal_fd = -1;
    uint64_t meta_offset, meta_device_sect, meta_device_size, meta_area_size, min_meta_len, meta_format = 0;
    uint64_t data_offset, data_device_sect, data_device_size, data_len;
    uint64_t journal_offset, journal_device_sect, journal_device_size, journal_len;

    uint32_t block_order = 0;
    uint64_t block_count = 0;
    uint32_t clean_entry_bitmap_size = 0;
    uint32_t clean_entry_size = 0, clean_dyn_size = 0; // for meta_v1/2

    void parse_config(std::map<std::string, std::string> & config);
    void open_data();
    void open_meta();
    void open_journal();
    void calc_lengths(bool skip_meta_check = false);
    void close_all();
    int trim_data(std::function<bool(uint64_t)> is_free);

    inline uint64_t dirty_dyn_size(uint64_t offset, uint64_t len)
    {
        // Checksums may be partial if write is not aligned with csum_block_size
        return clean_entry_bitmap_size + (csum_block_size && len > 0
            ? ((offset+len+csum_block_size-1)/csum_block_size - offset/csum_block_size)
                * (data_csum_type & 0xFF)
            : 0);
    }
};
