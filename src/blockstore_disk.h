// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#pragma once

#include <stdint.h>

#include <string>
#include <map>

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
    // Sparse write tracking granularity. 4 KB is a good choice. Must be a multiple of disk_alignment
    uint64_t bitmap_granularity = 4096;
    // By default, Blockstore locks all opened devices exclusively. This option can be used to disable locking
    bool disable_flock = false;

    int meta_fd = -1, data_fd = -1, journal_fd = -1;
    uint64_t meta_offset, meta_device_sect, meta_device_size, meta_len;
    uint64_t data_offset, data_device_sect, data_device_size, data_len;
    uint64_t journal_offset, journal_device_sect, journal_device_size, journal_len;

    uint32_t block_order;
    uint64_t block_count;
    uint32_t clean_entry_bitmap_size = 0, clean_entry_size = 0;

    void parse_config(std::map<std::string, std::string> & config);
    void open_data();
    void open_meta();
    void open_journal();
    void calc_lengths(bool skip_meta_check = false);
    void close_all();
};
