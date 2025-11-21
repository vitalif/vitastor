// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <sys/file.h>
#include "impl.h"

namespace v1 {

void blockstore_impl_t::parse_config(blockstore_config_t & config)
{
    return parse_config(config, false);
}

void blockstore_impl_t::parse_config(blockstore_config_t & config, bool init)
{
    // Online-configurable options:
    max_flusher_count = strtoull(config["max_flusher_count"].c_str(), NULL, 10);
    if (!max_flusher_count)
    {
        max_flusher_count = strtoull(config["flusher_count"].c_str(), NULL, 10);
    }
    min_flusher_count = strtoull(config["min_flusher_count"].c_str(), NULL, 10);
    journal_trim_interval = strtoull(config["journal_trim_interval"].c_str(), NULL, 10);
    max_write_iodepth = strtoull(config["max_write_iodepth"].c_str(), NULL, 10);
    throttle_small_writes = config["throttle_small_writes"] == "true" || config["throttle_small_writes"] == "1" || config["throttle_small_writes"] == "yes";
    throttle_target_iops = strtoull(config["throttle_target_iops"].c_str(), NULL, 10);
    throttle_target_mbs = strtoull(config["throttle_target_mbs"].c_str(), NULL, 10);
    throttle_target_parallelism = strtoull(config["throttle_target_parallelism"].c_str(), NULL, 10);
    throttle_threshold_us = strtoull(config["throttle_threshold_us"].c_str(), NULL, 10);
    if (config["autosync_writes"] != "")
    {
        autosync_writes = strtoull(config["autosync_writes"].c_str(), NULL, 10);
    }
    if (!max_flusher_count)
    {
        max_flusher_count = 256;
    }
    if (!min_flusher_count || journal.flush_journal)
    {
        min_flusher_count = 1;
    }
    if (!journal_trim_interval)
    {
        journal_trim_interval = 512;
    }
    if (!max_write_iodepth)
    {
        max_write_iodepth = 128;
    }
    if (!throttle_target_iops)
    {
        throttle_target_iops = 100;
    }
    if (!throttle_target_mbs)
    {
        throttle_target_mbs = 100;
    }
    if (!throttle_target_parallelism)
    {
        throttle_target_parallelism = 1;
    }
    if (!throttle_threshold_us)
    {
        throttle_threshold_us = 50;
    }
    if (!init)
    {
        return;
    }
    // Offline-configurable options:
    // Common disk options
    dsk.parse_config(config);
    // Parse
    if (config["readonly"] == "true" || config["readonly"] == "1" || config["readonly"] == "yes")
    {
        readonly = true;
    }
    if (config["disable_data_fsync"] == "true" || config["disable_data_fsync"] == "1" || config["disable_data_fsync"] == "yes")
    {
        disable_data_fsync = true;
    }
    if (config["disable_meta_fsync"] == "true" || config["disable_meta_fsync"] == "1" || config["disable_meta_fsync"] == "yes")
    {
        disable_meta_fsync = true;
    }
    if (config["disable_journal_fsync"] == "true" || config["disable_journal_fsync"] == "1" || config["disable_journal_fsync"] == "yes")
    {
        disable_journal_fsync = true;
    }
    if (config["flush_journal"] == "true" || config["flush_journal"] == "1" || config["flush_journal"] == "yes")
    {
        // Only flush journal and exit
        journal.flush_journal = true;
    }
    if (config["immediate_commit"] == "all")
    {
        immediate_commit = IMMEDIATE_ALL;
    }
    else if (config["immediate_commit"] == "small")
    {
        immediate_commit = IMMEDIATE_SMALL;
    }
    metadata_buf_size = strtoull(config["meta_buf_size"].c_str(), NULL, 10);
    inmemory_meta = config["inmemory_metadata"] != "false" && config["inmemory_metadata"] != "0" &&
        config["inmemory_metadata"] != "no";
    journal.sector_count = strtoull(config["journal_sector_buffer_count"].c_str(), NULL, 10);
    journal.no_same_sector_overwrites = config["journal_no_same_sector_overwrites"] == "true" ||
        config["journal_no_same_sector_overwrites"] == "1" || config["journal_no_same_sector_overwrites"] == "yes";
    journal.inmemory = config["inmemory_journal"] != "false" && config["inmemory_journal"] != "0" &&
        config["inmemory_journal"] != "no";
    log_level = strtoull(config["log_level"].c_str(), NULL, 10);
    // Validate
    if (journal.sector_count < 2)
    {
        journal.sector_count = 32;
    }
    if (metadata_buf_size < 65536)
    {
        metadata_buf_size = 4*1024*1024;
    }
    if (dsk.meta_device == dsk.data_device)
    {
        disable_meta_fsync = disable_data_fsync;
    }
    if (dsk.journal_device == dsk.meta_device)
    {
        disable_journal_fsync = disable_meta_fsync;
    }
    if (immediate_commit != IMMEDIATE_NONE && !disable_journal_fsync)
    {
        throw std::runtime_error("immediate_commit requires disable_journal_fsync");
    }
    if (immediate_commit == IMMEDIATE_ALL && !disable_data_fsync)
    {
        throw std::runtime_error("immediate_commit=all requires disable_journal_fsync and disable_data_fsync");
    }
    // init some fields
    journal.block_size = dsk.journal_block_size;
    journal.next_free = dsk.journal_block_size;
    journal.used_start = dsk.journal_block_size;
    // no free space because sector is initially unmapped
    journal.in_sector_pos = dsk.journal_block_size;
}

void blockstore_impl_t::calc_lengths()
{
    dsk.calc_lengths();
    journal.len = dsk.journal_len;
    journal.block_size = dsk.journal_block_size;
    journal.offset = dsk.journal_offset;
    if (inmemory_meta)
    {
        metadata_buffer = memalign(MEM_ALIGNMENT, dsk.meta_area_size);
        if (!metadata_buffer)
            throw std::runtime_error("Failed to allocate memory for the metadata ("+std::to_string(dsk.meta_area_size/1024/1024)+" MB)");
    }
    else if (dsk.clean_entry_bitmap_size || dsk.data_csum_type)
    {
        clean_bitmaps = (uint8_t*)malloc(dsk.block_count * 2 * dsk.clean_entry_bitmap_size);
        if (!clean_bitmaps)
        {
            throw std::runtime_error(
                "Failed to allocate memory for the metadata sparse write bitmap ("+
                std::to_string(dsk.block_count * 2 * dsk.clean_entry_bitmap_size / 1024 / 1024)+" MB)"
            );
        }
    }
    if (journal.inmemory)
    {
        journal.buffer = memalign(MEM_ALIGNMENT, journal.len);
        if (!journal.buffer)
            throw std::runtime_error("Failed to allocate memory for journal ("+std::to_string(journal.len/1024/1024)+" MB)");
    }
    else
    {
        journal.sector_buf = (uint8_t*)memalign(MEM_ALIGNMENT, journal.sector_count * dsk.journal_block_size);
        if (!journal.sector_buf)
            throw std::bad_alloc();
    }
    journal.sector_info = (journal_sector_info_t*)calloc(journal.sector_count, sizeof(journal_sector_info_t));
    if (!journal.sector_info)
    {
        throw std::bad_alloc();
    }
}

}
