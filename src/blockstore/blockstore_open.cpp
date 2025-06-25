// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <sys/file.h>
#include <stdexcept>
#include "blockstore_impl.h"

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
    flusher_start_threshold = strtoull(config["flusher_start_threshold"].c_str(), NULL, 10);
    max_write_iodepth = strtoull(config["max_write_iodepth"].c_str(), NULL, 10);
    throttle_small_writes = config["throttle_small_writes"] == "true" || config["throttle_small_writes"] == "1" || config["throttle_small_writes"] == "yes";
    throttle_target_iops = strtoull(config["throttle_target_iops"].c_str(), NULL, 10);
    throttle_target_mbs = strtoull(config["throttle_target_mbs"].c_str(), NULL, 10);
    throttle_target_parallelism = strtoull(config["throttle_target_parallelism"].c_str(), NULL, 10);
    throttle_threshold_us = strtoull(config["throttle_threshold_us"].c_str(), NULL, 10);
    perfect_csum_update = config["perfect_csum_update"] == "true" || config["perfect_csum_update"] == "1" || config["perfect_csum_update"] == "yes";
    if (config["autosync_writes"] != "")
    {
        autosync_writes = strtoull(config["autosync_writes"].c_str(), NULL, 10);
    }
    if (!max_flusher_count)
    {
        max_flusher_count = 256;
    }
    if (!min_flusher_count)
    {
        min_flusher_count = 1;
    }
    if (!journal_trim_interval)
    {
        journal_trim_interval = 1024;
    }
    if (!flusher_start_threshold)
    {
        flusher_start_threshold = 32;
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
    if (config["immediate_commit"] == "all")
    {
        immediate_commit = IMMEDIATE_ALL;
    }
    else if (config["immediate_commit"] == "small")
    {
        immediate_commit = IMMEDIATE_SMALL;
    }
    metadata_buf_size = strtoull(config["meta_buf_size"].c_str(), NULL, 10);
    meta_write_recheck_parallelism = strtoull(config["meta_write_recheck_parallelism"].c_str(), NULL, 10);
    log_level = strtoull(config["log_level"].c_str(), NULL, 10);
    // Validate
    if (metadata_buf_size < 65536)
    {
        metadata_buf_size = 4*1024*1024;
    }
    if (!meta_write_recheck_parallelism)
    {
        meta_write_recheck_parallelism = 16;
    }
    if (immediate_commit != IMMEDIATE_NONE && !dsk.disable_journal_fsync)
    {
        throw std::runtime_error("immediate_commit requires disable_journal_fsync");
    }
    if (immediate_commit == IMMEDIATE_ALL && !dsk.disable_data_fsync)
    {
        throw std::runtime_error("immediate_commit=all requires disable_journal_fsync and disable_data_fsync");
    }
}
