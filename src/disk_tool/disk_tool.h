// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#pragma once

#ifndef _LARGEFILE64_SOURCE
#define _LARGEFILE64_SOURCE
#endif

#include <map>
#include <set>
#include <vector>
#include <string>
#include <functional>

#include "json11/json11.hpp"
#include "blockstore_disk.h"
#include "blockstore.h"
#include "blockstore_heap.h"
#include "ondisk_formats.h"
#include "crc32c.h"
#include "allocator.h"

// vITADisk
#define VITASTOR_DISK_MAGIC 0x6b73694441544976
#define VITASTOR_DISK_MAX_SB_SIZE 128*1024
#define VITASTOR_PART_TYPE "e7009fac-a5a1-4d72-af72-53de13059903"
#define DEFAULT_HYBRID_JOURNAL "1G"
#define DEFAULT_HYBRID_SSD_JOURNAL "128M"

struct resizer_data_moving_t;

struct vitastor_dev_info_t
{
    std::string path;
    bool is_hdd;
    json11::Json pt; // pt = partition table
    int osd_part_count;
    uint64_t size;
    uint64_t free;
};

struct disk_tool_t
{
    /**** Parameters ****/

    std::map<std::string, std::string> options;
    bool test_mode = false;
    bool all = false, json = false, now = false;
    bool dump_with_blocks = false, dump_with_data = false;
    bool dump_as_old = false;
    int log_level = 1;
    blockstore_disk_t dsk;

    // resize data and/or move metadata and journal
    int iodepth;
    std::string new_meta_device, new_journal_device;
    uint64_t new_data_offset, new_data_len;
    uint64_t new_journal_offset, new_journal_len;
    uint64_t new_meta_offset, new_meta_len;

    /**** State ****/

    uint64_t meta_pos;
    uint64_t journal_pos, journal_calc_data_pos;

    uint8_t *buffer_area = NULL;
    bool first_block, first_entry;

    allocator_t *data_alloc = NULL;
    std::map<uint64_t, uint64_t> data_remap;
    std::map<uint64_t, uint64_t>::iterator remap_it;
    ring_loop_t *ringloop = NULL;
    ring_consumer_t ring_consumer;
    int remap_active;
    journal_entry_start je_start;
    uint8_t *new_journal_buf = NULL, *new_meta_buf = NULL, *new_journal_ptr = NULL, *new_journal_data = NULL;
    blockstore_meta_header_v3_t *new_meta_hdr = NULL;
    blockstore_disk_t new_dsk;
    blockstore_heap_t *new_heap = NULL;
    uint64_t new_journal_in_pos;
    int64_t data_idx_diff;
    uint64_t total_blocks, free_first, free_last;
    uint64_t new_clean_entry_bitmap_size, new_data_csum_size, new_clean_entry_size, new_entries_per_block;
    uint32_t new_meta_format = 0;
    int new_journal_fd = -1, new_meta_fd = -1;
    resizer_data_moving_t *moving_blocks = NULL;

    bool started;
    void *small_write_data = NULL;
    uint32_t data_crc32;
    bool data_csum_valid;
    uint32_t crc32_last;
    uint32_t new_crc32_prev;

    ~disk_tool_t();

    int dump_journal();
    void dump_journal_entry(int num, journal_entry *je, bool json);
    int process_journal(std::function<int(void*)> block_fn, bool do_open = true);
    int process_journal_block(void *buf, std::function<void(int, journal_entry*)> iter_fn);
    int process_meta(std::function<void(blockstore_meta_header_v3_t *)> hdr_fn,
        std::function<void(blockstore_heap_t *heap, heap_entry_t *obj, uint32_t meta_block_num)> obj_fn,
        std::function<void(uint64_t block_num, clean_disk_entry *entry_v1, uint8_t *bitmap)> record_fn,
        bool with_data, bool do_open);

    int dump_meta();
    void dump_meta_header(blockstore_meta_header_v3_t *hdr);
    void dump_meta_entry(uint64_t block_num, clean_disk_entry *entry, uint8_t *bitmap);
    void dump_heap_entry_as_old(blockstore_heap_t *heap, heap_entry_t *obj);
    void dump_heap_entry(blockstore_heap_t *heap, heap_entry_t *obj);

    int dump_load_check_superblock(const std::string & device);

    int write_json_journal(json11::Json entries);
    int write_json_meta(json11::Json meta);
    int write_json_heap(json11::Json meta, json11::Json journal);
    int index_journal_by_object(json11::Json journal,
        std::map<object_id, std::vector<json11::Json::object>> & journal_by_object);

    int resize_data(std::string device);
    int resize_parse_move_journal(std::map<std::string, std::string> & move_options, bool dry_run);
    int resize_parse_move_meta(std::map<std::string, std::string> & move_options, bool dry_run);

    int raw_resize();
    int resize_parse_params();
    void resize_init(blockstore_meta_header_v3_t *hdr);
    int resize_remap_blocks();
    int resize_copy_data();
    void resize_alloc_journal();
    void build_journal_start();
    void choose_journal_block(uint32_t je_size);
    int resize_rebuild_journal();
    int resize_write_new_journal();
    int resize_rebuild_meta();
    int resize_write_new_meta();
    void free_new_meta();

    int udev_import(std::string device);
    int read_sb(std::string device);
    int write_sb(std::string device);
    int update_sb(std::string device);
    int exec_osd(std::string device);
    int systemd_start_stop_osds(const std::vector<std::string> & cmd, const std::vector<std::string> & devices);
    int pre_exec_osd(std::string device);
    int purge_devices(const std::vector<std::string> & devices);
    int clear_osd_superblock(const std::string & dev);
    int trim_data(std::string device);

    json11::Json read_osd_superblock(std::string device, bool expect_exist = true, bool ignore_nonref = false);
    uint32_t write_osd_superblock(std::string device, json11::Json params);

    int prepare_one(std::map<std::string, std::string> options, int is_hdd, json11::Json::object & result);
    int check_existing_partition(std::string & dev_by_uuid);
    int fix_partition_type(std::string & dev_by_uuid);
    int prepare(std::vector<std::string> devices);
    std::vector<vitastor_dev_info_t> collect_devices(const std::vector<std::string> & devices);
    json11::Json add_partitions(vitastor_dev_info_t & devinfo, std::vector<std::string> sizes);
    std::vector<std::string> get_new_data_parts(vitastor_dev_info_t & dev,
        uint64_t osd_per_disk, uint64_t max_other_percent, uint64_t *check_new_count);
    int get_meta_partition(std::vector<vitastor_dev_info_t> & ssds, std::map<std::string, std::string> & options);

    int upgrade_simple_unit(std::string unit);
};

void disk_tool_simple_offsets(json11::Json cfg, bool json_output);

uint64_t sscanf_json(const char *fmt, const json11::Json & str);
void fromhexstr(const std::string & from, int bytes, uint8_t *to);
int disable_cache(std::string dev);
uint64_t get_device_size(const std::string & dev, bool should_exist = false);
std::string get_parent_device(std::string dev);
int shell_exec(const std::vector<std::string> & cmd, const std::string & in, std::string *out, std::string *err);
int write_zero(int fd, uint64_t offset, uint64_t size);
json11::Json read_parttable(std::string dev);
uint64_t dev_size_from_parttable(json11::Json pt);
uint64_t free_from_parttable(json11::Json pt);
int fix_partition_type_uuid(std::string & dev_by_uuid, const std::string & type_uuid);
std::string csum_type_str(uint32_t data_csum_type);
uint32_t csum_type_from_str(std::string data_csum_type);
