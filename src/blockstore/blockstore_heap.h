// Metadata storage version 3 ("heap")
// Copyright (c) Vitaliy Filippov, 2025+
// License: VNPL-1.1 (see README.md for details)

#pragma once

#include <map>
#include <set>
#include <deque>
#include <vector>

#include "../client/object_id.h"
#include "../../cpp-btree/btree_map.h"
#include "blockstore_disk.h"

struct pool_shard_settings_t
{
    uint32_t pg_count;
    uint32_t pg_stripe_size;
};

#define BS_HEAP_TYPE 3
#define BS_HEAP_SMALL_WRITE 1
#define BS_HEAP_BIG_WRITE 2
#define BS_HEAP_TOMBSTONE 3
#define BS_HEAP_STABLE 4

class blockstore_heap_t;

struct __attribute__((__packed__)) heap_write_t
{
    // size should have top bit cleared
    uint16_t size = 0;
    int16_t next_pos = 0;
    uint64_t lsn = 0;
    uint64_t version = 0;
    uint32_t offset = 0;
    uint32_t len = 0;
    uint64_t location = 0;
    uint8_t flags = 0; // 1|2|3 = small|big|tombstone, 4|0 = stable|unstable

    // uint8_t[] external_bitmap
    // uint8_t[] internal_bitmap
    // uint32_t[] checksums

    heap_write_t *next();
    uint32_t get_size(blockstore_heap_t *heap);
    uint32_t get_csum_size(blockstore_heap_t *heap);
    bool needs_recheck(blockstore_heap_t *heap);
    bool needs_compact(uint64_t compacted_lsn);
    bool is_compacted(uint64_t compacted_lsn);
    bool can_be_collapsed(blockstore_heap_t *heap);
    bool is_allowed_before_compacted(uint64_t compacted_lsn, bool is_last_entry);
    uint8_t *get_ext_bitmap(blockstore_heap_t *heap);
    uint8_t *get_int_bitmap(blockstore_heap_t *heap);
    uint8_t *get_checksums(blockstore_heap_t *heap);
    uint32_t *get_checksum(blockstore_heap_t *heap);
};

struct __attribute__((__packed__)) heap_object_t
{
    // size should have top bit cleared
    uint16_t size = 0;
    // linked list of write entries...
    // newest entries are stored first to simplify scanning
    int16_t write_pos = 0;
    uint32_t crc32c = 0;
    uint64_t inode = 0;
    uint64_t stripe = 0;

    heap_write_t *get_writes();
    uint32_t calc_crc32c();
};

struct heap_object_lsn_t
{
    object_id oid;
    uint64_t lsn;
};

inline bool operator < (const heap_object_lsn_t & a, const heap_object_lsn_t & b)
{
    return a.oid < b.oid || a.oid == b.oid && a.lsn < b.lsn;
}

struct heap_object_mvcc_t
{
    uint32_t readers = 0;
    heap_object_t *entry_copy = NULL;
};

struct __attribute__((__packed__)) heap_block_info_t
{
    uint32_t used_space = 0;
    uint32_t free_pos = 0;
    uint8_t *data = NULL;
};

struct __attribute__((__packed__)) heap_block_free_t
{
    uint32_t block_num = 0;
    uint32_t free_space = 0;
};

inline bool operator < (const heap_block_free_t & a, const heap_block_free_t & b)
{
    return a.free_space > b.free_space || a.free_space == b.free_space && a.block_num < b.block_num;
}

struct multilist_alloc_t
{
    const uint32_t count, maxn;
    std::vector<int32_t> sizes;
    std::vector<uint32_t> nexts, prevs, heads;

    multilist_alloc_t(uint32_t count, uint32_t maxn);
    bool is_free(uint32_t pos);
    uint32_t allocate(uint32_t size);
    uint32_t find(uint32_t size);
    void use_full(uint32_t pos);
    void use(uint32_t pos, uint32_t size);
    void do_free(uint32_t pos);
    void free(uint32_t pos);
#ifdef MULTILIST_TEST
    void print();
#endif
};

class blockstore_heap_t
{
    friend class heap_write_t;
    friend class heap_object_t;

    blockstore_disk_t *dsk = NULL;
    uint8_t* buffer_area = NULL;
    bool fail_on_warn = false;
    int log_level = 0;

    const uint32_t meta_block_count = 0;
    uint32_t target_block_free_space = 800;

    uint64_t next_lsn = 0;
    uint64_t compacted_lsn = 0;
    std::map<pool_id_t, pool_shard_settings_t> pool_shard_settings;
    // PG => inode => stripe => block number
    std::map<uint64_t, std::map<inode_t, btree::btree_map<uint64_t, uint64_t>>> block_index;
    std::deque<object_id> compact_queue;
    std::vector<heap_block_info_t> block_info;
    allocator_t *data_alloc = NULL;
    allocator_t *meta_alloc = NULL;
    uint32_t meta_alloc_count = 0;
    uint64_t meta_used_space = 0;
    multilist_alloc_t *buffer_alloc = NULL;
    std::set<heap_block_free_t> used_alloc_queue;
    std::map<heap_object_lsn_t, heap_object_mvcc_t> object_mvcc;
    std::map<uint64_t, uint32_t> mvcc_data_refs;
    std::map<uint64_t, uint32_t> mvcc_buffer_refs;
    std::map<uint64_t, uint64_t> inode_space_stats;
    uint64_t buffer_area_used_space = 0;
    uint64_t data_used_space = 0;

    std::deque<object_id> recheck_queue;
    int recheck_in_progress = 0;
    bool in_recheck = false;
    std::function<void(uint64_t, uint64_t, uint8_t*, std::function<void()>)> recheck_cb;
    int recheck_queue_depth = 0;

    const uint32_t max_write_entry_size;

    uint64_t get_pg_id(inode_t inode, uint64_t stripe);
    void compact_block(uint32_t block_num);
    uint32_t find_block_run(heap_block_info_t & block, uint32_t space);
    uint32_t find_block_space(uint32_t block_num, uint32_t space);
    uint32_t compact_object_to(heap_object_t *obj, uint64_t lsn, uint8_t *new_csums);
    heap_object_t *mvcc_save_copy(heap_object_t *obj);
    int add_object(object_id oid, heap_write_t *wr, uint32_t *modified_block);
    int update_object(uint32_t block_num, heap_object_t *obj, heap_write_t *wr, uint32_t *modified_block);
    void erase_object(uint32_t block_num, heap_object_t *obj);
    void reindex_block(uint32_t block_num, heap_object_t *from_obj);
    void erase_block_index(inode_t inode, uint64_t stripe);
    void free_object_space(inode_t inode, heap_write_t *from, heap_write_t *to, int mode = 0);
    void add_used_space(uint32_t block_num, int32_t used_delta);

public:
    blockstore_heap_t(blockstore_disk_t *dsk, uint8_t *buffer_area, int log_level = 0);
    ~blockstore_heap_t();
    // set initially compacted lsn - should be done before loading
    void set_compacted_lsn(uint64_t compacted_lsn);
    uint64_t get_compacted_lsn();
    // load data from the disk, returns count of loaded write entries
    uint64_t load_blocks(uint64_t disk_offset, uint64_t size, uint8_t *buf);
    // finish loading
    void finish_load();
    // recheck small write data after reading the database from disk
    bool recheck_small_writes(std::function<void(uint64_t, uint64_t, uint8_t*, std::function<void()>)> read_buffer, int queue_depth);
    // initialize metadata area (fill it with empty data)
    // returns 0 when done, EAGAIN when the caller has to wait more
    int initialize();
    // read from the metadata area
    // returns 0 when done, EAGAIN when the caller has to wait more
    int read();
    // reshard database according to the pool's PG count
    void reshard(pool_id_t pool, uint32_t pg_count, uint32_t pg_stripe_size);
    // read an object entry and lock it against removal
    // in the future, may become asynchronous
    heap_object_t *lock_and_read_entry(object_id oid, uint64_t & lsn);
    // re-read a locked object entry with the given lsn (pointer may be invalidated)
    heap_object_t *read_locked_entry(object_id oid, uint64_t lsn);
    // read an object entry without locking it
    heap_object_t *read_entry(object_id oid, uint32_t *block_num_ptr, bool for_update = false);
    // unlock an entry
    bool unlock_entry(object_id oid, uint64_t lsn);
    // set or verify checksums in a write request
    bool calc_checksums(heap_write_t *wr, uint8_t *data, bool set);
    // set or verify raw block checksums
    bool calc_block_checksums(uint32_t *block_csums, uint8_t *data, uint8_t *bitmap, uint32_t start, uint32_t end,
        bool set, std::function<void(uint32_t, uint32_t, uint32_t)> bad_block_cb);
    // auto-compacts the object, then adds a write entry to it and to the compaction queue
    // return 0 if OK, or maybe ENOSPC
    int post_write(object_id oid, heap_write_t *wr, uint32_t *modified_block);
    // stabilize an unstable object version
    // return 0 if OK, ENOENT if not exists
    int post_stabilize(object_id oid, uint64_t version, uint32_t *modified_block);
    // rollback an unstable object version
    // return 0 if OK, ENOENT if not exists, EBUSY if already stable
    int post_rollback(object_id oid, uint64_t version, uint32_t *modified_block);
    // forget an object
    // return error code
    int post_delete(object_id oid, uint32_t *modified_block);
    // get the next object to compact
    // guaranteed to return objects in min lsn order
    // returns 0 if OK, ENOENT if nothing to compact
    int get_next_compact(object_id & oid);
    // get the range of an object eligible for compaction
    void get_compact_range(heap_object_t *obj, uint64_t max_lsn, heap_write_t **begin_wr, heap_write_t **end_wr);
    // mark an object as compacted up to the given lsn
    int compact_object(object_id oid, uint64_t lsn, uint8_t *new_csums);
    // retrieve object listing from a PG
    int list_objects(uint32_t pg_num, uint64_t min_inode, uint64_t max_inode,
        obj_ver_id **result_list, size_t *stable_count, size_t *unstable_count);
    // set a block number for a new object and returns error status: 0, EAGAIN or ENOSPC
    int get_block_for_new_object(uint32_t & out_block_num);

    // data device block allocator functions
    uint64_t find_free_data();
    bool is_data_used(uint64_t location);
    void use_data(inode_t inode, uint64_t location);

    // buffer device allocator functions
    uint64_t find_free_buffer_area(uint64_t size);
    bool is_buffer_area_free(uint64_t location, uint64_t size);
    uint64_t alloc_buffer_area(inode_t inode, uint64_t size);
    void use_buffer_area(inode_t inode, uint64_t location, uint64_t size);
    void free_buffer_area(inode_t inode, uint64_t location, uint64_t size);
    uint64_t get_buffer_area_used_space();

    // get metadata block data buffer and used space
    uint8_t *get_meta_block(uint32_t block_num);
    uint32_t get_meta_block_used_space(uint32_t block_num);

    // get space usage statistics
    uint64_t get_data_used_space();
    const std::map<uint64_t, uint64_t> & get_inode_space_stats();
    uint64_t get_meta_total_space();
    uint64_t get_meta_used_space();
    uint32_t get_meta_nearfull_blocks();
    uint32_t get_compact_queue_size();

    // get maximum size for a temporary heap_write_t buffer
    uint32_t get_max_write_entry_size();

    // only for tests
    void set_fail_on_warn(bool fail);
};
