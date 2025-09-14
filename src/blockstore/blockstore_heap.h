// Metadata storage version 3 ("heap")
// Copyright (c) Vitaliy Filippov, 2025+
// License: VNPL-1.1 (see README.md for details)

#pragma once

#include <map>
#include <unordered_map>
#include <set>
#include <deque>
#include <vector>

#include "../client/object_id.h"
#include "../util/robin_hood.h"
#include "blockstore_disk.h"
#include "multilist.h"

struct pool_shard_settings_t
{
    uint32_t pg_count;
    uint32_t pg_stripe_size;
};

#define BS_HEAP_TYPE 7
#define BS_HEAP_OBJECT 1
#define BS_HEAP_SMALL_WRITE 2
#define BS_HEAP_BIG_WRITE 3
#define BS_HEAP_TOMBSTONE 4
#define BS_HEAP_INTENT_WRITE 5
#define BS_HEAP_STABLE 8

class blockstore_heap_t;

struct __attribute__((__packed__)) heap_small_write_t
{
    uint16_t size;
    int16_t next_pos;
    uint8_t flags;
    uint64_t lsn;
    uint64_t version;
    uint64_t location;
    uint32_t offset;
    uint32_t len;
};

struct __attribute__((__packed__)) heap_big_write_t
{
    uint16_t size;
    int16_t next_pos;
    uint8_t flags;
    uint64_t lsn;
    uint64_t version;
    uint32_t block_num;
};

struct __attribute__((__packed__)) heap_tombstone_t
{
    uint16_t size;
    int16_t next_pos;
    uint8_t flags;
    uint64_t lsn;
    uint64_t version;
};

struct __attribute__((__packed__)) heap_write_t
{
    // size should have top bit cleared
    uint16_t size = 0;
    int16_t next_pos = 0;
    uint8_t entry_type = 0; // BS_HEAP_*
    uint64_t lsn = 0;
    uint64_t version = 0;

    // uint8_t[] external_bitmap
    // uint8_t[] internal_bitmap
    // uint32_t[] checksums

    heap_write_t *next();
    inline uint8_t type() const { return (entry_type & BS_HEAP_TYPE); }
    inline heap_small_write_t& small() { return *(heap_small_write_t*)this; }
    inline heap_big_write_t& big() { return *(heap_big_write_t*)this; }
    uint32_t get_size(blockstore_heap_t *heap);
    uint32_t get_csum_size(blockstore_heap_t *heap);
    bool needs_recheck(blockstore_heap_t *heap);
    bool needs_compact(blockstore_heap_t *heap);
    bool is_compacted(uint64_t compacted_lsn);
    bool can_be_collapsed(blockstore_heap_t *heap);
    bool is_allowed_before_compacted(uint64_t compacted_lsn, bool is_last_entry);
    uint8_t *get_ext_bitmap(blockstore_heap_t *heap);
    uint8_t *get_int_bitmap(blockstore_heap_t *heap);
    uint8_t *get_checksums(blockstore_heap_t *heap);
    uint32_t *get_checksum(blockstore_heap_t *heap);
    uint64_t big_location(blockstore_heap_t *heap);
    void set_big_location(blockstore_heap_t *heap, uint64_t location);
};

struct __attribute__((__packed__)) heap_object_t
{
    // size should have top bit cleared
    uint16_t size = 0;
    // linked list of write entries...
    // newest entries are stored first to simplify scanning
    int16_t write_pos = 0;
    uint8_t entry_type = 0; // BS_HEAP_*
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

struct tmp_compact_item_t
{
    object_id oid;
    uint64_t lsn;
    bool compact;
};

struct heap_mvcc_copy_id_t
{
    object_id oid;
    uint64_t copy_id;
};

inline bool operator == (const heap_mvcc_copy_id_t & a, const heap_mvcc_copy_id_t & b)
{
    return a.oid.inode == b.oid.inode && a.oid.stripe == b.oid.stripe && a.copy_id == b.copy_id;
}

namespace std
{
    template<> struct hash<heap_mvcc_copy_id_t>
    {
        inline size_t operator()(const heap_mvcc_copy_id_t &s) const
        {
            size_t seed = std::hash<object_id>()(s.oid);
            // Copy-pasted from spp::hash_combine()
            seed ^= (s.copy_id + 0xc6a4a7935bd1e995 + (seed << 6) + (seed >> 2));
            return seed;
        }
    };
};

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

struct heap_inflight_lsn_t
{
    object_id oid;
    uint64_t flags;
};

struct heap_refqi_t
{
    uint64_t lsn;
    uint64_t inode;
    uint64_t location;
    uint32_t len;
    bool is_data;
};

using i64hash_t = robin_hood::hash<uint64_t>;
using heap_block_index_t = robin_hood::unordered_flat_map<uint64_t,
    robin_hood::unordered_flat_map<inode_t, robin_hood::unordered_flat_map<uint64_t, uint64_t, i64hash_t, std::equal_to<uint64_t>, 88>, i64hash_t>, i64hash_t>;
using heap_mvcc_map_t = robin_hood::unordered_flat_map<heap_mvcc_copy_id_t, heap_object_mvcc_t>;

class blockstore_heap_t
{
    friend class heap_write_t;
    friend class heap_object_t;

    blockstore_disk_t *dsk = NULL;
    uint8_t* buffer_area = NULL;
    bool abort_on_corruption = false;
    bool abort_on_overlap = true;
    int log_level = 0;

    const uint32_t meta_block_count = 0;
    uint32_t target_block_free_space = 800;

    uint64_t next_lsn = 0;
    robin_hood::unordered_flat_map<pool_id_t, pool_shard_settings_t> pool_shard_settings;
    // PG => inode => stripe => block number
    heap_block_index_t block_index;
    std::vector<heap_block_info_t> block_info;
    allocator_t *data_alloc = NULL;
    multilist_index_t *meta_alloc = NULL;
    uint32_t meta_alloc_count = 0;
    uint64_t meta_used_space = 0;
    multilist_alloc_t *buffer_alloc = NULL;
    heap_mvcc_map_t object_mvcc;
    std::unordered_map<uint64_t, uint32_t> mvcc_data_refs;
    std::unordered_map<uint64_t, uint32_t> mvcc_buffer_refs;
    std::map<uint64_t, uint64_t> inode_space_stats;
    uint64_t buffer_area_used_space = 0;
    uint64_t data_used_space = 0;

    // LSN queue: inflight (writing) -> completed [-> fsynced] -> compactable -> compacted [-> fsynced] -> trimmed and removed
    std::deque<heap_inflight_lsn_t> inflight_lsn;
    uint32_t to_compact_count = 0;
    uint64_t first_inflight_lsn = 0;
    uint64_t completed_lsn = 0;
    uint64_t fsynced_lsn = 0;
    uint64_t compacted_lsn = 0;
    uint64_t next_compact_lsn = 0;
    std::deque<heap_refqi_t> overwrite_ref_queue;

    std::vector<tmp_compact_item_t> tmp_compact_queue;
    std::deque<object_id> recheck_queue;
    int recheck_in_progress = 0;
    bool in_recheck = false;
    std::function<void(bool is_data, uint64_t offset, uint64_t len, uint8_t* buf, std::function<void()>)> recheck_cb;
    int recheck_queue_depth = 0;

    const uint32_t max_write_entry_size;

    uint64_t get_pg_id(inode_t inode, uint64_t stripe);
    void defragment_block(uint32_t block_num);
    uint32_t find_block_run(heap_block_info_t & block, uint32_t space);
    uint32_t find_block_space(uint32_t block_num, uint32_t space);
    uint32_t block_has_compactable(uint8_t *data);
    uint32_t compact_object_to(heap_object_t *obj, uint64_t lsn, uint8_t *new_csums, bool do_free);
    void copy_full_object(uint8_t *dst, heap_object_t *obj);
    bool mvcc_save_copy(heap_object_t *obj);
    bool mvcc_check_tracking(object_id oid);
    void free_mvcc(heap_mvcc_map_t::iterator mvcc_it);
    void allocate_block(heap_block_info_t & inf);
    int allocate_new_object(object_id oid, uint32_t full_object_size, uint32_t *modified_block, heap_object_t **new_obj);
    int add_object(object_id oid, heap_write_t *wr, uint32_t *modified_block);
    void mark_overwritten(uint64_t over_lsn, uint64_t inode, heap_write_t *wr, heap_write_t *end_wr, bool tracking_active);
    int update_object(uint32_t block_num, heap_object_t *obj, heap_write_t *wr, uint32_t *modified_block, uint32_t *moved_from_block);
    void init_erase(uint32_t block_num, heap_object_t *obj);
    void erase_object(uint32_t block_num, heap_object_t *obj, uint64_t lsn, bool tracking_active);
    void reindex_block(uint32_t block_num, heap_object_t *from_obj);
    void erase_block_index(inode_t inode, uint64_t stripe);
    void deref_data(uint64_t inode, uint64_t location, bool free_at_0);
    void deref_buffer(uint64_t inode, uint64_t location, uint32_t len, bool free_at_0);
    void deref_overwrites(uint64_t lsn);
    void free_object_space(inode_t inode, heap_write_t *from, heap_write_t *to, int mode = 0);
    void add_used_space(uint32_t block_num, int32_t used_delta);
    void push_inflight_lsn(object_id oid, uint64_t lsn, uint64_t flags);

public:
    blockstore_heap_t(blockstore_disk_t *dsk, uint8_t *buffer_area, int log_level = 0);
    ~blockstore_heap_t();
    // set initially compacted lsn - should be done before loading
    void set_compacted_lsn(uint64_t compacted_lsn);
    uint64_t get_compacted_lsn();
    // load data from the disk, returns count of loaded write entries
    void read_blocks(uint64_t disk_offset, uint64_t size, uint8_t *buf,
        std::function<void(heap_object_t*)> handle_object, std::function<void(uint32_t, uint32_t, uint8_t*)> handle_block);
    uint64_t load_blocks(uint64_t disk_offset, uint64_t size, uint8_t *buf);
    // finish loading
    void finish_load();
    // recheck small write data after reading the database from disk
    bool recheck_small_writes(std::function<void(bool is_data, uint64_t offset, uint64_t len, uint8_t* buf, std::function<void()>)> read_buffer, int queue_depth);
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
    heap_object_t *lock_and_read_entry(object_id oid, uint64_t & copy_id);
    // re-read a locked object entry with the given lsn (pointer may be invalidated)
    heap_object_t *read_locked_entry(object_id oid, uint64_t copy_id);
    // read an object entry without locking it
    heap_object_t *read_entry(object_id oid, uint32_t *block_num_ptr, bool for_update = false);
    // unlock an entry
    bool unlock_entry(object_id oid, uint64_t copy_id);
    // set or verify checksums in a write request
    bool calc_checksums(heap_write_t *wr, uint8_t *data, bool set, uint32_t offset = 0, uint32_t len = 0);
    // set or verify raw block checksums
    bool calc_block_checksums(uint32_t *block_csums, uint8_t *data, uint8_t *bitmap, uint32_t start, uint32_t end,
        bool set, std::function<void(uint32_t, uint32_t, uint32_t)> bad_block_cb);
    bool calc_block_checksums(uint32_t *block_csums, uint8_t *bitmap,
        uint32_t start, uint32_t end, std::function<uint8_t*(uint32_t start, uint32_t & len)> next,
        bool set, std::function<void(uint32_t, uint32_t, uint32_t)> bad_block_cb);
    // copy an object as is
    int copy_object(heap_object_t *obj, uint32_t *modified_block);
    // auto-compacts the object, then adds a write entry to it and to the compaction queue
    // return 0 if OK, or maybe ENOSPC
    int post_write(object_id oid, heap_write_t *wr, uint32_t *modified_block, uint32_t *moved_from_block);
    int post_write(uint32_t & block_num, object_id oid, heap_object_t *obj, heap_write_t *wr, uint32_t *moved_from_block);
    // stabilize an unstable object version
    // return 0 if OK, ENOENT if not exists
    int post_stabilize(object_id oid, uint64_t version, uint32_t *modified_block, uint64_t *new_lsn, uint64_t *new_to_lsn);
    // rollback an unstable object version
    // return 0 if OK, ENOENT if not exists, EBUSY if already stable
    int post_rollback(object_id oid, uint64_t version, uint64_t *new_lsn, uint32_t *modified_block);
    // forget an object
    // return error code
    int post_delete(object_id oid, uint64_t *new_lsn, uint32_t *modified_block);
    int post_delete(uint32_t block_num, heap_object_t *obj, uint64_t *new_lsn);
    // get the next object to compact
    // guaranteed to return objects in min lsn order
    // returns 0 if OK, ENOENT if nothing to compact
    int get_next_compact(object_id & oid);
    // get the range of an object eligible for compaction
    void get_compact_range(heap_object_t *obj, uint64_t max_lsn, heap_write_t **begin_wr, heap_write_t **end_wr);
    // mark an object as compacted up to the given lsn
    int compact_object(object_id oid, uint64_t lsn, uint8_t *new_csums);
    // retrieve object listing from a PG
    int list_objects(uint32_t pg_num, object_id min_oid, object_id max_oid,
        obj_ver_id **result_list, size_t *stable_count, size_t *unstable_count);
    // set a block number for a new object and returns error status: 0, EAGAIN or ENOSPC
    int get_block_for_new_object(uint32_t & out_block_num, uint32_t size = 0);

    // inflight write tracking
    void mark_lsn_completed(uint64_t lsn);
    void mark_lsn_fsynced(uint64_t lsn);
    void mark_lsn_compacted(uint64_t lsn, bool allow_undone = false);
    void mark_object_compacted(heap_object_t *obj, uint64_t max_lsn);
    void mark_lsn_trimmed(uint64_t lsn);
    uint64_t get_completed_lsn();
    uint64_t get_fsynced_lsn();

    // data device block allocator functions
    uint64_t find_free_data();
    bool is_data_used(uint64_t location);
    void use_data(inode_t inode, uint64_t location);
    void free_data(inode_t inode, uint64_t location);

    // buffer device allocator functions
    uint64_t find_free_buffer_area(uint64_t size);
    bool is_buffer_area_free(uint64_t location, uint64_t size);
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
    uint32_t get_inflight_queue_size();
    uint32_t get_compact_queue_size();
    uint32_t get_to_compact_count();

    // get maximum size for a temporary heap_write_t buffer
    uint32_t get_max_write_entry_size();

    // only for tests
    void set_abort_on_corruption(bool fail);
    void set_abort_on_overlap(bool fail);
};
