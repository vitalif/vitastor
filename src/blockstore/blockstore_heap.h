// Metadata storage version 3 ("lsm heap")
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
    uint32_t no_inode_stats;
};

#define BS_HEAP_TYPE 0x07
#define BS_HEAP_BIG_WRITE 1
#define BS_HEAP_SMALL_WRITE 2
#define BS_HEAP_INTENT_WRITE 3
#define BS_HEAP_BIG_INTENT 4
#define BS_HEAP_DELETE 5
#define BS_HEAP_COMMIT 6
#define BS_HEAP_ROLLBACK 7
#define BS_HEAP_STABLE 0x40
#define BS_HEAP_GARBAGE 0x80

class blockstore_heap_t;

struct heap_small_write_t;
struct heap_big_write_t;
struct heap_big_intent_t;

struct __attribute__((__packed__)) heap_entry_t
{
    uint16_t size;
    uint16_t entry_type;
    uint32_t crc32c;
    uint64_t lsn;
    uint64_t inode;
    uint64_t stripe;
    uint64_t version;

    // uint8_t[] external_bitmap
    // uint8_t[] internal_bitmap
    // uint32_t[] checksums

    inline uint8_t type() const { return (entry_type & BS_HEAP_TYPE); }
    inline heap_small_write_t& small() { return *(heap_small_write_t*)this; }
    inline heap_big_write_t& big() { return *(heap_big_write_t*)this; }
    inline heap_big_intent_t& big_intent() { return *(heap_big_intent_t*)this; }
    bool is_garbage();
    void set_garbage();
    bool is_overwrite();
    bool is_compactable();
    bool is_before(heap_entry_t *other);
    uint32_t get_size(blockstore_heap_t *heap);
    uint8_t *get_ext_bitmap(blockstore_heap_t *heap);
    uint8_t *get_int_bitmap(blockstore_heap_t *heap);
    uint8_t *get_checksums(blockstore_heap_t *heap);
    uint32_t *get_checksum(blockstore_heap_t *heap);
    uint64_t big_location(blockstore_heap_t *heap);
    void set_big_location(blockstore_heap_t *heap, uint64_t location);
    uint32_t calc_crc32c();
};

struct __attribute__((__packed__)) heap_small_write_t
{
    heap_entry_t hdr;

    uint64_t location; // FIXME: change to uint32_t and shift by block size
    uint32_t offset;
    uint32_t len;

    // Also includes 1 bitmap and 1 crc32c after the bitmap if checksums are disabled
};

struct __attribute__((__packed__)) heap_big_write_t
{
    heap_entry_t hdr;

    uint32_t block_num;
};

struct __attribute__((__packed__)) heap_big_intent_t
{
    heap_entry_t hdr;

    uint32_t block_num;
    uint32_t offset;
    uint32_t len;

    // Also includes 2 bitmaps and 1 crc32c if checksums are disabled
};

struct __attribute__((__packed__)) heap_list_item_t
{
    heap_list_item_t *prev;
    heap_list_item_t *next;
    uint32_t block_num;
    heap_entry_t entry;
};

struct heap_object_mvcc_t
{
    uint32_t readers = 0;
    heap_entry_t *garbage_entry = NULL;
};

struct heap_block_info_t
{
    uint32_t used_space = 0;
    uint64_t mod_lsn = 0, mod_lsn_to = 0; // only 1 block write of LSN sequence is allowed at a moment
    bool is_writing: 1;
    bool has_garbage: 1;
    std::vector<heap_list_item_t*> entries;
};

struct heap_inflight_lsn_t
{
    uint64_t flags;
    heap_entry_t *wr;
};

struct heap_compact_t
{
    uint64_t compact_lsn, compact_version;
    heap_entry_t *clean_wr;
    bool do_delete;
};

struct heap_li_hash
{
    size_t operator()(const heap_list_item_t* li) const noexcept
    {
        return robin_hood::hash_int(li->entry.stripe);
    }
};

struct heap_li_equal
{
    constexpr bool operator()(const heap_list_item_t* a, const heap_list_item_t* b) const noexcept
    {
        return a->entry.stripe == b->entry.stripe;
    }
};

using i64hash_t = robin_hood::hash<uint64_t>;
using heap_inode_map_t = robin_hood::unordered_flat_set<heap_list_item_t*, heap_li_hash, heap_li_equal, 88>;
using heap_block_index_t = robin_hood::unordered_flat_map<uint64_t,
    robin_hood::unordered_flat_map<inode_t, void*, i64hash_t>, i64hash_t>;
using heap_mvcc_map_t = robin_hood::unordered_flat_map<object_id, heap_object_mvcc_t>;

class blockstore_heap_t
{
    friend class heap_entry_t;

    blockstore_disk_t *dsk = NULL;
    uint8_t* buffer_area = NULL;
    int log_level = 0;
    const uint32_t meta_block_count = 0;
    const uint32_t max_entry_size = 0;

    robin_hood::unordered_flat_map<pool_id_t, pool_shard_settings_t> pool_shard_settings;
    // PG => inode => stripe => block number
    heap_block_index_t block_index;
    std::vector<heap_block_info_t> block_info;
    allocator_t *data_alloc = NULL;
    multilist_index_t *meta_alloc = NULL;
    uint32_t meta_nearfull_blocks = 0;
    uint64_t meta_used_space = 0;
    multilist_alloc_t *buffer_alloc = NULL;
    std::map<uint64_t, uint64_t> inode_space_stats;
    uint64_t buffer_area_used_space = 0;
    uint64_t data_used_space = 0;

    uint64_t next_lsn = 0;
    uint32_t last_allocated_block = UINT32_MAX;
    heap_mvcc_map_t object_mvcc;

    // LSN queue: inflight (writing) -> completed [-> fsynced]
    std::deque<heap_inflight_lsn_t> inflight_lsn;
    uint32_t to_compact_count = 0;
    uint64_t compacted_count = 0;
    uint32_t inflight_overwrite_count = 0;
    uint64_t first_inflight_lsn = 0;
    uint64_t completed_lsn = 0;
    uint64_t fsynced_lsn = 0;
    std::deque<object_id> compact_queue;

    bool marked_used_blocks = false;
    bool recheck_queue_filled = false;
    std::set<uint32_t> recheck_modified_blocks;
    std::deque<heap_entry_t*> recheck_queue;
    int recheck_in_progress = 0;
    bool in_recheck = false;
    std::function<void(bool is_data, uint64_t offset, uint64_t len, uint8_t* buf, std::function<void()>)> recheck_cb;
    int recheck_queue_depth = 0;

    void inode_map_put(void* & inode_idx, heap_list_item_t* li);
    void inode_map_get(void *inode_idx, heap_inode_map_t::iterator & li_it, heap_list_item_t* & li, uint64_t stripe);
    void inode_map_free(void* inode_idx);
    void inode_map_iterate(void* & inode_idx, std::function<void(heap_list_item_t*)> cb);
    void inode_map_replace(void* & inode_idx, const heap_inode_map_t::iterator & li_it, heap_list_item_t* new_li);
    void inode_map_erase(void* & inode_idx, const heap_inode_map_t::iterator & li_it, heap_list_item_t* li);

    uint64_t get_pg_id(inode_t inode, uint64_t stripe);
    bool validate_object(heap_entry_t *obj);
    void fill_recheck_queue();
    int mark_used_blocks();
    void recheck_buffer(heap_entry_t *cwr, uint8_t *buf);
    void defragment_block(uint32_t block_num);

    int allocate_entry(uint32_t entry_size, uint32_t *block_num, bool allow_last_free);
    void insert_list_item(heap_list_item_t *li);
    int add_entry(uint32_t wr_size, uint32_t *modified_block, bool allow_last_free,
        bool explicit_complete, std::function<void(heap_entry_t *wr)> fill_entry);
    int add_simple(heap_entry_t *obj, uint64_t version, uint32_t *modified_block, uint32_t entry_type);
    uint32_t meta_alloc_pos(const heap_block_info_t & inf);
    void modify_alloc(uint32_t block_num, std::function<void(heap_block_info_t &)> change_cb);
    void mark_garbage_up_to(heap_entry_t *wr);
    void mark_garbage(uint32_t block_num, heap_entry_t *prev_wr, uint32_t used_big);
    void push_inflight_lsn(uint64_t lsn, heap_entry_t *wr, uint64_t flags);
    void mark_completed_lsns(uint64_t mod_lsn);
    void apply_inflight(heap_inflight_lsn_t & inflight);
public:
    blockstore_heap_t(blockstore_disk_t *dsk, uint8_t *buffer_area, int log_level = 0);
    ~blockstore_heap_t();
    void start_load(uint64_t completed_lsn);
    // load data from the disk, returns EDOM on corruption
    int read_blocks(uint64_t disk_offset, uint64_t size, uint8_t *buf, bool allow_corrupted,
        std::function<void(uint32_t block_num, heap_entry_t* wr)> handle_write,
        std::function<void(uint32_t, uint32_t, uint8_t*)> handle_block);
    int load_blocks(uint64_t disk_offset, uint64_t size, uint8_t *buf,
        bool allow_corrupted, uint64_t &entries_loaded);
    // finish loading
    int finish_load(bool allow_corrupted = false);
    // get blocks which are modified during loading and should be written to the disk
    // before finishing initialization if not R/O
    std::vector<uint32_t> get_recheck_modified_blocks();
    // recheck small write data after reading the database from disk
    bool recheck_small_writes(std::function<void(bool is_data, uint64_t offset, uint64_t len, uint8_t* buf, std::function<void()>)> read_buffer, int queue_depth);
    // reshard database according to the pool's PG count
    void reshard(pool_id_t pool, uint32_t pg_count, uint32_t pg_stripe_size);
    void set_no_inode_stats(const std::vector<uint64_t> & pool_ids);
    void recalc_inode_space_stats(uint64_t pool_id, bool per_inode);
    // read an object entry and lock it against removal
    // in the future, may become asynchronous
    heap_entry_t *lock_and_read_entry(object_id oid);
    // re-read a locked object entry with the given lsn (pointer may be invalidated)
    heap_entry_t *read_locked_entry(object_id oid, uint64_t lsn);
    // read an object entry without locking it
    heap_entry_t *read_entry(object_id oid);
    // unlock an entry
    bool unlock_entry(object_id oid);
    // set or verify checksums in a write request
    bool calc_checksums(heap_entry_t *wr, uint8_t *data, bool set, uint32_t offset = 0, uint32_t len = 0);
    // set or verify raw block checksums
    bool calc_block_checksums(uint32_t *block_csums, uint8_t *data, uint8_t *bitmap, uint32_t start, uint32_t end,
        bool set, std::function<void(uint32_t, uint32_t, uint32_t)> bad_block_cb);
    bool calc_block_checksums(uint32_t *block_csums, uint8_t *bitmap,
        uint32_t start, uint32_t end, std::function<uint8_t*(uint32_t start, uint32_t & len)> next,
        bool set, std::function<void(uint32_t, uint32_t, uint32_t)> bad_block_cb);
    // adds a small_write or intent_write entry to an object
    // return 0 if OK, or maybe ENOSPC
    int add_small_write(object_id oid, heap_entry_t **obj_ptr, uint16_t type, uint64_t version,
        uint32_t offset, uint32_t len, uint64_t location, uint8_t *bitmap, uint8_t *data, uint32_t *modified_block);
    // adds a big_write (overwrite) entry to an object
    int add_big_write(object_id oid, heap_entry_t *old_head, bool stable, uint64_t version,
        uint32_t offset, uint32_t len, uint64_t location, uint8_t *bitmap, uint8_t *data, uint32_t *modified_block);
    // adds a "redirecting" big_intent entry to an object (same as big_write, used to avoid fsync on desktop SSDs)
    int add_redirect_intent(object_id oid, heap_entry_t **obj_ptr, uint64_t version,
        uint32_t offset, uint32_t len, uint64_t location, uint8_t *bitmap, uint8_t *data, uint32_t *modified_block);
    // adds a big_intent (atomic partial modification) entry to an object
    int add_big_intent(object_id oid, heap_entry_t **obj_ptr, uint64_t version,
        uint32_t offset, uint32_t len, uint8_t *bitmap, uint8_t *data, uint8_t *checksums, uint32_t *modified_block);
    // adds a compacted up to <version> entry to an object
    int add_compact(heap_entry_t *obj, uint64_t compact_version, uint64_t compact_lsn, uint64_t compact_location,
        bool do_delete, uint32_t *modified_block, uint8_t *new_int_bitmap, uint8_t *new_ext_bitmap, uint8_t *new_csums);
    // "punch holes" in a big_entry
    int punch_holes(heap_entry_t *wr, uint8_t *new_bitmap, uint8_t *new_csums, uint32_t *modified_block);
    // stabilize an unstable object version
    // return 0 if OK, ENOENT if not exists
    int add_commit(heap_entry_t *obj, uint64_t version, uint32_t *modified_block);
    // rollback an unstable object version
    // return 0 if OK, ENOENT if not exists, EBUSY if already stable
    int add_rollback(heap_entry_t *obj, uint64_t version, uint32_t *modified_block);
    // forget an object
    // return error code
    int add_delete(heap_entry_t *obj, uint32_t *modified_block);
    // get the next object to compact
    // guaranteed to return objects in min lsn order
    // returns 0 if OK, ENOENT if nothing to compact
    int get_next_compact(object_id & oid);
    void iterate_with_stable(heap_entry_t *obj, uint64_t max_lsn, std::function<bool(heap_entry_t*, bool stable)> cb);
    // iterate compactable entries
    heap_compact_t iterate_compaction(heap_entry_t *obj, uint64_t fsynced_lsn, bool under_pressure,
        std::function<void(heap_entry_t*)> small_wr_cb);
    // iterate all objects
    void iterate_objects(std::function<void(heap_entry_t*, uint32_t block_num)> cb);
    // retrieve object listing from a PG
    int list_objects(uint32_t pg_num, object_id min_oid, object_id max_oid,
        obj_ver_id **result_list, size_t *stable_count, size_t *unstable_count);

    // inflight write tracking
    void start_block_write(uint32_t block_num);
    void complete_block_write(uint32_t block_num);
    void complete_lsn_write(uint64_t lsn);
    uint64_t get_completed_lsn();
    uint64_t get_fsynced_lsn();
    void mark_lsn_fsynced(uint64_t lsn);

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
    void get_meta_block(uint32_t block_num, uint8_t *buffer);
    void fill_block_empty_space(uint8_t *buffer, uint32_t pos);
    uint32_t get_meta_block_used_space(uint32_t block_num);

    // get space usage statistics
    uint64_t get_data_used_space();
    const std::map<uint64_t, uint64_t> & get_inode_space_stats();
    uint64_t get_meta_total_space();
    uint64_t get_meta_used_space();
    uint32_t get_meta_nearfull_blocks();
    uint32_t get_compact_queue_size();
    uint32_t get_to_compact_count();
    uint64_t get_compacted_count();

    uint64_t entry_pos(uint32_t block_num, uint32_t offset);
    heap_entry_t *entry_from_pos(uint64_t entry_pos, bool allow_unallocated = false);
    heap_entry_t *prev(heap_entry_t *wr);
    uint32_t get_simple_entry_size();
    uint32_t get_big_entry_size();
    uint32_t get_big_intent_entry_size();
    uint32_t get_small_entry_size(uint32_t offset, uint32_t len);
    uint32_t get_csum_size(heap_entry_t *wr);
    uint32_t get_csum_size(uint32_t entry_type, uint32_t offset = 0, uint32_t len = 0);
};
