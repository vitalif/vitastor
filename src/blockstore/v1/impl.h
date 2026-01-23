// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#pragma once

#include "blockstore.h"
#include "blockstore_disk.h"
#include "ondisk_formats.h"

#include <sys/types.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>
#include <unistd.h>
#include <linux/fs.h>

#include <vector>
#include <list>
#include <deque>
#include <new>
#include <unordered_map>
#include <unordered_set>

#include "cpp-btree/btree_map.h"

#include "malloc_or_die.h"
#include "allocator.h"
#include "crc32c.h"

//#define BLOCKSTORE_DEBUG

namespace v1 {

#include "journal.h"

// 32 = 16 + 16 bytes per "clean" entry in memory (object_id => clean_entry)
struct __attribute__((__packed__)) clean_entry
{
    uint64_t version;
    uint64_t location;
};

// 64 = 24 + 40 bytes per dirty entry in memory (obj_ver_id => dirty_entry). Plus checksums
struct __attribute__((__packed__)) dirty_entry
{
    uint32_t state;
    uint32_t flags;    // unneeded, but present for alignment
    uint64_t location; // location in either journal or data -> in BYTES
    uint32_t offset;   // data offset within object (stripe)
    uint32_t len;      // data length
    uint64_t journal_sector; // journal sector used for this entry
    void* dyn_data;    // dynamic data: external bitmap and data block checksums. may be a pointer to the in-memory journal
};

// - Sync must be submitted after previous writes/deletes (not before!)
// - Reads to the same object must be submitted after previous writes/deletes
//   are written (not necessarily synced) in their location. This is because we
//   rely on read-modify-write for erasure coding and we must return new data
//   to calculate parity for subsequent writes
// - Writes may be submitted in any order, because they don't overlap. Each write
//   goes into a new location - either on the journal device or on the data device
// - Stable (stabilize) must be submitted after sync of that object is completed
//   It's even OK to return an error to the caller if that object is not synced yet
// - Journal trim may be processed only after all versions are moved to
//   the main storage AND after all read operations for older versions complete
// - If an operation can not be submitted because the ring is full
//   we should stop submission of other operations. Otherwise some "scatter" reads
//   may end up blocked for a long time.
// Otherwise, the submit order is free, that is all operations may be submitted immediately
// In fact, adding a write operation must immediately result in dirty_db being populated

struct used_clean_obj_t
{
    int refs;
    bool was_freed; // was freed by a parallel flush?
    bool was_changed; // was changed by a parallel flush?
};

// https://github.com/algorithm-ninja/cpp-btree
// https://github.com/greg7mdp/sparsepp/ was used previously, but it was TERRIBLY slow after resizing
// with sparsepp, random reads dropped to ~700 iops very fast with just as much as ~32k objects in the DB
typedef btree::btree_map<object_id, clean_entry> blockstore_clean_db_t;
typedef std::map<obj_ver_id, dirty_entry> blockstore_dirty_db_t;

#include "init.h"

#include "flush.h"

struct blockstore_op_private_t
{
    // Wait status
    int wait_for;
    uint64_t wait_detail, wait_detail2;
    int pending_ops;
    int op_state;

    // Read
    uint64_t clean_block_used;
    std::vector<copy_buffer_t> read_vec;

    // Sync, write
    uint64_t min_flushed_journal_sector, max_flushed_journal_sector;

    // Write
    struct iovec iov_zerofill[3];
    // Warning: must not have a default value here because it's written to before calling constructor in blockstore_write.cpp O_o
    uint64_t real_version;
    timespec tv_begin;

    // Sync
    std::vector<obj_ver_id> sync_big_writes, sync_small_writes;
};

struct pool_shard_settings_t
{
    uint32_t pg_count;
    uint32_t pg_stripe_size;
};

typedef uint64_t pool_pg_id_t;

class blockstore_impl_t: public blockstore_i
{
    blockstore_disk_t dsk;

    /******* OPTIONS *******/
    bool readonly = false;
    // It is safe to disable fsync() if drive write cache is writethrough
    bool disable_data_fsync = false, disable_meta_fsync = false, disable_journal_fsync = false;
    // Enable if you want every operation to be executed with an "implicit fsync"
    // Suitable only for server SSDs with capacitors, requires disabled data and journal fsyncs
    int immediate_commit = IMMEDIATE_NONE;
    bool inmemory_meta = false;
    // Maximum and minimum flusher count
    unsigned max_flusher_count, min_flusher_count;
    unsigned journal_trim_interval;
    // Maximum queue depth
    unsigned max_write_iodepth = 128;
    // Enable small (journaled) write throttling, useful for the SSD+HDD case
    bool throttle_small_writes = false;
    // Target data device iops, bandwidth and parallelism for throttling (100/100/1 is the default for HDD)
    int throttle_target_iops = 100;
    int throttle_target_mbs = 100;
    int throttle_target_parallelism = 1;
    // Minimum difference in microseconds between target and real execution times to throttle the response
    int throttle_threshold_us = 50;
    // Maximum writes between automatically added fsync operations
    uint64_t autosync_writes = 128;
    // Log level (0-10)
    int log_level = 0;
    /******* END OF OPTIONS *******/

    struct ring_consumer_t ring_consumer;

    std::map<pool_id_t, pool_shard_settings_t> clean_db_settings;
    std::map<pool_pg_id_t, blockstore_clean_db_t> clean_db_shards;
    std::map<uint64_t, int> no_inode_stats;
    std::map<uint64_t, uint64_t> inode_space_stats;
    uint8_t *clean_bitmaps = NULL;
    blockstore_dirty_db_t dirty_db;
    std::vector<blockstore_op_t*> submit_queue;
    std::vector<obj_ver_id> unsynced_big_writes, unsynced_small_writes;
    int unsynced_big_write_count = 0, unstable_unsynced = 0;
    int unsynced_queued_ops = 0;
    allocator_t *data_alloc = NULL;
    uint64_t used_blocks = 0;
    uint8_t *zero_object = NULL;

    void *metadata_buffer = NULL;

    struct journal_t journal;
    journal_flusher_t *flusher;
    int big_to_flush = 0;
    int write_iodepth = 0;
    bool alloc_dyn_data = false;

    // clean data blocks referenced by read operations
    std::map<uint64_t, used_clean_obj_t> used_clean_objects;

    bool live = false, queue_stall = false;
    ring_loop_i *ringloop;
    timerfd_manager_t *tfd;

    bool stop_sync_submitted;

    inline struct io_uring_sqe* get_sqe()
    {
        return ringloop->get_sqe();
    }

    friend class blockstore_init_meta;
    friend class blockstore_init_journal;
    friend struct blockstore_journal_check_t;
    friend class journal_flusher_t;
    friend class journal_flusher_co;

    void calc_lengths();
    void open_data();
    void open_meta();
    void open_journal();
    uint8_t* get_clean_entry_bitmap(uint64_t block_loc, int offset);

    blockstore_clean_db_t& clean_db_shard(object_id oid);
    void recalc_inode_space_stats(uint64_t pool_id, bool per_inode);

    // Journaling
    void prepare_journal_sector_write(int sector, blockstore_op_t *op);
    void handle_journal_write(ring_data_t *data, uint64_t flush_id);
    void disk_error_abort(const char *op, int retval, int expected);

    // Asynchronous init
    int initialized;
    int metadata_buf_size;
    blockstore_init_meta* metadata_init_reader;
    blockstore_init_journal* journal_init_reader;

    void check_wait(blockstore_op_t *op);
    void init_op(blockstore_op_t *op);

    // Read
    int dequeue_read(blockstore_op_t *read_op);
    void find_holes(std::vector<copy_buffer_t> & read_vec, uint32_t item_start, uint32_t item_end,
        std::function<int(int, bool, uint32_t, uint32_t)> callback);
    int fulfill_read(blockstore_op_t *read_op,
        uint64_t &fulfilled, uint32_t item_start, uint32_t item_end,
        uint32_t item_state, uint64_t item_version, uint64_t item_location,
        uint64_t journal_sector, uint8_t *csum, int *dyn_data);
    bool fulfill_clean_read(blockstore_op_t *read_op, uint64_t & fulfilled,
        uint8_t *clean_entry_bitmap, int *dyn_data,
        uint32_t item_start, uint32_t item_end, uint64_t clean_loc, uint64_t clean_ver);
    int fill_partial_checksum_blocks(std::vector<copy_buffer_t> & rv, uint64_t & fulfilled,
        uint8_t *clean_entry_bitmap, int *dyn_data, bool from_journal, uint8_t *read_buf, uint64_t read_offset, uint64_t read_end);
    int pad_journal_read(std::vector<copy_buffer_t> & rv, copy_buffer_t & cp,
        uint64_t dirty_offset, uint64_t dirty_end, uint64_t dirty_loc, uint8_t *csum_ptr, int *dyn_data,
        uint64_t offset, uint64_t submit_len, uint64_t & blk_begin, uint64_t & blk_end, uint8_t* & blk_buf);
    bool read_range_fulfilled(std::vector<copy_buffer_t> & rv, uint64_t & fulfilled, uint8_t *read_buf,
        uint8_t *clean_entry_bitmap, uint32_t item_start, uint32_t item_end);
    bool read_checksum_block(blockstore_op_t *op, int rv_pos, uint64_t &fulfilled, uint64_t clean_loc);
    uint8_t* read_clean_meta_block(blockstore_op_t *read_op, uint64_t clean_loc, int rv_pos);
    bool verify_padded_checksums(uint8_t *clean_entry_bitmap, uint8_t *csum_buf, uint32_t offset,
        iovec *iov, int n_iov, std::function<void(uint32_t, uint32_t, uint32_t)> bad_block_cb);
    bool verify_journal_checksums(uint8_t *csums, uint32_t offset,
        iovec *iov, int n_iov, std::function<void(uint32_t, uint32_t, uint32_t)> bad_block_cb);
    bool verify_clean_padded_checksums(blockstore_op_t *op, uint64_t clean_loc, uint8_t *dyn_data, bool from_journal,
        iovec *iov, int n_iov, std::function<void(uint32_t, uint32_t, uint32_t)> bad_block_cb);
    int fulfill_read_push(blockstore_op_t *op, void *buf, uint64_t offset, uint64_t len,
        uint32_t item_state, uint64_t item_version);
    void handle_read_event(ring_data_t *data, blockstore_op_t *op);

    // Write
    bool enqueue_write(blockstore_op_t *op);
    void cancel_all_writes(blockstore_op_t *op, blockstore_dirty_db_t::iterator dirty_it, int retval);
    int dequeue_write(blockstore_op_t *op);
    int dequeue_del(blockstore_op_t *op);
    int continue_write(blockstore_op_t *op);
    void release_journal_sectors(blockstore_op_t *op);
    void handle_write_event(ring_data_t *data, blockstore_op_t *op);

    // Sync
    int continue_sync(blockstore_op_t *op);
    void ack_sync(blockstore_op_t *op);

    // Stabilize
    int dequeue_stable(blockstore_op_t *op);
    int continue_stable(blockstore_op_t *op);
    void mark_stable(obj_ver_id ov, bool forget_dirty = false);
    void stabilize_object(object_id oid, uint64_t max_ver);
    blockstore_op_t* selective_sync(blockstore_op_t *op);
    int split_stab_op(blockstore_op_t *op, std::function<int(obj_ver_id v)> decider);

    // Rollback
    int dequeue_rollback(blockstore_op_t *op);
    int continue_rollback(blockstore_op_t *op);
    void mark_rolled_back(const obj_ver_id & ov);
    void erase_dirty(blockstore_dirty_db_t::iterator dirty_start, blockstore_dirty_db_t::iterator dirty_end, uint64_t clean_loc);
    void free_dirty_dyn_data(dirty_entry & e);

    // List
    void process_list(blockstore_op_t *op);

public:

    blockstore_impl_t(blockstore_config_t & config, ring_loop_i *ringloop, timerfd_manager_t *tfd);
    ~blockstore_impl_t();

    void parse_config(blockstore_config_t & config);
    void parse_config(blockstore_config_t & config, bool init);

    // Reshard database for a pool
    void* reshard_start(pool_id_t pool, uint32_t pg_count, uint32_t pg_stripe_size, uint64_t chunk_limit);
    bool reshard_continue(void *reshard_state, uint64_t chunk_limit);
    void reshard_abort(void *reshard_state);

    // Event loop
    void loop();

    // Returns true when blockstore is ready to process operations
    // (Although you're free to enqueue them before that)
    bool is_started();

    // Returns true when it's safe to destroy the instance. If destroying the instance
    // requires to purge some queues, starts that process. Should be called in the event
    // loop until it returns true.
    bool is_safe_to_stop();

    // Returns true if stalled
    bool is_stalled();

    // Submission
    void enqueue_op(blockstore_op_t *op);

    // Simplified synchronous operation: get object bitmap & current version
    int read_bitmap(object_id oid, uint64_t target_version, void *bitmap, uint64_t *result_version = NULL);

    // Unstable writes are added here (map of object_id -> version)
    std::unordered_map<object_id, uint64_t> unstable_writes;

    // Get space usage statistics
    const std::map<uint64_t, uint64_t> & get_inode_space_stats();

    // Set per-pool no_inode_stats
    void set_no_inode_stats(const std::vector<uint64_t> & pool_ids);

    // Print diagnostics to stdout
    void dump_diagnostics();

    // Get diagnostic string for an operation
    std::string get_op_diag(blockstore_op_t *op);

    inline uint32_t get_block_size() { return dsk.data_block_size; }
    inline uint64_t get_block_count() { return dsk.block_count; }
    inline uint64_t get_free_block_count() { return dsk.block_count - used_blocks; }
    inline uint32_t get_bitmap_granularity() { return dsk.disk_alignment; }
    inline uint64_t get_journal_size() { return dsk.journal_len; }
};

} // namespace v1
