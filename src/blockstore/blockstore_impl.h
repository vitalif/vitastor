// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#pragma once

#include "blockstore.h"
#include "blockstore_disk.h"
#include "blockstore_heap.h"
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

#include "malloc_or_die.h"

class blockstore_impl_t;

//#define BLOCKSTORE_DEBUG

// - Sync must be submitted after previous writes/deletes (not before!)
// - Reads may be submitted in parallel with writes/deletes because we use MVCC
// - Writes may be submitted in any order, because they don't overlap. Each write
//   goes into a new location - either on the journal device or on the data device
// - Stable (stabilize) must be submitted after sync of that object is completed
//   It's even OK to return an error to the caller if that object is not synced yet
// - compacted_lsn should be moved forward only after all versions are moved to the main storage
// - If an operation can not be submitted because the ring is full
//   we should stop submission of other operations. Otherwise some "scatter" reads
//   may end up blocked for a long time.
// Otherwise, the submission order is free.

#include "blockstore_init.h"

#include "blockstore_flush.h"

struct blockstore_op_private_t
{
    // Wait status
    int wait_for;
    uint64_t wait_detail;
    int pending_ops;
    int op_state;

    // Write, sync, stabilize
    uint32_t modified_block, modified_block2;

    // Read
    std::vector<copy_buffer_t> read_vec;

    // Write
    uint64_t location;
    bool is_big;

    // Stabilize, rollback
    int stab_pos;

    // Write
    struct iovec iov_zerofill[3];
    timespec tv_begin;
};

class blockstore_impl_t: public blockstore_i
{
public:
    blockstore_disk_t dsk;

    /******* OPTIONS *******/
    bool readonly = false;
    // Enable if you want every operation to be executed with an "implicit fsync"
    // Suitable only for server SSDs with capacitors, requires disabled data and journal fsyncs
    int immediate_commit = IMMEDIATE_NONE;
    bool inmemory_meta = false;
    uint32_t meta_write_recheck_parallelism = 0;
    // Maximum and minimum flusher count
    unsigned max_flusher_count = 0, min_flusher_count = 0;
    unsigned journal_trim_interval = 0;
    unsigned flusher_start_threshold = 0;
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
    // Enable correct block checksum validation on objects updated with small writes when checksum block
    // is larger than bitmap_granularity, at the expense of extra metadata fsyncs during compaction
    bool perfect_csum_update = false;
    /******* END OF OPTIONS *******/

    struct ring_consumer_t ring_consumer;

    blockstore_heap_t *heap = NULL;
    uint8_t* meta_superblock = NULL;
    uint8_t *buffer_area = NULL;
    std::vector<blockstore_op_t*> submit_queue;
    int unsynced_big_write_count = 0, unsynced_small_write_count = 0, unsynced_meta_write_count = 0;
    int unsynced_queued_ops = 0;
    uint8_t *zero_object = NULL;

    std::vector<uint32_t> pending_modified_blocks;
    robin_hood::unordered_flat_set<uint32_t> modified_blocks;

    journal_flusher_t *flusher;
    int write_iodepth = 0;
    int inflight_big = 0;
    bool fsyncing_data = false;

    bool live = false, queue_stall = false;
    ring_loop_i *ringloop = NULL;
    timerfd_manager_t *tfd = NULL;

    bool stop_sync_submitted = false;

    inline struct io_uring_sqe* get_sqe()
    {
        return ringloop->get_sqe();
    }

    void open_data();
    void open_meta();
    void open_journal();

    void disk_error_abort(const char *op, int retval, int expected);

    // Asynchronous init
    int initialized;
    int metadata_buf_size;
    blockstore_init_meta* metadata_init_reader;

    void init();
    void check_wait(blockstore_op_t *op);
    void init_op(blockstore_op_t *op);

    // Read
    int dequeue_read(blockstore_op_t *op);
    int fulfill_read(blockstore_op_t *op);
    uint32_t prepare_read(std::vector<copy_buffer_t> & read_vec, heap_entry_t *obj, heap_entry_t *wr, uint32_t start, uint32_t end);
    uint32_t prepare_read_with_bitmaps(std::vector<copy_buffer_t> & read_vec, heap_entry_t *obj, heap_entry_t *wr, uint32_t start, uint32_t end);
    uint32_t prepare_read_zero(std::vector<copy_buffer_t> & read_vec, uint32_t start, uint32_t end);
    uint32_t prepare_read_simple(std::vector<copy_buffer_t> & read_vec, heap_entry_t *obj, heap_entry_t *wr, uint32_t start, uint32_t end);
    void prepare_disk_read(std::vector<copy_buffer_t> & read_vec, int pos, heap_entry_t *obj, heap_entry_t *wr,
        uint32_t blk_start, uint32_t blk_end, uint32_t start, uint32_t end, uint32_t copy_flags);
    void find_holes(std::vector<copy_buffer_t> & read_vec, uint32_t item_start, uint32_t item_end,
        std::function<void(int&, uint32_t, uint32_t)> callback);
    void free_read_buffers(std::vector<copy_buffer_t> & rv);
    void handle_read_event(ring_data_t *data, blockstore_op_t *op);
    bool verify_read_checksums(blockstore_op_t *op);

    // Write
    bool enqueue_write(blockstore_op_t *op);
    void prepare_meta_block_write(uint32_t modified_block);
    bool meta_block_is_pending(uint32_t modified_block);
    bool intent_write_allowed(blockstore_op_t *op, heap_entry_t *obj);
    int dequeue_write(blockstore_op_t *op);
    int continue_write(blockstore_op_t *op);
    void handle_write_event(ring_data_t *data, blockstore_op_t *op);

    // Sync
    int continue_sync(blockstore_op_t *op);
    bool submit_fsyncs(int & wait_count);
    int do_sync(blockstore_op_t *op, int base_state);

    // Stabilize
    int dequeue_stable(blockstore_op_t *op);

    // List
    void process_list(blockstore_op_t *op);

/*public:*/

    blockstore_impl_t(blockstore_config_t & config, ring_loop_i *ringloop, timerfd_manager_t *tfd, bool mock_mode = false);
    ~blockstore_impl_t();

    void parse_config(blockstore_config_t & config);
    void parse_config(blockstore_config_t & config, bool init);

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

    // Set per-pool no_inode_stats
    void set_no_inode_stats(const std::vector<uint64_t> & pool_ids);

    // Print diagnostics to stdout
    void dump_diagnostics();

    // Get diagnostic string for an operation
    std::string get_op_diag(blockstore_op_t *op);

    const std::map<uint64_t, uint64_t> & get_inode_space_stats() { return heap->get_inode_space_stats(); }
    inline uint32_t get_block_size() { return dsk.data_block_size; }
    inline uint64_t get_block_count() { return dsk.block_count; }
    uint64_t get_free_block_count();
    inline uint32_t get_bitmap_granularity() { return dsk.disk_alignment; }
    inline uint64_t get_journal_size() { return dsk.journal_len; }
};
