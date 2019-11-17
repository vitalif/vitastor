#pragma once

#ifndef _LARGEFILE64_SOURCE
#define _LARGEFILE64_SOURCE
#endif
#include <sys/types.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdint.h>
#include <malloc.h>
#include <linux/fs.h>
#include <liburing.h>

#include <vector>
#include <map>
#include <list>
#include <deque>
#include <set>
#include <functional>

#include "sparsepp/sparsepp/spp.h"

#include "allocator.h"
#include "ringloop.h"

// States are not stored on disk. Instead, they're deduced from the journal

#define ST_IN_FLIGHT 1

#define ST_J_SUBMITTED 2
#define ST_J_WRITTEN 3
#define ST_J_SYNCED 4
#define ST_J_STABLE 5

#define ST_D_SUBMITTED 16
#define ST_D_WRITTEN 17
#define ST_D_SYNCED 18
#define ST_D_META_WRITTEN 19
#define ST_D_META_SYNCED 20
#define ST_D_STABLE 21

#define ST_DEL_SUBMITTED 32
#define ST_DEL_WRITTEN 33
#define ST_DEL_SYNCED 34
#define ST_DEL_STABLE 35

#define ST_CURRENT 48

#define IS_IN_FLIGHT(st) (st == ST_IN_FLIGHT || st == ST_J_SUBMITTED || st == ST_D_SUBMITTED || st == ST_DEL_SUBMITTED)
#define IS_STABLE(st) (st == ST_J_STABLE || st == ST_D_STABLE || st == ST_DEL_STABLE || st == ST_CURRENT)
#define IS_JOURNAL(st) (st >= ST_J_SUBMITTED && st <= ST_J_STABLE)
#define IS_BIG_WRITE(st) (st >= ST_D_SUBMITTED && st <= ST_D_STABLE)
#define IS_DELETE(st) (st >= ST_DEL_SUBMITTED && st <= ST_DEL_STABLE)
#define IS_UNSYNCED(st) (st >= ST_J_SUBMITTED && st <= ST_J_WRITTEN || st >= ST_D_SUBMITTED && st <= ST_D_META_WRITTEN || st >= ST_DEL_SUBMITTED && st <= ST_DEL_WRITTEN)

// Default object size is 128 KB
#define DEFAULT_ORDER 17
#define MAX_BLOCK_SIZE 128*1024*1024
#define DISK_ALIGNMENT 512

#define STRIPE_NUM(oid) ((oid) >> 4)
#define STRIPE_REPLICA(oid) ((oid) & 0xf)

#define BS_SUBMIT_GET_SQE(sqe, data) \
    BS_SUBMIT_GET_ONLY_SQE(sqe); \
    struct ring_data_t *data = ((ring_data_t*)sqe->user_data)

#define BS_SUBMIT_GET_ONLY_SQE(sqe) \
    struct io_uring_sqe *sqe = get_sqe();\
    if (!sqe)\
    {\
        /* Pause until there are more requests available */\
        op->wait_for = WAIT_SQE;\
        return 0;\
    }

#define BS_SUBMIT_GET_SQE_DECL(sqe) \
    sqe = get_sqe();\
    if (!sqe)\
    {\
        /* Pause until there are more requests available */\
        op->wait_for = WAIT_SQE;\
        return 0;\
    }

class blockstore;

class blockstore_operation;

// 16 bytes per object/stripe id
// stripe includes replica number in 4 least significant bits
struct __attribute__((__packed__)) object_id
{
    uint64_t inode;
    uint64_t stripe;
};

#include "blockstore_journal.h"

inline bool operator == (const object_id & a, const object_id & b)
{
    return a.inode == b.inode && a.stripe == b.stripe;
}

inline bool operator != (const object_id & a, const object_id & b)
{
    return a.inode != b.inode || a.stripe != b.stripe;
}

inline bool operator < (const object_id & a, const object_id & b)
{
    return a.inode < b.inode || a.inode == b.inode && a.stripe < b.stripe;
}

// 24 bytes per "clean" entry on disk with fixed metadata tables
// FIXME: maybe add crc32's to metadata
struct __attribute__((__packed__)) clean_disk_entry
{
    object_id oid;
    uint64_t version;
};

// 32 = 16 + 16 bytes per "clean" entry in memory (object_id => clean_entry)
struct __attribute__((__packed__)) clean_entry
{
    uint64_t version;
    uint64_t location;
};

// 56 = 24 + 32 bytes per dirty entry in memory (obj_ver_id => dirty_entry)
struct __attribute__((__packed__)) obj_ver_id
{
    object_id oid;
    uint64_t version;
};

inline bool operator < (const obj_ver_id & a, const obj_ver_id & b)
{
    return a.oid < b.oid || a.oid == b.oid && a.version < b.version;
}

struct __attribute__((__packed__)) dirty_entry
{
    uint32_t state;
    uint32_t flags;    // unneeded, but present for alignment
    uint64_t location; // location in either journal or data -> in BYTES
    uint32_t offset;   // data offset within object (stripe)
    uint32_t len;      // data length
    uint64_t journal_sector; // journal sector used for this entry
};

class oid_hash
{
public:
    size_t operator()(const object_id &s) const
    {
        size_t seed = 0;
        spp::hash_combine(seed, s.inode);
        spp::hash_combine(seed, s.stripe);
        return seed;
    }
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

#define OP_READ 1
#define OP_WRITE 2
#define OP_SYNC 3
#define OP_STABLE 4
#define OP_DELETE 5
#define OP_TYPE_MASK 0x7

// Suspend operation until there are more free SQEs
#define WAIT_SQE 1
// Suspend operation until version <wait_detail> of object <oid> is written
#define WAIT_IN_FLIGHT 2
// Suspend operation until there are <wait_detail> bytes of free space in the journal on disk
#define WAIT_JOURNAL 3
// Suspend operation until the next journal sector buffer is free
#define WAIT_JOURNAL_BUFFER 4

struct blockstore_operation
{
    // flags contain operation type and possibly other flags
    uint64_t flags;
    // finish callback
    std::function<void (blockstore_operation*)> callback;
    // For reads, writes & deletes: oid is the requested object
    object_id oid;
    // For reads: version=0 -> last stable, version=UINT64_MAX -> last unstable, version=X -> specific version
    // For writes & deletes: a new version is assigned automatically
    uint64_t version;
    // For reads & writes: offset & len are the requested part of the object, buf is the buffer
    uint32_t offset;
    // For stabilize requests: buf contains <len> obj_ver_id's to stabilize
    uint32_t len;
    uint8_t *buf; // FIXME: void*
    int retval;

    // FIXME: Move internal fields somewhere
    friend class blockstore;
    friend class blockstore_journal_check_t;
    friend void prepare_journal_sector_write(journal_t & journal, io_uring_sqe *sqe, std::function<void(ring_data_t*)> cb);
private:
    // Wait status
    int wait_for;
    uint64_t wait_detail;
    int pending_ops;

    // Read
    std::map<uint64_t, struct iovec> read_vec;

    // Sync, write
    uint64_t min_used_journal_sector, max_used_journal_sector;

    // Write
    struct iovec iov_zerofill[3];

    // Sync
    std::vector<obj_ver_id> sync_big_writes, sync_small_writes;
    std::list<blockstore_operation*>::iterator in_progress_ptr;
    int sync_state, prev_sync_count;
};

#include "blockstore_init.h"

#include "blockstore_flush.h"

class blockstore
{
    struct ring_consumer_t ring_consumer;

    // Another option is https://github.com/algorithm-ninja/cpp-btree
    spp::sparse_hash_map<object_id, clean_entry, oid_hash> clean_db;
    std::map<obj_ver_id, dirty_entry> dirty_db;
    std::list<blockstore_operation*> submit_queue; // FIXME: funny thing is that vector is better here
    std::vector<obj_ver_id> unsynced_big_writes, unsynced_small_writes;
    std::list<blockstore_operation*> in_progress_syncs; // ...and probably here, too
    uint32_t block_order, block_size;
    uint64_t block_count;
    allocator *data_alloc;
    uint8_t *zero_object;

    int meta_fd;
    int data_fd;

    uint64_t meta_offset, meta_size, meta_area, meta_len;
    uint64_t data_offset, data_size, data_len;

    struct journal_t journal;
    journal_flusher_t *flusher;

    ring_loop_t *ringloop;

    inline struct io_uring_sqe* get_sqe()
    {
        return ringloop->get_sqe();
    }

    friend class blockstore_init_meta;
    friend class blockstore_init_journal;
    friend class blockstore_journal_check_t;
    friend class journal_flusher_t;
    friend class journal_flusher_co;

    void calc_lengths(spp::sparse_hash_map<std::string, std::string> & config);
    void open_data(spp::sparse_hash_map<std::string, std::string> & config);
    void open_meta(spp::sparse_hash_map<std::string, std::string> & config);
    void open_journal(spp::sparse_hash_map<std::string, std::string> & config);

    // Asynchronous init
    int initialized;
    int metadata_buf_size;
    blockstore_init_meta* metadata_init_reader;
    blockstore_init_journal* journal_init_reader;

    void check_wait(blockstore_operation *op);

    // Read
    int dequeue_read(blockstore_operation *read_op);
    int fulfill_read(blockstore_operation *read_op, uint64_t &fulfilled, uint32_t item_start, uint32_t item_end,
        uint32_t item_state, uint64_t item_version, uint64_t item_location);
    int fulfill_read_push(blockstore_operation *read_op, uint64_t &fulfilled, uint32_t item_start,
        uint32_t item_state, uint64_t item_version, uint64_t item_location, uint32_t cur_start, uint32_t cur_end);
    void handle_read_event(ring_data_t *data, blockstore_operation *op);

    // Write
    void enqueue_write(blockstore_operation *op);
    int dequeue_write(blockstore_operation *op);
    void handle_write_event(ring_data_t *data, blockstore_operation *op);

    // Sync
    int dequeue_sync(blockstore_operation *op);
    void handle_sync_event(ring_data_t *data, blockstore_operation *op);
    int continue_sync(blockstore_operation *op);
    int ack_sync(blockstore_operation *op);

    // Stabilize
    int dequeue_stable(blockstore_operation *op);
    void handle_stable_event(ring_data_t *data, blockstore_operation *op);
    void stabilize_object(object_id oid, uint64_t max_ver);

public:

    blockstore(spp::sparse_hash_map<std::string, std::string> & config, ring_loop_t *ringloop);
    ~blockstore();

    // Event loop
    void loop();

    bool is_started();

    // Returns true when it's safe to destroy the instance. If destroying the instance
    // requires to purge some queues, starts that process. Should be called in the event
    // loop until it returns true.
    bool stop();

    // Submission
    int enqueue_op(blockstore_operation *op);
};
