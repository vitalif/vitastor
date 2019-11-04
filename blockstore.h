#pragma once

#define _LARGEFILE64_SOURCE
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
#include <deque>
#include <set>
#include <functional>

#include "allocator.h"
#include "sparsepp/sparsepp/spp.h"

// States are not stored on disk. Instead, they're deduced from the journal

#define ST_IN_FLIGHT 1
#define ST_J_WRITTEN 2
#define ST_J_SYNCED 3
#define ST_J_STABLE 4
#define ST_J_MOVED 5
#define ST_J_MOVE_SYNCED 6
#define ST_D_WRITTEN 16
#define ST_D_SYNCED 17
#define ST_D_META_WRITTEN 18
#define ST_D_META_SYNCED 19
#define ST_D_STABLE 20
#define ST_D_META_MOVED 21
#define ST_D_META_COMMITTED 22
#define ST_DEL_WRITTEN 23
#define ST_DEL_SYNCED 24
#define ST_DEL_STABLE 25
#define ST_DEL_MOVED 26
#define ST_CURRENT 32
#define IS_STABLE(st) ((st) == 4 || (st) == 5 || (st) == 6 || (st) == 20 || (st) == 21 || (st) == 22 || (st) == 32 || (st) == 24 || (st) == 25)
#define IS_JOURNAL(st) (st >= 2 && st <= 6)

// Default object size is 128 KB
#define DEFAULT_ORDER 17
#define MAX_BLOCK_SIZE 128*1024*1024
#define DISK_ALIGNMENT 512

#define STRIPE_NUM(oid) ((oid) >> 4)
#define STRIPE_REPLICA(oid) ((oid) & 0xf)

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
    return b.inode == a.inode && b.stripe == a.stripe;
}

// 32 bytes per "clean" entry on disk with fixed metadata tables
struct __attribute__((__packed__)) clean_disk_entry
{
    object_id oid;
    uint64_t version;
    uint8_t flags;
    uint8_t reserved[7];
};

// 28 bytes per "clean" entry in memory
struct __attribute__((__packed__)) clean_entry
{
    uint64_t version;
    uint32_t state;
    uint64_t location;
};

// 48 bytes per dirty entry in memory
struct __attribute__((__packed__)) dirty_entry
{
    uint64_t version;
    uint32_t state;
    uint32_t flags;
    uint64_t location; // location in either journal or data
    uint32_t offset;   // offset within stripe
    uint32_t size;     // entry size
};

typedef std::vector<dirty_entry> dirty_list;

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

// SYNC must be submitted after previous WRITEs/DELETEs (not before!)
// READs to the same object must be submitted after previous WRITEs/DELETEs
// Otherwise, the submit order is free, that is all operations may be submitted immediately
// In fact, adding a write operation must immediately result in dirty_queue being populated

#define OP_READ 1
#define OP_READ_DIRTY 2
#define OP_WRITE 3
#define OP_SYNC 4
#define OP_STABLE 5
#define OP_DELETE 6

#define WAIT_SQE 1
#define WAIT_IN_FLIGHT 2

struct blockstore_operation
{
    std::function<void (blockstore_operation*)> callback;

    uint32_t flags;
    object_id oid;
    uint64_t version;
    uint32_t offset;
    uint32_t len;
    uint8_t *buf;

    std::map<uint64_t, struct iovec> read_vec;
    int completed;
    int wait_for;
    uint64_t wait_version;
};

class blockstore;

#include "blockstore_init.h"

class blockstore
{
public:
    spp::sparse_hash_map<object_id, clean_entry, oid_hash> object_db;
    spp::sparse_hash_map<object_id, dirty_list, oid_hash> dirty_queue;
    std::deque<blockstore_operation*> submit_queue;
    std::set<blockstore_operation*> in_process_ops;
    uint32_t block_order, block_size;
    uint64_t block_count;
    allocator *data_alloc;

    int journal_fd;
    int meta_fd;
    int data_fd;

    uint64_t journal_offset, journal_size, journal_len;
    uint64_t meta_offset, meta_size, meta_area, meta_len;
    uint64_t data_offset, data_size, data_len;

    uint64_t journal_start, journal_end;
    uint32_t journal_crc32_last;

    struct io_uring *ring;

    blockstore(spp::sparse_hash_map<std::string, std::string> & config, struct io_uring *ring);
    ~blockstore();

    void calc_lengths(spp::sparse_hash_map<std::string, std::string> & config);
    void open_data(spp::sparse_hash_map<std::string, std::string> & config);
    void open_meta(spp::sparse_hash_map<std::string, std::string> & config);
    void open_journal(spp::sparse_hash_map<std::string, std::string> & config);

    // Asynchronous init
    int initialized;
    int metadata_buf_size;
    blockstore_init_meta* metadata_init_reader;
    blockstore_init_journal* journal_init_reader;
    int init_loop();

    // Read
    int read(blockstore_operation *read_op);
    int fulfill_read(blockstore_operation & read_op, uint32_t item_start, uint32_t item_end,
        uint32_t item_state, uint64_t item_version, uint64_t item_location);
    int fulfill_read_push(blockstore_operation & read_op, uint32_t item_start,
        uint32_t item_state, uint64_t item_version, uint64_t item_location, uint32_t cur_start, uint32_t cur_end);
};
