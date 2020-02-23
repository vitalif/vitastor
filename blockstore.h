#pragma once

#ifndef _LARGEFILE64_SOURCE
#define _LARGEFILE64_SOURCE
#endif

#include <stdint.h>

#include <map>
#include <unordered_map>
#include <functional>

#include "object_id.h"
#include "ringloop.h"

// Memory alignment for direct I/O (usually 512 bytes)
// All other alignments must be a multiple of this one
#define MEM_ALIGNMENT 512

// Default block size is 128 KB, current allowed range is 4K - 128M
#define DEFAULT_ORDER 17
#define MIN_BLOCK_SIZE 4*1024
#define MAX_BLOCK_SIZE 128*1024*1024

#define BS_OP_MIN 1
#define BS_OP_READ 1
#define BS_OP_WRITE 2
#define BS_OP_SYNC 3
#define BS_OP_STABLE 4
#define BS_OP_DELETE 5
#define BS_OP_LIST 6
#define BS_OP_ROLLBACK 7
#define BS_OP_SYNC_STAB_ALL 8
#define BS_OP_MAX 8

#define BS_OP_PRIVATE_DATA_SIZE 256

/* BS_OP_LIST:

Input:
- oid.stripe = parity block size
- len = PG count or 0 to list all objects
- offset = PG number

Output:
- retval = total obj_ver_id count
- version = stable obj_ver_id count
- buf = obj_ver_id array allocated by the blockstore. stable versions come first

*/

struct blockstore_op_t
{
    // operation
    uint64_t opcode;
    // finish callback
    std::function<void (blockstore_op_t*)> callback;
    // For reads, writes & deletes: oid is the requested object
    object_id oid;
    // For reads:
    //   version == 0 -> read the last stable version,
    //   version == UINT64_MAX -> read the last version,
    //   otherwise -> read the newest version that is <= the specified version
    //   after execution, version is equal to the version that was read from the blockstore
    // For writes & deletes:
    //   if version == 0, a new version is assigned automatically
    //   if version != 0, it is assigned for the new write if possible, otherwise -EINVAL is returned
    //   after execution, version is equal to the version that was written to the blockstore
    uint64_t version;
    // For reads & writes: offset & len are the requested part of the object, buf is the buffer
    uint32_t offset;
    // For stabilize requests: buf contains <len> obj_ver_id's to stabilize
    uint32_t len;
    void *buf;
    int retval;

    uint8_t private_data[BS_OP_PRIVATE_DATA_SIZE];
};

typedef std::unordered_map<std::string, std::string> blockstore_config_t;

class blockstore_impl_t;

class blockstore_t
{
    blockstore_impl_t *impl;
public:
    blockstore_t(blockstore_config_t & config, ring_loop_t *ringloop);
    ~blockstore_t();

    // Event loop
    void loop();

    // Returns true when blockstore is ready to process operations
    // (Although you're free to enqueue them before that)
    bool is_started();

    // Returns true when blockstore is stalled
    bool is_stalled();

    // Returns true when it's safe to destroy the instance. If destroying the instance
    // requires to purge some queues, starts that process. Should be called in the event
    // loop until it returns true.
    bool is_safe_to_stop();

    // Submission
    void enqueue_op(blockstore_op_t *op);

    // Insert operation into the beginning of the queue
    // Intended for the OSD syncer "thread" to be able to stabilize something when the journal is full
    void enqueue_op_first(blockstore_op_t *op);

    // Unstable writes are added here (map of object_id -> version)
    std::unordered_map<object_id, uint64_t> & get_unstable_writes();

    // FIXME rename to object_size
    uint32_t get_block_size();
    uint64_t get_block_count();

    uint32_t get_disk_alignment();
};
