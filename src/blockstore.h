// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#pragma once

#ifndef _LARGEFILE64_SOURCE
#define _LARGEFILE64_SOURCE
#endif

#include <stdint.h>

#include <string>
#include <map>
#include <unordered_map>
#include <functional>

#include "object_id.h"
#include "ringloop.h"
#include "timerfd_manager.h"

// Memory alignment for direct I/O (usually 512 bytes)
// All other alignments must be a multiple of this one
#ifndef MEM_ALIGNMENT
#define MEM_ALIGNMENT 512
#endif

// Default block size is 128 KB, current allowed range is 4K - 128M
#define DEFAULT_ORDER 17
#define MIN_BLOCK_SIZE 4*1024
#define MAX_BLOCK_SIZE 128*1024*1024
#define DEFAULT_BITMAP_GRANULARITY 4096

#define BS_OP_MIN 1
#define BS_OP_READ 1
#define BS_OP_WRITE 2
#define BS_OP_WRITE_STABLE 3
#define BS_OP_SYNC 4
#define BS_OP_STABLE 5
#define BS_OP_DELETE 6
#define BS_OP_LIST 7
#define BS_OP_ROLLBACK 8
#define BS_OP_SYNC_STAB_ALL 9
#define BS_OP_MAX 9

#define BS_OP_PRIVATE_DATA_SIZE 256

/*

Blockstore opcode documentation:

## BS_OP_READ / BS_OP_WRITE / BS_OP_WRITE_STABLE

Read or write object data. WRITE_STABLE writes a version that doesn't require marking as stable.

Input:
- oid = requested object
- version = requested version.
  For reads:
  - version == 0: read the last stable version,
  - version == UINT64_MAX: read the last version,
  - otherwise: read the newest version that is <= the specified version
  - reads aren't guaranteed to return data from previous unfinished writes
  For writes:
  - if version == 0, a new version is assigned automatically
  - if version != 0, it is assigned for the new write if possible, otherwise -EINVAL is returned
- offset, len = offset and length within object. length may be zero, in that case
  read operation only returns the version / write operation only bumps the version
- buf = pre-allocated buffer for data (read) / with data (write). may be NULL if len == 0.
- bitmap = pointer to the new 'external' object bitmap data. Its part which is respective to the
  write request is copied into the metadata area bitwise and stored there.

Output:
- retval = number of bytes actually read/written or negative error number (-EINVAL or -ENOSPC)
- version = the version actually read or written

## BS_OP_DELETE

Delete an object.

Input:
- oid = requested object
- version = requested version. Treated the same as with BS_OP_WRITE

Output:
- retval = 0 or negative error number (-EINVAL)
- version = the version actually written (delete is initially written as an object version)

## BS_OP_SYNC

Make sure all previously issued modifications reach physical media.

Input: Nothing except opcode
Output:
- retval = 0 or negative error number (-EINVAL)

## BS_OP_STABLE / BS_OP_ROLLBACK

Mark objects as stable / rollback previous unstable writes.

Input:
- len = count of obj_ver_id's to stabilize or rollback
  - stabilize: all object versions up to the requested version of each object are marked as stable
  - rollback: all objects are rolled back to the requested stable versions
- buf = pre-allocated obj_ver_id array <len> units long

Output:
- retval = 0 or negative error number (-EINVAL, -ENOENT if no such version or -EBUSY if not synced)

## BS_OP_SYNC_STAB_ALL

ONLY FOR TESTS! Sync and mark all unstable object versions as stable, at once.

Input: Nothing except opcode
Output:
- retval = 0 or negative error number (-EINVAL)

## BS_OP_LIST

Get a list of all objects in this Blockstore.

Input:
- oid.stripe = PG alignment
- len = PG count or 0 to list all objects
- offset = PG number
- oid.inode = min inode number or 0 to list all inodes
- version = max inode number or 0 to list all inodes

Output:
- retval = total obj_ver_id count
- version = stable obj_ver_id count
- buf = obj_ver_id array allocated by the blockstore. Stable versions come first.
  You must free it yourself after usage with free().
  Output includes all objects for which (((inode + stripe / <PG alignment>) % <PG count>) == <PG number>).

*/

struct blockstore_op_t
{
    // operation
    uint64_t opcode;
    // finish callback
    std::function<void (blockstore_op_t*)> callback;
    object_id oid;
    uint64_t version;
    uint32_t offset;
    uint32_t len;
    void *buf;
    void *bitmap;
    int retval;

    uint8_t private_data[BS_OP_PRIVATE_DATA_SIZE];
};

typedef std::unordered_map<std::string, std::string> blockstore_config_t;

class blockstore_impl_t;

class blockstore_t
{
    blockstore_impl_t *impl;
public:
    blockstore_t(blockstore_config_t & config, ring_loop_t *ringloop, timerfd_manager_t *tfd);
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

    // Simplified synchronous operation: get object bitmap & current version
    int read_bitmap(object_id oid, uint64_t target_version, void *bitmap, uint64_t *result_version = NULL);

    // Unstable writes are added here (map of object_id -> version)
    std::unordered_map<object_id, uint64_t> & get_unstable_writes();

    // Get per-inode space usage statistics
    std::map<uint64_t, uint64_t> & get_inode_space_stats();

    // FIXME rename to object_size
    uint32_t get_block_size();
    uint64_t get_block_count();
    uint64_t get_free_block_count();

    uint32_t get_bitmap_granularity();
};
