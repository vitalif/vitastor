// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#pragma once

#include "object_id.h"
#include "osd_id.h"

// Magic numbers
#define SECONDARY_OSD_OP_MAGIC      0x2bd7b10325434553l
#define SECONDARY_OSD_REPLY_MAGIC   0xbaa699b87b434553l
// Operation request / reply headers have fixed size after which comes data
#define OSD_PACKET_SIZE             0x80
// Opcodes
#define OSD_OP_MIN                  1
#define OSD_OP_SEC_READ             1
#define OSD_OP_SEC_WRITE            2
#define OSD_OP_SEC_WRITE_STABLE     3
#define OSD_OP_SEC_SYNC             4
#define OSD_OP_SEC_STABILIZE        5
#define OSD_OP_SEC_ROLLBACK         6
#define OSD_OP_SEC_DELETE           7
#define OSD_OP_TEST_SYNC_STAB_ALL   8
#define OSD_OP_SEC_LIST             9
#define OSD_OP_SHOW_CONFIG          10
#define OSD_OP_READ                 11
#define OSD_OP_WRITE                12
#define OSD_OP_SYNC                 13
#define OSD_OP_DELETE               14
#define OSD_OP_PING                 15
#define OSD_OP_SEC_READ_BMP         16
#define OSD_OP_SCRUB                17
#define OSD_OP_DESCRIBE             18
#define OSD_OP_SEC_LOCK             19
#define OSD_OP_MAX                  19
#define OSD_RW_MAX                  64*1024*1024
#define OSD_PROTOCOL_VERSION        1

#define OSD_OP_RECOVERY_RELATED     (uint32_t)1
#define OSD_OP_IGNORE_PG_LOCK       (uint32_t)2

// Memory alignment for direct I/O (usually 512 bytes)
#ifndef DIRECT_IO_ALIGNMENT
#define DIRECT_IO_ALIGNMENT 512
#endif

// Memory allocation alignment (page size is usually optimal)
#ifndef MEM_ALIGNMENT
#define MEM_ALIGNMENT 4096
#endif

// Constants for osd_reply_describe_item_t.loc_bad
#define LOC_OUTDATED 1
#define LOC_CORRUPTED 2
#define LOC_INCONSISTENT 4

#define OSD_LIST_PRIMARY 1

#define OSD_DEL_SUPPORT_LEFT_ON_DEAD 1
#define OSD_DEL_LEFT_ON_DEAD         2

#define OSD_SEC_LOCK_PG 1
#define OSD_SEC_UNLOCK_PG 2

// common request and reply headers
struct __attribute__((__packed__)) osd_op_header_t
{
    // magic & protocol version
    uint64_t magic;
    // operation id
    uint64_t id;
    // operation type
    uint64_t opcode;
};

struct __attribute__((__packed__)) osd_reply_header_t
{
    // magic & protocol version
    uint64_t magic;
    // operation id
    uint64_t id;
    // operation type
    uint64_t opcode;
    // return value
    int64_t retval;
};

// read or write to the secondary OSD
struct __attribute__((__packed__)) osd_op_sec_rw_t
{
    osd_op_header_t header;
    // object
    object_id oid;
    // read/write version (automatic or specific)
    // FIXME deny values close to UINT64_MAX
    uint64_t version;
    // offset
    uint32_t offset;
    // length
    uint32_t len;
    // bitmap/attribute length - bitmap comes after header, but before data
    uint32_t attr_len;
    // OSD_OP_RECOVERY_RELATED, OSD_OP_IGNORE_PG_LOCK
    uint32_t flags;
};

struct __attribute__((__packed__)) osd_reply_sec_rw_t
{
    osd_reply_header_t header;
    // for reads and writes: assigned or read version number
    uint64_t version;
    // for reads: bitmap/attribute length (just to double-check)
    uint32_t attr_len;
    uint32_t pad0;
};

// delete object on the secondary OSD
struct __attribute__((__packed__)) osd_op_sec_del_t
{
    osd_op_header_t header;
    // object
    object_id oid;
    // delete version (automatic or specific)
    uint64_t version;
    // OSD_OP_RECOVERY_RELATED, OSD_OP_IGNORE_PG_LOCK
    uint32_t flags;
    uint32_t pad0;
};

struct __attribute__((__packed__)) osd_reply_sec_del_t
{
    osd_reply_header_t header;
    uint64_t version;
};

// sync to the secondary OSD
struct __attribute__((__packed__)) osd_op_sec_sync_t
{
    osd_op_header_t header;
    // OSD_OP_RECOVERY_RELATED, OSD_OP_IGNORE_PG_LOCK
    uint32_t flags;
    uint32_t pad0;
};

struct __attribute__((__packed__)) osd_reply_sec_sync_t
{
    osd_reply_header_t header;
};

// stabilize or rollback objects on the secondary OSD
struct __attribute__((__packed__)) osd_op_sec_stab_t
{
    osd_op_header_t header;
    // obj_ver_id array length in bytes
    uint64_t len;
    // OSD_OP_RECOVERY_RELATED, OSD_OP_IGNORE_PG_LOCK
    uint32_t flags;
    uint32_t pad0;
};
typedef osd_op_sec_stab_t osd_op_sec_rollback_t;

struct __attribute__((__packed__)) osd_reply_sec_stab_t
{
    osd_reply_header_t header;
};
typedef osd_reply_sec_stab_t osd_reply_sec_rollback_t;

// bulk read bitmaps from a secondary OSD
struct __attribute__((__packed__)) osd_op_sec_read_bmp_t
{
    osd_op_header_t header;
    // obj_ver_id array length in bytes
    uint64_t len;
    // OSD_OP_RECOVERY_RELATED, OSD_OP_IGNORE_PG_LOCK
    uint32_t flags;
};

struct __attribute__((__packed__)) osd_reply_sec_read_bmp_t
{
    // retval is payload length in bytes. payload is {version,bitmap}[]
    osd_reply_header_t header;
};

// show configuration and remember peer information
struct __attribute__((__packed__)) osd_op_show_config_t
{
    osd_op_header_t header;
    // JSON request length
    uint64_t json_len;
};

struct __attribute__((__packed__)) osd_reply_show_config_t
{
    osd_reply_header_t header;
};

// list objects on replica
struct __attribute__((__packed__)) osd_op_sec_list_t
{
    osd_op_header_t header;
    // placement group total number and total count
    pg_num_t list_pg, pg_count;
    // size of an area that maps to one PG continuously
    uint64_t pg_stripe_size;
    // inode range (used to select pools)
    uint64_t min_inode, max_inode;
    // min/max oid stripe, added after inodes for backwards compatibility
    // also for backwards compatibility, max_stripe=UINT64_MAX means 0 and 0 means UINT64_MAX O_o
    uint64_t min_stripe, max_stripe;
    // max stable object count
    uint32_t stable_limit;
    // flags - OSD_LIST_PRIMARY or 0
    // for OSD_LIST_PRIMARY, only a single-PG listing is allowed
    uint64_t flags;
};

struct __attribute__((__packed__)) osd_reply_sec_list_t
{
    osd_reply_header_t header;
    // stable object version count. header.retval = total object version count
    // FIXME: maybe change to the number of bytes in the reply...
    uint64_t stable_count;
    // flags - OSD_LIST_PRIMARY or 0
    uint64_t flags;
};

// read, write or delete command for the primary OSD (must be within individual stripe)
struct __attribute__((__packed__)) osd_op_rw_t
{
    osd_op_header_t header;
    // inode
    uint64_t inode;
    // offset
    uint64_t offset;
    // length. 0 means to read all bitmaps of the specified range, but no data.
    uint32_t len;
    // flags (for future)
    uint32_t flags;
    // inode metadata revision
    uint64_t meta_revision;
    // object version for atomic "CAS" (compare-and-set) writes
    // writes and deletes fail with -EINTR if object version differs from (version-1)
    uint64_t version;
};

struct __attribute__((__packed__)) osd_reply_rw_t
{
    osd_reply_header_t header;
    // for reads: bitmap length
    uint32_t bitmap_len;
    uint32_t pad0;
    // for reads and writes: object version
    uint64_t version;
};

struct __attribute__((__packed__)) osd_reply_del_t
{
    osd_reply_header_t header;
    // OSD_DEL_SUPPORT_LEFT_ON_DEAD and/or OSD_DEL_LEFT_ON_DEAD or 0
    uint32_t flags;
    // for deletes, if flags & OSD_DEL_LEFT_ON_DEAD:
    // count of OSDs from which the object could be not deleted
    // these come directly after this del_left_on_dead_list_size as uint32_t[]
    // FIXME it's kind of a hack and will be removed in the future, when Vitastor will
    // have 'atomic deletions', i.e. when it will be able to remember deleted objects
    // and complete deletions automatically after extra OSDs are started
    uint32_t left_on_dead_count;
};

// sync to the primary OSD
struct __attribute__((__packed__)) osd_op_sync_t
{
    osd_op_header_t header;
};

struct __attribute__((__packed__)) osd_reply_sync_t
{
    osd_reply_header_t header;
};

// describe unclean object states in detail
struct __attribute__((__packed__)) osd_op_describe_t
{
    osd_op_header_t header;
    // state mask to filter objects by state (0 or 0xfff..ff = all objects)
    uint64_t object_state;
    // minimum inode and offset
    uint64_t min_inode, min_offset;
    // maximum inode and offset
    uint64_t max_inode, max_offset;
    // limit
    uint64_t limit;
    // pool and PG
    uint32_t pool_id;
    uint32_t pg_num;
};

struct __attribute__((__packed__)) osd_reply_describe_t
{
    osd_reply_header_t header;
    // size of the resulting <osd_reply_describe_item_t> array in bytes
    uint64_t result_bytes;
};

struct __attribute__((__packed__)) osd_reply_describe_item_t
{
    uint64_t inode;
    uint64_t stripe;
    uint32_t role;      // part number: 0 for replicas, 0..pg_size-1 for EC
    uint32_t loc_bad;   // LOC_OUTDATED / LOC_CORRUPTED / LOC_INCONSISTENT
    osd_num_t osd_num;  // OSD number
};

// lock/unlock PG for use by a primary OSD
struct __attribute__((__packed__)) osd_op_sec_lock_t
{
    osd_op_header_t header;
    // OSD_SEC_LOCK_PG or OSD_SEC_UNLOCK_PG
    uint64_t flags;
    // Pool ID and PG number
    uint64_t pool_id;
    uint64_t pg_num;
    // PG state as calculated by the primary OSD
    uint64_t pg_state;
};

struct __attribute__((__packed__)) osd_reply_sec_lock_t
{
    osd_reply_header_t header;
    uint64_t cur_primary;
};

// FIXME it would be interesting to try to unify blockstore_op and osd_op formats
union osd_any_op_t
{
    osd_op_header_t hdr;
    osd_op_sec_rw_t sec_rw;
    osd_op_sec_del_t sec_del;
    osd_op_sec_sync_t sec_sync;
    osd_op_sec_stab_t sec_stab;
    osd_op_sec_read_bmp_t sec_read_bmp;
    osd_op_sec_list_t sec_list;
    osd_op_sec_lock_t sec_lock;
    osd_op_show_config_t show_conf;
    osd_op_rw_t rw;
    osd_op_sync_t sync;
    osd_op_describe_t describe;
    uint8_t buf[OSD_PACKET_SIZE];
};

union osd_any_reply_t
{
    osd_reply_header_t hdr;
    osd_reply_sec_rw_t sec_rw;
    osd_reply_sec_del_t sec_del;
    osd_reply_sec_sync_t sec_sync;
    osd_reply_sec_stab_t sec_stab;
    osd_reply_sec_read_bmp_t sec_read_bmp;
    osd_reply_sec_list_t sec_list;
    osd_reply_sec_lock_t sec_lock;
    osd_reply_show_config_t show_conf;
    osd_reply_rw_t rw;
    osd_reply_del_t del;
    osd_reply_sync_t sync;
    osd_reply_describe_t describe;
    uint8_t buf[OSD_PACKET_SIZE];
};

extern const char* osd_op_names[];
