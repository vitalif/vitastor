#pragma once

#include "blockstore.h"
#include <stdint.h>

// Magic numbers
#define SECONDARY_OSD_OP_MAGIC      0x2bd7b10325434553l
#define SECONDARY_OSD_REPLY_MAGIC   0xbaa699b87b434553l
// Operation request headers and operation reply headers have fixed size after which comes data
#define OSD_OP_PACKET_SIZE          0x80
#define OSD_REPLY_PACKET_SIZE       0x40
// Opcodes
#define OSD_OP_MIN                  0x01
#define OSD_OP_SECONDARY_READ       0x01
#define OSD_OP_SECONDARY_WRITE      0x02
#define OSD_OP_SECONDARY_SYNC       0x03
#define OSD_OP_SECONDARY_STABILIZE  0x04
#define OSD_OP_SECONDARY_DELETE     0x05
#define OSD_OP_TEST_SYNC_STAB_ALL   0x06
#define OSD_OP_SECONDARY_LIST       0x07
#define OSD_OP_MAX                  0x07
// Alignment & limit for read/write operations
#define OSD_RW_ALIGN                512
#define OSD_RW_MAX                  64*1024*1024

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
    // return value
    int64_t retval;
};

// read or write to the secondary OSD
struct __attribute__((__packed__)) osd_op_secondary_rw_t
{
    osd_op_header_t header;
    // object
    object_id oid;
    // read/write version (automatic or specific)
    uint64_t version;
    // offset
    uint32_t offset;
    // length
    uint32_t len;
};

struct __attribute__((__packed__)) osd_reply_secondary_rw_t
{
    osd_reply_header_t header;
};

// delete object on the secondary OSD
struct __attribute__((__packed__)) osd_op_secondary_del_t
{
    osd_op_header_t header;
    // object
    object_id oid;
    // delete version (automatic or specific)
    uint64_t version;
};

struct __attribute__((__packed__)) osd_reply_secondary_del_t
{
    osd_reply_header_t header;
};

// sync to the secondary OSD
struct __attribute__((__packed__)) osd_op_secondary_sync_t
{
    osd_op_header_t header;
};

struct __attribute__((__packed__)) osd_reply_secondary_sync_t
{
    osd_reply_header_t header;
};

// stabilize objects on the secondary OSD
struct __attribute__((__packed__)) osd_op_secondary_stabilize_t
{
    osd_op_header_t header;
    // obj_ver_id array length in bytes
    uint32_t len;
};

struct __attribute__((__packed__)) osd_reply_secondary_stabilize_t
{
    osd_reply_header_t header;
};

// list objects on replica
struct __attribute__((__packed__)) osd_op_secondary_list_t
{
    osd_op_header_t header;
    // placement group number
    uint64_t pgnum;
};

struct __attribute__((__packed__)) osd_reply_secondary_list_t
{
    osd_reply_header_t header;
};

union osd_any_op_t
{
    osd_op_header_t hdr;
    osd_op_secondary_rw_t sec_rw;
    osd_op_secondary_del_t sec_del;
    osd_op_secondary_sync_t sec_sync;
    osd_op_secondary_stabilize_t sec_stabilize;
    osd_op_secondary_list_t sec_list;
};

union osd_any_reply_t
{
    osd_reply_header_t hdr;
    osd_reply_secondary_rw_t sec_rw;
    osd_reply_secondary_del_t sec_del;
    osd_reply_secondary_sync_t sec_sync;
    osd_reply_secondary_stabilize_t sec_stabilize;
    osd_reply_secondary_list_t sec_list;
};
