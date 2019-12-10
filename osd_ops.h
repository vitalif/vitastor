#pragma once

#include <stdint.h>

#define SECONDARY_OSD_OP_MAGIC      0xf3f003b966ace9ab2bd7b10325434553
#define SECONDARY_OSD_REPLY_MAGIC   0xd17a57243b580b99baa699b87b434553
#define OSD_OP_SECONDARY_READ       0x01
#define OSD_OP_SECONDARY_WRITE      0x02
#define OSD_OP_SECONDARY_SYNC       0x03
#define OSD_OP_SECONDARY_STABILIZE  0x04
#define OSD_OP_SECONDARY_DELETE     0x05
#define OSD_OP_SECONDARY_LIST       0x10

// common header of all operations
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
    struct __attribute__((__packed__))
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
    } packet;
    // buffer (direct IO aligned)
    void *buf;
};

struct __attribute__((__packed__)) osd_reply_secondary_rw_t
{
    struct __attribute__((__packed__))
    {
        osd_reply_header_t header;
        // buffer size
        uint64_t len;
    } packet;
    // buffer (direct IO aligned)
    void *buf;
};

// delete object on the secondary OSD
struct __attribute__((__packed__)) osd_op_secondary_del_t
{
    struct __attribute__((__packed__))
    {
        osd_op_header_t header;
        // object
        object_id oid;
        // delete version (automatic or specific)
        uint64_t version;
    } packet;
};

struct __attribute__((__packed__)) osd_reply_secondary_del_t
{
    struct __attribute__((__packed__))
    {
        osd_op_header_t header;
    } packet;
};

// sync to the secondary OSD
struct __attribute__((__packed__)) osd_op_secondary_sync_t
{
    struct __attribute__((__packed__))
    {
        osd_op_header_t header;
    } packet;
};

struct __attribute__((__packed__)) osd_reply_secondary_sync_t
{
    struct __attribute__((__packed__))
    {
        osd_op_header_t header;
    } packet;
};

// stabilize objects on the secondary OSD
struct __attribute__((__packed__)) osd_op_secondary_stabilize_t
{
    struct __attribute__((__packed__))
    {
        osd_op_header_t header;
        // oid array length
        uint32_t len;
    } packet;
    // oid array
    object_id *oids;
};

struct __attribute__((__packed__)) osd_reply_secondary_stabilize_t
{
    struct __attribute__((__packed__))
    {
        osd_op_header_t header;
    } packet;
};

// list objects on replica
struct __attribute__((__packed__)) osd_op_secondary_list_t
{
    struct __attribute__((__packed__))
    {
        osd_op_header_t header;
        // placement group number
        uint64_t pgnum;
    } packet;
};

struct __attribute__((__packed__)) osd_reply_secondary_list_t
{
    struct __attribute__((__packed__))
    {
        osd_reply_header_t header;
        // oid array length
        uint64_t len;
    } packet;
    // oid array
    object_id *oids;
};
