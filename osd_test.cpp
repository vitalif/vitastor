// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.0 (see README.md for details)

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <malloc.h>

#include <stdexcept>

#include "osd_ops.h"
#include "rw_blocking.h"
#include "test_pattern.h"

int connect_osd(const char *osd_address, int osd_port);

uint64_t test_read(int connect_fd, uint64_t inode, uint64_t stripe, uint64_t version, uint64_t offset, uint64_t len);

uint64_t test_write(int connect_fd, uint64_t inode, uint64_t stripe, uint64_t version, uint64_t pattern);

void* test_primary_read(int connect_fd, uint64_t inode, uint64_t offset, uint64_t len);

void test_primary_write(int connect_fd, uint64_t inode, uint64_t offset, uint64_t len, uint64_t pattern);

void test_primary_sync(int connect_fd);

void test_sync_stab_all(int connect_fd);

void test_list_stab(int connect_fd);

int main0(int narg, char *args[])
{
    int connect_fd;
    // Prepare data for cluster read
    connect_fd = connect_osd("127.0.0.1", 11203);
    test_write(connect_fd, 2, 0, 1, PATTERN0);
    close(connect_fd);
    connect_fd = connect_osd("127.0.0.1", 11204);
    test_write(connect_fd, 2, 1, 1, PATTERN1);
    close(connect_fd);
    connect_fd = connect_osd("127.0.0.1", 11205);
    test_write(connect_fd, 2, 2, 1, PATTERN0^PATTERN1);
    close(connect_fd);
    return 0;
}

int main1(int narg, char *args[])
{
    int connect_fd;
    void *data;
    // Cluster read
    connect_fd = connect_osd("127.0.0.1", 11203);
    data = test_primary_read(connect_fd, 2, 0, 128*1024);
    if (data)
    {
        check_pattern(data, 128*1024, PATTERN0);
        printf("inode=2 0-128K OK\n");
        free(data);
    }
    data = test_primary_read(connect_fd, 2, 0, 256*1024);
    if (data)
    {
        check_pattern(data, 128*1024, PATTERN0);
        check_pattern(data+128*1024, 128*1024, PATTERN1);
        printf("inode=2 0-256K OK\n");
        free(data);
    }
    close(connect_fd);
    return 0;
}

int main2(int narg, char *args[])
{
    int connect_fd;
    // Cluster write (sync not implemented yet)
    connect_fd = connect_osd("127.0.0.1", 11203);
    test_primary_write(connect_fd, 2, 0, 128*1024, PATTERN0);
    test_primary_write(connect_fd, 2, 128*1024, 128*1024, PATTERN1);
    test_sync_stab_all(connect_fd);
    close(connect_fd);
    connect_fd = connect_osd("127.0.0.1", 11204);
    if (connect_fd >= 0)
    {
        test_sync_stab_all(connect_fd);
        close(connect_fd);
    }
    connect_fd = connect_osd("127.0.0.1", 11205);
    if (connect_fd >= 0)
    {
        test_sync_stab_all(connect_fd);
        close(connect_fd);
    }
    return 0;
}

int main3(int narg, char *args[])
{
    int connect_fd;
    connect_fd = connect_osd("127.0.0.1", 11203);
    test_list_stab(connect_fd);
    close(connect_fd);
    return 0;
}

int main4(int narg, char *args[])
{
    int connect_fd;
    // Cluster write (sync not implemented yet)
    connect_fd = connect_osd("127.0.0.1", 11203);
    test_primary_write(connect_fd, 2, 0, 128*1024, PATTERN0);
    test_primary_write(connect_fd, 2, 128*1024, 128*1024, PATTERN1);
    test_primary_sync(connect_fd);
    close(connect_fd);
    return 0;
}

int main(int narg, char *args[])
{
    int connect_fd;
    connect_fd = connect_osd("192.168.7.2", 43051);
    test_read(connect_fd, 1, 1039663104, UINT64_MAX, 0, 128*1024);
    close(connect_fd);
    return 0;
}

int connect_osd(const char *osd_address, int osd_port)
{
    struct sockaddr_in addr;
    int r;
    if ((r = inet_pton(AF_INET, osd_address, &addr.sin_addr)) != 1)
    {
        fprintf(stderr, "server address: %s%s\n", osd_address, r == 0 ? " is not valid" : ": no ipv4 support");
        return -1;
    }
    addr.sin_family = AF_INET;
    addr.sin_port = htons(osd_port);

    int connect_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (connect_fd < 0)
    {
        perror("socket");
        return -1;
    }
    if (connect(connect_fd, (sockaddr*)&addr, sizeof(addr)) < 0)
    {
        perror("connect");
        return -1;
    }
    int one = 1;
    setsockopt(connect_fd, SOL_TCP, TCP_NODELAY, &one, sizeof(one));

    return connect_fd;
}

bool check_reply(int r, osd_any_op_t & op, osd_any_reply_t & reply, int expected)
{
    if (r != OSD_PACKET_SIZE)
    {
        printf("read failed\n");
        return false;
    }
    if (reply.hdr.magic != SECONDARY_OSD_REPLY_MAGIC ||
        reply.hdr.id != op.hdr.id || reply.hdr.opcode != op.hdr.opcode)
    {
        printf("bad reply: magic, id or opcode does not match request\n");
        return false;
    }
    if (expected >= 0 && reply.hdr.retval != expected)
    {
        printf("operation failed, retval=%ld\n", reply.hdr.retval);
        return false;
    }
    return true;
}

uint64_t test_read(int connect_fd, uint64_t inode, uint64_t stripe, uint64_t version, uint64_t offset, uint64_t len)
{
    osd_any_op_t op;
    osd_any_reply_t reply;
    op.hdr.magic = SECONDARY_OSD_OP_MAGIC;
    op.hdr.id = 1;
    op.hdr.opcode = OSD_OP_SEC_READ;
    op.sec_rw.oid = {
        .inode = inode,
        .stripe = stripe,
    };
    op.sec_rw.version = version;
    op.sec_rw.offset = offset;
    op.sec_rw.len = len;
    void *data = memalign(MEM_ALIGNMENT, op.sec_rw.len);
    write_blocking(connect_fd, op.buf, OSD_PACKET_SIZE);
    int r = read_blocking(connect_fd, reply.buf, OSD_PACKET_SIZE);
    if (!check_reply(r, op, reply, op.sec_rw.len))
    {
        free(data);
        return 0;
    }
    r = read_blocking(connect_fd, data, len);
    if (r != len)
    {
        free(data);
        perror("read data");
        return 0;
    }
    free(data);
    printf("Read %lx:%lx v%lu = v%lu\n", inode, stripe, version, reply.sec_rw.version);
    op.hdr.opcode = OSD_OP_SEC_LIST;
    op.sec_list.list_pg = 1;
    op.sec_list.pg_count = 1;
    op.sec_list.pg_stripe_size = 4*1024*1024;
    write_blocking(connect_fd, op.buf, OSD_PACKET_SIZE);
    r = read_blocking(connect_fd, reply.buf, OSD_PACKET_SIZE);
    if (reply.hdr.retval < 0 || !check_reply(r, op, reply, reply.hdr.retval))
    {
        return 0;
    }
    data = memalign(MEM_ALIGNMENT, sizeof(obj_ver_id)*reply.hdr.retval);
    r = read_blocking(connect_fd, data, sizeof(obj_ver_id)*reply.hdr.retval);
    if (r != sizeof(obj_ver_id)*reply.hdr.retval)
    {
        free(data);
        perror("read data");
        return 0;
    }
    obj_ver_id *ov = (obj_ver_id*)data;
    for (int i = 0; i < reply.hdr.retval; i++)
    {
        if (ov[i].oid.inode == inode && (ov[i].oid.stripe & ~(4096-1)) == (stripe & ~(4096-1)))
        {
            printf("list: %lx:%lx v%lu stable=%d\n", ov[i].oid.inode, ov[i].oid.stripe, ov[i].version, i < reply.sec_list.stable_count ? 1 : 0);
        }
    }
    return 0;
}

uint64_t test_write(int connect_fd, uint64_t inode, uint64_t stripe, uint64_t version, uint64_t pattern)
{
    osd_any_op_t op;
    osd_any_reply_t reply;
    op.hdr.magic = SECONDARY_OSD_OP_MAGIC;
    op.hdr.id = 1;
    op.hdr.opcode = OSD_OP_SEC_WRITE;
    op.sec_rw.oid = {
        .inode = inode,
        .stripe = stripe,
    };
    op.sec_rw.version = version;
    op.sec_rw.offset = 0;
    op.sec_rw.len = 128*1024;
    void *data = memalign(MEM_ALIGNMENT, op.sec_rw.len);
    for (int i = 0; i < (op.sec_rw.len)/sizeof(uint64_t); i++)
        ((uint64_t*)data)[i] = pattern;
    write_blocking(connect_fd, op.buf, OSD_PACKET_SIZE);
    write_blocking(connect_fd, data, op.sec_rw.len);
    int r = read_blocking(connect_fd, reply.buf, OSD_PACKET_SIZE);
    if (!check_reply(r, op, reply, op.sec_rw.len))
    {
        free(data);
        return 0;
    }
    version = reply.sec_rw.version;
    op.hdr.opcode = OSD_OP_TEST_SYNC_STAB_ALL;
    op.hdr.id = 2;
    write_blocking(connect_fd, op.buf, OSD_PACKET_SIZE);
    r = read_blocking(connect_fd, reply.buf, OSD_PACKET_SIZE);
    if (!check_reply(r, op, reply, 0))
    {
        free(data);
        return 0;
    }
    free(data);
    return version;
}

void* test_primary_read(int connect_fd, uint64_t inode, uint64_t offset, uint64_t len)
{
    osd_any_op_t op;
    osd_any_reply_t reply;
    op.hdr.magic = SECONDARY_OSD_OP_MAGIC;
    op.hdr.id = 1;
    op.hdr.opcode = OSD_OP_READ;
    op.rw.inode = inode;
    op.rw.offset = offset;
    op.rw.len = len;
    void *data = memalign(MEM_ALIGNMENT, len);
    write_blocking(connect_fd, op.buf, OSD_PACKET_SIZE);
    int r = read_blocking(connect_fd, reply.buf, OSD_PACKET_SIZE);
    if (!check_reply(r, op, reply, len))
    {
        free(data);
        return NULL;
    }
    r = read_blocking(connect_fd, data, len);
    if (r != len)
    {
        free(data);
        perror("read data");
        return NULL;
    }
    return data;
}

void test_primary_write(int connect_fd, uint64_t inode, uint64_t offset, uint64_t len, uint64_t pattern)
{
    osd_any_op_t op;
    osd_any_reply_t reply;
    op.hdr.magic = SECONDARY_OSD_OP_MAGIC;
    op.hdr.id = 1;
    op.hdr.opcode = OSD_OP_WRITE;
    op.rw.inode = inode;
    op.rw.offset = offset;
    op.rw.len = len;
    void *data = memalign(MEM_ALIGNMENT, len);
    set_pattern(data, len, pattern);
    write_blocking(connect_fd, op.buf, OSD_PACKET_SIZE);
    write_blocking(connect_fd, data, len);
    free(data);
    int r = read_blocking(connect_fd, reply.buf, OSD_PACKET_SIZE);
    assert(check_reply(r, op, reply, len));
}

void test_primary_sync(int connect_fd)
{
    osd_any_op_t op;
    osd_any_reply_t reply;
    op.hdr.magic = SECONDARY_OSD_OP_MAGIC;
    op.hdr.id = 1;
    op.hdr.opcode = OSD_OP_SYNC;
    write_blocking(connect_fd, op.buf, OSD_PACKET_SIZE);
    int r = read_blocking(connect_fd, reply.buf, OSD_PACKET_SIZE);
    assert(check_reply(r, op, reply, 0));
}

void test_sync_stab_all(int connect_fd)
{
    osd_any_op_t op;
    osd_any_reply_t reply;
    op.hdr.magic = SECONDARY_OSD_OP_MAGIC;
    op.hdr.id = 1;
    op.hdr.opcode = OSD_OP_TEST_SYNC_STAB_ALL;
    write_blocking(connect_fd, op.buf, OSD_PACKET_SIZE);
    int r = read_blocking(connect_fd, reply.buf, OSD_PACKET_SIZE);
    assert(check_reply(r, op, reply, 0));
}

void test_list_stab(int connect_fd)
{
    osd_any_op_t op;
    osd_any_reply_t reply;
    op.hdr.magic = SECONDARY_OSD_OP_MAGIC;
    op.hdr.id = 1;
    op.hdr.opcode = OSD_OP_SEC_LIST;
    op.sec_list.pg_count = 0;
    assert(write_blocking(connect_fd, op.buf, OSD_PACKET_SIZE) == OSD_PACKET_SIZE);
    int r = read_blocking(connect_fd, reply.buf, OSD_PACKET_SIZE);
    assert(check_reply(r, op, reply, -1));
    int total_count = reply.hdr.retval;
    int stable_count = reply.sec_list.stable_count;
    obj_ver_id *data = (obj_ver_id*)malloc(total_count * sizeof(obj_ver_id));
    assert(data);
    assert(read_blocking(connect_fd, data, total_count * sizeof(obj_ver_id)) == (total_count * sizeof(obj_ver_id)));
    int last_start = stable_count;
    for (int i = stable_count; i <= total_count; i++)
    {
        // Stabilize in portions of 32 entries
        if (i - last_start >= 32 || i == total_count)
        {
            op.hdr.opcode = OSD_OP_SEC_STABILIZE;
            op.sec_stab.len = sizeof(obj_ver_id) * (i - last_start);
            assert(write_blocking(connect_fd, op.buf, OSD_PACKET_SIZE) == OSD_PACKET_SIZE);
            assert(write_blocking(connect_fd, data + last_start, op.sec_stab.len) == op.sec_stab.len);
            r = read_blocking(connect_fd, reply.buf, OSD_PACKET_SIZE);
            assert(check_reply(r, op, reply, 0));
            last_start = i;
        }
    }
    obj_ver_id *data2 = (obj_ver_id*)malloc(sizeof(obj_ver_id) * 32);
    assert(data2);
    free(data2);
    free(data);
}
