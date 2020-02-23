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

int connect_osd(const char *osd_address, int osd_port);

uint64_t test_write(int connect_fd, uint64_t inode, uint64_t stripe, uint64_t version, uint64_t pattern);

void* test_primary_read(int connect_fd, uint64_t inode, uint64_t offset, uint64_t len);

bool check_pattern(void *buf, uint64_t offset, uint64_t len, uint64_t pattern);

#define PATTERN0 0x8c4641acc762840e
#define PATTERN1 0x70a549add9a2280a
#define PATTERN2 (PATTERN0 ^ PATTERN1)

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
    test_write(connect_fd, 2, 2, 1, PATTERN2);
    close(connect_fd);
    return 0;
}

int main(int narg, char *args[])
{
    int connect_fd;
    void *data;
    // Cluster read
    connect_fd = connect_osd("127.0.0.1", 11203);
    data = test_primary_read(connect_fd, 2, 0, 128*1024);
    if (data && check_pattern(data, 0, 128*1024, PATTERN0))
        printf("inode=2 0-128K OK\n");
    if (data)
        free(data);
    data = test_primary_read(connect_fd, 2, 0, 256*1024);
    if (data && check_pattern(data, 0, 128*1024, PATTERN0) &&
        check_pattern(data, 128*1024, 128*1024, PATTERN1))
        printf("inode=2 0-256K OK\n");
    if (data)
        free(data);
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
    if (reply.hdr.retval != expected)
    {
        printf("operation failed, retval=%ld\n", reply.hdr.retval);
        return false;
    }
    return true;
}

uint64_t test_write(int connect_fd, uint64_t inode, uint64_t stripe, uint64_t version, uint64_t pattern)
{
    osd_any_op_t op;
    osd_any_reply_t reply;
    op.hdr.magic = SECONDARY_OSD_OP_MAGIC;
    op.hdr.id = 1;
    op.hdr.opcode = OSD_OP_SECONDARY_WRITE;
    op.sec_rw.oid = {
        .inode = inode,
        .stripe = stripe,
    };
    op.sec_rw.version = version;
    op.sec_rw.offset = 0;
    op.sec_rw.len = 128*1024;
    void *data = memalign(512, op.sec_rw.len);
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
    void *data = memalign(512, len);
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

bool check_pattern(void *buf, uint64_t offset, uint64_t len, uint64_t pattern)
{
    for (int i = 0; i < len/sizeof(uint64_t); i++)
    {
        if (((uint64_t*)(buf+offset))[i] != pattern)
        {
            printf("(result + %lu bytes = %lx) != %lx\n", i*sizeof(uint64_t)+offset, ((uint64_t*)buf+offset)[i], pattern);
            return false;
        }
    }
    return true;
}
