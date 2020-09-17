// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.0 or GNU GPL-2.0+ (see README.md for details)

/**
 * Stub benchmarker
 */

#include <sys/types.h>
#include <time.h>
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
#include <signal.h>

#include <stdexcept>

#include "rw_blocking.h"
#include "osd_ops.h"

int connect_stub(const char *server_address, int server_port);

void run_bench(int peer_fd);

static uint64_t read_sum = 0, read_count = 0;
static uint64_t write_sum = 0, write_count = 0;
static uint64_t sync_sum = 0, sync_count = 0;

void handle_sigint(int sig)
{
    printf("4k randread: %lu us avg\n", read_count ? read_sum/read_count : 0);
    printf("4k randwrite: %lu us avg\n", write_count ? write_sum/write_count : 0);
    printf("sync: %lu us avg\n", sync_count ? sync_sum/sync_count : 0);
    exit(0);
}

int main(int narg, char *args[])
{
    if (narg < 2)
    {
        printf("USAGE: %s SERVER_IP [PORT]\n", args[0]);
        return 1;
    }
    int port = 11203;
    if (narg >= 3)
    {
        port = atoi(args[2]);
        if (port <= 0 || port >= 65536)
        {
            printf("Bad port number\n");
            return 1;
        }
    }
    signal(SIGINT, handle_sigint);
    int peer_fd = connect_stub(args[1], port);
    run_bench(peer_fd);
    close(peer_fd);
    return 0;
}

int connect_stub(const char *server_address, int server_port)
{
    struct sockaddr_in addr;
    int r;
    if ((r = inet_pton(AF_INET, server_address, &addr.sin_addr)) != 1)
    {
        fprintf(stderr, "server address: %s%s\n", server_address, r == 0 ? " is not valid" : ": no ipv4 support");
        return -1;
    }
    addr.sin_family = AF_INET;
    addr.sin_port = htons(server_port);
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
        printf("operation failed, retval=%ld (%s)\n", reply.hdr.retval, strerror(-reply.hdr.retval));
        return false;
    }
    return true;
}

void run_bench(int peer_fd)
{
    osd_any_op_t op;
    osd_any_reply_t reply;
    void *buf = NULL;
    int r;
    iovec iov[2];
    timespec tv_begin, tv_end;
    clock_gettime(CLOCK_REALTIME, &tv_begin);
    while (1)
    {
        // read
        op.hdr.magic = SECONDARY_OSD_OP_MAGIC;
        op.hdr.id = 1;
        op.hdr.opcode = OSD_OP_SEC_READ;
        op.sec_rw.oid.inode = 3;
        op.sec_rw.oid.stripe = (rand() << 17) % (1 << 29); // 512 MB
        op.sec_rw.version = 0;
        op.sec_rw.len = 4096;
        op.sec_rw.offset = (rand() * op.sec_rw.len) % (1 << 17);
        r = write_blocking(peer_fd, op.buf, OSD_PACKET_SIZE) == OSD_PACKET_SIZE;
        if (!r)
            break;
        buf = malloc(op.sec_rw.len);
        iov[0] = { reply.buf, OSD_PACKET_SIZE };
        iov[1] = { buf, op.sec_rw.len };
        r = readv_blocking(peer_fd, iov, 2) == (OSD_PACKET_SIZE + op.sec_rw.len);
        free(buf);
        if (!r || !check_reply(OSD_PACKET_SIZE, op, reply, op.sec_rw.len))
            break;
        clock_gettime(CLOCK_REALTIME, &tv_end);
        read_count++;
        read_sum += (
            (tv_end.tv_sec - tv_begin.tv_sec)*1000000 +
            tv_end.tv_nsec/1000 - tv_begin.tv_nsec/1000
        );
        tv_begin = tv_end;
        // write
        op.hdr.magic = SECONDARY_OSD_OP_MAGIC;
        op.hdr.id = 1;
        op.hdr.opcode = OSD_OP_SEC_WRITE;
        op.sec_rw.oid.inode = 3;
        op.sec_rw.oid.stripe = (rand() << 17) % (1 << 29); // 512 MB
        op.sec_rw.version = 0;
        op.sec_rw.len = 4096;
        op.sec_rw.offset = (rand() * op.sec_rw.len) % (1 << 17);
        buf = malloc(op.sec_rw.len);
        memset(buf, rand() % 255, op.sec_rw.len);
        iov[0] = { op.buf, OSD_PACKET_SIZE };
        iov[1] = { buf, op.sec_rw.len };
        r = writev_blocking(peer_fd, iov, 2) == (OSD_PACKET_SIZE + op.sec_rw.len);
        free(buf);
        if (!r)
            break;
        r = read_blocking(peer_fd, reply.buf, OSD_PACKET_SIZE);
        if (!check_reply(r, op, reply, op.sec_rw.len))
            break;
        clock_gettime(CLOCK_REALTIME, &tv_end);
        write_count++;
        write_sum += (
            (tv_end.tv_sec - tv_begin.tv_sec)*1000000 +
            tv_end.tv_nsec/1000 - tv_begin.tv_nsec/1000
        );
        tv_begin = tv_end;
        // sync/stab
        op.hdr.magic = SECONDARY_OSD_OP_MAGIC;
        op.hdr.id = 1;
        op.hdr.opcode = OSD_OP_TEST_SYNC_STAB_ALL;
        r = write_blocking(peer_fd, op.buf, OSD_PACKET_SIZE) == OSD_PACKET_SIZE;
        if (!r)
            break;
        r = read_blocking(peer_fd, reply.buf, OSD_PACKET_SIZE);
        if (!check_reply(r, op, reply, 0))
            break;
        clock_gettime(CLOCK_REALTIME, &tv_end);
        sync_count++;
        sync_sum += (
            (tv_end.tv_sec - tv_begin.tv_sec)*1000000 +
            tv_end.tv_nsec/1000 - tv_begin.tv_nsec/1000
        );
        tv_begin = tv_end;
    }
}
