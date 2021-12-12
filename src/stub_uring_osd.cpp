// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

/**
 * Stub "OSD" implemented on top of osd_messenger to test & compare
 * network performance with sync read/write and io_uring
 */

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

#include <stdexcept>

#include "ringloop.h"
#include "epoll_manager.h"
#include "messenger.h"

int bind_stub(const char *bind_address, int bind_port);

void stub_exec_op(osd_messenger_t *msgr, osd_op_t *op);

int main(int narg, char *args[])
{
    ring_consumer_t looper;
    ring_loop_t *ringloop = new ring_loop_t(512);
    epoll_manager_t *epmgr = new epoll_manager_t(ringloop);
    osd_messenger_t *msgr = new osd_messenger_t();
    msgr->osd_num = 1351;
    msgr->tfd = epmgr->tfd;
    msgr->ringloop = ringloop;
    msgr->repeer_pgs = [](osd_num_t) {};
    msgr->exec_op = [msgr](osd_op_t *op) { stub_exec_op(msgr, op); };
    json11::Json config = json11::Json::object { { "log_level", 1 } };
    msgr->parse_config(config);
    // Accept new connections
    int listen_fd = bind_stub("0.0.0.0", 11203);
    epmgr->set_fd_handler(listen_fd, false, [listen_fd, msgr](int fd, int events)
    {
        msgr->accept_connections(listen_fd);
    });
    looper.loop = [msgr, ringloop]()
    {
        msgr->read_requests();
        msgr->send_replies();
        ringloop->submit();
    };
    ringloop->register_consumer(&looper);
    printf("stub_uring_osd: waiting for clients\n");
    while (true)
    {
        ringloop->loop();
        ringloop->wait();
    }
    delete msgr;
    delete epmgr;
    delete ringloop;
    return 0;
}

int bind_stub(const char *bind_address, int bind_port)
{
    int listen_backlog = 128;

    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0)
    {
        throw std::runtime_error(std::string("socket: ") + strerror(errno));
    }
    int enable = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable));

    sockaddr_in addr;
    int r;
    if ((r = inet_pton(AF_INET, bind_address, &addr.sin_addr)) != 1)
    {
        close(listen_fd);
        throw std::runtime_error("bind address "+std::string(bind_address)+(r == 0 ? " is not valid" : ": no ipv4 support"));
    }
    addr.sin_family = AF_INET;
    addr.sin_port = htons(bind_port);

    if (bind(listen_fd, (sockaddr*)&addr, sizeof(addr)) < 0)
    {
        close(listen_fd);
        throw std::runtime_error(std::string("bind: ") + strerror(errno));
    }

    if (listen(listen_fd, listen_backlog) < 0)
    {
        close(listen_fd);
        throw std::runtime_error(std::string("listen: ") + strerror(errno));
    }

    fcntl(listen_fd, F_SETFL, fcntl(listen_fd, F_GETFL, 0) | O_NONBLOCK);

    return listen_fd;
}

void stub_exec_op(osd_messenger_t *msgr, osd_op_t *op)
{
    op->reply.hdr.magic = SECONDARY_OSD_REPLY_MAGIC;
    op->reply.hdr.id = op->req.hdr.id;
    op->reply.hdr.opcode = op->req.hdr.opcode;
    if (op->req.hdr.opcode == OSD_OP_SEC_READ)
    {
        op->reply.hdr.retval = op->req.sec_rw.len;
        op->buf = malloc(op->req.sec_rw.len);
        op->iov.push_back(op->buf, op->req.sec_rw.len);
    }
    else if (op->req.hdr.opcode == OSD_OP_SEC_WRITE || op->req.hdr.opcode == OSD_OP_SEC_WRITE_STABLE)
    {
        op->reply.hdr.retval = op->req.sec_rw.len;
    }
    else if (op->req.hdr.opcode == OSD_OP_TEST_SYNC_STAB_ALL)
    {
        op->reply.hdr.retval = 0;
    }
    else
    {
        printf("client %d: unsupported stub opcode: %lu\n", op->peer_fd, op->req.hdr.opcode);
        op->reply.hdr.retval = -EINVAL;
    }
    msgr->outbox_push(op);
}
