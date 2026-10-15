// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

// Stopping an OSD peer must cancel its operations at once, also while a send to the
// peer is stuck on a full socket, as with a powered-off host.
// Uses the real messenger over io_uring. Run: test_msgr_stop

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>

#include <stdexcept>
#include <vector>

#include "ringloop.h"
#include "epoll_manager.h"
#include "messenger.h"
#include "osd_ops.h"

#define SKIP_RETURN_CODE 77

// A connected loopback TCP pair with small buffers. The peer end is never read,
// so a large enough send to `our_fd` stays pending, as to a host that went away.
static void make_stuck_pair(int & our_fd, int & peer_fd)
{
    int small = 16384;
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    assert(listen_fd >= 0);
    // Inherited by the accepted socket
    setsockopt(listen_fd, SOL_SOCKET, SO_RCVBUF, &small, sizeof(small));
    sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    int r = bind(listen_fd, (sockaddr*)&addr, sizeof(addr));
    assert(r == 0);
    r = listen(listen_fd, 1);
    assert(r == 0);
    socklen_t addr_len = sizeof(addr);
    r = getsockname(listen_fd, (sockaddr*)&addr, &addr_len);
    assert(r == 0);
    our_fd = socket(AF_INET, SOCK_STREAM, 0);
    assert(our_fd >= 0);
    setsockopt(our_fd, SOL_SOCKET, SO_SNDBUF, &small, sizeof(small));
    r = connect(our_fd, (sockaddr*)&addr, sizeof(addr));
    assert(r == 0);
    peer_fd = accept(listen_fd, NULL, NULL);
    assert(peer_fd >= 0);
    close(listen_fd);
    fcntl(our_fd, F_SETFL, fcntl(our_fd, F_GETFL, 0) | O_NONBLOCK);
    // As handle_connect_epoll() sets it for OSD peer sockets
    struct linger sl = { .l_onoff = 1, .l_linger = 0 };
    setsockopt(our_fd, SOL_SOCKET, SO_LINGER, &sl, sizeof(sl));
}

static int count_unfinished(const std::vector<int> & retvals)
{
    int n = 0;
    for (int r: retvals)
        n += (r == 1);
    return n;
}

static void test_stop_client_with_stuck_send(ring_loop_t *ringloop, bool zerocopy)
{
    printf("test_stop_client_with_stuck_send(%s)\n", zerocopy ? "zerocopy" : "copy");
    epoll_manager_t *epmgr = new epoll_manager_t(ringloop);
    osd_messenger_t *msgr = new osd_messenger_t();
    msgr->osd_num = 1;
    msgr->tfd = epmgr->tfd;
    msgr->ringloop = ringloop;
    msgr->repeer_pgs = [](osd_num_t) {};
    msgr->parse_config(json11::Json::object { { "min_zerocopy_send_size", zerocopy ? 0 : -1 } }, true);
    // As init() does, without its keepalive timer
    msgr->has_sendmsg_zc = ringloop->has_sendmsg_zc();
    if (zerocopy && !msgr->has_sendmsg_zc)
    {
        printf("  no sendmsg_zc support, skipped\n");
        delete msgr;
        delete epmgr;
        return;
    }
    ring_consumer_t looper;
    looper.loop = [msgr, ringloop]()
    {
        msgr->read_requests();
        msgr->send_replies();
        ringloop->submit();
    };
    ringloop->register_consumer(&looper);

    // A connected OSD peer, as try_connect_peer_tcp() and handle_connect_epoll() make it
    int our_fd, peer_fd;
    make_stuck_pair(our_fd, peer_fd);
    osd_client_t *cl = new osd_client_t();
    const uint64_t client_id = msgr->next_client_id++;
    cl->client_id = client_id;
    cl->peer_fd = our_fd;
    cl->peer_state = PEER_CONNECTED;
    cl->osd_num = 2;
    cl->in_buf = (uint8_t*)malloc_or_die(65536);
    msgr->clients[client_id] = cl;
    msgr->clients_by_fd[our_fd] = cl;
    msgr->osd_peers[2] = cl;
    epmgr->set_fd_handler(our_fd, false, [](int, int) {});

    // Subops with 1 MiB of data each, far more than the socket buffers hold
    const int n_ops = 16, op_len = 1024*1024;
    std::vector<osd_op_t*> ops;
    std::vector<int> retvals(n_ops, 1);
    for (int i = 0; i < n_ops; i++)
    {
        osd_op_t *op = new osd_op_t();
        op->op_type = OSD_OP_OUT;
        op->client_id = client_id;
        op->req.hdr.magic = SECONDARY_OSD_OP_MAGIC;
        op->req.hdr.opcode = OSD_OP_SEC_WRITE_STABLE;
        op->req.sec_rw.oid = { .inode = 1, .stripe = (uint64_t)i << 17 };
        op->req.sec_rw.version = 1;
        op->req.sec_rw.len = op_len;
        op->buf = malloc_or_die(op_len);
        memset(op->buf, 0xab, op_len);
        op->iov.push_back(op->buf, op_len);
        op->callback = [&retvals, i](osd_op_t *op) { retvals[i] = op->reply.hdr.retval; };
        ops.push_back(op);
        msgr->outbox_push(op);
    }
    // Run until the socket buffers are full and a send stays pending
    for (int i = 0; i < 200; i++)
        ringloop->loop();
    assert(cl->write_msg.msg_iovlen > 0);
    assert(count_unfinished(retvals) == n_ops);

    // What the keepalive timer does when the peer stops answering pings
    msgr->stop_client(client_id);
    for (int i = 0; i < 2000 && count_unfinished(retvals) > 0; i++)
    {
        ringloop->loop();
        usleep(1000);
    }
    int unfinished = count_unfinished(retvals);
    printf("  %d of %d ops still unfinished 2 s after stop_client()\n", unfinished, n_ops);
    assert(unfinished == 0);
    for (int r: retvals)
        assert(r == -EPIPE);

    close(peer_fd);
    for (int i = 0; i < 100; i++)
    {
        ringloop->loop();
        usleep(1000);
    }
    for (auto op: ops)
        delete op;
    ringloop->unregister_consumer(&looper);
    delete msgr;
    delete epmgr;
    printf("test_stop_client_with_stuck_send(%s) passed\n", zerocopy ? "zerocopy" : "copy");
}

int main(int narg, char *args[])
{
    ring_loop_t *ringloop;
    try
    {
        ringloop = new ring_loop_t(RINGLOOP_DEFAULT_SIZE);
    }
    catch (std::runtime_error & e)
    {
        printf("test_msgr_stop: io_uring is not available (%s), skipped\n", e.what());
        return SKIP_RETURN_CODE;
    }
    test_stop_client_with_stuck_send(ringloop, false);
    test_stop_client_with_stuck_send(ringloop, true);
    delete ringloop;
    return 0;
}
