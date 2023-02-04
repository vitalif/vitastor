// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include "cluster_client.h"

void configure_single_pg_pool(cluster_client_t *cli)
{
    cli->st_cli.parse_state((etcd_kv_t){
        .key = "/config/pools",
        .value = json11::Json::object {
            { "1", json11::Json::object {
                { "name", "hddpool" },
                { "scheme", "replicated" },
                { "pg_size", 2 },
                { "pg_minsize", 1 },
                { "pg_count", 1 },
                { "failure_domain", "osd" },
            } }
        },
    });
    cli->st_cli.parse_state((etcd_kv_t){
        .key = "/config/pgs",
        .value = json11::Json::object {
            { "items", json11::Json::object {
                { "1", json11::Json::object {
                    { "1", json11::Json::object {
                        { "osd_set", json11::Json::array { 1, 2 } },
                        { "primary", 1 },
                    } }
                } }
            } }
        },
    });
    cli->st_cli.parse_state((etcd_kv_t){
        .key = "/pg/state/1/1",
        .value = json11::Json::object {
            { "peers", json11::Json::array { 1, 2 } },
            { "primary", 1 },
            { "state", json11::Json::array { "active" } },
        },
    });
    cli->st_cli.on_load_pgs_hook(true);
    std::map<std::string, etcd_kv_t> changes;
    cli->st_cli.on_change_hook(changes);
}

int *test_write(cluster_client_t *cli, uint64_t offset, uint64_t len, uint8_t c, std::function<void()> cb = NULL)
{
    printf("Post write %lx+%lx\n", offset, len);
    int *r = new int;
    *r = -1;
    cluster_op_t *op = new cluster_op_t();
    op->opcode = OSD_OP_WRITE;
    op->inode = 0x1000000000001;
    op->offset = offset;
    op->len = len;
    op->iov.push_back(malloc_or_die(len), len);
    memset(op->iov.buf[0].iov_base, c, len);
    op->callback = [r, cb](cluster_op_t *op)
    {
        if (*r == -1)
            printf("Error: Not allowed to complete yet\n");
        assert(*r != -1);
        *r = op->retval == op->len ? 1 : 0;
        free(op->iov.buf[0].iov_base);
        printf("Done write %lx+%lx r=%d\n", op->offset, op->len, op->retval);
        delete op;
        if (cb != NULL)
            cb();
    };
    cli->execute(op);
    return r;
}

int *test_sync(cluster_client_t *cli)
{
    printf("Post sync\n");
    int *r = new int;
    *r = -1;
    cluster_op_t *op = new cluster_op_t();
    op->opcode = OSD_OP_SYNC;
    op->callback = [r](cluster_op_t *op)
    {
        if (*r == -1)
            printf("Error: Not allowed to complete yet\n");
        assert(*r != -1);
        *r = op->retval == 0 ? 1 : 0;
        printf("Done sync r=%d\n", op->retval);
        delete op;
    };
    cli->execute(op);
    return r;
}

void can_complete(int *r)
{
    // Allow the operation to proceed so the test verifies
    // that it doesn't complete earlier than expected
    *r = -2;
}

void check_completed(int *r)
{
    assert(*r == 1);
    delete r;
}

void pretend_connected(cluster_client_t *cli, osd_num_t osd_num)
{
    printf("OSD %lu connected\n", osd_num);
    int peer_fd = cli->msgr.clients.size() ? std::prev(cli->msgr.clients.end())->first+1 : 10;
    cli->msgr.osd_peer_fds[osd_num] = peer_fd;
    cli->msgr.clients[peer_fd] = new osd_client_t();
    cli->msgr.clients[peer_fd]->osd_num = osd_num;
    cli->msgr.clients[peer_fd]->peer_state = PEER_CONNECTED;
    cli->msgr.wanted_peers.erase(osd_num);
    cli->msgr.repeer_pgs(osd_num);
}

void pretend_disconnected(cluster_client_t *cli, osd_num_t osd_num)
{
    printf("OSD %lu disconnected\n", osd_num);
    cli->msgr.stop_client(cli->msgr.osd_peer_fds.at(osd_num));
}

void check_disconnected(cluster_client_t *cli, osd_num_t osd_num)
{
    if (cli->msgr.osd_peer_fds.find(osd_num) != cli->msgr.osd_peer_fds.end())
    {
        printf("OSD %lu not disconnected as it ought to be\n", osd_num);
        assert(0);
    }
}

void check_op_count(cluster_client_t *cli, osd_num_t osd_num, int ops)
{
    int peer_fd = cli->msgr.osd_peer_fds.at(osd_num);
    int real_ops = cli->msgr.clients[peer_fd]->sent_ops.size();
    if (real_ops != ops)
    {
        printf("error: %d ops expected, but %d queued\n", ops, real_ops);
        assert(0);
    }
}

osd_op_t *find_op(cluster_client_t *cli, osd_num_t osd_num, uint64_t opcode, uint64_t offset, uint64_t len)
{
    int peer_fd = cli->msgr.osd_peer_fds.at(osd_num);
    auto op_it = cli->msgr.clients[peer_fd]->sent_ops.begin();
    while (op_it != cli->msgr.clients[peer_fd]->sent_ops.end())
    {
        auto op = op_it->second;
        if (op->req.hdr.opcode == opcode && (opcode == OSD_OP_SYNC ||
            op->req.rw.inode == 0x1000000000001 && op->req.rw.offset == offset && op->req.rw.len == len))
        {
            return op;
        }
        op_it++;
    }
    return NULL;
}

void pretend_op_completed(cluster_client_t *cli, osd_op_t *op, int64_t retval)
{
    assert(op);
    printf("Pretend completed %s %lx+%x\n", op->req.hdr.opcode == OSD_OP_SYNC
        ? "sync" : (op->req.hdr.opcode == OSD_OP_WRITE ? "write" : "read"), op->req.rw.offset, op->req.rw.len);
    uint64_t op_id = op->req.hdr.id;
    int peer_fd = op->peer_fd;
    cli->msgr.clients[peer_fd]->sent_ops.erase(op_id);
    op->reply.hdr.magic = SECONDARY_OSD_REPLY_MAGIC;
    op->reply.hdr.id = op->req.hdr.id;
    op->reply.hdr.opcode = op->req.hdr.opcode;
    op->reply.hdr.retval = retval < 0 ? retval : (op->req.hdr.opcode == OSD_OP_SYNC ? 0 : op->req.rw.len);
    // Copy lambda to be unaffected by `delete op`
    std::function<void(osd_op_t*)>(op->callback)(op);
}

void test1()
{
    json11::Json config;
    timerfd_manager_t *tfd = new timerfd_manager_t([](int fd, bool wr, std::function<void(int, int)> callback){});
    cluster_client_t *cli = new cluster_client_t(NULL, tfd, config);

    int *r1 = test_write(cli, 0, 4096, 0x55);
    configure_single_pg_pool(cli);
    pretend_connected(cli, 1);
    can_complete(r1);
    check_op_count(cli, 1, 1);
    pretend_op_completed(cli, find_op(cli, 1, OSD_OP_WRITE, 0, 4096), 0);
    check_completed(r1);
    pretend_disconnected(cli, 1);
    int *r2 = test_sync(cli);
    pretend_connected(cli, 1);
    check_op_count(cli, 1, 1);
    pretend_op_completed(cli, find_op(cli, 1, OSD_OP_WRITE, 0, 4096), 0);
    check_op_count(cli, 1, 1);
    can_complete(r2);
    pretend_op_completed(cli, find_op(cli, 1, OSD_OP_SYNC, 0, 0), 0);
    check_completed(r2);
    // Check that the client doesn't repeat operations once more
    pretend_disconnected(cli, 1);
    pretend_connected(cli, 1);
    check_op_count(cli, 1, 0);

    // Case:
    // Write(1) -> Complete Write(1) -> Overwrite(2) -> Complete Write(2)
    // -> Overwrite(3) -> Drop OSD connection -> Reestablish OSD connection
    // -> Complete All Posted Writes -> Sync -> Complete Sync
    // The resulting state of the block must be (3) over (2) over (1).
    // I.e. the part overwritten by (3) must remain as in (3) and so on.

    // More interesting case:
    // Same, but both Write(2) and Write(3) must consist of two parts:
    // one from an OSD 2 that drops connection and other from OSD 1 that doesn't.
    // The idea is that if the whole Write(2) is repeated when OSD 2 drops connection
    // then it may also overwrite a part in OSD 1 which shouldn't be overwritten.

    // Another interesting case:
    // A new operation added during replay (would also break with the previous implementation)

    r1 = test_write(cli, 0, 0x10000, 0x56);
    can_complete(r1);
    check_op_count(cli, 1, 1);
    pretend_op_completed(cli, find_op(cli, 1, OSD_OP_WRITE, 0, 0x10000), 0);
    check_completed(r1);

    r1 = test_write(cli, 0xE000, 0x4000, 0x57);
    can_complete(r1);
    check_op_count(cli, 1, 1);
    pretend_op_completed(cli, find_op(cli, 1, OSD_OP_WRITE, 0xE000, 0x4000), 0);
    check_completed(r1);

    r1 = test_write(cli, 0x10000, 0x4000, 0x58);

    pretend_disconnected(cli, 1);
    pretend_connected(cli, 1);
    cli->continue_ops(true);

    // Check replay
    {
        uint64_t replay_start = UINT64_MAX;
        uint64_t replay_end = 0;
        std::vector<osd_op_t*> replay_ops;
        auto osd_cl = cli->msgr.clients.at(cli->msgr.osd_peer_fds.at(1));
        for (auto & op_p: osd_cl->sent_ops)
        {
            auto op = op_p.second;
            assert(op->req.hdr.opcode == OSD_OP_WRITE);
            uint64_t offset = op->req.rw.offset;
            if (op->req.rw.offset < replay_start)
                replay_start = op->req.rw.offset;
            if (op->req.rw.offset+op->req.rw.len > replay_end)
                replay_end = op->req.rw.offset+op->req.rw.len;
            for (int buf_idx = 0; buf_idx < op->iov.count; buf_idx++)
            {
                for (int i = 0; i < op->iov.buf[buf_idx].iov_len; i++, offset++)
                {
                    uint8_t c = offset < 0xE000 ? 0x56 : (offset < 0x10000 ? 0x57 : 0x58);
                    if (((uint8_t*)op->iov.buf[buf_idx].iov_base)[i] != c)
                    {
                        printf("Write replay: mismatch at %lu\n", offset-op->req.rw.offset);
                        goto fail;
                    }
                }
            }
        fail:
            assert(offset == op->req.rw.offset+op->req.rw.len);
            replay_ops.push_back(op);
        }
        if (replay_start != 0 || replay_end != 0x14000)
        {
            printf("Write replay: range mismatch: %lx-%lx\n", replay_start, replay_end);
            assert(0);
        }
        for (auto op: replay_ops)
        {
            pretend_op_completed(cli, op, 0);
        }
    }
    // Check that the following write finally proceeds
    check_op_count(cli, 1, 1);
    can_complete(r1);
    pretend_op_completed(cli, find_op(cli, 1, OSD_OP_WRITE, 0x10000, 0x4000), 0);
    check_completed(r1);
    check_op_count(cli, 1, 0);

    // Check sync
    r2 = test_sync(cli);
    can_complete(r2);
    pretend_op_completed(cli, find_op(cli, 1, OSD_OP_SYNC, 0, 0), 0);
    check_completed(r2);

    // Check disconnect during write
    r1 = test_write(cli, 0, 4096, 0x59);
    check_op_count(cli, 1, 1);
    pretend_op_completed(cli, find_op(cli, 1, OSD_OP_WRITE, 0, 0x1000), -EPIPE);
    check_disconnected(cli, 1);
    pretend_connected(cli, 1);
    check_op_count(cli, 1, 1);
    pretend_op_completed(cli, find_op(cli, 1, OSD_OP_WRITE, 0, 0x1000), 0);
    check_op_count(cli, 1, 1);
    can_complete(r1);
    pretend_op_completed(cli, find_op(cli, 1, OSD_OP_WRITE, 0, 0x1000), 0);
    check_completed(r1);

    // Check disconnect inside operation callback (reenterability)
    // Probably doesn't happen too often, but possible in theory
    r1 = test_write(cli, 0, 0x1000, 0x60, [cli]()
    {
        pretend_disconnected(cli, 1);
    });
    r2 = test_write(cli, 0x1000, 0x1000, 0x61);
    check_op_count(cli, 1, 2);
    can_complete(r1);
    pretend_op_completed(cli, find_op(cli, 1, OSD_OP_WRITE, 0, 0x1000), 0);
    check_completed(r1);
    check_disconnected(cli, 1);
    pretend_connected(cli, 1);
    cli->continue_ops(true);
    check_op_count(cli, 1, 2);
    pretend_op_completed(cli, find_op(cli, 1, OSD_OP_WRITE, 0, 0x1000), 0);
    pretend_op_completed(cli, find_op(cli, 1, OSD_OP_WRITE, 0x1000, 0x1000), 0);
    check_op_count(cli, 1, 1);
    can_complete(r2);
    pretend_op_completed(cli, find_op(cli, 1, OSD_OP_WRITE, 0x1000, 0x1000), 0);
    check_completed(r2);

    // Free client
    delete cli;
    delete tfd;
    printf("[ok] write replay test\n");
}

void test2()
{
    std::map<object_id, cluster_buffer_t> unsynced_writes;
    cluster_op_t *op = new cluster_op_t();
    op->opcode = OSD_OP_WRITE;
    op->inode = 1;
    op->offset = 0;
    op->len = 4096;
    op->iov.push_back(malloc_or_die(4096*1024), 4096);
    // 0-4k = 0x55
    memset(op->iov.buf[0].iov_base, 0x55, op->iov.buf[0].iov_len);
    cluster_client_t::copy_write(op, unsynced_writes);
    // 8k-12k = 0x66
    op->offset = 8192;
    memset(op->iov.buf[0].iov_base, 0x66, op->iov.buf[0].iov_len);
    cluster_client_t::copy_write(op, unsynced_writes);
    // 4k-1M+4k = 0x77
    op->len = op->iov.buf[0].iov_len = 1048576;
    op->offset = 4096;
    memset(op->iov.buf[0].iov_base, 0x77, op->iov.buf[0].iov_len);
    cluster_client_t::copy_write(op, unsynced_writes);
    // check it
    assert(unsynced_writes.size() == 4);
    auto uit = unsynced_writes.begin();
    int i;
    assert(uit->first.inode == 1);
    assert(uit->first.stripe == 0);
    assert(uit->second.len == 4096);
    for (i = 0; i < uit->second.len && ((uint8_t*)uit->second.buf)[i] == 0x55; i++) {}
    assert(i == uit->second.len);
    uit++;
    assert(uit->first.inode == 1);
    assert(uit->first.stripe == 4096);
    assert(uit->second.len == 4096);
    for (i = 0; i < uit->second.len && ((uint8_t*)uit->second.buf)[i] == 0x77; i++) {}
    assert(i == uit->second.len);
    uit++;
    assert(uit->first.inode == 1);
    assert(uit->first.stripe == 8192);
    assert(uit->second.len == 4096);
    for (i = 0; i < uit->second.len && ((uint8_t*)uit->second.buf)[i] == 0x77; i++) {}
    assert(i == uit->second.len);
    uit++;
    assert(uit->first.inode == 1);
    assert(uit->first.stripe == 12*1024);
    assert(uit->second.len == 1016*1024);
    for (i = 0; i < uit->second.len && ((uint8_t*)uit->second.buf)[i] == 0x77; i++) {}
    assert(i == uit->second.len);
    uit++;
    // free memory
    free(op->iov.buf[0].iov_base);
    delete op;
    for (auto p: unsynced_writes)
    {
        free(p.second.buf);
    }
    printf("[ok] copy_write test\n");
}

int main(int narg, char *args[])
{
    test1();
    test2();
    return 0;
}
