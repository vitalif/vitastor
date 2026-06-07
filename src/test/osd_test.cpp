// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "osd.h"
#include "osd_primary.h"
#include "osd_test_fixture.h"

// Verify that an OSD configured with an etcd address issues a range read for
// /<prefix>/config/global and /<prefix>/config/pools right after construction.
//
// The etcd mock is paused before constructing the OSD, so the txn stays in
// the mock's queue instead of executing its callback synchronously inside
// the constructor — that gives us a chance to inspect what the OSD asked
// for without having to drive the rest of the startup sequence.
void test_load_global_config()
{
    osd_test_fixture_t f;
    f.st_cli->pause();
    f.start(json11::Json::object {
        { "osd_num", 1 },
        { "etcd_address", "127.0.0.1:2379" },
        { "etcd_prefix", "/vitastor" },
        { "run_primary", false },
    });

    assert(f.st_cli->queue.size() == 1);
    assert(f.st_cli->queue[0].api == "/kv/txn");
    auto & ops = f.st_cli->queue[0].payload["success"].array_items();
    assert(ops.size() == 2);
    assert(base64_decode(ops[0]["request_range"]["key"].string_value()) == "/vitastor/config/global");
    assert(base64_decode(ops[1]["request_range"]["key"].string_value()) == "/vitastor/config/pools");

    printf("test_load_global_config passed\n");
}

// Build a self-issued OSD_OP_WRITE op for the given inode/offset/length,
// filled with the given byte. client_id=0 (SELF_CLIENT) so finish_op
// delivers the reply through the callback we set rather than over the wire.
static osd_op_t *make_write_op(inode_t inode, uint64_t offset, uint64_t len, uint8_t fill)
{
    auto *op = new osd_op_t();
    op->op_type = OSD_OP_IN;
    op->client_id = 0;
    op->req.rw.header.magic = SECONDARY_OSD_OP_MAGIC;
    op->req.rw.header.id = 1;
    op->req.rw.header.opcode = OSD_OP_WRITE;
    op->req.rw.inode = inode;
    op->req.rw.offset = offset;
    op->req.rw.len = len;
    op->buf = malloc(len);
    memset(op->buf, fill, len);
    return op;
}

// Drive a single 4 KiB replicated write through the primary OSD state machine
// and verify it produces (1) a local blockstore write and (2) a peer subop,
// then completes the client op once both finish.
//
// Layout: pool 1, replicated x2, primary = OSD 1 (us), secondary = OSD 2.
void test_replicated_write()
{
    osd_test_fixture_t f;
    f.configure_replicated_pool(/*pool_id*/ 1, /*pg_size*/ 2, /*pg_minsize*/ 1, /*pg_count*/ 1,
        { { 1, 2 } });
    f.start(json11::Json::object {
        { "osd_num", 1 },
        { "etcd_address", "127.0.0.1:2379" },
        { "immediate_commit", "all" },
        { "block_size", 131072 },
        { "bitmap_granularity", 4096 },
    });
    // Peer 2 wasn't online during initial peering -> PG is INCOMPLETE.
    // Connecting it re-triggers peering; completing the LIST subops with
    // "no objects" lets the PG transition to PG_ACTIVE.
    f.connect_peer(2);
    f.complete_peering_empty();
    assert(f.pg(1, 1).state & PG_ACTIVE);

    auto *write_op = make_write_op(INODE_WITH_POOL(1, 1), 0, 4096, 0xab);
    int final_retval = -1;
    write_op->callback = [&final_retval](osd_op_t *op) {
        final_retval = op->reply.hdr.retval;
    };
    f.exec(write_op);

    // Stage 1: the primary always pumps one subop through SUBMIT_RMW_READ
    // even for a "fresh" full-block replicated write — for our setup that
    // subop is a zero-length local read that just resolves the object's
    // current version.
    assert(f.bs->queued.size() == 1);
    auto *zero_read = f.bs->take();
    assert(zero_read->opcode == BS_OP_READ);
    assert(zero_read->len == 0);
    zero_read->version = 0; // object doesn't exist yet -> current version 0
    zero_read->retval = 0;
    zero_read->callback(zero_read);

    // Stage 2: with fact_ver=0 and target_ver=1 the primary issues the
    // actual writes — one local (to bs) and one remote (to OSD 2).
    auto *peer = f.peer(2);
    assert(f.bs->queued.size() == 1);
    assert(peer->sent_ops.size() == 1);

    auto *local_write = f.bs->take();
    assert(local_write->opcode == BS_OP_WRITE_STABLE);
    assert(local_write->len == 4096);
    assert(local_write->oid.inode == INODE_WITH_POOL(1, 1));
    assert(local_write->oid.stripe == 0);
    assert(local_write->version == 1);

    auto sent_it = peer->sent_ops.begin();
    osd_op_t *remote_write = sent_it->second;
    peer->sent_ops.erase(sent_it);
    assert(remote_write->req.hdr.opcode == OSD_OP_SEC_WRITE_STABLE);
    assert(remote_write->osd_num == 2);
    assert(remote_write->req.sec_rw.oid.inode == INODE_WITH_POOL(1, 1));
    assert(remote_write->req.sec_rw.len == 4096);
    assert(remote_write->req.sec_rw.version == 1);

    // Local write completes first; the primary is still waiting for the peer.
    local_write->retval = local_write->len;
    local_write->callback(local_write);
    assert(final_retval == -1);

    // Peer reply triggers handle_primary_subop, which sees both subops done
    // and lets continue_primary_write reach finish_op.
    remote_write->reply.hdr.retval = remote_write->req.sec_rw.len;
    remote_write->reply.sec_rw.version = 1;
    remote_write->callback(remote_write);

    assert(final_retval == 4096);
    assert(f.pg(1, 1).inflight == 0);
    assert(f.pg(1, 1).write_queue.empty());

    delete write_op;
    printf("test_replicated_write passed\n");
}

int main(int narg, char *args[])
{
    test_load_global_config();
    test_replicated_write();
    return 0;
}
