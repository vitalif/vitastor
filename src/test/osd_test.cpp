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
    printf("test_load_global_config\n");

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
    op->req.rw.version = 0;
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
    printf("test_replicated_write\n");

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

// Regression test for a bug where a second scrub on a still-failing replica
// (already marked LOC_CORRUPTED) would clear the corruption flag.
void test_scrub_corruption_persists()
{
    printf("test_scrub_corruption_persists\n");

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
    f.connect_peer(2);
    f.complete_peering_empty();
    assert(f.pg(1, 1).state & PG_ACTIVE);

    // ---- Write one object so there is something to scrub ----
    inode_t inode = INODE_WITH_POOL(1, 1);
    auto *write_op = make_write_op(inode, 0, 4096, 0xab);
    int final_retval = -1;
    write_op->callback = [&final_retval](osd_op_t *op) {
        final_retval = op->reply.hdr.retval;
    };
    f.exec(write_op);

    // Stage 1: zero-length read for version resolution
    assert(f.bs->queued.size() == 1);
    auto *zero_read = f.bs->take();
    assert(zero_read->opcode == BS_OP_READ);
    assert(zero_read->len == 0);
    zero_read->version = 0;
    zero_read->retval = 0;
    zero_read->callback(zero_read);

    // Stage 2: local write + peer write
    assert(f.bs->queued.size() == 1);
    assert(f.peer(2)->sent_ops.size() == 1);
    auto *local_write = f.bs->take();
    auto *peer_write = f.peer_take(2, OSD_OP_SEC_WRITE_STABLE);

    // Complete both
    local_write->retval = local_write->len;
    local_write->callback(local_write);
    assert(final_retval == -1); // still waiting for peer
    peer_write->reply.hdr.retval = peer_write->req.sec_rw.len;
    peer_write->reply.sec_rw.version = 1;
    peer_write->callback(peer_write);

    assert(final_retval == 4096);
    assert(f.pg(1, 1).inflight == 0);
    assert(f.pg(1, 1).write_queue.empty());
    delete write_op;

    object_id oid = { .inode = inode, .stripe = 0 };

    // ============ SCRUB 1 — peer returns -EIO ============

    auto *scrub1 = new osd_op_t();
    scrub1->op_type = OSD_OP_IN;
    scrub1->client_id = 0;
    scrub1->req.rw.header.magic = SECONDARY_OSD_OP_MAGIC;
    scrub1->req.rw.header.id = 2;
    scrub1->req.rw.header.opcode = OSD_OP_SCRUB;
    scrub1->req.rw.inode = inode;
    scrub1->req.rw.offset = 0;
    scrub1->req.rw.len = 0;

    int scrub1_retval = -1;
    scrub1->callback = [&scrub1_retval](osd_op_t *op) {
        scrub1_retval = op->reply.hdr.retval;
    };
    f.exec(scrub1);

    // submit_scrub_subops submitted local BS_OP_READ + peer OSD_OP_SEC_READ
    assert(f.bs->queued.size() == 1);
    auto *local_r1 = f.bs->take(BS_OP_READ);
    memset(local_r1->buf, 0xab, local_r1->len);
    local_r1->retval = local_r1->len;
    local_r1->version = 1;
    local_r1->callback(local_r1);

    auto *peer_r1 = f.peer_take(2, OSD_OP_SEC_READ);
    peer_r1->reply.hdr.retval = -EIO;
    peer_r1->callback(peer_r1);

    assert(scrub1_retval == 0);

    // Verify: OSD 2 chunk is now LOC_CORRUPTED
    auto check_corrupted = [&]()
    {
        if (!(f.pg(1, 1).state & PG_HAS_CORRUPTED))
            return false;
        auto st_it = f.pg(1, 1).degraded_objects.find(oid);
        if (st_it == f.pg(1, 1).degraded_objects.end())
            return false;
        for (auto & chunk : st_it->second->osd_set)
            if (chunk.osd_num == 2 && (chunk.loc_bad & LOC_CORRUPTED))
                return true;
        return false;
    };
    bool osd2_corrupted = check_corrupted();
    assert(osd2_corrupted);
    printf("test_scrub_corruption_persists: scrub 1 -> LOC_CORRUPTED set\n");
    delete scrub1;

    // ============ SCRUB 2 — peer still returns -EIO ============

    auto *scrub2 = new osd_op_t();
    scrub2->op_type = OSD_OP_IN;
    scrub2->client_id = 0;
    scrub2->req.rw.header.magic = SECONDARY_OSD_OP_MAGIC;
    scrub2->req.rw.header.id = 3;
    scrub2->req.rw.header.opcode = OSD_OP_SCRUB;
    scrub2->req.rw.inode = inode;
    scrub2->req.rw.offset = 0;
    scrub2->req.rw.len = 0;

    int scrub2_retval = -1;
    scrub2->callback = [&scrub2_retval](osd_op_t *op) {
        scrub2_retval = op->reply.hdr.retval;
    };
    f.exec(scrub2);

    assert(f.bs->queued.size() == 1);
    auto *local_r2 = f.bs->take(BS_OP_READ);
    memset(local_r2->buf, 0xab, local_r2->len);
    local_r2->retval = local_r2->len;
    local_r2->version = 1;
    local_r2->callback(local_r2);

    auto *peer_r2 = f.peer_take(2, OSD_OP_SEC_READ);
    peer_r2->reply.hdr.retval = -EIO;
    peer_r2->callback(peer_r2);

    assert(scrub2_retval == 0);

    // THE KEY ASSERTION — LOC_CORRUPTED must persist
    osd2_corrupted = check_corrupted();
    if (!osd2_corrupted)
        printf("BUG: LOC_CORRUPTED cleared on second scrub\n");
    assert(osd2_corrupted);

    delete scrub2;
    printf("test_scrub_corruption_persists passed\n");
}

// Scrub two replicated OSDs that have identical data but *different* bitmaps.
// The expectation is that scrub detects the mismatch and marks at least one
// replica as corrupted (or inconsistent) because bitmaps differ even though
// data payloads are byte-identical.
bool test_scrub_same_data_diff_bitmaps(int ctr)
{
    bool more = false;
    printf("test_scrub_same_data_diff_bitmaps[%d]\n", ctr);

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
    f.connect_peer(2);
    object_id oid = { INODE_WITH_POOL(1, 1), 0 };
    f.reply_local_list({ { oid, 1 } }, 1);
    f.reply_peer_list(2, { { oid, 1 } }, 1);
    f.ringloop->loop();
    assert(f.pg(1, 1).state & PG_ACTIVE);

    // Scrub

    auto *scrub = new osd_op_t();
    scrub->op_type = OSD_OP_IN;
    scrub->client_id = 0;
    scrub->req.rw.header.magic = SECONDARY_OSD_OP_MAGIC;
    scrub->req.rw.header.id = 2;
    scrub->req.rw.header.opcode = OSD_OP_SCRUB;
    scrub->req.rw.inode = oid.inode;
    scrub->req.rw.offset = oid.stripe;
    scrub->req.rw.len = 0;

    int scrub_retval = -1;
    scrub->callback = [&scrub_retval](osd_op_t *op) {
        scrub_retval = op->reply.hdr.retval;
    };
    f.exec(scrub);

    // Reply with same data but different bitmaps (ctr == 0) or same bitmaps (ctr == 1)
    assert(f.bs->queued.size() == 1);
    auto *local_read = f.bs->take(BS_OP_READ);
    assert(local_read->opcode == BS_OP_READ);
    memset(local_read->buf, 0xab, local_read->len);
    memset(local_read->bitmap, 0xff, 4);
    local_read->retval = local_read->len;
    local_read->version = 1;
    local_read->callback(local_read);

    auto *peer_read = f.peer_take(2, OSD_OP_SEC_READ);
    assert(peer_read->iov.count == 1);
    memset(peer_read->iov.buf[0].iov_base, 0xab, peer_read->req.sec_rw.len);
    if (!ctr)
    {
        memset(peer_read->bitmap, 0xf0, 4);
        more = true;
    }
    else
        memset(peer_read->bitmap, 0xff, 4);
    peer_read->reply.hdr.retval = peer_read->req.sec_rw.len;
    peer_read->reply.sec_rw.attr_len = 4;
    peer_read->reply.sec_rw.version = 1;
    peer_read->callback(peer_read);

    assert(scrub_retval == 0);

    if (!ctr)
    {
        // Object should be marked as inconsistent
        auto check_inconsistent = [&]()
        {
            if (!(f.pg(1, 1).state & PG_HAS_INCONSISTENT))
                return false;
            auto st_it = f.pg(1, 1).inconsistent_objects.find(oid);
            if (st_it == f.pg(1, 1).inconsistent_objects.end())
                return false;
            return true;
        };
        bool marked = check_inconsistent();
        if (!marked)
            printf("BUG: scrub did not detect bitmap mismatch\n");
        assert(marked);
    }
    else
    {
        assert(f.pg(1, 1).state == PG_ACTIVE);
        assert(!f.pg(1, 1).degraded_objects.size());
        assert(!f.pg(1, 1).inconsistent_objects.size());
    }

    printf("test_scrub_same_data_diff_bitmaps[%d] passed\n", ctr);
    delete scrub;

    return more;
}

void test_ec33_scrub_only_first_part()
{
    printf("test_ec33_scrub_only_first_part\n");

    osd_test_fixture_t f;

    // EC 3+3 pool, PG 1, OSD set {1,2,3,4,5,6}, primary = OSD 2
    f.configure_ec33_pool();

    f.start(json11::Json::object {
        { "osd_num", 2 },
        { "etcd_address", "127.0.0.1:2379" },
        { "immediate_commit", "none" },
        { "block_size", 131072 },
        { "bitmap_granularity", 4096 },
    });

    // Connect peers 1..6
    for (int peer = 1; peer <= 6; peer++)
        if (peer != 2)
            f.connect_peer(peer);
    f.complete_peering_empty();
    f.ringloop->loop();
    assert(f.pg(1, 1).state == PG_ACTIVE);

    // Scrub
    object_id oid = { INODE_WITH_POOL(1, 1), 0 };
    auto *scrub = new osd_op_t();
    scrub->op_type = OSD_OP_IN;
    scrub->client_id = 0;
    scrub->req.rw.header.magic = SECONDARY_OSD_OP_MAGIC;
    scrub->req.rw.header.id = 2;
    scrub->req.rw.header.opcode = OSD_OP_SCRUB;
    scrub->req.rw.inode = oid.inode;
    scrub->req.rw.offset = oid.stripe;
    scrub->req.rw.len = 0;

    int scrub_retval = -1;
    scrub->callback = [&scrub_retval](osd_op_t *op) {
        scrub_retval = op->reply.hdr.retval;
    };
    f.exec(scrub);

    // Prepare data
    std::vector<uint8_t> data_buf(6 * 128*1024);
    std::vector<uint8_t> bmp_buf(6 * 4);
    {
        osd_num_t fake_osd_set[6] = { 1, 2, 3, 4, 5, 6 };
        // Only chunk 1 is full
        memset(data_buf.data(), 0xab, 1 * 128*1024);
        memset(bmp_buf.data(), 0xff, 1 * 4);
        use_ec(6, 3, true);
        osd_rmw_stripe_t stripes[6] = {};
        for (int i = 0; i < 6; i++)
            stripes[i].bmp_buf = bmp_buf.data() + i*4;
        split_stripes(3, 128*1024, 0, 128*1024, stripes);
        void *rmw_buf = calc_rmw(data_buf.data(), stripes, fake_osd_set, 6, 3, 6, fake_osd_set, 128*1024, 4);
        assert(rmw_buf);
        calc_rmw_parity_ec(stripes, 6, 3, fake_osd_set, fake_osd_set, 128*1024, 4);
        use_ec(6, 3, false);
        memcpy(data_buf.data() + 3*128*1024, rmw_buf + 0*128*1024, 128*1024);
        memcpy(data_buf.data() + 4*128*1024, rmw_buf + 1*128*1024, 128*1024);
        memcpy(data_buf.data() + 5*128*1024, rmw_buf + 2*128*1024, 128*1024);
        free(rmw_buf);
    }

    // peer 1 responds with corrupted data (empty chunk)
    // others are ok
    for (uint64_t peer = 1; peer <= 6; peer++)
    {
        if (peer == 2)
        {
            assert(f.bs->queued.size() == 1);
            auto *local_read = f.bs->take(BS_OP_READ);
            assert(local_read->opcode == BS_OP_READ);
            memcpy(local_read->buf, data_buf.data() + (peer-1)*128*1024, local_read->len);
            memcpy(local_read->bitmap, bmp_buf.data() + (peer-1)*4, 4);
            local_read->retval = local_read->len;
            local_read->version = 1;
            local_read->callback(local_read);
            continue;
        }
        auto *peer_read = f.peer_take(peer, OSD_OP_SEC_READ);
        assert(peer_read->req.sec_rw.len == 128*1024);
        assert(peer_read->iov.count == 1);
        if (peer == 1)
            memset(peer_read->iov.buf[0].iov_base, 0, peer_read->req.sec_rw.len);
        else
            memcpy(peer_read->iov.buf[0].iov_base, data_buf.data() + (peer-1)*128*1024, peer_read->req.sec_rw.len);
        memcpy(peer_read->bitmap, bmp_buf.data() + (peer-1)*4, 4);
        peer_read->reply.hdr.retval = peer_read->req.sec_rw.len;
        peer_read->reply.sec_rw.attr_len = 4;
        peer_read->reply.sec_rw.version = 1;
        peer_read->callback(peer_read);
    }

    assert(scrub_retval == 0);
    delete scrub;

    assert(f.pg(1, 1).inflight == 0);
    assert(f.pg(1, 1).state == (PG_ACTIVE|PG_HAS_CORRUPTED|PG_HAS_DEGRADED));
    assert(!f.pg(1, 1).inconsistent_objects.size());
    assert(f.pg(1, 1).degraded_objects.size() == 1);

    printf("test_ec33_scrub_only_first_part passed\n");
}

void test_ec33_recovery_missing_first_part()
{
    printf("test_ec33_recovery_missing_first_part\n");

    osd_test_fixture_t f;

    // EC 3+3 pool, PG 1, OSD set {1,2,3,4,5,6}, primary = OSD 2
    f.configure_ec33_pool();
    // FIXME: Fix this test without this key.
    f.st_cli->set("/vitastor/pg/history/1/1", json11::Json::object {
        { "osd_sets", json11::Json::array {
            json11::Json::array{ 1, 2, 3, 4, 5, 6 },
        } },
    });

    f.start(json11::Json::object {
        { "osd_num", 2 },
        { "etcd_address", "127.0.0.1:2379" },
        { "immediate_commit", "none" },
        { "block_size", 131072 },
        { "bitmap_granularity", 4096 },
        { "no_recovery", true },
    });

    // Connect peers 1..6
    for (int peer = 1; peer <= 6; peer++)
        if (peer != 2)
            f.connect_peer(peer);
    inode_t ino = INODE_WITH_POOL(1, 1);
    f.reply_local_list({ { { ino, 1 }, 1 } }, 1);
    f.reply_peer_list(1, {}, 0); // OSD 1 has missing stripe
    for (int peer = 3; peer <= 6; peer++)
        f.reply_peer_list(peer, { { { ino, (uint64_t)(peer-1) }, 1 } }, 1);
    f.ringloop->loop();
    assert(f.pg(1, 1).state == (PG_ACTIVE|PG_HAS_DEGRADED));

    // Recovery write
    auto *recovery_op = make_write_op(ino, 0, 0, 0);
    int recovery_retval = -1;
    recovery_op->callback = [&recovery_retval](osd_op_t *op)
    {
        recovery_retval = op->reply.hdr.retval;
    };
    f.exec(recovery_op);

    // OSD should read from 2, 3, 4 and write to all
    assert(!f.peer(1)->sent_ops.size());
    assert(!f.peer(5)->sent_ops.size());
    assert(!f.peer(6)->sent_ops.size());
    {
        auto *local_read = f.bs->take(BS_OP_READ);
        assert(local_read->len == 128*1024);
        memset(local_read->buf, 0xab, local_read->len);
        memset(local_read->bitmap, 0xff, 4);
        local_read->retval = local_read->len;
        local_read->version = 1;
        local_read->callback(local_read);
    }
    {
        auto *peer_read = f.peer_take(3, OSD_OP_SEC_READ);
        assert(peer_read->req.sec_rw.len == 128*1024);
        assert(peer_read->iov.count == 1);
        memset(peer_read->iov.buf[0].iov_base, 0xab, peer_read->req.sec_rw.len);
        memset(peer_read->bitmap, 0xff, 4);
        peer_read->reply.hdr.retval = peer_read->req.sec_rw.len;
        peer_read->reply.sec_rw.attr_len = 4;
        peer_read->reply.sec_rw.version = 1;
        peer_read->callback(peer_read);
    }
    {
        auto *peer_read = f.peer_take(4, OSD_OP_SEC_READ);
        assert(peer_read->req.sec_rw.len == 128*1024);
        assert(peer_read->iov.count == 1);
        memset(peer_read->iov.buf[0].iov_base, 0xab, peer_read->req.sec_rw.len); // 0xab xored 3 times -> 0xab
        memset(peer_read->bitmap, 0xff, 4); // 0xff xored 3 times -> 0xff
        peer_read->reply.hdr.retval = peer_read->req.sec_rw.len;
        peer_read->reply.sec_rw.attr_len = 4;
        peer_read->reply.sec_rw.version = 1;
        peer_read->callback(peer_read);
    }

    // Check writes
    {
        auto *peer_write = f.peer_take(1, OSD_OP_SEC_WRITE);
        assert(peer_write->req.sec_rw.len == 128*1024);
        assert(peer_write->req.sec_rw.version == ((1 << 16) | 1));
        assert(peer_write->req.sec_rw.attr_len == 4);
        assert(peer_write->iov.count == 1);
        assert(memcheck((uint8_t*)peer_write->iov.buf[0].iov_base, 0xab, peer_write->req.sec_rw.len));
        // peer 1 bitmap must be written
        assert(peer_write->req.sec_rw.attr_len == 4);
        printf("osd 1 bitmap: %08x\n", *(uint32_t*)peer_write->bitmap);
        assert(memcheck((uint8_t*)peer_write->bitmap, 0xff, 4));
        peer_write->reply = {};
        peer_write->reply.hdr.retval = peer_write->req.sec_rw.len;
        peer_write->reply.sec_rw.version = peer_write->req.sec_rw.version;
        peer_write->callback(peer_write);
    }
    {
        auto *local_write = f.bs->take(BS_OP_WRITE);
        assert(local_write->len == 0);
        assert(local_write->version == ((1 << 16) | 1));
        if (local_write->bitmap)
        {
            printf("osd 2 bitmap: %08x\n", *(uint32_t*)local_write->bitmap);
            assert(memcheck(local_write->bitmap, 0xff, 4));
        }
        local_write->retval = local_write->len;
        local_write->callback(local_write);
    }
    for (osd_num_t peer = 3; peer <= 6; peer++)
    {
        auto *peer_write = f.peer_take(peer, OSD_OP_SEC_WRITE);
        assert(peer_write->req.sec_rw.len == 0);
        assert(peer_write->req.sec_rw.version == ((1 << 16) | 1));
        if (peer_write->req.sec_rw.attr_len)
        {
            // peer 2-6 bitmaps may be either skipped or set to a correct value
            // first parity is 0xff, second is 0x11, third is 0x00
            printf("osd %ju bitmap: %08x\n", peer, *(uint32_t*)peer_write->bitmap);
            assert(peer_write->req.sec_rw.attr_len == 4);
            assert(memcheck((uint8_t*)peer_write->bitmap, peer <= 4 ? 0xff : (peer == 5 ? 0x11 : 0x00), 4));
        }
        peer_write->reply = {};
        peer_write->reply.hdr.retval = peer_write->req.sec_rw.len;
        peer_write->reply.sec_rw.version = peer_write->req.sec_rw.version;
        peer_write->callback(peer_write);
    }

    assert(recovery_retval == 0);
    delete recovery_op;

    // It triggers autosync so don't check for inflight

    printf("test_ec33_recovery_missing_first_part passed\n");
}

int main(int narg, char *args[])
{
    test_load_global_config();
    test_replicated_write();
    test_scrub_corruption_persists();
    test_ec33_scrub_only_first_part();
    for (int i = 0; test_scrub_same_data_diff_bitmaps(i); i++) {}
    test_ec33_recovery_missing_first_part();
    return 0;
}
