// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#undef NDEBUG
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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
    if (len > 0)
    {
        op->buf = malloc(len);
        memset(op->buf, fill, len);
    }
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
    f.bs_zero_read_ok(0);

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
    f.bs_zero_read_ok(0);

    // Stage 2: local write + peer write
    assert(f.bs->queued.size() == 1);
    assert(f.peer(2)->sent_ops.size() == 1);
    auto *local_write = f.bs->take();
    auto *peer_write = f.peer_take(2, OSD_OP_SEC_WRITE_STABLE);

    // Complete both
    local_write->retval = local_write->len;
    local_write->callback(local_write);
    assert(final_retval == -1); // still waiting for peer
    peer_write->reply.sec_rw.version = 1;
    f.peer_complete(peer_write, peer_write->req.sec_rw.len);

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
    f.peer_complete(peer_r1, -EIO);

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
    f.peer_complete(peer_r2, -EIO);

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
    peer_read->reply.sec_rw.attr_len = 4;
    peer_read->reply.sec_rw.version = 1;
    f.peer_complete(peer_read, peer_read->req.sec_rw.len);

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
    f.configure_ec_pool(3, 3);

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
        peer_read->reply.sec_rw.attr_len = 4;
        peer_read->reply.sec_rw.version = 1;
        f.peer_complete(peer_read, peer_read->req.sec_rw.len);
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
    f.configure_ec_pool(3, 3);

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
    inode_t ino2 = INODE_WITH_POOL(1, 2);
    f.reply_local_list({ { { ino, 1 }, 1 }, { { ino2, 1 }, 1 } }, 2);
    f.reply_peer_list(1, {}, 0); // OSD 1 has missing stripe
    for (int peer = 3; peer <= 6; peer++)
        f.reply_peer_list(peer, { { { ino, (uint64_t)(peer-1) }, 1 }, { { ino2, (uint64_t)(peer-1) }, 1 } }, 2);
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
        peer_read->reply.sec_rw.attr_len = 4;
        peer_read->reply.sec_rw.version = 1;
        f.peer_complete(peer_read, peer_read->req.sec_rw.len);
    }
    {
        auto *peer_read = f.peer_take(4, OSD_OP_SEC_READ);
        assert(peer_read->req.sec_rw.len == 128*1024);
        assert(peer_read->iov.count == 1);
        memset(peer_read->iov.buf[0].iov_base, 0xab, peer_read->req.sec_rw.len); // 0xab xored 3 times -> 0xab
        memset(peer_read->bitmap, 0xff, 4); // 0xff xored 3 times -> 0xff
        peer_read->reply.sec_rw.attr_len = 4;
        peer_read->reply.sec_rw.version = 1;
        f.peer_complete(peer_read, peer_read->req.sec_rw.len);
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
        peer_write->reply.sec_rw.version = peer_write->req.sec_rw.version;
        f.peer_complete(peer_write, peer_write->req.sec_rw.len);
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
        peer_write->reply.sec_rw.version = peer_write->req.sec_rw.version;
        f.peer_complete(peer_write, peer_write->req.sec_rw.len);
    }

    assert(recovery_retval == 0);
    delete recovery_op;

    // There is also ino2 so the PG doesn't change state and doesn't trigger autosync

    assert(f.pg(1, 1).inflight == 0);

    printf("test_ec33_recovery_missing_first_part passed\n");
}

// Check that partial parity-less EC writes don't destroy allocation bitmaps
void test_ec42_write_parityless()
{
    printf("test_ec42_write_parityless\n");

    osd_test_fixture_t f;

    f.configure_ec_pool(4, 2);
    // FIXME: Fix this test without this key.
    f.st_cli->set("/vitastor/pg/history/1/1", json11::Json::object {
        { "osd_sets", json11::Json::array {
            json11::Json::array{ 1, 2, 3, 4 },
        } },
    });

    f.start(json11::Json::object {
        { "osd_num", 2 },
        { "etcd_address", "127.0.0.1:2379" },
        { "immediate_commit", "none" },
        { "block_size", 131072 },
        { "bitmap_granularity", 4096 },
    });

    // Connect peers 1..4
    for (int peer = 1; peer <= 4; peer++)
        if (peer != 2)
            f.connect_peer(peer);
    f.complete_peering_empty();
    assert(f.pg(1, 1).state == (PG_ACTIVE|PG_DEGRADED|PG_LEFT_ON_DEAD));

    // Simple write
    auto *wr_op = make_write_op(INODE_WITH_POOL(1, 1), 0, 4096, 0xab);
    int wr_retval = -1;
    wr_op->callback = [&wr_retval](osd_op_t *op)
    {
        wr_retval = op->reply.hdr.retval;
    };
    f.exec(wr_op);

    // OSD should read from 1 and write to all
    {
        auto *peer_read = f.peer_take(1, OSD_OP_SEC_READ);
        assert(peer_read->req.sec_rw.len == 0);
        *(uint32_t*)peer_read->bitmap = 0xfffffffe;
        peer_read->reply.sec_rw.attr_len = 4;
        peer_read->reply.sec_rw.version = 1;
        f.peer_complete(peer_read, peer_read->req.sec_rw.len);
    }

    // Check writes
    {
        auto *peer_write = f.peer_take(1, OSD_OP_SEC_WRITE);
        assert(peer_write->req.sec_rw.offset == 0);
        assert(peer_write->req.sec_rw.len == 4*1024);
        assert(peer_write->req.sec_rw.version == ((1 << 16) | 1));
        assert(peer_write->req.sec_rw.attr_len == 4);
        assert(peer_write->iov.count == 1);
        assert(memcheck((uint8_t*)peer_write->iov.buf[0].iov_base, 0xab, peer_write->req.sec_rw.len));
        // peer 1 bitmap must be written
        assert(peer_write->req.sec_rw.attr_len == 4);
        printf("osd 1 bitmap: %08x\n", *(uint32_t*)peer_write->bitmap);
        assert(memcheck((uint8_t*)peer_write->bitmap, 0xff, 4));
        peer_write->reply = {};
        peer_write->reply.sec_rw.version = peer_write->req.sec_rw.version;
        f.peer_complete(peer_write, peer_write->req.sec_rw.len);
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
    for (osd_num_t peer = 3; peer <= 4; peer++)
    {
        auto *peer_write = f.peer_take(peer, OSD_OP_SEC_WRITE);
        assert(peer_write->req.sec_rw.len == 0);
        assert(peer_write->req.sec_rw.version == ((1 << 16) | 1));
        if (peer_write->req.sec_rw.attr_len)
        {
            // peer 2-6 bitmaps may be either skipped or set to a correct value
            // first parity is 0x0, second is 0x11
            printf("osd %ju bitmap: %08x\n", peer, *(uint32_t*)peer_write->bitmap);
            assert(peer_write->req.sec_rw.attr_len == 4);
            assert(memcheck((uint8_t*)peer_write->bitmap, peer <= 4 ? 0xff : (peer == 5 ? 0x00 : 0xff), 4));
        }
        peer_write->reply = {};
        peer_write->reply.sec_rw.version = peer_write->req.sec_rw.version;
        f.peer_complete(peer_write, peer_write->req.sec_rw.len);
    }

    assert(wr_retval == wr_op->req.rw.len);
    delete wr_op;

    assert(f.pg(1, 1).inflight == 0);

    printf("test_ec42_write_parityless passed\n");
}

// Regression test for the incorrect chained read bitmap reconstruction
// where read_bitmaps() did not mark missing parts with .missing if their
// data wasn't required by the read.
// EC 3+3, parent object is missing on OSDs 1 and 3 (chunks 0 and 2).
// missing_flags is [1, 0, 0, 0, 0, 0] — chunk 2 is unavailable but not
// needed, so its slot is the phantom source.
void test_ec33_chain_read_phantom_bitmap_source()
{
    printf("test_ec33_chain_read_phantom_bitmap_source\n");

    osd_test_fixture_t f;

    f.configure_ec_pool(3, 3, /*primary_osd*/ 6);
    f.st_cli->set("/vitastor/pg/history/1/1", json11::Json::object {
        { "osd_sets", json11::Json::array {
            json11::Json::array{ 1, 2, 3, 4, 5, 6 },
        } },
    });
    f.st_cli->set("/vitastor/config/inode/1/2", json11::Json::object {
        { "name", "child" },
        { "parent_id", 3 },
    }, 1);
    f.st_cli->set("/vitastor/config/inode/1/3", json11::Json::object {
        { "name", "parent" },
    }, 1);

    f.start(json11::Json::object {
        { "osd_num", 6 },
        { "etcd_address", "127.0.0.1:2379" },
        { "immediate_commit", "none" },
        { "block_size", 131072 },
        { "bitmap_granularity", 4096 },
        { "no_recovery", true },
    });

    inode_t child_inode  = INODE_WITH_POOL(1, 2);
    inode_t parent_inode = INODE_WITH_POOL(1, 3);

    for (int peer = 1; peer <= 5; peer++)
        f.connect_peer(peer);

    // Peering:
    //   child  stripe 0 osd_set = [1, 2, 3, 4, 5, 6]  (clean)
    //   parent stripe 0 osd_set = [0, 2, 0, 4, 5, 6]  (degraded)
    f.reply_peer_list(1, { { { child_inode, 0 }, 1 } }, 1);
    f.reply_peer_list(2, { { { child_inode, 1 }, 1 }, { { parent_inode, 1 }, 1 } }, 2);
    f.reply_peer_list(3, { { { child_inode, 2 }, 1 } }, 1);
    f.reply_peer_list(4, { { { child_inode, 3 }, 1 }, { { parent_inode, 3 }, 1 } }, 2);
    f.reply_peer_list(5, { { { child_inode, 4 }, 1 }, { { parent_inode, 4 }, 1 } }, 2);
    f.reply_local_list({ { { child_inode, 5 }, 1 }, { { parent_inode, 5 }, 1 } }, 2);
    f.ringloop->loop();
    assert(f.pg(1, 1).state == (PG_ACTIVE|PG_HAS_DEGRADED));

    // Fill parent bitmaps with distinguishable non-zero patterns
    uint8_t parent_bmp[6 * 4] = {
        0, 0, 0, 0, 0xAA, 0xBB, 0xCC, 0xDD, 0x11, 0x22, 0x33, 0x44
    };
    {
        osd_num_t fake_set[6] = { 1, 2, 3, 4, 5, 6 };
        std::vector<uint8_t> data_buf(6 * 128*1024);
        use_ec(6, 3, true);
        osd_rmw_stripe_t s[6] = {};
        for (int i = 0; i < 6; i++)
            s[i].bmp_buf = parent_bmp + i*4;
        split_stripes(3, 128*1024, 0, 128*1024, s);
        void *rmw_buf = calc_rmw(data_buf.data(), s, fake_set, 6, 3, 6, fake_set, 128*1024, 4);
        assert(rmw_buf);
        calc_rmw_parity_ec(s, 6, 3, fake_set, fake_set, 128*1024, 4);
        use_ec(6, 3, false);
        free(rmw_buf);
    }
    // the value we'll check, the first chunk is fully-allocated by calc_rmw_parity_ec()
    assert(*(uint32_t*)parent_bmp == 0xFFFFFFFF);

    // Send a chained read for child, 0-128k
    auto *read_op = new osd_op_t();
    read_op->op_type = OSD_OP_IN;
    read_op->client_id = 0;
    read_op->req.rw.header.magic = SECONDARY_OSD_OP_MAGIC;
    read_op->req.rw.header.id = 1;
    read_op->req.rw.header.opcode = OSD_OP_READ;
    read_op->req.rw.inode = child_inode;
    read_op->req.rw.offset = 0;
    read_op->req.rw.len = 128*1024;
    read_op->req.rw.meta_revision = 1;
    int read_retval = -1;
    uint32_t response_bmp = 0;
    read_op->callback = [&](osd_op_t *op)
    {
        read_retval = op->reply.hdr.retval;
        if (op->bitmap_buf)
            response_bmp = *(uint32_t*)op->bitmap_buf;
    };
    f.exec(read_op);

    // Pop a single-oid sec_read_bmp subop and reply with the given bitmap
    auto reply_bmp = [&](osd_num_t peer, inode_t ino, uint64_t stripe, uint32_t bmp)
    {
        auto *subop = f.peer_take(peer, OSD_OP_SEC_READ_BMP);
        assert(subop->req.sec_read_bmp.len == sizeof(obj_ver_id));
        auto *ov = (obj_ver_id*)subop->buf;
        assert(ov[0].oid.inode == ino);
        assert(ov[0].oid.stripe == stripe);
        free(subop->buf);
        subop->buf = malloc(8 + 4);
        *(uint64_t*)subop->buf = 1;
        memcpy((uint8_t*)subop->buf + 8, &bmp, 4);
        f.peer_complete(subop, 8 + 4);
    };

    reply_bmp(1, child_inode,  0, 0); // child chunk is empty
    reply_bmp(2, parent_inode, 1, *(uint32_t*)(parent_bmp + 1*4));
    reply_bmp(4, parent_inode, 3, *(uint32_t*)(parent_bmp + 3*4));
    reply_bmp(5, parent_inode, 4, *(uint32_t*)(parent_bmp + 4*4));

    auto try_sec_read = [&](osd_num_t peer, const uint8_t *chunk_bmp)
    {
        auto *cl = f.peer(peer);
        for (auto & kv: cl->sent_ops)
        {
            if (kv.second->req.hdr.opcode == OSD_OP_SEC_READ)
            {
                auto *subop = kv.second;
                printf("OSD %ju replies to sec_read %jx:%jx\n", peer, subop->req.sec_rw.oid.inode, subop->req.sec_rw.oid.stripe);
                assert(subop->req.sec_rw.oid.inode == parent_inode);
                cl->sent_ops.erase(subop->req.hdr.id);
                if (subop->bitmap)
                    memcpy(subop->bitmap, chunk_bmp, 4);
                subop->reply.hdr.retval = subop->req.sec_rw.len;
                subop->reply.sec_rw.attr_len = 4;
                subop->reply.sec_rw.version = 1;
                subop->callback(subop);
                return;
            }
        }
    };
    // Reply to reads from all OSDs except local for a universal check.
    // Actually it reads from OSD 2, 4, 5 (parent chunks 1, 3, 4) to recover parent chunk 0.
    try_sec_read(1, parent_bmp + 0*4);
    try_sec_read(2, parent_bmp + 1*4);
    try_sec_read(3, parent_bmp + 2*4);
    try_sec_read(4, parent_bmp + 3*4);
    try_sec_read(5, parent_bmp + 4*4);

    // The op should have finished
    assert(read_retval == (int)read_op->req.rw.len);

    // Check the bitmap (0xffffffff)
    printf("Response bitmap: %08x, expected ffffffff\n", response_bmp);
    assert(response_bmp == 0xffffffff);

    delete read_op;
    printf("test_ec33_chain_read_phantom_bitmap_source passed\n");
}

// Regression test for the bug where retrying a chained read after -EIO would
// crash on assert(op_data->st < base_state) in read_bitmaps() because the
// state counter was not reset before the retry.
void test_chained_read_eio_retry()
{
    printf("test_chained_read_eio_retry\n");

    osd_test_fixture_t f;

    f.configure_ec_pool(3, 3, /*primary_osd*/ 6);
    f.st_cli->set("/vitastor/pg/history/1/1", json11::Json::object {
        { "osd_sets", json11::Json::array {
            json11::Json::array{ 1, 2, 3, 4, 5, 6 },
        } },
    });
    f.st_cli->set("/vitastor/config/inode/1/2", json11::Json::object {
        { "name", "child" },
        { "parent_id", 3 },
    }, 1);
    f.st_cli->set("/vitastor/config/inode/1/3", json11::Json::object {
        { "name", "parent" },
    }, 1);

    f.start(json11::Json::object {
        { "osd_num", 6 },
        { "etcd_address", "127.0.0.1:2379" },
        { "immediate_commit", "none" },
        { "block_size", 131072 },
        { "bitmap_granularity", 4096 },
        { "no_recovery", true },
    });

    inode_t child_inode  = INODE_WITH_POOL(1, 2);
    inode_t parent_inode = INODE_WITH_POOL(1, 3);

    for (int peer = 1; peer <= 5; peer++)
        f.connect_peer(peer);

    // Peering:
    //   child  stripe 0 osd_set = [1, 2, 3, 4, 5, 6]  (clean)
    //   parent stripe 0 osd_set = [0, 2, 0, 4, 5, 6]  (degraded)
    f.reply_peer_list(1, { { { child_inode, 0 }, 1 } }, 1);
    f.reply_peer_list(2, { { { child_inode, 1 }, 1 }, { { parent_inode, 1 }, 1 } }, 2);
    f.reply_peer_list(3, { { { child_inode, 2 }, 1 } }, 1);
    f.reply_peer_list(4, { { { child_inode, 3 }, 1 }, { { parent_inode, 3 }, 1 } }, 2);
    f.reply_peer_list(5, { { { child_inode, 4 }, 1 }, { { parent_inode, 4 }, 1 } }, 2);
    f.reply_local_list({ { { child_inode, 5 }, 1 }, { { parent_inode, 5 }, 1 } }, 2);
    f.ringloop->loop();
    assert(f.pg(1, 1).state == (PG_ACTIVE|PG_HAS_DEGRADED));

    // Fill parent bitmaps with distinguishable non-zero patterns
    uint8_t parent_bmp[6 * 4] = {
        0, 0, 0, 0, 0xAA, 0xBB, 0xCC, 0xDD, 0x11, 0x22, 0x33, 0x44
    };
    {
        osd_num_t fake_set[6] = { 1, 2, 3, 4, 5, 6 };
        std::vector<uint8_t> data_buf(6 * 128*1024);
        use_ec(6, 3, true);
        osd_rmw_stripe_t s[6] = {};
        for (int i = 0; i < 6; i++)
            s[i].bmp_buf = parent_bmp + i*4;
        split_stripes(3, 128*1024, 0, 128*1024, s);
        void *rmw_buf = calc_rmw(data_buf.data(), s, fake_set, 6, 3, 6, fake_set, 128*1024, 4);
        assert(rmw_buf);
        calc_rmw_parity_ec(s, 6, 3, fake_set, fake_set, 128*1024, 4);
        use_ec(6, 3, false);
        free(rmw_buf);
    }
    assert(*(uint32_t*)parent_bmp == 0xFFFFFFFF);

    // Send a chained read for child, 0-128k
    auto *read_op = new osd_op_t();
    read_op->op_type = OSD_OP_IN;
    read_op->client_id = 0;
    read_op->req.rw.header.magic = SECONDARY_OSD_OP_MAGIC;
    read_op->req.rw.header.id = 2;
    read_op->req.rw.header.opcode = OSD_OP_READ;
    read_op->req.rw.inode = child_inode;
    read_op->req.rw.offset = 0;
    read_op->req.rw.len = 128*1024;
    read_op->req.rw.meta_revision = 1;
    int read_retval = -1;
    uint32_t response_bmp = 0;
    bool eio_injected = false;
    read_op->callback = [&](osd_op_t *op)
    {
        read_retval = op->reply.hdr.retval;
        if (op->bitmap_buf)
            response_bmp = *(uint32_t*)op->bitmap_buf;
    };
    f.exec(read_op);

    // Pop a single-oid sec_read_bmp subop and reply with the given bitmap
    auto reply_bmp = [&](osd_num_t peer, inode_t ino, uint64_t stripe, uint32_t bmp)
    {
        auto *subop = f.peer_take(peer, OSD_OP_SEC_READ_BMP);
        assert(subop->req.sec_read_bmp.len == sizeof(obj_ver_id));
        auto *ov = (obj_ver_id*)subop->buf;
        assert(ov[0].oid.inode == ino);
        assert(ov[0].oid.stripe == stripe);
        free(subop->buf);
        subop->buf = malloc(8 + 4);
        *(uint64_t*)subop->buf = 1;
        memcpy((uint8_t*)subop->buf + 8, &bmp, 4);
        f.peer_complete(subop, 8 + 4);
    };

    // --- First pass: reply to bitmap subops ---
    reply_bmp(1, child_inode,  0, 0); // child chunk is empty
    reply_bmp(2, parent_inode, 1, *(uint32_t*)(parent_bmp + 1*4));
    reply_bmp(4, parent_inode, 3, *(uint32_t*)(parent_bmp + 3*4));
    reply_bmp(5, parent_inode, 4, *(uint32_t*)(parent_bmp + 4*4));

    // --- First pass: reply to data sec_read subops, inject -EIO on one ---
    // For degraded parent stripe 0 (osd_set = [0, 2, 0, 4, 5, 6]),
    // reads only go to OSDs that have data (2, 4, 5, 6 — 6 is local).
    // Inject -EIO on OSD 2 (parent chunk 1).
    {
        auto *cl = f.peer(2);
        for (auto & kv: cl->sent_ops)
        {
            if (kv.second->req.hdr.opcode == OSD_OP_SEC_READ)
            {
                auto *subop = kv.second;
                cl->sent_ops.erase(subop->req.hdr.id);
                printf("  Injecting -EIO on OSD 2 sec_read\n");
                subop->reply.hdr.retval = -EIO;
                subop->callback(subop);
                eio_injected = true;
                break;
            }
        }
    }
    // Reply successfully to remaining sec_reads
    for (auto peer: { 4, 5 })
    {
        auto *cl = f.peer(peer);
        for (auto & kv: cl->sent_ops)
        {
            if (kv.second->req.hdr.opcode == OSD_OP_SEC_READ)
            {
                auto *subop = kv.second;
                cl->sent_ops.erase(subop->req.hdr.id);
                uint8_t *chunk_bmp = parent_bmp + (peer == 4 ? 3*4 : 4*4);
                if (subop->bitmap)
                    memcpy(subop->bitmap, chunk_bmp, 4);
                subop->reply.hdr.retval = subop->req.sec_rw.len;
                subop->reply.sec_rw.attr_len = 4;
                subop->reply.sec_rw.version = 1;
                subop->callback(subop);
                break;
            }
        }
    }
    assert(eio_injected);

    // --- After -EIO the OSD should have retried: resume_1 -> read_bitmaps -> submit_chained_read_requests
    // Reply to bitmap subops again (second pass) — may be on different OSDs this time
    for (int peer = 1; peer <= 5; peer++)
    {
        auto *cl = f.peer(peer);
        std::vector<osd_op_t*> bmp_ops;
        for (auto & kv: cl->sent_ops)
            if (kv.second->req.hdr.opcode == OSD_OP_SEC_READ_BMP)
                bmp_ops.push_back(kv.second);
        for (auto *subop: bmp_ops)
        {
            cl->sent_ops.erase(subop->req.hdr.id);
            // Determine inode from the subop buffer
            auto *ov = (obj_ver_id*)subop->buf;
            inode_t ino = ov[0].oid.inode;
            uint64_t stripe = ov[0].oid.stripe;
            // Find matching bitmap entry
            int bmp_idx = -1;
            if (ino == child_inode && stripe == 0)
                bmp_idx = 0;
            else if (ino == parent_inode)
                bmp_idx = stripe - 0; // stripe = chunk
            uint32_t bmp = 0;
            if (bmp_idx >= 0 && bmp_idx < 6)
                bmp = *(uint32_t*)(parent_bmp + bmp_idx * 4);
            free(subop->buf);
            subop->buf = malloc(8 + 4);
            *(uint64_t*)subop->buf = 1;
            memcpy((uint8_t*)subop->buf + 8, &bmp, 4);
            subop->reply.hdr.retval = 8 + 4;
            subop->callback(subop);
        }
    }

    // Reply to data sec_read subops (second pass) — all successful now
    for (int peer = 1; peer <= 5; peer++)
    {
        auto *cl = f.peer(peer);
        std::vector<osd_op_t*> read_ops;
        for (auto & kv: cl->sent_ops)
            if (kv.second->req.hdr.opcode == OSD_OP_SEC_READ)
                read_ops.push_back(kv.second);
        for (auto *subop: read_ops)
        {
            cl->sent_ops.erase(subop->req.hdr.id);
            if (subop->bitmap)
                memcpy(subop->bitmap, parent_bmp + 0*4, 4); // best effort
            subop->reply.hdr.retval = subop->req.sec_rw.len;
            subop->reply.sec_rw.attr_len = 4;
            subop->reply.sec_rw.version = 1;
            subop->callback(subop);
        }
    }
    // Handle any local BS_OP_READ subops too
    while (!f.bs->queued.empty())
    {
        auto *bs_op = f.bs->take(BS_OP_READ);
        memset(bs_op->buf, 0, bs_op->len);
        bs_op->retval = bs_op->len;
        bs_op->version = 1;
        bs_op->callback(bs_op);
    }

    // The op should have finished
    assert(read_retval == (int)read_op->req.rw.len);

    // Check the bitmap (0xffffffff)
    printf("Response bitmap: %08x, expected ffffffff\n", response_bmp);
    assert(response_bmp == 0xffffffff);

    delete read_op;
    printf("test_chained_read_eio_retry passed\n");
}

// Regression test for the bug in osd_flush.cpp where a remote
// stabilize/rollback failure permanently hangs the PG in PG_REPEERING.
bool test_flush_error_pg_repeer(int ctr)
{
    printf("test_flush_error_pg_repeer\n");

    osd_test_fixture_t f;
    f.configure_ec_pool(2, 1);
    f.start(json11::Json::object {
        { "osd_num", 2 },
        { "etcd_address", "127.0.0.1:2379" },
        { "immediate_commit", "all" },
        { "block_size", 131072 },
        { "bitmap_granularity", 4096 },
    });
    f.connect_peer(1);
    f.connect_peer(3);

    // Object exists on all osds but unstable on osd 1 and 3
    inode_t ino = INODE_WITH_POOL(1, 1);
    f.reply_peer_list(1, { { { ino, 0 }, 1 } }, /*stable_count*/ 0);
    f.reply_local_list({ { { ino, 1 }, 1 } }, /*stable_count*/ 1);
    f.reply_peer_list(3, { { { ino, 2 }, 1 } }, /*stable_count*/ 0);
    f.pump();

    auto & pg = f.pg(1, 1);
    assert(pg.state == (PG_ACTIVE|PG_HAS_UNCLEAN));
    auto fb = pg.flush_batch;
    assert(fb != nullptr);
    assert(fb->flush_ops == 2);

    if (ctr == 0)
    {
        // Complete OP_SEC_STAB on osd 1, then fail on osd 3
        auto *stab = f.peer_take(1, OSD_OP_SEC_STABILIZE);
        f.peer_complete(stab, 0);

        assert(pg.state == (PG_ACTIVE|PG_HAS_UNCLEAN));

        stab = f.peer_take(3, OSD_OP_SEC_STABILIZE);
        f.peer_complete(stab, -EPIPE);
    }
    else
    {
        // Fail OP_SEC_STAB on osd 3, then complete on osd 1
        auto *stab = f.peer_take(3, OSD_OP_SEC_STABILIZE);
        f.peer_complete(stab, -EPIPE);

        // The PG should only repeer when the batch is completed
        assert(pg.state == (PG_HAS_UNCLEAN|PG_REPEERING));
        assert(pg.flush_batch == fb);

        stab = f.peer_take(1, OSD_OP_SEC_STABILIZE);
        f.peer_complete(stab, 0);
    }

    // Now the PG should be peering
    assert(pg.state == PG_PEERING);
    assert(!pg.flush_batch);

    // FIXME: Make this test pass without leaks without the following line:
    f.complete_peering_empty();

    printf("test_flush_error_pg_repeer[%d] passed\n", ctr);
    return ctr < 1;
}

// Sync/repeer invariants:
//
// 1. dirty_pgs / dirty_osds are populated only for PGs that were
//    PG_ACTIVE at the moment of the write.
// 2. pg.inflight can only grow while PG is PG_ACTIVE.
//    prepare_primary_rw rejects new ops on non-active PGs.
// 3. When a PG deactivates (repeer / stop), every client that had
//    the PG in its cl->dirty_pgs is dropped synchronously, and
//    the PG is removed from the OSD-wide dirty_pgs.
// 4. A PG in PG_STOPPING or PG_REPEERING waits for inflight to
//    drain to 0 before finish_stop_pg / start_pg_peering fires.
// 5. continue_primary_sync only processes ACTIVE PGs. If a
//    client's cl->dirty_pgs somehow still refers to a non-active
//    PG (violation of (3)), the sync returns -EPIPE and drops
//    the client — the defensive guard.

static void do_lazy_write(osd_test_fixture_t &f, uint64_t client_id,
    inode_t inode, uint64_t offset)
{
    auto *wr = make_write_op(inode, offset, 4096, 0xab);
    wr->client_id = client_id;
    f.exec(wr);
    f.bs_zero_read_ok(0);
    f.bs_write_ok(BS_OP_WRITE_STABLE, 1);
    f.peer_write_ok(2, OSD_OP_SEC_WRITE_STABLE, 1);
}

void test_sync_nothing_to_sync()
{
    printf("test_sync_nothing_to_sync\n");

    osd_test_fixture_t f;
    f.configure_replicated_pool(1, 2, 1, 1, { { 1, 2 } });
    f.start(json11::Json::object {
        { "osd_num", 1 },
        { "etcd_address", "127.0.0.1:2379" },
        { "immediate_commit", "none" },
        { "block_size", 131072 },
        { "bitmap_granularity", 4096 },
    });
    f.connect_peer(2);
    f.complete_peering_empty();
    assert(f.pg(1, 1).state == PG_ACTIVE);

    uint64_t client_id = f.connect_client();

    auto *sync = new osd_op_t();
    sync->op_type = OSD_OP_IN;
    sync->client_id = client_id;
    sync->req.hdr.magic = SECONDARY_OSD_OP_MAGIC;
    sync->req.hdr.id = 42;
    sync->req.hdr.opcode = OSD_OP_SYNC;
    f.exec(sync);

    // No writes have happened => dirty_osds is empty => shortcut taken.
    assert(f.bs->queued.empty());
    assert(f.peer(2)->sent_ops.empty());
    assert(f.pg(1, 1).inflight == 0);
    assert(f.has_client(client_id));
    // The reply must have been outbox_push'd to the client.
    assert(f.client(client_id)->sent_ops.at(sync->req.hdr.id) == sync);
    assert(sync->reply.hdr.retval == 0);
    assert(f.syncs_in_progress_size() == 0);

    printf("test_sync_nothing_to_sync passed\n");
}

void test_sync_happy_path_replicated()
{
    printf("test_sync_happy_path_replicated\n");

    osd_test_fixture_t f;
    f.configure_replicated_pool(1, 2, 1, 1, { { 1, 2 } });
    f.start(json11::Json::object {
        { "osd_num", 1 },
        { "etcd_address", "127.0.0.1:2379" },
        { "immediate_commit", "none" },
        { "block_size", 131072 },
        { "bitmap_granularity", 4096 },
    });
    f.connect_peer(2);
    f.complete_peering_empty();

    inode_t inode = INODE_WITH_POOL(1, 1);
    uint64_t client_id = f.connect_client();

    do_lazy_write(f, client_id, inode, 0);
    assert(f.has_dirty_pg(1, 1));
    assert(f.has_dirty_osd(1));
    assert(f.has_dirty_osd(2));
    assert(f.client(client_id)->dirty_pgs.count({ .pool_id = 1, .pg_num = 1 }) == 1);
    assert(f.pg(1, 1).inflight == 0);   // write finished, no other ops

    auto *sync = new osd_op_t();
    sync->op_type = OSD_OP_IN;
    sync->client_id = client_id;
    sync->req.hdr.magic = SECONDARY_OSD_OP_MAGIC;
    sync->req.hdr.id = 43;
    sync->req.hdr.opcode = OSD_OP_SYNC;
    f.exec(sync);
    // Sync captured the dirty PG: dirty_pgs/dirty_osds emptied,
    // inflight++'d, cl->dirty_pgs cleared by the client-check block.
    assert(!f.has_dirty_pg(1, 1));
    assert(!f.has_dirty_osd(1));
    assert(!f.has_dirty_osd(2));
    assert(f.client(client_id)->dirty_pgs.empty());
    assert(f.pg(1, 1).inflight == 1);

    // One local BS_OP_SYNC and one peer SEC_SYNC.
    auto *local = f.bs->take(BS_OP_SYNC);
    auto *peer_sync = f.peer_take(2, OSD_OP_SEC_SYNC);
    f.peer_complete(peer_sync, 0);
    local->retval = 0;
    local->callback(local);

    // Sync finished: rm_inflight balanced, client alive, reply delivered.
    assert(f.pg(1, 1).inflight == 0);
    assert(f.has_client(client_id));
    assert(f.client(client_id)->sent_ops.at(sync->req.hdr.id) == sync);
    assert(sync->reply.hdr.retval == 0);
    assert(f.syncs_in_progress_size() == 0);

    printf("test_sync_happy_path_replicated passed\n");
}

// Invariant (3) via repeer without parallel inflight
void test_pg_repeer_drops_dirty_client()
{
    printf("test_pg_repeer_drops_dirty_client\n");

    osd_test_fixture_t f;
    f.configure_replicated_pool(1, 2, 1, 1, { { 1, 2 } });
    f.start(json11::Json::object {
        { "osd_num", 1 },
        { "etcd_address", "127.0.0.1:2379" },
        { "immediate_commit", "none" },
        { "block_size", 131072 },
        { "bitmap_granularity", 4096 },
    });
    f.connect_peer(2);
    f.complete_peering_empty();

    inode_t inode = INODE_WITH_POOL(1, 1);
    uint64_t client_id = f.connect_client();
    do_lazy_write(f, client_id, inode, 0);
    assert(f.has_client(client_id));
    assert(f.has_dirty_pg(1, 1));

    // Peer disconnect -> repeer_pgs -> repeer_pg(1,1). Since inflight
    // is 0, can_repeer() is true and PG immediately goes back to
    // PG_PEERING via start_pg_peering. drop_dirty_pg_connections is
    // called first and drops the client.
    f.disconnect_peer(2);

    assert(!f.has_client(client_id));
    assert(!f.has_dirty_pg(1, 1));
    // start_pg_peering -> PG_PEERING (peer no longer connected, may go
    // to PG_INCOMPLETE via cur_size < minsize check, either way not ACTIVE).
    assert(!(f.pg(1, 1).state & PG_ACTIVE));

    // FIXME: make it pass without leaks with:
    f.complete_peering_empty();

    printf("test_pg_repeer_drops_dirty_client passed\n");
}

// Invariant (3) via repeer with parallel inflight
void test_sync_replica_gone()
{
    printf("test_sync_replica_gone\n");

    osd_test_fixture_t f;
    f.configure_replicated_pool(/*pool_id*/ 1, /*pg_size*/ 2, /*pg_minsize*/ 1, /*pg_count*/ 1,
        { { 1, 2 } });
    f.start(json11::Json::object {
        { "osd_num", 1 },
        { "etcd_address", "127.0.0.1:2379" },
        { "immediate_commit", "none" }, // lazy writes
        { "block_size", 131072 },
        { "bitmap_granularity", 4096 },
    });
    f.connect_peer(2);
    f.complete_peering_empty();
    assert(f.pg(1, 1).state & PG_ACTIVE);

    inode_t inode = INODE_WITH_POOL(1, 1);

    uint64_t client_id = f.connect_client();

    // Client performs a lazy unsynced write
    auto *wr = make_write_op(inode, 0, 4096, 0xab);
    wr->client_id = client_id;
    f.exec(wr);
    {
        // stage 1: zero-length version-resolving read
        auto *zr = f.bs->take(BS_OP_READ);
        assert(zr->len == 0);
        zr->version = 0;
        zr->retval = 0;
        zr->callback(zr);
    }
    {
        // stage 2: local write + replica(OSD 2) write, both acked
        auto *lw = f.bs->take(BS_OP_WRITE_STABLE);
        auto *pw = f.peer_take(2, OSD_OP_SEC_WRITE_STABLE);
        lw->retval = lw->len;
        lw->callback(lw);
        pw->reply.sec_rw.version = 1;
        f.peer_complete(pw, pw->req.sec_rw.len);
    }

    // A concurrent read on the same PG stays in flight, holding inflight
    auto *rd = new osd_op_t();
    rd->op_type = OSD_OP_IN;
    rd->client_id = 0; // SELF_CLIENT: completes via callback, just here to hold inflight
    rd->req.rw.header.magic = SECONDARY_OSD_OP_MAGIC;
    rd->req.rw.header.id = 2;
    rd->req.rw.header.opcode = OSD_OP_READ;
    rd->req.rw.inode = inode;
    rd->req.rw.offset = 0;
    rd->req.rw.len = 4096;
    int rd_retval = -1;
    rd->callback = [&rd_retval](osd_op_t *op) { rd_retval = op->reply.hdr.retval; };
    f.exec(rd);
    auto *rd_bs = f.bs->take(BS_OP_READ); // captured but intentionally NOT completed yet
    assert(f.pg(1, 1).inflight == 1);

    // Disconnect OSD 2 => client should be dropped
    f.disconnect_peer(2);
    assert((f.pg(1, 1).state & (PG_ACTIVE|PG_REPEERING)) == PG_REPEERING);
    assert(!f.has_client(client_id));

    // Release inflight
    rd_bs->retval = rd_bs->len;
    rd_bs->version = 1;
    rd_bs->callback(rd_bs);
    assert(rd_retval == 4096);

    f.complete_peering_empty();

    delete rd;

    printf("test_sync_replica_gone passed\n");
}

// Sync interference with EC osd_flush
void test_sync_interference()
{
    printf("test_sync_interference\n");

    osd_test_fixture_t f;

    // EC 2+1 pool, 2 PGs with OSD 1 primary
    f.st_cli->set("/vitastor/config/pools", json11::Json::object {
        { "1", json11::Json::object {
            { "name", "pool_1" },
            { "scheme", "ec" },
            { "pg_size", 3 },
            { "pg_minsize", 2 },
            { "parity_chunks", 1 },
            { "pg_count", 2 },
            { "failure_domain", "osd" },
            { "immediate_commit", "none" },
        } },
    });
    f.st_cli->set("/vitastor/pg/config", json11::Json::object {
        { "items", json11::Json::object{
            { "1", json11::Json::object {
                { "1", json11::Json::object {
                    { "osd_set", json11::Json::array{ 1, 2, 3 } },
                    { "primary", "1" },
                } },
                { "2", json11::Json::object {
                    { "osd_set", json11::Json::array{ 1, 4, 5 } },
                    { "primary", 1 },
                } },
            } }
        } },
    });

    f.start(json11::Json::object {
        { "osd_num", 1 },
        { "etcd_address", "127.0.0.1:2379" },
        { "immediate_commit", "none" },
        { "block_size", 131072 },
        { "bitmap_granularity", 4096 },
    });

    // Connect peers 2..5
    for (int peer = 2; peer <= 5; peer++)
        f.connect_peer(peer);
    f.complete_peering_empty();
    f.ringloop->loop();
    assert(f.pg(1, 1).state == PG_ACTIVE);
    assert(f.pg(1, 2).state == PG_ACTIVE);

    inode_t inode = INODE_WITH_POOL(1, 1);

    auto bs_ok = [&](uint64_t op)
    {
        auto *wr = f.bs->take(op);
        wr->retval = 0;
        wr->callback(wr);
    };
    auto wr_ok = [&](osd_num_t peer, uint64_t version)
    {
        auto *pw = f.peer_take(peer, OSD_OP_SEC_WRITE);
        pw->reply.sec_rw.version = version;
        f.peer_complete(pw, pw->req.sec_rw.len);
    };

    // Two clients - one writes to PG 1, another to PG 2
    uint64_t client1 = f.connect_client();
    uint64_t client2 = f.connect_client();
    {
        auto *wr = make_write_op(inode, 0, 256*1024, 0xab);
        wr->client_id = client1;
        f.exec(wr);
        f.bs_zero_read_ok(0);
        f.bs_write_ok(BS_OP_WRITE, 1);
        wr_ok(2, 1);
        wr_ok(3, 1);
        assert(wr == f.client(client1)->sent_ops.at(wr->req.rw.header.id));
    }
    {
        auto *wr = make_write_op(inode, 256*1024, 256*1024, 0xac);
        wr->client_id = client2;
        f.exec(wr);
        f.bs_zero_read_ok(0);
        f.bs_write_ok(BS_OP_WRITE, 1);
        wr_ok(4, 1);
        wr_ok(5, 1);
        assert(wr == f.client(client2)->sent_ops.at(wr->req.rw.header.id));
    }

    // Try sync from client1
    auto *sync = new osd_op_t();
    sync->op_type = OSD_OP_IN;
    sync->client_id = client1;
    sync->req.hdr.magic = SECONDARY_OSD_OP_MAGIC;
    sync->req.hdr.id = 2;
    sync->req.hdr.opcode = OSD_OP_SYNC;
    f.exec(sync);

    // Check sync ops are present
    f.peer_take(2, OSD_OP_SEC_SYNC);
    for (osd_num_t p = 3; p <= 5; p++)
    {
        auto *ps = f.peer_take(p, OSD_OP_SEC_SYNC);
        f.peer_complete(ps, 0);
    }
    bs_ok(BS_OP_SYNC);

    // Disconnect OSD 2 => client 1 should be dropped, but not client 2
    f.disconnect_peer(2);
    assert((f.pg(1, 1).state & (PG_ACTIVE|PG_PEERING|PG_REPEERING)) == PG_PEERING);
    assert((f.pg(1, 2).state & (PG_ACTIVE|PG_PEERING|PG_REPEERING)) == PG_ACTIVE);
    assert(!f.has_client(client1));
    assert(f.has_client(client2));
    sync = NULL;

    f.complete_peering_empty();
    assert((f.pg(1, 1).state & (PG_ACTIVE|PG_PEERING|PG_REPEERING)) == PG_ACTIVE);

    // Now again sync from client 2
    sync = new osd_op_t();
    sync->op_type = OSD_OP_IN;
    sync->client_id = client2;
    sync->req.hdr.magic = SECONDARY_OSD_OP_MAGIC;
    sync->req.hdr.id = 2;
    sync->req.hdr.opcode = OSD_OP_SYNC;
    f.exec(sync);
    bs_ok(BS_OP_SYNC);
    for (osd_num_t p = 3; p <= 5; p++)
    {
        auto *ps = f.peer_take(p, OSD_OP_SEC_SYNC);
        f.peer_complete(ps, 0);
    }
    bs_ok(BS_OP_STABLE);
    // OSD 3 is not in unstable list because PG 1 is repeered again
    for (osd_num_t p = 4; p <= 5; p++)
    {
        auto *ps = f.peer_take(p, OSD_OP_SEC_STABILIZE);
        f.peer_complete(ps, 0);
    }
    assert(f.client(client2)->sent_ops.at(sync->req.hdr.id) == sync);

    printf("test_sync_interference passed\n");
}

// Invariant (3) via stop_pg
void test_pg_stop_drops_dirty_client()
{
    printf("test_pg_stop_drops_dirty_client\n");

    osd_test_fixture_t f;
    // Two PGs so that stopping PG 1 does not also nuke the whole
    // OSD's PG map — we still want to inspect PG 2 for sanity.
    f.configure_replicated_pool(1, 2, 1, 2, { { 1, 2 }, { 1, 2 } });
    f.start(json11::Json::object {
        { "osd_num", 1 },
        { "etcd_address", "127.0.0.1:2379" },
        { "immediate_commit", "none" },
        { "block_size", 131072 },
        { "bitmap_granularity", 4096 },
    });
    f.connect_peer(2);
    f.complete_peering_empty();

    inode_t inode = INODE_WITH_POOL(1, 1);
    uint64_t client_id = f.connect_client();
    do_lazy_write(f, client_id, inode, 0);
    assert(f.has_client(client_id));
    assert(f.has_dirty_pg(1, 1));

    // Move PG 1 primary to OSD 2
    f.st_cli->set("/vitastor/pg/config", json11::Json::object {
        { "items", json11::Json::object{
            { "1", json11::Json::object {
                { "1", json11::Json::object {
                    { "osd_set", json11::Json::array { 1, 2 } },
                    { "primary", 2 },
                } },
                { "2", json11::Json::object {
                    { "osd_set", json11::Json::array { 1, 2 } },
                    { "primary", 1 },
                } },
            } }
        } },
    });
    f.ringloop->loop();

    // Client dropped, dirty_pgs cleared, PG walked all the way to
    // OFFLINE and (unpaused etcd mock) got erased.
    assert(!f.has_client(client_id));
    assert(!f.has_dirty_pg(1, 1));
    // Depending on whether apply_pg_config re-takes it synchronously,
    // PG 1 is either fully gone or back in PG_PEERING/PG_STARTING.
    // Either way, not PG_STOPPING.
    if (f.has_pg(1, 1))
        assert(!(f.pg(1, 1).state & (PG_STOPPING | PG_OFFLINE)));
    assert(f.has_pg(1, 2) && (f.pg(1, 2).state & PG_ACTIVE));

    printf("test_pg_stop_drops_dirty_client passed\n");
}

// A second sync arriving while the first is in flight must wait for the first one,
// then complete as a noop because nothing was written between the two
void test_sync_queued_and_processed_in_order()
{
    printf("test_sync_queued_and_processed_in_order\n");

    osd_test_fixture_t f;
    f.configure_replicated_pool(1, 2, 1, 1, { { 1, 2 } });
    f.start(json11::Json::object {
        { "osd_num", 1 },
        { "etcd_address", "127.0.0.1:2379" },
        { "immediate_commit", "none" },
        { "block_size", 131072 },
        { "bitmap_granularity", 4096 },
    });
    f.connect_peer(2);
    f.complete_peering_empty();

    inode_t inode = INODE_WITH_POOL(1, 1);
    uint64_t client_id = f.connect_client();
    do_lazy_write(f, client_id, inode, 0);

    // Both syncs are SELF_CLIENT to complete via callbacks
    auto *sync1 = new osd_op_t();
    sync1->op_type = OSD_OP_IN;
    sync1->client_id = 0;   // SELF_CLIENT
    sync1->req.hdr.magic = SECONDARY_OSD_OP_MAGIC;
    sync1->req.hdr.id = 46;
    sync1->req.hdr.opcode = OSD_OP_SYNC;
    int r1 = -1;
    sync1->callback = [&r1](osd_op_t *op) { r1 = op->reply.hdr.retval; };
    f.exec(sync1);
    // Sync #1 captured the dirty PG and submitted its sub-syncs.
    assert(f.syncs_in_progress_size() == 1);
    auto *local1 = f.bs->take(BS_OP_SYNC);
    auto *peer1 = f.peer_take(2, OSD_OP_SEC_SYNC);

    auto *sync2 = new osd_op_t();
    sync2->op_type = OSD_OP_IN;
    sync2->client_id = 0;   // SELF_CLIENT
    sync2->req.hdr.magic = SECONDARY_OSD_OP_MAGIC;
    sync2->req.hdr.id = 47;
    sync2->req.hdr.opcode = OSD_OP_SYNC;
    int r2 = -1;
    sync2->callback = [&r2](osd_op_t *op) { r2 = op->reply.hdr.retval; };
    f.exec(sync2);
    // Sync 2 must be queued behind 1 — no new subops.
    assert(f.syncs_in_progress_size() == 2);
    assert(f.bs->queued.empty());
    assert(f.peer(2)->sent_ops.size() == 1); // still just sync #1's peer op

    // Finish sync 1. Its continuation should continue sync 2.
    f.peer_complete(peer1, 0);
    local1->retval = 0;
    local1->callback(local1);

    assert(r1 == 0);
    assert(r2 == 0);
    assert(f.pg(1, 1).inflight == 0);
    assert(f.syncs_in_progress_size() == 0);
    // Sync 2 did not fire any new subops.
    assert(f.bs->queued.empty());
    assert(f.peer(2)->sent_ops.empty());

    delete sync2;
    delete sync1;

    printf("test_sync_queued_and_processed_in_order passed\n");
}

// PG must wait for inflight to drain before stopping
void test_pg_stop_waits_for_inflight()
{
    printf("test_pg_stop_waits_for_inflight\n");

    osd_test_fixture_t f;
    f.configure_replicated_pool(1, 2, 1, 1, { { 1, 2 } });
    f.start(json11::Json::object {
        { "osd_num", 1 },
        { "etcd_address", "127.0.0.1:2379" },
        { "immediate_commit", "none" },
        { "block_size", 131072 },
        { "bitmap_granularity", 4096 },
    });
    f.connect_peer(2);
    f.complete_peering_empty();

    inode_t inode = INODE_WITH_POOL(1, 1);
    uint64_t client_id = f.connect_client();
    auto *wr = make_write_op(inode, 0, 4096, 0xab);
    wr->client_id = client_id;
    f.exec(wr);
    f.bs_zero_read_ok(0);
    // Local + peer stable writes now sit in the queues — DON'T complete
    // them, so inflight stays at 1.
    auto *local = f.bs->take(BS_OP_WRITE_STABLE);
    auto *peer_wr = f.peer_take(2, OSD_OP_SEC_WRITE_STABLE);
    assert(f.pg(1, 1).inflight >= 1);

    // Move PG 1 primary to OSD 2
    f.st_cli->set("/vitastor/pg/config", json11::Json::object {
        { "items", json11::Json::object{
            { "1", json11::Json::object {
                { "1", json11::Json::object {
                    { "osd_set", json11::Json::array { 1, 2 } },
                    { "primary", 2 },
                } },
            } }
        } },
    });

    // Held inflight blocks finish_stop_pg.
    assert(f.pg(1, 1).state & PG_STOPPING);
    assert(!(f.pg(1, 1).state & PG_OFFLINE));
    assert(f.pg(1, 1).inflight >= 1);
    assert(f.has_pg(1, 1));

    // Complete the sub-writes. continue_primary_write will notice PG
    // is no longer ACTIVE (line "if (!(pg.state & PG_ACTIVE))" after
    // resume_4), cancel the write with -EPIPE, and rm_inflight'ing
    // will finally trigger finish_stop_pg.
    local->retval = local->len;
    local->callback(local);
    peer_wr->reply.sec_rw.version = 1;
    f.peer_complete(peer_wr, peer_wr->req.sec_rw.len);

    assert(!f.has_pg(1, 1) || f.pg(1, 1).inflight == 0);
    // If apply_pg_config didn't retake it, it's either OFFLINE-then-
    // erased or PG_PEERING again. Never PG_STOPPING now.
    if (f.has_pg(1, 1))
        assert(!(f.pg(1, 1).state & PG_STOPPING));

    printf("test_pg_stop_waits_for_inflight passed\n");
}

// Reject writes on non-active PGs
void test_no_new_write_on_non_active_pg()
{
    printf("test_no_new_write_on_non_active_pg\n");

    osd_test_fixture_t f;
    f.configure_replicated_pool(1, 2, 1, 1, { { 1, 2 } });
    f.start(json11::Json::object {
        { "osd_num", 1 },
        { "etcd_address", "127.0.0.1:2379" },
        { "immediate_commit", "none" },
        { "block_size", 131072 },
        { "bitmap_granularity", 4096 },
    });
    f.connect_peer(2);

    inode_t inode = INODE_WITH_POOL(1, 1);
    uint64_t client_id = f.connect_client();

    assert(!(f.pg(1, 1).state & PG_ACTIVE));

    auto *wr = make_write_op(inode, 0, 4096, 0xab);
    wr->client_id = client_id;
    f.exec(wr);

    assert(!f.bs->take(BS_OP_WRITE, false));
    assert(!f.bs->take(BS_OP_WRITE_STABLE, false));
    // Reply is in the client's outbox with retval = -EPIPE.
    assert(f.client(client_id)->sent_ops.at(wr->req.rw.header.id) == wr);
    assert(wr->reply.hdr.retval == -EPIPE);
    // inflight untouched.
    assert(f.pg(1, 1).inflight == 0);

    f.complete_peering_empty();

    printf("test_no_new_write_on_non_active_pg passed\n");
}

// Writes must pause until a bumped PG epoch is reported to etcd
void test_pg_epoch_bump_blocks_write_until_reported()
{
    printf("test_pg_epoch_bump_blocks_write_until_reported\n");

    osd_test_fixture_t f;
    f.configure_replicated_pool(/*pool_id*/ 1, /*pg_size*/ 2, /*pg_minsize*/ 1, /*pg_count*/ 1,
        { { 1, 2 } });
    f.start(json11::Json::object {
        { "osd_num", 1 },
        { "etcd_address", "127.0.0.1:2379" },
        { "immediate_commit", "none" },
        { "block_size", 131072 },
        { "bitmap_granularity", 4096 },
        { "no_recovery", true },
    });

    // Bring PG to ACTIVE with epoch = 1, reported_epoch = 0.
    f.connect_peer(2);
    f.reply_local_list({ { { INODE_WITH_POOL(1, 1), 0x20000 }, 1 } }, 1);
    f.reply_peer_list(2, {}, 0);
    f.ringloop->loop();
    assert(f.pg(1, 1).state & PG_ACTIVE);
    assert(f.pg(1, 1).epoch == 1);
    assert(f.pg(1, 1).reported_epoch == 0);

    // Pause etcd mock so report_pg_states() never completes
    f.st_cli->pause();

    inode_t inode = INODE_WITH_POOL(1, 1);

    auto *wr = make_write_op(inode, 0, 4096, 0xab);
    int r1 = -1;
    wr->callback = [&](osd_op_t *op) { r1 = op->reply.hdr.retval; };
    f.exec(wr);
    f.bs_zero_read_ok(0);

    // The write should stop at epoch bump
    assert(!f.st_cli->queue.empty());
    assert(!f.peer(2)->sent_ops.size());
    assert(!f.bs->queued.size());

    // Send a second write - it shouldn't make OSD try to report PG state
    // the second time after finishing the first report
    auto *wr2 = make_write_op(inode, 0x40000, 4096, 0xab);
    int r2 = -1;
    wr2->callback = [&](osd_op_t *op) { r2 = op->reply.hdr.retval; };
    f.exec(wr2);
    f.bs_zero_read_ok(0);

    f.st_cli->resume(1);
    assert(f.st_cli->queue.empty());
    f.st_cli->resume();
    f.ringloop->loop();

    assert(f.pg(1, 1).reported_epoch == 1);
    assert(f.pg(1, 1).state & PG_ACTIVE);

    // Both writes should resume
    assert(f.bs->queued.size() == 2);
    assert(f.peer(2)->sent_ops.size() == 2);

    for (int i = 0; i < 2; i++)
    {
        auto *local_write = f.bs->take();
        assert(local_write->opcode == BS_OP_WRITE_STABLE);
        assert(local_write->len == 4096);
        assert(local_write->version == 0x10001);
        local_write->retval = local_write->len;
        local_write->callback(local_write);

        auto *peer_write = f.peer_take(2, OSD_OP_SEC_WRITE_STABLE);
        assert(peer_write->req.hdr.opcode == OSD_OP_SEC_WRITE_STABLE);
        assert(peer_write->req.sec_rw.version == 0x10001);
        peer_write->reply.sec_rw.version = 0x10001;
        f.peer_complete(peer_write, peer_write->req.sec_rw.len);
    }

    assert(f.pg(1, 1).inflight == 0);
    assert(f.pg(1, 1).write_queue.empty());

    assert(r1 == 4096 && r2 == 4096);
    delete wr;
    delete wr2;

    printf("test_pg_epoch_bump_blocks_write_until_reported passed\n");
}

static osd_op_t *make_self_rw(uint64_t opcode, inode_t inode, uint64_t offset, uint64_t len)
{
    auto *op = new osd_op_t();
    op->op_type = OSD_OP_IN;
    op->client_id = 0;
    op->req.rw.header.magic = SECONDARY_OSD_OP_MAGIC;
    op->req.rw.header.id = 100 + opcode;
    op->req.rw.header.opcode = opcode;
    op->req.rw.inode = inode;
    op->req.rw.offset = offset;
    op->req.rw.len = len;
    return op;
}

static bool maybe_complete_good_sec_read(osd_test_fixture_t &f, osd_num_t peer,
    const uint8_t *data, const uint8_t *bitmap, uint32_t chunk_size)
{
    auto *cl = f.peer(peer);
    for (auto &kv: cl->sent_ops)
    {
        auto *op = kv.second;
        if (op->req.hdr.opcode != OSD_OP_SEC_READ)
            continue;
        assert(op->iov.count == 1);
        assert(op->req.sec_rw.len == chunk_size);
        memcpy(op->iov.buf[0].iov_base, data, chunk_size);
        assert(op->bitmap);
        memcpy(op->bitmap, bitmap, chunk_size/4096/8);
        op->reply.sec_rw.attr_len = chunk_size/4096/8;
        op->reply.sec_rw.version = 1;
        f.peer_complete(op, chunk_size);
        return true;
    }
    return false;
}

void test_ec42_inconsistent_read()
{
    const uint32_t chunk_size = 128*1024;
    const uint32_t bitmap_size = 4;
    printf("test_ec42_inconsistent_read\n");

    osd_test_fixture_t f;
    f.configure_ec_pool(4, 2);
    f.start(json11::Json::object {
        { "osd_num", 2 },
        { "etcd_address", "127.0.0.1:2379" },
        { "immediate_commit", "none" },
        { "block_size", (uint64_t)chunk_size },
        { "bitmap_granularity", 4096 },
        { "no_recovery", true },
    });

    for (int peer = 1; peer <= 6; peer++)
        if (peer != 2)
            f.connect_peer(peer);

    inode_t inode = INODE_WITH_POOL(1, 1);
    object_id base_oid = { inode, 0 };

    // Role 3 is absent, but its OSD remains connected and stays in pg.cur_set.
    f.reply_peer_list(1, { { { inode, 0 }, 1 } }, 1);
    f.reply_local_list({ { { inode, 1 }, 1 } }, 1);
    f.reply_peer_list(3, { { { inode, 2 }, 1 } }, 1);
    f.reply_peer_list(4, {}, 0);
    f.reply_peer_list(5, { { { inode, 4 }, 1 } }, 1);
    f.reply_peer_list(6, { { { inode, 5 }, 1 } }, 1);
    f.ringloop->loop();
    assert(f.pg(1, 1).state & PG_ACTIVE);

    // Build a valid 4+2 codeword. D3 is deliberately nonzero: a correct
    // read of its range must return 0xa5, not a zero-filled sparse response.
    std::vector<uint8_t> chunks(6 * chunk_size);
    std::vector<uint8_t> bitmaps(6 * bitmap_size, 0xff);
    memset(chunks.data() + 0*chunk_size, 0xa1, chunk_size);
    memset(chunks.data() + 1*chunk_size, 0xb2, chunk_size);
    memset(chunks.data() + 2*chunk_size, 0xc3, chunk_size);
    memset(chunks.data() + 3*chunk_size, 0xd4, chunk_size);
    osd_num_t all_roles[6] = { 1, 2, 3, 4, 5, 6 };
    osd_rmw_stripe_t stripes[6] = {};
    for (int i = 0; i < 6; i++)
        stripes[i].bmp_buf = bitmaps.data() + i*bitmap_size;
    use_ec(6, 4, true);
    split_stripes(4, chunk_size, 0, 4*chunk_size, stripes);
    void *rmw_buf = calc_rmw(chunks.data(), stripes, all_roles, 6, 4, 6, all_roles, chunk_size, bitmap_size);
    assert(rmw_buf);
    calc_rmw_parity_ec(stripes, 6, 4, all_roles, all_roles, chunk_size, bitmap_size);
    use_ec(6, 4, false);
    memcpy(chunks.data() + 4*chunk_size, (uint8_t*)rmw_buf + 0*chunk_size, chunk_size);
    memcpy(chunks.data() + 5*chunk_size, (uint8_t*)rmw_buf + 1*chunk_size, chunk_size);
    free(rmw_buf);

    // Scrub sees one missing data role and one bad survivor. The other four
    // survivor chunks are a source subset with no matching extra chunk.
    chunks[4*chunk_size] ^= 1;
    auto *scrub = make_self_rw(OSD_OP_SCRUB, inode, 0, 0);
    int scrub_retval = -1;
    scrub->callback = [&scrub_retval](osd_op_t *op) { scrub_retval = op->reply.hdr.retval; };
    f.exec(scrub);

    auto reply_reads = [&](bool read4, bool read6)
    {
        auto *local_read = f.bs->take(BS_OP_READ);
        assert(local_read->len == chunk_size);
        memcpy(local_read->buf, chunks.data() + 1*chunk_size, chunk_size);
        assert(local_read->bitmap);
        memcpy(local_read->bitmap, bitmaps.data() + 1*bitmap_size, bitmap_size);
        local_read->retval = chunk_size;
        local_read->version = 1;
        local_read->callback(local_read);

        assert(maybe_complete_good_sec_read(f, 1, chunks.data() + 0*chunk_size, bitmaps.data() + 0*bitmap_size, chunk_size));
        assert(maybe_complete_good_sec_read(f, 3, chunks.data() + 2*chunk_size, bitmaps.data() + 2*bitmap_size, chunk_size));
        if (f.peer(4)->sent_ops.size())
        {
            assert(read4);
            auto *read_missing = f.peer_take(4, OSD_OP_SEC_READ);
            f.peer_complete(read_missing, -ENOENT);
        }
        assert(maybe_complete_good_sec_read(f, 5, chunks.data() + 4*chunk_size, bitmaps.data() + 4*bitmap_size, chunk_size));
        assert(maybe_complete_good_sec_read(f, 6, chunks.data() + 5*chunk_size, bitmaps.data() + 5*bitmap_size, chunk_size) || !read6);
    };
    reply_reads(false, true);

    assert(scrub_retval == 0);
    assert(f.pg(1, 1).state & PG_HAS_INCONSISTENT);
    assert(f.pg(1, 1).inconsistent_objects.count(base_oid));
    delete scrub;
    chunks[4*chunk_size] ^= 1;

    auto *read = make_self_rw(OSD_OP_READ, inode, 3*chunk_size, chunk_size);
    int read_retval = -1;
    read->callback = [&read_retval](osd_op_t *op) { read_retval = op->reply.hdr.retval; };
    f.exec(read);

    reply_reads(true, false);

    uint8_t *b = (uint8_t*)read->buf;
    printf("read %02x %02x %02x %02x %02x\n", b[0], b[chunk_size], b[2*chunk_size], b[3*chunk_size], b[4*chunk_size]);

    printf("read retval=%d first_byte=%02x\n", read_retval,
        read->iov.count >= 1 ? ((uint8_t*)read->iov.buf[read->iov.count-1].iov_base)[0] : 0);
    assert(read_retval == chunk_size);
    assert(read->iov.count >= 1); // bitmap+data
    assert(read->iov.buf[read->iov.count-1].iov_len == chunk_size);
    assert(memcmp(read->iov.buf[read->iov.count-1].iov_base, chunks.data()+3*chunk_size, chunk_size) == 0);

    delete read;
    printf("test_ec42_inconsistent_read passed\n");
}

int main(int narg, char *args[])
{
    test_load_global_config();
    test_replicated_write();
    test_scrub_corruption_persists();
    test_ec33_scrub_only_first_part();
    for (int i = 0; test_scrub_same_data_diff_bitmaps(i); i++) {}
    test_ec33_recovery_missing_first_part();
    test_ec42_write_parityless();
    test_ec33_chain_read_phantom_bitmap_source();
    test_chained_read_eio_retry();
    for (int i = 0; test_flush_error_pg_repeer(i); i++) {}
    test_sync_nothing_to_sync();
    test_sync_happy_path_replicated();
    test_pg_repeer_drops_dirty_client();
    test_sync_replica_gone();
    test_sync_interference();
    test_pg_stop_drops_dirty_client();
    test_sync_queued_and_processed_in_order();
    test_pg_stop_waits_for_inflight();
    test_no_new_write_on_non_active_pg();
    test_pg_epoch_bump_blocks_write_until_reported();
    test_ec42_inconsistent_read();
    return 0;
}
