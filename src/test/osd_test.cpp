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

// Find the object's current pg_osd_set_state_t in whatever "not clean" index
// the PG keeps it in. NULL means the object is considered clean.
static pg_osd_set_state_t *find_object_state(pg_t & pg, object_id oid)
{
    for (auto *idx: { &pg.inconsistent_objects, &pg.incomplete_objects,
        &pg.degraded_objects, &pg.misplaced_objects })
    {
        auto it = idx->find(oid);
        if (it != idx->end())
            return it->second;
    }
    return NULL;
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
        uint8_t *rmw_buf = (uint8_t*)calc_rmw(data_buf.data(), stripes, fake_osd_set, 6, 3, 6, fake_osd_set, 128*1024, 4);
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

void test_chained_read_large_chain_info()
{
    printf("test_chained_read_large_chain_info\n");

    osd_test_fixture_t f;
    f.configure_replicated_pool(1, 2, 1, 1, { { 1, 2 } });

    // Build 300-layer snapshot chain in pool 1:
    // Inode 1 (root) <- Inode 2 <- ... <- Inode 300 (leaf)
    const int NUM_SNAPS = 300;
    for (int i = 1; i <= NUM_SNAPS; i++)
    {
        json11::Json::object obj;
        if (i > 1)
            obj["parent_id"] = i - 1;
        f.st_cli->set("/vitastor/config/inode/1/" + std::to_string(i), obj);
    }

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

    inode_t leaf_inode = INODE_WITH_POOL(1, NUM_SNAPS);
    uint64_t meta_rev = f.st_cli->inode_config.at(leaf_inode).mod_revision;

    auto *read_op = new osd_op_t();
    read_op->op_type = OSD_OP_IN;
    read_op->client_id = client_id;
    read_op->req.rw.header.magic = SECONDARY_OSD_OP_MAGIC;
    read_op->req.rw.header.id = 1;
    read_op->req.rw.header.opcode = OSD_OP_READ;
    read_op->req.rw.inode = leaf_inode;
    read_op->req.rw.offset = 0;
    read_op->req.rw.len = 128 * 1024;
    read_op->req.rw.meta_revision = meta_rev;
    read_op->req.rw.flags = OSD_OP_RETURN_CHAIN;

    f.exec(read_op);

    // Verify op completed and replied to client
    assert(f.client(client_id)->sent_ops.count(read_op->req.hdr.id) == 1);
    auto *reply_op = f.client(client_id)->sent_ops.at(read_op->req.hdr.id);
    assert(reply_op == read_op);
    assert(reply_op->reply.hdr.retval == (int64_t)read_op->req.rw.len);

    // Replicated pool clean_entry_bitmap_size = 131072 / 4096 / 8 = 4 bytes
    // For chain_size = 300, key_index_bytes = osd_op_rw_t::chain_info_bytes(300) = 2
    // 32 blocks (128k / 4k) * 2 bytes = 64 bytes of chain_info
    // Total bitmap_len must be 4 + 64 = 68 bytes.
    // If osd_primary_chain.cpp missed multiplying key_index_bytes, bitmap_len would be 4 + 32 = 36.
    assert(reply_op->reply.rw.bitmap_len == 68);

    printf("test_chained_read_large_chain_info passed\n");
}


// A degraded EC read may leave its parity read ranges behind when the object state
// changes under the operation (recovery brings a chunk back while subops are in flight)
// and a failed subop retries the whole read. The retry then takes the fast path, which
// only assigns buffers for data stripes - a parity subop sent on a leftover range would
// write its reply into a freed buffer. The retry has to reset the read ranges.
void test_ec_degraded_read_retry_resets_parity()
{
    printf("test_ec_degraded_read_retry_resets_parity\n");

    osd_test_fixture_t f;
    // EC 2+2, set {1,2,3,4}, primary = OSD 2. OSD 1 (role 0) holds nothing, so the
    // object is degraded: it only exists at roles 1(local), 2 and 3
    f.configure_ec_pool(2, 2, /*primary_osd*/ 2);
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
        { "no_recovery", true },
    });
    inode_t inode = INODE_WITH_POOL(1, 1);

    f.connect_peer(1);
    f.connect_peer(3);
    f.connect_peer(4);
    // OSD 1 holds nothing -> the object is missing at role 0
    f.reply_peer_list(1, {}, 0);
    f.reply_peer_list(3, { { { inode, 2 }, 1 } }, 1);
    f.reply_peer_list(4, { { { inode, 3 }, 1 } }, 1);
    f.reply_local_list({ { { inode, 1 }, 1 } }, 1);
    f.ringloop->loop();
    assert(f.pg(1, 1).state == (PG_ACTIVE|PG_HAS_DEGRADED));

    // Read 0..32k. It lies in role 0, which is missing, so the attempt is degraded:
    // extend_missing_stripes() moves the read to role 1 (local) + role 2 (parity, OSD 3)
    auto *read_op = new osd_op_t();
    read_op->op_type = OSD_OP_IN;
    read_op->client_id = 0;
    read_op->req.rw.header.magic = SECONDARY_OSD_OP_MAGIC;
    read_op->req.rw.header.id = 1;
    read_op->req.rw.header.opcode = OSD_OP_READ;
    read_op->req.rw.inode = inode;
    read_op->req.rw.offset = 0;
    read_op->req.rw.len = 32768;
    int read_retval = -1;
    read_op->callback = [&](osd_op_t *op) { read_retval = op->reply.hdr.retval; };
    f.exec(read_op);

    // Attempt 1: local read of role 1 and a parity read from OSD 3
    auto *local_read = f.bs->take(BS_OP_READ);
    assert(local_read->offset == 0 && local_read->len == 32768);
    auto *parity_read = f.peer_take(3, OSD_OP_SEC_READ);
    assert(parity_read->req.sec_rw.oid.stripe == 2);
    assert(parity_read->req.sec_rw.offset == 0 && parity_read->req.sec_rw.len == 32768);

    // While the subops are in flight a peering round recomputes the object state
    // and finds it complete again: the PG goes to exactly PG_ACTIVE and the parity
    // range of the degraded attempt becomes stale. Peering itself won't run while
    // an op is inflight, but the object state can be replaced under a running op
    // by any parallel event - drive the resulting state directly
    f.pg(1, 1).state = PG_ACTIVE;

    // The local read completes, then the parity read fails with -EIO. The error
    // handler sees that the object state changed under the op and retries the read
    local_read->version = 1;
    local_read->retval = local_read->len;
    local_read->callback(local_read);
    f.peer_complete(parity_read, -EIO);

    // The retry takes the fast path now. It must not re-send the leftover parity
    // range - there is no read buffer for it anymore
    assert(!f.peer_has_sent_op(3, OSD_OP_SEC_READ));
    assert(!f.peer_has_sent_op(4, OSD_OP_SEC_READ));

    // The retry reads the recovered chunk normally and the op completes.
    // role 1 is not read this time - it was only pulled in to reconstruct role 0
    auto *recovered_read = f.peer_take(1, OSD_OP_SEC_READ);
    assert(recovered_read->req.sec_rw.oid.stripe == 0);
    assert(recovered_read->req.sec_rw.len == 32768);
    recovered_read->reply.sec_rw.version = 1;
    f.peer_complete(recovered_read, 32768);
    assert(read_retval == 32768);

    delete read_op;
    printf("test_ec_degraded_read_retry_resets_parity passed\n");
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

// EC 2+1 PG on OSD 2, active with one object unstable on OSDs 1 and 3:
// PG_ACTIVE|PG_HAS_UNCLEAN with a stabilize batch to OSDs 1 and 3 in flight
static pg_t & start_unclean_ec_pg(osd_test_fixture_t & f, inode_t ino)
{
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
    f.reply_peer_list(1, { { { ino, 0 }, 1 } }, /*stable_count*/ 0);
    f.reply_local_list({ { { ino, 1 }, 1 } }, /*stable_count*/ 1);
    f.reply_peer_list(3, { { { ino, 2 }, 1 } }, /*stable_count*/ 0);
    f.pump();
    auto & pg = f.pg(1, 1);
    assert(pg.state == (PG_ACTIVE|PG_HAS_UNCLEAN));
    assert(pg.flush_batch && pg.flush_batch->flush_ops == 2);
    return pg;
}

// A client write to the unclean object waits in write_queue (st=1) for the flush
static osd_op_t *park_write(osd_test_fixture_t & f, uint64_t client_id, inode_t ino, uint64_t offset = 0)
{
    auto & pg = f.pg(1, 1);
    auto *wr = make_write_op(ino, offset, 4096, 0xab);
    wr->client_id = client_id;
    f.exec(wr);
    assert(wr->op_data && wr->op_data->st == 1);
    assert(pg.inflight == 1 && pg.write_queue.size() == 1);
    return wr;
}

static void print_pg_state(osd_test_fixture_t & f, const char *when)
{
    if (!f.has_pg(1, 1))
    {
        printf("  %s: PG 1/1 forgotten\n", when);
        return;
    }
    auto & pg = f.pg(1, 1);
    printf("  %s: PG 1/1 state=0x%x inflight=%d write_queue=%zu flush_batch=%s\n",
        when, pg.state, pg.inflight, pg.write_queue.size(), pg.flush_batch ? "yes" : "no");
}

// The flush fails while a write waits for it. The write must fail with EPIPE
// (the client retries it) and the PG must peer again, not hang in REPEERING
// forever with inflight held by the waiting write
void test_flush_error_cancels_waiting_write()
{
    printf("test_flush_error_cancels_waiting_write\n");

    osd_test_fixture_t f;
    inode_t ino = INODE_WITH_POOL(1, 1);
    auto & pg = start_unclean_ec_pg(f, ino);
    uint64_t client_id = f.connect_client();
    auto *wr = park_write(f, client_id, ino);

    f.peer_complete(f.peer_take(1, OSD_OP_SEC_STABILIZE), 0);
    f.peer_complete(f.peer_take(3, OSD_OP_SEC_STABILIZE), -EPIPE);
    print_pg_state(f, "after the flush error");

    assert(f.client(client_id)->sent_ops.count(wr->req.rw.header.id));
    assert(wr->reply.hdr.retval == -EPIPE);
    assert(pg.inflight == 0);
    assert(pg.state == PG_PEERING);

    f.complete_peering_empty();

    printf("test_flush_error_cancels_waiting_write passed\n");
}

// Same, but the connection to OSD 3 dies: the PG goes to REPEERING first
// (held by the flush batch), then the stabilize fails with EPIPE
void test_peer_death_during_flush_cancels_waiting_write()
{
    printf("test_peer_death_during_flush_cancels_waiting_write\n");

    osd_test_fixture_t f;
    inode_t ino = INODE_WITH_POOL(1, 1);
    auto & pg = start_unclean_ec_pg(f, ino);
    uint64_t client_id = f.connect_client();
    auto *wr = park_write(f, client_id, ino);

    f.fail_peer_connection(3);
    assert(pg.state & PG_REPEERING);
    assert(pg.flush_batch);

    f.peer_complete(f.peer_take(1, OSD_OP_SEC_STABILIZE), 0);
    print_pg_state(f, "after the flush error");

    assert(f.client(client_id)->sent_ops.count(wr->req.rw.header.id));
    assert(wr->reply.hdr.retval == -EPIPE);
    assert(pg.inflight == 0);
    assert(pg.state == PG_PEERING);

    f.complete_peering_empty();

    printf("test_peer_death_during_flush_cancels_waiting_write passed\n");
}

// The monitor moves the primary away while the flush is in flight, so the PG
// is STOPPING when the flush fails. The PG must finish stopping instead of
// restarting peering over a non-empty write_queue (assert in reset_pg)
void test_flush_error_on_stopping_pg()
{
    printf("test_flush_error_on_stopping_pg\n");

    osd_test_fixture_t f;
    inode_t ino = INODE_WITH_POOL(1, 1);
    auto & pg = start_unclean_ec_pg(f, ino);
    uint64_t client_id = f.connect_client();
    auto *wr = park_write(f, client_id, ino);

    f.st_cli->set("/vitastor/pg/config", json11::Json::object {
        { "items", json11::Json::object{
            { "1", json11::Json::object {
                { "1", json11::Json::object {
                    { "osd_set", json11::Json::array { 1, 2, 3 } },
                    { "primary", 1 },
                } },
            } }
        } },
    });
    assert(pg.state & PG_STOPPING);
    assert(pg.flush_batch);

    f.peer_complete(f.peer_take(1, OSD_OP_SEC_STABILIZE), 0);
    f.peer_complete(f.peer_take(3, OSD_OP_SEC_STABILIZE), -EPIPE);
    print_pg_state(f, "after the flush error");

    assert(f.client(client_id)->sent_ops.count(wr->req.rw.header.id));
    assert(wr->reply.hdr.retval == -EPIPE);
    assert(!f.has_pg(1, 1) || f.pg(1, 1).state == PG_OFFLINE);

    printf("test_flush_error_on_stopping_pg passed\n");
}

// No flush error at all: more unclean objects than FLUSH_BATCH, so the last
// one waits for the second batch. The PG repeers between the batches, when no
// batch is in flight, and nothing would ever flush that object for the write
void test_repeer_between_flush_batches_cancels_waiting_write()
{
    printf("test_repeer_between_flush_batches_cancels_waiting_write\n");

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
    inode_t ino = INODE_WITH_POOL(1, 1);
    const uint64_t stripe_size = 2*131072, n_objects = 513;
    std::vector<obj_ver_id> peer1, local, peer3;
    for (uint64_t i = 0; i < n_objects; i++)
    {
        peer1.push_back({ { ino, i*stripe_size + 0 }, 1 });
        local.push_back({ { ino, i*stripe_size + 1 }, 1 });
        peer3.push_back({ { ino, i*stripe_size + 2 }, 1 });
    }
    f.reply_peer_list(1, peer1, /*stable_count*/ 0);
    f.reply_local_list(local, /*stable_count*/ n_objects);
    f.reply_peer_list(3, peer3, /*stable_count*/ 0);
    f.pump();
    auto & pg = f.pg(1, 1);
    assert(pg.state == (PG_ACTIVE|PG_HAS_UNCLEAN));
    assert(pg.flush_batch && pg.flush_batch->flush_ops == 2);

    // The last object is not in the first batch
    uint64_t client_id = f.connect_client();
    auto *wr = park_write(f, client_id, ino, (n_objects-1)*stripe_size);

    f.peer_complete(f.peer_take(1, OSD_OP_SEC_STABILIZE), 0);
    f.peer_complete(f.peer_take(3, OSD_OP_SEC_STABILIZE), 0);
    assert(pg.state == (PG_ACTIVE|PG_HAS_UNCLEAN));
    assert(!pg.flush_batch);

    f.repeer_pgs(3);
    print_pg_state(f, "after the repeer");

    assert(f.client(client_id)->sent_ops.count(wr->req.rw.header.id));
    assert(wr->reply.hdr.retval == -EPIPE);
    assert(pg.inflight == 0);
    assert(pg.state == PG_PEERING);

    f.complete_peering_empty();

    printf("test_repeer_between_flush_batches_cancels_waiting_write passed\n");
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

// Regression test for a bug where a repeated partially failed write to the same
// object didn't clear LOC_OUTDATED from the replicas that succeeded this time —
// so a copy stayed marked outdated even though it had just received the newest
// version, and every failing write only ever added outdated copies.
//
// Layout: pool 1, replicated x3, primary = OSD 1 (us), secondaries = OSD 2 and 3.
//
// Write 1 fails with ENOSPC on OSD 3     -> OSD 3 is marked outdated.
// Write 2 fails with ENOSPC on OSD 1 (us) but succeeds on OSD 2 and OSD 3
//   -> OSD 1 must become outdated AND OSD 3 must stop being outdated.
void test_repeated_partial_write_unmarks_outdated()
{
    printf("test_repeated_partial_write_unmarks_outdated\n");

    osd_test_fixture_t f;
    f.configure_replicated_pool(/*pool_id*/ 1, /*pg_size*/ 3, /*pg_minsize*/ 1, /*pg_count*/ 1,
        { { 1, 2, 3 } });
    f.start(json11::Json::object {
        { "osd_num", 1 },
        { "etcd_address", "127.0.0.1:2379" },
        { "immediate_commit", "all" },
        { "block_size", 131072 },
        { "bitmap_granularity", 4096 },
    });
    f.connect_peer(2);
    f.connect_peer(3);
    f.complete_peering_empty();
    assert(f.pg(1, 1).state & PG_ACTIVE);

    inode_t inode = INODE_WITH_POOL(1, 1);
    object_id oid = { .inode = inode, .stripe = 0 };

    // Is the copy on <osd_num> marked outdated? A copy that vanished from the
    // set entirely counts as outdated, too.
    auto outdated_on = [&](osd_num_t osd_num)
    {
        auto *st = find_object_state(f.pg(1, 1), oid);
        assert(st);
        for (auto & chunk: st->osd_set)
            if (chunk.osd_num == osd_num)
                return (chunk.loc_bad & LOC_OUTDATED) != 0;
        return true;
    };

    // ============ WRITE 1 — ENOSPC on OSD 3 ============

    // Full-block writes on purpose: a sub-block write to a degraded object
    // would turn into a read-modify-write and only obscure what's tested here.
    auto *write1 = make_write_op(inode, 0, 131072, 0xab);
    int retval1 = -1;
    write1->callback = [&retval1](osd_op_t *op) { retval1 = op->reply.hdr.retval; };
    f.exec(write1);

    // Zero-length read resolving the (not yet existing) object version
    f.bs_zero_read_ok(0);

    // One local write + one write per peer
    assert(f.bs->queued.size() == 1);
    f.bs_write_ok(BS_OP_WRITE_STABLE, 1);
    f.peer_write_ok(2, OSD_OP_SEC_WRITE_STABLE, 1);
    auto *peer3_w1 = f.peer_take(3, OSD_OP_SEC_WRITE_STABLE);
    assert(retval1 == -1); // still waiting for OSD 3
    f.peer_complete(peer3_w1, -ENOSPC);

    assert(retval1 == -ENOSPC);
    assert(f.pg(1, 1).state & PG_HAS_DEGRADED);
    assert(!outdated_on(1));
    assert(!outdated_on(2));
    assert(outdated_on(3));
    printf("test_repeated_partial_write_unmarks_outdated: write 1 -> OSD 3 outdated\n");
    delete write1;

    // ============ WRITE 2 — ENOSPC locally, OK on both peers ============

    auto *write2 = make_write_op(inode, 0, 131072, 0xcd);
    int retval2 = -1;
    write2->callback = [&retval2](osd_op_t *op) { retval2 = op->reply.hdr.retval; };
    f.exec(write2);

    // The object exists at version 1 now
    f.bs_zero_read_ok(1);

    assert(f.bs->queued.size() == 1);
    auto *local_w2 = f.bs->take(BS_OP_WRITE_STABLE);
    assert(local_w2->version == 2);
    local_w2->retval = -ENOSPC;
    local_w2->callback(local_w2);
    f.peer_write_ok(2, OSD_OP_SEC_WRITE_STABLE, 2);
    f.peer_write_ok(3, OSD_OP_SEC_WRITE_STABLE, 2);

    assert(retval2 == -ENOSPC);

    // THE KEY ASSERTION — OSD 3 took the new version, it isn't outdated anymore
    if (outdated_on(3))
        printf("BUG: OSD 3 is still marked outdated after successfully taking the new version\n");
    assert(!outdated_on(3));
    // ... and the OSD that missed this write is
    assert(outdated_on(1));
    assert(!outdated_on(2));

    assert(f.pg(1, 1).inflight == 0);
    assert(f.pg(1, 1).write_queue.empty());

    delete write2;
    printf("test_repeated_partial_write_unmarks_outdated passed\n");
}

// A partially failed write to a misplaced object must record the OSDs that
// took the new version, even if the object wasn't on them before.
//
// Layout: pool 1, replicated x3, PG target set 1, 2, 3, primary = OSD 1 (us).
// The object is still on OSD 1, 2 and 4 from the previous set, so the write
// goes to OSD 1, 2 and 3, and OSD 3 isn't in the object's location set yet.
void test_partial_write_to_misplaced_object()
{
    printf("test_partial_write_to_misplaced_object\n");

    osd_test_fixture_t f;
    f.configure_replicated_pool(/*pool_id*/ 1, /*pg_size*/ 3, /*pg_minsize*/ 1, /*pg_count*/ 1,
        { { 1, 2, 3 } });
    // OSD 4 is in the PG history, so peering lists it, too
    f.st_cli->set("/vitastor/pg/history/1/1", json11::Json::object {
        { "osd_sets", json11::Json::array {
            json11::Json::array{ 1, 2, 4 },
        } },
    });
    f.start(json11::Json::object {
        { "osd_num", 1 },
        { "etcd_address", "127.0.0.1:2379" },
        { "immediate_commit", "all" },
        { "block_size", 131072 },
        { "bitmap_granularity", 4096 },
        // Don't let rebalance move the object before the test writes to it
        { "no_rebalance", true },
        { "no_recovery", true },
    });
    f.connect_peer(2);
    f.connect_peer(3);
    f.connect_peer(4);

    inode_t inode = INODE_WITH_POOL(1, 1);
    object_id oid = { .inode = inode, .stripe = 0 };

    // v1 is on OSD 1, 2 and 4, OSD 3 doesn't have the object yet
    f.reply_local_list({ { oid, 1 } }, 1);
    f.reply_peer_list(2, { { oid, 1 } }, 1);
    f.reply_peer_list(3, {}, 0);
    f.reply_peer_list(4, { { oid, 1 } }, 1);
    f.pump();
    assert(f.pg(1, 1).state == (PG_ACTIVE|PG_HAS_MISPLACED));
    assert(f.pg(1, 1).misplaced_objects.count(oid));

    // Does the object's location set have a current copy on <osd_num>?
    auto current_on = [&](osd_num_t osd_num)
    {
        auto *st = find_object_state(f.pg(1, 1), oid);
        assert(st);
        for (auto & chunk: st->osd_set)
            if (chunk.osd_num == osd_num)
                return !(chunk.loc_bad & LOC_OUTDATED);
        return false;
    };

    // ============ WRITE — ENOSPC on OSD 1 and 2, OK on OSD 3 ============

    // Full-block write, so there's no read-modify-write
    auto *write = make_write_op(inode, 0, 131072, 0xab);
    int write_retval = -1;
    write->callback = [&write_retval](osd_op_t *op) { write_retval = op->reply.hdr.retval; };
    f.exec(write);

    // Zero-length read resolving the object version
    f.bs_zero_read_ok(1);

    // The write goes to the target set, not to OSD 4
    auto *local_w = f.bs->take(BS_OP_WRITE_STABLE);
    // The new version includes the PG epoch, which the PG history has bumped
    uint64_t new_ver = local_w->version;
    assert(new_ver > 1);
    local_w->retval = -ENOSPC;
    local_w->callback(local_w);
    f.peer_complete(f.peer_take(2, OSD_OP_SEC_WRITE_STABLE), -ENOSPC);
    f.peer_write_ok(3, OSD_OP_SEC_WRITE_STABLE, new_ver);
    assert(f.peer(4)->sent_ops.empty());
    assert(write_retval == -ENOSPC);

    // THE KEY ASSERTION — OSD 3 has the new version, so it's a current copy.
    // Otherwise no current copy is left and the object becomes incomplete.
    if (!current_on(3))
        printf("BUG: OSD 3 took the new version, but isn't recorded as a current copy\n");
    assert(current_on(3));
    assert(!current_on(1));
    assert(!current_on(2));
    assert(!current_on(4));
    assert(!(f.pg(1, 1).state & PG_HAS_INCOMPLETE));

    // ============ READ — served by OSD 3 instead of failing with EIO ============

    auto *read = make_self_rw(OSD_OP_READ, inode, 0, 4096);
    int read_retval = -1;
    read->callback = [&read_retval](osd_op_t *op) { read_retval = op->reply.hdr.retval; };
    f.exec(read);
    assert(read_retval == -1);
    auto *peer3_read = f.peer_take(3, OSD_OP_SEC_READ);
    assert(peer3_read->req.sec_rw.len == 4096);
    peer3_read->reply.sec_rw.version = new_ver;
    f.peer_complete(peer3_read, 4096);
    assert(read_retval == 4096);

    assert(f.pg(1, 1).inflight == 0);
    assert(f.pg(1, 1).write_queue.empty());

    delete read;
    delete write;
    printf("test_partial_write_to_misplaced_object passed\n");
}

// Regression test: continue_primary_write() ignored the object state returned by
// mark_partial_write(). It leaked a reference to the new state and released the old
// state twice, which is a use-after-free when mark_object() has already freed it
void test_partial_write_object_state_refs()
{
    printf("test_partial_write_object_state_refs\n");

    osd_test_fixture_t f;
    f.configure_replicated_pool(/*pool_id*/ 1, /*pg_size*/ 3, /*pg_minsize*/ 1, /*pg_count*/ 1,
        { { 1, 2, 3 } });
    f.start(json11::Json::object {
        { "osd_num", 1 },
        { "etcd_address", "127.0.0.1:2379" },
        { "immediate_commit", "all" },
        { "block_size", 131072 },
        { "bitmap_granularity", 4096 },
        { "no_recovery", true },
    });
    f.connect_peer(2);
    f.connect_peer(3);

    inode_t inode = INODE_WITH_POOL(1, 1);
    object_id oid = { .inode = inode, .stripe = 0 };

    // OSD 1 and 2 have the object, OSD 3 doesn't -> degraded after peering
    f.reply_local_list({ { oid, 1 } }, 1);
    f.reply_peer_list(2, { { oid, 1 } }, 1);
    f.reply_peer_list(3, {}, 0);
    f.ringloop->loop();
    assert(f.pg(1, 1).state == (PG_ACTIVE|PG_HAS_DEGRADED));
    auto *st = find_object_state(f.pg(1, 1), oid);
    assert(st && st->object_count == 1 && st->ref_count == 0);

    // ENOSPC locally, OK on OSD 2 and 3
    auto *write = make_write_op(inode, 0, 131072, 0xcd);
    int retval = -1;
    write->callback = [&retval](osd_op_t *op) { retval = op->reply.hdr.retval; };
    f.exec(write);
    f.bs_zero_read_ok(1);
    auto *local_w = f.bs->take(BS_OP_WRITE_STABLE);
    uint64_t version = local_w->version;
    local_w->retval = -ENOSPC;
    local_w->callback(local_w);
    f.peer_write_ok(2, OSD_OP_SEC_WRITE_STABLE, version);
    f.peer_write_ok(3, OSD_OP_SEC_WRITE_STABLE, version);
    assert(retval == -ENOSPC);
    assert(f.pg(1, 1).inflight == 0);

    // No operation is in flight, so nothing may hold a reference to the new state
    st = find_object_state(f.pg(1, 1), oid);
    assert(st && st->object_count == 1);
    assert(st->ref_count == 0);
    delete write;

    // The same for a partially failed write to a clean object: ENOSPC on OSD 3 only
    object_id oid2 = { .inode = inode, .stripe = 131072 };
    auto *write2 = make_write_op(inode, 131072, 131072, 0xef);
    int retval2 = -1;
    write2->callback = [&retval2](osd_op_t *op) { retval2 = op->reply.hdr.retval; };
    f.exec(write2);
    f.bs_zero_read_ok(0);
    auto *local_w2 = f.bs->take(BS_OP_WRITE_STABLE);
    uint64_t version2 = local_w2->version;
    local_w2->retval = local_w2->len;
    local_w2->callback(local_w2);
    f.peer_write_ok(2, OSD_OP_SEC_WRITE_STABLE, version2);
    f.peer_complete(f.peer_take(3, OSD_OP_SEC_WRITE_STABLE), -ENOSPC);
    assert(retval2 == -ENOSPC);
    assert(f.pg(1, 1).inflight == 0);
    st = find_object_state(f.pg(1, 1), oid2);
    assert(st && st->object_count == 1);
    assert(st->ref_count == 0);
    delete write2;

    printf("test_partial_write_object_state_refs passed\n");
}

// A repeated delete of an already deleted object must succeed as a no-op.
// The fact-version read of the repeat sees ENOENT - that must resolve to
// "nothing to delete", not to an error.
void test_repeated_delete_is_noop()
{
    printf("test_repeated_delete_is_noop\n");

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

    inode_t inode = INODE_WITH_POOL(1, 1);
    object_id oid = { .inode = inode, .stripe = 0 };
    // v1 is present on both members
    f.reply_local_list({ { oid, 1 } }, 1);
    f.reply_peer_list(2, { { oid, 1 } }, 1);
    f.pump();
    assert(f.pg(1, 1).state == PG_ACTIVE);

    auto make_delete_op = [&]()
    {
        auto *op = new osd_op_t();
        op->op_type = OSD_OP_IN;
        op->client_id = 0;
        op->req.rw.header.magic = SECONDARY_OSD_OP_MAGIC;
        op->req.rw.header.id = 1;
        op->req.rw.header.opcode = OSD_OP_DELETE;
        op->req.rw.inode = inode;
        op->req.rw.offset = 0;
        op->req.rw.len = 0;
        return op;
    };

    // First delete: removes the chain locally and on the peer
    int delete_retval = -1;
    auto *del1 = make_delete_op();
    del1->callback = [&delete_retval](osd_op_t *op) { delete_retval = op->reply.hdr.retval; };
    f.exec(del1);
    f.bs_zero_read_ok(1);
    {
        auto *bd = f.bs->take(BS_OP_DELETE);
        bd->version = 1;
        bd->retval = 0;
        bd->callback(bd);
    }
    f.peer_write_ok(2, OSD_OP_SEC_DELETE, 1);
    assert(delete_retval == 0);
    delete del1;

    // Repeated delete: nothing left anywhere, must ack cleanly
    delete_retval = -1;
    auto *del2 = make_delete_op();
    del2->callback = [&delete_retval](osd_op_t *op) { delete_retval = op->reply.hdr.retval; };
    f.exec(del2);
    // The local chain is gone: the fact read answers ENOENT
    f.bs_zero_read_ok(0);
    f.pump();
    // THE KEY ASSERTION - the repeat is a no-op success, not an error
    assert(delete_retval == 0);
    // And it really deleted nothing: no subops went out
    assert(f.peer(2)->sent_ops.empty());
    delete del2;

    printf("test_repeated_delete_is_noop passed\n");
}

// A read whose copies were removed by a delete applied mid-flight is not
// corruption: a read racing a delete legally sees both "before" and "after".
// Such a read joins the write_queue behind the running delete and retries when
// it's done - it may neither fail with -EIO nor leave the object marked
// INCOMPLETE, because a concurrent write may be legitimately in flight too
//
// Layout: pool 1 replicated x2, PG set 1,2, primary = OSD 1 (us). Object v1
// is on both members. A delete runs (applied locally, waiting for the peer),
// a read races it and both its subops answer raw -ENOENT
void test_read_enoent_waits_behind_running_delete()
{
    printf("test_read_enoent_waits_behind_running_delete\n");

    osd_test_fixture_t f;
    f.configure_replicated_pool(/*pool_id*/ 1, /*pg_size*/ 2, /*pg_minsize*/ 1, /*pg_count*/ 1,
        { { 1, 2 } });
    f.start(json11::Json::object {
        { "osd_num", 1 },
        { "etcd_address", "127.0.0.1:2379" },
        { "immediate_commit", "all" },
        { "no_recovery", true },
        { "block_size", 131072 },
        { "bitmap_granularity", 4096 },
    });
    f.connect_peer(2);

    inode_t inode = INODE_WITH_POOL(1, 1);
    object_id oid = { .inode = inode, .stripe = 0 };
    f.reply_local_list({ { oid, 1 } }, 1);
    // OSD 2 has nothing: the object is DEGRADED on OSD 1 alone, exactly the
    // seed-5 shape where a raw ENOENT is not excused by OP_DATA_ENOENT_OK
    f.reply_peer_list(2, {}, 0);
    f.pump();
    assert(f.pg(1, 1).state == (PG_ACTIVE|PG_HAS_DEGRADED));

    // Delete v1: fact read, then local delete + peer SEC_DELETE. Hold both
    int delete_retval = -1;
    auto *del = new osd_op_t();
    del->op_type = OSD_OP_IN;
    del->client_id = 0;
    del->req.rw.header.magic = SECONDARY_OSD_OP_MAGIC;
    del->req.rw.header.id = 1;
    del->req.rw.header.opcode = OSD_OP_DELETE;
    del->req.rw.inode = inode;
    del->req.rw.offset = 0;
    del->req.rw.len = 0;
    del->callback = [&delete_retval](osd_op_t *op) { delete_retval = op->reply.hdr.retval; };
    f.exec(del);
    f.bs_zero_read_ok(1);
    assert(f.pg(1, 1).write_queue.count(oid) == 1);

    // A read races the delete
    int read_retval = -1;
    uint64_t read_ver = 1;
    auto *read = new osd_op_t();
    read->op_type = OSD_OP_IN;
    read->client_id = 0;
    read->req.rw.header.magic = SECONDARY_OSD_OP_MAGIC;
    read->req.rw.header.id = 2;
    read->req.rw.header.opcode = OSD_OP_READ;
    read->req.rw.inode = inode;
    read->req.rw.offset = 0;
    read->req.rw.len = 4096;
    read->callback = [&read_retval, &read_ver](osd_op_t *op)
    {
        read_retval = op->reply.hdr.retval;
        read_ver = op->reply.rw.version;
    };
    f.exec(read);

    // The copy this read reads from is already deleted - the subop answers
    // raw -ENOENT
    auto *local_read = f.bs->take(BS_OP_READ);
    assert(local_read->len == 4096);
    local_read->retval = -ENOENT;
    local_read->callback(local_read);

    // THE KEY ASSERTION: the read neither fails nor rewrites the object set -
    // it waits in the write_queue behind the running delete. Pre-fix it would
    // have marked OSD 1 corrupted (falsely degrading the object) and retried
    // against OSD 2
    assert(read_retval == -1);
    assert(f.pg(1, 1).state == (PG_ACTIVE|PG_HAS_DEGRADED));
    assert(f.pg(1, 1).write_queue.count(oid) == 2);

    // The delete lands; the queue drains to the read
    auto *bd = f.bs->take(BS_OP_DELETE);
    bd->version = 1;
    bd->retval = 0;
    bd->callback(bd);
    f.peer_write_ok(2, OSD_OP_SEC_DELETE, 1);
    assert(delete_retval == 0);

    // The retry subops arrive. The object is really gone now, so the read has
    // to resolve as non-existent: zeros with version 0
    local_read = f.bs->take(BS_OP_READ);
    assert(local_read->len == 4096);
    local_read->retval = -ENOENT;
    local_read->callback(local_read);

    assert(read_retval == 4096);
    assert(read_ver == 0);
    assert(f.pg(1, 1).state == PG_ACTIVE);
    assert(f.pg(1, 1).write_queue.count(oid) == 0);

    delete del;
    delete read;
    printf("test_read_enoent_waits_behind_running_delete passed\n");
}

// The other side of the same branch: copies answering -ENOENT while NO
// write/delete runs on the object is genuine data loss. It must still fail
// with -EIO and mark the object INCOMPLETE - the ENOENT requeue must not
// swallow it
void test_read_enoent_without_racing_op_marks_corrupted()
{
    printf("test_read_enoent_without_racing_op_marks_corrupted\n");

    osd_test_fixture_t f;
    f.configure_replicated_pool(/*pool_id*/ 1, /*pg_size*/ 2, /*pg_minsize*/ 1, /*pg_count*/ 1,
        { { 1, 2 } });
    f.start(json11::Json::object {
        { "osd_num", 1 },
        { "etcd_address", "127.0.0.1:2379" },
        { "immediate_commit", "all" },
        { "block_size", 131072 },
        { "bitmap_granularity", 4096 },
        { "no_recovery", true },
    });

    f.connect_peer(2);

    inode_t inode = INODE_WITH_POOL(1, 1);
    object_id oid = { .inode = inode, .stripe = 0 };
    f.reply_local_list({ { oid, 1 } }, 1);
    // DEGRADED object (only OSD 1 has it): ENOENT is not excused, see above
    f.reply_peer_list(2, {}, 0);
    f.pump();
    assert(f.pg(1, 1).state == (PG_ACTIVE|PG_HAS_DEGRADED));

    int read_retval = 0;
    auto *read = new osd_op_t();
    read->op_type = OSD_OP_IN;
    read->client_id = 0;
    read->req.rw.header.magic = SECONDARY_OSD_OP_MAGIC;
    read->req.rw.header.id = 2;
    read->req.rw.header.opcode = OSD_OP_READ;
    read->req.rw.inode = inode;
    read->req.rw.offset = 0;
    read->req.rw.len = 4096;
    read->callback = [&read_retval](osd_op_t *op) { read_retval = op->reply.hdr.retval; };
    f.exec(read);

    auto *local_read = f.bs->take(BS_OP_READ);
    assert(local_read->len == 4096);
    local_read->retval = -ENOENT;
    local_read->callback(local_read);
    // (no peer subop: a replicated read goes to the one member holding the object)

    // Nobody was running on the object - this is corruption, not a race
    assert(read_retval == -EIO);
    assert(f.pg(1, 1).state & PG_HAS_INCOMPLETE);
    assert(f.pg(1, 1).write_queue.count(oid) == 0);

    delete read;
    printf("test_read_enoent_without_racing_op_marks_corrupted passed\n");
}

// Peering must not forget an outdated copy just because its OSD already holds
// the object at another role. A stale piece kept by a set member after a set
// change looks, by OSD number alone, like a copy that the target set already
// covers - but the target version at that role is held by a different OSD, so
// the stale copy has to land in the object's OSD set as LOC_OUTDATED, or
// deletes and cleanup would never reach it.
//
// Layout: EC 2+2 pool, PG 1/1, cur_set = role 0..3 => OSD 1..4. Object
// INODE_WITH_POOL(1,1) is fully present at version 2 (role R at OSD R+1), and
// OSD 2 additionally keeps the role-3 piece of the older version 1.
void test_peering_outdated_copy_other_role()
{
    printf("test_peering_outdated_copy_other_role\n");

    inode_t inode = INODE_WITH_POOL(1, 1);

    // One pg_t per scenario; the peer LIST results are fed straight into
    // calc_object_states, the way peering hands them over. calc_object_states
    // frees the buffers, so they are malloc'ed
    auto make_pg = []()
    {
        pg_t *pg = new pg_t();
        pg->pool_id = 1;
        pg->pg_num = 1;
        pg->scheme = POOL_SCHEME_EC;
        pg->pg_size = 4;
        pg->pg_cursize = 4;
        pg->pg_minsize = 2;
        pg->pg_data_size = 2;
        pg->target_set = { 1, 2, 3, 4 };
        pg->cur_set = { 1, 2, 3, 4 };
        pg->all_peers = { 1, 2, 3, 4 };
        pg->cur_peers = { 1, 2, 3, 4 };
        pg->peering_state = new pg_peering_state_t();
        return pg;
    };
    // A peer list of (stripe, version) entries, all stable
    auto add_list = [](pg_t *pg, osd_num_t osd_num, std::vector<std::pair<uint64_t, uint64_t>> items)
    {
        obj_ver_id *buf = (obj_ver_id*)malloc(sizeof(obj_ver_id)*items.size());
        for (int i = 0; i < items.size(); i++)
            buf[i] = { .oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = items[i].first }, .version = items[i].second };
        pg->peering_state->list_results[osd_num] = (pg_list_result_t){
            .buf = buf, .total_count = items.size(), .stable_count = items.size(),
        };
    };

    // ============ A: stale role-3 copy on OSD 2, which holds role 1 at target ============

    pg_t *pg = make_pg();
    add_list(pg, 1, { { 0, 2 } });
    add_list(pg, 2, { { 1, 2 }, { 3, 1 } });
    add_list(pg, 3, { { 2, 2 } });
    add_list(pg, 4, { { 3, 2 } });
    pg->calc_object_states(0);

    object_id oid = { .inode = inode, .stripe = 0 };
    assert(pg->state & PG_HAS_MISPLACED);
    assert(pg->total_count == 1 && pg->clean_count == 0);
    pg_osd_set_state_t *st = find_object_state(*pg, oid);
    if (!st)
        printf("BUG: the stale copy on OSD 2 was forgotten by peering\n");
    assert(st);
    bool outdated = false;
    for (auto & chunk: st->osd_set)
        if (chunk.role == 3 && chunk.osd_num == 2)
            outdated = (chunk.loc_bad & LOC_OUTDATED) != 0;
    assert(outdated);
    // Reads must still target the good copy, not the outdated one
    assert(st->read_target.size() == 4 && st->read_target[3] == 4);
    delete pg->peering_state;
    delete pg;
    printf("test_peering_outdated_copy_other_role: stale copy on OSD 2 marked outdated\n");

    // ============ B: the same role on the same OSD stays subsumed ============

    // The OSD's own history: an older version of the very piece it holds at the
    // target version is not a stray copy - it must not appear as outdated
    pg = make_pg();
    add_list(pg, 1, { { 0, 2 } });
    add_list(pg, 2, { { 1, 2 }, { 1, 1 } });
    add_list(pg, 3, { { 2, 2 } });
    add_list(pg, 4, { { 3, 2 } });
    pg->calc_object_states(0);
    assert(!(pg->state & PG_HAS_MISPLACED));
    assert(pg->total_count == 1 && pg->clean_count == 1);
    assert(!find_object_state(*pg, oid));
    delete pg->peering_state;
    delete pg;

    printf("test_peering_outdated_copy_other_role passed\n");
}

// PG epoch must be based on the value persisted in etcd, not on the version
// epochs of LIST copies. Copies of the epoch-7 chain may be fully deleted while
// etcd still remembers epoch 7; if peering recomputed the epoch from surviving
// older copies it would regress, and writes to recreated objects would reuse
// version numbers that copies missed by a delete may still hold - peering would
// then treat those stale copies as up-to-date.
void test_pg_epoch_base_is_reported_epoch()
{
    printf("test_pg_epoch_base_is_reported_epoch\n");

    osd_test_fixture_t f;
    f.configure_replicated_pool(/*pool_id*/ 1, /*pg_size*/ 2, /*pg_minsize*/ 1, /*pg_count*/ 1,
        { { 1, 2 } });
    // etcd persisted epoch 7; the LIST will only contain epoch-4 copies
    f.st_cli->set("/vitastor/pg/history/1/1", json11::Json::object {
        { "osd_sets", json11::Json::array { json11::Json::array { 1, 2 } } },
        { "all_peers", json11::Json::array { 1, 2 } },
        { "epoch", 7 },
    });
    f.start(json11::Json::object {
        { "osd_num", 1 },
        { "etcd_address", "127.0.0.1:2379" },
        { "immediate_commit", "none" },
        { "block_size", 131072 },
        { "bitmap_granularity", 4096 },
        { "no_recovery", true },
    });
    assert(f.st_cli->pool_config[1].pg_config[1].epoch == 7);
    f.connect_peer(2);
    f.reply_local_list({ { { INODE_WITH_POOL(1, 1), 0 }, 0x40001 } }, 1);
    f.reply_peer_list(2, { { { INODE_WITH_POOL(1, 1), 0 }, 0x40001 } }, 1);
    f.ringloop->loop();
    // A clean activation must not bump the epoch, but it also must not drag it
    // down to the 4 seen in copies
    assert(f.pg(1, 1).state == PG_ACTIVE);
    assert(f.pg(1, 1).epoch == 7);

    printf("test_pg_epoch_base_is_reported_epoch passed\n");
}

// A non-clean activation must bump the epoch above the persisted base, not above
// the version epochs of surviving copies
void test_pg_epoch_bump_above_reported_epoch()
{
    printf("test_pg_epoch_bump_above_reported_epoch\n");

    osd_test_fixture_t f;
    f.configure_replicated_pool(/*pool_id*/ 1, /*pg_size*/ 2, /*pg_minsize*/ 1, /*pg_count*/ 1,
        { { 1, 2 } });
    f.st_cli->set("/vitastor/pg/history/1/1", json11::Json::object {
        { "osd_sets", json11::Json::array { json11::Json::array { 1, 2 } } },
        { "all_peers", json11::Json::array { 1, 2 } },
        { "epoch", 7 },
    });
    f.start(json11::Json::object {
        { "osd_num", 1 },
        { "etcd_address", "127.0.0.1:2379" },
        { "immediate_commit", "none" },
        { "block_size", 131072 },
        { "bitmap_granularity", 4096 },
        { "no_recovery", true },
    });
    f.connect_peer(2);
    // Only one copy comes back -> the PG is not purely ACTIVE
    f.reply_local_list({ { { INODE_WITH_POOL(1, 1), 0 }, 0x40001 } }, 1);
    f.reply_peer_list(2, {}, 0);
    f.ringloop->loop();
    assert(f.pg(1, 1).state != PG_ACTIVE);
    assert(f.pg(1, 1).epoch == 8);

    printf("test_pg_epoch_bump_above_reported_epoch passed\n");
}

// A partial unstable chain left by a failed/rejected EC write must not become the
// committed version even when it holds pg_data_size pieces: that would turn a
// possibly-nonexistent write into committed data with zero redundancy. Peering must
// fall back to the older stable version and roll the fragments back. A stable
// fragment, on the other hand, is possibly-acked - the OSD can never know otherwise
// - so it stays the target and is never rolled back, incomplete or not.
// A partial chain of pg_data_size pieces is not committed either, even when a
// degraded set holds a fragment per member: an EC 2+2 pair on the two data roles
// of a 2-member set still cannot decode (subcase D: refused-write fragments of a
// fresh object - peering must delete them, not commit).
//
// Layout: EC 2+2, PG 1/1, cur_set = role 0..3 => OSD 1..4. Object INODE_WITH_POOL(1,1)
// is stable at v1 on all roles; a rejected write left v2 unstable on role 0 (OSD 1)
// and role 2 (OSD 3) only. Peer lists put stable entries first (stable_count prefix).
void test_peering_partial_unstable_ec_rolls_back()
{
    printf("test_peering_partial_unstable_ec_rolls_back\n");

    inode_t inode = INODE_WITH_POOL(1, 1);
    object_id oid = { .inode = inode, .stripe = 0 };

    auto make_pg = []()
    {
        pg_t *pg = new pg_t();
        pg->pool_id = 1;
        pg->pg_num = 1;
        pg->scheme = POOL_SCHEME_EC;
        pg->pg_size = 4;
        pg->pg_cursize = 4;
        pg->pg_minsize = 2;
        pg->pg_data_size = 2;
        pg->target_set = { 1, 2, 3, 4 };
        pg->cur_set = { 1, 2, 3, 4 };
        pg->all_peers = { 1, 2, 3, 4 };
        pg->cur_peers = { 1, 2, 3, 4 };
        pg->peering_state = new pg_peering_state_t();
        return pg;
    };
    // Peer list of (stripe, version) entries; the first `nstable` are stable
    auto add_list = [&](pg_t *pg, osd_num_t osd_num, std::vector<std::pair<uint64_t, uint64_t>> items,
        uint64_t nstable)
    {
        obj_ver_id *buf = (obj_ver_id*)malloc(sizeof(obj_ver_id)*items.size());
        for (int i = 0; i < items.size(); i++)
            buf[i] = { .oid = { .inode = inode, .stripe = items[i].first }, .version = items[i].second };
        pg->peering_state->list_results[osd_num] = (pg_list_result_t){
            .buf = buf, .total_count = items.size(), .stable_count = nstable,
        };
    };

    // ============ A: partial unstable v2 falls back to stable v1 ============

    pg_t *pg = make_pg();
    add_list(pg, 1, { { 0, 1 }, { 0, 2 } }, 1);
    add_list(pg, 2, { { 1, 1 } }, 1);
    add_list(pg, 3, { { 2, 1 }, { 2, 2 } }, 1);
    add_list(pg, 4, { { 3, 1 } }, 1);
    pg->calc_object_states(0);

    // v1 is the target: the object is clean at v1, reads see v1, fragments roll back
    assert(pg->total_count == 1 && pg->clean_count == 1);
    assert(!(pg->state & (PG_HAS_INCOMPLETE|PG_HAS_DEGRADED)));
    assert(pg->state & PG_HAS_UNCLEAN);
    assert(!find_object_state(*pg, oid));
    assert(pg->ver_override.count(oid) && pg->ver_override[oid] == 1);
    assert(pg->flush_actions.size() == 2);
    for (auto & fa: pg->flush_actions)
    {
        assert(fa.second.rollback && !fa.second.make_stable && fa.second.rollback_to == 1);
        assert((fa.first.oid.stripe & STRIPE_MASK) == (fa.first.osd_num == 1 ? 0u : 2u));
    }
    delete pg->peering_state;
    delete pg;

    // ============ B: fully materialized unstable v2 is committed as before ============

    pg = make_pg();
    add_list(pg, 1, { { 0, 1 }, { 0, 2 } }, 1);
    add_list(pg, 2, { { 1, 1 }, { 1, 2 } }, 1);
    add_list(pg, 3, { { 2, 1 }, { 2, 2 } }, 1);
    add_list(pg, 4, { { 3, 1 }, { 3, 2 } }, 1);
    pg->calc_object_states(0);

    assert(pg->total_count == 1 && pg->clean_count == 1);
    assert(pg->state & PG_HAS_UNCLEAN);
    assert(!pg->ver_override.count(oid));
    assert(pg->flush_actions.size() == 4);
    for (auto & fa: pg->flush_actions)
        assert(fa.second.make_stable && !fa.second.rollback && fa.second.stable_to == 2);
    delete pg->peering_state;
    delete pg;

    // ============ C: a stable fragment is possibly-acked: target, never rolled back ============

    pg = make_pg();
    add_list(pg, 1, { { 0, 1 }, { 0, 2 } }, 2);
    add_list(pg, 2, { { 1, 1 } }, 1);
    add_list(pg, 3, { { 2, 1 }, { 2, 2 } }, 2);
    add_list(pg, 4, { { 3, 1 } }, 1);
    pg->calc_object_states(0);

    // v2 (2 of 4 roles, stable) is degraded, not incomplete, and nothing is rolled back
    assert(pg->state & PG_HAS_DEGRADED);
    assert(!(pg->state & PG_HAS_INCOMPLETE));
    assert(pg->flush_actions.empty());
    delete pg->peering_state;
    delete pg;

    // ============ D: an unstable chain that was never whole. v2 exists on roles 0 and 1
    // only - pg_data_size pieces, but EC 2+2 needs all 4 to decode, and the object never
    // existed at any stable version: this is what a refused write to a fresh object
    // leaves behind (seed 1996). The old rule (n_roles >= pg_data_size) targeted such a
    // chain and peering-flush committed it; the new rule must refuse v2 and delete the
    // fragments entirely ============

    pg = make_pg();
    pg->pg_cursize = 2;
    pg->cur_set = { 1, 2 };
    pg->cur_peers = { 1, 2 };
    add_list(pg, 1, { { 0, 2 } }, 0);
    add_list(pg, 2, { { 1, 2 } }, 0);
    pg->calc_object_states(0);

    // Nothing is committed: the object stays absent, both fragments are deleted
    assert(pg->total_count == 0 && pg->clean_count == 0);
    assert(pg->state & PG_HAS_UNCLEAN);
    assert(!(pg->state & PG_HAS_INCOMPLETE));
    assert(!pg->ver_override.count(oid));
    assert(pg->flush_actions.size() == 2);
    for (auto & fa: pg->flush_actions)
    {
        assert(fa.second.rollback && !fa.second.make_stable && fa.second.rollback_to == 0);
        assert((fa.first.oid.stripe & STRIPE_MASK) == (fa.first.osd_num == 1 ? 0u : 1u));
    }
    delete pg->peering_state;
    delete pg;

    printf("test_peering_partial_unstable_ec_rolls_back passed\n");
}

// A dispatched SYNC holds its stabilize snapshot; a delete of a covered object must
// wait after its fact read while the snapshot is in flight (SEC_STABILIZE of a version
// the delete removes would answer -ENOENT) and must resume when the SYNC finishes.
// Replicated pools have no unstable versions, so this is an EC-only race
void test_delete_waits_for_stabilize_snapshot()
{
    printf("test_delete_waits_for_stabilize_snapshot\n");

    osd_test_fixture_t f;
    // EC 2+1, single PG on OSDs 1,2,3, OSD 1 is the primary
    f.configure_ec_pool(/*data_chunks*/ 2, /*parity_chunks*/ 1, /*primary_osd*/ 1);
    f.start(json11::Json::object {
        { "osd_num", 1 },
        { "etcd_address", "127.0.0.1:2379" },
        { "immediate_commit", "none" },
        { "block_size", 131072 },
        { "bitmap_granularity", 4096 },
    });
    f.connect_peer(2);
    f.connect_peer(3);
    f.complete_peering_empty();
    assert(f.pg(1, 1).state == PG_ACTIVE);

    inode_t inode = INODE_WITH_POOL(1, 1);
    uint64_t client_id = f.connect_client();

    // One full-stripe lazy write: v1 is unstable on all three members
    auto *wr = make_write_op(inode, 0, 256*1024, 0xab);
    wr->client_id = client_id;
    wr->req.hdr.id = 1;
    f.exec(wr);
    f.bs_zero_read_ok(0);
    f.bs_write_ok(BS_OP_WRITE, 1);
    {
        auto *pw2 = f.peer_take(2, OSD_OP_SEC_WRITE);
        pw2->reply.sec_rw.version = 1;
        f.peer_complete(pw2, pw2->req.sec_rw.len);
        auto *pw3 = f.peer_take(3, OSD_OP_SEC_WRITE);
        pw3->reply.sec_rw.version = 1;
        f.peer_complete(pw3, pw3->req.sec_rw.len);
    }
    assert(f.unstable_write_count_entries() == 3);

    // Dispatch a SYNC; it takes its snapshot and waits for subop replies
    auto *sync = new osd_op_t();
    sync->op_type = OSD_OP_IN;
    sync->client_id = client_id;
    sync->req.hdr.magic = SECONDARY_OSD_OP_MAGIC;
    sync->req.hdr.id = 43;
    sync->req.hdr.opcode = OSD_OP_SYNC;
    f.exec(sync);
    // The snapshot covers the object's base stripe with all three role copies
    assert(f.stabilize_ref_count() == 1);
    assert(f.stabilize_ref_of((object_id){ .inode = inode, .stripe = 0 }) == 3);
    assert(f.unstable_write_count_entries() == 0);

    // The delete dispatches its fact read at once, but must park after the read is
    // answered: purge+wait sit between the CAS check and the SEC_DELETE dispatch
    int delete_retval = -1;
    auto *del = new osd_op_t();
    del->op_type = OSD_OP_IN;
    del->client_id = 0;
    del->req.rw.header.magic = SECONDARY_OSD_OP_MAGIC;
    del->req.rw.header.id = 2;
    del->req.rw.header.opcode = OSD_OP_DELETE;
    del->req.rw.inode = inode;
    del->req.rw.offset = 0;
    del->req.rw.len = 0;
    del->callback = [&delete_retval](osd_op_t *op) { delete_retval = op->reply.hdr.retval; };
    f.exec(del);
    f.pump();   // flush handle_peers so the assertions below are meaningful
    assert(delete_retval == -1);
    assert(f.pg(1, 1).write_queue.size() == 1);
    // The fact read is in flight, no delete went anywhere yet
    // (the SYNC's own local BS_OP_SYNC and peer SEC_SYNCs are still pending, of course)
    assert(f.bs->take(BS_OP_DELETE, false) == NULL);
    assert(!f.peer_has_sent_op(2, OSD_OP_SEC_DELETE) && !f.peer_has_sent_op(3, OSD_OP_SEC_DELETE));
    // Answer the local fact read: v1 (the peer SEC_READs are answered by the mock)
    f.bs_zero_read_ok(1);
    assert(f.dels_awaiting_sync_count() == 1);
    assert(f.bs->take(BS_OP_DELETE, false) == NULL);
    assert(!f.peer_has_sent_op(2, OSD_OP_SEC_DELETE) && !f.peer_has_sent_op(3, OSD_OP_SEC_DELETE));

    // Finish the SYNC: SYNC subops, then STABILIZE subops
    {
        auto *local = f.bs->take(BS_OP_SYNC);
        local->retval = 0;
        local->callback(local);
        for (osd_num_t p = 2; p <= 3; p++)
        {
            auto *ps = f.peer_take(p, OSD_OP_SEC_SYNC);
            f.peer_complete(ps, 0);
        }
        auto *local_st = f.bs->take(BS_OP_STABLE);
        local_st->retval = 0;
        local_st->callback(local_st);
        for (osd_num_t p = 2; p <= 3; p++)
        {
            auto *pst = f.peer_take(p, OSD_OP_SEC_STABILIZE);
            f.peer_complete(pst, 0);
        }
    }
    assert(sync->reply.hdr.retval == 0);
    // The snapshot released its refs and woke the waiting delete, which dispatched
    // its deletes now that no snapshot covers the object
    assert(f.stabilize_ref_count() == 0);
    assert(f.dels_awaiting_sync_count() == 0);
    assert(delete_retval == -1);
    {
        auto *bd = f.bs->take(BS_OP_DELETE);
        bd->version = 2;
        bd->retval = 0;
        bd->callback(bd);
        f.peer_write_ok(2, OSD_OP_SEC_DELETE, 2);
        f.peer_write_ok(3, OSD_OP_SEC_DELETE, 2);
    }
    assert(delete_retval == 0);
    delete del;
}

// A repeer or stop must remove the PG's versions from unstable_writes: that map is what a
// client SYNC snapshot stabilizes, and a SYNC issued after the repeer would commit
// versions the new peering round is about to re-decide (and roll back)
void test_repeer_forgets_unstable_writes(bool stop_pg)
{
    printf("test_repeer_forgets_unstable_writes (stop_pg=%d)\n", stop_pg);
    osd_test_fixture_t f;
    // EC 2+1, single PG on OSDs 1,2,3, OSD 1 is the primary
    f.configure_ec_pool(/*data_chunks*/ 2, /*parity_chunks*/ 1, /*primary_osd*/ 1);
    f.start(json11::Json::object {
        { "osd_num", 1 },
        { "etcd_address", "127.0.0.1:2379" },
        { "immediate_commit", "none" },
        { "block_size", 131072 },
        { "bitmap_granularity", 4096 },
    });
    f.connect_peer(2);
    f.complete_peering_empty();
    f.connect_peer(3);
    f.complete_peering_empty();
    assert(f.pg(1, 1).state == PG_ACTIVE);
    uint64_t client_id = f.connect_client();

    // A full lazy EC write makes one object unstable on all three role copies, and a
    // SYNC from its writer covers it: the snapshot holds 3 stabilize refs and the
    // SYNC waits for its subop answers with the PG inflight
    inode_t inode_a = INODE_WITH_POOL(1, 1);
    {
        auto *wr = make_write_op(inode_a, 0, 256*1024, 0xab);
        wr->client_id = client_id;
        wr->req.hdr.id = 1;
        f.exec(wr);
        f.bs_zero_read_ok(0);
        f.bs_write_ok(BS_OP_WRITE, 1);
        auto *pw2 = f.peer_take(2, OSD_OP_SEC_WRITE);
        pw2->reply.sec_rw.version = 1;
        f.peer_complete(pw2, pw2->req.sec_rw.len);
        auto *pw3 = f.peer_take(3, OSD_OP_SEC_WRITE);
        pw3->reply.sec_rw.version = 1;
        f.peer_complete(pw3, pw3->req.sec_rw.len);
    }
    assert(f.unstable_write_count_entries() == 3);
    auto *sync1 = new osd_op_t();
    sync1->op_type = OSD_OP_IN;
    sync1->client_id = client_id;
    sync1->req.hdr.magic = SECONDARY_OSD_OP_MAGIC;
    sync1->req.hdr.id = 43;
    sync1->req.hdr.opcode = OSD_OP_SYNC;
    f.exec(sync1);
    assert(f.stabilize_ref_of((object_id){ .inode = inode_a, .stripe = 0 }) == 3);
    assert(f.unstable_write_count_entries() == 0);

    // A second full lazy write while the SYNC is in flight: the SYNC snapshot does
    // not cover it, so 3 new unstable versions are left pending the next SYNC
    inode_t inode_b = INODE_WITH_POOL(1, 2);
    {
        auto *wr = make_write_op(inode_b, 0, 256*1024, 0xab);
        wr->client_id = client_id;
        wr->req.hdr.id = 2;
        f.exec(wr);
        f.bs_zero_read_ok(0);
        f.bs_write_ok(BS_OP_WRITE, 1);
        auto *pw2 = f.peer_take(2, OSD_OP_SEC_WRITE);
        pw2->reply.sec_rw.version = 1;
        f.peer_complete(pw2, pw2->req.sec_rw.len);
        auto *pw3 = f.peer_take(3, OSD_OP_SEC_WRITE);
        pw3->reply.sec_rw.version = 1;
        f.peer_complete(pw3, pw3->req.sec_rw.len);
    }
    assert(f.unstable_write_count_entries() == 3);

    if (!stop_pg)
    {
        // Drop a peer to repeer the PG. The in-flight SYNC keeps the PG busy, so it goes to REPEERING
        f.fail_peer_connection(2);
        assert(f.pg(1, 1).state & PG_REPEERING);
    }
    else
    {
        // Stop the PG like the monitor does it
        f.st_cli->set("/vitastor/pg/config", json11::Json::object {
            { "items", json11::Json::object{
                { "1", json11::Json::object {
                    { "1", json11::Json::object {
                        { "osd_set", json11::Json::array { 1, 2, 3 } },
                        { "primary", 2 },
                    } },
                } }
            } },
        });
        f.ringloop->loop();
        assert(f.pg(1, 1).state & PG_STOPPING);
    }

    // repeer_pg must forget the PG's unstable writes before anything else: a SYNC
    // racing the re-peering would otherwise snapshot versions that the new peering
    // round is about to re-decide (and possibly roll back)
    assert(f.unstable_write_count_entries() == 0);
    assert(!f.has_dirty_pg(1, 1));
    assert(!f.has_client(client_id));

    // Drain the SYNC: its subops now answer normally (the peers stay connected,
    // only the writer's session died). The SYNC finishes with EPIPE, releases the
    // snapshot refs and the PG inflight, and continue_pg completes the stop - the
    // reset_pg inside finish_stop_pg also asserts that no unstable write survived
    while (true)
    {
        auto *local = f.bs->take(BS_OP_SYNC, false);
        if (local)
        {
            local->retval = 0;
            local->callback(local);
            continue;
        }
        bool advanced = false;
        for (osd_num_t peer : { (osd_num_t)2, (osd_num_t)3 })
        {
            auto *ps = f.peer_take_opt(peer, OSD_OP_SEC_SYNC);
            if (ps)
            {
                f.peer_complete(ps, 0);
                advanced = true;
                break;
            }
            auto *pst = f.peer_take_opt(peer, OSD_OP_SEC_STABILIZE);
            if (pst)
            {
                f.peer_complete(pst, 0);
                advanced = true;
                break;
            }
        }
        if (advanced)
            continue;
        auto *local_st = f.bs->take(BS_OP_STABLE, false);
        if (local_st)
        {
            local_st->retval = 0;
            local_st->callback(local_st);
            continue;
        }
        break;
    }
    f.pump();
    assert(f.stabilize_ref_count() == 0);

    // The stop completed: the PG is OFFLINE (possibly already erased). Note that a
    // SYNC racing the stop must not re-add the forgotten versions on its error
    // path - that would defeat the forget and stabilize rolled-back data later
    assert(f.unstable_write_count_entries() == 0);
    if (f.has_pg(1, 1))
        assert(!(f.pg(1, 1).state & PG_STOPPING));

    // FIXME: Make this test pass without leaks without the following line:
    f.complete_peering_empty();

    printf("test_repeer_forgets_unstable_writes (stop_pg=%d) passed\n", stop_pg);
}

// A delete must only wait for objects that are actually covered: a fresh unstable
// object written after the SYNC took its snapshot must delete without waiting
void test_delete_waits_only_for_covered_objects()
{
    printf("test_delete_waits_only_for_covered_objects\n");

    osd_test_fixture_t f;
    f.configure_ec_pool(/*data_chunks*/ 2, /*parity_chunks*/ 1, /*primary_osd*/ 1);
    f.start(json11::Json::object {
        { "osd_num", 1 },
        { "etcd_address", "127.0.0.1:2379" },
        { "immediate_commit", "none" },
        { "block_size", 131072 },
        { "bitmap_granularity", 4096 },
    });
    f.connect_peer(2);
    f.connect_peer(3);
    f.complete_peering_empty();
    assert(f.pg(1, 1).state == PG_ACTIVE);

    uint64_t client_id = f.connect_client();
    uint64_t op_id = 1;
    auto ec_write = [&](inode_t inode)
    {
        auto *wr = make_write_op(inode, 0, 256*1024, 0xab);
        wr->client_id = client_id;
        wr->req.hdr.id = ++op_id;
        f.exec(wr);
        f.bs_zero_read_ok(0);
        f.bs_write_ok(BS_OP_WRITE, 1);
        for (osd_num_t p = 2; p <= 3; p++)
        {
            auto *pw = f.peer_take(p, OSD_OP_SEC_WRITE);
            pw->reply.sec_rw.version = 1;
            f.peer_complete(pw, pw->req.sec_rw.len);
        }
    };
    ec_write(INODE_WITH_POOL(1, 1));
    assert(f.unstable_write_count_entries() == 3);

    auto *sync = new osd_op_t();
    sync->op_type = OSD_OP_IN;
    sync->client_id = client_id;
    sync->req.hdr.magic = SECONDARY_OSD_OP_MAGIC;
    sync->req.hdr.id = ++op_id;
    sync->req.hdr.opcode = OSD_OP_SYNC;
    f.exec(sync);
    assert(f.stabilize_ref_count() == 1);

    // A new write while the SYNC is in flight - its version isn't in the snapshot
    ec_write(INODE_WITH_POOL(1, 2));
    assert(f.unstable_write_count_entries() == 3);

    // Delete of the fresh object must not wait: purge removes its entries, refs don't cover it
    int delete_retval = -1;
    auto *del = new osd_op_t();
    del->op_type = OSD_OP_IN;
    del->client_id = 0;
    del->req.rw.header.magic = SECONDARY_OSD_OP_MAGIC;
    del->req.rw.header.id = ++op_id;
    del->req.rw.header.opcode = OSD_OP_DELETE;
    del->req.rw.inode = INODE_WITH_POOL(1, 2);
    del->req.rw.offset = 0;
    del->req.rw.len = 0;
    del->callback = [&delete_retval](osd_op_t *op) { delete_retval = op->reply.hdr.retval; };
    f.exec(del);
    f.pump();
    // It wasn't blocked: answer the local fact read and it moves on with no wait
    // record (the peer SEC_READs are answered by the mock)
    f.bs_zero_read_ok(1);
    assert(f.dels_awaiting_sync_count() == 0);
    // Its unstable entries were purged and, unlike a covered delete, it moved on
    assert(f.unstable_write_count_entries() == 0);
    {
        auto *bd = f.bs->take(BS_OP_DELETE);
        bd->version = 2;
        bd->retval = 0;
        bd->callback(bd);
        f.peer_write_ok(2, OSD_OP_SEC_DELETE, 2);
        f.peer_write_ok(3, OSD_OP_SEC_DELETE, 2);
    }
    assert(delete_retval == 0);
    delete del;

    // Finish the SYNC - it stabilizes only the first object
    {
        auto *local = f.bs->take(BS_OP_SYNC);
        local->retval = 0;
        local->callback(local);
        for (osd_num_t p = 2; p <= 3; p++)
        {
            auto *ps = f.peer_take(p, OSD_OP_SEC_SYNC);
            f.peer_complete(ps, 0);
        }
        auto *local_st = f.bs->take(BS_OP_STABLE);
        local_st->retval = 0;
        local_st->callback(local_st);
        for (osd_num_t p = 2; p <= 3; p++)
        {
            auto *pst = f.peer_take(p, OSD_OP_SEC_STABILIZE);
            f.peer_complete(pst, 0);
        }
    }
    assert(sync->reply.hdr.retval == 0);
    assert(f.stabilize_ref_count() == 0);

    printf("test_delete_waits_only_for_covered_objects passed\n");
}

// Test that add_object_to_set properly distinguishes healthy, partially corrupted,
// and all-corrupted objects in replicated pools without false positives or false negatives.
void test_replicated_add_object_to_set_degraded()
{
    printf("test_replicated_add_object_to_set_degraded\n");

    osd_test_fixture_t f;
    f.configure_replicated_pool(/*pool_id*/ 1, /*pg_size*/ 3, /*pg_minsize*/ 1, /*pg_count*/ 1,
        { { 1, 2, 3 } });
    f.start(json11::Json::object {
        { "osd_num", 1 },
        { "etcd_address", "127.0.0.1:2379" },
        { "immediate_commit", "all" },
        { "block_size", 131072 },
        { "bitmap_granularity", 4096 },
        { "no_recovery", true },
    });
    f.connect_peer(2);
    f.connect_peer(3);
    f.complete_peering_empty();

    auto & pg = f.pg(1, 1);
    assert(pg.state & PG_ACTIVE);
    assert(pg.scheme == POOL_SCHEME_REPLICATED);
    assert(pg.pg_size == 3);
    assert(pg.pg_cursize == 3);

    inode_t inode = INODE_WITH_POOL(1, 1);
    object_id oid_clean = { .inode = inode, .stripe = 0 };
    object_id oid_corrupted = { .inode = inode, .stripe = 4096 };

    // Case 1: All 3 replicas healthy and correctly placed.
    // Under the old bug (comparing n_roles < pg.pg_cursize), n_roles was set to 1
    // in replicated mode so 1 < 3 evaluated to true, incorrectly marking healthy
    // objects as OBJ_DEGRADED.
    // With the fix, a healthy object must evaluate clean (NULL state).
    pg_osd_set_t set_clean = {
        { .role = 0, .osd_num = 1, .loc_bad = 0 },
        { .role = 0, .osd_num = 2, .loc_bad = 0 },
        { .role = 0, .osd_num = 3, .loc_bad = 0 },
    };
    pg_osd_set_state_t *st_clean = f.add_object_to_set(pg, oid_clean, set_clean, pg.state, 0);
    assert(st_clean == NULL);
    assert(pg.degraded_objects.count(oid_clean) == 0);
    assert(!(pg.state & PG_HAS_DEGRADED));

    // Case 2: 3 replicas present on OSDs 1, 2, 3, but 1 replica is LOC_CORRUPTED.
    // If n_copies is used without subtracting n_corrupted, n_copies is 3 and
    // 3 < 3 evaluates to false, failing to mark the object as OBJ_DEGRADED and
    // falsely placing it in misplaced_objects instead of degraded_objects.
    // With (n_copies - n_corrupted), 3 - 1 = 2 < 3 evaluates to true, so:
    // - The object is marked OBJ_DEGRADED | OBJ_CORRUPTED
    // - pg.state has PG_HAS_DEGRADED and PG_HAS_CORRUPTED
    // - The object is tracked in pg.degraded_objects, not misplaced_objects
    pg_osd_set_t set_corrupted = {
        { .role = 0, .osd_num = 1, .loc_bad = 0 },
        { .role = 0, .osd_num = 2, .loc_bad = 0 },
        { .role = 0, .osd_num = 3, .loc_bad = LOC_CORRUPTED },
    };
    pg_osd_set_state_t *st_corrupted = f.add_object_to_set(pg, oid_corrupted, set_corrupted, pg.state, 0);
    assert(st_corrupted != NULL);
    assert(st_corrupted->state == (OBJ_DEGRADED | OBJ_CORRUPTED));
    assert(pg.state & PG_HAS_DEGRADED);
    assert(pg.state & PG_HAS_CORRUPTED);
    assert(pg.degraded_objects.count(oid_corrupted) == 1);
    assert(pg.misplaced_objects.count(oid_corrupted) == 0);

    // Case 3: All 3 replicas corrupted.
    // n_roles remains 0 < pg_data_size (1), must be marked OBJ_INCOMPLETE | OBJ_CORRUPTED.
    object_id oid_all_corrupted = { .inode = inode, .stripe = 8192 };
    pg_osd_set_t set_all_corrupted = {
        { .role = 0, .osd_num = 1, .loc_bad = LOC_CORRUPTED },
        { .role = 0, .osd_num = 2, .loc_bad = LOC_CORRUPTED },
        { .role = 0, .osd_num = 3, .loc_bad = LOC_CORRUPTED },
    };
    pg_osd_set_state_t *st_all_corrupted = f.add_object_to_set(pg, oid_all_corrupted, set_all_corrupted, pg.state, 0);
    assert(st_all_corrupted != NULL);
    assert(st_all_corrupted->state == (OBJ_INCOMPLETE | OBJ_CORRUPTED));
    assert(pg.state & PG_HAS_INCOMPLETE);
    assert(pg.incomplete_objects.count(oid_all_corrupted) == 1);

    printf("test_replicated_add_object_to_set_degraded passed\n");
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
    test_ec_degraded_read_retry_resets_parity();
    test_repeer_forgets_unstable_writes(false);
    test_repeer_forgets_unstable_writes(true);
    for (int i = 0; test_flush_error_pg_repeer(i); i++) {}
    test_flush_error_cancels_waiting_write();
    test_peer_death_during_flush_cancels_waiting_write();
    test_flush_error_on_stopping_pg();
    test_repeer_between_flush_batches_cancels_waiting_write();
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
    test_repeated_partial_write_unmarks_outdated();
    test_partial_write_to_misplaced_object();
    test_partial_write_object_state_refs();
    test_repeated_delete_is_noop();
    test_read_enoent_waits_behind_running_delete();
    test_read_enoent_without_racing_op_marks_corrupted();
    test_peering_outdated_copy_other_role();
    test_pg_epoch_base_is_reported_epoch();
    test_pg_epoch_bump_above_reported_epoch();
    test_chained_read_large_chain_info();
    test_peering_partial_unstable_ec_rolls_back();
    test_delete_waits_for_stabilize_snapshot();
    test_delete_waits_only_for_covered_objects();
    test_replicated_add_object_to_set_degraded();
    return 0;
}
