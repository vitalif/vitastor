// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <malloc.h>
#include "str_util.h"
#include "ringloop_mock.h"
#include "blockstore_impl.h"

struct bs_test_t
{
    blockstore_config_t config;
    disk_mock_t *data_disk = NULL;
    disk_mock_t *meta_disk = NULL;
    std::function<bool(io_uring_sqe*)> sqe_handler;
    ring_loop_mock_t *ringloop = NULL;
    timerfd_manager_t *tfd = NULL;
    blockstore_impl_t *bs = NULL;

    ~bs_test_t()
    {
        destroy();
    }

    void destroy_bs()
    {
        if (bs)
        {
            delete bs;
            bs = NULL;
        }
    }

    void destroy()
    {
        while (bs && !bs->is_safe_to_stop())
            ringloop->loop();
        destroy_bs();
        if (tfd)
        {
            delete tfd;
            tfd = NULL;
        }
        if (meta_disk)
        {
            delete meta_disk;
            meta_disk = NULL;
        }
        if (data_disk)
        {
            delete data_disk;
            data_disk = NULL;
        }
        if (ringloop)
        {
            delete ringloop;
            ringloop = NULL;
        }
    }

    void force_compaction()
    {
        bs->flusher->dump_diagnostics();
        bs->flusher->request_trim();
        while (bs->heap->get_compact_queue_size())
            ringloop->loop();
        while (bs->flusher->is_active())
            ringloop->loop();
        bs->flusher->release_trim();
        // Check that compaction succeeded
        assert(!bs->heap->get_to_compact_count());
    }

    void default_cfg()
    {
        config["data_device"] = "./test_data.bin";
        config["data_device_size"] = "1073741824";
        config["data_device_sect"] = "4096";
        config["meta_offset"] = "0";
        config["journal_offset"] = "16777216";
        config["data_offset"] = "33554432";
        config["disable_data_fsync"] = "1";
        config["immediate_commit"] = "all";
        config["log_level"] = "10";
        config["data_csum_type"] = "crc32c";
        config["csum_block_size"] = "4096";
        config["meta_format"] = "3";
    }

    void init()
    {
        if (!ringloop)
        {
            ringloop = new ring_loop_mock_t(RINGLOOP_DEFAULT_SIZE, [&](io_uring_sqe *sqe)
            {
                if (sqe_handler && sqe_handler(sqe))
                {
                }
                else if (sqe->fd == MOCK_DATA_FD)
                {
                    bool ok = data_disk->submit(sqe);
                    assert(ok);
                    ringloop->mark_completed((ring_data_t*)sqe->user_data);
                }
                else if (sqe->fd == MOCK_META_FD)
                {
                    bool ok = meta_disk->submit(sqe);
                    assert(ok);
                    ringloop->mark_completed((ring_data_t*)sqe->user_data);
                }
                else
                {
                    assert(0);
                }
            });
        }
        if (!tfd)
        {
            tfd = new timerfd_manager_t(nullptr);
        }
        if (!data_disk)
        {
            data_disk = new disk_mock_t("data disk", parse_size(config["data_device_size"]), config["disable_data_fsync"] != "1");
            data_disk->clear(0, parse_size(config["data_offset"]));
        }
        uint64_t meta_size = parse_size(config["meta_device_size"]);
        if (meta_size && !meta_disk)
        {
            meta_disk = new disk_mock_t("meta disk", meta_size, config["disable_meta_fsync"] != "1");
            meta_disk->clear(0, meta_size);
        }
        if (!bs)
        {
            bs = new blockstore_impl_t(config, ringloop, tfd, true);
            while (!bs->is_started())
                ringloop->loop();
            printf("blockstore initialized\n");
        }
    }

    void exec_op(blockstore_op_t *op, bool print = true)
    {
        bool done = false;
        op->callback = [&](blockstore_op_t *op)
        {
            if (print)
                printf("op opcode=%ju completed retval=%jd\n", op->opcode, op->retval);
            done = true;
        };
        bs->enqueue_op(op);
        while (!done)
            ringloop->loop();
        op->callback = nullptr;
    }
};

static void test_simple()
{
    printf("\n-- test_simple\n");

    bs_test_t test;
    test.default_cfg();
    test.init();

    // Write
    blockstore_op_t op;
    uint64_t version = 0;
    op.opcode = BS_OP_WRITE;
    op.oid = { .inode = 1, .stripe = 0 };
    op.version = 1;
    op.offset = 16384;
    op.len = 4096;
    op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 128*1024);
    memset(op.buf, 0xaa, 4096);
    test.exec_op(&op);
    assert(op.retval == op.len);

    // Sync
    printf("version %ju written, syncing\n", op.version);
    version = op.version;
    op.opcode = BS_OP_SYNC;
    test.exec_op(&op);
    assert(op.retval == 0);

    // Commit
    printf("commit version %ju\n", version);
    op.opcode = BS_OP_STABLE;
    op.len = 1;
    *((obj_ver_id*)op.buf) = {
        .oid = { .inode = 1, .stripe = 0 },
        .version = version,
    };
    test.exec_op(&op);
    assert(op.retval == 0);

    // Read
    printf("reading 0-128K\n");
    op.opcode = BS_OP_READ;
    op.oid = { .inode = 1, .stripe = 0 };
    op.version = UINT64_MAX;
    op.offset = 0;
    op.len = 128*1024;
    test.exec_op(&op);
    assert(op.retval == op.len);
    assert(op.version == 1);
    uint8_t *cmp = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 128*1024);
    memset(cmp, 0, 128*1024);
    memset(cmp+16384, 0xaa, 4096);
    if (memcmp(op.buf, cmp, 128*1024) == 0)
        printf("read successful\n");
    else
    {
        printf("read returned incorrect data\n");
        abort();
    }

    // Zero-length read
    printf("reading 0-0\n");
    op.version = UINT64_MAX;
    op.offset = 0;
    op.len = 0;
    test.exec_op(&op);
    assert(op.retval == op.len);
    assert(op.version == 1);

    // Small read
    printf("reading 16K-24K\n");
    op.version = UINT64_MAX;
    op.offset = 16*1024;
    op.len = 8*1024;
    test.exec_op(&op);
    assert(op.retval == op.len);
    assert(!memcmp(op.buf, cmp+16*1024, 8*1024));

    free(cmp);

    free(op.buf);
}

static void test_fsync(bool separate_meta)
{
    printf("\n-- test_fsync%s\n", separate_meta ? " separate_meta" : "");

    bs_test_t test;
    test.default_cfg();
    test.config["disable_data_fsync"] = "0";
    test.config["immediate_commit"] = "none";
    if (separate_meta)
    {
        test.config["meta_device"] = "./test_meta.bin";
        test.config["disable_meta_fsync"] = "0";
        test.config["meta_device_size"] = "33554432";
        test.config["meta_device_sect"] = "4096";
        test.config["data_offset"] = "0";
    }
    test.init();
    if (test.meta_disk)
        test.meta_disk->trace = 1;

    // Write
    printf("writing 16K+4K v1\n");
    blockstore_op_t op;
    op.opcode = BS_OP_WRITE_STABLE;
    op.oid = { .inode = 1, .stripe = 0 };
    op.version = 1;
    op.offset = 16384;
    op.len = 4096;
    op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 4096);
    memset(op.buf, 0xaa, 4096);
    test.exec_op(&op);
    assert(op.retval == op.len);

    // Destroy and restart without sync
    printf("destroying\n");
    test.destroy_bs();
    if (separate_meta)
        test.meta_disk->discard_buffers(true, 0);
    else
        test.data_disk->discard_buffers(true, 0);
    test.init();

    // Check ENOENT
    printf("checking for ENOENT\n");
    blockstore_op_t op2;
    op2.opcode = BS_OP_READ;
    op2.oid = { .inode = 1, .stripe = 0 };
    op2.version = UINT64_MAX;
    op2.offset = 0;
    op2.len = 128*1024;
    op2.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 128*1024);
    test.exec_op(&op2);
    assert(op2.retval == -ENOENT);

    // Write again
    printf("writing again\n");
    test.exec_op(&op);
    assert(op.retval == op.len);

    // Sync
    printf("version %ju written, syncing\n", op.version);
    op.opcode = BS_OP_SYNC;
    test.exec_op(&op);
    assert(op.retval == 0);

    // Discard and restart again
    printf("destroying again\n");
    test.destroy_bs();
    if (separate_meta)
        test.meta_disk->discard_buffers(true, 0);
    else
        test.data_disk->discard_buffers(true, 0);
    test.init();

    // Check that it's present now
    printf("checking for OK\n");
    op2.version = UINT64_MAX;
    test.exec_op(&op2);
    assert(op2.retval == op2.len);
    assert(is_zero(op2.buf, 16*1024));
    assert(memcmp(op2.buf+16*1024, op.buf, 4*1024) == 0);
    assert(is_zero(op2.buf+20*1024, 108*1024));

    // Check fsync during compaction - do a small write
    printf("writing 20K+4K v2\n");
    op.opcode = BS_OP_WRITE_STABLE;
    op.oid = { .inode = 1, .stripe = 0 };
    op.version = 2;
    op.offset = 20*1024;
    op.len = 4096;
    memset(op.buf, 0xab, 4096);
    test.exec_op(&op);
    assert(op.retval == op.len);

    op.opcode = BS_OP_SYNC;
    test.exec_op(&op);
    assert(op.retval == 0);

    // Check it by a read op
    op2.version = UINT64_MAX;
    test.exec_op(&op2);
    assert(op2.retval == op2.len);
    assert(is_zero(op2.buf, 16*1024));
    assert(memcheck(op2.buf+16*1024, 0xaa, 4*1024));
    assert(memcheck(op2.buf+20*1024, 0xab, 4*1024));
    assert(is_zero(op2.buf+24*1024, 104*1024));

    // Trigger & wait compaction
    test.force_compaction();

    // Restart and check data again
    test.destroy_bs();
    test.data_disk->discard_buffers(true, 0);
    test.init();

    op2.version = UINT64_MAX;
    test.exec_op(&op2);
    assert(op2.retval == op2.len);
    assert(is_zero(op2.buf, 16*1024));
    assert(memcheck(op2.buf+16*1024, 0xaa, 4*1024));
    assert(memcheck(op2.buf+20*1024, 0xab, 4*1024)); // <- would be lost without data device fsync
    assert(is_zero(op2.buf+24*1024, 104*1024));

    free(op.buf);
    free(op2.buf);
}

static void test_fsync_meta_before_complete()
{
    printf("\n-- test_fsync_meta_before_complete\n");

    bs_test_t test;
    test.default_cfg();
    test.config["disable_data_fsync"] = "0";
    test.config["immediate_commit"] = "none";
    test.config["meta_device"] = "./test_meta.bin";
    test.config["disable_meta_fsync"] = "0";
    test.config["meta_device_size"] = "33554432";
    test.config["meta_device_sect"] = "4096";
    test.config["data_offset"] = "0";
    test.init();
    test.meta_disk->trace = 1;

    // Write
    printf("writing 16K+4K v1\n");
    blockstore_op_t op;
    op.opcode = BS_OP_WRITE_STABLE;
    op.oid = { .inode = 1, .stripe = 0 };
    op.version = 1;
    op.offset = 16384;
    op.len = 4096;
    op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 4096);
    memset(op.buf, 0xaa, 4096);

    // Block metadata write execution
    uint64_t mod_off = 0;
    void *mod_buf = NULL;
    ring_data_t *mod_data = NULL;
    test.sqe_handler = [&](io_uring_sqe *sqe)
    {
        if (sqe->fd == MOCK_META_FD && sqe->opcode == IORING_OP_WRITEV)
        {
            assert(sqe->len == 1);
            mod_off = sqe->off;
            mod_buf = ((iovec*)sqe->addr)[0].iov_base;
            mod_data = (ring_data_t*)sqe->user_data;
            assert(((iovec*)sqe->addr)[0].iov_len == test.bs->dsk.meta_block_size);
            return true;
        }
        return false;
    };

    bool wr_done = false;
    op.callback = [&](blockstore_op_t *op)
    {
        printf("op opcode=%ju completed retval=%jd\n", op->opcode, op->retval);
        wr_done = true;
    };
    test.bs->enqueue_op(&op);

    while (!mod_buf)
        test.ringloop->loop();

    // Sync
    printf("syncing\n");
    blockstore_op_t sync_op;
    sync_op.opcode = BS_OP_SYNC;
    test.exec_op(&sync_op);
    assert(sync_op.retval == 0);

    // Now send the metadata write
    {
        ring_data_t data;
        io_uring_sqe sqe;
        iovec iov;
        sqe.opcode = IORING_OP_WRITEV;
        sqe.off = test.bs->dsk.meta_offset + mod_off;
        sqe.addr = (uint64_t)&iov;
        sqe.len = 1;
        sqe.rw_flags = 0;
        iov.iov_base = mod_buf;
        iov.iov_len = test.bs->dsk.meta_block_size;
        sqe.user_data = (uint64_t)mod_data;
        bool ok = test.meta_disk->submit(&sqe);
        assert(ok);
        test.ringloop->mark_completed(mod_data);
    }

    while (!wr_done)
        test.ringloop->loop();
    assert(op.retval == op.len);

    // Sync again
    printf("syncing again\n");
    test.exec_op(&sync_op);
    assert(sync_op.retval == 0);

    // Discard and restart
    printf("destroying again\n");
    test.destroy_bs();
    test.meta_disk->discard_buffers(true, 0);
    test.init();

    // Check that the write isn't lost
    printf("checking for OK\n");
    blockstore_op_t op2;
    op2.opcode = BS_OP_READ;
    op2.oid = { .inode = 1, .stripe = 0 };
    op2.version = UINT64_MAX;
    op2.offset = 0;
    op2.len = 128*1024;
    op2.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 128*1024);
    test.exec_op(&op2);
    assert(op2.retval == op2.len);
    assert(is_zero(op2.buf, 16*1024));
    assert(memcmp(op2.buf+16*1024, op.buf, 4*1024) == 0);
    assert(is_zero(op2.buf+20*1024, 108*1024));

    free(op.buf);
    free(op2.buf);
}

static void test_intent_over_unstable()
{
    printf("\n-- test_intent_over_unstable\n");

    bs_test_t test;
    test.default_cfg();
    test.init();

    // Write
    printf("writing\n");
    blockstore_op_t op;
    op.opcode = BS_OP_WRITE;
    op.oid = { .inode = 1, .stripe = 0 };
    op.version = 1;
    op.offset = 20480;
    op.len = 4096;
    op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 4096);
    memset(op.buf, 0xaa, 4096);
    test.exec_op(&op);
    assert(op.retval == op.len);

    // Write again
    printf("writing again\n");
    op.version = 2;
    op.offset = 28*1024;
    test.exec_op(&op);
    assert(op.retval == op.len);

    free(op.buf);
}

static void test_padded_csum_intent(bool perfect)
{
    printf("\n-- test_padded_csum_intent%s\n", perfect ? " perfect_csum_update" : "");

    bs_test_t test;
    test.default_cfg();
    test.config["csum_block_size"] = "16384";
    if (perfect)
        test.config["perfect_csum_update"] = "1";
    test.init();

    // Write
    printf("writing\n");
    blockstore_op_t op;
    op.opcode = BS_OP_WRITE_STABLE;
    op.oid = { .inode = 1, .stripe = 0 };
    op.version = 1;
    op.offset = 8192;
    op.len = 4096;
    op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 4096);
    memset(op.buf, 0xaa, 4096);
    test.exec_op(&op);
    assert(op.retval == op.len);

    // Read
    printf("reading\n");
    blockstore_op_t op2;
    op2.opcode = BS_OP_READ;
    op2.oid = { .inode = 1, .stripe = 0 };
    op2.version = UINT64_MAX;
    op2.offset = 0;
    op2.len = 128*1024;
    op2.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 128*1024);
    test.exec_op(&op2);
    assert(op2.retval == op2.len);
    assert(is_zero(op2.buf, 8*1024));
    assert(memcmp(op2.buf+8*1024, op.buf, 4*1024) == 0);
    assert(is_zero(op2.buf+12*1024, 116*1024));

    // Write again (intent if not "perfect")
    printf("writing (%s)\n", perfect ? "small" : "intent");
    op.version = 2;
    op.offset = 28*1024;
    memset(op.buf, 0xbb, 4096);
    test.exec_op(&op);
    assert(op.retval == op.len);

    // Check that these are really big+intent writes
    // (intent is not collapsible because of csum_block_size > bitmap_granularity)
    heap_entry_t *obj = test.bs->heap->read_entry((object_id){ .inode = 1, .stripe = 0 });
    auto wr = obj;
    assert(wr);
    assert(wr->entry_type == (perfect ? BS_HEAP_SMALL_WRITE : BS_HEAP_INTENT_WRITE) | BS_HEAP_STABLE);
    wr = test.bs->heap->prev(wr);
    assert(wr);
    assert(wr->entry_type == BS_HEAP_BIG_WRITE|BS_HEAP_STABLE);
    assert(!test.bs->heap->prev(wr));

    // Trigger & wait compaction
    test.bs->flusher->dump_diagnostics();
    test.bs->flusher->request_trim();
    while (test.bs->heap->get_compact_queue_size())
        test.ringloop->loop();
    while (test.bs->flusher->is_active())
        test.ringloop->loop();
    test.bs->flusher->release_trim();
    // Check that compaction succeeded
    assert(!test.bs->heap->get_to_compact_count());

    // Read again and check
    printf("reading compacted\n");
    op2.version = UINT64_MAX;
    test.exec_op(&op2);
    assert(op2.retval == op2.len);
    assert(memcheck(op2.buf, 0, 8*1024));
    assert(memcheck(op2.buf+8*1024, 0xaa, 4*1024));
    assert(memcheck(op2.buf+12*1024, 0, 16*1024));
    assert(memcheck(op2.buf+28*1024, 0xbb, 4*1024));
    assert(memcheck(op2.buf+32*1024, 0, 96*1024));

    obj = test.bs->heap->read_entry((object_id){ .inode = 1, .stripe = 0 });
    assert(test.bs->heap->prev(obj) && test.bs->heap->prev(obj)->is_garbage());

    free(op.buf);
    free(op2.buf);
}

static void test_perfect_csum_interrupted()
{
    printf("\n-- test_perfect_csum_interrupted\n");

    bs_test_t test;
    test.default_cfg();
    test.config["csum_block_size"] = "16384";
    test.config["perfect_csum_update"] = "1";
    test.config["disable_meta_fsync"] = "1";
    test.config["meta_device"] = "./test_meta.bin";
    test.config["meta_device_size"] = "33554432";
    test.config["meta_device_sect"] = "4096";
    test.config["data_offset"] = "0";
    test.init();

    // Write
    printf("writing\n");
    blockstore_op_t op;
    op.opcode = BS_OP_WRITE_STABLE;
    op.oid = { .inode = 1, .stripe = 0 };
    op.version = 1;
    op.offset = 16*1024;
    op.len = 12*1024;
    op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 12*1024);
    memset(op.buf, 0xaa, 12*1024);
    test.exec_op(&op);
    assert(op.retval == op.len);

    // Write again
    printf("writing (small)\n");
    op.version = 2;
    op.offset = 20*1024;
    op.len = 4*1024;
    memset(op.buf, 0xbb, 4096);
    test.exec_op(&op);
    assert(op.retval == op.len);

    // Trigger & block compaction after punch_holes
    bool modified = false;
    test.sqe_handler = [&](io_uring_sqe *sqe)
    {
        if (sqe->fd == MOCK_META_FD && sqe->opcode == IORING_OP_WRITEV)
        {
            bool ok = test.meta_disk->submit(sqe);
            assert(ok);
            modified = true;
            return true;
        }
        return false;
    };
    test.bs->flusher->request_trim();
    while (!modified)
        test.ringloop->loop();
    test.destroy_bs();
    test.init();

    // Read and check
    printf("rechecking reloaded\n");
    blockstore_op_t op2;
    op2.opcode = BS_OP_READ;
    op2.oid = { .inode = 1, .stripe = 0 };
    op2.version = UINT64_MAX;
    op2.offset = 0;
    op2.len = 128*1024;
    op2.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 128*1024);
    test.exec_op(&op2);
    assert(op2.retval == op2.len);
    assert(memcheck(op2.buf, 0, 16*1024));
    assert(memcheck(op2.buf+16*1024, 0xaa, 4*1024));
    assert(memcheck(op2.buf+20*1024, 0xbb, 4*1024));
    assert(memcheck(op2.buf+24*1024, 0xaa, 4*1024));
    assert(memcheck(op2.buf+28*1024, 0, 100*1024));

    free(op.buf);
    free(op2.buf);
}

static void test_padded_csum_parallel_read(bool perfect, uint32_t offset)
{
    printf("\n-- test_padded_csum_parallel_read%s offset=%u\n", perfect ? " perfect_csum_update" : "", offset);

    bs_test_t test;
    test.default_cfg();
    test.config["csum_block_size"] = "16384";
    test.config["atomic_write_size"] = "0";
    if (perfect)
        test.config["perfect_csum_update"] = "1";
    test.init();

    // Write
    printf("writing (initial)\n");
    blockstore_op_t op;
    op.opcode = BS_OP_WRITE_STABLE;
    op.oid = { .inode = 1, .stripe = 0 };
    op.version = 1;
    op.offset = 8192;
    op.len = 16384;
    op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 16384);
    memset(op.buf, 0xaa, 16384);
    test.exec_op(&op);
    assert(op.retval == op.len);

    // Write 2
    printf("writing (%u+%u)\n", offset, 4096);
    op.version = 2;
    op.offset = offset;
    op.len = 4096;
    memset(op.buf, 0xbb, 4096);
    test.exec_op(&op);
    assert(op.retval == op.len);

    // Trigger & wait compaction
    test.bs->flusher->request_trim();
    std::vector<ring_data_t*> flush_writes;
    test.sqe_handler = [&](io_uring_sqe *sqe)
    {
        if (sqe->fd == MOCK_DATA_FD && sqe->opcode == IORING_OP_WRITEV &&
            sqe->off >= test.bs->dsk.data_offset)
        {
            bool ok = test.data_disk->submit(sqe);
            assert(ok);
            flush_writes.push_back((ring_data_t*)sqe->user_data);
            return true;
        }
        return false;
    };
    // Wait for 2 flusher writes, execute and pause them
    while (test.bs->heap->get_compact_queue_size() && flush_writes.size() < 1)
        test.ringloop->loop();
    while (test.bs->flusher->is_active() && flush_writes.size() < 1)
        test.ringloop->loop();
    // Run a read operation in parallel - it shouldn't complain about checksum errors
    printf("reading in parallel\n");
    blockstore_op_t op2;
    op2.opcode = BS_OP_READ;
    op2.oid = { .inode = 1, .stripe = 0 };
    op2.version = 1;
    op2.offset = 0;
    op2.len = 128*1024;
    op2.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 128*1024);
    test.exec_op(&op2);
    assert(op2.retval == op2.len);
    // Continue flushing
    test.sqe_handler = NULL;
    for (auto & w: flush_writes)
        test.ringloop->mark_completed(w);
    flush_writes.clear();
    while (test.bs->heap->get_compact_queue_size() && flush_writes.size() < 2)
        test.ringloop->loop();
    while (test.bs->flusher->is_active() && flush_writes.size() < 2)
        test.ringloop->loop();
    test.bs->flusher->release_trim();
    // Check that compaction succeeded
    assert(!test.bs->heap->get_to_compact_count());

    free(op.buf);
    free(op2.buf);
}

static void test_compact_rollback()
{
    printf("\n-- test_compact_rollback\n");

    bs_test_t test;
    test.default_cfg();
    test.config["csum_block_size"] = "16384";
    test.config["atomic_write_size"] = "0";
    test.init();

    // Write
    printf("write\n");
    blockstore_op_t op;
    op.opcode = BS_OP_WRITE;
    op.oid = { .inode = 1, .stripe = 0 };
    op.version = 1;
    op.offset = 8192;
    op.len = 16384;
    op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 16384);
    memset(op.buf, 0xaa, 16384);
    test.exec_op(&op);
    assert(op.retval == op.len);

    // Rollback
    printf("rollback\n");
    op.opcode = BS_OP_ROLLBACK;
    op.len = 1;
    ((obj_ver_id*)op.buf)[0] = { .oid = { .inode = 1, .stripe = 0 }, .version = 0 };
    test.exec_op(&op);
    assert(op.retval == 0);

    // Trigger & wait compaction
    test.force_compaction();

    // Check that the object does not exist
    printf("checking that the object does not exist\n");
    blockstore_op_t op2;
    op2.opcode = BS_OP_READ;
    op2.oid = { .inode = 1, .stripe = 0 };
    op2.version = 1;
    op2.offset = 0;
    op2.len = 128*1024;
    op2.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 128*1024);
    test.exec_op(&op2);
    assert(op2.retval == -ENOENT);

    free(op.buf);
    free(op2.buf);
}

static void test_fsync_almost_batch_big()
{
    printf("\n-- test_fsync_almost_batch_big\n");

    bs_test_t test;
    test.default_cfg();
    test.config["disable_data_fsync"] = "0";
    test.config["immediate_commit"] = "none";
    test.init();

    int data_writes = 0;
    std::vector<ring_data_t*> fsyncs;
    test.sqe_handler = [&](io_uring_sqe *sqe)
    {
        if (sqe->fd == MOCK_DATA_FD && sqe->opcode == IORING_OP_WRITEV && sqe->off >= test.bs->dsk.data_offset)
        {
            data_writes++;
        }
        else if (sqe->fd == MOCK_DATA_FD && sqe->opcode == IORING_OP_FSYNC)
        {
            // execute fsync immediately but pause completion
            assert(data_writes);
            bool ok = test.data_disk->submit(sqe);
            assert(ok);
            fsyncs.push_back((ring_data_t*)sqe->user_data);
            return true;
        }
        return false;
    };

    printf("sending big_write 1\n");
    bool done1 = false;
    blockstore_op_t op;
    op.opcode = BS_OP_WRITE;
    op.oid = { .inode = 1, .stripe = 0 };
    op.version = 1;
    op.offset = 8192;
    op.len = 16384;
    op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, op.len);
    memset(op.buf, 0xaa, 16384);
    op.callback = [&](blockstore_op_t *op) { done1 = true; };
    test.bs->enqueue_op(&op);
    // Wait for data_write and the first fsync
    while (data_writes < 1 || !fsyncs.size())
        test.ringloop->loop();

    printf("sending big_write 2\n");
    bool done2 = false;
    blockstore_op_t op2;
    op2.opcode = BS_OP_WRITE;
    op2.oid = { .inode = 2, .stripe = 0 };
    op2.version = 1;
    op2.offset = 8192;
    op2.len = 16384;
    op2.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, op.len);
    memset(op.buf, 0xaa, 16384);
    op2.callback = [&](blockstore_op_t *op) { done2 = true; };
    test.bs->enqueue_op(&op2);
    // Wait for the second data_write
    while (data_writes < 2)
        test.ringloop->loop();
    // And loop several more times to continue second data_write
    for (int i = 0; i < 3; i++)
        test.ringloop->loop();

    // Unblock the first fsync and check that the second op didn't complete
    test.ringloop->mark_completed(fsyncs[0]);
    fsyncs.erase(fsyncs.begin());
    for (int iters = 0; !done1 && iters < 1000; iters++)
        test.ringloop->loop();
    assert(done1);
    if (done2)
        printf("BUG: second write is done, but its data is not fsynced\n");
    assert(!done2);

    // And wait for the second fsync
    for (int iters = 0; !fsyncs.size() && iters < 1000; iters++)
        test.ringloop->loop();
    assert(fsyncs.size());
    test.ringloop->mark_completed(fsyncs[0]);
    fsyncs.erase(fsyncs.begin());
    while (!done2)
        test.ringloop->loop();

    test.sqe_handler = nullptr;
    free(op.buf);
    free(op2.buf);
}

static void test_fsync_batch_big()
{
    printf("\n-- test_fsync_batch_big\n");

    bs_test_t test;
    test.default_cfg();
    test.config["disable_data_fsync"] = "0";
    test.config["immediate_commit"] = "none";
    test.init();

    int fsyncs = 0;
    test.sqe_handler = [&](io_uring_sqe *sqe)
    {
        if (sqe->fd == MOCK_DATA_FD && sqe->opcode == IORING_OP_FSYNC)
            fsyncs++;
        return false;
    };

    printf("sending big_write 1\n");
    bool done1 = false;
    blockstore_op_t op;
    op.opcode = BS_OP_WRITE;
    op.oid = { .inode = 1, .stripe = 0 };
    op.version = 1;
    op.offset = 8192;
    op.len = 16384;
    op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, op.len);
    memset(op.buf, 0xaa, 16384);
    op.callback = [&](blockstore_op_t *op) { done1 = true; };
    test.bs->enqueue_op(&op);

    printf("sending big_write 2\n");
    bool done2 = false;
    blockstore_op_t op2;
    op2.opcode = BS_OP_WRITE;
    op2.oid = { .inode = 2, .stripe = 0 };
    op2.version = 1;
    op2.offset = 8192;
    op2.len = 16384;
    op2.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, op.len);
    memset(op.buf, 0xaa, 16384);
    op2.callback = [&](blockstore_op_t *op) { done2 = true; };
    test.bs->enqueue_op(&op2);

    for (int iters = 0; (!done1 || !done2) && iters < 1000; iters++)
        test.ringloop->loop();
    assert(done1 && done2);
    assert(fsyncs == 1);

    test.sqe_handler = nullptr;
    free(op.buf);
    free(op2.buf);
}

static void test_padded_csum_sparse_leading_hole()
{
    printf("\n-- test_padded_csum_sparse_leading_hole\n");

    bs_test_t test;
    test.default_cfg();
    test.config["csum_block_size"] = "16384";
    test.init();

    // Initial write at offset=20K, len=4K on a fresh object.
    // The csum block is [16K..32K); granule [16K..20K) is a leading hole.
    printf("writing 20+4K\n");
    blockstore_op_t op;
    op.opcode = BS_OP_WRITE_STABLE;
    op.oid = { .inode = 1, .stripe = 0 };
    op.version = 1;
    op.offset = 20*1024;
    op.len = 4096;
    op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 4096);
    memset(op.buf, 0xaa, 4096);
    test.exec_op(&op);
    assert(op.retval == op.len);

    // Read it back - without the fix, verify_read_checksums returns -EDOM
    // because the recomputed CRC includes zero-padding for [16K..20K) while
    // the stored CRC was computed only over [20K..24K).
    printf("reading\n");
    blockstore_op_t op2;
    op2.opcode = BS_OP_READ;
    op2.oid = { .inode = 1, .stripe = 0 };
    op2.version = UINT64_MAX;
    op2.offset = 0;
    op2.len = 128*1024;
    op2.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 128*1024);
    test.exec_op(&op2);
    assert(op2.retval == op2.len);
    assert(is_zero(op2.buf, 20*1024));
    assert(memcheck(op2.buf+20*1024, 0xaa, 4*1024));
    assert(is_zero(op2.buf+24*1024, 104*1024));

    // Part 2 - add 2 more small writes
    printf("writing 24+4K\n");
    op.version = 2;
    op.offset = 24*1024;
    test.exec_op(&op);
    assert(op.retval == op.len);

    printf("writing 36+4K\n");
    op.version = 3;
    op.offset = 36*1024;
    test.exec_op(&op);
    assert(op.retval == op.len);

    printf("reading uncompacted\n");
    op2.version = UINT64_MAX;
    test.exec_op(&op2);
    assert(op2.retval == op2.len);
    assert(is_zero(op2.buf, 20*1024));
    assert(memcheck(op2.buf+20*1024, 0xaa, 8*1024));
    assert(is_zero(op2.buf+28*1024, 8*1024));
    assert(memcheck(op2.buf+36*1024, 0xaa, 4*1024));
    assert(is_zero(op2.buf+40*1024, 88*1024));

    // Trigger & wait compaction
    test.force_compaction();

    printf("reading compacted\n");
    op2.version = UINT64_MAX;
    test.exec_op(&op2);
    assert(op2.retval == op2.len);
    assert(is_zero(op2.buf, 20*1024));
    assert(memcheck(op2.buf+20*1024, 0xaa, 8*1024));
    assert(is_zero(op2.buf+28*1024, 8*1024));
    assert(memcheck(op2.buf+36*1024, 0xaa, 4*1024));
    assert(is_zero(op2.buf+40*1024, 88*1024));

    free(op.buf);
    free(op2.buf);
}

static void test_list_limit()
{
    printf("\n-- test_list_limit\n");

    bs_test_t test;
    test.default_cfg();
    test.init();

    void *st = test.bs->reshard_start(1, 1, 0x20000, 0);
    assert(!st);

    printf("writing 1:0 0+4K\n");
    blockstore_op_t op;
    op.opcode = BS_OP_WRITE_STABLE;
    op.oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0 };
    op.version = 1;
    op.offset = 0;
    op.len = 4096;
    op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 4096);
    memset(op.buf, 0xaa, 4096);
    test.exec_op(&op);
    assert(op.retval == op.len);

    printf("writing 1:128K 0+4K\n");
    op.oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 128*1024 };
    op.version = 1;
    test.exec_op(&op);
    assert(op.retval == op.len);

    printf("writing 1:256K 0+4K\n");
    op.oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 256*1024 };
    op.version = 1;
    test.exec_op(&op);
    assert(op.retval == op.len);

    printf("writing 4K+4K unstable\n");
    op.oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0*1024 };
    op.opcode = BS_OP_WRITE;
    op.version = 2;
    op.offset = 4096;
    test.exec_op(&op);
    assert(op.retval == op.len);

    op.opcode = BS_OP_LIST;
    free(op.buf);
    op.buf = NULL;
    op.min_oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0 };
    op.max_oid = { .inode = INODE_WITH_POOL(1, 1000), .stripe = 0 };
    op.pg_alignment = 128*1024;
    op.pg_count = 1;
    op.pg_number = 0;
    op.list_stable_limit = 1;

    test.exec_op(&op);
    assert(op.retval == 2);
    assert(op.version == 1);
    obj_ver_id *lst = (obj_ver_id*)op.buf;
    assert((lst[0] == (obj_ver_id){ .oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0 }, .version = 1 }));
    assert((lst[1] == (obj_ver_id){ .oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0 }, .version = 2 }));

    free(op.buf);
}

static void test_write_no_space_eagain()
{
    printf("\n-- test_write_no_space_eagain\n");

    bs_test_t test;
    test.default_cfg();
    // 1 MB journal (data_offset 32 MB - journal_offset)
    test.config["journal_offset"] = std::to_string(31*1024*1024);
    test.init();
    printf("blockstore initialized\n");

    uint8_t *buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 4096);
    memset(buf, 0xAA, 4096);

    // fill the journal with unstable writes
    int i = 0, blocked_retval = 0;
    auto do_write = [&]()
    {
        blockstore_op_t op;
        op.opcode = BS_OP_WRITE;
        op.oid = { .inode = 1, .stripe = (uint64_t)(i % 4) * 0x20000 };
        op.version = 1 + i / 4;
        op.offset = 0;
        op.len = 4096;
        op.buf = buf;
        test.exec_op(&op, false);
        if (op.retval != (int)op.len)
            blocked_retval = op.retval;
    };
    for (i = 0; i < 100000 && !blocked_retval; i++)
        do_write();
    printf("write #%d refused with retval=%d\n", i, blocked_retval);
    assert(blocked_retval == -EAGAIN);

    // check that a stabilize operation and one more write can proceed
    {
        blockstore_op_t op;
        op.opcode = BS_OP_STABLE;
        op.len = 1;
        op.buf = (uint8_t*)malloc_or_die(1 * sizeof(obj_ver_id));
        ((obj_ver_id*)op.buf)[0] = {
            .oid = { .inode = 1, .stripe = 0 },
            .version = 2,
        };
        test.exec_op(&op);
        assert(op.retval == 0);
        free(op.buf);
    }

    blocked_retval = 0;
    do_write();
    assert(!blocked_retval);

    free(buf);
}

// FIXME Add a simple intent_write / big_intent test

// Entries of one object which aren't durable yet must all live in the same metadata block, so
// that whatever survives a power outage is a prefix of the object's entry chain. When the block
// of the previous entry can't take another one right now - here because it is being written -
// the new write has to wait for it instead of going into another block, otherwise the outage
// may keep the newer entry and drop the older one, and the object comes back at a version
// whose predecessor is missing
static void test_write_stays_in_same_meta_block()
{
    printf("\n-- test_write_stays_in_same_meta_block\n");

    bs_test_t test;
    test.default_cfg();
    // Entries only become durable on an explicit fsync, which is what makes the rule apply
    test.config["disable_data_fsync"] = "0";
    test.config["immediate_commit"] = "none";
    test.config["meta_device"] = "./test_meta.bin";
    test.config["disable_meta_fsync"] = "0";
    test.config["meta_device_size"] = "33554432";
    test.config["meta_device_sect"] = "4096";
    test.config["data_offset"] = "0";
    test.init();

    object_id oid = { .inode = 1, .stripe = 0 };
    uint8_t *buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 128*1024);
    memset(buf, 0xaa, 128*1024);

    // Create the object and make it durable, so that only the two writes below are in question
    printf("writing v1 0+128K and syncing\n");
    blockstore_op_t op;
    op.opcode = BS_OP_WRITE_STABLE;
    op.oid = oid;
    op.version = 1;
    op.offset = 0;
    op.len = 128*1024;
    op.buf = buf;
    test.exec_op(&op);
    assert(op.retval == (int)op.len);
    op.opcode = BS_OP_SYNC;
    test.exec_op(&op);
    assert(op.retval == 0);

    // Record every metadata block write and hold the first one back. The superblock lives at
    // meta_offset itself, the blocks start one block further, and the buffer area shares the
    // same device because no separate journal device is configured
    uint64_t first_block_off = test.bs->dsk.meta_offset + test.bs->dsk.meta_block_size;
    uint64_t meta_end_off = test.bs->dsk.meta_offset + test.bs->dsk.meta_area_size;
    std::vector<uint64_t> meta_writes;
    ring_data_t *held = NULL;
    test.sqe_handler = [&](io_uring_sqe *sqe)
    {
        if (sqe->fd == MOCK_META_FD && sqe->opcode == IORING_OP_WRITEV &&
            sqe->off >= first_block_off && sqe->off < meta_end_off)
        {
            meta_writes.push_back(sqe->off);
            if (!held)
            {
                bool ok = test.meta_disk->submit(sqe);
                assert(ok);
                held = (ring_data_t*)sqe->user_data;
                return true;
            }
        }
        return false;
    };

    printf("writing v2 16K+4K and holding its metadata block write\n");
    blockstore_op_t wr2;
    bool wr2_done = false;
    wr2.opcode = BS_OP_WRITE_STABLE;
    wr2.oid = oid;
    wr2.version = 2;
    wr2.offset = 16384;
    wr2.len = 4096;
    wr2.buf = buf;
    memset(buf, 0xbb, 4096);
    wr2.callback = [&](blockstore_op_t *op) { wr2_done = true; };
    test.bs->enqueue_op(&wr2);
    for (int i = 0; i < 50 && !held; i++)
        test.ringloop->loop();
    assert(held);
    assert(!wr2_done);
    assert(meta_writes.size() == 1);

    // v2 isn't durable and its block is busy, so v3 must not be put into another block
    printf("writing v3 20K+4K - it must wait for v2's block instead of using another one\n");
    blockstore_op_t wr3;
    bool wr3_done = false;
    wr3.opcode = BS_OP_WRITE_STABLE;
    wr3.oid = oid;
    wr3.version = 3;
    wr3.offset = 20480;
    wr3.len = 4096;
    wr3.buf = buf + 4096;
    memset(buf + 4096, 0xcc, 4096);
    wr3.callback = [&](blockstore_op_t *op) { wr3_done = true; };
    test.bs->enqueue_op(&wr3);
    for (int i = 0; i < 50 && !wr3_done; i++)
        test.ringloop->loop();
    assert(!wr3_done);
    assert(meta_writes.size() == 1);

    printf("releasing the held metadata write\n");
    test.ringloop->mark_completed(held);
    for (int i = 0; i < 500 && !(wr2_done && wr3_done); i++)
        test.ringloop->loop();
    assert(wr2_done && wr3_done);
    assert(wr2.retval == (int)wr2.len);
    assert(wr3.retval == (int)wr3.len);

    // Both entries ended up in one and the same metadata block
    printf("metadata block writes: %zu\n", meta_writes.size());
    for (auto off: meta_writes)
        assert(off == meta_writes[0]);

    printf("reading it back\n");
    blockstore_op_t rd;
    rd.opcode = BS_OP_READ;
    rd.oid = oid;
    rd.version = 3;
    rd.offset = 0;
    rd.len = 128*1024;
    rd.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, rd.len);
    test.exec_op(&rd);
    assert(rd.retval == (int)rd.len);
    assert(memcheck(rd.buf, 0xaa, 16384));
    assert(memcheck(rd.buf + 16384, 0xbb, 4096));
    assert(memcheck(rd.buf + 20480, 0xcc, 4096));
    assert(memcheck(rd.buf + 24576, 0xaa, 128*1024 - 24576));
    free(rd.buf);

    test.sqe_handler = nullptr;
    free(buf);
}

// Compaction merges newer writes into the base entry's data block in place, which changes bytes
// that the base entry's checksums still describe. Reads never take those bytes from it, but the
// startup recheck does - and when a checksum block is larger than the write granularity it can't
// skip just the overwritten parts, because one poisoned part invalidates the checksum of the
// whole block. So before overwriting the block the flusher has to persist completed_lsn, which
// puts the base entry out of the recheck's scope entirely. Otherwise a crash in the middle of a
// compaction makes recovery declare the base entry unfinished and drop the whole object
// A stabilize right after a write leaves a commit entry on top of the object's chain. The
// startup recheck looks for buffered small writes whose data may be missing from the disk, and
// it must not stop at that commit - otherwise a write whose data never reached the platter
// stays in the metadata and the object comes back holding whatever the buffer area held before
static void test_recheck_under_commit()
{
    printf("\n-- test_recheck_under_commit\n");

    bs_test_t test;
    test.default_cfg();
    // A volatile write cache is what makes the data of an acknowledged write disappear
    test.config["disable_data_fsync"] = "0";
    test.config["immediate_commit"] = "none";
    test.init();

    object_id oid = { .inode = 1, .stripe = 0 };
    uint8_t *buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 128*1024);

    blockstore_op_t op;
    op.oid = oid;
    op.buf = buf;

    auto sync = [&]()
    {
        blockstore_op_t s;
        s.opcode = BS_OP_SYNC;
        test.exec_op(&s);
        assert(s.retval == 0);
    };
    auto stabilize = [&](uint64_t version)
    {
        blockstore_op_t s;
        s.opcode = BS_OP_STABLE;
        s.len = 1;
        s.buf = (uint8_t*)malloc_or_die(sizeof(obj_ver_id));
        *((obj_ver_id*)s.buf) = (obj_ver_id){ .oid = oid, .version = version };
        test.exec_op(&s);
        assert(s.retval == 0);
        free(s.buf);
    };

    printf("writing v1 0+128K, syncing and stabilizing it\n");
    memset(buf, 0xaa, 128*1024);
    op.opcode = BS_OP_WRITE;
    op.version = 1;
    op.offset = 0;
    op.len = 128*1024;
    test.exec_op(&op);
    assert(op.retval == (int)op.len);
    sync();
    stabilize(1);

    printf("writing v2 4096+4096 and syncing it, without stabilizing yet\n");
    memset(buf, 0xbb, 4096);
    op.version = 2;
    op.offset = 4096;
    op.len = 4096;
    test.exec_op(&op);
    assert(op.retval == (int)op.len);
    sync();

    // Swallow the data of the next write: the disk acknowledges it from its volatile cache
    // and then loses it, while the metadata entry does reach the platter
    uint64_t buf_start = test.bs->dsk.journal_offset;
    uint64_t buf_end = buf_start + test.bs->dsk.journal_len;
    bool swallowed = false;
    test.sqe_handler = [&](io_uring_sqe *sqe)
    {
        if (sqe->opcode == IORING_OP_WRITEV && sqe->fd == MOCK_DATA_FD &&
            sqe->off >= buf_start && sqe->off < buf_end)
        {
            auto *data = (ring_data_t*)sqe->user_data;
            data->res = data->iov.iov_len;
            test.ringloop->mark_completed(data);
            swallowed = true;
            return true;
        }
        return false;
    };

    printf("writing v3 8192+4096, losing its data in the disk cache\n");
    memset(buf, 0xcc, 4096);
    op.version = 3;
    op.offset = 8192;
    op.len = 4096;
    test.exec_op(&op);
    assert(op.retval == (int)op.len);
    assert(swallowed);
    test.sqe_handler = nullptr;

    printf("stabilizing v2 - its commit entry lands on top of v3\n");
    stabilize(2);

    printf("restarting the blockstore\n");
    test.destroy_bs();
    test.ringloop->reset();
    test.init();

    printf("v3 must be gone and the object must be readable as v2\n");
    blockstore_op_t rd;
    rd.opcode = BS_OP_READ;
    rd.oid = oid;
    rd.version = UINT64_MAX;
    rd.offset = 0;
    rd.len = 128*1024;
    rd.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, rd.len);
    test.exec_op(&rd);
    assert(rd.retval == (int)rd.len);
    assert(rd.version == 2);
    assert(memcheck(rd.buf, 0xaa, 4096));
    assert(memcheck(rd.buf + 4096, 0xbb, 4096));
    assert(memcheck(rd.buf + 8192, 0xaa, 128*1024 - 8192));
    free(rd.buf);
    free(buf);
}

// Compaction merges newer writes into the base data block in place, so after a crash the base
// entry's checksums no longer match the block. The startup recheck may only skip the parts which
// newer entries overwrite when one checksum block covers exactly one write granule. With a larger
// block - and with checksums disabled, where a big_intent still carries a single checksum of its
// whole extent - it can't, so the flusher has to persist completed_lsn before touching the block
static void test_compact_big_intent_survives_crash(uint32_t csum_block)
{
    printf("\n-- test_compact_big_intent_survives_crash(csum_block=%u)\n", csum_block);

    bs_test_t test;
    test.default_cfg();
    if (csum_block)
        test.config["csum_block_size"] = std::to_string(csum_block);
    else
    {
        test.config["data_csum_type"] = "none";
        test.config.erase("csum_block_size");
    }
    // A big write becomes a redirect intent - and thus a big_intent entry - only when the data
    // fsync is enabled and the write is stable
    test.config["disable_data_fsync"] = "0";
    test.config["immediate_commit"] = "none";
    test.init();

    object_id oid = { .inode = 1, .stripe = 0 };
    uint8_t *buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 128*1024);
    memset(buf, 0xaa, 128*1024);

    blockstore_op_t op;
    op.opcode = BS_OP_WRITE_STABLE;
    op.oid = oid;
    op.buf = buf;

    printf("writing v1 0+128K (becomes a big_intent)\n");
    op.version = 1;
    op.offset = 0;
    op.len = 128*1024;
    test.exec_op(&op);
    assert(op.retval == (int)op.len);

    printf("writing v2 and v3 on top of it\n");
    op.version = 2;
    op.offset = 4096;
    op.len = 4096;
    memset(buf, 0xbb, 4096);
    test.exec_op(&op);
    assert(op.retval == (int)op.len);
    op.version = 3;
    op.offset = 20480;
    op.len = 4096;
    memset(buf, 0xcc, 4096);
    test.exec_op(&op);
    assert(op.retval == (int)op.len);

    // Compaction only takes entries which are already fsynced
    op.opcode = BS_OP_SYNC;
    test.exec_op(&op);
    assert(op.retval == 0);

    // Find the base entry - the one whose data block compaction is about to overwrite
    heap_entry_t *base = test.bs->heap->read_entry(oid);
    while (base && base->type() != BS_HEAP_BIG_INTENT)
        base = test.bs->heap->prev(base);
    assert(base);
    uint64_t base_loc = base->big_location(test.bs->heap);
    printf("base entry is a big_intent l%ju at data offset %ju\n", base->lsn, base_loc);

    // Let compaction overwrite the base block, then hold its completion so that the compaction
    // never finishes - the block on the disk is already merged, the metadata still isn't
    uint64_t block_start = test.bs->dsk.data_offset + base_loc;
    uint64_t block_end = block_start + test.bs->dsk.data_block_size;
    bool overwritten = false;
    test.sqe_handler = [&](io_uring_sqe *sqe)
    {
        if (sqe->opcode == IORING_OP_WRITEV && sqe->fd == MOCK_DATA_FD &&
            sqe->off >= block_start && sqe->off < block_end)
        {
            bool ok = test.data_disk->submit(sqe);
            assert(ok);
            overwritten = true;
            return true;
        }
        return false;
    };

    printf("compacting, holding it as soon as the base block is overwritten\n");
    test.bs->flusher->request_trim();
    for (int i = 0; i < 1000 && !overwritten; i++)
        test.ringloop->loop();
    assert(overwritten);

    printf("restarting the blockstore as if the power went away mid-compaction\n");
    test.sqe_handler = nullptr;
    test.destroy_bs();
    test.ringloop->reset();
    test.init();

    printf("the object must still be there\n");
    blockstore_op_t rd;
    rd.opcode = BS_OP_READ;
    rd.oid = oid;
    rd.version = UINT64_MAX;
    rd.offset = 0;
    rd.len = 128*1024;
    rd.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, rd.len);
    test.exec_op(&rd);
    assert(rd.retval == (int)rd.len);
    assert(memcheck(rd.buf, 0xaa, 4096));
    assert(memcheck(rd.buf + 4096, 0xbb, 4096));
    assert(memcheck(rd.buf + 8192, 0xaa, 12288));
    assert(memcheck(rd.buf + 20480, 0xcc, 4096));
    assert(memcheck(rd.buf + 24576, 0xaa, 128*1024 - 24576));
    free(rd.buf);
    free(buf);
}

static void test_read_merge_exact_partial_blocks()
{
    printf("\n-- test_read_merge_exact_partial_blocks\n");

    bs_test_t test;
    test.default_cfg();
    test.config["csum_block_size"] = "32768";
    test.config["perfect_csum_update"] = "0";
    test.config["inmemory_journal"] = "0";
    test.init();

    // 1. Initial big write to make the object exist
    blockstore_op_t op;
    op.opcode = BS_OP_WRITE_STABLE;
    op.oid = { .inode = 1, .stripe = 0 };
    op.version = 1;
    op.offset = 0;
    op.len = 128*1024;
    op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 128*1024);
    memset(op.buf, 0x11, 128*1024);
    test.exec_op(&op);
    assert(op.retval == op.len);

    // Flush to make it stable
    test.bs->flusher->request_trim();
    while (test.bs->heap->get_compact_queue_size() || test.bs->flusher->is_active())
        test.ringloop->loop();
    test.bs->flusher->release_trim();

    // 2. Intent write: 16KB ~ 64KB
    blockstore_op_t op2;
    op2.opcode = BS_OP_WRITE_STABLE;
    op2.oid = { .inode = 1, .stripe = 0 };
    op2.version = 2;
    op2.offset = 16384;
    op2.len = 49152;
    op2.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 49152);
    memset(op2.buf, 0x22, 49152);
    test.exec_op(&op2);
    assert(op2.retval == op2.len);

    // 3. Read 28KB ~ 36KB
    int read_count = 0;
    test.sqe_handler = [&](io_uring_sqe *sqe) {
        if (sqe->fd == MOCK_DATA_FD) {
            // Only count reads that fall within our expected disk range (or just any read)
            if (sqe->opcode == IORING_OP_READV || sqe->opcode == IORING_OP_READ) {
                read_count++;
            }
        }
        return false;
    };

    blockstore_op_t op3;
    op3.opcode = BS_OP_READ;
    op3.oid = { .inode = 1, .stripe = 0 };
    op3.version = UINT64_MAX;
    op3.offset = 28672;
    op3.len = 8192;
    op3.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 8192);
    test.exec_op(&op3);
    assert(op3.retval == op3.len);

    // Verify exactly 1 read IO was issued
    assert(read_count == 1);

    test.sqe_handler = nullptr;
    free(op.buf);
    free(op2.buf);
    free(op3.buf);
}

// Compaction merges the newer writes into the base data block in place and gives the entry it
// adds the very same location. Walking down the chain to mark everything below it as garbage
// must therefore not free that block - not even when a big write of its own sits in between,
// which is what a rolled back big write leaves behind. Freeing it handed the block out to the
// next object while the entry still pointed at it, and the store refused to start with
// "double-claimed data block"
static void test_compact_over_rolled_back_big_write()
{
    printf("\n-- test_compact_over_rolled_back_big_write\n");

    bs_test_t test;
    test.default_cfg();
    test.init();

    object_id oid = { .inode = 1, .stripe = 0 };
    object_id other = { .inode = 1, .stripe = 0x20000 };
    uint32_t block_size = test.bs->dsk.data_block_size;
    uint8_t *buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, block_size);

    blockstore_op_t op;
    op.oid = oid;
    op.buf = buf;

    auto stabilize = [&](object_id o, uint64_t version, bool rollback)
    {
        blockstore_op_t s;
        s.opcode = rollback ? BS_OP_ROLLBACK : BS_OP_STABLE;
        s.len = 1;
        s.buf = (uint8_t*)malloc_or_die(sizeof(obj_ver_id));
        *((obj_ver_id*)s.buf) = (obj_ver_id){ .oid = o, .version = version };
        test.exec_op(&s);
        assert(s.retval == 0);
        free(s.buf);
    };
    // The newest entry which owns a data block - commits and small writes sit on top of it
    auto base_entry = [&](object_id o)
    {
        auto *wr = test.bs->heap->read_entry(o);
        while (wr && wr->type() != BS_HEAP_BIG_WRITE && wr->type() != BS_HEAP_BIG_INTENT)
            wr = test.bs->heap->prev(wr);
        assert(wr);
        return wr;
    };

    printf("write v1 (big) and stabilize it - this is the base block\n");
    op.opcode = BS_OP_WRITE;
    op.version = 1;
    op.offset = 0;
    op.len = block_size;
    memset(buf, 0xAA, block_size);
    test.exec_op(&op);
    assert(op.retval == (int)op.len);
    stabilize(oid, 1, false);
    uint64_t base_loc = base_entry(oid)->big_location(test.bs->heap);
    printf("base block is at %ju\n", base_loc);

    printf("write v2 (big) into another block and roll it back\n");
    op.version = 2;
    memset(buf, 0xBB, block_size);
    test.exec_op(&op);
    assert(op.retval == (int)op.len);
    uint64_t rolled_loc = base_entry(oid)->big_location(test.bs->heap);
    assert(rolled_loc != base_loc);
    stabilize(oid, 1, true);

    printf("write v2 again (small) on top of the base and stabilize it\n");
    op.version = 2;
    op.offset = 8192;
    op.len = 4096;
    memset(buf, 0xCC, 4096);
    test.exec_op(&op);
    assert(op.retval == (int)op.len);
    stabilize(oid, 2, false);

    test.force_compaction();
    // Garbage is only collected once the compacted entry is durable
    blockstore_op_t sync;
    sync.opcode = BS_OP_SYNC;
    test.exec_op(&sync);
    assert(sync.retval == 0);
    for (int i = 0; i < 1000 && test.bs->heap->get_fsynced_lsn() < test.bs->heap->get_completed_lsn(); i++)
        test.ringloop->loop();

    printf("the compacted entry must still own the base block\n");
    auto *compacted = base_entry(oid);
    assert(compacted->type() == BS_HEAP_BIG_WRITE);
    assert(compacted->big_location(test.bs->heap) == base_loc);
    assert(test.bs->heap->is_data_used(base_loc));
    // The rolled back write is gone, so its block is free again
    assert(!test.bs->heap->is_data_used(rolled_loc));

    printf("another object must not be given the same block\n");
    op.oid = other;
    op.opcode = BS_OP_WRITE_STABLE;
    op.version = 1;
    op.offset = 0;
    op.len = block_size;
    memset(buf, 0xDD, block_size);
    test.exec_op(&op);
    assert(op.retval == (int)op.len);
    assert(base_entry(other)->big_location(test.bs->heap) != base_loc);

    printf("reading the object back\n");
    blockstore_op_t rd;
    rd.opcode = BS_OP_READ;
    rd.oid = oid;
    rd.version = UINT64_MAX;
    rd.offset = 0;
    rd.len = block_size;
    rd.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, rd.len);
    test.exec_op(&rd);
    assert(rd.retval == (int)rd.len);
    assert(memcheck(rd.buf, 0xAA, 8192));
    assert(memcheck(rd.buf + 8192, 0xCC, 4096));
    assert(memcheck(rd.buf + 12288, 0xAA, block_size - 12288));
    free(rd.buf);

    free(buf);
}

// With a checksum block larger than the write granularity, compaction puts the intent writes it
// merges into read_vec as well - not to write them (their data is already in place inside the
// data block) but because recalculating the checksum of a block they only partially cover needs
// their bytes. Reading them was skipped whenever the journal was in memory and no checksum block
// needed padding, and then the checksum was calculated over a NULL buffer
static void test_compact_intent_write_in_partial_csum_block()
{
    printf("\n-- test_compact_intent_write_in_partial_csum_block\n");

    bs_test_t test;
    test.default_cfg();
    // The intent write and the small write below have to share one checksum block
    test.config["csum_block_size"] = "16384";
    test.init();

    object_id oid = { .inode = 1, .stripe = 0 };
    uint32_t block_size = test.bs->dsk.data_block_size;
    uint8_t *buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, block_size);

    blockstore_op_t op;
    op.opcode = BS_OP_WRITE_STABLE;
    op.oid = oid;
    op.buf = buf;

    printf("write v1 0+128K - the base data block\n");
    op.version = 1;
    op.offset = 0;
    op.len = block_size;
    memset(buf, 0xAA, block_size);
    test.exec_op(&op);
    assert(op.retval == (int)op.len);

    // A write of at most atomic_write_size bytes over a stable big write goes into the data
    // block in place instead of into the buffer area
    printf("write v2 0+4K - it becomes an intent write\n");
    op.version = 2;
    op.offset = 0;
    op.len = 4096;
    memset(buf, 0xBB, 4096);
    test.exec_op(&op);
    assert(op.retval == (int)op.len);
    assert(test.bs->heap->read_entry(oid)->type() == BS_HEAP_INTENT_WRITE);

    // The rest of the same checksum block, so that the block has no unwritten part left and
    // compaction doesn't have to pad anything
    printf("write v3 4K+12K - a buffered small write filling the rest of the checksum block\n");
    op.version = 3;
    op.offset = 4096;
    op.len = 12288;
    memset(buf, 0xCC, 12288);
    test.exec_op(&op);
    assert(op.retval == (int)op.len);
    assert(test.bs->heap->read_entry(oid)->type() == BS_HEAP_SMALL_WRITE);

    test.force_compaction();

    printf("reading the object back\n");
    blockstore_op_t rd;
    rd.opcode = BS_OP_READ;
    rd.oid = oid;
    rd.version = UINT64_MAX;
    rd.offset = 0;
    rd.len = block_size;
    rd.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, rd.len);
    test.exec_op(&rd);
    assert(rd.retval == (int)rd.len);
    assert(memcheck(rd.buf, 0xBB, 4096));
    assert(memcheck(rd.buf + 4096, 0xCC, 12288));
    assert(memcheck(rd.buf + 16384, 0xAA, block_size - 16384));
    free(rd.buf);

    free(buf);
}

int main(int narg, char *args[])
{
    test_simple();
    test_fsync(false);
    test_fsync(true);
    test_fsync_meta_before_complete();
    test_intent_over_unstable();
    test_padded_csum_intent(false);
    test_padded_csum_intent(true);
    test_padded_csum_sparse_leading_hole();
    test_perfect_csum_interrupted();
    test_padded_csum_parallel_read(false, 8192);
    test_padded_csum_parallel_read(true, 8192);
    test_padded_csum_parallel_read(false, 16384);
    test_padded_csum_parallel_read(true, 16384);
    test_compact_rollback();
    test_fsync_almost_batch_big();
    test_fsync_batch_big();
    test_list_limit();
    test_write_no_space_eagain();
    test_write_stays_in_same_meta_block();
    test_read_merge_exact_partial_blocks();
    test_compact_big_intent_survives_crash(16384);
    test_compact_big_intent_survives_crash(0);
    test_recheck_under_commit();
    test_compact_over_rolled_back_big_write();
    test_compact_intent_write_in_partial_csum_block();
    return 0;
}
