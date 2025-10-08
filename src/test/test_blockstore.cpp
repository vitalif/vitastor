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
            data_disk = new disk_mock_t(parse_size(config["data_device_size"]), config["disable_data_fsync"] != "1");
            data_disk->clear(0, parse_size(config["data_offset"]));
        }
        uint64_t meta_size = parse_size(config["meta_device_size"]);
        if (meta_size && !meta_disk)
        {
            meta_disk = new disk_mock_t(meta_size, config["disable_meta_fsync"] != "1");
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

    void exec_op(blockstore_op_t *op)
    {
        bool done = false;
        op->callback = [&](blockstore_op_t *op)
        {
            printf("op opcode=%lu completed retval=%d\n", op->opcode, op->retval);
            done = true;
        };
        bs->enqueue_op(op);
        while (!done)
            ringloop->loop();
        op->callback = nullptr;
    }
};

static bool memcheck(uint8_t *buf, uint8_t byte, size_t len)
{
    for (size_t i = 0; i < len; i++)
        if (buf[i] != byte)
            return false;
    return true;
}

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
    printf("writing\n");
    blockstore_op_t op;
    op.opcode = BS_OP_WRITE;
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
    assert(!test.bs->heap->prev(obj));

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

int main(int narg, char *args[])
{
    test_simple();
    test_fsync(false);
    test_fsync(true);
    test_intent_over_unstable();
    test_padded_csum_intent(false);
    test_padded_csum_intent(true);
    test_padded_csum_parallel_read(false, 8192);
    test_padded_csum_parallel_read(true, 8192);
    test_padded_csum_parallel_read(false, 16384);
    test_padded_csum_parallel_read(true, 16384);
    return 0;
}
