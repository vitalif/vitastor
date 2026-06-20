// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include <malloc.h>
#include "str_util.h"
#include "ringloop_mock.h"
#include "blockstore/v1/impl.h"

struct bs_test_t
{
    blockstore_config_t config;
    disk_mock_t *data_disk = NULL;
    disk_mock_t *meta_disk = NULL;
    std::function<bool(io_uring_sqe*)> sqe_handler;
    ring_loop_mock_t *ringloop = NULL;
    timerfd_manager_t *tfd = NULL;
    v1::blockstore_impl_t *bs = NULL;

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

    blockstore_disk_t& dsk()
    {
        return bs->dsk;
    }

    v1::journal_flusher_t* flusher()
    {
        return bs->flusher;
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
        config["meta_format"] = "2";
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
            bs = new v1::blockstore_impl_t(config, ringloop, tfd, true);
            while (!bs->is_started())
                ringloop->loop();
        }
    }

    void exec_op(blockstore_op_t *op)
    {
        bool done = false;
        op->callback = [&](blockstore_op_t *op)
        {
            done = true;
        };
        bs->enqueue_op(op);
        while (!done)
            ringloop->loop();
        op->callback = nullptr;
    }
};

// Check that journaled data corruption is preserved after flushing
// TODO more variations:
// - block 4k, block 16k
// - write at 16k, write at 36k
// - corrupt data, corrupt journal
// - inmemory, not inmemory
// - partial data checksum block read, full read
// - clean object at 1, at 0 (to check regressions with clean_loc_used)
static void test_preserve_corruption()
{
    printf("\n-- test_preserve_corruption\n");

    bs_test_t test;
    test.default_cfg();
    test.config["inmemory_journal"] = "0";
    test.config["data_csum_type"] = "crc32c";
    test.config["csum_block_size"] = "16384";
    test.init();
    printf("blockstore initialized\n");

    // Big_write without external bitmap(!) - also checks if journaled big_writes
    // are handled correctly (they don't have an external bitmap)
    printf("write v1 0+128k\n");
    blockstore_op_t op;
    op.opcode = BS_OP_WRITE_STABLE;
    op.oid = { .inode = 1, .stripe = 0 };
    op.version = 1;
    op.offset = 0;
    op.len = 128*1024;
    op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, op.len);
    memset(op.buf, 0xAA, op.len);
    test.exec_op(&op);
    assert(op.retval == op.len);

    // Small_write
    printf("write v2 16+4k\n");
    op.version = 2;
    op.offset = 16384;
    op.len = 4096;
    memset(op.buf, 0xAB, 4096);
    test.exec_op(&op);
    assert(op.retval == op.len);

    // Check that it's not compacted
    assert(test.flusher()->get_queue_size());

    // Read and verify original data before corruption
    uint64_t small_write_offset = 0, small_write_len = 0;
    test.sqe_handler = [&](io_uring_sqe *sqe)
    {
        if (sqe->off >= test.dsk().journal_offset && sqe->off < test.dsk().journal_offset + test.dsk().journal_len)
        {
            auto data = ((ring_data_t*)sqe->user_data);
            small_write_offset = sqe->off;
            small_write_len = data->iov.iov_len;
        }
        return false;
    };
    printf("read v2 (before corruption)\n");
    blockstore_op_t read_op;
    read_op.opcode = BS_OP_READ;
    read_op.oid = { .inode = 1, .stripe = 0 };
    read_op.version = 2;
    read_op.offset = 0;
    read_op.len = 128*1024;
    read_op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, read_op.len);
    test.exec_op(&read_op);
    assert(read_op.retval == read_op.len);
    assert(memcheck(read_op.buf, 0xAA, 16*1024));
    assert(memcheck(read_op.buf + 16*1024, 0xAB, 4*1024));
    assert(memcheck(read_op.buf + 20*1024, 0xAA, 108*1024));
    assert(small_write_offset && small_write_len);
    test.sqe_handler = nullptr;

    // Corrupt data on the disk
    printf("corrupting journaled data\n");
    {
        uint8_t *buf = (uint8_t*)malloc_or_die(small_write_len);
        memset(buf, 0xBB, small_write_len);
        io_uring_sqe sqe;
        ring_data_t data = {};
        iovec v = { .iov_base = buf, .iov_len = small_write_len };
        sqe.opcode = IORING_OP_WRITEV;
        sqe.off = small_write_offset;
        sqe.addr = (uint64_t)&v;
        sqe.len = 1;
        sqe.rw_flags = RWF_DSYNC;
        sqe.user_data = (uint64_t)&data;
        bool ok = test.data_disk->submit(&sqe);
        assert(ok);
        assert(data.res == small_write_len);
        free(buf);
    }

    // Read corrupted - should finish with -EDOM
    printf("read v2 (corrupted) - should fail with -EDOM\n");
    read_op.version = UINT64_MAX;
    test.exec_op(&read_op);
    assert(read_op.retval == -EDOM);

    // Read non-corrupted part - should succeed
    printf("read v2 (non-corrupted part) - should succeed\n");
    read_op.version = UINT64_MAX;
    read_op.offset = 32768;
    read_op.len = 16384;
    test.exec_op(&read_op);
    assert(read_op.retval == read_op.len);

    // Check that it's still not compacted
    assert(test.flusher()->get_queue_size());

    // Trigger compaction and intercept journal read
    printf("triggering compaction\n");
    test.flusher()->request_trim();
    while (test.flusher()->get_queue_size())
        test.ringloop->loop();
    while (test.flusher()->is_active())
        test.ringloop->loop();
    test.flusher()->release_trim();
    assert(!test.flusher()->get_queue_size());
    printf("compaction complete\n");

    // Store v1 can't cancel compaction because the journal is a ring buffer
    // so it compacts the object but preserves corruption
    printf("read v2 (after compaction) - should fail with -EDOM\n");
    read_op.version = UINT64_MAX;
    read_op.offset = 0;
    read_op.len = 128*1024;
    test.exec_op(&read_op);
    assert(read_op.retval == -EDOM);

    // Read non-corrupted part - should succeed
    printf("read v2 (non-corrupted part) - should succeed\n");
    read_op.version = UINT64_MAX;
    read_op.offset = 32768;
    read_op.len = 16384;
    test.exec_op(&read_op);
    assert(read_op.retval == read_op.len);

    free(op.buf);
    free(read_op.buf);
}

static void test_validate_padded_journal()
{
    printf("\n-- test_validate_padded_journal\n");

    bs_test_t test;
    test.default_cfg();
    test.config["inmemory_journal"] = "0";
    test.config["data_csum_type"] = "crc32c";
    test.config["csum_block_size"] = "16384";
    test.init();
    printf("blockstore initialized\n");

    printf("write v1 4+32k\n");
    blockstore_op_t op;
    op.opcode = BS_OP_WRITE_STABLE;
    op.oid = { .inode = 1, .stripe = 0 };
    op.version = 1;
    op.offset = 4*1024;
    op.len = 32*1024;
    op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, op.len);
    memset(op.buf, 0xAA, op.len);
    test.exec_op(&op);
    assert(op.retval == op.len);

    printf("read v1 0+128k\n");
    blockstore_op_t read_op;
    read_op.opcode = BS_OP_READ;
    read_op.oid = { .inode = 1, .stripe = 0 };
    read_op.version = UINT64_MAX;
    read_op.offset = 0;
    read_op.len = 128*1024;
    read_op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, read_op.len);
    test.exec_op(&read_op);
    assert(read_op.retval == read_op.len);
    assert(memcheck(read_op.buf, 0, 4*1024));
    assert(memcheck(read_op.buf + 4*1024, 0xAA, 32*1024));
    assert(memcheck(read_op.buf + 36*1024, 0, (128-36)*1024));

    printf("read v1 16+16k\n");
    read_op.version = UINT64_MAX;
    read_op.offset = 16*1024;
    read_op.len = 16*1024;
    test.exec_op(&read_op);
    assert(read_op.retval == read_op.len);
    assert(memcheck(read_op.buf, 0xAA, 16*1024));

    printf("read v1 0+16k\n");
    read_op.version = UINT64_MAX;
    read_op.offset = 0;
    read_op.len = 16*1024;
    test.exec_op(&read_op);
    assert(read_op.retval == read_op.len);
    assert(memcheck(read_op.buf, 0, 4*1024));
    assert(memcheck(read_op.buf + 4*1024, 0xAA, 12*1024));

    printf("small_write v2 8+32k\n");
    op.version = 2;
    op.offset = 8*1024;
    op.len = 32*1024;
    memset(op.buf, 0xAB, op.len);
    test.exec_op(&op);
    assert(op.retval == op.len);

    printf("read v1 0+128k\n");
    read_op.version = UINT64_MAX;
    read_op.offset = 0;
    read_op.len = 128*1024;
    test.exec_op(&read_op);
    assert(read_op.retval == read_op.len);

    printf("read v1 16+16k\n");
    read_op.version = UINT64_MAX;
    read_op.offset = 16*1024;
    read_op.len = 16*1024;
    test.exec_op(&read_op);
    assert(read_op.retval == read_op.len);

    printf("read v1 0+16k\n");
    read_op.version = UINT64_MAX;
    read_op.offset = 0;
    read_op.len = 16*1024;
    test.exec_op(&read_op);
    assert(read_op.retval == read_op.len);

    free(op.buf);
    free(read_op.buf);
}

// Check that read is retried and temporary read buffers are freed correctly (LSAN)
// TODO: Check retries in other configuration
static void test_read_retry_on_ring_full_1M_csum4k_clean()
{
    printf("\n-- test_read_retry_on_ring_full_1M_csum4k_clean\n");

    bs_test_t test;
    test.default_cfg();
    test.config["inmemory_metadata"] = "0";
    test.config["block_size"] = "1048576";
    test.config["data_csum_type"] = "crc32c";
    test.config["csum_block_size"] = "4096";
    test.init();
    printf("blockstore initialized\n");

    // Big_write without external bitmap(!) - also checks if journaled big_writes
    // are handled correctly (they don't have an external bitmap)
    printf("write v1 0+1M\n");
    blockstore_op_t op;
    op.opcode = BS_OP_WRITE_STABLE;
    op.oid = { .inode = 1, .stripe = 0 };
    op.version = 1;
    op.offset = 0;
    op.len = 1024*1024;
    op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, op.len);
    memset(op.buf, 0xAA, op.len);
    test.exec_op(&op);
    assert(op.retval == op.len);

    // Wait for flushing
    printf("triggering compaction\n");
    test.flusher()->request_trim();
    while (test.flusher()->get_queue_size())
        test.ringloop->loop();
    while (test.flusher()->is_active())
        test.ringloop->loop();
    test.flusher()->release_trim();
    assert(!test.flusher()->get_queue_size());
    printf("compaction complete\n");

    // Read with retry
    printf("read with ring-full-retries\n");
    blockstore_op_t read_op;
    read_op.opcode = BS_OP_READ;
    read_op.oid = { .inode = 1, .stripe = 0 };
    read_op.version = 2;
    read_op.offset = 0;
    read_op.len = 1024*1024;
    read_op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, read_op.len);
    int req_count = 0;
    int retry_on = 0;
    bool done = false;
    read_op.callback = [&](blockstore_op_t *op)
    {
        done = true;
    };
    // Force a retry after each sqe
    test.ringloop->set_fake_full([&]
    {
        if (req_count == retry_on)
        {
            printf("hit retry on sqe %d\n", req_count);
            req_count = 0;
            retry_on++;
            return true;
        }
        req_count++;
        return false;
    });
    test.bs->enqueue_op(&read_op);
    while (!done)
        test.ringloop->loop();
    read_op.callback = nullptr;
    assert(read_op.retval == read_op.len);

    free(op.buf);
    free(read_op.buf);
}

int main(int narg, char *args[])
{
    test_preserve_corruption();
    test_validate_padded_journal();
    test_read_retry_on_ring_full_1M_csum4k_clean();
    return 0;
}
