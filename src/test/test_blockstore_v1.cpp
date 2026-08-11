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

    uint64_t used_blocks()
    {
        return bs->used_blocks;
    }

    bool is_data_loc_used(uint64_t loc)
    {
        return bs->data_alloc->get(loc / bs->dsk.data_block_size);
    }

    void force_compaction()
    {
        printf("triggering compaction\n");
        flusher()->request_trim();
        while (flusher()->get_queue_size())
            ringloop->loop();
        while (flusher()->is_active())
            ringloop->loop();
        flusher()->release_trim();
        assert(!flusher()->get_queue_size());
        printf("compaction complete\n");
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
    test.force_compaction();

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

// Check that the flusher can pad a small write which covers only a part of a checksum
// block when the rest of that checksum block is a hole in the clean object.
// The flusher asks for the list of required reads without a buffer (read_buf = NULL),
// so fill_partial_checksum_blocks() must not rebase NULL into a bogus pointer.
static void test_flush_partial_csum_block_over_hole()
{
    printf("\n-- test_flush_partial_csum_block_over_hole\n");

    bs_test_t test;
    test.default_cfg();
    test.config["data_csum_type"] = "crc32c";
    test.config["csum_block_size"] = "8192";
    test.init();
    printf("blockstore initialized\n");

    // Clean version covering 0..8k only, so 8k..16k stays a hole in the clean bitmap
    // while the checksum block 8k..16k gets partially overwritten below
    printf("write v1 0+8k\n");
    blockstore_op_t op;
    op.opcode = BS_OP_WRITE_STABLE;
    op.oid = { .inode = 1, .stripe = 0 };
    op.version = 1;
    op.offset = 0;
    op.len = 8*1024;
    op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, op.len);
    memset(op.buf, 0xAA, op.len);
    test.exec_op(&op);
    assert(op.retval == op.len);

    test.force_compaction();

    // Small write aligned at the beginning of checksum block 8k..16k, but not at its end,
    // so the flusher has to pad it with old data - and the padding is a hole
    printf("small_write v2 8+4k\n");
    op.version = 2;
    op.offset = 8*1024;
    op.len = 4*1024;
    memset(op.buf, 0xAB, op.len);
    test.exec_op(&op);
    assert(op.retval == op.len);
    assert(test.flusher()->get_queue_size());

    // Used to segfault in journal_flusher_co::scan_dirty()
    test.force_compaction();

    printf("read v2 0+128k\n");
    blockstore_op_t read_op;
    read_op.opcode = BS_OP_READ;
    read_op.oid = { .inode = 1, .stripe = 0 };
    read_op.version = UINT64_MAX;
    read_op.offset = 0;
    read_op.len = 128*1024;
    read_op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, read_op.len);
    test.exec_op(&read_op);
    assert(read_op.retval == read_op.len);
    assert(memcheck(read_op.buf, 0xAA, 8*1024));
    assert(memcheck(read_op.buf + 8*1024, 0xAB, 4*1024));
    assert(memcheck(read_op.buf + 12*1024, 0, (128-12)*1024));

    free(op.buf);
    free(read_op.buf);
}

// Check that a read which starts before the object's data zero-fills the correct
// part of the read buffer. read_buf is already rebased by read_offset when the
// zero-filling callback runs, so it must not be subtracted a second time.
static void test_read_padded_big_write_at_offset()
{
    printf("\n-- test_read_padded_big_write_at_offset\n");

    bs_test_t test;
    test.default_cfg();
    test.config["data_csum_type"] = "crc32c";
    test.config["csum_block_size"] = "8192";
    test.init();
    printf("blockstore initialized\n");

    // The first write to a non-existent object is a big_write, so the object's data
    // starts at 16k and 0..16k is a hole
    printf("write v1 16+16k\n");
    blockstore_op_t op;
    op.opcode = BS_OP_WRITE_STABLE;
    op.oid = { .inode = 1, .stripe = 0 };
    op.version = 1;
    op.offset = 16*1024;
    op.len = 16*1024;
    op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, op.len);
    memset(op.buf, 0xAA, op.len);
    test.exec_op(&op);
    assert(op.retval == op.len);

    // Read 8k..24k. 8k..16k has to be zero-filled, 16k..24k has to be read from the disk.
    // Keep 16k of slack in front of the buffer so that a misplaced zero-fill lands in the
    // slack and fails the assertions below instead of corrupting the heap.
    printf("read v1 8+16k\n");
    uint8_t *read_base = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 32*1024);
    blockstore_op_t read_op;
    read_op.opcode = BS_OP_READ;
    read_op.oid = { .inode = 1, .stripe = 0 };
    read_op.version = UINT64_MAX;
    read_op.offset = 8*1024;
    read_op.len = 16*1024;
    read_op.buf = read_base + 16*1024;
    memset(read_base, 0xCC, 32*1024);
    test.exec_op(&read_op);
    assert(read_op.retval == read_op.len);
    assert(memcheck((uint8_t*)read_op.buf, 0, 8*1024));
    assert(memcheck((uint8_t*)read_op.buf + 8*1024, 0xAA, 8*1024));
    // The slack in front of the buffer must be untouched
    assert(memcheck(read_base, 0xCC, 16*1024));

    free(op.buf);
    free(read_base);
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
    test.force_compaction();

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

static void test_read_free_clean_loc_used()
{
    printf("\n-- test_read_free_clean_loc_used\n");

    bs_test_t test;
    test.default_cfg();
    test.init();
    printf("blockstore initialized\n");

    // big_write (block 0)
    printf("write obj2 v1 0+128k\n");
    blockstore_op_t op;
    op.opcode = BS_OP_WRITE_STABLE;
    op.oid = { .inode = 2, .stripe = 0 };
    op.version = 1;
    op.offset = 0;
    op.len = 128*1024;
    op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, op.len);
    memset(op.buf, 0xAA, op.len);
    test.exec_op(&op);
    assert(op.retval == op.len);

    // big_write (block 1)
    printf("write v1 0+128k\n");
    op.oid = { .inode = 1, .stripe = 0 };
    op.version = 1;
    test.exec_op(&op);
    assert(op.retval == op.len);

    // Force compaction
    test.force_compaction();

    // Start reading it but pause SQE completion
    printf("start reading 0+128k\n");
    ring_data_t *data_read = NULL;
    uint64_t read_loc = 0;
    test.sqe_handler = [&](io_uring_sqe *sqe)
    {
        if (sqe->off >= test.dsk().data_offset && sqe->fd == MOCK_DATA_FD && !data_read)
        {
            bool ok = test.data_disk->submit(sqe);
            assert(ok);
            read_loc = sqe->off - test.dsk().data_offset;
            data_read = (ring_data_t*)sqe->user_data;
            return true;
        }
        return false;
    };
    blockstore_op_t read_op;
    read_op.opcode = BS_OP_READ;
    read_op.oid = { .inode = 1, .stripe = 0 };
    read_op.version = 1;
    read_op.offset = 0;
    read_op.len = 128*1024;
    read_op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, read_op.len);
    bool done_read = false;
    read_op.callback = [&](blockstore_op_t *op)
    {
        done_read = true;
    };
    test.bs->enqueue_op(&read_op);
    while (!data_read)
        test.ringloop->loop();

    // Overwrite the object
    printf("write v2 0+128k\n");
    op.version = 2;
    memset(op.buf, 0xBB, op.len);
    test.exec_op(&op);
    assert(op.retval == op.len);

    // Force compaction
    test.force_compaction();

    // Check that the old location is still not freed
    printf("checking that data location 0x%lx is still used\n", read_loc);
    assert(test.is_data_loc_used(read_loc));
    printf("completing read\n");
    test.ringloop->mark_completed(data_read);
    data_read = NULL;
    while (!done_read)
        test.ringloop->loop();
    read_op.callback = NULL;
    printf("checking that data location 0x%lx is freed\n", read_loc);
    assert(!test.is_data_loc_used(read_loc));

    assert(read_op.retval == read_op.len);

    free(op.buf);
    free(read_op.buf);
}

static void test_meta_tail_reload()
{
    printf("\n-- test_meta_tail_reload\n");

    bs_test_t test;
    test.default_cfg();
    test.init();
    printf("blockstore initialized\n");

    {
        // write an overflowing metadata entry
        auto & dsk = test.dsk();
        ring_data_t data = {};
        io_uring_sqe sqe = {};
        clean_disk_entry *new_entry = (clean_disk_entry *)malloc_or_die(dsk.clean_entry_size);
        memset(new_entry, 0, dsk.clean_entry_size);
        iovec iov = { .iov_base = new_entry, .iov_len = dsk.clean_entry_size };
        sqe.opcode = IORING_OP_WRITEV;
        sqe.off = dsk.meta_offset + (2 + dsk.block_count / (dsk.meta_block_size / dsk.clean_entry_size)) * dsk.meta_block_size;
        sqe.addr = (uint64_t)&iov;
        sqe.len = 1;
        sqe.flags = RWF_DSYNC;
        sqe.user_data = (uint64_t)&data;
        // Entry data
        new_entry->oid = { .inode = 1, .stripe = 0 };
        new_entry->version = 1;
        *(uint32_t*)new_entry->bitmap = 0xFFFFFFFF;
        *(uint32_t*)((uint8_t*)new_entry + dsk.clean_entry_size - 4) = crc32c(0, new_entry, dsk.clean_entry_size - 4);
        bool ok = test.data_disk->submit(&sqe);
        assert(ok);
        free(new_entry);
    }

    test.destroy_bs();

    test.init();
    assert(!test.used_blocks());

    printf("blockstore re-initialized with an overflowing entry\n");
}

int main(int narg, char *args[])
{
    test_preserve_corruption();
    test_validate_padded_journal();
    test_flush_partial_csum_block_over_hole();
    test_read_padded_big_write_at_offset();
    test_read_retry_on_ring_full_1M_csum4k_clean();
    test_read_free_clean_loc_used();
    test_meta_tail_reload();
    return 0;
}
