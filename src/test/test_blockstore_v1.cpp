// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include <malloc.h>
#include "str_util.h"
#include "ringloop_mock.h"
#include "blockstore/v1/impl.h"
#include "blockstore/v1/internal.h"

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

    uint64_t free_block_count()
    {
        return bs->get_free_block_count();
    }

    bool is_data_loc_used(uint64_t loc)
    {
        return bs->data_alloc->get(loc / bs->dsk.data_block_size);
    }

    v1::dirty_entry* find_dirty_entry(object_id oid, uint64_t version)
    {
        auto it = bs->dirty_db.find((obj_ver_id){ .oid = oid, .version = version });
        return it == bs->dirty_db.end() ? NULL : &it->second;
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

    // initialize the beginning of data area with non-zero data to check
    // explicit padding instead of the on-disk data verification
    {
        std::vector<uint8_t> nonzero_data(1048576, 0xF8);
        ring_data_t data;
        io_uring_sqe sqe;
        iovec iov;
        sqe.opcode = IORING_OP_WRITEV;
        sqe.off = test.dsk().data_offset;
        sqe.addr = (uint64_t)&iov;
        sqe.len = 1;
        sqe.rw_flags = 0;
        iov.iov_base = nonzero_data.data();
        iov.iov_len = nonzero_data.size();
        sqe.user_data = (uint64_t)&data;
        bool ok = test.data_disk->submit(&sqe);
        assert(ok);
    }

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

// Check that a clean object with several holes inside one checksum block is read correctly.
// Such a read used to abort with assert(fulfilled == read_op->len) in dequeue_read():
// read_range_fulfilled() inserted all zero-fill ranges of a single hole at the same position,
// so read_vec ended up unsorted and the next find_holes() reported an already zero-filled
// range as a hole again, adding its length to `fulfilled` for the second time
static void test_read_sparse_clean_bitmap_multi_zero_runs()
{
    printf("\n-- test_read_sparse_clean_bitmap_multi_zero_runs\n");

    bs_test_t test;
    test.default_cfg();
    test.config["block_size"] = "131072";
    test.config["data_csum_type"] = "crc32c";
    // 8 bitmap granules per checksum block
    test.config["csum_block_size"] = "32768";
    test.init();
    printf("blockstore initialized\n");

    // The first write to a non-existent object is a big_write, so the bitmap built for the
    // clean entry during compaction has granule 0 and granules 6..7 of checksum block 0
    // clear and granules 1..5 set - that is, two separate zero runs inside one checksum block
    printf("write v1 4+20k\n");
    blockstore_op_t op;
    op.opcode = BS_OP_WRITE_STABLE;
    op.oid = { .inode = 1, .stripe = 0 };
    op.version = 1;
    op.offset = 4*1024;
    op.len = 20*1024;
    op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, op.len);
    memset(op.buf, 0xAA, op.len);
    test.exec_op(&op);
    assert(op.retval == op.len);

    // read_range_fulfilled() is only reached for clean objects - the journal path passes
    // clean_entry_bitmap = NULL because journal writes don't have holes - so compact first
    test.force_compaction();

    // Read the whole checksum block: 0..4k and 24..32k are zero-filled from the bitmap,
    // 4..24k is read from the disk.
    // Keep 32k of slack in front of the buffer so that a misplaced zero-fill lands in the
    // slack and fails the assertions below instead of corrupting the heap.
    printf("read v1 0+32k\n");
    uint8_t *read_base = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 64*1024);
    memset(read_base, 0xCC, 64*1024);
    blockstore_op_t read_op;
    read_op.opcode = BS_OP_READ;
    read_op.oid = { .inode = 1, .stripe = 0 };
    read_op.version = UINT64_MAX;
    read_op.offset = 0;
    read_op.len = 32*1024;
    read_op.buf = read_base + 32*1024;
    test.exec_op(&read_op);
    assert(read_op.retval == read_op.len);
    assert(memcheck(read_op.buf, 0, 4*1024));
    assert(memcheck(read_op.buf + 4*1024, 0xAA, 20*1024));
    assert(memcheck(read_op.buf + 24*1024, 0, 8*1024));
    // The slack in front of the buffer must be untouched
    assert(memcheck(read_base, 0xCC, 32*1024));

    // The same over the whole object - checksum blocks 1..3 are completely unallocated
    printf("read v1 0+128k\n");
    read_op.offset = 0;
    read_op.len = 128*1024;
    read_op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, read_op.len);
    test.exec_op(&read_op);
    assert(read_op.retval == read_op.len);
    assert(memcheck(read_op.buf, 0, 4*1024));
    assert(memcheck(read_op.buf + 4*1024, 0xAA, 20*1024));
    assert(memcheck(read_op.buf + 24*1024, 0, (128-24)*1024));

    free(read_op.buf);
    free(read_base);
    free(op.buf);
}

// Check that a read which doesn't intersect the object's data at all is zero-filled
// inside the read buffer. The first write to a non-existent object is a big_write with
// the offset and length of that write, so the dirty entry may cover any part of the
// block - and a read of any other part of the same block gets an item range which lies
// completely before or completely after it.
static void test_read_padded_big_write_outside_read()
{
    printf("\n-- test_read_padded_big_write_outside_read\n");

    bs_test_t test;
    test.default_cfg();
    test.config["data_csum_type"] = "crc32c";
    test.config["csum_block_size"] = "8192";
    test.init();
    printf("blockstore initialized\n");

    // The first write to a non-existent object is a big_write, so the object's data
    // is 16k..32k and the rest of the block is a hole
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

    // Keep 128k of slack on both sides of the read buffer so that a misplaced zero-fill
    // lands in the slack and fails the assertions below instead of corrupting the heap
    uint8_t *read_base = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 3*128*1024);
    blockstore_op_t read_op;
    read_op.opcode = BS_OP_READ;
    read_op.oid = { .inode = 1, .stripe = 0 };
    read_op.version = UINT64_MAX;
    read_op.buf = read_base + 128*1024;

    // Read 0..8k - the whole read is in front of the object's data
    printf("read v1 0+8k\n");
    read_op.offset = 0;
    read_op.len = 8*1024;
    memset(read_base, 0xCC, 3*128*1024);
    test.exec_op(&read_op);
    assert(read_op.retval == read_op.len);
    assert(memcheck((uint8_t*)read_op.buf, 0, 8*1024));
    // The slack around the buffer must be untouched
    assert(memcheck(read_base, 0xCC, 128*1024));
    assert(memcheck(read_base + 128*1024 + 8*1024, 0xCC, 2*128*1024 - 8*1024));

    // Read 40k..56k - the whole read is behind the object's data
    printf("read v1 40+16k\n");
    read_op.offset = 40*1024;
    read_op.len = 16*1024;
    memset(read_base, 0xCC, 3*128*1024);
    test.exec_op(&read_op);
    assert(read_op.retval == read_op.len);
    assert(memcheck((uint8_t*)read_op.buf, 0, 16*1024));
    // The slack around the buffer must be untouched
    assert(memcheck(read_base, 0xCC, 128*1024));
    assert(memcheck(read_base + 128*1024 + 16*1024, 0xCC, 2*128*1024 - 16*1024));

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
    printf("checking that data location 0x%jx is still used\n", read_loc);
    assert(test.is_data_loc_used(read_loc));
    printf("completing read\n");
    test.ringloop->mark_completed(data_read);
    data_read = NULL;
    while (!done_read)
        test.ringloop->loop();
    read_op.callback = NULL;
    printf("checking that data location 0x%jx is freed\n", read_loc);
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

static void test_write_no_space_eagain()
{
    printf("\n-- test_write_no_space_eagain\n");

    bs_test_t test;
    test.default_cfg();
    // 1 MB journal (data_offset 32 MB - journal_offset), so it fills in a few hundred writes
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
        test.exec_op(&op);
        if (op.retval != (int)op.len)
            blocked_retval = op.retval;
    };
    for (i = 0; i < 100000 && !blocked_retval; i++)
        do_write();
    printf("write #%d refused with retval=%d\n", i, blocked_retval);
    assert(blocked_retval == -EAGAIN);

    // check that a stabilize operation and one more write can proceed
    // journal space_check is simplified so it requires slightly more space than actually needed
    {
        blockstore_op_t op;
        op.opcode = BS_OP_STABLE;
        op.len = 4;
        op.buf = (uint8_t*)malloc_or_die(4 * sizeof(obj_ver_id));
        ((obj_ver_id*)op.buf)[0] = {
            .oid = { .inode = 1, .stripe = 0 },
            .version = 2,
        };
        ((obj_ver_id*)op.buf)[1] = {
            .oid = { .inode = 1, .stripe = 0x20000 },
            .version = 2,
        };
        ((obj_ver_id*)op.buf)[2] = {
            .oid = { .inode = 1, .stripe = 0x40000 },
            .version = 1,
        };
        ((obj_ver_id*)op.buf)[3] = {
            .oid = { .inode = 1, .stripe = 0x60000 },
            .version = 1,
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

// Check null dereference in read_bitmap over DELETE
static void test_read_bitmap_of_deleted_object()
{
    printf("\n-- test_read_bitmap_of_deleted_object\n");

    bs_test_t test;
    test.default_cfg();
    // 128k/16k*4 = 32 bytes of checksums --> dyn_data allocated dynamically
    test.config["data_csum_type"] = "crc32c";
    test.config["csum_block_size"] = "16384";
    test.init();
    printf("blockstore initialized\n");

    uint8_t bmp[16];
    memset(bmp, 0xCC, sizeof(bmp));

    printf("write v1 0+128k\n");
    blockstore_op_t op;
    op.opcode = BS_OP_WRITE_STABLE;
    op.oid = { .inode = 1, .stripe = 0 };
    op.version = 1;
    op.offset = 0;
    op.len = 128*1024;
    op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, op.len);
    op.bitmap = bmp;
    memset(op.buf, 0xAA, op.len);
    test.exec_op(&op);
    assert(op.retval == op.len);

    {
        // Check that big_write is here and with dyn_data
        v1::dirty_entry *de = test.find_dirty_entry(op.oid, 1);
        assert(de != NULL);
        assert(de->dyn_data != NULL);
    }

    {
        uint64_t version = 0;
        assert(test.dsk().clean_entry_bitmap_size <= sizeof(bmp));
        memset(bmp, 0, sizeof(bmp));
        assert(test.bs->read_bitmap(op.oid, UINT64_MAX, bmp, &version) == 0);
        assert(version == 1);
        assert(memcheck(bmp, 0xCC, test.dsk().clean_entry_bitmap_size));
    }

    {
        printf("delete v2\n");
        blockstore_op_t del_op;
        del_op.opcode = BS_OP_DELETE;
        del_op.oid = { .inode = 1, .stripe = 0 };
        del_op.version = 2;
        test.exec_op(&del_op);
        assert(del_op.retval == 0);
    }

    {
        // Check that the delete is still in dirty_db
        v1::dirty_entry *de = test.find_dirty_entry(op.oid, 2);
        assert(de != NULL);
        assert((de->state & BS_ST_TYPE_MASK) == BS_ST_DELETE);
        assert(de->dyn_data == NULL);
    }

    auto check_deleted_bmp = [&]()
    {
        uint64_t version = UINT64_MAX;
        memset(bmp, 0xCC, sizeof(bmp));
        assert(test.bs->read_bitmap(op.oid, UINT64_MAX, bmp, &version) == -ENOENT);
        assert(version == 0);
        assert(memcheck(bmp, 0, test.dsk().clean_entry_bitmap_size));
    };
    printf("read_bitmap of the deleted object - should be -ENOENT\n");
    check_deleted_bmp();

    {
        // Reading the data must agree with that
        printf("read of the deleted object - should be -ENOENT\n");
        blockstore_op_t read_op;
        read_op.opcode = BS_OP_READ;
        read_op.oid = { .inode = 1, .stripe = 0 };
        read_op.version = UINT64_MAX;
        read_op.offset = 0;
        read_op.len = 128*1024;
        read_op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, read_op.len);
        test.exec_op(&read_op);
        assert(read_op.retval == -ENOENT);
        free(read_op.buf);
    }

    // And once the delete is flushed too, via the clean_db path
    test.force_compaction();
    printf("read_bitmap after the delete is flushed - should be -ENOENT\n");
    check_deleted_bmp();

    // Write over delete to also check that it doesn't read NULL dyn_data
    printf("write v3 0+128k over the delete\n");
    op.version = 3;
    memset(op.buf, 0xAB, op.len);
    memset(bmp, 0xDE, sizeof(bmp));
    test.exec_op(&op);
    assert(op.retval == op.len);

    {
        uint64_t version = 0;
        memset(bmp, 0, sizeof(bmp));
        assert(test.bs->read_bitmap(op.oid, UINT64_MAX, bmp, &version) == 0);
        assert(memcheck(bmp, 0xDE, test.dsk().clean_entry_bitmap_size));
        assert(version == 3);
    }

    free(op.buf);
}

// used_blocks and inode_space_stats count one data block per object and only when a big_write
// or a delete is marked stable. counters should persist correctly over a restart.
static void test_used_blocks_replay_stable_over_small_write()
{
    printf("\n-- test_used_blocks_replay_stable_over_small_write\n");

    bs_test_t test;
    test.default_cfg();
    test.init();
    printf("blockstore initialized\n");

    uint64_t block_count = test.dsk().block_count;
    assert(!test.used_blocks());

    blockstore_op_t op;
    op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 128*1024);
    memset(op.buf, 0xAA, 128*1024);

    // Unstable big write. BS_OP_WRITE (not WRITE_STABLE) so that it needs an explicit
    // BS_OP_STABLE and stays in dirty_db until then.
    printf("write v1 0+128k (big)\n");
    op.opcode = BS_OP_WRITE;
    op.oid = { .inode = 1, .stripe = 0 };
    op.version = 1;
    op.offset = 0;
    op.len = 128*1024;
    test.exec_op(&op);
    assert(op.retval == (int)op.len);

    // Small write - the entry that used to be misread as "the block is already counted"
    printf("write v2 0+4k (small)\n");
    op.version = 2;
    op.len = 4096;
    test.exec_op(&op);
    assert(op.retval == (int)op.len);

    printf("write v3 0+128k (big)\n");
    op.version = 3;
    op.len = 128*1024;
    test.exec_op(&op);
    assert(op.retval == (int)op.len);

    printf("stabilize v3\n");
    op.opcode = BS_OP_STABLE;
    op.len = 1;
    *((obj_ver_id*)op.buf) = (obj_ver_id){ .oid = { .inode = 1, .stripe = 0 }, .version = 3 };
    test.exec_op(&op);
    assert(op.retval == 0);
    // The live path walks the whole range, so it has always got this one right
    printf("used_blocks after stabilize = %ju (expected 1)\n", test.used_blocks());
    assert(test.used_blocks() == 1);
    // Nothing must have been flushed - the journal alone has to reproduce the counters below
    assert(test.flusher()->get_queue_size());

    printf("restarting the blockstore (journal replay)\n");
    test.destroy_bs();
    test.init();
    uint64_t after_replay = test.used_blocks();
    printf("used_blocks after replay = %ju (expected 1)\n", after_replay);

    // Deleting the object must bring the counter back to zero, not below it
    printf("delete v4\n");
    op.opcode = BS_OP_DELETE;
    op.oid = { .inode = 1, .stripe = 0 };
    op.version = 4;
    op.offset = 0;
    op.len = 0;
    test.exec_op(&op);
    assert(op.retval == 0);
    uint64_t after_delete = test.used_blocks();
    uint64_t free_after_delete = test.free_block_count();
    printf("used_blocks after delete = %ju (expected 0), free_block_count = %ju (expected %ju)\n",
        after_delete, free_after_delete, block_count);

    assert(after_replay == 1);
    assert(after_delete == 0);
    assert(free_after_delete == block_count);

    free(op.buf);
}

// The same accounting rule for deletes. used_space counter must not underflow after a restart.
static void test_used_blocks_replay_delete_over_unstable_big_write()
{
    printf("\n-- test_used_blocks_replay_delete_over_unstable_big_write\n");

    bs_test_t test;
    test.default_cfg();
    test.init();
    printf("blockstore initialized\n");

    uint64_t block_count = test.dsk().block_count;
    assert(!test.used_blocks());

    blockstore_op_t op;
    op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 128*1024);
    memset(op.buf, 0xAA, 128*1024);

    printf("write v1 0+128k (big, left unstable)\n");
    op.opcode = BS_OP_WRITE;
    op.oid = { .inode = 1, .stripe = 0 };
    op.version = 1;
    op.offset = 0;
    op.len = 128*1024;
    test.exec_op(&op);
    assert(op.retval == (int)op.len);
    // An unstable big write owns a block but is not counted yet - that is what mark_stable() is for
    assert(!test.used_blocks());

    printf("delete v2\n");
    op.opcode = BS_OP_DELETE;
    op.version = 2;
    op.offset = 0;
    op.len = 0;
    test.exec_op(&op);
    assert(op.retval == 0);
    assert(!test.used_blocks());

    printf("restarting the blockstore (journal replay)\n");
    test.destroy_bs();
    test.init();
    uint64_t after_replay = test.used_blocks();
    uint64_t free_after_replay = test.free_block_count();
    printf("used_blocks after replay = %ju (expected 0), free_block_count = %ju (expected %ju)\n",
        after_replay, free_after_replay, block_count);
    assert(after_replay == 0);
    assert(free_after_replay == block_count);

    free(op.buf);
}

static void test_delete_over_ec_write()
{
    printf("\n-- test_delete_over_ec_write\n");

    bs_test_t test;
    test.default_cfg();
    test.init();
    printf("blockstore initialized\n");

    blockstore_op_t op;
    op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 128*1024);
    memset(op.buf, 0xAA, 128*1024);

    printf("write v1 0+128k (big, stable)\n");
    op.opcode = BS_OP_WRITE_STABLE;
    op.oid = { .inode = 1, .stripe = 0 };
    op.version = 1;
    op.offset = 0;
    op.len = 128*1024;
    test.exec_op(&op);
    assert(op.retval == (int)op.len);
    assert(test.used_blocks() > 0);

    printf("write v2 0+4k (big, unstable)\n");
    op.opcode = BS_OP_WRITE;
    op.version = 2;
    op.len = 4*1024;
    test.exec_op(&op);
    assert(op.retval == (int)op.len);

    printf("delete v3 - should mark all previous versions stable\n");
    op.opcode = BS_OP_DELETE;
    op.version = 3;
    op.offset = 0;
    op.len = 0;
    test.exec_op(&op);
    assert(op.retval == 0);

    {
        auto dirty = test.find_dirty_entry(op.oid, 2);
        assert(dirty);
        assert(IS_STABLE(dirty->state));
    }

    printf("restarting the blockstore (journal replay)\n");
    test.destroy_bs();
    test.init();

    {
        auto dirty = test.find_dirty_entry(op.oid, 2);
        if (!dirty)
        {
            dirty = test.find_dirty_entry(op.oid, 3);
            assert(!dirty);
        }
        else
        {
            assert(IS_STABLE(dirty->state));
            dirty = test.find_dirty_entry(op.oid, 3);
            assert(IS_DELETE(dirty->state));
        }
    }

    free(op.buf);
}

static void test_delete_over_ec_write_unstable()
{
    printf("\n-- test_delete_over_ec_write_unstable\n");

    bs_test_t test;
    test.default_cfg();
    test.config["disable_data_fsync"] = "0";
    test.config["immediate_commit"] = "none";
    test.init();
    printf("blockstore initialized\n");

    blockstore_op_t op;
    op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 128*1024);
    memset(op.buf, 0xAA, 128*1024);

    printf("write v1 0+128k (big, stable)\n");
    op.opcode = BS_OP_WRITE_STABLE;
    op.oid = { .inode = 1, .stripe = 0 };
    op.version = 1;
    op.offset = 0;
    op.len = 128*1024;
    test.exec_op(&op);
    assert(op.retval == (int)op.len);

    printf("write v2 0+4k (big, unstable)\n");
    op.opcode = BS_OP_WRITE;
    op.version = 2;
    op.len = 4*1024;
    test.exec_op(&op);
    assert(op.retval == (int)op.len);

    printf("delete v3 - should mark all previous versions stable, but only after sync\n");
    bool del_done = false;
    op.opcode = BS_OP_DELETE;
    op.version = 3;
    op.offset = 0;
    op.len = 0;
    op.callback = [&](blockstore_op_t *op)
    {
        del_done = true;
    };
    test.exec_op(&op);
    assert(op.retval == 0);

    {
        auto dirty = test.find_dirty_entry(op.oid, 3);
        assert(dirty);
        assert(!IS_SYNCED(dirty->state));
        assert(!IS_STABLE(dirty->state));
    }

    blockstore_op_t sync_op;
    sync_op.opcode = BS_OP_SYNC;
    test.exec_op(&sync_op);

    {
        auto dirty = test.find_dirty_entry(op.oid, 2);
        assert(dirty);
        assert(IS_STABLE(dirty->state));
        dirty = test.find_dirty_entry(op.oid, 3);
        assert(dirty);
        assert(IS_STABLE(dirty->state));
    }

    printf("restarting the blockstore (journal replay)\n");
    test.destroy_bs();
    test.init();

    {
        auto dirty = test.find_dirty_entry(op.oid, 2);
        if (!dirty)
        {
            dirty = test.find_dirty_entry(op.oid, 3);
            assert(!dirty);
        }
        else
        {
            assert(IS_STABLE(dirty->state));
            dirty = test.find_dirty_entry(op.oid, 3);
            assert(IS_DELETE(dirty->state));
        }
    }

    free(op.buf);
}

// Three journal writes are in flight at once and complete out of order: the newest first,
// then the middle one, and only then the oldest. An operation must not be acknowledged
// while a journal write submitted before it is still unfinished - a power outage right
// there would leave a hole in the journal in front of its entry, and recovery stops at the
// first hole, so the acknowledged operation would be lost.
static void test_journal_write_order()
{
    printf("\n-- test_journal_write_order\n");

    bs_test_t test;
    test.default_cfg();
    test.init();
    printf("blockstore initialized\n");

    uint64_t journal_start = test.dsk().journal_offset;
    uint64_t journal_end = test.dsk().journal_offset + test.dsk().journal_len;

    // Execute journal writes, but hold back their completions so that we can order them
    std::vector<ring_data_t*> journal_writes;
    test.sqe_handler = [&](io_uring_sqe *sqe)
    {
        if (sqe->fd == MOCK_DATA_FD && sqe->opcode == IORING_OP_WRITEV &&
            sqe->off >= journal_start && sqe->off < journal_end)
        {
            bool ok = test.data_disk->submit(sqe);
            assert(ok);
            journal_writes.push_back((ring_data_t*)sqe->user_data);
            return true;
        }
        return false;
    };

    // Full-block writes so that the data goes to the data area and the only journal write
    // of each operation is the one carrying its entry
    uint8_t *buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 128*1024);
    memset(buf, 0xAA, 128*1024);
    blockstore_op_t op[3];
    bool done[3] = { false, false, false };
    for (int i = 0; i < 3; i++)
    {
        op[i].opcode = BS_OP_WRITE;
        op[i].oid = { .inode = 1, .stripe = (uint64_t)i * 0x20000 };
        op[i].version = 1;
        op[i].offset = 0;
        op[i].len = 128*1024;
        op[i].buf = buf;
        op[i].callback = [&done, i](blockstore_op_t *op) { done[i] = true; };
        // One operation per batch, so that each entry lands in its own journal sector:
        // a sector with an unfinished write of its own can't be appended to
        test.bs->enqueue_op(&op[i]);
        for (int j = 0; j < 10 && journal_writes.size() < (size_t)i+1; j++)
            test.ringloop->loop();
    }
    printf("journal writes in flight: %zu\n", journal_writes.size());
    assert(journal_writes.size() == 3);
    assert(!done[0] && !done[1] && !done[2]);

    printf("completing the newest journal write\n");
    test.ringloop->mark_completed(journal_writes[2]);
    test.ringloop->loop();
    assert(!done[0] && !done[1] && !done[2]);

    printf("completing the middle one - the newest must still not be acknowledged\n");
    test.ringloop->mark_completed(journal_writes[1]);
    test.ringloop->loop();
    assert(!done[0]);
    assert(!done[1]);
    assert(!done[2]);

    printf("completing the oldest - now all three may be acknowledged\n");
    test.ringloop->mark_completed(journal_writes[0]);
    test.ringloop->loop();
    assert(done[0] && done[1] && done[2]);
    for (int i = 0; i < 3; i++)
        assert(op[i].retval == (int)op[i].len);

    test.sqe_handler = NULL;
    free(buf);
}

// open() marks the current journal sector as full so that the first entry starts a new
// one, and its offset is still zero at that point - it is a placeholder, not a real sector.
// The small write path used to flush that placeholder before moving on, which wrote a
// block of stale memory over the journal superblock at offset 0. That doesn't reset the
// journal - the store just refuses to start next time: "First entry of the journal is
// corrupt or unsupported", exit(1).
static void test_first_write_keeps_journal_superblock()
{
    printf("\n-- test_first_write_keeps_journal_superblock\n");

    bs_test_t test;
    test.default_cfg();
    // The placeholder sector is only flushed on the small write path
    test.config["disable_data_fsync"] = "0";
    test.config["immediate_commit"] = "none";
    // With the journal kept in memory the stray write happens to copy the superblock over
    // itself, because the placeholder points at the very first journal block. Without it
    // the source is an untouched sector buffer, and the superblock is really destroyed.
    test.config["inmemory_journal"] = "false";
    test.init();
    printf("blockstore initialized\n");

    uint8_t *buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 128*1024);
    memset(buf, 0xAA, 128*1024);
    object_id oid = { .inode = 1, .stripe = 0 };

    // The object has to exist first: a write to a missing object becomes a big write
    // regardless of its length, and big writes don't go through the path in question
    printf("creating the object\n");
    blockstore_op_t op;
    op.opcode = BS_OP_WRITE;
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
    op.opcode = BS_OP_STABLE;
    op.len = 1;
    op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, sizeof(obj_ver_id));
    *((obj_ver_id*)op.buf) = (obj_ver_id){ .oid = oid, .version = 1 };
    test.exec_op(&op);
    assert(op.retval == 0);
    free(op.buf);

    printf("restarting the blockstore\n");
    test.destroy_bs();
    test.init();

    // Watch for writes landing on the journal superblock. Only the flusher may write it,
    // when it trims the journal - which can't happen during a single write.
    uint64_t superblock = test.dsk().journal_offset;
    bool superblock_written = false;
    test.sqe_handler = [&](io_uring_sqe *sqe)
    {
        if (sqe->fd == MOCK_DATA_FD && sqe->opcode == IORING_OP_WRITEV && sqe->off == superblock)
            superblock_written = true;
        // Let it be executed as usual
        return false;
    };

    printf("first small write after the restart\n");
    memset(buf, 0xBB, 4096);
    op.opcode = BS_OP_WRITE;
    op.oid = oid;
    op.version = 2;
    op.offset = 0;
    op.len = 4096;
    op.buf = buf;
    test.exec_op(&op);
    assert(op.retval == (int)op.len);
    assert(!superblock_written);

    op.opcode = BS_OP_SYNC;
    test.exec_op(&op);
    assert(op.retval == 0);
    op.opcode = BS_OP_STABLE;
    op.len = 1;
    op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, sizeof(obj_ver_id));
    *((obj_ver_id*)op.buf) = (obj_ver_id){ .oid = oid, .version = 2 };
    test.exec_op(&op);
    assert(op.retval == 0);
    free(op.buf);

    printf("restarting again - a corrupt superblock would refuse to start\n");
    test.sqe_handler = NULL;
    test.destroy_bs();
    test.init();

    assert(!superblock_written);

    blockstore_op_t rd;
    rd.opcode = BS_OP_READ;
    rd.oid = oid;
    rd.version = UINT64_MAX;
    rd.offset = 0;
    rd.len = 8192;
    rd.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 8192);
    memset(rd.buf, 0, 8192);
    test.exec_op(&rd);
    assert(rd.retval == (int)rd.len);
    assert(memcheck(rd.buf, 0xBB, 4096));
    assert(memcheck(rd.buf + 4096, 0xAA, 4096));
    free(rd.buf);
    free(buf);
}

// Reading a small write straight from the journal used to divide by zero when checksums
// are disabled. The dirty read path handed fulfill_read() a checksum pointer whenever the
// journal is not kept in memory, without checking that there are any checksums at all -
// and csum_block_size is zero in that case, so fulfill_read_push() divided by it.
static void test_read_journal_small_write_without_csums()
{
    printf("\n-- test_read_journal_small_write_without_csums\n");

    bs_test_t test;
    test.default_cfg();
    // default_cfg leaves data_csum_type unset, so checksums are off and csum_block_size is 0
    assert(test.config.find("data_csum_type") == test.config.end());
    // With the journal in memory the read is served from there and no checksum pointer is
    // passed at all, so it has to be read from disk to hit the path
    test.config["inmemory_journal"] = "false";
    test.init();
    printf("blockstore initialized\n");
    assert(!test.dsk().csum_block_size);

    uint8_t *buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 128*1024);
    memset(buf, 0xAA, 128*1024);
    object_id oid = { .inode = 1, .stripe = 0 };

    // The object has to exist first: a write to a missing object becomes a big write
    // regardless of its length, and a big write goes to the data area, not the journal
    printf("creating the object\n");
    blockstore_op_t op;
    op.opcode = BS_OP_WRITE;
    op.oid = oid;
    op.version = 1;
    op.offset = 0;
    op.len = 128*1024;
    op.buf = buf;
    test.exec_op(&op);
    assert(op.retval == (int)op.len);

    printf("small write, stays in the journal\n");
    memset(buf, 0xBB, 4096);
    op.opcode = BS_OP_WRITE;
    op.oid = oid;
    op.version = 2;
    op.offset = 0;
    op.len = 4096;
    op.buf = buf;
    test.exec_op(&op);
    assert(op.retval == (int)op.len);

    printf("reading it back from the journal\n");
    blockstore_op_t rd;
    rd.opcode = BS_OP_READ;
    rd.oid = oid;
    rd.version = UINT64_MAX;
    rd.offset = 0;
    rd.len = 8192;
    rd.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 8192);
    memset(rd.buf, 0, 8192);
    test.exec_op(&rd);
    assert(rd.retval == (int)rd.len);
    assert(memcheck(rd.buf, 0xBB, 4096));
    assert(memcheck(rd.buf + 4096, 0xAA, 4096));
    free(rd.buf);
    free(buf);
}

int main(int narg, char *args[])
{
    test_preserve_corruption();
    test_validate_padded_journal();
    test_flush_partial_csum_block_over_hole();
    test_read_padded_big_write_at_offset();
    test_read_sparse_clean_bitmap_multi_zero_runs();
    test_read_padded_big_write_outside_read();
    test_read_retry_on_ring_full_1M_csum4k_clean();
    test_read_free_clean_loc_used();
    test_meta_tail_reload();
    test_write_no_space_eagain();
    test_read_bitmap_of_deleted_object();
    test_used_blocks_replay_stable_over_small_write();
    test_used_blocks_replay_delete_over_unstable_big_write();
    test_delete_over_ec_write();
    test_delete_over_ec_write_unstable();
    test_journal_write_order();
    test_first_write_keeps_journal_superblock();
    test_read_journal_small_write_without_csums();
    return 0;
}
