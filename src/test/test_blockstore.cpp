// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <malloc.h>
#include "ringloop_mock.h"
#include "blockstore_impl.h"

int main(int narg, char *args[])
{
    blockstore_config_t config;
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

    disk_mock_t *data_disk = NULL;
    ring_loop_mock_t *ringloop = new ring_loop_mock_t(RINGLOOP_DEFAULT_SIZE, [&](io_uring_sqe *sqe)
    {
        assert(sqe->fd == MOCK_DATA_FD);
        bool ok = data_disk->submit(sqe);
        assert(ok);
    });
    timerfd_manager_t *tfd = new timerfd_manager_t(nullptr);
    data_disk = new disk_mock_t(ringloop, 1073741824, false);
    blockstore_impl_t *bs = new blockstore_impl_t(config, ringloop, tfd, true);

    // Wait for blockstore init
    while (!bs->is_started())
        ringloop->loop();
    printf("init completed\n");

    // Write
    bool done = false;
    blockstore_op_t op;
    uint64_t version = 0;
    op.opcode = BS_OP_WRITE;
    op.oid = { .inode = 1, .stripe = 0 };
    op.version = 1;
    op.offset = 16384;
    op.len = 4096;
    op.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 128*1024);
    memset(op.buf, 0xaa, 4096);
    op.callback = [&](blockstore_op_t *op)
    {
        printf("op completed code=%lu retval=%d\n", op->opcode, op->retval);
        done = true;
    };
    bs->enqueue_op(&op);
    while (!done)
        ringloop->loop();
    assert(op.retval == op.len);

    // Sync
    printf("version %ju written, syncing\n", op.version);
    done = false;
    version = op.version;
    op.opcode = BS_OP_SYNC;
    bs->enqueue_op(&op);
    while (!done)
        ringloop->loop();
    assert(op.retval == 0);

    // Commit
    printf("commit version %ju\n", version);
    done = false;
    op.opcode = BS_OP_STABLE;
    op.len = 1;
    *((obj_ver_id*)op.buf) = {
        .oid = { .inode = 1, .stripe = 0 },
        .version = version,
    };
    bs->enqueue_op(&op);
    while (!done)
        ringloop->loop();
    assert(op.retval == 0);

    // Read
    printf("reading 0-128K\n");
    done = false;
    op.opcode = BS_OP_READ;
    op.oid = { .inode = 1, .stripe = 0 };
    op.version = UINT64_MAX;
    op.offset = 0;
    op.len = 128*1024;
    bs->enqueue_op(&op);
    while (!done)
        ringloop->loop();
    assert(op.retval == op.len);
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
    free(cmp);

    free(op.buf);

    // Destroy
    while (!bs->is_safe_to_stop())
        ringloop->loop();
    delete bs;
    delete tfd;
    delete data_disk;
    delete ringloop;
    return 0;
}
