// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <stdio.h>
#include <stdlib.h>

#include "epoll_manager.h"
#include "cluster_client.h"

void send_read(cluster_client_t *cli, uint64_t inode, uint64_t offset, std::function<void(int, uint64_t, uint8_t)> cb, uint64_t flags = 0)
{
    cluster_op_t *op = new cluster_op_t();
    op->opcode = OSD_OP_READ;
    op->inode = inode;
    op->offset = offset;
    op->len = 4096;
    op->flags = flags;
    op->iov.push_back(malloc_or_die(op->len), op->len);
    op->callback = [cb](cluster_op_t *op)
    {
        uint64_t version = op->version;
        int retval = op->retval;
        if (retval == op->len)
            retval = 0;
        uint8_t read_byte = ((uint8_t*)op->iov.buf[0].iov_base)[0];
        free(op->iov.buf[0].iov_base);
        delete op;
        if (cb != NULL)
            cb(retval, version, read_byte);
    };
    cli->execute(op);
}

void send_write(cluster_client_t *cli, uint64_t inode, uint64_t offset, int byte, uint64_t version, std::function<void(int)> cb)
{
    cluster_op_t *op = new cluster_op_t();
    op->opcode = OSD_OP_WRITE;
    op->inode = inode;
    op->offset = offset;
    op->len = 4096;
    op->version = version;
    op->iov.push_back(malloc_or_die(op->len), op->len);
    memset(op->iov.buf[0].iov_base, byte, op->len);
    op->callback = [cb](cluster_op_t *op)
    {
        int retval = op->retval;
        if (retval == op->len)
            retval = 0;
        free(op->iov.buf[0].iov_base);
        delete op;
        if (cb != NULL)
            cb(retval);
    };
    cli->execute(op);
}

void send_sync(cluster_client_t *cli, std::function<void(int)> cb)
{
    cluster_op_t *op = new cluster_op_t();
    op->opcode = OSD_OP_SYNC;
    op->callback = [cb](cluster_op_t *op)
    {
        int retval = op->retval;
        delete op;
        if (cb != NULL)
            cb(retval);
    };
    cli->execute(op);
}

struct test_data_t
{
    cluster_client_t *cli = NULL;
    ring_loop_t *ringloop = NULL;
    inode_t inode = 0;
    uint64_t offset = 0;
    bool done = false;
    uint64_t base_ver = 0;

    void wait()
    {
        while (!done)
        {
            ringloop->loop();
            if (done)
                break;
            ringloop->wait();
        }
    }
};

void test_basic(test_data_t *data, int state, int r, uint64_t v)
{
    if (state == 1)
        goto resume_1;
    else if (state == 2)
        goto resume_2;
    else if (state == 3)
        goto resume_3;
    else if (state == 4)
        goto resume_4;
    else if (state == 5)
        goto resume_5;
    else
        assert(state == 0);
    printf("Basic CAS test...\n");
    data->done = false;
    send_read(data->cli, data->inode, data->offset, [data](int r, uint64_t v, uint8_t b) { test_basic(data, 1, r, v); });
    return;
resume_1:
    if (r < 0)
    {
        fprintf(stderr, "Initial read operation failed\n");
        exit(1);
    }
    data->base_ver = v;
    // CAS v=1 = compare with zero, non-existing object
    send_write(data->cli, data->inode, data->offset, 0x01, data->base_ver+1, [data](int r) { test_basic(data, 2, r, 0); });
    return;
resume_2:
    if (r < 0)
    {
        fprintf(stderr, "CAS for non-existing object failed\n");
        exit(1);
    }
    // Check that read returns the new version
    send_read(data->cli, data->inode, data->offset, [data](int r, uint64_t v, uint8_t b) { test_basic(data, 3, r, v); });
    return;
resume_3:
    if (r < 0)
    {
        fprintf(stderr, "Read operation failed after write\n");
        exit(1);
    }
    if (v != data->base_ver+1)
    {
        fprintf(stderr, "Read operation failed to return the new version number\n");
        exit(1);
    }
    // CAS v=2 = compare with v=1, existing object
    send_write(data->cli, data->inode, data->offset, 0x02, data->base_ver+2, [data](int r) { test_basic(data, 4, r, 0); });
    return;
resume_4:
    if (r < 0)
    {
        fprintf(stderr, "CAS for existing object failed\n");
        exit(1);
    }
    // CAS v=2 again = compare with v=1, but version is 2. Must fail with -EINTR
    send_write(data->cli, data->inode, data->offset, 0x03, data->base_ver+2, [data](int r) { test_basic(data, 5, r, 0); });
    return;
resume_5:
    if (r != -EINTR)
    {
        fprintf(stderr, "CAS conflict detection failed\n");
        exit(1);
    }
    printf("Basic CAS test succeeded\n");
    data->offset += 0x200000;
    data->done = true;
}

void test_mixed(test_data_t *data, int state, int r, uint64_t v, uint8_t b)
{
    if (state == 1)
        goto resume_1;
    else if (state == 2)
        goto resume_2;
    else if (state == 3)
        goto resume_3;
    else if (state == 4)
        goto resume_4;
    else if (state == 5)
        goto resume_5;
    else if (state == 6)
        goto resume_6;
    else if (state == 7)
        goto resume_7;
    else if (state == 8)
        goto resume_8;
    else if (state == 9)
        goto resume_9;
    else
        assert(state == 0);
    data->done = false;
    if (data->cli->get_immediate_commit(data->inode))
    {
        printf("Mixed CAS/non-CAS write test skipped because immediate_commit is active and writeback cache is disabled\n");
        data->done = true;
        return;
    }
    printf("Mixed CAS/non-CAS write test...\n");
    // Simple read to initialize OSD connection
    send_read(data->cli, data->inode, data->offset, [data](int r, uint64_t v, uint8_t b) { test_mixed(data, 1, r, v, b); });
    return;
resume_1:
    if (r < 0)
    {
        fprintf(stderr, "Read operation failed: %d\n", r);
        exit(1);
    }
    // CAS write
    send_write(data->cli, data->inode, data->offset, 0x01, 1, [data](int r) { test_mixed(data, 2, r, 0, 0); });
    return;
resume_2:
    if (r < 0)
    {
        fprintf(stderr, "CAS write operation failed: %d\n", r);
        exit(1);
    }
    // Non-CAS write (write-repeat)
    send_write(data->cli, data->inode, data->offset, 0x02, 0, [data](int r) { test_mixed(data, 3, r, 0, 0); });
    return;
resume_3:
    if (r < 0)
    {
        fprintf(stderr, "Non-CAS write operation failed: %d\n", r);
        exit(1);
    }
    // Read object - it can't return the real version due to active write-back
    send_read(data->cli, data->inode, data->offset, [data](int r, uint64_t v, uint8_t b) { test_mixed(data, 4, r, v, b); });
    return;
resume_4:
    if (r < 0)
    {
        fprintf(stderr, "Read operation failed after write: %d\n", r);
        exit(1);
    }
    if (b != 0x02)
    {
        fprintf(stderr, "Read after a write-back/write-repeat failed to return data\n");
        exit(1);
    }
    if (v != 0)
    {
        fprintf(stderr, "Read after a write-back/write-repeat returns the version... Cool, but unexpected\n");
        exit(1);
    }
    // Write it anyway - it should flush part of the cache and return -EINTR (if write-repeat) or OK (if write-back)
    send_write(data->cli, data->inode, data->offset, 0x03, 2, [data](int r) { test_mixed(data, 5, r, 0, 0); });
    return;
resume_5:
    if (r < 0 && r != -EINTR)
    {
        fprintf(stderr, "CAS write operation failed: %d\n", r);
        exit(1);
    }
    if (r == -EINTR)
    {
        // Retry write - now we should get the version and succeed
        printf("...first CAS write returned -EINTR - OK, retrying\n");
        send_read(data->cli, data->inode, data->offset, [data](int r, uint64_t v, uint8_t b) { test_mixed(data, 6, r, v, b); });
        return;
resume_6:
        if (r < 0)
        {
            fprintf(stderr, "Read operation failed after write: %d\n", r);
            exit(1);
        }
        if (v == 0)
        {
            fprintf(stderr, "CAS write failed to invalidate the write-back cache and read returned version 0\n");
            exit(1);
        }
        data->base_ver = v;
        send_write(data->cli, data->inode, data->offset, 0x03, data->base_ver+1, [data](int r) { test_mixed(data, 7, r, 0, 0); });
        return;
resume_7:
        if (r < 0)
        {
            fprintf(stderr, "CAS write retry failed: %d\n", r);
            exit(1);
        }
    }
    // Now additionally do an fsync (if anything is left in the write-back cache, it can kill our CAS write)
    send_sync(data->cli, [data](int r) { test_mixed(data, 8, r, 0, 0); });
    return;
resume_8:
    if (r < 0)
    {
        fprintf(stderr, "SYNC failed: %d\n", r);
        exit(1);
    }
    // Check read
    send_read(data->cli, data->inode, data->offset, [data](int r, uint64_t v, uint8_t b) { test_mixed(data, 9, r, v, b); });
    return;
resume_9:
    if (r < 0)
    {
        fprintf(stderr, "Read operation failed after write: %d\n", r);
        exit(1);
    }
    if (b != 0x03)
    {
        fprintf(stderr, "CAS write data lost: 0x%02x is returned instead of 0x02\n", b);
        exit(1);
    }
    printf("Mixed CAS/non-CAS write test succeeded\n");
    data->offset += 0x200000;
    data->done = true;
}

void test_read_bypass(test_data_t *data, int state, int r, uint64_t v, uint8_t b)
{
    if (state == 1)
        goto resume_1;
    else if (state == 2)
        goto resume_2;
    else
        assert(state == 0);
    data->done = false;
    if (data->cli->get_immediate_commit(data->inode))
    {
        printf("Read cache bypass test skipped because immediate_commit is active\n");
        data->done = true;
        return;
    }
    printf("Read cache bypass test...\n");
    // Non-CAS write (write-repeat)
    send_write(data->cli, data->inode, data->offset, 0x01, 0, [data](int r) { test_read_bypass(data, 1, r, 0, 0); });
    return;
resume_1:
    if (r < 0)
    {
        fprintf(stderr, "Non-CAS write operation failed: %d\n", r);
        exit(1);
    }
    // Read object with bypass flag
    send_read(data->cli, data->inode, data->offset, [data](int r, uint64_t v, uint8_t b) { test_read_bypass(data, 2, r, v, b); }, OSD_OP_IGNORE_WRITEBACK);
    return;
resume_2:
    if (r < 0)
    {
        fprintf(stderr, "Read operation failed after write: %d\n", r);
        exit(1);
    }
    if (b != 0x01)
    {
        fprintf(stderr, "Read after a write-repeat failed to return data\n");
        exit(1);
    }
    if (v == 0)
    {
        fprintf(stderr, "Read failed to bypass cache after a write-repeat\n");
        exit(1);
    }
    printf("Read cache bypass test succeeded\n");
    data->offset += 0x200000;
    data->done = true;
}

int main(int narg, char *args[])
{
    setvbuf(stdout, NULL, _IONBF, 0);
    setvbuf(stderr, NULL, _IONBF, 0);
    json11::Json::object cfgo;
    for (int i = 1; i < narg; i++)
    {
        if (args[i][0] == '-' && args[i][1] == '-')
        {
            const char *opt = args[i]+2;
            cfgo[opt] = i == narg-1 ? "1" : args[++i];
        }
    }
    cfgo["client_enable_writeback"] = "0";
    cfgo["client_writeback_allowed"] = "0";
    json11::Json cfg(cfgo);
    uint64_t inode = (cfg["pool_id"].uint64_value() << (64-POOL_ID_BITS))
        | cfg["inode_id"].uint64_value();
    // Create client
    auto ringloop = new ring_loop_t(RINGLOOP_DEFAULT_SIZE);
    auto epmgr = new epoll_manager_t(ringloop);
    auto cli = cluster_client_t::create(ringloop, epmgr->tfd, cfg);
    test_data_t data = { .cli = cli, .ringloop = ringloop, inode = inode };
    // Wait for init
    cli->on_ready([&]() { data.done = true; });
    data.wait();
    // Run test_basic()
    test_basic(&data, 0, 0, 0);
    data.wait();
    // Run test_mixed()
    test_mixed(&data, 0, 0, 0, 0);
    data.wait();
    // Run test_read_bypass()
    test_read_bypass(&data, 0, 0, 0, 0);
    data.wait();
    // Reinitialize client with writeback
    printf("Reinitializing with writeback\n");
    delete cli;
    cfgo["client_enable_writeback"] = "1";
    cfgo["client_writeback_allowed"] = "1";
    cfg = cfgo;
    cli = cluster_client_t::create(ringloop, epmgr->tfd, cfg);
    data.cli = cli;
    data.done = false;
    cli->on_ready([&]() { data.done = true; });
    data.wait();
    // Run test_mixed() once more, now with writeback
    test_mixed(&data, 0, 0, 0, 0);
    data.wait();
    // Cleanup
    delete cli;
    delete epmgr;
    delete ringloop;
    return 0;
}
