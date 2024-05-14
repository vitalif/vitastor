// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <stdio.h>
#include <stdlib.h>

#include "epoll_manager.h"
#include "cluster_client.h"

void send_read(cluster_client_t *cli, uint64_t inode, std::function<void(int, uint64_t)> cb)
{
    cluster_op_t *op = new cluster_op_t();
    op->opcode = OSD_OP_READ;
    op->inode = inode;
    op->offset = 0;
    op->len = 4096;
    op->iov.push_back(malloc_or_die(op->len), op->len);
    op->callback = [cb](cluster_op_t *op)
    {
        uint64_t version = op->version;
        int retval = op->retval;
        if (retval == op->len)
            retval = 0;
        free(op->iov.buf[0].iov_base);
        delete op;
        if (cb != NULL)
            cb(retval, version);
    };
    cli->execute(op);
}

void send_write(cluster_client_t *cli, uint64_t inode, int byte, uint64_t version, std::function<void(int)> cb)
{
    cluster_op_t *op = new cluster_op_t();
    op->opcode = OSD_OP_WRITE;
    op->inode = inode;
    op->offset = 0;
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

int main(int narg, char *args[])
{
    json11::Json::object cfgo;
    for (int i = 1; i < narg; i++)
    {
        if (args[i][0] == '-' && args[i][1] == '-')
        {
            const char *opt = args[i]+2;
            cfgo[opt] = i == narg-1 ? "1" : args[++i];
        }
    }
    json11::Json cfg(cfgo);
    uint64_t inode = (cfg["pool_id"].uint64_value() << (64-POOL_ID_BITS))
        | cfg["inode_id"].uint64_value();
    uint64_t base_ver = 0;
    // Create client
    auto ringloop = new ring_loop_t(RINGLOOP_DEFAULT_SIZE);
    auto epmgr = new epoll_manager_t(ringloop);
    auto cli = new cluster_client_t(ringloop, epmgr->tfd, cfg);
    cli->on_ready([&]()
    {
        send_read(cli, inode, [&](int r, uint64_t v)
        {
            if (r < 0)
            {
                fprintf(stderr, "Initial read operation failed\n");
                exit(1);
            }
            base_ver = v;
            // CAS v=1 = compare with zero, non-existing object
            send_write(cli, inode, 0x01, base_ver+1, [&](int r)
            {
                if (r < 0)
                {
                    fprintf(stderr, "CAS for non-existing object failed\n");
                    exit(1);
                }
                // Check that read returns the new version
                send_read(cli, inode, [&](int r, uint64_t v)
                {
                    if (r < 0)
                    {
                        fprintf(stderr, "Read operation failed after write\n");
                        exit(1);
                    }
                    if (v != base_ver+1)
                    {
                        fprintf(stderr, "Read operation failed to return the new version number\n");
                        exit(1);
                    }
                    // CAS v=2 = compare with v=1, existing object
                    send_write(cli, inode, 0x02, base_ver+2, [&](int r)
                    {
                        if (r < 0)
                        {
                            fprintf(stderr, "CAS for existing object failed\n");
                            exit(1);
                        }
                        // CAS v=2 again = compare with v=1, but version is 2. Must fail with -EINTR
                        send_write(cli, inode, 0x03, base_ver+2, [&](int r)
                        {
                            if (r != -EINTR)
                            {
                                fprintf(stderr, "CAS conflict detection failed\n");
                                exit(1);
                            }
                            printf("Basic CAS test succeeded\n");
                            exit(0);
                        });
                    });
                });
            });
        });
    });
    while (1)
    {
        ringloop->loop();
        ringloop->wait();
    }
    return 0;
}
