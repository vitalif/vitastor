// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <malloc.h>
#include "blockstore.h"
#include "epoll_manager.h"

int main(int narg, char *args[])
{
    blockstore_config_t config;
    config["meta_device"] = "./test_meta.bin";
    config["journal_device"] = "./test_journal.bin";
    config["data_device"] = "./test_data.bin";
    ring_loop_t *ringloop = new ring_loop_t(RINGLOOP_DEFAULT_SIZE);
    epoll_manager_t *epmgr = new epoll_manager_t(ringloop);
    blockstore_i *bs = blockstore_i::create(config, ringloop, epmgr->tfd);

    blockstore_op_t op;
    int main_state = 0;
    uint64_t version = 0;
    ring_consumer_t main_cons;
    op.callback = [&](blockstore_op_t *op)
    {
        printf("op completed %d\n", op->retval);
        if (main_state == 1)
            main_state = 2;
        else if (main_state == 3)
            main_state = 4;
        else if (main_state == 5)
            main_state = 6;
        else if (main_state == 7)
            main_state = 8;
        else if (main_state == 9)
            main_state = 10;
    };
    main_cons.loop = [&]()
    {
        if (main_state == 0)
        {
            if (bs->is_started())
            {
                printf("init completed\n");
                op.opcode = BS_OP_WRITE;
                op.oid = { .inode = 1, .stripe = 0 };
                op.version = 0;
                op.offset = 16384;
                op.len = 4096;
                op.buf = (uint8_t*)memalign(512, 128*1024);
                memset(op.buf, 0xaa, 4096);
                bs->enqueue_op(&op);
                main_state = 1;
            }
        }
        else if (main_state == 2)
        {
            printf("version %ju written, syncing\n", op.version);
            version = op.version;
            op.opcode = BS_OP_SYNC;
            bs->enqueue_op(&op);
            main_state = 3;
        }
        else if (main_state == 4)
        {
            printf("stabilizing version %ju\n", version);
            op.opcode = BS_OP_STABLE;
            op.len = 1;
            *((obj_ver_id*)op.buf) = {
                .oid = { .inode = 1, .stripe = 0 },
                .version = version,
            };
            bs->enqueue_op(&op);
            main_state = 5;
        }
        else if (main_state == 6)
        {
            printf("stabilizing version %ju\n", version);
            op.opcode = BS_OP_STABLE;
            op.len = 1;
            *((obj_ver_id*)op.buf) = {
                .oid = { .inode = 1, .stripe = 0 },
                .version = version,
            };
            bs->enqueue_op(&op);
            main_state = 7;
        }
        else if (main_state == 8)
        {
            printf("reading 0-128K\n");
            op.opcode = BS_OP_READ;
            op.oid = { .inode = 1, .stripe = 0 };
            op.version = UINT64_MAX;
            op.offset = 0;
            op.len = 128*1024;
            bs->enqueue_op(&op);
            main_state = 9;
        }
        else if (main_state == 10)
        {
            void *cmp = memalign(512, 128*1024);
            memset(cmp, 0, 128*1024);
            memset(cmp+16384, 0xaa, 4096);
            int ok = 1;
            for (int i = 0; i < 128*1024; i += 4096)
            {
                if (memcmp(cmp+i, op.buf+i, 4096) != 0)
                {
                    printf("bitmap works incorrectly, bytes %d - %d differ (%02x, should be %02x)\n", i, i+4096, ((uint8_t*)op.buf)[i], ((uint8_t*)cmp)[i]);
                    ok = 0;
                }
            }
            if (ok)
                printf("bitmap works correctly\n");
            free(cmp);
            main_state = 11;
        }
    };

    ringloop->register_consumer(&main_cons);
    while (1)
    {
        ringloop->loop();
        ringloop->wait();
    }
    delete bs;
    delete epmgr;
    delete ringloop;
    return 0;
}
