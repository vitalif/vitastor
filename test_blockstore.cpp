#include <iostream>
#include "timerfd_interval.h"
#include "blockstore.h"

int main(int narg, char *args[])
{
    spp::sparse_hash_map<std::string, std::string> config;
    config["meta_device"] = "./test_meta.bin";
    config["journal_device"] = "./test_journal.bin";
    config["data_device"] = "./test_data.bin";
    ring_loop_t *ringloop = new ring_loop_t(512);
    blockstore *bs = new blockstore(config, ringloop);
    timerfd_interval tick_tfd(ringloop, 1, []()
    {
        printf("tick 1s\n");
    });

    blockstore_operation op;
    int main_state = 0;
    uint64_t version = 0;
    ring_consumer_t main_cons;
    op.callback = [&](blockstore_operation *op)
    {
        printf("op completed %d\n", op->retval);
        if (main_state == 1)
            main_state = 2;
        else if (main_state == 3)
            main_state = 4;
        else if (main_state == 5)
            main_state = 6;
    };
    main_cons.loop = [&]()
    {
        if (main_state == 0)
        {
            if (bs->is_started())
            {
                printf("init completed\n");
                op.flags = OP_WRITE;
                op.oid = { .inode = 1, .stripe = 0 };
                op.version = 0;
                op.offset = 4096;
                op.len = 4096;
                op.buf = (uint8_t*)memalign(512, 4096);
                memset(op.buf, 0xaa, 4096);
                bs->enqueue_op(&op);
                main_state = 1;
            }
        }
        else if (main_state == 2)
        {
            printf("version %u written, syncing\n", op.version);
            version = op.version;
            op.flags = OP_SYNC;
            bs->enqueue_op(&op);
            main_state = 3;
        }
        else if (main_state == 4)
        {
            printf("stabilizing version %u\n", version);
            op.flags = OP_STABLE;
            op.len = 1;
            *((obj_ver_id*)op.buf) = {
                .oid = { .inode = 1, .stripe = 0 },
                .version = version,
            };
            bs->enqueue_op(&op);
            main_state = 5;
        }
    };

    ringloop->register_consumer(main_cons);
    while (1)
    {
        ringloop->loop();
        ringloop->wait();
    }
    delete bs;
    delete ringloop;
    return 0;
}
