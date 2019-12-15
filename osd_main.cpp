#include "osd.h"

int main(int narg, char *args[])
{
    if (sizeof(osd_any_op_t) >= OSD_OP_PACKET_SIZE ||
        sizeof(osd_any_reply_t) >= OSD_REPLY_PACKET_SIZE)
    {
        perror("BUG: too small packet size");
        return 1;
    }
    blockstore_config_t config;
    config["meta_device"] = "./test_meta.bin";
    config["journal_device"] = "./test_journal.bin";
    config["data_device"] = "./test_data.bin";
    ring_loop_t *ringloop = new ring_loop_t(512);
    blockstore_t *bs = new blockstore_t(config, ringloop);
    osd_t *osd = new osd_t(config, bs, ringloop);
    while (1)
    {
        ringloop->loop();
        ringloop->wait();
    }
    delete osd;
    delete bs;
    delete ringloop;
    return 0;
}
