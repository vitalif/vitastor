#include "osd.h"

#include <signal.h>

void handle_sigint(int sig)
{
    exit(0);
}

int main(int narg, char *args[])
{
    if (sizeof(osd_any_op_t) > OSD_PACKET_SIZE ||
        sizeof(osd_any_reply_t) > OSD_PACKET_SIZE)
    {
        perror("BUG: too small packet size");
        return 1;
    }
    blockstore_config_t config;
    for (int i = 1; i < narg; i++)
    {
        if (args[i][0] == '-' && args[i][1] == '-' && i < narg-1)
        {
            char *opt = args[i]+2;
            config[opt] = args[++i];
        }
    }
    signal(SIGINT, handle_sigint);
    ring_loop_t *ringloop = new ring_loop_t(512);
    // FIXME: Create Blockstore from on-disk superblock config and check it against the OSD cluster config
    // FIXME: Prevent two OSD starting with same number
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
