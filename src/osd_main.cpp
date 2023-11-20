// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "osd.h"

#include <signal.h>

static osd_t *osd = NULL;
static bool force_stopping = false;

static void handle_sigint(int sig)
{
    if (osd && !force_stopping)
    {
        force_stopping = true;
        osd->force_stop(0);
        return;
    }
    exit(0);
}

static const char* help_text =
    "Vitastor OSD (block object storage daemon) " VERSION "\n"
    "(c) Vitaliy Filippov, 2019+ (VNPL-1.1)\n"
    "\n"
    "OSDs are usually started by vitastor-disk.\n"
    "Manual usage: vitastor-osd [--option value] ...\n"
;

int main(int narg, char *args[])
{
    setvbuf(stdout, NULL, _IONBF, 0);
    setvbuf(stderr, NULL, _IONBF, 0);
    if (sizeof(osd_any_op_t) > OSD_PACKET_SIZE ||
        sizeof(osd_any_reply_t) > OSD_PACKET_SIZE)
    {
        perror("BUG: too small packet size");
        return 1;
    }
    json11::Json::object config;
    for (int i = 1; i < narg; i++)
    {
        if (args[i][0] == '-' && args[i][1] == '-' && i < narg-1)
        {
            char *opt = args[i]+2;
            config[std::string(opt)] = std::string(args[++i]);
        }
        else if (!strcmp(args[i], "--help"))
        {
            printf("%s", help_text);
            return 0;
        }
    }
    if (!config.size())
    {
        printf("%s", help_text);
        return 1;
    }
    signal(SIGINT, handle_sigint);
    signal(SIGTERM, handle_sigint);
    ring_loop_t *ringloop = new ring_loop_t(RINGLOOP_DEFAULT_SIZE);
    osd = new osd_t(config, ringloop);
    while (1)
    {
        ringloop->loop();
        ringloop->wait();
    }
    delete osd;
    delete ringloop;
    return 0;
}
