// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include "osd.h"
#include "etcd_state_client_mock.h"
#include "ringloop_mock.h"
#include "blockstore_mock.h"

void test1()
{
    json11::Json config;
    timerfd_manager_t *tfd = new timerfd_manager_t([](int fd, bool wr, std::function<void(int, int)> callback){});
    etcd_state_client_mock_t *st_cli = new etcd_state_client_mock_t();
    ring_loop_mock_t *ringloop = new ring_loop_mock_t(RINGLOOP_DEFAULT_SIZE, [&](io_uring_sqe *sqe) {});
    st_cli->pause();
    osd_t *osd = new osd_t(config, ringloop, tfd, std::unique_ptr<etcd_state_client_t>(st_cli), [](blockstore_config_t & cfg)
    {
        return new blockstore_mock_t({});
    });
}

int main(int narg, char *args[])
{
    test1();
    return 0;
}
