// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include "cluster_client.h"
#include "etcd_state_client_http.h"

cluster_client_t* cluster_client_t::create(ring_loop_t *ringloop, timerfd_manager_t *tfd, json11::Json config)
{
    auto st_cli = new etcd_state_client_http_t(tfd);
    return new cluster_client_t(ringloop, tfd, config, std::unique_ptr<etcd_state_client_t>(st_cli));
}
