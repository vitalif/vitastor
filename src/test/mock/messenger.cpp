// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include <unistd.h>
#include <stdexcept>
#include <assert.h>

#include "messenger.h"

void osd_messenger_t::init()
{
}

osd_messenger_t::~osd_messenger_t()
{
    while (clients.size() > 0)
    {
        stop_client(clients.begin()->first, true);
    }
}

void osd_messenger_t::outbox_push(osd_op_t *cur_op)
{
    auto cl = clients.at(cur_op->client_id);
    if (cur_op->op_type == OSD_OP_OUT)
        cur_op->req.hdr.id = ++cl->send_op_id;
    cl->sent_ops[cur_op->req.hdr.id] = cur_op;
}

void osd_messenger_t::parse_config(const json11::Json & config)
{
}

void osd_messenger_t::connect_peer(uint64_t peer_osd, json11::Json peer_state)
{
    wanted_peers[peer_osd] = (osd_wanted_peer_t){
        .port = 1,
    };
}

void osd_messenger_t::read_requests()
{
}

void osd_messenger_t::send_replies()
{
}

json11::Json::object osd_messenger_t::read_config(const json11::Json & config)
{
    return json11::Json::object();
}

json11::Json::object osd_messenger_t::merge_configs(const json11::Json::object & cli_config,
    const json11::Json::object & file_config,
    const json11::Json::object & etcd_global_config,
    const json11::Json::object & etcd_osd_config)
{
    return cli_config;
}

void osd_messenger_t::destroy_rdma_conn(msgr_rdma_connection_t *rdma_conn)
{
}

void osd_messenger_t::accept_connections(int listen_fd)
{
}

#ifdef WITH_RDMA
json11::Json osd_messenger_t::connect_rdma(uint64_t client_id, std::string rdma_address, uint64_t client_max_msg)
{
    return json11::Json();
}

bool osd_messenger_t::is_rdma_enabled()
{
    return false;
}
#endif

#ifdef WITH_RDMACM
rdma_cm_id *osd_messenger_t::rdmacm_listen(const std::string & bind_address, int rdmacm_port, int *bound_port, int log_level)
{
    return NULL;
}

void osd_messenger_t::rdmacm_destroy_listener(rdma_cm_id *listener)
{
}
#endif
