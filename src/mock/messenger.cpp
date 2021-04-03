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
    clients[cur_op->peer_fd]->sent_ops[cur_op->req.hdr.id] = cur_op;
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
