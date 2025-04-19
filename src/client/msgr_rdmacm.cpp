// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include "msgr_rdma.h"
#include "messenger.h"

struct rdmacm_connecting_t
{
    rdma_cm_id *cmid = NULL;
    int peer_fd = -1;
    osd_num_t peer_osd = 0;
    std::string addr;
    sockaddr_storage parsed_addr = {};
    int rdmacm_port = 0;
    int tcp_port = 0;
    int timeout_ms = 0;
    int timeout_id = -1;
    msgr_rdma_context_t *rdma_context = NULL;
};

rdma_cm_id *osd_messenger_t::rdmacm_listen(const std::string & bind_address, int rdmacm_port, int *bound_port, int log_level)
{
    sockaddr_storage addr = {};
    rdma_cm_id *listener = NULL;
    int r = rdma_create_id(rdmacm_evch, &listener, NULL, RDMA_PS_TCP);
    if (r != 0)
    {
        fprintf(stderr, "Failed to create RDMA-CM ID: %s (code %d)\n", strerror(errno), errno);
        goto fail;
    }
    if (!string_to_addr(bind_address, 0, rdmacm_port, &addr))
    {
        fprintf(stderr, "Server address: %s is not valid\n", bind_address.c_str());
        goto fail;
    }
    r = rdma_bind_addr(listener, (sockaddr*)&addr);
    if (r != 0)
    {
        fprintf(stderr, "Failed to bind RDMA-CM to %s:%d: %s (code %d)\n", bind_address.c_str(), rdmacm_port, strerror(errno), errno);
        goto fail;
    }
    r = rdma_listen(listener, 128);
    if (r != 0)
    {
        fprintf(stderr, "Failed to listen to RDMA-CM address %s:%d: %s (code %d)\n", bind_address.c_str(), rdmacm_port, strerror(errno), errno);
        goto fail;
    }
    if (bound_port)
    {
        *bound_port = ntohs(rdma_get_src_port(listener));
    }
    if (log_level > 0)
    {
        fprintf(stderr, "Listening to RDMA-CM address %s port %d\n", bind_address.c_str(), *bound_port);
    }
    return listener;
fail:
    rdma_destroy_id(listener);
    return NULL;
}

void osd_messenger_t::rdmacm_destroy_listener(rdma_cm_id *listener)
{
    rdma_destroy_id(listener);
}

void osd_messenger_t::handle_rdmacm_events()
{
    // rdma_destroy_id infinitely waits for pthread_cond if called before all events are acked :-(...
    std::vector<rdma_cm_event> events_copy;
    while (1)
    {
        rdma_cm_event *ev = NULL;
        int r = rdma_get_cm_event(rdmacm_evch, &ev);
        if (r != 0)
        {
            if (errno == EAGAIN || errno == EINTR)
                break;
            fprintf(stderr, "Failed to get RDMA-CM event: %s (code %d)\n", strerror(errno), errno);
            exit(1);
        }
        // ...so we save a copy of all events EXCEPT connection requests, otherwise they sometimes fail with EVENT_DISCONNECT
        if (ev->event == RDMA_CM_EVENT_CONNECT_REQUEST)
        {
            rdmacm_accept(ev);
        }
        else
        {
            events_copy.push_back(*ev);
        }
        r = rdma_ack_cm_event(ev);
        if (r != 0)
        {
            fprintf(stderr, "Failed to ack (free) RDMA-CM event: %s (code %d)\n", strerror(errno), errno);
            exit(1);
        }
    }
    for (auto & evl: events_copy)
    {
        auto ev = &evl;
        if (ev->event == RDMA_CM_EVENT_CONNECT_REQUEST)
        {
            // Do nothing, handled above
        }
        else if (ev->event == RDMA_CM_EVENT_CONNECT_ERROR ||
            ev->event == RDMA_CM_EVENT_REJECTED ||
            ev->event == RDMA_CM_EVENT_DISCONNECTED ||
            ev->event == RDMA_CM_EVENT_DEVICE_REMOVAL)
        {
            auto event_type_name = ev->event == RDMA_CM_EVENT_CONNECT_ERROR ? "RDMA_CM_EVENT_CONNECT_ERROR" : (
                ev->event == RDMA_CM_EVENT_REJECTED ? "RDMA_CM_EVENT_REJECTED" : (
                ev->event == RDMA_CM_EVENT_DISCONNECTED ? "RDMA_CM_EVENT_DISCONNECTED" : "RDMA_CM_EVENT_DEVICE_REMOVAL"));
            auto cli_it = rdmacm_connections.find(ev->id);
            if (cli_it != rdmacm_connections.end())
            {
                fprintf(stderr, "Received %s event for peer %d, closing connection\n",
                    event_type_name, cli_it->second->peer_fd);
                stop_client(cli_it->second->peer_fd);
            }
            else if (rdmacm_connecting.find(ev->id) != rdmacm_connecting.end())
            {
                fprintf(stderr, "Received %s event for RDMA-CM OSD %ju connection\n",
                    event_type_name, rdmacm_connecting[ev->id]->peer_osd);
                rdmacm_established(ev);
            }
            else
            {
                fprintf(stderr, "Received %s event for an unknown RDMA-CM connection 0x%jx - ignoring\n",
                    event_type_name, (uint64_t)ev->id);
            }
        }
        else if (ev->event == RDMA_CM_EVENT_ADDR_RESOLVED || ev->event == RDMA_CM_EVENT_ADDR_ERROR)
        {
            rdmacm_address_resolved(ev);
        }
        else if (ev->event == RDMA_CM_EVENT_ROUTE_RESOLVED || ev->event == RDMA_CM_EVENT_ROUTE_ERROR)
        {
            rdmacm_route_resolved(ev);
        }
        else if (ev->event == RDMA_CM_EVENT_CONNECT_RESPONSE)
        {
            // Just OK
        }
        else if (ev->event == RDMA_CM_EVENT_UNREACHABLE || ev->event == RDMA_CM_EVENT_REJECTED)
        {
            // Handle error
            rdmacm_established(ev);
        }
        else if (ev->event == RDMA_CM_EVENT_ESTABLISHED)
        {
            rdmacm_established(ev);
        }
        else if (ev->event == RDMA_CM_EVENT_ADDR_CHANGE || ev->event == RDMA_CM_EVENT_TIMEWAIT_EXIT)
        {
            // Do nothing
        }
        else
        {
            // Other events are unexpected
            fprintf(stderr, "Unexpected RDMA-CM event type: %d\n", ev->event);
        }
    }
}

msgr_rdma_context_t* msgr_rdma_context_t::create_cm(ibv_context *ctx)
{
    auto rdma_context = new msgr_rdma_context_t;
    rdma_context->is_cm = true;
    rdma_context->context = ctx;
    rdma_context->pd = ibv_alloc_pd(ctx);
    if (!rdma_context->pd)
    {
        fprintf(stderr, "Couldn't allocate RDMA protection domain\n");
        delete rdma_context;
        return NULL;
    }
    rdma_context->odp = false;
    rdma_context->channel = ibv_create_comp_channel(rdma_context->context);
    if (!rdma_context->channel)
    {
        fprintf(stderr, "Couldn't create RDMA completion channel\n");
        delete rdma_context;
        return NULL;
    }
    rdma_context->max_cqe = 4096;
    rdma_context->cq = ibv_create_cq(rdma_context->context, rdma_context->max_cqe, NULL, rdma_context->channel, 0);
    if (!rdma_context->cq)
    {
        fprintf(stderr, "Couldn't create RDMA completion queue\n");
        delete rdma_context;
        return NULL;
    }
    if (ibv_query_device_ex(rdma_context->context, NULL, &rdma_context->attrx))
    {
        fprintf(stderr, "Couldn't query RDMA device for its features\n");
        delete rdma_context;
        return NULL;
    }
    return rdma_context;
}

msgr_rdma_context_t* osd_messenger_t::rdmacm_get_context(ibv_context *verbs)
{
    // Find the context by device
    // We assume that RDMA_CM ev->id->verbs is always the same for the same device (but PD for example isn't)
    msgr_rdma_context_t *rdma_context = NULL;
    for (auto ctx: rdma_contexts)
    {
        if (ctx->context == verbs)
        {
            rdma_context = ctx;
            break;
        }
    }
    if (!rdma_context)
    {
        // Wrap into a new msgr_rdma_context_t
        rdma_context = msgr_rdma_context_t::create_cm(verbs);
        if (!rdma_context)
            return NULL;
        fcntl(rdma_context->channel->fd, F_SETFL, fcntl(rdma_context->channel->fd, F_GETFL, 0) | O_NONBLOCK);
        tfd->set_fd_handler(rdma_context->channel->fd, false, [this, rdma_context](int notify_fd, int epoll_events)
        {
            handle_rdma_events(rdma_context);
        });
        handle_rdma_events(rdma_context);
        rdma_contexts.push_back(rdma_context);
    }
    return rdma_context;
}

msgr_rdma_context_t* osd_messenger_t::rdmacm_create_qp(rdma_cm_id *cmid)
{
    auto rdma_context = rdmacm_get_context(cmid->verbs);
    if (!rdma_context)
    {
        return NULL;
    }
    rdma_context->reserve_cqe(rdma_max_send+rdma_max_recv);
    auto max_sge = rdma_max_sge > rdma_context->attrx.orig_attr.max_sge
        ? rdma_context->attrx.orig_attr.max_sge : rdma_max_sge;
    ibv_qp_init_attr init_attr = {
        .send_cq = rdma_context->cq,
        .recv_cq = rdma_context->cq,
        .cap     = {
            .max_send_wr  = (uint32_t)rdma_max_send,
            .max_recv_wr  = (uint32_t)rdma_max_recv,
            .max_send_sge = (uint32_t)max_sge,
            .max_recv_sge = (uint32_t)max_sge,
        },
        .qp_type = IBV_QPT_RC,
    };
    int r = rdma_create_qp(cmid, rdma_context->pd, &init_attr);
    if (r != 0)
    {
        fprintf(stderr, "Failed to create a queue pair via RDMA-CM: %s (code %d)\n", strerror(errno), errno);
        rdma_context->reserve_cqe(-rdma_max_send-rdma_max_recv);
        return NULL;
    }
    return rdma_context;
}

void osd_messenger_t::rdmacm_accept(rdma_cm_event *ev)
{
    // Make a fake FD (FIXME: do not use FDs for identifying clients!)
    int fake_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fake_fd < 0)
    {
        fprintf(stderr, "Failed to allocate a fake socket for RDMA-CM client: %s (code %d)\n", strerror(errno), errno);
        rdma_destroy_id(ev->id);
        return;
    }
    auto rdma_context = rdmacm_create_qp(ev->id);
    if (!rdma_context)
    {
        rdma_destroy_id(ev->id);
        return;
    }
    // We don't need private_data, RDMA_READ or ATOMIC so use default 1
    rdma_conn_param conn_params = {
        .responder_resources = 1,
        .initiator_depth = 1,
        .retry_count = 7,
        .rnr_retry_count = 7,
    };
    if (rdma_accept(ev->id, &conn_params) != 0)
    {
        fprintf(stderr, "Failed to accept RDMA-CM connection: %s (code %d)\n", strerror(errno), errno);
        rdma_context->reserve_cqe(-rdma_max_send-rdma_max_recv);
        rdma_destroy_qp(ev->id);
        rdma_destroy_id(ev->id);
        return;
    }
    // Wait for RDMA_CM_ESTABLISHED, and enable the connection only after it
    auto conn = new rdmacm_connecting_t;
    conn->cmid = ev->id;
    conn->peer_fd = fake_fd;
    conn->parsed_addr = *(sockaddr_storage*)rdma_get_peer_addr(ev->id);
    conn->rdma_context = rdma_context;
    rdmacm_set_conn_timeout(conn);
    rdmacm_connecting[ev->id] = conn;
    fprintf(stderr, "[OSD %ju] new client %d: connection from %s via RDMA-CM\n", this->osd_num, conn->peer_fd,
        addr_to_string(conn->parsed_addr).c_str());
}

void osd_messenger_t::rdmacm_set_conn_timeout(rdmacm_connecting_t *conn)
{
    conn->timeout_ms = peer_connect_timeout*1000;
    if (peer_connect_timeout > 0)
    {
        conn->timeout_id = tfd->set_timer(1000*peer_connect_timeout, false, [this, cmid = conn->cmid](int timer_id)
        {
            auto conn = rdmacm_connecting.at(cmid);
            conn->timeout_id = -1;
            if (conn->peer_osd)
                fprintf(stderr, "RDMA-CM connection to %s timed out\n", conn->addr.c_str());
            else
                fprintf(stderr, "Incoming RDMA-CM connection from %s timed out\n", addr_to_string(conn->parsed_addr).c_str());
            rdmacm_on_connect_peer_error(cmid, -EPIPE);
        });
    }
}

void osd_messenger_t::rdmacm_on_connect_peer_error(rdma_cm_id *cmid, int res)
{
    auto conn = rdmacm_connecting.at(cmid);
    auto addr = conn->addr;
    auto tcp_port = conn->tcp_port;
    auto peer_osd = conn->peer_osd;
    if (conn->timeout_id >= 0)
        tfd->clear_timer(conn->timeout_id);
    if (conn->peer_fd >= 0)
        close(conn->peer_fd);
    if (conn->rdma_context)
        conn->rdma_context->reserve_cqe(-rdma_max_send-rdma_max_recv);
    if (conn->cmid)
    {
        if (conn->cmid->qp)
            rdma_destroy_qp(conn->cmid);
        rdma_destroy_id(conn->cmid);
    }
    rdmacm_connecting.erase(cmid);
    delete conn;
    if (peer_osd)
    {
        if (!disable_tcp)
        {
            // Fall back to TCP instead of just reporting the error to on_connect_peer()
            try_connect_peer_tcp(peer_osd, addr.c_str(), tcp_port);
        }
        else
        {
            // TCP is disabled
            on_connect_peer(peer_osd, res == 0 ? -EINVAL : (res > 0 ? -res : res));
        }
    }
}

void osd_messenger_t::rdmacm_try_connect_peer(uint64_t peer_osd, const std::string & addr, int rdmacm_port, int fallback_tcp_port)
{
    struct sockaddr_storage sa = {};
    if (!string_to_addr(addr, false, rdmacm_port, &sa))
    {
        fprintf(stderr, "Address %s is invalid\n", addr.c_str());
        on_connect_peer(peer_osd, -EINVAL);
        return;
    }
    rdma_cm_id *cmid = NULL;
    if (rdma_create_id(rdmacm_evch, &cmid, NULL, RDMA_PS_TCP) != 0)
    {
        int res = -errno;
        fprintf(stderr, "Failed to create RDMA-CM ID: %s (code %d), using TCP\n", strerror(errno), errno);
        if (!disable_tcp)
            try_connect_peer_tcp(peer_osd, addr.c_str(), fallback_tcp_port);
        else
            on_connect_peer(peer_osd, res);
        return;
    }
    // Make a fake FD (FIXME: do not use FDs for identifying clients!)
    int fake_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fake_fd < 0)
    {
        int res = -errno;
        rdma_destroy_id(cmid);
        // Can't create socket, pointless to try TCP
        on_connect_peer(peer_osd, res);
        return;
    }
    if (log_level > 0)
        fprintf(stderr, "Trying to connect to OSD %ju at %s:%d via RDMA-CM\n", peer_osd, addr.c_str(), rdmacm_port);
    auto conn = new rdmacm_connecting_t;
    rdmacm_connecting[cmid] = conn;
    conn->cmid = cmid;
    conn->peer_fd = fake_fd;
    conn->peer_osd = peer_osd;
    conn->addr = addr;
    conn->parsed_addr = sa;
    conn->rdmacm_port = rdmacm_port;
    conn->tcp_port = fallback_tcp_port;
    rdmacm_set_conn_timeout(conn);
    if (rdma_resolve_addr(cmid, NULL, (sockaddr*)&conn->parsed_addr, conn->timeout_ms) != 0)
    {
        auto res = -errno;
        // ENODEV means that the client doesn't have an RDMA device for this address
        if (res != -ENODEV || log_level > 0)
            fprintf(stderr, "Failed to resolve address %s via RDMA-CM: %s (code %d)\n", addr.c_str(), strerror(errno), errno);
        rdmacm_on_connect_peer_error(cmid, res);
        return;
    }
}

void osd_messenger_t::rdmacm_address_resolved(rdma_cm_event *ev)
{
    auto cmid = ev->id;
    auto conn_it = rdmacm_connecting.find(cmid);
    if (conn_it == rdmacm_connecting.end())
    {
        // Silently ignore unknown IDs
        return;
    }
    auto conn = conn_it->second;
    if (ev->event != RDMA_CM_EVENT_ADDR_RESOLVED || ev->status != 0)
    {
        fprintf(stderr, "Failed to resolve address %s via RDMA-CM: %s (code %d)\n", conn->addr.c_str(),
            ev->status > 0 ? "unknown error" : strerror(-ev->status), ev->status);
        rdmacm_on_connect_peer_error(cmid, ev->status);
        return;
    }
    auto rdma_context = rdmacm_create_qp(cmid);
    if (!rdma_context)
    {
        rdmacm_on_connect_peer_error(cmid, -EIO);
        return;
    }
    conn->rdma_context = rdma_context;
    if (rdma_resolve_route(cmid, conn->timeout_ms) != 0)
    {
        int res = -errno;
        fprintf(stderr, "Failed to resolve route to %s via RDMA-CM: %s (code %d)\n", conn->addr.c_str(), strerror(errno), errno);
        rdmacm_on_connect_peer_error(cmid, res);
        return;
    }
}

void osd_messenger_t::rdmacm_route_resolved(rdma_cm_event *ev)
{
    auto cmid = ev->id;
    auto conn_it = rdmacm_connecting.find(cmid);
    if (conn_it == rdmacm_connecting.end())
    {
        // Silently ignore unknown IDs
        return;
    }
    auto conn = conn_it->second;
    if (ev->event != RDMA_CM_EVENT_ROUTE_RESOLVED || ev->status != 0)
    {
        fprintf(stderr, "Failed to resolve route to %s via RDMA-CM: %s (code %d)\n", conn->addr.c_str(),
            ev->status > 0 ? "unknown error" : strerror(-ev->status), ev->status);
        rdmacm_on_connect_peer_error(cmid, ev->status);
        return;
    }
    // We don't need private_data, RDMA_READ or ATOMIC so use default 1
    rdma_conn_param conn_params = {
        .responder_resources = 1,
        .initiator_depth = 1,
        .retry_count = 7,
        .rnr_retry_count = 7,
    };
    if (rdma_connect(cmid, &conn_params) != 0)
    {
        int res = -errno;
        fprintf(stderr, "Failed to connect to %s:%d via RDMA-CM: %s (code %d)\n", conn->addr.c_str(), conn->rdmacm_port, strerror(errno), errno);
        rdmacm_on_connect_peer_error(cmid, res);
        return;
    }
}

void osd_messenger_t::rdmacm_established(rdma_cm_event *ev)
{
    auto cmid = ev->id;
    auto conn_it = rdmacm_connecting.find(cmid);
    if (conn_it == rdmacm_connecting.end())
    {
        // Silently ignore unknown IDs
        return;
    }
    auto conn = conn_it->second;
    auto peer_osd = conn->peer_osd;
    if (ev->event != RDMA_CM_EVENT_ESTABLISHED || ev->status != 0)
    {
        fprintf(stderr, "Failed to connect to %s:%d via RDMA-CM: %s (code %d)\n", conn->addr.c_str(), conn->rdmacm_port,
            ev->status > 0 ? "unknown error" : strerror(-ev->status), ev->status);
        rdmacm_on_connect_peer_error(cmid, ev->status);
        return;
    }
    // Wrap into a new msgr_rdma_connection_t
    msgr_rdma_connection_t *rc = new msgr_rdma_connection_t;
    rc->ctx = conn->rdma_context;
    rc->ctx->cm_refs++; // FIXME now unused, count also connecting_t's when used
    rc->max_send = rdma_max_send;
    rc->max_recv = rdma_max_recv;
    rc->max_sge = rdma_max_sge > rc->ctx->attrx.orig_attr.max_sge
        ? rc->ctx->attrx.orig_attr.max_sge : rdma_max_sge;
    rc->max_msg = rdma_max_msg;
    rc->cmid = conn->cmid;
    rc->qp = conn->cmid->qp;
    // And an osd_client_t
    auto cl = new osd_client_t();
    cl->is_incoming = true;
    cl->peer_addr = conn->parsed_addr;
    cl->peer_port = conn->rdmacm_port;
    cl->peer_fd = conn->peer_fd;
    cl->peer_state = PEER_RDMA;
    cl->connect_timeout_id = -1;
    cl->osd_num = peer_osd;
    cl->in_buf = malloc_or_die(receive_buffer_size);
    cl->rdma_conn = rc;
    clients[conn->peer_fd] = cl;
    if (conn->timeout_id >= 0)
    {
        tfd->clear_timer(conn->timeout_id);
    }
    delete conn;
    rdmacm_connecting.erase(cmid);
    rdmacm_connections[cmid] = cl;
    if (log_level > 0 && peer_osd)
    {
        fprintf(stderr, "Successfully connected with OSD %ju using RDMA-CM\n", peer_osd);
    }
    // Add initial receive request(s)
    try_recv_rdma(cl);
    if (peer_osd)
    {
        check_peer_config(cl);
    }
}
