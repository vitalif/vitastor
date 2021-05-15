// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include <stdio.h>
#include <stdlib.h>
#include "msgr_rdma.h"
#include "messenger.h"

std::string msgr_rdma_address_t::to_string()
{
    char msg[sizeof "0000:00000000:00000000:00000000000000000000000000000000"];
    sprintf(
        msg, "%04x:%06x:%06x:%016lx%016lx", lid, qpn, psn,
        htobe64(((uint64_t*)&gid)[0]), htobe64(((uint64_t*)&gid)[1])
    );
    return std::string(msg);
}

bool msgr_rdma_address_t::from_string(const char *str, msgr_rdma_address_t *dest)
{
    uint64_t* gid = (uint64_t*)&dest->gid;
    int n = sscanf(
        str, "%hx:%x:%x:%16lx%16lx", &dest->lid, &dest->qpn, &dest->psn, gid, gid+1
    );
    gid[0] = be64toh(gid[0]);
    gid[1] = be64toh(gid[1]);
    return n == 5;
}

msgr_rdma_context_t::~msgr_rdma_context_t()
{
    if (cq)
        ibv_destroy_cq(cq);
    if (channel)
        ibv_destroy_comp_channel(channel);
    if (mr)
        ibv_dereg_mr(mr);
    if (pd)
        ibv_dealloc_pd(pd);
    if (context)
        ibv_close_device(context);
}

msgr_rdma_connection_t::~msgr_rdma_connection_t()
{
    ctx->used_max_cqe -= max_send+max_recv;
    if (qp)
        ibv_destroy_qp(qp);
}

msgr_rdma_context_t *msgr_rdma_context_t::create(const char *ib_devname, uint8_t ib_port, uint8_t gid_index, uint32_t mtu)
{
    int res;
    ibv_device **dev_list = NULL;
    msgr_rdma_context_t *ctx = new msgr_rdma_context_t();
    ctx->mtu = mtu;

    dev_list = ibv_get_device_list(NULL);
    if (!dev_list)
    {
        fprintf(stderr, "Failed to get RDMA device list: %s\n", strerror(errno));
        goto cleanup;
    }
    if (!ib_devname)
    {
        ctx->dev = *dev_list;
        if (!ctx->dev)
        {
            fprintf(stderr, "No RDMA devices found\n");
            goto cleanup;
        }
    }
    else
    {
        int i;
        for (i = 0; dev_list[i]; ++i)
            if (!strcmp(ibv_get_device_name(dev_list[i]), ib_devname))
                break;
        ctx->dev = dev_list[i];
        if (!ctx->dev)
        {
            fprintf(stderr, "RDMA device %s not found\n", ib_devname);
            goto cleanup;
        }
    }

    ctx->context = ibv_open_device(ctx->dev);
    if (!ctx->context)
    {
        fprintf(stderr, "Couldn't get RDMA context for %s\n", ibv_get_device_name(ctx->dev));
        goto cleanup;
    }

    ctx->ib_port = ib_port;
    ctx->gid_index = gid_index;
    if ((res = ibv_query_port(ctx->context, ib_port, &ctx->portinfo)) != 0)
    {
        fprintf(stderr, "Couldn't get RDMA device %s port %d info: %s\n", ibv_get_device_name(ctx->dev), ib_port, strerror(res));
        goto cleanup;
    }
    ctx->my_lid = ctx->portinfo.lid;
    if (ctx->portinfo.link_layer != IBV_LINK_LAYER_ETHERNET && !ctx->my_lid)
    {
        fprintf(stderr, "RDMA device %s must have local LID because it's not Ethernet, but LID is zero\n", ibv_get_device_name(ctx->dev));
        goto cleanup;
    }
    if (ibv_query_gid(ctx->context, ib_port, gid_index, &ctx->my_gid))
    {
        fprintf(stderr, "Couldn't read RDMA device %s GID index %d\n", ibv_get_device_name(ctx->dev), gid_index);
        goto cleanup;
    }

    ctx->pd = ibv_alloc_pd(ctx->context);
    if (!ctx->pd)
    {
        fprintf(stderr, "Couldn't allocate RDMA protection domain\n");
        goto cleanup;
    }

    {
        if (ibv_query_device_ex(ctx->context, NULL, &ctx->attrx))
        {
            fprintf(stderr, "Couldn't query RDMA device for its features\n");
            goto cleanup;
        }
        if (!(ctx->attrx.odp_caps.general_caps & IBV_ODP_SUPPORT) ||
            !(ctx->attrx.odp_caps.general_caps & IBV_ODP_SUPPORT_IMPLICIT) ||
            !(ctx->attrx.odp_caps.per_transport_caps.rc_odp_caps & IBV_ODP_SUPPORT_SEND) ||
            !(ctx->attrx.odp_caps.per_transport_caps.rc_odp_caps & IBV_ODP_SUPPORT_RECV))
        {
            fprintf(stderr, "The RDMA device isn't implicit ODP (On-Demand Paging) capable or does not support RC send and receive with ODP\n");
            goto cleanup;
        }
    }

    ctx->mr = ibv_reg_mr(ctx->pd, NULL, SIZE_MAX, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_ON_DEMAND);
    if (!ctx->mr)
    {
        fprintf(stderr, "Couldn't register RDMA memory region\n");
        goto cleanup;
    }

    ctx->channel = ibv_create_comp_channel(ctx->context);
    if (!ctx->channel)
    {
        fprintf(stderr, "Couldn't create RDMA completion channel\n");
        goto cleanup;
    }

    ctx->max_cqe = 4096;
    ctx->cq = ibv_create_cq(ctx->context, ctx->max_cqe, NULL, ctx->channel, 0);
    if (!ctx->cq)
    {
        fprintf(stderr, "Couldn't create RDMA completion queue\n");
        goto cleanup;
    }

    if (dev_list)
        ibv_free_device_list(dev_list);
    return ctx;

cleanup:
    delete ctx;
    if (dev_list)
        ibv_free_device_list(dev_list);
    return NULL;
}

msgr_rdma_connection_t *msgr_rdma_connection_t::create(msgr_rdma_context_t *ctx, uint32_t max_send,
    uint32_t max_recv, uint32_t max_sge, uint32_t max_msg)
{
    msgr_rdma_connection_t *conn = new msgr_rdma_connection_t;

    max_sge = max_sge > ctx->attrx.orig_attr.max_sge ? ctx->attrx.orig_attr.max_sge : max_sge;

    conn->ctx = ctx;
    conn->max_send = max_send;
    conn->max_recv = max_recv;
    conn->max_sge = max_sge;
    conn->max_msg = max_msg;

    ctx->used_max_cqe += max_send+max_recv;
    if (ctx->used_max_cqe > ctx->max_cqe)
    {
        // Resize CQ
        // Mellanox ConnectX-4 supports up to 4194303 CQEs, so it's fine to put everything into a single CQ
        int new_max_cqe = ctx->max_cqe;
        while (ctx->used_max_cqe > new_max_cqe)
        {
            new_max_cqe *= 2;
        }
        if (ibv_resize_cq(ctx->cq, new_max_cqe) != 0)
        {
            fprintf(stderr, "Couldn't resize RDMA completion queue to %d entries\n", new_max_cqe);
            delete conn;
            return NULL;
        }
        ctx->max_cqe = new_max_cqe;
    }

    ibv_qp_init_attr init_attr = {
        .send_cq = ctx->cq,
        .recv_cq = ctx->cq,
        .cap     = {
            .max_send_wr  = max_send,
            .max_recv_wr  = max_recv,
            .max_send_sge = max_sge,
            .max_recv_sge = max_sge,
        },
        .qp_type = IBV_QPT_RC,
    };
    conn->qp = ibv_create_qp(ctx->pd, &init_attr);
    if (!conn->qp)
    {
        fprintf(stderr, "Couldn't create RDMA queue pair\n");
        delete conn;
        return NULL;
    }

    conn->addr.lid = ctx->my_lid;
    conn->addr.gid = ctx->my_gid;
    conn->addr.qpn = conn->qp->qp_num;
    conn->addr.psn = lrand48() & 0xffffff;

    ibv_qp_attr attr = {
        .qp_state        = IBV_QPS_INIT,
        .qp_access_flags = 0,
        .pkey_index      = 0,
        .port_num        = ctx->ib_port,
    };

    if (ibv_modify_qp(conn->qp, &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS))
    {
        fprintf(stderr, "Failed to switch RDMA queue pair to INIT state\n");
        delete conn;
        return NULL;
    }

    return conn;
}

static ibv_mtu mtu_to_ibv_mtu(uint32_t mtu)
{
    switch (mtu)
    {
        case 256:  return IBV_MTU_256;
        case 512:  return IBV_MTU_512;
        case 1024: return IBV_MTU_1024;
        case 2048: return IBV_MTU_2048;
        case 4096: return IBV_MTU_4096;
    }
    return IBV_MTU_4096;
}

int msgr_rdma_connection_t::connect(msgr_rdma_address_t *dest)
{
    auto conn = this;
    ibv_qp_attr attr = {
        .qp_state       = IBV_QPS_RTR,
        .path_mtu       = mtu_to_ibv_mtu(conn->ctx->mtu),
        .rq_psn         = dest->psn,
        .sq_psn         = conn->addr.psn,
        .dest_qp_num    = dest->qpn,
        .ah_attr        = {
            .grh        = {
                .dgid = dest->gid,
                .sgid_index = conn->ctx->gid_index,
                .hop_limit = 1, // FIXME can it vary?
            },
            .dlid       = dest->lid,
            .sl         = 0, // service level
            .src_path_bits = 0,
            .is_global  = (uint8_t)(dest->gid.global.interface_id ? 1 : 0),
            .port_num   = conn->ctx->ib_port,
        },
        .max_rd_atomic  = 1,
        .max_dest_rd_atomic = 1,
        // Timeout and min_rnr_timer actual values seem to be 4.096us*2^(timeout+1)
        .min_rnr_timer  = 1,
        .timeout        = 14,
        .retry_cnt      = 7,
        .rnr_retry      = 7,
    };
    // FIXME No idea if ibv_modify_qp is a blocking operation or not. No idea if it has a timeout and what it is.
    if (ibv_modify_qp(conn->qp, &attr, IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
        IBV_QP_DEST_QPN | IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER))
    {
        fprintf(stderr, "Failed to switch RDMA queue pair to RTR (ready-to-receive) state\n");
        return 1;
    }
    attr.qp_state = IBV_QPS_RTS;
    if (ibv_modify_qp(conn->qp, &attr, IBV_QP_STATE | IBV_QP_TIMEOUT |
        IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC))
    {
        fprintf(stderr, "Failed to switch RDMA queue pair to RTS (ready-to-send) state\n");
        return 1;
    }
    return 0;
}

bool osd_messenger_t::connect_rdma(int peer_fd, std::string rdma_address, uint64_t client_max_msg)
{
    // Try to connect to the peer using RDMA
    msgr_rdma_address_t addr;
    if (msgr_rdma_address_t::from_string(rdma_address.c_str(), &addr))
    {
        if (client_max_msg > rdma_max_msg)
        {
            client_max_msg = rdma_max_msg;
        }
        auto rdma_conn = msgr_rdma_connection_t::create(rdma_context, rdma_max_send, rdma_max_recv, rdma_max_sge, client_max_msg);
        if (rdma_conn)
        {
            int r = rdma_conn->connect(&addr);
            if (r != 0)
            {
                delete rdma_conn;
                fprintf(
                    stderr, "Failed to connect RDMA queue pair to %s (client %d)\n",
                    addr.to_string().c_str(), peer_fd
                );
            }
            else
            {
                // Remember connection, but switch to RDMA only after sending the configuration response
                auto cl = clients.at(peer_fd);
                cl->rdma_conn = rdma_conn;
                cl->peer_state = PEER_RDMA_CONNECTING;
                return true;
            }
        }
    }
    return false;
}

static void try_send_rdma_wr(osd_client_t *cl, ibv_sge *sge, int op_sge)
{
    ibv_send_wr *bad_wr = NULL;
    ibv_send_wr wr = {
        .wr_id = (uint64_t)(cl->peer_fd*2+1),
        .sg_list = sge,
        .num_sge = op_sge,
        .opcode = IBV_WR_SEND,
        .send_flags = IBV_SEND_SIGNALED,
    };
    int err = ibv_post_send(cl->rdma_conn->qp, &wr, &bad_wr);
    if (err || bad_wr)
    {
        fprintf(stderr, "RDMA send failed: %s\n", strerror(err));
        exit(1);
    }
    cl->rdma_conn->cur_send++;
}

bool osd_messenger_t::try_send_rdma(osd_client_t *cl)
{
    auto rc = cl->rdma_conn;
    if (!cl->send_list.size() || rc->cur_send > 0)
    {
        // Only send one batch at a time
        return true;
    }
    uint64_t op_size = 0, op_sge = 0;
    ibv_sge sge[rc->max_sge];
    while (rc->send_pos < cl->send_list.size())
    {
        iovec & iov = cl->send_list[rc->send_pos];
        if (op_size >= rc->max_msg || op_sge >= rc->max_sge)
        {
            try_send_rdma_wr(cl, sge, op_sge);
            op_sge = 0;
            op_size = 0;
            if (rc->cur_send >= rc->max_send)
            {
                break;
            }
        }
        uint32_t len = (uint32_t)(op_size+iov.iov_len-rc->send_buf_pos < rc->max_msg
            ? iov.iov_len-rc->send_buf_pos : rc->max_msg-op_size);
        sge[op_sge++] = {
            .addr = (uintptr_t)(iov.iov_base+rc->send_buf_pos),
            .length = len,
            .lkey = rc->ctx->mr->lkey,
        };
        op_size += len;
        rc->send_buf_pos += len;
        if (rc->send_buf_pos >= iov.iov_len)
        {
            rc->send_pos++;
            rc->send_buf_pos = 0;
        }
    }
    if (op_sge > 0)
    {
        try_send_rdma_wr(cl, sge, op_sge);
    }
    return true;
}

static void try_recv_rdma_wr(osd_client_t *cl, ibv_sge *sge, int op_sge)
{
    ibv_recv_wr *bad_wr = NULL;
    ibv_recv_wr wr = {
        .wr_id = (uint64_t)(cl->peer_fd*2),
        .sg_list = sge,
        .num_sge = op_sge,
    };
    int err = ibv_post_recv(cl->rdma_conn->qp, &wr, &bad_wr);
    if (err || bad_wr)
    {
        fprintf(stderr, "RDMA receive failed: %s\n", strerror(err));
        exit(1);
    }
    cl->rdma_conn->cur_recv++;
}

bool osd_messenger_t::try_recv_rdma(osd_client_t *cl)
{
    auto rc = cl->rdma_conn;
    while (rc->cur_recv < rc->max_recv)
    {
        void *buf = malloc_or_die(rc->max_msg);
        rc->recv_buffers.push_back(buf);
        ibv_sge sge = {
            .addr = (uintptr_t)buf,
            .length = (uint32_t)rc->max_msg,
            .lkey = rc->ctx->mr->lkey,
        };
        try_recv_rdma_wr(cl, &sge, 1);
    }
    return true;
}

#define RDMA_EVENTS_AT_ONCE 32

void osd_messenger_t::handle_rdma_events()
{
    // Request next notification
    ibv_cq *ev_cq;
    void *ev_ctx;
    // FIXME: This is inefficient as it calls read()...
    if (ibv_get_cq_event(rdma_context->channel, &ev_cq, &ev_ctx) == 0)
    {
        ibv_ack_cq_events(rdma_context->cq, 1);
    }
    if (ibv_req_notify_cq(rdma_context->cq, 0) != 0)
    {
        fprintf(stderr, "Failed to request RDMA completion notification, exiting\n");
        exit(1);
    }
    ibv_wc wc[RDMA_EVENTS_AT_ONCE];
    int event_count;
    do
    {
        event_count = ibv_poll_cq(rdma_context->cq, RDMA_EVENTS_AT_ONCE, wc);
        for (int i = 0; i < event_count; i++)
        {
            int client_id = wc[i].wr_id >> 1;
            bool is_send = wc[i].wr_id & 1;
            auto cl_it = clients.find(client_id);
            if (cl_it == clients.end())
            {
                continue;
            }
            osd_client_t *cl = cl_it->second;
            if (wc[i].status != IBV_WC_SUCCESS)
            {
                fprintf(stderr, "RDMA work request failed for client %d", client_id);
                if (cl->osd_num)
                {
                    fprintf(stderr, " (OSD %lu)", cl->osd_num);
                }
                fprintf(stderr, " with status: %s, stopping client\n", ibv_wc_status_str(wc[i].status));
                stop_client(client_id);
                continue;
            }
            if (!is_send)
            {
                cl->rdma_conn->cur_recv--;
                handle_read_buffer(cl, cl->rdma_conn->recv_buffers[0], wc[i].byte_len);
                free(cl->rdma_conn->recv_buffers[0]);
                cl->rdma_conn->recv_buffers.erase(cl->rdma_conn->recv_buffers.begin(), cl->rdma_conn->recv_buffers.begin()+1);
                try_recv_rdma(cl);
            }
            else
            {
                cl->rdma_conn->cur_send--;
                if (!cl->rdma_conn->cur_send)
                {
                    // Wait for the whole batch
                    for (int i = 0; i < cl->rdma_conn->send_pos; i++)
                    {
                        if (cl->outbox[i].flags & MSGR_SENDP_FREE)
                        {
                            // Reply fully sent
                            delete cl->outbox[i].op;
                        }
                    }
                    if (cl->rdma_conn->send_pos > 0)
                    {
                        cl->send_list.erase(cl->send_list.begin(), cl->send_list.begin()+cl->rdma_conn->send_pos);
                        cl->outbox.erase(cl->outbox.begin(), cl->outbox.begin()+cl->rdma_conn->send_pos);
                        cl->rdma_conn->send_pos = 0;
                    }
                    if (cl->rdma_conn->send_buf_pos > 0)
                    {
                        cl->send_list[0].iov_base += cl->rdma_conn->send_buf_pos;
                        cl->send_list[0].iov_len -= cl->rdma_conn->send_buf_pos;
                        cl->rdma_conn->send_buf_pos = 0;
                    }
                    try_send_rdma(cl);
                }
            }
        }
    } while (event_count > 0);
    for (auto cb: set_immediate)
    {
        cb();
    }
    set_immediate.clear();
}
