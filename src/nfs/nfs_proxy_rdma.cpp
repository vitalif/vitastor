// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// NFS RDMA support

#ifdef WITH_RDMACM

#define _XOPEN_SOURCE

#include <rdma/rdma_cma.h>
#include <fcntl.h>

#include "addr_util.h"

#include "proto/nfs.h"
#include "proto/rpc.h"
#include "proto/rpc_rdma.h"

#include "nfs_proxy.h"

#include "rdma_alloc.h"

#define NFS_RDMACM_PRIVATE_DATA_MAGIC_LE 0x180eabf6

struct __attribute__((__packed__)) nfs_rdmacm_private
{
    uint32_t format_identifier; // magic, should be 0xf6ab0e18 in big endian
    uint8_t version;            // version, 1
    uint8_t remote_invalidate;  // remote invalidation flag (1 or 0)
    uint8_t max_send_size;      // maximum RDMA Send operation size / 1024 - 1 (i.e. 0 is 1 KB, 255 is 256 KB)
    uint8_t max_recv_size;      // maximum RDMA Receive operation size / 1024 - 1 (i.e. 0 is 1 KB, 255 is 256 KB)
};

struct nfs_rdma_buf_t
{
    void *buf = NULL;
    size_t len = 0;
    ibv_mr *mr = NULL;
};

struct nfs_rdma_conn_t;

struct nfs_rdma_dev_state_t
{
    rdma_cm_id *cmid = NULL;
    ibv_device_attr dev_attr;
    ibv_comp_channel *channel = NULL;
    ibv_cq *cq = NULL;
    rdma_allocator_t *alloc = NULL;
    int max_cqe = 0, used_max_cqe = 0;

    static nfs_rdma_dev_state_t *create(rdma_cm_id *cmid, uint64_t rdma_malloc_round_to, uint64_t rdma_max_unused_buffers);
    ~nfs_rdma_dev_state_t();
};

nfs_rdma_dev_state_t *nfs_rdma_dev_state_t::create(rdma_cm_id *cmid, uint64_t rdma_malloc_round_to, uint64_t rdma_max_unused_buffers)
{
    nfs_rdma_dev_state_t *self = new nfs_rdma_dev_state_t;
    self->cmid = cmid;
    int r = ibv_query_device(cmid->verbs, &self->dev_attr);
    if (r != 0)
    {
        fprintf(stderr, "Failed to query listerning RDMA device: %s (code %d)\n", strerror(r), r);
        delete self;
        return NULL;
    }
    self->channel = ibv_create_comp_channel(cmid->verbs);
    if (!self->channel)
    {
        fprintf(stderr, "Couldn't create RDMA completion channel\n");
        delete self;
        return NULL;
    }
    self->max_cqe = 4096;
    self->cq = ibv_create_cq(cmid->verbs, self->max_cqe, NULL, self->channel, 0);
    if (!self->cq)
    {
        fprintf(stderr, "Couldn't create RDMA completion queue\n");
        delete self;
        return NULL;
    }
    self->alloc = rdma_malloc_create(cmid->pd, rdma_malloc_round_to, rdma_max_unused_buffers, IBV_ACCESS_LOCAL_WRITE);
    fcntl(self->channel->fd, F_SETFL, fcntl(self->channel->fd, F_GETFL, 0) | O_NONBLOCK);
    return self;
}

nfs_rdma_dev_state_t::~nfs_rdma_dev_state_t()
{
    if (cq)
    {
        ibv_destroy_cq(cq);
        cq = NULL;
    }
    if (channel)
    {
        ibv_destroy_comp_channel(channel);
        channel = NULL;
    }
    if (alloc)
    {
        rdma_malloc_destroy(alloc);
        alloc = NULL;
    }
}

struct nfs_rdma_context_t
{
    std::string bind_address;
    int rdmacm_port = 0;
    uint32_t max_iodepth = 16, max_send_wr = 1024;
    uint64_t rdma_malloc_round_to = 1048576, rdma_max_unused_buffers = 64*1048576;
    uint64_t max_send_size = 256*1024, max_recv_size = 256*1024;

    nfs_proxy_t *proxy = NULL;
    epoll_manager_t *epmgr = NULL;

    rdma_event_channel *rdmacm_evch = NULL;
    rdma_cm_id *listener_id = NULL;
    std::map<rdma_cm_id*, nfs_rdma_conn_t*> rdma_connections;

    ~nfs_rdma_context_t();
    void handle_rdmacm_events();
    void rdmacm_accept(rdma_cm_event *ev);
    void rdmacm_established(rdma_cm_event *ev);
};

struct nfs_rdma_conn_t
{
    nfs_rdma_context_t *ctx = NULL;
    nfs_client_t *client = NULL;
    nfs_rdma_dev_state_t *conn_dev = NULL;
    rdma_cm_id *id = NULL;
    int max_send_size = 256*1024, max_recv_size = 256*1024;
    int max_buf_size = 256*1024;
    int remote_max_send_size = 1024, remote_max_recv_size = 1024;
    int max_rdma_reads = 16;
    bool remote_invalidate = true;
    bool established = false;
    uint32_t cur_credit = 16;
    uint32_t cur_send = 0;
    uint32_t cur_rdma_reads = 0;
    std::vector<nfs_rdma_buf_t> recv_buffers;
    std::map<void*, nfs_rdma_buf_t> used_buffers;
    int next_recv_buf = 0;
    std::vector<rpc_op_t*> outbox;
    std::vector<int> outbox_wrs;
    std::vector<rpc_op_t*> chunk_inbox, chunk_read_postponed;
    int outbox_pos = 0;

    void handle_io();
    void post_initial_receives();
    ~nfs_rdma_conn_t();
    nfs_rdma_buf_t create_buf(size_t len);
    void post_recv(nfs_rdma_buf_t b);
    void post_send();
    int post_chunk_reads(rpc_op_t *rop, bool push);
    bool handle_recv(void *buf, size_t len);
    void free_rdma_rpc_op(rpc_op_t *rop);
    void reuse_buffer(void *buf);
    void rdma_encode_header(XDR *xdrs, rpc_op_t *rop, bool nomsg);
};

nfs_rdma_context_t* nfs_proxy_t::create_rdma(const std::string & bind_address, int rdmacm_port,
    uint32_t max_iodepth, uint32_t max_send_wr, uint64_t rdma_malloc_round_to, uint64_t rdma_max_unused_buffers)
{
    nfs_rdma_context_t* self = new nfs_rdma_context_t;
    self->proxy = this;
    self->epmgr = epmgr;
    self->bind_address = bind_address;
    self->rdmacm_port = rdmacm_port;
    self->max_iodepth = max_iodepth;
    self->max_send_wr = max_send_wr ? max_send_wr : 1024;
    self->rdma_malloc_round_to = rdma_malloc_round_to;
    self->rdma_max_unused_buffers = rdma_max_unused_buffers;
    self->rdmacm_evch = rdma_create_event_channel();
    if (!self->rdmacm_evch)
    {
        fprintf(stderr, "Failed to initialize RDMA-CM event channel: %s (code %d)\n", strerror(errno), errno);
        delete self;
        return NULL;
    }
    fcntl(self->rdmacm_evch->fd, F_SETFL, fcntl(self->rdmacm_evch->fd, F_GETFL, 0) | O_NONBLOCK);
    epmgr->tfd->set_fd_handler(self->rdmacm_evch->fd, false, [self](int rdmacm_eventfd, int epoll_events)
    {
        self->handle_rdmacm_events();
    });
    int r = rdma_create_id(self->rdmacm_evch, &self->listener_id, NULL, RDMA_PS_TCP);
    if (r != 0)
    {
        fprintf(stderr, "Failed to create RDMA-CM ID: %s (code %d)\n", strerror(errno), errno);
        delete self;
        return NULL;
    }
    sockaddr_storage addr;
    if (!string_to_addr(bind_address, 0, rdmacm_port, &addr))
    {
        fprintf(stderr, "Server address: %s is not valid\n", bind_address.c_str());
        delete self;
        return NULL;
    }
    r = rdma_bind_addr(self->listener_id, (sockaddr*)&addr);
    if (r != 0)
    {
        fprintf(stderr, "Failed to bind RDMA-CM to %s:%d: %s (code %d)\n", bind_address.c_str(), rdmacm_port, strerror(errno), errno);
        delete self;
        return NULL;
    }
    r = rdma_listen(self->listener_id, 128);
    if (r != 0)
    {
        fprintf(stderr, "Failed to listen RDMA-CM: %s (code %d)\n", strerror(errno), errno);
        delete self;
        return NULL;
    }
    return self;
}

void nfs_proxy_t::destroy_rdma()
{
    if (rdma_context)
    {
        delete rdma_context;
        rdma_context = NULL;
    }
}

nfs_rdma_context_t::~nfs_rdma_context_t()
{
    if (listener_id)
    {
        int r = rdma_destroy_id(listener_id);
        if (r != 0)
            fprintf(stderr, "Failed to destroy RDMA-CM ID: %s (code %d)\n", strerror(errno), errno);
        else
            listener_id = NULL;
    }
    if (rdmacm_evch)
    {
        epmgr->tfd->set_fd_handler(rdmacm_evch->fd, false, NULL);
        rdma_destroy_event_channel(rdmacm_evch);
        rdmacm_evch = NULL;
    }
}

void nfs_rdma_context_t::handle_rdmacm_events()
{
    rdma_cm_event *ev = NULL;
    std::vector<nfs_client_t*> stop_clients;
    while (1)
    {
        int r = rdma_get_cm_event(rdmacm_evch, &ev);
        if (r != 0)
        {
            if (errno == EAGAIN || errno == EINTR)
                break;
            fprintf(stderr, "Failed to get RDMA-CM event: %s (code %d)\n", strerror(errno), errno);
            exit(1);
        }
        if (ev->event == RDMA_CM_EVENT_CONNECT_REQUEST)
        {
            rdmacm_accept(ev);
        }
        else if (ev->event == RDMA_CM_EVENT_CONNECT_ERROR ||
            ev->event == RDMA_CM_EVENT_REJECTED ||
            ev->event == RDMA_CM_EVENT_DISCONNECTED ||
            ev->event == RDMA_CM_EVENT_DEVICE_REMOVAL)
        {
            auto event_type_name = ev->event == RDMA_CM_EVENT_CONNECT_ERROR ? "RDMA_CM_EVENT_CONNECT_ERROR" : (
                ev->event == RDMA_CM_EVENT_REJECTED ? "RDMA_CM_EVENT_REJECTED" : (
                ev->event == RDMA_CM_EVENT_DISCONNECTED ? "RDMA_CM_EVENT_DISCONNECTED" : "RDMA_CM_EVENT_DEVICE_REMOVAL"));
            auto conn_it = rdma_connections.find(ev->id);
            if (conn_it == rdma_connections.end())
            {
                fprintf(stderr, "Received %s event for an unknown connection 0x%jx - ignoring\n",
                    event_type_name, (uint64_t)ev->id);
            }
            else
            {
                fprintf(stderr, "Received %s event for connection 0x%jx - closing it\n",
                    event_type_name, (uint64_t)ev->id);
                auto conn = conn_it->second;
                stop_clients.push_back(conn->client);
            }
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
        r = rdma_ack_cm_event(ev);
        if (r != 0)
        {
            fprintf(stderr, "Failed to ack (free) RDMA-CM event: %s (code %d)\n", strerror(errno), errno);
            exit(1);
        }
    }
    // Stop only after flushing all events, otherwise rdma_destroy_id infinitely waits for pthread_cond
    for (auto cli: stop_clients)
    {
        cli->stop();
    }
}

void nfs_rdma_context_t::rdmacm_accept(rdma_cm_event *ev)
{
    nfs_rdma_dev_state_t *conn_dev = nfs_rdma_dev_state_t::create(ev->id, rdma_malloc_round_to, rdma_max_unused_buffers);
    if (!conn_dev)
    {
        rdma_destroy_id(ev->id);
        return;
    }
    ibv_qp_init_attr init_attr = {
        .send_cq = conn_dev->cq,
        .recv_cq = conn_dev->cq,
        .cap     = {
            // each op at each moment takes 1 RDMA_RECV or 1 RDMA_READ or 1 RDMA_WRITE + 1 RDMA_SEND
            .max_send_wr  = max_send_wr,
            .max_recv_wr  = max_iodepth,
            .max_send_sge = 1,
            .max_recv_sge = 1, // we don't need S/G currently
        },
        .qp_type = IBV_QPT_RC,
    };
    int r = rdma_create_qp(ev->id, NULL, &init_attr);
    if (r != 0)
    {
        fprintf(stderr, "Failed to create a queue pair via RDMA-CM: %s (code %d)\n", strerror(errno), errno);
        delete conn_dev;
        rdma_destroy_id(ev->id);
        return;
    }
    assert(ev->id->qp->send_cq == conn_dev->cq);
    assert(ev->id->qp->recv_cq == conn_dev->cq);
    nfs_rdmacm_private private_data = {
        .format_identifier = NFS_RDMACM_PRIVATE_DATA_MAGIC_LE,
        .version = 1,
        .remote_invalidate = 1,
        .max_send_size = (uint8_t)(max_send_size <= 256*1024 ? max_send_size/1024 - 1 : 255),
        .max_recv_size = (uint8_t)(max_recv_size <= 256*1024 ? max_recv_size/1024 - 1 : 255),
    };
    // should be responder_resources, but it's 0 for Linux NFS client
    // so probably RDMA-CM swaps these values in _accept for the "convenience" :)
    int max_rdma_reads = ev->param.conn.initiator_depth;
    // just 16 on ConnectX-4...
    if (!max_rdma_reads || max_rdma_reads > conn_dev->dev_attr.max_qp_rd_atom)
        max_rdma_reads = conn_dev->dev_attr.max_qp_rd_atom;
    if (max_rdma_reads > conn_dev->dev_attr.max_qp_init_rd_atom)
        max_rdma_reads = conn_dev->dev_attr.max_qp_init_rd_atom;
    if (max_rdma_reads > 255)
        max_rdma_reads = 255;
    rdma_conn_param conn_params = {
        .private_data = &private_data,
        .private_data_len = sizeof(private_data),
        .responder_resources = (uint8_t)max_rdma_reads, // min(max_qp_rd_atom of the local device, max_qp_init_rd_atom of the remote device)
        .initiator_depth = (uint8_t)max_rdma_reads, // max_qp_init_rd_atom of the local device
        .rnr_retry_count = 7,
    };
    r = rdma_accept(ev->id, &conn_params);
    if (r != 0)
    {
        fprintf(stderr, "Failed to accept RDMA-CM connection: %s (code %d)\n", strerror(errno), errno);
        rdma_destroy_qp(ev->id);
        delete conn_dev;
        rdma_destroy_id(ev->id);
    }
    else
    {
        auto conn = new nfs_rdma_conn_t();
        conn->ctx = this;
        conn->id = ev->id;
        conn->cur_credit = max_iodepth;
        conn->max_send_size = max_send_size;
        conn->max_recv_size = max_recv_size;
        conn->max_rdma_reads = max_rdma_reads;
        rdma_connections[ev->id] = conn;
        // Handle NFS private_data
        if (ev->param.conn.private_data_len >= sizeof(nfs_rdmacm_private))
        {
            nfs_rdmacm_private *private_data = (nfs_rdmacm_private *)ev->param.conn.private_data;
            if (private_data->format_identifier == NFS_RDMACM_PRIVATE_DATA_MAGIC_LE &&
                private_data->version == 1)
            {
                conn->remote_invalidate = private_data->remote_invalidate;
                conn->remote_max_send_size = (private_data->max_send_size+1) * 1024;
                conn->remote_max_recv_size = (private_data->max_recv_size+1) * 1024;
                if (conn->remote_max_recv_size < conn->max_send_size)
                    conn->max_send_size = conn->remote_max_recv_size;
            }
        }
        auto cli = this->proxy->create_client();
        cli->rdma_conn = conn;
        conn->client = cli;
        conn->conn_dev = conn_dev;
        epmgr->tfd->set_fd_handler(conn_dev->channel->fd, false, [conn](int channel_eventfd, int epoll_events)
        {
            conn->handle_io();
        });
        // run handle_io() once to reset poll state
        conn->handle_io();
        // Post initial receive requests
        conn->post_initial_receives();
    }
}

nfs_rdma_conn_t::~nfs_rdma_conn_t()
{
    assert(!outbox.size());
    assert(!chunk_inbox.size());
    assert(!chunk_read_postponed.size());
    for (auto & b: recv_buffers)
    {
        ibv_dereg_mr(b.mr);
        free(b.buf);
    }
    recv_buffers.clear();
    if (conn_dev)
    {
        ctx->epmgr->tfd->set_fd_handler(conn_dev->channel->fd, false, NULL);
        delete conn_dev;
        conn_dev = NULL;
    }
    if (id)
    {
        ctx->rdma_connections.erase(id);
        if (id->qp)
        {
            rdma_destroy_qp(id);
        }
        rdma_destroy_id(id);
    }
}

void nfs_rdma_context_t::rdmacm_established(rdma_cm_event *ev)
{
    auto conn_it = rdma_connections.find(ev->id);
    if (conn_it == rdma_connections.end())
    {
        fprintf(stderr, "Received RDMA_CM_EVENT_ESTABLISHED event for an unknown connection 0x%jx - ignoring\n", (uint64_t)ev->id);
        return;
    }
    fprintf(stderr, "Received RDMA_CM_EVENT_ESTABLISHED event for connection 0x%jx - connection established\n", (uint64_t)ev->id);
    auto conn = conn_it->second;
    conn->established = true;
}

void nfs_rdma_conn_t::post_initial_receives()
{
    for (int i = 0; i < cur_credit; i++)
    {
        auto b = create_buf(max_recv_size);
        recv_buffers.push_back(b);
        used_buffers[b.buf] = b;
        post_recv(b);
    }
}

nfs_rdma_buf_t nfs_rdma_conn_t::create_buf(size_t len)
{
    nfs_rdma_buf_t b;
    b.buf = malloc_or_die(len);
    b.len = len;
    b.mr = ibv_reg_mr(id->pd, b.buf, len, IBV_ACCESS_LOCAL_WRITE);
    if (!b.mr)
    {
        fprintf(stderr, "Failed to register RDMA memory region: %s\n", strerror(errno));
        exit(1);
    }
    return b;
}

void nfs_rdma_conn_t::post_recv(nfs_rdma_buf_t b)
{
    if (client->stopped)
    {
        return;
    }
    ibv_sge sge = {
        .addr = (uintptr_t)b.buf,
        .length = (uint32_t)b.len,
        .lkey = b.mr->lkey,
    };
    ibv_recv_wr *bad_wr = NULL;
    ibv_recv_wr wr = {
        .wr_id   = 1, // 1 is any read, 2 is any write :)
        .sg_list = &sge,
        .num_sge = 1,
    };
    int err = ibv_post_recv(id->qp, &wr, &bad_wr);
    if (err || bad_wr)
    {
        fprintf(stderr, "RDMA receive failed: %s\n", strerror(err));
        exit(1);
    }
}

void nfs_rdma_conn_t::rdma_encode_header(XDR *xdrs, rpc_op_t *rop, bool nomsg)
{
    rdma_msg outrmsg = {
        .rdma_xid = rop->in_rdma_msg.rdma_xid,
        .rdma_vers = rop->in_rdma_msg.rdma_vers,
        .rdma_credit = cur_credit,
        .rdma_body = {
            .proc = rop->rdma_error ? RDMA_ERROR : (nomsg ? RDMA_NOMSG : RDMA_MSG),
        },
    };
    if (rop->rdma_error)
    {
        outrmsg.rdma_body.rdma_error.err = rop->rdma_error;
        if (rop->rdma_error == ERR_VERS)
            outrmsg.rdma_body.rdma_error.range = (rpc_rdma_errvers){ 1, 1 };
    }
    else
    {
        // Copy chunks... it's a real shit
        outrmsg.rdma_body.rdma_msg = {
            .rdma_writes = rop->in_rdma_msg.rdma_body.rdma_msg.rdma_writes,
            .rdma_reply = rop->in_rdma_msg.rdma_body.rdma_msg.rdma_reply,
        };
    }
    int r = xdr_encode(xdrs, (xdrproc_t)xdr_rdma_msg, &outrmsg);
    assert(r);
}

void nfs_client_t::rdma_queue_reply(rpc_op_t *rop)
{
    rdma_conn->outbox.push_back(rop);
    rdma_conn->post_send();
}

void nfs_rdma_conn_t::post_send()
{
    while (chunk_read_postponed.size() > 0)
    {
        int posted = post_chunk_reads(chunk_read_postponed[0], false);
        if (posted)
            chunk_read_postponed.erase(chunk_read_postponed.begin(), chunk_read_postponed.begin()+1);
        else
            break;
    }
    while (outbox.size() > outbox_pos)
    {
        auto rop = outbox[outbox_pos];
        if (rop->buffer)
        {
            reuse_buffer(rop->buffer);
            rop->buffer = NULL;
        }
        XDR *hdr_xdr = ctx->proxy->get_xdr();
send_again:
        rdma_encode_header(hdr_xdr, rop, false);
        size_t hdr_size = xdr_encode_get_size(hdr_xdr);
        iovec *chunk_iov = NULL;
        iovec *iov_list = NULL;
        unsigned iov_count = 0;
        if (!rop->rdma_error)
        {
            xdr_encode_finish(rop->xdrs, &iov_list, &iov_count);
            assert(iov_count > 0);
            // READ3resok and READLINK3resok - extract last byte buffer from iovecs and send it in a "write chunk"
            if (rop->in_rdma_msg.rdma_body.rdma_msg.rdma_writes &&
                rop->in_msg.body.cbody.prog == NFS_PROGRAM &&
                (rop->in_msg.body.cbody.proc == NFS3_READ && ((READ3res*)rop->reply)->status == NFS3_OK ||
                rop->in_msg.body.cbody.proc == NFS3_READLINK && ((READLINK3res*)rop->reply)->status == NFS3_OK))
            {
                assert(iov_count > 1);
                iov_count--;
                chunk_iov = &iov_list[iov_count];
                auto expected_size = (rop->in_msg.body.cbody.proc == NFS3_READ
                    ? ((READ3res*)rop->reply)->resok.count
                    : ((READLINK3res*)rop->reply)->resok.data.size);
                assert(chunk_iov->iov_len == expected_size);
            }
        }
        size_t msg_size = 0;
        for (unsigned i = 0; i < iov_count; i++)
        {
            msg_size += iov_list[i].iov_len;
        }
        // Estimate reply WR count, create WR and SGE arrays
        xdr_write_chunk *reply_chunk = rop->in_rdma_msg.rdma_body.rdma_msg.rdma_reply;
        int reply_chunk_wr_count = (reply_chunk ? reply_chunk->target.target_len : 0);
        uint32_t wr_count = 1 + (chunk_iov ? 1 : 0) + (reply_chunk ? reply_chunk_wr_count : 0);
        if (wr_count > ctx->max_send_wr)
        {
            fprintf(stderr, "Reply fragmentation (%u) exceeds max_send_wr (%u), sending ERR_CHUNK\n", wr_count, ctx->max_send_wr);
chunk_error:
            xdr_reset(hdr_xdr);
            rop->rdma_error = ERR_CHUNK;
            goto send_again;
        }
        if (msg_size+hdr_size > max_send_size && !reply_chunk)
        {
            fprintf(stderr, "RPC message size (%zu) exceeds client's max_send_size (%u)"
                " and reply chunk is not provided, sending ERR_CHUNK\n", msg_size+hdr_size, max_send_size);
            goto chunk_error;
        }
        if (chunk_iov && chunk_iov->iov_len > rop->in_rdma_msg.rdma_body.rdma_msg.rdma_writes->entry.target.target_val[0].length)
        {
            fprintf(stderr, "%s write chunk size (%u) is smaller than read size (%zu), sending ERR_CHUNK\n",
                rop->in_msg.body.cbody.proc == NFS3_READ ? "READ3" : "READLINK3",
                rop->in_rdma_msg.rdma_body.rdma_msg.rdma_writes->entry.target.target_val[0].length,
                chunk_iov->iov_len);
            goto chunk_error;
        }
        if (cur_send + wr_count > ctx->max_send_wr)
        {
            // Retry later
            ctx->proxy->free_xdr(hdr_xdr);
            return;
        }
        // Check reply chunk size and set actual reply segment sizes
        bool reencode = false;
        if (reply_chunk)
        {
            size_t reply_chunk_len = 0;
            size_t left = msg_size;
            for (uint32_t i = 0; i < reply_chunk->target.target_len; i++)
            {
                reply_chunk_len += reply_chunk->target.target_val[i].length;
                if (reply_chunk->target.target_val[i].length > left)
                    reply_chunk->target.target_val[i].length = left;
                left -= reply_chunk->target.target_val[i].length;
            }
            if (left > 0)
            {
                // Message doesn't fit even in the reply chunk
                fprintf(stderr, "RPC message payload size (%zu) exceeds reply chunk size (%zu), sending ERR_CHUNK\n",
                    msg_size, reply_chunk_len);
                goto chunk_error;
            }
            reencode = true;
        }
        if (chunk_iov && chunk_iov->iov_len < rop->in_rdma_msg.rdma_body.rdma_msg.rdma_writes->entry.target.target_val[0].length)
        {
            // Set actual write chunk size
            reencode = true;
            rop->in_rdma_msg.rdma_body.rdma_msg.rdma_writes->entry.target.target_val[0].length = chunk_iov->iov_len;
        }
        if (reencode)
        {
            // ...and re-encode the header :-( shitty protocol
            xdr_reset(hdr_xdr);
            rdma_encode_header(hdr_xdr, rop, reply_chunk != NULL);
            hdr_size = xdr_encode_get_size(hdr_xdr);
        }
        ibv_sge sges[wr_count];
        ibv_send_wr wrs[wr_count];
        int wr_pos = 0;
        // Use a buffer from rdma_malloc for the reply
        assert(!rop->buffer);
        rop->buffer = rdma_malloc_alloc(conn_dev->alloc, hdr_size+msg_size);
        auto buf_lkey = rdma_malloc_get_lkey(conn_dev->alloc, rop->buffer);
        size_t pos = 0;
        {
            // Copy and free the RDMA-RPC header
            iovec *hdr_iov_list = NULL;
            unsigned hdr_iov_count = 0;
            xdr_encode_finish(hdr_xdr, &hdr_iov_list, &hdr_iov_count);
            assert(hdr_iov_count > 0);
            for (unsigned i = 0; i < hdr_iov_count; i++)
            {
                memcpy(rop->buffer + pos, hdr_iov_list[i].iov_base, hdr_iov_list[i].iov_len);
                pos += hdr_iov_list[i].iov_len;
            }
            assert(pos == hdr_size);
            ctx->proxy->free_xdr(hdr_xdr);
        }
        for (unsigned i = 0; i < iov_count; i++)
        {
            memcpy(rop->buffer + pos, iov_list[i].iov_base, iov_list[i].iov_len);
            pos += iov_list[i].iov_len;
        }
        // Include header size in msg_size now and then
        msg_size += hdr_size;
        assert(msg_size == pos);
        // Check if we have to use the reply chunk
        if (reply_chunk)
        {
            size_t pos = hdr_size;
            for (uint32_t i = 0; i < reply_chunk->target.target_len && pos < msg_size; i++)
            {
                uint32_t len = (reply_chunk->target.target_val[i].length < msg_size-pos
                    ? reply_chunk->target.target_val[i].length : msg_size-pos);
                sges[wr_pos] = {
                    .addr = (uintptr_t)(rop->buffer + pos),
                    .length = len,
                    .lkey = buf_lkey,
                };
                wrs[wr_pos] = {
                    .wr_id = 4, // 4 is chunk write
                    .opcode = IBV_WR_RDMA_WRITE,
                    .wr = {
                        .rdma = {
                            .remote_addr = reply_chunk->target.target_val[i].offset,
                            .rkey = reply_chunk->target.target_val[i].handle,
                        },
                    },
                };
                wr_pos++;
                pos += len;
            }
        }
        // Check if we have a DDP chunk
        xdr_rdma_segment *wr_chunk = NULL;
        if (chunk_iov != NULL)
        {
            wr_chunk = rop->in_rdma_msg.rdma_body.rdma_msg.rdma_writes->entry.target.target_val;
            sges[wr_pos] = {
                .addr = (uintptr_t)chunk_iov->iov_base,
                .length = (uint32_t)chunk_iov->iov_len,
                .lkey = rdma_malloc_get_lkey(conn_dev->alloc, chunk_iov->iov_base),
            };
            wrs[wr_pos] = {
                .wr_id = 4, // 4 is chunk write
                .opcode = IBV_WR_RDMA_WRITE,
                .wr = {
                    .rdma = {
                        .remote_addr = wr_chunk->offset,
                        .rkey = wr_chunk->handle,
                    },
                },
            };
            wr_pos++;
        }
        // Add the normal send
        sges[wr_pos] = {
            .addr = (uintptr_t)rop->buffer,
            .length = (uint32_t)(reply_chunk ? hdr_size : msg_size),
            .lkey = buf_lkey,
        };
        wrs[wr_pos] = {
            .wr_id = 2, // 2 is send
            .opcode = remote_invalidate && !reply_chunk && wr_chunk ? IBV_WR_SEND_WITH_INV : IBV_WR_SEND,
            .send_flags = IBV_SEND_SIGNALED,
            .invalidate_rkey = remote_invalidate && !reply_chunk && wr_chunk ? wr_chunk->handle : 0,
        };
        wr_pos++;
        // Send it all
        for (int i = 0; i < wr_pos; i++)
        {
            wrs[i].next = &wrs[i+1];
            wrs[i].sg_list = &sges[i];
            wrs[i].num_sge = 1;
        }
        wrs[wr_pos-1].next = NULL;
        ibv_send_wr *bad_wr = NULL;
        int err = ibv_post_send(id->qp, &wrs[0], &bad_wr);
        if (err || bad_wr)
        {
            fprintf(stderr, "Posting RDMA send failed: %s\n", strerror(err));
            exit(1);
        }
        cur_send += wr_pos;
        outbox_wrs.push_back(wr_pos);
        outbox_pos++;
    }
}

#define RDMA_EVENTS_AT_ONCE 32

void nfs_rdma_conn_t::handle_io()
{
    auto conn = this;
    // Request next notification
    ibv_cq *ev_cq;
    void *ev_ctx;
    // This is inefficient as it calls read()... (but there is no other way)
    if (ibv_get_cq_event(conn_dev->channel, &ev_cq, &ev_ctx) == 0)
    {
        ibv_ack_cq_events(conn_dev->cq, 1);
    }
    if (ibv_req_notify_cq(conn_dev->cq, 0) != 0)
    {
        fprintf(stderr, "Failed to request RDMA completion notification, exiting\n");
        exit(1);
    }
    ibv_wc wc[RDMA_EVENTS_AT_ONCE];
    int event_count;
    do
    {
        event_count = ibv_poll_cq(conn_dev->cq, RDMA_EVENTS_AT_ONCE, wc);
        for (int i = 0; i < event_count; i++)
        {
            if (wc[i].status != IBV_WC_SUCCESS)
            {
                fprintf(stderr, "RDMA work request failed for queue %d with status: %s, stopping client\n", wc[i].qp_num, ibv_wc_status_str(wc[i].status));
                conn->client->stop();
                // but continue to handle events to purge the queue
            }
            if (wc[i].wr_id == 1)
            {
                // 1 = receive
                auto b = conn->recv_buffers[conn->next_recv_buf];
                // Due to the credit-based flow control in RPC-RDMA, we can just remove that buffer and reuse it later
                conn->recv_buffers.erase(conn->recv_buffers.begin()+conn->next_recv_buf, conn->recv_buffers.begin()+conn->next_recv_buf+1);
                if (conn->cur_credit > 0 && !conn->client->stopped)
                {
                    // Increase client refcount while the RPC call is being processed
                    conn->client->refs++;
                    conn->cur_credit--;
                    conn->handle_recv(b.buf, wc[i].byte_len);
                }
                else
                {
                    fprintf(stderr, "Warning: NFS client credit exceeded for queue %d, stopping client\n", wc[i].qp_num);
                    conn->client->stop();
                }
            }
            else if (wc[i].wr_id == 2)
            {
                // 2 = send
                auto rop = conn->outbox[0];
                conn->cur_send -= conn->outbox_wrs[0];
                conn->outbox.erase(conn->outbox.begin(), conn->outbox.begin()+1);
                conn->outbox_wrs.erase(conn->outbox_wrs.begin(), conn->outbox_wrs.begin()+1);
                conn->outbox_pos--;
                // Retry send for corner cases of exceeded max_send_wr
                conn->post_send();
                // Free rpc_op
                conn->free_rdma_rpc_op(rop);
            }
            else if (wc[i].wr_id == 3)
            {
                // 3 = chunk read
                auto rop = conn->chunk_inbox[0];
                conn->chunk_inbox.erase(conn->chunk_inbox.begin(), conn->chunk_inbox.begin()+1);
                size_t read_chunk_count = 0;
                for (auto cur = rop->in_rdma_msg.rdma_body.rdma_msg.rdma_reads; cur; cur = cur->next)
                    read_chunk_count++;
                conn->cur_send -= read_chunk_count;
                conn->cur_rdma_reads -= read_chunk_count;
                conn->client->handle_rpc_op(rop);
                // Retry send for corner cases of exceeded max_send_wr
                conn->post_send();
            }
            else
            {
                fprintf(stderr, "BUG: unknown RDMA work completion with wr_id=%ju\n", wc[i].wr_id);
            }
        }
    } while (event_count > 0);
}

void nfs_rdma_conn_t::free_rdma_rpc_op(rpc_op_t *rop)
{
    if (rop->buffer)
    {
        rdma_malloc_free(conn_dev->alloc, rop->buffer);
        rop->buffer = NULL;
    }
    auto rdma_chunk = xdr_get_rdma_chunk(rop->xdrs);
    if (rdma_chunk)
    {
        rdma_malloc_free(conn_dev->alloc, rdma_chunk);
        xdr_set_rdma_chunk(rop->xdrs, NULL);
    }
    ctx->proxy->free_xdr(rop->xdrs);
    free(rop);
    cur_credit++;
    client->deref();
}

void nfs_rdma_conn_t::reuse_buffer(void *buf)
{
    auto ub = used_buffers.at(buf);
    recv_buffers.push_back(ub);
    post_recv(ub);
}

// returns false if handling is done, returns true if handling is continued asynchronously
bool nfs_rdma_conn_t::handle_recv(void *buf, size_t len)
{
    XDR *xdrs = ctx->proxy->get_xdr();
    xdr_set_rdma(xdrs);
    // Decode the RDMA-RPC header
    rdma_msg rmsg;
    if (!xdr_decode(xdrs, buf, len, (xdrproc_t)xdr_rdma_msg, &rmsg))
    {
        // Invalid message, ignore it
        fprintf(stderr, "Invalid RDMA-RPC header on connection 0x%jx, ignoring message\n", (uint64_t)this);
ignore_msg:
        reuse_buffer(buf);
        ctx->proxy->free_xdr(xdrs);
        client->deref();
        return 0;
    }
    if (rmsg.rdma_vers != 1 || rmsg.rdma_body.proc != RDMA_MSG)
    {
        // Bad RDMA-RPC version or message type
        fprintf(
            stderr, "Unsupported RDMA-RPC version (%d) or message type (%d), sending %s\n",
            rmsg.rdma_vers, rmsg.rdma_body.proc,
            rmsg.rdma_vers != 1 ? "ERR_VERS" : "ERR_CHUNK"
        );
        rpc_op_t *rop = (rpc_op_t*)malloc_or_die(sizeof(rpc_op_t));
        *rop = (rpc_op_t){
            .client = this,
            .xdrs = xdrs,
            .in_msg = {
                .xid = rmsg.rdma_xid,
            },
            .in_rdma_msg = rmsg,
            .rdma_error = rmsg.rdma_vers != 1 ? ERR_VERS : ERR_CHUNK,
        };
        rpc_queue_reply(rop);
        // Incoming buffer isn't needed to handle request, so return 0
        return 0;
    }
    rpc_msg inmsg;
    if (!xdr_rpc_msg(xdrs, &inmsg))
    {
        // Invalid message, ignore it
        fprintf(stderr, "Invalid RDMA-RPC body on connection 0x%jx, ignoring message\n", (uint64_t)this);
        goto ignore_msg;
    }
    if (inmsg.xid != rmsg.rdma_xid)
    {
        fprintf(stderr, "RPC XID (%u) and RDMA XID (%u) mismatch on connection 0x%jx, ignoring message\n", inmsg.xid, rmsg.rdma_xid, (uint64_t)this);
        goto ignore_msg;
    }
    if (inmsg.body.dir != RPC_CALL)
    {
        fprintf(stderr, "Non-RPC_CALL message direction (%d) on connection 0x%jx, ignoring message\n", inmsg.body.dir, (uint64_t)this);
        goto ignore_msg;
    }
    rpc_op_t *rop = client->create_rpc_op(xdrs, buf, &inmsg, &rmsg);
    if (!rop)
    {
        // No such procedure
        return 0;
    }
    // Read chunks may only be provided for WRITE3 and SYMLINK3
    if (inmsg.body.cbody.prog == NFS_PROGRAM &&
        rmsg.rdma_body.rdma_msg.rdma_reads &&
        inmsg.body.cbody.proc != NFS3_WRITE && inmsg.body.cbody.proc != NFS3_SYMLINK)
    {
        fprintf(stderr, "Read chunk(s) are provided for non-WRITE or SYMLINK operation (procedure %u), sending ERR_CHUNK\n", inmsg.body.cbody.proc);
        rop->rdma_error = ERR_CHUNK;
        rpc_queue_reply(rop);
        return 0;
    }
    // Write chunk may be provided only for READ3 and READLINK3
    if (inmsg.body.cbody.prog == NFS_PROGRAM &&
        rmsg.rdma_body.rdma_msg.rdma_writes &&
        inmsg.body.cbody.proc != NFS3_READ && inmsg.body.cbody.proc != NFS3_READLINK)
    {
        fprintf(stderr, "Write chunk(s) are provided for non-READ or READLINK operation (procedure %u), sending ERR_CHUNK\n", inmsg.body.cbody.proc);
        rop->rdma_error = ERR_CHUNK;
        rpc_queue_reply(rop);
        return 0;
    }
    // Only 1 write chunk is supported
    if (inmsg.body.cbody.prog == NFS_PROGRAM &&
        rmsg.rdma_body.rdma_msg.rdma_writes && (
        rmsg.rdma_body.rdma_msg.rdma_writes->next ||
        rmsg.rdma_body.rdma_msg.rdma_writes->entry.target.target_len != 1))
    {
        int n = 0, seg = 0;
        for (auto cur = rmsg.rdma_body.rdma_msg.rdma_writes; cur; cur = cur->next)
        {
            n++;
            seg += cur->entry.target.target_len;
        }
        fprintf(stderr, "Only 1 write chunk with 1 segment may be provided, but we got %d chunks with %d segments, sending ERR_CHUNK\n", n, seg);
        rop->rdma_error = ERR_CHUNK;
        rpc_queue_reply(rop);
        return 0;
    }
    // Process read chunk(s)
    if (rmsg.rdma_body.rdma_msg.rdma_reads)
    {
        int r = post_chunk_reads(rop, true);
        return (r >= 0 ? 1 : 0);
    }
    return client->handle_rpc_op(rop);
}

int nfs_rdma_conn_t::post_chunk_reads(rpc_op_t *rop, bool push)
{
    rop->referenced = 1;
    size_t read_chunk_size = 0;
    size_t read_chunk_count = 0;
    for (auto cur = rop->in_rdma_msg.rdma_body.rdma_msg.rdma_reads; cur; cur = cur->next)
    {
        read_chunk_size += cur->entry.target.length;
        read_chunk_count++;
    }
    if (read_chunk_count > max_rdma_reads)
    {
        fprintf(stderr, "Read chunk count (%zu) exceeds max_rdma_reads (%d), sending ERR_CHUNK\n", read_chunk_count, max_rdma_reads);
        rop->rdma_error = ERR_CHUNK;
        rpc_queue_reply(rop);
        return -1;
    }
    if (cur_send+read_chunk_count > ctx->max_send_wr ||
        cur_rdma_reads+read_chunk_count > max_rdma_reads)
    {
        // Send it later
        if (push)
            chunk_read_postponed.push_back(rop);
        return 0;
    }
    void *buf = rdma_malloc_alloc(conn_dev->alloc, read_chunk_size);
    auto buf_lkey = rdma_malloc_get_lkey(conn_dev->alloc, buf);
    ibv_sge chunk_sge[read_chunk_count];
    ibv_send_wr chunk_wr[read_chunk_count];
    size_t i = 0;
    size_t pos = 0;
    for (auto cur = rop->in_rdma_msg.rdma_body.rdma_msg.rdma_reads; cur; cur = cur->next)
    {
        chunk_sge[i] = {
            .addr = (uintptr_t)((uint8_t*)buf + pos),
            .length = cur->entry.target.length,
            .lkey = buf_lkey,
        };
        chunk_wr[i] = {
            .wr_id = 3, // 3 is chunk read
            .next = (i == read_chunk_count-1 ? NULL : &chunk_wr[i+1]),
            .sg_list = &chunk_sge[i],
            .num_sge = 1,
            .opcode = IBV_WR_RDMA_READ,
            .send_flags = (unsigned)(i == read_chunk_count-1 ? IBV_SEND_SIGNALED : 0),
            .wr = {
                .rdma = {
                    .remote_addr = cur->entry.target.offset,
                    .rkey = cur->entry.target.handle,
                },
            },
        };
        pos += cur->entry.target.length;
        i++;
    }
    assert(i == read_chunk_count);
    assert(pos == read_chunk_size);
    ibv_send_wr *bad_wr = NULL;
    int err = ibv_post_send(id->qp, &chunk_wr[0], &bad_wr);
    if (err || bad_wr)
    {
        fprintf(stderr, "Posting RDMA read failed: %s\n", strerror(err));
        exit(1);
    }
    xdr_set_rdma_chunk(rop->xdrs, buf);
    chunk_inbox.push_back(rop);
    cur_send += read_chunk_count;
    cur_rdma_reads += read_chunk_count;
    return 1;
}

void *nfs_client_t::rdma_malloc(size_t size)
{
    return rdma_malloc_alloc(rdma_conn->conn_dev->alloc, size);
}

void nfs_client_t::rdma_free(void *buf)
{
    rdma_malloc_free(rdma_conn->conn_dev->alloc, buf);
}

void nfs_client_t::destroy_rdma_conn()
{
    if (rdma_conn)
    {
        delete rdma_conn;
        rdma_conn = NULL;
    }
}

#endif
