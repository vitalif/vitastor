// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#define _XOPEN_SOURCE
#include <limits.h>
#include <sys/epoll.h>

#ifdef WITH_OPENSSL
#include <openssl/conf.h>
#include <openssl/evp.h>
#include <openssl/err.h>
#endif

#include "messenger.h"
#include "msgr_iothread.h"

void osd_messenger_t::outbox_push(osd_op_t *cur_op)
{
    assert(cur_op->client_id);
    auto cl_it = clients.find(cur_op->client_id);
    if (cl_it == clients.end() || cl_it->second->peer_state == PEER_STOPPED)
    {
        delete cur_op;
        return;
    }
    osd_client_t *cl = cl_it->second;
    if (cur_op->op_type == OSD_OP_OUT)
    {
        clock_gettime(CLOCK_REALTIME, &cur_op->tv_begin);
        cur_op->req.hdr.id = ++cl->send_op_id;
        cl->sent_ops[cur_op->req.hdr.id] = cur_op;
    }
    else
    {
        // Remove the operation from received op list
        bool found = false;
        for (auto it = cl->received_ops.begin(); it != cl->received_ops.end(); it++)
        {
            if (*it == cur_op)
            {
                found = true;
                cl->received_ops.erase(it, it+1);
                break;
            }
        }
        // Can't be not found because client IDs are unique
        assert(found);
        measure_exec(cur_op);
    }
    cl->write_ops.push_back(cur_op);
#ifdef WITH_RDMA
    if (cl->peer_state == PEER_RDMA)
    {
        try_send_rdma(cl);
        return;
    }
#endif
    if (!ringloop)
    {
        // FIXME: It's worse because it doesn't allow batching
        while (cl->write_ops.size())
        {
            try_send(cl);
        }
    }
    else
    {
        if ((cl->write_msg.msg_iovlen > 0 || !try_send(cl)) && (cl->write_state == 0))
        {
            cl->write_state = CL_WRITE_READY;
            write_ready_clients.push_back(cur_op->client_id);
        }
        ringloop->wakeup();
    }
}

bool osd_messenger_t::try_send(osd_client_t *cl)
{
    if (!cl->write_op && !cl->write_ops.size() || cl->write_msg.msg_iovlen > 0 || cl->peer_state == PEER_STOPPED || cl->peer_fd < 0)
    {
        return true;
    }
    assert(cl->peer_state != PEER_RDMA);
    while ((cl->write_op || cl->write_ops.size()) && cl->send_list.size() < IOV_MAX)
    {
        if (!cl->write_op)
        {
            cl->write_op = cl->write_ops.front();
            cl->write_ops.pop_front();
        }
        osd_op_t *op = cl->write_op;
        op_get_write_buffers(cl, cl->send_list);
        if (!cl->write_op && op->op_type == OSD_OP_IN)
        {
            cl->send_free_ops.push_back(op);
        }
    }
    if (ringloop && !use_sync_send_recv)
    {
        auto iothread = iothreads.size() ? iothreads[cl->peer_fd % iothreads.size()] : NULL;
        io_uring_sqe sqe_local;
        ring_data_t data_local;
        io_uring_sqe* sqe = (iothread ? &sqe_local : ringloop->get_sqe());
        if (iothread)
        {
            sqe_local = { .user_data = (uint64_t)&data_local };
            data_local = {};
        }
        if (!sqe)
        {
            return false;
        }
        cl->send_list_size = 0;
        for (auto & iov: cl->send_list)
        {
            cl->send_list_size += iov.iov_len;
        }
        cl->write_msg.msg_iov = cl->send_list.data();
        cl->write_msg.msg_iovlen = cl->send_list.size() < IOV_MAX ? cl->send_list.size() : IOV_MAX;
        cl->refs++;
        ring_data_t* data = ((ring_data_t*)sqe->user_data);
        data->callback = [this, cl](ring_data_t *data) { handle_send(data->res, data->prev, data->more, cl); };
        bool use_zc = has_sendmsg_zc && min_zerocopy_send_size >= 0;
        if (use_zc && min_zerocopy_send_size > 0 &&
            cl->send_list_size/cl->write_msg.msg_iovlen < min_zerocopy_send_size)
        {
            use_zc = false;
        }
        if (use_zc)
        {
            io_uring_prep_sendmsg_zc(sqe, cl->peer_fd, &cl->write_msg, MSG_WAITALL);
        }
        else
        {
            io_uring_prep_sendmsg(sqe, cl->peer_fd, &cl->write_msg, MSG_WAITALL);
        }
        if (iothread)
        {
            iothread->add_sqe(sqe_local);
        }
    }
    else
    {
        cl->write_msg.msg_iov = cl->send_list.data();
        cl->write_msg.msg_iovlen = cl->send_list.size() < IOV_MAX ? cl->send_list.size() : IOV_MAX;
        cl->refs++;
        int result = sendmsg(cl->peer_fd, &cl->write_msg, MSG_NOSIGNAL);
        if (result < 0)
        {
            result = -errno;
        }
        // like set_immediate
        tfd->set_timer_us(0, false, [this, result, cl](int){ handle_send(result, false, false, cl); });
    }
    return true;
}

void osd_messenger_t::send_replies()
{
    for (int i = 0; i < write_ready_clients.size(); i++)
    {
        uint64_t client_id = write_ready_clients[i];
        auto cl_it = clients.find(client_id);
        if (cl_it != clients.end() && cl_it->second->peer_state != PEER_RDMA && !try_send(cl_it->second))
        {
            write_ready_clients.erase(write_ready_clients.begin(), write_ready_clients.begin() + i);
            return;
        }
    }
    write_ready_clients.clear();
}

void osd_messenger_t::handle_send(int result, bool prev, bool more, osd_client_t *cl)
{
    if (!prev)
    {
        cl->write_msg.msg_iovlen = 0;
        cl->send_list.clear();
    }
    if (!more)
    {
        cl->refs--;
    }
    if (cl->peer_state == PEER_STOPPED)
    {
        if (cl->refs <= 0)
        {
            destroy_client(cl);
        }
        return;
    }
    if (result < 0 && result != -EAGAIN && result != -EINTR)
    {
        // this is a client socket, so don't panic. just disconnect it
        fprintf(stderr, "Client %ju socket write error: %d (%s). Disconnecting client\n", cl->client_id, -result, strerror(-result));
        stop_client(cl->client_id);
        return;
    }
    if (result >= 0)
    {
        if (prev)
        {
            // Second notification - only free a batch of postponed ops
            int i = 0;
            for (; i < cl->zc_free_list.size() && cl->zc_free_list[i]; i++)
                delete cl->zc_free_list[i];
            if (i > 0)
                cl->zc_free_list.erase(cl->zc_free_list.begin(), cl->zc_free_list.begin()+i+1);
            return;
        }
        if (cl->send_list_size > result)
        {
            fprintf(stderr, "Client %ju socket write error: expected to send "
                "%zu bytes with MSG_WAITALL but sent %u. Disconnecting client\n", cl->client_id, cl->send_list_size, result);
            stop_client(cl->peer_fd);
            return;
        }
        for (auto op: cl->send_free_ops)
        {
            if (more)
                cl->zc_free_list.push_back(op);
            else
                delete op;
        }
        if (more)
            cl->zc_free_list.push_back(NULL); // end marker
        cl->send_free_ops.clear();
        cl->write_state = cl->write_op || cl->write_ops.size() ? CL_WRITE_READY : 0;
#ifdef WITH_RDMA
        if (cl->rdma_conn && !cl->write_op && !cl->write_ops.size() && cl->peer_state == PEER_RDMA_CONNECTING)
        {
            // FIXME: Ignore pings during RDMA state transition
            if (log_level > 0)
            {
                fprintf(stderr, "Successfully connected with client %ju using RDMA\n", cl->client_id);
            }
            cl->peer_state = PEER_RDMA;
            // Add the initial receive request
            init_recv_rdma(cl);
        }
#endif
    }
    if (cl->write_state != 0)
    {
        write_ready_clients.push_back(cl->client_id);
    }
}

static inline bool op_write_headers(osd_op_t *op, std::function<bool(uint8_t*, size_t)> op_write_buf)
{
    // Header
    if (!op_write_buf((op->op_type == OSD_OP_IN ? op->reply.buf : op->req.buf), OSD_PACKET_SIZE))
        return false;
    // Bitmap
    if (op->op_type == OSD_OP_IN &&
        op->req.hdr.opcode == OSD_OP_SEC_READ &&
        op->reply.sec_rw.attr_len > 0)
    {
        if (!op_write_buf((uint8_t*)op->bitmap, op->reply.sec_rw.attr_len))
            return false;
    }
    else if (op->op_type == OSD_OP_OUT &&
        (op->req.hdr.opcode == OSD_OP_SEC_WRITE || op->req.hdr.opcode == OSD_OP_SEC_WRITE_STABLE) &&
        op->req.sec_rw.attr_len > 0)
    {
        if (!op_write_buf((uint8_t*)op->bitmap, op->req.sec_rw.attr_len))
            return false;
    }
    if (op->req.hdr.opcode == OSD_OP_SEC_READ_BMP)
    {
        if (op->op_type == OSD_OP_IN && op->reply.hdr.retval > 0)
        {
            if (!op_write_buf((uint8_t*)op->buf, (size_t)op->reply.hdr.retval))
                return false;
        }
        else if (op->op_type == OSD_OP_OUT && op->req.sec_read_bmp.len > 0)
        {
            if (!op_write_buf((uint8_t*)op->buf, (size_t)op->req.sec_read_bmp.len))
                return false;
        }
    }
    return true;
}

static inline bool op_has_data(osd_op_t *op)
{
    return (op->op_type == OSD_OP_IN
        ? (op->req.hdr.opcode == OSD_OP_READ ||
        op->req.hdr.opcode == OSD_OP_SEC_READ ||
        op->req.hdr.opcode == OSD_OP_SEC_LIST ||
        op->req.hdr.opcode == OSD_OP_SHOW_CONFIG ||
        op->req.hdr.opcode == OSD_OP_DESCRIBE)
        : (op->req.hdr.opcode == OSD_OP_WRITE ||
        op->req.hdr.opcode == OSD_OP_SEC_WRITE ||
        op->req.hdr.opcode == OSD_OP_SEC_WRITE_STABLE ||
        op->req.hdr.opcode == OSD_OP_SEC_STABILIZE ||
        op->req.hdr.opcode == OSD_OP_SEC_ROLLBACK ||
        op->req.hdr.opcode == OSD_OP_SHOW_CONFIG)) && op->iov.count > 0;
}

size_t osd_messenger_t::op_copy_to(osd_client_t *cl, uint8_t *dst, size_t dst_len)
{
    size_t done = 0;
    size_t from = cl->write_op_pos;
    auto op_write_buf = [&](uint8_t *src, size_t src_len)
    {
        if (from < src_len)
        {
            size_t n = src_len-from;
            if (n > dst_len-done)
                n = dst_len-done;
            memcpy(dst+done, src+from, n);
            done += n;
            cl->write_op_pos += n;
            from += n;
            if (from < src_len)
                return false;
            from = 0;
        }
        else
            from -= src_len;
        return true;
    };
    if (!op_write_headers(cl->write_op, op_write_buf))
    {
        return done;
    }
    // Operation data
    if (op_has_data(cl->write_op))
    {
        for (int i = 0; i < cl->write_op->iov.count; i++)
        {
            if (!op_write_buf((uint8_t*)cl->write_op->iov.buf[i].iov_base, cl->write_op->iov.buf[i].iov_len))
                return done;
        }
    }
    cl->write_op = NULL;
    cl->write_op_pos = 0;
    return done;
}

void osd_messenger_t::op_get_write_buffers(osd_client_t *cl, std::vector<iovec> & lst)
{
    size_t from = cl->write_op_pos;
    auto op_write_buf = [&](uint8_t *src, size_t src_len)
    {
        if (lst.size() >= IOV_MAX)
            return false;
        if (from < src_len)
        {
            lst.push_back((iovec){ .iov_base = src+from, .iov_len = src_len-from });
            cl->write_op_pos += src_len-from;
            from = 0;
        }
        else
            from -= src_len;
        return true;
    };
    if (!op_write_headers(cl->write_op, op_write_buf))
    {
        return;
    }
    // Operation data
    if (op_has_data(cl->write_op))
    {
        for (int i = 0; i < cl->write_op->iov.count; i++)
        {
            if (!op_write_buf((uint8_t*)cl->write_op->iov.buf[i].iov_base, cl->write_op->iov.buf[i].iov_len))
                return;
        }
    }
    cl->write_op = NULL;
    cl->write_op_pos = 0;
}
