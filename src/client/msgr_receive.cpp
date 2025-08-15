// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include "messenger.h"

void osd_messenger_t::read_requests()
{
    for (int i = 0; i < read_ready_clients.size(); i++)
    {
        int peer_fd = read_ready_clients[i];
        auto cl_it = clients.find(peer_fd);
        if (cl_it == clients.end() || !cl_it->second || cl_it->second->read_msg.msg_iovlen)
        {
            continue;
        }
        auto cl = cl_it->second;
        if (cl->read_remaining < receive_buffer_size)
        {
            cl->read_iov.iov_base = cl->in_buf;
            cl->read_iov.iov_len = receive_buffer_size;
            cl->read_msg.msg_iov = &cl->read_iov;
            cl->read_msg.msg_iovlen = 1;
        }
        else
        {
            cl->read_iov.iov_base = 0;
            cl->read_iov.iov_len = cl->read_remaining;
            cl->read_msg.msg_iov = cl->recv_list.get_iovec();
            cl->read_msg.msg_iovlen = cl->recv_list.get_size();
        }
        cl->refs++;
        if (ringloop && !use_sync_send_recv)
        {
            auto iothread = iothreads.size() ? iothreads[peer_fd % iothreads.size()] : NULL;
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
                cl->read_msg.msg_iovlen = 0;
                read_ready_clients.erase(read_ready_clients.begin(), read_ready_clients.begin() + i);
                return;
            }
            ring_data_t* data = ((ring_data_t*)sqe->user_data);
            data->callback = [this, cl](ring_data_t *data) { handle_read(data->res, cl); };
            io_uring_prep_recvmsg(sqe, peer_fd, &cl->read_msg, 0);
            if (iothread)
            {
                iothread->add_sqe(sqe_local);
            }
        }
        else
        {
            int result = recvmsg(peer_fd, &cl->read_msg, 0);
            if (result < 0)
            {
                result = -errno;
            }
            // like set_immediate
            tfd->set_timer_us(0, false, [this, result, cl](int){ handle_read(result, cl); });
        }
    }
    read_ready_clients.clear();
}

bool osd_messenger_t::handle_read(int result, osd_client_t *cl)
{
    bool ret = false;
    int peer_fd = cl->peer_fd;
    cl->read_msg.msg_iovlen = 0;
    cl->refs--;
    if (cl->peer_state == PEER_STOPPED)
    {
        if (cl->refs <= 0)
        {
            delete cl;
        }
        return false;
    }
    if (result <= 0 && result != -EAGAIN && result != -EINTR)
    {
        // this is a client socket, so don't panic on error. just disconnect it
        if (result != 0)
        {
            fprintf(stderr, "Client %d socket read error: %d (%s). Disconnecting client\n", cl->peer_fd, -result, strerror(-result));
        }
        stop_client(cl->peer_fd);
        return false;
    }
    if (result == -EAGAIN || result == -EINTR || result < cl->read_iov.iov_len)
    {
        cl->read_ready--;
        if (cl->read_ready > 0)
            read_ready_clients.push_back(cl->peer_fd);
    }
    else
    {
        read_ready_clients.push_back(cl->peer_fd);
    }
    if (result > 0)
    {
        if (cl->read_iov.iov_base == cl->in_buf)
        {
            if (!handle_read_buffer(cl, cl->in_buf, result))
            {
                clear_immediate_ops(peer_fd);
                return false;
            }
        }
        else
        {
            // Long data
            cl->read_remaining -= result;
            cl->recv_list.eat(result);
            if (cl->recv_list.done >= cl->recv_list.count)
            {
                if (!handle_finished_read(cl))
                {
                    clear_immediate_ops(peer_fd);
                    return false;
                }
            }
        }
        if (result >= cl->read_iov.iov_len)
        {
            ret = true;
        }
    }
    handle_immediate_ops();
    return ret;
}

void osd_messenger_t::clear_immediate_ops(int peer_fd)
{
    size_t i = 0, j = 0;
    while (i < set_immediate_ops.size())
    {
        if (set_immediate_ops[i]->peer_fd == peer_fd)
        {
            delete set_immediate_ops[i];
        }
        else
        {
            if (i != j)
                set_immediate_ops[j] = set_immediate_ops[i];
            j++;
        }
        i++;
    }
    set_immediate_ops.resize(j);
}

void osd_messenger_t::handle_immediate_ops()
{
    for (auto op: set_immediate_ops)
    {
        if (op->op_type == OSD_OP_IN)
        {
            exec_op(op);
        }
        else
        {
            // Copy lambda to be unaffected by `delete op`
            std::function<void(osd_op_t*)>(op->callback)(op);
        }
    }
    set_immediate_ops.clear();
}

bool osd_messenger_t::handle_read_buffer(osd_client_t *cl, void *curbuf, int remain)
{
    // Compose operation(s) from the buffer
    while (remain > 0)
    {
        if (!cl->read_op)
        {
            cl->read_op = new osd_op_t;
            cl->read_op->peer_fd = cl->peer_fd;
            cl->read_op->op_type = OSD_OP_IN;
            cl->recv_list.push_back(cl->read_op->req.buf, OSD_PACKET_SIZE);
            cl->read_remaining = OSD_PACKET_SIZE;
            cl->read_state = CL_READ_HDR;
        }
        while (cl->recv_list.done < cl->recv_list.count && remain > 0)
        {
            iovec* cur = cl->recv_list.get_iovec();
            if (cur->iov_len > remain)
            {
                memcpy(cur->iov_base, curbuf, remain);
                cl->read_remaining -= remain;
                cur->iov_len -= remain;
                cur->iov_base = (uint8_t*)cur->iov_base + remain;
                remain = 0;
            }
            else
            {
                memcpy(cur->iov_base, curbuf, cur->iov_len);
                curbuf = (uint8_t*)curbuf + cur->iov_len;
                cl->read_remaining -= cur->iov_len;
                remain -= cur->iov_len;
                cur->iov_len = 0;
                cl->recv_list.done++;
            }
        }
        if (cl->recv_list.done >= cl->recv_list.count)
        {
            if (!handle_finished_read(cl))
            {
                return false;
            }
        }
    }
    return true;
}

bool osd_messenger_t::handle_finished_read(osd_client_t *cl)
{
    // Reset OSD ping state
    cl->ping_time_remaining = 0;
    cl->idle_time_remaining = osd_idle_timeout;
    cl->recv_list.reset();
    if (cl->read_state == CL_READ_HDR)
    {
        if (cl->read_op->req.hdr.magic == SECONDARY_OSD_REPLY_MAGIC)
            return handle_reply_hdr(cl);
        else if (cl->read_op->req.hdr.magic == SECONDARY_OSD_OP_MAGIC)
        {
            if (cl->check_sequencing)
            {
                if (cl->read_op->req.hdr.id != cl->read_op_id)
                {
                    fprintf(stderr, "Warning: operation sequencing is broken on client %d: expected num %ju, got %ju, stopping client\n", cl->peer_fd, cl->read_op_id, cl->read_op->req.hdr.id);
                    stop_client(cl->peer_fd);
                    return false;
                }
                cl->read_op_id++;
            }
            handle_op_hdr(cl);
        }
        else
        {
            fprintf(stderr, "Received garbage: magic=%jx id=%ju opcode=%jx from %d\n", cl->read_op->req.hdr.magic, cl->read_op->req.hdr.id, cl->read_op->req.hdr.opcode, cl->peer_fd);
            stop_client(cl->peer_fd);
            return false;
        }
    }
    else if (cl->read_state == CL_READ_DATA)
    {
        // Operation is ready
        cl->received_ops.push_back(cl->read_op);
        set_immediate_ops.push_back(cl->read_op);
        cl->read_op = NULL;
        cl->read_state = 0;
    }
    else if (cl->read_state == CL_READ_REPLY_DATA)
    {
        // Reply is ready
        handle_reply_ready(cl->read_op);
        cl->read_op = NULL;
        cl->read_state = 0;
    }
    else
    {
        assert(0);
    }
    return true;
}

void osd_messenger_t::handle_op_hdr(osd_client_t *cl)
{
    osd_op_t *cur_op = cl->read_op;
    if (cur_op->req.hdr.opcode == OSD_OP_SEC_READ)
    {
        cl->read_remaining = 0;
    }
    else if (cur_op->req.hdr.opcode == OSD_OP_SEC_WRITE ||
        cur_op->req.hdr.opcode == OSD_OP_SEC_WRITE_STABLE)
    {
        if (cur_op->req.sec_rw.attr_len > 0)
        {
            if (cur_op->req.sec_rw.attr_len > sizeof(unsigned))
                cur_op->bitmap = cur_op->rmw_buf = malloc_or_die(cur_op->req.sec_rw.attr_len);
            else
                cur_op->bitmap = &cur_op->bmp_data;
            cl->recv_list.push_back(cur_op->bitmap, cur_op->req.sec_rw.attr_len);
        }
        if (cur_op->req.sec_rw.len > 0)
        {
            cur_op->buf = memalign_or_die(MEM_ALIGNMENT, cur_op->req.sec_rw.len);
            cl->recv_list.push_back(cur_op->buf, cur_op->req.sec_rw.len);
        }
        cl->read_remaining = cur_op->req.sec_rw.len + cur_op->req.sec_rw.attr_len;
    }
    else if (cur_op->req.hdr.opcode == OSD_OP_SEC_STABILIZE ||
        cur_op->req.hdr.opcode == OSD_OP_SEC_ROLLBACK)
    {
        if (cur_op->req.sec_stab.len > 0)
        {
            cur_op->buf = memalign_or_die(MEM_ALIGNMENT, cur_op->req.sec_stab.len);
            cl->recv_list.push_back(cur_op->buf, cur_op->req.sec_stab.len);
        }
        cl->read_remaining = cur_op->req.sec_stab.len;
    }
    else if (cur_op->req.hdr.opcode == OSD_OP_SEC_READ_BMP)
    {
        if (cur_op->req.sec_read_bmp.len > 0)
        {
            cur_op->buf = memalign_or_die(MEM_ALIGNMENT, cur_op->req.sec_read_bmp.len);
            cl->recv_list.push_back(cur_op->buf, cur_op->req.sec_read_bmp.len);
        }
        cl->read_remaining = cur_op->req.sec_read_bmp.len;
    }
    else if (cur_op->req.hdr.opcode == OSD_OP_WRITE)
    {
        if (cur_op->req.rw.len > 0)
        {
            cur_op->buf = memalign_or_die(MEM_ALIGNMENT, cur_op->req.rw.len);
            cl->recv_list.push_back(cur_op->buf, cur_op->req.rw.len);
        }
        cl->read_remaining = cur_op->req.rw.len;
    }
    else if (cur_op->req.hdr.opcode == OSD_OP_SHOW_CONFIG)
    {
        if (cur_op->req.show_conf.json_len > 0)
        {
            cur_op->buf = malloc_or_die(cur_op->req.show_conf.json_len+1);
            ((uint8_t*)cur_op->buf)[cur_op->req.show_conf.json_len] = 0;
            cl->recv_list.push_back(cur_op->buf, cur_op->req.show_conf.json_len);
        }
        cl->read_remaining = cur_op->req.show_conf.json_len;
    }
    /*else if (cur_op->req.hdr.opcode == OSD_OP_READ ||
        cur_op->req.hdr.opcode == OSD_OP_SCRUB ||
        cur_op->req.hdr.opcode == OSD_OP_DESCRIBE)
    {
        cl->read_remaining = 0;
    }*/
    if (cl->read_remaining > 0)
    {
        // Read data
        cl->read_state = CL_READ_DATA;
    }
    else
    {
        // Operation is ready
        cl->received_ops.push_back(cur_op);
        set_immediate_ops.push_back(cur_op);
        cl->read_op = NULL;
        cl->read_state = 0;
    }
}

bool osd_messenger_t::handle_reply_hdr(osd_client_t *cl)
{
    auto req_it = cl->sent_ops.find(cl->read_op->req.hdr.id);
    if (req_it == cl->sent_ops.end())
    {
        // Command out of sync. Drop connection
        fprintf(stderr, "Client %d command out of sync: id %ju\n", cl->peer_fd, cl->read_op->req.hdr.id);
        stop_client(cl->peer_fd);
        return false;
    }
    osd_op_t *op = req_it->second;
    memcpy(op->reply.buf, cl->read_op->req.buf, OSD_PACKET_SIZE);
    cl->sent_ops.erase(req_it);
    if (op->reply.hdr.opcode == OSD_OP_SEC_READ || op->reply.hdr.opcode == OSD_OP_READ)
    {
        // Read data. In this case we assume that the buffer is preallocated by the caller (!)
        unsigned bmp_len = (op->reply.hdr.opcode == OSD_OP_SEC_READ ? op->reply.sec_rw.attr_len : op->reply.rw.bitmap_len);
        unsigned expected_size = (op->reply.hdr.opcode == OSD_OP_SEC_READ ? op->req.sec_rw.len : op->req.rw.len);
        if (op->reply.hdr.retval >= 0 && (op->reply.hdr.retval != expected_size || bmp_len > op->bitmap_len))
        {
            // Check reply length to not overflow the buffer
            fprintf(stderr, "Client %d read reply of different length: expected %u+%u, got %jd+%u\n",
                cl->peer_fd, expected_size, op->bitmap_len, op->reply.hdr.retval, bmp_len);
            cl->sent_ops[op->req.hdr.id] = op;
            stop_client(cl->peer_fd);
            return false;
        }
        if (bmp_len > 0)
        {
            assert(op->bitmap);
            cl->recv_list.push_back(op->bitmap, bmp_len);
            cl->read_remaining += bmp_len;
        }
        if (op->reply.hdr.retval > 0)
        {
            assert(op->iov.count > 0);
            cl->recv_list.append(op->iov);
            cl->read_remaining += op->reply.hdr.retval;
        }
        if (cl->read_remaining == 0)
        {
            goto reuse;
        }
        delete cl->read_op;
        cl->read_op = op;
        cl->read_state = CL_READ_REPLY_DATA;
    }
    else if (op->reply.hdr.opcode == OSD_OP_SEC_LIST && op->reply.hdr.retval > 0)
    {
        assert(!op->iov.count);
        delete cl->read_op;
        cl->read_op = op;
        cl->read_state = CL_READ_REPLY_DATA;
        cl->read_remaining = sizeof(obj_ver_id) * op->reply.hdr.retval;
        op->buf = memalign_or_die(MEM_ALIGNMENT, cl->read_remaining);
        cl->recv_list.push_back(op->buf, cl->read_remaining);
    }
    else if (op->reply.hdr.opcode == OSD_OP_SEC_READ_BMP && op->reply.hdr.retval > 0)
    {
        assert(!op->iov.count);
        delete cl->read_op;
        cl->read_op = op;
        cl->read_state = CL_READ_REPLY_DATA;
        cl->read_remaining = op->reply.hdr.retval;
        free(op->buf);
        op->buf = memalign_or_die(MEM_ALIGNMENT, cl->read_remaining);
        cl->recv_list.push_back(op->buf, cl->read_remaining);
    }
    else if (op->reply.hdr.opcode == OSD_OP_SHOW_CONFIG && op->reply.hdr.retval > 0)
    {
        delete cl->read_op;
        cl->read_op = op;
        cl->read_state = CL_READ_REPLY_DATA;
        cl->read_remaining = op->reply.hdr.retval;
        free(op->buf);
        op->buf = malloc_or_die(op->reply.hdr.retval);
        cl->recv_list.push_back(op->buf, op->reply.hdr.retval);
    }
    else if (op->reply.hdr.opcode == OSD_OP_DESCRIBE && op->reply.describe.result_bytes > 0)
    {
        delete cl->read_op;
        cl->read_op = op;
        cl->read_state = CL_READ_REPLY_DATA;
        cl->read_remaining = op->reply.describe.result_bytes;
        free(op->buf);
        op->buf = malloc_or_die(op->reply.describe.result_bytes);
        cl->recv_list.push_back(op->buf, op->reply.describe.result_bytes);
    }
    else
    {
reuse:
        // It's fine to reuse cl->read_op for the next reply
        handle_reply_ready(op);
        cl->recv_list.push_back(cl->read_op->req.buf, OSD_PACKET_SIZE);
        cl->read_remaining = OSD_PACKET_SIZE;
        cl->read_state = CL_READ_HDR;
    }
    return true;
}

void osd_messenger_t::handle_reply_ready(osd_op_t *op)
{
    // Measure subop latency
    timespec tv_end;
    clock_gettime(CLOCK_REALTIME, &tv_end);
    stats.subop_stat_count[op->req.hdr.opcode]++;
    if (!stats.subop_stat_count[op->req.hdr.opcode])
    {
        stats.subop_stat_count[op->req.hdr.opcode]++;
        stats.subop_stat_sum[op->req.hdr.opcode] = 0;
    }
    stats.subop_stat_sum[op->req.hdr.opcode] += (
        (tv_end.tv_sec - op->tv_begin.tv_sec)*1000000 +
        (tv_end.tv_nsec - op->tv_begin.tv_nsec)/1000
    );
    set_immediate_ops.push_back(op);
}
