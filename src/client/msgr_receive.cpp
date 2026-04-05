// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#define _XOPEN_SOURCE
#include <limits.h>
#include "messenger.h"
#include "msgr_iothread.h"

void osd_messenger_t::read_requests()
{
    for (int i = 0; i < read_ready_clients.size(); i++)
    {
        uint64_t client_id = read_ready_clients[i];
        auto cl_it = clients.find(client_id);
        if (cl_it == clients.end() || !cl_it->second || cl_it->second->read_msg.msg_iovlen ||
            cl_it->second->peer_state != PEER_CONNECTED)
        {
            continue;
        }
        auto cl = cl_it->second;
        if (cl->read_op && cl->read_op_size-(cl->read_op_pos-OSD_PACKET_SIZE) >= receive_buffer_size)
        {
            op_get_read_buffers(cl, cl->recv_list);
        }
        if (!cl->recv_list.size())
        {
            cl->read_iov.iov_base = cl->in_buf;
            cl->read_iov.iov_len = receive_buffer_size;
            cl->read_msg.msg_iov = &cl->read_iov;
            cl->read_msg.msg_iovlen = 1;
        }
        else
        {
            cl->read_iov.iov_base = 0;
            cl->read_iov.iov_len = 0;
            cl->read_msg.msg_iov = cl->recv_list.data();
            cl->read_msg.msg_iovlen = cl->recv_list.size();
        }
        assert(!cl->read_op || cl->read_op_pos < OSD_PACKET_SIZE || cl->read_op_size >= (cl->read_op_pos-OSD_PACKET_SIZE));
        cl->refs++;
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
                cl->refs--;
                cl->read_msg.msg_iovlen = 0;
                read_ready_clients.erase(read_ready_clients.begin(), read_ready_clients.begin() + i);
                return;
            }
            ring_data_t* data = ((ring_data_t*)sqe->user_data);
            data->callback = [this, cl](ring_data_t *data) { handle_read(data->res, cl); };
            io_uring_prep_recvmsg(sqe, cl->peer_fd, &cl->read_msg, cl->recv_list.size() ? MSG_WAITALL : 0);
            if (iothread)
            {
                iothread->add_sqe(sqe_local);
            }
        }
        else
        {
            int result = recvmsg(cl->peer_fd, &cl->read_msg, 0);
            if (result < 0)
            {
                result = -errno;
            }
            // like set_immediate
            tfd->set_timer_us(0, false, [this, result, cl](int){ handle_read(result, cl); });
        }
    }
    read_ready_clients.clear();
    handle_immediate_ops();
}

void osd_messenger_t::handle_read(int result, osd_client_t *cl)
{
    cl->refs--;
    if (cl->peer_state == PEER_RDMA)
    {
        return;
    }
    if (cl->peer_state == PEER_STOPPED)
    {
        if (cl->refs <= 0)
        {
            destroy_client(cl);
        }
        return;
    }
    if (result <= 0 && result != -EAGAIN && result != -EINTR)
    {
        // this is a client socket, so don't panic on error. just disconnect it
        if (result != 0)
        {
            fprintf(stderr, "Client %ju socket read error: %d (%s). Disconnecting client\n", cl->client_id, -result, strerror(-result));
        }
        stop_client(cl->client_id);
out_wakeup:
        if (set_immediate_ops.size())
            ringloop->wakeup();
        return;
    }
    bool full_read = false;
    if (result > 0)
    {
        if (cl->read_iov.iov_base == cl->in_buf)
        {
            full_read = result >= cl->read_iov.iov_len;
            if (!handle_read_buffer(cl, cl->in_buf, result))
                goto out_wakeup;
        }
        else
        {
            // Reset OSD ping state
            cl->ping_time_remaining = 0;
            cl->idle_time_remaining = osd_idle_timeout;
            // Long data
            size_t i = 0;
            while (i < cl->recv_list.size() && result >= cl->recv_list[i].iov_len)
            {
                if (cl->read_csum_state && cl->recv_list[i].iov_len > 0 &&
                    i != cl->recv_list.size()-1) // skip the checksum itself
                {
                    XXH3_64bits_update(cl->read_csum_state, cl->recv_list[i].iov_base, cl->recv_list[i].iov_len);
                }
                result -= cl->recv_list[i].iov_len;
                i++;
            }
            if (i < cl->recv_list.size())
            {
                cl->recv_list[i].iov_base += result;
                cl->recv_list[i].iov_len -= result;
            }
            else
            {
                full_read = true;
            }
            cl->recv_list.erase(cl->recv_list.begin(), cl->recv_list.begin()+i);
            if (!cl->recv_list.size())
            {
                if (!handle_finished_op(cl))
                    goto out_wakeup;
            }
        }
    }
    cl->read_msg.msg_iovlen = 0;
    if (result == -EAGAIN || result == -EINTR || !full_read)
    {
        cl->read_ready--;
        if (cl->read_ready > 0)
            read_ready_clients.push_back(cl->client_id);
    }
    else
    {
        read_ready_clients.push_back(cl->client_id);
    }
    goto out_wakeup;
}

void osd_messenger_t::handle_immediate_ops()
{
    while (set_immediate_ops.size())
    {
        auto op = set_immediate_ops.front();
        set_immediate_ops.pop_front();
        if (op->op_type == OSD_OP_IN)
        {
            auto cl_it = clients.find(op->client_id);
            if (cl_it != clients.end() && cl_it->second->peer_state != PEER_STOPPED)
                exec_op(op);
            else
                delete op;
        }
        else
        {
            // Copy lambda to be unaffected by `delete op`
            std::function<void(osd_op_t*)>(op->callback)(op);
        }
    }
}

bool osd_messenger_t::handle_read_buffer(osd_client_t *cl, uint8_t *curbuf, size_t bufsize)
{
    // Reset OSD ping state
    cl->ping_time_remaining = 0;
    cl->idle_time_remaining = osd_idle_timeout;
    // Compose operation(s) from the buffer
    size_t done = 0;
    while (done < bufsize)
    {
        if (!cl->read_op)
        {
            cl->read_op = new osd_op_t;
            cl->read_op->client_id = cl->client_id;
            cl->read_op->op_type = OSD_OP_IN;
            cl->read_op_pos = 0;
            cl->read_op_size = 0;
            cl->read_op_inline_decrypt_in = 0;
            cl->read_op_inline_decrypt_pos = (size_t)-1;
            if (cl->proto_csum_status == MSGR_CSUM_FULL || cl->proto_csum_status == MSGR_CSUM_PAYLOAD)
            {
                if (!cl->read_csum_state)
                    cl->read_csum_state = XXH3_createState();
                XXH3_64bits_reset(cl->read_csum_state);
            }
        }
        if (cl->read_op_pos < OSD_PACKET_SIZE)
        {
            int len = OSD_PACKET_SIZE - cl->read_op_pos;
            if (len > bufsize-done)
                len = bufsize-done;
            memcpy(cl->read_op->req.buf + cl->read_op_pos, curbuf+done, len);
            done += len;
            cl->read_op_pos += len;
            if (cl->read_op_pos < OSD_PACKET_SIZE)
                return true;
            if (!handle_hdr(cl))
            {
                stop_client(cl->client_id);
                return false;
            }
        }
        if (!op_copy_from(cl, curbuf, bufsize, done))
        {
            return false;
        }
    }
    return true;
}

bool osd_messenger_t::handle_hdr(osd_client_t *cl)
{
    if (cl->proto_csum_status == MSGR_CSUM_FULL)
    {
        XXH3_64bits_update(cl->read_csum_state, cl->read_op->req.buf, OSD_PACKET_SIZE);
    }
    if (cl->read_op->req.hdr.magic == SECONDARY_OSD_REPLY_MAGIC)
    {
        auto req_it = cl->sent_ops.find(cl->read_op->req.hdr.id);
        if (req_it == cl->sent_ops.end())
        {
            // Command out of sync. Drop connection
            fprintf(stderr, "Client %ju command out of sync: id %ju\n", cl->client_id, cl->read_op->req.hdr.id);
            return false;
        }
        osd_op_t *op = req_it->second;
        memcpy(op->reply.buf, cl->read_op->req.buf, OSD_PACKET_SIZE);
        if (!allocate_reply_buffers(cl, op))
        {
            return false;
        }
        cl->sent_ops.erase(req_it);
        delete cl->read_op;
        cl->read_op = op;
    }
    else if (cl->read_op->req.hdr.magic == SECONDARY_OSD_OP_MAGIC)
    {
        if (cl->check_sequencing)
        {
            if (cl->read_op->req.hdr.id != cl->read_op_id)
            {
                fprintf(stderr, "Warning: operation sequencing is broken on client %d: expected num %ju, got %ju, stopping client\n", cl->peer_fd, cl->read_op_id, cl->read_op->req.hdr.id);
                return false;
            }
            cl->read_op_id++;
        }
        if (!allocate_op_buffers(cl))
        {
            return false;
        }
    }
    else
    {
        fprintf(stderr, "Received garbage: magic=%jx id=%ju opcode=%jx from client %ju\n", cl->read_op->req.hdr.magic, cl->read_op->req.hdr.id, cl->read_op->req.hdr.opcode, cl->client_id);
        return false;
    }
    return true;
}

bool osd_messenger_t::allocate_op_buffers(osd_client_t *cl)
{
    osd_op_t *cur_op = cl->read_op;
    cl->read_op_size = 0;
    if (cur_op->req.hdr.opcode == OSD_OP_SEC_WRITE ||
        cur_op->req.hdr.opcode == OSD_OP_SEC_WRITE_STABLE)
    {
        if (cur_op->req.sec_rw.attr_len > 0)
        {
            if (cur_op->req.sec_rw.attr_len > clean_entry_bitmap_size)
            {
                if (log_level > 1)
                {
                    fprintf(stderr, "Error: peer %ju secondary write request attr_len too large (%u > %u bytes), stopping\n", cl->client_id,
                        cur_op->req.sec_rw.attr_len, clean_entry_bitmap_size);
                }
                return false;
            }
            else if (cur_op->req.sec_rw.attr_len > sizeof(cur_op->bmp_data))
                cur_op->bitmap = cur_op->rmw_buf = malloc_or_die(cur_op->req.sec_rw.attr_len);
            else
                cur_op->bitmap = &cur_op->bmp_data;
        }
        if (cur_op->req.sec_rw.len > 0)
        {
            if (cur_op->req.sec_rw.len > bs_block_size)
            {
                if (log_level > 1)
                {
                    fprintf(stderr, "Error: peer %ju secondary write request size too large (%u > %u bytes), stopping\n", cl->client_id,
                        cur_op->req.sec_rw.len, bs_block_size);
                }
                return false;
            }
            cur_op->buf = memalign_or_die(MEM_ALIGNMENT, cur_op->req.sec_rw.len);
        }
        cl->read_op_size = cur_op->req.sec_rw.len + cur_op->req.sec_rw.attr_len;
    }
    else if (cur_op->req.hdr.opcode == OSD_OP_SEC_STABILIZE ||
        cur_op->req.hdr.opcode == OSD_OP_SEC_ROLLBACK)
    {
        if (cur_op->req.sec_stab.len > 0)
        {
            if (cur_op->req.sec_stab.len > MAX_SIMPLE_PAYLOAD_SIZE)
            {
                if (log_level > 1)
                {
                    fprintf(stderr, "Error: peer %ju stabilize request size too large (%lu > %u bytes), stopping\n", cl->client_id,
                        cur_op->req.sec_stab.len, MAX_SIMPLE_PAYLOAD_SIZE);
                }
                return false;
            }
            cur_op->buf = memalign_or_die(MEM_ALIGNMENT, cur_op->req.sec_stab.len);
        }
        cl->read_op_size = cur_op->req.sec_stab.len;
    }
    else if (cur_op->req.hdr.opcode == OSD_OP_SEC_READ_BMP)
    {
        if (cur_op->req.sec_read_bmp.len > 0)
        {
            if (cur_op->req.sec_read_bmp.len > MAX_SIMPLE_PAYLOAD_SIZE)
            {
                if (log_level > 1)
                {
                    fprintf(stderr, "Error: peer %ju sec_read_bmp request size too large (%lu > %u bytes), stopping\n", cl->client_id,
                        cur_op->req.sec_read_bmp.len, MAX_SIMPLE_PAYLOAD_SIZE);
                }
                return false;
            }
            cur_op->buf = memalign_or_die(MEM_ALIGNMENT, cur_op->req.sec_read_bmp.len);
        }
        cl->read_op_size = cur_op->req.sec_read_bmp.len;
    }
    else if (cur_op->req.hdr.opcode == OSD_OP_WRITE)
    {
        if (cur_op->req.rw.len > 0)
        {
            if (cur_op->req.rw.len > max_write_request_size)
            {
                if (log_level > 1)
                {
                    fprintf(stderr, "Error: peer %ju write request size too large (%u > %u bytes), stopping\n", cl->client_id,
                        cur_op->req.rw.len, max_write_request_size);
                }
                return false;
            }
            cur_op->buf = memalign_or_die(MEM_ALIGNMENT, cur_op->req.rw.len);
        }
        cl->read_op_size = cur_op->req.rw.len;
    }
    else if (cur_op->req.hdr.opcode == OSD_OP_SHOW_CONFIG)
    {
        if (cur_op->req.show_conf.json_len > 0)
        {
            if (cur_op->req.show_conf.json_len > MAX_SIMPLE_PAYLOAD_SIZE)
            {
                if (log_level > 1)
                {
                    fprintf(stderr, "Error: peer %ju show_config request length too large (%lu > %u bytes), stopping\n", cl->client_id,
                        cur_op->req.show_conf.json_len, MAX_SIMPLE_PAYLOAD_SIZE);
                }
                return false;
            }
            cur_op->buf = malloc_or_die(cur_op->req.show_conf.json_len+1);
            ((uint8_t*)cur_op->buf)[cur_op->req.show_conf.json_len] = 0;
        }
        cl->read_op_size = cur_op->req.show_conf.json_len;
    }
    if (cl->proto_csum_status == MSGR_CSUM_FULL ||
        cl->read_op_size > 0 && cl->proto_csum_status == MSGR_CSUM_PAYLOAD)
    {
        cl->read_op_size += 8;
    }
    return true;
}

bool osd_messenger_t::allocate_reply_buffers(osd_client_t *cl, osd_op_t *op)
{
    cl->read_op_size = 0;
    if (op->reply.hdr.opcode == OSD_OP_SEC_READ || op->reply.hdr.opcode == OSD_OP_READ)
    {
        // Read data. In this case we assume that the buffer is preallocated by the caller (!)
        uint32_t bmp_len = (op->reply.hdr.opcode == OSD_OP_SEC_READ ? op->reply.sec_rw.attr_len : op->reply.rw.bitmap_len);
        uint32_t expected_size = (op->reply.hdr.opcode == OSD_OP_SEC_READ ? op->req.sec_rw.len : op->req.rw.len);
        if (op->reply.hdr.retval >= 0 && (op->reply.hdr.retval != expected_size || bmp_len > op->bitmap_len))
        {
            // Check reply length to not overflow the buffer
            fprintf(stderr, "Error: peer %ju read reply has incorrect length: expected %u+%u, got %jd+%u, stopping\n",
                cl->client_id, expected_size, op->bitmap_len, op->reply.hdr.retval, bmp_len);
            return false;
        }
        if (op->reply.hdr.retval >= 0 && bmp_len > 0)
        {
            assert(op->bitmap);
            cl->read_op_size += bmp_len;
        }
        if (op->reply.hdr.retval > 0)
        {
            assert(op->iov.count > 0);
            cl->read_op_size += op->reply.hdr.retval;
        }
    }
    else if (op->reply.hdr.opcode == OSD_OP_SEC_LIST && op->reply.hdr.retval > 0)
    {
        assert(!op->iov.count);
        cl->read_op_size = sizeof(obj_ver_id) * op->reply.hdr.retval;
        if (cl->read_op_size/sizeof(obj_ver_id) != op->reply.hdr.retval) // check for overflow
        {
            fprintf(stderr, "Error: peer %ju object list length is too large: %jx objects, stopping\n",
                cl->client_id, op->reply.hdr.retval);
            return false;
        }
        op->buf = memalign_or_die(MEM_ALIGNMENT, cl->read_op_size);
    }
    else if (op->reply.hdr.opcode == OSD_OP_SEC_READ_BMP && op->reply.hdr.retval > 0)
    {
        assert(!op->iov.count);
        uint64_t expected_retval = (op->req.sec_read_bmp.len / sizeof(obj_ver_id) * (8 + clean_entry_bitmap_size));
        if (op->reply.hdr.retval != expected_retval)
        {
            fprintf(stderr, "Error: peer %ju bitmap read reply has incorrect length: expected %jx, got %jx, stopping\n",
                cl->client_id, expected_retval, op->reply.hdr.retval);
            return false;
        }
        cl->read_op_size = op->reply.hdr.retval;
        free(op->buf);
        op->buf = memalign_or_die(MEM_ALIGNMENT, cl->read_op_size);
    }
    else if (op->reply.hdr.opcode == OSD_OP_SHOW_CONFIG && op->reply.hdr.retval > 0)
    {
        cl->read_op_size = op->reply.hdr.retval;
        if (cl->read_op_size > MAX_SIMPLE_PAYLOAD_SIZE)
        {
            fprintf(stderr, "Error: peer %ju show_config response length too large (%ju > %u bytes), stopping\n",
                cl->client_id, op->reply.hdr.retval, MAX_SIMPLE_PAYLOAD_SIZE);
            return false;
        }
        free(op->buf);
        op->buf = malloc_or_die(op->reply.hdr.retval);
    }
    else if (op->reply.hdr.opcode == OSD_OP_DESCRIBE && op->reply.describe.result_bytes > 0)
    {
        cl->read_op_size = op->reply.describe.result_bytes;
        free(op->buf);
        op->buf = malloc_or_die(op->reply.describe.result_bytes);
    }
    if (cl->proto_csum_status == MSGR_CSUM_FULL ||
        cl->read_op_size > 0 && cl->proto_csum_status == MSGR_CSUM_PAYLOAD)
    {
        cl->read_op_size += 8;
    }
    return true;
}

bool osd_messenger_t::op_copy_from(osd_client_t *cl, uint8_t *src, size_t src_len, size_t & done)
{
    osd_op_t *op = cl->read_op;
    size_t from = cl->read_op_pos-OSD_PACKET_SIZE;
    auto op_read_buf = [&](uint8_t *dst, size_t dst_len, bool skip_csum = false)
    {
        if (from < dst_len)
        {
            size_t n = dst_len-from;
            if (n > src_len-done)
                n = src_len-done;
            if (cl->read_csum_state && !skip_csum)
            {
                // it may be skipped if !dst but checksum is still calculated
                XXH3_64bits_update(cl->read_csum_state, src+done, n);
            }
            if (dst)
                memcpy(dst+from, src+done, n);
            else
                assert(!this->osd_num); // NULL buffers are only used by clients
            done += n;
            cl->read_op_pos += n;
            from += n;
            if (from < dst_len)
                return false;
            from = 0;
        }
        else
            from -= dst_len;
        return true;
    };
    if (op->op_type == OSD_OP_IN)
    {
        if (op->req.hdr.opcode == OSD_OP_SEC_WRITE ||
            op->req.hdr.opcode == OSD_OP_SEC_WRITE_STABLE)
        {
            if (!op_read_buf((uint8_t*)op->bitmap, op->req.sec_rw.attr_len))
                return true;
            if (!op_read_buf((uint8_t*)op->buf, op->req.sec_rw.len))
                return true;
        }
        else if (op->req.hdr.opcode == OSD_OP_SEC_STABILIZE ||
            op->req.hdr.opcode == OSD_OP_SEC_ROLLBACK)
        {
            if (!op_read_buf((uint8_t*)op->buf, op->req.sec_stab.len))
                return true;
        }
        else if (op->req.hdr.opcode == OSD_OP_SEC_READ_BMP)
        {
            if (!op_read_buf((uint8_t*)op->buf, op->req.sec_read_bmp.len))
                return true;
        }
        else if (op->req.hdr.opcode == OSD_OP_WRITE)
        {
            if (!op_read_buf((uint8_t*)op->buf, op->req.rw.len))
                return true;
        }
        else if (op->req.hdr.opcode == OSD_OP_SHOW_CONFIG)
        {
            if (!op_read_buf((uint8_t*)op->buf, op->req.show_conf.json_len))
                return true;
        }
    }
    else
    {
        if (op->reply.hdr.opcode == OSD_OP_SEC_READ)
        {
            if (op->reply.sec_rw.attr_len > 0)
            {
                if (!op_read_buf((uint8_t*)op->bitmap, op->reply.sec_rw.attr_len))
                    return true;
            }
            if (op->reply.hdr.retval > 0)
            {
                for (int i = 0; i < op->iov.count; i++)
                    if (!op_read_buf((uint8_t*)op->iov.buf[i].iov_base, op->iov.buf[i].iov_len))
                        return true;
            }
        }
        else if (op->reply.hdr.opcode == OSD_OP_READ)
        {
            if (op->reply.rw.bitmap_len > 0)
            {
                if (!op_read_buf((uint8_t*)op->bitmap, op->reply.rw.bitmap_len))
                    return true;
            }
            if (op->reply.hdr.retval > 0)
            {
                if (op->enc)
                {
                    if (!op_decrypted_copy_data_from(cl, src, src_len, from, done))
                        return true;
                }
                else
                {
                    for (int i = 0; i < op->iov.count; i++)
                        if (!op_read_buf((uint8_t*)op->iov.buf[i].iov_base, op->iov.buf[i].iov_len))
                            return true;
                }
            }
        }
        else if (op->reply.hdr.opcode == OSD_OP_SEC_LIST && op->reply.hdr.retval > 0)
        {
            if (!op_read_buf((uint8_t*)op->buf, sizeof(obj_ver_id) * op->reply.hdr.retval))
                return true;
        }
        else if ((op->reply.hdr.opcode == OSD_OP_SEC_READ_BMP ||
            op->reply.hdr.opcode == OSD_OP_SHOW_CONFIG) && op->reply.hdr.retval > 0)
        {
            if (!op_read_buf((uint8_t*)op->buf, op->reply.hdr.retval))
                return true;
        }
        else if (op->reply.hdr.opcode == OSD_OP_DESCRIBE && op->reply.describe.result_bytes > 0)
        {
            if (!op_read_buf((uint8_t*)op->buf, op->reply.describe.result_bytes))
                return true;
        }
    }
    if (cl->proto_csum_status == MSGR_CSUM_FULL ||
        cl->read_op_size > 0 && cl->proto_csum_status == MSGR_CSUM_PAYLOAD)
    {
        if (!op_read_buf((uint8_t*)&op->csum, 8, true))
            return true;
    }
    return handle_finished_op(cl);
}

void osd_messenger_t::op_get_read_buffers(osd_client_t *cl, std::vector<iovec> & lst)
{
    osd_op_t *op = cl->read_op;
    size_t from = cl->read_op_pos-OSD_PACKET_SIZE;
    size_t done = 0;
    auto op_read_buf = [&](uint8_t *dst, size_t dst_len)
    {
        if (lst.size() >= IOV_MAX)
            return false;
        if (from < dst_len)
        {
            lst.push_back((iovec){ .iov_base = dst+from, .iov_len = dst_len-from });
            cl->read_op_pos += dst_len-from;
            done += dst_len-from;
            from = 0;
        }
        else
            from -= dst_len;
        return true;
    };
    if (op->op_type == OSD_OP_IN)
    {
        if (op->req.hdr.opcode == OSD_OP_SEC_WRITE ||
            op->req.hdr.opcode == OSD_OP_SEC_WRITE_STABLE)
        {
            if (!op_read_buf((uint8_t*)op->bitmap, op->req.sec_rw.attr_len))
                return;
            if (!op_read_buf((uint8_t*)op->buf, op->req.sec_rw.len))
                return;
        }
        else if (op->req.hdr.opcode == OSD_OP_SEC_STABILIZE ||
            op->req.hdr.opcode == OSD_OP_SEC_ROLLBACK)
        {
            if (!op_read_buf((uint8_t*)op->buf, op->req.sec_stab.len))
                return;
        }
        else if (op->req.hdr.opcode == OSD_OP_SEC_READ_BMP)
        {
            if (!op_read_buf((uint8_t*)op->buf, op->req.sec_read_bmp.len))
                return;
        }
        else if (op->req.hdr.opcode == OSD_OP_WRITE)
        {
            if (!op_read_buf((uint8_t*)op->buf, op->req.rw.len))
                return;
        }
        else if (op->req.hdr.opcode == OSD_OP_SHOW_CONFIG)
        {
            if (!op_read_buf((uint8_t*)op->buf, op->req.show_conf.json_len))
                return;
        }
    }
    else
    {
        if (op->reply.hdr.opcode == OSD_OP_SEC_READ)
        {
            if (op->reply.sec_rw.attr_len > 0)
            {
                if (!op_read_buf((uint8_t*)op->bitmap, op->reply.sec_rw.attr_len))
                    return;
            }
            if (op->reply.hdr.retval > 0)
            {
                for (int i = 0; i < op->iov.count; i++)
                    if (!op_read_buf((uint8_t*)op->iov.buf[i].iov_base, op->iov.buf[i].iov_len))
                        return;
            }
        }
        else if (op->reply.hdr.opcode == OSD_OP_READ)
        {
            if (op->reply.rw.bitmap_len > 0)
            {
                if (!op_read_buf((uint8_t*)op->bitmap, op->reply.rw.bitmap_len))
                    return;
            }
            if (op->reply.hdr.retval > 0)
            {
                if (op->enc)
                {
                    cl->read_op_inline_decrypt_pos = cl->read_op_pos;
                    cl->read_op_pos = cl->read_op_inline_decrypt_in + OSD_PACKET_SIZE + op->reply.rw.bitmap_len;
                    from = cl->read_op_inline_decrypt_in;
                }
                for (int i = 0; i < op->iov.count; i++)
                {
                    if (!op->iov.buf[i].iov_base)
                    {
                        // When we recvmsg directly into the operation without copying,
                        // we need some place for all buffers, so we allocate temporary
                        // buffers for all skipped parts
                        op_alloc_temp_buffers(op, i);
                    }
                    if (!op_read_buf((uint8_t*)op->iov.buf[i].iov_base, op->iov.buf[i].iov_len))
                        return;
                }
            }
        }
        else if (op->reply.hdr.opcode == OSD_OP_SEC_LIST && op->reply.hdr.retval > 0)
        {
            if (!op_read_buf((uint8_t*)op->buf, sizeof(obj_ver_id) * op->reply.hdr.retval))
                return;
        }
        else if ((op->reply.hdr.opcode == OSD_OP_SEC_READ_BMP ||
            op->reply.hdr.opcode == OSD_OP_SHOW_CONFIG) && op->reply.hdr.retval > 0)
        {
            if (!op_read_buf((uint8_t*)op->buf, op->reply.hdr.retval))
                return;
        }
        else if (op->reply.hdr.opcode == OSD_OP_DESCRIBE && op->reply.describe.result_bytes > 0)
        {
            if (!op_read_buf((uint8_t*)op->buf, op->reply.describe.result_bytes))
                return;
        }
    }
    if (cl->proto_csum_status == MSGR_CSUM_FULL ||
        cl->read_op_size > 0 && cl->proto_csum_status == MSGR_CSUM_PAYLOAD)
    {
        if (!op_read_buf((uint8_t*)&op->csum, 8))
            return;
    }
}

void osd_messenger_t::op_alloc_temp_buffers(osd_op_t *op, int i)
{
    size_t total_skip = 0;
    for (int j = i; j < op->iov.count; j++)
    {
        if (!op->iov.buf[j].iov_base)
        {
            total_skip += op->iov.buf[j].iov_len;
        }
    }
    assert(total_skip);
    assert(!op->rmw_buf);
    op->rmw_buf = malloc_or_die(total_skip);
    total_skip = 0;
    for (int j = i; j < op->iov.count; j++)
    {
        if (!op->iov.buf[j].iov_base)
        {
            op->iov.buf[j].iov_base = (uint8_t*)op->rmw_buf + total_skip;
            total_skip += op->iov.buf[j].iov_len;
        }
    }
}

bool osd_messenger_t::handle_finished_op(osd_client_t *cl)
{
    osd_op_t *op = cl->read_op;
    if (cl->proto_csum_status == MSGR_CSUM_FULL ||
        cl->read_op_size > 0 && cl->proto_csum_status == MSGR_CSUM_PAYLOAD)
    {
        uint64_t real_csum = XXH3_64bits_digest(cl->read_csum_state);
        if (op->csum != real_csum)
        {
            fprintf(stderr, "Client %ju checksum mismatch for received data: expected %016jx, got %016jx, disconnecting client\n",
                cl->client_id, op->csum, real_csum);
            stop_client(cl->client_id);
            return false;
        }
    }
    if (op->op_type == OSD_OP_IN)
    {
        // Operation is ready
        cl->received_ops.push_back(op);
    }
    else
    {
        // Inline decryption
        if (cl->read_op_inline_decrypt_pos != (size_t)-1)
        {
            op_decrypt_inline(cl);
            cl->read_op_inline_decrypt_pos = (size_t)-1;
        }
        // Measure subop (outbound op) latency
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
    }
    set_immediate_ops.push_back(op);
    cl->read_op = NULL;
    return true;
}
