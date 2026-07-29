// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#define _XOPEN_SOURCE
#include <limits.h>
#include "messenger.h"
#include "msgr_iothread.h"
#include "openssl_util.h"

#include <openssl/evp.h>
#include <openssl/err.h>

#define RDR_GCM     1
#define RDR_XTS     2
#define RDR_NO_CSUM 4

class msgr_op_reader_t
{
public:
    virtual bool read(uint8_t *dst, size_t dst_len, int flags = 0) = 0;
    virtual bool finish() = 0;
};

class copy_op_reader_t: public msgr_op_reader_t
{
protected:
    osd_messenger_t* msgr;
    osd_client_t* cl;
    size_t from;

    uint8_t *curbuf;
    size_t bufsize;
    size_t done;

public:
    copy_op_reader_t(osd_messenger_t* msgr, osd_client_t* cl, uint8_t *curbuf, size_t bufsize):
        msgr(msgr), cl(cl), from(cl->read_op_pos), curbuf(curbuf), bufsize(bufsize), done(0)
    {}

    void reset()
    {
        from = cl->read_op_pos;
    }

    bool read(uint8_t *dst, size_t dst_len, int flags = 0) override
    {
        if (from >= dst_len)
        {
            from -= dst_len;
            return true;
        }
        if (flags & RDR_XTS)
        {
            msgr->op_decrypted_copy_buf(cl, curbuf, bufsize, dst, dst_len, from, done);
        }
        else
        {
            size_t n = dst_len-from;
            if (n > bufsize-done)
                n = bufsize-done;
            if (!n)
                return false;
            if (cl->read_csum_state && !(flags & RDR_NO_CSUM))
            {
                // data may be skipped if dst == NULL but checksum is still calculated
                XXH3_64bits_update(cl->read_csum_state, curbuf+done, n);
            }
            // Here, dst == NULL is allowed
            if (dst != NULL)
                memcpy(dst+from, curbuf+done, n);
            done += n;
            cl->read_op_pos += n;
            from += n;
        }
        if (from < dst_len)
            return false;
        from = 0;
        return true;
    }

    bool finish() override
    {
        return true;
    }

    size_t get_done()
    {
        return done;
    }
};

class gcm_op_reader_t: public msgr_op_reader_t
{
    osd_messenger_t* msgr;
    osd_client_t* cl;
    size_t from;

    uint8_t *curbuf;
    size_t bufsize;
    size_t done;

public:
    gcm_op_reader_t(osd_messenger_t* msgr, osd_client_t* cl, uint8_t *curbuf, size_t bufsize):
        msgr(msgr), cl(cl), from(cl->read_op_pos), curbuf(curbuf), bufsize(bufsize), done(0)
    {
    }

    void reset()
    {
        from = cl->read_op_pos;
        if (!cl->dec_ctx)
        {
            if (msgr->decrypt_gcm_pool.size())
            {
                cl->dec_ctx = msgr->decrypt_gcm_pool.back();
                msgr->decrypt_gcm_pool.pop_back();
            }
            else
            {
#ifdef WITH_ISAL_CRYPTO
                cl->dec_ctx = (isal_gcm_context_data*)malloc_or_die(sizeof(isal_gcm_context_data));
#else
                cl->dec_ctx = EVP_CIPHER_CTX_new();
                assert(cl->dec_ctx);
                int r = EVP_DecryptInit_ex(cl->dec_ctx, EVP_aes_256_gcm(), NULL, NULL, NULL);
                if (r != 1)
                {
                    fprintf(stderr, "DecryptInit error: ");
                    ERR_print_errors_fp(stderr);
                    abort();
                }
#endif
            }
        }
        if (cl->peer_iv_ctr >= AES_256_GCM_MAX_IV_CTR)
        {
            // Rotate key every 2^32 messages
            bool ok = msgr->derive_aes_keys(cl, false, true);
            assert(ok);
        }
#ifdef WITH_ISAL_CRYPTO
        int r = isal_aes_gcm_init_256(&cl->peer_key_isal, cl->dec_ctx, cl->peer_key.data() + AES_256_GCM_KEY_SIZE, NULL, 0);
        if (r != 0)
        {
            fprintf(stderr, "isal_aes_gcm_init_256 error %d\n", r);
            abort();
        }
#else
        int r = EVP_DecryptInit_ex(cl->dec_ctx, NULL, NULL, cl->peer_key.data(), cl->peer_key.data() + AES_256_GCM_KEY_SIZE);
        if (r != 1)
        {
            fprintf(stderr, "DecryptInit error: ");
            ERR_print_errors_fp(stderr);
            abort();
        }
#endif
        // Increase IV
        cl->peer_iv_ctr++;
        (*(uint64_t*)(cl->peer_key.data() + AES_256_GCM_KEY_SIZE))++;
    }

    bool read(uint8_t *dst, size_t dst_len, int flags) override
    {
        if (from >= dst_len)
        {
            // Skip
            from -= dst_len;
            return true;
        }
        if (done >= bufsize)
            return false;
        size_t n = dst_len-from;
        if (n > bufsize-done)
            n = bufsize-done;
        if (flags & RDR_XTS)
        {
            msgr->op_decrypted_copy_buf(cl, curbuf, bufsize, dst, dst_len, from, done);
            n = 0;
        }
        else if (flags & RDR_GCM)
        {
            // Here, dst == NULL is not allowed
            assert(dst != NULL);
#ifdef WITH_ISAL_CRYPTO
            int r = isal_aes_gcm_dec_256_update(&cl->peer_key_isal, cl->dec_ctx, dst+from, curbuf+done, n);
            assert(!r);
#else
            int actual_out;
            if (EVP_DecryptUpdate(cl->dec_ctx, dst+from, &actual_out, curbuf+done, n) != 1)
            {
                fprintf(stderr, "DecryptUpdate error: ");
                ERR_print_errors_fp(stderr);
                abort();
            }
            assert(actual_out == n);
#endif
            if (cl->read_csum_state && !(flags & RDR_NO_CSUM))
            {
                XXH3_64bits_update(cl->read_csum_state, dst+from, n);
            }
            done += n;
        }
        else
        {
            if (cl->read_csum_state && !(flags & RDR_NO_CSUM))
            {
                // data may be skipped if dst == NULL but checksum is still calculated
                XXH3_64bits_update(cl->read_csum_state, curbuf+done, n);
            }
            // Here, dst == NULL is allowed
            if (dst != NULL)
                memcpy(dst+from, curbuf+done, n);
            done += n;
        }
        cl->read_op_pos += n;
        from += n;
        if (from < dst_len)
        {
            return false;
        }
        from = 0;
        return true;
    }

    bool finish() override
    {
        if (cl->dec_tag_size+bufsize-done < 16)
        {
            // Buffer part of the tag
            memcpy(cl->dec_tag+cl->dec_tag_size, curbuf+done, bufsize-done);
            cl->dec_tag_size += bufsize-done;
            done = bufsize;
            return false;
        }
#ifdef WITH_ISAL_CRYPTO
        uint8_t calc_tag[16];
        int r = isal_aes_gcm_dec_256_finalize(&cl->peer_key_isal, cl->dec_ctx, calc_tag, 16);
        assert(r == 0);
        if (cl->dec_tag_size > 0)
        {
            // Tag is partially buffered, append to it and compare
            memcpy(cl->dec_tag+cl->dec_tag_size, curbuf+done, 16-cl->dec_tag_size);
            done += 16-cl->dec_tag_size;
            r = !memcmp(calc_tag, cl->dec_tag, 16);
        }
        else
        {
            // Compare the full tag directly from the source buffer
            r = !memcmp(calc_tag, curbuf+done, 16);
            done += 16;
        }
#else
        int r;
        if (cl->dec_tag_size > 0)
        {
            // Tag is partially buffered, append to it and use it from there
            memcpy(cl->dec_tag+cl->dec_tag_size, curbuf+done, 16-cl->dec_tag_size);
            r = EVP_CIPHER_CTX_ctrl(cl->dec_ctx, EVP_CTRL_GCM_SET_TAG, 16, cl->dec_tag);
            assert(r == 1);
            done += 16-cl->dec_tag_size;
        }
        else
        {
            // Take full tag directly from the source buffer
            r = EVP_CIPHER_CTX_ctrl(cl->dec_ctx, EVP_CTRL_GCM_SET_TAG, 16, curbuf+done);
            assert(r == 1);
            done += 16;
        }
        int len = 0;
        r = EVP_DecryptFinal_ex(cl->dec_ctx, NULL, &len);
        assert(len == 0);
#endif
        if (r != 1)
        {
            fprintf(stderr, "Client %ju AES-GCM decryption failed\n", cl->client_id);
            cl->io_error = true;
            return false;
        }
        if (msgr->decrypt_gcm_pool.size() < msgr->max_cipher_pool_size)
            msgr->decrypt_gcm_pool.push_back(cl->dec_ctx);
        else
        {
#ifdef WITH_ISAL_CRYPTO
            free(cl->dec_ctx);
#else
            EVP_CIPHER_CTX_free(cl->dec_ctx);
#endif
        }
        cl->dec_ctx = NULL;
        cl->dec_tag_size = 0;
        return true;
    }

    size_t get_done()
    {
        return done;
    }
};

class get_op_reader_t: public msgr_op_reader_t
{
    osd_client_t* cl;
    size_t from;
    bool mpos;

public:
    get_op_reader_t(osd_messenger_t* msgr, osd_client_t* cl):
        cl(cl), from(cl->read_op_pos), mpos(false)
    {
        if (cl->read_op->op_type == OSD_OP_OUT &&
            cl->read_op->reply.hdr.opcode == OSD_OP_READ &&
            cl->read_op->reply.hdr.retval > 0)
        {
            // When we recvmsg directly into the operation without copying,
            // we need some place for all buffers, so we allocate temporary
            // buffers for all skipped parts
            alloc_temp_buffers(cl->read_op);
        }
    }

    void alloc_temp_buffers(osd_op_t *op)
    {
        size_t total_skip = 0;
        for (int j = 0; j < op->iov.count; j++)
        {
            if (!op->iov.buf[j].iov_base)
            {
                total_skip += op->iov.buf[j].iov_len;
            }
        }
        if (!total_skip)
        {
            return;
        }
        assert(!op->rmw_buf);
        op->rmw_buf = malloc_or_die(total_skip);
        total_skip = 0;
        for (int j = 0; j < op->iov.count; j++)
        {
            if (!op->iov.buf[j].iov_base)
            {
                op->iov.buf[j].iov_base = (uint8_t*)op->rmw_buf + total_skip;
                total_skip += op->iov.buf[j].iov_len;
            }
        }
    }

    bool read(uint8_t *dst, size_t dst_len, int flags) override
    {
        if (from >= dst_len)
        {
            // Skip
            from -= dst_len;
            return true;
        }
        if ((flags & RDR_GCM) && cl->gcm_enabled)
        {
            // Can't inplace read encrypted data
            return false;
        }
        if (cl->recv_list.size() >= IOV_MAX)
        {
            return false;
        }
        if ((flags & RDR_XTS) && cl->read_op->enc && !mpos)
        {
            mpos = true;
            cl->read_op_inline_decrypt_pos = cl->read_op_pos;
            cl->read_op_pos = cl->read_op_inline_decrypt_in + OSD_PACKET_SIZE + cl->read_op->reply.rw.bitmap_len;
            from = cl->read_op_inline_decrypt_in;
        }
        size_t n = dst_len-from;
        cl->recv_list.push_back((iovec){ dst+from, n });
        cl->recv_flags.push_back(flags);
        cl->read_op_pos += n;
        from = 0;
        return true;
    }

    bool finish() override
    {
        if (cl->gcm_enabled)
            return false;
        return true;
    }
};

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
        if (cl->read_op && cl->read_op_pos >= OSD_PACKET_SIZE && cl->read_op_size-(cl->read_op_pos-OSD_PACKET_SIZE) >= receive_buffer_size)
        {
            get_op_reader_t rdr(this, cl);
            if (!op_read_from(cl, rdr))
            {
                if (cl->io_error)
                {
                    stop_client(cl->client_id);
                    continue;
                }
            }
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
        if (!use_sync_send_recv)
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
            int result = recvmsg(cl->peer_fd, &cl->read_msg, cl->recv_list.size() ? MSG_WAITALL : 0);
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
                    !(cl->recv_flags[i] & RDR_NO_CSUM))
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
            cl->recv_flags.erase(cl->recv_flags.begin(), cl->recv_flags.begin()+i);
            if (!handle_finished_op(cl))
            {
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
    if (cl->gcm_enabled)
    {
        if (cl->hs)
        {
            ssize_t done = cl->hs->handle(curbuf, bufsize);
            if (done < 0)
            {
                fprintf(stderr, "Client %ju handshake failed: %s\n", cl->client_id, cl->hs->get_error().c_str());
                stop_client(cl->client_id);
                return false;
            }
            if (cl->hs->done() && !derive_aes_keys(cl, true, true))
            {
                stop_client(cl->client_id);
                return false;
            }
            curbuf += done;
            bufsize -= done;
            if (cl->hs->out_size())
            {
                if (cl->write_state == 0)
                {
                    cl->write_state = CL_WRITE_READY;
                    write_ready_clients.push_back(cl->client_id);
                }
            }
            if (cl->hs->done() && !cl->hs->out_size())
            {
                // Delete hs when done and nothing to send
                delete cl->hs;
                cl->hs = NULL;
            }
            else
            {
                if (done < bufsize)
                {
                    fprintf(stderr, "Client %ju extra data after handshake\n", cl->client_id);
                    stop_client(cl->client_id);
                    return false;
                }
                return true;
            }
        }
        return handle_buffer_with<gcm_op_reader_t>(cl, curbuf, bufsize);
    }
    return handle_buffer_with<copy_op_reader_t>(cl, curbuf, bufsize);
}

template<typename T>
bool osd_messenger_t::handle_buffer_with(osd_client_t *cl, uint8_t *curbuf, size_t bufsize)
{
    T rdr(this, cl, curbuf, bufsize);
    // Reset OSD ping state
    cl->ping_time_remaining = 0;
    cl->idle_time_remaining = osd_idle_timeout;
    // Compose operation(s) from the buffer
    while (true)
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
            rdr.reset();
        }
        if (!cl->read_op_pos && (cl->proto_csum_status == MSGR_CSUM_FULL || cl->proto_csum_status == MSGR_CSUM_PAYLOAD))
        {
            if (!cl->read_csum_state)
                cl->read_csum_state = XXH3_createState();
            if (cl->peer_key.size() == AES_256_GCM_KEY_SIZE + AES_256_GCM_IV_SIZE + XXH_SECRET_DEFAULT_SIZE)
                XXH3_64bits_reset_withSecret(cl->read_csum_state, cl->peer_key.data() + AES_256_GCM_KEY_SIZE + AES_256_GCM_IV_SIZE, XXH_SECRET_DEFAULT_SIZE);
            else
                XXH3_64bits_reset(cl->read_csum_state);
        }
        if (!op_read_from(cl, rdr) || !handle_finished_op(cl))
        {
            if (cl->io_error)
            {
                stop_client(cl->client_id);
                return false;
            }
            break;
        }
    }
    assert(rdr.get_done() == bufsize);
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
    if (!osd_num)
    {
        if (log_level > 1)
            fprintf(stderr, "Error: operation received from an OSD peer %ju, stopping\n", cl->client_id);
        return false;
    }
    else if (cur_op->req.hdr.opcode == OSD_OP_SEC_WRITE ||
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
            cur_op->buf = malloc_or_die(cur_op->req.sec_stab.len);
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
            cur_op->buf = malloc_or_die(cur_op->req.sec_read_bmp.len);
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
        if (op->reply.hdr.retval >= 0 && (op->reply.hdr.retval != expected_size || bmp_len != op->bitmap_len))
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
        op->buf = malloc_or_die(cl->read_op_size);
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
        op->buf = malloc_or_die(cl->read_op_size);
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

bool osd_messenger_t::op_read_from(osd_client_t *cl, msgr_op_reader_t & rdr)
{
    osd_op_t *op = cl->read_op;
    bool hdr = (cl->read_op_pos < OSD_PACKET_SIZE);
    if (hdr || op->op_type == OSD_OP_IN)
    {
        if (!rdr.read(op->req.buf, OSD_PACKET_SIZE, RDR_GCM | (cl->proto_csum_status == MSGR_CSUM_PAYLOAD ? RDR_NO_CSUM : 0)))
            return false;
        if (hdr)
        {
            if (!handle_hdr(cl))
            {
                cl->io_error = true;
                return false;
            }
            op = cl->read_op;
            if (op->op_type == OSD_OP_OUT)
                goto switched_type;
        }
        if (op->req.hdr.opcode == OSD_OP_SEC_WRITE ||
            op->req.hdr.opcode == OSD_OP_SEC_WRITE_STABLE)
        {
            if (!rdr.read((uint8_t*)op->bitmap, op->req.sec_rw.attr_len, RDR_GCM))
                return false;
            if (!rdr.read((uint8_t*)op->buf, op->req.sec_rw.len, cl->proto_csum_status == MSGR_CSUM_GCM ? RDR_GCM : 0))
                return false;
        }
        else if (op->req.hdr.opcode == OSD_OP_SEC_STABILIZE ||
            op->req.hdr.opcode == OSD_OP_SEC_ROLLBACK)
        {
            if (!rdr.read((uint8_t*)op->buf, op->req.sec_stab.len, RDR_GCM))
                return false;
        }
        else if (op->req.hdr.opcode == OSD_OP_SEC_READ_BMP)
        {
            if (!rdr.read((uint8_t*)op->buf, op->req.sec_read_bmp.len, RDR_GCM))
                return false;
        }
        else if (op->req.hdr.opcode == OSD_OP_WRITE)
        {
            if (!rdr.read((uint8_t*)op->buf, op->req.rw.len, cl->proto_csum_status == MSGR_CSUM_GCM ? RDR_GCM : 0))
                return false;
        }
        else if (op->req.hdr.opcode == OSD_OP_SHOW_CONFIG)
        {
            if (!rdr.read((uint8_t*)op->buf, op->req.show_conf.json_len, RDR_GCM))
                return false;
        }
    }
    else
    {
        if (!rdr.read(op->reply.buf, OSD_PACKET_SIZE, RDR_GCM | (cl->proto_csum_status == MSGR_CSUM_PAYLOAD ? RDR_NO_CSUM : 0)))
            return false;
switched_type:
        if (op->reply.hdr.opcode == OSD_OP_SEC_READ)
        {
            if (op->reply.sec_rw.attr_len > 0)
            {
                if (!rdr.read((uint8_t*)op->bitmap, op->reply.sec_rw.attr_len, RDR_GCM))
                    return false;
            }
            if (op->reply.hdr.retval > 0)
            {
                for (int i = 0; i < op->iov.count; i++)
                    if (!rdr.read((uint8_t*)op->iov.buf[i].iov_base, op->iov.buf[i].iov_len, (cl->proto_csum_status == MSGR_CSUM_GCM ? RDR_GCM : 0)))
                        return false;
            }
        }
        else if (op->reply.hdr.opcode == OSD_OP_READ)
        {
            if (op->reply.rw.bitmap_len > 0)
            {
                if (!rdr.read((uint8_t*)op->bitmap, op->reply.rw.bitmap_len, RDR_GCM))
                    return false;
            }
            if (op->reply.hdr.retval > 0)
            {
                for (int i = 0; i < op->iov.count; i++)
                    if (!rdr.read((uint8_t*)op->iov.buf[i].iov_base, op->iov.buf[i].iov_len, (op->enc ? RDR_XTS : 0) | (cl->proto_csum_status == MSGR_CSUM_GCM ? RDR_GCM : 0)))
                        return false;
            }
        }
        else if (op->reply.hdr.opcode == OSD_OP_SEC_LIST && op->reply.hdr.retval > 0)
        {
            if (!rdr.read((uint8_t*)op->buf, sizeof(obj_ver_id) * op->reply.hdr.retval, RDR_GCM))
                return false;
        }
        else if ((op->reply.hdr.opcode == OSD_OP_SEC_READ_BMP ||
            op->reply.hdr.opcode == OSD_OP_SHOW_CONFIG) && op->reply.hdr.retval > 0)
        {
            if (!rdr.read((uint8_t*)op->buf, op->reply.hdr.retval, RDR_GCM))
                return false;
        }
        else if (op->reply.hdr.opcode == OSD_OP_DESCRIBE && op->reply.describe.result_bytes > 0)
        {
            if (!rdr.read((uint8_t*)op->buf, op->reply.describe.result_bytes, RDR_GCM))
                return false;
        }
    }
    if (cl->proto_csum_status == MSGR_CSUM_FULL ||
        cl->read_op_size > 0 && cl->proto_csum_status == MSGR_CSUM_PAYLOAD)
    {
        if (!rdr.read((uint8_t*)&op->csum, 8, RDR_GCM|RDR_NO_CSUM))
            return false;
    }
    if (!rdr.finish())
        return false;
    assert(cl->read_op_pos == cl->read_op_size+OSD_PACKET_SIZE);
    return true;
}

bool osd_messenger_t::handle_finished_op(osd_client_t *cl)
{
    if (cl->read_op_pos < cl->read_op_size+OSD_PACKET_SIZE)
    {
        return true;
    }
    osd_op_t *op = cl->read_op;
    if (cl->proto_csum_status == MSGR_CSUM_FULL ||
        cl->read_op_size > 0 && cl->proto_csum_status == MSGR_CSUM_PAYLOAD)
    {
        uint64_t real_csum = XXH3_64bits_digest(cl->read_csum_state);
        if (op->csum != real_csum)
        {
            fprintf(stderr, "Client %ju checksum mismatch for received data: expected %016jx, got %016jx, disconnecting client\n",
                cl->client_id, op->csum, real_csum);
            cl->io_error = true;
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
    op_decrypt_free(cl);
    set_immediate_ops.push_back(op);
    cl->read_op = NULL;
    return true;
}
