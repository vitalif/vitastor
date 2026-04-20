// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#define _XOPEN_SOURCE
#include <limits.h>
#include <sys/epoll.h>

#include "messenger.h"
#include "msgr_iothread.h"

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>

#define WR_TLS     1
#define WR_XTS     2
#define WR_NO_CSUM 4

class msgr_op_writer_t
{
public:
    virtual bool write(uint8_t *src, size_t src_len, int flags = 0) = 0;
    virtual bool finish() = 0;
};

class copy_op_writer_t: public msgr_op_writer_t
{
protected:
    osd_messenger_t* msgr;
    osd_client_t* cl;
    size_t from;

    uint8_t *curbuf;
    size_t bufsize;
    size_t done;

public:
    copy_op_writer_t(osd_messenger_t* msgr, osd_client_t* cl, uint8_t *curbuf, size_t bufsize):
        msgr(msgr), cl(cl), from(cl->write_op_pos), curbuf(curbuf), bufsize(bufsize), done(0)
    {}

    void reset()
    {
        from = cl->write_op_pos;
    }

    bool write(uint8_t *src, size_t src_len, int flags = 0) override
    {
        if (from >= src_len)
        {
            from -= src_len;
            return true;
        }
        if (flags & WR_XTS)
        {
            msgr->op_encrypted_copy_buf(cl, curbuf, bufsize, src, src_len, from, done);
        }
        else
        {
            size_t n = src_len-from;
            if (n > bufsize-done)
                n = bufsize-done;
            if (cl->write_csum_state && !(flags & WR_NO_CSUM))
                XXH3_64bits_update(cl->write_csum_state, src+from, n);
            memcpy(curbuf+done, src+from, n);
            done += n;
            cl->write_op_pos += n;
            from += n;
        }
        if (from < src_len)
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

class ssl_op_writer_t: public msgr_op_writer_t
{
    osd_messenger_t* msgr;
    osd_client_t* cl;
    size_t from;

    uint8_t *curbuf;
    size_t bufsize;
    size_t done;

public:
    ssl_op_writer_t(osd_messenger_t* msgr, osd_client_t* cl, uint8_t *curbuf, size_t bufsize):
        msgr(msgr), cl(cl), from(cl->write_op_pos), curbuf(curbuf), bufsize(bufsize), done(0)
    {
    }

    void reset()
    {
        from = cl->write_op_pos;
    }

    void flush_ssl()
    {
        if (!cl->ssl_handshake_done)
        {
            if (!msgr->ssl_do_handshake(cl))
                return;
        }
        _flush_ssl();
    }

    bool _flush_ssl()
    {
        int r = BIO_read(cl->read_from_ssl, curbuf+done, bufsize-done);
        if (r > 0)
            done += r;
        if (done >= bufsize)
        {
            // Check if we've sent all buffered TLS data
            // ...Because we can't return true from this->write() if we haven't
            char *bio_buf = NULL;
            size_t bio_sz = BIO_get_mem_data(cl->read_from_ssl, &bio_buf);
            if (bio_sz > 0)
            {
                cl->ssl_more_to_buffer = true;
                return false;
            }
        }
        return true;
    }

    static inline bool write_to_ssl(osd_client_t *cl, uint8_t *src, size_t src_len, int flags, size_t & from)
    {
        size_t n = src_len-from;
        int ok = SSL_write_ex(cl->ssl_cli, src+from, n, &n);
        if (ok)
        {
            if (cl->write_csum_state && !(flags & WR_NO_CSUM))
                XXH3_64bits_update(cl->write_csum_state, src+from, n);
            cl->write_op_pos += n;
            from += n;
        }
        else
        {
            ok = SSL_get_error(cl->ssl_cli, ok);
            if (ok == SSL_ERROR_ZERO_RETURN)
            {
                fprintf(stderr, "Client %ju TLS disconnected\n", cl->client_id);
                cl->io_error = true;
                return false;
            }
            else if (ok != SSL_ERROR_WANT_READ && ok != SSL_ERROR_WANT_WRITE)
            {
                fprintf(stderr, "Client %ju TLS write error: %s. Disconnecting client\n", cl->client_id, ERR_error_string(ERR_get_error(), NULL));
                cl->io_error = true;
                return false;
            }
        }
        return true;
    }

    bool write(uint8_t *src, size_t src_len, int flags) override
    {
        if (from >= src_len)
        {
            if (cl->ssl_more_to_buffer && !_flush_ssl())
                return false;
            from -= src_len;
            return true;
        }
        if (!(flags & WR_TLS) || !cl->ssl_cli)
        {
            if (flags & WR_XTS)
            {
                msgr->op_encrypted_copy_buf(cl, curbuf, bufsize, src, src_len, from, done);
            }
            else
            {
                size_t n = src_len-from;
                if (n > bufsize-done)
                    n = bufsize-done;
                if (cl->write_csum_state && !(flags & WR_NO_CSUM))
                    XXH3_64bits_update(cl->write_csum_state, src+from, n);
                memcpy(curbuf+done, src+from, n);
                done += n;
                cl->write_op_pos += n;
                from += n;
            }
        }
        else
        {
            if (!cl->ssl_handshake_done)
            {
                if (!msgr->ssl_do_handshake(cl))
                    return false;
            }
            if (cl->ssl_handshake_done)
            {
                if (!write_to_ssl(cl, src, src_len, flags, from))
                    return false;
            }
            if (!_flush_ssl())
                return false;
        }
        if (from < src_len)
            return false;
        from = 0;
        return true;
    }

    bool finish() override
    {
        return _flush_ssl();
    }

    size_t get_done()
    {
        return done;
    }
};

class gcm_op_writer_t: public msgr_op_writer_t
{
    osd_messenger_t* msgr;
    osd_client_t* cl;
    size_t from;

    uint8_t *curbuf;
    size_t bufsize;
    size_t done;

public:
    gcm_op_writer_t(osd_messenger_t* msgr, osd_client_t* cl, uint8_t *curbuf, size_t bufsize):
        msgr(msgr), cl(cl), from(cl->write_op_pos), curbuf(curbuf), bufsize(bufsize), done(0)
    {
    }

    void reset()
    {
        from = cl->write_op_pos;
        init_ctx(msgr, cl);
    }

    static void init_ctx(osd_messenger_t* msgr, osd_client_t *cl)
    {
        if (!cl->enc_ctx)
        {
            if (msgr->encrypt_gcm_pool.size())
            {
                cl->enc_ctx = msgr->encrypt_gcm_pool.back();
                msgr->encrypt_gcm_pool.pop_back();
            }
            else
            {
#ifdef WITH_ISAL_CRYPTO
                cl->enc_ctx = (isal_gcm_context_data*)malloc_or_die(sizeof(isal_gcm_context_data));
#else
                cl->enc_ctx = EVP_CIPHER_CTX_new();
                assert(cl->enc_ctx);
                int r = EVP_EncryptInit_ex(cl->enc_ctx, EVP_aes_256_gcm(), NULL, NULL, NULL);
                if (r != 1)
                {
                    fprintf(stderr, "EncryptInit error: ");
                    ERR_print_errors_fp(stderr);
                    abort();
                }
#endif
            }
        }
        uint8_t iv[12] = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 };
#ifdef WITH_ISAL_CRYPTO
        int r = isal_aes_gcm_init_256(&msgr->test_osd_aes_key_isal, cl->enc_ctx, iv, NULL, 0);
        if (r != 0)
        {
            fprintf(stderr, "isal_aes_gcm_init_256 error %d\n", r);
            abort();
        }
#else
        int r = EVP_EncryptInit_ex(cl->enc_ctx, NULL, NULL, (uint8_t*)msgr->test_osd_aes_key.data(), iv);
        if (r != 1)
        {
            fprintf(stderr, "EncryptInit error: ");
            ERR_print_errors_fp(stderr);
            abort();
        }
#endif
    }

    static void free_ctx(osd_messenger_t* msgr, osd_client_t *cl)
    {
        if (msgr->encrypt_gcm_pool.size() < msgr->max_cipher_pool_size)
            msgr->encrypt_gcm_pool.push_back(cl->enc_ctx);
        else
        {
#ifdef WITH_ISAL_CRYPTO
            free(cl->enc_ctx);
#else
            EVP_CIPHER_CTX_free(cl->enc_ctx);
#endif
        }
        cl->enc_ctx = NULL;
    }

    bool write(uint8_t *src, size_t src_len, int flags) override
    {
        if (from >= src_len)
        {
            from -= src_len;
            return true;
        }
        if (!(flags & WR_TLS))
        {
            if (flags & WR_XTS)
            {
                msgr->op_encrypted_copy_buf(cl, curbuf, bufsize, src, src_len, from, done);
            }
            else
            {
                size_t n = src_len-from;
                if (n > bufsize-done)
                    n = bufsize-done;
                if (!n)
                    return false;
                if (cl->write_csum_state && !(flags & WR_NO_CSUM))
                    XXH3_64bits_update(cl->write_csum_state, src+from, n);
                memcpy(curbuf+done, src+from, n);
                done += n;
                cl->write_op_pos += n;
                from += n;
            }
        }
        else
        {
            size_t n = src_len-from;
            if (n > bufsize-done)
                n = bufsize-done;
            if (!n)
                return false;
#ifdef WITH_ISAL_CRYPTO
            int r = isal_aes_gcm_enc_256_update(&msgr->test_osd_aes_key_isal, cl->enc_ctx, curbuf+done, src+from, n);
            assert(!r);
#else
            int actual_out;
            if (EVP_EncryptUpdate(cl->enc_ctx, curbuf+done, &actual_out, src+from, n) != 1)
            {
                fprintf(stderr, "EncryptUpdate error: ");
                ERR_print_errors_fp(stderr);
                abort();
            }
            assert(actual_out == n);
#endif
            if (cl->write_csum_state && !(flags & WR_NO_CSUM))
                XXH3_64bits_update(cl->write_csum_state, src+from, n);
            done += n;
            cl->write_op_pos += n;
            from += n;
        }
        if (from < src_len)
            return false;
        from = 0;
        return true;
    }

    static void write_tag_to(osd_messenger_t *msgr, osd_client_t *cl, uint8_t *dst)
    {
#ifdef WITH_ISAL_CRYPTO
        int r = isal_aes_gcm_enc_256_finalize(&msgr->test_osd_aes_key_isal, cl->enc_ctx, dst, 16);
        assert(!r);
#else
        int actual_out = 0;
        int r = EVP_EncryptFinal_ex(cl->enc_ctx, NULL, &actual_out);
        if (r != 1)
        {
            fprintf(stderr, "EncryptFinal error: ");
            ERR_print_errors_fp(stderr);
            abort();
        }
        assert(actual_out == 0);
        r = EVP_CIPHER_CTX_ctrl(cl->enc_ctx, EVP_CTRL_GCM_GET_TAG, 16, dst);
        assert(r == 1);
#endif
    }

    bool finish() override
    {
        // Tag is 16 bytes
        if (done >= bufsize)
            return false;
        if (bufsize-done < 16 || cl->enc_tag_size)
        {
            // No space for the full tag, but msgr_rdma expects us to always fill the whole buffer
            if (!cl->enc_tag_size)
            {
                write_tag_to(msgr, cl, cl->enc_tag);
                cl->enc_tag_size = 16;
            }
            size_t n = bufsize-done;
            if (n > cl->enc_tag_size)
                n = cl->enc_tag_size;
            memcpy(curbuf+done, cl->enc_tag+16-cl->enc_tag_size, n);
            done += n;
            cl->enc_tag_size -= n;
            if (cl->enc_tag_size > 0)
                return false;
        }
        else
        {
            // The whole tag fits at once
            write_tag_to(msgr, cl, curbuf+done);
            done += 16;
        }
        free_ctx(msgr, cl);
        return true;
    }

    size_t get_done()
    {
        return done;
    }
};

// FIXME Split into 3 classes - basic, tls and gcm
class get_op_writer_t: public msgr_op_writer_t
{
    osd_messenger_t* msgr;
    osd_client_t* cl;
    size_t from;
    size_t enc_size;
    size_t done_enc;

    void ssl_extend_buf(size_t more = 0)
    {
        size_t min_cap = cl->ssl_out_buf_size*2;
        if (min_cap < cl->ssl_out_buf_size+more)
            min_cap = cl->ssl_out_buf_size+more;
        if (min_cap < 16384)
            min_cap = 16384;
        if (cl->ssl_out_buf_cap < min_cap)
        {
            uintptr_t old_buf = (uintptr_t)cl->ssl_out_buf;
            uintptr_t old_end = old_buf + cl->ssl_out_buf_cap;
            cl->ssl_out_buf = (uint8_t*)realloc_or_die(cl->ssl_out_buf, min_cap);
            cl->ssl_out_buf_cap = min_cap;
            for (auto & iov: cl->send_list)
            {
                if ((uintptr_t)iov.iov_base >= old_buf && (uintptr_t)iov.iov_base < old_end)
                    iov.iov_base = cl->ssl_out_buf + ((uintptr_t)iov.iov_base - old_buf);
            }
        }
    }

    void send_out_buf(size_t n)
    {
        if (cl->send_list.size() > 0)
        {
            iovec& last = cl->send_list.back();
            if (last.iov_base+last.iov_len == cl->ssl_out_buf+cl->ssl_out_buf_size)
            {
                last.iov_len += n;
                cl->ssl_out_buf_size += n;
                return;
            }
        }
        cl->send_list.push_back((iovec){ .iov_base = cl->ssl_out_buf+cl->ssl_out_buf_size, .iov_len = n });
        cl->ssl_out_buf_size += n;
    }

    void copy_ssl()
    {
        size_t n = 0;
        do
        {
            ssl_extend_buf();
            int r = BIO_read(cl->read_from_ssl, cl->ssl_out_buf+cl->ssl_out_buf_size, cl->ssl_out_buf_cap-cl->ssl_out_buf_size);
            if (r > 0)
                n += r;
        } while (cl->ssl_out_buf_size+n >= cl->ssl_out_buf_cap);
        if (n > 0)
            send_out_buf(n);
    }

public:
    get_op_writer_t(osd_messenger_t* msgr, osd_client_t* cl):
        msgr(msgr), cl(cl), from(cl->write_op_pos), enc_size(0), done_enc(0)
    {
    }

    void reset()
    {
        from = cl->write_op_pos;
        enc_size = 0;
        done_enc = 0;
        if (cl->gcm_enabled)
        {
            gcm_op_writer_t::init_ctx(msgr, cl);
        }
    }

    void flush_ssl()
    {
        if (!cl->ssl_handshake_done)
        {
            if (!msgr->ssl_do_handshake(cl))
                return;
        }
        if (cl->send_list.size() >= IOV_MAX)
        {
            return;
        }
        copy_ssl();
    }

    bool write(uint8_t *src, size_t src_len, int flags) override
    {
        if (from >= src_len)
        {
            // Skip
            from -= src_len;
            return true;
        }
        if (cl->send_list.size() >= IOV_MAX)
        {
            return false;
        }
        if (flags & WR_TLS)
        {
            if (cl->ssl_cli)
            {
                if (!cl->ssl_handshake_done)
                {
                    if (!msgr->ssl_do_handshake(cl))
                        return false;
                }
                if (cl->ssl_handshake_done)
                {
                    if (!ssl_op_writer_t::write_to_ssl(cl, src, src_len, flags, from))
                        return false;
                }
                // Copy data to client's temporary SSL output buffer
                copy_ssl();
                if (from < src_len)
                    return false;
                from = 0;
                return true;
            }
            else if (cl->enc_ctx)
            {
                // Encrypt data to client's temporary output buffer (all at once)
                size_t n = src_len-from;
                ssl_extend_buf(n);
#ifdef WITH_ISAL_CRYPTO
                int r = isal_aes_gcm_enc_256_update(&msgr->test_osd_aes_key_isal, cl->enc_ctx, cl->ssl_out_buf+cl->ssl_out_buf_size, src+from, n);
                assert(!r);
#else
                int actual_out;
                if (EVP_EncryptUpdate(cl->enc_ctx, cl->ssl_out_buf+cl->ssl_out_buf_size, &actual_out, src+from, n) != 1)
                {
                    fprintf(stderr, "EncryptUpdate error: ");
                    ERR_print_errors_fp(stderr);
                    abort();
                }
                assert(actual_out == n);
#endif
                if (cl->write_csum_state && !(flags & WR_NO_CSUM))
                    XXH3_64bits_update(cl->write_csum_state, src+from, n);
                send_out_buf(n);
                cl->write_op_pos += n;
                from += n;
                if (from < src_len)
                    return false;
                from = 0;
                return true;
            }
        }
        if (flags & WR_XTS)
        {
            if (!cl->write_op->enc_buf)
            {
                if (cl->send_list.size() >= IOV_MAX-1)
                {
                    // Make sure that 1 encrypted buffer and 1 checksum fits
                    return false;
                }
                // No way except than to allocate a temporary buffer and encrypt data to it
                assert(cl->write_op->req.hdr.opcode == OSD_OP_WRITE);
                enc_size = cl->write_op->req.rw.len - from + (from % 16);
                assert(enc_size > 0);
                cl->write_op->enc_buf = (uint8_t*)malloc_or_die(enc_size);
                cl->send_list.push_back((iovec){ .iov_base = cl->write_op->enc_buf, .iov_len = enc_size });
            }
            assert(enc_size > 0);
            msgr->op_encrypted_copy_buf(cl, cl->write_op->enc_buf, enc_size, src, src_len, from, done_enc);
            assert(from == src_len);
        }
        else
        {
            if (cl->write_csum_state && !(flags & WR_NO_CSUM))
                XXH3_64bits_update(cl->write_csum_state, src+from, src_len-from);
            cl->send_list.push_back((iovec){ src+from, src_len-from });
            cl->write_op_pos += src_len-from;
        }
        from = 0;
        return true;
    }

    bool finish() override
    {
        if (cl->ssl_cli)
        {
            if (cl->send_list.size() >= IOV_MAX)
                return false;
            copy_ssl();
        }
        else if (cl->enc_ctx)
        {
            if (cl->send_list.size() >= IOV_MAX)
                return false;
            // Tag is 16 bytes
            ssl_extend_buf(16);
            gcm_op_writer_t::write_tag_to(msgr, cl, cl->ssl_out_buf+cl->ssl_out_buf_size);
            send_out_buf(16);
            gcm_op_writer_t::free_ctx(msgr, cl);
        }
        return true;
    }
};

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
        while (cl->write_op || cl->write_ops.size())
        {
            try_send(cl);
        }
    }
    else
    {
        if (!try_send(cl) && cl->write_state == 0)
        {
            cl->write_state = CL_WRITE_READY;
            write_ready_clients.push_back(cl->client_id);
        }
        ringloop->wakeup();
    }
}

bool osd_messenger_t::ssl_do_handshake(osd_client_t *cl)
{
    if (cl->ssl_handshake_done)
    {
        return true;
    }
    int r = SSL_do_handshake(cl->ssl_cli);
    if (r > 0)
    {
        cl->ssl_handshake_done = true;
    }
    else
    {
        r = SSL_get_error(cl->ssl_cli, r);
        if (r != 0 && r != SSL_ERROR_WANT_READ && r != SSL_ERROR_WANT_WRITE)
        {
            fprintf(stderr, "Client %ju TLS handshake error: %s, stopping client\n", cl->client_id, ERR_error_string(ERR_get_error(), NULL));
            cl->io_error = true;
            return false;
        }
    }
    return true;
}

bool osd_messenger_t::try_send(osd_client_t *cl)
{
    if (cl->peer_state == PEER_STOPPED || cl->peer_fd < 0)
    {
        return true;
    }
    if (cl->write_msg.msg_iovlen > 0 || !ringloop->space_left() && !use_sync_send_recv)
    {
        return false;
    }
    assert(cl->peer_state != PEER_RDMA);
    get_op_writer_t wr(this, cl);
    while ((cl->write_op || cl->write_ops.size()) && cl->send_list.size() < IOV_MAX)
    {
        if (!cl->write_op)
        {
            next_write_op(cl);
            wr.reset();
        }
        osd_op_t *op = cl->write_op;
        if (!op_write_to(cl, wr))
        {
            if (cl->io_error)
            {
                stop_client(cl->client_id);
                return true;
            }
            break;
        }
        if (!cl->write_op && op->op_type == OSD_OP_IN)
        {
            cl->send_free_ops.push_back(op);
        }
    }
    if (!cl->send_list.size() && cl->ssl_cli)
    {
        wr.flush_ssl();
    }
    if (!cl->send_list.size())
    {
        cl->write_state = 0;
        return true;
    }
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
        assert(sqe);
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

size_t osd_messenger_t::copy_ops_to(osd_client_t *cl, uint8_t *dst, size_t dst_len)
{
    if (cl->gcm_enabled)
    {
        return copy_ops_to_with<gcm_op_writer_t>(cl, dst, dst_len);
    }
    return copy_ops_to_with<copy_op_writer_t>(cl, dst, dst_len);
}

template<typename T>
size_t osd_messenger_t::copy_ops_to_with(osd_client_t *cl, uint8_t *dst, size_t dst_len)
{
    T wr(this, cl, dst, dst_len);
    while (cl->write_op || cl->write_ops.size())
    {
        if (!cl->write_op)
        {
            next_write_op(cl);
            wr.reset();
        }
        osd_op_t *op = cl->write_op;
        if (!op_write_to(cl, wr))
        {
            if (cl->io_error)
                return 0;
            break;
        }
        if (!cl->write_op && op->op_type == OSD_OP_IN)
        {
            // this is a reply, free the op after sending it
            cl->send_free_ops.push_back(op);
        }
    }
    /*FIXME if (!wr.get_done() && cl->ssl_cli)
    {
        wr.flush_ssl();
    }*/
    return wr.get_done();
}

void osd_messenger_t::next_write_op(osd_client_t *cl)
{
    cl->write_op = cl->write_ops.front();
    cl->write_ops.pop_front();
    if (cl->proto_csum_status == MSGR_CSUM_FULL || cl->proto_csum_status == MSGR_CSUM_PAYLOAD)
    {
        if (!cl->write_csum_state)
            cl->write_csum_state = XXH3_createState();
        XXH3_64bits_reset(cl->write_csum_state);
    }
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
            {
                if (!((size_t)cl->zc_free_list[i] & 7))
                    delete cl->zc_free_list[i];
                else
                    free((void*)((size_t)cl->zc_free_list[i] & ~(size_t)7));
            }
            if (i > 0)
                cl->zc_free_list.erase(cl->zc_free_list.begin(), cl->zc_free_list.begin()+i+1);
            return;
        }
        if (cl->send_list_size > result)
        {
            fprintf(stderr, "Client %ju socket write error: expected to send "
                "%zu bytes with MSG_WAITALL but sent %u. Disconnecting client\n", cl->client_id, cl->send_list_size, result);
            stop_client(cl->client_id);
            return;
        }
        for (auto op: cl->send_free_ops)
        {
            if (more)
            {
                assert(!((size_t)op & 7));
                cl->zc_free_list.push_back(op);
            }
            else
                delete op;
        }
        if (more)
        {
            if (cl->ssl_out_buf_size)
            {
                cl->zc_free_list.push_back((osd_op_t*)((size_t)cl->ssl_out_buf | 1));
                cl->ssl_out_buf = NULL;
                cl->ssl_out_buf_cap = 0;
            }
            cl->zc_free_list.push_back(NULL); // end marker
        }
        cl->ssl_out_buf_size = 0;
        cl->send_free_ops.clear();
        cl->write_state = 0;
        if (cl->write_op || cl->write_ops.size())
            cl->write_state = CL_WRITE_READY;
        if ((cl->proto_csum_status & MSGR_CSUM_NEG) && !cl->write_op && !cl->write_ops.size())
        {
            // Checksums negotiated, enable
            cl->proto_csum_status = cl->proto_csum_status & (~MSGR_CSUM_NEG);
        }
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

static inline bool op_has_data_for_ssl(osd_op_t *op)
{
    return (op->op_type == OSD_OP_IN
        ? (op->req.hdr.opcode == OSD_OP_SEC_LIST ||
        op->req.hdr.opcode == OSD_OP_SHOW_CONFIG ||
        op->req.hdr.opcode == OSD_OP_DESCRIBE)
        : (op->req.hdr.opcode == OSD_OP_SEC_STABILIZE ||
        op->req.hdr.opcode == OSD_OP_SEC_ROLLBACK ||
        op->req.hdr.opcode == OSD_OP_SHOW_CONFIG)) && op->iov.count > 0;
}

static inline bool op_has_data_for_nonssl(osd_op_t *op)
{
    return (op->op_type == OSD_OP_IN
        ? (op->req.hdr.opcode == OSD_OP_READ ||
        op->req.hdr.opcode == OSD_OP_SEC_READ)
        : (op->req.hdr.opcode == OSD_OP_WRITE ||
        op->req.hdr.opcode == OSD_OP_SEC_WRITE ||
        op->req.hdr.opcode == OSD_OP_SEC_WRITE_STABLE)) && op->iov.count > 0;
}

bool osd_messenger_t::op_write_to(osd_client_t *cl, msgr_op_writer_t & wr)
{
    osd_op_t *op = cl->write_op;
    // Header
    if (!wr.write((op->op_type == OSD_OP_IN ? op->reply.buf : op->req.buf), OSD_PACKET_SIZE,
        WR_TLS | (cl->proto_csum_status == MSGR_CSUM_PAYLOAD ? WR_NO_CSUM : 0)))
    {
        return false;
    }
    // Bitmap
    if (op->op_type == OSD_OP_IN)
    {
        if (op->req.hdr.opcode == OSD_OP_SEC_READ && op->reply.sec_rw.attr_len > 0)
        {
            if (!wr.write((uint8_t*)op->bitmap, op->reply.sec_rw.attr_len, WR_TLS))
                return false;
        }
        else if (op->req.hdr.opcode == OSD_OP_SEC_READ_BMP && op->reply.hdr.retval > 0)
        {
            if (!wr.write((uint8_t*)op->buf, (size_t)op->reply.hdr.retval, WR_TLS))
                return false;
        }
        else if (op->req.hdr.opcode == OSD_OP_READ && op->reply.rw.bitmap_len > 0)
        {
            if (!wr.write((uint8_t*)op->bitmap, op->reply.rw.bitmap_len, WR_TLS))
                return false;
        }
    }
    else if (op->op_type == OSD_OP_OUT)
    {
        if ((op->req.hdr.opcode == OSD_OP_SEC_WRITE || op->req.hdr.opcode == OSD_OP_SEC_WRITE_STABLE) &&
            op->req.sec_rw.attr_len > 0)
        {
            if (!wr.write((uint8_t*)op->bitmap, op->req.sec_rw.attr_len, WR_TLS))
                return false;
        }
        else if (op->req.hdr.opcode == OSD_OP_SEC_READ_BMP && op->req.sec_read_bmp.len > 0)
        {
            if (!wr.write((uint8_t*)op->buf, (size_t)op->req.sec_read_bmp.len, WR_TLS))
                return false;
        }
    }
    // Operation data
    if (op_has_data_for_ssl(op))
    {
        for (int i = 0; i < cl->write_op->iov.count; i++)
        {
            auto & iov = cl->write_op->iov.buf[i];
            if (!wr.write((uint8_t*)iov.iov_base, iov.iov_len, WR_TLS))
                return false;
        }
    }
    else if (op_has_data_for_nonssl(op))
    {
        for (int i = 0; i < cl->write_op->iov.count; i++)
        {
            auto & iov = cl->write_op->iov.buf[i];
            if (!wr.write((uint8_t*)iov.iov_base, iov.iov_len, (op->enc ? WR_XTS : 0)))
                return false;
        }
    }
    if (cl->proto_csum_status == MSGR_CSUM_FULL ||
        cl->proto_csum_status == MSGR_CSUM_PAYLOAD && cl->write_op_pos > OSD_PACKET_SIZE)
    {
        cl->write_op->csum = XXH3_64bits_digest(cl->write_csum_state);
        if (!wr.write((uint8_t*)&cl->write_op->csum, 8, WR_TLS|WR_NO_CSUM))
            return false;
    }
    if (!wr.finish())
        return false;
    op_encrypt_free(cl);
    cl->write_op = NULL;
    cl->write_op_pos = 0;
    return true;
}
