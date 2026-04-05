// Copyright (c) Vitaliy Filippov, 2026+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include <assert.h>

#include "etcd_state_client.h"
#include "messenger.h"
#include "msgr_encrypt.h"

op_aes_xts_encrypt_t::op_aes_xts_encrypt_t()
{
#ifdef WITH_OPENSSL
    if (!(ctx = EVP_CIPHER_CTX_new()))
    {
        ERR_print_errors_fp(stderr);
        abort();
    }
    EVP_CIPHER_CTX_set_padding(ctx, 0);
    if (EVP_EncryptInit_ex(ctx, EVP_aes_256_xts(), NULL, NULL, NULL) != 1)
    {
        ERR_print_errors_fp(stderr);
        abort();
    }
#else
    fprintf(stderr, "Error: Vitastor is built without encryption support\n");
    abort();
#endif
}

op_aes_xts_encrypt_t::~op_aes_xts_encrypt_t()
{
    assert(!encrypted);
#ifdef WITH_OPENSSL
    EVP_CIPHER_CTX_free(ctx);
#endif
    if (tmp)
        free(tmp);
}

void op_aes_xts_encrypt_t::start(uint8_t *key, uint64_t start_offset, size_t block_size)
{
    assert(!encrypted);
    this->start_offset = start_offset;
    this->key = key;
    this->block_size = block_size;
    this->offset = 0;
    this->encrypted = false;
    this->tmp_pos = 0;
    if (tmp && tmp_size != block_size)
    {
        free(tmp);
        tmp = NULL;
        tmp_size = 0;
    }
#ifdef WITH_OPENSSL
    if (EVP_EncryptInit_ex(ctx, NULL, NULL, key, NULL) != 1)
    {
        ERR_print_errors_fp(stderr);
        abort();
    }
#endif
}

void op_aes_xts_encrypt_t::encrypt_block(uint8_t *in, uint8_t *out)
{
#ifdef WITH_OPENSSL
    uint8_t iv[16] = { 0 };
    *((uint64_t*)iv) = start_offset + offset - offset%block_size;
    if (EVP_EncryptInit_ex(ctx, NULL, NULL, NULL, iv) != 1)
    {
        ERR_print_errors_fp(stderr);
        abort();
    }
    int actual_out = 0;
    if (EVP_EncryptUpdate(ctx, out, &actual_out, in, block_size) != 1)
    {
        ERR_print_errors_fp(stderr);
        abort();
    }
    assert(actual_out == block_size);
#endif
}

void op_aes_xts_encrypt_t::update(uint8_t *in, size_t max_in, uint8_t *out, size_t max_out, size_t & done_in, size_t & done_out)
{
    // Fucking AES-XTS implementations (all of them) don't have streaming support,
    // crafting IV to resume encryption is slow, so we have to accumulate a full block
    // and encrypt it at once :-(
    // And then we have to support consuming it in parts because it's simpler for the
    // higher layers.
    if (encrypted)
    {
        // Copy accumulated and encrypted output
        assert(tmp);
        if (max_out > block_size - tmp_pos)
            max_out = block_size - tmp_pos;
        memcpy(out, tmp + tmp_pos, max_out);
        done_out += max_out;
        tmp_pos += max_out;
        if (tmp_pos >= block_size)
            encrypted = false;
    }
    else if (max_in < block_size - offset%block_size)
    {
        // Just accumulate input
        if (!tmp)
        {
            tmp = (uint8_t*)malloc_or_die(block_size);
            tmp_size = block_size;
        }
        memcpy(tmp + offset%block_size, in, max_in);
        done_in += max_in;
        offset += max_in;
    }
    else if (max_out < block_size)
    {
        // Accumulate and encrypt input in <tmp>, then copy part of it to <out>
        if (!tmp)
        {
            tmp = (uint8_t*)malloc_or_die(block_size);
            tmp_size = block_size;
        }
        max_in = block_size - offset%block_size;
        memcpy(tmp + offset%block_size, in, max_in);
        encrypt_block(tmp, tmp);
        encrypted = true;
        memcpy(out, tmp, max_out);
        tmp_pos = max_out;
        done_in += max_in;
        offset += max_in;
        done_out += max_out;
    }
    else if (!(offset%block_size))
    {
        // Full block - simplest case
        encrypt_block(in, out);
        done_in += block_size;
        offset += block_size;
        done_out += block_size;
    }
    else
    {
        // Accumulate input and encrypt directly to <output>
        assert(tmp);
        max_in = block_size - offset%block_size;
        memcpy(tmp + offset%block_size, in, max_in);
        encrypt_block(tmp, out);
        done_in += max_in;
        offset += max_in;
        done_out += block_size;
    }
}

void destroy_aes_xts_encrypt(op_aes_xts_encrypt_t *encrypt_ctx)
{
    delete encrypt_ctx;
}

op_aes_xts_decrypt_t::op_aes_xts_decrypt_t()
{
#ifdef WITH_OPENSSL
    if (!(ctx = EVP_CIPHER_CTX_new()))
    {
        ERR_print_errors_fp(stderr);
        abort();
    }
    EVP_CIPHER_CTX_set_padding(ctx, 0);
    if (EVP_DecryptInit_ex(ctx, EVP_aes_256_xts(), NULL, NULL, NULL) != 1)
    {
        ERR_print_errors_fp(stderr);
        abort();
    }
#else
    fprintf(stderr, "Error: Vitastor is built without encryption support\n");
    abort();
#endif
}

op_aes_xts_decrypt_t::~op_aes_xts_decrypt_t()
{
    assert(!decrypted);
#ifdef WITH_OPENSSL
    EVP_CIPHER_CTX_free(ctx);
#endif
    if (tmp)
        free(tmp);
}

void op_aes_xts_decrypt_t::start(uint8_t **key_chain, size_t chain_size, uint8_t *key_indexes, uint64_t start_offset, size_t block_size)
{
    assert(!decrypted);
    this->start_offset = start_offset;
    this->key_chain = chain_size > 1 ? key_chain : 0;
    this->chain_size = chain_size > 1 ? chain_size : 0;
    this->key_indexes = chain_size > 1 ? key_indexes : NULL;
    assert(chain_size <= 1 || key_indexes != NULL);
    this->block_size = block_size;
    this->offset = 0;
    this->tmp_pos = 0;
    if (tmp && tmp_size != block_size)
    {
        free(tmp);
        tmp = NULL;
        tmp_size = 0;
    }
#ifdef WITH_OPENSSL
    if (chain_size == 1 && key_chain[0] && EVP_DecryptInit_ex(ctx, NULL, NULL, key_chain[0], NULL) != 1)
    {
        ERR_print_errors_fp(stderr);
        abort();
    }
#endif
}

void op_aes_xts_decrypt_t::decrypt_block(uint8_t *in, uint8_t *out)
{
    uint8_t *key = NULL;
    if (chain_size)
    {
        assert(key_indexes[offset/block_size] < chain_size);
        key = key_chain[key_indexes[offset/block_size]];
        if (!key)
        {
            if (in != out)
                memcpy(out, in, block_size);
            return;
        }
    }
#ifdef WITH_OPENSSL
    uint8_t iv[16] = { 0 };
    *((uint64_t*)iv) = start_offset + offset - offset%block_size;
    if (EVP_DecryptInit_ex(ctx, NULL, NULL, key, iv) != 1)
    {
        ERR_print_errors_fp(stderr);
        abort();
    }
    int actual_out = 0;
    if (EVP_DecryptUpdate(ctx, out, &actual_out, in, block_size) != 1)
    {
        ERR_print_errors_fp(stderr);
        abort();
    }
    assert(actual_out == block_size);
#endif
}

// out may be NULL, in this case all input is still decrypted to calculate checksums,
// but part of it is skipped and not copied to out
void op_aes_xts_decrypt_t::update(uint8_t *in, size_t max_in, uint8_t *out, size_t max_out, size_t & done_in, size_t & done_out)
{
    // Fucking AES-XTS implementations (all of them) don't have streaming support,
    // crafting IV to resume decryption is slow, so we have to accumulate a full block
    // and decrypt it at once :-(
    // And then we have to support consuming it in parts because clients sometimes need
    // fragmented output.
    if (decrypted)
    {
        // Copy accumulated and decrypted output
        assert(tmp);
        if (max_out > block_size - tmp_pos)
            max_out = block_size - tmp_pos;
        if (out)
            memcpy(out, tmp + tmp_pos, max_out);
        done_out += max_out;
        tmp_pos += max_out;
        if (tmp_pos >= block_size)
            decrypted = false;
    }
    else if (max_in < block_size - offset%block_size)
    {
        // Just accumulate input
        if (!tmp)
        {
            tmp = (uint8_t*)malloc_or_die(block_size);
            tmp_size = block_size;
        }
        memcpy(tmp + offset%block_size, in, max_in);
        done_in += max_in;
        offset += max_in;
    }
    else if (max_out < block_size || !out)
    {
        // Accumulate and decrypt input in <tmp>, then copy part of it to <out>
        if (!tmp)
        {
            tmp = (uint8_t*)malloc_or_die(block_size);
            tmp_size = block_size;
        }
        max_in = block_size - offset%block_size;
        memcpy(tmp + offset%block_size, in, max_in);
        decrypt_block(tmp, tmp);
        decrypted = true;
        if (out)
            memcpy(out, tmp, max_out);
        tmp_pos = max_out;
        done_in += max_in;
        offset += max_in;
        done_out += max_out;
    }
    else if (!(offset%block_size))
    {
        // Full block - simplest case
        if (out)
            decrypt_block(in, out);
        done_in += block_size;
        offset += block_size;
        done_out += block_size;
    }
    else
    {
        // Accumulate input and decrypt directly to <output>
        assert(tmp);
        max_in = block_size - offset%block_size;
        memcpy(tmp + offset%block_size, in, max_in);
        assert(out);
        decrypt_block(tmp, out);
        done_in += max_in;
        offset += max_in;
        done_out += block_size;
    }
}

void destroy_aes_xts_decrypt(op_aes_xts_decrypt_t *decrypt_ctx)
{
    delete decrypt_ctx;
}

void osd_messenger_t::op_encrypted_copy_buf(osd_client_t *cl, uint8_t *enc_buf, size_t enc_len, uint8_t *plain, size_t plain_len, size_t & done_plain, size_t & done_enc)
{
    if (!cl->encrypt_ctx)
    {
        if (encrypt_ctx_pool.size())
        {
            cl->encrypt_ctx = encrypt_ctx_pool.back();
            encrypt_ctx_pool.pop_back();
        }
        else
            cl->encrypt_ctx = new op_aes_xts_encrypt_t();
        assert(cl->write_op->enc->key_chain[0]);
        cl->encrypt_ctx->start(cl->write_op->enc->key_chain[0], cl->write_op->req.rw.offset, cl->write_op->enc->bitmap_granularity);
    }
    while (done_enc < enc_len && (done_plain < plain_len || cl->encrypt_ctx->has_buffered()))
    {
        size_t done_in = 0;
        size_t done_out = 0;
        cl->encrypt_ctx->update(plain+done_plain, plain_len-done_plain, enc_buf+done_enc, enc_len-done_enc, done_in, done_out);
        if (cl->write_csum_state && done_out > 0)
            XXH3_64bits_update(cl->write_csum_state, enc_buf+done_enc, done_out);
        done_enc += done_out;
        cl->write_op_pos += done_in;
        done_plain += done_in;
    }
}

void osd_messenger_t::op_decrypted_copy_buf(osd_client_t *cl, uint8_t *enc_buf, size_t enc_len, uint8_t *plain, size_t plain_len, size_t & done_plain, size_t & done_enc)
{
    op_decrypt_start(cl);
    while (done_plain < plain_len && done_enc < enc_len)
    {
        size_t done_in = 0;
        size_t done_out = 0;
        // plain == NULL means skip output
        cl->decrypt_ctx->update(enc_buf+done_enc, enc_len-done_enc, plain ? plain+done_plain : NULL, plain_len-done_plain, done_in, done_out);
        if (cl->read_csum_state && done_in > 0)
            XXH3_64bits_update(cl->read_csum_state, enc_buf+done_enc, done_in);
        done_enc += done_in;
        cl->read_op_pos += done_out;
        cl->read_op_inline_decrypt_in += done_in;
        done_plain += done_out;
    }
}

void osd_messenger_t::op_decrypt_start(osd_client_t* cl)
{
    if (!cl->decrypt_ctx)
    {
        if (decrypt_ctx_pool.size())
        {
            cl->decrypt_ctx = decrypt_ctx_pool.back();
            decrypt_ctx_pool.pop_back();
        }
        else
            cl->decrypt_ctx = new op_aes_xts_decrypt_t();
        auto & enc = cl->read_op->enc;
        assert(cl->read_op->req.hdr.opcode == OSD_OP_READ);
        cl->decrypt_ctx->start(enc->key_chain, enc->chain_size,
            (cl->read_op->req.rw.flags & OSD_OP_RETURN_CHAIN) ? (uint8_t*)cl->read_op->bitmap + enc->read_chain_bitmap_pos : 0,
            cl->read_op->req.rw.offset, enc->bitmap_granularity);
    }
}

void osd_messenger_t::op_decrypt_inline(osd_client_t* cl)
{
    op_decrypt_start(cl);
    osd_op_t *op = cl->read_op;
    size_t from_in = cl->read_op_inline_decrypt_in;
    int i = 0;
    while (i < op->iov.count && from_in >= op->iov.buf[i].iov_len)
    {
        from_in -= op->iov.buf[i].iov_len;
        i++;
    }
    size_t from_out = cl->read_op_inline_decrypt_pos - OSD_PACKET_SIZE - op->reply.rw.bitmap_len;
    int j = 0;
    while (j < op->iov.count && from_out >= op->iov.buf[j].iov_len)
    {
        from_out -= op->iov.buf[j].iov_len;
        j++;
    }
    while (i < op->iov.count && j < op->iov.count)
    {
        uint8_t *in = (uint8_t*)op->iov.buf[i].iov_base + from_in;
        size_t in_len = op->iov.buf[i].iov_len - from_in;
        uint8_t *out = (uint8_t*)op->iov.buf[j].iov_base + from_out;
        size_t out_len = op->iov.buf[j].iov_len - from_out;
        size_t done_in = 0;
        size_t done_out = 0;
        cl->decrypt_ctx->update(in, in_len, out, out_len, done_in, done_out);
        if (done_in >= in_len)
        {
            i++;
            from_in = 0;
        }
        else
            from_in += done_in;
        if (done_out >= out_len)
        {
            j++;
            from_out = 0;
        }
        else
            from_out += done_out;
    }
    assert(j >= op->iov.count);
}

void osd_messenger_t::op_decrypt_free(osd_client_t* cl)
{
    if (cl->decrypt_ctx)
    {
        if (decrypt_ctx_pool.size() > max_aes_xts_pool_size)
            delete cl->decrypt_ctx;
        else
            decrypt_ctx_pool.push_back(cl->decrypt_ctx);
        cl->decrypt_ctx = NULL;
    }
}

void osd_messenger_t::op_encrypt_free(osd_client_t* cl)
{
    if (cl->encrypt_ctx)
    {
        if (encrypt_ctx_pool.size() > max_aes_xts_pool_size)
            delete cl->encrypt_ctx;
        else
            encrypt_ctx_pool.push_back(cl->encrypt_ctx);
        cl->encrypt_ctx = NULL;
    }
}
