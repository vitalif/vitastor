// Copyright (c) Vitaliy Filippov, 2026+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include <assert.h>

#ifdef WITH_ISAL_CRYPTO
#include <isa-l_crypto/isal_crypto_api.h>
#endif

#include "str_util.h"
#include "etcd_state_client.h"
#include "messenger.h"
#include "msgr_encrypt.h"
#include "http_client.h"
#include "openssl_util.h"

#include <openssl/ssl.h>
#include <openssl/err.h>

op_aes_xts_encrypt_t::op_aes_xts_encrypt_t()
{
#ifndef WITH_ISAL_CRYPTO
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
#endif
}

op_aes_xts_encrypt_t::~op_aes_xts_encrypt_t()
{
    assert(!encrypted);
#ifndef WITH_ISAL_CRYPTO
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
#ifndef WITH_ISAL_CRYPTO
    if (EVP_EncryptInit_ex(ctx, NULL, NULL, key, NULL) != 1)
    {
        ERR_print_errors_fp(stderr);
        abort();
    }
#endif
}

void op_aes_xts_encrypt_t::encrypt_block(uint8_t *in, uint8_t *out)
{
    uint8_t iv[16] = { 0 };
    *((uint64_t*)iv) = start_offset + offset - offset%block_size;
#ifdef WITH_ISAL_CRYPTO
    int r = isal_aes_xts_enc_256(key+32, key, iv, block_size, in, out);
    assert(r == 0 || r == ISAL_CRYPTO_ERR_XTS_SAME_KEYS);
#else
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
        {
            encrypted = false;
            done_in += 1;
        }
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
        done_in += max_in-1;
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
#ifndef WITH_ISAL_CRYPTO
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
#endif
}

op_aes_xts_decrypt_t::~op_aes_xts_decrypt_t()
{
    assert(!decrypted);
#ifndef WITH_ISAL_CRYPTO
    EVP_CIPHER_CTX_free(ctx);
#endif
    if (tmp)
        free(tmp);
}

void op_aes_xts_decrypt_t::start(uint8_t **key_chain, size_t chain_size, void *key_indexes, uint64_t start_offset, size_t block_size)
{
    assert(!decrypted);
    this->start_offset = start_offset;
    this->key_chain = key_chain;
    this->chain_size = chain_size;
    this->key_indexes = key_indexes;
    this->key_index_bytes = osd_op_rw_t::chain_info_bytes(chain_size);
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
#ifndef WITH_ISAL_CRYPTO
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
    if (chain_size > 1)
    {
        uint32_t key_index = key_index_bytes == 1
            ? ((uint8_t*)key_indexes)[offset/block_size]
            : (key_index_bytes == 2
                ? ((uint16_t*)key_indexes)[offset/block_size]
                : (key_index_bytes == 4
                    ? ((uint32_t*)key_indexes)[offset/block_size]
                    : UINT32_MAX));
        assert(key_index < chain_size);
        key = key_chain[key_index];
    }
    else
    {
        key = key_chain[0];
    }
    if (!key)
    {
        if (in != out)
            memcpy(out, in, block_size);
        return;
    }
    uint8_t iv[16] = { 0 };
    *((uint64_t*)iv) = start_offset + offset - offset%block_size;
#ifdef WITH_ISAL_CRYPTO
    int r = isal_aes_xts_dec_256(key+32, key, iv, block_size, in, out);
    assert(r == 0 || r == ISAL_CRYPTO_ERR_XTS_SAME_KEYS);
#else
    if (EVP_DecryptInit_ex(ctx, NULL, NULL, chain_size == 1 ? NULL : key, iv) != 1)
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
        {
            decrypted = false;
            done_in += 1;
        }
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
        done_in += max_in-1;
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
    if (!cl->xts_enc_ctx)
    {
        if (encrypt_xts_pool.size())
        {
            cl->xts_enc_ctx = encrypt_xts_pool.back();
            encrypt_xts_pool.pop_back();
        }
        else
            cl->xts_enc_ctx = new op_aes_xts_encrypt_t();
        assert(cl->write_op->enc->key_chain[0]);
        cl->xts_enc_ctx->start(cl->write_op->enc->key_chain[0], cl->write_op->req.rw.offset, cl->write_op->enc->bitmap_granularity);
    }
    while (done_plain < plain_len && done_enc < enc_len)
    {
        size_t done_in = 0;
        size_t done_out = 0;
        cl->xts_enc_ctx->update(plain+done_plain, plain_len-done_plain, enc_buf+done_enc, enc_len-done_enc, done_in, done_out);
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
        cl->xts_dec_ctx->update(enc_buf+done_enc, enc_len-done_enc, plain ? plain+done_plain : NULL, plain_len-done_plain, done_in, done_out);
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
    if (!cl->xts_dec_ctx)
    {
        if (decrypt_xts_pool.size())
        {
            cl->xts_dec_ctx = decrypt_xts_pool.back();
            decrypt_xts_pool.pop_back();
        }
        else
            cl->xts_dec_ctx = new op_aes_xts_decrypt_t();
        auto & enc = cl->read_op->enc;
        assert(cl->read_op->req.hdr.opcode == OSD_OP_READ);
        cl->xts_dec_ctx->start(enc->key_chain, enc->chain_size,
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
        cl->xts_dec_ctx->update(in, in_len, out, out_len, done_in, done_out);
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
    if (cl->xts_dec_ctx)
    {
        if (decrypt_xts_pool.size() > max_cipher_pool_size)
            delete cl->xts_dec_ctx;
        else
            decrypt_xts_pool.push_back(cl->xts_dec_ctx);
        cl->xts_dec_ctx = NULL;
    }
}

void osd_messenger_t::op_encrypt_free(osd_client_t* cl)
{
    if (cl->xts_enc_ctx)
    {
        if (encrypt_xts_pool.size() > max_cipher_pool_size)
            delete cl->xts_enc_ctx;
        else
            encrypt_xts_pool.push_back(cl->xts_enc_ctx);
        cl->xts_enc_ctx = NULL;
    }
}

bool osd_messenger_t::derive_aes_keys(osd_client_t *cl, bool update_my, bool update_peer)
{
    if (!cl->hs_result.shared_secret.size())
    {
        cl->hs_result = cl->hs->get_result();
    }
    std::vector<uint8_t> old_my = cl->my_key, old_peer = cl->peer_key;
    // Both keys include AES key and iv + xxhash3 secret
    const auto len = AES_256_GCM_KEY_SIZE + AES_256_GCM_IV_SIZE + XXH_SECRET_DEFAULT_SIZE;
    cl->my_key.resize(len);
    cl->peer_key.resize(len);
    bool ok = true;
    if (update_my || !old_my.size())
    {
        ok = ok && hs_ctx->derive_kdf(cl->hs_result.shared_secret.data(), cl->hs_result.shared_secret.size(),
            old_my.size() ? old_my.data() : NULL, old_my.size(),
            cl->is_incoming ? "server key" : "client key", cl->my_key.data(), len);
#ifdef WITH_ISAL_CRYPTO
        if (ok)
            isal_aes_gcm_pre_256(cl->my_key.data(), &cl->my_key_isal);
#endif
        cl->my_iv_ctr = 0;
    }
    if (update_peer || !old_peer.size())
    {
        ok = ok && hs_ctx->derive_kdf(cl->hs_result.shared_secret.data(), cl->hs_result.shared_secret.size(),
            old_peer.size() ? old_peer.data() : NULL, old_peer.size(),
            !cl->is_incoming ? "server key" : "client key", cl->peer_key.data(), len);
#ifdef WITH_ISAL_CRYPTO
        if (ok)
            isal_aes_gcm_pre_256(cl->peer_key.data(), &cl->peer_key_isal);
#endif
        cl->peer_iv_ctr = 0;
    }
    return ok;
}

void osd_messenger_t::init_tls()
{
    if (!tls_cert.empty() || !tls_key.empty() || !osd_tls_ca.empty() || !client_tls_ca.empty())
    {
        if (tls_cert.empty() || tls_key.empty() || osd_tls_ca.empty() || osd_num && client_tls_ca.empty())
        {
            if (osd_num)
                fprintf(stderr, "Vitastor transport encryption requires osd_cert, osd_pkey, osd_ca, client_ca options for OSDs\n");
            else
                fprintf(stderr, "Vitastor transport encryption requires cert, pkey and osd_ca options\n");
            exit(1);
        }
        else
        {
#ifndef __MOCK__
            gcm_enabled = true;
            hs_ctx = msgr_handshake_ctx_i::create_ctx();
            if (!hs_ctx->init(tls_cert, tls_key, osd_tls_ca, client_tls_ca))
            {
                fprintf(stderr, "Error: %s\n", hs_ctx->get_error().c_str());
                exit(1);
            }
#endif
        }
    }
}

void osd_messenger_t::init_tls_client(osd_client_t *cl)
{
    if (gcm_enabled)
    {
        cl->gcm_enabled = true;
        cl->hs = hs_ctx->create();
        cl->hs->init(cl->is_incoming);
        if (cl->hs->get_out().size())
        {
            if (cl->write_state == 0)
            {
                cl->write_state = CL_WRITE_READY;
                write_ready_clients.push_back(cl->client_id);
            }
        }
    }
}

void osd_messenger_t::destroy_tls()
{
#ifdef WITH_ISAL_CRYPTO
    for (isal_gcm_context_data *ctx: encrypt_gcm_pool)
    {
        free(ctx);
    }
    for (isal_gcm_context_data *ctx: decrypt_gcm_pool)
    {
        free(ctx);
    }
#else
    for (EVP_CIPHER_CTX *ctx: encrypt_gcm_pool)
    {
        EVP_CIPHER_CTX_free(ctx);
    }
    for (EVP_CIPHER_CTX *ctx: decrypt_gcm_pool)
    {
        EVP_CIPHER_CTX_free(ctx);
    }
#endif
    if (hs_ctx)
    {
        delete hs_ctx;
        hs_ctx = NULL;
    }
}
