// Copyright (c) Vitaliy Filippov, 2026+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include <stdint.h>

#ifdef WITH_ISAL_CRYPTO
#include <isa-l_crypto/aes_xts.h>
#endif

#include "../util/xxh_x86dispatch.h"
#include <openssl/conf.h>
#include <openssl/evp.h>
#include <openssl/err.h>

struct osd_client_t;

class op_aes_xts_encrypt_t
{
#ifndef WITH_ISAL_CRYPTO
    EVP_CIPHER_CTX *ctx = NULL;
#endif
    osd_client_t *cl = NULL;
    uint64_t start_offset = 0;
    uint8_t *key = NULL;
    size_t offset = 0;
    size_t block_size = 0;
    uint8_t *tmp = NULL;
    size_t tmp_size = 0;
    size_t tmp_pos = 0;
    bool encrypted = false;

    void encrypt_block(uint8_t *in, uint8_t *out);

public:
    op_aes_xts_encrypt_t();
    ~op_aes_xts_encrypt_t();

    void start(osd_client_t *cl, uint8_t *key, uint64_t start_offset, size_t block_size);
    void update(uint8_t *in, size_t max_in, uint8_t *out, size_t max_out, size_t & done_in, size_t & done_out);
};

void destroy_aes_xts_encrypt(op_aes_xts_encrypt_t *encrypt_ctx);

class op_aes_xts_decrypt_t
{
#ifndef WITH_ISAL_CRYPTO
    EVP_CIPHER_CTX *ctx = NULL;
#endif
    osd_client_t *cl = NULL;
    uint64_t start_offset = 0;
    uint8_t **key_chain = NULL;
    size_t chain_size = 0;
    void *key_indexes = NULL;
    int key_index_bytes = 0;
    size_t offset = 0;
    size_t block_size = 0;
    uint8_t *tmp = NULL;
    size_t tmp_size = 0;
    size_t tmp_pos = 0;
    bool decrypted = false;

    void decrypt_block(uint8_t *in, uint8_t *out);

public:
    op_aes_xts_decrypt_t();
    ~op_aes_xts_decrypt_t();

    void start(osd_client_t *cl, uint8_t **key_chain, size_t chain_size, void *key_indexes, uint64_t start_offset, size_t block_size);
    void update(uint8_t *in, size_t max_in, uint8_t *out, size_t max_out, size_t & done_in, size_t & done_out);
};

void destroy_aes_xts_decrypt(op_aes_xts_decrypt_t *decrypt_ctx);
