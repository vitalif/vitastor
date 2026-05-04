// Copyright (c) Vitaliy Filippov, 2026+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#pragma once

#include <stdint.h>

#include <string>
#include <vector>

#include <openssl/types.h>

#define AES_256_GCM_KEY_SIZE 32
#define AES_256_GCM_IV_SIZE 12
#define AES_256_GCM_MAX_IV_CTR ((uint64_t)1 << 32)

// TLS 1.3-like handshake

// 1. Client->server: EC public key
// 2. Server->client: EC public key, encrypted certificate and digital signature of the handshake
// 3. Client->server: encrypted certificate and digital signature of the handshake

struct msgr_handshake_result_t
{
    X509 *peer_cert = NULL;
    bool peer_is_osd = false;
    std::vector<uint8_t> shared_secret;
};

class msgr_handshake_i
{
public:
    // Workflow: create -> init -> handle_msg -> get_result/get_error -> destruct
    virtual ~msgr_handshake_i() = default;
    virtual bool init(bool server_mode) = 0;
    virtual ssize_t handle(uint8_t* in_buf, size_t in_size) = 0;
    virtual bool done() = 0;
    virtual uint8_t *get_out() = 0;
    virtual size_t out_size() = 0;
    virtual void eat_out(size_t n) = 0;
    virtual void reset_out() = 0;
    virtual msgr_handshake_result_t get_result() = 0;
    virtual std::string get_error() = 0;
};

class msgr_handshake_ctx_i
{
public:
    static msgr_handshake_ctx_i* create_ctx();
    virtual ~msgr_handshake_ctx_i() = default;
    virtual msgr_handshake_i* create() = 0;
    virtual bool init(const std::string & pem_cert, const std::string & pem_key,
        const std::string & pem_osd_ca, const std::string & pem_client_ca) = 0;
    virtual std::string get_error() = 0;
    virtual bool derive_kdf(const uint8_t* insecret, size_t insecret_len,
        const uint8_t* salt, size_t salt_len, const char *label, uint8_t *outsecret, size_t outsize) = 0;
};
