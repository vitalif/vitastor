// Copyright (c) Vitaliy Filippov, 2026+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#undef NDEBUG
#include <assert.h>
#include <stdint.h>
#include <string.h>

#include <string>
#include <vector>
#include <memory>

#include <openssl/conf.h>
#include <openssl/evp.h>
#include <openssl/obj_mac.h>
#include <openssl/ec.h>
#include <openssl/kdf.h>
#include <openssl/bio.h>
#include <openssl/pem.h>
#include <openssl/err.h>

#include "msgr_handshake.h"
#include "malloc_or_die.h"
#include "openssl_util.h"
#include "str_util.h"

#define AES_256_GCM_KEY_SIZE 32
#define AES_256_GCM_IV_SIZE 12

// TLS 1.3-like handshake

// 1. Client->server: EC public key
// 2. Server->client: EC public key, encrypted certificate and digital signature of the handshake
// 3. Client->server: encrypted certificate and digital signature of the handshake

// "vitaECDH" interleaved
#define MSGR_HS_MAGIC 0x4861447443694576l
#define MSGR_HS_MAX_LEN 131072

#define MSGR_HS_SERVER_INIT 0
#define MSGR_HS_CLIENT_INIT 1
#define MSGR_HS_SERVER_REPLY 2
#define MSGR_HS_CLIENT_REPLY 3
#define MSGR_HS_DONE 100
#define MSGR_HS_ERROR 101

struct __attribute__((__packed__)) msgr_handshake_hdr_t
{
    uint32_t msg_len;
    uint64_t magic;
    uint32_t type;
};

class msgr_handshake_ctx_t: public msgr_handshake_ctx_i
{
    friend class msgr_handshake_t;

    EVP_PKEY_CTX *pctx = NULL;
    EVP_PKEY *params = NULL;
    X509_STORE *ca = NULL;
    X509 *osd_ca = NULL;
    X509 *client_ca = NULL;
    X509 *admin_ca = NULL;
    std::string my_cert_pem;
    X509 *my_cert = NULL;
    EVP_PKEY *my_pubkey = NULL;
    const EVP_MD *md = NULL;
    EVP_PKEY *my_privkey = NULL;
    EVP_KDF_CTX* kdf_ctx = NULL;
    std::string error;

    bool on_error(const std::string & prefix);

public:
    ~msgr_handshake_ctx_t();
    msgr_handshake_i* create() override;
    bool init(const std::string & pem_cert, const std::string & pem_key,
        const std::string & pem_osd_ca, const std::string & pem_client_ca, const std::string & pem_admin_ca) override;
    std::string get_error() override;
    bool derive_kdf(const uint8_t* insecret, size_t insecret_len,
        const uint8_t* salt, size_t salt_len, const char *label, uint8_t *outsecret, size_t outsize) override;
};

class msgr_handshake_t: public msgr_handshake_i
{
    msgr_handshake_ctx_t *ctx = NULL;
    bool is_server = false;

    std::vector<uint8_t> full_handshake;
    std::vector<uint8_t> in_buf;
    uint8_t *out_buf = NULL;
    size_t out_buf_size = 0;
    std::string error;

    int state = 0;

    EVP_PKEY *ec_key = NULL;
    X509 *peer_cert = NULL;
    bool peer_is_osd = false;
    bool peer_is_admin = false;
    std::vector<uint8_t> shared_secret;
    std::vector<uint8_t> hs_key, peer_hs_key;

    msgr_handshake_hdr_t *cur_hdr = NULL;
    uint8_t *cur_buf = NULL;
    size_t cur_left = 0;

    bool on_error(const std::string & prefix);
    bool derive_shared_secret(EVP_PKEY *peer_ec_key);
    bool derive_hs_keys(const uint8_t *encoded_peer_key, size_t encoded_key_len);
    bool sign(std::vector<uint8_t> & out);
    bool verify(const uint8_t *signature, size_t signature_len);
    bool encrypt(const uint8_t* src, size_t len, uint8_t* dest);
    bool decrypt(const uint8_t* src, size_t & len, uint8_t* dest);
    bool make_client_init();
    bool make_server_reply();
    bool make_client_reply();
    bool verify_peer(const uint8_t *peer_cert_pem, size_t peer_cert_len);
    ssize_t start_msg(uint8_t* src, size_t len, uint32_t expected_type);
    bool read_with_len(const uint8_t* & dst, uint32_t & dst_len);
    bool handle_client_init();
    bool handle_server_reply();
    bool handle_client_reply();
    bool handle_peer_cert(const uint8_t *key, uint32_t key_len);
    void complete();

public:
    // Workflow: create -> init -> handle -> get_result/get_error -> destruct
    msgr_handshake_t(msgr_handshake_ctx_t *ctx): ctx(ctx) {}
    ~msgr_handshake_t();
    bool init(bool server_mode) override;
    ssize_t handle(uint8_t* in_buf, size_t in_size) override;
    bool done() override;
    uint8_t *get_out() override;
    size_t out_size() override;
    void eat_out(size_t n) override;
    void reset_out() override;
    msgr_handshake_result_t get_result() override;
    std::string get_error() override;
};

msgr_handshake_ctx_i* msgr_handshake_ctx_i::create_ctx()
{
    return new msgr_handshake_ctx_t();
}

msgr_handshake_i* msgr_handshake_ctx_t::create()
{
    return new msgr_handshake_t(this);
}

bool msgr_handshake_ctx_t::init(const std::string & pem_cert, const std::string & pem_key,
    const std::string & pem_osd_ca, const std::string & pem_client_ca, const std::string & pem_admin_ca)
{
    if (pem_cert.substr(0, 5) == "-----")
        my_cert_pem = pem_cert;
    else
    {
        my_cert_pem = read_file(pem_cert);
        if (my_cert_pem.empty())
        {
            error = "Failed to load certificate file";
            return false;
        }
    }
    {
        BIO *bio = BIO_new_mem_buf(my_cert_pem.data(), my_cert_pem.size());
        if (!bio)
            return on_error("BIO_new_mem_buf: ");
        my_cert = PEM_read_bio_X509(bio, NULL, 0, NULL);
        BIO_free(bio);
        if (!my_cert)
            return on_error("Failed to load certificate: ");
    }
    if (!(my_pubkey = X509_get0_pubkey(my_cert)))
        return on_error("X509_get0_pubkey: ");
    if (!(md = EVP_get_digestbynid(NID_sha384)))
        return on_error("EVP_get_digestbynid SHA384: ");
    if (!(my_privkey = openssl_load_key(pem_key)))
        return on_error("Failed to load private key: ");
    if (!(ca = X509_STORE_new()))
        return on_error("X509_STORE_CTX_new: ");
    if (!(osd_ca = openssl_load_cert(pem_osd_ca)))
        return on_error("Failed to load OSD CA certificate: ");
    if (X509_STORE_add_cert(ca, osd_ca) <= 0)
        return on_error("X509_STORE_add_cert OSD CA: ");
    if (!pem_client_ca.empty() && !(client_ca = openssl_load_cert(pem_client_ca)))
        return on_error("Failed to load client CA certificate: ");
    if (client_ca && X509_STORE_add_cert(ca, client_ca) <= 0)
        return on_error("X509_STORE_add_cert client CA: ");
    if (!pem_admin_ca.empty() && !(admin_ca = openssl_load_cert(pem_admin_ca)))
        return on_error("Failed to load admin CA certificate: ");
    if (admin_ca && X509_STORE_add_cert(ca, admin_ca) <= 0)
        return on_error("X509_STORE_add_cert admin CA: ");
    if (!(pctx = EVP_PKEY_CTX_new_id(EVP_PKEY_EC, NULL)))
        return on_error("EVP_PKEY_CTX_new_id: ");
    if (EVP_PKEY_paramgen_init(pctx) <= 0)
        return on_error("EVP_PKEY_paramgen_init: ");
    if (EVP_PKEY_CTX_set_ec_paramgen_curve_nid(pctx, /*NID_X9_62_prime256v1*/NID_secp384r1) <= 0)
        return on_error("EVP_PKEY_CTX_set_ec_paramgen_curve_nid: ");
    if (EVP_PKEY_paramgen(pctx, &params) <= 0)
        return on_error("EVP_PKEY_paramgen: ");
    EVP_KDF *kdf = EVP_KDF_fetch(NULL, "hkdf", NULL);
    if (!kdf)
        return on_error("EVP_KDF_fetch: ");
    kdf_ctx = EVP_KDF_CTX_new(kdf);
    EVP_KDF_free(kdf);
    return true;
}

std::string msgr_handshake_ctx_t::get_error()
{
    return error;
}

msgr_handshake_ctx_t::~msgr_handshake_ctx_t()
{
    if (pctx)
        EVP_PKEY_CTX_free(pctx);
    if (params)
        EVP_PKEY_free(params);
    if (ca)
        X509_STORE_free(ca);
    if (osd_ca)
        X509_free(osd_ca);
    if (client_ca)
        X509_free(client_ca);
    if (admin_ca)
        X509_free(admin_ca);
    my_pubkey = NULL;
    if (my_cert)
        X509_free(my_cert);
    if (my_privkey)
        EVP_PKEY_free(my_privkey);
    if (kdf_ctx)
        EVP_KDF_CTX_free(kdf_ctx);
}

bool msgr_handshake_ctx_t::on_error(const std::string & prefix)
{
    error = prefix+ERR_error_string(ERR_get_error(), NULL);
    return false;
}

bool msgr_handshake_ctx_t::derive_kdf(const uint8_t* insecret, size_t insecret_len,
    const uint8_t* salt, size_t salt_len, const char *label, uint8_t *outsecret, size_t outsize)
{
    OSSL_PARAM params[5];
    int n = 0;
    params[n++] = OSSL_PARAM_construct_utf8_string("digest", (char*)"sha384", (size_t)7);
    params[n++] = OSSL_PARAM_construct_octet_string("key", (void*)insecret, insecret_len);
    params[n++] = OSSL_PARAM_construct_octet_string("info", (void*)label, strlen(label)+1);
    params[n++] = OSSL_PARAM_construct_octet_string("salt", (salt ? (void*)salt : (void*)""), salt_len);
    params[n++] = OSSL_PARAM_construct_end();
    assert(n <= sizeof(params)/sizeof(OSSL_PARAM));
    if (EVP_KDF_CTX_set_params(kdf_ctx, params) <= 0)
        return false;
    if (EVP_KDF_derive(kdf_ctx, outsecret, outsize, NULL) <= 0)
        return false;
    return true;
}

msgr_handshake_t::~msgr_handshake_t()
{
    if (out_buf)
        free(out_buf);
    if (peer_cert)
        X509_free(peer_cert);
    if (ec_key)
        EVP_PKEY_free(ec_key);
}

bool msgr_handshake_t::on_error(const std::string & prefix)
{
    error = prefix+ERR_error_string(ERR_get_error(), NULL);
    state = MSGR_HS_ERROR;
    return false;
}

bool msgr_handshake_t::init(bool server_mode)
{
    this->ctx = ctx;
    std::unique_ptr<EVP_PKEY_CTX, decltype(&EVP_PKEY_CTX_free)> kctx(EVP_PKEY_CTX_new(ctx->params, NULL), EVP_PKEY_CTX_free);
    if (!kctx)
        return on_error("EVP_PKEY_CTX_new with EC params: ");
    if (EVP_PKEY_keygen_init(kctx.get()) <= 0)
        return on_error("EVP_PKEY_keygen_init: ");
    if (EVP_PKEY_keygen(kctx.get(), &ec_key) <= 0)
        return on_error("EVP_PKEY_keygen: ");
    if (!server_mode)
    {
        // Send initial message - only the EC public key
        if (!make_client_init())
            return false;
    }
    this->is_server = server_mode;
    this->state = server_mode ? MSGR_HS_SERVER_INIT : MSGR_HS_CLIENT_INIT;
    return true;
}

static void copy_to(std::vector<uint8_t> & buf, const void* src, uint32_t len)
{
    size_t old_size = buf.size();
    buf.resize(buf.size() + len);
    memcpy(buf.data() + old_size, src, len);
}

static void copy_to_raw(uint8_t* & buf, const void* src, size_t len)
{
    memcpy(buf, src, len);
    buf += len;
}

static void copy_to_with_len(std::vector<uint8_t> & buf, const void* src, uint32_t len)
{
    copy_to(buf, &len, sizeof(len));
    copy_to(buf, src, len);
}

bool msgr_handshake_t::derive_shared_secret(EVP_PKEY *peer_ec_key)
{
    EVP_PKEY_CTX *dh_ctx = NULL;
    if (!(dh_ctx = EVP_PKEY_CTX_new(ec_key, NULL)))
        return on_error("EVP_PKEY_CTX_new for ECDH: ");
    if (!EVP_PKEY_derive_init(dh_ctx))
    {
        EVP_PKEY_CTX_free(dh_ctx);
        return on_error("EVP_PKEY_derive_init: ");
    }
    if (!EVP_PKEY_derive_set_peer(dh_ctx, peer_ec_key))
    {
        EVP_PKEY_CTX_free(dh_ctx);
        return on_error("EVP_PKEY_derive_set_peer: ");
    }
    size_t len = 0;
    if (!EVP_PKEY_derive(dh_ctx, NULL, &len))
    {
        EVP_PKEY_CTX_free(dh_ctx);
        return on_error("EVP_PKEY_derive get length: ");
    }
    shared_secret.resize(len);
    assert(len == 48);
    if (!EVP_PKEY_derive(dh_ctx, shared_secret.data(), &len))
    {
        EVP_PKEY_CTX_free(dh_ctx);
        return on_error("EVP_PKEY_derive: ");
    }
    assert(len == shared_secret.size());
    shared_secret.resize(len);
    EVP_PKEY_CTX_free(dh_ctx);
    return true;
}

bool msgr_handshake_t::derive_hs_keys(const uint8_t *encoded_peer_key, size_t encoded_key_len)
{
    std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> peer_ec_key(EVP_PKEY_new(), EVP_PKEY_free);
    if (!peer_ec_key)
        return on_error("EVP_PKEY_new: ");
    if (EVP_PKEY_copy_parameters(peer_ec_key.get(), ec_key) <= 0)
        return on_error("EVP_PKEY_copy_parameters: ");
    if (EVP_PKEY_set1_encoded_public_key(peer_ec_key.get(), encoded_peer_key, encoded_key_len) <= 0)
        return on_error("Invalid handshake peer key: ");
    if (!derive_shared_secret(peer_ec_key.get()))
        return false;
    hs_key.resize(AES_256_GCM_KEY_SIZE + AES_256_GCM_IV_SIZE);
    peer_hs_key.resize(AES_256_GCM_KEY_SIZE + AES_256_GCM_IV_SIZE);
    if (!ctx->derive_kdf(shared_secret.data(), shared_secret.size(),
        NULL, 0, (state == MSGR_HS_SERVER_INIT ? "server hs key" : "client hs key"),
        hs_key.data(), hs_key.size()))
        return on_error("derive_kdf: ");
    if (!ctx->derive_kdf(shared_secret.data(), shared_secret.size(),
        NULL, 0, (state != MSGR_HS_SERVER_INIT ? "server hs key" : "client hs key"),
        peer_hs_key.data(), peer_hs_key.size()))
        return on_error("derive_kdf: ");
    return true;
}

bool msgr_handshake_t::sign(std::vector<uint8_t> & out)
{
    std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> md_ctx(EVP_MD_CTX_new(), EVP_MD_CTX_free);
    if (!md_ctx)
        return on_error("EVP_MD_CTX_create: ");
    if (EVP_DigestSignInit(md_ctx.get(), NULL, ctx->md, NULL, ctx->my_privkey) <= 0)
        return on_error("EVP_DigestSignInit: ");
    if (EVP_DigestSignUpdate(md_ctx.get(), full_handshake.data(), full_handshake.size()) <= 0)
        return on_error("EVP_DigestSignUpdate: ");
    size_t siglen = 0;
    if (EVP_DigestSignFinal(md_ctx.get(), NULL, &siglen) <= 0)
        return on_error("EVP_DigestSignFinal get length: ");
    size_t oldsize = out.size();
    out.resize(oldsize + siglen);
    if (EVP_DigestSignFinal(md_ctx.get(), out.data() + oldsize, &siglen) <= 0)
        return on_error("EVP_DigestSignFinal: ");
    out.resize(oldsize + siglen);
    return true;
}

bool msgr_handshake_t::verify(const uint8_t *signature, size_t signature_len)
{
    std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> md_ctx(EVP_MD_CTX_new(), EVP_MD_CTX_free);
    if (!md_ctx)
        return on_error("EVP_MD_CTX_create: ");
    if (EVP_DigestVerifyInit(md_ctx.get(), NULL, ctx->md, NULL, X509_get0_pubkey(peer_cert)) <= 0)
        return on_error("EVP_DigestVerifyInit: ");
    if (EVP_DigestVerifyUpdate(md_ctx.get(), full_handshake.data(), full_handshake.size()) <= 0)
        return on_error("EVP_DigestVerifyUpdate: ");
    if (EVP_DigestVerifyFinal(md_ctx.get(), signature, signature_len) <= 0)
        return false;
    return true;
}

bool msgr_handshake_t::encrypt(const uint8_t* src, size_t len, uint8_t* dest)
{
    std::unique_ptr<EVP_CIPHER_CTX, decltype(&EVP_CIPHER_CTX_free)> enc_ctx(EVP_CIPHER_CTX_new(), EVP_CIPHER_CTX_free);
    if (!ctx)
        return on_error("EVP_CIPHER_CTX_new: ");
    if (EVP_EncryptInit_ex(enc_ctx.get(), EVP_aes_256_gcm(), NULL, hs_key.data(), hs_key.data() + AES_256_GCM_KEY_SIZE) <= 0)
        return on_error("EVP_EncryptInit AES-256-GCM: ");
    int actual_out;
    if (EVP_EncryptUpdate(enc_ctx.get(), dest, &actual_out, src, len) <= 0)
        return on_error("EVP_EncryptUpdate: ");
    assert(actual_out == len);
    if (EVP_EncryptFinal_ex(enc_ctx.get(), NULL, &actual_out) <= 0)
        return on_error("EVP_EncryptFinal: ");
    if (EVP_CIPHER_CTX_ctrl(enc_ctx.get(), EVP_CTRL_GCM_GET_TAG, 16, dest+len) <= 0)
        return on_error("EVP_CTRL_GCM_GET_TAG: ");
    (*(uint64_t*)(hs_key.data() + AES_256_GCM_KEY_SIZE))++; // change IV
    return true;
}

bool msgr_handshake_t::decrypt(const uint8_t *src, size_t & len, uint8_t* dest)
{
    if (len <= 16) // only tag?!
    {
        len = 0;
        error = "Handshake decryption failed";
        state = MSGR_HS_ERROR;
        return false;
    }
    std::unique_ptr<EVP_CIPHER_CTX, decltype(&EVP_CIPHER_CTX_free)> dec_ctx(EVP_CIPHER_CTX_new(), EVP_CIPHER_CTX_free);
    if (!ctx)
        return on_error("EVP_CIPHER_CTX_new: ");
    if (EVP_DecryptInit_ex(dec_ctx.get(), EVP_aes_256_gcm(), NULL, peer_hs_key.data(), peer_hs_key.data() + AES_256_GCM_KEY_SIZE) <= 0)
        return on_error("EVP_DecryptInit AES-256-GCM: ");
    int actual_out;
    len -= 16;
    if (EVP_DecryptUpdate(dec_ctx.get(), dest, &actual_out, src, len) <= 0)
        return on_error("EVP_DecryptUpdate: ");
    assert(actual_out == len);
    if (EVP_CIPHER_CTX_ctrl(dec_ctx.get(), EVP_CTRL_GCM_SET_TAG, 16, (void*)(src+len)) <= 0)
        return on_error("EVP_CTRL_GCM_SET_TAG: ");
    if (EVP_DecryptFinal_ex(dec_ctx.get(), NULL, &actual_out) <= 0)
    {
        error = "Handshake decryption failed";
        state = MSGR_HS_ERROR;
        return false;
    }
    (*(uint64_t*)(peer_hs_key.data() + AES_256_GCM_KEY_SIZE))++; // change IV
    return true;
}

bool msgr_handshake_t::make_client_init()
{
    uint8_t *key = NULL;
    size_t key_len = EVP_PKEY_get1_encoded_public_key(ec_key, &key);
    if (!key_len)
        return on_error("EVP_PKEY_get1_encoded_public_key: ");
    const size_t old_out_size = out_buf_size;
    out_buf_size += key_len + sizeof(msgr_handshake_hdr_t);
    out_buf = (uint8_t*)realloc_or_die(out_buf, out_buf_size);
    uint8_t *buf = out_buf + old_out_size;
    msgr_handshake_hdr_t *hdr = (msgr_handshake_hdr_t *)buf;
    hdr->msg_len = key_len + sizeof(msgr_handshake_hdr_t);
    hdr->magic = MSGR_HS_MAGIC;
    hdr->type = MSGR_HS_CLIENT_INIT;
    memcpy(buf + sizeof(msgr_handshake_hdr_t), key, key_len);
    copy_to(full_handshake, &hdr->type, sizeof(hdr->type));
    copy_to_with_len(full_handshake, key, key_len);
    OPENSSL_free(key);
    return true;
}

bool msgr_handshake_t::make_server_reply()
{
    uint8_t *key = NULL;
    size_t key_len = EVP_PKEY_get1_encoded_public_key(ec_key, &key);
    if (!key_len)
        return on_error("EVP_PKEY_get1_encoded_public_key: ");
    // Append type, key and raw certificate to signed data and sign it
    msgr_handshake_hdr_t hdr = { .magic = MSGR_HS_MAGIC, .type = MSGR_HS_SERVER_REPLY };
    copy_to(full_handshake, &hdr.type, sizeof(hdr.type));
    copy_to_with_len(full_handshake, key, key_len);
    copy_to_with_len(full_handshake, ctx->my_cert_pem.data(), ctx->my_cert_pem.size());
    std::vector<uint8_t> signature;
    if (!sign(signature))
    {
        OPENSSL_free(key);
        return false;
    }
    // Encrypt certificate and signature
    std::vector<uint8_t> encrypt_data;
    copy_to_with_len(encrypt_data, ctx->my_cert_pem.data(), ctx->my_cert_pem.size());
    copy_to_with_len(encrypt_data, signature.data(), signature.size());
    encrypt_data.resize(encrypt_data.size()+16);
    if (!encrypt(encrypt_data.data(), encrypt_data.size()-16, encrypt_data.data()))
    {
        OPENSSL_free(key);
        return false;
    }
    // Construct message
    hdr.msg_len = sizeof(msgr_handshake_hdr_t) + 4 + key_len + encrypt_data.size();
    out_buf = (uint8_t*)realloc_or_die(out_buf, (out_buf_size += hdr.msg_len));
    uint8_t *cur = out_buf + out_buf_size - hdr.msg_len;
    copy_to_raw(cur, &hdr, sizeof(hdr));
    copy_to_raw(cur, &key_len, 4);
    copy_to_raw(cur, key, key_len);
    copy_to_raw(cur, encrypt_data.data(), encrypt_data.size());
    OPENSSL_free(key);
    return true;
}

bool msgr_handshake_t::make_client_reply()
{
    // Append type and raw certificate to signed data and sign it
    msgr_handshake_hdr_t hdr = { .magic = MSGR_HS_MAGIC, .type = MSGR_HS_CLIENT_REPLY };
    copy_to(full_handshake, &hdr.type, sizeof(hdr.type));
    copy_to_with_len(full_handshake, ctx->my_cert_pem.data(), ctx->my_cert_pem.size());
    std::vector<uint8_t> signature;
    if (!sign(signature))
        return false;
    // Encrypt certificate and signature
    std::vector<uint8_t> encrypt_data;
    copy_to_with_len(encrypt_data, ctx->my_cert_pem.data(), ctx->my_cert_pem.size());
    copy_to_with_len(encrypt_data, signature.data(), signature.size());
    encrypt_data.resize(encrypt_data.size()+16);
    if (!encrypt(encrypt_data.data(), encrypt_data.size()-16, encrypt_data.data()))
        return false;
    // Construct message
    hdr.msg_len = sizeof(msgr_handshake_hdr_t) + encrypt_data.size();
    out_buf = (uint8_t*)realloc_or_die(out_buf, (out_buf_size += hdr.msg_len));
    uint8_t *cur = out_buf + out_buf_size - hdr.msg_len;
    copy_to_raw(cur, &hdr, sizeof(hdr));
    copy_to_raw(cur, encrypt_data.data(), encrypt_data.size());
    return true;
}

bool msgr_handshake_t::verify_peer(const uint8_t *peer_cert_pem, size_t peer_cert_len)
{
    BIO *bio = BIO_new_mem_buf(peer_cert_pem, peer_cert_len);
    if (!bio)
        return on_error("BIO_new_mem_buf: ");
    peer_cert = PEM_read_bio_X509(bio, NULL, 0, NULL);
    BIO_free(bio);
    if (!peer_cert)
    {
        error = "Invalid peer certificate";
        state = MSGR_HS_ERROR;
        return false;
    }
    std::unique_ptr<X509_STORE_CTX, decltype(&X509_STORE_CTX_free)> ca_ctx(X509_STORE_CTX_new(), X509_STORE_CTX_free);
    if (!ca_ctx)
        return on_error("X509_STORE_CTX_new: ");
    if (X509_STORE_CTX_init(ca_ctx.get(), ctx->ca, peer_cert, NULL) <= 0)
        return on_error("X509_STORE_CTX_init: ");
    // Maybe use X509_VERIFY_PARAM_set_auth_level(X509_STORE_CTX_get0_param(ca_ctx.get()), 2) ?
    X509_STORE_CTX_set_default(ca_ctx.get(), is_server ? "ssl_client" : "ssl_server");
    if (X509_verify_cert(ca_ctx.get()) <= 0)
    {
        error = "Peer certificate verification failed: ";
        error += X509_verify_cert_error_string(X509_STORE_CTX_get_error(ca_ctx.get()));
        state = MSGR_HS_ERROR;
        return false;
    }
    auto peer_hash = X509_issuer_name_hash(peer_cert);
    peer_is_osd = (peer_hash == X509_subject_name_hash(ctx->osd_ca) &&
        X509_verify(peer_cert, X509_get0_pubkey(ctx->osd_ca)) > 0);
    if (is_server && ctx->admin_ca)
    {
        peer_is_admin = (peer_hash == X509_subject_name_hash(ctx->admin_ca) &&
            X509_verify(peer_cert, X509_get0_pubkey(ctx->admin_ca)) > 0);
    }
    if (!is_server && !peer_is_osd)
    {
        error = "Peer is not an OSD";
        state = MSGR_HS_ERROR;
        return false;
    }
    return true;
}

ssize_t msgr_handshake_t::start_msg(uint8_t* src, size_t len, uint32_t expected_type)
{
    size_t orig_len = len;
    size_t to_buffer = (len < sizeof(msgr_handshake_hdr_t)-in_buf.size()
        ? len : sizeof(msgr_handshake_hdr_t)-in_buf.size());
    in_buf.insert(in_buf.end(), src, src+to_buffer);
    len -= to_buffer;
    src += to_buffer;
    if (in_buf.size() < sizeof(msgr_handshake_hdr_t))
        return 0;
    cur_hdr = (msgr_handshake_hdr_t *)in_buf.data();
    if (cur_hdr->magic != MSGR_HS_MAGIC ||
        cur_hdr->type != expected_type ||
        cur_hdr->msg_len <= sizeof(msgr_handshake_hdr_t) ||
        cur_hdr->msg_len >= MSGR_HS_MAX_LEN)
    {
        error = "Invalid handshake packet magic, type or size";
        state = MSGR_HS_ERROR;
        return -1;
    }
    to_buffer = (len < cur_hdr->msg_len-in_buf.size()
        ? len : cur_hdr->msg_len-in_buf.size());
    in_buf.insert(in_buf.end(), src, src+to_buffer);
    cur_hdr = (msgr_handshake_hdr_t *)in_buf.data();
    len -= to_buffer;
    src += to_buffer;
    if (in_buf.size() < cur_hdr->msg_len)
        return 0;
    cur_left = cur_hdr->msg_len - sizeof(msgr_handshake_hdr_t);
    cur_buf = in_buf.data() + sizeof(msgr_handshake_hdr_t);
    return orig_len - len;
}

bool msgr_handshake_t::read_with_len(const uint8_t* & dst, uint32_t & dst_len)
{
    if (cur_left < 4)
    {
        error = "Handshake packet too short";
        state = MSGR_HS_ERROR;
        return false;
    }
    dst_len = *(uint32_t*)cur_buf;
    cur_buf += 4;
    cur_left -= 4;
    if (cur_left < dst_len)
    {
        error = "Handshake packet too short";
        state = MSGR_HS_ERROR;
        return false;
    }
    dst = cur_buf;
    cur_buf += dst_len;
    cur_left -= dst_len;
    return true;
}

bool msgr_handshake_t::handle_client_init()
{
    // Derive shared secret and handshake keys
    if (!derive_hs_keys(cur_buf, cur_left))
        return false;
    // Add type and key to full_handshake
    copy_to(full_handshake, &cur_hdr->type, sizeof(cur_hdr->type));
    copy_to_with_len(full_handshake, cur_buf, cur_left);
    in_buf.clear();
    return true;
}

// Decrypt and check peer certificate
bool msgr_handshake_t::handle_peer_cert(const uint8_t *key, uint32_t key_len)
{
    if (!decrypt(cur_buf, cur_left, cur_buf))
        return false;
    const uint8_t *peer_cert_pem = NULL;
    uint32_t peer_cert_len = 0;
    if (!read_with_len(peer_cert_pem, peer_cert_len))
        return false;
    // Parse and verify certificate
    if (!verify_peer(peer_cert_pem, peer_cert_len))
        return false;
    // Verify signature
    copy_to(full_handshake, &cur_hdr->type, sizeof(cur_hdr->type));
    if (key)
        copy_to_with_len(full_handshake, key, key_len);
    copy_to_with_len(full_handshake, peer_cert_pem, peer_cert_len);
    const uint8_t *signature = NULL;
    uint32_t signature_len = 0;
    if (!read_with_len(signature, signature_len))
        return false;
    if (!verify(signature, signature_len))
        return false;
    return true;
}

bool msgr_handshake_t::handle_server_reply()
{
    // Derive shared secret and handshake keys
    const uint8_t *key = NULL;
    uint32_t key_len = 0;
    if (!read_with_len(key, key_len))
        return false;
    if (!derive_hs_keys(key, key_len))
        return false;
    // Decrypt and check peer certificate
    if (!handle_peer_cert(key, key_len))
        return false;
    in_buf.clear();
    return true;
}

bool msgr_handshake_t::handle_client_reply()
{
    // Decrypt and check peer certificate
    if (!handle_peer_cert(NULL, 0))
        return false;
    in_buf.clear();
    return true;
}

void msgr_handshake_t::complete()
{
    state = MSGR_HS_DONE;
    hs_key.clear();
    peer_hs_key.clear();
    full_handshake.clear();
}

ssize_t msgr_handshake_t::handle(uint8_t* in_buf, size_t in_size)
{
    if (state == MSGR_HS_SERVER_INIT)
    {
        ssize_t r = start_msg(in_buf, in_size, MSGR_HS_CLIENT_INIT);
        if (r < 0)
            return r;
        if (r == 0)
            return in_size;
        if (!handle_client_init())
            return -1;
        // Send encrypted & signed response
        if (!make_server_reply())
            return -1;
        state = MSGR_HS_SERVER_REPLY;
        return r;
    }
    else if (state == MSGR_HS_CLIENT_INIT)
    {
        ssize_t r = start_msg(in_buf, in_size, MSGR_HS_SERVER_REPLY);
        if (r < 0)
            return r;
        if (r == 0)
            return in_size;
        if (!handle_server_reply())
            return -1;
        // Verification passed, send certificate to the server
        if (!make_client_reply())
            return -1;
        // Finished!
        complete();
        return r;
    }
    else if (state == MSGR_HS_SERVER_REPLY)
    {
        ssize_t r = start_msg(in_buf, in_size, MSGR_HS_CLIENT_REPLY);
        if (r < 0)
            return r;
        if (r == 0)
            return in_size;
        if (!handle_client_reply())
            return -1;
        // Verification passed
        // Finished!
        complete();
        return r;
    }
    else if (state == MSGR_HS_DONE)
    {
        return 0;
    }
    else if (state != MSGR_HS_ERROR)
    {
        error = "Unexpected handshake state: "+std::to_string(state);
    }
    return -1;
}

bool msgr_handshake_t::done()
{
    return (state == MSGR_HS_DONE);
}

uint8_t *msgr_handshake_t::get_out()
{
    return out_buf;
}

size_t msgr_handshake_t::out_size()
{
    return out_buf_size;
}

void msgr_handshake_t::eat_out(size_t n)
{
    if (n >= out_buf_size)
    {
        free(out_buf);
        out_buf = NULL;
        out_buf_size = 0;
    }
    else
    {
        memmove(out_buf, out_buf + n, out_buf_size - n);
        out_buf_size -= n;
    }
}

void msgr_handshake_t::reset_out()
{
    out_buf = NULL;
    out_buf_size = 0;
}

msgr_handshake_result_t msgr_handshake_t::get_result()
{
    if (state != MSGR_HS_DONE)
        return msgr_handshake_result_t{};
    X509_up_ref(peer_cert);
    return msgr_handshake_result_t{
        .peer_cert = peer_cert,
        .peer_is_osd = peer_is_osd,
        .peer_is_admin = peer_is_admin,
        .shared_secret = shared_secret,
    };
}

std::string msgr_handshake_t::get_error()
{
    return error;
}
