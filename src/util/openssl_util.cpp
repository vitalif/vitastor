// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include "openssl_util.h"
#include "str_util.h"

#include <openssl/ssl.h>

X509 *openssl_load_cert(const std::string & file_or_pem)
{
    std::string pem;
    BIO *bio = NULL;
    if (file_or_pem.substr(0, 5) != "-----")
    {
        pem = read_file(file_or_pem);
        bio = BIO_new_mem_buf(pem.data(), pem.size());
    }
    else
        bio = BIO_new_mem_buf(file_or_pem.data(), file_or_pem.size());
    if (!bio)
        return NULL;
    X509 *x509 = PEM_read_bio_X509(bio, NULL, 0, NULL);
    BIO_free(bio);
    return x509;
}

EVP_PKEY *openssl_load_key(const std::string & file_or_pem)
{
    std::string pem;
    BIO *bio = NULL;
    if (file_or_pem.substr(0, 5) != "-----")
    {
        pem = read_file(file_or_pem);
        bio = BIO_new_mem_buf(pem.data(), pem.size());
    }
    else
        bio = BIO_new_mem_buf(file_or_pem.data(), file_or_pem.size());
    if (!bio)
        return NULL;
    EVP_PKEY *pkey = PEM_read_bio_PrivateKey(bio, NULL, NULL, NULL);
    BIO_free(bio);
    return pkey;
}

bool openssl_ctx_add_ca(SSL_CTX *ssl_ctx, const std::string & file_or_pem)
{
    X509 *cert = openssl_load_cert(file_or_pem);
    bool ok = !!cert;
    if (cert)
    {
        X509_STORE *store = SSL_CTX_get_cert_store(ssl_ctx);
        X509_STORE_add_cert(store, cert);
        X509_free(cert);
    }
    return ok;
}

bool openssl_ctx_use_ca(SSL_CTX *ssl_ctx, const std::string & file_or_pem)
{
    if (file_or_pem.substr(0, 5) == "-----")
    {
        return openssl_ctx_add_ca(ssl_ctx, file_or_pem);
    }
    return file_or_pem.empty()
        ? !!SSL_CTX_set_default_verify_paths(ssl_ctx)
        : !!SSL_CTX_load_verify_locations(ssl_ctx, file_or_pem.c_str(), NULL);
}

std::string openssl_get_cn(X509 *x509)
{
    X509_NAME* subj = X509_get_subject_name(x509);
    int pos = X509_NAME_get_index_by_NID(subj, NID_commonName, -1);
    if (pos != -1)
    {
        X509_NAME_ENTRY* cn = X509_NAME_get_entry(subj, pos);
        ASN1_STRING* str = X509_NAME_ENTRY_get_data(cn);
        return std::string((const char*)ASN1_STRING_get0_data(str), ASN1_STRING_length(str));
    }
    return "";
}

bool openssl_ctx_use_cert(SSL_CTX *ssl_ctx, const std::string & file_or_pem, std::string & common_name)
{
    X509 *cert = openssl_load_cert(file_or_pem);
    bool ok = false;
    if (cert)
    {
        common_name = openssl_get_cn(cert);
        ok = SSL_CTX_use_certificate(ssl_ctx, cert);
        X509_free(cert);
    }
    return ok;
}

bool openssl_ctx_use_key(SSL_CTX *ssl_ctx, const std::string & file_or_pem)
{
    EVP_PKEY *pkey = openssl_load_key(file_or_pem);
    bool ok = false;
    if (pkey)
    {
        ok = SSL_CTX_use_PrivateKey(ssl_ctx, pkey);
        EVP_PKEY_free(pkey);
    }
    return ok;
}

bool openssl_bio_nonempty(BIO *bio)
{
    // SSL_ERROR_WANT_WRITE is absolutely non-informative with memory BIO, it basically never happens
    // So we have to check memory BIO for outstanding data
    char *bio_buf = NULL;
    size_t bio_sz = BIO_get_mem_data(bio, &bio_buf);
    return bio_sz > 0;
}
