// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#pragma once

#include <string>

#include <openssl/types.h>

X509 *openssl_load_cert(const std::string & file_or_pem);
EVP_PKEY *openssl_load_key(const std::string & file_or_pem);
bool openssl_ctx_add_ca(SSL_CTX *ssl_ctx, const std::string & file_or_pem);
bool openssl_ctx_use_ca(SSL_CTX *ssl_ctx, const std::string & file_or_pem);
std::string openssl_get_cn(X509 *x509);
bool openssl_ctx_use_cert(SSL_CTX *ssl_ctx, const std::string & file_or_pem, std::string & common_name);
bool openssl_ctx_use_key(SSL_CTX *ssl_ctx, const std::string & file_or_pem);
bool openssl_bio_nonempty(BIO *bio);
