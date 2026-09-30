// Copyright (c) Alexey Shabalin, 2026+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)
// I accept Vitastor CLA: see CLA-en.md for details

// Known-answer test for msgr_handshake_ctx_i::derive_kdf (HKDF-SHA384).
// Keys must be the same with any OpenSSL version, and the salt used for key rotation
// must not affect the next derive_kdf call without salt.

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

#include <string>
#include <vector>

#include "msgr_handshake.h"

// Self-signed EC P-384 certificate, only used to initialize the handshake context
static const char *test_key =
    "-----BEGIN PRIVATE KEY-----\n"
    "MIG2AgEAMBAGByqGSM49AgEGBSuBBAAiBIGeMIGbAgEBBDA0CIkBTlNDaxTDbpc3\n"
    "2JBY4ofK7PfLDw4D2jEEQ/kTQRAXOobqFCjSqefm2Bbmz9yhZANiAAQZEaeZPB3z\n"
    "60xhhxxGYn2/qUgRXcoyPkGBzBXT54esK/qcuumFo1/uoB2+PTNT9lRqYO9s4xhC\n"
    "naDV653Ynp6J8O0AnyhzqDaxHIjG84yWlslTWZMyDvqAtp1ZsIPprSk=\n"
    "-----END PRIVATE KEY-----\n";

static const char *test_cert =
    "-----BEGIN CERTIFICATE-----\n"
    "MIIBwjCCAUegAwIBAgIUMTD1mUZ7KJZYC2m28V+S3Y3M2wkwCgYIKoZIzj0EAwIw\n"
    "GDEWMBQGA1UEAwwNdml0YXN0b3ItdGVzdDAgFw0yNjA5MzAxODM3NDJaGA8yMTI2\n"
    "MDkwNjE4Mzc0MlowGDEWMBQGA1UEAwwNdml0YXN0b3ItdGVzdDB2MBAGByqGSM49\n"
    "AgEGBSuBBAAiA2IABBkRp5k8HfPrTGGHHEZifb+pSBFdyjI+QYHMFdPnh6wr+py6\n"
    "6YWjX+6gHb49M1P2VGpg72zjGEKdoNXrndienonw7QCfKHOoNrEciMbzjJaWyVNZ\n"
    "kzIO+oC2nVmwg+mtKaNQME4wHQYDVR0OBBYEFLkMxzaAAbRTajF+q6HbCWjJtDkP\n"
    "MB8GA1UdIwQYMBaAFLkMxzaAAbRTajF+q6HbCWjJtDkPMAwGA1UdEwQFMAMBAf8w\n"
    "CgYIKoZIzj0EAwIDaQAwZgIxAJ0uH7Y2LmXKnRHSgYLqmpeKFqsvmbph3oWEOErX\n"
    "+NJK56FzMceivmjvMkt4WMa69wIxAO14TLCFZeESLltGwIzdM0hv/Iysiec88ip+\n"
    "OlW2PuLyhw8f/4HHp7Ooyu0ki3AHEA==\n"
    "-----END CERTIFICATE-----\n";

static const char *hs_key_hex =
    "f5ed89f2161b25f4ac4a097179c12008804f2c8cfe757a8d810e166720dc6f67"
    "86f2852a19697bd869551b10";

static const char *key_hex =
    "2b4047bb889aa0e63c5ca73e2edfc8b29dca0887bed9058a390c341bfb8a677c"
    "d632115d5342470a48c76c6094a1e27945dffead049d308c53e3bbaf9a4e71f1"
    "5758e3b3414416f2c9f0ea59235f2a022ad836ff8d8b91ca926f5d5c07ac5dc8"
    "57f3ed36b1cd085b26c2471aa55c314804efeb9da022fd7a25c035cfdb773eb2"
    "75f10d6f2f1551bc53dca1d34b44ba602c3b2e975ddd068732fa5d40ac0302f9"
    "408f357361dd8ba6de9cfb42114663ae3bec06b94fce87227283e28464f27468"
    "1677d78682b142e17fcdfaf6ac24eca4c31b11f3fde6186d5e1850fda7ec6747"
    "7fb2d1e6b1d3998b38373904";

static const char *rotated_key_hex =
    "33b3f43c75660d051ef7465b4e9f61e74c3715af5bb3730a0093931874b0c2f2"
    "73363984f3ae5e7cc6023200bfd3bdff0ee55d6ee10feb78396b0f74663c59f6"
    "394a861c9f1f86b29b5d5fd491e196f05f18576592c86c7c0713ec9fad9e8053"
    "a9113ba72ad56b725e57bf887891abb56fcac346b8f5eb30cc58c255b904ee88"
    "2e7db934efadd32a76e235be842c4a647bcfe4d96a1d11b68d793d7356261bc0"
    "0b10f345a456674855cc6621440a94201dfef0ee724d0913c24f74c3e4f21faf"
    "a263f16f512a84103aeb50507466a2df51c2f35f9c49a27c6697d031a80dcd27"
    "78959c188de53c8a4d757260";

static void fill(uint8_t *buf, size_t len, uint8_t start)
{
    for (size_t i = 0; i < len; i++)
        buf[i] = (uint8_t)(start + i);
}

static std::string to_hex(const uint8_t *buf, size_t len)
{
    std::string r;
    char tmp[3];
    for (size_t i = 0; i < len; i++)
    {
        snprintf(tmp, sizeof(tmp), "%02x", buf[i]);
        r += tmp;
    }
    return r;
}

static void check(msgr_handshake_ctx_i *ctx, const char *name, const uint8_t *secret, size_t secret_len,
    const uint8_t *salt, size_t salt_len, const char *label, size_t out_len, const char *expected)
{
    std::vector<uint8_t> out(out_len);
    if (!ctx->derive_kdf(secret, secret_len, salt, salt_len, label, out.data(), out_len))
    {
        printf("%s: derive_kdf failed\n", name);
        exit(1);
    }
    std::string actual = to_hex(out.data(), out_len);
    if (actual != expected)
    {
        printf("%s: key mismatch\nexpected: %s\nactual:   %s\n", name, expected, actual.c_str());
        exit(1);
    }
}

int main(int narg, char *args[])
{
    // Same sizes as in msgr_handshake_t::derive_hs_keys() and osd_messenger_t::derive_aes_keys()
    const size_t hs_key_len = AES_256_GCM_KEY_SIZE + AES_256_GCM_IV_SIZE;
    const size_t key_len = AES_256_GCM_KEY_SIZE + AES_256_GCM_IV_SIZE + 192 /* XXH_SECRET_DEFAULT_SIZE */;
    uint8_t secret[48], old_key[key_len];
    fill(secret, sizeof(secret), 0x00);
    fill(old_key, sizeof(old_key), 0x80);
    msgr_handshake_ctx_i *ctx = msgr_handshake_ctx_i::create_ctx();
    if (!ctx->init(test_cert, test_key, test_cert, "", ""))
    {
        printf("handshake context init failed: %s\n", ctx->get_error().c_str());
        exit(1);
    }
    check(ctx, "handshake key", secret, sizeof(secret), NULL, 0, "server hs key", hs_key_len, hs_key_hex);
    check(ctx, "initial key", secret, sizeof(secret), NULL, 0, "client key", key_len, key_hex);
    // Key rotation uses the old key as salt
    check(ctx, "rotated key", secret, sizeof(secret), old_key, sizeof(old_key), "client key", key_len, rotated_key_hex);
    // Salt of the previous call must not affect the next call without salt
    check(ctx, "handshake key after rotation", secret, sizeof(secret), NULL, 0, "server hs key", hs_key_len, hs_key_hex);
    delete ctx;
    printf("OK\n");
    return 0;
}
