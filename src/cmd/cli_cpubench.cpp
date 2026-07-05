// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <openssl/rand.h>
#include "cli.h"
#include "messenger.h"
#include "msgr_encrypt.h"
#include "msgr_op.h"
#include "str_util.h"
#include "xxhash.h"
#include "xxh_x86dispatch.h"

// Prevent the compiler from proving that memory is unused.
static inline void clobber_memory(const void* p, size_t n)
{
#if defined(__GNUC__) || defined(__clang__)
    __asm__ __volatile__("" : : "r"(p), "m"(*(const char(*)[1])p) : "memory");
#else
    (void)p; (void)n;
#endif
}

void init_gcm(osd_client_t *cl)
{
    cl->my_key.resize(AES_256_GCM_KEY_SIZE+AES_256_GCM_IV_SIZE);
    RAND_bytes(cl->my_key.data(), AES_256_GCM_KEY_SIZE+AES_256_GCM_IV_SIZE);
#ifdef WITH_ISAL_CRYPTO
    cl->enc_ctx = (isal_gcm_context_data*)malloc_or_die(sizeof(isal_gcm_context_data));
    isal_aes_gcm_pre_256(cl->my_key.data(), &cl->my_key_isal);
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

void init_gcm_round(osd_client_t *cl)
{
#ifdef WITH_ISAL_CRYPTO
    int r = isal_aes_gcm_init_256(&cl->my_key_isal, cl->enc_ctx, cl->my_key.data() + AES_256_GCM_KEY_SIZE, NULL, 0);
    if (r != 0)
    {
        fprintf(stderr, "isal_aes_gcm_init_256 error %d\n", r);
        abort();
    }
#else
    int r = EVP_EncryptInit_ex(cl->enc_ctx, NULL, NULL, (uint8_t*)cl->my_key.data(), cl->my_key.data() + AES_256_GCM_KEY_SIZE);
    if (r != 1)
    {
        fprintf(stderr, "EncryptInit error: ");
        ERR_print_errors_fp(stderr);
        abort();
    }
#endif
}

void finalize_gcm_round(osd_client_t *cl)
{
    uint8_t tag[16];
#ifdef WITH_ISAL_CRYPTO
    int r = isal_aes_gcm_enc_256_finalize(&cl->my_key_isal, cl->enc_ctx, tag, 16);
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
    r = EVP_CIPHER_CTX_ctrl(cl->enc_ctx, EVP_CTRL_GCM_GET_TAG, 16, tag);
    assert(r == 1);
#endif
    clobber_memory(&tag, sizeof(tag));
}

void encrypt_gcm(osd_client_t *cl, uint8_t *in_buf, uint8_t *out_buf, size_t bufsize)
{
#ifdef WITH_ISAL_CRYPTO
    int r = isal_aes_gcm_enc_256_update(&cl->my_key_isal, cl->enc_ctx, out_buf, in_buf, bufsize);
    assert(!r);
#else
    int actual_out;
    if (EVP_EncryptUpdate(cl->enc_ctx, out_buf, &actual_out, in_buf, bufsize) != 1)
    {
        fprintf(stderr, "EncryptUpdate error: ");
        ERR_print_errors_fp(stderr);
        abort();
    }
    assert(actual_out == bufsize);
#endif
}

void bench_aes_xts(uint64_t millis, size_t bufsize, int csum_status, bool quiet, int json)
{
    size_t check_interval = 100;
    if (!quiet && !json)
    {
        printf("%s %s block... ",
            csum_status == MSGR_CSUM_GCM ? "AES-256-XTS + AES-256-GCM encrypt" :
                (csum_status == MSGR_CSUM_PAYLOAD ? "AES-256-GCM encrypt header + AES-256-XTS encrypt + xxhash3" :
                (csum_status == MSGR_CSUM_FULL ? "AES-256-XTS encrypt + xxhash3" : "AES-256-XTS encrypt")),
            format_size(bufsize).c_str());
    }
    uint8_t xts_key[AES_256_XTS_KEY_SIZE];
    RAND_bytes(xts_key, AES_256_XTS_KEY_SIZE);
    uint8_t *in_buf = (uint8_t*)malloc_or_die(bufsize);
    uint8_t *out_buf = (uint8_t*)malloc_or_die(bufsize);
    XXH3_state_t *hash_state = NULL;
    osd_client_t *cl = new osd_client_t();
    cl->proto_csum_status = csum_status;
    if (csum_status == MSGR_CSUM_GCM || csum_status == MSGR_CSUM_PAYLOAD)
    {
        init_gcm(cl);
    }
    if (csum_status == MSGR_CSUM_PAYLOAD || csum_status == MSGR_CSUM_FULL)
    {
        hash_state = XXH3_createState();
    }
    op_aes_xts_encrypt_t enc;
    timespec tv_begin, tv_end;
    clock_gettime(CLOCK_REALTIME, &tv_begin);
    uint64_t iters = 0;
    while (true)
    {
        if (csum_status == MSGR_CSUM_GCM)
        {
            init_gcm_round(cl);
        }
        if (csum_status == MSGR_CSUM_PAYLOAD || csum_status == MSGR_CSUM_FULL)
        {
            XXH3_64bits_reset(hash_state);
        }
        if (csum_status == MSGR_CSUM_PAYLOAD)
        {
            init_gcm_round(cl);
            encrypt_gcm(cl, in_buf, out_buf, OSD_PACKET_SIZE);
        }
        enc.start(cl, xts_key, 0, 4096);
        size_t done_in = 0, done_out = 0;
        while (done_in < bufsize || done_out < bufsize)
        {
            enc.update(in_buf, bufsize, out_buf, bufsize, done_in, done_out);
        }
        if (csum_status == MSGR_CSUM_PAYLOAD || csum_status == MSGR_CSUM_FULL)
        {
            XXH3_64bits_update(hash_state, in_buf, bufsize);
        }
        if (csum_status == MSGR_CSUM_GCM)
        {
            finalize_gcm_round(cl);
        }
        else if (csum_status)
        {
            uint64_t hash = XXH3_64bits_digest(hash_state);
            clobber_memory(&hash, sizeof(hash));
            if (csum_status == MSGR_CSUM_PAYLOAD)
            {
                encrypt_gcm(cl, (uint8_t*)&hash, out_buf+OSD_PACKET_SIZE, sizeof(hash));
                finalize_gcm_round(cl);
            }
        }
        if (!(iters % check_interval))
        {
            clock_gettime(CLOCK_REALTIME, &tv_end);
            uint64_t passed = tv_end.tv_sec*1000 - tv_begin.tv_sec*1000 + tv_end.tv_nsec/1000000 - tv_begin.tv_nsec/1000000;
            if (passed >= millis)
                break;
            else if (passed < 10)
                check_interval *= 10;
        }
        iters++;
    }
    uint64_t result_ms = tv_end.tv_sec*1000 - tv_begin.tv_sec*1000 + tv_end.tv_nsec/1000000 - tv_begin.tv_nsec/1000000;
    double result_mbps = 1000.0 * bufsize / 1048576 * iters / result_ms;
    if (!quiet)
    {
        if (json)
        {
            printf(
                "%s{ \"xxhash3\": %s, \"aes-256-xts\": true, \"aes-256-gcm\": %s, \"bufsize\": %zu, \"iters\": %ju, \"ms\": %ju, \"mbps\": %.2f }",
                json == 1 ? "" : ",\n    ",
                csum_status == MSGR_CSUM_PAYLOAD || csum_status == MSGR_CSUM_FULL ? "true" : "false",
                csum_status == MSGR_CSUM_PAYLOAD ? "\"header\"" : (csum_status == MSGR_CSUM_GCM ? "\"full\"" : "\"none\""),
                bufsize, iters, result_ms, result_mbps
            );
        }
        else
            printf("%ju iterations in %ju ms = %.2f MB/s\n", iters, result_ms, result_mbps);
    }
    if (csum_status == MSGR_CSUM_PAYLOAD || csum_status == MSGR_CSUM_FULL)
    {
        XXH3_freeState(hash_state);
        hash_state = NULL;
    }
    delete cl;
    free(out_buf);
    free(in_buf);
}

void bench_aes_gcm(uint64_t millis, size_t bufsize, bool with_csum, int json)
{
    size_t check_interval = 100;
    if (!json)
    {
        printf("%s %s block... ",
            with_csum ? "AES-256-GCM encrypt header + xxhash3" : "AES-256-GCM encrypt header and",
            format_size(bufsize).c_str());
    }
    uint8_t *in_buf = (uint8_t*)malloc_or_die(bufsize);
    uint8_t *out_buf = (uint8_t*)malloc_or_die(bufsize);
    XXH3_state_t *hash_state = NULL;
    osd_client_t *cl = new osd_client_t();
    init_gcm(cl);
    if (with_csum)
    {
        hash_state = XXH3_createState();
    }
    timespec tv_begin, tv_end;
    clock_gettime(CLOCK_REALTIME, &tv_begin);
    uint64_t iters = 0;
    while (true)
    {
        init_gcm_round(cl);
        encrypt_gcm(cl, in_buf, out_buf, OSD_PACKET_SIZE);
        if (with_csum)
        {
            XXH3_64bits_reset(hash_state);
            XXH3_64bits_update(hash_state, in_buf, bufsize);
            uint64_t hash = XXH3_64bits_digest(hash_state);
            encrypt_gcm(cl, (uint8_t*)&hash, out_buf+OSD_PACKET_SIZE, sizeof(hash));
        }
        else
        {
            encrypt_gcm(cl, in_buf, out_buf, bufsize);
        }
        finalize_gcm_round(cl);
        if (!(iters % check_interval))
        {
            clock_gettime(CLOCK_REALTIME, &tv_end);
            uint64_t passed = tv_end.tv_sec*1000 - tv_begin.tv_sec*1000 + tv_end.tv_nsec/1000000 - tv_begin.tv_nsec/1000000;
            if (passed >= millis)
                break;
            else if (passed < 10)
                check_interval *= 10;
        }
        iters++;
    }
    uint64_t result_ms = tv_end.tv_sec*1000 - tv_begin.tv_sec*1000 + tv_end.tv_nsec/1000000 - tv_begin.tv_nsec/1000000;
    double result_mbps = 1000.0 * bufsize / 1048576 * iters / result_ms;
    if (json)
    {
        printf(
            "%s{ \"xxhash3\": %s, \"aes-256-xts\": false, \"aes-256-gcm\": %s, \"bufsize\": %zu, \"iters\": %ju, \"ms\": %ju, \"mbps\": %.2f }",
            json == 1 ? "" : ",\n    ",
            with_csum ? "true" : "false",
            with_csum ? "\"header\"" : "\"full\"",
            bufsize, iters, result_ms, result_mbps
        );
    }
    else
    {
        printf("%ju iterations in %ju ms = %.2f MB/s\n", iters, result_ms, result_mbps);
    }
    if (with_csum)
    {
        XXH3_freeState(hash_state);
        hash_state = NULL;
    }
    delete cl;
    free(out_buf);
    free(in_buf);
}

void bench_xxh(uint64_t millis, size_t bufsize, int json)
{
    size_t check_interval = 100;
    if (!json)
    {
        printf("xxhash3 %s block... ", format_size(bufsize).c_str());
    }
    uint8_t *in_buf = (uint8_t*)malloc_or_die(bufsize);
    XXH3_state_t *hash_state = XXH3_createState();
    timespec tv_begin, tv_end;
    clock_gettime(CLOCK_REALTIME, &tv_begin);
    uint64_t iters = 0;
    while (true)
    {
        XXH3_64bits_reset(hash_state);
        XXH3_64bits_update(hash_state, in_buf, bufsize);
        uint64_t hash = XXH3_64bits_digest(hash_state);
        clobber_memory(&hash, sizeof(hash));
        if (!(iters % check_interval))
        {
            clock_gettime(CLOCK_REALTIME, &tv_end);
            uint64_t passed = tv_end.tv_sec*1000 - tv_begin.tv_sec*1000 + tv_end.tv_nsec/1000000 - tv_begin.tv_nsec/1000000;
            if (passed >= millis)
                break;
            else if (passed < 10)
                check_interval *= 10;
        }
        iters++;
    }
    uint64_t result_ms = tv_end.tv_sec*1000 - tv_begin.tv_sec*1000 + tv_end.tv_nsec/1000000 - tv_begin.tv_nsec/1000000;
    double result_mbps = 1000.0 * bufsize / 1048576 * iters / result_ms;
    if (json)
    {
        printf(
            "%s{ \"xxhash3\": true, \"aes-256-xts\": false, \"aes-256-gcm\": \"none\", \"bufsize\": %zu, \"iters\": %ju, \"ms\": %ju, \"mbps\": %.2f }",
            json == 1 ? "" : ",\n    ",
            bufsize, iters, result_ms, result_mbps
        );
    }
    else
    {
        printf("%ju iterations in %ju ms = %.2f MB/s\n", iters, result_ms, result_mbps);
    }
    XXH3_freeState(hash_state);
    hash_state = NULL;
    free(in_buf);
}

// Run hardware performance tests (for now, only encryption-related)
std::function<bool(cli_result_t &)> cli_tool_t::start_cpubench(json11::Json cfg)
{
    int json = !!json_output;
    if (!json_output)
    {
        printf("Vitastor transport encryption benchmark (AES-256-GCM, AES-256-XTS and xxhash3)\n");
        printf("\nWarmup...\n");
    }
    else
        printf("[\n    ");
    bench_aes_xts(1000, 1048576, 0, true, false);
    if (!json_output)
        printf("\nNo transport encryption, data checksums enabled, e2e unencrypted image\n");
    bench_xxh(2000, 1048576, json ? json++ : 0);
    bench_xxh(2000, 4096, json ? json++ : 0);
    if (!json_output)
        printf("\nHeader encryption with payload checksums, e2e unencrypted image\n");
    bench_aes_gcm(2000, 1048576, true, json ? json++ : 0);
    bench_aes_gcm(2000, 4096, true, json ? json++ : 0);
    if (!json_output)
        printf("\nFull transport encryption, e2e unencrypted image\n");
    bench_aes_gcm(2000, 1048576, false, json ? json++ : 0);
    bench_aes_gcm(2000, 4096, false, json ? json++ : 0);
    if (!json_output)
        printf("\nNo transport encryption, no checksums, e2e encrypted image\n");
    bench_aes_xts(2000, 1048576, 0, false, json ? json++ : 0);
    bench_aes_xts(2000, 4096, 0, false, json ? json++ : 0);
    if (!json_output)
        printf("\nNo transport encryption, e2e encrypted image, data checksums enabled\n");
    bench_aes_xts(2000, 1048576, MSGR_CSUM_FULL, false, json ? json++ : 0);
    bench_aes_xts(2000, 4096, MSGR_CSUM_FULL, false, json ? json++ : 0);
    if (!json_output)
        printf("\nHeader encryption with payload checksums, e2e encrypted image\n");
    bench_aes_xts(2000, 1048576, MSGR_CSUM_PAYLOAD, false, json ? json++ : 0);
    bench_aes_xts(2000, 4096, MSGR_CSUM_PAYLOAD, false, json ? json++ : 0);
    if (!json_output)
        printf("\nFull transport encryption, e2e encrypted image\n");
    bench_aes_xts(2000, 1048576, MSGR_CSUM_GCM, false, json ? json++ : 0);
    bench_aes_xts(2000, 4096, MSGR_CSUM_GCM, false, json ? json++ : 0);
    if (json_output)
        printf("\n]\n");
    return NULL;
}
