// 7.5k LOC header is not a header
// This is a real header for xxhash3

#pragma once

#if defined(__cplusplus) && !defined(XXH_NO_EXTERNC_GUARD)
extern "C" {
#endif

typedef enum {
    XXH_OK = 0,
    XXH_ERROR
} XXH_errorcode;
typedef uint64_t XXH64_hash_t;
XXH64_hash_t XXH3_64bits(const void* input, size_t length);
__attribute__((__pure__)) XXH64_hash_t XXH3_64bits( const void* input, size_t length);
__attribute__((__pure__)) XXH64_hash_t XXH3_64bits_withSeed( const void* input, size_t length, XXH64_hash_t seed);
__attribute__((__pure__)) XXH64_hash_t XXH3_64bits_withSecret( const void* data, size_t len, const void* secret, size_t secretSize);
typedef struct XXH3_state_s XXH3_state_t;
__attribute__((__malloc__)) XXH3_state_t* XXH3_createState(void);
XXH_errorcode XXH3_freeState(XXH3_state_t* statePtr);
void XXH3_copyState( XXH3_state_t* dst_state, const XXH3_state_t* src_state);
XXH_errorcode XXH3_64bits_reset( XXH3_state_t* statePtr);
XXH_errorcode XXH3_64bits_reset_withSeed( XXH3_state_t* statePtr, XXH64_hash_t seed);
XXH_errorcode XXH3_64bits_reset_withSecret( XXH3_state_t* statePtr, const void* secret, size_t secretSize);
XXH_errorcode XXH3_64bits_update ( XXH3_state_t* statePtr, const void* input, size_t length);
__attribute__((__pure__)) XXH64_hash_t XXH3_64bits_digest ( const XXH3_state_t* statePtr);

#if defined (__cplusplus) && !defined(XXH_NO_EXTERNC_GUARD)
} /* extern "C" */
#endif
