// 7.5k LOC header is not a header
// This is a real header for xxhash3

#pragma once

#include <stdint.h>
#include <stddef.h>

#if defined(__cplusplus) && !defined(XXH_NO_EXTERNC_GUARD)
extern "C" {
#endif

#if !defined(XXH_INLINE_ALL) && !defined(XXH_PRIVATE_API)
#  if defined(_WIN32) && defined(_MSC_VER) && (defined(XXH_IMPORT) || defined(XXH_EXPORT))
#    ifdef XXH_EXPORT
#      define XXH_PUBLIC_API __declspec(dllexport)
#    elif XXH_IMPORT
#      define XXH_PUBLIC_API __declspec(dllimport)
#    endif
#  else
#    define XXH_PUBLIC_API   /* do nothing */
#  endif
#endif

#ifdef __has_attribute
# define XXH_HAS_ATTRIBUTE(x) __has_attribute(x)
#else
# define XXH_HAS_ATTRIBUTE(x) 0
#endif

#if XXH_HAS_ATTRIBUTE(noescape)
# define XXH_NOESCAPE __attribute__((__noescape__))
#else
# define XXH_NOESCAPE
#endif

#define XXH_SECRET_DEFAULT_SIZE 192

typedef enum {
    XXH_OK = 0,
    XXH_ERROR
} XXH_errorcode;
typedef uint64_t XXH64_hash_t;
typedef struct {
    XXH64_hash_t low64;   /*!< `value & 0xFFFFFFFFFFFFFFFF` */
    XXH64_hash_t high64;  /*!< `value >> 64` */
} XXH128_hash_t;

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
