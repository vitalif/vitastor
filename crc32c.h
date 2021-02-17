#pragma once

#include <stdint.h>

// https://software.intel.com/sites/landingpage/IntrinsicsGuide/
// unsigned int _mm_crc32_u16 (unsigned int crc, unsigned short v)
// unsigned int _mm_crc32_u32 (unsigned int crc, unsigned int v)
// unsigned __int64 _mm_crc32_u64 (unsigned __int64 crc, unsigned __int64 v)
// unsigned int _mm_crc32_u8 (unsigned int crc, unsigned char v)

#ifdef __cplusplus
extern "C" {
#endif
uint32_t crc32c(uint32_t crc, const void *buf, size_t len);
#ifdef __cplusplus
};
#endif
