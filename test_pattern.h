#pragma once

#include <assert.h>
#include <stdint.h>

#define PATTERN0 0x8c4641acc762840e
#define PATTERN1 0x70a549add9a2280a
#define PATTERN2 0xffe3bad5f578a78e
#define PATTERN3 0x426bd7854eb08509

#define set_pattern(buf, len, pattern) for (uint64_t i = 0; i < len; i += 8) { *(uint64_t*)((void*)buf + i) = pattern; }
#define check_pattern(buf, len, pattern) for (uint64_t i = 0; i < len; i += 8) { assert(*(uint64_t*)(buf + i) == pattern); }
