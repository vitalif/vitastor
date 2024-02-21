// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#pragma once

#include <assert.h>
#include <stdint.h>

#define PATTERN0 0x8c4641acc762840e
#define PATTERN1 0x70a549add9a2280a
#define PATTERN2 0xffe3bad5f578a78e
#define PATTERN3 0x426bd7854eb08509

#define set_pattern(buf, len, pattern) for (uint64_t i = 0; i < len; i += 8) { *(uint64_t*)((uint8_t*)buf + i) = pattern; }
#define check_pattern(buf, len, pattern) { uint64_t bad = UINT64_MAX; for (uint64_t i = 0; i < len; i += 8) { if ((*(uint64_t*)((uint8_t*)buf + i)) != (pattern)) { bad = i; break; } } if (bad != UINT64_MAX) { printf("mismatch at %jx\n", bad); } assert(bad == UINT64_MAX); }
