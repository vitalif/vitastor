// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#pragma once

#include <string>
#include <stdint.h>

void check_size(int fd, uint64_t *size, uint64_t *sectsize, const std::string & name);
