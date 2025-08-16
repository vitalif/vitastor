// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#pragma once

#include <unistd.h>
#include <sys/uio.h>

#pragma GCC visibility push(default)

size_t read_blocking(int fd, void *read_buf, size_t remaining);
size_t write_blocking(int fd, void *write_buf, size_t remaining);
int readv_blocking(int fd, iovec *iov, int iovcnt);
int writev_blocking(int fd, iovec *iov, int iovcnt);
int sendv_blocking(int fd, iovec *iov, int iovcnt, int flags);

#pragma GCC visibility pop
