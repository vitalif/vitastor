#pragma once

#include <unistd.h>
#include <sys/uio.h>

int read_blocking(int fd, void *read_buf, size_t remaining);
int write_blocking(int fd, void *write_buf, size_t remaining);
int readv_blocking(int fd, iovec *iov, int iovcnt);
int writev_blocking(int fd, iovec *iov, int iovcnt);
