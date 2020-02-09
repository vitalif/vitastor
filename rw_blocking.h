#pragma once

#include <unistd.h>

int read_blocking(int fd, void *read_buf, size_t remaining);
int write_blocking(int fd, void *write_buf, size_t remaining);
