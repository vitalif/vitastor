// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include <errno.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>

#include "rw_blocking.h"

int read_blocking(int fd, void *read_buf, size_t remaining)
{
    size_t done = 0;
    while (done < remaining)
    {
        ssize_t r = read(fd, read_buf, remaining-done);
        if (r <= 0)
        {
            if (!errno)
            {
                // EOF
                return done;
            }
            else if (errno != EINTR && errno != EAGAIN && errno != EPIPE)
            {
                perror("read");
                exit(1);
            }
            continue;
        }
        done += r;
        read_buf = (uint8_t*)read_buf + r;
    }
    return done;
}

int write_blocking(int fd, void *write_buf, size_t remaining)
{
    size_t done = 0;
    while (done < remaining)
    {
        ssize_t r = write(fd, write_buf, remaining-done);
        if (r < 0)
        {
            if (errno != EINTR && errno != EAGAIN && errno != EPIPE)
            {
                perror("write");
                exit(1);
            }
            continue;
        }
        done += r;
        write_buf = (uint8_t*)write_buf + r;
    }
    return done;
}

int readv_blocking(int fd, iovec *iov, int iovcnt)
{
    int v = 0;
    int done = 0;
    while (v < iovcnt)
    {
        ssize_t r = readv(fd, iov+v, iovcnt-v);
        if (r < 0)
        {
            if (errno != EINTR && errno != EAGAIN && errno != EPIPE)
            {
                perror("writev");
                exit(1);
            }
            continue;
        }
        done += r;
        while (v < iovcnt)
        {
            if (iov[v].iov_len > r)
            {
                iov[v].iov_len -= r;
                iov[v].iov_base = (uint8_t*)iov[v].iov_base + r;
                break;
            }
            else
            {
                r -= iov[v].iov_len;
                v++;
            }
        }
    }
    return done;
}

int writev_blocking(int fd, iovec *iov, int iovcnt)
{
    int v = 0;
    int done = 0;
    while (v < iovcnt)
    {
        ssize_t r = writev(fd, iov+v, iovcnt-v);
        if (r < 0)
        {
            if (errno != EINTR && errno != EAGAIN && errno != EPIPE)
            {
                perror("writev");
                exit(1);
            }
            continue;
        }
        done += r;
        while (v < iovcnt)
        {
            if (iov[v].iov_len > r)
            {
                iov[v].iov_len -= r;
                iov[v].iov_base = (uint8_t*)iov[v].iov_base + r;
                break;
            }
            else
            {
                r -= iov[v].iov_len;
                v++;
            }
        }
    }
    return done;
}

int sendv_blocking(int fd, iovec *iov, int iovcnt, int flags)
{
    struct msghdr msg = { 0 };
    int v = 0;
    int done = 0;
    while (v < iovcnt)
    {
        msg.msg_iov = iov+v;
        msg.msg_iovlen = iovcnt-v;
        ssize_t r = sendmsg(fd, &msg, flags);
        if (r < 0)
        {
            if (errno != EINTR && errno != EAGAIN && errno != EPIPE)
            {
                perror("sendmsg");
                exit(1);
            }
            continue;
        }
        done += r;
        while (v < iovcnt)
        {
            if (iov[v].iov_len > r)
            {
                iov[v].iov_len -= r;
                iov[v].iov_base = (uint8_t*)iov[v].iov_base + r;
                break;
            }
            else
            {
                r -= iov[v].iov_len;
                v++;
            }
        }
    }
    return done;
}
