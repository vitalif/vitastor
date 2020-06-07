#include <errno.h>
#include <stdlib.h>
#include <stdio.h>

#include "rw_blocking.h"

int read_blocking(int fd, void *read_buf, size_t remaining)
{
    size_t done = 0;
    while (done < remaining)
    {
        size_t r = read(fd, read_buf, remaining-done);
        if (r <= 0)
        {
            if (!errno)
            {
                // EOF
                return done;
            }
            else if (errno != EAGAIN && errno != EPIPE)
            {
                perror("read");
                exit(1);
            }
            continue;
        }
        done += r;
        read_buf += r;
    }
    return done;
}

int write_blocking(int fd, void *write_buf, size_t remaining)
{
    size_t done = 0;
    while (done < remaining)
    {
        size_t r = write(fd, write_buf, remaining-done);
        if (r < 0)
        {
            if (errno != EAGAIN && errno != EPIPE)
            {
                perror("write");
                exit(1);
            }
            continue;
        }
        done += r;
        write_buf += r;
    }
    return done;
}

int readv_blocking(int fd, iovec *iov, int iovcnt)
{
    int v = 0;
    int done = 0;
    while (v < iovcnt)
    {
        ssize_t r = readv(fd, iov, iovcnt);
        if (r < 0)
        {
            if (errno != EAGAIN && errno != EPIPE)
            {
                perror("writev");
                exit(1);
            }
            continue;
        }
        while (v < iovcnt)
        {
            if (iov[v].iov_len > r)
            {
                iov[v].iov_len -= r;
                iov[v].iov_base += r;
                break;
            }
            else
            {
                v++;
            }
        }
        done += r;
    }
    return done;
}

int writev_blocking(int fd, iovec *iov, int iovcnt)
{
    int v = 0;
    int done = 0;
    while (v < iovcnt)
    {
        ssize_t r = writev(fd, iov, iovcnt);
        if (r < 0)
        {
            if (errno != EAGAIN && errno != EPIPE)
            {
                perror("writev");
                exit(1);
            }
            continue;
        }
        while (v < iovcnt)
        {
            if (iov[v].iov_len > r)
            {
                iov[v].iov_len -= r;
                iov[v].iov_base += r;
                break;
            }
            else
            {
                v++;
            }
        }
        done += r;
    }
    return done;
}
