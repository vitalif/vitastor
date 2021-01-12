// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#pragma once

#include <sys/uio.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "osd_ops.h"

#define OSD_OP_IN 0
#define OSD_OP_OUT 1

#define OSD_OP_INLINE_BUF_COUNT 16

// Kind of a vector with small-list-optimisation
struct osd_op_buf_list_t
{
    int count = 0, alloc = OSD_OP_INLINE_BUF_COUNT, done = 0;
    iovec *buf = NULL;
    iovec inline_buf[OSD_OP_INLINE_BUF_COUNT];

    inline osd_op_buf_list_t()
    {
        buf = inline_buf;
    }

    inline osd_op_buf_list_t(const osd_op_buf_list_t & other)
    {
        buf = inline_buf;
        append(other);
    }

    inline osd_op_buf_list_t & operator = (const osd_op_buf_list_t & other)
    {
        reset();
        append(other);
        return *this;
    }

    inline ~osd_op_buf_list_t()
    {
        if (buf && buf != inline_buf)
        {
            free(buf);
        }
    }

    inline void reset()
    {
        count = 0;
        done = 0;
    }

    inline iovec* get_iovec()
    {
        return buf + done;
    }

    inline int get_size()
    {
        return count - done;
    }

    inline void append(const osd_op_buf_list_t & other)
    {
        if (count+other.count > alloc)
        {
            if (buf == inline_buf)
            {
                int old = alloc;
                alloc = (((count+other.count+15)/16)*16);
                buf = (iovec*)malloc(sizeof(iovec) * alloc);
                if (!buf)
                {
                    printf("Failed to allocate %lu bytes\n", sizeof(iovec) * alloc);
                    exit(1);
                }
                memcpy(buf, inline_buf, sizeof(iovec) * old);
            }
            else
            {
                alloc = (((count+other.count+15)/16)*16);
                buf = (iovec*)realloc(buf, sizeof(iovec) * alloc);
                if (!buf)
                {
                    printf("Failed to allocate %lu bytes\n", sizeof(iovec) * alloc);
                    exit(1);
                }
            }
        }
        for (int i = 0; i < other.count; i++)
        {
            buf[count++] = other.buf[i];
        }
    }

    inline void push_back(void *nbuf, size_t len)
    {
        if (count >= alloc)
        {
            if (buf == inline_buf)
            {
                int old = alloc;
                alloc = ((alloc/16)*16 + 1);
                buf = (iovec*)malloc(sizeof(iovec) * alloc);
                if (!buf)
                {
                    printf("Failed to allocate %lu bytes\n", sizeof(iovec) * alloc);
                    exit(1);
                }
                memcpy(buf, inline_buf, sizeof(iovec)*old);
            }
            else
            {
                alloc = alloc < 16 ? 16 : (alloc+16);
                buf = (iovec*)realloc(buf, sizeof(iovec) * alloc);
                if (!buf)
                {
                    printf("Failed to allocate %lu bytes\n", sizeof(iovec) * alloc);
                    exit(1);
                }
            }
        }
        buf[count++] = { .iov_base = nbuf, .iov_len = len };
    }

    inline void eat(int result)
    {
        while (result > 0 && done < count)
        {
            iovec & iov = buf[done];
            if (iov.iov_len <= result)
            {
                result -= iov.iov_len;
                done++;
            }
            else
            {
                iov.iov_len -= result;
                iov.iov_base += result;
                break;
            }
        }
    }
};

struct blockstore_op_t;

struct osd_primary_op_data_t;

struct osd_op_t
{
    timespec tv_begin;
    uint64_t op_type = OSD_OP_IN;
    int peer_fd;
    osd_any_op_t req;
    osd_any_reply_t reply;
    blockstore_op_t *bs_op = NULL;
    void *buf = NULL;
    void *bitmap = NULL;
    unsigned bitmap_len = 0;
    unsigned bmp_data = 0;
    void *rmw_buf = NULL;
    osd_primary_op_data_t* op_data = NULL;
    std::function<void(osd_op_t*)> callback;

    osd_op_buf_list_t iov;

    ~osd_op_t();
};
