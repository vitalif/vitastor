// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// Efficient XDR implementation almost compatible with rpcgen (see run-rpcgen.sh)

#include "xdr_impl_inline.h"

XDR* xdr_create()
{
    return new XDR;
}

void xdr_destroy(XDR* xdrs)
{
    xdr_reset(xdrs);
    delete xdrs;
}

void xdr_set_rdma(XDR *xdrs)
{
    xdrs->rdma = true;
}

void xdr_set_rdma_chunk(XDR *xdrs, void *chunk)
{
    assert(!xdrs->rdma_chunk || !chunk);
    xdrs->rdma_chunk = chunk;
}

void* xdr_get_rdma_chunk(XDR *xdrs)
{
    return xdrs->rdma_chunk;
}

void xdr_reset(XDR *xdrs)
{
    for (auto buf: xdrs->allocs)
    {
        free(buf);
    }
    xdrs->buf = NULL;
    xdrs->rdma = false;
    xdrs->rdma_chunk = NULL;
    xdrs->rdma_chunk_used = false;
    xdrs->avail = 0;
    xdrs->allocs.resize(0);
    xdrs->in_linked_list.resize(0);
    xdrs->cur_out.resize(0);
    xdrs->last_end = 0;
    xdrs->buf_list.resize(0);
}

int xdr_decode(XDR *xdrs, void *buf, unsigned size, xdrproc_t fn, void *data)
{
    xdrs->x_op = XDR_DECODE;
    xdrs->buf = (uint8_t*)buf;
    xdrs->avail = size;
    return fn(xdrs, data);
}

int xdr_encode(XDR *xdrs, xdrproc_t fn, void *data)
{
    xdrs->x_op = XDR_ENCODE;
    return fn(xdrs, data);
}

size_t xdr_encode_get_size(XDR *xdrs)
{
    size_t len = 0;
    for (auto & buf: xdrs->buf_list)
    {
        len += buf.iov_len;
    }
    if (xdrs->last_end < xdrs->cur_out.size())
    {
        len += xdrs->cur_out.size() - xdrs->last_end;
    }
    return len;
}

void xdr_encode_finish(XDR *xdrs, iovec **iov_list, unsigned *iov_count)
{
    if (xdrs->last_end < xdrs->cur_out.size())
    {
        xdrs->buf_list.push_back((iovec){
            .iov_base = 0,
            .iov_len = xdrs->cur_out.size() - xdrs->last_end,
        });
        xdrs->last_end = xdrs->cur_out.size();
    }
    uint8_t *cur_buf = xdrs->cur_out.data();
    for (auto & buf: xdrs->buf_list)
    {
        if (!buf.iov_base)
        {
            buf.iov_base = cur_buf;
            cur_buf += buf.iov_len;
        }
    }
    *iov_list = xdrs->buf_list.data();
    *iov_count = xdrs->buf_list.size();
}

void xdr_dump_encoded(XDR *xdrs)
{
    for (auto & buf: xdrs->buf_list)
    {
        for (int i = 0; i < buf.iov_len; i++)
           printf("%02x", ((uint8_t*)buf.iov_base)[i]);
    }
    printf("\n");
}

void xdr_add_malloc(XDR *xdrs, void *buf)
{
    xdrs->allocs.push_back(buf);
}

void xdr_del_malloc(XDR *xdrs, void *buf)
{
    for (int i = 0; i < xdrs->allocs.size(); i++)
    {
        if (xdrs->allocs[i] == buf)
        {
            xdrs->allocs.erase(xdrs->allocs.begin()+i);
            break;
        }
    }
}

xdr_string_t xdr_copy_string(XDR *xdrs, const std::string & str)
{
    char *cp = (char*)malloc_or_die(str.size()+1);
    memcpy(cp, str.data(), str.size());
    cp[str.size()] = 0;
    xdr_add_malloc(xdrs, cp);
    return (xdr_string_t){ str.size(), cp };
}

xdr_string_t xdr_copy_string(XDR *xdrs, const char *str)
{
    return xdr_copy_string(xdrs, str, strlen(str));
}

xdr_string_t xdr_copy_string(XDR *xdrs, const char *str, size_t len)
{
    char *cp = (char*)malloc_or_die(len+1);
    memcpy(cp, str, len);
    cp[len] = 0;
    xdr_add_malloc(xdrs, cp);
    return (xdr_string_t){ len, cp };
}
