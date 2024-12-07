// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// Efficient XDR implementation almost compatible with rpcgen (see run-rpcgen.sh)

// XDR in a nutshell:
//
// int: big endian 32bit
// unsigned: BE 32bit
// enum: BE 32bit
// bool: BE 32bit 0/1
// hyper: BE 64bit
// unsigned hyper: BE 64bit
// float: BE float
// double: BE double
// quadruple: BE long double
// opaque[n] (fixed-length): bytes, padded to !(n%4)
// opaque (variable-length): BE 32bit length, then n bytes, padded to !(n%4)
// string: same as opaque
// array<T>[n] (fixed-length): n items of type T
// vector<T> (variable-length): BE 32bit length, then n items of type T
// struct: components in the same order as specified
// union: BE 32bit variant id, then variant of the union
// void: nothing (empty, 0 byte data)
// optional (XDR T*): BE 32bit 1/0, then T or nothing
// linked list: sequence of optional entries
//
// RPC over TCP:
//
// BE 32bit length, then rpc_msg, then the procedure message itself

#pragma once

#include "xdr_impl.h"

#include <string.h>
#include <endian.h>
#include <vector>

#include "malloc_or_die.h"

#define FALSE 0
#define TRUE 1
#define XDR_ENCODE 0
#define XDR_DECODE 1
#define BYTES_PER_XDR_UNIT 4
#define IXDR_PUT_U_LONG(a, b)
#define IXDR_GET_U_LONG(a) 0
#define IXDR_PUT_BOOL(a, b)
#define IXDR_GET_BOOL(a) 0
#define XDR_INLINE(xdrs, len) NULL

struct xdr_linked_list_t
{
    xdrproc_t fn;
    unsigned entry_size, size, cap;
    void *base;
    unsigned has_next, link_offset;
};

struct XDR
{
    int x_op;

    // For decoding:
    uint8_t *buf = NULL;
    unsigned avail = 0;
    std::vector<void*> allocs;
    std::vector<xdr_linked_list_t> in_linked_list;

    // For encoding:
    std::vector<uint8_t> cur_out;
    unsigned last_end = 0;
    std::vector<iovec> buf_list;
};

uint32_t inline len_pad4(uint32_t len)
{
    return ((len+3)/4) * 4;
}

inline int xdr_opaque(XDR *xdrs, void *data, uint32_t len)
{
    if (len <= 0)
    {
        return 1;
    }
    if (xdrs->x_op == XDR_DECODE)
    {
        uint32_t padded = len_pad4(len);
        if (xdrs->avail < padded)
            return 0;
        memcpy(data, xdrs->buf, len);
        xdrs->buf += padded;
        xdrs->avail -= padded;
    }
    else
    {
        unsigned old = xdrs->cur_out.size();
        uint32_t pad = (len & 3) ? (4 - (len & 3)) : 0;
        xdrs->cur_out.resize(old + len + pad);
        memcpy(xdrs->cur_out.data()+old, data, len);
        for (uint32_t i = 0; i < pad; i++)
            xdrs->cur_out[old+i] = 0;
    }
    return 1;
}

inline int xdr_bytes(XDR *xdrs, xdr_string_t *data, uint32_t maxlen)
{
    if (xdrs->x_op == XDR_DECODE)
    {
        if (xdrs->avail < 4)
            return 0;
        uint32_t len = be32toh(*((uint32_t*)xdrs->buf));
        uint32_t padded = len_pad4(len);
        if (xdrs->avail < 4+padded)
            return 0;
        data->size = len;
        data->data = (char*)(xdrs->buf+4);
        xdrs->buf += 4+padded;
        xdrs->avail -= 4+padded;
    }
    else
    {
        if (data->size < XDR_COPY_LENGTH)
        {
            unsigned old = xdrs->cur_out.size();
            xdrs->cur_out.resize(old + 4+data->size);
            *(uint32_t*)(xdrs->cur_out.data() + old) = htobe32(data->size);
            memcpy(xdrs->cur_out.data()+old+4, data->data, data->size);
        }
        else
        {
            unsigned old = xdrs->cur_out.size();
            xdrs->cur_out.resize(old + 4);
            *(uint32_t*)(xdrs->cur_out.data() + old) = htobe32(data->size);
            xdrs->buf_list.push_back((iovec){
                .iov_base = 0,
                .iov_len = xdrs->cur_out.size() - xdrs->last_end,
            });
            xdrs->last_end = xdrs->cur_out.size();
            xdrs->buf_list.push_back((iovec)
            {
                .iov_base = (void*)data->data,
                .iov_len = data->size,
            });
        }
        if (data->size & 3)
        {
            int pad = 4-(data->size & 3);
            unsigned old = xdrs->cur_out.size();
            xdrs->cur_out.resize(old+pad);
            for (int i = 0; i < pad; i++)
                xdrs->cur_out[old+i] = 0;
        }
    }
    return 1;
}

inline int xdr_string(XDR *xdrs, xdr_string_t *data, uint32_t maxlen)
{
    return xdr_bytes(xdrs, data, maxlen);
}

inline int xdr_u_int(XDR *xdrs, void *data)
{
    if (xdrs->x_op == XDR_DECODE)
    {
        if (xdrs->avail < 4)
            return 0;
        *((uint32_t*)data) = be32toh(*((uint32_t*)xdrs->buf));
        xdrs->buf += 4;
        xdrs->avail -= 4;
    }
    else
    {
        unsigned old = xdrs->cur_out.size();
        xdrs->cur_out.resize(old + 4);
        *(uint32_t*)(xdrs->cur_out.data() + old) = htobe32(*(uint32_t*)data);
    }
    return 1;
}

inline int xdr_enum(XDR *xdrs, void *data)
{
    return xdr_u_int(xdrs, data);
}

inline int xdr_bool(XDR *xdrs, void *data)
{
    return xdr_u_int(xdrs, data);
}

inline int xdr_uint64_t(XDR *xdrs, void *data)
{
    if (xdrs->x_op == XDR_DECODE)
    {
        if (xdrs->avail < 8)
            return 0;
        *((uint64_t*)data) = be64toh(*((uint64_t*)xdrs->buf));
        xdrs->buf += 8;
        xdrs->avail -= 8;
    }
    else
    {
        unsigned old = xdrs->cur_out.size();
        xdrs->cur_out.resize(old + 8);
        *(uint64_t*)(xdrs->cur_out.data() + old) = htobe64(*(uint64_t*)data);
    }
    return 1;
}

// Parse inconvenient shitty linked lists as arrays
inline int xdr_pointer(XDR *xdrs, char **data, unsigned entry_size, xdrproc_t entry_fn)
{
    if (xdrs->x_op == XDR_DECODE)
    {
        if (xdrs->avail < 4)
            return 0;
        uint32_t has_next = be32toh(*((uint32_t*)xdrs->buf));
        xdrs->buf += 4;
        xdrs->avail -= 4;
        *data = NULL;
        if (!xdrs->in_linked_list.size() ||
            xdrs->in_linked_list.back().fn != entry_fn)
        {
            if (has_next)
            {
                unsigned cap = 2;
                void *base = malloc_or_die(entry_size * cap);
                xdrs->in_linked_list.push_back((xdr_linked_list_t){
                    .fn = entry_fn,
                    .entry_size = entry_size,
                    .size = 1,
                    .cap = cap,
                    .base = base,
                    .has_next = 0,
                    .link_offset = 0,
                });
                *data = (char*)base;
                if (!entry_fn(xdrs, base))
                    return 0;
                auto & ll = xdrs->in_linked_list.back();
                while (ll.has_next)
                {
                    ll.has_next = 0;
                    if (ll.size >= ll.cap)
                    {
                        ll.cap *= 2;
                        ll.base = realloc_or_die(ll.base, ll.entry_size * ll.cap);
                    }
                    if (!entry_fn(xdrs, (uint8_t*)ll.base + ll.entry_size*ll.size))
                        return 0;
                    ll.size++;
                }
                for (unsigned i = 0; i < ll.size-1; i++)
                {
                    *(void**)((uint8_t*)ll.base + i*ll.entry_size + ll.link_offset) =
                        (uint8_t*)ll.base + (i+1)*ll.entry_size;
                }
                *data = (char*)ll.base;
                xdrs->allocs.push_back(ll.base);
                xdrs->in_linked_list.pop_back();
            }
        }
        else
        {
            auto & ll = xdrs->in_linked_list.back();
            xdrs->in_linked_list.back().has_next = has_next;
            xdrs->in_linked_list.back().link_offset = (uint8_t*)data - (uint8_t*)ll.base - ll.entry_size*ll.size;
        }
    }
    else
    {
        unsigned old = xdrs->cur_out.size();
        xdrs->cur_out.resize(old + 4);
        *(uint32_t*)(xdrs->cur_out.data() + old) = htobe32(*data ? 1 : 0);
        if (*data)
            entry_fn(xdrs, *data);
    }
    return 1;
}

inline int xdr_array(XDR *xdrs, char **data, uint32_t* len, uint32_t maxlen, uint32_t entry_size, xdrproc_t fn)
{
    if (xdrs->x_op == XDR_DECODE)
    {
        if (xdrs->avail < 4)
            return 0;
        *len = be32toh(*((uint32_t*)xdrs->buf));
        if (*len > maxlen)
            return 0;
        xdrs->buf += 4;
        xdrs->avail -= 4;
        *data = (char*)malloc_or_die(entry_size * (*len));
        for (uint32_t i = 0; i < *len; i++)
            fn(xdrs, *data + entry_size*i);
        xdrs->allocs.push_back(*data);
    }
    else
    {
        unsigned old = xdrs->cur_out.size();
        xdrs->cur_out.resize(old + 4);
        *(uint32_t*)(xdrs->cur_out.data() + old) = htobe32(*len);
        for (uint32_t i = 0; i < *len; i++)
            fn(xdrs, *data + entry_size*i);
    }
    return 1;
}
