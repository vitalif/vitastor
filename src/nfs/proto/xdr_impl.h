// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// Efficient XDR implementation almost compatible with rpcgen (see run-rpcgen.sh)

#pragma once

#include <sys/uio.h>
#include <stdint.h>
#include <string>

#define XDR_COPY_LENGTH 128

struct xdr_string_t
{
    size_t size;
    char *data;

    operator std::string()
    {
        return std::string(data, size);
    }

    bool operator == (const char *str)
    {
        if (!str)
            return false;
        int i;
        for (i = 0; i < size; i++)
            if (!str[i] || str[i] != data[i])
                return false;
        if (str[i])
            return false;
        return true;
    }

    bool operator != (const char *str)
    {
        return !(*this == str);
    }
};

typedef uint32_t u_int;
typedef uint32_t enum_t;
typedef uint32_t bool_t;
struct XDR;
typedef int (*xdrproc_t)(XDR *xdrs, void *data);

// Create an empty XDR object
XDR* xdr_create();

// Destroy the XDR object
void xdr_destroy(XDR* xdrs);

// Free resources from any previous xdr_decode/xdr_encode calls
void xdr_reset(XDR *xdrs);

// Mark XDR as used for RDMA
void xdr_set_rdma(XDR *xdrs);

// Set (single) RDMA chunk buffer for this xdr before decoding an RDMA message
void xdr_set_rdma_chunk(XDR *xdrs, void *chunk);

// Get the current RDMA chunk buffer
void* xdr_get_rdma_chunk(XDR *xdrs);

// Try to decode <size> bytes from buffer <buf> using <fn>
// Result may contain memory allocations that will be valid until the next call to xdr_{reset,destroy,decode,encode}
int xdr_decode(XDR *xdrs, void *buf, unsigned size, xdrproc_t fn, void *data);

// Try to encode <data> using <fn>
// May be mixed with xdr_decode
// May be called multiple times to encode multiple parts of the same message
int xdr_encode(XDR *xdrs, xdrproc_t fn, void *data);

// Get current size of encoded data in <xdrs>
size_t xdr_encode_get_size(XDR *xdrs);

// Get the result of previous xdr_encodes as a list of <struct iovec>'s
// in <iov_list> (start) and <iov_count> (count).
// The resulting iov_list is valid until the next call to xdr_{reset,destroy}.
// It may contain references to the original data, so original data must not
// be freed until the result is fully processed (sent).
void xdr_encode_finish(XDR *xdrs, iovec **iov_list, unsigned *iov_count);

// Remember an allocated buffer to free it later on xdr_reset() or xdr_destroy()
void xdr_add_malloc(XDR *xdrs, void *buf);

// Remove an allocated buffer from XDR
void xdr_del_malloc(XDR *xdrs, void *buf);

xdr_string_t xdr_copy_string(XDR *xdrs, const std::string & str);

xdr_string_t xdr_copy_string(XDR *xdrs, const char *str);

xdr_string_t xdr_copy_string(XDR *xdrs, const char *str, size_t len);

void xdr_dump_encoded(XDR *xdrs);
