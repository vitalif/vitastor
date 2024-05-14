#pragma once

#include "rpc.h"

struct rpc_op_t;

// Handler should return 1 if the request is processed asynchronously
// and requires the incoming message to not be freed until processing ends,
// 0 otherwise.
typedef int (*rpc_handler_t)(void *opaque, rpc_op_t *rop);

struct rpc_service_proc_t
{
    uint32_t prog;
    uint32_t vers;
    uint32_t proc;
    rpc_handler_t handler_fn;
    xdrproc_t req_fn;
    uint32_t req_size;
    xdrproc_t resp_fn;
    uint32_t resp_size;
    void *opaque;
};

inline bool operator < (const rpc_service_proc_t & a, const rpc_service_proc_t & b)
{
    return a.prog < b.prog || a.prog == b.prog && (a.vers < b.vers || a.vers == b.vers && a.proc < b.proc);
}

struct rpc_op_t
{
    void *client;
    uint8_t *buffer;
    XDR *xdrs;
    rpc_msg in_msg, out_msg;
    void *request;
    void *reply;
    xdrproc_t reply_fn;
    uint32_t reply_marker;
    bool referenced;
};

void rpc_queue_reply(rpc_op_t *rop);
