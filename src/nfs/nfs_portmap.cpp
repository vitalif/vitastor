// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// Portmap service for NFS proxy

#include <netinet/in.h>
#include <string.h>

#include "proto/portmap.h"
#include "proto/xdr_impl_inline.h"

#include "malloc_or_die.h"
#include "nfs_portmap.h"
#include "sha256.h"
#include "str_util.h"

/*
 * The NULL procedure. All protocols/versions must provide a NULL procedure
 * as index 0.
 * It is used by clients, and rpcinfo, to "ping" a service and verify that
 * the service is available and that it does support the indicated version.
 */
static int pmap2_null_proc(struct rpc_context *rpc, rpc_op_t *rop)
{
    rpc_queue_reply(rop);
    return 0;
}

/*
 * v2 GETPORT.
 * This is the lookup function for portmapper version 2.
 * A client provides program, version and protocol (tcp or udp)
 * and portmapper returns which port that service is available on,
 * (or 0 if no such program is registered.)
 */
static int pmap2_getport_proc(portmap_service_t *self, rpc_op_t *rop)
{
    PMAP2GETPORTargs *args = (PMAP2GETPORTargs *)rop->request;
    uint32_t *reply = (uint32_t *)rop->reply;
    auto it = self->reg_ports.lower_bound((portmap_id_t){
        .prog = args->prog,
        .vers = args->vers,
        .udp = args->prot == IPPROTO_UDP,
        .ipv6 = false,
    });
    if (it != self->reg_ports.end() &&
        it->prog == args->prog && it->vers == args->vers &&
        it->udp == (args->prot == IPPROTO_UDP))
    {
        *reply = it->port;
    }
    else
    {
        *reply = 0;
    }
    rpc_queue_reply(rop);
    return 0;
}

/*
 * v2 DUMP.
 * This RPC returns a list of all endpoints that are registered with
 * portmapper.
 */
static int pmap2_dump_proc(portmap_service_t *self, rpc_op_t *rop)
{
    pmap2_mapping_list *list = (pmap2_mapping_list*)malloc_or_die(sizeof(pmap2_mapping_list) * self->reg_ports.size());
    xdr_add_malloc(rop->xdrs, list);
    PMAP2DUMPres *reply = (PMAP2DUMPres *)rop->reply;
    int i = 0;
    for (auto it = self->reg_ports.begin(); it != self->reg_ports.end(); it++)
    {
        if (it->ipv6)
            continue;
        list[i] = {
            .map = {
                .prog = it->prog,
                .vers = it->vers,
                .prot = it->udp ? IPPROTO_UDP : IPPROTO_TCP,
                .port = it->port,
            },
            .next = list+i+1,
        };
        i++;
    }
    list[i-1].next = NULL;
    // Send reply
    reply->list = list;
    rpc_queue_reply(rop);
    return 0;
}

/*
 * v3 GETADDR.
 * This is the lookup function for portmapper version 3.
 */
static int pmap3_getaddr_proc(portmap_service_t *self, rpc_op_t *rop)
{
    PMAP3GETADDRargs *args = (PMAP3GETADDRargs *)rop->request;
    PMAP3GETADDRres *reply = (PMAP3GETADDRres *)rop->reply;
    portmap_id_t ref = (portmap_id_t){
        .prog = args->prog,
        .vers = args->vers,
        .udp = args->netid == "udp" || args->netid == "udp6",
        .ipv6 = args->netid == "tcp6" || args->netid == "udp6",
    };
    auto it = self->reg_ports.lower_bound(ref);
    if (it != self->reg_ports.end() &&
        it->prog == ref.prog && it->vers == ref.vers &&
        it->udp == ref.udp && it->ipv6 == ref.ipv6)
    {
        reply->addr = xdr_copy_string(rop->xdrs, it->addr);
    }
    else
    {
        reply->addr = {};
    }
    rpc_queue_reply(rop);
    return 0;
}

/*
 * v3 DUMP.
 * This RPC returns a list of all endpoints that are registered with
 * portmapper.
 */
static std::string netid_udp = "udp";
static std::string netid_udp6 = "udp6";
static std::string netid_tcp = "tcp";
static std::string netid_tcp6 = "tcp6";
static int pmap3_dump_proc(portmap_service_t *self, rpc_op_t *rop)
{
    PMAP3DUMPres *reply = (PMAP3DUMPres *)rop->reply;
    pmap3_mapping_list *list = (pmap3_mapping_list*)malloc_or_die(sizeof(pmap3_mapping_list*) * self->reg_ports.size());
    xdr_add_malloc(rop->xdrs, list);
    int i = 0;
    for (auto it = self->reg_ports.begin(); it != self->reg_ports.end(); it++)
    {
        list[i] = (pmap3_mapping_list){
            .map = (pmap3_mapping){
                .prog  = it->prog,
                .vers  = it->vers,
                .netid = xdr_copy_string(rop->xdrs, it->ipv6
                    ? (it->udp ? netid_udp6 : netid_tcp6)
                    : (it->udp ? netid_udp : netid_tcp)),
                .addr  = xdr_copy_string(rop->xdrs, it->addr), // 0.0.0.0.port
                .owner = xdr_copy_string(rop->xdrs, it->owner),
            },
            .next = list+i+1,
        };
        i++;
    }
    list[i-1].next = NULL;
    reply->list = list;
    rpc_queue_reply(rop);
    return 0;
}

portmap_service_t::portmap_service_t()
{
    struct rpc_service_proc_t pt[] = {
        {PMAP_PROGRAM, PMAP_V2, PMAP2_NULL, (rpc_handler_t)pmap2_null_proc, NULL, 0, NULL, 0, this},
        {PMAP_PROGRAM, PMAP_V2, PMAP2_GETPORT, (rpc_handler_t)pmap2_getport_proc, (xdrproc_t)xdr_PMAP2GETPORTargs, sizeof(PMAP2GETPORTargs), (xdrproc_t)xdr_u_int, sizeof(u_int), this},
        {PMAP_PROGRAM, PMAP_V2, PMAP2_DUMP, (rpc_handler_t)pmap2_dump_proc, NULL, 0, (xdrproc_t)xdr_PMAP2DUMPres, sizeof(PMAP2DUMPres), this},
        {PMAP_PROGRAM, PMAP_V3, PMAP3_NULL, (rpc_handler_t)pmap2_null_proc, NULL, 0, NULL, 0, this},
        {PMAP_PROGRAM, PMAP_V3, PMAP3_GETADDR, (rpc_handler_t)pmap3_getaddr_proc, (xdrproc_t)xdr_PMAP3GETADDRargs, sizeof(PMAP3GETADDRargs), (xdrproc_t)xdr_string, sizeof(xdr_string_t), this},
        {PMAP_PROGRAM, PMAP_V3, PMAP3_DUMP, (rpc_handler_t)pmap3_dump_proc, NULL, 0, (xdrproc_t)xdr_PMAP3DUMPres, sizeof(PMAP3DUMPres), this},
    };
    for (int i = 0; i < sizeof(pt)/sizeof(pt[0]); i++)
    {
        proc_table.push_back(pt[i]);
    }
}

std::string sha256(const std::string & str)
{
    std::string hash;
    hash.resize(32);
    SHA256_CTX ctx;
    sha256_init(&ctx);
    sha256_update(&ctx, (uint8_t*)str.data(), str.size());
    sha256_final(&ctx, (uint8_t*)hash.data());
    return hash;
}
