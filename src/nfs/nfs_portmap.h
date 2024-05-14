// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// Portmap service for NFS proxy

#pragma once

#include <string>
#include <set>
#include <vector>

#include "proto/rpc_impl.h"

struct portmap_id_t
{
    unsigned prog, vers;
    bool udp;
    bool ipv6;
    unsigned port;
    std::string owner;
    std::string addr;
};

class portmap_service_t
{
public:
    std::set<portmap_id_t> reg_ports;
    std::vector<rpc_service_proc_t> proc_table;
    portmap_service_t();
};

inline bool operator < (const portmap_id_t &a, const portmap_id_t &b)
{
    return a.prog < b.prog || a.prog == b.prog && a.vers < b.vers ||
        a.prog == b.prog && a.vers == b.vers && a.udp < b.udp ||
        a.prog == b.prog && a.vers == b.vers && a.udp == b.udp && a.ipv6 < b.ipv6;
}

std::string sha256(const std::string & str);
