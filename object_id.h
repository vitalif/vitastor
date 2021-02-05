// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#pragma once

#include <stdint.h>
#include <functional>

// 16 bytes per object/stripe id
// stripe = (start of the parity stripe + peer role)
// i.e. for example (256KB + one of 0,1,2)
struct __attribute__((__packed__)) object_id
{
    uint64_t inode;
    uint64_t stripe;
};

inline bool operator == (const object_id & a, const object_id & b)
{
    return a.inode == b.inode && a.stripe == b.stripe;
}

inline bool operator != (const object_id & a, const object_id & b)
{
    return a.inode != b.inode || a.stripe != b.stripe;
}

inline bool operator < (const object_id & a, const object_id & b)
{
    return a.inode < b.inode || a.inode == b.inode && a.stripe < b.stripe;
}

// 56 = 24 + 32 bytes per dirty entry in memory (obj_ver_id => dirty_entry)
struct __attribute__((__packed__)) obj_ver_id
{
    object_id oid;
    uint64_t version;
};

inline bool operator < (const obj_ver_id & a, const obj_ver_id & b)
{
    return a.oid < b.oid || a.oid == b.oid && a.version < b.version;
}

namespace std
{
    template<> struct hash<object_id>
    {
        inline size_t operator()(const object_id &s) const
        {
            size_t seed = 0;
            // Copy-pasted from spp::hash_combine()
            seed ^= (s.inode + 0xc6a4a7935bd1e995 + (seed << 6) + (seed >> 2));
            seed ^= (s.stripe + 0xc6a4a7935bd1e995 + (seed << 6) + (seed >> 2));
            return seed;
        }
    };

    template<> struct hash<obj_ver_id>
    {
        inline size_t operator()(const obj_ver_id &s) const
        {
            size_t seed = 0;
            // Copy-pasted from spp::hash_combine()
            seed ^= (s.oid.inode + 0xc6a4a7935bd1e995 + (seed << 6) + (seed >> 2));
            seed ^= (s.oid.stripe + 0xc6a4a7935bd1e995 + (seed << 6) + (seed >> 2));
            seed ^= (s.version + 0xc6a4a7935bd1e995 + (seed << 6) + (seed >> 2));
            return seed;
        }
    };
}
