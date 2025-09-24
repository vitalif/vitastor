// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// NFS proxy over VitastorKV database - GETATTR

#include <sys/time.h>

#include "nfs_proxy.h"
#include "nfs_kv.h"

// Attributes are always stored in the inode
void kv_read_inode(nfs_proxy_t *proxy, uint64_t ino,
    std::function<void(int res, const std::string & value, json11::Json ientry)> cb,
    bool allow_cache)
{
    if (!ino)
    {
        // Zero value can not exist
        cb(-ENOENT, "", json11::Json());
        return;
    }
    auto key = kv_inode_key(ino);
    proxy->db->get(key, [=](int res, const std::string & value)
    {
        if (res < 0)
        {
            if (res != -ENOENT)
                fprintf(stderr, "Error reading inode %s: %s (code %d)\n", kv_inode_key(ino).c_str(), strerror(-res), res);
            cb(res, "", json11::Json());
            return;
        }
        std::string err;
        auto attrs = json11::Json::parse(value, err);
        if (err != "")
        {
            fprintf(stderr, "Invalid JSON in inode %s = %s: %s\n", kv_inode_key(ino).c_str(), value.c_str(), err.c_str());
            res = -EIO;
        }
        cb(res, value, attrs);
    }, allow_cache);
}

int kv_nfs3_getattr_proc(void *opaque, rpc_op_t *rop)
{
    nfs_client_t *self = (nfs_client_t*)opaque;
    GETATTR3args *args = (GETATTR3args*)rop->request;
    GETATTR3res *reply = (GETATTR3res*)rop->reply;
    std::string fh = args->object;
    auto ino = kv_fh_inode(fh);
    if (self->parent->trace)
        fprintf(stderr, "[%d] GETATTR %ju\n", self->nfs_fd, ino);
    if (!kv_fh_valid(fh) || !ino)
    {
        *reply = (GETATTR3res){ .status = NFS3ERR_INVAL };
        rpc_queue_reply(rop);
        return 0;
    }
    kv_read_inode(self->parent, ino, [=](int res, const std::string & value, json11::Json attrs)
    {
        if (self->parent->trace)
            fprintf(stderr, "[%d] GETATTR %ju -> %s\n", self->nfs_fd, ino, value.c_str());
        if (res == 0 && self->parent->enforce_perms &&
            !kv_is_accessible(rop->auth_sys, attrs, ACCESS3_READ))
        {
            res = -EACCES;
        }
        if (res < 0)
        {
            *reply = (GETATTR3res){ .status = vitastor_nfs_map_err(-res) };
        }
        else
        {
            *reply = (GETATTR3res){
                .status = NFS3_OK,
                .resok = (GETATTR3resok){
                    .obj_attributes = get_kv_attributes(self->parent, ino, attrs),
                },
            };
        }
        rpc_queue_reply(rop);
    });
    return 1;
}

uint32_t kv_get_access(const authsys_parms & auth_sys, const json11::Json & attrs)
{
    uint32_t mode = attrs["mode"].is_null() ? (attrs["type"] == "dir" ? 0755 : 0644) : attrs["mode"].uint64_value();
    uint32_t uid = attrs["uid"].uint64_value();
    uint32_t gid = attrs["gid"].uint64_value();
    uint32_t access = 0;
    if (uid == auth_sys.uid)
    {
        access |= ((mode & (1 << 8)) ? ACCESS3_READ|ACCESS3_LOOKUP : 0);
        access |= ((mode & (1 << 7)) ? ACCESS3_MODIFY|ACCESS3_EXTEND|ACCESS3_DELETE : 0);
        access |= ((mode & (1 << 6)) ? ACCESS3_EXECUTE : 0);
    }
    for (uint32_t i = 0; i < auth_sys.gids.gids_len; i++)
    {
        if (gid == auth_sys.gids.gids_val[i])
            gid = auth_sys.gid;
    }
    if (gid == auth_sys.gid)
    {
        access |= ((mode & (1 << 5)) ? ACCESS3_READ|ACCESS3_LOOKUP : 0);
        access |= ((mode & (1 << 4)) ? ACCESS3_MODIFY|ACCESS3_EXTEND|ACCESS3_DELETE : 0);
        access |= ((mode & (1 << 3)) ? ACCESS3_EXECUTE : 0);
    }
    access |= ((mode & (1 << 2)) ? ACCESS3_READ|ACCESS3_LOOKUP : 0);
    access |= ((mode & (1 << 1)) ? ACCESS3_MODIFY|ACCESS3_EXTEND|ACCESS3_DELETE : 0);
    access |= ((mode & (1 << 0)) ? ACCESS3_EXECUTE : 0);
    return access;
}

bool kv_is_accessible(const authsys_parms & auth_sys, const json11::Json & attrs, uint32_t access)
{
    uint32_t mode = attrs["mode"].is_null() ? (attrs["type"] == "dir" ? 0755 : 0644) : attrs["mode"].uint64_value();
    uint32_t mask = 1 << (access == ACCESS3_EXECUTE ? 0
        : (access == ACCESS3_MODIFY || access == ACCESS3_EXTEND || access == ACCESS3_DELETE ? 1 : 2));
    if (mode & mask)
        return true;
    uint32_t uid = attrs["uid"].uint64_value();
    if ((mode & (mask << 6)) && (uint32_t)uid == auth_sys.uid)
        return true;
    if ((mode & (mask << 3)))
    {
        uint32_t gid = attrs["gid"].uint64_value();
        if (gid == auth_sys.gid)
            return true;
        for (uint32_t i = 0; i < auth_sys.gids.gids_len; i++)
            if (gid == auth_sys.gids.gids_val[i])
                return true;
    }
    return false;
}

int kv_nfs3_access_proc(void *opaque, rpc_op_t *rop)
{
    nfs_client_t *self = (nfs_client_t*)opaque;
    ACCESS3args *args = (ACCESS3args*)rop->request;
    ACCESS3res *reply = (ACCESS3res*)rop->reply;
    std::string fh = args->object;
    auto ino = kv_fh_inode(fh);
    if (self->parent->trace)
        fprintf(stderr, "[%d] ACCESS %ju\n", self->nfs_fd, ino);
    if (!kv_fh_valid(fh) || !ino)
    {
        *reply = (ACCESS3res){ .status = NFS3ERR_INVAL };
        rpc_queue_reply(rop);
        return 0;
    }
    kv_read_inode(self->parent, ino, [=](int res, const std::string & value, json11::Json attrs)
    {
        if (self->parent->trace)
            fprintf(stderr, "[%d] ACCESS %ju -> %s\n", self->nfs_fd, ino, value.c_str());
        if (res < 0)
        {
            *reply = (ACCESS3res){ .status = vitastor_nfs_map_err(-res) };
        }
        else
        {
            uint32_t actual = kv_get_access(rop->auth_sys, attrs);
            if (args->access & actual)
            {
                *reply = (ACCESS3res){
                    .status = NFS3_OK,
                    .resok = (ACCESS3resok){
                        .access = actual,
                    },
                };
            }
            else
            {
                *reply = (ACCESS3res){
                    .status = NFS3ERR_ACCES,
                };
            }
        }
        rpc_queue_reply(rop);
    });
    return 1;
}
