// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// NFS proxy over VitastorKV database - GETATTR

#include <sys/time.h>

#include "nfs_proxy.h"
#include "nfs_kv.h"

// Attributes are always stored in the inode
void kv_read_inode(nfs_client_t *self, uint64_t ino,
    std::function<void(int res, const std::string & value, json11::Json ientry)> cb,
    bool allow_cache)
{
    auto key = kv_inode_key(ino);
    self->parent->db->get(key, [=](int res, const std::string & value)
    {
        if (ino == KV_ROOT_INODE && res == -ENOENT)
        {
            // Allow root inode to not exist
            cb(0, "", json11::Json(json11::Json::object{ { "type", "dir" } }));
            return;
        }
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
    if (!kv_fh_valid(fh))
    {
        *reply = (GETATTR3res){ .status = NFS3ERR_INVAL };
        rpc_queue_reply(rop);
        return 0;
    }
    kv_read_inode(self, ino, [=](int res, const std::string & value, json11::Json attrs)
    {
        if (self->parent->trace)
            fprintf(stderr, "[%d] GETATTR %ju -> %s\n", self->nfs_fd, ino, value.c_str());
        if (res < 0)
        {
            *reply = (GETATTR3res){ .status = vitastor_nfs_map_err(-res) };
        }
        else
        {
            *reply = (GETATTR3res){
                .status = NFS3_OK,
                .resok = (GETATTR3resok){
                    .obj_attributes = get_kv_attributes(self, ino, attrs),
                },
            };
        }
        rpc_queue_reply(rop);
    });
    return 1;
}
