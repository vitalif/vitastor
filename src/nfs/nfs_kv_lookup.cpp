// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// NFS proxy over VitastorKV database - LOOKUP, READLINK

#include <sys/time.h>

#include "nfs_proxy.h"
#include "nfs_kv.h"

int kv_nfs3_lookup_proc(void *opaque, rpc_op_t *rop)
{
    nfs_client_t *self = (nfs_client_t*)opaque;
    LOOKUP3args *args = (LOOKUP3args*)rop->request;
    LOOKUP3res *reply = (LOOKUP3res*)rop->reply;
    inode_t dir_ino = kv_fh_inode(args->what.dir);
    std::string filename = args->what.name;
    if (self->parent->trace)
        fprintf(stderr, "[%d] LOOKUP %ju/%s\n", self->nfs_fd, dir_ino, filename.c_str());
    if (!dir_ino || filename == "")
    {
        *reply = (LOOKUP3res){ .status = NFS3ERR_INVAL };
        rpc_queue_reply(rop);
        return 0;
    }
    self->parent->db->get(kv_direntry_key(dir_ino, filename), [=](int res, const std::string & value)
    {
        if (res < 0)
        {
            *reply = (LOOKUP3res){ .status = vitastor_nfs_map_err(-res) };
            rpc_queue_reply(rop);
            return;
        }
        std::string err;
        auto direntry = json11::Json::parse(value, err);
        if (err != "")
        {
            fprintf(stderr, "Invalid JSON in direntry %s = %s: %s\n", kv_direntry_key(dir_ino, filename).c_str(), value.c_str(), err.c_str());
            *reply = (LOOKUP3res){ .status = NFS3ERR_IO };
            rpc_queue_reply(rop);
            return;
        }
        uint64_t ino = direntry["ino"].uint64_value();
        kv_read_inode(self->parent, ino, [=](int res, const std::string & value, json11::Json ientry)
        {
            if (res < 0)
            {
                *reply = (LOOKUP3res){ .status = vitastor_nfs_map_err(res == -ENOENT ? -EIO : res) };
                rpc_queue_reply(rop);
                return;
            }
            *reply = (LOOKUP3res){
                .status = NFS3_OK,
                .resok = (LOOKUP3resok){
                    .object = xdr_copy_string(rop->xdrs, kv_fh(ino)),
                    .obj_attributes = {
                        .attributes_follow = 1,
                        .attributes = get_kv_attributes(self, ino, ientry),
                    },
                },
            };
            rpc_queue_reply(rop);
        });
    });
    return 1;
}

int kv_nfs3_readlink_proc(void *opaque, rpc_op_t *rop)
{
    nfs_client_t *self = (nfs_client_t*)opaque;
    READLINK3args *args = (READLINK3args*)rop->request;
    if (self->parent->trace)
        fprintf(stderr, "[%d] READLINK %ju\n", self->nfs_fd, kv_fh_inode(args->symlink));
    READLINK3res *reply = (READLINK3res*)rop->reply;
    if (!kv_fh_valid(args->symlink) || args->symlink == NFS_ROOT_HANDLE)
    {
        // Invalid filehandle or trying to read symlink from root entry
        *reply = (READLINK3res){ .status = NFS3ERR_INVAL };
        rpc_queue_reply(rop);
        return 0;
    }
    kv_read_inode(self->parent, kv_fh_inode(args->symlink), [=](int res, const std::string & value, json11::Json attrs)
    {
        if (res < 0)
        {
            *reply = (READLINK3res){ .status = vitastor_nfs_map_err(-res) };
        }
        else if (attrs["type"] != "link")
        {
            *reply = (READLINK3res){ .status = NFS3ERR_INVAL };
        }
        else
        {
            *reply = (READLINK3res){
                .status = NFS3_OK,
                .resok = (READLINK3resok){
                    .data = xdr_copy_string(rop->xdrs, attrs["symlink"].string_value()),
                },
            };
        }
        rpc_queue_reply(rop);
    });
    return 1;
}
