// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// NFS proxy over VitastorKV database - FSSTAT, FSINFO, PATHCONF

#include <sys/time.h>

#include "str_util.h"

#include "nfs_proxy.h"

#include "nfs/nfs.h"

#include "cli.h"

// Get file system statistics
int kv_nfs3_fsstat_proc(void *opaque, rpc_op_t *rop)
{
    nfs_client_t *self = (nfs_client_t*)opaque;
    //FSSTAT3args *args = (FSSTAT3args*)rop->request;
    if (self->parent->trace)
        fprintf(stderr, "[%d] FSSTAT\n", self->nfs_fd);
    FSSTAT3res *reply = (FSSTAT3res*)rop->reply;
    uint64_t tbytes = 0, fbytes = 0;
    auto pst_it = self->parent->pool_stats.find(self->parent->default_pool_id);
    if (pst_it != self->parent->pool_stats.end())
    {
        auto ttb = pst_it->second["total_raw_tb"].number_value();
        auto ftb = (pst_it->second["total_raw_tb"].number_value() - pst_it->second["used_raw_tb"].number_value());
        tbytes = ttb / pst_it->second["raw_to_usable"].number_value() * ((uint64_t)2<<40);
        fbytes = ftb / pst_it->second["raw_to_usable"].number_value() * ((uint64_t)2<<40);
    }
    *reply = (FSSTAT3res){
        .status = NFS3_OK,
        .resok = (FSSTAT3resok){
            .obj_attributes = {
                .attributes_follow = 0,
                //.attributes = get_root_attributes(self),
            },
            .tbytes = tbytes, // total bytes
            .fbytes = fbytes, // free bytes
            .abytes = fbytes, // available bytes
            .tfiles = (size3)(1 << 31), // maximum total files
            .ffiles = (size3)(1 << 31), // free files
            .afiles = (size3)(1 << 31), // available files
            .invarsec = 0,
        },
    };
    rpc_queue_reply(rop);
    return 0;
}

int kv_nfs3_fsinfo_proc(void *opaque, rpc_op_t *rop)
{
    nfs_client_t *self = (nfs_client_t*)opaque;
    FSINFO3args *args = (FSINFO3args*)rop->request;
    FSINFO3res *reply = (FSINFO3res*)rop->reply;
    if (self->parent->trace)
        fprintf(stderr, "[%d] FSINFO %s\n", self->nfs_fd, std::string(args->fsroot).c_str());
    if (args->fsroot != KV_ROOT_HANDLE)
    {
        *reply = (FSINFO3res){ .status = NFS3ERR_INVAL };
    }
    else
    {
        // Fill info
        *reply = (FSINFO3res){
            .status = NFS3_OK,
            .resok = (FSINFO3resok){
                .obj_attributes = {
                    .attributes_follow = 0,
                    //.attributes = get_root_attributes(self),
                },
                .rtmax = 128*1024*1024,
                .rtpref = 128*1024*1024,
                .rtmult = 4096,
                .wtmax = 128*1024*1024,
                .wtpref = 128*1024*1024,
                .wtmult = 4096,
                .dtpref = 128,
                .maxfilesize = 0x7fffffffffffffff,
                .time_delta = {
                    .seconds = 1,
                    .nseconds = 0,
                },
                .properties = FSF3_SYMLINK | FSF3_HOMOGENEOUS,
            },
        };
    }
    rpc_queue_reply(rop);
    return 0;
}

int kv_nfs3_pathconf_proc(void *opaque, rpc_op_t *rop)
{
    nfs_client_t *self = (nfs_client_t*)opaque;
    PATHCONF3args *args = (PATHCONF3args*)rop->request;
    PATHCONF3res *reply = (PATHCONF3res*)rop->reply;
    if (self->parent->trace)
        fprintf(stderr, "[%d] PATHCONF %s\n", self->nfs_fd, std::string(args->object).c_str());
    if (args->object != KV_ROOT_HANDLE)
    {
        *reply = (PATHCONF3res){ .status = NFS3ERR_INVAL };
    }
    else
    {
        // Fill info
        *reply = (PATHCONF3res){
            .status = NFS3_OK,
            .resok = (PATHCONF3resok){
                .obj_attributes = {
                    // Without at least one reference to a non-constant value (local variable or something else),
                    // with gcc 8 we get "internal compiler error: side-effects element in no-side-effects CONSTRUCTOR" here
                    // FIXME: get rid of this after raising compiler requirement
                    .attributes_follow = 0,
                    //.attributes = get_root_attributes(self),
                },
                .linkmax = 0,
                .name_max = 255,
                .no_trunc = TRUE,
                .chown_restricted = FALSE,
                .case_insensitive = FALSE,
                .case_preserving = TRUE,
            },
        };
    }
    rpc_queue_reply(rop);
    return 0;
}

nfs_client_t::nfs_client_t()
{
    struct rpc_service_proc_t pt[] = {
        {NFS_PROGRAM, NFS_V3, NFS3_NULL,        nfs3_null_proc,           NULL,                            0,                        NULL,                           0,                       this},
        {NFS_PROGRAM, NFS_V3, NFS3_GETATTR,     kv_nfs3_getattr_proc,     (xdrproc_t)xdr_GETATTR3args,     sizeof(GETATTR3args),     (xdrproc_t)xdr_GETATTR3res,     sizeof(GETATTR3res),     this},
        {NFS_PROGRAM, NFS_V3, NFS3_SETATTR,     kv_nfs3_setattr_proc,     (xdrproc_t)xdr_SETATTR3args,     sizeof(SETATTR3args),     (xdrproc_t)xdr_SETATTR3res,     sizeof(SETATTR3res),     this},
        {NFS_PROGRAM, NFS_V3, NFS3_LOOKUP,      kv_nfs3_lookup_proc,      (xdrproc_t)xdr_LOOKUP3args,      sizeof(LOOKUP3args),      (xdrproc_t)xdr_LOOKUP3res,      sizeof(LOOKUP3res),      this},
        {NFS_PROGRAM, NFS_V3, NFS3_ACCESS,      nfs3_access_proc,         (xdrproc_t)xdr_ACCESS3args,      sizeof(ACCESS3args),      (xdrproc_t)xdr_ACCESS3res,      sizeof(ACCESS3res),      this},
        {NFS_PROGRAM, NFS_V3, NFS3_READLINK,    kv_nfs3_readlink_proc,    (xdrproc_t)xdr_READLINK3args,    sizeof(READLINK3args),    (xdrproc_t)xdr_READLINK3res,    sizeof(READLINK3res),    this},
        {NFS_PROGRAM, NFS_V3, NFS3_READ,        kv_nfs3_read_proc,        (xdrproc_t)xdr_READ3args,        sizeof(READ3args),        (xdrproc_t)xdr_READ3res,        sizeof(READ3res),        this},
        {NFS_PROGRAM, NFS_V3, NFS3_WRITE,       kv_nfs3_write_proc,       (xdrproc_t)xdr_WRITE3args,       sizeof(WRITE3args),       (xdrproc_t)xdr_WRITE3res,       sizeof(WRITE3res),       this},
        {NFS_PROGRAM, NFS_V3, NFS3_CREATE,      kv_nfs3_create_proc,      (xdrproc_t)xdr_CREATE3args,      sizeof(CREATE3args),      (xdrproc_t)xdr_CREATE3res,      sizeof(CREATE3res),      this},
        {NFS_PROGRAM, NFS_V3, NFS3_MKDIR,       kv_nfs3_mkdir_proc,       (xdrproc_t)xdr_MKDIR3args,       sizeof(MKDIR3args),       (xdrproc_t)xdr_MKDIR3res,       sizeof(MKDIR3res),       this},
        {NFS_PROGRAM, NFS_V3, NFS3_SYMLINK,     kv_nfs3_symlink_proc,     (xdrproc_t)xdr_SYMLINK3args,     sizeof(SYMLINK3args),     (xdrproc_t)xdr_SYMLINK3res,     sizeof(SYMLINK3res),     this},
        {NFS_PROGRAM, NFS_V3, NFS3_MKNOD,       kv_nfs3_mknod_proc,       (xdrproc_t)xdr_MKNOD3args,       sizeof(MKNOD3args),       (xdrproc_t)xdr_MKNOD3res,       sizeof(MKNOD3res),       this},
        {NFS_PROGRAM, NFS_V3, NFS3_REMOVE,      kv_nfs3_remove_proc,      (xdrproc_t)xdr_REMOVE3args,      sizeof(REMOVE3args),      (xdrproc_t)xdr_REMOVE3res,      sizeof(REMOVE3res),      this},
        {NFS_PROGRAM, NFS_V3, NFS3_RMDIR,       kv_nfs3_rmdir_proc,       (xdrproc_t)xdr_RMDIR3args,       sizeof(RMDIR3args),       (xdrproc_t)xdr_RMDIR3res,       sizeof(RMDIR3res),       this},
        {NFS_PROGRAM, NFS_V3, NFS3_RENAME,      kv_nfs3_rename_proc,      (xdrproc_t)xdr_RENAME3args,      sizeof(RENAME3args),      (xdrproc_t)xdr_RENAME3res,      sizeof(RENAME3res),      this},
        {NFS_PROGRAM, NFS_V3, NFS3_LINK,        kv_nfs3_link_proc,        (xdrproc_t)xdr_LINK3args,        sizeof(LINK3args),        (xdrproc_t)xdr_LINK3res,        sizeof(LINK3res),        this},
        {NFS_PROGRAM, NFS_V3, NFS3_READDIR,     kv_nfs3_readdir_proc,     (xdrproc_t)xdr_READDIR3args,     sizeof(READDIR3args),     (xdrproc_t)xdr_READDIR3res,     sizeof(READDIR3res),     this},
        {NFS_PROGRAM, NFS_V3, NFS3_READDIRPLUS, kv_nfs3_readdirplus_proc, (xdrproc_t)xdr_READDIRPLUS3args, sizeof(READDIRPLUS3args), (xdrproc_t)xdr_READDIRPLUS3res, sizeof(READDIRPLUS3res), this},
        {NFS_PROGRAM, NFS_V3, NFS3_FSSTAT,      kv_nfs3_fsstat_proc,      (xdrproc_t)xdr_FSSTAT3args,      sizeof(FSSTAT3args),      (xdrproc_t)xdr_FSSTAT3res,      sizeof(FSSTAT3res),      this},
        {NFS_PROGRAM, NFS_V3, NFS3_FSINFO,      kv_nfs3_fsinfo_proc,      (xdrproc_t)xdr_FSINFO3args,      sizeof(FSINFO3args),      (xdrproc_t)xdr_FSINFO3res,      sizeof(FSINFO3res),      this},
        {NFS_PROGRAM, NFS_V3, NFS3_PATHCONF,    kv_nfs3_pathconf_proc,    (xdrproc_t)xdr_PATHCONF3args,    sizeof(PATHCONF3args),    (xdrproc_t)xdr_PATHCONF3res,    sizeof(PATHCONF3res),    this},
        {NFS_PROGRAM, NFS_V3, NFS3_COMMIT,      nfs3_commit_proc,         (xdrproc_t)xdr_COMMIT3args,      sizeof(COMMIT3args),      (xdrproc_t)xdr_COMMIT3res,      sizeof(COMMIT3res),      this},
        {MOUNT_PROGRAM, MOUNT_V3, MOUNT3_NULL,    nfs3_null_proc,         NULL,                            0,                        NULL,                         0,                         this},
        {MOUNT_PROGRAM, MOUNT_V3, MOUNT3_MNT,     mount3_mnt_proc,        (xdrproc_t)xdr_nfs_dirpath,      sizeof(nfs_dirpath),      (xdrproc_t)xdr_nfs_mountres3, sizeof(nfs_mountres3),     this},
        {MOUNT_PROGRAM, MOUNT_V3, MOUNT3_DUMP,    mount3_dump_proc,       NULL,                            0,                        (xdrproc_t)xdr_nfs_mountlist, sizeof(nfs_mountlist),     this},
        {MOUNT_PROGRAM, MOUNT_V3, MOUNT3_UMNT,    mount3_umnt_proc,       (xdrproc_t)xdr_nfs_dirpath,      sizeof(nfs_dirpath),      NULL,                         0,                         this},
        {MOUNT_PROGRAM, MOUNT_V3, MOUNT3_UMNTALL, mount3_umntall_proc,    NULL,                            0,                        NULL,                         0,                         this},
        {MOUNT_PROGRAM, MOUNT_V3, MOUNT3_EXPORT,  mount3_export_proc,     NULL,                            0,                        (xdrproc_t)xdr_nfs_exports,   sizeof(nfs_exports),       this},
    };
    for (int i = 0; i < sizeof(pt)/sizeof(pt[0]); i++)
    {
        proc_table.insert(pt[i]);
    }
}

nfs_client_t::~nfs_client_t()
{
}
