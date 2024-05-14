// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// NFS proxy - common NULL, ACCESS, COMMIT, DUMP, EXPORT, MNT, UMNT, UMNTALL

#include <sys/time.h>

#include "nfs_proxy.h"
#include "proto/nfs.h"

nfsstat3 vitastor_nfs_map_err(int err)
{
    if (err < 0)
    {
        err = -err;
    }
    return (err == EINVAL ? NFS3ERR_INVAL
        : (err == ENOENT ? NFS3ERR_NOENT
        : (err == ENOSPC ? NFS3ERR_NOSPC
        : (err == EEXIST ? NFS3ERR_EXIST
        : (err == EISDIR ? NFS3ERR_ISDIR
        : (err == ENOTDIR ? NFS3ERR_NOTDIR
        : (err == ENOTEMPTY ? NFS3ERR_NOTEMPTY
        : (err == EIO ? NFS3ERR_IO : (err ? NFS3ERR_IO : NFS3_OK)))))))));
}

int nfs3_null_proc(void *opaque, rpc_op_t *rop)
{
    rpc_queue_reply(rop);
    return 0;
}

int nfs3_access_proc(void *opaque, rpc_op_t *rop)
{
    //nfs_client_t *self = (nfs_client_t*)opaque;
    ACCESS3args *args = (ACCESS3args*)rop->request;
    ACCESS3res *reply = (ACCESS3res*)rop->reply;
    *reply = (ACCESS3res){
        .status = NFS3_OK,
        .resok = (ACCESS3resok){
            .access = args->access,
        },
    };
    rpc_queue_reply(rop);
    return 0;
}

int nfs3_commit_proc(void *opaque, rpc_op_t *rop)
{
    nfs_client_t *self = (nfs_client_t*)opaque;
    //COMMIT3args *args = (COMMIT3args*)rop->request;
    cluster_op_t *op = new cluster_op_t;
    // fsync. we don't know how to fsync a single inode, so just fsync everything
    op->opcode = OSD_OP_SYNC;
    op->callback = [self, rop](cluster_op_t *op)
    {
        COMMIT3res *reply = (COMMIT3res*)rop->reply;
        *reply = (COMMIT3res){ .status = vitastor_nfs_map_err(op->retval) };
        *(uint64_t*)reply->resok.verf = self->parent->server_id;
        rpc_queue_reply(rop);
    };
    self->parent->cli->execute(op);
    return 1;
}

int mount3_mnt_proc(void *opaque, rpc_op_t *rop)
{
    nfs_client_t *self = (nfs_client_t*)opaque;
    //nfs_dirpath *args = (nfs_dirpath*)rop->request;
    if (self->parent->trace)
        fprintf(stderr, "[%d] MNT\n", self->nfs_fd);
    nfs_mountres3 *reply = (nfs_mountres3*)rop->reply;
    u_int flavor = RPC_AUTH_NONE;
    reply->fhs_status = MNT3_OK;
    reply->mountinfo.fhandle = xdr_copy_string(rop->xdrs, NFS_ROOT_HANDLE);
    reply->mountinfo.auth_flavors.auth_flavors_len = 1;
    reply->mountinfo.auth_flavors.auth_flavors_val = (u_int*)xdr_copy_string(rop->xdrs, (char*)&flavor, sizeof(u_int)).data;
    rpc_queue_reply(rop);
    return 0;
}

int mount3_dump_proc(void *opaque, rpc_op_t *rop)
{
    nfs_client_t *self = (nfs_client_t*)opaque;
    if (self->parent->trace)
        fprintf(stderr, "[%d] DUMP\n", self->nfs_fd);
    nfs_mountlist *reply = (nfs_mountlist*)rop->reply;
    *reply = (struct nfs_mountbody*)malloc_or_die(sizeof(struct nfs_mountbody));
    xdr_add_malloc(rop->xdrs, *reply);
    (*reply)->ml_hostname = xdr_copy_string(rop->xdrs, "127.0.0.1");
    (*reply)->ml_directory = xdr_copy_string(rop->xdrs, self->parent->export_root);
    (*reply)->ml_next = NULL;
    rpc_queue_reply(rop);
    return 0;
}

int mount3_umnt_proc(void *opaque, rpc_op_t *rop)
{
    //nfs_client_t *self = (nfs_client_t*)opaque;
    //nfs_dirpath *arg = (nfs_dirpath*)rop->request;
    // do nothing
    rpc_queue_reply(rop);
    return 0;
}

int mount3_umntall_proc(void *opaque, rpc_op_t *rop)
{
    // do nothing
    rpc_queue_reply(rop);
    return 0;
}

int mount3_export_proc(void *opaque, rpc_op_t *rop)
{
    nfs_client_t *self = (nfs_client_t*)opaque;
    nfs_exports *reply = (nfs_exports*)rop->reply;
    *reply = (struct nfs_exportnode*)calloc_or_die(1, sizeof(struct nfs_exportnode) + sizeof(struct nfs_groupnode));
    xdr_add_malloc(rop->xdrs, *reply);
    (*reply)->ex_dir = xdr_copy_string(rop->xdrs, self->parent->export_root);
    (*reply)->ex_groups = (struct nfs_groupnode*)(reply+1);
    (*reply)->ex_groups->gr_name = xdr_copy_string(rop->xdrs, "127.0.0.1");
    (*reply)->ex_groups->gr_next = NULL;
    (*reply)->ex_next = NULL;
    rpc_queue_reply(rop);
    return 0;
}
