// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// NFS proxy over VitastorKV database - READ

#include <sys/time.h>

#include "str_util.h"

#include "nfs_proxy.h"

#include "nfs/nfs.h"

#include "cli.h"

int kv_nfs3_read_proc(void *opaque, rpc_op_t *rop)
{
    nfs_client_t *self = (nfs_client_t*)opaque;
    READ3args *args = (READ3args*)rop->request;
    READ3res *reply = (READ3res*)rop->reply;
    inode_t ino = kv_fh_inode(args->file);
    if (args->count > MAX_REQUEST_SIZE || !ino)
    {
        *reply = (READ3res){ .status = NFS3ERR_INVAL };
        rpc_queue_reply(rop);
        return 0;
    }
    uint64_t alignment = self->parent->cli->st_cli.global_bitmap_granularity;
    auto pool_cfg = self->parent->cli->st_cli.pool_config.find(INODE_POOL(self->parent->fs_base_inode));
    if (pool_cfg != self->parent->cli->st_cli.pool_config.end())
    {
        alignment = pool_cfg->second.bitmap_granularity;
    }
    uint64_t aligned_offset = args->offset - (args->offset % alignment);
    uint64_t aligned_count = args->offset + args->count;
    if (aligned_count % alignment)
        aligned_count = aligned_count + alignment - (aligned_count % alignment);
    aligned_count -= aligned_offset;
    void *buf = malloc_or_die(aligned_count);
    xdr_add_malloc(rop->xdrs, buf);
    cluster_op_t *op = new cluster_op_t;
    op->opcode = OSD_OP_READ;
    op->inode = self->parent->fs_base_inode + ino;
    op->offset = aligned_offset;
    op->len = aligned_count;
    op->iov.push_back(buf, aligned_count);
    *reply = (READ3res){ .status = NFS3_OK };
    reply->resok.data.data = (char*)buf + args->offset - aligned_offset;
    reply->resok.data.size = args->count;
    op->callback = [rop](cluster_op_t *op)
    {
        READ3res *reply = (READ3res*)rop->reply;
        if (op->retval != op->len)
        {
            *reply = (READ3res){ .status = vitastor_nfs_map_err(-op->retval) };
        }
        else
        {
            auto & reply_ok = reply->resok;
            // reply_ok.data.data is already set above
            reply_ok.count = reply_ok.data.size;
            reply_ok.eof = 0;
        }
        rpc_queue_reply(rop);
        delete op;
    };
    self->parent->cli->execute(op);
    return 1;
}
