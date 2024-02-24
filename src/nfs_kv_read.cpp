// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// NFS proxy over VitastorKV database - READ

#include <sys/time.h>

#include "nfs_proxy.h"
#include "nfs_kv.h"

struct nfs_kv_read_state
{
    nfs_client_t *self = NULL;
    rpc_op_t *rop = NULL;
    bool allow_cache = true;
    inode_t ino = 0;
    uint64_t offset = 0, size = 0;
    std::function<void(int)> cb;
    // state
    int res = 0;
    json11::Json ientry;
    uint64_t aligned_size = 0, aligned_offset = 0;
    uint8_t *aligned_buf = NULL;
    cluster_op_t *op = NULL;
    uint8_t *buf = NULL;
};

static void nfs_kv_continue_read(nfs_kv_read_state *st, int state)
{
    if (state == 0)      {}
    else if (state == 1) goto resume_1;
    else if (state == 2) goto resume_2;
    else if (state == 3) goto resume_3;
    else
    {
        fprintf(stderr, "BUG: invalid state in nfs_kv_continue_read()");
        abort();
    }
    if (st->offset + sizeof(shared_file_header_t) < st->self->parent->shared_inode_threshold)
    {
        kv_read_inode(st->self, st->ino, [st](int res, const std::string & value, json11::Json attrs)
        {
            st->res = res;
            st->ientry = attrs;
            nfs_kv_continue_read(st, 1);
        }, st->allow_cache);
        return;
resume_1:
        if (st->res < 0 || kv_map_type(st->ientry["type"].string_value()) != NF3REG)
        {
            auto cb = std::move(st->cb);
            cb(st->res < 0 ? st->res : -EINVAL);
            return;
        }
        if (st->ientry["shared_ino"].uint64_value() != 0)
        {
            st->aligned_size = align_shared_size(st->self, st->offset+st->size);
            st->aligned_buf = (uint8_t*)malloc_or_die(st->aligned_size);
            st->buf = st->aligned_buf + sizeof(shared_file_header_t) + st->offset;
            st->op = new cluster_op_t;
            st->op->opcode = OSD_OP_READ;
            st->op->inode = st->self->parent->fs_base_inode + st->ientry["shared_ino"].uint64_value();
            st->op->offset = st->ientry["shared_offset"].uint64_value();
            if (st->offset+st->size > st->ientry["size"].uint64_value())
            {
                st->op->len = align_shared_size(st->self, st->ientry["size"].uint64_value());
                memset(st->aligned_buf+st->op->len, 0, st->aligned_size-st->op->len);
            }
            else
                st->op->len = st->aligned_size;
            st->op->iov.push_back(st->aligned_buf, st->op->len);
            st->op->callback = [st, state](cluster_op_t *op)
            {
                st->res = op->retval == op->len ? 0 : op->retval;
                delete op;
                nfs_kv_continue_read(st, 2);
            };
            st->self->parent->cli->execute(st->op);
            return;
resume_2:
            if (st->res < 0)
            {
                auto cb = std::move(st->cb);
                cb(st->res);
                return;
            }
            auto hdr = ((shared_file_header_t*)st->aligned_buf);
            if (hdr->magic != SHARED_FILE_MAGIC_V1 || hdr->inode != st->ino)
            {
                // Got unrelated data - retry from the beginning
                free(st->aligned_buf);
                st->aligned_buf = NULL;
                st->allow_cache = false;
                nfs_kv_continue_read(st, 0);
                return;
            }
            auto cb = std::move(st->cb);
            cb(0);
            return;
        }
    }
    st->aligned_offset = (st->offset & ~(st->self->parent->pool_alignment-1));
    st->aligned_size = ((st->offset + st->size + st->self->parent->pool_alignment-1) &
        ~(st->self->parent->pool_alignment-1)) - st->aligned_offset;
    st->aligned_buf = (uint8_t*)malloc_or_die(st->aligned_size);
    st->buf = st->aligned_buf + st->offset - st->aligned_offset;
    st->op = new cluster_op_t;
    st->op->opcode = OSD_OP_READ;
    st->op->inode = st->self->parent->fs_base_inode + st->ino;
    st->op->offset = st->aligned_offset;
    st->op->len = st->aligned_size;
    st->op->iov.push_back(st->aligned_buf, st->aligned_size);
    st->op->callback = [st](cluster_op_t *op)
    {
        st->res = op->retval;
        delete op;
        nfs_kv_continue_read(st, 3);
    };
    st->self->parent->cli->execute(st->op);
    return;
resume_3:
    auto cb = std::move(st->cb);
    cb(st->res < 0 ? st->res : 0);
    return;
}

int kv_nfs3_read_proc(void *opaque, rpc_op_t *rop)
{
    READ3args *args = (READ3args*)rop->request;
    READ3res *reply = (READ3res*)rop->reply;
    auto ino = kv_fh_inode(args->file);
    if (args->count > MAX_REQUEST_SIZE || !ino)
    {
        *reply = (READ3res){ .status = NFS3ERR_INVAL };
        rpc_queue_reply(rop);
        return 0;
    }
    auto st = new nfs_kv_read_state;
    st->self = (nfs_client_t*)opaque;
    st->rop = rop;
    st->ino = ino;
    st->offset = args->offset;
    st->size = args->count;
    st->cb = [st](int res)
    {
        READ3res *reply = (READ3res*)st->rop->reply;
        *reply = (READ3res){ .status = vitastor_nfs_map_err(res) };
        if (res == 0)
        {
            xdr_add_malloc(st->rop->xdrs, st->aligned_buf);
            reply->resok.data.data = (char*)st->buf;
            reply->resok.data.size = st->size;
            reply->resok.count = st->size;
            reply->resok.eof = 0;
        }
        rpc_queue_reply(st->rop);
        delete st;
    };
    if (st->self->parent->trace)
        fprintf(stderr, "[%d] READ %ju %ju+%ju\n", st->self->nfs_fd, st->ino, st->offset, st->size);
    nfs_kv_continue_read(st, 0);
    return 1;
}
