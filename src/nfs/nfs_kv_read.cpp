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
    int eof = 0;
    json11::Json ientry;
    std::string ientry_text;
    uint64_t aligned_size = 0, aligned_offset = 0;
    uint8_t *aligned_buf = NULL;
    cluster_op_t *op = NULL;
    uint8_t *buf = NULL;
    int retry = 0;
};

#define align_down(size) ((size) & ~(st->self->parent->kvfs->pool_alignment-1))
#define align_up(size) (((size) + st->self->parent->kvfs->pool_alignment-1) & ~(st->self->parent->kvfs->pool_alignment-1))

static void nfs_kv_continue_read(nfs_kv_read_state *st, int state)
{
    if (state == 0)      {}
    else if (state == 1) goto resume_1;
    else if (state == 2) goto resume_2;
    else if (state == 3) goto resume_3;
    else if (state == 4) goto resume_4;
    else
    {
        fprintf(stderr, "BUG: invalid state in nfs_kv_continue_read()");
        abort();
    }
resume_0:
    if (st->offset + sizeof(shared_file_header_t) < st->self->parent->kvfs->shared_inode_threshold)
    {
        kv_read_inode(st->self->parent, st->ino, [st](int res, const std::string & value, json11::Json attrs)
        {
            st->res = res;
            if (st->retry > 0 && !res && st->ientry_text == value)
            {
                fprintf(stderr, "Error: inode 0x%jx didn't change after retry - file data is lost?\n", st->ino);
                st->res = -EIO;
            }
            st->ientry_text = value;
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
        if (st->self->parent->enforce_perms && !kv_is_accessible(st->rop->auth_sys, st->ientry, ACCESS3_READ))
        {
            auto cb = std::move(st->cb);
            cb(-EACCES);
            return;
        }
        if (st->ientry["shared_ino"].uint64_value() != 0)
        {
            if (st->offset >= st->ientry["size"].uint64_value())
            {
                st->size = 0;
                st->eof = 1;
                auto cb = std::move(st->cb);
                cb(0);
                return;
            }
            st->op = new cluster_op_t;
            {
                st->op->opcode = OSD_OP_READ;
                st->op->inode = st->ientry["shared_ino"].uint64_value();
                // Always read including header to react if the file was possibly moved away
                auto read_offset = st->ientry["shared_offset"].uint64_value();
                st->op->offset = align_down(read_offset);
                if (st->op->offset < read_offset)
                {
                    st->op->iov.push_back(st->self->parent->kvfs->scrap_block.data(),
                        read_offset-st->op->offset);
                }
                auto read_size = st->offset+st->size;
                if (read_size > st->ientry["size"].uint64_value())
                {
                    st->eof = 1;
                    st->size = st->ientry["size"].uint64_value()-st->offset;
                    read_size = st->ientry["size"].uint64_value();
                }
                read_size += sizeof(shared_file_header_t);
                assert(!st->aligned_buf);
                st->aligned_buf = (uint8_t*)st->self->malloc_or_rdma(st->rop, read_size);
                st->buf = st->aligned_buf + sizeof(shared_file_header_t) + st->offset;
                st->op->iov.push_back(st->aligned_buf, read_size);
                st->op->len = align_up(read_offset+read_size) - st->op->offset;
                if (read_offset+read_size < st->op->offset+st->op->len)
                {
                    st->op->iov.push_back(st->self->parent->kvfs->scrap_block.data(),
                        st->op->offset+st->op->len - (read_offset+read_size));
                }
            }
            st->op->callback = [st](cluster_op_t *op)
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
                st->self->free_or_rdma(st->rop, st->aligned_buf);
                st->aligned_buf = NULL;
                auto cb = std::move(st->cb);
                cb(st->res);
                return;
            }
            auto hdr = ((shared_file_header_t*)st->aligned_buf);
            if (hdr->magic != SHARED_FILE_MAGIC_V1 || hdr->inode != st->ino)
            {
                // Got unrelated data - retry from the beginning
                fprintf(stderr, "Warning: got unrelated data for inode 0x%jx from shared inode"
                    " 0x%jx offset 0x%jx: probably a read/write conflict, retrying\n",
                    st->ino, st->ientry["shared_ino"].uint64_value(), st->ientry["shared_offset"].uint64_value());
                st->retry++;
                st->self->free_or_rdma(st->rop, st->aligned_buf);
                st->aligned_buf = NULL;
                st->allow_cache = false;
                goto resume_0;
            }
            auto cb = std::move(st->cb);
            cb(0);
            return;
        }
    }
    else if (st->self->rdma_conn || st->self->parent->enforce_perms)
    {
        // Take ientry from read_hack_cache for RDMA connections or for the permission check
        {
            auto rh_it = st->self->parent->kvfs->read_hack_cache.find(st->ino);
            if (rh_it != st->self->parent->kvfs->read_hack_cache.end())
            {
                st->ientry = rh_it->second;
            }
        }
        if (st->ientry.is_null())
        {
            kv_read_inode(st->self->parent, st->ino, [st](int res, const std::string & value, json11::Json attrs)
            {
                st->res = res;
                st->ientry = attrs;
                nfs_kv_continue_read(st, 4);
            }, st->allow_cache);
            return;
resume_4:
            if (st->res < 0 || kv_map_type(st->ientry["type"].string_value()) != NF3REG)
            {
                auto cb = std::move(st->cb);
                cb(st->res < 0 ? st->res : -EINVAL);
                return;
            }
            st->self->parent->kvfs->read_hack_cache[st->ino] = st->ientry;
        }
        if (st->self->parent->enforce_perms && !kv_is_accessible(st->rop->auth_sys, st->ientry, ACCESS3_READ))
        {
            auto cb = std::move(st->cb);
            cb(-EACCES);
            return;
        }
    }
    st->aligned_offset = align_down(st->offset);
    st->aligned_size = align_up(st->offset+st->size) - st->aligned_offset;
    assert(!st->aligned_buf);
    st->aligned_buf = (uint8_t*)st->self->malloc_or_rdma(st->rop, st->aligned_size);
    st->buf = st->aligned_buf + st->offset - st->aligned_offset;
    st->op = new cluster_op_t;
    st->op->opcode = OSD_OP_READ;
    st->op->inode = st->ino;
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
    if (st->res < 0)
    {
        st->self->free_or_rdma(st->rop, st->aligned_buf);
        st->aligned_buf = NULL;
    }
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
            reply->resok.data.data = (char*)st->buf;
            reply->resok.data.size = st->size;
            reply->resok.count = st->size;
            reply->resok.eof = st->eof;
            if (st->self->rdma_conn)
            {
                // FIXME Linux NFS RDMA transport has a bug - when the reply
                // doesn't contain post_op_attr, the data gets offsetted by
                // 84 bytes (size of attributes)...
                // So we have to fill it with RDMA. :-(
                reply->resok.file_attributes = (post_op_attr){
                    .attributes_follow = 1,
                    .attributes = get_kv_attributes(st->self->parent, st->ino, st->ientry),
                };
            }
        }
        rpc_queue_reply(st->rop);
        delete st;
    };
    if (st->self->parent->trace)
        fprintf(stderr, "[%d] READ %ju %ju+%ju\n", st->self->nfs_fd, st->ino, st->offset, st->size);
    nfs_kv_continue_read(st, 0);
    return 1;
}
