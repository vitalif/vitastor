// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// NFS proxy over VitastorKV database - WRITE

#include <sys/time.h>

#include "str_util.h"

#include "nfs_proxy.h"

#include "nfs/nfs.h"

#include "cli.h"

static void nfs_resize_write(nfs_client_t *self, rpc_op_t *rop, uint64_t inode, uint64_t new_size, uint64_t offset, uint64_t count, void *buf);

int kv_nfs3_write_proc(void *opaque, rpc_op_t *rop)
{
    nfs_client_t *self = (nfs_client_t*)opaque;
    WRITE3args *args = (WRITE3args*)rop->request;
    WRITE3res *reply = (WRITE3res*)rop->reply;
    inode_t ino = kv_fh_inode(args->file);
    if (!ino)
    {
        *reply = (WRITE3res){ .status = NFS3ERR_INVAL };
        rpc_queue_reply(rop);
        return 0;
    }
    if (args->count > MAX_REQUEST_SIZE)
    {
        *reply = (WRITE3res){ .status = NFS3ERR_INVAL };
        rpc_queue_reply(rop);
        return 0;
    }
    uint64_t count = args->count > args->data.size ? args->data.size : args->count;
    uint64_t alignment = self->parent->cli->st_cli.global_bitmap_granularity;
    auto pool_cfg = self->parent->cli->st_cli.pool_config.find(INODE_POOL(self->parent->fs_base_inode));
    if (pool_cfg != self->parent->cli->st_cli.pool_config.end())
    {
        alignment = pool_cfg->second.bitmap_granularity;
    }
    // Pre-fill reply
    *reply = (WRITE3res){
        .status = NFS3_OK,
        .resok = (WRITE3resok){
            //.file_wcc = ...,
            .count = (unsigned)count,
        },
    };
    if ((args->offset % alignment) != 0 || (count % alignment) != 0)
    {
        // Unaligned write, requires read-modify-write
        uint64_t aligned_offset = args->offset - (args->offset % alignment);
        uint64_t aligned_count = args->offset + args->count;
        if (aligned_count % alignment)
            aligned_count = aligned_count + alignment - (aligned_count % alignment);
        aligned_count -= aligned_offset;
        void *buf = malloc_or_die(aligned_count);
        xdr_add_malloc(rop->xdrs, buf);
        // Read
        cluster_op_t *op = new cluster_op_t;
        op->opcode = OSD_OP_READ;
        op->inode = self->parent->fs_base_inode + ino;
        op->offset = aligned_offset;
        op->len = aligned_count;
        op->iov.push_back(buf, aligned_count);
        op->callback = [self, rop, count](cluster_op_t *op)
        {
            if (op->retval != op->len)
            {
                WRITE3res *reply = (WRITE3res*)rop->reply;
                *reply = (WRITE3res){ .status = vitastor_nfs_map_err(-op->retval) };
                rpc_queue_reply(rop);
                return;
            }
            void *buf = op->iov.buf[0].iov_base;
            WRITE3args *args = (WRITE3args*)rop->request;
            memcpy((uint8_t*)buf + args->offset - op->offset, args->data.data, count);
            nfs_resize_write(self, rop, op->inode, args->offset+count, op->offset, op->len, buf);
            delete op;
        };
        self->parent->cli->execute(op);
    }
    else
    {
        nfs_resize_write(self, rop, ino, args->offset+count, args->offset, count, args->data.data);
    }
    return 1;
}

static void complete_extend_write(nfs_client_t *self, rpc_op_t *rop, inode_t inode, int res)
{
    WRITE3args *args = (WRITE3args*)rop->request;
    WRITE3res *reply = (WRITE3res*)rop->reply;
    if (res < 0)
    {
        *reply = (WRITE3res){ .status = vitastor_nfs_map_err(res) };
        rpc_queue_reply(rop);
        return;
    }
    bool imm = self->parent->cli->get_immediate_commit(inode);
    reply->resok.committed = args->stable != UNSTABLE || imm ? FILE_SYNC : UNSTABLE;
    *(uint64_t*)reply->resok.verf = self->parent->server_id;
    if (args->stable != UNSTABLE && !imm)
    {
        // Client requested a stable write. Add an fsync
        auto op = new cluster_op_t;
        op->opcode = OSD_OP_SYNC;
        op->callback = [rop](cluster_op_t *op)
        {
            if (op->retval != 0)
            {
                WRITE3res *reply = (WRITE3res*)rop->reply;
                *reply = (WRITE3res){ .status = vitastor_nfs_map_err(-op->retval) };
            }
            delete op;
            rpc_queue_reply(rop);
        };
        self->parent->cli->execute(op);
    }
    else
    {
        rpc_queue_reply(rop);
    }
}

static void complete_extend_inode(nfs_client_t *self, uint64_t inode, uint64_t new_size, int err)
{
    auto ext_it = self->extend_writes.lower_bound((extend_size_t){ .inode = inode, .new_size = 0 });
    while (ext_it != self->extend_writes.end() &&
        ext_it->first.inode == inode &&
        ext_it->first.new_size <= new_size)
    {
        ext_it->second.resize_res = err;
        if (ext_it->second.write_res <= 0)
        {
            complete_extend_write(self, ext_it->second.rop, inode, ext_it->second.write_res < 0
                ? ext_it->second.write_res : ext_it->second.resize_res);
            self->extend_writes.erase(ext_it++);
        }
        else
            ext_it++;
    }
}

static void extend_inode(nfs_client_t *self, uint64_t inode)
{
    // Send an extend request
    auto ext = &self->extends[inode];
    self->parent->db->set(kv_inode_key(inode), json11::Json(ext->attrs).dump().c_str(), [=](int res)
    {
        if (res < 0 && res != -EAGAIN)
        {
            fprintf(stderr, "Error extending inode %ju to %ju bytes: %s (code %d)\n", inode, ext->cur_extend, strerror(-res), res);
        }
        if (res == -EAGAIN || ext->next_extend > ext->cur_extend)
        {
            // Multiple concurrent resize requests received, try to repeat
            kv_read_inode(self, inode, [=](int res, const std::string & value, json11::Json attrs)
            {
                auto new_size = ext->next_extend > ext->cur_extend ? ext->next_extend : ext->cur_extend;
                if (res == 0 && attrs["size"].uint64_value() < new_size)
                {
                    ext->old_ientry = value;
                    ext->attrs = attrs.object_items();
                    ext->cur_extend = new_size;
                    ext->attrs["size"] = new_size;
                    extend_inode(self, inode);
                }
                else
                {
                    if (res < 0)
                    {
                        fprintf(stderr, "Error extending inode %ju to %ju bytes: %s (code %d)\n", inode, ext->cur_extend, strerror(-res), res);
                    }
                    ext->cur_extend = ext->next_extend = 0;
                    complete_extend_inode(self, inode, attrs["size"].uint64_value(), res);
                }
            });
            return;
        }
        auto new_size = ext->cur_extend;
        ext->cur_extend = ext->next_extend = 0;
        complete_extend_inode(self, inode, new_size, res);
    });
}

static void nfs_do_write(nfs_client_t *self, std::multimap<extend_size_t, extend_write_t>::iterator ewr_it,
    rpc_op_t *rop, uint64_t inode, uint64_t offset, uint64_t count, void *buf)
{
    cluster_op_t *op = new cluster_op_t;
    op->opcode = OSD_OP_WRITE;
    op->inode = self->parent->fs_base_inode + inode;
    op->offset = offset;
    op->len = count;
    op->iov.push_back(buf, count);
    op->callback = [self, ewr_it, rop](cluster_op_t *op)
    {
        auto inode = op->inode;
        int write_res = op->retval < 0 ? op->retval : (op->retval != op->len ? -ERANGE : 0);
        if (ewr_it == self->extend_writes.end())
        {
            complete_extend_write(self, rop, inode, write_res);
        }
        else
        {
            ewr_it->second.write_res = write_res;
            if (ewr_it->second.resize_res <= 0)
            {
                complete_extend_write(self, rop, inode, write_res < 0 ? write_res : ewr_it->second.resize_res);
                self->extend_writes.erase(ewr_it);
            }
        }
    };
    self->parent->cli->execute(op);
}

static void nfs_resize_write(nfs_client_t *self, rpc_op_t *rop, uint64_t inode, uint64_t new_size, uint64_t offset, uint64_t count, void *buf)
{
    // Check if we have to resize the inode during write
    kv_read_inode(self, inode, [=](int res, const std::string & value, json11::Json attrs)
    {
        if (res < 0)
        {
            *(WRITE3res*)rop->reply = (WRITE3res){ .status = vitastor_nfs_map_err(-res) };
            rpc_queue_reply(rop);
        }
        else if (kv_map_type(attrs["type"].string_value()) != NF3REG)
        {
            *(WRITE3res*)rop->reply = (WRITE3res){ .status = NFS3ERR_INVAL };
            rpc_queue_reply(rop);
        }
        else if (attrs["size"].uint64_value() < new_size)
        {
            auto ewr_it = self->extend_writes.emplace((extend_size_t){
                .inode = inode,
                .new_size = new_size,
            }, (extend_write_t){
                .rop = rop,
                .resize_res = 1,
                .write_res = 1,
            });
            auto ext = &self->extends[inode];
            if (ext->cur_extend > 0)
            {
                // Already resizing, just wait
                if (ext->next_extend < new_size)
                    ext->next_extend = new_size;
            }
            else
            {
                ext->old_ientry = value;
                ext->attrs = attrs.object_items();
                ext->cur_extend = new_size;
                ext->attrs["size"] = new_size;
                extend_inode(self, inode);
            }
            nfs_do_write(self, ewr_it, rop, inode, offset, count, buf);
        }
        else
        {
            nfs_do_write(self, self->extend_writes.end(), rop, inode, offset, count, buf);
        }
    });
}
