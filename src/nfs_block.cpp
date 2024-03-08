// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// NFS proxy over Vitastor block images

#include <sys/time.h>

#include "str_util.h"

#include "nfs_proxy.h"
#include "nfs_common.h"
#include "nfs_block.h"
#include "nfs/nfs.h"
#include "cli.h"

static unsigned len_pad4(unsigned len)
{
    return len + (len&3 ? 4-(len&3) : 0);
}

static std::string get_inode_name(nfs_client_t *self, diropargs3 & what)
{
    // Get name
    std::string dirhash = what.dir;
    std::string dir;
    if (dirhash != NFS_ROOT_HANDLE)
    {
        auto dir_it = self->parent->blockfs->dir_by_hash.find(dirhash);
        if (dir_it != self->parent->blockfs->dir_by_hash.end())
            dir = dir_it->second;
        else
            return "";
    }
    std::string name = what.name;
    return (dir.size()
        ? dir+"/"+name
        : self->parent->blockfs->name_prefix+name);
}

static fattr3 get_dir_attributes(nfs_client_t *self, std::string dir)
{
    auto & dinf = self->parent->blockfs->dir_info.at(dir);
    return (fattr3){
        .type = NF3DIR,
        .mode = 0755,
        .nlink = 1,
        .uid = 0,
        .gid = 0,
        .size = 4096,
        .used = 4096,
        .rdev = (specdata3){ 0 },
        .fsid = self->parent->fsid,
        .fileid = dinf.id,
        .atime = (nfstime3){ .seconds = (u_int)dinf.mtime.tv_sec, .nseconds = (u_int)dinf.mtime.tv_nsec },
        .mtime = (nfstime3){ .seconds = (u_int)dinf.mtime.tv_sec, .nseconds = (u_int)dinf.mtime.tv_nsec },
        .ctime = (nfstime3){ .seconds = (u_int)dinf.mtime.tv_sec, .nseconds = (u_int)dinf.mtime.tv_nsec },
    };
}

static fattr3 get_file_attributes(nfs_client_t *self, inode_t inode_num)
{
    auto & inode_cfg = self->parent->cli->st_cli.inode_config.at(inode_num);
    uint64_t used = 0;
    auto st_it = self->parent->inode_stats.find(inode_num);
    if (st_it != self->parent->inode_stats.end())
    {
        used = st_it->second["raw_used"].uint64_value();
        auto pst_it = self->parent->pool_stats.find(INODE_POOL(inode_num));
        if (pst_it != self->parent->pool_stats.end())
        {
            used /= pst_it->second["raw_to_usable"].number_value();
        }
    }
    return (fattr3){
        .type = NF3REG,
        .mode = 0644,
        .nlink = 1,
        .uid = 0,
        .gid = 0,
        .size = inode_cfg.size,
        .used = used,
        .rdev = (specdata3){ 0 },
        .fsid = self->parent->fsid,
        .fileid = inode_num,
        //.atime = (nfstime3){ .seconds = now.tv_sec, .nseconds = now.tv_nsec },
        //.mtime = (nfstime3){ .seconds = now.tv_sec, .nseconds = now.tv_nsec },
        //.ctime = (nfstime3){ .seconds = now.tv_sec, .nseconds = now.tv_nsec },
    };
}

static int block_nfs3_getattr_proc(void *opaque, rpc_op_t *rop)
{
    nfs_client_t *self = (nfs_client_t*)opaque;
    GETATTR3args *args = (GETATTR3args*)rop->request;
    GETATTR3res *reply = (GETATTR3res*)rop->reply;
    bool is_dir = false;
    std::string dirhash = args->object;
    std::string dir;
    if (args->object == NFS_ROOT_HANDLE)
        is_dir = true;
    else
    {
        auto dir_it = self->parent->blockfs->dir_by_hash.find(dirhash);
        if (dir_it != self->parent->blockfs->dir_by_hash.end())
        {
            is_dir = true;
            dir = dir_it->second;
        }
    }
    if (is_dir)
    {
        // Directory info
        *reply = (GETATTR3res){
            .status = NFS3_OK,
            .resok = (GETATTR3resok){
                .obj_attributes = get_dir_attributes(self, dir),
            },
        };
    }
    else
    {
        uint64_t inode_num = 0;
        auto inode_num_it = self->parent->blockfs->inode_by_hash.find(dirhash);
        if (inode_num_it != self->parent->blockfs->inode_by_hash.end())
            inode_num = inode_num_it->second;
        auto inode_it = self->parent->cli->st_cli.inode_config.find(inode_num);
        if (inode_num && inode_it != self->parent->cli->st_cli.inode_config.end())
        {
            // File info
            auto & inode_cfg = inode_it->second;
            *reply = (GETATTR3res){
                .status = NFS3_OK,
                .resok = (GETATTR3resok){
                    .obj_attributes = {
                        .type = NF3REG,
                        .mode = 0644,
                        .nlink = 1,
                        .uid = 0,
                        .gid = 0,
                        .size = inode_cfg.size,
                        .used = inode_cfg.size,
                        .rdev = (specdata3){ 0 },
                        .fsid = self->parent->fsid,
                        .fileid = inode_it->first,
                        //.atime = (nfstime3){ .seconds = now.tv_sec, .nseconds = now.tv_nsec },
                        //.mtime = (nfstime3){ .seconds = now.tv_sec, .nseconds = now.tv_nsec },
                        //.ctime = (nfstime3){ .seconds = now.tv_sec, .nseconds = now.tv_nsec },
                    },
                },
            };
        }
        else
        {
            // File not exists
            *reply = (GETATTR3res){ .status = NFS3ERR_NOENT };
        }
    }
    rpc_queue_reply(rop);
    return 0;
}

static int block_nfs3_setattr_proc(void *opaque, rpc_op_t *rop)
{
    nfs_client_t *self = (nfs_client_t*)opaque;
    SETATTR3args *args = (SETATTR3args*)rop->request;
    SETATTR3res *reply = (SETATTR3res*)rop->reply;
    std::string handle = args->object;
    auto ino_it = self->parent->blockfs->inode_by_hash.find(handle);
    if (ino_it == self->parent->blockfs->inode_by_hash.end())
    {
        if (handle == NFS_ROOT_HANDLE || self->parent->blockfs->dir_by_hash.find(handle) != self->parent->blockfs->dir_by_hash.end())
        {
            if (args->new_attributes.size.set_it)
            {
                *reply = (SETATTR3res){ .status = NFS3ERR_ISDIR };
            }
            else
            {
                // Silently ignore mode, uid, gid, atime, mtime changes
                *reply = (SETATTR3res){ .status = NFS3_OK };
            }
        }
        else
        {
            *reply = (SETATTR3res){ .status = NFS3ERR_NOENT };
        }
        rpc_queue_reply(rop);
        return 0;
    }
    if (args->new_attributes.size.set_it)
    {
        auto & inode_cfg = self->parent->cli->st_cli.inode_config.at(ino_it->second);
        self->parent->cmd->loop_and_wait(self->parent->cmd->start_modify(json11::Json::object {
            { "image", inode_cfg.name },
            { "resize", (uint64_t)args->new_attributes.size.size },
            { "force_size", true },
        }), [rop](const cli_result_t & r)
        {
            SETATTR3res *reply = (SETATTR3res*)rop->reply;
            *reply = (SETATTR3res){ .status = vitastor_nfs_map_err(r.err) };
            rpc_queue_reply(rop);
        });
        return 1;
    }
    // Silently ignore mode, uid, gid, atime, mtime changes
    *reply = (SETATTR3res){ .status = NFS3_OK };
    rpc_queue_reply(rop);
    return 0;
}

static int block_nfs3_lookup_proc(void *opaque, rpc_op_t *rop)
{
    nfs_client_t *self = (nfs_client_t*)opaque;
    LOOKUP3args *args = (LOOKUP3args*)rop->request;
    LOOKUP3res *reply = (LOOKUP3res*)rop->reply;
    std::string full_name = get_inode_name(self, args->what);
    if (full_name != "")
    {
        std::string fh = "S"+base64_encode(sha256(full_name));
        for (auto & ic: self->parent->cli->st_cli.inode_config)
        {
            if (ic.second.name == full_name)
            {
                *reply = (LOOKUP3res){
                    .status = NFS3_OK,
                    .resok = (LOOKUP3resok){
                        .object = xdr_copy_string(rop->xdrs, fh),
                        .obj_attributes = {
                            .attributes_follow = 1,
                            .attributes = get_file_attributes(self, ic.first),
                        },
                    },
                };
                rpc_queue_reply(rop);
                return 0;
            }
        }
        auto dir_it = self->parent->blockfs->dir_info.find(full_name);
        if (dir_it != self->parent->blockfs->dir_info.end())
        {
            *reply = (LOOKUP3res){
                .status = NFS3_OK,
                .resok = (LOOKUP3resok){
                    .object = xdr_copy_string(rop->xdrs, fh),
                    .obj_attributes = {
                        .attributes_follow = 1,
                        .attributes = get_dir_attributes(self, full_name),
                    },
                },
            };
            rpc_queue_reply(rop);
            return 0;
        }
    }
    *reply = (LOOKUP3res){ .status = NFS3ERR_NOENT };
    rpc_queue_reply(rop);
    return 0;
}

static int block_nfs3_access_proc(void *opaque, rpc_op_t *rop)
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

static int block_nfs3_readlink_proc(void *opaque, rpc_op_t *rop)
{
    //nfs_client_t *self = (nfs_client_t*)opaque;
    //READLINK3args *args = (READLINK3args*)rop->request;
    READLINK3res *reply = (READLINK3res*)rop->reply;
    // Not supported yet
    *reply = (READLINK3res){ .status = NFS3ERR_NOTSUPP };
    rpc_queue_reply(rop);
    return 0;
}

static int block_nfs3_read_proc(void *opaque, rpc_op_t *rop)
{
    nfs_client_t *self = (nfs_client_t*)opaque;
    READ3args *args = (READ3args*)rop->request;
    READ3res *reply = (READ3res*)rop->reply;
    std::string handle = args->file;
    auto ino_it = self->parent->blockfs->inode_by_hash.find(handle);
    if (ino_it == self->parent->blockfs->inode_by_hash.end())
    {
        *reply = (READ3res){ .status = NFS3ERR_NOENT };
        rpc_queue_reply(rop);
        return 0;
    }
    if (args->count > MAX_REQUEST_SIZE)
    {
        *reply = (READ3res){ .status = NFS3ERR_INVAL };
        rpc_queue_reply(rop);
        return 0;
    }
    uint64_t alignment = self->parent->cli->st_cli.global_bitmap_granularity;
    auto pool_cfg = self->parent->cli->st_cli.pool_config.find(INODE_POOL(ino_it->second));
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
    op->inode = ino_it->second;
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

static void nfs_resize_write(nfs_client_t *self, rpc_op_t *rop, uint64_t inode, uint64_t new_size, uint64_t offset, uint64_t count, void *buf);

static int block_nfs3_write_proc(void *opaque, rpc_op_t *rop)
{
    nfs_client_t *self = (nfs_client_t*)opaque;
    WRITE3args *args = (WRITE3args*)rop->request;
    WRITE3res *reply = (WRITE3res*)rop->reply;
    std::string handle = args->file;
    auto ino_it = self->parent->blockfs->inode_by_hash.find(handle);
    if (ino_it == self->parent->blockfs->inode_by_hash.end())
    {
        *reply = (WRITE3res){ .status = NFS3ERR_NOENT };
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
    auto pool_cfg = self->parent->cli->st_cli.pool_config.find(INODE_POOL(ino_it->second));
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
        op->inode = ino_it->second;
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
        nfs_resize_write(self, rop, ino_it->second, args->offset+count, args->offset, count, args->data.data);
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
    auto ext_it = self->parent->blockfs->extend_writes.lower_bound((extend_size_t){ .inode = inode, .new_size = 0 });
    while (ext_it != self->parent->blockfs->extend_writes.end() &&
        ext_it->first.inode == inode &&
        ext_it->first.new_size <= new_size)
    {
        ext_it->second.resize_res = err;
        if (ext_it->second.write_res <= 0)
        {
            complete_extend_write(self, ext_it->second.rop, inode, ext_it->second.write_res < 0
                ? ext_it->second.write_res : ext_it->second.resize_res);
            self->parent->blockfs->extend_writes.erase(ext_it++);
        }
        else
            ext_it++;
    }
}

static void extend_inode(nfs_client_t *self, uint64_t inode, uint64_t new_size)
{
    // Send an extend request
    auto & ext = self->parent->blockfs->extends[inode];
    ext.cur_extend = new_size;
    auto inode_it = self->parent->cli->st_cli.inode_config.find(inode);
    if (inode_it != self->parent->cli->st_cli.inode_config.end() &&
        inode_it->second.size < new_size)
    {
        self->parent->cmd->loop_and_wait(self->parent->cmd->start_modify(json11::Json::object {
            // FIXME: Resizing by ID is probably more correct
            { "image", inode_it->second.name },
            { "resize", new_size },
            { "inc_size", true },
            { "force_size", true },
        }), [=](const cli_result_t & r)
        {
            auto & ext = self->parent->blockfs->extends[inode];
            if (r.err)
            {
                fprintf(stderr, "Error extending inode %lu to %lu bytes: %s\n", inode, new_size, r.text.c_str());
            }
            if (r.err == EAGAIN || ext.next_extend > ext.cur_extend)
            {
                // Multiple concurrent resize requests received, try to repeat
                extend_inode(self, inode, ext.next_extend > ext.cur_extend ? ext.next_extend : ext.cur_extend);
                return;
            }
            ext.cur_extend = ext.next_extend = 0;
            complete_extend_inode(self, inode, new_size, r.err);
        });
    }
    else
    {
        complete_extend_inode(self, inode, new_size, 0);
    }
}

static void nfs_do_write(nfs_client_t *self, std::multimap<extend_size_t, extend_write_t>::iterator ewr_it,
    rpc_op_t *rop, uint64_t inode, uint64_t offset, uint64_t count, void *buf)
{
    cluster_op_t *op = new cluster_op_t;
    op->opcode = OSD_OP_WRITE;
    op->inode = inode;
    op->offset = offset;
    op->len = count;
    op->iov.push_back(buf, count);
    op->callback = [self, ewr_it, rop](cluster_op_t *op)
    {
        auto inode = op->inode;
        int write_res = op->retval < 0 ? op->retval : (op->retval != op->len ? -ERANGE : 0);
        if (ewr_it == self->parent->blockfs->extend_writes.end())
        {
            complete_extend_write(self, rop, inode, write_res);
        }
        else
        {
            ewr_it->second.write_res = write_res;
            if (ewr_it->second.resize_res <= 0)
            {
                complete_extend_write(self, rop, inode, write_res < 0 ? write_res : ewr_it->second.resize_res);
                self->parent->blockfs->extend_writes.erase(ewr_it);
            }
        }
    };
    self->parent->cli->execute(op);
}

static void nfs_resize_write(nfs_client_t *self, rpc_op_t *rop, uint64_t inode, uint64_t new_size, uint64_t offset, uint64_t count, void *buf)
{
    // Check if we have to resize the inode during write
    auto inode_it = self->parent->cli->st_cli.inode_config.find(inode);
    if (inode_it != self->parent->cli->st_cli.inode_config.end() &&
        inode_it->second.size < new_size)
    {
        auto ewr_it = self->parent->blockfs->extend_writes.emplace((extend_size_t){
            .inode = inode,
            .new_size = new_size,
        }, (extend_write_t){
            .rop = rop,
            .resize_res = 1,
            .write_res = 1,
        });
        auto & ext = self->parent->blockfs->extends[inode];
        if (ext.cur_extend > 0)
        {
            // Already resizing, just wait
            if (ext.next_extend < new_size)
                ext.next_extend = new_size;
        }
        else
        {
            extend_inode(self, inode, new_size);
        }
        nfs_do_write(self, ewr_it, rop, inode, offset, count, buf);
    }
    else
    {
        nfs_do_write(self, self->parent->blockfs->extend_writes.end(), rop, inode, offset, count, buf);
    }
}

static int block_nfs3_create_proc(void *opaque, rpc_op_t *rop)
{
    nfs_client_t *self = (nfs_client_t*)opaque;
    CREATE3args *args = (CREATE3args*)rop->request;
    CREATE3res *reply = (CREATE3res*)rop->reply;
    std::string full_name = get_inode_name(self, args->where);
    if (full_name == "")
    {
        *reply = (CREATE3res){ .status = NFS3ERR_NOENT };
        rpc_queue_reply(rop);
        return 0;
    }
    // Run create command
    self->parent->cmd->loop_and_wait(self->parent->cmd->start_create(json11::Json::object {
        { "image", full_name },
        { "pool", self->parent->default_pool },
        { "size", args->how.mode == NFS_EXCLUSIVE || !args->how.obj_attributes.size.set_it ? 0 : args->how.obj_attributes.size.size },
        { "force_size", true },
    }), [self, rop, full_name](const cli_result_t & r)
    {
        if (r.err)
            fprintf(stderr, "create(%s) failed: %s (code %d)\n", full_name.c_str(), r.text.c_str(), r.err);
        CREATE3res *reply = (CREATE3res*)rop->reply;
        *reply = (CREATE3res){ .status = vitastor_nfs_map_err(r.err) };
        if (!r.err)
        {
            auto inode_num = self->parent->cli->st_cli.inode_by_name.at(full_name);
            reply->resok = (CREATE3resok){
                .obj = {
                    .handle_follows = 1,
                    .handle = xdr_copy_string(rop->xdrs, "S"+base64_encode(sha256(full_name))),
                },
                .obj_attributes = {
                    .attributes_follow = 1,
                    .attributes = get_file_attributes(self, inode_num),
                },
                .dir_wcc = {
                    .before = {
                        .attributes_follow = 0,
                    },
                    .after = {
                        .attributes_follow = 0,
                    },
                },
            };
        }
        rpc_queue_reply(rop);
    });
    return 1;
}

static int block_nfs3_mkdir_proc(void *opaque, rpc_op_t *rop)
{
    nfs_client_t *self = (nfs_client_t*)opaque;
    MKDIR3args *args = (MKDIR3args*)rop->request;
    MKDIR3res *reply = (MKDIR3res*)rop->reply;
    std::string full_name = get_inode_name(self, args->where);
    if (full_name == "")
    {
        *reply = (MKDIR3res){ .status = NFS3ERR_NOENT };
        rpc_queue_reply(rop);
        return 0;
    }
    auto inode_it = self->parent->cli->st_cli.inode_by_name.find(full_name);
    if (inode_it != self->parent->cli->st_cli.inode_by_name.end())
    {
        *reply = (MKDIR3res){ .status = NFS3ERR_EXIST };
        rpc_queue_reply(rop);
        return 0;
    }
    auto dir_id_it = self->parent->blockfs->dir_info.find(full_name);
    if (dir_id_it != self->parent->blockfs->dir_info.end())
    {
        *reply = (MKDIR3res){ .status = NFS3ERR_EXIST };
        rpc_queue_reply(rop);
        return 0;
    }
    // FIXME: Persist empty directories in some etcd keys, like /vitastor/dir/...
    self->parent->blockfs->dir_info[full_name] = (nfs_dir_t){
        .id = self->parent->blockfs->next_dir_id++,
        .mod_rev = 0,
    };
    self->parent->blockfs->dir_by_hash["S"+base64_encode(sha256(full_name))] = full_name;
    *reply = (MKDIR3res){
        .status = NFS3_OK,
        .resok = (MKDIR3resok){
            .obj = {
                .handle_follows = 1,
                .handle = xdr_copy_string(rop->xdrs, "S"+base64_encode(sha256(full_name))),
            },
            .obj_attributes = {
                .attributes_follow = 1,
                .attributes = get_dir_attributes(self, full_name),
            },
            //.dir_wcc = ...
        },
    };
    rpc_queue_reply(rop);
    return 0;
}

static int block_nfs3_symlink_proc(void *opaque, rpc_op_t *rop)
{
//    nfs_client_t *self = (nfs_client_t*)opaque;
//    SYMLINK3args *args = (SYMLINK3args*)rop->request;
    SYMLINK3res *reply = (SYMLINK3res*)rop->reply;
    // Not supported yet
    *reply = (SYMLINK3res){ .status = NFS3ERR_NOTSUPP };
    rpc_queue_reply(rop);
    return 0;
}

static int block_nfs3_mknod_proc(void *opaque, rpc_op_t *rop)
{
//    nfs_client_t *self = (nfs_client_t*)opaque;
//    MKNOD3args *args = (MKNOD3args*)rop->request;
    MKNOD3res *reply = (MKNOD3res*)rop->reply;
    // Not supported yet
    *reply = (MKNOD3res){ .status = NFS3ERR_NOTSUPP };
    rpc_queue_reply(rop);
    return 0;
}

static int block_nfs3_remove_proc(void *opaque, rpc_op_t *rop)
{
    nfs_client_t *self = (nfs_client_t*)opaque;
    REMOVE3res *reply = (REMOVE3res*)rop->reply;
    REMOVE3args *args = (REMOVE3args*)rop->request;
    std::string full_name = get_inode_name(self, args->object);
    if (full_name == "")
    {
        *reply = (REMOVE3res){ .status = NFS3ERR_NOENT };
        rpc_queue_reply(rop);
        return 0;
    }
    // Run rm command
    self->parent->cmd->loop_and_wait(self->parent->cmd->start_rm(json11::Json::object {
        { "from", full_name },
    }), [rop](const cli_result_t & r)
    {
        REMOVE3res *reply = (REMOVE3res*)rop->reply;
        *reply = (REMOVE3res){ .status = vitastor_nfs_map_err(r.err) };
        if (!r.err)
        {
            reply->resok = (REMOVE3resok){
                //.dir_wcc = ...
            };
        }
        rpc_queue_reply(rop);
    });
    return 1;
}

static int block_nfs3_rmdir_proc(void *opaque, rpc_op_t *rop)
{
    nfs_client_t *self = (nfs_client_t*)opaque;
    RMDIR3args *args = (RMDIR3args*)rop->request;
    RMDIR3res *reply = (RMDIR3res*)rop->reply;
    std::string full_name = get_inode_name(self, args->object);
    if (full_name == "")
    {
        *reply = (RMDIR3res){ .status = NFS3ERR_INVAL };
        rpc_queue_reply(rop);
        return 0;
    }
    auto dir_it = self->parent->blockfs->dir_info.find(full_name);
    if (dir_it == self->parent->blockfs->dir_info.end())
    {
        *reply = (RMDIR3res){ .status = NFS3ERR_NOENT };
        rpc_queue_reply(rop);
        return 0;
    }
    std::string prefix = full_name+"/";
    for (auto & ic: self->parent->cli->st_cli.inode_config)
    {
        if (prefix != "" && ic.second.name.substr(0, prefix.size()) == prefix)
        {
            *reply = (RMDIR3res){ .status = NFS3ERR_NOTEMPTY };
            rpc_queue_reply(rop);
            return 0;
        }
    }
    self->parent->blockfs->dir_by_hash.erase("S"+base64_encode(sha256(full_name)));
    self->parent->blockfs->dir_info.erase(dir_it);
    *reply = (RMDIR3res){ .status = NFS3_OK };
    rpc_queue_reply(rop);
    return 0;
}

struct nfs_dir_rename_state
{
    nfs_client_t *self;
    rpc_op_t *rop;
    std::string old_name, new_name;
    std::vector<std::string> items;
};

static int continue_dir_rename(nfs_dir_rename_state *rename_st)
{
    nfs_client_t *self = rename_st->self;
    if (!rename_st->items.size())
    {
        std::string old_prefix = rename_st->old_name+"/";
        for (auto & ic: self->parent->cli->st_cli.inode_config)
        {
            if (ic.second.name.substr(0, old_prefix.size()) == old_prefix)
                rename_st->items.push_back(ic.second.name);
        }
    }
    if (!rename_st->items.size())
    {
        // old dir
        auto old_info = self->parent->blockfs->dir_info.at(rename_st->old_name);
        self->parent->blockfs->dir_info.erase(rename_st->old_name);
        self->parent->blockfs->dir_by_hash.erase("S"+base64_encode(sha256(rename_st->old_name)));
        // new dir
        self->parent->blockfs->dir_info[rename_st->new_name] = old_info;
        self->parent->blockfs->dir_by_hash["S"+base64_encode(sha256(rename_st->new_name))] = rename_st->new_name;
        RENAME3res *reply = (RENAME3res*)rename_st->rop->reply;
        *reply = (RENAME3res){
            .status = NFS3_OK,
            .resok = {
                //.fromdir_wcc = ...
                //.todir_wcc = ...
            },
        };
        rpc_queue_reply(rename_st->rop);
        delete rename_st;
        return 0;
    }
    std::string item = rename_st->items.back();
    rename_st->items.pop_back();
    self->parent->cmd->loop_and_wait(self->parent->cmd->start_modify(json11::Json::object {
        { "image", item },
        { "rename", rename_st->new_name + item.substr(rename_st->old_name.size()) },
    }), [rename_st](const cli_result_t & r)
    {
        if (r.err)
        {
            RENAME3res *reply = (RENAME3res*)rename_st->rop->reply;
            *reply = (RENAME3res){ .status = vitastor_nfs_map_err(r.err) };
            rpc_queue_reply(rename_st->rop);
            delete rename_st;
        }
        else
        {
            continue_dir_rename(rename_st);
        }
    });
    return 1;
}

static void nfs_do_rename(nfs_client_t *self, rpc_op_t *rop, std::string old_name, std::string new_name);

static int block_nfs3_rename_proc(void *opaque, rpc_op_t *rop)
{
    nfs_client_t *self = (nfs_client_t*)opaque;
    RENAME3args *args = (RENAME3args*)rop->request;
    std::string old_name = get_inode_name(self, args->from);
    std::string new_name = get_inode_name(self, args->to);
    if (old_name == "" || new_name == "")
    {
        RENAME3res *reply = (RENAME3res*)rop->reply;
        *reply = (RENAME3res){ .status = NFS3ERR_NOENT };
        rpc_queue_reply(rop);
        return 0;
    }
    bool old_is_dir = self->parent->blockfs->dir_info.find(old_name) != self->parent->blockfs->dir_info.end();
    bool new_is_dir = self->parent->blockfs->dir_info.find(new_name) != self->parent->blockfs->dir_info.end();
    bool old_is_file = false, new_is_file = false;
    for (auto & ic: self->parent->cli->st_cli.inode_config)
    {
        if (ic.second.name == new_name)
            new_is_file = true;
        if (ic.second.name == old_name)
            old_is_file = true;
        if (new_is_file && old_is_file)
            break;
    }
    if (old_is_dir)
    {
        // Check that destination is not a file
        if (new_is_file)
        {
            RENAME3res *reply = (RENAME3res*)rop->reply;
            *reply = (RENAME3res){ .status = NFS3ERR_NOTDIR };
            rpc_queue_reply(rop);
            return 0;
        }
        // Rename all images with this prefix
        nfs_dir_rename_state *rename_st = new nfs_dir_rename_state();
        rename_st->self = self;
        rename_st->rop = rop;
        rename_st->old_name = old_name;
        rename_st->new_name = new_name;
        return continue_dir_rename(rename_st);
    }
    if (!old_is_file)
    {
        RENAME3res *reply = (RENAME3res*)rop->reply;
        *reply = (RENAME3res){ .status = NFS3ERR_NOENT };
        rpc_queue_reply(rop);
        return 0;
    }
    if (new_is_dir)
    {
        RENAME3res *reply = (RENAME3res*)rop->reply;
        *reply = (RENAME3res){ .status = NFS3ERR_ISDIR };
        rpc_queue_reply(rop);
        return 0;
    }
    if (new_is_file)
    {
        // Rename over an existing file - remove old file
        self->parent->cmd->loop_and_wait(self->parent->cmd->start_rm(json11::Json::object {
            { "from", new_name },
        }), [self, rop, old_name, new_name](const cli_result_t & r)
        {
            nfs_do_rename(self, rop, old_name, new_name);
        });
    }
    else
    {
        nfs_do_rename(self, rop, old_name, new_name);
    }
    return 1;
}

static void nfs_do_rename(nfs_client_t *self, rpc_op_t *rop, std::string old_name, std::string new_name)
{
    // Run modify/rename command
    self->parent->cmd->loop_and_wait(self->parent->cmd->start_modify(json11::Json::object {
        { "image", old_name },
        { "rename", new_name },
    }), [rop](const cli_result_t & r)
    {
        RENAME3res *reply = (RENAME3res*)rop->reply;
        *reply = (RENAME3res){ .status = vitastor_nfs_map_err(r.err) };
        if (!r.err)
        {
            reply->resok = (RENAME3resok){
                //.fromdir_wcc = ...
                //.todir_wcc = ...
            };
        }
        rpc_queue_reply(rop);
    });
}

static int block_nfs3_link_proc(void *opaque, rpc_op_t *rop)
{
    //nfs_client_t *self = (nfs_client_t*)opaque;
    //LINK3args *args = (LINK3args*)rop->request;
    LINK3res *reply = (LINK3res*)rop->reply;
    // We don't support hard links
    *reply = (LINK3res){ NFS3ERR_NOTSUPP };
    rpc_queue_reply(rop);
    return 0;
}

static void fill_dir_entry(nfs_client_t *self, rpc_op_t *rop,
    std::map<std::string, nfs_dir_t>::iterator dir_id_it, struct entryplus3 *entry, bool is_plus)
{
    if (dir_id_it == self->parent->blockfs->dir_info.end())
    {
        return;
    }
    entry->fileid = dir_id_it->second.id;
    if (is_plus)
    {
        entry->name_attributes = (post_op_attr){
            .attributes_follow = 1,
            .attributes = get_dir_attributes(self, dir_id_it->first),
        };
        entry->name_handle = (post_op_fh3){
            .handle_follows = 1,
            .handle = xdr_copy_string(rop->xdrs, "S"+base64_encode(sha256(dir_id_it->first))),
        };
    }
}

static void block_nfs3_readdir_common(void *opaque, rpc_op_t *rop, bool is_plus)
{
    nfs_client_t *self = (nfs_client_t*)opaque;
    READDIRPLUS3args plus_args;
    READDIRPLUS3args *args = NULL;
    if (is_plus)
        args = ((READDIRPLUS3args*)rop->request);
    else
    {
        args = &plus_args;
        READDIR3args *in_args = ((READDIR3args*)rop->request);
        args->dir = in_args->dir;
        args->cookie = in_args->cookie;
        *((uint64_t*)args->cookieverf) = *((uint64_t*)in_args->cookieverf);
        args->dircount = 512;
        args->maxcount = in_args->count;
    }
    std::string dirhash = args->dir;
    std::string dir;
    if (dirhash != NFS_ROOT_HANDLE)
    {
        auto dir_it = self->parent->blockfs->dir_by_hash.find(dirhash);
        if (dir_it != self->parent->blockfs->dir_by_hash.end())
            dir = dir_it->second;
    }
    std::string prefix = dir.size() ? dir+"/" : self->parent->blockfs->name_prefix;
    std::map<std::string, struct entryplus3> entries;
    for (auto & ic: self->parent->cli->st_cli.inode_config)
    {
        auto & inode_cfg = ic.second;
        if (prefix != "" && inode_cfg.name.substr(0, prefix.size()) != prefix)
            continue;
        std::string subname = inode_cfg.name.substr(prefix.size());
        int p = 0;
        while (p < subname.size() && subname[p] == '/')
            p++;
        if (p > 0)
            subname = subname.substr(p);
        if (subname.size() == 0)
            continue;
        p = 0;
        while (p < subname.size() && subname[p] != '/')
            p++;
        if (p >= subname.size())
        {
            // fileid will change when the user creates snapshots
            // however, we hope that clients tolerate it well
            // Linux does, even though it complains about "fileid changed" in dmesg
            entries[subname].fileid = ic.first;
            if (is_plus)
            {
                entries[subname].name_attributes = (post_op_attr){
                    .attributes_follow = 1,
                    .attributes = get_file_attributes(self, ic.first),
                };
                entries[subname].name_handle = (post_op_fh3){
                    .handle_follows = 1,
                    .handle = xdr_copy_string(rop->xdrs, "S"+base64_encode(sha256(inode_cfg.name))),
                };
            }
        }
        else
        {
            // skip directories, they will be added from blockfs->dir_info
        }
    }
    // Add directories from blockfs->dir_info
    for (auto dir_id_it = self->parent->blockfs->dir_info.lower_bound(prefix);
        dir_id_it != self->parent->blockfs->dir_info.end(); dir_id_it++)
    {
        if (prefix != "" && dir_id_it->first.substr(0, prefix.size()) != prefix)
            break;
        if (dir_id_it->first.size() == prefix.size() ||
            dir_id_it->first.find("/", prefix.size()) != std::string::npos)
            continue;
        std::string subname = dir_id_it->first.substr(prefix.size());
        // for directories, fileid changes when the user restarts proxy
        fill_dir_entry(self, rop, dir_id_it, &entries[subname], is_plus);
    }
    // Add . and ..
    {
        auto dir_id_it = self->parent->blockfs->dir_info.find(dir);
        fill_dir_entry(self, rop, dir_id_it, &entries["."], is_plus);
        auto sl = dir.rfind("/");
        if (sl != std::string::npos)
        {
            auto dir_id_it = self->parent->blockfs->dir_info.find(dir.substr(0, sl));
            fill_dir_entry(self, rop, dir_id_it, &entries[".."], is_plus);
        }
    }
    // Offset results by the continuation cookie (equal to index in the listing)
    uint64_t idx = 1;
    void *prev = NULL;
    for (auto it = entries.begin(); it != entries.end();)
    {
        entryplus3 *entry = &it->second;
        // First fields of entry3 and entryplus3 are the same: fileid, name, cookie
        entry->name = xdr_copy_string(rop->xdrs, it->first);
        entry->cookie = idx++;
        if (prev)
        {
            if (is_plus)
                ((entryplus3*)prev)->nextentry = entry;
            else
                ((entry3*)prev)->nextentry = (entry3*)entry;
        }
        prev = entry;
        if (args->cookie > 0 && entry->cookie == args->cookie)
            entries.erase(entries.begin(), ++it);
        else
            it++;
    }
    // Now limit results based on maximum reply size
    // Sadly we have to calculate reply size by hand
    // reply without entries is 4+4+(dir_attributes ? sizeof(fattr3) : 0)+8+4 bytes
    int reply_size = 20;
    if (reply_size > args->maxcount)
    {
        // Error, too small max reply size
        if (is_plus)
        {
            READDIRPLUS3res *reply = (READDIRPLUS3res*)rop->reply;
            *reply = (READDIRPLUS3res){ .status = NFS3ERR_TOOSMALL };
            rpc_queue_reply(rop);
        }
        else
        {
            READDIR3res *reply = (READDIR3res*)rop->reply;
            *reply = (READDIR3res){ .status = NFS3ERR_TOOSMALL };
            rpc_queue_reply(rop);
        }
        return;
    }
    // 1 entry3 is (8+4+(filename_len+3)/4*4+8) bytes
    // 1 entryplus3 is (8+4+(filename_len+3)/4*4+8
    //   + 4+(name_attributes ? (sizeof(fattr3) = 84) : 0)
    //   + 4+(name_handle ? 4+(handle_len+3)/4*4 : 0)) bytes
    bool eof = true;
    for (auto it = entries.begin(); it != entries.end(); it++)
    {
        reply_size += 20+len_pad4(it->first.size())+(is_plus
            ? 8+88+len_pad4(it->second.name_handle.handle.size) : 0);
        if (reply_size > args->maxcount)
        {
            // Stop
            entries.erase(it, entries.end());
            eof = false;
            break;
        }
    }
    if (entries.end() != entries.begin())
    {
        auto last_it = entries.end();
        last_it--;
        if (is_plus)
            ((entryplus3*)&last_it->second)->nextentry = NULL;
        else
        {
            entry3* e = ((entry3*)&last_it->second);
            e->nextentry = NULL;
        }
    }
    // Send reply
    if (is_plus)
    {
        READDIRPLUS3res *reply = (READDIRPLUS3res*)rop->reply;
        *reply = { .status = NFS3_OK };
        *(uint64_t*)(reply->resok.cookieverf) = self->parent->blockfs->dir_info.at(dir).mod_rev;
        reply->resok.reply.entries = entries.size() ? &entries.begin()->second : NULL;
        reply->resok.reply.eof = eof;
    }
    else
    {
        READDIR3res *reply = (READDIR3res*)rop->reply;
        *reply = { .status = NFS3_OK };
        *(uint64_t*)(reply->resok.cookieverf) = self->parent->blockfs->dir_info.at(dir).mod_rev;
        reply->resok.reply.entries = entries.size() ? (entry3*)&entries.begin()->second : NULL;
        reply->resok.reply.eof = eof;
    }
    rpc_queue_reply(rop);
}

static int block_nfs3_readdir_proc(void *opaque, rpc_op_t *rop)
{
    block_nfs3_readdir_common(opaque, rop, false);
    return 0;
}

static int block_nfs3_readdirplus_proc(void *opaque, rpc_op_t *rop)
{
    block_nfs3_readdir_common(opaque, rop, true);
    return 0;
}

void block_fs_state_t::init(nfs_proxy_t *proxy, json11::Json cfg)
{
    name_prefix = cfg["subdir"].string_value();
    {
        int e = name_prefix.size();
        while (e > 0 && name_prefix[e-1] == '/')
            e--;
        int s = 0;
        while (s < e && name_prefix[s] == '/')
            s++;
        name_prefix = name_prefix.substr(s, e-s);
        if (name_prefix.size())
            name_prefix += "/";
    }
    // We need inode name hashes for NFS handles to remain stateless and <= 64 bytes long
    dir_info[""] = (nfs_dir_t){
        .id = 1,
        .mod_rev = 0,
    };
    clock_gettime(CLOCK_REALTIME, &dir_info[""].mtime);
    assert(proxy->cli->st_cli.on_inode_change_hook == NULL);
    proxy->cli->st_cli.on_inode_change_hook = [this, proxy](inode_t changed_inode, bool removed)
    {
        auto inode_cfg_it = proxy->cli->st_cli.inode_config.find(changed_inode);
        if (inode_cfg_it == proxy->cli->st_cli.inode_config.end())
        {
            return;
        }
        auto & inode_cfg = inode_cfg_it->second;
        std::string full_name = inode_cfg.name;
        if (proxy->blockfs->name_prefix != "" && full_name.substr(0, proxy->blockfs->name_prefix.size()) != proxy->blockfs->name_prefix)
        {
            return;
        }
        // Calculate directory modification time and revision (used as "cookie verifier")
        timespec now;
        clock_gettime(CLOCK_REALTIME, &now);
        dir_info[""].mod_rev = dir_info[""].mod_rev < inode_cfg.mod_revision ? inode_cfg.mod_revision : dir_info[""].mod_rev;
        dir_info[""].mtime = now;
        int pos = full_name.find('/', proxy->blockfs->name_prefix.size());
        while (pos >= 0)
        {
            std::string dir = full_name.substr(0, pos);
            auto & dinf = dir_info[dir];
            if (!dinf.id)
                dinf.id = next_dir_id++;
            dinf.mod_rev = dinf.mod_rev < inode_cfg.mod_revision ? inode_cfg.mod_revision : dinf.mod_rev;
            dinf.mtime = now;
            dir_by_hash["S"+base64_encode(sha256(dir))] = dir;
            pos = full_name.find('/', pos+1);
        }
        // Alter inode_by_hash
        if (removed)
        {
            auto ino_it = hash_by_inode.find(changed_inode);
            if (ino_it != hash_by_inode.end())
            {
                inode_by_hash.erase(ino_it->second);
                hash_by_inode.erase(ino_it);
            }
        }
        else
        {
            std::string hash = "S"+base64_encode(sha256(full_name));
            auto hbi_it = hash_by_inode.find(changed_inode);
            if (hbi_it != hash_by_inode.end() && hbi_it->second != hash)
            {
                // inode had a different name, remove old hash=>inode pointer
                inode_by_hash.erase(hbi_it->second);
            }
            inode_by_hash[hash] = changed_inode;
            hash_by_inode[changed_inode] = hash;
        }
    };
}

void nfs_block_procs(nfs_client_t *self)
{
    struct rpc_service_proc_t pt[] = {
        {NFS_PROGRAM, NFS_V3, NFS3_NULL,          nfs3_null_proc,              NULL,                            0,                        NULL,                           0,                       self},
        {NFS_PROGRAM, NFS_V3, NFS3_GETATTR,       block_nfs3_getattr_proc,     (xdrproc_t)xdr_GETATTR3args,     sizeof(GETATTR3args),     (xdrproc_t)xdr_GETATTR3res,     sizeof(GETATTR3res),     self},
        {NFS_PROGRAM, NFS_V3, NFS3_SETATTR,       block_nfs3_setattr_proc,     (xdrproc_t)xdr_SETATTR3args,     sizeof(SETATTR3args),     (xdrproc_t)xdr_SETATTR3res,     sizeof(SETATTR3res),     self},
        {NFS_PROGRAM, NFS_V3, NFS3_LOOKUP,        block_nfs3_lookup_proc,      (xdrproc_t)xdr_LOOKUP3args,      sizeof(LOOKUP3args),      (xdrproc_t)xdr_LOOKUP3res,      sizeof(LOOKUP3res),      self},
        {NFS_PROGRAM, NFS_V3, NFS3_ACCESS,        block_nfs3_access_proc,      (xdrproc_t)xdr_ACCESS3args,      sizeof(ACCESS3args),      (xdrproc_t)xdr_ACCESS3res,      sizeof(ACCESS3res),      self},
        {NFS_PROGRAM, NFS_V3, NFS3_READLINK,      block_nfs3_readlink_proc,    (xdrproc_t)xdr_READLINK3args,    sizeof(READLINK3args),    (xdrproc_t)xdr_READLINK3res,    sizeof(READLINK3res),    self},
        {NFS_PROGRAM, NFS_V3, NFS3_READ,          block_nfs3_read_proc,        (xdrproc_t)xdr_READ3args,        sizeof(READ3args),        (xdrproc_t)xdr_READ3res,        sizeof(READ3res),        self},
        {NFS_PROGRAM, NFS_V3, NFS3_WRITE,         block_nfs3_write_proc,       (xdrproc_t)xdr_WRITE3args,       sizeof(WRITE3args),       (xdrproc_t)xdr_WRITE3res,       sizeof(WRITE3res),       self},
        {NFS_PROGRAM, NFS_V3, NFS3_CREATE,        block_nfs3_create_proc,      (xdrproc_t)xdr_CREATE3args,      sizeof(CREATE3args),      (xdrproc_t)xdr_CREATE3res,      sizeof(CREATE3res),      self},
        {NFS_PROGRAM, NFS_V3, NFS3_MKDIR,         block_nfs3_mkdir_proc,       (xdrproc_t)xdr_MKDIR3args,       sizeof(MKDIR3args),       (xdrproc_t)xdr_MKDIR3res,       sizeof(MKDIR3res),       self},
        {NFS_PROGRAM, NFS_V3, NFS3_SYMLINK,       block_nfs3_symlink_proc,     (xdrproc_t)xdr_SYMLINK3args,     sizeof(SYMLINK3args),     (xdrproc_t)xdr_SYMLINK3res,     sizeof(SYMLINK3res),     self},
        {NFS_PROGRAM, NFS_V3, NFS3_MKNOD,         block_nfs3_mknod_proc,       (xdrproc_t)xdr_MKNOD3args,       sizeof(MKNOD3args),       (xdrproc_t)xdr_MKNOD3res,       sizeof(MKNOD3res),       self},
        {NFS_PROGRAM, NFS_V3, NFS3_REMOVE,        block_nfs3_remove_proc,      (xdrproc_t)xdr_REMOVE3args,      sizeof(REMOVE3args),      (xdrproc_t)xdr_REMOVE3res,      sizeof(REMOVE3res),      self},
        {NFS_PROGRAM, NFS_V3, NFS3_RMDIR,         block_nfs3_rmdir_proc,       (xdrproc_t)xdr_RMDIR3args,       sizeof(RMDIR3args),       (xdrproc_t)xdr_RMDIR3res,       sizeof(RMDIR3res),       self},
        {NFS_PROGRAM, NFS_V3, NFS3_RENAME,        block_nfs3_rename_proc,      (xdrproc_t)xdr_RENAME3args,      sizeof(RENAME3args),      (xdrproc_t)xdr_RENAME3res,      sizeof(RENAME3res),      self},
        {NFS_PROGRAM, NFS_V3, NFS3_LINK,          block_nfs3_link_proc,        (xdrproc_t)xdr_LINK3args,        sizeof(LINK3args),        (xdrproc_t)xdr_LINK3res,        sizeof(LINK3res),        self},
        {NFS_PROGRAM, NFS_V3, NFS3_READDIR,       block_nfs3_readdir_proc,     (xdrproc_t)xdr_READDIR3args,     sizeof(READDIR3args),     (xdrproc_t)xdr_READDIR3res,     sizeof(READDIR3res),     self},
        {NFS_PROGRAM, NFS_V3, NFS3_READDIRPLUS,   block_nfs3_readdirplus_proc, (xdrproc_t)xdr_READDIRPLUS3args, sizeof(READDIRPLUS3args), (xdrproc_t)xdr_READDIRPLUS3res, sizeof(READDIRPLUS3res), self},
        {NFS_PROGRAM, NFS_V3, NFS3_FSSTAT,        nfs3_fsstat_proc,            (xdrproc_t)xdr_FSSTAT3args,      sizeof(FSSTAT3args),      (xdrproc_t)xdr_FSSTAT3res,      sizeof(FSSTAT3res),      self},
        {NFS_PROGRAM, NFS_V3, NFS3_FSINFO,        nfs3_fsinfo_proc,            (xdrproc_t)xdr_FSINFO3args,      sizeof(FSINFO3args),      (xdrproc_t)xdr_FSINFO3res,      sizeof(FSINFO3res),      self},
        {NFS_PROGRAM, NFS_V3, NFS3_PATHCONF,      nfs3_pathconf_proc,          (xdrproc_t)xdr_PATHCONF3args,    sizeof(PATHCONF3args),    (xdrproc_t)xdr_PATHCONF3res,    sizeof(PATHCONF3res),    self},
        {NFS_PROGRAM, NFS_V3, NFS3_COMMIT,        nfs3_commit_proc,            (xdrproc_t)xdr_COMMIT3args,      sizeof(COMMIT3args),      (xdrproc_t)xdr_COMMIT3res,      sizeof(COMMIT3res),      self},
        {MOUNT_PROGRAM, MOUNT_V3, MOUNT3_NULL,    nfs3_null_proc,              NULL,                            0,                        NULL,                         0,                         self},
        {MOUNT_PROGRAM, MOUNT_V3, MOUNT3_MNT,     mount3_mnt_proc,             (xdrproc_t)xdr_nfs_dirpath,      sizeof(nfs_dirpath),      (xdrproc_t)xdr_nfs_mountres3, sizeof(nfs_mountres3),     self},
        {MOUNT_PROGRAM, MOUNT_V3, MOUNT3_DUMP,    mount3_dump_proc,            NULL,                            0,                        (xdrproc_t)xdr_nfs_mountlist, sizeof(nfs_mountlist),     self},
        {MOUNT_PROGRAM, MOUNT_V3, MOUNT3_UMNT,    mount3_umnt_proc,            (xdrproc_t)xdr_nfs_dirpath,      sizeof(nfs_dirpath),      NULL,                         0,                         self},
        {MOUNT_PROGRAM, MOUNT_V3, MOUNT3_UMNTALL, mount3_umntall_proc,         NULL,                            0,                        NULL,                         0,                         self},
        {MOUNT_PROGRAM, MOUNT_V3, MOUNT3_EXPORT,  mount3_export_proc,          NULL,                            0,                        (xdrproc_t)xdr_nfs_exports,   sizeof(nfs_exports),       self},
    };
    for (int i = 0; i < sizeof(pt)/sizeof(pt[0]); i++)
    {
        self->proc_table.insert(pt[i]);
    }
}
