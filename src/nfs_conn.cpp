// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// NFS connection handler for NFS proxy over VitastorKV database

#include <sys/time.h>

#include "str_util.h"

#include "nfs_proxy.h"

#include "nfs/nfs.h"

#include "cli.h"

#define TRUE 1
#define FALSE 0

#define MAX_REQUEST_SIZE 128*1024*1024

static unsigned len_pad4(unsigned len)
{
    return len + (len&3 ? 4-(len&3) : 0);
}

static nfsstat3 vitastor_nfs_map_err(int err)
{
    return (err == EINVAL ? NFS3ERR_INVAL
        : (err == ENOENT ? NFS3ERR_NOENT
        : (err == ENOSPC ? NFS3ERR_NOSPC
        : (err == EEXIST ? NFS3ERR_EXIST
        : (err == EIO ? NFS3ERR_IO : (err ? NFS3ERR_IO : NFS3_OK))))));
}

static int nfs3_null_proc(void *opaque, rpc_op_t *rop)
{
    rpc_queue_reply(rop);
    return 0;
}

static nfstime3 nfstime_from_str(const std::string & s)
{
    nfstime3 t;
    auto p = s.find(".");
    if (p != std::string::npos)
    {
        t.seconds = stoull_full(s.substr(0, p), 10);
        t.nseconds = stoull_full(s.substr(p+1), 10);
        p = s.size()-p-1;
        for (; p < 9; p++)
            t.nseconds *= 10;
        for (; p > 9; p--)
            t.nseconds /= 10;
    }
    else
        t.seconds = stoull_full(s, 10);
    return t;
}

static std::string nfstime_to_str(nfstime3 t)
{
    char buf[32];
    snprintf(buf, sizeof(buf), "%u.%09u", t.seconds, t.nseconds);
    return buf;
}

static int kv_map_type(const std::string & type)
{
    return (type == "" || type == "file" ? NF3REG :
        (type == "dir" ? NF3DIR :
        (type == "blk" ? NF3BLK :
        (type == "chr" ? NF3CHR :
        (type == "link" ? NF3LNK :
        (type == "sock" ? NF3SOCK :
        (type == "fifo" ? NF3FIFO : -1)))))));
}

static fattr3 get_kv_attributes(nfs_client_t *self, uint64_t ino, json11::Json attrs)
{
    auto type = kv_map_type(attrs["type"].string_value());
    auto mode = attrs["mode"].uint64_value();
    auto nlink = attrs["nlink"].uint64_value();
    nfstime3 mtime = nfstime_from_str(attrs["mtime"].string_value());
    nfstime3 atime = attrs["atime"].is_null() ? mtime : nfstime_from_str(attrs["atime"].string_value());
    // FIXME In theory we could store the binary structure itself instead of JSON
    return (fattr3){
        .type = (type == 0 ? NF3REG : (ftype3)type),
        .mode = (attrs["mode"].is_null() ? (type == NF3DIR ? 0755 : 0644) : (uint32_t)mode),
        .nlink = (nlink == 0 ? 1 : (uint32_t)nlink),
        .uid = (uint32_t)attrs["uid"].uint64_value(),
        .gid = (uint32_t)attrs["gid"].uint64_value(),
        .size = (type == NF3DIR ? 4096 : attrs["size"].uint64_value()),
        .used = (type == NF3DIR ? 4096 : attrs["alloc"].uint64_value()),
        .rdev = (type == NF3BLK || type == NF3CHR
            ? (specdata3){ (uint32_t)attrs["major"].uint64_value(), (uint32_t)attrs["minor"].uint64_value() }
            : (specdata3){}),
        .fsid = self->parent->fsid,
        .fileid = ino,
        .atime = atime,
        .mtime = mtime,
        .ctime = mtime,
    };
}

static std::string kv_direntry_key(uint64_t dir_ino, const std::string & filename)
{
    // encode as: d <length> <hex dir_ino> / <filename>
    char key[24] = { 0 };
    snprintf(key, sizeof(key), "d-%jx/", dir_ino);
    int n = strnlen(key, sizeof(key)-1) - 3;
    if (n < 10)
        key[1] = '0'+n;
    else
        key[1] = 'A'+(n-10);
    return (char*)key + filename;
}

static uint64_t kv_root_inode = 1;

static const char *kv_next_id_key = "id";

static const char *kv_root_handle = "R";

static std::string kv_direntry_filename(const std::string & key)
{
    // decode as: d <length> <hex dir_ino> / <filename>
    auto pos = key.find("/");
    if (pos != std::string::npos)
        return key.substr(pos+1);
    return key;
}

static std::string kv_inode_key(uint64_t ino)
{
    char key[24] = { 0 };
    snprintf(key, sizeof(key), "i-%jx", ino);
    int n = strnlen(key, sizeof(key)-1) - 2;
    if (n < 10)
        key[1] = '0'+n;
    else
        key[1] = 'A'+(n-10);
    return std::string(key, n+2);
}

static std::string kv_fh(uint64_t ino)
{
    return "S"+std::string((char*)&ino, 8);
}

static uint64_t kv_fh_inode(const std::string & fh)
{
    if (fh.size() == 1 && fh[0] == 'R')
    {
        return 1;
    }
    else if (fh.size() == 9 && fh[0] == 'S')
    {
        return *(uint64_t*)&fh[1];
    }
    else if (fh.size() > 17 && fh[0] == 'I')
    {
        return *(uint64_t*)&fh[fh.size()-8];
    }
    return 0;
}

static bool kv_fh_valid(const std::string & fh)
{
    return fh == kv_root_handle || fh.size() == 9 && fh[0] == 'S' || fh.size() > 17 && fh[0] == 'I';
}

// Attributes are always stored in the inode
static void kv_read_inode(nfs_client_t *self, uint64_t ino,
    std::function<void(int res, const std::string & value, json11::Json ientry)> cb)
{
    auto key = kv_inode_key(ino);
    self->parent->db->get(key, [=](int res, const std::string & value)
    {
        if (ino == kv_root_inode && res == -ENOENT)
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
    });
}

static int nfs3_getattr_proc(void *opaque, rpc_op_t *rop)
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

struct nfs_kv_setattr_state
{
    nfs_client_t *self = NULL;
    rpc_op_t *rop = NULL;
    uint64_t ino = 0;
    uint64_t old_size = 0, new_size = 0;
    json11::Json::object set_attrs;
    int res = 0, cas_res = 0;
    std::string ientry_text;
    json11::Json ientry;
    json11::Json::object new_attrs;
    std::function<void(int)> cb;
};

static void nfs_kv_continue_setattr(nfs_kv_setattr_state *st, int state)
{
    if (state == 0)      {}
    else if (state == 1) goto resume_1;
    else if (state == 2) goto resume_2;
    else if (state == 3) goto resume_3;
    else
    {
        fprintf(stderr, "BUG: invalid state in nfs_kv_continue_setattr()");
        abort();
    }
resume_0:
    kv_read_inode(st->self, st->ino, [st](int res, const std::string & value, json11::Json attrs)
    {
        st->res = res;
        st->ientry_text = value;
        st->ientry = attrs;
        nfs_kv_continue_setattr(st, 1);
    });
    return;
resume_1:
    if (st->res < 0)
    {
        auto cb = std::move(st->cb);
        cb(st->res);
        return;
    }
    if (st->ientry["type"].string_value() == "link" ||
        st->ientry["type"].string_value() != "file" &&
        st->ientry["type"].string_value() != "" &&
        !st->set_attrs["size"].is_null())
    {
        auto cb = std::move(st->cb);
        cb(-EINVAL);
        return;
    }
    // Now we can update it
    st->new_attrs = st->ientry.object_items();
    st->old_size = st->ientry["size"].uint64_value();
    for (auto & kv: st->set_attrs)
    {
        if (kv.first == "size")
        {
            st->new_size = kv.second.uint64_value();
        }
        st->new_attrs[kv.first] = kv.second;
    }
    st->self->parent->db->set(kv_inode_key(st->ino), json11::Json(st->new_attrs).dump(), [st](int res)
    {
        st->res = res;
        nfs_kv_continue_setattr(st, 2);
    }, [st](int res, const std::string & cas_value)
    {
        st->cas_res = res;
        return (res == 0 || res == -ENOENT && st->ino == kv_root_inode) && cas_value == st->ientry_text;
    });
    return;
resume_2:
    if (st->cas_res == -ENOENT)
    {
        st->res = -ENOENT;
    }
    if (st->res == -EAGAIN)
    {
        // Retry
        fprintf(stderr, "CAS failure during setattr, retrying\n");
        goto resume_0;
    }
    if (st->res < 0)
    {
        fprintf(stderr, "Failed to update inode %ju: %s (code %d)\n", st->ino, strerror(-st->res), st->res);
        auto cb = std::move(st->cb);
        cb(st->res);
        return;
    }
    if (!st->set_attrs["size"].is_null() &&
        st->ientry["size"].uint64_value() > st->set_attrs["size"].uint64_value())
    {
        // Delete extra data when downsizing
        st->self->parent->cmd->loop_and_wait(st->self->parent->cmd->start_rm_data(json11::Json::object {
            { "inode", INODE_NO_POOL(st->self->parent->fs_base_inode + st->ino) },
            { "pool", (uint64_t)INODE_POOL(st->self->parent->fs_base_inode + st->ino) },
            { "min_offset", st->set_attrs["size"].uint64_value() },
        }), [st](const cli_result_t & r)
        {
            if (r.err)
            {
                fprintf(stderr, "Failed to truncate inode %ju: %s (code %d)\n",
                    st->ino, r.text.c_str(), r.err);
            }
            st->res = r.err;
            nfs_kv_continue_setattr(st, 3);
        });
        return;
    }
resume_3:
    auto cb = std::move(st->cb);
    cb(0);
}

static int nfs3_setattr_proc(void *opaque, rpc_op_t *rop)
{
    nfs_kv_setattr_state *st = new nfs_kv_setattr_state;
    st->self = (nfs_client_t*)opaque;
    st->rop = rop;
    auto args = (SETATTR3args*)rop->request;
    auto reply = (SETATTR3res*)rop->reply;
    std::string fh = args->object;
    if (!kv_fh_valid(fh))
    {
        *reply = (SETATTR3res){ .status = NFS3ERR_INVAL };
        rpc_queue_reply(rop);
        delete st;
        return 0;
    }
    st->ino = kv_fh_inode(fh);
    if (args->new_attributes.size.set_it)
        st->set_attrs["size"] = args->new_attributes.size.size;
    if (args->new_attributes.mode.set_it)
        st->set_attrs["mode"] = (uint64_t)args->new_attributes.mode.mode;
    if (args->new_attributes.uid.set_it)
        st->set_attrs["uid"] = (uint64_t)args->new_attributes.uid.uid;
    if (args->new_attributes.gid.set_it)
        st->set_attrs["gid"] = (uint64_t)args->new_attributes.gid.gid;
    if (args->new_attributes.atime.set_it)
        st->set_attrs["atime"] = nfstime_to_str(args->new_attributes.atime.atime);
    if (args->new_attributes.mtime.set_it)
        st->set_attrs["mtime"] = nfstime_to_str(args->new_attributes.mtime.mtime);
    fprintf(stderr, "SETATTR %ju ATTRS %s\n", st->ino, json11::Json(st->set_attrs).dump().c_str());
    st->cb = [st](int res)
    {
        auto reply = (SETATTR3res*)st->rop->reply;
        if (res < 0)
        {
            *reply = (SETATTR3res){
                .status = vitastor_nfs_map_err(res),
            };
        }
        else
        {
            *reply = (SETATTR3res){
                .status = NFS3_OK,
                .resok = (SETATTR3resok){
                    .obj_wcc = (wcc_data){
                        .after = (post_op_attr){
                            .attributes_follow = 1,
                            .attributes = get_kv_attributes(st->self, st->ino, st->new_attrs),
                        },
                    },
                },
            };
        }
        rpc_queue_reply(st->rop);
        delete st;
    };
    nfs_kv_continue_setattr(st, 0);
    return 1;
}

static int nfs3_lookup_proc(void *opaque, rpc_op_t *rop)
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
        kv_read_inode(self, ino, [=](int res, const std::string & value, json11::Json ientry)
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

static int nfs3_access_proc(void *opaque, rpc_op_t *rop)
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

static int nfs3_readlink_proc(void *opaque, rpc_op_t *rop)
{
    nfs_client_t *self = (nfs_client_t*)opaque;
    READLINK3args *args = (READLINK3args*)rop->request;
    if (self->parent->trace)
        fprintf(stderr, "[%d] READLINK %ju\n", self->nfs_fd, kv_fh_inode(args->symlink));
    READLINK3res *reply = (READLINK3res*)rop->reply;
    if (!kv_fh_valid(args->symlink) || args->symlink == kv_root_handle)
    {
        // Invalid filehandle or trying to read symlink from root entry
        *reply = (READLINK3res){ .status = NFS3ERR_INVAL };
        rpc_queue_reply(rop);
        return 0;
    }
    kv_read_inode(self, kv_fh_inode(args->symlink), [=](int res, const std::string & value, json11::Json attrs)
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

static int nfs3_read_proc(void *opaque, rpc_op_t *rop)
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

static void nfs_resize_write(nfs_client_t *self, rpc_op_t *rop, uint64_t inode, uint64_t new_size, uint64_t offset, uint64_t count, void *buf);

static int nfs3_write_proc(void *opaque, rpc_op_t *rop)
{
    // FIXME: Pack small files into "shared inodes"
    // Insane algorithm...
    // Threshold should be equal to one allocation block of the pool
    // Write:
    // - If (offset+size <= threshold):
    //   - Read inode from cache
    //   - If inode does not exist - stop with -ENOENT
    //   - If it's empty (size == 0 || empty == true):
    //     - If preset size is larger than threshold:
    //       - Write data into non-shared inode
    //       - In parallel: clear empty flag
    //         - If CAS failure: re-read inode and restart
    //     - Otherwise:
    //       - Allocate/take a shared inode
    //       - Allocate space in its end
    //       - Write data into shared inode
    //         - If CAS failure: allocate another shared inode and retry
    //       - Write shared inode reference, set size
    //         - If CAS failure: free allocated shared space, re-read inode and restart
    //   - If it's not empty:
    //     - If non-shared:
    //       - Write data into non-shared inode
    //       - In parallel: check if data fits into inode size and extend if it doesn't
    //         - If CAS failure: re-read inode and retry to extend the size
    //     - If shared:
    //       - Read whole file from shared inode
    //         - If the file header in data doesn't match: re-read inode and restart
    //       - If data doesn't fit into the same shared inode:
    //         - Allocate space in a new shared inode
    //         - Write data into the new shared inode
    //           - If CAS failure: allocate another shared inode and retry
    //         - Update inode metadata (set new size and new shared inode)
    //           - If CAS failure: free allocated shared space, re-read inode and restart
    //       - If it fits:
    //         - Write updated data into the shared inode
    //         - Update shared inode header
    //           - If CAS failure: re-read inode and restart
    // - Otherwise:
    //   - Write data into non-shared inode
    //   - Read inode in parallel
    //     - If shared:
    //       - Read whole file from shared inode
    //       - Write data into non-shared inode
    //         - If CAS failure (block should not exist): restart
    //       - Update inode metadata (make non-shared, update size)
    //         - If CAS failure: restart
    //       - Zero out the shared inode header
    //         - If CAS failure: restart
    //     - Check if size fits
    //       - Extend if it doesn't
    // Read:
    // - If (offset+size <= threshold):
    //   - Read inode from cache
    //   - If empty: return zeroes
    //   - If shared:
    //     - Read the whole file from shared inode, or at least data and shared inode header
    //     - If the file header in data doesn't match: re-read inode and restart
    //   - If non-shared:
    //     - Read data from non-shared inode
    // - Otherwise:
    //   - Read data from non-shared inode
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

static void allocate_new_id(nfs_client_t *self, std::function<void(int res, uint64_t new_id)> cb)
{
    if (self->parent->fs_next_id <= self->parent->fs_allocated_id)
    {
        cb(0, self->parent->fs_next_id++);
        return;
    }
    else if (self->parent->fs_next_id > self->parent->fs_inode_count)
    {
        cb(-ENOSPC, 0);
        return;
    }
    self->parent->db->get(kv_next_id_key, [=](int res, const std::string & prev_str)
    {
        if (res < 0 && res != -ENOENT)
        {
            cb(res, 0);
            return;
        }
        uint64_t prev_val = stoull_full(prev_str);
        if (prev_val >= self->parent->fs_inode_count)
        {
            cb(-ENOSPC, 0);
            return;
        }
        if (prev_val < 1)
        {
            prev_val = 1;
        }
        uint64_t new_val = prev_val + self->parent->id_alloc_batch_size;
        if (new_val >= self->parent->fs_inode_count)
        {
            new_val = self->parent->fs_inode_count;
        }
        self->parent->db->set(kv_next_id_key, std::to_string(new_val), [=](int res)
        {
            if (res == -EAGAIN)
            {
                // CAS failure - retry
                allocate_new_id(self, cb);
            }
            else if (res < 0)
            {
                cb(res, 0);
            }
            else
            {
                self->parent->fs_next_id = prev_val+2;
                self->parent->fs_allocated_id = new_val;
                cb(0, prev_val+1);
            }
        }, [prev_val](int res, const std::string & value)
        {
            // FIXME: Allow to modify value from CAS callback? ("update" query)
            return res < 0 || stoull_full(value) == prev_val;
        });
    });
}

struct kv_create_state
{
    nfs_client_t *self = NULL;
    rpc_op_t *rop = NULL;
    bool exclusive = false;
    uint64_t verf = 0;
    uint64_t dir_ino = 0;
    std::string filename;
    uint64_t new_id = 0;
    json11::Json::object attrobj;
    json11::Json attrs;
    std::string direntry_text;
    uint64_t dup_ino = 0;
    std::function<void(int res)> cb;
};

static void kv_do_create(kv_create_state *st)
{
    if (st->self->parent->trace)
        fprintf(stderr, "[%d] CREATE %ju/%s ATTRS %s\n", st->self->nfs_fd, st->dir_ino, st->filename.c_str(), json11::Json(st->attrobj).dump().c_str());
    if (st->filename == "" || st->filename.find("/") != std::string::npos)
    {
        auto cb = std::move(st->cb);
        cb(-EINVAL);
        return;
    }
    // Generate inode ID
    allocate_new_id(st->self, [st](int res, uint64_t new_id)
    {
        if (res < 0)
        {
            auto cb = std::move(st->cb);
            cb(res);
            return;
        }
        st->new_id = new_id;
        auto direntry = json11::Json::object{ { "ino", st->new_id } };
        if (st->attrobj["type"].string_value() == "dir")
        {
            direntry["type"] = "dir";
        }
        st->attrs = std::move(st->attrobj);
        st->direntry_text = json11::Json(direntry).dump().c_str();
        // Set direntry
        st->self->parent->db->set(kv_direntry_key(st->dir_ino, st->filename), st->direntry_text, [st](int res)
        {
            if (res < 0)
            {
                st->self->parent->unallocated_ids.push_back(st->new_id);
                if (res == -EAGAIN)
                {
                    if (st->dup_ino)
                    {
                        st->new_id = st->dup_ino;
                        res = 0;
                    }
                    else
                        res = -EEXIST;
                }
                else
                    fprintf(stderr, "create %ju/%s failed: %s (code %d)\n", st->dir_ino, st->filename.c_str(), strerror(-res), res);
                auto cb = std::move(st->cb);
                cb(res);
            }
            else
            {
                st->self->parent->db->set(kv_inode_key(st->new_id), st->attrs.dump().c_str(), [st](int res)
                {
                    if (res == -EAGAIN)
                    {
                        res = -EEXIST;
                    }
                    if (res < 0)
                    {
                        st->self->parent->db->del(kv_direntry_key(st->dir_ino, st->filename), [st, res](int del_res)
                        {
                            if (!del_res)
                            {
                                st->self->parent->unallocated_ids.push_back(st->new_id);
                            }
                            auto cb = std::move(st->cb);
                            cb(res);
                        }, [st](int res, const std::string & value)
                        {
                            return res != -ENOENT && value == st->direntry_text;
                        });
                    }
                    else
                    {
                        auto cb = std::move(st->cb);
                        cb(0);
                    }
                }, [st](int res, const std::string & value)
                {
                    return res == -ENOENT;
                });
            }
        }, [st](int res, const std::string & value)
        {
            // CAS compare - check that the key doesn't exist
            if (res == 0)
            {
                std::string err;
                auto direntry = json11::Json::parse(value, err);
                if (err != "")
                {
                    fprintf(stderr, "Invalid JSON in direntry %s = %s: %s, overwriting\n",
                        kv_direntry_key(st->dir_ino, st->filename).c_str(), value.c_str(), err.c_str());
                    return true;
                }
                if (st->exclusive && direntry["verf"].uint64_value() == st->verf)
                {
                    st->dup_ino = direntry["ino"].uint64_value();
                    return false;
                }
                return false;
            }
            return true;
        });
    });
}

static void kv_create_setattr(json11::Json::object & attrobj, sattr3 & sattr)
{
    if (sattr.mode.set_it)
        attrobj["mode"] = (uint64_t)sattr.mode.mode;
    if (sattr.uid.set_it)
        attrobj["uid"] = (uint64_t)sattr.uid.uid;
    if (sattr.gid.set_it)
        attrobj["gid"] = (uint64_t)sattr.gid.gid;
    if (sattr.atime.set_it)
        attrobj["atime"] = nfstime_to_str(sattr.atime.atime);
    if (sattr.mtime.set_it)
        attrobj["mtime"] = nfstime_to_str(sattr.mtime.mtime);
}

template<class T, class Tok> static void kv_create_reply(kv_create_state *st, int res)
{
    T *reply = (T*)st->rop->reply;
    if (res < 0)
    {
        *reply = (T){ .status = vitastor_nfs_map_err(-res) };
    }
    else
    {
        *reply = (T){
            .status = NFS3_OK,
            .resok = (Tok){
                .obj = {
                    .handle_follows = 1,
                    .handle = xdr_copy_string(st->rop->xdrs, kv_fh(st->new_id)),
                },
                .obj_attributes = {
                    .attributes_follow = 1,
                    .attributes = get_kv_attributes(st->self, st->new_id, st->attrs),
                },
            },
        };
    }
    rpc_queue_reply(st->rop);
    delete st;
}

static int nfs3_create_proc(void *opaque, rpc_op_t *rop)
{
    kv_create_state *st = new kv_create_state;
    st->self = (nfs_client_t*)opaque;
    st->rop = rop;
    auto args = (CREATE3args*)rop->request;
    st->exclusive = args->how.mode == NFS_EXCLUSIVE;
    st->verf = st->exclusive ? *(uint64_t*)&args->how.verf : 0;
    st->dir_ino = kv_fh_inode(args->where.dir);
    st->filename = args->where.name;
    if (args->how.mode == NFS_EXCLUSIVE)
    {
        st->attrobj["verf"] = *(uint64_t*)&args->how.verf;
    }
    else if (args->how.mode == NFS_UNCHECKED)
    {
        kv_create_setattr(st->attrobj, args->how.obj_attributes);
        if (args->how.obj_attributes.size.set_it)
        {
            st->attrobj["size"] = (uint64_t)args->how.obj_attributes.size.size;
            st->attrobj["empty"] = true;
        }
    }
    st->cb = [st](int res) { kv_create_reply<CREATE3res, CREATE3resok>(st, res); };
    kv_do_create(st);
    return 1;
}

static int nfs3_mkdir_proc(void *opaque, rpc_op_t *rop)
{
    kv_create_state *st = new kv_create_state;
    st->self = (nfs_client_t*)opaque;
    st->rop = rop;
    auto args = (MKDIR3args*)rop->request;
    st->dir_ino = kv_fh_inode(args->where.dir);
    st->filename = args->where.name;
    st->attrobj["type"] = "dir";
    st->attrobj["parent_ino"] = st->dir_ino;
    kv_create_setattr(st->attrobj, args->attributes);
    st->cb = [st](int res) { kv_create_reply<MKDIR3res, MKDIR3resok>(st, res); };
    kv_do_create(st);
    return 1;
}

static int nfs3_symlink_proc(void *opaque, rpc_op_t *rop)
{
    kv_create_state *st = new kv_create_state;
    st->self = (nfs_client_t*)opaque;
    st->rop = rop;
    auto args = (SYMLINK3args*)rop->request;
    st->dir_ino = kv_fh_inode(args->where.dir);
    st->filename = args->where.name;
    st->attrobj["type"] = "link";
    st->attrobj["symlink"] = (std::string)args->symlink.symlink_data;
    kv_create_setattr(st->attrobj, args->symlink.symlink_attributes);
    st->cb = [st](int res) { kv_create_reply<SYMLINK3res, SYMLINK3resok>(st, res); };
    kv_do_create(st);
    return 1;
}

static int nfs3_mknod_proc(void *opaque, rpc_op_t *rop)
{
    kv_create_state *st = new kv_create_state;
    st->self = (nfs_client_t*)opaque;
    st->rop = rop;
    auto args = (MKNOD3args*)rop->request;
    st->dir_ino = kv_fh_inode(args->where.dir);
    st->filename = args->where.name;
    if (args->what.type == NF3CHR || args->what.type == NF3BLK)
    {
        st->attrobj["type"] = (args->what.type == NF3CHR ? "chr" : "blk");
        st->attrobj["major"] = (uint64_t)args->what.chr_device.spec.specdata1;
        st->attrobj["minor"] = (uint64_t)args->what.chr_device.spec.specdata2;
        kv_create_setattr(st->attrobj, args->what.chr_device.dev_attributes);
    }
    else if (args->what.type == NF3SOCK || args->what.type == NF3FIFO)
    {
        st->attrobj["type"] = (args->what.type == NF3SOCK ? "sock" : "fifo");
        kv_create_setattr(st->attrobj, args->what.sock_attributes);
    }
    else
    {
        *(MKNOD3res*)rop->reply = (MKNOD3res){ .status = NFS3ERR_INVAL };
        rpc_queue_reply(rop);
        delete st;
        return 0;
    }
    st->cb = [st](int res) { kv_create_reply<MKNOD3res, MKNOD3resok>(st, res); };
    kv_do_create(st);
    return 1;
}

struct kv_del_state
{
    nfs_client_t *self = NULL;
    rpc_op_t *rop = NULL;
    uint64_t dir_ino = 0;
    std::string filename;
    uint64_t ino = 0;
    void *list_handle = NULL;
    std::string prefix, list_key, direntry_text, ientry_text;
    json11::Json direntry, ientry;
    int type = 0;
    bool is_rmdir = false;
    bool rm_data = false;
    int res = 0, res2 = 0;
    std::function<void(int)> cb;
};

static void nfs_kv_continue_delete(kv_del_state *st, int state)
{
    // Overall algorithm:
    // 1) Get inode attributes and check that it's not a directory (REMOVE)
    // 2) Get inode attributes and check that it is a directory (RMDIR)
    // 3) Delete direntry with CAS
    // 4) Check that the directory didn't contain files (RMDIR) and restore it if it did
    // 5) Reduce inode refcount by 1 or delete inode
    // 6) If regular file and inode is deleted: delete data
    if (state == 0)      {}
    else if (state == 1) goto resume_1;
    else if (state == 2) goto resume_2;
    else if (state == 3) goto resume_3;
    else if (state == 4) goto resume_4;
    else if (state == 5) goto resume_5;
    else if (state == 6) goto resume_6;
    else if (state == 7) goto resume_7;
    else
    {
        fprintf(stderr, "BUG: invalid state in nfs_kv_continue_delete()");
        abort();
    }
    st->self->parent->db->get(kv_direntry_key(st->dir_ino, st->filename), [st](int res, const std::string & value)
    {
        st->res = res;
        st->direntry_text = value;
        nfs_kv_continue_delete(st, 1);
    });
    return;
resume_1:
    if (st->res < 0)
    {
        auto cb = std::move(st->cb);
        cb(st->res);
        return;
    }
    {
        std::string err;
        st->direntry = json11::Json::parse(st->direntry_text, err);
        if (err != "")
        {
            fprintf(stderr, "Invalid JSON in direntry %s = %s: %s, deleting\n",
                kv_direntry_key(st->dir_ino, st->filename).c_str(), st->direntry_text.c_str(), err.c_str());
            // Just delete direntry and skip inode
        }
        else
        {
            st->ino = st->direntry["ino"].uint64_value();
        }
    }
    // Get inode
    st->self->parent->db->get(kv_inode_key(st->ino), [st](int res, const std::string & value)
    {
        st->res = res;
        st->ientry_text = value;
        nfs_kv_continue_delete(st, 2);
    });
    return;
resume_2:
    if (st->res < 0)
    {
        fprintf(stderr, "error reading inode %s: %s (code %d)\n",
            kv_inode_key(st->ino).c_str(), strerror(-st->res), st->res);
        auto cb = std::move(st->cb);
        cb(st->res);
        return;
    }
    {
        std::string err;
        st->ientry = json11::Json::parse(st->ientry_text, err);
        if (err != "")
        {
            fprintf(stderr, "Invalid JSON in inode %s = %s: %s, treating as a regular file\n",
                kv_inode_key(st->ino).c_str(), st->ientry_text.c_str(), err.c_str());
        }
    }
    // (1-2) Check type
    st->type = kv_map_type(st->ientry["type"].string_value());
    if (st->type == -1 || st->is_rmdir != (st->type == NF3DIR))
    {
        auto cb = std::move(st->cb);
        cb(st->is_rmdir ? -ENOTDIR : -EISDIR);
        return;
    }
    // (3) Delete direntry with CAS
    st->self->parent->db->del(kv_direntry_key(st->dir_ino, st->filename), [st](int res)
    {
        st->res = res;
        nfs_kv_continue_delete(st, 3);
    }, [st](int res, const std::string & value)
    {
        return value == st->direntry_text;
    });
    return;
resume_3:
    if (st->res == -EAGAIN)
    {
        // CAS failure, restart from the beginning
        nfs_kv_continue_delete(st, 0);
        return;
    }
    else if (st->res < 0 && st->res != -ENOENT)
    {
        fprintf(stderr, "failed to remove direntry %s: %s (code %d)\n",
            kv_direntry_key(st->dir_ino, st->filename).c_str(), strerror(-st->res), st->res);
        auto cb = std::move(st->cb);
        cb(st->res);
        return;
    }
    if (!st->ino)
    {
        // direntry contained invalid JSON and was deleted, finish
        auto cb = std::move(st->cb);
        cb(0);
        return;
    }
    if (st->is_rmdir)
    {
        // (4) Check if directory actually is not empty
        st->list_handle = st->self->parent->db->list_start(kv_direntry_key(st->ino, ""));
        st->self->parent->db->list_next(st->list_handle, [st](int res, const std::string & key, const std::string & value)
        {
            st->res = res;
            st->list_key = key;
            st->self->parent->db->list_close(st->list_handle);
            nfs_kv_continue_delete(st, 4);
        });
        return;
resume_4:
        st->prefix = kv_direntry_key(st->ino, "");
        if (st->res == -ENOENT || st->list_key.size() < st->prefix.size() || st->list_key.substr(0, st->prefix.size()) != st->prefix)
        {
            // OK, directory is empty
        }
        else
        {
            // Not OK, restore direntry
            st->self->parent->db->del(kv_direntry_key(st->dir_ino, st->filename), [st](int res)
            {
                st->res2 = res;
                nfs_kv_continue_delete(st, 5);
            }, [st](int res, const std::string & value)
            {
                return res == -ENOENT;
            });
            return;
resume_5:
            if (st->res2 < 0)
            {
                fprintf(stderr, "failed to restore direntry %s (%s): %s (code %d)",
                    kv_direntry_key(st->dir_ino, st->filename).c_str(), st->direntry_text.c_str(), strerror(-st->res2), st->res2);
                fprintf(stderr, " - inode %ju may be left as garbage\n", st->ino);
            }
            if (st->res < 0)
            {
                fprintf(stderr, "failed to list entries from %s: %s (code %d)\n",
                    kv_direntry_key(st->ino, "").c_str(), strerror(-st->res), st->res);
            }
            auto cb = std::move(st->cb);
            cb(st->res < 0 ? st->res : -ENOTEMPTY);
            return;
        }
    }
    // (5) Reduce inode refcount by 1 or delete inode
    if (st->ientry["nlink"].uint64_value() > 1)
    {
        auto copy = st->ientry.object_items();
        copy["nlink"] = st->ientry["nlink"].uint64_value()-1;
        st->self->parent->db->set(kv_inode_key(st->ino), json11::Json(copy).dump(), [st](int res)
        {
            st->res = res;
            nfs_kv_continue_delete(st, 6);
        }, [st](int res, const std::string & old_value)
        {
            return old_value == st->ientry_text;
        });
    }
    else
    {
        st->self->parent->db->del(kv_inode_key(st->ino), [st](int res)
        {
            st->res = res;
            nfs_kv_continue_delete(st, 6);
        }, [st](int res, const std::string & old_value)
        {
            return old_value == st->ientry_text;
        });
    }
    return;
resume_6:
    if (st->res < 0)
    {
        // Assume EAGAIN is OK, maybe someone created a hard link in the meantime
        auto cb = std::move(st->cb);
        cb(st->res == -EAGAIN ? 0 : st->res);
        return;
    }
    // (6) If regular file and inode is deleted: delete data
    if ((!st->type || st->type == NF3REG) && st->ientry["nlink"].uint64_value() <= 1)
    {
        // Remove data
        st->self->parent->cmd->loop_and_wait(st->self->parent->cmd->start_rm_data(json11::Json::object {
            { "inode", INODE_NO_POOL(st->self->parent->fs_base_inode + st->ino) },
            { "pool", (uint64_t)INODE_POOL(st->self->parent->fs_base_inode + st->ino) },
        }), [st](const cli_result_t & r)
        {
            if (r.err)
            {
                fprintf(stderr, "Failed to remove inode %jx data: %s (code %d)\n",
                    st->ino, r.text.c_str(), r.err);
            }
            st->res = r.err;
            nfs_kv_continue_delete(st, 7);
        });
        return;
resume_7:
        auto cb = std::move(st->cb);
        cb(st->res);
        return;
    }
    auto cb = std::move(st->cb);
    cb(0);
}

static int nfs3_remove_proc(void *opaque, rpc_op_t *rop)
{
    kv_del_state *st = new kv_del_state;
    st->self = (nfs_client_t*)opaque;
    st->rop = rop;
    REMOVE3res *reply = (REMOVE3res*)rop->reply;
    REMOVE3args *args = (REMOVE3args*)rop->request;
    st->dir_ino = kv_fh_inode(args->object.dir);
    st->filename = args->object.name;
    if (st->self->parent->trace)
        fprintf(stderr, "[%d] REMOVE %ju/%s\n", st->self->nfs_fd, st->dir_ino, st->filename.c_str());
    if (!st->dir_ino)
    {
        *reply = (REMOVE3res){ .status = NFS3ERR_INVAL };
        rpc_queue_reply(rop);
        delete st;
        return 0;
    }
    st->cb = [st](int res)
    {
        *((REMOVE3res*)st->rop->reply) = (REMOVE3res){
            .status = vitastor_nfs_map_err(res),
        };
        rpc_queue_reply(st->rop);
        delete st;
    };
    nfs_kv_continue_delete(st, 0);
    return 1;
}

static int nfs3_rmdir_proc(void *opaque, rpc_op_t *rop)
{
    kv_del_state *st = new kv_del_state;
    st->self = (nfs_client_t*)opaque;
    st->rop = rop;
    RMDIR3args *args = (RMDIR3args*)rop->request;
    RMDIR3res *reply = (RMDIR3res*)rop->reply;
    st->dir_ino = kv_fh_inode(args->object.dir);
    st->filename = args->object.name;
    st->is_rmdir = true;
    if (st->self->parent->trace)
        fprintf(stderr, "[%d] RMDIR %ju/%s\n", st->self->nfs_fd, st->dir_ino, st->filename.c_str());
    if (!st->dir_ino)
    {
        *reply = (RMDIR3res){ .status = NFS3ERR_INVAL };
        rpc_queue_reply(rop);
        delete st;
        return 0;
    }
    st->cb = [st](int res)
    {
        *((RMDIR3res*)st->rop->reply) = (RMDIR3res){
            .status = vitastor_nfs_map_err(res),
        };
        rpc_queue_reply(st->rop);
        delete st;
    };
    nfs_kv_continue_delete(st, 0);
    return 1;
}

struct nfs_kv_rename_state
{
    nfs_client_t *self = NULL;
    rpc_op_t *rop = NULL;
    uint64_t old_dir_ino = 0, new_dir_ino = 0;
    std::string old_name, new_name;
    std::string old_direntry_text;
    std::string ientry_text;
    json11::Json direntry, ientry;
    int res = 0, res2 = 0;
    std::function<void(int)> cb;
};

static void nfs_kv_continue_rename(nfs_kv_rename_state *st, int state)
{
    // Simplified algorithm (non-atomic and without ENOTDIR/EISDIR):
    // 1) Check if the target directory exists
    // 2) Delete & save (using CAS) the source direntry
    // 3) Write the target direntry using CAS, fail if it already exists
    // 4) Restore the source direntry on failure
    // Atomic version would require something like a journal
    if (state == 0)      {}
    else if (state == 1) goto resume_1;
    else if (state == 2) goto resume_2;
    else if (state == 3) goto resume_3;
    else if (state == 4) goto resume_4;
    else if (state == 5) goto resume_5;
    else if (state == 6) goto resume_6;
    else
    {
        fprintf(stderr, "BUG: invalid state in nfs_kv_continue_rename()");
        abort();
    }
    kv_read_inode(st->self, st->new_dir_ino, [st](int res, const std::string & value, json11::Json attrs)
    {
        st->res = res == 0 ? (attrs["type"].string_value() == "dir" ? 0 : -ENOTDIR) : res;
        nfs_kv_continue_rename(st, 1);
    });
    return;
resume_1:
    if (st->res < 0)
    {
        auto cb = std::move(st->cb);
        cb(st->res);
        return;
    }
    // Read & delete the old direntry
    st->self->parent->db->del(kv_direntry_key(st->old_dir_ino, st->old_name), [st](int res)
    {
        st->res = res;
        nfs_kv_continue_rename(st, 2);
    }, [=](int res, const std::string & old_value)
    {
        st->res = res;
        st->old_direntry_text = old_value;
        return true;
    });
    return;
resume_2:
    if (st->res < 0)
    {
        auto cb = std::move(st->cb);
        cb(st->res);
        return;
    }
    {
        std::string err;
        st->direntry = json11::Json::parse(st->old_direntry_text, err);
        if (err != "")
        {
            fprintf(stderr, "Invalid JSON in direntry %s = %s: %s\n",
                kv_direntry_key(st->old_dir_ino, st->old_name).c_str(),
                st->old_direntry_text.c_str(), err.c_str());
        }
    }
    if (st->direntry["type"].string_value() == "dir" &&
        st->direntry["ino"].uint64_value() != 0 &&
        st->new_dir_ino != st->old_dir_ino)
    {
        // Read & check inode
        kv_read_inode(st->self, st->direntry["ino"].uint64_value(), [st](int res, const std::string & value, json11::Json ientry)
        {
            st->res = res;
            st->ientry_text = value;
            st->ientry = ientry;
            nfs_kv_continue_rename(st, 3);
        });
        return;
resume_3:
        if (st->res < 0)
        {
            auto cb = std::move(st->cb);
            cb(st->res);
            return;
        }
        // Change parent reference
        {
            auto ientry_new = st->ientry.object_items();
            ientry_new["parent_ino"] = st->new_dir_ino;
            st->self->parent->db->set(kv_inode_key(st->direntry["ino"].uint64_value()), json11::Json(ientry_new).dump(), [st](int res)
            {
                st->res = res;
                nfs_kv_continue_rename(st, 4);
            }, [st](int res, const std::string & old_value)
            {
                return old_value == st->ientry_text;
            });
        }
        return;
resume_4:
        if (st->res < 0)
        {
            auto cb = std::move(st->cb);
            cb(st->res);
            return;
        }
    }
    st->self->parent->db->set(kv_direntry_key(st->new_dir_ino, st->new_name), st->old_direntry_text, [st](int res)
    {
        st->res = res;
        nfs_kv_continue_rename(st, 5);
    }, [st](int res, const std::string & old_value)
    {
        return res == -ENOENT;
    });
    return;
resume_5:
    if (st->res < 0)
    {
        if (st->res == -EAGAIN)
            st->res = -EEXIST;
        st->res2 = st->res;
        st->self->parent->db->set(kv_direntry_key(st->old_dir_ino, st->old_name), st->old_direntry_text, [st](int res)
        {
            st->res = res;
            nfs_kv_continue_rename(st, 6);
        }, [st](int res, const std::string & old_value)
        {
            return res == -ENOENT;
        });
        return;
resume_6:
        if (st->res < 0)
        {
            if (st->res == -EAGAIN)
                st->res = -EEXIST;
            fprintf(stderr, "error restoring %s = %s after failed rename: %s (code %d)\n",
                kv_direntry_key(st->old_dir_ino, st->old_name).c_str(), st->old_direntry_text.c_str(),
                strerror(-st->res), st->res);
        }
        auto cb = std::move(st->cb);
        cb(st->res2);
        return;
    }
    auto cb = std::move(st->cb);
    cb(st->res);
}

static int nfs3_rename_proc(void *opaque, rpc_op_t *rop)
{
    auto st = new nfs_kv_rename_state;
    st->self = (nfs_client_t*)opaque;
    st->rop = rop;
    RENAME3args *args = (RENAME3args*)rop->request;
    st->old_dir_ino = kv_fh_inode(args->from.dir);
    st->new_dir_ino = kv_fh_inode(args->to.dir);
    st->old_name = args->from.name;
    st->new_name = args->to.name;
    if (st->self->parent->trace)
        fprintf(stderr, "[%d] RENAME %ju/%s -> %ju/%s\n", st->self->nfs_fd, st->old_dir_ino, st->old_name.c_str(), st->new_dir_ino, st->new_name.c_str());
    if (!st->old_dir_ino || !st->new_dir_ino || st->old_name == "" || st->new_name == "")
    {
        RENAME3res *reply = (RENAME3res*)rop->reply;
        *reply = (RENAME3res){ .status = NFS3ERR_INVAL };
        rpc_queue_reply(rop);
        delete st;
        return 0;
    }
    if (st->old_dir_ino == st->new_dir_ino && st->old_name == st->new_name)
    {
        RENAME3res *reply = (RENAME3res*)rop->reply;
        *reply = (RENAME3res){ .status = NFS3_OK };
        rpc_queue_reply(st->rop);
        delete st;
        return 0;
    }
    st->cb = [st](int res)
    {
        RENAME3res *reply = (RENAME3res*)st->rop->reply;
        *reply = (RENAME3res){ .status = vitastor_nfs_map_err(res) };
        rpc_queue_reply(st->rop);
        delete st;
    };
    nfs_kv_continue_rename(st, 0);
    return 1;
}

struct nfs_kv_link_state
{
    nfs_client_t *self = NULL;
    rpc_op_t *rop = NULL;
    uint64_t ino = 0;
    uint64_t dir_ino = 0;
    std::string filename;
    std::string ientry_text;
    json11::Json ientry;
    bool retrying = false;
    int wait = 0;
    int res = 0, res2 = 0;
    std::function<void(int)> cb;
};

static void nfs_kv_continue_link(nfs_kv_link_state *st, int state)
{
    // 1) Read the source inode
    // 2) If it's a directory - fail with -EISDIR
    // 3) Create the new direntry with the same inode reference
    // 4) Update the inode entry with refcount++
    // 5) Retry update if CAS failed but the inode exists
    // 6) Otherwise fail and remove the new direntry
    // Yeah we may leave a bad direntry if we crash
    // But the other option is to possibly leave an inode with too big refcount
    if (state == 0)      {}
    else if (state == 1) goto resume_1;
    else if (state == 2) goto resume_2;
    else if (state == 3) goto resume_3;
    else if (state == 4) goto resume_4;
    else
    {
        fprintf(stderr, "BUG: invalid state in nfs_kv_continue_link()");
        abort();
    }
resume_0:
    // Check that the source inode exists and is not a directory
    st->wait = st->retrying ? 1 : 2;
    st->res2 = 0;
    kv_read_inode(st->self, st->ino, [st](int res, const std::string & value, json11::Json attrs)
    {
        st->res = res == 0 ? (attrs["type"].string_value() == "dir" ? -EISDIR : 0) : res;
        st->ientry_text = value;
        st->ientry = attrs;
        if (!--st->wait)
            nfs_kv_continue_link(st, 1);
    });
    if (!st->retrying)
    {
        // Check that the new directory exists
        kv_read_inode(st->self, st->dir_ino, [st](int res, const std::string & value, json11::Json attrs)
        {
            st->res2 = res == 0 ? (attrs["type"].string_value() == "dir" ? 0 : -ENOTDIR) : res;
            if (!--st->wait)
                nfs_kv_continue_link(st, 1);
        });
    }
    return;
resume_1:
    if (st->res < 0 || st->res2 < 0)
    {
        auto cb = std::move(st->cb);
        cb(st->res < 0 ? st->res : st->res2);
        return;
    }
    // Write the new direntry
    if (!st->retrying)
    {
        st->self->parent->db->set(kv_direntry_key(st->dir_ino, st->filename),
            json11::Json(json11::Json::object{ { "ino", st->ino } }).dump(), [st](int res)
        {
            st->res = res;
            nfs_kv_continue_link(st, 2);
        }, [st](int res, const std::string & old_value)
        {
            return res == -ENOENT;
        });
        return;
resume_2:
        if (st->res < 0)
        {
            auto cb = std::move(st->cb);
            cb(st->res);
            return;
        }
    }
    // Increase inode refcount
    {
        auto new_ientry = st->ientry.object_items();
        auto nlink = new_ientry["nlink"].uint64_value();
        new_ientry["nlink"] = nlink ? nlink+1 : 2;
        st->ientry = new_ientry;
    }
    st->self->parent->db->set(kv_inode_key(st->ino), st->ientry.dump(), [st](int res)
    {
        st->res = res;
        nfs_kv_continue_link(st, 3);
    }, [st](int res, const std::string & old_value)
    {
        st->res2 = res;
        return res == 0 && old_value == st->ientry_text;
    });
    return;
resume_3:
    if (st->res2 == -ENOENT)
    {
        st->res = -ENOENT;
    }
    if (st->res == -EAGAIN)
    {
        // Re-read inode and retry
        st->retrying = true;
        goto resume_0;
    }
    if (st->res < 0)
    {
        // Maybe inode was deleted in the meantime, delete our direntry
        st->self->parent->db->del(kv_direntry_key(st->dir_ino, st->filename), [st](int res)
        {
            st->res2 = res;
            nfs_kv_continue_link(st, 4);
        });
        return;
resume_4:
        if (st->res2 < 0)
        {
            fprintf(stderr, "Warning: failed to delete new linked direntry %ju/%s: %s (code %d)\n",
                st->dir_ino, st->filename.c_str(), strerror(-st->res2), st->res2);
        }
    }
    auto cb = std::move(st->cb);
    cb(st->res);
}

// FIXME: We'll need some tests for the FS
static int nfs3_link_proc(void *opaque, rpc_op_t *rop)
{
    auto st = new nfs_kv_link_state;
    st->self = (nfs_client_t*)opaque;
    st->rop = rop;
    LINK3args *args = (LINK3args*)rop->request;
    st->ino = kv_fh_inode(args->file);
    st->dir_ino = kv_fh_inode(args->link.dir);
    st->filename = args->link.name;
    if (st->self->parent->trace)
        fprintf(stderr, "[%d] LINK %ju -> %ju/%s\n", st->self->nfs_fd, st->ino, st->dir_ino, st->filename.c_str());
    if (!st->ino || !st->dir_ino || st->filename == "")
    {
        LINK3res *reply = (LINK3res*)rop->reply;
        *reply = (LINK3res){ .status = NFS3ERR_INVAL };
        rpc_queue_reply(rop);
        delete st;
        return 0;
    }
    st->cb = [st](int res)
    {
        LINK3res *reply = (LINK3res*)st->rop->reply;
        if (res < 0)
        {
            *reply = (LINK3res){ .status = vitastor_nfs_map_err(res) };
        }
        else
        {
            *reply = (LINK3res){
                .status = NFS3_OK,
                .resok = (LINK3resok){
                    .file_attributes = (post_op_attr){
                        .attributes_follow = 1,
                        .attributes = get_kv_attributes(st->self, st->ino, st->ientry),
                    },
                },
            };
        }
        rpc_queue_reply(st->rop);
        delete st;
    };
    nfs_kv_continue_link(st, 0);
    return 1;
}

struct nfs_kv_readdir_state
{
    nfs_client_t *self = NULL;
    rpc_op_t *rop = NULL;
    // Request:
    bool is_plus = false;
    uint64_t cookie = 0;
    uint64_t cookieverf = 0;
    uint64_t dir_ino = 0;
    uint64_t maxcount = 0;
    std::function<void(int)> cb;
    // State:
    int res = 0;
    std::string prefix, start;
    void *list_handle;
    uint64_t parent_ino = 0;
    std::string ientry_text, parent_ientry_text;
    json11::Json ientry, parent_ientry;
    std::string cur_key, cur_value;
    int reply_size = 0;
    int to_skip = 0;
    uint64_t offset = 0;
    int getattr_running = 0, getattr_cur = 0;
    // Result:
    bool eof = false;
    //uint64_t cookieverf = 0; // same field
    std::vector<entryplus3> entries;
};

static void nfs_kv_continue_readdir(nfs_kv_readdir_state *st, int state);

static void kv_getattr_next(nfs_kv_readdir_state *st)
{
    while (st->is_plus && st->getattr_cur < st->entries.size() && st->getattr_running < st->self->parent->readdir_getattr_parallel)
    {
        auto idx = st->getattr_cur++;
        st->getattr_running++;
        kv_read_inode(st->self, st->entries[idx].fileid, [st, idx](int res, const std::string & value, json11::Json ientry)
        {
            if (res == 0)
            {
                st->entries[idx].name_attributes = (post_op_attr){
                    // FIXME: maybe do not read parent attributes and leave them to a GETATTR?
                    .attributes_follow = 1,
                    .attributes = get_kv_attributes(st->self, st->entries[idx].fileid, ientry),
                };
            }
            st->getattr_running--;
            kv_getattr_next(st);
            if (st->getattr_running == 0 && !st->list_handle)
            {
                nfs_kv_continue_readdir(st, 4);
            }
        });
    }
}

static void nfs_kv_continue_readdir(nfs_kv_readdir_state *st, int state)
{
    if (state == 0)      {}
    else if (state == 1) goto resume_1;
    else if (state == 2) goto resume_2;
    else if (state == 3) goto resume_3;
    else if (state == 4) goto resume_4;
    else
    {
        fprintf(stderr, "BUG: invalid state in nfs_kv_continue_readdir()");
        abort();
    }
    // Limit results based on maximum reply size
    // Sadly we have to calculate reply size by hand
    // reply without entries is 4+4+(dir_attributes ? sizeof(fattr3) : 0)+8+4 bytes
    st->reply_size = 20;
    if (st->reply_size > st->maxcount)
    {
        // Error, too small max reply size
        auto cb = std::move(st->cb);
        cb(-NFS3ERR_TOOSMALL);
        return;
    }
    // Add . and ..
    if (st->cookie <= 1)
    {
        kv_read_inode(st->self, st->dir_ino, [st](int res, const std::string & value, json11::Json ientry)
        {
            st->res = res;
            st->ientry_text = value;
            st->ientry = ientry;
            nfs_kv_continue_readdir(st, 1);
        });
        return;
resume_1:
        if (st->res < 0)
        {
            auto cb = std::move(st->cb);
            cb(st->res);
            return;
        }
        if (st->cookie == 0)
        {
            auto fh = kv_fh(st->dir_ino);
            auto entry_size = 20 + 4/*len_pad4(".")*/ + (st->is_plus ? 8 + 88 + len_pad4(fh.size()) : 0);
            if (st->reply_size + entry_size > st->maxcount)
            {
                auto cb = std::move(st->cb);
                cb(-NFS3ERR_TOOSMALL);
                return;
            }
            entryplus3 dot = {};
            dot.name = xdr_copy_string(st->rop->xdrs, ".");
            dot.fileid = st->dir_ino;
            dot.name_attributes = (post_op_attr){
                .attributes_follow = 1,
                .attributes = get_kv_attributes(st->self, st->dir_ino, st->ientry),
            };
            dot.name_handle = (post_op_fh3){
                .handle_follows = 1,
                .handle = xdr_copy_string(st->rop->xdrs, fh),
            };
            st->entries.push_back(dot);
            st->reply_size += entry_size;
        }
        st->parent_ino = st->ientry["parent_ino"].uint64_value();
        if (st->parent_ino)
        {
            kv_read_inode(st->self, st->ientry["parent_ino"].uint64_value(), [st](int res, const std::string & value, json11::Json ientry)
            {
                st->res = res;
                st->parent_ientry_text = value;
                st->parent_ientry = ientry;
                nfs_kv_continue_readdir(st, 2);
            });
            return;
resume_2:
            if (st->res < 0)
            {
                auto cb = std::move(st->cb);
                cb(st->res);
                return;
            }
        }
        auto fh = kv_fh(st->parent_ino);
        auto entry_size = 20 + 4/*len_pad4("..")*/ + (st->is_plus ? 8 + 88 + len_pad4(fh.size()) : 0);
        if (st->reply_size + entry_size > st->maxcount)
        {
            st->eof = false;
            auto cb = std::move(st->cb);
            cb(0);
            return;
        }
        entryplus3 dotdot = {};
        dotdot.name = xdr_copy_string(st->rop->xdrs, "..");
        dotdot.fileid = st->dir_ino;
        dotdot.name_attributes = (post_op_attr){
            // FIXME: maybe do not read parent attributes and leave them to a GETATTR?
            .attributes_follow = 1,
            .attributes = get_kv_attributes(st->self,
                st->parent_ino ? st->parent_ino : st->dir_ino,
                st->parent_ino ? st->parent_ientry : st->ientry),
        };
        dotdot.name_handle = (post_op_fh3){
            .handle_follows = 1,
            .handle = xdr_copy_string(st->rop->xdrs, fh),
        };
        st->entries.push_back(dotdot);
        st->reply_size += entry_size;
    }
    st->prefix = kv_direntry_key(st->dir_ino, "");
    st->eof = true;
    st->start = st->prefix;
    if (st->cookie > 1)
    {
        auto lc_it = st->self->parent->list_cookies.find((list_cookie_t){ st->dir_ino, st->cookieverf, st->cookie });
        if (lc_it != st->self->parent->list_cookies.end())
        {
            st->start = lc_it->second.key;
            st->to_skip = 1;
            st->offset = st->cookie;
        }
        else
        {
            st->to_skip = st->cookie-2;
            st->offset = 2;
            st->cookieverf = ((uint64_t)lrand48() | ((uint64_t)lrand48() << 31) | ((uint64_t)lrand48() << 62));
        }
    }
    else
    {
        st->to_skip = 0;
        st->offset = 2;
        st->cookieverf = ((uint64_t)lrand48() | ((uint64_t)lrand48() << 31) | ((uint64_t)lrand48() << 62));
    }
    {
        auto lc_it = st->self->parent->list_cookies.lower_bound((list_cookie_t){ st->dir_ino, st->cookieverf, 0 });
        if (lc_it != st->self->parent->list_cookies.end() &&
            lc_it->first.dir_ino == st->dir_ino &&
            lc_it->first.cookieverf == st->cookieverf &&
            lc_it->first.cookie < st->cookie)
        {
            auto lc_start = lc_it;
            while (lc_it != st->self->parent->list_cookies.end() && lc_it->first.cookieverf == st->cookieverf)
            {
                lc_it++;
            }
            st->self->parent->list_cookies.erase(lc_start, lc_it);
        }
    }
    st->getattr_cur = st->entries.size();
    st->list_handle = st->self->parent->db->list_start(st->start);
    st->self->parent->db->list_next(st->list_handle, [=](int res, const std::string & key, const std::string & value)
    {
        st->res = res;
        st->cur_key = key;
        st->cur_value = value;
        nfs_kv_continue_readdir(st, 3);
    });
    return;
    while (st->list_handle)
    {
        st->self->parent->db->list_next(st->list_handle, NULL);
        return;
resume_3:
        if (st->res == -ENOENT || st->cur_key.size() > st->prefix.size() || st->cur_key.substr(0, st->prefix.size()) != st->prefix)
        {
            st->self->parent->db->list_close(st->list_handle);
            st->list_handle = NULL;
            break;
        }
        if (st->to_skip > 0)
        {
            st->to_skip--;
            continue;
        }
        std::string err;
        auto direntry = json11::Json::parse(st->cur_value, err);
        if (err != "")
        {
            fprintf(stderr, "readdir: direntry %s contains invalid JSON: %s, skipping\n",
                st->cur_key.c_str(), st->cur_value.c_str());
            continue;
        }
        auto ino = direntry["ino"].uint64_value();
        auto name = kv_direntry_filename(st->cur_key);
        auto fh = kv_fh(ino);
        // 1 entry3 is (8+4+(filename_len+3)/4*4+8) bytes
        // 1 entryplus3 is (8+4+(filename_len+3)/4*4+8
        //   + 4+(name_attributes ? (sizeof(fattr3) = 84) : 0)
        //   + 4+(name_handle ? 4+(handle_len+3)/4*4 : 0)) bytes
        auto entry_size = 20 + len_pad4(name.size()) + (st->is_plus ? 8 + 88 + len_pad4(fh.size()) : 0);
        if (st->reply_size + entry_size > st->maxcount)
        {
            st->eof = false;
            st->self->parent->db->list_close(st->list_handle);
            st->list_handle = NULL;
            break;
        }
        st->reply_size += entry_size;
        auto idx = st->entries.size();
        st->entries.push_back((entryplus3){});
        auto entry = &st->entries[idx];
        entry->name = xdr_copy_string(st->rop->xdrs, name);
        entry->fileid = ino;
        entry->cookie = st->offset++;
        st->self->parent->list_cookies[(list_cookie_t){ st->dir_ino, st->cookieverf, entry->cookie }] = { .key = entry->name };
        if (st->is_plus)
        {
            entry->name_handle = (post_op_fh3){
                .handle_follows = 1,
                .handle = xdr_copy_string(st->rop->xdrs, fh),
            };
            kv_getattr_next(st);
        }
        st->self->parent->db->list_next(st->list_handle, NULL);
    }
resume_4:
    while (st->getattr_running > 0)
    {
        return;
    }
    void *prev = NULL;
    for (int i = 0; i < st->entries.size(); i++)
    {
        entryplus3 *entry = &st->entries[i];
        if (prev)
        {
            if (st->is_plus)
                ((entryplus3*)prev)->nextentry = entry;
            else
                ((entry3*)prev)->nextentry = (entry3*)entry;
        }
        prev = entry;
    }
    // Send reply
    auto cb = std::move(st->cb);
    cb(0);
}

static void nfs3_readdir_common(void *opaque, rpc_op_t *rop, bool is_plus)
{
    auto st = new nfs_kv_readdir_state;
    st->self = (nfs_client_t*)opaque;
    st->rop = rop;
    st->is_plus = is_plus;
    if (st->is_plus)
    {
        READDIRPLUS3args *args = (READDIRPLUS3args*)rop->request;
        st->dir_ino = kv_fh_inode(args->dir);
        st->cookie = args->cookie;
        st->cookieverf = *((uint64_t*)args->cookieverf);
        st->maxcount = args->maxcount;
    }
    else
    {
        READDIR3args *args = ((READDIR3args*)rop->request);
        st->dir_ino = kv_fh_inode(args->dir);
        st->cookie = args->cookie;
        st->cookieverf = *((uint64_t*)args->cookieverf);
        st->maxcount = args->count;
    }
    if (st->self->parent->trace)
        fprintf(stderr, "[%d] READDIR %ju VERF %jx OFFSET %ju LIMIT %ju\n", st->self->nfs_fd, st->dir_ino, st->cookieverf, st->cookie, st->maxcount);
    st->cb = [st](int res)
    {
        if (st->is_plus)
        {
            READDIRPLUS3res *reply = (READDIRPLUS3res*)st->rop->reply;
            *reply = (READDIRPLUS3res){ .status = vitastor_nfs_map_err(res) };
            *(uint64_t*)(reply->resok.cookieverf) = st->cookieverf;
            reply->resok.reply.entries = st->entries.size() ? &st->entries[0] : NULL;
            reply->resok.reply.eof = st->eof;
        }
        else
        {
            READDIR3res *reply = (READDIR3res*)st->rop->reply;
            *reply = (READDIR3res){ .status = vitastor_nfs_map_err(res) };
            *(uint64_t*)(reply->resok.cookieverf) = st->cookieverf;
            reply->resok.reply.entries = st->entries.size() ? (entry3*)&st->entries[0] : NULL;
            reply->resok.reply.eof = st->eof;
        }
        rpc_queue_reply(st->rop);
        delete st;
    };
    nfs_kv_continue_readdir(st, 0);
}

static int nfs3_readdir_proc(void *opaque, rpc_op_t *rop)
{
    nfs3_readdir_common(opaque, rop, false);
    return 0;
}

static int nfs3_readdirplus_proc(void *opaque, rpc_op_t *rop)
{
    nfs3_readdir_common(opaque, rop, true);
    return 0;
}

// Get file system statistics
static int nfs3_fsstat_proc(void *opaque, rpc_op_t *rop)
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

static int nfs3_fsinfo_proc(void *opaque, rpc_op_t *rop)
{
    nfs_client_t *self = (nfs_client_t*)opaque;
    FSINFO3args *args = (FSINFO3args*)rop->request;
    FSINFO3res *reply = (FSINFO3res*)rop->reply;
    if (self->parent->trace)
        fprintf(stderr, "[%d] FSINFO %s\n", self->nfs_fd, std::string(args->fsroot).c_str());
    if (args->fsroot != kv_root_handle)
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

static int nfs3_pathconf_proc(void *opaque, rpc_op_t *rop)
{
    nfs_client_t *self = (nfs_client_t*)opaque;
    PATHCONF3args *args = (PATHCONF3args*)rop->request;
    PATHCONF3res *reply = (PATHCONF3res*)rop->reply;
    if (self->parent->trace)
        fprintf(stderr, "[%d] PATHCONF %s\n", self->nfs_fd, std::string(args->object).c_str());
    if (args->object != kv_root_handle)
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

static int nfs3_commit_proc(void *opaque, rpc_op_t *rop)
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

static int mount3_mnt_proc(void *opaque, rpc_op_t *rop)
{
    nfs_client_t *self = (nfs_client_t*)opaque;
    //nfs_dirpath *args = (nfs_dirpath*)rop->request;
    if (self->parent->trace)
        fprintf(stderr, "[%d] MNT\n", self->nfs_fd);
    nfs_mountres3 *reply = (nfs_mountres3*)rop->reply;
    u_int flavor = RPC_AUTH_NONE;
    reply->fhs_status = MNT3_OK;
    reply->mountinfo.fhandle = xdr_copy_string(rop->xdrs, kv_root_handle);
    reply->mountinfo.auth_flavors.auth_flavors_len = 1;
    reply->mountinfo.auth_flavors.auth_flavors_val = (u_int*)xdr_copy_string(rop->xdrs, (char*)&flavor, sizeof(u_int)).data;
    rpc_queue_reply(rop);
    return 0;
}

static int mount3_dump_proc(void *opaque, rpc_op_t *rop)
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

static int mount3_umnt_proc(void *opaque, rpc_op_t *rop)
{
    //nfs_client_t *self = (nfs_client_t*)opaque;
    //nfs_dirpath *arg = (nfs_dirpath*)rop->request;
    // do nothing
    rpc_queue_reply(rop);
    return 0;
}

static int mount3_umntall_proc(void *opaque, rpc_op_t *rop)
{
    // do nothing
    rpc_queue_reply(rop);
    return 0;
}

static int mount3_export_proc(void *opaque, rpc_op_t *rop)
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

nfs_client_t::nfs_client_t()
{
    struct rpc_service_proc_t pt[] = {
        {NFS_PROGRAM, NFS_V3, NFS3_NULL,        nfs3_null_proc,        NULL,                            0,                        NULL,                           0,                       this},
        {NFS_PROGRAM, NFS_V3, NFS3_GETATTR,     nfs3_getattr_proc,     (xdrproc_t)xdr_GETATTR3args,     sizeof(GETATTR3args),     (xdrproc_t)xdr_GETATTR3res,     sizeof(GETATTR3res),     this},
        {NFS_PROGRAM, NFS_V3, NFS3_SETATTR,     nfs3_setattr_proc,     (xdrproc_t)xdr_SETATTR3args,     sizeof(SETATTR3args),     (xdrproc_t)xdr_SETATTR3res,     sizeof(SETATTR3res),     this},
        {NFS_PROGRAM, NFS_V3, NFS3_LOOKUP,      nfs3_lookup_proc,      (xdrproc_t)xdr_LOOKUP3args,      sizeof(LOOKUP3args),      (xdrproc_t)xdr_LOOKUP3res,      sizeof(LOOKUP3res),      this},
        {NFS_PROGRAM, NFS_V3, NFS3_ACCESS,      nfs3_access_proc,      (xdrproc_t)xdr_ACCESS3args,      sizeof(ACCESS3args),      (xdrproc_t)xdr_ACCESS3res,      sizeof(ACCESS3res),      this},
        {NFS_PROGRAM, NFS_V3, NFS3_READLINK,    nfs3_readlink_proc,    (xdrproc_t)xdr_READLINK3args,    sizeof(READLINK3args),    (xdrproc_t)xdr_READLINK3res,    sizeof(READLINK3res),    this},
        {NFS_PROGRAM, NFS_V3, NFS3_READ,        nfs3_read_proc,        (xdrproc_t)xdr_READ3args,        sizeof(READ3args),        (xdrproc_t)xdr_READ3res,        sizeof(READ3res),        this},
        {NFS_PROGRAM, NFS_V3, NFS3_WRITE,       nfs3_write_proc,       (xdrproc_t)xdr_WRITE3args,       sizeof(WRITE3args),       (xdrproc_t)xdr_WRITE3res,       sizeof(WRITE3res),       this},
        {NFS_PROGRAM, NFS_V3, NFS3_CREATE,      nfs3_create_proc,      (xdrproc_t)xdr_CREATE3args,      sizeof(CREATE3args),      (xdrproc_t)xdr_CREATE3res,      sizeof(CREATE3res),      this},
        {NFS_PROGRAM, NFS_V3, NFS3_MKDIR,       nfs3_mkdir_proc,       (xdrproc_t)xdr_MKDIR3args,       sizeof(MKDIR3args),       (xdrproc_t)xdr_MKDIR3res,       sizeof(MKDIR3res),       this},
        {NFS_PROGRAM, NFS_V3, NFS3_SYMLINK,     nfs3_symlink_proc,     (xdrproc_t)xdr_SYMLINK3args,     sizeof(SYMLINK3args),     (xdrproc_t)xdr_SYMLINK3res,     sizeof(SYMLINK3res),     this},
        {NFS_PROGRAM, NFS_V3, NFS3_MKNOD,       nfs3_mknod_proc,       (xdrproc_t)xdr_MKNOD3args,       sizeof(MKNOD3args),       (xdrproc_t)xdr_MKNOD3res,       sizeof(MKNOD3res),       this},
        {NFS_PROGRAM, NFS_V3, NFS3_REMOVE,      nfs3_remove_proc,      (xdrproc_t)xdr_REMOVE3args,      sizeof(REMOVE3args),      (xdrproc_t)xdr_REMOVE3res,      sizeof(REMOVE3res),      this},
        {NFS_PROGRAM, NFS_V3, NFS3_RMDIR,       nfs3_rmdir_proc,       (xdrproc_t)xdr_RMDIR3args,       sizeof(RMDIR3args),       (xdrproc_t)xdr_RMDIR3res,       sizeof(RMDIR3res),       this},
        {NFS_PROGRAM, NFS_V3, NFS3_RENAME,      nfs3_rename_proc,      (xdrproc_t)xdr_RENAME3args,      sizeof(RENAME3args),      (xdrproc_t)xdr_RENAME3res,      sizeof(RENAME3res),      this},
        {NFS_PROGRAM, NFS_V3, NFS3_LINK,        nfs3_link_proc,        (xdrproc_t)xdr_LINK3args,        sizeof(LINK3args),        (xdrproc_t)xdr_LINK3res,        sizeof(LINK3res),        this},
        {NFS_PROGRAM, NFS_V3, NFS3_READDIR,     nfs3_readdir_proc,     (xdrproc_t)xdr_READDIR3args,     sizeof(READDIR3args),     (xdrproc_t)xdr_READDIR3res,     sizeof(READDIR3res),     this},
        {NFS_PROGRAM, NFS_V3, NFS3_READDIRPLUS, nfs3_readdirplus_proc, (xdrproc_t)xdr_READDIRPLUS3args, sizeof(READDIRPLUS3args), (xdrproc_t)xdr_READDIRPLUS3res, sizeof(READDIRPLUS3res), this},
        {NFS_PROGRAM, NFS_V3, NFS3_FSSTAT,      nfs3_fsstat_proc,      (xdrproc_t)xdr_FSSTAT3args,      sizeof(FSSTAT3args),      (xdrproc_t)xdr_FSSTAT3res,      sizeof(FSSTAT3res),      this},
        {NFS_PROGRAM, NFS_V3, NFS3_FSINFO,      nfs3_fsinfo_proc,      (xdrproc_t)xdr_FSINFO3args,      sizeof(FSINFO3args),      (xdrproc_t)xdr_FSINFO3res,      sizeof(FSINFO3res),      this},
        {NFS_PROGRAM, NFS_V3, NFS3_PATHCONF,    nfs3_pathconf_proc,    (xdrproc_t)xdr_PATHCONF3args,    sizeof(PATHCONF3args),    (xdrproc_t)xdr_PATHCONF3res,    sizeof(PATHCONF3res),    this},
        {NFS_PROGRAM, NFS_V3, NFS3_COMMIT,      nfs3_commit_proc,      (xdrproc_t)xdr_COMMIT3args,      sizeof(COMMIT3args),      (xdrproc_t)xdr_COMMIT3res,      sizeof(COMMIT3res),      this},
        {MOUNT_PROGRAM, MOUNT_V3, MOUNT3_NULL,    nfs3_null_proc,      NULL,                            0,                        NULL,                         0,                         this},
        {MOUNT_PROGRAM, MOUNT_V3, MOUNT3_MNT,     mount3_mnt_proc,     (xdrproc_t)xdr_nfs_dirpath,      sizeof(nfs_dirpath),      (xdrproc_t)xdr_nfs_mountres3, sizeof(nfs_mountres3),     this},
        {MOUNT_PROGRAM, MOUNT_V3, MOUNT3_DUMP,    mount3_dump_proc,    NULL,                            0,                        (xdrproc_t)xdr_nfs_mountlist, sizeof(nfs_mountlist),     this},
        {MOUNT_PROGRAM, MOUNT_V3, MOUNT3_UMNT,    mount3_umnt_proc,    (xdrproc_t)xdr_nfs_dirpath,      sizeof(nfs_dirpath),      NULL,                         0,                         this},
        {MOUNT_PROGRAM, MOUNT_V3, MOUNT3_UMNTALL, mount3_umntall_proc, NULL,                            0,                        NULL,                         0,                         this},
        {MOUNT_PROGRAM, MOUNT_V3, MOUNT3_EXPORT,  mount3_export_proc,  NULL,                            0,                        (xdrproc_t)xdr_nfs_exports,   sizeof(nfs_exports),       this},
    };
    for (int i = 0; i < sizeof(pt)/sizeof(pt[0]); i++)
    {
        proc_table.insert(pt[i]);
    }
}

nfs_client_t::~nfs_client_t()
{
}
