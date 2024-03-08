// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// NFS proxy over VitastorKV database - REMOVE, RMDIR

#include <sys/time.h>

#include "nfs_proxy.h"
#include "nfs_kv.h"
#include "cli.h"

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
    bool allow_cache = true;
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
resume_0:
    st->self->parent->db->get(kv_direntry_key(st->dir_ino, st->filename), [st](int res, const std::string & value)
    {
        st->res = res;
        st->direntry_text = value;
        nfs_kv_continue_delete(st, 1);
    }, st->allow_cache);
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
    }, st->allow_cache);
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
        st->allow_cache = false;
        goto resume_0;
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
    if ((!st->type || st->type == NF3REG) && st->ientry["nlink"].uint64_value() <= 1 &&
        !st->ientry["shared_ino"].uint64_value())
    {
        // Remove data
        st->self->parent->cmd->loop_and_wait(st->self->parent->cmd->start_rm_data(json11::Json::object {
            { "inode", INODE_NO_POOL(st->self->parent->kvfs->fs_base_inode + st->ino) },
            { "pool", (uint64_t)INODE_POOL(st->self->parent->kvfs->fs_base_inode + st->ino) },
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

int kv_nfs3_remove_proc(void *opaque, rpc_op_t *rop)
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

int kv_nfs3_rmdir_proc(void *opaque, rpc_op_t *rop)
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
