// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// NFS proxy over VitastorKV database - RENAME

#include <sys/time.h>

#include "nfs_proxy.h"
#include "nfs_kv.h"
#include "cli.h"

struct nfs_kv_rename_state
{
    nfs_client_t *self = NULL;
    rpc_op_t *rop = NULL;
    // params:
    uint64_t old_dir_ino = 0, new_dir_ino = 0;
    std::string old_name, new_name;
    // state:
    bool allow_cache = true;
    std::string old_direntry_text, old_ientry_text, new_direntry_text, new_ientry_text;
    json11::Json old_direntry, old_ientry, new_direntry, new_ientry;
    std::string new_dir_prefix;
    void *list_handle = NULL;
    bool new_exists = false;
    bool rm_dest_data = false;
    int res = 0, res2 = 0;
    std::function<void(int)> cb;
};

static void nfs_kv_continue_rename(nfs_kv_rename_state *st, int state)
{
    // Algorithm (non-atomic of course):
    // 1) Read source direntry
    // 2) Read destination direntry
    // 3) If destination exists:
    // 3.1) Check file/folder compatibility (EISDIR/ENOTDIR)
    // 3.2) Check if destination is empty if it's a folder
    // 4) If not:
    // 4.1) Check that the destination directory is actually a directory
    // 5) Overwrite destination direntry, restart from beginning if CAS failure
    // 6) Delete source direntry, restart from beginning if CAS failure
    // 7) If the moved direntry was a regular file:
    // 7.1) Read inode
    // 7.2) Delete inode if its link count <= 1
    // 7.3) Delete inode data if its link count <= 1 and it's a regular non-shared file
    // 7.4) Reduce link count by 1 if it's > 1
    // 8) If the moved direntry is a directory:
    // 8.1) Change parent_ino reference in its inode
    if (state == 0)      {}
    else if (state == 1) goto resume_1;
    else if (state == 2) goto resume_2;
    else if (state == 3) goto resume_3;
    else if (state == 4) goto resume_4;
    else if (state == 5) goto resume_5;
    else if (state == 6) goto resume_6;
    else if (state == 7) goto resume_7;
    else if (state == 8) goto resume_8;
    else if (state == 9) goto resume_9;
    else if (state == 10) goto resume_10;
    else if (state == 11) goto resume_11;
    else if (state == 12) goto resume_12;
    else
    {
        fprintf(stderr, "BUG: invalid state in nfs_kv_continue_rename()");
        abort();
    }
resume_0:
    // Read the old direntry
    st->self->parent->db->get(kv_direntry_key(st->old_dir_ino, st->old_name), [=](int res, const std::string & value)
    {
        st->res = res;
        st->old_direntry_text = value;
        nfs_kv_continue_rename(st, 1);
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
        st->old_direntry = json11::Json::parse(st->old_direntry_text, err);
        if (err != "")
        {
            fprintf(stderr, "Invalid JSON in direntry %s = %s: %s\n",
                kv_direntry_key(st->old_dir_ino, st->old_name).c_str(),
                st->old_direntry_text.c_str(), err.c_str());
            auto cb = std::move(st->cb);
            cb(-EIO);
            return;
        }
    }
    // Read the new direntry
    st->self->parent->db->get(kv_direntry_key(st->new_dir_ino, st->new_name), [=](int res, const std::string & value)
    {
        st->res = res;
        st->new_direntry_text = value;
        nfs_kv_continue_rename(st, 2);
    }, st->allow_cache);
    return;
resume_2:
    if (st->res < 0 && st->res != -ENOENT)
    {
        auto cb = std::move(st->cb);
        cb(st->res);
        return;
    }
    if (st->res == 0)
    {
        std::string err;
        st->new_direntry = json11::Json::parse(st->new_direntry_text, err);
        if (err != "")
        {
            fprintf(stderr, "Invalid JSON in direntry %s = %s: %s\n",
                kv_direntry_key(st->new_dir_ino, st->new_name).c_str(),
                st->new_direntry_text.c_str(), err.c_str());
            auto cb = std::move(st->cb);
            cb(-EIO);
            return;
        }
    }
    st->new_exists = st->res == 0;
    if (st->new_exists)
    {
        // Check file/folder compatibility (EISDIR/ENOTDIR)
        if ((st->old_direntry["type"] == "dir") != (st->new_direntry["type"] == "dir"))
        {
            auto cb = std::move(st->cb);
            cb((st->new_direntry["type"] == "dir") ? -ENOTDIR : -EISDIR);
            return;
        }
        if (st->new_direntry["type"] == "dir")
        {
            // Check that the destination directory is empty
            st->new_dir_prefix = kv_direntry_key(st->new_direntry["ino"].uint64_value(), "");
            st->list_handle = st->self->parent->db->list_start(st->new_dir_prefix);
            st->self->parent->db->list_next(st->list_handle, [st](int res, const std::string & key, const std::string & value)
            {
                st->res = res;
                nfs_kv_continue_rename(st, 3);
            });
            return;
resume_3:
            st->self->parent->db->list_close(st->list_handle);
            if (st->res != -ENOENT)
            {
                auto cb = std::move(st->cb);
                cb(-ENOTEMPTY);
                return;
            }
        }
    }
    else
    {
        // Check that the new directory is actually a directory
        kv_read_inode(st->self->parent, st->new_dir_ino, [st](int res, const std::string & value, json11::Json attrs)
        {
            st->res = res == 0 ? (attrs["type"].string_value() == "dir" ? 0 : -ENOTDIR) : res;
            nfs_kv_continue_rename(st, 4);
        });
        return;
resume_4:
        if (st->res < 0)
        {
            auto cb = std::move(st->cb);
            cb(st->res);
            return;
        }
    }
    // Write the new direntry
    st->self->parent->db->set(kv_direntry_key(st->new_dir_ino, st->new_name), st->old_direntry_text, [st](int res)
    {
        st->res = res;
        nfs_kv_continue_rename(st, 5);
    }, [st](int res, const std::string & old_value)
    {
        return st->new_exists ? (old_value == st->new_direntry_text) : (res == -ENOENT);
    });
    return;
resume_5:
    if (st->res == -EAGAIN)
    {
        // CAS failure
        st->allow_cache = false;
        goto resume_0;
    }
    if (st->res < 0)
    {
        auto cb = std::move(st->cb);
        cb(st->res);
        return;
    }
    // Delete the old direntry
    st->self->parent->db->del(kv_direntry_key(st->old_dir_ino, st->old_name), [st](int res)
    {
        st->res = res;
        nfs_kv_continue_rename(st, 6);
    }, [=](int res, const std::string & old_value)
    {
        return res == 0 && old_value == st->old_direntry_text;
    });
    return;
resume_6:
    if (st->res == -EAGAIN)
    {
        // CAS failure
        st->allow_cache = false;
        goto resume_0;
    }
    if (st->res < 0)
    {
        auto cb = std::move(st->cb);
        cb(st->res);
        return;
    }
    st->allow_cache = true;
resume_7again:
    if (st->new_exists && st->new_direntry["type"].string_value() != "dir")
    {
        // (Maybe) delete old destination file data
        kv_read_inode(st->self->parent, st->new_direntry["ino"].uint64_value(), [st](int res, const std::string & value, json11::Json attrs)
        {
            st->res = res;
            st->new_ientry_text = value;
            st->new_ientry = attrs;
            nfs_kv_continue_rename(st, 7);
        }, st->allow_cache);
        return;
resume_7:
        if (st->res == 0)
        {
            // (5) Reduce inode refcount by 1 or delete inode
            if (st->new_ientry["nlink"].uint64_value() > 1)
            {
                auto copy = st->new_ientry.object_items();
                copy["nlink"] = st->new_ientry["nlink"].uint64_value()-1;
                copy["ctime"] = nfstime_now_str();
                copy.erase("verf");
                st->self->parent->db->set(kv_inode_key(st->new_direntry["ino"].uint64_value()), json11::Json(copy).dump(), [st](int res)
                {
                    st->res = res;
                    nfs_kv_continue_rename(st, 8);
                }, [st](int res, const std::string & old_value)
                {
                    return old_value == st->new_ientry_text;
                });
            }
            else
            {
                st->rm_dest_data = kv_map_type(st->new_ientry["type"].string_value()) == NF3REG
                    && !st->new_ientry["shared_ino"].uint64_value();
                st->self->parent->db->del(kv_inode_key(st->new_direntry["ino"].uint64_value()), [st](int res)
                {
                    st->res = res;
                    nfs_kv_continue_rename(st, 8);
                }, [st](int res, const std::string & old_value)
                {
                    return old_value == st->new_ientry_text;
                });
            }
            return;
resume_8:
            if (st->res == -EAGAIN)
            {
                // CAS failure - re-read inode
                st->allow_cache = false;
                goto resume_7again;
            }
            if (st->res < 0)
            {
                auto cb = std::move(st->cb);
                cb(st->res);
                return;
            }
            // Delete inode data if required
            if (st->rm_dest_data)
            {
                st->self->parent->cmd->loop_and_wait(st->self->parent->cmd->start_rm_data(json11::Json::object {
                    { "inode", INODE_NO_POOL(st->self->parent->kvfs->fs_base_inode + st->new_direntry["ino"].uint64_value()) },
                    { "pool", (uint64_t)INODE_POOL(st->self->parent->kvfs->fs_base_inode + st->new_direntry["ino"].uint64_value()) },
                }), [st](const cli_result_t & r)
                {
                    if (r.err)
                    {
                        fprintf(stderr, "Failed to remove inode %jx data: %s (code %d)\n",
                            st->new_direntry["ino"].uint64_value(), r.text.c_str(), r.err);
                    }
                    st->res = r.err;
                    nfs_kv_continue_rename(st, 9);
                });
                return;
resume_9:
                if (st->res < 0)
                {
                    auto cb = std::move(st->cb);
                    cb(st->res);
                    return;
                }
            }
        }
    }
    if (st->old_direntry["type"].string_value() == "dir" && st->new_dir_ino != st->old_dir_ino)
    {
        // Change parent_ino in old ientry
        st->allow_cache = true;
resume_10:
        kv_read_inode(st->self->parent, st->old_direntry["ino"].uint64_value(), [st](int res, const std::string & value, json11::Json ientry)
        {
            st->res = res;
            st->old_ientry_text = value;
            st->old_ientry = ientry;
            nfs_kv_continue_rename(st, 11);
        }, st->allow_cache);
        return;
resume_11:
        if (st->res < 0)
        {
            auto cb = std::move(st->cb);
            cb(st->res);
            return;
        }
        {
            auto ientry_new = st->old_ientry.object_items();
            ientry_new["parent_ino"] = st->new_dir_ino;
            ientry_new["ctime"] = nfstime_now_str();
            ientry_new.erase("verf");
            st->self->parent->db->set(kv_inode_key(st->old_direntry["ino"].uint64_value()), json11::Json(ientry_new).dump(), [st](int res)
            {
                st->res = res;
                nfs_kv_continue_rename(st, 12);
            }, [st](int res, const std::string & old_value)
            {
                return old_value == st->old_ientry_text;
            });
        }
        return;
resume_12:
        if (st->res == -EAGAIN)
        {
            // CAS failure - try again
            st->allow_cache = false;
            goto resume_10;
        }
        if (st->res < 0)
        {
            auto cb = std::move(st->cb);
            cb(st->res);
            return;
        }
    }
    if (!st->res)
    {
        st->self->parent->kvfs->touch_queue.insert(st->old_dir_ino);
        st->self->parent->kvfs->touch_queue.insert(st->new_dir_ino);
    }
    auto cb = std::move(st->cb);
    cb(st->res);
}

int kv_nfs3_rename_proc(void *opaque, rpc_op_t *rop)
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
