// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// NFS proxy over VitastorKV database - RENAME

#include <sys/time.h>

#include "str_util.h"

#include "nfs_proxy.h"

#include "nfs/nfs.h"

#include "cli.h"

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
