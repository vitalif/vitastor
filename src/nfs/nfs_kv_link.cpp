// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// NFS proxy over VitastorKV database - LINK

#include <sys/time.h>

#include "nfs_proxy.h"
#include "nfs_kv.h"

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
    kv_read_inode(st->self->parent, st->ino, [st](int res, const std::string & value, json11::Json attrs)
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
        kv_read_inode(st->self->parent, st->dir_ino, [st](int res, const std::string & value, json11::Json attrs)
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
        }, [](int res, const std::string & old_value)
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
        new_ientry["ctime"] = nfstime_now_str();
        st->ientry = new_ientry;
    }
    st->self->parent->kvfs->write_inode(st->ino, st->ientry, false, [st](int res)
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
    if (!st->res)
    {
        st->self->parent->kvfs->touch_queue.insert(st->dir_ino);
    }
    auto cb = std::move(st->cb);
    cb(st->res);
}

int kv_nfs3_link_proc(void *opaque, rpc_op_t *rop)
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
                        .attributes = get_kv_attributes(st->self->parent, st->ino, st->ientry),
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
