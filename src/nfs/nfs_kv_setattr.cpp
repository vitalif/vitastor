// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// NFS proxy over VitastorKV database - SETATTR

#include <sys/time.h>

#include "nfs_proxy.h"
#include "nfs_kv.h"
#include "cli.h"

struct nfs_kv_setattr_state
{
    nfs_client_t *self = NULL;
    rpc_op_t *rop = NULL;
    uint64_t ino = 0;
    uint64_t old_size = 0, new_size = 0;
    std::string expected_ctime;
    json11::Json::object set_attrs;
    int res = 0, cas_res = 0;
    std::string ientry_text;
    json11::Json ientry;
    json11::Json::object new_attrs;
    std::function<void(int)> cb;
};

static void nfs_kv_continue_setattr(nfs_kv_setattr_state *st, int state)
{
    // FIXME: NFS client does a lot of setattr calls, so maybe process them asynchronously
    if (state == 0)      {}
    else if (state == 1) goto resume_1;
    else if (state == 2) goto resume_2;
    else if (state == 3) goto resume_3;
    else
    {
        fprintf(stderr, "BUG: invalid state in nfs_kv_continue_setattr()");
        abort();
    }
    st->self->parent->kvfs->touch_queue.erase(st->ino);
resume_0:
    kv_read_inode(st->self->parent, st->ino, [st](int res, const std::string & value, json11::Json attrs)
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
    if (st->self->parent->enforce_perms && !kv_is_accessible(st->rop->auth_sys, st->ientry, ACCESS3_MODIFY))
    {
        auto cb = std::move(st->cb);
        cb(-EACCES);
        return;
    }
    if (st->ientry["type"].string_value() != "file" &&
        st->ientry["type"].string_value() != "" &&
        !st->set_attrs["size"].is_null())
    {
        auto cb = std::move(st->cb);
        cb(-EINVAL);
        return;
    }
    if (st->expected_ctime != "")
    {
        auto actual_ctime = (st->ientry["ctime"].is_null() ? st->ientry["mtime"] : st->ientry["ctime"]);
        if (actual_ctime != st->expected_ctime)
        {
            auto cb = std::move(st->cb);
            cb(NFS3ERR_NOT_SYNC);
            return;
        }
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
    st->new_attrs.erase("verf");
    st->new_attrs["ctime"] = nfstime_now_str();
    st->self->parent->kvfs->write_inode(st->ino, st->new_attrs, false, [st](int res)
    {
        st->res = res;
        nfs_kv_continue_setattr(st, 2);
    }, [st](int res, const std::string & cas_value)
    {
        st->cas_res = res;
        return res == 0 && cas_value == st->ientry_text;
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
        st->ientry["size"].uint64_value() > st->set_attrs["size"].uint64_value() &&
        !st->ientry["shared_ino"].uint64_value())
    {
        // Delete extra data when downsizing
        st->self->parent->cmd->loop_and_wait(st->self->parent->cmd->start_rm_data(json11::Json::object {
            { "inode", INODE_NO_POOL(st->ino) },
            { "pool", (uint64_t)INODE_POOL(st->ino) },
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
    cb(st->res);
}

int kv_nfs3_setattr_proc(void *opaque, rpc_op_t *rop)
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
    if (args->guard.check)
        st->expected_ctime = nfstime_to_str(args->guard.obj_ctime);
    if (args->new_attributes.size.set_it)
        st->set_attrs["size"] = args->new_attributes.size.size;
    if (args->new_attributes.mode.set_it)
        st->set_attrs["mode"] = (uint64_t)args->new_attributes.mode.mode;
    if (args->new_attributes.uid.set_it)
        st->set_attrs["uid"] = (uint64_t)args->new_attributes.uid.uid;
    if (args->new_attributes.gid.set_it)
        st->set_attrs["gid"] = (uint64_t)args->new_attributes.gid.gid;
    if (args->new_attributes.atime.set_it == SET_TO_SERVER_TIME)
        st->set_attrs["atime"] = nfstime_now_str();
    else if (args->new_attributes.atime.set_it == SET_TO_CLIENT_TIME)
        st->set_attrs["atime"] = nfstime_to_str(args->new_attributes.atime.atime);
    if (args->new_attributes.mtime.set_it == SET_TO_SERVER_TIME)
        st->set_attrs["mtime"] = nfstime_now_str();
    else if (args->new_attributes.mtime.set_it == SET_TO_CLIENT_TIME)
        st->set_attrs["mtime"] = nfstime_to_str(args->new_attributes.mtime.mtime);
    if (st->self->parent->trace)
        fprintf(stderr, "[%d] SETATTR %ju ATTRS %s\n", st->self->nfs_fd, st->ino, json11::Json(st->set_attrs).dump().c_str());
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
                            .attributes = get_kv_attributes(st->self->parent, st->ino, st->new_attrs),
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
