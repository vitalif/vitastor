// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// NFS proxy over VitastorKV database - READDIR, READDIRPLUS

#include <sys/time.h>

#include "nfs_proxy.h"
#include "nfs_kv.h"

static unsigned len_pad4(unsigned len)
{
    return len + (len&3 ? 4-(len&3) : 0);
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
    while (st->is_plus && st->getattr_cur < st->entries.size() && st->getattr_running < st->self->parent->kvfs->readdir_getattr_parallel)
    {
        auto idx = st->getattr_cur++;
        st->getattr_running++;
        kv_read_inode(st->self->parent, st->entries[idx].fileid, [st, idx](int res, const std::string & value, json11::Json ientry)
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
        kv_read_inode(st->self->parent, st->dir_ino, [st](int res, const std::string & value, json11::Json ientry)
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
            kv_read_inode(st->self->parent, st->ientry["parent_ino"].uint64_value(), [st](int res, const std::string & value, json11::Json ientry)
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
        auto lc_it = st->self->parent->kvfs->list_cookies.find((list_cookie_t){ st->dir_ino, st->cookieverf, st->cookie });
        if (lc_it != st->self->parent->kvfs->list_cookies.end())
        {
            st->start = st->prefix+lc_it->second.key;
            st->to_skip = 1;
            st->offset = st->cookie+1;
        }
        else
        {
            st->to_skip = st->cookie-2;
            st->offset = st->cookie;
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
        auto lc_it = st->self->parent->kvfs->list_cookies.lower_bound((list_cookie_t){ st->dir_ino, st->cookieverf, 0 });
        if (lc_it != st->self->parent->kvfs->list_cookies.end() &&
            lc_it->first.dir_ino == st->dir_ino &&
            lc_it->first.cookieverf == st->cookieverf &&
            lc_it->first.cookie < st->cookie)
        {
            auto lc_start = lc_it;
            while (lc_it != st->self->parent->kvfs->list_cookies.end() && lc_it->first.cookieverf == st->cookieverf)
            {
                lc_it++;
            }
            st->self->parent->kvfs->list_cookies.erase(lc_start, lc_it);
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
        if (st->res == -ENOENT || st->cur_key.size() < st->prefix.size() || st->cur_key.substr(0, st->prefix.size()) != st->prefix)
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
        if (st->self->parent->trace)
        {
            fprintf(stderr, "[%d] READDIR %ju %ju %s\n",
                st->self->nfs_fd, st->dir_ino, st->offset, name.c_str());
        }
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
        st->self->parent->kvfs->list_cookies[(list_cookie_t){ st->dir_ino, st->cookieverf, entry->cookie }] = { .key = name };
        if (st->is_plus)
        {
            entry->name_handle = (post_op_fh3){
                .handle_follows = 1,
                .handle = xdr_copy_string(st->rop->xdrs, fh),
            };
            kv_getattr_next(st);
        }
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

int kv_nfs3_readdir_proc(void *opaque, rpc_op_t *rop)
{
    nfs3_readdir_common(opaque, rop, false);
    return 0;
}

int kv_nfs3_readdirplus_proc(void *opaque, rpc_op_t *rop)
{
    nfs3_readdir_common(opaque, rop, true);
    return 0;
}
