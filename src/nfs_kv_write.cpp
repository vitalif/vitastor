// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// NFS proxy over VitastorKV database - WRITE

#include <sys/time.h>

#include "nfs_proxy.h"
#include "nfs_kv.h"

struct nfs_rmw_t
{
    nfs_kv_write_state *st = NULL;
    int continue_state = 0;
    uint64_t ino = 0;
    uint64_t offset = 0;
    uint8_t *buf = NULL;
    uint64_t size = 0;
    uint8_t *part_buf = NULL;
};

struct nfs_kv_write_state
{
    nfs_client_t *self = NULL;
    rpc_op_t *rop = NULL;
    uint64_t ino = 0;
    uint64_t offset = 0, size = 0;
    bool stable = false;
    uint8_t *buf = NULL;
    std::function<void(int res)> cb;
    // state
    bool allow_cache = true;
    int res = 0, res2 = 0;
    int waiting = 0;
    std::string ientry_text;
    json11::Json ientry;
    uint64_t new_size = 0;
    uint64_t aligned_size = 0;
    uint8_t *aligned_buf = NULL;
    uint64_t shared_inode = 0, shared_offset = 0;
    bool was_immediate = false;
    nfs_rmw_t rmw[2];
    kv_inode_extend_t *ext = NULL;

    ~nfs_kv_write_state()
    {
        if (aligned_buf)
        {
            free(aligned_buf);
            aligned_buf = NULL;
        }
    }
};

static void nfs_kv_continue_write(nfs_kv_write_state *st, int state);

static void finish_allocate_shared(nfs_client_t *self, int res)
{
    std::vector<shared_alloc_queue_t> waiting;
    waiting.swap(self->parent->kvfs->allocating_shared);
    for (auto & w: waiting)
    {
        w.st->res = res;
        if (res == 0)
        {
            w.st->shared_inode = self->parent->kvfs->cur_shared_inode;
            w.st->shared_offset = self->parent->kvfs->cur_shared_offset;
            self->parent->kvfs->cur_shared_offset += (w.size + self->parent->pool_alignment-1) & ~(self->parent->pool_alignment-1);
        }
        nfs_kv_continue_write(w.st, w.state);
    }
}

static void allocate_shared_inode(nfs_kv_write_state *st, int state, uint64_t size)
{
    if (st->self->parent->kvfs->cur_shared_inode == 0)
    {
        st->self->parent->kvfs->allocating_shared.push_back({ st, state, size });
        if (st->self->parent->kvfs->allocating_shared.size() > 1)
        {
            return;
        }
        allocate_new_id(st->self, [st](int res, uint64_t new_id)
        {
            if (res < 0)
            {
                finish_allocate_shared(st->self, res);
                return;
            }
            st->self->parent->kvfs->cur_shared_inode = new_id;
            st->self->parent->kvfs->cur_shared_offset = 0;
            st->self->parent->db->set(
                kv_inode_key(new_id), json11::Json(json11::Json::object{ { "type", "shared" } }).dump(),
                [st](int res)
                {
                    if (res < 0)
                    {
                        st->self->parent->kvfs->cur_shared_inode = 0;
                    }
                    finish_allocate_shared(st->self, res);
                },
                [](int res, const std::string & old_value)
                {
                    return res == -ENOENT;
                }
            );
        });
    }
}

uint64_t align_shared_size(nfs_client_t *self, uint64_t size)
{
    return (size + sizeof(shared_file_header_t) + self->parent->pool_alignment-1)
        & ~(self->parent->pool_alignment-1);
}

static void nfs_do_write(uint64_t ino, uint64_t offset, uint8_t *buf, uint64_t size, nfs_kv_write_state *st, int state)
{
    auto op = new cluster_op_t;
    op->opcode = OSD_OP_WRITE;
    op->inode = st->self->parent->fs_base_inode + ino;
    op->offset = offset;
    op->len = size;
    op->iov.push_back(buf, size);
    st->waiting++;
    op->callback = [st, state](cluster_op_t *op)
    {
        if (op->retval != op->len)
        {
            st->res = op->retval >= 0 ? -EIO : op->retval;
        }
        delete op;
        st->waiting--;
        if (!st->waiting)
        {
            nfs_kv_continue_write(st, state);
        }
    };
    st->self->parent->cli->execute(op);
}

static void nfs_do_shared_write(nfs_kv_write_state *st, int state)
{
    nfs_do_write(st->shared_inode, st->shared_offset, st->aligned_buf, st->aligned_size, st, state);
}

static void nfs_do_unshare_write(nfs_kv_write_state *st, int state)
{
    nfs_do_write(st->ino, 0, st->aligned_buf + sizeof(shared_file_header_t),
        st->aligned_size - sizeof(shared_file_header_t), st, state);
}

static void nfs_do_rmw(nfs_rmw_t *rmw)
{
    auto parent = rmw->st->self->parent;
    auto align = parent->pool_alignment;
    bool is_begin = (rmw->offset % align);
    bool is_end = ((rmw->offset+rmw->size) % align);
    // RMW either only at beginning or only at end and within a single block
    assert(is_begin != is_end);
    assert((rmw->offset/parent->pool_block_size) == ((rmw->offset+rmw->size-1)/parent->pool_block_size));
    if (!rmw->part_buf)
    {
        rmw->part_buf = (uint8_t*)malloc_or_die(align);
    }
    auto op = new cluster_op_t;
    op->opcode = OSD_OP_READ;
    op->inode = parent->fs_base_inode + rmw->ino;
    op->offset = (rmw->offset + (is_begin ? 0 : rmw->size)) & ~(align-1);
    op->len = align;
    op->iov.push_back(rmw->part_buf, op->len);
    rmw->st->waiting++;
    op->callback = [rmw](cluster_op_t *rd_op)
    {
        if (rd_op->retval != rd_op->len)
        {
            free(rmw->part_buf);
            rmw->part_buf = NULL;
            rmw->st->res = rd_op->retval >= 0 ? -EIO : rd_op->retval;
            rmw->st->waiting--;
            if (!rmw->st->waiting)
            {
                nfs_kv_continue_write(rmw->st, rmw->continue_state);
            }
        }
        else
        {
            auto parent = rmw->st->self->parent;
            auto align = parent->pool_alignment;
            bool is_begin = (rmw->offset % align);
            auto op = new cluster_op_t;
            op->opcode = OSD_OP_WRITE;
            op->inode = rmw->st->self->parent->fs_base_inode + rmw->ino;
            op->offset = rmw->offset & ~(align-1);
            op->len = (rmw->size + align-1) & ~(align-1);
            op->version = rd_op->version+1;
            if (is_begin)
            {
                op->iov.push_back(rmw->part_buf, rmw->offset % align);
            }
            op->iov.push_back(rmw->buf, rmw->size);
            if (!is_begin)
            {
                auto tail = ((rmw->offset+rmw->size) % align);
                op->iov.push_back(rmw->part_buf + tail, align - tail);
            }
            op->callback = [rmw](cluster_op_t *op)
            {
                if (op->retval == -EAGAIN)
                {
                    // CAS failure - retry
                    rmw->st->waiting--;
                    nfs_do_rmw(rmw);
                }
                else
                {
                    free(rmw->part_buf);
                    rmw->part_buf = NULL;
                    if (op->retval != op->len)
                    {
                        rmw->st->res = (op->retval >= 0 ? -EIO : op->retval);
                    }
                    rmw->st->waiting--;
                    if (!rmw->st->waiting)
                    {
                        nfs_kv_continue_write(rmw->st, rmw->continue_state);
                    }
                }
                delete op;
            };
            parent->cli->execute(op);
        }
        delete rd_op;
    };
    parent->cli->execute(op);
}

static void nfs_do_shared_read(nfs_kv_write_state *st, int state)
{
    auto op = new cluster_op_t;
    op->opcode = OSD_OP_READ;
    op->inode = st->self->parent->fs_base_inode + st->ientry["shared_ino"].uint64_value();
    op->offset = st->ientry["shared_offset"].uint64_value();
    op->len = align_shared_size(st->self, st->ientry["size"].uint64_value());
    op->iov.push_back(st->aligned_buf, op->len);
    op->callback = [st, state](cluster_op_t *op)
    {
        st->res = op->retval == op->len ? 0 : op->retval;
        delete op;
        nfs_kv_continue_write(st, state);
    };
    st->self->parent->cli->execute(op);
}

static void nfs_do_fsync(nfs_kv_write_state *st, int state)
{
    // Client requested a stable write. Add an fsync
    auto op = new cluster_op_t;
    op->opcode = OSD_OP_SYNC;
    op->callback = [st, state](cluster_op_t *op)
    {
        delete op;
        nfs_kv_continue_write(st, state);
    };
    st->self->parent->cli->execute(op);
}

static bool nfs_do_shared_readmodify(nfs_kv_write_state *st, int base_state, int state, bool unshare)
{
    assert(state <= base_state);
    if (state < base_state)       {}
    else if (state == base_state) goto resume_0;
    assert(!st->aligned_buf);
    st->aligned_size = unshare
        ? sizeof(shared_file_header_t) + (st->new_size + st->self->parent->pool_alignment-1) & ~(st->self->parent->pool_alignment-1)
        : align_shared_size(st->self, st->new_size);
    st->aligned_buf = (uint8_t*)malloc_or_die(st->aligned_size);
    memset(st->aligned_buf + sizeof(shared_file_header_t), 0, st->offset);
    if (st->ientry["shared_ino"].uint64_value() != 0)
    {
        // Read old data if shared non-empty
        nfs_do_shared_read(st, base_state);
        return false;
resume_0:
        if (st->res < 0)
        {
            auto cb = std::move(st->cb);
            cb(st->res);
            return true;
        }
        auto hdr = ((shared_file_header_t*)st->aligned_buf);
        if (hdr->magic != SHARED_FILE_MAGIC_V1 || hdr->inode != st->ino ||
            align_shared_size(st->self, hdr->size) > align_shared_size(st->self, st->ientry["size"].uint64_value()))
        {
            // Got unrelated data - retry from the beginning
            st->allow_cache = false;
            nfs_kv_continue_write(st, 0);
            return false;
        }
    }
    *((shared_file_header_t*)st->aligned_buf) = {
        .magic = SHARED_FILE_MAGIC_V1,
        .inode = st->ino,
        .size = st->new_size,
    };
    memcpy(st->aligned_buf + sizeof(shared_file_header_t) + st->offset, st->buf, st->size);
    memset(st->aligned_buf + sizeof(shared_file_header_t) + st->offset + st->size, 0,
        st->aligned_size - sizeof(shared_file_header_t) - st->offset - st->size);
    return true;
}

static void nfs_do_align_write(nfs_kv_write_state *st, uint64_t ino, uint64_t offset, int state)
{
    auto alignment = st->self->parent->pool_alignment;
    uint8_t *good_buf = st->buf;
    uint64_t good_offset = offset;
    uint64_t good_size = st->size;
    st->waiting++;
    if (offset % alignment)
    {
        // Requires read-modify-write in the beginning
        auto s = (alignment - (offset % alignment));
        if (good_size > s)
        {
            good_buf += s;
            good_offset += s;
            good_size -= s;
        }
        else
            good_size = 0;
        s = s > st->size ? st->size : s;
        st->rmw[0] = {
            .st = st,
            .continue_state = state,
            .ino = ino,
            .offset = offset,
            .buf = st->buf,
            .size = s,
        };
        nfs_do_rmw(&st->rmw[0]);
    }
    if ((offset+st->size-1) % alignment)
    {
        // Requires read-modify-write in the end
        auto s = ((offset+st->size-1) % alignment);
        if (good_size > s)
            good_size -= s;
        else
            good_size = 0;
        if (((offset+st->size-1) / alignment) > (offset / alignment))
        {
            st->rmw[1] = {
                .st = st,
                .continue_state = state,
                .ino = ino,
                .offset = offset + st->size-s,
                .buf = st->buf + st->size-s,
                .size = s,
            };
            nfs_do_rmw(&st->rmw[1]);
        }
    }
    if (good_size > 0)
    {
        // Normal write
        nfs_do_write(ino, good_offset, good_buf, good_size, st, state);
    }
    st->waiting--;
    if (!st->waiting)
    {
        nfs_kv_continue_write(st, state);
    }
}

static std::string new_normal_ientry(nfs_kv_write_state *st)
{
    auto ni = st->ientry.object_items();
    ni.erase("empty");
    ni.erase("shared_ino");
    ni.erase("shared_offset");
    ni.erase("shared_alloc");
    ni.erase("shared_ver");
    ni["size"] = st->ext->cur_extend;
    return json11::Json(ni).dump();
}

static std::string new_moved_ientry(nfs_kv_write_state *st)
{
    auto ni = st->ientry.object_items();
    ni.erase("empty");
    ni["shared_ino"] = st->shared_inode;
    ni["shared_offset"] = st->shared_offset;
    ni["shared_alloc"] = st->aligned_size;
    ni.erase("shared_ver");
    ni["size"] = st->new_size;
    return json11::Json(ni).dump();
}

static std::string new_shared_ientry(nfs_kv_write_state *st)
{
    auto ni = st->ientry.object_items();
    ni.erase("empty");
    ni["size"] = st->new_size;
    ni["shared_ver"] = ni["shared_ver"].uint64_value()+1;
    return json11::Json(ni).dump();
}

static void nfs_kv_extend_inode(nfs_kv_write_state *st, int state)
{
    if (state == 1)
    {
        goto resume_1;
    }
    st->ext->cur_extend = st->ext->next_extend;
    st->ext->next_extend = 0;
    st->res2 = -EAGAIN;
    st->self->parent->db->set(kv_inode_key(st->ino), new_normal_ientry(st), [st](int res)
    {
        st->res = res;
        nfs_kv_continue_write(st, 13);
    }, [st](int res, const std::string & old_value)
    {
        if (res != 0)
        {
            return false;
        }
        if (old_value == st->ientry_text)
        {
            return true;
        }
        std::string err;
        auto ientry = json11::Json::parse(old_value, err).object_items();
        if (err != "")
        {
            st->res2 = -EINVAL;
            return false;
        }
        else if (ientry.size() == st->ientry.object_items().size())
        {
            for (auto & kv: st->ientry.object_items())
            {
                if (kv.first != "size" && ientry[kv.first] != kv.second)
                {
                    // Something except size changed
                    return false;
                }
            }
            // OK, only size changed
            if (ientry["size"] >= st->new_size)
            {
                // Already extended
                st->res2 = 0;
                return false;
            }
            // size is different but can still be extended, other parameters don't differ
            return true;
        }
        return false;
    });
    return;
resume_1:
    if (st->res == -EAGAIN)
    {
        // EAGAIN may be OK in fact (see above)
        st->res = st->res2;
    }
    if (st->res == 0)
    {
        st->ext->done_extend = st->ext->cur_extend;
    }
    st->ext->cur_extend = 0;
    // Wake up other extenders anyway
    auto waiters = std::move(st->ext->waiters);
    for (auto & cb: waiters)
    {
        cb();
    }
}

// Packing small files into "shared inodes". Insane algorithm...
// Write:
// - If (offset+size <= threshold):
//   - Read inode from cache
//   - If inode does not exist - stop with -ENOENT
//   - If inode is not a regular file - stop with -EINVAL
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
//         - Update inode entry in any case to block parallel non-shared writes
//           - If CAS failure: re-read inode and restart
// - Otherwise:
//   - Write data into non-shared inode
//   - Read inode in parallel
//     - If not a regular file:
//       - Remove data
//       - Stop with -EINVAL
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

static void nfs_kv_continue_write(nfs_kv_write_state *st, int state)
{
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
    else if (state == 13) goto resume_13;
    else
    {
        fprintf(stderr, "BUG: invalid state in nfs_kv_continue_write()");
        abort();
    }
resume_0:
    if (!st->size)
    {
        auto cb = std::move(st->cb);
        cb(0);
        return;
    }
    kv_read_inode(st->self, st->ino, [st](int res, const std::string & value, json11::Json attrs)
    {
        st->res = res;
        st->ientry_text = value;
        st->ientry = attrs;
        nfs_kv_continue_write(st, 1);
    }, st->allow_cache);
    return;
resume_1:
    if (st->res < 0 ||
        st->ientry["type"].uint64_value() != 0 &&
        st->ientry["type"].uint64_value() != NF3REG)
    {
        auto cb = std::move(st->cb);
        cb(st->res == 0 ? -EINVAL : st->res);
        return;
    }
    st->was_immediate = st->self->parent->cli->get_immediate_commit(st->self->parent->fs_base_inode + st->ino);
    st->new_size = st->ientry["size"].uint64_value();
    if (st->new_size < st->offset + st->size)
    {
        st->new_size = st->offset + st->size;
    }
    if (st->offset + st->size + sizeof(shared_file_header_t) < st->self->parent->shared_inode_threshold)
    {
        if (st->ientry["size"].uint64_value() == 0 ||
            st->ientry["empty"].bool_value() &&
            st->ientry["size"].uint64_value() + sizeof(shared_file_header_t) < st->self->parent->shared_inode_threshold ||
            st->ientry["shared_ino"].uint64_value() != 0 &&
            st->ientry["size"].uint64_value() < st->offset+st->size &&
            st->ientry["shared_alloc"].uint64_value() < align_shared_size(st->self, st->offset+st->size))
        {
            // Either empty, or shared and requires moving into a larger place (redirect-write)
            allocate_shared_inode(st, 2, st->new_size);
            return;
resume_2:
            if (st->res < 0)
            {
                auto cb = std::move(st->cb);
                cb(st->res);
                return;
            }
resume_3:
            if (!nfs_do_shared_readmodify(st, 3, state, false))
                return;
            nfs_do_shared_write(st, 4); // FIXME assemble from parts, do not copy?
            return;
resume_4:
            if (st->res < 0)
            {
                auto cb = std::move(st->cb);
                cb(st->res);
                return;
            }
            st->self->parent->db->set(kv_inode_key(st->ino), new_moved_ientry(st), [st](int res)
            {
                st->res = res;
                nfs_kv_continue_write(st, 5);
            }, [st](int res, const std::string & old_value)
            {
                return res == 0 && old_value == st->ientry_text;
            });
            return;
resume_5:
            if (st->res < 0)
            {
                st->res2 = st->res;
                memset(st->aligned_buf, 0, st->aligned_size);
                nfs_do_shared_write(st, 6);
                return;
resume_6:
                free(st->aligned_buf);
                st->aligned_buf = NULL;
                if (st->res2 == -EAGAIN)
                {
                    goto resume_0;
                }
                else
                {
                    auto cb = std::move(st->cb);
                    cb(st->res2);
                    return;
                }
            }
            auto cb = std::move(st->cb);
            cb(0);
            return;
        }
        else if (st->ientry["shared_ino"].uint64_value() > 0)
        {
            // Non-empty, shared, can be updated in-place
            nfs_do_align_write(st, st->ientry["shared_ino"].uint64_value(),
                st->ientry["shared_offset"].uint64_value() + sizeof(shared_file_header_t) + st->offset, 7);
            return;
resume_7:
            if (st->res == 0 && st->stable && !st->was_immediate)
            {
                nfs_do_fsync(st, 8);
                return;
            }
            // We always have to change inode entry on shared writes
            st->self->parent->db->set(kv_inode_key(st->ino), new_shared_ientry(st), [st](int res)
            {
                st->res = res;
                nfs_kv_continue_write(st, 8);
            }, [st](int res, const std::string & old_value)
            {
                return res == 0 && old_value == st->ientry_text;
            });
            return;
resume_8:
            if (st->res == -EAGAIN)
            {
                goto resume_0;
            }
            auto cb = std::move(st->cb);
            cb(st->res);
            return;
        }
        // Fall through for non-shared
    }
    // Non-shared write
    if (st->ientry["shared_ino"].uint64_value() != 0)
    {
        // Unshare
resume_9:
        if (!nfs_do_shared_readmodify(st, 9, state, true))
            return;
        nfs_do_unshare_write(st, 10);
        return;
    }
    else
    {
        // Just write
        nfs_do_align_write(st, st->ino, st->offset, 10);
    }
resume_10:
    if (st->res == 0 && st->stable && !st->was_immediate)
    {
        nfs_do_fsync(st, 11);
        return;
    }
resume_11:
    if (st->res < 0)
    {
        auto cb = std::move(st->cb);
        cb(st->res);
        return;
    }
    if (st->ientry["empty"].bool_value() ||
        st->ientry["size"].uint64_value() < st->new_size ||
        st->ientry["shared_ino"].uint64_value() != 0)
    {
        st->ext = &st->self->parent->kvfs->extends[st->ino];
        st->ext->refcnt++;
resume_12:
        if (st->ext->next_extend < st->new_size)
        {
            // Aggregate inode extension requests
            st->ext->next_extend = st->new_size;
        }
        if (st->ext->cur_extend > 0)
        {
            // Wait for current extend which is already in progress
            st->ext->waiters.push_back([st](){ nfs_kv_continue_write(st, 12); });
            return;
        }
        if (st->ext->done_extend < st->new_size)
        {
            nfs_kv_extend_inode(st, 0);
            return;
resume_13:
            nfs_kv_extend_inode(st, 1);
        }
        st->ext->refcnt--;
        assert(st->ext->refcnt >= 0);
        if (st->ext->refcnt == 0)
        {
            st->self->parent->kvfs->extends.erase(st->ino);
        }
    }
    if (st->res == -EAGAIN)
    {
        // Restart
        goto resume_0;
    }
    auto cb = std::move(st->cb);
    cb(st->res);
}

int kv_nfs3_write_proc(void *opaque, rpc_op_t *rop)
{
    nfs_kv_write_state *st = new nfs_kv_write_state;
    st->self = (nfs_client_t*)opaque;
    st->rop = rop;
    WRITE3args *args = (WRITE3args*)rop->request;
    WRITE3res *reply = (WRITE3res*)rop->reply;
    st->ino = kv_fh_inode(args->file);
    st->offset = args->offset;
    st->size = (args->count > args->data.size ? args->data.size : args->count);
    if (!st->ino || st->size > MAX_REQUEST_SIZE)
    {
        *reply = (WRITE3res){ .status = NFS3ERR_INVAL };
        rpc_queue_reply(rop);
        delete st;
        return 0;
    }
    st->buf = (uint8_t*)args->data.data;
    st->stable = (args->stable != UNSTABLE);
    st->cb = [st](int res)
    {
        WRITE3res *reply = (WRITE3res*)st->rop->reply;
        *reply = (WRITE3res){ .status = vitastor_nfs_map_err(res) };
        if (res == 0)
        {
            reply->resok.count = (unsigned)st->size;
            reply->resok.committed = st->stable || st->was_immediate ? FILE_SYNC : UNSTABLE;
            *(uint64_t*)reply->resok.verf = st->self->parent->server_id;
        }
        rpc_queue_reply(st->rop);
        delete st;
    };
    nfs_kv_continue_write(st, 0);
    return 1;
}
