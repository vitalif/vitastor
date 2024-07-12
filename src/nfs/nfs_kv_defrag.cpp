// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// NFS proxy over VitastorKV database - defragmentation

#include <sys/time.h>

#include "nfs_proxy.h"
#include "nfs_common.h"
#include "nfs_kv.h"
#include "str_util.h"
#include "cli.h"

struct kv_fs_defrag_t
{
    nfs_proxy_t *proxy = NULL;
    inode_t shared_ino = 0;
    bool dry_run = false;
    bool no_rm = false;
    bool progress = true;
    uint64_t bitmap_granularity = 0;
    uint64_t buf_size = 0;
    uint8_t *block_buf = NULL;

    timespec prev_progress = {};
    int errcode = 0;
    bool reading = false;
    bool empty = false;
    uint64_t last_offset = 0;
    uint64_t real_size = 0;
    uint64_t buf_pos = 0;
    uint64_t iodepth = 0;
    uint64_t num_moved = 0, num_unused = 0;
    uint64_t bytes_moved = 0, bytes_unused = 0;
    uint64_t max_ctime = 0;
    bool handling = false;
    std::function<void(int, uint64_t, uint64_t, uint64_t)> cb;

    void read();
    void handle_read();
    void finish(int retval);
};

void kv_fs_defrag_t::finish(int retval)
{
    auto cb = std::move(this->cb);
    delete block_buf;
    block_buf = NULL;
    cb(retval, real_size, bytes_unused, max_ctime);
    delete this;
}

void kv_fs_defrag_t::read()
{
    if (errcode)
    {
        finish(errcode);
        return;
    }
    auto op = new cluster_op_t;
    op->opcode = OSD_OP_READ;
    op->inode = shared_ino;
    op->offset = last_offset;
    op->len = buf_size;
    op->iov.push_back(block_buf, buf_size);
    reading = true;
    op->callback = [this](cluster_op_t *op)
    {
        reading = false;
        if (op->retval != op->len)
        {
            fprintf(stderr, "Error reading 0x%jx bytes from volume 0x%jx at 0x%jx: %s (code %d)\n",
                op->len, shared_ino, op->offset, strerror(-op->retval), op->retval);
            finish(op->retval >= 0 ? -EIO : op->retval);
        }
        else
        {
            // Check that any data was actually read or it's the last iteration
            uint64_t bitmap_size = (op->len / bitmap_granularity + 7) / 8;
            uint64_t bitmap_pos = 0;
            empty = true;
            for (; bitmap_pos < bitmap_size; bitmap_pos += 8)
            {
                if (*((uint64_t*)(op->bitmap_buf + bitmap_pos)))
                    empty = false;
            }
            for (; bitmap_pos < bitmap_size; bitmap_pos++)
            {
                if (*((uint8_t*)(op->bitmap_buf + bitmap_pos)))
                    empty = false;
            }
            buf_pos = 0;
            handle_read();
        }
        delete op;
    };
    proxy->cli->execute(op);
}

void kv_fs_defrag_t::handle_read()
{
    if (handling)
    {
        return;
    }
    handling = true;
    while (!empty && !errcode && buf_pos < buf_size && iodepth < proxy->kvfs->defrag_iodepth)
    {
        // Next object header may be at any position after any number of zeroes
        // Commonly it's either in the beginning of a 4 KB sector or in the end of it
        if ((*(uint64_t*)(block_buf+buf_pos)) == SHARED_FILE_MAGIC_V1)
        {
            iodepth++;
            shared_file_header_t *hdr = (shared_file_header_t*)(block_buf+buf_pos);
            uint64_t shared_offset = last_offset + buf_pos;
            buf_pos += hdr->alloc;
            real_size = shared_offset + hdr->alloc;
            auto move_cb = [this, ino = hdr->inode, alloc = hdr->alloc, shared_offset](int res, bool was_moved)
            {
                if (res < 0 && res != -ENOENT)
                {
                    fprintf(stderr, "Error checking/moving inode 0x%jx from volume 0x%jx offset 0x%jx: %s (code %d)\n",
                        ino, shared_ino, shared_offset, strerror(-res), res);
                    errcode = res;
                }
                else
                {
                    if (was_moved)
                    {
                        bytes_moved += alloc;
                        num_moved++;
                    }
                    else
                    {
                        bytes_unused += alloc;
                        num_unused++;
                    }
                    if (proxy->trace)
                    {
                        fprintf(
                            stderr, was_moved
                                ? (dry_run
                                    ? "In use inode 0x%jx (%ju bytes) in volume 0x%jx at offset 0x%jx\n"
                                    : "Moved inode 0x%jx (%ju bytes) in volume 0x%jx at offset 0x%jx\n")
                                : "Unused inode 0x%jx (%ju bytes) in volume 0x%jx at offset 0x%jx\n",
                            ino, alloc, shared_ino, shared_offset
                        );
                    }
                    else if (progress)
                    {
                        timespec now;
                        clock_gettime(CLOCK_REALTIME, &now);
                        if (now.tv_sec >= prev_progress.tv_sec+2)
                        {
                            prev_progress = now;
                            fprintf(stderr, "Processed %s, %s %s, unused %s\n", format_size(real_size).c_str(),
                                dry_run ? "in use" : "moved", format_size(bytes_moved).c_str(), format_size(bytes_unused).c_str());
                        }
                    }
                }
                iodepth--;
                handle_read();
            };
            if (dry_run)
            {
                kv_read_inode(proxy, hdr->inode, [=](int res, const std::string & value, json11::Json attrs)
                {
                    uint64_t ctime = (uint64_t)attrs["ctime"].number_value();
                    if (max_ctime < ctime)
                        max_ctime = ctime;
                    move_cb(res, !res && attrs["shared_ino"] == shared_ino && attrs["shared_offset"] == shared_offset);
                });
            }
            else
            {
                nfs_move_inode_from(proxy, hdr->inode, shared_ino, shared_offset, move_cb);
            }
        }
        else
        {
            buf_pos += 8;
        }
    }
    handling = false;
    if (errcode)
    {
        if (iodepth)
        {
            // Wait for completion
            return;
        }
        finish(errcode);
    }
    else if (empty)
    {
        if (iodepth)
        {
            // Wait for completion
            return;
        }
        // Finish - now we can purge shared inode
        fprintf(
            stderr, dry_run
                ? "Estimated volume 0x%jx - in use %s (%ju files), unused %s (%ju files), last inode change time %s\n"
                : "Defragmented volume 0x%jx - moved %s (%ju files), unused %s (%ju files), last inode change time %s. Purging volume data\n",
            shared_ino, format_size(bytes_moved).c_str(), num_moved, format_size(bytes_unused).c_str(), num_unused, format_datetime(max_ctime).c_str()
        );
        if (dry_run || no_rm)
        {
            finish(0);
        }
        else
        {
            proxy->cmd->loop_and_wait(proxy->cmd->start_rm_data(json11::Json::object {
                { "inode", INODE_NO_POOL(shared_ino) },
                { "pool", (uint64_t)INODE_POOL(shared_ino) },
                { "progress", (uint64_t)proxy->trace }
            }), [this](const cli_result_t & r)
            {
                if (r.err)
                {
                    fprintf(stderr, "Failed to remove volume 0x%jx data: %s (code %d)\n",
                        shared_ino, r.text.c_str(), r.err);
                    finish(r.err);
                }
                else
                {
                    proxy->db->del(kv_inode_key(shared_ino), [=](int res)
                    {
                        if (res < 0)
                        {
                            fprintf(stderr, "Failed to remove volume key %s: %s (code %d)\n",
                                kv_inode_key(shared_ino).c_str(), strerror(-res), res);
                            finish(res);
                        }
                        else
                        {
                            proxy->db->del(kv_inode_prefix_key(shared_ino, "shared"), [=](int res)
                            {
                                if (res < 0)
                                {
                                    fprintf(stderr, "Failed to remove volume key %s: %s (code %d)\n",
                                        kv_inode_prefix_key(shared_ino, "shared").c_str(), strerror(-res), res);
                                }
                                finish(res);
                            });
                        }
                    });
                }
            });
        }
    }
    else if (!reading && buf_pos >= buf_size)
    {
        last_offset += buf_pos;
        read();
    }
}

// Linear read all object headers, check which of them are still alive, move them away
void kv_fs_state_t::defrag_volume(inode_t ino, bool no_rm, bool dry_run, std::function<void(int, uint64_t, uint64_t, uint64_t)> cb)
{
    auto pool_it = proxy->cli->st_cli.pool_config.find(INODE_POOL(ino));
    if (pool_it == proxy->cli->st_cli.pool_config.end())
    {
        fprintf(stderr, "Volume 0x%jx references a non-existing pool with ID %u, skipping\n", ino, INODE_POOL(ino));
        cb(0, 0, 0, 0);
        return;
    }
    auto st = new kv_fs_defrag_t;
    st->proxy = proxy;
    st->shared_ino = ino;
    st->dry_run = dry_run;
    st->no_rm = no_rm;
    st->buf_size = pool_it->second.pg_stripe_size * defrag_block_count;
    st->block_buf = (uint8_t*)malloc_or_die(st->buf_size);
    st->bitmap_granularity = pool_it->second.bitmap_granularity;
    st->cb = cb;
    clock_gettime(CLOCK_REALTIME, &st->prev_progress);
    st->read();
}

struct kv_fs_defrag_all_t
{
    std::function<void(int)> cb;
    nfs_proxy_t *proxy = NULL;
    bool dry_run = false;
    bool no_rm = false;
    bool recalc_stats = false;
    bool include_empty = false;

    timespec now = {};
    void *list_shared = NULL;
    uint64_t ino = 0;
    json11::Json ientry;
    bool recalc = false;
    uint64_t real_size = 0;
    uint64_t removed_size = 0;
    uint64_t opentime = 0;
    int res = 0;

    void run(int);
};

void kv_fs_defrag_all_t::run(int st)
{
    if (st == 1)
        goto resume_1;
    else if (st == 2)
        goto resume_2;
    else if (st == 3)
        goto resume_3;
    else if (st == 4)
        goto resume_4;
    else if (st == 5)
        goto resume_5;
    else if (st == 6)
        goto resume_6;
    clock_gettime(CLOCK_REALTIME, &now);
    list_shared = proxy->db->list_start("shared");
    proxy->db->list_next(list_shared, [this](int res, const std::string & key, const std::string & value)
    {
        if (res == -ENOENT || key.substr(0, 6) != "shared")
            this->res = -ENOENT;
        else
        {
            this->res = res;
            this->ino = kv_key_inode(key, 6);
        }
        run(1);
    });
    return;
    while (true)
    {
resume_1:
        if (res < 0)
        {
            if (res == -ENOENT)
                res = 0;
            break;
        }
        kv_read_inode(proxy, ino, [this](int res, const std::string & value, json11::Json attrs)
        {
            this->res = res;
            this->ientry = attrs;
            run(2);
        });
        return;
resume_2:
        if (res == -ENOENT)
        {
            // This shared inode is already removed
            proxy->db->del(kv_inode_prefix_key(ino, "shared"), [this](int res)
            {
                run(3);
            });
            return;
resume_3:
            proxy->db->list_next(list_shared, NULL);
            return;
        }
        real_size = ientry["size"].uint64_value();
        removed_size = ientry["removed"].uint64_value();
        opentime = (uint64_t)ientry["opentime"].number_value();
        recalc = false;
        if (!real_size && !opentime || recalc_stats)
        {
            // Statistics are missing - recalculate statistics
            recalc = true;
            fprintf(stderr, "Shared volume 0x%jx misses size and removal statistics, recalculating\n", ino);
            proxy->kvfs->defrag_volume(ino, true, true, [this](int res, uint64_t sz, uint64_t rm, uint64_t tm)
            {
                this->res = res;
                this->real_size = sz;
                this->removed_size = rm;
                this->opentime = tm;
                run(4);
            });
            return;
resume_4:
            if (res < 0)
            {
                break;
            }
            proxy->kvfs->update_inode(ino, true, [this](json11::Json::object & ientry)
            {
                ientry["size"] = real_size;
                ientry["removed"] = removed_size;
                ientry["opentime"] = opentime;
            }, [this](int res)
            {
                this->res = res;
                run(5);
            });
            return;
resume_5:
            if (res < 0)
            {
                fprintf(stderr, "Warning: Failed to update shared volume 0x%jx metadata: %s (code %d)\n", ino, strerror(-res), res);
            }
        }
        if ((opentime && opentime < now.tv_sec - proxy->kvfs->volume_untouched_sec || !opentime && include_empty) &&
            (real_size && removed_size || include_empty) &&
            removed_size >= (real_size * proxy->kvfs->defrag_percent / 100))
        {
            // This volume needs defrag
            fprintf(
                stderr, "Shared volume 0x%jx requires defragmentation: last "
                "open-for-append time %s, size %s, removed %s\n",
                ino, format_datetime(opentime).c_str(), format_size(real_size).c_str(), format_size(removed_size).c_str()
            );
            if (!recalc || !dry_run)
            {
                proxy->kvfs->defrag_volume(ino, no_rm, dry_run, [this](int res, uint64_t, uint64_t, uint64_t)
                {
                    this->res = res;
                    run(6);
                });
                return;
resume_6:
                if (res < 0)
                {
                    break;
                }
            }
        }
        else
        {
            fprintf(
                stderr, "Shared volume 0x%jx does not require defragmentation: last "
                "open-for-append time %s, size %s, removed %s\n",
                ino, format_datetime(opentime).c_str(), format_size(real_size).c_str(), format_size(removed_size).c_str()
            );
        }
        proxy->db->list_next(list_shared, NULL);
        return;
    }
    proxy->db->list_close(list_shared);
    auto cb = std::move(this->cb);
    cb(res);
    delete this;
}

void kv_fs_state_t::defrag_all(json11::Json cfg, std::function<void(int)> cb)
{
    auto st = new kv_fs_defrag_all_t;
    st->cb = cb;
    st->proxy = proxy;
    st->dry_run = cfg["dry_run"].bool_value();
    st->no_rm = cfg["no_rm"].bool_value();
    st->recalc_stats = cfg["recalc_stats"].bool_value();
    st->include_empty = cfg["include_empty"].bool_value();
    st->run(0);
}

void kv_fs_state_t::upgrade_db(std::function<void(int)> cb)
{
    // In the future, FS metadata format upgrades should be added here
    // Currently we only do one thing: we create missing shared inode list keys ("sharedXXX")
    proxy->db->get("version", [=](int res, const std::string & ver_value)
    {
        if (res < 0 && res != -ENOENT)
        {
            cb(res);
            return;
        }
        json11::Json ver;
        if (res == 0)
        {
            std::string err;
            ver = json11::Json::parse(ver_value, err);
            if (err != "")
            {
                fprintf(stderr, "Invalid JSON in `version` key, value: %s, error: %s\n", ver_value.c_str(), err.c_str());
                cb(-EINVAL);
                return;
            }
        }
        if (ver.uint64_value() > 1 || ver.is_object())
        {
            cb(0);
            return;
        }
        // Create missing shared inode index keys
        auto list_inodes = proxy->db->list_start("i");
        proxy->db->list_next(list_inodes, [=](int res, const std::string & key, const std::string & value)
        {
            if (res == -ENOENT || key.substr(0, 1) != "i" || key == "id")
            {
                proxy->db->list_close(list_inodes);
                proxy->db->set("version", "1", [=](int res)
                {
                    cb(0);
                }, [=](int res, const std::string & value)
                {
                    return res == -ENOENT || ver_value == value;
                });
                return;
            }
            uint64_t inode_id = kv_key_inode(key, 1);
            if (!inode_id)
            {
                fprintf(stderr, "Invalid inode key %s, skipping\n", key.c_str());
            }
            else
            {
                std::string err;
                auto ientry = json11::Json::parse(value, err);
                if (err != "")
                {
                    fprintf(stderr, "Invalid JSON in key %s (inode %ju), skipping\n", key.c_str(), inode_id);
                }
                else if (ientry["type"] == "shared")
                {
                    proxy->db->set(kv_inode_prefix_key(inode_id, "shared"), "{}", [=](int res)
                    {
                        if (res < 0)
                        {
                            fprintf(stderr, "Error writing key %s: %s (code %d)\n",
                                kv_inode_prefix_key(inode_id, "shared").c_str(), strerror(-res), res);
                        }
                        proxy->db->list_next(list_inodes, NULL);
                    });
                    return;
                }
            }
            proxy->db->list_next(list_inodes, NULL);
        });
    });
}
