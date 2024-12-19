// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// NFS proxy over VitastorKV database - common functions

#include <sys/time.h>

#include "str_util.h"
#include "nfs_proxy.h"
#include "nfs_common.h"
#include "nfs_kv.h"

nfstime3 nfstime_from_str(const std::string & s)
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

static std::string timespec_to_str(timespec t)
{
    char buf[64];
    snprintf(buf, sizeof(buf), "%ju.%09ju", t.tv_sec, t.tv_nsec);
    int l = strlen(buf);
    while (l > 0 && buf[l-1] == '0')
        l--;
    if (l > 0 && buf[l-1] == '.')
        l--;
    buf[l] = 0;
    return buf;
}

std::string nfstime_to_str(nfstime3 t)
{
    return timespec_to_str((timespec){ .tv_sec = t.seconds, .tv_nsec = t.nseconds });
}

std::string nfstime_now_str()
{
    timespec t;
    clock_gettime(CLOCK_REALTIME, &t);
    return timespec_to_str(t);
}

int kv_map_type(const std::string & type)
{
    return (type == "" || type == "file" ? NF3REG :
        (type == "dir" ? NF3DIR :
        (type == "blk" ? NF3BLK :
        (type == "chr" ? NF3CHR :
        (type == "link" ? NF3LNK :
        (type == "sock" ? NF3SOCK :
        (type == "fifo" ? NF3FIFO : -1)))))));
}

fattr3 get_kv_attributes(nfs_proxy_t *proxy, uint64_t ino, json11::Json attrs)
{
    auto type = kv_map_type(attrs["type"].string_value());
    auto mode = attrs["mode"].uint64_value();
    auto nlink = attrs["nlink"].uint64_value();
    nfstime3 mtime = nfstime_from_str(attrs["mtime"].string_value());
    nfstime3 atime = attrs["atime"].is_null() ? mtime : nfstime_from_str(attrs["atime"].string_value());
    nfstime3 ctime = attrs["ctime"].is_null() ? mtime : nfstime_from_str(attrs["ctime"].string_value());
    // In theory we could store the binary structure itself, but JSON is simpler :-)
    return (fattr3){
        .type = (type == 0 ? NF3REG : (ftype3)type),
        .mode = (attrs["mode"].is_null() ? (type == NF3DIR ? 0755 : 0644) : (uint32_t)mode),
        .nlink = (nlink == 0 ? 1 : (uint32_t)nlink),
        .uid = (uint32_t)attrs["uid"].uint64_value(),
        .gid = (uint32_t)attrs["gid"].uint64_value(),
        .size = (type == NF3DIR ? 4096 : attrs["size"].uint64_value()),
        // FIXME Counting actual used file size would require reworking statistics
        .used = (type == NF3DIR ? 4096 : attrs["size"].uint64_value()),
        .rdev = (type == NF3BLK || type == NF3CHR
            ? (specdata3){ (uint32_t)attrs["major"].uint64_value(), (uint32_t)attrs["minor"].uint64_value() }
            : (specdata3){}),
        .fsid = proxy->fsid,
        .fileid = ino,
        .atime = atime,
        .mtime = mtime,
        .ctime = ctime,
    };
}

std::string kv_direntry_key(uint64_t dir_ino, const std::string & filename)
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

std::string kv_direntry_filename(const std::string & key)
{
    // decode as: d <length> <hex dir_ino> / <filename>
    auto pos = key.find("/");
    if (pos != std::string::npos)
        return key.substr(pos+1);
    return key;
}

std::string kv_inode_prefix_key(uint64_t ino, const char *prefix)
{
    int max = 32+strlen(prefix);
    char key[max];
    snprintf(key, max, "%s%x", prefix, INODE_POOL(ino));
    int n = strnlen(key, max-1);
    snprintf(key+n+1, max-n-1, "%jx", INODE_NO_POOL(ino));
    int m = strnlen(key+n+1, max-n-2);
    key[n] = 'G'+m;
    return std::string(key);
}

std::string kv_inode_key(uint64_t ino)
{
    return kv_inode_prefix_key(ino, "i");
}

uint64_t kv_key_inode(const std::string & key, int prefix_len)
{
    if (key.size() < prefix_len)
        return 0;
    uint32_t pool_id = 0;
    char len_plus_g = 0;
    uint64_t inode_id = 0;
    char null_byte = 0;
    int scanned = sscanf(key.c_str()+prefix_len, "%x%c%jx%c", &pool_id, &len_plus_g, &inode_id, &null_byte);
    if (scanned != 3 || !inode_id || INODE_POOL(inode_id) != 0)
        return 0;
    return INODE_WITH_POOL(pool_id, inode_id);
}

std::string kv_fh(uint64_t ino)
{
    char key[32] = { 0 };
    snprintf(key, sizeof(key), "S%jx", ino);
    return key;
}

uint64_t kv_fh_inode(const std::string & fh)
{
    if (fh == NFS_ROOT_HANDLE)
    {
        return 1;
    }
    else if (fh[0] == 'S')
    {
        uint64_t ino = 0;
        int r = sscanf(fh.c_str()+1, "%jx", &ino);
        if (r == 1)
            return ino;
    }
    return 0;
}

bool kv_fh_valid(const std::string & fh)
{
    return fh == NFS_ROOT_HANDLE || fh[0] == 'S';
}

void nfs_kv_procs(nfs_client_t *self)
{
    struct rpc_service_proc_t pt[] = {
        {NFS_PROGRAM, NFS_V3, NFS3_NULL,        nfs3_null_proc,           NULL,                            0,                        NULL,                           0,                       self},
        {NFS_PROGRAM, NFS_V3, NFS3_GETATTR,     kv_nfs3_getattr_proc,     (xdrproc_t)xdr_GETATTR3args,     sizeof(GETATTR3args),     (xdrproc_t)xdr_GETATTR3res,     sizeof(GETATTR3res),     self},
        {NFS_PROGRAM, NFS_V3, NFS3_SETATTR,     kv_nfs3_setattr_proc,     (xdrproc_t)xdr_SETATTR3args,     sizeof(SETATTR3args),     (xdrproc_t)xdr_SETATTR3res,     sizeof(SETATTR3res),     self},
        {NFS_PROGRAM, NFS_V3, NFS3_LOOKUP,      kv_nfs3_lookup_proc,      (xdrproc_t)xdr_LOOKUP3args,      sizeof(LOOKUP3args),      (xdrproc_t)xdr_LOOKUP3res,      sizeof(LOOKUP3res),      self},
        {NFS_PROGRAM, NFS_V3, NFS3_ACCESS,      nfs3_access_proc,         (xdrproc_t)xdr_ACCESS3args,      sizeof(ACCESS3args),      (xdrproc_t)xdr_ACCESS3res,      sizeof(ACCESS3res),      self},
        {NFS_PROGRAM, NFS_V3, NFS3_READLINK,    kv_nfs3_readlink_proc,    (xdrproc_t)xdr_READLINK3args,    sizeof(READLINK3args),    (xdrproc_t)xdr_READLINK3res,    sizeof(READLINK3res),    self},
        {NFS_PROGRAM, NFS_V3, NFS3_READ,        kv_nfs3_read_proc,        (xdrproc_t)xdr_READ3args,        sizeof(READ3args),        (xdrproc_t)xdr_READ3res,        sizeof(READ3res),        self},
        {NFS_PROGRAM, NFS_V3, NFS3_WRITE,       kv_nfs3_write_proc,       (xdrproc_t)xdr_WRITE3args,       sizeof(WRITE3args),       (xdrproc_t)xdr_WRITE3res,       sizeof(WRITE3res),       self},
        {NFS_PROGRAM, NFS_V3, NFS3_CREATE,      kv_nfs3_create_proc,      (xdrproc_t)xdr_CREATE3args,      sizeof(CREATE3args),      (xdrproc_t)xdr_CREATE3res,      sizeof(CREATE3res),      self},
        {NFS_PROGRAM, NFS_V3, NFS3_MKDIR,       kv_nfs3_mkdir_proc,       (xdrproc_t)xdr_MKDIR3args,       sizeof(MKDIR3args),       (xdrproc_t)xdr_MKDIR3res,       sizeof(MKDIR3res),       self},
        {NFS_PROGRAM, NFS_V3, NFS3_SYMLINK,     kv_nfs3_symlink_proc,     (xdrproc_t)xdr_SYMLINK3args,     sizeof(SYMLINK3args),     (xdrproc_t)xdr_SYMLINK3res,     sizeof(SYMLINK3res),     self},
        {NFS_PROGRAM, NFS_V3, NFS3_MKNOD,       kv_nfs3_mknod_proc,       (xdrproc_t)xdr_MKNOD3args,       sizeof(MKNOD3args),       (xdrproc_t)xdr_MKNOD3res,       sizeof(MKNOD3res),       self},
        {NFS_PROGRAM, NFS_V3, NFS3_REMOVE,      kv_nfs3_remove_proc,      (xdrproc_t)xdr_REMOVE3args,      sizeof(REMOVE3args),      (xdrproc_t)xdr_REMOVE3res,      sizeof(REMOVE3res),      self},
        {NFS_PROGRAM, NFS_V3, NFS3_RMDIR,       kv_nfs3_rmdir_proc,       (xdrproc_t)xdr_RMDIR3args,       sizeof(RMDIR3args),       (xdrproc_t)xdr_RMDIR3res,       sizeof(RMDIR3res),       self},
        {NFS_PROGRAM, NFS_V3, NFS3_RENAME,      kv_nfs3_rename_proc,      (xdrproc_t)xdr_RENAME3args,      sizeof(RENAME3args),      (xdrproc_t)xdr_RENAME3res,      sizeof(RENAME3res),      self},
        {NFS_PROGRAM, NFS_V3, NFS3_LINK,        kv_nfs3_link_proc,        (xdrproc_t)xdr_LINK3args,        sizeof(LINK3args),        (xdrproc_t)xdr_LINK3res,        sizeof(LINK3res),        self},
        {NFS_PROGRAM, NFS_V3, NFS3_READDIR,     kv_nfs3_readdir_proc,     (xdrproc_t)xdr_READDIR3args,     sizeof(READDIR3args),     (xdrproc_t)xdr_READDIR3res,     sizeof(READDIR3res),     self},
        {NFS_PROGRAM, NFS_V3, NFS3_READDIRPLUS, kv_nfs3_readdirplus_proc, (xdrproc_t)xdr_READDIRPLUS3args, sizeof(READDIRPLUS3args), (xdrproc_t)xdr_READDIRPLUS3res, sizeof(READDIRPLUS3res), self},
        {NFS_PROGRAM, NFS_V3, NFS3_FSSTAT,      nfs3_fsstat_proc,         (xdrproc_t)xdr_FSSTAT3args,      sizeof(FSSTAT3args),      (xdrproc_t)xdr_FSSTAT3res,      sizeof(FSSTAT3res),      self},
        {NFS_PROGRAM, NFS_V3, NFS3_FSINFO,      nfs3_fsinfo_proc,         (xdrproc_t)xdr_FSINFO3args,      sizeof(FSINFO3args),      (xdrproc_t)xdr_FSINFO3res,      sizeof(FSINFO3res),      self},
        {NFS_PROGRAM, NFS_V3, NFS3_PATHCONF,    nfs3_pathconf_proc,       (xdrproc_t)xdr_PATHCONF3args,    sizeof(PATHCONF3args),    (xdrproc_t)xdr_PATHCONF3res,    sizeof(PATHCONF3res),    self},
        {NFS_PROGRAM, NFS_V3, NFS3_COMMIT,      nfs3_commit_proc,         (xdrproc_t)xdr_COMMIT3args,      sizeof(COMMIT3args),      (xdrproc_t)xdr_COMMIT3res,      sizeof(COMMIT3res),      self},
        {MOUNT_PROGRAM, MOUNT_V3, MOUNT3_NULL,    nfs3_null_proc,         NULL,                            0,                        NULL,                         0,                         self},
        {MOUNT_PROGRAM, MOUNT_V3, MOUNT3_MNT,     mount3_mnt_proc,        (xdrproc_t)xdr_nfs_dirpath,      sizeof(nfs_dirpath),      (xdrproc_t)xdr_nfs_mountres3, sizeof(nfs_mountres3),     self},
        {MOUNT_PROGRAM, MOUNT_V3, MOUNT3_DUMP,    mount3_dump_proc,       NULL,                            0,                        (xdrproc_t)xdr_nfs_mountlist, sizeof(nfs_mountlist),     self},
        {MOUNT_PROGRAM, MOUNT_V3, MOUNT3_UMNT,    mount3_umnt_proc,       (xdrproc_t)xdr_nfs_dirpath,      sizeof(nfs_dirpath),      NULL,                         0,                         self},
        {MOUNT_PROGRAM, MOUNT_V3, MOUNT3_UMNTALL, mount3_umntall_proc,    NULL,                            0,                        NULL,                         0,                         self},
        {MOUNT_PROGRAM, MOUNT_V3, MOUNT3_EXPORT,  mount3_export_proc,     NULL,                            0,                        (xdrproc_t)xdr_nfs_exports,   sizeof(nfs_exports),       self},
    };
    for (int i = 0; i < sizeof(pt)/sizeof(pt[0]); i++)
    {
        self->proc_table.insert(pt[i]);
    }
}

void kv_fs_state_t::init(nfs_proxy_t *proxy, json11::Json cfg)
{
    this->proxy = proxy;
    auto & pool_cfg = proxy->cli->st_cli.pool_config.at(proxy->default_pool_id);
    fs_kv_inode = cfg["fs"].uint64_value();
    if (fs_kv_inode)
    {
        if (!INODE_POOL(fs_kv_inode))
        {
            fprintf(stderr, "FS metadata inode number must include pool\n");
            exit(1);
        }
    }
    else
    {
        for (auto & ic: proxy->cli->st_cli.inode_config)
        {
            if (ic.second.name == cfg["fs"].string_value())
            {
                fs_kv_inode = ic.first;
                break;
            }
        }
        if (!fs_kv_inode)
        {
            fprintf(stderr, "FS metadata image \"%s\" does not exist\n", cfg["fs"].string_value().c_str());
            exit(1);
        }
    }
    if (proxy->cli->st_cli.inode_config.find(fs_kv_inode) != proxy->cli->st_cli.inode_config.end())
    {
        auto & name = proxy->cli->st_cli.inode_config.at(fs_kv_inode).name;
        if (pool_cfg.used_for_fs != name)
        {
            fprintf(stderr, "Please mark pool as used for this file system with `vitastor-cli modify-pool --used-for-fs %s %s`\n",
                name.c_str(), cfg["fs"].string_value().c_str());
            exit(1);
        }
    }
    auto img_it = proxy->cli->st_cli.inode_config.lower_bound(INODE_WITH_POOL(proxy->default_pool_id+1, 0));
    if (img_it != proxy->cli->st_cli.inode_config.begin())
    {
        img_it--;
        if (img_it != proxy->cli->st_cli.inode_config.begin() && INODE_POOL(img_it->first) == proxy->default_pool_id)
        {
            idgen[proxy->default_pool_id].min_id = INODE_NO_POOL(img_it->first) + 1;
        }
    }
    readdir_getattr_parallel = cfg["readdir_getattr_parallel"].uint64_value();
    if (!readdir_getattr_parallel)
        readdir_getattr_parallel = 8;
    id_alloc_batch_size = cfg["id_alloc_batch_size"].uint64_value();
    if (!id_alloc_batch_size)
        id_alloc_batch_size = 200;
    touch_interval = cfg["touch_interval"].uint64_value();
    if (!touch_interval)
        touch_interval = 1000; // ms
    else if (touch_interval < 100)
        touch_interval = 100;
    volume_stats_interval_mul = cfg["volume_stats_interval"].uint64_value() / touch_interval;
    if (!volume_stats_interval_mul)
        volume_stats_interval_mul = 1;
    volume_touch_interval_mul = cfg["volume_touch_interval"].uint64_value() / touch_interval;
    if (!volume_touch_interval_mul)
        volume_touch_interval_mul = 30;
    volume_untouched_sec = cfg["volume_untouched"].uint64_value();
    if (!volume_untouched_sec)
        volume_untouched_sec = 86400;
    if (volume_untouched_sec < 60)
        volume_untouched_sec = 60;
    defrag_percent = cfg["defrag_percent"].is_null() ? 50 : cfg["defrag_percent"].uint64_value();
    if (defrag_percent < 0)
        defrag_percent = 0;
    if (defrag_percent > 100)
        defrag_percent = 100;
    defrag_block_count = cfg["defrag_block_count"].is_null() ? 16 : cfg["defrag_block_count"].uint64_value();
    if (defrag_block_count < 1)
        defrag_block_count = 1;
    if (defrag_block_count > 1048576)
        defrag_block_count = 1048576;
    defrag_iodepth = cfg["defrag_iodepth"].is_null() ? 16 : cfg["defrag_iodepth"].uint64_value();
    if (defrag_iodepth < 1)
        defrag_iodepth = 1;
    if (defrag_iodepth > 1048576)
        defrag_iodepth = 1048576;
    pool_block_size = pool_cfg.pg_stripe_size;
    pool_alignment = pool_cfg.bitmap_granularity;
    // Open DB and wait
    int open_res = 0;
    bool open_done = false;
    proxy->db = new vitastorkv_dbw_t(proxy->cli);
    std::map<std::string, std::string> kv_cfg;
    for (auto & kv: cfg.object_items())
    {
        kv_cfg[kv.first] = kv.second.as_string();
    }
    // Open K/V DB
    proxy->db->open(fs_kv_inode, kv_cfg, [&](int res)
    {
        open_done = true;
        open_res = res;
    });
    while (!open_done)
    {
        proxy->ringloop->loop();
        if (open_done)
            break;
        proxy->ringloop->wait();
    }
    if (open_res < 0)
    {
        fprintf(stderr, "Failed to open key/value filesystem metadata index: %s (code %d)\n",
            strerror(-open_res), open_res);
        exit(1);
    }
    // Proceed
    fs_inode_count = ((uint64_t)1 << (64-POOL_ID_BITS)) - 1;
    shared_inode_threshold = pool_block_size;
    if (!cfg["shared_inode_threshold"].is_null())
    {
        shared_inode_threshold = cfg["shared_inode_threshold"].uint64_value();
    }
    zero_block.resize(pool_block_size < 1048576 ? 1048576 : pool_block_size);
    scrap_block.resize(pool_block_size < 1048576 ? 1048576 : pool_block_size);
    touch_timer_id = proxy->epmgr->tfd->set_timer(touch_interval, true, [this](int){ touch_inodes(); });
}

kv_fs_state_t::~kv_fs_state_t()
{
    if (proxy && touch_timer_id >= 0)
    {
        proxy->epmgr->tfd->clear_timer(touch_timer_id);
        touch_timer_id = -1;
    }
}

void kv_fs_state_t::write_inode(inode_t ino, json11::Json value, bool hack_cache, std::function<void(int)> cb, std::function<bool(int, const std::string &)> cas_cb)
{
    if (!proxy->rdma_context)
    {
        proxy->db->set(kv_inode_key(ino), value.dump(), cb, cas_cb);
        return;
    }
    // FIXME Linux NFS RDMA transport has a bug - it corrupts the data (by offsetting it 84 bytes)
    // when the READ reply doesn't contain post_op_attr. So we have to fill post_op_attr with RDMA. :-(
    // So we at least cache it to not repeat K/V requests every read.
    read_hack_cache.erase(ino);
    proxy->db->set(kv_inode_key(ino), value.dump(), [=](int res)
    {
        if (hack_cache || res != 0)
            read_hack_cache.erase(ino);
        else
            read_hack_cache[ino] = value;
        cb(res);
    }, cas_cb);
}

void kv_fs_state_t::update_inode(inode_t ino, bool allow_cache, std::function<void(json11::Json::object &)> change, std::function<void(int)> cb)
{
    // FIXME: Use "update" query
    kv_read_inode(proxy, ino, [=](int res, const std::string & value, json11::Json attrs)
    {
        if (!res)
        {
            read_hack_cache.erase(ino);
            auto ientry = attrs.object_items();
            change(ientry);
            bool *found = new bool;
            *found = true;
            json11::Json ientry_json(ientry);
            proxy->db->set(kv_inode_key(ino), ientry_json.dump(), [=](int res)
            {
                read_hack_cache.erase(ino);
                if (!*found)
                    res = -ENOENT;
                delete found;
                if (res == -EAGAIN)
                    update_inode(ino, false, change, cb);
                else if (cb)
                    cb(res);
            }, [value, found](int res, const std::string & old_value)
            {
                *found = res == 0;
                return res == 0 && old_value == value;
            });
        }
        else if (cb)
        {
            cb(res);
        }
    }, allow_cache);
}

void kv_fs_state_t::touch_inodes()
{
    // Clear RDMA read fattr3 "hack" cache every second
    read_hack_cache.clear();
    std::set<inode_t> q = std::move(touch_queue);
    for (auto ino: q)
    {
        update_inode(ino, true, [](json11::Json::object & ientry)
        {
            ientry["mtime"] = ientry["ctime"] = nfstime_now_str();
            ientry.erase("verf");
        }, NULL);
    }
    if (++volume_stats_ctr >= volume_stats_interval_mul)
    {
        volume_stats_ctr = 0;
        auto shr = std::move(volume_removed);
        for (auto & sp: shr)
        {
            update_inode(sp.first, true, [removed = sp.second](json11::Json::object & ientry)
            {
                ientry["removed"] = ientry["removed"].uint64_value() + removed;
            }, NULL);
        }
    }
    if (!((volume_touch_ctr++) % volume_touch_interval_mul) && cur_shared_inode)
    {
        volume_touch_ctr = 1;
        update_inode(cur_shared_inode, true, [size = cur_shared_offset](json11::Json::object & ientry)
        {
            ientry["opentime"] = nfstime_now_str();
            ientry["size"] = size;
        }, NULL);
    }
}
