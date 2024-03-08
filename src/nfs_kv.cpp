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

fattr3 get_kv_attributes(nfs_client_t *self, uint64_t ino, json11::Json attrs)
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

std::string kv_inode_key(uint64_t ino)
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

std::string kv_fh(uint64_t ino)
{
    return "S"+std::string((char*)&ino, 8);
}

uint64_t kv_fh_inode(const std::string & fh)
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

bool kv_fh_valid(const std::string & fh)
{
    return fh == NFS_ROOT_HANDLE || fh.size() == 9 && fh[0] == 'S' || fh.size() > 17 && fh[0] == 'I';
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
    // Check if we're using VitastorFS
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
    readdir_getattr_parallel = cfg["readdir_getattr_parallel"].uint64_value();
    if (!readdir_getattr_parallel)
        readdir_getattr_parallel = 8;
    id_alloc_batch_size = cfg["id_alloc_batch_size"].uint64_value();
    if (!id_alloc_batch_size)
        id_alloc_batch_size = 200;
    auto & pool_cfg = proxy->cli->st_cli.pool_config.at(proxy->default_pool_id);
    pool_block_size = pool_cfg.pg_stripe_size;
    pool_alignment = pool_cfg.bitmap_granularity;
    // Open DB and wait
    int open_res = 0;
    bool open_done = false;
    proxy->db = new kv_dbw_t(proxy->cli);
    proxy->db->open(fs_kv_inode, cfg, [&](int res)
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
    fs_base_inode = ((uint64_t)proxy->default_pool_id << (64-POOL_ID_BITS));
    fs_inode_count = ((uint64_t)1 << (64-POOL_ID_BITS)) - 1;
    shared_inode_threshold = pool_block_size;
    if (!cfg["shared_inode_threshold"].is_null())
    {
        shared_inode_threshold = cfg["shared_inode_threshold"].uint64_value();
    }
    zero_block.resize(pool_block_size < 1048576 ? 1048576 : pool_block_size);
    scrap_block.resize(pool_block_size < 1048576 ? 1048576 : pool_block_size);
}
