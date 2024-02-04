// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// NFS proxy over Vitastor block images - header

#pragma once

struct nfs_dir_t
{
    uint64_t id;
    uint64_t mod_rev;
    timespec mtime;
};

struct extend_size_t
{
    inode_t inode;
    uint64_t new_size;
};

inline bool operator < (const extend_size_t &a, const extend_size_t &b)
{
    return a.inode < b.inode || a.inode == b.inode && a.new_size < b.new_size;
}

struct extend_write_t
{
    rpc_op_t *rop;
    int resize_res, write_res; // 1 = started, 0 = completed OK, -errno = completed with error
};

struct extend_inode_t
{
    uint64_t cur_extend = 0, next_extend = 0;
};

struct block_fs_state_t
{
    // filehandle = "S"+base64(sha256(full name with prefix)) or "roothandle" for mount root)
    uint64_t next_dir_id = 2;
    // filehandle => dir with name_prefix
    std::map<std::string, std::string> dir_by_hash;
    // dir with name_prefix => dir info
    std::map<std::string, nfs_dir_t> dir_info;
    // filehandle => inode ID
    std::map<std::string, inode_t> inode_by_hash;
    // inode ID => filehandle
    std::map<inode_t, std::string> hash_by_inode;

    // inode extend requests in progress
    std::map<inode_t, extend_inode_t> extends;
    std::multimap<extend_size_t, extend_write_t> extend_writes;

    void init(nfs_proxy_t *proxy);
};

nfsstat3 vitastor_nfs_map_err(int err);
