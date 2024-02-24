// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// NFS proxy over VitastorKV database - header

#pragma once

#include "nfs/nfs.h"

#define KV_ROOT_INODE 1
#define KV_NEXT_ID_KEY "id"
#define SHARED_FILE_MAGIC_V1 0x711A5158A6EDF17E

struct nfs_kv_write_state;

struct list_cookie_t
{
    uint64_t dir_ino, cookieverf, cookie;
};

inline bool operator < (const list_cookie_t & a, const list_cookie_t & b)
{
    return a.dir_ino < b.dir_ino || a.dir_ino == b.dir_ino &&
        (a.cookieverf < b.cookieverf || a.cookieverf == b.cookieverf && a.cookie < b.cookie);
};

struct list_cookie_val_t
{
    std::string key;
};

struct shared_alloc_queue_t
{
    nfs_kv_write_state *st;
    int state;
    uint64_t size;
};

struct kv_inode_extend_t
{
    int refcnt = 0;
    uint64_t cur_extend = 0, next_extend = 0, done_extend = 0;
    std::vector<std::function<void()>> waiters;
};

struct kv_fs_state_t
{
    std::map<list_cookie_t, list_cookie_val_t> list_cookies;
    uint64_t fs_next_id = 1, fs_allocated_id = 0;
    std::vector<uint64_t> unallocated_ids;
    std::vector<shared_alloc_queue_t> allocating_shared;
    uint64_t cur_shared_inode = 0, cur_shared_offset = 0;
    std::map<inode_t, kv_inode_extend_t> extends;
};

struct shared_file_header_t
{
    uint64_t magic = 0;
    uint64_t inode = 0;
    uint64_t alloc = 0;
};

nfsstat3 vitastor_nfs_map_err(int err);
nfstime3 nfstime_from_str(const std::string & s);
std::string nfstime_to_str(nfstime3 t);
std::string nfstime_now_str();
int kv_map_type(const std::string & type);
fattr3 get_kv_attributes(nfs_client_t *self, uint64_t ino, json11::Json attrs);
std::string kv_direntry_key(uint64_t dir_ino, const std::string & filename);
std::string kv_direntry_filename(const std::string & key);
std::string kv_inode_key(uint64_t ino);
std::string kv_fh(uint64_t ino);
uint64_t kv_fh_inode(const std::string & fh);
bool kv_fh_valid(const std::string & fh);
void allocate_new_id(nfs_client_t *self, std::function<void(int res, uint64_t new_id)> cb);
void kv_read_inode(nfs_client_t *self, uint64_t ino,
    std::function<void(int res, const std::string & value, json11::Json ientry)> cb,
    bool allow_cache = false);
uint64_t align_shared_size(nfs_client_t *self, uint64_t size);

int kv_nfs3_getattr_proc(void *opaque, rpc_op_t *rop);
int kv_nfs3_setattr_proc(void *opaque, rpc_op_t *rop);
int kv_nfs3_lookup_proc(void *opaque, rpc_op_t *rop);
int kv_nfs3_readlink_proc(void *opaque, rpc_op_t *rop);
int kv_nfs3_read_proc(void *opaque, rpc_op_t *rop);
int kv_nfs3_write_proc(void *opaque, rpc_op_t *rop);
int kv_nfs3_create_proc(void *opaque, rpc_op_t *rop);
int kv_nfs3_mkdir_proc(void *opaque, rpc_op_t *rop);
int kv_nfs3_symlink_proc(void *opaque, rpc_op_t *rop);
int kv_nfs3_mknod_proc(void *opaque, rpc_op_t *rop);
int kv_nfs3_remove_proc(void *opaque, rpc_op_t *rop);
int kv_nfs3_rmdir_proc(void *opaque, rpc_op_t *rop);
int kv_nfs3_rename_proc(void *opaque, rpc_op_t *rop);
int kv_nfs3_link_proc(void *opaque, rpc_op_t *rop);
int kv_nfs3_readdir_proc(void *opaque, rpc_op_t *rop);
int kv_nfs3_readdirplus_proc(void *opaque, rpc_op_t *rop);
