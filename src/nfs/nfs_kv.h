// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// NFS proxy over VitastorKV database - header

#pragma once

#include "proto/nfs.h"

#define KV_ROOT_INODE 1
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
};

struct kv_inode_extend_t
{
    int refcnt = 0;
    uint64_t cur_extend = 0, next_extend = 0, done_extend = 0;
    std::vector<std::function<void()>> waiters;
};

struct kv_idgen_t
{
    uint64_t next_id = 1, allocated_id = 0;
    uint64_t min_id = 1;
    std::vector<uint64_t> unallocated_ids;
};

struct kv_fs_state_t
{
    nfs_proxy_t *proxy = NULL;
    int touch_timer_id = -1;

    uint64_t fs_kv_inode = 0;
    uint64_t fs_inode_count = 0;
    int readdir_getattr_parallel = 8, id_alloc_batch_size = 200;
    uint64_t pool_block_size = 0;
    uint64_t pool_alignment = 0;
    uint64_t shared_inode_threshold = 0;
    uint64_t touch_interval = 1000;
    uint64_t volume_stats_interval_mul = 1;
    uint64_t volume_touch_interval_mul = 30;
    uint64_t volume_untouched_sec = 86400;
    uint64_t defrag_percent = 50;
    uint64_t defrag_block_count = 16;
    uint64_t defrag_iodepth = 16;
    bool dry_run = false;

    std::map<list_cookie_t, list_cookie_val_t> list_cookies;
    std::map<pool_id_t, kv_idgen_t> idgen;
    std::vector<shared_alloc_queue_t> allocating_shared;
    uint64_t cur_shared_inode = 0, cur_shared_offset = 0;
    std::map<inode_t, kv_inode_extend_t> extends;
    std::set<inode_t> touch_queue;
    std::map<inode_t, uint64_t> volume_removed;
    std::map<inode_t, json11::Json> read_hack_cache;
    uint64_t volume_stats_ctr = 0;
    uint64_t volume_touch_ctr = 0;

    std::vector<uint8_t> zero_block;
    std::vector<uint8_t> scrap_block;

    void init(nfs_proxy_t *proxy, json11::Json cfg);
    void touch_inodes();
    void update_inode(inode_t ino, bool allow_cache, std::function<void(json11::Json::object &)> change, std::function<void(int)> cb);
    void upgrade_db(std::function<void(int)> cb);
    void defrag_all(json11::Json cfg, std::function<void(int)> cb);
    void defrag_volume(inode_t ino, bool no_rm, bool dry_run, std::function<void(int, uint64_t, uint64_t, uint64_t)> cb);
    void write_inode(inode_t ino, json11::Json value, bool hack_cache, std::function<void(int)> cb, std::function<bool(int, const std::string &)> cas_cb);
    ~kv_fs_state_t();
};

struct shared_file_header_t
{
    uint64_t magic = 0;
    uint64_t inode = 0;
    uint64_t alloc = 0;
};

struct nfs_rmw_t
{
    nfs_proxy_t *parent = NULL;
    uint64_t ino = 0;
    uint64_t offset = 0;
    uint8_t *buf1 = NULL;
    uint64_t size1 = 0;
    uint8_t *buf2 = NULL;
    uint64_t size2 = 0;
    uint8_t *part_buf = NULL;
    uint64_t version = 0;
    nfs_rmw_t *other = NULL;
    std::function<void(nfs_rmw_t *)> cb;
    int res = 0;
};

nfsstat3 vitastor_nfs_map_err(int err);
nfstime3 nfstime_from_str(const std::string & s);
std::string nfstime_to_str(nfstime3 t);
std::string nfstime_now_str();
int kv_map_type(const std::string & type);
fattr3 get_kv_attributes(nfs_proxy_t *proxy, uint64_t ino, json11::Json attrs);
std::string kv_direntry_key(uint64_t dir_ino, const std::string & filename);
std::string kv_direntry_filename(const std::string & key);
std::string kv_inode_prefix_key(uint64_t ino, const char *prefix);
std::string kv_inode_key(uint64_t ino);
uint64_t kv_key_inode(const std::string & key, int prefix_len = 1);
std::string kv_fh(uint64_t ino);
uint64_t kv_fh_inode(const std::string & fh);
bool kv_fh_valid(const std::string & fh);
void allocate_new_id(nfs_proxy_t *proxy, pool_id_t pool_id, std::function<void(int res, uint64_t new_id)> cb);
void kv_read_inode(nfs_proxy_t *proxy, uint64_t ino,
    std::function<void(int res, const std::string & value, json11::Json ientry)> cb,
    bool allow_cache = false);
uint64_t align_shared_size(nfs_client_t *self, uint64_t size);
void nfs_do_rmw(nfs_rmw_t *rmw);
void nfs_move_inode_from(nfs_proxy_t *proxy, uint64_t ino, uint64_t shared_ino,
    uint64_t shared_offset, std::function<void(int res, bool moved)> cb);
uint32_t kv_get_access(const authsys_parms & auth_sys, const json11::Json & attrs);
bool kv_is_accessible(const authsys_parms & auth_sys, const json11::Json & attrs, uint32_t access);

int kv_nfs3_access_proc(void *opaque, rpc_op_t *rop);
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
