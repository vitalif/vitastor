#pragma once

#include "cluster_client.h"
#include "epoll_manager.h"
#include "nfs_portmap.h"
#include "nfs/xdr_impl.h"
#include "kv_db.h"

#define RPC_INIT_BUF_SIZE 32768
#define MAX_REQUEST_SIZE 128*1024*1024
#define TRUE 1
#define FALSE 0

class cli_tool_t;

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

struct nfs_kv_write_state;

struct shared_alloc_queue_t
{
    nfs_kv_write_state *st;
    int state;
    uint64_t size;
};

struct inode_extend_t
{
    int refcnt = 0;
    uint64_t cur_extend = 0, next_extend = 0, done_extend = 0;
    std::vector<std::function<void()>> waiters;
};

class nfs_proxy_t
{
public:
    std::string bind_address;
    std::string name_prefix;
    uint64_t fsid = 1;
    uint64_t server_id = 0;
    std::string default_pool;
    std::string export_root;
    bool portmap_enabled;
    unsigned nfs_port;
    uint64_t fs_kv_inode = 0;
    uint64_t fs_base_inode = 0;
    uint64_t fs_inode_count = 0;
    int readdir_getattr_parallel = 8, id_alloc_batch_size = 200;
    int trace = 0;

    pool_id_t default_pool_id;
    uint64_t pool_block_size = 0;
    uint64_t pool_alignment = 0;
    uint64_t shared_inode_threshold = 0;

    portmap_service_t pmap;
    ring_loop_t *ringloop = NULL;
    epoll_manager_t *epmgr = NULL;
    cluster_client_t *cli = NULL;
    cli_tool_t *cmd = NULL;
    kv_dbw_t *db = NULL;
    std::map<list_cookie_t, list_cookie_val_t> list_cookies;
    uint64_t fs_next_id = 0, fs_allocated_id = 0;
    std::vector<uint64_t> unallocated_ids;
    std::vector<shared_alloc_queue_t> allocating_shared;
    uint64_t cur_shared_inode = 0, cur_shared_offset = 0;
    std::map<inode_t, inode_extend_t> extends;

    std::vector<XDR*> xdr_pool;

    // inode ID => statistics
    std::map<inode_t, json11::Json> inode_stats;
    // pool ID => statistics
    std::map<pool_id_t, json11::Json> pool_stats;

    ~nfs_proxy_t();

    static json11::Json::object parse_args(int narg, const char *args[]);
    void run(json11::Json cfg);
    void watch_stats();
    void parse_stats(etcd_kv_t & kv);
    void check_default_pool();
    void do_accept(int listen_fd);
    void daemonize();
};

// FIXME: Move to "proto"
struct rpc_cur_buffer_t
{
    uint8_t *buf;
    unsigned size;
    unsigned read_pos;
    unsigned parsed_pos;
    int refs;
};

struct rpc_used_buffer_t
{
    unsigned size;
    int refs;
};

struct rpc_free_buffer_t
{
    uint8_t *buf;
    unsigned size;
};

class nfs_client_t
{
public:
    nfs_proxy_t *parent = NULL;
    int nfs_fd;
    int epoll_events = 0;
    int refs = 0;
    bool stopped = false;
    std::set<rpc_service_proc_t> proc_table;

    // Read state
    rpc_cur_buffer_t cur_buffer = { 0 };
    std::map<uint8_t*, rpc_used_buffer_t> used_buffers;
    std::vector<rpc_free_buffer_t> free_buffers;

    iovec read_iov;
    msghdr read_msg = { 0 };

    // Write state
    msghdr write_msg = { 0 };
    std::vector<iovec> send_list, next_send_list;
    std::vector<rpc_op_t*> outbox, next_outbox;

    nfs_client_t();
    ~nfs_client_t();

    void select_read_buffer(unsigned wanted_size);
    void submit_read(unsigned wanted_size);
    void handle_read(int result);
    void submit_send();
    void handle_send(int result);
    int handle_rpc_message(void *base_buf, void *msg_buf, uint32_t msg_len);

    bool deref();
    void stop();
};

// FIXME: Move to "impl"
#include "nfs/nfs.h"

#define KV_ROOT_INODE 1
#define KV_NEXT_ID_KEY "id"
#define KV_ROOT_HANDLE "R"
#define SHARED_FILE_MAGIC_V1 0x711A5158A6EDF17E

struct shared_file_header_t
{
    uint64_t magic = 0;
    uint64_t inode = 0;
    uint64_t size = 0;
};

nfsstat3 vitastor_nfs_map_err(int err);
nfstime3 nfstime_from_str(const std::string & s);
std::string nfstime_to_str(nfstime3 t);
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
int kv_nfs3_fsstat_proc(void *opaque, rpc_op_t *rop);
int kv_nfs3_fsinfo_proc(void *opaque, rpc_op_t *rop);
int kv_nfs3_pathconf_proc(void *opaque, rpc_op_t *rop);
int nfs3_access_proc(void *opaque, rpc_op_t *rop);
int nfs3_null_proc(void *opaque, rpc_op_t *rop);
int nfs3_commit_proc(void *opaque, rpc_op_t *rop);
int mount3_mnt_proc(void *opaque, rpc_op_t *rop);
int mount3_dump_proc(void *opaque, rpc_op_t *rop);
int mount3_umnt_proc(void *opaque, rpc_op_t *rop);
int mount3_umntall_proc(void *opaque, rpc_op_t *rop);
int mount3_export_proc(void *opaque, rpc_op_t *rop);
