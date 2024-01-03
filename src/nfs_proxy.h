#pragma once

#include "cluster_client.h"
#include "epoll_manager.h"
#include "nfs_portmap.h"
#include "nfs/xdr_impl.h"
#include "kv_db.h"

#define RPC_INIT_BUF_SIZE 32768

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

    portmap_service_t pmap;
    ring_loop_t *ringloop = NULL;
    epoll_manager_t *epmgr = NULL;
    cluster_client_t *cli = NULL;
    cli_tool_t *cmd = NULL;
    kv_dbw_t *db = NULL;
    std::map<list_cookie_t, list_cookie_val_t> list_cookies;
    uint64_t fs_next_id = 0, fs_allocated_id = 0;
    std::vector<uint64_t> unallocated_ids;

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
    std::string old_ientry;
    json11::Json::object attrs;
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
    std::map<inode_t, extend_inode_t> extends;
    std::multimap<extend_size_t, extend_write_t> extend_writes;

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
