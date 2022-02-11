#pragma once

#include "cluster_client.h"
#include "epoll_manager.h"
#include "nfs_portmap.h"
#include "nfs/xdr_impl.h"

#define RPC_INIT_BUF_SIZE 32768

class cli_tool_t;

struct nfs_dir_t
{
    uint64_t id;
    uint64_t mod_rev;
    timespec mtime;
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

    pool_id_t default_pool_id;

    portmap_service_t pmap;
    ring_loop_t *ringloop = NULL;
    epoll_manager_t *epmgr = NULL;
    cluster_client_t *cli = NULL;
    cli_tool_t *cmd = NULL;

    std::vector<XDR*> xdr_pool;

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
