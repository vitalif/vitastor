#pragma once

#include <sys/types.h>
#include <time.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <malloc.h>
#include <arpa/inet.h>
#include <malloc.h>

#include <set>
#include <deque>

#include "blockstore.h"
#include "ringloop.h"
#include "timerfd_manager.h"
#include "osd_ops.h"
#include "osd_peering_pg.h"
#include "etcd_state_client.h"

#define OSD_OP_IN 0
#define OSD_OP_OUT 1

#define CL_READ_HDR 1
#define CL_READ_DATA 2
#define CL_READ_REPLY_DATA 3
#define CL_WRITE_READY 1
#define CL_WRITE_REPLY 2
#define MAX_EPOLL_EVENTS 64
#define OSD_OP_INLINE_BUF_COUNT 16

#define PEER_CONNECTING 1
#define PEER_CONNECTED 2

#define OSD_LOADING_PGS 0x01
#define OSD_CONNECTING_PEERS 0x02
#define OSD_PEERING_PGS 0x04
#define OSD_FLUSHING_PGS 0x08
#define OSD_RECOVERING 0x10

#define IMMEDIATE_NONE 0
#define IMMEDIATE_SMALL 1
#define IMMEDIATE_ALL 2

#define MAX_AUTOSYNC_INTERVAL 3600
#define DEFAULT_AUTOSYNC_INTERVAL 5
#define MAX_RECOVERY_QUEUE 2048
#define DEFAULT_RECOVERY_QUEUE 4
#define DEFAULT_PG_STRIPE_SIZE 4*1024*1024 // 4 MB by default

//#define OSD_STUB

extern const char* osd_op_names[];

struct osd_op_buf_list_t
{
    int count = 0, alloc = 0, sent = 0;
    iovec *buf = NULL;
    iovec inline_buf[OSD_OP_INLINE_BUF_COUNT];

    ~osd_op_buf_list_t()
    {
        if (buf && buf != inline_buf)
        {
            free(buf);
        }
    }

    inline iovec* get_iovec()
    {
        return (buf ? buf : inline_buf) + sent;
    }

    inline int get_size()
    {
        return count - sent;
    }

    inline void push_back(void *nbuf, size_t len)
    {
        if (count >= alloc)
        {
            if (!alloc)
            {
                alloc = OSD_OP_INLINE_BUF_COUNT;
                buf = inline_buf;
            }
            else if (buf == inline_buf)
            {
                int old = alloc;
                alloc = ((alloc/16)*16 + 1);
                buf = (iovec*)malloc(sizeof(iovec) * alloc);
                memcpy(buf, inline_buf, sizeof(iovec)*old);
            }
            else
            {
                alloc = ((alloc/16)*16 + 1);
                buf = (iovec*)realloc(buf, sizeof(iovec) * alloc);
            }
        }
        buf[count++] = { .iov_base = nbuf, .iov_len = len };
    }
};

struct osd_primary_op_data_t;

struct osd_op_t
{
    timespec tv_begin;
    uint64_t op_type = OSD_OP_IN;
    int peer_fd;
    osd_any_op_t req;
    osd_any_reply_t reply;
    blockstore_op_t *bs_op = NULL;
    void *buf = NULL;
    void *rmw_buf = NULL;
    osd_primary_op_data_t* op_data = NULL;
    std::function<void(osd_op_t*)> callback;

    osd_op_buf_list_t send_list;

    ~osd_op_t();
};

struct osd_client_t
{
    sockaddr_in peer_addr;
    int peer_port;
    int peer_fd;
    int peer_state;
    int connect_timeout_id = -1;
    osd_num_t osd_num = 0;

    void *in_buf = NULL;

    // Read state
    int read_ready = 0;
    osd_op_t *read_op = NULL;
    int read_reply_id = 0;
    iovec read_iov;
    msghdr read_msg;
    void *read_buf = NULL;
    int read_remaining = 0;
    int read_state = 0;

    // Outbound operations sent to this client (which is probably an OSD peer)
    std::map<int, osd_op_t*> sent_ops;

    // Outbound messages (replies or requests)
    std::deque<osd_op_t*> outbox;

    // PGs dirtied by this client's primary-writes (FIXME to drop the connection)
    std::set<pg_num_t> dirty_pgs;

    // Write state
    osd_op_t *write_op = NULL;
    msghdr write_msg;
    int write_state = 0;
};

struct osd_rmw_stripe_t;

struct osd_object_id_t
{
    osd_num_t osd_num;
    object_id oid;
};

struct osd_recovery_op_t
{
    int st = 0;
    bool degraded = false;
    pg_num_t pg_num = 0;
    object_id oid = { 0 };
    osd_op_t *osd_op = NULL;
};

struct osd_wanted_peer_t
{
    bool connecting;
    time_t last_connect_attempt, last_load_attempt;
    int address_index;
};

class osd_t
{
    // config

    blockstore_config_t config;
    int etcd_report_interval = 30;

    bool readonly = false;
    osd_num_t osd_num = 1; // OSD numbers start with 1
    bool run_primary = false;
    std::string bind_address;
    int bind_port, listen_backlog;
    // FIXME: Implement client queue depth limit
    int client_queue_depth = 128;
    bool allow_test_ops = true;
    int receive_buffer_size = 9000;
    int print_stats_interval = 3;
    int immediate_commit = IMMEDIATE_NONE;
    int autosync_interval = DEFAULT_AUTOSYNC_INTERVAL; // sync every 5 seconds
    int recovery_queue_depth = DEFAULT_RECOVERY_QUEUE;
    int peer_connect_interval = 5;
    int peer_connect_timeout = 5;
    int log_level = 0;

    // cluster state

    etcd_state_client_t st_cli;
    int etcd_failed_attempts = 0;
    std::string etcd_lease_id;
    json11::Json self_state;
    std::map<osd_num_t, osd_wanted_peer_t> wanted_peers;
    bool loading_peer_config = false;
    std::set<pg_num_t> pg_state_dirty;
    bool pg_config_applied = false;
    bool etcd_reporting_pg_state = false;
    bool etcd_reporting_stats = false;

    // peers and PGs

    std::map<uint64_t, int> osd_peer_fds;
    std::map<pg_num_t, pg_t> pgs;
    std::set<pg_num_t> dirty_pgs;
    uint64_t misplaced_objects = 0, degraded_objects = 0, incomplete_objects = 0;
    int peering_state = 0;
    unsigned pg_count = 0;
    uint64_t next_subop_id = 1;
    std::map<object_id, osd_recovery_op_t> recovery_ops;
    osd_op_t *autosync_op = NULL;

    // Unstable writes
    std::map<osd_object_id_t, uint64_t> unstable_writes;
    std::deque<osd_op_t*> syncs_in_progress;

    // client & peer I/O

    bool stopping = false;
    int inflight_ops = 0;
    blockstore_t *bs;
    uint32_t bs_block_size, bs_disk_alignment;
    uint64_t pg_stripe_size = DEFAULT_PG_STRIPE_SIZE;
    ring_loop_t *ringloop;
    timerfd_manager_t *tfd = NULL;

    int wait_state = 0;
    int epoll_fd = 0;
    int listening_port = 0;
    int listen_fd = 0;
    ring_consumer_t consumer;
    std::map<int, std::function<void(int, int)>> epoll_handlers;

    std::unordered_map<int,osd_client_t> clients;
    std::vector<int> read_ready_clients;
    std::vector<int> write_ready_clients;

    // op statistics
    uint64_t op_stat_sum[2][OSD_OP_MAX+1] = { 0 };
    uint64_t op_stat_count[2][OSD_OP_MAX+1] = { 0 };
    uint64_t op_stat_bytes[2][OSD_OP_MAX+1] = { 0 };
    uint64_t subop_stat_sum[2][OSD_OP_MAX+1] = { 0 };
    uint64_t subop_stat_count[2][OSD_OP_MAX+1] = { 0 };
    const char* recovery_stat_names[2] = { "degraded", "misplaced" };
    uint64_t recovery_stat_count[2][2] = { 0 };
    uint64_t recovery_stat_bytes[2][2] = { 0 };

    // cluster connection
    void parse_config(blockstore_config_t & config);
    void init_cluster();
    void on_change_etcd_state_hook(json11::Json::object & changes);
    void on_load_config_hook(json11::Json::object & changes);
    json11::Json on_load_pgs_checks_hook();
    void on_load_pgs_hook(bool success);
    void bind_socket();
    void acquire_lease();
    json11::Json get_osd_state();
    void create_osd_state();
    void renew_lease();
    void print_stats();
    void reset_stats();
    json11::Json get_statistics();
    void report_statistics();
    void report_pg_state(pg_t & pg);
    void report_pg_states();
    void apply_pg_count();
    void apply_pg_config();
    void on_connect_peer(osd_num_t peer_osd, int peer_fd);
    void load_and_connect_peers();

    // event loop, socket read/write
    void loop();
    void set_fd_handler(int fd, std::function<void(int, int)> handler);
    void handle_epoll_events();
    void read_requests();
    void handle_read(ring_data_t *data, int peer_fd);
    void handle_finished_read(osd_client_t & cl);
    void handle_op_hdr(osd_client_t *cl);
    void handle_reply_hdr(osd_client_t *cl);
    bool try_send(osd_client_t & cl);
    void send_replies();
    void handle_send(ring_data_t *data, int peer_fd);
    void outbox_push(osd_client_t & cl, osd_op_t *op);

    // peer handling (primary OSD logic)
    void connect_peer(osd_num_t osd_num, const char *peer_host, int peer_port);
    void handle_connect_result(int peer_fd);
    void check_peer_config(osd_client_t & cl);
    void cancel_osd_ops(osd_client_t & cl);
    void cancel_op(osd_op_t *op);
    void stop_client(int peer_fd);
    void parse_test_peer(std::string peer);
    void handle_peers();
    void repeer_pgs(osd_num_t osd_num);
    void start_pg_peering(pg_num_t pg_num);
    void submit_sync_and_list_subop(osd_num_t role_osd, pg_peering_state_t *ps);
    void submit_list_subop(osd_num_t role_osd, pg_peering_state_t *ps);
    void discard_list_subop(osd_op_t *list_op);
    bool stop_pg(pg_num_t pg_num);
    void finish_stop_pg(pg_t & pg);

    // flushing, recovery and backfill
    void submit_pg_flush_ops(pg_num_t pg_num);
    void handle_flush_op(bool rollback, pg_num_t pg_num, pg_flush_batch_t *fb, osd_num_t peer_osd, int retval);
    void submit_flush_op(pg_num_t pg_num, pg_flush_batch_t *fb, bool rollback, osd_num_t peer_osd, int count, obj_ver_id *data);
    bool pick_next_recovery(osd_recovery_op_t &op);
    void submit_recovery_op(osd_recovery_op_t *op);
    bool continue_recovery();
    pg_osd_set_state_t* change_osd_set(pg_osd_set_state_t *st, pg_t *pg);

    // op execution
    void exec_op(osd_op_t *cur_op);
    void finish_op(osd_op_t *cur_op, int retval);

    // secondary ops
    void exec_sync_stab_all(osd_op_t *cur_op);
    void exec_show_config(osd_op_t *cur_op);
    void exec_secondary(osd_op_t *cur_op);
    void secondary_op_callback(osd_op_t *cur_op);

    // primary ops
    void autosync();
    bool prepare_primary_rw(osd_op_t *cur_op);
    void continue_primary_read(osd_op_t *cur_op);
    void continue_primary_write(osd_op_t *cur_op);
    void continue_primary_sync(osd_op_t *cur_op);
    void continue_primary_del(osd_op_t *cur_op);
    bool check_write_queue(osd_op_t *cur_op, pg_t & pg);
    void remove_object_from_state(object_id & oid, pg_osd_set_state_t *object_state, pg_t &pg);
    bool finalize_primary_write(osd_op_t *cur_op, pg_t & pg, pg_osd_set_t & loc_set, int base_state);
    void handle_primary_subop(uint64_t opcode, osd_op_t *cur_op, int retval, int expected, uint64_t version);
    void handle_primary_bs_subop(osd_op_t *subop);
    void add_bs_subop_stats(osd_op_t *subop);
    void pg_cancel_write_queue(pg_t & pg, object_id oid, int retval);
    void submit_primary_subops(int submit_type, int read_pg_size, const uint64_t* osd_set, osd_op_t *cur_op);
    void submit_primary_del_subops(osd_op_t *cur_op, uint64_t *cur_set, pg_osd_set_t & loc_set);
    void submit_primary_sync_subops(osd_op_t *cur_op);
    void submit_primary_stab_subops(osd_op_t *cur_op);

    inline pg_num_t map_to_pg(object_id oid)
    {
        return (oid.inode + oid.stripe / pg_stripe_size) % pg_count + 1;
    }

public:
    osd_t(blockstore_config_t & config, blockstore_t *bs, ring_loop_t *ringloop);
    ~osd_t();
    void force_stop(int exitcode);
    bool shutdown();
};

inline bool operator == (const osd_object_id_t & a, const osd_object_id_t & b)
{
    return a.osd_num == b.osd_num && a.oid.inode == b.oid.inode && a.oid.stripe == b.oid.stripe;
}

inline bool operator < (const osd_object_id_t & a, const osd_object_id_t & b)
{
    return a.osd_num < b.osd_num || a.osd_num == b.osd_num && (
        a.oid.inode < b.oid.inode || a.oid.inode == b.oid.inode && a.oid.stripe < b.oid.stripe
    );
}
