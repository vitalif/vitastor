// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#pragma once

#include <sys/types.h>
#include <stdint.h>
#include <arpa/inet.h>

#include <set>
#include <map>
#include <deque>
#include <vector>

#include "malloc_or_die.h"
#include "json11/json11.hpp"
#include "msgr_op.h"
#include "timerfd_manager.h"
#include <ringloop.h>

#define CL_READ_HDR 1
#define CL_READ_DATA 2
#define CL_READ_REPLY_DATA 3
#define CL_WRITE_READY 1

#define PEER_CONNECTING 1
#define PEER_CONNECTED 2
#define PEER_RDMA_CONNECTING 3
#define PEER_RDMA 4
#define PEER_STOPPED 5

#define VITASTOR_CONFIG_PATH "/etc/vitastor/vitastor.conf"

#define MSGR_SENDP_HDR 1
#define MSGR_SENDP_FREE 2

struct msgr_sendp_t
{
    osd_op_t *op;
    int flags;
};

#ifdef WITH_RDMA
struct msgr_rdma_connection_t;
struct msgr_rdma_context_t;
#endif

struct osd_client_t
{
    int refs = 0;

    sockaddr_storage peer_addr;
    int peer_port;
    int peer_fd = -1;
    int peer_state;
    int connect_timeout_id = -1;
    int ping_time_remaining = 0;
    int idle_time_remaining = 0;
    osd_num_t osd_num = 0;

    void *in_buf = NULL;

#ifdef WITH_RDMA
    msgr_rdma_connection_t *rdma_conn = NULL;
#endif

    // Read state
    int read_ready = 0;
    osd_op_t *read_op = NULL;
    iovec read_iov = { 0 };
    msghdr read_msg = { 0 };
    int read_remaining = 0;
    int read_state = 0;
    osd_op_buf_list_t recv_list;

    // Incoming operations
    std::vector<osd_op_t*> received_ops;

    // Outbound operations
    std::map<uint64_t, osd_op_t*> sent_ops;

    // PGs dirtied by this client's primary-writes
    std::set<pool_pg_num_t> dirty_pgs;

    // Write state
    msghdr write_msg = { 0 };
    int write_state = 0;
    std::vector<iovec> send_list, next_send_list;
    std::vector<msgr_sendp_t> outbox, next_outbox;

    ~osd_client_t();
};

struct osd_wanted_peer_t
{
    json11::Json address_list;
    int port;
    time_t last_connect_attempt;
    bool connecting, address_changed;
    int address_index;
    std::string cur_addr;
    int cur_port;
};

struct osd_op_stats_t
{
    uint64_t op_stat_sum[OSD_OP_MAX+1] = { 0 };
    uint64_t op_stat_count[OSD_OP_MAX+1] = { 0 };
    uint64_t op_stat_bytes[OSD_OP_MAX+1] = { 0 };
    uint64_t subop_stat_sum[OSD_OP_MAX+1] = { 0 };
    uint64_t subop_stat_count[OSD_OP_MAX+1] = { 0 };
};

#include <mutex>
#include <condition_variable>
#include <thread>

#ifdef __MOCK__
class msgr_iothread_t;
#else
struct iothread_sqe_t
{
    io_uring_sqe sqe;
    ring_data_t data;
};

class msgr_iothread_t
{
protected:
    ring_loop_t ring;
    ring_loop_t *outer_loop = NULL;
    ring_data_t *outer_loop_data = NULL;
    int eventfd = -1;
    bool stopped = false;
    std::mutex mu;
    std::condition_variable cond;
    std::vector<iothread_sqe_t> queue;
    std::thread thread;

    void run();
public:

    msgr_iothread_t();
    ~msgr_iothread_t();

    void add_sqe(io_uring_sqe & sqe);
    void stop();
    void add_to_ringloop(ring_loop_t *outer_loop);
};
#endif

struct osd_messenger_t
{
protected:
    int keepalive_timer_id = -1;

    uint32_t receive_buffer_size = 0;
    int peer_connect_interval = 0;
    int peer_connect_timeout = 0;
    int osd_idle_timeout = 0;
    int osd_ping_timeout = 0;
    int log_level = 0;
    bool use_sync_send_recv = false;
    int iothread_count = 0;

#ifdef WITH_RDMA
    bool use_rdma = true;
    std::vector<std::string> osd_networks;
    std::string rdma_device;
    uint64_t rdma_port_num = 1, rdma_gid_index = -1, rdma_mtu = 0;
    msgr_rdma_context_t *rdma_context = NULL;
    uint64_t rdma_max_sge = 0, rdma_max_send = 0, rdma_max_recv = 0;
    uint64_t rdma_max_msg = 0;
    bool rdma_odp = false;
#endif

    std::vector<msgr_iothread_t*> iothreads;
    std::vector<int> read_ready_clients;
    std::vector<int> write_ready_clients;
    // We don't use ringloop->set_immediate here because we may have no ringloop in client :)
    std::vector<osd_op_t*> set_immediate_ops;

public:
    timerfd_manager_t *tfd;
    ring_loop_t *ringloop;
    // osd_num_t is only for logging and asserts
    osd_num_t osd_num;
    uint64_t next_subop_id = 1;
    std::map<int, osd_client_t*> clients;
    std::map<osd_num_t, osd_wanted_peer_t> wanted_peers;
    std::map<uint64_t, int> osd_peer_fds;
    // op statistics
    osd_op_stats_t stats, recovery_stats;

    void init();
    void parse_config(const json11::Json & config);
    void connect_peer(uint64_t osd_num, json11::Json peer_state);
    void stop_client(int peer_fd, bool force = false, bool force_delete = false);
    void outbox_push(osd_op_t *cur_op);
    std::function<void(osd_op_t*)> exec_op;
    std::function<void(osd_num_t)> repeer_pgs;
    std::function<bool(osd_client_t*, json11::Json)> check_config_hook;
    void read_requests();
    void send_replies();
    void accept_connections(int listen_fd);
    ~osd_messenger_t();

    static json11::Json::object read_config(const json11::Json & config);
    static json11::Json::object merge_configs(const json11::Json::object & cli_config,
        const json11::Json::object & file_config,
        const json11::Json::object & etcd_global_config,
        const json11::Json::object & etcd_osd_config);

#ifdef WITH_RDMA
    bool is_rdma_enabled();
    bool connect_rdma(int peer_fd, std::string rdma_address, uint64_t client_max_msg);
#endif

    void inc_op_stats(osd_op_stats_t & stats, uint64_t opcode, timespec & tv_begin, timespec & tv_end, uint64_t len);
    void measure_exec(osd_op_t *cur_op);

protected:
    void try_connect_peer(uint64_t osd_num);
    void try_connect_peer_addr(osd_num_t peer_osd, const char *peer_host, int peer_port);
    void handle_peer_epoll(int peer_fd, int epoll_events);
    void handle_connect_epoll(int peer_fd);
    void on_connect_peer(osd_num_t peer_osd, int peer_fd);
    void check_peer_config(osd_client_t *cl);
    void cancel_osd_ops(osd_client_t *cl);
    void cancel_op(osd_op_t *op);

    bool try_send(osd_client_t *cl);
    void handle_send(int result, osd_client_t *cl);

    bool handle_read(int result, osd_client_t *cl);
    bool handle_read_buffer(osd_client_t *cl, void *curbuf, int remain);
    bool handle_finished_read(osd_client_t *cl);
    void handle_op_hdr(osd_client_t *cl);
    bool handle_reply_hdr(osd_client_t *cl);
    void handle_reply_ready(osd_op_t *op);
    void handle_immediate_ops();
    void clear_immediate_ops(int peer_fd);

#ifdef WITH_RDMA
    void try_send_rdma(osd_client_t *cl);
    void try_send_rdma_odp(osd_client_t *cl);
    void try_send_rdma_nodp(osd_client_t *cl);
    bool try_recv_rdma(osd_client_t *cl);
    void handle_rdma_events();
#endif
};
