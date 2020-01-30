#pragma once

#include <sys/types.h>
#include <sys/time.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <malloc.h>
#include <arpa/inet.h>
#include <malloc.h>

#include <deque>

#include "blockstore.h"
#include "ringloop.h"
#include "osd_ops.h"
#include "osd_peering_pg.h"

#include "sparsepp/sparsepp/spp.h"

#define OSD_OP_IN 0
#define OSD_OP_OUT 1

#define CL_READ_OP 1
#define CL_READ_DATA 2
#define CL_READ_REPLY_DATA 3
#define SQE_SENT 0x100l
#define CL_WRITE_READY 1
#define CL_WRITE_REPLY 2
#define CL_WRITE_DATA 3
#define MAX_EPOLL_EVENTS 16

#define PEER_CONNECTING 1
#define PEER_CONNECTED 2

//#define OSD_STUB

// FIXME: add types for pg_num and osd_num?

struct osd_op_t
{
    int op_type;
    int peer_fd;
    union
    {
        osd_any_op_t op;
        uint8_t op_buf[OSD_PACKET_SIZE] = { 0 };
    };
    union
    {
        osd_any_reply_t reply;
        uint8_t reply_buf[OSD_PACKET_SIZE] = { 0 };
    };
    blockstore_op_t bs_op;
    void *buf = NULL;
    std::function<void(osd_op_t*)> callback;

    ~osd_op_t();
};

struct osd_peer_def_t
{
    uint64_t osd_num = 0;
    std::string addr;
    int port = 0;
    time_t last_connect_attempt = 0;
};

struct osd_client_t
{
    sockaddr_in peer_addr;
    int peer_port;
    int peer_fd;
    int peer_state;
    std::function<void(int)> connect_callback;
    uint64_t osd_num = 0;
    //int in_flight_ops = 0;

    // Read state
    bool read_ready = false;
    bool reading = false;
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

    // Write state
    osd_op_t *write_op = NULL;
    iovec write_iov;
    msghdr write_msg;
    void *write_buf = NULL;
    int write_remaining = 0;
    int write_state = 0;
};

class osd_t
{
    // config

    uint64_t osd_num = 1; // OSD numbers start with 1
    bool run_primary = false;
    std::vector<osd_peer_def_t> peers;
    blockstore_config_t config;
    std::string bind_address;
    int bind_port, listen_backlog;
    int client_queue_depth = 128;
    bool allow_test_ops = true;

    // peer OSDs

    std::map<uint64_t, int> osd_peer_fds;
    std::vector<pg_t> pgs;
    int peering_state = 0;
    unsigned pg_count = 0;

    // client & peer I/O

    bool stopping = false;
    int inflight_ops = 0;
    blockstore_t *bs;
    uint32_t bs_block_size, bs_disk_alignment;
    ring_loop_t *ringloop;

    int wait_state = 0;
    int epoll_fd = 0;
    int listen_fd = 0;
    ring_consumer_t consumer;

    std::unordered_map<int,osd_client_t> clients;
    std::vector<int> read_ready_clients;
    std::vector<int> write_ready_clients;

    // methods

    // event loop, socket read/write
    void loop();
    int handle_epoll_events();
    void read_requests();
    void handle_read(ring_data_t *data, int peer_fd);
    void handle_read_op(osd_client_t *cl);
    void handle_read_reply(osd_client_t *cl);
    void send_replies();
    void make_reply(osd_op_t *op);
    void handle_send(ring_data_t *data, int peer_fd);
    void outbox_push(osd_client_t & cl, osd_op_t *op);

    // peer handling (primary OSD logic)
    void connect_peer(unsigned osd_num, const char *peer_host, int peer_port, std::function<void(int)> callback);
    void handle_connect_result(int peer_fd);
    void stop_client(int peer_fd);
    osd_peer_def_t parse_peer(std::string peer);
    void init_primary();
    void handle_peers();
    void start_pg_peering(int i);

    // op execution
    void exec_op(osd_op_t *cur_op);

    // secondary ops
    void exec_sync_stab_all(osd_op_t *cur_op);
    void exec_show_config(osd_op_t *cur_op);
    void exec_secondary(osd_op_t *cur_op);
    void secondary_op_callback(osd_op_t *cur_op);

    // primary ops
    void exec_primary_read(osd_op_t *cur_op);
    void exec_primary_write(osd_op_t *cur_op);
    void exec_primary_sync(osd_op_t *cur_op);
    void make_primary_reply(osd_op_t *op);
public:
    osd_t(blockstore_config_t & config, blockstore_t *bs, ring_loop_t *ringloop);
    ~osd_t();
    bool shutdown();
};
