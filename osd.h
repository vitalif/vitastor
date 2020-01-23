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

#include <set>
#include <deque>

#include "blockstore.h"
#include "ringloop.h"
#include "osd_ops.h"

#include "sparsepp/sparsepp/spp.h"

#define STRIPE_NUM(stripe) ((stripe) >> 4)
#define STRIPE_REPLICA(stripe) ((stripe) & 0xf)

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

//#define OSD_STUB

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

#define PEER_CONNECTING 1
#define PEER_CONNECTED 2

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

struct osd_obj_loc_t
{
    uint64_t role;
    uint64_t osd_num;
    bool stable;
};

inline bool operator < (const osd_obj_loc_t &a, const osd_obj_loc_t &b)
{
    return a.role < b.role || a.role == b.role && a.osd_num < b.osd_num;
}

struct osd_obj_state_t
{
    std::vector<osd_obj_loc_t> loc;
    uint64_t state = 0;
    uint64_t object_count = 0;
};

struct osd_ver_override_t
{
    uint64_t max_ver;
    uint64_t target_ver;
};

inline bool operator < (const osd_obj_state_t &a, const osd_obj_state_t &b)
{
    return a.loc < b.loc;
}

namespace std
{
    template<> struct hash<osd_obj_state_t>
    {
        inline size_t operator()(const osd_obj_state_t &s) const
        {
            size_t seed = 0;
            for (int i = 0; i < s.loc.size(); i++)
            {
                // Copy-pasted from spp::hash_combine()
                seed ^= (s.loc[i].role + 0xc6a4a7935bd1e995 + (seed << 6) + (seed >> 2));
                seed ^= (s.loc[i].osd_num + 0xc6a4a7935bd1e995 + (seed << 6) + (seed >> 2));
                seed ^= ((s.loc[i].stable ? 1 : 0) + 0xc6a4a7935bd1e995 + (seed << 6) + (seed >> 2));
            }
            return seed;
        }
    };
}

// Placement group states
// Exactly one of these:
#define PG_OFFLINE (1<<0)
#define PG_PEERING (1<<1)
#define PG_INCOMPLETE (1<<2)
#define PG_ACTIVE (1<<3)
// Plus any of these:
#define PG_HAS_UNFOUND (1<<4)
#define PG_HAS_DEGRADED (1<<5)
#define PG_HAS_MISPLACED (1<<6)

// OSD object states
#define OBJ_CLEAN 0x01
#define OBJ_MISPLACED 0x02
#define OBJ_DEGRADED 0x03
#define OBJ_INCOMPLETE 0x04
#define OBJ_NONSTABILIZED 0x10000
#define OBJ_UNDERWRITTEN 0x20000
#define OBJ_OVERCOPIED 0x40000
#define OBJ_BUGGY 0x80000

class osd_t;

struct osd_pg_peering_state_t
{
    osd_t* self;
    // FIXME: add types for pg_num and osd_num?
    uint64_t pg_num;
    std::unordered_map<uint64_t, osd_op_t*> list_ops;
    int list_done = 0;
};

struct osd_pg_t
{
    int state;
    uint64_t pg_size = 3, pg_minsize = 2;
    uint64_t pg_num;
    // target_set = (role => osd_num). role starts from zero
    std::vector<uint64_t> target_set;
    // moved object map. by default, each object is considered to reside on the target_set.
    // this map stores all objects that differ.
    // this map may consume up to ~ (raw storage / object size) * 24 bytes in the worst case scenario
    // which is up to ~192 MB per 1 TB in the worst case scenario
    std::set<osd_obj_state_t> state_dict;
    spp::sparse_hash_map<object_id, const osd_obj_state_t*> obj_states;
    spp::sparse_hash_map<object_id, osd_ver_override_t> ver_override;
    osd_pg_peering_state_t *peering_state = NULL;
};

struct obj_ver_role
{
    object_id oid;
    uint64_t version;
    uint64_t osd_num;
    bool is_stable;
};

inline bool operator < (const obj_ver_role & a, const obj_ver_role & b)
{
    return a.oid < b.oid ||
        // object versions go in descending order
        a.oid == b.oid && a.version > b.version ||
        a.oid == b.oid && a.version == b.version ||
        a.oid == b.oid && a.version == b.version && a.osd_num < b.osd_num;
}

// Max 64 replicas
#define STRIPE_MASK 0x3F
#define STRIPE_SHIFT 6

struct osd_obj_state_check_t
{
    int start = 0;
    object_id oid = { 0 };
    uint64_t max_ver = 0;
    uint64_t target_ver = 0;
    uint64_t n_copies = 0, has_roles = 0, n_roles = 0, n_stable = 0, n_matched = 0;
    bool is_buggy = false;
    osd_obj_state_t state_obj;
};

struct osd_peer_def_t
{
    uint64_t osd_num = 0;
    std::string addr;
    int port = 0;
    time_t last_connect_attempt = 0;
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
    std::vector<osd_pg_t> pgs;
    int peering_state = 0;
    unsigned pg_count = 0;

    // client & peer I/O

    bool stopping = false;
    int inflight_ops = 0;
    blockstore_t *bs;
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
    void calc_object_states(osd_pg_t &pg);
    void remember_object(osd_pg_t &pg, osd_obj_state_check_t &st, std::vector<obj_ver_role> &all, int end);

    // op execution
    void exec_op(osd_op_t *cur_op);
    void exec_sync_stab_all(osd_op_t *cur_op);
    void exec_show_config(osd_op_t *cur_op);
    void exec_secondary(osd_op_t *cur_op);
    void secondary_op_callback(osd_op_t *cur_op);
public:
    osd_t(blockstore_config_t & config, blockstore_t *bs, ring_loop_t *ringloop);
    ~osd_t();
    bool shutdown();
};
