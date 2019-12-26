#pragma once

#include <sys/types.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <malloc.h>
#include <arpa/inet.h>
#include <malloc.h>

#include <unordered_map>
#include <deque>

#include "blockstore.h"
#include "ringloop.h"
#include "osd_ops.h"

#include "sparsepp/sparsepp/spp.h"

#define STRIPE_NUM(stripe) ((stripe) >> 4)
#define STRIPE_REPLICA(stripe) ((stripe) & 0xf)

#define OSD_OP_IN 0
#define OSD_OP_OUT 1

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

    ~osd_op_t();
};

struct osd_client_t
{
    sockaddr_in peer_addr;
    socklen_t peer_addr_size;
    int peer_fd;
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

    // Completed operations to send replies back to the client
    std::deque<osd_op_t*> completions;

    // Write state
    osd_op_t *write_op = NULL;
    iovec write_iov;
    msghdr write_msg;
    void *write_buf = NULL;
    int write_remaining = 0;
    int write_state = 0;
};

struct osd_pg_role_t
{
    int role;
    uint64_t osd_num;
};

typedef std::vector<osd_pg_role_t> osd_acting_set_t;

namespace std
{
    template<> struct hash<osd_acting_set_t>
    {
        inline size_t operator()(const osd_acting_set_t &s) const
        {
            size_t seed = 0;
            for (int i = 0; i < s.size(); i++)
            {
                // Copy-pasted from spp::hash_combine()
                seed ^= (s[i].role + 0xc6a4a7935bd1e995 + (seed << 6) + (seed >> 2));
                seed ^= (s[i].osd_num + 0xc6a4a7935bd1e995 + (seed << 6) + (seed >> 2));
            }
            return seed;
        }
    };
}

#define PG_ST_OFFLINE 1
#define PG_ST_PEERING 2
#define PG_ST_INCOMPLETE 3
#define PG_ST_DEGRADED 4
#define PG_ST_MISPLACED 5
#define PG_ST_ACTIVE 6

struct osd_pg_t
{
    int state;
    unsigned num;
    std::vector<osd_pg_role_t> target_set;
    // moved object map. by default, each object is considered to reside on the target_set.
    // this map stores all objects that differ.
    // this map may consume up to ~ (raw storage / object size) * 24 bytes in the worst case scenario
    // which is up to ~192 MB per 1 TB in the worst case scenario
    std::unordered_map<osd_acting_set_t, int> acting_set_ids;
    std::map<int, osd_acting_set_t> acting_sets;
    spp::sparse_hash_map<object_id, int> object_map;
};

class osd_t
{
    // config

    uint64_t osd_num = 0;
    blockstore_config_t config;
    std::string bind_address;
    int bind_port, listen_backlog;
    int client_queue_depth = 128;
    bool allow_test_ops = true;

    // peer OSDs

    std::map<uint64_t, int> osd_peer_fds;
    std::vector<osd_pg_t> pgs;
    unsigned pg_count;

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

    void loop();
    int handle_epoll_events();
    void stop_client(int peer_fd);
    void read_requests();
    void handle_read(ring_data_t *data, int peer_fd);
    void handle_read_op(osd_client_t *cl);
    void handle_read_reply(osd_client_t *cl);
    void send_replies();
    void make_reply(osd_op_t *op);
    void handle_send(ring_data_t *data, int peer_fd);

    void handle_reply(osd_op_t *cur_op);
    void enqueue_op(osd_op_t *cur_op);
    void secondary_op_callback(osd_op_t *cur_op);
public:
    osd_t(blockstore_config_t & config, blockstore_t *bs, ring_loop_t *ringloop);
    ~osd_t();
    bool shutdown();
};
