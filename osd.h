#pragma once

#include <arpa/inet.h>

#include <unordered_map>

#include "ringloop.h"
#include "osd_ops.h"

struct osd_op_t
{
    int peer_fd;
    union
    {
        osd_any_op_t op;
        uint8_t op_buf[OSD_OP_PACKET_SIZE] = { 0 };
    };
    union
    {
        osd_any_reply_t reply;
        uint8_t reply_buf[OSD_REPLY_PACKET_SIZE] = { 0 };
    };
    blockstore_operation bs_op;
    void *buf = NULL;

    ~osd_op_t()
    {
        if (buf)
            free(buf);
    }
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
    iovec read_iov;
    msghdr read_msg;
    void *read_buf = NULL;
    int read_remaining = 0;
    int read_state = 0;

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

class osd_t
{
    // config

    std::string bind_address;
    int bind_port, listen_backlog;
    int client_queue_depth = 128;

    // fields

    blockstore *bs;
    ring_loop_t *ringloop;

    int wait_state = 0;
    int epoll_fd = 0;
    int listen_fd = 0;
    ring_consumer_t consumer;

    std::unordered_map<int,osd_client_t> clients;
    std::vector<int> read_ready_clients;
    std::vector<int> write_ready_clients;

    void loop();
    int handle_epoll_events();
    void stop_client(int peer_fd);
    void read_requests();
    void handle_read(ring_data_t *data, int peer_fd);
    void enqueue_op(osd_op_t *cur_op);
    void send_replies();
    void make_reply(osd_op_t *op);
    void handle_send(ring_data_t *data, int peer_fd);
public:
    osd_t(blockstore_config_t & config, blockstore *bs, ring_loop_t *ringloop);
    ~osd_t();
    bool shutdown();
};
