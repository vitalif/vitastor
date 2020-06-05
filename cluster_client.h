#pragma once

#include "messenger.h"
#include "etcd_state_client.h"

#define MIN_BLOCK_SIZE 4*1024
#define MAX_BLOCK_SIZE 128*1024*1024
#define DEFAULT_BLOCK_SIZE 128*1024
#define DEFAULT_PG_STRIPE_SIZE 4*1024*1024
#define DEFAULT_DISK_ALIGNMENT 4096
#define DEFAULT_BITMAP_GRANULARITY 4096

struct cluster_op_t;

struct cluster_op_part_t
{
    cluster_op_t *parent;
    uint64_t offset;
    uint32_t len;
    pg_num_t pg_num;
    osd_num_t osd_num;
    void *buf;
    bool sent;
    bool done;
    osd_op_t op;
};

struct cluster_op_t
{
    uint64_t opcode; // OSD_OP_READ, OSD_OP_WRITE
    uint64_t inode;
    uint64_t offset;
    uint64_t len;
    int retval;
    void *buf;
    std::function<void(cluster_op_t*)> callback;
protected:
    bool needs_reslice = false;
    int sent_count = 0, done_count = 0;
    std::vector<cluster_op_part_t> parts;
    friend class cluster_client_t;
};

class cluster_client_t
{
    timerfd_manager_t *tfd;
    ring_loop_t *ringloop;

    uint64_t pg_part_count = 2;
    uint64_t pg_stripe_size = 0;
    uint64_t bs_block_size = 0;
    uint64_t bs_disk_alignment = 0;
    uint64_t bs_bitmap_granularity = 0;
    uint64_t pg_count = 0;
    int log_level;

    uint64_t op_id = 1;
    etcd_state_client_t st_cli;
    osd_messenger_t msgr;
    std::set<cluster_op_t*> sent_ops, unsent_ops;

public:
    cluster_client_t(ring_loop_t *ringloop, timerfd_manager_t *tfd);
    void execute(cluster_op_t *op);

protected:
    void continue_ops();
    void on_load_config_hook(json11::Json::object & cfg);
    void on_load_pgs_hook(bool success);
    void on_change_hook(json11::Json::object & changes);
    void on_change_osd_state_hook(uint64_t peer_osd);
    bool try_send(cluster_op_t *op, cluster_op_part_t *part);
    void handle_op_part(cluster_op_part_t *part);
};
