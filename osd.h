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
#include "epoll_manager.h"
#include "osd_peering_pg.h"
#include "messenger.h"
#include "etcd_state_client.h"

#define OSD_LOADING_PGS 0x01
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
    int print_stats_interval = 3;
    int immediate_commit = IMMEDIATE_NONE;
    int autosync_interval = DEFAULT_AUTOSYNC_INTERVAL; // sync every 5 seconds
    int recovery_queue_depth = DEFAULT_RECOVERY_QUEUE;
    int log_level = 0;

    // cluster state

    etcd_state_client_t st_cli;
    osd_messenger_t c_cli;
    int etcd_failed_attempts = 0;
    std::string etcd_lease_id;
    json11::Json self_state;
    bool loading_peer_config = false;
    std::set<pg_num_t> pg_state_dirty;
    bool pg_config_applied = false;
    bool etcd_reporting_pg_state = false;
    bool etcd_reporting_stats = false;

    // peers and PGs

    std::map<pg_num_t, pg_t> pgs;
    std::set<pg_num_t> dirty_pgs;
    uint64_t misplaced_objects = 0, degraded_objects = 0, incomplete_objects = 0;
    int peering_state = 0;
    unsigned pg_count = 0;
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
    epoll_manager_t *epmgr = NULL;

    int listening_port = 0;
    int listen_fd = 0;
    ring_consumer_t consumer;

    // op statistics
    osd_op_stats_t prev_stats;
    const char* recovery_stat_names[2] = { "degraded", "misplaced" };
    uint64_t recovery_stat_count[2][2] = { 0 };
    uint64_t recovery_stat_bytes[2][2] = { 0 };

    // cluster connection
    void parse_config(blockstore_config_t & config);
    void init_cluster();
    void on_change_osd_state_hook(osd_num_t peer_osd);
    void on_change_pg_history_hook(pg_num_t pg_num);
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

    // event loop, socket read/write
    void loop();

    // peer handling (primary OSD logic)
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
    void cancel_primary_write(osd_op_t *cur_op);
    void continue_primary_sync(osd_op_t *cur_op);
    void continue_primary_del(osd_op_t *cur_op);
    bool check_write_queue(osd_op_t *cur_op, pg_t & pg);
    void remove_object_from_state(object_id & oid, pg_osd_set_state_t *object_state, pg_t &pg);
    bool remember_unstable_write(osd_op_t *cur_op, pg_t & pg, pg_osd_set_t & loc_set, int base_state);
    void handle_primary_subop(osd_op_t *subop, osd_op_t *cur_op);
    void handle_primary_bs_subop(osd_op_t *subop);
    void add_bs_subop_stats(osd_op_t *subop);
    void pg_cancel_write_queue(pg_t & pg, osd_op_t *first_op, object_id oid, int retval);
    void submit_primary_subops(int submit_type, uint64_t op_version, int pg_size, const uint64_t* osd_set, osd_op_t *cur_op);
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
