// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#pragma once

#include <set>

#include "json11/json11.hpp"
#include "osd_id.h"
#include "timerfd_manager.h"

#define ETCD_CONFIG_WATCH_ID 1
#define ETCD_PG_STATE_WATCH_ID 2
#define ETCD_PG_HISTORY_WATCH_ID 3
#define ETCD_OSD_STATE_WATCH_ID 4
#define ETCD_TOTAL_WATCHES 4

#define DEFAULT_BLOCK_SIZE 128*1024
#define MIN_DATA_BLOCK_SIZE 4*1024
#define MAX_DATA_BLOCK_SIZE 128*1024*1024
#define DEFAULT_BITMAP_GRANULARITY 4096

#define IMMEDIATE_NONE 0
#define IMMEDIATE_SMALL 1
#define IMMEDIATE_ALL 2

struct etcd_kv_t
{
    std::string key;
    json11::Json value;
    uint64_t mod_revision = 0;
};

struct pg_config_t
{
    bool config_exists, history_exists, state_exists;
    osd_num_t primary;
    std::vector<osd_num_t> target_set;
    std::vector<std::vector<osd_num_t>> target_history;
    std::vector<osd_num_t> all_peers;
    bool pause;
    osd_num_t cur_primary;
    int cur_state;
    uint64_t epoch;
    uint64_t next_scrub;
};

struct pool_config_t
{
    bool exists;
    pool_id_t id;
    std::string name;
    uint64_t scheme;
    uint64_t pg_size, pg_minsize, parity_chunks;
    uint32_t data_block_size, bitmap_granularity, immediate_commit;
    uint64_t pg_count;
    uint64_t real_pg_count;
    std::string failure_domain;
    uint64_t max_osd_combinations;
    uint64_t pg_stripe_size;
    std::map<pg_num_t, pg_config_t> pg_config;
    uint64_t scrub_interval;
    std::vector<std::string> osd_tags;
    std::vector<std::string> primary_affinity_tags;
    std::string root_node;
};

struct inode_config_t
{
    uint64_t num = 0;
    std::string name;
    uint64_t size = 0;
    inode_t parent_id = 0;
    bool readonly = false;
    // Arbitrary metadata
    json11::Json meta;
    // Change revision of the metadata in etcd
    uint64_t mod_revision = 0;
};

struct inode_watch_t
{
    std::string name;
    inode_config_t cfg = {};
};

struct http_co_t;

struct etcd_state_client_t
{
protected:
    std::vector<std::string> local_ips;
    std::vector<std::string> etcd_addresses;
    std::vector<std::string> etcd_local;
    std::string selected_etcd_address;
    std::vector<std::string> addresses_to_try;
    std::vector<inode_watch_t*> watches;
    http_co_t *etcd_watch_ws = NULL, *keepalive_client = NULL;
    int ws_keepalive_timer = -1;
    int ws_alive = 0;
    bool rand_initialized = false;
    void add_etcd_url(std::string);
    void pick_next_etcd();
public:
    int etcd_keepalive_timeout = 30;
    int etcd_ws_keepalive_interval = 30;
    int max_etcd_attempts = 5;
    int etcd_quick_timeout = 1000;
    int etcd_slow_timeout = 5000;
    uint64_t global_block_size = DEFAULT_BLOCK_SIZE;
    uint32_t global_bitmap_granularity = DEFAULT_BITMAP_GRANULARITY;
    uint32_t global_immediate_commit = IMMEDIATE_NONE;

    std::string etcd_prefix;
    int log_level = 0;
    timerfd_manager_t *tfd = NULL;

    int etcd_watches_initialised = 0;
    uint64_t etcd_watch_revision = 0;
    std::map<pool_id_t, pool_config_t> pool_config;
    std::map<osd_num_t, json11::Json> peer_states;
    std::set<osd_num_t> seen_peers;
    std::map<inode_t, inode_config_t> inode_config;
    std::map<std::string, inode_t> inode_by_name;

    std::function<void(std::map<std::string, etcd_kv_t> &)> on_change_hook;
    std::function<void(json11::Json::object &)> on_load_config_hook;
    std::function<json11::Json()> load_pgs_checks_hook;
    std::function<void(bool)> on_load_pgs_hook;
    std::function<void(pool_id_t, pg_num_t)> on_change_pg_history_hook;
    std::function<void(osd_num_t)> on_change_osd_state_hook;
    std::function<void()> on_reload_hook;
    std::function<void(inode_t, bool)> on_inode_change_hook;
    std::function<void(http_co_t *)> on_start_watcher_hook;

    json11::Json::object serialize_inode_cfg(inode_config_t *cfg);
    etcd_kv_t parse_etcd_kv(const json11::Json & kv_json);
    std::vector<std::string> get_addresses();
    void etcd_call_oneshot(std::string etcd_address, std::string api, json11::Json payload, int timeout, std::function<void(std::string, json11::Json)> callback);
    void etcd_call(std::string api, json11::Json payload, int timeout, int retries, int interval, std::function<void(std::string, json11::Json)> callback);
    void etcd_txn(json11::Json txn, int timeout, int retries, int interval, std::function<void(std::string, json11::Json)> callback);
    void etcd_txn_slow(json11::Json txn, std::function<void(std::string, json11::Json)> callback);
    void start_etcd_watcher();
    void stop_ws_keepalive();
    void start_ws_keepalive();
    void load_global_config();
    void load_pgs();
    void reset_pg_exists();
    void clean_nonexistent_pgs();
    void parse_state(const etcd_kv_t & kv);
    void parse_config(const json11::Json & config);
    uint32_t parse_immediate_commit_string(const std::string immediate_commit_str);
    void insert_inode_config(const inode_config_t & cfg);
    inode_watch_t* watch_inode(std::string name);
    void close_watch(inode_watch_t* watch);
    int address_count();
    ~etcd_state_client_t();
};
