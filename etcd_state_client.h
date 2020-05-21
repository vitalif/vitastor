#pragma once

#include "http_client.h"
#include "timerfd_manager.h"

#define ETCD_CONFIG_WATCH_ID 1
#define ETCD_PG_STATE_WATCH_ID 2
#define ETCD_PG_HISTORY_WATCH_ID 3
#define ETCD_OSD_STATE_WATCH_ID 4

#define MAX_ETCD_ATTEMPTS 5
#define ETCD_SLOW_TIMEOUT 5000
#define ETCD_QUICK_TIMEOUT 1000

struct pg_config_t
{
    bool exists;
    osd_num_t primary;
    std::vector<osd_num_t> target_set;
    std::vector<std::vector<osd_num_t>> target_history;
    std::vector<osd_num_t> all_peers;
    bool pause;
    osd_num_t cur_primary;
    int cur_state;
};

struct json_kv_t
{
    std::string key;
    json11::Json value;
};

struct etcd_state_client_t
{
    // FIXME Allow multiple etcd addresses and select random address
    std::string etcd_address, etcd_prefix, etcd_api_path;
    int log_level = 0;
    timerfd_manager_t *tfd = NULL;

    int etcd_watches_initialised = 0;
    uint64_t etcd_watch_revision = 0;
    websocket_t *etcd_watch_ws = NULL;
    std::map<pg_num_t, pg_config_t> pg_config;
    std::map<osd_num_t, json11::Json> peer_states;

    std::function<void(json11::Json::object &)> on_change_hook;
    std::function<void(json11::Json::object &)> on_load_config_hook;
    std::function<json11::Json()> load_pgs_checks_hook;
    std::function<void(bool)> on_load_pgs_hook;

    json_kv_t parse_etcd_kv(const json11::Json & kv_json);
    void etcd_call(std::string api, json11::Json payload, int timeout, std::function<void(std::string, json11::Json)> callback);
    void etcd_txn(json11::Json txn, int timeout, std::function<void(std::string, json11::Json)> callback);
    void start_etcd_watcher();
    void load_global_config();
    void load_pgs();
    void parse_state(const std::string & key, const json11::Json & value);
};
