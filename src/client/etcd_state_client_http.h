// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#pragma once

#include "etcd_state_client.h"

struct http_context_t;

struct __attribute__((visibility("default"))) etcd_state_client_http_t: public etcd_state_client_t
{
protected:
    timerfd_manager_t *tfd = NULL;
    std::string selected_etcd_address;
    std::vector<std::string> addresses_to_try;
    int ws_keepalive_timer = -1;
    int ws_alive = 0;
    bool rand_initialized = false;
    int etcd_watches_initialised = 0;
    timespec etcd_last_reload = {};
    int load_pgs_timer_id = -1;
    http_co_t *keepalive_client = NULL;
    http_co_t *etcd_watch_ws = NULL;
    http_context_t *http_ctx = NULL;

    void pick_next_etcd();
    void start_etcd_watcher();
    void stop_ws_keepalive();
    void start_ws_keepalive();
    http_context_t *get_http_ctx();
public:
    etcd_state_client_http_t(timerfd_manager_t *tfd);
    void etcd_call_oneshot(std::string etcd_address, std::string api, json11::Json payload, int timeout, std::function<void(std::string, json11::Json)> callback) override;
    void etcd_call(std::string api, json11::Json payload, int timeout, int retries, int interval, std::function<void(std::string, json11::Json)> callback) override;
    void etcd_add_watch(json11::Json watch) override;
    void load_global_config() override;
    void load_pgs() override;
    void parse_config(const json11::Json & config) override;
    ~etcd_state_client_http_t();
};
