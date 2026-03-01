// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#pragma once

#include "etcd_state_client.h"

struct http_context_t;

struct __attribute__((visibility("default"))) etcd_state_client_http_t: public etcd_state_client_t
{
protected:
    timerfd_manager_t *tfd = NULL;
    int ws_keepalive_timer = -1;
    int ws_alive = 0;
    bool rand_initialized = false;
    int etcd_watches_initialised = 0;
    timespec etcd_last_reload = {};
    int load_pgs_timer_id = -1;
    http_co_t *keepalive_client = NULL;
    http_co_t *etcd_watch_ws = NULL;
    http_context_t *http_ctx = NULL;
    size_t local_to_try = 0;
    std::vector<http_url_t> etcd_urls_to_try;
    http_url_t selected_etcd_url;
    size_t resolve_count = 0;
    std::vector<std::function<void()>> on_resolve_queue;

    void pick_next_etcd(std::function<void()> cb);
    void pick_next_etcd_on_resolve();
    void start_etcd_watcher();
    void start_etcd_watcher_selected();
    void stop_ws_keepalive();
    void start_ws_keepalive();
    http_context_t *get_http_ctx();
public:
    etcd_state_client_http_t(timerfd_manager_t *tfd);
    void etcd_call_oneshot(const std::string & etcd_url, const std::string & api, json11::Json payload,
        int timeout, std::function<void(std::string, json11::Json)> callback) override;
    void etcd_call(const std::string & api, json11::Json payload, int timeout,
        int retries, int interval, std::function<void(std::string, json11::Json)> callback) override;
    void etcd_call_selected(const std::string & api, json11::Json payload, int timeout,
        int retries, int interval, std::function<void(std::string, json11::Json)> callback);
    void etcd_add_watch(json11::Json watch) override;
    void load_global_config() override;
    void load_pgs() override;
    void parse_config(const json11::Json & config) override;
    ~etcd_state_client_http_t();
};
