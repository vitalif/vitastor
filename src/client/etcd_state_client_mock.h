// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#pragma once

#include "etcd_state_client.h"

struct etcd_mock_key_data_t
{
    std::string value;
    uint64_t mod_revision;
    uint64_t lease_id;
};

struct etcd_mock_request_t
{
    std::string api;
    json11::Json payload;
    int timeout;
    int retries;
    int interval;
    std::function<void(std::string, json11::Json)> callback;
};

struct etcd_state_client_mock_t: public etcd_state_client_t
{
    uint64_t mod_revision = 0;
    bool paused = false;
    std::vector<etcd_mock_request_t> queue;
public:
    std::map<uint64_t, uint64_t> leases;
    std::map<std::string, etcd_mock_key_data_t> data;
    std::string username;
    etcd_state_client_mock_t();
    void set(const std::string& key, json11::Json data, uint64_t mod_revision = 0, uint64_t lease_id = 0);
    void pause();
    void resume(size_t n = 0);
    void etcd_call_oneshot(const std::string & etcd_address, const std::string & api, json11::Json payload, int timeout, std::function<void(std::string, json11::Json)> callback) override;
    void etcd_call(const std::string & api, json11::Json payload, int timeout, int retries, int interval, std::function<void(std::string, json11::Json)> callback) override;
    void etcd_call_nopause(std::string api, json11::Json payload, int timeout, int retries, int interval, std::function<void(std::string, json11::Json)> callback);
    void etcd_add_watch(json11::Json watch) override;
    std::string get_username() override;
    void load_global_config() override;
    void load_pgs() override;
};
