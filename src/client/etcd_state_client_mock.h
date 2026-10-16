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

class ring_loop_i;

struct etcd_state_client_mock_t;

// Mock etcd to connect multiple etcd_state_client_mock_t's to.
struct etcd_mock_t
{
    uint64_t mod_revision = 0;
    std::map<uint64_t, uint64_t> leases;
    std::map<std::string, etcd_mock_key_data_t> data;
    std::vector<etcd_state_client_mock_t*> clients;
    // When set, every response callback and watcher notification is delivered
    // through this ringloop (set_immediate), i.e. at its top level, never on the
    // caller's stack.  A real etcd answers over the network, so its reply and
    // watch notifications can never reenter the code that issued the request;
    // synchronous delivery lets report_pg_state -> etcd_txn -> apply_pg_config
    // -> stop_pg recurse into itself and free PGs under the iterator.  The data
    // map itself is still mutated synchronously - that is the etcd server, and
    // tests read it directly.  NULL keeps the old synchronous behavior.
    ring_loop_i *ringloop = NULL;

    etcd_mock_t();
    void register_client(etcd_state_client_mock_t *client);
    void unregister_client(etcd_state_client_mock_t *client);
    // Write a key and notify watchers. Raw value, like a real put would.
    void set(const std::string& key, const std::string& value, uint64_t mod_revision = 0, uint64_t lease_id = 0);
    // Execute a /kv/txn or /lease/grant request against the data
    void etcd_call(const std::string & api_name, json11::Json payload, std::function<void(std::string, json11::Json)> callback, etcd_state_client_mock_t *from = NULL);
    // Deliver changes to all watchers (now or via the ringloop, see above)
    void notify(std::map<std::string, etcd_kv_t> & changes);
    // Run a response callback now or at the ringloop's top level
    void deliver(std::function<void(std::string, json11::Json)> cb, std::string err, json11::Json res);
    void notify_now(std::map<std::string, etcd_kv_t> & changes);
};

struct etcd_state_client_mock_t: public etcd_state_client_t
{
    // The client doesn't own the store; the test does
    etcd_mock_t *store = NULL;
    bool paused = false;
    std::vector<etcd_mock_request_t> queue;
    std::string username;
    etcd_state_client_mock_t(etcd_mock_t & store);
    ~etcd_state_client_mock_t();
    void set(const std::string& key, json11::Json data, uint64_t mod_revision = 0, uint64_t lease_id = 0);
    void pause();
    void resume(size_t n = 0);
    void etcd_call_oneshot(const std::string & etcd_address, const std::string & api, json11::Json payload, int timeout, std::function<void(std::string, json11::Json)> callback) override;
    void etcd_call(const std::string & api_name, json11::Json payload, int timeout, int retries, int interval, std::function<void(std::string, json11::Json)> callback) override;
    void etcd_call_nopause(std::string api, json11::Json payload, int timeout, int retries, int interval, std::function<void(std::string, json11::Json)> callback);
    // The store applies a change to the client's own view of the cluster
    void mock_apply_changes(std::map<std::string, etcd_kv_t> & changes);
    void etcd_add_watch(json11::Json watch) override;
    std::string get_username() override;
    void load_global_config() override;
    void load_pgs() override;
};
