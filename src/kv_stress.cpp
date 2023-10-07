// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// Vitastor shared key/value database stress tester / benchmark

#define _XOPEN_SOURCE
#include <limits.h>

#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <unistd.h>
#include <fcntl.h>
//#include <signal.h>

#include "epoll_manager.h"
#include "str_util.h"
#include "kv_db.h"

const char *exe_name = NULL;

struct kv_test_listing_t
{
    void *handle = NULL;
    std::string next_after;
    std::set<std::string> inflights;
};

class kv_test_t
{
public:
    // Config
    json11::Json::object kv_cfg;
    uint64_t inode_id = 0;
    uint64_t op_count = 1000000;
    uint64_t parallelism = 4;
    uint64_t reopen_prob = 1;
    uint64_t get_prob = 30000;
    uint64_t add_prob = 20000;
    uint64_t update_prob = 20000;
    uint64_t del_prob = 30000;
    uint64_t list_prob = 300;
    uint64_t min_key_len = 10;
    uint64_t max_key_len = 70;
    uint64_t min_value_len = 50;
    uint64_t max_value_len = 300;
    // FIXME: Multiple clients
    // FIXME: Print op statistics

    // State
    kv_dbw_t *db = NULL;
    ring_loop_t *ringloop = NULL;
    epoll_manager_t *epmgr = NULL;
    cluster_client_t *cli = NULL;
    ring_consumer_t consumer;
    bool finished = false;
    uint64_t total_prob = 0;
    uint64_t ops_done = 0;
    uint64_t get_done = 0, add_done = 0, update_done = 0, del_done = 0, list_done = 0;
    int in_progress = 0;
    bool reopening = false;
    std::set<kv_test_listing_t*> listings;
    std::set<std::string> changing_keys;
    std::map<std::string, std::string> values;

    ~kv_test_t();

    static json11::Json::object parse_args(int narg, const char *args[]);
    void run(json11::Json cfg);
    void loop();
    void start_change(const std::string & key);
    void stop_change(const std::string & key);
};

kv_test_t::~kv_test_t()
{
    if (db)
        delete db;
    if (cli)
    {
        cli->flush();
        delete cli;
    }
    if (epmgr)
        delete epmgr;
    if (ringloop)
        delete ringloop;
}

json11::Json::object kv_test_t::parse_args(int narg, const char *args[])
{
    json11::Json::object cfg;
    for (int i = 1; i < narg; i++)
    {
        if (!strcmp(args[i], "-h") || !strcmp(args[i], "--help"))
        {
            printf(
                "Vitastor Key/Value DB stress tester / benchmark\n"
                "(c) Vitaliy Filippov, 2023+ (VNPL-1.1)\n"
                "\n"
                "USAGE: %s [--etcd_address ADDR]\n",
                exe_name
            );
            exit(0);
        }
        else if (args[i][0] == '-' && args[i][1] == '-')
        {
            const char *opt = args[i]+2;
            cfg[opt] = !strcmp(opt, "json") || i == narg-1 ? "1" : args[++i];
        }
    }
    return cfg;
}

void kv_test_t::run(json11::Json cfg)
{
    srand48(time(NULL));
    inode_id = INODE_WITH_POOL(cfg["pool_id"].uint64_value(), cfg["inode_id"].uint64_value());
    if (cfg["op_count"].uint64_value() > 0)
        op_count = cfg["op_count"].uint64_value();
    if (cfg["parallelism"].uint64_value() > 0)
        parallelism = cfg["parallelism"].uint64_value();
    if (!cfg["reopen_prob"].is_null())
        reopen_prob = cfg["reopen_prob"].uint64_value();
    if (!cfg["get_prob"].is_null())
        get_prob = cfg["get_prob"].uint64_value();
    if (!cfg["add_prob"].is_null())
        add_prob = cfg["add_prob"].uint64_value();
    if (!cfg["update_prob"].is_null())
        update_prob = cfg["update_prob"].uint64_value();
    if (!cfg["del_prob"].is_null())
        del_prob = cfg["del_prob"].uint64_value();
    if (!cfg["list_prob"].is_null())
        list_prob = cfg["list_prob"].uint64_value();
    if (!cfg["min_key_len"].is_null())
        min_key_len = cfg["min_key_len"].uint64_value();
    if (cfg["max_key_len"].uint64_value() > 0)
        max_key_len = cfg["max_key_len"].uint64_value();
    if (!cfg["min_value_len"].is_null())
        min_value_len = cfg["min_value_len"].uint64_value();
    if (cfg["max_value_len"].uint64_value() > 0)
        max_value_len = cfg["max_value_len"].uint64_value();
    if (!cfg["kv_memory_limit"].is_null())
        kv_cfg["kv_memory_limit"] = cfg["kv_memory_limit"];
    if (!cfg["kv_evict_max_misses"].is_null())
        kv_cfg["kv_evict_max_misses"] = cfg["kv_evict_max_misses"];
    if (!cfg["kv_evict_attempts_per_level"].is_null())
        kv_cfg["kv_evict_attempts_per_level"] = cfg["kv_evict_attempts_per_level"];
    if (!cfg["kv_evict_unused_age"].is_null())
        kv_cfg["kv_evict_unused_age"] = cfg["kv_evict_unused_age"];
    total_prob = reopen_prob+get_prob+add_prob+update_prob+del_prob+list_prob;
    // Create client
    ringloop = new ring_loop_t(512);
    epmgr = new epoll_manager_t(ringloop);
    cli = new cluster_client_t(ringloop, epmgr->tfd, cfg);
    db = new kv_dbw_t(cli);
    // Load image metadata
    while (!cli->is_ready())
    {
        ringloop->loop();
        if (cli->is_ready())
            break;
        ringloop->wait();
    }
    // Run
    reopening = true;
    db->open(inode_id, kv_cfg, [this](int res)
    {
        reopening = false;
        if (res < 0)
        {
            printf("ERROR: Open index: %d (%s)\n", res, strerror(-res));
            exit(1);
        }
        printf("Index opened\n");
        ringloop->wakeup();
    });
    consumer.loop = [this]() { loop(); };
    ringloop->register_consumer(&consumer);
    while (!finished)
    {
        ringloop->loop();
        if (!finished)
            ringloop->wait();
    }
    ringloop->unregister_consumer(&consumer);
    // Destroy the client
    delete db;
    db = NULL;
    cli->flush();
    delete cli;
    delete epmgr;
    delete ringloop;
    cli = NULL;
    epmgr = NULL;
    ringloop = NULL;
}

static const char *base64_chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789@+/";

std::string random_str(int len)
{
    std::string str;
    str.resize(len);
    for (int i = 0; i < len; i++)
    {
        str[i] = base64_chars[lrand48() % 64];
    }
    return str;
}

void kv_test_t::loop()
{
    if (reopening)
    {
        return;
    }
    if (ops_done >= op_count)
    {
        finished = true;
    }
    while (!finished && in_progress < parallelism)
    {
        uint64_t dice = (lrand48() % total_prob);
        if (dice < reopen_prob)
        {
            reopening = true;
            db->close([this]()
            {
                printf("Index closed\n");
                db->open(inode_id, kv_cfg, [this](int res)
                {
                    reopening = false;
                    if (res < 0)
                    {
                        printf("ERROR: Reopen index: %d (%s)\n", res, strerror(-res));
                        finished = true;
                        return;
                    }
                    printf("Index reopened\n");
                    ringloop->wakeup();
                });
            });
            return;
        }
        else if (dice < reopen_prob+get_prob)
        {
            // get existing
            auto key = random_str(max_key_len);
            auto k_it = values.lower_bound(key);
            if (k_it == values.end())
                continue;
            key = k_it->first;
            if (changing_keys.find(key) != changing_keys.end())
                continue;
            in_progress++;
            printf("get %s\n", key.c_str());
            db->get(key, [this, key](int res, const std::string & value)
            {
                ops_done++;
                in_progress--;
                auto it = values.find(key);
                if (res != (it == values.end() ? -ENOENT : 0))
                    printf("ERROR: get %s: %d (%s)\n", key.c_str(), res, strerror(-res));
                else if (it != values.end() && value != it->second)
                    printf("ERROR: get %s: mismatch: %s vs %s\n", key.c_str(), value.c_str(), it->second.c_str());
                else
                    get_done++;
                ringloop->wakeup();
            });
        }
        else if (dice < reopen_prob+get_prob+add_prob+update_prob)
        {
            bool is_add = false;
            std::string key;
            if (dice < reopen_prob+get_prob+add_prob)
            {
                // add
                is_add = true;
                uint64_t key_len = min_key_len + (max_key_len > min_key_len ? lrand48() % (max_key_len-min_key_len) : 0);
                key = random_str(key_len);
            }
            else
            {
                // update
                key = random_str(max_key_len);
                auto k_it = values.lower_bound(key);
                if (k_it == values.end())
                    continue;
                key = k_it->first;
            }
            uint64_t value_len = min_value_len + (max_value_len > min_value_len ? lrand48() % (max_value_len-min_value_len) : 0);
            auto value = random_str(value_len);
            start_change(key);
            in_progress++;
            printf("set %s\n", key.c_str());
            db->set(key, value, [this, key, value, is_add](int res)
            {
                stop_change(key);
                ops_done++;
                in_progress--;
                if (res != 0)
                    printf("ERROR: set %s = %s: %d (%s)\n", key.c_str(), value.c_str(), res, strerror(-res));
                else
                {
                    if (is_add)
                        add_done++;
                    else
                        update_done++;
                    values[key] = value;
                }
                ringloop->wakeup();
            }, NULL);
        }
        else if (dice < reopen_prob+get_prob+add_prob+update_prob+del_prob)
        {
            // delete
            auto key = random_str(max_key_len);
            auto k_it = values.lower_bound(key);
            if (k_it == values.end())
                continue;
            key = k_it->first;
            start_change(key);
            in_progress++;
            printf("del %s\n", key.c_str());
            db->del(key, [this, key](int res)
            {
                stop_change(key);
                ops_done++;
                in_progress--;
                if (res != 0)
                    printf("ERROR: del %s: %d (%s)\n", key.c_str(), res, strerror(-res));
                else
                {
                    del_done++;
                    values.erase(key);
                }
                ringloop->wakeup();
            }, NULL);
        }
        else if (dice < reopen_prob+get_prob+add_prob+update_prob+del_prob+list_prob)
        {
            // list
            in_progress++;
            auto key = random_str(max_key_len);
            auto lst = new kv_test_listing_t;
            lst->handle = db->list_start(key);
            auto k_it = values.lower_bound(key);
            lst->next_after = k_it == values.begin() ? "" : key;
            lst->inflights = changing_keys;
            listings.insert(lst);
            printf("list %s\n", key.c_str());
            db->list_next(lst->handle, [this, lst](int res, const std::string & key, const std::string & value)
            {
                if (res < 0)
                {
                    if (res != -ENOENT)
                        printf("ERROR: list: %d (%s)\n", res, strerror(-res));
                    else
                    {
                        list_done++;
                        auto k_it = values.upper_bound(lst->next_after);
                        while (k_it != values.end() && lst->inflights.find(k_it->first) != lst->inflights.end())
                            k_it++;
                        if (k_it != values.end())
                            printf("ERROR: list: missed all keys from %s\n", k_it->first.c_str());
                    }
                    ops_done++;
                    in_progress--;
                    db->list_close(lst->handle);
                    delete lst;
                    listings.erase(lst);
                    ringloop->wakeup();
                }
                else
                {
                    // Do not check modified keys in listing
                    // Listing may return their old or new state
                    if (lst->inflights.find(key) == lst->inflights.end())
                    {
                        auto k_it = values.upper_bound(lst->next_after);
                        while (k_it != values.end() && lst->inflights.find(k_it->first) != lst->inflights.end())
                            k_it++;
                        if (k_it == values.end())
                            printf("ERROR: list: returned extra key %s\n", key.c_str());
                        else if (k_it->second != value)
                            printf("ERROR: list: mismatch: %s = %s but should be %s\n", key.c_str(), value.c_str(), k_it->second.c_str());
                        lst->next_after = k_it->first;
                    }
                    db->list_next(lst->handle, NULL);
                }
            });
        }
    }
}

void kv_test_t::start_change(const std::string & key)
{
    changing_keys.insert(key);
    for (auto lst: listings)
    {
        lst->inflights.insert(key);
    }
}

void kv_test_t::stop_change(const std::string & key)
{
    changing_keys.erase(key);
}

int main(int narg, const char *args[])
{
    setvbuf(stdout, NULL, _IONBF, 0);
    setvbuf(stderr, NULL, _IONBF, 0);
    exe_name = args[0];
    kv_test_t *p = new kv_test_t();
    p->run(kv_test_t::parse_args(narg, args));
    delete p;
    return 0;
}
