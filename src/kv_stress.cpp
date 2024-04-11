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
    uint64_t count = 0, done = 0;
    void *handle = NULL;
    std::string next_after;
    std::set<std::string> inflights;
    timespec tv_begin;
    bool error = false;
};

struct kv_test_lat_t
{
    const char *name = NULL;
    uint64_t usec = 0, count = 0;
};

struct kv_test_stat_t
{
    kv_test_lat_t get, add, update, del, list;
    uint64_t list_keys = 0;
};

class kv_test_t
{
public:
    // Config
    json11::Json::object kv_cfg;
    std::string key_prefix, key_suffix;
    uint64_t inode_id = 0;
    uint64_t op_count = 1000000;
    uint64_t runtime_sec = 0;
    uint64_t parallelism = 4;
    uint64_t reopen_prob = 1;
    uint64_t get_prob = 30000;
    uint64_t add_prob = 20000;
    uint64_t update_prob = 20000;
    uint64_t del_prob = 5000;
    uint64_t list_prob = 300;
    uint64_t min_key_len = 10;
    uint64_t max_key_len = 70;
    uint64_t min_value_len = 50;
    uint64_t max_value_len = 300;
    uint64_t min_list_count = 10;
    uint64_t max_list_count = 1000;
    uint64_t print_stats_interval = 1;
    bool json_output = false;
    uint64_t log_level = 1;
    bool trace = false;
    bool stop_on_error = false;
    // FIXME: Multiple clients
    kv_test_stat_t stat, prev_stat;
    timespec prev_stat_time, start_stat_time;

    // State
    kv_dbw_t *db = NULL;
    ring_loop_t *ringloop = NULL;
    epoll_manager_t *epmgr = NULL;
    cluster_client_t *cli = NULL;
    ring_consumer_t consumer;
    bool finished = false;
    uint64_t total_prob = 0;
    uint64_t ops_sent = 0, ops_done = 0;
    int stat_timer_id = -1;
    int in_progress = 0;
    bool reopening = false;
    std::set<kv_test_listing_t*> listings;
    std::set<std::string> changing_keys;
    std::map<std::string, std::string> values;

    ~kv_test_t();

    static json11::Json::object parse_args(int narg, const char *args[]);
    void parse_config(json11::Json cfg);
    void run(json11::Json cfg);
    void loop();
    void print_stats(kv_test_stat_t & prev_stat, timespec & prev_stat_time);
    void print_total_stats();
    void start_change(const std::string & key);
    void stop_change(const std::string & key);
    void add_stat(kv_test_lat_t & stat, timespec tv_begin);
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
                "USAGE: %s --pool_id POOL_ID --inode_id INODE_ID [OPTIONS]\n"
                "  --op_count 1000000\n"
                "    Total operations to run during test. 0 means unlimited\n"
                "  --key_prefix \"\"\n"
                "    Prefix for all keys read or written (to avoid collisions)\n"
                "  --key_suffix \"\"\n"
                "    Suffix for all keys read or written (to avoid collisions, but scan all DB)\n"
                "  --runtime 0\n"
                "    Run for this number of seconds. 0 means unlimited\n"
                "  --parallelism 4\n"
                "    Run this number of operations in parallel\n"
                "  --get_prob 30000\n"
                "    Fraction of key retrieve operations\n"
                "  --add_prob 20000\n"
                "    Fraction of key addition operations\n"
                "  --update_prob 20000\n"
                "    Fraction of key update operations\n"
                "  --del_prob 30000\n"
                "    Fraction of key delete operations\n"
                "  --list_prob 300\n"
                "    Fraction of listing operations\n"
                "  --reopen_prob 1\n"
                "    Fraction of database reopens\n"
                "  --min_key_len 10\n"
                "    Minimum key size in bytes\n"
                "  --max_key_len 70\n"
                "    Maximum key size in bytes\n"
                "  --min_value_len 50\n"
                "    Minimum value size in bytes\n"
                "  --max_value_len 300\n"
                "    Maximum value size in bytes\n"
                "  --min_list_count 10\n"
                "    Minimum number of keys read in listing (0 = all keys)\n"
                "  --max_list_count 1000\n"
                "    Maximum number of keys read in listing\n"
                "  --print_stats 1\n"
                "    Print operation statistics every this number of seconds\n"
                "  --json\n"
                "    JSON output\n"
                "  --stop_on_error 0\n"
                "    Stop on first execution error, mismatch, lost key or extra key during listing\n"
                "  --kv_block_size 4k\n"
                "    Key-value B-Tree block size\n"
                "  --kv_memory_limit 128M\n"
                "    Maximum memory to use for vitastor-kv index cache\n"
                "  --kv_allocate_blocks 4\n"
                "    Number of PG blocks used for new tree block allocation in parallel\n"
                "  --kv_evict_max_misses 10\n"
                "    Eviction algorithm parameter: retry eviction from another random spot\n"
                "    if this number of keys is used currently or was used recently\n"
                "  --kv_evict_attempts_per_level 3\n"
                "    Retry eviction at most this number of times per tree level, starting\n"
                "    with bottom-most levels\n"
                "  --kv_evict_unused_age 1000\n"
                "    Evict only keys unused during this number of last operations\n"
                "  --kv_log_level 1\n"
                "    Log level. 0 = errors, 1 = warnings, 10 = trace operations\n",
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

void kv_test_t::parse_config(json11::Json cfg)
{
    inode_id = INODE_WITH_POOL(cfg["pool_id"].uint64_value(), cfg["inode_id"].uint64_value());
    if (cfg["op_count"].uint64_value() > 0)
        op_count = cfg["op_count"].uint64_value();
    key_prefix = cfg["key_prefix"].string_value();
    key_suffix = cfg["key_suffix"].string_value();
    if (cfg["runtime"].uint64_value() > 0)
        runtime_sec = cfg["runtime"].uint64_value();
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
    if (!cfg["min_list_count"].is_null())
        min_list_count = cfg["min_list_count"].uint64_value();
    if (!cfg["max_list_count"].is_null())
        max_list_count = cfg["max_list_count"].uint64_value();
    if (!cfg["print_stats"].is_null())
        print_stats_interval = cfg["print_stats"].uint64_value();
    if (!cfg["json"].is_null())
        json_output = true;
    if (!cfg["stop_on_error"].is_null())
        stop_on_error = cfg["stop_on_error"].bool_value();
    if (!cfg["kv_block_size"].is_null())
        kv_cfg["kv_block_size"] = cfg["kv_block_size"];
    if (!cfg["kv_memory_limit"].is_null())
        kv_cfg["kv_memory_limit"] = cfg["kv_memory_limit"];
    if (!cfg["kv_allocate_blocks"].is_null())
        kv_cfg["kv_allocate_blocks"] = cfg["kv_allocate_blocks"];
    if (!cfg["kv_evict_max_misses"].is_null())
        kv_cfg["kv_evict_max_misses"] = cfg["kv_evict_max_misses"];
    if (!cfg["kv_evict_attempts_per_level"].is_null())
        kv_cfg["kv_evict_attempts_per_level"] = cfg["kv_evict_attempts_per_level"];
    if (!cfg["kv_evict_unused_age"].is_null())
        kv_cfg["kv_evict_unused_age"] = cfg["kv_evict_unused_age"];
    if (!cfg["kv_log_level"].is_null())
    {
        log_level = cfg["kv_log_level"].uint64_value();
        trace = log_level >= 10;
        kv_cfg["kv_log_level"] = cfg["kv_log_level"];
    }
    total_prob = reopen_prob+get_prob+add_prob+update_prob+del_prob+list_prob;
    stat.get.name = "get";
    stat.add.name = "add";
    stat.update.name = "update";
    stat.del.name = "del";
    stat.list.name = "list";
}

void kv_test_t::run(json11::Json cfg)
{
    srand48(time(NULL));
    parse_config(cfg);
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
            fprintf(stderr, "ERROR: Open index: %d (%s)\n", res, strerror(-res));
            exit(1);
        }
        if (trace)
            printf("Index opened\n");
        ringloop->wakeup();
    });
    consumer.loop = [this]() { loop(); };
    ringloop->register_consumer(&consumer);
    if (print_stats_interval)
        stat_timer_id = epmgr->tfd->set_timer(print_stats_interval*1000, true, [this](int) { print_stats(prev_stat, prev_stat_time); });
    clock_gettime(CLOCK_REALTIME, &start_stat_time);
    prev_stat_time = start_stat_time;
    while (!finished)
    {
        ringloop->loop();
        if (!finished)
            ringloop->wait();
    }
    if (stat_timer_id >= 0)
        epmgr->tfd->clear_timer(stat_timer_id);
    ringloop->unregister_consumer(&consumer);
    // Print total stats
    print_total_stats();
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
    while (!finished && ops_sent < op_count && in_progress < parallelism)
    {
        uint64_t dice = (lrand48() % total_prob);
        if (dice < reopen_prob)
        {
            reopening = true;
            db->close([this]()
            {
                if (trace)
                    printf("Index closed\n");
                db->open(inode_id, kv_cfg, [this](int res)
                {
                    reopening = false;
                    if (res < 0)
                    {
                        fprintf(stderr, "ERROR: Reopen index: %d (%s)\n", res, strerror(-res));
                        finished = true;
                        return;
                    }
                    if (trace)
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
            ops_sent++;
            if (trace)
                printf("get %s\n", key.c_str());
            timespec tv_begin;
            clock_gettime(CLOCK_REALTIME, &tv_begin);
            db->get(key, [this, key, tv_begin](int res, const std::string & value)
            {
                add_stat(stat.get, tv_begin);
                ops_done++;
                in_progress--;
                auto it = values.find(key);
                if (res != (it == values.end() ? -ENOENT : 0))
                {
                    fprintf(stderr, "ERROR: get %s: %d (%s)\n", key.c_str(), res, strerror(-res));
                    if (stop_on_error)
                        exit(1);
                }
                else if (it != values.end() && value != it->second)
                {
                    fprintf(stderr, "ERROR: get %s: mismatch: %s vs %s\n", key.c_str(), value.c_str(), it->second.c_str());
                    if (stop_on_error)
                        exit(1);
                }
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
                key = key_prefix + random_str(key_len) + key_suffix;
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
            if (changing_keys.find(key) != changing_keys.end())
                continue;
            uint64_t value_len = min_value_len + (max_value_len > min_value_len ? lrand48() % (max_value_len-min_value_len) : 0);
            auto value = random_str(value_len);
            start_change(key);
            ops_sent++;
            in_progress++;
            if (trace)
                printf("set %s = %s\n", key.c_str(), value.c_str());
            timespec tv_begin;
            clock_gettime(CLOCK_REALTIME, &tv_begin);
            db->set(key, value, [this, key, value, tv_begin, is_add](int res)
            {
                add_stat(is_add ? stat.add : stat.update, tv_begin);
                stop_change(key);
                ops_done++;
                in_progress--;
                if (res != 0)
                {
                    fprintf(stderr, "ERROR: set %s = %s: %d (%s)\n", key.c_str(), value.c_str(), res, strerror(-res));
                    if (stop_on_error)
                        exit(1);
                }
                else
                {
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
            if (changing_keys.find(key) != changing_keys.end())
                continue;
            start_change(key);
            ops_sent++;
            in_progress++;
            if (trace)
                printf("del %s\n", key.c_str());
            timespec tv_begin;
            clock_gettime(CLOCK_REALTIME, &tv_begin);
            db->del(key, [this, key, tv_begin](int res)
            {
                add_stat(stat.del, tv_begin);
                stop_change(key);
                ops_done++;
                in_progress--;
                if (res != 0)
                {
                    fprintf(stderr, "ERROR: del %s: %d (%s)\n", key.c_str(), res, strerror(-res));
                    if (stop_on_error)
                        exit(1);
                }
                else
                {
                    values.erase(key);
                }
                ringloop->wakeup();
            }, NULL);
        }
        else if (dice < reopen_prob+get_prob+add_prob+update_prob+del_prob+list_prob)
        {
            // list
            ops_sent++;
            in_progress++;
            auto key = random_str(max_key_len);
            auto lst = new kv_test_listing_t;
            auto k_it = values.lower_bound(key);
            lst->count = min_list_count + (max_list_count > min_list_count ? lrand48() % (max_list_count-min_list_count) : 0);
            lst->handle = db->list_start(k_it == values.begin() ? key_prefix : key);
            lst->next_after = k_it == values.begin() ? key_prefix : key;
            lst->inflights = changing_keys;
            listings.insert(lst);
            if (trace)
                printf("list from %s\n", key.c_str());
            clock_gettime(CLOCK_REALTIME, &lst->tv_begin);
            db->list_next(lst->handle, [this, lst](int res, const std::string & key, const std::string & value)
            {
                if (log_level >= 11)
                    printf("list: %s = %s\n", key.c_str(), value.c_str());
                if (res >= 0 && key_prefix.size() && (key.size() < key_prefix.size() ||
                    key.substr(0, key_prefix.size()) != key_prefix))
                {
                    // stop at this key
                    res = -ENOENT;
                }
                if (res < 0 || (lst->count > 0 && lst->done >= lst->count))
                {
                    add_stat(stat.list, lst->tv_begin);
                    if (res == 0)
                    {
                        // ok (done >= count)
                    }
                    else if (res != -ENOENT)
                    {
                        fprintf(stderr, "ERROR: list: %d (%s)\n", res, strerror(-res));
                        lst->error = true;
                    }
                    else
                    {
                        auto k_it = lst->next_after == "" ? values.begin() : values.upper_bound(lst->next_after);
                        while (k_it != values.end())
                        {
                            while (k_it != values.end() && lst->inflights.find(k_it->first) != lst->inflights.end())
                                k_it++;
                            if (k_it != values.end())
                            {
                                fprintf(stderr, "ERROR: list: missing key %s\n", (k_it++)->first.c_str());
                                lst->error = true;
                            }
                        }
                    }
                    if (lst->error && stop_on_error)
                        exit(1);
                    ops_done++;
                    in_progress--;
                    db->list_close(lst->handle);
                    delete lst;
                    listings.erase(lst);
                    ringloop->wakeup();
                }
                else
                {
                    stat.list_keys++;
                    // Do not check modified keys in listing
                    // Listing may return their old or new state
                    if ((!key_suffix.size() || key.size() >= key_suffix.size() &&
                        key.substr(key.size()-key_suffix.size()) == key_suffix) &&
                        lst->inflights.find(key) == lst->inflights.end())
                    {
                        lst->done++;
                        auto k_it = lst->next_after == "" ? values.begin() : values.upper_bound(lst->next_after);
                        while (true)
                        {
                            while (k_it != values.end() && lst->inflights.find(k_it->first) != lst->inflights.end())
                            {
                                k_it++;
                            }
                            if (k_it == values.end() || k_it->first > key)
                            {
                                fprintf(stderr, "ERROR: list: extra key %s\n", key.c_str());
                                lst->error = true;
                                break;
                            }
                            else if (k_it->first < key)
                            {
                                fprintf(stderr, "ERROR: list: missing key %s\n", k_it->first.c_str());
                                lst->error = true;
                                lst->next_after = k_it->first;
                                k_it++;
                            }
                            else
                            {
                                if (k_it->second != value)
                                {
                                    fprintf(stderr, "ERROR: list: mismatch: %s = %s but should be %s\n",
                                        key.c_str(), value.c_str(), k_it->second.c_str());
                                    lst->error = true;
                                }
                                lst->next_after = k_it->first;
                                break;
                            }
                        }
                    }
                    db->list_next(lst->handle, NULL);
                }
            });
        }
    }
}

void kv_test_t::add_stat(kv_test_lat_t & stat, timespec tv_begin)
{
    timespec tv_end;
    clock_gettime(CLOCK_REALTIME, &tv_end);
    int64_t usec = (tv_end.tv_sec - tv_begin.tv_sec)*1000000 +
        (tv_end.tv_nsec - tv_begin.tv_nsec)/1000;
    if (usec > 0)
        stat.usec += usec;
    stat.count++;
}

void kv_test_t::print_stats(kv_test_stat_t & prev_stat, timespec & prev_stat_time)
{
    timespec cur_stat_time;
    clock_gettime(CLOCK_REALTIME, &cur_stat_time);
    int64_t usec = (cur_stat_time.tv_sec - prev_stat_time.tv_sec)*1000000 +
        (cur_stat_time.tv_nsec - prev_stat_time.tv_nsec)/1000;
    if (usec > 0)
    {
        kv_test_lat_t *lats[] = { &stat.get, &stat.add, &stat.update, &stat.del, &stat.list };
        kv_test_lat_t *prev[] = { &prev_stat.get, &prev_stat.add, &prev_stat.update, &prev_stat.del, &prev_stat.list };
        if (!json_output)
        {
            char buf[128] = { 0 };
            for (int i = 0; i < sizeof(lats)/sizeof(lats[0]); i++)
            {
                snprintf(buf, sizeof(buf)-1, "%.1f %s/s (%ju us)", (lats[i]->count-prev[i]->count)*1000000.0/usec,
                    lats[i]->name, (lats[i]->usec-prev[i]->usec)/(lats[i]->count-prev[i]->count > 0 ? lats[i]->count-prev[i]->count : 1));
                int k;
                for (k = strlen(buf); k < strlen(lats[i]->name)+21; k++)
                    buf[k] = ' ';
                buf[k] = 0;
                printf("%s", buf);
            }
            printf("\n");
        }
        else
        {
            int64_t runtime = (cur_stat_time.tv_sec - start_stat_time.tv_sec)*1000000 +
                (cur_stat_time.tv_nsec - start_stat_time.tv_nsec)/1000;
            printf("{\"runtime\":%.1f", (double)runtime/1000000.0);
            for (int i = 0; i < sizeof(lats)/sizeof(lats[0]); i++)
            {
                if (lats[i]->count > prev[i]->count)
                {
                    printf(
                        ",\"%s\":{\"avg\":{\"iops\":%.1f,\"usec\":%ju},\"total\":{\"count\":%ju,\"usec\":%ju}}",
                        lats[i]->name, (lats[i]->count-prev[i]->count)*1000000.0/usec,
                        (lats[i]->usec-prev[i]->usec)/(lats[i]->count-prev[i]->count),
                        lats[i]->count, lats[i]->usec
                    );
                }
            }
            printf("}\n");
        }
    }
    prev_stat = stat;
    prev_stat_time = cur_stat_time;
}

void kv_test_t::print_total_stats()
{
    if (!json_output)
        printf("Total:\n");
    kv_test_stat_t start_stats;
    timespec start_stat_time = this->start_stat_time;
    print_stats(start_stats, start_stat_time);
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
