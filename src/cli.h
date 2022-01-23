// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

// Common CLI tool header

#pragma once

#include "json11/json11.hpp"
#include "object_id.h"
#include "ringloop.h"
#include <functional>

struct rm_inode_t;
struct snap_merger_t;
struct snap_flattener_t;
struct snap_remover_t;

class epoll_manager_t;
class cluster_client_t;
struct inode_config_t;

class cli_tool_t
{
public:
    uint64_t iodepth = 0, parallel_osds = 0;
    bool progress = true;
    bool list_first = false;
    bool json_output = false;
    int log_level = 0;
    bool color = false;

    ring_loop_t *ringloop = NULL;
    epoll_manager_t *epmgr = NULL;
    cluster_client_t *cli = NULL;

    int waiting = 0;
    json11::Json etcd_result;
    ring_consumer_t consumer;
    std::function<bool(void)> action_cb;

    void run(json11::Json cfg);

    void change_parent(inode_t cur, inode_t new_parent);
    inode_config_t* get_inode_cfg(const std::string & name);

    static json11::Json::object parse_args(int narg, const char *args[]);
    static void help();

    friend struct rm_inode_t;
    friend struct snap_merger_t;
    friend struct snap_flattener_t;
    friend struct snap_remover_t;

    std::function<bool(void)> start_df(json11::Json);
    std::function<bool(void)> start_ls(json11::Json);
    std::function<bool(void)> start_create(json11::Json);
    std::function<bool(void)> start_modify(json11::Json);
    std::function<bool(void)> start_rm(json11::Json);
    std::function<bool(void)> start_merge(json11::Json);
    std::function<bool(void)> start_flatten(json11::Json);
    std::function<bool(void)> start_snap_rm(json11::Json);
    std::function<bool(void)> start_alloc_osd(json11::Json cfg, uint64_t *out = NULL);
    std::function<bool(void)> simple_offsets(json11::Json cfg);

    void etcd_txn(json11::Json txn);
};

uint64_t parse_size(std::string size_str);

std::string print_table(json11::Json items, json11::Json header, bool use_esc);

std::string format_size(uint64_t size);

std::string format_lat(uint64_t lat);

std::string format_q(double depth);

bool stupid_glob(const std::string str, const std::string glob);
