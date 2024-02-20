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

struct cli_result_t
{
    int err;
    std::string text;
    json11::Json data;
};

class cli_tool_t
{
public:
    uint64_t iodepth = 4, parallel_osds = 32;
    bool progress = false;
    bool list_first = false;
    bool json_output = false;
    int log_level = 0;
    bool is_command_line = false;
    bool color = false;

    ring_loop_t *ringloop = NULL;
    epoll_manager_t *epmgr = NULL;
    cluster_client_t *cli = NULL;

    int waiting = 0;
    cli_result_t etcd_err;
    json11::Json etcd_result;

    void parse_config(json11::Json::object & cfg);
    json11::Json parse_tags(std::string tags);

    void change_parent(inode_t cur, inode_t new_parent, cli_result_t *result);
    inode_config_t* get_inode_cfg(const std::string & name);

    friend struct rm_inode_t;
    friend struct snap_merger_t;
    friend struct snap_flattener_t;
    friend struct snap_remover_t;

    std::function<bool(cli_result_t &)> start_status(json11::Json);
    std::function<bool(cli_result_t &)> start_describe(json11::Json);
    std::function<bool(cli_result_t &)> start_fix(json11::Json);
    std::function<bool(cli_result_t &)> start_ls(json11::Json);
    std::function<bool(cli_result_t &)> start_create(json11::Json);
    std::function<bool(cli_result_t &)> start_modify(json11::Json);
    std::function<bool(cli_result_t &)> start_rm_data(json11::Json);
    std::function<bool(cli_result_t &)> start_merge(json11::Json);
    std::function<bool(cli_result_t &)> start_flatten(json11::Json);
    std::function<bool(cli_result_t &)> start_rm(json11::Json);
    std::function<bool(cli_result_t &)> start_rm_osd(json11::Json cfg);
    std::function<bool(cli_result_t &)> start_alloc_osd(json11::Json cfg);
    std::function<bool(cli_result_t &)> start_pool_create(json11::Json);
    std::function<bool(cli_result_t &)> start_pool_modify(json11::Json);
    std::function<bool(cli_result_t &)> start_pool_rm(json11::Json);
    std::function<bool(cli_result_t &)> start_pool_ls(json11::Json);

    // Should be called like loop_and_wait(start_status(), <completion callback>)
    void loop_and_wait(std::function<bool(cli_result_t &)> loop_cb, std::function<void(const cli_result_t &)> complete_cb);

    void etcd_txn(json11::Json txn);
};

std::string print_table(json11::Json items, json11::Json header, bool use_esc);

std::string format_lat(uint64_t lat);

std::string format_q(double depth);

bool stupid_glob(const std::string str, const std::string glob);
