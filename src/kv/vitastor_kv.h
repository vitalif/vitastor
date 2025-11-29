// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// Vitastor shared key/value database
// Parallel optimistic B-Tree O:-)

#pragma once

#include <stdint.h>
#include <sys/uio.h>

#include <string>
#include <map>
#include <functional>

#define VITASTOR_KV_API_VERSION 1

class cluster_client_t;

struct kv_db_t;

struct __attribute__((visibility("default"))) vitastorkv_dbw_t
{
    // cli = vitastor_c_get_internal_client(client)
    vitastorkv_dbw_t(cluster_client_t *cli);
    ~vitastorkv_dbw_t();

    void open(uint64_t inode_id, std::map<std::string, std::string> cfg, std::function<void(int)> cb);
    void set_config(std::map<std::string, std::string> cfg);
    void close(std::function<void()> cb);
    void rescue(std::function<void(int res, const std::string & key, const std::string & value)> cb);

    uint64_t get_size();

    void get(const std::string & key, std::function<void(int res, const std::string & value)> cb,
        bool allow_old_cached = false);
    void set(const std::string & key, const std::string & value, std::function<void(int res)> cb,
        std::function<bool(int res, const std::string & value)> cas_compare = NULL);
    void del(const std::string & key, std::function<void(int res)> cb,
        std::function<bool(int res, const std::string & value)> cas_compare = NULL);

    void* list_start(const std::string & start);
    void list_next(void *handle, std::function<void(int res, const std::string & key, const std::string & value)> cb);
    void list_close(void *handle);

    kv_db_t *db;
};
