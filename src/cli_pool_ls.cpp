/*
 =========================================================================
 Copyright (c) 2023 MIND Software LLC. All Rights Reserved.
 This file is part of the Software-Defined Storage MIND UStor Project.
 For more information about this product, please visit https://mindsw.io
 or contact us directly at info@mindsw.io
 =========================================================================
 */

#include <algorithm>
#include <numeric>
#include <string>
#include "cli.h"
#include "cluster_client.h"
#include "str_util.h"
#include "pg_states.h"

// List pools with space statistics
struct pool_ls_t
{
    cli_tool_t *parent;
    pool_id_t list_pool_id = 0;
    std::string list_pool_name;
    std::string sort_field;
    std::set<std::string> only_names;
    bool show_df_format = false;
    bool show_stats = false;
    bool reverse = false;
    bool show_all = false;
    int max_count = 0;
    int state = 0;
    json11::Json space_info;
    cli_result_t result;
    std::map<pool_id_t, json11::Json::object> pool_stats;

    bool is_done()
    {
        return state == 100;
    }

    std::string item_as_string(const json11::Json& item)
    {
        if (item.is_array())
        {
            if (item.array_items().empty())
                return std::string{};
            std::string result = item.array_items().at(0).as_string();
            std::for_each(
                std::next(item.array_items().begin()),
                item.array_items().end(),
                [&result](const json11::Json& a)
                {
                    result += ", " + a.as_string();
                });
            return result;
        }
        else
            return item.as_string();
    }

    void get_stats()
    {
        if (state == 1)
            goto resume_1;
        if (list_pool_name != "")
        {
            for (auto & ic: parent->cli->st_cli.pool_config)
            {
                if (ic.second.name == list_pool_name)
                {
                    list_pool_id = ic.first;
                    break;
                }
            }
            if (!list_pool_id)
            {
                result = (cli_result_t){ .err = ENOENT, .text = "Pool "+list_pool_name+" does not exist" };
                state = 100;
                return;
            }
        }
        else if (list_pool_id !=0)
        {
            for (auto & ic: parent->cli->st_cli.pool_config)
            {
                if (ic.second.id == list_pool_id)
                {
                    list_pool_name = ic.second.name;
                    break;
                }
            }
            if (list_pool_name == "")
            {
                result = (cli_result_t){ .err = ENOENT, .text = "Pool "+list_pool_name+" does not exist" };
                state = 100;
                return;
            }
        }
        // Space statistics - pool/stats/<pool>
        parent->etcd_txn(json11::Json::object {
            { "success", json11::Json::array {
                json11::Json::object {
                    { "request_range", json11::Json::object {
                        { "key", base64_encode(
                            parent->cli->st_cli.etcd_prefix+"/pool/stats/"
                        ) },
                        { "range_end", base64_encode(
                            parent->cli->st_cli.etcd_prefix+"/pool/stats0"
                        ) },
                    } },
                },
                json11::Json::object {
                    { "request_range", json11::Json::object {
                        { "key", base64_encode(
                            parent->cli->st_cli.etcd_prefix+"/osd/stats/"
                        ) },
                        { "range_end", base64_encode(
                            parent->cli->st_cli.etcd_prefix+"/osd/stats0"
                        ) },
                    } },
                },
                json11::Json::object {
                    { "request_range", json11::Json::object {
                        { "key", base64_encode(
                            parent->cli->st_cli.etcd_prefix+"/inode/stats"+
                            (list_pool_id ? "/"+std::to_string(list_pool_id) : "")+"/"
                        ) },
                        { "range_end", base64_encode(
                            parent->cli->st_cli.etcd_prefix+"/inode/stats"+
                            (list_pool_id ? "/"+std::to_string(list_pool_id) : "")+"0"
                        ) },
                    } },
                },
                json11::Json::object {
                    { "request_range", json11::Json::object {
                        { "key", base64_encode(
                            parent->cli->st_cli.etcd_prefix+"/pg/stats"+
                            (list_pool_id ? "/"+std::to_string(list_pool_id) : "")+"/"
                        ) },
                        { "range_end", base64_encode(
                            parent->cli->st_cli.etcd_prefix+"/pg/stats"+
                            (list_pool_id ? "/"+std::to_string(list_pool_id) : "")+"0"
                        ) },
                    } },
                },
            } },
        });
        state = 1;
resume_1:
        if (parent->waiting > 0)
            return;
        if (parent->etcd_err.err)
        {
            result = parent->etcd_err;
            state = 100;
            return;
        }
        space_info = parent->etcd_result;
        std::map<pool_id_t, uint64_t> osd_free;
        for (auto & kv_item: space_info["responses"][0]["response_range"]["kvs"].array_items())
        {
            auto kv = parent->cli->st_cli.parse_etcd_kv(kv_item);
            // pool ID
            pool_id_t pool_id;
            char null_byte = 0;
            int scanned = sscanf(kv.key.substr(parent->cli->st_cli.etcd_prefix.length()).c_str(), "/pool/stats/%u%c", &pool_id, &null_byte);
            if (scanned != 1 || !pool_id || pool_id >= POOL_ID_MAX)
            {
                fprintf(stderr, "Invalid key in etcd: %s\n", kv.key.c_str());
                continue;
            }
            // pool/stats/<N>
            pool_stats[pool_id] = kv.value.object_items();
        }
        for (auto & kv_item: space_info["responses"][1]["response_range"]["kvs"].array_items())
        {
            auto kv = parent->cli->st_cli.parse_etcd_kv(kv_item);
            // osd ID
            osd_num_t osd_num;
            char null_byte = 0;
            int scanned = sscanf(kv.key.substr(parent->cli->st_cli.etcd_prefix.length()).c_str(), "/osd/stats/%lu%c", &osd_num, &null_byte);
            if (scanned != 1 || !osd_num || osd_num >= POOL_ID_MAX)
            {
                fprintf(stderr, "Invalid key in etcd: %s\n", kv.key.c_str());
                continue;
            }
            // osd/stats/<N>::free
            osd_free[osd_num] = kv.value["free"].uint64_value();
        }
        // Performance statistics
        double pool_read_iops = 0;
        double pool_read_bps = 0;
        double pool_read_lat = 0;
        double pool_write_iops = 0;
        double pool_write_bps = 0;
        double pool_write_lat = 0;
        double pool_delete_iops = 0;
        double pool_delete_bps = 0;
        double pool_delete_lat = 0;
        uint32_t pool_inode_stats_count = 0;
        for (auto & kv_item: space_info["responses"][2]["response_range"]["kvs"].array_items())
        {
            auto kv = parent->cli->st_cli.parse_etcd_kv(kv_item);
            // pool ID & inode number
            pool_id_t pool_id;
            inode_t only_inode_num;
            char null_byte = 0;
            int scanned = sscanf(kv.key.substr(parent->cli->st_cli.etcd_prefix.length()).c_str(),
                "/inode/stats/%u/%lu%c", &pool_id, &only_inode_num, &null_byte);
            if (scanned != 2 || !pool_id || pool_id >= POOL_ID_MAX || INODE_POOL(only_inode_num) != 0)
            {
                fprintf(stderr, "Invalid key in etcd: %s\n", kv.key.c_str());
                continue;
            }
            pool_read_iops += kv.value["read"]["iops"].number_value();
            pool_read_bps  += kv.value["read"]["bps"].number_value();
            pool_read_lat  += kv.value["read"]["lat"].number_value();
            pool_write_iops += kv.value["write"]["iops"].number_value();
            pool_write_bps  += kv.value["write"]["bps"].number_value();
            pool_write_lat  += kv.value["write"]["lat"].number_value();
            pool_delete_iops += kv.value["delete"]["iops"].number_value();
            pool_delete_bps  += kv.value["delete"]["bps"].number_value();
            pool_delete_lat  += kv.value["delete"]["lat"].number_value();
            pool_inode_stats_count++;
        }
        pool_read_bps = pool_inode_stats_count ? (pool_read_bps/pool_inode_stats_count) : 0;
        pool_write_bps = pool_inode_stats_count ? (pool_write_bps/pool_inode_stats_count) : 0;
        pool_delete_bps = pool_inode_stats_count ? (pool_delete_bps/pool_inode_stats_count) : 0;
        pool_read_lat = pool_inode_stats_count ? (pool_read_lat/pool_inode_stats_count) : 0;
        pool_write_lat = pool_inode_stats_count ? (pool_write_lat/pool_inode_stats_count) : 0;
        pool_delete_lat = pool_inode_stats_count ? (pool_delete_lat/pool_inode_stats_count) : 0;
        // Calculate recovery percent
        uint64_t object_count = 0;
        uint64_t degraded_count = 0;
        uint64_t misplaced_count = 0;
        for (auto & kv_item: space_info["responses"][3]["response_range"]["kvs"].array_items())
        {
            auto kv = parent->cli->st_cli.parse_etcd_kv(kv_item);
            // pool ID & pg number
            pool_id_t pool_id;
            pg_num_t pg_num = 0;
            char null_byte = 0;
            int scanned = sscanf(kv.key.substr(parent->cli->st_cli.etcd_prefix.length()).c_str(),
                "/pg/stats/%u/%u%c", &pool_id, &pg_num, &null_byte);
            if (scanned != 2 || !pool_id || pool_id >= POOL_ID_MAX)
            {
                fprintf(stderr, "Invalid key in etcd: %s\n", kv.key.c_str());
                continue;
            }
            object_count += kv.value["object_count"].uint64_value();
            degraded_count += kv.value["degraded_count"].uint64_value();
            misplaced_count += kv.value["misplaced_count"].uint64_value();
        }
        // Calculate max_avail for each pool
        for (auto & pp: parent->cli->st_cli.pool_config)
        {
            auto & pool_cfg = pp.second;
            uint64_t pool_avail = UINT64_MAX;
            std::map<osd_num_t, uint64_t> pg_per_osd;
            json11::Json::array osd_set;
            for (auto & pgp: pool_cfg.pg_config)
            {
                for (auto pg_osd: pgp.second.target_set)
                {
                    if (pg_osd != 0)
                    {
                        pg_per_osd[pg_osd]++;
                    }
                }
            }
            for (auto pg_per_pair: pg_per_osd)
            {
                uint64_t pg_free = osd_free[pg_per_pair.first] * pool_cfg.pg_count / pg_per_pair.second;
                if (pool_avail > pg_free)
                {
                    pool_avail = pg_free;
                }
                osd_set.push_back(pg_per_pair.first);
            }
            if (pool_avail == UINT64_MAX)
            {
                pool_avail = 0;
            }
            if (pool_cfg.scheme != POOL_SCHEME_REPLICATED)
            {
                pool_avail *= (pool_cfg.pg_size - pool_cfg.parity_chunks);
            }

            bool active = pool_cfg.real_pg_count > 0;
            bool incomplete = false;
            bool has_incomplete = false;
            bool degraded = false;
            bool has_degraded = false;
            bool has_misplaced = false;
            for (auto pg_it = pool_cfg.pg_config.begin(); pg_it != pool_cfg.pg_config.end(); pg_it++)
            {
                if (!(pg_it->second.cur_state & PG_ACTIVE))
                {
                    active = false;
                }
                if (pg_it->second.cur_state & PG_INCOMPLETE)
                {
                    incomplete = true;
                }
                if (pg_it->second.cur_state & PG_HAS_INCOMPLETE)
                {
                    has_incomplete = true;
                }
                if (pg_it->second.cur_state & PG_DEGRADED)
                {
                    degraded = true;
                }
                if (pg_it->second.cur_state & PG_HAS_DEGRADED)
                {
                    has_degraded = true;
                }
                if (pg_it->second.cur_state & PG_HAS_MISPLACED)
                {
                    has_misplaced = true;
                }
            }
            // incomplete > has_incomplete > degraded > has_degraded > has_misplaced
            std::string status;
            if (active)
            {
                if (incomplete)
                    status ="incomplete";
                else if (has_incomplete)
                    status ="has_incomplete";
                else if (degraded)
                    status ="degraded";
                else if (has_degraded)
                    status ="has_degraded";
                else if (has_misplaced)
                    status ="has_misplaced";
                else
                    status ="active";
            }
            else
            {
                status ="inactive";
            }

            pool_stats[pool_cfg.id] = json11::Json::object {
                { "id", (uint64_t)(pool_cfg.id) },
                { "name", pool_cfg.name },
                { "status", status },
                { "recovery", object_count ? (double)( (degraded_count + misplaced_count)/object_count) : 0 },
                { "pg_count", pool_cfg.pg_count },
                { "real_pg_count", pool_cfg.real_pg_count },
                { "scheme", pool_cfg.scheme == POOL_SCHEME_REPLICATED ? "replicated" : "ec" },
                { "scheme_name", pool_cfg.scheme == POOL_SCHEME_REPLICATED
                    ? std::to_string(pool_cfg.pg_size)+"/"+std::to_string(pool_cfg.pg_minsize)
                    : "EC "+std::to_string(pool_cfg.pg_size-pool_cfg.parity_chunks)+"+"+std::to_string(pool_cfg.parity_chunks) },
                { "used_raw", (uint64_t)(pool_stats[pool_cfg.id]["used_raw_tb"].number_value() * ((uint64_t)1<<40)) },
                { "total_raw", (uint64_t)(pool_stats[pool_cfg.id]["total_raw_tb"].number_value() * ((uint64_t)1<<40)) },
                { "max_available", pool_avail },
                { "raw_to_usable", pool_stats[pool_cfg.id]["raw_to_usable"].number_value() },
                { "space_efficiency", pool_stats[pool_cfg.id]["space_efficiency"].number_value() },
                { "pg_real_size", pool_stats[pool_cfg.id]["pg_real_size"].uint64_value() },
                { "failure_domain", pool_cfg.failure_domain },
                { "root_node", pool_cfg.root_node },
                { "osd_tags", pool_cfg.osd_tags },
                { "osd_count",  pg_per_osd.size() },
                { "osd_set",  osd_set },
                { "primary_affinity_tags", pool_cfg.primary_affinity_tags },
                { "pg_minsize", pool_cfg.pg_minsize },
                { "pg_size", pool_cfg.pg_size },
                { "parity_chunks",pool_cfg.parity_chunks },
                { "max_osd_combinations",pool_cfg.max_osd_combinations },
                { "block_size", (uint64_t)pool_cfg.data_block_size },
                { "bitmap_granularity",(uint64_t)pool_cfg.bitmap_granularity },
                { "pg_stripe_size",pool_cfg.pg_stripe_size },
                { "scrub_interval",pool_cfg.scrub_interval },
                { "read_iops", pool_read_iops },
                { "read_bps", pool_read_bps },
                { "read_lat", pool_read_lat },
                { "write_iops", pool_write_iops },
                { "write_bps", pool_write_bps },
                { "write_lat", pool_write_lat },
                { "delete_iops", pool_delete_iops },
                { "delete_bps", pool_delete_bps },
                { "delete_lat", pool_delete_lat} ,
            };
        }
    }

    json11::Json::array to_list()
    {
        json11::Json::array list;
        for (auto & kv: pool_stats)
        {
            if (!only_names.size())
            {
                list.push_back(kv.second);
            }
            else
            {
                for (auto glob: only_names)
                {
                    if (stupid_glob(kv.second["name"].string_value(), glob))
                    {
                        list.push_back(kv.second);
                        break;
                    }
                }
            }
        }
        if (sort_field == "name" ||
            sort_field == "scheme_name" ||
            sort_field == "scheme" ||
            sort_field == "failure_domain" ||
            sort_field == "root_node" ||
            sort_field == "osd_tags_fmt" ||
            sort_field == "primary_affinity_tags_fmt" ||
            sort_field == "status" )
        {
            std::sort(list.begin(), list.end(), [this](json11::Json a, json11::Json b)
            {
                auto av = a[sort_field].as_string();
                auto bv = b[sort_field].as_string();
                return reverse ? av > bv : av < bv;
            });
        }
        else
        {
            std::sort(list.begin(), list.end(), [this](json11::Json a, json11::Json b)
            {
                auto av = a[sort_field].number_value();
                auto bv = b[sort_field].number_value();
                return reverse ? av > bv : av < bv;
            });
        }
        if (max_count > 0 && list.size() > max_count)
        {
            list.resize(max_count);
        }
        return list;
    }

    void loop()
    {
        get_stats();
        if (parent->waiting > 0)
            return;
        if (state == 100)
            return;
        if (parent->json_output)
        {
            // JSON output

            json11::Json::array array = to_list();
            if (list_pool_id != 0)
            {
                for (auto & a: array)
                {
                    if (a["id"].uint64_value() == list_pool_id)
                    {
                        result.data = a;
                        break;
                    }
                }
            }
            else
                result.data = array;

            state = 100;
            return;
        }
        for (auto & kv: pool_stats)
        {
            double raw_to = kv.second["raw_to_usable"].number_value();
            if (raw_to < 0.000001 && raw_to > -0.000001)
                raw_to = 1;
            kv.second["pg_count_fmt"] = kv.second["real_pg_count"] == kv.second["pg_count"]
                ? kv.second["real_pg_count"].as_string()
                : kv.second["real_pg_count"].as_string()+"->"+kv.second["pg_count"].as_string();
            kv.second["total_fmt"] =  format_size(kv.second["total_raw"].uint64_value() / raw_to);
            kv.second["used_fmt"] = format_size(kv.second["used_raw"].uint64_value() / raw_to);
            kv.second["max_avail_fmt"] = format_size(kv.second["max_available"].uint64_value());
            kv.second["used_pct"] = format_q(kv.second["total_raw"].uint64_value()
                ? (100 - 100*kv.second["max_available"].uint64_value() *
                    kv.second["raw_to_usable"].number_value() / kv.second["total_raw"].uint64_value())
                : 100)+"%";
            kv.second["eff_fmt"] = format_q(kv.second["space_efficiency"].number_value()*100)+"%";
            kv.second["recovery_pct"] = format_q(kv.second["recovery"].number_value()*100)+"%";
            kv.second["osd_tags_fmt"] = item_as_string(kv.second["osd_tags"]);
            kv.second["primary_affinity_tags_fmt"] = item_as_string(kv.second["primary_affinity_tags"]);
            kv.second["read_bw"] = format_size(kv.second["read_bps"].uint64_value())+"/s";
            kv.second["write_bw"] = format_size(kv.second["write_bps"].uint64_value())+"/s";
            kv.second["delete_bw"] = format_size(kv.second["delete_bps"].uint64_value())+"/s";
            kv.second["read_iops"] = format_q(kv.second["read_iops"].number_value());
            kv.second["write_iops"] = format_q(kv.second["write_iops"].number_value());
            kv.second["delete_iops"] = format_q(kv.second["delete_iops"].number_value());
            kv.second["read_lat_f"] = format_lat(kv.second["read_lat"].uint64_value());
            kv.second["write_lat_f"] = format_lat(kv.second["write_lat"].uint64_value());
            kv.second["delete_lat_f"] = format_lat(kv.second["delete_lat"].uint64_value());
        }
        if (list_pool_id != 0)
        {
            auto array = to_list();
            for (auto & a: array)
            {
                if (a["id"].uint64_value() == list_pool_id)
                {
                    result.data = a;
                    break;
                }
            }

            result.text = print_pool_details(result.data, parent->color);
            state = 100;
            return;
        }
        // Table output: id, name, scheme_name, pg_count, total, used, max_avail, used%, efficiency, status, recovery, root_node, failure_domain, osd_tags, primary_affinity_tags
        json11::Json::array cols;
        if (!show_df_format)
        {
            cols.push_back(json11::Json::object{
                { "key", "id" },
                { "title", "ID" },
            });
        }
        cols.push_back(json11::Json::object{
            { "key", "name" },
            { "title", "NAME" },
        });
        cols.push_back(json11::Json::object{
            { "key", "scheme_name" },
            { "title", "SCHEME" },
        });
        cols.push_back(json11::Json::object{
            { "key", "pg_count_fmt" },
            { "title", "PGS" },
        });
        cols.push_back(json11::Json::object{
            { "key", "total_fmt" },
            { "title", "TOTAL" },
        });
        cols.push_back(json11::Json::object{
            { "key", "used_fmt" },
            { "title", "USED" },
        });
        cols.push_back(json11::Json::object{
            { "key", "max_avail_fmt" },
            { "title", "AVAILABLE" },
        });
        cols.push_back(json11::Json::object{
            { "key", "used_pct" },
            { "title", "USED%" },
        });
        cols.push_back(json11::Json::object{
            { "key", "eff_fmt" },
            { "title", "EFFICIENCY" },
        });
        if (!show_df_format)
        {
            cols.push_back(json11::Json::object{
                { "key", "status" },
                { "title", "STATUS" },
            });
            cols.push_back(json11::Json::object{
                { "key", "recovery_pct" },
                { "title", "RECOVERY" },
            });
        }
        if (show_stats)
        {
            cols.push_back(json11::Json::object{
                { "key", "read_bw" },
                { "title", "READ" },
            });
            cols.push_back(json11::Json::object{
                { "key", "read_iops" },
                { "title", "IOPS" },
            });
            cols.push_back(json11::Json::object{
                { "key", "read_lat_f" },
                { "title", "LAT" },
            });
            cols.push_back(json11::Json::object{
                { "key", "write_bw" },
                { "title", "WRITE" },
            });
            cols.push_back(json11::Json::object{
                { "key", "write_iops" },
                { "title", "IOPS" },
            });
            cols.push_back(json11::Json::object{
                { "key", "write_lat_f" },
                { "title", "LAT" },
            });
            cols.push_back(json11::Json::object{
                { "key", "delete_bw" },
                { "title", "DEL" },
            });
            cols.push_back(json11::Json::object{
                { "key", "delete_iops" },
                { "title", "IOPS" },
            });
            cols.push_back(json11::Json::object{
                { "key", "delete_lat_f" },
                { "title", "LAT" },
            });
        }
        if (show_all)
        {
            cols.push_back(json11::Json::object{
                { "key", "root_node" },
                { "title", "ROOT" },
            });
            cols.push_back(json11::Json::object{
                { "key", "failure_domain" },
                { "title", "FAILURE_DOMAIN" },
            });
            cols.push_back(json11::Json::object{
                { "key", "osd_tags_fmt" },
                { "title", "OSD_TAGS" },
            });
            cols.push_back(json11::Json::object{
                { "key", "primary_affinity_tags_fmt" },
                { "title", "AFFINITY_TAGS" },
            });
        }

        result.data = to_list();
        result.text = print_table(result.data, cols, parent->color);
        state = 100;
    }

    std::string print_pool_details(json11::Json items, bool use_esc)
    {
        std::string start_esc = use_esc ? "\033[1m" : "";
        std::string end_esc = use_esc ? "\033[0m" : "";
        std::string result ="Pool details: \n";
        result +=start_esc+"  pool id:                  "+end_esc+ items["id"].as_string() +"\n";
        result +=start_esc+"  pool name:                "+end_esc+ items["name"].as_string() +"\n";
        result +=start_esc+"  scheme:                   "+end_esc+ items["scheme_name"].as_string() +"\n";
        result +=start_esc+"  placement group count:    "+end_esc+ items["pg_count_fmt"].as_string() +"\n";
        result +=start_esc+"  total:                    "+end_esc+ items["total_fmt"].as_string() +"\n";
        result +=start_esc+"  used:                     "+end_esc+ items["used_fmt"].as_string() +" ("+items["used_pct"].as_string()+")"+ "\n";
        result +=start_esc+"  max available:            "+end_esc+ items["max_available"].as_string() +"\n";
        result +=start_esc+"  space efficiency:         "+end_esc+ items["eff_fmt"].as_string() +"\n";
        result +=start_esc+"  status:                   "+end_esc+ items["status"].as_string() +"\n";
        result +=start_esc+"  recovery:                 "+end_esc+ items["recovery_pct"].as_string() +"\n";

        result +=start_esc+"  root node:                "+end_esc+ items["root_node"].as_string() +"\n";
        result +=start_esc+"  failure domain:           "+end_esc+ items["failure_domain"].as_string() +"\n";

        result +=start_esc+"  pg size:                  "+end_esc+ items["pg_size"].as_string() +"\n";
        result +=start_esc+"  pg minsize:               "+end_esc+ items["pg_minsize"].as_string() +"\n";
		result +=start_esc+"  parity chunks:            "+end_esc+ items["parity_chunks"].as_string() +"\n";
		result +=start_esc+"  max osd combinations:     "+end_esc+ items["max_osd_combinations"].as_string() +"\n";
		result +=start_esc+"  block size:               "+end_esc+ items["block_size"].as_string() +"\n";
		result +=start_esc+"  bitmap granularity:       "+end_esc+ items["bitmap_granularity"].as_string() +"\n";
		result +=start_esc+"  pg stripe size:           "+end_esc+ items["pg_stripe_size"].as_string() +"\n";
		result +=start_esc+"  scrub interval:           "+end_esc+ items["scrub_interval"].as_string() +"\n";

        result +=start_esc+"  osd count:                "+end_esc+ items["osd_count"].as_string() +"\n";
        result +=start_esc+"  osd:                      "+end_esc+ item_as_string(items["osd_set"]) +"\n";
        result +=start_esc+"  osd tags:                 "+end_esc+ item_as_string(items["osd_tags"]) +"\n";
        result +=start_esc+"  primary affinity tags:    "+end_esc+ item_as_string(items["primary_affinity_tags"]) +"\n";

        result +=start_esc+"  read bandwidth:           "+end_esc+ items["read_bw"].as_string() +"\n";
        result +=start_esc+"  read IOPS:                "+end_esc+ items["read_iops"].as_string() +"\n";
        result +=start_esc+"  read latency:             "+end_esc+ items["read_lat_f"].as_string() +"\n";
        result +=start_esc+"  write bandwidth:          "+end_esc+ items["write_bw"].as_string() +"\n";
        result +=start_esc+"  write IOPS:               "+end_esc+ items["write_iops"].as_string() +"\n";
        result +=start_esc+"  write latency:            "+end_esc+ items["write_lat_f"].as_string() +"\n";
        result +=start_esc+"  delete bandwidth:         "+end_esc+ items["delete_bw"].as_string() +"\n";
        result +=start_esc+"  delete IOPS:              "+end_esc+ items["delete_iops"].as_string() +"\n";
        result +=start_esc+"  delete latency:           "+end_esc+ items["delete_lat_f"].as_string() +"\n";

        return result;
    }
};

std::function<bool(cli_result_t &)> cli_tool_t::start_pool_ls(json11::Json cfg)
{
    auto lister = new pool_ls_t();
    lister->parent = this;
    lister->list_pool_id = cfg["pool"].uint64_value();
    lister->list_pool_name = lister->list_pool_id ? "" : cfg["pool"].as_string();
    lister->show_all = cfg["long"].bool_value();
    lister->show_stats = cfg["stats"].bool_value();
    lister->sort_field = cfg["sort"].string_value();
    lister->show_df_format = cfg["dfformat"].bool_value();
    if ((lister->sort_field == "osd_tags") ||
        (lister->sort_field == "primary_affinity_tags" ))
        lister->sort_field = lister->sort_field + "_fmt";
    lister->reverse = cfg["reverse"].bool_value();
    lister->max_count = cfg["count"].uint64_value();
    for (auto & item: cfg["names"].array_items())
    {
        lister->only_names.insert(item.string_value());
    }
    return [lister](cli_result_t & result)
    {
        lister->loop();
        if (lister->is_done())
        {
            result = lister->result;
            delete lister;
            return true;
        }
        return false;
    };
}
