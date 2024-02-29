// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <algorithm>
#include "cli.h"
#include "cluster_client.h"
#include "str_util.h"
#include "pg_states.h"

// List pools with space statistics
// - df - minimal list with % used space
// - pool-ls - same but with PG state and recovery %
// - pool-ls -l - same but also include I/O statistics
// - pool-ls --detail - use list format, include PG states, I/O stats and all pool parameters
struct pool_lister_t
{
    cli_tool_t *parent;
    std::string sort_field;
    std::set<std::string> only_names;
    bool reverse = false;
    int max_count = 0;
    bool show_recovery = false;
    bool show_stats = false;
    bool detailed = false;

    int state = 0;
    cli_result_t result;
    std::map<pool_id_t, json11::Json::object> pool_stats;
    struct io_stats_t
    {
        uint64_t count = 0;
        uint64_t read_iops = 0;
        uint64_t read_bps = 0;
        uint64_t read_lat = 0;
        uint64_t write_iops = 0;
        uint64_t write_bps = 0;
        uint64_t write_lat = 0;
        uint64_t delete_iops = 0;
        uint64_t delete_bps = 0;
        uint64_t delete_lat = 0;
    };
    struct object_counts_t
    {
        uint64_t object_count = 0;
        uint64_t misplaced_count = 0;
        uint64_t degraded_count = 0;
        uint64_t incomplete_count = 0;
    };

    bool is_done()
    {
        return state == 100;
    }

    void get_pool_stats(int base_state)
    {
        if (state == base_state+1)
            goto resume_1;
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
                            parent->cli->st_cli.etcd_prefix+"/config/pools"
                        ) },
                    } },
                },
            } },
        });
        state = base_state+1;
resume_1:
        if (parent->waiting > 0)
            return;
        if (parent->etcd_err.err)
        {
            result = parent->etcd_err;
            state = 100;
            return;
        }
        auto space_info = parent->etcd_result;
        auto config_pools = space_info["responses"][2]["response_range"]["kvs"][0];
        if (!config_pools.is_null())
        {
            config_pools = parent->cli->st_cli.parse_etcd_kv(config_pools).value;
        }
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
        std::map<pool_id_t, uint64_t> osd_free;
        for (auto & kv_item: space_info["responses"][1]["response_range"]["kvs"].array_items())
        {
            auto kv = parent->cli->st_cli.parse_etcd_kv(kv_item);
            // osd ID
            osd_num_t osd_num;
            char null_byte = 0;
            int scanned = sscanf(kv.key.substr(parent->cli->st_cli.etcd_prefix.length()).c_str(), "/osd/stats/%ju%c", &osd_num, &null_byte);
            if (scanned != 1 || !osd_num || osd_num >= POOL_ID_MAX)
            {
                fprintf(stderr, "Invalid key in etcd: %s\n", kv.key.c_str());
                continue;
            }
            // osd/stats/<N>::free
            osd_free[osd_num] = kv.value["free"].uint64_value();
        }
        // Calculate max_avail for each pool
        for (auto & pp: parent->cli->st_cli.pool_config)
        {
            auto & pool_cfg = pp.second;
            uint64_t pool_avail = UINT64_MAX;
            std::map<osd_num_t, uint64_t> pg_per_osd;
            bool active = pool_cfg.real_pg_count > 0;
            uint64_t pg_states = 0;
            for (auto & pgp: pool_cfg.pg_config)
            {
                if (!(pgp.second.cur_state & PG_ACTIVE))
                {
                    active = false;
                }
                pg_states |= pgp.second.cur_state;
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
                uint64_t pg_free = osd_free[pg_per_pair.first] * pool_cfg.real_pg_count / pg_per_pair.second;
                if (pool_avail > pg_free)
                {
                    pool_avail = pg_free;
                }
            }
            if (pool_avail == UINT64_MAX)
            {
                pool_avail = 0;
            }
            if (pool_cfg.scheme != POOL_SCHEME_REPLICATED)
            {
                pool_avail *= (pool_cfg.pg_size - pool_cfg.parity_chunks);
            }
            // incomplete > has_incomplete > degraded > has_degraded > has_misplaced
            std::string status;
            if (!active)
                status = "inactive";
            else if (pg_states & PG_INCOMPLETE)
                status = "incomplete";
            else if (pg_states & PG_HAS_INCOMPLETE)
                status = "has_incomplete";
            else if (pg_states & PG_DEGRADED)
                status = "degraded";
            else if (pg_states & PG_HAS_DEGRADED)
                status = "has_degraded";
            else if (pg_states & PG_HAS_MISPLACED)
                status = "has_misplaced";
            else
                status = "active";
            pool_stats[pool_cfg.id] = json11::Json::object {
                { "id", (uint64_t)pool_cfg.id },
                { "name", pool_cfg.name },
                { "status", status },
                { "pg_count", pool_cfg.pg_count },
                { "real_pg_count", pool_cfg.real_pg_count },
                { "scheme_name", pool_cfg.scheme == POOL_SCHEME_REPLICATED
                    ? std::to_string(pool_cfg.pg_size)+"/"+std::to_string(pool_cfg.pg_minsize)
                    : "EC "+std::to_string(pool_cfg.pg_size-pool_cfg.parity_chunks)+"+"+std::to_string(pool_cfg.parity_chunks) },
                { "used_raw", (uint64_t)(pool_stats[pool_cfg.id]["used_raw_tb"].number_value() * ((uint64_t)1<<40)) },
                { "total_raw", (uint64_t)(pool_stats[pool_cfg.id]["total_raw_tb"].number_value() * ((uint64_t)1<<40)) },
                { "max_available", pool_avail },
                { "raw_to_usable", pool_stats[pool_cfg.id]["raw_to_usable"].number_value() },
                { "space_efficiency", pool_stats[pool_cfg.id]["space_efficiency"].number_value() },
                { "pg_real_size", pool_stats[pool_cfg.id]["pg_real_size"].uint64_value() },
                { "osd_count", pg_per_osd.size() },
            };
        }
        // Include full pool config
        for (auto & pp: config_pools.object_items())
        {
            if (!pp.second.is_object())
            {
                continue;
            }
            auto pool_id = stoull_full(pp.first);
            auto & st = pool_stats[pool_id];
            for (auto & kv: pp.second.object_items())
            {
                if (st.find(kv.first) == st.end())
                    st[kv.first] = kv.second;
            }
        }
    }

    void get_pg_stats(int base_state)
    {
        if (state == base_state+1)
            goto resume_1;
        // Space statistics - pool/stats/<pool>
        parent->etcd_txn(json11::Json::object {
            { "success", json11::Json::array {
                json11::Json::object {
                    { "request_range", json11::Json::object {
                        { "key", base64_encode(
                            parent->cli->st_cli.etcd_prefix+"/pg/stats/"
                        ) },
                        { "range_end", base64_encode(
                            parent->cli->st_cli.etcd_prefix+"/pg/stats0"
                        ) },
                    } },
                },
            } },
        });
        state = base_state+1;
resume_1:
        if (parent->waiting > 0)
            return;
        if (parent->etcd_err.err)
        {
            result = parent->etcd_err;
            state = 100;
            return;
        }
        auto pg_stats = parent->etcd_result["responses"][0]["response_range"]["kvs"];
        // Calculate recovery percent
        std::map<pool_id_t, object_counts_t> counts;
        for (auto & kv_item: pg_stats.array_items())
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
            auto & cnt = counts[pool_id];
            cnt.object_count += kv.value["object_count"].uint64_value();
            cnt.misplaced_count += kv.value["misplaced_count"].uint64_value();
            cnt.degraded_count += kv.value["degraded_count"].uint64_value();
            cnt.incomplete_count += kv.value["incomplete_count"].uint64_value();
        }
        for (auto & pp: pool_stats)
        {
            auto & cnt = counts[pp.first];
            auto & st = pp.second;
            st["object_count"] = cnt.object_count;
            st["misplaced_count"] = cnt.misplaced_count;
            st["degraded_count"] = cnt.degraded_count;
            st["incomplete_count"] = cnt.incomplete_count;
        }
    }

    void get_inode_stats(int base_state)
    {
        if (state == base_state+1)
            goto resume_1;
        // Space statistics - pool/stats/<pool>
        parent->etcd_txn(json11::Json::object {
            { "success", json11::Json::array {
                json11::Json::object {
                    { "request_range", json11::Json::object {
                        { "key", base64_encode(
                            parent->cli->st_cli.etcd_prefix+"/inode/stats/"
                        ) },
                        { "range_end", base64_encode(
                            parent->cli->st_cli.etcd_prefix+"/inode/stats0"
                        ) },
                    } },
                },
            } },
        });
        state = base_state+1;
resume_1:
        if (parent->waiting > 0)
            return;
        if (parent->etcd_err.err)
        {
            result = parent->etcd_err;
            state = 100;
            return;
        }
        auto inode_stats = parent->etcd_result["responses"][0]["response_range"]["kvs"];
        // Performance statistics
        std::map<pool_id_t, io_stats_t> pool_io;
        for (auto & kv_item: inode_stats.array_items())
        {
            auto kv = parent->cli->st_cli.parse_etcd_kv(kv_item);
            // pool ID & inode number
            pool_id_t pool_id;
            inode_t only_inode_num;
            char null_byte = 0;
            int scanned = sscanf(kv.key.substr(parent->cli->st_cli.etcd_prefix.length()).c_str(),
                "/inode/stats/%u/%ju%c", &pool_id, &only_inode_num, &null_byte);
            if (scanned != 2 || !pool_id || pool_id >= POOL_ID_MAX || INODE_POOL(only_inode_num) != 0)
            {
                fprintf(stderr, "Invalid key in etcd: %s\n", kv.key.c_str());
                continue;
            }
            auto & io = pool_io[pool_id];
            io.read_iops += kv.value["read"]["iops"].uint64_value();
            io.read_bps += kv.value["read"]["bps"].uint64_value();
            io.read_lat += kv.value["read"]["lat"].uint64_value();
            io.write_iops += kv.value["write"]["iops"].uint64_value();
            io.write_bps += kv.value["write"]["bps"].uint64_value();
            io.write_lat += kv.value["write"]["lat"].uint64_value();
            io.delete_iops += kv.value["delete"]["iops"].uint64_value();
            io.delete_bps += kv.value["delete"]["bps"].uint64_value();
            io.delete_lat += kv.value["delete"]["lat"].uint64_value();
            io.count++;
        }
        for (auto & pp: pool_stats)
        {
            auto & io = pool_io[pp.first];
            if (io.count > 0)
            {
                io.read_lat /= io.count;
                io.write_lat /= io.count;
                io.delete_lat /= io.count;
            }
            auto & st = pp.second;
            st["read_iops"] = io.read_iops;
            st["read_bps"] = io.read_bps;
            st["read_lat"] = io.read_lat;
            st["write_iops"] = io.write_iops;
            st["write_bps"] = io.write_bps;
            st["write_lat"] = io.write_lat;
            st["delete_iops"] = io.delete_iops;
            st["delete_bps"] = io.delete_bps;
            st["delete_lat"] = io.delete_lat;
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
        if (sort_field == "name" || sort_field == "scheme" ||
            sort_field == "scheme_name" || sort_field == "status")
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
        if (state == 1)
            goto resume_1;
        if (state == 2)
            goto resume_2;
        if (state == 3)
            goto resume_3;
        if (state == 100)
            return;
        show_stats = show_stats || detailed;
        show_recovery = show_recovery || detailed;
resume_1:
        get_pool_stats(0);
        if (parent->waiting > 0)
            return;
        if (show_stats)
        {
resume_2:
            get_inode_stats(1);
            if (parent->waiting > 0)
                return;
        }
        if (show_recovery)
        {
resume_3:
            get_pg_stats(2);
            if (parent->waiting > 0)
                return;
        }
        if (parent->json_output)
        {
            // JSON output
            result.data = to_list();
            state = 100;
            return;
        }
        json11::Json::array list;
        for (auto & kv: pool_stats)
        {
            auto & st = kv.second;
            double raw_to = st["raw_to_usable"].number_value();
            if (raw_to < 0.000001 && raw_to > -0.000001)
                raw_to = 1;
            st["pg_count_fmt"] = st["real_pg_count"] == st["pg_count"]
                ? st["real_pg_count"].as_string()
                : st["real_pg_count"].as_string()+"->"+st["pg_count"].as_string();
            st["total_fmt"] = format_size(st["total_raw"].uint64_value() / raw_to);
            st["used_fmt"] = format_size(st["used_raw"].uint64_value() / raw_to);
            st["max_avail_fmt"] = format_size(st["max_available"].uint64_value());
            st["used_pct"] = format_q(st["total_raw"].uint64_value()
                ? (100 - 100*st["max_available"].uint64_value() *
                    st["raw_to_usable"].number_value() / st["total_raw"].uint64_value())
                : 100)+"%";
            st["eff_fmt"] = format_q(st["space_efficiency"].number_value()*100)+"%";
            if (show_stats)
            {
                st["read_bw"] = format_size(st["read_bps"].uint64_value())+"/s";
                st["write_bw"] = format_size(st["write_bps"].uint64_value())+"/s";
                st["delete_bw"] = format_size(st["delete_bps"].uint64_value())+"/s";
                st["read_iops"] = format_q(st["read_iops"].number_value());
                st["write_iops"] = format_q(st["write_iops"].number_value());
                st["delete_iops"] = format_q(st["delete_iops"].number_value());
                st["read_lat_f"] = format_lat(st["read_lat"].uint64_value());
                st["write_lat_f"] = format_lat(st["write_lat"].uint64_value());
                st["delete_lat_f"] = format_lat(st["delete_lat"].uint64_value());
            }
            if (show_recovery)
            {
                auto object_count = st["object_count"].uint64_value();
                auto recovery_pct = 100.0 * (object_count - (st["misplaced_count"].uint64_value() +
                    st["degraded_count"].uint64_value() + st["incomplete_count"].uint64_value())) /
                    (object_count ? object_count : 1);
                st["recovery_fmt"] = format_q(recovery_pct)+"%";
            }
        }
        if (detailed)
        {
            for (auto & kv: pool_stats)
            {
                auto & st = kv.second;
                auto total = st["object_count"].uint64_value();
                auto obj_size = st["block_size"].uint64_value();
                if (!obj_size)
                    obj_size = parent->cli->st_cli.global_block_size;
                if (st["scheme"] == "ec")
                    obj_size *= st["pg_size"].uint64_value() - st["parity_chunks"].uint64_value();
                else if (st["scheme"] == "xor")
                    obj_size *= st["pg_size"].uint64_value() - 1;
                auto n = st["misplaced_count"].uint64_value();
                if (n > 0)
                    st["misplaced_fmt"] = format_size(n * obj_size) + " / " + format_q(100.0 * n / total);
                n = st["degraded_count"].uint64_value();
                if (n > 0)
                    st["degraded_fmt"] = format_size(n * obj_size) + " / " + format_q(100.0 * n / total);
                n = st["incomplete_count"].uint64_value();
                if (n > 0)
                    st["incomplete_fmt"] = format_size(n * obj_size) + " / " + format_q(100.0 * n / total);
                st["read_fmt"] = st["read_bw"].string_value()+", "+st["read_iops"].string_value()+" op/s, "+
                    st["read_lat_f"].string_value()+" lat";
                st["write_fmt"] = st["write_bw"].string_value()+", "+st["write_iops"].string_value()+" op/s, "+
                    st["write_lat_f"].string_value()+" lat";
                st["delete_fmt"] = st["delete_bw"].string_value()+", "+st["delete_iops"].string_value()+" op/s, "+
                    st["delete_lat_f"].string_value()+" lat";
                if (st["scheme"] == "replicated")
                    st["scheme_name"] = "x"+st["pg_size"].as_string();
                if (st["failure_domain"].string_value() == "")
                    st["failure_domain"] = "host";
                st["osd_tags_fmt"] = implode(", ", st["osd_tags"]);
                st["primary_affinity_tags_fmt"] = implode(", ", st["primary_affinity_tags"]);
                if (st["block_size"].uint64_value())
                    st["block_size_fmt"] = format_size(st["block_size"].uint64_value());
                if (st["bitmap_granularity"].uint64_value())
                    st["bitmap_granularity_fmt"] = format_size(st["bitmap_granularity"].uint64_value());
                if (st["no_inode_stats"].bool_value())
                    st["inode_stats_fmt"] = "disabled";
            }
            // All pool parameters are only displayed in the "detailed" mode
            // because there's too many of them to show them in table
            auto cols = std::vector<std::pair<std::string, std::string>>{
                { "name", "Name" },
                { "id", "ID" },
                { "scheme_name", "Scheme" },
                { "status", "Status" },
                { "pg_count_fmt", "PGs" },
                { "pg_minsize", "PG minsize" },
                { "failure_domain", "Failure domain" },
                { "root_node", "Root node" },
                { "osd_tags_fmt", "OSD tags" },
                { "primary_affinity_tags_fmt", "Primary affinity" },
                { "block_size_fmt", "Block size" },
                { "bitmap_granularity_fmt", "Bitmap granularity" },
                { "immediate_commit", "Immediate commit" },
                { "scrub_interval", "Scrub interval" },
                { "inode_stats_fmt", "Per-inode stats" },
                { "pg_stripe_size", "PG stripe size" },
                { "max_osd_combinations", "Max OSD combinations" },
                { "total_fmt", "Total" },
                { "used_fmt", "Used" },
                { "max_avail_fmt", "Available" },
                { "used_pct", "Used%" },
                { "eff_fmt", "Efficiency" },
                { "osd_count", "OSD count" },
                { "misplaced_fmt", "Misplaced" },
                { "degraded_fmt", "Degraded" },
                { "incomplete_fmt", "Incomplete" },
                { "read_fmt", "Read" },
                { "write_fmt", "Write" },
                { "delete_fmt", "Delete" },
            };
            auto list = to_list();
            size_t title_len = 0;
            for (auto & item: list)
            {
                title_len = print_detail_title_len(item, cols, title_len);
            }
            for (auto & item: list)
            {
                if (result.text != "")
                    result.text += "\n";
                result.text += print_detail(item, cols, title_len, parent->color);
            }
            state = 100;
            return;
        }
        // Table output: name, scheme_name, pg_count, total, used, max_avail, used%, efficiency
        json11::Json::array cols;
        cols.push_back(json11::Json::object{
            { "key", "name" },
            { "title", "NAME" },
        });
        cols.push_back(json11::Json::object{
            { "key", "scheme_name" },
            { "title", "SCHEME" },
        });
        cols.push_back(json11::Json::object{
            { "key", "status" },
            { "title", "STATUS" },
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
        if (show_recovery)
        {
            cols.push_back(json11::Json::object{ { "key", "recovery_fmt" }, { "title", "RECOVERY" } });
        }
        if (show_stats)
        {
            cols.push_back(json11::Json::object{ { "key", "read_bw" }, { "title", "READ" } });
            cols.push_back(json11::Json::object{ { "key", "read_iops" }, { "title", "IOPS" } });
            cols.push_back(json11::Json::object{ { "key", "read_lat_f" }, { "title", "LAT" } });
            cols.push_back(json11::Json::object{ { "key", "write_bw" }, { "title", "WRITE" } });
            cols.push_back(json11::Json::object{ { "key", "write_iops" }, { "title", "IOPS" } });
            cols.push_back(json11::Json::object{ { "key", "write_lat_f" }, { "title", "LAT" } });
            cols.push_back(json11::Json::object{ { "key", "delete_bw" }, { "title", "DELETE" } });
            cols.push_back(json11::Json::object{ { "key", "delete_iops" }, { "title", "IOPS" } });
            cols.push_back(json11::Json::object{ { "key", "delete_lat_f" }, { "title", "LAT" } });
        }
        result.data = to_list();
        result.text = print_table(result.data, cols, parent->color);
        state = 100;
    }
};

size_t print_detail_title_len(json11::Json item, std::vector<std::pair<std::string, std::string>> names, size_t prev_len)
{
    size_t title_len = prev_len;
    for (auto & kv: names)
    {
        if (!item[kv.first].is_null() && (!item[kv.first].is_string() || item[kv.first].string_value() != ""))
        {
            size_t len = utf8_length(kv.second);
            title_len = title_len < len ? len : title_len;
        }
    }
    return title_len;
}

std::string print_detail(json11::Json item, std::vector<std::pair<std::string, std::string>> names, size_t title_len, bool use_esc)
{
    std::string str;
    for (auto & kv: names)
    {
        if (!item[kv.first].is_null() && (!item[kv.first].is_string() || item[kv.first].string_value() != ""))
        {
            str += kv.second;
            str += ": ";
            size_t len = utf8_length(kv.second);
            for (int j = 0; j < title_len-len; j++)
                str += ' ';
            if (use_esc)
                str += "\033[1m";
            str += item[kv.first].as_string();
            if (use_esc)
                str += "\033[0m";
            str += "\n";
        }
    }
    return str;
}

std::function<bool(cli_result_t &)> cli_tool_t::start_pool_ls(json11::Json cfg)
{
    auto lister = new pool_lister_t();
    lister->parent = this;
    lister->show_recovery = cfg["show_recovery"].bool_value();
    lister->show_stats = cfg["long"].bool_value();
    lister->detailed = cfg["detail"].bool_value();
    lister->sort_field = cfg["sort"].string_value();
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

std::string implode(const std::string & sep, json11::Json array)
{
    if (array.is_number() || array.is_bool() || array.is_string())
    {
        return array.as_string();
    }
    std::string res;
    bool first = true;
    for (auto & item: array.array_items())
    {
        res += (first ? item.as_string() : sep+item.as_string());
        first = false;
    }
    return res;
}
