// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "cli_fix.h"
#include "cluster_client.h"
#include "pg_states.h"
#include "str_util.h"
#include "json_util.h"

std::vector<uint64_t> parse_uint64_list(json11::Json val);

// Find object(s) in the cluster using raw secondary listing operations.
// Only for troubleshooting.
struct cli_raw_ls_t
{
    json11::Json cfg;
    pool_id_t pool_id = 0;
    pg_num_t pg_num = 0;
    pg_num_t pg_count = 0;
    uint32_t pg_stripe_size = 0;
    std::set<uint64_t> osds;
    std::vector<osd_num_t> osd_list;
    uint64_t min_inode = 0, max_inode = 0, min_offset = 0, max_offset = 0;
    bool offset_set = false;

    cli_tool_t *parent = NULL;
    int state = 0;
    cli_result_t result;
    bool first = true;
    size_t osd_pos = 0;

    bool is_done()
    {
        return state == 100;
    }

    void loop()
    {
        if (state == 1)
            goto resume_1;
        if (state == 100)
            return;
        pool_id = cfg["pool_id"].uint64_value();
        pg_num = (pg_num_t)cfg["pg_num"].uint64_value();
        pg_count = (pg_num_t)cfg["pg_count"].uint64_value();
        pg_stripe_size = cfg["pg_stripe_size"].uint64_value();
        if (!pool_id)
        {
            result = (cli_result_t){ .err = EINVAL, .text = "Pool ID is required" };
            state = 100;
            return;
        }
        if (cfg["min_inode"].is_null() && cfg["max_inode"].is_null())
        {
            min_inode = INODE_WITH_POOL(cfg["pool_id"].uint64_value(), 0);
            max_inode = INODE_WITH_POOL(cfg["pool_id"].uint64_value(), UINT64_MAX);
        }
        else
        {
            min_inode = INODE_WITH_POOL(cfg["pool_id"].uint64_value(), stoull_full(cfg["min_inode"].as_string()));
            max_inode = INODE_WITH_POOL(cfg["pool_id"].uint64_value(), stoull_full(cfg["max_inode"].as_string()));
        }
        if (cfg["offset"].is_null())
        {
            if (!pg_num)
            {
                result = (cli_result_t){ .err = EINVAL, .text = "PG number is required if offset is not specified" };
                state = 100;
                return;
            }
            min_offset = 0;
            max_offset = UINT64_MAX;
        }
        else
        {
            min_offset = max_offset = stoull_full(cfg["offset"].as_string());
            offset_set = true;
        }
        for (auto osd_num: parse_uint64_list(cfg["osds"]))
        {
            osds.insert(osd_num);
        }
        if (!pg_count || !pg_stripe_size || !osds.size())
        {
            auto pool_it = parent->cli->st_cli->pool_config.find(pool_id);
            if (pool_it == parent->cli->st_cli->pool_config.end())
            {
                result = (cli_result_t){ .err = EINVAL, .text = "pg_count, pg_stripe_size and osds are required if the pool does not exist" };
                state = 100;
                return;
            }
            if (!pg_count)
            {
                pg_count = pool_it->second.real_pg_count;
            }
            if (!pg_stripe_size)
            {
                pg_stripe_size = pool_it->second.pg_stripe_size;
            }
            if (!osds.size())
            {
                for (auto & pgp: pool_it->second.pg_config)
                {
                    for (osd_num_t pg_osd: pgp.second.target_set)
                        osds.insert(pg_osd);
                    for (osd_num_t pg_osd: pgp.second.all_peers)
                        osds.insert(pg_osd);
                    for (auto & hist_item: pgp.second.target_history)
                        for (auto pg_osd: hist_item)
                            osds.insert(pg_osd);
                }
                osds.erase(0);
            }
        }
        if (offset_set)
        {
            min_offset = (min_offset / pg_stripe_size) * pg_stripe_size;
            max_offset = ((max_offset / pg_stripe_size) * pg_stripe_size) | STRIPE_MASK;
            pg_num = (min_offset/pg_stripe_size) % pg_count + 1; // like map_to_pg()
            fprintf(stderr, "Selected PG %u\n", pg_num);
        }
        parent->cli->init_msgr();
        osd_list = std::vector<osd_num_t>(osds.begin(), osds.end());
        osd_pos = 0;
        state = 1;
    resume_1:
        for (; osd_pos < osd_list.size() && parent->waiting < parent->parallel_osds; osd_pos++)
        {
            uint64_t osd_num = osd_list[osd_pos];
            if (parent->cli->st_cli->peer_states[osd_num].is_null())
            {
                fprintf(stderr, "OSD %ju is unavailable, skipping\n", osd_num);
                continue;
            }
            osd_op_t *op = new osd_op_t;
            op->req = (osd_any_op_t){
                .sec_list = {
                    .header = {
                        .magic = SECONDARY_OSD_OP_MAGIC,
                        .opcode = OSD_OP_SEC_LIST,
                    },
                    .list_pg = pg_num,
                    .pg_count = pg_count,
                    .pg_stripe_size = pg_stripe_size,
                    .min_inode = min_inode,
                    .max_inode = max_inode,
                    .min_stripe = min_offset,
                    .max_stripe = max_offset,
                },
            };
            op->callback = [this, osd_num](osd_op_t *op)
            {
                if (op->reply.hdr.retval < 0)
                {
                    fprintf(stderr, "OSD %ju listing failed: retval=%jd\n", osd_num, op->reply.hdr.retval);
                }
                else
                {
                    for (uint64_t i = 0; i < op->reply.hdr.retval; i++)
                    {
                        auto & ov = ((obj_ver_id*)op->buf)[i];
                        if (parent->json_output)
                        {
                            printf("%s{\"osd\":%ju,\"inode\":\"0x%jx\",\"stripe\":\"0x%jx\",\"version\":%ju,\"stable\":%s}",
                                first ? "" : ",\n", osd_num, ov.oid.inode, ov.oid.stripe, ov.version,
                                i < op->reply.sec_list.stable_count ? "true" : "false");
                            first = false;
                        }
                        else
                        {
                            printf("OSD %ju - %jx:%jx v%ju%s\n",
                                osd_num, ov.oid.inode, ov.oid.stripe, ov.version,
                                i < op->reply.sec_list.stable_count ? " stable" : "");
                        }
                    }
                }
                parent->waiting--;
                loop();
                delete op;
            };
            parent->waiting++;
            parent->cli->execute_raw(osd_num, op);
        }
        if (parent->waiting > 0)
        {
            return;
        }
        if (!first)
        {
            printf("\n");
        }
        state = 100;
    }
};

std::function<bool(cli_result_t &)> cli_tool_t::start_raw_ls(json11::Json cfg)
{
    auto raw_ls = new cli_raw_ls_t();
    raw_ls->parent = this;
    raw_ls->cfg = cfg;
    return [raw_ls](cli_result_t & result)
    {
        raw_ls->loop();
        if (raw_ls->is_done())
        {
            result = raw_ls->result;
            delete raw_ls;
            return true;
        }
        return false;
    };
}
