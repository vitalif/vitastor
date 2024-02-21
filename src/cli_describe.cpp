// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "cli_fix.h"
#include "cluster_client.h"
#include "pg_states.h"
#include "str_util.h"

std::vector<uint64_t> parse_uint64_list(json11::Json val)
{
    std::vector<uint64_t> ret;
    if (val.is_number())
        ret.push_back(val.uint64_value());
    else if (val.is_string())
    {
        const std::string & s = val.string_value();
        for (int i = 0, p = -1; i <= s.size(); i++)
        {
            if (p < 0 && i < s.size() && (isdigit(s[i]) || s[i] == 'x'))
                p = i;
            else if (p >= 0 && (i >= s.size() || !isdigit(s[i]) && s[i] != 'x'))
            {
                ret.push_back(stoull_full(s.substr(p, i-p), 0));
                p = -1;
            }
        }
    }
    else if (val.is_array())
    {
        for (auto & pg_num: val.array_items())
            ret.push_back(pg_num.uint64_value());
    }
    return ret;
}

struct cli_describe_t
{
    uint64_t object_state = 0;
    pool_id_t only_pool = 0;
    std::vector<uint64_t> only_osds;
    uint64_t min_inode = 0, max_inode = 0;
    uint64_t min_offset = 0, max_offset = 0;

    cli_tool_t *parent = NULL;
    int state = 0;
    int count = 0;

    json11::Json options;
    cli_result_t result;
    json11::Json::array describe_items;

    bool is_done()
    {
        return state == 100;
    }

    void parse_options(json11::Json cfg)
    {
        only_pool = cfg["pool"].uint64_value();
        if (!only_pool && cfg["pool"].is_string())
        {
            for (auto & pp: parent->cli->st_cli.pool_config)
            {
                if (pp.second.name == cfg["pool"].string_value())
                {
                    only_pool = pp.first;
                    break;
                }
            }
        }
        min_inode = cfg["inode"].uint64_value();
        if (min_inode)
        {
            if (!INODE_POOL(min_inode))
                min_inode |= (uint64_t)only_pool << (64-POOL_ID_BITS);
            max_inode = min_inode;
            min_offset = max_offset = 0;
        }
        else
        {
            min_inode = stoull_full(cfg["min_inode"].string_value(), 0); // to support 0x...
            max_inode = stoull_full(cfg["max_inode"].string_value(), 0);
            min_offset = stoull_full(cfg["min_offset"].string_value(), 0);
            max_offset = stoull_full(cfg["max_offset"].string_value(), 0);
            if (!min_inode && !max_inode && only_pool)
            {
                min_inode = (uint64_t)only_pool << (64-POOL_ID_BITS);
                max_inode = ((uint64_t)only_pool << (64-POOL_ID_BITS)) |
                    (((uint64_t)1 << (64-POOL_ID_BITS)) - 1);
            }
        }
        only_osds = parse_uint64_list(cfg["osds"]);
        object_state = stoull_full(cfg["object_state"].string_value(), 0);
        if (!object_state && cfg["object_state"].is_string())
        {
            if (cfg["object_state"].string_value().find("inconsistent") != std::string::npos)
                object_state |= OBJ_INCONSISTENT;
            if (cfg["object_state"].string_value().find("corrupted") != std::string::npos)
                object_state |= OBJ_CORRUPTED;
            if (cfg["object_state"].string_value().find("incomplete") != std::string::npos)
                object_state |= OBJ_INCOMPLETE;
            if (cfg["object_state"].string_value().find("degraded") != std::string::npos)
                object_state |= OBJ_DEGRADED;
            if (cfg["object_state"].string_value().find("misplaced") != std::string::npos)
                object_state |= OBJ_MISPLACED;
        }
    }

    void loop()
    {
        if (state == 1)
            goto resume_1;
        if (state == 100)
            return;
        parse_options(options);
        if (min_inode && !INODE_POOL(min_inode))
        {
            result = (cli_result_t){ .err = EINVAL, .text = "Pool is not specified" };
            state = 100;
            return;
        }
        if (!only_osds.size())
        {
            uint64_t min_pool = min_inode >> (64-POOL_ID_BITS);
            uint64_t max_pool = max_inode >> (64-POOL_ID_BITS);
            for (auto & pp: parent->cli->st_cli.pool_config)
            {
                if (pp.first >= min_pool && (!max_pool || pp.first <= max_pool))
                {
                    for (auto & pgp: pp.second.pg_config)
                        only_osds.push_back(pgp.second.cur_primary);
                }
            }
        }
        remove_duplicates(only_osds);
        parent->cli->init_msgr();
        if (parent->json_output && parent->is_command_line)
        {
            printf("[\n");
        }
        for (int i = 0; i < only_osds.size(); i++)
        {
            osd_op_t *op = new osd_op_t;
            op->req = (osd_any_op_t){
                .describe = {
                    .header = {
                        .magic = SECONDARY_OSD_OP_MAGIC,
                        .id = parent->cli->next_op_id(),
                        .opcode = OSD_OP_DESCRIBE,
                    },
                    .object_state = object_state,
                    .min_inode = min_inode,
                    .min_offset = min_offset,
                    .max_inode = max_inode,
                    .max_offset = max_offset,
                },
            };
            op->callback = [this, osd_num = only_osds[i]](osd_op_t *op)
            {
                if (op->reply.hdr.retval < 0)
                {
                    fprintf(
                        stderr, "Failed to describe objects on OSD %ju (retval=%jd)\n",
                        osd_num, op->reply.hdr.retval
                    );
                }
                else if (op->reply.describe.result_bytes != op->reply.hdr.retval * sizeof(osd_reply_describe_item_t))
                {
                    fprintf(
                        stderr, "Invalid response size from OSD %ju (expected %ju bytes, got %ju bytes)\n",
                        osd_num, op->reply.hdr.retval * sizeof(osd_reply_describe_item_t), op->reply.describe.result_bytes
                    );
                }
                else
                {
                    osd_reply_describe_item_t *items = (osd_reply_describe_item_t *)op->buf;
                    for (int i = 0; i < op->reply.hdr.retval; i++)
                    {
                        if (!parent->json_output || parent->is_command_line)
                        {
#define FMT "{\"inode\":\"0x%jx\",\"stripe\":\"0x%jx\",\"part\":%u,\"osd_num\":%ju%s%s%s}"
                            printf(
                                (parent->json_output
                                    ? (count > 0 ? ",\n  " FMT : "  " FMT)
                                    : "%jx:%jx part %u on OSD %ju%s%s%s\n"),
#undef FMT
                                items[i].inode, items[i].stripe,
                                items[i].role, items[i].osd_num,
                                (items[i].loc_bad & LOC_CORRUPTED ? (parent->json_output ? ",\"corrupted\":true" : " corrupted") : ""),
                                (items[i].loc_bad & LOC_INCONSISTENT ? (parent->json_output ? ",\"inconsistent\":true" : " inconsistent") : ""),
                                (items[i].loc_bad & LOC_OUTDATED ? (parent->json_output ? ",\"outdated\":true" : " outdated") : "")
                            );
                        }
                        else
                        {
                            auto json_item = json11::Json::object {
                                { "inode", (uint64_t)items[i].inode },
                                { "stripe", (uint64_t)items[i].stripe },
                                { "part", (uint64_t)items[i].role },
                                { "osd_num", (uint64_t)items[i].osd_num },
                            };
                            if (items[i].loc_bad & LOC_CORRUPTED)
                                json_item["corrupted"] = true;
                            if (items[i].loc_bad & LOC_INCONSISTENT)
                                json_item["inconsistent"] = true;
                            if (items[i].loc_bad & LOC_OUTDATED)
                                json_item["outdated"] = true;
                            describe_items.push_back(json_item);
                        }
                        count++;
                    }
                }
                delete op;
                parent->waiting--;
                if (!parent->waiting)
                    loop();
            };
            parent->waiting++;
            parent->cli->execute_raw(only_osds[i], op);
        }
    resume_1:
        state = 1;
        if (parent->waiting > 0)
        {
            return;
        }
        if (parent->json_output && parent->is_command_line)
        {
            printf(count > 0 ? "\n]\n" : "]\n");
        }
        else
        {
            result.data = describe_items;
        }
        state = 100;
        describe_items.clear();
    }
};

std::function<bool(cli_result_t &)> cli_tool_t::start_describe(json11::Json cfg)
{
    auto describer = new cli_describe_t();
    describer->parent = this;
    describer->options = cfg;
    return [describer](cli_result_t & result)
    {
        describer->loop();
        if (describer->is_done())
        {
            result = describer->result;
            delete describer;
            return true;
        }
        return false;
    };
}
