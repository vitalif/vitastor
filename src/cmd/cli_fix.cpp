// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "cli_fix.h"
#include "cluster_client.h"
#include "pg_states.h"
#include "str_util.h"
#include "json_util.h"

struct cli_fix_t
{
    std::vector<object_id> objects;
    int part = -1;
    int processed_count = 0;
    std::set<osd_num_t> bad_osds;
    bool no_check = false;

    cli_tool_t *parent = NULL;
    int state = 0;

    json11::Json options;
    cli_result_t result;
    json11::Json::array fix_result;

    bool is_done()
    {
        return state == 100;
    }

    void parse_objects_str(std::string str)
    {
        str = trim(str);
        if (str[0] == '[')
        {
            std::string json_err;
            json11::Json list = json11::Json::parse(str, json_err);
            if (json_err != "")
                fprintf(stderr, "Invalid JSON object list input: %s\n", json_err.c_str());
            else
                parse_object_list(list);
        }
        else
        {
            const char *s = str.c_str();
            char *e = NULL;
            int len = str.size();
            object_id oid;
            for (int p = 0; p < len; p++)
            {
                if (isdigit(s[p]))
                {
                    int p0 = p;
                    oid.inode = strtoull(s+p, &e, 0);
                    p = e-s;
                    while (p < len && !isdigit(s[p]) && s[p] != ':')
                        p++;
                    if (s[p] != ':')
                    {
                        fprintf(stderr, "Invalid object ID in input: %s\n", std::string(s+p0, p-p0).c_str());
                        continue;
                    }
                    p++;
                    while (p < len && !isdigit(s[p]))
                        p++;
                    oid.stripe = strtoull(s+p, &e, 0) & ~STRIPE_MASK;
                    p = e-s;
                    if (oid.inode)
                        objects.push_back(oid);
                    else
                        fprintf(stderr, "Invalid object ID in input: %s\n", std::string(s+p0, p-p0).c_str());
                }
            }
        }
    }

    void parse_object_list(json11::Json list)
    {
        for (auto & obj: list.array_items())
        {
            object_id oid = (object_id){
                .inode = stoull_full(obj["inode"].string_value(), 0),
                .stripe = stoull_full(obj["stripe"].string_value(), 0) & ~STRIPE_MASK,
            };
            if (oid.inode)
                objects.push_back(oid);
            else
                fprintf(stderr, "Invalid JSON object ID in input: %s, bad or missing \"inode\" field\n", obj.dump().c_str());
        }
    }

    void parse_options(json11::Json cfg)
    {
        json11::Json object_list;
        if (cfg["objects"].is_null())
            parse_objects_str(read_all_fd(0));
        else if (cfg["objects"].is_string())
            parse_objects_str(cfg["objects"].string_value());
        else
            parse_object_list(cfg["objects"].array_items());
        for (auto osd_num: parse_uint64_list(cfg["bad_osds"]))
            bad_osds.insert(osd_num);
        no_check = json_is_false(cfg["check"]);
        if (cfg["part"].is_number() || cfg["part"].is_string())
            part = cfg["part"].uint64_value();
    }

    void loop()
    {
        if (state == 1)
            goto resume_1;
        if (state == 100)
            return;
        parse_options(options);
        if (!objects.size())
        {
            result = (cli_result_t){ .err = EINVAL, .text = "Object list is not specified" };
            state = 100;
            return;
        }
        if (!bad_osds.size())
        {
            result = (cli_result_t){ .err = EINVAL, .text = "OSDs are not specified" };
            state = 100;
            return;
        }
        remove_duplicates(objects);
        parent->cli->init_msgr();
    resume_1:
        state = 1;
        while (processed_count < objects.size())
        {
            if (parent->waiting >= parent->iodepth*parent->parallel_osds)
            {
                return;
            }
            auto & obj = objects[processed_count++];
            auto pool_cfg_it = parent->cli->st_cli.pool_config.find(INODE_POOL(obj.inode));
            if (pool_cfg_it == parent->cli->st_cli.pool_config.end())
            {
                fprintf(stderr, "Object %jx:%jx is from unknown pool\n", obj.inode, obj.stripe);
                continue;
            }
            auto & pool_cfg = pool_cfg_it->second;
            pg_num_t pg_num = (obj.stripe/pool_cfg.pg_stripe_size) % pool_cfg.real_pg_count + 1; // like map_to_pg()
            auto pg_it = pool_cfg.pg_config.find(pg_num);
            if (pg_it == pool_cfg.pg_config.end() ||
                !pg_it->second.cur_primary || !(pg_it->second.cur_state & PG_ACTIVE))
            {
                fprintf(
                    stderr, "Object %jx:%jx is from PG %u/%u which is not currently active\n",
                    obj.inode, obj.stripe, pool_cfg_it->first, pg_num
                );
                continue;
            }
            osd_num_t primary_osd = pg_it->second.cur_primary;
            // Describe -> Remove some copies -> Scrub again
            osd_op_t *op = new osd_op_t;
            op->req = (osd_any_op_t){
                .describe = {
                    .header = {
                        .magic = SECONDARY_OSD_OP_MAGIC,
                        .id = parent->cli->next_op_id(),
                        .opcode = OSD_OP_DESCRIBE,
                    },
                    .min_inode = obj.inode,
                    .min_offset = obj.stripe,
                    .max_inode = obj.inode,
                    .max_offset = obj.stripe,
                },
            };
            op->callback = [this, primary_osd, &obj](osd_op_t *op)
            {
                if (op->reply.hdr.retval < 0 || op->reply.describe.result_bytes != op->reply.hdr.retval * sizeof(osd_reply_describe_item_t))
                {
                    fprintf(stderr, "Failed to describe objects on OSD %ju (retval=%jd)\n", primary_osd, op->reply.hdr.retval);
                    parent->waiting--;
                    loop();
                }
                else
                {
                    osd_reply_describe_item_t *items = (osd_reply_describe_item_t *)op->buf;
                    int *rm_count = (int*)malloc_or_die(sizeof(int));
                    *rm_count = 1; // just in case if anything gets called instantly
                    for (int i = 0; i < op->reply.hdr.retval; i++)
                    {
                        if (((items[i].loc_bad & LOC_INCONSISTENT) || no_check) &&
                            bad_osds.find(items[i].osd_num) != bad_osds.end() &&
                            (part == -1 || items[i].role == part))
                        {
                            // Remove
                            uint64_t rm_osd_num = items[i].osd_num;
                            osd_op_t *rm_op = new osd_op_t;
                            rm_op->req = (osd_any_op_t){
                                .sec_del = {
                                    .header = {
                                        .magic = SECONDARY_OSD_OP_MAGIC,
                                        .id = parent->cli->next_op_id(),
                                        .opcode = OSD_OP_SEC_DELETE,
                                    },
                                    .oid = {
                                        .inode = op->req.describe.min_inode,
                                        .stripe = op->req.describe.min_offset | items[i].role,
                                    },
                                    .version = 0,
                                },
                            };
                            rm_op->callback = [this, primary_osd, rm_osd_num, rm_count, &obj](osd_op_t *rm_op)
                            {
                                (*rm_count)--;
                                if (rm_op->reply.hdr.retval < 0)
                                {
                                    fprintf(
                                        stderr, "Failed to remove object %jx:%jx from OSD %ju (retval=%jd)\n",
                                        rm_op->req.sec_del.oid.inode, rm_op->req.sec_del.oid.stripe,
                                        rm_osd_num, rm_op->reply.hdr.retval
                                    );
                                }
                                else if (parent->json_output)
                                {
                                    fix_result.push_back(json11::Json::object {
                                        { "inode", (uint64_t)rm_op->req.sec_del.oid.inode },
                                        { "stripe", (uint64_t)rm_op->req.sec_del.oid.stripe & ~STRIPE_MASK },
                                        { "part", (uint64_t)rm_op->req.sec_del.oid.stripe & STRIPE_MASK },
                                        { "osd_num", (uint64_t)rm_osd_num },
                                    });
                                }
                                else
                                {
                                    printf(
                                        "Removed %jx:%jx (part %ju) from OSD %ju\n",
                                        rm_op->req.sec_del.oid.inode, rm_op->req.sec_del.oid.stripe & ~STRIPE_MASK,
                                        rm_op->req.sec_del.oid.stripe & STRIPE_MASK, rm_osd_num
                                    );
                                }
                                delete rm_op;
                                if (!(*rm_count))
                                {
                                    // Scrub
                                    free(rm_count);
                                    osd_op_t *scrub_op = new osd_op_t;
                                    scrub_op->req = (osd_any_op_t){
                                        .rw = {
                                            .header = {
                                                .magic = SECONDARY_OSD_OP_MAGIC,
                                                .id = parent->cli->next_op_id(),
                                                .opcode = OSD_OP_SCRUB,
                                            },
                                            .inode = obj.inode,
                                            .offset = obj.stripe,
                                            .len = 0,
                                        },
                                    };
                                    scrub_op->callback = [this, primary_osd, &obj](osd_op_t *scrub_op)
                                    {
                                        if (scrub_op->reply.hdr.retval < 0 && scrub_op->reply.hdr.retval != -ENOENT)
                                        {
                                            fprintf(
                                                stderr, "Failed to scrub %jx:%jx on OSD %ju (retval=%jd)\n",
                                                obj.inode, obj.stripe, primary_osd, scrub_op->reply.hdr.retval
                                            );
                                        }
                                        delete scrub_op;
                                        parent->waiting--;
                                        loop();
                                    };
                                    parent->cli->execute_raw(primary_osd, scrub_op);
                                }
                            };
                            (*rm_count)++;
                            parent->cli->execute_raw(rm_osd_num, rm_op);
                        }
                    }
                    (*rm_count)--;
                    if (!*rm_count)
                    {
                        free(rm_count);
                        parent->waiting--;
                        loop();
                    }
                }
                delete op;
            };
            parent->waiting++;
            parent->cli->execute_raw(primary_osd, op);
        }
        if (parent->waiting > 0)
        {
            return;
        }
        if (parent->json_output)
        {
            result.data = fix_result;
        }
        state = 100;
    }
};

std::function<bool(cli_result_t &)> cli_tool_t::start_fix(json11::Json cfg)
{
    auto fixer = new cli_fix_t();
    fixer->parent = this;
    fixer->options = cfg;
    return [fixer](cli_result_t & result)
    {
        fixer->loop();
        if (fixer->is_done())
        {
            result = fixer->result;
            delete fixer;
            return true;
        }
        return false;
    };
}
