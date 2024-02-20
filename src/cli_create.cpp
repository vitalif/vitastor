// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <ctype.h>
#include "cli.h"
#include "cluster_client.h"
#include "str_util.h"

// Create an image, snapshot or clone
//
// Snapshot creation performs a etcd transaction which:
// - Checks that the image exists
// - Checks that the snapshot doesn't exist
// - Renames the inode to a new name with snapshot (say, testimg -> testimg@0)
// - Sets the readonly flag for the old inode
// - Creates a new inode with the same name pointing to the old inode as parent
// - Adjusts /index/image/*
//
// The same algorithm can be easily implemented in any other language or even via etcdctl,
// however we have it here for completeness
struct image_creator_t
{
    cli_tool_t *parent;

    pool_id_t new_pool_id = 0;
    std::string new_pool_name;
    std::string image_name, new_snap, new_parent;
    json11::Json new_meta;
    uint64_t size;
    bool force_size = false;

    pool_id_t old_pool_id = 0;
    inode_t new_parent_id = 0;
    inode_t new_id = 0, old_id = 0;
    uint64_t max_id_mod_rev = 0, cfg_mod_rev = 0, idx_mod_rev = 0;
    inode_config_t new_cfg;

    int state = 0;
    cli_result_t result;

    bool is_done()
    {
        return state == 100;
    }

    void loop()
    {
        if (state >= 1)
            goto resume_1;
        if (image_name == "")
        {
            // FIXME: EINVAL -> specific codes for every error
            result = (cli_result_t){ .err = EINVAL, .text = "Image name is missing" };
            state = 100;
            return;
        }
        if (image_name.find('@') != std::string::npos)
        {
            result = (cli_result_t){ .err = EINVAL, .text = "Image name can't contain @ character" };
            state = 100;
            return;
        }
        if (new_pool_id)
        {
            auto & pools = parent->cli->st_cli.pool_config;
            if (pools.find(new_pool_id) == pools.end())
            {
                result = (cli_result_t){ .err = ENOENT, .text = "Pool "+std::to_string(new_pool_id)+" does not exist" };
                state = 100;
                return;
            }
        }
        else if (new_pool_name != "")
        {
            for (auto & ic: parent->cli->st_cli.pool_config)
            {
                if (ic.second.name == new_pool_name)
                {
                    new_pool_id = ic.first;
                    break;
                }
            }
            if (!new_pool_id)
            {
                result = (cli_result_t){ .err = ENOENT, .text = "Pool "+new_pool_name+" does not exist" };
                state = 100;
                return;
            }
        }
        else if (parent->cli->st_cli.pool_config.size() == 1)
        {
            auto it = parent->cli->st_cli.pool_config.begin();
            new_pool_id = it->first;
        }
        state = 1;
    resume_1:
        if (new_snap == "")
            create_image();
        else
            create_snapshot();
    }

    void create_image()
    {
        if (state == 2)
            goto resume_2;
        else if (state == 3)
            goto resume_3;
        for (auto & ic: parent->cli->st_cli.inode_config)
        {
            if (ic.second.name == image_name)
            {
                result = (cli_result_t){ .err = EEXIST, .text = "Image "+image_name+" already exists" };
                state = 100;
                return;
            }
            if (ic.second.name == new_parent)
            {
                new_parent_id = ic.second.num;
                if (!new_pool_id)
                {
                    new_pool_id = INODE_POOL(ic.second.num);
                }
                if (!size)
                {
                    size = ic.second.size;
                }
            }
        }
        if (new_parent != "" && !new_parent_id)
        {
            result = (cli_result_t){ .err = ENOENT, .text = "Parent image "+new_parent+" not found" };
            state = 100;
            return;
        }
        if (!new_pool_id)
        {
            result = (cli_result_t){ .err = EINVAL, .text = "Pool name or ID is missing" };
            state = 100;
            return;
        }
        if (!size && !force_size)
        {
            result = (cli_result_t){ .err = EINVAL, .text = "Image size is missing" };
            state = 100;
            return;
        }
        do
        {
            parent->etcd_txn(json11::Json::object {
                { "success", json11::Json::array { get_next_id() } }
            });
            state = 2;
resume_2:
            if (parent->waiting > 0)
                return;
            if (parent->etcd_err.err)
            {
                result = parent->etcd_err;
                state = 100;
                return;
            }
            extract_next_id(parent->etcd_result["responses"][0]);
            attempt_create();
            state = 3;
resume_3:
            if (parent->waiting > 0)
                return;
            if (parent->etcd_err.err)
            {
                result = parent->etcd_err;
                state = 100;
                return;
            }
            if (!parent->etcd_result["succeeded"].bool_value() &&
                parent->etcd_result["responses"][0]["response_range"]["kvs"].array_items().size() > 0)
            {
                result = (cli_result_t){ .err = EEXIST, .text = "Image "+image_name+" already exists" };
                state = 100;
                return;
            }
        } while (!parent->etcd_result["succeeded"].bool_value());
        // Save into inode_config for library users to be able to take it from there immediately
        new_cfg.mod_revision = parent->etcd_result["responses"][0]["response_put"]["header"]["revision"].uint64_value();
        parent->cli->st_cli.insert_inode_config(new_cfg);
        result = (cli_result_t){
            .err = 0,
            .text = "Image "+image_name+" created",
            .data = json11::Json::object {
                { "name", image_name },
                { "pool", new_pool_name },
                { "parent", new_parent },
                { "size", size },
            }
        };
        state = 100;
    }

    void create_snapshot()
    {
        if (state == 2)
            goto resume_2;
        else if (state == 3)
            goto resume_3;
        else if (state == 4)
            goto resume_4;
        for (auto & ic: parent->cli->st_cli.inode_config)
        {
            if (ic.second.name == image_name+"@"+new_snap)
            {
                result = (cli_result_t){ .err = EEXIST, .text = "Snapshot "+image_name+"@"+new_snap+" already exists" };
                state = 100;
                return;
            }
        }
        if (new_parent != "")
        {
            result = (cli_result_t){ .err = EINVAL, .text = "Parent can't be specified for snapshots" };
            state = 100;
            return;
        }
        do
        {
            // In addition to next_id, get: size, old_id, old_pool_id, new_parent, cfg_mod_rev, idx_mod_rev
resume_2:
resume_3:
            get_image_details();
            if (parent->waiting > 0)
                return;
            if (!old_id)
            {
                result = (cli_result_t){ .err = ENOENT, .text = "Image "+image_name+" does not exist" };
                state = 100;
                return;
            }
            if (!new_pool_id)
            {
                // Create snapshot in the same pool by default
                new_pool_id = old_pool_id;
            }
            attempt_create();
            state = 4;
resume_4:
            if (parent->waiting > 0)
                return;
            if (parent->etcd_err.err)
            {
                result = parent->etcd_err;
                state = 100;
                return;
            }
            if (!parent->etcd_result["succeeded"].bool_value() &&
                parent->etcd_result["responses"][0]["response_range"]["kvs"].array_items().size() > 0)
            {
                result = (cli_result_t){ .err = EEXIST, .text = "Snapshot "+image_name+"@"+new_snap+" already exists" };
                state = 100;
                return;
            }
        } while (!parent->etcd_result["succeeded"].bool_value());
        // Save into inode_config for library users to be able to take it from there immediately
        new_cfg.mod_revision = parent->etcd_result["responses"][0]["response_put"]["header"]["revision"].uint64_value();
        parent->cli->st_cli.insert_inode_config(new_cfg);
        result = (cli_result_t){
            .err = 0,
            .text = "Snapshot "+image_name+"@"+new_snap+" created",
            .data = json11::Json::object {
                { "name", image_name+"@"+new_snap },
                { "pool", (uint64_t)new_pool_id },
                { "parent", new_parent },
                { "size", size },
            }
        };
        state = 100;
    }

    json11::Json::object get_next_id()
    {
        return json11::Json::object {
            { "request_range", json11::Json::object {
                { "key", base64_encode(
                    parent->cli->st_cli.etcd_prefix+"/index/maxid/"+std::to_string(new_pool_id)
                ) },
            } },
        };
    }

    void extract_next_id(json11::Json response)
    {
        new_id = 1;
        max_id_mod_rev = 0;
        if (response["response_range"]["kvs"].array_items().size() > 0)
        {
            auto kv = parent->cli->st_cli.parse_etcd_kv(response["response_range"]["kvs"][0]);
            new_id = 1+INODE_NO_POOL(kv.value.uint64_value());
            max_id_mod_rev = kv.mod_revision;
        }
        // Also check existing inodes - for the case when some inodes are created without changing /index/maxid
        auto ino_it = parent->cli->st_cli.inode_config.lower_bound(INODE_WITH_POOL(new_pool_id+1, 0));
        if (ino_it != parent->cli->st_cli.inode_config.begin())
        {
            ino_it--;
            if (INODE_POOL(ino_it->first) == new_pool_id && new_id < 1+INODE_NO_POOL(ino_it->first))
                new_id = 1+INODE_NO_POOL(ino_it->first);
        }
    }

    void get_image_details()
    {
        if (state == 2)
            goto resume_2;
        else if (state == 3)
            goto resume_3;
        parent->etcd_txn(json11::Json::object { { "success", json11::Json::array {
            get_next_id(),
            json11::Json::object {
                { "request_range", json11::Json::object {
                    { "key", base64_encode(
                        parent->cli->st_cli.etcd_prefix+"/index/image/"+image_name
                    ) },
                } },
            },
        } } });
        state = 2;
resume_2:
        if (parent->waiting > 0)
            return;
        if (parent->etcd_err.err)
        {
            result = parent->etcd_err;
            state = 100;
            return;
        }
        extract_next_id(parent->etcd_result["responses"][0]);
        old_id = 0;
        old_pool_id = 0;
        cfg_mod_rev = idx_mod_rev = 0;
        if (parent->etcd_result["responses"][1]["response_range"]["kvs"].array_items().size() == 0)
        {
            for (auto & ic: parent->cli->st_cli.inode_config)
            {
                if (ic.second.name == image_name)
                {
                    old_id = INODE_NO_POOL(ic.first);
                    old_pool_id = INODE_POOL(ic.first);
                    size = ic.second.size;
                    new_parent_id = ic.second.parent_id;
                    cfg_mod_rev = ic.second.mod_revision;
                    break;
                }
            }
        }
        else
        {
            // FIXME: Parse kvs in etcd_state_client automatically
            {
                auto kv = parent->cli->st_cli.parse_etcd_kv(parent->etcd_result["responses"][1]["response_range"]["kvs"][0]);
                old_id = INODE_NO_POOL(kv.value["id"].uint64_value());
                old_pool_id = (pool_id_t)kv.value["pool_id"].uint64_value();
                idx_mod_rev = kv.mod_revision;
                if (!old_id || !old_pool_id || old_pool_id >= POOL_ID_MAX)
                {
                    result = (cli_result_t){ .err = ENOENT, .text = "Invalid pool or inode ID in etcd key "+kv.key };
                    state = 100;
                    return;
                }
            }
            parent->etcd_txn(json11::Json::object {
                { "success", json11::Json::array {
                    json11::Json::object {
                        { "request_range", json11::Json::object {
                            { "key", base64_encode(
                                parent->cli->st_cli.etcd_prefix+"/config/inode/"+
                                std::to_string(old_pool_id)+"/"+std::to_string(old_id)
                            ) },
                        } },
                    },
                } },
            });
            state = 3;
resume_3:
            if (parent->waiting > 0)
                return;
            if (parent->etcd_err.err)
            {
                result = parent->etcd_err;
                state = 100;
                return;
            }
            {
                auto kv = parent->cli->st_cli.parse_etcd_kv(parent->etcd_result["responses"][0]["response_range"]["kvs"][0]);
                size = kv.value["size"].uint64_value();
                new_parent_id = kv.value["parent_id"].uint64_value();
                uint64_t parent_pool_id = kv.value["parent_pool_id"].uint64_value();
                if (new_parent_id)
                {
                    new_parent_id = INODE_WITH_POOL(parent_pool_id ? parent_pool_id : old_pool_id, new_parent_id);
                }
                cfg_mod_rev = kv.mod_revision;
            }
        }
    }

    void attempt_create()
    {
        new_cfg = {
            .num = INODE_WITH_POOL(new_pool_id, new_id),
            .name = image_name,
            .size = size,
            .parent_id = (new_snap != "" ? INODE_WITH_POOL(old_pool_id, old_id) : new_parent_id),
            .readonly = false,
            .meta = new_meta,
        };
        json11::Json::array checks = json11::Json::array {
            json11::Json::object {
                { "target", "VERSION" },
                { "version", 0 },
                { "key", base64_encode(
                    parent->cli->st_cli.etcd_prefix+"/config/inode/"+
                    std::to_string(new_pool_id)+"/"+std::to_string(new_id)
                ) },
            },
            json11::Json::object {
                { "target", "VERSION" },
                { "version", 0 },
                { "key", base64_encode(
                    parent->cli->st_cli.etcd_prefix+"/index/image/"+image_name+
                    (new_snap != "" ? "@"+new_snap : "")
                ) },
            },
            json11::Json::object {
                { "target", "MOD" },
                { "mod_revision", max_id_mod_rev },
                { "key", base64_encode(parent->cli->st_cli.etcd_prefix+"/index/maxid/"+std::to_string(new_pool_id)) },
            },
        };
        json11::Json::array success = json11::Json::array {
            json11::Json::object {
                { "request_put", json11::Json::object {
                    { "key", base64_encode(
                        parent->cli->st_cli.etcd_prefix+"/config/inode/"+
                        std::to_string(new_pool_id)+"/"+std::to_string(new_id)
                    ) },
                    { "value", base64_encode(
                        json11::Json(parent->cli->st_cli.serialize_inode_cfg(&new_cfg)).dump()
                    ) },
                } },
            },
            json11::Json::object {
                { "request_put", json11::Json::object {
                    { "key", base64_encode(parent->cli->st_cli.etcd_prefix+"/index/image/"+image_name) },
                    { "value", base64_encode(json11::Json(json11::Json::object{
                        { "id", new_id },
                        { "pool_id", (uint64_t)new_pool_id },
                    }).dump()) },
                } },
            },
            json11::Json::object {
                { "request_put", json11::Json::object {
                    { "key", base64_encode(
                        parent->cli->st_cli.etcd_prefix+"/index/maxid/"+
                        std::to_string(new_pool_id)
                    ) },
                    { "value", base64_encode(std::to_string(new_id)) }
                } },
            },
        };
        json11::Json::array failure = json11::Json::array {
            json11::Json::object {
                { "request_range", json11::Json::object {
                    { "key", base64_encode(
                        parent->cli->st_cli.etcd_prefix+"/index/image/"+
                        image_name+(new_snap != "" ? "@"+new_snap : "")
                    ) },
                } },
            },
        };
        if (new_snap != "")
        {
            inode_config_t snap_cfg = {
                .num = INODE_WITH_POOL(old_pool_id, old_id),
                .name = image_name+"@"+new_snap,
                .size = size,
                .parent_id = new_parent_id,
                .readonly = true,
            };
            checks.push_back(json11::Json::object {
                { "target", "MOD" },
                { "mod_revision", cfg_mod_rev },
                { "key", base64_encode(
                    parent->cli->st_cli.etcd_prefix+"/config/inode/"+
                    std::to_string(old_pool_id)+"/"+std::to_string(old_id)
                ) },
            });
            checks.push_back(json11::Json::object {
                { "target", "MOD" },
                { "mod_revision", idx_mod_rev },
                { "key", base64_encode(parent->cli->st_cli.etcd_prefix+"/index/image/"+image_name) }
            });
            success.push_back(json11::Json::object {
                { "request_put", json11::Json::object {
                    { "key", base64_encode(
                        parent->cli->st_cli.etcd_prefix+"/config/inode/"+
                        std::to_string(old_pool_id)+"/"+std::to_string(old_id)
                    ) },
                    { "value", base64_encode(
                        json11::Json(parent->cli->st_cli.serialize_inode_cfg(&snap_cfg)).dump()
                    ) },
                } },
            });
            success.push_back(json11::Json::object {
                { "request_put", json11::Json::object {
                    { "key", base64_encode(parent->cli->st_cli.etcd_prefix+"/index/image/"+image_name+"@"+new_snap) },
                    { "value", base64_encode(json11::Json(json11::Json::object{
                        { "id", old_id },
                        { "pool_id", (uint64_t)old_pool_id },
                    }).dump()) },
                } },
            });
        };
        parent->etcd_txn(json11::Json::object {
            { "compare", checks },
            { "success", success },
            { "failure", failure },
        });
    }
};

std::function<bool(cli_result_t &)> cli_tool_t::start_create(json11::Json cfg)
{
    auto image_creator = new image_creator_t();
    image_creator->parent = this;
    image_creator->image_name = cfg["image"].string_value();
    image_creator->new_pool_id = cfg["pool"].uint64_value();
    image_creator->new_pool_name = cfg["pool"].string_value();
    image_creator->force_size = cfg["force_size"].bool_value();
    if (cfg["image_meta"].is_object())
    {
        image_creator->new_meta = cfg["image_meta"];
    }
    if (cfg["snapshot"].string_value() != "")
    {
        image_creator->new_snap = cfg["snapshot"].string_value();
    }
    image_creator->new_parent = cfg["parent"].string_value();
    if (cfg["size"].string_value() != "")
    {
        bool ok;
        image_creator->size = parse_size(cfg["size"].string_value(), &ok);
        if (!ok)
        {
            return [size = cfg["size"].string_value()](cli_result_t & result)
            {
                result = (cli_result_t){ .err = EINVAL, .text = "Invalid syntax for size: "+size };
                return true;
            };
        }
        if ((image_creator->size % 4096) && !cfg["force_size"].bool_value())
        {
            delete image_creator;
            return [](cli_result_t & result)
            {
                result = (cli_result_t){ .err = EINVAL, .text = "Size should be a multiple of 4096" };
                return true;
            };
        }
        if (image_creator->new_snap != "")
        {
            delete image_creator;
            return [](cli_result_t & result)
            {
                result = (cli_result_t){ .err = EINVAL, .text = "Size can't be specified for snapshots" };
                return true;
            };
        }
    }
    return [image_creator](cli_result_t & result)
    {
        image_creator->loop();
        if (image_creator->is_done())
        {
            result = image_creator->result;
            delete image_creator;
            return true;
        }
        return false;
    };
}
