// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <ctype.h>
#include "cli.h"
#include "cluster_client.h"
#include "base64.h"

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
    uint64_t size;

    pool_id_t old_pool_id = 0;
    inode_t new_parent_id = 0;
    inode_t new_id = 0, old_id = 0;
    uint64_t max_id_mod_rev = 0, cfg_mod_rev = 0, idx_mod_rev = 0;
    json11::Json result;

    int state = 0;

    bool is_done()
    {
        return state == 100;
    }

    void loop()
    {
        if (state >= 1)
            goto resume_1;
        if (new_pool_id)
        {
            auto & pools = parent->cli->st_cli.pool_config;
            if (pools.find(new_pool_id) == pools.end())
            {
                fprintf(stderr, "Pool %u does not exist\n", new_pool_id);
                exit(1);
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
                fprintf(stderr, "Pool %s does not exist\n", new_pool_name.c_str());
                exit(1);
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
        if (!new_pool_id)
        {
            fprintf(stderr, "Pool name or ID is missing\n");
            exit(1);
        }
        if (!size)
        {
            fprintf(stderr, "Image size is missing\n");
            exit(1);
        }
        for (auto & ic: parent->cli->st_cli.inode_config)
        {
            if (ic.second.name == image_name)
            {
                fprintf(stderr, "Image %s already exists\n", image_name.c_str());
                exit(1);
            }
        }
        do
        {
            etcd_txn(json11::Json::object {
                { "success", json11::Json::array { get_next_id() } }
            });
            state = 2;
resume_2:
            if (parent->waiting > 0)
                return;
            extract_next_id(result["responses"][0]);
            attempt_create();
            state = 3;
resume_3:
            if (parent->waiting > 0)
                return;
            if (!result["succeeded"].bool_value() &&
                result["responses"][0]["response_range"]["kvs"].array_items().size() > 0)
            {
                fprintf(stderr, "Image %s already exists\n", image_name.c_str());
                exit(1);
            }
        } while (!result["succeeded"].bool_value());
        if (parent->progress)
        {
            printf("Image %s created\n", image_name.c_str());
        }
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
                fprintf(stderr, "Snapshot %s@%s already exists\n", image_name.c_str(), new_snap.c_str());
                exit(1);
            }
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
                fprintf(stderr, "Image %s does not exist\n", image_name.c_str());
                exit(1);
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
            if (!result["succeeded"].bool_value() &&
                result["responses"][0]["response_range"]["kvs"].array_items().size() > 0)
            {
                fprintf(stderr, "Snapshot %s@%s already exists\n", image_name.c_str(), new_snap.c_str());
                exit(1);
            }
        } while (!result["succeeded"].bool_value());
        if (parent->progress)
        {
            printf("Snapshot %s@%s created\n", image_name.c_str(), new_snap.c_str());
        }
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
        auto ino_it = parent->cli->st_cli.inode_config.lower_bound(INODE_WITH_POOL(new_pool_id, 0));
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
        etcd_txn(json11::Json::object { { "success", json11::Json::array {
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
        extract_next_id(result["responses"][0]);
        old_id = 0;
        old_pool_id = 0;
        cfg_mod_rev = idx_mod_rev = 0;
        if (result["responses"][1]["response_range"]["kvs"].array_items().size() == 0)
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
                auto kv = parent->cli->st_cli.parse_etcd_kv(result["responses"][1]["response_range"]["kvs"][0]);
                old_id = INODE_NO_POOL(kv.value["id"].uint64_value());
                old_pool_id = (pool_id_t)kv.value["pool_id"].uint64_value();
                idx_mod_rev = kv.mod_revision;
                if (!old_id || !old_pool_id || old_pool_id >= POOL_ID_MAX)
                {
                    fprintf(stderr, "Invalid pool or inode ID in etcd key %s\n", kv.key.c_str());
                    exit(1);
                }
            }
            etcd_txn(json11::Json::object {
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
            {
                auto kv = parent->cli->st_cli.parse_etcd_kv(result["responses"][0]["response_range"]["kvs"][0]);
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
        inode_config_t new_cfg = {
            .num = INODE_WITH_POOL(new_pool_id, new_id),
            .name = image_name,
            .size = size,
            .parent_id = (new_snap != "" ? INODE_WITH_POOL(old_pool_id, old_id) : new_parent_id),
            .readonly = false,
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
        etcd_txn(json11::Json::object {
            { "compare", checks },
            { "success", success },
            { "failure", failure },
        });
    }

    void etcd_txn(json11::Json txn)
    {
        parent->waiting++;
        parent->cli->st_cli.etcd_txn(txn, ETCD_SLOW_TIMEOUT, [this](std::string err, json11::Json res)
        {
            parent->waiting--;
            if (err != "")
            {
                fprintf(stderr, "Error reading from etcd: %s\n", err.c_str());
                exit(1);
            }
            this->result = res;
            parent->ringloop->wakeup();
        });
    }
};

uint64_t parse_size(std::string size_str)
{
    uint64_t mul = 1;
    char type_char = tolower(size_str[size_str.length()-1]);
    if (type_char == 'k' || type_char == 'm' || type_char == 'g' || type_char == 't')
    {
        if (type_char == 'k')
            mul = 1l<<10;
        else if (type_char == 'm')
            mul = 1l<<20;
        else if (type_char == 'g')
            mul = 1l<<30;
        else /*if (type_char == 't')*/
            mul = 1l<<40;
        size_str = size_str.substr(0, size_str.length()-1);
    }
    uint64_t size = json11::Json(size_str).uint64_value() * mul;
    if (size == 0 && size_str != "0" && (size_str != "" || mul != 1))
    {
        fprintf(stderr, "Invalid syntax for size: %s\n", size_str.c_str());
        exit(1);
    }
    return size;
}

std::function<bool(void)> cli_tool_t::start_create(json11::Json cfg)
{
    json11::Json::array cmd = cfg["command"].array_items();
    auto image_creator = new image_creator_t();
    image_creator->parent = this;
    image_creator->image_name = cmd.size() > 1 ? cmd[1].string_value() : "";
    image_creator->new_pool_id = cfg["pool"].uint64_value();
    image_creator->new_pool_name = cfg["pool"].string_value();
    if (cfg["snapshot"].string_value() != "")
    {
        image_creator->new_snap = cfg["snapshot"].string_value();
    }
    else if (cmd[0] == "snap-create")
    {
        int p = image_creator->image_name.find('@');
        if (p == std::string::npos || p == image_creator->image_name.length()-1)
        {
            fprintf(stderr, "Please specify new snapshot name after @\n");
            exit(1);
        }
        image_creator->new_snap = image_creator->image_name.substr(p + 1);
        image_creator->image_name = image_creator->image_name.substr(0, p);
    }
    image_creator->new_parent = cfg["parent"].string_value();
    if (cfg["size"].string_value() != "")
    {
        image_creator->size = parse_size(cfg["size"].string_value());
        if (image_creator->size % 4096)
        {
            fprintf(stderr, "Size should be a multiple of 4096\n");
            exit(1);
        }
        if (image_creator->new_snap != "")
        {
            fprintf(stderr, "--size can't be specified for snapshots\n");
            exit(1);
        }
    }
    if (image_creator->image_name == "")
    {
        fprintf(stderr, "Image name is missing\n");
        exit(1);
    }
    if (image_creator->image_name.find('@') != std::string::npos)
    {
        fprintf(stderr, "Image name can't contain @ character\n");
        exit(1);
    }
    return [image_creator]()
    {
        image_creator->loop();
        if (image_creator->is_done())
        {
            delete image_creator;
            return true;
        }
        return false;
    };
}
