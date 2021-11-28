// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "cli.h"
#include "cluster_client.h"
#include "base64.h"

// Rename, resize image (and purge extra data on shrink) or change its readonly status
struct image_changer_t
{
    cli_tool_t *parent;

    std::string image_name;
    std::string new_name;
    uint64_t new_size = 0;
    bool set_readonly = false, set_readwrite = false, force = false;
    // interval between fsyncs
    int fsync_interval = 128;

    uint64_t inode_num = 0;
    inode_config_t cfg;
    json11::Json::array checks, success;
    bool has_children = false;

    int state = 0;
    std::function<bool(void)> cb;

    bool is_done()
    {
        return state == 100;
    }

    void loop()
    {
        if (state == 1)
            goto resume_1;
        else if (state == 2)
            goto resume_2;
        for (auto & ic: parent->cli->st_cli.inode_config)
        {
            if (ic.second.name == image_name)
            {
                inode_num = ic.first;
                cfg = ic.second;
                break;
            }
            if (new_name != "" && ic.second.name == new_name)
            {
                fprintf(stderr, "Image %s already exists\n", new_name.c_str());
                exit(1);
            }
        }
        if (!inode_num)
        {
            fprintf(stderr, "Image %s does not exist\n", image_name.c_str());
            exit(1);
        }
        for (auto & ic: parent->cli->st_cli.inode_config)
        {
            if (ic.second.parent_id == inode_num)
            {
                has_children = true;
                break;
            }
        }
        if (new_size != 0)
        {
            if (cfg.size >= new_size)
            {
                // Check confirmation when trimming an image with children
                if (has_children && !force)
                {
                    fprintf(stderr, "Image %s has children. Refusing to shrink it without --force", image_name.c_str());
                    exit(1);
                }
                // Shrink the image first
                cb = parent->start_rm(json11::Json::object {
                    { "inode", INODE_NO_POOL(inode_num) },
                    { "pool", (uint64_t)INODE_POOL(inode_num) },
                    { "fsync-interval", fsync_interval },
                    { "min-offset", new_size },
                });
resume_1:
                while (!cb())
                {
                    state = 1;
                    return;
                }
                cb = NULL;
            }
            cfg.size = new_size;
        }
        if (set_readonly)
        {
            cfg.readonly = true;
        }
        if (set_readwrite)
        {
            cfg.readonly = false;
            // Check confirmation when making an image with children read-write
            if (!force)
            {
                fprintf(stderr, "Image %s has children. Refusing to make it read-write without --force", image_name.c_str());
                exit(1);
            }
        }
        if (new_name != "")
        {
            cfg.name = new_name;
        }
        {
            std::string cur_cfg_key = base64_encode(parent->cli->st_cli.etcd_prefix+
                "/config/inode/"+std::to_string(INODE_POOL(inode_num))+
                "/"+std::to_string(INODE_NO_POOL(inode_num)));
            checks.push_back(json11::Json::object {
                { "target", "MOD" },
                { "key", cur_cfg_key },
                { "result", "LESS" },
                { "mod_revision", cfg.mod_revision+1 },
            });
            success.push_back(json11::Json::object {
                { "request_put", json11::Json::object {
                    { "key", cur_cfg_key },
                    { "value", base64_encode(json11::Json(
                        parent->cli->st_cli.serialize_inode_cfg(&cfg)
                    ).dump()) },
                } }
            });
        }
        if (new_name != "")
        {
            std::string old_idx_key = base64_encode(
                parent->cli->st_cli.etcd_prefix+"/index/image/"+image_name
            );
            std::string new_idx_key = base64_encode(
                parent->cli->st_cli.etcd_prefix+"/index/image/"+new_name
            );
            checks.push_back(json11::Json::object {
                { "target", "MOD" },
                { "key", old_idx_key },
                { "result", "LESS" },
                { "mod_revision", cfg.mod_revision+1 },
            });
            checks.push_back(json11::Json::object {
                { "target", "VERSION" },
                { "version", 0 },
                { "key", new_idx_key },
            });
            success.push_back(json11::Json::object {
                { "request_delete_range", json11::Json::object {
                    { "key", old_idx_key },
                } }
            });
            success.push_back(json11::Json::object {
                { "request_put", json11::Json::object {
                    { "key", new_idx_key },
                    { "value", base64_encode(json11::Json(json11::Json::object{
                        { "id", INODE_NO_POOL(inode_num) },
                        { "pool_id", (uint64_t)INODE_POOL(inode_num) },
                    }).dump()) },
                } }
            });
        }
        parent->waiting++;
        parent->cli->st_cli.etcd_txn(json11::Json::object {
            { "compare", checks },
            { "success", success },
        }, ETCD_SLOW_TIMEOUT, [this](std::string err, json11::Json res)
        {
            if (err != "")
            {
                fprintf(stderr, "Error changing %s: %s\n", image_name.c_str(), err.c_str());
                exit(1);
            }
            if (!res["succeeded"].bool_value())
            {
                fprintf(stderr, "Image %s was modified by someone else, please repeat your request\n", image_name.c_str());
                exit(1);
            }
            parent->waiting--;
            parent->ringloop->wakeup();
        });
        state = 2;
resume_2:
        if (parent->waiting > 0)
            return;
        printf("Image %s changed\n", image_name.c_str());
        state = 100;
    }
};

std::function<bool(void)> cli_tool_t::start_modify(json11::Json cfg)
{
    json11::Json::array cmd = cfg["command"].array_items();
    auto changer = new image_changer_t();
    changer->parent = this;
    changer->image_name = cmd.size() > 1 ? cmd[1].string_value() : "";
    if (changer->image_name == "")
    {
        fprintf(stderr, "Image name is missing\n");
        exit(1);
    }
    changer->new_name = cfg["rename"].string_value();
    if (changer->new_name == changer->image_name)
    {
        changer->new_name = "";
    }
    changer->new_size = cfg["size"].uint64_value();
    if (changer->new_size != 0 && (changer->new_size % 4096))
    {
        fprintf(stderr, "Image size should be a multiple of 4096\n");
        exit(1);
    }
    changer->force = cfg["force"].bool_value();
    changer->set_readonly = cfg["readonly"].bool_value();
    changer->set_readwrite = cfg["readwrite"].bool_value();
    changer->fsync_interval = cfg["fsync-interval"].uint64_value();
    if (!changer->fsync_interval)
        changer->fsync_interval = 128;
    // FIXME Check that the image doesn't have children when shrinking
    return [changer]()
    {
        changer->loop();
        if (changer->is_done())
        {
            delete changer;
            return true;
        }
        return false;
    };
}
