// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "cli.h"
#include "cluster_client.h"
#include "str_util.h"

// Rename, resize image (and purge extra data on shrink) or change its readonly status
struct image_changer_t
{
    cli_tool_t *parent;

    std::string image_name;
    std::string new_name;
    uint64_t new_size = 0;
    bool force_size = false, inc_size = false;
    bool set_readonly = false, set_readwrite = false, force = false;
    // interval between fsyncs
    int fsync_interval = 128;

    uint64_t inode_num = 0;
    inode_config_t cfg;
    json11::Json::array checks, success;
    bool has_children = false;

    int state = 0;
    std::function<bool(cli_result_t &)> cb;
    cli_result_t result;

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
        if (image_name == "")
        {
            result = (cli_result_t){ .err = EINVAL, .text = "Image name is missing" };
            state = 100;
            return;
        }
        if (new_size != 0 && (new_size % 4096) && !force_size)
        {
            result = (cli_result_t){ .err = EINVAL, .text = "Image size should be a multiple of 4096" };
            state = 100;
            return;
        }
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
                result = (cli_result_t){ .err = EEXIST, .text = "Image "+new_name+" already exists" };
                state = 100;
                return;
            }
        }
        if (!inode_num)
        {
            result = (cli_result_t){ .err = ENOENT, .text = "Image "+image_name+" does not exist" };
            state = 100;
            return;
        }
        for (auto & ic: parent->cli->st_cli.inode_config)
        {
            if (ic.second.parent_id == inode_num)
            {
                has_children = true;
                break;
            }
        }
        if ((!set_readwrite || !cfg.readonly) &&
            (!set_readonly || cfg.readonly) &&
            (!new_size && !force_size || cfg.size == new_size || cfg.size >= new_size && inc_size) &&
            (new_name == "" || new_name == image_name))
        {
            result = (cli_result_t){ .err = 0, .text = "No change", .data = json11::Json::object {
                { "error_code", 0 },
                { "error_text", "No change" },
            }};
            state = 100;
            return;
        }
        if ((new_size != 0 || force_size) && (cfg.size < new_size || !inc_size))
        {
            if (cfg.size >= new_size)
            {
                // Check confirmation when trimming an image with children
                if (has_children && !force)
                {
                    result = (cli_result_t){ .err = EINVAL, .text = "Image "+image_name+" has children. Refusing to shrink it without --force" };
                    state = 100;
                    return;
                }
                // Shrink the image first
                cb = parent->start_rm_data(json11::Json::object {
                    { "inode", INODE_NO_POOL(inode_num) },
                    { "pool", (uint64_t)INODE_POOL(inode_num) },
                    { "fsync-interval", fsync_interval },
                    { "min-offset", ((new_size+4095)/4096)*4096 },
                });
resume_1:
                while (!cb(result))
                {
                    state = 1;
                    return;
                }
                cb = NULL;
                if (result.err)
                {
                    state = 100;
                    return;
                }
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
            if (has_children && !force)
            {
                result = (cli_result_t){ .err = EINVAL, .text = "Image "+image_name+" has children. Refusing to make it read-write without --force" };
                state = 100;
                return;
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
        parent->etcd_txn(json11::Json::object {
            { "compare", checks },
            { "success", success },
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
        if (!parent->etcd_result["succeeded"].bool_value())
        {
            result = (cli_result_t){ .err = EAGAIN, .text = "Image "+image_name+" was modified by someone else, please repeat your request" };
            state = 100;
            return;
        }
        // Save into inode_config for library users to be able to take it from there immediately
        cfg.mod_revision = parent->etcd_result["responses"][0]["response_put"]["header"]["revision"].uint64_value();
        if (new_name != "")
        {
            parent->cli->st_cli.inode_by_name.erase(image_name);
        }
        parent->cli->st_cli.insert_inode_config(cfg);
        result = (cli_result_t){
            .err = 0,
            .text = "Image "+image_name+" modified",
            .data = json11::Json::object {
                { "name", image_name },
                { "inode", INODE_NO_POOL(inode_num) },
                { "pool", (uint64_t)INODE_POOL(inode_num) },
                { "size", new_size },
            }
        };
        state = 100;
    }
};

std::function<bool(cli_result_t &)> cli_tool_t::start_modify(json11::Json cfg)
{
    auto changer = new image_changer_t();
    changer->parent = this;
    changer->image_name = cfg["image"].string_value();
    changer->new_name = cfg["rename"].string_value();
    changer->new_size = parse_size(cfg["resize"].as_string());
    changer->force_size = cfg["force_size"].bool_value();
    changer->inc_size = cfg["inc_size"].bool_value();
    changer->force = cfg["force"].bool_value();
    changer->set_readonly = cfg["readonly"].bool_value();
    changer->set_readwrite = cfg["readwrite"].bool_value();
    changer->fsync_interval = cfg["fsync_interval"].uint64_value();
    if (!changer->fsync_interval)
        changer->fsync_interval = 128;
    // FIXME Check that the image doesn't have children when shrinking
    return [changer](cli_result_t & result)
    {
        changer->loop();
        if (changer->is_done())
        {
            result = changer->result;
            delete changer;
            return true;
        }
        return false;
    };
}
