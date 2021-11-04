// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "cli.h"
#include "cluster_client.h"
#include "base64.h"

// Create an image, snapshot or clone
//
// Snapshot creation does a etcd transaction which:
// - Changes the name of old inode to the name of the snapshot (say, testimg -> testimg@0)
// - Sets the readonly flag for the old inode
// - Creates a new inode with the same name pointing to the old inode as parent
// - Adjusts /index/image/*
//
// The same algorithm can be easily implemented in any other language or even via etcdctl,
// however we have it here for completeness
struct image_creator_t
{
    cli_tool_t *parent;

    int state = 0;

    bool is_done()
    {
        return true;
    }

    void loop()
    {
        return;
    }
};

std::function<bool(void)> cli_tool_t::start_create(json11::Json cfg)
{
    json11::Json::array cmd = cfg["command"].array_items();
    auto image_creator = new image_creator_t();
    image_creator->parent = this;
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
