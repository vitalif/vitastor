// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "disk_tool.h"
#include "rw_blocking.h"
#include "str_util.h"
#include "json_util.h"

int disk_tool_t::resize_data(std::string device)
{
    if (options.find("move_journal") == options.end() &&
        options.find("move_data") == options.end() &&
        options.find("journal_size") == options.end() &&
        options.find("data_size") == options.end())
    {
        fprintf(stderr, "None of --move-journal, --move-data, --journal-size, --data-size options are specified - nothing to do!\n");
        return 1;
    }
    if (stoull_full(device))
        device = "/dev/vitastor/osd"+device+"-data";
    json11::Json sb = read_osd_superblock(device, true, false);
    if (sb.is_null())
        return 1;
    auto sb_params = json_to_string_map(sb["params"].object_items());
    try
    {
        dsk.parse_config(sb_params);
        dsk.data_io = dsk.meta_io = dsk.journal_io = "cached";
        dsk.open_data();
        dsk.open_meta();
        dsk.open_journal();
        dsk.calc_lengths(true);
    }
    catch (std::exception & e)
    {
        dsk.close_all();
        fprintf(stderr, "%s\n", e.what());
        return 1;
    }
    // Save FD numbers because calc_lengths() relies on them
    int old_journal_fd = dsk.journal_fd, old_meta_fd = dsk.meta_fd, old_data_fd = dsk.data_fd;
    dsk.close_all();
    bool dry_run = options.find("dry_run") != options.end();
    auto old_journal_device = dsk.journal_device;
    auto old_meta_device = dsk.meta_device;
    new_journal_len = dsk.journal_len;
    if (options.find("journal_size") != options.end())
    {
        new_journal_len = parse_size(options["journal_size"]);
        if (options.find("move_journal") == options.end())
            options["move_journal"] = dsk.journal_device == dsk.data_device ? "" : dsk.journal_device;
    }
    uint64_t new_data_dev_size = 0;
    if (options.find("data_size") != options.end())
    {
        new_data_dev_size = parse_size(options["data_size"]);
        new_data_dev_size = options["data_size"] == "max" || new_data_dev_size > dsk.data_device_size
            ? dsk.data_device_size : new_data_dev_size;
        dsk.data_device_size = new_data_dev_size;
        dsk.cfg_data_size = 0;
        dsk.journal_fd = old_journal_fd;
        dsk.meta_fd = old_meta_fd;
        dsk.data_fd = old_data_fd;
        dsk.calc_lengths(true);
        dsk.journal_fd = -1;
        dsk.meta_fd = -1;
        dsk.data_fd = -1;
    }
    std::map<std::string, std::string> move_options;
    if (options.find("move_journal") != options.end())
    {
        if (resize_parse_move_journal(move_options, dry_run) != 0)
            return 1;
    }
    if (options.find("move_meta") != options.end())
    {
        if (resize_parse_move_meta(move_options, dry_run) != 0)
            return 1;
    }
    auto new_journal_device = move_options.find("new_journal_device") != move_options.end()
        ? move_options["new_journal_device"] : dsk.journal_device;
    auto new_meta_device = move_options.find("new_meta_device") != move_options.end()
        ? move_options["new_meta_device"] : dsk.meta_device;
    // Calculate new data & meta offsets
    new_data_offset = 4096 + (new_journal_device == dsk.data_device ? new_journal_len : 0) +
        (new_meta_device == dsk.data_device ? dsk.meta_len : 0);
    new_data_offset += ((dsk.data_offset-new_data_offset) % dsk.data_block_size);
    if (new_data_offset != dsk.data_offset)
        move_options["new_data_offset"] = std::to_string(new_data_offset);
    if (new_data_dev_size != 0)
        move_options["new_data_len"] = std::to_string(new_data_dev_size-new_data_offset);
    new_meta_offset = 4096 + (new_meta_device == new_journal_device ? new_journal_len : 0);
    if (new_meta_offset != dsk.meta_offset)
        move_options["new_meta_offset"] = std::to_string(new_meta_offset);
    // Run resize
    auto orig_options = std::move(options);
    options = sb_params;
    for (auto & kv: move_options)
        options[kv.first] = kv.second;
    if (!json)
    {
        std::string cmd;
        for (auto & kv: move_options)
            cmd += "  "+kv.first+" = "+kv.second+"\n";
        fprintf(stderr, "Running resize:\n%s", cmd.c_str());
    }
    if (!dry_run && raw_resize() != 0)
        return 1;
    // Write new superblocks
    json11::Json::object new_sb_params = sb["params"].object_items();
    if (move_options.find("new_journal_device") != move_options.end())
        new_sb_params["journal_device"] = move_options["new_journal_device"];
    if (move_options.find("new_meta_device") != move_options.end())
        new_sb_params["meta_device"] = move_options["new_meta_device"];
    new_sb_params["data_offset"] = new_data_offset;
    new_sb_params["meta_offset"] = new_meta_offset;
    if (move_options.find("new_data_len") != move_options.end())
        new_sb_params["data_size"] = stoull_full(move_options["new_data_len"]);
    std::set<std::string> clear_superblocks, write_superblocks;
    write_superblocks.insert(dsk.data_device);
    write_superblocks.insert(new_journal_device);
    write_superblocks.insert(new_meta_device);
    if (write_superblocks.find(old_journal_device) == write_superblocks.end())
        clear_superblocks.insert(old_journal_device);
    if (write_superblocks.find(old_meta_device) == write_superblocks.end())
        clear_superblocks.insert(old_meta_device);
    for (auto & dev: clear_superblocks)
    {
        if (!json)
            fprintf(stderr, "Clearing OSD superblock on %s\n", dev.c_str());
        if (!dry_run && clear_osd_superblock(dev) != 0)
            return 1;
    }
    for (auto & dev: write_superblocks)
    {
        if (!json)
            fprintf(stderr, "Writing new OSD superblock to %s\n", dev.c_str());
        if (!dry_run && !write_osd_superblock(dev, new_sb_params))
            return 1;
    }
    if (json)
    {
        printf("%s\n", json11::Json(json11::Json::object {
            { "new_sb_params", new_sb_params },
        }).dump().c_str());
    }
    return 0;
}

int disk_tool_t::resize_parse_move_journal(std::map<std::string, std::string> & move_options, bool dry_run)
{
    if (options["move_journal"] == "")
    {
        // move back to the data device
        // but first check if not already there :)
        if (dsk.journal_device == dsk.data_device && new_journal_len == dsk.journal_len)
        {
            // already there
            fprintf(stderr, "journal is already on data device and has the same size\n");
            return 0;
        }
        move_options["new_journal_device"] = dsk.data_device;
        move_options["new_journal_offset"] = "4096";
        move_options["new_journal_len"] = std::to_string(new_journal_len);
    }
    else
    {
        std::string real_dev = realpath_str(options["move_journal"], false);
        if (real_dev == "")
            return 1;
        std::string parent_dev = get_parent_device(real_dev);
        if (parent_dev == "")
            return 1;
        if (parent_dev == real_dev)
        {
            // whole disk - create partition
            std::string old_real_dev = realpath_str(dsk.journal_device);
            if (old_real_dev == "")
                return 1;
            if (options.find("force") == options.end() &&
                get_parent_device(old_real_dev) == parent_dev)
            {
                // already there
                fprintf(stderr, "journal is already on a partition of %s, add --force to create a new partition\n", options["move_journal"].c_str());
                return 0;
            }
            new_journal_len = ((new_journal_len+1024*1024-1)/1024/1024)*1024*1024;
            if (!dry_run)
            {
                auto devinfos = collect_devices({ real_dev });
                if (devinfos.size() == 0)
                    return 1;
                std::vector<std::string> sizes;
                sizes.push_back(std::to_string(new_journal_len/1024/1024)+"MiB");
                auto new_parts = add_partitions(devinfos[0], sizes);
                if (!new_parts.array_items().size())
                    return 1;
                options["move_journal"] = "/dev/disk/by-partuuid/"+strtolower(new_parts[0]["uuid"].string_value());
            }
            else
                options["move_journal"] = "<new journal partition on "+parent_dev+">";
        }
        else
        {
            // already a partition - check that it's a GPT partition with correct type
            if ((options.find("force") == options.end()
                ? check_existing_partition(options["move_journal"])
                : fix_partition_type(options["move_journal"])) != 0)
            {
                return 1;
            }
            new_journal_len = get_device_size(options["move_journal"], true);
            if (new_journal_len == UINT64_MAX)
                return 1;
        }
        new_journal_len -= 4096;
        move_options["new_journal_device"] = options["move_journal"];
        move_options["new_journal_offset"] = "4096";
        move_options["new_journal_len"] = std::to_string(new_journal_len);
    }
    return 0;
}

int disk_tool_t::resize_parse_move_meta(std::map<std::string, std::string> & move_options, bool dry_run)
{
    if (options["move_meta"] == "")
    {
        // move back to the data device
        // but first check if not already there :)
        if (dsk.meta_device == dsk.data_device)
        {
            // already there
            fprintf(stderr, "metadata is already on data device\n");
            return 0;
        }
        auto new_journal_device = move_options.find("new_journal_device") != move_options.end()
            ? move_options["new_journal_device"] : dsk.journal_device;
        move_options["new_meta_device"] = dsk.data_device;
        move_options["new_meta_len"] = std::to_string(dsk.meta_len);
    }
    else
    {
        std::string real_dev = realpath_str(options["move_meta"], false);
        if (real_dev == "")
            return 1;
        std::string parent_dev = get_parent_device(real_dev);
        if (parent_dev == "")
            return 1;
        uint64_t new_meta_len = 0;
        if (parent_dev == real_dev)
        {
            // whole disk - create partition
            std::string old_real_dev = realpath_str(dsk.meta_device);
            if (old_real_dev == "")
                return 1;
            if (options.find("force") == options.end() &&
                get_parent_device(old_real_dev) == parent_dev)
            {
                // already there
                fprintf(stderr, "metadata is already on a partition of %s\n", options["move_meta"].c_str());
                return 0;
            }
            new_meta_len = ((dsk.meta_len+1024*1024-1)/1024/1024)*1024*1024;
            if (!dry_run)
            {
                auto devinfos = collect_devices({ real_dev });
                if (devinfos.size() == 0)
                    return 1;
                std::vector<std::string> sizes;
                sizes.push_back(std::to_string(new_meta_len/1024/1024)+"MiB");
                auto new_parts = add_partitions(devinfos[0], sizes);
                if (!new_parts.array_items().size())
                    return 1;
                options["move_meta"] = "/dev/disk/by-partuuid/"+strtolower(new_parts[0]["uuid"].string_value());
            }
            else
                options["move_meta"] = "<new metadata partition on "+parent_dev+">";
        }
        else
        {
            // already a partition - check that it's a GPT partition with correct type
            if ((options.find("force") == options.end()
                ? check_existing_partition(options["move_meta"])
                : fix_partition_type(options["move_meta"])) != 0)
            {
                return 1;
            }
            new_meta_len = get_device_size(options["move_meta"], true);
            if (new_meta_len == UINT64_MAX)
                return 1;
        }
        new_meta_len -= 4096;
        move_options["new_meta_len"] = std::to_string(new_meta_len);
        move_options["new_meta_device"] = options["move_meta"];
        move_options["new_meta_offset"] = "4096";
    }
    return 0;
}
