// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "disk_tool.h"
#include "str_util.h"
#include "json_util.h"
#include "osd_id.h"

int disk_tool_t::prepare_one(std::map<std::string, std::string> options, int is_hdd)
{
    static const char *allow_additional_params[] = {
        "autosync_writes",
        "data_io",
        "meta_io",
        "journal_io",
        "max_write_iodepth",
        "max_write_iodepth",
        "min_flusher_count",
        "max_flusher_count",
        "inmemory_metadata",
        "inmemory_journal",
        "journal_sector_buffer_count",
        "journal_no_same_sector_overwrites",
        "throttle_small_writes",
        "throttle_target_iops",
        "throttle_target_mbs",
        "throttle_target_parallelism",
        "throttle_threshold_us",
    };
    if (options.find("force") == options.end())
    {
        std::vector<std::string> all_devs = { options["data_device"], options["meta_device"], options["journal_device"] };
        for (int i = 0; i < all_devs.size(); i++)
        {
            const auto & dev = all_devs[i];
            if (dev == "")
                continue;
            if (dev.substr(0, 22) != "/dev/disk/by-partuuid/")
            {
                // Partitions should be identified by GPT partition UUID
                fprintf(stderr, "%s does not start with /dev/disk/by-partuuid/. Partitions should be identified by GPT partition UUIDs\n", dev.c_str());
                return 1;
            }
            std::string real_dev = realpath_str(dev, false);
            if (real_dev == "")
                return 1;
            std::string parent_dev = get_parent_device(real_dev);
            if (parent_dev == "")
                return 1;
            if (parent_dev == real_dev)
            {
                fprintf(stderr, "%s is not a partition, not creating OSD without --force\n", dev.c_str());
                return 1;
            }
            if (i == 0 && is_hdd == -1)
                is_hdd = trim(read_file("/sys/block/"+parent_dev.substr(5)+"/queue/rotational")) == "1";
            if (check_existing_partition(dev) != 0)
                return 1;
        }
    }
    for (auto dev: std::vector<std::string>{"data", "meta", "journal"})
    {
        if (options[dev+"_device"] != "" && options["disable_"+dev+"_fsync"] == "auto")
        {
            int r = disable_cache(realpath_str(options[dev+"_device"], false));
            if (r != 0)
            {
                if (r == 1)
                    fprintf(stderr, "Warning: disable_%s_fsync is auto, but cache status check failed. Leaving fsync on\n", dev.c_str());
                options["disable_"+dev+"_fsync"] = "0";
            }
            else
                options["disable_"+dev+"_fsync"] = "1";
        }
    }
    if (options["meta_device"] == "" || options["meta_device"] == options["data_device"])
    {
        options["disable_meta_fsync"] = options["disable_data_fsync"];
    }
    if (options["journal_device"] == "" || options["journal_device"] == options["meta_device"])
    {
        options["disable_journal_fsync"] = options["disable_meta_fsync"];
    }
    else if (options["journal_device"] == options["data_device"])
    {
        options["disable_journal_fsync"] = options["disable_data_fsync"];
    }
    // Calculate offsets if the same device is used for two or more of data, meta, and journal
    if (options["journal_size"] == "" && (options["journal_device"] == "" || options["journal_device"] == options["data_device"]))
    {
        options["journal_size"] = is_hdd || !json_is_true(options["disable_data_fsync"]) ? "128M" : "32M";
    }
    bool is_hybrid = is_hdd && options["journal_device"] != "" && options["journal_device"] != options["data_device"];
    if (is_hdd)
    {
        if (options["block_size"] == "")
            options["block_size"] = "1M";
        if (is_hybrid && options["throttle_small_writes"] == "")
            options["throttle_small_writes"] = "1";
        if (!is_hybrid && options.find("data_csum_type") != options.end() && options.at("data_csum_type") != "")
            options["csum_block_size"] = "32k";
    }
    else if (!json_is_true(options["disable_data_fsync"]))
    {
        if (options.find("min_flusher_count") == options.end())
            options["min_flusher_count"] = "32";
        if (options.find("max_flusher_count") == options.end())
            options["max_flusher_count"] = "256";
        if (options.find("autosync_writes") == options.end())
            options["autosync_writes"] = "512";
    }
    json11::Json::object sb;
    blockstore_disk_t dsk;
    try
    {
        dsk.parse_config(options);
        dsk.data_io = dsk.meta_io = dsk.journal_io = "direct";
        dsk.open_data();
        dsk.open_meta();
        dsk.open_journal();
        dsk.calc_lengths(true);
        sb = json11::Json::object {
            { "data_device", options["data_device"] },
            { "meta_device", options["meta_device"] },
            { "journal_device", options["journal_device"] },
            { "block_size", (uint64_t)dsk.data_block_size },
            { "meta_block_size", dsk.meta_block_size },
            { "journal_block_size", dsk.journal_block_size },
            { "data_size", dsk.cfg_data_size },
            { "disk_alignment", (uint64_t)dsk.disk_alignment },
            { "bitmap_granularity", dsk.bitmap_granularity },
            { "disable_device_lock", dsk.disable_flock },
            { "journal_offset", 4096 },
            { "meta_offset", 4096 + (dsk.meta_device == dsk.journal_device ? dsk.journal_len : 0) },
            { "data_offset", 4096 + (dsk.data_device == dsk.meta_device ? dsk.meta_len : 0) +
                (dsk.data_device == dsk.journal_device ? dsk.journal_len : 0) },
            { "journal_no_same_sector_overwrites", !is_hdd || is_hybrid },
            { "journal_sector_buffer_count", 1024 },
            { "disable_data_fsync", json_is_true(options["disable_data_fsync"]) },
            { "disable_meta_fsync", json_is_true(options["disable_meta_fsync"]) },
            { "disable_journal_fsync", json_is_true(options["disable_journal_fsync"]) },
            { "skip_cache_check", json_is_true(options["skip_cache_check"]) },
            { "immediate_commit", json_is_true(options["disable_data_fsync"])
                ? (json_is_true(options["disable_journal_fsync"]) ? "all" : "small") : "none" },
        };
        for (int i = 0; i < sizeof(allow_additional_params)/sizeof(allow_additional_params[0]); i++)
        {
            auto it = options.find(allow_additional_params[i]);
            if (it != options.end() && it->second != "")
            {
                sb[it->first] = it->second;
            }
        }
    }
    catch (std::exception & e)
    {
        dsk.close_all();
        fprintf(stderr, "%s\n", e.what());
        return 1;
    }
    std::string osd_num_str;
    if (shell_exec({ "vitastor-cli", "alloc-osd" }, "", &osd_num_str, NULL) != 0)
    {
        dsk.close_all();
        return 1;
    }
    osd_num_t osd_num = stoull_full(trim(osd_num_str), 10);
    if (!osd_num)
    {
        dsk.close_all();
        fprintf(stderr, "Could not create OSD. vitastor-cli alloc-osd didn't return a valid OSD number:\n%s", osd_num_str.c_str());
        return 1;
    }
    sb["osd_num"] = osd_num;
    // Zero out metadata and journal
    if (write_zero(dsk.meta_fd, dsk.meta_offset, dsk.meta_len) != 0 ||
        write_zero(dsk.journal_fd, dsk.journal_offset, dsk.journal_len) != 0)
    {
        fprintf(stderr, "Failed to zero out metadata or journal: %s\n", strerror(errno));
        dsk.close_all();
        return 1;
    }
    dsk.close_all();
    // Write superblocks
    bool sep_m = options["meta_device"] != "" &&
        options["meta_device"] != options["data_device"];
    bool sep_j = options["journal_device"] != "" &&
        options["journal_device"] != options["data_device"] &&
        options["journal_device"] != options["meta_device"];
    if (!write_osd_superblock(options["data_device"], sb) ||
        sep_m && !write_osd_superblock(options["meta_device"], sb) ||
        sep_j && !write_osd_superblock(options["journal_device"], sb))
    {
        return 1;
    }
    auto desc = realpath_str(options["data_device"]);
    if (sep_m)
        desc += " with metadata on "+realpath_str(options["meta_device"]);
    if (sep_j)
        desc += (sep_m ? " and journal on " : " with journal on ") + realpath_str(options["journal_device"]);
    fprintf(stderr, "Initialized OSD %ju on %s\n", osd_num, desc.c_str());
    if (shell_exec({ "systemctl", "enable", "--now", "vitastor-osd@"+std::to_string(osd_num) }, "", NULL, NULL) != 0)
    {
        fprintf(stderr, "Failed to enable systemd unit vitastor-osd@%ju\n", osd_num);
        return 1;
    }
    return 0;
}

int disk_tool_t::check_existing_partition(const std::string & dev)
{
    std::string out;
    if (shell_exec({ "wipefs", dev }, "", &out, NULL) != 0 || out != "")
    {
        fprintf(stderr, "%s contains data, not creating OSD without --force. wipefs shows:\n%s", dev.c_str(), out.c_str());
        return 1;
    }
    json11::Json sb = read_osd_superblock(dev, false);
    if (!sb.is_null())
    {
        fprintf(stderr, "%s already contains Vitastor OSD superblock, not creating OSD without --force\n", dev.c_str());
        return 1;
    }
    if (fix_partition_type(dev) != 0)
    {
        fprintf(stderr, "%s has incorrect type and we failed to change it to Vitastor type\n", dev.c_str());
        return 1;
    }
    return 0;
}

std::vector<vitastor_dev_info_t> disk_tool_t::collect_devices(const std::vector<std::string> & devices)
{
    std::vector<vitastor_dev_info_t> devinfo;
    for (auto & dev: devices)
    {
        // Check if the device is a whole disk
        if (dev.substr(0, 5) != "/dev/")
        {
            fprintf(stderr, "%s does not start with /dev/, ignoring\n", dev.c_str());
            continue;
        }
        struct stat sys_st;
        uint64_t dev_size = get_device_size(dev, false);
        if (dev_size == UINT64_MAX)
        {
            return {};
        }
        else if (!dev_size)
        {
            fprintf(stderr, "%s does not exist, skipping\n", dev.c_str());
            continue;
        }
        if (stat(("/sys/block/"+dev.substr(5)).c_str(), &sys_st) < 0)
        {
            if (errno == ENOENT)
            {
                fprintf(stderr, "%s is probably a partition (no entry in /sys/block/), ignoring\n", dev.c_str());
                continue;
            }
            fprintf(stderr, "Error checking /sys/block/%s: %s\n", dev.c_str()+5, strerror(errno));
            return {};
        }
        // Check if the device is an SSD
        bool is_hdd = trim(read_file("/sys/block/"+dev.substr(5)+"/queue/rotational")) == "1";
        // Check if it has a partition table
        json11::Json pt = read_parttable(dev);
        if (pt.is_bool() && !pt.bool_value())
        {
            // Error reading table
            return {};
        }
        if (pt.is_null())
        {
            // No partition table
            std::string out;
            int r = shell_exec({ "wipefs", dev }, "", &out, NULL);
            if (r != 0 || out != "")
            {
                fprintf(stderr, "%s contains data, skipping:\n  %s\n", dev.c_str(), str_replace(trim(out), "\n", "\n  ").c_str());
                continue;
            }
        }
        int osds = 0;
        for (const auto & p: pt["partitions"].array_items())
            if (strtolower(p["type"].string_value()) == VITASTOR_PART_TYPE)
                osds++;
        devinfo.push_back((vitastor_dev_info_t){
            .path = dev,
            .is_hdd = is_hdd,
            .pt = pt,
            .osd_part_count = osds,
            .size = !pt.is_null() ? dev_size_from_parttable(pt) : dev_size,
            .free = !pt.is_null() ? free_from_parttable(pt) : dev_size,
        });
    }
    if (!devinfo.size())
    {
        fprintf(stderr, "No suitable devices found\n");
    }
    return devinfo;
}

// Return null in case of an error
json11::Json disk_tool_t::add_partitions(vitastor_dev_info_t & devinfo, std::vector<std::string> sizes)
{
    std::string script = "label: gpt\n\n";
    std::set<std::string> is_old;
    for (auto part: devinfo.pt["partitions"].array_items())
    {
        // Old partitions
        is_old.insert(part["uuid"].string_value());
        script += part["node"].string_value()+": ";
        int n = 0;
        for (auto & kv: part.object_items())
        {
            if (kv.first != "node")
            {
                if (n++)
                    script += ", ";
                script += kv.first+"="+(kv.second.is_string() ? kv.second.string_value() : kv.second.dump());
            }
        }
        script += "\n";
    }
    for (auto size: sizes)
    {
        script += "+ "+size+" "+std::string(VITASTOR_PART_TYPE)+"\n";
    }
    std::string out;
    if (shell_exec({ "sfdisk", "--no-reread", "--force", devinfo.path }, script, &out, NULL) != 0)
    {
        fprintf(stderr, "Failed to add %zu partition(s) with sfdisk\n", sizes.size());
        return {};
    }
    // Get new partition table and find created partitions
    json11::Json newpt = read_parttable(devinfo.path);
    json11::Json::array new_parts;
    for (const auto & part: newpt["partitions"].array_items())
    {
        if (is_old.find(part["uuid"].string_value()) == is_old.end())
        {
            new_parts.push_back(part);
        }
    }
    if (new_parts.size() != sizes.size())
    {
        fprintf(stderr, "Failed to add %zu partition(s) with sfdisk: new partitions not found in table\n", sizes.size());
        return {};
    }
    // Check if new nodes exist and run partprobe if not
    // FIXME: We could use parted instead of sfdisk because partprobe is already a part of parted
    int iter = 0, r;
    while (true)
    {
        for (const auto & part: new_parts)
        {
            struct stat st;
            if (stat(part["node"].string_value().c_str(), &st) < 0)
            {
                if (errno == ENOENT)
                {
                    iter++;
                    // Run partprobe
                    std::string out;
                    if (iter > 1 || (r = shell_exec({ "partprobe", devinfo.path }, "", &out, NULL)) != 0)
                    {
                        fprintf(
                            stderr, iter == 1 && r == 255
                                ? "partprobe utility is required to reread partition table while disk %s is in use\n"
                                : "partprobe failed to re-read partition table while disk %s is in use\n",
                            devinfo.path.c_str()
                        );
                        return {};
                    }
                    break;
                }
                else
                {
                    fprintf(stderr, "Failed to lstat %s: %s\n", part["node"].string_value().c_str(), strerror(errno));
                    return {};
                }
            }
        }
        break;
    }
    // Wait until device symlinks in /dev/disk/by-partuuid/ appear
    bool exists = false;
    iter = 0;
    while (!exists && iter < 300) // max 30 sec
    {
        exists = true;
        for (const auto & part: new_parts)
        {
            std::string link_path = "/dev/disk/by-partuuid/"+strtolower(part["uuid"].string_value());
            struct stat st;
            if (lstat(link_path.c_str(), &st) < 0)
            {
                if (errno == ENOENT)
                    exists = false;
                else
                {
                    fprintf(stderr, "Failed to lstat %s: %s\n", link_path.c_str(), strerror(errno));
                    return {};
                }
            }
        }
        if (!exists)
        {
            struct timespec ts = { .tv_sec = 0, .tv_nsec = 100000000 }; // 100ms
            iter += (nanosleep(&ts, NULL) == 0);
        }
    }
    devinfo.pt = newpt;
    devinfo.osd_part_count += sizes.size();
    devinfo.free = free_from_parttable(newpt);
    return new_parts;
}

std::vector<std::string> disk_tool_t::get_new_data_parts(vitastor_dev_info_t & dev,
    uint64_t osd_per_disk, uint64_t max_other_percent)
{
    std::vector<std::string> use_parts;
    uint64_t want_parts = 0;
    if (dev.pt.is_null())
    {
        want_parts = osd_per_disk;
    }
    else
    {
        // Disk already has partitions. If these are empty Vitastor OSD partitions, we can use them
        uint64_t osds_exist = 0, osds_size = 0;
        for (const auto & part: dev.pt["partitions"].array_items())
        {
            if (strtolower(part["type"].string_value()) == VITASTOR_PART_TYPE)
            {
                // Check if an existing Vitastor partition is empty
                json11::Json sb = read_osd_superblock(part["node"].string_value(), false);
                if (sb.is_null())
                {
                    // Use this partition
                    use_parts.push_back(part["uuid"].string_value());
                    osds_exist++;
                }
                else
                {
                    std::string part_path = "/dev/disk/by-partuuid/"+strtolower(part["uuid"].string_value());
                    bool is_meta = sb["params"]["meta_device"].string_value() == part_path;
                    bool is_journal = sb["params"]["journal_device"].string_value() == part_path;
                    bool is_data = sb["params"]["data_device"].string_value() == part_path;
                    fprintf(
                        stderr, "%s is already initialized for OSD %ju%s, skipping\n",
                        part["node"].string_value().c_str(), sb["params"]["osd_num"].uint64_value(),
                        (is_data ? " data" : (is_meta ? " meta" : (is_journal ? " journal" : "")))
                    );
                    if (is_data || sb["params"]["data_device"].string_value().substr(0, 22) != "/dev/disk/by-partuuid/")
                    {
                        osds_size += part["size"].uint64_value()*dev.pt["sectorsize"].uint64_value();
                        osds_exist++;
                    }
                }
            }
        }
        // Still create OSD(s) if a disk has no more than (max_other_percent) other data
        if (osds_exist >= osd_per_disk || (dev.free+osds_size) < dev.size*(100-max_other_percent)/100)
            fprintf(stderr, "%s is already partitioned, skipping\n", dev.path.c_str());
        else
            want_parts = osd_per_disk-osds_exist;
    }
    if (want_parts > 0)
    {
        // Disk is not partitioned yet - create OSD partition(s)
        std::vector<std::string> sizes;
        auto each_size = std::to_string((dev.free - 1048576) / 1048576 / want_parts)+"MiB";
        for (uint64_t i = 0; i < want_parts-1; i++)
            sizes.push_back(each_size);
        sizes.push_back("+");
        auto new_parts = add_partitions(dev, sizes);
        for (const auto & part: new_parts.array_items())
            use_parts.push_back(part["uuid"].string_value());
    }
    return use_parts;
}

int disk_tool_t::get_meta_partition(std::vector<vitastor_dev_info_t> & ssds, std::map<std::string, std::string> & options)
{
    uint64_t journal_size = parse_size(options["journal_size"]);
    journal_size = ((journal_size+1024*1024-1)/1024/1024)*1024*1024;
    // Calculate metadata size
    uint64_t meta_size = 0;
    try
    {
        blockstore_disk_t dsk;
        dsk.parse_config(options);
        dsk.data_io = dsk.meta_io = dsk.journal_io = "direct";
        dsk.open_data();
        dsk.open_meta();
        dsk.open_journal();
        dsk.calc_lengths(true);
        dsk.close_all();
        meta_size = dsk.meta_len;
    }
    catch (std::exception & e)
    {
        fprintf(stderr, "%s\n", e.what());
        return 1;
    }
    // Leave some extra space for future metadata formats and round metadata area size to multiples of 1 MB
    uint64_t meta_reserve_multiple = 2, min_meta_size = (uint64_t)1024*1024*1024;
    if (options.find("meta_reserve") != options.end())
    {
        int p1 = options["meta_reserve"].find("x"), p2 = options["meta_reserve"].find(",");
        if (p1 >= 0 && p2 >= 0)
        {
            meta_reserve_multiple = stoull_full(options["meta_reserve"].substr(p1 < p2 ? 0 : p2, p1 - (p1 < p2 ? 0 : p2)));
            min_meta_size = parse_size(options["meta_reserve"].substr(p1 < p2 ? p2 : 0, p1 < p2 ? options["meta_reserve"].size()-p2 : p2));
        }
        else if (p1 >= 0)
            meta_reserve_multiple = stoull_full(options["meta_reserve"].substr(0, p1));
        else
            min_meta_size = parse_size(options["meta_reserve"]);
    }
    meta_size = ((meta_size+1024*1024-1)/1024/1024)*1024*1024;
    meta_size *= meta_reserve_multiple;
    if (meta_size < min_meta_size)
        meta_size = min_meta_size;
    // Pick an SSD for journal&meta, balancing the number of serviced OSDs across SSDs
    int sel = -1;
    for (int i = 0; i < ssds.size(); i++)
        if (ssds[i].free >= (meta_size+journal_size+4096*2) && (sel == -1 || ssds[sel].osd_part_count > ssds[i].osd_part_count))
            sel = i;
    if (sel < 0)
    {
        fprintf(
            stderr, "Could not find free space for new SSD journal and metadata (need %ju + %ju MiB)\n",
            meta_size/1024/1024, journal_size/1024/1024
        );
        return 1;
    }
    // Create partitions
    auto new_parts = add_partitions(ssds[sel], {
        std::to_string(journal_size/1024/1024)+"MiB",
        std::to_string(meta_size/1024/1024)+"MiB"
    });
    if (new_parts.is_null())
    {
        return 1;
    }
    ssds[sel].osd_part_count += 2;
    options["journal_device"] = "/dev/disk/by-partuuid/"+strtolower(new_parts[0]["uuid"].string_value());
    options["meta_device"] = "/dev/disk/by-partuuid/"+strtolower(new_parts[1]["uuid"].string_value());
    return 0;
}

int disk_tool_t::prepare(std::vector<std::string> devices)
{
    if (options.find("data_device") != options.end() && options["data_device"] != "")
    {
        if (options.find("hybrid") != options.end() || options.find("osd_per_disk") != options.end() || devices.size())
        {
            fprintf(stderr, "Device list (positional arguments) and --hybrid are incompatible with --data_device\n");
            return 1;
        }
        return prepare_one(options, options.find("hdd") != options.end() ? 1 : 0);
    }
    if (!devices.size())
    {
        fprintf(stderr, "Device list missing\n");
        return 1;
    }
    options.erase("data_device");
    options.erase("meta_device");
    options.erase("journal_device");
    bool hybrid = options.find("hybrid") != options.end();
    auto devinfo = collect_devices(devices);
    if (!devinfo.size())
    {
        return 1;
    }
    uint64_t osd_per_disk = stoull_full(options["osd_per_disk"]);
    if (!osd_per_disk)
        osd_per_disk = 1;
    uint64_t max_other_percent = 10;
    if (options.find("max_other") != options.end())
    {
        max_other_percent = stoull_full(trim(options["max_other"], " \n\r\t%"));
        if (max_other_percent > 100)
            max_other_percent = 100;
    }
    std::vector<vitastor_dev_info_t> ssds;
    if (options.find("disable_data_fsync") == options.end())
        options["disable_data_fsync"] = "auto";
    if (hybrid)
    {
        if (options.find("disable_meta_fsync") == options.end())
            options["disable_meta_fsync"] = "auto";
        options["disable_journal_fsync"] = options["disable_meta_fsync"];
        for (auto & dev: devinfo)
            if (!dev.is_hdd)
                ssds.push_back(dev);
        if (!ssds.size())
        {
            fprintf(stderr, "No SSDs found\n");
            return 1;
        }
        else if (ssds.size() == devinfo.size())
        {
            fprintf(stderr, "No HDDs found\n");
            return 1;
        }
        if (options["journal_size"] == "")
            options["journal_size"] = DEFAULT_HYBRID_JOURNAL;
    }
    else
    {
        options.erase("disable_meta_fsync");
        options.erase("disable_journal_fsync");
    }
    auto journal_size = options["journal_size"];
    for (auto & dev: devinfo)
    {
        if (!hybrid || dev.is_hdd)
        {
            // Select new partitions and create an OSD on each of them
            for (const auto & uuid: get_new_data_parts(dev, osd_per_disk, max_other_percent))
            {
                options["force"] = true;
                options["data_device"] = "/dev/disk/by-partuuid/"+strtolower(uuid);
                if (hybrid)
                {
                    // Select/create journal and metadata partitions
                    int r = get_meta_partition(ssds, options);
                    if (r != 0)
                    {
                        return 1;
                    }
                    options.erase("journal_size");
                }
                // Treat all disks as SSDs if not in the hybrid mode
                prepare_one(options, dev.is_hdd ? 1 : 0);
                if (hybrid)
                {
                    options["journal_size"] = journal_size;
                    options.erase("journal_device");
                    options.erase("meta_device");
                }
            }
        }
    }
    return 0;
}
