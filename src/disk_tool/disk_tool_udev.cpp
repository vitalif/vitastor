// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <sys/file.h>

#include "disk_tool.h"
#include "rw_blocking.h"
#include "str_util.h"
#include "json_util.h"

struct __attribute__((__packed__)) vitastor_disk_superblock_t
{
    uint64_t magic;
    uint32_t crc32c;
    uint32_t size;
    uint8_t json_data[];
};

static std::string udev_escape(std::string str)
{
    std::string r;
    int p = str.find_first_of("\"\' \t\r\n"), prev = 0;
    if (p == std::string::npos)
    {
        return str;
    }
    while (p != std::string::npos)
    {
        r += str.substr(prev, p-prev);
        r += "\\";
        prev = p;
        p = str.find_first_of("\"\' \t\r\n", p+1);
    }
    r += str.substr(prev);
    return r;
}

int disk_tool_t::udev_import(std::string device)
{
    json11::Json sb = read_osd_superblock(device);
    if (sb.is_null())
    {
        return 1;
    }
    uint64_t osd_num = sb["params"]["osd_num"].uint64_value();
    // Print variables for udev
    printf("VITASTOR_OSD_NUM=%ju\n", osd_num);
    printf("VITASTOR_ALIAS=osd%ju-%s\n", osd_num, sb["device_type"].string_value().c_str());
    printf("VITASTOR_DATA_DEVICE=%s\n", udev_escape(sb["params"]["data_device"].string_value()).c_str());
    if (sb["real_meta_device"].string_value() != "" && sb["real_meta_device"] != sb["real_data_device"])
        printf("VITASTOR_META_DEVICE=%s\n", udev_escape(sb["params"]["meta_device"].string_value()).c_str());
    if (sb["real_journal_device"].string_value() != "" && sb["real_journal_device"] != sb["real_meta_device"])
        printf("VITASTOR_JOURNAL_DEVICE=%s\n", udev_escape(sb["params"]["journal_device"].string_value()).c_str());
    return 0;
}

int disk_tool_t::read_sb(std::string device)
{
    json11::Json sb = read_osd_superblock(device, true, options.find("force") != options.end());
    if (sb.is_null())
    {
        return 1;
    }
    printf("%s\n", sb["params"].dump().c_str());
    return 0;
}

int disk_tool_t::write_sb(std::string device)
{
    std::string input;
    int r;
    char buf[4096];
    while (1)
    {
        r = read(0, buf, sizeof(buf));
        if (r <= 0 && errno != EAGAIN)
            break;
        input += std::string(buf, r);
    }
    std::string json_err;
    json11::Json params = json11::Json::parse(input, json_err);
    if (json_err != "" || !params["osd_num"].uint64_value() || params["data_device"].string_value() == "")
    {
        fprintf(stderr, "Invalid JSON input\n");
        return 1;
    }
    return !write_osd_superblock(device, params);
}

int disk_tool_t::update_sb(std::string device)
{
    json11::Json sb = read_osd_superblock(device, true, options.find("force") != options.end());
    if (sb.is_null())
    {
        return 1;
    }
    auto sb_obj = sb["params"].object_items();
    for (auto & kv: options)
    {
        if (kv.first != "force")
        {
            sb_obj[kv.first] = kv.second;
        }
    }
    return !write_osd_superblock(device, sb_obj);
}

uint32_t disk_tool_t::write_osd_superblock(std::string device, json11::Json params)
{
    std::string json_data = params.dump();
    uint32_t sb_size = sizeof(vitastor_disk_superblock_t)+json_data.size();
    if (sb_size > VITASTOR_DISK_MAX_SB_SIZE)
    {
        fprintf(stderr, "JSON data for superblock is too large\n");
        return 0;
    }
    uint64_t buf_len = ((sb_size+4095)/4096) * 4096;
    uint8_t *buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, buf_len);
    memset(buf, 0, buf_len);
    vitastor_disk_superblock_t *sb = (vitastor_disk_superblock_t*)buf;
    sb->magic = VITASTOR_DISK_MAGIC;
    sb->size = sb_size;
    memcpy(sb->json_data, json_data.c_str(), json_data.size());
    sb->crc32c = crc32c(0, &sb->size, sb->size - ((uint8_t*)&sb->size - buf));
    int fd = open(device.c_str(), (options["io"] == "cached" ? 0 : O_DIRECT) | O_RDWR);
    if (fd < 0)
    {
        fprintf(stderr, "Failed to open device %s: %s\n", device.c_str(), strerror(errno));
        free(buf);
        return 0;
    }
    int r = write_blocking(fd, buf, buf_len);
    if (r < 0)
    {
        fprintf(stderr, "Failed to write to %s: %s\n", device.c_str(), strerror(errno));
        close(fd);
        free(buf);
        return 0;
    }
    close(fd);
    free(buf);
    shell_exec({ "udevadm", "trigger", "--settle", device }, "", NULL, NULL);
    return sb_size;
}

json11::Json disk_tool_t::read_osd_superblock(std::string device, bool expect_exist, bool ignore_errors)
{
    vitastor_disk_superblock_t *sb = NULL;
    uint8_t *buf = NULL;
    json11::Json osd_params;
    std::string json_err;
    std::string real_device, device_type, real_data, real_meta, real_journal;
    int r, fd = open(device.c_str(), (options["io"] == "cached" ? 0 : O_DIRECT) | O_RDWR);
    if (fd < 0)
    {
        fprintf(stderr, "Failed to open device %s: %s\n", device.c_str(), strerror(errno));
        return osd_params;
    }
    buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 4096);
    r = read_blocking(fd, buf, 4096);
    if (r != 4096)
    {
        fprintf(stderr, "Failed to read OSD superblock from %s: %s\n", device.c_str(), strerror(errno));
        goto ex;
    }
    sb = (vitastor_disk_superblock_t*)buf;
    if (sb->magic != VITASTOR_DISK_MAGIC && !ignore_errors)
    {
        if (expect_exist)
            fprintf(stderr, "Invalid OSD superblock on %s: magic number mismatch\n", device.c_str());
        goto ex;
    }
    if (sb->size > VITASTOR_DISK_MAX_SB_SIZE ||
        // +2 is minimal json: {}
        sb->size < sizeof(vitastor_disk_superblock_t)+2)
    {
        if (expect_exist)
            fprintf(stderr, "Invalid OSD superblock on %s: invalid size\n", device.c_str());
        goto ex;
    }
    if (sb->size > 4096)
    {
        uint64_t sb_size = ((sb->size+4095)/4096)*4096;
        free(buf);
        buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, sb_size);
        lseek64(fd, 0, 0);
        r = read_blocking(fd, buf, sb_size);
        if (r != sb_size)
        {
            fprintf(stderr, "Failed to read OSD superblock from %s: %s\n", device.c_str(), strerror(errno));
            goto ex;
        }
        sb = (vitastor_disk_superblock_t*)buf;
    }
    if (sb->crc32c != crc32c(0, &sb->size, sb->size - ((uint8_t*)&sb->size - buf)) && !ignore_errors)
    {
        if (expect_exist)
            fprintf(stderr, "Invalid OSD superblock on %s: crc32 mismatch\n", device.c_str());
        goto ex;
    }
    osd_params = json11::Json::parse(std::string((char*)sb->json_data, sb->size - sizeof(vitastor_disk_superblock_t)), json_err);
    if (json_err != "")
    {
        if (expect_exist)
            fprintf(stderr, "Invalid OSD superblock on %s: invalid JSON\n", device.c_str());
        goto ex;
    }
    // Validate superblock
    if (!osd_params["osd_num"].uint64_value() && !ignore_errors)
    {
        if (expect_exist)
            fprintf(stderr, "OSD superblock on %s lacks osd_num\n", device.c_str());
        osd_params = json11::Json();
        goto ex;
    }
    if (osd_params["data_device"].string_value() == "" && !ignore_errors)
    {
        if (expect_exist)
            fprintf(stderr, "OSD superblock on %s lacks data_device\n", device.c_str());
        osd_params = json11::Json();
        goto ex;
    }
    real_device = realpath_str(device);
    real_data = realpath_str(osd_params["data_device"].string_value());
    real_meta = osd_params["meta_device"].string_value() != "" && osd_params["meta_device"] != osd_params["data_device"]
        ? realpath_str(osd_params["meta_device"].string_value()) : "";
    real_journal = osd_params["journal_device"].string_value() != "" && osd_params["journal_device"] != osd_params["meta_device"]
        ? realpath_str(osd_params["journal_device"].string_value()) : "";
    if (real_journal == real_meta)
    {
        real_journal = "";
    }
    if (real_meta == real_data)
    {
        real_meta = "";
    }
    if (real_device == real_data)
    {
        device_type = "data";
    }
    else if (real_device == real_meta)
    {
        device_type = "meta";
    }
    else if (real_device == real_journal)
    {
        device_type = "journal";
    }
    else if (!ignore_errors)
    {
        if (expect_exist)
            fprintf(stderr, "Invalid OSD superblock on %s: does not refer to the device itself\n", device.c_str());
        osd_params = json11::Json();
        goto ex;
    }
    osd_params = json11::Json::object{
        { "params", osd_params },
        { "device_type", device_type },
        { "real_data_device", real_data },
        { "real_meta_device", real_meta },
        { "real_journal_device", real_journal },
    };
ex:
    free(buf);
    close(fd);
    return osd_params;
}

int disk_tool_t::systemd_start_stop_osds(const std::vector<std::string> & cmd, const std::vector<std::string> & devices)
{
    if (!devices.size())
    {
        fprintf(stderr, "Device path is missing\n");
        return 1;
    }
    std::vector<std::string> svcs;
    for (auto & device: devices)
    {
        json11::Json sb = read_osd_superblock(device);
        if (!sb.is_null())
        {
            svcs.push_back("vitastor-osd@"+sb["params"]["osd_num"].as_string());
        }
    }
    if (!svcs.size())
    {
        return 1;
    }
    std::vector<char*> argv;
    argv.push_back((char*)"systemctl");
    for (auto & s: cmd)
    {
        argv.push_back((char*)s.c_str());
    }
    for (auto & s: svcs)
    {
        argv.push_back((char*)s.c_str());
    }
    argv.push_back(NULL);
    execvpe("systemctl", argv.data(), environ);
    return 0;
}

int disk_tool_t::exec_osd(std::string device)
{
    json11::Json sb = read_osd_superblock(device);
    if (sb.is_null())
    {
        return 1;
    }
    std::string osd_binary = "vitastor-osd";
    if (options["osd-binary"] != "")
    {
        osd_binary = options["osd-binary"];
    }
    std::vector<std::string> argstr;
    argstr.push_back(osd_binary.c_str());
    for (auto & kv: sb["params"].object_items())
    {
        argstr.push_back("--"+kv.first);
        argstr.push_back(kv.second.is_string() ? kv.second.string_value() : kv.second.dump());
    }
    char *argv[argstr.size()+1];
    for (int i = 0; i < argstr.size(); i++)
    {
        argv[i] = (char*)argstr[i].c_str();
    }
    argv[argstr.size()] = NULL;
    return execvpe(osd_binary.c_str(), argv, environ);
}

static int check_disabled_cache(std::string dev)
{
    int r = disable_cache(dev);
    if (r == 1)
    {
        fprintf(
            stderr, "Warning: fsync is disabled for %s, but cache status check failed."
            " Ensure that cache is in write-through mode yourself or you may lose data.\n", dev.c_str()
        );
    }
    else if (r == -1)
    {
        fprintf(
            stderr, "Error: fsync is disabled for %s, but its cache is in write-back mode"
            " and we failed to make it write-through. Data loss is presumably possible."
            " Either switch the cache to write-through mode yourself or disable the check"
            " using skip_cache_check=1 in the superblock.\n", dev.c_str()
        );
        return 1;
    }
    return 0;
}

int disk_tool_t::pre_exec_osd(std::string device)
{
    json11::Json sb = read_osd_superblock(device);
    if (sb.is_null())
    {
        return 1;
    }
    if (!json_is_true(sb["params"]["skip_cache_check"]))
    {
        if (json_is_true(sb["params"]["disable_data_fsync"]) &&
            check_disabled_cache(sb["real_data_device"].string_value()) != 0)
        {
            return 1;
        }
        if (json_is_true(sb["params"]["disable_meta_fsync"]) &&
            sb["real_meta_device"].string_value() != "" && sb["real_meta_device"] != sb["real_data_device"] &&
            check_disabled_cache(sb["real_meta_device"].string_value()) != 0)
        {
            return 1;
        }
        if (json_is_true(sb["params"]["disable_journal_fsync"]) &&
            sb["real_journal_device"].string_value() != "" && sb["real_journal_device"] != sb["real_meta_device"] &&
            check_disabled_cache(sb["real_journal_device"].string_value()) != 0)
        {
            return 1;
        }
    }
    return 0;
}

int disk_tool_t::clear_osd_superblock(const std::string & dev)
{
    uint8_t *buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 4096);
    int fd = -1, r = open(dev.c_str(), (options["io"] == "cached" ? 0 : O_DIRECT) | O_RDWR);
    if (r >= 0)
    {
        fd = r;
        r = read_blocking(fd, buf, 4096);
        if (r == 4096)
        {
            // Clear magic and CRC
            memset(buf, 0, 12);
            r = lseek64(fd, 0, 0);
            if (r == 0)
            {
                r = write_blocking(fd, buf, 4096);
                if (r == 4096)
                    r = 0;
            }
        }
    }
    if (fd >= 0)
        close(fd);
    free(buf);
    buf = NULL;
    return r;
}

int disk_tool_t::purge_devices(const std::vector<std::string> & devices)
{
    std::set<uint64_t> osd_numbers;
    json11::Json::array superblocks;
    for (auto & device: devices)
    {
        json11::Json sb = read_osd_superblock(device);
        if (!sb.is_null())
        {
            uint64_t osd_num = sb["params"]["osd_num"].uint64_value();
            if (osd_numbers.find(osd_num) == osd_numbers.end())
            {
                osd_numbers.insert(osd_num);
                superblocks.push_back(sb);
            }
        }
    }
    if (!osd_numbers.size())
    {
        return 0;
    }
    std::vector<std::string> rm_osd_cli = { "vitastor-cli", "rm-osd" };
    for (auto osd_num: osd_numbers)
    {
        rm_osd_cli.push_back(std::to_string(osd_num));
    }
    // Check for data loss
    if (options["force"] != "")
    {
        rm_osd_cli.push_back("--force");
    }
    else if (options["allow_data_loss"] != "")
    {
        rm_osd_cli.push_back("--allow-data-loss");
    }
    rm_osd_cli.push_back("--dry-run");
    std::string dry_run_ignore_stdout;
    if (shell_exec(rm_osd_cli, "", &dry_run_ignore_stdout, NULL) != 0)
    {
        return 1;
    }
    // Disable & stop OSDs
    std::vector<std::string> systemctl_cli = { "systemctl", "disable", "--now" };
    for (auto osd_num: osd_numbers)
    {
        systemctl_cli.push_back("vitastor-osd@"+std::to_string(osd_num));
    }
    if (shell_exec(systemctl_cli, "", NULL, NULL) != 0)
    {
        return 1;
    }
    // Remove OSD metadata
    rm_osd_cli.pop_back();
    if (shell_exec(rm_osd_cli, "", NULL, NULL) != 0)
    {
        return 1;
    }
    // Destroy OSD superblocks
    for (auto & sb: superblocks)
    {
        for (auto dev_type: std::vector<std::string>{ "data", "meta", "journal" })
        {
            auto dev = sb["real_"+dev_type+"_device"].string_value();
            if (dev != "")
            {
                int r = clear_osd_superblock(dev);
                if (r != 0)
                {
                    fprintf(stderr, "Failed to clear OSD %ju %s device %s superblock: %s\n",
                        sb["params"]["osd_num"].uint64_value(), dev_type.c_str(), dev.c_str(), strerror(errno));
                }
                else
                {
                    fprintf(stderr, "OSD %ju %s device %s superblock cleared\n",
                        sb["params"]["osd_num"].uint64_value(), dev_type.c_str(), dev.c_str());
                }
                if (sb["params"][dev_type+"_device"].string_value().substr(0, 22) == "/dev/disk/by-partuuid/")
                {
                    // Delete the partition itself
                    auto uuid_to_del = strtolower(sb["params"][dev_type+"_device"].string_value().substr(22));
                    auto parent_dev = get_parent_device(dev);
                    if (parent_dev == "" || parent_dev == dev)
                    {
                        fprintf(stderr, "Failed to delete partition %s: failed to find parent device\n", dev.c_str());
                        continue;
                    }
                    auto pt = read_parttable(parent_dev);
                    if (!pt.is_object())
                        continue;
                    json11::Json::array newpt = pt["partitions"].array_items();
                    for (int i = 0; i < newpt.size(); i++)
                    {
                        if (strtolower(newpt[i]["uuid"].string_value()) == uuid_to_del)
                        {
                            auto old_part = newpt[i];
                            newpt.erase(newpt.begin()+i, newpt.begin()+i+1);
                            vitastor_dev_info_t devinfo = {
                                .path = parent_dev,
                                .pt = json11::Json::object{ { "partitions", newpt } },
                            };
                            add_partitions(devinfo, {});
                            struct stat st;
                            if (stat(old_part["node"].string_value().c_str(), &st) == 0 ||
                                errno != ENOENT)
                            {
                                std::string out;
                                shell_exec({ "partprobe", parent_dev }, "", &out, NULL);
                            }
                            break;
                        }
                    }
                }
            }
        }
    }
    return 0;
}
