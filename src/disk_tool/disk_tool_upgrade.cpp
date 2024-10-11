// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <regex>
#include "disk_tool.h"
#include "str_util.h"

static std::map<std::string, std::string> read_vitastor_unit(std::string unit)
{
    std::smatch m;
    if (unit == "" || !std::regex_match(unit, m, std::regex(".*/vitastor-osd\\d+\\.service")))
    {
        fprintf(stderr, "unit file name does not match <path>/vitastor-osd<NUMBER>.service\n");
        return {};
    }
    std::string text = read_file(unit);
    if (!std::regex_search(text, m, std::regex("\nExecStart\\s*=[^\n]+vitastor-osd\\s*(([^\\\\\n&>\\d]+|\\\\[ \t\r]*\n|\\d[^>])+)")))
    {
        fprintf(stderr, "Failed to extract ExecStart command from %s\n", unit.c_str());
        return {};
    }
    std::string cmd = trim(m[1]);
    cmd = str_replace(cmd, "\\\n", " ");
    std::string key;
    std::map<std::string, std::string> r;
    auto ns = std::regex("\\S+");
    for (auto it = std::sregex_token_iterator(cmd.begin(), cmd.end(), ns, 0), end = std::sregex_token_iterator();
        it != end; it++)
    {
        if (key == "" && ((std::string)(*it)).substr(0, 2) == "--")
            key = ((std::string)(*it)).substr(2);
        else if (key != "")
        {
            r[key] = *it;
            key = "";
        }
    }
    return r;
}

int disk_tool_t::upgrade_simple_unit(std::string unit)
{
    if (stoull_full(unit) != 0)
    {
        // OSD number
        unit = "/etc/systemd/system/vitastor-osd"+unit+".service";
    }
    auto options = read_vitastor_unit(unit);
    if (!options.size())
        return 1;
    if (!stoull_full(options["osd_num"], 10) || options["data_device"] == "")
    {
        fprintf(stderr, "osd_num or data_device are missing in %s\n", unit.c_str());
        return 1;
    }
    if (options["data_device"].substr(0, 22) != "/dev/disk/by-partuuid/" ||
        options["meta_device"] != "" && options["meta_device"].substr(0, 22) != "/dev/disk/by-partuuid/" ||
        options["journal_device"] != "" && options["journal_device"].substr(0, 22) != "/dev/disk/by-partuuid/")
    {
        fprintf(
            stderr, "data_device, meta_device and journal_device must begin with"
            " /dev/disk/by-partuuid/ i.e. they must be GPT partitions identified by UUIDs"
        );
        return 1;
    }
    // Stop and disable the service
    auto service_name = unit.substr(unit.rfind('/') + 1);
    if (shell_exec({ "systemctl", "disable", "--now", service_name }, "", NULL, NULL) != 0)
    {
        return 1;
    }
    uint64_t j_o = parse_size(options["journal_offset"]);
    uint64_t m_o = parse_size(options["meta_offset"]);
    uint64_t d_o = parse_size(options["data_offset"]);
    bool m_is_d = options["meta_device"] == "" || options["meta_device"] == options["data_device"];
    bool j_is_m = options["journal_device"] == "" || options["journal_device"] == options["meta_device"];
    bool j_is_d = j_is_m && m_is_d || options["journal_device"] == options["data_device"];
    if (d_o < 4096 || j_o < 4096 || m_o < 4096)
    {
        // Resize data
        uint64_t blk = stoull_full(options["block_size"]);
        blk = blk ? blk : 128*1024;
        std::map<std::string, uint64_t> resize;
        if (d_o < 4096 || m_is_d && m_o < 4096 && m_o < d_o || j_is_d && j_o < 4096 && j_o < d_o)
        {
            resize["new_data_offset"] = d_o+blk;
            if (m_is_d && m_o < d_o)
                resize["new_meta_offset"] = m_o+blk;
            if (j_is_d && j_o < d_o)
                resize["new_journal_offset"] = j_o+blk;
        }
        if (!m_is_d && m_o < 4096)
        {
            resize["new_meta_offset"] = m_o+4096;
            if (j_is_m && m_o < j_o)
                resize["new_journal_offset"] = j_o+4096;
        }
        if (!j_is_d && !j_is_m && j_o < 4096)
            resize["new_journal_offset"] = j_o+4096;
        disk_tool_t resizer;
        resizer.options = options;
        for (auto & kv: resize)
            resizer.options[kv.first] = std::to_string(kv.second);
        if (resizer.raw_resize() != 0)
        {
            // FIXME: Resize with backup or journal
            fprintf(
                stderr, "Failed to resize data to make space for the superblock\n"
                "Sorry, but your OSD may now be corrupted depending on what went wrong during resize :-(\n"
                "Please review the messages above and take action accordingly\n"
            );
            return 1;
        }
        for (auto & kv: resize)
            options[kv.first.substr(4)] = std::to_string(kv.second);
    }
    // Write superblocks
    if (!write_osd_superblock(options["data_device"], options) ||
        (!m_is_d && !write_osd_superblock(options["meta_device"], options)) ||
        (!j_is_m && !j_is_d && !write_osd_superblock(options["journal_device"], options)))
    {
        return 1;
    }
    // Change partition types
    if (fix_partition_type(options["data_device"]) != 0 ||
        (!m_is_d && fix_partition_type(options["meta_device"]) != 0) ||
        (!j_is_m && !j_is_d && fix_partition_type(options["journal_device"]) != 0))
    {
        return 1;
    }
    // Enable the new unit
    if (shell_exec({ "systemctl", "enable", "--now", "vitastor-osd@"+options["osd_num"] }, "", NULL, NULL) != 0)
    {
        fprintf(stderr, "Failed to enable systemd unit vitastor-osd@%s\n", options["osd_num"].c_str());
        return 1;
    }
    fprintf(
        stderr, "\nOK: Converted OSD %s to the new scheme. The new service name is vitastor-osd@%s\n",
        options["osd_num"].c_str(), options["osd_num"].c_str()
    );
    return 0;
}
