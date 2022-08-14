// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "disk_tool.h"
#include "str_util.h"

static const char *help_text =
    "Vitastor disk management tool\n"
    "(c) Vitaliy Filippov, 2022+ (VNPL-1.1)\n"
    "\n"
    "COMMANDS:\n"
    "\n"
    "vitastor-disk prepare [OPTIONS] [devices...]\n"
    "  Initialize disk(s) for Vitastor OSD(s).\n"
    "  There are two forms of this command. In the first form, you pass <devices> which\n"
    "  must be raw disks (not partitions). They are partitioned automatically and OSDs\n"
    "  are initialized on all of them.\n"
    "  In the second form, you omit <devices> and pass --data_device, --journal_device\n"
    "  and/or --meta_device which must be already existing partitions. In this case\n"
    "  a single OSD is created.\n"
    "  OPTIONS may include:\n"
    "    --hybrid\n"
    "      Prepare hybrid (HDD+SSD) OSDs using provided devices. SSDs will be used for\n"
    "      journals and metadata, HDDs will be used for data. Partitions for journals and\n"
    "      metadata will be created automatically. SSD/HDD are found by the `rotational`\n"
    "      flag of devices. In hybrid mode, object size is 1 MB instead of 128 KB by\n"
    "      default, and journal size is 1 GB instead of 32 MB by default.\n"
    "    --osd_per_disk <N>         Create <N> OSDs on each disk (default 1)\n"
    "    --data_device <DEV>        Create a single OSD using partition <DEV> for data\n"
    "    --meta_device <DEV>        Create a single OSD using partition <DEV> for metadata\n"
    "    --journal_device <DEV>     Create a single OSD using partition <DEV> for journal\n"
    "    --journal_size 1G/32M      Set journal size\n"
    "    --object_size 1M/128k      Set blockstore object size\n"
    "    --disable_ssd_cache 1      Disable cache and fsyncs for SSD journal and metadata\n"
    "    --disable_hdd_cache 1      Disable cache and fsyncs for HDD data\n"
    "    --bitmap_granularity 4k    Set bitmap granularity\n"
    "    --data_device_block 4k     Override data device block size\n"
    "    --meta_device_block 4k     Override metadata device block size\n"
    "    --journal_device_block 4k  Override journal device block size\n"
    "    --meta_reserve 2x,1G\n"
    "      New metadata partitions in --hybrid mode are created larger than actual\n"
    "      metadata size to ease possible future extension. The default is to allocate\n"
    "      2 times more space and at least 1G. Use this option to override.\n"
    "    --max_other 10%\n"
    "      Use disks for OSD data even if they already have non-Vitastor partitions,\n"
    "      but only if these take up no more than this percent of disk space.\n"
    "\n"
    "vitastor-disk resize <ALL_OSD_PARAMETERS> <NEW_LAYOUT> [--iodepth 32]\n"
    "  Resize data area and/or rewrite/move journal and metadata\n"
    "  ALL_OSD_PARAMETERS must include all (at least all disk-related)\n"
    "  parameters from OSD command line (i.e. from systemd unit).\n"
    "  NEW_LAYOUT may include new disk layout parameters:\n"
    "    [--new_data_offset <NUMBER>]     resize data area so it starts at <NUMBER>\n"
    "    [--new_data_len <NUMBER>]        resize data area to <NUMBER> bytes\n"
    "    [--new_meta_device <PATH>]       use <PATH> for new metadata\n"
    "    [--new_meta_offset <NUMBER>]     make new metadata area start at <NUMBER>\n"
    "    [--new_meta_len <NUMBER>]        make new metadata area <NUMBER> bytes long\n"
    "    [--new_journal_device <PATH>]    use <PATH> for new journal\n"
    "    [--new_journal_offset <NUMBER>]  make new journal area start at <NUMBER>\n"
    "    [--new_journal_len <NUMBER>]     make new journal area <NUMBER> bytes long\n"
    "  If any of the new layout parameter options are not specified, old values\n"
    "  will be used.\n"
    "\n"
    "vitastor-disk start|stop|restart|enable|disable [--now] <device> [device2 device3 ...]\n"
    "  Manipulate Vitastor OSDs using systemd by their device paths.\n"
    "  Commands are passed to systemctl with vitastor-osd@<num> units as arguments.\n"
    "  When --now is added to enable/disable, OSDs are also immediately started/stopped.\n"
    "\n"
    "vitastor-disk read-sb <device>\n"
    "  Try to read Vitastor OSD superblock from <device> and print it in JSON format.\n"
    "\n"
    "vitastor-disk write-sb <device>\n"
    "  Read JSON from STDIN and write it into Vitastor OSD superblock on <device>.\n"
    "\n"
    "vitastor-disk udev <device>\n"
    "  Try to read Vitastor OSD superblock from <device> and print variables for udev.\n"
    "\n"
    "vitastor-disk exec-osd <device>\n"
    "  Read Vitastor OSD superblock from <device> and start the OSD with parameters from it.\n"
    "  Intended for use from startup scripts (i.e. from systemd units).\n"
    "\n"
    "vitastor-disk pre-exec <device>\n"
    "  Read Vitastor OSD superblock from <device> and perform pre-start checks for the OSD.\n"
    "  For now, this only checks that device cache is in write-through mode if fsync is disabled.\n"
    "  Intended for use from startup scripts (i.e. from systemd units).\n"
    "\n"
    "vitastor-disk dump-journal [--all] [--json] <journal_file> <journal_block_size> <offset> <size>\n"
    "  Dump journal in human-readable or JSON (if --json is specified) format.\n"
    "  Without --all, only actual part of the journal is dumped.\n"
    "  With --all, the whole journal area is scanned for journal entries,\n"
    "  some of which may be outdated.\n"
    "\n"
    "vitastor-disk dump-meta <meta_file> <meta_block_size> <offset> <size>\n"
    "  Dump metadata in JSON format.\n"
    "\n"
    "vitastor-disk simple-offsets <device>\n"
    "  Calculate offsets for old simple&stupid (no superblock) OSD deployment. Options:\n"
    "  --object_size 128k       Set blockstore block size\n"
    "  --bitmap_granularity 4k  Set bitmap granularity\n"
    "  --journal_size 16M       Set journal size\n"
    "  --device_block_size 4k   Set device block size\n"
    "  --journal_offset 0       Set journal offset\n"
    "  --device_size 0          Set device size\n"
    "  --format text            Result format: json, options, env, or text\n"
    "\n"
    "Use vitastor-disk --help <command> for command details or vitastor-disk --help --all for all details.\n"
;

disk_tool_t::~disk_tool_t()
{
    if (data_alloc)
    {
        delete data_alloc;
        data_alloc = NULL;
    }
}

int main(int argc, char *argv[])
{
    disk_tool_t self = {};
    std::vector<char*> cmd;
    char *exe_name = strrchr(argv[0], '/');
    exe_name = exe_name ? exe_name+1 : argv[0];
    bool aliased = false;
    if (!strcmp(exe_name, "vitastor-dump-journal"))
    {
        cmd.push_back((char*)"dump-journal");
        aliased = true;
    }
    for (int i = 1; i < argc; i++)
    {
        if (!strcmp(argv[i], "--all"))
        {
            self.all = true;
        }
        else if (!strcmp(argv[i], "--json"))
        {
            self.json = true;
        }
        else if (!strcmp(argv[i], "--hybrid"))
        {
            self.options["hybrid"] = "1";
        }
        else if (!strcmp(argv[i], "--help"))
        {
            cmd.insert(cmd.begin(), (char*)"help");
        }
        else if (!strcmp(argv[i], "--now"))
        {
            self.now = true;
        }
        else if (!strcmp(argv[i], "--force"))
        {
            self.options["force"] = "1";
        }
        else if (argv[i][0] == '-' && argv[i][1] == '-')
        {
            char *key = argv[i]+2;
            self.options[key] = argv[++i];
        }
        else
        {
            cmd.push_back(argv[i]);
        }
    }
    if (!cmd.size())
    {
        cmd.push_back((char*)"help");
    }
    if (!strcmp(cmd[0], "dump-journal"))
    {
        if (cmd.size() < 5)
        {
            fprintf(stderr, "USAGE: %s%s [--all] [--json] <journal_file> <journal_block_size> <offset> <size>\n", argv[0], aliased ? "" : " dump-journal");
            return 1;
        }
        self.dsk.journal_device = cmd[1];
        self.dsk.journal_block_size = strtoul(cmd[2], NULL, 10);
        self.dsk.journal_offset = strtoull(cmd[3], NULL, 10);
        self.dsk.journal_len = strtoull(cmd[4], NULL, 10);
        return self.dump_journal();
    }
    else if (!strcmp(cmd[0], "dump-meta"))
    {
        if (cmd.size() < 5)
        {
            fprintf(stderr, "USAGE: %s dump-meta <meta_file> <meta_block_size> <offset> <size>\n", argv[0]);
            return 1;
        }
        self.dsk.meta_device = cmd[1];
        self.dsk.meta_block_size = strtoul(cmd[2], NULL, 10);
        self.dsk.meta_offset = strtoull(cmd[3], NULL, 10);
        self.dsk.meta_len = strtoull(cmd[4], NULL, 10);
        return self.dump_meta();
    }
    else if (!strcmp(cmd[0], "resize"))
    {
        return self.resize_data();
    }
    else if (!strcmp(cmd[0], "simple-offsets"))
    {
        // Calculate offsets for simple & stupid OSD deployment without superblock
        if (cmd.size() > 1)
        {
            self.options["device"] = cmd[1];
        }
        disk_tool_simple_offsets(self.options, self.json);
        return 0;
    }
    else if (!strcmp(cmd[0], "udev"))
    {
        if (cmd.size() != 2)
        {
            fprintf(stderr, "Exactly 1 device path argument is required\n");
            return 1;
        }
        return self.udev_import(cmd[1]);
    }
    else if (!strcmp(cmd[0], "read-sb"))
    {
        if (cmd.size() != 2)
        {
            fprintf(stderr, "Exactly 1 device path argument is required\n");
            return 1;
        }
        return self.read_sb(cmd[1]);
    }
    else if (!strcmp(cmd[0], "write-sb"))
    {
        if (cmd.size() != 2)
        {
            fprintf(stderr, "Exactly 1 device path argument is required\n");
            return 1;
        }
        return self.write_sb(cmd[1]);
    }
    else if (!strcmp(cmd[0], "start") || !strcmp(cmd[0], "stop") ||
        !strcmp(cmd[0], "restart") || !strcmp(cmd[0], "enable") || !strcmp(cmd[0], "disable"))
    {
        std::vector<std::string> systemd_cmd;
        systemd_cmd.push_back(cmd[0]);
        if (self.now && (!strcmp(cmd[0], "enable") || !strcmp(cmd[0], "disable")))
        {
            systemd_cmd.push_back("--now");
        }
        return self.systemd_start_stop_osds(systemd_cmd, std::vector<std::string>(cmd.begin()+1, cmd.end()));
    }
    else if (!strcmp(cmd[0], "exec-osd"))
    {
        if (cmd.size() != 2)
        {
            fprintf(stderr, "Exactly 1 device path argument is required\n");
            return 1;
        }
        return self.exec_osd(cmd[1]);
    }
    else if (!strcmp(cmd[0], "pre-exec"))
    {
        if (cmd.size() != 2)
        {
            fprintf(stderr, "Exactly 1 device path argument is required\n");
            return 1;
        }
        return self.pre_exec_osd(cmd[1]);
    }
    else
    {
        print_help(help_text, "vitastor-disk", cmd.size() > 1 ? cmd[1] : "", self.all);
    }
    return 0;
}
