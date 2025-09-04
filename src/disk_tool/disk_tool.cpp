// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "disk_tool.h"
#include "str_util.h"

static const char *help_text =
    "Vitastor disk management tool " VITASTOR_VERSION "\n"
    "(c) Vitaliy Filippov, 2022+ (VNPL-1.1)\n"
    "\n"
    "COMMANDS:\n"
    "\n"
    "vitastor-disk prepare [OPTIONS] [devices...]\n"
    "  Initialize disk(s) for Vitastor OSD(s).\n"
    "  \n"
    "  There are two modes of this command. In the first mode, you pass <devices> which\n"
    "  must be raw disks (not partitions). They are partitioned automatically and OSDs\n"
    "  are initialized on all of them.\n"
    "  \n"
    "  In the second mode, you omit <devices> and pass --data_device, --journal_device\n"
    "  and/or --meta_device which must be already existing partitions identified by their\n"
    "  GPT partition UUIDs. In this case a single OSD is created.\n"
    "  \n"
    "  Requires `vitastor-cli`, `wipefs`, `sfdisk` and `partprobe` (from parted) utilities.\n"
    "  \n"
    "  Options (automatic mode):\n"
    "    --osd_per_disk <N>\n"
    "      Create <N> OSDs on each disk (default 1)\n"
    "    --hybrid\n"
    "      Prepare hybrid (HDD+SSD, NVMe+SATA or etc) OSDs using provided devices. By default,\n"
    "      any passed SSDs will be used for journals and metadata, HDDs will be used for data,\n"
    "      but you can override this behaviour with --fast-devices option. Journal and metadata\n"
    "      partitions will be created automatically. In the default mode, SSD and HDD disks\n"
    "      are distinguished by the `/sys/block/.../queue/rotational` flag. When HDDs are used\n"
    "      for data in hybrid mode, default block_size is 1 MB instead of 128 KB, default journal\n"
    "      size is 1 GB instead of 32 MB, and throttle_small_writes is enabled by default.\n"
    "    --fast-devices /dev/nvmeX,/dev/nvmeY\n"
    "      In --hybrid mode, use these devices for journal and metadata instead of auto-detecting\n"
    "      and extracting them from the main [devices...] list.\n"
    "    --disable_data_fsync auto\n"
    "      Disable data device cache and fsync (1/yes/true = on, default auto)\n"
    "    --disable_meta_fsync auto\n"
    "      Disable metadata/journal device cache and fsync (default auto)\n"
    "    --meta_reserve 2x,1G\n"
    "      New metadata partitions in --hybrid mode are created larger than actual\n"
    "      metadata size to ease possible future extension. The default is to allocate\n"
    "      2 times more space and at least 1G. Use this option to override.\n"
    "    --max_other 10%\n"
    "      Use disks for OSD data even if they already have non-Vitastor partitions,\n"
    "      but only if these take up no more than this percent of disk space.\n"
    "    --dry-run\n"
    "      Check and print new OSD count for each disk but do not actually create them.\n"
    "  \n"
    "  Options (single-device mode):\n"
    "    --data_device <DEV>        Use partition <DEV> for data\n"
    "    --meta_device <DEV>        Use partition <DEV> for metadata (optional)\n"
    "    --journal_device <DEV>     Use partition <DEV> for journal (optional)\n"
    "    --disable_data_fsync 0     Disable data device cache and fsync (default off)\n"
    "    --disable_meta_fsync 0     Disable metadata device cache and fsync (default off)\n"
    "    --disable_journal_fsync 0  Disable journal device cache and fsync (default off)\n"
    "    --hdd                      Enable HDD defaults (1M block, 1G journal, throttling)\n"
    "    --force                    Bypass partition safety checks (for emptiness and so on)\n"
    "  \n"
    "  Options (both modes):\n"
    "    --journal_size 32M/1G      Set journal size (area or partition size)\n"
    "    --block_size 128k/1M       Set blockstore object size\n"
    "    --bitmap_granularity 4k    Set bitmap granularity\n"
    "    --data_csum_type none      Set data checksum type (crc32c or none)\n"
    "    --csum_block_size 4k/32k   Set data checksum block size (SSD/HDD default)\n"
    "    --data_device_block 4k     Override data device block size\n"
    "    --meta_device_block 4k     Override metadata device block size\n"
    "    --journal_device_block 4k  Override journal device block size\n"
    "    --discard_on_start 0       TRIM unused data device blocks every OSD start (default off)\n"
    "    --min_discard_size 1M      Minimum TRIM block size\n"
    "    --json                     Enable JSON output\n"
    "  \n"
    "  immediate_commit setting is automatically derived from \"disable fsync\" options.\n"
    "  It's set to \"all\" when fsync is disabled on all devices, and to \"small\" if fsync\n"
    "  is only disabled on journal device.\n"
    "  \n"
    "  When data/meta/journal fsyncs are disabled, the OSD startup script automatically\n"
    "  checks the device cache status on start and tries to disable cache for SATA/SAS disks.\n"
    "  If it doesn't succeed it issues a warning in the system log.\n"
    "  \n"
    "  You can also pass other OSD options here as arguments and they'll be persisted\n"
    "  in the superblock: data_io, meta_io, journal_io,\n"
    "  inmemory_metadata, inmemory_journal, max_write_iodepth,\n"
    "  min_flusher_count, max_flusher_count, journal_sector_buffer_count,\n"
    "  journal_no_same_sector_overwrites, throttle_small_writes, throttle_target_iops,\n"
    "  throttle_target_mbs, throttle_target_parallelism, throttle_threshold_us.\n"
    "\n"
    "vitastor-disk upgrade-simple <UNIT_FILE|OSD_NUMBER>\n"
    "  Upgrade an OSD created by old (0.7.1 and older) make-osd.sh or make-osd-hybrid.js scripts.\n"
    "  \n"
    "  Adds superblocks to OSD devices, disables old vitastor-osdN unit and replaces it with vitastor-osd@N.\n"
    "  Can be invoked with an osd number of with a path to systemd service file UNIT_FILE which\n"
    "  must be /etc/systemd/system/vitastor-osd<OSD_NUMBER>.service.\n"
    "  \n"
    "  Note that the procedure isn't atomic and may ruin OSD data in case of an interrupt,\n"
    "  so don't upgrade all your OSDs in parallel.\n"
    "  \n"
    "  Requires the `sfdisk` utility.\n"
    "\n"
    "vitastor-disk resize <osd_num>|<osd_device> [OPTIONS]\n"
    "  Resize data area and/or move journal and metadata:\n"
    "    --move-journal TARGET    move journal to TARGET\n"
    "    --move-meta TARGET       move metadata to TARGET\n"
    "    --journal-size NEW_SIZE  resize journal to NEW_SIZE\n"
    "    --data-size NEW_SIZE     resize data device to NEW_SIZE\n"
    "    --json                   enable JSON output\n"
    "    --dry-run                only show new layout, do not apply it\n"
    "  \n"
    "  NEW_SIZE may include k/m/g/t suffixes.\n"
    "  TARGET may be one of:\n"
    "    <partition>    move journal/metadata to an existing GPT partition\n"
    "    <raw_device>   create a GPT partition on <raw_device> and move journal/metadata to it\n"
    "    \"\"           (empty string) move journal/metadata back to the data device\n"
    "\n"
    "vitastor-disk raw-resize <ALL_OSD_PARAMETERS> <NEW_LAYOUT> [--iodepth 32]\n"
    "  Resize data area and/or rewrite/move journal and metadata (manual format).\n"
    "  ALL_OSD_PARAMETERS must include all (at least all disk-related)\n"
    "  parameters from OSD command line (i.e. from systemd unit or superblock).\n"
    "  NEW_LAYOUT may include new disk layout parameters:\n"
    "    --new_data_offset SIZE     resize data area so it starts at SIZE\n"
    "    --new_data_len SIZE        resize data area to SIZE bytes\n"
    "    --new_meta_device PATH     use PATH for new metadata\n"
    "    --new_meta_offset SIZE     make new metadata area start at SIZE\n"
    "    --new_meta_len SIZE        make new metadata area SIZE bytes long\n"
    "    --new_journal_device PATH  use PATH for new journal\n"
    "    --new_journal_offset SIZE  make new journal area start at SIZE\n"
    "    --new_journal_len SIZE     make new journal area SIZE bytes long\n"
    "  SIZE may include k/m/g/t suffixes. If any of the new layout parameter\n"
    "  options are not specified, old values will be used.\n"
    "\n"
    "vitastor-disk trim <osd_num>|<osd_device> [<osd_num>|<osd_device>...]\n"
    "  Try to discard unused blocks (SSD TRIM) on the data device of each of the OSD(s).\n"
    "  May only be used on stopped OSDs. Options:\n"
    "    --min_discard_size 1M      Minimum TRIM block size\n"
    "    --discard_granularity 0    Override device's discard granularity\n"
    "\n"
    "vitastor-disk start|stop|restart|enable|disable [--now] <device> [device2 device3 ...]\n"
    "  Manipulate Vitastor OSDs using systemd by their device paths.\n"
    "  Commands are passed to systemctl with vitastor-osd@<num> units as arguments.\n"
    "  When --now is added to enable/disable, OSDs are also immediately started/stopped.\n"
    "\n"
    "vitastor-disk purge [--json] [--force] [--allow-data-loss] <device> [device2 device3 ...]\n"
    "  Purge Vitastor OSD(s) on specified device(s). Uses vitastor-cli rm-osd to check\n"
    "  if deletion is possible without data loss and to actually remove metadata from etcd.\n"
    "  --force and --allow-data-loss options may be used to ignore safety check results.\n"
    "  --json enables JSON output.\n"
    "  \n"
    "  Requires `vitastor-cli`, `sfdisk` and `partprobe` (from parted) utilities.\n"
    "\n"
    "vitastor-disk read-sb [--force] <device>\n"
    "  Try to read Vitastor OSD superblock from <device> and print it in JSON format.\n"
    "  --force allows to ignore validation errors.\n"
    "\n"
    "vitastor-disk write-sb <device>\n"
    "  Read JSON from STDIN and write it into Vitastor OSD superblock on <device>.\n"
    "\n"
    "vitastor-disk update-sb <device> [--force] [--<parameter> <value>] [...]\n"
    "  Read Vitastor OSD superblock from <device>, update parameters in it and write it back.\n"
    "  --force allows to ignore validation errors.\n"
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
    "vitastor-disk dump-journal [OPTIONS] <osd_device>\n"
    "vitastor-disk dump-journal [OPTIONS] <journal_file> <journal_block_size> <offset> <size>\n"
    "  Dump journal in text or JSON (if --json is specified) format.\n"
    "  You can specify any OSD device (data, metadata or journal), or the layout manually.\n"
    "  Options:\n"
    "  --all             Scan the whole journal area for entries and dump them, even outdated ones\n"
    "  --json            Dump journal in JSON format\n"
    "  --format entries  (Default) Dump actual journal entries as an array, without data\n"
    "  --format data     Same as \"entries\", but also include small write data\n"
    "  --format blocks   Dump as an array of journal blocks each containing array of entries\n"
    "\n"
    "vitastor-disk write-journal <osd_device>\n"
    "vitastor-disk write-journal <journal_file> <journal_block_size> <bitmap_size> <offset> <size>\n"
    "  Write journal from JSON taken from standard input in the same format as produced by\n"
    "  `dump-journal --json --format data`.\n"
    "  You can specify any OSD device (data, metadata or journal), or the layout manually.\n"
    "\n"
    "vitastor-disk dump-meta <osd_device>\n"
    "vitastor-disk dump-meta <meta_file> <meta_block_size> <offset> <size>\n"
    "  Dump metadata in JSON format.\n"
    "  You can specify any OSD device (data, metadata or journal), or the layout manually.\n"
    "\n"
    "vitastor-disk write-meta <osd_device>\n"
    "vitastor-disk write-meta <meta_file> <offset> <size>\n"
    "  Write metadata from JSON taken from standard input in the same format as produced by `dump-meta`.\n"
    "  You can specify any OSD device (data, metadata or journal), or the layout manually.\n"
    "\n"
    "vitastor-disk simple-offsets <device>\n"
    "  Calculate offsets for old simple&stupid (no superblock) OSD deployment. Options:\n"
    "    --object_size 128k       Set blockstore block size\n"
    "    --bitmap_granularity 4k  Set bitmap granularity\n"
    "    --journal_size 32M       Set journal size\n"
    "    --data_csum_type none    Set data checksum type (crc32c or none)\n"
    "    --csum_block_size 4k     Set data checksum block size\n"
    "    --device_block_size 4k   Set device block size\n"
    "    --journal_offset 0       Set journal offset\n"
    "    --device_size 0          Set device size\n"
    "    --format text            Result format: json, options, env, or text\n"
    "\n"
    "Default I/O mode for commands involving disk I/O is O_DIRECT. If you don't want it, add --io cached.\n"
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
    else if (!strcmp(exe_name, "vitastor-disk-test"))
    {
        self.test_mode = true;
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
        else if (!strcmp(argv[i], "--hdd"))
        {
            self.options["hdd"] = "1";
        }
        else if (!strcmp(argv[i], "--help") || !strcmp(argv[i], "-h"))
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
        else if (!strcmp(argv[i], "--dry-run") || !strcmp(argv[i], "--dry_run"))
        {
            self.options["dry_run"] = "1";
        }
        else if (!strcmp(argv[i], "--allow-data-loss"))
        {
            self.options["allow_data_loss"] = "1";
        }
        else if (argv[i][0] == '-' && argv[i][1] == '-' && i < argc-1)
        {
            char *key = argv[i]+2;
            self.options[str_replace(key, "-", "_")] = argv[++i];
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
        if (cmd.size() != 2 && cmd.size() < 5)
        {
            print_help(help_text, aliased ? "vitastor-dump-journal" : "vitastor-disk", cmd[0], false);
            return 1;
        }
        self.dsk.journal_device = cmd[1];
        if (cmd.size() > 2)
        {
            self.dsk.journal_block_size = strtoul(cmd[2], NULL, 10);
            self.dsk.journal_offset = strtoull(cmd[3], NULL, 10);
            self.dsk.journal_len = strtoull(cmd[4], NULL, 10);
        }
        else
        {
            // First argument is an OSD device - take metadata layout parameters from it
            if (self.dump_load_check_superblock(self.dsk.journal_device))
                return 1;
        }
        return self.dump_journal();
    }
    else if (!strcmp(cmd[0], "write-journal"))
    {
        if (cmd.size() != 2 && cmd.size() < 6)
        {
            print_help(help_text, "vitastor-disk", cmd[0], false);
            return 1;
        }
        self.new_journal_device = cmd[1];
        if (cmd.size() > 2)
        {
            self.dsk.journal_block_size = strtoul(cmd[2], NULL, 10);
            self.dsk.clean_entry_bitmap_size = strtoul(cmd[3], NULL, 10);
            self.new_journal_offset = strtoull(cmd[4], NULL, 10);
            self.new_journal_len = strtoull(cmd[5], NULL, 10);
        }
        else
        {
            // First argument is an OSD device - take metadata layout parameters from it
            if (self.dump_load_check_superblock(self.new_journal_device))
                return 1;
            self.new_journal_device = self.dsk.journal_device;
            self.new_journal_offset = self.dsk.journal_offset;
            self.new_journal_len = self.dsk.journal_len;
        }
        std::string json_err;
        json11::Json entries = json11::Json::parse(read_all_fd(0), json_err);
        if (json_err != "")
        {
            fprintf(stderr, "Invalid JSON: %s\n", json_err.c_str());
            return 1;
        }
        if (entries[0]["type"] == "start")
        {
            self.dsk.data_csum_type = csum_type_from_str(entries[0]["data_csum_type"].string_value());
            self.dsk.csum_block_size = entries[0]["csum_block_size"].uint64_value();
        }
        if (self.options["data_csum_type"] != "")
        {
            self.dsk.data_csum_type = csum_type_from_str(self.options["data_csum_type"]);
        }
        if (self.options["csum_block_size"] != "")
        {
            self.dsk.csum_block_size = stoull_full(self.options["csum_block_size"], 0);
        }
        return self.write_json_journal(entries);
    }
    else if (!strcmp(cmd[0], "dump-meta"))
    {
        if (cmd.size() != 2 && cmd.size() < 5)
        {
            print_help(help_text, "vitastor-disk", cmd[0], false);
            return 1;
        }
        self.dsk.meta_device = cmd[1];
        if (cmd.size() > 2)
        {
            self.dsk.meta_block_size = strtoul(cmd[2], NULL, 10);
            self.dsk.meta_offset = strtoull(cmd[3], NULL, 10);
            self.dsk.meta_len = strtoull(cmd[4], NULL, 10);
        }
        else
        {
            // First argument is an OSD device - take metadata layout parameters from it
            if (self.dump_load_check_superblock(self.dsk.meta_device))
                return 1;
        }
        return self.dump_meta();
    }
    else if (!strcmp(cmd[0], "write-meta"))
    {
        if (cmd.size() != 2 && cmd.size() < 4)
        {
            print_help(help_text, "vitastor-disk", cmd[0], false);
            return 1;
        }
        self.new_meta_device = cmd[1];
        if (cmd.size() > 2)
        {
            self.new_meta_offset = strtoull(cmd[2], NULL, 10);
            self.new_meta_len = strtoull(cmd[3], NULL, 10);
        }
        else
        {
            // First argument is an OSD device - take metadata layout parameters from it
            if (self.dump_load_check_superblock(self.new_meta_device))
                return 1;
            self.new_meta_device = self.dsk.meta_device;
            self.new_meta_offset = self.dsk.meta_offset;
            self.new_meta_len = self.dsk.meta_len;
        }
        std::string json_err;
        json11::Json meta = json11::Json::parse(read_all_fd(0), json_err);
        if (json_err != "")
        {
            fprintf(stderr, "Invalid JSON: %s\n", json_err.c_str());
            return 1;
        }
        return self.write_json_meta(meta);
    }
    else if (!strcmp(cmd[0], "resize"))
    {
        if (cmd.size() != 2)
        {
            fprintf(stderr, "Exactly 1 OSD number or OSD device path argument is required\n");
            return 1;
        }
        return self.resize_data(cmd[1]);
    }
    else if (!strcmp(cmd[0], "raw-resize"))
    {
        return self.raw_resize();
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
    else if (!strcmp(cmd[0], "trim"))
    {
        if (cmd.size() < 2)
        {
            fprintf(stderr, "OSD number(s) or device path(s) are required\n");
            return 1;
        }
        for (int i = 1; i < cmd.size(); i++)
        {
            self.trim_data(cmd[i]);
        }
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
    else if (!strcmp(cmd[0], "update-sb"))
    {
        if (cmd.size() != 2)
        {
            fprintf(stderr, "Exactly 1 device path argument is required\n");
            return 1;
        }
        return self.update_sb(cmd[1]);
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
    else if (!strcmp(cmd[0], "purge"))
    {
        return self.purge_devices(std::vector<std::string>(cmd.begin()+1, cmd.end()));
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
    else if (!strcmp(cmd[0], "prepare"))
    {
        std::vector<std::string> devs;
        for (int i = 1; i < cmd.size(); i++)
        {
            devs.push_back(cmd[i]);
        }
        return self.prepare(devs);
    }
    else if (!strcmp(cmd[0], "upgrade-simple"))
    {
        if (cmd.size() != 2)
        {
            fprintf(stderr, "Exactly 1 OSD number or systemd unit path is required\n");
            return 1;
        }
        return self.upgrade_simple_unit(cmd[1]);
    }
    else
    {
        print_help(help_text, "vitastor-disk", cmd.size() > 1 ? cmd[1] : "", self.all);
    }
    return 0;
}
