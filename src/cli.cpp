// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

/**
 * CLI tool and also a library for administrative tasks
 */

#include <vector>
#include <algorithm>

#include "cli.h"
#include "epoll_manager.h"
#include "cluster_client.h"
#include "pg_states.h"
#include "str_util.h"

static const char *exe_name = NULL;

static const char* help_text =
    "Vitastor command-line tool " VERSION "\n"
    "(c) Vitaliy Filippov, 2019+ (VNPL-1.1)\n"
    "\n"
    "COMMANDS:\n"
    "\n"
    "vitastor-cli status\n"
    "  Show cluster status\n"
    "\n"
    "vitastor-cli df\n"
    "  Show pool space statistics\n"
    "\n"
    "vitastor-cli ls [-l] [-p POOL] [--sort FIELD] [-r] [-n N] [<glob> ...]\n"
    "  List images (only matching <glob> patterns if passed).\n"
    "  -p|--pool POOL  Filter images by pool ID or name\n"
    "  -l|--long       Also report allocated size and I/O statistics\n"
    "  --del           Also include delete operation statistics\n"
    "  --sort FIELD    Sort by specified field (name, size, used_size, <read|write|delete>_<iops|bps|lat|queue>)\n"
    "  -r|--reverse    Sort in descending order\n"
    "  -n|--count N    Only list first N items\n"
    "\n"
    "vitastor-cli create -s|--size <size> [-p|--pool <id|name>] [--parent <parent_name>[@<snapshot>]] <name>\n"
    "  Create an image. You may use K/M/G/T suffixes for <size>. If --parent is specified,\n"
    "  a copy-on-write image clone is created. Parent must be a snapshot (readonly image).\n"
    "  Pool must be specified if there is more than one pool.\n"
    "\n"
    "vitastor-cli create --snapshot <snapshot> [-p|--pool <id|name>] <image>\n"
    "vitastor-cli snap-create [-p|--pool <id|name>] <image>@<snapshot>\n"
    "  Create a snapshot of image <name>. May be used live if only a single writer is active.\n"
    "\n"
    "vitastor-cli modify <name> [--rename <new-name>] [--resize <size>] [--readonly | --readwrite] [-f|--force]\n"
    "  Rename, resize image or change its readonly status. Images with children can't be made read-write.\n"
    "  If the new size is smaller than the old size, extra data will be purged.\n"
    "  You should resize file system in the image, if present, before shrinking it.\n"
    "  -f|--force  Proceed with shrinking or setting readwrite flag even if the image has children.\n"
    "\n"
    "vitastor-cli rm <from> [<to>] [--writers-stopped]\n"
    "  Remove <from> or all layers between <from> and <to> (<to> must be a child of <from>),\n"
    "  rebasing all their children accordingly. --writers-stopped allows merging to be a bit\n"
    "  more effective in case of a single 'slim' read-write child and 'fat' removed parent:\n"
    "  the child is merged into parent and parent is renamed to child in that case.\n"
    "  In other cases parent layers are always merged into children.\n"
    "\n"
    "vitastor-cli flatten <layer>\n"
    "  Flatten a layer, i.e. merge data and detach it from parents.\n"
    "\n"
    "vitastor-cli rm-data --pool <pool> --inode <inode> [--wait-list] [--min-offset <offset>]\n"
    "  Remove inode data without changing metadata.\n"
    "  --wait-list   Retrieve full objects listings before starting to remove objects.\n"
    "                Requires more memory, but allows to show correct removal progress.\n"
    "  --min-offset  Purge only data starting with specified offset.\n"
    "\n"
    "vitastor-cli merge-data <from> <to> [--target <target>]\n"
    "  Merge layer data without changing metadata. Merge <from>..<to> to <target>.\n"
    "  <to> must be a child of <from> and <target> may be one of the layers between\n"
    "  <from> and <to>, including <from> and <to>.\n"
    "\n"
    "vitastor-cli describe [--osds <osds>] [--object-state <states>] [--pool <pool>] [--inode <ino>] [--min-inode <ino>] [--max-inode <ino>] [--min-offset <offset>] [--max-offset <offset>]\n"
    "  Describe unclean object locations in the cluster.\n"
    "  --osds <osds>\n"
    "      Only list objects from primary OSD(s) <osds>.\n"
    "  --object-state <states>\n"
    "      Only list objects in given state(s). State(s) may include:\n"
    "      degraded, misplaced, incomplete, corrupted, inconsistent.\n"
    "  --pool <pool name or number>\n"
    "      Only list objects in the given pool.\n"
    "  --inode, --min-inode, --max-inode\n"
    "      Restrict listing to specific inode numbers.\n"
    "  --min-offset, --max-offset\n"
    "      Restrict listing to specific offsets inside inodes.\n"
    "\n"
    "vitastor-cli fix [--objects <objects>] [--bad-osds <osds>] [--part <part>] [--check no]\n"
    "  Fix inconsistent objects in the cluster by deleting some copies.\n"
    "  --objects <objects>\n"
    "      Objects to fix, either in plain text or JSON format. If not specified,\n"
    "      object list will be read from STDIN in one of the same formats.\n"
    "      Plain text format: 0x<inode>:0x<stripe> <any delimiter> 0x<inode>:0x<stripe> ...\n"
    "      JSON format: [{\"inode\":\"0x...\",\"stripe\":\"0x...\"},...]\n"
    "  --bad-osds <osds>\n"
    "      Remove inconsistent copies/parts of objects from these OSDs, effectively\n"
    "      marking them bad and allowing Vitastor to recover objects from other copies.\n"
    "  --part <number>\n"
    "      Only remove EC part <number> (from 0 to pg_size-1), required for extreme\n"
    "      edge cases where one OSD has multiple parts of a EC object.\n"
    "  --check no\n"
    "      Do not recheck that requested objects are actually inconsistent,\n"
    "      delete requested copies/parts anyway.\n"
    "\n"
    "vitastor-cli alloc-osd\n"
    "  Allocate a new OSD number and reserve it by creating empty /osd/stats/<n> key.\n"
    "\n"
    "vitastor-cli rm-osd [--force] [--allow-data-loss] [--dry-run] <osd_id> [osd_id...]\n"
    "  Remove metadata and configuration for specified OSD(s) from etcd.\n"
    "  Refuses to remove OSDs with data without --force and --allow-data-loss.\n"
    "  With --dry-run only checks if deletion is possible without data loss and\n"
    "  redundancy degradation.\n"
    "\n"
    "vitastor-cli create-pool <name> --scheme <scheme> -s <pg_size> --pg_minsize <pg_minsize> -n <pg_count> --parity_chunks <number> [OPTIONS]\n"
    "   Create a pool.\n"
    "  --scheme <scheme>\n"
    "       Redundancy scheme used for data in this pool. One of: \"replicated\", \"xor\", \"ec\" or \"jerasure\".\n"
    "       It's \"replicated\" by default.\n"
    "  --ec <N>+<K>\n"
    "       Shortcut for 'ec' scheme. scheme = ec, pg_size = N+K, parity_chunks = K.\n"
    "  -s|--pg_size <size>\n"
    "       Total number of disks for PGs of this pool - i.e., number of replicas for replicated pools and number of data plus parity disks for EC/XOR pools.\n"
    "  --pg_minsize <size>\n"
    "       Number of available live OSDs for PGs of this pool to remain active.\n"
    "  -n|--pg_count <count>\n"
    "       Number of PGs for this pool.\n"
    "  --parity_chunks <number>\n"
    "       Number of parity chunks for EC/XOR pools\n"
    "  -f|--force\n"
    "       Proceed without checking pool/OSD params (pg_size, block_size, bitmap_granularity, and immediate_commit).\n"
    "  --failure_domain <failure_domain>\n"
    "       Failure domain specification. Must be \"host\" or \"osd\" or refer to one of the placement tree levels, defined in placement_levels.\n"
    "  --max_osd_combinations <number>\n"
    "       This parameter specifies the maximum number of combinations to generate when optimising PG placement.\n"
    "  --block_size <size>\n"
    "       Block size for this pool.\n"
    "  --bitmap_granularity <granularity>\n"
    "       \"Sector\" size of virtual disks in this pool.\n"
    "  --immediate_commit <all|small|none>\n"
    "       Immediate commit setting for this pool. One of \"all\", \"small\" and \"none\".\n"
    "  --pg_stripe_size <size>\n"
    "       Specifies the stripe size for this pool according to which images are split into different PGs.\n"
    "  --root_node <node>\n"
    "       Specifies the root node of the OSD tree to restrict this pool OSDs to.\n"
    "  --osd_tags <tags>\n"
    "       Specifies OSD tags to restrict this pool to.\n"
    "       Example: --osd_tags tag0 or --osd_tags tag0,tag1\n"
    "  --primary_affinity_tags <tags>\n"
    "       Specifies OSD tags to prefer putting primary OSDs in this pool to.\n"
    "       Example: --primary_affinity_tags tag0 or --primary_affinity_tags tag0,tag1\n"
    "  --scrub_interval <time_interval>\n"
    "       Automatic scrubbing interval for this pool. Format: number + unit s/m/h/d/M/y.\n"
    "  Examples:\n"
    "       vitastor-cli create-pool test_x4 -s 4 -n 32\n"
    "       vitastor-cli create-pool test_ec42 --ec 4+2 -n 32\n"
    "\n"
    "vitastor-cli modify-pool <id|name> [--name <new_name>] [-s <pg_size>] [--pg_minsize <pg_minsize>] [-n <pg_count>] [OPTIONS]\n"
    "   Modify an existing pool.\n"
    "  --name <new_name>\n"
    "       Change name of this pool.\n"
    "  -s|--pg_size <size>\n"
    "       Total number of disks for PGs of this pool - i.e., number of replicas for replicated pools and number of data plus parity disks for EC/XOR pools.\n"
    "  --pg_minsize <size>\n"
    "       Number of available live OSDs for PGs of this pool to remain active.\n"
    "  -n|--pg_count <count>\n"
    "       Number of PGs for this pool.\n"
    "  -f|--force\n"
    "       Proceed without checking pool/OSD params (block_size, bitmap_granularity and immediate_commit).\n"
    "  --failure_domain <failure_domain>\n"
    "       Failure domain specification. Must be \"host\" or \"osd\" or refer to one of the placement tree levels, defined in placement_levels.\n"
    "  --max_osd_combinations <number>\n"
    "       This parameter specifies the maximum number of combinations to generate when optimising PG placement.\n"
    "  --block_size <size>\n"
    "       Block size for this pool.\n"
    "  --immediate_commit <all|small|none>\n"
    "       Immediate commit setting for this pool. One of \"all\", \"small\" and \"none\".\n"
    "  --pg_stripe_size <size>\n"
    "       Specifies the stripe size for this pool according to which images are split into different PGs.\n"
    "  --root_node <node>\n"
    "       Specifies the root node of the OSD tree to restrict this pool OSDs to.\n"
    "  --osd_tags <tags>\n"
    "       Specifies OSD tags to restrict this pool to.\n"
    "       Example: --osd_tags tag0 or --osd_tags tag0,tag1\n"
    "  --primary_affinity_tags <tags>\n"
    "       Specifies OSD tags to prefer putting primary OSDs in this pool to.\n"
    "       Example: --primary_affinity_tags tag0 or --primary_affinity_tags tag0,tag1\n"
    "  --scrub_interval <time_interval>\n"
    "       Automatic scrubbing interval for this pool. Format: number + unit s/m/h/d/M/y.\n"
    "  Examples:\n"
    "       vitastor-cli modify-pool pool_A -name pool_B\n"
    "       vitastor-cli modify-pool 2 -s 4 -n 128 --block_size 262144\n"
    "\n"
    "vitastor-cli rm-pool [--force] <id|name>\n"
    "  Remove existing pool from cluster.\n"
    "  Refuses to remove pools with related Image and/or Snapshot data without --force.\n"
    "  Examples:\n"
    "       vitastor-cli rm-pool test_pool\n"
    "       vitastor-cli rm-pool --force 2\n"
    "\n"
    "vitastor-cli ls-pool [-l] [-p POOL] [--sort FIELD] [-r] [-n N] [--stats] [<glob> ...]\n"
    "  List pool (only matching <glob> patterns if passed).\n"
    "  -p|--pool POOL  Show in detail pool ID or name\n"
    "  -l|--long       Show all available field\n"
    "  --sort FIELD    Sort by specified field (id, name, pg_count, scheme_name, used_byte, total, max_available, used_pct, space_efficiency, status, restore, root_node, failure_domain, osd_tags, primary_affinity_tags)\n"
    "  -r|--reverse    Sort in descending order\n"
    "  -n|--count N    Only list first N items\n"
    "  --stats         Performance statistics\n"
    "  Examples:\n"
    "       vitastor-cli ls-pool -l\n"
    "       vitastor-cli ls-pool -l --sort pool_name\n"
    "       vitastor-cli ls-pool -p 2\n"
    "\n"
    "Use vitastor-cli --help <command> for command details or vitastor-cli --help --all for all details.\n"
    "\n"
    "GLOBAL OPTIONS:\n"
    "  --config_file FILE  Path to Vitastor configuration file\n"
    "  --etcd_address URL  Etcd connection address\n"
    "  --iodepth N         Send N operations in parallel to each OSD when possible (default 32)\n"
    "  --parallel_osds M   Work with M osds in parallel when possible (default 4)\n"
    "  --progress 1|0      Report progress (default 1)\n"
    "  --cas 1|0           Use CAS writes for flatten, merge, rm (default is decide automatically)\n"
    "  --no-color          Disable colored output\n"
    "  --json              JSON output\n"
;

static json11::Json::object parse_args(int narg, const char *args[])
{
    json11::Json::object cfg;
    json11::Json::array cmd;
    cfg["progress"] = "1";
    for (int i = 1; i < narg; i++)
    {
        bool argHasValue = (!(i == narg-1) && (args[i+1][0] != '-'));

        if (args[i][0] == '-' && args[i][1] == 'h' && args[i][2] == 0)
        {
            cfg["help"] = "1";
        }
        else if (args[i][0] == '-' && args[i][1] == 'l' && args[i][2] == 0)
        {
            cfg["long"] = "1";
        }
        else if (args[i][0] == '-' && args[i][1] == 'n' && args[i][2] == 0)
        {
            cfg["count"] = argHasValue ? args[++i] : "";
        }
        else if (args[i][0] == '-' && args[i][1] == 'p' && args[i][2] == 0)
        {
            cfg["pool"] = argHasValue ? args[++i] : "";
        }
        else if (args[i][0] == '-' && args[i][1] == 's' && args[i][2] == 0)
        {
            cfg["size"] = argHasValue ? args[++i] : "";
        }
        else if (args[i][0] == '-' && args[i][1] == 'r' && args[i][2] == 0)
        {
            cfg["reverse"] = "1";
        }
        else if (args[i][0] == '-' && args[i][1] == 'f' && args[i][2] == 0)
        {
            cfg["force"] = "1";
        }
        else if (args[i][0] == '-' && args[i][1] == '-')
        {
            const char *opt = args[i]+2;
            if (!strcmp(opt, "json") ||
                !strcmp(opt, "wait-list") || !strcmp(opt, "wait_list") ||
                !strcmp(opt, "long") || !strcmp(opt, "del") ||
                !strcmp(opt, "no-color") || !strcmp(opt, "no_color") ||
                !strcmp(opt, "readonly") || !strcmp(opt, "readwrite") ||
                !strcmp(opt, "force") || !strcmp(opt, "reverse") ||
                !strcmp(opt, "allow-data-loss") || !strcmp(opt, "allow_data_loss") ||
                !strcmp(opt, "dry-run") || !strcmp(opt, "dry_run") ||
                !strcmp(opt, "help") || !strcmp(opt, "all") || !strcmp(opt, "stats") ||
                !strcmp(opt, "writers-stopped") || !strcmp(opt, "writers_stopped"))
            {
                cfg[opt] = "1";
            }
            else
            {
                cfg[opt] = argHasValue ? args[++i] : "";
            }
        }
        else
        {
            cmd.push_back(std::string(args[i]));
        }
    }
    if (!cmd.size())
    {
        std::string exe(exe_name);
        if (exe.size() >= 11 && exe.substr(exe.size()-11) == "vitastor-rm")
        {
            cmd.push_back("rm-data");
        }
    }
    if (!cmd.size() || cfg["help"].bool_value())
    {
        print_help(help_text, "vitastor-cli", cmd.size() ? cmd[0].string_value() : "", cfg["all"].bool_value());
    }
    cfg["command"] = cmd;
    return cfg;
}

static int run(cli_tool_t *p, json11::Json::object cfg)
{
    cli_result_t result = {};
    p->is_command_line = true;
    p->parse_config(cfg);
    json11::Json::array cmd = cfg["command"].array_items();
    cfg.erase("command");
    std::function<bool(cli_result_t &)> action_cb;
    if (!cmd.size())
    {
        result = { .err = EINVAL, .text = "command is missing" };
    }
    else if (cmd[0] == "status")
    {
        // Show cluster status
        action_cb = p->start_status(cfg);
    }
    else if (cmd[0] == "df")
    {
        // Show pool space stats
        cfg["dfformat"] = "1";
        action_cb = p->start_pool_ls(cfg);
    }
    else if (cmd[0] == "ls")
    {
        // List images
        if (cmd.size() > 1)
        {
            cmd.erase(cmd.begin(), cmd.begin()+1);
            cfg["names"] = cmd;
        }
        action_cb = p->start_ls(cfg);
    }
    else if (cmd[0] == "snap-create")
    {
        // Create snapshot
        std::string name = cmd.size() > 1 ? cmd[1].string_value() : "";
        int pos = name.find('@');
        if (pos == std::string::npos || pos == name.length()-1)
        {
            result = (cli_result_t){ .err = EINVAL, .text = "Please specify new snapshot name after @" };
        }
        else
        {
            cfg["image"] = name.substr(0, pos);
            cfg["snapshot"] = name.substr(pos + 1);
            action_cb = p->start_create(cfg);
        }
    }
    else if (cmd[0] == "create")
    {
        // Create image/snapshot
        if (cmd.size() > 1)
        {
            cfg["image"] = cmd[1];
        }
        action_cb = p->start_create(cfg);
    }
    else if (cmd[0] == "modify")
    {
        // Modify image
        if (cmd.size() > 1)
        {
            cfg["image"] = cmd[1];
        }
        action_cb = p->start_modify(cfg);
    }
    else if (cmd[0] == "rm-data")
    {
        // Delete inode data
        action_cb = p->start_rm_data(cfg);
    }
    else if (cmd[0] == "rm-osd")
    {
        // Delete OSD metadata from etcd
        if (cmd.size() > 1)
        {
            cmd.erase(cmd.begin(), cmd.begin()+1);
            cfg["osd_id"] = cmd;
        }
        action_cb = p->start_rm_osd(cfg);
    }
    else if (cmd[0] == "merge-data")
    {
        // Merge layer data without affecting metadata
        if (cmd.size() > 1)
        {
            cfg["from"] = cmd[1];
            if (cmd.size() > 2)
                cfg["to"] = cmd[2];
        }
        action_cb = p->start_merge(cfg);
    }
    else if (cmd[0] == "flatten")
    {
        // Merge layer data without affecting metadata
        if (cmd.size() > 1)
        {
            cfg["image"] = cmd[1];
        }
        action_cb = p->start_flatten(cfg);
    }
    else if (cmd[0] == "rm")
    {
        // Remove multiple snapshots and rebase their children
        if (cmd.size() > 1)
        {
            cfg["from"] = cmd[1];
            if (cmd.size() > 2)
                cfg["to"] = cmd[2];
        }
        action_cb = p->start_rm(cfg);
    }
    else if (cmd[0] == "describe")
    {
        // Describe unclean objects
        action_cb = p->start_describe(cfg);
    }
    else if (cmd[0] == "fix")
    {
        // Fix inconsistent objects (by deleting some copies)
        action_cb = p->start_fix(cfg);
    }
    else if (cmd[0] == "alloc-osd")
    {
        // Allocate a new OSD number
        action_cb = p->start_alloc_osd(cfg);
    }
    else if (cmd[0] == "create-pool")
    {
        // Create a new pool
        if (cmd.size() > 1 && cfg["name"].is_null())
        {
            cfg["name"] = cmd[1];
        }
        action_cb = p->start_pool_create(cfg);
    }
    else if (cmd[0] == "modify-pool")
    {
        // Modify existing pool
        if (cmd.size() > 1)
        {
            cfg["pool"] = cmd[1];
        }
        action_cb = p->start_pool_modify(cfg);
    }
    else if (cmd[0] == "rm-pool")
    {
        // Remove existing pool
        if (cmd.size() > 1)
        {
            cfg["pool"] = cmd[1];
        }
        action_cb = p->start_pool_rm(cfg);
    }
    else if (cmd[0] == "ls-pool")
    {
        // Show pool list
        if (cmd.size() > 1)
        {
            cmd.erase(cmd.begin(), cmd.begin()+1);
            cfg["names"] = cmd;
        }
        action_cb = p->start_pool_ls(cfg);
    }
    else
    {
        result = { .err = EINVAL, .text = "unknown command: "+cmd[0].string_value() };
    }
    if (action_cb != NULL)
    {
        // Create client
        json11::Json cfg_j = cfg;
        p->ringloop = new ring_loop_t(RINGLOOP_DEFAULT_SIZE);
        p->epmgr = new epoll_manager_t(p->ringloop);
        p->cli = new cluster_client_t(p->ringloop, p->epmgr->tfd, cfg_j);
        // Smaller timeout by default for more interactiveness
        p->cli->st_cli.etcd_slow_timeout = p->cli->st_cli.etcd_quick_timeout;
        p->loop_and_wait(action_cb, [&](const cli_result_t & r)
        {
            result = r;
            action_cb = NULL;
        });
        // Loop until it completes
        while (action_cb != NULL)
        {
            p->ringloop->loop();
            if (action_cb != NULL)
                p->ringloop->wait();
        }
        // Destroy the client
        p->cli->flush();
        delete p->cli;
        delete p->epmgr;
        delete p->ringloop;
        p->cli = NULL;
        p->epmgr = NULL;
        p->ringloop = NULL;
    }
    // Print result
    fflush(stderr);
    fflush(stdout);
    if (p->json_output && !result.data.is_null())
    {
        printf("%s\n", result.data.dump().c_str());
    }
    else if (p->json_output && result.err)
    {
        printf("%s\n", json11::Json(json11::Json::object {
            { "error_code", result.err },
            { "error_text", result.text },
        }).dump().c_str());
    }
    else if (result.text != "")
    {
        fprintf(result.err ? stderr : stdout, result.text[result.text.size()-1] == '\n' ? "%s" : "%s\n", result.text.c_str());
    }
    return result.err;
}

int main(int narg, const char *args[])
{
    setvbuf(stdout, NULL, _IONBF, 0);
    setvbuf(stderr, NULL, _IONBF, 0);
    exe_name = args[0];
    cli_tool_t *p = new cli_tool_t();
    int r = run(p, parse_args(narg, args));
    delete p;
    return r;
}
