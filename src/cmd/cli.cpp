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
    "vitastor-cli modify <name> [--rename <new-name>] [--resize <size>] [--readonly | --readwrite] [-f|--force] [--down-ok]\n"
    "  Rename, resize image or change its readonly status. Images with children can't be made read-write.\n"
    "  If the new size is smaller than the old size, extra data will be purged.\n"
    "  You should resize file system in the image, if present, before shrinking it.\n"
    "  -f|--force  Proceed with shrinking or setting readwrite flag even if the image has children.\n"
    "  --down-ok   Proceed with shrinking even if some data will be left on unavailable OSDs.\n"
    "\n"
    "vitastor-cli rm <from> [<to>] [--writers-stopped] [--down-ok]\n"
    "  Remove <from> or all layers between <from> and <to> (<to> must be a child of <from>),\n"
    "  rebasing all their children accordingly. --writers-stopped allows merging to be a bit\n"
    "  more effective in case of a single 'slim' read-write child and 'fat' removed parent:\n"
    "  the child is merged into parent and parent is renamed to child in that case.\n"
    "  In other cases parent layers are always merged into children.\n"
    "  Other options:\n"
    "  --down-ok  Continue deletion/merging even if some data will be left on unavailable OSDs.\n"
    "\n"
    "vitastor-cli flatten <layer>\n"
    "  Flatten a layer, i.e. merge data and detach it from parents.\n"
    "\n"
    "vitastor-cli rm-data --pool <pool> --inode <inode> [--wait-list] [--min-offset <offset>]\n"
    "  Remove inode data without changing metadata.\n"
    "  --wait-list   Retrieve full objects listings before starting to remove objects.\n"
    "                Requires more memory, but allows to show correct removal progress.\n"
    "  --min-offset  Purge only data starting with specified offset.\n"
    "  --max-offset  Purge only data before specified offset.\n"
    "\n"
    "vitastor-cli merge-data <from> <to> [--target <target>]\n"
    "  Merge layer data without changing metadata. Merge <from>..<to> to <target>.\n"
    "  <to> must be a child of <from> and <target> may be one of the layers between\n"
    "  <from> and <to>, including <from> and <to>.\n"
    "\n"
    "vitastor-cli describe [OPTIONS]\n"
    "  Describe unclean object locations in the cluster. Options:\n"
    "  --osds <osds>\n"
    "      Only list objects from primary OSD(s) <osds>.\n"
    "  --object-state <states>\n"
    "      Only list objects in given state(s). State(s) may include:\n"
    "      degraded, misplaced, incomplete, corrupted, inconsistent.\n"
    "  --pool <pool name or number>\n"
    "      Only list objects in the given pool.\n"
    "  --pg <pg number>\n"
    "      Only list objects in the given PG of the pool.\n"
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
    "vitastor-cli osd-tree [-l|--long]\n"
    "  Show current OSD tree, optionally with I/O statistics if -l is specified.\n"
    "\n"
    "vitastor-cli osds|ls-osd|osd-ls [-l|--long]\n"
    "  Show current OSDs as list, optionally with I/O statistics if -l is specified.\n"
    "\n"
    "vitastor-cli modify-osd [--tags tag1,tag2,...] [--reweight <number>] [--noout true/false] <osd_number>\n"
    "  Set OSD reweight, tags or noout flag.\n"
    "\n"
    "vitastor-cli pg-list|pg-ls|list-pg|ls-pg|ls-pgs [OPTIONS] [state1+state2] [^state3] [...]\n"
    "  List PGs with any of listed state filters (^ or ! in the beginning is negation). Options:\n"
    "    --pool <pool name or number>  Only list PGs of the given pool.\n"
    "    --min <min pg number>         Only list PGs with number >= min.\n"
    "    --max <max pg number>         Only list PGs with number <= max.\n"
    "  Examples:\n"
    "    vitastor-cli pg-list active+degraded\n"
    "    vitastor-cli pg-list ^active\n"
    "\n"
    "vitastor-cli create-pool|pool-create <name> (-s <pg_size>|--ec <N>+<K>) -n <pg_count> [OPTIONS]\n"
    "  Create a pool. Required parameters:\n"
    "    -s|--pg_size R   Number of replicas for replicated pools\n"
    "    --ec N+K         Number of data (N) and parity (K) chunks for erasure-coded pools\n"
    "    -n|--pg_count N  PG count for the new pool (start with 10*<OSD count>/pg_size rounded to a power of 2)\n"
    "  Optional parameters:\n"
    "    --pg_minsize <number>         R or N+K minus number of failures to tolerate without downtime\n"
    "    --failure_domain host         Failure domain: host, osd or a level from placement_levels. Default: host\n"
    "    --root_node <node>            Put pool only on child OSDs of this placement tree node\n"
    "    --osd_tags <tag>[,<tag>]...   Put pool only on OSDs tagged with all specified tags\n"
    "    --block_size 128k             Put pool only on OSDs with this data block size\n"
    "    --bitmap_granularity 4k       Put pool only on OSDs with this logical sector size\n"
    "    --immediate_commit all        Put pool only on OSDs with this or larger immediate_commit (none < small < all)\n"
    "    --level_placement <rules>     Use additional failure domain rules (example: \"dc=112233\")\n"
    "    --raw_placement <rules>       Specify raw PG generation rules (see documentation for details)\n"
    "    --primary_affinity_tags tags  Prefer to put primary copies on OSDs with all specified tags\n"
    "    --scrub_interval <time>       Enable regular scrubbing for this pool. Format: number + unit s/m/h/d/M/y\n"
    "    --used_for_fs <name>          Mark pool as used for VitastorFS with metadata in image <name>\n"
    "    --pg_stripe_size <number>     Increase object grouping stripe\n"
    "    --max_osd_combinations 10000  Maximum number of random combinations for LP solver input\n"
    "    --wait                        Wait for the new pool to come online\n"
    "    -f|--force                    Do not check that cluster has enough OSDs to create the pool\n"
    "  Examples:\n"
    "    vitastor-cli create-pool test_x4 -s 4 -n 32\n"
    "    vitastor-cli create-pool test_ec42 --ec 4+2 -n 32\n"
    "\n"
    "vitastor-cli modify-pool|pool-modify <id|name> [--name <new_name>] [PARAMETERS...]\n"
    "  Modify an existing pool. Modifiable parameters:\n"
    "    [-s|--pg_size <number>] [--pg_minsize <number>] [-n|--pg_count <count>]\n"
    "    [--failure_domain <level>] [--root_node <node>] [--osd_tags <tags>] [--used_for_fs <name>]\n"
    "    [--max_osd_combinations <number>] [--primary_affinity_tags <tags>] [--scrub_interval <time>]\n"
    "    [--level_placement <rules>] [--raw_placement <rules>]\n"
    "  Non-modifiable parameters (changing them WILL lead to data loss):\n"
    "    [--block_size <size>] [--bitmap_granularity <size>]\n"
    "    [--immediate_commit <all|small|none>] [--pg_stripe_size <size>]\n"
    "  These, however, can still be modified with -f|--force.\n"
    "  See create-pool for parameter descriptions.\n"
    "  Examples:\n"
    "    vitastor-cli modify-pool pool_A --name pool_B\n"
    "    vitastor-cli modify-pool 2 --pg_size 4 -n 128\n"
    "\n"
    "vitastor-cli rm-pool|pool-rm [--force] <id|name>\n"
    "  Remove a pool. Refuses to remove pools with images without --force.\n"
    "\n"
    "vitastor-cli ls-pools|pool-ls|ls-pool|pools [-l] [--detail] [--sort FIELD] [-r] [-n N] [--stats] [<glob> ...]\n"
    "  List pools (only matching <glob> patterns if passed).\n"
    "  -l|--long       Also report I/O statistics\n"
    "  --detail        Use list format (not table), show all details\n"
    "  --sort FIELD    Sort by specified field (see fields in --json output)\n"
    "  -r|--reverse    Sort in descending order\n"
    "  -n|--count N    Only list first N items\n"
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
    "  --color 1|0         Enable/disable colored output and CR symbols (default 1 if stdout is a terminal)\n"
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
            if (!strcmp(opt, "json") || !strcmp(opt, "wait") ||
                !strcmp(opt, "wait-list") || !strcmp(opt, "wait_list") ||
                !strcmp(opt, "long") || !strcmp(opt, "detail") || !strcmp(opt, "del") ||
                !strcmp(opt, "no-color") || !strcmp(opt, "no_color") ||
                !strcmp(opt, "readonly") || !strcmp(opt, "readwrite") ||
                !strcmp(opt, "force") || !strcmp(opt, "reverse") ||
                !strcmp(opt, "allow-data-loss") || !strcmp(opt, "allow_data_loss") ||
                !strcmp(opt, "down-ok") || !strcmp(opt, "down_ok") ||
                !strcmp(opt, "dry-run") || !strcmp(opt, "dry_run") ||
                !strcmp(opt, "help") || !strcmp(opt, "all") ||
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
    else if (cmd[0] == "osd-tree")
    {
        // Print OSD tree
        action_cb = p->start_osd_tree(cfg);
    }
    else if (cmd[0] == "osds" || cmd[0] == "ls-osds" || cmd[0] == "ls-osd" || cmd[0] == "osd-ls")
    {
        // Print OSD list
        cfg["flat"] = true;
        action_cb = p->start_osd_tree(cfg);
    }
    else if (cmd[0] == "modify-osd")
    {
        // Modify OSD configuration
        if (cmd.size() > 1)
            cfg["osd_num"] = cmd[1];
        action_cb = p->start_modify_osd(cfg);
    }
    else if (cmd[0] == "pg-list" || cmd[0] == "pg-ls" || cmd[0] == "list-pg" || cmd[0] == "ls-pg" || cmd[0] == "ls-pgs")
    {
        // Modify OSD configuration
        if (cmd.size() > 1)
        {
            cmd.erase(cmd.begin(), cmd.begin()+1);
            cfg["pg_state"] = cmd;
        }
        action_cb = p->start_pg_list(cfg);
    }
    else if (cmd[0] == "create-pool" || cmd[0] == "pool-create")
    {
        // Create a new pool
        if (cmd.size() > 1 && cfg["name"].is_null())
        {
            cfg["name"] = cmd[1];
        }
        action_cb = p->start_pool_create(cfg);
    }
    else if (cmd[0] == "modify-pool" || cmd[0] == "pool-modify")
    {
        // Modify existing pool
        if (cmd.size() > 1)
        {
            cfg["old_name"] = cmd[1];
        }
        action_cb = p->start_pool_modify(cfg);
    }
    else if (cmd[0] == "rm-pool" || cmd[0] == "pool-rm")
    {
        // Remove existing pool
        if (cmd.size() > 1)
        {
            cfg["pool"] = cmd[1];
        }
        action_cb = p->start_pool_rm(cfg);
    }
    else if (cmd[0] == "ls-pool" || cmd[0] == "pool-ls" || cmd[0] == "ls-pools" || cmd[0] == "pools")
    {
        // Show pool list
        cfg["show_recovery"] = 1;
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
