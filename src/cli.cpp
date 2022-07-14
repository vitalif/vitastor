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
#include "base64.h"

static const char *exe_name = NULL;

static void help();

static json11::Json::object parse_args(int narg, const char *args[])
{
    json11::Json::object cfg;
    json11::Json::array cmd;
    cfg["progress"] = "1";
    for (int i = 1; i < narg; i++)
    {
        if (!strcmp(args[i], "-h") || !strcmp(args[i], "--help"))
        {
            help();
        }
        else if (args[i][0] == '-' && args[i][1] == 'l')
        {
            cfg["long"] = "1";
        }
        else if (args[i][0] == '-' && args[i][1] == 'n')
        {
            cfg["count"] = args[++i];
        }
        else if (args[i][0] == '-' && args[i][1] == 'p')
        {
            cfg["pool"] = args[++i];
        }
        else if (args[i][0] == '-' && args[i][1] == 's')
        {
            cfg["size"] = args[++i];
        }
        else if (args[i][0] == '-' && args[i][1] == 'r')
        {
            cfg["reverse"] = "1";
        }
        else if (args[i][0] == '-' && args[i][1] == 'f')
        {
            cfg["force"] = "1";
        }
        else if (args[i][0] == '-' && args[i][1] == '-')
        {
            const char *opt = args[i]+2;
            cfg[opt] = i == narg-1 || !strcmp(opt, "json") || !strcmp(opt, "wait-list") ||
                !strcmp(opt, "long") || !strcmp(opt, "del") || !strcmp(opt, "no-color") ||
                !strcmp(opt, "readonly") || !strcmp(opt, "readwrite") ||
                !strcmp(opt, "force") || !strcmp(opt, "reverse") ||
                !strcmp(opt, "writers-stopped") && strcmp("1", args[i+1]) != 0
                ? "1" : args[++i];
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
    cfg["command"] = cmd;
    return cfg;
}

static void help()
{
    printf(
        "Vitastor command-line tool\n"
        "(c) Vitaliy Filippov, 2019+ (VNPL-1.1)\n"
        "\n"
        "USAGE:\n"
        "%s status\n"
        "  Show cluster status\n"
        "\n"
        "%s df\n"
        "  Show pool space statistics\n"
        "\n"
        "%s ls [-l] [-p POOL] [--sort FIELD] [-r] [-n N] [<glob> ...]\n"
        "  List images (only matching <glob> patterns if passed).\n"
        "  -p|--pool POOL  Filter images by pool ID or name\n"
        "  -l|--long       Also report allocated size and I/O statistics\n"
        "  --del           Also include delete operation statistics\n"
        "  --sort FIELD    Sort by specified field (name, size, used_size, <read|write|delete>_<iops|bps|lat|queue>)\n"
        "  -r|--reverse    Sort in descending order\n"
        "  -n|--count N    Only list first N items\n"
        "\n"
        "%s create -s|--size <size> [-p|--pool <id|name>] [--parent <parent_name>[@<snapshot>]] <name>\n"
        "  Create an image. You may use K/M/G/T suffixes for <size>. If --parent is specified,\n"
        "  a copy-on-write image clone is created. Parent must be a snapshot (readonly image).\n"
        "  Pool must be specified if there is more than one pool.\n"
        "\n"
        "%s create --snapshot <snapshot> [-p|--pool <id|name>] <image>\n"
        "%s snap-create [-p|--pool <id|name>] <image>@<snapshot>\n"
        "  Create a snapshot of image <name>. May be used live if only a single writer is active.\n"
        "\n"
        "%s modify <name> [--rename <new-name>] [--resize <size>] [--readonly | --readwrite] [-f|--force]\n"
        "  Rename, resize image or change its readonly status. Images with children can't be made read-write.\n"
        "  If the new size is smaller than the old size, extra data will be purged.\n"
        "  You should resize file system in the image, if present, before shrinking it.\n"
        "  -f|--force  Proceed with shrinking or setting readwrite flag even if the image has children.\n"
        "\n"
        "%s rm <from> [<to>] [--writers-stopped]\n"
        "  Remove <from> or all layers between <from> and <to> (<to> must be a child of <from>),\n"
        "  rebasing all their children accordingly. --writers-stopped allows merging to be a bit\n"
        "  more effective in case of a single 'slim' read-write child and 'fat' removed parent:\n"
        "  the child is merged into parent and parent is renamed to child in that case.\n"
        "  In other cases parent layers are always merged into children.\n"
        "\n"
        "%s flatten <layer>\n"
        "  Flatten a layer, i.e. merge data and detach it from parents.\n"
        "\n"
        "%s rm-data --pool <pool> --inode <inode> [--wait-list] [--min-offset <offset>]\n"
        "  Remove inode data without changing metadata.\n"
        "  --wait-list   Retrieve full objects listings before starting to remove objects.\n"
        "                Requires more memory, but allows to show correct removal progress.\n"
        "  --min-offset  Purge only data starting with specified offset.\n"
        "\n"
        "%s merge-data <from> <to> [--target <target>]\n"
        "  Merge layer data without changing metadata. Merge <from>..<to> to <target>.\n"
        "  <to> must be a child of <from> and <target> may be one of the layers between\n"
        "  <from> and <to>, including <from> and <to>.\n"
        "\n"
        "%s alloc-osd\n"
        "  Allocate a new OSD number and reserve it by creating empty /osd/stats/<n> key.\n"
        "\n"
        "GLOBAL OPTIONS:\n"
        "  --etcd_address <etcd_address>\n"
        "  --iodepth N         Send N operations in parallel to each OSD when possible (default 32)\n"
        "  --parallel_osds M   Work with M osds in parallel when possible (default 4)\n"
        "  --progress 1|0      Report progress (default 1)\n"
        "  --cas 1|0           Use CAS writes for flatten, merge, rm (default is decide automatically)\n"
        "  --no-color          Disable colored output\n"
        "  --json              JSON output\n"
        ,
        exe_name, exe_name, exe_name, exe_name, exe_name, exe_name,
        exe_name, exe_name, exe_name, exe_name, exe_name, exe_name
    );
    exit(0);
}

static int run(cli_tool_t *p, json11::Json::object cfg)
{
    cli_result_t result = {};
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
        action_cb = p->start_df(cfg);
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
    else if (cmd[0] == "alloc-osd")
    {
        // Allocate a new OSD number
        action_cb = p->start_alloc_osd(cfg);
    }
    else
    {
        result = { .err = EINVAL, .text = "unknown command: "+cmd[0].string_value() };
    }
    if (action_cb != NULL)
    {
        // Create client
        json11::Json cfg_j = cfg;
        p->ringloop = new ring_loop_t(512);
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
        delete p->cli;
        delete p->epmgr;
        delete p->ringloop;
        p->cli = NULL;
        p->epmgr = NULL;
        p->ringloop = NULL;
    }
    // Print result
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
