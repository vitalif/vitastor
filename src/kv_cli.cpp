// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// Vitastor shared key/value database test CLI

#define _XOPEN_SOURCE
#include <limits.h>

#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <unistd.h>
#include <fcntl.h>
//#include <signal.h>

#include "epoll_manager.h"
#include "str_util.h"
#include "kv_db.h"

const char *exe_name = NULL;

class kv_cli_t
{
public:
    kv_dbw_t *db = NULL;
    ring_loop_t *ringloop = NULL;
    epoll_manager_t *epmgr = NULL;
    cluster_client_t *cli = NULL;
    bool interactive = false;
    int in_progress = 0;
    char *cur_cmd = NULL;
    int cur_cmd_size = 0, cur_cmd_alloc = 0;
    bool finished = false, eof = false;
    json11::Json::object cfg;

    ~kv_cli_t();

    static json11::Json::object parse_args(int narg, const char *args[]);
    void run(const json11::Json::object & cfg);
    void read_cmd();
    void next_cmd();
    void handle_cmd(const std::string & cmd, std::function<void()> cb);
};

kv_cli_t::~kv_cli_t()
{
    if (cur_cmd)
    {
        free(cur_cmd);
        cur_cmd = NULL;
    }
    cur_cmd_alloc = 0;
    if (db)
        delete db;
    if (cli)
    {
        cli->flush();
        delete cli;
    }
    if (epmgr)
        delete epmgr;
    if (ringloop)
        delete ringloop;
}

json11::Json::object kv_cli_t::parse_args(int narg, const char *args[])
{
    json11::Json::object cfg;
    for (int i = 1; i < narg; i++)
    {
        if (!strcmp(args[i], "-h") || !strcmp(args[i], "--help"))
        {
            printf(
                "Vitastor Key/Value CLI\n"
                "(c) Vitaliy Filippov, 2023+ (VNPL-1.1)\n"
                "\n"
                "USAGE: %s [--etcd_address ADDR] [OTHER OPTIONS]\n",
                exe_name
            );
            exit(0);
        }
        else if (args[i][0] == '-' && args[i][1] == '-')
        {
            const char *opt = args[i]+2;
            cfg[opt] = !strcmp(opt, "json") || i == narg-1 ? "1" : args[++i];
        }
    }
    return cfg;
}

void kv_cli_t::run(const json11::Json::object & cfg)
{
    // Create client
    ringloop = new ring_loop_t(512);
    epmgr = new epoll_manager_t(ringloop);
    cli = new cluster_client_t(ringloop, epmgr->tfd, cfg);
    db = new kv_dbw_t(cli);
    // Load image metadata
    while (!cli->is_ready())
    {
        ringloop->loop();
        if (cli->is_ready())
            break;
        ringloop->wait();
    }
    // Run
    fcntl(0, F_SETFL, fcntl(0, F_GETFL, 0) | O_NONBLOCK);
    try
    {
        epmgr->tfd->set_fd_handler(0, false, [this](int fd, int events)
        {
            if (events & EPOLLIN)
            {
                read_cmd();
            }
            if (events & EPOLLRDHUP)
            {
                epmgr->tfd->set_fd_handler(0, false, NULL);
                finished = true;
            }
        });
        interactive = true;
        printf("> ");
    }
    catch (std::exception & e)
    {
        // Can't add to epoll, STDIN is probably a file
        read_cmd();
    }
    while (!finished)
    {
        ringloop->loop();
        if (!finished)
            ringloop->wait();
    }
    // Destroy the client
    delete db;
    db = NULL;
    cli->flush();
    delete cli;
    delete epmgr;
    delete ringloop;
    cli = NULL;
    epmgr = NULL;
    ringloop = NULL;
}

void kv_cli_t::read_cmd()
{
    if (!cur_cmd_alloc)
    {
        cur_cmd_alloc = 65536;
        cur_cmd = (char*)malloc_or_die(cur_cmd_alloc);
    }
    while (cur_cmd_size < cur_cmd_alloc)
    {
        int r = read(0, cur_cmd+cur_cmd_size, cur_cmd_alloc-cur_cmd_size);
        if (r < 0 && errno != EAGAIN)
            fprintf(stderr, "Error reading from stdin: %s\n", strerror(errno));
        if (r > 0)
            cur_cmd_size += r;
        if (r == 0)
            eof = true;
        if (r <= 0)
            break;
    }
    next_cmd();
}

void kv_cli_t::next_cmd()
{
    if (in_progress > 0)
    {
        return;
    }
    int pos = 0;
    for (; pos < cur_cmd_size; pos++)
    {
        if (cur_cmd[pos] == '\n' || cur_cmd[pos] == '\r')
        {
            auto cmd = trim(std::string(cur_cmd, pos));
            pos++;
            memmove(cur_cmd, cur_cmd+pos, cur_cmd_size-pos);
            cur_cmd_size -= pos;
            in_progress++;
            handle_cmd(cmd, [this]()
            {
                in_progress--;
                if (interactive)
                    printf("> ");
                next_cmd();
                if (!in_progress)
                    read_cmd();
            });
            break;
        }
    }
    if (eof && !in_progress)
    {
        finished = true;
    }
}

void kv_cli_t::handle_cmd(const std::string & cmd, std::function<void()> cb)
{
    if (cmd == "")
    {
        cb();
        return;
    }
    auto pos = cmd.find_first_of(" \t");
    if (pos != std::string::npos)
    {
        while (pos < cmd.size()-1 && (cmd[pos+1] == ' ' || cmd[pos+1] == '\t'))
            pos++;
    }
    auto opname = strtolower(pos == std::string::npos ? cmd : cmd.substr(0, pos));
    if (opname == "open")
    {
        uint64_t pool_id = 0;
        inode_t inode_id = 0;
        uint32_t kv_block_size = 0;
        int scanned = sscanf(cmd.c_str() + pos+1, "%lu %lu %u", &pool_id, &inode_id, &kv_block_size);
        if (scanned == 2)
        {
            kv_block_size = 4096;
        }
        if (scanned < 2 || !pool_id || !inode_id || !kv_block_size || (kv_block_size & (kv_block_size-1)) != 0)
        {
            fprintf(stderr, "Usage: open <pool_id> <inode_id> [block_size]. Block size must be a power of 2. Default is 4096.\n");
            cb();
            return;
        }
        cfg["kv_block_size"] = (uint64_t)kv_block_size;
        db->open(INODE_WITH_POOL(pool_id, inode_id), cfg, [=](int res)
        {
            if (res < 0)
                fprintf(stderr, "Error opening index: %s (code %d)\n", strerror(-res), res);
            else
                printf("Index opened. Current size: %lu bytes\n", db->get_size());
            cb();
        });
    }
    else if (opname == "config")
    {
        auto pos2 = cmd.find_first_of(" \t", pos+1);
        if (pos2 == std::string::npos)
        {
            fprintf(stderr, "Usage: config <property> <value>\n");
            cb();
            return;
        }
        auto key = trim(cmd.substr(pos+1, pos2-pos-1));
        auto value = parse_size(trim(cmd.substr(pos2+1)));
        if (key != "kv_memory_limit" &&
            key != "kv_allocate_blocks" &&
            key != "kv_evict_max_misses" &&
            key != "kv_evict_attempts_per_level" &&
            key != "kv_evict_unused_age")
        {
            fprintf(stderr, "Allowed properties: kv_memory_limit, kv_allocate_blocks, kv_evict_max_misses, kv_evict_attempts_per_level, kv_evict_unused_age\n");
        }
        else
        {
            cfg[key] = value;
            db->set_config(cfg);
        }
        cb();
    }
    else if (opname == "get" || opname == "set" || opname == "del")
    {
        if (opname == "get" || opname == "del")
        {
            if (pos == std::string::npos)
            {
                fprintf(stderr, "Usage: %s <key>\n", opname.c_str());
                cb();
                return;
            }
            auto key = trim(cmd.substr(pos+1));
            if (opname == "get")
            {
                db->get(key, [this, cb](int res, const std::string & value)
                {
                    if (res < 0)
                        fprintf(stderr, "Error: %s (code %d)\n", strerror(-res), res);
                    else
                    {
                        write(1, value.c_str(), value.size());
                        write(1, "\n", 1);
                    }
                    cb();
                });
            }
            else
            {
                db->del(key, [this, cb](int res)
                {
                    if (res < 0)
                        fprintf(stderr, "Error: %s (code %d)\n", strerror(-res), res);
                    else
                        printf("OK\n");
                    cb();
                });
            }
        }
        else
        {
            auto pos2 = cmd.find_first_of(" \t", pos+1);
            if (pos2 == std::string::npos)
            {
                fprintf(stderr, "Usage: set <key> <value>\n");
                cb();
                return;
            }
            auto key = trim(cmd.substr(pos+1, pos2-pos-1));
            auto value = trim(cmd.substr(pos2+1));
            db->set(key, value, [this, cb](int res)
            {
                if (res < 0)
                    fprintf(stderr, "Error: %s (code %d)\n", strerror(-res), res);
                else
                    printf("OK\n");
                cb();
            });
        }
    }
    else if (opname == "list")
    {
        std::string start, end;
        if (pos != std::string::npos)
        {
            auto pos2 = cmd.find_first_of(" \t", pos+1);
            if (pos2 != std::string::npos)
            {
                start = trim(cmd.substr(pos+1, pos2-pos-1));
                end = trim(cmd.substr(pos2+1));
            }
            else
            {
                start = trim(cmd.substr(pos+1));
            }
        }
        void *handle = db->list_start(start);
        db->list_next(handle, [=](int res, const std::string & key, const std::string & value)
        {
            if (res < 0)
            {
                if (res != -ENOENT)
                {
                    fprintf(stderr, "Error: %s (code %d)\n", strerror(-res), res);
                }
                db->list_close(handle);
                cb();
            }
            else
            {
                printf("%s = %s\n", key.c_str(), value.c_str());
                db->list_next(handle, NULL);
            }
        });
    }
    else if (opname == "close")
    {
        db->close([=]()
        {
            printf("Index closed\n");
            cb();
        });
    }
    else if (opname == "quit" || opname == "q")
    {
        ::close(0);
        finished = true;
    }
    else
    {
        fprintf(
            stderr, "Unknown operation: %s. Supported operations:\n"
            "open <pool_id> <inode_id> [block_size]\n"
            "config <property> <value>\n"
            "get <key>\nset <key> <value>\ndel <key>\nlist [<start> [end]]\n"
            "close\nquit\n", opname.c_str()
        );
        cb();
    }
}

int main(int narg, const char *args[])
{
    setvbuf(stdout, NULL, _IONBF, 0);
    setvbuf(stderr, NULL, _IONBF, 0);
    exe_name = args[0];
    kv_cli_t *p = new kv_cli_t();
    p->run(kv_cli_t::parse_args(narg, args));
    delete p;
    return 0;
}
