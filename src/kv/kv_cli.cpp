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

#include "cluster_client.h"
#include "epoll_manager.h"
#include "str_util.h"
#include "vitastor_kv.h"

#define KV_LIST_BUF_SIZE 65536

const char *exe_name = NULL;

class kv_cli_t
{
public:
    std::map<std::string, std::string> cfg;
    std::vector<std::string> cli_cmd;

    vitastorkv_dbw_t *db = NULL;
    ring_loop_t *ringloop = NULL;
    epoll_manager_t *epmgr = NULL;
    cluster_client_t *cli = NULL;
    int load_parallelism = 16;
    bool opened = false;
    bool interactive = false, is_file = false;
    int in_progress = 0;
    char *cur_cmd = NULL;
    int cur_cmd_size = 0, cur_cmd_alloc = 0;
    bool finished = false, eof = false;

    std::function<void(int)> load_cb;
    bool loading_json = false, in_loadjson = false;
    int load_state = 0;
    std::string load_key;

    ~kv_cli_t();

    void parse_args(int narg, const char *args[]);
    void run();
    void read_cmd();
    void next_cmd();
    std::vector<std::string> parse_cmd(const std::string & cmdstr);
    void handle_cmd(const std::vector<std::string> & cmd, std::function<void(int)> cb);
    void loadjson();
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

void kv_cli_t::parse_args(int narg, const char *args[])
{
    bool db = false;
    for (int i = 1; i < narg; i++)
    {
        if (!strcmp(args[i], "-h") || !strcmp(args[i], "--help"))
        {
            printf(
                "Vitastor Key/Value CLI\n"
                "(c) Vitaliy Filippov, 2023+ (VNPL-1.1)\n"
                "\n"
                "USAGE: %s [OPTIONS] [<IMAGE> [<COMMAND>]]\n"
                "\n"
                "COMMANDS:\n"
                "  get <key>\n"
                "  set <key> <value>\n"
                "  del <key>\n"
                "  list [<start> [end]]\n"
                "  dump [<start> [end]]\n"
                "  dumpjson [<start> [end]]\n"
                "  loadjson\n"
                "\n"
                "<IMAGE> should be the name of Vitastor image with the DB.\n"
                "Without <COMMAND>, you get an interactive DB shell.\n"
                "\n"
                "OPTIONS:\n"
                "  --kv_block_size 4k\n"
                "    Key-value B-Tree block size\n"
                "  --kv_memory_limit 128M\n"
                "    Maximum memory to use for vitastor-kv index cache\n"
                "  --kv_allocate_blocks 4\n"
                "    Number of PG blocks used for new tree block allocation in parallel\n"
                "  --kv_evict_max_misses 10\n"
                "    Eviction algorithm parameter: retry eviction from another random spot\n"
                "    if this number of keys is used currently or was used recently\n"
                "  --kv_evict_attempts_per_level 3\n"
                "    Retry eviction at most this number of times per tree level, starting\n"
                "    with bottom-most levels\n"
                "  --kv_evict_unused_age 1000\n"
                "    Evict only keys unused during this number of last operations\n"
                "  --kv_log_level 1\n"
                "    Log level. 0 = errors, 1 = warnings, 10 = trace operations\n"
                ,
                exe_name
            );
            exit(0);
        }
        else if (args[i][0] == '-' && args[i][1] == '-')
        {
            const char *opt = args[i]+2;
            cfg[opt] = !strcmp(opt, "json") || i == narg-1 ? "1" : args[++i];
        }
        else if (!db)
        {
            cfg["db"] = args[i];
            db = true;
        }
        else
        {
            cli_cmd.push_back(args[i]);
        }
    }
}

void kv_cli_t::run()
{
    // Create client
    ringloop = new ring_loop_t(512);
    epmgr = new epoll_manager_t(ringloop);
    cli = new cluster_client_t(ringloop, epmgr->tfd, cfg);
    db = new vitastorkv_dbw_t(cli);
    // Load image metadata
    while (!cli->is_ready())
    {
        ringloop->loop();
        if (cli->is_ready())
            break;
        ringloop->wait();
    }
    // Open if DB is set in options
    if (cfg.find("db") != cfg.end())
    {
        bool done = false;
        handle_cmd({ "open", cfg.at("db") }, [&done](int res) { if (res != 0) exit(1); done = true; });
        while (!done)
        {
            ringloop->loop();
            if (done)
                break;
            ringloop->wait();
        }
    }
    // Run single command from CLI
    if (cli_cmd.size())
    {
        bool done = false;
        handle_cmd(cli_cmd, [&done](int res) { if (res != 0) exit(1); done = true; });
        while (!done)
        {
            ringloop->loop();
            if (done)
                break;
            ringloop->wait();
        }
    }
    else
    {
        // Run interactive shell
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
            interactive = isatty(0);
            if (interactive)
                printf("> ");
        }
        catch (std::exception & e)
        {
            // Can't add to epoll, STDIN is probably a file
            is_file = true;
            read_cmd();
        }
        while (!finished)
        {
            ringloop->loop();
            if (!finished)
                ringloop->wait();
        }
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
    if (loading_json)
    {
        loadjson();
        return;
    }
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
            handle_cmd(parse_cmd(cmd), [this](int res)
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

struct kv_cli_list_t
{
    vitastorkv_dbw_t *db = NULL;
    std::string buf;
    void *handle = NULL;
    int format = 0;
    int n = 0;
    std::function<void(int)> cb;

    void write(const std::string & str)
    {
        if (buf.capacity() < KV_LIST_BUF_SIZE)
            buf.reserve(KV_LIST_BUF_SIZE);
        if (buf.size() + str.size() > buf.capacity())
            flush();
        buf.append(str.data(), str.size());
    }

    void flush()
    {
        size_t done = 0;
        while (done < buf.size())
        {
            ssize_t res = ::write(1, buf.data()+done, buf.size()-done);
            if (res > 0)
                done += res;
        }
    }
};

std::vector<std::string> kv_cli_t::parse_cmd(const std::string & str)
{
    std::vector<std::string> res;
    size_t pos = 0;
    auto cmd = scan_escaped(str, pos);
    if (cmd.empty())
        return res;
    res.push_back(cmd);
    int max_args = (cmd == "set" || cmd == "config" ||
        cmd == "list" || cmd == "dump" || cmd == "dumpjson" ? 3 :
        (cmd == "open" || cmd == "get" || cmd == "del" ? 2 : 1));
    while (pos < str.size() && res.size() < max_args)
    {
        if (res.size() == max_args-1)
        {
            // Allow unquoted last argument
            pos = str.find_first_not_of(" \t\r\n", pos);
            if (pos == std::string::npos)
                break;
            if (str[pos] != '"' && str[pos] != '\'')
            {
                res.push_back(trim(str.substr(pos)));
                break;
            }
        }
        auto arg = scan_escaped(str, pos);
        if (arg.size())
            res.push_back(arg);
    }
    return res;
}

void kv_cli_t::loadjson()
{
    // simple streaming json parser
    if (in_progress >= load_parallelism || in_loadjson)
    {
        return;
    }
    in_loadjson = true;
    if (load_state == 5)
    {
st_5:
        if (!in_progress)
        {
            loading_json = false;
            auto cb = std::move(load_cb);
            cb(0);
        }
        in_loadjson = false;
        return;
    }
    do
    {
        read_cmd();
        size_t pos = 0;
        while (true)
        {
            while (pos < cur_cmd_size && is_white(cur_cmd[pos]))
            {
                pos++;
            }
            if (pos >= cur_cmd_size)
            {
                break;
            }
            if (load_state == 0 || load_state == 2)
            {
                char expected = "{ :"[load_state];
                if (cur_cmd[pos] != expected)
                {
                    fprintf(stderr, "Unexpected %c, expected %c\n", cur_cmd[pos], expected);
                    exit(1);
                }
                pos++;
                load_state++;
            }
            else if (load_state == 1 || load_state == 3)
            {
                if (cur_cmd[pos] != '"')
                {
                    fprintf(stderr, "Unexpected %c, expected \"\n", cur_cmd[pos]);
                    exit(1);
                }
                size_t prev = pos;
                auto str = scan_escaped(cur_cmd, cur_cmd_size, pos, false);
                if (pos == prev)
                {
                    break;
                }
                load_state++;
                if (load_state == 2)
                {
                    load_key = str;
                }
                else
                {
                    in_progress++;
                    handle_cmd({ "set", load_key, str }, [this](int res)
                    {
                        in_progress--;
                        next_cmd();
                    });
                    if (in_progress >= load_parallelism)
                    {
                        break;
                    }
                }
            }
            else if (load_state == 4)
            {
                if (cur_cmd[pos] == ',')
                {
                    pos++;
                    load_state = 1;
                }
                else if (cur_cmd[pos] == '}')
                {
                    pos++;
                    load_state = 5;
                    goto st_5;
                }
                else
                {
                    fprintf(stderr, "Unexpected %c, expected , or }\n", cur_cmd[pos]);
                    exit(1);
                }
            }
        }
        if (pos < cur_cmd_size)
        {
            memmove(cur_cmd, cur_cmd+pos, cur_cmd_size-pos);
        }
        cur_cmd_size -= pos;
    } while (loading_json && is_file);
    in_loadjson = false;
}

void kv_cli_t::handle_cmd(const std::vector<std::string> & cmd, std::function<void(int)> cb)
{
    if (!cmd.size())
    {
        cb(-EINVAL);
        return;
    }
    auto & opname = cmd[0];
    if (!opened && opname != "open" && opname != "config" && opname != "quit" && opname != "q")
    {
        fprintf(stderr, "Error: database not opened\n");
        cb(-EINVAL);
        return;
    }
    if (opname == "open")
    {
        auto name = cmd.size() > 1 ? cmd[1] : "";
        uint64_t pool_id = 0;
        inode_t inode_id = 0;
        int scanned = sscanf(name.c_str(), "%ju %ju", &pool_id, &inode_id);
        if (scanned < 2 || !pool_id || !inode_id)
        {
            inode_id = 0;
            name = trim(name);
            for (auto & ic: cli->st_cli.inode_config)
            {
                if (ic.second.name == name)
                {
                    inode_id = ic.first;
                    break;
                }
            }
            if (!inode_id)
            {
                fprintf(stderr, "Usage: open <image> OR open <pool_id> <inode_id>\n");
                cb(-EINVAL);
                return;
            }
        }
        else
            inode_id = INODE_WITH_POOL(pool_id, inode_id);
        db->open(inode_id, cfg, [=](int res)
        {
            if (res < 0)
            {
                fprintf(stderr, "Error opening index: %s (code %d)\n", strerror(-res), res);
            }
            else
            {
                opened = true;
                fprintf(interactive ? stdout : stderr, "Index opened. Current size: %ju bytes\n", db->get_size());
            }
            cb(res);
        });
    }
    else if (opname == "config")
    {
        if (cmd.size() < 3)
        {
            fprintf(stderr, "Usage: config <property> <value>\n");
            cb(-EINVAL);
            return;
        }
        auto & key = cmd[1];
        auto & value = cmd[2];
        if (key != "kv_memory_limit" &&
            key != "kv_allocate_blocks" &&
            key != "kv_evict_max_misses" &&
            key != "kv_evict_attempts_per_level" &&
            key != "kv_evict_unused_age" &&
            key != "kv_log_level" &&
            key != "kv_block_size")
        {
            fprintf(
                stderr, "Allowed properties: kv_block_size, kv_memory_limit, kv_allocate_blocks,"
                " kv_evict_max_misses, kv_evict_attempts_per_level, kv_evict_unused_age, kv_log_level\n"
            );
            cb(-EINVAL);
        }
        else if (key == "kv_block_size")
        {
            if (opened)
            {
                fprintf(stderr, "kv_block_size can't be set after opening DB\n");
                cb(-EINVAL);
            }
            else
            {
                cfg[key] = value;
                cb(0);
            }
        }
        else
        {
            cfg[key] = value;
            db->set_config(cfg);
            cb(0);
        }
    }
    else if (opname == "get" || opname == "set" || opname == "del")
    {
        if (opname == "get" || opname == "del")
        {
            if (cmd.size() < 2)
            {
                fprintf(stderr, "Usage: %s <key>\n", opname.c_str());
                cb(-EINVAL);
                return;
            }
            auto & key = cmd[1];
            if (opname == "get")
            {
                db->get(key, [cb](int res, const std::string & value)
                {
                    if (res < 0)
                        fprintf(stderr, "Error: %s (code %d)\n", strerror(-res), res);
                    else
                    {
                        if (write(1, value.c_str(), value.size()) < 0 || write(1, "\n", 1) < 0)
                            exit(1);
                    }
                    cb(res);
                });
            }
            else
            {
                db->del(key, [this, cb](int res)
                {
                    if (res < 0)
                        fprintf(stderr, "Error: %s (code %d)\n", strerror(-res), res);
                    else
                        fprintf(interactive ? stdout : stderr, "OK\n");
                    cb(res);
                });
            }
        }
        else
        {
            if (cmd.size() < 3)
            {
                fprintf(stderr, "Usage: set <key> <value>\n");
                cb(-EINVAL);
                return;
            }
            auto & key = cmd[1];
            auto & value = cmd[2];
            db->set(key, value, [this, cb, l = loading_json](int res)
            {
                if (res < 0)
                    fprintf(stderr, "Error: %s (code %d)\n", strerror(-res), res);
                else if (!l)
                    fprintf(interactive ? stdout : stderr, "OK\n");
                cb(res);
            });
        }
    }
    else if (opname == "list" || opname == "dump" || opname == "dumpjson")
    {
        kv_cli_list_t *lst = new kv_cli_list_t;
        std::string start = cmd.size() >= 2 ? cmd[1] : "";
        std::string end = cmd.size() >= 3 ? cmd[2] : "";
        lst->handle = db->list_start(start);
        lst->db = db;
        lst->format = opname == "dump" ? 1 : (opname == "dumpjson" ? 2 : 0);
        lst->cb = std::move(cb);
        db->list_next(lst->handle, [lst](int res, const std::string & key, const std::string & value)
        {
            if (res < 0)
            {
                if (res != -ENOENT)
                    fprintf(stderr, "Error: %s (code %d)\n", strerror(-res), res);
                if (lst->format == 2)
                    lst->write("\n}\n");
                lst->flush();
                lst->db->list_close(lst->handle);
                lst->cb(res == -ENOENT ? 0 : res);
                delete lst;
            }
            else
            {
                if (lst->format == 2)
                {
                    lst->write(lst->n ? ",\n  " : "{\n  ");
                    lst->write(addslashes(key));
                    lst->write(": ");
                    lst->write(addslashes(value));
                }
                else if (lst->format == 1)
                {
                    lst->write("set ");
                    lst->write(auto_addslashes(key));
                    lst->write(" ");
                    lst->write(value);
                    lst->write("\n");
                }
                else
                {
                    lst->write(key);
                    lst->write(" = ");
                    lst->write(value);
                    lst->write("\n");
                }
                lst->n++;
                lst->db->list_next(lst->handle, NULL);
            }
        });
    }
    else if (opname == "loadjson")
    {
        loading_json = true;
        load_state = 0;
        load_cb = cb;
        loadjson();
    }
    else if (opname == "close")
    {
        db->close([=]()
        {
            fprintf(interactive ? stdout : stderr, "Index closed\n");
            opened = false;
            cb(0);
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
            "open <image>\nopen <pool_id> <inode_id>\n"
            "config <property> <value>\n"
            "get <key>\nset <key> <value>\ndel <key>\n"
            "list [<start> [end]]\ndump [<start> [end]]\ndumpjson [<start> [end]]\nloadjson\n"
            "close\nquit\n", opname.c_str()
        );
        cb(-EINVAL);
    }
}

int main(int narg, const char *args[])
{
    setvbuf(stdout, NULL, _IONBF, 0);
    setvbuf(stderr, NULL, _IONBF, 0);
    exe_name = args[0];
    kv_cli_t *p = new kv_cli_t();
    p->parse_args(narg, args);
    p->run();
    delete p;
    return 0;
}
