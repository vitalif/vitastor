// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// NFS proxy entrypoint, common for both pseudo-FS and Vitastor-KV based FS

#define _XOPEN_SOURCE
#include <limits.h>

#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/wait.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>

#include "proto/nfs.h"
#include "proto/rpc.h"
#include "proto/portmap.h"

#include "addr_util.h"
#include "str_util.h"
#include "json_util.h"
#include "nfs_proxy.h"
#include "nfs_kv.h"
#include "nfs_block.h"
#include "nfs_common.h"
#include "http_client.h"
#include "cli.h"

#define ETCD_INODE_STATS_WATCH_ID 101
#define ETCD_POOL_STATS_WATCH_ID 102

const char *exe_name = NULL;

nfs_proxy_t::~nfs_proxy_t()
{
#ifdef WITH_RDMACM
    destroy_rdma();
#endif
    if (kvfs)
        delete kvfs;
    if (blockfs)
        delete blockfs;
    if (db)
        delete db;
    if (cmd)
        delete cmd;
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

static const char* help_text =
    "Vitastor NFS 3.0 proxy " VITASTOR_VERSION "\n"
    "(c) Vitaliy Filippov, 2021+ (VNPL-1.1)\n"
    "\n"
    "vitastor-nfs (--fs <NAME> | --block) [-o <OPT>] mount <MOUNTPOINT>\n"
    "  Start local filesystem server and mount file system to <MOUNTPOINT>.\n"
    "  Use regular `umount <MOUNTPOINT>` to unmount the FS.\n"
    "  The server will be automatically stopped when the FS is unmounted.\n"
    "  -o|--options <OPT>  Pass additional NFS mount options (ex.: -o async).\n"
    "\n"
    "vitastor-nfs (--fs <NAME> | --block) start\n"
    "  Start network NFS server. Options:\n"
    "  --bind <IP>           bind service to <IP> address (default 0.0.0.0)\n"
    "  --port <PORT>         use port <PORT> for NFS services (default is 2049)\n"
    "                        specify \"auto\" to auto-select and print port\n"
    "  --portmap 0           do not listen on port 111 (portmap/rpcbind, requires root)\n"
    "  --nfs_rdma <PORT>     enable NFS-RDMA at RDMA-CM port <PORT> (you can try 20049)\n"
    "                        if RDMA is enabled and --port is set to 0, TCP will be disabled\n"
    "  --nfs_rdma_credit 16  maximum operation credit for RDMA clients (max iodepth)\n"
    "  --nfs_rdma_send 1024  maximum RDMA send operation count (should be larger than iodepth)\n"
    "  --nfs_rdma_alloc 1M   RDMA memory allocation rounding\n"
    "  --nfs_rdma_gc 64M     maximum unused RDMA buffers\n"
    "\n"
    "vitastor-nfs --fs <NAME> upgrade\n"
    "  Upgrade FS metadata. Can be run online, but server(s) should be restarted\n"
    "  after upgrade.\n"
    "\n"
    "vitastor-nfs --fs <NAME> defrag [OPTIONS] [--dry-run]\n"
    "  Defragment volumes used for small file storage having more than <defrag_percent> %%\n"
    "  of data removed. Can be run online.\n"
    "  In VitastorFS, small files are stored in large \"volumes\" / \"shared inodes\" one\n"
    "  after another. When you delete or extend such files, they are moved and garbage is left\n"
    "  behind. Defragmentation removes garbage and moves data still in use to new volumes.\n"
    "  Options:\n"
    "  --volume_untouched 86400\n"
    "    Defragment volumes last appended to at least this number of seconds ago\n"
    "  --defrag_percent 50\n"
    "    Defragment volumes with at least this %% of removed data\n"
    "  --defrag_block_count 16\n"
    "    Read this number of pool blocks at once during defrag\n"
    "  --defrag_iodepth 16\n"
    "    Move up to this number of files in parallel during defrag\n"
    "  --trace\n"
    "    Print verbose defragmentation status\n"
    "  --dry-run\n"
    "    Skip modifications, only print status\n"
    "  --recalc-stats\n"
    "    Recalculate all volume statistics\n"
    "  --include-empty\n"
    "    Include old and empty volumes; make sure to restart NFS servers before using it\n"
    "  --no-rm\n"
    "    Move, but do not delete data\n"
    "\n"
    "OPTIONS:\n"
    "  --fs <NAME>       use VitastorFS with metadata in image <NAME>\n"
    "  --block           use pseudo-FS presenting images as files\n"
    "  --pool <POOL>     use <POOL> as default pool for new files\n"
    "  --subdir <DIR>    export <DIR> instead of root directory (pseudo-FS only)\n"
    "  --nfspath <PATH>  set NFS export path to <PATH> (default is /)\n"
    "  --pidfile <FILE>  write process ID to the specified file\n"
    "  --logfile <FILE>  log to the specified file\n"
    "  --foreground 1    stay in foreground, do not daemonize\n"
    "\n"
    "NFS proxy is stateless if you use immediate_commit=all in your cluster and if\n"
    "you do not use client_enable_writeback=true, so you can freely use multiple\n"
    "NFS proxies with L3 load balancing in this case.\n"
    "\n"
    "Example start and mount commands for a custom NFS port:\n"
    "  vitastor-nfs start --block --etcd_address 192.168.5.10:2379 --portmap 0 --port 2050 --pool testpool\n"
    "  mount localhost:/ /mnt/ -o port=2050,mountport=2050,nfsvers=3,soft,nolock,tcp\n"
    "Or just:\n"
    "  vitastor-nfs mount --block --pool testpool /mnt/\n"
;

json11::Json::object nfs_proxy_t::parse_args(int narg, const char *args[])
{
    json11::Json::object cfg;
    std::vector<std::string> cmd;
    for (int i = 1; i < narg; i++)
    {
        if (!strcmp(args[i], "-h") || !strcmp(args[i], "--help"))
        {
            printf("%s", help_text);
            exit(0);
        }
        else if (!strcmp(args[i], "-o") || !strcmp(args[i], "--options"))
        {
            if (i >= narg-1)
            {
                printf("%s", help_text);
                exit(0);
            }
            const std::string & old = cfg["options"].string_value();
            cfg["options"] = old != "" ? old+","+args[i+1] : args[i+1];
        }
        else if (args[i][0] == '-' && args[i][1] == '-')
        {
            const char *opt = args[i]+2;
            cfg[str_replace(opt, "-", "_")] = !strcmp(opt, "json") || !strcmp(opt, "block") ||
                !strcmp(opt, "dry-run") || !strcmp(opt, "recalc-stats") ||
                !strcmp(opt, "include-empty") || !strcmp(opt, "no-rm") || i == narg-1 ? "1" : args[++i];
        }
        else
        {
            cmd.push_back(args[i]);
        }
    }
    if (cfg.find("block") == cfg.end() && cfg.find("fs") == cfg.end())
    {
        fprintf(stderr, "Specify one of --block or --fs NAME. Use vitastor-nfs --help for details\n");
        exit(1);
    }
    if (cmd.size() >= 2 && cmd[0] == "mount")
    {
        cfg["mount"] = cmd[1];
    }
    else if (cmd.size() >= 1 && cmd[0] == "start")
    {
    }
    else if (cmd.size() >= 1 && (cmd[0] == "upgrade" || cmd[0] == "defrag") && cfg["fs"].string_value() != "")
    {
        cfg["cmd"] = cmd[0];
    }
    else
    {
        printf("%s", help_text);
        exit(1);
    }
    return cfg;
}

void nfs_proxy_t::run(json11::Json cfg)
{
    timespec tv;
    clock_gettime(CLOCK_REALTIME, &tv);
    srand48(tv.tv_sec*1000000000 + tv.tv_nsec);
    server_id = (uint64_t)lrand48() | ((uint64_t)lrand48() << 31) | ((uint64_t)lrand48() << 62);
    // Parse options
    mountpoint = cfg["mount"].string_value();
    if (cfg["logfile"].string_value() != "")
        logfile = cfg["logfile"].string_value();
    pidfile = cfg["pidfile"].string_value();
    trace = cfg["log_level"].uint64_value() > 5 || cfg["trace"].uint64_value() > 0;
    bind_address = cfg["bind"].string_value();
    if (bind_address == "")
        bind_address = "0.0.0.0";
    default_pool = cfg["pool"].as_string();
    portmap_enabled = !json_is_false(cfg["portmap"]);
    nfs_port = cfg["port"].uint64_value() & 0xffff;
    nfs_rdma_port = cfg["nfs_rdma"].uint64_value() & 0xffff;
    // Allow RDMA-only mode if port is explicitly set to 0
    // Allow port auto-selection in server mode if explicitly set to --port auto
    nfs_port_auto = cfg["port"] == "auto";
    if (!nfs_port)
        nfs_port = nfs_port_auto ? 0 : (!cfg["port"].is_null() && nfs_rdma_port ? -1 : 2049);
    nfs_rdma_credit = cfg["nfs_rdma_credit"].uint64_value();
    if (!nfs_rdma_credit)
        nfs_rdma_credit = 16;
    nfs_rdma_max_send = cfg["nfs_rdma_send"].uint64_value();
    if (!nfs_rdma_max_send)
        nfs_rdma_max_send = 1024;
    nfs_rdma_alloc = cfg["nfs_rdma_alloc"].uint64_value();
    if (!nfs_rdma_alloc)
        nfs_rdma_alloc = 1048576;
    nfs_rdma_gc = cfg["nfs_rdma_gc"].uint64_value();
    if (!nfs_rdma_gc)
        nfs_rdma_gc = 64*1048576;
    export_root = cfg["nfspath"].string_value();
    if (!export_root.size())
        export_root = "/";
    if (cfg["client_writeback_allowed"].is_null())
    {
        // NFS is always aware of fsync, so we allow write-back cache
        // by default if it's enabled
        auto obj = cfg.object_items();
        obj["client_writeback_allowed"] = true;
        cfg = obj;
    }
    if (mountpoint != "")
    {
        bind_address = "127.0.0.1";
        nfs_port = 0;
        portmap_enabled = false;
        exit_on_umount = true;
    }
    mountopts = cfg["options"].string_value();
    fsname = cfg["fs"].string_value();
    // Create client
    ringloop = new ring_loop_t(RINGLOOP_DEFAULT_SIZE);
    epmgr = new epoll_manager_t(ringloop);
    cli = new cluster_client_t(ringloop, epmgr->tfd, cfg);
    cmd = new cli_tool_t();
    cmd->ringloop = ringloop;
    cmd->epmgr = epmgr;
    cmd->cli = cli;
    watch_stats();
    // Init Pseudo-FS before starting client because it depends on inode_change_hook
    if (fsname == "")
    {
        blockfs = new block_fs_state_t();
        blockfs->init(this, cfg);
    }
    // Load image metadata
    while (!cli->is_ready())
    {
        ringloop->loop();
        if (cli->is_ready())
            break;
        ringloop->wait();
    }
    // Check default pool
    check_default_pool();
    // Init VitastorFS after starting client because it depends on loaded inode configuration
    if (fsname != "")
    {
        kvfs = new kv_fs_state_t();
        kvfs->init(this, cfg);
    }
    if (cfg["cmd"].is_null())
    {
        run_server(cfg);
    }
    else if (cfg["cmd"] == "defrag")
    {
        kvfs->defrag_all(cfg, [this](int res) { finished = true; });
    }
    else if (cfg["cmd"] == "upgrade")
    {
        kvfs->upgrade_db([this](int res) { finished = true; });
    }
    while (!finished)
    {
        ringloop->loop();
        ringloop->wait();
    }
    // Destroy the client
    cli->flush();
    if (kvfs)
    {
        delete kvfs;
        kvfs = NULL;
    }
    if (blockfs)
    {
        delete blockfs;
        blockfs = NULL;
    }
    if (db)
    {
        delete db;
        db = NULL;
    }
    delete cli;
    delete epmgr;
    delete ringloop;
    cli = NULL;
    epmgr = NULL;
    ringloop = NULL;
}

void nfs_proxy_t::run_server(json11::Json cfg)
{
    if (nfs_port != -1)
    {
        // Create NFS socket and add it to epoll
        int nfs_socket = create_and_bind_socket(bind_address, nfs_port, 128, &listening_port);
        fcntl(nfs_socket, F_SETFL, fcntl(nfs_socket, F_GETFL, 0) | O_NONBLOCK);
        epmgr->tfd->set_fd_handler(nfs_socket, false, [this](int nfs_socket, int epoll_events)
        {
            if (epoll_events & EPOLLRDHUP)
            {
                fprintf(stderr, "Listening portmap socket disconnected, exiting\n");
                exit(1);
            }
            else
            {
                do_accept(nfs_socket);
            }
        });
        if (nfs_port_auto)
        {
            printf("Port: %d\n", listening_port);
        }
    }
    else
    {
        listening_port = nfs_rdma_port;
    }
    // Self-register portmap and NFS
    pmap.reg_ports.insert((portmap_id_t){
        .prog = PMAP_PROGRAM,
        .vers = PMAP_V2,
        .port = (unsigned)(portmap_enabled ? 111 : listening_port),
        .owner = "portmapper-service",
        .addr = portmap_enabled ? "0.0.0.0.0.111" : ("0.0.0.0.0."+std::to_string(listening_port)),
    });
    pmap.reg_ports.insert((portmap_id_t){
        .prog = PMAP_PROGRAM,
        .vers = PMAP_V3,
        .port = (unsigned)(portmap_enabled ? 111 : listening_port),
        .owner = "portmapper-service",
        .addr = portmap_enabled ? "0.0.0.0.0.111" : ("0.0.0.0.0."+std::to_string(listening_port)),
    });
    pmap.reg_ports.insert((portmap_id_t){
        .prog = NFS_PROGRAM,
        .vers = NFS_V3,
        .port = (unsigned)listening_port,
        .owner = "nfs-server",
        .addr = "0.0.0.0.0."+std::to_string(listening_port),
    });
    pmap.reg_ports.insert((portmap_id_t){
        .prog = MOUNT_PROGRAM,
        .vers = MOUNT_V3,
        .port = (unsigned)listening_port,
        .owner = "rpc.mountd",
        .addr = "0.0.0.0.0."+std::to_string(listening_port),
    });
    if (portmap_enabled)
    {
        // Create portmap socket and add it to epoll
        int portmap_socket = create_and_bind_socket(bind_address, 111, 128, NULL);
        fcntl(portmap_socket, F_SETFL, fcntl(portmap_socket, F_GETFL, 0) | O_NONBLOCK);
        epmgr->tfd->set_fd_handler(portmap_socket, false, [this](int portmap_socket, int epoll_events)
        {
            if (epoll_events & EPOLLRDHUP)
            {
                fprintf(stderr, "Listening portmap socket disconnected, exiting\n");
                exit(1);
            }
            else
            {
                do_accept(portmap_socket);
            }
        });
    }
    if (nfs_rdma_port)
    {
        rdma_context = create_rdma(bind_address, nfs_rdma_port, nfs_rdma_credit, nfs_rdma_max_send, nfs_rdma_alloc, nfs_rdma_gc);
    }
    if (mountpoint != "")
    {
        mount_fs();
    }
    if (cfg["foreground"].is_null())
    {
        daemonize();
    }
    if (pidfile != "")
    {
        write_pid();
    }
}

void nfs_proxy_t::watch_stats()
{
    assert(cli->st_cli.on_start_watcher_hook == NULL);
    cli->st_cli.on_start_watcher_hook = [this](http_co_t *etcd_watch_ws)
    {
        cli->st_cli.etcd_txn_slow(json11::Json::object {
            { "success", json11::Json::array {
                json11::Json::object {
                    { "request_range", json11::Json::object {
                        { "key", base64_encode(cli->st_cli.etcd_prefix+"/inode/stats/") },
                        { "range_end", base64_encode(cli->st_cli.etcd_prefix+"/inode/stats0") },
                    } }
                },
                json11::Json::object {
                    { "request_range", json11::Json::object {
                        { "key", base64_encode(cli->st_cli.etcd_prefix+"/pool/stats/") },
                        { "range_end", base64_encode(cli->st_cli.etcd_prefix+"/pool/stats0") },
                    } }
                },
            } },
        }, [this](std::string err, json11::Json res)
        {
            for (auto & rsp: res["responses"].array_items())
            {
                for (auto & item: rsp["response_range"]["kvs"].array_items())
                {
                    etcd_kv_t kv = cli->st_cli.parse_etcd_kv(item);
                    parse_stats(kv);
                }
            }
            if (cli->st_cli.etcd_watch_ws)
            {
                auto watch_rev = res["header"]["revision"].uint64_value()+1;
                http_post_message(cli->st_cli.etcd_watch_ws, WS_TEXT, json11::Json(json11::Json::object {
                    { "create_request", json11::Json::object {
                        { "key", base64_encode(cli->st_cli.etcd_prefix+"/inode/stats/") },
                        { "range_end", base64_encode(cli->st_cli.etcd_prefix+"/inode/stats0") },
                        { "start_revision", watch_rev },
                        { "watch_id", ETCD_INODE_STATS_WATCH_ID },
                        { "progress_notify", true },
                    } }
                }).dump());
                http_post_message(cli->st_cli.etcd_watch_ws, WS_TEXT, json11::Json(json11::Json::object {
                    { "create_request", json11::Json::object {
                        { "key", base64_encode(cli->st_cli.etcd_prefix+"/pool/stats/") },
                        { "range_end", base64_encode(cli->st_cli.etcd_prefix+"/pool/stats0") },
                        { "start_revision", watch_rev },
                        { "watch_id", ETCD_POOL_STATS_WATCH_ID },
                        { "progress_notify", true },
                    } }
                }).dump());
            }
        });
    };
    cli->st_cli.on_change_hook = [this, old_hook = cli->st_cli.on_change_hook](std::map<std::string, etcd_kv_t> & changes)
    {
        for (auto & p: changes)
        {
            parse_stats(p.second);
        }
    };
}

void nfs_proxy_t::parse_stats(etcd_kv_t & kv)
{
    auto & key = kv.key;
    if (key.substr(0, cli->st_cli.etcd_prefix.length()+13) == cli->st_cli.etcd_prefix+"/inode/stats/")
    {
        pool_id_t pool_id = 0;
        inode_t inode_num = 0;
        char null_byte = 0;
        int scanned = sscanf(key.c_str() + cli->st_cli.etcd_prefix.length()+13, "%u/%ju%c", &pool_id, &inode_num, &null_byte);
        if (scanned != 2 || !pool_id || pool_id >= POOL_ID_MAX)
        {
            fprintf(stderr, "Bad etcd key %s, ignoring\n", key.c_str());
        }
        else
        {
            inode_stats[INODE_WITH_POOL(pool_id, inode_num)] = kv.value;
        }
    }
    else if (key.substr(0, cli->st_cli.etcd_prefix.length()+12) == cli->st_cli.etcd_prefix+"/pool/stats/")
    {
        pool_id_t pool_id = 0;
        char null_byte = 0;
        int scanned = sscanf(key.c_str() + cli->st_cli.etcd_prefix.length()+12, "%u%c", &pool_id, &null_byte);
        if (scanned != 1 || !pool_id || pool_id >= POOL_ID_MAX)
        {
            fprintf(stderr, "Bad etcd key %s, ignoring\n", key.c_str());
        }
        else
        {
            pool_stats[pool_id] = kv.value;
        }
    }
}

void nfs_proxy_t::check_default_pool()
{
    if (default_pool == "")
    {
        if (cli->st_cli.pool_config.size() == 1)
        {
            auto pool_it = cli->st_cli.pool_config.begin();
            default_pool_id = pool_it->first;
            default_pool = pool_it->second.name;
        }
        else
        {
            fprintf(stderr, "There are %zu pools. Please select default pool with --pool option\n", cli->st_cli.pool_config.size());
            exit(1);
        }
    }
    else
    {
        for (auto & p: cli->st_cli.pool_config)
        {
            if (p.second.name == default_pool)
            {
                default_pool_id = p.first;
                break;
            }
        }
        if (!default_pool_id)
        {
            fprintf(stderr, "Pool %s is not found\n", default_pool.c_str());
            exit(1);
        }
    }
}

nfs_client_t *nfs_proxy_t::create_client()
{
    auto cli = new nfs_client_t();
    cli->parent = this;
    if (kvfs)
        nfs_kv_procs(cli);
    else
        nfs_block_procs(cli);
    for (auto & fn: pmap.proc_table)
        cli->proc_table.insert(fn);
    rpc_clients.insert(cli);
    return cli;
}

void nfs_proxy_t::do_accept(int listen_fd)
{
    struct sockaddr_storage addr;
    socklen_t addr_size = sizeof(addr);
    int nfs_fd = 0;
    while ((nfs_fd = accept(listen_fd, (struct sockaddr *)&addr, &addr_size)) >= 0)
    {
        if (trace)
            fprintf(stderr, "New client %d: connection from %s\n", nfs_fd, addr_to_string(addr).c_str());
        active_connections++;
        fcntl(nfs_fd, F_SETFL, fcntl(nfs_fd, F_GETFL, 0) | O_NONBLOCK);
        int one = 1;
        setsockopt(nfs_fd, SOL_TCP, TCP_NODELAY, &one, sizeof(one));
        auto cli = this->create_client();
        cli->nfs_fd = nfs_fd;
        epmgr->tfd->set_fd_handler(nfs_fd, true, [cli](int nfs_fd, int epoll_events)
        {
            // Handle incoming event
            if (epoll_events & EPOLLRDHUP)
            {
                if (cli->parent->trace)
                    fprintf(stderr, "Client %d disconnected\n", nfs_fd);
                cli->stop();
                return;
            }
            cli->epoll_events |= epoll_events;
            if (epoll_events & EPOLLIN)
            {
                // Something is available for reading
                cli->submit_read(0);
            }
            if (epoll_events & EPOLLOUT)
            {
                cli->submit_send();
            }
        });
    }
    if (nfs_fd < 0 && errno != EAGAIN)
    {
        fprintf(stderr, "Failed to accept connection: %s\n", strerror(errno));
        exit(1);
    }
}

// FIXME Move these functions to "rpc_context"
void nfs_client_t::select_read_buffer(unsigned wanted_size)
{
    if (free_buffers.size())
    {
        auto & b = free_buffers.back();
        if (b.size < wanted_size)
        {
            cur_buffer = {
                .buf = (uint8_t*)malloc_or_die(wanted_size),
                .size = wanted_size,
            };
        }
        else
        {
            cur_buffer = {
                .buf = b.buf,
                .size = b.size,
            };
        }
        free_buffers.pop_back();
    }
    else
    {
        unsigned sz = RPC_INIT_BUF_SIZE;
        if (sz < wanted_size)
        {
            sz = wanted_size;
        }
        cur_buffer = {
            .buf = (uint8_t*)malloc_or_die(sz),
            .size = sz,
        };
    }
}

void nfs_client_t::submit_read(unsigned wanted_size)
{
    if (read_msg.msg_iovlen)
    {
        return;
    }
    io_uring_sqe* sqe = parent->ringloop->get_sqe();
    if (!sqe)
    {
        read_msg.msg_iovlen = 0;
        parent->ringloop->wakeup();
        return;
    }
    if (!cur_buffer.buf || cur_buffer.size <= cur_buffer.read_pos)
    {
        assert(!wanted_size);
        if (cur_buffer.buf)
        {
            if (cur_buffer.refs > 0)
            {
                used_buffers[cur_buffer.buf] = (rpc_used_buffer_t){
                    .size = cur_buffer.size,
                    .refs = cur_buffer.refs,
                };
            }
            else
            {
                free_buffers.push_back((rpc_free_buffer_t){
                    .buf = cur_buffer.buf,
                    .size = cur_buffer.size,
                });
            }
        }
        select_read_buffer(wanted_size);
    }
    assert(wanted_size <= cur_buffer.size-cur_buffer.read_pos);
    read_iov = {
        .iov_base = cur_buffer.buf+cur_buffer.read_pos,
        .iov_len = wanted_size ? wanted_size : cur_buffer.size-cur_buffer.read_pos,
    };
    read_msg.msg_iov = &read_iov;
    read_msg.msg_iovlen = 1;
    ring_data_t* data = ((ring_data_t*)sqe->user_data);
    data->callback = [this](ring_data_t *data) { handle_read(data->res); };
    my_uring_prep_recvmsg(sqe, nfs_fd, &read_msg, 0);
    refs++;
}

void nfs_client_t::handle_read(int result)
{
    read_msg.msg_iovlen = 0;
    if (deref())
        return;
    if (result <= 0 && result != -EAGAIN && result != -EINTR && result != -ECANCELED)
    {
        printf("Failed read from client %d: %d (%s)\n", nfs_fd, result, strerror(-result));
        stop();
        return;
    }
    if (result > 0)
    {
        cur_buffer.read_pos += result;
        assert(cur_buffer.read_pos <= cur_buffer.size);
        // Try to parse incoming RPC messages
        uint8_t *data = cur_buffer.buf + cur_buffer.parsed_pos;
        unsigned left = cur_buffer.read_pos - cur_buffer.parsed_pos;
        while (left > 0)
        {
            // Assemble all fragments
            unsigned fragments = 0;
            uint32_t wanted = 0;
            while (1)
            {
                fragments++;
                wanted += 4;
                if (left < wanted)
                {
                    break;
                }
                // FIXME: Limit message size
                uint32_t frag_size = be32toh(*(uint32_t*)(data + wanted - 4));
                wanted += (frag_size & 0x7FFFFFFF);
                if (left < wanted || (frag_size & 0x80000000))
                {
                    break;
                }
            }
            if (left >= wanted)
            {
                if (fragments > 1)
                {
                    // Merge fragments. Fragmented messages are probably not that common,
                    // so it's probably fine to do an additional memory copy
                    unsigned frag_offset = 8+be32toh(*(uint32_t*)(data));
                    unsigned dest_offset = 4+be32toh(*(uint32_t*)(data));
                    unsigned frag_num = 1;
                    while (frag_num < fragments)
                    {
                        uint32_t frag_size = be32toh(*(uint32_t*)(data + frag_offset - 4)) & 0x7FFFFFFF;
                        memmove(data + dest_offset, data + frag_offset, frag_size);
                        frag_offset += 4+frag_size;
                        dest_offset += frag_size;
                        frag_num++;
                    }
                }
                // Increase client refcount while the RPC call is being processed
                refs++;
                // Handle full message
                int referenced = handle_rpc_message(cur_buffer.buf, data+4, wanted-4*fragments);
                cur_buffer.refs += referenced ? 1 : 0;
                cur_buffer.parsed_pos += 4+wanted-4*fragments;
                data += wanted;
                left -= wanted;
            }
            else if (cur_buffer.size >= (data - cur_buffer.buf + wanted))
            {
                // Read the tail and come back
                submit_read(wanted-left);
                return;
            }
            else
            {
                // No place to put the whole tail
                if (cur_buffer.refs > 0)
                {
                    used_buffers[cur_buffer.buf] = (rpc_used_buffer_t){
                        .size = cur_buffer.size,
                        .refs = cur_buffer.refs,
                    };
                    select_read_buffer(wanted);
                    memcpy(cur_buffer.buf, data, left);
                }
                else if (cur_buffer.size < wanted)
                {
                    uint8_t *old_buf = cur_buffer.buf;
                    select_read_buffer(wanted);
                    memcpy(cur_buffer.buf, data, left);
                    free(old_buf);
                }
                else
                {
                    memmove(cur_buffer.buf, data, left);
                }
                cur_buffer.read_pos = left;
                cur_buffer.parsed_pos = 0;
                // Restart from the beginning
                submit_read(wanted-left);
                return;
            }
        }
    }
    submit_read(0);
}

void nfs_client_t::submit_send()
{
    if (write_msg.msg_iovlen || !send_list.size())
    {
        return;
    }
    io_uring_sqe* sqe = parent->ringloop->get_sqe();
    if (!sqe)
    {
        write_msg.msg_iovlen = 0;
        parent->ringloop->wakeup();
        return;
    }
    write_msg.msg_iov = send_list.data();
    write_msg.msg_iovlen = send_list.size() < IOV_MAX ? send_list.size() : IOV_MAX;
    ring_data_t* data = ((ring_data_t*)sqe->user_data);
    data->callback = [this](ring_data_t *data) { handle_send(data->res); };
    my_uring_prep_sendmsg(sqe, nfs_fd, &write_msg, 0);
    refs++;
}

bool nfs_client_t::deref()
{
    refs--;
    if (stopped && refs <= 0)
    {
        stop();
        return true;
    }
    return false;
}

void nfs_client_t::stop()
{
    stopped = true;
    if (refs <= 0)
    {
#ifdef WITH_RDMACM
        destroy_rdma_conn();
#endif
        auto parent = this->parent;
        parent->rpc_clients.erase(this);
        parent->active_connections--;
        if (nfs_fd >= 0)
        {
            parent->epmgr->tfd->set_fd_handler(nfs_fd, true, NULL);
            close(nfs_fd);
        }
        delete this;
        parent->check_exit();
    }
}

void nfs_client_t::handle_send(int result)
{
    write_msg.msg_iovlen = 0;
    if (deref())
        return;
    if (result <= 0 && result != -EAGAIN && result != -EINTR)
    {
        printf("Failed send to client %d: %d (%s)\n", nfs_fd, result, strerror(-result));
        stop();
        return;
    }
    if (result > 0)
    {
        int done = 0;
        while (result > 0 && done < send_list.size())
        {
            iovec & iov = send_list[done];
            if (iov.iov_len <= result)
            {
                auto rop = outbox[done];
                if (rop)
                {
                    // Reply fully sent
                    parent->free_xdr(rop->xdrs);
                    if (rop->buffer && rop->referenced)
                    {
                        // Dereference the buffer
                        if (rop->buffer == cur_buffer.buf)
                        {
                            cur_buffer.refs--;
                        }
                        else
                        {
                            auto & ub = used_buffers.at(rop->buffer);
                            assert(ub.refs > 0);
                            ub.refs--;
                            if (ub.refs == 0)
                            {
                                // FIXME Maybe put free_buffers into parent
                                free_buffers.push_back((rpc_free_buffer_t){
                                    .buf = (uint8_t*)rop->buffer,
                                    .size = ub.size,
                                });
                                used_buffers.erase(rop->buffer);
                            }
                        }
                    }
                    free(rop);
                    if (deref())
                    {
                        return;
                    }
                }
                result -= iov.iov_len;
                done++;
            }
            else
            {
                iov.iov_len -= result;
                iov.iov_base = (uint8_t*)iov.iov_base + result;
                break;
            }
        }
        if (done > 0)
        {
            send_list.erase(send_list.begin(), send_list.begin()+done);
            outbox.erase(outbox.begin(), outbox.begin()+done);
        }
        if (next_send_list.size())
        {
            send_list.insert(send_list.end(), next_send_list.begin(), next_send_list.end());
            outbox.insert(outbox.end(), next_outbox.begin(), next_outbox.end());
            next_send_list.clear();
            next_outbox.clear();
        }
        if (outbox.size() > 0)
        {
            submit_send();
        }
    }
}

void rpc_queue_reply(rpc_op_t *rop)
{
    nfs_client_t *self = (nfs_client_t*)rop->client;
    int r = xdr_encode(rop->xdrs, (xdrproc_t)xdr_rpc_msg, &rop->out_msg);
    assert(r);
    if (rop->reply_fn != NULL)
    {
        r = xdr_encode(rop->xdrs, rop->reply_fn, rop->reply);
        assert(r);
    }
#ifdef WITH_RDMACM
    if (!self->rdma_conn)
#endif
    {
        iovec *iov_list = NULL;
        unsigned iov_count = 0;
        xdr_encode_finish(rop->xdrs, &iov_list, &iov_count);
        assert(iov_count > 0);
        rop->reply_marker = 0;
        for (unsigned i = 0; i < iov_count; i++)
        {
            rop->reply_marker += iov_list[i].iov_len;
        }
        rop->reply_marker = htobe32(rop->reply_marker | 0x80000000);
        auto & to_send_list = self->write_msg.msg_iovlen ? self->next_send_list : self->send_list;
        auto & to_outbox = self->write_msg.msg_iovlen ? self->next_outbox : self->outbox;
        to_send_list.push_back((iovec){ .iov_base = &rop->reply_marker, .iov_len = 4 });
        to_outbox.push_back(NULL);
        for (unsigned i = 0; i < iov_count; i++)
        {
            to_send_list.push_back(iov_list[i]);
            to_outbox.push_back(NULL);
        }
        to_outbox[to_outbox.size()-1] = rop;
        self->submit_send();
    }
#ifdef WITH_RDMACM
    else
    {
        self->rdma_queue_reply(rop);
    }
#endif
}

XDR *nfs_proxy_t::get_xdr()
{
    // Take an XDR object from the pool
    XDR *xdrs;
    if (xdr_pool.size())
    {
        xdrs = xdr_pool.back();
        xdr_pool.pop_back();
    }
    else
    {
        xdrs = xdr_create();
    }
    return xdrs;
}

void nfs_proxy_t::free_xdr(XDR *xdrs)
{
    xdr_reset(xdrs);
    xdr_pool.push_back(xdrs);
}

int nfs_client_t::handle_rpc_message(void *base_buf, void *msg_buf, uint32_t msg_len)
{
    XDR *xdrs = parent->get_xdr();
    // Decode the RPC header
    char inmsg_data[sizeof(rpc_msg)];
    rpc_msg *inmsg = (rpc_msg*)&inmsg_data;
    if (!xdr_decode(xdrs, msg_buf, msg_len, (xdrproc_t)xdr_rpc_msg, inmsg))
    {
        // Invalid message, ignore it
        parent->free_xdr(xdrs);
        return 0;
    }
    if (inmsg->body.dir != RPC_CALL)
    {
        // Reply sent to the server? Strange thing. Also ignore it
        parent->free_xdr(xdrs);
        return 0;
    }
    if (inmsg->body.cbody.rpcvers != RPC_MSG_VERSION)
    {
        // Bad RPC version
        rpc_op_t *rop = (rpc_op_t*)malloc_or_die(sizeof(rpc_op_t));
        u_int x = RPC_MSG_VERSION;
        *rop = (rpc_op_t){
            .client = this,
            .xdrs = xdrs,
            .out_msg = (rpc_msg){
                .xid = inmsg->xid,
                .body = (rpc_msg_body){
                    .dir = RPC_REPLY,
                    .rbody = (rpc_reply_body){
                        .stat = RPC_MSG_DENIED,
                        .rreply = (rpc_rejected_reply){
                            .stat = RPC_MISMATCH,
                            .mismatch_info = (rpc_mismatch_info){
                                // Without at least one reference to a non-constant value (local variable or something else),
                                // with gcc 8 we get "internal compiler error: side-effects element in no-side-effects CONSTRUCTOR" here
                                // FIXME: get rid of this after raising compiler requirement
                                .min_version = x,
                                .max_version = RPC_MSG_VERSION,
                            },
                        },
                    },
                },
            },
        };
        rpc_queue_reply(rop);
        // Incoming buffer isn't needed to handle request, so return 0
        return 0;
    }
    auto rop = create_rpc_op(xdrs, base_buf, inmsg, NULL);
    if (!rop)
    {
        // No such procedure
        return 0;
    }
    return handle_rpc_op(rop);
}

rpc_op_t *nfs_client_t::create_rpc_op(XDR *xdrs, void *buffer, rpc_msg *inmsg, rdma_msg *rmsg)
{
    // Find decoder for the request
    auto proc_it = proc_table.find((rpc_service_proc_t){
        .prog = inmsg->body.cbody.prog,
        .vers = inmsg->body.cbody.vers,
        .proc = inmsg->body.cbody.proc,
    });
    if (proc_it == proc_table.end())
    {
        // Procedure not implemented
        uint32_t min_vers = 0, max_vers = 0;
        auto prog_it = proc_table.lower_bound((rpc_service_proc_t){
            .prog = inmsg->body.cbody.prog,
        });
        if (prog_it != proc_table.end())
        {
            min_vers = prog_it->vers;
            auto max_vers_it = proc_table.lower_bound((rpc_service_proc_t){
                .prog = inmsg->body.cbody.prog+1,
            });
            assert(max_vers_it != proc_table.begin());
            max_vers_it--;
            assert(max_vers_it->prog == inmsg->body.cbody.prog);
            max_vers = max_vers_it->vers;
        }
        rpc_op_t *rop = (rpc_op_t*)malloc_or_die(sizeof(rpc_op_t));
        *rop = (rpc_op_t){
            .client = this,
            .buffer = buffer,
            .xdrs = xdrs,
            .out_msg = (rpc_msg){
                .xid = inmsg->xid,
                .body = (rpc_msg_body){
                    .dir = RPC_REPLY,
                    .rbody = (rpc_reply_body){
                        .stat = RPC_MSG_ACCEPTED,
                        .areply = (rpc_accepted_reply){
                            .reply_data = (rpc_accepted_reply_body){
                                .stat = (min_vers == 0
                                    ? RPC_PROG_UNAVAIL
                                    : (min_vers <= inmsg->body.cbody.vers &&
                                        max_vers >= inmsg->body.cbody.vers
                                        ? RPC_PROC_UNAVAIL
                                        : RPC_PROG_MISMATCH)),
                                .mismatch_info = (rpc_mismatch_info){ .min_version = min_vers, .max_version = max_vers },
                            },
                        },
                    },
                },
            },
        };
        // FIXME: malloc and avoid copy?
        memcpy(&rop->in_msg, inmsg, sizeof(rpc_msg));
        if (rmsg)
        {
            memcpy(&rop->in_rdma_msg, rmsg, sizeof(rdma_msg));
        }
        rpc_queue_reply(rop);
        // Incoming buffer isn't needed to handle request, so return 0
        return NULL;
    }
    // Allocate memory
    rpc_op_t *rop = (rpc_op_t*)malloc_or_die(
        sizeof(rpc_op_t) + proc_it->req_size + proc_it->resp_size
    );
    rpc_reply_stat x = RPC_MSG_ACCEPTED;
    *rop = (rpc_op_t){
        .client = this,
        .buffer = buffer,
        .xdrs = xdrs,
        .out_msg = (rpc_msg){
            .xid = inmsg->xid,
            .body = (rpc_msg_body){
                .dir = RPC_REPLY,
                .rbody = (rpc_reply_body){
                    // Without at least one reference to a non-constant value (local variable or something else),
                    // with gcc 8 we get "internal compiler error: side-effects element in no-side-effects CONSTRUCTOR" here
                    // FIXME: get rid of this after raising compiler requirement
                    .stat = x,
                },
            },
        },
        .request = ((uint8_t*)rop) + sizeof(rpc_op_t),
        .reply = ((uint8_t*)rop) + sizeof(rpc_op_t) + proc_it->req_size,
    };
    // FIXME: malloc and avoid copy?
    memcpy(&rop->in_msg, inmsg, sizeof(rpc_msg));
    if (rmsg)
    {
        memcpy(&rop->in_rdma_msg, rmsg, sizeof(rdma_msg));
    }
    return rop;
}

int nfs_client_t::handle_rpc_op(rpc_op_t *rop)
{
    // Try to decode the request
    // req_fn may be NULL, that means function has no arguments
    auto proc_it = proc_table.find((rpc_service_proc_t){
        .prog = rop->in_msg.body.cbody.prog,
        .vers = rop->in_msg.body.cbody.vers,
        .proc = rop->in_msg.body.cbody.proc,
    });
    if (proc_it == proc_table.end() || proc_it->req_fn && !proc_it->req_fn(rop->xdrs, rop->request))
    {
        // Invalid request
        rop->out_msg.body.rbody.areply.reply_data.stat = RPC_GARBAGE_ARGS;
        rpc_queue_reply(rop);
        // Incoming buffer isn't needed to handle request, so return 0
        return 0;
    }
    rop->out_msg.body.rbody.areply.reply_data.stat = RPC_SUCCESS;
    rop->reply_fn = proc_it->resp_fn;
    rop->referenced = 0;
    int ref = proc_it->handler_fn(proc_it->opaque, rop);
    if (ref)
        rop->referenced = 1;
    return ref;
}

void *nfs_client_t::malloc_or_rdma(rpc_op_t *rop, size_t size)
{
#ifdef WITH_RDMACM
    if (!rdma_conn)
    {
#endif
        void *buf = malloc_or_die(size);
        xdr_add_malloc(rop->xdrs, buf);
        return buf;
#ifdef WITH_RDMACM
    }
    void *buf = rdma_malloc(size);
    xdr_set_rdma_chunk(rop->xdrs, buf);
    return buf;
#endif
}

void nfs_client_t::free_or_rdma(rpc_op_t *rop, void *buf)
{
#ifdef WITH_RDMACM
    if (!rdma_conn)
    {
#endif
        xdr_del_malloc(rop->xdrs, buf);
        free(buf);
#ifdef WITH_RDMACM
    }
    else
    {
        xdr_set_rdma_chunk(rop->xdrs, NULL);
        rdma_free(buf);
    }
#endif
}

void nfs_proxy_t::daemonize()
{
    // Stop all clients because client I/O sometimes breaks during daemonize
    // I.e. the new process stops receiving events on the old FD
    // It doesn't happen if we call sleep(1) here, but we don't want to call sleep(1)...
    for (auto & cli: rpc_clients)
        cli->stop();
    if (fork())
        exit(0);
    setsid();
    if (fork())
        exit(0);
    if (chdir("/") != 0)
        fprintf(stderr, "Warning: Failed to chdir into /\n");
    close(0);
    close(1);
    close(2);
    open("/dev/null", O_RDONLY);
    open(logfile.c_str(), O_WRONLY|O_APPEND|O_CREAT, 0666);
    open(logfile.c_str(), O_WRONLY|O_APPEND|O_CREAT, 0666);
}

void nfs_proxy_t::write_pid()
{
    int fd = open(pidfile.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0666);
    if (fd < 0)
    {
        fprintf(stderr, "Failed to create pid file %s: %s (code %d)\n", pidfile.c_str(), strerror(errno), errno);
        return;
    }
    auto pid = std::to_string(getpid());
    if (write(fd, pid.c_str(), pid.size()) < 0)
    {
        fprintf(stderr, "Failed to write pid to %s: %s (code %d)\n", pidfile.c_str(), strerror(errno), errno);
    }
    close(fd);
}

static pid_t wanted_pid = 0;
static bool child_finished = false;
static int child_status = -1;

void single_child_handler(int signal)
{
    child_finished = true;
    waitpid(wanted_pid, &child_status, WNOHANG);
}

void nfs_proxy_t::mount_fs()
{
    check_already_mounted();
    signal(SIGCHLD, single_child_handler);
    auto pid = fork();
    if (pid < 0)
    {
        fprintf(stderr, "Failed to fork: %s (code %d)\n", strerror(errno), errno);
        exit(1);
    }
    if (pid > 0)
    {
        // Parent - loop and wait until child finishes
        wanted_pid = pid;
        exit_on_umount = false;
        while (!child_finished)
        {
            ringloop->loop();
            ringloop->wait();
        }
        if (!WIFEXITED(child_status) || WEXITSTATUS(child_status) != 0)
        {
            // Mounting failed
            exit(1);
        }
        if (fsname != "")
            fprintf(stderr, "Successfully mounted VitastorFS %s at %s\n", fsname.c_str(), mountpoint.c_str());
        else
            fprintf(stderr, "Successfully mounted Vitastor pseudo-FS at %s\n", mountpoint.c_str());
        finished = false;
        exit_on_umount = true;
    }
    else
    {
        // Child
        std::string src = ("localhost:"+export_root);
        std::string opts = ("port="+std::to_string(listening_port)+",mountport="+std::to_string(listening_port)+",nfsvers=3,nolock,tcp");
        bool hard = false, async = false;
        for (auto & opt: explode(",", mountopts, true))
        {
            if (opt == "hard")
                hard = true;
            else if (opt == "async")
                async = true;
            else if (opt.substr(0, 4) != "port" && opt.substr(0, 9) != "mountport" &&
                opt.substr(0, 7) != "nfsvers" && opt.substr(0, 5) != "proto" &&
                opt != "udp" && opt != "tcp" && opt != "rdma")
            {
                opts += ","+opt;
            }
        }
        if (!hard)
            opts += ",soft";
        if (!async)
            opts += ",sync";
        const char *args[] = { "mount", src.c_str(), mountpoint.c_str(), "-o", opts.c_str(), NULL };
        execvp("mount", (char* const*)args);
        fprintf(stderr, "Failed to run mount %s %s -o %s: %s (code %d)\n",
            src.c_str(), mountpoint.c_str(), opts.c_str(), strerror(errno), errno);
        exit(1);
    }
}

void nfs_proxy_t::check_already_mounted()
{
    std::string realpoint = realpath_str(mountpoint, false);
    if (realpoint == "")
    {
        return;
    }
    std::string mountstr = read_file("/proc/mounts");
    if (mountstr == "")
    {
        return;
    }
    auto mounts = explode("\n", mountstr, true);
    for (auto & str: mounts)
    {
        auto mnt = explode(" ", str, true);
        if (mnt.size() >= 2 && mnt[1] == realpoint)
        {
            fprintf(stderr, "%s is already mounted\n", mountpoint.c_str());
            exit(1);
        }
    }
}

void nfs_proxy_t::check_exit()
{
    if (active_connections || !exit_on_umount)
    {
        return;
    }
    fprintf(stderr, "All active NFS connections are closed, checking /proc/mounts\n");
    std::string mountstr = read_file("/proc/mounts");
    if (mountstr == "")
    {
        return;
    }
    auto port_opt = "port="+std::to_string(listening_port);
    auto mountport_opt = "mountport="+std::to_string(listening_port);
    auto mounts = explode("\n", mountstr, true);
    for (auto & str: mounts)
    {
        auto opts = explode(" ", str, true);
        if (opts[2].size() >= 3 && opts[2].substr(0, 3) == "nfs" && opts.size() >= 4)
        {
            opts = explode(",", opts[3], true);
            bool port_found = false;
            bool addr_found = false;
            for (auto & opt: opts)
            {
                if (opt == port_opt || opt == mountport_opt)
                    port_found = true;
                if (opt == "addr=127.0.0.1" || opt == "mountaddr=127.0.0.1")
                    addr_found = true;
            }
            if (port_found && addr_found)
            {
                // OK, do not unmount
                fprintf(stderr, "NFS mount to 127.0.0.1:%d still active, leaving server active\n", listening_port);
                return;
            }
        }
    }
    fprintf(stderr, "NFS mount to 127.0.0.1:%d not found, exiting\n", listening_port);
    // Not found, unmount
    finished = true;
}

int main(int narg, const char *args[])
{
    setvbuf(stdout, NULL, _IONBF, 0);
    setvbuf(stderr, NULL, _IONBF, 0);
    exe_name = args[0];
    nfs_proxy_t *p = new nfs_proxy_t();
    p->run(nfs_proxy_t::parse_args(narg, args));
    delete p;
    return 0;
}
