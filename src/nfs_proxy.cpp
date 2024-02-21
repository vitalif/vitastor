// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// Simplified NFS proxy
// Presents all images as files
// Keeps image/file list in memory and is thus unsuitable for a large number of files

#define _XOPEN_SOURCE
#include <limits.h>

#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <unistd.h>
#include <fcntl.h>
//#include <signal.h>

#include "nfs/nfs.h"
#include "nfs/rpc.h"
#include "nfs/portmap.h"

#include "addr_util.h"
#include "str_util.h"
#include "nfs_proxy.h"
#include "http_client.h"
#include "cli.h"

#define ETCD_INODE_STATS_WATCH_ID 101
#define ETCD_POOL_STATS_WATCH_ID 102

const char *exe_name = NULL;

nfs_proxy_t::~nfs_proxy_t()
{
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

json11::Json::object nfs_proxy_t::parse_args(int narg, const char *args[])
{
    json11::Json::object cfg;
    for (int i = 1; i < narg; i++)
    {
        if (!strcmp(args[i], "-h") || !strcmp(args[i], "--help"))
        {
            printf(
                "Vitastor NFS 3.0 proxy\n"
                "(c) Vitaliy Filippov, 2021-2022 (VNPL-1.1)\n"
                "\n"
                "USAGE:\n"
                "  %s [STANDARD OPTIONS] [OTHER OPTIONS]\n"
                "  --subdir <DIR>    export images prefixed <DIR>/ (default empty - export all images)\n"
                "  --portmap 0       do not listen on port 111 (portmap/rpcbind, requires root)\n"
                "  --bind <IP>       bind service to <IP> address (default 0.0.0.0)\n"
                "  --nfspath <PATH>  set NFS export path to <PATH> (default is /)\n"
                "  --port <PORT>     use port <PORT> for NFS services (default is 2049)\n"
                "  --pool <POOL>     use <POOL> as default pool for new files (images)\n"
                "  --foreground 1    stay in foreground, do not daemonize\n"
                "\n"
                "NFS proxy is stateless if you use immediate_commit=all in your cluster and if\n"
                "you do not use client_enable_writeback=true, so you can freely use multiple\n"
                "NFS proxies with L3 load balancing in this case.\n"
                "\n"
                "Example start and mount commands for a custom NFS port:\n"
                "  %s --etcd_address 192.168.5.10:2379 --portmap 0 --port 2050 --pool testpool\n"
                "  mount localhost:/ /mnt/ -o port=2050,mountport=2050,nfsvers=3,soft,nolock,tcp\n",
                exe_name, exe_name
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

void nfs_proxy_t::run(json11::Json cfg)
{
    timespec tv;
    clock_gettime(CLOCK_REALTIME, &tv);
    srand48(tv.tv_sec*1000000000 + tv.tv_nsec);
    server_id = (uint64_t)lrand48() | ((uint64_t)lrand48() << 31) | ((uint64_t)lrand48() << 62);
    // Parse options
    bind_address = cfg["bind"].string_value();
    if (bind_address == "")
        bind_address = "0.0.0.0";
    default_pool = cfg["pool"].as_string();
    portmap_enabled = !json_is_false(cfg["portmap"]);
    nfs_port = cfg["port"].uint64_value() & 0xffff;
    if (!nfs_port)
        nfs_port = 2049;
    export_root = cfg["nfspath"].string_value();
    if (!export_root.size())
        export_root = "/";
    name_prefix = cfg["subdir"].string_value();
    {
        int e = name_prefix.size();
        while (e > 0 && name_prefix[e-1] == '/')
            e--;
        int s = 0;
        while (s < e && name_prefix[s] == '/')
            s++;
        name_prefix = name_prefix.substr(s, e-s);
        if (name_prefix.size())
            name_prefix += "/";
    }
    if (cfg["client_writeback_allowed"].is_null())
    {
        // NFS is always aware of fsync, so we allow write-back cache
        // by default if it's enabled
        auto obj = cfg.object_items();
        obj["client_writeback_allowed"] = true;
        cfg = obj;
    }
    // Create client
    ringloop = new ring_loop_t(RINGLOOP_DEFAULT_SIZE);
    epmgr = new epoll_manager_t(ringloop);
    cli = new cluster_client_t(ringloop, epmgr->tfd, cfg);
    cmd = new cli_tool_t();
    cmd->ringloop = ringloop;
    cmd->epmgr = epmgr;
    cmd->cli = cli;
    // We need inode name hashes for NFS handles to remain stateless and <= 64 bytes long
    dir_info[""] = (nfs_dir_t){
        .id = 1,
        .mod_rev = 0,
    };
    clock_gettime(CLOCK_REALTIME, &dir_info[""].mtime);
    watch_stats();
    assert(cli->st_cli.on_inode_change_hook == NULL);
    cli->st_cli.on_inode_change_hook = [this](inode_t changed_inode, bool removed)
    {
        auto inode_cfg_it = cli->st_cli.inode_config.find(changed_inode);
        if (inode_cfg_it == cli->st_cli.inode_config.end())
        {
            return;
        }
        auto & inode_cfg = inode_cfg_it->second;
        std::string full_name = inode_cfg.name;
        if (name_prefix != "" && full_name.substr(0, name_prefix.size()) != name_prefix)
        {
            return;
        }
        // Calculate directory modification time and revision (used as "cookie verifier")
        timespec now;
        clock_gettime(CLOCK_REALTIME, &now);
        dir_info[""].mod_rev = dir_info[""].mod_rev < inode_cfg.mod_revision ? inode_cfg.mod_revision : dir_info[""].mod_rev;
        dir_info[""].mtime = now;
        int pos = full_name.find('/', name_prefix.size());
        while (pos >= 0)
        {
            std::string dir = full_name.substr(0, pos);
            auto & dinf = dir_info[dir];
            if (!dinf.id)
                dinf.id = next_dir_id++;
            dinf.mod_rev = dinf.mod_rev < inode_cfg.mod_revision ? inode_cfg.mod_revision : dinf.mod_rev;
            dinf.mtime = now;
            dir_by_hash["S"+base64_encode(sha256(dir))] = dir;
            pos = full_name.find('/', pos+1);
        }
        // Alter inode_by_hash
        if (removed)
        {
            auto ino_it = hash_by_inode.find(changed_inode);
            if (ino_it != hash_by_inode.end())
            {
                inode_by_hash.erase(ino_it->second);
                hash_by_inode.erase(ino_it);
            }
        }
        else
        {
            std::string hash = "S"+base64_encode(sha256(full_name));
            auto hbi_it = hash_by_inode.find(changed_inode);
            if (hbi_it != hash_by_inode.end() && hbi_it->second != hash)
            {
                // inode had a different name, remove old hash=>inode pointer
                inode_by_hash.erase(hbi_it->second);
            }
            inode_by_hash[hash] = changed_inode;
            hash_by_inode[changed_inode] = hash;
        }
    };
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
    // Self-register portmap and NFS
    pmap.reg_ports.insert((portmap_id_t){
        .prog = PMAP_PROGRAM,
        .vers = PMAP_V2,
        .port = portmap_enabled ? 111 : nfs_port,
        .owner = "portmapper-service",
        .addr = portmap_enabled ? "0.0.0.0.0.111" : ("0.0.0.0.0."+std::to_string(nfs_port)),
    });
    pmap.reg_ports.insert((portmap_id_t){
        .prog = PMAP_PROGRAM,
        .vers = PMAP_V3,
        .port = portmap_enabled ? 111 : nfs_port,
        .owner = "portmapper-service",
        .addr = portmap_enabled ? "0.0.0.0.0.111" : ("0.0.0.0.0."+std::to_string(nfs_port)),
    });
    pmap.reg_ports.insert((portmap_id_t){
        .prog = NFS_PROGRAM,
        .vers = NFS_V3,
        .port = nfs_port,
        .owner = "nfs-server",
        .addr = "0.0.0.0.0."+std::to_string(nfs_port),
    });
    pmap.reg_ports.insert((portmap_id_t){
        .prog = MOUNT_PROGRAM,
        .vers = MOUNT_V3,
        .port = nfs_port,
        .owner = "rpc.mountd",
        .addr = "0.0.0.0.0."+std::to_string(nfs_port),
    });
    // Create NFS socket and add it to epoll
    int nfs_socket = create_and_bind_socket(bind_address, nfs_port, 128, NULL);
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
    if (cfg["foreground"].is_null())
    {
        daemonize();
    }
    while (true)
    {
        ringloop->loop();
        ringloop->wait();
    }
    // Destroy the client
    cli->flush();
    delete cli;
    delete epmgr;
    delete ringloop;
    cli = NULL;
    epmgr = NULL;
    ringloop = NULL;
}

void nfs_proxy_t::watch_stats()
{
    assert(cli->st_cli.on_start_watcher_hook == NULL);
    cli->st_cli.on_start_watcher_hook = [this](http_co_t *etcd_watch_ws)
    {
        http_post_message(etcd_watch_ws, WS_TEXT, json11::Json(json11::Json::object {
            { "create_request", json11::Json::object {
                { "key", base64_encode(cli->st_cli.etcd_prefix+"/inode/stats/") },
                { "range_end", base64_encode(cli->st_cli.etcd_prefix+"/inode/stats0") },
                { "start_revision", cli->st_cli.etcd_watch_revision },
                { "watch_id", ETCD_INODE_STATS_WATCH_ID },
                { "progress_notify", true },
            } }
        }).dump());
        http_post_message(etcd_watch_ws, WS_TEXT, json11::Json(json11::Json::object {
            { "create_request", json11::Json::object {
                { "key", base64_encode(cli->st_cli.etcd_prefix+"/pool/stats/") },
                { "range_end", base64_encode(cli->st_cli.etcd_prefix+"/pool/stats0") },
                { "start_revision", cli->st_cli.etcd_watch_revision },
                { "watch_id", ETCD_POOL_STATS_WATCH_ID },
                { "progress_notify", true },
            } }
        }).dump());
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
        if (scanned != 2 || !pool_id || pool_id >= POOL_ID_MAX || !inode_num)
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
            default_pool = cli->st_cli.pool_config.begin()->second.name;
            default_pool_id = cli->st_cli.pool_config.begin()->first;
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

void nfs_proxy_t::do_accept(int listen_fd)
{
    struct sockaddr_storage addr;
    socklen_t addr_size = sizeof(addr);
    int nfs_fd = 0;
    while ((nfs_fd = accept(listen_fd, (struct sockaddr *)&addr, &addr_size)) >= 0)
    {
        fprintf(stderr, "New client %d: connection from %s\n", nfs_fd, addr_to_string(addr).c_str());
        fcntl(nfs_fd, F_SETFL, fcntl(nfs_fd, F_GETFL, 0) | O_NONBLOCK);
        int one = 1;
        setsockopt(nfs_fd, SOL_TCP, TCP_NODELAY, &one, sizeof(one));
        auto cli = new nfs_client_t();
        cli->parent = this;
        cli->nfs_fd = nfs_fd;
        for (auto & fn: pmap.proc_table)
        {
            cli->proc_table.insert(fn);
        }
        epmgr->tfd->set_fd_handler(nfs_fd, true, [cli](int nfs_fd, int epoll_events)
        {
            // Handle incoming event
            if (epoll_events & EPOLLRDHUP)
            {
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
    if (result <= 0 && result != -EAGAIN && result != -EINTR)
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
        submit_read(0);
    }
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
        parent->epmgr->tfd->set_fd_handler(nfs_fd, true, NULL);
        close(nfs_fd);
        delete this;
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
                    xdr_reset(rop->xdrs);
                    parent->xdr_pool.push_back(rop->xdrs);
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
                                    .buf = rop->buffer,
                                    .size = ub.size,
                                });
                                used_buffers.erase(rop->buffer);
                            }
                        }
                    }
                    free(rop);
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
    iovec *iov_list = NULL;
    unsigned iov_count = 0;
    int r = xdr_encode(rop->xdrs, (xdrproc_t)xdr_rpc_msg, &rop->out_msg);
    assert(r);
    if (rop->reply_fn != NULL)
    {
        r = xdr_encode(rop->xdrs, rop->reply_fn, rop->reply);
        assert(r);
    }
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

int nfs_client_t::handle_rpc_message(void *base_buf, void *msg_buf, uint32_t msg_len)
{
    // Take an XDR object from the pool
    XDR *xdrs;
    if (parent->xdr_pool.size())
    {
        xdrs = parent->xdr_pool.back();
        parent->xdr_pool.pop_back();
    }
    else
    {
        xdrs = xdr_create();
    }
    // Decode the RPC header
    char inmsg_data[sizeof(rpc_msg)];
    rpc_msg *inmsg = (rpc_msg*)&inmsg_data;
    if (!xdr_decode(xdrs, msg_buf, msg_len, (xdrproc_t)xdr_rpc_msg, inmsg))
    {
        // Invalid message, ignore it
        xdr_reset(xdrs);
        parent->xdr_pool.push_back(xdrs);
        return 0;
    }
    if (inmsg->body.dir != RPC_CALL)
    {
        // Reply sent to the server? Strange thing. Also ignore it
        xdr_reset(xdrs);
        parent->xdr_pool.push_back(xdrs);
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
        rpc_queue_reply(rop);
        // Incoming buffer isn't needed to handle request, so return 0
        return 0;
    }
    // Allocate memory
    rpc_op_t *rop = (rpc_op_t*)malloc_or_die(
        sizeof(rpc_op_t) + proc_it->req_size + proc_it->resp_size
    );
    rpc_reply_stat x = RPC_MSG_ACCEPTED;
    *rop = (rpc_op_t){
        .client = this,
        .buffer = (uint8_t*)base_buf,
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
    memcpy(&rop->in_msg, inmsg, sizeof(rpc_msg));
    // Try to decode the request
    // req_fn may be NULL, that means function has no arguments
    if (proc_it->req_fn && !proc_it->req_fn(xdrs, rop->request))
    {
        // Invalid request
        rop->out_msg.body.rbody.areply.reply_data.stat = RPC_GARBAGE_ARGS;
        rpc_queue_reply(rop);
        // Incoming buffer isn't needed to handle request, so return 0
        return 0;
    }
    rop->out_msg.body.rbody.areply.reply_data.stat = RPC_SUCCESS;
    rop->reply_fn = proc_it->resp_fn;
    int ref = proc_it->handler_fn(proc_it->opaque, rop);
    rop->referenced = ref ? 1 : 0;
    return ref;
}

void nfs_proxy_t::daemonize()
{
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
    open("/dev/null", O_WRONLY);
    open("/dev/null", O_WRONLY);
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
