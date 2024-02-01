// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
// Similar to qemu-nbd, but sets timeout and uses io_uring

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <linux/genetlink.h>
#include <linux/nbd.h>
#include <linux/netlink.h>
#include <sys/ioctl.h>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "cluster_client.h"
#include "epoll_manager.h"

#include <netlink/attr.h>
#include <netlink/genl/ctrl.h>
#include <netlink/genl/genl.h>
#include <netlink/handlers.h>
#include <netlink/msg.h>
#include <netlink/netlink.h>
#include <netlink/socket.h>
#include <netlink/errno.h>

#ifdef HAVE_NBD_NETLINK_H

#include <linux/nbd-netlink.h>

#define fail(...)                   \
  do {                              \
    fprintf(stderr, __VA_ARGS__);   \
    exit(1);                        \
  } while (0)

struct netlink_ctx {
  struct nl_sock *sk;
  int driver_id;
};

static void netlink_sock_alloc(struct netlink_ctx *ctx)
{
  struct nl_sock *sk;
  int nl_driver_id;

  sk = nl_socket_alloc();
  if (!sk)
  {
    fail("Failed to alloc netlink socket\n");
  }

  if (genl_connect(sk))
  {
    nl_socket_free(sk);
    fail("Couldn't connect to the generic netlink socket\n");
  }

  nl_driver_id = genl_ctrl_resolve(sk, "nbd");
  if (nl_driver_id < 0)
  {
    nl_socket_free(sk);
    fail("Couldn't resolve the nbd netlink family\n");
  }

  ctx->driver_id = nl_driver_id;
  ctx->sk = sk;
}

static void netlink_sock_free(struct netlink_ctx *ctx)
{
  free(ctx->sk);
  ctx->sk = NULL;
}

static int netlink_status_cb(struct nl_msg *sk_msg, void *devnum)
{
  struct nlmsghdr *nl_hdr;
  struct genlmsghdr *gnl_hdr;
  struct nlattr *msg_attr[NBD_ATTR_MAX + 1];
  struct nlattr *attr_data;
  int attr_len;
  uint32_t* dev_num;

  dev_num = (uint32_t*)devnum;

  nl_hdr = nlmsg_hdr(sk_msg);
  gnl_hdr = (struct genlmsghdr *)nlmsg_data(nl_hdr);
  attr_data = genlmsg_attrdata(gnl_hdr, 0);
  attr_len = genlmsg_attrlen(gnl_hdr, 0);

  if (nla_parse(msg_attr, NBD_ATTR_MAX, attr_data, attr_len, NULL))
  {
    fail("Failed to parse netlink response\n");
  }

  if (!msg_attr[NBD_ATTR_INDEX])
  {
    fail("Got malformed netlink reponse\n");
  }

  *dev_num = nla_get_u32(msg_attr[NBD_ATTR_INDEX]);

  return NL_OK;
}

static int netlink_configure(const int *sockfd, int sock_size,
                               int dev_num, uint64_t size,
                               uint64_t blocksize, uint64_t flags,
                               uint64_t cflags, uint64_t timeout,
                               uint64_t conn_timeout)
{
  struct netlink_ctx ctx;
  struct nlattr *msg_attr, *msg_opt_attr;
  struct nl_msg *msg;
  int i, err, sock;
  uint32_t devnum;

  netlink_sock_alloc(&ctx);

  // A callback we set for a response we get on send
  nl_socket_modify_cb(ctx.sk, NL_CB_VALID, NL_CB_CUSTOM, netlink_status_cb, &devnum);

  msg = nlmsg_alloc();
  if (!msg)
  {
    netlink_sock_free(&ctx);
    fail("Failed to allocate netlking message\n");
  }

  genlmsg_put(msg, NL_AUTO_PORT, NL_AUTO_SEQ, ctx.driver_id, 0, 0, NBD_CMD_CONNECT, 0);

  if (dev_num >= 0)
  {
    NLA_PUT_U32(msg, NBD_ATTR_INDEX, (uint32_t)dev_num);
  }

  NLA_PUT_U64(msg, NBD_ATTR_SIZE_BYTES, size);
  NLA_PUT_U64(msg, NBD_ATTR_BLOCK_SIZE_BYTES, blocksize);
  NLA_PUT_U64(msg, NBD_ATTR_SERVER_FLAGS, flags);
  NLA_PUT_U64(msg, NBD_ATTR_CLIENT_FLAGS, cflags);

  if (timeout)
  {
    NLA_PUT_U64(msg, NBD_ATTR_TIMEOUT, timeout);
  }

  if (conn_timeout)
  {
    NLA_PUT_U64(msg, NBD_ATTR_DEAD_CONN_TIMEOUT, conn_timeout);
  }

  msg_attr = nla_nest_start(msg, NBD_ATTR_SOCKETS);
  if (!msg_attr)
  {
    goto nla_put_failure;
  }

  for (i = 0; i < sock_size; i++)
  {
    msg_opt_attr = nla_nest_start(msg, NBD_SOCK_ITEM);
    if (!msg_opt_attr)
    {
      goto nla_put_failure;
    }

    sock = sockfd[i];
    NLA_PUT_U32(msg, NBD_SOCK_FD, sock);

    nla_nest_end(msg, msg_opt_attr);
  }

  nla_nest_end(msg, msg_attr);

  if((err = nl_send_sync(ctx.sk, msg)) < 0)
  {
    netlink_sock_free(&ctx);
    return err;
  }

  netlink_sock_free(&ctx);

  return devnum;

nla_put_failure:
  nlmsg_free(msg);
  netlink_sock_free(&ctx);
  fail("Failed to create netlink message\n");
}

static void netlink_disconnect(uint32_t dev_num)
{
  struct netlink_ctx ctx;
  struct nl_msg *msg;
  int err;

  netlink_sock_alloc(&ctx);

  msg = nlmsg_alloc();
  if (!msg)
  {
    netlink_sock_free(&ctx);
    fail("Failed to allocate netlking message\n");
  }

  genlmsg_put(msg, NL_AUTO_PORT, NL_AUTO_SEQ, ctx.driver_id, 0, 0, NBD_CMD_DISCONNECT, 0);
  NLA_PUT_U32(msg, NBD_ATTR_INDEX, dev_num);

  if ((err = nl_send_sync(ctx.sk, msg)) < 0)
  {
    netlink_sock_free(&ctx);
    fail("Failed to send netlink message %d\n", err);
  }

  netlink_sock_free(&ctx);

  return;

nla_put_failure:
  nlmsg_free(msg);
  netlink_sock_free(&ctx);
  fail("Failed to create netlink message\n");
}

#undef fail

#else

static int netlink_configure(const int *sockfd, int sock_size,
                               int dev_num, uint64_t size,
                               uint64_t blocksize, uint64_t flags,
                               uint64_t cflags, uint64_t timeout,
                               uint64_t conn_timeout)
{
  fprintf(stderr, "netlink is not supported\n");
  exit(1);

  return 0;
}


static void netlink_disconnect(uint32_t dev_num)
{
  fprintf(stderr, "netlink is not supported\n");
  exit(1);
}

#endif

#ifndef MSG_ZEROCOPY
#define MSG_ZEROCOPY 0
#endif

const char *exe_name = NULL;

class nbd_proxy
{
protected:
    std::string image_name;
    uint64_t inode = 0;
    uint64_t device_size = 0;
    uint64_t nbd_lease = 0;
    int nbd_timeout = 300;
    int nbd_max_devices = 64;
    int nbd_max_part = 3;
    inode_watch_t *watch = NULL;

    ring_loop_t *ringloop = NULL;
    epoll_manager_t *epmgr = NULL;
    cluster_client_t *cli = NULL;
    ring_consumer_t consumer;

    std::vector<iovec> send_list, next_send_list;
    std::vector<void*> to_free;
    int nbd_fd = -1;
    void *recv_buf = NULL;
    int receive_buffer_size = 9000;
    nbd_request cur_req;
    cluster_op_t *cur_op = NULL;
    void *cur_buf = NULL;
    int cur_left = 0;
    int read_state = 0;
    int read_ready = 0;
    msghdr read_msg = { 0 }, send_msg = { 0 };
    iovec read_iov = { 0 };

    std::string logfile = "/dev/null";

public:
    ~nbd_proxy()
    {
        if (recv_buf)
        {
            free(recv_buf);
            recv_buf = NULL;
        }
    }

    static json11::Json::object parse_args(int narg, const char *args[])
    {
        json11::Json::object cfg;
        int pos = 0;
        for (int i = 1; i < narg; i++)
        {
            if (!strcmp(args[i], "-h") || !strcmp(args[i], "--help"))
            {
                help();
            }
            else if (args[i][0] == '-' && args[i][1] == '-')
            {
                const char *opt = args[i]+2;
                cfg[opt] = !strcmp(opt, "json") || i == narg-1 ? "1" : args[++i];
            }
            else if (pos == 0)
            {
                cfg["command"] = args[i];
                pos++;
            }
            else if (pos == 1 && (cfg["command"] == "map" || cfg["command"] == "unmap"))
            {
                int n = 0;
                if (sscanf(args[i], "/dev/nbd%d", &n) > 0)
                    cfg["dev_num"] = n;
                else
                    cfg["dev_num"] = args[i];
                pos++;
            }
        }
        return cfg;
    }

    void exec(json11::Json cfg)
    {
        if (cfg["command"] == "map")
        {
            start(cfg);
        }
        else if (cfg["command"] == "unmap")
        {
            if (cfg["dev_num"].is_null())
            {
                fprintf(stderr, "device name or number is missing\n");
                exit(1);
            }

            if (cfg["netlink"].is_null())
            {
                unmap(cfg["dev_num"].uint64_value());
            }
            else
            {
                netlink_disconnect(cfg["dev_num"].uint64_value());
            }
        }
        else if (cfg["command"] == "ls" || cfg["command"] == "list" || cfg["command"] == "list-mapped")
        {
            auto mapped = list_mapped();
            print_mapped(mapped, !cfg["json"].is_null());
        }
        else
        {
            help();
        }
    }

    static void help()
    {
        printf(
            "Vitastor NBD proxy\n"
            "(c) Vitaliy Filippov, 2020-2021 (VNPL-1.1)\n\n"
            "USAGE:\n"
            "  %s map [OPTIONS] (--image <image> | --pool <pool> --inode <inode> --size <size in bytes>)\n"
            "  %s unmap /dev/nbd0\n"
            "  %s ls [--json]\n"
            "OPTIONS:\n"
            "  All usual Vitastor config options like --config_file <path_to_config> plus NBD-specific:\n"
            "  --nbd_timeout 300\n"
            "    Timeout for I/O operations in seconds after exceeding which the kernel stops\n"
            "    the device. You can set it to 0 to disable the timeout, but beware that you\n"
            "    won't be able to stop the device at all if vitastor-nbd process dies.\n"
            "  --nbd_max_devices 64 --nbd_max_part 3\n"
            "    Options for the \"nbd\" kernel module when modprobing it (nbds_max and max_part).\n"
            "    note that maximum allowed (nbds_max)*(1+max_part) is 256.\n"
            "    Note that nbd_timeout, nbd_max_devices and nbd_max_part options may also be specified\n"
            "    in /etc/vitastor/vitastor.conf or in other configuration file specified with --config_file.\n"
            "  --nbd_lease 60\n"
            "    Timeout in seconds which is waited at max before nbd device\n"
            "    is returned after no I/O is performed on device.\n"
            "    By default is not set.\n"
            "  --nbd_destroy_on_disconnect 1\n"
            "    Delete the nbd device on disconnect\n."
            "  --nbd_disconnect_on_close 1\n"
            "    Disconnect the nbd device on close by last opener.\n"
            "  --nbd_ro 1\n"
            "    Set device into read only mode.\n"
            "  --logfile /path/to/log/file.txt\n"
            "    Wite log messages to the specified file instead of dropping them (in background mode)\n"
            "    or printing them to the standard output (in foreground mode).\n"
            "  --dev_num N\n"
            "    Use the specified device /dev/nbdN instead of automatic selection.\n"
            "  --foreground 1\n"
            "    Stay in foreground, do not daemonize.\n"
            "  --netlink 1\n"
            "    Use netlink to configure NBD device.\n",
            exe_name, exe_name, exe_name
        );
        exit(0);
    }

    void unmap(int dev_num)
    {
        char path[64] = { 0 };
        sprintf(path, "/dev/nbd%d", dev_num);
        int r, nbd = open(path, O_RDWR);
        if (nbd < 0)
        {
            perror("open");
            exit(1);
        }
        r = ioctl(nbd, NBD_DISCONNECT);
        if (r < 0)
        {
            perror("NBD_DISCONNECT");
            exit(1);
        }
        close(nbd);
    }

    void start(json11::Json cfg)
    {
        // Check options
        if (cfg["image"].string_value() != "")
        {
            // Use image name
            image_name = cfg["image"].string_value();
            inode = 0;
        }
        else
        {
            // Use pool, inode number and size
            if (!cfg["size"].uint64_value())
            {
                fprintf(stderr, "device size is missing\n");
                exit(1);
            }
            device_size = cfg["size"].uint64_value();
            inode = cfg["inode"].uint64_value();
            uint64_t pool = cfg["pool"].uint64_value();
            if (pool)
            {
                inode = (inode & (((uint64_t)1 << (64-POOL_ID_BITS)) - 1)) | (pool << (64-POOL_ID_BITS));
            }
            if (!(inode >> (64-POOL_ID_BITS)))
            {
                fprintf(stderr, "pool is missing\n");
                exit(1);
            }
        }
        auto file_config = osd_messenger_t::read_config(cfg);
        if (file_config["nbd_max_devices"].is_number() || file_config["nbd_max_devices"].is_string())
        {
            nbd_max_devices = file_config["nbd_max_devices"].uint64_value();
        }
        if (file_config["nbd_max_part"].is_number() || file_config["nbd_max_part"].is_string())
        {
            nbd_max_part = file_config["nbd_max_part"].uint64_value();
        }
        if (file_config["nbd_timeout"].is_number() || file_config["nbd_timeout"].is_string())
        {
            nbd_timeout = file_config["nbd_timeout"].uint64_value();
        }
        if (cfg["nbd_lease"].is_number() || cfg["nbd_lease"].is_string())
        {
            nbd_lease = cfg["nbd_lease"].uint64_value();
        }

        if (cfg["client_writeback_allowed"].is_null())
        {
            // NBD is always aware of fsync, so we allow write-back cache
            // by default if it's enabled
            auto obj = cfg.object_items();
            obj["client_writeback_allowed"] = true;
            cfg = obj;
        }
        // Create client
        ringloop = new ring_loop_t(RINGLOOP_DEFAULT_SIZE);
        epmgr = new epoll_manager_t(ringloop);
        cli = new cluster_client_t(ringloop, epmgr->tfd, cfg);
        if (!inode)
        {
            // Load image metadata
            while (!cli->is_ready())
            {
                ringloop->loop();
                if (cli->is_ready())
                    break;
                ringloop->wait();
            }
            watch = cli->st_cli.watch_inode(image_name);
            device_size = watch->cfg.size;
            if (!watch->cfg.num || !device_size)
            {
                // Image does not exist
                fprintf(stderr, "Image %s does not exist\n", image_name.c_str());
                exit(1);
            }
        }

        // Initialize NBD
        int sockfd[2];
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, sockfd) < 0)
        {
            perror("socketpair");
            exit(1);
        }

        fcntl(sockfd[0], F_SETFL, fcntl(sockfd[0], F_GETFL, 0) | O_NONBLOCK);
        nbd_fd = sockfd[0];
        load_module();
        bool bg = cfg["foreground"].is_null();

        if (!cfg["netlink"].is_null())
        {
            int devnum = -1;
            if (!cfg["dev_num"].is_null())
            {
                devnum = (int)cfg["dev_num"].uint64_value();
            }

            uint64_t flags = NBD_FLAG_SEND_FLUSH;
            uint64_t cflags = 0;

            #ifdef NBD_FLAG_READ_ONLY
            if (!cfg["nbd_ro"].is_null())
            {
                flags |= NBD_FLAG_READ_ONLY;
            }
            #endif

            #ifdef NBD_CFLAG_DESTROY_ON_DISCONNECT
            if (!cfg["nbd_destroy_on_disconnect"].is_null())
            {
                cflags |= NBD_CFLAG_DESTROY_ON_DISCONNECT;
            }
            #endif

            #ifdef NBD_CFLAG_DISCONNECT_ON_CLOSE
            if (!cfg["nbd_disconnect_on_close"].is_null())
            {
                cflags |= NBD_CFLAG_DISCONNECT_ON_CLOSE;
            }
            #endif

            int err = netlink_configure(sockfd + 1, 1, devnum, device_size, 4096, flags, cflags, nbd_timeout, nbd_lease);
            if (err < 0) {
                if (err == -NLE_BUSY) {
                    errno = EBUSY;
                } else {
                    errno = EIO;
                }

                perror("netlink_configure");
                exit(1);
            }

            printf("/dev/nbd%d\n", err);
        }
        else
        {
            if (!cfg["dev_num"].is_null())
            {
                if (run_nbd(sockfd, cfg["dev_num"].int64_value(), device_size, NBD_FLAG_SEND_FLUSH, nbd_timeout, bg) < 0)
                {
                    perror("run_nbd");
                    exit(1);
                }
            }
            else
            {
                // Find an unused device
                int i = 0;
                while (true)
                {
                    int r = run_nbd(sockfd, i, device_size, NBD_FLAG_SEND_FLUSH, nbd_timeout, bg);
                    if (r == 0)
                    {
                        printf("/dev/nbd%d\n", i);
                        break;
                    }
                    else if (r == -1 && errno == ENOENT)
                    {
                        fprintf(stderr, "No free NBD devices found\n");
                        exit(1);
                    }
                    else if (r == -2 && errno == EBUSY)
                    {
                        i++;
                    }
                    else
                    {
                        printf("%d %d\n", r, errno);
                        perror("run_nbd");
                        exit(1);
                    }
                }
            }
        }


        if (cfg["logfile"].is_string())
        {
            logfile = cfg["logfile"].string_value();
        }
        if (bg)
        {
            daemonize();
        }
        // Initialize read state
        read_state = CL_READ_HDR;
        recv_buf = malloc_or_die(receive_buffer_size);
        cur_buf = &cur_req;
        cur_left = sizeof(nbd_request);
        consumer.loop = [this]()
        {
            submit_read();
            submit_send();
            ringloop->submit();
        };
        ringloop->register_consumer(&consumer);
        // Add FD to epoll
        bool stop = false;
        epmgr->tfd->set_fd_handler(sockfd[0], false, [this, &stop](int peer_fd, int epoll_events)
        {
            if (epoll_events & EPOLLRDHUP)
            {
                close(peer_fd);
                stop = true;
            }
            else
            {
                read_ready++;
                submit_read();
            }
        });
        while (!stop)
        {
            ringloop->loop();
            ringloop->wait();
        }
        stop = false;
        cluster_op_t *close_sync = new cluster_op_t;
        close_sync->opcode = OSD_OP_SYNC;
        close_sync->callback = [&stop](cluster_op_t *op)
        {
            stop = true;
            delete op;
        };
        cli->execute(close_sync);
        while (!stop)
        {
            ringloop->loop();
            ringloop->wait();
        }
        cli->flush();
        delete cli;
        delete epmgr;
        delete ringloop;
        cli = NULL;
        epmgr = NULL;
        ringloop = NULL;
    }

    void load_module()
    {
        if (access("/sys/module/nbd", F_OK) == 0)
        {
            return;
        }
        int r;
        // Kernel built-in default is 16 devices with up to 16 partitions per device which is a big shit
        // 64 also isn't too high, but the possible maximum is nbds_max=256 max_part=0 and it won't reserve
        // any block device minor numbers for partitions
        if ((r = system(("modprobe nbd nbds_max="+std::to_string(nbd_max_devices)+" max_part="+std::to_string(nbd_max_part)).c_str())) != 0)
        {
            if (r < 0)
                perror("Failed to load NBD kernel module");
            else
                fprintf(stderr, "Failed to load NBD kernel module\n");
            exit(1);
        }
    }

    void daemonize()
    {
        if (fork())
            exit(0);
        setsid();
        if (fork())
            exit(0);
        close(0);
        close(1);
        close(2);
        open("/dev/null", O_RDONLY);
        open(logfile.c_str(), O_WRONLY|O_APPEND|O_CREAT, 0666);
        open(logfile.c_str(), O_WRONLY|O_APPEND|O_CREAT, 0666);
        if (chdir("/") != 0)
            fprintf(stderr, "Warning: Failed to chdir into /\n");
    }

    json11::Json::object list_mapped()
    {
        const char *self_filename = exe_name;
        for (int i = 0; exe_name[i] != 0; i++)
        {
            if (exe_name[i] == '/')
                self_filename = exe_name+i+1;
        }
        char path[64] = { 0 };
        json11::Json::object mapped;
        int dev_num = -1;
        int pid;
        while (true)
        {
            dev_num++;
            sprintf(path, "/sys/block/nbd%d", dev_num);
            if (access(path, F_OK) != 0)
                break;
            sprintf(path, "/sys/block/nbd%d/pid", dev_num);
            std::string pid_str = read_file(path);
            if (pid_str == "")
                continue;
            if (sscanf(pid_str.c_str(), "%d", &pid) < 1)
            {
                printf("Failed to read pid from /sys/block/nbd%d/pid\n", dev_num);
                continue;
            }
            sprintf(path, "/proc/%d/cmdline", pid);
            std::string cmdline = read_file(path);
            std::vector<const char*> argv;
            int last = 0;
            for (int i = 0; i < cmdline.size(); i++)
            {
                if (cmdline[i] == 0)
                {
                    argv.push_back(cmdline.c_str()+last);
                    last = i+1;
                }
            }
            if (argv.size() > 0)
            {
                const char *pid_filename = argv[0];
                for (int i = 0; argv[0][i] != 0; i++)
                {
                    if (argv[0][i] == '/')
                        pid_filename = argv[0]+i+1;
                }
                if (!strcmp(pid_filename, self_filename))
                {
                    json11::Json::object cfg = nbd_proxy::parse_args(argv.size(), argv.data());
                    if (cfg["command"] == "map")
                    {
                        cfg.erase("command");
                        cfg["pid"] = pid;
                        mapped["/dev/nbd"+std::to_string(dev_num)] = cfg;
                    }
                }
            }
        }
        return mapped;
    }

    void print_mapped(json11::Json mapped, bool json)
    {
        if (json)
        {
            printf("%s\n", mapped.dump().c_str());
        }
        else
        {
            for (auto & dev: mapped.object_items())
            {
                printf("%s\n", dev.first.c_str());
                for (auto & k: dev.second.object_items())
                {
                    printf("%s: %s\n", k.first.c_str(), k.second.as_string().c_str());
                }
                printf("\n");
            }
        }
    }

    std::string read_file(char *path)
    {
        int fd = open(path, O_RDONLY);
        if (fd < 0)
        {
            if (errno == ENOENT)
                return "";
            auto err = "open "+std::string(path);
            perror(err.c_str());
            exit(1);
        }
        std::string r;
        while (true)
        {
            int l = r.size();
            r.resize(l + 1024);
            int rd = read(fd, (void*)(r.c_str() + l), 1024);
            if (rd <= 0)
            {
                r.resize(l);
                break;
            }
            r.resize(l + rd);
        }
        close(fd);
        return r;
    }

protected:
    int run_nbd(int sockfd[2], int dev_num, uint64_t size, uint64_t flags, unsigned timeout, bool bg)
    {
        // Check handle size
        assert(sizeof(cur_req.handle) == 8);
        char path[64] = { 0 };
        sprintf(path, "/dev/nbd%d", dev_num);
        int r, nbd = open(path, O_RDWR), qd_fd;
        if (nbd < 0)
        {
            return -1;
        }
        r = ioctl(nbd, NBD_SET_SOCK, sockfd[1]);
        if (r < 0)
        {
            goto end_close;
        }
        r = ioctl(nbd, NBD_SET_BLKSIZE, 4096);
        if (r < 0)
        {
            goto end_unmap;
        }
        r = ioctl(nbd, NBD_SET_SIZE, size);
        if (r < 0)
        {
            goto end_unmap;
        }
        ioctl(nbd, NBD_SET_FLAGS, flags);
        if (timeout > 0)
        {
            r = ioctl(nbd, NBD_SET_TIMEOUT, (unsigned long)timeout);
            if (r < 0)
            {
                goto end_unmap;
            }
        }
        // Configure request size
        sprintf(path, "/sys/block/nbd%d/queue/max_sectors_kb", dev_num);
        qd_fd = open(path, O_WRONLY);
        if (qd_fd < 0)
        {
            goto end_unmap;
        }
        r = write(qd_fd, "32768", 5);
        if (r != 5)
        {
            fprintf(stderr, "Warning: Failed to configure max_sectors_kb\n");
        }
        close(qd_fd);
        if (!fork())
        {
            // Run in child
            close(sockfd[0]);
            if (bg)
            {
                daemonize();
            }
            r = ioctl(nbd, NBD_DO_IT);
            if (r < 0)
            {
                fprintf(stderr, "NBD device terminated with error: %s\n", strerror(errno));
            }
            close(sockfd[1]);
            ioctl(nbd, NBD_CLEAR_QUE);
            ioctl(nbd, NBD_CLEAR_SOCK);
            exit(0);
        }
        close(sockfd[1]);
        close(nbd);
        return 0;
    end_close:
        r = errno;
        close(nbd);
        errno = r;
        return -2;
    end_unmap:
        r = errno;
        ioctl(nbd, NBD_CLEAR_SOCK);
        close(nbd);
        errno = r;
        return -3;
    }

    void submit_send()
    {
        if (!send_list.size() || send_msg.msg_iovlen > 0)
        {
            return;
        }
        io_uring_sqe* sqe = ringloop->get_sqe();
        if (!sqe)
        {
            return;
        }
        ring_data_t* data = ((ring_data_t*)sqe->user_data);
        data->callback = [this](ring_data_t *data) { handle_send(data->res); };
        send_msg.msg_iov = send_list.data();
        send_msg.msg_iovlen = send_list.size();
        my_uring_prep_sendmsg(sqe, nbd_fd, &send_msg, MSG_ZEROCOPY);
    }

    void handle_send(int result)
    {
        send_msg.msg_iovlen = 0;
        if (result < 0 && result != -EAGAIN)
        {
            fprintf(stderr, "Socket disconnected: %s\n", strerror(-result));
            exit(1);
        }
        int to_eat = 0;
        while (result > 0 && to_eat < send_list.size())
        {
            if (result >= send_list[to_eat].iov_len)
            {
                free(to_free[to_eat]);
                result -= send_list[to_eat].iov_len;
                to_eat++;
            }
            else
            {
                send_list[to_eat].iov_base = (uint8_t*)send_list[to_eat].iov_base + result;
                send_list[to_eat].iov_len -= result;
                break;
            }
        }
        if (to_eat > 0)
        {
            send_list.erase(send_list.begin(), send_list.begin() + to_eat);
            to_free.erase(to_free.begin(), to_free.begin() + to_eat);
        }
        for (int i = 0; i < next_send_list.size(); i++)
        {
            send_list.push_back(next_send_list[i]);
        }
        next_send_list.clear();
        if (send_list.size() > 0)
        {
            ringloop->wakeup();
        }
    }

    void submit_read()
    {
        if (!read_ready || read_msg.msg_iovlen > 0)
        {
            return;
        }
        io_uring_sqe* sqe = ringloop->get_sqe();
        if (!sqe)
        {
            return;
        }
        ring_data_t* data = ((ring_data_t*)sqe->user_data);
        data->callback = [this](ring_data_t *data) { handle_read(data->res); };
        if (cur_left < receive_buffer_size)
        {
            read_iov.iov_base = recv_buf;
            read_iov.iov_len = receive_buffer_size;
        }
        else
        {
            read_iov.iov_base = cur_buf;
            read_iov.iov_len = cur_left;
        }
        read_msg.msg_iov = &read_iov;
        read_msg.msg_iovlen = 1;
        my_uring_prep_recvmsg(sqe, nbd_fd, &read_msg, 0);
    }

    void handle_read(int result)
    {
        read_msg.msg_iovlen = 0;
        if (result < 0 && result != -EAGAIN)
        {
            fprintf(stderr, "Socket disconnected: %s\n", strerror(-result));
            exit(1);
        }
        if (result == -EAGAIN || result < read_iov.iov_len)
        {
            read_ready--;
        }
        if (read_ready > 0)
        {
            ringloop->wakeup();
        }
        void *b = recv_buf;
        while (result > 0)
        {
            if (read_iov.iov_base == recv_buf)
            {
                int inc = result >= cur_left ? cur_left : result;
                memcpy(cur_buf, b, inc);
                cur_left -= inc;
                result -= inc;
                cur_buf = (uint8_t*)cur_buf + inc;
                b = (uint8_t*)b + inc;
            }
            else
            {
                assert(result <= cur_left);
                cur_left -= result;
                cur_buf = (uint8_t*)cur_buf + result;
                result = 0;
            }
            if (cur_left <= 0)
            {
                handle_finished_read();
            }
        }
    }

    void handle_finished_read()
    {
        if (read_state == CL_READ_HDR)
        {
            int req_type = be32toh(cur_req.type);
            if (be32toh(cur_req.magic) == NBD_REQUEST_MAGIC && req_type == NBD_CMD_DISC)
            {
                // Disconnect
                close(nbd_fd);
                exit(0);
            }
            if (be32toh(cur_req.magic) != NBD_REQUEST_MAGIC ||
                req_type != NBD_CMD_READ && req_type != NBD_CMD_WRITE && req_type != NBD_CMD_FLUSH)
            {
                printf("Unexpected request: magic=%x type=%x, terminating\n", cur_req.magic, req_type);
                exit(1);
            }
            uint64_t handle = *((uint64_t*)cur_req.handle);
#ifdef DEBUG
            printf("request %lx +%x %lx\n", be64toh(cur_req.from), be32toh(cur_req.len), handle);
#endif
            void *buf = NULL;
            cluster_op_t *op = new cluster_op_t;
            if (req_type == NBD_CMD_READ || req_type == NBD_CMD_WRITE)
            {
                op->opcode = req_type == NBD_CMD_READ ? OSD_OP_READ : OSD_OP_WRITE;
                op->inode = inode ? inode : watch->cfg.num;
                op->offset = be64toh(cur_req.from);
                op->len = be32toh(cur_req.len);
                buf = malloc_or_die(sizeof(nbd_reply) + op->len);
                op->iov.push_back((uint8_t*)buf + sizeof(nbd_reply), op->len);
            }
            else if (req_type == NBD_CMD_FLUSH)
            {
                op->opcode = OSD_OP_SYNC;
                buf = malloc_or_die(sizeof(nbd_reply));
            }
            op->callback = [this, buf, handle](cluster_op_t *op)
            {
#ifdef DEBUG
                printf("reply %lx e=%d\n", handle, op->retval);
#endif
                nbd_reply *reply = (nbd_reply*)buf;
                reply->magic = htobe32(NBD_REPLY_MAGIC);
                memcpy(reply->handle, &handle, 8);
                reply->error = htobe32(op->retval < 0 ? -op->retval : 0);
                auto & to_list = send_msg.msg_iovlen > 0 ? next_send_list : send_list;
                if (op->retval < 0 || op->opcode != OSD_OP_READ)
                    to_list.push_back({ .iov_base = buf, .iov_len = sizeof(nbd_reply) });
                else
                    to_list.push_back({ .iov_base = buf, .iov_len = sizeof(nbd_reply) + op->len });
                to_free.push_back(buf);
                delete op;
                ringloop->wakeup();
            };
            if (req_type == NBD_CMD_WRITE)
            {
                cur_op = op;
                cur_buf = (uint8_t*)buf + sizeof(nbd_reply);
                cur_left = op->len;
                read_state = CL_READ_DATA;
            }
            else
            {
                cur_op = NULL;
                cur_buf = &cur_req;
                cur_left = sizeof(nbd_request);
                read_state = CL_READ_HDR;
                cli->execute(op);
            }
        }
        else
        {
            if (cur_op->opcode == OSD_OP_WRITE && watch->cfg.readonly)
            {
                cur_op->retval = -EROFS;
                std::function<void(cluster_op_t*)>(cur_op->callback)(cur_op);
            }
            else
            {
                cli->execute(cur_op);
            }
            cur_op = NULL;
            cur_buf = &cur_req;
            cur_left = sizeof(nbd_request);
            read_state = CL_READ_HDR;
        }
    }
};

int main(int narg, const char *args[])
{
    setvbuf(stdout, NULL, _IONBF, 0);
    setvbuf(stderr, NULL, _IONBF, 0);
    exe_name = args[0];
    nbd_proxy *p = new nbd_proxy();
    p->exec(nbd_proxy::parse_args(narg, args));
    delete p;
    return 0;
}
