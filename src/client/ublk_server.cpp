// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
// ublk-based Vitastor block device in userspace

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/ioctl.h>
#include <dirent.h>

#include <fcntl.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/mman.h>
#include <unistd.h>

#include "../liburing/include/ublk_cmd.h"
#include "cluster_client.h"
#include "epoll_manager.h"
#include "str_util.h"

const char *exe_name = NULL;

const char *help_text =
    "Vitastor ublk server " VITASTOR_VERSION "\n"
    "(c) Vitaliy Filippov, 2025+ (VNPL-1.1)\n"
    "\n"
    "COMMANDS:\n"
    "\n"
    "vitastor-ublk map [OPTIONS] (--image <image> | --pool <pool> --inode <inode> --size <size in bytes>)\n"
    "  Map a ublk device. Options:\n"
    "  --recover\n"
    "    Recover a mapped device if the previous ublk server is dead.\n"
    "  --queue_depth 256\n"
    "    Maximum queue size for the device.\n"
    "  --max_io_size 1M\n"
    "    Maximum single I/O size for the device. Default: max(1 MB, pool block size * EC part count).\n"
    "  --readonly\n"
    "    Make the device read-only.\n"
    "  --hdd\n"
    "    Mark the device as rotational.\n"
    "  --logfile /path/to/log/file.txt\n"
    "    Write log messages to the specified file instead of dropping them (in background mode)\n"
    "    or printing them to the standard output (in foreground mode).\n"
    "  --dev_num N\n"
    "    Use the specified device /dev/ublkbN instead of automatic selection (alternative syntax\n"
    "    to /dev/ublkbN positional parameter).\n"
    "  --foreground 1\n"
    "    Stay in foreground, do not daemonize.\n"
    "\n"
    "vitastor-ublk unmap [--force] /dev/ublkb<N>\n"
    "  Unmap a Vitastor ublk device. Do not check if it's actually mapped if --force is specified.\n"
    "\n"
    "vitastor-ublk ls [--json]\n"
    "  List mapped Vitastor ublk devices, optionally in JSON format.\n"
    "\n"
    "Use vitastor-ublk --help <command> for command details or vitastor-ublk --help --all for all details.\n"
    "\n"
    "All usual Vitastor config options like --config_path <path_to_config> may also be specified in CLI.\n"
;

class ublk_server
{
protected:
    std::string image_name;
    uint64_t inode = 0;
    uint64_t device_size = 0;
    int req_dev_num = -1;
    bool readonly = false;
    bool hdd = false;
    bool recover = false;
    uint16_t queue_depth = 256;
    uint32_t max_io_size = 0;

    ring_loop_t *ringloop = NULL;
    epoll_manager_t *epmgr = NULL;
    cluster_client_t *cli = NULL;
    inode_watch_t *watch = NULL;

    std::string logfile = "/dev/null";

public:
    ublk_server()
    {
        ringloop = new ring_loop_t(RINGLOOP_DEFAULT_SIZE, false, true);
    }

    ~ublk_server()
    {
        if (ctrl_fd >= 0)
        {
            close(ctrl_fd);
            ctrl_fd = -1;
        }
        if (cdev_fd >= 0)
        {
            close(cdev_fd);
            cdev_fd = -1;
        }
        for (auto & buf: buffers)
        {
            free(buf);
        }
        buffers.clear();
        if (ringloop)
        {
            delete ringloop;
            ringloop = NULL;
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
                cfg["help"] = 1;
            }
            else if (args[i][0] == '-' && args[i][1] == '-')
            {
                const char *opt = args[i]+2;
                cfg[opt] = !strcmp(opt, "json") || !strcmp(opt, "all") ||
                    !strcmp(opt, "readonly") || !strcmp(opt, "hdd") || !strcmp(opt, "recover") ||
                    !strcmp(opt, "force") || i == narg-1 ? "1" : args[++i];
            }
            else if (pos == 0)
            {
                cfg["command"] = args[i];
                pos++;
            }
            else if (pos == 1)
            {
                char c = 0;
                int n = 0;
                if (sscanf(args[i], "/dev/ublkb%d%c", &n, &c) == 1)
                    cfg["dev_num"] = n;
                else if (sscanf(args[i], "/dev/ublkc%d%c", &n, &c) == 1)
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
        if (cfg["help"].bool_value())
        {
            goto help;
        }
        if (cfg["command"] == "map")
        {
            start(cfg);
        }
        else if (cfg["command"] == "unmap")
        {
            if (!cfg["dev_num"].is_number() &&
                cfg["dev_num"].string_value() != "0" &&
                !cfg["dev_num"].uint64_value())
            {
                fprintf(stderr, "device name or number is missing\n");
                exit(1);
            }
            open_control();
            unmap_device(cfg["dev_num"].uint64_value(), cfg["unpriv"].bool_value(), cfg["wait"].bool_value());
        }
        else if (cfg["command"] == "ls" || cfg["command"] == "list" || cfg["command"] == "list-mapped")
        {
            auto mapped = list_mapped();
            print_mapped(mapped, !cfg["json"].is_null());
        }
        else
        {
help:
            print_help(help_text, "vitastor-ublk", cfg["command"].string_value(), cfg["all"].bool_value());
            exit(0);
        }
    }

    void start(json11::Json cfg)
    {
        // Check options
        if (cfg["dev_num"].string_value() != "" || cfg["dev_num"].is_number())
        {
            req_dev_num = cfg["dev_num"].uint64_value();
        }
        if (cfg["image"].string_value() != "")
        {
            // Use image name
            image_name = cfg["image"].string_value();
            inode = 0;
        }
        else
        {
            // Use pool, inode number and size
            device_size = cfg["size"].is_string()
                ? parse_size(cfg["size"].string_value())
                : cfg["size"].uint64_value();
            if (!device_size)
            {
                fprintf(stderr, "device size is missing\n");
                exit(1);
            }
            inode = cfg["inode"].uint64_value();
            uint64_t pool = cfg["pool"].uint64_value();
            if (pool)
            {
                inode = INODE_WITH_POOL(pool, inode);
            }
            if (!INODE_POOL(inode))
            {
                fprintf(stderr, "pool is missing\n");
                exit(1);
            }
        }
        if (cfg["client_writeback_allowed"].is_null())
        {
            // ublk is always aware of fsync, so we allow write-back cache
            // by default if it's enabled
            auto obj = cfg.object_items();
            obj["client_writeback_allowed"] = true;
            cfg = obj;
        }
        readonly = cfg["readonly"].bool_value();
        hdd = cfg["hdd"].bool_value();
        recover = cfg["recover"].bool_value();
        if (recover && req_dev_num < 0)
        {
            fprintf(stderr, "device is missing\n");
            exit(1);
        }

        // Create client
        epmgr = new epoll_manager_t(ringloop);
        cli = new cluster_client_t(ringloop, epmgr->tfd, cfg);

        // cli->config contains merged config
        if (!cfg["queue_depth"].is_null())
        {
            queue_depth = cfg["queue_depth"].uint64_value();
        }
        else if (cli->config.find("ublk_queue_depth") != cli->config.end())
        {
            queue_depth = cli->config["ublk_queue_depth"].uint64_value();
        }
        if (!cfg["max_io_size"].is_null())
        {
            max_io_size = parse_size(cfg["max_io_size"].string_value());
        }
        else if (cli->config.find("ublk_max_io_size") != cli->config.end())
        {
            max_io_size = cli->config["ublk_max_io_size"].is_string()
                ? parse_size(cli->config["ublk_max_io_size"].string_value())
                : cli->config["ublk_max_io_size"].uint64_value();
        }

        // Load image metadata
        while (!cli->is_ready())
        {
            ringloop->loop();
            if (cli->is_ready())
                break;
            ringloop->wait();
        }
        if (!inode)
        {
            watch = cli->st_cli.watch_inode(image_name);
            device_size = watch->cfg.size;
            if (!watch->cfg.num || !device_size)
            {
                // Image does not exist
                fprintf(stderr, "Image %s does not exist\n", image_name.c_str());
                exit(1);
            }
        }
        const bool writeback = cli->get_immediate_commit(inode);
        auto pool_it = cli->st_cli.pool_config.find(INODE_POOL(inode ? inode : watch->cfg.num));
        if (pool_it == cli->st_cli.pool_config.end())
        {
            fprintf(stderr, "Pool %u does not exist\n", INODE_POOL(inode ? inode : watch->cfg.num));
            exit(1);
        }
        auto & pool_cfg = pool_it->second;
        uint32_t pg_data_size = pool_cfg.data_block_size * (pool_cfg.scheme == POOL_SCHEME_REPLICATED
            ? 1 : pool_cfg.pg_size-pool_cfg.parity_chunks);
        if (max_io_size & (max_io_size-1))
        {
            fprintf(stderr, "max_io_size must be a power of 2\n");
            exit(1);
        }
        uint32_t buf_size = max_io_size ? max_io_size : (1024*1024 < pg_data_size ? pg_data_size : 1024*1024);
        uint32_t bitmap_granularity = pool_cfg.bitmap_granularity;

        load_module();

        bool bg = cfg["foreground"].is_null();
        if (cfg["logfile"].string_value() != "")
        {
            logfile = cfg["logfile"].string_value();
        }

        open_control();
        if (recover)
        {
            recover_device(req_dev_num);
        }
        else
        {
            add_device(
                req_dev_num,
                (writeback ? UBLK_ATTR_VOLATILE_CACHE : 0) |
                (readonly ? UBLK_ATTR_READ_ONLY : 0) | (hdd ? UBLK_ATTR_ROTATIONAL : 0),
                queue_depth, bitmap_granularity, buf_size, pg_data_size, device_size
            );
        }
        int notifyfd[2] = { -1, -1 };
        if (bg)
        {
            if (socketpair(AF_UNIX, SOCK_STREAM, 0, notifyfd) < 0)
            {
                perror("socketpair");
                exit(1);
            }
            daemonize_fork(notifyfd);
            close(notifyfd[0]);
        }
        start_device(recover);
        if (bg)
        {
            daemonize_reopen_stdio();
            int ok = 0;
            write(notifyfd[1], &ok, sizeof(ok));
            close(notifyfd[1]);
        }
        else
            printf("/dev/ublkb%d\n", ublk_dev.dev_id);
        stop = false;
        while (!stop)
        {
            ringloop->loop();
            ringloop->wait();
        }
        cluster_op_t *close_sync = new cluster_op_t;
        close_sync->opcode = OSD_OP_SYNC;
        close_sync->callback = [this](cluster_op_t *op)
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
        cli = NULL;
        epmgr = NULL;
    }

    void load_module()
    {
        if (access("/sys/module/ublk_drv", F_OK) == 0)
        {
            return;
        }
        int r;
        if ((r = system("modprobe ublk_drv")) != 0)
        {
            if (r < 0)
                perror("Failed to load ublk_drv kernel module");
            else
                fprintf(stderr, "Failed to load ublk_drv kernel module\n");
            exit(1);
        }
    }

    void daemonize_fork(int *notifyfd)
    {
        if (fork())
        {
            // Parent - check status
            close(notifyfd[1]);
            int child_errno = 1;
            read(notifyfd[0], &child_errno, sizeof(child_errno));
            if (!child_errno)
                printf("/dev/ublkb%d\n", ublk_dev.dev_id);
            exit(child_errno);
        }
        setsid();
        if (fork())
            exit(0);
    }

    void daemonize_reopen_stdio()
    {
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
        int n_in_dev = 0;
        DIR *d = opendir("/dev");
        if (!d)
        {
            fprintf(stderr, "Failed to list /dev: %s (code %d)\n", strerror(errno), errno);
            exit(1);
        }
        dirent *ent;
        while ((ent = readdir(d)) != NULL)
        {
            if (!strncmp(ent->d_name, "ublkc", strlen("ublkc")))
                n_in_dev++;
        }
        closedir(d);
        json11::Json::object mapped;
        const char *self_filename = exe_name;
        for (int i = 0; exe_name[i] != 0; i++)
        {
            if (exe_name[i] == '/')
                self_filename = exe_name+i+1;
        }
        char path[64] = { 0 };
        int dev_num = -1, n_in_ctrl = 0;
        open_control();
        while (true)
        {
            dev_num++;
            int res = get_dev_info(dev_num, false);
            if (res == -ENODEV)
            {
                if (n_in_ctrl >= n_in_dev)
                    break;
                continue;
            }
            n_in_ctrl++;
            sprintf(path, "/proc/%d/cmdline", ublk_dev.ublksrv_pid);
            std::string cmdline = read_file(path);
            if (cmdline == "")
            {
                // Process is dead
                mapped["/dev/ublkb"+std::to_string(dev_num)] = json11::Json::object{{"dead", true}};
                continue;
            }
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
                    json11::Json::object cfg = ublk_server::parse_args(argv.size(), argv.data());
                    if (cfg["command"] == "map")
                    {
                        cfg.erase("command");
                        cfg["pid"] = ublk_dev.ublksrv_pid;
                        mapped["/dev/ublkb"+std::to_string(dev_num)] = cfg;
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
    bool stop = false;
    bool new_opcodes = true;
    uint64_t ublk_features = 0;
    int max_wait_time_ms = 5000;
    int ctrl_fd = -1, cdev_fd = -1;
    ublksrv_ctrl_dev_info ublk_dev = {};
    ublksrv_io_desc *ublk_queue = NULL;
    std::vector<uint8_t*> buffers;

    void open_control()
    {
        ctrl_fd = open("/dev/ublk-control", O_RDWR);
        if (ctrl_fd < 0)
        {
            fprintf(stderr, "Failed to open /dev/ublk-control: %s (code %d)\n", strerror(errno), errno);
            exit(1);
        }
        // Check features
        int res = sync_ublk_cmd(UBLK_U_CMD_GET_FEATURES, &ublk_features, 8, 0);
        if (res == -EOPNOTSUPP)
        {
            new_opcodes = false;
        }
        else if (res != 0)
        {
            fprintf(stderr, "Failed to get ublk features: %s (code %d)\n", strerror(-res), res);
            exit(1);
        }
    }

    void add_device(int32_t dev_num, uint32_t attrs, uint16_t queue_depth, uint32_t phys_block_size,
        uint32_t max_io_buf_bytes, uint64_t opt_block_size, uint64_t device_size)
    {
        // Add device
        ublk_dev.dev_id = dev_num;
        ublk_dev.nr_hw_queues = 1;
        ublk_dev.queue_depth = queue_depth;
        ublk_dev.max_io_buf_bytes = max_io_buf_bytes;
        ublk_dev.flags = UBLK_F_USER_RECOVERY | UBLK_F_USER_RECOVERY_REISSUE;
        int res = sync_ublk_cmd(new_opcodes ? UBLK_U_CMD_ADD_DEV : UBLK_CMD_ADD_DEV, &ublk_dev, sizeof(ublk_dev));
        if (res != 0)
        {
            fprintf(stderr, "Failed to add ublk device: %s (code %d)\n", strerror(-res), res);
            exit(1);
        }
        // Wait until the device appears
        std::string ublkc_path = "/dev/ublkc"+std::to_string(ublk_dev.dev_id);
        int wait_time = 0;
        while (wait_time < max_wait_time_ms)
        {
            cdev_fd = open(ublkc_path.c_str(), O_RDWR);
            if (cdev_fd >= 0)
                break;
            else if (errno != ENOENT)
            {
                fprintf(stderr, "Failed to open %s: %s (code %d)", ublkc_path.c_str(), strerror(errno), errno);
                exit(1);
            }
            usleep(100);
            wait_time += 100;
        }
        close(cdev_fd);
        cdev_fd = -1;
        // Set device params
        uint8_t io_opt_shift = 0;
        while ((opt_block_size >> io_opt_shift) > 1)
        {
            io_opt_shift++;
        }
        uint8_t phys_shift = 0;
        while ((phys_block_size >> phys_shift) > 1)
        {
            phys_shift++;
        }
        ublk_params params = {
            .len = sizeof(ublk_params),
            .types = UBLK_PARAM_TYPE_BASIC,
            .basic = {
                .attrs = attrs, // UBLK_ATTR_READ_ONLY | UBLK_ATTR_ROTATIONAL | UBLK_ATTR_VOLATILE_CACHE | UBLK_ATTR_FUA
                .logical_bs_shift = phys_shift,
                .physical_bs_shift = phys_shift,
                .io_opt_shift = io_opt_shift,
                .io_min_shift = phys_shift,
                .max_sectors = max_io_buf_bytes / 512,
                .chunk_sectors = 0,
                .dev_sectors = device_size / 512,
                .virt_boundary_mask = 0,
            },
            .discard = {
                .discard_alignment = 0,
                .discard_granularity = 0,
                .max_discard_sectors = 0,
                .max_write_zeroes_sectors = 0,
                .max_discard_segments = 0,
            },
        };
        res = sync_unpriv_cmd(false, new_opcodes ? UBLK_U_CMD_SET_PARAMS : UBLK_CMD_SET_PARAMS, &params, sizeof(params));
        if (res != 0)
        {
            fprintf(stderr, "Failed to set ublk device params: %s (code %d)\n", strerror(-res), res);
            exit(1);
        }
    }

    void map_ublk_queue()
    {
        const unsigned page_sz = getpagesize();
        size_t cmd_buf_size = (ublk_dev.queue_depth * sizeof(ublksrv_io_desc) + page_sz-1) / page_sz * page_sz;
        //const unsigned queue_offset = (UBLK_MAX_QUEUE_DEPTH * sizeof(ublksrv_io_desc) + page_sz-1) / page_sz * page_sz;
        //off = q_id * queue_offset;
        ublk_queue = (ublksrv_io_desc*)mmap(0, cmd_buf_size, PROT_READ, MAP_SHARED | MAP_POPULATE, cdev_fd, 0);
        if ((void*)ublk_queue == MAP_FAILED)
        {
            fprintf(stderr, "Failed to mmap() ublk queue buffer\n");
            exit(1);
        }
    }

    void recover_device(uint32_t dev_num)
    {
        ublk_dev.dev_id = dev_num;
        int res = sync_ublk_cmd(new_opcodes ? UBLK_U_CMD_GET_DEV_INFO : UBLK_CMD_GET_DEV_INFO, &ublk_dev, sizeof(ublk_dev));
        if (res != 0)
        {
            fprintf(stderr, "Failed to get /dev/ublkb%u device info: %s (code %d)\n", dev_num, strerror(-res), res);
            exit(1);
        }
        if (ublk_dev.nr_hw_queues != 1)
        {
            fprintf(stderr, "Device /dev/ublkb%u is not supported because it has %d queues\n", dev_num, ublk_dev.nr_hw_queues);
            exit(1);
        }
        if (ublk_dev.ublksrv_pid != 0)
        {
            res = kill(ublk_dev.ublksrv_pid, 0);
            if (res == 0)
            {
                fprintf(stderr, "Device /dev/ublkb%u is still alive, daemon PID is %u\n", dev_num, ublk_dev.ublksrv_pid);
                exit(1);
            }
            else if (errno != ESRCH)
            {
                fprintf(stderr, "Device /dev/ublkb%u is still alive, failed to check if the daemon with PID %u is running: %s (code %d)\n",
                    dev_num, ublk_dev.ublksrv_pid, strerror(errno), errno);
                exit(1);
            }
        }
        // Send the "start recovery" command
        res = sync_unpriv_cmd(false, new_opcodes ? UBLK_U_CMD_START_USER_RECOVERY : UBLK_CMD_START_USER_RECOVERY, NULL, 0);
        if (res != 0)
        {
            fprintf(stderr, "Failed to start /dev/ublkb%u device recovery: %s (code %d)\n", dev_num, strerror(-res), res);
            exit(1);
        }
    }

    void start_device(bool recover)
    {
        std::string ublkc_path = "/dev/ublkc"+std::to_string(ublk_dev.dev_id);
        cdev_fd = open(ublkc_path.c_str(), O_RDWR|O_NONBLOCK);
        if (cdev_fd < 0)
        {
            fprintf(stderr, "Failed to open %s: %s (code %d)", ublkc_path.c_str(), strerror(errno), errno);
            exit(1);
        }
        // FIXME Here we could optionally do ublk_get_queue_affinity
        // Map queue command buffer
        map_ublk_queue();
        // submit initial fetch requests to ublk driver
        for (int i = 0; i < ublk_dev.queue_depth; i++)
        {
            buffers.push_back((uint8_t*)memalign_or_die(MEM_ALIGNMENT, ublk_dev.max_io_buf_bytes));
            submit_request(new_opcodes ? UBLK_U_IO_FETCH_REQ : UBLK_IO_FETCH_REQ, i, 0);
        }
        ringloop->submit();
        // start device
        ublk_dev.ublksrv_pid = getpid();
        int res = sync_unpriv_cmd(false, (recover
            ? (new_opcodes ? UBLK_U_CMD_END_USER_RECOVERY : UBLK_CMD_END_USER_RECOVERY)
            : (new_opcodes ? UBLK_U_CMD_START_DEV : UBLK_CMD_START_DEV)), NULL, 0, ublk_dev.ublksrv_pid);
        if (res != 0)
        {
            fprintf(stderr, "Failed to start ublk device: %s (code %d)\n", strerror(-res), res);
            exit(1);
        }
        close(ctrl_fd);
        ctrl_fd = -1;
    }

    void submit_request(uint64_t ublk_cmd, int i, int res)
    {
        io_uring_sqe *sqe = ringloop->get_sqe();
        ring_data_t* data = ((ring_data_t*)sqe->user_data);
        sqe->fd = cdev_fd;
        sqe->opcode = IORING_OP_URING_CMD;
        //sqe->flags = IOSQE_FIXED_FILE;
        sqe->flags = 0;
        sqe->rw_flags = 0;
        sqe->off = ublk_cmd;
        ublksrv_io_cmd *cmd = (ublksrv_io_cmd *)&sqe->addr3; // sqe128 command buffer address
        cmd->q_id = 0;
        cmd->tag = i;
        cmd->addr = (uint64_t)buffers[i];
        cmd->result = res;
        data->callback = [this, i](ring_data_t *data) { exec_request(data->res, i); };
    }

    void exec_request(int res, int i)
    {
        if (res != 0)
        {
            // Note: res may be also UBLK_IO_RES_NEED_GET_DATA if UBLK_F_NEED_GET_DATA is enabled,
            // in this case you should submit_request(UBLK_IO_NEED_GET_DATA, i) again with buffer
            if (res == -ENODEV)
            {
                // ublk device is removed
                stop = true;
                return;
            }
            fprintf(stderr, "Fetching ublk request failed: %s (code %d)\n", strerror(-res), res);
            exit(1);
        }
        ublksrv_io_desc *iod = &ublk_queue[i];
        uint8_t opcode = ublksrv_get_op(iod);
        if (opcode == UBLK_IO_OP_FLUSH)
        {
            cluster_op_t *op = new cluster_op_t;
            op->opcode = OSD_OP_SYNC;
            op->callback = [this, i](cluster_op_t *op)
            {
                submit_request(new_opcodes ? UBLK_U_IO_COMMIT_AND_FETCH_REQ : UBLK_IO_COMMIT_AND_FETCH_REQ, i, op->retval);
                delete op;
            };
            cli->execute(op);
        }
        else if (opcode == UBLK_IO_OP_WRITE_ZEROES || opcode == UBLK_IO_OP_DISCARD)
        {
            submit_request(new_opcodes ? UBLK_U_IO_COMMIT_AND_FETCH_REQ : UBLK_IO_COMMIT_AND_FETCH_REQ, i, -EINVAL);
        }
        else if (opcode == UBLK_IO_OP_READ || opcode == UBLK_IO_OP_WRITE)
        {
            cluster_op_t *op = new cluster_op_t;
            op->opcode = opcode == UBLK_IO_OP_READ ? OSD_OP_READ : OSD_OP_WRITE;
            op->inode = inode ? inode : watch->cfg.num;
            op->offset = iod->start_sector * 512;
            op->len = iod->nr_sectors * 512;
            op->iov.push_back(buffers[i], op->len);
            op->callback = [this, i](cluster_op_t *op)
            {
                submit_request(new_opcodes ? UBLK_U_IO_COMMIT_AND_FETCH_REQ : UBLK_IO_COMMIT_AND_FETCH_REQ, i, op->retval);
                delete op;
            };
            cli->execute(op);
        }
        else
        {
            submit_request(new_opcodes ? UBLK_U_IO_COMMIT_AND_FETCH_REQ : UBLK_IO_COMMIT_AND_FETCH_REQ, i, -EINVAL);
        }
    }

    int get_dev_info(int dev_num, bool unpriv)
    {
        // Get device info
        ublk_dev.dev_id = dev_num;
        int res = unpriv
            ? sync_unpriv_cmd(true, new_opcodes ? UBLK_U_CMD_GET_DEV_INFO2 : UBLK_CMD_GET_DEV_INFO2, &ublk_dev, sizeof(ublk_dev))
            : sync_ublk_cmd(new_opcodes ? UBLK_U_CMD_GET_DEV_INFO : UBLK_CMD_GET_DEV_INFO, &ublk_dev, sizeof(ublk_dev));
        if (res != 0 && res != -ENODEV)
        {
            fprintf(stderr, "Failed to get device info from /dev/ublkc%d: %s (code %d)\n", dev_num, strerror(-res), res);
            exit(1);
        }
        return res;
    }

    void unmap_device(int dev_num, bool unpriv, bool wait)
    {
        int res = 0;
        // Stop the device
        ublk_dev.dev_id = dev_num;
        res = sync_unpriv_cmd(unpriv, new_opcodes ? UBLK_U_CMD_STOP_DEV : UBLK_CMD_STOP_DEV, NULL, 0);
        if (res != 0)
        {
            fprintf(stderr, "Failed to stop device /dev/ublkc%d: %s (code %d)\n", dev_num, strerror(-res), res);
            exit(1);
        }
        // Delete the device
        res = sync_unpriv_cmd(unpriv, new_opcodes ? (wait ? UBLK_U_CMD_DEL_DEV : UBLK_U_CMD_DEL_DEV_ASYNC) : UBLK_CMD_DEL_DEV, NULL, 0);
        if (res != 0)
        {
            fprintf(stderr, "Failed to delete device /dev/ublkc%d: %s (code %d)\n", dev_num, strerror(-res), res);
            exit(1);
        }
    }

    int sync_unpriv_cmd(bool unpriv, uint32_t cmd_op, void *addr, uint32_t len, uint64_t data0 = 0)
    {
        int res;
        if (unpriv)
        {
            static const int path_max = 64;
            char buf[path_max + len];
            memset(buf, 0, path_max);
            memcpy(buf + path_max, addr, len);
            snprintf(buf, path_max, "/dev/ublkc%d", ublk_dev.dev_id);
            res = sync_ublk_cmd(cmd_op, buf, sizeof(buf), path_max, data0);
            if (!res)
                memcpy(addr, buf + path_max, len);
        }
        else
        {
            res = sync_ublk_cmd(cmd_op, addr, len, 0, data0);
        }
        return res;
    }

    int sync_ublk_cmd(uint32_t cmd_op, void *addr, uint32_t len, uint16_t dev_path_len = 0, uint64_t data0 = 0)
    {
        io_uring_sqe *sqe = ringloop->get_sqe();
        sqe->fd = ctrl_fd;
        sqe->opcode = IORING_OP_URING_CMD;
        sqe->ioprio = 0;
        sqe->off = cmd_op;
        ublksrv_ctrl_cmd *cmd = (ublksrv_ctrl_cmd *)&sqe->addr3; // sqe128 command buffer address
        cmd->dev_id = ublk_dev.dev_id;
        cmd->queue_id = -1;
        cmd->addr = (uint64_t)addr;
        cmd->len = len;
        cmd->data[0] = data0;
        cmd->dev_path_len = dev_path_len;
        ring_data_t* data = ((ring_data_t*)sqe->user_data);
        bool done = false;
        int res = 0;
        data->callback = [&](ring_data_t *data)
        {
            res = data->res;
            done = true;
        };
        ringloop->submit();
        while (!done)
        {
            ringloop->loop();
            if (!done)
                ringloop->wait();
        }
        return res;
    }
};

int main(int narg, const char *args[])
{
    setvbuf(stdout, NULL, _IONBF, 0);
    setvbuf(stderr, NULL, _IONBF, 0);
    exe_name = args[0];
    ublk_server *p = new ublk_server();
    p->exec(ublk_server::parse_args(narg, args));
    delete p;
    return 0;
}
