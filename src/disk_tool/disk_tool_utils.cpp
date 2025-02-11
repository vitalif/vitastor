// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <sys/wait.h>
#include <dirent.h>

#include "disk_tool.h"
#include "rw_blocking.h"
#include "str_util.h"

uint64_t sscanf_json(const char *fmt, const json11::Json & str)
{
    uint64_t value = 0;
    if (fmt)
        sscanf(str.string_value().c_str(), "%jx", &value);
    else if (str.string_value().size() > 2 && (str.string_value()[0] == '0' && str.string_value()[1] == 'x'))
        sscanf(str.string_value().c_str(), "0x%jx", &value);
    else
        value = str.uint64_value();
    return value;
}

static int fromhex(char c)
{
    if (c >= '0' && c <= '9')
        return (c-'0');
    else if (c >= 'a' && c <= 'f')
        return (c-'a'+10);
    else if (c >= 'A' && c <= 'F')
        return (c-'A'+10);
    return -1;
}

void fromhexstr(const std::string & from, int bytes, uint8_t *to)
{
    for (int i = 0; i < from.size() && i < bytes; i++)
    {
        int x = fromhex(from[2*i]), y = fromhex(from[2*i+1]);
        if (x < 0 || y < 0)
            break;
        to[i] = x*16 + y;
    }
}

// returns 1 = check error, 0 = write through, -1 = write back
// (similar to 1 = warning, -1 = error, 0 = success in disable_cache)
static int check_queue_cache(std::string dev, std::string parent_dev)
{
    auto r = read_file("/sys/block/"+dev+"/queue/write_cache", true);
    if (r == "")
        r = read_file("/sys/block/"+parent_dev+"/queue/write_cache");
    if (r == "")
        return 1;
    return trim(r) == "write through" ? 0 : -1;
}

// returns 1 = warning, -1 = error, 0 = success
int disable_cache(std::string dev)
{
    auto parent_dev = get_parent_device(dev);
    if (parent_dev == "")
        return 1;
    auto scsi_disk = "/sys/block/"+parent_dev.substr(5)+"/device/scsi_disk";
    DIR *dir = opendir(scsi_disk.c_str());
    if (!dir)
    {
        if (errno == ENOENT)
        {
            // Not a SCSI/SATA device, just check /sys/block/.../queue/write_cache
            return check_queue_cache(dev.substr(5), parent_dev.substr(5));
        }
        else
        {
            fprintf(stderr, "Can't read directory %s: %s\n", scsi_disk.c_str(), strerror(errno));
            return 1;
        }
    }
    else
    {
        dirent *de = readdir(dir);
        while (de && de->d_name[0] == '.' && (de->d_name[1] == 0 || de->d_name[1] == '.' && de->d_name[2] == 0))
            de = readdir(dir);
        if (!de)
        {
            // Not a SCSI/SATA device, just check /sys/block/.../queue/write_cache
            closedir(dir);
            return check_queue_cache(dev.substr(5), parent_dev.substr(5));
        }
        scsi_disk += "/";
        scsi_disk += de->d_name;
        if (readdir(dir) != NULL)
        {
            // Error, multiple scsi_disk/* entries
            closedir(dir);
            fprintf(stderr, "Multiple entries in %s found\n", scsi_disk.c_str());
            return 1;
        }
        closedir(dir);
        // Check cache_type
        scsi_disk += "/cache_type";
        std::string cache_type = trim(read_file(scsi_disk));
        if (cache_type == "")
            return 1;
        if (cache_type != "write through")
        {
            int fd = open(scsi_disk.c_str(), O_WRONLY);
            if (fd < 0 || write_blocking(fd, (void*)"write through", strlen("write through")) != strlen("write through"))
            {
                if (fd >= 0)
                    close(fd);
                fprintf(stderr, "Can't write to %s: %s\n", scsi_disk.c_str(), strerror(errno));
                return -1;
            }
            close(fd);
        }
    }
    return 0;
}

uint64_t get_device_size(const std::string & dev, bool should_exist)
{
    struct stat dev_st;
    if (stat(dev.c_str(), &dev_st) < 0)
    {
        if (errno == ENOENT && !should_exist)
        {
            return 0;
        }
        fprintf(stderr, "Error checking %s: %s\n", dev.c_str(), strerror(errno));
        return UINT64_MAX;
    }
    uint64_t dev_size = dev_st.st_size;
    if (S_ISBLK(dev_st.st_mode))
    {
        int fd = open(dev.c_str(), O_DIRECT|O_RDWR);
        if (fd < 0)
        {
            fprintf(stderr, "Failed to open %s: %s\n", dev.c_str(), strerror(errno));
            return UINT64_MAX;
        }
        if (ioctl(fd, BLKGETSIZE64, &dev_size) < 0)
        {
            fprintf(stderr, "Failed to get %s size: %s\n", dev.c_str(), strerror(errno));
            close(fd);
            return UINT64_MAX;
        }
        close(fd);
    }
    return dev_size;
}

std::string get_parent_device(std::string dev)
{
    if (dev.substr(0, 5) != "/dev/")
    {
        fprintf(stderr, "%s is outside /dev/\n", dev.c_str());
        return "";
    }
    dev = dev.substr(5);
    // check if it's a partition - partitions aren't present in /sys/block/
    struct stat st;
    auto chk = "/sys/block/"+dev;
    if (stat(chk.c_str(), &st) == 0)
    {
        // present in /sys/block/ - not a partition
        return "/dev/"+dev;
    }
    else if (errno != ENOENT)
    {
        fprintf(stderr, "Failed to stat %s: %s\n", chk.c_str(), strerror(errno));
        return "";
    }
    int i = dev.size();
    while (i > 0 && isdigit(dev[i-1]))
        i--;
    if (i >= 2 && dev[i-1] == 'p' && isdigit(dev[i-2])) // nvme0n1p1
        i--;
    // Check that such block device exists
    chk = "/sys/block/"+dev.substr(0, i);
    if (stat(chk.c_str(), &st) < 0)
    {
        if (errno != ENOENT)
        {
            fprintf(stderr, "Failed to stat %s: %s\n", chk.c_str(), strerror(errno));
            return "";
        }
        return "/dev/"+dev;
    }
    return "/dev/"+dev.substr(0, i);
}

int shell_exec(const std::vector<std::string> & cmd, const std::string & in, std::string *out, std::string *err)
{
    int child_stdin[2], child_stdout[2], child_stderr[2];
    pid_t pid;
    if (pipe(child_stdin) == -1)
        goto err_pipe1;
    if (pipe(child_stdout) == -1)
        goto err_pipe2;
    if (pipe(child_stderr) == -1)
        goto err_pipe3;
    if ((pid = fork()) == -1)
        goto err_fork;
    if (pid)
    {
        // Parent
        // We should do select() to do something serious, but this is for simple cases
        close(child_stdin[0]);
        close(child_stdout[1]);
        close(child_stderr[1]);
        write_blocking(child_stdin[1], (void*)in.data(), in.size());
        close(child_stdin[1]);
        std::string s;
        s = read_all_fd(child_stdout[0]);
        if (out)
            out->swap(s);
        close(child_stdout[0]);
        s = read_all_fd(child_stderr[0]);
        if (err)
            err->swap(s);
        close(child_stderr[0]);
        int wstatus = 0;
        waitpid(pid, &wstatus, 0);
        return WEXITSTATUS(wstatus);
    }
    else
    {
        // Child
        dup2(child_stdin[0], 0);
        if (out)
            dup2(child_stdout[1], 1);
        if (err)
            dup2(child_stderr[1], 2);
        close(child_stdin[0]);
        close(child_stdin[1]);
        close(child_stdout[0]);
        close(child_stdout[1]);
        close(child_stderr[0]);
        close(child_stderr[1]);
        char *argv[cmd.size()+1];
        for (int i = 0; i < cmd.size(); i++)
            argv[i] = (char*)cmd[i].c_str();
        argv[cmd.size()] = NULL;
        execvp(argv[0], argv);
        std::string full_cmd;
        for (int i = 0; i < cmd.size(); i++)
        {
            full_cmd += cmd[i];
            full_cmd += " ";
        }
        full_cmd.resize(full_cmd.size() > 0 ? full_cmd.size()-1 : 0);
        fprintf(stderr, "error running %s: %s", full_cmd.c_str(), strerror(errno));
        exit(255);
    }
err_fork:
    close(child_stderr[1]);
    close(child_stderr[0]);
err_pipe3:
    close(child_stdout[1]);
    close(child_stdout[0]);
err_pipe2:
    close(child_stdin[1]);
    close(child_stdin[0]);
err_pipe1:
    return 255;
}

int write_zero(int fd, uint64_t offset, uint64_t size)
{
    uint64_t buf_len = 1024*1024;
    void *zero_buf = memalign_or_die(MEM_ALIGNMENT, buf_len);
    memset(zero_buf, 0, buf_len);
    ssize_t r;
    while (size > 0)
    {
        r = pwrite(fd, zero_buf, size > buf_len ? buf_len : size, offset);
        if (r > 0)
        {
            size -= r;
            offset += r;
        }
        else if (errno != EAGAIN && errno != EINTR)
        {
            free(zero_buf);
            return -1;
        }
    }
    free(zero_buf);
    return 0;
}

// Returns false in case of an error
// Returns null if there is no partition table
json11::Json read_parttable(std::string dev)
{
    std::string part_dump;
    int r = shell_exec({ "sfdisk", "--json", dev }, "", &part_dump, NULL);
    if (r == 255)
    {
        fprintf(stderr, "Error running sfdisk --json %s\n", dev.c_str());
        return json11::Json(false);
    }
    // Decode partition table
    json11::Json pt;
    if (part_dump != "")
    {
        std::string err;
        pt = json11::Json::parse(part_dump, err);
        if (err != "")
        {
            fprintf(stderr, "sfdisk --json %s returned bad JSON: %s\n", dev.c_str(), part_dump.c_str());
            return json11::Json(false);
        }
        pt = pt["partitiontable"];
        if (pt.is_object() && pt["label"].string_value() != "gpt")
        {
            fprintf(stderr, "%s contains \"%s\" partition table, only GPT is supported, skipping\n", dev.c_str(), pt["label"].string_value().c_str());
            return json11::Json(false);
        }
    }
    return pt;
}

uint64_t dev_size_from_parttable(json11::Json pt)
{
    uint64_t free = pt["lastlba"].uint64_value() + 1 - pt["firstlba"].uint64_value();
    if (!pt["sectorsize"].uint64_value())
        free *= 512;
    else
        free *= pt["sectorsize"].uint64_value();
    return free;
}

uint64_t free_from_parttable(json11::Json pt)
{
    uint64_t free = pt["lastlba"].uint64_value() + 1 - pt["firstlba"].uint64_value();
    for (const auto & part: pt["partitions"].array_items())
        free -= part["size"].uint64_value();
    if (!pt["sectorsize"].uint64_value())
        free *= 512;
    else
        free *= pt["sectorsize"].uint64_value();
    return free;
}

int fix_partition_type_uuid(std::string & dev_by_uuid, const std::string & type_uuid)
{
    bool is_partuuid = dev_by_uuid.substr(0, 22) == "/dev/disk/by-partuuid/";
    auto uuid = is_partuuid ? strtolower(dev_by_uuid.substr(22)) : "";
    auto node = realpath_str(dev_by_uuid, false);
    std::string parent_dev = get_parent_device(node);
    if (parent_dev == "")
        return 1;
    auto pt = read_parttable(parent_dev);
    if (pt.is_null() || pt.is_bool())
        return 1;
    bool found = false;
    std::string script = "label: gpt\n\n";
    for (const auto & part: pt["partitions"].array_items())
    {
        bool this_part = (part["node"].string_value() == node) &&
            (!is_partuuid || strtolower(part["uuid"].string_value()) == uuid);
        if (this_part)
        {
            found = true;
            if (!is_partuuid)
            {
                if (part["uuid"] == "")
                {
                    fprintf(stderr, "Could not determine partition UUID for %s. Please use GPT partitions\n", dev_by_uuid.c_str());
                    return 1;
                }
                auto new_dev = "/dev/disk/by-partuuid/"+strtolower(part["uuid"].string_value());
                fprintf(stderr, "Using %s instead of %s\n", new_dev.c_str(), dev_by_uuid.c_str());
                dev_by_uuid = new_dev;
            }
            if (strtolower(part["type"].string_value()) == type_uuid)
            {
                // Already correct type
                return 0;
            }
        }
        script += part["node"].string_value()+": ";
        bool first = true;
        for (const auto & kv: part.object_items())
        {
            if (kv.first != "node")
            {
                script += (first ? "" : ", ")+kv.first+"="+
                    (kv.first == "type" && this_part
                        ? type_uuid
                        : (kv.second.is_string() ? kv.second.string_value() : kv.second.dump()));
                first = false;
            }
        }
        script += "\n";
    }
    if (!found)
    {
        fprintf(stderr, "Could not find partition table entry for %s\n", dev_by_uuid.c_str());
        return 1;
    }
    std::string out;
    return shell_exec({ "sfdisk", "--no-reread", "--no-tell-kernel", "--force", parent_dev }, script, &out, NULL);
}

std::string csum_type_str(uint32_t data_csum_type)
{
    std::string csum_type;
    if (data_csum_type == BLOCKSTORE_CSUM_NONE)
        csum_type = "none";
    else if (data_csum_type == BLOCKSTORE_CSUM_CRC32C)
        csum_type = "crc32c";
    else
        csum_type = std::to_string(data_csum_type);
    return csum_type;
}

uint32_t csum_type_from_str(std::string data_csum_type)
{
    if (data_csum_type == "crc32c")
        return BLOCKSTORE_CSUM_CRC32C;
    return stoull_full(data_csum_type, 0);
}
