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
        sscanf(str.string_value().c_str(), "%lx", &value);
    else if (str.string_value().size() > 2 && (str.string_value()[0] == '0' && str.string_value()[1] == 'x'))
        sscanf(str.string_value().c_str(), "0x%lx", &value);
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

std::string realpath_str(std::string path, bool nofail)
{
    char *p = realpath((char*)path.c_str(), NULL);
    if (!p)
    {
        fprintf(stderr, "Failed to resolve %s: %s\n", path.c_str(), strerror(errno));
        return nofail ? path : "";
    }
    std::string rp(p);
    free(p);
    return rp;
}

std::string read_file(std::string file, bool allow_enoent)
{
    std::string res;
    int fd = open(file.c_str(), O_RDONLY);
    if (fd < 0 || (res = read_all_fd(fd)) == "")
    {
        int err = errno;
        if (fd >= 0)
            close(fd);
        if (!allow_enoent || err != ENOENT)
            fprintf(stderr, "Can't read %s: %s\n", file.c_str(), strerror(err));
        return "";
    }
    close(fd);
    return res;
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
    auto scsi_disk = "/sys/block/"+parent_dev+"/device/scsi_disk";
    DIR *dir = opendir(scsi_disk.c_str());
    if (!dir)
    {
        if (errno == ENOENT)
        {
            // Not a SCSI/SATA device, just check /sys/block/.../queue/write_cache
            return check_queue_cache(dev.substr(5), parent_dev);
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
            return check_queue_cache(dev.substr(5), parent_dev);
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

std::string get_parent_device(std::string dev)
{
    if (dev.substr(0, 5) != "/dev/")
    {
        fprintf(stderr, "%s is outside /dev/\n", dev.c_str());
        return "";
    }
    dev = dev.substr(5);
    int i = dev.size();
    while (i > 0 && isdigit(dev[i-1]))
        i--;
    if (i >= 1 && dev[i-1] == '-') // dm-0, dm-1
        return dev;
    else if (i >= 2 && dev[i-1] == 'p' && isdigit(dev[i-2])) // nvme0n1p1
        i--;
    // Check that such block device exists
    struct stat st;
    auto chk = "/sys/block/"+dev.substr(0, i);
    if (stat(chk.c_str(), &st) < 0)
    {
        if (errno != ENOENT)
        {
            fprintf(stderr, "Failed to stat %s: %s\n", chk.c_str(), strerror(errno));
            return "";
        }
        return dev;
    }
    return dev.substr(0, i);
}

bool json_is_true(const json11::Json & val)
{
    if (val.is_string())
        return val == "true" || val == "yes" || val == "1";
    return val.bool_value();
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

int fix_partition_type(std::string dev_by_uuid)
{
    auto uuid = strtolower(dev_by_uuid.substr(dev_by_uuid.rfind('/')+1));
    std::string parent_dev = get_parent_device(realpath_str(dev_by_uuid, false));
    if (parent_dev == "")
        return 1;
    auto pt = read_parttable("/dev/"+parent_dev);
    if (pt.is_null() || pt.is_bool())
        return 1;
    std::string script = "label: gpt\n\n";
    for (const auto & part: pt["partitions"].array_items())
    {
        bool this_part = (strtolower(part["uuid"].string_value()) == uuid);
        if (this_part && strtolower(part["type"].string_value()) == "e7009fac-a5a1-4d72-af72-53de13059903")
        {
            // Already correct type
            return 0;
        }
        script += part["node"].string_value()+": ";
        bool first = true;
        for (const auto & kv: part.object_items())
        {
            if (kv.first != "node")
            {
                script += (first ? "" : ", ")+kv.first+"="+
                    (kv.first == "type" && this_part
                        ? "e7009fac-a5a1-4d72-af72-53de13059903"
                        : (kv.second.is_string() ? kv.second.string_value() : kv.second.dump()));
                first = false;
            }
        }
        script += "\n";
    }
    std::string out;
    return shell_exec({ "sfdisk", "--no-reread", "--force", "/dev/"+parent_dev }, script, &out, NULL);
}
