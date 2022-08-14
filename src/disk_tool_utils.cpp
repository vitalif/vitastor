// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <sys/wait.h>

#include "disk_tool.h"
#include "rw_blocking.h"
#include "str_util.h"

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

std::string read_all_fd(int fd)
{
    int res_size = 0;
    std::string res;
    while (1)
    {
        res.resize(res_size+1024);
        int r = read(fd, (char*)res.data()+res_size, res.size()-res_size);
        if (r > 0)
            res_size += r;
        else if (!r || errno != EAGAIN && errno != EINTR)
            break;
    }
    res.resize(res_size);
    return res;
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

int check_queue_cache(std::string dev, std::string parent_dev)
{
    auto r = read_file("/sys/block/"+dev+"/queue/write_cache", true);
    if (r == "")
        r = read_file("/sys/block/"+parent_dev+"/queue/write_cache");
    if (r == "")
        return 1;
    return trim(r) == "write through" ? 0 : -1;
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
        dup2(child_stdout[1], 1);
        if (err)
            dup2(child_stderr[1], 2);
        close(child_stdin[0]);
        close(child_stdin[1]);
        close(child_stdout[0]);
        close(child_stdout[1]);
        close(child_stderr[0]);
        close(child_stderr[1]);
        //char *argv[] = { (char*)"/bin/sh", (char*)"-c", (char*)cmd.c_str(), NULL };
        char *argv[cmd.size()+1];
        for (int i = 0; i < cmd.size(); i++)
        {
            argv[i] = (char*)cmd[i].c_str();
        }
        argv[cmd.size()-1] = NULL;
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
    int r = shell_exec({ "/sbin/sfdisk", "--dump", dev, "--json" }, "", &part_dump, NULL);
    if (r == 255)
    {
        fprintf(stderr, "Error running /sbin/sfdisk --dump %s --json\n", dev.c_str());
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
            fprintf(stderr, "sfdisk --dump %s --json returned bad JSON: %s\n", dev.c_str(), part_dump.c_str());
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

uint64_t free_from_parttable(json11::Json pt)
{
    uint64_t free = pt["lastlba"].uint64_value() + 1 - pt["firstlba"].uint64_value();
    for (const auto & part: pt["partitions"].array_items())
    {
        free -= part["size"].uint64_value();
    }
    free *= pt["sectorsize"].uint64_value();
    return free;
}
