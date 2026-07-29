// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include <sys/file.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <linux/fs.h>
#include <unistd.h>
#include <string.h>

#include <stdexcept>

#include "disk_util.h"

void check_size(int fd, uint64_t *size, uint64_t *sectsize, const std::string & name)
{
    int sect;
    struct stat st;
    if (fstat(fd, &st) < 0)
    {
        throw std::runtime_error("Failed to stat "+name);
    }
    if (S_ISREG(st.st_mode))
    {
        *size = st.st_size;
        if (sectsize)
        {
            *sectsize = st.st_blksize;
        }
    }
    else if (S_ISBLK(st.st_mode))
    {
        if (ioctl(fd, BLKGETSIZE64, size) < 0 ||
            ioctl(fd, BLKSSZGET, &sect) < 0)
        {
            throw std::runtime_error("Failed to get "+name+" size or block size: "+strerror(errno));
        }
        if (sectsize)
        {
            *sectsize = sect;
        }
    }
    else
    {
        throw std::runtime_error(name+" is neither a file nor a block device");
    }
}
