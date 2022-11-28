// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <sys/file.h>

#include <stdexcept>

#include "blockstore_impl.h"
#include "blockstore_disk.h"
#include "str_util.h"

static uint32_t is_power_of_two(uint64_t value)
{
    uint32_t l = 0;
    while (value > 1)
    {
        if (value & 1)
        {
            return 64;
        }
        value = value >> 1;
        l++;
    }
    return l;
}

void blockstore_disk_t::parse_config(std::map<std::string, std::string> & config)
{
    // Parse
    if (config["disable_device_lock"] == "true" || config["disable_device_lock"] == "1" || config["disable_device_lock"] == "yes")
    {
        disable_flock = true;
    }
    cfg_journal_size = parse_size(config["journal_size"]);
    data_device = config["data_device"];
    data_offset = parse_size(config["data_offset"]);
    cfg_data_size = parse_size(config["data_size"]);
    meta_device = config["meta_device"];
    meta_offset = parse_size(config["meta_offset"]);
    data_block_size = parse_size(config["block_size"]);
    journal_device = config["journal_device"];
    journal_offset = parse_size(config["journal_offset"]);
    disk_alignment = parse_size(config["disk_alignment"]);
    journal_block_size = parse_size(config["journal_block_size"]);
    meta_block_size = parse_size(config["meta_block_size"]);
    bitmap_granularity = parse_size(config["bitmap_granularity"]);
    if (config["data_csum_type"] == "crc32c")
    {
        data_csum_type = BLOCKSTORE_CSUM_CRC32C;
    }
    else if (config["data_csum_type"] == "" || config["data_csum_type"] == "none")
    {
        data_csum_type = BLOCKSTORE_CSUM_NONE;
    }
    else
    {
        throw std::runtime_error("data_csum_type="+config["data_csum_type"]+" is unsupported, only \"crc32c\" and \"none\" are supported");
    }
    csum_block_size = parse_size(config["csum_block_size"]);
    // Validate
    if (!data_block_size)
    {
        data_block_size = (1 << DEFAULT_DATA_BLOCK_ORDER);
    }
    if ((block_order = is_power_of_two(data_block_size)) >= 64 || data_block_size < MIN_DATA_BLOCK_SIZE || data_block_size >= MAX_DATA_BLOCK_SIZE)
    {
        throw std::runtime_error("Bad block size");
    }
    if (!disk_alignment)
    {
        disk_alignment = 4096;
    }
    else if (disk_alignment % DIRECT_IO_ALIGNMENT)
    {
        throw std::runtime_error("disk_alignment must be a multiple of "+std::to_string(DIRECT_IO_ALIGNMENT));
    }
    if (!journal_block_size)
    {
        journal_block_size = 4096;
    }
    else if (journal_block_size % DIRECT_IO_ALIGNMENT)
    {
        throw std::runtime_error("journal_block_size must be a multiple of "+std::to_string(DIRECT_IO_ALIGNMENT));
    }
    if (!meta_block_size)
    {
        meta_block_size = 4096;
    }
    else if (meta_block_size % DIRECT_IO_ALIGNMENT)
    {
        throw std::runtime_error("meta_block_size must be a multiple of "+std::to_string(DIRECT_IO_ALIGNMENT));
    }
    if (data_offset % disk_alignment)
    {
        throw std::runtime_error("data_offset must be a multiple of disk_alignment = "+std::to_string(disk_alignment));
    }
    if (!bitmap_granularity)
    {
        bitmap_granularity = DEFAULT_BITMAP_GRANULARITY;
    }
    else if (bitmap_granularity % disk_alignment)
    {
        throw std::runtime_error("Sparse write tracking granularity must be a multiple of disk_alignment = "+std::to_string(disk_alignment));
    }
    if (data_block_size % bitmap_granularity)
    {
        throw std::runtime_error("Data block size must be a multiple of sparse write tracking granularity");
    }
    if (!data_csum_type)
    {
        csum_block_size = 0;
    }
    else if (!csum_block_size)
    {
        csum_block_size = bitmap_granularity;
    }
    if (csum_block_size && (csum_block_size % bitmap_granularity))
    {
        throw std::runtime_error("Checksum block size must be a multiple of sparse write tracking granularity");
    }
    if (csum_block_size && (data_block_size % csum_block_size))
    {
        throw std::runtime_error("Checksum block size must be a divisor of data block size");
    }
    if (meta_device == "")
    {
        meta_device = data_device;
    }
    if (journal_device == "")
    {
        journal_device = meta_device;
    }
    if (meta_offset % meta_block_size)
    {
        throw std::runtime_error("meta_offset must be a multiple of meta_block_size = "+std::to_string(meta_block_size));
    }
    if (journal_offset % journal_block_size)
    {
        throw std::runtime_error("journal_offset must be a multiple of journal_block_size = "+std::to_string(journal_block_size));
    }
    clean_entry_bitmap_size = data_block_size / bitmap_granularity / 8;
    clean_dyn_size = clean_entry_bitmap_size*2 + (csum_block_size
        ? data_block_size/csum_block_size*(data_csum_type & 0xFF) : 0);
    clean_entry_size = sizeof(clean_disk_entry) + clean_dyn_size + 4 /*entry_csum*/;
}

void blockstore_disk_t::calc_lengths(bool skip_meta_check)
{
    // data
    data_len = data_device_size - data_offset;
    if (data_fd == meta_fd && data_offset < meta_offset)
    {
        data_len = meta_offset - data_offset;
    }
    if (data_fd == journal_fd && data_offset < journal_offset)
    {
        data_len = data_len < journal_offset-data_offset
            ? data_len : journal_offset-data_offset;
    }
    if (cfg_data_size != 0)
    {
        if (data_len < cfg_data_size)
        {
            throw std::runtime_error("Data area ("+std::to_string(data_len)+
                " bytes) is smaller than configured size ("+std::to_string(cfg_data_size)+" bytes)");
        }
        data_len = cfg_data_size;
    }
    // meta
    uint64_t meta_area_size = (meta_fd == data_fd ? data_device_size : meta_device_size) - meta_offset;
    if (meta_fd == data_fd && meta_offset <= data_offset)
    {
        meta_area_size = data_offset - meta_offset;
    }
    if (meta_fd == journal_fd && meta_offset <= journal_offset)
    {
        meta_area_size = meta_area_size < journal_offset-meta_offset
            ? meta_area_size : journal_offset-meta_offset;
    }
    // journal
    journal_len = (journal_fd == data_fd ? data_device_size : (journal_fd == meta_fd ? meta_device_size : journal_device_size)) - journal_offset;
    if (journal_fd == data_fd && journal_offset <= data_offset)
    {
        journal_len = data_offset - journal_offset;
    }
    if (journal_fd == meta_fd && journal_offset <= meta_offset)
    {
        journal_len = journal_len < meta_offset-journal_offset
            ? journal_len : meta_offset-journal_offset;
    }
    // required metadata size
    block_count = data_len / data_block_size;
    meta_len = (1 + (block_count - 1 + meta_block_size / clean_entry_size) / (meta_block_size / clean_entry_size)) * meta_block_size;
    if (!skip_meta_check && meta_area_size < meta_len)
    {
        throw std::runtime_error("Metadata area is too small, need at least "+std::to_string(meta_len)+" bytes");
    }
    // requested journal size
    if (!skip_meta_check && cfg_journal_size > journal_len)
    {
        throw std::runtime_error("Requested journal_size is too large");
    }
    else if (cfg_journal_size > 0)
    {
        journal_len = cfg_journal_size;
    }
    if (journal_len < MIN_JOURNAL_SIZE)
    {
        throw std::runtime_error("Journal is too small, need at least "+std::to_string(MIN_JOURNAL_SIZE)+" bytes");
    }
}

// FIXME: Move to utils
static void check_size(int fd, uint64_t *size, uint64_t *sectsize, std::string name)
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

void blockstore_disk_t::open_data()
{
    data_fd = open(data_device.c_str(), O_DIRECT|O_RDWR);
    if (data_fd == -1)
    {
        throw std::runtime_error("Failed to open data device "+data_device+": "+std::string(strerror(errno)));
    }
    check_size(data_fd, &data_device_size, &data_device_sect, "data device");
    if (disk_alignment % data_device_sect)
    {
        throw std::runtime_error(
            "disk_alignment ("+std::to_string(disk_alignment)+
            ") is not a multiple of data device sector size ("+std::to_string(data_device_sect)+")"
        );
    }
    if (data_offset >= data_device_size)
    {
        throw std::runtime_error("data_offset exceeds device size = "+std::to_string(data_device_size));
    }
    if (!disable_flock && flock(data_fd, LOCK_EX|LOCK_NB) != 0)
    {
        throw std::runtime_error(std::string("Failed to lock data device: ") + strerror(errno));
    }
}

void blockstore_disk_t::open_meta()
{
    if (meta_device != data_device)
    {
        meta_fd = open(meta_device.c_str(), O_DIRECT|O_RDWR);
        if (meta_fd == -1)
        {
            throw std::runtime_error("Failed to open metadata device "+meta_device+": "+std::string(strerror(errno)));
        }
        check_size(meta_fd, &meta_device_size, &meta_device_sect, "metadata device");
        if (meta_offset >= meta_device_size)
        {
            throw std::runtime_error("meta_offset exceeds device size = "+std::to_string(meta_device_size));
        }
        if (!disable_flock && flock(meta_fd, LOCK_EX|LOCK_NB) != 0)
        {
            throw std::runtime_error(std::string("Failed to lock metadata device: ") + strerror(errno));
        }
    }
    else
    {
        meta_fd = data_fd;
        meta_device_sect = data_device_sect;
        meta_device_size = 0;
        if (meta_offset >= data_device_size)
        {
            throw std::runtime_error("meta_offset exceeds device size = "+std::to_string(data_device_size));
        }
    }
    if (meta_block_size % meta_device_sect)
    {
        throw std::runtime_error(
            "meta_block_size ("+std::to_string(meta_block_size)+
            ") is not a multiple of data device sector size ("+std::to_string(meta_device_sect)+")"
        );
    }
}

void blockstore_disk_t::open_journal()
{
    if (journal_device != meta_device)
    {
        journal_fd = open(journal_device.c_str(), O_DIRECT|O_RDWR);
        if (journal_fd == -1)
        {
            throw std::runtime_error("Failed to open journal device "+journal_device+": "+std::string(strerror(errno)));
        }
        check_size(journal_fd, &journal_device_size, &journal_device_sect, "journal device");
        if (!disable_flock && flock(journal_fd, LOCK_EX|LOCK_NB) != 0)
        {
            throw std::runtime_error(std::string("Failed to lock journal device: ") + strerror(errno));
        }
    }
    else
    {
        journal_fd = meta_fd;
        journal_device_sect = meta_device_sect;
        journal_device_size = 0;
        if (journal_offset >= data_device_size)
        {
            throw std::runtime_error("journal_offset exceeds device size");
        }
    }
    if (journal_block_size % journal_device_sect)
    {
        throw std::runtime_error(
            "journal_block_size ("+std::to_string(journal_block_size)+
            ") is not a multiple of journal device sector size ("+std::to_string(journal_device_sect)+")"
        );
    }
}

void blockstore_disk_t::close_all()
{
    if (data_fd >= 0)
        close(data_fd);
    if (meta_fd >= 0 && meta_fd != data_fd)
        close(meta_fd);
    if (journal_fd >= 0 && journal_fd != meta_fd)
        close(journal_fd);
    data_fd = meta_fd = journal_fd = -1;
}
