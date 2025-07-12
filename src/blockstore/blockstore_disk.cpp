// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <sys/file.h>
#include <sys/ioctl.h>

#include <stdexcept>

#include "blockstore.h"
#include "ondisk_formats.h"
#include "blockstore_disk.h"
#include "str_util.h"
#include "allocator.h"

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
    meta_block_target_free_space = parse_size(config["meta_block_target_free_space"]);
    bitmap_granularity = parse_size(config["bitmap_granularity"]);
    meta_format = stoull_full(config["meta_format"]);
    atomic_write_size = (config.find("atomic_write_size") != config.end()
        ? parse_size(config["atomic_write_size"]) : 4096);
    if (config.find("data_io") == config.end() &&
        config.find("meta_io") == config.end() &&
        config.find("journal_io") == config.end())
    {
        bool cached_io_data = config["cached_io_data"] == "true" || config["cached_io_data"] == "yes" || config["cached_io_data"] == "1";
        bool cached_io_meta = cached_io_data && (meta_device == data_device || meta_device == "") &&
            config.find("cached_io_meta") == config.end() ||
            config["cached_io_meta"] == "true" || config["cached_io_meta"] == "yes" || config["cached_io_meta"] == "1";
        bool cached_io_journal = cached_io_meta && (journal_device == meta_device || journal_device == "") &&
            config.find("cached_io_journal") == config.end() ||
            config["cached_io_journal"] == "true" || config["cached_io_journal"] == "yes" || config["cached_io_journal"] == "1";
        data_io = cached_io_data ? "cached" : "direct";
        meta_io = cached_io_meta ? "cached" : "direct";
        journal_io = cached_io_journal ? "cached" : "direct";
    }
    else
    {
        data_io = config.find("data_io") != config.end() ? config["data_io"] : "direct";
        meta_io = config.find("meta_io") != config.end()
            ? config["meta_io"]
            : (meta_device == data_device || meta_device == "" ? data_io : "direct");
        journal_io = config.find("journal_io") != config.end()
            ? config["journal_io"]
            : (journal_device == meta_device || journal_device == "" ? meta_io : "direct");
    }
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
    discard_on_start = config.find("discard_on_start") != config.end() &&
        (config["discard_on_start"] == "true" || config["discard_on_start"] == "1" || config["discard_on_start"] == "yes");
    min_discard_size = parse_size(config["min_discard_size"]);
    if (!min_discard_size)
        min_discard_size = 1024*1024;
    discard_granularity = parse_size(config["discard_granularity"]);
    inmemory_meta = config["inmemory_metadata"] != "false" && config["inmemory_metadata"] != "0" &&
        config["inmemory_metadata"] != "no";
    inmemory_journal = config["inmemory_journal"] != "false" && config["inmemory_journal"] != "0" &&
        config["inmemory_journal"] != "no";
    disable_data_fsync = config["disable_data_fsync"] == "true" || config["disable_data_fsync"] == "1" || config["disable_data_fsync"] == "yes";
    disable_meta_fsync = config["disable_meta_fsync"] == "true" || config["disable_meta_fsync"] == "1" || config["disable_meta_fsync"] == "yes";
    disable_journal_fsync = config["disable_journal_fsync"] == "true" || config["disable_journal_fsync"] == "1" || config["disable_journal_fsync"] == "yes";
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
    else if (journal_block_size > MAX_DATA_BLOCK_SIZE)
    {
        throw std::runtime_error("journal_block_size must not exceed "+std::to_string(MAX_DATA_BLOCK_SIZE));
    }
    if (!meta_block_size)
    {
        meta_block_size = 4096;
    }
    else if (meta_block_size % DIRECT_IO_ALIGNMENT)
    {
        throw std::runtime_error("meta_block_size must be a multiple of "+std::to_string(DIRECT_IO_ALIGNMENT));
    }
    else if (meta_block_size > MAX_DATA_BLOCK_SIZE)
    {
        throw std::runtime_error("meta_block_size must not exceed "+std::to_string(MAX_DATA_BLOCK_SIZE));
    }
    if (!meta_block_target_free_space)
    {
        meta_block_target_free_space = 800;
    }
    if (meta_block_target_free_space >= meta_block_size)
    {
        throw std::runtime_error("meta_block_target_free_space must not exceed "+std::to_string(meta_block_size));
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
    if (meta_device == data_device)
    {
        disable_meta_fsync = disable_data_fsync;
    }
    if (journal_device == meta_device)
    {
        disable_journal_fsync = disable_meta_fsync;
    }
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
    meta_area_size = (meta_fd == data_fd ? data_device_size : meta_device_size) - meta_offset;
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
    clean_entry_bitmap_size = data_block_size / bitmap_granularity / 8;
    clean_dyn_size = clean_entry_bitmap_size*2 + (csum_block_size
        ? data_block_size/csum_block_size*(data_csum_type & 0xFF) : 0);
    uint32_t entries_per_block = ((meta_block_size-meta_block_target_free_space) /
        (24 /*sizeof(heap_object_t)*/ + 33 /*sizeof(heap_write_t)*/ + clean_dyn_size));
    min_meta_len = (block_count+entries_per_block-1) / entries_per_block * meta_block_size;
    meta_format = BLOCKSTORE_META_FORMAT_HEAP;
    if (!skip_meta_check && meta_area_size < min_meta_len)
    {
        throw std::runtime_error("Metadata area is too small, need at least "+std::to_string(min_meta_len)+" bytes, have only "+std::to_string(meta_area_size)+" bytes");
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

static int bs_openmode(const std::string & mode)
{
    if (mode == "directsync")
        return O_DIRECT|O_SYNC;
    else if (mode == "cached")
        return O_SYNC;
    else
        return O_DIRECT;
}

void blockstore_disk_t::open_data()
{
    data_fd = open(data_device.c_str(), bs_openmode(data_io) | O_RDWR);
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
    if (meta_device != data_device || meta_io != data_io)
    {
        meta_fd = open(meta_device.c_str(), bs_openmode(meta_io) | O_RDWR);
        if (meta_fd == -1)
        {
            throw std::runtime_error("Failed to open metadata device "+meta_device+": "+std::string(strerror(errno)));
        }
        check_size(meta_fd, &meta_device_size, &meta_device_sect, "metadata device");
        if (meta_offset >= meta_device_size)
        {
            throw std::runtime_error("meta_offset exceeds device size = "+std::to_string(meta_device_size));
        }
        if (!disable_flock && meta_device != data_device && flock(meta_fd, LOCK_EX|LOCK_NB) != 0)
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
    if (journal_device != meta_device || journal_io != meta_io)
    {
        journal_fd = open(journal_device.c_str(), bs_openmode(journal_io) | O_RDWR);
        if (journal_fd == -1)
        {
            throw std::runtime_error("Failed to open journal device "+journal_device+": "+std::string(strerror(errno)));
        }
        check_size(journal_fd, &journal_device_size, &journal_device_sect, "journal device");
        if (!disable_flock && journal_device != meta_device && flock(journal_fd, LOCK_EX|LOCK_NB) != 0)
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

// Sadly DISCARD only works through ioctl(), but it seems to always block the device queue,
// so it's not a big deal that we can only run it synchronously.
int blockstore_disk_t::trim_data(std::function<bool(uint64_t)> is_free)
{
    int r = 0;
    uint64_t j = 0, i = 0;
    uint64_t discarded = 0;
    for (; i <= block_count; i++)
    {
        if (i >= block_count || is_free(i))
        {
            if (i > j && (i-j)*data_block_size >= min_discard_size)
            {
                uint64_t range[2] = { data_offset + j*data_block_size, (i-j)*data_block_size };
                if (discard_granularity)
                {
                    range[1] += range[0];
                    if (range[1] % discard_granularity)
                        range[1] = range[1] - (range[1] % discard_granularity);
                    if (range[0] % discard_granularity)
                        range[0] = range[0] + discard_granularity - (range[0] % discard_granularity);
                    if (range[0] >= range[1])
                        continue;
                    range[1] -= range[0];
                }
                r = ioctl(data_fd, BLKDISCARD, &range);
                if (r != 0)
                {
                    fprintf(stderr, "Failed to execute BLKDISCARD %ju+%ju on %s: %s (code %d)\n",
                        range[0], range[1], data_device.c_str(), strerror(-r), r);
                    return -errno;
                }
                discarded += range[1];
            }
            j = i+1;
        }
    }
    fprintf(stderr, "%s (%ju bytes) of unused data discarded on %s\n", format_size(discarded).c_str(), discarded, data_device.c_str());
    return 0;
}
