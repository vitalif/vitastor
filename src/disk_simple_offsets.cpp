// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <fcntl.h>
#include <sys/ioctl.h>
#include <ctype.h>
#include <unistd.h>
#include <sys/stat.h>

#include "json11/json11.hpp"
#include "str_util.h"
#include "blockstore.h"
#include "blockstore_disk.h"

// Calculate offsets for a block device and print OSD command line parameters
void disk_tool_simple_offsets(json11::Json cfg, bool json_output)
{
    std::string device = cfg["device"].string_value();
    if (device == "")
    {
        fprintf(stderr, "Device path is missing\n");
        exit(1);
    }
    uint64_t data_block_size = parse_size(cfg["object_size"].string_value());
    uint64_t bitmap_granularity = parse_size(cfg["bitmap_granularity"].string_value());
    uint64_t journal_size = parse_size(cfg["journal_size"].string_value());
    uint64_t device_block_size = parse_size(cfg["device_block_size"].string_value());
    uint64_t journal_offset = parse_size(cfg["journal_offset"].string_value());
    uint64_t device_size = parse_size(cfg["device_size"].string_value());
    uint32_t csum_block_size = parse_size(cfg["csum_block_size"].string_value());
    uint32_t data_csum_type = BLOCKSTORE_CSUM_NONE;
    if (cfg["data_csum_type"] == "crc32c")
        data_csum_type = BLOCKSTORE_CSUM_CRC32C;
    else if (cfg["data_csum_type"].string_value() != "" && cfg["data_csum_type"].string_value() != "none")
    {
        fprintf(
            stderr, "data_csum_type=%s is unsupported, only \"crc32c\" and \"none\" are supported",
            cfg["data_csum_type"].string_value().c_str()
        );
        exit(1);
    }
    std::string format = cfg["format"].string_value();
    if (json_output)
        format = "json";
    if (!data_block_size)
        data_block_size = 1 << DEFAULT_DATA_BLOCK_ORDER;
    if (!bitmap_granularity)
        bitmap_granularity = DEFAULT_BITMAP_GRANULARITY;
    if (!journal_size)
        journal_size = 32*1024*1024;
    if (!device_block_size)
        device_block_size = 4096;
    if (!data_csum_type)
        csum_block_size = 0;
    else if (!csum_block_size)
        csum_block_size = bitmap_granularity;
    uint64_t orig_device_size = device_size;
    if (!device_size)
    {
        struct stat st;
        if (stat(device.c_str(), &st) < 0)
        {
            fprintf(stderr, "Can't stat %s: %s\n", device.c_str(), strerror(errno));
            exit(1);
        }
        if (S_ISBLK(st.st_mode))
        {
            int fd = open(device.c_str(), O_DIRECT|O_RDONLY);
            if (fd < 0 || ioctl(fd, BLKGETSIZE64, &device_size) < 0)
            {
                fprintf(stderr, "Failed to get device size for %s: %s\n", device.c_str(), strerror(errno));
                exit(1);
            }
            close(fd);
            if (st.st_blksize < device_block_size)
            {
                fprintf(
                    stderr, "Warning: %s reports %lu byte blocks, but we use %lu."
                    " Set --device_block_size=%lu if you're sure it works well with %lu byte blocks.\n",
                    device.c_str(), st.st_blksize, device_block_size, st.st_blksize, st.st_blksize
                );
            }
        }
        else if (S_ISREG(st.st_mode))
        {
            device_size = st.st_size;
        }
        else
        {
            fprintf(stderr, "%s is neither a block device nor a regular file\n", device.c_str());
            exit(1);
        }
    }
    if (!device_size)
    {
        fprintf(stderr, "Failed to get device size for %s\n", device.c_str());
        exit(1);
    }
    if (device_block_size < 512 || device_block_size > 1048576 ||
        device_block_size & (device_block_size-1) != 0)
    {
        fprintf(stderr, "Invalid device block size specified: %lu\n", device_block_size);
        exit(1);
    }
    if (data_block_size < device_block_size || data_block_size > MAX_DATA_BLOCK_SIZE ||
        data_block_size & (data_block_size-1) != 0)
    {
        fprintf(stderr, "Invalid object size specified: %lu\n", data_block_size);
        exit(1);
    }
    if (bitmap_granularity < device_block_size || bitmap_granularity > data_block_size ||
        bitmap_granularity & (bitmap_granularity-1) != 0)
    {
        fprintf(stderr, "Invalid bitmap granularity specified: %lu\n", bitmap_granularity);
        exit(1);
    }
    if (csum_block_size && (data_block_size % csum_block_size))
    {
        fprintf(stderr, "csum_block_size must be a divisor of data_block_size\n");
        exit(1);
    }
    journal_offset = ((journal_offset+device_block_size-1)/device_block_size)*device_block_size;
    uint64_t meta_offset = journal_offset + ((journal_size+device_block_size-1)/device_block_size)*device_block_size;
    uint64_t data_csum_size = (data_csum_type ? data_block_size/csum_block_size*(data_csum_type & 0xFF) : 0);
    uint64_t clean_entry_bitmap_size = data_block_size/bitmap_granularity/8;
    uint64_t clean_entry_size = 24 /*sizeof(clean_disk_entry)*/ + 2*clean_entry_bitmap_size + data_csum_size + 4 /*entry_csum*/;
    uint64_t entries_per_block = device_block_size / clean_entry_size;
    uint64_t object_count = ((device_size-meta_offset)/data_block_size);
    uint64_t meta_size = (1 + (object_count+entries_per_block-1)/entries_per_block) * device_block_size;
    uint64_t data_offset = meta_offset + meta_size;
    if (format == "json")
    {
        // JSON
        printf("%s\n", json11::Json(json11::Json::object {
            { "meta_block_size", device_block_size },
            { "journal_block_size", device_block_size },
            { "data_size", device_size-data_offset },
            { "data_device", device },
            { "journal_offset", journal_offset },
            { "meta_offset", meta_offset },
            { "data_offset", data_offset },
        }).dump().c_str());
    }
    else if (format == "env")
    {
        // Env
        printf(
            "meta_block_size=%lu\njournal_block_size=%lu\ndata_size=%lu\n"
            "data_device=%s\njournal_offset=%lu\nmeta_offset=%lu\ndata_offset=%lu\n",
            device_block_size, device_block_size, device_size-data_offset,
            device.c_str(), journal_offset, meta_offset, data_offset
        );
    }
    else
    {
        // OSD command-line options
        if (format != "options")
        {
            fprintf(stderr, "Metadata size: %s\nOptions for the OSD:\n", format_size(meta_size).c_str());
        }
        if (device_block_size != 4096)
        {
            printf("--meta_block_size %lu\n--journal_block_size %lu\n", device_block_size, device_block_size);
        }
        if (orig_device_size)
        {
            printf("--data_size %lu\n", device_size-data_offset);
        }
        printf(
            "--data_device %s\n--journal_offset %lu\n--meta_offset %lu\n--data_offset %lu\n",
            device.c_str(), journal_offset, meta_offset, data_offset
        );
    }
}
