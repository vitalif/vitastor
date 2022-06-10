// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#define _LARGEFILE64_SOURCE
#include <sys/types.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdint.h>
#include <malloc.h>
#include <linux/fs.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <stdio.h>

#include "object_id.h"
#include "osd_id.h"

// "VITAstor"
#define BLOCKSTORE_META_MAGIC_V1 0x726F747341544956l
#define BLOCKSTORE_META_VERSION_V1 1

#define DIRECT_IO_ALIGNMENT 512
#define MEM_ALIGNMENT 4096

struct __attribute__((__packed__)) clean_disk_entry_v0_t
{
    object_id oid;
    uint64_t version;
    uint8_t bitmap[];
};

struct __attribute__((__packed__)) blockstore_meta_header_v1_t
{
    uint64_t zero;
    uint64_t magic;
    uint64_t version;
    uint32_t meta_block_size;
    uint32_t data_block_size;
    uint32_t bitmap_granularity;
};

struct meta_dumper_t
{
    char *meta_device;
    uint32_t meta_block_size;
    uint64_t meta_offset;
    uint64_t meta_len;
    uint64_t meta_pos;
    int fd;
};

int main(int argc, char *argv[])
{
    meta_dumper_t self = { 0 };
    int b = 1;
    if (argc < b+4)
    {
        printf("USAGE: %s <meta_file> <meta_block_size> <offset> <size>\n", argv[0]);
        return 1;
    }
    self.meta_device = argv[b];
    self.meta_block_size = strtoul(argv[b+1], NULL, 10);
    self.meta_offset = strtoull(argv[b+2], NULL, 10);
    self.meta_len = strtoull(argv[b+3], NULL, 10);
    if (self.meta_block_size % DIRECT_IO_ALIGNMENT)
    {
        printf("Invalid metadata block size\n");
        return 1;
    }
    self.fd = open(self.meta_device, O_DIRECT|O_RDONLY);
    if (self.fd == -1)
    {
        printf("Failed to open metadata device\n");
        return 1;
    }
    // Read all metadata into memory
    void *data = memalign(MEM_ALIGNMENT, self.meta_len);
    if (!data)
    {
        printf("Failed to allocate %lu MB of memory\n", self.meta_len/1024/1024);
        close(self.fd);
        return 1;
    }
    while (self.meta_pos < self.meta_len)
    {
        int r = pread(self.fd, data+self.meta_pos, self.meta_len-self.meta_pos, self.meta_offset+self.meta_pos);
        assert(r > 0);
        self.meta_pos += r;
    }
    close(self.fd);
    // Check superblock
    blockstore_meta_header_v1_t *hdr = (blockstore_meta_header_v1_t *)data;
    if (hdr->zero == 0 &&
        hdr->magic == BLOCKSTORE_META_MAGIC_V1 &&
        hdr->version == BLOCKSTORE_META_VERSION_V1)
    {
        // Vitastor 0.6-0.7 - static array of clean_disk_entry_v0_t with bitmaps
        if (hdr->meta_block_size != self.meta_block_size)
        {
            printf("Using block size %u bytes based on information from the superblock\n", hdr->meta_block_size);
            self.meta_block_size = hdr->meta_block_size;
        }
        uint64_t clean_entry_bitmap_size = hdr->data_block_size / hdr->bitmap_granularity / 8;
        uint64_t clean_entry_size = sizeof(clean_disk_entry_v0_t) + 2*clean_entry_bitmap_size;
        uint64_t block_num = 0;
        printf(
            "{\"version\":\"0.6\",\"meta_block_size\":%u,\"data_block_size\":%u,\"bitmap_granularity\":%u,\"entries\":[\n",
            hdr->meta_block_size, hdr->data_block_size, hdr->bitmap_granularity
        );
        bool first = true;
        for (uint64_t meta_pos = self.meta_block_size; meta_pos < self.meta_len; meta_pos += self.meta_block_size)
        {
            for (uint64_t ioff = 0; ioff < self.meta_block_size-clean_entry_size; ioff += clean_entry_size, block_num++)
            {
                clean_disk_entry_v0_t *entry = (clean_disk_entry_v0_t*)(data + meta_pos + ioff);
                if (entry->oid.inode)
                {
                    printf(
#define ENTRY_FMT "{\"block\":%lu,\"pool\":%u,\"inode\":%lu,\"stripe\":%lu,\"version\":%lu,\"bitmap\":\""
                        (first ? (",\n" ENTRY_FMT) : ENTRY_FMT),
#undef ENTRY_FMT
                        block_num, INODE_POOL(entry->oid.inode), INODE_NO_POOL(entry->oid.inode),
                        entry->oid.stripe, entry->version
                    );
                    first = false;
                    for (uint64_t i = 0; i < clean_entry_bitmap_size; i++)
                    {
                        printf("%02x", entry->bitmap[i]);
                    }
                    printf("\",\"ext_bitmap\":\"");
                    for (uint64_t i = 0; i < clean_entry_bitmap_size; i++)
                    {
                        printf("%02x", entry->bitmap[clean_entry_bitmap_size + i]);
                    }
                    printf("\"}");
                }
            }
        }
        printf("]}\n");
    }
    else
    {
        // Vitastor 0.4-0.5 - static array of clean_disk_entry_v0_t
        uint64_t clean_entry_size = sizeof(clean_disk_entry_v0_t);
        uint64_t block_num = 0;
        printf("{\"version\":\"0.5\",\"meta_block_size\":%u,\"entries\":[\n", self.meta_block_size);
        bool first = true;
        for (uint64_t meta_pos = 0; meta_pos < self.meta_len; meta_pos += self.meta_block_size)
        {
            for (uint64_t ioff = 0; ioff < self.meta_block_size-clean_entry_size; ioff += clean_entry_size, block_num++)
            {
                clean_disk_entry_v0_t *entry = (clean_disk_entry_v0_t*)(data + meta_pos + ioff);
                if (entry->oid.inode)
                {
                    printf(
#define ENTRY_FMT "{\"block\":%lu,\"pool\":%u,\"inode\":%lu,\"stripe\":%lu,\"version\":%lu}"
                        (first ? (",\n" ENTRY_FMT) : ENTRY_FMT),
#undef ENTRY_FMT
                        block_num, INODE_POOL(entry->oid.inode), INODE_NO_POOL(entry->oid.inode),
                        entry->oid.stripe, entry->version
                    );
                    first = false;
                }
            }
        }
        printf("]}\n");
    }
    free(data);
}
