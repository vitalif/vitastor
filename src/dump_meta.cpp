// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#define _LARGEFILE64_SOURCE
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdint.h>
#include <stdlib.h>
#include <malloc.h>
#include <errno.h>
#include <assert.h>
#include <stdio.h>

#include "blockstore_impl.h"
#include "osd_id.h"
#include "rw_blocking.h"

struct meta_dumper_t
{
    char *meta_device;
    uint32_t meta_block_size;
    uint64_t meta_offset;
    uint64_t meta_len;
    uint64_t meta_pos;
    int fd;

    int dump();
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
    return self.dump();
}

int meta_dumper_t::dump()
{
    if (this->meta_block_size % DIRECT_IO_ALIGNMENT)
    {
        printf("Invalid metadata block size\n");
        return 1;
    }
    this->fd = open(this->meta_device, O_DIRECT|O_RDONLY);
    if (this->fd == -1)
    {
        printf("Failed to open metadata device\n");
        return 1;
    }
    int buf_size = 1024*1024;
    if (buf_size % this->meta_block_size)
        buf_size = 8*this->meta_block_size;
    if (buf_size > this->meta_len)
        buf_size = this->meta_len;
    void *data = memalign_or_die(MEM_ALIGNMENT, buf_size);
    lseek64(this->fd, this->meta_offset, 0);
    read_blocking(this->fd, data, buf_size);
    // Check superblock
    blockstore_meta_header_v1_t *hdr = (blockstore_meta_header_v1_t *)data;
    if (hdr->zero == 0 &&
        hdr->magic == BLOCKSTORE_META_MAGIC_V1 &&
        hdr->version == BLOCKSTORE_META_VERSION_V1)
    {
        // Vitastor 0.6-0.7 - static array of clean_disk_entry with bitmaps
        if (hdr->meta_block_size != this->meta_block_size)
        {
            printf("Using block size of %u bytes based on information from the superblock\n", hdr->meta_block_size);
            this->meta_block_size = hdr->meta_block_size;
            if (buf_size % this->meta_block_size)
            {
                buf_size = 8*this->meta_block_size;
                free(data);
                data = memalign_or_die(MEM_ALIGNMENT, buf_size);
            }
        }
        this->meta_offset += this->meta_block_size;
        this->meta_len -= this->meta_block_size;
        uint64_t clean_entry_bitmap_size = hdr->data_block_size / hdr->bitmap_granularity / 8;
        uint64_t clean_entry_size = sizeof(clean_disk_entry) + 2*clean_entry_bitmap_size;
        uint64_t block_num = 0;
        printf(
            "{\"version\":\"0.6\",\"meta_block_size\":%u,\"data_block_size\":%u,\"bitmap_granularity\":%u,\"entries\":[\n",
            hdr->meta_block_size, hdr->data_block_size, hdr->bitmap_granularity
        );
        bool first = true;
        lseek64(this->fd, this->meta_offset, 0);
        while (this->meta_pos < this->meta_len)
        {
            uint64_t read_len = buf_size < this->meta_len-this->meta_pos ? buf_size : this->meta_len-this->meta_pos;
            read_blocking(this->fd, data, read_len);
            this->meta_pos += read_len;
            for (uint64_t blk = 0; blk < read_len; blk += this->meta_block_size)
            {
                for (uint64_t ioff = 0; ioff < this->meta_block_size-clean_entry_size; ioff += clean_entry_size, block_num++)
                {
                    clean_disk_entry *entry = (clean_disk_entry*)(data + blk + ioff);
                    if (entry->oid.inode)
                    {
                        printf(
#define ENTRY_FMT "{\"block\":%lu,\"pool\":%u,\"inode\":%lu,\"stripe\":%lu,\"version\":%lu,\"bitmap\":\""
                            (first ? ENTRY_FMT : (",\n" ENTRY_FMT)),
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
        }
        printf("\n]}\n");
    }
    else
    {
        // Vitastor 0.4-0.5 - static array of clean_disk_entry
        uint64_t clean_entry_size = sizeof(clean_disk_entry);
        uint64_t block_num = 0;
        printf("{\"version\":\"0.5\",\"meta_block_size\":%u,\"entries\":[\n", this->meta_block_size);
        bool first = true;
        while (this->meta_pos < this->meta_len)
        {
            uint64_t read_len = buf_size < this->meta_len-this->meta_pos ? buf_size : this->meta_len-this->meta_pos;
            read_blocking(this->fd, data, read_len);
            this->meta_pos += read_len;
            for (uint64_t blk = 0; blk < read_len; blk += this->meta_block_size)
            {
                for (uint64_t ioff = 0; ioff < this->meta_block_size-clean_entry_size; ioff += clean_entry_size, block_num++)
                {
                    clean_disk_entry *entry = (clean_disk_entry*)(data + blk + ioff);
                    if (entry->oid.inode)
                    {
                        printf(
#define ENTRY_FMT "{\"block\":%lu,\"pool\":%u,\"inode\":%lu,\"stripe\":%lu,\"version\":%lu}"
                            (first ? ENTRY_FMT : (",\n" ENTRY_FMT)),
#undef ENTRY_FMT
                            block_num, INODE_POOL(entry->oid.inode), INODE_NO_POOL(entry->oid.inode),
                            entry->oid.stripe, entry->version
                        );
                        first = false;
                    }
                }
            }
        }
        printf("\n]}\n");
    }
    free(data);
    close(this->fd);
    return 0;
}
