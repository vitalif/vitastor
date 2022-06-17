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

#include "blockstore_impl.h"
#include "osd_id.h"
#include "crc32c.h"
#include "rw_blocking.h"

struct meta_tool_t
{
    std::map<std::string, std::string> options;

    char *journal_device;
    uint32_t journal_block_size;
    uint64_t journal_offset;
    uint64_t journal_len;
    uint64_t journal_pos;
    bool all;

    char *meta_device;
    uint32_t meta_block_size;
    uint64_t meta_offset;
    uint64_t meta_len;
    uint64_t meta_pos;

    char *data_device;
    uint64_t data_offset;
    uint64_t data_len;

    int journal_fd;
    int meta_fd;

    bool started;
    uint32_t crc32_last;

    int dump_journal();
    int dump_journal_block(void *buf);
    int dump_meta();
};

int main(int argc, char *argv[])
{
    meta_tool_t self = {};
    std::vector<char*> cmd;
    char *exe_name = strrchr(argv[0], '/');
    exe_name = exe_name ? exe_name+1 : argv[0];
    bool aliased = false;
    if (!strcmp(exe_name, "vitastor-dump-journal"))
    {
        cmd.push_back((char*)"dump-journal");
        aliased = true;
    }
    for (int i = 1; i < argc; i++)
    {
        if (!strcmp(argv[i], "--all"))
        {
            self.all = true;
        }
        else if (!strcmp(argv[i], "--help"))
        {
            cmd.clear();
            cmd.push_back((char*)"help");
        }
        else if (argv[i][0] == '-' && argv[i][1] == '-')
        {
            char *key = argv[i];
            self.options[key] = argv[++i];
        }
        else
        {
            cmd.push_back(argv[i]);
        }
    }
    if (cmd.size() && !strcmp(cmd[0], "dump-journal"))
    {
        if (cmd.size() < 5)
        {
            printf("USAGE: %s%s [--all] <journal_file> <journal_block_size> <offset> <size>\n", argv[0], aliased ? "" : " dump-journal");
            return 1;
        }
        self.journal_device = cmd[1];
        self.journal_block_size = strtoul(cmd[2], NULL, 10);
        self.journal_offset = strtoull(cmd[3], NULL, 10);
        self.journal_len = strtoull(cmd[4], NULL, 10);
        return self.dump_journal();
    }
    else if (cmd.size() && !strcmp(cmd[0], "dump-meta"))
    {
        if (cmd.size() < 5)
        {
            printf("USAGE: %s dump-meta <meta_file> <meta_block_size> <offset> <size>\n", argv[0]);
            return 1;
        }
        self.meta_device = cmd[1];
        self.meta_block_size = strtoul(cmd[2], NULL, 10);
        self.meta_offset = strtoull(cmd[3], NULL, 10);
        self.meta_len = strtoull(cmd[4], NULL, 10);
        return self.dump_meta();
    }
    else
    {
        printf(
            "USAGE:\n"
            "  %s dump-journal [--all] <journal_file> <journal_block_size> <offset> <size>\n"
            "  %s dump-meta <meta_file> <meta_block_size> <offset> <size>\n"
            ,
            argv[0], argv[0]
        );
    }
    return 0;
}

int meta_tool_t::dump_journal()
{
    auto & self = *this;
    if (self.journal_block_size < DIRECT_IO_ALIGNMENT || (self.journal_block_size % DIRECT_IO_ALIGNMENT) ||
        self.journal_block_size > 128*1024)
    {
        printf("Invalid journal block size\n");
        return 1;
    }
    self.journal_fd = open(self.journal_device, O_DIRECT|O_RDONLY);
    if (self.journal_fd == -1)
    {
        printf("Failed to open journal\n");
        return 1;
    }
    void *data = memalign(MEM_ALIGNMENT, self.journal_block_size);
    self.journal_pos = 0;
    if (self.all)
    {
        while (self.journal_pos < self.journal_len)
        {
            int r = pread(self.journal_fd, data, self.journal_block_size, self.journal_offset+self.journal_pos);
            assert(r == self.journal_block_size);
            uint64_t s;
            for (s = 0; s < self.journal_block_size; s += 8)
            {
                if (*((uint64_t*)((uint8_t*)data+s)) != 0)
                    break;
            }
            if (s == self.journal_block_size)
            {
                printf("offset %08lx: zeroes\n", self.journal_pos);
                self.journal_pos += self.journal_block_size;
            }
            else if (((journal_entry*)data)->magic == JOURNAL_MAGIC)
            {
                printf("offset %08lx:\n", self.journal_pos);
                self.dump_journal_block(data);
            }
            else
            {
                printf("offset %08lx: no magic in the beginning, looks like random data (pattern=%lx)\n", self.journal_pos, *((uint64_t*)data));
                self.journal_pos += self.journal_block_size;
            }
        }
    }
    else
    {
        int r = pread(self.journal_fd, data, self.journal_block_size, self.journal_offset+self.journal_pos);
        assert(r == self.journal_block_size);
        journal_entry *je = (journal_entry*)(data);
        if (je->magic != JOURNAL_MAGIC || je->type != JE_START || je_crc32(je) != je->crc32)
        {
            printf("offset %08lx: journal superblock is invalid\n", self.journal_pos);
        }
        else
        {
            printf("offset %08lx:\n", self.journal_pos);
            self.dump_journal_block(data);
            self.started = false;
            self.journal_pos = je->start.journal_start;
            while (1)
            {
                if (self.journal_pos >= self.journal_len)
                    self.journal_pos = self.journal_block_size;
                r = pread(self.journal_fd, data, self.journal_block_size, self.journal_offset+self.journal_pos);
                assert(r == self.journal_block_size);
                printf("offset %08lx:\n", self.journal_pos);
                r = self.dump_journal_block(data);
                if (r <= 0)
                {
                    printf("end of the journal\n");
                    break;
                }
            }
        }
    }
    free(data);
    close(self.journal_fd);
    return 0;
}

int meta_tool_t::dump_journal_block(void *buf)
{
    uint32_t pos = 0;
    journal_pos += journal_block_size;
    int entry = 0;
    bool wrapped = false;
    while (pos < journal_block_size)
    {
        journal_entry *je = (journal_entry*)((uint8_t*)buf + pos);
        if (je->magic != JOURNAL_MAGIC || je->type < JE_MIN || je->type > JE_MAX ||
            !all && started && je->crc32_prev != crc32_last)
        {
            break;
        }
        bool crc32_valid = je_crc32(je) == je->crc32;
        if (!all && !crc32_valid)
        {
            break;
        }
        started = true;
        crc32_last = je->crc32;
        printf("entry % 3d: crc32=%08x %s prev=%08x ", entry, je->crc32, (crc32_valid ? "(valid)" : "(invalid)"), je->crc32_prev);
        if (je->type == JE_START)
        {
            printf("je_start start=%08lx\n", je->start.journal_start);
        }
        else if (je->type == JE_SMALL_WRITE || je->type == JE_SMALL_WRITE_INSTANT)
        {
            printf(
                "je_small_write%s oid=%lx:%lx ver=%lu offset=%u len=%u loc=%08lx",
                je->type == JE_SMALL_WRITE_INSTANT ? "_instant" : "",
                je->small_write.oid.inode, je->small_write.oid.stripe,
                je->small_write.version, je->small_write.offset, je->small_write.len,
                je->small_write.data_offset
            );
            if (journal_pos + je->small_write.len > journal_len)
            {
                // data continues from the beginning of the journal
                journal_pos = journal_block_size;
                wrapped = true;
            }
            if (journal_pos != je->small_write.data_offset)
            {
                printf(" (mismatched, calculated = %lu)", journal_pos);
            }
            journal_pos += je->small_write.len;
            if (journal_pos >= journal_len)
            {
                journal_pos = journal_block_size;
                wrapped = true;
            }
            uint32_t data_crc32 = 0;
            void *data = memalign(MEM_ALIGNMENT, je->small_write.len);
            assert(pread(this->journal_fd, data, je->small_write.len, journal_offset+je->small_write.data_offset) == je->small_write.len);
            data_crc32 = crc32c(0, data, je->small_write.len);
            free(data);
            printf(
                " data_crc32=%08x%s", je->small_write.crc32_data,
                (data_crc32 != je->small_write.crc32_data) ? " (invalid)" : " (valid)"
            );
            printf("\n");
        }
        else if (je->type == JE_BIG_WRITE || je->type == JE_BIG_WRITE_INSTANT)
        {
            printf(
                "je_big_write%s oid=%lx:%lx ver=%lu loc=%08lx\n",
                je->type == JE_BIG_WRITE_INSTANT ? "_instant" : "",
                je->big_write.oid.inode, je->big_write.oid.stripe, je->big_write.version, je->big_write.location
            );
        }
        else if (je->type == JE_STABLE)
        {
            printf("je_stable oid=%lx:%lx ver=%lu\n", je->stable.oid.inode, je->stable.oid.stripe, je->stable.version);
        }
        else if (je->type == JE_ROLLBACK)
        {
            printf("je_rollback oid=%lx:%lx ver=%lu\n", je->rollback.oid.inode, je->rollback.oid.stripe, je->rollback.version);
        }
        else if (je->type == JE_DELETE)
        {
            printf("je_delete oid=%lx:%lx ver=%lu\n", je->del.oid.inode, je->del.oid.stripe, je->del.version);
        }
        pos += je->size;
        entry++;
    }
    if (wrapped)
    {
        journal_pos = journal_len;
    }
    return entry;
}

int meta_tool_t::dump_meta()
{
    if (this->meta_block_size % DIRECT_IO_ALIGNMENT)
    {
        printf("Invalid metadata block size\n");
        return 1;
    }
    this->meta_fd = open(this->meta_device, O_DIRECT|O_RDONLY);
    if (this->meta_fd == -1)
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
    lseek64(this->meta_fd, this->meta_offset, 0);
    read_blocking(this->meta_fd, data, buf_size);
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
        lseek64(this->meta_fd, this->meta_offset, 0);
        while (this->meta_pos < this->meta_len)
        {
            uint64_t read_len = buf_size < this->meta_len-this->meta_pos ? buf_size : this->meta_len-this->meta_pos;
            read_blocking(this->meta_fd, data, read_len);
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
            read_blocking(this->meta_fd, data, read_len);
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
    close(this->meta_fd);
    return 0;
}
