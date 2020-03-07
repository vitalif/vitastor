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
#include "crc32c.h"

struct journal_dump_t
{
    char *journal_device;
    uint32_t journal_block;
    uint64_t journal_offset;
    uint64_t journal_len;
    uint64_t journal_pos;
    int fd;

    void dump_block(void *buf);
};

int main(int argc, char *argv[])
{
    if (argc < 5)
    {
        printf("USAGE: %s <journal_file> <journal_block_size> <offset> <size>\n", argv[0]);
        return 1;
    }
    journal_dump_t self;
    self.journal_device = argv[1];
    self.journal_block = strtoul(argv[2], NULL, 10);
    self.journal_offset = strtoull(argv[3], NULL, 10);
    self.journal_len = strtoull(argv[4], NULL, 10);
    if (self.journal_block < MEM_ALIGNMENT || (self.journal_block % MEM_ALIGNMENT) ||
        self.journal_block > 128*1024)
    {
        printf("Invalid journal block size\n");
        return 1;
    }
    self.fd = open(self.journal_device, O_DIRECT|O_RDONLY);
    if (self.fd == -1)
    {
        printf("Failed to open journal\n");
        return 1;
    }
    void *data = memalign(MEM_ALIGNMENT, self.journal_block);
    self.journal_pos = 0;
    while (self.journal_pos < self.journal_len)
    {
        int r = pread(self.fd, data, self.journal_block, self.journal_offset+self.journal_pos);
        assert(r == self.journal_block);
        uint64_t s;
        for (s = 0; s < self.journal_block; s += 8)
        {
            if (*((uint64_t*)(data+s)) != 0)
                break;
        }
        if (s == self.journal_block)
        {
            printf("offset %08lx: zeroes\n", self.journal_pos);
            self.journal_pos += self.journal_block;
        }
        else if (((journal_entry*)data)->magic == JOURNAL_MAGIC)
        {
            printf("offset %08lx:\n", self.journal_pos);
            self.dump_block(data);
        }
        else
        {
            printf("offset %08lx: no magic in the beginning, looks like random data (pattern=%lx)\n", self.journal_pos, *((uint64_t*)data));
            self.journal_pos += self.journal_block;
        }
    }
    free(data);
    close(self.fd);
    return 0;
}

void journal_dump_t::dump_block(void *buf)
{
    uint32_t pos = 0;
    journal_pos += journal_block;
    int entry = 0;
    bool wrapped = false;
    while (pos < journal_block)
    {
        journal_entry *je = (journal_entry*)(buf + pos);
        if (je->magic != JOURNAL_MAGIC || je->type < JE_START || je->type > JE_DELETE)
        {
            break;
        }
        const char *crc32_valid = je_crc32(je) == je->crc32 ? "(valid)" : "(invalid)";
        printf("entry % 3d: crc32=%08x %s prev=%08x ", entry, je->crc32, crc32_valid, je->crc32_prev);
        if (je->type == JE_START)
        {
            printf("je_start start=%08lx\n", je->start.journal_start);
        }
        else if (je->type == JE_SMALL_WRITE)
        {
            printf(
                "je_small_write oid=%lu:%lu ver=%lu offset=%u len=%u loc=%08lx",
                je->small_write.oid.inode, je->small_write.oid.stripe,
                je->small_write.version, je->small_write.offset, je->small_write.len,
                je->small_write.data_offset
            );
            if (journal_pos + je->small_write.len > journal_len)
            {
                // data continues from the beginning of the journal
                journal_pos = journal_block;
                wrapped = true;
            }
            if (journal_pos != je->small_write.data_offset)
            {
                printf(" (mismatched, calculated = %lu)", journal_pos);
            }
            journal_pos += je->small_write.len;
            if (journal_pos >= journal_len)
            {
                journal_pos = journal_block;
                wrapped = true;
            }
            uint32_t data_crc32 = 0;
            void *data = memalign(MEM_ALIGNMENT, je->small_write.len);
            assert(pread(fd, data, je->small_write.len, journal_offset+je->small_write.data_offset) == je->small_write.len);
            data_crc32 = crc32c(0, data, je->small_write.len);
            free(data);
            printf(
                " data_crc32=%08x%s", je->small_write.crc32_data,
                (data_crc32 != je->small_write.crc32_data) ? " (invalid)" : " (valid)"
            );
            printf("\n");
        }
        else if (je->type == JE_BIG_WRITE)
        {
            printf("je_big_write oid=%lu:%lu ver=%lu loc=%08lx\n", je->big_write.oid.inode, je->big_write.oid.stripe, je->big_write.version, je->big_write.location);
        }
        else if (je->type == JE_STABLE)
        {
            printf("je_stable oid=%lu:%lu ver=%lu\n", je->stable.oid.inode, je->stable.oid.stripe, je->stable.version);
        }
        else if (je->type == JE_ROLLBACK)
        {
            printf("je_rollback oid=%lu:%lu ver=%lu\n", je->rollback.oid.inode, je->rollback.oid.stripe, je->rollback.version);
        }
        else if (je->type == JE_DELETE)
        {
            printf("je_delete oid=%lu:%lu ver=%lu\n", je->del.oid.inode, je->del.oid.stripe, je->del.version);
        }
        pos += je->size;
        entry++;
    }
    if (wrapped)
    {
        journal_pos = journal_len;
    }
}
