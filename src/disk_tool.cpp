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

#define DM_ST_EMPTY 0
#define DM_ST_TO_READ 1
#define DM_ST_READING 2
#define DM_ST_TO_WRITE 3
#define DM_ST_WRITING 4

struct resizer_data_moving_t
{
    int state = 0;
    void *buf = NULL;
    uint64_t old_loc, new_loc;
};

struct disk_tool_t
{
    /**** Parameters ****/

    std::map<std::string, std::string> options;

    std::string journal_device;
    uint32_t journal_block_size;
    uint64_t journal_offset;
    uint64_t journal_len;
    bool all;

    std::string meta_device;
    uint32_t meta_block_size;
    uint64_t meta_offset;
    uint64_t meta_len;
    uint64_t meta_pos;

    std::string data_device;
    uint64_t data_offset;
    uint64_t data_len;
    uint64_t data_block_size;

    uint64_t clean_entry_bitmap_size, clean_entry_size;
    uint32_t bitmap_granularity;

    // resize data and/or move metadata and journal
    int iodepth;
    char *new_meta_device, *new_journal_device;
    uint64_t new_data_offset, new_data_len;
    uint64_t new_journal_offset, new_journal_len;
    uint64_t new_meta_offset, new_meta_len;

    /**** State ****/

    uint64_t journal_pos, journal_calc_data_pos;

    int journal_fd, meta_fd;
    bool first;

    int data_fd;
    allocator *data_alloc;
    std::map<uint64_t, uint64_t> data_remap;
    std::map<uint64_t, uint64_t>::iterator remap_it;
    ring_loop_t *ringloop;
    ring_consumer_t ring_consumer;
    int remap_active;
    uint8_t *new_buf, *new_journal_ptr, *new_journal_data;
    uint64_t new_journal_in_pos;
    int64_t data_idx_diff;
    uint64_t total_blocks, free_first, free_last;
    uint64_t new_clean_entry_bitmap_size, new_clean_entry_size, new_entries_per_block;
    int new_journal_fd, new_meta_fd;
    resizer_data_moving_t *moving_blocks;

    bool started;
    void *small_write_data;
    uint32_t data_crc32;
    uint32_t crc32_last;
    uint32_t new_crc32_prev;

    /**** Commands ****/

    int dump_journal();
    int dump_meta();
    int resize_data();

    /**** Methods ****/

    void dump_journal_entry(int num, journal_entry *je);
    int process_journal(std::function<int(void*)> block_fn);
    int process_journal_block(void *buf, std::function<void(int, journal_entry*)> iter_fn);
    int process_meta(std::function<void(blockstore_meta_header_v1_t *)> hdr_fn,
        std::function<void(uint64_t, clean_disk_entry*, uint8_t*)> record_fn);
    void dump_meta_header(blockstore_meta_header_v1_t *hdr);
    void dump_meta_entry(uint64_t block_num, clean_disk_entry *entry, uint8_t *bitmap);

    void resize_init(blockstore_meta_header_v1_t *hdr);
    int resize_remap_blocks();
    int resize_copy_data();
    int resize_rewrite_journal();
    int resize_rewrite_meta();
};

int main(int argc, char *argv[])
{
    disk_tool_t self = {};
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
            char *key = argv[i]+2;
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
            fprintf(stderr, "USAGE: %s%s [--all] <journal_file> <journal_block_size> <offset> <size>\n", argv[0], aliased ? "" : " dump-journal");
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
            fprintf(stderr, "USAGE: %s dump-meta <meta_file> <meta_block_size> <offset> <size>\n", argv[0]);
            return 1;
        }
        self.meta_device = cmd[1];
        self.meta_block_size = strtoul(cmd[2], NULL, 10);
        self.meta_offset = strtoull(cmd[3], NULL, 10);
        self.meta_len = strtoull(cmd[4], NULL, 10);
        return self.dump_meta();
    }
    else if (cmd.size() && !strcmp(cmd[0], "resize"))
    {
        return self.resize_data();
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

int disk_tool_t::dump_journal()
{
    if (journal_block_size < DIRECT_IO_ALIGNMENT || (journal_block_size % DIRECT_IO_ALIGNMENT) ||
        journal_block_size > 128*1024)
    {
        fprintf(stderr, "Invalid journal block size\n");
        return 1;
    }
    journal_fd = open(journal_device.c_str(), O_DIRECT|O_RDONLY);
    if (journal_fd < 0)
    {
        fprintf(stderr, "Failed to open journal device %s: %s\n", journal_device.c_str(), strerror(errno));
        return 1;
    }
    if (all)
    {
        void *journal_buf = memalign_or_die(MEM_ALIGNMENT, journal_block_size);
        journal_pos = 0;
        while (journal_pos < journal_len)
        {
            int r = pread(journal_fd, journal_buf, journal_block_size, journal_offset+journal_pos);
            assert(r == journal_block_size);
            uint64_t s;
            for (s = 0; s < journal_block_size; s += 8)
            {
                if (*((uint64_t*)((uint8_t*)journal_buf+s)) != 0)
                    break;
            }
            if (s == journal_block_size)
            {
                printf("offset %08lx: zeroes\n", journal_pos);
                journal_pos += journal_block_size;
            }
            else if (((journal_entry*)journal_buf)->magic == JOURNAL_MAGIC)
            {
                printf("offset %08lx:\n", journal_pos);
                process_journal_block(journal_buf, [this](int num, journal_entry *je) { dump_journal_entry(num, je); });
            }
            else
            {
                printf("offset %08lx: no magic in the beginning, looks like random data (pattern=%lx)\n", journal_pos, *((uint64_t*)journal_buf));
                journal_pos += journal_block_size;
            }
        }
        free(journal_buf);
    }
    else
    {
        process_journal([this](void *data)
        {
            printf("offset %08lx:\n", journal_pos);
            int r = process_journal_block(data, [this](int num, journal_entry *je) { dump_journal_entry(num, je); });
            if (r <= 0)
                printf("end of the journal\n");
            return r;
        });
    }
    close(journal_fd);
    return 0;
}

int disk_tool_t::process_journal(std::function<int(void*)> block_fn)
{
    void *data = memalign_or_die(MEM_ALIGNMENT, journal_block_size);
    journal_pos = 0;
    int r = pread(journal_fd, data, journal_block_size, journal_offset+journal_pos);
    assert(r == journal_block_size);
    journal_entry *je = (journal_entry*)(data);
    if (je->magic != JOURNAL_MAGIC || je->type != JE_START || je_crc32(je) != je->crc32)
    {
        fprintf(stderr, "offset %08lx: journal superblock is invalid\n", journal_pos);
        return 1;
    }
    else
    {
        block_fn(data);
        started = false;
        journal_pos = je->start.journal_start;
        while (1)
        {
            if (journal_pos >= journal_len)
                journal_pos = journal_block_size;
            r = pread(journal_fd, data, journal_block_size, journal_offset+journal_pos);
            assert(r == journal_block_size);
            r = block_fn(data);
            if (r <= 0)
                break;
        }
    }
    free(data);
    return 0;
}

int disk_tool_t::process_journal_block(void *buf, std::function<void(int, journal_entry*)> iter_fn)
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
        if (je->type == JE_SMALL_WRITE || je->type == JE_SMALL_WRITE_INSTANT)
        {
            journal_calc_data_pos = journal_pos;
            if (journal_pos + je->small_write.len > journal_len)
            {
                // data continues from the beginning of the journal
                journal_calc_data_pos = journal_pos = journal_block_size;
                wrapped = true;
            }
            journal_pos += je->small_write.len;
            if (journal_pos >= journal_len)
            {
                journal_pos = journal_block_size;
                wrapped = true;
            }
            small_write_data = memalign_or_die(MEM_ALIGNMENT, je->small_write.len);
            assert(pread(journal_fd, small_write_data, je->small_write.len, journal_offset+je->small_write.data_offset) == je->small_write.len);
            data_crc32 = crc32c(0, small_write_data, je->small_write.len);
        }
        iter_fn(entry, je);
        if (je->type == JE_SMALL_WRITE || je->type == JE_SMALL_WRITE_INSTANT)
        {
            free(small_write_data);
            small_write_data = NULL;
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

void disk_tool_t::dump_journal_entry(int num, journal_entry *je)
{
    printf("entry % 3d: crc32=%08x %s prev=%08x ", num, je->crc32, (je_crc32(je) == je->crc32 ? "(valid)" : "(invalid)"), je->crc32_prev);
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
        if (journal_calc_data_pos != je->small_write.data_offset)
        {
            printf(" (mismatched, calculated = %lu)", journal_pos);
        }
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
}

int disk_tool_t::process_meta(std::function<void(blockstore_meta_header_v1_t *)> hdr_fn,
    std::function<void(uint64_t, clean_disk_entry*, uint8_t*)> record_fn)
{
    if (meta_block_size % DIRECT_IO_ALIGNMENT)
    {
        fprintf(stderr, "Invalid metadata block size: is not a multiple of %d\n", DIRECT_IO_ALIGNMENT);
        return 1;
    }
    meta_fd = open(meta_device.c_str(), O_DIRECT|O_RDONLY);
    if (meta_fd < 0)
    {
        fprintf(stderr, "Failed to open metadata device %s: %s\n", meta_device.c_str(), strerror(errno));
        return 1;
    }
    int buf_size = 1024*1024;
    if (buf_size % meta_block_size)
        buf_size = 8*meta_block_size;
    if (buf_size > meta_len)
        buf_size = meta_len;
    void *data = memalign_or_die(MEM_ALIGNMENT, buf_size);
    lseek64(meta_fd, meta_offset, 0);
    read_blocking(meta_fd, data, buf_size);
    // Check superblock
    blockstore_meta_header_v1_t *hdr = (blockstore_meta_header_v1_t *)data;
    if (hdr->zero == 0 &&
        hdr->magic == BLOCKSTORE_META_MAGIC_V1 &&
        hdr->version == BLOCKSTORE_META_VERSION_V1)
    {
        // Vitastor 0.6-0.7 - static array of clean_disk_entry with bitmaps
        if (hdr->meta_block_size != meta_block_size)
        {
            fprintf(stderr, "Using block size of %u bytes based on information from the superblock\n", hdr->meta_block_size);
            meta_block_size = hdr->meta_block_size;
            if (buf_size % meta_block_size)
            {
                buf_size = 8*meta_block_size;
                free(data);
                data = memalign_or_die(MEM_ALIGNMENT, buf_size);
            }
        }
        bitmap_granularity = hdr->bitmap_granularity;
        clean_entry_bitmap_size = hdr->data_block_size / hdr->bitmap_granularity / 8;
        clean_entry_size = sizeof(clean_disk_entry) + 2*clean_entry_bitmap_size;
        uint64_t block_num = 0;
        hdr_fn(hdr);
        meta_pos = meta_block_size;
        lseek64(meta_fd, meta_offset+meta_pos, 0);
        while (meta_pos < meta_len)
        {
            uint64_t read_len = buf_size < meta_len-meta_pos ? buf_size : meta_len-meta_pos;
            read_blocking(meta_fd, data, read_len);
            meta_pos += read_len;
            for (uint64_t blk = 0; blk < read_len; blk += meta_block_size)
            {
                for (uint64_t ioff = 0; ioff < meta_block_size-clean_entry_size; ioff += clean_entry_size, block_num++)
                {
                    clean_disk_entry *entry = (clean_disk_entry*)(data + blk + ioff);
                    if (entry->oid.inode)
                    {
                        record_fn(block_num, entry, entry->bitmap);
                    }
                }
            }
        }
    }
    else
    {
        // Vitastor 0.4-0.5 - static array of clean_disk_entry
        clean_entry_bitmap_size = 0;
        clean_entry_size = sizeof(clean_disk_entry);
        uint64_t block_num = 0;
        hdr_fn(NULL);
        while (meta_pos < meta_len)
        {
            uint64_t read_len = buf_size < meta_len-meta_pos ? buf_size : meta_len-meta_pos;
            read_blocking(meta_fd, data, read_len);
            meta_pos += read_len;
            for (uint64_t blk = 0; blk < read_len; blk += meta_block_size)
            {
                for (uint64_t ioff = 0; ioff < meta_block_size-clean_entry_size; ioff += clean_entry_size, block_num++)
                {
                    clean_disk_entry *entry = (clean_disk_entry*)(data + blk + ioff);
                    if (entry->oid.inode)
                    {
                        record_fn(block_num, entry, NULL);
                    }
                }
            }
        }
    }
    free(data);
    close(meta_fd);
    return 0;
}

int disk_tool_t::dump_meta()
{
    int r = process_meta(
        [this](blockstore_meta_header_v1_t *hdr) { dump_meta_header(hdr); },
        [this](uint64_t block_num, clean_disk_entry *entry, uint8_t *bitmap) { dump_meta_entry(block_num, entry, bitmap); }
    );
    printf("\n]}\n");
    return r;
}

void disk_tool_t::dump_meta_header(blockstore_meta_header_v1_t *hdr)
{
    if (hdr)
    {
        printf(
            "{\"version\":\"0.6\",\"meta_block_size\":%u,\"data_block_size\":%u,\"bitmap_granularity\":%u,\"entries\":[\n",
            hdr->meta_block_size, hdr->data_block_size, hdr->bitmap_granularity
        );
    }
    else
    {
        printf("{\"version\":\"0.5\",\"meta_block_size\":%u,\"entries\":[\n", meta_block_size);
    }
    first = true;
}

void disk_tool_t::dump_meta_entry(uint64_t block_num, clean_disk_entry *entry, uint8_t *bitmap)
{
    printf(
#define ENTRY_FMT "{\"block\":%lu,\"pool\":%u,\"inode\":%lu,\"stripe\":%lu,\"version\":%lu"
        (first ? ENTRY_FMT : (",\n" ENTRY_FMT)),
#undef ENTRY_FMT
        block_num, INODE_POOL(entry->oid.inode), INODE_NO_POOL(entry->oid.inode),
        entry->oid.stripe, entry->version
    );
    if (bitmap)
    {
        printf(",\"bitmap\":\"");
        for (uint64_t i = 0; i < clean_entry_bitmap_size; i++)
        {
            printf("%02x", bitmap[i]);
        }
        printf("\",\"ext_bitmap\":\"");
        for (uint64_t i = 0; i < clean_entry_bitmap_size; i++)
        {
            printf("%02x", bitmap[clean_entry_bitmap_size + i]);
        }
        printf("\"}");
    }
    else
    {
        printf("}");
    }
    first = false;
}

int disk_tool_t::resize_data()
{
    int r;
    // Parse parameters
    r = resize_parse_params();
    if (r != 0)
        return r;
    // Check parameters and fill allocator
    r = process_meta(
        [this](blockstore_meta_header_v1_t *hdr)
        {
            resize_init(hdr);
        },
        [this](uint64_t block_num, clean_disk_entry *entry, uint8_t *bitmap)
        {
            data_alloc->set(block_num, true);
        }
    );
    if (r != 0)
        return r;
    r = process_journal([this](void *buf)
    {
        return process_journal_block(buf, [this](int num, journal_entry *je)
        {
            if (je->type == JE_BIG_WRITE || je->type == JE_BIG_WRITE_INSTANT)
            {
                data_alloc->set(je->big_write.location / data_block_size, true);
            }
        });
    });
    if (r != 0)
        return r;
    // Remap blocks
    r = resize_remap_blocks();
    if (r != 0)
        return r;
    // Copy data blocks into new places
    r = resize_copy_data();
    if (r != 0)
        return r;
    // Rewrite journal
    r = resize_rewrite_journal();
    if (r != 0)
        return r;
    // Rewrite metadata
    r = resize_rewrite_meta();
    if (r != 0)
        return r;
    return 0;
}

int disk_tool_t::resize_parse_params()
{
    auto & config = options;
    // FIXME: Deduplicate with blockstore_open.cpp !
    journal_len = strtoull(config["journal_size"].c_str(), NULL, 10);
    data_device = config["data_device"];
    data_offset = strtoull(config["data_offset"].c_str(), NULL, 10);
    data_len = strtoull(config["data_size"].c_str(), NULL, 10);
    meta_device = config["meta_device"];
    meta_offset = strtoull(config["meta_offset"].c_str(), NULL, 10);
    data_block_size = strtoull(config["block_size"].c_str(), NULL, 10);
    journal_device = config["journal_device"];
    journal_offset = strtoull(config["journal_offset"].c_str(), NULL, 10);
    journal_block_size = strtoull(config["journal_block_size"].c_str(), NULL, 10);
    meta_block_size = strtoull(config["meta_block_size"].c_str(), NULL, 10);
    iodepth = strtoull(config["iodepth"].c_str(), NULL, 10);
    // Validate
    if (!data_block_size)
        data_block_size = (1 << DEFAULT_ORDER);
    if (data_block_size < MIN_BLOCK_SIZE || data_block_size >= MAX_BLOCK_SIZE)
        throw std::runtime_error("Bad block size");
    if (!journal_block_size)
        journal_block_size = 4096;
    else if (journal_block_size % DIRECT_IO_ALIGNMENT)
        throw std::runtime_error("journal_block_size must be a multiple of "+std::to_string(DIRECT_IO_ALIGNMENT));
    if (!meta_block_size)
        meta_block_size = 4096;
    else if (meta_block_size % DIRECT_IO_ALIGNMENT)
        throw std::runtime_error("meta_block_size must be a multiple of "+std::to_string(DIRECT_IO_ALIGNMENT));
    if (journal_device == meta_device || meta_device == "" && journal_device == data_device)
        journal_device = "";
    if (meta_device == data_device)
        meta_device = "";
    if (meta_offset % meta_block_size)
        throw std::runtime_error("meta_offset must be a multiple of meta_block_size = "+std::to_string(meta_block_size));
    if (journal_offset % journal_block_size)
        throw std::runtime_error("journal_offset must be a multiple of journal_block_size = "+std::to_string(journal_block_size));
    if (!iodepth)
        iodepth = 32;
    // Check offsets
    
}

void disk_tool_t::resize_init(blockstore_meta_header_v1_t *hdr)
{
    if (hdr && data_block_size != hdr->data_block_size)
    {
        if (data_block_size)
        {
            fprintf(stderr, "Using data block size of %u bytes from metadata superblock\n", hdr->data_block_size);
        }
        data_block_size = hdr->data_block_size;
    }
    if (((new_data_len-data_len) % data_block_size) ||
        ((new_data_offset-data_offset) % data_block_size))
    {
        fprintf(stderr, "Data alignment mismatch\n");
        exit(1);
    }
    data_idx_diff = (new_data_offset-data_offset) / data_block_size;
    free_first = new_data_offset > data_offset ? data_idx_diff : 0;
    free_last = (new_data_offset+new_data_len < data_offset+data_len)
        ? (data_offset+data_len-new_data_offset-new_data_len) / data_block_size
        : 0;
    new_clean_entry_bitmap_size = data_block_size / (hdr ? hdr->bitmap_granularity : 4096) / 8;
    new_clean_entry_size = sizeof(clean_disk_entry) + 2 * new_clean_entry_bitmap_size;
    new_entries_per_block = meta_block_size/new_clean_entry_size;
    uint64_t new_meta_blocks = 1 + (new_data_len/data_block_size + new_entries_per_block-1) / new_entries_per_block;
    if (new_meta_len < meta_block_size*new_meta_blocks)
    {
        fprintf(stderr, "New metadata area size is too small, should be at least %lu bytes\n", meta_block_size*new_meta_blocks);
        exit(1);
    }
}

int disk_tool_t::resize_remap_blocks()
{
    total_blocks = data_len / data_block_size;
    for (uint64_t i = 0; i < free_first; i++)
    {
        if (data_alloc->get(i))
            data_remap[i] = 0;
        else
            data_alloc->set(i, true);
    }
    for (uint64_t i = 0; i < free_last; i++)
    {
        if (data_alloc->get(total_blocks-i))
            data_remap[total_blocks-i] = 0;
        else
            data_alloc->set(total_blocks-i, true);
    }
    for (auto & p: data_remap)
    {
        uint64_t new_loc = data_alloc->find_free();
        if (new_loc == UINT64_MAX)
        {
            fprintf(stderr, "Not enough space to move data\n");
            return 1;
        }
        data_remap[p.first] = new_loc;
    }
    return 0;
}

int disk_tool_t::resize_copy_data()
{
    if (iodepth <= 0 || iodepth > 4096)
    {
        iodepth = 32;
    }
    ringloop = new ring_loop_t(iodepth < 512 ? 512 : iodepth);
    data_fd = open(data_device.c_str(), O_DIRECT|O_RDWR);
    if (data_fd < 0)
    {
        fprintf(stderr, "Failed to open data device %s: %s\n", data_device.c_str(), strerror(errno));
        delete ringloop;
        return 1;
    }
    moving_blocks = new resizer_data_moving_t[iodepth];
    moving_blocks[0].buf = memalign_or_die(MEM_ALIGNMENT, iodepth*data_block_size);
    for (int i = 1; i < iodepth; i++)
    {
        moving_blocks[i].buf = moving_blocks[0].buf + i*data_block_size;
    }
    remap_active = 1;
    remap_it = data_remap.begin();
    ring_consumer.loop = [this]()
    {
        remap_active = 0;
        for (int i = 0; i < iodepth; i++)
        {
            if (moving_blocks[i].state == DM_ST_EMPTY && remap_it != data_remap.end())
            {
                uint64_t old_loc = remap_it->first, new_loc = remap_it->second;
                moving_blocks[i].state = DM_ST_TO_READ;
                moving_blocks[i].old_loc = old_loc;
                moving_blocks[i].new_loc = new_loc;
                remap_it++;
            }
            if (moving_blocks[i].state == DM_ST_TO_READ)
            {
                struct io_uring_sqe *sqe = ringloop->get_sqe();
                if (sqe)
                {
                    moving_blocks[i].state = DM_ST_READING;
                    struct ring_data_t *data = ((ring_data_t*)sqe->user_data);
                    data->iov = (struct iovec){ moving_blocks[i].buf, data_block_size };
                    my_uring_prep_readv(sqe, data_fd, &data->iov, 1, data_offset + moving_blocks[i].old_loc*data_block_size);
                    data->callback = [this, i](ring_data_t *data)
                    {
                        if (data->res != data_block_size)
                        {
                            fprintf(
                                stderr, "Failed to read %lu bytes at %lu from %s: %s\n", data_block_size,
                                data_offset + moving_blocks[i].old_loc*data_block_size, data_device.c_str(),
                                data->res < 0 ? strerror(-data->res) : "short read"
                            );
                            exit(1);
                        }
                        moving_blocks[i].state = DM_ST_TO_WRITE;
                        ringloop->wakeup();
                    };
                 }
            }
            if (moving_blocks[i].state == DM_ST_TO_WRITE)
            {
                struct io_uring_sqe *sqe = ringloop->get_sqe();
                if (sqe)
                {
                    moving_blocks[i].state = DM_ST_WRITING;
                    struct ring_data_t *data = ((ring_data_t*)sqe->user_data);
                    data->iov = (struct iovec){ moving_blocks[i].buf, data_block_size };
                    my_uring_prep_writev(sqe, data_fd, &data->iov, 1, data_offset + moving_blocks[i].new_loc*data_block_size);
                    data->callback = [this, i](ring_data_t *data)
                    {
                        if (data->res != data_block_size)
                        {
                            fprintf(
                                stderr, "Failed to write %lu bytes at %lu to %s: %s\n", data_block_size,
                                data_offset + moving_blocks[i].new_loc*data_block_size, data_device.c_str(),
                                data->res < 0 ? strerror(-data->res) : "short write"
                            );
                            exit(1);
                        }
                        moving_blocks[i].state = DM_ST_EMPTY;
                        ringloop->wakeup();
                    };
                }
            }
            remap_active += moving_blocks[i].state != DM_ST_EMPTY ? 1 : 0;
        }
        ringloop->submit();
    };
    ringloop->register_consumer(&ring_consumer);
    while (1)
    {
        ringloop->loop();
        if (!remap_active)
            break;
        ringloop->wait();
    }
    ringloop->unregister_consumer(&ring_consumer);
    free(moving_blocks[0].buf);
    delete[] moving_blocks;
    close(data_fd);
    delete ringloop;
    return 0;
}

int disk_tool_t::resize_rewrite_journal()
{
    // Simply overwriting on the fly may be impossible because old and new areas may overlap
    // For now, just build new journal data in memory
    new_buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, new_journal_len);
    new_journal_ptr = new_buf;
    new_journal_data = new_journal_ptr + journal_block_size;
    memset(new_buf, 0, new_journal_len);
    process_journal([this](void *buf)
    {
        return process_journal_block(buf, [this](int num, journal_entry *je)
        {
            journal_entry *ne = (journal_entry*)(new_journal_ptr + new_journal_in_pos);
            if (je->type == JE_START)
            {
                *((journal_entry_start*)ne) = (journal_entry_start){
                    .magic = JOURNAL_MAGIC,
                    .type = JE_START,
                    .size = sizeof(ne->start),
                    .journal_start = journal_block_size,
                    .version = JOURNAL_VERSION,
                };
                ne->crc32 = je_crc32(ne);
                new_journal_ptr += journal_block_size;
            }
            else
            {
                if (journal_block_size < new_journal_in_pos+je->size)
                {
                    new_journal_ptr = new_journal_data;
                    if (new_journal_ptr-new_buf >= new_journal_len)
                    {
                        fprintf(stderr, "Error: live entries don't fit to the new journal\n");
                        exit(1);
                    }
                    new_journal_data += journal_block_size;
                    new_journal_in_pos = 0;
                    if (journal_block_size < je->size)
                    {
                        fprintf(stderr, "Error: journal entry too large (%u bytes)\n", je->size);
                        exit(1);
                    }
                }
                memcpy(ne, je, je->size);
                ne->crc32_prev = new_crc32_prev;
                if (je->type == JE_BIG_WRITE || je->type == JE_BIG_WRITE_INSTANT)
                {
                    // Change the block reference
                    auto remap_it = data_remap.find(ne->big_write.location / data_block_size);
                    if (remap_it != data_remap.end())
                    {
                        ne->big_write.location = remap_it->second * data_block_size;
                    }
                    ne->big_write.location += data_idx_diff;
                }
                else if (je->type == JE_SMALL_WRITE || je->type == JE_SMALL_WRITE_INSTANT)
                {
                    ne->small_write.data_offset = new_journal_data-new_buf;
                    if (ne->small_write.data_offset + ne->small_write.len > new_journal_len)
                    {
                        fprintf(stderr, "Error: live entries don't fit to the new journal\n");
                        exit(1);
                    }
                    memcpy(new_journal_data, small_write_data, ne->small_write.len);
                    new_journal_data += ne->small_write.len;
                }
                ne->crc32 = je_crc32(ne);
                new_journal_in_pos += ne->size;
                new_crc32_prev = ne->crc32;
            }
        });
    });
    // FIXME: Write new journal and metadata with journaling if they overlap with old
    new_journal_fd = open(new_journal_device, O_DIRECT|O_RDWR);
    if (new_journal_fd < 0)
    {
        fprintf(stderr, "Failed to open new journal device %s: %s\n", new_journal_device, strerror(errno));
        return 1;
    }
    lseek64(new_journal_fd, new_journal_offset, 0);
    write_blocking(new_journal_fd, new_buf, new_journal_len);
    fsync(new_journal_fd);
    close(new_journal_fd);
    free(new_buf);
    return 0;
}

int disk_tool_t::resize_rewrite_meta()
{
    new_buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, new_meta_len);
    memset(new_buf, 0, new_meta_len);
    int r = process_meta(
        [this](blockstore_meta_header_v1_t *hdr)
        {
            blockstore_meta_header_v1_t *new_hdr = (blockstore_meta_header_v1_t *)new_buf;
            new_hdr->zero = 0;
            new_hdr->magic = BLOCKSTORE_META_MAGIC_V1;
            new_hdr->version = BLOCKSTORE_META_VERSION_V1;
            new_hdr->meta_block_size = meta_block_size;
            new_hdr->data_block_size = data_block_size;
            new_hdr->bitmap_granularity = bitmap_granularity ? bitmap_granularity : 4096;
        },
        [this](uint64_t block_num, clean_disk_entry *entry, uint8_t *bitmap)
        {
            auto remap_it = data_remap.find(block_num);
            if (remap_it != data_remap.end())
                block_num = remap_it->second;
            if (block_num < free_first || block_num >= total_blocks-free_last)
                return;
            block_num += data_idx_diff;
            clean_disk_entry *new_entry = (clean_disk_entry*)(new_buf + meta_block_size +
                meta_block_size*(block_num / new_entries_per_block) +
                new_clean_entry_size*(block_num % new_entries_per_block));
            new_entry->oid = entry->oid;
            new_entry->version = entry->version;
            if (bitmap)
                memcpy(new_entry->bitmap, bitmap, 2*new_clean_entry_bitmap_size);
            else
                memset(new_entry->bitmap, 0xff, 2*new_clean_entry_bitmap_size);
        }
    );
    if (r != 0)
    {
        free(new_buf);
        return r;
    }
    new_meta_fd = open(new_meta_device, O_DIRECT|O_RDWR);
    if (new_meta_fd < 0)
    {
        fprintf(stderr, "Failed to open new metadata device %s: %s\n", new_meta_device, strerror(errno));
        return 1;
    }
    lseek64(new_meta_fd, new_meta_offset, 0);
    write_blocking(new_meta_fd, new_buf, new_meta_len);
    fsync(new_meta_fd);
    close(new_meta_fd);
    free(new_buf);
    return 0;
}
