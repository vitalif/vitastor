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

#include "json11/json11.hpp"
#include "blockstore_impl.h"
#include "blockstore_disk.h"
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
    bool all;
    bool json;
    blockstore_disk_t dsk;

    // resize data and/or move metadata and journal
    int iodepth;
    std::string new_meta_device, new_journal_device;
    uint64_t new_data_offset, new_data_len;
    uint64_t new_journal_offset, new_journal_len;
    uint64_t new_meta_offset, new_meta_len;

    /**** State ****/

    uint64_t meta_pos;
    uint64_t journal_pos, journal_calc_data_pos;

    bool first, first2;

    allocator *data_alloc;
    std::map<uint64_t, uint64_t> data_remap;
    std::map<uint64_t, uint64_t>::iterator remap_it;
    ring_loop_t *ringloop;
    ring_consumer_t ring_consumer;
    int remap_active;
    uint8_t *new_journal_buf, *new_meta_buf, *new_journal_ptr, *new_journal_data;
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

    ~disk_tool_t();

    void dump_journal_entry(int num, journal_entry *je, bool json);
    int process_journal(std::function<int(void*)> block_fn);
    int process_journal_block(void *buf, std::function<void(int, journal_entry*)> iter_fn);
    int process_meta(std::function<void(blockstore_meta_header_v1_t *)> hdr_fn,
        std::function<void(uint64_t, clean_disk_entry*, uint8_t*)> record_fn);
    void dump_meta_header(blockstore_meta_header_v1_t *hdr);
    void dump_meta_entry(uint64_t block_num, clean_disk_entry *entry, uint8_t *bitmap);

    int resize_parse_params();
    void resize_init(blockstore_meta_header_v1_t *hdr);
    int resize_remap_blocks();
    int resize_copy_data();
    int resize_rewrite_journal();
    int resize_write_new_journal();
    int resize_rewrite_meta();
    int resize_write_new_meta();
};

void disk_tool_simple_offsets(json11::Json cfg, bool json_output);

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
        else if (!strcmp(argv[i], "--json"))
        {
            self.json = true;
        }
        else if (!strcmp(argv[i], "--help"))
        {
            cmd.clear();
            cmd.push_back((char*)"help");
        }
        else if (!strcmp(argv[i], "--force"))
        {
            self.options["force"] = "1";
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
            fprintf(stderr, "USAGE: %s%s [--all] [--json] <journal_file> <journal_block_size> <offset> <size>\n", argv[0], aliased ? "" : " dump-journal");
            return 1;
        }
        self.dsk.journal_device = cmd[1];
        self.dsk.journal_block_size = strtoul(cmd[2], NULL, 10);
        self.dsk.journal_offset = strtoull(cmd[3], NULL, 10);
        self.dsk.journal_len = strtoull(cmd[4], NULL, 10);
        return self.dump_journal();
    }
    else if (cmd.size() && !strcmp(cmd[0], "dump-meta"))
    {
        if (cmd.size() < 5)
        {
            fprintf(stderr, "USAGE: %s dump-meta <meta_file> <meta_block_size> <offset> <size>\n", argv[0]);
            return 1;
        }
        self.dsk.meta_device = cmd[1];
        self.dsk.meta_block_size = strtoul(cmd[2], NULL, 10);
        self.dsk.meta_offset = strtoull(cmd[3], NULL, 10);
        self.dsk.meta_len = strtoull(cmd[4], NULL, 10);
        return self.dump_meta();
    }
    else if (cmd.size() && !strcmp(cmd[0], "resize"))
    {
        return self.resize_data();
    }
    else if (cmd.size() && !strcmp(cmd[0], "simple-offsets"))
    {
        // Calculate offsets for simple & stupid OSD deployment without superblock
        if (cmd.size() > 1)
        {
            self.options["device"] = cmd[1];
        }
        disk_tool_simple_offsets(self.options, self.json);
        return 0;
    }
    else
    {
        printf(
            "Vitastor disk management tool\n"
            "(c) Vitaliy Filippov, 2022+ (VNPL-1.1)\n"
            "\n"
            "USAGE:\n"
            "%s dump-journal [--all] [--json] <journal_file> <journal_block_size> <offset> <size>\n"
            "  Dump journal in human-readable or JSON (if --json is specified) format.\n"
            "  Without --all, only actual part of the journal is dumped.\n"
            "  With --all, the whole journal area is scanned for journal entries,\n"
            "  some of which may be outdated.\n"
            "\n"
            "%s dump-meta <meta_file> <meta_block_size> <offset> <size>\n"
            "  Dump metadata in JSON format.\n"
            "\n"
            "%s resize <ALL_OSD_PARAMETERS> <NEW_PARAMETERS> [--iodepth 32]\n"
            "  Resize data area and/or rewrite/move journal and metadata\n"
            "  ALL_OSD_PARAMETERS must include all (at least all disk-related)\n"
            "  parameters from OSD command line (i.e. from systemd unit).\n"
            "  NEW_PARAMETERS include new disk layout parameters:\n"
            "    [--new_data_offset <NUMBER>]     resize data area so it starts at <NUMBER>\n"
            "    [--new_data_len <NUMBER>]        resize data area to <NUMBER> bytes\n"
            "    [--new_meta_device <PATH>]       use <PATH> for new metadata\n"
            "    [--new_meta_offset <NUMBER>]     make new metadata area start at <NUMBER>\n"
            "    [--new_meta_len <NUMBER>]        make new metadata area <NUMBER> bytes long\n"
            "    [--new_journal_device <PATH>]    use <PATH> for new journal\n"
            "    [--new_journal_offset <NUMBER>]  make new journal area start at <NUMBER>\n"
            "    [--new_journal_len <NUMBER>]     make new journal area <NUMBER> bytes long\n"
            "  If any of the new layout parameter options are not specified, old values\n"
            "  will be used.\n"
            "\n"
            "%s simple-offsets <device>\n"
            "  Calculate offsets for simple&stupid (no superblock) OSD deployment. Options:\n"
            "  --object_size 128k       Set blockstore block size\n"
            "  --bitmap_granularity 4k  Set bitmap granularity\n"
            "  --journal_size 16M       Set journal size\n"
            "  --device_block_size 4k   Set device block size\n"
            "  --journal_offset 0       Set journal offset\n"
            "  --device_size 0          Set device size\n"
            "  --format text            Result format: json, options, env, or text\n"
            ,
            argv[0], argv[0], argv[0], argv[0]
        );
    }
    return 0;
}

disk_tool_t::~disk_tool_t()
{
    if (data_alloc)
    {
        delete data_alloc;
        data_alloc = NULL;
    }
}

int disk_tool_t::dump_journal()
{
    if (dsk.journal_block_size < DIRECT_IO_ALIGNMENT || (dsk.journal_block_size % DIRECT_IO_ALIGNMENT) ||
        dsk.journal_block_size > 128*1024)
    {
        fprintf(stderr, "Invalid journal block size\n");
        return 1;
    }
    first = true;
    if (json)
        printf("[\n");
    if (all)
    {
        dsk.journal_fd = open(dsk.journal_device.c_str(), O_DIRECT|O_RDONLY);
        if (dsk.journal_fd < 0)
        {
            fprintf(stderr, "Failed to open journal device %s: %s\n", dsk.journal_device.c_str(), strerror(errno));
            return 1;
        }
        void *journal_buf = memalign_or_die(MEM_ALIGNMENT, dsk.journal_block_size);
        journal_pos = 0;
        while (journal_pos < dsk.journal_len)
        {
            int r = pread(dsk.journal_fd, journal_buf, dsk.journal_block_size, dsk.journal_offset+journal_pos);
            assert(r == dsk.journal_block_size);
            uint64_t s;
            for (s = 0; s < dsk.journal_block_size; s += 8)
            {
                if (*((uint64_t*)((uint8_t*)journal_buf+s)) != 0)
                    break;
            }
            if (json)
            {
                printf("%s{\"offset\":\"0x%lx\"", first ? "" : ",\n", journal_pos);
                first = false;
            }
            if (s == dsk.journal_block_size)
            {
                if (json)
                    printf(",\"type\":\"zero\"}");
                else
                    printf("offset %08lx: zeroes\n", journal_pos);
                journal_pos += dsk.journal_block_size;
            }
            else if (((journal_entry*)journal_buf)->magic == JOURNAL_MAGIC)
            {
                if (!json)
                    printf("offset %08lx:\n", journal_pos);
                else
                    printf(",\"entries\":[\n");
                first2 = true;
                process_journal_block(journal_buf, [this](int num, journal_entry *je) { dump_journal_entry(num, je, json); });
                if (json)
                    printf(first2 ? "]}" : "\n]}");
            }
            else
            {
                if (json)
                    printf(",\"type\":\"data\",\"pattern\":\"%08lx\"}", *((uint64_t*)journal_buf));
                else
                    printf("offset %08lx: no magic in the beginning, looks like random data (pattern=%08lx)\n", journal_pos, *((uint64_t*)journal_buf));
                journal_pos += dsk.journal_block_size;
            }
        }
        free(journal_buf);
        close(dsk.journal_fd);
        dsk.journal_fd = -1;
    }
    else
    {
        process_journal([this](void *data)
        {
            first2 = true;
            if (!json)
                printf("offset %08lx:\n", journal_pos);
            auto pos = journal_pos;
            int r = process_journal_block(data, [this, pos](int num, journal_entry *je)
            {
                if (json && first2)
                {
                    printf("%s{\"offset\":\"0x%lx\",\"entries\":[\n", first ? "" : ",\n", pos);
                    first = false;
                }
                dump_journal_entry(num, je, json);
            });
            if (json)
                printf(first2 ? "" : "\n]}");
            else if (r <= 0)
                printf("end of the journal\n");
            return r;
        });
    }
    if (json)
        printf(first ? "]\n" : "\n]\n");
    return 0;
}

int disk_tool_t::process_journal(std::function<int(void*)> block_fn)
{
    dsk.journal_fd = open(dsk.journal_device.c_str(), O_DIRECT|O_RDONLY);
    if (dsk.journal_fd < 0)
    {
        fprintf(stderr, "Failed to open journal device %s: %s\n", dsk.journal_device.c_str(), strerror(errno));
        return 1;
    }
    void *data = memalign_or_die(MEM_ALIGNMENT, dsk.journal_block_size);
    journal_pos = 0;
    int r = pread(dsk.journal_fd, data, dsk.journal_block_size, dsk.journal_offset+journal_pos);
    assert(r == dsk.journal_block_size);
    journal_entry *je = (journal_entry*)(data);
    if (je->magic != JOURNAL_MAGIC || je->type != JE_START || je_crc32(je) != je->crc32)
    {
        fprintf(stderr, "offset %08lx: journal superblock is invalid\n", journal_pos);
        r = 1;
    }
    else
    {
        started = false;
        crc32_last = 0;
        block_fn(data);
        started = false;
        crc32_last = 0;
        journal_pos = je->start.journal_start;
        while (1)
        {
            if (journal_pos >= dsk.journal_len)
                journal_pos = dsk.journal_block_size;
            r = pread(dsk.journal_fd, data, dsk.journal_block_size, dsk.journal_offset+journal_pos);
            assert(r == dsk.journal_block_size);
            r = block_fn(data);
            if (r <= 0)
                break;
        }
    }
    close(dsk.journal_fd);
    dsk.journal_fd = -1;
    free(data);
    return r;
}

int disk_tool_t::process_journal_block(void *buf, std::function<void(int, journal_entry*)> iter_fn)
{
    uint32_t pos = 0;
    journal_pos += dsk.journal_block_size;
    int entry = 0;
    bool wrapped = false;
    while (pos <= dsk.journal_block_size-JOURNAL_ENTRY_HEADER_SIZE)
    {
        journal_entry *je = (journal_entry*)((uint8_t*)buf + pos);
        if (je->magic != JOURNAL_MAGIC || je->type < JE_MIN || je->type > JE_MAX ||
            !all && started && je->crc32_prev != crc32_last || pos > dsk.journal_block_size-je->size)
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
            if (journal_pos + je->small_write.len > dsk.journal_len)
            {
                // data continues from the beginning of the journal
                journal_calc_data_pos = journal_pos = dsk.journal_block_size;
                wrapped = true;
            }
            journal_pos += je->small_write.len;
            if (journal_pos >= dsk.journal_len)
            {
                journal_pos = dsk.journal_block_size;
                wrapped = true;
            }
            small_write_data = memalign_or_die(MEM_ALIGNMENT, je->small_write.len);
            assert(pread(dsk.journal_fd, small_write_data, je->small_write.len, dsk.journal_offset+je->small_write.data_offset) == je->small_write.len);
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
        journal_pos = dsk.journal_len;
    }
    return entry;
}

void disk_tool_t::dump_journal_entry(int num, journal_entry *je, bool json)
{
    if (json)
    {
        if (!first2)
            printf(",\n");
        first2 = false;
        printf(
            "{\"crc32\":\"%08x\",\"valid\":%s,\"crc32_prev\":\"%08x\"",
            je->crc32, (je_crc32(je) == je->crc32 ? "true" : "false"), je->crc32_prev
        );
    }
    else
    {
        printf(
            "entry % 3d: crc32=%08x %s prev=%08x ",
            num, je->crc32, (je_crc32(je) == je->crc32 ? "(valid)" : "(invalid)"), je->crc32_prev
        );
    }
    if (je->type == JE_START)
    {
        printf(
            json ? ",\"type\":\"start\",\"start\":\"0x%lx\"}" : "je_start start=%08lx\n",
            je->start.journal_start
        );
    }
    else if (je->type == JE_SMALL_WRITE || je->type == JE_SMALL_WRITE_INSTANT)
    {
        printf(
            json ? ",\"type\":\"small_write%s\",\"inode\":\"0x%lx\",\"stripe\":\"0x%lx\",\"ver\":\"%lu\",\"offset\":%u,\"len\":%u,\"loc\":\"0x%lx\""
                : "je_small_write%s oid=%lx:%lx ver=%lu offset=%u len=%u loc=%08lx",
            je->type == JE_SMALL_WRITE_INSTANT ? "_instant" : "",
            je->small_write.oid.inode, je->small_write.oid.stripe,
            je->small_write.version, je->small_write.offset, je->small_write.len,
            je->small_write.data_offset
        );
        if (journal_calc_data_pos != je->small_write.data_offset)
        {
            printf(json ? ",\"bad_loc\":true,\"calc_loc\":\"0x%lx\""
                : " (mismatched, calculated = %lu)", journal_pos);
        }
        printf(
            json ? ",\"data_crc32\":\"%08x\",\"data_valid\":%s}" : " data_crc32=%08x%s\n",
            je->small_write.crc32_data,
            (data_crc32 != je->small_write.crc32_data
                ? (json ? "false" : " (invalid)")
                : (json ? "true" : " (valid)"))
        );
    }
    else if (je->type == JE_BIG_WRITE || je->type == JE_BIG_WRITE_INSTANT)
    {
        printf(
            json ? ",\"type\":\"big_write%s\",\"inode\":\"0x%lx\",\"stripe\":\"0x%lx\",\"ver\":\"%lu\",\"loc\":\"0x%lx\"}"
                : "je_big_write%s oid=%lx:%lx ver=%lu loc=%08lx\n",
            je->type == JE_BIG_WRITE_INSTANT ? "_instant" : "",
            je->big_write.oid.inode, je->big_write.oid.stripe, je->big_write.version, je->big_write.location
        );
    }
    else if (je->type == JE_STABLE)
    {
        printf(
            json ? ",\"type\":\"stable\",\"inode\":\"0x%lx\",\"stripe\":\"0x%lx\",\"ver\":\"%lu\"}"
                : "je_stable oid=%lx:%lx ver=%lu\n",
            je->stable.oid.inode, je->stable.oid.stripe, je->stable.version
        );
    }
    else if (je->type == JE_ROLLBACK)
    {
        printf(
            json ? ",\"type\":\"rollback\",\"inode\":\"0x%lx\",\"stripe\":\"0x%lx\",\"ver\":\"%lu\"}"
                : "je_rollback oid=%lx:%lx ver=%lu\n",
            je->rollback.oid.inode, je->rollback.oid.stripe, je->rollback.version
        );
    }
    else if (je->type == JE_DELETE)
    {
        printf(
            json ? ",\"type\":\"delete\",\"inode\":\"0x%lx\",\"stripe\":\"0x%lx\",\"ver\":\"%lu\"}"
                : "je_delete oid=%lx:%lx ver=%lu\n",
            je->del.oid.inode, je->del.oid.stripe, je->del.version
        );
    }
}

int disk_tool_t::process_meta(std::function<void(blockstore_meta_header_v1_t *)> hdr_fn,
    std::function<void(uint64_t, clean_disk_entry*, uint8_t*)> record_fn)
{
    if (dsk.meta_block_size % DIRECT_IO_ALIGNMENT)
    {
        fprintf(stderr, "Invalid metadata block size: is not a multiple of %d\n", DIRECT_IO_ALIGNMENT);
        return 1;
    }
    dsk.meta_fd = open(dsk.meta_device.c_str(), O_DIRECT|O_RDONLY);
    if (dsk.meta_fd < 0)
    {
        fprintf(stderr, "Failed to open metadata device %s: %s\n", dsk.meta_device.c_str(), strerror(errno));
        return 1;
    }
    int buf_size = 1024*1024;
    if (buf_size % dsk.meta_block_size)
        buf_size = 8*dsk.meta_block_size;
    if (buf_size > dsk.meta_len)
        buf_size = dsk.meta_len;
    void *data = memalign_or_die(MEM_ALIGNMENT, buf_size);
    lseek64(dsk.meta_fd, dsk.meta_offset, 0);
    read_blocking(dsk.meta_fd, data, buf_size);
    // Check superblock
    blockstore_meta_header_v1_t *hdr = (blockstore_meta_header_v1_t *)data;
    if (hdr->zero == 0 &&
        hdr->magic == BLOCKSTORE_META_MAGIC_V1 &&
        hdr->version == BLOCKSTORE_META_VERSION_V1)
    {
        // Vitastor 0.6-0.7 - static array of clean_disk_entry with bitmaps
        if (hdr->meta_block_size != dsk.meta_block_size)
        {
            fprintf(stderr, "Using block size of %u bytes based on information from the superblock\n", hdr->meta_block_size);
            dsk.meta_block_size = hdr->meta_block_size;
            if (buf_size % dsk.meta_block_size)
            {
                buf_size = 8*dsk.meta_block_size;
                free(data);
                data = memalign_or_die(MEM_ALIGNMENT, buf_size);
            }
        }
        dsk.bitmap_granularity = hdr->bitmap_granularity;
        dsk.clean_entry_bitmap_size = hdr->data_block_size / hdr->bitmap_granularity / 8;
        dsk.clean_entry_size = sizeof(clean_disk_entry) + 2*dsk.clean_entry_bitmap_size;
        uint64_t block_num = 0;
        hdr_fn(hdr);
        meta_pos = dsk.meta_block_size;
        lseek64(dsk.meta_fd, dsk.meta_offset+meta_pos, 0);
        while (meta_pos < dsk.meta_len)
        {
            uint64_t read_len = buf_size < dsk.meta_len-meta_pos ? buf_size : dsk.meta_len-meta_pos;
            read_blocking(dsk.meta_fd, data, read_len);
            meta_pos += read_len;
            for (uint64_t blk = 0; blk < read_len; blk += dsk.meta_block_size)
            {
                for (uint64_t ioff = 0; ioff <= dsk.meta_block_size-dsk.clean_entry_size; ioff += dsk.clean_entry_size, block_num++)
                {
                    clean_disk_entry *entry = (clean_disk_entry*)((uint8_t*)data + blk + ioff);
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
        dsk.clean_entry_bitmap_size = 0;
        dsk.clean_entry_size = sizeof(clean_disk_entry);
        uint64_t block_num = 0;
        hdr_fn(NULL);
        while (meta_pos < dsk.meta_len)
        {
            uint64_t read_len = buf_size < dsk.meta_len-meta_pos ? buf_size : dsk.meta_len-meta_pos;
            read_blocking(dsk.meta_fd, data, read_len);
            meta_pos += read_len;
            for (uint64_t blk = 0; blk < read_len; blk += dsk.meta_block_size)
            {
                for (uint64_t ioff = 0; ioff < dsk.meta_block_size-dsk.clean_entry_size; ioff += dsk.clean_entry_size, block_num++)
                {
                    clean_disk_entry *entry = (clean_disk_entry*)((uint8_t*)data + blk + ioff);
                    if (entry->oid.inode)
                    {
                        record_fn(block_num, entry, NULL);
                    }
                }
            }
        }
    }
    free(data);
    close(dsk.meta_fd);
    dsk.meta_fd = -1;
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
        printf("{\"version\":\"0.5\",\"meta_block_size\":%lu,\"entries\":[\n", dsk.meta_block_size);
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
        for (uint64_t i = 0; i < dsk.clean_entry_bitmap_size; i++)
        {
            printf("%02x", bitmap[i]);
        }
        printf("\",\"ext_bitmap\":\"");
        for (uint64_t i = 0; i < dsk.clean_entry_bitmap_size; i++)
        {
            printf("%02x", bitmap[dsk.clean_entry_bitmap_size + i]);
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
    fprintf(stderr, "Reading metadata\n");
    data_alloc = new allocator((new_data_len < dsk.data_len ? dsk.data_len : new_data_len) / dsk.data_block_size);
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
    fprintf(stderr, "Reading journal\n");
    r = process_journal([this](void *buf)
    {
        return process_journal_block(buf, [this](int num, journal_entry *je)
        {
            if (je->type == JE_BIG_WRITE || je->type == JE_BIG_WRITE_INSTANT)
            {
                data_alloc->set(je->big_write.location / dsk.data_block_size, true);
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
    fprintf(stderr, "Moving data blocks\n");
    r = resize_copy_data();
    if (r != 0)
        return r;
    // Rewrite journal
    fprintf(stderr, "Rebuilding journal\n");
    r = resize_rewrite_journal();
    if (r != 0)
        return r;
    // Rewrite metadata
    fprintf(stderr, "Rebuilding metadata\n");
    r = resize_rewrite_meta();
    if (r != 0)
        return r;
    // Write new journal
    fprintf(stderr, "Writing new journal\n");
    r = resize_write_new_journal();
    if (r != 0)
        return r;
    // Write new metadata
    fprintf(stderr, "Writing new metadata\n");
    r = resize_write_new_meta();
    if (r != 0)
        return r;
    fprintf(stderr, "Done\n");
    return 0;
}

int disk_tool_t::resize_parse_params()
{
    try
    {
        dsk.parse_config(options);
        dsk.open_data();
        dsk.open_meta();
        dsk.open_journal();
        dsk.calc_lengths();
        dsk.close_all();
    }
    catch (std::exception & e)
    {
        dsk.close_all();
        fprintf(stderr, "Error: %s\n", e.what());
        return 1;
    }
    iodepth = strtoull(options["iodepth"].c_str(), NULL, 10);
    if (!iodepth)
        iodepth = 32;
    new_meta_device = options.find("new_meta_device") != options.end()
        ? options["new_meta_device"] : dsk.meta_device;
    new_journal_device = options.find("new_journal_device") != options.end()
        ? options["new_journal_device"] : dsk.journal_device;
    new_data_offset = options.find("new_data_offset") != options.end()
        ? strtoull(options["new_data_offset"].c_str(), NULL, 10) : dsk.data_offset;
    new_data_len = options.find("new_data_len") != options.end()
        ? strtoull(options["new_data_len"].c_str(), NULL, 10) : dsk.data_len;
    new_meta_offset = options.find("new_meta_offset") != options.end()
        ? strtoull(options["new_meta_offset"].c_str(), NULL, 10) : dsk.meta_offset;
    new_meta_len = options.find("new_meta_len") != options.end()
        ? strtoull(options["new_meta_len"].c_str(), NULL, 10) : 0; // will be calculated in resize_init()
    new_journal_offset = options.find("new_journal_offset") != options.end()
        ? strtoull(options["new_journal_offset"].c_str(), NULL, 10) : dsk.journal_offset;
    new_journal_len = options.find("new_journal_len") != options.end()
        ? strtoull(options["new_journal_len"].c_str(), NULL, 10) : dsk.journal_len;
    if (new_meta_device == dsk.meta_device &&
        new_journal_device == dsk.journal_device &&
        new_data_offset == dsk.data_offset &&
        new_data_len == dsk.data_len &&
        new_meta_offset == dsk.meta_offset &&
        (new_meta_len == dsk.meta_len || new_meta_len == 0) &&
        new_journal_offset == dsk.journal_offset &&
        new_journal_len == dsk.journal_len &&
        options.find("force") == options.end())
    {
        // No difference
        fprintf(stderr, "No difference, specify --force to rewrite journal and meta anyway\n");
        return 1;
    }
    return 0;
}

void disk_tool_t::resize_init(blockstore_meta_header_v1_t *hdr)
{
    if (hdr && dsk.data_block_size != hdr->data_block_size)
    {
        if (dsk.data_block_size)
        {
            fprintf(stderr, "Using data block size of %u bytes from metadata superblock\n", hdr->data_block_size);
        }
        dsk.data_block_size = hdr->data_block_size;
    }
    if (((new_data_len-dsk.data_len) % dsk.data_block_size) ||
        ((new_data_offset-dsk.data_offset) % dsk.data_block_size))
    {
        fprintf(stderr, "Data alignment mismatch\n");
        exit(1);
    }
    data_idx_diff = ((int64_t)(dsk.data_offset-new_data_offset)) / dsk.data_block_size;
    free_first = new_data_offset > dsk.data_offset ? (new_data_offset-dsk.data_offset) / dsk.data_block_size : 0;
    free_last = (new_data_offset+new_data_len < dsk.data_offset+dsk.data_len)
        ? (dsk.data_offset+dsk.data_len-new_data_offset-new_data_len) / dsk.data_block_size
        : 0;
    new_clean_entry_bitmap_size = dsk.data_block_size / (hdr ? hdr->bitmap_granularity : 4096) / 8;
    new_clean_entry_size = sizeof(clean_disk_entry) + 2 * new_clean_entry_bitmap_size;
    new_entries_per_block = dsk.meta_block_size/new_clean_entry_size;
    uint64_t new_meta_blocks = 1 + (new_data_len/dsk.data_block_size + new_entries_per_block-1) / new_entries_per_block;
    if (!new_meta_len)
    {
        new_meta_len = dsk.meta_block_size*new_meta_blocks;
    }
    if (new_meta_len < dsk.meta_block_size*new_meta_blocks)
    {
        fprintf(stderr, "New metadata area size is too small, should be at least %lu bytes\n", dsk.meta_block_size*new_meta_blocks);
        exit(1);
    }
    // Check that new metadata, journal and data areas don't overlap
    if (new_meta_device == dsk.data_device && new_meta_offset < new_data_offset+new_data_len &&
        new_meta_offset+new_meta_len > new_data_offset)
    {
        fprintf(stderr, "New metadata area overlaps with data\n");
        exit(1);
    }
    if (new_journal_device == dsk.data_device && new_journal_offset < new_data_offset+new_data_len &&
        new_journal_offset+new_journal_len > new_data_offset)
    {
        fprintf(stderr, "New journal area overlaps with data\n");
        exit(1);
    }
    if (new_journal_device == new_meta_device && new_journal_offset < new_meta_offset+new_meta_len &&
        new_journal_offset+new_journal_len > new_meta_offset)
    {
        fprintf(stderr, "New journal area overlaps with metadata\n");
        exit(1);
    }
}

int disk_tool_t::resize_remap_blocks()
{
    total_blocks = dsk.data_len / dsk.data_block_size;
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
        data_alloc->set(new_loc, true);
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
    dsk.data_fd = open(dsk.data_device.c_str(), O_DIRECT|O_RDWR);
    if (dsk.data_fd < 0)
    {
        fprintf(stderr, "Failed to open data device %s: %s\n", dsk.data_device.c_str(), strerror(errno));
        delete ringloop;
        ringloop = NULL;
        return 1;
    }
    moving_blocks = new resizer_data_moving_t[iodepth];
    moving_blocks[0].buf = memalign_or_die(MEM_ALIGNMENT, iodepth*dsk.data_block_size);
    for (int i = 1; i < iodepth; i++)
    {
        moving_blocks[i].buf = (uint8_t*)moving_blocks[0].buf + i*dsk.data_block_size;
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
                    data->iov = (struct iovec){ moving_blocks[i].buf, dsk.data_block_size };
                    my_uring_prep_readv(sqe, dsk.data_fd, &data->iov, 1, dsk.data_offset + moving_blocks[i].old_loc*dsk.data_block_size);
                    data->callback = [this, i](ring_data_t *data)
                    {
                        if (data->res != dsk.data_block_size)
                        {
                            fprintf(
                                stderr, "Failed to read %u bytes at %lu from %s: %s\n", dsk.data_block_size,
                                dsk.data_offset + moving_blocks[i].old_loc*dsk.data_block_size, dsk.data_device.c_str(),
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
                    data->iov = (struct iovec){ moving_blocks[i].buf, dsk.data_block_size };
                    my_uring_prep_writev(sqe, dsk.data_fd, &data->iov, 1, dsk.data_offset + moving_blocks[i].new_loc*dsk.data_block_size);
                    data->callback = [this, i](ring_data_t *data)
                    {
                        if (data->res != dsk.data_block_size)
                        {
                            fprintf(
                                stderr, "Failed to write %u bytes at %lu to %s: %s\n", dsk.data_block_size,
                                dsk.data_offset + moving_blocks[i].new_loc*dsk.data_block_size, dsk.data_device.c_str(),
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
    moving_blocks = NULL;
    close(dsk.data_fd);
    dsk.data_fd = -1;
    delete ringloop;
    ringloop = NULL;
    return 0;
}

int disk_tool_t::resize_rewrite_journal()
{
    // Simply overwriting on the fly may be impossible because old and new areas may overlap
    // For now, just build new journal data in memory
    new_journal_buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, new_journal_len);
    new_journal_ptr = new_journal_buf;
    new_journal_data = new_journal_ptr + dsk.journal_block_size;
    new_journal_in_pos = 0;
    memset(new_journal_buf, 0, new_journal_len);
    process_journal([this](void *buf)
    {
        return process_journal_block(buf, [this](int num, journal_entry *je)
        {
            if (je->type == JE_START)
            {
                journal_entry *ne = (journal_entry*)(new_journal_ptr + new_journal_in_pos);
                *((journal_entry_start*)ne) = (journal_entry_start){
                    .magic = JOURNAL_MAGIC,
                    .type = JE_START,
                    .size = sizeof(journal_entry_start),
                    .journal_start = dsk.journal_block_size,
                    .version = JOURNAL_VERSION,
                };
                ne->crc32 = je_crc32(ne);
                new_journal_ptr += dsk.journal_block_size;
                new_journal_data = new_journal_ptr+dsk.journal_block_size;
                new_journal_in_pos = 0;
            }
            else
            {
                if (dsk.journal_block_size < new_journal_in_pos+je->size)
                {
                    new_journal_ptr = new_journal_data;
                    if (new_journal_ptr-new_journal_buf >= new_journal_len)
                    {
                        fprintf(stderr, "Error: live entries don't fit to the new journal\n");
                        exit(1);
                    }
                    new_journal_data = new_journal_ptr+dsk.journal_block_size;
                    new_journal_in_pos = 0;
                    if (dsk.journal_block_size < je->size)
                    {
                        fprintf(stderr, "Error: journal entry too large (%u bytes)\n", je->size);
                        exit(1);
                    }
                }
                journal_entry *ne = (journal_entry*)(new_journal_ptr + new_journal_in_pos);
                memcpy(ne, je, je->size);
                ne->crc32_prev = new_crc32_prev;
                if (je->type == JE_BIG_WRITE || je->type == JE_BIG_WRITE_INSTANT)
                {
                    // Change the block reference
                    auto remap_it = data_remap.find(ne->big_write.location / dsk.data_block_size);
                    if (remap_it != data_remap.end())
                    {
                        ne->big_write.location = remap_it->second * dsk.data_block_size;
                    }
                    ne->big_write.location += data_idx_diff * dsk.data_block_size;
                }
                else if (je->type == JE_SMALL_WRITE || je->type == JE_SMALL_WRITE_INSTANT)
                {
                    ne->small_write.data_offset = new_journal_data-new_journal_buf;
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
    return 0;
}

int disk_tool_t::resize_write_new_journal()
{
    new_journal_fd = open(new_journal_device.c_str(), O_DIRECT|O_RDWR);
    if (new_journal_fd < 0)
    {
        fprintf(stderr, "Failed to open new journal device %s: %s\n", new_journal_device.c_str(), strerror(errno));
        return 1;
    }
    lseek64(new_journal_fd, new_journal_offset, 0);
    write_blocking(new_journal_fd, new_journal_buf, new_journal_len);
    fsync(new_journal_fd);
    close(new_journal_fd);
    new_journal_fd = -1;
    free(new_journal_buf);
    new_journal_buf = NULL;
    return 0;
}

int disk_tool_t::resize_rewrite_meta()
{
    new_meta_buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, new_meta_len);
    memset(new_meta_buf, 0, new_meta_len);
    int r = process_meta(
        [this](blockstore_meta_header_v1_t *hdr)
        {
            blockstore_meta_header_v1_t *new_hdr = (blockstore_meta_header_v1_t *)new_meta_buf;
            new_hdr->zero = 0;
            new_hdr->magic = BLOCKSTORE_META_MAGIC_V1;
            new_hdr->version = BLOCKSTORE_META_VERSION_V1;
            new_hdr->meta_block_size = dsk.meta_block_size;
            new_hdr->data_block_size = dsk.data_block_size;
            new_hdr->bitmap_granularity = dsk.bitmap_granularity ? dsk.bitmap_granularity : 4096;
        },
        [this](uint64_t block_num, clean_disk_entry *entry, uint8_t *bitmap)
        {
            auto remap_it = data_remap.find(block_num);
            if (remap_it != data_remap.end())
                block_num = remap_it->second;
            if (block_num < free_first || block_num >= total_blocks-free_last)
            {
                fprintf(stderr, "BUG: remapped block not in range\n");
                exit(1);
            }
            block_num += data_idx_diff;
            clean_disk_entry *new_entry = (clean_disk_entry*)(new_meta_buf + dsk.meta_block_size +
                dsk.meta_block_size*(block_num / new_entries_per_block) +
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
        free(new_meta_buf);
        new_meta_buf = NULL;
        return r;
    }
    return 0;
}

int disk_tool_t::resize_write_new_meta()
{
    new_meta_fd = open(new_meta_device.c_str(), O_DIRECT|O_RDWR);
    if (new_meta_fd < 0)
    {
        fprintf(stderr, "Failed to open new metadata device %s: %s\n", new_meta_device.c_str(), strerror(errno));
        return 1;
    }
    lseek64(new_meta_fd, new_meta_offset, 0);
    write_blocking(new_meta_fd, new_meta_buf, new_meta_len);
    fsync(new_meta_fd);
    close(new_meta_fd);
    new_meta_fd = -1;
    free(new_meta_buf);
    new_meta_buf = NULL;
    return 0;
}
