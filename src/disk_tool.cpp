// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#define _LARGEFILE64_SOURCE
#include <sys/types.h>
#include <dirent.h>
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
#include "str_util.h"
#include "crc32c.h"
#include "rw_blocking.h"

#define DM_ST_EMPTY 0
#define DM_ST_TO_READ 1
#define DM_ST_READING 2
#define DM_ST_TO_WRITE 3
#define DM_ST_WRITING 4

// vITADisk
#define VITASTOR_DISK_MAGIC 0x6b73694441544976
#define VITASTOR_DISK_MAX_SB_SIZE 128*1024

struct __attribute__((__packed__)) vitastor_disk_superblock_t
{
    uint64_t magic;
    uint32_t crc32c;
    uint32_t size;
    uint8_t json_data[];
};

struct resizer_data_moving_t
{
    int state = 0;
    void *buf = NULL;
    uint64_t old_loc, new_loc;
};

static const char *help_text =
    "Vitastor disk management tool\n"
    "(c) Vitaliy Filippov, 2022+ (VNPL-1.1)\n"
    "\n"
    "COMMANDS:\n"
    "\n"
    "vitastor-disk resize <ALL_OSD_PARAMETERS> <NEW_LAYOUT> [--iodepth 32]\n"
    "  Resize data area and/or rewrite/move journal and metadata\n"
    "  ALL_OSD_PARAMETERS must include all (at least all disk-related)\n"
    "  parameters from OSD command line (i.e. from systemd unit).\n"
    "  NEW_LAYOUT may include new disk layout parameters:\n"
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
    "vitastor-disk start|stop|restart|enable|disable [--now] <device> [device2 device3 ...]\n"
    "  Manipulate Vitastor OSDs using systemd by their device paths.\n"
    "  Commands are passed to systemctl with vitastor-osd@<num> units as arguments.\n"
    "  When --now is added to enable/disable, OSDs are also immediately started/stopped.\n"
    "\n"
    "vitastor-disk read-sb <device>\n"
    "  Try to read Vitastor OSD superblock from <device> and print it in JSON format.\n"
    "\n"
    "vitastor-disk write-sb <device>\n"
    "  Read JSON from STDIN and write it into Vitastor OSD superblock on <device>.\n"
    "\n"
    "vitastor-disk udev <device>\n"
    "  Try to read Vitastor OSD superblock from <device> and print variables for udev.\n"
    "\n"
    "vitastor-disk exec-osd <device>\n"
    "  Read Vitastor OSD superblock from <device> and start the OSD with parameters from it.\n"
    "  Intended for use from startup scripts (i.e. from systemd units).\n"
    "\n"
    "vitastor-disk pre-exec <device>\n"
    "  Read Vitastor OSD superblock from <device> and perform pre-start checks for the OSD.\n"
    "  For now, this only checks that device cache is in write-through mode if fsync is disabled.\n"
    "  Intended for use from startup scripts (i.e. from systemd units).\n"
    "\n"
    "vitastor-disk dump-journal [--all] [--json] <journal_file> <journal_block_size> <offset> <size>\n"
    "  Dump journal in human-readable or JSON (if --json is specified) format.\n"
    "  Without --all, only actual part of the journal is dumped.\n"
    "  With --all, the whole journal area is scanned for journal entries,\n"
    "  some of which may be outdated.\n"
    "\n"
    "vitastor-disk dump-meta <meta_file> <meta_block_size> <offset> <size>\n"
    "  Dump metadata in JSON format.\n"
    "\n"
    "vitastor-disk simple-offsets <device>\n"
    "  Calculate offsets for old simple&stupid (no superblock) OSD deployment. Options:\n"
    "  --object_size 128k       Set blockstore block size\n"
    "  --bitmap_granularity 4k  Set bitmap granularity\n"
    "  --journal_size 16M       Set journal size\n"
    "  --device_block_size 4k   Set device block size\n"
    "  --journal_offset 0       Set journal offset\n"
    "  --device_size 0          Set device size\n"
    "  --format text            Result format: json, options, env, or text\n"
    "\n"
    "Use vitastor-disk --help <command> for command details or vitastor-disk --help --all for all details.\n"
;

struct disk_tool_t
{
    /**** Parameters ****/

    std::map<std::string, std::string> options;
    bool all, json, now;
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

    int udev_import(std::string device);
    int read_sb(std::string device);
    int write_sb(std::string device);
    int exec_osd(std::string device);
    int systemd_start_stop_osds(std::vector<std::string> cmd, std::vector<std::string> devices);
    int pre_exec_osd(std::string device);

    json11::Json read_osd_superblock(std::string device);
    uint32_t write_osd_superblock(std::string device, json11::Json params);
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
            cmd.insert(cmd.begin(), (char*)"help");
        }
        else if (!strcmp(argv[i], "--now"))
        {
            self.now = true;
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
    if (!cmd.size())
    {
        cmd.push_back((char*)"help");
    }
    if (!strcmp(cmd[0], "dump-journal"))
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
    else if (!strcmp(cmd[0], "dump-meta"))
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
    else if (!strcmp(cmd[0], "resize"))
    {
        return self.resize_data();
    }
    else if (!strcmp(cmd[0], "simple-offsets"))
    {
        // Calculate offsets for simple & stupid OSD deployment without superblock
        if (cmd.size() > 1)
        {
            self.options["device"] = cmd[1];
        }
        disk_tool_simple_offsets(self.options, self.json);
        return 0;
    }
    else if (!strcmp(cmd[0], "udev"))
    {
        if (cmd.size() != 2)
        {
            fprintf(stderr, "Exactly 1 device path argument is required\n");
            return 1;
        }
        return self.udev_import(cmd[1]);
    }
    else if (!strcmp(cmd[0], "read-sb"))
    {
        if (cmd.size() != 2)
        {
            fprintf(stderr, "Exactly 1 device path argument is required\n");
            return 1;
        }
        return self.read_sb(cmd[1]);
    }
    else if (!strcmp(cmd[0], "write-sb"))
    {
        if (cmd.size() != 2)
        {
            fprintf(stderr, "Exactly 1 device path argument is required\n");
            return 1;
        }
        return self.write_sb(cmd[1]);
    }
    else if (!strcmp(cmd[0], "start") || !strcmp(cmd[0], "stop") ||
        !strcmp(cmd[0], "restart") || !strcmp(cmd[0], "enable") || !strcmp(cmd[0], "disable"))
    {
        std::vector<std::string> systemd_cmd;
        systemd_cmd.push_back(cmd[0]);
        if (self.now && (!strcmp(cmd[0], "enable") || !strcmp(cmd[0], "disable")))
        {
            systemd_cmd.push_back("--now");
        }
        return self.systemd_start_stop_osds(systemd_cmd, std::vector<std::string>(cmd.begin()+1, cmd.end()));
    }
    else if (!strcmp(cmd[0], "exec-osd"))
    {
        if (cmd.size() != 2)
        {
            fprintf(stderr, "Exactly 1 device path argument is required\n");
            return 1;
        }
        return self.exec_osd(cmd[1]);
    }
    else
    {
        print_help(help_text, "vitastor-disk", cmd.size() > 1 ? cmd[1] : "", self.all);
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

static std::string udev_escape(std::string str)
{
    std::string r;
    int p = str.find_first_of("\"\' \t\r\n"), prev = 0;
    if (p == std::string::npos)
    {
        return str;
    }
    while (p != std::string::npos)
    {
        r += str.substr(prev, p-prev);
        r += "\\";
        prev = p;
        p = str.find_first_of("\"\' \t\r\n", p+1);
    }
    r += str.substr(prev);
    return r;
}

static std::string realpath_str(std::string path)
{
    char *p = realpath((char*)path.c_str(), NULL);
    if (!p)
    {
        fprintf(stderr, "Failed to resolve %s: %s\n", path.c_str(), strerror(errno));
        return path;
    }
    std::string rp(p);
    free(p);
    return rp;
}

int disk_tool_t::udev_import(std::string device)
{
    json11::Json sb = read_osd_superblock(device);
    if (sb.is_null())
    {
        return 1;
    }
    uint64_t osd_num = sb["params"]["osd_num"].uint64_value();
    // Print variables for udev
    printf("VITASTOR_OSD_NUM=%lu\n", osd_num);
    printf("VITASTOR_ALIAS=osd%lu%s\n", osd_num, sb["device_type"].string_value().c_str());
    printf("VITASTOR_DATA_DEVICE=%s\n", udev_escape(sb["params"]["data_device"].string_value()).c_str());
    if (sb["real_meta_device"].string_value() != "" && sb["real_meta_device"] != sb["real_data_device"])
        printf("VITASTOR_META_DEVICE=%s\n", udev_escape(sb["params"]["meta_device"].string_value()).c_str());
    if (sb["real_journal_device"].string_value() != "" && sb["real_journal_device"] != sb["real_meta_device"])
        printf("VITASTOR_JOURNAL_DEVICE=%s\n", udev_escape(sb["params"]["journal_device"].string_value()).c_str());
    return 0;
}

int disk_tool_t::read_sb(std::string device)
{
    json11::Json sb = read_osd_superblock(device);
    if (sb.is_null())
    {
        return 1;
    }
    printf("%s\n", sb["params"].dump().c_str());
    return 0;
}

int disk_tool_t::write_sb(std::string device)
{
    std::string input;
    int r;
    char buf[4096];
    while (1)
    {
        r = read(0, buf, sizeof(buf));
        if (r <= 0 && errno != EAGAIN)
            break;
        input += std::string(buf, r);
    }
    std::string json_err;
    json11::Json params = json11::Json::parse(input, json_err);
    if (json_err != "" || !params["osd_num"].uint64_value() || params["data_device"].string_value() == "")
    {
        fprintf(stderr, "Invalid JSON input\n");
        return 1;
    }
    return !write_osd_superblock(device, params);
}

uint32_t disk_tool_t::write_osd_superblock(std::string device, json11::Json params)
{
    std::string json_data = params.dump();
    uint32_t sb_size = sizeof(vitastor_disk_superblock_t)+json_data.size();
    if (sb_size > VITASTOR_DISK_MAX_SB_SIZE)
    {
        fprintf(stderr, "JSON data for superblock is too large\n");
        return 0;
    }
    uint64_t buf_len = ((sb_size+4095)/4096) * 4096;
    uint8_t *buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, buf_len);
    memset(buf, 0, buf_len);
    vitastor_disk_superblock_t *sb = (vitastor_disk_superblock_t*)buf;
    sb->magic = VITASTOR_DISK_MAGIC;
    sb->size = sb_size;
    memcpy(sb->json_data, json_data.c_str(), json_data.size());
    sb->crc32c = crc32c(0, &sb->size, sb->size - ((uint8_t*)&sb->size - buf));
    int fd = open(device.c_str(), O_DIRECT|O_RDWR);
    if (fd < 0)
    {
        fprintf(stderr, "Failed to open device %s: %s\n", device.c_str(), strerror(errno));
        free(buf);
        return 0;
    }
    int r = write_blocking(fd, buf, buf_len);
    if (r < 0)
    {
        fprintf(stderr, "Failed to write to %s: %s\n", device.c_str(), strerror(errno));
        close(fd);
        free(buf);
        return 0;
    }
    close(fd);
    free(buf);
    return sb_size;
}

json11::Json disk_tool_t::read_osd_superblock(std::string device)
{
    vitastor_disk_superblock_t *sb = NULL;
    uint8_t *buf = NULL;
    json11::Json osd_params;
    std::string json_err;
    std::string real_device, device_type, real_data, real_meta, real_journal;
    int r, fd = open(device.c_str(), O_DIRECT|O_RDWR);
    if (fd < 0)
    {
        fprintf(stderr, "Failed to open device %s: %s\n", device.c_str(), strerror(errno));
        return osd_params;
    }
    buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, 4096);
    r = read_blocking(fd, buf, 4096);
    if (r != 4096)
    {
        fprintf(stderr, "Failed to read OSD superblock from %s: %s\n", device.c_str(), strerror(errno));
        goto ex;
    }
    sb = (vitastor_disk_superblock_t*)buf;
    if (sb->magic != VITASTOR_DISK_MAGIC)
    {
        fprintf(stderr, "Invalid OSD superblock on %s: magic number mismatch\n", device.c_str());
        goto ex;
    }
    if (sb->size > VITASTOR_DISK_MAX_SB_SIZE ||
        // +2 is minimal json: {}
        sb->size < sizeof(vitastor_disk_superblock_t)+2)
    {
        fprintf(stderr, "Invalid OSD superblock on %s: invalid size\n", device.c_str());
        goto ex;
    }
    if (sb->size > 4096)
    {
        uint64_t sb_size = ((sb->size+4095)/4096)*4096;
        free(buf);
        buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, sb_size);
        lseek64(fd, 0, 0);
        r = read_blocking(fd, buf, sb_size);
        if (r != sb_size)
        {
            fprintf(stderr, "Failed to read OSD superblock from %s: %s\n", device.c_str(), strerror(errno));
            goto ex;
        }
        sb = (vitastor_disk_superblock_t*)buf;
    }
    if (sb->crc32c != crc32c(0, &sb->size, sb->size - ((uint8_t*)&sb->size - buf)))
    {
        fprintf(stderr, "Invalid OSD superblock on %s: crc32 mismatch\n", device.c_str());
        goto ex;
    }
    osd_params = json11::Json::parse(std::string((char*)sb->json_data, sb->size - sizeof(vitastor_disk_superblock_t)), json_err);
    if (json_err != "")
    {
        fprintf(stderr, "Invalid OSD superblock on %s: invalid JSON\n", device.c_str());
        goto ex;
    }
    // Validate superblock
    if (!osd_params["osd_num"].uint64_value())
    {
        fprintf(stderr, "OSD superblock on %s lacks osd_num\n", device.c_str());
        osd_params = json11::Json::object{};
        goto ex;
    }
    if (osd_params["data_device"].string_value() == "")
    {
        fprintf(stderr, "OSD superblock on %s lacks data_device\n", device.c_str());
        osd_params = json11::Json::object{};
        goto ex;
    }
    real_device = realpath_str(device);
    real_data = realpath_str(osd_params["data_device"].string_value());
    real_meta = osd_params["meta_device"] != "" && osd_params["meta_device"] != osd_params["data_device"]
        ? realpath_str(osd_params["meta_device"].string_value()) : "";
    real_journal = osd_params["journal_device"] != "" && osd_params["journal_device"] != osd_params["meta_device"]
        ? realpath_str(osd_params["journal_device"].string_value()) : "";
    if (real_journal == real_meta)
    {
        real_journal = "";
    }
    if (real_meta == real_data)
    {
        real_meta = "";
    }
    if (real_device == real_data)
    {
        device_type = "data";
    }
    else if (real_device == real_meta)
    {
        device_type = "meta";
    }
    else if (real_device == real_journal)
    {
        device_type = "journal";
    }
    else
    {
        fprintf(stderr, "Invalid OSD superblock on %s: does not refer to the device itself\n", device.c_str());
        osd_params = json11::Json::object{};
        goto ex;
    }
    osd_params = json11::Json::object{
        { "params", osd_params },
        { "device_type", device_type },
        { "real_data_device", real_data },
        { "real_meta_device", real_meta },
        { "real_journal_device", real_journal },
    };
ex:
    free(buf);
    close(fd);
    return osd_params;
}

int disk_tool_t::systemd_start_stop_osds(std::vector<std::string> cmd, std::vector<std::string> devices)
{
    if (!devices.size())
    {
        fprintf(stderr, "Device path is missing\n");
        return 1;
    }
    std::vector<std::string> svcs;
    for (auto & device: devices)
    {
        json11::Json sb = read_osd_superblock(device);
        if (!sb.is_null())
        {
            svcs.push_back("vitastor-osd@"+sb["params"]["osd_num"].as_string());
        }
    }
    if (!svcs.size())
    {
        return 1;
    }
    std::vector<char*> argv;
    argv.push_back((char*)"systemctl");
    for (auto & s: cmd)
    {
        argv.push_back((char*)s.c_str());
    }
    for (auto & s: svcs)
    {
        argv.push_back((char*)s.c_str());
    }
    argv.push_back(NULL);
    execvpe("systemctl", argv.data(), environ);
    return 0;
}

int disk_tool_t::exec_osd(std::string device)
{
    json11::Json sb = read_osd_superblock(device);
    if (sb.is_null())
    {
        return 1;
    }
    std::string osd_binary = "vitastor-osd";
    if (options["osd-binary"] != "")
    {
        osd_binary = options["osd-binary"];
    }
    std::vector<std::string> argstr;
    argstr.push_back(osd_binary.c_str());
    for (auto & kv: sb["params"].object_items())
    {
        argstr.push_back("--"+kv.first);
        argstr.push_back(kv.second.is_string() ? kv.second.string_value() : kv.second.dump());
    }
    char *argv[argstr.size()+1];
    for (int i = 0; i < argstr.size(); i++)
    {
        argv[i] = (char*)argstr[i].c_str();
    }
    argv[argstr.size()] = NULL;
    execvpe(osd_binary.c_str(), argv, environ);
    return 0;
}

static std::string read_file(std::string file)
{
    char buf[64];
    int fd = open(file.c_str(), O_RDONLY), r;
    if (fd < 0 || (r = read(fd, buf, sizeof(buf))) < 0)
    {
        if (fd >= 0)
            close(fd);
        fprintf(stderr, "Can't read %s: %s\n", file.c_str(), strerror(errno));
        return "";
    }
    close(fd);
    return std::string(buf, r);
}

static int check_queue_cache(std::string dev, std::string parent_dev)
{
    auto r = read_file("/sys/block/"+dev+"/queue/write_cache");
    if (r == "")
        r = read_file("/sys/block/"+parent_dev+"/queue/write_cache");
    if (r == "")
        return 1;
    return r == "write through" ? 0 : -1;
}

// returns 1 = warning, -1 = error, 0 = success
static int disable_cache(std::string dev)
{
    if (dev.substr(0, 5) != "/dev/")
    {
        fprintf(stderr, "%s is outside /dev/\n", dev.c_str());
        return 1;
    }
    dev = dev.substr(5);
    int i = dev.size();
    while (i > 0 && isdigit(dev[i-1]))
        i--;
    // Check if it's a partition
    if (i >= 2 && dev[i-1] == 'p' && isdigit(dev[i-2]))
        i--;
    auto parent_dev = dev.substr(0, i);
    auto scsi_disk = "/sys/block/"+parent_dev+"/device/scsi_disk";
    DIR *dir = opendir(scsi_disk.c_str());
    if (!dir)
    {
        if (errno == ENOENT)
        {
            // Not a SCSI/SATA device, just check /sys/block/.../queue/write_cache
            return check_queue_cache(dev, parent_dev);
        }
        else
        {
            fprintf(stderr, "Can't read directory %s: %s\n", scsi_disk.c_str(), strerror(errno));
            return 1;
        }
    }
    else
    {
        dirent *de = readdir(dir);
        while (de && de->d_name[0] == '.' && (de->d_name[1] == 0 || de->d_name[1] == '.' && de->d_name[2] == 0))
            de = readdir(dir);
        if (!de)
        {
            // Not a SCSI/SATA device, just check /sys/block/.../queue/write_cache
            closedir(dir);
            return check_queue_cache(dev, parent_dev);
        }
        scsi_disk += "/";
        scsi_disk += de->d_name;
        if (readdir(dir) != NULL)
        {
            // Error, multiple scsi_disk/* entries
            closedir(dir);
            fprintf(stderr, "Multiple entries in %s found\n", scsi_disk.c_str());
            return 1;
        }
        closedir(dir);
        // Check cache_type
        scsi_disk += "/cache_type";
        std::string cache_type = read_file(scsi_disk);
        if (cache_type == "")
            return 1;
        if (cache_type == "write back")
        {
            int fd = open(scsi_disk.c_str(), O_WRONLY);
            if (fd < 0 || write_blocking(fd, (void*)"write through", strlen("write through")) != strlen("write through"))
            {
                if (fd >= 0)
                    close(fd);
                fprintf(stderr, "Can't write to %s: %s\n", scsi_disk.c_str(), strerror(errno));
                return -1;
            }
            close(fd);
        }
    }
    return 0;
}

static int check_disabled_cache(std::string dev)
{
    int r = disable_cache(dev);
    if (r == 1)
    {
        fprintf(
            stderr, "Warning: fsync is disabled for %s, but cache status check failed."
            " Ensure that cache is in write-through mode yourself or you may lose data.\n", dev.c_str()
        );
    }
    else if (r == -1)
    {
        fprintf(
            stderr, "Error: fsync is disabled for %s, but its cache is in write-back mode"
            " and we failed to make it write-through. Data loss is presumably possible."
            " Either switch the cache to write-through mode yourself or disable the check"
            " using skip_cache_check=1 in the superblock.\n", dev.c_str()
        );
        return 1;
    }
    return 0;
}

static bool json_is_true(const json11::Json & val)
{
    if (val.is_string())
        return val == "true" || val == "yes" || val == "1";
    return val.bool_value();
}

int disk_tool_t::pre_exec_osd(std::string device)
{
    json11::Json sb = read_osd_superblock(device);
    if (sb.is_null())
    {
        return 1;
    }
    if (!sb["params"]["skip_cache_check"].uint64_value())
    {
        if (json_is_true(sb["params"]["disable_data_fsync"]) &&
            check_disabled_cache(sb["real_data_device"].string_value()) != 0)
        {
            return 1;
        }
        if (json_is_true(sb["params"]["disable_meta_fsync"]) &&
            sb["real_meta_device"].string_value() != "" && sb["real_meta_device"] != sb["real_data_device"] &&
            check_disabled_cache(sb["real_meta_device"].string_value()) != 0)
        {
            return 1;
        }
        if (json_is_true(sb["params"]["disable_journal_fsync"]) &&
            sb["real_journal_device"].string_value() != "" && sb["real_journal_device"] != sb["real_meta_device"] &&
            check_disabled_cache(sb["real_journal_device"].string_value()) != 0)
        {
            return 1;
        }
    }
    return 0;
}
