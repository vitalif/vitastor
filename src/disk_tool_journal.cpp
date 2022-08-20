// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "disk_tool.h"

int disk_tool_t::dump_journal()
{
    dump_with_blocks = options["format"] == "blocks";
    dump_with_data = options["format"] == "data" || options["format"] == "blocks,data";
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
                    if (dump_with_blocks)
                        printf("%s{\"offset\":\"0x%lx\",\"entries\":[\n", first ? "" : ",\n", pos);
                    first = false;
                }
                dump_journal_entry(num, je, json);
            });
            if (json)
            {
                if (dump_with_blocks && !first2)
                    printf("\n]}");
            }
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
        if (je->small_write.size > sizeof(journal_entry_small_write))
        {
            printf(json ? ",\"bitmap\":\"" : " (bitmap: ");
            for (int i = sizeof(journal_entry_small_write); i < je->small_write.size; i++)
            {
                printf("%02x", ((uint8_t*)je)[i]);
            }
            printf(json ? "\"" : ")");
        }
        if (dump_with_data)
        {
            printf(json ? ",\"data\":\"" : " (data: ");
            for (int i = 0; i < je->small_write.len; i++)
            {
                printf("%02x", ((uint8_t*)small_write_data)[i]);
            }
            printf(json ? "\"" : ")");
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
            json ? ",\"type\":\"big_write%s\",\"inode\":\"0x%lx\",\"stripe\":\"0x%lx\",\"ver\":\"%lu\",\"loc\":\"0x%lx\""
                : "je_big_write%s oid=%lx:%lx ver=%lu loc=%08lx",
            je->type == JE_BIG_WRITE_INSTANT ? "_instant" : "",
            je->big_write.oid.inode, je->big_write.oid.stripe, je->big_write.version, je->big_write.location
        );
        if (je->big_write.size > sizeof(journal_entry_big_write))
        {
            printf(json ? ",\"bitmap\":\"" : " (bitmap: ");
            for (int i = sizeof(journal_entry_big_write); i < je->small_write.size; i++)
            {
                printf("%02x", ((uint8_t*)je)[i]);
            }
            printf(json ? "\"" : ")");
        }
        printf(json ? "}" : "\n");
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

static uint64_t sscanf_num(const char *fmt, const std::string & str)
{
    uint64_t value = 0;
    sscanf(str.c_str(), fmt, &value);
    return value;
}

static int fromhex(char c)
{
    if (c >= '0' && c <= '9')
        return (c-'0');
    else if (c >= 'a' && c <= 'f')
        return (c-'a'+10);
    else if (c >= 'A' && c <= 'F')
        return (c-'A'+10);
    return -1;
}

static void fromhexstr(const std::string & from, int bytes, uint8_t *to)
{
    for (int i = 0; i < from.size() && i < bytes; i++)
    {
        int x = fromhex(from[2*i]), y = fromhex(from[2*i+1]);
        if (x < 0 || y < 0)
            break;
        to[i] = x*16 + y;
    }
}

int disk_tool_t::write_json_journal(json11::Json entries)
{
    new_journal_buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, new_journal_len);
    new_journal_ptr = new_journal_buf;
    new_journal_data = new_journal_ptr + dsk.journal_block_size;
    new_journal_in_pos = 0;
    memset(new_journal_buf, 0, new_journal_len);
    std::map<std::string,uint16_t> type_by_name = {
        { "start", JE_START },
        { "small_write", JE_SMALL_WRITE },
        { "small_write_instant", JE_SMALL_WRITE_INSTANT },
        { "big_write", JE_BIG_WRITE },
        { "big_write_instant", JE_BIG_WRITE_INSTANT },
        { "stable", JE_STABLE },
        { "delete", JE_DELETE },
        { "rollback", JE_ROLLBACK },
    };
    for (const auto & rec: entries.array_items())
    {
        auto t_it = type_by_name.find(rec["type"].string_value());
        if (t_it == type_by_name.end())
        {
            fprintf(stderr, "Unknown journal entry type \"%s\", skipping\n", rec["type"].string_value().c_str());
            continue;
        }
        uint16_t type = t_it->second;
        uint32_t entry_size = (type == JE_START
            ? sizeof(journal_entry_start)
            : (type == JE_SMALL_WRITE || type == JE_SMALL_WRITE_INSTANT
                ? sizeof(journal_entry_small_write) + dsk.clean_entry_bitmap_size
                : (type == JE_BIG_WRITE || type == JE_BIG_WRITE_INSTANT
                    ? sizeof(journal_entry_big_write) + dsk.clean_entry_bitmap_size
                    : sizeof(journal_entry_del))));
        if (dsk.journal_block_size < new_journal_in_pos + entry_size)
        {
            new_journal_ptr = new_journal_data;
            if (new_journal_ptr-new_journal_buf >= new_journal_len)
            {
                fprintf(stderr, "Error: entries don't fit to the new journal\n");
                free(new_journal_buf);
                return 1;
            }
            new_journal_data = new_journal_ptr+dsk.journal_block_size;
            new_journal_in_pos = 0;
            if (dsk.journal_block_size < entry_size)
            {
                fprintf(stderr, "Error: journal entry too large (%u bytes)\n", entry_size);
                free(new_journal_buf);
                return 1;
            }
        }
        journal_entry *ne = (journal_entry*)(new_journal_ptr + new_journal_in_pos);
        if (type == JE_START)
        {
            *((journal_entry_start*)ne) = (journal_entry_start){
                .magic = JOURNAL_MAGIC,
                .type = type,
                .size = entry_size,
                .journal_start = dsk.journal_block_size,
                .version = JOURNAL_VERSION,
            };
            new_journal_ptr += dsk.journal_block_size;
            new_journal_data = new_journal_ptr+dsk.journal_block_size;
            new_journal_in_pos = 0;
        }
        else if (type == JE_SMALL_WRITE || type == JE_SMALL_WRITE_INSTANT)
        {
            if (new_journal_data - new_journal_buf + ne->small_write.len > new_journal_len)
            {
                fprintf(stderr, "Error: entries don't fit to the new journal\n");
                free(new_journal_buf);
                return 1;
            }
            *((journal_entry_small_write*)ne) = (journal_entry_small_write){
                .magic = JOURNAL_MAGIC,
                .type = type,
                .size = entry_size,
                .crc32_prev = new_crc32_prev,
                .oid = {
                    .inode = sscanf_num("0x%lx", rec["inode"].string_value()),
                    .stripe = sscanf_num("0x%lx", rec["stripe"].string_value()),
                },
                .version = rec["ver"].uint64_value(),
                .offset = (uint32_t)rec["offset"].uint64_value(),
                .len = (uint32_t)rec["len"].uint64_value(),
                .data_offset = (uint64_t)(new_journal_data-new_journal_buf),
                .crc32_data = (uint32_t)sscanf_num("%x", rec["data_crc32"].string_value()),
            };
            fromhexstr(rec["bitmap"].string_value(), dsk.clean_entry_bitmap_size, ((uint8_t*)ne) + sizeof(journal_entry_small_write));
            fromhexstr(rec["data"].string_value(), ne->small_write.len, new_journal_data);
            if (rec["data"].is_string())
                ne->small_write.crc32_data = crc32c(0, new_journal_data, ne->small_write.len);
            new_journal_data += ne->small_write.len;
        }
        else if (type == JE_BIG_WRITE || type == JE_BIG_WRITE_INSTANT)
        {
            *((journal_entry_big_write*)ne) = (journal_entry_big_write){
                .magic = JOURNAL_MAGIC,
                .type = type,
                .size = entry_size,
                .crc32_prev = new_crc32_prev,
                .oid = {
                    .inode = sscanf_num("0x%lx", rec["inode"].string_value()),
                    .stripe = sscanf_num("0x%lx", rec["stripe"].string_value()),
                },
                .version = rec["ver"].uint64_value(),
                .location = sscanf_num("0x%lx", rec["loc"].string_value()),
            };
            fromhexstr(rec["bitmap"].string_value(), dsk.clean_entry_bitmap_size, ((uint8_t*)ne) + sizeof(journal_entry_big_write));
        }
        else if (type == JE_STABLE || type == JE_ROLLBACK || type == JE_DELETE)
        {
            *((journal_entry_del*)ne) = (journal_entry_del){
                .magic = JOURNAL_MAGIC,
                .type = type,
                .size = entry_size,
                .crc32_prev = new_crc32_prev,
                .oid = {
                    .inode = sscanf_num("0x%lx", rec["inode"].string_value()),
                    .stripe = sscanf_num("0x%lx", rec["stripe"].string_value()),
                },
                .version = rec["ver"].uint64_value(),
            };
        }
        ne->crc32 = je_crc32(ne);
        new_crc32_prev = ne->crc32;
        new_journal_in_pos += ne->size;
    }
    int r = resize_write_new_journal();
    free(new_journal_buf);
    return r;
}
