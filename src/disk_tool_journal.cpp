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
    first_block = true;
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
                printf("%s{\"offset\":\"0x%lx\"", first_block ? "" : ",\n", journal_pos);
                first_block = false;
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
                first_entry = true;
                process_journal_block(journal_buf, [this](int num, journal_entry *je) { dump_journal_entry(num, je, json); });
                if (json)
                    printf(first_entry ? "]}" : "\n]}");
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
        first_entry = true;
        process_journal([this](void *data)
        {
            if (json && dump_with_blocks)
                first_entry = true;
            if (!json)
                printf("offset %08lx:\n", journal_pos);
            auto pos = journal_pos;
            int r = process_journal_block(data, [this, pos](int num, journal_entry *je)
            {
                if (json && dump_with_blocks && first_entry)
                    printf("%s{\"offset\":\"0x%lx\",\"entries\":[\n", first_block ? "" : ",\n", pos);
                dump_journal_entry(num, je, json);
                first_block = false;
            });
            if (json && dump_with_blocks && !first_entry)
                printf("\n]}");
            else if (!json && r <= 0)
                printf("end of the journal\n");
            return r;
        });
    }
    if (json)
        printf(first_block ? "]\n" : "\n]\n");
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
    else if (je->start.size != JE_START_V0_SIZE && je->start.version != JOURNAL_VERSION_V1 && je->start.version != JOURNAL_VERSION_V2)
    {
        fprintf(stderr, "offset %08lx: journal superblock contains version %lu, but I only understand 0, 1 and 2\n",
            journal_pos, je->start.size == JE_START_V0_SIZE ? 0 : je->start.version);
        r = 1;
    }
    else
    {
        memcpy(&je_start, je, sizeof(je_start));
        if (je_start.size == JE_START_V0_SIZE)
            je_start.version = 0;
        if (je_start.version < JOURNAL_VERSION_V2)
        {
            je_start.data_csum_type = 0;
            je_start.csum_block_size = 0;
        }
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
            data_crc32 = je_start.csum_block_size ? 0 : crc32c(0, small_write_data, je->small_write.len);
            data_csum_valid = (data_crc32 == je->small_write.crc32_data);
            if (je_start.csum_block_size && je->small_write.len > 0)
            {
                // like in enqueue_write()
                uint32_t start = je->small_write.offset / je_start.csum_block_size;
                uint32_t end = (je->small_write.offset+je->small_write.len-1) / je_start.csum_block_size;
                uint32_t data_csum_size = (end-start+1) * (je_start.data_csum_type & 0xFF);
                if (je->size < sizeof(journal_entry_small_write) + data_csum_size)
                {
                    data_csum_valid = false;
                }
                else
                {
                    uint32_t calc_csum = 0;
                    uint32_t *block_csums = (uint32_t*)((uint8_t*)je + je->size - data_csum_size);
                    if (start == end)
                    {
                        calc_csum = crc32c(0, (uint8_t*)small_write_data, je->small_write.len);
                        data_csum_valid = data_csum_valid && (calc_csum == *block_csums++);
                    }
                    else
                    {
                        // First block
                        calc_csum = crc32c(0, (uint8_t*)small_write_data,
                            je_start.csum_block_size*(start+1)-je->small_write.offset);
                        data_csum_valid = data_csum_valid && (calc_csum == *block_csums++);
                        // Intermediate blocks
                        for (uint32_t i = start+1; i < end; i++)
                        {
                            calc_csum = crc32c(0, (uint8_t*)small_write_data +
                                je_start.csum_block_size*i-je->small_write.offset, je_start.csum_block_size);
                            data_csum_valid = data_csum_valid && (calc_csum == *block_csums++);
                        }
                        // Last block
                        calc_csum = crc32c(
                            0, (uint8_t*)small_write_data + end*je_start.csum_block_size - je->small_write.offset,
                            je->small_write.offset+je->small_write.len - end*je_start.csum_block_size
                        );
                        data_csum_valid = data_csum_valid && (calc_csum == *block_csums++);
                    }
                }
            }
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
        if (!first_entry)
            printf(",\n");
        first_entry = false;
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
            json ? ",\"type\":\"start\",\"start\":\"0x%lx\"" : "je_start start=%08lx",
            je->start.journal_start
        );
        if (je->start.data_csum_type)
        {
            printf(
                json ? ",\"data_csum_type\":\"%s\",\"csum_block_size\":%u" : " data_csum_type=%s csum_block_size=%u",
                csum_type_str(je->start.data_csum_type).c_str(), je->start.csum_block_size
            );
        }
        printf(json ? "}" : "\n");
    }
    else if (je->type == JE_SMALL_WRITE || je->type == JE_SMALL_WRITE_INSTANT)
    {
        auto & sw = je->small_write;
        printf(
            json ? ",\"type\":\"small_write%s\",\"inode\":\"0x%lx\",\"stripe\":\"0x%lx\",\"ver\":\"%lu\",\"offset\":%u,\"len\":%u,\"loc\":\"0x%lx\""
                : "je_small_write%s oid=%lx:%lx ver=%lu offset=%u len=%u loc=%08lx",
            je->type == JE_SMALL_WRITE_INSTANT ? "_instant" : "",
            sw.oid.inode, sw.oid.stripe, sw.version, sw.offset, sw.len, sw.data_offset
        );
        if (journal_calc_data_pos != sw.data_offset)
        {
            printf(json ? ",\"bad_loc\":true,\"calc_loc\":\"0x%lx\""
                : " (mismatched, calculated = %lu)", journal_pos);
        }
        uint32_t data_csum_size = (!je_start.csum_block_size
            ? 0
            : ((sw.offset + sw.len - 1)/je_start.csum_block_size - sw.offset/je_start.csum_block_size + 1)
                *(je_start.data_csum_type & 0xFF));
        if (je->size > sizeof(journal_entry_small_write) + data_csum_size)
        {
            printf(json ? ",\"bitmap\":\"" : " (bitmap: ");
            for (int i = sizeof(journal_entry_small_write); i < je->size - data_csum_size; i++)
            {
                printf("%02x", ((uint8_t*)je)[i]);
            }
            printf(json ? "\"" : ")");
        }
        if (dump_with_data)
        {
            printf(json ? ",\"data\":\"" : " (data: ");
            for (int i = 0; i < sw.len; i++)
            {
                printf("%02x", ((uint8_t*)small_write_data)[i]);
            }
            printf(json ? "\"" : ")");
        }
        if (data_csum_size > 0 && je->size >= sizeof(journal_entry_small_write) + data_csum_size)
        {
            printf(json ? ",\"block_csums\":\"" : " block_csums=");
            uint8_t *block_csums = (uint8_t*)je + je->size - data_csum_size;
            for (int i = 0; i < data_csum_size; i++)
                printf("%02x", block_csums[i]);
            printf(json ? "\"" : "");
        }
        else
        {
            printf(json ? ",\"data_crc32\":\"%08x\"" : " data_crc32=%08x", sw.crc32_data);
        }
        printf(
            json ? ",\"data_valid\":%s}" : "%s\n",
            (data_csum_valid
                ? (json ? "true" : " (valid)")
                : (json ? "false" : " (invalid)"))
        );
    }
    else if (je->type == JE_BIG_WRITE || je->type == JE_BIG_WRITE_INSTANT)
    {
        auto & bw = je->big_write;
        printf(
            json ? ",\"type\":\"big_write%s\",\"inode\":\"0x%lx\",\"stripe\":\"0x%lx\",\"ver\":\"%lu\",\"offset\":%u,\"len\":%u,\"loc\":\"0x%lx\""
                : "je_big_write%s oid=%lx:%lx ver=%lu offset=%u len=%u loc=%08lx",
            je->type == JE_BIG_WRITE_INSTANT ? "_instant" : "",
            bw.oid.inode, bw.oid.stripe, bw.version, bw.offset, bw.len, bw.location
        );
        uint32_t data_csum_size = (!je_start.csum_block_size
            ? 0
            : ((bw.offset + bw.len - 1)/je_start.csum_block_size - bw.offset/je_start.csum_block_size + 1)
                *(je_start.data_csum_type & 0xFF));
        if (data_csum_size > 0 && je->size >= sizeof(journal_entry_big_write) + data_csum_size)
        {
            printf(json ? ",\"block_csums\":\"" : " block_csums=");
            uint8_t *block_csums = (uint8_t*)je + je->size - data_csum_size;
            for (int i = 0; i < data_csum_size; i++)
                printf("%02x", block_csums[i]);
            printf(json ? "\"" : "");
        }
        if (bw.size > sizeof(journal_entry_big_write) + data_csum_size)
        {
            printf(json ? ",\"bitmap\":\"" : " (bitmap: ");
            for (int i = sizeof(journal_entry_big_write); i < bw.size - data_csum_size; i++)
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
    // Write start entry into the first block
    *((journal_entry_start*)new_journal_buf) = (journal_entry_start){
        .magic = JOURNAL_MAGIC,
        .type = JE_START,
        .size = sizeof(journal_entry_start),
        .journal_start = dsk.journal_block_size,
        .version = JOURNAL_VERSION_V2,
        .data_csum_type = dsk.data_csum_type,
        .csum_block_size = dsk.csum_block_size,
    };
    ((journal_entry*)new_journal_buf)->crc32 = je_crc32((journal_entry*)new_journal_buf);
    new_journal_ptr += dsk.journal_block_size;
    new_journal_data = new_journal_ptr+dsk.journal_block_size;
    new_journal_in_pos = 0;
    for (const auto & rec: entries.array_items())
    {
        auto t_it = type_by_name.find(rec["type"].string_value());
        if (t_it == type_by_name.end())
        {
            fprintf(stderr, "Unknown journal entry type \"%s\", skipping\n", rec["type"].string_value().c_str());
            continue;
        }
        uint16_t type = t_it->second;
        if (type == JE_START)
            continue;
        uint32_t entry_size = (type == JE_START
            ? sizeof(journal_entry_start)
            : (type == JE_SMALL_WRITE || type == JE_SMALL_WRITE_INSTANT
                ? sizeof(journal_entry_small_write) + dsk.clean_entry_bitmap_size +
                    (dsk.data_csum_type ? rec["len"].uint64_value()/dsk.csum_block_size*(dsk.data_csum_type & 0xFF) : 0)
                : (type == JE_BIG_WRITE || type == JE_BIG_WRITE_INSTANT
                    ? sizeof(journal_entry_big_write) + dsk.clean_entry_bitmap_size +
                        (dsk.data_csum_type ? rec["len"].uint64_value()/dsk.csum_block_size*(dsk.data_csum_type & 0xFF) : 0)
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
        if (type == JE_SMALL_WRITE || type == JE_SMALL_WRITE_INSTANT)
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
                    .inode = sscanf_json(NULL, rec["inode"]),
                    .stripe = sscanf_json(NULL, rec["stripe"]),
                },
                .version = rec["ver"].uint64_value(),
                .offset = (uint32_t)rec["offset"].uint64_value(),
                .len = (uint32_t)rec["len"].uint64_value(),
                .data_offset = (uint64_t)(new_journal_data-new_journal_buf),
                .crc32_data = !dsk.data_csum_type ? 0 : (uint32_t)sscanf_json("%x", rec["data_crc32"]),
            };
            uint32_t data_csum_size = !dsk.data_csum_type ? 0 : ne->small_write.len/dsk.csum_block_size*(dsk.data_csum_type & 0xFF);
            fromhexstr(rec["bitmap"].string_value(), dsk.clean_entry_bitmap_size, ((uint8_t*)ne) + sizeof(journal_entry_small_write) + data_csum_size);
            fromhexstr(rec["data"].string_value(), ne->small_write.len, new_journal_data);
            if (dsk.data_csum_type)
                fromhexstr(rec["block_csums"].string_value(), data_csum_size, ((uint8_t*)ne) + sizeof(journal_entry_small_write));
            if (rec["data"].is_string())
            {
                if (!dsk.data_csum_type)
                    ne->small_write.crc32_data = crc32c(0, new_journal_data, ne->small_write.len);
                else if (dsk.data_csum_type == BLOCKSTORE_CSUM_CRC32C)
                {
                    uint32_t *block_csums = (uint32_t*)(((uint8_t*)ne) + sizeof(journal_entry_small_write));
                    for (uint32_t i = 0; i < ne->small_write.len; i += dsk.csum_block_size, block_csums++)
                        *block_csums = crc32c(0, new_journal_data+i, dsk.csum_block_size);
                }
            }
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
                    .inode = sscanf_json(NULL, rec["inode"]),
                    .stripe = sscanf_json(NULL, rec["stripe"]),
                },
                .version = rec["ver"].uint64_value(),
                .offset = (uint32_t)rec["offset"].uint64_value(),
                .len = (uint32_t)rec["len"].uint64_value(),
                .location = sscanf_json(NULL, rec["loc"]),
            };
            uint32_t data_csum_size = !dsk.data_csum_type ? 0 : ne->big_write.len/dsk.csum_block_size*(dsk.data_csum_type & 0xFF);
            fromhexstr(rec["bitmap"].string_value(), dsk.clean_entry_bitmap_size, ((uint8_t*)ne) + sizeof(journal_entry_big_write) + data_csum_size);
            if (dsk.data_csum_type)
                fromhexstr(rec["block_csums"].string_value(), data_csum_size, ((uint8_t*)ne) + sizeof(journal_entry_big_write));
        }
        else if (type == JE_STABLE || type == JE_ROLLBACK || type == JE_DELETE)
        {
            *((journal_entry_del*)ne) = (journal_entry_del){
                .magic = JOURNAL_MAGIC,
                .type = type,
                .size = entry_size,
                .crc32_prev = new_crc32_prev,
                .oid = {
                    .inode = sscanf_json(NULL, rec["inode"]),
                    .stripe = sscanf_json(NULL, rec["stripe"]),
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
