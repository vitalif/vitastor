// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "disk_tool.h"
#include "rw_blocking.h"
#include "osd_id.h"
#include "json_util.h"
#include "malloc_or_die.h"

#define FREE_SPACE_BIT 0x8000

int disk_tool_t::process_meta(std::function<void(blockstore_meta_header_v3_t *)> hdr_fn,
    std::function<void(blockstore_heap_t *heap, heap_object_t *obj, uint32_t meta_block_num)> obj_fn,
    std::function<void(uint64_t block_num, clean_disk_entry *entry_v1, uint8_t *bitmap)> record_fn,
    bool with_data, bool do_open)
{
    int r = 0;
    if (dsk.meta_block_size % DIRECT_IO_ALIGNMENT)
    {
        fprintf(stderr, "Invalid metadata block size: is not a multiple of %d\n", DIRECT_IO_ALIGNMENT);
        return 1;
    }
    int buf_size = 1024*1024;
    if (buf_size % dsk.meta_block_size)
        buf_size = 8*dsk.meta_block_size;
    uint8_t *data = NULL;
    data = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, buf_size);
    blockstore_meta_header_v3_t *hdr = (blockstore_meta_header_v3_t *)data;
    if (do_open)
    {
        if (dsk.meta_fd >= 0)
        {
            fprintf(stderr, "Bug: Metadata device is already opened\n");
close_error:
            r = 1;
            goto close_free;
        }
        dsk.meta_fd = open(dsk.meta_device.c_str(), (options["io"] == "cached" ? 0 : O_DIRECT) | O_RDONLY);
        if (dsk.meta_fd < 0)
        {
            fprintf(stderr, "Failed to open metadata device %s: %s\n", dsk.meta_device.c_str(), strerror(errno));
            goto close_error;
        }
    }
    else if (dsk.meta_fd < 0)
    {
        fprintf(stderr, "Bug: Metadata device is not opened\n");
        goto close_error;
    }
    // Check superblock
    lseek64(dsk.meta_fd, dsk.meta_offset, 0);
    read_blocking(dsk.meta_fd, hdr, dsk.meta_block_size);
    if (hdr->zero == 0 && hdr->magic == BLOCKSTORE_META_MAGIC_V1 && hdr->version == BLOCKSTORE_META_FORMAT_HEAP)
    {
        if (hdr->data_csum_type != 0 &&
            hdr->data_csum_type != BLOCKSTORE_CSUM_CRC32C)
        {
            goto csum_unknown;
        }
        if (!dsk.journal_len && !with_data)
        {
            fprintf(stderr, "Buffer area (former journal) location must be specified to dump \"heap\" with data\n");
            goto close_error;
        }
        // Load buffer_area
        if (with_data)
        {
            buffer_area = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, dsk.journal_len);
            if (dsk.journal_device == dsk.meta_device || dsk.journal_device == "")
            {
                dsk.journal_fd = dsk.meta_fd;
            }
            else if (do_open)
            {
                if (dsk.journal_fd >= 0)
                {
                    fprintf(stderr, "Bug: Metadata device is already opened\n");
                    goto close_error;
                }
                dsk.journal_fd = open(dsk.journal_device.c_str(), (options["io"] == "cached" ? 0 : O_DIRECT) | O_RDONLY);
                if (dsk.journal_fd < 0)
                {
                    fprintf(stderr, "Failed to open journal device %s: %s\n", dsk.journal_device.c_str(), strerror(errno));
                    goto close_error;
                }
            }
            else if (dsk.journal_fd < 0)
            {
                fprintf(stderr, "Bug: journal device is not opened\n");
                goto close_error;
            }
            uint64_t journal_pos = 0;
            lseek64(dsk.journal_fd, dsk.journal_offset+journal_pos, 0);
            while (journal_pos < dsk.journal_len)
            {
                uint64_t read_len = buf_size < dsk.journal_len-journal_pos ? buf_size : dsk.journal_len-journal_pos;
                read_blocking(dsk.journal_fd, buffer_area+journal_pos, read_len);
                journal_pos += read_len;
            }
        }
        blockstore_heap_t *heap = new blockstore_heap_t(&dsk, buffer_area, log_level);
        // Load heap and just iterate it in memory
        hdr_fn(hdr);
        hdr = NULL;
        meta_pos = dsk.meta_block_size;
        lseek64(dsk.meta_fd, dsk.meta_offset+meta_pos, 0);
        while (meta_pos < dsk.meta_area_size)
        {
            uint64_t read_len = buf_size < dsk.meta_area_size-meta_pos ? buf_size : dsk.meta_area_size-meta_pos;
            read_blocking(dsk.meta_fd, data, read_len);
            heap->read_blocks(meta_pos-dsk.meta_block_size, read_len, data, [&](heap_object_t *obj)
            {
                obj_fn(heap, obj, ((uint8_t*)obj-data+meta_pos)/dsk.meta_block_size);
            }, [](uint32_t, uint32_t, uint8_t*){});
            meta_pos += read_len;
        }
        delete heap;
    }
    else if (hdr->zero == 0 && hdr->magic == BLOCKSTORE_META_MAGIC_V1)
    {
        dsk.meta_format = hdr->version;
        dsk.calc_lengths();
        dsk.check_lengths();
        if (hdr->version == BLOCKSTORE_META_FORMAT_V1)
        {
            // Vitastor 0.6-0.8 - static array of clean_disk_entry with bitmaps
            hdr->data_csum_type = 0;
            hdr->csum_block_size = 0;
            hdr->header_csum = 0;
        }
        else if (hdr->version == BLOCKSTORE_META_FORMAT_V2)
        {
            // Vitastor 0.9 - static array of clean_disk_entry with bitmaps and checksums
            if (hdr->data_csum_type != 0 &&
                hdr->data_csum_type != BLOCKSTORE_CSUM_CRC32C)
            {
csum_unknown:
                fprintf(stderr, "I don't know checksum format %u, the only supported format is crc32c = %u.\n", hdr->data_csum_type, BLOCKSTORE_CSUM_CRC32C);
                goto close_error;
            }
        }
        else
        {
            // Unsupported version
            fprintf(stderr, "Metadata format is too new for me (stored version is %ju, max supported %u).\n", hdr->version, BLOCKSTORE_META_FORMAT_V2);
            goto close_error;
        }
        if (hdr->meta_block_size != dsk.meta_block_size)
        {
            fprintf(stderr, "Using block size of %u bytes based on information from the superblock\n", hdr->meta_block_size);
            dsk.meta_block_size = hdr->meta_block_size;
        }
        dsk.meta_format = hdr->version;
        dsk.data_block_size = hdr->data_block_size;
        dsk.csum_block_size = hdr->csum_block_size;
        dsk.data_csum_type = hdr->data_csum_type;
        dsk.bitmap_granularity = hdr->bitmap_granularity;
        dsk.clean_entry_bitmap_size = (hdr->data_block_size / hdr->bitmap_granularity + 7) / 8;
        dsk.clean_entry_size = sizeof(clean_disk_entry) + 2*dsk.clean_entry_bitmap_size
            + (hdr->data_csum_type
                ? ((hdr->data_block_size+hdr->csum_block_size-1)/hdr->csum_block_size
                    *(hdr->data_csum_type & 0xff))
                : 0)
            + (dsk.meta_format == BLOCKSTORE_META_FORMAT_V2 ? 4 /*entry_csum*/ : 0);
        // Read
        uint64_t block_num = 0;
        hdr_fn(hdr);
        hdr = NULL;
        meta_pos = dsk.meta_block_size;
        lseek64(dsk.meta_fd, dsk.meta_offset+meta_pos, 0);
        while (meta_pos < dsk.min_meta_len)
        {
            uint64_t read_len = buf_size < dsk.min_meta_len-meta_pos ? buf_size : dsk.min_meta_len-meta_pos;
            read_blocking(dsk.meta_fd, data, read_len);
            meta_pos += read_len;
            for (uint64_t blk = 0; blk < read_len; blk += dsk.meta_block_size)
            {
                for (uint64_t ioff = 0; ioff <= dsk.meta_block_size-dsk.clean_entry_size; ioff += dsk.clean_entry_size, block_num++)
                {
                    clean_disk_entry *entry = (clean_disk_entry*)((uint8_t*)data + blk + ioff);
                    if (entry->oid.inode)
                    {
                        if (dsk.data_csum_type)
                        {
                            uint32_t *entry_csum = (uint32_t*)((uint8_t*)entry + dsk.clean_entry_size - 4);
                            if (*entry_csum != crc32c(0, entry, dsk.clean_entry_size - 4))
                            {
                                fprintf(stderr, "Metadata entry %lu is corrupt (checksum mismatch), skipping\n", block_num);
                                continue;
                            }
                        }
                        record_fn(block_num, entry, entry->bitmap);
                    }
                }
            }
        }
    }
    else
    {
        // Vitastor 0.4-0.5 - static array of clean_disk_entry without header
        lseek64(dsk.meta_fd, dsk.meta_offset, 0);
        dsk.clean_entry_bitmap_size = 0;
        dsk.clean_entry_size = sizeof(clean_disk_entry);
        uint64_t block_num = 0;
        hdr_fn(NULL);
        while (meta_pos < dsk.meta_area_size)
        {
            uint64_t read_len = buf_size < dsk.meta_area_size-meta_pos ? buf_size : dsk.meta_area_size-meta_pos;
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
close_free:
    free(data);
    if (buffer_area)
    {
        free(buffer_area);
        buffer_area = NULL;
    }
    if (do_open)
    {
        close(dsk.meta_fd);
        dsk.meta_fd = -1;
        if (dsk.journal_fd >= 0)
        {
            if (dsk.journal_fd != dsk.meta_fd)
                close(dsk.journal_fd);
            dsk.journal_fd = -1;
        }
    }
    return r;
}

int disk_tool_t::dump_load_check_superblock(const std::string & device)
{
    json11::Json sb = read_osd_superblock(device, true, false);
    if (sb.is_null())
        return 1;
    try
    {
        auto cfg = json_to_string_map(sb["params"].object_items());
        dsk.parse_config(cfg);
        dsk.data_io = dsk.meta_io = dsk.journal_io = "cached";
        dsk.open_data();
        dsk.open_meta();
        dsk.open_journal();
        dsk.calc_lengths();
    }
    catch (std::exception & e)
    {
        dsk.close_all();
        fprintf(stderr, "%s\n", e.what());
        return 1;
    }
    dsk.close_all();
    return 0;
}

int disk_tool_t::dump_meta()
{
    int r = process_meta(
        [this](blockstore_meta_header_v3_t *hdr)
        {
            if (dump_as_old)
            {
                hdr->version = BLOCKSTORE_META_FORMAT_V2;
                hdr->compacted_lsn = 0;
                hdr->header_csum = 0;
                hdr->header_csum = crc32c(0, hdr, sizeof(blockstore_meta_header_v2_t));
            }
            dump_meta_header(hdr);
        },
        [this](blockstore_heap_t *heap, heap_object_t *obj, uint32_t meta_block_num)
        {
            if (dump_as_old)
                dump_heap_entry_as_old(heap, obj);
            else
                dump_heap_entry(heap, obj);
        },
        [this](uint64_t block_num, clean_disk_entry *entry, uint8_t *bitmap) { dump_meta_entry(block_num, entry, bitmap); },
        true, true
    );
    if (r == 0)
        printf("\n]}\n");
    return r;
}

void disk_tool_t::dump_meta_header(blockstore_meta_header_v3_t *hdr)
{
    if (hdr)
    {
        if (hdr->version == BLOCKSTORE_META_FORMAT_V1)
        {
            printf(
                "{\"version\":\"0.6\",\"meta_block_size\":%u,\"data_block_size\":%u,\"bitmap_granularity\":%u,"
                "\"entries\":[\n",
                hdr->meta_block_size, hdr->data_block_size, hdr->bitmap_granularity
            );
        }
        else if (hdr->version == BLOCKSTORE_META_FORMAT_V2)
        {
            printf(
                "{\"version\":\"0.9\",\"meta_block_size\":%u,\"data_block_size\":%u,\"bitmap_granularity\":%u,"
                "\"data_csum_type\":\"%s\",\"csum_block_size\":%u,\"entries\":[\n",
                hdr->meta_block_size, hdr->data_block_size, hdr->bitmap_granularity,
                csum_type_str(hdr->data_csum_type).c_str(), hdr->csum_block_size
            );
        }
        else if (hdr->version == BLOCKSTORE_META_FORMAT_HEAP)
        {
            printf(
                "{\"version\":\"3.0\",\"meta_block_size\":%u,\"data_block_size\":%u,\"bitmap_granularity\":%u,"
                "\"data_csum_type\":\"%s\",\"csum_block_size\":%u,\"entries\":[\n",
                hdr->meta_block_size, hdr->data_block_size, hdr->bitmap_granularity,
                csum_type_str(hdr->data_csum_type).c_str(), hdr->csum_block_size
            );
        }
    }
    else
    {
        printf("{\"version\":\"0.5\",\"meta_block_size\":%u,\"entries\":[\n", dsk.meta_block_size);
    }
    first_entry = true;
}

void disk_tool_t::dump_heap_entry_as_old(blockstore_heap_t *heap, heap_object_t *obj)
{
    heap_write_t *wr = NULL;
    for (wr = obj->get_writes(); wr && wr->entry_type != (BS_HEAP_BIG_WRITE|BS_HEAP_STABLE) &&
        wr->entry_type != (BS_HEAP_TOMBSTONE|BS_HEAP_STABLE); wr = wr->next())
    {
    }
    if (!wr || wr->entry_type != (BS_HEAP_BIG_WRITE|BS_HEAP_STABLE))
    {
        return;
    }
    printf(
#define ENTRY_FMT "{\"block\":%u,\"pool\":%u,\"inode\":\"0x%jx\",\"stripe\":\"0x%jx\",\"version\":%ju"
        (first_entry ? ENTRY_FMT : (",\n" ENTRY_FMT)),
#undef ENTRY_FMT
        wr->big().block_num, INODE_POOL(obj->inode), INODE_NO_POOL(obj->inode),
        obj->stripe, wr->version
    );
    printf(",\"bitmap\":\"");
    uint8_t* bitmap = wr->get_int_bitmap(heap);
    for (uint64_t i = 0; i < dsk.clean_entry_bitmap_size; i++)
    {
        printf("%02x", bitmap[i]);
    }
    bitmap = wr->get_ext_bitmap(heap);
    printf("\",\"ext_bitmap\":\"");
    for (uint64_t i = 0; i < dsk.clean_entry_bitmap_size; i++)
    {
        printf("%02x", bitmap[i]);
    }
    uint8_t *csums = wr->get_checksums(heap);
    uint32_t csum_size = wr->get_csum_size(heap);
    if (csums)
    {
        printf("\",\"block_csums\":\"");
        for (uint32_t i = 0; i < csum_size; i++)
        {
            printf("%02x", csums[i]);
        }
    }
    if (wr->get_checksum(heap))
    {
        printf("\",\"crc32c\":\"%08x", *wr->get_checksum(heap));
    }
    printf("\"}");
    first_entry = false;
}

void disk_tool_t::dump_heap_entry(blockstore_heap_t *heap, heap_object_t *obj)
{
    printf(
#define ENTRY_FMT "{\"pool\":%u,\"inode\":\"0x%jx\",\"stripe\":\"0x%jx\",\"writes\":["
        (first_entry ? ENTRY_FMT : (",\n" ENTRY_FMT)),
#undef ENTRY_FMT
        INODE_POOL(obj->inode), INODE_NO_POOL(obj->inode), obj->stripe
    );
    heap_write_t *wr = NULL;
    bool first_wr = true;
    for (wr = obj->get_writes(); wr; wr = wr->next())
    {
        printf(
#define ENTRY_FMT "{\"lsn\":%ju,\"version\":%ju,\"type\":\"%s\",\"stable\":%s"
            (first_wr ? ENTRY_FMT : ("," ENTRY_FMT)),
#undef ENTRY_FMT
            wr->lsn, wr->version, (wr->entry_type & BS_HEAP_TYPE) == BS_HEAP_SMALL_WRITE ? "small" : (
                (wr->entry_type & BS_HEAP_TYPE) == BS_HEAP_BIG_WRITE ? "big" : (
                (wr->entry_type & BS_HEAP_TYPE) == BS_HEAP_INTENT_WRITE ? "intent" : (
                (wr->entry_type & BS_HEAP_TYPE) == BS_HEAP_TOMBSTONE ? "tombstone" : "unknown"))),
            (wr->entry_type & BS_HEAP_STABLE) ? "true" : "false"
        );
        if ((wr->entry_type & BS_HEAP_TYPE) == BS_HEAP_BIG_WRITE)
        {
            printf(",\"location\":%ju", wr->big_location(heap));
        }
        else if ((wr->entry_type & BS_HEAP_TYPE) == BS_HEAP_INTENT_WRITE)
        {
            printf(",\"offset\":%u,\"len\":%u", wr->small().offset, wr->small().len);
        }
        else if ((wr->entry_type & BS_HEAP_TYPE) == BS_HEAP_SMALL_WRITE)
        {
            if (!dump_with_data)
            {
                printf(",\"offset\":%u,\"len\":%u,\"location\":%ju", wr->small().offset, wr->small().len, wr->small().location);
            }
            else
            {
                printf(",\"data\":\"");
                for (uint32_t i = 0; i < wr->small().len; i++)
                    printf("%02x", buffer_area[wr->small().location + i]);
                printf("\"");
            }
        }
        uint8_t* bitmap = wr->get_int_bitmap(heap);
        if (bitmap)
        {
            printf(",\"bitmap\":\"");
            for (uint64_t i = 0; i < dsk.clean_entry_bitmap_size; i++)
                printf("%02x", bitmap[i]);
            printf("\"");
        }
        bitmap = wr->get_ext_bitmap(heap);
        if (bitmap)
        {
            printf(",\"ext_bitmap\":\"");
            for (uint64_t i = 0; i < dsk.clean_entry_bitmap_size; i++)
                printf("%02x", bitmap[i]);
            printf("\"");
        }
        uint8_t *csums = wr->get_checksums(heap);
        if (csums)
        {
            printf(",\"block_csums\":\"");
            uint32_t csum_size = wr->get_csum_size(heap);
            for (uint32_t i = 0; i < csum_size; i++)
                printf("%02x", csums[i]);
            printf("\"");
        }
        if (wr->get_checksum(heap))
        {
            printf(",\"data_crc32c\":\"%08x\"", *wr->get_checksum(heap));
        }
        printf("}");
        first_wr = false;
    }
    printf("]}");
    first_entry = false;
}

void disk_tool_t::dump_meta_entry(uint64_t block_num, clean_disk_entry *entry, uint8_t *bitmap)
{
    printf(
#define ENTRY_FMT "{\"block\":%ju,\"pool\":%u,\"inode\":\"0x%jx\",\"stripe\":\"0x%jx\",\"version\":%ju"
        (first_entry ? ENTRY_FMT : (",\n" ENTRY_FMT)),
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
        if (dsk.csum_block_size && dsk.data_csum_type)
        {
            uint8_t *csums = bitmap + dsk.clean_entry_bitmap_size*2;
            printf("\",\"block_csums\":\"");
            for (uint64_t i = 0; i < (dsk.data_block_size+dsk.csum_block_size-1)/dsk.csum_block_size*(dsk.data_csum_type & 0xFF); i++)
            {
                printf("%02x", csums[i]);
            }
        }
        printf("\"}");
    }
    else
    {
        printf("}");
    }
    first_entry = false;
}

int disk_tool_t::write_json_meta(json11::Json meta)
{
    new_meta_buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, new_meta_len);
    memset(new_meta_buf, 0, new_meta_len);
    blockstore_meta_header_v2_t *new_hdr = (blockstore_meta_header_v2_t *)new_meta_buf;
    new_hdr->zero = 0;
    new_hdr->magic = BLOCKSTORE_META_MAGIC_V1;
    new_hdr->version = meta["version"].uint64_value() == BLOCKSTORE_META_FORMAT_V1
        ? BLOCKSTORE_META_FORMAT_V1 : BLOCKSTORE_META_FORMAT_V2;
    new_hdr->meta_block_size = meta["meta_block_size"].uint64_value()
        ? meta["meta_block_size"].uint64_value() : 4096;
    new_hdr->data_block_size = meta["data_block_size"].uint64_value()
        ? meta["data_block_size"].uint64_value() : 131072;
    new_hdr->bitmap_granularity = meta["bitmap_granularity"].uint64_value()
        ? meta["bitmap_granularity"].uint64_value() : 4096;
    if (new_hdr->version >= BLOCKSTORE_META_FORMAT_V2)
    {
        new_hdr->data_csum_type = meta["data_csum_type"].is_number()
            ? meta["data_csum_type"].uint64_value()
            : (meta["data_csum_type"].string_value() == "crc32c"
                ? BLOCKSTORE_CSUM_CRC32C
                : BLOCKSTORE_CSUM_NONE);
        new_hdr->csum_block_size = meta["csum_block_size"].uint64_value();
        new_hdr->header_csum = crc32c(0, new_hdr, sizeof(blockstore_meta_header_v2_t));
    }
    uint32_t new_clean_entry_header_size = (new_hdr->version == BLOCKSTORE_META_FORMAT_V1
        ? sizeof(clean_disk_entry) : sizeof(clean_disk_entry) + 4 /*entry_csum*/);
    new_clean_entry_bitmap_size = (new_hdr->data_block_size / new_hdr->bitmap_granularity + 7) / 8;
    new_data_csum_size = (new_hdr->data_csum_type
        ? ((new_hdr->data_block_size+new_hdr->csum_block_size-1)/new_hdr->csum_block_size*(new_hdr->data_csum_type & 0xFF))
        : 0);
    new_clean_entry_size = new_clean_entry_header_size + 2*new_clean_entry_bitmap_size + new_data_csum_size;
    new_entries_per_block = new_hdr->meta_block_size / new_clean_entry_size;
    // FIXME: Use a streaming json parser
    for (const auto & e: meta["entries"].array_items())
    {
        uint64_t data_block = e["block"].uint64_value();
        uint64_t mb = 1 + data_block/new_entries_per_block;
        if (mb >= new_meta_len/new_hdr->meta_block_size)
        {
            free(new_meta_buf);
            new_meta_buf = NULL;
            fprintf(stderr, "Metadata (data block %ju) doesn't fit into the new area\n", data_block);
            return 1;
        }
        clean_disk_entry *new_entry = (clean_disk_entry*)(new_meta_buf +
            new_hdr->meta_block_size*mb +
            new_clean_entry_size*(data_block % new_entries_per_block));
        new_entry->oid.inode = (sscanf_json(NULL, e["pool"]) << (64-POOL_ID_BITS)) | sscanf_json(NULL, e["inode"]);
        new_entry->oid.stripe = sscanf_json(NULL, e["stripe"]);
        new_entry->version = sscanf_json(NULL, e["version"]);
        fromhexstr(e["bitmap"].string_value(), new_clean_entry_bitmap_size,
            ((uint8_t*)new_entry) + sizeof(clean_disk_entry));
        fromhexstr(e["ext_bitmap"].string_value(), new_clean_entry_bitmap_size,
            ((uint8_t*)new_entry) + sizeof(clean_disk_entry) + new_clean_entry_bitmap_size);
        if (new_hdr->version == BLOCKSTORE_META_FORMAT_V2)
        {
            if (new_hdr->data_csum_type != 0)
            {
                fromhexstr(e["data_csum"].string_value(), new_data_csum_size,
                    ((uint8_t*)new_entry) + sizeof(clean_disk_entry) + 2*new_clean_entry_bitmap_size);
            }
            uint32_t *new_entry_csum = (uint32_t*)(((uint8_t*)new_entry) + new_clean_entry_size - 4);
            *new_entry_csum = crc32c(0, new_entry, new_clean_entry_size - 4);
        }
    }
    int r = resize_write_new_meta();
    free_new_meta();
    return r;
}

int disk_tool_t::write_json_heap(json11::Json meta, json11::Json journal)
{
    new_meta_hdr->zero = 0;
    new_meta_hdr->magic = BLOCKSTORE_META_MAGIC_V1;
    new_meta_hdr->version = BLOCKSTORE_META_FORMAT_HEAP;
    new_meta_hdr->meta_block_size = meta["meta_block_size"].uint64_value()
        ? meta["meta_block_size"].uint64_value() : 4096;
    new_meta_hdr->data_block_size = meta["data_block_size"].uint64_value()
        ? meta["data_block_size"].uint64_value() : 131072;
    new_meta_hdr->bitmap_granularity = meta["bitmap_granularity"].uint64_value()
        ? meta["bitmap_granularity"].uint64_value() : 4096;
    new_meta_hdr->data_csum_type = meta["data_csum_type"].is_number()
        ? meta["data_csum_type"].uint64_value()
        : (meta["data_csum_type"].string_value() == "crc32c"
            ? BLOCKSTORE_CSUM_CRC32C
            : BLOCKSTORE_CSUM_NONE);
    new_meta_hdr->csum_block_size = meta["csum_block_size"].uint64_value();
    new_meta_hdr->header_csum = crc32c(0, new_meta_hdr, sizeof(blockstore_meta_header_v3_t));
    new_clean_entry_bitmap_size = (new_meta_hdr->data_block_size / new_meta_hdr->bitmap_granularity + 7) / 8;
    new_clean_entry_size = 0;
    new_entries_per_block = 0;
    new_data_csum_size = (new_meta_hdr->data_csum_type
        ? ((new_meta_hdr->data_block_size+new_meta_hdr->csum_block_size-1)/new_meta_hdr->csum_block_size*(new_meta_hdr->data_csum_type & 0xFF))
        : 0);
    new_journal_buf = new_journal_len ? (uint8_t*)memalign(MEM_ALIGNMENT, new_journal_len) : NULL;
    if (new_journal_len)
    {
        memset(new_journal_buf, 0, new_journal_len);
    }
    uint64_t total_used_space = 0;
    uint32_t used_space = 0;
    // FIXME: Use a streaming json parser
    if (meta["version"] == "3.0")
    {
        // New format
        std::vector<uint8_t> object_buf;
        new_heap = new blockstore_heap_t(&dsk, new_journal_buf, 0);
        for (const auto & meta_entry: meta["entries"].array_items())
        {
            bool invalid = false;
            object_id oid = {
                .inode = (sscanf_json(NULL, meta_entry["pool"]) << (64-POOL_ID_BITS)) | sscanf_json(NULL, meta_entry["inode"]),
                .stripe = sscanf_json(NULL, meta_entry["stripe"]),
            };
            object_buf.clear();
            object_buf.resize(sizeof(heap_object_t));
            heap_object_t *obj = (heap_object_t*)object_buf.data();
            obj->size = sizeof(heap_object_t);
            obj->write_pos = meta_entry["writes"].array_items().size() ? sizeof(heap_object_t) : 0;
            obj->entry_type = BS_HEAP_OBJECT;
            obj->inode = oid.inode;
            obj->stripe = oid.stripe;
            size_t pos = sizeof(heap_object_t);
            heap_write_t *last_wr = NULL;
            for (auto & write_entry: meta_entry["writes"].array_items())
            {
                object_buf.resize(object_buf.size() + new_heap->get_max_write_entry_size());
                heap_write_t *wr = (heap_write_t*)(object_buf.data() + pos);
                last_wr = wr;
                uint8_t wr_type = 0;
                if (write_entry["type"] == "small")
                    wr_type = BS_HEAP_SMALL_WRITE;
                else if (write_entry["type"] == "intent")
                    wr_type = BS_HEAP_INTENT_WRITE;
                else if (write_entry["type"] == "big")
                    wr_type = BS_HEAP_BIG_WRITE;
                else if (write_entry["type"] == "tombstone")
                    wr_type = BS_HEAP_TOMBSTONE;
                else
                {
                    fprintf(stderr, "Write entry in %s has invalid type: %s, skipping object\n", meta_entry.dump().c_str(), write_entry["type"].dump().c_str());
                    invalid = true;
                    break;
                }
                wr->entry_type = wr_type | (write_entry["stable"].bool_value() ? BS_HEAP_STABLE : 0);
                wr->lsn = write_entry["lsn"].uint64_value();
                wr->version = write_entry["version"].uint64_value();
                wr->size = wr->get_size(new_heap);
                wr->next_pos = wr->size;
                if (wr_type == BS_HEAP_SMALL_WRITE || wr_type == BS_HEAP_INTENT_WRITE)
                {
                    wr->small().offset = write_entry["offset"].uint64_value();
                    wr->small().len = write_entry["len"].uint64_value();
                    wr->small().location = write_entry["location"].uint64_value();
                    if (wr_type == BS_HEAP_SMALL_WRITE && write_entry["data"].is_string() && wr->small().len > 0)
                    {
                        if (!new_journal_buf)
                        {
                            fprintf(stderr, "Loading small write data requires overwriting buffer area\n");
                            free_new_meta();
                            return 1;
                        }
                        wr->small().location = new_heap->find_free_buffer_area(wr->small().len);
                        fromhexstr(write_entry["data"].string_value(), wr->small().len, new_journal_buf + wr->small().location);
                    }
                }
                else if (wr_type == BS_HEAP_BIG_WRITE)
                {
                    uint64_t loc = write_entry["location"].uint64_value();
                    assert(!(loc % dsk.data_block_size));
                    assert((loc / dsk.data_block_size) < 0xFFFF0000);
                    wr->set_big_location(new_heap, loc);
                }
                if (write_entry["bitmap"].is_string() && wr->get_int_bitmap(new_heap))
                {
                    fromhexstr(write_entry["bitmap"].string_value(), new_clean_entry_bitmap_size, wr->get_int_bitmap(new_heap));
                }
                if (write_entry["ext_bitmap"].is_string() && wr->get_ext_bitmap(new_heap))
                {
                    fromhexstr(write_entry["ext_bitmap"].string_value(), new_clean_entry_bitmap_size, wr->get_ext_bitmap(new_heap));
                }
                if (write_entry["block_csums"].is_string() && wr->get_checksums(new_heap))
                {
                    fromhexstr(write_entry["block_csums"].string_value(), wr->get_csum_size(new_heap), wr->get_ext_bitmap(new_heap));
                }
                if (write_entry["data_crc32c"].is_string() && wr->get_checksum(new_heap))
                {
                    *wr->get_checksum(new_heap) = sscanf_json("%jx", write_entry["data_crc32c"]);
                }
            }
            if (invalid)
            {
                continue;
            }
            last_wr->next_pos = 0;
            new_heap->copy_object(obj, NULL);
        }
    }
    else
    {
        if (!journal.is_array())
        {
            fprintf(stderr, "Metadata should include journal in you want to convert it to the \"heap\" format\n");
close_err:
            free(new_meta_buf);
            new_meta_buf = NULL;
            return 1;
        }
        std::map<object_id, std::vector<json11::Json::object>> journal_by_object;
        if (index_journal_by_object(journal, journal_by_object) != 0)
        {
            goto close_err;
        }
        journal = json11::Json();
        // Convert old format to the new format
        uint64_t next_lsn = 0;
        uint64_t meta_offset = 0;
        const uint32_t space_per_object = sizeof(heap_object_t) + sizeof(heap_write_t) +
            new_clean_entry_bitmap_size*2 + new_data_csum_size;
        uint64_t buffer_pos = 0;
        // FIXME: Rather ugly. Remove the dependency on dsk from heap?
        blockstore_disk_t dsk;
        dsk.bitmap_granularity = new_meta_hdr->bitmap_granularity;
        dsk.block_count = 16;
        dsk.data_block_size = new_meta_hdr->data_block_size;
        dsk.clean_entry_bitmap_size = new_clean_entry_bitmap_size;
        dsk.csum_block_size = new_meta_hdr->csum_block_size;
        dsk.data_csum_type = new_meta_hdr->data_csum_type;
        dsk.journal_len = 4096;
        dsk.meta_area_size = new_meta_len;
        dsk.meta_block_size = new_meta_hdr->meta_block_size;
        dsk.meta_block_target_free_space = 800;
        blockstore_heap_t heap(&dsk, NULL, 0);
        for (const auto & meta_entry: meta["entries"].array_items())
        {
            object_id oid = {
                .inode = (sscanf_json(NULL, meta_entry["pool"]) << (64-POOL_ID_BITS)) | sscanf_json(NULL, meta_entry["inode"]),
                .stripe = sscanf_json(NULL, meta_entry["stripe"]),
            };
            uint32_t space_for_this = space_per_object;
            auto j_it = journal_by_object.find(oid);
            if (j_it != journal_by_object.end())
            {
                for (auto & rec: j_it->second)
                {
                    if (rec["type"] == "small_write" || rec["type"] == "small_write_instant")
                    {
                        uint64_t off = rec["offset"].uint64_value();
                        uint64_t len = rec["len"].uint64_value();
                        if (off+len > new_meta_hdr->data_block_size)
                        {
                            fprintf(stderr, "Journal entry has too large offset or length: %s\n", json11::Json(rec).dump().c_str());
                            goto close_err;
                        }
                        space_for_this += sizeof(heap_write_t) + new_clean_entry_bitmap_size +
                            ((off+len+new_meta_hdr->csum_block_size-1)/new_meta_hdr->csum_block_size - off/new_meta_hdr->csum_block_size) * (new_meta_hdr->data_csum_type & 0xFF);
                    }
                    else /*if (rec["type"] == "big_write" || rec["type"] == "big_write_instant")*/
                    {
                        space_for_this += sizeof(heap_write_t) + 2*new_clean_entry_bitmap_size + new_data_csum_size;
                    }
                }
            }
            if (space_for_this > new_meta_hdr->meta_block_size)
            {
                fprintf(stderr, "Object doesn't fit in a single metadata block. Object meta: %s, object journal: %s\n",
                    meta_entry.dump().c_str(), json11::Json(j_it->second).dump().c_str());
                goto close_err;
            }
            if (used_space + space_for_this > new_meta_hdr->meta_block_size-dsk.meta_block_target_free_space)
            {
                if (used_space < new_meta_hdr->meta_block_size-2)
                {
                    *((uint16_t*)(new_meta_buf + meta_offset + used_space)) = FREE_SPACE_BIT | (uint16_t)(new_meta_hdr->meta_block_size-used_space);
                }
                meta_offset += new_meta_hdr->meta_block_size;
                used_space = 0;
                if (meta_offset >= new_meta_len)
                {
                    fprintf(stderr, "Metadata doesn't fit into the new area (total used space: %ju, minimum free space in block: %u/%u)\n",
                        total_used_space, dsk.meta_block_target_free_space, new_meta_hdr->meta_block_size);
                    goto close_err;
                }
            }
            heap_object_t *obj = (heap_object_t*)(new_meta_buf + meta_offset + used_space);
            obj->size = sizeof(heap_object_t);
            obj->write_pos = sizeof(heap_object_t);
            obj->entry_type = BS_HEAP_OBJECT;
            obj->inode = oid.inode;
            obj->stripe = oid.stripe;
            heap_write_t *wr = obj->get_writes();
            wr->next_pos = 0;
            wr->entry_type = BS_HEAP_BIG_WRITE|BS_HEAP_STABLE;
            wr->lsn = ++next_lsn;
            wr->version = sscanf_json(NULL, meta_entry["version"]);
            wr->set_big_location(&heap, meta_entry["block"].uint64_value() * new_meta_hdr->data_block_size);
            wr->size = wr->get_size(&heap);
            fromhexstr(meta_entry["bitmap"].string_value(), new_clean_entry_bitmap_size, wr->get_int_bitmap(&heap));
            fromhexstr(meta_entry["ext_bitmap"].string_value(), new_clean_entry_bitmap_size, wr->get_ext_bitmap(&heap));
            if (new_meta_hdr->data_csum_type != 0)
                fromhexstr(meta_entry["data_csum"].string_value(), new_data_csum_size, wr->get_checksums(&heap));
            if (j_it != journal_by_object.end())
            {
                for (auto & rec: j_it->second)
                {
                    wr->next_pos = wr->get_size(&heap);
                    wr = wr->next();
                    wr->next_pos = 0;
                    wr->lsn = ++next_lsn;
                    wr->version = rec["ver"].uint64_value();
                    uint64_t wr_offset = rec["offset"].uint64_value();
                    uint64_t wr_len = rec["len"].uint64_value();
                    if (rec["type"] == "small_write" || rec["type"] == "small_write_instant")
                    {
                        if (wr_len > 0 && !rec["data"].is_string())
                        {
                            fprintf(stderr, "Error: entry data is missing, please generate the dump with --json --format data\n");
                            goto close_err;
                        }
                        wr->entry_type = BS_HEAP_SMALL_WRITE | (rec["type"] == "small_write_instant" ? BS_HEAP_STABLE : 0);
                        wr->small().offset = wr_offset;
                        wr->small().len = wr_len;
                        wr->small().location = buffer_pos;
                        fromhexstr(rec["bitmap"].string_value(), new_clean_entry_bitmap_size, wr->get_ext_bitmap(&heap));
                        fromhexstr(rec["data"].string_value(), wr_len, new_journal_buf+buffer_pos);
                        if (wr_len > 0)
                        {
                            if (!new_meta_hdr->data_csum_type)
                                *wr->get_checksum(&heap) = crc32c(0, new_journal_buf+buffer_pos, wr_len);
                            else
                                heap.calc_block_checksums((uint32_t*)wr->get_checksums(&heap), new_journal_buf+buffer_pos, NULL, wr_offset, wr_offset+wr_len, true, NULL);
                        }
                        buffer_pos += wr_len;
                    }
                    else if (rec["type"] == "big_write" || rec["type"] == "big_write_instant")
                    {
                        wr->entry_type = BS_HEAP_BIG_WRITE | (rec["type"] == "big_write_instant" ? BS_HEAP_STABLE : 0);
                        wr->set_big_location(&heap, sscanf_json(NULL, rec["loc"]));
                        bitmap_set(wr->get_int_bitmap(&heap), wr_offset, wr_len, new_meta_hdr->bitmap_granularity);
                        fromhexstr(rec["bitmap"].string_value(), new_clean_entry_bitmap_size, wr->get_ext_bitmap(&heap));
                        if (new_meta_hdr->data_csum_type != 0)
                        {
                            if ((wr_offset % new_meta_hdr->csum_block_size) || (wr_len % new_meta_hdr->csum_block_size))
                            {
                                fprintf(stderr,
                                    "Error: big_write journal entries not aligned to csum_block_size can't be converted between v0.9 and v3.0 metadata\n"
                                    "Stop writes and flush the journal or convert OSDs one by one without the journal if you still want to do it.\n");
                                goto close_err;
                            }
                            fromhexstr(rec["block_csums"].string_value(),
                                ((wr_offset+wr_len+new_meta_hdr->csum_block_size-1)/new_meta_hdr->csum_block_size
                                    - wr_offset/new_meta_hdr->csum_block_size) * (new_meta_hdr->data_csum_type & 0xFF),
                                wr->get_checksums(&heap));
                        }
                    }
                    else
                    {
                        assert(0);
                    }
                    wr->size = wr->get_size(&heap);
                }
            }
            obj->crc32c = obj->calc_crc32c();
            assert(((uint8_t*)wr + wr->size - (uint8_t*)obj) == space_for_this);
            used_space += space_for_this;
            total_used_space += space_for_this;
        }
        if (used_space > 0 && used_space < new_meta_hdr->meta_block_size-2)
        {
            *((uint16_t*)(new_meta_buf + meta_offset + used_space)) = FREE_SPACE_BIT | (uint16_t)(new_meta_hdr->meta_block_size-used_space);
        }
    }
    int r = resize_write_new_meta();
    if (r == 0)
    {
        r = resize_write_new_journal();
    }
    free_new_meta();
    return r;
}

int disk_tool_t::index_journal_by_object(json11::Json journal,
    std::map<object_id, std::vector<json11::Json::object>> & journal_by_object)
{
    for (const auto & rec: journal.array_items())
    {
        object_id oid = {
            .inode = sscanf_json(NULL, rec["inode"]),
            .stripe = sscanf_json(NULL, rec["stripe"]),
        };
        auto & jbo = journal_by_object[oid];
        if (rec["type"] == "small_write" || rec["type"] == "small_write_instant")
        {
            jbo.push_back(rec.object_items());
        }
        else if (rec["type"] == "big_write" || rec["type"] == "big_write_instant")
        {
            if (rec["type"] == "big_write_instant")
                jbo.clear();
            jbo.push_back(rec.object_items());
        }
        else if (rec["type"] == "delete")
        {
            jbo.clear();
        }
        else if (rec["type"] == "stable")
        {
            uint64_t commit_to = rec["version"].uint64_value();
            for (size_t i = 0; i < jbo.size(); i++)
            {
                if (jbo[i]["version"].uint64_value() <= commit_to)
                {
                    if (jbo[i]["type"] == "big_write")
                    {
                        jbo.erase(jbo.begin(), jbo.begin()+i);
                        i = 0;
                        jbo[i]["type"] = "big_write_instant";
                    }
                    else if (jbo[i]["type"] == "small_write")
                    {
                        jbo[i]["type"] = "small_write_instant";
                    }
                }
            }
        }
        else if (rec["type"] == "rollback")
        {
            uint64_t rollback_to = rec["version"].uint64_value();
            for (size_t i = jbo.size()-1; i >= 0; i--)
            {
                if (jbo[i]["version"].uint64_value() > rollback_to)
                    jbo.erase(jbo.begin()+i, jbo.begin()+i+1);
            }
        }
        else
        {
            fprintf(stderr, "Unknown journal entry type: %s\n", rec.dump().c_str());
            return -1;
        }
    }
    return 0;
}
