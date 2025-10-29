// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#define _XOPEN_SOURCE
#include <limits.h>

#include "disk_tool.h"
#include "rw_blocking.h"
#include "str_util.h"
#include "malloc_or_die.h"

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

int disk_tool_t::raw_resize()
{
    int r;
    // Parse parameters
    r = resize_parse_params();
    if (r != 0)
        goto ret;
    // Fill allocator
    fprintf(stderr, "Reading metadata\n");
    data_alloc = new allocator_t((new_data_len < dsk.data_len ? dsk.data_len : new_data_len) / dsk.data_block_size);
    r = process_meta(
        [this](blockstore_meta_header_v3_t *hdr)
        {
            resize_init(hdr);
        },
        [this](blockstore_heap_t *heap, heap_entry_t *obj, uint32_t meta_block_num)
        {
            for (auto wr = obj; wr; wr = heap->prev(wr))
            {
                if ((wr->entry_type & BS_HEAP_TYPE) == BS_HEAP_BIG_WRITE ||
                    (wr->entry_type & BS_HEAP_TYPE) == BS_HEAP_BIG_INTENT)
                {
                    data_alloc->set(wr->big().block_num, true);
                }
            }
        },
        [this](uint64_t block_num, clean_disk_entry *entry, uint8_t *bitmap)
        {
            data_alloc->set(block_num, true);
        },
        true, true
    );
    if (r != 0)
        goto ret;
    if (dsk.meta_format != BLOCKSTORE_META_FORMAT_HEAP)
    {
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
            goto ret;
    }
    // Remap blocks
    r = resize_remap_blocks();
    if (r != 0)
        goto ret;
    // Copy data blocks into new places
    fprintf(stderr, "Moving data blocks\n");
    r = resize_copy_data();
    if (r != 0)
        goto ret;
    // Rewrite metadata
    resize_alloc_journal();
    fprintf(stderr, "Rebuilding metadata\n");
    r = resize_rebuild_meta();
    if (r != 0)
        goto ret;
    if (new_meta_format != BLOCKSTORE_META_FORMAT_HEAP)
    {
        // Rewrite journal
        fprintf(stderr, "Rebuilding journal\n");
        r = resize_rebuild_journal();
        if (r != 0)
            goto ret;
        fprintf(stderr, "Writing new journal\n");
    }
    else
        fprintf(stderr, "Writing new buffer area\n");
    // Write new journal
    r = resize_write_new_journal();
    if (r != 0)
        goto ret;
    // Write new metadata
    fprintf(stderr, "Writing new metadata\n");
    r = resize_write_new_meta();
    if (r != 0)
        goto ret;
    fprintf(stderr, "Done\n");
ret:
    free_new_meta();
    return 0;
}

int disk_tool_t::resize_parse_params()
{
    try
    {
        dsk.parse_config(options);
        dsk.data_io = dsk.meta_io = dsk.journal_io = "cached";
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
        ? parse_size(options["new_data_offset"]) : dsk.data_offset;
    new_data_len = options.find("new_data_len") != options.end()
        ? parse_size(options["new_data_len"])
        : (options.find("new_data_offset") != options.end()
            ? dsk.data_device_size-new_data_offset
            : dsk.data_len);
    new_meta_offset = options.find("new_meta_offset") != options.end()
        ? parse_size(options["new_meta_offset"]) : dsk.meta_offset;
    new_meta_len = options.find("new_meta_len") != options.end()
        ? parse_size(options["new_meta_len"]) : 0; // will be calculated in resize_init()
    new_journal_offset = options.find("new_journal_offset") != options.end()
        ? parse_size(options["new_journal_offset"]) : dsk.journal_offset;
    new_journal_len = options.find("new_journal_len") != options.end()
        ? parse_size(options["new_journal_len"]) : dsk.journal_len;
    new_meta_format = options.find("new_meta_format") != options.end()
        ? stoull_full(options["new_meta_format"]) : 0;
    if (new_data_len+new_data_offset > dsk.data_device_size)
        new_data_len = dsk.data_device_size-new_data_offset;
    if (new_meta_device == dsk.data_device && new_data_offset < new_meta_offset &&
        new_data_len+new_data_offset > new_meta_offset)
        new_data_len = new_meta_offset-new_data_offset;
    if (new_journal_device == dsk.data_device && new_data_offset < new_journal_offset &&
        new_data_len+new_data_offset > new_journal_offset)
        new_data_len = new_journal_offset-new_data_offset;
    if (new_meta_device == dsk.meta_device &&
        new_journal_device == dsk.journal_device &&
        new_data_offset == dsk.data_offset &&
        new_data_len == dsk.data_len &&
        new_meta_offset == dsk.meta_offset &&
        (new_meta_len == dsk.meta_area_size || new_meta_len == 0) &&
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

void disk_tool_t::resize_init(blockstore_meta_header_v3_t *hdr)
{
    if (hdr && dsk.data_block_size != hdr->data_block_size)
    {
        if (dsk.data_block_size)
        {
            fprintf(stderr, "Using data block size of %u bytes from metadata superblock\n", hdr->data_block_size);
        }
        dsk.data_block_size = hdr->data_block_size;
    }
    if (hdr && (dsk.data_csum_type != hdr->data_csum_type || dsk.csum_block_size != hdr->csum_block_size))
    {
        if (dsk.data_csum_type)
        {
            fprintf(stderr, "Using data checksum type %s from metadata superblock\n", csum_type_str(hdr->data_csum_type).c_str());
        }
        dsk.data_csum_type = hdr->data_csum_type;
        dsk.csum_block_size = hdr->csum_block_size;
    }
    if (hdr && dsk.meta_format != hdr->version)
    {
        dsk.meta_format = hdr->version;
    }
    if (new_meta_format == 0)
    {
        new_meta_format = hdr && hdr->version == BLOCKSTORE_META_FORMAT_HEAP ? BLOCKSTORE_META_FORMAT_HEAP : BLOCKSTORE_META_FORMAT_V2;
    }
    dsk.calc_lengths();
    if (((new_data_offset-dsk.data_offset) % dsk.data_block_size))
    {
        fprintf(stderr, "Data alignment mismatch: old data offset is 0x%jx, new is 0x%jx, but alignment on %x should be equal\n",
            dsk.data_offset, new_data_offset, dsk.data_block_size);
        exit(1);
    }
    data_idx_diff = ((int64_t)(dsk.data_offset-new_data_offset)) / dsk.data_block_size;
    free_first = new_data_offset > dsk.data_offset ? (new_data_offset-dsk.data_offset) / dsk.data_block_size : 0;
    free_last = (new_data_offset+new_data_len < dsk.data_offset+dsk.data_len)
        ? (dsk.data_offset+dsk.data_len-new_data_offset-new_data_len) / dsk.data_block_size
        : 0;
    uint32_t new_clean_entry_header_size = sizeof(clean_disk_entry) + 4 /*entry_csum*/;
    new_clean_entry_bitmap_size = dsk.data_block_size / (hdr ? hdr->bitmap_granularity : 4096) / 8;
    new_data_csum_size = (dsk.data_csum_type
        ? ((dsk.data_block_size+dsk.csum_block_size-1)/dsk.csum_block_size*(dsk.data_csum_type & 0xFF))
        : 0);
    new_clean_entry_size = new_clean_entry_header_size + 2*new_clean_entry_bitmap_size + new_data_csum_size;
    new_entries_per_block = dsk.meta_block_size/new_clean_entry_size;
    uint64_t new_meta_blocks = 1 + (new_data_len/dsk.data_block_size + new_entries_per_block-1) / new_entries_per_block;
    if (!new_meta_len)
    {
        new_meta_len = dsk.meta_block_size*new_meta_blocks;
    }
    if (new_meta_len < dsk.meta_block_size*new_meta_blocks)
    {
        fprintf(stderr, "New metadata area size is too small, should be at least %ju bytes\n", dsk.meta_block_size*new_meta_blocks);
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
        if (data_alloc->get(total_blocks-i-1))
            data_remap[total_blocks-i-1] = 0;
        else
            data_alloc->set(total_blocks-i-1, true);
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
    ringloop = new ring_loop_t(iodepth < RINGLOOP_DEFAULT_SIZE ? RINGLOOP_DEFAULT_SIZE : iodepth);
    dsk.data_fd = open(dsk.data_device.c_str(), (options["io"] == "cached" ? 0 : O_DIRECT) | O_RDWR);
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
                    io_uring_prep_readv(sqe, dsk.data_fd, &data->iov, 1, dsk.data_offset + moving_blocks[i].old_loc*dsk.data_block_size);
                    data->callback = [this, i](ring_data_t *data)
                    {
                        if (data->res != dsk.data_block_size)
                        {
                            fprintf(
                                stderr, "Failed to read %u bytes at %ju from %s: %s\n", dsk.data_block_size,
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
                    io_uring_prep_writev(sqe, dsk.data_fd, &data->iov, 1, dsk.data_offset + moving_blocks[i].new_loc*dsk.data_block_size);
                    data->callback = [this, i](ring_data_t *data)
                    {
                        if (data->res != dsk.data_block_size)
                        {
                            fprintf(
                                stderr, "Failed to write %u bytes at %ju to %s: %s\n", dsk.data_block_size,
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

void disk_tool_t::resize_alloc_journal()
{
    new_journal_buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, new_journal_len);
    memset(new_journal_buf, 0, new_journal_len);
    new_journal_ptr = new_journal_buf;
    new_journal_data = new_journal_ptr + dsk.journal_block_size;
    new_journal_in_pos = 0;
}

void disk_tool_t::build_journal_start()
{
    journal_entry *ne = (journal_entry*)(new_journal_ptr + new_journal_in_pos);
    *((journal_entry_start*)ne) = (journal_entry_start){
        .magic = JOURNAL_MAGIC,
        .type = JE_START,
        .size = sizeof(journal_entry_start),
        .journal_start = dsk.journal_block_size,
        .version = JOURNAL_VERSION_V2,
        .data_csum_type = dsk.data_csum_type,
        .csum_block_size = dsk.csum_block_size,
    };
    ne->crc32 = je_crc32(ne);
    new_journal_ptr += dsk.journal_block_size;
    new_journal_data = new_journal_ptr+dsk.journal_block_size;
    new_journal_in_pos = 0;
}

void disk_tool_t::choose_journal_block(uint32_t je_size)
{
    if (dsk.journal_block_size < new_journal_in_pos+je_size)
    {
        new_journal_ptr = new_journal_data;
        if (new_journal_ptr-new_journal_buf >= new_journal_len)
        {
            fprintf(stderr, "Error: live entries don't fit to the new journal\n");
            exit(1);
        }
        new_journal_data = new_journal_ptr+dsk.journal_block_size;
        new_journal_in_pos = 0;
        if (dsk.journal_block_size < je_size)
        {
            fprintf(stderr, "Error: journal entry too large (%u bytes)\n", je_size);
            exit(1);
        }
    }
}

int disk_tool_t::resize_rebuild_journal()
{
    // Simply overwriting on the fly may be impossible because old and new areas may overlap
    // For now, just build new journal data in memory
    process_journal([this](void *buf)
    {
        return process_journal_block(buf, [this](int num, journal_entry *je)
        {
            if (je->type == JE_START)
            {
                if (je_start.data_csum_type != dsk.data_csum_type ||
                    je_start.csum_block_size != dsk.csum_block_size)
                {
                    fprintf(
                        stderr, "Error: journal header has different checksum parameters: %s/%u vs %s/%u\n",
                        csum_type_str(je_start.data_csum_type).c_str(), je_start.csum_block_size,
                        csum_type_str(dsk.data_csum_type).c_str(), dsk.csum_block_size
                    );
                    exit(1);
                }
                build_journal_start();
            }
            else
            {
                choose_journal_block(je->size);
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
    new_journal_fd = open(new_journal_device.c_str(), (options["io"] == "cached" ? 0 : O_DIRECT) | O_RDWR);
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
    return 0;
}

void disk_tool_t::remap_big_write(heap_entry_t *wr)
{
    uint64_t block_num = wr->big().block_num;
    auto remap_it = data_remap.find(block_num);
    if (remap_it != data_remap.end())
        block_num = remap_it->second;
    if (block_num < free_first || block_num >= total_blocks-free_last)
    {
        fprintf(stderr, "BUG: remapped block %ju not in range %ju..%ju\n", block_num, free_first, total_blocks-free_last);
        exit(1);
    }
    block_num += data_idx_diff;
    wr->big().block_num = block_num;
}

void disk_tool_t::remap_small_write(heap_entry_t *wr)
{
    if (new_meta_format == BLOCKSTORE_META_FORMAT_HEAP && wr->small().len > 0)
    {
        if (new_journal_ptr-new_journal_buf+wr->small().len > new_journal_len)
        {
            fprintf(stderr, "Small write data doesn't fit into the new buffer area\n");
            exit(1);
        }
        memcpy(new_journal_ptr, buffer_area+wr->small().location, wr->small().len);
        wr->small().location = new_journal_ptr-new_journal_buf;
        new_journal_ptr += wr->small().len;
    }
}

void disk_tool_t::fill_old_clean_entry(blockstore_heap_t *heap, heap_entry_t *big_wr)
{
    uint64_t block_num = big_wr->big().block_num;
    clean_disk_entry *new_entry = (clean_disk_entry*)(new_meta_buf + dsk.meta_block_size +
        dsk.meta_block_size*(block_num / new_entries_per_block) +
        new_clean_entry_size*(block_num % new_entries_per_block));
    new_entry->oid = (object_id){ .inode = big_wr->inode, .stripe = big_wr->stripe };
    new_entry->version = big_wr->version;
    memcpy(new_entry->bitmap, big_wr->get_ext_bitmap(heap), new_clean_entry_bitmap_size);
    memcpy(new_entry->bitmap + new_clean_entry_bitmap_size, big_wr->get_int_bitmap(heap), new_clean_entry_bitmap_size);
    memcpy(new_entry->bitmap + 2*new_clean_entry_bitmap_size, big_wr->get_checksums(heap), new_data_csum_size);
    uint32_t *new_entry_csum = (uint32_t*)(((uint8_t*)new_entry) + new_clean_entry_size - 4);
    *new_entry_csum = crc32c(0, new_entry, new_clean_entry_size - 4);
}

void disk_tool_t::fill_old_journal_entry(blockstore_heap_t *heap, heap_entry_t *wr)
{
    assert(wr->type() == BS_HEAP_SMALL_WRITE ||
        wr->type() == BS_HEAP_BIG_WRITE ||
        wr->type() == BS_HEAP_BIG_INTENT);
    uint32_t je_size = ((wr->entry_type & BS_HEAP_TYPE) == BS_HEAP_SMALL_WRITE
        ? sizeof(journal_entry_small_write) + dsk.dirty_dyn_size(wr->small().offset, wr->small().len)
        : sizeof(journal_entry_big_write) + dsk.dirty_dyn_size(0, dsk.data_block_size));
    choose_journal_block(je_size);
    journal_entry *je = (journal_entry*)(new_journal_ptr + new_journal_in_pos);
    je->magic = JOURNAL_MAGIC;
    je->type = (wr->entry_type & BS_HEAP_STABLE) ? JE_SMALL_WRITE_INSTANT : JE_SMALL_WRITE;
    je->size = je_size;
    je->crc32_prev = new_crc32_prev;
    je->small_write.oid = (object_id){ .inode = wr->inode, .stripe = wr->stripe };
    je->small_write.version = wr->version;
    if (wr->type() == BS_HEAP_SMALL_WRITE)
    {
        je->small_write.offset = wr->small().offset;
        je->small_write.len = wr->small().len;
        je->small_write.data_offset = new_journal_data-new_journal_buf;
        if (je->small_write.data_offset + je->small_write.len > new_journal_len)
        {
            fprintf(stderr, "Error: live entries don't fit to the new journal\n");
            exit(1);
        }
        memcpy(new_journal_data, buffer_area+wr->small().location, je->small_write.len);
        new_journal_data += je->small_write.len;
        if (dsk.data_csum_type == 0 && wr->get_checksum(heap))
            je->small_write.crc32_data = *wr->get_checksum(heap);
    }
    else
    {
        je->big_write.location = wr->big_location(heap);
    }
    memcpy((uint8_t*)je + je->size, wr->get_ext_bitmap(heap), new_clean_entry_bitmap_size);
    if (dsk.data_csum_type != 0 && wr->get_checksums(heap))
    {
        memcpy((uint8_t*)je + je->size + new_clean_entry_bitmap_size, wr->get_checksums(heap), heap->get_csum_size(wr));
    }
    je->crc32 = je_crc32(je);
    new_journal_in_pos += je->size;
    new_crc32_prev = je->crc32;
}

int disk_tool_t::resize_rebuild_meta()
{
    new_meta_buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, new_meta_len);
    memset(new_meta_buf, 0, new_meta_len);
    new_meta_hdr = (blockstore_meta_header_v3_t *)new_meta_buf;
    meta_pos = dsk.meta_block_size;
    uint64_t next_lsn = 0;
    std::vector<heap_entry_t*> writes;
    int r = process_meta(
        [&](blockstore_meta_header_v3_t *hdr)
        {
            new_meta_hdr->zero = 0;
            new_meta_hdr->magic = BLOCKSTORE_META_MAGIC_V1;
            new_meta_hdr->version = new_meta_format == 0 ? BLOCKSTORE_META_FORMAT_HEAP : new_meta_format;
            new_meta_hdr->meta_block_size = dsk.meta_block_size;
            new_meta_hdr->data_block_size = dsk.data_block_size;
            new_meta_hdr->bitmap_granularity = dsk.bitmap_granularity ? dsk.bitmap_granularity : 4096;
            new_meta_hdr->data_csum_type = dsk.data_csum_type;
            new_meta_hdr->csum_block_size = dsk.csum_block_size;
            new_meta_hdr->completed_lsn = hdr->completed_lsn;
            new_meta_hdr->header_csum = 0;
            new_meta_hdr->header_csum = crc32c(0, new_meta_hdr, new_meta_hdr->version == BLOCKSTORE_META_FORMAT_HEAP
                ? sizeof(blockstore_meta_header_v3_t) : sizeof(blockstore_meta_header_v2_t));
            if (hdr->version == BLOCKSTORE_META_FORMAT_HEAP && new_meta_format != BLOCKSTORE_META_FORMAT_HEAP)
            {
                build_journal_start();
            }
        },
        [&](blockstore_heap_t *heap, heap_entry_t *obj, uint32_t meta_block_num)
        {
            heap->iterate_with_stable(obj, obj->lsn, [&](heap_entry_t *wr, bool stable)
            {
                if (wr->type() == BS_HEAP_DELETE && stable)
                {
                    // Object is deleted, skip it
                    return false;
                }
                else if (wr->type() == BS_HEAP_BIG_WRITE || wr->type() == BS_HEAP_BIG_INTENT)
                {
                    remap_big_write(wr);
                }
                else if (wr->type() == BS_HEAP_SMALL_WRITE)
                {
                    remap_small_write(wr);
                }
                else if (new_meta_format != BLOCKSTORE_META_FORMAT_HEAP)
                {
                    fprintf(stderr, "Object %jx:%jx can't be converted to the old format because it contains an entry of type 0x%x%s\n",
                        wr->inode, wr->stripe, wr->entry_type,
                        (wr->type() == BS_HEAP_INTENT_WRITE ? " (intent_write)" : ""));
                    exit(1);
                }
                if (new_meta_format == BLOCKSTORE_META_FORMAT_HEAP)
                {
                    // New -> New
                    if ((meta_pos % dsk.meta_block_size) + wr->size > dsk.meta_block_size)
                    {
                        meta_pos = (meta_pos % dsk.meta_block_size) + dsk.meta_block_size;
                        if (meta_pos >= new_meta_len)
                        {
                            fprintf(stderr, "New metadata doesn't fit into the provided area\n");
                            exit(1);
                        }
                    }
                    memcpy(new_meta_buf + meta_pos, wr, wr->size);
                    if (wr->type() == BS_HEAP_BIG_WRITE && stable)
                    {
                        // Skip older writes
                        return false;
                    }
                }
                else
                {
                    // New -> Old
                    if (wr->type() == BS_HEAP_BIG_WRITE && stable)
                    {
                        fill_old_clean_entry(heap, wr);
                        return false;
                    }
                    else
                    {
                        writes.push_back(wr);
                    }
                }
                return true;
            });
            if (writes.size())
            {
                for (size_t i = writes.size(); i > 0; i--)
                {
                    fill_old_journal_entry(heap, writes[i-1]);
                }
                writes.clear();
            }
        },
        [&](uint64_t block_num, clean_disk_entry *entry, uint8_t *bitmap)
        {
            auto remap_it = data_remap.find(block_num);
            if (remap_it != data_remap.end())
                block_num = remap_it->second;
            if (block_num < free_first || block_num >= total_blocks-free_last)
            {
                fprintf(stderr, "BUG: remapped block %ju not in range %ju..%ju\n", block_num, free_first, total_blocks-free_last);
                exit(1);
            }
            block_num += data_idx_diff;
            if (new_meta_format == BLOCKSTORE_META_FORMAT_HEAP)
            {
                // Old -> New
                auto big_entry_size = sizeof(heap_big_write_t) + dsk.clean_entry_bitmap_size*2 +
                    (!dsk.data_csum_type ? 0 : dsk.data_block_size/dsk.csum_block_size * (dsk.data_csum_type & 0xFF));
                if ((meta_pos % dsk.meta_block_size) + big_entry_size > dsk.meta_block_size)
                {
                    meta_pos = (meta_pos % dsk.meta_block_size) + dsk.meta_block_size;
                    if (meta_pos >= new_meta_len)
                    {
                        fprintf(stderr, "New metadata doesn't fit into the provided area\n");
                        exit(1);
                    }
                }
                heap_entry_t *wr = (heap_entry_t*)(new_meta_buf + meta_pos);
                wr->size = big_entry_size;
                wr->entry_type = BS_HEAP_BIG_WRITE|BS_HEAP_STABLE;
                wr->inode = entry->oid.inode;
                wr->stripe = entry->oid.stripe;
                wr->version = entry->version;
                wr->big().block_num = block_num;
                wr->lsn = ++next_lsn;
                if (bitmap)
                {
                    memcpy(((uint8_t*)wr) + sizeof(heap_big_write_t), bitmap, new_clean_entry_bitmap_size);
                    memcpy(((uint8_t*)wr) + sizeof(heap_big_write_t) + new_clean_entry_bitmap_size, bitmap+new_clean_entry_bitmap_size, new_clean_entry_bitmap_size);
                    memcpy(((uint8_t*)wr) + sizeof(heap_big_write_t) + 2*new_clean_entry_bitmap_size, bitmap+2*new_clean_entry_bitmap_size, new_data_csum_size);
                }
                wr->crc32c = wr->calc_crc32c();
            }
            else
            {
                // Old -> Old
                clean_disk_entry *new_entry = (clean_disk_entry*)(new_meta_buf + dsk.meta_block_size +
                    dsk.meta_block_size*(block_num / new_entries_per_block) +
                    new_clean_entry_size*(block_num % new_entries_per_block));
                new_entry->oid = entry->oid;
                new_entry->version = entry->version;
                if (bitmap)
                    memcpy(new_entry->bitmap, bitmap, 2*new_clean_entry_bitmap_size + new_data_csum_size);
                else
                    memset(new_entry->bitmap, 0xff, 2*new_clean_entry_bitmap_size);
                uint32_t *new_entry_csum = (uint32_t*)(((uint8_t*)new_entry) + new_clean_entry_size - 4);
                *new_entry_csum = crc32c(0, new_entry, new_clean_entry_size - 4);
            }
        },
        true, true
    );
    return r;
}

int disk_tool_t::resize_write_new_meta()
{
    new_meta_fd = open(new_meta_device.c_str(), (options["io"] == "cached" ? 0 : O_DIRECT) | O_RDWR);
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
    return 0;
}

void disk_tool_t::free_new_meta()
{
    if ((uint8_t*)new_meta_hdr != new_meta_buf)
    {
        free(new_meta_hdr);
        new_meta_hdr = NULL;
    }
    if (new_meta_buf)
    {
        free(new_meta_buf);
        new_meta_buf = NULL;
    }
    if (new_journal_buf)
    {
        free(new_journal_buf);
        new_journal_buf = NULL;
    }
}
