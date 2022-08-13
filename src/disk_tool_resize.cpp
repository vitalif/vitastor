// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "disk_tool.h"
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
