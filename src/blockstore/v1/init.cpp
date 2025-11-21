// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "impl.h"
#include "internal.h"

namespace v1 {

#define INIT_META_EMPTY 0
#define INIT_META_READING 1
#define INIT_META_READ_DONE 2
#define INIT_META_WRITING 3

#define GET_SQE() \
    sqe = bs->get_sqe();\
    if (!sqe)\
        throw std::runtime_error("io_uring is full during initialization");\
    data = ((ring_data_t*)sqe->user_data)

static bool iszero(uint64_t *buf, int len)
{
    for (int i = 0; i < len; i++)
        if (buf[i] != 0)
            return false;
    return true;
}

blockstore_init_meta::blockstore_init_meta(blockstore_impl_t *bs)
{
    this->bs = bs;
}

void blockstore_init_meta::handle_event(ring_data_t *data, int buf_num)
{
    if (data->res < 0)
    {
        throw std::runtime_error(
            std::string("read metadata failed at offset ") + std::to_string(buf_num >= 0 ? bufs[buf_num].offset : last_read_offset) +
            std::string(": ") + strerror(-data->res)
        );
    }
    if (buf_num >= 0)
    {
        bufs[buf_num].state = (bufs[buf_num].state == INIT_META_READING
            ? INIT_META_READ_DONE
            : INIT_META_EMPTY);
    }
    submitted--;
    bs->ringloop->wakeup();
}

int blockstore_init_meta::loop()
{
    if (wait_state == 1)      goto resume_1;
    else if (wait_state == 2) goto resume_2;
    else if (wait_state == 3) goto resume_3;
    else if (wait_state == 4) goto resume_4;
    else if (wait_state == 5) goto resume_5;
    else if (wait_state == 6) goto resume_6;
    printf("Reading blockstore metadata\n");
    if (bs->inmemory_meta)
        metadata_buffer = bs->metadata_buffer;
    else
        metadata_buffer = memalign(MEM_ALIGNMENT, 2*bs->metadata_buf_size);
    if (!metadata_buffer)
        throw std::runtime_error("Failed to allocate metadata read buffer");
    // Read superblock
    GET_SQE();
    last_read_offset = 0;
    data->iov = { metadata_buffer, (size_t)bs->dsk.meta_block_size };
    data->callback = [this](ring_data_t *data) { handle_event(data, -1); };
    io_uring_prep_readv(sqe, bs->dsk.meta_fd, &data->iov, 1, bs->dsk.meta_offset);
    bs->ringloop->submit();
    submitted++;
resume_1:
    if (submitted > 0)
    {
        wait_state = 1;
        return 1;
    }
    if (iszero((uint64_t*)metadata_buffer, bs->dsk.meta_block_size / sizeof(uint64_t)))
    {
        {
            blockstore_meta_header_v2_t *hdr = (blockstore_meta_header_v2_t *)metadata_buffer;
            hdr->zero = 0;
            hdr->magic = BLOCKSTORE_META_MAGIC_V1;
            hdr->version = bs->dsk.meta_format;
            hdr->meta_block_size = bs->dsk.meta_block_size;
            hdr->data_block_size = bs->dsk.data_block_size;
            hdr->bitmap_granularity = bs->dsk.bitmap_granularity;
            if (bs->dsk.meta_format >= BLOCKSTORE_META_FORMAT_V2)
            {
                hdr->data_csum_type = bs->dsk.data_csum_type;
                hdr->csum_block_size = bs->dsk.csum_block_size;
                hdr->header_csum = 0;
                hdr->header_csum = crc32c(0, hdr, sizeof(*hdr));
            }
        }
        if (bs->readonly)
        {
            printf("Skipping metadata initialization because blockstore is readonly\n");
        }
        else
        {
            printf("Initializing metadata area\n");
            GET_SQE();
            last_read_offset = 0;
            data->iov = (struct iovec){ metadata_buffer, (size_t)bs->dsk.meta_block_size };
            data->callback = [this](ring_data_t *data) { handle_event(data, -1); };
            io_uring_prep_writev(sqe, bs->dsk.meta_fd, &data->iov, 1, bs->dsk.meta_offset);
            bs->ringloop->submit();
            submitted++;
        resume_3:
            if (submitted > 0)
            {
                wait_state = 3;
                return 1;
            }
            zero_on_init = true;
        }
    }
    else
    {
        blockstore_meta_header_v2_t *hdr = (blockstore_meta_header_v2_t *)metadata_buffer;
        if (hdr->zero != 0 || hdr->magic != BLOCKSTORE_META_MAGIC_V1 || hdr->version < BLOCKSTORE_META_FORMAT_V1)
        {
            printf(
                "Metadata is corrupt or too old (pre-0.6.x).\n"
                " If this is a new OSD, please zero out the metadata area before starting it.\n"
                " If you need to upgrade from 0.5.x, convert metadata with vitastor-disk.\n"
            );
            exit(1);
        }
        if (hdr->version == BLOCKSTORE_META_FORMAT_V2)
        {
            uint32_t csum = hdr->header_csum;
            hdr->header_csum = 0;
            if (crc32c(0, hdr, sizeof(*hdr)) != csum)
            {
                printf("Metadata header is corrupt (checksum mismatch).\n");
                exit(1);
            }
            hdr->header_csum = csum;
            if (bs->dsk.meta_format != BLOCKSTORE_META_FORMAT_V2)
            {
                bs->dsk.meta_format = BLOCKSTORE_META_FORMAT_V2;
                bs->dsk.calc_lengths();
            }
        }
        else if (hdr->version == BLOCKSTORE_META_FORMAT_V1)
        {
            hdr->data_csum_type = 0;
            hdr->csum_block_size = 0;
            hdr->header_csum = 0;
            // Enable compatibility mode - entries without checksums
            if (bs->dsk.meta_format != BLOCKSTORE_META_FORMAT_V1 ||
                bs->dsk.data_csum_type != 0 || bs->dsk.csum_block_size != 0)
            {
                bs->dsk.data_csum_type = 0;
                bs->dsk.csum_block_size = 0;
                bs->dsk.meta_format = BLOCKSTORE_META_FORMAT_V1;
                bs->dsk.calc_lengths();
                printf("Warning: Starting with metadata in the old format without checksums, as stored on disk\n");
            }
        }
        else if (hdr->version == BLOCKSTORE_META_FORMAT_HEAP)
        {
            printf(
                "OSD is started with meta_format %ju, but actual stored version is %ju on disk."
                " Please update the OSD superblock or startup options.\n",
                bs->dsk.meta_format, hdr->version
            );
            exit(1);
        }
        else if (hdr->version > BLOCKSTORE_META_FORMAT_V2)
        {
            printf(
                "Metadata format is too new for me (stored version is %ju, max supported %u).\n",
                hdr->version, BLOCKSTORE_META_FORMAT_V2
            );
            exit(1);
        }
        if (hdr->meta_block_size != bs->dsk.meta_block_size ||
            hdr->data_block_size != bs->dsk.data_block_size ||
            hdr->bitmap_granularity != bs->dsk.bitmap_granularity ||
            hdr->data_csum_type != bs->dsk.data_csum_type ||
            hdr->csum_block_size != bs->dsk.csum_block_size)
        {
            printf(
                "Configuration stored in metadata superblock"
                " (meta_block_size=%u, data_block_size=%u, bitmap_granularity=%u, data_csum_type=%u, csum_block_size=%u)"
                " differs from OSD configuration (%u/%u/%u, %u/%u).\n",
                hdr->meta_block_size, hdr->data_block_size, hdr->bitmap_granularity,
                hdr->data_csum_type, hdr->csum_block_size,
                bs->dsk.meta_block_size, bs->dsk.data_block_size, bs->dsk.bitmap_granularity,
                bs->dsk.data_csum_type, bs->dsk.csum_block_size
            );
            exit(1);
        }
    }
    // Skip superblock
    md_offset = bs->dsk.meta_block_size;
    next_offset = md_offset;
    entries_per_block = bs->dsk.meta_block_size / bs->dsk.clean_entry_size;
    // Read the rest of the metadata
resume_2:
    if (next_offset < bs->dsk.meta_area_size && submitted == 0)
    {
        // Submit one read
        for (int i = 0; i < 2; i++)
        {
            if (!bufs[i].state)
            {
                bufs[i].buf = (uint8_t*)metadata_buffer + (bs->inmemory_meta
                    ? next_offset-md_offset
                    : i*bs->metadata_buf_size);
                bufs[i].offset = next_offset;
                bufs[i].size = bs->dsk.meta_area_size-next_offset > bs->metadata_buf_size
                    ? bs->metadata_buf_size : bs->dsk.meta_area_size-next_offset;
                bufs[i].state = INIT_META_READING;
                submitted++;
                next_offset += bufs[i].size;
                GET_SQE();
                assert(bufs[i].size <= 0x7fffffff);
                data->iov = { bufs[i].buf, (size_t)bufs[i].size };
                data->callback = [this, i](ring_data_t *data) { handle_event(data, i); };
                if (!zero_on_init)
                    io_uring_prep_readv(sqe, bs->dsk.meta_fd, &data->iov, 1, bs->dsk.meta_offset + bufs[i].offset);
                else
                {
                    // Fill metadata with zeroes
                    memset(data->iov.iov_base, 0, data->iov.iov_len);
                    io_uring_prep_writev(sqe, bs->dsk.meta_fd, &data->iov, 1, bs->dsk.meta_offset + bufs[i].offset);
                }
                bs->ringloop->submit();
                break;
            }
        }
    }
    for (int i = 0; i < 2; i++)
    {
        if (bufs[i].state == INIT_META_READ_DONE)
        {
            // Handle result
            bool changed = false;
            for (uint64_t sector = 0; sector < bufs[i].size; sector += bs->dsk.meta_block_size)
            {
                // handle <count> entries
                if (handle_meta_block(bufs[i].buf + sector, entries_per_block,
                    ((bufs[i].offset + sector - md_offset) / bs->dsk.meta_block_size) * entries_per_block))
                    changed = true;
            }
            if (changed && !bs->inmemory_meta && !bs->readonly)
            {
                // write the modified buffer back
                GET_SQE();
                assert(bufs[i].size <= 0x7fffffff);
                data->iov = { bufs[i].buf, (size_t)bufs[i].size };
                data->callback = [this, i](ring_data_t *data) { handle_event(data, i); };
                io_uring_prep_writev(sqe, bs->dsk.meta_fd, &data->iov, 1, bs->dsk.meta_offset + bufs[i].offset);
                bs->ringloop->submit();
                bufs[i].state = INIT_META_WRITING;
                submitted++;
            }
            else
            {
                bufs[i].state = 0;
            }
            bs->ringloop->wakeup();
        }
    }
    if (submitted > 0)
    {
        wait_state = 2;
        return 1;
    }
    if (entries_to_zero.size() && !bs->inmemory_meta && !bs->readonly)
    {
        std::sort(entries_to_zero.begin(), entries_to_zero.end());
        // we have to zero out additional entries
        for (i = 0; i < entries_to_zero.size(); )
        {
            next_offset = entries_to_zero[i]/entries_per_block;
            for (j = i; j < entries_to_zero.size() && entries_to_zero[j]/entries_per_block == next_offset; j++) {}
            GET_SQE();
            last_read_offset = (1+next_offset)*bs->dsk.meta_block_size;
            data->iov = { metadata_buffer, (size_t)bs->dsk.meta_block_size };
            data->callback = [this](ring_data_t *data) { handle_event(data, -1); };
            io_uring_prep_readv(sqe, bs->dsk.meta_fd, &data->iov, 1, bs->dsk.meta_offset + (1+next_offset)*bs->dsk.meta_block_size);
            bs->ringloop->submit();
            submitted++;
resume_5:
            if (submitted > 0)
            {
                wait_state = 5;
                return 1;
            }
            for (; i < j; i++)
            {
                uint64_t pos = (entries_to_zero[i] % entries_per_block);
                memset((uint8_t*)metadata_buffer + pos*bs->dsk.clean_entry_size, 0, bs->dsk.clean_entry_size);
            }
            GET_SQE();
            data->iov = { metadata_buffer, (size_t)bs->dsk.meta_block_size };
            data->callback = [this](ring_data_t *data) { handle_event(data, -1); };
            io_uring_prep_writev(sqe, bs->dsk.meta_fd, &data->iov, 1, bs->dsk.meta_offset + (1+next_offset)*bs->dsk.meta_block_size);
            bs->ringloop->submit();
            submitted++;
resume_6:
            if (submitted > 0)
            {
                wait_state = 6;
                return 1;
            }
        }
        entries_to_zero.clear();
    }
    // metadata read finished
    printf("Metadata entries loaded: %ju, free blocks: %ju / %ju\n", entries_loaded, bs->data_alloc->get_free_count(), bs->dsk.block_count);
    if (!bs->inmemory_meta)
    {
        free(metadata_buffer);
        metadata_buffer = NULL;
    }
    if (zero_on_init && !bs->disable_meta_fsync)
    {
        GET_SQE();
        io_uring_prep_fsync(sqe, bs->dsk.meta_fd, IORING_FSYNC_DATASYNC);
        last_read_offset = 0;
        data->iov = { 0 };
        data->callback = [this](ring_data_t *data) { handle_event(data, -1); };
        submitted++;
        bs->ringloop->submit();
    resume_4:
        if (submitted > 0)
        {
            wait_state = 4;
            return 1;
        }
    }
    return 0;
}

bool blockstore_init_meta::handle_meta_block(uint8_t *buf, uint64_t entries_per_block, uint64_t done_cnt)
{
    bool updated = false;
    uint64_t max_i = entries_per_block;
    if (max_i > bs->dsk.block_count-done_cnt)
        max_i = bs->dsk.block_count-done_cnt;
    for (uint64_t i = 0; i < max_i; i++)
    {
        clean_disk_entry *entry = (clean_disk_entry*)(buf + i*bs->dsk.clean_entry_size);
        if (entry->oid.inode > 0)
        {
            if (bs->dsk.meta_format >= BLOCKSTORE_META_FORMAT_V2)
            {
                // Check entry crc32
                uint32_t *entry_csum = (uint32_t*)((uint8_t*)entry + bs->dsk.clean_entry_size - 4);
                if (*entry_csum != crc32c(0, entry, bs->dsk.clean_entry_size - 4))
                {
                    printf("Metadata entry %ju is corrupt (checksum mismatch: %08x vs %08x), skipping\n", done_cnt+i, *entry_csum, crc32c(0, entry, bs->dsk.clean_entry_size - 4));
                    // zero out the invalid entry, otherwise we'll hit "tried to overwrite non-zero metadata entry" later
                    if (bs->inmemory_meta)
                    {
                        memset(entry, 0, bs->dsk.clean_entry_size);
                    }
                    else
                    {
                        entries_to_zero.push_back(done_cnt+i);
                    }
                    continue;
                }
            }
            if (!bs->inmemory_meta && bs->dsk.clean_entry_bitmap_size)
            {
                memcpy(bs->clean_bitmaps + (done_cnt+i) * 2 * bs->dsk.clean_entry_bitmap_size, &entry->bitmap, 2 * bs->dsk.clean_entry_bitmap_size);
            }
            auto & clean_db = bs->clean_db_shard(entry->oid);
            auto clean_it = clean_db.find(entry->oid);
            if (clean_it == clean_db.end() || clean_it->second.version < entry->version)
            {
                if (clean_it != clean_db.end())
                {
                    // free the previous block
                    // here we have to zero out the previous entry because otherwise we'll hit
                    // "tried to overwrite non-zero metadata entry" later
                    uint64_t old_clean_loc = clean_it->second.location / bs->dsk.data_block_size;
                    if (bs->inmemory_meta)
                    {
                        uint64_t sector = (old_clean_loc / entries_per_block) * bs->dsk.meta_block_size;
                        uint64_t pos = (old_clean_loc % entries_per_block);
                        clean_disk_entry *old_entry = (clean_disk_entry*)((uint8_t*)bs->metadata_buffer + sector + pos*bs->dsk.clean_entry_size);
                        memset(old_entry, 0, bs->dsk.clean_entry_size);
                    }
                    else if (old_clean_loc >= done_cnt)
                    {
                        updated = true;
                        uint64_t sector = ((old_clean_loc - done_cnt) / entries_per_block) * bs->dsk.meta_block_size;
                        uint64_t pos = (old_clean_loc % entries_per_block);
                        clean_disk_entry *old_entry = (clean_disk_entry*)(buf + sector + pos*bs->dsk.clean_entry_size);
                        memset(old_entry, 0, bs->dsk.clean_entry_size);
                    }
                    else
                    {
                        entries_to_zero.push_back(clean_it->second.location / bs->dsk.data_block_size);
                    }
#ifdef BLOCKSTORE_DEBUG
                    printf("Free block %ju from %jx:%jx v%ju (new location is %ju)\n",
                        old_clean_loc,
                        clean_it->first.inode, clean_it->first.stripe, clean_it->second.version,
                        done_cnt+i);
#endif
                    bs->data_alloc->set(old_clean_loc, false);
                }
                else
                {
                    bs->inode_space_stats[entry->oid.inode] += bs->dsk.data_block_size;
                    bs->used_blocks++;
                }
                entries_loaded++;
#ifdef BLOCKSTORE_DEBUG
                printf("Allocate block (clean entry) %ju: %jx:%jx v%ju\n", done_cnt+i, entry->oid.inode, entry->oid.stripe, entry->version);
#endif
                bs->data_alloc->set(done_cnt+i, true);
                clean_db[entry->oid] = (struct clean_entry){
                    .version = entry->version,
                    .location = (done_cnt+i) * bs->dsk.data_block_size,
                };
            }
            else
            {
                // here we also have to zero out the entry
                updated = true;
                memset(entry, 0, bs->dsk.clean_entry_size);
#ifdef BLOCKSTORE_DEBUG
                printf("Old clean entry %ju: %jx:%jx v%ju\n", done_cnt+i, entry->oid.inode, entry->oid.stripe, entry->version);
#endif
            }
        }
    }
    return updated;
}

blockstore_init_journal::blockstore_init_journal(blockstore_impl_t *bs)
{
    this->bs = bs;
    next_free = bs->journal.block_size;
    simple_callback = [this](ring_data_t *data1)
    {
        if (data1->res != data1->iov.iov_len)
        {
            throw std::runtime_error(std::string("I/O operation failed while reading journal: ") + strerror(-data1->res));
        }
        wait_count--;
    };
}

void blockstore_init_journal::handle_event(ring_data_t *data1)
{
    if (data1->res <= 0)
    {
        throw std::runtime_error(
            std::string("read journal failed at offset ") + std::to_string(journal_pos) +
            std::string(": ") + strerror(-data1->res)
        );
    }
    done.push_back({
        .buf = submitted_buf,
        .pos = journal_pos,
        .len = (uint64_t)data1->res,
    });
    journal_pos += data1->res;
    if (journal_pos >= bs->journal.len)
    {
        // Continue from the beginning
        journal_pos = bs->journal.block_size;
        wrapped = true;
    }
    submitted_buf = NULL;
}

int blockstore_init_journal::loop()
{
    if (wait_state == 1)
        goto resume_1;
    else if (wait_state == 2)
        goto resume_2;
    else if (wait_state == 3)
        goto resume_3;
    else if (wait_state == 4)
        goto resume_4;
    else if (wait_state == 5)
        goto resume_5;
    else if (wait_state == 6)
        goto resume_6;
    else if (wait_state == 7)
        goto resume_7;
    printf("Reading blockstore journal\n");
    if (!bs->journal.inmemory)
        submitted_buf = memalign_or_die(MEM_ALIGNMENT, 2*bs->journal.block_size);
    else
        submitted_buf = bs->journal.buffer;
    // Read first block of the journal
    sqe = bs->get_sqe();
    if (!sqe)
        throw std::runtime_error("io_uring is full while trying to read journal");
    data = ((ring_data_t*)sqe->user_data);
    data->iov = { submitted_buf, (size_t)bs->journal.block_size };
    data->callback = simple_callback;
    io_uring_prep_readv(sqe, bs->dsk.journal_fd, &data->iov, 1, bs->journal.offset);
    bs->ringloop->submit();
    wait_count = 1;
resume_1:
    if (wait_count > 0)
    {
        wait_state = 1;
        return 1;
    }
    if (iszero((uint64_t*)submitted_buf, bs->journal.block_size / sizeof(uint64_t)))
    {
        // Journal is empty
        // FIXME handle this wrapping to journal_block_size better (maybe)
        bs->journal.used_start = bs->journal.block_size;
        bs->journal.next_free = bs->journal.block_size;
        // Initialize journal "superblock" and the first block
        memset(submitted_buf, 0, 2*bs->journal.block_size);
        *((journal_entry_start*)submitted_buf) = {
            .crc32 = 0,
            .magic = JOURNAL_MAGIC,
            .type = JE_START,
            .size = sizeof(journal_entry_start),
            .reserved = 0,
            .journal_start = bs->journal.block_size,
            .version = JOURNAL_VERSION_V2,
            .data_csum_type = bs->dsk.data_csum_type,
            .csum_block_size = bs->dsk.csum_block_size,
        };
        ((journal_entry_start*)submitted_buf)->crc32 = je_crc32((journal_entry*)submitted_buf);
        if (bs->readonly)
        {
            printf("Skipping journal initialization because blockstore is readonly\n");
        }
        else
        {
            // Cool effect. Same operations result in journal replay.
            // FIXME: Randomize initial crc32. Track crc32 when trimming.
            printf("Resetting journal\n");
            GET_SQE();
            data->iov = (struct iovec){ submitted_buf, (size_t)(2*bs->journal.block_size) };
            data->callback = simple_callback;
            io_uring_prep_writev(sqe, bs->dsk.journal_fd, &data->iov, 1, bs->journal.offset);
            wait_count++;
            bs->ringloop->submit();
        resume_6:
            if (wait_count > 0)
            {
                wait_state = 6;
                return 1;
            }
            if (!bs->disable_journal_fsync)
            {
                GET_SQE();
                io_uring_prep_fsync(sqe, bs->dsk.journal_fd, IORING_FSYNC_DATASYNC);
                data->iov = { 0 };
                data->callback = simple_callback;
                wait_count++;
                bs->ringloop->submit();
            }
        resume_4:
            if (wait_count > 0)
            {
                wait_state = 4;
                return 1;
            }
        }
        if (!bs->journal.inmemory)
        {
            free(submitted_buf);
        }
    }
    else
    {
        // First block always contains a single JE_START entry
        je_start = (journal_entry_start*)submitted_buf;
        if (je_start->magic != JOURNAL_MAGIC ||
            je_start->type != JE_START ||
            je_crc32((journal_entry*)je_start) != je_start->crc32 ||
            je_start->size != JE_START_V0_SIZE && je_start->size != JE_START_V1_SIZE && je_start->size != JE_START_V2_SIZE)
        {
            // Entry is corrupt
            fprintf(stderr, "First entry of the journal is corrupt or unsupported\n");
            exit(1);
        }
        if (je_start->size == JE_START_V0_SIZE ||
            (je_start->version != JOURNAL_VERSION_V1 || je_start->size != JE_START_V1_SIZE) &&
            (je_start->version != JOURNAL_VERSION_V2 || je_start->size != JE_START_V2_SIZE && je_start->size != JE_START_V1_SIZE))
        {
            fprintf(
                stderr, "The code only supports journal versions 2 and 1, but it is %ju on disk."
                    " Please use vitastor-disk to rewrite the journal\n",
                je_start->size == JE_START_V0_SIZE ? 0 : je_start->version
            );
            exit(1);
        }
        if (je_start->version == JOURNAL_VERSION_V1 ||
            je_start->version == JOURNAL_VERSION_V2 && je_start->size == JE_START_V1_SIZE)
        {
            je_start->data_csum_type = 0;
            je_start->csum_block_size = 0;
        }
        if (je_start->data_csum_type != bs->dsk.data_csum_type ||
            je_start->csum_block_size != bs->dsk.csum_block_size)
        {
            printf(
                "Configuration stored in journal superblock (data_csum_type=%u, csum_block_size=%u)"
                " differs from OSD configuration (%u/%u).\n",
                je_start->data_csum_type, je_start->csum_block_size,
                bs->dsk.data_csum_type, bs->dsk.csum_block_size
            );
            exit(1);
        }
        next_free = journal_pos = bs->journal.used_start = je_start->journal_start;
        if (!bs->journal.inmemory)
            free(submitted_buf);
        submitted_buf = NULL;
        crc32_last = 0;
        // Read journal
        while (1)
        {
        resume_2:
            if (submitted_buf)
            {
                wait_state = 2;
                return 1;
            }
            if (!wrapped || journal_pos < bs->journal.used_start)
            {
                GET_SQE();
                uint64_t end = bs->journal.len;
                if (journal_pos < bs->journal.used_start)
                    end = bs->journal.used_start;
                if (!bs->journal.inmemory)
                    submitted_buf = memalign_or_die(MEM_ALIGNMENT, JOURNAL_BUFFER_SIZE);
                else
                    submitted_buf = (uint8_t*)bs->journal.buffer + journal_pos;
                data->iov = {
                    submitted_buf,
                    (size_t)(end - journal_pos < JOURNAL_BUFFER_SIZE ? end - journal_pos : JOURNAL_BUFFER_SIZE),
                };
                data->callback = [this](ring_data_t *data1) { handle_event(data1); };
                io_uring_prep_readv(sqe, bs->dsk.journal_fd, &data->iov, 1, bs->journal.offset + journal_pos);
                bs->ringloop->submit();
            }
            while (done.size() > 0)
            {
                handle_res = handle_journal_part(done[0].buf, done[0].pos, done[0].len);
                if (handle_res == 0)
                {
                    // journal ended
                    // zero out corrupted entry, if required
                    if (init_write_buf && !bs->readonly)
                    {
                        GET_SQE();
                        data->iov = { init_write_buf, (size_t)bs->journal.block_size };
                        data->callback = simple_callback;
                        io_uring_prep_writev(sqe, bs->dsk.journal_fd, &data->iov, 1, bs->journal.offset + init_write_sector);
                        wait_count++;
                        bs->ringloop->submit();
                    resume_7:
                        if (wait_count > 0)
                        {
                            wait_state = 7;
                            return 1;
                        }
                        if (!bs->disable_journal_fsync)
                        {
                            GET_SQE();
                            data->iov = { 0 };
                            data->callback = simple_callback;
                            io_uring_prep_fsync(sqe, bs->dsk.journal_fd, IORING_FSYNC_DATASYNC);
                            wait_count++;
                            bs->ringloop->submit();
                        }
                    resume_5:
                        if (wait_count > 0)
                        {
                            wait_state = 5;
                            return 1;
                        }
                    }
                    // wait for the next read to complete, then stop
                resume_3:
                    if (submitted_buf)
                    {
                        wait_state = 3;
                        return 1;
                    }
                    // free buffers
                    if (!bs->journal.inmemory)
                        for (auto & e: done)
                            free(e.buf);
                    done.clear();
                    break;
                }
                else if (handle_res == 1)
                {
                    // OK, remove it
                    if (!bs->journal.inmemory)
                    {
                        free(done[0].buf);
                    }
                    done.erase(done.begin());
                }
                else if (handle_res == 2)
                {
                    // Need to wait for more reads
                    break;
                }
            }
            if (!submitted_buf)
            {
                break;
            }
        }
    }
    for (auto ov: double_allocs)
    {
        auto dirty_it = bs->dirty_db.find(ov);
        if (dirty_it != bs->dirty_db.end() &&
            IS_BIG_WRITE(dirty_it->second.state) &&
            dirty_it->second.location == UINT64_MAX)
        {
            printf("Fatal error (bug): %jx:%jx v%ju big_write journal_entry was allocated over another object\n",
                dirty_it->first.oid.inode, dirty_it->first.oid.stripe, dirty_it->first.version);
            exit(1);
        }
    }
    bs->flusher->mark_trim_possible();
    bs->journal.dirty_start = bs->journal.next_free;
    printf(
        "Journal entries loaded: %ju, free journal space: %ju bytes (%08jx..%08jx is used), free blocks: %ju / %ju\n",
        entries_loaded,
        (bs->journal.next_free >= bs->journal.used_start
            ? bs->journal.len-bs->journal.block_size - (bs->journal.next_free-bs->journal.used_start)
            : bs->journal.used_start - bs->journal.next_free),
        bs->journal.used_start, bs->journal.next_free,
        bs->data_alloc->get_free_count(), bs->dsk.block_count
    );
    bs->journal.crc32_last = crc32_last;
    return 0;
}

int blockstore_init_journal::handle_journal_part(void *buf, uint64_t done_pos, uint64_t len)
{
    uint64_t proc_pos, pos;
    if (continue_pos != 0)
    {
        proc_pos = (continue_pos / bs->journal.block_size) * bs->journal.block_size;
        pos = continue_pos % bs->journal.block_size;
        continue_pos = 0;
        goto resume;
    }
    while (next_free >= done_pos && next_free < done_pos+len)
    {
        proc_pos = next_free;
        pos = 0;
        next_free += bs->journal.block_size;
        if (next_free >= bs->journal.len)
        {
            next_free = bs->journal.block_size;
        }
    resume:
        while (pos < bs->journal.block_size)
        {
            auto buf_pos = proc_pos - done_pos + pos;
            journal_entry *je = (journal_entry*)((uint8_t*)buf + buf_pos);
            if (je->magic != JOURNAL_MAGIC || buf_pos+je->size > len || je_crc32(je) != je->crc32 ||
                je->type < JE_MIN || je->type > JE_MAX || started && je->crc32_prev != crc32_last)
            {
                if (pos == 0)
                {
                    // invalid entry in the beginning, this is definitely the end of the journal
                    bs->journal.next_free = proc_pos;
                    return 0;
                }
                else
                {
                    // allow partially filled sectors
                    break;
                }
            }
            if (je->type == JE_SMALL_WRITE || je->type == JE_SMALL_WRITE_INSTANT)
            {
#ifdef BLOCKSTORE_DEBUG
                printf(
                    "je_small_write%s oid=%jx:%jx ver=%ju offset=%u len=%u\n",
                    je->type == JE_SMALL_WRITE_INSTANT ? "_instant" : "",
                    je->small_write.oid.inode, je->small_write.oid.stripe, je->small_write.version,
                    je->small_write.offset, je->small_write.len
                );
#endif
                // oid, version, offset, len
                uint64_t prev_free = next_free;
                if (next_free + je->small_write.len > bs->journal.len)
                {
                    // data continues from the beginning of the journal
                    next_free = bs->journal.block_size;
                }
                uint64_t location = next_free;
                next_free += je->small_write.len;
                if (next_free >= bs->journal.len)
                {
                    next_free = bs->journal.block_size;
                }
                if (location != je->small_write.data_offset)
                {
                    char err[1024];
                    snprintf(err, 1024, "BUG: calculated journal data offset (%08jx) != stored journal data offset (%08jx)", location, je->small_write.data_offset);
                    throw std::runtime_error(err);
                }
                small_write_data.clear();
                if (location >= done_pos && location+je->small_write.len <= done_pos+len)
                {
                    // data is within this buffer
                    small_write_data.push_back((iovec){
                        .iov_base = (uint8_t*)buf + location - done_pos,
                        .iov_len = je->small_write.len,
                    });
                }
                else
                {
                    // this case is even more interesting because we must carry data crc32 check to next buffer(s)
                    uint64_t covered = 0;
                    for (int i = 0; i < done.size(); i++)
                    {
                        if (location+je->small_write.len > done[i].pos &&
                            location < done[i].pos+done[i].len)
                        {
                            uint64_t part_end = (location+je->small_write.len < done[i].pos+done[i].len
                                ? location+je->small_write.len : done[i].pos+done[i].len);
                            uint64_t part_begin = (location < done[i].pos ? done[i].pos : location);
                            covered += part_end - part_begin;
                            small_write_data.push_back((iovec){
                                .iov_base = (uint8_t*)done[i].buf + part_begin - done[i].pos,
                                .iov_len = (size_t)(part_end - part_begin),
                            });
                        }
                    }
                    if (covered < je->small_write.len)
                    {
                        continue_pos = proc_pos+pos;
                        next_free = prev_free;
                        return 2;
                    }
                }
                bool data_csum_valid = true;
                if (!bs->dsk.csum_block_size)
                {
                    uint32_t data_crc32 = 0;
                    for (auto & sd: small_write_data)
                    {
                        data_crc32 = crc32c(data_crc32, sd.iov_base, sd.iov_len);
                    }
                    data_csum_valid = data_crc32 == je->small_write.crc32_data;
                    if (!data_csum_valid)
                    {
                        printf(
                            "Journal entry data is corrupt for small_write%s oid=%jx:%jx ver=%ju offset=%u len=%u - data crc32 %x != %x\n",
                            je->type == JE_SMALL_WRITE_INSTANT ? "_instant" : "",
                            je->small_write.oid.inode, je->small_write.oid.stripe, je->small_write.version,
                            je->small_write.offset, je->small_write.len,
                            data_crc32, je->small_write.crc32_data
                        );
                    }
                }
                else if (je->small_write.len > 0)
                {
                    // FIXME: deduplicate with disk_tool_journal.cpp
                    // like in enqueue_write()
                    uint32_t start = je->small_write.offset / bs->dsk.csum_block_size;
                    uint32_t end = (je->small_write.offset+je->small_write.len-1) / bs->dsk.csum_block_size;
                    uint32_t data_csum_size = (end-start+1) * (bs->dsk.data_csum_type & 0xFF);
                    uint32_t required_size = sizeof(journal_entry_small_write) + bs->dsk.clean_entry_bitmap_size + data_csum_size;
                    if (je->size != required_size)
                    {
                        printf(
                            "Journal entry data has invalid size for small_write%s oid=%jx:%jx ver=%ju offset=%u len=%u - should be %u bytes but is %u bytes\n",
                            je->type == JE_SMALL_WRITE_INSTANT ? "_instant" : "",
                            je->small_write.oid.inode, je->small_write.oid.stripe, je->small_write.version,
                            je->small_write.offset, je->small_write.len,
                            required_size, je->size
                        );
                        data_csum_valid = false;
                    }
                    else
                    {
                        int sd_num = 0;
                        size_t sd_pos = 0;
                        uint32_t *block_csums = (uint32_t*)((uint8_t*)je + sizeof(journal_entry_small_write) + bs->dsk.clean_entry_bitmap_size);
                        for (uint32_t pos = start; pos <= end; pos++, block_csums++)
                        {
                            size_t block_left = (pos == start
                                ? (start == end
                                    ? je->small_write.len
                                    : bs->dsk.csum_block_size - je->small_write.offset%bs->dsk.csum_block_size)
                                : (pos < end
                                    ? bs->dsk.csum_block_size
                                    : (je->small_write.offset + je->small_write.len)%bs->dsk.csum_block_size));
                            if (pos > start && pos == end && block_left == 0)
                            {
                                // full last block
                                block_left = bs->dsk.csum_block_size;
                            }
                            uint32_t block_crc32 = 0;
                            while (block_left > 0)
                            {
                                assert(sd_num < small_write_data.size());
                                if (small_write_data[sd_num].iov_len >= sd_pos+block_left)
                                {
                                    block_crc32 = crc32c(block_crc32, (uint8_t*)small_write_data[sd_num].iov_base+sd_pos, block_left);
                                    sd_pos += block_left;
                                    break;
                                }
                                else
                                {
                                    block_crc32 = crc32c(block_crc32, (uint8_t*)small_write_data[sd_num].iov_base+sd_pos, small_write_data[sd_num].iov_len-sd_pos);
                                    block_left -= (small_write_data[sd_num].iov_len-sd_pos);
                                    sd_pos = 0;
                                    sd_num++;
                                }
                            }
                            if (block_crc32 != *block_csums)
                            {
                                printf(
                                    "Journal entry data is corrupt for small_write%s oid=%jx:%jx ver=%ju offset=%u len=%u - block %u crc32 %x != %x\n",
                                    je->type == JE_SMALL_WRITE_INSTANT ? "_instant" : "",
                                    je->small_write.oid.inode, je->small_write.oid.stripe, je->small_write.version,
                                    je->small_write.offset, je->small_write.len,
                                    pos, block_crc32, *block_csums
                                );
                                data_csum_valid = false;
                                break;
                            }
                        }
                    }
                }
                if (!data_csum_valid)
                {
                    // journal entry is corrupt, stop here
                    // interesting thing is that we must clear the corrupt entry if we're not readonly,
                    // because we don't write next entries in the same journal block
                    memset((uint8_t*)buf + proc_pos - done_pos + pos, 0, bs->journal.block_size - pos);
                    bs->journal.next_free = prev_free;
                    init_write_buf = (uint8_t*)buf + proc_pos - done_pos;
                    init_write_sector = proc_pos;
                    return 0;
                }
                auto & clean_db = bs->clean_db_shard(je->small_write.oid);
                auto clean_it = clean_db.find(je->small_write.oid);
                if (clean_it == clean_db.end() ||
                    clean_it->second.version < je->small_write.version)
                {
                    obj_ver_id ov = {
                        .oid = je->small_write.oid,
                        .version = je->small_write.version,
                    };
                    uint64_t dyn_size = bs->dsk.dirty_dyn_size(je->small_write.offset, je->small_write.len);
                    void *dyn = NULL;
                    void *dyn_from = (uint8_t*)je + sizeof(journal_entry_small_write);
                    if (!bs->alloc_dyn_data)
                    {
                        // Bitmap without checksum is only 4 bytes for 128k objects, save it inline
                        // It can even contain 4 byte bitmap + 4 byte CRC32 for 4 kb writes :)
                        memcpy(&dyn, dyn_from, dyn_size);
                    }
                    else
                    {
                        // FIXME Using large blockstore objects will result in a lot of small
                        // allocations for entry bitmaps. This can only be fixed by using
                        // a patched map with dynamic entry size, but not the btree_map,
                        // because it doesn't keep iterators valid all the time.
                        dyn = malloc_or_die(dyn_size+sizeof(int));
                        *((int*)dyn) = 1;
                        memcpy((uint8_t*)dyn+sizeof(int), dyn_from, dyn_size);
                    }
                    bs->dirty_db.emplace(ov, (dirty_entry){
                        .state = (BS_ST_SMALL_WRITE | BS_ST_SYNCED),
                        .flags = 0,
                        .location = location,
                        .offset = je->small_write.offset,
                        .len = je->small_write.len,
                        .journal_sector = proc_pos,
                        .dyn_data = dyn,
                    });
                    bs->journal.used_sectors[proc_pos]++;
#ifdef BLOCKSTORE_DEBUG
                    printf(
                        "journal offset %08jx is used by %jx:%jx v%ju (%ju refs)\n",
                        proc_pos, ov.oid.inode, ov.oid.stripe, ov.version, bs->journal.used_sectors[proc_pos]
                    );
#endif
                    auto & unstab = bs->unstable_writes[ov.oid];
                    unstab = unstab < ov.version ? ov.version : unstab;
                    if (je->type == JE_SMALL_WRITE_INSTANT)
                    {
                        bs->mark_stable(ov, true);
                    }
                }
            }
            else if (je->type == JE_BIG_WRITE || je->type == JE_BIG_WRITE_INSTANT)
            {
#ifdef BLOCKSTORE_DEBUG
                printf(
                    "je_big_write%s oid=%jx:%jx ver=%ju loc=%ju\n",
                    je->type == JE_BIG_WRITE_INSTANT ? "_instant" : "",
                    je->big_write.oid.inode, je->big_write.oid.stripe, je->big_write.version, je->big_write.location / bs->dsk.data_block_size
                );
#endif
                auto dirty_it = bs->dirty_db.upper_bound((obj_ver_id){
                    .oid = je->big_write.oid,
                    .version = UINT64_MAX,
                });
                if (dirty_it != bs->dirty_db.begin() && bs->dirty_db.size() > 0)
                {
                    dirty_it--;
                    if (dirty_it->first.oid == je->big_write.oid &&
                        dirty_it->first.version >= je->big_write.version &&
                        (dirty_it->second.state & BS_ST_TYPE_MASK) == BS_ST_DELETE)
                    {
                        // It is allowed to overwrite a deleted object with a
                        // version number smaller than deletion version number,
                        // because the presence of a BIG_WRITE entry means that
                        // its data and metadata are already flushed.
                        // We don't know if newer versions are flushed, but
                        // the previous delete definitely is.
                        // So we forget previous dirty entries, but retain the clean one.
                        // This feature is required for writes happening shortly
                        // after deletes.
                        erase_dirty_object(dirty_it);
                    }
                }
                auto & clean_db = bs->clean_db_shard(je->big_write.oid);
                auto clean_it = clean_db.find(je->big_write.oid);
                if (clean_it == clean_db.end() ||
                    clean_it->second.version < je->big_write.version)
                {
                    // oid, version, block
                    obj_ver_id ov = {
                        .oid = je->big_write.oid,
                        .version = je->big_write.version,
                    };
                    uint64_t dyn_size = bs->dsk.dirty_dyn_size(je->big_write.offset, je->big_write.len);
                    void *dyn = NULL;
                    void *dyn_from = (uint8_t*)je + sizeof(journal_entry_big_write);
                    if (!bs->alloc_dyn_data)
                    {
                        // Bitmap without checksum is only 4 bytes for 128k objects, save it inline
                        memcpy(&dyn, dyn_from, dyn_size);
                    }
                    else
                    {
                        // FIXME Using large blockstore objects will result in a lot of small
                        // allocations for entry bitmaps. This can only be fixed by using
                        // a patched map with dynamic entry size, but not the btree_map,
                        // because it doesn't keep iterators valid all the time.
                        dyn = malloc_or_die(dyn_size+sizeof(int));
                        *((int*)dyn) = 1;
                        memcpy((uint8_t*)dyn+sizeof(int), dyn_from, dyn_size);
                    }
                    auto dirty_it = bs->dirty_db.emplace(ov, (dirty_entry){
                        .state = (BS_ST_BIG_WRITE | BS_ST_SYNCED),
                        .flags = 0,
                        .location = je->big_write.location,
                        .offset = je->big_write.offset,
                        .len = je->big_write.len,
                        .journal_sector = proc_pos,
                        .dyn_data = dyn,
                    }).first;
                    if (bs->data_alloc->get(je->big_write.location / bs->dsk.data_block_size))
                    {
                        // This is probably a big_write that's already flushed and freed, but it may
                        // also indicate a bug. So we remember such entries and recheck them afterwards.
                        // If it's not a bug they won't be present after reading the whole journal.
                        dirty_it->second.location = UINT64_MAX;
                        double_allocs.push_back(ov);
                    }
                    else
                    {
#ifdef BLOCKSTORE_DEBUG
                        printf(
                            "Allocate block (journal) %ju: %jx:%jx v%ju\n",
                            je->big_write.location / bs->dsk.data_block_size,
                            ov.oid.inode, ov.oid.stripe, ov.version
                        );
#endif
                        bs->data_alloc->set(je->big_write.location / bs->dsk.data_block_size, true);
                    }
                    bs->journal.used_sectors[proc_pos]++;
#ifdef BLOCKSTORE_DEBUG
                    printf(
                        "journal offset %08jx is used by %jx:%jx v%ju (%ju refs)\n",
                        proc_pos, ov.oid.inode, ov.oid.stripe, ov.version, bs->journal.used_sectors[proc_pos]
                    );
#endif
                    auto & unstab = bs->unstable_writes[ov.oid];
                    unstab = unstab < ov.version ? ov.version : unstab;
                    if (je->type == JE_BIG_WRITE_INSTANT)
                    {
                        bs->mark_stable(ov, true);
                    }
                }
            }
            else if (je->type == JE_STABLE)
            {
#ifdef BLOCKSTORE_DEBUG
                printf("je_stable oid=%jx:%jx ver=%ju\n", je->stable.oid.inode, je->stable.oid.stripe, je->stable.version);
#endif
                // oid, version
                obj_ver_id ov = {
                    .oid = je->stable.oid,
                    .version = je->stable.version,
                };
                bs->mark_stable(ov, true);
            }
            else if (je->type == JE_ROLLBACK)
            {
#ifdef BLOCKSTORE_DEBUG
                printf("je_rollback oid=%jx:%jx ver=%ju\n", je->rollback.oid.inode, je->rollback.oid.stripe, je->rollback.version);
#endif
                // rollback dirty writes of <oid> up to <version>
                obj_ver_id ov = {
                    .oid = je->rollback.oid,
                    .version = je->rollback.version,
                };
                bs->mark_rolled_back(ov);
            }
            else if (je->type == JE_DELETE)
            {
#ifdef BLOCKSTORE_DEBUG
                printf("je_delete oid=%jx:%jx ver=%ju\n", je->del.oid.inode, je->del.oid.stripe, je->del.version);
#endif
                bool dirty_exists = false;
                auto dirty_it = bs->dirty_db.upper_bound((obj_ver_id){
                    .oid = je->del.oid,
                    .version = UINT64_MAX,
                });
                if (dirty_it != bs->dirty_db.begin())
                {
                    dirty_it--;
                    dirty_exists = dirty_it->first.oid == je->del.oid;
                }
                auto & clean_db = bs->clean_db_shard(je->del.oid);
                auto clean_it = clean_db.find(je->del.oid);
                bool clean_exists = (clean_it != clean_db.end() &&
                    clean_it->second.version < je->del.version);
                if (!clean_exists && dirty_exists)
                {
                    // Clean entry doesn't exist. This means that the delete is already flushed.
                    // So we must not flush this object anymore.
                    erase_dirty_object(dirty_it);
                }
                else if (clean_exists || dirty_exists)
                {
                    // oid, version
                    obj_ver_id ov = {
                        .oid = je->del.oid,
                        .version = je->del.version,
                    };
                    bs->dirty_db.emplace(ov, (dirty_entry){
                        .state = (BS_ST_DELETE | BS_ST_SYNCED),
                        .flags = 0,
                        .location = 0,
                        .offset = 0,
                        .len = 0,
                        .journal_sector = proc_pos,
                    });
                    bs->journal.used_sectors[proc_pos]++;
                    // Deletions are treated as immediately stable, because
                    // "2-phase commit" (write->stabilize) isn't sufficient for them anyway
                    bs->mark_stable(ov, true);
                }
                // Ignore delete if neither preceding dirty entries nor the clean one are present
            }
            started = true;
            pos += je->size;
            crc32_last = je->crc32;
            entries_loaded++;
        }
    }
    bs->journal.next_free = next_free;
    return 1;
}

void blockstore_init_journal::erase_dirty_object(blockstore_dirty_db_t::iterator dirty_it)
{
    auto oid = dirty_it->first.oid;
    bool exists = !IS_DELETE(dirty_it->second.state);
    auto dirty_end = dirty_it;
    dirty_end++;
    while (1)
    {
        if (dirty_it == bs->dirty_db.begin())
        {
            break;
        }
        dirty_it--;
        if (dirty_it->first.oid != oid)
        {
            dirty_it++;
            break;
        }
    }
    auto & clean_db = bs->clean_db_shard(oid);
    auto clean_it = clean_db.find(oid);
    uint64_t clean_loc = clean_it != clean_db.end()
        ? clean_it->second.location : UINT64_MAX;
    if (exists && clean_loc == UINT64_MAX)
    {
        auto & sp = bs->inode_space_stats[oid.inode];
        if (sp > bs->dsk.data_block_size)
            sp -= bs->dsk.data_block_size;
        else
            bs->inode_space_stats.erase(oid.inode);
        bs->used_blocks--;
    }
    bs->erase_dirty(dirty_it, dirty_end, clean_loc);
    // Remove it from the flusher's queue, too
    // Otherwise it may end up referring to a small unstable write after reading the rest of the journal
    bs->flusher->remove_flush(oid);
}

} // namespace v1
