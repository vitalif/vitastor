// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <stdexcept>
#include "blockstore_impl.h"
#include "blockstore_internal.h"
#include "str_util.h"
#include "crc32c.h"

#define INIT_META_EMPTY 0
#define INIT_META_READING 1
#define INIT_META_READ_DONE 2

#define GET_SQE() \
    sqe = bs->get_sqe();\
    if (!sqe)\
        throw std::runtime_error("io_uring is full during initialization");\
    data = ((ring_data_t*)sqe->user_data)

struct blockstore_recheck_io_t
{
    bool is_data;
    uint64_t offset;
    uint64_t len;
    uint8_t *buf;
    std::function<void(uint8_t *buf)> cb;

    blockstore_recheck_io_t(bool is_data, uint64_t offset, uint64_t len, std::function<void(uint8_t *buf)>&& cb):
        is_data(is_data), offset(offset), len(len), buf(NULL), cb(std::move(cb))
    {
    }
};

blockstore_init_meta::blockstore_init_meta(blockstore_impl_t *bs)
{
    this->bs = bs;
}

void blockstore_init_meta::handle_event(ring_data_t *data, int buf_num, const char *op)
{
    if (data->res != data->iov.iov_len)
    {
        throw std::runtime_error(strprintf(
            "%s failed at offset %ju: got %s (code %d), but expected %zu",
            op, (buf_num >= 0 ? bufs[buf_num].offset : last_read_offset), strerror(-data->res),
            data->res, data->iov.iov_len
        ));
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
    else if (wait_state == 7) goto resume_7;
    else if (wait_state == 8) goto resume_8;
    else if (wait_state == 9) goto resume_9;
    metadata_buffer = memalign(MEM_ALIGNMENT, 2*bs->metadata_buf_size);
    if (!metadata_buffer)
        throw std::runtime_error("Failed to allocate metadata read buffer");
    // Read metadata superblock
    GET_SQE();
    last_read_offset = 0;
    data->iov = { bs->meta_superblock, (size_t)bs->dsk.meta_block_size };
    data->callback = [this](ring_data_t *data) { handle_event(data, -1, "read metadata header"); };
    io_uring_prep_readv(sqe, bs->dsk.meta_fd, &data->iov, 1, bs->dsk.meta_offset);
    bs->ringloop->submit();
    submitted++;
resume_1:
    if (submitted > 0)
    {
        wait_state = 1;
        return 1;
    }
    if (is_zero((uint64_t*)bs->meta_superblock, bs->dsk.meta_block_size))
    {
        assert(bs->dsk.meta_format == BLOCKSTORE_META_FORMAT_HEAP);
        blockstore_meta_header_v3_t *hdr = (blockstore_meta_header_v3_t *)bs->meta_superblock;
        hdr->zero = 0;
        hdr->magic = BLOCKSTORE_META_MAGIC_V1;
        hdr->version = bs->dsk.meta_format;
        hdr->meta_block_size = bs->dsk.meta_block_size;
        hdr->data_block_size = bs->dsk.data_block_size;
        hdr->bitmap_granularity = bs->dsk.bitmap_granularity;
        hdr->completed_lsn = 0;
        hdr->data_csum_type = bs->dsk.data_csum_type;
        hdr->csum_block_size = bs->dsk.csum_block_size;
        hdr->meta_area_size = bs->dsk.meta_area_size;
        hdr->set_crc32c();
        if (bs->readonly)
        {
            printf("Skipping metadata initialization because blockstore is readonly\n");
        }
        else
        {
            printf("Initializing metadata area\n");
        }
        zero_on_init = true;
    }
    else
    {
        blockstore_meta_header_v3_t *hdr = (blockstore_meta_header_v3_t *)bs->meta_superblock;
        if (hdr->zero != 0 || hdr->magic != BLOCKSTORE_META_MAGIC_V1 || hdr->version < BLOCKSTORE_META_FORMAT_V1)
        {
            printf(
                "Metadata is corrupt or too old (pre-0.6.x).\n"
                " If this is a new OSD, please zero out the metadata area before starting it.\n"
                " If you need to upgrade from 0.5.x, convert metadata with vitastor-disk.\n"
            );
            exit(1);
        }
        if (hdr->version != BLOCKSTORE_META_FORMAT_HEAP)
        {
            printf(
                "OSD is started with meta_format 3, but actually stored format is %ju on disk."
                " Please update the OSD superblock or startup options.\n",
                hdr->version
            );
            exit(1);
        }
        if (hdr->meta_block_size != bs->dsk.meta_block_size ||
            hdr->data_block_size != bs->dsk.data_block_size ||
            hdr->bitmap_granularity != bs->dsk.bitmap_granularity ||
            hdr->data_csum_type != bs->dsk.data_csum_type ||
            hdr->csum_block_size != bs->dsk.csum_block_size ||
            hdr->meta_area_size != bs->dsk.meta_area_size)
        {
            printf(
                "Configuration stored in metadata superblock"
                " (meta_block_size=%u, data_block_size=%u, bitmap_granularity=%u, data_csum_type=%u, csum_block_size=%u, meta_area_size=%ju)"
                " differs from OSD configuration (%ju/%ju/%u, %u/%u, %ju).\n",
                hdr->meta_block_size, hdr->data_block_size, hdr->bitmap_granularity,
                hdr->data_csum_type, hdr->csum_block_size, hdr->meta_area_size,
                bs->dsk.meta_block_size, bs->dsk.data_block_size, bs->dsk.bitmap_granularity,
                bs->dsk.data_csum_type, bs->dsk.csum_block_size, bs->dsk.meta_area_size
            );
            exit(1);
        }
        uint32_t csum = hdr->header_csum;
        hdr->header_csum = 0;
        if (crc32c(0, hdr, sizeof(*hdr)) != csum)
        {
            printf("Metadata header is corrupt (checksum mismatch).\n");
            exit(1);
        }
        hdr->header_csum = csum;
    }
    bs->heap->start_load(((blockstore_meta_header_v3_t *)bs->meta_superblock)->completed_lsn);
    if (bs->dsk.inmemory_journal && !zero_on_init)
    {
        // Read buffer area
        printf("Reading buffered data\n");
        md_offset = 0;
        while (md_offset < bs->dsk.journal_len)
        {
            GET_SQE();
            data->iov = (iovec){
                bs->buffer_area + md_offset,
                (size_t)(bs->dsk.journal_len - md_offset < bs->metadata_buf_size ? bs->dsk.journal_len - md_offset : bs->metadata_buf_size),
            };
            data->callback = [this](ring_data_t *data) { handle_event(data, -1, "read buffer area"); };
            io_uring_prep_readv(sqe, bs->dsk.journal_fd, &data->iov, 1, bs->dsk.journal_offset + md_offset);
            md_offset += data->iov.iov_len;
            submitted++;
            bs->ringloop->submit();
resume_3:
            if (submitted > 0)
            {
                wait_state = 3;
                return 1;
            }
        }
    }
    printf("Reading blockstore heap metadata\n");
    // Skip superblock
    md_offset = bs->dsk.meta_block_size;
    next_offset = md_offset;
    // Read the rest of the metadata
resume_4:
    if (next_offset < bs->dsk.meta_area_size && submitted == 0 && (!zero_on_init || !bs->readonly))
    {
        // Submit one read
        for (int i = 0; i < 2; i++)
        {
            if (!bufs[i].state)
            {
                bufs[i].buf = (uint8_t*)metadata_buffer + i*bs->metadata_buf_size;
                bufs[i].offset = next_offset;
                bufs[i].size = bs->dsk.meta_area_size-next_offset > bs->metadata_buf_size
                    ? bs->metadata_buf_size : bs->dsk.meta_area_size-next_offset;
                bufs[i].state = INIT_META_READING;
                submitted++;
                next_offset += bufs[i].size;
                GET_SQE();
                assert(bufs[i].size <= 0x7fffffff);
                data->iov = { bufs[i].buf, (size_t)bufs[i].size };
                if (!zero_on_init)
                {
                    data->callback = [this, i](ring_data_t *data) { handle_event(data, i, "read metadata"); };
                    io_uring_prep_readv(sqe, bs->dsk.meta_fd, &data->iov, 1, bs->dsk.meta_offset + bufs[i].offset);
                }
                else
                {
                    // Fill metadata with empty block pattern
                    data->callback = [this, i](ring_data_t *data) { handle_event(data, i, "clear metadata"); };
                    memset(bufs[i].buf, 0, bufs[i].size);
                    for (uint64_t o = 0; o < bufs[i].size; o += bs->dsk.meta_block_size)
                        bs->heap->fill_block_empty_space(bufs[i].buf + o, 0);
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
            if (!zero_on_init)
            {
                uint64_t loaded = 0;
                int r = bs->heap->load_blocks(bufs[i].offset-bs->dsk.meta_block_size, bufs[i].size, bufs[i].buf, bs->skip_corrupted_meta_entries, loaded);
                if (r != 0)
                    exit(1);
                entries_loaded += loaded;
            }
            bufs[i].state = 0;
            bs->ringloop->wakeup();
        }
    }
    if (submitted > 0)
    {
        wait_state = 4;
        return 1;
    }
    // metadata read/clear finished
    bs->heap->finish_load();
    printf("Metadata entries loaded: %ju, rechecking unfinished writes and garbage entries\n", entries_loaded);
    // asynchronous recheck
resume_6:
    wait_state = 6;
    bs->heap->recheck_small_writes([this](bool is_data, uint64_t offset, uint64_t len, std::function<void(uint8_t*)> cb)
    {
        if (!len)
        {
            wait_state = 7;
            bs->ringloop->wakeup();
            return;
        }
        recheck_ios.push_back(new blockstore_recheck_io_t(is_data, offset, len, std::move(cb)));
    }, bs->meta_write_recheck_parallelism);
    while (recheck_ios.size())
    {
        blockstore_recheck_io_t* rc = recheck_ios.back();
        sqe = bs->get_sqe();
        if (!sqe)
            break;
        if (!rc->buf)
            rc->buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, rc->len);
        data = ((ring_data_t*)sqe->user_data);
        data->iov = (iovec){ rc->buf, rc->len };
        data->callback = [this, rc](ring_data_t *data)
        {
            if (data->res < 0)
            {
                fprintf(stderr, "Buffer area read failed at offset %ju: %d\n", rc->offset, data->res);
                exit(1);
            }
            rc->cb(rc->buf);
            free(rc->buf);
            delete rc;
        };
        io_uring_prep_readv(sqe, (rc->is_data ? bs->dsk.data_fd : bs->dsk.journal_fd), &data->iov, 1,
            (rc->is_data ? bs->dsk.data_offset : bs->dsk.journal_offset) + rc->offset);
        recheck_ios.pop_back();
    }
    return 1;
resume_7:
    if (bs->heap->finish_recheck() != 0)
    {
        exit(1);
    }
    recheck_mod = bs->heap->get_recheck_modified_blocks();
    if (bs->readonly)
    {
        recheck_mod.clear();
        printf("Actual metadata entries: %ju\n", bs->heap->get_live_entries());
    }
    else
    {
        printf("Actual metadata entries: %ju, clearing garbage in %zu metadata blocks\n", bs->heap->get_live_entries(), recheck_mod.size());
    }
    for (i = 0; i < recheck_mod.size(); i++)
    {
resume_8:
        if (wait_count >= bs->meta_write_recheck_parallelism || !(sqe = bs->get_sqe()))
        {
            bs->ringloop->submit();
            wait_state = 8;
            return 1;
        }
        uint32_t block_num = recheck_mod[i];
        uint64_t block_offset = bs->dsk.meta_offset + (uint64_t)(block_num+1) * bs->dsk.meta_block_size;
        data = ((ring_data_t*)sqe->user_data);
        uint8_t *buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, bs->dsk.meta_block_size);
        bs->heap->get_meta_block(block_num, buf);
        data->iov = { buf, bs->dsk.meta_block_size };
        data->callback = [this, buf, block_offset](ring_data_t *data)
        {
            wait_count--;
            free(buf);
            if (data->res != bs->dsk.meta_block_size)
            {
                throw std::runtime_error(
                    "write metadata failed at offset " + std::to_string(block_offset) + ": " + strerror(-data->res)
                );
            }
        };
        io_uring_prep_writev(sqe, bs->dsk.meta_fd, &data->iov, 1, block_offset);
        wait_count++;
    }
resume_9:
    if (wait_count > 0)
    {
        bs->ringloop->submit();
        wait_state = 9;
        return 1;
    }
    free(metadata_buffer);
    metadata_buffer = NULL;
do_fsync:
    if (!bs->dsk.disable_meta_fsync && !bs->readonly)
    {
        GET_SQE();
        io_uring_prep_fsync(sqe, bs->dsk.meta_fd, IORING_FSYNC_DATASYNC);
        last_read_offset = 0;
        data->iov = { 0 };
        data->callback = [this](ring_data_t *data) { handle_event(data, -1, "fsync metadata"); };
        submitted++;
        bs->ringloop->submit();
    resume_5:
        if (submitted > 0)
        {
            wait_state = 5;
            return 1;
        }
    }
    if (zero_on_init && !header_written && !bs->readonly)
    {
        GET_SQE();
        header_written = true;
        last_read_offset = 0;
        data->iov = (struct iovec){ bs->meta_superblock, (size_t)bs->dsk.meta_block_size };
        data->callback = [this](ring_data_t *data) { handle_event(data, -1, "write metadata header"); };
        io_uring_prep_writev(sqe, bs->dsk.meta_fd, &data->iov, 1, bs->dsk.meta_offset);
        bs->ringloop->submit();
        submitted++;
    resume_2:
        if (submitted > 0)
        {
            wait_state = 2;
            return 1;
        }
        if (!bs->dsk.disable_meta_fsync)
        {
            goto do_fsync;
        }
    }
    printf("Loading finished. Data used: %ju / %ju bytes (%s / %s)\n",
        bs->heap->get_data_used_space(), bs->dsk.block_count * bs->dsk.data_block_size,
        format_size(bs->heap->get_data_used_space()).c_str(),
        format_size(bs->dsk.block_count * bs->dsk.data_block_size).c_str());
    return 0;
}
