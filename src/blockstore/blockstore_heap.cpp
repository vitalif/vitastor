// Metadata storage version 3 ("heap")
// Copyright (c) Vitaliy Filippov, 2025+
// License: VNPL-1.1 (see README.md for details)

#include "blockstore_heap.h"

#include "../util/allocator.h"
#include "../util/crc32c.h"
#include "../util/malloc_or_die.h"

#define BS_HEAP_FREE_MVCC 1
#define BS_HEAP_FREE_MAIN 2
#define FREE_SPACE_BIT 0x8000

#define HEAP_INFLIGHT_DONE 1
#define HEAP_INFLIGHT_COMPACTABLE 2

heap_write_t *heap_write_t::next()
{
    return (next_pos ? (heap_write_t*)((uint8_t*)this + next_pos) : NULL);
}

uint32_t heap_write_t::get_size(blockstore_heap_t *heap)
{
    return (sizeof(heap_write_t) +
        ((flags & BS_HEAP_TYPE) != BS_HEAP_TOMBSTONE
            ? heap->dsk->clean_entry_bitmap_size
            : 0) +
        ((flags & BS_HEAP_TYPE) == BS_HEAP_BIG_WRITE
            ? heap->dsk->clean_entry_bitmap_size
            : 0) +
        get_csum_size(heap));
}

uint32_t heap_write_t::get_csum_size(blockstore_heap_t *heap)
{
    if (!heap->dsk->csum_block_size)
    {
        return ((flags & BS_HEAP_TYPE) == BS_HEAP_SMALL_WRITE || (flags & BS_HEAP_TYPE) == BS_HEAP_INTENT_WRITE ? 4 : 0);
    }
    if ((flags & BS_HEAP_TYPE) == BS_HEAP_TOMBSTONE)
    {
        return 0;
    }
    if ((flags & BS_HEAP_TYPE) == BS_HEAP_BIG_WRITE)
    {
        // We always store full checksums for "big" entries to prevent ENOSPC on compaction
        // when (big_write+small_write) are smaller than (compacted big_write)
        // However, we only use part of it related to offset..offset+len
        return heap->dsk->data_block_size/heap->dsk->csum_block_size * (heap->dsk->data_csum_type & 0xFF);
    }
    return ((offset+len+heap->dsk->csum_block_size-1)/heap->dsk->csum_block_size - offset/heap->dsk->csum_block_size)
        * (heap->dsk->data_csum_type & 0xFF);
}

bool heap_write_t::needs_recheck(blockstore_heap_t *heap)
{
    return len > 0 && lsn > heap->compacted_lsn && (flags == (BS_HEAP_SMALL_WRITE|BS_HEAP_STABLE)
        || flags == BS_HEAP_SMALL_WRITE || flags == (BS_HEAP_INTENT_WRITE|BS_HEAP_STABLE));
}

bool heap_write_t::needs_compact(uint64_t compacted_lsn)
{
    return lsn > compacted_lsn && flags == (BS_HEAP_SMALL_WRITE|BS_HEAP_STABLE);
}

bool heap_write_t::is_compacted(uint64_t compacted_lsn)
{
    return lsn <= compacted_lsn && (flags == (BS_HEAP_SMALL_WRITE|BS_HEAP_STABLE) || flags == (BS_HEAP_INTENT_WRITE|BS_HEAP_STABLE));
}

bool heap_write_t::can_be_collapsed(blockstore_heap_t *heap)
{
    return flags == BS_HEAP_INTENT_WRITE ||
        !heap->dsk->csum_block_size || heap->dsk->csum_block_size == heap->dsk->bitmap_granularity ||
        !(offset % heap->dsk->csum_block_size) && !(len % heap->dsk->csum_block_size);
}

bool heap_write_t::is_allowed_before_compacted(uint64_t compacted_lsn, bool is_last_entry)
{
    return lsn <= compacted_lsn && (is_last_entry
        ? (flags == (BS_HEAP_BIG_WRITE|BS_HEAP_STABLE))
        : (flags == (BS_HEAP_SMALL_WRITE|BS_HEAP_STABLE) || flags == (BS_HEAP_INTENT_WRITE|BS_HEAP_STABLE)));
}

uint8_t *heap_write_t::get_ext_bitmap(blockstore_heap_t *heap)
{
    if ((flags & BS_HEAP_TYPE) == BS_HEAP_TOMBSTONE)
        return NULL;
    return ((uint8_t*)this + sizeof(heap_write_t));
}

uint8_t *heap_write_t::get_int_bitmap(blockstore_heap_t *heap)
{
    if ((flags & BS_HEAP_TYPE) != BS_HEAP_BIG_WRITE)
        return NULL;
    return ((uint8_t*)this + sizeof(heap_write_t) + heap->dsk->clean_entry_bitmap_size);
}

uint8_t *heap_write_t::get_checksums(blockstore_heap_t *heap)
{
    if (!heap->dsk->csum_block_size)
        return NULL;
    if (len && ((flags & BS_HEAP_TYPE) == BS_HEAP_SMALL_WRITE ||
        (flags & BS_HEAP_TYPE) == BS_HEAP_INTENT_WRITE))
        return ((uint8_t*)this + sizeof(heap_write_t) + heap->dsk->clean_entry_bitmap_size);
    if ((flags & BS_HEAP_TYPE) != BS_HEAP_BIG_WRITE)
        return NULL;
    return ((uint8_t*)this + sizeof(heap_write_t) + 2*heap->dsk->clean_entry_bitmap_size);
}

uint32_t *heap_write_t::get_checksum(blockstore_heap_t *heap)
{
    if (heap->dsk->csum_block_size || !len ||
        (flags & BS_HEAP_TYPE) != BS_HEAP_SMALL_WRITE && (flags & BS_HEAP_TYPE) != BS_HEAP_INTENT_WRITE)
        return NULL;
    return (uint32_t*)((uint8_t*)this + sizeof(heap_write_t) + heap->dsk->clean_entry_bitmap_size);
}

heap_write_t *heap_object_t::get_writes()
{
    return (heap_write_t*)((uint8_t*)this + write_pos);
}

uint32_t heap_object_t::calc_crc32c()
{
    uint32_t old_crc32c = crc32c;
    crc32c = 0;
    uint32_t res = ::crc32c(0, (uint8_t*)this, sizeof(this));
    for (heap_write_t *wr = get_writes(); wr; wr = wr->next())
    {
        res = ::crc32c(res, (uint8_t*)wr, wr->size);
    }
    crc32c = old_crc32c;
    return res;
}

uint64_t blockstore_heap_t::get_pg_id(inode_t inode, uint64_t stripe)
{
    uint64_t pg_num = 0;
    uint64_t pool_id = (inode >> (64-POOL_ID_BITS));
    auto sh_it = pool_shard_settings.find(pool_id);
    if (sh_it != pool_shard_settings.end())
    {
        // like map_to_pg()
        pg_num = (stripe / sh_it->second.pg_stripe_size) % sh_it->second.pg_count + 1;
    }
    return ((pool_id << (64-POOL_ID_BITS)) | pg_num);
}

blockstore_heap_t::blockstore_heap_t(blockstore_disk_t *dsk, uint8_t *buffer_area, int log_level):
    dsk(dsk),
    buffer_area(buffer_area),
    log_level(log_level),
    meta_block_count(dsk->meta_area_size/dsk->meta_block_size-1), // first block is the superblock
    target_block_free_space(dsk->meta_block_target_free_space),
    max_write_entry_size(sizeof(heap_write_t) + 2*dsk->clean_entry_bitmap_size +
        (dsk->csum_block_size ? dsk->data_block_size/dsk->csum_block_size*(dsk->data_csum_type & 0xFF) : 4 /*sizeof crc32c*/))
{
    assert(target_block_free_space < dsk->meta_block_size);
    assert(dsk->meta_block_size < 32768);
    assert(sizeof(heap_object_t) < sizeof(heap_write_t));
    for (int i = 0; i < meta_alloc_buckets; i++)
        meta_allocs[i] = new allocator_t(meta_block_count);
    block_info.resize(meta_block_count);
    data_alloc = new allocator_t(dsk->block_count);
    if (!target_block_free_space)
        target_block_free_space = 800;
    buffer_alloc = new multilist_alloc_t(dsk->journal_len / dsk->bitmap_granularity, dsk->data_block_size / dsk->bitmap_granularity - 1);
}

blockstore_heap_t::~blockstore_heap_t()
{
    for (auto & inf: block_info)
    {
        if (inf.data)
        {
            free(inf.data);
        }
    }
    block_info.clear();
    for (auto & mvcc: object_mvcc)
    {
        if (mvcc.second.entry_copy)
        {
            free(mvcc.second.entry_copy);
        }
    }
    object_mvcc.clear();
    for (int i = 0; i < meta_alloc_buckets; i++)
    {
        if (meta_allocs[i])
            delete meta_allocs[i];
    }
    if (data_alloc)
    {
        delete data_alloc;
    }
    if (buffer_alloc)
    {
        delete buffer_alloc;
    }
}

// set initially compacted lsn - should be done before loading
void blockstore_heap_t::set_compacted_lsn(uint64_t compacted_lsn)
{
    assert(!next_lsn || next_lsn >= compacted_lsn);
    this->compacted_lsn = compacted_lsn;
}

uint64_t blockstore_heap_t::get_compacted_lsn()
{
    return compacted_lsn;
}

struct verify_offset_t
{
    uint32_t start;
    uint32_t end;
    uint32_t type;
};

inline bool operator < (const verify_offset_t & a, const verify_offset_t & b)
{
    return a.end < b.end;
}

static uint32_t free_writes(heap_write_t *wr, heap_write_t *to)
{
    uint32_t freed = 0;
    while (wr && wr != to)
    {
        auto next_wr = wr->next();
        uint16_t size = wr->size;
        memset((uint8_t*)wr, 0, size);
        *((uint16_t*)wr) = FREE_SPACE_BIT | size;
        freed += size;
        wr = next_wr;
    }
    return freed;
}

// EASY PEASY LEMON SQUEEZIE
uint64_t blockstore_heap_t::load_blocks(uint64_t disk_offset, uint64_t size, uint8_t *buf)
{
    uint64_t entries_loaded = 0;
    for (uint64_t buf_offset = 0; buf_offset < size; buf_offset += dsk->meta_block_size)
    {
        uint32_t block_num = (disk_offset + buf_offset) / dsk->meta_block_size;
        assert(block_num < block_info.size());
        uint32_t block_offset = 0, used_space = 0;
        std::set<verify_offset_t> offsets_seen;
        while (block_offset < dsk->meta_block_size - 2)
        {
            uint8_t *data = buf + buf_offset + block_offset;
            uint16_t & region_marker = *((uint16_t*)data);
            if (!region_marker)
            {
                // Block or the rest of block is apparently empty
                if (block_offset > 0)
                {
                    region_marker = FREE_SPACE_BIT | (dsk->meta_block_size - block_offset);
                }
                break;
            }
            if (region_marker & FREE_SPACE_BIT)
            {
                // Free space
                block_offset += (region_marker & ~FREE_SPACE_BIT);
                continue;
            }
            if (region_marker > dsk->meta_block_size-block_offset)
            {
                fprintf(stderr, "Warning: Entry is too large in metadata block %u at %u (%u > max %u bytes), skipping the rest of block\n",
                    block_num, block_offset, region_marker, dsk->meta_block_size-block_offset);
                if (fail_on_warn)
                    abort();
                if (block_offset > 0)
                {
                    memset(data, 0, dsk->meta_block_size-block_offset);
                    region_marker = FREE_SPACE_BIT | (dsk->meta_block_size-block_offset);
                }
                break;
            }
            if (region_marker < sizeof(heap_object_t))
            {
                fprintf(stderr, "Warning: Entry is too small in metadata block %u at %u (%u < min %ju bytes), skipping\n",
                    block_num, block_offset, region_marker, sizeof(heap_object_t));
skip_corrupted:
                if (fail_on_warn)
                    abort();
skip_object:
                if (block_offset > 0 && region_marker > 0)
                {
                    if (region_marker >= 2)
                        memset(data+2, 0, region_marker-2);
                    region_marker |= FREE_SPACE_BIT;
                }
                block_offset += (region_marker & ~FREE_SPACE_BIT);
                continue;
            }
            if (region_marker != sizeof(heap_object_t))
            {
                // Write entry
                block_offset += region_marker;
                continue;
            }
            offsets_seen.insert((verify_offset_t){ .start = block_offset, .end = block_offset + region_marker, .type = 1 });
            heap_object_t *obj = (heap_object_t *)data;
            if (!obj->write_pos)
            {
                fprintf(stderr, "Warning: Object %jx:%jx in metadata block %u at %u does not contain writes, skipping\n",
                    obj->inode, obj->stripe, block_num, block_offset);
                goto skip_corrupted;
            }
            // Verify write chain
            if (obj->write_pos < -(int16_t)block_offset || obj->write_pos > (int16_t)(dsk->meta_block_size-block_offset-sizeof(heap_write_t)))
            {
                fprintf(stderr, "Warning: Object %jx:%jx in metadata block %u at %u write offset (%d) exceeds block boundaries, skipping object\n",
                    obj->inode, obj->stripe, block_num, block_offset, obj->write_pos);
                goto skip_corrupted;
            }
            if (obj->write_pos < 0 && obj->write_pos > -sizeof(heap_write_t) ||
                obj->write_pos > 0 && obj->write_pos < sizeof(heap_object_t))
            {
                fprintf(stderr, "Warning: Object %jx:%jx in metadata block %u at %u write offset (%d) intersects the object itself, skipping object\n",
                    obj->inode, obj->stripe, block_num, block_offset, obj->write_pos);
                goto skip_corrupted;
            }
            uint32_t wr_i = 0;
            for (auto wr = obj->get_writes(); wr; wr = wr->next(), wr_i++)
            {
                uint32_t wr_pos = ((uint8_t*)wr - buf - buf_offset);
                if (wr->size != wr->get_size(this))
                {
                    fprintf(stderr, "Warning: Object %jx:%jx in metadata block %u at %u list entry #%u at %u size is invalid: %u instead of %u, skipping object\n",
                        obj->inode, obj->stripe, block_num, block_offset, wr_i, wr_pos, wr->size, wr->get_size(this));
                    goto skip_corrupted;
                }
                auto offset_it = offsets_seen.upper_bound({ .end = wr_pos });
                if (offset_it != offsets_seen.end() && offset_it->start < wr_pos+wr->size)
                {
                    fprintf(stderr, "Warning: Object %jx:%jx in metadata block %u at %u list entry #%u (%u..%u) intersects with other entries (%u..%u) or is double-claimed, skipping object\n",
                        obj->inode, obj->stripe, block_num, block_offset, wr_i, wr_pos, wr_pos+wr->size, offset_it->start, offset_it->end);
                    goto skip_corrupted;
                }
                if (wr->next_pos < -(int16_t)wr_pos || wr->next_pos > (int16_t)(dsk->meta_block_size - wr_pos))
                {
                    fprintf(stderr, "Warning: Object %jx:%jx in metadata block %u at %u list entry #%u at %u next item offset (%d) exceeds block boundaries, skipping object\n",
                        obj->inode, obj->stripe, block_num, block_offset, wr_i, wr_pos, wr->next_pos);
                    goto skip_corrupted;
                }
                offsets_seen.insert({ .start = wr_pos, .end = wr_pos+wr->size, .type = 2 });
            }
            // Check for duplicates
            uint64_t lsn = obj->get_writes()->lsn;
            auto oid = (object_id){ .inode = obj->inode, .stripe = obj->stripe };
            uint32_t dup_block;
            heap_object_t *dup_obj = read_entry(oid, &dup_block);
            if (dup_obj != NULL)
            {
                if (dup_obj->get_writes()->lsn >= lsn)
                {
                    // Object is duplicated on disk
                    fprintf(stderr, "Warning: Object %jx:%jx in metadata block %u at %u is an older duplicate, skipping\n",
                        obj->inode, obj->stripe, block_num, block_offset);
                    goto skip_object;
                }
                else
                {
                    fprintf(stderr, "Warning: Object %jx:%jx in metadata block %u at %u is a newer duplicate, overriding\n",
                        obj->inode, obj->stripe, block_num, block_offset);
                    erase_object(dup_block, dup_obj, 0, false);
                }
            }
            // Verify checksums
            uint32_t expected_crc32c = obj->calc_crc32c();
            if (obj->crc32c != expected_crc32c)
            {
                fprintf(stderr, "Warning: Object %jx:%jx in metadata block %u at %u is corrupt (crc32c mismatch: expected %08x, got %08x), skipping\n",
                    obj->inode, obj->stripe, block_num, block_offset, expected_crc32c, obj->crc32c);
                goto skip_corrupted;
            }
            bool to_recheck = false, to_compact = true;
            heap_write_t *remove_wr = NULL;
            uint32_t remove_i = 0;
            wr_i = 0;
            for (auto wr = obj->get_writes(); wr; wr = wr->next(), wr_i++)
            {
                if (wr->is_compacted(this->compacted_lsn))
                {
                    to_compact = true;
                    continue;
                }
                if ((wr->flags & BS_HEAP_TYPE) == BS_HEAP_SMALL_WRITE &&
                    !is_buffer_area_free(wr->location, wr->len))
                {
                    fprintf(stderr, "Notice: write %jx:%jx v%lu (l%lu) buffered data overlaps with other writes, skipping object\n",
                        obj->inode, obj->stripe, wr->version, wr->lsn);
                    goto skip_object;
                }
                if ((wr->flags & BS_HEAP_TYPE) == BS_HEAP_BIG_WRITE &&
                    is_data_used(wr->location))
                {
                    fprintf(stderr, "Notice: write %jx:%jx v%lu (l%lu) data overlaps with other writes, skipping object\n",
                        obj->inode, obj->stripe, wr->version, wr->lsn);
                    goto skip_object;
                }
                if (wr->needs_recheck(this))
                {
                    if (!buffer_area || (wr->flags & BS_HEAP_TYPE) == BS_HEAP_INTENT_WRITE)
                    {
                        to_recheck = true;
                    }
                    // recheck small write data immediately
                    else if (!calc_checksums(wr, buffer_area + wr->location, false))
                    {
                        // entry is invalid (not fully written before OSD crash) - remove it and all newer (previous) entries too
                        remove_wr = wr;
                        remove_i = wr_i;
                    }
                }
            }
            if (remove_wr)
            {
                if (!remove_wr->next_pos)
                {
                    // Skip the whole object
                    fprintf(stderr, "Notice: the whole object %jx:%jx only has unfinished writes, rolling back\n",
                        obj->inode, obj->stripe);
                    goto skip_object;
                }
                if (log_level > 3)
                {
                    fprintf(stderr, "Notice: %u unfinished writes to %jx:%jx v%jx since lsn %ju, rolling back\n",
                        remove_i+1, obj->inode, obj->stripe, obj->get_writes()->version, remove_wr->lsn);
                }
                auto next_wr = remove_wr->next();
                free_writes(obj->get_writes(), next_wr);
                obj->write_pos = next_wr ? (uint8_t*)next_wr - (uint8_t*)obj : NULL;
                obj->crc32c = obj->calc_crc32c();
            }
            if (to_compact)
            {
                compact_object_to(obj, compacted_lsn, NULL, false);
            }
            // Allocate space
            used_space += obj->size;
            for (auto wr = obj->get_writes(); wr; wr = wr->next())
            {
                used_space += wr->size;
                if ((wr->flags & BS_HEAP_TYPE) == BS_HEAP_SMALL_WRITE)
                {
                    use_buffer_area(obj->inode, wr->location, wr->len);
                }
                else if ((wr->flags & BS_HEAP_TYPE) == BS_HEAP_BIG_WRITE)
                {
                    // Mark data block as used
                    use_data(obj->inode, wr->location);
                }
                if (wr->lsn > this->compacted_lsn)
                {
                    tmp_compact_queue.push_back((tmp_compact_item_t){ .oid = oid, .lsn = wr->lsn, .compact = wr->needs_compact(0) });
                }
            }
            if (lsn > next_lsn)
            {
                next_lsn = lsn;
            }
            if (to_recheck)
            {
                recheck_queue.push_back(oid);
            }
            // btree_map<ui64, ui32> anyway stores std::pair<ui64, ui32>'s of 16 bytes size
            // so we can store block_offset in it too
            block_index[get_pg_id(obj->inode, obj->stripe)][obj->inode][obj->stripe] = (uint32_t)block_num*dsk->meta_block_size + block_offset;
            entries_loaded += wr_i;
            block_offset += obj->size;
        }
        uint8_t *copy = NULL;
        if (block_offset > 0)
        {
            // Do not store free blocks in memory
            copy = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, dsk->meta_block_size);
            memcpy(copy, buf+buf_offset, block_offset);
            memset(copy+block_offset, 0, dsk->meta_block_size-block_offset);
        }
        block_info[block_num] = {
            .used_space = 0,
            .data = copy,
        };
        if (block_offset > 0)
        {
            add_used_space(block_num, used_space);
        }
    }
    return entries_loaded;
}

void blockstore_heap_t::finish_load()
{
    completed_lsn = first_inflight_lsn = next_lsn+1;
    if (!tmp_compact_queue.size())
    {
        return;
    }
    std::sort(tmp_compact_queue.begin(), tmp_compact_queue.end(), [this](const tmp_compact_item_t & a, const tmp_compact_item_t & b)
    {
        return a.lsn < b.lsn;
    });
    first_inflight_lsn = tmp_compact_queue[0].lsn;
    if (compacted_lsn < tmp_compact_queue[0].lsn-1)
    {
        compacted_lsn = tmp_compact_queue[0].lsn-1;
    }
    for (auto & e: tmp_compact_queue)
    {
        push_inflight_lsn(e.oid, e.lsn, HEAP_INFLIGHT_DONE | (e.compact ? HEAP_INFLIGHT_COMPACTABLE : 0));
    }
    if (compacted_lsn+1-first_inflight_lsn < inflight_lsn.size())
    {
        auto it = inflight_lsn.begin() + (compacted_lsn+1-first_inflight_lsn);
        while (it != inflight_lsn.end() && !(it->flags & HEAP_INFLIGHT_COMPACTABLE))
        {
            compacted_lsn++;
            it++;
        }
    }
    tmp_compact_queue.clear();
}

bool blockstore_heap_t::calc_checksums(heap_write_t *wr, uint8_t *data, bool set)
{
    if (!dsk->csum_block_size)
    {
        if ((wr->flags & BS_HEAP_TYPE) != BS_HEAP_SMALL_WRITE &&
            (wr->flags & BS_HEAP_TYPE) != BS_HEAP_INTENT_WRITE)
        {
            return true;
        }
        // Single checksum
        uint32_t *wr_csum = wr->get_checksum(this);
        uint32_t real_csum = crc32c(0, data, wr->len);
        if (set)
        {
            *wr_csum = real_csum;
            return true;
        }
        return ((*wr_csum) == real_csum);
    }
    uint32_t offset = ((wr->flags & BS_HEAP_TYPE) == BS_HEAP_BIG_WRITE
        ? (wr->offset / dsk->csum_block_size) * (dsk->data_csum_type & 0xFF) : 0);
    return calc_block_checksums((uint32_t*)(wr->get_checksums(this) + offset), data, wr->get_int_bitmap(this),
        wr->offset, wr->offset+wr->len, set, NULL);
}

bool blockstore_heap_t::calc_block_checksums(uint32_t *block_csums, uint8_t *data, uint8_t *bitmap, uint32_t start, uint32_t end,
    bool set, std::function<void(uint32_t, uint32_t, uint32_t)> bad_block_cb)
{
    bool res = true;
    uint32_t pos = start;
    uint32_t block_end = (start/dsk->csum_block_size + 1)*dsk->csum_block_size;
    uint32_t block_crc = 0;
    while (pos < end)
    {
        if (bitmap)
        {
            while (pos < end && pos < block_end)
            {
                if (!bitmap[pos/dsk->bitmap_granularity/8] & (1 << ((pos/dsk->bitmap_granularity) % 8)))
                    block_crc = crc32c_pad(block_crc, NULL, 0, dsk->bitmap_granularity, 0);
                else
                    block_crc = crc32c(block_crc, data, dsk->bitmap_granularity);
                data += dsk->bitmap_granularity;
                pos += dsk->bitmap_granularity;
            }
        }
        else
        {
            block_crc = crc32c(block_crc, data, (end > block_end ? block_end : end) - pos);
            pos = (end > block_end ? block_end : end);
        }
        if (set)
        {
            *block_csums = block_crc;
        }
        else if (block_crc != *block_csums)
        {
            if (bad_block_cb)
            {
                bad_block_cb(pos-start, *block_csums, block_crc);
                res = false;
            }
            else
                return false;
        }
        block_csums++;
    }
    return res;
}

bool blockstore_heap_t::recheck_small_writes(std::function<void(bool is_data, uint64_t offset, uint64_t len, uint8_t* buf, std::function<void()>)> read_buffer, int queue_depth)
{
    if (in_recheck)
    {
        // Recheck already entered
        return false;
    }
    if (read_buffer)
    {
        recheck_cb = read_buffer;
        recheck_queue_depth = queue_depth;
    }
    in_recheck = true;
    while (recheck_queue.size() > 0 && recheck_in_progress < recheck_queue_depth)
    {
        object_id oid = recheck_queue.front();
        recheck_queue.pop_front();
        heap_object_t *obj = read_entry(oid, NULL);
        assert(obj);
        for (auto wr = obj->get_writes(); wr; wr = wr->next())
        {
            if (wr->needs_recheck(this))
            {
                bool is_intent = (wr->flags == (BS_HEAP_INTENT_WRITE|BS_HEAP_STABLE));
                uint64_t loc = wr->location;
                if (is_intent)
                {
                    auto next_wr = wr->next();
                    assert(next_wr && next_wr->flags == (BS_HEAP_BIG_WRITE|BS_HEAP_STABLE));
                    loc = wr->offset + next_wr->location;
                }
                recheck_in_progress++;
                uint8_t *buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, wr->len);
                if (log_level > 5)
                {
                    fprintf(stderr, "Notice: rechecking %u bytes at %ju in %s area (lsn %lu)\n", wr->len, loc, is_intent ? "data" : "buffer", wr->lsn);
                }
                recheck_cb(is_intent, loc, wr->len, buf, [this, oid, lsn = wr->lsn, buf]()
                {
                    uint32_t block_num = 0;
                    heap_object_t *obj = read_entry(oid, &block_num);
                    if (obj)
                    {
                        heap_write_t *wr;
                        int wr_i = 0;
                        for (wr = obj->get_writes(); wr && wr->lsn != lsn; wr = wr->next())
                        {
                            wr_i++;
                        }
                        if (wr && !calc_checksums(wr, buf, false))
                        {
                            // Erase all writes to the object from this one to the newest
                            if (!wr->next_pos)
                            {
                                fprintf(stderr, "Notice: the whole object %jx:%jx only has unfinished writes, rolling back\n",
                                    obj->inode, obj->stripe);
                                erase_object(block_num, obj, 0, false);
                            }
                            else
                            {
                                if (log_level > 3)
                                {
                                    fprintf(stderr, "Notice: %u unfinished writes to %jx:%jx v%jx since lsn %ju, rolling back\n",
                                        wr_i+1, obj->inode, obj->stripe, wr->version, wr->lsn);
                                }
                                auto next_wr = wr->next();
                                uint32_t freed = free_writes(obj->get_writes(), next_wr);
                                obj->write_pos = next_wr ? (uint8_t*)next_wr - (uint8_t*)obj : NULL;
                                obj->crc32c = obj->calc_crc32c();
                                add_used_space(block_num, -freed);
                            }
                        }
                    }
                    free(buf);
                    recheck_in_progress--;
                    recheck_small_writes(NULL, 0);
                });
            }
        }
    }
    in_recheck = false;
    if (!recheck_queue.size() && !recheck_in_progress)
    {
        auto cb = std::move(recheck_cb);
        recheck_queue_depth = 0;
        if (cb)
        {
            cb(false, 0, 0, NULL, NULL);
        }
        return true;
    }
    return false;
}

void blockstore_heap_t::reshard(pool_id_t pool, uint32_t pg_count, uint32_t pg_stripe_size)
{
    auto & pool_settings = pool_shard_settings[pool];
    if (pool_settings.pg_count == pg_count && pool_settings.pg_stripe_size == pg_stripe_size)
    {
        return;
    }
    uint64_t pool_id = (uint64_t)pool;
    std::map<uint64_t, std::map<inode_t, btree::btree_map<uint64_t, uint64_t>>> new_shards;
    auto sh_it = block_index.lower_bound((pool_id << (64-POOL_ID_BITS)));
    while (sh_it != block_index.end() && (sh_it->first >> (64-POOL_ID_BITS)) == pool_id)
    {
        for (auto & inode_pair: sh_it->second)
        {
            inode_t inode = inode_pair.first;
            for (auto & pair: inode_pair.second)
            {
                // like map_to_pg()
                uint64_t pg_num = (pair.first / pg_stripe_size) % pg_count + 1;
                uint64_t shard_id = (pool_id << (64-POOL_ID_BITS)) | pg_num;
                new_shards[shard_id][inode][pair.first] = std::move(pair.second);
            }
        }
        block_index.erase(sh_it++);
    }
    for (sh_it = new_shards.begin(); sh_it != new_shards.end(); sh_it++)
    {
        auto & to = block_index[sh_it->first];
        to.swap(sh_it->second);
    }
    pool_settings = (pool_shard_settings_t){
        .pg_count = pg_count,
        .pg_stripe_size = pg_stripe_size,
    };
}

heap_object_t *blockstore_heap_t::lock_and_read_entry(object_id oid, uint64_t & copy_id)
{
    auto obj = read_entry(oid, NULL);
    if (!obj)
    {
        return NULL;
    }
    auto mvcc_it = object_mvcc.lower_bound({ .oid = { .inode = oid.inode, .stripe = oid.stripe+1 }, .lsn = 0 });
    copy_id = 1;
    if (mvcc_it != object_mvcc.begin())
    {
        mvcc_it--;
        if (mvcc_it->first.oid == oid)
        {
            if (mvcc_it->second.entry_copy)
            {
                // Already modified, need to create another copy
                copy_id = mvcc_it->first.lsn+1;
            }
            else
            {
                copy_id = mvcc_it->first.lsn;
                mvcc_it->second.readers++;
                return obj;
            }
        }
    }
    object_mvcc[(heap_object_lsn_t){ .oid = oid, .lsn = copy_id }] = (heap_object_mvcc_t){ .readers = 1 };
    return obj;
}

heap_object_t *blockstore_heap_t::read_locked_entry(object_id oid, uint64_t copy_id)
{
    auto mvcc_it = object_mvcc.find((heap_object_lsn_t){ .oid = oid, .lsn = copy_id });
    if (mvcc_it == object_mvcc.end())
    {
        return NULL;
    }
    if (mvcc_it->second.entry_copy)
    {
        return mvcc_it->second.entry_copy;
    }
    return read_entry(oid, NULL);
}

bool blockstore_heap_t::unlock_entry(object_id oid, uint64_t copy_id)
{
    auto mvcc_it = object_mvcc.find((heap_object_lsn_t){ .oid = oid, .lsn = copy_id });
    if (mvcc_it == object_mvcc.end())
    {
        return false;
    }
    mvcc_it->second.readers--;
    if (!mvcc_it->second.readers)
    {
        if (mvcc_it->second.entry_copy)
        {
            // Free refcounted data & buffer blocks
            heap_object_t *obj = (heap_object_t*)mvcc_it->second.entry_copy;
            free_object_space(obj->inode, obj->get_writes(), NULL, BS_HEAP_FREE_MVCC);
            bool is_last_mvcc = true;
            if (mvcc_it != object_mvcc.end())
            {
                // object_mvcc may contain multiple copied entries, but always in a whole sequence
                auto next_it = std::next(mvcc_it);
                if (next_it->first.oid == oid && next_it->second.entry_copy)
                    is_last_mvcc = false;
            }
            if (is_last_mvcc && mvcc_it != object_mvcc.begin())
            {
                auto prev_it = std::prev(mvcc_it);
                if (prev_it->first.oid == oid && prev_it->second.entry_copy)
                    is_last_mvcc = false;
            }
            if (is_last_mvcc)
            {
                // Free data references from the newest object version when the last MVCC is freed
                heap_object_t *new_obj = read_entry(oid, NULL);
                if (new_obj)
                {
                    free_object_space(new_obj->inode, new_obj->get_writes(), NULL, BS_HEAP_FREE_MAIN);
                }
            }
            free(mvcc_it->second.entry_copy);
        }
        object_mvcc.erase(mvcc_it);
    }
    return true;
}

heap_object_t *blockstore_heap_t::read_entry(object_id oid, uint32_t *block_num_ptr, bool for_update)
{
    auto pool_pg_id = get_pg_id(oid.inode, oid.stripe);
    auto & pg_index = block_index[pool_pg_id];
    auto inode_it = pg_index.find(oid.inode);
    if (inode_it == pg_index.end())
    {
        return NULL;
    }
    auto stripe_it = inode_it->second.find(oid.stripe);
    if (stripe_it == inode_it->second.end())
    {
        return NULL;
    }
    uint64_t block_pos = stripe_it->second;
    uint32_t block_num = block_pos / dsk->meta_block_size;
    assert(block_info[block_num].data != NULL);
    heap_object_t *obj = (heap_object_t*)(block_info[block_num].data + (block_pos % dsk->meta_block_size));
    assert(obj->inode == oid.inode && obj->stripe == oid.stripe);
    if (block_num_ptr)
    {
        *block_num_ptr = block_num;
    }
    if (for_update)
    {
        mvcc_save_copy(obj);
    }
    return obj;
}

void blockstore_heap_t::get_compact_range(heap_object_t *obj, uint64_t max_lsn, heap_write_t **begin_wr, heap_write_t **end_wr)
{
    *begin_wr = NULL;
    *end_wr = NULL;
    for (auto wr = obj->get_writes(); wr; wr = wr->next())
    {
        if (wr->is_compacted(max_lsn))
        {
            *begin_wr = wr;
            *end_wr = wr;
        }
        else if (*begin_wr)
        {
            bool is_last = !wr->next();
            if (is_last)
            {
                *end_wr = wr;
            }
            // all subsequent small write entries must also be compacted
            assert(wr->is_allowed_before_compacted(UINT64_MAX, is_last));
        }
    }
}

uint32_t blockstore_heap_t::compact_object_to(heap_object_t *obj, uint64_t compact_lsn, uint8_t *new_csums, bool do_free)
{
    const int cap = dsk->meta_block_size/sizeof(heap_write_t);
    heap_write_t *compacted_wrs[cap];
    int compacted_wr_count = 0;
    bool skip_csums = false;
    heap_write_t *big_wr = NULL, *pre_wr = NULL;
    for (auto wr = obj->get_writes(); wr; wr = wr->next())
    {
        if (wr->is_compacted(compact_lsn))
        {
            compacted_wrs[compacted_wr_count++] = wr;
        }
        if (compacted_wr_count)
        {
            bool is_last = !wr->next();
            if ((wr->flags & BS_HEAP_TYPE) == BS_HEAP_BIG_WRITE)
            {
                big_wr = wr;
            }
            // all subsequent small write entries must also be compacted
            assert(compacted_wr_count == 1 || wr->is_allowed_before_compacted(compact_lsn, is_last));
            if (!new_csums && !wr->can_be_collapsed(this))
            {
                skip_csums = true;
            }
        }
        else
        {
            pre_wr = wr;
        }
    }
    if (compacted_wr_count == 0)
    {
        return 0;
    }
    if (do_free)
    {
        free_object_space(obj->inode, compacted_wrs[0], big_wr);
    }
    // Collapse compacted_wrs[] into big_wr
    big_wr->lsn = compacted_wrs[0]->lsn;
    big_wr->version = compacted_wrs[0]->version;
    big_wr->len += big_wr->offset;
    memcpy(big_wr->get_ext_bitmap(this), compacted_wrs[0]->get_ext_bitmap(this), dsk->clean_entry_bitmap_size);
    for (int i = 0; i < compacted_wr_count; i++)
    {
        auto cur_wr = compacted_wrs[i];
        if (big_wr->offset > cur_wr->offset)
            big_wr->offset = cur_wr->offset;
        if (big_wr->len < cur_wr->offset+cur_wr->len)
            big_wr->len = cur_wr->offset+cur_wr->len;
    }
    big_wr->len -= big_wr->offset;
    uint8_t *int_bmp = big_wr->get_int_bitmap(this);
    uint8_t *csums = big_wr->get_checksums(this);
    const uint32_t csum_size = (dsk->data_csum_type & 0xFF);
    for (int i = compacted_wr_count-1; i >= 0; i--)
    {
        auto cur_wr = compacted_wrs[i];
        bitmap_set(int_bmp, cur_wr->offset, cur_wr->len, dsk->bitmap_granularity);
        // copy checksums
        if (csums && !skip_csums && !new_csums)
        {
            assert(i == compacted_wr_count-1 ||
                (cur_wr->offset % dsk->csum_block_size) == 0 &&
                (cur_wr->len % dsk->csum_block_size) == 0);
            memcpy(csums + cur_wr->offset/dsk->csum_block_size*csum_size,
                cur_wr->get_checksums(this), cur_wr->len/dsk->csum_block_size*csum_size);
        }
    }
    if (csums && new_csums)
    {
        memcpy(csums, new_csums, big_wr->get_csum_size(this));
    }
    // Remove collapsed writes
    uint32_t freed = free_writes(compacted_wrs[0], big_wr);
    if (pre_wr)
    {
        pre_wr->next_pos = (uint8_t*)big_wr - (uint8_t*)pre_wr;
    }
    else
    {
        obj->write_pos = (uint8_t*)big_wr - (uint8_t*)obj;
    }
    obj->crc32c = obj->calc_crc32c();
    return freed;
}

void blockstore_heap_t::compact_block(uint32_t block_num)
{
    auto & inf = block_info[block_num];
    assert(inf.data);
    uint8_t *new_data = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, dsk->meta_block_size);
    const uint8_t *end = inf.data+dsk->meta_block_size;
    const uint8_t *new_end = new_data+dsk->meta_block_size;
    uint8_t *old = inf.data;
    uint8_t *cur = new_data;
    while (old < end)
    {
        uint16_t region_marker = *((uint16_t*)old);
        if (region_marker & FREE_SPACE_BIT)
        {
            // free space
            old += (region_marker & ~FREE_SPACE_BIT);
            continue;
        }
        if (region_marker != sizeof(heap_object_t))
        {
            // heap_write_t, skip
            old += region_marker;
            continue;
        }
        // object header
        heap_object_t *obj = (heap_object_t *)old;
        heap_object_t *new_obj = (heap_object_t *)cur;
        memcpy(cur, obj, sizeof(heap_object_t));
        new_obj->write_pos = sizeof(heap_object_t);
        cur += sizeof(heap_object_t);
        for (auto wr = obj->get_writes(); wr; wr = wr->next())
        {
            assert(cur <= new_end-wr->size);
            memcpy(cur, wr, wr->size);
            auto new_wr = (heap_write_t*)cur;
            new_wr->next_pos = new_wr->next_pos ? new_wr->size : 0;
            cur += wr->size;
        }
        new_obj->crc32c = new_obj->calc_crc32c();
        block_index[get_pg_id(obj->inode, obj->stripe)][obj->inode][obj->stripe] = (uint64_t)block_num*dsk->meta_block_size + ((uint8_t*)new_obj - new_data);
        old += region_marker;
    }
    if (cur != new_data+dsk->meta_block_size)
    {
        assert(cur <= new_data+dsk->meta_block_size-2);
        *((uint16_t*)cur) = FREE_SPACE_BIT | (dsk->meta_block_size-(cur-new_data));
        memset(cur+2, 0, dsk->meta_block_size-(cur-new_data)-2);
    }
    free(inf.data);
    inf.data = new_data;
    inf.free_pos = cur-new_data;
    assert(inf.used_space == (cur-new_data));
}

int blockstore_heap_t::get_block_for_new_object(uint32_t & out_block_num)
{
    for (int i = 0; i < meta_alloc_buckets; i++)
    {
        uint64_t block_num = meta_allocs[i]->find_free();
        if (block_num < block_info.size())
        {
            out_block_num = block_num;
            return 0;
        }
    }
    return ENOSPC;
}

uint32_t blockstore_heap_t::find_block_run(heap_block_info_t & inf, uint32_t space)
{
    uint8_t *data = inf.data + inf.free_pos;
    uint8_t *end = inf.data + dsk->meta_block_size;
    uint8_t *last_free = NULL;
    while (data < end)
    {
        uint16_t region_marker = *((uint16_t*)data);
        assert(region_marker);
        if (region_marker & FREE_SPACE_BIT)
        {
            if (!last_free)
            {
                last_free = data;
            }
            else
            {
                // Merge free regions
                *((uint16_t*)last_free) += (region_marker & ~FREE_SPACE_BIT);
                *((uint16_t*)data) = 0;
            }
            uint16_t region_size = *((uint16_t*)last_free) & ~FREE_SPACE_BIT;
            if (region_size == space)
            {
                inf.free_pos = last_free-inf.data+space;
                return last_free-inf.data;
            }
            else if (region_size >= space+2)
            {
                inf.free_pos = last_free-inf.data+space;
                uint16_t *next_marker = (uint16_t*)(last_free+space);
                *next_marker = FREE_SPACE_BIT | (region_size-space);
                return last_free-inf.data;
            }
        }
        else
        {
            last_free = NULL;
        }
        data += (region_marker & ~FREE_SPACE_BIT);
    }
    return UINT32_MAX;
}

uint32_t blockstore_heap_t::find_block_space(uint32_t block_num, uint32_t space)
{
    auto & inf = block_info.at(block_num);
    uint32_t free_pos = inf.free_pos;
    uint32_t res = find_block_run(inf, space);
    if (res != UINT32_MAX)
    {
        return res;
    }
    if (free_pos != 0)
    {
        inf.free_pos = 0;
        res = find_block_run(inf, space);
        if (res != UINT32_MAX)
        {
            return res;
        }
    }
    compact_block(block_num);
    return find_block_run(inf, space);
}

int blockstore_heap_t::add_object(object_id oid, heap_write_t *wr, uint32_t *modified_block)
{
    // By now, initial small_writes are not allowed
    if ((wr->flags & BS_HEAP_TYPE) != BS_HEAP_BIG_WRITE &&
        (wr->flags & BS_HEAP_TYPE) != BS_HEAP_TOMBSTONE)
    {
        return EINVAL;
    }
    if (!wr->version)
    {
        wr->version = 1;
    }
    const uint32_t wr_size = wr->get_size(this);
    // Allocate block
    uint32_t block_num = 0;
    int res = get_block_for_new_object(block_num);
    if (res != 0)
    {
        return res;
    }
    auto & inf = block_info.at(block_num);
    if (!inf.data)
    {
        inf.data = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, dsk->meta_block_size);
        memset(inf.data, 0, dsk->meta_block_size);
        *((uint16_t*)inf.data) = FREE_SPACE_BIT | dsk->meta_block_size;
    }
    if (modified_block)
    {
        *modified_block = block_num;
    }
    const uint32_t offset = find_block_space(block_num, sizeof(heap_object_t)+wr_size);
    if (offset == UINT32_MAX)
    {
        return ENOSPC;
    }
    block_index[get_pg_id(oid.inode, oid.stripe)][oid.inode][oid.stripe] = (uint64_t)block_num*dsk->meta_block_size + offset;
    // and just append the object entry
    heap_object_t *new_entry = (heap_object_t *)(inf.data + offset);
    new_entry->write_pos = sizeof(heap_object_t);
    new_entry->inode = oid.inode;
    new_entry->stripe = oid.stripe;
    heap_write_t *new_wr = new_entry->get_writes();
    memcpy(new_wr, wr, wr_size);
    new_wr->next_pos = 0;
    new_wr->size = wr_size;
    new_wr->lsn = ++next_lsn;
    wr->lsn = new_wr->lsn;
    push_inflight_lsn(oid, new_wr->lsn, new_wr->needs_compact(0) ? HEAP_INFLIGHT_COMPACTABLE : 0);
    if ((wr->flags & BS_HEAP_TYPE) == BS_HEAP_BIG_WRITE)
    {
        uint8_t *int_bitmap = new_wr->get_int_bitmap(this);
        memset(int_bitmap, 0, dsk->clean_entry_bitmap_size);
        bitmap_set(int_bitmap, wr->offset, wr->len, dsk->bitmap_granularity);
    }
    new_entry->size = sizeof(heap_object_t);
    new_entry->crc32c = new_entry->calc_crc32c();
    add_used_space(block_num, sizeof(heap_object_t) + wr_size);
    return 0;
}

bool blockstore_heap_t::mvcc_check_tracking(object_id oid)
{
    auto mvcc_it = object_mvcc.lower_bound({ .oid = { .inode = oid.inode, .stripe = oid.stripe }, .lsn = 0 });
    if (mvcc_it == object_mvcc.end())
    {
        // no copies
        return false;
    }
    return (mvcc_it->first.oid == oid && mvcc_it->second.entry_copy);
}

// returns tracking_active, i.e. true if there exists at least one copied MVCC version of the object
// it's used for reference tracking because tracking_active=false means that there is 1 implicit reference
// for heap_writes of the current version of the object and true means that there isn't
bool blockstore_heap_t::mvcc_save_copy(heap_object_t *obj)
{
    auto oid = (object_id){ .inode = obj->inode, .stripe = obj->stripe };
    auto mvcc_it = object_mvcc.lower_bound({ .oid = { .inode = oid.inode, .stripe = oid.stripe+1 }, .lsn = 0 });
    if (mvcc_it == object_mvcc.begin())
    {
        // no copies
        return false;
    }
    mvcc_it--;
    if (mvcc_it->first.oid != oid)
    {
        // no copies :-)
        return false;
    }
    if (mvcc_it->second.entry_copy)
    {
        // active current copy :-)
        return true;
    }
    assert(obj->size == sizeof(heap_object_t));
    uint32_t total_size = obj->size;
    for (auto wr = obj->get_writes(); wr; wr = wr->next())
    {
        total_size += wr->size;
    }
    heap_object_t *obj_copy = (heap_object_t*)malloc_or_die(total_size);
    memcpy(obj_copy, obj, sizeof(heap_object_t));
    mvcc_it->second.entry_copy = obj_copy;
    total_size = obj->size;
    obj_copy->write_pos = obj->size;
    for (auto wr = obj->get_writes(); wr; wr = wr->next())
    {
        auto new_wr = (heap_write_t*)((uint8_t*)obj_copy + total_size);
        memcpy((uint8_t*)new_wr, wr, wr->size);
        new_wr->next_pos = wr->next_pos ? wr->size : 0;
        total_size += wr->size;
    }
    uint32_t add_ref = 1;
    bool for_obj = false;
    // save_copy is performed when the object is modified, so object_mvcc may only
    // contain 1 version with entry_copy == NULL
    if (mvcc_it == object_mvcc.begin() || std::prev(mvcc_it)->first.oid != oid)
    {
        // Init refcounts for the copy and for the object itself, when it's the first MVCC entry
        add_ref = 2;
        for_obj = true;
    }
    for (auto wr = obj->get_writes(); wr; wr = wr->next())
    {
        if ((wr->flags & BS_HEAP_TYPE) == BS_HEAP_BIG_WRITE)
        {
            mvcc_data_refs[wr->location] += add_ref;
            if (wr->flags & BS_HEAP_STABLE)
            {
                if (!for_obj)
                {
                    break;
                }
                add_ref = 1;
            }
        }
        else if ((wr->flags & BS_HEAP_TYPE) == BS_HEAP_SMALL_WRITE)
        {
            mvcc_buffer_refs[wr->location] += add_ref;
        }
    }
    // copied :-)
    return true;
}

void blockstore_heap_t::mark_overwritten(uint64_t over_lsn, uint64_t inode, heap_write_t *wr, heap_write_t *end_wr, bool tracking_active)
{
    while (wr && wr != end_wr)
    {
        if (wr->needs_compact(0))
        {
            mark_lsn_compacted(wr->lsn, true);
        }
        if ((wr->flags & BS_HEAP_TYPE) == BS_HEAP_BIG_WRITE)
        {
            overwrite_ref_queue.push_back((heap_refqi_t){ .lsn = over_lsn, .inode = inode, .location = wr->location, .len = 0, .is_data = true });
            mvcc_data_refs[wr->location] += !tracking_active;
        }
        else if ((wr->flags & BS_HEAP_TYPE) == BS_HEAP_SMALL_WRITE && wr->size > 0)
        {
            overwrite_ref_queue.push_back((heap_refqi_t){ .lsn = over_lsn, .inode = inode, .location = wr->location, .len = wr->len, .is_data = false });
            mvcc_buffer_refs[wr->location] += !tracking_active;
        }
        wr = wr->next();
    }
}

int blockstore_heap_t::update_object(uint32_t block_num, heap_object_t *obj, heap_write_t *wr, uint32_t *modified_block)
{
    const auto oid = (object_id){ .inode = obj->inode, .stripe = obj->stripe };
    const uint32_t wr_size = wr->get_size(this);
    auto & inf = block_info.at(block_num);
    assert(inf.data);
    bool is_overwrite = (wr->flags == (BS_HEAP_BIG_WRITE|BS_HEAP_STABLE) || wr->flags == (BS_HEAP_TOMBSTONE|BS_HEAP_STABLE));
    if (dsk->meta_block_size-inf.used_space < wr_size+2)
    {
        // Something in the block has to be compacted
        return ENOSPC;
    }
    auto first_wr = obj->get_writes();
    if ((first_wr->flags & BS_HEAP_TYPE) == BS_HEAP_TOMBSTONE && !is_overwrite)
    {
        // Small overwrites are only allowed over live objects
        return EINVAL;
    }
    if (!(first_wr->flags & BS_HEAP_STABLE) && (wr->flags & BS_HEAP_STABLE))
    {
        // Stable overwrites are not allowed over unstable
        return EINVAL;
    }
    if (wr->version <= first_wr->version)
    {
        if (!wr->version)
        {
            wr->version = first_wr->version + 1;
        }
        else
        {
            // Overwrites with a smaller version are forbidden
            return EINVAL;
        }
    }
    if (modified_block)
    {
        *modified_block = block_num;
    }
    // Save a copy of the object - only when overwriting
    bool tracking_active = is_overwrite ? mvcc_save_copy(obj) : mvcc_check_tracking(oid);
    if (tracking_active)
    {
        // MVCC reference tracking is in action for the object, increase the refcount
        if ((wr->flags & BS_HEAP_TYPE) == BS_HEAP_BIG_WRITE)
        {
            mvcc_data_refs[wr->location]++;
        }
        else if ((wr->flags & BS_HEAP_TYPE) == BS_HEAP_SMALL_WRITE)
        {
            mvcc_buffer_refs[wr->location]++;
        }
    }
    const uint8_t *old_data = inf.data;
    const uint32_t offset = find_block_space(block_num, wr_size);
    if (old_data != inf.data)
    {
        obj = read_entry(oid, NULL);
        first_wr = obj->get_writes();
    }
    assert(offset != UINT32_MAX);
    memcpy(inf.data + offset, wr, wr_size);
    heap_write_t *new_wr = (heap_write_t*)(inf.data + offset);
    new_wr->size = wr_size;
    new_wr->lsn = ++next_lsn;
    int32_t used_delta = wr_size;
    if (is_overwrite)
    {
        mark_overwritten(new_wr->lsn, obj->inode, first_wr, NULL, tracking_active);
        // Free old write entries
        used_delta -= free_writes(first_wr, NULL);
        new_wr->next_pos = 0;
    }
    else if ((wr->flags & BS_HEAP_TYPE) == BS_HEAP_INTENT_WRITE &&
        (first_wr->flags & BS_HEAP_TYPE) == BS_HEAP_INTENT_WRITE)
    {
        assert(wr->flags == (BS_HEAP_INTENT_WRITE|BS_HEAP_STABLE));
        auto second_wr = first_wr->next();
        bitmap_set(second_wr->get_int_bitmap(this), first_wr->offset, first_wr->len, dsk->bitmap_granularity);
        used_delta -= free_writes(first_wr, second_wr);
        new_wr->next_pos = (uint8_t*)second_wr - (uint8_t*)new_wr;
    }
    else
    {
        new_wr->next_pos = ((uint8_t*)obj + obj->write_pos) - (uint8_t*)new_wr;
    }
    wr->lsn = new_wr->lsn;
    push_inflight_lsn(oid, new_wr->lsn, new_wr->needs_compact(0) ? HEAP_INFLIGHT_COMPACTABLE : 0);
    if ((wr->flags & BS_HEAP_TYPE) == BS_HEAP_BIG_WRITE)
    {
        uint8_t *int_bitmap = new_wr->get_int_bitmap(this);
        memset(int_bitmap, 0, dsk->clean_entry_bitmap_size);
        bitmap_set(int_bitmap, wr->offset, wr->len, dsk->bitmap_granularity);
    }
    obj->write_pos = offset - ((uint8_t*)obj - inf.data);
    obj->crc32c = obj->calc_crc32c();
    // Change block free space
    add_used_space(block_num, used_delta);
    return 0;
}

int blockstore_heap_t::post_write(object_id oid, heap_write_t *wr, uint32_t *modified_block)
{
    uint32_t block_num = 0;
    heap_object_t *obj = read_entry(oid, &block_num);
    if (!obj)
    {
        return add_object(oid, wr, modified_block);
    }
    return update_object(block_num, obj, wr, modified_block);
}

int blockstore_heap_t::post_write(uint32_t & block_num, object_id oid, heap_object_t *obj, heap_write_t *wr)
{
    if (!obj)
    {
        return add_object(oid, wr, &block_num);
    }
    return update_object(block_num, obj, wr, &block_num);
}

int blockstore_heap_t::post_stabilize(object_id oid, uint64_t version, uint32_t *modified_block, uint64_t *new_lsn, uint64_t *new_to_lsn)
{
    uint32_t block_num = 0;
    heap_object_t *obj = read_entry(oid, &block_num);
    if (!obj)
    {
        // No such object
        return ENOENT;
    }
    auto & inf = block_info.at(block_num);
    assert(inf.data);
    heap_write_t *unstable_wr = NULL;
    heap_write_t *unstable_big_wr = NULL;
    heap_write_t *wr = obj->get_writes();
    if (wr->version < version)
    {
        // No such version
        return ENOENT;
    }
    uint64_t stab_count = 0;
    for (; wr; wr = wr->next())
    {
        if ((wr->flags & BS_HEAP_STABLE))
        {
            break;
        }
        else if (wr->version <= version)
        {
            stab_count++;
            unstable_wr = wr;
            if (!unstable_big_wr &&
                ((wr->flags & BS_HEAP_TYPE) == BS_HEAP_BIG_WRITE ||
                (wr->flags & BS_HEAP_TYPE) == BS_HEAP_TOMBSTONE))
            {
                unstable_big_wr = wr;
            }
        }
    }
    if (!unstable_wr)
    {
        // Version is already stable
        return 0;
    }
    if (modified_block)
    {
        *modified_block = block_num;
    }
    // Save a copy of the object
    if (unstable_big_wr && unstable_big_wr->next())
    {
        // Remove previous stable entry series
        bool tracking_active = mvcc_save_copy(obj);
        mark_overwritten(next_lsn+1, obj->inode, unstable_big_wr->next(), NULL, tracking_active);
        add_used_space(block_num, -free_writes(unstable_big_wr->next(), NULL));
        unstable_big_wr->next_pos = 0;
    }
    // Set the stability flag and assign new LSNs
    if (new_lsn)
    {
        *new_lsn = next_lsn+1;
    }
    if (new_to_lsn)
    {
        *new_to_lsn = next_lsn+stab_count;
    }
    next_lsn += stab_count;
    uint64_t last_lsn = next_lsn;
    for (wr = obj->get_writes(); wr; wr = wr->next())
    {
        if (!(wr->flags & BS_HEAP_STABLE) && wr->version <= version)
        {
            wr->flags |= BS_HEAP_STABLE;
            wr->lsn = last_lsn--;
            push_inflight_lsn(oid, wr->lsn, wr->needs_compact(0) ? HEAP_INFLIGHT_COMPACTABLE : 0);
        }
    }
    obj->crc32c = obj->calc_crc32c();
    return 0;
}

int blockstore_heap_t::post_rollback(object_id oid, uint64_t version, uint64_t *new_lsn, uint32_t *modified_block)
{
    uint32_t block_num = 0;
    heap_object_t *obj = read_entry(oid, &block_num);
    if (!obj)
    {
        // No such object
        return ENOENT;
    }
    auto & inf = block_info.at(block_num);
    assert(inf.data);
    heap_write_t *wr = obj->get_writes();
    if (wr->version < version)
    {
        // No such version
        return ENOENT;
    }
    if (wr->version == version && (wr->flags & BS_HEAP_STABLE))
    {
        // Already rolled back
        return 0;
    }
    for (; wr && wr->version > version; wr = wr->next())
    {
        if (wr->flags & BS_HEAP_STABLE)
        {
            // Already committed, can't rollback
            return EBUSY;
        }
    }
    if (modified_block)
    {
        *modified_block = block_num;
    }
    bool tracking_active = mvcc_save_copy(obj);
    ++next_lsn;
    if (new_lsn)
    {
        *new_lsn = next_lsn;
    }
    push_inflight_lsn(oid, next_lsn, 0);
    if (!wr)
    {
        erase_object(block_num, obj, next_lsn, tracking_active);
    }
    else
    {
        // Erase head versions
        heap_write_t *first_wr = obj->get_writes();
        mark_overwritten(next_lsn, obj->inode, first_wr, wr, tracking_active);
        add_used_space(block_num, -free_writes(first_wr, wr));
        obj->write_pos = ((uint8_t*)wr - (uint8_t*)obj);
        obj->crc32c = obj->calc_crc32c();
    }
    return 0;
}

int blockstore_heap_t::post_delete(object_id oid, uint64_t *new_lsn, uint32_t *modified_block)
{
    uint32_t block_num = 0;
    heap_object_t *obj = read_entry(oid, &block_num);
    if (!obj)
    {
        // No such object
        return ENOENT;
    }
    if (modified_block)
    {
        *modified_block = block_num;
    }
    return post_delete(block_num, obj, new_lsn);
}

int blockstore_heap_t::post_delete(uint32_t block_num, heap_object_t *obj, uint64_t *new_lsn)
{
    bool tracking_active = mvcc_save_copy(obj);
    auto & inf = block_info.at(block_num);
    assert(inf.data);
    ++next_lsn;
    if (new_lsn)
    {
        *new_lsn = next_lsn;
    }
    push_inflight_lsn((object_id){ .inode = obj->inode, .stripe = obj->stripe }, next_lsn, 0);
    erase_object(block_num, obj, next_lsn, tracking_active);
    return 0;
}

int blockstore_heap_t::get_next_compact(object_id & oid)
{
    if (next_compact_lsn < first_inflight_lsn)
    {
        next_compact_lsn = first_inflight_lsn;
    }
    while (next_compact_lsn-first_inflight_lsn < inflight_lsn.size())
    {
        if (next_compact_lsn > (dsk->disable_meta_fsync && dsk->disable_journal_fsync ? completed_lsn : fsynced_lsn))
        {
            return ENOENT;
        }
        auto & item = inflight_lsn[next_compact_lsn-first_inflight_lsn];
        if (!(item.flags & HEAP_INFLIGHT_COMPACTABLE))
        {
            next_compact_lsn++;
            continue;
        }
        if (!(item.flags & HEAP_INFLIGHT_DONE))
        {
            break;
        }
        next_compact_lsn++;
        oid = item.oid;
        return 0;
    }
    return ENOENT;
}

int blockstore_heap_t::compact_object(object_id oid, uint64_t compact_lsn, uint8_t *new_csums)
{
    uint32_t block_num = 0;
    heap_object_t *obj = read_entry(oid, &block_num);
    if (!obj)
    {
        // No such object
        return ENOENT;
    }
    mvcc_save_copy(obj);
    int res = EAGAIN;
    uint32_t freed = compact_object_to(obj, compact_lsn, new_csums, true);
    if (freed)
    {
        add_used_space(block_num, -freed);
        res = 0;
    }
    return res;
}

void blockstore_heap_t::deref_data(uint64_t inode, uint64_t location, bool free_at_0)
{
    auto ref_it = mvcc_data_refs.find(location);
    if (ref_it != mvcc_data_refs.end())
    {
        assert(ref_it->second > 0);
        ref_it->second--;
        if (!ref_it->second)
        {
            mvcc_data_refs.erase(ref_it);
            ref_it = mvcc_data_refs.end();
        }
    }
    if (ref_it == mvcc_data_refs.end() && free_at_0)
    {
        assert(data_alloc->get(location >> dsk->block_order));
        data_alloc->set(location >> dsk->block_order, false);
        auto & space = inode_space_stats[inode];
        assert(space >= dsk->data_block_size);
        space -= dsk->data_block_size;
        data_used_space -= dsk->data_block_size;
        if (!space)
            inode_space_stats.erase(inode);
    }
}

void blockstore_heap_t::deref_buffer(uint64_t inode, uint64_t location, uint32_t len, bool free_at_0)
{
    auto ref_it = mvcc_buffer_refs.find(location);
    if (ref_it != mvcc_buffer_refs.end())
    {
        assert(ref_it->second > 0);
        ref_it->second--;
        if (!ref_it->second)
        {
            mvcc_buffer_refs.erase(ref_it);
            ref_it = mvcc_buffer_refs.end();
        }
    }
    if (ref_it == mvcc_buffer_refs.end() && free_at_0)
    {
        free_buffer_area(inode, location, len);
    }
}

void blockstore_heap_t::deref_overwrites(uint64_t lsn)
{
    while (overwrite_ref_queue.size() > 0)
    {
        // Dereference item
        auto & el = overwrite_ref_queue.front();
        if (el.lsn > lsn)
            break;
        if (el.is_data)
            deref_data(el.inode, el.location, true);
        else
            deref_buffer(el.inode, el.location, el.len, true);
        overwrite_ref_queue.pop_front();
    }
}

void blockstore_heap_t::free_object_space(inode_t inode, heap_write_t *from, heap_write_t *to, int mode)
{
    for (heap_write_t *wr = from; wr && wr != to; wr = wr->next())
    {
        if ((wr->flags & BS_HEAP_TYPE) == BS_HEAP_BIG_WRITE)
        {
            deref_data(inode, wr->location, mode != BS_HEAP_FREE_MAIN);
            if (mode == BS_HEAP_FREE_MVCC && (wr->flags & BS_HEAP_STABLE))
            {
                // Stop at the last visible version
                break;
            }
        }
        else if ((wr->flags & BS_HEAP_TYPE) == BS_HEAP_SMALL_WRITE)
        {
            deref_buffer(inode, wr->location, wr->len, mode != BS_HEAP_FREE_MAIN);
        }
    }
}

void blockstore_heap_t::erase_block_index(inode_t inode, uint64_t stripe)
{
    auto & pg_index = block_index[get_pg_id(inode, stripe)];
    auto & inode_index = pg_index[inode];
    inode_index.erase(stripe);
    if (!inode_index.size())
    {
        pg_index.erase(inode);
    }
}

void blockstore_heap_t::erase_object(uint32_t block_num, heap_object_t *obj, uint64_t lsn, bool tracking_active)
{
    // Erase object
    if (!lsn)
    {
        for (auto wr = obj->get_writes(); wr; wr = wr->next())
        {
            if (wr->needs_compact(0))
            {
                mark_lsn_compacted(wr->lsn, true);
            }
        }
        free_object_space(obj->inode, obj->get_writes(), NULL);
    }
    else
    {
        mark_overwritten(lsn, obj->inode, obj->get_writes(), NULL, tracking_active);
    }
    erase_block_index(obj->inode, obj->stripe);
    auto freed = free_writes(obj->get_writes(), NULL);
    auto obj_size = obj->size;
    memset((uint8_t*)obj, 0, obj_size);
    *((uint16_t*)obj) = FREE_SPACE_BIT | obj_size;
    add_used_space(block_num, -obj_size-freed);
}

void blockstore_heap_t::add_used_space(uint32_t block_num, int32_t used_delta)
{
    auto & inf = block_info.at(block_num);
    meta_used_space += used_delta;
    auto minthresh = dsk->meta_block_size-target_block_free_space;
    auto maxthresh = dsk->meta_block_size-sizeof(heap_object_t)-2*max_write_entry_size;
    auto thresh = minthresh;
    auto old_used_space = inf.used_space;
    inf.used_space += used_delta;
    for (int i = 0; i < meta_alloc_buckets; )
    {
        if (old_used_space > thresh && inf.used_space <= thresh)
        {
            meta_allocs[i]->set(block_num, false);
            if (!i)
                meta_alloc_count--;
        }
        else if (old_used_space <= thresh && inf.used_space > thresh)
        {
            meta_allocs[i]->set(block_num, true);
            if (!i)
                meta_alloc_count++;
        }
        i++;
        thresh = (i == meta_alloc_buckets-1 ? maxthresh : minthresh + (maxthresh-minthresh)*i/(meta_alloc_buckets-1));
    }
}

int blockstore_heap_t::list_objects(uint32_t pg_num, uint64_t min_inode, uint64_t max_inode,
    obj_ver_id **result_list, size_t *stable_count, size_t *unstable_count)
{
    obj_ver_id *res = NULL;
    size_t res_size = 0, res_alloc = 0;
    obj_ver_id *unstable = NULL;
    size_t unstable_size = 0, unstable_alloc = 0;
    uint64_t pool_id = (min_inode >> (64-POOL_ID_BITS));
    if (pool_id == 0 || pool_id != (max_inode >> (64-POOL_ID_BITS)))
    {
        return EINVAL;
    }
    auto sh_it = pool_shard_settings.find(pool_id);
    uint32_t pg_count = (sh_it != pool_shard_settings.end() ? sh_it->second.pg_count : 0);
    if (pg_num == 0 || pg_num > (pg_count == 0 ? 1 : pg_count))
    {
        return EINVAL;
    }
    uint64_t pool_pg_id = (pool_id << (64-POOL_ID_BITS)) | (pg_count == 0 ? 0 : pg_num);
    auto first_it = block_index[pool_pg_id].lower_bound(min_inode);
    auto last_it = block_index[pool_pg_id].upper_bound(max_inode);
    for (auto inode_it = first_it; inode_it != last_it; inode_it++)
    {
        for (auto & stripe_pair: inode_it->second)
        {
            auto oid = (object_id){ .inode = inode_it->first, .stripe = stripe_pair.first };
            const uint64_t block_pos = stripe_pair.second;
            const uint32_t block_num = block_pos / dsk->meta_block_size;
            heap_object_t *obj = (heap_object_t*)(block_info[block_num].data + (block_pos % dsk->meta_block_size));
            assert(obj->inode == oid.inode && obj->stripe == oid.stripe);
            uint64_t stable_version = 0;
            auto first_wr = obj->get_writes();
            for (auto wr = first_wr; wr; wr = wr->next())
            {
                if (wr->flags & BS_HEAP_STABLE)
                {
                    stable_version = wr->version;
                    break;
                }
            }
            if (!(first_wr->flags & BS_HEAP_STABLE))
            {
                if (unstable_size >= unstable_alloc)
                {
                    unstable_alloc = (!unstable_alloc ? 128 : unstable_alloc*2);
                    unstable = (obj_ver_id*)realloc_or_die(unstable, sizeof(obj_ver_id) * unstable_alloc);
                }
                unstable[unstable_size++] = (obj_ver_id){ .oid = oid, .version = stable_version };
            }
            if (stable_version)
            {
                if (res_size >= res_alloc)
                {
                    res_alloc = (!res_alloc ? 128 : res_alloc*2);
                    res = (obj_ver_id*)realloc_or_die(res, sizeof(obj_ver_id) * res_alloc);
                }
                res[res_size++] = (obj_ver_id){ .oid = oid, .version = stable_version };
            }
        }
    }
    if (unstable_size)
    {
        if (res_size+unstable_size > res_alloc)
        {
            res_alloc = res_size+unstable_size;
            res = (obj_ver_id*)realloc_or_die(res, sizeof(obj_ver_id) * res_alloc);
        }
        memcpy(res + res_size, unstable, sizeof(obj_ver_id) * unstable_size);
        free(unstable);
        unstable = NULL;
    }
    *result_list = res;
    *stable_count = res_size;
    *unstable_count = unstable_size;
    return 0;
}

uint64_t blockstore_heap_t::find_free_data()
{
    uint64_t loc = data_alloc->find_free();
    if (loc != UINT64_MAX)
    {
        loc = loc << dsk->block_order;
    }
    return loc;
}

bool blockstore_heap_t::is_data_used(uint64_t location)
{
    return data_alloc->get(location >> dsk->block_order);
}

void blockstore_heap_t::use_data(inode_t inode, uint64_t location)
{
    assert(!data_alloc->get(location >> dsk->block_order));
    data_alloc->set(location >> dsk->block_order, true);
    inode_space_stats[inode] += dsk->data_block_size;
    data_used_space += dsk->data_block_size;
}

uint64_t blockstore_heap_t::find_free_buffer_area(uint64_t size)
{
    assert(!(size % dsk->bitmap_granularity));
    uint32_t pos = buffer_alloc->find(size / dsk->bitmap_granularity);
    if (pos == UINT32_MAX)
    {
        return UINT64_MAX;
    }
    return pos * dsk->bitmap_granularity;
}

bool blockstore_heap_t::is_buffer_area_free(uint64_t location, uint64_t size)
{
    assert(!(location % dsk->bitmap_granularity));
    return buffer_alloc->is_free(location / dsk->bitmap_granularity);
}

void blockstore_heap_t::use_buffer_area(inode_t inode, uint64_t location, uint64_t size)
{
    assert(!(size % dsk->bitmap_granularity));
    buffer_alloc->use(location / dsk->bitmap_granularity, size / dsk->bitmap_granularity);
    buffer_area_used_space += size;
}

void blockstore_heap_t::free_buffer_area(inode_t inode, uint64_t location, uint64_t size)
{
    assert(!(location % dsk->bitmap_granularity));
    buffer_alloc->free(location / dsk->bitmap_granularity);
    buffer_area_used_space -= size;
}

uint64_t blockstore_heap_t::get_buffer_area_used_space()
{
    return buffer_area_used_space;
}

uint8_t *blockstore_heap_t::get_meta_block(uint32_t block_num)
{
    auto & inf = block_info.at(block_num);
    return inf.data;
}

uint32_t blockstore_heap_t::get_meta_block_used_space(uint32_t block_num)
{
    auto & inf = block_info.at(block_num);
    return inf.used_space;
}

uint64_t blockstore_heap_t::get_data_used_space()
{
    return data_used_space;
}

const std::map<uint64_t, uint64_t> & blockstore_heap_t::get_inode_space_stats()
{
    return inode_space_stats;
}

uint64_t blockstore_heap_t::get_meta_total_space()
{
    return (uint64_t)meta_block_count*dsk->meta_block_size;
}

uint64_t blockstore_heap_t::get_meta_used_space()
{
    return meta_used_space;
}

uint32_t blockstore_heap_t::get_meta_nearfull_blocks()
{
    return meta_alloc_count;
}

uint32_t blockstore_heap_t::get_compact_queue_size()
{
    return to_compact_count;
}

uint32_t blockstore_heap_t::get_inflight_queue_size()
{
    return inflight_lsn.size();
}

uint32_t blockstore_heap_t::get_max_write_entry_size()
{
    return max_write_entry_size;
}

void blockstore_heap_t::set_fail_on_warn(bool fail)
{
    fail_on_warn = fail;
}

void blockstore_heap_t::push_inflight_lsn(object_id oid, uint64_t lsn, uint64_t flags)
{
    uint64_t next_inf = first_inflight_lsn + inflight_lsn.size();
    if (flags & HEAP_INFLIGHT_COMPACTABLE)
    {
        to_compact_count++;
    }
    if (lsn == next_inf)
    {
        inflight_lsn.push_back((heap_inflight_lsn_t){ .oid = oid, .flags = flags });
    }
    else
    {
        if (lsn > next_inf)
        {
            inflight_lsn.resize(lsn-first_inflight_lsn+1, (heap_inflight_lsn_t){ .flags = HEAP_INFLIGHT_DONE });
        }
        inflight_lsn[lsn-first_inflight_lsn] = (heap_inflight_lsn_t){ .oid = oid, .flags = flags };
    }
}

void blockstore_heap_t::mark_lsn_completed(uint64_t lsn)
{
    assert(lsn >= first_inflight_lsn && lsn < first_inflight_lsn+inflight_lsn.size());
    auto it = inflight_lsn.begin() + (lsn-first_inflight_lsn);
    assert(!(it->flags & HEAP_INFLIGHT_DONE));
    it->flags |= HEAP_INFLIGHT_DONE;
    if (lsn == compacted_lsn+1 && !(it->flags & HEAP_INFLIGHT_COMPACTABLE))
    {
        assert(compacted_lsn+1 >= first_inflight_lsn);
        while (it != inflight_lsn.end() && (it->flags == HEAP_INFLIGHT_DONE))
        {
            it++;
            compacted_lsn++;
        }
    }
    if (lsn > completed_lsn)
    {
        assert(completed_lsn+1 >= first_inflight_lsn);
        if (completed_lsn+1-first_inflight_lsn < inflight_lsn.size())
        {
            auto old_completed_lsn = completed_lsn;
            auto it = inflight_lsn.begin() + (completed_lsn+1-first_inflight_lsn);
            while (it != inflight_lsn.end() && (it->flags & HEAP_INFLIGHT_DONE))
            {
                completed_lsn++;
                it++;
            }
            if (old_completed_lsn != completed_lsn && dsk->disable_meta_fsync && dsk->disable_journal_fsync)
            {
                deref_overwrites(completed_lsn);
            }
        }
    }
}

void blockstore_heap_t::mark_lsn_fsynced(uint64_t lsn)
{
    if (lsn > fsynced_lsn)
    {
        assert(lsn >= first_inflight_lsn && lsn <= completed_lsn);
        fsynced_lsn = lsn;
        deref_overwrites(lsn);
    }
}

void blockstore_heap_t::mark_lsn_compacted(uint64_t lsn, bool allow_undone)
{
    assert(lsn >= first_inflight_lsn && lsn < first_inflight_lsn+inflight_lsn.size());
    auto & item = inflight_lsn[lsn - first_inflight_lsn];
    assert((item.flags & HEAP_INFLIGHT_DONE) || allow_undone);
    if (!(item.flags & HEAP_INFLIGHT_COMPACTABLE))
        return;
    item.flags -= HEAP_INFLIGHT_COMPACTABLE;
    to_compact_count--;
    if (lsn == compacted_lsn+1)
    {
        assert(completed_lsn+1 >= first_inflight_lsn);
        while (compacted_lsn+1-first_inflight_lsn < inflight_lsn.size() &&
            (inflight_lsn[compacted_lsn+1-first_inflight_lsn].flags == HEAP_INFLIGHT_DONE))
        {
            compacted_lsn++;
        }
    }
}

void blockstore_heap_t::mark_object_compacted(heap_object_t *obj, uint64_t max_lsn)
{
    for (auto wr = obj->get_writes(); wr; wr = wr->next())
    {
        if (wr->is_compacted(max_lsn))
        {
            mark_lsn_compacted(wr->lsn);
        }
    }
}

void blockstore_heap_t::mark_lsn_trimmed(uint64_t lsn)
{
    assert(lsn >= first_inflight_lsn && lsn < first_inflight_lsn+inflight_lsn.size());
    while (first_inflight_lsn <= lsn && inflight_lsn.size() > 0)
    {
        assert(inflight_lsn[0].flags == HEAP_INFLIGHT_DONE);
        // In the future maybe we could skip unflushable LSNs here
        compact_object(inflight_lsn[0].oid, lsn, NULL);
        inflight_lsn.pop_front();
        first_inflight_lsn++;
    }
}

uint64_t blockstore_heap_t::get_completed_lsn()
{
    return completed_lsn;
}

uint64_t blockstore_heap_t::get_fsynced_lsn()
{
    return dsk->disable_meta_fsync && dsk->disable_journal_fsync ? completed_lsn : fsynced_lsn;
}
