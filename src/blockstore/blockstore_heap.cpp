// Metadata storage version 3 ("heap")
// Copyright (c) Vitaliy Filippov, 2025+
// License: VNPL-1.1 (see README.md for details)

#include <assert.h>
#include <string.h>

#include <stdexcept>
#include <algorithm>

#include "blockstore_heap.h"
#include "../util/allocator.h"
#include "../util/crc32c.h"
#include "../util/malloc_or_die.h"

#define BS_HEAP_FREE_MVCC 1
#define BS_HEAP_FREE_MAIN 2
#define FREE_SPACE_BIT 0x8000

#define HEAP_INFLIGHT_DONE 1
#define HEAP_INFLIGHT_COMPACTABLE 2

#define MIN_ALLOC (sizeof(heap_object_t)+sizeof(heap_tombstone_t))

static constexpr uint32_t heap_entry_type_pos = 4;

heap_write_t *heap_write_t::next()
{
    return (next_pos ? (heap_write_t*)((uint8_t*)this + next_pos) : NULL);
}

uint32_t heap_write_t::get_size(blockstore_heap_t *heap)
{
    if (type() == BS_HEAP_BIG_WRITE)
    {
        return sizeof(heap_big_write_t) + heap->dsk->clean_entry_bitmap_size*2 + get_csum_size(heap);
    }
    if (type() == BS_HEAP_TOMBSTONE)
    {
        return sizeof(heap_tombstone_t);
    }
    return sizeof(heap_small_write_t) + heap->dsk->clean_entry_bitmap_size + get_csum_size(heap);
}

uint32_t heap_write_t::get_csum_size(blockstore_heap_t *heap)
{
    if (!heap->dsk->csum_block_size)
    {
        return (type() == BS_HEAP_SMALL_WRITE || type() == BS_HEAP_INTENT_WRITE ? 4 : 0);
    }
    if (type() == BS_HEAP_TOMBSTONE)
    {
        return 0;
    }
    if (type() == BS_HEAP_BIG_WRITE)
    {
        // We always store full checksums for "big" entries to prevent ENOSPC on compaction
        // when (big_write+small_write) are smaller than (compacted big_write)
        // However, we only use part of it related to offset..offset+len
        return heap->dsk->data_block_size/heap->dsk->csum_block_size * (heap->dsk->data_csum_type & 0xFF);
    }
    return ((small().offset+small().len+heap->dsk->csum_block_size-1)/heap->dsk->csum_block_size - small().offset/heap->dsk->csum_block_size)
        * (heap->dsk->data_csum_type & 0xFF);
}

bool heap_write_t::needs_recheck(blockstore_heap_t *heap)
{
    if (type() != BS_HEAP_SMALL_WRITE && type() != BS_HEAP_INTENT_WRITE)
    {
        return false;
    }
    return small().len > 0 && lsn > heap->compacted_lsn;
}

bool heap_write_t::needs_compact(blockstore_heap_t *heap)
{
    if (entry_type == (BS_HEAP_SMALL_WRITE|BS_HEAP_STABLE))
    {
        return true;
    }
    else if (entry_type == (BS_HEAP_INTENT_WRITE|BS_HEAP_STABLE) && heap->dsk->csum_block_size > heap->dsk->bitmap_granularity)
    {
        return ((small().offset % heap->dsk->csum_block_size) || (small().len % heap->dsk->csum_block_size));
    }
    return false;
}

bool heap_write_t::is_compacted(uint64_t compacted_lsn)
{
    return lsn <= compacted_lsn && (entry_type == (BS_HEAP_SMALL_WRITE|BS_HEAP_STABLE) || entry_type == (BS_HEAP_INTENT_WRITE|BS_HEAP_STABLE));
}

bool heap_write_t::can_be_collapsed(blockstore_heap_t *heap)
{
    if (type() == BS_HEAP_BIG_WRITE || type() == BS_HEAP_TOMBSTONE)
    {
        return false;
    }
    if (!heap->dsk->csum_block_size || heap->dsk->csum_block_size == heap->dsk->bitmap_granularity)
    {
        return true;
    }
    return !(small().offset % heap->dsk->csum_block_size) && !(small().len % heap->dsk->csum_block_size);
}

bool heap_write_t::is_allowed_before_compacted(uint64_t compacted_lsn, bool is_last_entry)
{
    return lsn <= compacted_lsn && (is_last_entry
        ? (entry_type == (BS_HEAP_BIG_WRITE|BS_HEAP_STABLE))
        : (entry_type == (BS_HEAP_SMALL_WRITE|BS_HEAP_STABLE) || entry_type == (BS_HEAP_INTENT_WRITE|BS_HEAP_STABLE)));
}

uint8_t *heap_write_t::get_ext_bitmap(blockstore_heap_t *heap)
{
    if (type() == BS_HEAP_TOMBSTONE)
        return NULL;
    return ((uint8_t*)this + (type() == BS_HEAP_BIG_WRITE ? sizeof(heap_big_write_t) : sizeof(heap_small_write_t)));
}

uint8_t *heap_write_t::get_int_bitmap(blockstore_heap_t *heap)
{
    if (type() != BS_HEAP_BIG_WRITE)
        return NULL;
    return ((uint8_t*)this + (type() == BS_HEAP_BIG_WRITE ? sizeof(heap_big_write_t) : sizeof(heap_small_write_t)) + heap->dsk->clean_entry_bitmap_size);
}

uint8_t *heap_write_t::get_checksums(blockstore_heap_t *heap)
{
    if (!heap->dsk->csum_block_size)
        return NULL;
    if ((type() == BS_HEAP_SMALL_WRITE || type() == BS_HEAP_INTENT_WRITE) && small().len > 0)
        return ((uint8_t*)this + sizeof(heap_small_write_t) + heap->dsk->clean_entry_bitmap_size);
    if (type() != BS_HEAP_BIG_WRITE)
        return NULL;
    return ((uint8_t*)this + sizeof(heap_big_write_t) + 2*heap->dsk->clean_entry_bitmap_size);
}

uint32_t *heap_write_t::get_checksum(blockstore_heap_t *heap)
{
    if (heap->dsk->csum_block_size ||
        type() != BS_HEAP_SMALL_WRITE && type() != BS_HEAP_INTENT_WRITE ||
        small().len == 0)
    {
        return NULL;
    }
    return (uint32_t*)((uint8_t*)this + sizeof(heap_small_write_t) + heap->dsk->clean_entry_bitmap_size);
}

heap_write_t *heap_object_t::get_writes()
{
    return (heap_write_t*)((uint8_t*)this + write_pos);
}

uint32_t heap_object_t::calc_crc32c()
{
    uint32_t old_crc32c = crc32c;
    crc32c = 0;
    uint32_t res = ::crc32c(0, (uint8_t*)this, sizeof(heap_object_t));
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
    max_write_entry_size(sizeof(heap_small_write_t) + 2*dsk->clean_entry_bitmap_size +
        (dsk->csum_block_size ? dsk->data_block_size/dsk->csum_block_size*(dsk->data_csum_type & 0xFF) : 4 /*sizeof crc32c*/))
{
    assert(target_block_free_space < dsk->meta_block_size);
    assert(dsk->meta_block_size < 32768);
    assert(dsk->meta_area_size > 0);
    assert(dsk->journal_len > 0);
    meta_alloc = new multilist_index_t(meta_block_count, 1 + dsk->meta_block_size/MIN_ALLOC, 0);
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
    delete meta_alloc;
    delete data_alloc;
    delete buffer_alloc;
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
    bool handled;
};

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
void blockstore_heap_t::read_blocks(uint64_t disk_offset, uint64_t disk_size, uint8_t *buf,
    std::function<void(heap_object_t*)> handle_object, std::function<void(uint32_t, uint32_t, uint8_t*)> handle_block)
{
    for (uint64_t buf_offset = 0; buf_offset < disk_size; buf_offset += dsk->meta_block_size)
    {
        uint32_t block_num = (disk_offset + buf_offset) / dsk->meta_block_size;
        assert(block_num < block_info.size());
        uint32_t block_offset = 0;
        std::map<uint32_t, verify_offset_t> offsets_seen;
        while (block_offset <= dsk->meta_block_size-2)
        {
            heap_write_t *skip_erase_wr = NULL, *skip_erase_to = NULL;
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
                if (abort_on_corruption)
                    abort();
                if (block_offset > 0)
                {
                    memset(data, 0, dsk->meta_block_size-block_offset);
                    region_marker = FREE_SPACE_BIT | (dsk->meta_block_size-block_offset);
                }
                break;
            }
            const uint8_t entry_type = data[heap_entry_type_pos];
            if (entry_type != BS_HEAP_OBJECT)
            {
                // Write entry (probably) (or garbage)
                if ((entry_type & BS_HEAP_TYPE) < BS_HEAP_OBJECT ||
                    (entry_type & BS_HEAP_TYPE) > BS_HEAP_INTENT_WRITE ||
                    (entry_type & ~(BS_HEAP_TYPE|BS_HEAP_STABLE)))
                {
                    fprintf(stderr, "Warning: Entry of unknown type %u in metadata block %u at %u, skipping\n",
                        entry_type, block_num, block_offset);
                }
                auto offset_it = offsets_seen.find(block_offset+region_marker);
                if (offset_it == offsets_seen.end())
                {
                    offsets_seen[block_offset+region_marker] = (verify_offset_t){ .start = block_offset, .handled = false };
                }
                block_offset += region_marker;
                continue;
            }
            if (region_marker != sizeof(heap_object_t))
            {
                fprintf(stderr, "Warning: Object entry has invalid size in metadata block %u at %u (%u != %ju bytes), skipping\n",
                    block_num, block_offset, region_marker, sizeof(heap_object_t));
skip_corrupted:
                if (abort_on_corruption)
                    abort();
skip_object:
                offsets_seen[block_offset+region_marker].handled = false;
                for (auto wr = skip_erase_wr; wr && wr != skip_erase_to; wr = wr->next())
                {
                    uint32_t wr_pos = ((uint8_t*)wr - buf - buf_offset);
                    offsets_seen[wr_pos+wr->size].handled = false;
                }
skip_unseen:
                if (block_offset > 0 && region_marker > 0)
                {
                    if (region_marker >= 2)
                        memset(data+2, 0, region_marker-2);
                    region_marker |= FREE_SPACE_BIT;
                }
                block_offset += (region_marker & ~FREE_SPACE_BIT);
                continue;
            }
            heap_object_t *obj = (heap_object_t *)data;
            {
                auto offset_it = offsets_seen.upper_bound(block_offset);
                if (offset_it != offsets_seen.end() && offset_it->second.start < block_offset+region_marker && offset_it->second.handled)
                {
                    fprintf(stderr, "Warning: Object %jx:%jx in metadata block %u at %u intersects with a write entry, skipping\n",
                        obj->inode, obj->stripe, block_num, block_offset);
                    if (abort_on_corruption)
                        abort();
                    goto skip_unseen;
                }
                offsets_seen[block_offset+region_marker] = (verify_offset_t){ .start = block_offset, .handled = true };
            }
            if (!obj->write_pos)
            {
                fprintf(stderr, "Warning: Object %jx:%jx in metadata block %u at %u does not contain writes, skipping\n",
                    obj->inode, obj->stripe, block_num, block_offset);
                if (abort_on_corruption)
                    abort();
                goto skip_unseen;
            }
            // Verify write chain
            if (obj->write_pos < -(int16_t)block_offset || obj->write_pos > (int16_t)(dsk->meta_block_size-block_offset-sizeof(heap_small_write_t)))
            {
                fprintf(stderr, "Warning: Object %jx:%jx in metadata block %u at %u write offset (%d) exceeds block boundaries, skipping object\n",
                    obj->inode, obj->stripe, block_num, block_offset, obj->write_pos);
                goto skip_corrupted;
            }
            if (obj->write_pos < 0 && obj->write_pos > -sizeof(heap_small_write_t) ||
                obj->write_pos > 0 && obj->write_pos < sizeof(heap_object_t))
            {
                fprintf(stderr, "Warning: Object %jx:%jx in metadata block %u at %u write offset (%d) intersects the object itself, skipping object\n",
                    obj->inode, obj->stripe, block_num, block_offset, obj->write_pos);
                goto skip_corrupted;
            }
            uint32_t wr_i = 0;
            skip_erase_wr = obj->get_writes();
            for (auto wr = obj->get_writes(); wr; wr = wr->next(), wr_i++)
            {
                uint32_t wr_pos = ((uint8_t*)wr - buf - buf_offset);
                if ((wr->entry_type & BS_HEAP_TYPE) < BS_HEAP_SMALL_WRITE ||
                    (wr->entry_type & BS_HEAP_TYPE) > BS_HEAP_INTENT_WRITE ||
                    (wr->entry_type & ~(BS_HEAP_TYPE|BS_HEAP_STABLE)))
                {
                    fprintf(stderr, "Warning: Object %jx:%jx in metadata block %u at %u list entry #%u at %u type %u is invalid, skipping object\n",
                        obj->inode, obj->stripe, block_num, block_offset, wr_i, wr_pos, entry_type);
                    goto skip_corrupted;
                }
                if (wr->size != wr->get_size(this))
                {
                    fprintf(stderr, "Warning: Object %jx:%jx in metadata block %u at %u list entry #%u at %u size is invalid: %u instead of %u, skipping object\n",
                        obj->inode, obj->stripe, block_num, block_offset, wr_i, wr_pos, wr->size, wr->get_size(this));
                    goto skip_corrupted;
                }
                auto offset_it = offsets_seen.upper_bound(wr_pos);
                if (offset_it != offsets_seen.end() && offset_it->second.start < wr_pos+wr->size && offset_it->second.handled)
                {
                    fprintf(stderr, "Warning: Object %jx:%jx in metadata block %u at %u list entry #%u (%u..%u) intersects with other entries (%u..%u) or is double-claimed, skipping object\n",
                        obj->inode, obj->stripe, block_num, block_offset, wr_i, wr_pos, wr_pos+wr->size, offset_it->second.start, offset_it->first);
                    goto skip_corrupted;
                }
                if (wr->next_pos < -(int16_t)wr_pos || wr->next_pos > (int16_t)(dsk->meta_block_size - wr_pos))
                {
                    fprintf(stderr, "Warning: Object %jx:%jx in metadata block %u at %u list entry #%u at %u next item offset (%d) exceeds block boundaries, skipping object\n",
                        obj->inode, obj->stripe, block_num, block_offset, wr_i, wr_pos, wr->next_pos);
                    goto skip_corrupted;
                }
                offsets_seen[wr_pos+wr->size] = (verify_offset_t){ .start = wr_pos, .handled = true };
                skip_erase_to = wr->next();
            }
            // Check for duplicates
            auto oid = (object_id){ .inode = obj->inode, .stripe = obj->stripe };
            uint32_t dup_block;
            heap_object_t *dup_obj = read_entry(oid, &dup_block);
            if (dup_obj != NULL)
            {
                uint64_t lsn = 0;
                for (auto wr = obj->get_writes(); wr; wr = wr->next())
                {
                    lsn = lsn < wr->lsn ? wr->lsn : lsn;
                }
                uint64_t dup_lsn = 0;
                for (auto wr = dup_obj->get_writes(); wr; wr = wr->next())
                {
                    dup_lsn = dup_lsn < wr->lsn ? wr->lsn : dup_lsn;
                }
                if (dup_lsn >= lsn)
                {
                    // Object is duplicated on disk
                    fprintf(stderr, "Warning: Object %jx:%jx in metadata block %u at %u is an older duplicate (lsn %lu <= %lu), skipping\n",
                        obj->inode, obj->stripe, block_num, block_offset, dup_lsn, lsn);
                    goto skip_object;
                }
                else
                {
                    fprintf(stderr, "Warning: Object %jx:%jx in metadata block %u at %u is a newer duplicate (lsn %lu < %lu), overriding\n",
                        obj->inode, obj->stripe, block_num, block_offset, dup_lsn, lsn);
                    init_erase(dup_block, dup_obj);
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
                if (wr->type() == BS_HEAP_SMALL_WRITE &&
                    !is_buffer_area_free(wr->small().location, wr->small().len))
                {
                    fprintf(stderr, "Error: write %jx:%jx v%lu (l%lu) buffered data overlaps with other writes, skipping object\n",
                        obj->inode, obj->stripe, wr->version, wr->lsn);
                    if (abort_on_overlap)
                        abort();
                    goto skip_object;
                }
                if (wr->type() == BS_HEAP_BIG_WRITE && is_data_used(wr->big().location))
                {
                    fprintf(stderr, "Error: write %jx:%jx v%lu (l%lu) data overlaps with other writes, skipping object\n",
                        obj->inode, obj->stripe, wr->version, wr->lsn);
                    if (abort_on_overlap)
                        abort();
                    goto skip_object;
                }
                if (wr->needs_recheck(this))
                {
                    if (!buffer_area || wr->type() == BS_HEAP_INTENT_WRITE)
                    {
                        to_recheck = true;
                    }
                    // recheck small write data immediately
                    else if (!calc_checksums(wr, buffer_area + wr->small().location, false))
                    {
                        // entry is invalid (not fully written before OSD crash) - remove it and all newer (previous) entries too
                        if (wr->type() == BS_HEAP_INTENT_WRITE &&
                            wr->next() && wr->next()->type() == BS_HEAP_BIG_WRITE &&
                            wr->next()->version == wr->version)
                        {
                            // BIG_WRITE+INTENT_WRITE pair
                            wr = wr->next();
                            wr_i++;
                        }
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
            if (to_recheck)
            {
                recheck_queue.push_back(oid);
            }
            handle_object(obj);
            block_offset += obj->size;
        }
        for (auto & op: offsets_seen)
        {
            if (!op.second.handled)
            {
                uint16_t & region_marker = *(uint16_t*)(buf + buf_offset + op.second.start);
                region_marker |= FREE_SPACE_BIT;
            }
        }
        handle_block(block_num, block_offset, buf+buf_offset);
    }
}

uint64_t blockstore_heap_t::load_blocks(uint64_t disk_offset, uint64_t size, uint8_t *buf)
{
    uint64_t entries_loaded = 0;
    uint32_t used_space = 0;
    read_blocks(disk_offset, size, buf, [&](heap_object_t *obj)
    {
        // Allocate space
        used_space += obj->size;
        uint32_t wr_i = 0;
        for (auto wr = obj->get_writes(); wr; wr = wr->next(), wr_i++)
        {
            used_space += wr->size;
            if (wr->type() == BS_HEAP_SMALL_WRITE)
            {
                use_buffer_area(obj->inode, wr->small().location, wr->small().len);
            }
            else if (wr->type() == BS_HEAP_BIG_WRITE)
            {
                // Mark data block as used
                use_data(obj->inode, wr->big().location);
            }
            if (wr->lsn > this->compacted_lsn)
            {
                tmp_compact_queue.push_back((tmp_compact_item_t){
                    .oid = (object_id){ .inode = obj->inode, .stripe = obj->stripe },
                    .lsn = wr->lsn,
                    .compact = wr->needs_compact(this),
                });
            }
            if (wr->lsn > next_lsn)
            {
                next_lsn = wr->lsn;
            }
        }
        // maps anyway store std::pair<ui64, ui32>'s of 16 bytes size
        // so we can store block_offset in it too
        block_index[get_pg_id(obj->inode, obj->stripe)][obj->inode][obj->stripe] = (uint8_t*)obj - buf + disk_offset;
        entries_loaded += wr_i;
    }, [&](uint32_t block_num, uint32_t last_offset, uint8_t *buf)
    {
        uint8_t *copy = NULL;
        if (used_space > 0)
        {
            // Do not store free blocks in memory
            copy = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, dsk->meta_block_size);
            memcpy(copy, buf, last_offset);
            memset(copy+last_offset, 0, dsk->meta_block_size-last_offset);
            if (last_offset <= dsk->meta_block_size-2)
                *(uint16_t*)(copy+last_offset) = FREE_SPACE_BIT | (dsk->meta_block_size-last_offset);
        }
        block_info[block_num] = {
            .used_space = 0,
            .data = copy,
        };
        if (used_space > 0)
        {
            add_used_space(block_num, used_space);
        }
        used_space = 0;
    });
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

bool blockstore_heap_t::calc_checksums(heap_write_t *wr, uint8_t *data, bool set, uint32_t offset, uint32_t len)
{
    if (!dsk->csum_block_size)
    {
        if (wr->type() == BS_HEAP_BIG_WRITE)
        {
            return true;
        }
        // Single checksum
        uint32_t *wr_csum = wr->get_checksum(this);
        if (!wr_csum)
        {
            return true;
        }
        uint32_t real_csum = crc32c(0, data, wr->small().len);
        if (set)
        {
            *wr_csum = real_csum;
            return true;
        }
        return ((*wr_csum) == real_csum);
    }
    if (wr->type() == BS_HEAP_BIG_WRITE)
    {
        return calc_block_checksums((uint32_t*)(wr->get_checksums(this) + offset/dsk->csum_block_size * (dsk->data_csum_type & 0xFF)),
            data, wr->get_int_bitmap(this), offset, offset+len, set, NULL);
    }
    return calc_block_checksums((uint32_t*)wr->get_checksums(this), data, NULL,
        wr->small().offset, wr->small().offset+wr->small().len, set, NULL);
}

bool blockstore_heap_t::calc_block_checksums(uint32_t *block_csums, uint8_t *data, uint8_t *bitmap, uint32_t start, uint32_t end,
    bool set, std::function<void(uint32_t, uint32_t, uint32_t)> bad_block_cb)
{
    return calc_block_checksums(block_csums, bitmap, start, end, [&](uint32_t pos, uint32_t & len)
    {
        len = UINT32_MAX;
        return data+pos-start;
    }, set, bad_block_cb);
}

static uint32_t crc32c_iter(uint32_t prev_crc, const std::function<uint8_t*(uint32_t start, uint32_t & len)> & next, uint32_t pos, uint32_t size)
{
    uint32_t cur_len = 0;
    while (size > 0)
    {
        uint8_t *data = next(pos, cur_len);
        assert(data);
        cur_len = (cur_len < size ? cur_len : size);
        prev_crc = crc32c(prev_crc, data, cur_len);
        pos += cur_len;
        size -= cur_len;
    }
    return prev_crc;
}

bool blockstore_heap_t::calc_block_checksums(uint32_t *block_csums, uint8_t *bitmap,
    uint32_t start, uint32_t end, std::function<uint8_t*(uint32_t start, uint32_t & len)> next,
    bool set, std::function<void(uint32_t, uint32_t, uint32_t)> bad_block_cb)
{
    bool res = true;
    uint32_t pos = start;
    uint32_t block_end = (start/dsk->csum_block_size + 1)*dsk->csum_block_size;
    uint32_t block_crc = 0;
    bool isset = false;
    while (pos < end)
    {
        uint32_t blk_start = pos;
        if (bitmap)
        {
            uint32_t prev = pos;
            while (pos < end && pos < block_end)
            {
                while (pos < end && pos < block_end && !(bitmap[pos/dsk->bitmap_granularity/8] & (1 << ((pos/dsk->bitmap_granularity) % 8))))
                    pos += dsk->bitmap_granularity;
                // zero padding at the beginning or at the end of the block is not counted
                if (pos > prev && prev > 0 && pos < block_end)
                    block_crc = crc32c_pad(block_crc, NULL, 0, pos-prev, 0);
                prev = pos;
                while (pos < end && pos < block_end && (bitmap[pos/dsk->bitmap_granularity/8] & (1 << ((pos/dsk->bitmap_granularity) % 8))))
                    pos += dsk->bitmap_granularity;
                if (pos > prev)
                {
                    isset = true;
                    block_crc = crc32c_iter(block_crc, next, prev, pos-prev);
                }
                prev = pos;
            }
        }
        else
        {
            block_crc = crc32c_iter(block_crc, next, pos, (end > block_end ? block_end : end)-pos);
            pos = (end > block_end ? block_end : end);
            isset = true;
        }
        if (set)
        {
            *block_csums = block_crc;
        }
        else if (isset && block_crc != *block_csums)
        {
            if (bad_block_cb)
            {
                bad_block_cb(blk_start, *block_csums, block_crc);
                res = false;
            }
            else
                return false;
        }
        block_end += dsk->csum_block_size;
        block_crc = 0;
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
                bool is_intent = wr->type() == BS_HEAP_INTENT_WRITE;
                uint64_t loc = wr->small().location;
                if (is_intent)
                {
                    auto next_wr = wr->next();
                    assert(next_wr && next_wr->entry_type == (BS_HEAP_BIG_WRITE | (wr->entry_type & BS_HEAP_STABLE)));
                    loc = wr->small().offset + next_wr->big().location;
                }
                recheck_in_progress++;
                uint8_t *buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, wr->small().len);
                if (log_level > 5)
                {
                    fprintf(stderr, "Notice: rechecking %u bytes at %ju in %s area (lsn %lu)\n",
                        wr->small().len, loc, is_intent ? "data" : "buffer", wr->lsn);
                }
                recheck_cb(is_intent, loc, wr->small().len, buf, [this, oid, lsn = wr->lsn, buf]()
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
                            if (wr->type() == BS_HEAP_INTENT_WRITE &&
                                wr->next() && wr->next()->type() == BS_HEAP_BIG_WRITE &&
                                wr->next()->version == wr->version)
                            {
                                // BIG_WRITE+INTENT_WRITE pair
                                wr = wr->next();
                                wr_i++;
                            }
                            // Erase all writes to the object from this one to the newest
                            if (!wr->next_pos)
                            {
                                fprintf(stderr, "Notice: the whole object %jx:%jx only has unfinished writes, rolling back\n",
                                    obj->inode, obj->stripe);
                                init_erase(block_num, obj);
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
    uint32_t old_pg_count = !pool_settings.pg_count ? 1 : pool_settings.pg_count;
    uint64_t pool_id = (uint64_t)pool;
    heap_block_index_t new_shards;
    for (uint32_t pg_num = 0; pg_num <= old_pg_count; pg_num++)
    {
        auto sh_it = block_index.find((pool_id << (64-POOL_ID_BITS)) | pg_num);
        if (sh_it == block_index.end())
        {
            continue;
        }
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
        block_index.erase(sh_it);
    }
    for (auto sh_it = new_shards.begin(); sh_it != new_shards.end(); sh_it++)
    {
        block_index[sh_it->first] = std::move(sh_it->second);
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
    heap_mvcc_copy_id_t mvcc_id = { .oid = { .inode = oid.inode, .stripe = oid.stripe }, .copy_id = 1 };
    auto mvcc_it = object_mvcc.find(mvcc_id);
    if (mvcc_it != object_mvcc.end())
    {
        while (true)
        {
            mvcc_id.copy_id++;
            auto next_it = object_mvcc.find(mvcc_id);
            if (next_it == object_mvcc.end())
            {
                mvcc_id.copy_id--;
                break;
            }
            mvcc_it = next_it;
        }
        if (mvcc_it->second.entry_copy)
        {
            // Already modified, need to create another copy
            mvcc_id.copy_id++;
            object_mvcc[mvcc_id] = (heap_object_mvcc_t){ .readers = 1 };
        }
        else
        {
            mvcc_it->second.readers++;
        }
    }
    else
    {
        object_mvcc[mvcc_id] = (heap_object_mvcc_t){ .readers = 1 };
    }
    copy_id = mvcc_id.copy_id;
    return obj;
}

heap_object_t *blockstore_heap_t::read_locked_entry(object_id oid, uint64_t copy_id)
{
    auto mvcc_it = object_mvcc.find((heap_mvcc_copy_id_t){ .oid = oid, .copy_id = copy_id });
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

void blockstore_heap_t::free_mvcc(heap_mvcc_map_t::iterator mvcc_it)
{
    if (mvcc_it->second.entry_copy)
    {
        // Free refcounted data & buffer blocks
        heap_object_t *obj = (heap_object_t*)mvcc_it->second.entry_copy;
        free_object_space(obj->inode, obj->get_writes(), NULL, BS_HEAP_FREE_MVCC);
        free(mvcc_it->second.entry_copy);
    }
    object_mvcc.erase(mvcc_it);
}

bool blockstore_heap_t::unlock_entry(object_id oid, uint64_t copy_id)
{
    auto mvcc_id = (heap_mvcc_copy_id_t){ .oid = oid, .copy_id = copy_id };
    auto mvcc_it = object_mvcc.find(mvcc_id);
    if (mvcc_it == object_mvcc.end())
    {
        return false;
    }
    mvcc_it->second.readers--;
    if (!mvcc_it->second.readers)
    {
        mvcc_id.copy_id++;
        if (object_mvcc.find(mvcc_id) != object_mvcc.end())
        {
            // Next entry isn't freed yet
            return true;
        }
        mvcc_id.copy_id--;
        // Free this entry
        free_mvcc(mvcc_it);
        // Free all previous entries with 0 refcount
        while (mvcc_id.copy_id > 1)
        {
            mvcc_id.copy_id--;
            mvcc_it = object_mvcc.find(mvcc_id);
            assert(mvcc_it != object_mvcc.end());
            if (mvcc_it->second.readers)
            {
                mvcc_id.copy_id++;
                break;
            }
            free_mvcc(mvcc_it);
        }
        if (mvcc_id.copy_id == 1)
        {
            // Free data references from the newest object version when the last MVCC is freed
            heap_object_t *new_obj = read_entry(oid, NULL);
            if (new_obj)
            {
                free_object_space(new_obj->inode, new_obj->get_writes(), NULL, BS_HEAP_FREE_MAIN);
            }
        }
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
            if (!*begin_wr)
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
            // all subsequent small write entries must also be compacted
            assert(compacted_wr_count == 1 || wr->is_allowed_before_compacted(compact_lsn, is_last));
            if (wr->type() == BS_HEAP_BIG_WRITE)
            {
                big_wr = wr;
            }
            else if (!new_csums && !wr->can_be_collapsed(this))
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
    memcpy(big_wr->get_ext_bitmap(this), compacted_wrs[0]->get_ext_bitmap(this), dsk->clean_entry_bitmap_size);
    uint8_t *int_bmp = big_wr->get_int_bitmap(this);
    uint8_t *csums = big_wr->get_checksums(this);
    const uint32_t csum_size = (dsk->data_csum_type & 0xFF);
    for (int i = compacted_wr_count-1; i >= 0; i--)
    {
        auto cur_wr = compacted_wrs[i];
        assert(cur_wr->type() == BS_HEAP_SMALL_WRITE || cur_wr->type() == BS_HEAP_INTENT_WRITE);
        bitmap_set(int_bmp, cur_wr->small().offset, cur_wr->small().len, dsk->bitmap_granularity);
        // copy checksums
        if (csums && !skip_csums && !new_csums)
        {
            assert(i == compacted_wr_count-1 ||
                (cur_wr->small().offset % dsk->csum_block_size) == 0 &&
                (cur_wr->small().len % dsk->csum_block_size) == 0);
            memcpy(csums + cur_wr->small().offset/dsk->csum_block_size*csum_size,
                cur_wr->get_checksums(this), cur_wr->small().len/dsk->csum_block_size*csum_size);
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

void blockstore_heap_t::defragment_block(uint32_t block_num)
{
    auto & inf = block_info[block_num];
    assert(inf.data);
    uint8_t *new_data = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, dsk->meta_block_size);
    const uint8_t *end = inf.data+dsk->meta_block_size;
    const uint8_t *new_end = new_data+dsk->meta_block_size;
    uint8_t *old = inf.data;
    uint8_t *cur = new_data;
    while (old <= end-2)
    {
        uint16_t region_marker = *((uint16_t*)old);
        if (region_marker & FREE_SPACE_BIT)
        {
            // free space
            old += (region_marker & ~FREE_SPACE_BIT);
            continue;
        }
        if (old[heap_entry_type_pos] != BS_HEAP_OBJECT)
        {
            // heap_write_t, skip
            old += region_marker;
            continue;
        }
        // object header
        heap_object_t *obj = (heap_object_t *)old;
        assert(obj->size == sizeof(heap_object_t));
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

int blockstore_heap_t::get_block_for_new_object(uint32_t & out_block_num, uint32_t size)
{
    if (!size)
        size = sizeof(heap_object_t)+get_max_write_entry_size();
    uint32_t maxfull = dsk->meta_block_size/MIN_ALLOC - (size+MIN_ALLOC-1)/MIN_ALLOC;
    uint32_t nearfull = dsk->meta_block_size/MIN_ALLOC - target_block_free_space/MIN_ALLOC;
    if (nearfull > maxfull)
        nearfull = maxfull;
    for (int i = 1; i < nearfull; i++)
    {
        out_block_num = meta_alloc->find(i);
        if (out_block_num != UINT32_MAX)
            return 0;
    }
    out_block_num = meta_alloc->find(0);
    if (out_block_num != UINT32_MAX)
        return 0;
    for (int i = nearfull; i <= maxfull; i++)
    {
        out_block_num = meta_alloc->find(i);
        if (out_block_num != UINT32_MAX)
            return 0;
    }
    return ENOSPC;
}

uint32_t blockstore_heap_t::find_block_run(heap_block_info_t & inf, uint32_t space)
{
    uint8_t *data = inf.data + inf.free_pos;
    uint8_t *end = inf.data + dsk->meta_block_size;
    uint8_t *last_free = NULL;
    while (data <= end-2)
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
                inf.free_pos = last_free-inf.data;
            }
            uint16_t region_size = *((uint16_t*)last_free) & ~FREE_SPACE_BIT;
            assert(last_free-inf.data+region_size <= dsk->meta_block_size);
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

uint32_t blockstore_heap_t::block_has_compactable(uint8_t *data)
{
    uint32_t sum = 0;
    uint8_t *end = data + dsk->meta_block_size;
    while (data < end)
    {
        uint16_t region_marker = *((uint16_t*)data);
        assert(region_marker);
        if (!(region_marker & FREE_SPACE_BIT) &&
            data[heap_entry_type_pos] != BS_HEAP_OBJECT)
        {
            heap_write_t *wr = (heap_write_t*)data;
            if (wr->entry_type == (BS_HEAP_SMALL_WRITE|BS_HEAP_STABLE) ||
                wr->entry_type == (BS_HEAP_INTENT_WRITE|BS_HEAP_STABLE))
            {
                // May be freed in the future
                sum += wr->size;
            }
        }
        data += (region_marker & ~FREE_SPACE_BIT);
    }
    return sum;
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
    defragment_block(block_num);
    return find_block_run(inf, space);
}

void blockstore_heap_t::allocate_block(heap_block_info_t & inf)
{
    if (!inf.data)
    {
        inf.data = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, dsk->meta_block_size);
        memset(inf.data, 0, dsk->meta_block_size);
        *((uint16_t*)inf.data) = FREE_SPACE_BIT | dsk->meta_block_size;
    }
}

int blockstore_heap_t::allocate_new_object(object_id oid, uint32_t full_object_size, uint32_t *modified_block, heap_object_t **new_obj)
{
    uint32_t block_num = 0;
    // Allocate block (always leave at least <max_write_entry_size> free_space in the block)
    int res = get_block_for_new_object(block_num, full_object_size+max_write_entry_size);
    if (res != 0)
    {
        return res;
    }
    auto & inf = block_info.at(block_num);
    allocate_block(inf);
    if (modified_block)
    {
        *modified_block = block_num;
    }
    const uint32_t offset = find_block_space(block_num, full_object_size);
    if (offset == UINT32_MAX)
    {
        return ENOSPC;
    }
    add_used_space(block_num, full_object_size);
    block_index[get_pg_id(oid.inode, oid.stripe)][oid.inode][oid.stripe] = (uint64_t)block_num*dsk->meta_block_size + offset;
    *new_obj = (heap_object_t *)(inf.data + offset);
    return 0;
}

int blockstore_heap_t::add_object(object_id oid, heap_write_t *wr, uint32_t *modified_block)
{
    // By now, initial small_writes are not allowed
    if (wr->type() != BS_HEAP_BIG_WRITE &&
        wr->type() != BS_HEAP_TOMBSTONE)
    {
        return EINVAL;
    }
    const uint32_t wr_size = wr->get_size(this);
    heap_object_t *new_obj = NULL;
    int res = allocate_new_object(oid, sizeof(heap_object_t)+wr_size, modified_block, &new_obj);
    if (res != 0)
    {
        return res;
    }
    // Fill the object entry
    new_obj->size = sizeof(heap_object_t);
    new_obj->write_pos = sizeof(heap_object_t);
    new_obj->entry_type = BS_HEAP_OBJECT;
    new_obj->inode = oid.inode;
    new_obj->stripe = oid.stripe;
    heap_write_t *new_wr = new_obj->get_writes();
    memcpy(new_wr, wr, wr_size);
    new_wr->next_pos = 0;
    new_wr->size = wr_size;
    new_wr->lsn = ++next_lsn;
    wr->lsn = new_wr->lsn;
    push_inflight_lsn(oid, new_wr->lsn, new_wr->needs_compact(this) ? HEAP_INFLIGHT_COMPACTABLE : 0);
    new_obj->crc32c = new_obj->calc_crc32c();
    return 0;
}

int blockstore_heap_t::copy_object(heap_object_t *obj, uint32_t *modified_block)
{
    // Allocate block (always leave at least <max_write_entry_size> free_space in the block)
    auto oid = (object_id){ .inode = obj->inode, .stripe = obj->stripe };
    assert(!read_entry(oid, NULL));
    uint32_t full_object_size = obj->size;
    for (auto wr = obj->get_writes(); wr; wr = wr->next())
    {
        full_object_size += wr->size;
    }
    heap_object_t *new_obj = NULL;
    int res = allocate_new_object(oid, full_object_size, modified_block, &new_obj);
    if (res != 0)
    {
        return res;
    }
    copy_full_object((uint8_t*)new_obj, obj);
    new_obj->crc32c = new_obj->calc_crc32c();
    return 0;
}

bool blockstore_heap_t::mvcc_check_tracking(object_id oid)
{
    auto mvcc_it = object_mvcc.find((heap_mvcc_copy_id_t){ .oid = oid, .copy_id = 1 });
    if (mvcc_it == object_mvcc.end())
    {
        // no copies
        return false;
    }
    return mvcc_it->second.entry_copy;
}

void blockstore_heap_t::copy_full_object(uint8_t *dst, heap_object_t *obj)
{
    memcpy(dst, obj, sizeof(heap_object_t));
    ((heap_object_t*)dst)->write_pos = obj->size;
    dst += obj->size;
    for (auto wr = obj->get_writes(); wr; wr = wr->next())
    {
        memcpy(dst, wr, wr->size);
        if (wr->next_pos)
            ((heap_write_t*)dst)->next_pos = wr->size;
        dst += wr->size;
    }
}

// returns tracking_active, i.e. true if there exists at least one copied MVCC version of the object
// it's used for reference tracking because tracking_active=false means that there is 1 implicit reference
// for heap_writes of the current version of the object and true means that there isn't
bool blockstore_heap_t::mvcc_save_copy(heap_object_t *obj)
{
    auto oid = (object_id){ .inode = obj->inode, .stripe = obj->stripe };
    auto mvcc_id = (heap_mvcc_copy_id_t){ .oid = oid, .copy_id = 1 };
    auto mvcc_it = object_mvcc.find(mvcc_id);
    if (mvcc_it == object_mvcc.end())
    {
        // no copies
        return false;
    }
    while (true)
    {
        mvcc_id.copy_id++;
        auto next_it = object_mvcc.find(mvcc_id);
        if (next_it == object_mvcc.end())
        {
            mvcc_id.copy_id--;
            break;
        }
        mvcc_it = next_it;
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
    copy_full_object((uint8_t*)obj_copy, obj);
    mvcc_it->second.entry_copy = obj_copy;
    uint32_t add_ref = 1;
    bool for_obj = false;
    // save_copy is performed when the object is modified, so object_mvcc may only
    // contain 1 version with entry_copy == NULL
    if (mvcc_id.copy_id == 1)
    {
        // Init refcounts for the copy and for the object itself, when it's the first MVCC entry
        add_ref = 2;
        for_obj = true;
    }
    for (auto wr = obj->get_writes(); wr; wr = wr->next())
    {
        if (wr->type() == BS_HEAP_BIG_WRITE)
        {
            mvcc_data_refs[wr->big().location] += add_ref;
            if (wr->entry_type & BS_HEAP_STABLE)
            {
                if (!for_obj)
                {
                    break;
                }
                add_ref = 1;
            }
        }
        else if (wr->type() == BS_HEAP_SMALL_WRITE && wr->small().len > 0)
        {
            mvcc_buffer_refs[wr->small().location] += add_ref;
        }
    }
    // copied :-)
    return true;
}

void blockstore_heap_t::mark_overwritten(uint64_t over_lsn, uint64_t inode, heap_write_t *wr, heap_write_t *end_wr, bool tracking_active)
{
    while (wr && wr != end_wr)
    {
        if (wr->needs_compact(this))
        {
            mark_lsn_compacted(wr->lsn, true);
        }
        if (wr->type() == BS_HEAP_BIG_WRITE)
        {
            overwrite_ref_queue.push_back((heap_refqi_t){ .lsn = over_lsn, .inode = inode, .location = wr->big().location, .len = 0, .is_data = true });
            mvcc_data_refs[wr->big().location] += !tracking_active;
        }
        else if (wr->type() == BS_HEAP_SMALL_WRITE && wr->small().len > 0)
        {
            overwrite_ref_queue.push_back((heap_refqi_t){ .lsn = over_lsn, .inode = inode, .location = wr->small().location, .len = wr->small().len, .is_data = false });
            mvcc_buffer_refs[wr->small().location] += !tracking_active;
        }
        wr = wr->next();
    }
}

int blockstore_heap_t::update_object(uint32_t block_num, heap_object_t *obj, heap_write_t *wr, uint32_t *modified_block, uint32_t *moved_from_block)
{
    const auto oid = (object_id){ .inode = obj->inode, .stripe = obj->stripe };
    // First some validation
    bool is_overwrite = (wr->entry_type == (BS_HEAP_BIG_WRITE|BS_HEAP_STABLE) || wr->entry_type == (BS_HEAP_TOMBSTONE|BS_HEAP_STABLE));
    auto first_wr = obj->get_writes();
    if (first_wr->type() == BS_HEAP_TOMBSTONE && !is_overwrite)
    {
        // Small overwrites are only allowed over live objects
        return EINVAL;
    }
    if (!(first_wr->entry_type & BS_HEAP_STABLE) && (wr->entry_type & BS_HEAP_STABLE))
    {
        // Stable overwrites are not allowed over unstable
        return EINVAL;
    }
    if (wr->entry_type == BS_HEAP_INTENT_WRITE && (first_wr->entry_type & BS_HEAP_STABLE))
    {
        // Unstable intent writes over stable are not allowed
        return EINVAL;
    }
    if (wr->type() == BS_HEAP_INTENT_WRITE &&
        first_wr->type() == BS_HEAP_INTENT_WRITE &&
        !first_wr->can_be_collapsed(this))
    {
        // Intent writes are not allowed over noncollapsible intent writes
        return EINVAL;
    }
    if (wr->version < first_wr->version)
    {
        // Overwrites with a smaller version are forbidden
        return EINVAL;
    }
    // Then a free space check
    const uint32_t wr_size = wr->get_size(this);
    auto *inf = &block_info.at(block_num);
    assert(inf->data);
    if (inf->used_space+wr_size > dsk->meta_block_size-2)
    {
        // Something in the block has to be compacted
        if (block_has_compactable(inf->data) >= inf->used_space+wr_size-(dsk->meta_block_size-2))
        {
            return EAGAIN;
        }
        // Otherwise, move the object
        uint32_t full_size = obj->size;
        for (auto wr = obj->get_writes(); wr; wr = wr->next())
        {
            full_size += wr->size;
        }
        uint32_t new_block = 0;
        int res = get_block_for_new_object(new_block, full_size+wr_size);
        if (res == ENOSPC)
        {
            return ENOSPC;
        }
        inf = &block_info.at(new_block);
        if (inf->used_space+full_size+wr_size > dsk->meta_block_size-2)
        {
            return ENOSPC;
        }
        allocate_block(*inf);
        if (moved_from_block)
        {
            *moved_from_block = block_num;
        }
        uint32_t new_offset = find_block_space(new_block, full_size);
        assert(new_offset != UINT32_MAX);
        copy_full_object(inf->data + new_offset, obj);
        erase_object(block_num, obj, 0, false);
        block_num = new_block;
        obj = (heap_object_t*)(inf->data + new_offset);
        block_index[get_pg_id(oid.inode, oid.stripe)][oid.inode][oid.stripe] = (uint64_t)new_block*dsk->meta_block_size + new_offset;
        add_used_space(new_block, full_size);
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
        if (wr->type() == BS_HEAP_BIG_WRITE)
        {
            mvcc_data_refs[wr->big().location]++;
        }
        else if (wr->type() == BS_HEAP_SMALL_WRITE && wr->small().len > 0)
        {
            mvcc_buffer_refs[wr->small().location]++;
        }
    }
    const uint8_t *old_data = inf->data;
    const uint32_t offset = find_block_space(block_num, wr_size);
    if (old_data != inf->data)
    {
        obj = read_entry(oid, NULL);
        first_wr = obj->get_writes();
    }
    assert(offset != UINT32_MAX);
    memcpy(inf->data + offset, wr, wr_size);
    heap_write_t *new_wr = (heap_write_t*)(inf->data + offset);
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
    else if (first_wr->type() == BS_HEAP_INTENT_WRITE &&
        first_wr->can_be_collapsed(this))
    {
        auto second_wr = first_wr->next();
        assert(second_wr->type() == BS_HEAP_BIG_WRITE);
        auto first_offset = first_wr->small().offset;
        auto first_len = first_wr->small().len;
        second_wr->version = first_wr->version;
        bitmap_set(second_wr->get_int_bitmap(this), first_offset, first_len, dsk->bitmap_granularity);
        if (dsk->csum_block_size)
        {
            const uint32_t csum_size = (dsk->data_csum_type & 0xFF);
            memcpy(second_wr->get_checksums(this) + first_offset/dsk->csum_block_size*csum_size,
                first_wr->get_checksums(this), first_len/dsk->csum_block_size*csum_size);
        }
        used_delta -= free_writes(first_wr, second_wr);
        new_wr->next_pos = (uint8_t*)second_wr - (uint8_t*)new_wr;
    }
    else
    {
        new_wr->next_pos = ((uint8_t*)obj + obj->write_pos) - (uint8_t*)new_wr;
    }
    wr->lsn = new_wr->lsn;
    push_inflight_lsn(oid, new_wr->lsn, new_wr->needs_compact(this) ? HEAP_INFLIGHT_COMPACTABLE : 0);
    obj->write_pos = offset - ((uint8_t*)obj - inf->data);
    obj->crc32c = obj->calc_crc32c();
    // Change block free space
    add_used_space(block_num, used_delta);
    return 0;
}

int blockstore_heap_t::post_write(object_id oid, heap_write_t *wr, uint32_t *modified_block, uint32_t *moved_from_block)
{
    uint32_t block_num = 0;
    heap_object_t *obj = read_entry(oid, &block_num);
    if (!obj)
    {
        return add_object(oid, wr, modified_block);
    }
    return update_object(block_num, obj, wr, modified_block, moved_from_block);
}

int blockstore_heap_t::post_write(uint32_t & block_num, object_id oid, heap_object_t *obj, heap_write_t *wr, uint32_t *moved_from_block)
{
    if (!obj)
    {
        return add_object(oid, wr, &block_num);
    }
    return update_object(block_num, obj, wr, &block_num, moved_from_block);
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
        if ((wr->entry_type & BS_HEAP_STABLE))
        {
            break;
        }
        else if (wr->version <= version)
        {
            stab_count++;
            unstable_wr = wr;
            if (!unstable_big_wr &&
                (wr->type() == BS_HEAP_BIG_WRITE ||
                wr->type() == BS_HEAP_TOMBSTONE))
            {
                unstable_big_wr = wr;
                break;
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
        if (!(wr->entry_type & BS_HEAP_STABLE) && wr->version <= version)
        {
            wr->entry_type |= BS_HEAP_STABLE;
            wr->lsn = last_lsn--;
            push_inflight_lsn(oid, wr->lsn, wr->needs_compact(this) ? HEAP_INFLIGHT_COMPACTABLE : 0);
        }
    }
    assert(last_lsn == next_lsn-stab_count);
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
    if (wr->version == version && (wr->entry_type & BS_HEAP_STABLE))
    {
        // Already rolled back
        return 0;
    }
    for (; wr && wr->version > version; wr = wr->next())
    {
        if (wr->entry_type & BS_HEAP_STABLE)
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
        assert(data_alloc->get(location / dsk->data_block_size));
        data_alloc->set(location / dsk->data_block_size, false);
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
    assert(len > 0);
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
        if (wr->type() == BS_HEAP_BIG_WRITE)
        {
            deref_data(inode, wr->big().location, mode != BS_HEAP_FREE_MAIN);
            if (mode == BS_HEAP_FREE_MVCC && (wr->entry_type & BS_HEAP_STABLE))
            {
                // Stop at the last visible version
                break;
            }
        }
        else if (wr->type() == BS_HEAP_SMALL_WRITE && wr->small().len > 0)
        {
            deref_buffer(inode, wr->small().location, wr->small().len, mode != BS_HEAP_FREE_MAIN);
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

void blockstore_heap_t::init_erase(uint32_t block_num, heap_object_t *obj)
{
    for (auto wr = obj->get_writes(); wr; wr = wr->next())
    {
        if (wr->needs_compact(this))
            mark_lsn_compacted(wr->lsn, true);
    }
    free_object_space(obj->inode, obj->get_writes(), NULL);
    erase_object(block_num, obj, 0, false);
}

void blockstore_heap_t::erase_object(uint32_t block_num, heap_object_t *obj, uint64_t lsn, bool tracking_active)
{
    // Erase object
    if (lsn > 0)
        mark_overwritten(lsn, obj->inode, obj->get_writes(), NULL, tracking_active);
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
    auto thresh = (dsk->meta_block_size-target_block_free_space) - (dsk->meta_block_size-target_block_free_space)%MIN_ALLOC;
    auto old_used_space = inf.used_space;
    inf.used_space += used_delta;
    meta_alloc->change(block_num,
        dsk->meta_block_size/MIN_ALLOC - (dsk->meta_block_size-old_used_space)/MIN_ALLOC,
        dsk->meta_block_size/MIN_ALLOC - (dsk->meta_block_size-inf.used_space)/MIN_ALLOC);
    if (old_used_space > thresh && inf.used_space <= thresh)
        meta_alloc_count--;
    if (old_used_space <= thresh && inf.used_space > thresh)
        meta_alloc_count++;
}

int blockstore_heap_t::list_objects(uint32_t pg_num, object_id min_oid, object_id max_oid,
    obj_ver_id **result_list, size_t *stable_count, size_t *unstable_count)
{
    obj_ver_id *res = NULL;
    size_t res_size = 0, res_alloc = 0;
    obj_ver_id *unstable = NULL;
    size_t unstable_size = 0, unstable_alloc = 0;
    uint64_t pool_id = (min_oid.inode >> (64-POOL_ID_BITS));
    if (pool_id == 0 || pool_id != (max_oid.inode >> (64-POOL_ID_BITS)))
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
    auto first_it = block_index[pool_pg_id].begin();
    auto last_it = block_index[pool_pg_id].end();
    for (auto inode_it = first_it; inode_it != last_it; inode_it++)
    {
        if (inode_it->first < min_oid.inode || inode_it->first > max_oid.inode)
        {
            continue;
        }
        for (auto & stripe_pair: inode_it->second)
        {
            auto oid = (object_id){ .inode = inode_it->first, .stripe = stripe_pair.first };
            if (oid < min_oid || max_oid < oid)
            {
                continue;
            }
            const uint64_t block_pos = stripe_pair.second;
            const uint32_t block_num = block_pos / dsk->meta_block_size;
            heap_object_t *obj = (heap_object_t*)(block_info[block_num].data + (block_pos % dsk->meta_block_size));
            assert(obj->inode == oid.inode && obj->stripe == oid.stripe);
            uint64_t stable_version = 0;
            auto first_wr = obj->get_writes();
            for (auto wr = first_wr; wr; wr = wr->next())
            {
                if (wr->entry_type & BS_HEAP_STABLE)
                {
                    stable_version = wr->version;
                    break;
                }
                else
                {
                    if (unstable_size >= unstable_alloc)
                    {
                        unstable_alloc = (!unstable_alloc ? 128 : unstable_alloc*2);
                        unstable = (obj_ver_id*)realloc_or_die(unstable, sizeof(obj_ver_id) * unstable_alloc);
                    }
                    unstable[unstable_size++] = (obj_ver_id){ .oid = oid, .version = wr->version };
                }
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
        loc = loc * dsk->data_block_size;
    }
    return loc;
}

bool blockstore_heap_t::is_data_used(uint64_t location)
{
    return data_alloc->get(location / dsk->data_block_size);
}

void blockstore_heap_t::use_data(inode_t inode, uint64_t location)
{
    assert(!data_alloc->get(location / dsk->data_block_size));
    data_alloc->set(location / dsk->data_block_size, true);
    inode_space_stats[inode] += dsk->data_block_size;
    data_used_space += dsk->data_block_size;
}

void blockstore_heap_t::free_data(inode_t inode, uint64_t location)
{
    assert(data_alloc->get(location / dsk->data_block_size));
    data_alloc->set(location / dsk->data_block_size, false);
    inode_space_stats[inode] -= dsk->data_block_size;
    data_used_space -= dsk->data_block_size;
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
    return !size || buffer_alloc->is_free(location / dsk->bitmap_granularity);
}

void blockstore_heap_t::use_buffer_area(inode_t inode, uint64_t location, uint64_t size)
{
    if (!size)
    {
        return;
    }
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
    return inflight_lsn.size() - (next_compact_lsn < first_inflight_lsn ? 0 : next_compact_lsn-first_inflight_lsn);
}

uint32_t blockstore_heap_t::get_to_compact_count()
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

void blockstore_heap_t::set_abort_on_corruption(bool fail)
{
    abort_on_corruption = fail;
}

void blockstore_heap_t::set_abort_on_overlap(bool fail)
{
    abort_on_overlap = fail;
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
        assert(lsn <= completed_lsn);
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
