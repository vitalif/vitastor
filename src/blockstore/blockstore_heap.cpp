// Metadata storage version 3 ("lsm heap")
// Copyright (c) Vitaliy Filippov, 2025+
// License: VNPL-1.1 (see README.md for details)

#include <assert.h>
#include <string.h>
#include <stddef.h>

#include <stdexcept>
#include <algorithm>

#include "blockstore_heap.h"
#include "../util/allocator.h"
#include "../util/crc32c.h"
#include "../util/malloc_or_die.h"

#define BS_HEAP_FREE_MVCC 1
#define BS_HEAP_FREE_MAIN 2
#define FREE_SPACE_BIT 0x8000
#define META_ALLOC_LEVELS 8

#define HEAP_INFLIGHT_DONE 1
#define HEAP_INFLIGHT_COMPACTABLE 2
#define HEAP_INFLIGHT_COMPACTED 4
#define HEAP_INFLIGHT_GC 8

static inline heap_list_item_t *list_item(heap_entry_t *wr)
{
    return (heap_list_item_t*)((uint8_t*)wr - offsetof(struct heap_list_item_t, entry));
}

heap_entry_t *blockstore_heap_t::prev(heap_entry_t *wr)
{
    auto li = list_item(wr);
    return li->prev ? &li->prev->entry : NULL;
}

uint32_t blockstore_heap_t::get_simple_entry_size()
{
    return sizeof(heap_entry_t);
}

uint32_t blockstore_heap_t::get_big_entry_size()
{
    // We always store full checksums for "big" entries to prevent ENOSPC on compaction
    // when (big_write+small_write) are smaller than (compacted big_write)
    // However, we only use part of it related to offset..offset+len
    return sizeof(heap_big_write_t) + dsk->clean_entry_bitmap_size*2 +
        (!dsk->data_csum_type ? 0 : dsk->data_block_size/dsk->csum_block_size * (dsk->data_csum_type & 0xFF));
}

uint32_t blockstore_heap_t::get_small_entry_size(uint32_t offset, uint32_t len)
{
    return sizeof(heap_small_write_t) + dsk->clean_entry_bitmap_size +
        (!dsk->data_csum_type ? 4 : (dsk->data_csum_type & 0xFF) *
            ((offset+len+dsk->csum_block_size-1)/dsk->csum_block_size - offset/dsk->csum_block_size));
}

uint32_t blockstore_heap_t::get_csum_size(heap_entry_t *wr)
{
    if (wr->type() == BS_HEAP_SMALL_WRITE)
    {
        return get_csum_size(wr->type(), wr->small().offset, wr->small().len);
    }
    return get_csum_size(wr->type());
}

uint32_t blockstore_heap_t::get_csum_size(uint32_t entry_type, uint32_t offset, uint32_t len)
{
    if (!dsk->data_csum_type)
    {
        return 0;
    }
    if ((entry_type & BS_HEAP_TYPE) == BS_HEAP_SMALL_WRITE ||
        (entry_type & BS_HEAP_TYPE) == BS_HEAP_INTENT_WRITE)
    {
        return ((dsk->data_csum_type & 0xFF) *
            ((offset+len+dsk->csum_block_size-1)/dsk->csum_block_size - offset/dsk->csum_block_size));
    }
    else if ((entry_type & BS_HEAP_TYPE) == BS_HEAP_BIG_WRITE)
    {
        return (dsk->data_block_size/dsk->csum_block_size * (dsk->data_csum_type & 0xFF));
    }
    return 0;
}

uint32_t heap_entry_t::get_size(blockstore_heap_t *heap)
{
    if (type() == BS_HEAP_BIG_WRITE)
    {
        return heap->get_big_entry_size();
    }
    if (type() == BS_HEAP_SMALL_WRITE || type() == BS_HEAP_INTENT_WRITE)
    {
        return heap->get_small_entry_size(small().offset, small().len);
    }
    return heap->get_simple_entry_size();
}

bool heap_entry_t::is_overwrite()
{
    return ((entry_type & ~BS_HEAP_GARBAGE) == (BS_HEAP_BIG_WRITE|BS_HEAP_STABLE) ||
        (entry_type & ~BS_HEAP_GARBAGE) == (BS_HEAP_DELETE|BS_HEAP_STABLE));
}

bool heap_entry_t::is_compactable()
{
    return !is_overwrite() && (entry_type & BS_HEAP_STABLE) ||
        (entry_type & ~BS_HEAP_GARBAGE) == BS_HEAP_COMMIT ||
        (entry_type & ~BS_HEAP_GARBAGE) == BS_HEAP_ROLLBACK;
}

bool heap_entry_t::is_before(heap_entry_t *other)
{
    return lsn < other->lsn || lsn == other->lsn && !is_overwrite() && other->is_overwrite();
}

bool heap_entry_t::is_garbage()
{
    return (entry_type & BS_HEAP_GARBAGE);
}

void heap_entry_t::set_garbage()
{
    entry_type |= BS_HEAP_GARBAGE;
}

uint8_t *heap_entry_t::get_ext_bitmap(blockstore_heap_t *heap)
{
    if (type() == BS_HEAP_DELETE)
        return NULL;
    return ((uint8_t*)this + (type() == BS_HEAP_BIG_WRITE ? sizeof(heap_big_write_t) : sizeof(heap_small_write_t)));
}

uint8_t *heap_entry_t::get_int_bitmap(blockstore_heap_t *heap)
{
    if (type() != BS_HEAP_BIG_WRITE)
        return NULL;
    return ((uint8_t*)this + (type() == BS_HEAP_BIG_WRITE ? sizeof(heap_big_write_t) : sizeof(heap_small_write_t)) + heap->dsk->clean_entry_bitmap_size);
}

uint8_t *heap_entry_t::get_checksums(blockstore_heap_t *heap)
{
    if (!heap->dsk->csum_block_size)
        return NULL;
    if ((type() == BS_HEAP_SMALL_WRITE || type() == BS_HEAP_INTENT_WRITE) && small().len > 0)
        return ((uint8_t*)this + sizeof(heap_small_write_t) + heap->dsk->clean_entry_bitmap_size);
    if (type() != BS_HEAP_BIG_WRITE)
        return NULL;
    return ((uint8_t*)this + sizeof(heap_big_write_t) + 2*heap->dsk->clean_entry_bitmap_size);
}

uint32_t *heap_entry_t::get_checksum(blockstore_heap_t *heap)
{
    if (heap->dsk->csum_block_size ||
        type() != BS_HEAP_SMALL_WRITE && type() != BS_HEAP_INTENT_WRITE ||
        small().len == 0)
    {
        return NULL;
    }
    return (uint32_t*)((uint8_t*)this + sizeof(heap_small_write_t) + heap->dsk->clean_entry_bitmap_size);
}

uint64_t heap_entry_t::big_location(blockstore_heap_t *heap)
{
    return ((uint64_t)big().block_num) * heap->dsk->data_block_size;
}

void heap_entry_t::set_big_location(blockstore_heap_t *heap, uint64_t location)
{
    assert(!(location % heap->dsk->data_block_size));
    big().block_num = location / heap->dsk->data_block_size;
}

uint32_t heap_entry_t::calc_crc32c()
{
    auto old_crc32c = crc32c;
    crc32c = 0;
    uint32_t res = ::crc32c(0, (uint8_t*)this, size);
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
    big_entry_size(get_big_entry_size())
{
    assert(dsk->meta_block_size < 32768);
    assert(dsk->meta_area_size > 0);
    assert(dsk->journal_len > 0);
    meta_alloc = new multilist_index_t(meta_block_count, META_ALLOC_LEVELS+1, 0);
    block_info.resize(meta_block_count);
    assert(dsk->block_count <= 0xFFFF0000);
    data_alloc = new allocator_t(dsk->block_count);
    buffer_alloc = new multilist_alloc_t(dsk->journal_len / dsk->bitmap_granularity, dsk->data_block_size / dsk->bitmap_granularity - 1);
}

blockstore_heap_t::~blockstore_heap_t()
{
    for (auto & inf: block_info)
    {
        for (auto & entry: inf.entries)
        {
            free(entry);
        }
    }
    block_info.clear();
    object_mvcc.clear();
    delete meta_alloc;
    delete data_alloc;
    delete buffer_alloc;
}

int blockstore_heap_t::read_blocks(uint64_t disk_offset, uint64_t disk_size, uint8_t *buf,
    std::function<void(uint32_t block_num, heap_entry_t* wr)> handle_write, std::function<void(uint32_t, uint32_t, uint8_t*)> handle_block)
{
    for (uint64_t buf_offset = 0; buf_offset < disk_size; buf_offset += dsk->meta_block_size)
    {
        uint32_t block_num = (disk_offset + buf_offset) / dsk->meta_block_size;
        assert(block_num < block_info.size());
        uint32_t block_offset = 0;
        while (block_offset <= dsk->meta_block_size-2)
        {
            uint8_t *data = buf + buf_offset + block_offset;
            heap_entry_t *wr = (heap_entry_t*)data;
            if (!wr->size)
            {
                // Block or the rest of block is apparently empty
                // FIXME: Prevent all-zero blocks
                if (block_offset > 0)
                {
                    wr->size = FREE_SPACE_BIT | (dsk->meta_block_size - block_offset);
                }
                break;
            }
            if ((wr->size & ~FREE_SPACE_BIT) > dsk->meta_block_size-block_offset)
            {
                fprintf(stderr, "Error: entry is too large in metadata block %u at %u (%u > max %u bytes). Metadata is corrupted, aborting\n",
                    block_num, block_offset, (wr->size & ~FREE_SPACE_BIT), dsk->meta_block_size-block_offset);
                return EDOM;
            }
            if (wr->size & FREE_SPACE_BIT)
            {
                // Free space
                block_offset += (wr->size & ~FREE_SPACE_BIT);
                continue;
            }
            if ((wr->entry_type & BS_HEAP_TYPE) < BS_HEAP_BIG_WRITE ||
                (wr->entry_type & BS_HEAP_TYPE) > BS_HEAP_ROLLBACK ||
                (wr->entry_type & ~(BS_HEAP_TYPE|BS_HEAP_STABLE)))
            {
                fprintf(stderr, "Error: entry has unknown type %u in metadata block %u at %u. Metadata is corrupted, aborting\n",
                    wr->entry_type, block_num, block_offset);
                return EDOM;
            }
            if (wr->size != wr->get_size(this))
            {
                fprintf(stderr, "Error: entry %jx:%jx v%ju has invalid size in metadata block %u at %u (%u != expected %u bytes). Metadata is corrupted, aborting\n",
                    wr->inode, wr->stripe, wr->version, block_num, block_offset, wr->size, wr->get_size(this));
                return EDOM;
            }
            // Verify crc
            uint32_t expected_crc32c = wr->calc_crc32c();
            if (wr->crc32c != expected_crc32c)
            {
                fprintf(stderr, "Error: entry %jx:%jx v%ju in metadata block %u at %u is corrupt (crc32c mismatch: expected %08x, got %08x). Metadata is corrupted, aborting\n",
                    wr->inode, wr->stripe, wr->version,
                    block_num, block_offset, expected_crc32c, wr->crc32c);
                return EDOM;
            }
            handle_write(block_num, wr);
            block_offset += wr->size;
        }
        handle_block(block_num, block_offset, buf+buf_offset);
    }
    return 0;
}

int blockstore_heap_t::load_blocks(uint64_t disk_offset, uint64_t size, uint8_t *buf, uint64_t &entries_loaded)
{
    entries_loaded = 0;
    uint32_t used_space = 0;
    return read_blocks(disk_offset, size, buf, [&](uint32_t block_num, heap_entry_t *wr_orig)
    {
        heap_list_item_t *li = (heap_list_item_t*)malloc_or_die(wr_orig->size + sizeof(heap_list_item_t) - sizeof(heap_entry_t));
        li->block_num = block_num;
        li->prev = li->next = NULL;
        memcpy(&li->entry, wr_orig, wr_orig->size);
        auto wr = &li->entry;
        if (wr->lsn > next_lsn)
        {
            next_lsn = wr->lsn;
        }
        entries_loaded++;
        auto & inode_idx = block_index[get_pg_id(wr->inode, wr->stripe)][wr->inode];
        auto & idx = inode_idx[wr->stripe];
        insert_list_item(idx, li);
        if (li->next && (li->next->entry.is_overwrite() || li->next->entry.is_garbage()))
        {
            // Mark <wr> as garbage
            wr->set_garbage();
            block_info[li->block_num].has_garbage = true;
        }
        else if (wr->is_overwrite())
        {
            // Mark all previous entries as garbage
            for (auto prev_li = li->prev; prev_li; prev_li = prev_li->prev)
            {
                prev_li->entry.set_garbage();
                block_info[prev_li->block_num].has_garbage = true; // FIXME modify_alloc
            }
        }
        if (!wr->is_garbage())
        {
            used_space += wr->size;
        }
        auto & inf = block_info[li->block_num];
        if (!inf.entries.size())
        {
            inf.entries.reserve(dsk->meta_block_size / sizeof(heap_entry_t));
        }
        inf.entries.push_back(li);
    }, [&](uint32_t block_num, uint32_t last_offset, uint8_t *buf)
    {
        modify_alloc(block_num, [&](heap_block_info_t & inf)
        {
            inf.used_space = used_space;
        });
        used_space = 0;
    });
}

void blockstore_heap_t::fill_recheck_queue()
{
    for (auto & pgp: block_index)
    {
        for (auto & ip: pgp.second)
        {
            for (auto & op: ip.second)
            {
                auto obj = &op.second.ptr->entry;
                if (obj->type() == BS_HEAP_INTENT_WRITE)
                {
                    // Recheck only the latest intent_write
                    recheck_queue.push_back(obj);
                }
                else
                {
                    // Or a series of small_writes
                    for (auto wr = obj; wr && wr->type() == BS_HEAP_SMALL_WRITE; wr = prev(wr))
                    {
                        if (wr->small().len > 0)
                        {
                            recheck_queue.push_back(wr);
                        }
                    }
                }
            }
        }
    }
}

void blockstore_heap_t::mark_used_blocks()
{
    for (auto & pgp: block_index)
    {
        for (auto & ip: pgp.second)
        {
            for (auto & op: ip.second)
            {
                bool added = false;
                auto wr = &op.second.ptr->entry;
                while (wr)
                {
                    if (wr->type() == BS_HEAP_SMALL_WRITE)
                    {
                        use_buffer_area(wr->inode, wr->small().location, wr->small().len);
                    }
                    else if (wr->type() == BS_HEAP_BIG_WRITE)
                    {
                        use_data(wr->inode, wr->big_location(this));
                    }
                    if (wr->is_compactable() && !added)
                    {
                        compact_queue.push_back((object_id){ .inode = wr->inode, .stripe = wr->stripe });
                        added = true;
                    }
                    wr = prev(wr);
                }
            }
        }
    }
}

void blockstore_heap_t::recheck_buffer(heap_entry_t *cwr, uint8_t *buf)
{
    if (cwr->size & FREE_SPACE_BIT) // FIXME
    {
        // Already freed
        return;
    }
    if (!calc_checksums(cwr, buf, false))
    {
        // write entry is invalid, erase it and all newer entries
        auto & inode_idx = block_index[get_pg_id(cwr->inode, cwr->stripe)][cwr->inode];
        auto li = inode_idx[cwr->stripe].ptr;
        int rolled_back = 0;
        auto free_entry = [&]()
        {
            uint32_t block_num = li->block_num;
            auto prev = li->prev;
            auto wr_size = li->entry.size;
            free(li);
            modify_alloc(block_num, [&](heap_block_info_t & inf)
            {
                inf.used_space -= wr_size;
                for (auto it = inf.entries.begin(); it != inf.entries.end(); it++)
                {
                    if (*it == li)
                    {
                        inf.entries.erase(it);
                        break;
                    }
                }
            });
            recheck_modified_blocks.insert(block_num);
            li = prev;
            rolled_back++;
        };
        while (li && cwr != &li->entry)
        {
            free_entry();
        }
        assert(li);
        if (li->prev)
        {
            fprintf(stderr, "Notice: %u unfinished writes to %jx:%jx v%jx since lsn %ju, rolling back\n",
                rolled_back+1, cwr->inode, cwr->stripe, prev(cwr)->version, prev(cwr)->lsn);
            inode_idx[cwr->stripe].ptr = li->prev;
            li->prev->next = NULL;
        }
        else
        {
            fprintf(stderr, "Notice: the whole object %jx:%jx only has unfinished writes, rolling back\n",
                cwr->inode, cwr->stripe);
            inode_idx.erase(cwr->stripe);
        }
        free_entry();
    }
}

bool blockstore_heap_t::recheck_small_writes(std::function<void(bool is_data, uint64_t offset, uint64_t len, uint8_t* buf, std::function<void()>)> read_buffer, int queue_depth)
{
    if (in_recheck)
    {
        // Recheck already entered
        return false;
    }
    if (!recheck_queue_filled)
    {
        fill_recheck_queue();
    }
    if (read_buffer)
    {
        recheck_cb = read_buffer;
        recheck_queue_depth = queue_depth;
    }
    in_recheck = true;
    while (recheck_queue.size() > 0 && recheck_in_progress < recheck_queue_depth)
    {
        heap_entry_t *wr = recheck_queue.front();
        recheck_queue.pop_front();
        bool is_intent = wr->type() == BS_HEAP_INTENT_WRITE;
        uint64_t loc = wr->small().location;
        if (is_intent)
        {
            auto prev_wr = prev(wr);
            if (!prev_wr || prev_wr->entry_type != (BS_HEAP_BIG_WRITE | (wr->entry_type & BS_HEAP_STABLE)) && prev_wr->entry_type != wr->entry_type)
            {
                fprintf(stderr, "Error: intent_write entry %jx:%jx v%ju l%ju is not written over a big_write\n",
                    wr->inode, wr->stripe, wr->version, wr->lsn);
                exit(1);
            }
            loc = wr->small().offset + prev_wr->big_location(this);
        }
        if (log_level > 5)
        {
            fprintf(stderr, "Notice: rechecking %u bytes at %ju in %s area (lsn %ju)\n",
                wr->small().len, loc, is_intent ? "data" : "buffer", wr->lsn);
        }
        if (!is_intent && buffer_area)
        {
            recheck_buffer(wr, buffer_area+loc);
        }
        else
        {
            recheck_in_progress++;
            uint8_t *buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, wr->small().len);
            recheck_cb(is_intent, loc, wr->small().len, buf, [this, wr, buf]()
            {
                recheck_buffer(wr, buf);
                free(buf);
                recheck_in_progress--;
                recheck_small_writes(NULL, 0);
            });
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

void blockstore_heap_t::finish_load()
{
    if (!marked_used_blocks)
    {
        // We can't mark data/buffers as used before loading and rechecking the whole store, so mark them here
        mark_used_blocks();
        marked_used_blocks = true;
    }
    completed_lsn = next_lsn;
    first_inflight_lsn = next_lsn+1;
    std::sort(compact_queue.begin(), compact_queue.end(), [this](const object_id & a, const object_id & b)
    {
        auto ao = read_entry(a);
        auto bo = read_entry(b);
        return ao->lsn < bo->lsn;
    });
}

bool blockstore_heap_t::calc_checksums(heap_entry_t *wr, uint8_t *data, bool set, uint32_t offset, uint32_t len)
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

heap_entry_t *blockstore_heap_t::lock_and_read_entry(object_id oid)
{
    auto obj = read_entry(oid);
    if (!obj)
    {
        return NULL;
    }
    auto & mvcc = object_mvcc[oid];
    mvcc.readers++;
    return obj;
}

heap_entry_t *blockstore_heap_t::read_locked_entry(object_id oid, uint64_t lsn)
{
    auto obj = read_entry(oid);
    assert(obj);
    for (auto wr = obj; wr; wr = prev(wr))
    {
        if (wr->is_overwrite())
        {
            if (lsn == wr->lsn)
            {
                return obj;
            }
            else
            {
                obj = prev(wr);
            }
        }
    }
    return NULL;
}

bool blockstore_heap_t::unlock_entry(object_id oid)
{
    auto mvcc_it = object_mvcc.find(oid);
    if (mvcc_it == object_mvcc.end())
    {
        return false;
    }
    mvcc_it->second.readers--;
    if (!mvcc_it->second.readers)
    {
        auto garbage_entry = mvcc_it->second.garbage_entry;
        object_mvcc.erase(mvcc_it);
        if (garbage_entry)
        {
            mark_garbage_up_to(garbage_entry);
        }
    }
    return true;
}

heap_entry_t *blockstore_heap_t::read_entry(object_id oid)
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
    return &stripe_it->second.ptr->entry;
}

int blockstore_heap_t::allocate_entry(uint32_t entry_size, uint32_t *block_num, bool allow_last_free)
{
    if (last_allocated_block != UINT32_MAX)
    {
        // First try to write into the same block as the previous time
        auto & inf = block_info.at(last_allocated_block);
        auto free_space = dsk->meta_block_size - inf.used_space;
        if (inf.is_writing || free_space < entry_size /* FIXME edge cases with +2? */ ||
            // Do not allow to make the last non-nearfull block nearfull
            !allow_last_free && meta_nearfull_blocks >= meta_block_count-1 &&
            free_space >= big_entry_size && free_space < big_entry_size+entry_size)
        {
            last_allocated_block = UINT32_MAX;
        }
    }
    if (last_allocated_block == UINT32_MAX)
    {
        int i;
        for (i = 0; last_allocated_block == UINT32_MAX && i < META_ALLOC_LEVELS-1; i++)
        {
            // First try to write into most free blocks
            last_allocated_block = meta_alloc->find(i);
        }
        if (last_allocated_block != UINT32_MAX && i == META_ALLOC_LEVELS-1 && !allow_last_free && meta_nearfull_blocks >= meta_block_count-1)
        {
            // Do not allow to make the last non-nearfull block nearfull
            auto & inf = block_info.at(last_allocated_block);
            auto free_space = dsk->meta_block_size - inf.used_space;
            if (free_space >= big_entry_size && free_space < big_entry_size+entry_size)
            {
                last_allocated_block = UINT32_MAX;
            }
        }
        if (last_allocated_block == UINT32_MAX)
        {
            // Then into nearfull blocks
            for (uint32_t b = meta_alloc->find(META_ALLOC_LEVELS-1); b != UINT32_MAX; b = meta_alloc->next(b))
            {
                auto & inf = block_info.at(b);
                auto free_space = dsk->meta_block_size - inf.used_space;
                if (free_space >= entry_size)
                {
                    last_allocated_block = b;
                    break;
                }
            }
        }
        if (last_allocated_block == UINT32_MAX)
        {
            // Then fail :)
            return ENOSPC;
        }
    }
    if (!allow_last_free && meta_nearfull_blocks >= meta_block_count-1)
    {
        // Do not allow to make the last non-nearfull block nearfull
        auto & inf = block_info.at(last_allocated_block);
        if (dsk->meta_block_size-inf.used_space >= big_entry_size &&
            dsk->meta_block_size-inf.used_space+entry_size < big_entry_size)
        {
            last_allocated_block = UINT32_MAX;
            return ENOSPC;
        }
    }
    // Write into the same block
    auto & inf = block_info.at(last_allocated_block);
    if (inf.has_garbage)
    {
        size_t i = 0, j = 0;
        for (; i < inf.entries.size(); i++)
        {
            if (inf.entries[i]->entry.is_garbage())
            {
                // old entry invalidated by a newer one, mark it as freeable on block write
                // assign a 'virtual' LSN to track GC completion
                assert(!inf.mod_lsn_to || inf.mod_lsn_to == next_lsn);
                uint64_t gc_lsn = ++next_lsn;
                inf.mod_lsn = inf.mod_lsn ? inf.mod_lsn : gc_lsn;
                inf.mod_lsn_to = gc_lsn;
                push_inflight_lsn(gc_lsn, &inf.entries[i]->entry, HEAP_INFLIGHT_GC);
            }
            else if (j != i)
            {
                inf.entries[j++] = inf.entries[i];
            }
        }
        inf.entries.resize(j);
        inf.has_garbage = false;
    }
    *block_num = last_allocated_block;
    modify_alloc(last_allocated_block, [&](heap_block_info_t & inf)
    {
        inf.used_space += entry_size;
    });
    return 0;
}

void blockstore_heap_t::insert_list_item(heap_idx_t & idx, heap_list_item_t *li)
{
    auto old_head = idx.ptr;
    if (old_head && !old_head->entry.is_before(&li->entry))
    {
        // BIG_WRITE may be inserted into the middle of the sequence during compaction
        // and it overrides SMALL_WRITEs and COMMITs with the same LSN
        // However, all entries of other types (say DELETE) override previous ones
        auto prev_li = old_head->prev;
        while (prev_li && !prev_li->entry.is_before(&li->entry))
        {
            prev_li = prev_li->prev;
        }
        // Insert <li> between <old_head> and <prev_li>
        li->prev = prev_li;
        if (prev_li)
            prev_li->next = li;
        old_head->prev = li;
        li->next = old_head;
    }
    else
    {
        li->prev = idx.ptr;
        li->next = NULL;
        if (idx.ptr)
            idx.ptr->next = li;
        idx.ptr = li;
    }
}

int blockstore_heap_t::add_entry(uint32_t wr_size, uint32_t *modified_block,
    bool allow_last_free, std::function<void(heap_entry_t *wr)> fill_entry)
{
    uint32_t block_num;
    int res = allocate_entry(wr_size, &block_num, allow_last_free);
    if (res != 0)
    {
        return res;
    }
    if (modified_block)
    {
        *modified_block = block_num;
    }
    auto li = (heap_list_item_t*)malloc_or_die(wr_size + sizeof(heap_list_item_t) - sizeof(heap_entry_t));
    auto new_wr = &li->entry;
    auto & inf = block_info.at(block_num);
    if (!inf.entries.size())
        inf.entries.reserve(dsk->meta_block_size / sizeof(heap_entry_t));
    inf.entries.push_back(li);
    assert(!inf.mod_lsn_to || inf.mod_lsn_to == next_lsn);
    new_wr->lsn = ++next_lsn;
    fill_entry(new_wr);
    inf.mod_lsn = inf.mod_lsn ? inf.mod_lsn : next_lsn;
    inf.mod_lsn_to = next_lsn;
    // Remember the object as dirty and remove older entries when this block is written and fsynced
    auto oid = (object_id){ .inode = new_wr->inode, .stripe = new_wr->stripe };
    push_inflight_lsn(next_lsn, new_wr,
        (new_wr->is_overwrite() ? HEAP_INFLIGHT_COMPACTED : 0) |
        (new_wr->is_compactable() ? HEAP_INFLIGHT_COMPACTABLE : 0));
    auto & idx = block_index[get_pg_id(oid.inode, oid.stripe)][oid.inode][oid.stripe];
    insert_list_item(idx, li);
    li->block_num = block_num;
    new_wr->size = wr_size;
    new_wr->crc32c = new_wr->calc_crc32c();
    return 0;
}

// 1st step: post a write

int blockstore_heap_t::add_small_write(object_id oid, heap_entry_t *old_head, uint16_t type, uint64_t version,
    uint32_t offset, uint32_t len, uint64_t location, uint8_t *bitmap, uint8_t *data, uint32_t *modified_block)
{
    if (!old_head || old_head->type() == BS_HEAP_DELETE || old_head->version > version ||
        type != (BS_HEAP_SMALL_WRITE|BS_HEAP_STABLE) && type != BS_HEAP_SMALL_WRITE && type != (BS_HEAP_INTENT_WRITE|BS_HEAP_STABLE) ||
        (type & BS_HEAP_STABLE) && !(old_head->entry_type & BS_HEAP_STABLE))
    {
        return EINVAL;
    }
    uint32_t wr_size = get_small_entry_size(offset, len);
    return add_entry(wr_size, modified_block, false, [&](heap_entry_t *wr)
    {
        wr->entry_type = type;
        wr->inode = oid.inode;
        wr->stripe = oid.stripe;
        wr->version = version;
        wr->small().offset = offset;
        wr->small().len = len;
        wr->small().location = location;
        if (bitmap)
            memcpy(wr->get_ext_bitmap(this), bitmap, dsk->clean_entry_bitmap_size);
        else if (old_head)
        {
            old_head = read_entry(oid);
            memcpy(wr->get_ext_bitmap(this), old_head->get_ext_bitmap(this), dsk->clean_entry_bitmap_size);
        }
        else
            memset(wr->get_ext_bitmap(this), 0, dsk->clean_entry_bitmap_size);
        calc_checksums(wr, (uint8_t*)data, true);
    });
}

int blockstore_heap_t::add_big_write(object_id oid, heap_entry_t *old_head, bool stable, uint64_t version,
    uint32_t offset, uint32_t len, uint64_t location, uint8_t *bitmap, uint8_t *data, uint32_t *modified_block)
{
    if (stable && old_head && !(old_head->entry_type & BS_HEAP_STABLE))
    {
        return EINVAL;
    }
    uint32_t wr_size = get_big_entry_size();
    return add_entry(wr_size, modified_block, false, [&](heap_entry_t *wr)
    {
        wr->entry_type = BS_HEAP_BIG_WRITE | (stable ? BS_HEAP_STABLE : 0);
        wr->inode = oid.inode;
        wr->stripe = oid.stripe;
        wr->version = version;
        wr->set_big_location(this, location);
        if (bitmap)
            memcpy(wr->get_ext_bitmap(this), bitmap, dsk->clean_entry_bitmap_size);
        else
            memset(wr->get_ext_bitmap(this), 0, dsk->clean_entry_bitmap_size);
        memset(wr->get_int_bitmap(this), 0, dsk->clean_entry_bitmap_size);
        bitmap_set(wr->get_int_bitmap(this), offset, len, dsk->bitmap_granularity);
        if (dsk->data_csum_type)
        {
            memset(wr->get_checksums(this), 0, get_csum_size(wr));
            calc_checksums(wr, (uint8_t*)data, true, offset, len);
        }
    });
}

int blockstore_heap_t::add_compact(heap_entry_t *obj, uint64_t to_lsn, uint32_t *modified_block, uint8_t *new_csums)
{
    // Slightly tricky - we don't want to compact an object if it's overwritten or deleted during compaction
    {
        heap_entry_t *old_wr = obj;
        while (old_wr && !old_wr->is_overwrite())
        {
            old_wr = prev(old_wr);
        }
        if (!old_wr)
        {
            // Check if we have to remove the object at all
            bool has_entry = false;
            iterate_with_stable(obj, obj->lsn, [&](heap_entry_t *old_wr, bool stable)
            {
                has_entry = true;
                return false;
            });
            if (!has_entry)
            {
                uint64_t compact_lsn = obj->lsn;
                return add_entry(get_simple_entry_size(), modified_block, false, [&](heap_entry_t *wr)
                {
                    wr->entry_type = BS_HEAP_DELETE|BS_HEAP_STABLE;
                    wr->inode = obj->inode;
                    wr->stripe = obj->stripe;
                    wr->version = 0;
                    wr->lsn = compact_lsn;
                });
            }
        }
        else if (old_wr->lsn > to_lsn)
        {
            return ENOENT;
        }
    }
    auto oid = (object_id){ .inode = obj->inode, .stripe = obj->stripe };
    uint32_t wr_size = get_big_entry_size();
    return add_entry(wr_size, modified_block, true, [&](heap_entry_t *new_wr)
    {
        // obj and old_wr are invalid, re-read them - the block could have been compacted
        obj = read_entry(oid);
        while (obj && obj->lsn > to_lsn)
        {
            // skip new entries
            obj = prev(obj);
        }
        assert(obj);
        new_wr->entry_type = BS_HEAP_BIG_WRITE | BS_HEAP_STABLE;
        new_wr->inode = obj->inode;
        new_wr->stripe = obj->stripe;
        memset(new_wr->get_int_bitmap(this), 0, dsk->clean_entry_bitmap_size);
        bool need_copy = false, bitmap_copied = false;
        std::vector<heap_entry_t*> cswr;
        // Determine the latest compacted entry
        uint64_t compact_lsn = obj->lsn, compact_version = obj->version;
        iterate_with_stable(obj, to_lsn, [&](heap_entry_t *old_wr, bool stable)
        {
            if (!stable)
            {
                // This entry is still uncommitted, so it's not compacted and makes a gap
                compact_lsn = old_wr->lsn-1;
                compact_version = prev(old_wr)->version;
            }
            return !old_wr->is_overwrite();
        });
        new_wr->version = compact_version;
        new_wr->lsn = compact_lsn;
        bool found = false;
        iterate_with_stable(obj, compact_lsn, [&](heap_entry_t *old_wr, bool stable)
        {
            if (!stable)
                return true;
            if (old_wr->type() == BS_HEAP_SMALL_WRITE || old_wr->type() == BS_HEAP_INTENT_WRITE)
            {
                if (!bitmap_copied)
                {
                    memcpy(new_wr->get_ext_bitmap(this), old_wr->get_ext_bitmap(this), dsk->clean_entry_bitmap_size);
                    bitmap_copied = true;
                }
                bitmap_set(new_wr->get_int_bitmap(this), old_wr->small().offset, old_wr->small().len, dsk->bitmap_granularity);
                if (dsk->data_csum_type && old_wr->small().len > 0)
                {
                    if (dsk->csum_block_size == dsk->bitmap_granularity)
                        cswr.push_back(old_wr);
                    else
                        need_copy = true;
                }
            }
            else if (old_wr->type() == BS_HEAP_BIG_WRITE)
            {
                found = true;
                new_wr->big().block_num = old_wr->big().block_num;
                mem_or(new_wr->get_int_bitmap(this), old_wr->get_int_bitmap(this), dsk->clean_entry_bitmap_size);
                if (need_copy)
                    memcpy(new_wr->get_checksums(this), new_csums, dsk->data_block_size/dsk->csum_block_size*(dsk->data_csum_type & 0xFF));
                else if (dsk->data_csum_type)
                {
                    // Copy checksums in the reverse order
                    memcpy(new_wr->get_checksums(this), old_wr->get_checksums(this), dsk->data_block_size/dsk->csum_block_size*(dsk->data_csum_type & 0xFF));
                    for (size_t i = cswr.size(); i > 0; i--)
                    {
                        heap_entry_t *old_wr = cswr[i-1];
                        memcpy(new_wr->get_checksums(this) + old_wr->small().offset/dsk->csum_block_size*(dsk->data_csum_type & 0xFF),
                            old_wr->get_checksums(this), old_wr->small().len/dsk->csum_block_size*(dsk->data_csum_type & 0xFF));
                    }
                }
                return false;
            }
            return true;
        });
        assert(found);
    });
}

// A bit of a hack: overwrite the bitmap in an existing entry
int blockstore_heap_t::add_punch_holes(heap_entry_t *obj, uint64_t to_lsn, uint64_t version, uint8_t *new_bitmap, uint8_t *new_csums, uint32_t *modified_block)
{
    assert(dsk->data_csum_type && dsk->csum_block_size > dsk->bitmap_granularity);
    assert(new_csums);
    // Abort if the object is overwritten or deleted during compaction
    heap_entry_t *wr = obj;
    while (wr && wr->lsn != to_lsn && !wr->is_overwrite())
    {
        wr = prev(wr);
    }
    if (!wr || wr->lsn > to_lsn)
    {
        return ENOENT;
    }
    auto & idx = block_index[get_pg_id(obj->inode, obj->stripe)][obj->inode][obj->stripe];
    assert(idx.ptr);
    uint32_t block_num = idx.ptr->block_num;
    auto & inf = block_info.at(block_num);
    if (inf.is_writing)
    {
        return EAGAIN;
    }
    *modified_block = block_num;
    memcpy(wr->get_int_bitmap(this), new_bitmap, dsk->clean_entry_bitmap_size);
    memcpy(wr->get_checksums(this), new_csums, dsk->data_block_size/dsk->csum_block_size*(dsk->data_csum_type & 0xFF));
    return 0;
}

int blockstore_heap_t::add_simple(heap_entry_t *obj, uint64_t version, uint32_t *modified_block, uint32_t entry_type)
{
    uint32_t wr_size = get_simple_entry_size();
    return add_entry(wr_size, modified_block, false, [&](heap_entry_t *wr)
    {
        wr->entry_type = entry_type;
        wr->inode = obj->inode;
        wr->stripe = obj->stripe;
        wr->version = version;
    });
}

int blockstore_heap_t::add_commit(heap_entry_t *obj, uint64_t version, uint32_t *modified_block)
{
    heap_entry_t *wr = obj;
    bool found = false, uncommitted = false;
    uint64_t commit_version = 0;
    while (wr)
    {
        if (wr->type() == BS_HEAP_ROLLBACK)
        {
            auto rollback_version = wr->version;
            wr = prev(wr);
            while (wr->version > rollback_version)
            {
                assert(!(wr->entry_type & BS_HEAP_STABLE));
                wr = prev(wr);
            }
            continue;
        }
        if (wr->type() == BS_HEAP_COMMIT)
        {
            commit_version = wr->version;
            wr = prev(wr);
            continue;
        }
        if (wr->version == version)
        {
            found = true;
            if (!(wr->entry_type & BS_HEAP_STABLE) && wr->version > commit_version)
            {
                uncommitted = true;
            }
            break;
        }
        if (wr->is_overwrite())
        {
            break;
        }
        wr = prev(wr);
    }
    if (!found)
    {
        return ENOENT;
    }
    if (!uncommitted)
    {
        return EBUSY;
    }
    return add_simple(obj, version, modified_block, BS_HEAP_COMMIT);
}

int blockstore_heap_t::add_rollback(heap_entry_t *obj, uint64_t version, uint32_t *modified_block)
{
    heap_entry_t *wr = obj;
    bool found_uncommitted = false;
    uint64_t commit_version = 0;
    while (wr && !wr->is_overwrite())
    {
        if (wr->type() == BS_HEAP_ROLLBACK)
        {
            auto rollback_version = wr->version;
            wr = prev(wr);
            while (wr->version > rollback_version)
            {
                assert(!(wr->entry_type & BS_HEAP_STABLE));
                wr = prev(wr);
            }
            continue;
        }
        if (wr->type() == BS_HEAP_COMMIT)
        {
            commit_version = wr->version;
            wr = prev(wr);
            continue;
        }
        bool stable = (wr->entry_type & BS_HEAP_STABLE) || wr->version <= commit_version;
        if (stable)
        {
            if (wr->version > version)
            {
                return EBUSY;
            }
            else if (wr->version == version)
            {
                break;
            }
            else if (wr->version < version)
            {
                return ENOENT;
            }
        }
        else if (wr->version > version)
        {
            found_uncommitted = true;
        }
        wr = prev(wr);
    }
    if (!found_uncommitted)
    {
        return 0;
    }
    return add_simple(obj, version, modified_block, BS_HEAP_ROLLBACK);
}

int blockstore_heap_t::add_delete(heap_entry_t *obj, uint32_t *modified_block)
{
    assert(obj);
    return add_simple(obj, 0, modified_block, BS_HEAP_DELETE|BS_HEAP_STABLE);
}

// 2nd step: mark the block as being written (to prevent further in-memory updates to it),
// then mark it as written, then mark LSN as fsynced, then compact objects

uint32_t blockstore_heap_t::meta_alloc_pos(const heap_block_info_t & inf)
{
    if (inf.is_writing || inf.used_space > dsk->meta_block_size-sizeof(heap_entry_t))
    {
        // 100% full - no entry can be written into this block at all
        return META_ALLOC_LEVELS;
    }
    if (inf.used_space > dsk->meta_block_size-big_entry_size)
    {
        // nearfull - big_entries won't fit into this block so it can't be used for compaction
        return META_ALLOC_LEVELS-1;
    }
    // normal block
    return inf.used_space / ((dsk->meta_block_size-big_entry_size) / (META_ALLOC_LEVELS-1));
}

void blockstore_heap_t::modify_alloc(uint32_t block_num, std::function<void(heap_block_info_t &)> change_cb)
{
    auto & inf = block_info.at(block_num);
    uint32_t old_pos = meta_alloc_pos(inf);
    uint32_t old_used = inf.used_space;
    change_cb(inf);
    uint32_t new_pos = meta_alloc_pos(inf);
    uint32_t new_used = inf.used_space;
    meta_alloc->change(block_num, old_pos, new_pos);
    meta_used_space -= old_used;
    meta_used_space += new_used;
    if ((old_pos < META_ALLOC_LEVELS-1) != (new_pos < META_ALLOC_LEVELS-1))
    {
        meta_nearfull_blocks += (new_pos >= META_ALLOC_LEVELS-1 ? 1 : -1);
    }
}

void blockstore_heap_t::start_block_write(uint32_t block_num)
{
    modify_alloc(block_num, [&](heap_block_info_t & inf)
    {
        assert(!inf.is_writing);
        inf.is_writing = true;
    });
    // FIXME also get_meta_block
}

void blockstore_heap_t::complete_block_write(uint32_t block_num)
{
    uint64_t mod_lsn = 0, mod_lsn_to = 0;
    modify_alloc(block_num, [&](heap_block_info_t & inf)
    {
        assert(inf.is_writing);
        inf.is_writing = false;
        mod_lsn = inf.mod_lsn;
        mod_lsn_to = inf.mod_lsn_to;
        inf.mod_lsn = 0;
        inf.mod_lsn_to = 0;
    });
    if (mod_lsn)
    {
        auto it = inflight_lsn.begin() + (mod_lsn-first_inflight_lsn);
        for (uint64_t lsn = mod_lsn; lsn <= mod_lsn_to; lsn++, it++)
        {
            assert(!(it->flags & HEAP_INFLIGHT_DONE));
            it->flags |= HEAP_INFLIGHT_DONE;
        }
        mark_completed_lsns();
    }
}

void blockstore_heap_t::mark_garbage_up_to(heap_entry_t *wr)
{
    auto mvcc_it = object_mvcc.find((object_id){ .inode = wr->inode, .stripe = wr->stripe });
    if (mvcc_it != object_mvcc.end())
    {
        // Postpone until all readers complete
        auto & mvcc = mvcc_it->second;
        mvcc.garbage_entry = !mvcc.garbage_entry || mvcc.garbage_entry->lsn < wr->lsn ? wr : mvcc.garbage_entry;
        return;
    }
    assert((wr->type() == BS_HEAP_BIG_WRITE || wr->type() == BS_HEAP_DELETE) && (wr->entry_type & BS_HEAP_STABLE));
    uint32_t used_big = (wr->type() == BS_HEAP_BIG_WRITE ? wr->big().block_num : UINT32_MAX);
    wr = prev(wr);
    while (wr && !wr->is_garbage())
    {
        auto prev_wr = prev(wr);
        mark_garbage(list_item(wr)->block_num, wr, used_big);
        wr = prev_wr;
    }
}

void blockstore_heap_t::mark_garbage(uint32_t block_num, heap_entry_t *prev_wr, uint32_t used_big)
{
    prev_wr->set_garbage();
    // And this is the moment when we can free the data reference
    if (prev_wr->type() == BS_HEAP_SMALL_WRITE && prev_wr->small().len > 0)
    {
        free_buffer_area(prev_wr->inode, prev_wr->small().location, prev_wr->small().len);
    }
    else if (prev_wr->type() == BS_HEAP_BIG_WRITE && prev_wr->big().block_num != used_big)
    {
        free_data(prev_wr->inode, prev_wr->big_location(this));
    }
    if (prev_wr->is_compactable())
    {
        to_compact_count--;
    }
    modify_alloc(block_num, [&](heap_block_info_t & inf)
    {
        inf.used_space -= prev_wr->size;
        inf.has_garbage = true;
    });
}

int blockstore_heap_t::get_next_compact(object_id & oid)
{
    if (!compact_queue.size())
    {
        return ENOENT;
    }
    oid = compact_queue.front();
    compact_queue.pop_front();
    return 0;
}

void blockstore_heap_t::iterate_with_stable(heap_entry_t *obj, uint64_t max_lsn, std::function<bool(heap_entry_t*, bool)> cb)
{
    auto old_wr = obj;
    while (old_wr && old_wr->lsn > max_lsn)
    {
        // skip new entries
        old_wr = prev(old_wr);
    }
    uint64_t commit_version = 0, rollback_version = UINT64_MAX;
    for (; old_wr; old_wr = prev(old_wr))
    {
        if (old_wr->type() == BS_HEAP_ROLLBACK)
        {
            rollback_version = old_wr->version;
        }
        else if (old_wr->type() == BS_HEAP_COMMIT)
        {
            commit_version = old_wr->version;
        }
        else
        {
            // 1) 1 2 3 ROLLBACK(2) COMMIT(3) -> impossible
            // 2) 1 2 3 4 ROLLBACK(3) COMMIT(2) -> OK
            // 3) 1 2 3 ROLLBACK(2) 3 COMMIT(3) -> first 3 shouldn't be treated as stable
            // 4) 1 2 3 COMMIT(3) ROLLBACK(2) -> impossible
            //    I.e. a rollback always has version >= previous commit
            // 5) 1 2 3 4 5 ROLLBACK(4) 5 ROLLBACK(3)
            if (old_wr->version > rollback_version)
            {
                continue;
            }
            auto cont = cb(old_wr, (old_wr->entry_type & BS_HEAP_STABLE) || (old_wr->version <= commit_version));
            if (!cont)
            {
                break;
            }
        }
    }
}

heap_compact_t blockstore_heap_t::iterate_compaction(heap_entry_t *obj, uint64_t fsynced_lsn, bool under_pressure, std::function<void(heap_entry_t*)> small_wr_cb)
{
    heap_compact_t res = {};
    uint64_t commit_version = 0, rollback_version = UINT64_MAX;
    bool has_small = false;
    for (heap_entry_t *wr = obj; wr; wr = prev(wr))
    {
        // 1) 1 2 3 ROLLBACK(2) COMMIT(3) -> impossible
        // 2) 1 2 3 4 ROLLBACK(3) COMMIT(2) -> OK
        // 3) 1 2 3 ROLLBACK(2) 3 COMMIT(3) -> first 3 shouldn't be treated as stable
        // 4) 1 2 3 COMMIT(3) ROLLBACK(2) -> impossible
        //    I.e. a rollback always has version >= previous commit
        // 5) 1 2 3 4 5 ROLLBACK(4) 5 ROLLBACK(3)
        if (wr->type() == BS_HEAP_ROLLBACK)
        {
            if (wr->lsn <= fsynced_lsn && !res.compact_lsn)
            {
                res.compact_lsn = wr->lsn;
                res.compact_version = wr->version;
            }
            rollback_version = wr->version;
            continue;
        }
        if (wr->type() == BS_HEAP_COMMIT)
        {
            if (wr->lsn <= fsynced_lsn && !res.compact_lsn)
            {
                res.compact_lsn = wr->lsn;
                res.compact_version = wr->version;
            }
            commit_version = wr->version;
            continue;
        }
        bool rolled_back = (wr->version > rollback_version);
        bool stable = !rolled_back && ((wr->entry_type & BS_HEAP_STABLE) || (wr->version <= commit_version));
        if (!stable || wr->lsn > fsynced_lsn)
        {
            // Skip unstable or non-fsynced writes
            if (!under_pressure && (wr->type() == BS_HEAP_BIG_WRITE || wr->type() == BS_HEAP_DELETE))
            {
                // We may postpone compaction if we have an unstable overwrite when not under pressure
                res.compact_lsn = 0;
                res.compact_version = 0;
                return res;
            }
            continue;
        }
        if (wr->type() == BS_HEAP_BIG_WRITE)
        {
            // Stable big_write is here
            res.clean_loc = wr->big_location(this);
            res.clean_version = wr->version;
            res.clean_lsn = wr->lsn;
            return res;
        }
        if (wr->type() == BS_HEAP_DELETE)
        {
            // Object is deleted
            assert(!has_small);
            if (wr->entry_type & BS_HEAP_STABLE)
            {
                // Already have the stable bit, no need to generate a compaction entry
                res.compact_lsn = 0;
                res.compact_version = 0;
            }
            return res;
        }
        // We finally have something compactable
        if (!res.compact_lsn)
        {
            res.compact_lsn = wr->lsn;
            res.compact_version = wr->version;
        }
        if (wr->type() == BS_HEAP_SMALL_WRITE || wr->type() == BS_HEAP_INTENT_WRITE)
        {
            has_small = true;
            small_wr_cb(wr);
        }
    }
    return res;
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
            heap_entry_t *obj = &stripe_pair.second.ptr->entry;
            assert(obj->inode == oid.inode && obj->stripe == oid.stripe);
            uint64_t stable_version = 0;
            auto first_wr = obj;
            for (auto wr = first_wr; wr; wr = prev(wr))
            {
                if ((wr->entry_type & BS_HEAP_STABLE) || wr->type() == BS_HEAP_COMMIT || wr->type() == BS_HEAP_ROLLBACK)
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

void blockstore_heap_t::get_meta_block(uint32_t block_num, uint8_t *buffer)
{
    auto & inf = block_info.at(block_num);
    size_t pos = 0;
    for (auto li: inf.entries)
    {
        memcpy(buffer+pos, &li->entry, li->entry.size);
        pos += li->entry.size;
    }
    assert(pos <= dsk->meta_block_size);
    memset(buffer+pos, 0, dsk->meta_block_size-pos);
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
    return meta_nearfull_blocks;
}

uint32_t blockstore_heap_t::get_compact_queue_size()
{
    return compact_queue.size();
}

uint32_t blockstore_heap_t::get_to_compact_count()
{
    return to_compact_count;
}

uint32_t blockstore_heap_t::get_inflight_queue_size()
{
    return inflight_lsn.size();
}

void blockstore_heap_t::push_inflight_lsn(uint64_t lsn, heap_entry_t *wr, uint64_t flags)
{
    uint64_t next_inf = first_inflight_lsn + inflight_lsn.size();
    if (flags & HEAP_INFLIGHT_COMPACTABLE)
    {
        to_compact_count++;
    }
    if (lsn == next_inf)
    {
        inflight_lsn.push_back((heap_inflight_lsn_t){ .flags = flags, .wr = wr });
    }
    else
    {
        if (lsn > next_inf)
        {
            inflight_lsn.resize(lsn-first_inflight_lsn+1, (heap_inflight_lsn_t){ .flags = HEAP_INFLIGHT_DONE });
        }
        inflight_lsn[lsn-first_inflight_lsn] = (heap_inflight_lsn_t){ .flags = flags, .wr = wr };
    }
}

void blockstore_heap_t::mark_completed_lsns()
{
    if (dsk->disable_meta_fsync && dsk->disable_journal_fsync)
    {
        // Apply effects immediately if metadata doesn't need fsyncing
        while (inflight_lsn.size())
        {
            auto & first = inflight_lsn.front();
            if (!(first.flags & HEAP_INFLIGHT_DONE))
            {
                break;
            }
            completed_lsn++;
            apply_inflight(first);
            inflight_lsn.pop_front();
            first_inflight_lsn++;
        }
    }
    else
    {
        // Only advanced completed_lsn
        for (auto it = inflight_lsn.begin(); it != inflight_lsn.end() && (it->flags & HEAP_INFLIGHT_DONE); it++)
        {
            completed_lsn++;
        }
    }
}

void blockstore_heap_t::mark_lsn_fsynced(uint64_t lsn)
{
    assert(!dsk->disable_meta_fsync || !dsk->disable_journal_fsync);
    if (lsn > fsynced_lsn)
    {
        assert(lsn <= completed_lsn);
        while (lsn >= first_inflight_lsn)
        {
            apply_inflight(inflight_lsn.front());
            inflight_lsn.pop_front();
            first_inflight_lsn++;
        }
        fsynced_lsn = lsn;
    }
}

void blockstore_heap_t::apply_inflight(heap_inflight_lsn_t & inflight)
{
    auto wr = inflight.wr;
    if (inflight.flags & HEAP_INFLIGHT_COMPACTED)
    {
        // Mark previous entries as garbage, sequentially
        mark_garbage_up_to(wr);
    }
    else if (inflight.flags & HEAP_INFLIGHT_COMPACTABLE)
    {
        // Add to the compaction queue
        compact_queue.push_back((object_id){ .inode = wr->inode, .stripe = wr->stripe });
    }
    else if (inflight.flags & HEAP_INFLIGHT_GC)
    {
        // Remove entry
        auto prev = list_item(wr)->prev;
        auto next = list_item(wr)->next;
        if (prev)
        {
            prev->next = next;
        }
        if (!next)
        {
            block_index[get_pg_id(wr->inode, wr->stripe)][wr->inode].erase(wr->stripe);
        }
        else
        {
            next->prev = prev;
            if (!prev && next->entry.entry_type == (BS_HEAP_DELETE|BS_HEAP_STABLE))
            {
                // free BS_HEAP_DELETEs when their refcount becomes 1
                mark_garbage(next->block_num, &next->entry, UINT32_MAX);
            }
        }
        free(list_item(wr));
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
