// Metadata storage version 3 ("heap")
// Copyright (c) Vitaliy Filippov, 2025+
// License: VNPL-1.1 (see README.md for details)

#include "blockstore_heap.h"

#include "../util/allocator.h"
#include "../util/crc32c.h"
#include "../util/malloc_or_die.h"

#define BS_HEAP_FREE_MVCC 1
#define BS_HEAP_FREE_MAIN 2

heap_write_t *heap_write_t::next(blockstore_heap_t *heap)
{
    return (heap_write_t*)((uint8_t*)this + get_size(heap));
}

uint32_t heap_write_t::get_size(blockstore_heap_t *heap)
{
    return (sizeof(heap_write_t) +
        heap->dsk->clean_entry_bitmap_size +
        ((flags & BS_HEAP_TYPE) == BS_HEAP_BIG_WRITE
            ? heap->dsk->clean_entry_bitmap_size
            : 0) +
        get_csum_size(heap));
}

uint32_t heap_write_t::get_csum_size(blockstore_heap_t *heap)
{
    if (!heap->dsk->csum_block_size)
    {
        return ((flags & BS_HEAP_TYPE) == BS_HEAP_SMALL_WRITE ? 4 : 0);
    }
    return ((offset+len+heap->dsk->csum_block_size-1)/heap->dsk->csum_block_size - offset/heap->dsk->csum_block_size)
        * (heap->dsk->data_csum_type & 0xFF);
}

bool heap_write_t::needs_recheck(blockstore_heap_t *heap)
{
    return len > 0 && lsn >= heap->compacted_lsn && (flags == (BS_HEAP_SMALL_WRITE|BS_HEAP_STABLE) || flags == BS_HEAP_SMALL_WRITE);
}

bool heap_write_t::needs_compact(uint64_t compacted_lsn)
{
    return lsn > compacted_lsn && flags == (BS_HEAP_SMALL_WRITE|BS_HEAP_STABLE);
}

bool heap_write_t::is_compacted(uint64_t compacted_lsn)
{
    return lsn <= compacted_lsn && flags == (BS_HEAP_SMALL_WRITE|BS_HEAP_STABLE);
}

bool heap_write_t::can_be_collapsed(blockstore_heap_t *heap)
{
    return !heap->dsk->csum_block_size || heap->dsk->csum_block_size == heap->dsk->bitmap_granularity ||
        !(offset % heap->dsk->csum_block_size) && !(len % heap->dsk->csum_block_size);
}

bool heap_write_t::is_allowed_before_compacted(uint64_t compacted_lsn, bool is_last_entry)
{
    return lsn <= compacted_lsn && flags == ((is_last_entry ? BS_HEAP_BIG_WRITE : BS_HEAP_SMALL_WRITE) | BS_HEAP_STABLE);
}

uint8_t *heap_write_t::get_ext_bitmap(blockstore_heap_t *heap)
{
    return ((uint8_t*)this + sizeof(heap_write_t));
}

uint8_t *heap_write_t::get_int_bitmap(blockstore_heap_t *heap)
{
    if ((flags & BS_HEAP_TYPE) != BS_HEAP_BIG_WRITE || !len)
        return NULL;
    return ((uint8_t*)this + sizeof(heap_write_t) + heap->dsk->clean_entry_bitmap_size);
}

uint8_t *heap_write_t::get_checksums(blockstore_heap_t *heap)
{
    if (!heap->dsk->csum_block_size || !len)
        return NULL;
    if ((flags & BS_HEAP_TYPE) == BS_HEAP_SMALL_WRITE)
        return ((uint8_t*)this + sizeof(heap_write_t) + heap->dsk->clean_entry_bitmap_size);
    if ((flags & BS_HEAP_TYPE) != BS_HEAP_BIG_WRITE)
        return NULL;
    return ((uint8_t*)this + sizeof(heap_write_t) + 2*heap->dsk->clean_entry_bitmap_size);
}

uint32_t *heap_write_t::get_checksum(blockstore_heap_t *heap)
{
    if (heap->dsk->csum_block_size || (flags & BS_HEAP_TYPE) != BS_HEAP_SMALL_WRITE || !len)
        return NULL;
    return (uint32_t*)((uint8_t*)this + sizeof(heap_write_t) + heap->dsk->clean_entry_bitmap_size);
}

heap_object_t *heap_object_t::next()
{
    return (heap_object_t*)((uint8_t*)this + size);
}

heap_write_t *heap_object_t::get_writes()
{
    return (heap_write_t*)((uint8_t*)this + sizeof(heap_object_t));
}

uint32_t heap_object_t::calc_crc32c()
{
    return ::crc32c(0, (uint8_t*)&inode, size - ((uint8_t*)(&inode) - (uint8_t*)this));
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
    meta_alloc = new allocator_t(meta_block_count);
    block_info.resize(meta_block_count);
    buffer_by_end.insert((heap_extent_t){ .start = 0, .end = dsk->journal_len });
    buffer_by_size.insert((heap_extent_t){ .start = 0, .end = dsk->journal_len });
    data_alloc = new allocator_t(dsk->block_count);
    if (!target_block_free_space)
        target_block_free_space = 800;
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
    if (meta_alloc)
    {
        delete meta_alloc;
    }
    if (data_alloc)
    {
        delete data_alloc;
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

// EASY PEASY LEMON SQUEEZIE
uint64_t blockstore_heap_t::load_blocks(uint64_t disk_offset, uint64_t size, uint8_t *buf)
{
    uint64_t entries_loaded = 0;
    for (uint64_t buf_offset = 0; buf_offset < size; buf_offset += dsk->meta_block_size)
    {
        uint32_t block_num = (disk_offset + buf_offset) / dsk->meta_block_size;
        assert(block_num < block_info.size());
        uint32_t virtual_free_space = 0;
        uint32_t block_offset = 0;
        uint32_t block_end = dsk->meta_block_size - sizeof(heap_object_t);
        uint32_t src_offset = 0;
        while (block_offset < block_end)
        {
            heap_object_t *obj = (heap_object_t *)(buf + buf_offset + block_offset);
            if (!obj->size)
            {
                break;
            }
            if (obj->size < sizeof(heap_object_t))
            {
                fprintf(stderr, "Warning: Object is too small in metadata block %u at %u (%u bytes), skipping the rest of block\n",
                    block_num, src_offset, obj->size);
skip_block:
                if (fail_on_warn)
                    abort();
                if (block_offset > 0)
                    memset((void*)obj, 0, dsk->meta_block_size-block_offset);
                break;
            }
            if (obj->size > dsk->meta_block_size-block_offset)
            {
                fprintf(stderr, "Warning: Object is too large in metadata block %u at %u (%u bytes), skipping the rest of block\n",
                    block_num, src_offset, obj->size);
                goto skip_block;
            }
            uint32_t expected_crc32c = obj->calc_crc32c();
            if (obj->crc32c != expected_crc32c)
            {
                fprintf(stderr, "Warning: Object is corrupt in metadata block %u at %u (crc32c mismatch: expected %08x, got %08x), skipping\n",
                    block_num, src_offset, expected_crc32c, obj->crc32c);
skip_object:
                uint32_t obj_size = obj->size;
                src_offset += obj_size;
                uint32_t to_copy = dsk->meta_block_size-block_offset-obj_size;
                memmove(obj, (uint8_t*)obj + obj_size, to_copy);
                continue;
            }
            if (!obj->write_count)
            {
                fprintf(stderr, "Warning: Object in metadata block %u at %u does not contain writes, skipping\n", block_num, src_offset);
                if (fail_on_warn)
                    abort();
                goto skip_object;
            }
            uint64_t to_compact = 0;
            bool to_recheck = false;
            heap_write_t *wr = obj->get_writes();
            uint32_t calc_obj_size = sizeof(heap_object_t);
            uint32_t remove_entry_bytes = 0;
            uint32_t remove_entry_count = 0;
            uint64_t remove_lsn = 0;
            for (uint16_t wr_i = 0; wr_i < obj->write_count; wr_i++)
            {
                auto sz = wr->get_size(this);
                calc_obj_size += sz;
                if (calc_obj_size > obj->size)
                {
                    fprintf(stderr, "Warning: Object write entries exceed object size in metadata block %u at %u, skipping object\n",
                        block_num, src_offset);
                    if (fail_on_warn)
                        abort();
                    goto skip_object;
                }
                if (wr->needs_recheck(this))
                {
                    if (!buffer_area)
                    {
                        to_recheck = true;
                    }
                    // recheck small write data immediately
                    else if (!calc_checksums(wr, buffer_area + wr->location, false))
                    {
                        // entry is invalid (not fully written before OSD crash) - remove it and all newer (previous) entries too
                        remove_entry_bytes = calc_obj_size - sizeof(heap_object_t);
                        remove_entry_count = wr_i+1;
                        remove_lsn = wr->lsn;
                    }
                }
                if ((wr->flags & BS_HEAP_TYPE) == BS_HEAP_SMALL_WRITE)
                {
                    use_buffer_area(obj->inode, wr->location, wr->len);
                }
                else if ((wr->flags & BS_HEAP_TYPE) == BS_HEAP_BIG_WRITE)
                {
                    // Mark data block as used
                    use_data(obj->inode, wr->location);
                }
                if (wr->needs_compact(this->compacted_lsn))
                {
                    to_compact = to_compact ? to_compact : wr->lsn;
                }
                else if (wr->is_compacted(this->compacted_lsn))
                {
                    if (wr->can_be_collapsed(this))
                    {
                        virtual_free_space += sz;
                    }
                    else
                    {
                        // We can't just collapse the object entry when csum_block_size is larger
                        // than bitmap_granularity, so we add the object into the compact queue
                        to_compact = to_compact ? to_compact : wr->lsn;
                    }
                }
                wr = (heap_write_t*)((uint8_t*)wr + sz);
            }
            if (obj->write_count == remove_entry_count)
            {
                // Skip the whole object
                goto skip_object;
            }
            uint64_t lsn = obj->get_writes()->lsn;
            auto oid = (object_id){ .inode = obj->inode, .stripe = obj->stripe };
            uint32_t dup_block;
            heap_object_t *dup_obj = read_entry(oid, &dup_block);
            if (dup_obj != NULL)
            {
                if (dup_obj->get_writes()->lsn >= lsn)
                {
                    // Object is duplicated on disk
                    fprintf(stderr, "Warning: Object in metadata block %u at %u is an older duplicate, skipping\n",
                        block_num, src_offset);
                    free_object_space(obj->inode, obj->get_writes(), (heap_write_t*)obj->next());
                    goto skip_object;
                }
                else
                {
                    fprintf(stderr, "Warning: Object in metadata block %u at %u is a newer duplicate, overriding\n",
                        block_num, src_offset);
                    free_object_space(dup_obj->inode, dup_obj->get_writes(), (heap_write_t*)dup_obj->next());
                    compact_block(dup_block, oid);
                }
            }
            if (remove_entry_count)
            {
                if (log_level > 3)
                {
                    fprintf(stderr, "Notice: %u unfinished writes to %jx:%jx v%jx since lsn %ju, rolling back\n",
                        remove_entry_count, obj->inode, obj->stripe, obj->get_writes()->version, remove_lsn);
                }
                uint32_t to_copy = dsk->meta_block_size-block_offset-obj->size + obj->size-sizeof(heap_object_t)-remove_entry_bytes;
                if (to_copy < remove_entry_bytes)
                {
                    memset((uint8_t*)obj + sizeof(heap_object_t), 0, remove_entry_bytes);
                }
                memmove((uint8_t*)obj + sizeof(heap_object_t), (uint8_t*)obj + sizeof(heap_object_t) + remove_entry_bytes, to_copy);
                obj->size -= remove_entry_bytes;
                obj->write_count -= remove_entry_count;
                obj->crc32c = obj->calc_crc32c();
                src_offset += remove_entry_bytes;
            }
            if (lsn > next_lsn)
            {
                next_lsn = lsn;
            }
            if (to_compact)
            {
                if (compact_queue_lsn.find(oid) == compact_queue_lsn.end())
                {
                    compact_queue.push_back(oid);
                }
                compact_queue_lsn[oid] = to_compact;
            }
            if (to_recheck)
            {
                recheck_queue.push_back(oid);
            }
            // btree_map<ui64, ui32> anyway stores std::pair<ui64, ui32>'s of 16 bytes size
            // so we can store block_offset in it too
            block_index[get_pg_id(obj->inode, obj->stripe)][obj->inode][obj->stripe] = (uint32_t)block_num*dsk->meta_block_size + block_offset;
            entries_loaded += obj->write_count;
            src_offset += obj->size;
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
            .used_space = block_offset,
            .virtual_free_space = virtual_free_space,
            .data = copy,
        };
        if (block_offset > 0)
        {
            mark_allocated_block(block_num);
        }
    }
    return entries_loaded;
}

void blockstore_heap_t::finish_load()
{
    std::sort(compact_queue.begin(), compact_queue.end(), [this](const object_id & a, const object_id & b)
    {
        return compact_queue_lsn[a] < compact_queue_lsn[b];
    });
}

bool blockstore_heap_t::calc_checksums(heap_write_t *wr, uint8_t *data, bool set)
{
    if (!dsk->csum_block_size)
    {
        if ((wr->flags & BS_HEAP_TYPE) != BS_HEAP_SMALL_WRITE)
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
    return calc_block_checksums((uint32_t*)wr->get_checksums(this), data, wr->get_int_bitmap(this),
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

bool blockstore_heap_t::recheck_small_writes(std::function<void(uint64_t, uint64_t, uint8_t*, std::function<void()>)> read_buffer, int queue_depth)
{
    if (buffer_area)
    {
        // Already checked
        return true;
    }
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
        for (heap_write_t *wr = obj->get_writes(); wr < (heap_write_t*)obj->next(); wr = wr->next(this))
        {
            if (wr->needs_recheck(this))
            {
                recheck_in_progress++;
                uint8_t *buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, wr->len);
                recheck_cb(wr->location, wr->len, buf, [this, oid, lsn = wr->lsn, buf]()
                {
                    uint32_t block_num = 0;
                    heap_object_t *obj = read_entry(oid, &block_num);
                    if (obj)
                    {
                        heap_write_t *wr = obj->get_writes(), *end = (heap_write_t*)obj->next();
                        uint32_t wr_i = 0;
                        while (wr < end && wr->lsn != lsn)
                        {
                            wr = wr->next(this);
                            wr_i++;
                        }
                        if (wr < end && !calc_checksums(wr, buf, false))
                        {
                            // Erase all writes to the object from this one to the newest
                            if (log_level > 3)
                            {
                                fprintf(stderr, "Notice: %u unfinished writes to %jx:%jx v%jx since lsn %ju, rolling back\n",
                                    wr_i+1, obj->inode, obj->stripe, obj->get_writes()->version, wr->lsn);
                            }
                            auto & inf = block_info.at(block_num);
                            unmark_allocated_block(block_num);
                            uint32_t to_move, to_erase;
                            obj->write_count -= wr_i+1;
                            if (!obj->write_count)
                            {
                                to_erase = obj->size;
                                to_move = inf.used_space - ((uint8_t*)obj->next() - inf.data);
                                memmove((uint8_t*)obj, wr->next(this), to_move);
                            }
                            else
                            {
                                to_erase = (uint8_t*)wr->next(this) - (uint8_t*)obj->get_writes();
                                to_move = inf.used_space - ((uint8_t*)wr->next(this) - inf.data);
                                memmove((uint8_t*)obj->get_writes(), wr->next(this), to_move);
                                obj->size -= to_erase;
                                obj->crc32c = obj->calc_crc32c();
                            }
                            memset(inf.data+inf.used_space-to_erase, 0, to_erase);
                            inf.used_space -= to_erase;
                            mark_allocated_block(block_num);
                            reindex_block(block_num, obj);
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
            cb(0, 0, NULL, NULL);
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

heap_object_t *blockstore_heap_t::lock_and_read_entry(object_id oid, uint64_t & lsn)
{
    auto obj = read_entry(oid, NULL);
    if (!obj)
    {
        return NULL;
    }
    lsn = obj->get_writes()->lsn;
    auto & mvcc = object_mvcc[(heap_object_lsn_t){ .oid = oid, .lsn = lsn }];
    mvcc.readers++;
    if (mvcc.entry_copy)
    {
        return mvcc.entry_copy;
    }
    return obj;
}

heap_object_t *blockstore_heap_t::read_locked_entry(object_id oid, uint64_t lsn)
{
    auto mvcc_it = object_mvcc.find((heap_object_lsn_t){ .oid = oid, .lsn = lsn });
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

bool blockstore_heap_t::unlock_entry(object_id oid, uint64_t lsn)
{
    auto mvcc_it = object_mvcc.find((heap_object_lsn_t){ .oid = oid, .lsn = lsn });
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
            free_object_space(obj->inode, obj->get_writes(), (heap_write_t*)obj->next(), BS_HEAP_FREE_MVCC);
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
                    free_object_space(new_obj->inode, new_obj->get_writes(), (heap_write_t*)new_obj->next(), BS_HEAP_FREE_MAIN);
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
    heap_write_t *wr = obj->get_writes();
    for (uint16_t wr_i = 0; wr_i < obj->write_count; wr_i++, wr = wr->next(this))
    {
        if (wr->is_compacted(max_lsn))
        {
            *begin_wr = wr;
        }
        if (*begin_wr)
        {
            bool is_last = (wr_i == obj->write_count-1);
            if (is_last)
            {
                *end_wr = wr;
            }
            // all subsequent small write entries must also be compacted
            assert(wr->is_allowed_before_compacted(UINT64_MAX, is_last));
        }
    }
}

bool blockstore_heap_t::compact_object_to(heap_object_t *obj, uint64_t compact_lsn, heap_object_t *to_obj, uint8_t *new_csums)
{
    heap_write_t *wr = obj->get_writes();
    assert(obj->write_count <= 1024);
    heap_write_t *compacted_wrs[obj->write_count];
    int compacted_wr_count = 0;
    bool has_more = false;
    bool skip = false;
    heap_write_t *big_wr = NULL;
    for (uint16_t wr_i = 0; wr_i < obj->write_count; wr_i++)
    {
        if (wr->is_compacted(compact_lsn))
        {
            compacted_wrs[compacted_wr_count++] = wr;
        }
        else if (wr->needs_compact(compact_lsn))
        {
            has_more = true;
        }
        if (compacted_wr_count)
        {
            bool is_last = (wr_i == obj->write_count-1);
            if ((wr->flags & BS_HEAP_TYPE) == BS_HEAP_BIG_WRITE)
            {
                big_wr = wr;
            }
            // all subsequent small write entries must also be compacted
            assert(wr->is_allowed_before_compacted(compact_lsn, is_last));
            if (!new_csums && !wr->can_be_collapsed(this))
            {
                skip = true;
            }
        }
        wr = wr->next(this);
    }
    if (compacted_wr_count == 0 || skip)
    {
        return false;
    }
    if (!has_more)
    {
        compact_queue_lsn.erase((object_id){ .inode = obj->inode, .stripe = obj->stripe });
    }
    free_object_space(obj->inode, compacted_wrs[0], big_wr);
    // Generate a collapsed BIG_WRITE entry
    uint8_t collapsed_buf[max_write_entry_size];
    heap_write_t *collapsed_wr = (heap_write_t*)collapsed_buf;
    collapsed_wr->lsn = compacted_wrs[0]->lsn;
    collapsed_wr->version = compacted_wrs[0]->version;
    collapsed_wr->offset = big_wr->offset;
    collapsed_wr->len = big_wr->offset + big_wr->len;
    collapsed_wr->location = big_wr->location;
    collapsed_wr->flags = BS_HEAP_BIG_WRITE|BS_HEAP_STABLE;
    memcpy(collapsed_wr->get_ext_bitmap(this), compacted_wrs[0]->get_ext_bitmap(this), dsk->clean_entry_bitmap_size);
    memcpy(collapsed_wr->get_int_bitmap(this), big_wr->get_int_bitmap(this), dsk->clean_entry_bitmap_size);
    for (int i = 0; i < compacted_wr_count; i++)
    {
        auto cur_wr = compacted_wrs[i];
        if (collapsed_wr->offset > cur_wr->offset)
        {
            collapsed_wr->offset = cur_wr->offset;
        }
        if (collapsed_wr->len < cur_wr->offset+cur_wr->len)
        {
            collapsed_wr->len = cur_wr->offset+cur_wr->len;
        }
    }
    collapsed_wr->len -= collapsed_wr->offset;
    assert(collapsed_wr->get_size(this) <= max_write_entry_size);
    uint8_t *int_bmp = collapsed_wr->get_int_bitmap(this);
    uint8_t *csums = collapsed_wr->get_checksums(this);
    const uint32_t csum_size = (dsk->data_csum_type & 0xFF);
    for (int i = compacted_wr_count-1; i >= 0; i--)
    {
        auto cur_wr = compacted_wrs[i];
        bitmap_set(int_bmp, cur_wr->offset, cur_wr->len, dsk->bitmap_granularity);
        // copy checksums
        if (csums && !new_csums)
        {
            assert(i == compacted_wr_count-1 ||
                (cur_wr->offset % dsk->csum_block_size) == 0 &&
                (cur_wr->len % dsk->csum_block_size) == 0);
            memcpy(csums + (cur_wr->offset/dsk->csum_block_size - collapsed_wr->offset/dsk->csum_block_size)*csum_size,
                cur_wr->get_checksums(this), cur_wr->len/dsk->csum_block_size*csum_size);
        }
    }
    if (csums && new_csums)
    {
        memcpy(csums, new_csums, collapsed_wr->get_csum_size(this));
    }
    // Copy it over the old entries
    uint32_t copy_wr_bytes = (uint8_t*)compacted_wrs[0] - (uint8_t*)obj;
    if (to_obj != obj)
    {
        memmove(to_obj, obj, copy_wr_bytes);
    }
    memcpy((uint8_t*)to_obj + copy_wr_bytes, collapsed_wr, collapsed_wr->get_size(this));
    to_obj->write_count -= compacted_wr_count;
    to_obj->size = (uint8_t*)compacted_wrs[0] - (uint8_t*)obj + collapsed_wr->get_size(this);
    to_obj->crc32c = to_obj->calc_crc32c();
    return true;
}

void blockstore_heap_t::compact_block(uint32_t block_num, object_id skip_oid)
{
    const uint8_t *data = block_info[block_num].data;
    assert(data);
    const heap_object_t *block_end = (heap_object_t *)((uint8_t*)data + block_info[block_num].used_space);
    heap_object_t *obj = (heap_object_t *)data;
    heap_object_t *to_obj = (heap_object_t *)data;
    while (obj < block_end && obj->size)
    {
        heap_object_t *next_obj = obj->next();
        if (obj->inode == skip_oid.inode && obj->stripe == skip_oid.stripe)
        {
            obj = next_obj;
            continue;
        }
        bool compacted = compact_object_to(obj, compacted_lsn, to_obj, NULL);
        if (to_obj != obj)
        {
            block_index[get_pg_id(obj->inode, obj->stripe)][obj->inode][obj->stripe] = (uint64_t)block_num*dsk->meta_block_size + ((uint8_t*)to_obj - data);
            if (!compacted)
            {
                memmove(to_obj, obj, obj->size);
            }
        }
        to_obj = to_obj->next();
        obj = next_obj;
    }
    uint32_t new_used_space = (uint8_t*)to_obj - (uint8_t*)data;
    if (new_used_space < block_info[block_num].used_space)
    {
        memset((void*)to_obj, 0, block_info[block_num].used_space - new_used_space);
    }
    unmark_allocated_block(block_num);
    block_info[block_num].used_space = new_used_space;
    block_info[block_num].virtual_free_space = 0;
    mark_allocated_block(block_num);
}

int blockstore_heap_t::get_block_for_new_object(uint32_t & out_block_num)
{
    // Blocks with at least target_block_free_space are tried first in number order
    uint64_t block_num = meta_alloc->find_free();
    if (block_num >= block_info.size())
    {
        // Blocks with less than target_block_free_space are tried second, in free space order
        auto u_it = used_alloc_queue.begin();
        if (u_it == used_alloc_queue.end() || u_it->free_space < sizeof(heap_object_t) + 2*max_write_entry_size)
        {
            return compact_queue.size() ? EAGAIN : ENOSPC;
        }
        block_num = u_it->block_num;
    }
    out_block_num = block_num;
    return 0;
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
    uint32_t wr_size = wr->get_size(this);
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
    }
    if (modified_block)
    {
        *modified_block = block_num;
    }
    // Compact block
    if (block_info[block_num].virtual_free_space)
    {
        compact_block(block_num, {});
    }
    block_index[get_pg_id(oid.inode, oid.stripe)][oid.inode][oid.stripe] = (uint64_t)block_num*dsk->meta_block_size + inf.used_space;
    // and just append the object entry
    heap_object_t *new_entry = (heap_object_t *)(inf.data + inf.used_space);
    new_entry->inode = oid.inode;
    new_entry->stripe = oid.stripe;
    new_entry->write_count = 1;
    heap_write_t *new_wr = new_entry->get_writes();
    memcpy(new_wr, wr, wr_size);
    new_wr->lsn = ++next_lsn;
    if ((wr->flags & BS_HEAP_TYPE) == BS_HEAP_BIG_WRITE)
    {
        uint8_t *int_bitmap = new_wr->get_int_bitmap(this);
        memset(int_bitmap, 0, dsk->clean_entry_bitmap_size);
        bitmap_set(int_bitmap, wr->offset, wr->len, dsk->bitmap_granularity);
    }
    if (wr->needs_compact(0) &&
        compact_queue_lsn.find(oid) == compact_queue_lsn.end())
    {
        compact_queue.push_back(oid);
        compact_queue_lsn[oid] = new_wr->lsn;
    }
    new_entry->size = sizeof(heap_object_t) + wr_size;
    new_entry->crc32c = new_entry->calc_crc32c();
    unmark_allocated_block(block_num);
    inf.used_space += new_entry->size;
    mark_allocated_block(block_num);
    return 0;
}

heap_object_t *blockstore_heap_t::mvcc_save_copy(heap_object_t *obj)
{
    auto oid = (object_id){ .inode = obj->inode, .stripe = obj->stripe };
    auto lsn = obj->get_writes()->lsn;
    auto mvcc_it = object_mvcc.find((heap_object_lsn_t){ .oid = oid, .lsn = lsn });
    if (mvcc_it == object_mvcc.end())
    {
        return NULL;
    }
    assert(!mvcc_it->second.entry_copy);
    heap_object_t *obj_copy = (heap_object_t*)malloc_or_die(obj->size);
    memcpy(obj_copy, obj, obj->size);
    mvcc_it->second.entry_copy = obj_copy;
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
    for (auto wr = obj->get_writes(); wr < (heap_write_t*)obj->next(); wr = wr->next(this))
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
    return mvcc_it->second.entry_copy;
}

int blockstore_heap_t::update_object(uint32_t block_num, heap_object_t *obj, heap_write_t *wr, uint32_t *modified_block)
{
    auto oid = (object_id){ .inode = obj->inode, .stripe = obj->stripe };
    uint32_t wr_size = wr->get_size(this);
    auto & inf = block_info.at(block_num);
    assert(inf.data);
    bool is_overwrite = (wr->flags == (BS_HEAP_BIG_WRITE|BS_HEAP_STABLE) || wr->flags == (BS_HEAP_TOMBSTONE|BS_HEAP_STABLE));
    uint32_t new_object_size = (is_overwrite ? sizeof(heap_object_t)+wr_size : obj->size+wr_size);
    if (dsk->meta_block_size-inf.used_space+inf.virtual_free_space+obj->size < new_object_size)
    {
        // Something in the block has to be compacted
        return compact_queue.size() ? EAGAIN : ENOSPC;
    }
    if ((obj->get_writes()->flags & BS_HEAP_TYPE) == BS_HEAP_TOMBSTONE && !is_overwrite)
    {
        // Small overwrites are only allowed over live objects
        return EINVAL;
    }
    if (!(obj->get_writes()->flags & BS_HEAP_STABLE) && (wr->flags & BS_HEAP_STABLE))
    {
        // Stable overwrites are not allowed over unstable
        return EINVAL;
    }
    if (wr->version <= obj->get_writes()->version)
    {
        if (!wr->version)
        {
            wr->version = obj->get_writes()->version + 1;
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
    // Save a copy of the object
    bool free_copy = false;
    heap_object_t *obj_copy = mvcc_save_copy(obj);
    bool tracking_active = !!obj_copy;
    if (!tracking_active)
    {
        auto mvcc_it = object_mvcc.lower_bound((heap_object_lsn_t){ .oid = oid, .lsn = 0 });
        tracking_active = (mvcc_it != object_mvcc.end() && mvcc_it->first.oid == oid && mvcc_it->second.entry_copy);
    }
    if (!obj_copy)
    {
        obj_copy = (heap_object_t*)malloc_or_die(obj->size);
        memcpy(obj_copy, obj, obj->size);
        free_copy = true;
    }
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
    // Compact block, skipping the object at the same time
    if (is_overwrite)
    {
        free_object_space(obj->inode, obj->get_writes(), (heap_write_t*)obj->next());
    }
    compact_block(block_num, oid);
    // Remove block from allocation maps
    unmark_allocated_block(block_num);
    // Add the object to block again
    obj = (heap_object_t*)(inf.data + inf.used_space);
    memcpy(obj, obj_copy, sizeof(heap_object_t));
    block_index[get_pg_id(obj->inode, obj->stripe)][obj->inode][obj->stripe] = (uint32_t)block_num*dsk->meta_block_size + inf.used_space;
    heap_write_t *new_wr = obj->get_writes();
    memcpy(new_wr, wr, wr_size);
    new_wr->lsn = ++next_lsn;
    if ((wr->flags & BS_HEAP_TYPE) == BS_HEAP_BIG_WRITE)
    {
        uint8_t *int_bitmap = new_wr->get_int_bitmap(this);
        memset(int_bitmap, 0, dsk->clean_entry_bitmap_size);
        bitmap_set(int_bitmap, wr->offset, wr->len, dsk->bitmap_granularity);
    }
    if (wr->flags == (BS_HEAP_BIG_WRITE|BS_HEAP_STABLE) ||
        wr->flags == (BS_HEAP_TOMBSTONE|BS_HEAP_STABLE))
    {
        obj->write_count = 1;
        obj->size = sizeof(heap_object_t) + wr_size;
    }
    else
    {
        memcpy((uint8_t*)new_wr + wr_size, obj_copy->get_writes(), obj_copy->size - sizeof(heap_object_t));
        obj->write_count++;
        obj->size += wr_size;
    }
    if (wr->needs_compact(0) &&
        compact_queue_lsn.find(oid) == compact_queue_lsn.end())
    {
        compact_queue.push_back(oid);
        compact_queue_lsn[oid] = new_wr->lsn;
    }
    obj->crc32c = obj->calc_crc32c();
    inf.used_space += obj->size;
    mark_allocated_block(block_num);
    if (free_copy)
    {
        free(obj_copy);
    }
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

int blockstore_heap_t::post_stabilize(object_id oid, uint64_t version, uint32_t *modified_block)
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
    if (inf.virtual_free_space)
    {
        compact_block(block_num, {});
        obj = read_entry(oid, &block_num);
        assert(obj);
    }
    assert(obj->write_count > 0);
    uint32_t unstable_idx = UINT32_MAX;
    uint32_t unstable_big_idx = UINT32_MAX;
    heap_write_t *unstable_big_wr = NULL;
    heap_write_t *wr = obj->get_writes();
    uint32_t wr_i;
    if (wr->version < version)
    {
        // No such version
        return ENOENT;
    }
    for (wr = obj->get_writes(), wr_i = 0; wr_i < obj->write_count; wr_i++, wr = wr->next(this))
    {
        if (!(wr->flags & BS_HEAP_STABLE) && wr->version <= version)
        {
            unstable_idx = wr_i;
            if (unstable_big_idx == UINT32_MAX &&
                ((wr->flags & BS_HEAP_TYPE) == BS_HEAP_BIG_WRITE ||
                (wr->flags & BS_HEAP_TYPE) == BS_HEAP_TOMBSTONE))
            {
                unstable_big_wr = wr;
                unstable_big_idx = wr_i;
            }
        }
    }
    if (unstable_idx == UINT32_MAX)
    {
        // Version is already stable
        return 0;
    }
    if (modified_block)
    {
        *modified_block = block_num;
    }
    // Save a copy of the object
    mvcc_save_copy(obj);
    if (unstable_big_idx != UINT32_MAX && unstable_big_idx+1 < obj->write_count)
    {
        // Remove previous stable entry series
        unmark_allocated_block(block_num);
        assert(unstable_big_wr);
        free_object_space(obj->inode, unstable_big_wr->next(this), (heap_write_t*)obj->next());
        auto after_wr = unstable_big_wr->next(this);
        uint32_t to_copy = inf.used_space - ((uint8_t*)obj + obj->size - (uint8_t*)inf.data);
        uint32_t to_erase = (uint8_t*)obj + obj->size - (uint8_t*)after_wr;
        memmove((void*)after_wr, obj->next(), to_copy);
        memset(inf.data+inf.used_space-to_erase, 0, to_erase);
        obj->size -= to_erase;
        obj->write_count = unstable_big_idx+1;
        inf.used_space -= to_erase;
        reindex_block(block_num, obj->next());
        mark_allocated_block(block_num);
    }
    // Set the stability flag
    uint64_t to_compact = 0;
    for (wr = obj->get_writes(), wr_i = 0; wr_i < obj->write_count; wr_i++, wr = wr->next(this))
    {
        if (!(wr->flags & BS_HEAP_STABLE) && wr->version <= version)
        {
            wr->flags |= BS_HEAP_STABLE;
        }
        if (wr->needs_compact(0))
        {
            to_compact = wr->lsn;
        }
    }
    if (to_compact && compact_queue_lsn.find(oid) == compact_queue_lsn.end())
    {
        compact_queue.push_back(oid);
        compact_queue_lsn[oid] = to_compact;
    }
    obj->crc32c = obj->calc_crc32c();
    return 0;
}

int blockstore_heap_t::post_rollback(object_id oid, uint64_t version, uint32_t *modified_block)
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
    if (inf.virtual_free_space)
    {
        compact_block(block_num, {});
        obj = read_entry(oid, &block_num);
        assert(obj);
    }
    assert(obj->write_count > 0);
    uint32_t wr_i;
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
    for (wr_i = 0; wr_i < obj->write_count && wr->version > version; wr_i++, wr = wr->next(this))
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
    mvcc_save_copy(obj);
    if (wr_i >= obj->write_count)
    {
        erase_object(block_num, obj);
    }
    else
    {
        unmark_allocated_block(block_num);
        // Erase head versions
        heap_write_t *first_wr = obj->get_writes();
        free_object_space(obj->inode, first_wr, wr);
        uint32_t to_copy = inf.used_space - ((uint8_t*)wr - (uint8_t*)inf.data);
        uint32_t to_erase = (uint8_t*)wr - (uint8_t*)first_wr;
        memmove((void*)first_wr, wr, to_copy);
        memset(inf.data+inf.used_space-to_erase, 0, to_erase);
        obj->size -= to_erase;
        obj->write_count -= wr_i;
        obj->crc32c = obj->calc_crc32c();
        inf.used_space -= to_erase;
        mark_allocated_block(block_num);
        reindex_block(block_num, obj->next());
    }
    return 0;
}

int blockstore_heap_t::post_delete(object_id oid, uint32_t *modified_block)
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
    mvcc_save_copy(obj);
    auto & inf = block_info.at(block_num);
    assert(inf.data);
    if (inf.virtual_free_space)
    {
        free_object_space(obj->inode, obj->get_writes(), (heap_write_t*)obj->next());
        compact_block(block_num, oid);
        erase_block_index(oid.inode, oid.stripe);
    }
    else
    {
        erase_object(block_num, obj);
    }
    return 0;
}

int blockstore_heap_t::get_next_compact(object_id & oid)
{
    auto begin_it = compact_queue.begin(), compact_it = begin_it;
    for (; compact_it != compact_queue.end(); compact_it++)
    {
        auto lsn_it = compact_queue_lsn.find(*compact_it);
        if (lsn_it != compact_queue_lsn.end())
        {
            oid = *compact_it;
            compact_queue.erase(begin_it, compact_it+1);
            compact_queue_lsn.erase(lsn_it);
            return 0;
        }
    }
    compact_queue.clear();
    return ENOENT;
}

// FIXME try to use virtual_free_space when possible
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
    auto & inf = block_info.at(block_num);
    uint32_t old_size = obj->size;
    int res = EAGAIN;
    if (compact_object_to(obj, compact_lsn, obj, new_csums))
    {
        unmark_allocated_block(block_num);
        uint32_t new_size = obj->size;
        uint32_t to_copy = inf.used_space - ((uint8_t*)obj + old_size - (uint8_t*)inf.data);
        memmove((uint8_t*)obj + new_size, (uint8_t*)obj + old_size, to_copy);
        memset(inf.data+inf.used_space-(old_size-new_size), 0, old_size-new_size);
        res = 0;
        inf.used_space -= old_size-new_size;
        reindex_block(block_num, obj);
        mark_allocated_block(block_num);
    }
    return res;
}

void blockstore_heap_t::free_object_space(inode_t inode, heap_write_t *from, heap_write_t *to, int mode)
{
    for (heap_write_t *wr = from; wr < to; wr = wr->next(this))
    {
        if ((wr->flags & BS_HEAP_TYPE) == BS_HEAP_BIG_WRITE)
        {
            auto ref_it = mvcc_data_refs.find(wr->location);
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
            if (ref_it == mvcc_data_refs.end() && mode != BS_HEAP_FREE_MAIN)
            {
                assert(data_alloc->get(wr->location >> dsk->block_order));
                data_alloc->set(wr->location >> dsk->block_order, false);
                auto & space = inode_space_stats[inode];
                assert(space >= dsk->data_block_size);
                space -= dsk->data_block_size;
                data_used_space -= dsk->data_block_size;
                if (!space)
                    inode_space_stats.erase(inode);
            }
            if (mode == BS_HEAP_FREE_MVCC && (wr->flags & BS_HEAP_STABLE))
            {
                // Stop at the last visible version
                break;
            }
        }
        else if ((wr->flags & BS_HEAP_TYPE) == BS_HEAP_SMALL_WRITE)
        {
            auto ref_it = mvcc_buffer_refs.find(wr->location);
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
            if (ref_it == mvcc_buffer_refs.end() && mode != BS_HEAP_FREE_MAIN)
            {
                free_buffer_area(inode, wr->location, wr->len);
            }
        }
    }
}

void blockstore_heap_t::reindex_block(uint32_t block_num, heap_object_t *from_obj)
{
    auto & inf = block_info.at(block_num);
    for (heap_object_t *obj = from_obj; obj < (heap_object_t*)(inf.data + inf.used_space); obj = obj->next())
    {
        assert(obj->size > 0);
        block_index[get_pg_id(obj->inode, obj->stripe)][obj->inode][obj->stripe] = (uint32_t)block_num*dsk->meta_block_size + ((uint8_t*)obj - inf.data);
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

void blockstore_heap_t::erase_object(uint32_t block_num, heap_object_t *obj)
{
    auto & inf = block_info.at(block_num);
    unmark_allocated_block(block_num);
    // Erase object
    erase_block_index(obj->inode, obj->stripe);
    free_object_space(obj->inode, obj->get_writes(), (heap_write_t*)((uint8_t*)obj + obj->size));
    uint32_t to_copy = inf.used_space - ((uint8_t*)obj + obj->size - (uint8_t*)inf.data);
    uint32_t to_erase = obj->size;
    memmove(obj, (uint8_t*)obj + to_erase, to_copy);
    memset(inf.data+inf.used_space-to_erase, 0, to_erase);
    inf.used_space -= to_erase;
    mark_allocated_block(block_num);
    reindex_block(block_num, obj);
}

void blockstore_heap_t::unmark_allocated_block(uint32_t block_num)
{
    auto & inf = block_info.at(block_num);
    meta_used_space -= inf.used_space-inf.virtual_free_space;
    if (inf.used_space-inf.virtual_free_space > dsk->meta_block_size-target_block_free_space)
    {
        meta_alloc_count--;
        meta_alloc->set(block_num, false);
        used_alloc_queue.erase((heap_block_free_t){
            .block_num = block_num,
            .free_space = (uint32_t)(dsk->meta_block_size-inf.used_space+inf.virtual_free_space),
        });
    }
}

void blockstore_heap_t::mark_allocated_block(uint32_t block_num)
{
    auto & inf = block_info.at(block_num);
    meta_used_space += inf.used_space-inf.virtual_free_space;
    if (inf.used_space-inf.virtual_free_space > dsk->meta_block_size-target_block_free_space)
    {
        meta_alloc_count++;
        meta_alloc->set(block_num, true);
        used_alloc_queue.insert((heap_block_free_t){
            .block_num = block_num,
            .free_space = (uint32_t)(dsk->meta_block_size-inf.used_space+inf.virtual_free_space),
        });
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
            heap_write_t *first_wr = obj->get_writes();
            heap_write_t *last_wr = (heap_write_t*)((uint8_t*)obj + obj->size);
            uint64_t stable_version = 0;
            for (heap_write_t *wr = first_wr; wr < last_wr; wr = wr->next(this))
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
    auto free_it = buffer_by_size.lower_bound((heap_extent_t){ .start = 0, .end = size });
    if (free_it == buffer_by_size.end())
    {
        return UINT64_MAX;
    }
    return free_it->start;
}

bool blockstore_heap_t::is_buffer_area_free(uint64_t location, uint64_t size)
{
    auto free_it = buffer_by_end.lower_bound((heap_extent_t){ .end = location+size });
    return (free_it != buffer_by_end.end() && free_it->start <= location);
}

void blockstore_heap_t::use_buffer_area(inode_t inode, uint64_t location, uint64_t size)
{
    auto free_it = buffer_by_end.lower_bound((heap_extent_t){ .end = location+size });
    assert(free_it != buffer_by_end.end() && free_it->start <= location && free_it->end >= location+size);
    heap_extent_t extent = *free_it;
    buffer_by_end.erase(free_it);
    buffer_by_size.erase(extent);
    if (extent.start == location)
    {
        extent.start += size;
        buffer_by_end.insert(extent);
        buffer_by_size.insert(extent);
    }
    else if (extent.end == location+size)
    {
        extent.end -= size;
        buffer_by_end.insert(extent);
        buffer_by_size.insert(extent);
    }
    else
    {
        buffer_by_end.insert((heap_extent_t){ .start = extent.start, .end = location });
        buffer_by_size.insert((heap_extent_t){ .start = extent.start, .end = location });
        buffer_by_end.insert((heap_extent_t){ .start = location+size, .end = extent.end });
        buffer_by_size.insert((heap_extent_t){ .start = location+size, .end = extent.end });
    }
    buffer_area_used_space += size;
}

void blockstore_heap_t::free_buffer_area(inode_t inode, uint64_t location, uint64_t size)
{
    auto next_it = buffer_by_end.lower_bound((heap_extent_t){ .end = location+size });
    auto prev_it = next_it == buffer_by_end.begin() ? buffer_by_end.end() : std::prev(next_it);
    assert(next_it == buffer_by_end.end() || next_it->start >= location+size);
    assert(prev_it == buffer_by_end.end() || prev_it->end <= location);
    bool merge_prev = (prev_it != buffer_by_end.end() && prev_it->end == location);
    bool merge_next = (next_it != buffer_by_end.end() && next_it->start == location+size);
    uint64_t prev_start = merge_prev ? prev_it->start : 0;
    uint64_t next_end = merge_next ? next_it->end : 0;
    if (merge_prev && merge_next)
    {
        buffer_by_size.erase(*prev_it);
        buffer_by_size.erase(*next_it);
        buffer_by_end.erase(prev_it);
        buffer_by_end.erase(next_it);
        buffer_by_end.insert((heap_extent_t){ .start = prev_start, .end = next_end });
        buffer_by_size.insert((heap_extent_t){ .start = prev_start, .end = next_end });
    }
    else if (merge_prev)
    {
        buffer_by_size.erase(*prev_it);
        buffer_by_end.erase(prev_it);
        buffer_by_end.insert((heap_extent_t){ .start = prev_start, .end = location+size });
        buffer_by_size.insert((heap_extent_t){ .start = prev_start, .end = location+size });
    }
    else if (merge_next)
    {
        buffer_by_size.erase(*next_it);
        buffer_by_end.erase(next_it);
        buffer_by_end.insert((heap_extent_t){ .start = location, .end = next_end });
        buffer_by_size.insert((heap_extent_t){ .start = location, .end = next_end });
    }
    else
    {
        buffer_by_end.insert((heap_extent_t){ .start = location, .end = location+size });
        buffer_by_size.insert((heap_extent_t){ .start = location, .end = location+size });
    }
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
    return inf.used_space-inf.virtual_free_space;
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
    return compact_queue.size();
}

uint32_t blockstore_heap_t::get_max_write_entry_size()
{
    return max_write_entry_size;
}

void blockstore_heap_t::set_fail_on_warn(bool fail)
{
    fail_on_warn = fail;
}
