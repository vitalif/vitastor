// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include "../util/malloc_or_die.h"
#include "../util/allocator.h"
#include "blockstore_heap.h"
#include "../util/crc32c.h"

static int count_writes(heap_object_t *obj)
{
    int n = 0;
    for (auto wr = obj->get_writes(); wr; wr = wr->next())
    {
        n++;
    }
    return n;
}

#define FREE_SPACE_BIT 0x8000

bool check_used_space(blockstore_heap_t & heap, blockstore_disk_t & dsk, uint32_t block_num)
{
    uint8_t *data = heap.get_meta_block(block_num);
    uint8_t *end = data+dsk.meta_block_size;
    uint32_t used = 0;
    while (data < end)
    {
        uint16_t region_marker = *((uint16_t*)data);
        if (!(region_marker & FREE_SPACE_BIT))
        {
            used += region_marker;
        }
        data += (region_marker & ~FREE_SPACE_BIT);
    }
    return used == heap.get_meta_block_used_space(block_num);
}

int count_free_fragments(blockstore_heap_t & heap, blockstore_disk_t & dsk, uint32_t block_num)
{
    uint8_t *data = heap.get_meta_block(block_num);
    uint8_t *end = data+dsk.meta_block_size;
    int fragments = 0;
    bool is_free = false;
    while (data < end)
    {
        uint16_t region_marker = *((uint16_t*)data);
        if ((region_marker & FREE_SPACE_BIT) && !is_free)
        {
            fragments++;
        }
        is_free = !!(region_marker & FREE_SPACE_BIT);
        data += (region_marker & ~FREE_SPACE_BIT);
    }
    return fragments;
}

int _test_do_big_write(blockstore_heap_t & heap, blockstore_disk_t & dsk, uint64_t inode, uint64_t stripe, uint64_t version, uint64_t location,
    bool stable = true, uint32_t offset = 0, uint32_t len = 0)
{
    if (!offset && !len)
        len = dsk.data_block_size;
    object_id oid = { .inode = INODE_WITH_POOL(1, inode), .stripe = stripe };
    uint8_t wr_buf[heap.get_max_write_entry_size()];
    heap_write_t *wr = (heap_write_t*)wr_buf;
    wr->version = version;
    wr->offset = offset;
    wr->len = len;
    wr->location = location;
    wr->flags = BS_HEAP_BIG_WRITE | (stable ? BS_HEAP_STABLE : 0);
    assert(heap.get_max_write_entry_size() >= wr->get_size(&heap));
    assert(wr->get_size(&heap) == sizeof(heap_write_t) + 2*dsk.clean_entry_bitmap_size + (dsk.csum_block_size
        ? dsk.data_block_size/dsk.csum_block_size*4 : 0));
    memset(wr->get_ext_bitmap(&heap), 0xff, dsk.clean_entry_bitmap_size);
    if (dsk.csum_block_size)
        memset(wr->get_checksums(&heap), 0xde, dsk.data_block_size/dsk.csum_block_size*4);
    uint32_t mblock;
    return heap.post_write(oid, wr, &mblock);
}

void _test_big_write(blockstore_heap_t & heap, blockstore_disk_t & dsk, uint64_t inode, uint64_t stripe, uint64_t version, uint64_t location,
    bool stable = true, uint32_t offset = 0, uint32_t len = 0)
{
    heap.use_data(INODE_WITH_POOL(1, inode), location); // blocks are allocated before write and outside the heap_t
    int res = _test_do_big_write(heap, dsk, inode, stripe, version, location, stable, offset, len);
    assert(res == 0);
    assert(heap.is_data_used(location));
}

int _test_do_small_write(blockstore_heap_t & heap, blockstore_disk_t & dsk, uint64_t inode, uint64_t stripe, uint64_t version,
    uint32_t offset, uint32_t len, uint64_t location, bool stable = true, uint32_t *checksums = NULL, bool is_intent = false)
{
    object_id oid = { .inode = INODE_WITH_POOL(1, inode), .stripe = stripe };
    uint8_t wr_buf[heap.get_max_write_entry_size()];
    heap_write_t *wr = (heap_write_t*)wr_buf;
    wr->version = version;
    wr->offset = offset;
    wr->len = len;
    wr->location = location;
    wr->flags = (is_intent ? BS_HEAP_INTENT_WRITE : BS_HEAP_SMALL_WRITE) | (stable ? BS_HEAP_STABLE : 0);
    assert(wr->get_size(&heap) == sizeof(heap_write_t) + dsk.clean_entry_bitmap_size + (dsk.csum_block_size
        ? ((offset+len+dsk.csum_block_size-1)/dsk.csum_block_size - offset/dsk.csum_block_size)*4 : 4));
    memset(wr->get_ext_bitmap(&heap), 0xff, dsk.clean_entry_bitmap_size);
    assert(!wr->get_int_bitmap(&heap));
    if (checksums)
    {
        if (dsk.csum_block_size)
            memcpy(wr->get_checksums(&heap), checksums, wr->get_csum_size(&heap));
        else
            *wr->get_checksum(&heap) = *checksums;
    }
    else if (dsk.csum_block_size)
        memset(wr->get_checksums(&heap), 0xab, ((offset+len+dsk.csum_block_size-1)/dsk.csum_block_size - offset/dsk.csum_block_size)*4);
    else
        *wr->get_checksum(&heap) = 0xabababab;
    uint32_t mblock;
    return heap.post_write(oid, wr, &mblock);
}

void _test_small_write(blockstore_heap_t & heap, blockstore_disk_t & dsk, uint64_t inode, uint64_t stripe, uint64_t version,
    uint32_t offset, uint32_t len, uint64_t location, bool stable = true, uint32_t *checksums = NULL, bool is_intent = false)
{
    if (!is_intent)
        heap.use_buffer_area(INODE_WITH_POOL(1, inode), location, len); // blocks are allocated before write and outside the heap_t
    int res = _test_do_small_write(heap, dsk, inode, stripe, version, offset, len, location, stable, checksums, is_intent);
    assert(res == 0);
    if (!is_intent)
        assert(!heap.is_buffer_area_free(location, len));
}

void _test_init(blockstore_disk_t & dsk, bool csum)
{
    std::map<std::string, std::string> config;
    if (csum)
        config["data_csum_type"] = "crc32c";
    dsk.parse_config(config);
    dsk.data_device_size = 1*1024*1024*1024;
    dsk.meta_device_size = 4*1024*1024;
    dsk.journal_device_size = 4*1024*1024;
    dsk.data_fd = 0;
    dsk.meta_fd = 1;
    dsk.journal_fd = 2;
    dsk.disable_journal_fsync = dsk.disable_meta_fsync = true;
    dsk.calc_lengths();
}

void test_mvcc(bool csum)
{
    blockstore_disk_t dsk;
    _test_init(dsk, csum);
    std::vector<uint8_t> buffer_area(dsk.journal_device_size);
    blockstore_heap_t heap(&dsk, buffer_area.data());
    heap.finish_load();

    // write, read, modify, check basic mvcc
    {
        assert(_test_do_small_write(heap, dsk, 1, 0, 1, 0, 4096, 0) == EINVAL);

        assert(heap.find_free_data() == 0);

        _test_big_write(heap, dsk, 1, 0, 1, 0);
        assert(heap.get_meta_block_used_space(0) == sizeof(heap_object_t) + sizeof(heap_write_t) +
            2*dsk.clean_entry_bitmap_size + (dsk.csum_block_size ? dsk.data_block_size/dsk.csum_block_size*4 : 0));
        assert(check_used_space(heap, dsk, 0));
        assert(heap.get_meta_used_space() == heap.get_meta_block_used_space(0));

        assert(heap.find_free_data() == 0x20000);

        object_id oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0 };
        uint64_t copy_id = 0;
        heap_object_t *obj = heap.lock_and_read_entry(oid, copy_id);
        assert(obj);
        assert(copy_id == 1);
        assert(count_writes(obj) == 1);
        heap_write_t *wr = obj->get_writes();
        assert(wr->lsn == 1);
        assert(wr->version == 1);
        assert(wr->offset == 0);
        assert(wr->len == dsk.data_block_size);
        assert(wr->location == 0);
        assert(wr->flags == BS_HEAP_BIG_WRITE|BS_HEAP_STABLE);
        uint64_t old_size = obj->size + wr->size;

        assert(heap.read_locked_entry(oid, copy_id) == obj);

        _test_small_write(heap, dsk, 1, 0, 2, 8192, 4096, 16384, true);
        obj = heap.read_entry(oid, NULL);
        assert(check_used_space(heap, dsk, 0));
        assert(heap.get_meta_block_used_space(0) == old_size + obj->get_writes()->get_size(&heap));

        assert(_test_do_small_write(heap, dsk, 1, 0, 1, 0, 4096, 0) == EINVAL);

        assert(!heap.read_locked_entry(oid, UINT64_MAX));
        assert(heap.read_locked_entry(oid, copy_id) == obj); // small_write isn't MVCCed

        _test_big_write(heap, dsk, 1, 0, 3, 0x20000);
        obj = heap.read_entry(oid, NULL);
        assert(count_writes(obj) == 1);
        wr = obj->get_writes();
        assert(wr->lsn == 3);
        assert(wr->version == 3);
        assert(wr->flags == BS_HEAP_BIG_WRITE|BS_HEAP_STABLE);

        assert(heap.read_locked_entry(oid, copy_id) != obj); // big_write is MVCCed
        obj = heap.read_locked_entry(oid, copy_id);
        assert(count_writes(obj) == 2);
        wr = obj->get_writes();
        assert(wr->lsn == 2);

        assert(!heap.unlock_entry(oid, UINT64_MAX));
        assert(heap.unlock_entry(oid, copy_id));
    }

    printf("OK test_mvcc %s\n", csum ? "csum" : "no_csum");
}

void test_update(bool csum)
{
    blockstore_disk_t dsk;
    _test_init(dsk, csum);
    std::vector<uint8_t> buffer_area(dsk.journal_device_size);
    blockstore_heap_t heap(&dsk, buffer_area.data());
    heap.finish_load();

    {
        _test_big_write(heap, dsk, 1, 0, 1, 0x20000);

        _test_big_write(heap, dsk, 1, 0x20000, 1, 0x40000);

        _test_small_write(heap, dsk, 1, 0, 2, 8192, 4096, 16384, true);
    }

    printf("OK test_update %s\n", csum ? "csum" : "no_csum");
}

void test_delete(bool csum)
{
    blockstore_disk_t dsk;
    _test_init(dsk, csum);
    std::vector<uint8_t> buffer_area(dsk.journal_device_size);
    blockstore_heap_t heap(&dsk, buffer_area.data());
    heap.finish_load();

    {
        _test_big_write(heap, dsk, 1, 0, 1, 0x20000);

        _test_big_write(heap, dsk, 1, 0x20000, 1, 0x40000);

        auto & space = heap.get_inode_space_stats();
        assert(space.at(INODE_WITH_POOL(1, 1)) == 0x40000);
        assert(heap.get_data_used_space() == 0x40000);

        object_id oid = { .inode = INODE_WITH_POOL(1, 2), .stripe = 0 };
        int res = heap.post_delete(oid, NULL, NULL);
        assert(res == ENOENT);

        uint32_t mblock = 100;
        uint64_t new_lsn = 0;
        oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0 };
        res = heap.post_delete(oid, &new_lsn, &mblock);
        assert(mblock == 0);
        assert(res == 0);
        heap.mark_lsn_completed(new_lsn);

        uint64_t lsn = 0;
        heap_object_t *obj = heap.lock_and_read_entry(oid, lsn);
        assert(!obj);
    }

    printf("OK test_delete %s\n", csum ? "csum" : "no_csum");
}

void test_compact_block()
{
    blockstore_disk_t dsk;
    _test_init(dsk, true);
    std::vector<uint8_t> buffer_area(dsk.journal_device_size);
    dsk.meta_area_size = 4096*3;
    blockstore_heap_t heap(&dsk, buffer_area.data());
    heap.finish_load();

    uint32_t big_write_size = (sizeof(heap_object_t) + sizeof(heap_write_t) + 2*dsk.clean_entry_bitmap_size + dsk.data_block_size/dsk.csum_block_size*4);
    uint32_t small_write_size = (sizeof(heap_write_t) + dsk.clean_entry_bitmap_size + 4);
    assert(big_write_size == 197);
    assert(small_write_size == 45);
    uint32_t nwr = dsk.meta_block_size/(big_write_size+small_write_size);

    {
        for (uint32_t i = 0; i < nwr*2; i++)
        {
            _test_big_write(heap, dsk, 1, i*0x20000, 1, i*0x20000);
            _test_small_write(heap, dsk, 1, i*0x20000, 2, 0, 4096, i*4096, true);
        }
        assert(_test_do_big_write(heap, dsk, 1, (nwr*2+1)*0x20000, 1, (nwr*2+1)*0x20000) == ENOSPC);
        // Compact all small writes
        for (uint32_t i = 0; i < nwr*2; i++)
        {
            int res = heap.compact_object((object_id){ .inode = INODE_WITH_POOL(1, 1), .stripe = i*0x20000 }, 1000000, NULL);
            assert(res == 0);
        }
        // Check fragmentation
        assert(count_free_fragments(heap, dsk, 0) == nwr);
        assert(count_free_fragments(heap, dsk, 1) == nwr);
        // Write 3 more objects
        _test_big_write(heap, dsk, 1, nwr*2*0x20000, 1, nwr*2*0x20000);
        _test_big_write(heap, dsk, 1, (nwr*2+1)*0x20000, 1, (nwr*2+1)*0x20000);
        _test_big_write(heap, dsk, 1, (nwr*2+2)*0x20000, 1, (nwr*2+2)*0x20000);
        assert(count_free_fragments(heap, dsk, 0) == 1);
    }

    printf("OK test_compact_block\n");
}

void test_compact(bool csum, bool stable)
{
    int res;
    blockstore_disk_t dsk;
    _test_init(dsk, csum);
    std::vector<uint8_t> buffer_area(dsk.journal_device_size);

    blockstore_heap_t heap(&dsk, buffer_area.data());
    heap.finish_load();

    _test_big_write(heap, dsk, 1, 0, 1, 0x20000, true, 0, 4096);

    // write unstable - stabilize - compact
    object_id oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0 };
    uint64_t copy_id = 0;
    heap_object_t *obj = heap.lock_and_read_entry(oid, copy_id);
    assert(obj);
    assert(count_writes(obj) == 1);
    assert(obj->get_writes()->flags == BS_HEAP_BIG_WRITE|BS_HEAP_STABLE);
    uint8_t ref_int_bitmap[dsk.clean_entry_bitmap_size];
    memset(ref_int_bitmap, 0, dsk.clean_entry_bitmap_size);
    bitmap_set(ref_int_bitmap, 0, 4096, 4096);
    assert(!memcmp(obj->get_writes()->get_int_bitmap(&heap), ref_int_bitmap, dsk.clean_entry_bitmap_size));
    uint64_t old_size = obj->size + obj->get_writes()->size;

    _test_small_write(heap, dsk, 1, 0, 3, 8192, 4096, 16384, stable);
    obj = heap.read_entry(oid, NULL);
    uint64_t wr_size = obj->get_writes()->get_size(&heap);
    assert(obj->get_writes()->lsn == 2);
    assert(check_used_space(heap, dsk, 0));
    assert(heap.get_meta_block_used_space(0) == old_size + wr_size);

    _test_big_write(heap, dsk, 2, 0, 1, 0x40000, true, 0, 4096);

    obj = heap.read_locked_entry(oid, copy_id);
    assert(obj);
    assert(count_writes(obj) == 2);

    heap.mark_lsn_completed(1);
    heap.mark_lsn_completed(2);
    heap.mark_lsn_completed(3);

    uint32_t mblock;
    object_id compact_oid = {};
    if (!stable)
    {
        res = heap.get_next_compact(compact_oid);
        assert(res == ENOENT);

        uint64_t new_lsn = 0, new_to_lsn = 0;
        res = heap.post_stabilize({ .inode = INODE_WITH_POOL(1, 2), .stripe = 0 }, 3, NULL, &new_lsn, &new_to_lsn);
        assert(res == ENOENT);
        res = heap.post_stabilize(oid, 5, NULL, &new_lsn, &new_to_lsn);
        assert(res == ENOENT);
        res = heap.post_stabilize(oid, 1, &mblock, &new_lsn, &new_to_lsn);
        assert(res == 0);
        assert(new_lsn == 0);
        assert(new_to_lsn == 0);
        res = heap.post_stabilize(oid, 3, &mblock, &new_lsn, &new_to_lsn);
        assert(res == 0);
        assert(mblock == 0);
        assert(new_lsn == 4);
        assert(new_to_lsn == 4);
        assert(check_used_space(heap, dsk, 0));
        assert(heap.get_meta_block_used_space(0) == 2*old_size + wr_size);
        heap.mark_lsn_completed(4);
    }

    assert(heap.get_compact_queue_size() == 1);
    res = heap.get_next_compact(compact_oid);
    assert(res == 0);
    assert(oid == compact_oid);

    heap_write_t *compact_begin = NULL, *compact_end = NULL;
    obj = heap.read_entry(oid, NULL);
    assert(obj);
    assert(count_writes(obj) == 2);
    heap.get_compact_range(obj, 4, &compact_begin, &compact_end);
    assert(compact_begin == obj->get_writes());
    assert(compact_end == obj->get_writes()->next());

    heap.mark_object_compacted(obj, 4);
    assert(heap.get_compacted_lsn() == (stable ? 3 : 4));
    assert(heap.get_compact_queue_size() == 0);

    res = heap.compact_object((object_id){ .inode = INODE_WITH_POOL(1, 3), .stripe = 0 }, compact_begin->lsn, NULL);
    assert(res == ENOENT);

    heap.mark_lsn_trimmed((stable ? 3 : 4));
    assert(check_used_space(heap, dsk, 0));
    assert(heap.get_meta_block_used_space(0) == 2*old_size);

    obj = heap.read_entry(oid, NULL);
    assert(obj);
    assert(count_writes(obj) == 1);
    assert(obj->get_writes()->version == 3);
    bitmap_set(ref_int_bitmap, 8192, 4096, 4096);
    assert(!memcmp(obj->get_writes()->get_int_bitmap(&heap), ref_int_bitmap, dsk.clean_entry_bitmap_size));
    if (csum)
    {
        uint8_t ref_csums[dsk.data_block_size/dsk.csum_block_size*4];
        memset(ref_csums, 0xde, sizeof(ref_csums));
        memset(ref_csums+8, 0xab, 4);
        assert(!memcmp(obj->get_writes()->get_checksums(&heap), ref_csums, sizeof(ref_csums)));
    }

    obj = heap.read_entry({ .inode = INODE_WITH_POOL(1, 2), .stripe = 0 }, NULL);
    assert(obj);
    assert(count_writes(obj) == 1);
    assert(obj->get_writes()->version == 1);

    int unlock_res = heap.unlock_entry(oid, copy_id);
    assert(unlock_res);

    printf("OK test_compact %s %s\n", stable ? "stable" : "unstable", csum ? "csum" : "no_csum");
}

void test_modify_bitmap()
{
    blockstore_disk_t dsk;
    _test_init(dsk, false);
    std::vector<uint8_t> buffer_area(dsk.journal_device_size);

    blockstore_heap_t heap(&dsk, buffer_area.data());
    heap.finish_load();

    _test_big_write(heap, dsk, 1, 0, 1, 0x20000);

    uint64_t copy_id = 0;
    object_id oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0 };
    heap_object_t *obj = heap.lock_and_read_entry(oid, copy_id);
    assert(obj);

    uint32_t modified_block = 1;
    obj = heap.read_entry(oid, &modified_block, true);
    assert(obj);
    assert(modified_block == 0);
    uint8_t *bmp = obj->get_writes()->get_int_bitmap(&heap);
    bitmap_clear(bmp, 4096, 16384, dsk.bitmap_granularity);
    obj->crc32c = obj->calc_crc32c();

    uint8_t ref_int_bitmap[dsk.clean_entry_bitmap_size];
    memset(ref_int_bitmap, 0xFF, dsk.clean_entry_bitmap_size);

    obj = heap.read_locked_entry(oid, copy_id);
    assert(obj);
    assert(!memcmp(obj->get_writes()->get_int_bitmap(&heap), ref_int_bitmap, dsk.clean_entry_bitmap_size));

    obj = heap.read_entry(oid, NULL);
    assert(obj);
    bitmap_clear(ref_int_bitmap, 4096, 16384, dsk.bitmap_granularity);
    assert(!memcmp(obj->get_writes()->get_int_bitmap(&heap), ref_int_bitmap, dsk.clean_entry_bitmap_size));

    int unlock_res = heap.unlock_entry(oid, copy_id);
    assert(unlock_res);

    printf("OK test_modify_bitmap\n");
}

void test_recheck(bool async, bool csum, bool intent)
{
    blockstore_disk_t dsk;
    _test_init(dsk, csum);
    std::vector<uint8_t> buffer_area(dsk.journal_device_size);
    std::vector<uint8_t> tmp;

    memset(buffer_area.data(), 0xab, 12288);
    uint32_t csum_4096 = crc32c(0, buffer_area.data(), 4096);
    uint32_t csum_8192 = crc32c(0, buffer_area.data(), 8192);
    uint32_t csum_12288 = crc32c(0, buffer_area.data(), 12288);
    uint32_t three_csums[3] = { csum_4096, csum_4096, csum_4096 };

    // write
    {
        blockstore_heap_t heap(&dsk, buffer_area.data());
        heap.finish_load();

        // object 1
        _test_big_write(heap, dsk, 1, 0, 1, 0x20000);
        _test_small_write(heap, dsk, 1, 0, 2, 8192, 8*1024, 16*1024, true, csum ? three_csums : &csum_8192, intent);

        // object 2
        _test_big_write(heap, dsk, 2, 0, 1, 0x40000);
        _test_small_write(heap, dsk, 2, 0, 2, 8192, 12*1024, 24*1024, true, csum ? three_csums : &csum_12288, intent);

        // persist
        assert(heap.get_meta_block_used_space(0) > 0);
        tmp.resize(dsk.meta_block_size);
        memcpy(tmp.data(), heap.get_meta_block(0), dsk.meta_block_size);
    }

    // reload heap
    {
        memset(buffer_area.data()+16*1024, 0xab, 20*1024); // valid data
        memset(buffer_area.data()+20*1024+64, 0xcc, 4); // invalid data in the second block of the first write

        blockstore_heap_t heap(&dsk, async ? NULL : buffer_area.data(), 10);
        heap.load_blocks(0, dsk.meta_block_size, tmp.data());

        int calls = 0;
        bool done = heap.recheck_small_writes([&](bool is_data, uint64_t offset, uint64_t len, uint8_t *buf, std::function<void()> cb)
        {
            calls++;
            if (len)
            {
                if (!intent)
                {
                    assert(!is_data);
                    assert(offset == 16384 && len == 8192 || offset == 24*1024 && len == 12*1024);
                    memcpy(buf, buffer_area.data()+offset, len);
                }
                else
                {
                    assert(is_data);
                    assert(offset == 0x20000+8192 && len == 8192 || 0x40000+8192 && len == 12*1024);
                    memcpy(buf, buffer_area.data() + (offset == 0x20000+8192 ? 16*1024 : 24*1024), len);
                }
                assert(cb);
                cb();
            }
        }, 1);
        assert(done);
        assert(calls == (async || intent ? 3 : 1));

        heap.finish_load();

        // read object 1 - big_write should be there but small_write should be rechecked and removed
        object_id oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0 };
        heap_object_t *obj = heap.read_entry(oid, NULL);
        assert(obj);
        assert(count_writes(obj) == 1);
        heap_write_t *wr = obj->get_writes();
        assert(wr->lsn == 1);
        assert(wr->version == 1);
        assert(wr->offset == 0);
        assert(wr->len == dsk.data_block_size);
        assert(wr->location == 0x20000);
        assert(wr->flags == BS_HEAP_BIG_WRITE|BS_HEAP_STABLE);

        // read object 2 - both writes should be present
        oid = { .inode = INODE_WITH_POOL(1, 2), .stripe = 0 };
        obj = heap.read_entry(oid, NULL);
        assert(obj);
        assert(count_writes(obj) == 2);
        wr = obj->get_writes();
        assert(wr->lsn == 4);
        assert(wr->version == 2);
        assert(wr->offset == 8192);
        assert(wr->len == 12*1024);
        assert(wr->location == 24*1024);
        assert(wr->flags == BS_HEAP_SMALL_WRITE|BS_HEAP_STABLE);
    }

    printf("OK test_recheck %s %s %s\n", async ? "async" : "sync", csum ? "csum" : "no_csum", intent ? "intent" : "buffered");
}

void test_corruption()
{
    int res;
    blockstore_disk_t dsk;
    _test_init(dsk, false);
    std::vector<uint8_t> buffer_area(dsk.journal_device_size);
    std::vector<uint8_t> tmp;

    // write
    {
        blockstore_heap_t heap(&dsk, buffer_area.data());
        heap.finish_load();

        // big_write
        _test_big_write(heap, dsk, 1, 0, 1, 0x20000);

        object_id oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0 };
        heap_object_t *obj = heap.read_entry(oid, NULL);
        assert(obj);

        // big_write object 2
        _test_big_write(heap, dsk, 1, 0x20000, 1, 0x40000);

        // big_write object 3
        _test_big_write(heap, dsk, 1, 0x40000, 1, 0x60000);

        // tombstone object 4
        oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0x60000 };
        uint8_t wr_buf[heap.get_max_write_entry_size()];
        heap_write_t *wr = (heap_write_t*)wr_buf;
        wr->version = 2;
        wr->offset = 0;
        wr->len = 0;
        wr->location = 0;
        wr->flags = BS_HEAP_TOMBSTONE|BS_HEAP_STABLE;
        assert(!wr->get_checksums(&heap));
        res = heap.post_write(oid, wr, NULL);
        assert(res == 0);

        // try to do a small_write over a tombstone to fail
        wr->version = 3;
        wr->flags = BS_HEAP_SMALL_WRITE|BS_HEAP_STABLE;
        res = heap.post_write(oid, wr, NULL);
        assert(res == EINVAL);

        // persist
        assert(heap.get_meta_block_used_space(0) > 0);
        assert(heap.get_meta_block_used_space(1) == 0);
        tmp.resize(dsk.meta_block_size);
        memcpy(tmp.data(), heap.get_meta_block(0), dsk.meta_block_size);
    }

    // reload heap with corruption
    {
        blockstore_heap_t heap(&dsk, buffer_area.data());
        heap.set_fail_on_warn(true);
        tmp.data()[10]++; // corrupt the first object
        heap.load_blocks(0, dsk.meta_block_size, tmp.data());
        heap.finish_load();

        // read object - object should be not present (checksum is invalid)
        object_id oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0 };
        uint64_t lsn = 0;
        heap_object_t *obj = heap.lock_and_read_entry(oid, lsn);
        assert(!obj);

        // object 2 should be present
        oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0x20000 };
        obj = heap.lock_and_read_entry(oid, lsn);
        assert(obj);
        assert(count_writes(obj) == 1);
        heap_write_t *wr = obj->get_writes();
        assert(wr->location == 0x40000);
        assert(wr->flags == BS_HEAP_BIG_WRITE|BS_HEAP_STABLE);

        // object 3 should be present
        oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0x40000 };
        obj = heap.read_entry(oid, NULL);
        assert(obj);
        assert(count_writes(obj) == 1);
        wr = obj->get_writes();
        assert(wr->location == 0x60000);
        assert(wr->flags == BS_HEAP_BIG_WRITE|BS_HEAP_STABLE);

        // object 4 should be a tombstone
        oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0x60000 };
        obj = heap.read_entry(oid, NULL);
        assert(obj);
        assert(count_writes(obj) == 1);
        wr = obj->get_writes();
        assert(wr->flags == BS_HEAP_TOMBSTONE|BS_HEAP_STABLE);
    }

    printf("OK test_corruption\n");
}

void test_full_overwrite(bool stable)
{
    int res;
    blockstore_disk_t dsk;
    _test_init(dsk, false);
    std::vector<uint8_t> buffer_area(dsk.journal_device_size);

    // write
    {
        blockstore_heap_t heap(&dsk, buffer_area.data());
        heap.finish_load();

        // big_write
        _test_big_write(heap, dsk, 1, 0, 1, 0x20000);
        heap.mark_lsn_completed(1);

        // read it to test mvcc
        object_id oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0 };
        uint64_t copy_id = 0;
        heap_object_t *obj = heap.lock_and_read_entry(oid, copy_id);
        assert(obj);

        // small_write
        _test_small_write(heap, dsk, 1, 0, 2, 8192, 4096, 16384, true);
        heap.mark_lsn_completed(2);

        // big_write again
        _test_big_write(heap, dsk, 1, 0, 3, 0x40000, stable, 16384, 4096);
        assert(!heap.is_buffer_area_free(16384, 4096)); // should not be freed because MVCC includes it
        assert(heap.is_data_used(0x20000)); // should NOT be freed - still referenced by MVCC
        heap.mark_lsn_completed(3);

        // free mvcc
        heap.unlock_entry(oid, copy_id);
        if (stable)
        {
            assert(!heap.is_data_used(0x20000)); // should now be freed
        }

        // small_write again
        if (!stable)
        {
            res = _test_do_small_write(heap, dsk, 1, 0, 4, 20480, 4096, 20480, true);
            assert(res == EINVAL);
        }
        _test_small_write(heap, dsk, 1, 0, 4, 20480, 4096, 20480, stable);
        heap.mark_lsn_completed(4);

        if (!stable)
        {
            res = heap.post_stabilize(oid, 4, NULL, NULL, NULL);
            heap.mark_lsn_completed(5);
            assert(res == 0);
        }

        // read object
        obj = heap.read_entry(oid, NULL);
        assert(obj);
        assert(count_writes(obj) == 2);
        heap_write_t *wr = obj->get_writes();
        assert(wr->version == 4);
        assert(wr->location == 20480);
        assert(wr->flags == BS_HEAP_SMALL_WRITE|BS_HEAP_STABLE);
        wr = wr->next();
        assert(wr->version == 3);
        assert(wr->location == 0x40000);
        assert(wr->flags == BS_HEAP_BIG_WRITE|BS_HEAP_STABLE);

        // check that the data block 0x20000 is freed and 0x40000 is used
        assert(!heap.is_data_used(0x20000));
        assert(heap.is_data_used(0x40000));
        assert(heap.is_buffer_area_free(16384, 4096));
        assert(!heap.is_buffer_area_free(20480, 4096));
    }

    printf("OK test_full_overwrite %s\n", stable ? "stable" : "unstable");
}

void test_reshard_list()
{
    int res;
    blockstore_disk_t dsk;
    _test_init(dsk, false);
    std::vector<uint8_t> buffer_area(dsk.journal_device_size);

    // write
    {
        blockstore_heap_t heap(&dsk, buffer_area.data());
        heap.finish_load();

        _test_big_write(heap, dsk, 1, 0, 1, 0x20000);
        _test_big_write(heap, dsk, 1, 0x20000, 1, 0x40000);
        _test_big_write(heap, dsk, 1, 0x40000, 1, 0);
        _test_big_write(heap, dsk, 2, 0x60000, 1, 0x60000);
        _test_big_write(heap, dsk, 2, 0x60000, 2, 0x80000, false);

        obj_ver_id *listing = NULL;
        size_t stable_count = 0, unstable_count = 0;
        res = heap.list_objects(1, INODE_WITH_POOL(0, 1), INODE_WITH_POOL(1, 1), &listing, &stable_count, &unstable_count);
        assert(res == EINVAL);
        res = heap.list_objects(1, INODE_WITH_POOL(1, 1), INODE_WITH_POOL(2, 1), &listing, &stable_count, &unstable_count);
        assert(res == EINVAL);
        res = heap.list_objects(2, INODE_WITH_POOL(1, 1), INODE_WITH_POOL(1, 1), &listing, &stable_count, &unstable_count);
        assert(res == EINVAL);

        res = heap.list_objects(1, INODE_WITH_POOL(1, 1), INODE_WITH_POOL(1, UINT64_MAX), &listing, &stable_count, &unstable_count);
        assert(res == 0);
        assert(stable_count == 4);
        assert(unstable_count == 1);
        free(listing);
        listing = NULL;

        res = heap.list_objects(1, INODE_WITH_POOL(1, 1), INODE_WITH_POOL(1, 1), &listing, &stable_count, &unstable_count);
        assert(res == 0);
        assert(stable_count == 3);
        assert(unstable_count == 0);
        free(listing);
        listing = NULL;

        heap.reshard(1, 2, 0x20000);

        assert(heap.read_entry((object_id){ .inode = INODE_WITH_POOL(1, 1), .stripe = 0 }, NULL));
        assert(heap.read_entry((object_id){ .inode = INODE_WITH_POOL(1, 1), .stripe = 0x20000 }, NULL));
        assert(heap.read_entry((object_id){ .inode = INODE_WITH_POOL(1, 1), .stripe = 0x40000 }, NULL));
        assert(heap.read_entry((object_id){ .inode = INODE_WITH_POOL(1, 2), .stripe = 0x60000 }, NULL));
        assert(!heap.read_entry((object_id){ .inode = INODE_WITH_POOL(1, 2), .stripe = 0x80000 }, NULL));

        res = heap.list_objects(3, INODE_WITH_POOL(1, 1), INODE_WITH_POOL(1, 1), &listing, &stable_count, &unstable_count);
        assert(res == EINVAL);
        res = heap.list_objects(1, INODE_WITH_POOL(1, 1), INODE_WITH_POOL(1, 1), &listing, &stable_count, &unstable_count);
        assert(res == 0);
        assert(stable_count == 2);
        assert(unstable_count == 0);
        free(listing);
        listing = NULL;

        res = heap.list_objects(2, INODE_WITH_POOL(1, 1), INODE_WITH_POOL(1, UINT64_MAX), &listing, &stable_count, &unstable_count);
        assert(res == 0);
        assert(stable_count == 2);
        assert(unstable_count == 1);
        free(listing);
        listing = NULL;
    }

    printf("OK test_reshard_list\n");
}

void _test_invalid_data_setup(blockstore_disk_t & dsk, std::vector<uint8_t> & buffer_area, std::vector<uint8_t> & tmp)
{
    tmp.clear();
    tmp.resize(dsk.meta_block_size*2);

    heap_object_t *obj = (heap_object_t*)tmp.data();
    obj->size = sizeof(heap_object_t);
    obj->inode = INODE_WITH_POOL(1, 1);
    obj->write_pos = sizeof(heap_object_t);
    heap_write_t *wr = obj->get_writes();
    wr->next_pos = 0;
    wr->lsn = 1;
    wr->version = 1;
    wr->flags = BS_HEAP_TOMBSTONE;
    wr->size = sizeof(heap_write_t);
    obj->crc32c = obj->calc_crc32c();

    obj = (heap_object_t*)((uint8_t*)wr + wr->size);
    obj->size = sizeof(heap_object_t);
    obj->inode = INODE_WITH_POOL(1, 3);
    obj->stripe = 0;
    obj->write_pos = sizeof(heap_object_t);
    wr = obj->get_writes();
    wr->lsn = 1;
    wr->version = 1;
    wr->flags = BS_HEAP_TOMBSTONE;
    wr->size = sizeof(heap_write_t);
    obj->crc32c = obj->calc_crc32c();

    obj = (heap_object_t*)(tmp.data() + dsk.meta_block_size);
    obj->size = sizeof(heap_object_t);
    obj->inode = INODE_WITH_POOL(1, 2);
    obj->write_pos = sizeof(heap_object_t);
    wr = obj->get_writes();
    wr->lsn = 2;
    wr->version = 1;
    wr->flags = BS_HEAP_TOMBSTONE;
    wr->size = sizeof(heap_write_t);
    obj->crc32c = obj->calc_crc32c();
}

void test_invalid_data()
{
    blockstore_disk_t dsk;
    _test_init(dsk, false);
    std::vector<uint8_t> buffer_area(dsk.journal_device_size);
    std::vector<uint8_t> tmp;

    // Too small object
    {
        _test_invalid_data_setup(dsk, buffer_area, tmp);
        heap_object_t *obj = (heap_object_t*)tmp.data();
        obj->size = sizeof(heap_object_t)-2;
        *((uint16_t*)(tmp.data()+sizeof(heap_object_t)-2)) = 0x8002;

        blockstore_heap_t heap(&dsk, buffer_area.data());
        heap.load_blocks(0, dsk.meta_block_size*2, tmp.data());
        heap.finish_load();

        object_id oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0 };
        assert(!heap.read_entry(oid, NULL));

        oid = { .inode = INODE_WITH_POOL(1, 2), .stripe = 0 };
        assert(heap.read_entry(oid, NULL));
    }

    // Too large object
    {
        _test_invalid_data_setup(dsk, buffer_area, tmp);
        heap_object_t *obj = (heap_object_t*)tmp.data();
        obj->size = dsk.meta_block_size+1;
        obj->crc32c = obj->calc_crc32c();

        blockstore_heap_t heap(&dsk, buffer_area.data());
        heap.load_blocks(0, dsk.meta_block_size*2, tmp.data());
        heap.finish_load();

        object_id oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0 };
        assert(!heap.read_entry(oid, NULL));

        oid = { .inode = INODE_WITH_POOL(1, 2), .stripe = 0 };
        assert(heap.read_entry(oid, NULL));
    }

    // No writes
    {
        _test_invalid_data_setup(dsk, buffer_area, tmp);
        heap_object_t *obj = (heap_object_t*)tmp.data();
        obj->write_pos = 0;
        obj->crc32c = obj->calc_crc32c();

        blockstore_heap_t heap(&dsk, buffer_area.data());
        heap.load_blocks(0, dsk.meta_block_size*2, tmp.data());
        heap.finish_load();

        object_id oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0 };
        assert(!heap.read_entry(oid, NULL));

        oid = { .inode = INODE_WITH_POOL(1, 2), .stripe = 0 };
        assert(heap.read_entry(oid, NULL));

        oid = { .inode = INODE_WITH_POOL(1, 3), .stripe = 0 };
        assert(heap.read_entry(oid, NULL));
    }

    // Bad crc32c
    {
        _test_invalid_data_setup(dsk, buffer_area, tmp);
        heap_object_t *obj = (heap_object_t*)tmp.data();
        obj->write_pos = sizeof(heap_object_t);
        obj->crc32c = obj->calc_crc32c()+1;

        blockstore_heap_t heap(&dsk, buffer_area.data());
        heap.load_blocks(0, dsk.meta_block_size*2, tmp.data());
        heap.finish_load();

        object_id oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0 };
        assert(!heap.read_entry(oid, NULL));

        oid = { .inode = INODE_WITH_POOL(1, 2), .stripe = 0 };
        assert(heap.read_entry(oid, NULL));

        oid = { .inode = INODE_WITH_POOL(1, 3), .stripe = 0 };
        assert(heap.read_entry(oid, NULL));
    }

    // Bad write size
    {
        _test_invalid_data_setup(dsk, buffer_area, tmp);
        heap_object_t *obj = (heap_object_t*)tmp.data();
        obj->get_writes()->size--;
        obj->crc32c = obj->calc_crc32c();

        blockstore_heap_t heap(&dsk, buffer_area.data());
        heap.load_blocks(0, dsk.meta_block_size*2, tmp.data());
        heap.finish_load();

        object_id oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0 };
        assert(!heap.read_entry(oid, NULL));

        oid = { .inode = INODE_WITH_POOL(1, 2), .stripe = 0 };
        assert(heap.read_entry(oid, NULL));
    }

    // Bad write positions:
    // 1) exceeds block back
    // 2) exceeds block forward
    // 3) intersects with object end
    // 4) intersects with object beginning
    for (int i = 0; i < 4; i++)
    {
        _test_invalid_data_setup(dsk, buffer_area, tmp);
        heap_object_t *obj = (heap_object_t*)(tmp.data() + sizeof(heap_object_t) + sizeof(heap_write_t));
        if (i == 0)
            obj->write_pos = -(int16_t)(sizeof(heap_object_t)+sizeof(heap_write_t)+1);
        else if (i == 1)
            obj->write_pos = dsk.meta_block_size-sizeof(heap_object_t)-2*sizeof(heap_write_t)+1;
        else if (i == 2)
            obj->write_pos = -1;
        else if (i == 3)
            obj->write_pos = 5;
        obj->crc32c = obj->calc_crc32c();

        blockstore_heap_t heap(&dsk, buffer_area.data());
        heap.load_blocks(0, dsk.meta_block_size*2, tmp.data());
        heap.finish_load();

        object_id oid = { .inode = INODE_WITH_POOL(1, 3), .stripe = 0 };
        assert(!heap.read_entry(oid, NULL));

        oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0 };
        assert(heap.read_entry(oid, NULL));

        oid = { .inode = INODE_WITH_POOL(1, 2), .stripe = 0 };
        assert(heap.read_entry(oid, NULL));
    }

    // Object write intersects with other writes
    {
        _test_invalid_data_setup(dsk, buffer_area, tmp);
        // Object2 Object1 BadLength Write2
        uint8_t *nb = tmp.data() + dsk.meta_block_size;
        memcpy(nb, tmp.data() + sizeof(heap_object_t) + sizeof(heap_write_t), sizeof(heap_object_t));
        nb += sizeof(heap_object_t);
        memcpy(nb, tmp.data(), sizeof(heap_object_t));
        nb += sizeof(heap_object_t);
        *((uint16_t*)nb) = sizeof(heap_write_t) + 4;
        nb += 2;
        memcpy(nb, tmp.data() + 2*sizeof(heap_object_t) + sizeof(heap_write_t), sizeof(heap_write_t));

        heap_object_t *obj = (heap_object_t*)(tmp.data() + dsk.meta_block_size);
        obj->write_pos = 2*sizeof(heap_object_t) + 2;
        obj->crc32c = obj->calc_crc32c();

        obj = (heap_object_t*)(tmp.data() + dsk.meta_block_size + sizeof(heap_object_t));
        obj->write_pos = sizeof(heap_object_t);
        obj->crc32c = obj->calc_crc32c();

        memset(tmp.data(), 0, dsk.meta_block_size);

        blockstore_heap_t heap(&dsk, buffer_area.data());
        heap.load_blocks(0, dsk.meta_block_size*2, tmp.data());
        heap.finish_load();

        object_id oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0 };
        assert(!heap.read_entry(oid, NULL));

        oid = { .inode = INODE_WITH_POOL(1, 3), .stripe = 0 };
        assert(heap.read_entry(oid, NULL));
    }

    // Write list entry exceeds block boundaries
    for (int i = 0; i < 2; i++)
    {
        _test_invalid_data_setup(dsk, buffer_area, tmp);
        heap_object_t *obj = (heap_object_t*)tmp.data();
        obj->get_writes()->next_pos = (i == 0 ? -sizeof(heap_object_t)-1 : dsk.meta_block_size - sizeof(heap_object_t) - sizeof(heap_write_t) + 1);
        obj->crc32c = obj->calc_crc32c();

        blockstore_heap_t heap(&dsk, buffer_area.data());
        heap.load_blocks(0, dsk.meta_block_size*2, tmp.data());
        heap.finish_load();

        object_id oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0 };
        assert(!heap.read_entry(oid, NULL));

        oid = { .inode = INODE_WITH_POOL(1, 3), .stripe = 0 };
        assert(heap.read_entry(oid, NULL));

        oid = { .inode = INODE_WITH_POOL(1, 2), .stripe = 0 };
        assert(heap.read_entry(oid, NULL));
    }

    printf("OK test_invalid_data\n");
}

void test_destructor_mvcc()
{
    blockstore_disk_t dsk;
    _test_init(dsk, false);
    std::vector<uint8_t> buffer_area(dsk.journal_device_size);

    {
        blockstore_heap_t heap(&dsk, buffer_area.data());
        heap.finish_load();

        // some writes
        _test_big_write(heap, dsk, 1, 0, 1, 0x20000);

        // read it to test mvcc
        object_id oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0 };
        uint64_t copy_id = 0;
        heap_object_t *obj = heap.lock_and_read_entry(oid, copy_id);
        assert(obj);

        _test_small_write(heap, dsk, 1, 0, 2, 8192, 4096, 16384, true);
    }

    printf("OK test_destructor_mvcc\n");
}

void test_rollback()
{
    int res;
    blockstore_disk_t dsk;
    _test_init(dsk, false);
    std::vector<uint8_t> buffer_area(dsk.journal_device_size);
    std::vector<uint8_t> tmp;

    {
        blockstore_heap_t heap(&dsk, buffer_area.data());
        heap.finish_load();

        // some writes
        _test_big_write(heap, dsk, 1, 0, 1, 0x20000);
        heap.mark_lsn_completed(1);
        _test_small_write(heap, dsk, 1, 0, 2, 8192, 4096, 16384, true);
        heap.mark_lsn_completed(2);

        // read it to test mvcc
        object_id oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0 };
        uint64_t copy_id = 0;
        heap_object_t *obj = heap.lock_and_read_entry(oid, copy_id);
        assert(obj);

        // already stable
        uint64_t new_lsn = 0;
        uint32_t mblock = 0;
        res = heap.post_rollback(oid, 2, &new_lsn, &mblock);
        assert(res == 0);
        res = heap.post_rollback(oid, 1, &new_lsn, NULL);
        assert(res == EBUSY);

        // unstable writes
        _test_big_write(heap, dsk, 1, 0, 3, 0x40000, false, 16384, 4096);
        heap.mark_lsn_completed(3);
        _test_small_write(heap, dsk, 1, 0, 4, 20480, 4096, 20480, false);
        heap.mark_lsn_completed(4);

        // second read
        uint64_t copy2_id = 0;
        obj = heap.lock_and_read_entry(oid, copy2_id);
        assert(obj);
        assert(copy2_id == copy_id);

        // rollback
        assert(heap.is_data_used(0x20000));
        assert(heap.is_data_used(0x40000));
        assert(!heap.is_buffer_area_free(16384, 4096));
        assert(!heap.is_buffer_area_free(20480, 4096));
        res = heap.post_rollback({ .inode = INODE_WITH_POOL(1, 1), .stripe = 0x20000 }, 2, NULL, NULL);
        assert(res == ENOENT);
        res = heap.post_rollback(oid, 5, NULL, NULL);
        assert(res == ENOENT);
        res = heap.post_rollback(oid, 2, &new_lsn, NULL);
        assert(res == 0);
        heap.mark_lsn_completed(new_lsn);
        assert(heap.is_data_used(0x20000));
        assert(heap.is_data_used(0x40000));
        assert(!heap.is_buffer_area_free(16384, 4096));
        assert(!heap.is_buffer_area_free(20480, 4096));

        // free mvcc
        heap.unlock_entry(oid, copy2_id);
        heap.unlock_entry(oid, copy_id);
        assert(heap.is_data_used(0x20000));
        assert(!heap.is_data_used(0x40000));
        assert(!heap.is_buffer_area_free(16384, 4096));
        assert(heap.is_buffer_area_free(20480, 4096));

        // check object data
        obj = heap.read_entry(oid, NULL);
        assert(obj);
        assert(count_writes(obj) == 2);
        heap_write_t *wr = obj->get_writes();
        assert(wr->version == 2);
        assert(wr->location == 16384);
        assert(wr->len == 4096);
        assert(wr->flags == BS_HEAP_SMALL_WRITE|BS_HEAP_STABLE);
        wr = wr->next();
        assert(wr->version == 1);
        assert(wr->location == 0x20000);
        assert(wr->flags == BS_HEAP_BIG_WRITE|BS_HEAP_STABLE);

        assert(heap.is_data_used(0x20000));
        assert(!heap.is_data_used(0x40000));
        assert(!heap.is_buffer_area_free(16384, 4096));
        assert(heap.is_buffer_area_free(20480, 4096));
    }

    {
        blockstore_heap_t heap(&dsk, buffer_area.data());
        heap.finish_load();

        object_id oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0x20000 };
        _test_big_write(heap, dsk, 1, 0x20000, 1, 0x20000, false);

        uint64_t new_lsn = 0;
        res = heap.post_rollback(oid, 0, &new_lsn, NULL);
        assert(res == 0);
        heap.mark_lsn_completed(new_lsn);

        assert(!heap.read_entry(oid, NULL));
        assert(!heap.is_data_used(0x20000));
    }

    printf("OK test_rollback\n");
}

void test_alloc_buffer()
{
    blockstore_disk_t dsk;
    _test_init(dsk, false);
    std::vector<uint8_t> buffer_area(dsk.journal_device_size);

    {
        multilist_alloc_t alloc(2048, 31);
        alloc.use(1998, 1);
        alloc.verify();
        alloc.use(70, 1);
        alloc.verify();
        alloc.use(206, 1);
        alloc.verify();
    }

    blockstore_heap_t heap(&dsk, buffer_area.data());
    heap.finish_load();

    uint64_t pos;

    for (int i = 0; i < 4096/64; i++)
    {
        pos = heap.find_free_buffer_area(64*1024);
        assert(pos == i*64*1024);
        heap.use_buffer_area(1, pos, 64*1024);
        assert(heap.get_buffer_area_used_space() == (i+1)*64*1024);
        assert(!heap.is_buffer_area_free(i*64*1024+4096, 4096));
        if (i < 4096/64-1)
            assert(heap.is_buffer_area_free((i+1)*64*1024, 64*1024));
    }

    pos = heap.find_free_buffer_area(4096);
    assert(pos == UINT64_MAX);

    for (int i = 0; i < 4096/64/2; i++)
    {
        heap.free_buffer_area(1, i*2*64*1024, 64*1024);
        assert(heap.get_buffer_area_used_space() == 4096*1024-(i+1)*64*1024);
    }

    for (int i = 0; i < 4096/64/2*16; i++)
    {
        pos = heap.find_free_buffer_area(4096);
        assert(pos != UINT64_MAX);
        heap.use_buffer_area(1, pos, 4096);
    }

    assert(heap.get_buffer_area_used_space() == 4096*1024);
    pos = heap.find_free_buffer_area(4096);
    assert(pos == UINT64_MAX);

    for (int i = 0; i < 4096/64/2*16; i++)
    {
        heap.free_buffer_area(1, (i/16)*2*64*1024+4096*(i%16), 4096);
    }

    pos = heap.find_free_buffer_area(64*1024);
    assert(pos != UINT64_MAX);

    printf("OK test_alloc_buffer\n");
}

void test_full_alloc()
{
    blockstore_disk_t dsk;
    std::map<std::string, std::string> config;
    config["data_csum_type"] = "crc32c";
    dsk.parse_config(config);
    dsk.data_device_size = 8*1024*1024;
    dsk.meta_device_size = 5*4096;
    dsk.journal_device_size = 4*1024*1024;
    dsk.data_fd = 0;
    dsk.meta_fd = 1;
    dsk.journal_fd = 2;
    dsk.calc_lengths();
    std::vector<uint8_t> buffer_area(dsk.journal_device_size);

    blockstore_heap_t heap(&dsk, buffer_area.data());
    heap.finish_load();
    assert(heap.get_meta_total_space() == 4*4096);

    uint32_t big_write_size = (sizeof(heap_object_t) + sizeof(heap_write_t) + 2*dsk.clean_entry_bitmap_size + dsk.data_block_size/dsk.csum_block_size*4);
    uint32_t small_write_size = (sizeof(heap_write_t) + dsk.clean_entry_bitmap_size + 4);
    assert(big_write_size == 197);
    assert(small_write_size == 45);
    uint32_t b_4s = (big_write_size + 4*small_write_size); // 377
    uint32_t epb = (4096-800+b_4s-1)/b_4s; // entries per block
    for (int j = 0; j < 4; j++)
    {
        assert(heap.get_meta_nearfull_blocks() == j);
        for (int i = j*epb; i < j*epb+epb; i++)
        {
            _test_big_write(heap, dsk, 1, i*0x20000, 1, i*0x20000);
            _test_small_write(heap, dsk, 1, i*0x20000, 2, 8192, 4096, i*16384, true);
            _test_small_write(heap, dsk, 1, i*0x20000, 3, 8192, 4096, i*16384+4096, true);
            _test_small_write(heap, dsk, 1, i*0x20000, 4, 8192, 4096, i*16384+2*4096, true);
            _test_small_write(heap, dsk, 1, i*0x20000, 5, 8192, 4096, i*16384+3*4096, true);
            assert(heap.get_meta_block_used_space(0) == (i < epb ? i+1 : epb)*b_4s);
            assert(heap.get_meta_block_used_space(1) == (i < epb ? 0 : (i < 2*epb ? i+1-epb : epb)*b_4s));
            assert(heap.get_meta_block_used_space(2) == (i < 2*epb ? 0 : (i < 3*epb ? i+1-2*epb : epb)*b_4s));
            assert(heap.get_meta_block_used_space(3) == (i < 3*epb ? 0 : (i < 4*epb ? i+1-3*epb : epb)*b_4s));
        }
    }

    // After filling all blocks to (4096-800), most free blocks should start to be allocated first
    for (int i = 0; i < 8; i++)
    {
        assert(heap.get_meta_nearfull_blocks() == 4);
        _test_big_write(heap, dsk, 1, (40+i)*0x20000, 1, (40+i)*0x20000);
        assert(heap.get_meta_block_used_space(i % 4) == (epb*b_4s + big_write_size*(i/4+1)));
    }

    // New writes are prevented if it may lead to inability to overwrite any object
    // - i.e. if the block doesn't have at least <max_overwrite_size> free space as the result
    assert(_test_do_big_write(heap, dsk, 1, 48*0x20000, 1, 48*0x20000) == ENOSPC);

    // Overwrites are, however, allowed until the block is almost empty
    for (int i = 0; i < 6; i++)
    {
        assert(_test_do_small_write(heap, dsk, 1, 0, 6+i, 0, 4096, epb*4*16384+i*4096) == 0);
    }
    assert(dsk.meta_block_size-heap.get_meta_block_used_space(0) < big_write_size);
    assert(_test_do_small_write(heap, dsk, 1, 0, 12, 0, 4096, 48*16384+8*4096) == ENOSPC);

    // Check that used_alloc_queue doesn't return used blocks
    {
        // object from block 2
        uint64_t new_lsn = 0;
        object_id oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 11*0x20000 };
        int res = heap.post_delete(oid, &new_lsn, NULL);
        assert(res == 0);
        heap.mark_lsn_completed(new_lsn);

        uint32_t block_num = 0;
        assert(!heap.read_entry(oid, &block_num));

        _test_big_write(heap, dsk, 1, 11*0x20000, 6, 48*0x20000);
        assert(heap.read_entry(oid, &block_num));
        assert(block_num == 1);
    }

    printf("OK test_full_alloc\n");
}

void test_duplicate()
{
    blockstore_disk_t dsk;
    _test_init(dsk, false);
    std::vector<uint8_t> buffer_area(dsk.journal_device_size);
    std::vector<uint8_t> tmp;

    tmp.resize(dsk.meta_block_size*2);

    // write
    {
        blockstore_heap_t heap(&dsk, buffer_area.data());
        heap.finish_load();

        // big_write
        _test_big_write(heap, dsk, 1, 0, 1, 0x20000);

        // persist
        assert(heap.get_meta_block_used_space(0) > 0);
        memcpy(tmp.data(), heap.get_meta_block(0), dsk.meta_block_size);

        // update object
        _test_big_write(heap, dsk, 1, 0, 2, 0x40000);

        // persist again to block 2
        assert(heap.get_meta_block_used_space(0) > 0);
        memcpy(tmp.data()+dsk.meta_block_size, heap.get_meta_block(0), dsk.meta_block_size);
    }

    // reload heap with duplicate
    {
        blockstore_heap_t heap(&dsk, buffer_area.data());
        heap.set_fail_on_warn(true);
        heap.load_blocks(0, 2*dsk.meta_block_size, tmp.data());
        heap.finish_load();

        // read object - version 2 should be present
        object_id oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0 };
        heap_object_t *obj = heap.read_entry(oid, NULL);
        assert(obj);
        assert(count_writes(obj) == 1);
        heap_write_t *wr = obj->get_writes();
        assert(wr->version == 2);
        assert(wr->location == 0x40000);
        assert(wr->flags == BS_HEAP_BIG_WRITE|BS_HEAP_STABLE);

        assert(heap.get_meta_block_used_space(0) == 0);
        assert(heap.get_meta_block_used_space(1) == obj->size+wr->size);
        assert(heap.is_data_used(0x40000));
        assert(!heap.is_data_used(0x20000));
    }

    // reload heap with duplicate in different order
    {
        blockstore_heap_t heap(&dsk, buffer_area.data());
        heap.set_fail_on_warn(true);
        heap.load_blocks(dsk.meta_block_size, dsk.meta_block_size, tmp.data()+dsk.meta_block_size);
        heap.load_blocks(0, dsk.meta_block_size, tmp.data());
        heap.finish_load();

        // read object - version 2 should be present
        object_id oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0 };
        heap_object_t *obj = heap.read_entry(oid, NULL);
        assert(obj);
        assert(count_writes(obj) == 1);
        heap_write_t *wr = obj->get_writes();
        assert(wr->version == 2);
        assert(wr->location == 0x40000);
        assert(wr->flags == BS_HEAP_BIG_WRITE|BS_HEAP_STABLE);

        assert(heap.get_meta_block_used_space(0) == 0);
        assert(heap.get_meta_block_used_space(1) == obj->size+wr->size);
        assert(heap.is_data_used(0x40000));
        assert(!heap.is_data_used(0x20000));
    }

    printf("OK test_duplicate\n");
}

void test_autocompact(bool csum)
{
    blockstore_disk_t dsk;
    _test_init(dsk, csum);
    std::vector<uint8_t> buffer_area(dsk.journal_device_size);
    std::vector<uint8_t> tmp;

    tmp.resize(dsk.meta_block_size);

    uint32_t big_write_size = 0, small_write_size = 0;

    // write
    {
        blockstore_heap_t heap(&dsk, buffer_area.data());
        heap.finish_load();

        // some writes
        uint32_t buffer_csum = crc32c(0, buffer_area.data()+4*4096, 4096);
        _test_big_write(heap, dsk, 1, 0, 1, 0x20000);
        _test_small_write(heap, dsk, 1, 0, 2, 4096, 4096, 4*4096, true, &buffer_csum);
        _test_small_write(heap, dsk, 1, 0, 3, 3*4096, 4096, 5*4096, true, &buffer_csum);
        _test_small_write(heap, dsk, 1, 0, 4, 5*4096, 4096, 6*4096, true, &buffer_csum);
        _test_small_write(heap, dsk, 1, 0, 5, 7*4096, 4096, 7*4096, true, &buffer_csum);

        _test_big_write(heap, dsk, 1, 0x40000, 1, 0x60000);

        // check lsn
        object_id oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0 };
        heap_object_t *obj = heap.read_entry(oid, NULL);
        assert(obj);
        assert(count_writes(obj) == 5);
        assert(obj->get_writes()->lsn == 5);

        big_write_size = obj->get_writes()->next()->next()->next()->next()->get_size(&heap);
        small_write_size = obj->get_writes()->get_size(&heap);

        // persist
        assert(heap.get_meta_block_used_space(0) == 2*sizeof(heap_object_t) + 2*big_write_size + 4*small_write_size);
        memcpy(tmp.data(), heap.get_meta_block(0), dsk.meta_block_size);
    }

    // reload heap with autocompaction
    {
        blockstore_heap_t heap(&dsk, buffer_area.data());
        heap.set_fail_on_warn(true);
        heap.set_compacted_lsn(3);
        assert(heap.get_compacted_lsn() == 3);
        heap.load_blocks(0, dsk.meta_block_size, tmp.data());
        heap.finish_load();

        // read object - all entries should be present first...
        object_id oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0 };
        heap_object_t *obj = heap.read_entry(oid, NULL);
        assert(obj);
        assert(count_writes(obj) == 3);
        heap_write_t *wr = obj->get_writes();
        assert(wr->lsn == 5);
        assert(wr->version == 5);
        assert(wr->offset == 7*4096);
        assert(wr->len == 4096);
        assert(wr->location == 7*4096);
        assert(wr->flags == BS_HEAP_SMALL_WRITE|BS_HEAP_STABLE);
        wr = wr->next();
        assert(wr->lsn == 4);
        assert(wr->version == 4);
        assert(wr->offset == 5*4096);
        assert(wr->len == 4096);
        assert(wr->location == 6*4096);
        assert(wr->flags == BS_HEAP_SMALL_WRITE|BS_HEAP_STABLE);
        wr = wr->next();
        assert(wr->lsn == 3);
        assert(wr->version == 3);
        assert(wr->offset == 0);
        assert(wr->len == dsk.data_block_size);
        assert(wr->location == 0x20000);
        assert(wr->flags == BS_HEAP_BIG_WRITE|BS_HEAP_STABLE);

        // check that blocks are auto-freed
        assert(heap.is_data_used(0x20000));
        assert(heap.is_buffer_area_free(4*4096, 4096));
        assert(heap.is_buffer_area_free(5*4096, 4096));
        assert(!heap.is_buffer_area_free(6*4096, 4096));
        assert(!heap.is_buffer_area_free(7*4096, 4096));

        assert(heap.get_meta_block_used_space(0) == 2*sizeof(heap_object_t) + 2*big_write_size + 2*small_write_size);
    }

    printf("OK test_autocompact %s\n", csum ? "csum" : "no_csum");
}

void test_intent_write(bool csum)
{
    blockstore_disk_t dsk;
    _test_init(dsk, csum);
    std::vector<uint8_t> buffer_area(dsk.journal_device_size);
    memset(buffer_area.data(), 0xab, 4096);

    {
        blockstore_heap_t heap(&dsk, buffer_area.data());
        heap.finish_load();

        _test_big_write(heap, dsk, 1, 0, 1, 0x20000, true, 0, 4096);
        heap.mark_lsn_completed(1);

        _test_small_write(heap, dsk, 1, 0, 2, 8192, 4096, 0, true, NULL, true);
        heap.mark_lsn_completed(2);

        _test_small_write(heap, dsk, 1, 0, 3, 16384, 4096, 0, true, NULL, true);
        heap.mark_lsn_completed(3);

        object_id oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0 };
        heap_object_t *obj = heap.read_entry(oid, NULL);
        assert(obj);
        assert(count_writes(obj) == 2); // intent overwrites previous intent
        assert(obj->get_writes()->lsn == 3);

        uint8_t ref_int_bitmap[dsk.clean_entry_bitmap_size];
        memset(ref_int_bitmap, 0, dsk.clean_entry_bitmap_size);
        bitmap_set(ref_int_bitmap, 0, 4096, 4096);
        bitmap_set(ref_int_bitmap, 8192, 4096, 4096);
        assert(!memcmp(obj->get_writes()->next()->get_int_bitmap(&heap), ref_int_bitmap, dsk.clean_entry_bitmap_size));
        if (csum)
        {
            uint8_t ref_csums[dsk.data_block_size/dsk.csum_block_size*4];
            memset(ref_csums, 0xde, sizeof(ref_csums));
            memset(ref_csums+8, 0xab, 4);
            assert(!memcmp(obj->get_writes()->next()->get_checksums(&heap), ref_csums, sizeof(ref_csums)));
        }

        assert(check_used_space(heap, dsk, 0));
    }

    printf("OK test_intent_write %s\n", csum ? "csum" : "no_csum");
}

int main(int narg, char *args[])
{
    test_mvcc(true);
    test_mvcc(false);
    test_update(true);
    test_update(false);
    test_delete(true);
    test_delete(false);
    test_compact_block();
    test_compact(true, true);
    test_compact(true, false);
    test_compact(false, true);
    test_compact(false, false);
    test_modify_bitmap();
    test_recheck(false, true, false);
    test_recheck(false, false, false);
    test_recheck(true, true, false);
    test_recheck(true, false, false);
    test_recheck(false, true, true);
    test_recheck(false, false, true);
    test_recheck(true, true, true);
    test_recheck(true, false, true);
    test_corruption();
    test_full_overwrite(true);
    test_full_overwrite(false);
    test_reshard_list();
    test_invalid_data();
    test_destructor_mvcc();
    test_rollback();
    test_alloc_buffer();
    test_full_alloc();
    test_duplicate();
    test_autocompact(true);
    test_autocompact(false);
    test_intent_write(true);
    test_intent_write(false);
    return 0;
}
