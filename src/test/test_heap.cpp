// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "../util/malloc_or_die.h"
#include "../util/allocator.h"
#include "blockstore_heap.h"
#include "../util/crc32c.h"

static int count_writes(blockstore_heap_t & heap, heap_entry_t *obj)
{
    int n = 0;
    for (auto wr = obj; wr; wr = heap.prev(wr))
    {
        n++;
    }
    return n;
}

#define FREE_SPACE_BIT 0x8000
#define GARBAGE_BIT ((uint64_t)1 << 63)

bool check_used_space(blockstore_heap_t & heap, blockstore_disk_t & dsk, uint32_t block_num)
{
    uint8_t *buf = (uint8_t*)malloc_or_die(dsk.meta_block_size);
    heap.get_meta_block(block_num, buf);
    uint8_t *data = buf;
    uint8_t *end = data+dsk.meta_block_size;
    uint32_t used = 0;
    while (data < end)
    {
        heap_entry_t *wr = ((heap_entry_t*)data);
        if (!(wr->size & FREE_SPACE_BIT) && !wr->is_garbage())
        {
            used += wr->size;
        }
        if (!wr->size)
        {
            break;
        }
        data += (wr->size & ~FREE_SPACE_BIT);
    }
    free(buf);
    return used == heap.get_meta_block_used_space(block_num);
}

int _test_do_big_write(blockstore_heap_t & heap, blockstore_disk_t & dsk, uint64_t inode, uint64_t stripe, uint64_t version, uint64_t location,
    bool stable, uint32_t offset, uint32_t len, uint8_t *data, uint32_t *mblock = NULL)
{
    if (!offset && !len)
        len = dsk.data_block_size;
    object_id oid = { .inode = INODE_WITH_POOL(1, inode), .stripe = stripe };
    heap_entry_t *obj = heap.read_entry(oid);
    uint8_t ext_bitmap[dsk.clean_entry_bitmap_size];
    memset(ext_bitmap, 0xff, dsk.clean_entry_bitmap_size);
    return heap.add_big_write(oid, obj, stable, version, offset, len, location, ext_bitmap, data, mblock);
}

void _test_big_write(blockstore_heap_t & heap, blockstore_disk_t & dsk, uint64_t inode, uint64_t stripe, uint64_t version, uint64_t location,
    bool stable, uint32_t offset, uint32_t len, uint8_t *data, uint32_t expected_mblock = 0)
{
    heap.use_data(INODE_WITH_POOL(1, inode), location); // blocks are allocated before write and outside the heap_t
    uint32_t mblock = 999999;
    int res = _test_do_big_write(heap, dsk, inode, stripe, version, location, stable, offset, len, data, &mblock);
    assert(res == 0);
    assert(heap.is_data_used(location));
    assert(mblock == expected_mblock || expected_mblock == UINT32_MAX);
    heap.start_block_write(mblock);
    heap.complete_block_write(mblock);
}

int _test_do_small_write(blockstore_heap_t & heap, blockstore_disk_t & dsk, uint64_t inode, uint64_t stripe, uint64_t version,
    uint32_t offset, uint32_t len, uint64_t location, bool stable, uint8_t *data, bool is_intent = false, uint32_t *mblock = NULL)
{
    object_id oid = { .inode = INODE_WITH_POOL(1, inode), .stripe = stripe };
    heap_entry_t *obj = heap.read_entry(oid);
    uint16_t type = (is_intent ? BS_HEAP_INTENT_WRITE : BS_HEAP_SMALL_WRITE) | (stable ? BS_HEAP_STABLE : 0);
    uint8_t ext_bitmap[dsk.clean_entry_bitmap_size];
    memset(ext_bitmap, 0xff, dsk.clean_entry_bitmap_size);
    return heap.add_small_write(oid, obj, type, version, offset, len, location, ext_bitmap, data, mblock);
}

void _test_small_write(blockstore_heap_t & heap, blockstore_disk_t & dsk, uint64_t inode, uint64_t stripe, uint64_t version,
    uint32_t offset, uint32_t len, uint64_t location, bool stable, uint8_t *data, bool is_intent = false,
    uint32_t expected_mblock = 0)
{
    if (!is_intent)
        heap.use_buffer_area(INODE_WITH_POOL(1, inode), location, len); // blocks are allocated before write and outside the heap_t
    uint32_t mblock = 999999;
    int res = _test_do_small_write(heap, dsk, inode, stripe, version, offset, len, location, stable, data, is_intent, &mblock);
    assert(res == 0);
    if (!is_intent)
        assert(!heap.is_buffer_area_free(location, len));
    assert(mblock == expected_mblock || expected_mblock == UINT32_MAX);
    heap.start_block_write(mblock);
    heap.complete_block_write(mblock);
}

void _test_init(blockstore_disk_t & dsk, bool csum, std::function<void(std::map<std::string, std::string> &)> cfg_cb = NULL)
{
    std::map<std::string, std::string> config;
    if (csum)
        config["data_csum_type"] = "crc32c";
    if (cfg_cb)
        cfg_cb(config);
    dsk.parse_config(config);
    dsk.data_device = "data";
    dsk.meta_device = "meta";
    dsk.journal_device = "journal";
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
        assert(_test_do_small_write(heap, dsk, 1, 0, 1, 0, 4096, 0, true, buffer_area.data()) == EINVAL);

        assert(heap.find_free_data() == 0);

        _test_big_write(heap, dsk, 1, 0, 1, 0, true, 0, 0, buffer_area.data());
        assert(heap.get_meta_block_used_space(0) == heap.get_big_entry_size());
        assert(check_used_space(heap, dsk, 0));
        assert(heap.get_meta_used_space() == heap.get_meta_block_used_space(0));

        assert(heap.find_free_data() == 0x20000);

        object_id oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0 };
        heap_entry_t *obj = heap.lock_and_read_entry(oid);
        assert(obj);
        assert(count_writes(heap, obj) == 1);
        assert(obj->lsn == 1);
        assert(obj->entry_type == BS_HEAP_BIG_WRITE|BS_HEAP_STABLE);
        assert(obj->version == 1);
        assert(obj->big_location(&heap) == 0);
        uint64_t old_size = obj->size;

        _test_small_write(heap, dsk, 1, 0, 2, 8192, 4096, 16384, true, buffer_area.data()+16384, false);
        obj = heap.read_entry(oid);
        assert(count_writes(heap, obj) == 2);
        assert(check_used_space(heap, dsk, 0));
        assert(heap.get_meta_block_used_space(0) == old_size + obj->size);

        assert(_test_do_small_write(heap, dsk, 1, 0, 1, 0, 4096, 0, true, buffer_area.data()) == EINVAL);

        _test_big_write(heap, dsk, 1, 0, 3, 128*1024, true, 0, 0, buffer_area.data());
        obj = heap.read_entry(oid);
        assert(count_writes(heap, obj) == 3);
        assert(obj->lsn == 3);
        assert(obj->version == 3);
        assert(obj->entry_type == BS_HEAP_BIG_WRITE|BS_HEAP_STABLE);

        assert(count_writes(heap, heap.read_entry(oid)) == 3); // MVCC prevents GC of old entries

        assert(heap.unlock_entry(oid));
        assert(count_writes(heap, heap.read_entry(oid)) == 3); // Now we unlock it and old entries are GCed, but left in the list
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
        _test_big_write(heap, dsk, 1, 0, 1, 0, true, 0, 0, buffer_area.data());

        _test_small_write(heap, dsk, 1, 0, 2, 8192, 4096, 16384, true, buffer_area.data()+16384, false);

        object_id oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0 };
        assert(count_writes(heap, heap.read_entry(oid)) == 2);
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
        // Add 1:0 and 1:20000

        _test_big_write(heap, dsk, 1, 0, 1, 0x20000, true, 0, 0, buffer_area.data());

        _test_big_write(heap, dsk, 1, 0x20000, 1, 0x40000, true, 0, 0, buffer_area.data());

        auto & space = heap.get_inode_space_stats();
        assert(space.at(INODE_WITH_POOL(1, 1)) == 0x40000);
        assert(heap.get_data_used_space() == 0x40000);

        // Delete 1:0

        uint32_t mblock = 999999;
        object_id oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0 };
        auto obj = heap.read_entry(oid);
        assert(obj);
        int res = heap.add_delete(obj, &mblock);
        assert(mblock == 0);
        assert(res == 0);

        heap.start_block_write(mblock);
        assert(space.at(INODE_WITH_POOL(1, 1)) == 0x40000);
        assert(heap.get_data_used_space() == 0x40000);
        heap.complete_block_write(mblock);

        obj = heap.read_entry(oid);
        assert(obj);
        assert(count_writes(heap, obj) == 2);
        assert(obj->entry_type == (BS_HEAP_DELETE|BS_HEAP_STABLE));

        assert(space.at(INODE_WITH_POOL(1, 1)) == 0x20000);
        assert(heap.get_data_used_space() == 0x20000);

        // Write version 1 over delete again
        _test_big_write(heap, dsk, 1, 0, 1, 0x20000, true, 0, 0, buffer_area.data());

        obj = heap.read_entry(oid);
        assert(obj);
        assert(count_writes(heap, obj) == 2);
        assert(obj->entry_type == (BS_HEAP_BIG_WRITE|BS_HEAP_STABLE));

        // Delete it again...
        res = heap.add_delete(obj, &mblock);
        assert(mblock == 0);
        assert(res == 0);
        heap.start_block_write(mblock);
        heap.complete_block_write(mblock);

        obj = heap.read_entry(oid);
        assert(obj);

        // Now the trickiest part - check that the delete entry itself disappears
        // when all previous entries disappear from the disk too. It happens only
        // during block defragmentation so we fill the block 0 to 100%
        assert(heap.get_meta_block_used_space(0) == heap.get_big_entry_size() + heap.get_simple_entry_size());
        int i = 0;
        while (dsk.meta_block_size-heap.get_meta_block_used_space(0) >= heap.get_big_entry_size())
        {
            _test_big_write(heap, dsk, 1, 0x40000+0x20000*i, 1, 0x60000+0x20000*i, true, 0, 0, buffer_area.data());
            i++;
        }

        obj = heap.read_entry(oid);
        assert(!obj);
    }

    printf("OK test_delete %s\n", csum ? "csum" : "no_csum");
}

void test_defrag_block()
{
    blockstore_disk_t dsk;
    _test_init(dsk, true);
    std::vector<uint8_t> buffer_area(dsk.journal_device_size);
    dsk.meta_area_size = 4096*3;
    blockstore_heap_t heap(&dsk, buffer_area.data());
    heap.finish_load();

    uint32_t big_write_size = heap.get_big_entry_size();
    uint32_t small_write_size = heap.get_small_entry_size(0, 4096);
    assert(big_write_size == 180);
    assert(small_write_size == 64);
    uint32_t nwr = 0;
    bool add = false;
    if ((dsk.meta_block_size % (big_write_size+small_write_size)) >= big_write_size)
    {
        nwr = (dsk.meta_block_size / (big_write_size+small_write_size)) +
            (dsk.meta_block_size-small_write_size) / (big_write_size+small_write_size);
        add = (dsk.meta_block_size - small_write_size -
            (dsk.meta_block_size-small_write_size) % (big_write_size+small_write_size)) >= big_write_size;
    }
    else
    {
        nwr = dsk.meta_block_size/(big_write_size+small_write_size)*2-1;
    }

    {
        uint32_t used = 0;
        uint32_t expected_block = 0;
        for (uint32_t i = 0; i < nwr; i++)
        {
            _test_big_write(heap, dsk, 1, i*0x20000, 1, i*0x20000, true, 0, 0, buffer_area.data(), expected_block);
            used += big_write_size;
            if (dsk.meta_block_size-used < small_write_size)
            {
                used = 0;
                expected_block++;
            }
            _test_small_write(heap, dsk, 1, i*0x20000, 2, 0, 4096, i*4096, true, buffer_area.data()+i*4096, false, expected_block);
            used += small_write_size;
            if (dsk.meta_block_size-used < big_write_size)
            {
                used = 0;
                expected_block++;
            }
        }
        if (add)
        {
            _test_big_write(heap, dsk, 1, nwr*0x20000, 1, nwr*0x20000, true, 0, 0, buffer_area.data(), 1);
            used += big_write_size;
        }
        // The next write should be rejected because allowing it would block compaction
        assert(_test_do_big_write(heap, dsk, 1, (nwr+1)*0x20000, 1, (nwr+1)*0x20000, true, 0, 0, buffer_area.data()) == ENOSPC);
        // Compact all small writes
        uint8_t bitmap[dsk.clean_entry_bitmap_size];
        memset(bitmap, 0xFF, dsk.clean_entry_bitmap_size);
        uint32_t mblock = 999999;
        for (uint32_t i = 0; i < nwr; i++)
        {
            auto obj = heap.read_entry((object_id){ .inode = INODE_WITH_POOL(1, 1), .stripe = i*0x20000 });
            assert(obj);
            assert(heap.prev(obj)->entry_type == (BS_HEAP_BIG_WRITE|BS_HEAP_STABLE));
            int res = heap.add_compact(obj, obj->version, obj->lsn, heap.prev(obj)->big_location(&heap),
                false, &mblock, bitmap, bitmap, NULL);
            assert(res == 0);
            heap.start_block_write(mblock);
            heap.complete_block_write(mblock);
        }
    }

    printf("OK test_defrag_block\n");
}

void test_compact(bool csum, bool stable)
{
    int res;
    blockstore_disk_t dsk;
    _test_init(dsk, csum);
    std::vector<uint8_t> buffer_area(dsk.journal_device_size);

    blockstore_heap_t heap(&dsk, buffer_area.data());
    heap.finish_load();

    memset(buffer_area.data(), 0x19, 4096);
    _test_big_write(heap, dsk, 1, 0, 1, 0x20000, true, 0, 4096, buffer_area.data());

    // write unstable - stabilize - compact
    object_id oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0 };
    heap_entry_t *obj = heap.read_entry(oid);
    assert(obj);
    assert(count_writes(heap, obj) == 1);
    assert(obj->entry_type == BS_HEAP_BIG_WRITE|BS_HEAP_STABLE);
    uint8_t ref_int_bitmap[dsk.clean_entry_bitmap_size];
    memset(ref_int_bitmap, 0, dsk.clean_entry_bitmap_size);
    bitmap_set(ref_int_bitmap, 0, 4096, 4096);
    assert(!memcmp(obj->get_int_bitmap(&heap), ref_int_bitmap, dsk.clean_entry_bitmap_size));
    uint64_t old_size = obj->size;

    memset(buffer_area.data()+8192, 0xAA, 4096);
    _test_small_write(heap, dsk, 1, 0, 3, 8192, 4096, 16384, stable, buffer_area.data()+8192, false);
    obj = heap.read_entry(oid);
    old_size += obj->get_size(&heap);
    assert(obj->lsn == 2);
    assert(check_used_space(heap, dsk, 0));
    assert(heap.get_meta_block_used_space(0) == old_size);

    object_id oid2 = { .inode = INODE_WITH_POOL(1, 2), .stripe = 0 };
    _test_big_write(heap, dsk, 2, 0, 1, 0x40000, true, 0, 4096, buffer_area.data());

    uint32_t mblock = 999999;
    object_id compact_oid = {};
    if (!stable)
    {
        res = heap.get_next_compact(compact_oid);
        assert(res == ENOENT);

        auto obj2 = heap.read_entry(oid2);
        res = heap.add_commit(obj2, 3, NULL);
        assert(res == ENOENT);
        res = heap.add_commit(obj2, 5, NULL);
        assert(res == ENOENT);
        auto obj = heap.read_entry(oid);
        res = heap.add_commit(obj, 1, &mblock);
        assert(res == EBUSY); // already stable
        res = heap.add_commit(obj, 3, &mblock);
        assert(res == 0);
        assert(mblock == 0);
        assert(check_used_space(heap, dsk, 0));
        assert(heap.get_meta_block_used_space(0) == old_size + heap.get_big_entry_size() + heap.get_simple_entry_size());
        heap.start_block_write(mblock);
        heap.complete_block_write(mblock);
    }

    assert(heap.get_to_compact_count() == 1);
    res = heap.get_next_compact(compact_oid);
    assert(res == 0);
    assert(oid == compact_oid);

    obj = heap.read_entry(oid);
    assert(obj);
    assert(count_writes(heap, obj) == (stable ? 2 : 3));
    int small_writes = 0;
    heap_entry_t *small_wr = NULL;
    // FIXME: Check more iterate_compaction cases, also check more compact_object cases
    auto compact_info = heap.iterate_compaction(obj, heap.get_fsynced_lsn(), false, [&](heap_entry_t *wr)
    {
        small_wr = wr;
        small_writes++;
    });
    assert(compact_info.compact_lsn == (stable ? 2 : 4));
    assert(compact_info.compact_version == 3);
    assert(compact_info.clean_wr->lsn == 1);
    assert(small_writes == 1);
    assert(small_wr->lsn == 2);

    bitmap_set(ref_int_bitmap, 8192, 4096, 4096);
    {
        uint32_t csums[dsk.data_block_size/(dsk.csum_block_size ? dsk.csum_block_size : 4096)] = {};
        csums[0] = crc32c(0, buffer_area.data(), 4096);
        csums[2] = crc32c(0, buffer_area.data()+8192, 4096);
        res = heap.add_compact(obj, compact_info.compact_version, compact_info.compact_lsn,
            compact_info.clean_wr->big_location(&heap), compact_info.do_delete,
            &mblock, ref_int_bitmap, ref_int_bitmap, (uint8_t*)csums);
        assert(res == 0);
    }
    assert(mblock == 0);
    heap.start_block_write(mblock);
    heap.complete_block_write(mblock);

    assert(heap.get_to_compact_count() == 0);

    assert(check_used_space(heap, dsk, 0));
    assert(heap.get_meta_block_used_space(0) == 2*heap.get_big_entry_size());

    obj = heap.read_entry(oid);
    assert(obj);
    assert(count_writes(heap, obj) == (stable ? 3 : 4));
    assert(obj->version == 3);
    assert(!memcmp(obj->get_int_bitmap(&heap), ref_int_bitmap, dsk.clean_entry_bitmap_size));
    if (csum)
    {
        assert(heap.calc_checksums(obj, buffer_area.data(), false));
        uint32_t csums[dsk.data_block_size/(dsk.csum_block_size ? dsk.csum_block_size : 4096)] = {};
        csums[0] = crc32c(0, buffer_area.data(), 4096);
        csums[2] = crc32c(0, buffer_area.data()+8192, 4096);
        assert(!memcmp(obj->get_checksums(&heap), csums, dsk.data_block_size/dsk.csum_block_size*4));
    }

    obj = heap.read_entry({ .inode = INODE_WITH_POOL(1, 2), .stripe = 0 });
    assert(obj);
    assert(count_writes(heap, obj) == 1);
    assert(obj->version == 1);

    printf("OK test_compact %s %s\n", stable ? "stable" : "unstable", csum ? "csum" : "no_csum");
}

void test_modify_bitmap()
{
    blockstore_disk_t dsk;
    _test_init(dsk, true, [&](std::map<std::string, std::string> & config) { config["csum_block_size"] = "32k"; });
    std::vector<uint8_t> buffer_area(dsk.journal_device_size);

    blockstore_heap_t heap(&dsk, buffer_area.data());
    heap.finish_load();

    memset(buffer_area.data(), 0x19, 8192);
    _test_big_write(heap, dsk, 1, 0, 1, 0x20000, true, 0, 8192, buffer_area.data());

    memset(buffer_area.data()+8192, 0xAA, 4096);
    _test_small_write(heap, dsk, 1, 0, 2, 8192, 4096, 8192, true, buffer_area.data()+8192, false);

    object_id oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0 };
    heap_entry_t *obj = heap.read_entry(oid);
    assert(obj);
    assert(count_writes(heap, obj) == 2);

    uint8_t new_bmp[dsk.clean_entry_bitmap_size];
    memcpy(new_bmp, heap.prev(obj)->get_int_bitmap(&heap), dsk.clean_entry_bitmap_size);
    bitmap_clear(new_bmp, 4096, 32768-4096, dsk.bitmap_granularity);
    uint8_t new_csums[dsk.data_block_size/32768*4];
    memset(new_csums, 0, dsk.data_block_size/32768*4);
    new_csums[0] = crc32c(0, buffer_area.data(), 4096);

    uint32_t mblock = 999999;
    int res = heap.punch_holes(heap.prev(obj), new_bmp, new_csums, &mblock);
    assert(res == 0);
    assert(mblock == 0);
    heap.start_block_write(mblock);
    heap.complete_block_write(mblock);

    obj = heap.read_entry(oid);
    assert(obj);
    assert(count_writes(heap, obj) == 2);
    assert(memcmp(heap.prev(obj)->get_int_bitmap(&heap), new_bmp, dsk.clean_entry_bitmap_size) == 0);
    assert(memcmp(heap.prev(obj)->get_checksums(&heap), new_csums, dsk.data_block_size/32768*4) == 0);

    printf("OK test_modify_bitmap\n");
}

void test_recheck(bool async, bool csum, bool intent)
{
    blockstore_disk_t dsk;
    _test_init(dsk, csum);
    std::vector<uint8_t> buffer_area(dsk.journal_device_size);
    std::vector<uint8_t> tmp;

    memset(buffer_area.data(), 0xab, 12288);

    // write
    {
        blockstore_heap_t heap(&dsk, buffer_area.data());
        heap.finish_load();

        // object 1
        _test_big_write(heap, dsk, 1, 0, 1, 0x20000, true, 0, 8192, buffer_area.data());
        _test_small_write(heap, dsk, 1, 0, 2, 8*1024, 8*1024, 16*1024, true, buffer_area.data(), intent);

        // object 2
        _test_big_write(heap, dsk, 2, 0, 1, 0x40000, true, 0, 8192, buffer_area.data());
        if (intent)
            _test_small_write(heap, dsk, 2, 0, 2, 20*1024, 4*1024, 36*1024, true, buffer_area.data(), intent);
        _test_small_write(heap, dsk, 2, 0, intent ? 3 : 2, 8*1024, 12*1024, 24*1024, true, buffer_area.data(), intent);

        // persist
        assert(heap.get_meta_block_used_space(0) > 0);
        tmp.resize(dsk.meta_block_size);
        heap.get_meta_block(0, tmp.data());
    }

    // reload heap
    {
        memset(buffer_area.data()+16*1024, 0xab, 20*1024); // valid data
        memset(buffer_area.data()+20*1024+64, 0xcc, 4); // invalid data in the second block of the first write

        blockstore_heap_t heap(&dsk, async ? NULL : buffer_area.data(), 10);
        uint64_t entries_loaded;
        heap.load_blocks(0, dsk.meta_block_size, tmp.data(), false, entries_loaded);

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
        heap_entry_t *obj = heap.read_entry(oid);
        assert(obj);
        assert(count_writes(heap, obj) == 1);
        assert(obj->lsn == 1);
        assert(obj->entry_type == BS_HEAP_BIG_WRITE|BS_HEAP_STABLE);
        assert(obj->version == 1);
        assert(obj->big_location(&heap) == 0x20000);

        // read object 2 - both writes should be present
        oid = { .inode = INODE_WITH_POOL(1, 2), .stripe = 0 };
        obj = heap.read_entry(oid);
        assert(obj);
        assert(count_writes(heap, obj) == (intent ? 3 : 2));
        assert(obj->lsn == (intent ? 5 : 4));
        assert(obj->entry_type == BS_HEAP_SMALL_WRITE|BS_HEAP_STABLE);
        assert(obj->version == (intent ? 3 : 2));
        assert(obj->small().offset == 8192);
        assert(obj->small().len == 12*1024);
        assert(obj->small().location == 24*1024);
    }

    printf("OK test_recheck %s %s %s\n", async ? "async" : "sync", csum ? "csum" : "no_csum", intent ? "intent" : "buffered");
}

void test_corruption()
{
    blockstore_disk_t dsk;
    _test_init(dsk, false);
    std::vector<uint8_t> buffer_area(dsk.journal_device_size);
    std::vector<uint8_t> tmp;

    // write
    {
        blockstore_heap_t heap(&dsk, buffer_area.data());
        heap.finish_load();

        // big_write
        _test_big_write(heap, dsk, 1, 0, 1, 0x20000, true, 0, 0, buffer_area.data());

        object_id oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0 };
        heap_entry_t *obj = heap.read_entry(oid);
        assert(obj);

        // big_write object 2
        _test_big_write(heap, dsk, 1, 0x20000, 1, 0x40000, true, 0, 0, buffer_area.data());

        // big_write object 3
        _test_big_write(heap, dsk, 1, 0x40000, 1, 0x60000, true, 0, 0, buffer_area.data());

        // persist
        assert(heap.get_meta_block_used_space(0) > 0);
        assert(heap.get_meta_block_used_space(1) == 0);
        tmp.resize(dsk.meta_block_size);
        heap.get_meta_block(0, tmp.data());
    }

    // reload heap with corruption
    {
        blockstore_heap_t heap(&dsk, buffer_area.data());
        tmp.data()[10]++; // corrupt the first object
        uint64_t entries_loaded;
        assert(heap.load_blocks(0, dsk.meta_block_size, tmp.data(), false, entries_loaded) == EDOM);
    }

    // reload heap with bad entry size
    {
        blockstore_heap_t heap(&dsk, buffer_area.data());
        auto entry = ((heap_entry_t*)tmp.data());
        entry->size++;
        entry->crc32c = entry->calc_crc32c();
        uint64_t entries_loaded;
        assert(heap.load_blocks(0, dsk.meta_block_size, tmp.data(), false, entries_loaded) == EDOM);
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
        _test_big_write(heap, dsk, 1, 0, 1, 0x20000, true, 0, 0, buffer_area.data());

        // read it to test mvcc
        object_id oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0 };
        heap_entry_t *obj = heap.lock_and_read_entry(oid);
        assert(obj);

        // small_write
        _test_small_write(heap, dsk, 1, 0, 2, 8192, 4096, 16384, true, buffer_area.data());

        // big_write again
        _test_big_write(heap, dsk, 1, 0, 3, 0x40000, stable, 16384, 4096, buffer_area.data());
        assert(!heap.is_buffer_area_free(16384, 4096)); // should not be freed because MVCC includes it
        assert(heap.is_data_used(0x20000)); // should NOT be freed - still referenced by MVCC

        // free mvcc
        heap.unlock_entry(oid);
        if (stable)
        {
            assert(!heap.is_data_used(0x20000)); // should now be freed
        }

        // small_write again
        if (!stable)
        {
            res = _test_do_small_write(heap, dsk, 1, 0, 4, 20480, 4096, 20480, true, buffer_area.data());
            assert(res == EINVAL);
        }
        _test_small_write(heap, dsk, 1, 0, 4, 20480, 4096, 20480, stable, buffer_area.data());

        if (!stable)
        {
            auto obj = heap.read_entry(oid);
            uint32_t mblock = 999999;
            res = heap.add_commit(obj, 4, &mblock);
            assert(res == 0);
            assert(mblock == 0);
            heap.start_block_write(mblock);
            heap.complete_block_write(mblock);
        }

        // read object
        obj = heap.read_entry(oid);
        assert(obj);
        assert(count_writes(heap, obj) == (stable ? 2 : 5));
        assert(obj->version == 4);
        assert((stable ? obj : heap.prev(obj))->type() == BS_HEAP_SMALL_WRITE);
        assert((stable ? obj : heap.prev(obj))->small().location == 20480);
        auto wr = stable ? heap.prev(obj) : heap.prev(heap.prev(obj));
        assert(wr->version == 3);
        assert(wr->entry_type == BS_HEAP_BIG_WRITE|BS_HEAP_STABLE);
        assert(wr->big_location(&heap) == 0x40000);

        if (!stable)
        {
            // old data block will be freed only after compaction on unstable overwrite
            // it COULD be fixed but it complicates the logic and it seems we don't need it
            assert(heap.is_data_used(0x20000));
            assert(heap.is_data_used(0x40000));
            assert(!heap.is_buffer_area_free(16384, 4096));
            assert(!heap.is_buffer_area_free(20480, 4096));

            uint8_t bitmap[dsk.clean_entry_bitmap_size];
            memset(bitmap, 0xFF, dsk.clean_entry_bitmap_size);
            uint32_t mblock = 999999;
            res = heap.add_compact(obj, obj->version, obj->lsn, wr->big_location(&heap),
                false, &mblock, bitmap, bitmap, NULL);
            assert(res == 0);
            assert(mblock == 0);
            heap.start_block_write(mblock);
            heap.complete_block_write(mblock);

            assert(heap.get_to_compact_count() == 0);

            assert(!heap.is_data_used(0x20000));
            assert(heap.is_data_used(0x40000));
            assert(heap.is_buffer_area_free(16384, 4096));
            assert(heap.is_buffer_area_free(20480, 4096));
        }
        else
        {
            // check that the data block 0x20000 is freed and 0x40000 is used
            assert(!heap.is_data_used(0x20000));
            assert(heap.is_data_used(0x40000));
            assert(heap.is_buffer_area_free(16384, 4096));
            assert(!heap.is_buffer_area_free(20480, 4096));
        }
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

        _test_big_write(heap, dsk, 1, 0, 1, 0x20000, true, 0, 0, buffer_area.data());
        _test_big_write(heap, dsk, 1, 0x20000, 1, 0x40000, true, 0, 0, buffer_area.data());
        _test_big_write(heap, dsk, 1, 0x40000, 1, 0, true, 0, 0, buffer_area.data());
        _test_big_write(heap, dsk, 2, 0x60000, 1, 0x60000, true, 0, 0, buffer_area.data());
        _test_big_write(heap, dsk, 2, 0x60000, 2, 0x80000, false, 0, 0, buffer_area.data());
        _test_small_write(heap, dsk, 2, 0x60000, 3, 4096, 4096, 0, false, buffer_area.data()+4096, false);

        obj_ver_id *listing = NULL;
        size_t stable_count = 0, unstable_count = 0;
        res = heap.list_objects(1, (object_id){ .inode = INODE_WITH_POOL(0, 1) },
            (object_id){ .inode = INODE_WITH_POOL(1, 1), .stripe = UINT64_MAX }, &listing, &stable_count, &unstable_count);
        assert(res == EINVAL);
        res = heap.list_objects(1, (object_id){ .inode = INODE_WITH_POOL(1, 1) },
            (object_id){ .inode = INODE_WITH_POOL(2, 1), .stripe = UINT64_MAX }, &listing, &stable_count, &unstable_count);
        assert(res == EINVAL);
        res = heap.list_objects(2, (object_id){ .inode = INODE_WITH_POOL(1, 1) },
            (object_id){ .inode = INODE_WITH_POOL(1, 1), .stripe = UINT64_MAX }, &listing, &stable_count, &unstable_count);
        assert(res == EINVAL);

        res = heap.list_objects(1, (object_id){ .inode = INODE_WITH_POOL(1, 1) },
            (object_id){ .inode = INODE_WITH_POOL(1, UINT64_MAX), .stripe = UINT64_MAX }, &listing, &stable_count, &unstable_count);
        assert(res == 0);
        assert(stable_count == 4);
        assert(unstable_count == 2);
        free(listing);
        listing = NULL;

        res = heap.list_objects(1, (object_id){ .inode = INODE_WITH_POOL(1, 1) },
            (object_id){ .inode = INODE_WITH_POOL(1, 1), .stripe = UINT64_MAX }, &listing, &stable_count, &unstable_count);
        assert(res == 0);
        assert(stable_count == 3);
        assert(unstable_count == 0);
        free(listing);
        listing = NULL;

        heap.reshard(1, 2, 0x20000);

        assert(heap.read_entry((object_id){ .inode = INODE_WITH_POOL(1, 1), .stripe = 0 }));
        assert(heap.read_entry((object_id){ .inode = INODE_WITH_POOL(1, 1), .stripe = 0x20000 }));
        assert(heap.read_entry((object_id){ .inode = INODE_WITH_POOL(1, 1), .stripe = 0x40000 }));
        assert(heap.read_entry((object_id){ .inode = INODE_WITH_POOL(1, 2), .stripe = 0x60000 }));
        assert(!heap.read_entry((object_id){ .inode = INODE_WITH_POOL(1, 2), .stripe = 0x80000 }));

        res = heap.list_objects(3, (object_id){ .inode = INODE_WITH_POOL(1, 1) },
            (object_id){ .inode = INODE_WITH_POOL(1, 1), .stripe = UINT64_MAX }, &listing, &stable_count, &unstable_count);
        assert(res == EINVAL);
        res = heap.list_objects(1, (object_id){ .inode = INODE_WITH_POOL(1, 1) },
            (object_id){ .inode = INODE_WITH_POOL(1, 1), .stripe = UINT64_MAX }, &listing, &stable_count, &unstable_count);
        assert(res == 0);
        assert(stable_count == 2);
        assert(unstable_count == 0);
        free(listing);
        listing = NULL;

        res = heap.list_objects(2, (object_id){ .inode = INODE_WITH_POOL(1, 1) },
            (object_id){ .inode = INODE_WITH_POOL(1, UINT64_MAX), .stripe = UINT64_MAX }, &listing, &stable_count, &unstable_count);
        assert(res == 0);
        assert(stable_count == 2);
        assert(unstable_count == 2);
        free(listing);
        listing = NULL;
    }

    printf("OK test_reshard_list\n");
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
        _test_big_write(heap, dsk, 1, 0, 1, 0x20000, true, 0, 0, buffer_area.data());

        // read it to test mvcc
        object_id oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0 };
        heap_entry_t *obj = heap.lock_and_read_entry(oid);
        assert(obj);

        _test_small_write(heap, dsk, 1, 0, 2, 8192, 4096, 16384, true, buffer_area.data()+16384, false);
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
        _test_big_write(heap, dsk, 1, 0, 1, 0x20000, true, 0, 0, buffer_area.data());
        _test_small_write(heap, dsk, 1, 0, 2, 8192, 4096, 16384, true, buffer_area.data()+16384, false);

        object_id oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0 };
        heap_entry_t *obj = heap.read_entry(oid);
        assert(obj);

        uint32_t mblock = 0;
        // already rolled back to 2
        res = heap.add_rollback(obj, 2, &mblock);
        assert(res == 0);
        // can't be rolled back to 1
        res = heap.add_rollback(obj, 1, NULL);
        assert(res == EBUSY);

        // unstable writes
        _test_big_write(heap, dsk, 1, 0, 3, 0x40000, false, 16384, 4096, buffer_area.data());
        _test_small_write(heap, dsk, 1, 0, 4, 20480, 4096, 20480, false, buffer_area.data()+16384, false);

        obj = heap.read_entry(oid);
        assert(obj);

        // rollback
        assert(heap.is_data_used(0x20000));
        assert(heap.is_data_used(0x40000));
        assert(!heap.is_buffer_area_free(16384, 4096));
        assert(!heap.is_buffer_area_free(20480, 4096));
        res = heap.add_rollback(obj, 5, NULL);
        assert(res == ENOENT);
        res = heap.add_rollback(obj, 2, &mblock);
        assert(res == 0);
        assert(mblock == 0);
        heap.start_block_write(mblock);
        heap.complete_block_write(mblock);
        assert(heap.is_data_used(0x20000));
        assert(heap.is_data_used(0x40000));
        assert(!heap.is_buffer_area_free(16384, 4096));
        assert(!heap.is_buffer_area_free(20480, 4096));

        // check object data
        obj = heap.read_entry(oid);
        assert(obj);
        assert(count_writes(heap, obj) == 5);
        assert(obj->entry_type == BS_HEAP_ROLLBACK);
        assert(obj->lsn == 5);
        auto wr = heap.prev(obj);
        assert(wr->version == 4);
        assert(!(wr->entry_type & BS_HEAP_STABLE));
        wr = heap.prev(wr);
        assert(wr->version == 3);
        assert(!(wr->entry_type & BS_HEAP_STABLE));
        wr = heap.prev(wr);
        assert(wr->version == 2);
        assert(wr->lsn == 2);
        assert(wr->entry_type == BS_HEAP_SMALL_WRITE|BS_HEAP_STABLE);
        assert(wr->small().location == 16384);
        assert(wr->small().len == 4096);
        wr = heap.prev(wr);
        assert(wr->version == 1);
        assert(wr->entry_type == BS_HEAP_BIG_WRITE|BS_HEAP_STABLE);
        assert(wr->big_location(&heap) == 0x20000);

        // compact without rollback (can we do it at all?)
        uint8_t bitmap[dsk.clean_entry_bitmap_size];
        memset(bitmap, 0xFF, dsk.clean_entry_bitmap_size);
        res = heap.add_compact(obj, 2, 2, wr->big_location(&heap),
            false, &mblock, bitmap, bitmap, NULL);
        assert(res == 0);
        assert(mblock == 0);
        heap.start_block_write(mblock);
        heap.complete_block_write(mblock);

        obj = heap.read_entry(oid);
        assert(obj);
        assert(count_writes(heap, obj) == 6);

        assert(heap.get_to_compact_count() == 1);

        assert(heap.is_data_used(0x20000));
        assert(heap.is_data_used(0x40000));
        assert(heap.is_buffer_area_free(16384, 4096));
        assert(!heap.is_buffer_area_free(20480, 4096));
    }

    {
        blockstore_heap_t heap(&dsk, buffer_area.data());
        heap.finish_load();

        // Remove a big write at all
        object_id oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0x20000 };
        _test_big_write(heap, dsk, 1, 0x20000, 1, 0x20000, false, 0, 0, buffer_area.data());

        heap_entry_t *obj = heap.read_entry(oid);
        assert(obj);
        uint32_t mblock = 999999;
        res = heap.add_rollback(obj, 0, &mblock);
        assert(res == 0);
        assert(mblock == 0);
        heap.start_block_write(mblock);
        heap.complete_block_write(mblock);

        // Check that it's not present
        int count = 0;
        obj = heap.read_entry(oid);
        heap.iterate_with_stable(obj, obj->lsn, [&](heap_entry_t *wr, bool stable)
        {
            count++;
            return true;
        });
        assert(count == 0);

        // But the data is still in place, removed only on compaction
        assert(heap.is_data_used(0x20000));

        res = heap.add_compact(obj, 0, 2, 0, true, &mblock, NULL, NULL, NULL);
        assert(res == 0);
        assert(mblock == 0);
        heap.start_block_write(mblock);
        heap.complete_block_write(mblock);

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
        assert(heap.is_buffer_area_free(i*64*1024+4096, 0)); // zero length is always free
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
    dsk.data_device_size = 64*1024*1024;
    dsk.meta_device_size = 5*4096;
    dsk.journal_device_size = 4*1024*1024;
    dsk.data_device = "data";
    dsk.meta_device = "meta";
    dsk.journal_device = "journal";
    dsk.calc_lengths();
    std::vector<uint8_t> buffer_area(dsk.journal_device_size);

    blockstore_heap_t heap(&dsk, buffer_area.data());
    heap.finish_load();
    assert(heap.get_meta_total_space() == 4*4096);

    uint32_t big_write_size = heap.get_big_entry_size();
    uint32_t small_write_size = heap.get_small_entry_size(0, 4096);
    assert(big_write_size == 180);
    assert(small_write_size == 64);
    uint32_t epb = dsk.meta_block_size/big_write_size;
    for (int j = 0; j < 4; j++)
    {
        assert(heap.get_meta_nearfull_blocks() == j);
        for (int i = j*epb; i < j*epb+epb-(j == 3); i++)
        {
            _test_big_write(heap, dsk, 1, i*0x20000, 1, i*0x20000, true, 0, 0, buffer_area.data(), j);
            assert(heap.get_meta_block_used_space(0) == (i < epb ? i+1 : epb)*big_write_size);
            assert(heap.get_meta_block_used_space(1) == (i < epb ? 0 : (i < 2*epb ? i+1-epb : epb)*big_write_size));
            assert(heap.get_meta_block_used_space(2) == (i < 2*epb ? 0 : (i < 3*epb ? i+1-2*epb : epb)*big_write_size));
            assert(heap.get_meta_block_used_space(3) == (i < 3*epb ? 0 : (i < 4*epb ? i+1-3*epb : epb)*big_write_size));
        }
    }

    // New writes are prevented if it may block compaction i.e. if all blocks will have less than <big_entry_size> free space
    assert(ENOSPC == _test_do_big_write(heap, dsk, 1, epb*4*0x20000, 1, epb*4*0x20000, true, 0, 0, buffer_area.data(), 0));

    // We can still do some more overwrites into 3 of 4 nearfull blocks
    int rest_fit = (big_write_size + dsk.meta_block_size % big_write_size)/small_write_size +
        (dsk.meta_block_size % big_write_size)/small_write_size * 2;
    for (int i = 0; i < rest_fit; i++)
    {
        _test_small_write(heap, dsk, 1, 1*0x20000, 5+i, 8192, 4096, (4*epb-1)*16384+3*4096+i*4096, true, buffer_area.data(), false, UINT32_MAX /*any block*/);
    }
    assert(ENOSPC == _test_do_small_write(heap, dsk, 1, 1*0x20000, 5+rest_fit, 8192, 4096, (4*epb-1)*16384+3*4096+rest_fit*4096, true, buffer_area.data(), false, 0));

    printf("OK test_full_alloc\n");
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

        _test_big_write(heap, dsk, 1, 0, 1, 0x20000, true, 0, 4096, buffer_area.data());

        _test_small_write(heap, dsk, 1, 0, 2, 8192, 4096, 0, true, buffer_area.data(), true);

        _test_small_write(heap, dsk, 1, 0, 3, 16384, 4096, 0, true, buffer_area.data(), true);

        object_id oid = { .inode = INODE_WITH_POOL(1, 1), .stripe = 0 };
        heap_entry_t *obj = heap.read_entry(oid);
        assert(obj);
        assert(count_writes(heap, obj) == 3);
        assert(obj->lsn == 3);
        assert(heap.prev(heap.prev(obj))->entry_type == BS_HEAP_BIG_WRITE|BS_HEAP_STABLE);

        assert(check_used_space(heap, dsk, 0));
    }

    printf("OK test_intent_write %s\n", csum ? "csum" : "no_csum");
}

int main(int narg, char *args[])
{
    test_mvcc(false);
    test_mvcc(true);
    test_update(true);
    test_update(false);
    test_delete(true);
    test_delete(false);
    test_defrag_block();
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
    test_destructor_mvcc();
    test_rollback();
    test_alloc_buffer();
    test_full_alloc();
    test_intent_write(true);
    test_intent_write(false);
    return 0;
}
