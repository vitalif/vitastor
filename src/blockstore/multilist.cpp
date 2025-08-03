// Variable-length O(1) disk space allocator
// Copyright (c) Vitaliy Filippov, 2025+
// License: VNPL-1.1 (see README.md for details)

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <set>
#include "multilist.h"

multilist_alloc_t::multilist_alloc_t(uint32_t count, uint32_t maxn):
    count(count), maxn(maxn)
{
    // not-so-memory-efficient: 16 MB memory per 1 GB buffer space, but buffer spaces are small, so OK
    assert(count > 1 && count < 0x80000000);
    sizes.resize(count);
    nexts.resize(count); // nexts[i] = 0 -> area is used; nexts[i] = 1 -> no next; nexts[i] >= 2 -> next item
    prevs.resize(count);
    heads.resize(maxn); // heads[i] = 0 -> empty list; heads[i] >= 1 -> list head
    sizes[0] = count;
    sizes[count-1] = -count; // end
    nexts[0] = 1;
    heads[maxn-1] = 1;
#ifdef MULTILIST_TRACE
    print();
#endif
}

bool multilist_alloc_t::is_free(uint32_t pos)
{
    assert(pos < count);
    if (sizes[pos] < 0)
        pos += sizes[pos]+1;
    while (pos > 0 && !sizes[pos])
        pos--;
    return nexts[pos] > 0;
}

uint32_t multilist_alloc_t::find(uint32_t size)
{
    assert(size > 0);
    assert(size <= maxn);
    for (uint32_t i = size-1; i < maxn; i++)
    {
        if (heads[i])
        {
            return heads[i]-1;
        }
    }
    return UINT32_MAX;
}

void multilist_alloc_t::verify()
{
    std::set<uint32_t> reachable;
    for (int i = 0; i < maxn; i++)
    {
        uint32_t cur = heads[i];
        while (cur)
        {
            if (!nexts[cur-1])
            {
                fprintf(stderr, "ERROR: item %d from freelist %d is not free\n", cur-1, i);
                print();
                abort();
            }
            if (nexts[cur-1] >= count+2)
            {
                fprintf(stderr, "ERROR: next out of range at %d: %d\n", cur-1, nexts[cur-1]);
                print();
                abort();
            }
            if (!(i < maxn-1 ? sizes[cur-1] == i+1 : (sizes[cur-1] >= i+1)))
            {
                fprintf(stderr, "ERROR: item %d is in wrong freelist: expected size %d, but actual size is %d\n", cur-1, i+1, sizes[cur-1]);
                print();
                abort();
            }
            if (reachable.find(cur-1) != reachable.end())
            {
                fprintf(stderr, "ERROR: doubly-claimed item %d\n", cur-1);
                print();
                abort();
            }
            reachable.insert(cur-1);
            cur = nexts[cur-1]-1;
        }
    }
    for (int i = 0; i < count; )
    {
        if (sizes[i])
        {
            assert(i+sizes[i] <= count);
            if (sizes[i] > 1 && sizes[i+sizes[i]-1] != -sizes[i])
            {
                fprintf(stderr, "ERROR: start/end mismatch at %d: sizes[%d] should be %d, but is %d\n", i, i+sizes[i]-1, -sizes[i], sizes[i+sizes[i]-1]);
                print();
                abort();
            }
            for (int j = i+1; j < i+sizes[i]-1; j++)
            {
                if (sizes[j])
                {
                    fprintf(stderr, "ERROR: internal non-zero at %d: %d\n", j, sizes[j]);
                    print();
                    abort();
                }
            }
            if (nexts[i] && reachable.find(i) == reachable.end())
            {
                fprintf(stderr, "ERROR: %d is unreachable from heads\n", i);
                print();
                abort();
            }
            if (nexts[i] >= 2)
            {
                if (nexts[i] >= 2+count)
                {
                    fprintf(stderr, "ERROR: next out of range at %d: %d\n", i, nexts[i]);
                    print();
                    abort();
                }
                if (prevs[nexts[i]-2] != i+1)
                {
                    fprintf(stderr, "ERROR: prev[next] (%d) != this (%d) at %d", prevs[nexts[i]-2], i+1, i);
                    print();
                    abort();
                }
            }
            i += (sizes[i] > 1 ? sizes[i] : 1);
        }
        else
            i++;
    }
}

void multilist_alloc_t::print()
{
    printf("heads:");
    for (int i = 0; i < maxn; i++)
        if (heads[i])
            printf(" %u=%u", i, heads[i]);
    printf("\n");
    printf("sizes:");
    for (int i = 0; i < count; i++)
        if (sizes[i])
            printf(" %d=%d", i, sizes[i]);
    printf("\n");
    printf("prevs:");
    for (int i = 0; i < count; i++)
        if (prevs[i])
            printf(" %d=%d", i, prevs[i]);
    printf("\n");
    printf("nexts:");
    for (int i = 0; i < count; i++)
        if (nexts[i])
            printf(" %d=%d", i, nexts[i]);
    printf("\n");
    printf("items:");
    for (int i = 0; i < count; )
    {
        if (sizes[i])
        {
            printf(" %u=(s:%d,n:%u,p:%u)", i, sizes[i], nexts[i], prevs[i]);
            assert(i+sizes[i] <= count);
            i += (sizes[i] > 1 ? sizes[i] : 1);
        }
        else
            i++;
    }
    printf("\n");
}

void multilist_alloc_t::use(uint32_t pos, uint32_t size)
{
    assert(pos < count);
    if (sizes[pos] <= 0)
    {
        uint32_t start = pos;
        if (sizes[start] < 0)
            start += sizes[start]+1;
        else
            while (start > 0 && !sizes[start])
                start--;
        assert(sizes[start] >= size);
        use_full(start);
        uint32_t full = sizes[start];
        sizes[pos-1] = -pos+start;
        sizes[start] = pos-start;
        free(start);
        sizes[pos+size-1] = -size;
        sizes[pos] = size;
        if (pos+size < start+full)
        {
            sizes[start+full-1] = -(start+full-pos-size);
            sizes[pos+size] = start+full-pos-size;
            free(pos+size);
        }
    }
    else
    {
        assert(sizes[pos] >= size);
        use_full(pos);
        if (sizes[pos] > size)
        {
            uint32_t full = sizes[pos];
            sizes[pos+size-1] = -size;
            sizes[pos] = size;
            sizes[pos+full-1] = -full+size;
            sizes[pos+size] = full-size;
            free(pos+size);
        }
    }
#ifdef MULTILIST_TRACE
    print();
#endif
}

void multilist_alloc_t::use_full(uint32_t pos)
{
    uint32_t prevsize = sizes[pos];
    assert(prevsize);
    assert(nexts[pos]);
    uint32_t pi = (prevsize < maxn ? prevsize : maxn)-1;
    if (heads[pi] == pos+1)
        heads[pi] = nexts[pos]-1;
    if (prevs[pos])
        nexts[prevs[pos]-1] = nexts[pos];
    if (nexts[pos] >= 2)
        prevs[nexts[pos]-2] = prevs[pos];
    prevs[pos] = 0;
    nexts[pos] = 0;
}

void multilist_alloc_t::free(uint32_t pos)
{
    do_free(pos);
#ifdef MULTILIST_TRACE
    print();
#endif
}

void multilist_alloc_t::do_free(uint32_t pos)
{
    assert(!nexts[pos]);
    uint32_t size = sizes[pos];
    assert(size > 0);
    // merge with previous?
    if (pos > 0 && nexts[pos+(sizes[pos-1] == 1 ? -1 : sizes[pos-1])] > 0)
    {
        assert(sizes[pos-1] < 0 || sizes[pos-1] == 1);
        uint32_t prevsize = sizes[pos-1] < 0 ? -sizes[pos-1] : 1;
        use_full(pos-prevsize);
        sizes[pos] = 0;
        sizes[pos-1] = 0;
        size += prevsize;
        pos -= prevsize;
        sizes[pos+size-1] = -size;
        sizes[pos] = size;
    }
    // merge with next?
    if (pos+size < count && nexts[pos+size] >= 1)
    {
        uint32_t nextsize = sizes[pos+size];
        use_full(pos+size);
        sizes[pos+size] = 0;
        sizes[pos+size-1] = 0;
        size += nextsize;
        sizes[pos+size-1] = -size;
        sizes[pos] = size;
    }
    uint32_t ni = (size < maxn ? size : maxn)-1;
    nexts[pos] = heads[ni]+1;
    prevs[pos] = 0;
    if (heads[ni])
        prevs[heads[ni]-1] = pos+1;
    heads[ni] = pos+1;
}

multilist_index_t::multilist_index_t(uint32_t count, uint32_t max_used, uint32_t init_used):
    count(count), max_used(max_used)
{
    assert(init_used < max_used);
    nexts.resize(count, UINT32_MAX);
    prevs.resize(count, UINT32_MAX);
    heads.resize(max_used, UINT32_MAX);
    for (size_t i = 0; i < count-1; i++)
    {
        nexts[i] = i+1;
        prevs[i+1] = i;
    }
    prevs[0] = UINT32_MAX;
    nexts[count-1] = UINT32_MAX;
    heads[init_used] = 0;
}

uint32_t multilist_index_t::find(uint32_t wanted_used)
{
    assert(wanted_used < max_used);
    return heads[wanted_used];
}

void multilist_index_t::change(uint32_t pos, uint32_t old_used, uint32_t new_used)
{
    if (new_used == old_used)
        return;
    assert(old_used < max_used && new_used < max_used);
    if (prevs[pos] != UINT32_MAX)
        nexts[prevs[pos]] = nexts[pos];
    if (nexts[pos] != UINT32_MAX)
        prevs[nexts[pos]] = prevs[pos];
    if (heads[old_used] == pos)
        heads[old_used] = nexts[pos];
    prevs[pos] = UINT32_MAX;
    if (heads[new_used] != UINT32_MAX)
        prevs[heads[new_used]] = pos;
    nexts[pos] = heads[new_used];
    heads[new_used] = pos;
}

void multilist_index_t::print()
{
    printf("heads:");
    for (int i = 0; i < max_used; i++)
        if (heads[i] != UINT32_MAX)
            printf(" %u=%u", i, heads[i]);
    printf("\n");
    printf("prevs:");
    for (int i = 0; i < count; i++)
        if (prevs[i] != UINT32_MAX)
            printf(" %d=%d", i, prevs[i]);
    printf("\n");
    printf("nexts:");
    for (int i = 0; i < count; i++)
        if (nexts[i] != UINT32_MAX)
            printf(" %d=%d", i, nexts[i]);
    printf("\n");
}
