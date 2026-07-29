// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// Simple & stupid freelist memory allocator (allocates buffers within regions)

#include <stdio.h>
#include <assert.h>
#include <errno.h>
#include <unistd.h>
#include <sys/mman.h>
#include "dma_alloc_impl.h"
#include "malloc_or_die.h"

#define HUGEPAGE_SIZE (2UL << 20)

dma_allocator_t::dma_allocator_t(size_t region_size, size_t max_unused, size_t alignment)
{
    this->page_size = (size_t)sysconf(_SC_PAGESIZE);
    if ((alignment & (alignment - 1)) || alignment > this->page_size)
    {
        fprintf(stderr, "DMA allocator alignment must be a power of 2 and not exceed page_size");
        abort();
    }
    this->region_size = region_size ? region_size : 1048576;
    this->max_unused = max_unused ? max_unused : 500*1048576;
    this->alignment = alignment ? alignment : 8;
    size_t region_align = (alignment < page_size ? page_size : alignment);
    this->region_size = ((this->region_size + region_align-1) / region_align) * region_align;
    this->try_hugepages = !(this->region_size % HUGEPAGE_SIZE) && !(HUGEPAGE_SIZE % alignment);
}

void dma_allocator_t::destroy_region(dma_region_t *rgn)
{
    freebuffers -= rgn->len;
    dereg_mr(rgn->buf, rgn->len, rgn->handle);
    if (rgn->mapped)
        munmap(rgn->buf, rgn->len);
    else
        ::free(rgn->buf);
    regions.erase(rgn);
    delete rgn;
}

void dma_allocator_t::free_unused_buffers(size_t max_unused, bool force)
{
    auto free_it = freelist.end();
    if (free_it == freelist.begin())
        return;
    free_it--;
    do
    {
        auto frag_it = frags.find(free_it->buf);
        assert(frag_it != frags.end());
        if (frag_it->second.len != frag_it->second.rgn->len)
        {
            if (force)
            {
                fprintf(stderr, "BUG: Attempt to destroy DMA allocator while buffers are not freed yet\n");
                abort();
            }
            break;
        }
        destroy_region(frag_it->second.rgn);
        frags.erase(frag_it);
        if (free_it == freelist.begin())
        {
            freelist.erase(free_it);
            break;
        }
        freelist.erase(free_it--);
    } while (freebuffers > max_unused);
}

dma_allocator_t::~dma_allocator_t()
{
    assert(!freebuffers);
    assert(!regions.size());
    assert(!frags.size());
    assert(!freelist.size());
}

void *dma_allocator_t::alloc(size_t size, void **handle, size_t *offset)
{
    auto it = freelist.lower_bound((dma_free_t){ .len = size });
    size = (size + alignment - 1) & ~(alignment - 1);
    if (it == freelist.end())
    {
        // round size up to region_size
        size_t alloc_size = ((size + region_size - 1) / region_size) * region_size;
        dma_region_t *r = new dma_region_t;
        if (try_hugepages)
        {
            r->buf = mmap(NULL, alloc_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE | MAP_HUGETLB, -1, 0);
            if (r->buf != MAP_FAILED)
                r->mapped = true;
        }
        if (!try_hugepages || r->buf == MAP_FAILED)
            r->buf = memalign_or_die(page_size, alloc_size);
        r->len = alloc_size;
        r->handle = reg_mr(r->buf, r->len);
        regions.insert(r);
        frags[r->buf] = (dma_frag_t){ .rgn = r, .len = alloc_size, .is_free = true };
        it = freelist.insert((dma_free_t){ .len = alloc_size, .buf = r->buf }).first;
        freebuffers += alloc_size;
    }
    void *ptr = it->buf;
    auto & frag = frags.at(ptr);
    freelist.erase(it);
    assert(frag.len >= size && frag.is_free);
    if (frag.len == frag.rgn->len)
    {
        freebuffers -= frag.rgn->len;
    }
    if (frag.len == size)
    {
        frag.is_free = false;
    }
    else
    {
        freelist.insert((dma_free_t){ .len = frag.len-size, .buf = ptr });
        frag.len -= size;
        ptr = (uint8_t*)ptr + frag.len;
        frags[ptr] = (dma_frag_t){ .rgn = frag.rgn, .len = size, .is_free = false };
    }
    if (handle)
    {
        *handle = frag.rgn->handle;
    }
    if (offset)
    {
        *offset = ((uint8_t*)ptr - (uint8_t*)frag.rgn->buf);
    }
    return ptr;
}

void dma_allocator_t::free(void *buf)
{
    auto frag_it = frags.find(buf);
    if (frag_it == frags.end())
    {
        fprintf(stderr, "BUG: Attempt to double-free DMA buffer fragment 0x%jx\n", (size_t)buf);
        return;
    }
    auto prev_it = frag_it, next_it = frag_it;
    if (frag_it != frags.begin())
        prev_it--;
    next_it++;
    bool merge_back = prev_it != frag_it &&
        prev_it->second.is_free &&
        prev_it->second.rgn == frag_it->second.rgn &&
        (uint8_t*)prev_it->first+prev_it->second.len == frag_it->first;
    bool merge_next = next_it != frags.end() &&
        next_it->second.is_free &&
        next_it->second.rgn == frag_it->second.rgn &&
        next_it->first == (uint8_t*)frag_it->first+frag_it->second.len;
    if (merge_back && merge_next)
    {
        prev_it->second.len += frag_it->second.len + next_it->second.len;
        freelist.erase((dma_free_t){ .len = next_it->second.len, .buf = next_it->first });
        frags.erase(next_it);
        frags.erase(frag_it);
        frag_it = prev_it;
    }
    else if (merge_back)
    {
        freelist.erase((dma_free_t){ .len = prev_it->second.len, .buf = prev_it->first });
        prev_it->second.len += frag_it->second.len;
        frags.erase(frag_it);
        freelist.insert((dma_free_t){ .len = prev_it->second.len, .buf = prev_it->first });
        frag_it = prev_it;
    }
    else if (merge_next)
    {
        frag_it->second.is_free = true;
        frag_it->second.len += next_it->second.len;
        freelist.erase((dma_free_t){ .len = next_it->second.len, .buf = next_it->first });
        frags.erase(next_it);
        freelist.insert((dma_free_t){ .len = frag_it->second.len, .buf = frag_it->first });
    }
    else
    {
        frag_it->second.is_free = true;
        freelist.insert((dma_free_t){ .len = frag_it->second.len, .buf = frag_it->first });
    }
    assert(frag_it->second.len <= frag_it->second.rgn->len);
    if (frag_it->second.len == frag_it->second.rgn->len)
    {
        // The whole buffer is freed
        freebuffers += frag_it->second.rgn->len;
        if (freebuffers > max_unused)
        {
            free_unused_buffers(max_unused, false);
        }
    }
}

void* dma_allocator_t::get_handle(void *buf, size_t *offset)
{
    auto frag_it = frags.upper_bound(buf);
    if (frag_it != frags.begin())
    {
        frag_it--;
        if ((uint8_t*)frag_it->first + frag_it->second.len > buf)
        {
            if (offset)
                *offset = (uint8_t*)buf - (uint8_t*)frag_it->second.rgn->buf;
            return frag_it->second.rgn->handle;
        }
    }
    fprintf(stderr, "BUG: Attempt to use an unknown DMA allocator buffer fragment 0x%zx\n", (size_t)buf);
    abort();
}
