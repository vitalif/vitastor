// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// Simple & stupid RDMA-enabled memory allocator (allocates buffers within ibv_mr's)

#include <stdio.h>
#include <assert.h>
#include <map>
#include <set>
#ifdef WITH_RDMA
#include <infiniband/verbs.h>
#endif
#include "rdma_alloc.h"
#include "malloc_or_die.h"

struct dma_region_t
{
    void *buf = NULL;
    size_t len = 0;
    void *handle = NULL;
};

struct dma_frag_t
{
    dma_region_t *rgn = NULL;
    size_t len = 0;
    bool is_free = false;
};

struct dma_free_t
{
    size_t len = 0;
    void *buf = NULL;
};

inline bool operator < (const dma_free_t &a, const dma_free_t &b)
{
    return a.len < b.len || a.len == b.len && a.buf < b.buf;
}

class dma_allocator_t: public dma_allocator_i
{
protected:
    size_t region_size = 1048576;
    size_t max_unused = 500*1048576;

    std::set<dma_region_t*> regions;
    std::map<void*, dma_frag_t> frags;
    std::set<dma_free_t> freelist;
    size_t freebuffers = 0;

    virtual void* reg_mr(void* buf, size_t len) = 0;
    virtual void dereg_mr(void *handle) = 0;
    void free_unused_buffers(size_t max_unused, bool force);
public:
    dma_allocator_t(size_t region_size, size_t max_unused);
    virtual ~dma_allocator_t();
    void *alloc(size_t size) override;
    void free(void *buf) override;
    void *get_handle(void *buf) override;
};

#ifdef WITH_RDMA
class rdma_allocator_t: public dma_allocator_t
{
    int ibv_access = IBV_ACCESS_LOCAL_WRITE;
    ibv_pd *pd = NULL;

public:
    rdma_allocator_t(ibv_pd *pd, size_t region_size, size_t max_unused, int ibv_access):
        dma_allocator_t(region_size, max_unused)
    {
        this->pd = pd;
        this->ibv_access = ibv_access;
    }

    ~rdma_allocator_t()
    {
        free_unused_buffers(0, true);
    }

    void* reg_mr(void* buf, size_t len) override
    {
        ibv_mr *mr = ibv_reg_mr(pd, buf, len, ibv_access);
        if (!mr)
        {
            fprintf(stderr, "Failed to register RDMA memory region: %s\n", strerror(errno));
            exit(1);
        }
        return mr;
    }

    void dereg_mr(void *handle) override
    {
        ibv_dereg_mr((ibv_mr*)handle);
    }
};
#endif

dma_allocator_t::dma_allocator_t(size_t region_size, size_t max_unused)
{
    this->region_size = region_size ? region_size : 1048576;
    this->max_unused = max_unused ? max_unused : 500*1048576;
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
                fprintf(stderr, "BUG: Attempt to destroy RDMA allocator while buffers are not freed yet\n");
                abort();
            }
            break;
        }
        freebuffers -= frag_it->second.rgn->len;
        dereg_mr(frag_it->second.rgn->handle);
        free(frag_it->second.rgn);
        regions.erase(frag_it->second.rgn);
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

void *dma_allocator_t::alloc(size_t size)
{
    auto it = freelist.lower_bound((dma_free_t){ .len = size });
    if (it == freelist.end())
    {
        // round size up to dma_malloc_size (1 MB)
        size_t alloc_size = ((size + region_size - 1) / region_size) * region_size;
        dma_region_t *r = (dma_region_t*)malloc_or_die(alloc_size + sizeof(dma_region_t));
        r->buf = r+1;
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
    return ptr;
}

void dma_allocator_t::free(void *buf)
{
    auto frag_it = frags.find(buf);
    if (frag_it == frags.end())
    {
        fprintf(stderr, "BUG: Attempt to double-free RDMA buffer fragment 0x%jx\n", (size_t)buf);
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

void* dma_allocator_t::get_handle(void *buf)
{
    auto frag_it = frags.upper_bound(buf);
    if (frag_it != frags.begin())
    {
        frag_it--;
        if ((uint8_t*)frag_it->first + frag_it->second.len > buf)
            return frag_it->second.rgn->handle;
    }
    fprintf(stderr, "BUG: Attempt to use an unknown DMA allocator buffer fragment 0x%zx\n", (size_t)buf);
    abort();
}

#ifdef WITH_RDMA
dma_allocator_i *rdma_malloc_create(ibv_pd *pd, size_t region_size, size_t max_unused, int rdma_access)
{
    return new rdma_allocator_t(pd, region_size, max_unused, rdma_access);
}
#endif
