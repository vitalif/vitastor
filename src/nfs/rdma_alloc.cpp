// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// Simple & stupid RDMA-enabled memory allocator (allocates buffers within ibv_mr's)

#include <stdio.h>
#include <assert.h>
#include <map>
#include <set>
#include "rdma_alloc.h"
#include "malloc_or_die.h"

struct rdma_region_t
{
    void *buf = NULL;
    size_t len = 0;
    ibv_mr *mr = NULL;
};

struct rdma_frag_t
{
    rdma_region_t *rgn = NULL;
    size_t len = 0;
    bool is_free = false;
};

struct rdma_free_t
{
    size_t len = 0;
    void *buf = NULL;
};

inline bool operator < (const rdma_free_t &a, const rdma_free_t &b)
{
    return a.len < b.len || a.len == b.len && a.buf < b.buf;
}

struct rdma_allocator_t
{
    size_t rdma_alloc_size = 1048576;
    size_t rdma_max_unused = 500*1048576;
    int rdma_access = IBV_ACCESS_LOCAL_WRITE;
    ibv_pd *pd = NULL;

    std::set<rdma_region_t*> regions;
    std::map<void*, rdma_frag_t> frags;
    std::set<rdma_free_t> freelist;
    size_t freebuffers = 0;
};

rdma_allocator_t *rdma_malloc_create(ibv_pd *pd, size_t rdma_alloc_size, size_t rdma_max_unused, int rdma_access)
{
    rdma_allocator_t *self = new rdma_allocator_t();
    self->pd = pd;
    self->rdma_alloc_size = rdma_alloc_size ? rdma_alloc_size : 1048576;
    self->rdma_max_unused = rdma_max_unused ? rdma_max_unused : 500*1048576;
    self->rdma_access = rdma_access;
    return self;
}

static void rdma_malloc_free_unused_buffers(rdma_allocator_t *self, size_t max_unused, bool force)
{
    auto free_it = self->freelist.end();
    if (free_it == self->freelist.begin())
        return;
    free_it--;
    do
    {
        auto frag_it = self->frags.find(free_it->buf);
        assert(frag_it != self->frags.end());
        if (frag_it->second.len != frag_it->second.rgn->len)
        {
            if (force)
            {
                fprintf(stderr, "BUG: Attempt to destroy RDMA allocator while buffers are not freed yet\n");
                abort();
            }
            break;
        }
        self->freebuffers -= frag_it->second.rgn->len;
        ibv_dereg_mr(frag_it->second.rgn->mr);
        free(frag_it->second.rgn);
        self->regions.erase(frag_it->second.rgn);
        self->frags.erase(frag_it);
        if (free_it == self->freelist.begin())
        {
            self->freelist.erase(free_it);
            break;
        }
        self->freelist.erase(free_it--);
    } while (self->freebuffers > max_unused);
}

void rdma_malloc_destroy(rdma_allocator_t *self)
{
    rdma_malloc_free_unused_buffers(self, 0, true);
    assert(!self->freebuffers);
    assert(!self->regions.size());
    assert(!self->frags.size());
    assert(!self->freelist.size());
    delete self;
}

void *rdma_malloc_alloc(rdma_allocator_t *self, size_t size)
{
    auto it = self->freelist.lower_bound((rdma_free_t){ .len = size });
    if (it == self->freelist.end())
    {
        // round size up to rdma_malloc_size (1 MB)
        size_t alloc_size = ((size + self->rdma_alloc_size - 1) / self->rdma_alloc_size) * self->rdma_alloc_size;
        rdma_region_t *r = (rdma_region_t*)malloc_or_die(alloc_size + sizeof(rdma_region_t));
        r->buf = r+1;
        r->len = alloc_size;
        r->mr = ibv_reg_mr(self->pd, r->buf, r->len, self->rdma_access);
        if (!r->mr)
        {
            fprintf(stderr, "Failed to register RDMA memory region: %s\n", strerror(errno));
            exit(1);
        }
        self->regions.insert(r);
        self->frags[r->buf] = (rdma_frag_t){ .rgn = r, .len = alloc_size, .is_free = true };
        it = self->freelist.insert((rdma_free_t){ .len = alloc_size, .buf = r->buf }).first;
        self->freebuffers += alloc_size;
    }
    void *ptr = it->buf;
    auto & frag = self->frags.at(ptr);
    self->freelist.erase(it);
    assert(frag.len >= size && frag.is_free);
    if (frag.len == frag.rgn->len)
    {
        self->freebuffers -= frag.rgn->len;
    }
    if (frag.len == size)
    {
        frag.is_free = false;
    }
    else
    {
        frag.len -= size;
        ptr = (uint8_t*)ptr + frag.len;
        self->freelist.insert((rdma_free_t){ .len = frag.len, .buf = frag.rgn->buf });
        self->frags[ptr] = (rdma_frag_t){ .rgn = frag.rgn, .len = size, .is_free = false };
    }
    return ptr;
}

void rdma_malloc_free(rdma_allocator_t *self, void *buf)
{
    auto frag_it = self->frags.find(buf);
    if (frag_it == self->frags.end())
    {
        fprintf(stderr, "BUG: Attempt to double-free RDMA buffer fragment 0x%jx\n", (size_t)buf);
        return;
    }
    auto prev_it = frag_it, next_it = frag_it;
    if (frag_it != self->frags.begin())
        prev_it--;
    next_it++;
    bool merge_back = prev_it != frag_it &&
        prev_it->second.is_free &&
        prev_it->second.rgn == frag_it->second.rgn &&
        (uint8_t*)prev_it->first+prev_it->second.len == frag_it->first;
    bool merge_next = next_it != self->frags.end() &&
        next_it->second.is_free &&
        next_it->second.rgn == frag_it->second.rgn &&
        next_it->first == (uint8_t*)frag_it->first+frag_it->second.len;
    if (merge_back && merge_next)
    {
        prev_it->second.len += frag_it->second.len + next_it->second.len;
        self->freelist.erase((rdma_free_t){ .len = next_it->second.len, .buf = next_it->first });
        self->frags.erase(next_it);
        self->frags.erase(frag_it);
        frag_it = prev_it;
    }
    else if (merge_back)
    {
        prev_it->second.len += frag_it->second.len;
        self->frags.erase(frag_it);
        frag_it = prev_it;
    }
    else if (merge_next)
    {
        frag_it->second.is_free = true;
        frag_it->second.len += next_it->second.len;
        self->freelist.erase((rdma_free_t){ .len = next_it->second.len, .buf = next_it->first });
        self->frags.erase(next_it);
    }
    else
    {
        frag_it->second.is_free = true;
        self->freelist.insert((rdma_free_t){ .len = frag_it->second.len, .buf = frag_it->first });
    }
    assert(frag_it->second.len <= frag_it->second.rgn->len);
    if (frag_it->second.len == frag_it->second.rgn->len)
    {
        // The whole buffer is freed
        self->freebuffers += frag_it->second.rgn->len;
        if (self->freebuffers > self->rdma_max_unused)
        {
            rdma_malloc_free_unused_buffers(self, self->rdma_max_unused, false);
        }
    }
}

uint32_t rdma_malloc_get_lkey(rdma_allocator_t *self, void *buf)
{
    auto frag_it = self->frags.upper_bound(buf);
    if (frag_it != self->frags.begin())
    {
        frag_it--;
        if ((uint8_t*)frag_it->first + frag_it->second.len > buf)
            return frag_it->second.rgn->mr->lkey;
    }
    fprintf(stderr, "BUG: Attempt to use an unknown RDMA buffer fragment 0x%zx\n", (size_t)buf);
    abort();
}
