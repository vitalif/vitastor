#include "allocator.h"

#include <stdlib.h>
#include <malloc.h>

allocator *allocator_create(uint64_t blocks)
{
    if (blocks >= 0x80000000 || blocks <= 1)
    {
        return NULL;
    }
    uint64_t p2 = 1, total = 1;
    while (p2 * 64 < blocks)
    {
        p2 = p2 * 64;
        total += p2;
    }
    total -= p2;
    total += (blocks+63) / 64;
    allocator *buf = (allocator*)memalign(sizeof(uint64_t), (2 + total)*sizeof(uint64_t));
    buf->size = blocks;
    buf->last_one_mask = (blocks % 64) == 0
        ? UINT64_MAX
        : ~(UINT64_MAX << (64 - blocks % 64));
    for (uint64_t i = 0; i < blocks; i++)
    {
        buf->mask[i] = 0;
    }
    return buf;
}

void allocator_destroy(allocator *alloc)
{
    free(alloc);
}

void allocator_set(allocator *alloc, uint64_t addr, bool value)
{
    if (addr >= alloc->size)
    {
        return;
    }
    uint64_t p2 = 1, offset = 0;
    while (p2 * 64 < alloc->size)
    {
        offset += p2;
        p2 = p2 * 64;
    }
    uint64_t cur_addr = addr;
    bool is_last = true;
    uint64_t value64 = value ? 1 : 0;
    while (1)
    {
        uint64_t last = offset + cur_addr/64;
        uint64_t bit = cur_addr % 64;
        if (((alloc->mask[last] >> bit) & 1) != value64)
        {
            if (value)
            {
                alloc->mask[last] = alloc->mask[last] | (1 << bit);
                if (alloc->mask[last] != (!is_last || cur_addr/64 < alloc->size/64
                    ? UINT64_MAX : alloc->last_one_mask))
                {
                    break;
                }
            }
            else
            {
                alloc->mask[last] = alloc->mask[last] & ~(1 << bit);
                if (alloc->mask[last] != 0)
                {
                    break;
                }
            }
            is_last = false;
            if (p2 > 1)
            {
                p2 = p2 / 64;
                offset -= p2;
                cur_addr /= 64;
            }
            else
            {
                break;
            }
        }
        else
        {
            break;
        }
    }
}

uint64_t allocator_find_free(allocator *alloc)
{
    uint64_t p2 = 1, offset = 0, addr = 0, f, i;
    while (p2 < alloc->size)
    {
        uint64_t mask = alloc->mask[offset + addr];
        for (i = 0, f = 1; i < 64; i++, f <<= 1)
        {
            if (!(mask & f))
            {
                break;
            }
        }
        if (i == 64)
        {
            // No space
            return UINT64_MAX;
        }
        addr = (addr * 64) | i;
        if (addr >= alloc->size)
        {
            // No space
            return UINT64_MAX;
        }
        offset += p2;
        p2 = p2 * 64;
    }
    return addr;
}
