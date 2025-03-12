// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <stdexcept>
#include "allocator.h"

#include <stdlib.h>
#include <malloc.h>

allocator_t::allocator_t(uint64_t blocks)
{
    if (blocks >= 0x80000000 || blocks <= 1)
    {
        throw std::invalid_argument("blocks");
    }
    uint64_t p2 = 1;
    total = 0;
    while (p2 * 64 < blocks)
    {
        total += p2;
        p2 = p2 * 64;
    }
    total += (blocks+63) / 64;
    mask = new uint64_t[total];
    size = free = blocks;
    last_one_mask = (blocks % 64) == 0
        ? UINT64_MAX
        : (((uint64_t)1 << (blocks % 64)) - 1);
    for (uint64_t i = 0; i < total; i++)
    {
        mask[i] = 0;
    }
}

allocator_t::~allocator_t()
{
    delete[] mask;
}

bool allocator_t::get(uint64_t addr)
{
    if (addr >= size)
    {
        return false;
    }
    uint64_t p2 = 1, offset = 0;
    while (p2 * 64 < size)
    {
        offset += p2;
        p2 = p2 * 64;
    }
    return ((mask[offset + addr/64] >> (addr % 64)) & 1);
}

void allocator_t::set(uint64_t addr, bool value)
{
    if (addr >= size)
    {
        return;
    }
    uint64_t p2 = 1, offset = 0;
    while (p2 * 64 < size)
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
        if (((mask[last] >> bit) & 1) != value64)
        {
            if (is_last)
            {
                free += value ? -1 : 1;
            }
            if (value)
            {
                mask[last] = mask[last] | ((uint64_t)1 << bit);
                if (mask[last] != (!is_last || cur_addr/64 < size/64
                    ? UINT64_MAX : last_one_mask))
                {
                    break;
                }
            }
            else
            {
                mask[last] = mask[last] & ~((uint64_t)1 << bit);
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

uint64_t allocator_t::find_free()
{
    uint64_t p2 = 1, offset = 0, addr = 0, f, i;
    while (p2 < size)
    {
        if (offset+addr >= total)
        {
            return UINT64_MAX;
        }
        uint64_t m = mask[offset + addr];
        for (i = 0, f = 1; i < 64; i++, f <<= 1)
        {
            if (!(m & f))
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
        offset += p2;
        p2 = p2 * 64;
    }
    return addr;
}

uint64_t allocator_t::get_free_count()
{
    return free;
}

// FIXME: Move to utils?
void bitmap_set(void *bitmap, uint64_t start, uint64_t len, uint64_t bitmap_granularity)
{
    if (start == 0 && len == 32*bitmap_granularity)
        *((uint32_t*)bitmap) = UINT32_MAX;
    else if (start == 0 && len == 64*bitmap_granularity)
        *((uint64_t*)bitmap) = UINT64_MAX;
    else
    {
        unsigned bit_start = start / bitmap_granularity;
        unsigned bit_end = ((start + len) + bitmap_granularity - 1) / bitmap_granularity;
        while (bit_start < bit_end)
        {
            if (!(bit_start & 7) && bit_end >= bit_start+8)
            {
                ((uint8_t*)bitmap)[bit_start / 8] = UINT8_MAX;
                bit_start += 8;
            }
            else
            {
                ((uint8_t*)bitmap)[bit_start / 8] |= 1 << (bit_start % 8);
                bit_start++;
            }
        }
    }
}

void bitmap_clear(void *bitmap, uint64_t start, uint64_t len, uint64_t bitmap_granularity)
{
    if (start == 0 && len == 32*bitmap_granularity)
        *((uint32_t*)bitmap) = 0;
    else if (start == 0 && len == 64*bitmap_granularity)
        *((uint64_t*)bitmap) = 0;
    else
    {
        unsigned bit_start = start / bitmap_granularity;
        unsigned bit_end = ((start + len) + bitmap_granularity - 1) / bitmap_granularity;
        while (bit_start < bit_end)
        {
            if (!(bit_start & 7) && bit_end >= bit_start+8)
            {
                ((uint8_t*)bitmap)[bit_start / 8] = 0;
                bit_start += 8;
            }
            else
            {
                ((uint8_t*)bitmap)[bit_start / 8] &= (0xFF ^ (1 << (bit_start % 8)));
                bit_start++;
            }
        }
    }
}

bool bitmap_check(void *bitmap, uint64_t start, uint64_t len, uint64_t bitmap_granularity)
{
    bool r = false;
    if (start == 0 && len == 32*bitmap_granularity)
        r = !!*((uint32_t*)bitmap);
    else if (start == 0 && len == 64*bitmap_granularity)
        r = !!*((uint64_t*)bitmap);
    else
    {
        unsigned bit_start = start / bitmap_granularity;
        unsigned bit_end = ((start + len) + bitmap_granularity - 1) / bitmap_granularity;
        while (bit_start < bit_end)
        {
            if (!(bit_start & 7) && bit_end >= bit_start+8)
            {
                r = r || !!((uint8_t*)bitmap)[bit_start / 8];
                bit_start += 8;
            }
            else
            {
                r = r || (((uint8_t*)bitmap)[bit_start / 8] & (1 << (bit_start % 8)));
                bit_start++;
            }
        }
    }
    return r;
}
