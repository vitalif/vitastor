#include <stdexcept>
#include "allocator.h"

#include <stdlib.h>
#include <malloc.h>

allocator::allocator(uint64_t blocks)
{
    if (blocks >= 0x80000000 || blocks <= 1)
    {
        throw std::invalid_argument("blocks");
    }
    uint64_t p2 = 1, total = 1;
    while (p2 * 64 < blocks)
    {
        p2 = p2 * 64;
        total += p2;
    }
    total -= p2;
    total += (blocks+63) / 64;
    mask = new uint64_t[2 + total];
    size = blocks;
    last_one_mask = (blocks % 64) == 0
        ? UINT64_MAX
        : ~(UINT64_MAX << (64 - blocks % 64));
    for (uint64_t i = 0; i < total; i++)
    {
        mask[i] = 0;
    }
}

allocator::~allocator()
{
    delete[] mask;
}

void allocator::set(uint64_t addr, bool value)
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
            if (value)
            {
                mask[last] = mask[last] | (1l << bit);
                if (mask[last] != (!is_last || cur_addr/64 < size/64
                    ? UINT64_MAX : last_one_mask))
                {
                    break;
                }
            }
            else
            {
                mask[last] = mask[last] & ~(1l << bit);
                if (mask[last] != 0)
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

uint64_t allocator::find_free()
{
    uint64_t p2 = 1, offset = 0, addr = 0, f, i;
    while (p2 < size)
    {
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
        if (addr >= size)
        {
            // No space
            return UINT64_MAX;
        }
        offset += p2;
        p2 = p2 * 64;
    }
    return addr;
}
