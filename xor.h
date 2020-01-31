#pragma once

#include <stdint.h>

inline void memxor(const void *r1, const void *r2, void *res, unsigned int len)
{
    unsigned int i;
    for (i = 0; i < len; ++i)
    {
        ((uint8_t*)res)[i] = ((uint8_t*)r1)[i] ^ ((uint8_t*)r2)[i];
    }
}
