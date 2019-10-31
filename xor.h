#pragma once

#include <stdint.h>

inline void memxor(uint8_t *r1, uint8_t *r2, uint8_t *res, unsigned int len)
{
    unsigned int i;
    for (i = 0; i < len; ++i)
    {
        res[i] = r1[i] ^ r2[i];
    }
}
