// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>

#include "malloc_or_die.h"
#include "errno.h"
#include "crc32c.h"

int main(int narg, char *args[])
{
    int bufsize = 65536;
    uint8_t *buf = (uint8_t*)malloc_or_die(bufsize);
    uint32_t csum = 0;
    while (1)
    {
        int r = read(0, buf, bufsize);
        if (r <= 0 && errno != EAGAIN && errno != EINTR)
            break;
        csum = crc32c(csum, buf, r);
    }
    free(buf);
    printf("%08x\n", csum);
    return 0;
}
