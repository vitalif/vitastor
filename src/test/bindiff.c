// Copyright (c) Vitaliy Filippov, 2004+
// License: VNPL-1.1 (see README.md for details)

#ifndef _LARGEFILE64_SOURCE
#define _LARGEFILE64_SOURCE
#endif

#include <string.h>
#include <sys/stat.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>

#define BUFSIZE 0x100000

uint64_t filelength(int fd)
{
    struct stat st;
    if (fstat(fd, &st) < 0)
    {
        fprintf(stderr, "fstat failed: %s\n", strerror(errno));
        return 0;
    }
    if (st.st_size < 0)
    {
        return 0;
    }
    return (uint64_t)st.st_size;
}

size_t read_blocking(int fd, void *read_buf, size_t remaining)
{
    size_t done = 0;
    while (done < remaining)
    {
        ssize_t r = read(fd, read_buf, remaining-done);
        if (r <= 0)
        {
            if (!errno)
            {
                // EOF
                return done;
            }
            else if (errno != EINTR && errno != EAGAIN && errno != EPIPE)
            {
                perror("read");
                exit(1);
            }
            continue;
        }
        done += (size_t)r;
        read_buf = (uint8_t*)read_buf + r;
    }
    return done;
}

size_t write_blocking(int fd, void *write_buf, size_t remaining)
{
    size_t done = 0;
    while (done < remaining)
    {
        ssize_t r = write(fd, write_buf, remaining-done);
        if (r < 0)
        {
            if (errno != EINTR && errno != EAGAIN && errno != EPIPE)
            {
                perror("write");
                exit(1);
            }
            continue;
        }
        done += (size_t)r;
        write_buf = (uint8_t*)write_buf + r;
    }
    return done;
}

int main(int narg, char *args[])
{
    int fd1 = -1, fd2 = -1;
    uint8_t *buf1 = NULL, *buf2 = NULL;
    uint64_t addr = 0, l1 = 0, l2 = 0, l = 0, diffl = 0;
    size_t buf1_len = 0, buf2_len = 0, i = 0, j = 0, dl = 0;
    int argoff = 0;
    int nosource = 0;
    fprintf(stderr, "VMX HexDiff v2.1\nLicense: GPLv3.0+, (c) 2005+, Vitaliy Filippov\n");
    argoff = 1;
    if (narg > argoff && strcmp(args[argoff], "-n") == 0)
    {
        nosource = 1;
        argoff++;
    }
    if (narg < argoff+2)
    {
        fprintf(stderr, "USAGE: bindiff [-n] <file1> <file2>\n"
            "This will create hex patch file1->file2 and write it to stdout.\n"
            "[-n] = do not write file1 data in patch, only file2.\n");
        return -1;
    }
    fd1 = open(args[argoff], O_RDONLY);
    if (fd1 < 0)
    {
        fprintf(stderr, "Couldn't open %s: %s\n", args[argoff], strerror(errno));
        return -1;
    }
    fd2 = open(args[argoff+1], O_RDONLY);
    if (fd2 < 0)
    {
        fprintf(stderr, "Couldn't open %s: %s\n", args[argoff+1], strerror(errno));
        close(fd1);
        return -1;
    }
    l1 = filelength(fd1);
    l2 = filelength(fd2);
    if (l1 < l2)
        l = l1;
    else
        l = l2;
    addr = diffl = 0;
    buf1 = malloc(BUFSIZE+1);
    buf2 = malloc(BUFSIZE+1);
    while ((buf1_len = read_blocking(fd1, buf1, BUFSIZE)) > 0 && (buf2_len = read_blocking(fd2, buf2, BUFSIZE)) > 0)
    {
        buf1[buf1_len] = buf2[buf2_len] = 0;
        for (dl = 0, i = 0; i <= buf1_len && i <= buf2_len; i++, addr++)
        {
            if (buf1[i] != buf2[i])
            {
                dl++;
            }
            else if (dl)
            {
                printf("%08jX: ", addr-dl);
                if (!nosource)
                {
                    for (j = i-dl; j < i; j++)
                        printf("%02X", buf1[j]);
                    printf(" ");
                }
                for (j = i-dl; j < i; j++)
                    printf("%02X", buf2[j]);
                printf("\n");
                diffl += dl;
                dl = 0;
            }
        }
        addr--;
    }
    if (l1 < l2)
    {
        printf("%08zX: ", i);
        while ((buf2_len = read_blocking(fd2, buf2, BUFSIZE)) > 0)
        {
            for (j = 0; j < buf2_len; j++, i++)
                printf("%02X", buf2[j]);
        }
        printf("\n");
    }
    else if (l1 > l2)
    {
        printf("SIZE %08zX\n", l2);
    }
    if (diffl != 0 || l1 != l2)
    {
        fprintf(stderr, "Difference in %zu of %zu common bytes\n", diffl, l);
        if (l1 != l2)
            fprintf(stderr, "Length difference!\nFile \"%s\": %zu\nFile \"%s\": %zu\n", args [1], l1, args [2], l2);
    }
    else
    {
        fprintf(stderr, "Files are equal\n");
    }
    free(buf1);
    free(buf2);
    return 0;
}
