#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>

#include <stdexcept>

#include "addr_util.h"
#include "ringloop.h"
#include "epoll_manager.h"
#include "messenger.h"

int main(int narg, char *args[])
{
    if (narg < 2)
    {
        fprintf(stderr, "USAGE: ./test_atomic <device> [--verify]\n");
        return 1;
    }
    uint64_t write_size = 128*1024;
    ring_consumer_t looper;
    ring_loop_t *ringloop = new ring_loop_t(RINGLOOP_DEFAULT_SIZE);
    uint8_t *buf = (uint8_t*)memalign_or_die(4096, write_size);
    memset(buf, 0xA7, write_size);
    int fd = open(args[1], O_DIRECT|O_RDWR|O_EXCL);
    if (fd < 0)
    {
        perror("open");
        return 1;
    }
    bool verify = narg >= 3 && !strcmp(args[2], "--verify");
    fprintf(stderr, "%s %s\n", verify ? "Verifying" : "Writing", args[1]);
    int inflight = 0;
    uint64_t offset = 0;
    uint64_t prev_offset = 0;
    looper.loop = [&]()
    {
        while (inflight < 16 && offset < (uint64_t)50*1000*1024*1024-write_size)
        {
            io_uring_sqe *sqe = ringloop->get_sqe();
            ring_data_t *data = (ring_data_t*)sqe->user_data;
            if (!verify)
            {
                data->iov = { buf, write_size };
                io_uring_prep_writev(sqe, fd, &data->iov, 1, offset);
                data->callback = [&, offset](ring_data_t *data)
                {
                    if (data->res != write_size)
                    {
                        fprintf(stderr, "Error writing at offset %lu: %s (code %d)\n", offset, strerror(-data->res), -data->res);
                        exit(1);
                    }
                    inflight--;
                };
            }
            else
            {
                uint8_t *buf2 = (uint8_t*)memalign_or_die(4096, write_size);
                memset(buf2, 0, write_size);
                data->iov = { buf2, write_size };
                io_uring_prep_readv(sqe, fd, &data->iov, 1, offset);
                data->callback = [&, buf2, offset](ring_data_t *data)
                {
                    if (data->res != write_size)
                    {
                        fprintf(stderr, "Error reading at offset %ju: %s (code %d)\n", offset, strerror(-data->res), -data->res);
                        exit(1);
                    }
                    for (uint64_t i = 0; i < write_size; i++)
                    {
                        if (buf2[i] != buf2[0])
                        {
                            fprintf(stderr, "Atomicity broken at offset %ju+%ju: byte is %02x\n", offset, i, buf2[i]);
                            exit(1);
                        }
                    }
                    free(buf2);
                    inflight--;
                    if (offset >= prev_offset+1024*1024*1024)
                    {
                        fprintf(stderr, "Verified at %ju\n", offset);
                        prev_offset = offset;
                    }
                };
            }
            offset += write_size + 4096;
            inflight++;
        }
        ringloop->submit();
    };
    ringloop->register_consumer(&looper);
    while (true)
    {
        ringloop->loop();
        ringloop->wait();
    }
    close(fd);
    free(buf);
    delete ringloop;
    return 0;
}
