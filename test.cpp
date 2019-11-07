#define _LARGEFILE64_SOURCE
#include <sys/types.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdint.h>
#include <malloc.h>
#include <linux/fs.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <stdio.h>
#include <liburing.h>

#include <map>

static int setup_context(unsigned entries, struct io_uring *ring)
{
    int ret = io_uring_queue_init(entries, ring, 0);
    if (ret < 0)
    {
        fprintf(stderr, "queue_init: %s\n", strerror(-ret));
        return -1;
    }
    return 0;
}

static void test_write(struct io_uring *ring, int fd)
{
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    assert(sqe);
    uint8_t *buf = (uint8_t*)memalign(512, 1024*1024*1024);
    struct iovec iov = { buf, 1024*1024*1024 };
    io_uring_prep_writev(sqe, fd, &iov, 1, 0);
    io_uring_sqe_set_data(sqe, 0);
    io_uring_submit_and_wait(ring, 1);
    struct io_uring_cqe *cqe;
    io_uring_peek_cqe(ring, &cqe);
    int ret = cqe->res;
    //int ret = writev(fd, &iov, 1);
    if (ret < 0)
        printf("cqe failed: %d %s\n", ret, strerror(-ret));
    else
        printf("result: %d\n", ret);
    io_uring_cqe_seen(ring, cqe);
    free(buf);
}

int main(int argc, char *argv[])
{
    std::map<int, std::string> strs;
    strs.emplace(12, "str");
    auto it = strs.upper_bound(11);
    printf("s = %d %s %d\n", it->first, it->second.c_str(), it == strs.begin());
    it--;
    printf("s = %d %s\n", it->first, it->second.c_str());
    struct io_uring ring;
    int fd = open("testfile", O_RDWR | O_DIRECT, 0644);
    if (fd < 0)
    {
        perror("open infile");
        return 1;
    }
    if (setup_context(32, &ring))
        return 1;
    test_write(&ring, fd);
    close(fd);
    io_uring_queue_exit(&ring);
    return 0;
}
