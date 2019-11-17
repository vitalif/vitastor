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
#include <vector>
#include <deque>

#include "blockstore.h"
#include "cpp-btree/btree_map.h"

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
    io_uring_sqe_set_data(sqe, buf);
    io_uring_submit_and_wait(ring, 1);
    struct io_uring_cqe *cqe;
    io_uring_peek_cqe(ring, &cqe);
    int ret = cqe->res;
    //int ret = writev(fd, &iov, 1);
    if (ret < 0)
        printf("cqe failed: %d %s\n", ret, strerror(-ret));
    else
        printf("result: %d user_data: %d -> %d\n", ret, sqe->user_data, cqe->user_data);
    io_uring_cqe_seen(ring, cqe);
    free(buf);
}

class obj_ver_hash
{
public:
    size_t operator()(const obj_ver_id &s) const
    {
        size_t seed = 0;
        spp::hash_combine(seed, s.oid.inode);
        spp::hash_combine(seed, s.oid.stripe);
        spp::hash_combine(seed, s.version);
        return seed;
    }
};

inline bool operator == (const obj_ver_id & a, const obj_ver_id & b)
{
    return a.oid == b.oid && a.version == b.version;
}

int main00(int argc, char *argv[])
{
    // queue with random removal: vector is best :D
    // deque: 8.1s
    // vector: 6.6s
    // list: 9.3s
    for (int i = 0; i < 10000; i++)
    {
        std::list<int> q;
        for (int i = 0; i < 20480; i++)
        {
            for (auto it = q.begin(); it != q.end();)
            {
                if (rand() < RAND_MAX/2)
                {
                    //q.erase(it); -> for deque and vector
                    auto p = it++;
                    q.erase(p);
                }
                else
                    it++;
            }
            q.push_back(rand());
        }
    }
    return 0;
}

int main01(int argc, char *argv[])
{
    // deque: 2.091s
    // vector: 18.733s
    // list: 5.216s
    // good, at least in this test deque is fine
    for (int i = 0; i < 10000; i++)
    {
        std::deque<int> q;
        for (int i = 0; i < 20480; i++)
        {
            int r = rand();
            if (r < RAND_MAX/4 && q.size() > 0)
                q.pop_front();
                //q.erase(q.begin());
            else
                q.push_back(r);
        }
    }
    return 0;
}

int main_vec(int argc, char *argv[])
{
    // vector: 16 elements -> 0.047s, 256 elements -> 1.622s, 1024 elements -> 16.087s, 2048 elements -> 55.8s
    for (int i = 0; i < 100000; i++)
    {
        std::vector<iovec> v;
        for (int i = 0; i < 2048; i++)
        {
            int r = rand();
            int n = 0;
            auto it = v.begin();
            for (; it != v.end(); it++)
                if (it->iov_len >= r)
                    break;
            v.insert(it, (iovec){ .iov_base = 0, .iov_len = (size_t)r });
        }
    }
    return 0;
}

int main_map(int argc, char *argv[])
{
    // map: 16 elements -> 0.105s, 256 elements -> 2.634s, 1024 elements -> 12.55s, 2048 elements -> 27.475s
    // conclustion: vector is better in fulfill_read
    for (int i = 0; i < 100000; i++)
    {
        std::map<int,iovec> v;
        for (int i = 0; i < 2048; i++)
        {
            int r = rand();
            v[r] = (iovec){ .iov_base = 0, .iov_len = (size_t)r };
        }
    }
    return 0;
}

int main0(int argc, char *argv[])
{
    // std::map 5M entries monotone -> 2.115s, random -> 8.782s
    // btree_map 5M entries monotone -> 0.458s, random -> 5.429s
    // absl::btree_map 5M entries random -> 5.09s
    // sparse_hash_map 5M entries -> 2.193s, random -> 2.586s
    //btree::btree_map<obj_ver_id, dirty_entry> dirty_db;
    //std::map<obj_ver_id, dirty_entry> dirty_db;
    spp::sparse_hash_map<obj_ver_id, dirty_entry, obj_ver_hash> dirty_db;
    for (int i = 0; i < 5000000; i++)
    {
        dirty_db[(obj_ver_id){
            .oid = (object_id){
                .inode = (uint64_t)rand(),
                .stripe = (uint64_t)i,
            },
            .version = 1,
        }] = (dirty_entry){
            .state = ST_D_META_SYNCED,
            .flags = 0,
            .location = (uint64_t)i << 17,
            .offset = 0,
            .len = 1 << 17,
        };
    }
    return 0;
}

int main1(int argc, char *argv[])
{
    std::vector<uint64_t> v1, v2;
    v1.reserve(10000);
    v2.reserve(10000);
    for (int i = 0; i < 10000; i++)
    {
        v1.push_back(i);
        v2.push_back(i);
    }
    for (int i = 0; i < 100000; i++)
    {
        // haha (core i5-2500 | i7-6800HQ)
        // vector 10000 items: 4.37/100000 | 3.66
        // vector 100000 items: 9.68/10000 | 0.95
        // deque 10000 items: 28.432/100000
        // list 10000 items: 320.695/100000
        std::vector<uint64_t> v3;
        v3.insert(v3.end(), v1.begin(), v1.end());
        v3.insert(v3.end(), v2.begin(), v2.end());
    }
    return 0;
}

int main(int argc, char *argv[])
{
    std::map<int, std::string> strs;
    strs.emplace(12, "str");
    auto it = strs.upper_bound(13);
    //printf("s = %d %s %d\n", it->first, it->second.c_str(), it == strs.begin());
    it--;
    printf("%d\n", it == strs.end());
    //printf("s = %d %s\n", it->first, it->second.c_str());
    struct io_uring ring;
    int fd = open("/dev/loop0", O_RDWR | O_DIRECT, 0644);
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
