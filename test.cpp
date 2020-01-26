#define _LARGEFILE64_SOURCE
#include <sys/types.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/time.h>
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

#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <map>
#include <vector>
#include <deque>
#include <algorithm>

#include "blockstore.h"
#include "blockstore_impl.h"
#include "osd_peering_pg.h"
//#include "cpp-btree/btree_map.h"

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
        printf("result: %d user_data: %lld -> %lld\n", ret, sqe->user_data, cqe->user_data);
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

int main02(int argc, char *argv[])
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

int main03(int argc, char *argv[])
{
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0), enable = 1;
    assert(listen_fd >= 0);
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable));
    struct sockaddr_in bind_addr;
    assert(inet_pton(AF_INET, "0.0.0.0", &bind_addr.sin_addr) == 1);
    bind_addr.sin_family = AF_INET;
    bind_addr.sin_port = htons(13892);
    int r = bind(listen_fd, (sockaddr*)&bind_addr, sizeof(bind_addr));
    if (r)
    {
        perror("bind");
        return 1;
    }
    assert(listen(listen_fd, 128) == 0);
    struct sockaddr_in peer_addr;
    socklen_t peer_addr_size = sizeof(peer_addr);
    int peer_fd = accept(listen_fd, (sockaddr*)&peer_addr, &peer_addr_size);
    assert(peer_fd >= 0);
    //fcntl(peer_fd, F_SETFL, fcntl(listen_fd, F_GETFL, 0) | O_NONBLOCK);

    struct io_uring ring;
    assert(setup_context(32, &ring) == 0);
    void *buf = memalign(512, 4096*1024);

    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    assert(sqe);
    struct iovec iov = { buf, 4096*1024 };
    struct msghdr msg = { 0 };
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    io_uring_prep_recvmsg(sqe, peer_fd, &msg, 0);
    io_uring_sqe_set_data(sqe, buf);
    io_uring_submit_and_wait(&ring, 1);
    struct io_uring_cqe *cqe;
    io_uring_peek_cqe(&ring, &cqe);
    int ret = cqe->res;
    printf("cqe result: %d\n", ret);
    // ok, io_uring's sendmsg always reads as much data as is available and finishes
    io_uring_cqe_seen(&ring, cqe);
    close(peer_fd);
    close(listen_fd);
    io_uring_queue_exit(&ring);

    return 0;
}

int main04(int argc, char *argv[])
{
    /*spp::sparse_hash_set<obj_ver_id> osd1, osd2;
    // fill takes 18.9 s
    for (int i = 0; i < 1024*1024*8*2; i++)
    {
        obj_ver_id ovid = { { rand() % 500, rand() }, rand() };
        osd1.insert(ovid);
        osd2.insert(ovid);
    }
    for (int i = 0; i < 50000; i++)
    {
        obj_ver_id ovid = { { rand() % 500, rand() }, rand() };
        osd1.insert(ovid);
        ovid = { { rand() % 500, rand() }, rand() };
        osd2.insert(ovid);
    }
    // diff takes only 2.3 s
    spp::sparse_hash_set<obj_ver_id> osd1diff;
    for (obj_ver_id e: osd1)
    {
        auto it = osd2.find(e);
        if (it != osd2.end())
            osd2.erase(it);
        else
            osd1diff.insert(e);
    }*/
    // fill vector takes 2 s
    std::vector<obj_ver_role> to_sort;
    to_sort.resize(1024*1024*8*2*3);
    printf("Filling\n");
    for (int i = 0; i < 1024*1024*8*2*3; i++)
    {
        to_sort[i] = {
            .oid = (object_id){
                .inode = (uint64_t)(rand() % 500),
                .stripe = (uint64_t)rand(),
            },
            .version = (uint64_t)rand(),
            .osd_num = (uint64_t)(rand() % 16),
        };
    }
    printf("Sorting\n");
    // sorting the whole array takes 7 s
    // sorting in 3 parts... almost the same, 6 s
    std::sort(to_sort.begin(), to_sort.end());
    return 0;
}

int main05(int argc, char *argv[])
{
    // FIXME extract this into a test
    pg_t pg = {
        .state = PG_PEERING,
        .pg_num = 1,
        .target_set = { 1, 2, 3 },
        .peering_state = new pg_peering_state_t(),
    };
    pg.peering_state->list_done = 3;
    for (uint64_t osd_num = 1; osd_num <= 3; osd_num++)
    {
        pg_list_result_t r = {
            .buf = (obj_ver_id*)malloc(sizeof(obj_ver_id) * 1024*1024*8),
            .total_count = 1024*1024*8,
            .stable_count = (uint64_t)(1024*1024*8 - (osd_num == 1 ? 10 : 0)),
        };
        for (uint64_t i = 0; i < r.total_count; i++)
        {
            r.buf[i] = {
                .oid = {
                    .inode = 1,
                    .stripe = (i << STRIPE_SHIFT) | (osd_num-1),
                },
                .version = (uint64_t)(osd_num == 1 && i >= r.total_count - 10 ? 2 : 1),
            };
        }
        pg.peering_state->list_results[osd_num] = r;
    }
    pg.calc_object_states();
    printf("deviation variants=%ld clean=%lu\n", pg.state_dict.size(), pg.clean_count);
    for (auto it: pg.state_dict)
    {
        printf("dev: state=%lx\n", it.second.state);
    }
    return 0;
}

int main(int argc, char *argv[])
{
    timeval fill_start, fill_end, filter_end;
    spp::sparse_hash_map<object_id, clean_entry> clean_db;
    //std::map<object_id, clean_entry> clean_db;
    //btree::btree_map<object_id, clean_entry> clean_db;
    gettimeofday(&fill_start, NULL);
    printf("filling\n");
    uint64_t total = 1024*1024*8*4;
    clean_db.resize(total);
    for (uint64_t i = 0; i < total; i++)
    {
        clean_db[(object_id){
            .inode = 1,
            //.stripe = (i << STRIPE_SHIFT),
            .stripe = (((367*i) % total) << STRIPE_SHIFT),
        }] = (clean_entry){
            .version = 1,
            .location = i << DEFAULT_ORDER,
        };
    }
    gettimeofday(&fill_end, NULL);
    // no resize():
    // spp = 17.87s (seq), 41.81s (rand), 3.29s (seq+resize), 8.3s (rand+resize), ~1.3G RAM in all cases
    // std::unordered_map = 6.14 sec, ~2.3G RAM
    // std::map = 13 sec (seq), 5.54 sec (rand), ~2.5G RAM
    // cpp-btree = 2.47 sec (seq) ~1.2G RAM, 20.6 sec (pseudo-random 367*i % total) ~1.5G RAM
    printf("filled %.2f sec\n", (fill_end.tv_sec - fill_start.tv_sec) + (fill_end.tv_usec - fill_start.tv_usec) / 1000000.0);
    for (int pg = 0; pg < 100; pg++)
    {
        obj_ver_id* buf1 = (obj_ver_id*)malloc(sizeof(obj_ver_id) * ((total+99)/100));
        int j = 0;
        for (auto it: clean_db)
            if ((it.first % 100) == pg)
                buf1[j++] = { .oid = it.first, .version = it.second.version };
        free(buf1);
        printf("filtered %d\n", j);
    }
    gettimeofday(&filter_end, NULL);
    // spp = 42.15 sec / 60 sec (rand)
    // std::unordered_map = 43.7 sec
    // std::map = 156.13 sec
    // cpp-btree = 21.87 sec (seq), 44.33 sec (rand)
    printf("100 times filter %.2f sec\n", (filter_end.tv_sec - fill_end.tv_sec) + (filter_end.tv_usec - fill_end.tv_usec) / 1000000.0);
    return 0;
}
