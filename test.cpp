// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.0 (see README.md for details)

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
#include <math.h>

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
    btree::btree_map<obj_ver_id, dirty_entry> dirty_db;
    //std::map<obj_ver_id, dirty_entry> dirty_db;
    //spp::sparse_hash_map<obj_ver_id, dirty_entry, obj_ver_hash> dirty_db;
    for (int i = 0; i < 5000000; i++)
    {
        dirty_db[(obj_ver_id){
            .oid = (object_id){
                .inode = (uint64_t)rand(),
                .stripe = (uint64_t)i,
            },
            .version = 1,
        }] = (dirty_entry){
            .state = ST_D_SYNCED,
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

uint64_t jumphash(uint64_t key, int count)
{
    uint64_t b = 0;
    uint64_t seed = key;
    for (int j = 1; j < count; j++)
    {
        seed = 2862933555777941757ull*seed + 3037000493ull; // LCPRNG
        if (seed < (UINT64_MAX / (j+1)))
        {
            b = j;
        }
    }
    return b;
}

void jumphash_prepare(int count, uint64_t *out_weights, uint64_t *in_weights)
{
    if (count <= 0)
    {
        return;
    }
    uint64_t total_weight = in_weights[0];
    out_weights[0] = UINT64_MAX;
    for (int j = 1; j < count; j++)
    {
        total_weight += in_weights[j];
        out_weights[j] = UINT64_MAX / total_weight * in_weights[j];
    }
}

uint64_t jumphash_weights(uint64_t key, int count, uint64_t *prepared_weights)
{
    uint64_t b = 0;
    uint64_t seed = key;
    for (int j = 1; j < count; j++)
    {
        seed = 2862933555777941757ull*seed + 3037000493ull; // LCPRNG
        if (seed < prepared_weights[j])
        {
            b = j;
        }
    }
    return b;
}

void jumphash3(uint64_t key, int count, uint64_t *weights, uint64_t *r)
{
    r[0] = 0;
    r[1] = 1;
    r[2] = 2;
    uint64_t total_weight = weights[0]+weights[1]+weights[2];
    uint64_t seed = key;
    for (int j = 3; j < count; j++)
    {
        seed = 2862933555777941757ull*seed + 3037000493ull; // LCPRNG
        total_weight += weights[j];
        if (seed < UINT64_MAX*1.0*weights[j]/total_weight)
            r[0] = j;
        else
        {
            seed = 2862933555777941757ull*seed + 3037000493ull; // LCPRNG
            if (seed < UINT64_MAX*1.0*weights[j]/total_weight)
                r[1] = j;
            else
            {
                seed = 2862933555777941757ull*seed + 3037000493ull; // LCPRNG
                if (seed < UINT64_MAX*1.0*weights[j]/total_weight)
                    r[2] = j;
            }
        }
    }
}

uint64_t crush(uint64_t key, int count, uint64_t *weights)
{
    uint64_t b = 0;
    uint64_t seed = 0;
    uint64_t max = 0;
    for (int j = 0; j < count; j++)
    {
        seed = (key + 0xc6a4a7935bd1e995 + (seed << 6) + (seed >> 2));
        seed ^= (j + 0xc6a4a7935bd1e995 + (seed << 6) + (seed >> 2));
        seed = 2862933555777941757ull*seed + 3037000493ull; // LCPRNG
        seed = -log(((double)seed) / (1ul << 32) / (1ul << 32)) * weights[j];
        if (seed > max)
        {
            max = seed;
            b = j;
        }
    }
    return b;
}

void crush3(uint64_t key, int count, uint64_t *weights, uint64_t *r, uint64_t total_weight)
{
    uint64_t seed = 0;
    uint64_t max = 0;
    for (int k1 = 0; k1 < count; k1++)
    {
        for (int k2 = k1+1; k2 < count; k2++)
        {
            if (k2 == k1)
            {
                continue;
            }
            for (int k3 = k2+1; k3 < count; k3++)
            {
                if (k3 == k1 || k3 == k2)
                {
                    continue;
                }
                seed = (key + 0xc6a4a7935bd1e995 + (seed << 6) + (seed >> 2));
                seed ^= (k1 + 0xc6a4a7935bd1e995 + (seed << 6) + (seed >> 2));
                seed ^= (k2 + 0xc6a4a7935bd1e995 + (seed << 6) + (seed >> 2));
                seed ^= (k3 + 0xc6a4a7935bd1e995 + (seed << 6) + (seed >> 2));
                seed = 2862933555777941757ull*seed + 3037000493ull; // LCPRNG
                //seed = ((double)seed) / (1ul << 32) / (1ul << 32) * (weights[k1] + weights[k2] + weights[k3]);
                seed = ((double)seed) / (1ul << 32) / (1ul << 32) * (1 -
                    (1 - 1.0*weights[k1]/total_weight)*
                    (1 - 1.0*weights[k2]/total_weight)*
                    (1 - 1.0*weights[k3]/total_weight)
                ) * UINT64_MAX;
                if (seed > max)
                {
                    r[0] = k1;
                    r[1] = k2;
                    r[2] = k3;
                    max = seed;
                }
            }
        }
    }
}

int main(int argc, char *argv[])
{
    int host_count = 6;
    uint64_t host_weights[] = {
        34609*3,
        34931*3,
        35850+36387+35859,
        36387,
        36387*2,
        36387,
    };
    /*int osd_count[] = { 3, 3, 3, 1, 2 };
    uint64_t osd_weights[][3] = {
        { 34609, 34609, 34609 },
        { 34931, 34931, 34931 },
        { 35850, 36387, 35859 },
        { 36387 },
        { 36387, 36387 },
    };*/
    uint64_t total_weight = 0;
    for (int i = 0; i < host_count; i++)
    {
        total_weight += host_weights[i];
    }
    uint64_t host_weights_prepared[host_count];
    jumphash_prepare(host_count, host_weights_prepared, host_weights);
    uint64_t total_pgs[host_count] = { 0 };
    int pg_count = 256;
    double uniformity[pg_count] = { 0 };
    for (uint64_t pg = 1; pg <= pg_count; pg++)
    {
        uint64_t r[3];

/*
        // Select first host
        //r[0] = jumphash_weights(pg, host_count, host_weights_prepared);
        r[0] = crush(pg, host_count, host_weights);
        // Select second host
        uint64_t seed = pg;
        r[1] = r[0];
        while (r[1] == r[0])
        {
            seed = 2862933555777941757ull*seed + 3037000493ull; // LCPRNG
            //r[1] = jumphash_weights(seed, host_count, host_weights_prepared);
            r[1] = crush(seed, host_count, host_weights);
        }
        // Select third host
        seed = pg;
        r[2] = r[0];
        while (r[2] == r[0] || r[2] == r[1])
        {
            seed = 2862933555777941757ull*seed + 3037000493ull; // LCPRNG
            //r[2] = jumphash_weights(seed, host_count, host_weights_prepared);
            r[2] = crush(seed, host_count, host_weights);
        }
*/

/*
        // Select second host
        uint64_t host_weights1[host_count];
        for (int i = 0; i < r[0]; i++)
            host_weights1[i] = host_weights[i];
        for (int i = r[0]+1; i < host_count; i++)
            host_weights1[i-1] = host_weights[i];
        r[1] = crush(pg, host_count-1, host_weights1);
        // Select third host
        for (int i = r[1]+1; i < host_count-1; i++)
            host_weights1[i-1] = host_weights[i];
        r[2] = crush(pg, host_count-2, host_weights1);
        // Transform numbers
        r[2] = r[2] >= r[1] ? 1+r[2] : r[2];
        r[2] = r[2] >= r[0] ? 1+r[2] : r[2];
        r[1] = r[1] >= r[0] ? 1+r[1] : r[1];
*/

        crush3(pg, host_count, host_weights, r, total_weight);
        uint64_t shift = (2862933555777941757ull*pg + 3037000493ull) % host_count;
        if (shift == 1)
        {
            uint64_t tmp;
            tmp = r[0];
            r[0] = r[1];
            r[1] = r[2];
            r[2] = tmp;
        }
        else if (shift == 2)
        {
            uint64_t tmp;
            tmp = r[0];
            r[0] = r[2];
            r[2] = r[1];
            r[1] = tmp;
        }

        total_pgs[r[0]]++;
        total_pgs[r[1]]++;
        total_pgs[r[2]]++;

        double u = 0;
        for (int i = 0; i < host_count; i++)
        {
            double d = abs(1 - total_pgs[i]/3.0/pg * total_weight/host_weights[i]);
            u += d;
        }
        uniformity[pg-1] = u/host_count;

        printf("pg %lu: hosts %lu, %lu, %lu ; avg deviation = %.2f\n", pg, r[0], r[1], r[2], u/host_count);
    }
    printf("total PGs: ");
    for (int i = 0; i < host_count; i++)
    {
        printf(i > 0 ? ", %lu (%.2f)" : "%lu (%.2f)", total_pgs[i], total_pgs[i]/3.0/pg_count * total_weight/host_weights[i]);
    }
    printf("\n");
    return 0;
}
