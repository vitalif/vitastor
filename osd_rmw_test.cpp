// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.0 (see README.md for details)

#include <string.h>
#include "osd_rmw.cpp"
#include "test_pattern.h"

void dump_stripes(osd_rmw_stripe_t *stripes, int pg_size);
void test1();
void test4();
void test5();
void test6();
void test7();
void test8();
void test9();
void test10();
void test11();

/***

Cases:

1. split(offset=128K-4K, len=8K)
   = [ [ 128K-4K, 128K ], [ 0, 4K ], [ 0, 0 ] ]

2. read(offset=128K-4K, len=8K, osd_set=[1,0,3])
   = { read: [ [ 0, 128K ], [ 0, 4K ], [ 0, 4K ] ] }

3. cover_read(0, 128K, { req: [ 128K-4K, 4K ] })
   = { read: [ 0, 128K-4K ] }

4. write(offset=128K-4K, len=8K, osd_set=[1,0,3])
   = {
     read: [ [ 0, 128K ], [ 4K, 128K ], [ 4K, 128K ] ],
     write: [ [ 128K-4K, 128K ], [ 0, 4K ], [ 0, 128K ] ],
     input buffer: [ write0, write1 ],
     rmw buffer: [ write2, read0, read1, read2 ],
   }
   + check write2 buffer

5. write(offset=0, len=128K+64K, osd_set=[1,0,3])
   = {
     req: [ [ 0, 128K ], [ 0, 64K ], [ 0, 0 ] ],
     read: [ [ 64K, 128K ], [ 64K, 128K ], [ 64K, 128K ] ],
     write: [ [ 0, 128K ], [ 0, 64K ], [ 0, 128K ] ],
     input buffer: [ write0, write1 ],
     rmw buffer: [ write2, read0, read1, read2 ],
   }

6. write(offset=0, len=128K+64K, osd_set=[1,2,3])
   = {
     req: [ [ 0, 128K ], [ 0, 64K ], [ 0, 0 ] ],
     read: [ [ 0, 0 ], [ 64K, 128K ], [ 0, 0 ] ],
     write: [ [ 0, 128K ], [ 0, 64K ], [ 0, 128K ] ],
     input buffer: [ write0, write1 ],
     rmw buffer: [ write2, read1 ],
   }

7. calc_rmw(offset=128K-4K, len=8K, osd_set=[1,0,3], write_set=[1,2,3])
   = {
     read: [ [ 0, 128K ], [ 0, 128K ], [ 0, 128K ] ],
     write: [ [ 128K-4K, 128K ], [ 0, 4K ], [ 0, 128K ] ],
     input buffer: [ write0, write1 ],
     rmw buffer: [ write2, read0, read1, read2 ],
   }
   then, after calc_rmw_parity_xor(): {
     write: [ [ 128K-4K, 128K ], [ 0, 128K ], [ 0, 128K ] ],
     write1==read1,
   }
   + check write1 buffer
   + check write2 buffer

8. calc_rmw(offset=0, len=128K+4K, osd_set=[0,2,3], write_set=[1,2,3])
   = {
     read: [ [ 0, 0 ], [ 4K, 128K ], [ 0, 0 ] ],
     write: [ [ 0, 128K ], [ 0, 4K ], [ 0, 128K ] ],
     input buffer: [ write0, write1 ],
     rmw buffer: [ write2, read1 ],
   }
   + check write2 buffer

9. object recovery case:
   calc_rmw(offset=0, len=0, read_osd_set=[0,2,3], write_osd_set=[1,2,3])
   = {
     read: [ [ 0, 128K ], [ 0, 128K ], [ 0, 128K ] ],
     write: [ [ 0, 0 ], [ 0, 0 ], [ 0, 0 ] ],
     input buffer: NULL,
     rmw buffer: [ read0, read1, read2 ],
   }
   then, after calc_rmw_parity_xor(): {
     write: [ [ 0, 128K ], [ 0, 0 ], [ 0, 0 ] ],
     write0==read0,
   }
   + check write0 buffer

10. full overwrite/recovery case:
   calc_rmw(offset=0, len=256K, read_osd_set=[1,0,0], write_osd_set=[1,2,3])
   = {
     read: [ [ 0, 0 ], [ 0, 0 ], [ 0, 0 ] ],
     write: [ [ 0, 128K ], [ 0, 128K ], [ 0, 128K ] ],
     input buffer: [ write0, write1 ],
     rmw buffer: [ write2 ],
   }
   then, after calc_rmw_parity_xor(): all the same
   + check write2 buffer

10. partial recovery case:
   calc_rmw(offset=128K, len=128K, read_osd_set=[1,0,0], write_osd_set=[1,2,3])
   = {
     read: [ [ 0, 128K ], [ 0, 0 ], [ 0, 0 ] ],
     write: [ [ 0, 0 ], [ 0, 128K ], [ 0, 128K ] ],
     input buffer: [ write1 ],
     rmw buffer: [ write2, read0 ],
   }
   then, after calc_rmw_parity_xor(): all the same
   + check write2 buffer

***/

int main(int narg, char *args[])
{
    // Test 1
    test1();
    // Test 4
    test4();
    // Test 5
    test5();
    // Test 6
    test6();
    // Test 7
    test7();
    // Test 8
    test8();
    // Test 9
    test9();
    // Test 10
    test10();
    // Test 11
    test11();
    // End
    printf("all ok\n");
    return 0;
}

void dump_stripes(osd_rmw_stripe_t *stripes, int pg_size)
{
    printf("request");
    for (int i = 0; i < pg_size; i++)
    {
        printf(" {%uK-%uK}", stripes[i].req_start/1024, stripes[i].req_end/1024);
    }
    printf("\n");
    printf("read");
    for (int i = 0; i < pg_size; i++)
    {
        printf(" {%uK-%uK}", stripes[i].read_start/1024, stripes[i].read_end/1024);
    }
    printf("\n");
    printf("write");
    for (int i = 0; i < pg_size; i++)
    {
        printf(" {%uK-%uK}", stripes[i].write_start/1024, stripes[i].write_end/1024);
    }
    printf("\n");
}

void test1()
{
    osd_num_t osd_set[3] = { 1, 0, 3 };
    osd_rmw_stripe_t stripes[3] = { 0 };
    // Test 1.1
    split_stripes(2, 128*1024, 128*1024-4096, 8192, stripes);
    assert(stripes[0].req_start == 128*1024-4096 && stripes[0].req_end == 128*1024);
    assert(stripes[1].req_start == 0 && stripes[1].req_end == 4096);
    assert(stripes[2].req_end == 0);
    // Test 1.2
    for (int i = 0; i < 3; i++)
    {
        stripes[i].read_start = stripes[i].req_start;
        stripes[i].read_end = stripes[i].req_end;
    }
    assert(extend_missing_stripes(stripes, osd_set, 2, 3) == 0);
    assert(stripes[0].read_start == 0 && stripes[0].read_end == 128*1024);
    assert(stripes[2].read_start == 0 && stripes[2].read_end == 4096);
    // Test 1.3
    stripes[0] = { .req_start = 128*1024-4096, .req_end = 128*1024 };
    cover_read(0, 128*1024, stripes[0]);
    assert(stripes[0].read_start == 0 && stripes[0].read_end == 128*1024-4096);
}

void test4()
{
    osd_num_t osd_set[3] = { 1, 0, 3 };
    osd_rmw_stripe_t stripes[3] = { 0 };
    // Test 4.1
    split_stripes(2, 128*1024, 128*1024-4096, 8192, stripes);
    void* write_buf = malloc(8192);
    void* rmw_buf = calc_rmw(write_buf, stripes, osd_set, 3, 2, 2, osd_set, 128*1024);
    assert(stripes[0].read_start == 0 && stripes[0].read_end == 128*1024);
    assert(stripes[1].read_start == 4096 && stripes[1].read_end == 128*1024);
    assert(stripes[2].read_start == 4096 && stripes[2].read_end == 128*1024);
    assert(stripes[0].write_start == 128*1024-4096 && stripes[0].write_end == 128*1024);
    assert(stripes[1].write_start == 0 && stripes[1].write_end == 4096);
    assert(stripes[2].write_start == 0 && stripes[2].write_end == 128*1024);
    assert(stripes[0].read_buf == rmw_buf+128*1024);
    assert(stripes[1].read_buf == rmw_buf+128*1024*2);
    assert(stripes[2].read_buf == rmw_buf+128*1024*3-4096);
    assert(stripes[0].write_buf == write_buf);
    assert(stripes[1].write_buf == write_buf+4096);
    assert(stripes[2].write_buf == rmw_buf);
    // Test 4.2
    set_pattern(write_buf, 8192, PATTERN0);
    set_pattern(stripes[0].read_buf, 128*1024, PATTERN1); // old data
    set_pattern(stripes[1].read_buf, 128*1024-4096, UINT64_MAX); // didn't read it, it's missing
    set_pattern(stripes[2].read_buf, 128*1024-4096, 0); // old parity = 0
    calc_rmw_parity_xor(stripes, 3, osd_set, osd_set, 128*1024);
    check_pattern(stripes[2].write_buf, 4096, PATTERN0^PATTERN1); // new parity
    check_pattern(stripes[2].write_buf+4096, 128*1024-4096*2, 0); // new parity
    check_pattern(stripes[2].write_buf+128*1024-4096, 4096, PATTERN0^PATTERN1); // new parity
    free(rmw_buf);
    free(write_buf);
}

void test5()
{
    osd_num_t osd_set[3] = { 1, 0, 3 };
    osd_rmw_stripe_t stripes[3] = { 0 };
    // Test 5.1
    split_stripes(2, 128*1024, 0, 64*1024*3, stripes);
    assert(stripes[0].req_start == 0 && stripes[0].req_end == 128*1024);
    assert(stripes[1].req_start == 0 && stripes[1].req_end == 64*1024);
    assert(stripes[2].req_end == 0);
    // Test 5.2
    void *write_buf = malloc(64*1024*3);
    void *rmw_buf = calc_rmw(write_buf, stripes, osd_set, 3, 2, 2, osd_set, 128*1024);
    assert(stripes[0].read_start == 64*1024 && stripes[0].read_end == 128*1024);
    assert(stripes[1].read_start == 64*1024 && stripes[1].read_end == 128*1024);
    assert(stripes[2].read_start == 64*1024 && stripes[2].read_end == 128*1024);
    assert(stripes[0].write_start == 0 && stripes[0].write_end == 128*1024);
    assert(stripes[1].write_start == 0 && stripes[1].write_end == 64*1024);
    assert(stripes[2].write_start == 0 && stripes[2].write_end == 128*1024);
    assert(stripes[0].read_buf == rmw_buf+128*1024);
    assert(stripes[1].read_buf == rmw_buf+64*3*1024);
    assert(stripes[2].read_buf == rmw_buf+64*4*1024);
    assert(stripes[0].write_buf == write_buf);
    assert(stripes[1].write_buf == write_buf+128*1024);
    assert(stripes[2].write_buf == rmw_buf);
    free(rmw_buf);
    free(write_buf);
}

void test6()
{
    osd_num_t osd_set[3] = { 1, 2, 3 };
    osd_rmw_stripe_t stripes[3] = { 0 };
    // Test 6.1
    split_stripes(2, 128*1024, 0, 64*1024*3, stripes);
    void *write_buf = malloc(64*1024*3);
    void *rmw_buf = calc_rmw(write_buf, stripes, osd_set, 3, 2, 3, osd_set, 128*1024);
    assert(stripes[0].read_end == 0);
    assert(stripes[1].read_start == 64*1024 && stripes[1].read_end == 128*1024);
    assert(stripes[2].read_end == 0);
    assert(stripes[0].write_start == 0 && stripes[0].write_end == 128*1024);
    assert(stripes[1].write_start == 0 && stripes[1].write_end == 64*1024);
    assert(stripes[2].write_start == 0 && stripes[2].write_end == 128*1024);
    assert(stripes[0].read_buf == 0);
    assert(stripes[1].read_buf == rmw_buf+128*1024);
    assert(stripes[2].read_buf == 0);
    assert(stripes[0].write_buf == write_buf);
    assert(stripes[1].write_buf == write_buf+128*1024);
    assert(stripes[2].write_buf == rmw_buf);
    free(rmw_buf);
    free(write_buf);
}

void test7()
{
    osd_num_t osd_set[3] = { 1, 0, 3 };
    osd_num_t write_osd_set[3] = { 1, 2, 3 };
    osd_rmw_stripe_t stripes[3] = { 0 };
    // Test 7.1
    split_stripes(2, 128*1024, 128*1024-4096, 8192, stripes);
    void *write_buf = malloc(8192);
    void *rmw_buf = calc_rmw(write_buf, stripes, osd_set, 3, 2, 2, write_osd_set, 128*1024);
    assert(stripes[0].read_start == 0 && stripes[0].read_end == 128*1024);
    assert(stripes[1].read_start == 0 && stripes[1].read_end == 128*1024);
    assert(stripes[2].read_start == 0 && stripes[2].read_end == 128*1024);
    assert(stripes[0].write_start == 128*1024-4096 && stripes[0].write_end == 128*1024);
    assert(stripes[1].write_start == 0 && stripes[1].write_end == 4096);
    assert(stripes[2].write_start == 0 && stripes[2].write_end == 128*1024);
    assert(stripes[0].read_buf == rmw_buf+128*1024);
    assert(stripes[1].read_buf == rmw_buf+128*1024*2);
    assert(stripes[2].read_buf == rmw_buf+128*1024*3);
    assert(stripes[0].write_buf == write_buf);
    assert(stripes[1].write_buf == write_buf+4096);
    assert(stripes[2].write_buf == rmw_buf);
    // Test 7.2
    set_pattern(write_buf, 8192, PATTERN0);
    set_pattern(stripes[0].read_buf, 128*1024, PATTERN1); // old data
    set_pattern(stripes[1].read_buf, 128*1024, UINT64_MAX); // didn't read it, it's missing
    set_pattern(stripes[2].read_buf, 128*1024, 0); // old parity = 0
    calc_rmw_parity_xor(stripes, 3, osd_set, write_osd_set, 128*1024);
    assert(stripes[0].write_start == 128*1024-4096 && stripes[0].write_end == 128*1024);
    assert(stripes[1].write_start == 0 && stripes[1].write_end == 128*1024);
    assert(stripes[2].write_start == 0 && stripes[2].write_end == 128*1024);
    assert(stripes[1].write_buf == stripes[1].read_buf);
    check_pattern(stripes[1].write_buf, 4096, PATTERN0);
    check_pattern(stripes[1].write_buf+4096, 128*1024-4096, PATTERN1);
    check_pattern(stripes[2].write_buf, 4096, PATTERN0^PATTERN1); // new parity
    check_pattern(stripes[2].write_buf+4096, 128*1024-4096*2, 0); // new parity
    check_pattern(stripes[2].write_buf+128*1024-4096, 4096, PATTERN0^PATTERN1); // new parity
    free(rmw_buf);
    free(write_buf);
}

void test8()
{
    osd_num_t osd_set[3] = { 0, 2, 3 };
    osd_num_t write_osd_set[3] = { 1, 2, 3 };
    osd_rmw_stripe_t stripes[3] = { 0 };
    // Test 8.1
    split_stripes(2, 128*1024, 0, 128*1024+4096, stripes);
    void *write_buf = malloc(128*1024+4096);
    void *rmw_buf = calc_rmw(write_buf, stripes, osd_set, 3, 2, 2, write_osd_set, 128*1024);
    assert(stripes[0].read_start == 0 && stripes[0].read_end == 0);
    assert(stripes[1].read_start == 4096 && stripes[1].read_end == 128*1024);
    assert(stripes[2].read_start == 0 && stripes[2].read_end == 0);
    assert(stripes[0].write_start == 0 && stripes[0].write_end == 128*1024);
    assert(stripes[1].write_start == 0 && stripes[1].write_end == 4096);
    assert(stripes[2].write_start == 0 && stripes[2].write_end == 128*1024);
    assert(stripes[0].read_buf == NULL);
    assert(stripes[1].read_buf == rmw_buf+128*1024);
    assert(stripes[2].read_buf == NULL);
    assert(stripes[0].write_buf == write_buf);
    assert(stripes[1].write_buf == write_buf+128*1024);
    assert(stripes[2].write_buf == rmw_buf);
    // Test 8.2
    set_pattern(write_buf, 128*1024+4096, PATTERN0);
    set_pattern(stripes[1].read_buf, 128*1024-4096, PATTERN1);
    calc_rmw_parity_xor(stripes, 3, osd_set, write_osd_set, 128*1024);
    assert(stripes[0].write_start == 0 && stripes[0].write_end == 128*1024); // recheck again
    assert(stripes[1].write_start == 0 && stripes[1].write_end == 4096);     // recheck again
    assert(stripes[2].write_start == 0 && stripes[2].write_end == 128*1024); // recheck again
    assert(stripes[0].write_buf == write_buf);                               // recheck again
    assert(stripes[1].write_buf == write_buf+128*1024);                      // recheck again
    assert(stripes[2].write_buf == rmw_buf);                                 // recheck again
    check_pattern(stripes[2].write_buf, 4096, 0); // new parity
    check_pattern(stripes[2].write_buf+4096, 128*1024-4096, PATTERN0^PATTERN1); // new parity
    free(rmw_buf);
    free(write_buf);
}

void test9()
{
    osd_num_t osd_set[3] = { 0, 2, 3 };
    osd_num_t write_osd_set[3] = { 1, 2, 3 };
    osd_rmw_stripe_t stripes[3] = { 0 };
    // Test 9.0
    split_stripes(2, 128*1024, 64*1024, 0, stripes);
    assert(stripes[0].req_start == 0 && stripes[0].req_end == 0);
    assert(stripes[1].req_start == 0 && stripes[1].req_end == 0);
    assert(stripes[2].req_start == 0 && stripes[2].req_end == 0);
    // Test 9.1
    void *write_buf = NULL;
    void *rmw_buf = calc_rmw(write_buf, stripes, osd_set, 3, 2, 3, write_osd_set, 128*1024);
    assert(stripes[0].read_start == 0 && stripes[0].read_end == 128*1024);
    assert(stripes[1].read_start == 0 && stripes[1].read_end == 128*1024);
    assert(stripes[2].read_start == 0 && stripes[2].read_end == 128*1024);
    assert(stripes[0].write_start == 0 && stripes[0].write_end == 0);
    assert(stripes[1].write_start == 0 && stripes[1].write_end == 0);
    assert(stripes[2].write_start == 0 && stripes[2].write_end == 0);
    assert(stripes[0].read_buf == rmw_buf);
    assert(stripes[1].read_buf == rmw_buf+128*1024);
    assert(stripes[2].read_buf == rmw_buf+128*1024*2);
    assert(stripes[0].write_buf == NULL);
    assert(stripes[1].write_buf == NULL);
    assert(stripes[2].write_buf == NULL);
    // Test 9.2
    set_pattern(stripes[1].read_buf, 128*1024, 0);
    set_pattern(stripes[2].read_buf, 128*1024, PATTERN1);
    calc_rmw_parity_xor(stripes, 3, osd_set, write_osd_set, 128*1024);
    assert(stripes[0].write_start == 0 && stripes[0].write_end == 128*1024);
    assert(stripes[1].write_start == 0 && stripes[1].write_end == 0);
    assert(stripes[2].write_start == 0 && stripes[2].write_end == 0);
    assert(stripes[0].write_buf == rmw_buf);
    assert(stripes[1].write_buf == NULL);
    assert(stripes[2].write_buf == NULL);
    check_pattern(stripes[0].read_buf, 128*1024, PATTERN1);
    check_pattern(stripes[0].write_buf, 128*1024, PATTERN1);
    free(rmw_buf);
}

void test10()
{
    osd_num_t osd_set[3] = { 1, 0, 0 };
    osd_num_t write_osd_set[3] = { 1, 2, 3 };
    osd_rmw_stripe_t stripes[3] = { 0 };
    // Test 10.0
    split_stripes(2, 128*1024, 0, 256*1024, stripes);
    assert(stripes[0].req_start == 0 && stripes[0].req_end == 128*1024);
    assert(stripes[1].req_start == 0 && stripes[1].req_end == 128*1024);
    assert(stripes[2].req_start == 0 && stripes[2].req_end == 0);
    // Test 10.1
    void *write_buf = malloc(256*1024);
    void *rmw_buf = calc_rmw(write_buf, stripes, osd_set, 3, 2, 3, write_osd_set, 128*1024);
    assert(rmw_buf);
    assert(stripes[0].read_start == 0 && stripes[0].read_end == 0);
    assert(stripes[1].read_start == 0 && stripes[1].read_end == 0);
    assert(stripes[2].read_start == 0 && stripes[2].read_end == 0);
    assert(stripes[0].write_start == 0 && stripes[0].write_end == 128*1024);
    assert(stripes[1].write_start == 0 && stripes[1].write_end == 128*1024);
    assert(stripes[2].write_start == 0 && stripes[2].write_end == 128*1024);
    assert(stripes[0].read_buf == NULL);
    assert(stripes[1].read_buf == NULL);
    assert(stripes[2].read_buf == NULL);
    assert(stripes[0].write_buf == write_buf);
    assert(stripes[1].write_buf == write_buf+128*1024);
    assert(stripes[2].write_buf == rmw_buf);
    // Test 10.2
    set_pattern(stripes[0].write_buf, 128*1024, PATTERN1);
    set_pattern(stripes[1].write_buf, 128*1024, PATTERN2);
    calc_rmw_parity_xor(stripes, 3, osd_set, write_osd_set, 128*1024);
    assert(stripes[0].write_start == 0 && stripes[0].write_end == 128*1024);
    assert(stripes[1].write_start == 0 && stripes[1].write_end == 128*1024);
    assert(stripes[2].write_start == 0 && stripes[2].write_end == 128*1024);
    assert(stripes[0].write_buf == write_buf);
    assert(stripes[1].write_buf == write_buf+128*1024);
    assert(stripes[2].write_buf == rmw_buf);
    check_pattern(stripes[2].write_buf, 128*1024, PATTERN1^PATTERN2);
    free(rmw_buf);
    free(write_buf);
}

void test11()
{
    osd_num_t osd_set[3] = { 1, 0, 0 };
    osd_num_t write_osd_set[3] = { 1, 2, 3 };
    osd_rmw_stripe_t stripes[3] = { 0 };
    // Test 11.0
    split_stripes(2, 128*1024, 128*1024, 256*1024, stripes);
    assert(stripes[0].req_start == 0 && stripes[0].req_end == 0);
    assert(stripes[1].req_start == 0 && stripes[1].req_end == 128*1024);
    assert(stripes[2].req_start == 0 && stripes[2].req_end == 0);
    // Test 11.1
    void *write_buf = malloc(256*1024);
    void *rmw_buf = calc_rmw(write_buf, stripes, osd_set, 3, 2, 3, write_osd_set, 128*1024);
    assert(rmw_buf);
    assert(stripes[0].read_start == 0 && stripes[0].read_end == 128*1024);
    assert(stripes[1].read_start == 0 && stripes[1].read_end == 0);
    assert(stripes[2].read_start == 0 && stripes[2].read_end == 0);
    assert(stripes[0].write_start == 0 && stripes[0].write_end == 0);
    assert(stripes[1].write_start == 0 && stripes[1].write_end == 128*1024);
    assert(stripes[2].write_start == 0 && stripes[2].write_end == 128*1024);
    assert(stripes[0].read_buf == rmw_buf+128*1024);
    assert(stripes[1].read_buf == NULL);
    assert(stripes[2].read_buf == NULL);
    assert(stripes[0].write_buf == NULL);
    assert(stripes[1].write_buf == write_buf);
    assert(stripes[2].write_buf == rmw_buf);
    // Test 11.2
    set_pattern(stripes[0].read_buf, 128*1024, PATTERN1);
    set_pattern(stripes[1].write_buf, 128*1024, PATTERN2);
    calc_rmw_parity_xor(stripes, 3, osd_set, write_osd_set, 128*1024);
    assert(stripes[0].write_start == 0 && stripes[0].write_end == 0);
    assert(stripes[1].write_start == 0 && stripes[1].write_end == 128*1024);
    assert(stripes[2].write_start == 0 && stripes[2].write_end == 128*1024);
    assert(stripes[0].write_buf == NULL);
    assert(stripes[1].write_buf == write_buf);
    assert(stripes[2].write_buf == rmw_buf);
    check_pattern(stripes[2].write_buf, 128*1024, PATTERN1^PATTERN2);
    free(rmw_buf);
    free(write_buf);
}
