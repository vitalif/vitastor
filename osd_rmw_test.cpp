#include <string.h>
#include "osd_rmw.cpp"
#include "test_pattern.h"

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
   then, after calc_rmw_parity(): {
     write: [ [ 128K-4K, 128K ], [ 0, 128K ], [ 0, 128K ] ],
     write1==read1,
   }
   + check write1 buffer
   + check write2 buffer

***/

void test7();

int main(int narg, char *args[])
{
    osd_num_t osd_set[3] = { 1, 0, 3 };
    osd_rmw_stripe_t stripes[3] = { 0 };
    // Test 1
    split_stripes(2, 128*1024, 128*1024-4096, 8192, stripes);
    assert(stripes[0].req_start == 128*1024-4096 && stripes[0].req_end == 128*1024);
    assert(stripes[1].req_start == 0 && stripes[1].req_end == 4096);
    assert(stripes[2].req_end == 0);
    // Test 2
    for (int i = 0; i < 3; i++)
    {
        stripes[i].read_start = stripes[i].req_start;
        stripes[i].read_end = stripes[i].req_end;
    }
    assert(extend_missing_stripes(stripes, osd_set, 2, 3) == 0);
    assert(stripes[0].read_start == 0 && stripes[0].read_end == 128*1024);
    assert(stripes[2].read_start == 0 && stripes[2].read_end == 4096);
    // Test 3
    stripes[0] = { .req_start = 128*1024-4096, .req_end = 128*1024 };
    cover_read(0, 128*1024, stripes[0]);
    assert(stripes[0].read_start == 0 && stripes[0].read_end == 128*1024-4096);
    // Test 4.1
    memset(stripes, 0, sizeof(stripes));
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
    calc_rmw_parity(stripes, 3, osd_set, osd_set, 128*1024);
    check_pattern(stripes[2].write_buf, 4096, PATTERN0^PATTERN1); // new parity
    check_pattern(stripes[2].write_buf+4096, 128*1024-4096*2, 0); // new parity
    check_pattern(stripes[2].write_buf+128*1024-4096, 4096, PATTERN0^PATTERN1); // new parity
    free(rmw_buf);
    free(write_buf);
    // Test 5.1
    memset(stripes, 0, sizeof(stripes));
    split_stripes(2, 128*1024, 0, 64*1024*3, stripes);
    assert(stripes[0].req_start == 0 && stripes[0].req_end == 128*1024);
    assert(stripes[1].req_start == 0 && stripes[1].req_end == 64*1024);
    assert(stripes[2].req_end == 0);
    // Test 5.2
    write_buf = malloc(64*1024*3);
    rmw_buf = calc_rmw(write_buf, stripes, osd_set, 3, 2, 2, osd_set, 128*1024);
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
    // Test 6.1
    memset(stripes, 0, sizeof(stripes));
    split_stripes(2, 128*1024, 0, 64*1024*3, stripes);
    osd_set[1] = 2;
    write_buf = malloc(64*1024*3);
    rmw_buf = calc_rmw(write_buf, stripes, osd_set, 3, 2, 3, osd_set, 128*1024);
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
    osd_set[1] = 0;
    // Test 7
    test7();
    // End
    printf("all ok\n");
    return 0;
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
    calc_rmw_parity(stripes, 3, osd_set, write_osd_set, 128*1024);
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
