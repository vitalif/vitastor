#include <string.h>
#include "osd_rmw.cpp"
#include "test_pattern.h"

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
    void* rmw_buf = calc_rmw_reads(write_buf, stripes, osd_set, 3, 2, 2);
    assert(stripes[0].read_start == 0 && stripes[0].read_end == 128*1024);
    assert(stripes[1].read_start == 4096 && stripes[1].read_end == 128*1024);
    assert(stripes[2].read_start == 4096 && stripes[2].read_end == 128*1024);
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
    calc_rmw_parity(stripes, 3);
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
    rmw_buf = calc_rmw_reads(write_buf, stripes, osd_set, 3, 2, 2);
    assert(stripes[0].read_start == 64*1024 && stripes[0].read_end == 128*1024);
    assert(stripes[1].read_start == 64*1024 && stripes[1].read_end == 128*1024);
    assert(stripes[2].read_start == 64*1024 && stripes[2].read_end == 128*1024);
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
    rmw_buf = calc_rmw_reads(write_buf, stripes, osd_set, 3, 2, 3);
    assert(stripes[0].read_end == 0);
    assert(stripes[1].read_start == 64*1024 && stripes[1].read_end == 128*1024);
    assert(stripes[2].read_end == 0);
    assert(stripes[0].read_buf == 0);
    assert(stripes[1].read_buf == rmw_buf+128*1024);
    assert(stripes[2].read_buf == 0);
    assert(stripes[0].write_buf == write_buf);
    assert(stripes[1].write_buf == write_buf+128*1024);
    assert(stripes[2].write_buf == rmw_buf);
    free(rmw_buf);
    free(write_buf);
    osd_set[1] = 0;
    // End
    printf("all ok\n");
    return 0;
}
