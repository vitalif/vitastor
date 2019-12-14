BLOCKSTORE_OBJS := allocator.o blockstore.o blockstore_init.o blockstore_open.o blockstore_journal.o blockstore_read.o \
	blockstore_write.o blockstore_sync.o blockstore_stable.o blockstore_flush.o crc32c.o ringloop.o timerfd_interval.o osd.o
CXXFLAGS := -g -O3 -Wall -Wno-sign-compare -Wno-comment -Wno-parentheses -Wno-pointer-arith -fPIC -fdiagnostics-color=always
all: $(BLOCKSTORE_OBJS) test test_blockstore libfio_blockstore.so osd
clean:
	rm -f *.o
crc32c.o: crc32c.c
	g++ $(CXXFLAGS) -c -o $@ $<
%.o: %.cpp allocator.h blockstore_flush.h blockstore.h blockstore_init.h blockstore_journal.h crc32c.h ringloop.h xor.h timerfd_interval.h
	g++ $(CXXFLAGS) -c -o $@ $<
osd: $(BLOCKSTORE_OBJS) osd_main.cpp osd.h osd_ops.h
	g++ $(CXXFLAGS) -ltcmalloc_minimal -luring -o osd osd_main.cpp $(BLOCKSTORE_OBJS)
test: test.cpp
	g++ $(CXXFLAGS) -o test -luring test.cpp
test_blockstore: $(BLOCKSTORE_OBJS) test_blockstore.cpp
	g++ $(CXXFLAGS) -o test_blockstore -ltcmalloc_minimal -luring test_blockstore.cpp $(BLOCKSTORE_OBJS)
test_allocator: test_allocator.cpp allocator.o
	g++ $(CXXFLAGS) -o test_allocator test_allocator.cpp allocator.o
libfio_blockstore.so: fio_engine.cpp $(BLOCKSTORE_OBJS)
	g++ $(CXXFLAGS) -ltcmalloc_minimal -shared -luring -o libfio_blockstore.so fio_engine.cpp $(BLOCKSTORE_OBJS)
