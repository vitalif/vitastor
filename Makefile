BLOCKSTORE_OBJS := allocator.o blockstore.o blockstore_init.o blockstore_open.o blockstore_journal.o blockstore_read.o \
	blockstore_write.o blockstore_sync.o blockstore_stable.o blockstore_flush.o crc32c.o ringloop.o timerfd_interval.o
all: $(BLOCKSTORE_OBJS) test test_blockstore libfio_blockstore.so
clean:
	rm -f *.o
crc32c.o: crc32c.c
	g++ -g -O3 -fPIC -c -o $@ $<
%.o: %.cpp allocator.h blockstore_flush.h blockstore.h blockstore_init.h blockstore_journal.h crc32c.h ringloop.h xor.h timerfd_interval.h
	g++ -g -O3 -Wall -Wno-sign-compare -Wno-parentheses -Wno-pointer-arith -fPIC -c -o $@ $<
test: test.cpp
	g++ -g -O3 -o test -luring test.cpp
test_blockstore: $(BLOCKSTORE_OBJS) test_blockstore.cpp
	g++ -g -o test_blockstore -ltcmalloc_minimal -luring test_blockstore.cpp $(BLOCKSTORE_OBJS)
test_allocator: test_allocator.cpp allocator.o
	g++ -g -o test_allocator test_allocator.cpp allocator.o
libfio_blockstore.so: fio_engine.cpp $(BLOCKSTORE_OBJS)
	g++ -g -O3 -ltcmalloc_minimal -Wno-pointer-arith -fPIC -shared -luring -o libfio_blockstore.so fio_engine.cpp $(BLOCKSTORE_OBJS)
