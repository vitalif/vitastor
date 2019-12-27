BLOCKSTORE_OBJS := allocator.o blockstore.o blockstore_impl.o blockstore_init.o blockstore_open.o blockstore_journal.o blockstore_read.o \
	blockstore_write.o blockstore_sync.o blockstore_stable.o blockstore_flush.o crc32c.o ringloop.o timerfd_interval.o
CXXFLAGS := -g -O3 -Wall -Wno-sign-compare -Wno-comment -Wno-parentheses -Wno-pointer-arith -fPIC -fdiagnostics-color=always
all: $(BLOCKSTORE_OBJS) test test_blockstore libfio_blockstore.so osd libfio_sec_osd.so
clean:
	rm -f *.o
crc32c.o: crc32c.c
	g++ $(CXXFLAGS) -c -o $@ $<
json11.o: json11/json11.cpp
	g++ $(CXXFLAGS) -c -o json11.o json11/json11.cpp
%.o: %.cpp allocator.h blockstore_flush.h blockstore.h blockstore_impl.h blockstore_init.h blockstore_journal.h crc32c.h ringloop.h xor.h timerfd_interval.h object_id.h
	g++ $(CXXFLAGS) -c -o $@ $<
osd_exec_secondary.o: osd_exec_secondary.cpp osd.h osd_ops.h
	g++ $(CXXFLAGS) -c -o $@ $<
osd_read.o: osd_read.cpp osd.h osd_ops.h
	g++ $(CXXFLAGS) -c -o $@ $<
osd_send.o: osd_send.cpp osd.h osd_ops.h
	g++ $(CXXFLAGS) -c -o $@ $<
osd: $(BLOCKSTORE_OBJS) osd_main.cpp osd.h osd_ops.h osd.o osd_exec_secondary.o osd_read.o osd_send.o json11.o
	g++ $(CXXFLAGS) -ltcmalloc_minimal -luring -o osd osd_main.cpp osd.o osd_exec_secondary.o osd_read.o osd_send.o json11.o $(BLOCKSTORE_OBJS)
test: test.cpp
	g++ $(CXXFLAGS) -o test -luring test.cpp
test_blockstore: $(BLOCKSTORE_OBJS) test_blockstore.cpp
	g++ $(CXXFLAGS) -o test_blockstore -ltcmalloc_minimal -luring test_blockstore.cpp $(BLOCKSTORE_OBJS)
test_allocator: test_allocator.cpp allocator.o
	g++ $(CXXFLAGS) -o test_allocator test_allocator.cpp allocator.o
libfio_blockstore.so: fio_engine.cpp $(BLOCKSTORE_OBJS)
	g++ $(CXXFLAGS) -ltcmalloc_minimal -shared -luring -o libfio_blockstore.so fio_engine.cpp $(BLOCKSTORE_OBJS)
libfio_sec_osd.so: fio_sec_osd.cpp osd_ops.h
	g++ $(CXXFLAGS) -ltcmalloc_minimal -shared -luring -o libfio_sec_osd.so fio_sec_osd.cpp
