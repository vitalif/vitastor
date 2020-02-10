BLOCKSTORE_OBJS := allocator.o blockstore.o blockstore_impl.o blockstore_init.o blockstore_open.o blockstore_journal.o blockstore_read.o \
	blockstore_write.o blockstore_sync.o blockstore_stable.o blockstore_rollback.o blockstore_flush.o crc32c.o ringloop.o timerfd_interval.o
CXXFLAGS := -g -O3 -Wall -Wno-sign-compare -Wno-comment -Wno-parentheses -Wno-pointer-arith -fPIC -fdiagnostics-color=always
all: $(BLOCKSTORE_OBJS) libfio_blockstore.so osd libfio_sec_osd.so test_blockstore stub_osd test_osd
clean:
	rm -f *.o

crc32c.o: crc32c.c
	g++ $(CXXFLAGS) -c -o $@ $<
json11.o: json11/json11.cpp
	g++ $(CXXFLAGS) -c -o json11.o json11/json11.cpp
allocator.o: allocator.cpp allocator.h
	g++ $(CXXFLAGS) -c -o $@ $<
ringloop.o: ringloop.cpp ringloop.h
	g++ $(CXXFLAGS) -c -o $@ $<
timerfd_interval.o: timerfd_interval.cpp timerfd_interval.h
	g++ $(CXXFLAGS) -c -o $@ $<

%.o: %.cpp allocator.h blockstore_flush.h blockstore.h blockstore_impl.h blockstore_init.h blockstore_journal.h crc32c.h ringloop.h timerfd_interval.h object_id.h
	g++ $(CXXFLAGS) -c -o $@ $<

libblockstore.so: $(BLOCKSTORE_OBJS)
	g++ $(CXXFLAGS) -o libblockstore.so -shared $(BLOCKSTORE_OBJS) -ltcmalloc_minimal -luring
libfio_blockstore.so: ./libblockstore.so fio_engine.cpp json11.o
	g++ $(CXXFLAGS) -shared -o libfio_blockstore.so fio_engine.cpp json11.o ./libblockstore.so -ltcmalloc_minimal -luring

OSD_OBJS := osd.o osd_exec_secondary.o osd_receive.o osd_send.o osd_peering.o osd_peering_pg.o osd_primary.o json11.o timerfd_interval.o
osd_exec_secondary.o: osd_exec_secondary.cpp osd.h osd_ops.h
	g++ $(CXXFLAGS) -c -o $@ $<
osd_receive.o: osd_receive.cpp osd.h osd_ops.h
	g++ $(CXXFLAGS) -c -o $@ $<
osd_send.o: osd_send.cpp osd.h osd_ops.h
	g++ $(CXXFLAGS) -c -o $@ $<
osd_peering.o: osd_peering.cpp osd.h osd_ops.h osd_peering_pg.h
	g++ $(CXXFLAGS) -c -o $@ $<
osd_peering_pg.o: osd_peering_pg.cpp object_id.h osd_peering_pg.h
	g++ $(CXXFLAGS) -c -o $@ $<
osd_primary.o: osd_primary.cpp osd.h osd_ops.h osd_peering_pg.h xor.h
	g++ $(CXXFLAGS) -c -o $@ $<
osd.o: osd.cpp osd.h osd_ops.h osd_peering_pg.h
	g++ $(CXXFLAGS) -c -o $@ $<
osd: ./libblockstore.so osd_main.cpp osd.h osd_ops.h $(OSD_OBJS)
	g++ $(CXXFLAGS) -o osd osd_main.cpp $(OSD_OBJS) ./libblockstore.so -ltcmalloc_minimal -luring
stub_osd: stub_osd.cpp osd_ops.h rw_blocking.o
	g++ $(CXXFLAGS) -o stub_osd stub_osd.cpp rw_blocking.o -ltcmalloc_minimal
rw_blocking.o: rw_blocking.cpp rw_blocking.h
	g++ $(CXXFLAGS) -c -o $@ $<
test_osd: test_osd.cpp osd_ops.h rw_blocking.o
	g++ $(CXXFLAGS) -o test_osd test_osd.cpp rw_blocking.o -ltcmalloc_minimal

libfio_sec_osd.so: fio_sec_osd.cpp osd_ops.h rw_blocking.o
	g++ $(CXXFLAGS) -ltcmalloc_minimal -shared -o libfio_sec_osd.so fio_sec_osd.cpp rw_blocking.o -luring

test_blockstore: ./libblockstore.so test_blockstore.cpp
	g++ $(CXXFLAGS) -o test_blockstore test_blockstore.cpp ./libblockstore.so -ltcmalloc_minimal -luring
test: test.cpp osd_peering_pg.o
	g++ $(CXXFLAGS) -o test test.cpp osd_peering_pg.o -luring
test_allocator: test_allocator.cpp allocator.o
	g++ $(CXXFLAGS) -o test_allocator test_allocator.cpp allocator.o
