BLOCKSTORE_OBJS := allocator.o blockstore.o blockstore_impl.o blockstore_init.o blockstore_open.o blockstore_journal.o blockstore_read.o \
	blockstore_write.o blockstore_sync.o blockstore_stable.o blockstore_rollback.o blockstore_flush.o crc32c.o ringloop.o
# -fsanitize=address
CXXFLAGS := -g -O3 -Wall -Wno-sign-compare -Wno-comment -Wno-parentheses -Wno-pointer-arith -fPIC -fdiagnostics-color=always
all: $(BLOCKSTORE_OBJS) libfio_blockstore.so osd libfio_sec_osd.so stub_osd stub_bench osd_test dump_journal
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
timerfd_interval.o: timerfd_interval.cpp timerfd_interval.h ringloop.h
	g++ $(CXXFLAGS) -c -o $@ $<
timerfd_manager.o: timerfd_manager.cpp timerfd_manager.h ringloop.h
	g++ $(CXXFLAGS) -c -o $@ $<

%.o: %.cpp allocator.h blockstore_flush.h blockstore.h blockstore_impl.h blockstore_init.h blockstore_journal.h crc32c.h ringloop.h object_id.h
	g++ $(CXXFLAGS) -c -o $@ $<
dump_journal: dump_journal.cpp crc32c.o blockstore_journal.h
	g++ $(CXXFLAGS) -o $@ $< crc32c.o

libblockstore.so: $(BLOCKSTORE_OBJS)
	g++ $(CXXFLAGS) -o libblockstore.so -shared $(BLOCKSTORE_OBJS) -ltcmalloc_minimal -luring
libfio_blockstore.so: ./libblockstore.so fio_engine.cpp json11.o
	g++ $(CXXFLAGS) -shared -o libfio_blockstore.so fio_engine.cpp json11.o ./libblockstore.so -ltcmalloc_minimal -luring

OSD_OBJS := osd.o osd_secondary.o osd_receive.o osd_send.o osd_peering.o osd_flush.o osd_peering_pg.o \
	osd_primary.o osd_primary_subops.o etcd_state_client.o cluster_client.o osd_cluster.o http_client.o pg_states.o \
	osd_rmw.o json11.o base64.o timerfd_manager.o
base64.o: base64.cpp base64.h
	g++ $(CXXFLAGS) -c -o $@ $<
osd_secondary.o: osd_secondary.cpp osd.h osd_ops.h ringloop.h
	g++ $(CXXFLAGS) -c -o $@ $<
osd_receive.o: osd_receive.cpp osd.h osd_ops.h ringloop.h
	g++ $(CXXFLAGS) -c -o $@ $<
osd_send.o: osd_send.cpp osd.h osd_ops.h ringloop.h
	g++ $(CXXFLAGS) -c -o $@ $<
osd_peering.o: osd_peering.cpp osd.h osd_ops.h osd_peering_pg.h ringloop.h
	g++ $(CXXFLAGS) -c -o $@ $<
osd_cluster.o: osd_cluster.cpp osd.h osd_ops.h ringloop.h
	g++ $(CXXFLAGS) -c -o $@ $<
http_client.o: http_client.cpp http_client.h
	g++ $(CXXFLAGS) -c -o $@ $<
etcd_state_client.o: etcd_state_client.cpp etcd_state_client.h http_client.h pg_states.h
	g++ $(CXXFLAGS) -c -o $@ $<
cluster_client.o: cluster_client.cpp cluster_client.h osd_ops.h timerfd_manager.h ringloop.h
	g++ $(CXXFLAGS) -c -o $@ $<
osd_flush.o: osd_flush.cpp osd.h osd_ops.h osd_peering_pg.h ringloop.h
	g++ $(CXXFLAGS) -c -o $@ $<
osd_peering_pg.o: osd_peering_pg.cpp object_id.h osd_peering_pg.h pg_states.h
	g++ $(CXXFLAGS) -c -o $@ $<
pg_states.o: pg_states.cpp pg_states.h
	g++ $(CXXFLAGS) -c -o $@ $<
osd_rmw.o: osd_rmw.cpp osd_rmw.h xor.h
	g++ $(CXXFLAGS) -c -o $@ $<
osd_rmw_test: osd_rmw_test.cpp osd_rmw.cpp osd_rmw.h xor.h
	g++ $(CXXFLAGS) -o $@ $<
osd_primary.o: osd_primary.cpp osd_primary.h osd_rmw.h osd.h osd_ops.h osd_peering_pg.h xor.h ringloop.h
	g++ $(CXXFLAGS) -c -o $@ $<
osd_primary_subops.o: osd_primary_subops.cpp osd_primary.h osd_rmw.h osd.h osd_ops.h osd_peering_pg.h xor.h ringloop.h
	g++ $(CXXFLAGS) -c -o $@ $<
osd.o: osd.cpp osd.h http_client.h osd_ops.h osd_peering_pg.h ringloop.h
	g++ $(CXXFLAGS) -c -o $@ $<
osd: ./libblockstore.so osd_main.cpp osd.h osd_ops.h $(OSD_OBJS)
	g++ $(CXXFLAGS) -o osd osd_main.cpp $(OSD_OBJS) ./libblockstore.so -ltcmalloc_minimal -luring
stub_osd: stub_osd.cpp osd_ops.h rw_blocking.o
	g++ $(CXXFLAGS) -o stub_osd stub_osd.cpp rw_blocking.o -ltcmalloc_minimal
stub_bench: stub_bench.cpp osd_ops.h rw_blocking.o
	g++ $(CXXFLAGS) -o stub_bench stub_bench.cpp rw_blocking.o -ltcmalloc_minimal
rw_blocking.o: rw_blocking.cpp rw_blocking.h
	g++ $(CXXFLAGS) -c -o $@ $<
osd_test: osd_test.cpp osd_ops.h rw_blocking.o
	g++ $(CXXFLAGS) -o osd_test osd_test.cpp rw_blocking.o -ltcmalloc_minimal
osd_peering_pg_test: osd_peering_pg_test.cpp osd_peering_pg.o
	g++ $(CXXFLAGS) -o $@ $< osd_peering_pg.o -ltcmalloc_minimal

libfio_sec_osd.so: fio_sec_osd.cpp osd_ops.h rw_blocking.o
	g++ $(CXXFLAGS) -ltcmalloc_minimal -shared -o libfio_sec_osd.so fio_sec_osd.cpp rw_blocking.o -luring

test_blockstore: ./libblockstore.so test_blockstore.cpp timerfd_interval.o
	g++ $(CXXFLAGS) -o test_blockstore test_blockstore.cpp timerfd_interval.o ./libblockstore.so -ltcmalloc_minimal -luring
test: test.cpp osd_peering_pg.o
	g++ $(CXXFLAGS) -o test test.cpp osd_peering_pg.o -luring -lm
test_allocator: test_allocator.cpp allocator.o
	g++ $(CXXFLAGS) -o test_allocator test_allocator.cpp allocator.o
