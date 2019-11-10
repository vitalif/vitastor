all: allocator.o blockstore.o blockstore_init.o blockstore_open.o blockstore_read.o \
	blockstore_write.o blockstore_sync.o blockstore_stable.o crc32c.o ringloop.o test
clean:
	rm -f *.o
crc32c.o: crc32c.c
	g++ -c -o $@ $<
%.o: %.cpp blockstore.h
	g++ -c -o $@ $<
test: test.cpp
	g++ -o test -luring test.cpp
