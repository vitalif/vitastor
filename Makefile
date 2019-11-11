all: allocator.o blockstore.o blockstore_init.o blockstore_open.o blockstore_journal.o blockstore_read.o \
	blockstore_write.o blockstore_sync.o blockstore_stable.o crc32c.o ringloop.o test
clean:
	rm -f *.o
crc32c.o: crc32c.c
	g++ -c -o $@ $<
%.o: %.cpp blockstore.h
	g++ -Wall -Wno-sign-compare -Wno-parentheses -c -o $@ $<
test: test.cpp
	g++ -O3 -o test -luring test.cpp
