all: allocator.o blockstore.o blockstore_init.o blockstore_open.o blockstore_read.o blockstore_write.o crc32c.o ringloop.o test
clean:
	rm -f *.o
crc32c.o: crc32c.c
	gcc -c -o $@ $<
%.o: %.cpp
	gcc -c -o $@ $<
test: test.cpp
	gcc -o test -luring test.cpp
