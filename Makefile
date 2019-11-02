all: allocator.o blockstore.o blockstore_open.o blockstore_read.o test
%.o: %.cpp
	gcc -c -o $@ $<
test: test.cpp
	gcc -o test -luring test.cpp
