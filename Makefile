all: allocator.o blockstore.o
%.o: %.cpp
	gcc -c -o $@ $<
