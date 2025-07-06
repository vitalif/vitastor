cmake_minimum_required(VERSION 2.8.12)

project(vitastor)

### Test stubs

# stub_osd, stub_bench, osd_test
add_executable(stub_osd stub_osd.cpp ../util/rw_blocking.cpp ../util/addr_util.cpp)
target_link_libraries(stub_osd tcmalloc_minimal)
add_executable(stub_bench stub_bench.cpp ../util/rw_blocking.cpp ../util/addr_util.cpp)
target_link_libraries(stub_bench tcmalloc_minimal)
add_executable(osd_test osd_test.cpp ../util/rw_blocking.cpp ../util/addr_util.cpp)
target_link_libraries(osd_test tcmalloc_minimal)

# bindiff
add_executable(bindiff
	bindiff.c
)

# stub_uring_osd
add_executable(stub_uring_osd
	stub_uring_osd.cpp
)
target_link_libraries(stub_uring_osd
	vitastor_common
	${LIBURING_LIBRARIES}
	${IBVERBS_LIBRARIES}
	${RDMACM_LIBRARIES}
	tcmalloc_minimal
)

# test_allocator
add_executable(test_allocator EXCLUDE_FROM_ALL test_allocator.cpp ../util/allocator.cpp)
add_dependencies(build_tests test_allocator)
add_test(NAME test_allocator COMMAND test_allocator)

# test_heap
add_executable(test_heap
	test_heap.cpp
	../blockstore/multilist.cpp
	../blockstore/blockstore_heap.cpp
	../util/crc32c.c
	../util/allocator.cpp
	../blockstore/blockstore_disk.cpp
	../util/str_util.cpp
)
add_test(NAME test_heap COMMAND test_heap)
target_compile_options(test_heap PRIVATE -coverage)
target_link_options(test_heap PRIVATE -coverage)

# test_cas
add_executable(test_cas
	test_cas.cpp
)
target_link_libraries(test_cas
	vitastor_client
)

# test_crc32
add_executable(test_crc32
	test_crc32.cpp
)
target_link_libraries(test_crc32
	vitastor_blk
)

## test_blockstore, test_shit
#add_executable(test_blockstore test_blockstore.cpp)
#target_link_libraries(test_blockstore blockstore)
#add_executable(test_shit test_shit.cpp osd_peering_pg.cpp)
#target_link_libraries(test_shit ${LIBURING_LIBRARIES} m)
