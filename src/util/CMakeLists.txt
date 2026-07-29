cmake_minimum_required(VERSION 2.8...3.30)

project(vitastor)

# libutil.a
add_library(util STATIC
	addr_util.cpp
	allocator.cpp
	crc32c.c
	disk_util.cpp
	epoll_manager.cpp
	json_util.cpp
	openssl_util.cpp
	ringloop.cpp
	rw_blocking.cpp
	sha256.c
	str_util.cpp
	timerfd_manager.cpp
	../../json11/json11.cpp
	xxh_x86dispatch.c
)
if (RDMACM_LIBRARIES)
	target_sources(util PRIVATE
		dma_alloc_impl.cpp
	)
endif (RDMACM_LIBRARIES)
if (RDMACM_LIBRARIES)
	target_sources(util PRIVATE
		rdma_alloc.cpp
	)
endif (RDMACM_LIBRARIES)
target_compile_options(util PUBLIC -fPIC)
target_link_libraries(util
	${LIBURING_LIBRARIES}
)

# test_rdma_alloc
add_executable(test_rdma_alloc EXCLUDE_FROM_ALL test_rdma_alloc.cpp dma_alloc_impl.cpp)
add_dependencies(build_tests test_rdma_alloc)
add_test(NAME test_rdma_alloc COMMAND test_rdma_alloc)
