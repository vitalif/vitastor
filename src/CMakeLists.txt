cmake_minimum_required(VERSION 2.8)

project(vitastor)

include(GNUInstallDirs)

set(QEMU_PLUGINDIR qemu CACHE STRING "QEMU plugin directory suffix (qemu-kvm on RHEL)")
set(WITH_ASAN false CACHE BOOL "Build with AddressSanitizer")
if("${CMAKE_INSTALL_PREFIX}" MATCHES "^/usr/local/?$")
	if(EXISTS "/etc/debian_version")
		set(CMAKE_INSTALL_LIBDIR "lib/${CMAKE_LIBRARY_ARCHITECTURE}")
	endif()
	set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}")
endif()

add_definitions(-DVERSION="0.6.2")
add_definitions(-Wall -Wno-sign-compare -Wno-comment -Wno-parentheses -Wno-pointer-arith -I ${CMAKE_SOURCE_DIR}/src)
if (${WITH_ASAN})
	add_definitions(-fsanitize=address -fno-omit-frame-pointer)
	add_link_options(-fsanitize=address -fno-omit-frame-pointer)
endif (${WITH_ASAN})

set(CMAKE_BUILD_TYPE RelWithDebInfo)
string(REGEX REPLACE "([\\/\\-]O)[12]?" "\\13" CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")
string(REGEX REPLACE "([\\/\\-]O)[12]?" "\\13" CMAKE_CXX_FLAGS_MINSIZEREL "${CMAKE_CXX_FLAGS_MINSIZEREL}")
string(REGEX REPLACE "([\\/\\-]O)[12]?" "\\13" CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO}")
string(REGEX REPLACE "([\\/\\-]D) *NDEBUG" "" CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")
string(REGEX REPLACE "([\\/\\-]D) *NDEBUG" "" CMAKE_CXX_FLAGS_MINSIZEREL "${CMAKE_CXX_FLAGS_MINSIZEREL}")
string(REGEX REPLACE "([\\/\\-]D) *NDEBUG" "" CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO}")
string(REGEX REPLACE "([\\/\\-]O)[12]?" "\\13" CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE}")
string(REGEX REPLACE "([\\/\\-]O)[12]?" "\\13" CMAKE_C_FLAGS_MINSIZEREL "${CMAKE_C_FLAGS_MINSIZEREL}")
string(REGEX REPLACE "([\\/\\-]O)[12]?" "\\13" CMAKE_C_FLAGS_RELWITHDEBINFO "${CMAKE_C_FLAGS_RELWITHDEBINFO}")
string(REGEX REPLACE "([\\/\\-]D) *NDEBUG" "" CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE}")
string(REGEX REPLACE "([\\/\\-]D) *NDEBUG" "" CMAKE_C_FLAGS_MINSIZEREL "${CMAKE_C_FLAGS_MINSIZEREL}")
string(REGEX REPLACE "([\\/\\-]D) *NDEBUG" "" CMAKE_C_FLAGS_RELWITHDEBINFO "${CMAKE_C_FLAGS_RELWITHDEBINFO}")

find_package(PkgConfig)
pkg_check_modules(LIBURING REQUIRED liburing)
pkg_check_modules(GLIB REQUIRED glib-2.0)

include_directories(
	../
	/usr/include/jerasure
	${LIBURING_INCLUDE_DIRS}
)

# libvitastor_blk.so
add_library(vitastor_blk SHARED
	allocator.cpp blockstore.cpp blockstore_impl.cpp blockstore_init.cpp blockstore_open.cpp blockstore_journal.cpp blockstore_read.cpp
	blockstore_write.cpp blockstore_sync.cpp blockstore_stable.cpp blockstore_rollback.cpp blockstore_flush.cpp crc32c.c ringloop.cpp
)
target_link_libraries(vitastor_blk
	${LIBURING_LIBRARIES}
	tcmalloc_minimal
)

# libfio_vitastor_blk.so
add_library(fio_vitastor_blk SHARED
	fio_engine.cpp
	../json11/json11.cpp
)
target_link_libraries(fio_vitastor_blk
	vitastor_blk
)

# libvitastor_common.a
add_library(vitastor_common STATIC
	epoll_manager.cpp etcd_state_client.cpp
	messenger.cpp msgr_stop.cpp msgr_op.cpp msgr_send.cpp msgr_receive.cpp ringloop.cpp ../json11/json11.cpp
	http_client.cpp osd_ops.cpp pg_states.cpp timerfd_manager.cpp base64.cpp
)
target_compile_options(vitastor_common PUBLIC -fPIC)

# vitastor-osd
add_executable(vitastor-osd
	osd_main.cpp osd.cpp osd_secondary.cpp osd_peering.cpp osd_flush.cpp osd_peering_pg.cpp
	osd_primary.cpp osd_primary_chain.cpp osd_primary_sync.cpp osd_primary_write.cpp osd_primary_subops.cpp
	osd_cluster.cpp osd_rmw.cpp
)
target_link_libraries(vitastor-osd
	vitastor_common
	vitastor_blk
	Jerasure
)

# libfio_vitastor_sec.so
add_library(fio_vitastor_sec SHARED
	fio_sec_osd.cpp
	rw_blocking.cpp
)
target_link_libraries(fio_vitastor_sec
	tcmalloc_minimal
)

# libvitastor_client.so
add_library(vitastor_client SHARED
	cluster_client.cpp
)
target_link_libraries(vitastor_client
	vitastor_common
	tcmalloc_minimal
	${LIBURING_LIBRARIES}
)

# libfio_vitastor.so
add_library(fio_vitastor SHARED
	fio_cluster.cpp
)
target_link_libraries(fio_vitastor
	vitastor_client
)

# vitastor-nbd
add_executable(vitastor-nbd
	nbd_proxy.cpp
)
target_link_libraries(vitastor-nbd
	vitastor_client
)

# vitastor-rm
add_executable(vitastor-rm
	rm_inode.cpp
)
target_link_libraries(vitastor-rm
	vitastor_client
)

# vitastor-dump-journal
add_executable(vitastor-dump-journal
	dump_journal.cpp crc32c.c
)

# qemu_driver.so
add_library(qemu_proxy STATIC qemu_proxy.cpp)
target_compile_options(qemu_proxy PUBLIC -fPIC)
target_include_directories(qemu_proxy PUBLIC
	../qemu/b/qemu
	../qemu/include
	${GLIB_INCLUDE_DIRS}
)
target_link_libraries(qemu_proxy
	vitastor_client
)
add_library(qemu_vitastor SHARED
	qemu_driver.c
)
target_link_libraries(qemu_vitastor
	qemu_proxy
)
set_target_properties(qemu_vitastor PROPERTIES
	PREFIX ""
	OUTPUT_NAME "block-vitastor"
)

### Test stubs

# stub_osd, stub_bench, osd_test
add_executable(stub_osd stub_osd.cpp rw_blocking.cpp)
target_link_libraries(stub_osd tcmalloc_minimal)
add_executable(stub_bench stub_bench.cpp rw_blocking.cpp)
target_link_libraries(stub_bench tcmalloc_minimal)
add_executable(osd_test osd_test.cpp rw_blocking.cpp)
target_link_libraries(osd_test tcmalloc_minimal)

# osd_rmw_test
add_executable(osd_rmw_test osd_rmw_test.cpp allocator.cpp)
target_link_libraries(osd_rmw_test Jerasure tcmalloc_minimal)

# stub_uring_osd
add_executable(stub_uring_osd
	stub_uring_osd.cpp
)
target_link_libraries(stub_uring_osd
	vitastor_common
	${LIBURING_LIBRARIES}
	tcmalloc_minimal
)

# osd_peering_pg_test
add_executable(osd_peering_pg_test osd_peering_pg_test.cpp osd_peering_pg.cpp)
target_link_libraries(osd_peering_pg_test tcmalloc_minimal)

# test_allocator
add_executable(test_allocator test_allocator.cpp allocator.cpp)

# test_cluster_client
add_executable(test_cluster_client
	test_cluster_client.cpp
	pg_states.cpp osd_ops.cpp cluster_client.cpp msgr_op.cpp mock/messenger.cpp msgr_stop.cpp
	etcd_state_client.cpp timerfd_manager.cpp ../json11/json11.cpp
)
target_compile_definitions(test_cluster_client PUBLIC -D__MOCK__)
target_include_directories(test_cluster_client PUBLIC ${CMAKE_SOURCE_DIR}/src/mock)

## test_blockstore, test_shit
#add_executable(test_blockstore test_blockstore.cpp)
#target_link_libraries(test_blockstore blockstore)
#add_executable(test_shit test_shit.cpp osd_peering_pg.cpp)
#target_link_libraries(test_shit ${LIBURING_LIBRARIES} m)

### Install

install(TARGETS vitastor-osd vitastor-dump-journal vitastor-nbd vitastor-rm RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
install(TARGETS fio_vitastor fio_vitastor_blk fio_vitastor_sec vitastor_blk vitastor_client LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(TARGETS qemu_vitastor LIBRARY DESTINATION /usr/${CMAKE_INSTALL_LIBDIR}/${QEMU_PLUGINDIR})
