cmake_minimum_required(VERSION 2.8.12)

project(vitastor)

include(GNUInstallDirs)
include(CTest)
include(CheckIncludeFile)

find_package(PkgConfig)

set(WITH_QEMU false CACHE BOOL "Build QEMU driver inside Vitastor source tree")
set(WITH_FIO true CACHE BOOL "Build FIO driver")
set(QEMU_PLUGINDIR qemu CACHE STRING "QEMU plugin directory suffix (qemu-kvm on RHEL)")
set(WITH_ASAN false CACHE BOOL "Build with AddressSanitizer")
if("${CMAKE_INSTALL_PREFIX}" MATCHES "^/usr/local/?$")
	if(EXISTS "/etc/debian_version")
		set(CMAKE_INSTALL_LIBDIR "lib/${CMAKE_LIBRARY_ARCHITECTURE}")
	endif()
	set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}")
endif()

add_definitions(-DVERSION="1.4.1")
add_definitions(-Wall -Wno-sign-compare -Wno-comment -Wno-parentheses -Wno-pointer-arith -fdiagnostics-color=always -fno-omit-frame-pointer -I ${CMAKE_SOURCE_DIR}/src)
add_link_options(-fno-omit-frame-pointer)
if (${WITH_ASAN})
	add_definitions(-fsanitize=address)
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

macro(install_symlink filepath sympath)
	install(CODE "execute_process(COMMAND ${CMAKE_COMMAND} -E create_symlink ${filepath} \$ENV{DESTDIR}${sympath})")
	install(CODE "message(\"-- Created symlink: ${sympath} -> ${filepath}\")")
endmacro(install_symlink)

check_include_file("linux/nbd-netlink.h" HAVE_NBD_NETLINK_H)

find_package(PkgConfig)
pkg_check_modules(LIBURING REQUIRED liburing)
if (${WITH_QEMU})
	pkg_check_modules(GLIB REQUIRED glib-2.0)
endif (${WITH_QEMU})
pkg_check_modules(IBVERBS libibverbs)
if (IBVERBS_LIBRARIES)
	add_definitions(-DWITH_RDMA)
endif (IBVERBS_LIBRARIES)
pkg_check_modules(ISAL libisal)
if (ISAL_LIBRARIES)
	add_definitions(-DWITH_ISAL)
endif (ISAL_LIBRARIES)

add_custom_target(build_tests)
add_custom_target(test
	COMMAND
	echo leak:tcmalloc > ${CMAKE_CURRENT_BINARY_DIR}/lsan-suppress.txt &&
	env LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_BINARY_DIR}/lsan-suppress.txt ${CMAKE_CTEST_COMMAND}
)
add_dependencies(test build_tests)

include_directories(
	../
	/usr/include/jerasure
	${LIBURING_INCLUDE_DIRS}
	${IBVERBS_INCLUDE_DIRS}
)

# libvitastor_blk.so
add_library(vitastor_blk SHARED
	allocator.cpp blockstore.cpp blockstore_impl.cpp blockstore_disk.cpp blockstore_init.cpp blockstore_open.cpp blockstore_journal.cpp blockstore_read.cpp
	blockstore_write.cpp blockstore_sync.cpp blockstore_stable.cpp blockstore_rollback.cpp blockstore_flush.cpp crc32c.c ringloop.cpp
)
target_link_libraries(vitastor_blk
	${LIBURING_LIBRARIES}
	tcmalloc_minimal
	# for timerfd_manager
	vitastor_common
)
set_target_properties(vitastor_blk PROPERTIES VERSION ${VERSION} SOVERSION 0)

if (${WITH_FIO})
	# libfio_vitastor_blk.so
	add_library(fio_vitastor_blk SHARED
		fio_engine.cpp
		../json11/json11.cpp
	)
	target_link_libraries(fio_vitastor_blk
		vitastor_blk
	)
endif (${WITH_FIO})

# libvitastor_common.a
set(MSGR_RDMA "")
if (IBVERBS_LIBRARIES)
	set(MSGR_RDMA "msgr_rdma.cpp")
endif (IBVERBS_LIBRARIES)
add_library(vitastor_common STATIC
	epoll_manager.cpp etcd_state_client.cpp messenger.cpp addr_util.cpp
	msgr_stop.cpp msgr_op.cpp msgr_send.cpp msgr_receive.cpp ringloop.cpp ../json11/json11.cpp
	http_client.cpp osd_ops.cpp pg_states.cpp timerfd_manager.cpp str_util.cpp ${MSGR_RDMA}
)
target_compile_options(vitastor_common PUBLIC -fPIC)

# vitastor-osd
add_executable(vitastor-osd
	osd_main.cpp osd.cpp osd_secondary.cpp osd_peering.cpp osd_flush.cpp osd_peering_pg.cpp
	osd_primary.cpp osd_primary_chain.cpp osd_primary_sync.cpp osd_primary_write.cpp osd_primary_subops.cpp
	osd_cluster.cpp osd_rmw.cpp osd_scrub.cpp osd_primary_describe.cpp
)
target_link_libraries(vitastor-osd
	vitastor_common
	vitastor_blk
	Jerasure
	${ISAL_LIBRARIES}
	${IBVERBS_LIBRARIES}
)

if (${WITH_FIO})
	# libfio_vitastor_sec.so
	add_library(fio_vitastor_sec SHARED
		fio_sec_osd.cpp
		rw_blocking.cpp
		addr_util.cpp
	)
	target_link_libraries(fio_vitastor_sec
		tcmalloc_minimal
	)
endif (${WITH_FIO})

# libvitastor_client.so
add_library(vitastor_client SHARED
	cluster_client.cpp
	cluster_client_list.cpp
	cluster_client_wb.cpp
	vitastor_c.cpp
	cli_common.cpp
	cli_alloc_osd.cpp
	cli_status.cpp
	cli_describe.cpp
	cli_fix.cpp
	cli_df.cpp
	cli_ls.cpp
	cli_create.cpp
	cli_modify.cpp
	cli_flatten.cpp
	cli_merge.cpp
	cli_rm_data.cpp
	cli_rm.cpp
	cli_rm_osd.cpp
)
set_target_properties(vitastor_client PROPERTIES PUBLIC_HEADER "vitastor_c.h")
target_link_libraries(vitastor_client
	vitastor_common
	${LIBURING_LIBRARIES}
	${IBVERBS_LIBRARIES}
)
set_target_properties(vitastor_client PROPERTIES VERSION ${VERSION} SOVERSION 0)

if (${WITH_FIO})
	# libfio_vitastor.so
	add_library(fio_vitastor SHARED
		fio_cluster.cpp
	)
	target_link_libraries(fio_vitastor
		vitastor_client
	)
endif (${WITH_FIO})

# vitastor-nbd
pkg_check_modules(NL3 REQUIRED libnl-3.0 libnl-genl-3.0)
add_executable(vitastor-nbd   
	nbd_proxy.cpp 
)
target_include_directories(vitastor-nbd PUBLIC ${NL3_INCLUDE_DIRS})
target_link_libraries(vitastor-nbd vitastor_client ${NL3_LIBRARIES})
if(HAVE_NBD_NETLINK_H)
	target_compile_definitions(vitastor-nbd PUBLIC HAVE_NBD_NETLINK_H)
endif()

# vitastor-nfs
add_executable(vitastor-nfs
	nfs_proxy.cpp
	nfs_conn.cpp
	nfs_portmap.cpp
	sha256.c
	nfs/xdr_impl.cpp
	nfs/rpc_xdr.cpp
	nfs/portmap_xdr.cpp
	nfs/nfs_xdr.cpp
)
target_link_libraries(vitastor-nfs
	vitastor_client
)

# vitastor-cli
add_executable(vitastor-cli
	cli.cpp
)
target_link_libraries(vitastor-cli
	vitastor_client
)
configure_file(vitastor.pc.in vitastor.pc @ONLY)

# vitastor-disk
add_executable(vitastor-disk
	disk_tool.cpp disk_simple_offsets.cpp
	disk_tool_journal.cpp disk_tool_meta.cpp disk_tool_prepare.cpp disk_tool_resize.cpp disk_tool_udev.cpp disk_tool_utils.cpp disk_tool_upgrade.cpp
	crc32c.c str_util.cpp ../json11/json11.cpp rw_blocking.cpp allocator.cpp ringloop.cpp blockstore_disk.cpp
)
target_link_libraries(vitastor-disk
	tcmalloc_minimal
	${LIBURING_LIBRARIES}
)

if (${WITH_QEMU})
	# qemu_driver.so
	add_library(qemu_vitastor SHARED
		qemu_driver.c
	)
	target_compile_options(qemu_vitastor PUBLIC -DVITASTOR_SOURCE_TREE)
	target_include_directories(qemu_vitastor PUBLIC
		../qemu/b/qemu
		../qemu/include
		${GLIB_INCLUDE_DIRS}
	)
	target_link_libraries(qemu_vitastor
		vitastor_client
	)
	set_target_properties(qemu_vitastor PROPERTIES
		PREFIX ""
		OUTPUT_NAME "block-vitastor"
	)
endif (${WITH_QEMU})

### Test stubs

# stub_osd, stub_bench, osd_test
add_executable(stub_osd stub_osd.cpp rw_blocking.cpp addr_util.cpp)
target_link_libraries(stub_osd tcmalloc_minimal)
add_executable(stub_bench stub_bench.cpp rw_blocking.cpp addr_util.cpp)
target_link_libraries(stub_bench tcmalloc_minimal)
add_executable(osd_test osd_test.cpp rw_blocking.cpp addr_util.cpp)
target_link_libraries(osd_test tcmalloc_minimal)

# osd_rmw_test
add_executable(osd_rmw_test EXCLUDE_FROM_ALL osd_rmw_test.cpp allocator.cpp)
target_link_libraries(osd_rmw_test Jerasure ${ISAL_LIBRARIES} tcmalloc_minimal)
add_dependencies(build_tests osd_rmw_test)
add_test(NAME osd_rmw_test COMMAND osd_rmw_test)

if (ISAL_LIBRARIES)
	add_executable(osd_rmw_test_je EXCLUDE_FROM_ALL osd_rmw_test.cpp allocator.cpp)
	target_compile_definitions(osd_rmw_test_je PUBLIC -DNO_ISAL)
	target_link_libraries(osd_rmw_test_je Jerasure tcmalloc_minimal)
	add_dependencies(build_tests osd_rmw_test_je)
	add_test(NAME osd_rmw_test_jerasure COMMAND osd_rmw_test_je)
endif (ISAL_LIBRARIES)

# stub_uring_osd
add_executable(stub_uring_osd
	stub_uring_osd.cpp
)
target_link_libraries(stub_uring_osd
	vitastor_common
	${LIBURING_LIBRARIES}
	${IBVERBS_LIBRARIES}
	tcmalloc_minimal
)

# osd_peering_pg_test
add_executable(osd_peering_pg_test EXCLUDE_FROM_ALL osd_peering_pg_test.cpp osd_peering_pg.cpp)
target_link_libraries(osd_peering_pg_test tcmalloc_minimal)
add_dependencies(build_tests osd_peering_pg_test)
add_test(NAME osd_peering_pg_test COMMAND osd_peering_pg_test)

# test_allocator
add_executable(test_allocator EXCLUDE_FROM_ALL test_allocator.cpp allocator.cpp)
add_dependencies(build_tests test_allocator)
add_test(NAME test_allocator COMMAND test_allocator)

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

# test_cluster_client
add_executable(test_cluster_client
	EXCLUDE_FROM_ALL
	test_cluster_client.cpp
	pg_states.cpp osd_ops.cpp cluster_client.cpp cluster_client_list.cpp cluster_client_wb.cpp msgr_op.cpp mock/messenger.cpp msgr_stop.cpp
	etcd_state_client.cpp timerfd_manager.cpp str_util.cpp ../json11/json11.cpp
)
target_compile_definitions(test_cluster_client PUBLIC -D__MOCK__)
target_include_directories(test_cluster_client PUBLIC ${CMAKE_SOURCE_DIR}/src/mock)
add_dependencies(build_tests test_cluster_client)
add_test(NAME test_cluster_client COMMAND test_cluster_client)

## test_blockstore, test_shit
#add_executable(test_blockstore test_blockstore.cpp)
#target_link_libraries(test_blockstore blockstore)
#add_executable(test_shit test_shit.cpp osd_peering_pg.cpp)
#target_link_libraries(test_shit ${LIBURING_LIBRARIES} m)

### Install

install(TARGETS vitastor-osd vitastor-disk vitastor-nbd vitastor-nfs vitastor-cli RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
install_symlink(vitastor-disk ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_BINDIR}/vitastor-dump-journal)
install_symlink(vitastor-cli ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_BINDIR}/vitastor-rm)
install_symlink(vitastor-cli ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_BINDIR}/vita)
install(
	TARGETS vitastor_blk vitastor_client
	LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
	PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/vitastor.pc DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)
if (${WITH_FIO})
	install(TARGETS fio_vitastor fio_vitastor_blk fio_vitastor_sec LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif (${WITH_FIO})
if (${WITH_QEMU})
	install(TARGETS qemu_vitastor LIBRARY DESTINATION /usr/${CMAKE_INSTALL_LIBDIR}/${QEMU_PLUGINDIR})
endif (${WITH_QEMU})
