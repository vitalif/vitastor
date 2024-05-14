cmake_minimum_required(VERSION 2.8.12)

project(vitastor)

# libvitastor_common.a
set(MSGR_RDMA "")
if (IBVERBS_LIBRARIES)
	set(MSGR_RDMA "msgr_rdma.cpp")
endif (IBVERBS_LIBRARIES)
add_library(vitastor_common STATIC
	../util/epoll_manager.cpp etcd_state_client.cpp messenger.cpp ../util/addr_util.cpp
	msgr_stop.cpp msgr_op.cpp msgr_send.cpp msgr_receive.cpp ../util/ringloop.cpp ../../json11/json11.cpp
	http_client.cpp osd_ops.cpp pg_states.cpp ../util/timerfd_manager.cpp ../util/str_util.cpp ${MSGR_RDMA}
)
target_compile_options(vitastor_common PUBLIC -fPIC)

# libvitastor_client.so
add_library(vitastor_client SHARED
	cluster_client.cpp
	cluster_client_list.cpp
	cluster_client_wb.cpp
	vitastor_c.cpp
)
set_target_properties(vitastor_client PROPERTIES PUBLIC_HEADER "vitastor_c.h")
target_link_libraries(vitastor_client
	vitastor_common
	vitastor_cli
	${LIBURING_LIBRARIES}
	${IBVERBS_LIBRARIES}
)
set_target_properties(vitastor_client PROPERTIES VERSION ${VERSION} SOVERSION 0)
configure_file(vitastor.pc.in vitastor.pc @ONLY)

if (${WITH_FIO})
	# libfio_vitastor.so
	add_library(fio_vitastor SHARED
		fio_cluster.cpp
	)
	target_link_libraries(fio_vitastor
		vitastor_client
	)

	# libfio_vitastor_sec.so
	add_library(fio_vitastor_sec SHARED
		fio_sec_osd.cpp
		../util/rw_blocking.cpp
		../util/addr_util.cpp
	)
	target_link_libraries(fio_vitastor_sec
		tcmalloc_minimal
	)
endif (${WITH_FIO})

# vitastor-nbd
pkg_check_modules(NL3 libnl-3.0 libnl-genl-3.0)
add_executable(vitastor-nbd
	nbd_proxy.cpp
)
target_include_directories(vitastor-nbd PUBLIC ${NL3_INCLUDE_DIRS})
target_link_libraries(vitastor-nbd vitastor_client ${NL3_LIBRARIES})
if (HAVE_NBD_NETLINK_H AND NL3_LIBRARIES)
	target_compile_definitions(vitastor-nbd PUBLIC HAVE_NBD_NETLINK_H)
endif (HAVE_NBD_NETLINK_H AND NL3_LIBRARIES)

if (${WITH_QEMU})
	# qemu_driver.so
	add_library(qemu_vitastor SHARED
		qemu_driver.c
	)
	target_compile_options(qemu_vitastor PUBLIC -DVITASTOR_SOURCE_TREE)
	target_include_directories(qemu_vitastor PUBLIC
		../../qemu/b/qemu
		../../qemu/include
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

# test_cluster_client
add_executable(test_cluster_client
	EXCLUDE_FROM_ALL
	../test/test_cluster_client.cpp
	pg_states.cpp osd_ops.cpp cluster_client.cpp cluster_client_list.cpp cluster_client_wb.cpp msgr_op.cpp ../test/mock/messenger.cpp msgr_stop.cpp
	etcd_state_client.cpp ../util/timerfd_manager.cpp ../util/str_util.cpp ../../json11/json11.cpp
)
target_compile_definitions(test_cluster_client PUBLIC -D__MOCK__)
target_include_directories(test_cluster_client BEFORE PUBLIC ${CMAKE_SOURCE_DIR}/src/test/mock)
add_dependencies(build_tests test_cluster_client)
add_test(NAME test_cluster_client COMMAND test_cluster_client)
