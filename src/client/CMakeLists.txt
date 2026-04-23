cmake_minimum_required(VERSION 2.8...3.30)

project(vitastor)

# libvitastor_common.a
add_library(vitastor_common STATIC
	etcd_state_client.cpp
	msgr_stop.cpp
	msgr_op.cpp
	../../json11/json11.cpp
	osd_ops.cpp
	pg_states.cpp
	msgr_encrypt.cpp
	../util/allocator.cpp
	../util/addr_util.cpp
	../util/timerfd_manager.cpp
	../util/str_util.cpp
	../util/json_util.cpp
	../util/xxh_x86dispatch.c
	 ../util/openssl_util.cpp
)
target_compile_options(vitastor_common PUBLIC -fPIC)
target_link_libraries(vitastor_common ${OPENSSL_LIBRARIES} ${ISAL_CRYPTO_LIBRARIES})

# libvitastor_net.a
set(MSGR_RDMA "")
if (IBVERBS_LIBRARIES)
	set(MSGR_RDMA "msgr_rdma.cpp")
endif (IBVERBS_LIBRARIES)
set(MSGR_RDMACM "")
if (RDMACM_LIBRARIES)
	set(MSGR_RDMACM "msgr_rdmacm.cpp")
endif (RDMACM_LIBRARIES)
add_library(vitastor_net STATIC
	../util/epoll_manager.cpp
	etcd_state_client_http.cpp
	messenger.cpp
	msgr_iothread.cpp
	msgr_send.cpp
	msgr_receive.cpp
	msgr_encrypt.cpp
	../util/ringloop.cpp
	http_client.cpp
	${MSGR_RDMA}
	${MSGR_RDMACM}
)
target_link_libraries(vitastor_net pthread vitastor_common ${CARES_LIBRARIES})
target_compile_options(vitastor_net PUBLIC -fPIC)

# libvitastor_client.so
add_library(vitastor_client SHARED
	cluster_client.cpp
	cluster_client_real.cpp
	cluster_client_list.cpp
	cluster_client_wb.cpp
	cluster_client_icache.cpp
	vitastor_c.cpp
)
set_target_properties(vitastor_client PROPERTIES PUBLIC_HEADER "client/vitastor_c.h")
target_link_libraries(vitastor_client
	vitastor_net
	vitastor_cli
	${LIBURING_LIBRARIES}
	${IBVERBS_LIBRARIES}
	${RDMACM_LIBRARIES}
	${OPENSSL_LIBRARIES}
	${ISAL_CRYPTO_LIBRARIES}
)
set_target_properties(vitastor_client PROPERTIES VERSION ${VITASTOR_VERSION} SOVERSION 0)
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

# vitastor-ublk
add_executable(vitastor-ublk
	ublk_server.cpp
)
target_link_libraries(vitastor-ublk vitastor_client)

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
	cluster_client.cpp
	cluster_client_list.cpp
	cluster_client_wb.cpp
	cluster_client_icache.cpp
	../test/mock/messenger.cpp
	../test/mock/vault.cpp
	etcd_state_client_mock.cpp
)
target_link_libraries(test_cluster_client vitastor_common ${LIBURING_LIBRARIES} ${OPENSSL_LIBRARIES} ${ISAL_CRYPTO_LIBRARIES})
target_include_directories(test_cluster_client BEFORE PUBLIC ${CMAKE_SOURCE_DIR}/src/test/mock)
add_dependencies(build_tests test_cluster_client)
add_test(NAME test_cluster_client COMMAND test_cluster_client)
