cmake_minimum_required(VERSION 2.8...3.30)

project(vitastor)

# libosd
add_library(osd STATIC
	osd.cpp osd_secondary.cpp osd_peering.cpp osd_flush.cpp osd_peering_pg.cpp
	osd_primary.cpp osd_primary_chain.cpp osd_primary_sync.cpp osd_primary_write.cpp osd_primary_subops.cpp
	osd_cluster.cpp osd_rmw.cpp osd_scrub.cpp osd_primary_describe.cpp
)
target_link_libraries(osd pthread)
target_compile_options(osd PUBLIC -fPIC)

# vitastor-osd
add_executable(vitastor-osd
	osd_main.cpp
)
target_link_libraries(vitastor-osd
	osd
	vitastor_net
	vitastor_blk
	Jerasure
	${ISAL_LIBRARIES}
	${IBVERBS_LIBRARIES}
	${RDMACM_LIBRARIES}
	${OPENSSL_LIBRARIES}
)

# osd_test
add_executable(osd_test
	EXCLUDE_FROM_ALL
	../test/osd_test.cpp
	../client/etcd_state_client_mock.cpp
	../blockstore/blockstore_mock.cpp
	../test/mock/messenger.cpp
	../test/ringloop_mock.cpp
)
target_link_libraries(osd_test
	osd
	vitastor_common
	Jerasure
	${ISAL_LIBRARIES}
	${LIBURING_LIBRARIES}
)
add_test(NAME osd_test COMMAND osd_test)
add_dependencies(build_tests osd_test)

# osd_rmw_test
add_executable(osd_rmw_test EXCLUDE_FROM_ALL osd_rmw_test.cpp ../util/allocator.cpp)
target_link_libraries(osd_rmw_test Jerasure ${ISAL_LIBRARIES})
add_dependencies(build_tests osd_rmw_test)
add_test(NAME osd_rmw_test COMMAND osd_rmw_test)

if (ISAL_LIBRARIES)
	add_executable(osd_rmw_test_je EXCLUDE_FROM_ALL osd_rmw_test.cpp ../util/allocator.cpp)
	target_compile_definitions(osd_rmw_test_je PUBLIC -DNO_ISAL)
	target_link_libraries(osd_rmw_test_je Jerasure)
	add_dependencies(build_tests osd_rmw_test_je)
	add_test(NAME osd_rmw_test_jerasure COMMAND osd_rmw_test_je)
endif (ISAL_LIBRARIES)

# osd_peering_pg_test
add_executable(osd_peering_pg_test EXCLUDE_FROM_ALL osd_peering_pg_test.cpp osd_peering_pg.cpp)
add_dependencies(build_tests osd_peering_pg_test)
add_test(NAME osd_peering_pg_test COMMAND osd_peering_pg_test)
