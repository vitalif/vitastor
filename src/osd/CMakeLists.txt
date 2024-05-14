cmake_minimum_required(VERSION 2.8.12)

project(vitastor)

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

# osd_rmw_test
add_executable(osd_rmw_test EXCLUDE_FROM_ALL osd_rmw_test.cpp ../util/allocator.cpp)
target_link_libraries(osd_rmw_test Jerasure ${ISAL_LIBRARIES} tcmalloc_minimal)
add_dependencies(build_tests osd_rmw_test)
add_test(NAME osd_rmw_test COMMAND osd_rmw_test)

if (ISAL_LIBRARIES)
	add_executable(osd_rmw_test_je EXCLUDE_FROM_ALL osd_rmw_test.cpp ../util/allocator.cpp)
	target_compile_definitions(osd_rmw_test_je PUBLIC -DNO_ISAL)
	target_link_libraries(osd_rmw_test_je Jerasure tcmalloc_minimal)
	add_dependencies(build_tests osd_rmw_test_je)
	add_test(NAME osd_rmw_test_jerasure COMMAND osd_rmw_test_je)
endif (ISAL_LIBRARIES)

# osd_peering_pg_test
add_executable(osd_peering_pg_test EXCLUDE_FROM_ALL osd_peering_pg_test.cpp osd_peering_pg.cpp)
target_link_libraries(osd_peering_pg_test tcmalloc_minimal)
add_dependencies(build_tests osd_peering_pg_test)
add_test(NAME osd_peering_pg_test COMMAND osd_peering_pg_test)
