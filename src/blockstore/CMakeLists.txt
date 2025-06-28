cmake_minimum_required(VERSION 2.8.12)

project(vitastor)

# libvitastor_blk.so
add_library(vitastor_blk SHARED
	../util/allocator.cpp ../util/crc32c.c ../util/ringloop.cpp
	blockstore_heap.cpp blockstore_disk.cpp
	blockstore.cpp blockstore_impl.cpp blockstore_init.cpp blockstore_open.cpp
	blockstore_flush.cpp blockstore_read.cpp blockstore_stable.cpp blockstore_sync.cpp blockstore_write.cpp
	v1/flush.cpp v1/impl.cpp v1/init.cpp v1/journal.cpp v1/open.cpp v1/read.cpp v1/rollback.cpp v1/stable.cpp v1/sync.cpp v1/write.cpp
)
target_link_libraries(vitastor_blk
	${LIBURING_LIBRARIES}
	${ISAL_LIBRARIES}
	# for timerfd_manager
	vitastor_common
)
set_target_properties(vitastor_blk PROPERTIES VERSION ${VITASTOR_VERSION} SOVERSION 0)

if (${WITH_FIO})
	# libfio_vitastor_blk.so
	add_library(fio_vitastor_blk SHARED
		fio_engine.cpp
		../../json11/json11.cpp
	)
	target_link_libraries(fio_vitastor_blk
		vitastor_blk
	)
endif (${WITH_FIO})
