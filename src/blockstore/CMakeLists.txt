cmake_minimum_required(VERSION 2.8.12)

project(vitastor)

# libvitastor_blk.so
add_library(vitastor_blk SHARED
	../util/allocator.cpp blockstore.cpp blockstore_impl.cpp blockstore_disk.cpp blockstore_init.cpp blockstore_open.cpp blockstore_journal.cpp blockstore_read.cpp
	blockstore_write.cpp blockstore_sync.cpp blockstore_stable.cpp blockstore_rollback.cpp blockstore_flush.cpp ../util/crc32c.c ../util/ringloop.cpp
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
		../../json11/json11.cpp
	)
	target_link_libraries(fio_vitastor_blk
		vitastor_blk
	)
endif (${WITH_FIO})
