cmake_minimum_required(VERSION 2.8.12)

project(vitastor)

# vitastor-disk
add_executable(vitastor-disk
	disk_tool.cpp disk_simple_offsets.cpp
	disk_tool_discard.cpp disk_tool_journal.cpp disk_tool_meta.cpp disk_tool_prepare.cpp disk_tool_resize.cpp
	disk_tool_resize_auto.cpp disk_tool_udev.cpp disk_tool_utils.cpp disk_tool_upgrade.cpp
	../util/crc32c.c ../util/str_util.cpp ../util/json_util.cpp ../../json11/json11.cpp ../util/rw_blocking.cpp ../util/allocator.cpp ../util/ringloop.cpp ../blockstore/blockstore_disk.cpp
)
target_link_libraries(vitastor-disk
	tcmalloc_minimal
	${LIBURING_LIBRARIES}
)
