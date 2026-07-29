cmake_minimum_required(VERSION 2.8...3.30)

project(vitastor)

# vitastor-disk
add_executable(vitastor-disk
	disk_simple_offsets.cpp
	disk_tool.cpp
	disk_tool_discard.cpp
	disk_tool.h
	disk_tool_journal.cpp
	disk_tool_meta.cpp
	disk_tool_prepare.cpp
	disk_tool_resize_auto.cpp
	disk_tool_resize.cpp
	disk_tool_udev.cpp
	disk_tool_upgrade.cpp
	disk_tool_utils.cpp
	../blockstore/blockstore_disk.cpp
	../blockstore/blockstore_heap.cpp
	../blockstore/multilist.cpp
)
target_link_libraries(vitastor-disk
	${LIBURING_LIBRARIES}
	# for crc32_iscsi
	${ISAL_LIBRARIES}
	util
)
