cmake_minimum_required(VERSION 2.8.12)

project(vitastor)

# libvitastor_kv.so
add_library(vitastor_kv SHARED
	kv_db.cpp
)
set_target_properties(vitastor_kv PROPERTIES PUBLIC_HEADER "vitastor_kv.h")
target_link_libraries(vitastor_kv
	vitastor_client
)
set_target_properties(vitastor_kv PROPERTIES VERSION ${VERSION} SOVERSION 0)

# vitastor-kv
add_executable(vitastor-kv
	kv_cli.cpp
)
target_link_libraries(vitastor-kv
	vitastor_kv
)

add_executable(vitastor-kv-stress
	kv_stress.cpp
)
target_link_libraries(vitastor-kv-stress
	vitastor_kv
)
