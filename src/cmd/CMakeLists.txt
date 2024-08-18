cmake_minimum_required(VERSION 2.8.12)

project(vitastor)

# libvitastor_cli.a
add_library(vitastor_cli STATIC
	cli_common.cpp
	cli_alloc_osd.cpp
	cli_status.cpp
	cli_describe.cpp
	cli_fix.cpp
	cli_ls.cpp
	cli_create.cpp
	cli_dd.cpp
	cli_modify.cpp
	cli_modify_osd.cpp
	cli_osd_tree.cpp
	cli_pg_ls.cpp
	cli_flatten.cpp
	cli_merge.cpp
	cli_rm_data.cpp
	cli_rm.cpp
	cli_rm_osd.cpp
	cli_pool_cfg.cpp
	cli_pool_create.cpp
	cli_pool_ls.cpp
	cli_pool_modify.cpp
	cli_pool_rm.cpp
)
target_compile_options(vitastor_cli PUBLIC -fPIC)

# vitastor-cli
add_executable(vitastor-cli
	cli.cpp
)
target_link_libraries(vitastor-cli
	vitastor_client
)
