cmake_minimum_required(VERSION 2.8...3.30)

project(vitastor)

set(OPENAPI_JSON_H "${CMAKE_CURRENT_BINARY_DIR}/openapi.json.h")
add_custom_command(
	OUTPUT ${OPENAPI_JSON_H}
	COMMAND ${CMAKE_COMMAND} -E echo const char* openapi_description = R\\\"json\\\( > ${OPENAPI_JSON_H}
	COMMAND ${CMAKE_COMMAND} -E cat ${CMAKE_CURRENT_SOURCE_DIR}/openapi.json >> ${OPENAPI_JSON_H}
	COMMAND ${CMAKE_COMMAND} -E echo "\\)json\\\"\\;" >> ${OPENAPI_JSON_H}
	DEPENDS openapi.json
)

# libvitastor_cli.a
add_library(vitastor_cli STATIC
	cli_common.cpp
	cli_alloc_osd.cpp
	cli_cpubench.cpp
	cli_describe.cpp
	cli_fix.cpp
	cli_ls.cpp
	cli_create.cpp
	cli_dd.cpp
	cli_modify.cpp
	cli_modify_osd.cpp
	cli_modify_user.cpp
	cli_osd_tree.cpp
	cli_pg_ls.cpp
	cli_flatten.cpp
	cli_merge.cpp
	cli_rm_data.cpp
	cli_rm_wildcard.cpp
	cli_rm.cpp
	cli_rm_osd.cpp
	cli_pool_cfg.cpp
	cli_pool_create.cpp
	cli_pool_ls.cpp
	cli_pool_modify.cpp
	cli_pool_rm.cpp
	cli_raw_ls.cpp
	cli_serve.cpp
	cli_status.cpp
	cli_user_ls.cpp
	${OPENAPI_JSON_H}
)
target_compile_options(vitastor_cli PUBLIC -fPIC)
target_include_directories(vitastor_cli PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

# vitastor-cli
add_executable(vitastor-cli
	cli.cpp
)
target_link_libraries(vitastor-cli
	vitastor_client_int
)
