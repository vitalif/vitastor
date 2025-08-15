cmake_minimum_required(VERSION 2.8.12)

project(vitastor)

include(GNUInstallDirs)
include(CTest)
include(CheckIncludeFile)

find_package(PkgConfig)

set(WITH_QEMU false CACHE BOOL "Build QEMU driver inside Vitastor source tree")
set(WITH_FIO true CACHE BOOL "Build FIO driver")
set(QEMU_PLUGINDIR qemu CACHE STRING "QEMU plugin directory suffix (qemu-kvm on RHEL)")
set(WITH_ASAN false CACHE BOOL "Build with AddressSanitizer")
set(WITH_SYSTEM_LIBURING false CACHE BOOL "Use system liburing")
if("${CMAKE_INSTALL_PREFIX}" MATCHES "^/usr/local/?$")
	if(EXISTS "/etc/debian_version")
		set(CMAKE_INSTALL_LIBDIR "lib/${CMAKE_LIBRARY_ARCHITECTURE}")
	endif()
	set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}")
endif()

add_definitions(-DVITASTOR_VERSION="2.2.3")
add_definitions(-D_GNU_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -Wall -Wno-sign-compare -Wno-comment -Wno-parentheses -Wno-pointer-arith -fdiagnostics-color=always -fno-omit-frame-pointer -I ${CMAKE_SOURCE_DIR}/src)
add_link_options(-fno-omit-frame-pointer)
if (${WITH_ASAN})
	add_definitions(-fsanitize=address)
	add_link_options(-fsanitize=address -fno-omit-frame-pointer)
endif (${WITH_ASAN})

set(CMAKE_BUILD_TYPE RelWithDebInfo)
string(REGEX REPLACE "([\\/\\-]O)[^ \t\r\n]*" "\\13" CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")
string(REGEX REPLACE "([\\/\\-]O)[^ \t\r\n]*" "\\13" CMAKE_CXX_FLAGS_MINSIZEREL "${CMAKE_CXX_FLAGS_MINSIZEREL}")
string(REGEX REPLACE "([\\/\\-]O)[^ \t\r\n]*" "\\13" CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO}")
string(REGEX REPLACE "([\\/\\-]D) *NDEBUG" "" CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")
string(REGEX REPLACE "([\\/\\-]D) *NDEBUG" "" CMAKE_CXX_FLAGS_MINSIZEREL "${CMAKE_CXX_FLAGS_MINSIZEREL}")
string(REGEX REPLACE "([\\/\\-]D) *NDEBUG" "" CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO}")
string(REGEX REPLACE "([\\/\\-]O)[^ \t\r\n]*" "\\13" CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE}")
string(REGEX REPLACE "([\\/\\-]O)[^ \t\r\n]*" "\\13" CMAKE_C_FLAGS_MINSIZEREL "${CMAKE_C_FLAGS_MINSIZEREL}")
string(REGEX REPLACE "([\\/\\-]O)[^ \t\r\n]*" "\\13" CMAKE_C_FLAGS_RELWITHDEBINFO "${CMAKE_C_FLAGS_RELWITHDEBINFO}")
string(REGEX REPLACE "([\\/\\-]D) *NDEBUG" "" CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE}")
string(REGEX REPLACE "([\\/\\-]D) *NDEBUG" "" CMAKE_C_FLAGS_MINSIZEREL "${CMAKE_C_FLAGS_MINSIZEREL}")
string(REGEX REPLACE "([\\/\\-]D) *NDEBUG" "" CMAKE_C_FLAGS_RELWITHDEBINFO "${CMAKE_C_FLAGS_RELWITHDEBINFO}")

macro(install_symlink filepath sympath)
	install(CODE "execute_process(COMMAND ${CMAKE_COMMAND} -E create_symlink ${filepath} \$ENV{DESTDIR}${sympath})")
	install(CODE "message(\"-- Created symlink: ${sympath} -> ${filepath}\")")
endmacro(install_symlink)

check_include_file("linux/nbd-netlink.h" HAVE_NBD_NETLINK_H)

find_package(PkgConfig)
if (${WITH_QEMU})
	pkg_check_modules(GLIB REQUIRED glib-2.0)
endif (${WITH_QEMU})
pkg_check_modules(IBVERBS libibverbs)
if (IBVERBS_LIBRARIES)
	add_definitions(-DWITH_RDMA)
endif (IBVERBS_LIBRARIES)
pkg_check_modules(ISAL libisal)
if (ISAL_LIBRARIES)
	add_definitions(-DWITH_ISAL)
endif (ISAL_LIBRARIES)
pkg_check_modules(RDMACM librdmacm)
if (RDMACM_LIBRARIES)
	add_definitions(-DWITH_RDMACM)
endif (RDMACM_LIBRARIES)

if (${WITH_SYSTEM_LIBURING})
	pkg_check_modules(LIBURING REQUIRED liburing>=2.10)
	include_directories(${LIBURING_INCLUDE_DIRS})
else()
	include_directories(${CMAKE_SOURCE_DIR}/src/liburing/include)
	add_subdirectory(liburing)
	set(LIBURING_LIBRARIES uring)
endif (${WITH_SYSTEM_LIBURING})

add_custom_target(build_tests)
add_custom_target(test
	COMMAND
	echo leak:tcmalloc > ${CMAKE_CURRENT_BINARY_DIR}/lsan-suppress.txt &&
	env LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_BINARY_DIR}/lsan-suppress.txt ${CMAKE_CTEST_COMMAND}
)
add_dependencies(test build_tests)

include_directories(
	../
	${CMAKE_SOURCE_DIR}/src/blockstore
	${CMAKE_SOURCE_DIR}/src/cmd
	${CMAKE_SOURCE_DIR}/src/client
	${CMAKE_SOURCE_DIR}/src/disk_tool
	${CMAKE_SOURCE_DIR}/src/kv
	${CMAKE_SOURCE_DIR}/src/nfs
	${CMAKE_SOURCE_DIR}/src/osd
	${CMAKE_SOURCE_DIR}/src/test
	${CMAKE_SOURCE_DIR}/src/util
	/usr/include/jerasure
	${IBVERBS_INCLUDE_DIRS}
)

add_subdirectory(blockstore)
add_subdirectory(cmd)
add_subdirectory(client)
add_subdirectory(disk_tool)
add_subdirectory(kv)
add_subdirectory(nfs)
add_subdirectory(osd)
add_subdirectory(test)

### Install

install(TARGETS vitastor-osd vitastor-disk vitastor-nbd vitastor-nfs vitastor-cli vitastor-kv vitastor-kv-stress RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
install_symlink(vitastor-disk ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_BINDIR}/vitastor-dump-journal)
install_symlink(vitastor-cli ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_BINDIR}/vitastor-rm)
install_symlink(vitastor-cli ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_BINDIR}/vita)
install(
	TARGETS vitastor_blk vitastor_client vitastor_kv
	LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
	PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/client/vitastor.pc DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)
if (${WITH_FIO})
	install(TARGETS fio_vitastor fio_vitastor_blk fio_vitastor_sec LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif (${WITH_FIO})
if (${WITH_QEMU})
	install(TARGETS qemu_vitastor LIBRARY DESTINATION /usr/${CMAKE_INSTALL_LIBDIR}/${QEMU_PLUGINDIR})
endif (${WITH_QEMU})
