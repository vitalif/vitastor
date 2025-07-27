cmake_minimum_required(VERSION 2.8.12)

project(vitastor)

set(VITASTOR_VERSION "2.4.4")

include(CTest)

add_subdirectory(src)
add_dependencies(test build_tests)
