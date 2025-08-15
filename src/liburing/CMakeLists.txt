cmake_minimum_required(VERSION 2.8.12)

project(vitastor)

# liburing.a
add_library(uring STATIC
	queue.c register.c setup.c syscall.c version.c
)
target_compile_options(uring PUBLIC -fPIC)
target_compile_definitions(uring PUBLIC _GNU_SOURCE _LARGEFILE_SOURCE _FILE_OFFSET_BITS=64)
