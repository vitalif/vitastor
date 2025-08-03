cmake_minimum_required(VERSION 2.8.12)

project(vitastor)

set(VITASTOR_VERSION "2.4.4")

include(CTest)

add_custom_target(build_tests)
add_custom_target(test
	COMMAND
	echo leak:tcmalloc > ${CMAKE_CURRENT_BINARY_DIR}/lsan-suppress.txt &&
	env LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_BINARY_DIR}/lsan-suppress.txt ${CMAKE_CTEST_COMMAND}
)
# make -j16 -C ../../build test_heap && ../../build/src/test/test_heap
# make -j16 -C ../../build test_heap && rm -f $(find ../../build -name '*.gcda') && ctest -V -T test -T coverage -R heap --test-dir ../../build && (cd ../../build; gcovr -f ../src --html --html-nested -o coverage/index.html; cd ../src/test)
# make -j16 -C ../../build test_blockstore && rm -f $(find ../../build -name '*.gcda') && ctest -V -T test -T coverage -R blockstore --test-dir ../../build && (cd ../../build; gcovr -f ../src --html --html-nested -o coverage/index.html; cd ../src/test)
# kcov --include-path=../../../src ../../kcov ./test_blockstore
add_dependencies(test build_tests)
add_subdirectory(src)
