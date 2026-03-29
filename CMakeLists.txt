cmake_minimum_required(VERSION 2.8...3.30)

project(vitastor)

set(VITASTOR_VERSION "3.0.8")

include(CTest)

add_custom_target(build_tests)
set_property(TEST PROPERTY ENVIRONMENT LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_BINARY_DIR}/lsan-suppress.txt)
add_test(gen_lsan_suppress
    ${CMAKE_COMMAND} -E echo leak:tcmalloc > "${CMAKE_CURRENT_BINARY_DIR}/lsan-suppress.txt"
)
set_tests_properties(gen_lsan_suppress PROPERTIES FIXTURES_SETUP f_lsan_suppress)
set_property(TEST PROPERTY FIXTURES_REQUIRED f_lsan_suppress)
# make -j16 -C ../../build test_heap && ../../build/src/test/test_heap
# make -j16 -C ../../build test_heap && rm -f $(find ../../build -name '*.gcda') && ctest -V -T test -T coverage -R heap --test-dir ../../build && (cd ../../build; gcovr -f ../src --html --html-nested -o coverage/index.html; cd ../src/test)
# make -j16 -C ../../build test_blockstore && rm -f $(find ../../build -name '*.gcda') && ctest -V -T test -T coverage -R blockstore --test-dir ../../build && (cd ../../build; gcovr -f ../src --html --html-nested -o coverage/index.html; cd ../src/test)
# kcov --include-path=../../../src ../../kcov ./test_blockstore
add_subdirectory(src)
