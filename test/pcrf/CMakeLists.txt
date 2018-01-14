set(TARGET pcrf_test)
project(${TARGET})

add_executable(${TARGET} pcrf_test.c)

target_link_libraries(${TARGET} mdbx)

