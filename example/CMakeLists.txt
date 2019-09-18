set(TARGET mdbx_example)
project(${TARGET})

add_executable(${TARGET} example-mdbx.c)

target_link_libraries(${TARGET} mdbx)
