set(TARGET mdbx_tutorial)
project(${TARGET})

add_executable(${TARGET} sample-mdbx.c)

target_link_libraries(${TARGET} mdbx)

