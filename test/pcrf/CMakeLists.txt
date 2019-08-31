set(TARGET pcrf_test)
add_executable(${TARGET} pcrf_test.c)
target_include_directories(${TARGET} PRIVATE "${PROJECT_SOURCE_DIR}")
target_link_libraries(${TARGET} mdbx)

