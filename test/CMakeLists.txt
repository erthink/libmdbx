enable_language(CXX)
include(../cmake/compiler.cmake)

set(LIBMDBX_TEST_SOURCES
  base.h
  cases.cc
  chrono.cc
  chrono.h
  config.cc
  config.h
  copy.cc
  dead.cc
  hill.cc
  jitter.cc
  keygen.cc
  keygen.h
  log.cc
  log.h
  main.cc
  osal.h
  osal-unix.cc
  osal-windows.cc
  test.cc
  test.h
  try.cc
  utils.cc
  utils.h
  append.cc
  ttl.cc
  nested.cc
  )

if(NOT MDBX_BUILD_CXX)
  probe_libcxx_filesystem()
  list(APPEND LIBMDBX_TEST_SOURCES "${MDBX_SOURCE_DIR}/mdbx.c++" ../mdbx.h++)
endif()

add_executable(mdbx_test ${LIBMDBX_TEST_SOURCES})

if(MDBX_CXX_STANDARD)
  set_target_properties(mdbx_test PROPERTIES
    CXX_STANDARD ${MDBX_CXX_STANDARD} CXX_STANDARD_REQUIRED ON)
endif()

set_target_properties(mdbx_test PROPERTIES
  INTERPROCEDURAL_OPTIMIZATION $<BOOL:${INTERPROCEDURAL_OPTIMIZATION}>)
target_setup_options(mdbx_test)

if(NOT MDBX_BUILD_CXX AND LIBCXX_FILESYSTEM)
  if(CMAKE_COMPILER_IS_ELBRUSCXX AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 1.26.0
      AND NOT CMAKE_VERSION VERSION_LESS 3.13)
    target_link_options(mdbx_test PRIVATE "-Wl,--allow-multiple-definition")
  endif()
  target_link_libraries(mdbx_test ${LIBCXX_FILESYSTEM})
endif()

target_link_libraries(mdbx_test ${TOOL_MDBX_LIB} ${LIB_MATH} ${CMAKE_THREAD_LIBS_INIT})
if(${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
  target_link_libraries(mdbx_test winmm.lib)
endif()

if(UNIX AND NOT SUBPROJECT)
  add_executable(pcrf_test pcrf/pcrf_test.c)
  target_include_directories(pcrf_test PRIVATE "${PROJECT_SOURCE_DIR}")
  target_link_libraries(pcrf_test ${TOOL_MDBX_LIB})
endif()
