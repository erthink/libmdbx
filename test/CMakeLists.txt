add_executable(mdbx_test
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

set_target_properties(mdbx_test PROPERTIES
  INTERPROCEDURAL_OPTIMIZATION $<BOOL:${INTERPROCEDURAL_OPTIMIZATION}>
  CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON)
target_setup_options(mdbx_test)

target_link_libraries(mdbx_test ${TOOL_MDBX_LIB} ${LIB_MATH} ${CMAKE_THREAD_LIBS_INIT})
if(${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
  target_link_libraries(mdbx_test winmm.lib)
endif()

if(UNIX AND NOT SUBPROJECT)
  add_executable(pcrf_test pcrf/pcrf_test.c)
  target_include_directories(pcrf_test PRIVATE "${PROJECT_SOURCE_DIR}")
  target_link_libraries(pcrf_test ${TOOL_MDBX_LIB})
endif()
