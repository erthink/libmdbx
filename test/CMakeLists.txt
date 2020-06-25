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

list(FIND CMAKE_CXX_COMPILE_FEATURES cxx_std_20 HAS_CXX20)
list(FIND CMAKE_CXX_COMPILE_FEATURES cxx_std_17 HAS_CXX17)
list(FIND CMAKE_CXX_COMPILE_FEATURES cxx_std_14 HAS_CXX14)
list(FIND CMAKE_CXX_COMPILE_FEATURES cxx_std_11 HAS_CXX11)
if(NOT DEFINED MDBX_CXX_STANDARD)
  if(DEFINED CMAKE_CXX_STANDARD)
    set(MDBX_CXX_STANDARD ${CMAKE_CXX_STANDARD})
  elseif(NOT HAS_CXX20 LESS 0)
    set(MDBX_CXX_STANDARD 20)
  elseif(NOT HAS_CXX17 LESS 0)
    set(MDBX_CXX_STANDARD 17)
  elseif(NOT HAS_CXX14 LESS 0)
    set(MDBX_CXX_STANDARD 14)
  elseif(NOT HAS_CXX11 LESS 0)
    set(MDBX_CXX_STANDARD 11)
  endif()
endif()
if(MDBX_CXX_STANDARD)
  message(STATUS "Use C++${MDBX_CXX_STANDARD} for libmdbx")
  if(NOT SUBPROJECT OR NOT DEFINED CMAKE_CXX_STANDARD)
    set(CMAKE_CXX_STANDARD ${MDBX_CXX_STANDARD})
  endif()
endif()

if(MDBX_CXX_STANDARD)
  set_target_properties(mdbx_test PROPERTIES
    CXX_STANDARD ${MDBX_CXX_STANDARD} CXX_STANDARD_REQUIRED ON)
endif()

set_target_properties(mdbx_test PROPERTIES
  INTERPROCEDURAL_OPTIMIZATION $<BOOL:${INTERPROCEDURAL_OPTIMIZATION}>)
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
