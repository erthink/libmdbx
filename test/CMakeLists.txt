list(FIND CMAKE_CXX_COMPILE_FEATURES cxx_std_17 HAS_CXX17)
list(FIND CMAKE_CXX_COMPILE_FEATURES cxx_std_14 HAS_CXX14)
list(FIND CMAKE_CXX_COMPILE_FEATURES cxx_relaxed_constexpr HAS_RELAXED_CONSTEXPR)
if(NOT DEFINED MDBX_CXX_STANDARD)
  if(DEFINED CMAKE_CXX_STANDARD)
    set(MDBX_CXX_STANDARD ${CMAKE_CXX_STANDARD})
  elseif(NOT HAS_CXX17 LESS 0)
    set(MDBX_CXX_STANDARD 17)
  elseif(NOT HAS_CXX14 LESS 0)
    set(MDBX_CXX_STANDARD 14)
  else()
    set(MDBX_CXX_STANDARD 11)
  endif()
endif()
message(STATUS "Use C++${MDBX_CXX_STANDARD} for libmdbx's test")
if(NOT SUBPROJECT OR NOT DEFINED CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD ${MDBX_CXX_STANDARD})
endif()

if(${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
  set(TEST_OSAL windows)
else()
  set(TEST_OSAL unix)
endif()

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
  osal-${TEST_OSAL}.cc
  test.cc
  test.h
  try.cc
  utils.cc
  utils.h
  append.cc
  ttl.cc
  )

set_target_properties(mdbx_test PROPERTIES
  INTERPROCEDURAL_OPTIMIZATION $<BOOL:${INTERPROCEDURAL_OPTIMIZATION}>
  POSITION_INDEPENDENT_CODE ON
  CXX_STANDARD ${MDBX_CXX_STANDARD}
  CXX_STANDARD_REQUIRED ON
  )

if(CC_HAS_FASTMATH)
  target_compile_options(mdbx_test PRIVATE "-ffast-math")
endif()
if(CC_HAS_VISIBILITY AND (LTO_ENABLED OR INTERPROCEDURAL_OPTIMIZATION))
  set_target_properties(mdbx_test PROPERTIES LINK_FLAGS "-fvisibility=hidden")
endif()

target_link_libraries(mdbx_test mdbx ${LIB_MATH})
if(${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
  target_link_libraries(mdbx_test winmm.lib)
endif()

if(UNIX AND NOT SUBPROJECT)
  add_subdirectory(pcrf)
endif()
