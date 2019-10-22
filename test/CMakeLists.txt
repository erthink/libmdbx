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
  nested.cc
  )

set_target_properties(mdbx_test PROPERTIES
  INTERPROCEDURAL_OPTIMIZATION $<BOOL:${INTERPROCEDURAL_OPTIMIZATION}>
  CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON)

if(CC_HAS_FASTMATH)
  target_compile_options(mdbx_test PRIVATE "-ffast-math")
endif()
if(CC_HAS_VISIBILITY AND (LTO_ENABLED OR INTERPROCEDURAL_OPTIMIZATION))
  set_target_properties(mdbx_test PROPERTIES LINK_FLAGS "-fvisibility=hidden")
endif()

target_link_libraries(mdbx_test mdbx ${LIB_MATH} ${CMAKE_THREAD_LIBS_INIT})
if(${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
  target_link_libraries(mdbx_test winmm.lib)
endif()

if(UNIX AND NOT SUBPROJECT)
  add_subdirectory(pcrf)
endif()
