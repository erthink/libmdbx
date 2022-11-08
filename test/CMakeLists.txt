enable_language(CXX)
include(../cmake/compiler.cmake)

set(LIBMDBX_TEST_SOURCES
  base.h++
  cases.c++
  chrono.c++
  chrono.h++
  config.c++
  config.h++
  copy.c++
  dead.c++
  hill.c++
  jitter.c++
  keygen.c++
  keygen.h++
  log.c++
  log.h++
  main.c++
  osal.h++
  osal-unix.c++
  osal-windows.c++
  test.c++
  test.h++
  try.c++
  utils.c++
  utils.h++
  append.c++
  ttl.c++
  nested.c++
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
  if(CMAKE_COMPILER_IS_ELBRUSCXX AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 1.25.23
      AND NOT CMAKE_VERSION VERSION_LESS 3.13)
    target_link_options(mdbx_test PRIVATE "-Wl,--allow-multiple-definition")
  endif()
  target_link_libraries(mdbx_test ${LIBCXX_FILESYSTEM})
endif()

if(CMAKE_VERSION VERSION_LESS 3.1)
  target_link_libraries(mdbx_test ${TOOL_MDBX_LIB} ${LIB_MATH} ${CMAKE_THREAD_LIBS_INIT})
else()
  target_link_libraries(mdbx_test ${TOOL_MDBX_LIB} ${LIB_MATH} Threads::Threads)
endif()
if(${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
  target_link_libraries(mdbx_test winmm.lib)
endif()

if(UNIX AND NOT SUBPROJECT)
  add_executable(pcrf_test pcrf/pcrf_test.c)
  target_include_directories(pcrf_test PRIVATE "${PROJECT_SOURCE_DIR}")
  target_link_libraries(pcrf_test ${TOOL_MDBX_LIB})
endif()

################################################################################

if (CMAKE_CROSSCOMPILING AND NOT CMAKE_CROSSCOMPILING_EMULATOR)
  message(WARNING "No emulator to run cross-compiled tests")
  add_test(NAME fake_since_no_crosscompiling_emulator COMMAND ${CMAKE_COMMAND} -E
    echo "No emulator to run cross-compiled tests")
else()

  string(RANDOM LENGTH 9 ALPHABET "1234567890" test_seed)
  message(STATUS "The ${test_seed} will be used for seeding tests. Re-run cmake to re-seed it.")

  add_test(NAME smoke COMMAND ${MDBX_OUTPUT_DIR}/mdbx_test
    --loglevel=verbose
    --keygen.seed=${test_seed}
    --progress --console=no --pathname=smoke.db --dont-cleanup-after basic)
  set_tests_properties(smoke PROPERTIES
    TIMEOUT 600
    RUN_SERIAL OFF)
  if(MDBX_BUILD_TOOLS)
    add_test(NAME smoke_chk COMMAND ${MDBX_OUTPUT_DIR}/mdbx_chk -nvv smoke.db)
    set_tests_properties(smoke_chk PROPERTIES
      DEPENDS smoke
      TIMEOUT 60
      REQUIRED_FILES smoke.db)
    add_test(NAME smoke_chk_copy COMMAND ${MDBX_OUTPUT_DIR}/mdbx_chk -nvv smoke.db-copy)
    set_tests_properties(smoke_chk_copy PROPERTIES
      DEPENDS smoke
      TIMEOUT 60
      REQUIRED_FILES smoke.db-copy)
  endif()

  add_test(NAME dupsort_writemap COMMAND ${MDBX_OUTPUT_DIR}/mdbx_test
    --loglevel=notice
    --keygen.seed=${test_seed}
    --table=+data.integer --keygen.split=29 --datalen.min=min --datalen.max=max --progress --console=no
    --repeat=2 --pathname=dupsort_writemap.db --dont-cleanup-after basic)
  set_tests_properties(dupsort_writemap PROPERTIES
    TIMEOUT 600
    RUN_SERIAL OFF)
  if(MDBX_BUILD_TOOLS)
    add_test(NAME dupsort_writemap_chk COMMAND ${MDBX_OUTPUT_DIR}/mdbx_chk -nvv dupsort_writemap.db)
    set_tests_properties(dupsort_writemap_chk PROPERTIES
      DEPENDS dupsort_writemap
      TIMEOUT 60
      REQUIRED_FILES dupsort_writemap.db)
    add_test(NAME dupsort_writemap_chk_copy COMMAND ${MDBX_OUTPUT_DIR}/mdbx_chk -nvv dupsort_writemap.db-copy)
    set_tests_properties(dupsort_writemap_chk_copy PROPERTIES
      DEPENDS dupsort_writemap
      TIMEOUT 60
      REQUIRED_FILES dupsort_writemap.db-copy)
  endif()

  add_test(NAME uniq_nested COMMAND ${MDBX_OUTPUT_DIR}/mdbx_test
    --loglevel=notice
    --mode=-writemap,-nosync-safe,-lifo --progress --console=no --repeat=2 --pathname=uniq_nested.db --dont-cleanup-after basic)
  set_tests_properties(uniq_nested PROPERTIES
    TIMEOUT 1800
    RUN_SERIAL OFF)
  if(MDBX_BUILD_TOOLS)
    add_test(NAME uniq_nested_chk COMMAND ${MDBX_OUTPUT_DIR}/mdbx_chk -nvv uniq_nested.db)
    set_tests_properties(uniq_nested_chk PROPERTIES
      DEPENDS uniq_nested
      TIMEOUT 60
      REQUIRED_FILES uniq_nested.db)
    add_test(NAME uniq_nested_chk_copy COMMAND ${MDBX_OUTPUT_DIR}/mdbx_chk -nvv uniq_nested.db-copy)
    set_tests_properties(uniq_nested_chk_copy PROPERTIES
      DEPENDS uniq_nested
      TIMEOUT 60
      REQUIRED_FILES uniq_nested.db-copy)
  endif()

endif()
