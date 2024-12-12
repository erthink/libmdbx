# Copyright (c) 2020-2024 Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> ###############################################
# SPDX-License-Identifier: Apache-2.0

enable_language(CXX)
include(../cmake/compiler.cmake)

function(add_extra_test name)
  set(options DISABLED)
  set(oneValueArgs TIMEOUT)
  set(multiValueArgs SOURCE LIBRARY DEPEND DLLPATH)
  cmake_parse_arguments(params "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})

  if(params_UNPARSED_ARGUMENTS)
    message(FATAL_ERROR "Unknown keywords given to add_extra_test(): \"${params_UNPARSED_ARGUMENTS}\".")
  endif()

  macro(oops)
    message(FATAL_ERROR "add_extra_test(): Opps, " ${ARGV})
  endmacro()

  if(NOT params_SOURCE)
    set(params_SOURCE extra/${name}.c++)
  endif()

  set(target "test_extra_${name}")
  add_executable(${target} ${params_SOURCE})
  target_include_directories(${target} PRIVATE "${PROJECT_SOURCE_DIR}")
  target_link_libraries(${target} ${TOOL_MDBX_LIB})
  set_target_properties(${target} PROPERTIES SKIP_BUILD_RPATH FALSE BUILD_WITH_INSTALL_RPATH FALSE)

  if(MDBX_BUILD_CXX AND MDBX_CXX_STANDARD)
    set_target_properties(${target} PROPERTIES CXX_STANDARD ${MDBX_CXX_STANDARD} CXX_STANDARD_REQUIRED ON)
  endif()

  if(params_DEPEND)
    add_dependencies(${target} ${params_DEPEND})
  endif()

  if(TOOL_MDBX_DLLCRUTCH)
    string(TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE_UPPERCASE)
    foreach(dep IN LISTS params_LIBRARY)
      get_target_property(type ${dep} TYPE)
      if(type STREQUAL SHARED_LIBRARY)
        # Windows don't have RPATH feature, therefore we should prepare PATH or copy DLL(s)...
        if(CMAKE_CONFIGURATION_TYPES)
          # Could not provide static ENVIRONMENT property with configuration-depended path
          set(dir FALSE)
        else(CMAKE_CONFIGURATION_TYPES)
          get_target_property(filename ${dep} IMPORTED_LOCATION_${CMAKE_BUILD_TYPE_UPPERCASE})
          if(NOT filename)
            get_target_property(filename ${dep} IMPORTED_LOCATION)
          endif()
          get_target_property(filename ${dep} LOCATION_${CMAKE_BUILD_TYPE_UPPERCASE})
          if(NOT filename)
            get_target_property(filename ${dep} LOCATION)
          endif()
          if(filename)
            get_filename_component(dir ${filename} DIRECTORY)
          else(filename)
            get_target_property(dir ${dep} LIBRARY_OUTPUT_DIRECTORY_${CMAKE_BUILD_TYPE_UPPERCASE})
            if(NOT dir)
              get_target_property(dir ${dep} RUNTIME_OUTPUT_DIRECTORY_${CMAKE_BUILD_TYPE_UPPERCASE})
            endif()
            if(NOT dir)
              get_target_property(dir ${dep} LIBRARY_OUTPUT_DIRECTORY)
            endif()
            if(NOT dir)
              get_target_property(dir ${dep} RUNTIME_OUTPUT_DIRECTORY)
            endif()
          endif(filename)
        endif(CMAKE_CONFIGURATION_TYPES)
        if(dir)
          list(APPEND params_DLLPATH ${dir})
        else(dir)
          # Path is configuration-depended or not available, should copy dll
          add_custom_command(
            TARGET ${target}
            POST_BUILD
            COMMAND if exist "$<TARGET_PDB_FILE:${dep}>" ${CMAKE_COMMAND} -E copy_if_different
                    "$<TARGET_PDB_FILE:${dep}>" "$<TARGET_FILE_DIR:${target}>")
          add_custom_command(
            TARGET ${target}
            POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E copy_if_different "$<TARGET_FILE:${dep}>" "$<TARGET_FILE_DIR:${target}>"
            COMMENT "${TOOL_MDBX_DLLCRUTCH}: Copy shared library ${dep} for test ${target}")
        endif(dir)
      endif()
    endforeach(dep)
  endif(TOOL_MDBX_DLLCRUTCH)

  if(NOT params_DISABLED AND NOT (CMAKE_CROSSCOMPILING AND NOT CMAKE_CROSSCOMPILING_EMULATOR))
    add_test(extra_${name} ${MDBX_OUTPUT_DIR}/${target})
    if(params_TIMEOUT)
      if(MEMORYCHECK_COMMAND
         OR CMAKE_MEMORYCHECK_COMMAND
         OR ENABLE_MEMCHECK)
        # FIXME: unless there are any other ideas how to fix the timeouts problem when testing under Valgrind.
        math(EXPR params_TIMEOUT "${params_TIMEOUT} * 42")
      endif()
      set_tests_properties(extra_${name} PROPERTIES TIMEOUT ${params_TIMEOUT})
    endif()
    if(params_DLLPATH)
      # Compose DLL's path in the ENVIRONMENT property
      if(WIN32)
        set(params_DLLPATH_ENV "${params_DLLPATH};$ENV{PATH}")
      else()
        set(params_DLLPATH_ENV "${params_DLLPATH}:$ENV{PATH}")
        string(REPLACE ":" ";" params_DLLPATH_ENV "${params_DLLPATH_ENV}")
      endif()
      list(REMOVE_DUPLICATES params_DLLPATH_ENV)
      if(WIN32)
        string(REPLACE ";" "\\;" params_DLLPATH_ENV "${params_DLLPATH_ENV}")
      else()
        string(REPLACE ";" ":" params_DLLPATH_ENV "${params_DLLPATH_ENV}")
      endif()
      set_tests_properties(extra_${name} PROPERTIES ENVIRONMENT "PATH=${params_DLLPATH_ENV}")
    endif()
  endif()
endfunction(add_extra_test)

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
    fork.c++)

if(MDBX_BUILD_CXX)
  add_extra_test(probe DISABLED)
else()
  probe_libcxx_filesystem()
  list(APPEND LIBMDBX_TEST_SOURCES "${MDBX_SOURCE_DIR}/mdbx.c++" ../mdbx.h++)
endif()

add_executable(mdbx_test ${LIBMDBX_TEST_SOURCES})
target_compile_definitions(mdbx_test PRIVATE MDBX_BUILD_TEST=1 MDBX_BUILD_CXX=1)

if(MDBX_CXX_STANDARD)
  set_target_properties(mdbx_test PROPERTIES CXX_STANDARD ${MDBX_CXX_STANDARD} CXX_STANDARD_REQUIRED ON)
endif()

set_target_properties(mdbx_test PROPERTIES INTERPROCEDURAL_OPTIMIZATION $<BOOL:${INTERPROCEDURAL_OPTIMIZATION}>)
target_setup_options(mdbx_test)

if(NOT MDBX_BUILD_CXX)
  target_compile_definitions(mdbx_test PRIVATE MDBX_BUILD_CXX=1)
  if(WIN32)
    target_compile_definitions(mdbx_test PRIVATE MDBX_WITHOUT_MSVC_CRT=0)
  endif()
endif()

if(NOT MDBX_BUILD_CXX AND LIBCXX_FILESYSTEM)
  if(CMAKE_COMPILER_IS_ELBRUSCXX
     AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 1.25.23
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
if(WIN32)
  target_link_libraries(mdbx_test winmm.lib)
endif()

if(NOT SUBPROJECT)
  if(UNIX)
    add_executable(test_extra_pcrf extra/pcrf/pcrf_test.c)
    target_include_directories(test_extra_pcrf PRIVATE "${PROJECT_SOURCE_DIR}")
    target_link_libraries(test_extra_pcrf ${TOOL_MDBX_LIB})
  endif()
endif()

# ######################################################################################################################

if(CMAKE_CROSSCOMPILING AND NOT CMAKE_CROSSCOMPILING_EMULATOR)
  message(WARNING "No emulator to run cross-compiled tests")
  add_test(NAME fake_since_no_crosscompiling_emulator COMMAND ${CMAKE_COMMAND} -E echo
                                                              "No emulator to run cross-compiled tests")
else()

  string(
    RANDOM
    LENGTH 9
    ALPHABET "1234567890" test_seed)
  message(STATUS "The ${test_seed} will be used for seeding tests. Re-run cmake to re-seed it.")

  add_test(NAME smoke COMMAND ${MDBX_OUTPUT_DIR}/mdbx_test --loglevel=verbose --prng-seed=${test_seed} --progress
                              --console=no --pathname=smoke.db --dont-cleanup-after basic)
  set_tests_properties(smoke PROPERTIES TIMEOUT 600 RUN_SERIAL OFF)
  if(MDBX_BUILD_TOOLS)
    add_test(NAME smoke_chk COMMAND ${MDBX_OUTPUT_DIR}/mdbx_chk -nvv smoke.db)
    set_tests_properties(
      smoke_chk
      PROPERTIES DEPENDS
                 smoke
                 TIMEOUT
                 60
                 FAIL_REGULAR_EXPRESSION
                 "cooperative mode"
                 REQUIRED_FILES
                 smoke.db)
    add_test(NAME smoke_chk_copy COMMAND ${MDBX_OUTPUT_DIR}/mdbx_chk -nvv smoke.db-copy)
    set_tests_properties(
      smoke_chk_copy
      PROPERTIES DEPENDS
                 smoke
                 TIMEOUT
                 60
                 FAIL_REGULAR_EXPRESSION
                 "cooperative mode"
                 REQUIRED_FILES
                 smoke.db-copy)
  endif()

  add_test(
    NAME dupsort_writemap
    COMMAND
      ${MDBX_OUTPUT_DIR}/mdbx_test --loglevel=notice --prng-seed=${test_seed} --table=+data.fixed --keygen.split=29
      --datalen=rnd --progress --console=no --repeat=2 --pathname=dupsort_writemap.db --dont-cleanup-after basic)
  set_tests_properties(dupsort_writemap PROPERTIES TIMEOUT 3600 RUN_SERIAL OFF)
  if(MDBX_BUILD_TOOLS)
    add_test(NAME dupsort_writemap_chk COMMAND ${MDBX_OUTPUT_DIR}/mdbx_chk -nvvwc dupsort_writemap.db)
    set_tests_properties(dupsort_writemap_chk PROPERTIES DEPENDS dupsort_writemap TIMEOUT 60 REQUIRED_FILES
                                                         dupsort_writemap.db)
    add_test(NAME dupsort_writemap_chk_copy COMMAND ${MDBX_OUTPUT_DIR}/mdbx_chk -nvvc dupsort_writemap.db-copy)
    set_tests_properties(
      dupsort_writemap_chk_copy
      PROPERTIES DEPENDS
                 dupsort_writemap
                 TIMEOUT
                 60
                 FAIL_REGULAR_EXPRESSION
                 "monopolistic mode"
                 REQUIRED_FILES
                 dupsort_writemap.db-copy)
  endif()

  add_test(NAME uniq_nested
           COMMAND ${MDBX_OUTPUT_DIR}/mdbx_test --loglevel=notice --mode=-writemap,-nosync-safe,-lifo --progress
                   --console=no --repeat=2 --pathname=uniq_nested.db --dont-cleanup-after basic)
  set_tests_properties(uniq_nested PROPERTIES TIMEOUT 1800 RUN_SERIAL OFF)
  if(MDBX_BUILD_TOOLS)
    add_test(NAME uniq_nested_chk COMMAND ${MDBX_OUTPUT_DIR}/mdbx_chk -nvvw uniq_nested.db)
    set_tests_properties(
      uniq_nested_chk
      PROPERTIES DEPENDS
                 uniq_nested
                 TIMEOUT
                 60
                 FAIL_REGULAR_EXPRESSION
                 "cooperative mode"
                 REQUIRED_FILES
                 uniq_nested.db)
    add_test(NAME uniq_nested_chk_copy COMMAND ${MDBX_OUTPUT_DIR}/mdbx_chk -nvv uniq_nested.db-copy)
    set_tests_properties(
      uniq_nested_chk_copy
      PROPERTIES DEPENDS
                 uniq_nested
                 TIMEOUT
                 60
                 FAIL_REGULAR_EXPRESSION
                 "cooperative mode"
                 REQUIRED_FILES
                 uniq_nested.db-copy)
  endif()

  if(NOT SUBPROJECT)
    if(UNIX)
      add_extra_test(upsert_alldups SOURCE extra/upsert_alldups.c)
      add_extra_test(dupfix_addodd SOURCE extra/dupfix_addodd.c)
    endif()
    if(MDBX_BUILD_CXX)
      if(NOT WIN32 OR NOT MDBX_CXX_STANDARD LESS 17)
        add_extra_test(cursor_closing)
        add_extra_test(early_close_dbi)
        add_extra_test(maindb_ordinal)
        add_extra_test(dupfix_multiple)
        add_extra_test(doubtless_positioning TIMEOUT 10800)
        add_extra_test(crunched_delete TIMEOUT 10800)
        add_extra_test(dbi)
        add_extra_test(open)
      endif()
      add_extra_test(hex_base64_base58)
    endif()
  endif()

endif()
