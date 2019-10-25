##
##  Copyright 2019 Leonid Yuriev <leo@yuriev.ru>
##  and other libmdbx authors: please see AUTHORS file.
##  All rights reserved.
##
##  Redistribution and use in source and binary forms, with or without
##  modification, are permitted only as authorized by the OpenLDAP
##  Public License.
##
##  A copy of this license is available in the file LICENSE in the
##  top-level directory of the distribution or, alternatively, at
##  <http://www.OpenLDAP.org/license.html>.
##

##
##  libmdbx = { Revised and extended descendant of Symas LMDB. }
##  Please see README.md at https://github.com/leo-yuriev/libmdbx
##
##  Libmdbx is superior to LMDB in terms of features and reliability,
##  not inferior in performance. libmdbx works on Linux, FreeBSD, MacOS X
##  and other systems compliant with POSIX.1-2008, but also support Windows
##  as a complementary platform.
##
##  The next version is under active non-public development and will be
##  released as MithrilDB and libmithrildb for libraries & packages.
##  Admittedly mythical Mithril is resembling silver but being stronger and
##  lighter than steel. Therefore MithrilDB is rightly relevant name.
##
##  MithrilDB will be radically different from libmdbx by the new database
##  format and API based on C++17, as well as the Apache 2.0 License.
##  The goal of this revolution is to provide a clearer and robust API,
##  add more features and new valuable properties of database.
##
##  The Future will (be) Positive. Всё будет хорошо.
##

cmake_minimum_required(VERSION 3.8.2)
cmake_policy(PUSH)
cmake_policy(VERSION 3.8.2)
if(NOT CMAKE_VERSION VERSION_LESS 3.13)
  cmake_policy(SET CMP0077 NEW)
endif()
if(NOT CMAKE_VERSION VERSION_LESS 3.12)
  cmake_policy(SET CMP0075 NEW)
endif()
if(NOT CMAKE_VERSION VERSION_LESS 3.9)
  cmake_policy(SET CMP0069 NEW)
  include(CheckIPOSupported)
  check_ipo_supported(RESULT CMAKE_INTERPROCEDURAL_OPTIMIZATION_AVAILABLE)
else()
  set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_AVAILABLE FALSE)
endif()

if(DEFINED PROJECT_NAME)
  set(SUBPROJECT ON)
  set(NOT_SUBPROJECT OFF)
  if(NOT DEFINED BUILD_TESTING)
    set(BUILD_TESTING OFF)
  endif()
else()
  set(SUBPROJECT OFF)
  set(NOT_SUBPROJECT ON)
  project(libmdbx C CXX)
  if(NOT DEFINED BUILD_TESTING)
    set(BUILD_TESTING ON)
  endif()
endif()

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE MinSizeRel CACHE STRING
    "Choose the type of build, options are: Debug Release RelWithDebInfo MinSizeRel."
    FORCE)
endif()

macro(add_mdbx_option NAME DESCRIPTION DEFAULT)
  list(APPEND MDBX_BUILD_OPTIONS ${NAME})
  if(NOT ${DEFAULT} STREQUAL "AUTO")
    option(${NAME} "${DESCRIPTION}" ${DEFAULT})
  endif()
endmacro()

# only for compatibility testing
# set(CMAKE_CXX_STANDARD 14)

if(NOT "$ENV{TEAMCITY_PROCESS_FLOW_ID}" STREQUAL "")
  set(CI TEAMCITY)
  message(STATUS "TeamCity CI")
elseif(NOT "$ENV{TRAVIS}" STREQUAL "")
  set(CI TRAVIS)
  message(STATUS "Travis CI")
elseif(NOT "$ENV{CIRCLECI}" STREQUAL "")
  set(CI CIRCLE)
  message(STATUS "Circle CI")
elseif(NOT "$ENV{APPVEYOR}" STREQUAL "")
  set(CI APPVEYOR)
  message(STATUS "AppVeyor CI")
elseif(NOT "$ENV{CI}" STREQUAL "")
  set(CI "$ENV{CI}")
  message(STATUS "Other CI (${CI})")
else()
  message(STATUS "Assume No any CI environment")
  unset(CI)
endif()

# output all mdbx-related targets in single directory
if(NOT DEFINED MDBX_OUTPUT_DIR)
  set(MDBX_OUTPUT_DIR ${CMAKE_CURRENT_BINARY_DIR})
endif()
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${MDBX_OUTPUT_DIR})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${MDBX_OUTPUT_DIR})
set(CMAKE_PDB_OUTPUT_DIRECTORY ${MDBX_OUTPUT_DIR})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${MDBX_OUTPUT_DIR})

include(CheckLibraryExists)
include(CheckIncludeFiles)
include(CheckCCompilerFlag)
include(CheckSymbolExists)
include(CheckCSourceRuns)
include(CheckCXXSourceRuns)
include(CheckCSourceCompiles)
include(CheckCXXSourceCompiles)
include(TestBigEndian)
include(CheckFunctionExists)
include(FindPackageMessage)
include(CheckStructHasMember)
include(CMakeDependentOption)
include(GNUInstallDirs)

if(CMAKE_C_COMPILER_ID STREQUAL "MSVC" AND MSVC_VERSION LESS 1900)
  message(SEND_ERROR "MSVC compiler ${MSVC_VERSION} is too old for building MDBX."
    " At least 'Microsoft Visual Studio 2015' is required.")
endif()

# Set default build type to Release. This is to ease a User's life.
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING
    "Choose the type of build, options are: Debug Release RelWithDebInfo MinSizeRel."
    FORCE)
endif()
string(TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE_UPPERCASE)

include(cmake/utils.cmake)
include(cmake/compiler.cmake)
include(cmake/profile.cmake)

find_program(ECHO echo)
find_program(CAT cat)
find_program(GIT git)
find_program(LD ld)

# CHECK_INCLUDE_FILES(unistd.h HAVE_UNISTD_H)
# CHECK_INCLUDE_FILES(sys/uio.h HAVE_SYS_UIO_H)
# CHECK_INCLUDE_FILES(sys/stat.h HAVE_SYS_STAT_H)

CHECK_FUNCTION_EXISTS(pow NOT_NEED_LIBM)
if(NOT_NEED_LIBM)
  set(LIB_MATH "")
else()
  set(CMAKE_REQUIRED_LIBRARIES m)
  CHECK_FUNCTION_EXISTS(pow HAVE_LIBM)
  if(HAVE_LIBM)
    set(LIB_MATH m)
  else()
    message(FATAL_ERROR "No libm found for math support")
  endif()
endif()

find_package(Threads REQUIRED)

if(SUBPROJECT)
  if(NOT DEFINED BUILD_SHARED_LIBS)
    option(BUILD_SHARED_LIBS "Build shared libraries (DLLs)" OFF)
  endif()
  if(NOT DEFINED CMAKE_POSITION_INDEPENDENT_CODE)
    option(CMAKE_POSITION_INDEPENDENT_CODE "Generate position independed (PIC)" ON)
  endif()
else()
  option(BUILD_SHARED_LIBS "Build shared libraries (DLLs)" ON)
  option(CMAKE_POSITION_INDEPENDENT_CODE "Generate position independed (PIC)" ON)
  if (CC_HAS_ARCH_NATIVE)
    option(BUILD_FOR_NATIVE_CPU "Generate code for the compiling machine CPU" OFF)
  endif()

  if(CMAKE_CONFIGURATION_TYPES OR NOT CMAKE_BUILD_TYPE_UPPERCASE STREQUAL "DEBUG")
    set(INTERPROCEDURAL_OPTIMIZATION_DEFAULT ON)
  else()
    set(INTERPROCEDURAL_OPTIMIZATION_DEFAULT OFF)
  endif()

  if(CMAKE_INTERPROCEDURAL_OPTIMIZATION_AVAILABLE
      OR GCC_LTO_AVAILABLE OR MSVC_LTO_AVAILABLE OR CLANG_LTO_AVAILABLE)
    option(INTERPROCEDURAL_OPTIMIZATION "Enable interprocedural/LTO optimization" ${INTERPROCEDURAL_OPTIMIZATION_DEFAULT})
  endif()

  if(INTERPROCEDURAL_OPTIMIZATION)
    if(GCC_LTO_AVAILABLE)
      set(LTO_ENABLED TRUE)
      set(CMAKE_AR ${CMAKE_GCC_AR} CACHE PATH "Path to ar program with LTO-plugin" FORCE)
      set(CMAKE_NM ${CMAKE_GCC_NM} CACHE PATH "Path to nm program with LTO-plugin" FORCE)
      set(CMAKE_RANLIB ${CMAKE_GCC_RANLIB} CACHE PATH "Path to ranlib program with LTO-plugin" FORCE)
      message(STATUS "MDBX indulge Link-Time Optimization by GCC")
    elseif(CLANG_LTO_AVAILABLE)
      set(LTO_ENABLED TRUE)
      set(CMAKE_AR ${CMAKE_CLANG_AR} CACHE PATH "Path to ar program with LTO-plugin" FORCE)
      set(CMAKE_NM ${CMAKE_CLANG_NM} CACHE PATH "Path to nm program with LTO-plugin" FORCE)
      set(CMAKE_RANLIB ${CMAKE_CLANG_RANLIB} CACHE PATH "Path to ranlib program with LTO-plugin" FORCE)
      message(STATUS "MDBX indulge Link-Time Optimization by CLANG")
    elseif(MSVC_LTO_AVAILABLE)
      set(LTO_ENABLED TRUE)
      message(STATUS "MDBX indulge Link-Time Optimization by MSVC")
    elseif(CMAKE_INTERPROCEDURAL_OPTIMIZATION_AVAILABLE)
      message(STATUS "MDBX indulge Interprocedural Optimization by CMake")
      set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
      set(LTO_ENABLED TRUE)
    else()
      message(WARNING "Unable to engage interprocedural/LTO optimization.")
    endif()
  else()
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION FALSE)
    set(LTO_ENABLED FALSE)
  endif()

  find_program(VALGRIND valgrind)
  if(VALGRIND)
    # LY: cmake is ugly and nasty.
    #      - therefore memcheck-options should be defined before including ctest;
    #      - otherwise ctest may ignore it.
    set(MEMORYCHECK_SUPPRESSIONS_FILE
      "${PROJECT_SOURCE_DIR}/test/valgrind_suppress.txt"
      CACHE FILEPATH "Suppressions file for Valgrind" FORCE)
    set(MEMORYCHECK_COMMAND_OPTIONS
      "--trace-children=yes --leak-check=full --track-origins=yes --error-exitcode=42 --error-markers=@ --errors-for-leak-kinds=definite --fair-sched=yes --suppressions=${MEMORYCHECK_SUPPRESSIONS_FILE}"
      CACHE STRING "Valgrind options" FORCE)
    set(VALGRIND_COMMAND_OPTIONS "${MEMORYCHECK_COMMAND_OPTIONS}" CACHE STRING "Valgrind options" FORCE)
  endif()

  #
  # Enable 'make tags' target.
  find_program(CTAGS ctags)
  if(CTAGS)
    add_custom_target(tags COMMAND ${CTAGS} -R -f tags
      WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    add_custom_target(ctags DEPENDS tags)
  endif(CTAGS)

  #
  # Enable 'make reformat' target.
  find_program(CLANG_FORMAT
    NAMES clang-format-6.0 clang-format-5.0 clang-format-4.0
    clang-format-3.9 clang-format-3.8 clang-format-3.7 clang-format)
  if(CLANG_FORMAT AND UNIX)
    add_custom_target(reformat
      VERBATIM
      COMMAND
      git ls-files |
      grep -E \\.\(c|cxx|cc|cpp|h|hxx|hpp\)\(\\.in\)?\$ |
      xargs ${CLANG_FORMAT} -i --style=file
      WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
  endif()

  if(NOT "${PROJECT_BINARY_DIR}" STREQUAL "${PROJECT_SOURCE_DIR}")
    add_custom_target(distclean)
    add_custom_command(TARGET distclean
      COMMAND ${CMAKE_COMMAND} -E remove_directory "${PROJECT_BINARY_DIR}"
      COMMENT "Removing the build directory and its content")
  elseif(IS_DIRECTORY .git AND GIT)
    add_custom_target(distclean)
    add_custom_command(TARGET distclean
      WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
      COMMAND ${GIT} submodule foreach --recursive git clean -f -X -d
      COMMAND ${GIT} clean -f -X -d
      COMMENT "Removing all build files from the source directory")
  endif()

  setup_compile_flags()
endif(SUBPROJECT)

list(FIND CMAKE_C_COMPILE_FEATURES c_std_11 HAS_C11)
if(NOT HAS_C11 LESS 0)
  set(MDBX_C_STANDARD 11)
else()
  set(MDBX_C_STANDARD 99)
endif()
message(STATUS "Use C${MDBX_C_STANDARD} for libmdbx")

##############################################################################
##############################################################################
#
#         ####   #####    #####     #     ####   #    #   ####
#        #    #  #    #     #       #    #    #  ##   #  #
#        #    #  #    #     #       #    #    #  # #  #   ####
#        #    #  #####      #       #    #    #  #  # #       #
#        #    #  #          #       #    #    #  #   ##  #    #
#         ####   #          #       #     ####   #    #   ####
#

set(MDBX_BUILD_OPTIONS ENABLE_ASAN MDBX_USE_VALGRIND ENABLE_GPROF ENABLE_GCOV)
add_mdbx_option(MDBX_BUILD_SHARED_LIBRARY "Build libmdbx as shared library (DLL)" ${BUILD_SHARED_LIBS})
add_mdbx_option(MDBX_ALLOY_BUILD "Build MDBX library as single object file" ON)
add_mdbx_option(MDBX_TXN_CHECKOWNER "Checking transaction matches the calling thread inside libmdbx's API" ON)
add_mdbx_option(MDBX_TXN_CHECKPID "Paranoid checking PID inside libmdbx's API" AUTO)
mark_as_advanced(MDBX_TXN_CHECKPID)
if(${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
  add_mdbx_option(MDBX_DISABLE_GNU_SOURCE "Don't use nonstandard GNU/Linux extension functions" OFF)
  mark_as_advanced(MDBX_DISABLE_GNU_SOURCE)
endif()
if(${CMAKE_SYSTEM_NAME} STREQUAL "Darwin")
  add_mdbx_option(MDBX_OSX_SPEED_INSTEADOF_DURABILITY "Disable use fcntl(F_FULLFSYNC) in favor of speed" OFF)
  mark_as_advanced(MDBX_OSX_SPEED_INSTEADOF_DURABILITY)
endif()
if(${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
  add_mdbx_option(MDBX_AVOID_CRT "Avoid dependence from MSVC CRT" ${NOT_SUBPROJECT})
  if(NOT MDBX_BUILD_SHARED_LIBRARY)
    add_mdbx_option(MDBX_CONFIG_MANUAL_TLS_CALLBACK
      "Provide mdbx_dll_handler() for manual initialization" OFF)
    mark_as_advanced(MDBX_CONFIG_MANUAL_TLS_CALLBACK)
  endif()
else()
  add_mdbx_option(MDBX_USE_ROBUST "Use POSIX.1-2008 robust mutexes" AUTO)
  mark_as_advanced(MDBX_USE_ROBUST)
  add_mdbx_option(MDBX_USE_OFDLOCKS "Use Open file description locks (aka OFD locks, non-POSIX)" AUTO)
  mark_as_advanced(MDBX_USE_OFDLOCKS)
endif()
option(MDBX_ENABLE_TESTS "Build MDBX tests." ${BUILD_TESTING})

################################################################################
################################################################################

add_subdirectory(src)
if(MDBX_ENABLE_TESTS)
  add_subdirectory(test)
endif()

set(PACKAGE "libmdbx")
set(CPACK_PACKAGE_VERSION_MAJOR ${MDBX_VERSION_MAJOR})
set(CPACK_PACKAGE_VERSION_MINOR ${MDBX_VERSION_MINOR})
set(CPACK_PACKAGE_VERSION_PATCH ${MDBX_VERSION_RELEASE})
set(CPACK_PACKAGE_VERSION_COMMIT ${MDBX_VERSION_REVISION})
set(PACKAGE_VERSION "${CPACK_PACKAGE_VERSION_MAJOR}.${CPACK_PACKAGE_VERSION_MINOR}.${CPACK_PACKAGE_VERSION_PATCH}.${CPACK_PACKAGE_VERSION_COMMIT}")
message(STATUS "libmdbx package version is ${PACKAGE_VERSION}")

cmake_policy(POP)
