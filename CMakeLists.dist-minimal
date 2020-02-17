##
## This is the minimal template for CMakeList.txt which could be used
## to build libmdbx from the "amalgamated form" of libmdbx's source code.
##
## The amalgamated form is intended to embedding libmdbx in other projects
## in cases when using as git-submodule is not acceptable or inconveniently.
##
## The amalgamated form could be generated from full git repository
## on Linux just by `make dist`.
##

##
##  Copyright 2020 Leonid Yuriev <leo@yuriev.ru>
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
##  Please see README.md at https://github.com/erthink/libmdbx
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
if(NOT CMAKE_VERSION VERSION_LESS 3.9)
  cmake_policy(SET CMP0069 NEW)
endif()
if(NOT CMAKE_VERSION VERSION_LESS 3.12)
  cmake_policy(SET CMP0075 NEW)
endif()
if(NOT CMAKE_VERSION VERSION_LESS 3.13)
  cmake_policy(SET CMP0077 NEW)
endif()

if(DEFINED PROJECT_NAME)
  set(SUBPROJECT ON)
  set(NOT_SUBPROJECT OFF)
else()
  set(SUBPROJECT OFF)
  set(NOT_SUBPROJECT ON)
  project(libmdbx C CXX)
endif()

find_package(Threads REQUIRED)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE MinSizeRel CACHE STRING
    "Choose the type of build, options are: Debug Release RelWithDebInfo MinSizeRel."
    FORCE)
endif()

list(FIND CMAKE_C_COMPILE_FEATURES c_std_11 HAS_C11)
if(NOT HAS_C11 LESS 0)
  set(MDBX_C_STANDARD 11)
else()
  set(MDBX_C_STANDARD 99)
endif()
message(STATUS "Use C${MDBX_C_STANDARD} for libmdbx")

# not supported by this (minimal) script
add_definitions(-DMDBX_AVOID_CRT=0)

# provide build timestamp
string(TIMESTAMP MDBX_BUILD_TIMESTAMP UTC)
add_definitions(-DMDBX_BUILD_TIMESTAMP="${MDBX_BUILD_TIMESTAMP}")

# provide compiler info
execute_process(COMMAND sh -c "${CMAKE_C_COMPILER} --version | head -1"
  OUTPUT_VARIABLE MDBX_BUILD_COMPILER
  OUTPUT_STRIP_TRAILING_WHITESPACE
  ERROR_QUIET
  RESULT_VARIABLE rc)
if(rc OR NOT MDBX_BUILD_COMPILER)
  string(STRIP "${CMAKE_C_COMPILER_ID}-${CMAKE_C_COMPILER_VERSION}" MDBX_BUILD_COMPILER)
endif()
add_definitions(-DMDBX_BUILD_COMPILER="${MDBX_BUILD_COMPILER}")

# provide cpu/arch-system pair
if(CMAKE_C_COMPILER_TARGET)
  set(MDBX_BUILD_TARGET "${CMAKE_C_COMPILER_TARGET}")
elseif(CMAKE_C_PLATFORM_ID AND NOT CMAKE_C_PLATFORM_ID STREQUAL CMAKE_SYSTEM_NAME)
  string(STRIP "${CMAKE_C_PLATFORM_ID}-${CMAKE_SYSTEM_NAME}" MDBX_BUILD_TARGET)
elseif(CMAKE_LIBRARY_ARCHITECTURE)
  string(STRIP "${CMAKE_LIBRARY_ARCHITECTURE}-${CMAKE_SYSTEM_NAME}" MDBX_BUILD_TARGET)
elseif(CMAKE_GENERATOR_PLATFORM AND NOT CMAKE_C_PLATFORM_ID STREQUAL CMAKE_SYSTEM_NAME)
  string(STRIP "${CMAKE_GENERATOR_PLATFORM}-${CMAKE_SYSTEM_NAME}" MDBX_BUILD_TARGET)
elseif(CMAKE_SYSTEM_ARCH)
  string(STRIP "${CMAKE_SYSTEM_ARCH}-${CMAKE_SYSTEM_NAME}" MDBX_BUILD_TARGET)
else()
  string(STRIP "${CMAKE_SYSTEM_PROCESSOR}-${CMAKE_SYSTEM_NAME}" MDBX_BUILD_TARGET)
endif()
add_definitions(-DMDBX_BUILD_TARGET="${MDBX_BUILD_TARGET}")

# provide build target-config
if(CMAKE_CONFIGURATION_TYPES)
  add_definitions(-DMDBX_BUILD_CONFIG="$<CONFIG>")
else()
  add_definitions(-DMDBX_BUILD_CONFIG="${CMAKE_BUILD_TYPE}")
endif()

# provide build cflags
set(MDBX_BUILD_FLAGS "")
list(APPEND MDBX_BUILD_FLAGS ${CMAKE_C_FLAGS})
list(APPEND MDBX_BUILD_FLAGS ${CMAKE_C_DEFINES})
if(CMAKE_CONFIGURATION_TYPES)
  add_definitions(-DMDBX_BUILD_FLAGS_CONFIG="$<$<CONFIG:Debug>:${CMAKE_C_FLAGS_DEBUG} ${CMAKE_C_DEFINES_DEBUG}>$<$<CONFIG:Release>:${CMAKE_C_FLAGS_RELEASE} ${CMAKE_C_DEFINES_RELEASE}>$<$<CONFIG:RelWithDebInfo>:${CMAKE_C_FLAGS_RELWITHDEBINFO} ${CMAKE_C_DEFINES_RELWITHDEBINFO}>$<$<CONFIG:MinSizeRel>:${CMAKE_C_FLAGS_MINSIZEREL} ${CMAKE_C_DEFINES_MINSIZEREL}>")
else()
  string(TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE_UPPERCASE)
  list(APPEND MDBX_BUILD_FLAGS ${CMAKE_C_FLAGS_${CMAKE_BUILD_TYPE_UPPERCASE}})
  list(APPEND MDBX_BUILD_FLAGS ${CMAKE_C_DEFINES_${CMAKE_BUILD_TYPE_UPPERCASE}})
endif()
list(REMOVE_DUPLICATES MDBX_BUILD_FLAGS)
string(REPLACE ";" " " MDBX_BUILD_FLAGS "${MDBX_BUILD_FLAGS}")
add_definitions(-DMDBX_BUILD_FLAGS="${MDBX_BUILD_FLAGS}")

# shared library
if(NOT DEFINED MDBX_BUILD_SHARED_LIBRARY)
  if(DEFINED BUILD_SHARED_LIBS)
    option(MDBX_BUILD_SHARED_LIBRARY "Build libmdbx as shared library (DLL)" ${BUILD_SHARED_LIBS})
  else()
    option(MDBX_BUILD_SHARED_LIBRARY "Build libmdbx as shared library (DLL)" ON)
  endif()
endif()
if(MDBX_BUILD_SHARED_LIBRARY)
  add_library(mdbx SHARED mdbx.c mdbx.h)
  set_target_properties(mdbx PROPERTIES
    C_STANDARD ${MDBX_C_STANDARD} C_STANDARD_REQUIRED ON
    PUBLIC_HEADER mdbx.h)
  target_compile_definitions(mdbx PRIVATE LIBMDBX_EXPORTS INTERFACE LIBMDBX_IMPORTS)
  if(DEFINED INTERPROCEDURAL_OPTIMIZATION)
    set_target_properties(mdbx PROPERTIES
      INTERPROCEDURAL_OPTIMIZATION $<BOOL:${INTERPROCEDURAL_OPTIMIZATION}>)
  endif()
  target_link_libraries(mdbx PRIVATE ${CMAKE_THREAD_LIBS_INIT})
  if(WIN32)
    target_link_libraries(mdbx PRIVATE ntdll.lib)
  endif()
  if(${CMAKE_SYSTEM_NAME} STREQUAL "SunOS" OR ${CMAKE_SYSTEM_NAME} STREQUAL "Solaris")
    target_link_libraries(mdbx PRIVATE kstat)
  endif()
endif()

# static library used for tools, to avoid rpath/dll-path troubles
add_library(mdbx-static STATIC mdbx.c mdbx.h)
set_target_properties(mdbx-static PROPERTIES
  C_STANDARD ${MDBX_C_STANDARD} C_STANDARD_REQUIRED ON
  PUBLIC_HEADER mdbx.h)
target_link_libraries(mdbx-static INTERFACE ${CMAKE_THREAD_LIBS_INIT})
if(DEFINED INTERPROCEDURAL_OPTIMIZATION)
  set_target_properties(mdbx-static PROPERTIES
    INTERPROCEDURAL_OPTIMIZATION $<BOOL:${INTERPROCEDURAL_OPTIMIZATION}>)
endif()
if(WIN32)
  target_link_libraries(mdbx-static INTERFACE ntdll.lib)
endif()
if(${CMAKE_SYSTEM_NAME} STREQUAL "SunOS" OR ${CMAKE_SYSTEM_NAME} STREQUAL "Solaris")
  target_link_libraries(mdbx-static INTERFACE kstat)
endif()

# mdbx-tools
foreach(TOOL mdbx_chk mdbx_copy mdbx_stat mdbx_dump mdbx_load)
  add_executable(${TOOL} ${TOOL}.c)
  set_target_properties(${TOOL} PROPERTIES
    C_STANDARD ${MDBX_C_STANDARD} C_STANDARD_REQUIRED ON)
  if(DEFINED INTERPROCEDURAL_OPTIMIZATION)
    set_target_properties(${TOOL} PROPERTIES
      INTERPROCEDURAL_OPTIMIZATION $<BOOL:${INTERPROCEDURAL_OPTIMIZATION}>)
  endif()
  target_link_libraries(${TOOL} mdbx-static)
endforeach()

# installation
if(MDBX_BUILD_SHARED_LIBRARY)
  if(CMAKE_VERSION VERSION_LESS 3.12)
    install(TARGETS mdbx EXPORT libmdbx
      RUNTIME
        DESTINATION bin
        COMPONENT runtime
      LIBRARY
        DESTINATION lib
        COMPONENT runtime
      OBJECTS
        DESTINATION lib
        COMPONENT devel
      ARCHIVE
        DESTINATION lib
        COMPONENT devel
      PUBLIC_HEADER
        DESTINATION include
        COMPONENT devel
      INCLUDES
        DESTINATION include
        COMPONENT devel)
  else()
    install(TARGETS mdbx EXPORT libmdbx
      RUNTIME
        DESTINATION bin
        COMPONENT runtime
      LIBRARY
        DESTINATION lib
        COMPONENT runtime
        NAMELINK_COMPONENT devel
      OBJECTS
        DESTINATION lib
        COMPONENT devel
      ARCHIVE
        DESTINATION lib
        COMPONENT devel
      PUBLIC_HEADER
        DESTINATION include
        COMPONENT devel
      INCLUDES
        DESTINATION include
        COMPONENT devel)
  endif()
endif()

if(CMAKE_VERSION VERSION_LESS 3.12)
  install(TARGETS mdbx-static EXPORT libmdbx EXCLUDE_FROM_ALL
    RUNTIME
      DESTINATION bin
      COMPONENT runtime
    LIBRARY
      DESTINATION lib
      COMPONENT runtime
    OBJECTS
      DESTINATION lib
      COMPONENT devel
    ARCHIVE
      DESTINATION lib
      COMPONENT devel
    PUBLIC_HEADER
      DESTINATION include
      COMPONENT devel
    INCLUDES
      DESTINATION include
      COMPONENT devel)
else()
  install(TARGETS mdbx-static EXPORT libmdbx EXCLUDE_FROM_ALL
    RUNTIME
      DESTINATION bin
      COMPONENT runtime
    LIBRARY
      DESTINATION lib
      COMPONENT runtime
      NAMELINK_COMPONENT devel
    OBJECTS
      DESTINATION lib
      COMPONENT devel
    ARCHIVE
      DESTINATION lib
      COMPONENT devel
    PUBLIC_HEADER
      DESTINATION include
      COMPONENT devel
    INCLUDES
      DESTINATION include
      COMPONENT devel)
endif()

install(
  TARGETS
    mdbx_chk
    mdbx_stat
    mdbx_copy
    mdbx_dump
    mdbx_load
  RUNTIME
    DESTINATION bin
    COMPONENT runtime)

install(
  FILES
    man1/mdbx_chk.1
    man1/mdbx_stat.1
    man1/mdbx_copy.1
    man1/mdbx_dump.1
    man1/mdbx_load.1
  DESTINATION man/man1
  COMPONENT doc)

cmake_policy(POP)
