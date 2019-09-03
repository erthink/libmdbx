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

# Get version
if(HAVE_MDBX_VERSIONINFO)
  fetch_version(MDBX "${CMAKE_CURRENT_SOURCE_DIR}/../VERSION")
  message(STATUS "libmdbx version is ${MDBX_VERSION}")
else()
  set(MDBX_VERSION "unversioned")
endif()

if(MDBX_ALLOY_MODE)
  set(LIBMDBX_SOURCES alloy.c)
else()
  if(${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
    set(LIBMDBX_OSAL windows)
  else()
    set(LIBMDBX_OSAL posix)
  endif()
  set(LIBMDBX_SOURCES
    elements/defs.h elements/internals.h elements/osal.h
    elements/core.c elements/osal.c elements/lck-${LIBMDBX_OSAL}.c)
endif()
list(APPEND LIBMDBX_SOURCES ../mdbx.h
  "${CMAKE_CURRENT_BINARY_DIR}/version.c"
  "${CMAKE_CURRENT_BINARY_DIR}/mdbx-config.h")

if(BUILD_SHARED_LIBS)
  add_library(mdbx SHARED ${LIBMDBX_SOURCES})
  target_compile_definitions(mdbx PRIVATE LIBMDBX_EXPORTS INTERFACE LIBMDBX_IMPORTS)
  if(${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
    target_compile_definitions(mdbx PRIVATE MDBX_BUILD_DLL)
  endif()
  if(CC_HAS_VISIBILITY AND (LTO_ENABLED OR INTERPROCEDURAL_OPTIMIZATION))
    set_target_properties(mdbx PROPERTIES LINK_FLAGS "-fvisibility=hidden")
  endif()
  target_link_libraries(mdbx PRIVATE ${CMAKE_THREAD_LIBS_INIT})
else()
  add_library(mdbx STATIC ${LIBMDBX_SOURCES})
  target_link_libraries(mdbx INTERFACE ${CMAKE_THREAD_LIBS_INIT})
endif()

set_target_properties(mdbx PROPERTIES
  INTERPROCEDURAL_OPTIMIZATION $<BOOL:${INTERPROCEDURAL_OPTIMIZATION}>
  C_STANDARD 11
  C_STANDARD_REQUIRED OFF
  PUBLIC_HEADER "../mdbx.h")
target_include_directories(mdbx PRIVATE
  "${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_CURRENT_BINARY_DIR}")

if(CC_HAS_FASTMATH)
  target_compile_options(mdbx PRIVATE "-ffast-math")
endif()
if(BUILD_FOR_NATIVE_CPU AND CC_HAS_ARCH_NATIVE)
  target_compile_options(mdbx PUBLIC "-march=native")
endif()
if(CC_HAS_VISIBILITY)
  target_compile_options(mdbx PRIVATE "-fvisibility=hidden")
endif()

install(TARGETS mdbx
  LIBRARY DESTINATION lib COMPONENT runtime
  RUNTIME DESTINATION bin COMPONENT runtime
  ARCHIVE DESTINATION lib/static COMPONENT devel
  PUBLIC_HEADER DESTINATION include/libmdbx
  INCLUDES DESTINATION include COMPONENT devel)

foreach(file mdbx.h LICENSE README.md AUTHORS)
  install(FILES "../${file}" DESTINATION "include/libmdbx")
endforeach()

################################################################################
#
# library build info (used in library version output)
#

# get definitions as a string of "-Dxyz=124 ..."
get_target_property(MDBX_DEFINITIONS mdbx COMPILE_DEFINITIONS)
if(NOT MDBX_DEFINITIONS)
  set(MDBX_DEFINITIONS "")
endif()
list(REMOVE_DUPLICATES MDBX_DEFINITIONS)
string(REGEX REPLACE "([^;]+)" " -D\\1" MDBX_DEFINITIONS "${MDBX_DEFINITIONS}")
string(STRIP MDBX_DEFINITIONS "${MDBX_DEFINITIONS}")

# get target compile options as a list
get_target_property(mdbx_compile_options mdbx COMPILE_OPTIONS)
if(NOT mdbx_compile_options)
  set(mdbx_compile_options "")
endif()

# append cmake's common cxx flags and defines
string(REPLACE " " ";" cmake_cxx_options "${CMAKE_C_FLAGS}" "${CMAKE_C_DEFINES}")
list(INSERT mdbx_compile_options 0 "${cmake_c_options}")
unset(cmake_c_options)

# append cmake's build-type flags and defines
if(NOT CMAKE_CONFIGURATION_TYPES)
  string(REPLACE " " ";" cmake_cxx_options
    "${CMAKE_C_FLAGS_${CMAKE_BUILD_TYPE_UPPERCASE}}"
    "${CMAKE_C_DEFINES_${CMAKE_BUILD_TYPE_UPPERCASE}}")
  list(APPEND mdbx_compile_options "${cmake_c_options}")
  unset(cmake_c_options)
endif()

# append linker dll's options
if(LIBMDBX_TYPE STREQUAL "SHARED")
  string(REPLACE " " ";" cmake_shared_linker_options "${CMAKE_SHARED_LINKER_FLAGS}")
  list(APPEND mdbx_compile_options ${cmake_shared_linker_options})
  unset(cmake_shared_linker_options)
endif()

# drop duplicates in the option list
list(REMOVE_DUPLICATES mdbx_compile_options)

# make string of space separated flags
string(REPLACE ";" " " MDBX_COMPILE_FLAGS "${mdbx_compile_options}")
unset(mdbx_compile_options)
string(STRIP "${MDBX_COMPILE_FLAGS}${MDBX_DEFINITIONS}" MDBX_COMPILE_FLAGS)
if(CMAKE_CONFIGURATION_TYPES)
  # add dynamic part via per-configuration define
  message(STATUS "MDBX Compile Flags: ${MDBX_COMPILE_FLAGS} <AND CONFIGURATION DEPENDENT>")
  set(MDBX_COMPILE_FLAGS "${MDBX_COMPILE_FLAGS}")
  add_definitions(
    -DMDBX_COMPILE_FLAGS="$<$<CONFIG:Debug>:${CMAKE_C_FLAGS_DEBUG} ${CMAKE_C_DEFINES_DEBUG}>$<$<CONFIG:Release>:${CMAKE_C_FLAGS_RELEASE} ${CMAKE_C_DEFINES_RELEASE}>$<$<CONFIG:RelWithDebInfo>:${CMAKE_C_FLAGS_RELWITHDEBINFO} ${CMAKE_C_DEFINES_RELWITHDEBINFO}>$<$<CONFIG:MinSizeRel>:${CMAKE_C_FLAGS_MINSIZEREL} ${CMAKE_C_DEFINES_MINSIZEREL}>"
    )
else()
  message(STATUS "MDBX Compile Flags: ${MDBX_COMPILE_FLAGS}")
  set(MDBX_COMPILE_FLAGS "${MDBX_COMPILE_FLAGS}")
endif()

# get compiler info
execute_process(COMMAND sh -c "${CMAKE_C_COMPILER} --version | head -1"
  OUTPUT_VARIABLE MDBX_BUILD_COMPILER
  OUTPUT_STRIP_TRAILING_WHITESPACE
  ERROR_QUIET
  RESULT_VARIABLE rc)
if(rc OR NOT MDBX_BUILD_COMPILER)
  set(MDBX_BUILD_COMPILER "")
else()
  set(MDBX_BUILD_COMPILER " | ${MDBX_BUILD_COMPILER}" )
endif()
string(STRIP "${CMAKE_C_COMPILER_ID}-${CMAKE_C_COMPILER_VERSION}${MDBX_BUILD_COMPILER}" MDBX_BUILD_COMPILER)

# make a build-target triplet
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
if(CMAKE_CONFIGURATION_TYPES)
  add_definitions(-DMDBX_BUILD_CONFIG="$<CONFIG>")
else()
  add_definitions(-DMDBX_BUILD_CONFIG="${CMAKE_BUILD_TYPE}")
endif()

# options
string(TIMESTAMP MDBX_BUILD_TIMESTAMP UTC)
set(MDBX_OPTIONS_STRING -DCMAKE_INSTALL_PREFIX="${CMAKE_INSTALL_PREFIX}")
foreach(item IN LISTS MDBX_BUILD_OPTIONS)
  if(DEFINED ${item} AND NOT "${${item}}" STREQUAL "AUTO")
    string(APPEND MDBX_OPTIONS_STRING " -D${item}=${${item}}")
    message(STATUS "${item}: ${${item}}")
  else()
    unset(${item} CACHE)
    set(${item}_AUTO ON)
    string(APPEND MDBX_OPTIONS_STRING " -D${item}=AUTO")
    message(STATUS "${item}: AUTO")
  endif()
endforeach()
set(options VERSION C_COMPILER CXX_COMPILER DEFINITIONS)
foreach(item IN LISTS options)
  if(DEFINED ${item})
    set(value "${${item}}")
  elseif(DEFINED MDBX_${item})
    set(item MDBX_${item})
    set(value "${${item}}")
  elseif(DEFINED CMAKE_${item})
    set(item CMAKE_${item})
    set(value "${${item}}")
  else()
    set(value "undefined")
  endif()
  message(STATUS "${item}: ${value}")
endforeach(item)

# generate version file
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/elements/version.c.in"
  "${CMAKE_CURRENT_BINARY_DIR}/version.c" ESCAPE_QUOTES)
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/elements/config.h.in"
  "${CMAKE_CURRENT_BINARY_DIR}/mdbx-config.h" ESCAPE_QUOTES)

target_compile_definitions(mdbx PRIVATE MDBX_CONFIG_H="${CMAKE_CURRENT_BINARY_DIR}/mdbx-config.h")

add_subdirectory(tools)
