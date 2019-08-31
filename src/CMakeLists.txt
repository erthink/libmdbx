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

#
# Get version
if(HAVE_MDBX_VERSIONINFO)
  fetch_version(MDBX "${CMAKE_CURRENT_SOURCE_DIR}/../VERSION")
  message(STATUS "libmdbx version is ${MDBX_VERSION}")
else()
  set(MDBX_VERSION "unversioned")
endif()

add_library(mdbx_objects OBJECT ${CMAKE_CURRENT_BINARY_DIR}/version.c alloy.c)
set_target_properties(mdbx_objects PROPERTIES
  INTERPROCEDURAL_OPTIMIZATION $<BOOL:${INTERPROCEDURAL_OPTIMIZATION}>
  POSITION_INDEPENDENT_CODE ON
  C_STANDARD 11
  C_STANDARD_REQUIRED OFF
  PUBLIC_HEADER "../mdbx.h"
  )
target_compile_definitions(mdbx_objects PRIVATE "LIBMDBX_EXPORTS")
target_include_directories(mdbx_objects PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_CURRENT_BINARY_DIR}")

if(CC_HAS_FASTMATH)
  target_compile_options(mdbx_objects PRIVATE "-ffast-math")
endif()
if(BUILD_FOR_NATIVE_CPU AND CC_HAS_ARCH_NATIVE)
  target_compile_options(mdbx_objects PUBLIC "-march=native")
endif()
if(CC_HAS_VISIBILITY)
  target_compile_options(mdbx_objects PRIVATE "-fvisibility=hidden")
endif()

if(BUILD_SHARED_LIBS)
  set(LIBMDBX_TYPE SHARED)
else()
  set(LIBMDBX_TYPE STATIC)
endif()

add_library(mdbx ${LIBMDBX_TYPE} $<TARGET_OBJECTS:mdbx_objects>)
target_link_libraries(mdbx INTERFACE ${CMAKE_THREAD_LIBS_INIT})
if(LIBFPTU_TYPE STREQUAL "SHARED")
  target_compile_definitions(mdbx INTERFACE "LIBMDBX_IMPORTS")
  if(CC_HAS_VISIBILITY AND (LTO_ENABLED OR INTERPROCEDURAL_OPTIMIZATION))
    set_target_properties(mdbx PROPERTIES LINK_FLAGS "-fvisibility=hidden")
  endif()
endif()

install(TARGETS mdbx
  LIBRARY DESTINATION lib COMPONENT runtime
  RUNTIME DESTINATION bin COMPONENT runtime
  ARCHIVE DESTINATION lib/static COMPONENT devel
  PUBLIC_HEADER DESTINATION include/libmdbx INCLUDES DESTINATION include COMPONENT devel
  )

foreach(file mdbx.h LICENSE README.md AUTHORS)
  install(FILES "../${file}" DESTINATION "include/libmdbx")
endforeach()

################################################################################
#
# library build info (used in library version output)
#

# get definitions as a string of "-Dxyz=124 ..."
get_target_property(MDBX_DEFINITIONS mdbx_objects COMPILE_DEFINITIONS)
if(NOT MDBX_DEFINITIONS)
  set(MDBX_DEFINITIONS "")
endif()
list(REMOVE_DUPLICATES MDBX_DEFINITIONS)
string(REGEX REPLACE "([^;]+)" " -D\\1" MDBX_DEFINITIONS "${MDBX_DEFINITIONS}")
string(STRIP MDBX_DEFINITIONS "${MDBX_DEFINITIONS}")

# get target compile options as a list
get_target_property(mdbx_compile_options mdbx_objects COMPILE_OPTIONS)
if(NOT mdbx_compile_options)
  set(mdbx_compile_options "")
endif()

# append cmake's common cxx flags and defines
string(REPLACE " " ";" cmake_cxx_options "${CMAKE_C_FLAGS}" "${CMAKE_C_DEFINES}")
list(INSERT mdbx_compile_options 0 "${cmake_c_options}")
unset(cmake_c_options)

# append cmake's build-type flags and defines
if(NOT CMAKE_CONFIGURATION_TYPES)
  string(REPLACE " " ";" cmake_cxx_options "${CMAKE_C_FLAGS_${CMAKE_BUILD_TYPE_UPPERCASE}}" "${CMAKE_C_DEFINES_${CMAKE_BUILD_TYPE_UPPERCASE}}")
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
  set(MDBX_COMPILE_FLAGS "MDBX_COMPILE_FLAGS \"${MDBX_COMPILE_FLAGS}\"")
  add_definitions(
    -DMDBX_COMPILE_FLAGS="$<$<CONFIG:Debug>:${CMAKE_C_FLAGS_DEBUG} ${CMAKE_C_DEFINES_DEBUG}>$<$<CONFIG:Release>:${CMAKE_C_FLAGS_RELEASE} ${CMAKE_C_DEFINES_RELEASE}>$<$<CONFIG:RelWithDebInfo>:${CMAKE_C_FLAGS_RELWITHDEBINFO} ${CMAKE_C_DEFINES_RELWITHDEBINFO}>$<$<CONFIG:MinSizeRel>:${CMAKE_C_FLAGS_MINSIZEREL} ${CMAKE_C_DEFINES_MINSIZEREL}>"
    )
else()
  message(STATUS "MDBX Compile Flags: ${MDBX_COMPILE_FLAGS}")
  set(MDBX_COMPILE_FLAGS "\"${MDBX_COMPILE_FLAGS}\"")
endif()

# make a build-target triplet
if(CMAKE_CONFIGURATION_TYPES)
  # via per-configuration define
  add_definitions(-DMDBX_BUILD_TARGET="${CMAKE_SYSTEM_NAME}-${CMAKE_SYSTEM_PROCESSOR}-$<CONFIG>")
  set(MDBX_BUILD_TARGET "MDBX_BUILD_TARGET")
else()
  string(STRIP "\"${CMAKE_SYSTEM_NAME}-${CMAKE_SYSTEM_PROCESSOR}-${CMAKE_BUILD_TYPE}\"" MDBX_BUILD_TARGET)
endif()

# generate version file
string(TIMESTAMP MDBX_BUILD_TIMESTAMP UTC)
string(REPLACE "  " " " MDBX_OPTIONS "-DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX}"
  " -DENABLE_ASAN=${ENABLE_ASAN}"
  " -DENABLE_VALGRIND=${ENABLE_VALGRIND}"
  " -DENABLE_GPROF=${ENABLE_GPROF}"
  " -DENABLE_GCOV=${ENABLE_GCOV}"
  " -DENABLE_BACKTRACE=${ENABLE_BACKTRACE}"
  )

string(STRIP "${CMAKE_C_COMPILER_ID}-${CMAKE_C_COMPILER_VERSION}" MDBX_BUILD_COMPILER)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/elements/version.c.in ${CMAKE_CURRENT_BINARY_DIR}/version.c)

set(options VERSION C_COMPILER C_COMPILER DEFINITIONS
  ENABLE_GCOV ENABLE_GPROF ENABLE_VALGRIND ENABLE_BACKTRACE BUILD_TARGET
  HAVE_BFD ENABLE_ASAN)
foreach(option IN LISTS options)
  if(DEFINED MDBX_${option})
    set(value ${MDBX_${option}})
  elseif(DEFINED ${option})
    set(value "${${option}}")
  else()
    set(value "${CMAKE_${option}}")
  endif()
  message(STATUS "MDBX_${option}: ${value}")
endforeach(option)

add_subdirectory(tools)
