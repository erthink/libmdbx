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
fetch_version(MDBX "${CMAKE_CURRENT_SOURCE_DIR}/../VERSION")
message(STATUS "libmdbx version is ${MDBX_VERSION}")

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
  "${CMAKE_CURRENT_SOURCE_DIR}/elements/version.c"
  "${CMAKE_CURRENT_SOURCE_DIR}/elements/config.h")

if(MDBX_BUILD_SHARED_LIBRARY)
  add_library(mdbx SHARED ${LIBMDBX_SOURCES})
  target_compile_definitions(mdbx PRIVATE LIBMDBX_EXPORTS INTERFACE LIBMDBX_IMPORTS)
  set(MDBX_LIBDEP_MODE PRIVATE)
else()
  add_library(mdbx STATIC ${LIBMDBX_SOURCES})
  set(MDBX_LIBDEP_MODE PUBLIC)
endif()

if(CC_HAS_VISIBILITY AND (LTO_ENABLED OR INTERPROCEDURAL_OPTIMIZATION))
  set_target_properties(mdbx PROPERTIES LINK_FLAGS "-fvisibility=hidden")
endif()

if(${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
  if(MSVC)
    if(NOT MSVC_LIB_EXE)
      # Find lib.exe
      get_filename_component(CL_NAME ${CMAKE_C_COMPILER} NAME)
      string(REPLACE cl.exe lib.exe MSVC_LIB_EXE ${CL_NAME})
      find_program(MSVC_LIB_EXE ${MSVC_LIB_EXE})
    endif()
    if(MSVC_LIB_EXE)
      message(STATUS "Found MSVC's lib tool: ${MSVC_LIB_EXE}")
      set(MDBX_NTDLL_EXTRA_IMPLIB ${CMAKE_CURRENT_BINARY_DIR}/mdbx_ntdll_extra.lib)
      add_custom_command(OUTPUT ${MDBX_NTDLL_EXTRA_IMPLIB}
        COMMENT "Create extra-import-library for ntdll.dll"
        MAIN_DEPENDENCY "${CMAKE_CURRENT_SOURCE_DIR}/elements/ntdll.def"
        COMMAND ${MSVC_LIB_EXE} /def:"${CMAKE_CURRENT_SOURCE_DIR}/elements/ntdll.def" /out:"${MDBX_NTDLL_EXTRA_IMPLIB}" ${INITIAL_CMAKE_STATIC_LINKER_FLAGS})
    else()
      message(SEND_ERROR "MSVC's lib tool not found")
    endif()
  elseif(MINGW OR MINGW64)
    if(NOT DLLTOOL)
      # Find dlltool
      get_filename_component(GCC_NAME ${CMAKE_C_COMPILER} NAME)
      string(REPLACE gcc dlltool DLLTOOL_NAME ${GCC_NAME})
      find_program(DLLTOOL NAMES ${DLLTOOL_NAME})
    endif()
    if(DLLTOOL)
      message(STATUS "Found dlltool: ${DLLTOOL}")
      set(MDBX_NTDLL_EXTRA_IMPLIB "${CMAKE_CURRENT_BINARY_DIR}/mdbx_ntdll_extra.a")
      add_custom_command(OUTPUT ${MDBX_NTDLL_EXTRA_IMPLIB}
        COMMENT "Create extra-import-library for ntdll.dll"
        MAIN_DEPENDENCY "${CMAKE_CURRENT_SOURCE_DIR}/elements/ntdll.def"
        COMMAND ${DLLTOOL} -d "${CMAKE_CURRENT_SOURCE_DIR}/elements/ntdll.def" -l "${MDBX_NTDLL_EXTRA_IMPLIB}")
    else()
      message(SEND_ERROR "dlltool not found")
    endif()
  endif()
endif()

target_link_libraries(mdbx ${MDBX_LIBDEP_MODE} ${CMAKE_THREAD_LIBS_INIT})
if(${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
  target_link_libraries(mdbx ${MDBX_LIBDEP_MODE} ntdll.lib)
  if(MDBX_NTDLL_EXTRA_IMPLIB)
    # LY: Sometimes Cmake requires a nightmarish magic for simple things.
    # 1) create a target out of the library compilation result
    add_custom_target(ntdll_extra_target DEPENDS ${MDBX_NTDLL_EXTRA_IMPLIB})
    # 2) create an library target out of the library compilation result
    add_library(ntdll_extra STATIC IMPORTED GLOBAL)
    add_dependencies(ntdll_extra ntdll_extra_target)
    # 3) specify where the library is (and where to find the headers)
    set_target_properties(ntdll_extra
      PROPERTIES
      IMPORTED_LOCATION ${MDBX_NTDLL_EXTRA_IMPLIB})
    target_link_libraries(mdbx ${MDBX_LIBDEP_MODE} ntdll_extra)
  endif()
endif()

set_target_properties(mdbx PROPERTIES
  INTERPROCEDURAL_OPTIMIZATION $<BOOL:${INTERPROCEDURAL_OPTIMIZATION}>
  C_STANDARD ${MDBX_C_STANDARD} C_STANDARD_REQUIRED ON
  PUBLIC_HEADER "../mdbx.h")

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
  PUBLIC_HEADER DESTINATION include
  INCLUDES DESTINATION include COMPONENT devel)

################################################################################
#
# library build info (used in library version output)
#
set(MDBX_BUILD_FLAGS "")

# append cmake's build-type flags and defines
if(NOT CMAKE_CONFIGURATION_TYPES)
  list(APPEND MDBX_BUILD_FLAGS ${CMAKE_C_FLAGS_${CMAKE_BUILD_TYPE_UPPERCASE}})
  list(APPEND MDBX_BUILD_FLAGS ${CMAKE_C_DEFINES_${CMAKE_BUILD_TYPE_UPPERCASE}})
endif()

# append linker dll's options
if(LIBMDBX_TYPE STREQUAL "SHARED")
  list(APPEND MDBX_BUILD_FLAGS ${CMAKE_SHARED_LINKER_FLAGS})
endif()

# get definitions
get_target_property(defs_list mdbx COMPILE_DEFINITIONS)
if(defs_list)
  list(APPEND MDBX_BUILD_FLAGS ${defs_list})
endif()

# get target compile options
get_target_property(options_list mdbx COMPILE_OPTIONS)
if(options_list)
  list(APPEND MDBX_BUILD_FLAGS ${options_list})
endif()

list(REMOVE_DUPLICATES MDBX_BUILD_FLAGS)
string(REPLACE ";" " " MDBX_BUILD_FLAGS "${MDBX_BUILD_FLAGS}")
if(CMAKE_CONFIGURATION_TYPES)
  # add dynamic part via per-configuration define
  message(STATUS "MDBX Compile Flags: ${MDBX_BUILD_FLAGS} <AND CONFIGURATION DEPENDENT>")
  add_definitions(-DMDBX_BUILD_FLAGS_CONFIG="$<$<CONFIG:Debug>:${CMAKE_C_FLAGS_DEBUG} ${CMAKE_C_DEFINES_DEBUG}>$<$<CONFIG:Release>:${CMAKE_C_FLAGS_RELEASE} ${CMAKE_C_DEFINES_RELEASE}>$<$<CONFIG:RelWithDebInfo>:${CMAKE_C_FLAGS_RELWITHDEBINFO} ${CMAKE_C_DEFINES_RELWITHDEBINFO}>$<$<CONFIG:MinSizeRel>:${CMAKE_C_FLAGS_MINSIZEREL} ${CMAKE_C_DEFINES_MINSIZEREL}>")
else()
  message(STATUS "MDBX Compile Flags: ${MDBX_BUILD_FLAGS}")
endif()

# get compiler info
execute_process(COMMAND sh -c "${CMAKE_C_COMPILER} --version | head -1"
  OUTPUT_VARIABLE MDBX_BUILD_COMPILER
  OUTPUT_STRIP_TRAILING_WHITESPACE
  ERROR_QUIET
  RESULT_VARIABLE rc)
if(rc OR NOT MDBX_BUILD_COMPILER)
  string(STRIP "${CMAKE_C_COMPILER_ID}-${CMAKE_C_COMPILER_VERSION}" MDBX_BUILD_COMPILER)
endif()

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
  set(MDBX_BUILD_CONFIG ${CMAKE_BUILD_TYPE})
endif()

# options
string(TIMESTAMP MDBX_BUILD_TIMESTAMP UTC)
set(options VERSION C_COMPILER CXX_COMPILER)
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

# generate version and config files
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/elements/version.c.in"
  "${CMAKE_CURRENT_SOURCE_DIR}/elements/version.c" ESCAPE_QUOTES)

file(SHA256 "${CMAKE_CURRENT_SOURCE_DIR}/elements/version.c" MDBX_SOURCERY_DIGEST)
string(MAKE_C_IDENTIFIER "${MDBX_GIT_DESCRIBE}" MDBX_SOURCERY_SUFFIX)
set(MDBX_BUILD_SOURCERY "${MDBX_SOURCERY_DIGEST}_${MDBX_SOURCERY_SUFFIX}")

configure_file("${CMAKE_CURRENT_SOURCE_DIR}/elements/config.h.in"
  "${CMAKE_CURRENT_SOURCE_DIR}/elements/config.h" ESCAPE_QUOTES)
add_definitions(-DMDBX_CONFIG_H="config.h")

add_subdirectory(tools)
