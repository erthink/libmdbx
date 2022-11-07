cmake_minimum_required(VERSION 2.8.7)
set(TARGET mdbx)
project(${TARGET})

set(MDBX_VERSION_MAJOR 0)
set(MDBX_VERSION_MINOR 3)
set(MDBX_VERSION_RELEASE 1)
set(MDBX_VERSION_REVISION 0)

set(MDBX_VERSION_STRING ${MDBX_VERSION_MAJOR}.${MDBX_VERSION_MINOR}.${MDBX_VERSION_RELEASE})

enable_language(C)
enable_language(CXX)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED on)

add_definitions(-DNDEBUG=1 -DMDBX_DEBUG=0 -DLIBMDBX_EXPORTS=1 -D_GNU_SOURCE=1)

find_package(Threads REQUIRED)

get_directory_property(hasParent PARENT_DIRECTORY)
if(hasParent)
  set(STANDALONE_BUILD 0)
else()
  set(STANDALONE_BUILD 1)
  enable_testing()

  if (CMAKE_C_COMPILER_ID MATCHES GNU)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O2")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g3")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Werror")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wextra")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -ffunction-sections")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fvisibility=hidden")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=gnu11")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -pthread")
  endif()

  if (CMAKE_CXX_COMPILER_ID MATCHES GNU)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -W")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wpointer-arith")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-sign-compare")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wformat-security")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Woverloaded-virtual")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wwrite-strings")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fmax-errors=20")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fvisibility=hidden")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-parameter -Wunused-function -Wunused-variable -Wunused-value -Wmissing-declarations")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-missing-field-initializers")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wcast-qual")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ggdb")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-strict-aliasing")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -finline-functions-called-once")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-packed-bitfield-compat")

    set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g3")
    set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O3 -g3")
  endif()

  if (COVERAGE)
    if (NOT "${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
      message(FATAL_ERROR "Coverage requires -DCMAKE_BUILD_TYPE=Debug Current value=${CMAKE_BUILD_TYPE}")
    endif()

    message(STATUS "Setting coverage compiler flags")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -ggdb3 -O0 --coverage -fprofile-arcs -ftest-coverage")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS}     -g -ggdb3 -O0 --coverage -fprofile-arcs -ftest-coverage")
    add_definitions(-DCOVERAGE_TEST)
  endif()

  if (NOT TRAVIS)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address -fsanitize=leak -fstack-protector-strong -static-libasan")
  endif()
endif()

set(${TARGET}_SRC
  mdbx.h
  src/bits.h
  src/defs.h
  src/lck-linux.c
  src/mdbx.c
  src/osal.c
  src/osal.h
  src/version.c
  )

add_library(${TARGET}_STATIC STATIC
  ${${TARGET}_SRC}
  )

add_library(${TARGET} ALIAS ${TARGET}_STATIC)

add_library(${TARGET}_SHARED SHARED
  ${${TARGET}_SRC}
  )

set_target_properties(${TARGET}_SHARED PROPERTIES
  VERSION ${MDBX_VERSION_STRING}
  SOVERSION ${MDBX_VERSION_MAJOR}.${MDBX_VERSION_MINOR}
  OUTPUT_NAME ${TARGET}
  CLEAN_DIRECT_OUTPUT 1
  )

set_target_properties(${TARGET}_STATIC PROPERTIES
  VERSION ${MDBX_VERSION_STRING}
  SOVERSION ${MDBX_VERSION_MAJOR}.${MDBX_VERSION_MINOR}
  OUTPUT_NAME ${TARGET}
  CLEAN_DIRECT_OUTPUT 1
  )

target_include_directories(${TARGET}_STATIC PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(${TARGET}_SHARED PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(${TARGET}_STATIC ${CMAKE_THREAD_LIBS_INIT})
target_link_libraries(${TARGET}_SHARED ${CMAKE_THREAD_LIBS_INIT})
if(UNIX AND NOT APPLE)
  target_link_libraries(${TARGET}_STATIC rt)
  target_link_libraries(${TARGET}_SHARED rt)
endif()

install(TARGETS ${TARGET}_STATIC DESTINATION ${CMAKE_INSTALL_PREFIX}/lib64 COMPONENT mdbx)
install(TARGETS ${TARGET}_SHARED DESTINATION ${CMAKE_INSTALL_PREFIX}/lib64 COMPONENT mdbx)
install(FILES mdbx.h DESTINATION ${CMAKE_INSTALL_PREFIX}/include COMPONENT mdbx-devel)

add_subdirectory(src/tools)
add_subdirectory(test)
add_subdirectory(test/pcrf)
add_subdirectory(tutorial)

##############################################################################

set(CPACK_GENERATOR "RPM")
set(CPACK_RPM_COMPONENT_INSTALL ON)

# Version
if (NOT "$ENV{BUILD_NUMBER}" STREQUAL "")
  set(CPACK_PACKAGE_RELEASE $ENV{BUILD_NUMBER})
else()
  if (NOT "$ENV{CI_PIPELINE_ID}" STREQUAL "")
    set(CPACK_PACKAGE_RELEASE $ENV{CI_PIPELINE_ID})
  else()
    set(CPACK_PACKAGE_RELEASE 1)
  endif()
endif()
set(CPACK_RPM_PACKAGE_RELEASE ${CPACK_PACKAGE_RELEASE})

set(CPACK_PACKAGE_VERSION ${MDBX_VERSION_STRING})
set(CPACK_PACKAGE_VERSION_FULL ${CPACK_PACKAGE_VERSION}-${CPACK_PACKAGE_RELEASE})

set(CPACK_RPM_mdbx-devel_PACKAGE_REQUIRES "mdbx = ${CPACK_PACKAGE_VERSION}")

set(CPACK_RPM_SPEC_INSTALL_POST "/bin/true")
set(CPACK_RPM_mdbx_PACKAGE_NAME mdbx)
set(CPACK_RPM_mdbx-devel_PACKAGE_NAME mdbx-devel)
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "The revised and extended descendant of Symas LMDB")

set(CPACK_PACKAGE_VENDOR "???")
set(CPACK_PACKAGE_CONTACT "Vladimir Romanov")
set(CPACK_PACKAGE_RELOCATABLE false)
set(CPACK_RPM_PACKAGE_ARCHITECTURE "x86_64")
set(CPACK_RPM_PACKAGE_REQUIRES "")
set(CPACK_RPM_PACKAGE_GROUP "Applications/Database")

set(CPACK_RPM_mdbx_FILE_NAME "${CPACK_RPM_mdbx_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION_FULL}.${CPACK_RPM_PACKAGE_ARCHITECTURE}.rpm")
set(CPACK_RPM_mdbx-devel_FILE_NAME "${CPACK_RPM_mdbx-devel_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION_FULL}.${CPACK_RPM_PACKAGE_ARCHITECTURE}.rpm")

set(CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION
  /usr/local
  /usr/local/bin
  /usr/local/lib64
  /usr/local/include
  /usr/local/man
  /usr/local/man/man1
  )

include(CPack)
