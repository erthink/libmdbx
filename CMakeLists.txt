
if(WIN32)
  message(STATUS "MDBX locks: lck-windows.c")
  set (mdbx_sources src/mdbx.c src/osal.c src/version.c src/lck-windows.c )
elseif(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  message(STATUS "MDBX locks: lck-linux.c")
  set (mdbx_sources src/mdbx.c src/osal.c src/version.c src/lck-linux.c )
else()
  message(STATUS "MDBX locks: lck-posix.c")
  set (mdbx_sources src/mdbx.c src/osal.c src/version.c src/lck-posix.c )
endif()

 

include_directories("${CMAKE_CURRENT_SOURCE_DIR}")

add_library(mdbx  ${mdbx_sources})

target_link_libraries(mdbx PRIVATE ${CMAKE_THREAD_LIBS_INIT})

if(WIN32)
  target_link_libraries(mdbx ntdll)
endif()
