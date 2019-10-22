set(MDBX_TOOLS mdbx_chk mdbx_copy mdbx_dump mdbx_load mdbx_stat)

# use, i.e. don't skip the full RPATH for the build tree
set(CMAKE_SKIP_BUILD_RPATH FALSE)

# when building, don't use the install RPATH already (but later on when installing)
set(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)

# add the automatically determined parts of the RPATH
# which point to directories outside the build tree to the install RPATH
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

# the RPATH to be used when installing, but only if it's not a system directory
list(FIND CMAKE_PLATFORM_IMPLICIT_LINK_DIRECTORIES "${CMAKE_INSTALL_PREFIX}/lib" isSystemDir)
if(isSystemDir EQUAL -1)
  if(${CMAKE_SYSTEM_NAME} STREQUAL "Darwin")
    set(CMAKE_INSTALL_RPATH "@executable_path/../lib")
  else()
    set(CMAKE_INSTALL_RPATH "\$ORIGIN/../lib")
  endif()
endif()

foreach(TOOL ${MDBX_TOOLS})
  if(${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
    add_executable(${TOOL} ${TOOL}.c wingetopt.c wingetopt.h)
  else()
    add_executable(${TOOL} ${TOOL}.c)
  endif()

  target_link_libraries(${TOOL} mdbx ${CMAKE_THREAD_LIBS_INIT})
  set_target_properties(${TOOL} PROPERTIES
    C_STANDARD ${MDBX_C_STANDARD} C_STANDARD_REQUIRED ON
    INTERPROCEDURAL_OPTIMIZATION $<BOOL:${INTERPROCEDURAL_OPTIMIZATION}>)

  install(TARGETS ${TOOL} DESTINATION ${CMAKE_INSTALL_PREFIX}/bin COMPONENT mdbx)
  install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/../man1/${TOOL}.1 DESTINATION ${CMAKE_INSTALL_PREFIX}/man/man1 COMPONENT mdbx)
endforeach()

if(LIB_MATH)
  target_link_libraries(mdbx_chk ${LIB_MATH})
  target_link_libraries(mdbx_stat ${LIB_MATH})
endif()
