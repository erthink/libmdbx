set(MDBX_TOOLS
  mdbx_chk
  mdbx_copy
  mdbx_dump
  mdbx_load
  mdbx_stat
  )

foreach (TOOL ${MDBX_TOOLS})
  if(${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
    add_executable(${TOOL} ${TOOL}.c wingetopt.c wingetopt.h)
  else()
    add_executable(${TOOL} ${TOOL}.c)
  endif()

  target_link_libraries(${TOOL} mdbx ${CMAKE_THREAD_LIBS_INIT})
  set_target_properties(${TOOL} PROPERTIES
    INTERPROCEDURAL_OPTIMIZATION $<BOOL:${INTERPROCEDURAL_OPTIMIZATION}>)

  install(TARGETS ${TOOL} DESTINATION ${CMAKE_INSTALL_PREFIX}/bin COMPONENT mdbx)
  if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${TOOL}.1)
    install(FILES ${TOOL}.1 DESTINATION ${CMAKE_INSTALL_PREFIX}/man/man1 COMPONENT mdbx)
  endif()
endforeach()

if(LIB_MATH)
  target_link_libraries(mdbx_chk ${LIB_MATH})
  target_link_libraries(mdbx_stat ${LIB_MATH})
endif()
