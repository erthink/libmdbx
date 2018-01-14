project(mdbx_tools)

set(MDBX_TOOLS
  mdbx_chk
  mdbx_copy
  mdbx_dump
  mdbx_load
  mdbx_stat
  )

foreach (TOOL ${MDBX_TOOLS})
  add_executable(${TOOL} ${TOOL}.c)

  target_link_libraries(${TOOL} mdbx)
  install(TARGETS ${TOOL} DESTINATION ${CMAKE_INSTALL_PREFIX}/bin COMPONENT mdbx)
  if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${TOOL}.1)
    install(FILES ${TOOL}.1 DESTINATION ${CMAKE_INSTALL_PREFIX}/man/man1 COMPONENT mdbx)
  endif()
endforeach()
