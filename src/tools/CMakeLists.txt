project(mdbx_tools)

set(MDBX_TOOLS
        mdbx_chk
        mdbx_copy
        mdbx_dump
        mdbx_load
        mdbx_stat
        )

foreach (D_FILE ${MDBX_TOOLS})
    add_executable(${D_FILE}
            ${D_FILE}.c
            )

    target_link_libraries(${D_FILE}
            mdbx
            )
    install(TARGETS ${D_FILE} DESTINATION ${CMAKE_INSTALL_PREFIX}/bin COMPONENT mdbx)
    if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${D_FILE}.1)
        install(FILES ${D_FILE}.1 DESTINATION ${CMAKE_INSTALL_PREFIX}/man/man1 COMPONENT mdbx)
    endif()
endforeach ()
