project(mdbx_tools)

add_executable(mdbx_chk
        mdbx_chk.c
        )

target_link_libraries(mdbx_chk
        libmdbx
        )

add_executable(mdbx_copy
        mdbx_copy.c
        )

target_link_libraries(mdbx_copy
        libmdbx
        )

add_executable(mdbx_dump
        mdbx_dump.c
        )

target_link_libraries(mdbx_dump
        libmdbx
        )


add_executable(mdbx_load
        mdbx_load.c
        )

target_link_libraries(mdbx_load
        libmdbx
        )

add_executable(mdbx_stat
        mdbx_stat.c
        )

target_link_libraries(mdbx_stat
        libmdbx
        )
