set(TARGET mdbx_test)
project(${TARGET})

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-missing-declarations")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-cast-qual")

add_executable(${TARGET}
        base.h
        cases.cc
        chrono.cc
        chrono.h
        config.cc
        config.h
        dead.cc
        hill.cc
        jitter.cc
        keygen.cc
        keygen.h
        log.cc
        log.h
        main.cc
        osal.h
        osal-unix.cc
        test.cc
        test.h
        try.cc
        utils.cc
        utils.h
)

target_link_libraries(${TARGET}
        mdbx
        )

