cmake_minimum_required(VERSION 2.8.7)

set(TARGET libmdbx)
project(${TARGET})

enable_language(C)
enable_language(CXX)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED on)

add_definitions(-DNDEBUG=1 -DMDBX_DEBUG=0 -DLIBMDBX_EXPORTS=1 -D_GNU_SOURCE=1)

get_directory_property(hasParent PARENT_DIRECTORY)
if (hasParent)
    set(STANDALONE_BUILD 0)
else ()
    set(STANDALONE_BUILD 1)
    enable_testing()

    if (CMAKE_C_COMPILER_ID MATCHES GNU)
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O2")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g3")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-constant-logical-operand")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Werror")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wextra")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -ffunction-sections")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fvisibility=hidden")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=gnu11")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -pthread")
    endif ()

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
    endif ()

    if (COVERAGE)
        if (NOT "${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
            message(FATAL_ERROR "Coverage requires -DCMAKE_BUILD_TYPE=Debug Current value=${CMAKE_BUILD_TYPE}")
        endif ()

        message(STATUS "Setting coverage compiler flags")

        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -ggdb3 -O0 --coverage -fprofile-arcs -ftest-coverage")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS}     -g -ggdb3 -O0 --coverage -fprofile-arcs -ftest-coverage")

        add_definitions(-DCOVERAGE_TEST)
    endif ()

    if (TRAVIS)
        add_definitions(-DNO_MLOCK)
    else ()
        set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address -fsanitize=leak -fstack-protector-strong -static-libasan")
    endif ()
endif ()

add_library(${TARGET} STATIC
        mdbx.h
        src/bits.h
        src/defs.h
        src/lck-posix.c
        src/mdbx.c
        src/osal.c
        src/osal.h
        src/version.c
        )

target_include_directories(${TARGET} PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
        )

add_subdirectory(tutorial)
add_subdirectory(test)
add_subdirectory(src/tools)
