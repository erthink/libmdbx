# Copyright (c) 2020-2026 Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> ###############################################
# SPDX-License-Identifier: Apache-2.0
#
project(mdbx_ut_and_examples)

add_executable(mdbx_example example-mdbx.c)
if(NOT DEFINED MDBX_LIBRARY)
  set(MDBX_LIBRARY mdbx)
endif()
target_link_libraries(mdbx_example ${MDBX_LIBRARY})

if(CMAKE_CROSSCOMPILING AND NOT CMAKE_CROSSCOMPILING_EMULATOR)
  message(WARNING "No emulator to run cross-compiled tests")
  add_test(NAME fake_since_no_crosscompiling_emulator COMMAND ${CMAKE_COMMAND} -E echo
                                                              "No emulator to run cross-compiled tests")
else()
  add_test(NAME example_c_api COMMAND mdbx_example)
endif()
