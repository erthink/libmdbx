/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025
/// \copyright SPDX-License-Identifier: Apache-2.0

#pragma once

#include "../src/essentials.h"

#ifdef _MSC_VER
#pragma warning(push, 1)
#pragma warning(disable : 4548) /* expression before comma has no effect;                                              \
                                   expected expression with side - effect */
#pragma warning(disable : 4530) /* C++ exception handler used, but unwind                                              \
                                   semantics are not enabled. Specify /EHsc */
#pragma warning(disable : 4577) /* 'noexcept' used with no exception handling                                          \
                                   mode specified; termination on exception                                            \
                                   is not guaranteed. Specify /EHsc */
#endif                          /* _MSC_VER (warnings) */

#if defined(_WIN32) || defined(_WIN64) || defined(_WINDOWS)
/* If you wish to build your application for a previous Windows platform,
 * include WinSDKVer.h and set the _WIN32_WINNT macro to the platform you
 * wish to support before including SDKDDKVer.h.
 *
 * TODO: #define _WIN32_WINNT WIN32_MUSTDIE */
#include <SDKDDKVer.h>
#endif /* WINDOWS */

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#if defined(_WIN32) || defined(_WIN64) || defined(_WINDOWS)
#include <io.h>
#else
#include <fcntl.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#endif

#ifdef _BSD_SOURCE
#include <endian.h>
#endif

#include <algorithm>
#include <cassert>
#include <cinttypes> // for PRId64, PRIu64
#include <cstdarg>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#define MDBX_INTERNAL
#define xMDBX_TOOLS /* Avoid using internal eASSERT() */
#include "../mdbx.h++"
#include "../src/osal.h"

#include "../src/options.h"

#ifdef _MSC_VER
#pragma warning(pop)
#pragma warning(disable : 4201) /* nonstandard extension used: nameless                                                \
                                   struct/union */
#pragma warning(disable : 4127) /* conditional expression is constant */
#if _MSC_VER < 1900
#pragma warning(disable : 4510) /* default constructor could                                                           \
                                   not be generated */
#pragma warning(disable : 4512) /* assignment operator could                                                           \
                                   not be generated  */
#pragma warning(disable : 4610) /* user-defined constructor required */
#ifndef snprintf
#define snprintf(buffer, buffer_size, format, ...) _snprintf_s(buffer, buffer_size, _TRUNCATE, format, __VA_ARGS__)
#endif
#ifndef vsnprintf
#define vsnprintf(buffer, buffer_size, format, args) _vsnprintf_s(buffer, buffer_size, _TRUNCATE, format, args)
#endif
#pragma warning(disable : 4996) /* 'vsnprintf': This function or variable                                              \
                                   may be unsafe */
#endif
#endif /* _MSC_VER */
