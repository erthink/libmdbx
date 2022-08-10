/*
 * Copyright 2017-2022 Leonid Yuriev <leo@yuriev.ru>
 * and other libmdbx authors: please see AUTHORS file.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 */

#pragma once

#ifndef NOMINMAX
#define NOMINMAX
#endif

/* Workaround for modern libstdc++ with CLANG < 4.x */
#if defined(__SIZEOF_INT128__) && !defined(__GLIBCXX_TYPE_INT_N_0) &&          \
    defined(__clang__) && __clang_major__ < 4
#define __GLIBCXX_BITSIZE_INT_N_0 128
#define __GLIBCXX_TYPE_INT_N_0 __int128
#endif /* Workaround for modern libstdc++ with CLANG < 4.x */

#if defined(_WIN32) || defined(_WIN64) || defined(_WINDOWS)
#ifndef _WIN32_WINNT
#define _WIN32_WINNT 0x0601 /* Windows 7 */
#endif
#ifdef _MSC_VER
#ifndef _CRT_SECURE_NO_WARNINGS
#define _CRT_SECURE_NO_WARNINGS
#endif /* _CRT_SECURE_NO_WARNINGS */
#pragma warning(push, 1)
#pragma warning(disable : 4548) /* expression before comma has no effect;      \
                                   expected expression with side - effect */
#pragma warning(disable : 4530) /* C++ exception handler used, but unwind      \
                                   semantics are not enabled. Specify /EHsc */
#pragma warning(disable : 4577) /* 'noexcept' used with no exception handling  \
                                   mode specified; termination on exception    \
                                   is not guaranteed. Specify /EHsc */
#endif                          /* _MSC_VER (warnings) */

/* If you wish to build your application for a previous Windows platform,
 * include WinSDKVer.h and set the _WIN32_WINNT macro to the platform you
 * wish to support before including SDKDDKVer.h.
 *
 * TODO: #define _WIN32_WINNT WIN32_MUSTDIE */
#include <SDKDDKVer.h>
#endif /* WINDOWS */

#ifdef __APPLE__
#define _DARWIN_C_SOURCE
#endif

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

#define MDBX_INTERNAL_FUNC
#define MDBX_INTERNAL_VAR extern
#define xMDBX_TOOLS /* Avoid using internal eASSERT() */
#include "../mdbx.h++"
#include "../src/base.h"
#include "../src/osal.h"

#if !defined(__thread) && (defined(_MSC_VER) || defined(__DMC__))
#define __thread __declspec(thread)
#endif /* __thread */

#include "../src/options.h"

#ifdef _MSC_VER
#pragma warning(pop)
#pragma warning(disable : 4201) /* nonstandard extension used :                \
                                   nameless struct / union */
#pragma warning(disable : 4127) /* conditional expression is constant */
#if _MSC_VER < 1900
#pragma warning(disable : 4510) /* default constructor could                   \
                                   not be generated */
#pragma warning(disable : 4512) /* assignment operator could                   \
                                   not be generated  */
#pragma warning(disable : 4610) /* user-defined constructor required */
#ifndef snprintf
#define snprintf(buffer, buffer_size, format, ...)                             \
  _snprintf_s(buffer, buffer_size, _TRUNCATE, format, __VA_ARGS__)
#endif
#ifndef vsnprintf
#define vsnprintf(buffer, buffer_size, format, args)                           \
  _vsnprintf_s(buffer, buffer_size, _TRUNCATE, format, args)
#endif
#pragma warning(disable : 4996) /* 'vsnprintf': This function or variable      \
                                   may be unsafe */
#endif
#endif /* _MSC_VER */
