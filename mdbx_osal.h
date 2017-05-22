/* https://en.wikipedia.org/wiki/Operating_system_abstraction_layer */

/*
 * Copyright 2015-2017 Leonid Yuriev <leo@yuriev.ru>
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

#ifndef __has_attribute
#define __has_attribute(x) (0)
#endif

#ifndef __dll_export
#if defined(_WIN32) || defined(__CYGWIN__)
#if defined(__GNUC__) || __has_attribute(dllexport)
#define __dll_export __attribute__((dllexport))
#elif defined(_MSC_VER)
#define __dll_export __declspec(dllexport)
#else
#define __dll_export
#endif
#elif defined(__GNUC__) || __has_attribute(visibility)
#define __dll_export __attribute__((visibility("default")))
#else
#define __dll_export
#endif
#endif /* __dll_export */

#ifndef __dll_import
#if defined(_WIN32) || defined(__CYGWIN__)
#if defined(__GNUC__) || __has_attribute(dllimport)
#define __dll_import __attribute__((dllimport))
#elif defined(_MSC_VER)
#define __dll_import __declspec(dllimport)
#else
#define __dll_import
#endif
#else
#define __dll_import
#endif
#endif /* __dll_import */

#if defined(LIBMDBX_EXPORTS)
#define LIBMDBX_API __dll_export
#elif defined(LIBMDBX_IMPORTS)
#define LIBMDBX_API __dll_import
#else
#define LIBMDBX_API
#endif /* LIBMDBX_API */

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4514) /* 'xyz': unreferenced inline function         \
                                   has been removed */
#pragma warning(disable : 4710) /* 'xyz': function not inlined */
#pragma warning(disable : 4711) /* function 'xyz' selected for                 \
                                   automatic inline expansion */
#pragma warning(disable : 4061) /* enumerator 'abc' in switch of enum          \
                                   'xyz' is not explicitly handled by a case   \
                                   label */
#pragma warning(disable : 4201) /* nonstandard extension used :                \
                                   nameless struct / union */
#pragma warning(disable : 4127) /* conditional expression is constant          \
                                   */

#pragma warning(push, 1)
#pragma warning(disable : 4530) /* C++ exception handler used, but             \
                                    unwind semantics are not enabled. Specify  \
                                    /EHsc */
#pragma warning(disable : 4577) /* 'noexcept' used with no exception           \
                                    handling mode specified; termination on    \
                                    exception is not guaranteed. Specify /EHsc \
                                    */
#endif                          /* _MSC_VER (warnings) */

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>

#if defined(_WIN32) || defined(_WIN64)

#include <windows.h>
#include <winnt.h>
typedef unsigned mode_t;
typedef HANDLE mdbx_filehandle_t;
typedef DWORD mdbx_pid_t;
typedef DWORD mdbx_tid_t;
#define MDBX_ENODATA ERROR_HANDLE_EOF
#define MDBX_EINVAL ERROR_INVALID_PARAMETER
#define MDBX_EACCESS ERROR_ACCESS_DENIED
#define MDBX_ENOMEM ERROR_OUTOFMEMORY
#define MDBX_EROFS ERROR_FILE_READ_ONLY
#define MDBX_ENOSYS ERROR_NOT_SUPPORTED
#define MDBX_EIO ERROR_WRITE_FAULT

#else

#include <errno.h>     /* for error codes */
#include <pthread.h>   /* for pthread_t */
#include <sys/types.h> /* for pid_t */
#include <sys/uio.h>   /* for truct iovec */
#define HAVE_STRUCT_IOVEC 1
typedef int mdbx_filehandle_t;
typedef pid_t mdbx_pid_t;
typedef pthread_t mdbx_tid_t;
#define MDBX_ENODATA ENODATA
#define MDBX_EINVAL EINVAL
#define MDBX_EACCESS EACCES
#define MDBX_ENOMEM ENOMEM
#define MDBX_EROFS EROFS
#define MDBX_ENOSYS ENOSYS
#define MDBX_EIO EIO
#endif

/*--------------------------------------------------------------------------*/
