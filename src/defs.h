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
/* *INDENT-OFF* */
/* clang-format off */

#ifndef __GNUC_PREREQ
#   if defined(__GNUC__) && defined(__GNUC_MINOR__)
#       define __GNUC_PREREQ(maj, min) \
          ((__GNUC__ << 16) + __GNUC_MINOR__ >= ((maj) << 16) + (min))
#   else
#       define __GNUC_PREREQ(maj, min) (0)
#   endif
#endif /* __GNUC_PREREQ */

#ifndef __CLANG_PREREQ
#   ifdef __clang__
#       define __CLANG_PREREQ(maj,min) \
          ((__clang_major__ << 16) + __clang_minor__ >= ((maj) << 16) + (min))
#   else
#       define __CLANG_PREREQ(maj,min) (0)
#   endif
#endif /* __CLANG_PREREQ */

#ifndef __GLIBC_PREREQ
#   if defined(__GLIBC__) && defined(__GLIBC_MINOR__)
#       define __GLIBC_PREREQ(maj, min) \
          ((__GLIBC__ << 16) + __GLIBC_MINOR__ >= ((maj) << 16) + (min))
#   else
#       define __GLIBC_PREREQ(maj, min) (0)
#   endif
#endif /* __GLIBC_PREREQ */

#ifndef __has_attribute
#   define __has_attribute(x) (0)
#endif

#ifndef __has_feature
#   define __has_feature(x) (0)
#endif

#ifndef __has_extension
#   define __has_extension(x) (0)
#endif

#ifndef __has_builtin
#   define __has_builtin(x) (0)
#endif

#if __has_feature(thread_sanitizer)
#   define __SANITIZE_THREAD__ 1
#endif

#if __has_feature(address_sanitizer)
#   define __SANITIZE_ADDRESS__ 1
#endif

/*----------------------------------------------------------------------------*/

#ifndef __extern_C
#   ifdef __cplusplus
#       define __extern_C extern "C"
#   else
#       define __extern_C
#   endif
#endif /* __extern_C */

#ifndef __cplusplus
#   ifndef bool
#       define bool _Bool
#   endif
#   ifndef true
#       define true (1)
#   endif
#   ifndef false
#       define false (0)
#   endif
#endif

#if !defined(nullptr) && !defined(__cplusplus) || (__cplusplus < 201103L && !defined(_MSC_VER))
#   define nullptr NULL
#endif

/*----------------------------------------------------------------------------*/

#if !defined(__thread) && (defined(_MSC_VER) || defined(__DMC__))
#   define __thread __declspec(thread)
#endif /* __thread */

#ifndef __alwaysinline
#   if defined(__GNUC__) || __has_attribute(always_inline)
#       define __alwaysinline __inline __attribute__((always_inline))
#   elif defined(_MSC_VER)
#       define __alwaysinline __forceinline
#   else
#       define __alwaysinline
#   endif
#endif /* __alwaysinline */

#ifndef __noinline
#   if defined(__GNUC__) || __has_attribute(noinline)
#       define __noinline __attribute__((noinline))
#   elif defined(_MSC_VER)
#       define __noinline __declspec(noinline)
#   elif defined(__SUNPRO_C) || defined(__sun) || defined(sun)
#       define __noinline inline
#   elif !defined(__INTEL_COMPILER)
#       define __noinline /* FIXME ? */
#   endif
#endif /* __noinline */

#ifndef __must_check_result
#   if defined(__GNUC__) || __has_attribute(warn_unused_result)
#       define __must_check_result __attribute__((warn_unused_result))
#   else
#       define __must_check_result
#   endif
#endif /* __must_check_result */

#ifndef __deprecated
#   if defined(__GNUC__) || __has_attribute(deprecated)
#       define __deprecated __attribute__((deprecated))
#   elif defined(_MSC_VER)
#       define __deprecated __declspec(deprecated)
#   else
#       define __deprecated
#   endif
#endif /* __deprecated */

#if !defined(__noop) && !defined(_MSC_VER)
#	ifdef __cplusplus
		static inline void __noop_consume_args() {}
		template <typename First, typename... Rest>
		static inline void
		__noop_consume_args(const First &first, const Rest &... rest) {
			(void) first; __noop_consume_args(rest...);
		}
#		define __noop(...) __noop_consume_args(__VA_ARGS__)
#	elif defined(__GNUC__) && (!defined(__STRICT_ANSI__) || !__STRICT_ANSI__)
		static __inline void __noop_consume_args(void* anchor, ...) {
			(void) anchor;
		}
#		define __noop(...) __noop_consume_args(0, ##__VA_ARGS__)
#	else
#		define __noop(...) do {} while(0)
#	endif
#endif /* __noop */

#ifndef __fallthrough
#	if __GNUC_PREREQ(7, 0) || __has_attribute(fallthrough)
#		define __fallthrough __attribute__((fallthrough))
#	else
#		define __fallthrough __noop()
#	endif
#endif /* __fallthrough */

#ifndef __unreachable
#	if __GNUC_PREREQ(4,5)
#		define __unreachable() __builtin_unreachable()
#	elif defined(_MSC_VER)
#		define __unreachable() __assume(0)
#	else
#		define __unreachable() __noop()
#	endif
#endif /* __unreachable */

#ifndef __prefetch
#	if defined(__GNUC__) || defined(__clang__)
#		define __prefetch(ptr) __builtin_prefetch(ptr)
#	else
#		define __prefetch(ptr) __noop(ptr)
#	endif
#endif /* __prefetch */

#ifndef __aligned
#   if defined(__GNUC__) || __has_attribute(aligned)
#       define __aligned(N) __attribute__((aligned(N)))
#   elif defined(_MSC_VER)
#       define __aligned(N) __declspec(align(N))
#   else
#       define __aligned(N)
#   endif
#endif /* __aligned */

#ifndef __noreturn
#   if defined(__GNUC__) || __has_attribute(noreturn)
#       define __noreturn __attribute__((noreturn))
#   elif defined(_MSC_VER)
#       define __noreturn __declspec(noreturn)
#   else
#       define __noreturn
#   endif
#endif /* __noreturn */

#ifndef __nothrow
#   if defined(__GNUC__) || __has_attribute(nothrow)
#       define __nothrow __attribute__((nothrow))
#   elif defined(_MSC_VER) && defined(__cplusplus)
#       define __nothrow __declspec(nothrow)
#   else
#       define __nothrow
#   endif
#endif /* __nothrow */

#ifndef __pure_function
    /* Many functions have no effects except the return value and their
     * return value depends only on the parameters and/or global variables.
     * Such a function can be subject to common subexpression elimination
     * and loop optimization just as an arithmetic operator would be.
     * These functions should be declared with the attribute pure. */
#   if defined(__GNUC__) || __has_attribute(pure)
#       define __pure_function __attribute__((pure))
#   else
#       define __pure_function
#   endif
#endif /* __pure_function */

#ifndef __const_function
    /* Many functions do not examine any values except their arguments,
     * and have no effects except the return value. Basically this is just
     * slightly more strict class than the PURE attribute, since function
     * is not allowed to read global memory.
     *
     * Note that a function that has pointer arguments and examines the
     * data pointed to must not be declared const. Likewise, a function
     * that calls a non-const function usually must not be const.
     * It does not make sense for a const function to return void. */
#   if defined(__GNUC__) || __has_attribute(const)
#       define __const_function __attribute__((const))
#   else
#       define __const_function
#   endif
#endif /* __const_function */

#ifndef __dll_hidden
#   if defined(__GNUC__) || __has_attribute(visibility)
#       define __hidden __attribute__((visibility("hidden")))
#   else
#       define __hidden
#   endif
#endif /* __dll_hidden */

#ifndef __optimize
#   if defined(__OPTIMIZE__)
#     if defined(__clang__) && !__has_attribute(optimize)
#           define __optimize(ops)
#       elif defined(__GNUC__) || __has_attribute(optimize)
#           define __optimize(ops) __attribute__((optimize(ops)))
#       else
#           define __optimize(ops)
#       endif
#   else
#           define __optimize(ops)
#   endif
#endif /* __optimize */

#ifndef __hot
#   if defined(__OPTIMIZE__)
#       if defined(__clang__) && !__has_attribute(hot)
            /* just put frequently used functions in separate section */
#           define __hot __attribute__((section("text.hot"))) __optimize("O3")
#       elif defined(__GNUC__) || __has_attribute(hot)
#           define __hot __attribute__((hot)) __optimize("O3")
#       else
#           define __hot  __optimize("O3")
#       endif
#   else
#       define __hot
#   endif
#endif /* __hot */

#ifndef __cold
#   if defined(__OPTIMIZE__)
#       if defined(__clang__) && !__has_attribute(cold)
            /* just put infrequently used functions in separate section */
#           define __cold __attribute__((section("text.unlikely"))) __optimize("Os")
#       elif defined(__GNUC__) || __has_attribute(cold)
#           define __cold __attribute__((cold)) __optimize("Os")
#       else
#           define __cold __optimize("Os")
#       endif
#   else
#       define __cold
#   endif
#endif /* __cold */

#ifndef __flatten
#   if defined(__OPTIMIZE__) && (defined(__GNUC__) || __has_attribute(flatten))
#       define __flatten __attribute__((flatten))
#   else
#       define __flatten
#   endif
#endif /* __flatten */

#ifndef likely
#   if defined(__GNUC__) || defined(__clang__)
#       define likely(cond) __builtin_expect(!!(cond), 1)
#   else
#       define likely(x) (x)
#   endif
#endif /* likely */

#ifndef unlikely
#   if defined(__GNUC__) || defined(__clang__)
#       define unlikely(cond) __builtin_expect(!!(cond), 0)
#   else
#       define unlikely(x) (x)
#   endif
#endif /* unlikely */

/* Wrapper around __func__, which is a C99 feature */
#if defined(__STDC_VERSION__) && __STDC_VERSION__ >= 199901L
#   define mdbx_func_ __func__
#elif (defined(__GNUC__) && __GNUC__ >= 2) || defined(__clang__) || defined(_MSC_VER)
#   define mdbx_func_ __FUNCTION__
#else
#   define mdbx_func_ "<mdbx_unknown>"
#endif

/*----------------------------------------------------------------------------*/

#if defined(USE_VALGRIND)
#   include <valgrind/memcheck.h>
#   ifndef VALGRIND_DISABLE_ADDR_ERROR_REPORTING_IN_RANGE
        /* LY: available since Valgrind 3.10 */
#       define VALGRIND_DISABLE_ADDR_ERROR_REPORTING_IN_RANGE(a,s)
#       define VALGRIND_ENABLE_ADDR_ERROR_REPORTING_IN_RANGE(a,s)
#   endif
#elif !defined(RUNNING_ON_VALGRIND)
#   define VALGRIND_CREATE_MEMPOOL(h,r,z)
#   define VALGRIND_DESTROY_MEMPOOL(h)
#   define VALGRIND_MEMPOOL_TRIM(h,a,s)
#   define VALGRIND_MEMPOOL_ALLOC(h,a,s)
#   define VALGRIND_MEMPOOL_FREE(h,a)
#   define VALGRIND_MEMPOOL_CHANGE(h,a,b,s)
#   define VALGRIND_MAKE_MEM_NOACCESS(a,s)
#   define VALGRIND_MAKE_MEM_DEFINED(a,s)
#   define VALGRIND_MAKE_MEM_UNDEFINED(a,s)
#   define VALGRIND_DISABLE_ADDR_ERROR_REPORTING_IN_RANGE(a,s)
#   define VALGRIND_ENABLE_ADDR_ERROR_REPORTING_IN_RANGE(a,s)
#   define VALGRIND_CHECK_MEM_IS_ADDRESSABLE(a,s) (0)
#   define VALGRIND_CHECK_MEM_IS_DEFINED(a,s) (0)
#   define RUNNING_ON_VALGRIND (0)
#endif /* USE_VALGRIND */

#ifdef __SANITIZE_ADDRESS__
#   include <sanitizer/asan_interface.h>
#elif !defined(ASAN_POISON_MEMORY_REGION)
#   define ASAN_POISON_MEMORY_REGION(addr, size) \
        ((void)(addr), (void)(size))
#   define ASAN_UNPOISON_MEMORY_REGION(addr, size) \
        ((void)(addr), (void)(size))
#endif /* __SANITIZE_ADDRESS__ */

/*----------------------------------------------------------------------------*/

#ifndef ARRAY_LENGTH
#   ifdef __cplusplus
        template <typename T, size_t N>
        char (&__ArraySizeHelper(T (&array)[N]))[N];
#       define ARRAY_LENGTH(array) (sizeof(::__ArraySizeHelper(array)))
#   else
#       define ARRAY_LENGTH(array) (sizeof(array) / sizeof(array[0]))
#   endif
#endif /* ARRAY_LENGTH */

#ifndef ARRAY_END
#   define ARRAY_END(array) (&array[ARRAY_LENGTH(array)])
#endif /* ARRAY_END */

#ifndef STRINGIFY
#   define STRINGIFY_HELPER(x) #x
#   define STRINGIFY(x) STRINGIFY_HELPER(x)
#endif /* STRINGIFY */

#ifndef offsetof
#   define offsetof(type, member)  __builtin_offsetof(type, member)
#endif /* offsetof */

#ifndef container_of
#   define container_of(ptr, type, member) \
        ((type *)((char *)(ptr) - offsetof(type, member)))
#endif /* container_of */

#define MDBX_TETRAD(a, b, c, d)                                                \
  ((uint32_t)(a) << 24 | (uint32_t)(b) << 16 | (uint32_t)(c) << 8 | (d))

#define MDBX_STRING_TETRAD(str) MDBX_TETRAD(str[0], str[1], str[2], str[3])

#define FIXME "FIXME: " __FILE__ ", " STRINGIFY(__LINE__)

#ifndef STATIC_ASSERT_MSG
#   if (defined(__STDC_VERSION__) && __STDC_VERSION__ >= 201112L) \
          || __has_feature(c_static_assert)
#       define STATIC_ASSERT_MSG(expr, msg) _Static_assert(expr, msg)
#   elif defined(static_assert)
#       define STATIC_ASSERT_MSG(expr, msg) static_assert(expr, msg)
#   elif defined(_MSC_VER)
#       include <crtdbg.h>
#       define STATIC_ASSERT_MSG(expr, msg) _STATIC_ASSERT(expr)
#   else
#       define STATIC_ASSERT_MSG(expr, msg) switch (0) {case 0:case (expr):;}
#   endif
#endif /* STATIC_ASSERT */

#ifndef STATIC_ASSERT
#   define STATIC_ASSERT(expr) STATIC_ASSERT_MSG(expr, #expr)
#endif

/* *INDENT-ON* */
/* clang-format on */
