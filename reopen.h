/*
 * Copyright 2015-2017 Leonid Yuriev <leo@yuriev.ru>.
 * Copyright 2015,2016 Peter-Service R&D LLC.
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

#ifndef _REOPEN_H
#define _REOPEN_H

#ifndef __CLANG_PREREQ
#	ifdef __clang__
#		define __CLANG_PREREQ(maj,min) \
			((__clang_major__ << 16) + __clang_minor__ >= ((maj) << 16) + (min))
#	else
#		define __CLANG_PREREQ(maj,min) (0)
#	endif
#endif /* __CLANG_PREREQ */

#ifndef __has_attribute
#	define __has_attribute(x) (0)
#endif

#if !defined(__thread) && (defined(_MSC_VER) || defined(__DMC__))
#	define __thread __declspec(thread)
#endif

#ifndef __forceinline
#	if defined(__GNUC__) || defined(__clang__)
#		define __forceinline __inline __attribute__((always_inline))
#	elif ! defined(_MSC_VER)
#		define __forceinline
#	endif
#endif /* __forceinline */

#ifndef __noinline
#	if defined(__GNUC__) || defined(__clang__)
#		define __noinline __attribute__((noinline))
#	elif defined(_MSC_VER)
#		define __noinline __declspec(noinline)
#	endif
#endif /* __noinline */

#ifndef __must_check_result
#	if defined(__GNUC__) || defined(__clang__)
#		define __must_check_result __attribute__((warn_unused_result))
#	else
#		define __must_check_result
#	endif
#endif /* __must_check_result */

#ifndef __hot
#	if defined(__OPTIMIZE__) && (defined(__GNUC__) && !defined(__clang__))
#		define __hot __attribute__((hot, optimize("O3")))
#	elif defined(__GNUC__)
		/* cland case, just put frequently used functions in separate section */
#		define __hot __attribute__((section("text.hot")))
#	else
#		define __hot
#	endif
#endif /* __hot */

#ifndef __cold
#	if defined(__OPTIMIZE__) && (defined(__GNUC__) && !defined(__clang__))
#		define __cold __attribute__((cold, optimize("Os")))
#	elif defined(__GNUC__)
		/* cland case, just put infrequently used functions in separate section */
#		define __cold __attribute__((section("text.unlikely")))
#	else
#		define __cold
#	endif
#endif /* __cold */

#ifndef __flatten
#	if defined(__OPTIMIZE__) && (defined(__GNUC__) || defined(__clang__))
#		define __flatten __attribute__((flatten))
#	else
#		define __flatten
#	endif
#endif /* __flatten */

#ifndef __aligned
#	if defined(__GNUC__) || defined(__clang__)
#		define __aligned(N) __attribute__((aligned(N)))
#	elif defined(__MSC_VER)
#		define __aligned(N) __declspec(align(N))
#	else
#		define __aligned(N)
#	endif
#endif /* __align */

#ifndef __noreturn
#	if defined(__GNUC__) || defined(__clang__)
#		define __noreturn __attribute__((noreturn))
#	elif defined(__MSC_VER)
#		define __noreturn __declspec(noreturn)
#	else
#		define __noreturn
#	endif
#endif

#ifndef __nothrow
#	if defined(__GNUC__) || defined(__clang__)
#		define __nothrow __attribute__((nothrow))
#	elif defined(__MSC_VER)
#		define __nothrow __declspec(nothrow)
#	else
#		define __nothrow
#	endif
#endif

#ifndef CACHELINE_SIZE
#	if defined(__ia64__) || defined(__ia64) || defined(_M_IA64)
#		define CACHELINE_SIZE 128
#	else
#		define CACHELINE_SIZE 64
#	endif
#endif

#ifndef __cache_aligned
#	define __cache_aligned __aligned(CACHELINE_SIZE)
#endif

#ifndef likely
#	if defined(__GNUC__) || defined(__clang__)
#		ifdef __cplusplus
			/* LY: workaround for "pretty" boost */
			static __inline __attribute__((always_inline))
				bool likely(bool cond) { return __builtin_expect(cond, 1); }
#		else
#			define likely(cond) __builtin_expect(!!(cond), 1)
#		endif
#	else
#		define likely(x) (x)
#	endif
#endif /* likely */

#ifndef unlikely
#	if defined(__GNUC__) || defined(__clang__)
#		ifdef __cplusplus
			/* LY: workaround for "pretty" boost */
			static __inline __attribute__((always_inline))
				bool unlikely(bool cond) { return __builtin_expect(cond, 0); }
#		else
#			define unlikely(cond) __builtin_expect(!!(cond), 0)
#		endif
#	else
#		define unlikely(x) (x)
#	endif
#endif /* unlikely */

#ifndef __extern_C
#    ifdef __cplusplus
#        define __extern_C extern "C"
#    else
#        define __extern_C
#    endif
#endif

#ifndef __noop
#    define __noop() do {} while (0)
#endif

/* -------------------------------------------------------------------------- */

#include <assert.h>

/* Prototype should match libc runtime. ISO POSIX (2003) & LSB 3.1 */
__extern_C void __assert_fail(
		const char* assertion,
		const char* file,
		unsigned line,
		const char* function) __nothrow __noreturn;

/* -------------------------------------------------------------------------- */

#if defined(HAVE_VALGRIND) || defined(USE_VALGRIND)
	/* Get debugging help from Valgrind */
#	include <valgrind/memcheck.h>
#	ifndef VALGRIND_DISABLE_ADDR_ERROR_REPORTING_IN_RANGE
		/* LY: available since Valgrind 3.10 */
#		define VALGRIND_DISABLE_ADDR_ERROR_REPORTING_IN_RANGE(a,s)
#		define VALGRIND_ENABLE_ADDR_ERROR_REPORTING_IN_RANGE(a,s)
#	endif
#else
#	define VALGRIND_CREATE_MEMPOOL(h,r,z)
#	define VALGRIND_DESTROY_MEMPOOL(h)
#	define VALGRIND_MEMPOOL_TRIM(h,a,s)
#	define VALGRIND_MEMPOOL_ALLOC(h,a,s)
#	define VALGRIND_MEMPOOL_FREE(h,a)
#	define VALGRIND_MEMPOOL_CHANGE(h,a,b,s)
#	define VALGRIND_MAKE_MEM_NOACCESS(a,s)
#	define VALGRIND_MAKE_MEM_DEFINED(a,s)
#	define VALGRIND_MAKE_MEM_UNDEFINED(a,s)
#	define VALGRIND_DISABLE_ADDR_ERROR_REPORTING_IN_RANGE(a,s)
#	define VALGRIND_ENABLE_ADDR_ERROR_REPORTING_IN_RANGE(a,s)
#	define VALGRIND_CHECK_MEM_IS_ADDRESSABLE(a,s) (0)
#	define VALGRIND_CHECK_MEM_IS_DEFINED(a,s) (0)
#endif /* ! USE_VALGRIND */

#if defined(__has_feature)
#	if __has_feature(thread_sanitizer)
#		define __SANITIZE_THREAD__ 1
#	endif
#endif

#ifdef __SANITIZE_THREAD__
#	define ATTRIBUTE_NO_SANITIZE_THREAD __attribute__((no_sanitize_thread, noinline))
#else
#	define ATTRIBUTE_NO_SANITIZE_THREAD
#endif

#if defined(__has_feature)
#	if __has_feature(address_sanitizer)
#		define __SANITIZE_ADDRESS__ 1
#	endif
#endif

#ifdef __SANITIZE_ADDRESS__
#	include <sanitizer/asan_interface.h>
#	define ATTRIBUTE_NO_SANITIZE_ADDRESS __attribute__((no_sanitize_address, noinline))
#else
#	define ASAN_POISON_MEMORY_REGION(addr, size) \
		((void)(addr), (void)(size))
#	define ASAN_UNPOISON_MEMORY_REGION(addr, size) \
		((void)(addr), (void)(size))
#	define ATTRIBUTE_NO_SANITIZE_ADDRESS
#endif /* __SANITIZE_ADDRESS__ */

#endif /* _REOPEN_H */
