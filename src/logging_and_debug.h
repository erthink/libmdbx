/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024

#pragma once

#include "essentials.h"

#ifndef __Wpedantic_format_voidptr
MDBX_MAYBE_UNUSED static inline const void *
__Wpedantic_format_voidptr(const void *ptr) {
  return ptr;
}
#define __Wpedantic_format_voidptr(ARG) __Wpedantic_format_voidptr(ARG)
#endif /* __Wpedantic_format_voidptr */

MDBX_INTERNAL void MDBX_PRINTF_ARGS(4, 5)
    debug_log(int level, const char *function, int line, const char *fmt, ...)
        MDBX_PRINTF_ARGS(4, 5);
MDBX_INTERNAL void debug_log_va(int level, const char *function, int line,
                                const char *fmt, va_list args);

#if MDBX_DEBUG
#define LOG_ENABLED(LVL) unlikely(LVL <= globals.loglevel)
#define AUDIT_ENABLED()                                                        \
  unlikely((globals.runtime_flags & (unsigned)MDBX_DBG_AUDIT))
#else /* MDBX_DEBUG */
#define LOG_ENABLED(LVL) (LVL < MDBX_LOG_VERBOSE && LVL <= globals.loglevel)
#define AUDIT_ENABLED() (0)
#endif /* LOG_ENABLED() & AUDIT_ENABLED() */

#if MDBX_FORCE_ASSERTIONS
#define ASSERT_ENABLED() (1)
#elif MDBX_DEBUG
#define ASSERT_ENABLED()                                                       \
  likely((globals.runtime_flags & (unsigned)MDBX_DBG_ASSERT))
#else
#define ASSERT_ENABLED() (0)
#endif /* ASSERT_ENABLED() */

#define DEBUG_EXTRA(fmt, ...)                                                  \
  do {                                                                         \
    if (LOG_ENABLED(MDBX_LOG_EXTRA))                                           \
      debug_log(MDBX_LOG_EXTRA, __func__, __LINE__, fmt, __VA_ARGS__);         \
  } while (0)

#define DEBUG_EXTRA_PRINT(fmt, ...)                                            \
  do {                                                                         \
    if (LOG_ENABLED(MDBX_LOG_EXTRA))                                           \
      debug_log(MDBX_LOG_EXTRA, nullptr, 0, fmt, __VA_ARGS__);                 \
  } while (0)

#define TRACE(fmt, ...)                                                        \
  do {                                                                         \
    if (LOG_ENABLED(MDBX_LOG_TRACE))                                           \
      debug_log(MDBX_LOG_TRACE, __func__, __LINE__, fmt "\n", __VA_ARGS__);    \
  } while (0)

#define DEBUG(fmt, ...)                                                        \
  do {                                                                         \
    if (LOG_ENABLED(MDBX_LOG_DEBUG))                                           \
      debug_log(MDBX_LOG_DEBUG, __func__, __LINE__, fmt "\n", __VA_ARGS__);    \
  } while (0)

#define VERBOSE(fmt, ...)                                                      \
  do {                                                                         \
    if (LOG_ENABLED(MDBX_LOG_VERBOSE))                                         \
      debug_log(MDBX_LOG_VERBOSE, __func__, __LINE__, fmt "\n", __VA_ARGS__);  \
  } while (0)

#define NOTICE(fmt, ...)                                                       \
  do {                                                                         \
    if (LOG_ENABLED(MDBX_LOG_NOTICE))                                          \
      debug_log(MDBX_LOG_NOTICE, __func__, __LINE__, fmt "\n", __VA_ARGS__);   \
  } while (0)

#define WARNING(fmt, ...)                                                      \
  do {                                                                         \
    if (LOG_ENABLED(MDBX_LOG_WARN))                                            \
      debug_log(MDBX_LOG_WARN, __func__, __LINE__, fmt "\n", __VA_ARGS__);     \
  } while (0)

#undef ERROR /* wingdi.h                                                       \
  Yeah, morons from M$ put such definition to the public header. */

#define ERROR(fmt, ...)                                                        \
  do {                                                                         \
    if (LOG_ENABLED(MDBX_LOG_ERROR))                                           \
      debug_log(MDBX_LOG_ERROR, __func__, __LINE__, fmt "\n", __VA_ARGS__);    \
  } while (0)

#define FATAL(fmt, ...)                                                        \
  debug_log(MDBX_LOG_FATAL, __func__, __LINE__, fmt "\n", __VA_ARGS__);

#if MDBX_DEBUG
#define ASSERT_FAIL(env, msg, func, line) mdbx_assert_fail(env, msg, func, line)
#else /* MDBX_DEBUG */
MDBX_NORETURN __cold void assert_fail(const char *msg, const char *func,
                                      unsigned line);
#define ASSERT_FAIL(env, msg, func, line)                                      \
  do {                                                                         \
    (void)(env);                                                               \
    assert_fail(msg, func, line);                                              \
  } while (0)
#endif /* MDBX_DEBUG */

#define ENSURE_MSG(env, expr, msg)                                             \
  do {                                                                         \
    if (unlikely(!(expr)))                                                     \
      ASSERT_FAIL(env, msg, __func__, __LINE__);                               \
  } while (0)

#define ENSURE(env, expr) ENSURE_MSG(env, expr, #expr)

/* assert(3) variant in environment context */
#define eASSERT(env, expr)                                                     \
  do {                                                                         \
    if (ASSERT_ENABLED())                                                      \
      ENSURE(env, expr);                                                       \
  } while (0)

/* assert(3) variant in cursor context */
#define cASSERT(mc, expr) eASSERT((mc)->txn->env, expr)

/* assert(3) variant in transaction context */
#define tASSERT(txn, expr) eASSERT((txn)->env, expr)

#ifndef xMDBX_TOOLS /* Avoid using internal eASSERT() */
#undef assert
#define assert(expr) eASSERT(nullptr, expr)
#endif

MDBX_MAYBE_UNUSED static inline void jitter4testing(bool tiny) {
#if MDBX_DEBUG
  if (globals.runtime_flags & (unsigned)MDBX_DBG_JITTER)
    osal_jitter(tiny);
#else
  (void)tiny;
#endif
}

MDBX_MAYBE_UNUSED MDBX_INTERNAL void page_list(page_t *mp);

MDBX_INTERNAL const char *pagetype_caption(const uint8_t type,
                                           char buf4unknown[16]);
/* Key size which fits in a DKBUF (debug key buffer). */
#define DKBUF_MAX 127
#define DKBUF char dbg_kbuf[DKBUF_MAX * 4 + 2]
#define DKEY(x) mdbx_dump_val(x, dbg_kbuf, DKBUF_MAX * 2 + 1)
#define DVAL(x)                                                                \
  mdbx_dump_val(x, dbg_kbuf + DKBUF_MAX * 2 + 1, DKBUF_MAX * 2 + 1)

#if MDBX_DEBUG
#define DKBUF_DEBUG DKBUF
#define DKEY_DEBUG(x) DKEY(x)
#define DVAL_DEBUG(x) DVAL(x)
#else
#define DKBUF_DEBUG ((void)(0))
#define DKEY_DEBUG(x) ("-")
#define DVAL_DEBUG(x) ("-")
#endif
