/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024

#pragma once

#include "essentials.h"

#ifndef __cplusplus

#ifdef MDBX_HAVE_C11ATOMICS
#define osal_memory_fence(order, write) atomic_thread_fence((write) ? mo_c11_store(order) : mo_c11_load(order))
#else /* MDBX_HAVE_C11ATOMICS */
#define osal_memory_fence(order, write)                                                                                \
  do {                                                                                                                 \
    osal_compiler_barrier();                                                                                           \
    if (write && order > (MDBX_CPU_WRITEBACK_INCOHERENT ? mo_Relaxed : mo_AcquireRelease))                             \
      osal_memory_barrier();                                                                                           \
  } while (0)
#endif /* MDBX_HAVE_C11ATOMICS */

#if defined(MDBX_HAVE_C11ATOMICS) && defined(__LCC__)
#define atomic_store32(p, value, order)                                                                                \
  ({                                                                                                                   \
    const uint32_t value_to_store = (value);                                                                           \
    atomic_store_explicit(MDBX_c11a_rw(uint32_t, p), value_to_store, mo_c11_store(order));                             \
    value_to_store;                                                                                                    \
  })
#define atomic_load32(p, order) atomic_load_explicit(MDBX_c11a_ro(uint32_t, p), mo_c11_load(order))
#define atomic_store64(p, value, order)                                                                                \
  ({                                                                                                                   \
    const uint64_t value_to_store = (value);                                                                           \
    atomic_store_explicit(MDBX_c11a_rw(uint64_t, p), value_to_store, mo_c11_store(order));                             \
    value_to_store;                                                                                                    \
  })
#define atomic_load64(p, order) atomic_load_explicit(MDBX_c11a_ro(uint64_t, p), mo_c11_load(order))
#endif /* LCC && MDBX_HAVE_C11ATOMICS */

#ifndef atomic_store32
MDBX_MAYBE_UNUSED static __always_inline uint32_t atomic_store32(mdbx_atomic_uint32_t *p, const uint32_t value,
                                                                 enum mdbx_memory_order order) {
  STATIC_ASSERT(sizeof(mdbx_atomic_uint32_t) == 4);
#ifdef MDBX_HAVE_C11ATOMICS
  assert(atomic_is_lock_free(MDBX_c11a_rw(uint32_t, p)));
  atomic_store_explicit(MDBX_c11a_rw(uint32_t, p), value, mo_c11_store(order));
#else  /* MDBX_HAVE_C11ATOMICS */
  if (order != mo_Relaxed)
    osal_compiler_barrier();
  p->weak = value;
  osal_memory_fence(order, true);
#endif /* MDBX_HAVE_C11ATOMICS */
  return value;
}
#endif /* atomic_store32 */

#ifndef atomic_load32
MDBX_MAYBE_UNUSED static __always_inline uint32_t atomic_load32(const volatile mdbx_atomic_uint32_t *p,
                                                                enum mdbx_memory_order order) {
  STATIC_ASSERT(sizeof(mdbx_atomic_uint32_t) == 4);
#ifdef MDBX_HAVE_C11ATOMICS
  assert(atomic_is_lock_free(MDBX_c11a_ro(uint32_t, p)));
  return atomic_load_explicit(MDBX_c11a_ro(uint32_t, p), mo_c11_load(order));
#else  /* MDBX_HAVE_C11ATOMICS */
  osal_memory_fence(order, false);
  const uint32_t value = p->weak;
  if (order != mo_Relaxed)
    osal_compiler_barrier();
  return value;
#endif /* MDBX_HAVE_C11ATOMICS */
}
#endif /* atomic_load32 */

/*------------------------------------------------------------------------------
 * safe read/write volatile 64-bit fields on 32-bit architectures. */

/* LY: for testing non-atomic 64-bit txnid on 32-bit arches.
 * #define xMDBX_TXNID_STEP (UINT32_MAX / 3) */
#ifndef xMDBX_TXNID_STEP
#if MDBX_64BIT_CAS
#define xMDBX_TXNID_STEP 1u
#else
#define xMDBX_TXNID_STEP 2u
#endif
#endif /* xMDBX_TXNID_STEP */

#ifndef atomic_store64
MDBX_MAYBE_UNUSED static __always_inline uint64_t atomic_store64(mdbx_atomic_uint64_t *p, const uint64_t value,
                                                                 enum mdbx_memory_order order) {
  STATIC_ASSERT(sizeof(mdbx_atomic_uint64_t) == 8);
#if MDBX_64BIT_ATOMIC
#if __GNUC_PREREQ(11, 0)
  STATIC_ASSERT(__alignof__(mdbx_atomic_uint64_t) >= sizeof(uint64_t));
#endif /* GNU C >= 11 */
#ifdef MDBX_HAVE_C11ATOMICS
  assert(atomic_is_lock_free(MDBX_c11a_rw(uint64_t, p)));
  atomic_store_explicit(MDBX_c11a_rw(uint64_t, p), value, mo_c11_store(order));
#else  /* MDBX_HAVE_C11ATOMICS */
  if (order != mo_Relaxed)
    osal_compiler_barrier();
  p->weak = value;
  osal_memory_fence(order, true);
#endif /* MDBX_HAVE_C11ATOMICS */
#else  /* !MDBX_64BIT_ATOMIC */
  osal_compiler_barrier();
  atomic_store32(&p->low, (uint32_t)value, mo_Relaxed);
  jitter4testing(true);
  atomic_store32(&p->high, (uint32_t)(value >> 32), order);
  jitter4testing(true);
#endif /* !MDBX_64BIT_ATOMIC */
  return value;
}
#endif /* atomic_store64 */

#ifndef atomic_load64
MDBX_MAYBE_UNUSED static
#if MDBX_64BIT_ATOMIC
    __always_inline
#endif /* MDBX_64BIT_ATOMIC */
        uint64_t
        atomic_load64(const volatile mdbx_atomic_uint64_t *p, enum mdbx_memory_order order) {
  STATIC_ASSERT(sizeof(mdbx_atomic_uint64_t) == 8);
#if MDBX_64BIT_ATOMIC
#ifdef MDBX_HAVE_C11ATOMICS
  assert(atomic_is_lock_free(MDBX_c11a_ro(uint64_t, p)));
  return atomic_load_explicit(MDBX_c11a_ro(uint64_t, p), mo_c11_load(order));
#else  /* MDBX_HAVE_C11ATOMICS */
  osal_memory_fence(order, false);
  const uint64_t value = p->weak;
  if (order != mo_Relaxed)
    osal_compiler_barrier();
  return value;
#endif /* MDBX_HAVE_C11ATOMICS */
#else  /* !MDBX_64BIT_ATOMIC */
  osal_compiler_barrier();
  uint64_t value = (uint64_t)atomic_load32(&p->high, order) << 32;
  jitter4testing(true);
  value |= atomic_load32(&p->low, (order == mo_Relaxed) ? mo_Relaxed : mo_AcquireRelease);
  jitter4testing(true);
  for (;;) {
    osal_compiler_barrier();
    uint64_t again = (uint64_t)atomic_load32(&p->high, order) << 32;
    jitter4testing(true);
    again |= atomic_load32(&p->low, (order == mo_Relaxed) ? mo_Relaxed : mo_AcquireRelease);
    jitter4testing(true);
    if (likely(value == again))
      return value;
    value = again;
  }
#endif /* !MDBX_64BIT_ATOMIC */
}
#endif /* atomic_load64 */

MDBX_MAYBE_UNUSED static __always_inline void atomic_yield(void) {
#if defined(_WIN32) || defined(_WIN64)
  YieldProcessor();
#elif defined(__ia32__) || defined(__e2k__)
  __builtin_ia32_pause();
#elif defined(__ia64__)
#if defined(__HP_cc__) || defined(__HP_aCC__)
  _Asm_hint(_HINT_PAUSE);
#else
  __asm__ __volatile__("hint @pause");
#endif
#elif defined(__aarch64__) || (defined(__ARM_ARCH) && __ARM_ARCH > 6) || defined(__ARM_ARCH_6K__)
#ifdef __CC_ARM
  __yield();
#else
  __asm__ __volatile__("yield");
#endif
#elif (defined(__mips64) || defined(__mips64__)) && defined(__mips_isa_rev) && __mips_isa_rev >= 2
  __asm__ __volatile__("pause");
#elif defined(__mips) || defined(__mips__) || defined(__mips64) || defined(__mips64__) || defined(_M_MRX000) ||        \
    defined(_MIPS_) || defined(__MWERKS__) || defined(__sgi)
  __asm__ __volatile__(".word 0x00000140");
#elif defined(__linux__) || defined(__gnu_linux__) || defined(_UNIX03_SOURCE)
  sched_yield();
#elif (defined(_GNU_SOURCE) && __GLIBC_PREREQ(2, 1)) || defined(_OPEN_THREADS)
  pthread_yield();
#endif
}

#if MDBX_64BIT_CAS
MDBX_MAYBE_UNUSED static __always_inline bool atomic_cas64(mdbx_atomic_uint64_t *p, uint64_t c, uint64_t v) {
#ifdef MDBX_HAVE_C11ATOMICS
  STATIC_ASSERT(sizeof(long long) >= sizeof(uint64_t));
  assert(atomic_is_lock_free(MDBX_c11a_rw(uint64_t, p)));
  return atomic_compare_exchange_strong(MDBX_c11a_rw(uint64_t, p), &c, v);
#elif defined(__GNUC__) || defined(__clang__)
  return __sync_bool_compare_and_swap(&p->weak, c, v);
#elif defined(_MSC_VER)
  return c == (uint64_t)_InterlockedCompareExchange64((volatile __int64 *)&p->weak, v, c);
#elif defined(__APPLE__)
  return OSAtomicCompareAndSwap64Barrier(c, v, &p->weak);
#else
#error FIXME: Unsupported compiler
#endif
}
#endif /* MDBX_64BIT_CAS */

MDBX_MAYBE_UNUSED static __always_inline bool atomic_cas32(mdbx_atomic_uint32_t *p, uint32_t c, uint32_t v) {
#ifdef MDBX_HAVE_C11ATOMICS
  STATIC_ASSERT(sizeof(int) >= sizeof(uint32_t));
  assert(atomic_is_lock_free(MDBX_c11a_rw(uint32_t, p)));
  return atomic_compare_exchange_strong(MDBX_c11a_rw(uint32_t, p), &c, v);
#elif defined(__GNUC__) || defined(__clang__)
  return __sync_bool_compare_and_swap(&p->weak, c, v);
#elif defined(_MSC_VER)
  STATIC_ASSERT(sizeof(volatile long) == sizeof(volatile uint32_t));
  return c == (uint32_t)_InterlockedCompareExchange((volatile long *)&p->weak, v, c);
#elif defined(__APPLE__)
  return OSAtomicCompareAndSwap32Barrier(c, v, &p->weak);
#else
#error FIXME: Unsupported compiler
#endif
}

MDBX_MAYBE_UNUSED static __always_inline uint32_t atomic_add32(mdbx_atomic_uint32_t *p, uint32_t v) {
#ifdef MDBX_HAVE_C11ATOMICS
  STATIC_ASSERT(sizeof(int) >= sizeof(uint32_t));
  assert(atomic_is_lock_free(MDBX_c11a_rw(uint32_t, p)));
  return atomic_fetch_add(MDBX_c11a_rw(uint32_t, p), v);
#elif defined(__GNUC__) || defined(__clang__)
  return __sync_fetch_and_add(&p->weak, v);
#elif defined(_MSC_VER)
  STATIC_ASSERT(sizeof(volatile long) == sizeof(volatile uint32_t));
  return (uint32_t)_InterlockedExchangeAdd((volatile long *)&p->weak, v);
#elif defined(__APPLE__)
  return OSAtomicAdd32Barrier(v, &p->weak);
#else
#error FIXME: Unsupported compiler
#endif
}

#define atomic_sub32(p, v) atomic_add32(p, 0 - (v))

MDBX_MAYBE_UNUSED static __always_inline uint64_t safe64_txnid_next(uint64_t txnid) {
  txnid += xMDBX_TXNID_STEP;
#if !MDBX_64BIT_CAS
  /* avoid overflow of low-part in safe64_reset() */
  txnid += (UINT32_MAX == (uint32_t)txnid);
#endif
  return txnid;
}

/* Atomically make target value >= SAFE64_INVALID_THRESHOLD */
MDBX_MAYBE_UNUSED static __always_inline void safe64_reset(mdbx_atomic_uint64_t *p, bool single_writer) {
  if (single_writer) {
#if MDBX_64BIT_ATOMIC && MDBX_WORDBITS >= 64
    atomic_store64(p, UINT64_MAX, mo_AcquireRelease);
#else
    atomic_store32(&p->high, UINT32_MAX, mo_AcquireRelease);
#endif /* MDBX_64BIT_ATOMIC && MDBX_WORDBITS >= 64 */
  } else {
#if MDBX_64BIT_CAS && MDBX_64BIT_ATOMIC
    /* atomically make value >= SAFE64_INVALID_THRESHOLD by 64-bit operation */
    atomic_store64(p, UINT64_MAX, mo_AcquireRelease);
#elif MDBX_64BIT_CAS
    /* atomically make value >= SAFE64_INVALID_THRESHOLD by 32-bit operation */
    atomic_store32(&p->high, UINT32_MAX, mo_AcquireRelease);
#else
    /* it is safe to increment low-part to avoid ABA, since xMDBX_TXNID_STEP > 1
     * and overflow was preserved in safe64_txnid_next() */
    STATIC_ASSERT(xMDBX_TXNID_STEP > 1);
    atomic_add32(&p->low, 1) /* avoid ABA in safe64_reset_compare() */;
    atomic_store32(&p->high, UINT32_MAX, mo_AcquireRelease);
    atomic_add32(&p->low, 1) /* avoid ABA in safe64_reset_compare() */;
#endif /* MDBX_64BIT_CAS && MDBX_64BIT_ATOMIC */
  }
  assert(p->weak >= SAFE64_INVALID_THRESHOLD);
  jitter4testing(true);
}

MDBX_MAYBE_UNUSED static __always_inline bool safe64_reset_compare(mdbx_atomic_uint64_t *p, uint64_t compare) {
  /* LY: This function is used to reset `txnid` from hsr-handler in case
   *     the asynchronously cancellation of read transaction. Therefore,
   *     there may be a collision between the cleanup performed here and
   *     asynchronous termination and restarting of the read transaction
   *     in another process/thread. In general we MUST NOT reset the `txnid`
   *     if a new transaction was started (i.e. if `txnid` was changed). */
#if MDBX_64BIT_CAS
  bool rc = atomic_cas64(p, compare, UINT64_MAX);
#else
  /* LY: There is no gold ratio here since shared mutex is too costly,
   *     in such way we must acquire/release it for every update of txnid,
   *     i.e. twice for each read transaction). */
  bool rc = false;
  if (likely(atomic_load32(&p->low, mo_AcquireRelease) == (uint32_t)compare &&
             atomic_cas32(&p->high, (uint32_t)(compare >> 32), UINT32_MAX))) {
    if (unlikely(atomic_load32(&p->low, mo_AcquireRelease) != (uint32_t)compare))
      atomic_cas32(&p->high, UINT32_MAX, (uint32_t)(compare >> 32));
    else
      rc = true;
  }
#endif /* MDBX_64BIT_CAS */
  jitter4testing(true);
  return rc;
}

MDBX_MAYBE_UNUSED static __always_inline void safe64_write(mdbx_atomic_uint64_t *p, const uint64_t v) {
  assert(p->weak >= SAFE64_INVALID_THRESHOLD);
#if MDBX_64BIT_ATOMIC && MDBX_64BIT_CAS
  atomic_store64(p, v, mo_AcquireRelease);
#else  /* MDBX_64BIT_ATOMIC */
  osal_compiler_barrier();
  /* update low-part but still value >= SAFE64_INVALID_THRESHOLD */
  atomic_store32(&p->low, (uint32_t)v, mo_Relaxed);
  assert(p->weak >= SAFE64_INVALID_THRESHOLD);
  jitter4testing(true);
  /* update high-part from SAFE64_INVALID_THRESHOLD to actual value */
  atomic_store32(&p->high, (uint32_t)(v >> 32), mo_AcquireRelease);
#endif /* MDBX_64BIT_ATOMIC */
  assert(p->weak == v);
  jitter4testing(true);
}

MDBX_MAYBE_UNUSED static __always_inline uint64_t safe64_read(const mdbx_atomic_uint64_t *p) {
  jitter4testing(true);
  uint64_t v;
  do
    v = atomic_load64(p, mo_AcquireRelease);
  while (!MDBX_64BIT_ATOMIC && unlikely(v != p->weak));
  return v;
}

#if 0 /* unused for now */
MDBX_MAYBE_UNUSED static __always_inline bool safe64_is_valid(uint64_t v) {
#if MDBX_WORDBITS >= 64
  return v < SAFE64_INVALID_THRESHOLD;
#else
  return (v >> 32) != UINT32_MAX;
#endif /* MDBX_WORDBITS */
}

MDBX_MAYBE_UNUSED static __always_inline bool
 safe64_is_valid_ptr(const mdbx_atomic_uint64_t *p) {
#if MDBX_64BIT_ATOMIC
  return atomic_load64(p, mo_AcquireRelease) < SAFE64_INVALID_THRESHOLD;
#else
  return atomic_load32(&p->high, mo_AcquireRelease) != UINT32_MAX;
#endif /* MDBX_64BIT_ATOMIC */
}
#endif /* unused for now */

/* non-atomic write with safety for reading a half-updated value */
MDBX_MAYBE_UNUSED static __always_inline void safe64_update(mdbx_atomic_uint64_t *p, const uint64_t v) {
#if MDBX_64BIT_ATOMIC
  atomic_store64(p, v, mo_Relaxed);
#else
  safe64_reset(p, true);
  safe64_write(p, v);
#endif /* MDBX_64BIT_ATOMIC */
}

/* non-atomic increment with safety for reading a half-updated value */
MDBX_MAYBE_UNUSED static
#if MDBX_64BIT_ATOMIC
    __always_inline
#endif /* MDBX_64BIT_ATOMIC */
    void
    safe64_inc(mdbx_atomic_uint64_t *p, const uint64_t v) {
  assert(v > 0);
  safe64_update(p, safe64_read(p) + v);
}

#endif /* !__cplusplus */
