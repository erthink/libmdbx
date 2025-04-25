/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025

#pragma once

/* Test if the flags f are set in a flag word w. */
#define F_ISSET(w, f) (((w) & (f)) == (f))

/* Round n up to an even number. */
#define EVEN_CEIL(n) (((n) + 1UL) & -2L) /* sign-extending -2 to match n+1U */

/* Round n down to an even number. */
#define EVEN_FLOOR(n) ((n) & ~(size_t)1)

/*
 *                /
 *                | -1, a < b
 * CMP2INT(a,b) = <  0, a == b
 *                |  1, a > b
 *                \
 */
#define CMP2INT(a, b) (((a) != (b)) ? (((a) < (b)) ? -1 : 1) : 0)

/* Pointer displacement without casting to char* to avoid pointer-aliasing */
#define ptr_disp(ptr, disp) ((void *)(((intptr_t)(ptr)) + ((intptr_t)(disp))))

/* Pointer distance as signed number of bytes */
#define ptr_dist(more, less) (((intptr_t)(more)) - ((intptr_t)(less)))

#define MDBX_ASAN_POISON_MEMORY_REGION(addr, size)                                                                     \
  do {                                                                                                                 \
    TRACE("POISON_MEMORY_REGION(%p, %zu) at %u", (void *)(addr), (size_t)(size), __LINE__);                            \
    ASAN_POISON_MEMORY_REGION(addr, size);                                                                             \
  } while (0)

#define MDBX_ASAN_UNPOISON_MEMORY_REGION(addr, size)                                                                   \
  do {                                                                                                                 \
    TRACE("UNPOISON_MEMORY_REGION(%p, %zu) at %u", (void *)(addr), (size_t)(size), __LINE__);                          \
    ASAN_UNPOISON_MEMORY_REGION(addr, size);                                                                           \
  } while (0)

MDBX_NOTHROW_CONST_FUNCTION MDBX_MAYBE_UNUSED static inline size_t branchless_abs(intptr_t value) {
  assert(value > INT_MIN);
  const size_t expanded_sign = (size_t)(value >> (sizeof(value) * CHAR_BIT - 1));
  return ((size_t)value + expanded_sign) ^ expanded_sign;
}

MDBX_NOTHROW_CONST_FUNCTION MDBX_MAYBE_UNUSED static inline bool is_powerof2(size_t x) { return (x & (x - 1)) == 0; }

MDBX_NOTHROW_CONST_FUNCTION MDBX_MAYBE_UNUSED static inline size_t floor_powerof2(size_t value, size_t granularity) {
  assert(is_powerof2(granularity));
  return value & ~(granularity - 1);
}

MDBX_NOTHROW_CONST_FUNCTION MDBX_MAYBE_UNUSED static inline size_t ceil_powerof2(size_t value, size_t granularity) {
  return floor_powerof2(value + granularity - 1, granularity);
}

MDBX_NOTHROW_CONST_FUNCTION MDBX_MAYBE_UNUSED MDBX_INTERNAL unsigned log2n_powerof2(size_t value_uintptr);

MDBX_NOTHROW_CONST_FUNCTION MDBX_MAYBE_UNUSED MDBX_INTERNAL unsigned ceil_log2n(size_t value_uintptr);

MDBX_NOTHROW_CONST_FUNCTION MDBX_INTERNAL uint64_t rrxmrrxmsx_0(uint64_t v);

struct monotime_cache {
  uint64_t value;
  int expire_countdown;
};

MDBX_MAYBE_UNUSED static inline uint64_t monotime_since_cached(uint64_t begin_timestamp, struct monotime_cache *cache) {
  if (cache->expire_countdown)
    cache->expire_countdown -= 1;
  else {
    cache->value = osal_monotime();
    cache->expire_countdown = 42 / 3;
  }
  return cache->value - begin_timestamp;
}
