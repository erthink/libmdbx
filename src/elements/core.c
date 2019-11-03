/*
 * Copyright 2015-2019 Leonid Yuriev <leo@yuriev.ru>
 * and other libmdbx authors: please see AUTHORS file.
 * All rights reserved.
 *
 * This code is derived from "LMDB engine" written by
 * Howard Chu (Symas Corporation), which itself derived from btree.c
 * written by Martin Hedenfalk.
 *
 * ---
 *
 * Portions Copyright 2011-2015 Howard Chu, Symas Corp. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 *
 * ---
 *
 * Portions Copyright (c) 2009, 2010 Martin Hedenfalk <martin@bzero.se>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE. */

#include "internals.h"

/*------------------------------------------------------------------------------
 * Internal inlines */

static __pure_function __inline bool is_powerof2(size_t x) {
  return (x & (x - 1)) == 0;
}

static __pure_function __inline size_t roundup_powerof2(size_t value,
                                                        size_t granularity) {
  assert(is_powerof2(granularity));
  return (value + granularity - 1) & ~(granularity - 1);
}

static __pure_function unsigned log2n(size_t value) {
  assert(value > 0 && value < INT32_MAX && is_powerof2(value));
  assert((value & -(int32_t)value) == value);
#if __GNUC_PREREQ(4, 1) || __has_builtin(__builtin_ctzl)
  return __builtin_ctzl(value);
#elif defined(_MSC_VER)
  unsigned long index;
  _BitScanForward(&index, (unsigned long)value);
  return index;
#else
  static const uint8_t debruijn_ctz32[32] = {
      0,  1,  28, 2,  29, 14, 24, 3, 30, 22, 20, 15, 25, 17, 4,  8,
      31, 27, 13, 23, 21, 19, 16, 7, 26, 12, 18, 6,  11, 5,  10, 9};
  return debruijn_ctz32[(uint32_t)(value * 0x077CB531u) >> 27];
#endif
}

/*------------------------------------------------------------------------------
 * Unaligned access */

static __pure_function __maybe_unused __inline unsigned
field_alignment(unsigned alignment_baseline, size_t field_offset) {
  unsigned merge = alignment_baseline | (unsigned)field_offset;
  return merge & -(int)merge;
}

/* read-thunk for UB-sanitizer */
static __pure_function __inline uint8_t peek_u8(const uint8_t *ptr) {
  return *ptr;
}

/* write-thunk for UB-sanitizer */
static __inline void poke_u8(uint8_t *ptr, const uint8_t v) { *ptr = v; }

static __pure_function __inline uint16_t
unaligned_peek_u16(const unsigned expected_alignment, const void *ptr) {
  assert((uintptr_t)ptr % expected_alignment == 0);
  if (MDBX_UNALIGNED_OK || (expected_alignment % sizeof(uint16_t)) == 0)
    return *(const uint16_t *)ptr;
  else {
    uint16_t v;
    memcpy(&v, ptr, sizeof(v));
    return v;
  }
}

static __inline void unaligned_poke_u16(const unsigned expected_alignment,
                                        void *ptr, const uint16_t v) {
  assert((uintptr_t)ptr % expected_alignment == 0);
  if (MDBX_UNALIGNED_OK || (expected_alignment % sizeof(v)) == 0)
    *(uint16_t *)ptr = v;
  else
    memcpy(ptr, &v, sizeof(v));
}

static __pure_function __inline uint32_t
unaligned_peek_u32(const unsigned expected_alignment, const void *ptr) {
  assert((uintptr_t)ptr % expected_alignment == 0);
  if (MDBX_UNALIGNED_OK || (expected_alignment % sizeof(uint32_t)) == 0)
    return *(const uint32_t *)ptr;
  else if ((expected_alignment % sizeof(uint16_t)) == 0) {
    const uint16_t lo =
        ((const uint16_t *)ptr)[__BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__];
    const uint16_t hi =
        ((const uint16_t *)ptr)[__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__];
    return lo | (uint32_t)hi << 16;
  } else {
    uint32_t v;
    memcpy(&v, ptr, sizeof(v));
    return v;
  }
}

static __inline void unaligned_poke_u32(const unsigned expected_alignment,
                                        void *ptr, const uint32_t v) {
  assert((uintptr_t)ptr % expected_alignment == 0);
  if (MDBX_UNALIGNED_OK || (expected_alignment % sizeof(v)) == 0)
    *(uint32_t *)ptr = v;
  else if ((expected_alignment % sizeof(uint16_t)) == 0) {
    ((uint16_t *)ptr)[__BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__] = (uint16_t)v;
    ((uint16_t *)ptr)[__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__] =
        (uint16_t)(v >> 16);
  } else
    memcpy(ptr, &v, sizeof(v));
}

static __pure_function __inline uint64_t
unaligned_peek_u64(const unsigned expected_alignment, const void *ptr) {
  assert((uintptr_t)ptr % expected_alignment == 0);
  if (MDBX_UNALIGNED_OK || (expected_alignment % sizeof(uint64_t)) == 0)
    return *(const uint64_t *)ptr;
  else if ((expected_alignment % sizeof(uint32_t)) == 0) {
    const uint32_t lo =
        ((const uint32_t *)ptr)[__BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__];
    const uint32_t hi =
        ((const uint32_t *)ptr)[__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__];
    return lo | (uint64_t)hi << 32;
  } else {
    uint64_t v;
    memcpy(&v, ptr, sizeof(v));
    return v;
  }
}

static __inline void unaligned_poke_u64(const unsigned expected_alignment,
                                        void *ptr, const uint64_t v) {
  assert((uintptr_t)ptr % expected_alignment == 0);
  if (MDBX_UNALIGNED_OK || (expected_alignment % sizeof(v)) == 0)
    *(uint64_t *)ptr = v;
  else if ((expected_alignment % sizeof(uint32_t)) == 0) {
    ((uint32_t *)ptr)[__BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__] = (uint32_t)v;
    ((uint32_t *)ptr)[__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__] =
        (uint32_t)(v >> 32);
  } else
    memcpy(ptr, &v, sizeof(v));
}

#define UNALIGNED_PEEK_8(ptr, struct, field)                                   \
  peek_u8((const uint8_t *)(ptr) + offsetof(struct, field))
#define UNALIGNED_POKE_8(ptr, struct, field, value)                            \
  poke_u8((uint8_t *)(ptr) + offsetof(struct, field), value)

#define UNALIGNED_PEEK_16(ptr, struct, field)                                  \
  unaligned_peek_u16(1, (const char *)(ptr) + offsetof(struct, field))
#define UNALIGNED_POKE_16(ptr, struct, field, value)                           \
  unaligned_poke_u16(1, (char *)(ptr) + offsetof(struct, field), value)

#define UNALIGNED_PEEK_32(ptr, struct, field)                                  \
  unaligned_peek_u32(1, (const char *)(ptr) + offsetof(struct, field))
#define UNALIGNED_POKE_32(ptr, struct, field, value)                           \
  unaligned_poke_u32(1, (char *)(ptr) + offsetof(struct, field), value)

#define UNALIGNED_PEEK_64(ptr, struct, field)                                  \
  unaligned_peek_u64(1, (const char *)(ptr) + offsetof(struct, field))
#define UNALIGNED_POKE_64(ptr, struct, field, value)                           \
  unaligned_poke_u64(1, (char *)(ptr) + offsetof(struct, field), value)

/* Get the page number pointed to by a branch node */
static __pure_function __inline pgno_t node_pgno(const MDBX_node *node) {
  pgno_t pgno = UNALIGNED_PEEK_32(node, MDBX_node, mn_pgno32);
  if (sizeof(pgno) > 4)
    pgno |= ((uint64_t)UNALIGNED_PEEK_8(node, MDBX_node, mn_extra)) << 32;
  return pgno;
}

/* Set the page number in a branch node */
static __inline void node_set_pgno(MDBX_node *node, pgno_t pgno) {
  assert(pgno >= MIN_PAGENO && pgno <= MAX_PAGENO);

  UNALIGNED_POKE_32(node, MDBX_node, mn_pgno32, (uint32_t)pgno);
  if (sizeof(pgno) > 4)
    UNALIGNED_POKE_8(node, MDBX_node, mn_extra,
                     (uint8_t)((uint64_t)pgno >> 32));
}

/* Get the size of the data in a leaf node */
static __pure_function __inline size_t node_ds(const MDBX_node *node) {
  return UNALIGNED_PEEK_32(node, MDBX_node, mn_dsize);
}

/* Set the size of the data for a leaf node */
static __inline void node_set_ds(MDBX_node *node, size_t size) {
  assert(size < INT_MAX);
  UNALIGNED_POKE_32(node, MDBX_node, mn_dsize, (uint32_t)size);
}

/* The size of a key in a node */
static __pure_function __inline size_t node_ks(const MDBX_node *node) {
  return UNALIGNED_PEEK_16(node, MDBX_node, mn_ksize);
}

/* Set the size of the key for a leaf node */
static __inline void node_set_ks(MDBX_node *node, size_t size) {
  assert(size < INT16_MAX);
  UNALIGNED_POKE_16(node, MDBX_node, mn_ksize, (uint16_t)size);
}

static __pure_function __inline uint8_t node_flags(const MDBX_node *node) {
  return UNALIGNED_PEEK_8(node, MDBX_node, mn_flags);
}

static __inline void node_set_flags(MDBX_node *node, uint8_t flags) {
  UNALIGNED_POKE_8(node, MDBX_node, mn_flags, flags);
}

/* Size of the node header, excluding dynamic data at the end */
#define NODESIZE offsetof(MDBX_node, mn_data)

/* Address of the key for the node */
static __pure_function __inline void *node_key(const MDBX_node *node) {
  return (char *)node + NODESIZE;
}

/* Address of the data for a node */
static __pure_function __inline void *node_data(const MDBX_node *node) {
  return (char *)node_key(node) + node_ks(node);
}

/* Size of a node in a leaf page with a given key and data.
 * This is node header plus key plus data size. */
static __pure_function __inline size_t node_size(const MDBX_val *key,
                                                 const MDBX_val *value) {
  return NODESIZE +
         EVEN((key ? key->iov_len : 0) + (value ? value->iov_len : 0));
}

static __pure_function __inline pgno_t peek_pgno(const void *ptr) {
  if (sizeof(pgno_t) == sizeof(uint32_t))
    return (pgno_t)unaligned_peek_u32(1, ptr);
  else if (sizeof(pgno_t) == sizeof(uint64_t))
    return (pgno_t)unaligned_peek_u64(1, ptr);
  else {
    pgno_t pgno;
    memcpy(&pgno, ptr, sizeof(pgno));
    return pgno;
  }
}

static __inline void poke_pgno(void *ptr, const pgno_t pgno) {
  if (sizeof(pgno) == sizeof(uint32_t))
    unaligned_poke_u32(1, ptr, pgno);
  else if (sizeof(pgno) == sizeof(uint64_t))
    unaligned_poke_u64(1, ptr, pgno);
  else
    memcpy(ptr, &pgno, sizeof(pgno));
}

static __pure_function __inline pgno_t
node_largedata_pgno(const MDBX_node *node) {
  assert(node_flags(node) & F_BIGDATA);
  return peek_pgno(node_data(node));
}

/* Calculate the size of a leaf node.
 *
 * The size depends on the environment's page size; if a data item
 * is too large it will be put onto an overflow page and the node
 * size will only include the key and not the data. Sizes are always
 * rounded up to an even number of bytes, to guarantee 2-byte alignment
 * of the MDBX_node headers.
 *
 * [in] env   The environment handle.
 * [in] key   The key for the node.
 * [in] data  The data for the node.
 *
 * Returns The number of bytes needed to store the node. */
static __pure_function __inline size_t
leaf_size(const MDBX_env *env, const MDBX_val *key, const MDBX_val *data) {
  size_t node_bytes = node_size(key, data);
  if (node_bytes > env->me_nodemax) {
    /* put on overflow page */
    node_bytes = node_size(key, nullptr) + sizeof(pgno_t);
  }

  return node_bytes + sizeof(indx_t);
}

/* Calculate the size of a branch node.
 *
 * The size should depend on the environment's page size but since
 * we currently don't support spilling large keys onto overflow
 * pages, it's simply the size of the MDBX_node header plus the
 * size of the key. Sizes are always rounded up to an even number
 * of bytes, to guarantee 2-byte alignment of the MDBX_node headers.
 *
 * [in] env The environment handle.
 * [in] key The key for the node.
 *
 * Returns The number of bytes needed to store the node. */
static __pure_function __inline size_t branch_size(const MDBX_env *env,
                                                   const MDBX_val *key) {
  /* Size of a node in a branch page with a given key.
   * This is just the node header plus the key, there is no data. */
  size_t node_bytes = node_size(key, nullptr);
  if (unlikely(node_bytes > env->me_nodemax)) {
    /* put on overflow page */
    /* not implemented */
    mdbx_assert_fail(env, "INDXSIZE(key) <= env->me_nodemax", __func__,
                     __LINE__);
    node_bytes = node_size(key, nullptr) + sizeof(pgno_t);
  }

  return node_bytes + sizeof(indx_t);
}

/*----------------------------------------------------------------------------*/

static __pure_function __inline size_t pgno2bytes(const MDBX_env *env,
                                                  pgno_t pgno) {
  mdbx_assert(env, (1u << env->me_psize2log) == env->me_psize);
  return ((size_t)pgno) << env->me_psize2log;
}

static __pure_function __inline MDBX_page *pgno2page(const MDBX_env *env,
                                                     pgno_t pgno) {
  return (MDBX_page *)(env->me_map + pgno2bytes(env, pgno));
}

static __pure_function __inline pgno_t bytes2pgno(const MDBX_env *env,
                                                  size_t bytes) {
  mdbx_assert(env, (env->me_psize >> env->me_psize2log) == 1);
  return (pgno_t)(bytes >> env->me_psize2log);
}

static __pure_function __inline size_t pgno_align2os_bytes(const MDBX_env *env,
                                                           pgno_t pgno) {
  return roundup_powerof2(pgno2bytes(env, pgno), env->me_os_psize);
}

static __pure_function __inline pgno_t pgno_align2os_pgno(const MDBX_env *env,
                                                          pgno_t pgno) {
  return bytes2pgno(env, pgno_align2os_bytes(env, pgno));
}

static __pure_function __inline size_t bytes_align2os_bytes(const MDBX_env *env,
                                                            size_t bytes) {
  return roundup_powerof2(roundup_powerof2(bytes, env->me_psize),
                          env->me_os_psize);
}

/* Address of first usable data byte in a page, after the header */
static __pure_function __inline void *page_data(const MDBX_page *mp) {
  return (char *)mp + PAGEHDRSZ;
}

static __pure_function __inline const MDBX_page *data_page(const void *data) {
  return container_of(data, MDBX_page, mp_ptrs);
}

static __pure_function __inline MDBX_meta *page_meta(MDBX_page *mp) {
  return (MDBX_meta *)page_data(mp);
}

/* Number of nodes on a page */
static __pure_function __inline unsigned page_numkeys(const MDBX_page *mp) {
  return mp->mp_lower >> 1;
}

/* The amount of space remaining in the page */
static __pure_function __inline unsigned page_room(const MDBX_page *mp) {
  return mp->mp_upper - mp->mp_lower;
}

static __pure_function __inline unsigned page_space(const MDBX_env *env) {
  STATIC_ASSERT(PAGEHDRSZ % 2 == 0);
  return env->me_psize - PAGEHDRSZ;
}

static __pure_function __inline unsigned page_used(const MDBX_env *env,
                                                   const MDBX_page *mp) {
  return page_space(env) - page_room(mp);
}

/* The percentage of space used in the page, in a percents. */
static __pure_function __maybe_unused __inline double
page_fill(const MDBX_env *env, const MDBX_page *mp) {
  return page_used(env, mp) * 100.0 / page_space(env);
}

static __pure_function __inline bool
page_fill_enough(const MDBX_page *mp, unsigned spaceleft_threshold,
                 unsigned minkeys_threshold) {
  return page_room(mp) < spaceleft_threshold &&
         page_numkeys(mp) >= minkeys_threshold;
}

/* The number of overflow pages needed to store the given size. */
static __pure_function __inline pgno_t number_of_ovpages(const MDBX_env *env,
                                                         size_t bytes) {
  return bytes2pgno(env, PAGEHDRSZ - 1 + bytes) + 1;
}

/* Address of node i in page p */
static __pure_function __inline MDBX_node *page_node(const MDBX_page *mp,
                                                     unsigned i) {
  assert((mp->mp_flags & (P_LEAF2 | P_OVERFLOW | P_META)) == 0);
  assert(page_numkeys(mp) > (unsigned)(i));
  assert(mp->mp_ptrs[i] % 2 == 0);
  return (MDBX_node *)((char *)mp + mp->mp_ptrs[i] + PAGEHDRSZ);
}

/* The address of a key in a LEAF2 page.
 * LEAF2 pages are used for MDBX_DUPFIXED sorted-duplicate sub-DBs.
 * There are no node headers, keys are stored contiguously. */
static __pure_function __inline void *
page_leaf2key(const MDBX_page *mp, unsigned i, size_t keysize) {
  assert(mp->mp_leaf2_ksize == keysize);
  (void)keysize;
  return (char *)mp + PAGEHDRSZ + (i * mp->mp_leaf2_ksize);
}

/* Set the node's key into keyptr. */
static __inline void get_key(const MDBX_node *node, MDBX_val *keyptr) {
  keyptr->iov_len = node_ks(node);
  keyptr->iov_base = node_key(node);
}

/* Set the node's key into keyptr, if requested. */
static __inline void get_key_optional(const MDBX_node *node,
                                      MDBX_val *keyptr /* __may_null */) {
  if (keyptr)
    get_key(node, keyptr);
}

/*------------------------------------------------------------------------------
 * LY: temporary workaround for Elbrus's memcmp() bug. */

#if defined(__e2k__) && !__GLIBC_PREREQ(2, 24)
int __hot mdbx_e2k_memcmp_bug_workaround(const void *s1, const void *s2,
                                         size_t n) {
  if (unlikely(n > 42
               /* LY: align followed access if reasonable possible */
               && (((uintptr_t)s1) & 7) != 0 &&
               (((uintptr_t)s1) & 7) == (((uintptr_t)s2) & 7))) {
    if (((uintptr_t)s1) & 1) {
      const int diff = *(uint8_t *)s1 - *(uint8_t *)s2;
      if (diff)
        return diff;
      s1 = (char *)s1 + 1;
      s2 = (char *)s2 + 1;
      n -= 1;
    }

    if (((uintptr_t)s1) & 2) {
      const uint16_t a = *(uint16_t *)s1;
      const uint16_t b = *(uint16_t *)s2;
      if (likely(a != b))
        return (__builtin_bswap16(a) > __builtin_bswap16(b)) ? 1 : -1;
      s1 = (char *)s1 + 2;
      s2 = (char *)s2 + 2;
      n -= 2;
    }

    if (((uintptr_t)s1) & 4) {
      const uint32_t a = *(uint32_t *)s1;
      const uint32_t b = *(uint32_t *)s2;
      if (likely(a != b))
        return (__builtin_bswap32(a) > __builtin_bswap32(b)) ? 1 : -1;
      s1 = (char *)s1 + 4;
      s2 = (char *)s2 + 4;
      n -= 4;
    }
  }

  while (n >= 8) {
    const uint64_t a = *(uint64_t *)s1;
    const uint64_t b = *(uint64_t *)s2;
    if (likely(a != b))
      return (__builtin_bswap64(a) > __builtin_bswap64(b)) ? 1 : -1;
    s1 = (char *)s1 + 8;
    s2 = (char *)s2 + 8;
    n -= 8;
  }

  if (n & 4) {
    const uint32_t a = *(uint32_t *)s1;
    const uint32_t b = *(uint32_t *)s2;
    if (likely(a != b))
      return (__builtin_bswap32(a) > __builtin_bswap32(b)) ? 1 : -1;
    s1 = (char *)s1 + 4;
    s2 = (char *)s2 + 4;
  }

  if (n & 2) {
    const uint16_t a = *(uint16_t *)s1;
    const uint16_t b = *(uint16_t *)s2;
    if (likely(a != b))
      return (__builtin_bswap16(a) > __builtin_bswap16(b)) ? 1 : -1;
    s1 = (char *)s1 + 2;
    s2 = (char *)s2 + 2;
  }

  return (n & 1) ? *(uint8_t *)s1 - *(uint8_t *)s2 : 0;
}

int __hot mdbx_e2k_strcmp_bug_workaround(const char *s1, const char *s2) {
  while (true) {
    int diff = *(uint8_t *)s1 - *(uint8_t *)s2;
    if (likely(diff != 0) || *s1 == '\0')
      return diff;
    s1 += 1;
    s2 += 1;
  }
}

int __hot mdbx_e2k_strncmp_bug_workaround(const char *s1, const char *s2,
                                          size_t n) {
  while (n > 0) {
    int diff = *(uint8_t *)s1 - *(uint8_t *)s2;
    if (likely(diff != 0) || *s1 == '\0')
      return diff;
    s1 += 1;
    s2 += 1;
    n -= 1;
  }
  return 0;
}

size_t __hot mdbx_e2k_strlen_bug_workaround(const char *s) {
  size_t n = 0;
  while (*s) {
    s += 1;
    n += 1;
  }
  return n;
}

size_t __hot mdbx_e2k_strnlen_bug_workaround(const char *s, size_t maxlen) {
  size_t n = 0;
  while (maxlen > n && *s) {
    s += 1;
    n += 1;
  }
  return n;
}
#endif /* Elbrus's memcmp() bug. */

/*------------------------------------------------------------------------------
 * safe read/write volatile 64-bit fields on 32-bit architectures. */

static __inline void atomic_yield(void) {
#if defined(_WIN32) || defined(_WIN64)
  YieldProcessor();
#elif defined(__x86_64__) || defined(__i386__) || defined(__e2k__)
  __builtin_ia32_pause();
#elif defined(__ia64__)
#if defined(__HP_cc__) || defined(__HP_aCC__)
  _Asm_hint(_HINT_PAUSE);
#else
  __asm__ __volatile__("hint @pause");
#endif
#elif defined(__arm__) || defined(__aarch64__)
#ifdef __CC_ARM
  __yield();
#else
  __asm__ __volatile__("yield");
#endif
#elif (defined(__mips64) || defined(__mips64__)) && defined(__mips_isa_rev) && \
    __mips_isa_rev >= 2
  __asm__ __volatile__("pause");
#elif defined(__mips) || defined(__mips__) || defined(__mips64) ||             \
    defined(__mips64__) || defined(_M_MRX000) || defined(_MIPS_) ||            \
    defined(__MWERKS__) || defined(__sgi)
  __asm__ __volatile__(".word 0x00000140");
#else
  pthread_yield();
#endif
}

#if MDBX_64BIT_CAS
static __inline bool atomic_cas64(volatile uint64_t *p, uint64_t c,
                                  uint64_t v) {
#if defined(ATOMIC_VAR_INIT) || defined(ATOMIC_LLONG_LOCK_FREE)
  STATIC_ASSERT(sizeof(long long int) == 8);
  STATIC_ASSERT(atomic_is_lock_free(p));
  return atomic_compare_exchange_strong((_Atomic uint64_t *)p, &c, v);
#elif defined(__GNUC__) || defined(__clang__)
  return __sync_bool_compare_and_swap(p, c, v);
#elif defined(_MSC_VER)
  return c ==
         (uint64_t)_InterlockedCompareExchange64((volatile int64_t *)p, v, c);
#elif defined(__APPLE__)
  return OSAtomicCompareAndSwap64Barrier(c, v, (volatile uint64_t *)p);
#else
#error FIXME: Unsupported compiler
#endif
}
#endif /* MDBX_64BIT_CAS */

static __inline bool atomic_cas32(volatile uint32_t *p, uint32_t c,
                                  uint32_t v) {
#if defined(ATOMIC_VAR_INIT) || defined(ATOMIC_LONG_LOCK_FREE)
  STATIC_ASSERT(sizeof(long int) >= 4);
  STATIC_ASSERT(atomic_is_lock_free(p));
  return atomic_compare_exchange_strong((_Atomic uint32_t *)p, &c, v);
#elif defined(__GNUC__) || defined(__clang__)
  return __sync_bool_compare_and_swap(p, c, v);
#elif defined(_MSC_VER)
  STATIC_ASSERT(sizeof(volatile long) == sizeof(volatile uint32_t));
  return c == (uint32_t)_InterlockedCompareExchange((volatile long *)p, v, c);
#elif defined(__APPLE__)
  return OSAtomicCompareAndSwap32Barrier(c, v, (volatile int32_t *)p);
#else
#error FIXME: Unsupported compiler
#endif
}

static __inline uint32_t atomic_add32(volatile uint32_t *p, uint32_t v) {
#if defined(ATOMIC_VAR_INIT) || defined(ATOMIC_LONG_LOCK_FREE)
  STATIC_ASSERT(sizeof(long int) >= 4);
  STATIC_ASSERT(atomic_is_lock_free(p));
  return atomic_fetch_add((_Atomic uint32_t *)p, v);
#elif defined(__GNUC__) || defined(__clang__)
  return __sync_fetch_and_add(p, v);
#elif defined(_MSC_VER)
  STATIC_ASSERT(sizeof(volatile long) == sizeof(volatile uint32_t));
  return _InterlockedExchangeAdd((volatile long *)p, v);
#elif defined(__APPLE__)
  return OSAtomicAdd32Barrier(v, (volatile int32_t *)p);
#else
#error FIXME: Unsupported compiler
#endif
}

#define atomic_sub32(p, v) atomic_add32(p, 0 - (v))

static __maybe_unused __inline bool safe64_is_valid(uint64_t v) {
#if MDBX_WORDBITS >= 64
  return v < SAFE64_INVALID_THRESHOLD;
#else
  return (v >> 32) != UINT32_MAX;
#endif /* MDBX_WORDBITS */
}

static __maybe_unused __inline bool
safe64_is_valid_ptr(const mdbx_safe64_t *ptr) {
  mdbx_compiler_barrier();
#if MDBX_64BIT_ATOMIC
  return ptr->atomic < SAFE64_INVALID_THRESHOLD;
#else
  return ptr->high != UINT32_MAX;
#endif /* MDBX_64BIT_ATOMIC */
}

static __inline uint64_t safe64_txnid_next(uint64_t txnid) {
  txnid += MDBX_TXNID_STEP;
#if !MDBX_64BIT_CAS
  /* avoid overflow of low-part in safe64_reset() */
  txnid += (UINT32_MAX == (uint32_t)txnid);
#endif
  return txnid;
}

static __inline void safe64_reset(mdbx_safe64_t *ptr, bool single_writer) {
  mdbx_compiler_barrier();
#if !MDBX_64BIT_CAS
  if (!single_writer) {
    STATIC_ASSERT(MDBX_TXNID_STEP > 1);
    /* it is safe to increment low-part to avoid ABA, since MDBX_TXNID_STEP > 1
     * and overflow was preserved in safe64_txnid_next() */
    atomic_add32(&ptr->low, 1) /* avoid ABA in safe64_reset_compare() */;
    ptr->high = UINT32_MAX /* atomically make >= SAFE64_INVALID_THRESHOLD */;
    atomic_add32(&ptr->low, 1) /* avoid ABA in safe64_reset_compare() */;
  } else
#else
  (void)single_writer;
#endif /* !MDBX_64BIT_CAS */
#if MDBX_64BIT_ATOMIC
    ptr->atomic = UINT64_MAX;
#else
  /* atomically make value >= SAFE64_INVALID_THRESHOLD */
  ptr->high = UINT32_MAX;
#endif /* MDBX_64BIT_ATOMIC */
  assert(ptr->inconsistent >= SAFE64_INVALID_THRESHOLD);
  mdbx_flush_noncoherent_cpu_writeback();
  mdbx_jitter4testing(true);
}

static __inline bool safe64_reset_compare(mdbx_safe64_t *ptr, txnid_t compare) {
  mdbx_compiler_barrier();
  /* LY: This function is used to reset `mr_txnid` from OOM-kick in case
   *     the asynchronously cancellation of read transaction. Therefore,
   *     there may be a collision between the cleanup performed here and
   *     asynchronous termination and restarting of the read transaction
   *     in another proces/thread. In general we MUST NOT reset the `mr_txnid`
   *     if a new transaction was started (i.e. if `mr_txnid` was changed). */
#if MDBX_64BIT_CAS
  bool rc = atomic_cas64(&ptr->inconsistent, compare, UINT64_MAX);
  mdbx_flush_noncoherent_cpu_writeback();
#else
  /* LY: There is no gold ratio here since shared mutex is too costly,
   *     in such way we must acquire/release it for every update of mr_txnid,
   *     i.e. twice for each read transaction). */
  bool rc = false;
  if (likely(ptr->low == (uint32_t)compare &&
             atomic_cas32(&ptr->high, (uint32_t)(compare >> 32), UINT32_MAX))) {
    if (unlikely(ptr->low != (uint32_t)compare))
      atomic_cas32(&ptr->high, UINT32_MAX, (uint32_t)(compare >> 32));
    else
      rc = true;
  }
#endif /* MDBX_64BIT_CAS */
  mdbx_jitter4testing(true);
  return rc;
}

static __inline void safe64_write(mdbx_safe64_t *ptr, const uint64_t v) {
  mdbx_compiler_barrier();
  assert(ptr->inconsistent >= SAFE64_INVALID_THRESHOLD);
#if MDBX_64BIT_ATOMIC
  ptr->atomic = v;
#else  /* MDBX_64BIT_ATOMIC */
  /* update low-part but still value >= SAFE64_INVALID_THRESHOLD */
  ptr->low = (uint32_t)v;
  assert(ptr->inconsistent >= SAFE64_INVALID_THRESHOLD);
  mdbx_flush_noncoherent_cpu_writeback();
  mdbx_jitter4testing(true);
  /* update high-part from SAFE64_INVALID_THRESHOLD to actual value */
  ptr->high = (uint32_t)(v >> 32);
#endif /* MDBX_64BIT_ATOMIC */
  assert(ptr->inconsistent == v);
  mdbx_flush_noncoherent_cpu_writeback();
  mdbx_jitter4testing(true);
}

static __always_inline uint64_t safe64_read(const mdbx_safe64_t *ptr) {
  mdbx_compiler_barrier();
  mdbx_jitter4testing(true);
  uint64_t v;
#if MDBX_64BIT_ATOMIC
  v = ptr->atomic;
#else  /* MDBX_64BIT_ATOMIC */
  uint32_t hi, lo;
  do {
    hi = ptr->high;
    mdbx_compiler_barrier();
    mdbx_jitter4testing(true);
    lo = ptr->low;
    mdbx_compiler_barrier();
    mdbx_jitter4testing(true);
  } while (unlikely(hi != ptr->high));
  v = lo | (uint64_t)hi << 32;
#endif /* MDBX_64BIT_ATOMIC */
  mdbx_jitter4testing(true);
  return v;
}

static __inline void safe64_update(mdbx_safe64_t *ptr, const uint64_t v) {
  safe64_reset(ptr, true);
  safe64_write(ptr, v);
}

/*----------------------------------------------------------------------------*/
/* rthc (tls keys and destructors) */

typedef struct rthc_entry_t {
  MDBX_reader *begin;
  MDBX_reader *end;
  mdbx_thread_key_t thr_tls_key;
  bool key_valid;
} rthc_entry_t;

#if MDBX_DEBUG
#define RTHC_INITIAL_LIMIT 1
#else
#define RTHC_INITIAL_LIMIT 16
#endif

static bin128_t bootid;
#if defined(_WIN32) || defined(_WIN64)
static CRITICAL_SECTION rthc_critical_section;
static CRITICAL_SECTION lcklist_critical_section;
#else
int __cxa_thread_atexit_impl(void (*dtor)(void *), void *obj, void *dso_symbol)
    __attribute__((__weak__));
#ifdef __APPLE__ /* FIXME: Thread-Local Storage destructors & DSO-unloading */
int __cxa_thread_atexit_impl(void (*dtor)(void *), void *obj,
                             void *dso_symbol) {
  (void)dtor;
  (void)obj;
  (void)dso_symbol;
  return -1;
}
#endif           /* __APPLE__ */

static pthread_mutex_t lcklist_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t rthc_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t rthc_cond = PTHREAD_COND_INITIALIZER;
static mdbx_thread_key_t rthc_key;
static volatile uint32_t rthc_pending;

static void __cold workaround_glibc_bug21031(void) {
  /* Workaround for https://sourceware.org/bugzilla/show_bug.cgi?id=21031
   *
   * Due race between pthread_key_delete() and __nptl_deallocate_tsd()
   * The destructor(s) of thread-local-storate object(s) may be running
   * in another thread(s) and be blocked or not finished yet.
   * In such case we get a SEGFAULT after unload this library DSO.
   *
   * So just by yielding a few timeslices we give a chance
   * to such destructor(s) for completion and avoids segfault. */
  sched_yield();
  sched_yield();
  sched_yield();
}
#endif

static unsigned rthc_count, rthc_limit;
static rthc_entry_t *rthc_table;
static rthc_entry_t rthc_table_static[RTHC_INITIAL_LIMIT];

static __inline void rthc_lock(void) {
#if defined(_WIN32) || defined(_WIN64)
  EnterCriticalSection(&rthc_critical_section);
#else
  mdbx_ensure(nullptr, pthread_mutex_lock(&rthc_mutex) == 0);
#endif
}

static __inline void rthc_unlock(void) {
#if defined(_WIN32) || defined(_WIN64)
  LeaveCriticalSection(&rthc_critical_section);
#else
  mdbx_ensure(nullptr, pthread_mutex_unlock(&rthc_mutex) == 0);
#endif
}

static __inline int thread_key_create(mdbx_thread_key_t *key) {
  int rc;
#if defined(_WIN32) || defined(_WIN64)
  *key = TlsAlloc();
  rc = (*key != TLS_OUT_OF_INDEXES) ? MDBX_SUCCESS : GetLastError();
#else
  rc = pthread_key_create(key, nullptr);
#endif
  mdbx_trace("&key = %p, value 0x%x, rc %d", __Wpedantic_format_voidptr(key),
             (unsigned)*key, rc);
  return rc;
}

static __inline void thread_key_delete(mdbx_thread_key_t key) {
  mdbx_trace("key = 0x%x", (unsigned)key);
#if defined(_WIN32) || defined(_WIN64)
  mdbx_ensure(nullptr, TlsFree(key));
#else
  mdbx_ensure(nullptr, pthread_key_delete(key) == 0);
  workaround_glibc_bug21031();
#endif
}

static __inline void *thread_rthc_get(mdbx_thread_key_t key) {
#if defined(_WIN32) || defined(_WIN64)
  return TlsGetValue(key);
#else
  return pthread_getspecific(key);
#endif
}

static void thread_rthc_set(mdbx_thread_key_t key, const void *value) {
#if defined(_WIN32) || defined(_WIN64)
  mdbx_ensure(nullptr, TlsSetValue(key, (void *)value));
#else
#define MDBX_THREAD_RTHC_ZERO 0
#define MDBX_THREAD_RTHC_REGISTERD 1
#define MDBX_THREAD_RTHC_COUNTED 2
  static __thread uint32_t thread_registration_state;
  if (value && unlikely(thread_registration_state == MDBX_THREAD_RTHC_ZERO)) {
    thread_registration_state = MDBX_THREAD_RTHC_REGISTERD;
    mdbx_trace("thread registered 0x%" PRIxPTR, (uintptr_t)mdbx_thread_self());
    if (&__cxa_thread_atexit_impl == nullptr ||
        __cxa_thread_atexit_impl(mdbx_rthc_thread_dtor,
                                 &thread_registration_state,
                                 (void *)&mdbx_version /* dso_anchor */)) {
      mdbx_ensure(nullptr, pthread_setspecific(
                               rthc_key, &thread_registration_state) == 0);
      thread_registration_state = MDBX_THREAD_RTHC_COUNTED;
      const unsigned count_before = atomic_add32(&rthc_pending, 1);
      mdbx_ensure(nullptr, count_before < INT_MAX);
      mdbx_trace("fallback to pthreads' tsd, key 0x%x, count %u",
                 (unsigned)rthc_key, count_before);
      (void)count_before;
    }
  }
  mdbx_ensure(nullptr, pthread_setspecific(key, value) == 0);
#endif
}

__cold void mdbx_rthc_global_init(void) {
  rthc_limit = RTHC_INITIAL_LIMIT;
  rthc_table = rthc_table_static;
#if defined(_WIN32) || defined(_WIN64)
  InitializeCriticalSection(&rthc_critical_section);
  InitializeCriticalSection(&lcklist_critical_section);
#else
  mdbx_ensure(nullptr,
              pthread_key_create(&rthc_key, mdbx_rthc_thread_dtor) == 0);
  mdbx_trace("pid %d, &mdbx_rthc_key = %p, value 0x%x", mdbx_getpid(),
             __Wpedantic_format_voidptr(&rthc_key), (unsigned)rthc_key);
#endif
  /* checking time conversion, this also avoids racing on 32-bit architectures
   * during writing calculated 64-bit ratio(s) into memory. */
  uint32_t proba = UINT32_MAX;
  while (true) {
    unsigned time_conversion_checkup =
        mdbx_osal_monotime_to_16dot16(mdbx_osal_16dot16_to_monotime(proba));
    unsigned one_more = (proba < UINT32_MAX) ? proba + 1 : proba;
    unsigned one_less = (proba > 0) ? proba - 1 : proba;
    mdbx_ensure(nullptr, time_conversion_checkup >= one_less &&
                             time_conversion_checkup <= one_more);
    if (proba == 0)
      break;
    proba >>= 1;
  }

  bootid = mdbx_osal_bootid();
}

/* dtor called for thread, i.e. for all mdbx's environment objects */
__cold void mdbx_rthc_thread_dtor(void *ptr) {
  rthc_lock();
  mdbx_trace(">> pid %d, thread 0x%" PRIxPTR ", rthc %p", mdbx_getpid(),
             (uintptr_t)mdbx_thread_self(), ptr);

  const uint32_t self_pid = mdbx_getpid();
  for (unsigned i = 0; i < rthc_count; ++i) {
    if (!rthc_table[i].key_valid)
      continue;
    const mdbx_thread_key_t key = rthc_table[i].thr_tls_key;
    MDBX_reader *const rthc = thread_rthc_get(key);
    if (rthc < rthc_table[i].begin || rthc >= rthc_table[i].end)
      continue;
#if !defined(_WIN32) && !defined(_WIN64)
    if (pthread_setspecific(key, nullptr) != 0) {
      mdbx_trace("== thread 0x%" PRIxPTR
                 ", rthc %p: ignore race with tsd-key deletion",
                 (uintptr_t)mdbx_thread_self(), ptr);
      continue /* ignore race with tsd-key deletion by mdbx_env_close() */;
    }
#endif

    mdbx_trace("== thread 0x%" PRIxPTR
               ", rthc %p, [%i], %p ... %p (%+i), rtch-pid %i, "
               "current-pid %i",
               (uintptr_t)mdbx_thread_self(), __Wpedantic_format_voidptr(rthc),
               i, __Wpedantic_format_voidptr(rthc_table[i].begin),
               __Wpedantic_format_voidptr(rthc_table[i].end),
               (int)(rthc - rthc_table[i].begin), rthc->mr_pid, self_pid);
    if (rthc->mr_pid == self_pid) {
      mdbx_trace("==== thread 0x%" PRIxPTR ", rthc %p, cleanup",
                 (uintptr_t)mdbx_thread_self(),
                 __Wpedantic_format_voidptr(rthc));
      rthc->mr_pid = 0;
    }
  }

#if defined(_WIN32) || defined(_WIN64)
  mdbx_trace("<< thread 0x%" PRIxPTR ", rthc %p", (uintptr_t)mdbx_thread_self(),
             ptr);
  rthc_unlock();
#else
  const char self_registration = *(char *)ptr;
  *(char *)ptr = MDBX_THREAD_RTHC_ZERO;
  mdbx_trace("== thread 0x%" PRIxPTR ", rthc %p, pid %d, self-status %d",
             (uintptr_t)mdbx_thread_self(), ptr, mdbx_getpid(),
             self_registration);
  if (self_registration == MDBX_THREAD_RTHC_COUNTED)
    mdbx_ensure(nullptr, atomic_sub32(&rthc_pending, 1) > 0);

  if (rthc_pending == 0) {
    mdbx_trace("== thread 0x%" PRIxPTR ", rthc %p, pid %d, wake",
               (uintptr_t)mdbx_thread_self(), ptr, mdbx_getpid());
    mdbx_ensure(nullptr, pthread_cond_broadcast(&rthc_cond) == 0);
  }

  mdbx_trace("<< thread 0x%" PRIxPTR ", rthc %p", (uintptr_t)mdbx_thread_self(),
             ptr);
  /* Allow tail call optimization, i.e. gcc should generate the jmp instruction
   * instead of a call for pthread_mutex_unlock() and therefore CPU could not
   * return to current DSO's code section, which may be unloaded immediately
   * after the mutex got released. */
  pthread_mutex_unlock(&rthc_mutex);
#endif
}

__cold void mdbx_rthc_global_dtor(void) {
  mdbx_trace(">> pid %d", mdbx_getpid());

  rthc_lock();
#if !defined(_WIN32) && !defined(_WIN64)
  char *rthc = (char *)pthread_getspecific(rthc_key);
  mdbx_trace("== thread 0x%" PRIxPTR ", rthc %p, pid %d, self-status %d",
             (uintptr_t)mdbx_thread_self(), __Wpedantic_format_voidptr(rthc),
             mdbx_getpid(), rthc ? *rthc : -1);
  if (rthc) {
    const char self_registration = *(char *)rthc;
    *rthc = MDBX_THREAD_RTHC_ZERO;
    if (self_registration == MDBX_THREAD_RTHC_COUNTED)
      mdbx_ensure(nullptr, atomic_sub32(&rthc_pending, 1) > 0);
  }

  struct timespec abstime;
  mdbx_ensure(nullptr, clock_gettime(CLOCK_REALTIME, &abstime) == 0);
  abstime.tv_nsec += 1000000000l / 10;
  if (abstime.tv_nsec >= 1000000000l) {
    abstime.tv_nsec -= 1000000000l;
    abstime.tv_sec += 1;
  }
#if MDBX_DEBUG > 0
  abstime.tv_sec += 600;
#endif

  for (unsigned left; (left = rthc_pending) > 0;) {
    mdbx_trace("pid %d, pending %u, wait for...", mdbx_getpid(), left);
    const int rc = pthread_cond_timedwait(&rthc_cond, &rthc_mutex, &abstime);
    if (rc && rc != EINTR)
      break;
  }
  thread_key_delete(rthc_key);
#endif

  const uint32_t self_pid = mdbx_getpid();
  for (unsigned i = 0; i < rthc_count; ++i) {
    if (!rthc_table[i].key_valid)
      continue;
    const mdbx_thread_key_t key = rthc_table[i].thr_tls_key;
    thread_key_delete(key);
    for (MDBX_reader *rthc = rthc_table[i].begin; rthc < rthc_table[i].end;
         ++rthc) {
      mdbx_trace("== [%i] = key %zu, %p ... %p, rthc %p (%+i), "
                 "rthc-pid %i, current-pid %i",
                 i, (size_t)key,
                 __Wpedantic_format_voidptr(rthc_table[i].begin),
                 __Wpedantic_format_voidptr(rthc_table[i].end),
                 __Wpedantic_format_voidptr(rthc),
                 (int)(rthc - rthc_table[i].begin), rthc->mr_pid, self_pid);
      if (rthc->mr_pid == self_pid) {
        rthc->mr_pid = 0;
        mdbx_trace("== cleanup %p", __Wpedantic_format_voidptr(rthc));
      }
    }
  }

  rthc_limit = rthc_count = 0;
  if (rthc_table != rthc_table_static)
    mdbx_free(rthc_table);
  rthc_table = nullptr;
  rthc_unlock();

#if defined(_WIN32) || defined(_WIN64)
  DeleteCriticalSection(&lcklist_critical_section);
  DeleteCriticalSection(&rthc_critical_section);
#else
  /* LY: yielding a few timeslices to give a more chance
   * to racing destructor(s) for completion. */
  workaround_glibc_bug21031();
#endif

  mdbx_trace("<< pid %d\n", mdbx_getpid());
}

__cold int mdbx_rthc_alloc(mdbx_thread_key_t *key, MDBX_reader *begin,
                           MDBX_reader *end) {
  int rc;
  if (key) {
#ifndef NDEBUG
    *key = (mdbx_thread_key_t)0xBADBADBAD;
#endif /* NDEBUG */
    rc = thread_key_create(key);
    if (rc != MDBX_SUCCESS)
      return rc;
  }

  rthc_lock();
  const mdbx_thread_key_t new_key = key ? *key : 0;
  mdbx_trace(">> key %zu, rthc_count %u, rthc_limit %u", (size_t)new_key,
             rthc_count, rthc_limit);
  if (rthc_count == rthc_limit) {
    rthc_entry_t *new_table =
        mdbx_realloc((rthc_table == rthc_table_static) ? nullptr : rthc_table,
                     sizeof(rthc_entry_t) * rthc_limit * 2);
    if (new_table == nullptr) {
      rc = MDBX_ENOMEM;
      goto bailout;
    }
    if (rthc_table == rthc_table_static)
      memcpy(new_table, rthc_table_static, sizeof(rthc_table_static));
    rthc_table = new_table;
    rthc_limit *= 2;
  }
  mdbx_trace("== [%i] = key %zu, %p ... %p", rthc_count, (size_t)new_key,
             __Wpedantic_format_voidptr(begin),
             __Wpedantic_format_voidptr(end));
  rthc_table[rthc_count].key_valid = key ? true : false;
  rthc_table[rthc_count].thr_tls_key = key ? new_key : 0;
  rthc_table[rthc_count].begin = begin;
  rthc_table[rthc_count].end = end;
  ++rthc_count;
  mdbx_trace("<< key %zu, rthc_count %u, rthc_limit %u", (size_t)new_key,
             rthc_count, rthc_limit);
  rthc_unlock();
  return MDBX_SUCCESS;

bailout:
  if (key)
    thread_key_delete(*key);
  rthc_unlock();
  return rc;
}

__cold void mdbx_rthc_remove(const mdbx_thread_key_t key) {
  thread_key_delete(key);
  rthc_lock();
  mdbx_trace(">> key %zu, rthc_count %u, rthc_limit %u", (size_t)key,
             rthc_count, rthc_limit);

  for (unsigned i = 0; i < rthc_count; ++i) {
    if (rthc_table[i].key_valid && key == rthc_table[i].thr_tls_key) {
      const uint32_t self_pid = mdbx_getpid();
      mdbx_trace("== [%i], %p ...%p, current-pid %d", i,
                 __Wpedantic_format_voidptr(rthc_table[i].begin),
                 __Wpedantic_format_voidptr(rthc_table[i].end), self_pid);

      for (MDBX_reader *rthc = rthc_table[i].begin; rthc < rthc_table[i].end;
           ++rthc) {
        if (rthc->mr_pid == self_pid) {
          rthc->mr_pid = 0;
          mdbx_trace("== cleanup %p", __Wpedantic_format_voidptr(rthc));
        }
      }
      if (--rthc_count > 0)
        rthc_table[i] = rthc_table[rthc_count];
      else if (rthc_table != rthc_table_static) {
        mdbx_free(rthc_table);
        rthc_table = rthc_table_static;
        rthc_limit = RTHC_INITIAL_LIMIT;
      }
      break;
    }
  }

  mdbx_trace("<< key %zu, rthc_count %u, rthc_limit %u", (size_t)key,
             rthc_count, rthc_limit);
  rthc_unlock();
}

//------------------------------------------------------------------------------

#define RTHC_ENVLIST_END ((MDBX_env *)((size_t)50459))
static MDBX_env *inprocess_lcklist_head = RTHC_ENVLIST_END;

static __inline void lcklist_lock(void) {
#if defined(_WIN32) || defined(_WIN64)
  EnterCriticalSection(&lcklist_critical_section);
#else
  mdbx_ensure(nullptr, pthread_mutex_lock(&lcklist_mutex) == 0);
#endif
}

static __inline void lcklist_unlock(void) {
#if defined(_WIN32) || defined(_WIN64)
  LeaveCriticalSection(&lcklist_critical_section);
#else
  mdbx_ensure(nullptr, pthread_mutex_unlock(&lcklist_mutex) == 0);
#endif
}

static uint64_t rrxmrrxmsx_0(uint64_t v) {
  /* Pelle Evensen's mixer, https://bit.ly/2HOfynt */
  v ^= (v << 39 | v >> 25) ^ (v << 14 | v >> 50);
  v *= UINT64_C(0xA24BAED4963EE407);
  v ^= (v << 40 | v >> 24) ^ (v << 15 | v >> 49);
  v *= UINT64_C(0x9FB21C651E98DF25);
  return v ^ v >> 28;
}

static int uniq_peek(const mdbx_mmap_t *pending, mdbx_mmap_t *scan) {
  int rc;
  uint64_t bait;
  if (pending->address) {
    bait = pending->lck->mti_bait_uniqueness;
    rc = MDBX_SUCCESS;
  } else {
    bait = 0 /* hush MSVC warning */;
    rc = mdbx_msync(scan, 0, sizeof(MDBX_lockinfo), true);
    if (rc == MDBX_SUCCESS)
      rc =
          mdbx_pread(pending->fd, &bait, sizeof(scan->lck->mti_bait_uniqueness),
                     offsetof(MDBX_lockinfo, mti_bait_uniqueness));
  }
  if (likely(rc == MDBX_SUCCESS) && bait == scan->lck->mti_bait_uniqueness)
    rc = MDBX_RESULT_TRUE;

  mdbx_trace("uniq-peek: %s, bait 0x%016" PRIx64 ",%s rc %d",
             pending->lck ? "mem" : "file", bait,
             (rc == MDBX_RESULT_TRUE) ? " found," : (rc ? " FAILED," : ""), rc);
  return rc;
}

static int uniq_poke(const mdbx_mmap_t *pending, mdbx_mmap_t *scan,
                     uint64_t *abra) {
  if (*abra == 0) {
    const size_t tid = mdbx_thread_self();
    size_t uit = 0;
    memcpy(&uit, &tid, (sizeof(tid) < sizeof(uit)) ? sizeof(tid) : sizeof(uit));
    *abra =
        rrxmrrxmsx_0(mdbx_osal_monotime() + UINT64_C(5873865991930747) * uit);
  }
  const uint64_t cadabra =
      rrxmrrxmsx_0(*abra + UINT64_C(7680760450171793) * (unsigned)mdbx_getpid())
          << 24 |
      *abra >> 40;
  scan->lck->mti_bait_uniqueness = cadabra;
  mdbx_flush_noncoherent_cpu_writeback();
  *abra = *abra * UINT64_C(6364136223846793005) + 1;
  return uniq_peek(pending, scan);
}

__cold static int uniq_check(const mdbx_mmap_t *pending, MDBX_env **found) {
  *found = nullptr;
  uint64_t salt = 0;
  for (MDBX_env *scan = inprocess_lcklist_head; scan != RTHC_ENVLIST_END;
       scan = scan->me_lcklist_next) {
    int err = scan->me_lck_mmap.lck->mti_bait_uniqueness
                  ? uniq_peek(pending, &scan->me_lck_mmap)
                  : uniq_poke(pending, &scan->me_lck_mmap, &salt);
    if (err == MDBX_ENODATA) {
      uint64_t length;
      if (likely(mdbx_filesize(pending->fd, &length) == MDBX_SUCCESS &&
                 length == 0)) {
        /* LY: skip checking since LCK-file is empty, i.e. just created. */
        mdbx_debug("uniq-probe: %s", "unique (new/empty lck)");
        return MDBX_RESULT_TRUE;
      }
    }
    if (err == MDBX_RESULT_TRUE)
      err = uniq_poke(pending, &scan->me_lck_mmap, &salt);
    if (err == MDBX_RESULT_TRUE) {
      (void)mdbx_msync(&scan->me_lck_mmap, 0, sizeof(MDBX_lockinfo), false);
      err = uniq_poke(pending, &scan->me_lck_mmap, &salt);
    }
    if (err == MDBX_RESULT_TRUE) {
      err = uniq_poke(pending, &scan->me_lck_mmap, &salt);
      *found = scan;
      mdbx_debug("uniq-probe: found %p", __Wpedantic_format_voidptr(*found));
      return MDBX_RESULT_FALSE;
    }
    if (unlikely(err != MDBX_SUCCESS)) {
      mdbx_debug("uniq-probe: failed rc %d", err);
      return err;
    }
  }

  mdbx_debug("uniq-probe: %s", "unique");
  return MDBX_RESULT_TRUE;
}

static int lcklist_detach_locked(MDBX_env *env) {
  MDBX_env *inprocess_neighbor = nullptr;
  int rc = MDBX_SUCCESS;
  if (env->me_lcklist_next != nullptr) {
    mdbx_ensure(env, env->me_lcklist_next != nullptr);
    mdbx_ensure(env, inprocess_lcklist_head != RTHC_ENVLIST_END);
    for (MDBX_env **ptr = &inprocess_lcklist_head; *ptr != RTHC_ENVLIST_END;
         ptr = &(*ptr)->me_lcklist_next) {
      if (*ptr == env) {
        *ptr = env->me_lcklist_next;
        env->me_lcklist_next = nullptr;
        break;
      }
    }
    mdbx_ensure(env, env->me_lcklist_next == nullptr);
  }

  rc = likely(mdbx_getpid() == env->me_pid)
           ? uniq_check(&env->me_lck_mmap, &inprocess_neighbor)
           : MDBX_PANIC;
  if (!inprocess_neighbor && env->me_live_reader)
    (void)mdbx_rpid_clear(env);
  if (!MDBX_IS_ERROR(rc))
    rc = mdbx_lck_destroy(env, inprocess_neighbor);
  return rc;
}

/*------------------------------------------------------------------------------
 * LY: State of the art quicksort-based sorting, with internal stack and
 *     shell-insertion-sort for small chunks (less than half of SORT_THRESHOLD).
 */

/* LY: Large threshold give some boost due less overhead in the inner qsort
 *     loops, but also a penalty in cases reverse-sorted data.
 *     So, 42 is magically but reasonable:
 *      - 0-3% faster than std::sort (from GNU C++ STL 2018) in most cases.
 *      - slower by a few ticks in a few cases for sequences shorter than 21. */
#define SORT_THRESHOLD 42

#define SORT_SWAP(TYPE, a, b)                                                  \
  do {                                                                         \
    const TYPE swap_tmp = (a);                                                 \
    (a) = (b);                                                                 \
    (b) = swap_tmp;                                                            \
  } while (0)

#define SORT_SHELLPASS(TYPE, CMP, begin, end, gap)                             \
  for (TYPE *i = begin + gap; i < end; ++i) {                                  \
    for (TYPE *j = i - (gap); j >= begin && CMP(*i, *j); j -= gap) {           \
      const TYPE tmp = *i;                                                     \
      do {                                                                     \
        j[gap] = *j;                                                           \
        j -= gap;                                                              \
      } while (j >= begin && CMP(tmp, *j));                                    \
      j[gap] = tmp;                                                            \
      break;                                                                   \
    }                                                                          \
  }

#define SORT_PUSH(low, high)                                                   \
  do {                                                                         \
    top->lo = (low);                                                           \
    top->hi = (high);                                                          \
    ++top;                                                                     \
  } while (0)

#define SORT_POP(low, high)                                                    \
  do {                                                                         \
    --top;                                                                     \
    low = top->lo;                                                             \
    high = top->hi;                                                            \
  } while (0)

#define SORT_IMPL(NAME, TYPE, CMP)                                             \
                                                                               \
  typedef struct {                                                             \
    TYPE *lo, *hi;                                                             \
  } NAME##_stack;                                                              \
                                                                               \
  static __hot void NAME(TYPE *const begin, TYPE *const end) {                 \
    const ptrdiff_t length = end - begin;                                      \
    if (length < 2)                                                            \
      return;                                                                  \
                                                                               \
    if (length > SORT_THRESHOLD / 2) {                                         \
      NAME##_stack stack[sizeof(unsigned) * CHAR_BIT], *top = stack;           \
                                                                               \
      TYPE *hi = end - 1;                                                      \
      TYPE *lo = begin;                                                        \
      while (true) {                                                           \
        TYPE *mid = lo + ((hi - lo) >> 1);                                     \
        if (CMP(*mid, *lo))                                                    \
          SORT_SWAP(TYPE, *mid, *lo);                                          \
        if (CMP(*hi, *mid)) {                                                  \
          SORT_SWAP(TYPE, *hi, *mid);                                          \
          if (CMP(*mid, *lo))                                                  \
            SORT_SWAP(TYPE, *mid, *lo);                                        \
        }                                                                      \
                                                                               \
        TYPE *right = hi - 1;                                                  \
        TYPE *left = lo + 1;                                                   \
        do {                                                                   \
          while (CMP(*mid, *right))                                            \
            --right;                                                           \
          while (CMP(*left, *mid))                                             \
            ++left;                                                            \
          if (left < right) {                                                  \
            SORT_SWAP(TYPE, *left, *right);                                    \
            if (mid == left)                                                   \
              mid = right;                                                     \
            else if (mid == right)                                             \
              mid = left;                                                      \
            ++left;                                                            \
            --right;                                                           \
          } else if (left == right) {                                          \
            ++left;                                                            \
            --right;                                                           \
            break;                                                             \
          }                                                                    \
        } while (left <= right);                                               \
                                                                               \
        if (lo + SORT_THRESHOLD > right) {                                     \
          if (left + SORT_THRESHOLD > hi) {                                    \
            if (top == stack)                                                  \
              break;                                                           \
            else                                                               \
              SORT_POP(lo, hi);                                                \
          } else                                                               \
            lo = left;                                                         \
        } else if (left + SORT_THRESHOLD > hi)                                 \
          hi = right;                                                          \
        else if (right - lo > hi - left) {                                     \
          SORT_PUSH(lo, right);                                                \
          lo = left;                                                           \
        } else {                                                               \
          SORT_PUSH(left, hi);                                                 \
          hi = right;                                                          \
        }                                                                      \
      }                                                                        \
    }                                                                          \
                                                                               \
    SORT_SHELLPASS(TYPE, CMP, begin, end, 8);                                  \
    SORT_SHELLPASS(TYPE, CMP, begin, end, 1);                                  \
    for (TYPE *scan = begin + 1; scan < end; ++scan)                           \
      assert(CMP(scan[-1], scan[0]));                                          \
  }

/*------------------------------------------------------------------------------
 * LY: Binary search */

#define SEARCH_IMPL(NAME, TYPE_LIST, TYPE_ARG, CMP)                            \
  static __always_inline TYPE_LIST *NAME(TYPE_LIST *first, unsigned length,    \
                                         const TYPE_ARG item) {                \
    TYPE_LIST *const begin = first, *const end = begin + length;               \
                                                                               \
    while (length > 3) {                                                       \
      const unsigned half = length >> 1;                                       \
      TYPE_LIST *const middle = first + half;                                  \
      if (CMP(*middle, item)) {                                                \
        first = middle + 1;                                                    \
        length -= half + 1;                                                    \
      } else                                                                   \
        length = half;                                                         \
    }                                                                          \
                                                                               \
    switch (length) {                                                          \
    case 3:                                                                    \
      if (!CMP(*first, item))                                                  \
        break;                                                                 \
      ++first;                                                                 \
      /* fall through */                                                       \
      __fallthrough;                                                           \
    case 2:                                                                    \
      if (!CMP(*first, item))                                                  \
        break;                                                                 \
      ++first;                                                                 \
      /* fall through */                                                       \
      __fallthrough;                                                           \
    case 1:                                                                    \
      if (CMP(*first, item))                                                   \
        ++first;                                                               \
    }                                                                          \
                                                                               \
    for (TYPE_LIST *scan = begin; scan < first; ++scan)                        \
      assert(CMP(*scan, item));                                                \
    for (TYPE_LIST *scan = first; scan < end; ++scan)                          \
      assert(!CMP(*scan, item));                                               \
    (void)begin, (void)end;                                                    \
                                                                               \
    return first;                                                              \
  }

/*----------------------------------------------------------------------------*/

static __inline size_t pnl2bytes(const size_t size) {
  assert(size > 0 && size <= MDBX_PNL_MAX * 2);
  size_t bytes = roundup_powerof2(MDBX_ASSUME_MALLOC_OVERHEAD +
                                      sizeof(pgno_t) * (size + 2),
                                  MDBX_PNL_GRANULATE * sizeof(pgno_t)) -
                 MDBX_ASSUME_MALLOC_OVERHEAD;
  return bytes;
}

static __inline pgno_t bytes2pnl(const size_t bytes) {
  size_t size = bytes / sizeof(pgno_t);
  assert(size > 2 && size <= MDBX_PNL_MAX * 2);
  return (pgno_t)size - 2;
}

static MDBX_PNL mdbx_pnl_alloc(size_t size) {
  size_t bytes = pnl2bytes(size);
  MDBX_PNL pl = mdbx_malloc(bytes);
  if (likely(pl)) {
#if __GLIBC_PREREQ(2, 12) || defined(__FreeBSD__) || defined(__OpenBSD__) ||   \
    defined(malloc_usable_size)
    bytes = malloc_usable_size(pl);
#endif /* malloc_usable_size */
    pl[0] = bytes2pnl(bytes);
    assert(pl[0] >= size);
    pl[1] = 0;
    pl += 1;
  }
  return pl;
}

static void mdbx_pnl_free(MDBX_PNL pl) {
  if (likely(pl))
    mdbx_free(pl - 1);
}

/* Shrink the PNL to the default size if it has grown larger */
static void mdbx_pnl_shrink(MDBX_PNL *ppl) {
  assert(bytes2pnl(pnl2bytes(MDBX_PNL_INITIAL)) == MDBX_PNL_INITIAL);
  assert(MDBX_PNL_SIZE(*ppl) <= MDBX_PNL_MAX &&
         MDBX_PNL_ALLOCLEN(*ppl) >= MDBX_PNL_SIZE(*ppl));
  MDBX_PNL_SIZE(*ppl) = 0;
  if (unlikely(MDBX_PNL_ALLOCLEN(*ppl) >
               MDBX_PNL_INITIAL * 2 - MDBX_CACHELINE_SIZE / sizeof(pgno_t))) {
    size_t bytes = pnl2bytes(MDBX_PNL_INITIAL);
    MDBX_PNL pl = mdbx_realloc(*ppl - 1, bytes);
    if (likely(pl)) {
#if __GLIBC_PREREQ(2, 12) || defined(__FreeBSD__) || defined(__OpenBSD__) ||   \
    defined(malloc_usable_size)
      bytes = malloc_usable_size(pl);
#endif /* malloc_usable_size */
      *pl = bytes2pnl(bytes);
      *ppl = pl + 1;
    }
  }
}

/* Grow the PNL to the size growed to at least given size */
static int mdbx_pnl_reserve(MDBX_PNL *ppl, const size_t wanna) {
  const size_t allocated = MDBX_PNL_ALLOCLEN(*ppl);
  assert(MDBX_PNL_SIZE(*ppl) <= MDBX_PNL_MAX &&
         MDBX_PNL_ALLOCLEN(*ppl) >= MDBX_PNL_SIZE(*ppl));
  if (likely(allocated >= wanna))
    return MDBX_SUCCESS;

  if (unlikely(wanna > /* paranoia */ MDBX_PNL_MAX))
    return MDBX_TXN_FULL;

  const size_t size = (wanna + wanna - allocated < MDBX_PNL_MAX)
                          ? wanna + wanna - allocated
                          : MDBX_PNL_MAX;
  size_t bytes = pnl2bytes(size);
  MDBX_PNL pl = mdbx_realloc(*ppl - 1, bytes);
  if (likely(pl)) {
#if __GLIBC_PREREQ(2, 12) || defined(__FreeBSD__) || defined(__OpenBSD__) ||   \
    defined(malloc_usable_size)
    bytes = malloc_usable_size(pl);
#endif /* malloc_usable_size */
    *pl = bytes2pnl(bytes);
    assert(*pl >= wanna);
    *ppl = pl + 1;
    return MDBX_SUCCESS;
  }
  return MDBX_ENOMEM;
}

/* Make room for num additional elements in an PNL */
static __inline int __must_check_result mdbx_pnl_need(MDBX_PNL *ppl,
                                                      size_t num) {
  assert(MDBX_PNL_SIZE(*ppl) <= MDBX_PNL_MAX &&
         MDBX_PNL_ALLOCLEN(*ppl) >= MDBX_PNL_SIZE(*ppl));
  assert(num <= MDBX_PNL_MAX);
  const size_t wanna = MDBX_PNL_SIZE(*ppl) + num;
  return likely(MDBX_PNL_ALLOCLEN(*ppl) >= wanna)
             ? MDBX_SUCCESS
             : mdbx_pnl_reserve(ppl, wanna);
}

static __inline void mdbx_pnl_xappend(MDBX_PNL pl, pgno_t pgno) {
  assert(MDBX_PNL_SIZE(pl) < MDBX_PNL_ALLOCLEN(pl));
  if (mdbx_assert_enabled()) {
    for (unsigned i = MDBX_PNL_SIZE(pl); i > 0; --i)
      assert(pgno != pl[i]);
  }
  MDBX_PNL_SIZE(pl) += 1;
  MDBX_PNL_LAST(pl) = pgno;
}

/* Append an pgno onto an unsorted PNL */
static __hot int __must_check_result mdbx_pnl_append(MDBX_PNL *ppl,
                                                     pgno_t pgno) {
  /* Too big? */
  if (unlikely(MDBX_PNL_SIZE(*ppl) == MDBX_PNL_ALLOCLEN(*ppl))) {
    int rc = mdbx_pnl_need(ppl, MDBX_PNL_GRANULATE);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }
  mdbx_pnl_xappend(*ppl, pgno);
  return MDBX_SUCCESS;
}

/* Append an PNL onto an unsorted PNL */
static int __must_check_result mdbx_pnl_append_list(MDBX_PNL *ppl,
                                                    MDBX_PNL append) {
  const unsigned len = MDBX_PNL_SIZE(append);
  if (likely(len)) {
    int rc = mdbx_pnl_need(ppl, MDBX_PNL_SIZE(append));
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;

    const MDBX_PNL pnl = *ppl;
    unsigned w = MDBX_PNL_SIZE(pnl), r = 1;
    do
      pnl[++w] = append[r];
    while (++r <= len);
    MDBX_PNL_SIZE(pnl) = w;
  }
  return MDBX_SUCCESS;
}

/* Append an pgno range onto an unsorted PNL */
static __hot int __must_check_result mdbx_pnl_append_range(MDBX_PNL *ppl,
                                                           pgno_t pgno,
                                                           unsigned n) {
  assert(n > 0);
  int rc = mdbx_pnl_need(ppl, n);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  const MDBX_PNL pnl = *ppl;
#if MDBX_PNL_ASCENDING
  unsigned w = MDBX_PNL_SIZE(pnl);
  do
    pnl[++w] = pgno++;
  while (--n);
  MDBX_PNL_SIZE(pnl) = w;
#else
  unsigned w = MDBX_PNL_SIZE(pnl) + n;
  MDBX_PNL_SIZE(pnl) = w;
  do
    pnl[w--] = --n + pgno;
  while (n);
#endif

  return MDBX_SUCCESS;
}

/* Append an pgno range into the sorted PNL */
static __hot int __must_check_result mdbx_pnl_insert_range(MDBX_PNL *ppl,
                                                           pgno_t pgno,
                                                           unsigned n) {
  assert(n > 0);
  int rc = mdbx_pnl_need(ppl, n);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  const MDBX_PNL pnl = *ppl;
  unsigned r = MDBX_PNL_SIZE(pnl), w = r + n;
  MDBX_PNL_SIZE(pnl) = w;
  while (r && MDBX_PNL_DISORDERED(pnl[r], pgno))
    pnl[w--] = pnl[r--];

  for (pgno_t fill = MDBX_PNL_ASCENDING ? pgno + n : pgno; w > r; --w)
    pnl[w] = MDBX_PNL_ASCENDING ? --fill : fill++;

  return MDBX_SUCCESS;
}

static bool __hot mdbx_pnl_check(const MDBX_PNL pl, const pgno_t limit) {
  assert(limit >= MIN_PAGENO && limit <= MAX_PAGENO + 1);
  if (likely(MDBX_PNL_SIZE(pl))) {
    assert(MDBX_PNL_LEAST(pl) >= MIN_PAGENO);
    assert(MDBX_PNL_MOST(pl) < limit);
    assert(MDBX_PNL_SIZE(pl) <= MDBX_PNL_MAX);
    if (unlikely(MDBX_PNL_SIZE(pl) > MDBX_PNL_MAX * 3 / 2))
      return false;
    if (unlikely(MDBX_PNL_LEAST(pl) < MIN_PAGENO))
      return false;
    if (unlikely(MDBX_PNL_MOST(pl) >= limit))
      return false;
    for (const pgno_t *scan = &MDBX_PNL_LAST(pl); --scan > pl;) {
      assert(MDBX_PNL_ORDERED(scan[0], scan[1]));
      if (unlikely(!MDBX_PNL_ORDERED(scan[0], scan[1])))
        return false;
    }
  }
  return true;
}

static __inline bool mdbx_pnl_check4assert(const MDBX_PNL pl,
                                           const pgno_t limit) {
  if (unlikely(pl == nullptr))
    return true;
  assert(MDBX_PNL_ALLOCLEN(pl) >= MDBX_PNL_SIZE(pl));
  if (unlikely(MDBX_PNL_ALLOCLEN(pl) < MDBX_PNL_SIZE(pl)))
    return false;
  return mdbx_pnl_check(pl, limit);
}

/* Merge an PNL onto an PNL. The destination PNL must be big enough */
static void __hot mdbx_pnl_xmerge(MDBX_PNL dst, const MDBX_PNL src) {
  assert(mdbx_pnl_check4assert(dst, MAX_PAGENO + 1));
  assert(mdbx_pnl_check(src, MAX_PAGENO + 1));
  const size_t total = MDBX_PNL_SIZE(dst) + MDBX_PNL_SIZE(src);
  assert(MDBX_PNL_ALLOCLEN(dst) >= total);
  pgno_t *w = dst + total;
  pgno_t *d = dst + MDBX_PNL_SIZE(dst);
  const pgno_t *s = src + MDBX_PNL_SIZE(src);
  dst[0] = /* detent for scan below */ (MDBX_PNL_ASCENDING ? 0 : ~(pgno_t)0);
  while (s > src) {
    while (MDBX_PNL_ORDERED(*s, *d))
      *w-- = *d--;
    *w-- = *s--;
  }
  MDBX_PNL_SIZE(dst) = (pgno_t)total;
  assert(mdbx_pnl_check4assert(dst, MAX_PAGENO + 1));
}

SORT_IMPL(pgno_sort, pgno_t, MDBX_PNL_ORDERED)
static __hot void mdbx_pnl_sort(MDBX_PNL pnl) {
  pgno_sort(MDBX_PNL_BEGIN(pnl), MDBX_PNL_END(pnl));
  assert(mdbx_pnl_check(pnl, MAX_PAGENO + 1));
}

/* Search for an pgno in an PNL.
 * Returns The index of the first item greater than or equal to pgno. */
SEARCH_IMPL(pgno_bsearch, pgno_t, pgno_t, MDBX_PNL_ORDERED)

static __hot unsigned mdbx_pnl_search(MDBX_PNL pnl, pgno_t id) {
  assert(mdbx_pnl_check4assert(pnl, MAX_PAGENO + 1));
  pgno_t *begin = MDBX_PNL_BEGIN(pnl);
  pgno_t *it = pgno_bsearch(begin, MDBX_PNL_SIZE(pnl), id);
  pgno_t *end = begin + MDBX_PNL_SIZE(pnl);
  assert(it >= begin && it <= end);
  if (it != begin)
    assert(MDBX_PNL_ORDERED(it[-1], id));
  if (it != end)
    assert(!MDBX_PNL_ORDERED(it[0], id));
  return (unsigned)(it - begin + 1);
}

static __hot unsigned mdbx_pnl_exist(MDBX_PNL pnl, pgno_t id) {
  unsigned n = mdbx_pnl_search(pnl, id);
  return (n <= MDBX_PNL_SIZE(pnl) && pnl[n] == id) ? n : 0;
}

/*----------------------------------------------------------------------------*/

static __inline size_t txl2bytes(const size_t size) {
  assert(size > 0 && size <= MDBX_TXL_MAX * 2);
  size_t bytes = roundup_powerof2(MDBX_ASSUME_MALLOC_OVERHEAD +
                                      sizeof(txnid_t) * (size + 2),
                                  MDBX_TXL_GRANULATE * sizeof(txnid_t)) -
                 MDBX_ASSUME_MALLOC_OVERHEAD;
  return bytes;
}

static __inline size_t bytes2txl(const size_t bytes) {
  size_t size = bytes / sizeof(txnid_t);
  assert(size > 2 && size <= MDBX_TXL_MAX * 2);
  return size - 2;
}

static MDBX_TXL mdbx_txl_alloc(void) {
  size_t bytes = txl2bytes(MDBX_TXL_INITIAL);
  MDBX_TXL tl = mdbx_malloc(bytes);
  if (likely(tl)) {
#if __GLIBC_PREREQ(2, 12) || defined(__FreeBSD__) || defined(__OpenBSD__) ||   \
    defined(malloc_usable_size)
    bytes = malloc_usable_size(tl);
#endif /* malloc_usable_size */
    tl[0] = bytes2txl(bytes);
    assert(tl[0] >= MDBX_TXL_INITIAL);
    tl[1] = 0;
    tl += 1;
  }
  return tl;
}

static void mdbx_txl_free(MDBX_TXL tl) {
  if (likely(tl))
    mdbx_free(tl - 1);
}

static int mdbx_txl_reserve(MDBX_TXL *ptl, const size_t wanna) {
  const size_t allocated = (size_t)MDBX_PNL_ALLOCLEN(*ptl);
  assert(MDBX_PNL_SIZE(*ptl) <= MDBX_TXL_MAX &&
         MDBX_PNL_ALLOCLEN(*ptl) >= MDBX_PNL_SIZE(*ptl));
  if (likely(allocated >= wanna))
    return MDBX_SUCCESS;

  if (unlikely(wanna > /* paranoia */ MDBX_TXL_MAX))
    return MDBX_TXN_FULL;

  const size_t size = (wanna + wanna - allocated < MDBX_TXL_MAX)
                          ? wanna + wanna - allocated
                          : MDBX_TXL_MAX;
  size_t bytes = txl2bytes(size);
  MDBX_TXL tl = mdbx_realloc(*ptl - 1, bytes);
  if (likely(tl)) {
#if __GLIBC_PREREQ(2, 12) || defined(__FreeBSD__) || defined(__OpenBSD__) ||   \
    defined(malloc_usable_size)
    bytes = malloc_usable_size(tl);
#endif /* malloc_usable_size */
    *tl = bytes2txl(bytes);
    assert(*tl >= wanna);
    *ptl = tl + 1;
    return MDBX_SUCCESS;
  }
  return MDBX_ENOMEM;
}

static __inline int __must_check_result mdbx_txl_need(MDBX_TXL *ptl,
                                                      size_t num) {
  assert(MDBX_PNL_SIZE(*ptl) <= MDBX_TXL_MAX &&
         MDBX_PNL_ALLOCLEN(*ptl) >= MDBX_PNL_SIZE(*ptl));
  assert(num <= MDBX_PNL_MAX);
  const size_t wanna = (size_t)MDBX_PNL_SIZE(*ptl) + num;
  return likely(MDBX_PNL_ALLOCLEN(*ptl) >= wanna)
             ? MDBX_SUCCESS
             : mdbx_txl_reserve(ptl, wanna);
}

static __inline void mdbx_txl_xappend(MDBX_TXL tl, txnid_t id) {
  assert(MDBX_PNL_SIZE(tl) < MDBX_PNL_ALLOCLEN(tl));
  MDBX_PNL_SIZE(tl) += 1;
  MDBX_PNL_LAST(tl) = id;
}

#define TXNID_SORT_CMP(first, last) ((first) > (last))
SORT_IMPL(txnid_sort, txnid_t, TXNID_SORT_CMP)
static void mdbx_txl_sort(MDBX_TXL tl) {
  txnid_sort(MDBX_PNL_BEGIN(tl), MDBX_PNL_END(tl));
}

static int __must_check_result mdbx_txl_append(MDBX_TXL *ptl, txnid_t id) {
  if (unlikely(MDBX_PNL_SIZE(*ptl) == MDBX_PNL_ALLOCLEN(*ptl))) {
    int rc = mdbx_txl_need(ptl, MDBX_TXL_GRANULATE);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }
  mdbx_txl_xappend(*ptl, id);
  return MDBX_SUCCESS;
}

/*----------------------------------------------------------------------------*/

#define DP_SORT_CMP(first, last) ((first).pgno < (last).pgno)
SORT_IMPL(dp_sort, MDBX_DP, DP_SORT_CMP)
static __inline MDBX_DPL mdbx_dpl_sort(MDBX_DPL dl) {
  assert(dl->length <= MDBX_DPL_TXNFULL);
  assert(dl->sorted <= dl->length);
  if (dl->sorted != dl->length) {
    dl->sorted = dl->length;
    dp_sort(dl + 1, dl + dl->length + 1);
  }
  return dl;
}

/* Returns the index of the first dirty-page whose pgno
 * member is greater than or equal to id. */
#define DP_SEARCH_CMP(dp, id) ((dp).pgno < (id))
SEARCH_IMPL(dp_bsearch, MDBX_DP, pgno_t, DP_SEARCH_CMP)

static unsigned __hot mdbx_dpl_search(MDBX_DPL dl, pgno_t pgno) {
  if (dl->sorted < dl->length) {
    /* unsorted tail case  */
#if MDBX_DEBUG
    for (const MDBX_DP *ptr = dl + dl->sorted; --ptr > dl;) {
      assert(ptr[0].pgno < ptr[1].pgno);
      assert(ptr[0].pgno >= NUM_METAS);
    }
#endif

    /* try linear search until the threshold */
    if (dl->length - dl->sorted < SORT_THRESHOLD / 2) {
      unsigned i = dl->length;
      while (i - dl->sorted > 7) {
        if (dl[i].pgno == pgno)
          return i;
        if (dl[i - 1].pgno == pgno)
          return i - 1;
        if (dl[i - 2].pgno == pgno)
          return i - 2;
        if (dl[i - 3].pgno == pgno)
          return i - 3;
        if (dl[i - 4].pgno == pgno)
          return i - 4;
        if (dl[i - 5].pgno == pgno)
          return i - 5;
        if (dl[i - 6].pgno == pgno)
          return i - 6;
        if (dl[i - 7].pgno == pgno)
          return i - 7;
        i -= 8;
      }
      while (i > dl->sorted) {
        if (dl[i].pgno == pgno)
          return i;
        --i;
      }

      MDBX_DPL it = dp_bsearch(dl + 1, i, pgno);
      return (unsigned)(it - dl);
    }

    /* sort a whole */
    dl->sorted = dl->length;
    dp_sort(dl + 1, dl + dl->length + 1);
  }

#if MDBX_DEBUG
  for (const MDBX_DP *ptr = dl + dl->length; --ptr > dl;) {
    assert(ptr[0].pgno < ptr[1].pgno);
    assert(ptr[0].pgno >= NUM_METAS);
  }
#endif

  MDBX_DPL it = dp_bsearch(dl + 1, dl->length, pgno);
  return (unsigned)(it - dl);
}

static __inline MDBX_page *mdbx_dpl_find(MDBX_DPL dl, pgno_t pgno) {
  const unsigned i = mdbx_dpl_search(dl, pgno);
  assert((int)i > 0);
  return (i <= dl->length && dl[i].pgno == pgno) ? dl[i].ptr : nullptr;
}

static __hot MDBX_page *mdbx_dpl_remove(MDBX_DPL dl, pgno_t prno) {
  unsigned i = mdbx_dpl_search(dl, prno);
  assert((int)i > 0);
  MDBX_page *mp = nullptr;
  if (i <= dl->length && dl[i].pgno == prno) {
    dl->sorted -= dl->sorted >= i;
    mp = dl[i].ptr;
    while (i < dl->length) {
      dl[i] = dl[i + 1];
      ++i;
    }
    dl->length -= 1;
  }
  return mp;
}

static __inline int __must_check_result mdbx_dpl_append(MDBX_DPL dl,
                                                        pgno_t pgno,
                                                        MDBX_page *page) {
  assert(dl->length <= MDBX_DPL_TXNFULL);
#if MDBX_DEBUG
  for (unsigned i = dl->length; i > 0; --i) {
    assert(dl[i].pgno != pgno);
    if (unlikely(dl[i].pgno == pgno))
      return MDBX_PROBLEM;
  }
#endif

  if (unlikely(dl->length == MDBX_DPL_TXNFULL))
    return MDBX_TXN_FULL;

  /* append page */
  const unsigned n = dl->length + 1;
  if (n == 1 || (dl->sorted >= dl->length && dl[n - 1].pgno < pgno))
    dl->sorted = n;
  dl->length = n;
  dl[n].pgno = pgno;
  dl[n].ptr = page;
  return MDBX_SUCCESS;
}

static __inline void mdbx_dpl_clear(MDBX_DPL dl) {
  dl->sorted = dl->length = 0;
}

/*----------------------------------------------------------------------------*/

#ifndef MDBX_ALLOY
uint8_t mdbx_runtime_flags = MDBX_RUNTIME_FLAGS_INIT;
uint8_t mdbx_loglevel = MDBX_DEBUG;
MDBX_debug_func *mdbx_debug_logger;
#endif /* MDBX_ALLOY */

static bool mdbx_refund(MDBX_txn *txn);
static __must_check_result int mdbx_page_retire(MDBX_cursor *mc, MDBX_page *mp);
static __must_check_result int mdbx_page_loose(MDBX_txn *txn, MDBX_page *mp);
static int mdbx_page_alloc(MDBX_cursor *mc, unsigned num, MDBX_page **mp,
                           int flags);
static int mdbx_page_new(MDBX_cursor *mc, uint32_t flags, unsigned num,
                         MDBX_page **mp);
static int mdbx_page_touch(MDBX_cursor *mc);
static int mdbx_cursor_touch(MDBX_cursor *mc);

#define MDBX_END_NAMES                                                         \
  {                                                                            \
    "committed", "empty-commit", "abort", "reset", "reset-tmp", "fail-begin",  \
        "fail-beginchild"                                                      \
  }
enum {
  /* mdbx_txn_end operation number, for logging */
  MDBX_END_COMMITTED,
  MDBX_END_EMPTY_COMMIT,
  MDBX_END_ABORT,
  MDBX_END_RESET,
  MDBX_END_RESET_TMP,
  MDBX_END_FAIL_BEGIN,
  MDBX_END_FAIL_BEGINCHILD
};
#define MDBX_END_OPMASK 0x0F  /* mask for mdbx_txn_end() operation number */
#define MDBX_END_UPDATE 0x10  /* update env state (DBIs) */
#define MDBX_END_FREE 0x20    /* free txn unless it is MDBX_env.me_txn0 */
#define MDBX_END_EOTDONE 0x40 /* txn's cursors already closed */
#define MDBX_END_SLOT 0x80    /* release any reader slot if MDBX_NOTLS */
static int mdbx_txn_end(MDBX_txn *txn, unsigned mode);

static int __must_check_result mdbx_page_get(MDBX_cursor *mc, pgno_t pgno,
                                             MDBX_page **mp, int *lvl);
static int __must_check_result mdbx_page_search_root(MDBX_cursor *mc,
                                                     MDBX_val *key, int modify);

#define MDBX_PS_MODIFY 1
#define MDBX_PS_ROOTONLY 2
#define MDBX_PS_FIRST 4
#define MDBX_PS_LAST 8
static int __must_check_result mdbx_page_search(MDBX_cursor *mc, MDBX_val *key,
                                                int flags);
static int __must_check_result mdbx_page_merge(MDBX_cursor *csrc,
                                               MDBX_cursor *cdst);
static int __must_check_result mdbx_page_flush(MDBX_txn *txn,
                                               const unsigned keep);

#define MDBX_SPLIT_REPLACE MDBX_APPENDDUP /* newkey is not new */
static int __must_check_result mdbx_page_split(MDBX_cursor *mc,
                                               const MDBX_val *newkey,
                                               MDBX_val *newdata,
                                               pgno_t newpgno, unsigned nflags);

static int __must_check_result mdbx_read_header(MDBX_env *env, MDBX_meta *meta,
                                                uint64_t *filesize);
static int __must_check_result mdbx_sync_locked(MDBX_env *env, unsigned flags,
                                                MDBX_meta *const pending);
static int mdbx_env_close0(MDBX_env *env);

static MDBX_node *mdbx_node_search(MDBX_cursor *mc, MDBX_val *key, int *exactp);

static int __must_check_result mdbx_node_add_branch(MDBX_cursor *mc,
                                                    unsigned indx,
                                                    const MDBX_val *key,
                                                    pgno_t pgno);
static int __must_check_result mdbx_node_add_leaf(MDBX_cursor *mc,
                                                  unsigned indx,
                                                  const MDBX_val *key,
                                                  MDBX_val *data,
                                                  unsigned flags);
static int __must_check_result mdbx_node_add_leaf2(MDBX_cursor *mc,
                                                   unsigned indx,
                                                   const MDBX_val *key);

static void mdbx_node_del(MDBX_cursor *mc, size_t ksize);
static void mdbx_node_shrink(MDBX_page *mp, unsigned indx);
static int __must_check_result mdbx_node_move(MDBX_cursor *csrc,
                                              MDBX_cursor *cdst, int fromleft);
static int __must_check_result mdbx_node_read(MDBX_cursor *mc, MDBX_node *leaf,
                                              MDBX_val *data);
static int __must_check_result mdbx_rebalance(MDBX_cursor *mc);
static int __must_check_result mdbx_update_key(MDBX_cursor *mc,
                                               const MDBX_val *key);

static void mdbx_cursor_pop(MDBX_cursor *mc);
static int __must_check_result mdbx_cursor_push(MDBX_cursor *mc, MDBX_page *mp);

static int __must_check_result mdbx_audit_ex(MDBX_txn *txn,
                                             unsigned retired_stored,
                                             bool dont_filter_gc);
static __maybe_unused __inline int __must_check_result
mdbx_audit(MDBX_txn *txn) {
  return mdbx_audit_ex(txn, 0, (txn->mt_flags & MDBX_RDONLY) != 0);
}

static int __must_check_result mdbx_page_check(MDBX_env *env,
                                               const MDBX_page *const mp,
                                               bool maybe_unfinished);
static int __must_check_result mdbx_cursor_check(MDBX_cursor *mc, bool pending);
static int __must_check_result mdbx_cursor_del0(MDBX_cursor *mc);
static int __must_check_result mdbx_del0(MDBX_txn *txn, MDBX_dbi dbi,
                                         MDBX_val *key, MDBX_val *data,
                                         unsigned flags);
static int __must_check_result mdbx_cursor_sibling(MDBX_cursor *mc,
                                                   int move_right);
static int __must_check_result mdbx_cursor_next(MDBX_cursor *mc, MDBX_val *key,
                                                MDBX_val *data,
                                                MDBX_cursor_op op);
static int __must_check_result mdbx_cursor_prev(MDBX_cursor *mc, MDBX_val *key,
                                                MDBX_val *data,
                                                MDBX_cursor_op op);
static int __must_check_result mdbx_cursor_set(MDBX_cursor *mc, MDBX_val *key,
                                               MDBX_val *data,
                                               MDBX_cursor_op op, int *exactp);
static int __must_check_result mdbx_cursor_first(MDBX_cursor *mc, MDBX_val *key,
                                                 MDBX_val *data);
static int __must_check_result mdbx_cursor_last(MDBX_cursor *mc, MDBX_val *key,
                                                MDBX_val *data);

static int __must_check_result mdbx_cursor_init(MDBX_cursor *mc, MDBX_txn *txn,
                                                MDBX_dbi dbi);
static int __must_check_result mdbx_xcursor_init0(MDBX_cursor *mc);
static int __must_check_result mdbx_xcursor_init1(MDBX_cursor *mc,
                                                  MDBX_node *node);
static int __must_check_result mdbx_xcursor_init2(MDBX_cursor *mc,
                                                  MDBX_xcursor *src_mx,
                                                  int force);
static void mdbx_cursor_copy(const MDBX_cursor *csrc, MDBX_cursor *cdst);

static int __must_check_result mdbx_drop0(MDBX_cursor *mc, int subs);
static int __must_check_result mdbx_fetch_sdb(MDBX_txn *txn, MDBX_dbi dbi);

static MDBX_cmp_func mdbx_cmp_memn, mdbx_cmp_memnr, mdbx_cmp_int_align4,
    mdbx_cmp_int_align2, mdbx_cmp_int_unaligned;

static const char *__mdbx_strerr(int errnum) {
  /* Table of descriptions for MDBX errors */
  static const char *const tbl[] = {
      "MDBX_KEYEXIST: Key/data pair already exists",
      "MDBX_NOTFOUND: No matching key/data pair found",
      "MDBX_PAGE_NOTFOUND: Requested page not found",
      "MDBX_CORRUPTED: Database is corrupted",
      "MDBX_PANIC: Environment had fatal error",
      "MDBX_VERSION_MISMATCH: DB version mismatch libmdbx",
      "MDBX_INVALID: File is not an MDBX file",
      "MDBX_MAP_FULL: Environment mapsize limit reached",
      "MDBX_DBS_FULL: Too may DBI (maxdbs reached)",
      "MDBX_READERS_FULL: Too many readers (maxreaders reached)",
      NULL /* MDBX_TLS_FULL (-30789): unused in MDBX */,
      "MDBX_TXN_FULL: Transaction has too many dirty pages, "
      "i.e transaction too big",
      "MDBX_CURSOR_FULL: Internal error - cursor stack limit reached",
      "MDBX_PAGE_FULL: Internal error - page has no more space",
      "MDBX_MAP_RESIZED: Database contents grew beyond environment mapsize",
      "MDBX_INCOMPATIBLE: Operation and DB incompatible, or DB flags changed",
      "MDBX_BAD_RSLOT: Invalid reuse of reader locktable slot",
      "MDBX_BAD_TXN: Transaction must abort, has a child, or is invalid",
      "MDBX_BAD_VALSIZE: Unsupported size of key/DB name/data, or wrong "
      "DUPFIXED size",
      "MDBX_BAD_DBI: The specified DBI handle was closed/changed unexpectedly",
      "MDBX_PROBLEM: Unexpected problem - txn should abort",
      "MDBX_BUSY: Another write transaction is running or "
      "environment is already used while opening with MDBX_EXCLUSIVE flag",
  };

  if (errnum >= MDBX_KEYEXIST && errnum <= MDBX_LAST_ERRCODE) {
    int i = errnum - MDBX_KEYEXIST;
    return tbl[i];
  }

  switch (errnum) {
  case MDBX_SUCCESS:
    return "MDBX_SUCCESS: Successful";
  case MDBX_EMULTIVAL:
    return "MDBX_EMULTIVAL: Unable to update multi-value for the given key";
  case MDBX_EBADSIGN:
    return "MDBX_EBADSIGN: Wrong signature of a runtime object(s)";
  case MDBX_WANNA_RECOVERY:
    return "MDBX_WANNA_RECOVERY: Database should be recovered, but this could "
           "NOT be done in a read-only mode";
  case MDBX_EKEYMISMATCH:
    return "MDBX_EKEYMISMATCH: The given key value is mismatched to the "
           "current cursor position";
  case MDBX_TOO_LARGE:
    return "MDBX_TOO_LARGE: Database is too large for current system, "
           "e.g. could NOT be mapped into RAM";
  case MDBX_THREAD_MISMATCH:
    return "MDBX_THREAD_MISMATCH: A thread has attempted to use a not "
           "owned object, e.g. a transaction that started by another thread";
  default:
    return NULL;
  }
}

const char *__cold mdbx_strerror_r(int errnum, char *buf, size_t buflen) {
  const char *msg = __mdbx_strerr(errnum);
  if (!msg && buflen > 0 && buflen < INT_MAX) {
#if defined(_WIN32) || defined(_WIN64)
    const DWORD size = FormatMessageA(
        FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, NULL,
        errnum, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), buf, (DWORD)buflen,
        NULL);
    return size ? buf : NULL;
#elif defined(_GNU_SOURCE) && defined(__GLIBC__)
    /* GNU-specific */
    if (errnum > 0)
      msg = strerror_r(errnum, buf, buflen);
#elif (_POSIX_C_SOURCE >= 200112L || _XOPEN_SOURCE >= 600)
    /* XSI-compliant */
    if (errnum > 0 && strerror_r(errnum, buf, buflen) == 0)
      msg = buf;
#else
    if (errnum > 0) {
      msg = strerror(errnum);
      if (msg) {
        strncpy(buf, msg, buflen);
        msg = buf;
      }
    }
#endif
    if (!msg) {
      (void)snprintf(buf, buflen, "error %d", errnum);
      msg = buf;
    }
    buf[buflen - 1] = '\0';
  }
  return msg;
}

const char *__cold mdbx_strerror(int errnum) {
#if defined(_WIN32) || defined(_WIN64)
  static char buf[1024];
  return mdbx_strerror_r(errnum, buf, sizeof(buf));
#else
  const char *msg = __mdbx_strerr(errnum);
  if (!msg) {
    if (errnum > 0)
      msg = strerror(errnum);
    if (!msg) {
      static char buf[32];
      (void)snprintf(buf, sizeof(buf) - 1, "error %d", errnum);
      msg = buf;
    }
  }
  return msg;
#endif
}

#if defined(_WIN32) || defined(_WIN64) /* Bit of madness for Windows */
const char *mdbx_strerror_r_ANSI2OEM(int errnum, char *buf, size_t buflen) {
  const char *msg = __mdbx_strerr(errnum);
  if (!msg && buflen > 0 && buflen < INT_MAX) {
    const DWORD size = FormatMessageA(
        FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, NULL,
        errnum, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), buf, (DWORD)buflen,
        NULL);
    if (size && CharToOemBuffA(buf, buf, size))
      msg = buf;
  }
  return msg;
}

const char *mdbx_strerror_ANSI2OEM(int errnum) {
  static char buf[1024];
  return mdbx_strerror_r_ANSI2OEM(errnum, buf, sizeof(buf));
}
#endif /* Bit of madness for Windows */

static txnid_t mdbx_oomkick(MDBX_env *env, const txnid_t laggard);

void __cold mdbx_debug_log(int level, const char *function, int line,
                           const char *fmt, ...) {
  va_list args;

  va_start(args, fmt);
  if (mdbx_debug_logger)
    mdbx_debug_logger(level, function, line, fmt, args);
  else {
#if defined(_WIN32) || defined(_WIN64)
    if (IsDebuggerPresent()) {
      int prefix_len = 0;
      char *prefix = nullptr;
      if (function && line > 0)
        prefix_len = mdbx_asprintf(&prefix, "%s:%d ", function, line);
      else if (function)
        prefix_len = mdbx_asprintf(&prefix, "%s: ", function);
      else if (line > 0)
        prefix_len = mdbx_asprintf(&prefix, "%d: ", line);
      if (prefix_len > 0 && prefix) {
        OutputDebugStringA(prefix);
        mdbx_free(prefix);
      }
      char *msg = nullptr;
      int msg_len = mdbx_vasprintf(&msg, fmt, args);
      if (msg_len > 0 && msg) {
        OutputDebugStringA(msg);
        mdbx_free(msg);
      }
    }
#else
    if (function && line > 0)
      fprintf(stderr, "%s:%d ", function, line);
    else if (function)
      fprintf(stderr, "%s: ", function);
    else if (line > 0)
      fprintf(stderr, "%d: ", line);
    vfprintf(stderr, fmt, args);
    fflush(stderr);
#endif
  }
  va_end(args);
}

/* Dump a key in ascii or hexadecimal. */
const char *mdbx_dump_val(const MDBX_val *key, char *const buf,
                          const size_t bufsize) {
  if (!key)
    return "<null>";
  if (!buf || bufsize < 4)
    return nullptr;
  if (!key->iov_len)
    return "<empty>";

  const uint8_t *const data = key->iov_base;
  bool is_ascii = true;
  unsigned i;
  for (i = 0; is_ascii && i < key->iov_len; i++)
    if (data[i] < ' ' || data[i] > 127)
      is_ascii = false;

  if (is_ascii) {
    int len =
        snprintf(buf, bufsize, "%.*s",
                 (key->iov_len > INT_MAX) ? INT_MAX : (int)key->iov_len, data);
    assert(len > 0 && (unsigned)len < bufsize);
    (void)len;
  } else {
    char *const detent = buf + bufsize - 2;
    char *ptr = buf;
    *ptr++ = '<';
    for (i = 0; i < key->iov_len; i++) {
      const ptrdiff_t left = detent - ptr;
      assert(left > 0);
      int len = snprintf(ptr, left, "%02x", data[i]);
      if (len < 0 || len >= left)
        break;
      ptr += len;
    }
    if (ptr < detent) {
      ptr[0] = '>';
      ptr[1] = '\0';
    }
  }
  return buf;
}

/*------------------------------------------------------------------------------
 LY: debug stuff */

static const char *mdbx_leafnode_type(MDBX_node *n) {
  static const char *const tp[2][2] = {{"", ": DB"},
                                       {": sub-page", ": sub-DB"}};
  return F_ISSET(node_flags(n), F_BIGDATA)
             ? ": overflow page"
             : tp[F_ISSET(node_flags(n), F_DUPDATA)]
                 [F_ISSET(node_flags(n), F_SUBDATA)];
}

/* Display all the keys in the page. */
static __maybe_unused void mdbx_page_list(MDBX_page *mp) {
  pgno_t pgno = mp->mp_pgno;
  const char *type, *state = IS_DIRTY(mp) ? ", dirty" : "";
  MDBX_node *node;
  unsigned i, nkeys, nsize, total = 0;
  MDBX_val key;
  DKBUF;

  switch (mp->mp_flags &
          (P_BRANCH | P_LEAF | P_LEAF2 | P_META | P_OVERFLOW | P_SUBP)) {
  case P_BRANCH:
    type = "Branch page";
    break;
  case P_LEAF:
    type = "Leaf page";
    break;
  case P_LEAF | P_SUBP:
    type = "Leaf sub-page";
    break;
  case P_LEAF | P_LEAF2:
    type = "Leaf2 page";
    break;
  case P_LEAF | P_LEAF2 | P_SUBP:
    type = "Leaf2 sub-page";
    break;
  case P_OVERFLOW:
    mdbx_verbose("Overflow page %" PRIaPGNO " pages %u%s\n", pgno, mp->mp_pages,
                 state);
    return;
  case P_META:
    mdbx_verbose("Meta-page %" PRIaPGNO " txnid %" PRIu64 "\n", pgno,
                 page_meta(mp)->mm_txnid_a.inconsistent);
    return;
  default:
    mdbx_verbose("Bad page %" PRIaPGNO " flags 0x%X\n", pgno, mp->mp_flags);
    return;
  }

  nkeys = page_numkeys(mp);
  mdbx_verbose("%s %" PRIaPGNO " numkeys %u%s\n", type, pgno, nkeys, state);

  for (i = 0; i < nkeys; i++) {
    if (IS_LEAF2(mp)) { /* LEAF2 pages have no mp_ptrs[] or node headers */
      key.iov_len = nsize = mp->mp_leaf2_ksize;
      key.iov_base = page_leaf2key(mp, i, nsize);
      total += nsize;
      mdbx_verbose("key %u: nsize %u, %s\n", i, nsize, DKEY(&key));
      continue;
    }
    node = page_node(mp, i);
    key.iov_len = node_ks(node);
    key.iov_base = node->mn_data;
    nsize = (unsigned)(NODESIZE + key.iov_len);
    if (IS_BRANCH(mp)) {
      mdbx_verbose("key %u: page %" PRIaPGNO ", %s\n", i, node_pgno(node),
                   DKEY(&key));
      total += nsize;
    } else {
      if (F_ISSET(node_flags(node), F_BIGDATA))
        nsize += sizeof(pgno_t);
      else
        nsize += (unsigned)node_ds(node);
      total += nsize;
      nsize += sizeof(indx_t);
      mdbx_verbose("key %u: nsize %u, %s%s\n", i, nsize, DKEY(&key),
                   mdbx_leafnode_type(node));
    }
    total = EVEN(total);
  }
  mdbx_verbose("Total: header %u + contents %u + unused %u\n",
               IS_LEAF2(mp) ? PAGEHDRSZ : PAGEHDRSZ + mp->mp_lower, total,
               page_room(mp));
}

/*----------------------------------------------------------------------------*/

/* Check if there is an inited xcursor, so XCURSOR_REFRESH() is proper */
#define XCURSOR_INITED(mc)                                                     \
  ((mc)->mc_xcursor && ((mc)->mc_xcursor->mx_cursor.mc_flags & C_INITIALIZED))

/* Update sub-page pointer, if any, in mc->mc_xcursor.
 * Needed when the node which contains the sub-page may have moved.
 * Called with mp = mc->mc_pg[mc->mc_top], ki = mc->mc_ki[mc->mc_top]. */
#define XCURSOR_REFRESH(mc, mp, ki)                                            \
  do {                                                                         \
    MDBX_page *xr_pg = (mp);                                                   \
    MDBX_node *xr_node = page_node(xr_pg, ki);                                 \
    if ((node_flags(xr_node) & (F_DUPDATA | F_SUBDATA)) == F_DUPDATA)          \
      (mc)->mc_xcursor->mx_cursor.mc_pg[0] = node_data(xr_node);               \
  } while (0)

/* Perform act while tracking temporary cursor mn */
#define WITH_CURSOR_TRACKING(mn, act)                                          \
  do {                                                                         \
    mdbx_cassert(&(mn),                                                        \
                 mn.mc_txn->mt_cursors != NULL /* must be not rdonly txt */);  \
    MDBX_cursor mc_dummy, **tp = &(mn).mc_txn->mt_cursors[mn.mc_dbi];          \
    MDBX_cursor *tracked = &(mn);                                              \
    if ((mn).mc_flags & C_SUB) {                                               \
      mc_dummy.mc_flags = C_INITIALIZED;                                       \
      mc_dummy.mc_xcursor = (MDBX_xcursor *)&(mn);                             \
      tracked = &mc_dummy;                                                     \
    }                                                                          \
    tracked->mc_next = *tp;                                                    \
    *tp = tracked;                                                             \
    { act; }                                                                   \
    *tp = tracked->mc_next;                                                    \
  } while (0)

int mdbx_cmp(MDBX_txn *txn, MDBX_dbi dbi, const MDBX_val *a,
             const MDBX_val *b) {
  mdbx_assert(NULL, txn->mt_signature == MDBX_MT_SIGNATURE);
  return txn->mt_dbxs[dbi].md_cmp(a, b);
}

int mdbx_dcmp(MDBX_txn *txn, MDBX_dbi dbi, const MDBX_val *a,
              const MDBX_val *b) {
  mdbx_assert(NULL, txn->mt_signature == MDBX_MT_SIGNATURE);
  return txn->mt_dbxs[dbi].md_dcmp(a, b);
}

/* Allocate memory for a page.
 * Re-use old malloc'd pages first for singletons, otherwise just malloc.
 * Set MDBX_TXN_ERROR on failure. */
static MDBX_page *mdbx_page_malloc(MDBX_txn *txn, unsigned num) {
  MDBX_env *env = txn->mt_env;
  MDBX_page *np = env->me_dpages;
  size_t size = env->me_psize;
  if (likely(num == 1 && np)) {
    ASAN_UNPOISON_MEMORY_REGION(np, size);
    VALGRIND_MEMPOOL_ALLOC(env, np, size);
    VALGRIND_MAKE_MEM_DEFINED(&np->mp_next, sizeof(np->mp_next));
    env->me_dpages = np->mp_next;
  } else {
    size = pgno2bytes(env, num);
    np = mdbx_malloc(size);
    if (unlikely(!np)) {
      txn->mt_flags |= MDBX_TXN_ERROR;
      return np;
    }
    VALGRIND_MEMPOOL_ALLOC(env, np, size);
  }

  if ((env->me_flags & MDBX_NOMEMINIT) == 0) {
    /* For a single page alloc, we init everything after the page header.
     * For multi-page, we init the final page; if the caller needed that
     * many pages they will be filling in at least up to the last page. */
    size_t skip = PAGEHDRSZ;
    if (num > 1)
      skip += pgno2bytes(env, num - 1);
    memset((char *)np + skip, 0, size - skip);
  }
#if MDBX_DEBUG
  np->mp_pgno = 0;
#endif
  VALGRIND_MAKE_MEM_UNDEFINED(np, size);
  np->mp_flags = 0;
  np->mp_pages = num;
  return np;
}

/* Free a dirty page */
static void mdbx_dpage_free(MDBX_env *env, MDBX_page *dp, unsigned pages) {
#if MDBX_DEBUG
  dp->mp_pgno = MAX_PAGENO + 1;
#endif
  if (pages == 1) {
    dp->mp_next = env->me_dpages;
    VALGRIND_MEMPOOL_FREE(env, dp);
    env->me_dpages = dp;
  } else {
    /* large pages just get freed directly */
    VALGRIND_MEMPOOL_FREE(env, dp);
    mdbx_free(dp);
  }
}

/* Return all dirty pages to dpage list */
static void mdbx_dlist_free(MDBX_txn *txn) {
  MDBX_env *env = txn->mt_env;
  const MDBX_DPL dl = txn->tw.dirtylist;
  const size_t n = dl->length;

  for (size_t i = 1; i <= n; i++) {
    MDBX_page *dp = dl[i].ptr;
    mdbx_dpage_free(env, dp, IS_OVERFLOW(dp) ? dp->mp_pages : 1);
  }

  mdbx_dpl_clear(dl);
}

static __inline MDBX_db *mdbx_outer_db(MDBX_cursor *mc) {
  mdbx_cassert(mc, (mc->mc_flags & C_SUB) != 0);
  MDBX_xcursor *mx = container_of(mc->mc_db, MDBX_xcursor, mx_db);
  MDBX_cursor_couple *couple = container_of(mx, MDBX_cursor_couple, inner);
  mdbx_cassert(mc, mc->mc_db == &couple->outer.mc_xcursor->mx_db);
  mdbx_cassert(mc, mc->mc_dbx == &couple->outer.mc_xcursor->mx_dbx);
  return couple->outer.mc_db;
}

static __cold __maybe_unused bool mdbx_dirtylist_check(MDBX_txn *txn) {
  unsigned loose = 0;
  for (unsigned i = txn->tw.dirtylist->length; i > 0; --i) {
    const MDBX_page *const dp = txn->tw.dirtylist[i].ptr;
    if (!dp)
      continue;
    mdbx_tassert(txn, dp->mp_pgno == txn->tw.dirtylist[i].pgno);
    if (unlikely(dp->mp_pgno != txn->tw.dirtylist[i].pgno))
      return false;

    mdbx_tassert(txn, dp->mp_flags & P_DIRTY);
    if (unlikely((dp->mp_flags & P_DIRTY) == 0))
      return false;
    if (dp->mp_flags & P_LOOSE) {
      mdbx_tassert(txn, dp->mp_flags == (P_LOOSE | P_DIRTY));
      if (unlikely(dp->mp_flags != (P_LOOSE | P_DIRTY)))
        return false;
      loose += 1;
    }

    const unsigned num = IS_OVERFLOW(dp) ? dp->mp_pages : 1;
    mdbx_tassert(txn, txn->mt_next_pgno >= dp->mp_pgno + num);
    if (unlikely(txn->mt_next_pgno < dp->mp_pgno + num))
      return false;

    if (i < txn->tw.dirtylist->sorted) {
      mdbx_tassert(txn, txn->tw.dirtylist[i + 1].pgno >= dp->mp_pgno + num);
      if (unlikely(txn->tw.dirtylist[i + 1].pgno < dp->mp_pgno + num))
        return false;
    }

    const unsigned rpa = mdbx_pnl_search(txn->tw.reclaimed_pglist, dp->mp_pgno);
    mdbx_tassert(txn, rpa > MDBX_PNL_SIZE(txn->tw.reclaimed_pglist) ||
                          txn->tw.reclaimed_pglist[rpa] != dp->mp_pgno);
    if (rpa <= MDBX_PNL_SIZE(txn->tw.reclaimed_pglist) &&
        unlikely(txn->tw.reclaimed_pglist[rpa] == dp->mp_pgno))
      return false;
    if (num > 1) {
      const unsigned rpb =
          mdbx_pnl_search(txn->tw.reclaimed_pglist, dp->mp_pgno + num - 1);
      mdbx_tassert(txn, rpa == rpb);
      if (unlikely(rpa != rpb))
        return false;
    }
  }

  mdbx_tassert(txn, loose == txn->tw.loose_count);
  if (unlikely(loose != txn->tw.loose_count))
    return false;

  if (txn->tw.dirtylist->length - txn->tw.dirtylist->sorted <
      SORT_THRESHOLD / 2) {
    for (unsigned i = 1; i <= MDBX_PNL_SIZE(txn->tw.retired_pages); ++i) {
      const MDBX_page *const dp =
          mdbx_dpl_find(txn->tw.dirtylist, txn->tw.retired_pages[i]);
      mdbx_tassert(txn, !dp);
      if (unlikely(dp))
        return false;
    }
  }

  return true;
}

static void mdbx_refund_reclaimed(MDBX_txn *txn) {
  /* Scanning in descend order */
  pgno_t next_pgno = txn->mt_next_pgno;
  const MDBX_PNL pnl = txn->tw.reclaimed_pglist;
  mdbx_tassert(txn, MDBX_PNL_SIZE(pnl) && MDBX_PNL_MOST(pnl) == next_pgno - 1);
#if MDBX_PNL_ASCENDING
  unsigned i = MDBX_PNL_SIZE(pnl);
  mdbx_tassert(txn, pnl[i] == next_pgno - 1);
  while (--next_pgno, --i > 0 && pnl[i] == next_pgno - 1)
    ;
  MDBX_PNL_SIZE(pnl) = i;
#else
  unsigned i = 1;
  mdbx_tassert(txn, pnl[i] == next_pgno - 1);
  unsigned len = MDBX_PNL_SIZE(pnl);
  while (--next_pgno, ++i <= len && pnl[i] == next_pgno - 1)
    ;
  MDBX_PNL_SIZE(pnl) = len -= i - 1;
  for (unsigned move = 0; move < len; ++move)
    pnl[1 + move] = pnl[i + move];
#endif
  mdbx_verbose("refunded %" PRIaPGNO " pages: %" PRIaPGNO " -> %" PRIaPGNO,
               txn->mt_next_pgno - next_pgno, txn->mt_next_pgno, next_pgno);
  txn->mt_next_pgno = next_pgno;
  mdbx_tassert(
      txn, mdbx_pnl_check4assert(txn->tw.reclaimed_pglist, txn->mt_next_pgno));
}

static void mdbx_refund_loose(MDBX_txn *txn) {
  mdbx_tassert(txn, mdbx_dirtylist_check(txn));
  mdbx_tassert(txn, txn->tw.loose_pages != nullptr);
  mdbx_tassert(txn, txn->tw.loose_count > 0);

  const MDBX_DPL dl = txn->tw.dirtylist;
  mdbx_tassert(txn, dl->length >= txn->tw.loose_count);
  mdbx_tassert(txn, txn->tw.spill_pages == nullptr ||
                        dl->length >= MDBX_PNL_SIZE(txn->tw.spill_pages));

  pgno_t onstack[MDBX_CACHELINE_SIZE * 8 / sizeof(pgno_t)];
  MDBX_PNL suitable = onstack;

  if (dl->length - dl->sorted > txn->tw.loose_count) {
    /* Dirty list is useless since unsorted. */
    if (bytes2pnl(sizeof(onstack)) < txn->tw.loose_count) {
      suitable = mdbx_pnl_alloc(txn->tw.loose_count);
      if (unlikely(!suitable))
        return /* this is not a reason for transaction fail */;
    }

    /* Collect loose-pages which may be refunded. */
    mdbx_tassert(txn, txn->mt_next_pgno >= MIN_PAGENO + txn->tw.loose_count);
    pgno_t most = MIN_PAGENO;
    unsigned w = 0;
    for (const MDBX_page *dp = txn->tw.loose_pages; dp; dp = dp->mp_next) {
      mdbx_tassert(txn, dp->mp_flags == (P_LOOSE | P_DIRTY));
      mdbx_tassert(txn, txn->mt_next_pgno > dp->mp_pgno);
      if (likely(txn->mt_next_pgno - txn->tw.loose_count <= dp->mp_pgno)) {
        mdbx_tassert(txn,
                     w < ((suitable == onstack) ? bytes2pnl(sizeof(onstack))
                                                : MDBX_PNL_ALLOCLEN(suitable)));
        suitable[++w] = dp->mp_pgno;
        most = (dp->mp_pgno > most) ? dp->mp_pgno : most;
      }
    }

    if (most + 1 == txn->mt_next_pgno) {
      /* Sort suitable list and refund pages at the tail. */
      MDBX_PNL_SIZE(suitable) = w;
      mdbx_pnl_sort(suitable);

      /* Scanning in descend order */
      const int step = MDBX_PNL_ASCENDING ? -1 : 1;
      const int begin = MDBX_PNL_ASCENDING ? MDBX_PNL_SIZE(suitable) : 1;
      const int end = MDBX_PNL_ASCENDING ? 0 : MDBX_PNL_SIZE(suitable) + 1;
      mdbx_tassert(txn, suitable[begin] >= suitable[end - step]);
      mdbx_tassert(txn, most == suitable[begin]);

      for (int i = begin + step; i != end; i += step) {
        if (suitable[i] != most - 1)
          break;
        most -= 1;
      }
      const unsigned refunded = txn->mt_next_pgno - most;
      mdbx_verbose("refund-sorted %u pages %" PRIaPGNO " -> %" PRIaPGNO,
                   refunded, most, txn->mt_next_pgno);
      txn->tw.loose_count -= refunded;
      txn->tw.dirtyroom += refunded;
      txn->mt_next_pgno = most;

      /* Filter-out dirty list */
      unsigned r = 0;
      w = 0;
      if (dl->sorted) {
        do {
          if (dl[++r].pgno < most) {
            if (++w != r)
              dl[w] = dl[r];
          }
        } while (r < dl->sorted);
        dl->sorted = w;
      }
      while (r < dl->length) {
        if (dl[++r].pgno < most) {
          if (++w != r)
            dl[w] = dl[r];
        }
      }
      dl->length = w;
      mdbx_tassert(txn, txn->mt_parent ||
                            txn->tw.dirtyroom + txn->tw.dirtylist->length ==
                                MDBX_DPL_TXNFULL);
      goto unlink_loose;
    }
  } else {
    /* Dirtylist is mostly sorted, just refund loose pages at the end. */
    mdbx_dpl_sort(dl);
    mdbx_tassert(txn, dl->length < 2 || dl[1].pgno < dl[dl->length].pgno);
    mdbx_tassert(txn, dl->sorted == dl->length);

    /* Scan dirtylist tail-forward and cutoff suitable pages. */
    while (dl->length && dl[dl->length].pgno == txn->mt_next_pgno - 1 &&
           dl[dl->length].ptr->mp_flags == (P_LOOSE | P_DIRTY)) {
      MDBX_page *dp = dl[dl->length].ptr;
      mdbx_verbose("refund-unsorted page %" PRIaPGNO, dp->mp_pgno);
      mdbx_tassert(txn, dp->mp_pgno == dl[dl->length].pgno);
      dl->length -= 1;
    }

    if (dl->sorted != dl->length) {
      const unsigned refunded = dl->sorted - dl->length;
      dl->sorted = dl->length;
      txn->tw.loose_count -= refunded;
      txn->tw.dirtyroom += refunded;
      txn->mt_next_pgno -= refunded;
      mdbx_tassert(txn, txn->mt_parent ||
                            txn->tw.dirtyroom + txn->tw.dirtylist->length ==
                                MDBX_DPL_TXNFULL);

      /* Filter-out loose chain & dispose refunded pages. */
    unlink_loose:
      for (MDBX_page **link = &txn->tw.loose_pages; *link;) {
        MDBX_page *dp = *link;
        mdbx_tassert(txn, dp->mp_flags == (P_LOOSE | P_DIRTY));
        if (txn->mt_next_pgno > dp->mp_pgno) {
          link = &dp->mp_next;
        } else {
          *link = dp->mp_next;
          if ((txn->mt_flags & MDBX_WRITEMAP) == 0)
            mdbx_dpage_free(txn->mt_env, dp, 1);
        }
      }
    }
  }

  mdbx_tassert(txn, mdbx_dirtylist_check(txn));
  mdbx_tassert(txn, txn->mt_parent ||
                        txn->tw.dirtyroom + txn->tw.dirtylist->length ==
                            MDBX_DPL_TXNFULL);
  if (suitable != onstack)
    mdbx_pnl_free(suitable);
  txn->tw.loose_refund_wl = txn->mt_next_pgno;
}

static bool mdbx_refund(MDBX_txn *txn) {
  const pgno_t before = txn->mt_next_pgno;

  if (txn->tw.loose_pages && txn->tw.loose_refund_wl > txn->mt_next_pgno)
    mdbx_refund_loose(txn);

  while (true) {
    if (MDBX_PNL_SIZE(txn->tw.reclaimed_pglist) == 0 ||
        MDBX_PNL_MOST(txn->tw.reclaimed_pglist) != txn->mt_next_pgno - 1)
      break;

    mdbx_refund_reclaimed(txn);
    if (!txn->tw.loose_pages || txn->tw.loose_refund_wl <= txn->mt_next_pgno)
      break;

    const pgno_t memo = txn->mt_next_pgno;
    mdbx_refund_loose(txn);
    if (memo == txn->mt_next_pgno)
      break;
  }

  return before != txn->mt_next_pgno;
}

static __cold void mdbx_kill_page(MDBX_env *env, MDBX_page *mp, pgno_t pgno,
                                  unsigned npages) {
  mdbx_assert(env, pgno >= NUM_METAS && npages);
  if (IS_DIRTY(mp) || (env->me_flags & MDBX_WRITEMAP)) {
    const size_t bytes = pgno2bytes(env, npages);
    memset(mp, 0, bytes);
    mp->mp_pgno = pgno;
    if ((env->me_flags & MDBX_WRITEMAP) == 0)
      mdbx_pwrite(env->me_fd, mp, bytes, pgno2bytes(env, pgno));
  } else {
    struct iovec iov[MDBX_COMMIT_PAGES];
    iov[0].iov_len = env->me_psize;
    iov[0].iov_base = (char *)env->me_pbuf + env->me_psize;
    size_t iov_off = pgno2bytes(env, pgno);
    unsigned n = 1;
    while (--npages) {
      iov[n] = iov[0];
      if (++n == MDBX_COMMIT_PAGES) {
        mdbx_pwritev(env->me_fd, iov, MDBX_COMMIT_PAGES, iov_off,
                     pgno2bytes(env, MDBX_COMMIT_PAGES));
        iov_off += pgno2bytes(env, MDBX_COMMIT_PAGES);
        n = 0;
      }
    }
    mdbx_pwritev(env->me_fd, iov, n, iov_off, pgno2bytes(env, n));
  }
}

/* Retire, loosen or free a single page.
 *
 * Saves single pages to a list for future reuse
 * in this same txn. It has been pulled from the GC
 * and already resides on the dirty list, but has been
 * deleted. Use these pages first before pulling again
 * from the GC.
 *
 * If the page wasn't dirtied in this txn, just add it
 * to this txn's free list. */

static __hot int mdbx_page_loose(MDBX_txn *txn, MDBX_page *mp) {
  const unsigned npages = IS_OVERFLOW(mp) ? mp->mp_pages : 1;
  const pgno_t pgno = mp->mp_pgno;

  if (txn->mt_parent) {
    mdbx_tassert(txn, (txn->mt_env->me_flags & MDBX_WRITEMAP) == 0);
    mdbx_tassert(txn, mp != pgno2page(txn->mt_env, pgno));
    /* If txn has a parent, make sure the page is in our dirty list. */
    MDBX_page *dp = mdbx_dpl_find(txn->tw.dirtylist, pgno);
    /* TODO: use extended flag-mask to track parent's dirty-pages */
    if (dp == nullptr) {
      mp->mp_next = txn->tw.retired2parent_pages;
      txn->tw.retired2parent_pages = mp;
      txn->tw.retired2parent_count += npages;
      return MDBX_SUCCESS;
    }
    if (unlikely(mp != dp)) { /* bad cursor? */
      mdbx_error(
          "wrong page 0x%p #%" PRIaPGNO " in the dirtylist, expecting %p",
          __Wpedantic_format_voidptr(dp), pgno, __Wpedantic_format_voidptr(mp));
      txn->mt_flags |= MDBX_TXN_ERROR;
      return MDBX_PROBLEM;
    }
    /* ok, it's ours */
  }

  mdbx_debug("loosen page %" PRIaPGNO, pgno);
  const bool is_dirty = IS_DIRTY(mp);
  if (MDBX_DEBUG || unlikely((txn->mt_env->me_flags & MDBX_PAGEPERTURB) != 0)) {
    mdbx_kill_page(txn->mt_env, mp, pgno, npages);
    VALGRIND_MAKE_MEM_UNDEFINED(mp, PAGEHDRSZ);
  }
  VALGRIND_MAKE_MEM_NOACCESS(page_data(mp),
                             pgno2bytes(txn->mt_env, npages) - PAGEHDRSZ);
  ASAN_POISON_MEMORY_REGION(page_data(mp),
                            pgno2bytes(txn->mt_env, npages) - PAGEHDRSZ);

  if (unlikely(npages >
               1 /* overflow pages doesn't comes to the loose-list */)) {
    if (is_dirty) {
      /* Remove from dirty list */
      MDBX_page *dp = mdbx_dpl_remove(txn->tw.dirtylist, pgno);
      if (unlikely(dp != mp)) {
        mdbx_error("not found page 0x%p #%" PRIaPGNO " in the dirtylist",
                   __Wpedantic_format_voidptr(mp), pgno);
        txn->mt_flags |= MDBX_TXN_ERROR;
        return MDBX_PROBLEM;
      }
      txn->tw.dirtyroom++;
      mdbx_tassert(txn, txn->mt_parent ||
                            txn->tw.dirtyroom + txn->tw.dirtylist->length ==
                                MDBX_DPL_TXNFULL);
      if ((txn->mt_flags & MDBX_WRITEMAP) == 0)
        mdbx_dpage_free(txn->mt_env, mp, npages);
    }

    if (unlikely(pgno + npages == txn->mt_next_pgno)) {
      txn->mt_next_pgno = pgno;
      mdbx_refund(txn);
      return MDBX_SUCCESS;
    }

    int rc = mdbx_pnl_insert_range(&txn->tw.reclaimed_pglist, pgno, npages);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    mdbx_tassert(txn, mdbx_pnl_check4assert(txn->tw.reclaimed_pglist,
                                            txn->mt_next_pgno));
    return MDBX_SUCCESS;
  }

  mp->mp_flags = P_LOOSE | P_DIRTY;
  mp->mp_next = txn->tw.loose_pages;
  txn->tw.loose_pages = mp;
  txn->tw.loose_count++;
  if (unlikely(txn->mt_next_pgno == mp->mp_pgno + 1))
    mdbx_refund(txn);

  return MDBX_SUCCESS;
}

static __hot int mdbx_page_retire(MDBX_cursor *mc, MDBX_page *mp) {
  const unsigned npages = IS_OVERFLOW(mp) ? mp->mp_pages : 1;
  const pgno_t pgno = mp->mp_pgno;
  MDBX_txn *const txn = mc->mc_txn;

  if (unlikely(mc->mc_flags & C_SUB)) {
    MDBX_db *outer = mdbx_outer_db(mc);
    mdbx_cassert(mc, !IS_BRANCH(mp) || outer->md_branch_pages > 0);
    outer->md_branch_pages -= IS_BRANCH(mp);
    mdbx_cassert(mc, !IS_LEAF(mp) || outer->md_leaf_pages > 0);
    outer->md_leaf_pages -= IS_LEAF(mp);
    mdbx_cassert(mc, !IS_OVERFLOW(mp));
  }
  mdbx_cassert(mc, !IS_BRANCH(mp) || mc->mc_db->md_branch_pages > 0);
  mc->mc_db->md_branch_pages -= IS_BRANCH(mp);
  mdbx_cassert(mc, !IS_LEAF(mp) || mc->mc_db->md_leaf_pages > 0);
  mc->mc_db->md_leaf_pages -= IS_LEAF(mp);
  mdbx_cassert(mc, !IS_OVERFLOW(mp) || mc->mc_db->md_overflow_pages >= npages);
  mc->mc_db->md_overflow_pages -= IS_OVERFLOW(mp) ? npages : 0;

  if (IS_DIRTY(mp)) {
    int rc = mdbx_page_loose(txn, mp);
    if (unlikely(rc != MDBX_SUCCESS))
      mc->mc_flags &= ~(C_INITIALIZED | C_EOF);
    return rc;
  }

  if (txn->tw.spill_pages) {
    const unsigned i = mdbx_pnl_exist(txn->tw.spill_pages, pgno << 1);
    if (i) {
      /* This page is no longer spilled */
      mdbx_tassert(txn, i == MDBX_PNL_SIZE(txn->tw.spill_pages) ||
                            txn->tw.spill_pages[i + 1] >= (pgno + npages) << 1);
      txn->tw.spill_pages[i] |= 1;
      if (i == MDBX_PNL_SIZE(txn->tw.spill_pages))
        MDBX_PNL_SIZE(txn->tw.spill_pages) -= 1;
      int rc = mdbx_page_loose(txn, mp);
      if (unlikely(rc != MDBX_SUCCESS))
        mc->mc_flags &= ~(C_INITIALIZED | C_EOF);
      return rc;
    }
  }

  mdbx_tassert(txn, mp == pgno2page(txn->mt_env, pgno));
  int rc = mdbx_pnl_append_range(&txn->tw.retired_pages, pgno, npages);
  mdbx_tassert(txn, mdbx_dpl_find(txn->tw.dirtylist, pgno) == nullptr);
  return rc;
}

static __must_check_result __inline int mdbx_retire_pgno(MDBX_cursor *mc,
                                                         const pgno_t pgno) {
  MDBX_page *mp;
  int rc = mdbx_page_get(mc, pgno, &mp, NULL);
  if (likely(rc == MDBX_SUCCESS))
    rc = mdbx_page_retire(mc, mp);
  return rc;
}

/* Set or clear P_KEEP in dirty, non-overflow, non-sub pages watched by txn.
 *
 * [in] mc      A cursor handle for the current operation.
 * [in] pflags  Flags of the pages to update:
 *                - P_DIRTY to set P_KEEP,
 *                - P_DIRTY|P_KEEP to clear it.
 * [in] all     No shortcuts. Needed except after a full mdbx_page_flush().
 *
 * Returns 0 on success, non-zero on failure. */
static int mdbx_pages_xkeep(MDBX_cursor *mc, unsigned pflags, bool all) {
  const unsigned Mask = P_SUBP | P_DIRTY | P_LOOSE | P_KEEP;
  MDBX_txn *txn = mc->mc_txn;
  MDBX_cursor *m3, *m0 = mc;
  MDBX_xcursor *mx;
  MDBX_page *dp, *mp;
  unsigned i, j;
  int rc = MDBX_SUCCESS;

  /* Mark pages seen by cursors: First m0, then tracked cursors */
  for (i = txn->mt_numdbs;;) {
    if (mc->mc_flags & C_INITIALIZED) {
      for (m3 = mc;; m3 = &mx->mx_cursor) {
        mp = NULL;
        for (j = 0; j < m3->mc_snum; j++) {
          mp = m3->mc_pg[j];
          if ((mp->mp_flags & Mask) == pflags)
            mp->mp_flags ^= P_KEEP;
        }
        mx = m3->mc_xcursor;
        /* Proceed to mx if it is at a sub-database */
        if (!(mx && (mx->mx_cursor.mc_flags & C_INITIALIZED)))
          break;
        if (!(mp && IS_LEAF(mp)))
          break;
        if (!(node_flags(page_node(mp, m3->mc_ki[j - 1])) & F_SUBDATA))
          break;
      }
    }
    mc = mc->mc_next;
    for (; !mc || mc == m0; mc = txn->mt_cursors[--i])
      if (i == 0)
        goto mark_done;
  }

mark_done:
  if (all) {
    /* Mark dirty root pages */
    for (i = 0; i < txn->mt_numdbs; i++) {
      if (txn->mt_dbflags[i] & DB_DIRTY) {
        pgno_t pgno = txn->mt_dbs[i].md_root;
        if (pgno == P_INVALID)
          continue;
        int level;
        if (unlikely((rc = mdbx_page_get(m0, pgno, &dp, &level)) !=
                     MDBX_SUCCESS))
          break;
        if ((dp->mp_flags & Mask) == pflags && level <= 1)
          dp->mp_flags ^= P_KEEP;
      }
    }
  }

  return rc;
}

/* Spill pages from the dirty list back to disk.
 * This is intended to prevent running into MDBX_TXN_FULL situations,
 * but note that they may still occur in a few cases:
 *
 * 1) our estimate of the txn size could be too small. Currently this
 *  seems unlikely, except with a large number of MDBX_MULTIPLE items.
 *
 * 2) child txns may run out of space if their parents dirtied a
 *  lot of pages and never spilled them. TODO: we probably should do
 *  a preemptive spill during mdbx_txn_begin() of a child txn, if
 *  the parent's dirtyroom is below a given threshold.
 *
 * Otherwise, if not using nested txns, it is expected that apps will
 * not run into MDBX_TXN_FULL any more. The pages are flushed to disk
 * the same way as for a txn commit, e.g. their P_DIRTY flag is cleared.
 * If the txn never references them again, they can be left alone.
 * If the txn only reads them, they can be used without any fuss.
 * If the txn writes them again, they can be dirtied immediately without
 * going thru all of the work of mdbx_page_touch(). Such references are
 * handled by mdbx_page_unspill().
 *
 * Also note, we never spill DB root pages, nor pages of active cursors,
 * because we'll need these back again soon anyway. And in nested txns,
 * we can't spill a page in a child txn if it was already spilled in a
 * parent txn. That would alter the parent txns' data even though
 * the child hasn't committed yet, and we'd have no way to undo it if
 * the child aborted.
 *
 * [in] mc    cursor A cursor handle identifying the transaction and
 *            database for which we are checking space.
 * [in] key   For a put operation, the key being stored.
 * [in] data  For a put operation, the data being stored.
 *
 * Returns 0 on success, non-zero on failure. */
static int mdbx_page_spill(MDBX_cursor *mc, MDBX_val *key, MDBX_val *data) {
  if (mc->mc_flags & C_SUB)
    return MDBX_SUCCESS;

  MDBX_txn *txn = mc->mc_txn;
  MDBX_DPL dl = txn->tw.dirtylist;

  /* Estimate how much space this op will take */
  pgno_t i = mc->mc_db->md_depth;
  /* Named DBs also dirty the main DB */
  if (mc->mc_dbi >= CORE_DBS)
    i += txn->mt_dbs[MAIN_DBI].md_depth;
  /* For puts, roughly factor in the key+data size */
  if (key)
    i += bytes2pgno(txn->mt_env, node_size(key, data) + txn->mt_env->me_psize);
  i += i; /* double it for good measure */
  pgno_t need = i;

  if (txn->tw.dirtyroom > i)
    return MDBX_SUCCESS;

  if (!txn->tw.spill_pages) {
    txn->tw.spill_pages = mdbx_pnl_alloc(MDBX_DPL_TXNFULL / 8);
    if (unlikely(!txn->tw.spill_pages))
      return MDBX_ENOMEM;
  } else {
    /* purge deleted slots */
    MDBX_PNL sl = txn->tw.spill_pages;
    pgno_t num = MDBX_PNL_SIZE(sl), j = 0;
    for (i = 1; i <= num; i++) {
      if ((sl[i] & 1) == 0)
        sl[++j] = sl[i];
    }
    MDBX_PNL_SIZE(sl) = j;
  }

  /* Preserve pages which may soon be dirtied again */
  int rc = mdbx_pages_xkeep(mc, P_DIRTY, true);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout;

  /* Less aggressive spill - we originally spilled the entire dirty list,
   * with a few exceptions for cursor pages and DB root pages. But this
   * turns out to be a lot of wasted effort because in a large txn many
   * of those pages will need to be used again. So now we spill only 1/8th
   * of the dirty pages. Testing revealed this to be a good tradeoff,
   * better than 1/2, 1/4, or 1/10. */
  if (need < MDBX_DPL_TXNFULL / 8)
    need = MDBX_DPL_TXNFULL / 8;

  /* Save the page IDs of all the pages we're flushing */
  /* flush from the tail forward, this saves a lot of shifting later on. */
  for (i = dl->length; i && need; i--) {
    pgno_t pn = dl[i].pgno << 1;
    MDBX_page *dp = dl[i].ptr;
    if (dp->mp_flags & (P_LOOSE | P_KEEP))
      continue;
    /* Can't spill twice,
     * make sure it's not already in a parent's spill list. */
    if (txn->mt_parent) {
      MDBX_txn *parent;
      for (parent = txn->mt_parent; parent; parent = parent->mt_parent) {
        if (parent->tw.spill_pages &&
            mdbx_pnl_exist(parent->tw.spill_pages, pn)) {
          dp->mp_flags |= P_KEEP;
          break;
        }
      }
      if (parent)
        continue;
    }
    rc = mdbx_pnl_append(&txn->tw.spill_pages, pn);
    if (unlikely(rc != MDBX_SUCCESS))
      goto bailout;
    need--;
  }
  mdbx_pnl_sort(txn->tw.spill_pages);

  /* Flush the spilled part of dirty list */
  rc = mdbx_page_flush(txn, i);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout;

  /* Reset any dirty pages we kept that page_flush didn't see */
  rc = mdbx_pages_xkeep(mc, P_DIRTY | P_KEEP, i != 0);

bailout:
  txn->mt_flags |= rc ? MDBX_TXN_ERROR : MDBX_TXN_SPILLS;
  return rc;
}

/*----------------------------------------------------------------------------*/

#define METAPAGE(env, n) page_meta(pgno2page(env, n))
#define METAPAGE_END(env) METAPAGE(env, NUM_METAS)

static __inline txnid_t meta_txnid(const MDBX_env *env, const MDBX_meta *meta,
                                   bool allow_volatile) {
  mdbx_assert(env, meta >= METAPAGE(env, 0) || meta < METAPAGE_END(env));
  txnid_t a = safe64_read(&meta->mm_txnid_a);
  txnid_t b = safe64_read(&meta->mm_txnid_b);
  if (allow_volatile)
    return (a == b) ? a : 0;
  mdbx_assert(env, a == b);
  return a;
}

static __inline txnid_t mdbx_meta_txnid_stable(const MDBX_env *env,
                                               const MDBX_meta *meta) {
  return meta_txnid(env, meta, false);
}

static __inline txnid_t mdbx_meta_txnid_fluid(const MDBX_env *env,
                                              const MDBX_meta *meta) {
  return meta_txnid(env, meta, true);
}

static __inline void mdbx_meta_update_begin(const MDBX_env *env,
                                            MDBX_meta *meta, txnid_t txnid) {
  mdbx_assert(env, meta >= METAPAGE(env, 0) || meta < METAPAGE_END(env));
  mdbx_assert(env, meta->mm_txnid_a.inconsistent < txnid &&
                       meta->mm_txnid_b.inconsistent < txnid);
  (void)env;
  safe64_update(&meta->mm_txnid_a, txnid);
}

static __inline void mdbx_meta_update_end(const MDBX_env *env, MDBX_meta *meta,
                                          txnid_t txnid) {
  mdbx_assert(env, meta >= METAPAGE(env, 0) || meta < METAPAGE_END(env));
  mdbx_assert(env, meta->mm_txnid_a.inconsistent == txnid);
  mdbx_assert(env, meta->mm_txnid_b.inconsistent < txnid);
  (void)env;
  mdbx_jitter4testing(true);
  safe64_update(&meta->mm_txnid_b, txnid);
}

static __inline void mdbx_meta_set_txnid(const MDBX_env *env, MDBX_meta *meta,
                                         txnid_t txnid) {
  mdbx_assert(env, meta < METAPAGE(env, 0) || meta > METAPAGE_END(env));
  (void)env;
  /* update inconsistent since this function used ONLY for filling meta-image
   * for writing, but not the actual meta-page */
  meta->mm_txnid_a.inconsistent = txnid;
  meta->mm_txnid_b.inconsistent = txnid;
}

static __inline uint64_t mdbx_meta_sign(const MDBX_meta *meta) {
  uint64_t sign = MDBX_DATASIGN_NONE;
#if 0 /* TODO */
  sign = hippeus_hash64(...);
#else
  (void)meta;
#endif
  /* LY: newer returns MDBX_DATASIGN_NONE or MDBX_DATASIGN_WEAK */
  return (sign > MDBX_DATASIGN_WEAK) ? sign : ~sign;
}

enum meta_choise_mode { prefer_last, prefer_noweak, prefer_steady };

static __inline bool mdbx_meta_ot(const enum meta_choise_mode mode,
                                  const MDBX_env *env, const MDBX_meta *a,
                                  const MDBX_meta *b) {
  mdbx_jitter4testing(true);
  txnid_t txnid_a = mdbx_meta_txnid_fluid(env, a);
  txnid_t txnid_b = mdbx_meta_txnid_fluid(env, b);

  mdbx_jitter4testing(true);
  switch (mode) {
  default:
    assert(false);
    __unreachable();
    /* fall through */
    __fallthrough;
  case prefer_steady:
    if (META_IS_STEADY(a) != META_IS_STEADY(b))
      return META_IS_STEADY(b);
    /* fall through */
    __fallthrough;
  case prefer_noweak:
    if (META_IS_WEAK(a) != META_IS_WEAK(b))
      return !META_IS_WEAK(b);
    /* fall through */
    __fallthrough;
  case prefer_last:
    mdbx_jitter4testing(true);
    if (txnid_a == txnid_b)
      return META_IS_STEADY(b) || (META_IS_WEAK(a) && !META_IS_WEAK(b));
    return txnid_a < txnid_b;
  }
}

static __inline bool mdbx_meta_eq(const MDBX_env *env, const MDBX_meta *a,
                                  const MDBX_meta *b) {
  mdbx_jitter4testing(true);
  const txnid_t txnid = mdbx_meta_txnid_fluid(env, a);
  if (!txnid || txnid != mdbx_meta_txnid_fluid(env, b))
    return false;

  mdbx_jitter4testing(true);
  if (META_IS_STEADY(a) != META_IS_STEADY(b))
    return false;

  mdbx_jitter4testing(true);
  return true;
}

static int mdbx_meta_eq_mask(const MDBX_env *env) {
  MDBX_meta *m0 = METAPAGE(env, 0);
  MDBX_meta *m1 = METAPAGE(env, 1);
  MDBX_meta *m2 = METAPAGE(env, 2);

  int rc = mdbx_meta_eq(env, m0, m1) ? 1 : 0;
  if (mdbx_meta_eq(env, m1, m2))
    rc += 2;
  if (mdbx_meta_eq(env, m2, m0))
    rc += 4;
  return rc;
}

static __inline MDBX_meta *mdbx_meta_recent(const enum meta_choise_mode mode,
                                            const MDBX_env *env, MDBX_meta *a,
                                            MDBX_meta *b) {
  const bool a_older_that_b = mdbx_meta_ot(mode, env, a, b);
  mdbx_assert(env, !mdbx_meta_eq(env, a, b));
  return a_older_that_b ? b : a;
}

static __inline MDBX_meta *mdbx_meta_ancient(const enum meta_choise_mode mode,
                                             const MDBX_env *env, MDBX_meta *a,
                                             MDBX_meta *b) {
  const bool a_older_that_b = mdbx_meta_ot(mode, env, a, b);
  mdbx_assert(env, !mdbx_meta_eq(env, a, b));
  return a_older_that_b ? a : b;
}

static __inline MDBX_meta *
mdbx_meta_mostrecent(const enum meta_choise_mode mode, const MDBX_env *env) {
  MDBX_meta *m0 = METAPAGE(env, 0);
  MDBX_meta *m1 = METAPAGE(env, 1);
  MDBX_meta *m2 = METAPAGE(env, 2);

  MDBX_meta *head = mdbx_meta_recent(mode, env, m0, m1);
  head = mdbx_meta_recent(mode, env, head, m2);
  return head;
}

static __hot MDBX_meta *mdbx_meta_steady(const MDBX_env *env) {
  return mdbx_meta_mostrecent(prefer_steady, env);
}

static __hot MDBX_meta *mdbx_meta_head(const MDBX_env *env) {
  return mdbx_meta_mostrecent(prefer_last, env);
}

static __hot txnid_t mdbx_recent_committed_txnid(const MDBX_env *env) {
  while (true) {
    const MDBX_meta *head = mdbx_meta_head(env);
    const txnid_t recent = mdbx_meta_txnid_fluid(env, head);
    mdbx_compiler_barrier();
    if (likely(head == mdbx_meta_head(env) &&
               recent == mdbx_meta_txnid_fluid(env, head)))
      return recent;
  }
}

static __hot txnid_t mdbx_recent_steady_txnid(const MDBX_env *env) {
  while (true) {
    const MDBX_meta *head = mdbx_meta_steady(env);
    const txnid_t recent = mdbx_meta_txnid_fluid(env, head);
    mdbx_compiler_barrier();
    if (likely(head == mdbx_meta_steady(env) &&
               recent == mdbx_meta_txnid_fluid(env, head)))
      return recent;
  }
}

static __hot txnid_t mdbx_reclaiming_detent(const MDBX_env *env) {
  if (F_ISSET(env->me_flags, MDBX_UTTERLY_NOSYNC))
    return likely(env->me_txn0->mt_owner == mdbx_thread_self())
               ? env->me_txn0->mt_txnid - MDBX_TXNID_STEP
               : mdbx_recent_committed_txnid(env);

  return mdbx_recent_steady_txnid(env);
}

static const char *mdbx_durable_str(const MDBX_meta *const meta) {
  if (META_IS_WEAK(meta))
    return "Weak";
  if (META_IS_STEADY(meta))
    return (meta->mm_datasync_sign == mdbx_meta_sign(meta)) ? "Steady"
                                                            : "Tainted";
  return "Legacy";
}

/*----------------------------------------------------------------------------*/

/* Find oldest txnid still referenced. */
static txnid_t mdbx_find_oldest(MDBX_txn *txn) {
  mdbx_tassert(txn, (txn->mt_flags & MDBX_RDONLY) == 0);
  MDBX_env *env = txn->mt_env;
  const txnid_t edge = mdbx_reclaiming_detent(env);
  mdbx_tassert(txn, edge <= txn->mt_txnid);

  MDBX_lockinfo *const lck = env->me_lck;
  if (unlikely(lck == NULL /* exclusive mode */))
    return env->me_lckless_stub.oldest = edge;

  const txnid_t last_oldest = lck->mti_oldest_reader;
  mdbx_tassert(txn, edge >= last_oldest);
  if (likely(last_oldest == edge))
    return edge;

  const uint32_t nothing_changed = MDBX_STRING_TETRAD("None");
  const uint32_t snap_readers_refresh_flag = lck->mti_readers_refresh_flag;
  mdbx_jitter4testing(false);
  if (snap_readers_refresh_flag == nothing_changed)
    return last_oldest;

  txnid_t oldest = edge;
  lck->mti_readers_refresh_flag = nothing_changed;
  mdbx_flush_noncoherent_cpu_writeback();
  const unsigned snap_nreaders = lck->mti_numreaders;
  for (unsigned i = 0; i < snap_nreaders; ++i) {
    if (lck->mti_readers[i].mr_pid) {
      /* mdbx_jitter4testing(true); */
      const txnid_t snap = safe64_read(&lck->mti_readers[i].mr_txnid);
      if (oldest > snap && last_oldest <= /* ignore pending updates */ snap) {
        oldest = snap;
        if (oldest == last_oldest)
          return oldest;
      }
    }
  }

  if (oldest != last_oldest) {
    mdbx_notice("update oldest %" PRIaTXN " -> %" PRIaTXN, last_oldest, oldest);
    mdbx_tassert(txn, oldest >= lck->mti_oldest_reader);
    lck->mti_oldest_reader = oldest;
  }
  return oldest;
}

/* Find largest mvcc-snapshot still referenced. */
static __cold pgno_t mdbx_find_largest(MDBX_env *env, pgno_t largest) {
  MDBX_lockinfo *const lck = env->me_lck;
  if (likely(lck != NULL /* exclusive mode */)) {
    const unsigned snap_nreaders = lck->mti_numreaders;
    for (unsigned i = 0; i < snap_nreaders; ++i) {
    retry:
      if (lck->mti_readers[i].mr_pid) {
        /* mdbx_jitter4testing(true); */
        const pgno_t snap_pages = lck->mti_readers[i].mr_snapshot_pages_used;
        const txnid_t snap_txnid = safe64_read(&lck->mti_readers[i].mr_txnid);
        mdbx_memory_barrier();
        if (unlikely(snap_pages != lck->mti_readers[i].mr_snapshot_pages_used ||
                     snap_txnid != safe64_read(&lck->mti_readers[i].mr_txnid)))
          goto retry;
        if (largest < snap_pages &&
            lck->mti_oldest_reader <= /* ignore pending updates */ snap_txnid &&
            snap_txnid <= env->me_txn0->mt_txnid)
          largest = snap_pages;
      }
    }
  }

  return largest;
}

/* Add a page to the txn's dirty list */
static int __must_check_result mdbx_page_dirty(MDBX_txn *txn, MDBX_page *mp) {
  const int rc = mdbx_dpl_append(txn->tw.dirtylist, mp->mp_pgno, mp);
  if (unlikely(rc != MDBX_SUCCESS)) {
    txn->mt_flags |= MDBX_TXN_ERROR;
    return rc;
  }
  txn->tw.dirtyroom--;
  mdbx_tassert(txn, txn->mt_parent ||
                        txn->tw.dirtyroom + txn->tw.dirtylist->length ==
                            MDBX_DPL_TXNFULL);
  return MDBX_SUCCESS;
}

/* Turn on/off readahead. It's harmful when the DB is larger than RAM. */
static int __cold mdbx_set_readahead(MDBX_env *env, const size_t offset,
                                     const size_t length, const bool enable) {
  assert(length > 0);
  mdbx_notice("readahead %s %u..%u", enable ? "ON" : "OFF",
              bytes2pgno(env, offset), bytes2pgno(env, offset + length));

#if defined(F_RDAHEAD)
  if (unlikely(fcntl(env->me_fd, F_RDAHEAD, enable) == -1))
    return errno;
#endif /* F_RDAHEAD */

  if (enable) {
#if defined(F_RDADVISE)
    struct radvisory hint;
    hint.ra_offset = offset;
    hint.ra_count = length;
    (void)/* Ignore ENOTTY for DB on the ram-disk and so on */ fcntl(
        env->me_fd, F_RDADVISE, &hint);
#endif /* F_RDADVISE */
#if defined(MADV_WILLNEED)
    if (unlikely(madvise(env->me_map + offset, length, MADV_WILLNEED) != 0))
      return errno;
#elif defined(POSIX_MADV_WILLNEED)
    rc = posix_madvise(env->me_map + offset, length, POSIX_MADV_WILLNEED);
    if (unlikely(rc != 0))
      return errno;
#elif defined(_WIN32) || defined(_WIN64)
    if (mdbx_PrefetchVirtualMemory) {
      WIN32_MEMORY_RANGE_ENTRY hint;
      hint.VirtualAddress = env->me_map + offset;
      hint.NumberOfBytes = length;
      (void)mdbx_PrefetchVirtualMemory(GetCurrentProcess(), 1, &hint, 0);
    }
#elif defined(POSIX_FADV_WILLNEED)
    int err = posix_fadvise(env->me_fd, offset, length, POSIX_FADV_WILLNEED);
    if (unlikely(err != 0))
      return err;
#endif /* MADV_WILLNEED */
  } else {
#if defined(MADV_RANDOM)
    if (unlikely(madvise(env->me_map + offset, length, MADV_RANDOM) != 0))
      return errno;
#elif defined(POSIX_MADV_RANDOM)
    int err = posix_madvise(env->me_map + offset, length, POSIX_MADV_RANDOM);
    if (unlikely(err != 0))
      return err;
#elif defined(POSIX_FADV_RANDOM)
    int err = posix_fadvise(env->me_fd, offset, length, POSIX_FADV_RANDOM);
    if (unlikely(err != 0))
      return err;
#endif /* MADV_RANDOM */
  }
  return MDBX_SUCCESS;
}

static __cold int mdbx_mapresize(MDBX_env *env, const pgno_t size_pgno,
                                 const pgno_t limit_pgno) {
  const size_t limit_bytes = pgno_align2os_bytes(env, limit_pgno);
  const size_t size_bytes = pgno_align2os_bytes(env, size_pgno);

  mdbx_verbose("resize datafile/mapping: "
               "present %" PRIuPTR " -> %" PRIuPTR ", "
               "limit %" PRIuPTR " -> %" PRIuPTR,
               env->me_dxb_mmap.current, size_bytes, env->me_dxb_mmap.limit,
               limit_bytes);

  mdbx_assert(env, limit_bytes >= size_bytes);
  mdbx_assert(env, bytes2pgno(env, size_bytes) >= size_pgno);
  mdbx_assert(env, bytes2pgno(env, limit_bytes) >= limit_pgno);

#if defined(_WIN32) || defined(_WIN64)
  /* Acquire guard in exclusive mode for:
   *   - to avoid collision between read and write txns around env->me_dbgeo;
   *   - to avoid attachment of new reading threads (see mdbx_rdt_lock); */
  mdbx_srwlock_AcquireExclusive(&env->me_remap_guard);
  mdbx_handle_array_t *suspended = NULL;
  mdbx_handle_array_t array_onstack;
  int rc = MDBX_SUCCESS;
  if (limit_bytes == env->me_dxb_mmap.limit &&
      size_bytes == env->me_dxb_mmap.current &&
      size_bytes == env->me_dxb_mmap.filesize)
    goto bailout;

  /* 1) Windows allows only extending a read-write section, but not a
   *    corresponing mapped view. Therefore in other cases we must suspend
   *    the local threads for safe remap.
   * 2) At least on Windows 10 1803 the entire mapped section is unavailable
   *    for short time during NtExtendSection() or VirtualAlloc() execution.
   *
   * THEREFORE LOCAL THREADS SUSPENDING IS ALWAYS REQUIRED! */
  array_onstack.limit = ARRAY_LENGTH(array_onstack.handles);
  array_onstack.count = 0;
  suspended = &array_onstack;
  rc = mdbx_suspend_threads_before_remap(env, &suspended);
  if (rc != MDBX_SUCCESS) {
    mdbx_error("failed suspend-for-remap: errcode %d", rc);
    goto bailout;
  }
#else
  /* Acquire guard to avoid collision between read and write txns
   * around env->me_dbgeo */
  int rc = mdbx_fastmutex_acquire(&env->me_remap_guard);
  if (rc != MDBX_SUCCESS)
    return rc;
  if (limit_bytes == env->me_dxb_mmap.limit &&
      size_bytes == env->me_dxb_mmap.current)
    goto bailout;
#endif /* Windows */

  const size_t prev_limit = env->me_dxb_mmap.limit;
  const void *const prev_addr = env->me_map;
  const size_t prev_size = env->me_dxb_mmap.current;
  if (size_bytes < prev_size) {
    mdbx_notice("resize-MADV_%s %u..%u",
                (env->me_flags & MDBX_WRITEMAP) ? "REMOVE" : "DONTNEED",
                size_pgno, bytes2pgno(env, prev_size));
#if defined(MADV_REMOVE)
    if ((env->me_flags & MDBX_WRITEMAP) == 0 ||
        madvise(env->me_map + size_bytes, prev_size - size_bytes,
                MADV_REMOVE) != 0)
#endif
#if defined(MADV_DONTNEED)
      (void)madvise(env->me_map + size_bytes, prev_size - size_bytes,
                    MADV_DONTNEED);
#elif defined(POSIX_MADV_DONTNEED)
    (void)posix_madvise(env->me_map + size_bytes, prev_size - size_bytes,
                        POSIX_MADV_DONTNEED);
#elif defined(POSIX_FADV_DONTNEED)
    (void)posix_fadvise(env->me_fd, size_bytes, prev_size - size_bytes,
                        POSIX_FADV_DONTNEED);
#else
    __noop();
#endif /* MADV_DONTNEED */
    if (*env->me_discarded_tail > size_pgno)
      *env->me_discarded_tail = size_pgno;
  }

  rc = mdbx_mresize(env->me_flags, &env->me_dxb_mmap, size_bytes, limit_bytes);
  if (rc == MDBX_SUCCESS && (env->me_flags & MDBX_NORDAHEAD) == 0) {
    rc = mdbx_is_readahead_reasonable(size_bytes, 0);
    if (rc == MDBX_RESULT_FALSE)
      rc = mdbx_set_readahead(
          env, 0, (size_bytes > prev_size) ? size_bytes : prev_size, false);
    else if (rc == MDBX_RESULT_TRUE) {
      rc = MDBX_SUCCESS;
      const size_t readahead_pivot =
          (limit_bytes != prev_limit || env->me_dxb_mmap.address != prev_addr
#if defined(_WIN32) || defined(_WIN64)
           || prev_size > size_bytes
#endif /* Windows */
           )
              ? 0 /* reassign readahead to the entire map
                     because it was remapped */
              : prev_size;
      if (size_bytes > readahead_pivot) {
        *env->me_discarded_tail = size_pgno;
        rc = mdbx_set_readahead(env, readahead_pivot,
                                size_bytes - readahead_pivot, true);
      }
    }
  }

bailout:
  if (rc == MDBX_SUCCESS) {
#if defined(_WIN32) || defined(_WIN64)
    mdbx_assert(env, size_bytes == env->me_dxb_mmap.current);
    mdbx_assert(env, size_bytes <= env->me_dxb_mmap.filesize);
    mdbx_assert(env, limit_bytes == env->me_dxb_mmap.limit);
#endif /* Windows */
#ifdef MDBX_USE_VALGRIND
    if (prev_limit != env->me_dxb_mmap.limit || prev_addr != env->me_map) {
      VALGRIND_DISCARD(env->me_valgrind_handle);
      env->me_valgrind_handle = 0;
      if (env->me_dxb_mmap.limit)
        env->me_valgrind_handle =
            VALGRIND_CREATE_BLOCK(env->me_map, env->me_dxb_mmap.limit, "mdbx");
    }
#endif /* MDBX_USE_VALGRIND */
  } else {
    if (rc != MDBX_RESULT_TRUE) {
      mdbx_error("failed resize datafile/mapping: "
                 "present %" PRIuPTR " -> %" PRIuPTR ", "
                 "limit %" PRIuPTR " -> %" PRIuPTR ", errcode %d",
                 env->me_dxb_mmap.current, size_bytes, env->me_dxb_mmap.limit,
                 limit_bytes, rc);
    } else {
      mdbx_notice("unable resize datafile/mapping: "
                  "present %" PRIuPTR " -> %" PRIuPTR ", "
                  "limit %" PRIuPTR " -> %" PRIuPTR ", errcode %d",
                  env->me_dxb_mmap.current, size_bytes, env->me_dxb_mmap.limit,
                  limit_bytes, rc);
    }
    if (!env->me_dxb_mmap.address) {
      env->me_flags |= MDBX_FATAL_ERROR;
      if (env->me_txn)
        env->me_txn->mt_flags |= MDBX_TXN_ERROR;
      rc = MDBX_PANIC;
    }
  }

#if defined(_WIN32) || defined(_WIN64)
  int err = MDBX_SUCCESS;
  mdbx_srwlock_ReleaseExclusive(&env->me_remap_guard);
  if (suspended) {
    err = mdbx_resume_threads_after_remap(suspended);
    if (suspended != &array_onstack)
      mdbx_free(suspended);
  }
#else
  int err = mdbx_fastmutex_release(&env->me_remap_guard);
#endif /* Windows */
  if (err != MDBX_SUCCESS) {
    mdbx_fatal("failed resume-after-remap: errcode %d", err);
    return MDBX_PANIC;
  }
  return rc;
}

/* Allocate page numbers and memory for writing.  Maintain mt_last_reclaimed,
 * mt_reclaimed_pglist and mt_next_pgno.  Set MDBX_TXN_ERROR on failure.
 *
 * If there are free pages available from older transactions, they
 * are re-used first. Otherwise allocate a new page at mt_next_pgno.
 * Do not modify the GC, just merge GC records into mt_reclaimed_pglist
 * and move mt_last_reclaimed to say which records were consumed.  Only this
 * function can create mt_reclaimed_pglist and move
 * mt_last_reclaimed/mt_next_pgno.
 *
 * [in] mc    cursor A cursor handle identifying the transaction and
 *            database for which we are allocating.
 * [in] num   the number of pages to allocate.
 * [out] mp   Address of the allocated page(s). Requests for multiple pages
 *            will always be satisfied by a single contiguous chunk of memory.
 *
 * Returns 0 on success, non-zero on failure.*/

#define MDBX_ALLOC_CACHE 1
#define MDBX_ALLOC_GC 2
#define MDBX_ALLOC_NEW 4
#define MDBX_ALLOC_KICK 8
#define MDBX_ALLOC_ALL                                                         \
  (MDBX_ALLOC_CACHE | MDBX_ALLOC_GC | MDBX_ALLOC_NEW | MDBX_ALLOC_KICK)

static int mdbx_page_alloc(MDBX_cursor *mc, unsigned num, MDBX_page **mp,
                           int flags) {
  int rc;
  MDBX_txn *txn = mc->mc_txn;
  MDBX_env *env = txn->mt_env;
  MDBX_page *np;

  if (likely(flags & MDBX_ALLOC_GC)) {
    flags |= env->me_flags & (MDBX_COALESCE | MDBX_LIFORECLAIM);
    if (unlikely(mc->mc_flags & C_RECLAIMING)) {
      /* If mc is updating the GC, then the retired-list cannot play
       * catch-up with itself by growing while trying to save it. */
      flags &=
          ~(MDBX_ALLOC_GC | MDBX_ALLOC_KICK | MDBX_COALESCE | MDBX_LIFORECLAIM);
    } else if (unlikely(txn->mt_dbs[FREE_DBI].md_entries == 0)) {
      /* avoid (recursive) search inside empty tree and while tree is updating,
       * https://github.com/leo-yuriev/libmdbx/issues/31 */
      flags &= ~MDBX_ALLOC_GC;
    }
  }

  if (likely(flags & MDBX_ALLOC_CACHE)) {
    /* If there are any loose pages, just use them */
    mdbx_assert(env, mp && num);
    if (likely(num == 1 && txn->tw.loose_pages)) {
      if (txn->tw.loose_refund_wl > txn->mt_next_pgno) {
        mdbx_refund(txn);
        if (unlikely(!txn->tw.loose_pages))
          goto skip_cache;
      }

      np = txn->tw.loose_pages;
      txn->tw.loose_pages = np->mp_next;
      txn->tw.loose_count--;
      mdbx_debug("db %d use loose page %" PRIaPGNO, DDBI(mc), np->mp_pgno);
      mdbx_tassert(txn, np->mp_pgno < txn->mt_next_pgno);
      mdbx_ensure(env, np->mp_pgno >= NUM_METAS);
      VALGRIND_MAKE_MEM_UNDEFINED(page_data(np), page_space(txn->mt_env));
      ASAN_UNPOISON_MEMORY_REGION(page_data(np), page_space(txn->mt_env));
      *mp = np;
      return MDBX_SUCCESS;
    }
  }
skip_cache:

  mdbx_tassert(
      txn, mdbx_pnl_check4assert(txn->tw.reclaimed_pglist, txn->mt_next_pgno));
  pgno_t pgno, *re_list = txn->tw.reclaimed_pglist;
  unsigned range_begin = 0, re_len = MDBX_PNL_SIZE(re_list);
  txnid_t oldest = 0, last = 0;
  const unsigned wanna_range = num - 1;

  while (1) { /* oom-kick retry loop */
    /* If our dirty list is already full, we can't do anything */
    if (unlikely(txn->tw.dirtyroom == 0)) {
      rc = MDBX_TXN_FULL;
      goto fail;
    }

    MDBX_cursor recur;
    for (MDBX_cursor_op op = MDBX_FIRST;;
         op = (flags & MDBX_LIFORECLAIM) ? MDBX_PREV : MDBX_NEXT) {
      MDBX_val key, data;

      /* Seek a big enough contiguous page range.
       * Prefer pages with lower pgno. */
      mdbx_tassert(txn, mdbx_pnl_check4assert(txn->tw.reclaimed_pglist,
                                              txn->mt_next_pgno));
      if (likely(flags & MDBX_ALLOC_CACHE) && re_len > wanna_range &&
          (!(flags & MDBX_COALESCE) || op == MDBX_FIRST)) {
        mdbx_tassert(txn, MDBX_PNL_LAST(re_list) < txn->mt_next_pgno &&
                              MDBX_PNL_FIRST(re_list) < txn->mt_next_pgno);
        range_begin = MDBX_PNL_ASCENDING ? 1 : re_len;
        pgno = MDBX_PNL_LEAST(re_list);
        if (likely(wanna_range == 0))
          goto done;
#if MDBX_PNL_ASCENDING
        mdbx_tassert(txn, pgno == re_list[1] && range_begin == 1);
        while (true) {
          unsigned range_end = range_begin + wanna_range;
          if (re_list[range_end] - pgno == wanna_range)
            goto done;
          if (range_end == re_len)
            break;
          pgno = re_list[++range_begin];
        }
#else
        mdbx_tassert(txn, pgno == re_list[re_len] && range_begin == re_len);
        while (true) {
          if (re_list[range_begin - wanna_range] - pgno == wanna_range)
            goto done;
          if (range_begin == wanna_range)
            break;
          pgno = re_list[--range_begin];
        }
#endif /* MDBX_PNL sort-order */
      }

      if (op == MDBX_FIRST) { /* 1st iteration, setup cursor, etc */
        if (unlikely(!(flags & MDBX_ALLOC_GC)))
          break /* reclaiming is prohibited for now */;

        /* Prepare to fetch more and coalesce */
        oldest = (flags & MDBX_LIFORECLAIM) ? mdbx_find_oldest(txn)
                                            : *env->me_oldest;
        rc = mdbx_cursor_init(&recur, txn, FREE_DBI);
        if (unlikely(rc != MDBX_SUCCESS))
          goto fail;
        if (flags & MDBX_LIFORECLAIM) {
          /* Begin from oldest reader if any */
          if (oldest > 2) {
            last = oldest - 1;
            op = MDBX_SET_RANGE;
          }
        } else if (txn->tw.last_reclaimed) {
          /* Continue lookup from txn->tw.last_reclaimed to oldest reader */
          last = txn->tw.last_reclaimed;
          op = MDBX_SET_RANGE;
        }

        key.iov_base = &last;
        key.iov_len = sizeof(last);
      }

      if (!(flags & MDBX_LIFORECLAIM)) {
        /* Do not try fetch more if the record will be too recent */
        if (op != MDBX_FIRST && ++last >= oldest) {
          oldest = mdbx_find_oldest(txn);
          if (oldest <= last)
            break;
        }
      }

      rc = mdbx_cursor_get(&recur, &key, NULL, op);
      if (rc == MDBX_NOTFOUND && (flags & MDBX_LIFORECLAIM)) {
        if (op == MDBX_SET_RANGE)
          continue;
        txnid_t snap = mdbx_find_oldest(txn);
        if (oldest < snap) {
          oldest = snap;
          last = oldest - 1;
          key.iov_base = &last;
          key.iov_len = sizeof(last);
          op = MDBX_SET_RANGE;
          rc = mdbx_cursor_get(&recur, &key, NULL, op);
        }
      }
      if (unlikely(rc)) {
        if (rc == MDBX_NOTFOUND)
          break;
        goto fail;
      }

      if (unlikely(key.iov_len != sizeof(txnid_t))) {
        rc = MDBX_CORRUPTED;
        goto fail;
      }
      last = unaligned_peek_u64(4, key.iov_base);
      if (unlikely(last < 1 || last >= SAFE64_INVALID_THRESHOLD)) {
        rc = MDBX_CORRUPTED;
        goto fail;
      }
      if (oldest <= last) {
        oldest = mdbx_find_oldest(txn);
        if (oldest <= last) {
          if (flags & MDBX_LIFORECLAIM)
            continue;
          break;
        }
      }

      if (flags & MDBX_LIFORECLAIM) {
        /* skip IDs of records that already reclaimed */
        if (txn->tw.lifo_reclaimed) {
          unsigned i;
          for (i = (unsigned)MDBX_PNL_SIZE(txn->tw.lifo_reclaimed); i > 0; --i)
            if (txn->tw.lifo_reclaimed[i] == last)
              break;
          if (i)
            continue;
        }
      }

      /* Reading next GC record */
      np = recur.mc_pg[recur.mc_top];
      if (unlikely((rc = mdbx_node_read(
                        &recur, page_node(np, recur.mc_ki[recur.mc_top]),
                        &data)) != MDBX_SUCCESS))
        goto fail;

      if ((flags & MDBX_LIFORECLAIM) && !txn->tw.lifo_reclaimed) {
        txn->tw.lifo_reclaimed = mdbx_txl_alloc();
        if (unlikely(!txn->tw.lifo_reclaimed)) {
          rc = MDBX_ENOMEM;
          goto fail;
        }
      }

      /* Append PNL from GC record to me_reclaimed_pglist */
      mdbx_cassert(mc, (mc->mc_flags & C_GCFREEZE) == 0);
      pgno_t *gc_pnl = (pgno_t *)data.iov_base;
      mdbx_tassert(txn, data.iov_len >= MDBX_PNL_SIZEOF(gc_pnl));
      if (unlikely(data.iov_len < MDBX_PNL_SIZEOF(gc_pnl) ||
                   !mdbx_pnl_check(gc_pnl, txn->mt_next_pgno))) {
        rc = MDBX_CORRUPTED;
        goto fail;
      }
      const unsigned gc_len = MDBX_PNL_SIZE(gc_pnl);
      rc = mdbx_pnl_need(&txn->tw.reclaimed_pglist, gc_len);
      if (unlikely(rc != MDBX_SUCCESS))
        goto fail;
      re_list = txn->tw.reclaimed_pglist;

      /* Remember ID of GC record */
      if (flags & MDBX_LIFORECLAIM) {
        if ((rc = mdbx_txl_append(&txn->tw.lifo_reclaimed, last)) != 0)
          goto fail;
      }
      txn->tw.last_reclaimed = last;

      if (mdbx_log_enabled(MDBX_LOG_EXTRA)) {
        mdbx_debug_extra("PNL read txn %" PRIaTXN " root %" PRIaPGNO
                         " num %u, PNL",
                         last, txn->mt_dbs[FREE_DBI].md_root, gc_len);
        unsigned i;
        for (i = gc_len; i; i--)
          mdbx_debug_extra_print(" %" PRIaPGNO, gc_pnl[i]);
        mdbx_debug_extra_print("%s", "\n");
      }

      /* Merge in descending sorted order */
      const unsigned prev_re_len = MDBX_PNL_SIZE(re_list);
      mdbx_pnl_xmerge(re_list, gc_pnl);
      /* re-check to avoid duplicates */
      if (unlikely(!mdbx_pnl_check(re_list, txn->mt_next_pgno))) {
        rc = MDBX_CORRUPTED;
        goto fail;
      }

      re_len = MDBX_PNL_SIZE(re_list);
      mdbx_tassert(txn, re_len == 0 || re_list[re_len] < txn->mt_next_pgno);
      if (re_len && unlikely(MDBX_PNL_MOST(re_list) == txn->mt_next_pgno - 1)) {
        /* Refund suitable pages into "unallocated" space */
        mdbx_refund(txn);
        re_list = txn->tw.reclaimed_pglist;
        re_len = MDBX_PNL_SIZE(re_list);
      }

      if (unlikely((flags & MDBX_ALLOC_CACHE) == 0)) {
        /* Done for a kick-reclaim mode, actually no page needed */
        return MDBX_SUCCESS;
      }

      /* Don't try to coalesce too much. */
      if (unlikely(re_len > MDBX_DPL_TXNFULL / 4))
        break;
      if (re_len /* current size */ >= env->me_maxgc_ov1page ||
          (re_len > prev_re_len && re_len - prev_re_len /* delta from prev */ >=
                                       env->me_maxgc_ov1page / 2))
        flags &= ~MDBX_COALESCE;
    }

    if ((flags & (MDBX_COALESCE | MDBX_ALLOC_CACHE)) ==
            (MDBX_COALESCE | MDBX_ALLOC_CACHE) &&
        re_len > wanna_range) {
      range_begin = MDBX_PNL_ASCENDING ? 1 : re_len;
      pgno = MDBX_PNL_LEAST(re_list);
      if (likely(wanna_range == 0))
        goto done;
#if MDBX_PNL_ASCENDING
      mdbx_tassert(txn, pgno == re_list[1] && range_begin == 1);
      while (true) {
        unsigned range_end = range_begin + wanna_range;
        if (re_list[range_end] - pgno == wanna_range)
          goto done;
        if (range_end == re_len)
          break;
        pgno = re_list[++range_begin];
      }
#else
      mdbx_tassert(txn, pgno == re_list[re_len] && range_begin == re_len);
      while (true) {
        if (re_list[range_begin - wanna_range] - pgno == wanna_range)
          goto done;
        if (range_begin == wanna_range)
          break;
        pgno = re_list[--range_begin];
      }
#endif /* MDBX_PNL sort-order */
    }

    /* Use new pages from the map when nothing suitable in the GC */
    range_begin = 0;
    pgno = txn->mt_next_pgno;
    rc = MDBX_MAP_FULL;
    const pgno_t next = pgno_add(pgno, num);
    if (likely(next <= txn->mt_end_pgno)) {
      rc = MDBX_NOTFOUND;
      if (likely(flags & MDBX_ALLOC_NEW))
        goto done;
    }

    const MDBX_meta *head = mdbx_meta_head(env);
    if ((flags & MDBX_ALLOC_GC) &&
        ((flags & MDBX_ALLOC_KICK) || rc == MDBX_MAP_FULL)) {
      MDBX_meta *steady = mdbx_meta_steady(env);

      if (oldest == mdbx_meta_txnid_stable(env, steady) &&
          !META_IS_STEADY(head) && META_IS_STEADY(steady)) {
        /* LY: Here an oom was happened:
         *  - all pages had allocated;
         *  - reclaiming was stopped at the last steady-sync;
         *  - the head-sync is weak.
         * Now we need make a sync to resume reclaiming. If both
         * MDBX_NOSYNC and MDBX_MAPASYNC flags are set, then assume that
         * utterly no-sync write mode was requested. In such case
         * don't make a steady-sync, but only a legacy-mode checkpoint,
         * just for resume reclaiming only, not for data consistency. */

        mdbx_debug("kick-gc: head %" PRIaTXN "-%s, tail %" PRIaTXN
                   "-%s, oldest %" PRIaTXN,
                   mdbx_meta_txnid_stable(env, head), mdbx_durable_str(head),
                   mdbx_meta_txnid_stable(env, steady),
                   mdbx_durable_str(steady), oldest);

        const unsigned syncflags = F_ISSET(env->me_flags, MDBX_UTTERLY_NOSYNC)
                                       ? env->me_flags
                                       : env->me_flags & MDBX_WRITEMAP;
        MDBX_meta meta = *head;
        if (mdbx_sync_locked(env, syncflags, &meta) == MDBX_SUCCESS) {
          txnid_t snap = mdbx_find_oldest(txn);
          if (snap > oldest)
            continue;
        }
      }

      if (rc == MDBX_MAP_FULL && oldest < txn->mt_txnid - MDBX_TXNID_STEP) {
        if (mdbx_oomkick(env, oldest) > oldest)
          continue;
      }
    }

    if (rc == MDBX_MAP_FULL && next < head->mm_geo.upper) {
      mdbx_assert(env, next > txn->mt_end_pgno);
      pgno_t aligned = pgno_align2os_pgno(
          env, pgno_add(next, head->mm_geo.grow - next % head->mm_geo.grow));

      if (aligned > head->mm_geo.upper)
        aligned = head->mm_geo.upper;
      mdbx_assert(env, aligned > txn->mt_end_pgno);

      mdbx_verbose("try growth datafile to %" PRIaPGNO " pages (+%" PRIaPGNO
                   ")",
                   aligned, aligned - txn->mt_end_pgno);
      rc = mdbx_mapresize(env, aligned, head->mm_geo.upper);
      if (rc == MDBX_SUCCESS) {
        env->me_txn->mt_end_pgno = aligned;
        if (!mp)
          return rc;
        goto done;
      }

      mdbx_warning("unable growth datafile to %" PRIaPGNO "pages (+%" PRIaPGNO
                   "), errcode %d",
                   aligned, aligned - txn->mt_end_pgno, rc);
    }

  fail:
    mdbx_tassert(txn, mdbx_pnl_check4assert(txn->tw.reclaimed_pglist,
                                            txn->mt_next_pgno));
    if (mp) {
      *mp = NULL;
      txn->mt_flags |= MDBX_TXN_ERROR;
    }
    mdbx_assert(env, rc != MDBX_SUCCESS);
    return rc;
  }

done:
  mdbx_tassert(txn, mp && num);
  mdbx_ensure(env, pgno >= NUM_METAS);
  if (env->me_flags & MDBX_WRITEMAP) {
    np = pgno2page(env, pgno);
    /* LY: reset no-access flag from mdbx_loose_page() */
    VALGRIND_MAKE_MEM_UNDEFINED(np, pgno2bytes(env, num));
    ASAN_UNPOISON_MEMORY_REGION(np, pgno2bytes(env, num));
  } else {
    if (unlikely(!(np = mdbx_page_malloc(txn, num)))) {
      rc = MDBX_ENOMEM;
      goto fail;
    }
  }

  if (range_begin) {
    mdbx_cassert(mc, (mc->mc_flags & C_GCFREEZE) == 0);
    mdbx_tassert(txn, pgno < txn->mt_next_pgno);
    mdbx_tassert(txn, pgno == re_list[range_begin]);
    /* Cutoff allocated pages from me_reclaimed_pglist */
#if MDBX_PNL_ASCENDING
    for (unsigned i = range_begin + num; i <= re_len;)
      re_list[range_begin++] = re_list[i++];
    MDBX_PNL_SIZE(re_list) = re_len = range_begin - 1;
#else
    MDBX_PNL_SIZE(re_list) = re_len -= num;
    for (unsigned i = range_begin - num; i < re_len;)
      re_list[++i] = re_list[++range_begin];
#endif
    mdbx_tassert(txn, mdbx_pnl_check4assert(txn->tw.reclaimed_pglist,
                                            txn->mt_next_pgno));
  } else {
    txn->mt_next_pgno = pgno + num;
    mdbx_assert(env, txn->mt_next_pgno <= txn->mt_end_pgno);
  }

  if (unlikely(env->me_flags & MDBX_PAGEPERTURB))
    memset(np, -1, pgno2bytes(env, num));
  VALGRIND_MAKE_MEM_UNDEFINED(np, pgno2bytes(env, num));

  np->mp_pgno = pgno;
  np->mp_leaf2_ksize = 0;
  np->mp_flags = 0;
  np->mp_pages = num;
  rc = mdbx_page_dirty(txn, np);
  if (unlikely(rc != MDBX_SUCCESS))
    goto fail;
  *mp = np;

  mdbx_tassert(
      txn, mdbx_pnl_check4assert(txn->tw.reclaimed_pglist, txn->mt_next_pgno));
  return MDBX_SUCCESS;
}

/* Copy the used portions of a non-overflow page.
 * [in] dst page to copy into
 * [in] src page to copy from
 * [in] psize size of a page */
__hot static void mdbx_page_copy(MDBX_page *dst, MDBX_page *src,
                                 unsigned psize) {
  STATIC_ASSERT(UINT16_MAX > MAX_PAGESIZE - PAGEHDRSZ);
  STATIC_ASSERT(MIN_PAGESIZE > PAGEHDRSZ + NODESIZE * 4);
  if (!IS_LEAF2(src)) {
    size_t upper = src->mp_upper, lower = src->mp_lower, unused = upper - lower;

    /* If page isn't full, just copy the used portion. Adjust
     * alignment so memcpy may copy words instead of bytes. */
    if (unused > sizeof(void *) * 42) {
      lower = roundup_powerof2(lower + PAGEHDRSZ, sizeof(void *));
      upper = (upper + PAGEHDRSZ) & ~(sizeof(void *) - 1);
      memcpy(dst, src, lower);
      memcpy((char *)dst + upper, (char *)src + upper, psize - upper);
      return;
    }
  }
  memcpy(dst, src, psize);
}

/* Pull a page off the txn's spill list, if present.
 *
 * If a page being referenced was spilled to disk in this txn, bring
 * it back and make it dirty/writable again.
 *
 * [in] txn   the transaction handle.
 * [in] mp    the page being referenced. It must not be dirty.
 * [out] ret  the writable page, if any.
 *            ret is unchanged if mp wasn't spilled. */
__hot static int __must_check_result mdbx_page_unspill(MDBX_txn *txn,
                                                       MDBX_page *mp,
                                                       MDBX_page **ret) {
  MDBX_env *env = txn->mt_env;
  pgno_t pgno = mp->mp_pgno, pn = pgno << 1;

  for (const MDBX_txn *tx2 = txn; tx2; tx2 = tx2->mt_parent) {
    if (!tx2->tw.spill_pages)
      continue;
    unsigned i = mdbx_pnl_exist(tx2->tw.spill_pages, pn);
    if (!i)
      continue;
    if (txn->tw.dirtyroom == 0)
      return MDBX_TXN_FULL;
    unsigned num = IS_OVERFLOW(mp) ? mp->mp_pages : 1;
    MDBX_page *np = mp;
    if ((env->me_flags & MDBX_WRITEMAP) == 0) {
      np = mdbx_page_malloc(txn, num);
      if (unlikely(!np))
        return MDBX_ENOMEM;
      if (unlikely(num > 1))
        memcpy(np, mp, pgno2bytes(env, num));
      else
        mdbx_page_copy(np, mp, env->me_psize);
    }
    mdbx_debug("unspill page %" PRIaPGNO, mp->mp_pgno);
    if (tx2 == txn) {
      /* If in current txn, this page is no longer spilled.
       * If it happens to be the last page, truncate the spill list.
       * Otherwise mark it as deleted by setting the LSB. */
      txn->tw.spill_pages[i] |= 1;
      if (i == MDBX_PNL_SIZE(txn->tw.spill_pages))
        MDBX_PNL_SIZE(txn->tw.spill_pages) -= 1;
    } /* otherwise, if belonging to a parent txn, the
       * page remains spilled until child commits */

    int rc = mdbx_page_dirty(txn, np);
    if (likely(rc == MDBX_SUCCESS)) {
      np->mp_flags |= P_DIRTY;
      *ret = np;
    }
    return rc;
  }
  return MDBX_SUCCESS;
}

/* Touch a page: make it dirty and re-insert into tree with updated pgno.
 * Set MDBX_TXN_ERROR on failure.
 *
 * [in] mc  cursor pointing to the page to be touched
 *
 * Returns 0 on success, non-zero on failure. */
__hot static int mdbx_page_touch(MDBX_cursor *mc) {
  MDBX_page *mp = mc->mc_pg[mc->mc_top], *np;
  MDBX_txn *txn = mc->mc_txn;
  MDBX_cursor *m2, *m3;
  pgno_t pgno;
  int rc;

  mdbx_cassert(mc, !IS_OVERFLOW(mp));
  if (!F_ISSET(mp->mp_flags, P_DIRTY)) {
    if (txn->mt_flags & MDBX_TXN_SPILLS) {
      np = NULL;
      rc = mdbx_page_unspill(txn, mp, &np);
      if (unlikely(rc))
        goto fail;
      if (likely(np))
        goto done;
    }

    if (unlikely((rc = mdbx_pnl_need(&txn->tw.retired_pages, 1)) ||
                 (rc = mdbx_page_alloc(mc, 1, &np, MDBX_ALLOC_ALL))))
      goto fail;
    pgno = np->mp_pgno;
    mdbx_debug("touched db %d page %" PRIaPGNO " -> %" PRIaPGNO, DDBI(mc),
               mp->mp_pgno, pgno);
    mdbx_cassert(mc, mp->mp_pgno != pgno);
    mdbx_pnl_xappend(txn->tw.retired_pages, mp->mp_pgno);
    mdbx_tassert(txn, mdbx_dpl_find(txn->tw.dirtylist, mp->mp_pgno) == nullptr);
    /* Update the parent page, if any, to point to the new page */
    if (mc->mc_top) {
      MDBX_page *parent = mc->mc_pg[mc->mc_top - 1];
      MDBX_node *node = page_node(parent, mc->mc_ki[mc->mc_top - 1]);
      node_set_pgno(node, pgno);
    } else {
      mc->mc_db->md_root = pgno;
    }
  } else if (txn->mt_parent && !IS_SUBP(mp)) {
    mdbx_tassert(txn, (txn->mt_env->me_flags & MDBX_WRITEMAP) == 0);
    pgno = mp->mp_pgno;
    /* If txn has a parent, make sure the page is in our dirty list. */
    const MDBX_page *const dp = mdbx_dpl_find(txn->tw.dirtylist, pgno);
    if (dp) {
      if (unlikely(mp != dp)) { /* bad cursor? */
        mdbx_error("wrong page 0x%p #%" PRIaPGNO
                   " in the dirtylist, expecting %p",
                   __Wpedantic_format_voidptr(dp), pgno,
                   __Wpedantic_format_voidptr(mp));
        mc->mc_flags &= ~(C_INITIALIZED | C_EOF);
        rc = MDBX_PROBLEM;
        goto fail;
      }
      return MDBX_SUCCESS;
    }

    mdbx_debug("clone db %d page %" PRIaPGNO, DDBI(mc), mp->mp_pgno);
    mdbx_cassert(mc, txn->tw.dirtylist->length <= MDBX_DPL_TXNFULL);
    /* No - copy it */
    np = mdbx_page_malloc(txn, 1);
    if (unlikely(!np)) {
      rc = MDBX_ENOMEM;
      goto fail;
    }
    rc = mdbx_dpl_append(txn->tw.dirtylist, pgno, np);
    if (unlikely(rc)) {
      mdbx_dpage_free(txn->mt_env, np, 1);
      goto fail;
    }
  } else {
    return MDBX_SUCCESS;
  }

  mdbx_page_copy(np, mp, txn->mt_env->me_psize);
  np->mp_pgno = pgno;
  np->mp_flags |= P_DIRTY;

done:
  /* Adjust cursors pointing to mp */
  mc->mc_pg[mc->mc_top] = np;
  m2 = txn->mt_cursors[mc->mc_dbi];
  if (mc->mc_flags & C_SUB) {
    for (; m2; m2 = m2->mc_next) {
      m3 = &m2->mc_xcursor->mx_cursor;
      if (m3->mc_snum < mc->mc_snum)
        continue;
      if (m3->mc_pg[mc->mc_top] == mp)
        m3->mc_pg[mc->mc_top] = np;
    }
  } else {
    for (; m2; m2 = m2->mc_next) {
      if (m2->mc_snum < mc->mc_snum)
        continue;
      if (m2 == mc)
        continue;
      if (m2->mc_pg[mc->mc_top] == mp) {
        m2->mc_pg[mc->mc_top] = np;
        if (XCURSOR_INITED(m2) && IS_LEAF(np))
          XCURSOR_REFRESH(m2, np, m2->mc_ki[mc->mc_top]);
      }
    }
  }
  return MDBX_SUCCESS;

fail:
  txn->mt_flags |= MDBX_TXN_ERROR;
  return rc;
}

__cold int mdbx_env_sync_ex(MDBX_env *env, int force, int nonblock) {
  if (unlikely(!env))
    return MDBX_EINVAL;

  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

#if MDBX_TXN_CHECKPID
  if (unlikely(env->me_pid != mdbx_getpid())) {
    env->me_flags |= MDBX_FATAL_ERROR;
    return MDBX_PANIC;
  }
#endif /* MDBX_TXN_CHECKPID */

  unsigned flags = env->me_flags & ~MDBX_NOMETASYNC;
  if (unlikely(flags & (MDBX_RDONLY | MDBX_FATAL_ERROR)))
    return MDBX_EACCESS;

  if (unlikely(!env->me_map))
    return MDBX_EPERM;

  int rc = MDBX_RESULT_TRUE /* means "nothing to sync" */;
  bool need_unlock = false;
  if (nonblock && *env->me_unsynced_pages == 0)
    goto fastpath;

  const bool outside_txn = (env->me_txn0->mt_owner != mdbx_thread_self());
  if (outside_txn) {
    int err = mdbx_txn_lock(env, nonblock);
    if (unlikely(err != MDBX_SUCCESS))
      return err;
    need_unlock = true;
  }

  const MDBX_meta *head = mdbx_meta_head(env);
  pgno_t unsynced_pages = *env->me_unsynced_pages;
  if (!META_IS_STEADY(head) || unsynced_pages) {
    const pgno_t autosync_threshold = *env->me_autosync_threshold;
    const uint64_t autosync_period = *env->me_autosync_period;
    if (force || (autosync_threshold && unsynced_pages >= autosync_threshold) ||
        (autosync_period &&
         mdbx_osal_monotime() - *env->me_sync_timestamp >= autosync_period))
      flags &= MDBX_WRITEMAP /* clear flags for full steady sync */;

    if (outside_txn) {
      if (unsynced_pages > /* FIXME: define threshold */ 16 &&
          (flags & (MDBX_NOSYNC | MDBX_MAPASYNC)) == 0) {
        mdbx_assert(env, ((flags ^ env->me_flags) & MDBX_WRITEMAP) == 0);
        const size_t usedbytes = pgno_align2os_bytes(env, head->mm_geo.next);

        mdbx_txn_unlock(env);

        /* LY: pre-sync without holding lock to reduce latency for writer(s) */
        int err = (flags & MDBX_WRITEMAP)
                      ? mdbx_msync(&env->me_dxb_mmap, 0, usedbytes, false)
                      : mdbx_filesync(env->me_fd, MDBX_SYNC_DATA);
        if (unlikely(err != MDBX_SUCCESS))
          return err;

        err = mdbx_txn_lock(env, nonblock);
        if (unlikely(err != MDBX_SUCCESS))
          return err;

        /* LY: head and unsynced_pages may be changed. */
        head = mdbx_meta_head(env);
        unsynced_pages = *env->me_unsynced_pages;
      }
      env->me_txn0->mt_txnid = meta_txnid(env, head, false);
      mdbx_find_oldest(env->me_txn0);
      rc = MDBX_RESULT_FALSE /* means "some data was synced" */;
    }

    if (!META_IS_STEADY(head) ||
        ((flags & (MDBX_NOSYNC | MDBX_MAPASYNC)) == 0 && unsynced_pages)) {
      mdbx_debug("meta-head %" PRIaPGNO ", %s, sync_pending %" PRIaPGNO,
                 data_page(head)->mp_pgno, mdbx_durable_str(head),
                 unsynced_pages);
      MDBX_meta meta = *head;
      int err = mdbx_sync_locked(env, flags | MDBX_SHRINK_ALLOWED, &meta);
      if (unlikely(err != MDBX_SUCCESS)) {
        if (need_unlock)
          mdbx_txn_unlock(env);
        return err;
      }
      rc = MDBX_RESULT_FALSE /* means "some data was synced" */;
    }
  }

fastpath:
  /* LY: sync meta-pages if MDBX_NOMETASYNC enabled
   *     and someone was not synced above. */
  if (rc == MDBX_RESULT_TRUE && (env->me_flags & MDBX_NOMETASYNC) != 0) {
    const txnid_t head_txnid = mdbx_recent_committed_txnid(env);
    if (*env->me_meta_sync_txnid != (uint32_t)head_txnid) {
      rc = (flags & MDBX_WRITEMAP)
               ? mdbx_msync(&env->me_dxb_mmap, 0, pgno2bytes(env, NUM_METAS),
                            false)
               : mdbx_filesync(env->me_fd, MDBX_SYNC_DATA | MDBX_SYNC_IODQ);
      if (likely(rc == MDBX_SUCCESS))
        *env->me_meta_sync_txnid = (uint32_t)head_txnid;
    }
  }
  if (need_unlock)
    mdbx_txn_unlock(env);
  return rc;
}

__cold int mdbx_env_sync(MDBX_env *env) {
  return mdbx_env_sync_ex(env, true, false);
}

__cold int mdbx_env_sync_poll(MDBX_env *env) {
  return mdbx_env_sync_ex(env, false, true);
}

/* Back up parent txn's cursors, then grab the originals for tracking */
static int mdbx_cursor_shadow(MDBX_txn *src, MDBX_txn *dst) {
  MDBX_cursor *mc, *bk;
  MDBX_xcursor *mx;

  for (int i = src->mt_numdbs; --i >= 0;) {
    dst->mt_cursors[i] = NULL;
    if ((mc = src->mt_cursors[i]) != NULL) {
      size_t size = sizeof(MDBX_cursor);
      if (mc->mc_xcursor)
        size += sizeof(MDBX_xcursor);
      for (; mc; mc = bk->mc_next) {
        bk = mdbx_malloc(size);
        if (unlikely(!bk))
          return MDBX_ENOMEM;
        *bk = *mc;
        mc->mc_backup = bk;
        mc->mc_db = &dst->mt_dbs[i];
        /* Kill pointers into src to reduce abuse: The
         * user may not use mc until dst ends. But we need a valid
         * txn pointer here for cursor fixups to keep working. */
        mc->mc_txn = dst;
        mc->mc_dbflag = &dst->mt_dbflags[i];
        if ((mx = mc->mc_xcursor) != NULL) {
          *(MDBX_xcursor *)(bk + 1) = *mx;
          mx->mx_cursor.mc_txn = dst;
        }
        mc->mc_next = dst->mt_cursors[i];
        dst->mt_cursors[i] = mc;
      }
    }
  }
  return MDBX_SUCCESS;
}

/* Close this write txn's cursors, give parent txn's cursors back to parent.
 *
 * [in] txn     the transaction handle.
 * [in] merge   true to keep changes to parent cursors, false to revert.
 *
 * Returns 0 on success, non-zero on failure. */
static void mdbx_cursors_eot(MDBX_txn *txn, unsigned merge) {
  MDBX_cursor **cursors = txn->mt_cursors, *mc, *next, *bk;
  MDBX_xcursor *mx;
  int i;

  for (i = txn->mt_numdbs; --i >= 0;) {
    for (mc = cursors[i]; mc; mc = next) {
      unsigned stage = mc->mc_signature;
      mdbx_ensure(txn->mt_env,
                  stage == MDBX_MC_SIGNATURE || stage == MDBX_MC_WAIT4EOT);
      next = mc->mc_next;
      mdbx_tassert(txn, !next || next->mc_signature == MDBX_MC_SIGNATURE ||
                            next->mc_signature == MDBX_MC_WAIT4EOT);
      if ((bk = mc->mc_backup) != NULL) {
        if (merge) {
          /* Commit changes to parent txn */
          mc->mc_next = bk->mc_next;
          mc->mc_backup = bk->mc_backup;
          mc->mc_txn = bk->mc_txn;
          mc->mc_db = bk->mc_db;
          mc->mc_dbflag = bk->mc_dbflag;
          if ((mx = mc->mc_xcursor) != NULL)
            mx->mx_cursor.mc_txn = bk->mc_txn;
        } else {
          /* Abort nested txn */
          *mc = *bk;
          if ((mx = mc->mc_xcursor) != NULL)
            *mx = *(MDBX_xcursor *)(bk + 1);
        }
        bk->mc_signature = 0;
        mdbx_free(bk);
      }
      if (stage == MDBX_MC_WAIT4EOT) {
        mc->mc_signature = 0;
        mdbx_free(mc);
      } else {
        mc->mc_signature = MDBX_MC_READY4CLOSE;
        mc->mc_flags = 0 /* reset C_UNTRACK */;
      }
    }
    cursors[i] = NULL;
  }
}

#if defined(MDBX_USE_VALGRIND) || defined(__SANITIZE_ADDRESS__)
/* Find largest mvcc-snapshot still referenced by this process. */
static pgno_t mdbx_find_largest_this(MDBX_env *env, pgno_t largest) {
  MDBX_lockinfo *const lck = env->me_lck;
  if (likely(lck != NULL /* exclusive mode */)) {
    const unsigned snap_nreaders = lck->mti_numreaders;
    for (unsigned i = 0; i < snap_nreaders; ++i) {
    retry:
      if (lck->mti_readers[i].mr_pid == env->me_pid) {
        /* mdbx_jitter4testing(true); */
        const pgno_t snap_pages = lck->mti_readers[i].mr_snapshot_pages_used;
        const txnid_t snap_txnid = safe64_read(&lck->mti_readers[i].mr_txnid);
        mdbx_memory_barrier();
        if (unlikely(snap_pages != lck->mti_readers[i].mr_snapshot_pages_used ||
                     snap_txnid != safe64_read(&lck->mti_readers[i].mr_txnid)))
          goto retry;
        if (largest < snap_pages &&
            lck->mti_oldest_reader <= /* ignore pending updates */ snap_txnid &&
            snap_txnid < SAFE64_INVALID_THRESHOLD)
          largest = snap_pages;
      }
    }
  }
  return largest;
}

static void mdbx_txn_valgrind(MDBX_env *env, MDBX_txn *txn) {
#if !defined(__SANITIZE_ADDRESS__)
  if (!RUNNING_ON_VALGRIND)
    return;
#endif

  if (txn) { /* transaction start */
    if (env->me_poison_edge < txn->mt_next_pgno)
      env->me_poison_edge = txn->mt_next_pgno;
    VALGRIND_MAKE_MEM_DEFINED(env->me_map, pgno2bytes(env, txn->mt_next_pgno));
    ASAN_UNPOISON_MEMORY_REGION(env->me_map,
                                pgno2bytes(env, txn->mt_next_pgno));
    /* don't touch more, it should be already poisoned */
  } else { /* transaction end */
    bool should_unlock = false;
    pgno_t last = MAX_PAGENO;
    if (env->me_txn0 && env->me_txn0->mt_owner == mdbx_thread_self()) {
      /* inside write-txn */
      MDBX_meta *head = mdbx_meta_head(env);
      last = head->mm_geo.next;
    } else if (mdbx_txn_lock(env, true) == MDBX_SUCCESS) {
      /* no write-txn */
      last = NUM_METAS;
      should_unlock = true;
    } else {
      /* write txn is running, therefore shouldn't poison any memory range */
      return;
    }

    last = mdbx_find_largest_this(env, last);
    const pgno_t edge = env->me_poison_edge;
    if (edge > last) {
      mdbx_assert(env, last >= NUM_METAS);
      env->me_poison_edge = last;
      VALGRIND_MAKE_MEM_NOACCESS(env->me_map + pgno2bytes(env, last),
                                 pgno2bytes(env, edge - last));
      ASAN_POISON_MEMORY_REGION(env->me_map + pgno2bytes(env, last),
                                pgno2bytes(env, edge - last));
    }
    if (should_unlock)
      mdbx_txn_unlock(env);
  }
}
#endif /* MDBX_USE_VALGRIND */

/* Common code for mdbx_txn_begin() and mdbx_txn_renew(). */
static int mdbx_txn_renew0(MDBX_txn *txn, unsigned flags) {
  MDBX_env *env = txn->mt_env;
  int rc;

#if MDBX_TXN_CHECKPID
  if (unlikely(env->me_pid != mdbx_getpid())) {
    env->me_flags |= MDBX_FATAL_ERROR;
    return MDBX_PANIC;
  }
#endif /* MDBX_TXN_CHECKPID */

  STATIC_ASSERT(sizeof(MDBX_reader) == 32);
#ifdef MDBX_OSAL_LOCK
  STATIC_ASSERT(offsetof(MDBX_lockinfo, mti_wmutex) % MDBX_CACHELINE_SIZE == 0);
  STATIC_ASSERT(offsetof(MDBX_lockinfo, mti_rmutex) % MDBX_CACHELINE_SIZE == 0);
#else
  STATIC_ASSERT(
      offsetof(MDBX_lockinfo, mti_oldest_reader) % MDBX_CACHELINE_SIZE == 0);
  STATIC_ASSERT(offsetof(MDBX_lockinfo, mti_numreaders) % MDBX_CACHELINE_SIZE ==
                0);
#endif
  STATIC_ASSERT(offsetof(MDBX_lockinfo, mti_readers) % MDBX_CACHELINE_SIZE ==
                0);

  if (flags & MDBX_RDONLY) {
    txn->mt_flags = MDBX_RDONLY | (env->me_flags & MDBX_NOTLS);
    MDBX_reader *r = txn->to.reader;
    STATIC_ASSERT(sizeof(size_t) == sizeof(r->mr_tid));
    if (likely(env->me_flags & MDBX_ENV_TXKEY)) {
      mdbx_assert(env, !(env->me_flags & MDBX_NOTLS));
      r = thread_rthc_get(env->me_txkey);
      if (likely(r)) {
        mdbx_assert(env, r->mr_pid == env->me_pid);
        mdbx_assert(env, r->mr_tid == mdbx_thread_self());
      }
    } else {
      mdbx_assert(env, !env->me_lck || (env->me_flags & MDBX_NOTLS));
    }

    if (likely(r)) {
      if (unlikely(r->mr_pid != env->me_pid ||
                   r->mr_txnid.inconsistent < SAFE64_INVALID_THRESHOLD))
        return MDBX_BAD_RSLOT;
    } else if (env->me_lck) {
      unsigned slot, nreaders;
      const size_t tid = mdbx_thread_self();
      mdbx_assert(env, env->me_lck->mti_magic_and_version == MDBX_LOCK_MAGIC);
      mdbx_assert(env, env->me_lck->mti_os_and_format == MDBX_LOCK_FORMAT);

      rc = mdbx_rdt_lock(env);
      if (unlikely(MDBX_IS_ERROR(rc)))
        return rc;
      if (unlikely(env->me_flags & MDBX_FATAL_ERROR)) {
        mdbx_rdt_unlock(env);
        return MDBX_PANIC;
      }
#if defined(_WIN32) || defined(_WIN64)
      if (unlikely(!env->me_map)) {
        mdbx_rdt_unlock(env);
        return MDBX_EPERM;
      }
#endif /* Windows */
      rc = MDBX_SUCCESS;

      if (unlikely(env->me_live_reader != env->me_pid)) {
        rc = mdbx_rpid_set(env);
        if (unlikely(rc != MDBX_SUCCESS)) {
          mdbx_rdt_unlock(env);
          return rc;
        }
        env->me_live_reader = env->me_pid;
      }

      while (1) {
        nreaders = env->me_lck->mti_numreaders;
        for (slot = 0; slot < nreaders; slot++)
          if (env->me_lck->mti_readers[slot].mr_pid == 0)
            break;

        if (likely(slot < env->me_maxreaders))
          break;

        rc = mdbx_reader_check0(env, true, NULL);
        if (rc != MDBX_RESULT_TRUE) {
          mdbx_rdt_unlock(env);
          return (rc == MDBX_SUCCESS) ? MDBX_READERS_FULL : rc;
        }
      }

      r = &env->me_lck->mti_readers[slot];
      /* Claim the reader slot, carefully since other code
       * uses the reader table un-mutexed: First reset the
       * slot, next publish it in lck->mti_numreaders.  After
       * that, it is safe for mdbx_env_close() to touch it.
       * When it will be closed, we can finally claim it. */
      r->mr_pid = 0;
      safe64_reset(&r->mr_txnid, true);
      if (slot == nreaders)
        env->me_lck->mti_numreaders = ++nreaders;
      r->mr_tid = tid;
      r->mr_pid = env->me_pid;
      mdbx_rdt_unlock(env);

      if (likely(env->me_flags & MDBX_ENV_TXKEY)) {
        mdbx_assert(env, env->me_live_reader == env->me_pid);
        thread_rthc_set(env->me_txkey, r);
      }
    }

    while (1) {
      MDBX_meta *const meta = mdbx_meta_head(env);
      mdbx_jitter4testing(false);
      const txnid_t snap = mdbx_meta_txnid_fluid(env, meta);
      mdbx_jitter4testing(false);
      if (likely(r)) {
        safe64_reset(&r->mr_txnid, false);
        r->mr_snapshot_pages_used = meta->mm_geo.next;
        r->mr_snapshot_pages_retired = meta->mm_pages_retired;
        safe64_write(&r->mr_txnid, snap);
        mdbx_jitter4testing(false);
        mdbx_assert(env, r->mr_pid == mdbx_getpid());
        mdbx_assert(env, r->mr_tid == mdbx_thread_self());
        mdbx_assert(env, r->mr_txnid.inconsistent == snap);
        mdbx_compiler_barrier();
        env->me_lck->mti_readers_refresh_flag = true;
        mdbx_flush_noncoherent_cpu_writeback();
      }
      mdbx_jitter4testing(true);

      /* Snap the state from current meta-head */
      txn->mt_txnid = snap;
      txn->mt_geo = meta->mm_geo;
      memcpy(txn->mt_dbs, meta->mm_dbs, CORE_DBS * sizeof(MDBX_db));
      txn->mt_canary = meta->mm_canary;

      /* LY: Retry on a race, ITS#7970. */
      mdbx_compiler_barrier();
      if (likely(meta == mdbx_meta_head(env) &&
                 snap == mdbx_meta_txnid_fluid(env, meta) &&
                 snap >= *env->me_oldest)) {
        mdbx_jitter4testing(false);
        break;
      }
    }

    if (unlikely(txn->mt_txnid == 0 ||
                 txn->mt_txnid >= SAFE64_INVALID_THRESHOLD)) {
      mdbx_error("%s", "environment corrupted by died writer, must shutdown!");
      rc = MDBX_WANNA_RECOVERY;
      goto bailout;
    }
    mdbx_assert(env, txn->mt_txnid >= *env->me_oldest);
    txn->to.reader = r;
    txn->mt_dbxs = env->me_dbxs; /* mostly static anyway */
    mdbx_ensure(env, txn->mt_txnid >=
                         /* paranoia is appropriate here */ *env->me_oldest);
  } else {
    /* Not yet touching txn == env->me_txn0, it may be active */
    mdbx_jitter4testing(false);
    rc = mdbx_txn_lock(env, F_ISSET(flags, MDBX_TRYTXN));
    if (unlikely(rc))
      return rc;
    if (unlikely(env->me_flags & MDBX_FATAL_ERROR)) {
      mdbx_txn_unlock(env);
      return MDBX_PANIC;
    }
#if defined(_WIN32) || defined(_WIN64)
    if (unlikely(!env->me_map)) {
      mdbx_txn_unlock(env);
      return MDBX_EPERM;
    }
#endif /* Windows */

    mdbx_jitter4testing(false);
    MDBX_meta *meta = mdbx_meta_head(env);
    mdbx_jitter4testing(false);
    txn->mt_canary = meta->mm_canary;
    const txnid_t snap = mdbx_meta_txnid_stable(env, meta);
    txn->mt_txnid = safe64_txnid_next(snap);
    if (unlikely(txn->mt_txnid >= SAFE64_INVALID_THRESHOLD)) {
      mdbx_debug("%s", "txnid overflow!");
      rc = MDBX_TXN_FULL;
      goto bailout;
    }

    txn->mt_flags = flags;
    txn->mt_child = NULL;
    txn->tw.loose_pages = NULL;
    txn->tw.loose_count = 0;
    txn->tw.dirtyroom = MDBX_DPL_TXNFULL;
    txn->tw.dirtylist = env->me_dirtylist;
    mdbx_dpl_clear(txn->tw.dirtylist);
    MDBX_PNL_SIZE(txn->tw.retired_pages) = 0;
    txn->tw.spill_pages = NULL;
    txn->tw.last_reclaimed = 0;
    if (txn->tw.lifo_reclaimed)
      MDBX_PNL_SIZE(txn->tw.lifo_reclaimed) = 0;
    env->me_txn = txn;
    memcpy(txn->mt_dbiseqs, env->me_dbiseqs, env->me_maxdbs * sizeof(unsigned));
    /* Copy the DB info and flags */
    memcpy(txn->mt_dbs, meta->mm_dbs, CORE_DBS * sizeof(MDBX_db));
    /* Moved to here to avoid a data race in read TXNs */
    txn->mt_geo = meta->mm_geo;
    txn->tw.loose_refund_wl = txn->mt_next_pgno;
  }

  /* Setup db info */
  txn->mt_numdbs = env->me_numdbs;
  mdbx_compiler_barrier();
  for (unsigned i = CORE_DBS; i < txn->mt_numdbs; i++) {
    unsigned x = env->me_dbflags[i];
    txn->mt_dbs[i].md_flags = x & PERSISTENT_FLAGS;
    txn->mt_dbflags[i] =
        (x & MDBX_VALID) ? DB_VALID | DB_USRVALID | DB_STALE : 0;
  }
  txn->mt_dbflags[MAIN_DBI] = DB_VALID | DB_USRVALID;
  txn->mt_dbflags[FREE_DBI] = DB_VALID;

  if (unlikely(env->me_flags & MDBX_FATAL_ERROR)) {
    mdbx_warning("%s", "environment had fatal error, must shutdown!");
    rc = MDBX_PANIC;
  } else {
    const size_t size = pgno2bytes(env, txn->mt_end_pgno);
    if (unlikely(size > env->me_dxb_mmap.limit)) {
      if (txn->mt_geo.upper > MAX_PAGENO ||
          bytes2pgno(env, pgno2bytes(env, txn->mt_geo.upper)) !=
              txn->mt_geo.upper) {
        rc = MDBX_MAP_RESIZED;
        goto bailout;
      }
      rc = mdbx_mapresize(env, txn->mt_end_pgno, txn->mt_geo.upper);
      if (rc != MDBX_SUCCESS) {
        if (rc == MDBX_RESULT_TRUE)
          rc = MDBX_MAP_RESIZED;
        goto bailout;
      }
    }
    if (txn->mt_flags & MDBX_RDONLY) {
#if defined(_WIN32) || defined(_WIN64)
      if (size > env->me_dbgeo.lower && env->me_dbgeo.shrink) {
        txn->mt_flags |= MDBX_SHRINK_ALLOWED;
        mdbx_srwlock_AcquireShared(&env->me_remap_guard);
      }
#endif
    } else {
      env->me_dxb_mmap.current = size;
    }
#if defined(MDBX_USE_VALGRIND) || defined(__SANITIZE_ADDRESS__)
    mdbx_txn_valgrind(env, txn);
#endif
    txn->mt_owner = mdbx_thread_self();
    return MDBX_SUCCESS;
  }
bailout:
  mdbx_tassert(txn, rc != MDBX_SUCCESS);
  mdbx_txn_end(txn, MDBX_END_SLOT | MDBX_END_FAIL_BEGIN);
  return rc;
}

static __always_inline int check_txn(const MDBX_txn *txn, int bad_bits) {
  if (unlikely(!txn))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_signature != MDBX_MT_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(txn->mt_flags & bad_bits))
    return MDBX_BAD_TXN;

#if MDBX_TXN_CHECKOWNER
  if ((txn->mt_flags & MDBX_NOTLS) == 0 &&
      unlikely(txn->mt_owner != mdbx_thread_self()))
    return txn->mt_owner ? MDBX_THREAD_MISMATCH : MDBX_BAD_TXN;
#endif /* MDBX_TXN_CHECKOWNER */

  return MDBX_SUCCESS;
}

static __always_inline int check_txn_rw(const MDBX_txn *txn, int bad_bits) {
  if (unlikely(!txn))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_signature != MDBX_MT_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(txn->mt_flags & bad_bits))
    return MDBX_BAD_TXN;

  if (unlikely(F_ISSET(txn->mt_flags, MDBX_RDONLY)))
    return MDBX_EACCESS;

#if MDBX_TXN_CHECKOWNER
  if (unlikely(txn->mt_owner != mdbx_thread_self()))
    return txn->mt_owner ? MDBX_THREAD_MISMATCH : MDBX_BAD_TXN;
#endif /* MDBX_TXN_CHECKOWNER */

  return MDBX_SUCCESS;
}

int mdbx_txn_renew(MDBX_txn *txn) {
  int rc;

  if (unlikely(!txn))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_signature != MDBX_MT_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely((txn->mt_flags & MDBX_RDONLY) == 0))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_owner != 0))
    return MDBX_THREAD_MISMATCH;

  rc = mdbx_txn_renew0(txn, MDBX_RDONLY);
  if (rc == MDBX_SUCCESS) {
    txn->mt_owner = mdbx_thread_self();
    mdbx_debug("renew txn %" PRIaTXN "%c %p on env %p, root page %" PRIaPGNO
               "/%" PRIaPGNO,
               txn->mt_txnid, (txn->mt_flags & MDBX_RDONLY) ? 'r' : 'w',
               (void *)txn, (void *)txn->mt_env, txn->mt_dbs[MAIN_DBI].md_root,
               txn->mt_dbs[FREE_DBI].md_root);
  }
  return rc;
}

int mdbx_txn_begin(MDBX_env *env, MDBX_txn *parent, unsigned flags,
                   MDBX_txn **ret) {
  MDBX_txn *txn;
  int rc;
  unsigned size, tsize;

  if (unlikely(!ret))
    return MDBX_EINVAL;
  *ret = NULL;

  if (unlikely(!env))
    return MDBX_EINVAL;
  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

#if MDBX_TXN_CHECKPID
  if (unlikely(env->me_pid != mdbx_getpid()))
    env->me_flags |= MDBX_FATAL_ERROR;
#endif /* MDBX_TXN_CHECKPID */

  if (unlikely(env->me_flags & MDBX_FATAL_ERROR))
    return MDBX_PANIC;

#if !defined(_WIN32) && !defined(_WIN64)
  /* Don't check env->me_map until lock to
   * avoid race with re-mapping for shrinking */
  if (unlikely(!env->me_map))
    return MDBX_EPERM;
#endif /* Windows */

  if (unlikely(flags & ~MDBX_TXN_BEGIN_FLAGS))
    return MDBX_EINVAL;

  if (unlikely(env->me_flags & MDBX_RDONLY &
               ~flags)) /* write txn in RDONLY env */
    return MDBX_EACCESS;

  flags |= env->me_flags & MDBX_WRITEMAP;

  if (parent) {
    /* Nested transactions: Max 1 child, write txns only, no writemap */
    rc = check_txn_rw(parent, MDBX_RDONLY | MDBX_WRITEMAP | MDBX_TXN_BLOCKED);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;

#if defined(_WIN32) || defined(_WIN64)
    if (unlikely(!env->me_map))
      return MDBX_EPERM;
#endif /* Windows */

    flags |= parent->mt_flags &
             (MDBX_TXN_BEGIN_FLAGS | MDBX_SHRINK_ALLOWED | MDBX_TXN_SPILLS);
    /* Child txns save MDBX_pgstate and use own copy of cursors */
    size = env->me_maxdbs * (sizeof(MDBX_db) + sizeof(MDBX_cursor *) + 1);
    size += tsize = sizeof(MDBX_txn);
  } else if (flags & MDBX_RDONLY) {
    if (env->me_txn0 && unlikely(env->me_txn0->mt_owner == mdbx_thread_self()))
      return MDBX_BUSY;
    size = env->me_maxdbs * (sizeof(MDBX_db) + 1);
    size += tsize = sizeof(MDBX_txn);
  } else {
    /* Reuse preallocated write txn. However, do not touch it until
     * mdbx_txn_renew0() succeeds, since it currently may be active. */
    txn = env->me_txn0;
    if (unlikely(txn->mt_owner == mdbx_thread_self()))
      return MDBX_BUSY;
    goto renew;
  }
  if (unlikely((txn = mdbx_malloc(size)) == NULL)) {
    mdbx_debug("calloc: %s", "failed");
    return MDBX_ENOMEM;
  }
  memset(txn, 0, tsize);
  txn->mt_dbxs = env->me_dbxs; /* static */
  txn->mt_dbs = (MDBX_db *)((char *)txn + tsize);
  txn->mt_dbflags = (uint8_t *)txn + size - env->me_maxdbs;
  txn->mt_flags = flags;
  txn->mt_env = env;

  if (parent) {
    mdbx_tassert(txn, mdbx_dirtylist_check(parent));
    txn->mt_cursors = (MDBX_cursor **)(txn->mt_dbs + env->me_maxdbs);
    txn->mt_dbiseqs = parent->mt_dbiseqs;
    txn->tw.dirtylist = mdbx_malloc(sizeof(MDBX_DP) * (MDBX_DPL_TXNFULL + 1));
    txn->tw.reclaimed_pglist =
        mdbx_pnl_alloc(MDBX_PNL_ALLOCLEN(parent->tw.reclaimed_pglist));
    if (!txn->tw.dirtylist || !txn->tw.reclaimed_pglist) {
      mdbx_pnl_free(txn->tw.reclaimed_pglist);
      mdbx_free(txn->tw.dirtylist);
      mdbx_free(txn);
      return MDBX_ENOMEM;
    }
    mdbx_dpl_clear(txn->tw.dirtylist);
    memcpy(txn->tw.reclaimed_pglist, parent->tw.reclaimed_pglist,
           MDBX_PNL_SIZEOF(parent->tw.reclaimed_pglist));
    mdbx_assert(env, mdbx_pnl_check4assert(
                         txn->tw.reclaimed_pglist,
                         (txn->mt_next_pgno /* LY: intentional assigment here,
                                                   only for assertion */
                          = parent->mt_next_pgno)));

    txn->tw.last_reclaimed = parent->tw.last_reclaimed;
    if (parent->tw.lifo_reclaimed) {
      txn->tw.lifo_reclaimed = parent->tw.lifo_reclaimed;
      parent->tw.lifo_reclaimed =
          (void *)(intptr_t)MDBX_PNL_SIZE(parent->tw.lifo_reclaimed);
    }

    txn->tw.retired_pages = parent->tw.retired_pages;
    parent->tw.retired_pages =
        (void *)(intptr_t)MDBX_PNL_SIZE(parent->tw.retired_pages);

    txn->mt_txnid = parent->mt_txnid;
    txn->tw.dirtyroom = parent->tw.dirtyroom;
    txn->mt_geo = parent->mt_geo;
    txn->tw.loose_refund_wl = parent->tw.loose_refund_wl;
    txn->mt_canary = parent->mt_canary;
    parent->mt_flags |= MDBX_TXN_HAS_CHILD;
    parent->mt_child = txn;
    txn->mt_parent = parent;
    txn->mt_numdbs = parent->mt_numdbs;
    txn->mt_owner = parent->mt_owner;
    memcpy(txn->mt_dbs, parent->mt_dbs, txn->mt_numdbs * sizeof(MDBX_db));
    /* Copy parent's mt_dbflags, but clear DB_NEW */
    for (unsigned i = 0; i < txn->mt_numdbs; i++)
      txn->mt_dbflags[i] = parent->mt_dbflags[i] & ~(DB_FRESH | DB_CREAT);
    mdbx_tassert(parent,
                 parent->mt_parent ||
                     parent->tw.dirtyroom + parent->tw.dirtylist->length ==
                         MDBX_DPL_TXNFULL);
    env->me_txn = txn;
    rc = mdbx_cursor_shadow(parent, txn);
    if (unlikely(rc != MDBX_SUCCESS))
      mdbx_txn_end(txn, MDBX_END_FAIL_BEGINCHILD);
  } else { /* MDBX_RDONLY */
    txn->mt_dbiseqs = env->me_dbiseqs;
  renew:
    rc = mdbx_txn_renew0(txn, flags);
  }

  if (unlikely(rc != MDBX_SUCCESS)) {
    if (txn != env->me_txn0)
      mdbx_free(txn);
  } else {
    mdbx_assert(env, (txn->mt_flags &
                      ~(MDBX_RDONLY | MDBX_WRITEMAP | MDBX_SHRINK_ALLOWED |
                        MDBX_NOMETASYNC | MDBX_NOSYNC | MDBX_MAPASYNC)) == 0);
    txn->mt_signature = MDBX_MT_SIGNATURE;
    *ret = txn;
    mdbx_debug("begin txn %" PRIaTXN "%c %p on env %p, root page %" PRIaPGNO
               "/%" PRIaPGNO,
               txn->mt_txnid, (flags & MDBX_RDONLY) ? 'r' : 'w', (void *)txn,
               (void *)env, txn->mt_dbs[MAIN_DBI].md_root,
               txn->mt_dbs[FREE_DBI].md_root);
  }

  return rc;
}

int mdbx_txn_info(MDBX_txn *txn, MDBX_txn_info *info, int scan_rlt) {
  int rc = check_txn(txn, MDBX_TXN_BLOCKED - MDBX_TXN_HAS_CHILD);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!info))
    return MDBX_EINVAL;

  MDBX_env *const env = txn->mt_env;
#if MDBX_TXN_CHECKPID
  if (unlikely(env->me_pid != mdbx_getpid())) {
    env->me_flags |= MDBX_FATAL_ERROR;
    return MDBX_PANIC;
  }
#endif /* MDBX_TXN_CHECKPID */

  info->txn_id = txn->mt_txnid;
  info->txn_space_used = pgno2bytes(env, txn->mt_geo.next);

  if (txn->mt_flags & MDBX_RDONLY) {
    const MDBX_meta *head_meta;
    txnid_t head_txnid;
    uint64_t head_retired;
    do {
      /* fetch info from volatile head */
      head_meta = mdbx_meta_head(env);
      head_txnid = mdbx_meta_txnid_fluid(env, head_meta);
      head_retired = head_meta->mm_pages_retired;
      info->txn_space_limit_soft = pgno2bytes(env, head_meta->mm_geo.now);
      info->txn_space_limit_hard = pgno2bytes(env, head_meta->mm_geo.upper);
      info->txn_space_leftover =
          pgno2bytes(env, head_meta->mm_geo.now - head_meta->mm_geo.next);
      mdbx_compiler_barrier();
    } while (unlikely(head_meta != mdbx_meta_head(env) ||
                      head_txnid != mdbx_meta_txnid_fluid(env, head_meta)));

    info->txn_reader_lag = head_txnid - info->txn_id;
    info->txn_space_dirty = info->txn_space_retired = 0;
    if (txn->to.reader &&
        head_retired > txn->to.reader->mr_snapshot_pages_retired) {
      info->txn_space_dirty = info->txn_space_retired =
          pgno2bytes(env, (pgno_t)(head_retired -
                                   txn->to.reader->mr_snapshot_pages_retired));

      size_t retired_next_reader = 0;
      MDBX_lockinfo *const lck = env->me_lck;
      if (scan_rlt && info->txn_reader_lag > 1 && lck) {
        /* find next more recent reader */
        txnid_t next_reader = head_txnid;
        const unsigned snap_nreaders = lck->mti_numreaders;
        for (unsigned i = 0; i < snap_nreaders; ++i) {
        retry:
          if (lck->mti_readers[i].mr_pid) {
            mdbx_jitter4testing(true);
            const txnid_t snap_txnid =
                safe64_read(&lck->mti_readers[i].mr_txnid);
            const uint64_t snap_retired =
                lck->mti_readers[i].mr_snapshot_pages_retired;
            mdbx_compiler_barrier();
            if (unlikely(snap_retired !=
                         lck->mti_readers[i].mr_snapshot_pages_retired) ||
                snap_txnid != safe64_read(&lck->mti_readers[i].mr_txnid))
              goto retry;
            if (snap_txnid <= txn->mt_txnid) {
              retired_next_reader = 0;
              break;
            }
            if (snap_txnid < next_reader) {
              next_reader = snap_txnid;
              retired_next_reader = pgno2bytes(
                  env, (pgno_t)(snap_retired -
                                txn->to.reader->mr_snapshot_pages_retired));
            }
          }
        }
      }
      info->txn_space_dirty = retired_next_reader;
    }
  } else {
    info->txn_space_limit_soft = pgno2bytes(env, txn->mt_geo.now);
    info->txn_space_limit_hard = pgno2bytes(env, txn->mt_geo.upper);
    info->txn_space_retired = pgno2bytes(
        env, txn->mt_child ? (unsigned)(uintptr_t)txn->tw.retired_pages
                           : MDBX_PNL_SIZE(txn->tw.retired_pages));
    info->txn_space_leftover = pgno2bytes(env, txn->tw.dirtyroom);
    info->txn_space_dirty =
        pgno2bytes(env, MDBX_DPL_TXNFULL - txn->tw.dirtyroom);
    info->txn_reader_lag = INT64_MAX;
    MDBX_lockinfo *const lck = env->me_lck;
    if (scan_rlt && lck) {
      txnid_t oldest_snapshot = txn->mt_txnid;
      const unsigned snap_nreaders = lck->mti_numreaders;
      if (snap_nreaders) {
        oldest_snapshot = mdbx_find_oldest(txn);
        if (oldest_snapshot == txn->mt_txnid - 1) {
          /* check if there is at least one reader */
          bool exists = false;
          for (unsigned i = 0; i < snap_nreaders; ++i) {
            if (lck->mti_readers[i].mr_pid &&
                txn->mt_txnid > safe64_read(&lck->mti_readers[i].mr_txnid)) {
              exists = true;
              break;
            }
          }
          oldest_snapshot += !exists;
        }
      }
      info->txn_reader_lag = txn->mt_txnid - oldest_snapshot;
    }
  }

  return MDBX_SUCCESS;
}

MDBX_env *mdbx_txn_env(MDBX_txn *txn) {
  if (unlikely(!txn || txn->mt_signature != MDBX_MT_SIGNATURE ||
               txn->mt_env->me_signature != MDBX_ME_SIGNATURE))
    return NULL;
  return txn->mt_env;
}

uint64_t mdbx_txn_id(MDBX_txn *txn) {
  if (unlikely(!txn || txn->mt_signature != MDBX_MT_SIGNATURE))
    return 0;
  return txn->mt_txnid;
}

int mdbx_txn_flags(MDBX_txn *txn) {
  if (unlikely(!txn || txn->mt_signature != MDBX_MT_SIGNATURE))
    return -1;
  return txn->mt_flags;
}

/* Export or close DBI handles opened in this txn. */
static void mdbx_dbis_update(MDBX_txn *txn, int keep) {
  MDBX_dbi n = txn->mt_numdbs;
  if (n) {
    bool locked = false;
    MDBX_env *env = txn->mt_env;
    uint8_t *tdbflags = txn->mt_dbflags;

    for (unsigned i = n; --i >= CORE_DBS;) {
      if (likely((tdbflags[i] & DB_CREAT) == 0))
        continue;
      if (!locked) {
        mdbx_ensure(env,
                    mdbx_fastmutex_acquire(&env->me_dbi_lock) == MDBX_SUCCESS);
        locked = true;
      }
      if (keep) {
        env->me_dbflags[i] = txn->mt_dbs[i].md_flags | MDBX_VALID;
        mdbx_compiler_barrier();
        if (env->me_numdbs <= i)
          env->me_numdbs = i + 1;
      } else {
        char *ptr = env->me_dbxs[i].md_name.iov_base;
        if (ptr) {
          env->me_dbxs[i].md_name.iov_len = 0;
          mdbx_compiler_barrier();
          mdbx_assert(env, env->me_dbflags[i] == 0);
          env->me_dbiseqs[i]++;
          env->me_dbxs[i].md_name.iov_base = NULL;
          mdbx_free(ptr);
        }
      }
    }

    if (unlikely(locked))
      mdbx_ensure(env,
                  mdbx_fastmutex_release(&env->me_dbi_lock) == MDBX_SUCCESS);
  }
}

/* End a transaction, except successful commit of a nested transaction.
 * May be called twice for readonly txns: First reset it, then abort.
 * [in] txn   the transaction handle to end
 * [in] mode  why and how to end the transaction */
static int mdbx_txn_end(MDBX_txn *txn, unsigned mode) {
  MDBX_env *env = txn->mt_env;
  static const char *const names[] = MDBX_END_NAMES;

#if MDBX_TXN_CHECKPID
  if (unlikely(txn->mt_env->me_pid != mdbx_getpid())) {
    env->me_flags |= MDBX_FATAL_ERROR;
    return MDBX_PANIC;
  }
#endif /* MDBX_TXN_CHECKPID */

  mdbx_debug("%s txn %" PRIaTXN "%c %p on mdbenv %p, root page %" PRIaPGNO
             "/%" PRIaPGNO,
             names[mode & MDBX_END_OPMASK], txn->mt_txnid,
             (txn->mt_flags & MDBX_RDONLY) ? 'r' : 'w', (void *)txn,
             (void *)env, txn->mt_dbs[MAIN_DBI].md_root,
             txn->mt_dbs[FREE_DBI].md_root);

  mdbx_ensure(env, txn->mt_txnid >=
                       /* paranoia is appropriate here */ *env->me_oldest);

  int rc = MDBX_SUCCESS;
  if (F_ISSET(txn->mt_flags, MDBX_RDONLY)) {
    if (txn->to.reader) {
      MDBX_reader *slot = txn->to.reader;
      mdbx_assert(env, slot->mr_pid == env->me_pid);
      if (likely(!F_ISSET(txn->mt_flags, MDBX_TXN_FINISHED))) {
        mdbx_assert(env, txn->mt_txnid == slot->mr_txnid.inconsistent &&
                             slot->mr_txnid.inconsistent >=
                                 env->me_lck->mti_oldest_reader);
#if defined(MDBX_USE_VALGRIND) || defined(__SANITIZE_ADDRESS__)
        mdbx_txn_valgrind(env, nullptr);
#endif
        slot->mr_snapshot_pages_used = 0;
        safe64_reset(&slot->mr_txnid, false);
        env->me_lck->mti_readers_refresh_flag = true;
        mdbx_flush_noncoherent_cpu_writeback();
      } else {
        mdbx_assert(env, slot->mr_pid == env->me_pid);
        mdbx_assert(env,
                    slot->mr_txnid.inconsistent >= SAFE64_INVALID_THRESHOLD);
      }
      if (mode & MDBX_END_SLOT) {
        if ((env->me_flags & MDBX_ENV_TXKEY) == 0)
          slot->mr_pid = 0;
        txn->to.reader = NULL;
      }
    }
#if defined(_WIN32) || defined(_WIN64)
    if (txn->mt_flags & MDBX_SHRINK_ALLOWED)
      mdbx_srwlock_ReleaseShared(&env->me_remap_guard);
#endif
    txn->mt_numdbs = 0; /* prevent further DBI activity */
    txn->mt_flags = MDBX_RDONLY | MDBX_TXN_FINISHED;
    txn->mt_owner = 0;
  } else if (!F_ISSET(txn->mt_flags, MDBX_TXN_FINISHED)) {
#if defined(MDBX_USE_VALGRIND) || defined(__SANITIZE_ADDRESS__)
    if (txn == env->me_txn0)
      mdbx_txn_valgrind(env, nullptr);
#endif
    /* Export or close DBI handles created in this txn */
    mdbx_dbis_update(txn, mode & MDBX_END_UPDATE);
    if (!(mode & MDBX_END_EOTDONE)) /* !(already closed cursors) */
      mdbx_cursors_eot(txn, 0);
    if (!(env->me_flags & MDBX_WRITEMAP))
      mdbx_dlist_free(txn);

    txn->mt_flags = MDBX_TXN_FINISHED;
    txn->mt_owner = 0;
    env->me_txn = txn->mt_parent;
    if (txn == env->me_txn0) {
      mdbx_assert(env, txn->mt_parent == NULL);
      mdbx_pnl_shrink(&txn->tw.retired_pages);
      mdbx_pnl_shrink(&txn->tw.reclaimed_pglist);
      /* The writer mutex was locked in mdbx_txn_begin. */
      mdbx_txn_unlock(env);
    } else {
      mdbx_assert(env, txn->mt_parent != NULL);
      mdbx_assert(env, mdbx_pnl_check4assert(txn->tw.reclaimed_pglist,
                                             txn->mt_next_pgno));
      MDBX_txn *const parent = txn->mt_parent;
      env->me_txn->mt_child = NULL;
      env->me_txn->mt_flags &= ~MDBX_TXN_HAS_CHILD;
      mdbx_pnl_free(txn->tw.reclaimed_pglist);
      mdbx_pnl_free(txn->tw.spill_pages);

      if (txn->tw.lifo_reclaimed) {
        mdbx_assert(env, MDBX_PNL_SIZE(txn->tw.lifo_reclaimed) >=
                             (unsigned)(uintptr_t)parent->tw.lifo_reclaimed);
        MDBX_PNL_SIZE(txn->tw.lifo_reclaimed) =
            (unsigned)(uintptr_t)parent->tw.lifo_reclaimed;
        parent->tw.lifo_reclaimed = txn->tw.lifo_reclaimed;
      }

      if (txn->tw.retired_pages) {
        mdbx_assert(env, MDBX_PNL_SIZE(txn->tw.retired_pages) >=
                             (unsigned)(uintptr_t)parent->tw.retired_pages);
        MDBX_PNL_SIZE(txn->tw.retired_pages) =
            (unsigned)(uintptr_t)parent->tw.retired_pages;
        parent->tw.retired_pages = txn->tw.retired_pages;
      }

      mdbx_free(txn->tw.dirtylist);

      if (parent->mt_geo.upper != txn->mt_geo.upper ||
          parent->mt_geo.now != txn->mt_geo.now) {
        /* undo resize performed by child txn */
        rc = mdbx_mapresize(env, parent->mt_geo.now, parent->mt_geo.upper);
        if (rc == MDBX_RESULT_TRUE) {
          /* unable undo resize (it is regular for Windows),
           * therefore promote size changes from child to the parent txn */
          mdbx_notice("unable undo resize performed by child txn, promote to "
                      "the parent (%u->%u, %u->%u)",
                      txn->mt_geo.now, parent->mt_geo.now, txn->mt_geo.upper,
                      parent->mt_geo.upper);
          parent->mt_geo.now = txn->mt_geo.now;
          parent->mt_geo.upper = txn->mt_geo.upper;
          rc = MDBX_SUCCESS;
        } else if (unlikely(rc != MDBX_SUCCESS)) {
          mdbx_error("error %d while undo resize performed by child txn, fail "
                     "the parent",
                     rc);
          parent->mt_flags |= MDBX_TXN_ERROR;
          if (!env->me_dxb_mmap.address)
            env->me_flags |= MDBX_FATAL_ERROR;
        }
      }
    }
  }

  mdbx_assert(env, txn == env->me_txn0 || txn->mt_owner == 0);
  if ((mode & MDBX_END_FREE) != 0 && txn != env->me_txn0) {
    txn->mt_signature = 0;
    mdbx_free(txn);
  }

  return rc;
}

int mdbx_txn_reset(MDBX_txn *txn) {
  int rc = check_txn(txn, 0);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  /* This call is only valid for read-only txns */
  if (unlikely((txn->mt_flags & MDBX_RDONLY) == 0))
    return MDBX_EINVAL;

  /* LY: don't close DBI-handles */
  rc = mdbx_txn_end(txn, MDBX_END_RESET | MDBX_END_UPDATE);
  if (rc == MDBX_SUCCESS) {
    mdbx_tassert(txn, txn->mt_signature == MDBX_MT_SIGNATURE);
    mdbx_tassert(txn, txn->mt_owner == 0);
  }
  return rc;
}

int mdbx_txn_abort(MDBX_txn *txn) {
  int rc = check_txn(txn, 0);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (F_ISSET(txn->mt_flags, MDBX_RDONLY))
    /* LY: don't close DBI-handles */
    return mdbx_txn_end(txn, MDBX_END_ABORT | MDBX_END_UPDATE | MDBX_END_SLOT |
                                 MDBX_END_FREE);

  if (txn->mt_child)
    mdbx_txn_abort(txn->mt_child);

  return mdbx_txn_end(txn, MDBX_END_ABORT | MDBX_END_SLOT | MDBX_END_FREE);
}

static __inline int mdbx_backlog_size(MDBX_txn *txn) {
  int reclaimed = MDBX_PNL_SIZE(txn->tw.reclaimed_pglist);
  return reclaimed + txn->tw.loose_count;
}

static __inline int mdbx_backlog_extragap(MDBX_env *env) {
  /* LY: extra page(s) for b-tree rebalancing */
  return (env->me_flags & MDBX_LIFORECLAIM) ? 2 : 1;
}

/* LY: Prepare a backlog of pages to modify GC itself,
 * while reclaiming is prohibited. It should be enough to prevent search
 * in mdbx_page_alloc() during a deleting, when GC tree is unbalanced. */
static int mdbx_prep_backlog(MDBX_txn *txn, MDBX_cursor *mc) {
  /* LY: extra page(s) for b-tree rebalancing */
  const int extra =
      mdbx_backlog_extragap(txn->mt_env) +
      MDBX_PNL_SIZEOF(txn->tw.retired_pages) / txn->mt_env->me_maxkey_limit;

  if (mdbx_backlog_size(txn) < mc->mc_db->md_depth + extra) {
    mc->mc_flags &= ~C_RECLAIMING;
    int rc = mdbx_cursor_touch(mc);
    if (unlikely(rc))
      return rc;

    while (unlikely(mdbx_backlog_size(txn) < extra)) {
      rc = mdbx_page_alloc(mc, 1, NULL, MDBX_ALLOC_GC);
      if (unlikely(rc)) {
        if (rc != MDBX_NOTFOUND)
          return rc;
        break;
      }
    }
    mc->mc_flags |= C_RECLAIMING;
  }

  return MDBX_SUCCESS;
}

static void mdbx_prep_backlog_data(MDBX_txn *txn, MDBX_cursor *mc,
                                   size_t bytes) {
  const int wanna = (int)number_of_ovpages(txn->mt_env, bytes) +
                    mdbx_backlog_extragap(txn->mt_env);
  if (unlikely(wanna > mdbx_backlog_size(txn))) {
    mc->mc_flags &= ~C_RECLAIMING;
    do {
      if (mdbx_page_alloc(mc, 1, NULL, MDBX_ALLOC_GC) != MDBX_SUCCESS)
        break;
    } while (wanna > mdbx_backlog_size(txn));
    mc->mc_flags |= C_RECLAIMING;
  }
}

/* Count all the pages in each DB and in the freelist and make sure
 * it matches the actual number of pages being used.
 * All named DBs must be open for a correct count. */
static __cold int mdbx_audit_ex(MDBX_txn *txn, unsigned retired_stored,
                                bool dont_filter_gc) {
  pgno_t pending = 0;
  if ((txn->mt_flags & MDBX_RDONLY) == 0) {
    pending = txn->tw.loose_count + MDBX_PNL_SIZE(txn->tw.reclaimed_pglist) +
              (MDBX_PNL_SIZE(txn->tw.retired_pages) - retired_stored) +
              txn->tw.retired2parent_count;
    for (MDBX_txn *parent = txn->mt_parent; parent; parent = parent->mt_parent)
      pending += parent->tw.loose_count;
  }

  MDBX_cursor_couple cx;
  int rc = mdbx_cursor_init(&cx.outer, txn, FREE_DBI);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  pgno_t freecount = 0;
  MDBX_val key, data;
  while ((rc = mdbx_cursor_get(&cx.outer, &key, &data, MDBX_NEXT)) == 0) {
    if (!dont_filter_gc) {
      if (unlikely(key.iov_len != sizeof(txnid_t)))
        return MDBX_CORRUPTED;
      txnid_t id = unaligned_peek_u64(4, key.iov_base);
      if (txn->tw.lifo_reclaimed) {
        for (unsigned i = 1; i <= MDBX_PNL_SIZE(txn->tw.lifo_reclaimed); ++i)
          if (id == txn->tw.lifo_reclaimed[i])
            goto skip;
      } else if (id <= txn->tw.last_reclaimed)
        goto skip;
    }

    freecount += *(pgno_t *)data.iov_base;
  skip:;
  }
  mdbx_tassert(txn, rc == MDBX_NOTFOUND);

  for (MDBX_dbi i = FREE_DBI; i < txn->mt_numdbs; i++)
    txn->mt_dbflags[i] &= ~DB_AUDITED;

  pgno_t count = 0;
  for (MDBX_dbi i = FREE_DBI; i <= MAIN_DBI; i++) {
    if (!(txn->mt_dbflags[i] & DB_VALID))
      continue;
    rc = mdbx_cursor_init(&cx.outer, txn, i);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    txn->mt_dbflags[i] |= DB_AUDITED;
    if (txn->mt_dbs[i].md_root == P_INVALID)
      continue;
    count += txn->mt_dbs[i].md_branch_pages + txn->mt_dbs[i].md_leaf_pages +
             txn->mt_dbs[i].md_overflow_pages;

    if (i != MAIN_DBI)
      continue;
    rc = mdbx_page_search(&cx.outer, NULL, MDBX_PS_FIRST);
    while (rc == MDBX_SUCCESS) {
      MDBX_page *mp = cx.outer.mc_pg[cx.outer.mc_top];
      for (unsigned j = 0; j < page_numkeys(mp); j++) {
        MDBX_node *node = page_node(mp, j);
        if (node_flags(node) == F_SUBDATA) {
          if (unlikely(node_ds(node) < sizeof(MDBX_db)))
            return MDBX_CORRUPTED;
          MDBX_db db_copy, *db;
          memcpy(db = &db_copy, node_data(node), sizeof(db_copy));
          if ((txn->mt_flags & MDBX_RDONLY) == 0) {
            for (MDBX_dbi k = txn->mt_numdbs; --k > MAIN_DBI;) {
              if ((txn->mt_dbflags[k] & DB_VALID) &&
                  /* txn->mt_dbxs[k].md_name.iov_len > 0 && */
                  node_ks(node) == txn->mt_dbxs[k].md_name.iov_len &&
                  memcmp(node_key(node), txn->mt_dbxs[k].md_name.iov_base,
                         node_ks(node)) == 0) {
                txn->mt_dbflags[k] |= DB_AUDITED;
                if (txn->mt_dbflags[k] & DB_DIRTY) {
                  mdbx_tassert(txn, (txn->mt_dbflags[k] & DB_STALE) == 0);
                  db = txn->mt_dbs + k;
                }
                break;
              }
            }
          }
          count +=
              db->md_branch_pages + db->md_leaf_pages + db->md_overflow_pages;
        }
      }
      rc = mdbx_cursor_sibling(&cx.outer, 1);
    }
    mdbx_tassert(txn, rc == MDBX_NOTFOUND);
  }

  for (MDBX_dbi i = FREE_DBI; i < txn->mt_numdbs; i++) {
    if ((txn->mt_dbflags[i] & (DB_VALID | DB_AUDITED | DB_STALE)) != DB_VALID)
      continue;
    if (F_ISSET(txn->mt_dbflags[i], DB_DIRTY | DB_CREAT)) {
      count += txn->mt_dbs[i].md_branch_pages + txn->mt_dbs[i].md_leaf_pages +
               txn->mt_dbs[i].md_overflow_pages;
    } else {
      mdbx_warning("audit %s@%" PRIaTXN
                   ": unable account dbi %d / \"%*s\", state 0x%02x",
                   txn->mt_parent ? "nested-" : "", txn->mt_txnid, i,
                   (int)txn->mt_dbxs[i].md_name.iov_len,
                   (const char *)txn->mt_dbxs[i].md_name.iov_base,
                   txn->mt_dbflags[i]);
    }
  }

  if (pending + freecount + count + NUM_METAS == txn->mt_next_pgno)
    return MDBX_SUCCESS;

  if ((txn->mt_flags & MDBX_RDONLY) == 0)
    mdbx_error("audit @%" PRIaTXN ": %u(pending) = %u(loose-count) + "
               "%u(reclaimed-list) + %u(retired-pending) - %u(retired-stored) "
               "+ %u(retired2parent)",
               txn->mt_txnid, pending, txn->tw.loose_count,
               MDBX_PNL_SIZE(txn->tw.reclaimed_pglist),
               txn->tw.retired_pages ? MDBX_PNL_SIZE(txn->tw.retired_pages) : 0,
               retired_stored, txn->tw.retired2parent_count);
  mdbx_error("audit @%" PRIaTXN ": %" PRIaPGNO "(pending) + %" PRIaPGNO
             "(free) + %" PRIaPGNO "(count) = %" PRIaPGNO
             "(total) <> %" PRIaPGNO "(next-pgno)",
             txn->mt_txnid, pending, freecount, count + NUM_METAS,
             pending + freecount + count + NUM_METAS, txn->mt_next_pgno);
  return MDBX_PROBLEM;
}

static __inline void clean_reserved_gc_pnl(MDBX_env *env, MDBX_val pnl) {
  /* PNL is initially empty, zero out at least the length */
  memset(pnl.iov_base, 0, sizeof(pgno_t));
  if ((env->me_flags & (MDBX_WRITEMAP | MDBX_NOMEMINIT)) == 0)
    /* zero out to avoid leaking values from uninitialized malloc'ed memory
     * to the file in non-writemap mode if length of the saving page-list
     * was changed during space reservation. */
    memset(pnl.iov_base, 0, pnl.iov_len);
}

/* Cleanup reclaimed GC records, than save the retired-list as of this
 * transaction to GC (aka freeDB). This recursive changes the reclaimed-list
 * loose-list and retired-list. Keep trying until it stabilizes. */
static int mdbx_update_gc(MDBX_txn *txn) {
  /* txn->tw.reclaimed_pglist[] can grow and shrink during this call.
   * txn->tw.last_reclaimed and txn->tw.retired_pages[] can only grow.
   * Page numbers cannot disappear from txn->tw.retired_pages[]. */
  MDBX_env *const env = txn->mt_env;
  const bool lifo = (env->me_flags & MDBX_LIFORECLAIM) != 0;
  const char *dbg_prefix_mode = lifo ? "    lifo" : "    fifo";
  (void)dbg_prefix_mode;
  mdbx_trace("\n>>> @%" PRIaTXN, txn->mt_txnid);

  unsigned retired_stored = 0, loop = 0;
  MDBX_cursor mc;
  int rc = mdbx_cursor_init(&mc, txn, FREE_DBI);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout_notracking;

  mc.mc_flags |= C_RECLAIMING;
  mc.mc_next = txn->mt_cursors[FREE_DBI];
  txn->mt_cursors[FREE_DBI] = &mc;

retry:
  mdbx_trace("%s", " >> restart");
  mdbx_tassert(
      txn, mdbx_pnl_check4assert(txn->tw.reclaimed_pglist, txn->mt_next_pgno));
  mdbx_tassert(txn, mdbx_dirtylist_check(txn));
  mdbx_tassert(txn, txn->tw.dirtyroom + txn->tw.dirtylist->length ==
                        MDBX_DPL_TXNFULL);
  if (unlikely(/* paranoia */ ++loop > 42)) {
    mdbx_error("too more loops %u, bailout", loop);
    rc = MDBX_PROBLEM;
    goto bailout;
  }

  unsigned settled = 0, cleaned_gc_slot = 0, reused_gc_slot = 0,
           filled_gc_slot = ~0u;
  txnid_t cleaned_gc_id = 0, gc_rid = txn->tw.last_reclaimed;
  while (true) {
    /* Come back here after each Put() in case retired-list changed */
    MDBX_val key, data;
    mdbx_trace("%s", " >> continue");

    mdbx_tassert(txn, mdbx_pnl_check4assert(txn->tw.reclaimed_pglist,
                                            txn->mt_next_pgno));
    if (txn->tw.lifo_reclaimed) {
      if (cleaned_gc_slot < MDBX_PNL_SIZE(txn->tw.lifo_reclaimed)) {
        settled = 0;
        cleaned_gc_slot = 0;
        reused_gc_slot = 0;
        filled_gc_slot = ~0u;
        /* LY: cleanup reclaimed records. */
        do {
          cleaned_gc_id = txn->tw.lifo_reclaimed[++cleaned_gc_slot];
          mdbx_tassert(txn,
                       cleaned_gc_slot > 0 && cleaned_gc_id < *env->me_oldest);
          key.iov_base = &cleaned_gc_id;
          key.iov_len = sizeof(cleaned_gc_id);
          rc = mdbx_cursor_get(&mc, &key, NULL, MDBX_SET);
          if (rc == MDBX_NOTFOUND)
            continue;
          if (unlikely(rc != MDBX_SUCCESS))
            goto bailout;
          rc = mdbx_prep_backlog(txn, &mc);
          if (unlikely(rc != MDBX_SUCCESS))
            goto bailout;
          mdbx_tassert(txn, cleaned_gc_id < *env->me_oldest);
          mdbx_trace("%s.cleanup-reclaimed-id [%u]%" PRIaTXN, dbg_prefix_mode,
                     cleaned_gc_slot, cleaned_gc_id);
          rc = mdbx_cursor_del(&mc, 0);
          if (unlikely(rc != MDBX_SUCCESS))
            goto bailout;
        } while (cleaned_gc_slot < MDBX_PNL_SIZE(txn->tw.lifo_reclaimed));
        mdbx_txl_sort(txn->tw.lifo_reclaimed);
        gc_rid = MDBX_PNL_LAST(txn->tw.lifo_reclaimed);
      }
    } else {
      /* If using records from GC which we have not yet deleted,
       * now delete them and any we reserved for me_reclaimed_pglist. */
      while (cleaned_gc_id <= txn->tw.last_reclaimed) {
        gc_rid = cleaned_gc_id;
        settled = 0;
        rc = mdbx_cursor_first(&mc, &key, NULL);
        if (unlikely(rc != MDBX_SUCCESS)) {
          if (rc == MDBX_NOTFOUND)
            break;
          goto bailout;
        }
        if (unlikely(key.iov_len != sizeof(txnid_t))) {
          rc = MDBX_CORRUPTED;
          goto bailout;
        }
        cleaned_gc_id = unaligned_peek_u64(4, key.iov_base);
        if (unlikely(cleaned_gc_id < 1 ||
                     cleaned_gc_id >= SAFE64_INVALID_THRESHOLD)) {
          rc = MDBX_CORRUPTED;
          goto bailout;
        }
        if (cleaned_gc_id > txn->tw.last_reclaimed)
          break;
        if (cleaned_gc_id < txn->tw.last_reclaimed) {
          rc = mdbx_prep_backlog(txn, &mc);
          if (unlikely(rc != MDBX_SUCCESS))
            goto bailout;
        }
        mdbx_tassert(txn, cleaned_gc_id <= txn->tw.last_reclaimed);
        mdbx_tassert(txn, cleaned_gc_id < *env->me_oldest);
        mdbx_trace("%s.cleanup-reclaimed-id %" PRIaTXN, dbg_prefix_mode,
                   cleaned_gc_id);
        rc = mdbx_cursor_del(&mc, 0);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
      }
    }

    mdbx_tassert(txn, mdbx_pnl_check4assert(txn->tw.reclaimed_pglist,
                                            txn->mt_next_pgno));
    mdbx_tassert(txn, mdbx_dirtylist_check(txn));
    mdbx_tassert(txn, txn->tw.dirtyroom + txn->tw.dirtylist->length ==
                          MDBX_DPL_TXNFULL);
    if (mdbx_audit_enabled()) {
      rc = mdbx_audit_ex(txn, retired_stored, false);
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;
    }

    /* return suitable into unallocated space */
    if (mdbx_refund(txn)) {
      mdbx_tassert(txn, mdbx_pnl_check4assert(txn->tw.reclaimed_pglist,
                                              txn->mt_next_pgno));
      if (mdbx_audit_enabled()) {
        rc = mdbx_audit_ex(txn, retired_stored, false);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
      }
    }

    /* handle loose pages - put ones into the reclaimed- or retired-list */
    if (txn->tw.loose_pages) {
      /* Return loose page numbers to me_reclaimed_pglist,
       * though usually none are left at this point.
       * The pages themselves remain in dirtylist. */
      if (unlikely(!txn->tw.lifo_reclaimed && txn->tw.last_reclaimed < 1)) {
        if (txn->tw.loose_count > 0) {
          /* Put loose page numbers in tw.retired_pages,
           * since unable to return them to me_reclaimed_pglist. */
          if (unlikely((rc = mdbx_pnl_need(&txn->tw.retired_pages,
                                           txn->tw.loose_count)) != 0))
            goto bailout;
          for (MDBX_page *mp = txn->tw.loose_pages; mp; mp = mp->mp_next)
            mdbx_pnl_xappend(txn->tw.retired_pages, mp->mp_pgno);
          mdbx_trace("%s: append %u loose-pages to retired-pages",
                     dbg_prefix_mode, txn->tw.loose_count);
        }
      } else {
        /* Room for loose pages + temp PNL with same */
        rc = mdbx_pnl_need(&txn->tw.reclaimed_pglist,
                           2 * txn->tw.loose_count + 2);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
        MDBX_PNL loose = txn->tw.reclaimed_pglist +
                         MDBX_PNL_ALLOCLEN(txn->tw.reclaimed_pglist) -
                         txn->tw.loose_count - 1;
        unsigned count = 0;
        for (MDBX_page *mp = txn->tw.loose_pages; mp; mp = mp->mp_next) {
          mdbx_tassert(txn, mp->mp_flags == (P_LOOSE | P_DIRTY));
          loose[++count] = mp->mp_pgno;
        }
        mdbx_tassert(txn, count == txn->tw.loose_count);
        MDBX_PNL_SIZE(loose) = count;
        mdbx_pnl_sort(loose);
        mdbx_pnl_xmerge(txn->tw.reclaimed_pglist, loose);
        mdbx_trace("%s: append %u loose-pages to reclaimed-pages",
                   dbg_prefix_mode, txn->tw.loose_count);
      }

      /* filter-out list of dirty-pages from loose-pages */
      const MDBX_DPL dl = txn->tw.dirtylist;
      unsigned w = 0;
      for (unsigned r = w; ++r <= dl->length;) {
        MDBX_page *dp = dl[r].ptr;
        mdbx_tassert(txn, (dp->mp_flags & P_DIRTY));
        mdbx_tassert(txn, dl[r].pgno + (IS_OVERFLOW(dp) ? dp->mp_pages : 1) <=
                              txn->mt_next_pgno);
        if ((dp->mp_flags & P_LOOSE) == 0) {
          if (++w != r)
            dl[w] = dl[r];
        } else {
          mdbx_tassert(txn, dp->mp_flags == (P_LOOSE | P_DIRTY));
          if ((env->me_flags & MDBX_WRITEMAP) == 0)
            mdbx_dpage_free(env, dp, 1);
        }
      }
      mdbx_trace("%s: filtered-out loose-pages from %u -> %u dirty-pages",
                 dbg_prefix_mode, dl->length, w);
      mdbx_tassert(txn, txn->tw.loose_count == dl->length - w);
      dl->length = w;
      dl->sorted = 0;
      txn->tw.dirtyroom += txn->tw.loose_count;
      txn->tw.loose_pages = NULL;
      txn->tw.loose_count = 0;
    }

    /* handle retired-list - store ones into single gc-record */
    if (retired_stored < MDBX_PNL_SIZE(txn->tw.retired_pages)) {
      if (unlikely(!retired_stored)) {
        /* Make sure last page of GC is touched and on retired-list */
        mc.mc_flags &= ~C_RECLAIMING;
        rc = mdbx_page_search(&mc, NULL, MDBX_PS_LAST | MDBX_PS_MODIFY);
        mc.mc_flags |= C_RECLAIMING;
        if (unlikely(rc != MDBX_SUCCESS) && rc != MDBX_NOTFOUND)
          goto bailout;
      }
      /* Write to last page of GC */
      key.iov_len = sizeof(txn->mt_txnid);
      key.iov_base = &txn->mt_txnid;
      do {
        data.iov_len = MDBX_PNL_SIZEOF(txn->tw.retired_pages);
        mdbx_prep_backlog_data(txn, &mc, data.iov_len);
        rc = mdbx_cursor_put(&mc, &key, &data, MDBX_RESERVE);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
        /* Retry if tw.retired_pages[] grew during the Put() */
      } while (data.iov_len < MDBX_PNL_SIZEOF(txn->tw.retired_pages));

      retired_stored = (unsigned)MDBX_PNL_SIZE(txn->tw.retired_pages);
      mdbx_pnl_sort(txn->tw.retired_pages);
      mdbx_assert(env, data.iov_len == MDBX_PNL_SIZEOF(txn->tw.retired_pages));
      memcpy(data.iov_base, txn->tw.retired_pages, data.iov_len);

      mdbx_trace("%s.put-retired #%u @ %" PRIaTXN, dbg_prefix_mode,
                 retired_stored, txn->mt_txnid);

      if (mdbx_log_enabled(MDBX_LOG_EXTRA)) {
        unsigned i = retired_stored;
        mdbx_debug_extra("PNL write txn %" PRIaTXN " root %" PRIaPGNO
                         " num %u, PNL",
                         txn->mt_txnid, txn->mt_dbs[FREE_DBI].md_root, i);
        for (; i; i--)
          mdbx_debug_extra_print(" %" PRIaPGNO, txn->tw.retired_pages[i]);
        mdbx_debug_extra_print("%s", "\n");
      }
      continue;
    }

    /* handle reclaimed and loost pages - merge and store both into gc */
    mdbx_tassert(txn, mdbx_pnl_check4assert(txn->tw.reclaimed_pglist,
                                            txn->mt_next_pgno));
    mdbx_tassert(txn, txn->tw.loose_count == 0);

    mdbx_trace("%s", " >> reserving");
    if (mdbx_audit_enabled()) {
      rc = mdbx_audit_ex(txn, retired_stored, false);
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;
    }
    const unsigned amount = (unsigned)MDBX_PNL_SIZE(txn->tw.reclaimed_pglist);
    const unsigned left = amount - settled;
    mdbx_trace("%s: amount %u, settled %d, left %d, lifo-reclaimed-slots %u, "
               "reused-gc-slots %u",
               dbg_prefix_mode, amount, settled, (int)left,
               txn->tw.lifo_reclaimed
                   ? (unsigned)MDBX_PNL_SIZE(txn->tw.lifo_reclaimed)
                   : 0,
               reused_gc_slot);
    if (0 >= (int)left)
      break;

    const unsigned prefer_max_scatter = 257;
    txnid_t reservation_gc_id;
    if (lifo) {
      const unsigned lifo_len =
          txn->tw.lifo_reclaimed
              ? (unsigned)MDBX_PNL_SIZE(txn->tw.lifo_reclaimed)
              : 0;
      if (lifo_len < prefer_max_scatter &&
          left > (lifo_len - reused_gc_slot) * env->me_maxgc_ov1page) {
        /* LY: need just a txn-id for save page list. */
        mc.mc_flags &= ~C_RECLAIMING;
        rc = mdbx_page_alloc(&mc, 0, NULL, MDBX_ALLOC_GC | MDBX_ALLOC_KICK);
        mc.mc_flags |= C_RECLAIMING;
        if (likely(rc == MDBX_SUCCESS)) {
          /* LY: ok, reclaimed from GC. */
          mdbx_trace("%s: took @%" PRIaTXN " from GC, continue",
                     dbg_prefix_mode, MDBX_PNL_LAST(txn->tw.lifo_reclaimed));
          continue;
        }
        if (unlikely(rc != MDBX_NOTFOUND))
          /* LY: other troubles... */
          goto bailout;

        /* LY: GC is empty, will look any free txn-id in high2low order. */
        if (unlikely(gc_rid == 0)) {
          mdbx_tassert(txn, txn->tw.last_reclaimed == 0);
          txn->tw.last_reclaimed = gc_rid = mdbx_find_oldest(txn) - 1;
          if (txn->tw.lifo_reclaimed == nullptr) {
            txn->tw.lifo_reclaimed = mdbx_txl_alloc();
            if (unlikely(!txn->tw.lifo_reclaimed)) {
              rc = MDBX_ENOMEM;
              goto bailout;
            }
          }
        }

        mdbx_tassert(txn, txn->tw.lifo_reclaimed != nullptr);
        rc = MDBX_RESULT_TRUE;
        do {
          if (unlikely(gc_rid <= 1)) {
            if (unlikely(MDBX_PNL_SIZE(txn->tw.lifo_reclaimed) <=
                         reused_gc_slot)) {
              mdbx_notice("** restart: reserve depleted (reused_gc_slot %u >= "
                          "lifo_reclaimed %u" PRIaTXN,
                          reused_gc_slot,
                          (unsigned)MDBX_PNL_SIZE(txn->tw.lifo_reclaimed));
              goto retry;
            }
            break;
          }

          mdbx_tassert(txn, gc_rid > 1 && gc_rid < SAFE64_INVALID_THRESHOLD);
          rc = mdbx_txl_append(&txn->tw.lifo_reclaimed, --gc_rid);
          if (unlikely(rc != MDBX_SUCCESS))
            goto bailout;
          cleaned_gc_slot += 1 /* mark GC cleanup is not needed. */;

          mdbx_trace("%s: append @%" PRIaTXN
                     " to lifo-reclaimed, cleaned-gc-slot = %u",
                     dbg_prefix_mode, gc_rid, cleaned_gc_slot);
        } while (gc_rid &&
                 MDBX_PNL_SIZE(txn->tw.lifo_reclaimed) < prefer_max_scatter &&
                 left > ((unsigned)MDBX_PNL_SIZE(txn->tw.lifo_reclaimed) -
                         reused_gc_slot) *
                            env->me_maxgc_ov1page);
        if (reused_gc_slot && rc != MDBX_RESULT_TRUE) {
          mdbx_trace("%s: restart innter-loop since reused_gc_slot %u > 0",
                     dbg_prefix_mode, reused_gc_slot);
          continue;
        }
      }

      const unsigned i =
          (unsigned)MDBX_PNL_SIZE(txn->tw.lifo_reclaimed) - reused_gc_slot;
      mdbx_tassert(txn, i > 0 && i <= MDBX_PNL_SIZE(txn->tw.lifo_reclaimed));
      reservation_gc_id = txn->tw.lifo_reclaimed[i];
      mdbx_trace("%s: take @%" PRIaTXN " from lifo-reclaimed[%u]",
                 dbg_prefix_mode, reservation_gc_id, i);
    } else {
      mdbx_tassert(txn, txn->tw.lifo_reclaimed == NULL);
      if (unlikely(gc_rid == 0)) {
        gc_rid = mdbx_find_oldest(txn) - 1;
        rc = mdbx_cursor_get(&mc, &key, NULL, MDBX_FIRST);
        if (rc == MDBX_SUCCESS) {
          if (unlikely(key.iov_len != sizeof(txnid_t))) {
            rc = MDBX_CORRUPTED;
            goto bailout;
          }
          txnid_t gc_first = unaligned_peek_u64(4, key.iov_base);
          if (unlikely(gc_first < 1 || gc_first >= SAFE64_INVALID_THRESHOLD)) {
            rc = MDBX_CORRUPTED;
            goto bailout;
          }
          if (gc_rid >= gc_first)
            gc_rid = gc_first - 1;
          if (unlikely(gc_rid == 0)) {
            mdbx_error("%s", "** no GC tail-space to store");
            rc = MDBX_PROBLEM;
            goto bailout;
          }
        } else if (rc != MDBX_NOTFOUND)
          goto bailout;
        mdbx_tassert(txn, txn->tw.last_reclaimed == 0);
        txn->tw.last_reclaimed = gc_rid;
      }
      reservation_gc_id = gc_rid--;
      mdbx_trace("%s: take @%" PRIaTXN " from head-gc-id", dbg_prefix_mode,
                 reservation_gc_id);
    }
    ++reused_gc_slot;

    unsigned chunk = left;
    if (unlikely(chunk > env->me_maxgc_ov1page)) {
      const unsigned avail_gc_slots =
          txn->tw.lifo_reclaimed
              ? (unsigned)MDBX_PNL_SIZE(txn->tw.lifo_reclaimed) -
                    reused_gc_slot + 1
              : (gc_rid < INT16_MAX) ? (unsigned)gc_rid : INT16_MAX;
      if (avail_gc_slots > 1) {
        if (chunk < env->me_maxgc_ov1page * 2)
          chunk /= 2;
        else {
          const unsigned threshold =
              env->me_maxgc_ov1page * ((avail_gc_slots < prefer_max_scatter)
                                           ? avail_gc_slots
                                           : prefer_max_scatter);
          if (left < threshold)
            chunk = env->me_maxgc_ov1page;
          else {
            const unsigned tail = left - threshold + env->me_maxgc_ov1page + 1;
            unsigned span = 1;
            unsigned avail = (unsigned)((pgno2bytes(env, span) - PAGEHDRSZ) /
                                        sizeof(pgno_t)) /*- 1 + span */;
            if (tail > avail) {
              for (unsigned i = amount - span; i > 0; --i) {
                if (MDBX_PNL_ASCENDING
                        ? (txn->tw.reclaimed_pglist[i] + span)
                        : (txn->tw.reclaimed_pglist[i] - span) ==
                              txn->tw.reclaimed_pglist[i + span]) {
                  span += 1;
                  avail = (unsigned)((pgno2bytes(env, span) - PAGEHDRSZ) /
                                     sizeof(pgno_t)) -
                          1 + span;
                  if (avail >= tail)
                    break;
                }
              }
            }

            chunk = (avail >= tail) ? tail - span
                                    : (avail_gc_slots > 3 &&
                                       reused_gc_slot < prefer_max_scatter - 3)
                                          ? avail - span
                                          : tail;
          }
        }
      }
    }
    mdbx_tassert(txn, chunk > 0);

    mdbx_trace("%s: rc_rid %" PRIaTXN ", reused_gc_slot %u, reservation-id "
               "%" PRIaTXN,
               dbg_prefix_mode, gc_rid, reused_gc_slot, reservation_gc_id);

    mdbx_trace("%s: chunk %u, gc-per-ovpage %u", dbg_prefix_mode, chunk,
               env->me_maxgc_ov1page);

    mdbx_tassert(txn, reservation_gc_id < *env->me_oldest);
    if (unlikely(reservation_gc_id < 1 ||
                 reservation_gc_id >= *env->me_oldest)) {
      mdbx_error("%s", "** internal error (reservation_gc_id)");
      rc = MDBX_PROBLEM;
      goto bailout;
    }

    key.iov_len = sizeof(reservation_gc_id);
    key.iov_base = &reservation_gc_id;
    data.iov_len = (chunk + 1) * sizeof(pgno_t);
    mdbx_trace("%s.reserve: %u [%u...%u] @%" PRIaTXN, dbg_prefix_mode, chunk,
               settled + 1, settled + chunk + 1, reservation_gc_id);
    mdbx_prep_backlog_data(txn, &mc, data.iov_len);
    rc = mdbx_cursor_put(&mc, &key, &data, MDBX_RESERVE | MDBX_NOOVERWRITE);
    mdbx_tassert(txn, mdbx_pnl_check4assert(txn->tw.reclaimed_pglist,
                                            txn->mt_next_pgno));
    if (unlikely(rc != MDBX_SUCCESS))
      goto bailout;

    clean_reserved_gc_pnl(env, data);
    settled += chunk;
    mdbx_trace("%s.settled %u (+%u), continue", dbg_prefix_mode, settled,
               chunk);

    if (txn->tw.lifo_reclaimed &&
        unlikely(amount < MDBX_PNL_SIZE(txn->tw.reclaimed_pglist))) {
      mdbx_notice("** restart: reclaimed-list growth %u -> %u", amount,
                  (unsigned)MDBX_PNL_SIZE(txn->tw.reclaimed_pglist));
      goto retry;
    }

    continue;
  }

  mdbx_tassert(
      txn,
      cleaned_gc_slot ==
          (txn->tw.lifo_reclaimed ? MDBX_PNL_SIZE(txn->tw.lifo_reclaimed) : 0));

  mdbx_trace("%s", " >> filling");
  /* Fill in the reserved records */
  filled_gc_slot =
      txn->tw.lifo_reclaimed
          ? (unsigned)MDBX_PNL_SIZE(txn->tw.lifo_reclaimed) - reused_gc_slot
          : reused_gc_slot;
  rc = MDBX_SUCCESS;
  mdbx_tassert(
      txn, mdbx_pnl_check4assert(txn->tw.reclaimed_pglist, txn->mt_next_pgno));
  mdbx_tassert(txn, mdbx_dirtylist_check(txn));
  if (MDBX_PNL_SIZE(txn->tw.reclaimed_pglist)) {
    MDBX_val key, data;
    key.iov_len = data.iov_len = 0; /* avoid MSVC warning */
    key.iov_base = data.iov_base = NULL;

    const unsigned amount = MDBX_PNL_SIZE(txn->tw.reclaimed_pglist);
    unsigned left = amount;
    if (txn->tw.lifo_reclaimed == nullptr) {
      mdbx_tassert(txn, lifo == 0);
      rc = mdbx_cursor_first(&mc, &key, &data);
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;
    } else {
      mdbx_tassert(txn, lifo != 0);
    }

    while (true) {
      txnid_t fill_gc_id;
      mdbx_trace("%s: left %u of %u", dbg_prefix_mode, left,
                 (unsigned)MDBX_PNL_SIZE(txn->tw.reclaimed_pglist));
      if (txn->tw.lifo_reclaimed == nullptr) {
        mdbx_tassert(txn, lifo == 0);
        fill_gc_id = unaligned_peek_u64(4, key.iov_base);
        if (filled_gc_slot-- == 0 || fill_gc_id > txn->tw.last_reclaimed) {
          mdbx_notice(
              "** restart: reserve depleted (filled_slot %u, fill_id %" PRIaTXN
              " > last_reclaimed %" PRIaTXN,
              filled_gc_slot, fill_gc_id, txn->tw.last_reclaimed);
          goto retry;
        }
      } else {
        mdbx_tassert(txn, lifo != 0);
        if (++filled_gc_slot >
            (unsigned)MDBX_PNL_SIZE(txn->tw.lifo_reclaimed)) {
          mdbx_notice("** restart: reserve depleted (filled_gc_slot %u > "
                      "lifo_reclaimed %u" PRIaTXN,
                      filled_gc_slot,
                      (unsigned)MDBX_PNL_SIZE(txn->tw.lifo_reclaimed));
          goto retry;
        }
        fill_gc_id = txn->tw.lifo_reclaimed[filled_gc_slot];
        mdbx_trace("%s.seek-reservation @%" PRIaTXN " at lifo_reclaimed[%u]",
                   dbg_prefix_mode, fill_gc_id, filled_gc_slot);
        key.iov_base = &fill_gc_id;
        key.iov_len = sizeof(fill_gc_id);
        rc = mdbx_cursor_get(&mc, &key, &data, MDBX_SET_KEY);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
      }
      mdbx_tassert(txn, cleaned_gc_slot ==
                            (txn->tw.lifo_reclaimed
                                 ? MDBX_PNL_SIZE(txn->tw.lifo_reclaimed)
                                 : 0));
      mdbx_tassert(txn, fill_gc_id > 0 && fill_gc_id < *env->me_oldest);
      key.iov_base = &fill_gc_id;
      key.iov_len = sizeof(fill_gc_id);

      mdbx_tassert(txn, data.iov_len >= sizeof(pgno_t) * 2);
      mc.mc_flags |= C_GCFREEZE;
      unsigned chunk = (unsigned)(data.iov_len / sizeof(pgno_t)) - 1;
      if (unlikely(chunk > left)) {
        mdbx_trace("%s: chunk %u > left %u, @%" PRIaTXN, dbg_prefix_mode, chunk,
                   left, fill_gc_id);
        if ((loop < 5 && chunk - left > loop / 2) ||
            chunk - left > env->me_maxgc_ov1page) {
          data.iov_len = (left + 1) * sizeof(pgno_t);
          if (loop < 7)
            mc.mc_flags &= ~C_GCFREEZE;
        }
        chunk = left;
      }
      rc = mdbx_cursor_put(&mc, &key, &data, MDBX_CURRENT | MDBX_RESERVE);
      mc.mc_flags &= ~C_GCFREEZE;
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;
      clean_reserved_gc_pnl(env, data);

      if (unlikely(txn->tw.loose_count ||
                   amount != MDBX_PNL_SIZE(txn->tw.reclaimed_pglist))) {
        mdbx_notice("** restart: reclaimed-list changed (%u -> %u, %u)", amount,
                    MDBX_PNL_SIZE(txn->tw.reclaimed_pglist),
                    txn->tw.loose_count);
        goto retry;
      }
      if (unlikely(txn->tw.lifo_reclaimed
                       ? cleaned_gc_slot < MDBX_PNL_SIZE(txn->tw.lifo_reclaimed)
                       : cleaned_gc_id < txn->tw.last_reclaimed)) {
        mdbx_notice("%s", "** restart: reclaimed-slots changed");
        goto retry;
      }

      pgno_t *dst = data.iov_base;
      *dst++ = chunk;
      pgno_t *src = MDBX_PNL_BEGIN(txn->tw.reclaimed_pglist) + left - chunk;
      memcpy(dst, src, chunk * sizeof(pgno_t));
      pgno_t *from = src, *to = src + chunk;
      mdbx_trace("%s.fill: %u [ %u:%" PRIaPGNO "...%u:%" PRIaPGNO
                 "] @%" PRIaTXN,
                 dbg_prefix_mode, chunk,
                 (unsigned)(from - txn->tw.reclaimed_pglist), from[0],
                 (unsigned)(to - txn->tw.reclaimed_pglist), to[-1], fill_gc_id);

      left -= chunk;
      if (mdbx_audit_enabled()) {
        rc = mdbx_audit_ex(txn, retired_stored + amount - left, true);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
      }
      if (left == 0) {
        rc = MDBX_SUCCESS;
        break;
      }

      if (txn->tw.lifo_reclaimed == nullptr) {
        mdbx_tassert(txn, lifo == 0);
        rc = mdbx_cursor_next(&mc, &key, &data, MDBX_NEXT);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
      } else {
        mdbx_tassert(txn, lifo != 0);
      }
    }
  }

  mdbx_tassert(txn, rc == MDBX_SUCCESS);
  if (unlikely(txn->tw.loose_count != 0 ||
               filled_gc_slot !=
                   (txn->tw.lifo_reclaimed
                        ? (unsigned)MDBX_PNL_SIZE(txn->tw.lifo_reclaimed)
                        : 0))) {
    mdbx_notice("** restart: reserve excess (filled-slot %u, loose-count %u)",
                filled_gc_slot, txn->tw.loose_count);
    goto retry;
  }

  mdbx_tassert(txn,
               txn->tw.lifo_reclaimed == NULL ||
                   cleaned_gc_slot == MDBX_PNL_SIZE(txn->tw.lifo_reclaimed));

bailout:
  txn->mt_cursors[FREE_DBI] = mc.mc_next;

bailout_notracking:
  MDBX_PNL_SIZE(txn->tw.reclaimed_pglist) = 0;
  mdbx_trace("<<< %u loops, rc = %d", loop, rc);
  return rc;
}

static int mdbx_flush_iov(MDBX_txn *const txn, struct iovec *iov,
                          unsigned iov_items, size_t iov_off,
                          size_t iov_bytes) {
  MDBX_env *const env = txn->mt_env;
  int rc = mdbx_pwritev(env->me_fd, iov, iov_items, iov_off, iov_bytes);
  if (unlikely(rc != MDBX_SUCCESS)) {
    mdbx_error("Write error: %s", mdbx_strerror(rc));
    txn->mt_flags |= MDBX_TXN_ERROR;
  }

  for (unsigned i = 0; i < iov_items; i++)
    mdbx_dpage_free(env, (MDBX_page *)iov[i].iov_base,
                    bytes2pgno(env, iov[i].iov_len));

  return rc;
}

/* Flush (some) dirty pages to the map, after clearing their dirty flag.
 * [in] txn   the transaction that's being committed
 * [in] keep  number of initial pages in dirtylist to keep dirty.
 * Returns 0 on success, non-zero on failure. */
static int mdbx_page_flush(MDBX_txn *txn, const unsigned keep) {
  struct iovec iov[MDBX_COMMIT_PAGES];
  const MDBX_DPL dl = (keep || txn->tw.loose_count > 1)
                          ? mdbx_dpl_sort(txn->tw.dirtylist)
                          : txn->tw.dirtylist;
  MDBX_env *const env = txn->mt_env;
  pgno_t flush_begin = MAX_PAGENO;
  pgno_t flush_end = MIN_PAGENO;
  unsigned iov_items = 0;
  size_t iov_bytes = 0;
  size_t iov_off = 0;
  unsigned r, w;
  for (r = w = keep; ++r <= dl->length;) {
    MDBX_page *dp = dl[r].ptr;
    mdbx_tassert(txn,
                 dp->mp_pgno >= MIN_PAGENO && dp->mp_pgno < txn->mt_next_pgno);
    mdbx_tassert(txn, dp->mp_flags & P_DIRTY);

    /* Don't flush this page yet */
    if (dp->mp_flags & (P_LOOSE | P_KEEP)) {
      dp->mp_flags &= ~P_KEEP;
      dl[++w] = dl[r];
      continue;
    }

    const unsigned npages = IS_OVERFLOW(dp) ? dp->mp_pages : 1;
    flush_begin = (flush_begin < dp->mp_pgno) ? flush_begin : dp->mp_pgno;
    flush_end =
        (flush_end > dp->mp_pgno + npages) ? flush_end : dp->mp_pgno + npages;
    *env->me_unsynced_pages += npages;
    dp->mp_flags &= ~P_DIRTY;
    dp->mp_validator = 0 /* TODO */;

    if ((env->me_flags & MDBX_WRITEMAP) == 0) {
      const size_t size = pgno2bytes(env, npages);
      if (iov_off + iov_bytes != pgno2bytes(env, dp->mp_pgno) ||
          iov_items == ARRAY_LENGTH(iov) || iov_bytes + size > MAX_WRITE) {
        if (iov_items) {
          int rc = mdbx_flush_iov(txn, iov, iov_items, iov_off, iov_bytes);
          if (unlikely(rc != MDBX_SUCCESS))
            return rc;
#if MDBX_CPU_CACHE_MMAP_NONCOHERENT
#if defined(__linux__) || defined(__gnu_linux__)
          if (mdbx_linux_kernel_version >= 0x02060b00)
          /* Linux kernels older than version 2.6.11 ignore the addr and nbytes
           * arguments, making this function fairly expensive. Therefore, the
           * whole cache is always flushed. */
#endif /* Linux */
            mdbx_invalidate_mmap_noncoherent_cache(env->me_map + iov_off,
                                                   iov_bytes);
#endif /* MDBX_CPU_CACHE_MMAP_NONCOHERENT */
          iov_items = 0;
          iov_bytes = 0;
        }
        iov_off = pgno2bytes(env, dp->mp_pgno);
      }
      iov[iov_items].iov_base = dp;
      iov[iov_items].iov_len = size;
      iov_items += 1;
      iov_bytes += size;
    }
  }

  if (iov_items) {
    int rc = mdbx_flush_iov(txn, iov, iov_items, iov_off, iov_bytes);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }

#if MDBX_CPU_CACHE_MMAP_NONCOHERENT &&                                         \
    (defined(__linux__) || defined(__gnu_linux__))
  if ((env->me_flags & MDBX_WRITEMAP) == 0 &&
      mdbx_linux_kernel_version < 0x02060b00)
    /* Linux kernels older than version 2.6.11 ignore the addr and nbytes
     * arguments, making this function fairly expensive. Therefore, the
     * whole cache is always flushed. */
    mdbx_invalidate_mmap_noncoherent_cache(
        env->me_map + pgno2bytes(env, flush_begin),
        pgno2bytes(env, flush_end - flush_begin));
#endif /* MDBX_CPU_CACHE_MMAP_NONCOHERENT && Linux */

  /* TODO: use flush_begin & flush_end for msync() & sync_file_range(). */
  (void)flush_begin;
  (void)flush_end;

  txn->tw.dirtyroom += r - 1 - w;
  dl->length = w;
  mdbx_tassert(txn, txn->mt_parent ||
                        txn->tw.dirtyroom + txn->tw.dirtylist->length ==
                            MDBX_DPL_TXNFULL);
  return MDBX_SUCCESS;
}

/* Check for misused dbi handles */
#define TXN_DBI_CHANGED(txn, dbi)                                              \
  ((txn)->mt_dbiseqs[dbi] != (txn)->mt_env->me_dbiseqs[dbi])

/* Import DBI which opened after txn started into context */
static __cold bool mdbx_txn_import_dbi(MDBX_txn *txn, MDBX_dbi dbi) {
  MDBX_env *env = txn->mt_env;
  if (dbi < CORE_DBS || dbi >= env->me_numdbs)
    return false;

  mdbx_ensure(env, mdbx_fastmutex_acquire(&env->me_dbi_lock) == MDBX_SUCCESS);
  const unsigned snap_numdbs = env->me_numdbs;
  mdbx_compiler_barrier();
  for (unsigned i = CORE_DBS; i < snap_numdbs; ++i) {
    if (i >= txn->mt_numdbs)
      txn->mt_dbflags[i] = 0;
    if (!(txn->mt_dbflags[i] & DB_USRVALID) &&
        (env->me_dbflags[i] & MDBX_VALID)) {
      txn->mt_dbs[i].md_flags = env->me_dbflags[i] & PERSISTENT_FLAGS;
      txn->mt_dbflags[i] = DB_VALID | DB_USRVALID | DB_STALE;
      mdbx_tassert(txn, txn->mt_dbxs[i].md_cmp != NULL);
    }
  }
  txn->mt_numdbs = snap_numdbs;

  mdbx_ensure(env, mdbx_fastmutex_release(&env->me_dbi_lock) == MDBX_SUCCESS);
  return txn->mt_dbflags[dbi] & DB_USRVALID;
}

/* Check txn and dbi arguments to a function */
static __inline bool TXN_DBI_EXIST(MDBX_txn *txn, MDBX_dbi dbi,
                                   unsigned validity) {
  if (likely(dbi < txn->mt_numdbs && (txn->mt_dbflags[dbi] & validity)))
    return true;

  return mdbx_txn_import_dbi(txn, dbi);
}

int mdbx_txn_commit(MDBX_txn *txn) {
  int rc = check_txn(txn, MDBX_TXN_BLOCKED - MDBX_TXN_HAS_CHILD);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  MDBX_env *env = txn->mt_env;
#if MDBX_TXN_CHECKPID
  if (unlikely(env->me_pid != mdbx_getpid())) {
    env->me_flags |= MDBX_FATAL_ERROR;
    return MDBX_PANIC;
  }
#endif /* MDBX_TXN_CHECKPID */

  /* mdbx_txn_end() mode for a commit which writes nothing */
  unsigned end_mode =
      MDBX_END_EMPTY_COMMIT | MDBX_END_UPDATE | MDBX_END_SLOT | MDBX_END_FREE;
  if (unlikely(F_ISSET(txn->mt_flags, MDBX_RDONLY)))
    goto done;

  if (txn->mt_child) {
    rc = mdbx_txn_commit(txn->mt_child);
    mdbx_tassert(txn, txn->mt_child == NULL);
    if (unlikely(rc != MDBX_SUCCESS))
      goto fail;
  }

  if (unlikely(txn != env->me_txn)) {
    mdbx_debug("%s", "attempt to commit unknown transaction");
    rc = MDBX_EINVAL;
    goto fail;
  }

  if (txn->mt_parent) {
    MDBX_txn *const parent = txn->mt_parent;
    mdbx_tassert(txn, mdbx_dirtylist_check(txn));

    /* Preserve space for spill list to avoid parent's state corruption
     * if allocation fails. */
    if (txn->tw.spill_pages && parent->tw.spill_pages) {
      rc = mdbx_pnl_need(&parent->tw.spill_pages,
                         MDBX_PNL_SIZE(txn->tw.spill_pages));
      if (unlikely(rc != MDBX_SUCCESS))
        goto fail;
    }

    //-------------------------------------------------------------------------

    parent->tw.lifo_reclaimed = txn->tw.lifo_reclaimed;
    txn->tw.lifo_reclaimed = NULL;

    parent->tw.retired_pages = txn->tw.retired_pages;
    txn->tw.retired_pages = NULL;

    mdbx_pnl_free(parent->tw.reclaimed_pglist);
    parent->tw.reclaimed_pglist = txn->tw.reclaimed_pglist;
    txn->tw.reclaimed_pglist = NULL;
    parent->tw.last_reclaimed = txn->tw.last_reclaimed;

    parent->mt_geo = txn->mt_geo;
    parent->mt_canary = txn->mt_canary;
    parent->mt_flags = txn->mt_flags;

    /* Merge our cursors into parent's and close them */
    mdbx_cursors_eot(txn, 1);

    /* Update parent's DB table. */
    memcpy(parent->mt_dbs, txn->mt_dbs, txn->mt_numdbs * sizeof(MDBX_db));
    parent->mt_numdbs = txn->mt_numdbs;
    parent->mt_dbflags[FREE_DBI] = txn->mt_dbflags[FREE_DBI];
    parent->mt_dbflags[MAIN_DBI] = txn->mt_dbflags[MAIN_DBI];
    for (unsigned i = CORE_DBS; i < txn->mt_numdbs; i++) {
      /* preserve parent's DB_NEW status */
      parent->mt_dbflags[i] =
          txn->mt_dbflags[i] | (parent->mt_dbflags[i] & (DB_CREAT | DB_FRESH));
    }

    /* Remove refunded pages from parent's dirty & spill lists */
    MDBX_DPL dst = mdbx_dpl_sort(parent->tw.dirtylist);
    while (dst->length && dst[dst->length].pgno >= parent->mt_next_pgno) {
      MDBX_page *mp = dst[dst->length].ptr;
      if (mp && (txn->mt_env->me_flags & MDBX_WRITEMAP) == 0)
        mdbx_dpage_free(txn->mt_env, mp, IS_OVERFLOW(mp) ? mp->mp_pages : 1);
      dst->length -= 1;
    }
    parent->tw.dirtyroom += dst->sorted - dst->length;
    dst->sorted = dst->length;
    mdbx_tassert(parent,
                 parent->mt_parent ||
                     parent->tw.dirtyroom + parent->tw.dirtylist->length ==
                         MDBX_DPL_TXNFULL);

    if (parent->tw.spill_pages && MDBX_PNL_SIZE(parent->tw.spill_pages) > 0 &&
        MDBX_PNL_MOST(parent->tw.spill_pages) >= parent->mt_next_pgno << 1) {
      const MDBX_PNL ps = parent->tw.spill_pages;
#if MDBX_PNL_ASCENDING
      unsigned i = MDBX_PNL_SIZE(ps);
      assert(MDBX_PNL_MOST(ps) == MDBX_PNL_LAST(ps));
      do
        i -= 1;
      while (i && ps[i] >= parent->mt_next_pgno << 1);
      MDBX_PNL_SIZE(ps) = i;
#else
      assert(MDBX_PNL_MOST(ps) == MDBX_PNL_FIRST(ps));
      unsigned i = 1, len = MDBX_PNL_SIZE(ps);
      while (i < len && ps[i + 1] >= parent->mt_next_pgno << 1)
        ++i;
      MDBX_PNL_SIZE(ps) = len -= i;
      for (unsigned k = 1; k <= len; ++k)
        ps[k] = ps[k + i];
#endif
    }

    /* Remove anything in our dirty list from parent's spill list */
    MDBX_DPL src = mdbx_dpl_sort(txn->tw.dirtylist);
    if (likely(src->length > 0) && parent->tw.spill_pages &&
        MDBX_PNL_SIZE(parent->tw.spill_pages) > 0) {
      MDBX_PNL sp = parent->tw.spill_pages;
      assert(mdbx_pnl_check4assert(sp, txn->mt_next_pgno));

      const unsigned len = MDBX_PNL_SIZE(parent->tw.spill_pages);
      MDBX_PNL_SIZE(sp) = ~(pgno_t)0;

      /* Mark our dirty pages as deleted in parent spill list */
      unsigned r, w, i = 1;
      w = r = len;
      do {
        pgno_t pn = src[i].pgno << 1;
        while (pn > sp[r])
          r--;
        if (pn == sp[r]) {
          sp[r] = 1;
          w = --r;
        }
      } while (++i <= src->length);

      /* Squash deleted pagenums if we deleted any */
      for (r = w; ++r <= len;)
        if ((sp[r] & 1) == 0)
          sp[++w] = sp[r];
      MDBX_PNL_SIZE(sp) = w;
      assert(mdbx_pnl_check4assert(sp, txn->mt_next_pgno << 1));
    }

    /* Remove anything in our spill list from parent's dirty list */
    if (txn->tw.spill_pages && MDBX_PNL_SIZE(txn->tw.spill_pages) > 0) {
      const MDBX_PNL sp = txn->tw.spill_pages;
      mdbx_pnl_sort(sp);
      /* Scanning in ascend order */
      const int step = MDBX_PNL_ASCENDING ? 1 : -1;
      const int begin = MDBX_PNL_ASCENDING ? 1 : MDBX_PNL_SIZE(sp);
      const int end = MDBX_PNL_ASCENDING ? MDBX_PNL_SIZE(sp) + 1 : 0;
      mdbx_tassert(txn, sp[begin] <= sp[end - step]);

      unsigned r, w = r = mdbx_dpl_search(dst, sp[begin] >> 1);
      mdbx_tassert(txn, dst->sorted == dst->length);
      for (int i = begin; r <= dst->length;) {
        mdbx_tassert(txn, (sp[i] & 1) == 0);
        const pgno_t pgno = sp[i] >> 1;
        if (dst[r].pgno < pgno) {
          dst[w++] = dst[r++];
        } else if (dst[r].pgno > pgno) {
          i += step;
          if (i == end)
            while (r <= dst->length)
              dst[w++] = dst[r++];
        } else {
          MDBX_page *dp = dst[r++].ptr;
          if ((env->me_flags & MDBX_WRITEMAP) == 0)
            mdbx_dpage_free(env, dp, IS_OVERFLOW(dp) ? dp->mp_pages : 1);
        }
      }
      mdbx_tassert(txn, r == dst->length + 1);
      dst->length = w;
      parent->tw.dirtyroom += r - w;
    }
    assert(dst->sorted == dst->length);
    mdbx_tassert(parent,
                 parent->mt_parent ||
                     parent->tw.dirtyroom + parent->tw.dirtylist->length ==
                         MDBX_DPL_TXNFULL);

    unsigned d, s, l;
    /* Find length of merging our dirty list with parent's */
    for (l = 0, d = dst->length, s = src->length; d > 0 && s > 0; ++l) {
      const pgno_t s_pgno = src[s].pgno;
      const pgno_t d_pgno = dst[d].pgno;
      d -= d_pgno >= s_pgno;
      s -= d_pgno <= s_pgno;
    }
    assert(dst->sorted == dst->length);
    dst->sorted = l += d + s;
    assert(dst->sorted >= dst->length);
    parent->tw.dirtyroom -= dst->sorted - dst->length;

    /* Merge our dirty list into parent's */
    for (d = dst->length, s = src->length; d > 0 && s > 0; --l) {
      if (dst[d].pgno > src[s].pgno)
        dst[l] = dst[d--];
      else if (dst[d].pgno < src[s].pgno)
        dst[l] = src[s--];
      else {
        MDBX_page *dp = dst[d--].ptr;
        if (dp && (env->me_flags & MDBX_WRITEMAP) == 0)
          mdbx_dpage_free(env, dp, IS_OVERFLOW(dp) ? dp->mp_pgno : 1);
        dst[l] = src[s--];
      }
    }
    if (s) {
      do
        dst[l--] = src[s--];
      while (s > 0);
    } else if (d) {
      do
        dst[l--] = dst[d--];
      while (d > 0);
    }
    assert(l == 0);
    dst->length = dst->sorted;
    mdbx_free(txn->tw.dirtylist);
    txn->tw.dirtylist = nullptr;
    mdbx_tassert(parent,
                 parent->mt_parent ||
                     parent->tw.dirtyroom + parent->tw.dirtylist->length ==
                         MDBX_DPL_TXNFULL);

    if (txn->tw.spill_pages) {
      if (parent->tw.spill_pages) {
        /* Must not fail since space was preserved above. */
        rc = mdbx_pnl_append_list(&parent->tw.spill_pages, txn->tw.spill_pages);
        mdbx_assert(env, rc == MDBX_SUCCESS);
        (void)rc;
        mdbx_pnl_free(txn->tw.spill_pages);
        mdbx_pnl_sort(parent->tw.spill_pages);
      } else {
        parent->tw.spill_pages = txn->tw.spill_pages;
      }
    }
    if (parent->tw.spill_pages)
      assert(mdbx_pnl_check4assert(parent->tw.spill_pages,
                                   parent->mt_next_pgno << 1));

    /* Append our loose page list to parent's */
    if (txn->tw.loose_pages) {
      MDBX_page **lp = &parent->tw.loose_pages;
      while (*lp)
        lp = &(*lp)->mp_next;
      *lp = txn->tw.loose_pages;
      parent->tw.loose_count += txn->tw.loose_count;
    }
    if (txn->tw.retired2parent_pages) {
      MDBX_page *mp = txn->tw.retired2parent_pages;
      do {
        MDBX_page *next = mp->mp_next;
        rc = mdbx_page_loose(parent, mp);
        if (unlikely(rc != MDBX_SUCCESS))
          goto fail;
        mp = next;
      } while (mp);
    }

    env->me_txn = parent;
    parent->mt_child = NULL;
    txn->mt_signature = 0;
    mdbx_free(txn);
    mdbx_tassert(parent, mdbx_dirtylist_check(parent));

    /* Scan parent's loose page for suitable for refund */
    for (MDBX_page *mp = parent->tw.loose_pages; mp; mp = mp->mp_next) {
      if (mp->mp_pgno == parent->mt_next_pgno - 1) {
        mdbx_refund(parent);
        break;
      }
    }
    mdbx_tassert(parent, mdbx_dirtylist_check(parent));
    return MDBX_SUCCESS;
  }

  mdbx_tassert(txn, txn->tw.dirtyroom + txn->tw.dirtylist->length ==
                        MDBX_DPL_TXNFULL);
  mdbx_cursors_eot(txn, 0);
  end_mode |= MDBX_END_EOTDONE;

  if (txn->tw.dirtylist->length == 0 &&
      (txn->mt_flags & (MDBX_TXN_DIRTY | MDBX_TXN_SPILLS)) == 0)
    goto done;

  mdbx_debug("committing txn %" PRIaTXN " %p on mdbenv %p, root page %" PRIaPGNO
             "/%" PRIaPGNO,
             txn->mt_txnid, (void *)txn, (void *)env,
             txn->mt_dbs[MAIN_DBI].md_root, txn->mt_dbs[FREE_DBI].md_root);

  /* Update DB root pointers */
  if (txn->mt_numdbs > CORE_DBS) {
    MDBX_cursor mc;
    MDBX_val data;
    data.iov_len = sizeof(MDBX_db);

    rc = mdbx_cursor_init(&mc, txn, MAIN_DBI);
    if (unlikely(rc != MDBX_SUCCESS))
      goto fail;
    for (MDBX_dbi i = CORE_DBS; i < txn->mt_numdbs; i++) {
      if (txn->mt_dbflags[i] & DB_DIRTY) {
        if (unlikely(TXN_DBI_CHANGED(txn, i))) {
          rc = MDBX_BAD_DBI;
          goto fail;
        }
        MDBX_db *db = &txn->mt_dbs[i];
        db->md_mod_txnid = txn->mt_txnid;
        data.iov_base = db;
        WITH_CURSOR_TRACKING(mc,
                             rc = mdbx_cursor_put(&mc, &txn->mt_dbxs[i].md_name,
                                                  &data, F_SUBDATA));
        if (unlikely(rc != MDBX_SUCCESS))
          goto fail;
      }
    }
  }

  rc = mdbx_update_gc(txn);
  if (unlikely(rc != MDBX_SUCCESS))
    goto fail;

  if (mdbx_audit_enabled()) {
    rc = mdbx_audit_ex(txn, MDBX_PNL_SIZE(txn->tw.retired_pages), true);
    if (unlikely(rc != MDBX_SUCCESS))
      goto fail;
  }

  rc = mdbx_page_flush(txn, 0);
  if (likely(rc == MDBX_SUCCESS)) {
    if (txn->mt_dbs[MAIN_DBI].md_flags & DB_DIRTY)
      txn->mt_dbs[MAIN_DBI].md_mod_txnid = txn->mt_txnid;

    MDBX_meta meta, *head = mdbx_meta_head(env);
    meta.mm_magic_and_version = head->mm_magic_and_version;
    meta.mm_extra_flags = head->mm_extra_flags;
    meta.mm_validator_id = head->mm_validator_id;
    meta.mm_extra_pagehdr = head->mm_extra_pagehdr;
    meta.mm_pages_retired =
        head->mm_pages_retired + MDBX_PNL_SIZE(txn->tw.retired_pages);

    meta.mm_geo = txn->mt_geo;
    meta.mm_dbs[FREE_DBI] = txn->mt_dbs[FREE_DBI];
    meta.mm_dbs[MAIN_DBI] = txn->mt_dbs[MAIN_DBI];
    meta.mm_canary = txn->mt_canary;
    mdbx_meta_set_txnid(env, &meta, txn->mt_txnid);

    rc = mdbx_sync_locked(
        env, env->me_flags | txn->mt_flags | MDBX_SHRINK_ALLOWED, &meta);
  }
  if (unlikely(rc != MDBX_SUCCESS)) {
    env->me_flags |= MDBX_FATAL_ERROR;
    goto fail;
  }

  if (likely(env->me_lck))
    env->me_lck->mti_readers_refresh_flag = false;
  end_mode = MDBX_END_COMMITTED | MDBX_END_UPDATE | MDBX_END_EOTDONE;

done:
  return mdbx_txn_end(txn, end_mode);

fail:
  mdbx_txn_abort(txn);
  return rc;
}

/* Read the environment parameters of a DB environment
 * before mapping it into memory. */
static int __cold mdbx_read_header(MDBX_env *env, MDBX_meta *dest,
                                   uint64_t *filesize) {
  int rc = mdbx_filesize(env->me_fd, filesize);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  memset(dest, 0, sizeof(MDBX_meta));
  dest->mm_datasync_sign = MDBX_DATASIGN_WEAK;
  rc = MDBX_CORRUPTED;

  /* Read twice all meta pages so we can find the latest one. */
  unsigned loop_limit = NUM_METAS * 2;
  for (unsigned loop_count = 0; loop_count < loop_limit; ++loop_count) {
    /* We don't know the page size on first time.
     * So, just guess it. */
    unsigned guess_pagesize = dest->mm_psize;
    if (guess_pagesize == 0)
      guess_pagesize =
          (loop_count > NUM_METAS) ? env->me_psize : env->me_os_psize;

    const unsigned meta_number = loop_count % NUM_METAS;
    const unsigned offset = guess_pagesize * meta_number;

    char buffer[MIN_PAGESIZE];
    unsigned retryleft = 42;
    while (1) {
      mdbx_trace("reading meta[%d]: offset %u, bytes %u, retry-left %u",
                 meta_number, offset, MIN_PAGESIZE, retryleft);
      int err = mdbx_pread(env->me_fd, buffer, MIN_PAGESIZE, offset);
      if (err != MDBX_SUCCESS) {
        if (err == MDBX_ENODATA && offset == 0 && loop_count == 0 &&
            *filesize == 0 && (env->me_flags & MDBX_RDONLY) == 0)
          mdbx_notice("read meta: empty file (%d, %s)", err,
                      mdbx_strerror(err));
        else
          mdbx_error("read meta[%u,%u]: %i, %s", offset, MIN_PAGESIZE, err,
                     mdbx_strerror(err));
        return err;
      }

      char again[MIN_PAGESIZE];
      err = mdbx_pread(env->me_fd, again, MIN_PAGESIZE, offset);
      if (err != MDBX_SUCCESS) {
        mdbx_error("read meta[%u,%u]: %i, %s", offset, MIN_PAGESIZE, err,
                   mdbx_strerror(err));
        return err;
      }

      if (memcmp(buffer, again, MIN_PAGESIZE) == 0 || --retryleft == 0)
        break;

      mdbx_verbose("meta[%u] was updated, re-read it", meta_number);
    }

    MDBX_page *const page = (MDBX_page *)buffer;
    MDBX_meta *const meta = page_meta(page);
    if (meta->mm_magic_and_version != MDBX_DATA_MAGIC &&
        meta->mm_magic_and_version != MDBX_DATA_MAGIC_DEVEL) {
      mdbx_error("meta[%u] has invalid magic/version %" PRIx64, meta_number,
                 meta->mm_magic_and_version);
      return ((meta->mm_magic_and_version >> 8) != MDBX_MAGIC)
                 ? MDBX_INVALID
                 : MDBX_VERSION_MISMATCH;
    }

    if (page->mp_pgno != meta_number) {
      mdbx_error("meta[%u] has invalid pageno %" PRIaPGNO, meta_number,
                 page->mp_pgno);
      return MDBX_INVALID;
    }

    if (page->mp_flags != P_META) {
      mdbx_error("page #%u not a meta-page", meta_number);
      return MDBX_INVALID;
    }

    if (!retryleft) {
      mdbx_error("meta[%u] is too volatile, skip it", meta_number);
      continue;
    }

    /* LY: check pagesize */
    if (!is_powerof2(meta->mm_psize) || meta->mm_psize < MIN_PAGESIZE ||
        meta->mm_psize > MAX_PAGESIZE) {
      mdbx_notice("meta[%u] has invalid pagesize (%u), skip it", meta_number,
                  meta->mm_psize);
      rc = is_powerof2(meta->mm_psize) ? MDBX_VERSION_MISMATCH : MDBX_INVALID;
      continue;
    }

    if (meta_number == 0 && guess_pagesize != meta->mm_psize) {
      dest->mm_psize = meta->mm_psize;
      mdbx_verbose("meta[%u] took pagesize %u", meta_number, meta->mm_psize);
    }

    if (safe64_read(&meta->mm_txnid_a) != safe64_read(&meta->mm_txnid_b)) {
      mdbx_warning("meta[%u] not completely updated, skip it", meta_number);
      continue;
    }

    /* LY: check signature as a checksum */
    if (META_IS_STEADY(meta) &&
        meta->mm_datasync_sign != mdbx_meta_sign(meta)) {
      mdbx_notice("meta[%u] has invalid steady-checksum (0x%" PRIx64
                  " != 0x%" PRIx64 "), skip it",
                  meta_number, meta->mm_datasync_sign, mdbx_meta_sign(meta));
      continue;
    }

    mdbx_debug("read meta%" PRIaPGNO " = root %" PRIaPGNO "/%" PRIaPGNO
               ", geo %" PRIaPGNO "/%" PRIaPGNO "-%" PRIaPGNO "/%" PRIaPGNO
               " +%u -%u, txn_id %" PRIaTXN ", %s",
               page->mp_pgno, meta->mm_dbs[MAIN_DBI].md_root,
               meta->mm_dbs[FREE_DBI].md_root, meta->mm_geo.lower,
               meta->mm_geo.next, meta->mm_geo.now, meta->mm_geo.upper,
               meta->mm_geo.grow, meta->mm_geo.shrink,
               meta->mm_txnid_a.inconsistent, mdbx_durable_str(meta));

    /* LY: check min-pages value */
    if (meta->mm_geo.lower < MIN_PAGENO || meta->mm_geo.lower > MAX_PAGENO) {
      mdbx_notice("meta[%u] has invalid min-pages (%" PRIaPGNO "), skip it",
                  meta_number, meta->mm_geo.lower);
      rc = MDBX_INVALID;
      continue;
    }

    /* LY: check max-pages value */
    if (meta->mm_geo.upper < MIN_PAGENO || meta->mm_geo.upper > MAX_PAGENO ||
        meta->mm_geo.upper < meta->mm_geo.lower) {
      mdbx_notice("meta[%u] has invalid max-pages (%" PRIaPGNO "), skip it",
                  meta_number, meta->mm_geo.upper);
      rc = MDBX_INVALID;
      continue;
    }

    /* LY: check last_pgno */
    if (meta->mm_geo.next < MIN_PAGENO || meta->mm_geo.next - 1 > MAX_PAGENO) {
      mdbx_notice("meta[%u] has invalid next-pageno (%" PRIaPGNO "), skip it",
                  meta_number, meta->mm_geo.next);
      rc = MDBX_CORRUPTED;
      continue;
    }

    /* LY: check filesize & used_bytes */
    const uint64_t used_bytes = meta->mm_geo.next * (uint64_t)meta->mm_psize;
    if (used_bytes > *filesize) {
      /* Here could be a race with DB-shrinking performed by other process */
      rc = mdbx_filesize(env->me_fd, filesize);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
      if (used_bytes > *filesize) {
        mdbx_notice("meta[%u] used-bytes (%" PRIu64
                    ") beyond filesize (%" PRIu64 "), skip it",
                    meta_number, used_bytes, *filesize);
        rc = MDBX_CORRUPTED;
        continue;
      }
    }

    /* LY: check mapsize limits */
    const uint64_t mapsize_min = meta->mm_geo.lower * (uint64_t)meta->mm_psize;
    STATIC_ASSERT(MAX_MAPSIZE < PTRDIFF_MAX - MAX_PAGESIZE);
    STATIC_ASSERT(MIN_MAPSIZE < MAX_MAPSIZE);
    if (mapsize_min < MIN_MAPSIZE || mapsize_min > MAX_MAPSIZE) {
      mdbx_notice("meta[%u] has invalid min-mapsize (%" PRIu64 "), skip it",
                  meta_number, mapsize_min);
      rc = MDBX_VERSION_MISMATCH;
      continue;
    }

    const uint64_t mapsize_max = meta->mm_geo.upper * (uint64_t)meta->mm_psize;
    STATIC_ASSERT(MIN_MAPSIZE < MAX_MAPSIZE);
    if (mapsize_max > MAX_MAPSIZE ||
        MAX_PAGENO < roundup_powerof2((size_t)mapsize_max, env->me_os_psize) /
                         (size_t)meta->mm_psize) {
      if (meta->mm_geo.next - 1 > MAX_PAGENO || used_bytes > MAX_MAPSIZE) {
        mdbx_notice("meta[%u] has too large max-mapsize (%" PRIu64 "), skip it",
                    meta_number, mapsize_max);
        rc = MDBX_TOO_LARGE;
        continue;
      }

      /* allow to open large DB from a 32-bit environment */
      mdbx_notice("meta[%u] has too large max-mapsize (%" PRIu64 "), "
                  "but size of used space still acceptable (%" PRIu64 ")",
                  meta_number, mapsize_max, used_bytes);
      meta->mm_geo.upper = (pgno_t)(MAX_MAPSIZE / meta->mm_psize);
    }

    /* LY: check and silently put mm_geo.now into [geo.lower...geo.upper].
     *
     * Copy-with-compaction by previous version of libmdbx could produce DB-file
     * less than meta.geo.lower bound, in case actual filling is low or no data
     * at all. This is not a problem as there is no damage or loss of data.
     * Therefore it is better not to consider such situation as an error, but
     * silently correct it. */
    if (meta->mm_geo.now < meta->mm_geo.lower)
      meta->mm_geo.now = meta->mm_geo.lower;
    if (meta->mm_geo.now > meta->mm_geo.upper &&
        meta->mm_geo.next <= meta->mm_geo.upper)
      meta->mm_geo.now = meta->mm_geo.upper;

    if (meta->mm_geo.next > meta->mm_geo.now) {
      mdbx_notice("meta[%u] next-pageno (%" PRIaPGNO
                  ") is beyond end-pgno (%" PRIaPGNO "), skip it",
                  meta_number, meta->mm_geo.next, meta->mm_geo.now);
      rc = MDBX_CORRUPTED;
      continue;
    }

    /* LY: GC root */
    if (meta->mm_dbs[FREE_DBI].md_root == P_INVALID) {
      if (meta->mm_dbs[FREE_DBI].md_branch_pages ||
          meta->mm_dbs[FREE_DBI].md_depth ||
          meta->mm_dbs[FREE_DBI].md_entries ||
          meta->mm_dbs[FREE_DBI].md_leaf_pages ||
          meta->mm_dbs[FREE_DBI].md_overflow_pages) {
        mdbx_notice("meta[%u] has false-empty GC, skip it", meta_number);
        rc = MDBX_CORRUPTED;
        continue;
      }
    } else if (meta->mm_dbs[FREE_DBI].md_root >= meta->mm_geo.next) {
      mdbx_notice("meta[%u] has invalid GC-root %" PRIaPGNO ", skip it",
                  meta_number, meta->mm_dbs[FREE_DBI].md_root);
      rc = MDBX_CORRUPTED;
      continue;
    }

    /* LY: MainDB root */
    if (meta->mm_dbs[MAIN_DBI].md_root == P_INVALID) {
      if (meta->mm_dbs[MAIN_DBI].md_branch_pages ||
          meta->mm_dbs[MAIN_DBI].md_depth ||
          meta->mm_dbs[MAIN_DBI].md_entries ||
          meta->mm_dbs[MAIN_DBI].md_leaf_pages ||
          meta->mm_dbs[MAIN_DBI].md_overflow_pages) {
        mdbx_notice("meta[%u] has false-empty maindb", meta_number);
        rc = MDBX_CORRUPTED;
        continue;
      }
    } else if (meta->mm_dbs[MAIN_DBI].md_root >= meta->mm_geo.next) {
      mdbx_notice("meta[%u] has invalid maindb-root %" PRIaPGNO ", skip it",
                  meta_number, meta->mm_dbs[MAIN_DBI].md_root);
      rc = MDBX_CORRUPTED;
      continue;
    }

    if (safe64_read(&meta->mm_txnid_a) == 0) {
      mdbx_warning("meta[%u] has zero txnid, skip it", meta_number);
      continue;
    }

    if (mdbx_meta_ot(prefer_noweak, env, dest, meta)) {
      *dest = *meta;
      if (META_IS_WEAK(dest))
        loop_limit += 1; /* LY: should re-read to hush race with update */
      mdbx_verbose("latch meta[%u]", meta_number);
    }
  }

  if (META_IS_WEAK(dest)) {
    mdbx_error("%s", "no usable meta-pages, database is corrupted");
    return rc;
  }

  return MDBX_SUCCESS;
}

static MDBX_page *__cold mdbx_meta_model(const MDBX_env *env, MDBX_page *model,
                                         unsigned num) {

  mdbx_ensure(env, is_powerof2(env->me_psize));
  mdbx_ensure(env, env->me_psize >= MIN_PAGESIZE);
  mdbx_ensure(env, env->me_psize <= MAX_PAGESIZE);
  mdbx_ensure(env, env->me_dbgeo.lower >= MIN_MAPSIZE);
  mdbx_ensure(env, env->me_dbgeo.upper <= MAX_MAPSIZE);
  mdbx_ensure(env, env->me_dbgeo.now >= env->me_dbgeo.lower);
  mdbx_ensure(env, env->me_dbgeo.now <= env->me_dbgeo.upper);

  memset(model, 0, sizeof(*model));
  model->mp_pgno = num;
  model->mp_flags = P_META;
  MDBX_meta *const model_meta = page_meta(model);
  model_meta->mm_magic_and_version = MDBX_DATA_MAGIC;

  model_meta->mm_geo.lower = bytes2pgno(env, env->me_dbgeo.lower);
  model_meta->mm_geo.upper = bytes2pgno(env, env->me_dbgeo.upper);
  model_meta->mm_geo.grow = (uint16_t)bytes2pgno(env, env->me_dbgeo.grow);
  model_meta->mm_geo.shrink = (uint16_t)bytes2pgno(env, env->me_dbgeo.shrink);
  model_meta->mm_geo.now = bytes2pgno(env, env->me_dbgeo.now);
  model_meta->mm_geo.next = NUM_METAS;

  mdbx_ensure(env, model_meta->mm_geo.lower >= MIN_PAGENO);
  mdbx_ensure(env, model_meta->mm_geo.upper <= MAX_PAGENO);
  mdbx_ensure(env, model_meta->mm_geo.now >= model_meta->mm_geo.lower);
  mdbx_ensure(env, model_meta->mm_geo.now <= model_meta->mm_geo.upper);
  mdbx_ensure(env, model_meta->mm_geo.next >= MIN_PAGENO);
  mdbx_ensure(env, model_meta->mm_geo.next <= model_meta->mm_geo.now);
  mdbx_ensure(env,
              model_meta->mm_geo.grow == bytes2pgno(env, env->me_dbgeo.grow));
  mdbx_ensure(env, model_meta->mm_geo.shrink ==
                       bytes2pgno(env, env->me_dbgeo.shrink));

  model_meta->mm_psize = env->me_psize;
  model_meta->mm_flags = (uint16_t)env->me_flags;
  model_meta->mm_flags |=
      MDBX_INTEGERKEY; /* this is mm_dbs[FREE_DBI].md_flags */
  model_meta->mm_dbs[FREE_DBI].md_root = P_INVALID;
  model_meta->mm_dbs[MAIN_DBI].md_root = P_INVALID;
  mdbx_meta_set_txnid(env, model_meta, MIN_TXNID + num);
  model_meta->mm_datasync_sign = mdbx_meta_sign(model_meta);
  return (MDBX_page *)((uint8_t *)model + env->me_psize);
}

/* Fill in most of the zeroed meta-pages for an empty database environment.
 * Return pointer to recenly (head) meta-page. */
static MDBX_meta *__cold mdbx_init_metas(const MDBX_env *env, void *buffer) {
  MDBX_page *page0 = (MDBX_page *)buffer;
  MDBX_page *page1 = mdbx_meta_model(env, page0, 0);
  MDBX_page *page2 = mdbx_meta_model(env, page1, 1);
  mdbx_meta_model(env, page2, 2);
  mdbx_assert(env, !mdbx_meta_eq(env, page_meta(page0), page_meta(page1)));
  mdbx_assert(env, !mdbx_meta_eq(env, page_meta(page1), page_meta(page2)));
  mdbx_assert(env, !mdbx_meta_eq(env, page_meta(page2), page_meta(page0)));
  return page_meta(page2);
}

static int mdbx_sync_locked(MDBX_env *env, unsigned flags,
                            MDBX_meta *const pending) {
  mdbx_assert(env, ((env->me_flags ^ flags) & MDBX_WRITEMAP) == 0);
  MDBX_meta *const meta0 = METAPAGE(env, 0);
  MDBX_meta *const meta1 = METAPAGE(env, 1);
  MDBX_meta *const meta2 = METAPAGE(env, 2);
  MDBX_meta *const head = mdbx_meta_head(env);

  mdbx_assert(env, mdbx_meta_eq_mask(env) == 0);
  mdbx_assert(env,
              pending < METAPAGE(env, 0) || pending > METAPAGE(env, NUM_METAS));
  mdbx_assert(env, (env->me_flags & (MDBX_RDONLY | MDBX_FATAL_ERROR)) == 0);
  mdbx_assert(env, !META_IS_STEADY(head) || *env->me_unsynced_pages != 0);
  mdbx_assert(env, pending->mm_geo.next <= pending->mm_geo.now);

  if (flags & (MDBX_NOSYNC | MDBX_MAPASYNC)) {
    /* Check auto-sync conditions */
    const pgno_t autosync_threshold = *env->me_autosync_threshold;
    const uint64_t autosync_period = *env->me_autosync_period;
    if ((autosync_threshold && *env->me_unsynced_pages >= autosync_threshold) ||
        (autosync_period &&
         mdbx_osal_monotime() - *env->me_sync_timestamp >= autosync_period))
      flags &= MDBX_WRITEMAP | MDBX_SHRINK_ALLOWED; /* force steady */
  }

  pgno_t shrink = 0;
  if (flags & MDBX_SHRINK_ALLOWED) {
    /* LY: check conditions to discard unused pages */
    const pgno_t largest_pgno = mdbx_find_largest(
        env, (head->mm_geo.next > pending->mm_geo.next) ? head->mm_geo.next
                                                        : pending->mm_geo.next);
    mdbx_assert(env, largest_pgno >= NUM_METAS);
#if defined(MDBX_USE_VALGRIND) || defined(__SANITIZE_ADDRESS__)
    const pgno_t edge = env->me_poison_edge;
    if (edge > largest_pgno) {
      env->me_poison_edge = largest_pgno;
      VALGRIND_MAKE_MEM_NOACCESS(env->me_map + pgno2bytes(env, largest_pgno),
                                 pgno2bytes(env, edge - largest_pgno));
      ASAN_POISON_MEMORY_REGION(env->me_map + pgno2bytes(env, largest_pgno),
                                pgno2bytes(env, edge - largest_pgno));
    }
#endif /* MDBX_USE_VALGRIND */
#if defined(MADV_DONTNEED)
    const size_t largest_aligned2os_bytes =
        pgno_align2os_bytes(env, largest_pgno);
    const pgno_t largest_aligned2os_pgno =
        bytes2pgno(env, largest_aligned2os_bytes);
    const pgno_t prev_discarded_pgno = *env->me_discarded_tail;
    if (prev_discarded_pgno >
        largest_aligned2os_pgno +
            /* 1M threshold to avoid unreasonable madvise() call */
            bytes2pgno(env, MEGABYTE)) {
      mdbx_notice("open-MADV_%s %u..%u", "DONTNEED", *env->me_discarded_tail,
                  largest_pgno);
      *env->me_discarded_tail = largest_aligned2os_pgno;
      const size_t prev_discarded_bytes =
          pgno2bytes(env, prev_discarded_pgno) & ~(env->me_os_psize - 1);
      mdbx_ensure(env, prev_discarded_bytes > largest_aligned2os_bytes);
      int advise = MADV_DONTNEED;
#if defined(MADV_FREE) &&                                                      \
    0 /* MADV_FREE works for only anon vma at the moment */
      if ((env->me_flags & MDBX_WRITEMAP) &&
          mdbx_linux_kernel_version > 0x04050000)
        advise = MADV_FREE;
#endif /* MADV_FREE */
      int err = madvise(env->me_map + largest_aligned2os_bytes,
                        prev_discarded_bytes - largest_aligned2os_bytes, advise)
                    ? errno
                    : MDBX_SUCCESS;
      mdbx_assert(env, err == MDBX_SUCCESS);
      (void)err;
    }
#endif /* MADV_FREE || MADV_DONTNEED */

    /* LY: check conditions to shrink datafile */
    const pgno_t backlog_gap =
        pending->mm_dbs[FREE_DBI].md_depth + mdbx_backlog_extragap(env);
    if (pending->mm_geo.shrink && pending->mm_geo.now - pending->mm_geo.next >
                                      pending->mm_geo.shrink + backlog_gap) {
      if (pending->mm_geo.now > largest_pgno &&
          pending->mm_geo.now - largest_pgno >
              pending->mm_geo.shrink + backlog_gap) {
        const pgno_t aligner = pending->mm_geo.grow ? pending->mm_geo.grow
                                                    : pending->mm_geo.shrink;
        const pgno_t with_backlog_gap = largest_pgno + backlog_gap;
        const pgno_t aligned = pgno_align2os_pgno(
            env, with_backlog_gap + aligner - with_backlog_gap % aligner);
        const pgno_t bottom =
            (aligned > pending->mm_geo.lower) ? aligned : pending->mm_geo.lower;
        if (pending->mm_geo.now > bottom) {
          flags &= MDBX_WRITEMAP | MDBX_SHRINK_ALLOWED; /* force steady */
          shrink = pending->mm_geo.now - bottom;
          pending->mm_geo.now = bottom;
          if (mdbx_meta_txnid_stable(env, head) ==
              pending->mm_txnid_a.inconsistent)
            mdbx_meta_set_txnid(
                env, pending,
                safe64_txnid_next(pending->mm_txnid_a.inconsistent));
        }
      }
    }
  }

  /* LY: step#1 - sync previously written/updated data-pages */
  int rc = *env->me_unsynced_pages ? MDBX_RESULT_TRUE /* carry non-steady */
                                   : MDBX_RESULT_FALSE /* carry steady */;
  if (rc != MDBX_RESULT_FALSE && (flags & MDBX_NOSYNC) == 0) {
    mdbx_assert(env, ((flags ^ env->me_flags) & MDBX_WRITEMAP) == 0);
    MDBX_meta *const recent_steady_meta = mdbx_meta_steady(env);
    if (flags & MDBX_WRITEMAP) {
      const size_t usedbytes = pgno_align2os_bytes(env, pending->mm_geo.next);
      rc = mdbx_msync(&env->me_dxb_mmap, 0, usedbytes, flags & MDBX_MAPASYNC);
      if (unlikely(rc != MDBX_SUCCESS))
        goto fail;
      rc = MDBX_RESULT_TRUE /* carry non-steady */;
      if ((flags & MDBX_MAPASYNC) == 0) {
        if (unlikely(pending->mm_geo.next > recent_steady_meta->mm_geo.now)) {
          rc = mdbx_filesync(env->me_fd, MDBX_SYNC_SIZE);
          if (unlikely(rc != MDBX_SUCCESS))
            goto fail;
        }
        rc = MDBX_RESULT_FALSE /* carry steady */;
      }
    } else {
      rc = mdbx_filesync(env->me_fd,
                         (pending->mm_geo.next > recent_steady_meta->mm_geo.now)
                             ? MDBX_SYNC_DATA | MDBX_SYNC_SIZE
                             : MDBX_SYNC_DATA);
      if (unlikely(rc != MDBX_SUCCESS))
        goto fail;
    }
  }

  /* Steady or Weak */
  if (rc == MDBX_RESULT_FALSE /* carry steady */) {
    pending->mm_datasync_sign = mdbx_meta_sign(pending);
    *env->me_unsynced_pages = 0;
    *env->me_sync_timestamp = mdbx_osal_monotime();
  } else {
    assert(rc == MDBX_RESULT_TRUE /* carry non-steady */);
    pending->mm_datasync_sign = F_ISSET(env->me_flags, MDBX_UTTERLY_NOSYNC)
                                    ? MDBX_DATASIGN_NONE
                                    : MDBX_DATASIGN_WEAK;
  }

  MDBX_meta *target = nullptr;
  if (mdbx_meta_txnid_stable(env, head) == pending->mm_txnid_a.inconsistent) {
    mdbx_assert(env, memcmp(&head->mm_dbs, &pending->mm_dbs,
                            sizeof(head->mm_dbs)) == 0);
    mdbx_assert(env, memcmp(&head->mm_canary, &pending->mm_canary,
                            sizeof(head->mm_canary)) == 0);
    mdbx_assert(env, memcmp(&head->mm_geo, &pending->mm_geo,
                            sizeof(pending->mm_geo)) == 0);
    if (!META_IS_STEADY(head) && META_IS_STEADY(pending))
      target = head;
    else {
      mdbx_ensure(env, mdbx_meta_eq(env, head, pending));
      mdbx_debug("%s", "skip update meta");
      return MDBX_SUCCESS;
    }
  } else if (head == meta0)
    target = mdbx_meta_ancient(prefer_steady, env, meta1, meta2);
  else if (head == meta1)
    target = mdbx_meta_ancient(prefer_steady, env, meta0, meta2);
  else {
    mdbx_assert(env, head == meta2);
    target = mdbx_meta_ancient(prefer_steady, env, meta0, meta1);
  }

  /* LY: step#2 - update meta-page. */
  mdbx_debug("writing meta%" PRIaPGNO " = root %" PRIaPGNO "/%" PRIaPGNO
             ", geo %" PRIaPGNO "/%" PRIaPGNO "-%" PRIaPGNO "/%" PRIaPGNO
             " +%u -%u, txn_id %" PRIaTXN ", %s",
             data_page(target)->mp_pgno, pending->mm_dbs[MAIN_DBI].md_root,
             pending->mm_dbs[FREE_DBI].md_root, pending->mm_geo.lower,
             pending->mm_geo.next, pending->mm_geo.now, pending->mm_geo.upper,
             pending->mm_geo.grow, pending->mm_geo.shrink,
             pending->mm_txnid_a.inconsistent, mdbx_durable_str(pending));

  mdbx_debug("meta0: %s, %s, txn_id %" PRIaTXN ", root %" PRIaPGNO
             "/%" PRIaPGNO,
             (meta0 == head) ? "head" : (meta0 == target) ? "tail" : "stay",
             mdbx_durable_str(meta0), mdbx_meta_txnid_fluid(env, meta0),
             meta0->mm_dbs[MAIN_DBI].md_root, meta0->mm_dbs[FREE_DBI].md_root);
  mdbx_debug("meta1: %s, %s, txn_id %" PRIaTXN ", root %" PRIaPGNO
             "/%" PRIaPGNO,
             (meta1 == head) ? "head" : (meta1 == target) ? "tail" : "stay",
             mdbx_durable_str(meta1), mdbx_meta_txnid_fluid(env, meta1),
             meta1->mm_dbs[MAIN_DBI].md_root, meta1->mm_dbs[FREE_DBI].md_root);
  mdbx_debug("meta2: %s, %s, txn_id %" PRIaTXN ", root %" PRIaPGNO
             "/%" PRIaPGNO,
             (meta2 == head) ? "head" : (meta2 == target) ? "tail" : "stay",
             mdbx_durable_str(meta2), mdbx_meta_txnid_fluid(env, meta2),
             meta2->mm_dbs[MAIN_DBI].md_root, meta2->mm_dbs[FREE_DBI].md_root);

  mdbx_assert(env, !mdbx_meta_eq(env, pending, meta0));
  mdbx_assert(env, !mdbx_meta_eq(env, pending, meta1));
  mdbx_assert(env, !mdbx_meta_eq(env, pending, meta2));

  mdbx_assert(env, ((env->me_flags ^ flags) & MDBX_WRITEMAP) == 0);
  mdbx_ensure(env, target == head || mdbx_meta_txnid_stable(env, target) <
                                         pending->mm_txnid_a.inconsistent);
  if (env->me_flags & MDBX_WRITEMAP) {
    mdbx_jitter4testing(true);
    if (likely(target != head)) {
      /* LY: 'invalidate' the meta. */
      mdbx_meta_update_begin(env, target, pending->mm_txnid_a.inconsistent);
      target->mm_datasync_sign = MDBX_DATASIGN_WEAK;
#ifndef NDEBUG
      /* debug: provoke failure to catch a violators, but don't touch mm_psize
       * and mm_flags to allow readers catch actual pagesize. */
      uint8_t *provoke_begin = (uint8_t *)&target->mm_dbs[FREE_DBI].md_root;
      uint8_t *provoke_end = (uint8_t *)&target->mm_datasync_sign;
      memset(provoke_begin, 0xCC, provoke_end - provoke_begin);
      mdbx_jitter4testing(false);
#endif

      /* LY: update info */
      target->mm_geo = pending->mm_geo;
      target->mm_dbs[FREE_DBI] = pending->mm_dbs[FREE_DBI];
      target->mm_dbs[MAIN_DBI] = pending->mm_dbs[MAIN_DBI];
      target->mm_canary = pending->mm_canary;
      target->mm_pages_retired = pending->mm_pages_retired;
      mdbx_jitter4testing(true);
      mdbx_flush_noncoherent_cpu_writeback();

      /* LY: 'commit' the meta */
      mdbx_meta_update_end(env, target, pending->mm_txnid_b.inconsistent);
      mdbx_jitter4testing(true);
    } else {
      /* dangerous case (target == head), only mm_datasync_sign could
       * me updated, check assertions once again */
      mdbx_ensure(env, mdbx_meta_txnid_stable(env, head) ==
                               pending->mm_txnid_a.inconsistent &&
                           !META_IS_STEADY(head) && META_IS_STEADY(pending));
      mdbx_ensure(env, memcmp(&head->mm_geo, &pending->mm_geo,
                              sizeof(head->mm_geo)) == 0);
      mdbx_ensure(env, memcmp(&head->mm_dbs, &pending->mm_dbs,
                              sizeof(head->mm_dbs)) == 0);
      mdbx_ensure(env, memcmp(&head->mm_canary, &pending->mm_canary,
                              sizeof(head->mm_canary)) == 0);
    }
    target->mm_datasync_sign = pending->mm_datasync_sign;
    mdbx_flush_noncoherent_cpu_writeback();
    mdbx_jitter4testing(true);
  } else {
    rc = mdbx_pwrite(env->me_fd, pending, sizeof(MDBX_meta),
                     (uint8_t *)target - env->me_map);
    if (unlikely(rc != MDBX_SUCCESS)) {
    undo:
      mdbx_debug("%s", "write failed, disk error?");
      /* On a failure, the pagecache still contains the new data.
       * Try write some old data back, to prevent it from being used. */
      mdbx_pwrite(env->me_fd, (void *)target, sizeof(MDBX_meta),
                  (uint8_t *)target - env->me_map);
      goto fail;
    }
    mdbx_invalidate_mmap_noncoherent_cache(target, sizeof(MDBX_meta));
  }

  /* LY: step#3 - sync meta-pages. */
  mdbx_assert(env, ((env->me_flags ^ flags) & MDBX_WRITEMAP) == 0);
  if ((flags & (MDBX_NOSYNC | MDBX_NOMETASYNC)) == 0) {
    mdbx_assert(env, ((flags ^ env->me_flags) & MDBX_WRITEMAP) == 0);
    if (flags & MDBX_WRITEMAP) {
      const size_t offset = (uint8_t *)data_page(head) - env->me_dxb_mmap.dxb;
      const size_t paged_offset = offset & ~(env->me_os_psize - 1);
      const size_t paged_length = roundup_powerof2(
          env->me_psize + offset - paged_offset, env->me_os_psize);
      rc = mdbx_msync(&env->me_dxb_mmap, paged_offset, paged_length,
                      flags & MDBX_MAPASYNC);
      if (unlikely(rc != MDBX_SUCCESS))
        goto fail;
    } else {
      rc = mdbx_filesync(env->me_fd, MDBX_SYNC_DATA | MDBX_SYNC_IODQ);
      if (rc != MDBX_SUCCESS)
        goto undo;
    }
    *env->me_meta_sync_txnid = (uint32_t)pending->mm_txnid_a.inconsistent;
  }

  /* LY: shrink datafile if needed */
  if (unlikely(shrink)) {
    mdbx_verbose("shrink to %" PRIaPGNO " pages (-%" PRIaPGNO ")",
                 pending->mm_geo.now, shrink);
    rc = mdbx_mapresize(env, pending->mm_geo.now, pending->mm_geo.upper);
    if (MDBX_IS_ERROR(rc))
      goto fail;
  }

  return MDBX_SUCCESS;

fail:
  env->me_flags |= MDBX_FATAL_ERROR;
  return rc;
}

int __cold mdbx_env_get_maxkeysize(MDBX_env *env) {
  if (!env || env->me_signature != MDBX_ME_SIGNATURE || !env->me_maxkey_limit)
    return (MDBX_EINVAL > 0) ? -MDBX_EINVAL : MDBX_EINVAL;
  return env->me_maxkey_limit;
}

#define mdbx_nodemax(pagesize)                                                 \
  (((((pagesize)-PAGEHDRSZ) / MDBX_MINKEYS) & ~(uintptr_t)1) - sizeof(indx_t))

#define mdbx_maxkey(nodemax) ((nodemax)-NODESIZE - sizeof(MDBX_db))

#define mdbx_maxgc_ov1page(pagesize)                                           \
  (((pagesize)-PAGEHDRSZ) / sizeof(pgno_t) - 1)

static void __cold mdbx_setup_pagesize(MDBX_env *env, const size_t pagesize) {
  STATIC_ASSERT(PTRDIFF_MAX > MAX_MAPSIZE);
  STATIC_ASSERT(MIN_PAGESIZE > sizeof(MDBX_page) + sizeof(MDBX_meta));
  mdbx_ensure(env, is_powerof2(pagesize));
  mdbx_ensure(env, pagesize >= MIN_PAGESIZE);
  mdbx_ensure(env, pagesize <= MAX_PAGESIZE);
  env->me_psize = (unsigned)pagesize;

  STATIC_ASSERT(mdbx_maxgc_ov1page(MIN_PAGESIZE) > 4);
  STATIC_ASSERT(mdbx_maxgc_ov1page(MAX_PAGESIZE) < MDBX_DPL_TXNFULL);
  const intptr_t maxgc_ov1page = (pagesize - PAGEHDRSZ) / sizeof(pgno_t) - 1;
  mdbx_ensure(env,
              maxgc_ov1page > 42 && maxgc_ov1page < (intptr_t)MDBX_DPL_TXNFULL);
  env->me_maxgc_ov1page = (unsigned)maxgc_ov1page;

  STATIC_ASSERT(mdbx_nodemax(MIN_PAGESIZE) > 42);
  STATIC_ASSERT(mdbx_nodemax(MAX_PAGESIZE) < UINT16_MAX);
  const intptr_t nodemax = mdbx_nodemax(pagesize);
  mdbx_ensure(env, nodemax > 42 && nodemax < UINT16_MAX && nodemax % 2 == 0);
  env->me_nodemax = (unsigned)nodemax;

  STATIC_ASSERT(mdbx_maxkey(MIN_PAGESIZE) > 42);
  STATIC_ASSERT(mdbx_maxkey(MIN_PAGESIZE) < MIN_PAGESIZE);
  STATIC_ASSERT(mdbx_maxkey(MAX_PAGESIZE) > 42);
  STATIC_ASSERT(mdbx_maxkey(MAX_PAGESIZE) < MAX_PAGESIZE);
  const intptr_t maxkey_limit = mdbx_maxkey(env->me_nodemax);
  mdbx_ensure(env, maxkey_limit > 42 && (size_t)maxkey_limit < pagesize &&
                       maxkey_limit % 2 == 0);
  env->me_maxkey_limit = (unsigned)maxkey_limit;

  env->me_psize2log = log2n(pagesize);
  mdbx_assert(env, pgno2bytes(env, 1) == pagesize);
  mdbx_assert(env, bytes2pgno(env, pagesize + pagesize) == 2);
}

int __cold mdbx_env_create(MDBX_env **penv) {
  MDBX_env *env = mdbx_calloc(1, sizeof(MDBX_env));
  if (unlikely(!env))
    return MDBX_ENOMEM;

  env->me_maxreaders = DEFAULT_READERS;
  env->me_maxdbs = env->me_numdbs = CORE_DBS;
  env->me_fd = INVALID_HANDLE_VALUE;
  env->me_lfd = INVALID_HANDLE_VALUE;
  env->me_pid = mdbx_getpid();

  int rc;
  const size_t os_psize = mdbx_syspagesize();
  if (unlikely(!is_powerof2(os_psize) || os_psize < MIN_PAGESIZE)) {
    mdbx_error("unsuitable system pagesize %" PRIuPTR, os_psize);
    rc = MDBX_INCOMPATIBLE;
    goto bailout;
  }
  env->me_os_psize = (unsigned)os_psize;
  mdbx_setup_pagesize(env, env->me_os_psize);

  rc = mdbx_fastmutex_init(&env->me_dbi_lock);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout;

#if defined(_WIN32) || defined(_WIN64)
  mdbx_srwlock_Init(&env->me_remap_guard);
  InitializeCriticalSection(&env->me_windowsbug_lock);
#else
  rc = mdbx_fastmutex_init(&env->me_remap_guard);
  if (unlikely(rc != MDBX_SUCCESS)) {
    mdbx_fastmutex_destroy(&env->me_dbi_lock);
    goto bailout;
  }
  rc = mdbx_fastmutex_init(&env->me_lckless_stub.wmutex);
  if (unlikely(rc != MDBX_SUCCESS)) {
    mdbx_fastmutex_destroy(&env->me_remap_guard);
    mdbx_fastmutex_destroy(&env->me_dbi_lock);
    goto bailout;
  }
#endif /* Windows */

  VALGRIND_CREATE_MEMPOOL(env, 0, 0);
  env->me_signature = MDBX_ME_SIGNATURE;
  *penv = env;
  return MDBX_SUCCESS;

bailout:
  mdbx_free(env);
  *penv = nullptr;
  return rc;
}

__cold LIBMDBX_API int
mdbx_env_set_geometry(MDBX_env *env, intptr_t size_lower, intptr_t size_now,
                      intptr_t size_upper, intptr_t growth_step,
                      intptr_t shrink_threshold, intptr_t pagesize) {
  if (unlikely(!env))
    return MDBX_EINVAL;

  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

#if MDBX_TXN_CHECKPID
  if (unlikely(env->me_pid != mdbx_getpid()))
    env->me_flags |= MDBX_FATAL_ERROR;
#endif /* MDBX_TXN_CHECKPID */

  if (unlikely(env->me_flags & MDBX_FATAL_ERROR))
    return MDBX_PANIC;

  const bool inside_txn =
      (env->me_txn0 && env->me_txn0->mt_owner == mdbx_thread_self());

#if MDBX_DEBUG
  if (growth_step < 0)
    growth_step = 1;
  if (shrink_threshold < 0)
    shrink_threshold = 1;
#endif

  bool need_unlock = false;
  int rc = MDBX_PROBLEM;
  if (env->me_map) {
    /* env already mapped */
    if (unlikely(env->me_flags & MDBX_RDONLY))
      return MDBX_EACCESS;

    if (!inside_txn) {
      int err = mdbx_txn_lock(env, false);
      if (unlikely(err != MDBX_SUCCESS))
        return err;
      need_unlock = true;
    }
    MDBX_meta *head = mdbx_meta_head(env);
    if (!inside_txn) {
      env->me_txn0->mt_txnid = meta_txnid(env, head, false);
      mdbx_find_oldest(env->me_txn0);
    }

    /* get untouched params from DB */
    if (pagesize < 0)
      pagesize = env->me_psize;
    if (size_lower < 0)
      size_lower = pgno2bytes(env, head->mm_geo.lower);
    if (size_now < 0)
      size_now = pgno2bytes(env, head->mm_geo.now);
    if (size_upper < 0)
      size_upper = pgno2bytes(env, head->mm_geo.upper);
    if (growth_step < 0)
      growth_step = pgno2bytes(env, head->mm_geo.grow);
    if (shrink_threshold < 0)
      shrink_threshold = pgno2bytes(env, head->mm_geo.shrink);

    if (pagesize != (intptr_t)env->me_psize) {
      rc = MDBX_EINVAL;
      goto bailout;
    }
    const size_t usedbytes =
        pgno2bytes(env, mdbx_find_largest(env, head->mm_geo.next));
    if ((size_t)size_upper < usedbytes) {
      rc = MDBX_MAP_FULL;
      goto bailout;
    }
    if ((size_t)size_now < usedbytes)
      size_now = usedbytes;
  } else {
    /* env NOT yet mapped */
    if (unlikely(inside_txn))
      return MDBX_PANIC;

    if (pagesize < 0) {
      pagesize = env->me_os_psize;
      if ((uintptr_t)pagesize > MAX_PAGESIZE)
        pagesize = MAX_PAGESIZE;
      mdbx_assert(env, (uintptr_t)pagesize >= MIN_PAGESIZE);
    }
  }

  if (pagesize == 0)
    pagesize = MIN_PAGESIZE;
  else if (pagesize == INTPTR_MAX)
    pagesize = MAX_PAGESIZE;

  if (pagesize < (intptr_t)MIN_PAGESIZE || pagesize > (intptr_t)MAX_PAGESIZE ||
      !is_powerof2(pagesize)) {
    rc = MDBX_EINVAL;
    goto bailout;
  }

  if (size_lower <= 0) {
    size_lower = MIN_MAPSIZE;
    if (MIN_MAPSIZE / pagesize < MIN_PAGENO)
      size_lower = MIN_PAGENO * pagesize;
  }

  if (size_now <= 0) {
    size_now = DEFAULT_MAPSIZE;
    if (size_now < size_lower)
      size_now = size_lower;
    if (size_upper >= size_lower && size_now > size_upper)
      size_now = size_upper;
  }

  if (size_upper <= 0) {
    if ((size_t)size_now >= MAX_MAPSIZE / 2)
      size_upper = MAX_MAPSIZE;
    else if (MAX_MAPSIZE != MAX_MAPSIZE32 &&
             (size_t)size_now >= MAX_MAPSIZE32 / 2 &&
             (size_t)size_now <= MAX_MAPSIZE32 / 4 * 3)
      size_upper = MAX_MAPSIZE32;
    else {
      size_upper = size_now + size_now;
      if ((size_t)size_upper < DEFAULT_MAPSIZE * 2)
        size_upper = DEFAULT_MAPSIZE * 2;
    }
    if ((size_t)size_upper / pagesize > MAX_PAGENO)
      size_upper = pagesize * MAX_PAGENO;
  }

  if (unlikely(size_lower < (intptr_t)MIN_MAPSIZE || size_lower > size_upper)) {
    rc = MDBX_EINVAL;
    goto bailout;
  }

  if ((uint64_t)size_lower / pagesize < MIN_PAGENO) {
    rc = MDBX_EINVAL;
    goto bailout;
  }

  if (unlikely((size_t)size_upper > MAX_MAPSIZE ||
               (uint64_t)size_upper / pagesize > MAX_PAGENO)) {
    rc = MDBX_TOO_LARGE;
    goto bailout;
  }

  size_lower = roundup_powerof2(size_lower, env->me_os_psize);
  size_upper = roundup_powerof2(size_upper, env->me_os_psize);
  size_now = roundup_powerof2(size_now, env->me_os_psize);

  /* LY: подбираем значение size_upper:
   *  - кратное размеру системной страницы
   *  - без нарушения MAX_MAPSIZE и MAX_PAGENO */
  while (unlikely((size_t)size_upper > MAX_MAPSIZE ||
                  (uint64_t)size_upper / pagesize > MAX_PAGENO)) {
    if ((size_t)size_upper < env->me_os_psize + MIN_MAPSIZE ||
        (size_t)size_upper < env->me_os_psize * (MIN_PAGENO + 1)) {
      /* паранойа на случай переполнения при невероятных значениях */
      rc = MDBX_EINVAL;
      goto bailout;
    }
    size_upper -= env->me_os_psize;
    if ((size_t)size_upper < (size_t)size_lower)
      size_lower = size_upper;
  }
  mdbx_assert(env, (size_upper - size_lower) % env->me_os_psize == 0);

  if (size_now < size_lower)
    size_now = size_lower;
  if (size_now > size_upper)
    size_now = size_upper;

  if (growth_step < 0) {
    growth_step = ((size_t)(size_upper - size_lower)) / 42;
    if (growth_step > size_lower)
      growth_step = size_lower;
    if (growth_step < 65536)
      growth_step = 65536;
    if ((size_t)growth_step > MEGABYTE * 16)
      growth_step = MEGABYTE * 16;
  }
  if (growth_step == 0 && shrink_threshold > 0)
    growth_step = 1;
  growth_step = roundup_powerof2(growth_step, env->me_os_psize);
  if (bytes2pgno(env, growth_step) > UINT16_MAX)
    growth_step = pgno2bytes(env, UINT16_MAX);

  if (shrink_threshold < 0)
    shrink_threshold = growth_step + growth_step;
  shrink_threshold = roundup_powerof2(shrink_threshold, env->me_os_psize);
  if (bytes2pgno(env, shrink_threshold) > UINT16_MAX)
    shrink_threshold = pgno2bytes(env, UINT16_MAX);

  /* save user's geo-params for future open/create */
  env->me_dbgeo.lower = size_lower;
  env->me_dbgeo.now = size_now;
  env->me_dbgeo.upper = size_upper;
  env->me_dbgeo.grow = growth_step;
  env->me_dbgeo.shrink = shrink_threshold;
  rc = MDBX_SUCCESS;

  mdbx_ensure(env, pagesize >= MIN_PAGESIZE);
  mdbx_ensure(env, pagesize <= MAX_PAGESIZE);
  mdbx_ensure(env, is_powerof2(pagesize));
  mdbx_ensure(env, is_powerof2(env->me_os_psize));

  mdbx_ensure(env, env->me_dbgeo.lower >= MIN_MAPSIZE);
  mdbx_ensure(env, env->me_dbgeo.lower / pagesize >= MIN_PAGENO);
  mdbx_ensure(env, env->me_dbgeo.lower % pagesize == 0);
  mdbx_ensure(env, env->me_dbgeo.lower % env->me_os_psize == 0);

  mdbx_ensure(env, env->me_dbgeo.upper <= MAX_MAPSIZE);
  mdbx_ensure(env, env->me_dbgeo.upper / pagesize <= MAX_PAGENO);
  mdbx_ensure(env, env->me_dbgeo.upper % pagesize == 0);
  mdbx_ensure(env, env->me_dbgeo.upper % env->me_os_psize == 0);

  mdbx_ensure(env, env->me_dbgeo.now >= env->me_dbgeo.lower);
  mdbx_ensure(env, env->me_dbgeo.now <= env->me_dbgeo.upper);
  mdbx_ensure(env, env->me_dbgeo.now % pagesize == 0);
  mdbx_ensure(env, env->me_dbgeo.now % env->me_os_psize == 0);

  mdbx_ensure(env, env->me_dbgeo.grow % pagesize == 0);
  mdbx_ensure(env, env->me_dbgeo.grow % env->me_os_psize == 0);
  mdbx_ensure(env, env->me_dbgeo.shrink % pagesize == 0);
  mdbx_ensure(env, env->me_dbgeo.shrink % env->me_os_psize == 0);

  if (env->me_map) {
    /* apply new params to opened environment */
    mdbx_ensure(env, pagesize == (intptr_t)env->me_psize);
    MDBX_meta meta;
    MDBX_meta *head = nullptr;
    const mdbx_geo_t *current_geo;
    if (inside_txn) {
      current_geo = &env->me_txn->mt_geo;
    } else {
      head = mdbx_meta_head(env);
      meta = *head;
      current_geo = &meta.mm_geo;
    }

    mdbx_geo_t new_geo;
    new_geo.lower = bytes2pgno(env, env->me_dbgeo.lower);
    new_geo.now = bytes2pgno(env, env->me_dbgeo.now);
    new_geo.upper = bytes2pgno(env, env->me_dbgeo.upper);
    new_geo.grow = (uint16_t)bytes2pgno(env, env->me_dbgeo.grow);
    new_geo.shrink = (uint16_t)bytes2pgno(env, env->me_dbgeo.shrink);
    new_geo.next = current_geo->next;

    mdbx_ensure(env,
                pgno_align2os_bytes(env, new_geo.lower) == env->me_dbgeo.lower);
    mdbx_ensure(env,
                pgno_align2os_bytes(env, new_geo.upper) == env->me_dbgeo.upper);
    mdbx_ensure(env,
                pgno_align2os_bytes(env, new_geo.now) == env->me_dbgeo.now);
    mdbx_ensure(env,
                pgno_align2os_bytes(env, new_geo.grow) == env->me_dbgeo.grow);
    mdbx_ensure(env, pgno_align2os_bytes(env, new_geo.shrink) ==
                         env->me_dbgeo.shrink);

    mdbx_ensure(env, env->me_dbgeo.lower >= MIN_MAPSIZE);
    mdbx_ensure(env, new_geo.lower >= MIN_PAGENO);
    mdbx_ensure(env, env->me_dbgeo.upper <= MAX_MAPSIZE);
    mdbx_ensure(env, new_geo.upper <= MAX_PAGENO);
    mdbx_ensure(env, new_geo.now >= new_geo.next);
    mdbx_ensure(env, new_geo.upper >= new_geo.now);
    mdbx_ensure(env, new_geo.now >= new_geo.lower);

    if (memcmp(current_geo, &new_geo, sizeof(mdbx_geo_t)) != 0) {
#if defined(_WIN32) || defined(_WIN64)
      /* Was DB shrinking disabled before and now it will be enabled? */
      if (new_geo.lower < new_geo.upper && new_geo.shrink &&
          !(current_geo->lower < current_geo->upper && current_geo->shrink)) {
        if (!env->me_lck) {
          rc = MDBX_EPERM;
          goto bailout;
        }
        rc = mdbx_rdt_lock(env);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;

        /* Check if there are any reading threads that do not use the SRWL */
        const size_t CurrentTid = GetCurrentThreadId();
        const MDBX_reader *const begin = env->me_lck->mti_readers;
        const MDBX_reader *const end = begin + env->me_lck->mti_numreaders;
        for (const MDBX_reader *reader = begin; reader < end; ++reader) {
          if (reader->mr_pid == env->me_pid && reader->mr_tid &&
              reader->mr_tid != CurrentTid) {
            /* At least one thread may don't use SRWL */
            rc = MDBX_EPERM;
            break;
          }
        }

        mdbx_rdt_unlock(env);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
      }
#endif

      if (new_geo.now != current_geo->now ||
          new_geo.upper != current_geo->upper) {
        rc = mdbx_mapresize(env, new_geo.now, new_geo.upper);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
        mdbx_assert(env, (head == nullptr) == inside_txn);
        if (head)
          head = /* base address could be changed */ mdbx_meta_head(env);
      }
      if (inside_txn) {
        env->me_txn->mt_geo = new_geo;
        if ((env->me_txn->mt_flags & MDBX_TXN_DIRTY) == 0) {
          env->me_txn->mt_flags |= MDBX_TXN_DIRTY;
          *env->me_unsynced_pages += 1;
        }
      } else {
        *env->me_unsynced_pages += 1;
        mdbx_meta_set_txnid(
            env, &meta, safe64_txnid_next(mdbx_meta_txnid_stable(env, head)));
        rc = mdbx_sync_locked(env, env->me_flags, &meta);
      }
    }
  } else if (pagesize != (intptr_t)env->me_psize) {
    mdbx_setup_pagesize(env, pagesize);
  }

bailout:
  if (need_unlock)
    mdbx_txn_unlock(env);
  return rc;
}

int __cold mdbx_env_set_mapsize(MDBX_env *env, size_t size) {
  return mdbx_env_set_geometry(env, size, size, size, -1, -1, -1);
}

int __cold mdbx_env_set_maxdbs(MDBX_env *env, MDBX_dbi dbs) {
  if (unlikely(dbs > MAX_DBI))
    return MDBX_EINVAL;

  if (unlikely(!env))
    return MDBX_EINVAL;

  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(env->me_map))
    return MDBX_EPERM;

  env->me_maxdbs = dbs + CORE_DBS;
  return MDBX_SUCCESS;
}

int __cold mdbx_env_set_maxreaders(MDBX_env *env, unsigned readers) {
  if (unlikely(readers < 1 || readers > MDBX_READERS_LIMIT))
    return MDBX_EINVAL;

  if (unlikely(!env))
    return MDBX_EINVAL;

  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(env->me_map))
    return MDBX_EPERM;

  env->me_maxreaders = readers;
  return MDBX_SUCCESS;
}

int __cold mdbx_env_get_maxreaders(MDBX_env *env, unsigned *readers) {
  if (!env || !readers)
    return MDBX_EINVAL;

  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

  *readers = env->me_maxreaders;
  return MDBX_SUCCESS;
}

/* Further setup required for opening an MDBX environment */
static int __cold mdbx_setup_dxb(MDBX_env *env, const int lck_rc) {
  uint64_t filesize_before;
  MDBX_meta meta;
  int rc = MDBX_RESULT_FALSE;
  int err = mdbx_read_header(env, &meta, &filesize_before);
  if (unlikely(err != MDBX_SUCCESS)) {
    if (lck_rc != /* lck exclusive */ MDBX_RESULT_TRUE || err != MDBX_ENODATA ||
        (env->me_flags & MDBX_RDONLY) != 0)
      return err;

    mdbx_debug("%s", "create new database");
    rc = /* new database */ MDBX_RESULT_TRUE;

    if (!env->me_dbgeo.now) {
      /* set defaults if not configured */
      err = mdbx_env_set_mapsize(env, DEFAULT_MAPSIZE);
      if (unlikely(err != MDBX_SUCCESS))
        return err;
    }

    void *buffer = mdbx_calloc(NUM_METAS, env->me_psize);
    if (!buffer)
      return MDBX_ENOMEM;

    meta = *mdbx_init_metas(env, buffer);
    err = mdbx_pwrite(env->me_fd, buffer, env->me_psize * NUM_METAS, 0);
    mdbx_free(buffer);
    if (unlikely(err != MDBX_SUCCESS))
      return err;

    err = mdbx_ftruncate(env->me_fd, filesize_before = env->me_dbgeo.now);
    if (unlikely(err != MDBX_SUCCESS))
      return err;

#ifndef NDEBUG /* just for checking */
    err = mdbx_read_header(env, &meta, &filesize_before);
    if (unlikely(err != MDBX_SUCCESS))
      return err;
#endif
  }

  mdbx_verbose("header: root %" PRIaPGNO "/%" PRIaPGNO ", geo %" PRIaPGNO
               "/%" PRIaPGNO "-%" PRIaPGNO "/%" PRIaPGNO
               " +%u -%u, txn_id %" PRIaTXN ", %s",
               meta.mm_dbs[MAIN_DBI].md_root, meta.mm_dbs[FREE_DBI].md_root,
               meta.mm_geo.lower, meta.mm_geo.next, meta.mm_geo.now,
               meta.mm_geo.upper, meta.mm_geo.grow, meta.mm_geo.shrink,
               meta.mm_txnid_a.inconsistent, mdbx_durable_str(&meta));

  mdbx_setup_pagesize(env, meta.mm_psize);
  const size_t used_bytes = pgno2bytes(env, meta.mm_geo.next);
  const size_t used_aligned2os_bytes =
      roundup_powerof2(used_bytes, env->me_os_psize);
  if ((env->me_flags & MDBX_RDONLY) /* readonly */
      || lck_rc != MDBX_RESULT_TRUE /* not exclusive */) {
    /* use present params from db */
    const size_t pagesize = meta.mm_psize;
    err = mdbx_env_set_geometry(
        env, meta.mm_geo.lower * pagesize, meta.mm_geo.now * pagesize,
        meta.mm_geo.upper * pagesize, meta.mm_geo.grow * pagesize,
        meta.mm_geo.shrink * pagesize, meta.mm_psize);
    if (unlikely(err != MDBX_SUCCESS)) {
      mdbx_error("%s", "could not use present dbsize-params from db");
      return MDBX_INCOMPATIBLE;
    }
  } else if (env->me_dbgeo.now) {
    /* silently growth to last used page */
    if (env->me_dbgeo.now < used_aligned2os_bytes)
      env->me_dbgeo.now = used_aligned2os_bytes;
    if (env->me_dbgeo.upper < used_aligned2os_bytes)
      env->me_dbgeo.upper = used_aligned2os_bytes;

    /* apply preconfigured params, but only if substantial changes:
     *  - upper or lower limit changes
     *  - shrink threshold or growth step
     * But ignore change just a 'now/current' size. */
    if (bytes_align2os_bytes(env, env->me_dbgeo.upper) !=
            pgno2bytes(env, meta.mm_geo.upper) ||
        bytes_align2os_bytes(env, env->me_dbgeo.lower) !=
            pgno2bytes(env, meta.mm_geo.lower) ||
        bytes_align2os_bytes(env, env->me_dbgeo.shrink) !=
            pgno2bytes(env, meta.mm_geo.shrink) ||
        bytes_align2os_bytes(env, env->me_dbgeo.grow) !=
            pgno2bytes(env, meta.mm_geo.grow)) {

      if (env->me_dbgeo.shrink && env->me_dbgeo.now > used_bytes)
        /* pre-shrink if enabled */
        env->me_dbgeo.now = used_bytes + env->me_dbgeo.shrink -
                            used_bytes % env->me_dbgeo.shrink;

      err = mdbx_env_set_geometry(env, env->me_dbgeo.lower, env->me_dbgeo.now,
                                  env->me_dbgeo.upper, env->me_dbgeo.grow,
                                  env->me_dbgeo.shrink, meta.mm_psize);
      if (unlikely(err != MDBX_SUCCESS)) {
        mdbx_error("%s", "could not apply preconfigured dbsize-params to db");
        return MDBX_INCOMPATIBLE;
      }

      /* update meta fields */
      meta.mm_geo.now = bytes2pgno(env, env->me_dbgeo.now);
      meta.mm_geo.lower = bytes2pgno(env, env->me_dbgeo.lower);
      meta.mm_geo.upper = bytes2pgno(env, env->me_dbgeo.upper);
      meta.mm_geo.grow = (uint16_t)bytes2pgno(env, env->me_dbgeo.grow);
      meta.mm_geo.shrink = (uint16_t)bytes2pgno(env, env->me_dbgeo.shrink);

      mdbx_verbose("amended: root %" PRIaPGNO "/%" PRIaPGNO ", geo %" PRIaPGNO
                   "/%" PRIaPGNO "-%" PRIaPGNO "/%" PRIaPGNO
                   " +%u -%u, txn_id %" PRIaTXN ", %s",
                   meta.mm_dbs[MAIN_DBI].md_root, meta.mm_dbs[FREE_DBI].md_root,
                   meta.mm_geo.lower, meta.mm_geo.next, meta.mm_geo.now,
                   meta.mm_geo.upper, meta.mm_geo.grow, meta.mm_geo.shrink,
                   meta.mm_txnid_a.inconsistent, mdbx_durable_str(&meta));
    } else {
      /* fetch back 'now/current' size, since it was ignored during comparison
       * and may differ. */
      env->me_dbgeo.now = pgno_align2os_bytes(env, meta.mm_geo.now);
    }
    mdbx_ensure(env, meta.mm_geo.now >= meta.mm_geo.next);
  } else {
    /* geo-params are not pre-configured by user,
     * get current values from the meta. */
    env->me_dbgeo.now = pgno2bytes(env, meta.mm_geo.now);
    env->me_dbgeo.lower = pgno2bytes(env, meta.mm_geo.lower);
    env->me_dbgeo.upper = pgno2bytes(env, meta.mm_geo.upper);
    env->me_dbgeo.grow = pgno2bytes(env, meta.mm_geo.grow);
    env->me_dbgeo.shrink = pgno2bytes(env, meta.mm_geo.shrink);
  }

  mdbx_ensure(env,
              pgno_align2os_bytes(env, meta.mm_geo.now) == env->me_dbgeo.now);
  mdbx_ensure(env, env->me_dbgeo.now >= used_bytes);
  if (unlikely(filesize_before != env->me_dbgeo.now)) {
    if (lck_rc != /* lck exclusive */ MDBX_RESULT_TRUE) {
      mdbx_verbose("filesize mismatch (expect %" PRIuPTR "b/%" PRIaPGNO
                   "p, have %" PRIu64 "b/%" PRIaPGNO "p), "
                   "assume other process working",
                   env->me_dbgeo.now, bytes2pgno(env, env->me_dbgeo.now),
                   filesize_before, bytes2pgno(env, (size_t)filesize_before));
    } else {
      mdbx_notice("filesize mismatch (expect %" PRIuSIZE "b/%" PRIaPGNO
                  "p, have %" PRIu64 "b/%" PRIaPGNO "p)",
                  env->me_dbgeo.now, bytes2pgno(env, env->me_dbgeo.now),
                  filesize_before, bytes2pgno(env, (size_t)filesize_before));
      if (filesize_before < used_bytes) {
        mdbx_error("last-page beyond end-of-file (last %" PRIaPGNO
                   ", have %" PRIaPGNO ")",
                   meta.mm_geo.next, bytes2pgno(env, (size_t)filesize_before));
        return MDBX_CORRUPTED;
      }

      if (env->me_flags & MDBX_RDONLY) {
        if (filesize_before & (env->me_os_psize - 1)) {
          mdbx_error("%s", "filesize should be rounded-up to system page");
          return MDBX_WANNA_RECOVERY;
        }
        mdbx_warning("%s", "ignore filesize mismatch in readonly-mode");
      } else {
        mdbx_verbose("will resize datafile to %" PRIuSIZE " bytes, %" PRIaPGNO
                     " pages",
                     env->me_dbgeo.now, bytes2pgno(env, env->me_dbgeo.now));
      }
    }
  }

  err = mdbx_mmap(env->me_flags, &env->me_dxb_mmap, env->me_dbgeo.now,
                  env->me_dbgeo.upper, lck_rc);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

#if defined(MADV_DODUMP) && defined(MADV_DONTDUMP)
  const size_t meta_length = pgno2bytes(env, NUM_METAS);
  (void)madvise(env->me_map, meta_length, MADV_DODUMP);
  (void)madvise(env->me_map + meta_length, env->me_dxb_mmap.limit - meta_length,
                (mdbx_runtime_flags & MDBX_DBG_DUMP) ? MADV_DODUMP
                                                     : MADV_DONTDUMP);
#endif

  *env->me_discarded_tail = bytes2pgno(env, used_aligned2os_bytes);
  if (used_aligned2os_bytes < env->me_dxb_mmap.current) {
#if defined(MADV_REMOVE)
    if (lck_rc && (env->me_flags & MDBX_WRITEMAP) != 0) {
      mdbx_notice("open-MADV_%s %u..%u", "REMOVE", *env->me_discarded_tail,
                  bytes2pgno(env, env->me_dxb_mmap.current));
      (void)madvise(env->me_map + used_aligned2os_bytes,
                    env->me_dxb_mmap.current - used_aligned2os_bytes,
                    MADV_REMOVE);
    }
#endif /* MADV_REMOVE */
#if defined(MADV_DONTNEED)
    mdbx_notice("open-MADV_%s %u..%u", "DONTNEED", *env->me_discarded_tail,
                bytes2pgno(env, env->me_dxb_mmap.current));
    (void)madvise(env->me_map + used_aligned2os_bytes,
                  env->me_dxb_mmap.current - used_aligned2os_bytes,
                  MADV_DONTNEED);
#elif defined(POSIX_MADV_DONTNEED)
    (void)madvise(env->me_map + used_aligned2os_bytes,
                  env->me_dxb_mmap.current - used_aligned2os_bytes,
                  POSIX_MADV_DONTNEED);
#elif defined(POSIX_FADV_DONTNEED)
    (void)posix_fadvise(env->me_fd, used_aligned2os_bytes,
                        env->me_dxb_mmap.current - used_aligned2os_bytes,
                        POSIX_FADV_DONTNEED);
#endif /* MADV_DONTNEED */
  }

#ifdef MDBX_USE_VALGRIND
  env->me_valgrind_handle =
      VALGRIND_CREATE_BLOCK(env->me_map, env->me_dxb_mmap.limit, "mdbx");
#endif

  const bool readahead = (env->me_flags & MDBX_NORDAHEAD) == 0 &&
                         mdbx_is_readahead_reasonable(env->me_dxb_mmap.current,
                                                      0) == MDBX_RESULT_TRUE;
  err = mdbx_set_readahead(env, 0, env->me_dxb_mmap.current, readahead);
  if (err != MDBX_SUCCESS)
    return err;

  mdbx_assert(env, used_bytes >= pgno2bytes(env, NUM_METAS) &&
                       used_bytes <= env->me_dxb_mmap.limit);
#if defined(MDBX_USE_VALGRIND) || defined(__SANITIZE_ADDRESS__)
  VALGRIND_MAKE_MEM_NOACCESS(env->me_map + used_bytes,
                             env->me_dxb_mmap.limit - used_bytes);
  ASAN_POISON_MEMORY_REGION(env->me_map + used_bytes,
                            env->me_dxb_mmap.limit - used_bytes);
  env->me_poison_edge = bytes2pgno(env, env->me_dxb_mmap.limit);
#endif /* MDBX_USE_VALGRIND */

  /* NOTE: AddressSanitizer (at least GCC 7.x, 8.x) could generate
   *       false-positive alarm here. I have no other explanation for this
   *       except due to an internal ASAN error, as the problem is reproduced
   *       in a single-threaded application under the active assert() above. */
  const unsigned meta_clash_mask = mdbx_meta_eq_mask(env);
  if (meta_clash_mask) {
    mdbx_error("meta-pages are clashed: mask 0x%d", meta_clash_mask);
    return MDBX_WANNA_RECOVERY;
  }

  while (1) {
    MDBX_meta *head = mdbx_meta_head(env);
    const txnid_t head_txnid = mdbx_meta_txnid_fluid(env, head);
    if (head_txnid == meta.mm_txnid_a.inconsistent)
      break;

    if (lck_rc == /* lck exclusive */ MDBX_RESULT_TRUE) {
      mdbx_assert(env, META_IS_STEADY(&meta) && !META_IS_STEADY(head));
      if (env->me_flags & MDBX_RDONLY) {
        mdbx_error("rollback needed: (from head %" PRIaTXN
                   " to steady %" PRIaTXN "), but unable in read-only mode",
                   head_txnid, meta.mm_txnid_a.inconsistent);
        return MDBX_WANNA_RECOVERY /* LY: could not recovery/rollback */;
      }

      const MDBX_meta *const meta0 = METAPAGE(env, 0);
      const MDBX_meta *const meta1 = METAPAGE(env, 1);
      const MDBX_meta *const meta2 = METAPAGE(env, 2);
      txnid_t undo_txnid = 0 /* zero means undo is unneeded */;
      while (
          (head != meta0 && mdbx_meta_txnid_fluid(env, meta0) == undo_txnid) ||
          (head != meta1 && mdbx_meta_txnid_fluid(env, meta1) == undo_txnid) ||
          (head != meta2 && mdbx_meta_txnid_fluid(env, meta2) == undo_txnid))
        undo_txnid = safe64_txnid_next(undo_txnid);
      if (unlikely(undo_txnid >= meta.mm_txnid_a.inconsistent)) {
        mdbx_fatal("rollback failed: no suitable txnid (0,1,2) < %" PRIaTXN,
                   meta.mm_txnid_a.inconsistent);
        return MDBX_PANIC /* LY: could not recovery/rollback */;
      }

      /* LY: rollback weak checkpoint */
      mdbx_trace("rollback: from %" PRIaTXN ", to %" PRIaTXN " as %" PRIaTXN,
                 head_txnid, meta.mm_txnid_a.inconsistent, undo_txnid);
      mdbx_ensure(env, head_txnid == mdbx_meta_txnid_stable(env, head));

      if (env->me_flags & MDBX_WRITEMAP) {
        /* It is possible to update txnid without safe64_write(),
         * since DB opened exclusive for now */
        head->mm_txnid_a.inconsistent = undo_txnid;
        head->mm_datasync_sign = MDBX_DATASIGN_WEAK;
        head->mm_txnid_b.inconsistent = undo_txnid;
        const size_t offset = (uint8_t *)data_page(head) - env->me_dxb_mmap.dxb;
        const size_t paged_offset = offset & ~(env->me_os_psize - 1);
        const size_t paged_length = roundup_powerof2(
            env->me_psize + offset - paged_offset, env->me_os_psize);
        err = mdbx_msync(&env->me_dxb_mmap, paged_offset, paged_length, false);
      } else {
        MDBX_meta rollback = *head;
        mdbx_meta_set_txnid(env, &rollback, undo_txnid);
        rollback.mm_datasync_sign = MDBX_DATASIGN_WEAK;
        err = mdbx_pwrite(env->me_fd, &rollback, sizeof(MDBX_meta),
                          (uint8_t *)head - (uint8_t *)env->me_map);
      }
      if (err) {
        mdbx_error("error %d rollback from %" PRIaTXN ", to %" PRIaTXN
                   " as %" PRIaTXN,
                   err, head_txnid, meta.mm_txnid_a.inconsistent, undo_txnid);
        return err;
      }

      mdbx_invalidate_mmap_noncoherent_cache(env->me_map,
                                             pgno2bytes(env, NUM_METAS));
      mdbx_ensure(env, undo_txnid == mdbx_meta_txnid_fluid(env, head));
      mdbx_ensure(env, 0 == mdbx_meta_eq_mask(env));
      continue;
    }

    if (!env->me_lck) {
      /* LY: without-lck (read-only) mode, so it is imposible that other
       * process made weak checkpoint. */
      mdbx_error("%s", "without-lck, unable recovery/rollback");
      return MDBX_WANNA_RECOVERY;
    }

    /* LY: assume just have a collision with other running process,
     *     or someone make a weak checkpoint */
    mdbx_verbose("%s", "assume collision or online weak checkpoint");
    break;
  }

  const MDBX_meta *head = mdbx_meta_head(env);
  if (lck_rc == /* lck exclusive */ MDBX_RESULT_TRUE) {
    /* re-check size after mmap */
    if ((env->me_dxb_mmap.current & (env->me_os_psize - 1)) != 0 ||
        env->me_dxb_mmap.current < used_bytes) {
      mdbx_error("unacceptable/unexpected datafile size %" PRIuPTR,
                 env->me_dxb_mmap.current);
      return MDBX_PROBLEM;
    }
    if (env->me_dxb_mmap.current != env->me_dbgeo.now &&
        (env->me_flags & MDBX_RDONLY) == 0) {
      meta.mm_geo.now = bytes2pgno(env, env->me_dxb_mmap.current);
      mdbx_verbose("update meta-geo to filesize %" PRIuPTR " bytes, %" PRIaPGNO
                   " pages",
                   env->me_dxb_mmap.current, meta.mm_geo.now);
    }

    if (memcmp(&meta.mm_geo, &head->mm_geo, sizeof(meta.mm_geo))) {
      const txnid_t txnid = mdbx_meta_txnid_stable(env, head);
      const txnid_t next_txnid = safe64_txnid_next(txnid);
      mdbx_verbose("updating meta.geo: "
                   "from l%" PRIaPGNO "-n%" PRIaPGNO "-u%" PRIaPGNO
                   "/s%u-g%u (txn#%" PRIaTXN "), "
                   "to l%" PRIaPGNO "-n%" PRIaPGNO "-u%" PRIaPGNO
                   "/s%u-g%u (txn#%" PRIaTXN ")",
                   head->mm_geo.lower, head->mm_geo.now, head->mm_geo.upper,
                   head->mm_geo.shrink, head->mm_geo.grow, txnid,
                   meta.mm_geo.lower, meta.mm_geo.now, meta.mm_geo.upper,
                   meta.mm_geo.shrink, meta.mm_geo.grow, next_txnid);

      mdbx_ensure(env, mdbx_meta_eq(env, &meta, head));
      mdbx_meta_set_txnid(env, &meta, next_txnid);
      *env->me_unsynced_pages += 1;
      err = mdbx_sync_locked(env, env->me_flags | MDBX_SHRINK_ALLOWED, &meta);
      if (err) {
        mdbx_error("error %d, while updating meta.geo: "
                   "from l%" PRIaPGNO "-n%" PRIaPGNO "-u%" PRIaPGNO
                   "/s%u-g%u (txn#%" PRIaTXN "), "
                   "to l%" PRIaPGNO "-n%" PRIaPGNO "-u%" PRIaPGNO
                   "/s%u-g%u (txn#%" PRIaTXN ")",
                   err, head->mm_geo.lower, head->mm_geo.now,
                   head->mm_geo.upper, head->mm_geo.shrink, head->mm_geo.grow,
                   txnid, meta.mm_geo.lower, meta.mm_geo.now, meta.mm_geo.upper,
                   meta.mm_geo.shrink, meta.mm_geo.grow, next_txnid);
        return err;
      }
    }
  }

  return rc;
}

/******************************************************************************/

/* Open and/or initialize the lock region for the environment. */
static int __cold mdbx_setup_lck(MDBX_env *env, char *lck_pathname,
                                 mode_t mode) {
  mdbx_assert(env, env->me_fd != INVALID_HANDLE_VALUE);
  mdbx_assert(env, env->me_lfd == INVALID_HANDLE_VALUE);

  int err = mdbx_openfile(lck_pathname, O_RDWR | O_CREAT, mode, &env->me_lfd,
                          (env->me_flags & MDBX_EXCLUSIVE) ? true : false);
  if (err != MDBX_SUCCESS) {
    if (!(err == MDBX_ENOFILE && (env->me_flags & MDBX_EXCLUSIVE)) &&
        !((err == MDBX_EROFS || err == MDBX_EACCESS || err == MDBX_EPERM) &&
          (env->me_flags & MDBX_RDONLY)))
      return err;

    /* LY: without-lck mode (e.g. exclusive or on read-only filesystem) */
    /* beginning of a locked section ---------------------------------------- */
    lcklist_lock();
    mdbx_assert(env, env->me_lcklist_next == nullptr);
    env->me_lfd = INVALID_HANDLE_VALUE;
    const int rc = mdbx_lck_seize(env);
    if (MDBX_IS_ERROR(rc)) {
      /* Calling lcklist_detach_locked() is required to restore POSIX-filelock
       * and this job will be done by mdbx_env_close0(). */
      lcklist_unlock();
      return rc;
    }
    /* insert into inprocess lck-list */
    env->me_lcklist_next = inprocess_lcklist_head;
    inprocess_lcklist_head = env;
    lcklist_unlock();
    /* end of a locked section ---------------------------------------------- */

    env->me_oldest = &env->me_lckless_stub.oldest;
    env->me_sync_timestamp = &env->me_lckless_stub.sync_timestamp;
    env->me_autosync_period = &env->me_lckless_stub.autosync_period;
    env->me_unsynced_pages = &env->me_lckless_stub.autosync_pending;
    env->me_autosync_threshold = &env->me_lckless_stub.autosync_threshold;
    env->me_discarded_tail = &env->me_lckless_stub.discarded_tail;
    env->me_meta_sync_txnid = &env->me_lckless_stub.meta_sync_txnid;
    env->me_maxreaders = UINT_MAX;
#ifdef MDBX_OSAL_LOCK
    env->me_wmutex = &env->me_lckless_stub.wmutex;
#endif
    mdbx_debug("lck-setup:%s%s%s", " lck-less",
               (env->me_flags & MDBX_RDONLY) ? " readonly" : "",
               (rc == MDBX_RESULT_TRUE) ? " exclusive" : " cooperative");
    return rc;
  }

  /* beginning of a locked section ------------------------------------------ */
  lcklist_lock();
  mdbx_assert(env, env->me_lcklist_next == nullptr);

  /* Try to get exclusive lock. If we succeed, then
   * nobody is using the lock region and we should initialize it. */
  err = mdbx_lck_seize(env);
  if (MDBX_IS_ERROR(err)) {
  bailout:
    /* Calling lcklist_detach_locked() is required to restore POSIX-filelock
     * and this job will be done by mdbx_env_close0(). */
    lcklist_unlock();
    return err;
  }

  MDBX_env *inprocess_neighbor = nullptr;
  if (err == MDBX_RESULT_TRUE) {
    err = uniq_check(&env->me_lck_mmap, &inprocess_neighbor);
    if (MDBX_IS_ERROR(err))
      goto bailout;
    if (inprocess_neighbor &&
        ((mdbx_runtime_flags & MDBX_DBG_LEGACY_MULTIOPEN) == 0 ||
         (inprocess_neighbor->me_flags & MDBX_EXCLUSIVE) != 0)) {
      err = MDBX_BUSY;
      goto bailout;
    }
  }
  const int lck_seize_rc = err;

  mdbx_debug("lck-setup:%s%s%s", " with-lck",
             (env->me_flags & MDBX_RDONLY) ? " readonly" : "",
             (lck_seize_rc == MDBX_RESULT_TRUE) ? " exclusive"
                                                : " cooperative");

  uint64_t size;
  err = mdbx_filesize(env->me_lfd, &size);
  if (unlikely(err != MDBX_SUCCESS))
    goto bailout;

  if (lck_seize_rc == MDBX_RESULT_TRUE) {
    size = roundup_powerof2(env->me_maxreaders * sizeof(MDBX_reader) +
                                sizeof(MDBX_lockinfo),
                            env->me_os_psize);
#ifndef NDEBUG
    err = mdbx_ftruncate(env->me_lfd, 0);
    if (unlikely(err != MDBX_SUCCESS))
      goto bailout;
#endif
    mdbx_jitter4testing(false);
  } else {
    if (env->me_flags & MDBX_EXCLUSIVE) {
      err = MDBX_BUSY;
      goto bailout;
    }
    if (size > INT_MAX || (size & (env->me_os_psize - 1)) != 0 ||
        size < env->me_os_psize) {
      mdbx_error("lck-file has invalid size %" PRIu64 " bytes", size);
      err = MDBX_PROBLEM;
      goto bailout;
    }
  }

  const size_t maxreaders =
      ((size_t)size - sizeof(MDBX_lockinfo)) / sizeof(MDBX_reader);
  if (size > 65536 || maxreaders < 2 || maxreaders > MDBX_READERS_LIMIT) {
    mdbx_error("lck-size too big (up to %" PRIuPTR " readers)", maxreaders);
    err = MDBX_PROBLEM;
    goto bailout;
  }
  env->me_maxreaders = (unsigned)maxreaders;

  err = mdbx_mmap(MDBX_WRITEMAP, &env->me_lck_mmap, (size_t)size, (size_t)size,
                  lck_seize_rc);
  if (unlikely(err != MDBX_SUCCESS))
    goto bailout;

#ifdef MADV_DODUMP
  (void)madvise(env->me_lck, size, MADV_DODUMP);
#endif

#ifdef MADV_WILLNEED
  if (madvise(env->me_lck, size, MADV_WILLNEED) < 0)
    goto bailout;
#endif

  if (lck_seize_rc == MDBX_RESULT_TRUE) {
    /* LY: exlcusive mode, reset lck */
    memset(env->me_lck, 0, (size_t)size);
    mdbx_jitter4testing(false);
    env->me_lck->mti_magic_and_version = MDBX_LOCK_MAGIC;
    env->me_lck->mti_os_and_format = MDBX_LOCK_FORMAT;
  } else {
    if (env->me_lck->mti_magic_and_version != MDBX_LOCK_MAGIC) {
      mdbx_error("%s", "lock region has invalid magic/version");
      err = ((env->me_lck->mti_magic_and_version >> 8) != MDBX_MAGIC)
                ? MDBX_INVALID
                : MDBX_VERSION_MISMATCH;
      goto bailout;
    }
    if (env->me_lck->mti_os_and_format != MDBX_LOCK_FORMAT) {
      mdbx_error("lock region has os/format 0x%" PRIx32 ", expected 0x%" PRIx32,
                 env->me_lck->mti_os_and_format, MDBX_LOCK_FORMAT);
      err = MDBX_VERSION_MISMATCH;
      goto bailout;
    }
  }

  err = mdbx_lck_init(env, inprocess_neighbor, lck_seize_rc);
  if (MDBX_IS_ERROR(err))
    goto bailout;

  mdbx_ensure(env, env->me_lcklist_next == nullptr);
  /* insert into inprocess lck-list */
  env->me_lcklist_next = inprocess_lcklist_head;
  inprocess_lcklist_head = env;
  lcklist_unlock();
  /* end of a locked section ------------------------------------------------ */

  mdbx_assert(env, !MDBX_IS_ERROR(lck_seize_rc));
  env->me_oldest = &env->me_lck->mti_oldest_reader;
  env->me_sync_timestamp = &env->me_lck->mti_sync_timestamp;
  env->me_autosync_period = &env->me_lck->mti_autosync_period;
  env->me_unsynced_pages = &env->me_lck->mti_unsynced_pages;
  env->me_autosync_threshold = &env->me_lck->mti_autosync_threshold;
  env->me_discarded_tail = &env->me_lck->mti_discarded_tail;
  env->me_meta_sync_txnid = &env->me_lck->mti_meta_sync_txnid;
#ifdef MDBX_OSAL_LOCK
  env->me_wmutex = &env->me_lck->mti_wmutex;
#endif
  return lck_seize_rc;
}

__cold int mdbx_is_readahead_reasonable(size_t volume, intptr_t redundancy) {
  if (volume <= 1024 * 1024 * 4ul)
    return MDBX_RESULT_TRUE;

  const intptr_t pagesize = mdbx_syspagesize();
  if (unlikely(pagesize < MIN_PAGESIZE || !is_powerof2(pagesize)))
    return MDBX_INCOMPATIBLE;

#if defined(_WIN32) || defined(_WIN64)
  MEMORYSTATUSEX info;
  memset(&info, 0, sizeof(info));
  info.dwLength = sizeof(info);
  if (!GlobalMemoryStatusEx(&info))
    return GetLastError();
#endif
  const int log2page = log2n(pagesize);

#if defined(_WIN32) || defined(_WIN64)
  const intptr_t total_ram_pages = (intptr_t)(info.ullTotalPhys >> log2page);
#elif defined(_SC_PHYS_PAGES)
  const intptr_t total_ram_pages = sysconf(_SC_PHYS_PAGES);
  if (total_ram_pages == -1)
    return errno;
#elif defined(_SC_AIX_REALMEM)
  const intptr_t total_ram_Kb = sysconf(_SC_AIX_REALMEM);
  if (total_ram_Kb == -1)
    return errno;
  const intptr_t total_ram_pages = (total_ram_Kb << 10) >> log2page;
#elif defined(HW_USERMEM) || defined(HW_PHYSMEM64) || defined(HW_MEMSIZE) ||   \
    defined(HW_PHYSMEM)
  size_t ram, len = sizeof(ram);
  static const int mib[] = {
    CTL_HW,
#if defined(HW_USERMEM)
    HW_USERMEM
#elif defined(HW_PHYSMEM64)
    HW_PHYSMEM64
#elif defined(HW_MEMSIZE)
    HW_MEMSIZE
#else
    HW_PHYSMEM
#endif
  };
  if (sysctl(
#ifdef SYSCTL_LEGACY_NONCONST_MIB
          (int *)
#endif
              mib,
          ARRAY_LENGTH(mib), &ram, &len, NULL, 0) != 0)
    return errno;
  if (len != sizeof(ram))
    return MDBX_ENOSYS;
  const intptr_t total_ram_pages = (intptr_t)(ram >> log2page);
#else
#error "FIXME: Get User-accessible or physical RAM"
#endif
  if (total_ram_pages < 1)
    return MDBX_ENOSYS;

  const intptr_t volume_pages = (volume + pagesize - 1) >> log2page;
  const intptr_t redundancy_pages =
      (redundancy < 0) ? -(intptr_t)((-redundancy + pagesize - 1) >> log2page)
                       : (intptr_t)(redundancy + pagesize - 1) >> log2page;
  if (volume_pages >= total_ram_pages ||
      volume_pages + redundancy_pages >= total_ram_pages)
    return MDBX_RESULT_FALSE;

#if defined(_WIN32) || defined(_WIN64)
  const intptr_t avail_ram_pages = (intptr_t)(info.ullAvailPhys >> log2page);
#elif defined(_SC_AVPHYS_PAGES)
  const intptr_t avail_ram_pages = sysconf(_SC_AVPHYS_PAGES);
  if (avail_ram_pages == -1)
    return errno;
#elif defined(__MACH__)
  mach_msg_type_number_t count = HOST_VM_INFO_COUNT;
  vm_statistics_data_t vmstat;
  mach_port_t mport = mach_host_self();
  kern_return_t kerr = host_statistics(mach_host_self(), HOST_VM_INFO,
                                       (host_info_t)&vmstat, &count);
  mach_port_deallocate(mach_task_self(), mport);
  if (unlikely(kerr != KERN_SUCCESS))
    return MDBX_ENOSYS;
  const intptr_t avail_ram_pages = vmstat.free_count;
#elif defined(VM_TOTAL) || defined(VM_METER)
  struct vmtotal info;
  size_t len = sizeof(info);
  static const int mib[] = {
    CTL_VM,
#if defined(VM_TOTAL)
    VM_TOTAL
#elif defined(VM_METER)
    VM_METER
#endif
  };
  if (sysctl(
#ifdef SYSCTL_LEGACY_NONCONST_MIB
          (int *)
#endif
              mib,
          ARRAY_LENGTH(mib), &info, &len, NULL, 0) != 0)
    return errno;
  if (len != sizeof(info))
    return MDBX_ENOSYS;
  const intptr_t avail_ram_pages = info.t_free;
#else
#error "FIXME: Get Available RAM"
#endif
  if (avail_ram_pages < 1)
    return MDBX_ENOSYS;

  return (volume_pages + redundancy_pages >= avail_ram_pages)
             ? MDBX_RESULT_FALSE
             : MDBX_RESULT_TRUE;
}

/* Only a subset of the mdbx_env flags can be changed
 * at runtime. Changing other flags requires closing the
 * environment and re-opening it with the new flags. */
#define CHANGEABLE                                                             \
  (MDBX_NOSYNC | MDBX_NOMETASYNC | MDBX_MAPASYNC | MDBX_NOMEMINIT |            \
   MDBX_COALESCE | MDBX_PAGEPERTURB)
#define CHANGELESS                                                             \
  (MDBX_NOSUBDIR | MDBX_RDONLY | MDBX_WRITEMAP | MDBX_NOTLS | MDBX_NORDAHEAD | \
   MDBX_LIFORECLAIM | MDBX_EXCLUSIVE)

#if VALID_FLAGS & PERSISTENT_FLAGS & (CHANGEABLE | CHANGELESS)
#error "Persistent DB flags & env flags overlap, but both go in mm_flags"
#endif

int __cold mdbx_env_open(MDBX_env *env, const char *path, unsigned flags,
                         mode_t mode) {
  if (unlikely(!env || !path))
    return MDBX_EINVAL;

  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

  if (flags & ~(CHANGEABLE | CHANGELESS))
    return MDBX_EINVAL;

  if (env->me_fd != INVALID_HANDLE_VALUE ||
      (env->me_flags & MDBX_ENV_ACTIVE) != 0)
    return MDBX_EPERM;

  size_t len_full, len = strlen(path);
  if (flags & MDBX_NOSUBDIR) {
    len_full = len + sizeof(MDBX_LOCK_SUFFIX) + len + 1;
  } else {
    len_full = len + sizeof(MDBX_LOCKNAME) + len + sizeof(MDBX_DATANAME);
  }
  char *lck_pathname = mdbx_malloc(len_full);
  if (!lck_pathname)
    return MDBX_ENOMEM;

  char *dxb_pathname;
  if (flags & MDBX_NOSUBDIR) {
    dxb_pathname = lck_pathname + len + sizeof(MDBX_LOCK_SUFFIX);
    sprintf(lck_pathname, "%s" MDBX_LOCK_SUFFIX, path);
    strcpy(dxb_pathname, path);
  } else {
    dxb_pathname = lck_pathname + len + sizeof(MDBX_LOCKNAME);
    sprintf(lck_pathname, "%s" MDBX_LOCKNAME, path);
    sprintf(dxb_pathname, "%s" MDBX_DATANAME, path);
  }

  int rc = MDBX_SUCCESS;
  flags |= env->me_flags;
  if (flags & MDBX_RDONLY) {
    /* LY: silently ignore irrelevant flags when
     * we're only getting read access */
    flags &= ~(MDBX_WRITEMAP | MDBX_MAPASYNC | MDBX_NOSYNC | MDBX_NOMETASYNC |
               MDBX_COALESCE | MDBX_LIFORECLAIM | MDBX_NOMEMINIT);
  } else {
    env->me_dirtylist = mdbx_calloc(MDBX_DPL_TXNFULL + 1, sizeof(MDBX_DP));
    if (!env->me_dirtylist)
      rc = MDBX_ENOMEM;
  }

  const uint32_t saved_me_flags = env->me_flags;
  env->me_flags = (flags & ~MDBX_FATAL_ERROR) | MDBX_ENV_ACTIVE;
  if (rc)
    goto bailout;

  env->me_path = mdbx_strdup(path);
  env->me_dbxs = mdbx_calloc(env->me_maxdbs, sizeof(MDBX_dbx));
  env->me_dbflags = mdbx_calloc(env->me_maxdbs, sizeof(env->me_dbflags[0]));
  env->me_dbiseqs = mdbx_calloc(env->me_maxdbs, sizeof(env->me_dbiseqs[0]));
  if (!(env->me_dbxs && env->me_path && env->me_dbflags && env->me_dbiseqs)) {
    rc = MDBX_ENOMEM;
    goto bailout;
  }
  env->me_dbxs[FREE_DBI].md_cmp =
      mdbx_cmp_int_align4; /* aligned MDBX_INTEGERKEY */

  int oflags;
  if (F_ISSET(flags, MDBX_RDONLY))
    oflags = O_RDONLY;
  else if (mode != 0) {
    if ((flags & MDBX_NOSUBDIR) == 0) {
#if defined(_WIN32) || defined(_WIN64)
      if (!CreateDirectoryA(path, nullptr)) {
        rc = GetLastError();
        if (rc != ERROR_ALREADY_EXISTS)
          goto bailout;
      }
#else
      const mode_t dir_mode =
          (/* inherit read/write permissions for group and others */ mode &
           (S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH)) |
          /* always add read/write/search for owner */ S_IRWXU |
          ((mode & S_IRGRP) ? /* +search if readable by group */ S_IXGRP : 0) |
          ((mode & S_IROTH) ? /* +search if readable by others */ S_IXOTH : 0);
      if (mkdir(path, dir_mode)) {
        rc = errno;
        if (rc != EEXIST)
          goto bailout;
      }
#endif
    }
    oflags = O_RDWR | O_CREAT;
  } else
    oflags = O_RDWR;

  rc = mdbx_openfile(dxb_pathname, oflags, mode, &env->me_fd,
                     (env->me_flags & MDBX_EXCLUSIVE) ? true : false);
  if (rc != MDBX_SUCCESS)
    goto bailout;

  const int lck_rc = mdbx_setup_lck(env, lck_pathname, mode);
  if (MDBX_IS_ERROR(lck_rc)) {
    rc = lck_rc;
    goto bailout;
  }

  const int dxb_rc = mdbx_setup_dxb(env, lck_rc);
  if (MDBX_IS_ERROR(dxb_rc)) {
    rc = dxb_rc;
    goto bailout;
  }

  mdbx_debug("opened dbenv %p", (void *)env);
  if (env->me_lck) {
    const unsigned mode_flags =
        MDBX_WRITEMAP | MDBX_NOSYNC | MDBX_NOMETASYNC | MDBX_MAPASYNC;
    if (lck_rc == MDBX_RESULT_TRUE) {
      env->me_lck->mti_envmode = env->me_flags & (mode_flags | MDBX_RDONLY);
      rc = mdbx_lck_downgrade(env);
      mdbx_debug("lck-downgrade-%s: rc %i",
                 (env->me_flags & MDBX_EXCLUSIVE) ? "partial" : "full", rc);
      if (rc != MDBX_SUCCESS)
        goto bailout;
    } else {
      rc = mdbx_reader_check0(env, false, NULL);
      if (MDBX_IS_ERROR(rc))
        goto bailout;
      if ((env->me_flags & MDBX_RDONLY) == 0) {
        while (env->me_lck->mti_envmode == MDBX_RDONLY) {
          if (atomic_cas32(&env->me_lck->mti_envmode, MDBX_RDONLY,
                           env->me_flags & mode_flags))
            break;
          atomic_yield();
        }
        if ((env->me_lck->mti_envmode ^ env->me_flags) & mode_flags) {
          mdbx_error("%s", "current mode/flags incompatible with requested");
          rc = MDBX_INCOMPATIBLE;
          goto bailout;
        }
      }
    }

    if ((env->me_flags & MDBX_NOTLS) == 0) {
      rc = mdbx_rthc_alloc(&env->me_txkey, &env->me_lck->mti_readers[0],
                           &env->me_lck->mti_readers[env->me_maxreaders]);
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;
      env->me_flags |= MDBX_ENV_TXKEY;
    }
  }

  if ((flags & MDBX_RDONLY) == 0) {
    rc = MDBX_ENOMEM;
    MDBX_txn *txn;
    int tsize = sizeof(MDBX_txn),
        size =
            tsize + env->me_maxdbs * (sizeof(MDBX_db) + sizeof(MDBX_cursor *) +
                                      sizeof(unsigned) + 1);
    if ((env->me_pbuf = mdbx_calloc(
             1 /* page buffer */ + 1 /* page killer bufer */, env->me_psize)) &&
        (txn = mdbx_calloc(1, size))) {
      txn->mt_dbs = (MDBX_db *)((char *)txn + tsize);
      txn->mt_cursors = (MDBX_cursor **)(txn->mt_dbs + env->me_maxdbs);
      txn->mt_dbiseqs = (unsigned *)(txn->mt_cursors + env->me_maxdbs);
      txn->mt_dbflags = (uint8_t *)(txn->mt_dbiseqs + env->me_maxdbs);
      txn->mt_env = env;
      txn->mt_dbxs = env->me_dbxs;
      txn->mt_flags = MDBX_TXN_FINISHED;
      env->me_txn0 = txn;
      txn->tw.retired_pages = mdbx_pnl_alloc(MDBX_PNL_INITIAL);
      txn->tw.reclaimed_pglist = mdbx_pnl_alloc(MDBX_PNL_INITIAL);
      if (txn->tw.retired_pages && txn->tw.reclaimed_pglist)
        rc = MDBX_SUCCESS;
    }
  }

#if MDBX_DEBUG
  if (rc == MDBX_SUCCESS) {
    MDBX_meta *meta = mdbx_meta_head(env);
    MDBX_db *db = &meta->mm_dbs[MAIN_DBI];

    mdbx_debug("opened database version %u, pagesize %u",
               (uint8_t)meta->mm_magic_and_version, env->me_psize);
    mdbx_debug("using meta page %" PRIaPGNO ", txn %" PRIaTXN,
               data_page(meta)->mp_pgno, mdbx_meta_txnid_fluid(env, meta));
    mdbx_debug("depth: %u", db->md_depth);
    mdbx_debug("entries: %" PRIu64, db->md_entries);
    mdbx_debug("branch pages: %" PRIaPGNO, db->md_branch_pages);
    mdbx_debug("leaf pages: %" PRIaPGNO, db->md_leaf_pages);
    mdbx_debug("overflow pages: %" PRIaPGNO, db->md_overflow_pages);
    mdbx_debug("root: %" PRIaPGNO, db->md_root);
    mdbx_debug("schema_altered: %" PRIaTXN, db->md_mod_txnid);
  }
#endif

bailout:
  if (rc) {
    rc = mdbx_env_close0(env) ? MDBX_PANIC : rc;
    env->me_flags = saved_me_flags | MDBX_FATAL_ERROR;
  }
  mdbx_free(lck_pathname);
  return rc;
}

/* Destroy resources from mdbx_env_open(), clear our readers & DBIs */
static int __cold mdbx_env_close0(MDBX_env *env) {
  if (!(env->me_flags & MDBX_ENV_ACTIVE)) {
    mdbx_ensure(env, env->me_lcklist_next == nullptr);
    return MDBX_SUCCESS;
  }

  env->me_flags &= ~MDBX_ENV_ACTIVE;
  env->me_oldest = nullptr;
  env->me_sync_timestamp = nullptr;
  env->me_autosync_period = nullptr;
  env->me_unsynced_pages = nullptr;
  env->me_autosync_threshold = nullptr;
  env->me_discarded_tail = nullptr;
  env->me_meta_sync_txnid = nullptr;
  if (env->me_flags & MDBX_ENV_TXKEY)
    mdbx_rthc_remove(env->me_txkey);

  lcklist_lock();
  const int rc = lcklist_detach_locked(env);
  lcklist_unlock();

  if (env->me_map) {
    mdbx_munmap(&env->me_dxb_mmap);
#ifdef MDBX_USE_VALGRIND
    VALGRIND_DISCARD(env->me_valgrind_handle);
    env->me_valgrind_handle = -1;
#endif
  }
  if (env->me_fd != INVALID_HANDLE_VALUE) {
    (void)mdbx_closefile(env->me_fd);
    env->me_fd = INVALID_HANDLE_VALUE;
  }

  if (env->me_lck)
    mdbx_munmap(&env->me_lck_mmap);

  if (env->me_lfd != INVALID_HANDLE_VALUE) {
    (void)mdbx_closefile(env->me_lfd);
    env->me_lfd = INVALID_HANDLE_VALUE;
  }

  if (env->me_dbxs) {
    for (unsigned i = env->me_maxdbs; --i >= CORE_DBS;)
      mdbx_free(env->me_dbxs[i].md_name.iov_base);
    mdbx_free(env->me_dbxs);
  }
  mdbx_free(env->me_pbuf);
  mdbx_free(env->me_dbiseqs);
  mdbx_free(env->me_dbflags);
  mdbx_free(env->me_path);
  mdbx_free(env->me_dirtylist);
  if (env->me_txn0) {
    mdbx_txl_free(env->me_txn0->tw.lifo_reclaimed);
    mdbx_pnl_free(env->me_txn0->tw.retired_pages);
    mdbx_pnl_free(env->me_txn0->tw.spill_pages);
    mdbx_pnl_free(env->me_txn0->tw.reclaimed_pglist);
    mdbx_free(env->me_txn0);
  }
  env->me_flags = 0;
  return rc;
}

int __cold mdbx_env_close_ex(MDBX_env *env, int dont_sync) {
  MDBX_page *dp;
  int rc = MDBX_SUCCESS;

  if (unlikely(!env))
    return MDBX_EINVAL;

  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

#if MDBX_TXN_CHECKPID || !(defined(_WIN32) || defined(_WIN64))
  /* Check the PID even if MDBX_TXN_CHECKPID=0 on non-Windows
   * platforms (i.e. where fork() is available).
   * This is required to legitimize a call after fork()
   * from a child process, that should be allowed to free resources. */
  if (unlikely(env->me_pid != mdbx_getpid()))
    env->me_flags |= MDBX_FATAL_ERROR;
#endif /* MDBX_TXN_CHECKPID */

  if ((env->me_flags & (MDBX_RDONLY | MDBX_FATAL_ERROR)) == 0) {
    if (env->me_txn0 && env->me_txn0->mt_owner &&
        env->me_txn0->mt_owner != mdbx_thread_self())
      return MDBX_BUSY;
    if (!dont_sync) {
#if defined(_WIN32) || defined(_WIN64)
      /* On windows, without blocking is impossible to determine whether another
       * process is running a writing transaction or not.
       * Because in the "owner died" condition kernel don't release
       * file lock immediately. */
      rc = mdbx_env_sync_ex(env, true, false);
      rc = (rc == MDBX_RESULT_TRUE) ? MDBX_SUCCESS : rc;
#else
      rc = mdbx_env_sync_ex(env, true, true);
      rc = (rc == MDBX_BUSY || rc == EAGAIN || rc == EACCES || rc == EBUSY ||
            rc == EWOULDBLOCK || rc == MDBX_RESULT_TRUE)
               ? MDBX_SUCCESS
               : rc;
#endif
    }
  }

  while ((dp = env->me_dpages) != NULL) {
    ASAN_UNPOISON_MEMORY_REGION(&dp->mp_next, sizeof(dp->mp_next));
    VALGRIND_MAKE_MEM_DEFINED(&dp->mp_next, sizeof(dp->mp_next));
    env->me_dpages = dp->mp_next;
    mdbx_free(dp);
  }
  VALGRIND_DESTROY_MEMPOOL(env);

  rc = mdbx_env_close0(env) ? MDBX_PANIC : rc;
  mdbx_ensure(env, mdbx_fastmutex_destroy(&env->me_dbi_lock) == MDBX_SUCCESS);
#if defined(_WIN32) || defined(_WIN64)
  /* me_remap_guard don't have destructor (Slim Reader/Writer Lock) */
  DeleteCriticalSection(&env->me_windowsbug_lock);
#else
  mdbx_ensure(env,
              mdbx_fastmutex_destroy(&env->me_remap_guard) == MDBX_SUCCESS);
#endif /* Windows */

#ifdef MDBX_OSAL_LOCK
  mdbx_ensure(env, mdbx_fastmutex_destroy(&env->me_lckless_stub.wmutex) ==
                       MDBX_SUCCESS);
#endif

  mdbx_ensure(env, env->me_lcklist_next == nullptr);
  env->me_pid = 0;
  env->me_signature = 0;
  mdbx_free(env);

  return rc;
}

__cold int mdbx_env_close(MDBX_env *env) {
  return mdbx_env_close_ex(env, false);
}

/* Compare two items pointing at aligned unsigned int's. */
static int __hot mdbx_cmp_int_align4(const MDBX_val *a, const MDBX_val *b) {
  mdbx_assert(NULL, a->iov_len == b->iov_len);
  switch (a->iov_len) {
  case 4:
    return CMP2INT(unaligned_peek_u32(4, a->iov_base),
                   unaligned_peek_u32(4, b->iov_base));
  case 8:
    return CMP2INT(unaligned_peek_u64(4, a->iov_base),
                   unaligned_peek_u64(4, b->iov_base));
  default:
    mdbx_assert_fail(NULL, "invalid size for INTEGERKEY/INTEGERDUP", __func__,
                     __LINE__);
    return 0;
  }
}

/* Compare two items pointing at 2-byte aligned unsigned int's. */
static int __hot mdbx_cmp_int_align2(const MDBX_val *a, const MDBX_val *b) {
  mdbx_assert(NULL, a->iov_len == b->iov_len);
  switch (a->iov_len) {
  case 4:
    return CMP2INT(unaligned_peek_u32(2, a->iov_base),
                   unaligned_peek_u32(2, b->iov_base));
  case 8:
    return CMP2INT(unaligned_peek_u64(2, a->iov_base),
                   unaligned_peek_u64(2, b->iov_base));
  default:
    mdbx_assert_fail(NULL, "invalid size for INTEGERKEY/INTEGERDUP", __func__,
                     __LINE__);
    return 0;
  }
}

/* Compare two items pointing at unsigneds of unknown alignment.
 *
 * This is also set as MDBX_INTEGERDUP|MDBX_DUPFIXED's MDBX_dbx.md_dcmp. */
static int __hot mdbx_cmp_int_unaligned(const MDBX_val *a, const MDBX_val *b) {
  mdbx_assert(NULL, a->iov_len == b->iov_len);
  switch (a->iov_len) {
  case 4:
    return CMP2INT(unaligned_peek_u32(1, a->iov_base),
                   unaligned_peek_u32(1, b->iov_base));
  case 8:
    return CMP2INT(unaligned_peek_u64(1, a->iov_base),
                   unaligned_peek_u64(1, b->iov_base));
  default:
    mdbx_assert_fail(NULL, "invalid size for INTEGERKEY/INTEGERDUP", __func__,
                     __LINE__);
    return 0;
  }
}

/* Compare two items lexically */
static int __hot mdbx_cmp_memn(const MDBX_val *a, const MDBX_val *b) {
  if (a->iov_len == b->iov_len)
    return memcmp(a->iov_base, b->iov_base, a->iov_len);

  const int diff_len = (a->iov_len < b->iov_len) ? -1 : 1;
  const size_t shortest = (a->iov_len < b->iov_len) ? a->iov_len : b->iov_len;
  int diff_data = memcmp(a->iov_base, b->iov_base, shortest);
  return likely(diff_data) ? diff_data : diff_len;
}

/* Compare two items in reverse byte order */
static int __hot mdbx_cmp_memnr(const MDBX_val *a, const MDBX_val *b) {
  const uint8_t *pa = (const uint8_t *)a->iov_base + a->iov_len;
  const uint8_t *pb = (const uint8_t *)b->iov_base + b->iov_len;
  const size_t shortest = (a->iov_len < b->iov_len) ? a->iov_len : b->iov_len;

  const uint8_t *const end = pa - shortest;
  while (pa != end) {
    int diff = *--pa - *--pb;
    if (likely(diff))
      return diff;
  }
  return CMP2INT(a->iov_len, b->iov_len);
}

/* Search for key within a page, using binary search.
 * Returns the smallest entry larger or equal to the key.
 * If exactp is non-null, stores whether the found entry was an exact match
 * in *exactp (1 or 0).
 * Updates the cursor index with the index of the found entry.
 * If no entry larger or equal to the key is found, returns NULL. */
static MDBX_node *__hot mdbx_node_search(MDBX_cursor *mc, MDBX_val *key,
                                         int *exactp) {
  int low, high;
  int rc = 0;
  MDBX_page *mp = mc->mc_pg[mc->mc_top];
  MDBX_node *node = nullptr;
  MDBX_val nodekey;
  MDBX_cmp_func *cmp;
  DKBUF;

  const unsigned nkeys = page_numkeys(mp);

  mdbx_debug("searching %u keys in %s %spage %" PRIaPGNO, nkeys,
             IS_LEAF(mp) ? "leaf" : "branch", IS_SUBP(mp) ? "sub-" : "",
             mp->mp_pgno);

  low = IS_LEAF(mp) ? 0 : 1;
  high = nkeys - 1;
  cmp = mc->mc_dbx->md_cmp;

  /* Branch pages have no data, so if using integer keys,
   * alignment is guaranteed. Use faster mdbx_cmp_int_ai.
   */
  if (cmp == mdbx_cmp_int_align2 && IS_BRANCH(mp))
    cmp = mdbx_cmp_int_align4;

  unsigned i = 0;
  if (IS_LEAF2(mp)) {
    mdbx_cassert(mc, mp->mp_leaf2_ksize == mc->mc_db->md_xsize);
    nodekey.iov_len = mp->mp_leaf2_ksize;
    node = (MDBX_node *)(intptr_t)-1; /* fake */
    while (low <= high) {
      i = (low + high) >> 1;
      nodekey.iov_base = page_leaf2key(mp, i, nodekey.iov_len);
      mdbx_cassert(mc, (char *)mp + mc->mc_txn->mt_env->me_psize >=
                           (char *)nodekey.iov_base + nodekey.iov_len);
      rc = cmp(key, &nodekey);
      mdbx_debug("found leaf index %u [%s], rc = %i", i, DKEY(&nodekey), rc);
      if (rc == 0)
        break;
      if (rc > 0)
        low = i + 1;
      else
        high = i - 1;
    }
  } else {
    while (low <= high) {
      i = (low + high) >> 1;

      node = page_node(mp, i);
      nodekey.iov_len = node_ks(node);
      nodekey.iov_base = node_key(node);
      mdbx_cassert(mc, (char *)mp + mc->mc_txn->mt_env->me_psize >=
                           (char *)nodekey.iov_base + nodekey.iov_len);

      rc = cmp(key, &nodekey);
      if (IS_LEAF(mp))
        mdbx_debug("found leaf index %u [%s], rc = %i", i, DKEY(&nodekey), rc);
      else
        mdbx_debug("found branch index %u [%s -> %" PRIaPGNO "], rc = %i", i,
                   DKEY(&nodekey), node_pgno(node), rc);
      if (rc == 0)
        break;
      if (rc > 0)
        low = i + 1;
      else
        high = i - 1;
    }
  }

  if (rc > 0) /* Found entry is less than the key. */
    i++;      /* Skip to get the smallest entry larger than key. */

  if (exactp)
    *exactp = (rc == 0 && nkeys > 0);
  /* store the key index */
  mdbx_cassert(mc, i <= UINT16_MAX);
  mc->mc_ki[mc->mc_top] = (indx_t)i;
  if (i >= nkeys)
    /* There is no entry larger or equal to the key. */
    return NULL;

  /* page_node is fake for LEAF2 */
  return IS_LEAF2(mp) ? node : page_node(mp, i);
}

#if 0 /* unused for now */
static void mdbx_cursor_adjust(MDBX_cursor *mc, func) {
  MDBX_cursor *m2;

  for (m2 = mc->mc_txn->mt_cursors[mc->mc_dbi]; m2; m2 = m2->mc_next) {
    if (m2->mc_pg[m2->mc_top] == mc->mc_pg[mc->mc_top]) {
      func(mc, m2);
    }
  }
}
#endif

/* Pop a page off the top of the cursor's stack. */
static void mdbx_cursor_pop(MDBX_cursor *mc) {
  if (mc->mc_snum) {
    mdbx_debug("popped page %" PRIaPGNO " off db %d cursor %p",
               mc->mc_pg[mc->mc_top]->mp_pgno, DDBI(mc), (void *)mc);

    mc->mc_snum--;
    if (mc->mc_snum) {
      mc->mc_top--;
    } else {
      mc->mc_flags &= ~C_INITIALIZED;
    }
  }
}

/* Push a page onto the top of the cursor's stack.
 * Set MDBX_TXN_ERROR on failure. */
static int mdbx_cursor_push(MDBX_cursor *mc, MDBX_page *mp) {
  mdbx_debug("pushing page %" PRIaPGNO " on db %d cursor %p", mp->mp_pgno,
             DDBI(mc), (void *)mc);

  if (unlikely(mc->mc_snum >= CURSOR_STACK)) {
    mc->mc_txn->mt_flags |= MDBX_TXN_ERROR;
    return MDBX_CURSOR_FULL;
  }

  mdbx_cassert(mc, mc->mc_snum < UINT16_MAX);
  mc->mc_top = mc->mc_snum++;
  mc->mc_pg[mc->mc_top] = mp;
  mc->mc_ki[mc->mc_top] = 0;

  return MDBX_SUCCESS;
}

/* Find the address of the page corresponding to a given page number.
 * Set MDBX_TXN_ERROR on failure.
 *
 * [in] mc    the cursor accessing the page.
 * [in] pgno  the page number for the page to retrieve.
 * [out] ret  address of a pointer where the page's address will be
 *            stored.
 * [out] lvl  dirtylist inheritance level of found page. 1=current txn,
 *            0=mapped page.
 *
 * Returns 0 on success, non-zero on failure. */
__hot static int mdbx_page_get(MDBX_cursor *mc, pgno_t pgno, MDBX_page **ret,
                               int *lvl) {
  MDBX_txn *txn = mc->mc_txn;
  if (unlikely(pgno >= txn->mt_next_pgno)) {
    mdbx_debug("page %" PRIaPGNO " not found", pgno);
    txn->mt_flags |= MDBX_TXN_ERROR;
    return MDBX_PAGE_NOTFOUND;
  }

  MDBX_env *env = txn->mt_env;
  MDBX_page *p = NULL;
  int level;
  if ((txn->mt_flags & (MDBX_RDONLY | MDBX_WRITEMAP)) == 0) {
    level = 1;
    do {
      /* Spilled pages were dirtied in this txn and flushed
       * because the dirty list got full. Bring this page
       * back in from the map (but don't unspill it here,
       * leave that unless page_touch happens again). */
      if (txn->tw.spill_pages && mdbx_pnl_exist(txn->tw.spill_pages, pgno << 1))
        goto mapped;
      p = mdbx_dpl_find(txn->tw.dirtylist, pgno);
      if (p)
        goto done;
      level++;
    } while ((txn = txn->mt_parent) != NULL);
  }
  level = 0;

mapped:
  p = pgno2page(env, pgno);

done:
  txn = nullptr /* avoid future use */;
  if (unlikely(p->mp_pgno != pgno)) {
    mdbx_error("mismatch pgno %" PRIaPGNO " (actual) != %" PRIaPGNO
               " (expected)",
               p->mp_pgno, pgno);
    return MDBX_CORRUPTED;
  }

  if (unlikely(p->mp_upper < p->mp_lower || ((p->mp_lower | p->mp_upper) & 1) ||
               PAGEHDRSZ + p->mp_upper > env->me_psize) &&
      !IS_OVERFLOW(p)) {
    mdbx_error("invalid page lower(%u)/upper(%u), pg-limit %u", p->mp_lower,
               p->mp_upper, page_space(env));
    return MDBX_CORRUPTED;
  }
  /* TODO: more checks here, including p->mp_validator */

  if (mdbx_audit_enabled()) {
    int err = mdbx_page_check(env, p, true);
    if (unlikely(err != MDBX_SUCCESS))
      return err;
  }

  *ret = p;
  if (lvl)
    *lvl = level;
  return MDBX_SUCCESS;
}

/* Finish mdbx_page_search() / mdbx_page_search_lowest().
 * The cursor is at the root page, set up the rest of it. */
__hot static int mdbx_page_search_root(MDBX_cursor *mc, MDBX_val *key,
                                       int flags) {
  MDBX_page *mp = mc->mc_pg[mc->mc_top];
  int rc;
  DKBUF;

  while (IS_BRANCH(mp)) {
    MDBX_node *node;
    int i;

    mdbx_debug("branch page %" PRIaPGNO " has %u keys", mp->mp_pgno,
               page_numkeys(mp));
    /* Don't assert on branch pages in the GC. We can get here
     * while in the process of rebalancing a GC branch page; we must
     * let that proceed. ITS#8336 */
    mdbx_cassert(mc, !mc->mc_dbi || page_numkeys(mp) > 1);
    mdbx_debug("found index 0 to page %" PRIaPGNO, node_pgno(page_node(mp, 0)));

    if (flags & (MDBX_PS_FIRST | MDBX_PS_LAST)) {
      i = 0;
      if (flags & MDBX_PS_LAST) {
        i = page_numkeys(mp) - 1;
        /* if already init'd, see if we're already in right place */
        if (mc->mc_flags & C_INITIALIZED) {
          if (mc->mc_ki[mc->mc_top] == i) {
            mc->mc_top = mc->mc_snum++;
            mp = mc->mc_pg[mc->mc_top];
            goto ready;
          }
        }
      }
    } else {
      int exact;
      node = mdbx_node_search(mc, key, &exact);
      if (node == NULL)
        i = page_numkeys(mp) - 1;
      else {
        i = mc->mc_ki[mc->mc_top];
        if (!exact) {
          mdbx_cassert(mc, i > 0);
          i--;
        }
      }
      mdbx_debug("following index %u for key [%s]", i, DKEY(key));
    }

    mdbx_cassert(mc, i < (int)page_numkeys(mp));
    node = page_node(mp, i);

    if (unlikely((rc = mdbx_page_get(mc, node_pgno(node), &mp, NULL)) != 0))
      return rc;

    mc->mc_ki[mc->mc_top] = (indx_t)i;
    if (unlikely(rc = mdbx_cursor_push(mc, mp)))
      return rc;

  ready:
    if (flags & MDBX_PS_MODIFY) {
      if (unlikely((rc = mdbx_page_touch(mc)) != 0))
        return rc;
      mp = mc->mc_pg[mc->mc_top];
    }
  }

  if (unlikely(!IS_LEAF(mp))) {
    mdbx_debug("internal error, index points to a page with 0x%02x flags!?",
               mp->mp_flags);
    mc->mc_txn->mt_flags |= MDBX_TXN_ERROR;
    return MDBX_CORRUPTED;
  }

  mdbx_debug("found leaf page %" PRIaPGNO " for key [%s]", mp->mp_pgno,
             DKEY(key));
  mc->mc_flags |= C_INITIALIZED;
  mc->mc_flags &= ~C_EOF;

  return MDBX_SUCCESS;
}

static int mdbx_fetch_sdb(MDBX_txn *txn, MDBX_dbi dbi) {
  MDBX_cursor mc;
  if (unlikely(TXN_DBI_CHANGED(txn, dbi)))
    return MDBX_BAD_DBI;
  int rc = mdbx_cursor_init(&mc, txn, MAIN_DBI);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;
  rc = mdbx_page_search(&mc, &txn->mt_dbxs[dbi].md_name, 0);
  if (unlikely(rc != MDBX_SUCCESS))
    return (rc == MDBX_NOTFOUND) ? MDBX_BAD_DBI : rc;

  MDBX_val data;
  int exact = 0;
  MDBX_node *node = mdbx_node_search(&mc, &txn->mt_dbxs[dbi].md_name, &exact);
  if (unlikely(!exact))
    return MDBX_BAD_DBI;
  if (unlikely((node_flags(node) & (F_DUPDATA | F_SUBDATA)) != F_SUBDATA))
    return MDBX_INCOMPATIBLE; /* not a named DB */
  rc = mdbx_node_read(&mc, node, &data);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(data.iov_len < sizeof(MDBX_db)))
    return MDBX_INCOMPATIBLE; /* not a named DB */

  uint16_t md_flags = UNALIGNED_PEEK_16(data.iov_base, MDBX_db, md_flags);
  /* The txn may not know this DBI, or another process may
   * have dropped and recreated the DB with other flags. */
  if (unlikely((txn->mt_dbs[dbi].md_flags & PERSISTENT_FLAGS) != md_flags))
    return MDBX_INCOMPATIBLE;

  memcpy(&txn->mt_dbs[dbi], data.iov_base, sizeof(MDBX_db));
  txn->mt_dbflags[dbi] &= ~DB_STALE;
  return MDBX_SUCCESS;
}

/* Search for the lowest key under the current branch page.
 * This just bypasses a numkeys check in the current page
 * before calling mdbx_page_search_root(), because the callers
 * are all in situations where the current page is known to
 * be underfilled. */
__hot static int mdbx_page_search_lowest(MDBX_cursor *mc) {
  MDBX_page *mp = mc->mc_pg[mc->mc_top];
  mdbx_cassert(mc, IS_BRANCH(mp));
  MDBX_node *node = page_node(mp, 0);
  int rc;

  if (unlikely((rc = mdbx_page_get(mc, node_pgno(node), &mp, NULL)) != 0))
    return rc;

  mc->mc_ki[mc->mc_top] = 0;
  if (unlikely(rc = mdbx_cursor_push(mc, mp)))
    return rc;
  return mdbx_page_search_root(mc, NULL, MDBX_PS_FIRST);
}

/* Search for the page a given key should be in.
 * Push it and its parent pages on the cursor stack.
 *
 * [in,out] mc  the cursor for this operation.
 * [in] key     the key to search for, or NULL for first/last page.
 * [in] flags   If MDBX_PS_MODIFY is set, visited pages in the DB
 *              are touched (updated with new page numbers).
 *              If MDBX_PS_FIRST or MDBX_PS_LAST is set, find first or last
 * leaf.
 *              This is used by mdbx_cursor_first() and mdbx_cursor_last().
 *              If MDBX_PS_ROOTONLY set, just fetch root node, no further
 *              lookups.
 *
 * Returns 0 on success, non-zero on failure. */
__hot static int mdbx_page_search(MDBX_cursor *mc, MDBX_val *key, int flags) {
  int rc;
  pgno_t root;

  /* Make sure the txn is still viable, then find the root from
   * the txn's db table and set it as the root of the cursor's stack. */
  if (unlikely(mc->mc_txn->mt_flags & MDBX_TXN_BLOCKED)) {
    mdbx_debug("%s", "transaction has failed, must abort");
    return MDBX_BAD_TXN;
  }

  /* Make sure we're using an up-to-date root */
  if (unlikely(*mc->mc_dbflag & DB_STALE)) {
    rc = mdbx_fetch_sdb(mc->mc_txn, mc->mc_dbi);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }
  root = mc->mc_db->md_root;

  if (unlikely(root == P_INVALID)) { /* Tree is empty. */
    mdbx_debug("%s", "tree is empty");
    return MDBX_NOTFOUND;
  }

  mdbx_cassert(mc, root >= NUM_METAS);
  if (!mc->mc_pg[0] || mc->mc_pg[0]->mp_pgno != root)
    if (unlikely((rc = mdbx_page_get(mc, root, &mc->mc_pg[0], NULL)) != 0))
      return rc;

  mc->mc_snum = 1;
  mc->mc_top = 0;

  mdbx_debug("db %d root page %" PRIaPGNO " has flags 0x%X", DDBI(mc), root,
             mc->mc_pg[0]->mp_flags);

  if (flags & MDBX_PS_MODIFY) {
    if (unlikely(rc = mdbx_page_touch(mc)))
      return rc;
  }

  if (flags & MDBX_PS_ROOTONLY)
    return MDBX_SUCCESS;

  return mdbx_page_search_root(mc, key, flags);
}

/* Return the data associated with a given node.
 *
 * [in] mc      The cursor for this operation.
 * [in] leaf    The node being read.
 * [out] data   Updated to point to the node's data.
 *
 * Returns 0 on success, non-zero on failure. */
static __inline int mdbx_node_read(MDBX_cursor *mc, MDBX_node *node,
                                   MDBX_val *data) {
  data->iov_len = node_ds(node);
  data->iov_base = node_data(node);
  if (unlikely(F_ISSET(node_flags(node), F_BIGDATA))) {
    /* Read overflow data. */
    MDBX_page *omp; /* overflow page */
    int rc = mdbx_page_get(mc, node_largedata_pgno(node), &omp, NULL);
    if (unlikely((rc != MDBX_SUCCESS))) {
      mdbx_debug("read overflow page %" PRIaPGNO " failed",
                 node_largedata_pgno(node));
      return rc;
    }
    data->iov_base = page_data(omp);
  }
  return MDBX_SUCCESS;
}

int mdbx_get(MDBX_txn *txn, MDBX_dbi dbi, MDBX_val *key, MDBX_val *data) {
  DKBUF;
  mdbx_debug("===> get db %u key [%s]", dbi, DKEY(key));

  int rc = check_txn(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!key || !data))
    return MDBX_EINVAL;

  if (unlikely(!TXN_DBI_EXIST(txn, dbi, DB_USRVALID)))
    return MDBX_EINVAL;

  MDBX_cursor_couple cx;
  rc = mdbx_cursor_init(&cx.outer, txn, dbi);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  int exact = 0;
  return mdbx_cursor_set(&cx.outer, key, data, MDBX_SET, &exact);
}

int mdbx_get_nearest(MDBX_txn *txn, MDBX_dbi dbi, MDBX_val *key,
                     MDBX_val *data) {
  DKBUF;
  mdbx_debug("===> get db %u key [%s]", dbi, DKEY(key));

  int rc = check_txn(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!key || !data))
    return MDBX_EINVAL;

  if (unlikely(!TXN_DBI_EXIST(txn, dbi, DB_USRVALID)))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_flags & MDBX_TXN_BLOCKED))
    return MDBX_BAD_TXN;

  MDBX_cursor_couple cx;
  rc = mdbx_cursor_init(&cx.outer, txn, dbi);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  MDBX_val save_data = *data;
  int exact = 0;
  rc = mdbx_cursor_set(&cx.outer, key, data, MDBX_SET_RANGE, &exact);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (exact && (txn->mt_dbs[dbi].md_flags & MDBX_DUPSORT) != 0) {
    *data = save_data;
    exact = 0;
    rc = mdbx_cursor_set(&cx.outer, key, data, MDBX_GET_BOTH_RANGE, &exact);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }

  return exact ? MDBX_SUCCESS : MDBX_RESULT_TRUE;
}

int mdbx_get_ex(MDBX_txn *txn, MDBX_dbi dbi, MDBX_val *key, MDBX_val *data,
                size_t *values_count) {
  DKBUF;
  mdbx_debug("===> get db %u key [%s]", dbi, DKEY(key));

  int rc = check_txn(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!key || !data))
    return MDBX_EINVAL;

  if (unlikely(!TXN_DBI_EXIST(txn, dbi, DB_USRVALID)))
    return MDBX_EINVAL;

  MDBX_cursor_couple cx;
  rc = mdbx_cursor_init(&cx.outer, txn, dbi);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  int exact = 0;
  rc = mdbx_cursor_set(&cx.outer, key, data, MDBX_SET_KEY, &exact);
  if (unlikely(rc != MDBX_SUCCESS)) {
    if (rc == MDBX_NOTFOUND && values_count)
      *values_count = 0;
    return rc;
  }

  if (values_count) {
    *values_count = 1;
    if (cx.outer.mc_xcursor != NULL) {
      MDBX_node *node = page_node(cx.outer.mc_pg[cx.outer.mc_top],
                                  cx.outer.mc_ki[cx.outer.mc_top]);
      if (F_ISSET(node_flags(node), F_DUPDATA)) {
        mdbx_tassert(txn, cx.outer.mc_xcursor == &cx.inner &&
                              (cx.inner.mx_cursor.mc_flags & C_INITIALIZED));
        *values_count =
            (sizeof(*values_count) >= sizeof(cx.inner.mx_db.md_entries) ||
             cx.inner.mx_db.md_entries <= PTRDIFF_MAX)
                ? (size_t)cx.inner.mx_db.md_entries
                : PTRDIFF_MAX;
      }
    }
  }
  return MDBX_SUCCESS;
}

/* Find a sibling for a page.
 * Replaces the page at the top of the cursor's stack with the specified
 * sibling, if one exists.
 *
 * [in] mc          The cursor for this operation.
 * [in] move_right  Non-zero if the right sibling is requested,
 *                  otherwise the left sibling.
 *
 * Returns 0 on success, non-zero on failure. */
static int mdbx_cursor_sibling(MDBX_cursor *mc, int move_right) {
  int rc;
  MDBX_node *indx;
  MDBX_page *mp;

  if (unlikely(mc->mc_snum < 2))
    return MDBX_NOTFOUND; /* root has no siblings */

  mdbx_cursor_pop(mc);
  mdbx_debug("parent page is page %" PRIaPGNO ", index %u",
             mc->mc_pg[mc->mc_top]->mp_pgno, mc->mc_ki[mc->mc_top]);

  if (move_right
          ? (mc->mc_ki[mc->mc_top] + 1u >= page_numkeys(mc->mc_pg[mc->mc_top]))
          : (mc->mc_ki[mc->mc_top] == 0)) {
    mdbx_debug("no more keys left, moving to %s sibling",
               move_right ? "right" : "left");
    if (unlikely((rc = mdbx_cursor_sibling(mc, move_right)) != MDBX_SUCCESS)) {
      /* undo cursor_pop before returning */
      mc->mc_top++;
      mc->mc_snum++;
      return rc;
    }
  } else {
    if (move_right)
      mc->mc_ki[mc->mc_top]++;
    else
      mc->mc_ki[mc->mc_top]--;
    mdbx_debug("just moving to %s index key %u", move_right ? "right" : "left",
               mc->mc_ki[mc->mc_top]);
  }
  mdbx_cassert(mc, IS_BRANCH(mc->mc_pg[mc->mc_top]));

  indx = page_node(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top]);
  if (unlikely((rc = mdbx_page_get(mc, node_pgno(indx), &mp, NULL)) != 0)) {
    /* mc will be inconsistent if caller does mc_snum++ as above */
    mc->mc_flags &= ~(C_INITIALIZED | C_EOF);
    return rc;
  }

  rc = mdbx_cursor_push(mc, mp);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;
  if (!move_right)
    mc->mc_ki[mc->mc_top] = (indx_t)page_numkeys(mp) - 1;

  return MDBX_SUCCESS;
}

/* Move the cursor to the next data item. */
static int mdbx_cursor_next(MDBX_cursor *mc, MDBX_val *key, MDBX_val *data,
                            MDBX_cursor_op op) {
  MDBX_page *mp;
  MDBX_node *node;
  int rc;

  if ((mc->mc_flags & C_DEL) && op == MDBX_NEXT_DUP)
    return MDBX_NOTFOUND;

  if (!(mc->mc_flags & C_INITIALIZED))
    return mdbx_cursor_first(mc, key, data);

  mp = mc->mc_pg[mc->mc_top];
  if (mc->mc_flags & C_EOF) {
    if (mc->mc_ki[mc->mc_top] + 1u >= page_numkeys(mp))
      return MDBX_NOTFOUND;
    mc->mc_flags ^= C_EOF;
  }

  if (mc->mc_db->md_flags & MDBX_DUPSORT) {
    node = page_node(mp, mc->mc_ki[mc->mc_top]);
    if (F_ISSET(node_flags(node), F_DUPDATA)) {
      if (op == MDBX_NEXT || op == MDBX_NEXT_DUP) {
        rc =
            mdbx_cursor_next(&mc->mc_xcursor->mx_cursor, data, NULL, MDBX_NEXT);
        if (op != MDBX_NEXT || rc != MDBX_NOTFOUND) {
          if (likely(rc == MDBX_SUCCESS))
            get_key_optional(node, key);
          return rc;
        }
      }
    } else {
      mc->mc_xcursor->mx_cursor.mc_flags &= ~(C_INITIALIZED | C_EOF);
      if (op == MDBX_NEXT_DUP)
        return MDBX_NOTFOUND;
    }
  }

  mdbx_debug("cursor_next: top page is %" PRIaPGNO " in cursor %p", mp->mp_pgno,
             (void *)mc);
  if (mc->mc_flags & C_DEL) {
    mc->mc_flags ^= C_DEL;
    goto skip;
  }

  if (mc->mc_ki[mc->mc_top] + 1u >= page_numkeys(mp)) {
    mdbx_debug("%s", "=====> move to next sibling page");
    if (unlikely((rc = mdbx_cursor_sibling(mc, 1)) != MDBX_SUCCESS)) {
      mc->mc_flags |= C_EOF;
      return rc;
    }
    mp = mc->mc_pg[mc->mc_top];
    mdbx_debug("next page is %" PRIaPGNO ", key index %u", mp->mp_pgno,
               mc->mc_ki[mc->mc_top]);
  } else
    mc->mc_ki[mc->mc_top]++;

skip:
  mdbx_debug("==> cursor points to page %" PRIaPGNO
             " with %u keys, key index %u",
             mp->mp_pgno, page_numkeys(mp), mc->mc_ki[mc->mc_top]);

  if (IS_LEAF2(mp)) {
    if (likely(key)) {
      key->iov_len = mc->mc_db->md_xsize;
      key->iov_base = page_leaf2key(mp, mc->mc_ki[mc->mc_top], key->iov_len);
    }
    return MDBX_SUCCESS;
  }

  mdbx_cassert(mc, IS_LEAF(mp));
  node = page_node(mp, mc->mc_ki[mc->mc_top]);

  if (F_ISSET(node_flags(node), F_DUPDATA)) {
    rc = mdbx_xcursor_init1(mc, node);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }
  if (data) {
    if (unlikely((rc = mdbx_node_read(mc, node, data)) != MDBX_SUCCESS))
      return rc;

    if (F_ISSET(node_flags(node), F_DUPDATA)) {
      rc = mdbx_cursor_first(&mc->mc_xcursor->mx_cursor, data, NULL);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
    }
  }

  get_key_optional(node, key);
  return MDBX_SUCCESS;
}

/* Move the cursor to the previous data item. */
static int mdbx_cursor_prev(MDBX_cursor *mc, MDBX_val *key, MDBX_val *data,
                            MDBX_cursor_op op) {
  MDBX_page *mp;
  MDBX_node *node;
  int rc;

  if ((mc->mc_flags & C_DEL) && op == MDBX_PREV_DUP)
    return MDBX_NOTFOUND;

  if (!(mc->mc_flags & C_INITIALIZED)) {
    rc = mdbx_cursor_last(mc, key, data);
    if (unlikely(rc))
      return rc;
    mc->mc_ki[mc->mc_top]++;
  }

  mp = mc->mc_pg[mc->mc_top];
  if ((mc->mc_db->md_flags & MDBX_DUPSORT) &&
      mc->mc_ki[mc->mc_top] < page_numkeys(mp)) {
    node = page_node(mp, mc->mc_ki[mc->mc_top]);
    if (F_ISSET(node_flags(node), F_DUPDATA)) {
      if (op == MDBX_PREV || op == MDBX_PREV_DUP) {
        rc =
            mdbx_cursor_prev(&mc->mc_xcursor->mx_cursor, data, NULL, MDBX_PREV);
        if (op != MDBX_PREV || rc != MDBX_NOTFOUND) {
          if (likely(rc == MDBX_SUCCESS)) {
            get_key_optional(node, key);
            mc->mc_flags &= ~C_EOF;
          }
          return rc;
        }
      }
    } else {
      mc->mc_xcursor->mx_cursor.mc_flags &= ~(C_INITIALIZED | C_EOF);
      if (op == MDBX_PREV_DUP)
        return MDBX_NOTFOUND;
    }
  }

  mdbx_debug("cursor_prev: top page is %" PRIaPGNO " in cursor %p", mp->mp_pgno,
             (void *)mc);

  mc->mc_flags &= ~(C_EOF | C_DEL);

  if (mc->mc_ki[mc->mc_top] == 0) {
    mdbx_debug("%s", "=====> move to prev sibling page");
    if ((rc = mdbx_cursor_sibling(mc, 0)) != MDBX_SUCCESS) {
      return rc;
    }
    mp = mc->mc_pg[mc->mc_top];
    mc->mc_ki[mc->mc_top] = (indx_t)page_numkeys(mp) - 1;
    mdbx_debug("prev page is %" PRIaPGNO ", key index %u", mp->mp_pgno,
               mc->mc_ki[mc->mc_top]);
  } else
    mc->mc_ki[mc->mc_top]--;

  mdbx_debug("==> cursor points to page %" PRIaPGNO
             " with %u keys, key index %u",
             mp->mp_pgno, page_numkeys(mp), mc->mc_ki[mc->mc_top]);

  if (IS_LEAF2(mp)) {
    if (likely(key)) {
      key->iov_len = mc->mc_db->md_xsize;
      key->iov_base = page_leaf2key(mp, mc->mc_ki[mc->mc_top], key->iov_len);
    }
    return MDBX_SUCCESS;
  }

  mdbx_cassert(mc, IS_LEAF(mp));
  node = page_node(mp, mc->mc_ki[mc->mc_top]);

  if (F_ISSET(node_flags(node), F_DUPDATA)) {
    rc = mdbx_xcursor_init1(mc, node);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }
  if (data) {
    if (unlikely((rc = mdbx_node_read(mc, node, data)) != MDBX_SUCCESS))
      return rc;

    if (F_ISSET(node_flags(node), F_DUPDATA)) {
      rc = mdbx_cursor_last(&mc->mc_xcursor->mx_cursor, data, NULL);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
    }
  }

  get_key_optional(node, key);
  return MDBX_SUCCESS;
}

/* Set the cursor on a specific data item. */
__hot static int mdbx_cursor_set(MDBX_cursor *mc, MDBX_val *key, MDBX_val *data,
                                 MDBX_cursor_op op, int *exactp) {
  int rc;
  MDBX_page *mp;
  MDBX_node *node = NULL;
  DKBUF;

  if ((mc->mc_db->md_flags & MDBX_INTEGERKEY) &&
      unlikely(key->iov_len != sizeof(uint32_t) &&
               key->iov_len != sizeof(uint64_t))) {
    mdbx_cassert(mc, !"key-size is invalid for MDBX_INTEGERKEY");
    return MDBX_BAD_VALSIZE;
  }

  if (mc->mc_xcursor)
    mc->mc_xcursor->mx_cursor.mc_flags &= ~(C_INITIALIZED | C_EOF);

  /* See if we're already on the right page */
  if (mc->mc_flags & C_INITIALIZED) {
    MDBX_val nodekey;

    mdbx_cassert(mc, IS_LEAF(mc->mc_pg[mc->mc_top]));
    mp = mc->mc_pg[mc->mc_top];
    if (!page_numkeys(mp)) {
      mc->mc_ki[mc->mc_top] = 0;
      return MDBX_NOTFOUND;
    }
    if (IS_LEAF2(mp)) {
      nodekey.iov_len = mc->mc_db->md_xsize;
      nodekey.iov_base = page_leaf2key(mp, 0, nodekey.iov_len);
    } else {
      node = page_node(mp, 0);
      get_key(node, &nodekey);
    }
    rc = mc->mc_dbx->md_cmp(key, &nodekey);
    if (unlikely(rc == 0)) {
      /* Probably happens rarely, but first node on the page
       * was the one we wanted. */
      mc->mc_ki[mc->mc_top] = 0;
      if (exactp)
        *exactp = 1;
      goto set1;
    }
    if (rc > 0) {
      const unsigned nkeys = page_numkeys(mp);
      unsigned i;
      if (nkeys > 1) {
        if (IS_LEAF2(mp)) {
          nodekey.iov_base = page_leaf2key(mp, nkeys - 1, nodekey.iov_len);
        } else {
          node = page_node(mp, nkeys - 1);
          get_key(node, &nodekey);
        }
        rc = mc->mc_dbx->md_cmp(key, &nodekey);
        if (rc == 0) {
          /* last node was the one we wanted */
          mdbx_cassert(mc, nkeys >= 1 && nkeys <= UINT16_MAX + 1);
          mc->mc_ki[mc->mc_top] = (indx_t)(nkeys - 1);
          if (exactp)
            *exactp = 1;
          goto set1;
        }
        if (rc < 0) {
          if (mc->mc_ki[mc->mc_top] < page_numkeys(mp)) {
            /* This is definitely the right page, skip search_page */
            if (IS_LEAF2(mp)) {
              nodekey.iov_base =
                  page_leaf2key(mp, mc->mc_ki[mc->mc_top], nodekey.iov_len);
            } else {
              node = page_node(mp, mc->mc_ki[mc->mc_top]);
              get_key(node, &nodekey);
            }
            rc = mc->mc_dbx->md_cmp(key, &nodekey);
            if (rc == 0) {
              /* current node was the one we wanted */
              if (exactp)
                *exactp = 1;
              goto set1;
            }
          }
          rc = 0;
          mc->mc_flags &= ~C_EOF;
          goto set2;
        }
      }
      /* If any parents have right-sibs, search.
       * Otherwise, there's nothing further. */
      for (i = 0; i < mc->mc_top; i++)
        if (mc->mc_ki[i] < page_numkeys(mc->mc_pg[i]) - 1)
          break;
      if (i == mc->mc_top) {
        /* There are no other pages */
        mdbx_cassert(mc, nkeys <= UINT16_MAX);
        mc->mc_ki[mc->mc_top] = (uint16_t)nkeys;
        return MDBX_NOTFOUND;
      }
    }
    if (!mc->mc_top) {
      /* There are no other pages */
      mc->mc_ki[mc->mc_top] = 0;
      if (op == MDBX_SET_RANGE && !exactp) {
        rc = 0;
        goto set1;
      } else
        return MDBX_NOTFOUND;
    }
  } else {
    mc->mc_pg[0] = 0;
  }

  rc = mdbx_page_search(mc, key, 0);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  mp = mc->mc_pg[mc->mc_top];
  mdbx_cassert(mc, IS_LEAF(mp));

set2:
  node = mdbx_node_search(mc, key, exactp);
  if (exactp != NULL && !*exactp) {
    /* MDBX_SET specified and not an exact match. */
    return MDBX_NOTFOUND;
  }

  if (node == NULL) {
    mdbx_debug("%s", "===> inexact leaf not found, goto sibling");
    if (unlikely((rc = mdbx_cursor_sibling(mc, 1)) != MDBX_SUCCESS)) {
      mc->mc_flags |= C_EOF;
      return rc; /* no entries matched */
    }
    mp = mc->mc_pg[mc->mc_top];
    mdbx_cassert(mc, IS_LEAF(mp));
    node = page_node(mp, 0);
  }

set1:
  mc->mc_flags |= C_INITIALIZED;
  mc->mc_flags &= ~C_EOF;

  if (IS_LEAF2(mp)) {
    if (op == MDBX_SET_RANGE || op == MDBX_SET_KEY) {
      key->iov_len = mc->mc_db->md_xsize;
      key->iov_base = page_leaf2key(mp, mc->mc_ki[mc->mc_top], key->iov_len);
    }
    return MDBX_SUCCESS;
  }

  if (F_ISSET(node_flags(node), F_DUPDATA)) {
    rc = mdbx_xcursor_init1(mc, node);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }
  if (likely(data)) {
    if (F_ISSET(node_flags(node), F_DUPDATA)) {
      if (op == MDBX_SET || op == MDBX_SET_KEY || op == MDBX_SET_RANGE) {
        rc = mdbx_cursor_first(&mc->mc_xcursor->mx_cursor, data, NULL);
      } else {
        int ex2, *ex2p;
        if (op == MDBX_GET_BOTH) {
          ex2p = &ex2;
          ex2 = 0;
        } else {
          ex2p = NULL;
        }
        rc = mdbx_cursor_set(&mc->mc_xcursor->mx_cursor, data, NULL,
                             MDBX_SET_RANGE, ex2p);
        if (unlikely(rc != MDBX_SUCCESS))
          return rc;
      }
    } else if (op == MDBX_GET_BOTH || op == MDBX_GET_BOTH_RANGE) {
      MDBX_val olddata;
      if (unlikely((rc = mdbx_node_read(mc, node, &olddata)) != MDBX_SUCCESS))
        return rc;
      if (unlikely(mc->mc_dbx->md_dcmp == NULL))
        return MDBX_EINVAL;
      rc = mc->mc_dbx->md_dcmp(data, &olddata);
      if (rc) {
        if (op != MDBX_GET_BOTH_RANGE || rc > 0)
          return MDBX_NOTFOUND;
        rc = 0;
      }
      *data = olddata;
    } else {
      if (mc->mc_xcursor)
        mc->mc_xcursor->mx_cursor.mc_flags &= ~(C_INITIALIZED | C_EOF);
      if (unlikely((rc = mdbx_node_read(mc, node, data)) != MDBX_SUCCESS))
        return rc;
    }
  }

  /* The key already matches in all other cases */
  if (op == MDBX_SET_RANGE || op == MDBX_SET_KEY)
    get_key_optional(node, key);

  mdbx_debug("==> cursor placed on key [%s], data [%s]", DKEY(key), DVAL(data));
  return rc;
}

/* Move the cursor to the first item in the database. */
static int mdbx_cursor_first(MDBX_cursor *mc, MDBX_val *key, MDBX_val *data) {
  int rc;

  if (mc->mc_xcursor)
    mc->mc_xcursor->mx_cursor.mc_flags &= ~(C_INITIALIZED | C_EOF);

  if (!(mc->mc_flags & C_INITIALIZED) || mc->mc_top) {
    rc = mdbx_page_search(mc, NULL, MDBX_PS_FIRST);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }
  mdbx_cassert(mc, IS_LEAF(mc->mc_pg[mc->mc_top]));

  mc->mc_flags |= C_INITIALIZED;
  mc->mc_flags &= ~C_EOF;
  mc->mc_ki[mc->mc_top] = 0;

  if (IS_LEAF2(mc->mc_pg[mc->mc_top])) {
    key->iov_len = mc->mc_db->md_xsize;
    key->iov_base = page_leaf2key(mc->mc_pg[mc->mc_top], 0, key->iov_len);
    return MDBX_SUCCESS;
  }

  MDBX_node *node = page_node(mc->mc_pg[mc->mc_top], 0);
  if (likely(data)) {
    if (F_ISSET(node_flags(node), F_DUPDATA)) {
      rc = mdbx_xcursor_init1(mc, node);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
      rc = mdbx_cursor_first(&mc->mc_xcursor->mx_cursor, data, NULL);
      if (unlikely(rc))
        return rc;
    } else {
      if (unlikely((rc = mdbx_node_read(mc, node, data)) != MDBX_SUCCESS))
        return rc;
    }
  }
  get_key_optional(node, key);
  return MDBX_SUCCESS;
}

/* Move the cursor to the last item in the database. */
static int mdbx_cursor_last(MDBX_cursor *mc, MDBX_val *key, MDBX_val *data) {
  int rc;

  if (mc->mc_xcursor)
    mc->mc_xcursor->mx_cursor.mc_flags &= ~(C_INITIALIZED | C_EOF);

  if (likely((mc->mc_flags & (C_EOF | C_DEL)) != C_EOF)) {
    if (!(mc->mc_flags & C_INITIALIZED) || mc->mc_top) {
      rc = mdbx_page_search(mc, NULL, MDBX_PS_LAST);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
    }
    mdbx_cassert(mc, IS_LEAF(mc->mc_pg[mc->mc_top]));
  }

  mc->mc_ki[mc->mc_top] = (indx_t)page_numkeys(mc->mc_pg[mc->mc_top]) - 1;
  mc->mc_flags |= C_INITIALIZED | C_EOF;

  if (IS_LEAF2(mc->mc_pg[mc->mc_top])) {
    key->iov_len = mc->mc_db->md_xsize;
    key->iov_base = page_leaf2key(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top],
                                  key->iov_len);
    return MDBX_SUCCESS;
  }

  MDBX_node *node = page_node(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top]);
  if (likely(data)) {
    if (F_ISSET(node_flags(node), F_DUPDATA)) {
      rc = mdbx_xcursor_init1(mc, node);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
      rc = mdbx_cursor_last(&mc->mc_xcursor->mx_cursor, data, NULL);
      if (unlikely(rc))
        return rc;
    } else {
      if (unlikely((rc = mdbx_node_read(mc, node, data)) != MDBX_SUCCESS))
        return rc;
    }
  }

  get_key_optional(node, key);
  return MDBX_SUCCESS;
}

int mdbx_cursor_get(MDBX_cursor *mc, MDBX_val *key, MDBX_val *data,
                    MDBX_cursor_op op) {
  if (unlikely(mc == NULL))
    return MDBX_EINVAL;

  if (unlikely(mc->mc_signature != MDBX_MC_SIGNATURE))
    return MDBX_EBADSIGN;

  int rc = check_txn(mc->mc_txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  int exact = 0;
  int (*mfunc)(MDBX_cursor * mc, MDBX_val * key, MDBX_val * data);
  switch (op) {
  case MDBX_GET_CURRENT: {
    if (unlikely(!(mc->mc_flags & C_INITIALIZED)))
      return MDBX_EINVAL;
    MDBX_page *mp = mc->mc_pg[mc->mc_top];
    const unsigned nkeys = page_numkeys(mp);
    if (mc->mc_ki[mc->mc_top] >= nkeys) {
      mdbx_cassert(mc, nkeys <= UINT16_MAX);
      mc->mc_ki[mc->mc_top] = (uint16_t)nkeys;
      return MDBX_NOTFOUND;
    }
    mdbx_cassert(mc, nkeys > 0);

    rc = MDBX_SUCCESS;
    if (IS_LEAF2(mp)) {
      key->iov_len = mc->mc_db->md_xsize;
      key->iov_base = page_leaf2key(mp, mc->mc_ki[mc->mc_top], key->iov_len);
    } else {
      MDBX_node *node = page_node(mp, mc->mc_ki[mc->mc_top]);
      get_key_optional(node, key);
      if (data) {
        if (F_ISSET(node_flags(node), F_DUPDATA)) {
          if (unlikely(!(mc->mc_xcursor->mx_cursor.mc_flags & C_INITIALIZED))) {
            rc = mdbx_xcursor_init1(mc, node);
            if (unlikely(rc != MDBX_SUCCESS))
              return rc;
            rc = mdbx_cursor_first(&mc->mc_xcursor->mx_cursor, data, NULL);
            if (unlikely(rc))
              return rc;
          }
          rc = mdbx_cursor_get(&mc->mc_xcursor->mx_cursor, data, NULL,
                               MDBX_GET_CURRENT);
        } else {
          rc = mdbx_node_read(mc, node, data);
        }
        if (unlikely(rc))
          return rc;
      }
    }
    break;
  }
  case MDBX_GET_BOTH:
  case MDBX_GET_BOTH_RANGE:
    if (unlikely(data == NULL))
      return MDBX_EINVAL;
    if (unlikely(mc->mc_xcursor == NULL))
      return MDBX_INCOMPATIBLE;
    /* fall through */
    __fallthrough;
  case MDBX_SET:
  case MDBX_SET_KEY:
  case MDBX_SET_RANGE:
    if (unlikely(key == NULL))
      return MDBX_EINVAL;
    rc = mdbx_cursor_set(mc, key, data, op,
                         op == MDBX_SET_RANGE ? NULL : &exact);
    break;
  case MDBX_GET_MULTIPLE:
    if (unlikely(data == NULL || !(mc->mc_flags & C_INITIALIZED)))
      return MDBX_EINVAL;
    if (unlikely(!(mc->mc_db->md_flags & MDBX_DUPFIXED)))
      return MDBX_INCOMPATIBLE;
    rc = MDBX_SUCCESS;
    if (!(mc->mc_xcursor->mx_cursor.mc_flags & C_INITIALIZED) ||
        (mc->mc_xcursor->mx_cursor.mc_flags & C_EOF))
      break;
    goto fetchm;
  case MDBX_NEXT_MULTIPLE:
    if (unlikely(data == NULL))
      return MDBX_EINVAL;
    if (unlikely(!(mc->mc_db->md_flags & MDBX_DUPFIXED)))
      return MDBX_INCOMPATIBLE;
    rc = mdbx_cursor_next(mc, key, data, MDBX_NEXT_DUP);
    if (rc == MDBX_SUCCESS) {
      if (mc->mc_xcursor->mx_cursor.mc_flags & C_INITIALIZED) {
        MDBX_cursor *mx;
      fetchm:
        mx = &mc->mc_xcursor->mx_cursor;
        data->iov_len =
            page_numkeys(mx->mc_pg[mx->mc_top]) * mx->mc_db->md_xsize;
        data->iov_base = page_data(mx->mc_pg[mx->mc_top]);
        mx->mc_ki[mx->mc_top] = (indx_t)page_numkeys(mx->mc_pg[mx->mc_top]) - 1;
      } else {
        rc = MDBX_NOTFOUND;
      }
    }
    break;
  case MDBX_PREV_MULTIPLE:
    if (data == NULL)
      return MDBX_EINVAL;
    if (!(mc->mc_db->md_flags & MDBX_DUPFIXED))
      return MDBX_INCOMPATIBLE;
    rc = MDBX_SUCCESS;
    if (!(mc->mc_flags & C_INITIALIZED))
      rc = mdbx_cursor_last(mc, key, data);
    if (rc == MDBX_SUCCESS) {
      MDBX_cursor *mx = &mc->mc_xcursor->mx_cursor;
      if (mx->mc_flags & C_INITIALIZED) {
        rc = mdbx_cursor_sibling(mx, 0);
        if (rc == MDBX_SUCCESS)
          goto fetchm;
      } else {
        rc = MDBX_NOTFOUND;
      }
    }
    break;
  case MDBX_NEXT:
  case MDBX_NEXT_DUP:
  case MDBX_NEXT_NODUP:
    rc = mdbx_cursor_next(mc, key, data, op);
    break;
  case MDBX_PREV:
  case MDBX_PREV_DUP:
  case MDBX_PREV_NODUP:
    rc = mdbx_cursor_prev(mc, key, data, op);
    break;
  case MDBX_FIRST:
    rc = mdbx_cursor_first(mc, key, data);
    break;
  case MDBX_FIRST_DUP:
    mfunc = mdbx_cursor_first;
  mmove:
    if (unlikely(data == NULL || !(mc->mc_flags & C_INITIALIZED)))
      return MDBX_EINVAL;
    if (unlikely(mc->mc_xcursor == NULL))
      return MDBX_INCOMPATIBLE;
    if (mc->mc_ki[mc->mc_top] >= page_numkeys(mc->mc_pg[mc->mc_top])) {
      mc->mc_ki[mc->mc_top] = (indx_t)page_numkeys(mc->mc_pg[mc->mc_top]);
      return MDBX_NOTFOUND;
    }
    {
      MDBX_node *node = page_node(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top]);
      if (!F_ISSET(node_flags(node), F_DUPDATA)) {
        get_key_optional(node, key);
        rc = mdbx_node_read(mc, node, data);
        break;
      }
    }
    if (unlikely(!(mc->mc_xcursor->mx_cursor.mc_flags & C_INITIALIZED)))
      return MDBX_EINVAL;
    rc = mfunc(&mc->mc_xcursor->mx_cursor, data, NULL);
    break;
  case MDBX_LAST:
    rc = mdbx_cursor_last(mc, key, data);
    break;
  case MDBX_LAST_DUP:
    mfunc = mdbx_cursor_last;
    goto mmove;
  default:
    mdbx_debug("unhandled/unimplemented cursor operation %u", op);
    return MDBX_EINVAL;
  }

  mc->mc_flags &= ~C_DEL;
  return rc;
}

/* Touch all the pages in the cursor stack. Set mc_top.
 * Makes sure all the pages are writable, before attempting a write operation.
 * [in] mc The cursor to operate on. */
static int mdbx_cursor_touch(MDBX_cursor *mc) {
  int rc = MDBX_SUCCESS;

  if (mc->mc_dbi >= CORE_DBS &&
      (*mc->mc_dbflag & (DB_DIRTY | DB_DUPDATA)) == 0) {
    mdbx_cassert(mc, (mc->mc_flags & C_RECLAIMING) == 0);
    /* Touch DB record of named DB */
    MDBX_cursor_couple cx;
    if (TXN_DBI_CHANGED(mc->mc_txn, mc->mc_dbi))
      return MDBX_BAD_DBI;
    rc = mdbx_cursor_init(&cx.outer, mc->mc_txn, MAIN_DBI);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    rc = mdbx_page_search(&cx.outer, &mc->mc_dbx->md_name, MDBX_PS_MODIFY);
    if (unlikely(rc))
      return rc;
    *mc->mc_dbflag |= DB_DIRTY;
  }
  mc->mc_top = 0;
  if (mc->mc_snum) {
    do {
      rc = mdbx_page_touch(mc);
    } while (!rc && ++(mc->mc_top) < mc->mc_snum);
    mc->mc_top = mc->mc_snum - 1;
  }
  return rc;
}

int mdbx_cursor_put(MDBX_cursor *mc, MDBX_val *key, MDBX_val *data,
                    unsigned flags) {
  MDBX_env *env;
  MDBX_page *fp, *sub_root = NULL;
  uint16_t fp_flags;
  MDBX_val xdata, *rdata, dkey, olddata;
  MDBX_db nested_dupdb;
  unsigned mcount = 0, dcount = 0, nospill;
  size_t nsize;
  int rc2;
  unsigned nflags;
  DKBUF;

  if (unlikely(mc == NULL || key == NULL))
    return MDBX_EINVAL;

  if (unlikely(mc->mc_signature != MDBX_MC_SIGNATURE))
    return MDBX_EBADSIGN;

  int rc = check_txn_rw(mc->mc_txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  env = mc->mc_txn->mt_env;

  /* Check this first so counter will always be zero on any early failures. */
  if (flags & MDBX_MULTIPLE) {
    if (unlikely(!F_ISSET(mc->mc_db->md_flags, MDBX_DUPFIXED)))
      return MDBX_INCOMPATIBLE;
    if (unlikely(data[1].iov_len >= INT_MAX))
      return MDBX_EINVAL;
    dcount = (unsigned)data[1].iov_len;
    data[1].iov_len = 0;
  }

  if (flags & MDBX_RESERVE) {
    if (unlikely(mc->mc_db->md_flags & (MDBX_DUPSORT | MDBX_REVERSEDUP)))
      return MDBX_INCOMPATIBLE;
    data->iov_base = nullptr;
  }

  nospill = flags & MDBX_NOSPILL;
  flags &= ~MDBX_NOSPILL;

  if (unlikely(mc->mc_txn->mt_flags & (MDBX_RDONLY | MDBX_TXN_BLOCKED)))
    return (mc->mc_txn->mt_flags & MDBX_RDONLY) ? MDBX_EACCESS : MDBX_BAD_TXN;

  if (unlikely(key->iov_len > env->me_maxkey_limit))
    return MDBX_BAD_VALSIZE;

  if (unlikely(data->iov_len > ((mc->mc_db->md_flags & MDBX_DUPSORT)
                                    ? env->me_maxkey_limit
                                    : MDBX_MAXDATASIZE)))
    return MDBX_BAD_VALSIZE;

  if ((mc->mc_db->md_flags & MDBX_INTEGERKEY) &&
      unlikely(key->iov_len != sizeof(uint32_t) &&
               key->iov_len != sizeof(uint64_t))) {
    mdbx_cassert(mc, !"key-size is invalid for MDBX_INTEGERKEY");
    return MDBX_BAD_VALSIZE;
  }

  if ((mc->mc_db->md_flags & MDBX_INTEGERDUP) &&
      unlikely(data->iov_len != sizeof(uint32_t) &&
               data->iov_len != sizeof(uint64_t))) {
    mdbx_cassert(mc, !"data-size is invalid MDBX_INTEGERDUP");
    return MDBX_BAD_VALSIZE;
  }

  mdbx_debug("==> put db %d key [%s], size %" PRIuPTR
             ", data [%s] size %" PRIuPTR,
             DDBI(mc), DKEY(key), key ? key->iov_len : 0,
             DVAL((flags & MDBX_RESERVE) ? nullptr : data), data->iov_len);

  int dupdata_flag = 0;
  if ((flags & MDBX_CURRENT) != 0 && (mc->mc_flags & C_SUB) == 0) {
    /* Опция MDBX_CURRENT означает, что запрошено обновление текущей записи,
     * на которой сейчас стоит курсор. Проверяем что переданный ключ совпадает
     * со значением в текущей позиции курсора.
     * Здесь проще вызвать mdbx_cursor_get(), так как для обслуживания таблиц
     * с MDBX_DUPSORT также требуется текущий размер данных. */
    MDBX_val current_key, current_data;
    rc = mdbx_cursor_get(mc, &current_key, &current_data, MDBX_GET_CURRENT);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    if (mc->mc_dbx->md_cmp(key, &current_key) != 0)
      return MDBX_EKEYMISMATCH;

    if (F_ISSET(mc->mc_db->md_flags, MDBX_DUPSORT)) {
      MDBX_node *node = page_node(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top]);
      if (F_ISSET(node_flags(node), F_DUPDATA)) {
        mdbx_cassert(mc,
                     mc->mc_xcursor != NULL &&
                         (mc->mc_xcursor->mx_cursor.mc_flags & C_INITIALIZED));
        /* Если за ключом более одного значения, либо если размер данных
         * отличается, то вместо inplace обновления требуется удаление и
         * последующая вставка. */
        if (mc->mc_xcursor->mx_db.md_entries > 1 ||
            current_data.iov_len != data->iov_len) {
          rc = mdbx_cursor_del(mc, 0);
          if (rc != MDBX_SUCCESS)
            return rc;
          flags -= MDBX_CURRENT;
        }
      } else if (unlikely(node_size(key, data) > env->me_nodemax)) {
        rc = mdbx_cursor_del(mc, 0);
        if (rc != MDBX_SUCCESS)
          return rc;
        flags -= MDBX_CURRENT;
      }
    }
  }

  if (mc->mc_db->md_root == P_INVALID) {
    /* new database, cursor has nothing to point to */
    mc->mc_snum = 0;
    mc->mc_top = 0;
    mc->mc_flags &= ~C_INITIALIZED;
    rc = MDBX_NO_ROOT;
  } else if ((flags & MDBX_CURRENT) == 0) {
    int exact = 0;
    MDBX_val d2;
    if (flags & MDBX_APPEND) {
      MDBX_val k2;
      rc = mdbx_cursor_last(mc, &k2, &d2);
      if (rc == 0) {
        rc = mc->mc_dbx->md_cmp(key, &k2);
        if (rc > 0) {
          rc = MDBX_NOTFOUND;
          mc->mc_ki[mc->mc_top]++;
        } else if (unlikely(rc < 0 || (flags & MDBX_APPENDDUP) == 0)) {
          /* new key is <= last key */
          rc = MDBX_EKEYMISMATCH;
        }
      }
    } else {
      rc = mdbx_cursor_set(mc, key, &d2, MDBX_SET, &exact);
    }
    if ((flags & MDBX_NOOVERWRITE) &&
        (rc == MDBX_SUCCESS || rc == MDBX_EKEYMISMATCH)) {
      mdbx_debug("duplicate key [%s]", DKEY(key));
      *data = d2;
      return MDBX_KEYEXIST;
    }
    if (rc && unlikely(rc != MDBX_NOTFOUND))
      return rc;
  }

  mc->mc_flags &= ~C_DEL;

  /* Cursor is positioned, check for room in the dirty list */
  if (!nospill) {
    if (flags & MDBX_MULTIPLE) {
      rdata = &xdata;
      xdata.iov_len = data->iov_len * dcount;
    } else {
      rdata = data;
    }
    if (unlikely(rc2 = mdbx_page_spill(mc, key, rdata)))
      return rc2;
  }

  if (rc == MDBX_NO_ROOT) {
    MDBX_page *np;
    /* new database, write a root leaf page */
    mdbx_debug("%s", "allocating new root leaf page");
    if (unlikely(rc2 = mdbx_page_new(mc, P_LEAF, 1, &np))) {
      return rc2;
    }
    rc2 = mdbx_cursor_push(mc, np);
    if (unlikely(rc2 != MDBX_SUCCESS))
      return rc2;
    mc->mc_db->md_root = np->mp_pgno;
    mc->mc_db->md_depth++;
    *mc->mc_dbflag |= DB_DIRTY;
    if ((mc->mc_db->md_flags & (MDBX_DUPSORT | MDBX_DUPFIXED)) == MDBX_DUPFIXED)
      np->mp_flags |= P_LEAF2;
    mc->mc_flags |= C_INITIALIZED;
  } else {
    /* make sure all cursor pages are writable */
    rc2 = mdbx_cursor_touch(mc);
    if (unlikely(rc2))
      return rc2;
  }

  bool insert_key, insert_data, do_sub = false;
  insert_key = insert_data = (rc != MDBX_SUCCESS);
  if (insert_key) {
    /* The key does not exist */
    mdbx_debug("inserting key at index %i", mc->mc_ki[mc->mc_top]);
    if ((mc->mc_db->md_flags & MDBX_DUPSORT) &&
        node_size(key, data) > env->me_nodemax) {
      /* Too big for a node, insert in sub-DB.  Set up an empty
       * "old sub-page" for prep_subDB to expand to a full page. */
      fp_flags = P_LEAF | P_DIRTY;
      fp = env->me_pbuf;
      fp->mp_leaf2_ksize = (uint16_t)data->iov_len; /* used if MDBX_DUPFIXED */
      fp->mp_lower = fp->mp_upper = 0;
      olddata.iov_len = PAGEHDRSZ;
      goto prep_subDB;
    }
  } else {
    /* there's only a key anyway, so this is a no-op */
    if (IS_LEAF2(mc->mc_pg[mc->mc_top])) {
      char *ptr;
      unsigned ksize = mc->mc_db->md_xsize;
      if (key->iov_len != ksize)
        return MDBX_BAD_VALSIZE;
      ptr = page_leaf2key(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top], ksize);
      memcpy(ptr, key->iov_base, ksize);
    fix_parent:
      /* if overwriting slot 0 of leaf, need to
       * update branch key if there is a parent page */
      if (mc->mc_top && !mc->mc_ki[mc->mc_top]) {
        unsigned dtop = 1;
        mc->mc_top--;
        /* slot 0 is always an empty key, find real slot */
        while (mc->mc_top && !mc->mc_ki[mc->mc_top]) {
          mc->mc_top--;
          dtop++;
        }
        if (mc->mc_ki[mc->mc_top])
          rc2 = mdbx_update_key(mc, key);
        else
          rc2 = MDBX_SUCCESS;
        mdbx_cassert(mc, mc->mc_top + dtop < UINT16_MAX);
        mc->mc_top += (uint16_t)dtop;
        if (rc2)
          return rc2;
      }

      if (mdbx_audit_enabled()) {
        int err = mdbx_cursor_check(mc, false);
        if (unlikely(err != MDBX_SUCCESS))
          return err;
      }
      return MDBX_SUCCESS;
    }

  more:;
    if (mdbx_audit_enabled()) {
      int err = mdbx_cursor_check(mc, false);
      if (unlikely(err != MDBX_SUCCESS))
        return err;
    }
    MDBX_node *node = page_node(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top]);

    /* overflow page overwrites need special handling */
    if (unlikely(F_ISSET(node_flags(node), F_BIGDATA))) {
      int level, ovpages,
          dpages = (node_size(key, data) > env->me_nodemax)
                       ? number_of_ovpages(env, data->iov_len)
                       : 0;

      const pgno_t pg = node_largedata_pgno(node);
      MDBX_page *omp;
      if (unlikely((rc2 = mdbx_page_get(mc, pg, &omp, &level)) != 0))
        return rc2;
      ovpages = omp->mp_pages;

      /* Is the ov page large enough? */
      if (unlikely(mc->mc_flags & C_GCFREEZE)
              ? ovpages >= dpages
              : ovpages ==
                    /* LY: add configurable threshold to keep reserve space */
                    dpages) {
        if (!IS_DIRTY(omp) && (level || (env->me_flags & MDBX_WRITEMAP))) {
          rc = mdbx_page_unspill(mc->mc_txn, omp, &omp);
          if (unlikely(rc))
            return rc;
          level = 0; /* dirty in this txn or clean */
        }
        /* Is it dirty? */
        if (IS_DIRTY(omp)) {
          /* yes, overwrite it. Note in this case we don't
           * bother to try shrinking the page if the new data
           * is smaller than the overflow threshold. */
          if (unlikely(level > 1)) {
            /* It is writable only in a parent txn */
            MDBX_page *np = mdbx_page_malloc(mc->mc_txn, ovpages);
            if (unlikely(!np))
              return MDBX_ENOMEM;
            /* Note - this page is already counted in parent's dirtyroom */
            rc2 = mdbx_dpl_append(mc->mc_txn->tw.dirtylist, pg, np);
            if (unlikely(rc2 != MDBX_SUCCESS)) {
              rc = rc2;
              mdbx_dpage_free(env, np, ovpages);
              goto fail;
            }

            /* Currently we make the page look as with put() in the
             * parent txn, in case the user peeks at MDBX_RESERVEd
             * or unused parts. Some users treat ovpages specially. */
            const size_t whole = pgno2bytes(env, ovpages);
            /* Skip the part where MDBX will put *data.
             * Copy end of page, adjusting alignment so
             * compiler may copy words instead of bytes. */
            const size_t off =
                (PAGEHDRSZ + data->iov_len) & -(intptr_t)sizeof(size_t);
            memcpy((size_t *)((char *)np + off), (size_t *)((char *)omp + off),
                   whole - off);
            memcpy(np, omp, PAGEHDRSZ); /* Copy header of page */
            omp = np;
          }
          node_set_ds(node, data->iov_len);
          if (F_ISSET(flags, MDBX_RESERVE))
            data->iov_base = page_data(omp);
          else
            memcpy(page_data(omp), data->iov_base, data->iov_len);

          if (mdbx_audit_enabled()) {
            int err = mdbx_cursor_check(mc, false);
            if (unlikely(err != MDBX_SUCCESS))
              return err;
          }
          return MDBX_SUCCESS;
        }
      }
      if ((rc2 = mdbx_page_retire(mc, omp)) != MDBX_SUCCESS)
        return rc2;
    } else {
      olddata.iov_len = node_ds(node);
      olddata.iov_base = node_data(node);
      mdbx_cassert(mc, (char *)olddata.iov_base + olddata.iov_len <=
                           (char *)(mc->mc_pg[mc->mc_top]) + env->me_psize);

      /* DB has dups? */
      if (F_ISSET(mc->mc_db->md_flags, MDBX_DUPSORT)) {
        /* Prepare (sub-)page/sub-DB to accept the new item, if needed.
         * fp: old sub-page or a header faking it.
         * mp: new (sub-)page.  offset: growth in page size.
         * xdata: node data with new page or DB. */
        unsigned i;
        size_t offset = 0;
        MDBX_page *mp = fp = xdata.iov_base = env->me_pbuf;
        mp->mp_pgno = mc->mc_pg[mc->mc_top]->mp_pgno;

        /* Was a single item before, must convert now */
        if (!F_ISSET(node_flags(node), F_DUPDATA)) {

          /* Just overwrite the current item */
          if (flags & MDBX_CURRENT) {
            mdbx_cassert(mc, node_size(key, data) <= env->me_nodemax);
            goto current;
          }

          /* does data match? */
          if (!mc->mc_dbx->md_dcmp(data, &olddata)) {
            if (unlikely(flags & (MDBX_NODUPDATA | MDBX_APPENDDUP)))
              return MDBX_KEYEXIST;
            /* overwrite it */
            mdbx_cassert(mc, node_size(key, data) <= env->me_nodemax);
            goto current;
          }

          /* Back up original data item */
          dupdata_flag = 1;
          dkey.iov_len = olddata.iov_len;
          dkey.iov_base = memcpy(fp + 1, olddata.iov_base, olddata.iov_len);

          /* Make sub-page header for the dup items, with dummy body */
          fp->mp_flags = P_LEAF | P_DIRTY | P_SUBP;
          fp->mp_lower = 0;
          xdata.iov_len = PAGEHDRSZ + dkey.iov_len + data->iov_len;
          if (mc->mc_db->md_flags & MDBX_DUPFIXED) {
            fp->mp_flags |= P_LEAF2;
            fp->mp_leaf2_ksize = (uint16_t)data->iov_len;
            xdata.iov_len += 2 * data->iov_len; /* leave space for 2 more */
            mdbx_cassert(mc, xdata.iov_len <= env->me_psize);
          } else {
            xdata.iov_len += 2 * (sizeof(indx_t) + NODESIZE) +
                             (dkey.iov_len & 1) + (data->iov_len & 1);
            mdbx_cassert(mc, xdata.iov_len <= env->me_psize);
          }
          fp->mp_upper = (uint16_t)(xdata.iov_len - PAGEHDRSZ);
          olddata.iov_len = xdata.iov_len; /* pretend olddata is fp */
        } else if (node_flags(node) & F_SUBDATA) {
          /* Data is on sub-DB, just store it */
          flags |= F_DUPDATA | F_SUBDATA;
          goto put_sub;
        } else {
          /* Data is on sub-page */
          fp = olddata.iov_base;
          switch (flags) {
          default:
            if (!(mc->mc_db->md_flags & MDBX_DUPFIXED)) {
              offset = node_size(data, nullptr) + sizeof(indx_t);
              break;
            }
            offset = fp->mp_leaf2_ksize;
            if (page_room(fp) < offset) {
              offset *= 4; /* space for 4 more */
              break;
            }
            /* FALLTHRU: Big enough MDBX_DUPFIXED sub-page */
            __fallthrough;
          case MDBX_CURRENT | MDBX_NODUPDATA:
          case MDBX_CURRENT:
            fp->mp_flags |= P_DIRTY;
            fp->mp_pgno = mp->mp_pgno;
            mc->mc_xcursor->mx_cursor.mc_pg[0] = fp;
            flags |= F_DUPDATA;
            goto put_sub;
          }
          xdata.iov_len = olddata.iov_len + offset;
        }

        fp_flags = fp->mp_flags;
        if (NODESIZE + node_ks(node) + xdata.iov_len > env->me_nodemax) {
          /* Too big for a sub-page, convert to sub-DB */
          fp_flags &= ~P_SUBP;
        prep_subDB:
          nested_dupdb.md_xsize = 0;
          nested_dupdb.md_flags = 0;
          if (mc->mc_db->md_flags & MDBX_DUPFIXED) {
            fp_flags |= P_LEAF2;
            nested_dupdb.md_xsize = fp->mp_leaf2_ksize;
            nested_dupdb.md_flags = MDBX_DUPFIXED;
            if (mc->mc_db->md_flags & MDBX_INTEGERDUP)
              nested_dupdb.md_flags |= MDBX_INTEGERKEY;
          }
          nested_dupdb.md_depth = 1;
          nested_dupdb.md_branch_pages = 0;
          nested_dupdb.md_leaf_pages = 1;
          nested_dupdb.md_overflow_pages = 0;
          nested_dupdb.md_entries = page_numkeys(fp);
          xdata.iov_len = sizeof(nested_dupdb);
          xdata.iov_base = &nested_dupdb;
          if ((rc = mdbx_page_alloc(mc, 1, &mp, MDBX_ALLOC_ALL)))
            return rc;
          mc->mc_db->md_leaf_pages += 1;
          mdbx_cassert(mc, env->me_psize > olddata.iov_len);
          offset = env->me_psize - (unsigned)olddata.iov_len;
          flags |= F_DUPDATA | F_SUBDATA;
          nested_dupdb.md_root = mp->mp_pgno;
          nested_dupdb.md_seq = nested_dupdb.md_mod_txnid = 0;
          sub_root = mp;
        }
        if (mp != fp) {
          mp->mp_flags = fp_flags | P_DIRTY;
          mp->mp_leaf2_ksize = fp->mp_leaf2_ksize;
          mp->mp_lower = fp->mp_lower;
          mdbx_cassert(mc, fp->mp_upper + offset <= UINT16_MAX);
          mp->mp_upper = (indx_t)(fp->mp_upper + offset);
          if (unlikely(fp_flags & P_LEAF2)) {
            memcpy(page_data(mp), page_data(fp),
                   page_numkeys(fp) * fp->mp_leaf2_ksize);
          } else {
            memcpy((char *)mp + mp->mp_upper + PAGEHDRSZ,
                   (char *)fp + fp->mp_upper + PAGEHDRSZ,
                   olddata.iov_len - fp->mp_upper - PAGEHDRSZ);
            memcpy((char *)(&mp->mp_ptrs), (char *)(&fp->mp_ptrs),
                   page_numkeys(fp) * sizeof(mp->mp_ptrs[0]));
            for (i = 0; i < page_numkeys(fp); i++) {
              mdbx_cassert(mc, mp->mp_ptrs[i] + offset <= UINT16_MAX);
              mp->mp_ptrs[i] += (indx_t)offset;
            }
          }
        }

        rdata = &xdata;
        flags |= F_DUPDATA;
        do_sub = true;
        if (!insert_key)
          mdbx_node_del(mc, 0);
        goto new_sub;
      }

      /* MDBX passes F_SUBDATA in 'flags' to write a DB record */
      if (unlikely((node_flags(node) ^ flags) & F_SUBDATA))
        return MDBX_INCOMPATIBLE;

    current:
      if (data->iov_len == olddata.iov_len) {
        mdbx_cassert(mc, EVEN(key->iov_len) == EVEN(node_ks(node)));
        /* same size, just replace it. Note that we could
         * also reuse this node if the new data is smaller,
         * but instead we opt to shrink the node in that case. */
        if (F_ISSET(flags, MDBX_RESERVE))
          data->iov_base = olddata.iov_base;
        else if (!(mc->mc_flags & C_SUB))
          memcpy(olddata.iov_base, data->iov_base, data->iov_len);
        else {
          mdbx_cassert(mc, page_numkeys(mc->mc_pg[mc->mc_top]) == 1);
          mdbx_cassert(mc, PAGETYPE(mc->mc_pg[mc->mc_top]) == P_LEAF);
          mdbx_cassert(mc, node_ds(node) == 0);
          mdbx_cassert(mc, node_flags(node) == 0);
          mdbx_cassert(mc, key->iov_len < UINT16_MAX);
          node_set_ks(node, key->iov_len);
          memcpy(node_key(node), key->iov_base, key->iov_len);
          mdbx_cassert(mc, (char *)node_key(node) + node_ds(node) <
                               (char *)(mc->mc_pg[mc->mc_top]) + env->me_psize);
          goto fix_parent;
        }

        if (mdbx_audit_enabled()) {
          int err = mdbx_cursor_check(mc, false);
          if (unlikely(err != MDBX_SUCCESS))
            return err;
        }
        return MDBX_SUCCESS;
      }
    }
    mdbx_node_del(mc, 0);
  }

  rdata = data;

new_sub:
  nflags = flags & NODE_ADD_FLAGS;
  nsize = IS_LEAF2(mc->mc_pg[mc->mc_top]) ? key->iov_len
                                          : leaf_size(env, key, rdata);
  if (page_room(mc->mc_pg[mc->mc_top]) < nsize) {
    if ((flags & (F_DUPDATA | F_SUBDATA)) == F_DUPDATA)
      nflags &= ~MDBX_APPEND; /* sub-page may need room to grow */
    if (!insert_key)
      nflags |= MDBX_SPLIT_REPLACE;
    rc = mdbx_page_split(mc, key, rdata, P_INVALID, nflags);
    if (rc == MDBX_SUCCESS && mdbx_audit_enabled())
      rc = mdbx_cursor_check(mc, false);
  } else {
    /* There is room already in this leaf page. */
    if (IS_LEAF2(mc->mc_pg[mc->mc_top])) {
      mdbx_cassert(mc, (nflags & (F_BIGDATA | F_SUBDATA | F_DUPDATA)) == 0 &&
                           rdata->iov_len == 0);
      rc = mdbx_node_add_leaf2(mc, mc->mc_ki[mc->mc_top], key);
    } else
      rc = mdbx_node_add_leaf(mc, mc->mc_ki[mc->mc_top], key, rdata, nflags);
    if (likely(rc == 0)) {
      /* Adjust other cursors pointing to mp */
      MDBX_cursor *m2, *m3;
      MDBX_dbi dbi = mc->mc_dbi;
      unsigned i = mc->mc_top;
      MDBX_page *mp = mc->mc_pg[i];

      for (m2 = mc->mc_txn->mt_cursors[dbi]; m2; m2 = m2->mc_next) {
        m3 = (mc->mc_flags & C_SUB) ? &m2->mc_xcursor->mx_cursor : m2;
        if (m3 == mc || m3->mc_snum < mc->mc_snum || m3->mc_pg[i] != mp)
          continue;
        if (m3->mc_ki[i] >= mc->mc_ki[i] && insert_key) {
          m3->mc_ki[i]++;
        }
        if (XCURSOR_INITED(m3))
          XCURSOR_REFRESH(m3, mp, m3->mc_ki[i]);
      }
    }
  }

  if (likely(rc == MDBX_SUCCESS)) {
    /* Now store the actual data in the child DB. Note that we're
     * storing the user data in the keys field, so there are strict
     * size limits on dupdata. The actual data fields of the child
     * DB are all zero size. */
    if (do_sub) {
      int xflags;
      size_t ecount;
    put_sub:
      xdata.iov_len = 0;
      xdata.iov_base = nullptr;
      MDBX_node *node = page_node(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top]);
      if (flags & MDBX_CURRENT) {
        xflags = (flags & MDBX_NODUPDATA)
                     ? MDBX_CURRENT | MDBX_NOOVERWRITE | MDBX_NOSPILL
                     : MDBX_CURRENT | MDBX_NOSPILL;
      } else {
        rc2 = mdbx_xcursor_init1(mc, node);
        if (unlikely(rc2 != MDBX_SUCCESS))
          return rc2;
        xflags = (flags & MDBX_NODUPDATA) ? MDBX_NOOVERWRITE | MDBX_NOSPILL
                                          : MDBX_NOSPILL;
      }
      if (sub_root)
        mc->mc_xcursor->mx_cursor.mc_pg[0] = sub_root;
      /* converted, write the original data first */
      if (dupdata_flag) {
        rc = mdbx_cursor_put(&mc->mc_xcursor->mx_cursor, &dkey, &xdata, xflags);
        if (unlikely(rc))
          goto bad_sub;
        /* we've done our job */
        dkey.iov_len = 0;
      }
      if (!(node_flags(node) & F_SUBDATA) || sub_root) {
        /* Adjust other cursors pointing to mp */
        MDBX_cursor *m2;
        MDBX_xcursor *mx = mc->mc_xcursor;
        unsigned i = mc->mc_top;
        MDBX_page *mp = mc->mc_pg[i];
        const int nkeys = page_numkeys(mp);

        for (m2 = mc->mc_txn->mt_cursors[mc->mc_dbi]; m2; m2 = m2->mc_next) {
          if (m2 == mc || m2->mc_snum < mc->mc_snum)
            continue;
          if (!(m2->mc_flags & C_INITIALIZED))
            continue;
          if (m2->mc_pg[i] == mp) {
            if (m2->mc_ki[i] == mc->mc_ki[i]) {
              rc2 = mdbx_xcursor_init2(m2, mx, dupdata_flag);
              if (unlikely(rc2 != MDBX_SUCCESS))
                return rc2;
            } else if (!insert_key && m2->mc_ki[i] < nkeys) {
              XCURSOR_REFRESH(m2, mp, m2->mc_ki[i]);
            }
          }
        }
      }
      mdbx_cassert(mc, mc->mc_xcursor->mx_db.md_entries < PTRDIFF_MAX);
      ecount = (size_t)mc->mc_xcursor->mx_db.md_entries;
      if (flags & MDBX_APPENDDUP)
        xflags |= MDBX_APPEND;
      rc = mdbx_cursor_put(&mc->mc_xcursor->mx_cursor, data, &xdata, xflags);
      if (flags & F_SUBDATA) {
        void *db = node_data(node);
        memcpy(db, &mc->mc_xcursor->mx_db, sizeof(MDBX_db));
      }
      insert_data = (ecount != (size_t)mc->mc_xcursor->mx_db.md_entries);
    }
    /* Increment count unless we just replaced an existing item. */
    if (insert_data)
      mc->mc_db->md_entries++;
    if (insert_key) {
      /* Invalidate txn if we created an empty sub-DB */
      if (unlikely(rc))
        goto bad_sub;
      /* If we succeeded and the key didn't exist before,
       * make sure the cursor is marked valid. */
      mc->mc_flags |= C_INITIALIZED;
    }
    if (flags & MDBX_MULTIPLE) {
      if (!rc) {
        mcount++;
        /* let caller know how many succeeded, if any */
        data[1].iov_len = mcount;
        if (mcount < dcount) {
          data[0].iov_base = (char *)data[0].iov_base + data[0].iov_len;
          insert_key = insert_data = false;
          goto more;
        }
      }
    }
    if (rc == MDBX_SUCCESS && mdbx_audit_enabled())
      rc = mdbx_cursor_check(mc, false);
    return rc;
  bad_sub:
    if (unlikely(rc == MDBX_KEYEXIST))
      mdbx_error("unexpected %s", "MDBX_KEYEXIST");
    /* should not happen, we deleted that item */
    rc = MDBX_PROBLEM;
  }
fail:
  mc->mc_txn->mt_flags |= MDBX_TXN_ERROR;
  return rc;
}

int mdbx_cursor_del(MDBX_cursor *mc, unsigned flags) {
  if (unlikely(!mc))
    return MDBX_EINVAL;

  if (unlikely(mc->mc_signature != MDBX_MC_SIGNATURE))
    return MDBX_EBADSIGN;

  int rc = check_txn_rw(mc->mc_txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!(mc->mc_flags & C_INITIALIZED)))
    return MDBX_EINVAL;

  if (unlikely(mc->mc_ki[mc->mc_top] >= page_numkeys(mc->mc_pg[mc->mc_top])))
    return MDBX_NOTFOUND;

  if (unlikely(!(flags & MDBX_NOSPILL) &&
               (rc = mdbx_page_spill(mc, NULL, NULL))))
    return rc;

  rc = mdbx_cursor_touch(mc);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  MDBX_page *mp = mc->mc_pg[mc->mc_top];
  if (IS_LEAF2(mp))
    goto del_key;

  MDBX_node *node = page_node(mp, mc->mc_ki[mc->mc_top]);
  if (F_ISSET(node_flags(node), F_DUPDATA)) {
    if (flags & MDBX_NODUPDATA) {
      /* mdbx_cursor_del0() will subtract the final entry */
      mc->mc_db->md_entries -= mc->mc_xcursor->mx_db.md_entries - 1;
      mc->mc_xcursor->mx_cursor.mc_flags &= ~C_INITIALIZED;
    } else {
      if (!F_ISSET(node_flags(node), F_SUBDATA)) {
        mc->mc_xcursor->mx_cursor.mc_pg[0] = node_data(node);
      }
      rc = mdbx_cursor_del(&mc->mc_xcursor->mx_cursor, MDBX_NOSPILL);
      if (unlikely(rc))
        return rc;
      /* If sub-DB still has entries, we're done */
      if (mc->mc_xcursor->mx_db.md_entries) {
        if (node_flags(node) & F_SUBDATA) {
          /* update subDB info */
          void *db = node_data(node);
          memcpy(db, &mc->mc_xcursor->mx_db, sizeof(MDBX_db));
        } else {
          MDBX_cursor *m2;
          /* shrink fake page */
          mdbx_node_shrink(mp, mc->mc_ki[mc->mc_top]);
          node = page_node(mp, mc->mc_ki[mc->mc_top]);
          mc->mc_xcursor->mx_cursor.mc_pg[0] = node_data(node);
          /* fix other sub-DB cursors pointed at fake pages on this page */
          for (m2 = mc->mc_txn->mt_cursors[mc->mc_dbi]; m2; m2 = m2->mc_next) {
            if (m2 == mc || m2->mc_snum < mc->mc_snum)
              continue;
            if (!(m2->mc_flags & C_INITIALIZED))
              continue;
            if (m2->mc_pg[mc->mc_top] == mp) {
              MDBX_node *inner = node;
              if (m2->mc_ki[mc->mc_top] >= page_numkeys(mp))
                continue;
              if (m2->mc_ki[mc->mc_top] != mc->mc_ki[mc->mc_top]) {
                inner = page_node(mp, m2->mc_ki[mc->mc_top]);
                if (node_flags(inner) & F_SUBDATA)
                  continue;
              }
              m2->mc_xcursor->mx_cursor.mc_pg[0] = node_data(inner);
            }
          }
        }
        mc->mc_db->md_entries--;
        mdbx_cassert(mc, mc->mc_db->md_entries > 0 && mc->mc_db->md_depth > 0 &&
                             mc->mc_db->md_root != P_INVALID);
        return rc;
      } else {
        mc->mc_xcursor->mx_cursor.mc_flags &= ~C_INITIALIZED;
      }
      /* otherwise fall thru and delete the sub-DB */
    }

    if (node_flags(node) & F_SUBDATA) {
      /* add all the child DB's pages to the free list */
      rc = mdbx_drop0(&mc->mc_xcursor->mx_cursor, 0);
      if (unlikely(rc))
        goto fail;
    }
  }
  /* MDBX passes F_SUBDATA in 'flags' to delete a DB record */
  else if (unlikely((node_flags(node) ^ flags) & F_SUBDATA)) {
    rc = MDBX_INCOMPATIBLE;
    goto fail;
  }

  /* add overflow pages to free list */
  if (F_ISSET(node_flags(node), F_BIGDATA)) {
    MDBX_page *omp;
    if (unlikely(
            (rc = mdbx_page_get(mc, node_largedata_pgno(node), &omp, NULL)) ||
            (rc = mdbx_page_retire(mc, omp))))
      goto fail;
  }

del_key:
  return mdbx_cursor_del0(mc);

fail:
  mc->mc_txn->mt_flags |= MDBX_TXN_ERROR;
  return rc;
}

/* Allocate and initialize new pages for a database.
 * Set MDBX_TXN_ERROR on failure.
 *
 * [in] mc a  cursor on the database being added to.
 * [in] flags flags defining what type of page is being allocated.
 * [in] num   the number of pages to allocate. This is usually 1,
 *            unless allocating overflow pages for a large record.
 * [out] mp   Address of a page, or NULL on failure.
 *
 * Returns 0 on success, non-zero on failure. */
static int mdbx_page_new(MDBX_cursor *mc, unsigned flags, unsigned num,
                         MDBX_page **mp) {
  MDBX_page *np;
  int rc;

  if (unlikely((rc = mdbx_page_alloc(mc, num, &np, MDBX_ALLOC_ALL))))
    return rc;
  *mp = np;
  mdbx_debug("allocated new page #%" PRIaPGNO ", size %u", np->mp_pgno,
             mc->mc_txn->mt_env->me_psize);
  np->mp_flags = (uint16_t)(flags | P_DIRTY);
  np->mp_lower = 0;
  np->mp_upper = (indx_t)(mc->mc_txn->mt_env->me_psize - PAGEHDRSZ);

  mc->mc_db->md_branch_pages += IS_BRANCH(np);
  mc->mc_db->md_leaf_pages += IS_LEAF(np);
  if (unlikely(IS_OVERFLOW(np))) {
    mc->mc_db->md_overflow_pages += num;
    np->mp_pages = num;
    mdbx_cassert(mc, !(mc->mc_flags & C_SUB));
  } else if (unlikely(mc->mc_flags & C_SUB)) {
    MDBX_db *outer = mdbx_outer_db(mc);
    outer->md_branch_pages += IS_BRANCH(np);
    outer->md_leaf_pages += IS_LEAF(np);
  }

  return MDBX_SUCCESS;
}

static int __must_check_result mdbx_node_add_leaf2(MDBX_cursor *mc,
                                                   unsigned indx,
                                                   const MDBX_val *key) {
  MDBX_page *mp = mc->mc_pg[mc->mc_top];
  DKBUF;
  mdbx_debug("add to leaf2-%spage %" PRIaPGNO " index %i, "
             " key size %" PRIuPTR " [%s]",
             IS_SUBP(mp) ? "sub-" : "", mp->mp_pgno, indx,
             key ? key->iov_len : 0, DKEY(key));

  mdbx_cassert(mc, key);
  mdbx_cassert(mc, PAGETYPE(mp) == (P_LEAF | P_LEAF2));
  const unsigned ksize = mc->mc_db->md_xsize;
  mdbx_cassert(mc, ksize == key->iov_len);
  const unsigned nkeys = page_numkeys(mp);

  /* Just using these for counting */
  const intptr_t lower = mp->mp_lower + sizeof(indx_t);
  const intptr_t upper = mp->mp_upper - (ksize - sizeof(indx_t));
  if (unlikely(lower > upper)) {
    mc->mc_txn->mt_flags |= MDBX_TXN_ERROR;
    return MDBX_PAGE_FULL;
  }
  mp->mp_lower = (indx_t)lower;
  mp->mp_upper = (indx_t)upper;

  char *const ptr = page_leaf2key(mp, indx, ksize);
  mdbx_cassert(mc, nkeys >= indx);
  const unsigned diff = nkeys - indx;
  if (likely(diff > 0))
    /* Move higher keys up one slot. */
    memmove(ptr + ksize, ptr, diff * ksize);
  /* insert new key */
  memcpy(ptr, key->iov_base, ksize);
  return MDBX_SUCCESS;
}

static int __must_check_result mdbx_node_add_branch(MDBX_cursor *mc,
                                                    unsigned indx,
                                                    const MDBX_val *key,
                                                    pgno_t pgno) {
  MDBX_page *mp = mc->mc_pg[mc->mc_top];
  DKBUF;
  mdbx_debug("add to branch-%spage %" PRIaPGNO " index %i, node-pgno %" PRIaPGNO
             " key size %" PRIuPTR " [%s]",
             IS_SUBP(mp) ? "sub-" : "", mp->mp_pgno, indx, pgno,
             key ? key->iov_len : 0, DKEY(key));

  mdbx_cassert(mc, PAGETYPE(mp) == P_BRANCH);
  STATIC_ASSERT(NODESIZE % 2 == 0);

  /* Move higher pointers up one slot. */
  const unsigned nkeys = page_numkeys(mp);
  mdbx_cassert(mc, nkeys >= indx);
  for (unsigned i = nkeys; i > indx; --i)
    mp->mp_ptrs[i] = mp->mp_ptrs[i - 1];

  /* Adjust free space offsets. */
  const size_t branch_bytes = branch_size(mc->mc_txn->mt_env, key);
  const intptr_t lower = mp->mp_lower + sizeof(indx_t);
  const intptr_t upper = mp->mp_upper - (branch_bytes - sizeof(indx_t));
  if (unlikely(lower > upper)) {
    mc->mc_txn->mt_flags |= MDBX_TXN_ERROR;
    return MDBX_PAGE_FULL;
  }
  mp->mp_lower = (indx_t)lower;
  mp->mp_ptrs[indx] = mp->mp_upper = (indx_t)upper;

  /* Write the node data. */
  MDBX_node *node = page_node(mp, indx);
  node_set_pgno(node, pgno);
  node_set_flags(node, 0);
  UNALIGNED_POKE_8(node, MDBX_node, mn_extra, 0);
  node_set_ks(node, 0);
  if (likely(key != NULL)) {
    node_set_ks(node, key->iov_len);
    memcpy(node_key(node), key->iov_base, key->iov_len);
  }
  return MDBX_SUCCESS;
}

static int __must_check_result mdbx_node_add_leaf(MDBX_cursor *mc,
                                                  unsigned indx,
                                                  const MDBX_val *key,
                                                  MDBX_val *data,
                                                  unsigned flags) {
  MDBX_page *mp = mc->mc_pg[mc->mc_top];
  DKBUF;
  mdbx_debug("add to leaf-%spage %" PRIaPGNO " index %i, data size %" PRIuPTR
             " key size %" PRIuPTR " [%s]",
             IS_SUBP(mp) ? "sub-" : "", mp->mp_pgno, indx,
             data ? data->iov_len : 0, key ? key->iov_len : 0, DKEY(key));
  mdbx_cassert(mc, key != NULL && data != NULL);
  mdbx_cassert(mc, PAGETYPE(mp) == P_LEAF);
  MDBX_page *largepage = NULL;

  size_t leaf_bytes = 0;
  if (unlikely(flags & F_BIGDATA)) {
    /* Data already on overflow page. */
    STATIC_ASSERT(sizeof(pgno_t) % 2 == 0);
    leaf_bytes = node_size(key, nullptr) + sizeof(pgno_t) + sizeof(indx_t);
  } else if (unlikely(node_size(key, data) > mc->mc_txn->mt_env->me_nodemax)) {
    /* Put data on overflow page. */
    mdbx_cassert(mc, !F_ISSET(mc->mc_db->md_flags, MDBX_DUPSORT));
    const pgno_t ovpages = number_of_ovpages(mc->mc_txn->mt_env, data->iov_len);
    int rc = mdbx_page_new(mc, P_OVERFLOW, ovpages, &largepage);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    mdbx_debug("allocated %u overflow page(s) %" PRIaPGNO "for %" PRIuPTR
               " data bytes",
               largepage->mp_pages, largepage->mp_pgno, data->iov_len);
    flags |= F_BIGDATA;
    leaf_bytes = node_size(key, nullptr) + sizeof(pgno_t) + sizeof(indx_t);
  } else {
    leaf_bytes = node_size(key, data) + sizeof(indx_t);
  }
  mdbx_cassert(mc, leaf_bytes == leaf_size(mc->mc_txn->mt_env, key, data));

  /* Move higher pointers up one slot. */
  const unsigned nkeys = page_numkeys(mp);
  mdbx_cassert(mc, nkeys >= indx);
  for (unsigned i = nkeys; i > indx; --i)
    mp->mp_ptrs[i] = mp->mp_ptrs[i - 1];

  /* Adjust free space offsets. */
  const intptr_t lower = mp->mp_lower + sizeof(indx_t);
  const intptr_t upper = mp->mp_upper - (leaf_bytes - sizeof(indx_t));
  if (unlikely(lower > upper)) {
    mc->mc_txn->mt_flags |= MDBX_TXN_ERROR;
    return MDBX_PAGE_FULL;
  }
  mp->mp_lower = (indx_t)lower;
  mp->mp_ptrs[indx] = mp->mp_upper = (indx_t)upper;

  /* Write the node data. */
  MDBX_node *node = page_node(mp, indx);
  node_set_ks(node, key->iov_len);
  node_set_flags(node, (uint8_t)flags);
  UNALIGNED_POKE_8(node, MDBX_node, mn_extra, 0);
  node_set_ds(node, data->iov_len);
  memcpy(node_key(node), key->iov_base, key->iov_len);

  void *nodedata = node_data(node);
  if (likely(largepage == NULL)) {
    if (unlikely(flags & F_BIGDATA))
      memcpy(nodedata, data->iov_base, sizeof(pgno_t));
    else if (unlikely(flags & MDBX_RESERVE))
      data->iov_base = nodedata;
    else if (likely(nodedata != data->iov_base))
      memcpy(nodedata, data->iov_base, data->iov_len);
  } else {
    poke_pgno(nodedata, largepage->mp_pgno);
    nodedata = page_data(largepage);
    if (unlikely(flags & MDBX_RESERVE))
      data->iov_base = nodedata;
    else if (likely(nodedata != data->iov_base))
      memcpy(nodedata, data->iov_base, data->iov_len);
  }
  return MDBX_SUCCESS;
}

/* Delete the specified node from a page.
 * [in] mc Cursor pointing to the node to delete.
 * [in] ksize The size of a node. Only used if the page is
 * part of a MDBX_DUPFIXED database. */
static void mdbx_node_del(MDBX_cursor *mc, size_t ksize) {
  MDBX_page *mp = mc->mc_pg[mc->mc_top];
  int indx = mc->mc_ki[mc->mc_top];
  int i, j, nkeys, ptr;
  MDBX_node *node;
  char *base;

  mdbx_debug("delete node %u on %s page %" PRIaPGNO, indx,
             IS_LEAF(mp) ? "leaf" : "branch", mp->mp_pgno);
  nkeys = page_numkeys(mp);
  mdbx_cassert(mc, indx < nkeys);

  if (IS_LEAF2(mp)) {
    mdbx_cassert(mc, ksize >= sizeof(indx_t));
    unsigned diff = nkeys - 1 - indx;
    base = page_leaf2key(mp, indx, ksize);
    if (diff)
      memmove(base, base + ksize, diff * ksize);
    mdbx_cassert(mc, mp->mp_lower >= sizeof(indx_t));
    mp->mp_lower -= sizeof(indx_t);
    mdbx_cassert(mc,
                 (size_t)UINT16_MAX - mp->mp_upper >= ksize - sizeof(indx_t));
    mp->mp_upper += (indx_t)(ksize - sizeof(indx_t));
    return;
  }

  node = page_node(mp, indx);
  size_t sz = NODESIZE + node_ks(node);
  if (IS_LEAF(mp)) {
    if (F_ISSET(node_flags(node), F_BIGDATA))
      sz += sizeof(pgno_t);
    else
      sz += node_ds(node);
  }
  sz = EVEN(sz);

  ptr = mp->mp_ptrs[indx];
  for (i = j = 0; i < nkeys; i++) {
    if (i != indx) {
      mp->mp_ptrs[j] = mp->mp_ptrs[i];
      if (mp->mp_ptrs[i] < ptr) {
        mdbx_cassert(mc, (size_t)UINT16_MAX - mp->mp_ptrs[j] >= sz);
        mp->mp_ptrs[j] += (indx_t)sz;
      }
      j++;
    }
  }

  base = (char *)mp + mp->mp_upper + PAGEHDRSZ;
  memmove(base + sz, base, ptr - mp->mp_upper);

  mdbx_cassert(mc, mp->mp_lower >= sizeof(indx_t));
  mp->mp_lower -= sizeof(indx_t);
  mdbx_cassert(mc, (size_t)UINT16_MAX - mp->mp_upper >= sz);
  mp->mp_upper += (indx_t)sz;
}

/* Compact the main page after deleting a node on a subpage.
 * [in] mp The main page to operate on.
 * [in] indx The index of the subpage on the main page. */
static void mdbx_node_shrink(MDBX_page *mp, unsigned indx) {
  MDBX_node *node;
  MDBX_page *sp, *xp;
  char *base;
  size_t nsize, delta, len, ptr;
  int i;

  node = page_node(mp, indx);
  sp = (MDBX_page *)node_data(node);
  delta = page_room(sp);
  assert(delta > 0);

  /* Prepare to shift upward, set len = length(subpage part to shift) */
  if (IS_LEAF2(sp)) {
    delta &= /* do not make the node uneven-sized */ ~1u;
    if (unlikely(delta) == 0)
      return;
    nsize = node_ds(node) - delta;
    assert(nsize % 1 == 0);
    len = nsize;
  } else {
    xp = (MDBX_page *)((char *)sp + delta); /* destination subpage */
    for (i = page_numkeys(sp); --i >= 0;) {
      assert(sp->mp_ptrs[i] >= delta);
      xp->mp_ptrs[i] = (indx_t)(sp->mp_ptrs[i] - delta);
    }
    nsize = node_ds(node) - delta;
    len = PAGEHDRSZ;
  }
  sp->mp_upper = sp->mp_lower;
  sp->mp_pgno = mp->mp_pgno;
  node_set_ds(node, nsize);

  /* Shift <lower nodes...initial part of subpage> upward */
  base = (char *)mp + mp->mp_upper + PAGEHDRSZ;
  memmove(base + delta, base, (char *)sp + len - base);

  ptr = mp->mp_ptrs[indx];
  for (i = page_numkeys(mp); --i >= 0;) {
    if (mp->mp_ptrs[i] <= ptr) {
      assert((size_t)UINT16_MAX - mp->mp_ptrs[i] >= delta);
      mp->mp_ptrs[i] += (indx_t)delta;
    }
  }
  assert((size_t)UINT16_MAX - mp->mp_upper >= delta);
  mp->mp_upper += (indx_t)delta;
}

/* Initial setup of a sorted-dups cursor.
 *
 * Sorted duplicates are implemented as a sub-database for the given key.
 * The duplicate data items are actually keys of the sub-database.
 * Operations on the duplicate data items are performed using a sub-cursor
 * initialized when the sub-database is first accessed. This function does
 * the preliminary setup of the sub-cursor, filling in the fields that
 * depend only on the parent DB.
 *
 * [in] mc The main cursor whose sorted-dups cursor is to be initialized. */
static int mdbx_xcursor_init0(MDBX_cursor *mc) {
  MDBX_xcursor *mx = mc->mc_xcursor;
  if (unlikely(mx == nullptr))
    return MDBX_CORRUPTED;

  mx->mx_cursor.mc_xcursor = NULL;
  mx->mx_cursor.mc_txn = mc->mc_txn;
  mx->mx_cursor.mc_db = &mx->mx_db;
  mx->mx_cursor.mc_dbx = &mx->mx_dbx;
  mx->mx_cursor.mc_dbi = mc->mc_dbi;
  mx->mx_cursor.mc_dbflag = &mx->mx_dbflag;
  mx->mx_cursor.mc_snum = 0;
  mx->mx_cursor.mc_top = 0;
  mx->mx_cursor.mc_flags = C_SUB;
  mx->mx_dbx.md_name.iov_len = 0;
  mx->mx_dbx.md_name.iov_base = NULL;
  mx->mx_dbx.md_cmp = mc->mc_dbx->md_dcmp;
  mx->mx_dbx.md_dcmp = NULL;
  return MDBX_SUCCESS;
}

/* Final setup of a sorted-dups cursor.
 * Sets up the fields that depend on the data from the main cursor.
 * [in] mc The main cursor whose sorted-dups cursor is to be initialized.
 * [in] node The data containing the MDBX_db record for the sorted-dup database.
 */
static int mdbx_xcursor_init1(MDBX_cursor *mc, MDBX_node *node) {
  MDBX_xcursor *mx = mc->mc_xcursor;
  if (unlikely(mx == nullptr))
    return MDBX_CORRUPTED;

  if (node_flags(node) & F_SUBDATA) {
    if (unlikely(node_ds(node) != sizeof(MDBX_db)))
      return MDBX_CORRUPTED;
    memcpy(&mx->mx_db, node_data(node), sizeof(MDBX_db));
    mx->mx_cursor.mc_pg[0] = 0;
    mx->mx_cursor.mc_snum = 0;
    mx->mx_cursor.mc_top = 0;
    mx->mx_cursor.mc_flags = C_SUB;
  } else {
    if (unlikely(node_ds(node) <= PAGEHDRSZ))
      return MDBX_CORRUPTED;
    MDBX_page *fp = node_data(node);
    mx->mx_db.md_xsize = 0;
    mx->mx_db.md_flags = 0;
    mx->mx_db.md_depth = 1;
    mx->mx_db.md_branch_pages = 0;
    mx->mx_db.md_leaf_pages = 1;
    mx->mx_db.md_overflow_pages = 0;
    mx->mx_db.md_entries = page_numkeys(fp);
    mx->mx_db.md_root = fp->mp_pgno;
    mx->mx_cursor.mc_snum = 1;
    mx->mx_cursor.mc_top = 0;
    mx->mx_cursor.mc_flags = C_INITIALIZED | C_SUB;
    mx->mx_cursor.mc_pg[0] = fp;
    mx->mx_cursor.mc_ki[0] = 0;
    if (mc->mc_db->md_flags & MDBX_DUPFIXED) {
      mx->mx_db.md_flags = MDBX_DUPFIXED;
      mx->mx_db.md_xsize = fp->mp_leaf2_ksize;
      if (mc->mc_db->md_flags & MDBX_INTEGERDUP)
        mx->mx_db.md_flags |= MDBX_INTEGERKEY;
    }
  }
  mdbx_debug("Sub-db -%u root page %" PRIaPGNO, mx->mx_cursor.mc_dbi,
             mx->mx_db.md_root);
  mx->mx_dbflag = DB_VALID | DB_USRVALID | DB_DUPDATA;
  return MDBX_SUCCESS;
}

/* Fixup a sorted-dups cursor due to underlying update.
 * Sets up some fields that depend on the data from the main cursor.
 * Almost the same as init1, but skips initialization steps if the
 * xcursor had already been used.
 * [in] mc The main cursor whose sorted-dups cursor is to be fixed up.
 * [in] src_mx The xcursor of an up-to-date cursor.
 * [in] new_dupdata True if converting from a non-F_DUPDATA item. */
static int mdbx_xcursor_init2(MDBX_cursor *mc, MDBX_xcursor *src_mx,
                              int new_dupdata) {
  MDBX_xcursor *mx = mc->mc_xcursor;
  if (unlikely(mx == nullptr))
    return MDBX_CORRUPTED;

  if (new_dupdata) {
    mx->mx_cursor.mc_snum = 1;
    mx->mx_cursor.mc_top = 0;
    mx->mx_cursor.mc_flags |= C_INITIALIZED;
    mx->mx_cursor.mc_ki[0] = 0;
    mx->mx_dbflag = DB_VALID | DB_USRVALID | DB_DUPDATA;
    mx->mx_dbx.md_cmp = src_mx->mx_dbx.md_cmp;
  } else if (!(mx->mx_cursor.mc_flags & C_INITIALIZED)) {
    return MDBX_SUCCESS;
  }
  mx->mx_db = src_mx->mx_db;
  mx->mx_cursor.mc_pg[0] = src_mx->mx_cursor.mc_pg[0];
  mdbx_debug("Sub-db -%u root page %" PRIaPGNO, mx->mx_cursor.mc_dbi,
             mx->mx_db.md_root);
  return MDBX_SUCCESS;
}

/* Initialize a cursor for a given transaction and database. */
static int mdbx_cursor_init(MDBX_cursor *mc, MDBX_txn *txn, MDBX_dbi dbi) {
  mc->mc_signature = MDBX_MC_SIGNATURE;
  mc->mc_next = NULL;
  mc->mc_backup = NULL;
  mc->mc_dbi = dbi;
  mc->mc_txn = txn;
  mc->mc_db = &txn->mt_dbs[dbi];
  mc->mc_dbx = &txn->mt_dbxs[dbi];
  mc->mc_dbflag = &txn->mt_dbflags[dbi];
  mc->mc_snum = 0;
  mc->mc_top = 0;
  mc->mc_pg[0] = 0;
  mc->mc_flags = 0;
  mc->mc_ki[0] = 0;
  mc->mc_xcursor = NULL;

  if (txn->mt_dbs[dbi].md_flags & MDBX_DUPSORT) {
    STATIC_ASSERT(offsetof(MDBX_cursor_couple, outer) == 0);
    MDBX_xcursor *mx = &container_of(mc, MDBX_cursor_couple, outer)->inner;
    mdbx_tassert(txn, mx != NULL);
    mx->mx_cursor.mc_signature = MDBX_MC_SIGNATURE;
    mc->mc_xcursor = mx;
    int rc = mdbx_xcursor_init0(mc);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }

  int rc = MDBX_SUCCESS;
  if (unlikely(*mc->mc_dbflag & DB_STALE)) {
    rc = mdbx_page_search(mc, NULL, MDBX_PS_ROOTONLY);
    rc = (rc != MDBX_NOTFOUND) ? rc : MDBX_SUCCESS;
  }
  return rc;
}

int mdbx_cursor_open(MDBX_txn *txn, MDBX_dbi dbi, MDBX_cursor **ret) {
  if (unlikely(!ret))
    return MDBX_EINVAL;
  *ret = NULL;

  int rc = check_txn(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!TXN_DBI_EXIST(txn, dbi, DB_VALID)))
    return MDBX_EINVAL;

  if (unlikely(dbi == FREE_DBI && !F_ISSET(txn->mt_flags, MDBX_RDONLY)))
    return MDBX_EACCESS;

  const size_t size = (txn->mt_dbs[dbi].md_flags & MDBX_DUPSORT)
                          ? sizeof(MDBX_cursor_couple)
                          : sizeof(MDBX_cursor);

  MDBX_cursor *mc;
  if (likely((mc = mdbx_malloc(size)) != NULL)) {
    rc = mdbx_cursor_init(mc, txn, dbi);
    if (unlikely(rc != MDBX_SUCCESS)) {
      mdbx_free(mc);
      return rc;
    }
    if (txn->mt_cursors) {
      mc->mc_next = txn->mt_cursors[dbi];
      txn->mt_cursors[dbi] = mc;
      mc->mc_flags |= C_UNTRACK;
    }
  } else {
    return MDBX_ENOMEM;
  }

  *ret = mc;
  return MDBX_SUCCESS;
}

int mdbx_cursor_renew(MDBX_txn *txn, MDBX_cursor *mc) {
  if (unlikely(!mc))
    return MDBX_EINVAL;

  if (unlikely(mc->mc_signature != MDBX_MC_SIGNATURE &&
               mc->mc_signature != MDBX_MC_READY4CLOSE))
    return MDBX_EINVAL;

  int rc = check_txn(mc->mc_txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!TXN_DBI_EXIST(txn, mc->mc_dbi, DB_VALID)))
    return MDBX_EINVAL;

  if (unlikely(mc->mc_backup))
    return MDBX_EINVAL;

  if (unlikely((mc->mc_flags & C_UNTRACK) || txn->mt_cursors)) {
    MDBX_cursor **prev = &mc->mc_txn->mt_cursors[mc->mc_dbi];
    while (*prev && *prev != mc)
      prev = &(*prev)->mc_next;
    if (*prev == mc)
      *prev = mc->mc_next;
    mc->mc_signature = MDBX_MC_READY4CLOSE;
  }

  if (unlikely(txn->mt_flags & MDBX_TXN_BLOCKED))
    return MDBX_BAD_TXN;

  return mdbx_cursor_init(mc, txn, mc->mc_dbi);
}

/* Return the count of duplicate data items for the current key */
int mdbx_cursor_count(MDBX_cursor *mc, size_t *countp) {
  if (unlikely(mc == NULL))
    return MDBX_EINVAL;

  if (unlikely(mc->mc_signature != MDBX_MC_SIGNATURE))
    return MDBX_EBADSIGN;

  int rc = check_txn(mc->mc_txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(countp == NULL || !(mc->mc_flags & C_INITIALIZED)))
    return MDBX_EINVAL;

  if (!mc->mc_snum) {
    *countp = 0;
    return MDBX_NOTFOUND;
  }

  MDBX_page *mp = mc->mc_pg[mc->mc_top];
  if ((mc->mc_flags & C_EOF) && mc->mc_ki[mc->mc_top] >= page_numkeys(mp)) {
    *countp = 0;
    return MDBX_NOTFOUND;
  }

  *countp = 1;
  if (mc->mc_xcursor != NULL) {
    MDBX_node *node = page_node(mp, mc->mc_ki[mc->mc_top]);
    if (F_ISSET(node_flags(node), F_DUPDATA)) {
      mdbx_cassert(mc, mc->mc_xcursor && (mc->mc_xcursor->mx_cursor.mc_flags &
                                          C_INITIALIZED));
      *countp = unlikely(mc->mc_xcursor->mx_db.md_entries > PTRDIFF_MAX)
                    ? PTRDIFF_MAX
                    : (size_t)mc->mc_xcursor->mx_db.md_entries;
    }
  }
  return MDBX_SUCCESS;
}

void mdbx_cursor_close(MDBX_cursor *mc) {
  if (mc) {
    mdbx_ensure(NULL, mc->mc_signature == MDBX_MC_SIGNATURE ||
                          mc->mc_signature == MDBX_MC_READY4CLOSE);
    if (!mc->mc_backup) {
      /* Remove from txn, if tracked.
       * A read-only txn (!C_UNTRACK) may have been freed already,
       * so do not peek inside it.  Only write txns track cursors. */
      if ((mc->mc_flags & C_UNTRACK) && mc->mc_txn->mt_cursors) {
        MDBX_cursor **prev = &mc->mc_txn->mt_cursors[mc->mc_dbi];
        while (*prev && *prev != mc)
          prev = &(*prev)->mc_next;
        if (*prev == mc)
          *prev = mc->mc_next;
      }
      mc->mc_signature = 0;
      mdbx_free(mc);
    } else {
      /* cursor closed before nested txn ends */
      mdbx_cassert(mc, mc->mc_signature == MDBX_MC_SIGNATURE);
      mc->mc_signature = MDBX_MC_WAIT4EOT;
    }
  }
}

MDBX_txn *mdbx_cursor_txn(MDBX_cursor *mc) {
  if (unlikely(!mc || mc->mc_signature != MDBX_MC_SIGNATURE))
    return NULL;
  MDBX_txn *txn = mc->mc_txn;
  if (unlikely(!txn || txn->mt_signature != MDBX_MT_SIGNATURE))
    return NULL;
  if (unlikely(txn->mt_flags & MDBX_TXN_FINISHED))
    return NULL;
  return txn;
}

MDBX_dbi mdbx_cursor_dbi(MDBX_cursor *mc) {
  if (unlikely(!mc || mc->mc_signature != MDBX_MC_SIGNATURE))
    return UINT_MAX;
  return mc->mc_dbi;
}

/* Replace the key for a branch node with a new key.
 * Set MDBX_TXN_ERROR on failure.
 * [in] mc Cursor pointing to the node to operate on.
 * [in] key The new key to use.
 * Returns 0 on success, non-zero on failure. */
static int mdbx_update_key(MDBX_cursor *mc, const MDBX_val *key) {
  MDBX_page *mp;
  MDBX_node *node;
  char *base;
  size_t len;
  int delta, ksize, oksize;
  int ptr, i, nkeys, indx;
  DKBUF;

  indx = mc->mc_ki[mc->mc_top];
  mp = mc->mc_pg[mc->mc_top];
  node = page_node(mp, indx);
  ptr = mp->mp_ptrs[indx];
  if (MDBX_DEBUG) {
    MDBX_val k2;
    char kbuf2[DKBUF_MAXKEYSIZE * 2 + 1];
    k2.iov_base = node_key(node);
    k2.iov_len = node_ks(node);
    mdbx_debug("update key %u (ofs %u) [%s] to [%s] on page %" PRIaPGNO, indx,
               ptr, mdbx_dump_val(&k2, kbuf2, sizeof(kbuf2)), DKEY(key),
               mp->mp_pgno);
  }

  /* Sizes must be 2-byte aligned. */
  ksize = EVEN(key->iov_len);
  oksize = EVEN(node_ks(node));
  delta = ksize - oksize;

  /* Shift node contents if EVEN(key length) changed. */
  if (delta) {
    if (delta > (int)page_room(mp)) {
      /* not enough space left, do a delete and split */
      mdbx_debug("Not enough room, delta = %d, splitting...", delta);
      pgno_t pgno = node_pgno(node);
      mdbx_node_del(mc, 0);
      int rc = mdbx_page_split(mc, key, NULL, pgno, MDBX_SPLIT_REPLACE);
      if (rc == MDBX_SUCCESS && mdbx_audit_enabled())
        rc = mdbx_cursor_check(mc, true);
      return rc;
    }

    nkeys = page_numkeys(mp);
    for (i = 0; i < nkeys; i++) {
      if (mp->mp_ptrs[i] <= ptr) {
        mdbx_cassert(mc, mp->mp_ptrs[i] >= delta);
        mp->mp_ptrs[i] -= (indx_t)delta;
      }
    }

    base = (char *)mp + mp->mp_upper + PAGEHDRSZ;
    len = ptr - mp->mp_upper + NODESIZE;
    memmove(base - delta, base, len);
    mdbx_cassert(mc, mp->mp_upper >= delta);
    mp->mp_upper -= (indx_t)delta;

    node = page_node(mp, indx);
  }

  /* But even if no shift was needed, update ksize */
  node_set_ks(node, key->iov_len);

  if (key->iov_len)
    memcpy(node_key(node), key->iov_base, key->iov_len);
  return MDBX_SUCCESS;
}

/* Move a node from csrc to cdst. */
static int mdbx_node_move(MDBX_cursor *csrc, MDBX_cursor *cdst, int fromleft) {
  int rc;
  DKBUF;

  MDBX_page *psrc = csrc->mc_pg[csrc->mc_top];
  MDBX_page *pdst = cdst->mc_pg[cdst->mc_top];
  mdbx_cassert(csrc, PAGETYPE(psrc) == PAGETYPE(pdst));
  mdbx_cassert(csrc, csrc->mc_dbi == cdst->mc_dbi);
  mdbx_cassert(csrc, csrc->mc_top == cdst->mc_top);
  if (unlikely(PAGETYPE(psrc) != PAGETYPE(pdst))) {
  bailout:
    csrc->mc_txn->mt_flags |= MDBX_TXN_ERROR;
    return MDBX_PROBLEM;
  }

  MDBX_val key4move;
  switch (PAGETYPE(psrc)) {
  case P_BRANCH: {
    const MDBX_node *srcnode = page_node(psrc, csrc->mc_ki[csrc->mc_top]);
    mdbx_cassert(csrc, node_flags(srcnode) == 0);
    const pgno_t srcpg = node_pgno(srcnode);
    key4move.iov_len = node_ks(srcnode);
    key4move.iov_base = node_key(srcnode);

    if (csrc->mc_ki[csrc->mc_top] == 0) {
      const uint16_t snum = csrc->mc_snum;
      mdbx_cassert(csrc, snum > 0);
      /* must find the lowest key below src */
      rc = mdbx_page_search_lowest(csrc);
      MDBX_page *lowest_page = csrc->mc_pg[csrc->mc_top];
      if (unlikely(rc))
        return rc;
      mdbx_cassert(csrc, IS_LEAF(lowest_page));
      if (unlikely(!IS_LEAF(lowest_page)))
        goto bailout;
      if (IS_LEAF2(lowest_page)) {
        key4move.iov_len = csrc->mc_db->md_xsize;
        key4move.iov_base = page_leaf2key(lowest_page, 0, key4move.iov_len);
      } else {
        const MDBX_node *lowest_node = page_node(lowest_page, 0);
        key4move.iov_len = node_ks(lowest_node);
        key4move.iov_base = node_key(lowest_node);
      }

      /* restore cursor after mdbx_page_search_lowest() */
      csrc->mc_snum = snum;
      csrc->mc_top = snum - 1;
      csrc->mc_ki[csrc->mc_top] = 0;

      /* paranoia */
      mdbx_cassert(csrc, psrc == csrc->mc_pg[csrc->mc_top]);
      mdbx_cassert(csrc, IS_BRANCH(psrc));
      if (unlikely(!IS_BRANCH(psrc)))
        goto bailout;
    }

    if (cdst->mc_ki[cdst->mc_top] == 0) {
      const uint16_t snum = cdst->mc_snum;
      mdbx_cassert(csrc, snum > 0);
      MDBX_cursor mn;
      mdbx_cursor_copy(cdst, &mn);
      mn.mc_xcursor = NULL;
      /* must find the lowest key below dst */
      rc = mdbx_page_search_lowest(&mn);
      if (unlikely(rc))
        return rc;
      MDBX_page *const lowest_page = mn.mc_pg[mn.mc_top];
      mdbx_cassert(cdst, IS_LEAF(lowest_page));
      if (unlikely(!IS_LEAF(lowest_page)))
        goto bailout;
      MDBX_val key;
      if (IS_LEAF2(lowest_page)) {
        key.iov_len = mn.mc_db->md_xsize;
        key.iov_base = page_leaf2key(lowest_page, 0, key.iov_len);
      } else {
        MDBX_node *lowest_node = page_node(lowest_page, 0);
        key.iov_len = node_ks(lowest_node);
        key.iov_base = node_key(lowest_node);
      }

      /* restore cursor after mdbx_page_search_lowest() */
      mn.mc_snum = snum;
      mn.mc_top = snum - 1;
      mn.mc_ki[mn.mc_top] = 0;

      const intptr_t delta =
          EVEN(key.iov_len) - EVEN(node_ks(page_node(mn.mc_pg[mn.mc_top], 0)));
      const intptr_t needed =
          branch_size(cdst->mc_txn->mt_env, &key4move) + delta;
      const intptr_t have = page_room(pdst);
      if (unlikely(needed > have))
        return MDBX_RESULT_TRUE;

      if (unlikely((rc = mdbx_page_touch(csrc)) ||
                   (rc = mdbx_page_touch(cdst))))
        return rc;
      psrc = csrc->mc_pg[csrc->mc_top];
      pdst = cdst->mc_pg[cdst->mc_top];

      rc = mdbx_update_key(&mn, &key);
      if (unlikely(rc))
        return rc;
    } else {
      const size_t needed = branch_size(cdst->mc_txn->mt_env, &key4move);
      const size_t have = page_room(pdst);
      if (unlikely(needed > have))
        return MDBX_RESULT_TRUE;

      if (unlikely((rc = mdbx_page_touch(csrc)) ||
                   (rc = mdbx_page_touch(cdst))))
        return rc;
      psrc = csrc->mc_pg[csrc->mc_top];
      pdst = cdst->mc_pg[cdst->mc_top];
    }

    mdbx_debug("moving %s-node %u [%s] on page %" PRIaPGNO
               " to node %u on page %" PRIaPGNO,
               "branch", csrc->mc_ki[csrc->mc_top], DKEY(&key4move),
               psrc->mp_pgno, cdst->mc_ki[cdst->mc_top], pdst->mp_pgno);
    /* Add the node to the destination page. */
    rc =
        mdbx_node_add_branch(cdst, cdst->mc_ki[cdst->mc_top], &key4move, srcpg);
  } break;

  case P_LEAF: {
    /* Mark src and dst as dirty. */
    if (unlikely((rc = mdbx_page_touch(csrc)) || (rc = mdbx_page_touch(cdst))))
      return rc;
    psrc = csrc->mc_pg[csrc->mc_top];
    pdst = cdst->mc_pg[cdst->mc_top];
    const MDBX_node *srcnode = page_node(psrc, csrc->mc_ki[csrc->mc_top]);
    MDBX_val data;
    data.iov_len = node_ds(srcnode);
    data.iov_base = node_data(srcnode);
    key4move.iov_len = node_ks(srcnode);
    key4move.iov_base = node_key(srcnode);
    mdbx_debug("moving %s-node %u [%s] on page %" PRIaPGNO
               " to node %u on page %" PRIaPGNO,
               "leaf", csrc->mc_ki[csrc->mc_top], DKEY(&key4move),
               psrc->mp_pgno, cdst->mc_ki[cdst->mc_top], pdst->mp_pgno);
    /* Add the node to the destination page. */
    rc = mdbx_node_add_leaf(cdst, cdst->mc_ki[cdst->mc_top], &key4move, &data,
                            node_flags(srcnode));
  } break;

  case P_LEAF | P_LEAF2: {
    /* Mark src and dst as dirty. */
    if (unlikely((rc = mdbx_page_touch(csrc)) || (rc = mdbx_page_touch(cdst))))
      return rc;
    psrc = csrc->mc_pg[csrc->mc_top];
    pdst = cdst->mc_pg[cdst->mc_top];
    key4move.iov_len = csrc->mc_db->md_xsize;
    key4move.iov_base =
        page_leaf2key(psrc, csrc->mc_ki[csrc->mc_top], key4move.iov_len);
    mdbx_debug("moving %s-node %u [%s] on page %" PRIaPGNO
               " to node %u on page %" PRIaPGNO,
               "leaf2", csrc->mc_ki[csrc->mc_top], DKEY(&key4move),
               psrc->mp_pgno, cdst->mc_ki[cdst->mc_top], pdst->mp_pgno);
    /* Add the node to the destination page. */
    rc = mdbx_node_add_leaf2(cdst, cdst->mc_ki[cdst->mc_top], &key4move);
  } break;

  default:
    goto bailout;
  }

  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  /* Delete the node from the source page. */
  mdbx_node_del(csrc, key4move.iov_len);

  mdbx_cassert(csrc, psrc == csrc->mc_pg[csrc->mc_top]);
  mdbx_cassert(cdst, pdst == cdst->mc_pg[cdst->mc_top]);
  mdbx_cassert(csrc, PAGETYPE(psrc) == PAGETYPE(pdst));

  {
    /* Adjust other cursors pointing to mp */
    MDBX_cursor *m2, *m3;
    const MDBX_dbi dbi = csrc->mc_dbi;
    mdbx_cassert(csrc, csrc->mc_top == cdst->mc_top);
    if (fromleft) {
      /* If we're adding on the left, bump others up */
      for (m2 = csrc->mc_txn->mt_cursors[dbi]; m2; m2 = m2->mc_next) {
        m3 = (csrc->mc_flags & C_SUB) ? &m2->mc_xcursor->mx_cursor : m2;
        if (!(m3->mc_flags & C_INITIALIZED) || m3->mc_top < csrc->mc_top)
          continue;
        if (m3 != cdst && m3->mc_pg[csrc->mc_top] == pdst &&
            m3->mc_ki[csrc->mc_top] >= cdst->mc_ki[csrc->mc_top]) {
          m3->mc_ki[csrc->mc_top]++;
        }
        if (m3 != csrc && m3->mc_pg[csrc->mc_top] == psrc &&
            m3->mc_ki[csrc->mc_top] == csrc->mc_ki[csrc->mc_top]) {
          m3->mc_pg[csrc->mc_top] = pdst;
          m3->mc_ki[csrc->mc_top] = cdst->mc_ki[cdst->mc_top];
          mdbx_cassert(csrc, csrc->mc_top > 0);
          m3->mc_ki[csrc->mc_top - 1]++;
        }
        if (XCURSOR_INITED(m3) && IS_LEAF(psrc))
          XCURSOR_REFRESH(m3, m3->mc_pg[csrc->mc_top], m3->mc_ki[csrc->mc_top]);
      }
    } else {
      /* Adding on the right, bump others down */
      for (m2 = csrc->mc_txn->mt_cursors[dbi]; m2; m2 = m2->mc_next) {
        m3 = (csrc->mc_flags & C_SUB) ? &m2->mc_xcursor->mx_cursor : m2;
        if (m3 == csrc)
          continue;
        if (!(m3->mc_flags & C_INITIALIZED) || m3->mc_top < csrc->mc_top)
          continue;
        if (m3->mc_pg[csrc->mc_top] == psrc) {
          if (!m3->mc_ki[csrc->mc_top]) {
            m3->mc_pg[csrc->mc_top] = pdst;
            m3->mc_ki[csrc->mc_top] = cdst->mc_ki[cdst->mc_top];
            mdbx_cassert(csrc, csrc->mc_top > 0);
            m3->mc_ki[csrc->mc_top - 1]--;
          } else {
            m3->mc_ki[csrc->mc_top]--;
          }
          if (XCURSOR_INITED(m3) && IS_LEAF(psrc))
            XCURSOR_REFRESH(m3, m3->mc_pg[csrc->mc_top],
                            m3->mc_ki[csrc->mc_top]);
        }
      }
    }
  }

  /* Update the parent separators. */
  if (csrc->mc_ki[csrc->mc_top] == 0) {
    mdbx_cassert(csrc, csrc->mc_top > 0);
    if (csrc->mc_ki[csrc->mc_top - 1] != 0) {
      MDBX_val key;
      if (IS_LEAF2(psrc)) {
        key.iov_len = psrc->mp_leaf2_ksize;
        key.iov_base = page_leaf2key(psrc, 0, key.iov_len);
      } else {
        MDBX_node *srcnode = page_node(psrc, 0);
        key.iov_len = node_ks(srcnode);
        key.iov_base = node_key(srcnode);
      }
      mdbx_debug("update separator for source page %" PRIaPGNO " to [%s]",
                 psrc->mp_pgno, DKEY(&key));
      MDBX_cursor mn;
      mdbx_cursor_copy(csrc, &mn);
      mn.mc_xcursor = NULL;
      mdbx_cassert(csrc, mn.mc_snum > 0);
      mn.mc_snum--;
      mn.mc_top--;
      /* We want mdbx_rebalance to find mn when doing fixups */
      WITH_CURSOR_TRACKING(mn, rc = mdbx_update_key(&mn, &key));
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
    }
    if (IS_BRANCH(psrc)) {
      const MDBX_val nullkey = {0, 0};
      const indx_t ix = csrc->mc_ki[csrc->mc_top];
      csrc->mc_ki[csrc->mc_top] = 0;
      rc = mdbx_update_key(csrc, &nullkey);
      csrc->mc_ki[csrc->mc_top] = ix;
      mdbx_cassert(csrc, rc == MDBX_SUCCESS);
    }
  }

  if (cdst->mc_ki[cdst->mc_top] == 0) {
    mdbx_cassert(cdst, cdst->mc_top > 0);
    if (cdst->mc_ki[cdst->mc_top - 1] != 0) {
      MDBX_val key;
      if (IS_LEAF2(pdst)) {
        key.iov_len = pdst->mp_leaf2_ksize;
        key.iov_base = page_leaf2key(pdst, 0, key.iov_len);
      } else {
        MDBX_node *srcnode = page_node(pdst, 0);
        key.iov_len = node_ks(srcnode);
        key.iov_base = node_key(srcnode);
      }
      mdbx_debug("update separator for destination page %" PRIaPGNO " to [%s]",
                 pdst->mp_pgno, DKEY(&key));
      MDBX_cursor mn;
      mdbx_cursor_copy(cdst, &mn);
      mn.mc_xcursor = NULL;
      mdbx_cassert(cdst, mn.mc_snum > 0);
      mn.mc_snum--;
      mn.mc_top--;
      /* We want mdbx_rebalance to find mn when doing fixups */
      WITH_CURSOR_TRACKING(mn, rc = mdbx_update_key(&mn, &key));
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
    }
    if (IS_BRANCH(pdst)) {
      const MDBX_val nullkey = {0, 0};
      const indx_t ix = cdst->mc_ki[cdst->mc_top];
      cdst->mc_ki[cdst->mc_top] = 0;
      rc = mdbx_update_key(cdst, &nullkey);
      cdst->mc_ki[cdst->mc_top] = ix;
      mdbx_cassert(cdst, rc == MDBX_SUCCESS);
    }
  }

  return MDBX_SUCCESS;
}

/* Merge one page into another.
 *
 * The nodes from the page pointed to by csrc will be copied to the page
 * pointed to by cdst and then the csrc page will be freed.
 *
 * [in] csrc Cursor pointing to the source page.
 * [in] cdst Cursor pointing to the destination page.
 *
 * Returns 0 on success, non-zero on failure. */
static int mdbx_page_merge(MDBX_cursor *csrc, MDBX_cursor *cdst) {
  MDBX_val key;
  int rc;

  mdbx_cassert(csrc, csrc != cdst);
  const MDBX_page *const psrc = csrc->mc_pg[csrc->mc_top];
  MDBX_page *pdst = cdst->mc_pg[cdst->mc_top];
  mdbx_debug("merging page %" PRIaPGNO " into %" PRIaPGNO, psrc->mp_pgno,
             pdst->mp_pgno);

  mdbx_cassert(csrc, PAGETYPE(psrc) == PAGETYPE(pdst));
  mdbx_cassert(csrc,
               csrc->mc_dbi == cdst->mc_dbi && csrc->mc_db == cdst->mc_db);
  mdbx_cassert(csrc, csrc->mc_snum > 1); /* can't merge root page */
  mdbx_cassert(cdst, cdst->mc_snum > 1);
  mdbx_cassert(cdst, cdst->mc_snum < cdst->mc_db->md_depth ||
                         IS_LEAF(cdst->mc_pg[cdst->mc_db->md_depth - 1]));
  mdbx_cassert(csrc, csrc->mc_snum < csrc->mc_db->md_depth ||
                         IS_LEAF(csrc->mc_pg[csrc->mc_db->md_depth - 1]));
  mdbx_cassert(cdst, page_room(pdst) >= page_used(cdst->mc_txn->mt_env, psrc));
  const int pagetype = PAGETYPE(psrc);

  /* Move all nodes from src to dst */
  const unsigned dst_nkeys = page_numkeys(pdst);
  const unsigned src_nkeys = page_numkeys(psrc);
  if (likely(src_nkeys)) {
    unsigned j = dst_nkeys;
    if (unlikely(pagetype & P_LEAF2)) {
      /* Mark dst as dirty. */
      if (unlikely(rc = mdbx_page_touch(cdst)))
        return rc;

      key.iov_len = csrc->mc_db->md_xsize;
      key.iov_base = page_data(psrc);
      unsigned i = 0;
      do {
        rc = mdbx_node_add_leaf2(cdst, j++, &key);
        if (unlikely(rc != MDBX_SUCCESS))
          return rc;
        key.iov_base = (char *)key.iov_base + key.iov_len;
      } while (++i != src_nkeys);
    } else {
      MDBX_node *srcnode = page_node(psrc, 0);
      key.iov_len = node_ks(srcnode);
      key.iov_base = node_key(srcnode);
      if (pagetype & P_BRANCH) {
        MDBX_cursor mn;
        mdbx_cursor_copy(csrc, &mn);
        mn.mc_xcursor = NULL;
        /* must find the lowest key below src */
        rc = mdbx_page_search_lowest(&mn);
        if (unlikely(rc))
          return rc;
        if (IS_LEAF2(mn.mc_pg[mn.mc_top])) {
          key.iov_len = mn.mc_db->md_xsize;
          key.iov_base = page_leaf2key(mn.mc_pg[mn.mc_top], 0, key.iov_len);
        } else {
          MDBX_node *lowest = page_node(mn.mc_pg[mn.mc_top], 0);
          key.iov_len = node_ks(lowest);
          key.iov_base = node_key(lowest);

          const size_t dst_room = page_room(pdst);
          const size_t src_used = page_used(cdst->mc_txn->mt_env, psrc);
          const size_t space_needed = src_used - node_ks(srcnode) + key.iov_len;
          if (unlikely(space_needed > dst_room))
            return MDBX_RESULT_TRUE;
        }
      }

      /* Mark dst as dirty. */
      if (unlikely(rc = mdbx_page_touch(cdst)))
        return rc;

      unsigned i = 0;
      while (true) {
        if (pagetype & P_LEAF) {
          MDBX_val data;
          data.iov_len = node_ds(srcnode);
          data.iov_base = node_data(srcnode);
          rc = mdbx_node_add_leaf(cdst, j++, &key, &data, node_flags(srcnode));
        } else {
          mdbx_cassert(csrc, node_flags(srcnode) == 0);
          rc = mdbx_node_add_branch(cdst, j++, &key, node_pgno(srcnode));
        }
        if (unlikely(rc != MDBX_SUCCESS))
          return rc;

        if (++i == src_nkeys)
          break;
        srcnode = page_node(psrc, i);
        key.iov_len = node_ks(srcnode);
        key.iov_base = node_key(srcnode);
      }
    }

    pdst = cdst->mc_pg[cdst->mc_top];
    mdbx_debug("dst page %" PRIaPGNO " now has %u keys (%.1f%% filled)",
               pdst->mp_pgno, page_numkeys(pdst),
               page_fill(cdst->mc_txn->mt_env, pdst));

    mdbx_cassert(csrc, psrc == csrc->mc_pg[csrc->mc_top]);
    mdbx_cassert(cdst, pdst == cdst->mc_pg[cdst->mc_top]);
  }

  /* Unlink the src page from parent and add to free list. */
  csrc->mc_top--;
  mdbx_node_del(csrc, 0);
  if (csrc->mc_ki[csrc->mc_top] == 0) {
    const MDBX_val nullkey = {0, 0};
    rc = mdbx_update_key(csrc, &nullkey);
    if (unlikely(rc)) {
      csrc->mc_top++;
      return rc;
    }
  }
  csrc->mc_top++;

  mdbx_cassert(csrc, psrc == csrc->mc_pg[csrc->mc_top]);
  mdbx_cassert(cdst, pdst == cdst->mc_pg[cdst->mc_top]);

  {
    /* Adjust other cursors pointing to mp */
    MDBX_cursor *m2, *m3;
    const MDBX_dbi dbi = csrc->mc_dbi;
    const unsigned top = csrc->mc_top;

    for (m2 = csrc->mc_txn->mt_cursors[dbi]; m2; m2 = m2->mc_next) {
      m3 = (csrc->mc_flags & C_SUB) ? &m2->mc_xcursor->mx_cursor : m2;
      if (m3 == csrc || top >= m3->mc_snum)
        continue;
      if (m3->mc_pg[top] == psrc) {
        m3->mc_pg[top] = pdst;
        mdbx_cassert(m3, dst_nkeys + m3->mc_ki[top] <= UINT16_MAX);
        m3->mc_ki[top] += (indx_t)dst_nkeys;
        m3->mc_ki[top - 1] = cdst->mc_ki[top - 1];
      } else if (m3->mc_pg[top - 1] == csrc->mc_pg[top - 1] &&
                 m3->mc_ki[top - 1] > csrc->mc_ki[top - 1]) {
        m3->mc_ki[top - 1]--;
      }
      if (XCURSOR_INITED(m3) && IS_LEAF(psrc))
        XCURSOR_REFRESH(m3, m3->mc_pg[top], m3->mc_ki[top]);
    }
  }

  /* If not operating on GC, allow this page to be reused
   * in this txn. Otherwise just add to free list. */
  rc = mdbx_page_retire(csrc, (MDBX_page *)psrc);
  if (unlikely(rc))
    return rc;

  mdbx_cassert(cdst, cdst->mc_db->md_entries > 0);
  mdbx_cassert(cdst, cdst->mc_snum <= cdst->mc_db->md_depth);
  mdbx_cassert(cdst, cdst->mc_top > 0);
  mdbx_cassert(cdst, cdst->mc_snum == cdst->mc_top + 1);
  MDBX_page *const top_page = cdst->mc_pg[cdst->mc_top];
  const indx_t top_indx = cdst->mc_ki[cdst->mc_top];
  const uint16_t save_snum = cdst->mc_snum;
  const uint16_t save_depth = cdst->mc_db->md_depth;
  mdbx_cursor_pop(cdst);
  rc = mdbx_rebalance(cdst);
  if (unlikely(rc))
    return rc;

  mdbx_cassert(cdst, cdst->mc_db->md_entries > 0);
  mdbx_cassert(cdst, cdst->mc_snum <= cdst->mc_db->md_depth);
  mdbx_cassert(cdst, cdst->mc_snum == cdst->mc_top + 1);

  if (IS_LEAF(cdst->mc_pg[cdst->mc_top])) {
    /* LY: don't touch cursor if top-page is a LEAF */
    mdbx_cassert(cdst, IS_LEAF(cdst->mc_pg[cdst->mc_top]) ||
                           PAGETYPE(cdst->mc_pg[cdst->mc_top]) == pagetype);
    return MDBX_SUCCESS;
  }

  if (pagetype != PAGETYPE(top_page)) {
    /* LY: LEAF-page becomes BRANCH, unable restore cursor's stack */
    goto bailout;
  }

  if (top_page == cdst->mc_pg[cdst->mc_top]) {
    /* LY: don't touch cursor if prev top-page already on the top */
    mdbx_cassert(cdst, cdst->mc_ki[cdst->mc_top] == top_indx);
    mdbx_cassert(cdst, IS_LEAF(cdst->mc_pg[cdst->mc_top]) ||
                           PAGETYPE(cdst->mc_pg[cdst->mc_top]) == pagetype);
    return MDBX_SUCCESS;
  }

  const int new_snum = save_snum - save_depth + cdst->mc_db->md_depth;
  if (unlikely(new_snum < 1 || new_snum > cdst->mc_db->md_depth)) {
    /* LY: out of range, unable restore cursor's stack */
    goto bailout;
  }

  if (top_page == cdst->mc_pg[new_snum - 1]) {
    mdbx_cassert(cdst, cdst->mc_ki[new_snum - 1] == top_indx);
    /* LY: restore cursor stack */
    cdst->mc_snum = (uint16_t)new_snum;
    cdst->mc_top = (uint16_t)new_snum - 1;
    mdbx_cassert(cdst, cdst->mc_snum < cdst->mc_db->md_depth ||
                           IS_LEAF(cdst->mc_pg[cdst->mc_db->md_depth - 1]));
    mdbx_cassert(cdst, IS_LEAF(cdst->mc_pg[cdst->mc_top]) ||
                           PAGETYPE(cdst->mc_pg[cdst->mc_top]) == pagetype);
    return MDBX_SUCCESS;
  }

  MDBX_page *const stub_page = (MDBX_page *)(~(uintptr_t)top_page);
  const indx_t stub_indx = top_indx;
  if (save_depth > cdst->mc_db->md_depth &&
      ((cdst->mc_pg[save_snum - 1] == top_page &&
        cdst->mc_ki[save_snum - 1] == top_indx) ||
       (cdst->mc_pg[save_snum - 1] == stub_page &&
        cdst->mc_ki[save_snum - 1] == stub_indx))) {
    /* LY: restore cursor stack */
    cdst->mc_pg[new_snum - 1] = top_page;
    cdst->mc_ki[new_snum - 1] = top_indx;
    cdst->mc_pg[new_snum] = (MDBX_page *)(~(uintptr_t)cdst->mc_pg[new_snum]);
    cdst->mc_ki[new_snum] = ~cdst->mc_ki[new_snum];
    cdst->mc_snum = (uint16_t)new_snum;
    cdst->mc_top = (uint16_t)new_snum - 1;
    mdbx_cassert(cdst, cdst->mc_snum < cdst->mc_db->md_depth ||
                           IS_LEAF(cdst->mc_pg[cdst->mc_db->md_depth - 1]));
    mdbx_cassert(cdst, IS_LEAF(cdst->mc_pg[cdst->mc_top]) ||
                           PAGETYPE(cdst->mc_pg[cdst->mc_top]) == pagetype);
    return MDBX_SUCCESS;
  }

bailout:
  /* LY: unable restore cursor's stack */
  cdst->mc_flags &= ~C_INITIALIZED;
  return MDBX_CURSOR_FULL;
}

/* Copy the contents of a cursor.
 * [in] csrc The cursor to copy from.
 * [out] cdst The cursor to copy to. */
static void mdbx_cursor_copy(const MDBX_cursor *csrc, MDBX_cursor *cdst) {
  unsigned i;

  mdbx_cassert(csrc,
               csrc->mc_txn->mt_txnid >= *csrc->mc_txn->mt_env->me_oldest);
  cdst->mc_txn = csrc->mc_txn;
  cdst->mc_dbi = csrc->mc_dbi;
  cdst->mc_db = csrc->mc_db;
  cdst->mc_dbx = csrc->mc_dbx;
  cdst->mc_snum = csrc->mc_snum;
  cdst->mc_top = csrc->mc_top;
  cdst->mc_flags = csrc->mc_flags;

  for (i = 0; i < csrc->mc_snum; i++) {
    cdst->mc_pg[i] = csrc->mc_pg[i];
    cdst->mc_ki[i] = csrc->mc_ki[i];
  }
}

/* Rebalance the tree after a delete operation.
 * [in] mc Cursor pointing to the page where rebalancing should begin.
 * Returns 0 on success, non-zero on failure. */
static int mdbx_rebalance(MDBX_cursor *mc) {
  int rc;

  mdbx_cassert(mc, mc->mc_snum > 0);
  mdbx_cassert(mc, mc->mc_snum < mc->mc_db->md_depth ||
                       IS_LEAF(mc->mc_pg[mc->mc_db->md_depth - 1]));
  const int pagetype = PAGETYPE(mc->mc_pg[mc->mc_top]);

  const unsigned minkeys = (P_BRANCH == 1) ? (pagetype & P_BRANCH) + 1
                                           : (pagetype & P_BRANCH) ? 2 : 1;

  /* The threshold of minimum page fill factor, in form of a negative binary
   * exponent, i.e. 2 means 1/(2**3) == 1/4 == 25%. Pages emptier than this
   * are candidates for merging. */
  const unsigned threshold_fill_exp2 = 2;

  /* The threshold of minimum page fill factor, as a number of free bytes on a
   * page. Pages emptier than this are candidates for merging. */
  const unsigned spaceleft_threshold =
      page_space(mc->mc_txn->mt_env) -
      (page_space(mc->mc_txn->mt_env) >> threshold_fill_exp2);

  mdbx_debug("rebalancing %s page %" PRIaPGNO " (has %u keys, %.1f%% full)",
             (pagetype & P_LEAF) ? "leaf" : "branch",
             mc->mc_pg[mc->mc_top]->mp_pgno,
             page_numkeys(mc->mc_pg[mc->mc_top]),
             page_fill(mc->mc_txn->mt_env, mc->mc_pg[mc->mc_top]));

  if (page_fill_enough(mc->mc_pg[mc->mc_top], spaceleft_threshold, minkeys)) {
    mdbx_debug("no need to rebalance page %" PRIaPGNO ", above fill threshold",
               mc->mc_pg[mc->mc_top]->mp_pgno);
    mdbx_cassert(mc, mc->mc_db->md_entries > 0);
    return MDBX_SUCCESS;
  }

  if (mc->mc_snum < 2) {
    MDBX_page *const mp = mc->mc_pg[0];
    const unsigned nkeys = page_numkeys(mp);
    mdbx_cassert(mc, (mc->mc_db->md_entries == 0) == (nkeys == 0));
    if (IS_SUBP(mp)) {
      mdbx_debug("%s", "Can't rebalance a subpage, ignoring");
      mdbx_cassert(mc, pagetype & P_LEAF);
      return MDBX_SUCCESS;
    }
    if (nkeys == 0) {
      mdbx_cassert(mc, IS_LEAF(mp));
      mdbx_debug("%s", "tree is completely empty");
      mc->mc_db->md_root = P_INVALID;
      mc->mc_db->md_depth = 0;
      mdbx_cassert(mc, mc->mc_db->md_branch_pages == 0 &&
                           mc->mc_db->md_overflow_pages == 0 &&
                           mc->mc_db->md_leaf_pages == 1);
      /* Adjust cursors pointing to mp */
      const MDBX_dbi dbi = mc->mc_dbi;
      for (MDBX_cursor *m2 = mc->mc_txn->mt_cursors[dbi]; m2;
           m2 = m2->mc_next) {
        MDBX_cursor *m3 =
            (mc->mc_flags & C_SUB) ? &m2->mc_xcursor->mx_cursor : m2;
        if (m3 == mc || !(m3->mc_flags & C_INITIALIZED))
          continue;
        if (m3->mc_pg[0] == mp) {
          m3->mc_snum = 0;
          m3->mc_top = 0;
          m3->mc_flags &= ~C_INITIALIZED;
        }
      }
      mc->mc_snum = 0;
      mc->mc_top = 0;
      mc->mc_flags &= ~C_INITIALIZED;

      rc = mdbx_page_retire(mc, mp);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
    } else if (IS_BRANCH(mp) && nkeys == 1) {
      mdbx_debug("%s", "collapsing root page!");
      mc->mc_db->md_root = node_pgno(page_node(mp, 0));
      rc = mdbx_page_get(mc, mc->mc_db->md_root, &mc->mc_pg[0], NULL);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
      mc->mc_db->md_depth--;
      mc->mc_ki[0] = mc->mc_ki[1];
      for (int i = 1; i < mc->mc_db->md_depth; i++) {
        mc->mc_pg[i] = mc->mc_pg[i + 1];
        mc->mc_ki[i] = mc->mc_ki[i + 1];
      }

      /* Adjust other cursors pointing to mp */
      MDBX_cursor *m2, *m3;
      MDBX_dbi dbi = mc->mc_dbi;
      for (m2 = mc->mc_txn->mt_cursors[dbi]; m2; m2 = m2->mc_next) {
        m3 = (mc->mc_flags & C_SUB) ? &m2->mc_xcursor->mx_cursor : m2;
        if (m3 == mc || !(m3->mc_flags & C_INITIALIZED))
          continue;
        if (m3->mc_pg[0] == mp) {
          for (int i = 0; i < mc->mc_db->md_depth; i++) {
            m3->mc_pg[i] = m3->mc_pg[i + 1];
            m3->mc_ki[i] = m3->mc_ki[i + 1];
          }
          m3->mc_snum--;
          m3->mc_top--;
        }
      }
      mdbx_cassert(mc, IS_LEAF(mc->mc_pg[mc->mc_top]) ||
                           PAGETYPE(mc->mc_pg[mc->mc_top]) == pagetype);
      mdbx_cassert(mc, mc->mc_snum < mc->mc_db->md_depth ||
                           IS_LEAF(mc->mc_pg[mc->mc_db->md_depth - 1]));

      rc = mdbx_page_retire(mc, mp);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
    } else {
      mdbx_debug("root page %" PRIaPGNO
                 " doesn't need rebalancing (flags 0x%x)",
                 mp->mp_pgno, mp->mp_flags);
    }
    return MDBX_SUCCESS;
  }

  /* The parent (branch page) must have at least 2 pointers,
   * otherwise the tree is invalid. */
  const unsigned pre_top = mc->mc_top - 1;
  mdbx_cassert(mc, IS_BRANCH(mc->mc_pg[pre_top]));
  mdbx_cassert(mc, !IS_SUBP(mc->mc_pg[0]));
  mdbx_cassert(mc, page_numkeys(mc->mc_pg[pre_top]) > 1);

  /* Leaf page fill factor is below the threshold.
   * Try to move keys from left or right neighbor, or
   * merge with a neighbor page. */

  /* Find neighbors. */
  MDBX_cursor mn;
  mdbx_cursor_copy(mc, &mn);
  mn.mc_xcursor = NULL;

  MDBX_page *left = nullptr, *right = nullptr;
  if (mn.mc_ki[pre_top] > 0) {
    rc = mdbx_page_get(
        &mn, node_pgno(page_node(mn.mc_pg[pre_top], mn.mc_ki[pre_top] - 1)),
        &left, NULL);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    mdbx_cassert(mc, PAGETYPE(left) == PAGETYPE(mc->mc_pg[mc->mc_top]));
  }
  if (mn.mc_ki[pre_top] + 1u < page_numkeys(mn.mc_pg[pre_top])) {
    rc = mdbx_page_get(
        &mn, node_pgno(page_node(mn.mc_pg[pre_top], mn.mc_ki[pre_top] + 1)),
        &right, NULL);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    mdbx_cassert(mc, PAGETYPE(right) == PAGETYPE(mc->mc_pg[mc->mc_top]));
  }

  int ki = mc->mc_ki[mc->mc_top];
  bool fromleft;
  if (!left || (right && page_room(left) < page_room(right))) {
    mdbx_debug("merging %s neighbor", "right");
    mn.mc_pg[mn.mc_top] = right;
    mn.mc_ki[pre_top] += 1;
    mn.mc_ki[mn.mc_top] = 0;
    mc->mc_ki[mc->mc_top] = (indx_t)page_numkeys(mc->mc_pg[mc->mc_top]);
    fromleft = false;
  } else {
    mdbx_debug("merging %s neighbor", "left");
    mn.mc_pg[mn.mc_top] = left;
    mn.mc_ki[pre_top] -= 1;
    mn.mc_ki[mn.mc_top] = (indx_t)page_numkeys(mn.mc_pg[mn.mc_top]) - 1;
    mc->mc_ki[mc->mc_top] = 0;
    fromleft = true;
  }

  mdbx_debug("found neighbor page %" PRIaPGNO " (%u keys, %.1f%% full)",
             mn.mc_pg[mn.mc_top]->mp_pgno, page_numkeys(mn.mc_pg[mn.mc_top]),
             page_fill(mc->mc_txn->mt_env, mn.mc_pg[mn.mc_top]));

  /* If the neighbor page is above threshold and has enough keys,
   * move one key from it. Otherwise we should try to merge them.
   * (A branch page must never have less than 2 keys.) */
  if (page_fill_enough(mn.mc_pg[mn.mc_top], spaceleft_threshold, minkeys + 1)) {
    rc = mdbx_node_move(&mn, mc, fromleft);
    if (likely(rc == MDBX_SUCCESS))
      ki += fromleft /* if we inserted on left, bump position up */;
    else if (unlikely(rc != MDBX_RESULT_TRUE))
      return rc;
    mdbx_cassert(mc, IS_LEAF(mc->mc_pg[mc->mc_top]) ||
                         PAGETYPE(mc->mc_pg[mc->mc_top]) == pagetype);
    mdbx_cassert(mc, mc->mc_snum < mc->mc_db->md_depth ||
                         IS_LEAF(mc->mc_pg[mc->mc_db->md_depth - 1]));
  } else {
    if (!fromleft) {
      rc = mdbx_page_merge(&mn, mc);
      if (unlikely(MDBX_IS_ERROR(rc)))
        return rc;
      mdbx_cassert(mc, IS_LEAF(mc->mc_pg[mc->mc_top]) ||
                           PAGETYPE(mc->mc_pg[mc->mc_top]) == pagetype);
      mdbx_cassert(mc, mc->mc_snum < mc->mc_db->md_depth ||
                           IS_LEAF(mc->mc_pg[mc->mc_db->md_depth - 1]));
    } else {
      int new_ki = ki + page_numkeys(mn.mc_pg[mn.mc_top]);
      mn.mc_ki[mn.mc_top] += mc->mc_ki[mn.mc_top] + 1;
      /* We want mdbx_rebalance to find mn when doing fixups */
      WITH_CURSOR_TRACKING(mn, rc = mdbx_page_merge(mc, &mn));
      if (likely(rc == MDBX_SUCCESS)) {
        ki = new_ki;
        mdbx_cursor_copy(&mn, mc);
      } else if (unlikely(rc != MDBX_RESULT_TRUE))
        return rc;
      mdbx_cassert(mc, IS_LEAF(mc->mc_pg[mc->mc_top]) ||
                           PAGETYPE(mc->mc_pg[mc->mc_top]) == pagetype);
      mdbx_cassert(mc, mc->mc_snum < mc->mc_db->md_depth ||
                           IS_LEAF(mc->mc_pg[mc->mc_db->md_depth - 1]));
    }
  }
  mc->mc_ki[mc->mc_top] = (indx_t)ki;
  return MDBX_SUCCESS;
}

static __cold int mdbx_page_check(MDBX_env *env, const MDBX_page *const mp,
                                  bool maybe_unfinished) {
  const unsigned nkeys = page_numkeys(mp);
  char *const end_of_page = (char *)mp + env->me_psize;
  mdbx_assert(env, mp->mp_pgno >= MIN_PAGENO && mp->mp_pgno <= MAX_PAGENO);
  if (unlikely(mp->mp_pgno < MIN_PAGENO || mp->mp_pgno > MAX_PAGENO))
    return MDBX_CORRUPTED;
  if (IS_OVERFLOW(mp)) {
    mdbx_assert(env, mp->mp_pages >= 1 && mp->mp_pages < MAX_PAGENO / 2);
    if (unlikely(mp->mp_pages < 1 && mp->mp_pages >= MAX_PAGENO / 2))
      return MDBX_CORRUPTED;
    mdbx_assert(env, mp->mp_pgno <= MAX_PAGENO - mp->mp_pages);
    if (unlikely(mp->mp_pgno > MAX_PAGENO - mp->mp_pages))
      return MDBX_CORRUPTED;
    return MDBX_SUCCESS;
  }
  if (!(IS_DIRTY(mp) && maybe_unfinished)) {
    mdbx_assert(env, nkeys >= 2 || !IS_BRANCH(mp));
    if (unlikely(nkeys < 2 && IS_BRANCH(mp)))
      return MDBX_CORRUPTED;
  }

  for (unsigned i = IS_LEAF(mp) ? 0 : 1; i < nkeys; ++i) {
    if (IS_LEAF2(mp)) {
      const size_t ksize = mp->mp_leaf2_ksize;
      const char *const key = page_leaf2key(mp, i, ksize);
      mdbx_assert(env, key + ksize <= end_of_page);
      if (unlikely(end_of_page < key + ksize))
        return MDBX_CORRUPTED;
    } else {
      const MDBX_node *const node = page_node(mp, i);
      const char *node_end = (char *)node + NODESIZE;
      mdbx_assert(env, node_end <= end_of_page);
      if (unlikely(node_end > end_of_page))
        return MDBX_CORRUPTED;
      if (IS_LEAF(mp) || i > 0) {
        size_t ksize = node_ks(node);
        char *key = node_key(node);
        mdbx_assert(env, key + ksize <= end_of_page);
        if (unlikely(end_of_page < key + ksize))
          return MDBX_CORRUPTED;
      }
      if (IS_BRANCH(mp))
        continue;
      if (node_flags(node) == F_BIGDATA /* data on large-page */) {
        continue;
      }
      const size_t dsize = node_ds(node);
      const char *const data = node_data(node);
      mdbx_assert(env, data + dsize <= end_of_page);
      if (unlikely(end_of_page < data + dsize))
        return MDBX_CORRUPTED;

      switch (node_flags(node)) {
      default:
        mdbx_assert(env, false);
        return MDBX_CORRUPTED;
      case 0 /* usual */:
        break;
      case F_SUBDATA /* sub-db */:
        mdbx_assert(env, dsize >= sizeof(MDBX_db));
        if (unlikely(dsize < sizeof(MDBX_db)))
          return MDBX_CORRUPTED;
        break;
      case F_SUBDATA | F_DUPDATA /* dupsorted sub-tree */:
        mdbx_assert(env, dsize == sizeof(MDBX_db));
        if (unlikely(dsize != sizeof(MDBX_db)))
          return MDBX_CORRUPTED;
        break;
      case F_DUPDATA /* short sub-page */:
        mdbx_assert(env, dsize > PAGEHDRSZ);
        if (unlikely(dsize <= PAGEHDRSZ))
          return MDBX_CORRUPTED;
        else {
          const MDBX_page *const sp = (MDBX_page *)data;
          const char *const end_of_subpage = data + dsize;
          const int nsubkeys = page_numkeys(sp);
          switch (sp->mp_flags & ~P_DIRTY /* ignore for sub-pages */) {
          case P_LEAF | P_SUBP:
          case P_LEAF | P_LEAF2 | P_SUBP:
            break;
          default:
            mdbx_assert(env, false);
            return MDBX_CORRUPTED;
          }

          for (int j = 0; j < nsubkeys; j++) {
            if (IS_LEAF2(sp)) {
              /* LEAF2 pages have no mp_ptrs[] or node headers */
              size_t sub_ksize = sp->mp_leaf2_ksize;
              char *sub_key = page_leaf2key(sp, j, sub_ksize);
              mdbx_assert(env, sub_key + sub_ksize <= end_of_subpage);
              if (unlikely(end_of_subpage < sub_key + sub_ksize))
                return MDBX_CORRUPTED;
            } else {
              mdbx_assert(env, IS_LEAF(sp));
              if (unlikely(!IS_LEAF(sp)))
                return MDBX_CORRUPTED;
              const MDBX_node *const sub_node = page_node(sp, j);
              const char *sub_node_end = (char *)sub_node + NODESIZE;
              mdbx_assert(env, sub_node_end <= end_of_subpage);
              if (unlikely(sub_node_end > end_of_subpage))
                return MDBX_CORRUPTED;
              mdbx_assert(env, node_flags(sub_node) == 0);
              if (unlikely(node_flags(sub_node) != 0))
                return MDBX_CORRUPTED;

              size_t sub_ksize = node_ks(sub_node);
              char *sub_key = node_key(sub_node);
              size_t sub_dsize = node_ds(sub_node);
              char *sub_data = node_data(sub_node);
              mdbx_assert(env, sub_key + sub_ksize <= end_of_subpage);
              if (unlikely(end_of_subpage < sub_key + sub_ksize))
                return MDBX_CORRUPTED;
              mdbx_assert(env, sub_data + sub_dsize <= end_of_subpage);
              if (unlikely(end_of_subpage < sub_data + sub_dsize))
                return MDBX_CORRUPTED;
            }
          }
        }
        break;
      }
    }
  }
  return MDBX_SUCCESS;
}

static __cold int mdbx_cursor_check(MDBX_cursor *mc, bool pending) {
  mdbx_tassert(mc->mc_txn, mc->mc_txn->mt_parent ||
                               mc->mc_txn->tw.dirtyroom +
                                       mc->mc_txn->tw.dirtylist->length ==
                                   MDBX_DPL_TXNFULL);
  mdbx_cassert(mc, mc->mc_top == mc->mc_snum - 1);
  if (unlikely(mc->mc_top != mc->mc_snum - 1))
    return MDBX_CURSOR_FULL;
  mdbx_cassert(mc, pending ? mc->mc_snum <= mc->mc_db->md_depth
                           : mc->mc_snum == mc->mc_db->md_depth);
  if (unlikely(pending ? mc->mc_snum > mc->mc_db->md_depth
                       : mc->mc_snum != mc->mc_db->md_depth))
    return MDBX_CURSOR_FULL;

  for (int n = 0; n < mc->mc_snum; ++n) {
    MDBX_page *mp = mc->mc_pg[n];
    const unsigned nkeys = page_numkeys(mp);
    const bool expect_branch = (n < mc->mc_db->md_depth - 1) ? true : false;
    const bool expect_nested_leaf =
        (n + 1 == mc->mc_db->md_depth - 1) ? true : false;
    const bool branch = IS_BRANCH(mp) ? true : false;
    mdbx_cassert(mc, branch == expect_branch);
    if (unlikely(branch != expect_branch))
      return MDBX_CURSOR_FULL;
    if (!pending) {
      mdbx_cassert(mc,
                   nkeys > mc->mc_ki[n] || (!branch && nkeys == mc->mc_ki[n] &&
                                            (mc->mc_flags & C_EOF) != 0));
      if (unlikely(nkeys <= mc->mc_ki[n] &&
                   !(!branch && nkeys == mc->mc_ki[n] &&
                     (mc->mc_flags & C_EOF) != 0)))
        return MDBX_CURSOR_FULL;
    } else {
      mdbx_cassert(mc, nkeys + 1 >= mc->mc_ki[n]);
      if (unlikely(nkeys + 1 < mc->mc_ki[n]))
        return MDBX_CURSOR_FULL;
    }

    int err = mdbx_page_check(mc->mc_txn->mt_env, mp, pending);
    if (unlikely(err != MDBX_SUCCESS))
      return err;

    for (unsigned i = 0; i < nkeys; ++i) {
      if (branch) {
        MDBX_node *node = page_node(mp, i);
        mdbx_cassert(mc, node_flags(node) == 0);
        if (unlikely(node_flags(node) != 0))
          return MDBX_CURSOR_FULL;
        pgno_t pgno = node_pgno(node);
        MDBX_page *np;
        int rc = mdbx_page_get(mc, pgno, &np, NULL);
        mdbx_cassert(mc, rc == MDBX_SUCCESS);
        if (unlikely(rc != MDBX_SUCCESS))
          return rc;
        const bool nested_leaf = IS_LEAF(np) ? true : false;
        mdbx_cassert(mc, nested_leaf == expect_nested_leaf);
        if (unlikely(nested_leaf != expect_nested_leaf))
          return MDBX_CURSOR_FULL;
        err = mdbx_page_check(mc->mc_txn->mt_env, np, pending);
        if (unlikely(err != MDBX_SUCCESS))
          return err;
      }
    }
  }
  return MDBX_SUCCESS;
}

/* Complete a delete operation started by mdbx_cursor_del(). */
static int mdbx_cursor_del0(MDBX_cursor *mc) {
  int rc;
  MDBX_page *mp;
  indx_t ki;
  unsigned nkeys;
  MDBX_cursor *m2, *m3;
  MDBX_dbi dbi = mc->mc_dbi;

  mdbx_cassert(mc, IS_LEAF(mc->mc_pg[mc->mc_top]));
  ki = mc->mc_ki[mc->mc_top];
  mp = mc->mc_pg[mc->mc_top];
  mdbx_node_del(mc, mc->mc_db->md_xsize);
  mc->mc_db->md_entries--;
  {
    /* Adjust other cursors pointing to mp */
    for (m2 = mc->mc_txn->mt_cursors[dbi]; m2; m2 = m2->mc_next) {
      m3 = (mc->mc_flags & C_SUB) ? &m2->mc_xcursor->mx_cursor : m2;
      if (m3 == mc || !(m2->mc_flags & m3->mc_flags & C_INITIALIZED))
        continue;
      if (m3->mc_snum < mc->mc_snum)
        continue;
      if (m3->mc_pg[mc->mc_top] == mp) {
        if (m3->mc_ki[mc->mc_top] == ki) {
          m3->mc_flags |= C_DEL;
          if (mc->mc_db->md_flags & MDBX_DUPSORT) {
            /* Sub-cursor referred into dataset which is gone */
            m3->mc_xcursor->mx_cursor.mc_flags &= ~(C_INITIALIZED | C_EOF);
          }
          continue;
        } else if (m3->mc_ki[mc->mc_top] > ki) {
          m3->mc_ki[mc->mc_top]--;
        }
        if (XCURSOR_INITED(m3))
          XCURSOR_REFRESH(m3, m3->mc_pg[mc->mc_top], m3->mc_ki[mc->mc_top]);
      }
    }
  }
  rc = mdbx_rebalance(mc);

  if (likely(rc == MDBX_SUCCESS)) {
    /* DB is totally empty now, just bail out.
     * Other cursors adjustments were already done
     * by mdbx_rebalance and aren't needed here. */
    if (!mc->mc_snum) {
      mdbx_cassert(mc, mc->mc_db->md_entries == 0 && mc->mc_db->md_depth == 0 &&
                           mc->mc_db->md_root == P_INVALID);
      mc->mc_flags |= C_DEL | C_EOF;
      return rc;
    }

    ki = mc->mc_ki[mc->mc_top];
    mp = mc->mc_pg[mc->mc_top];
    mdbx_cassert(mc, IS_LEAF(mc->mc_pg[mc->mc_top]));
    nkeys = page_numkeys(mp);
    mdbx_cassert(mc, (mc->mc_db->md_entries > 0 && nkeys > 0) ||
                         ((mc->mc_flags & C_SUB) &&
                          mc->mc_db->md_entries == 0 && nkeys == 0));

    /* Adjust THIS and other cursors pointing to mp */
    for (m2 = mc->mc_txn->mt_cursors[dbi]; m2; m2 = m2->mc_next) {
      m3 = (mc->mc_flags & C_SUB) ? &m2->mc_xcursor->mx_cursor : m2;
      if (m3 == mc || !(m2->mc_flags & m3->mc_flags & C_INITIALIZED))
        continue;
      if (m3->mc_snum < mc->mc_snum)
        continue;
      if (m3->mc_pg[mc->mc_top] == mp) {
        /* if m3 points past last node in page, find next sibling */
        if (m3->mc_ki[mc->mc_top] >= nkeys) {
          rc = mdbx_cursor_sibling(m3, true);
          if (rc == MDBX_NOTFOUND) {
            m3->mc_flags |= C_EOF;
            rc = MDBX_SUCCESS;
            continue;
          } else if (unlikely(rc != MDBX_SUCCESS))
            break;
        }
        if (m3->mc_ki[mc->mc_top] >= ki || m3->mc_pg[mc->mc_top] != mp) {
          if ((mc->mc_db->md_flags & MDBX_DUPSORT) != 0 &&
              (m3->mc_flags & C_EOF) == 0) {
            MDBX_node *node =
                page_node(m3->mc_pg[m3->mc_top], m3->mc_ki[m3->mc_top]);
            /* If this node has dupdata, it may need to be reinited
             * because its data has moved.
             * If the xcursor was not initd it must be reinited.
             * Else if node points to a subDB, nothing is needed. */
            if (node_flags(node) & F_DUPDATA) {
              if (m3->mc_xcursor->mx_cursor.mc_flags & C_INITIALIZED) {
                if (!(node_flags(node) & F_SUBDATA))
                  m3->mc_xcursor->mx_cursor.mc_pg[0] = node_data(node);
              } else {
                rc = mdbx_xcursor_init1(m3, node);
                if (unlikely(rc != MDBX_SUCCESS))
                  break;
                m3->mc_xcursor->mx_cursor.mc_flags |= C_DEL;
              }
            }
          }
        }
      }
    }

    if (mc->mc_ki[mc->mc_top] >= nkeys) {
      rc = mdbx_cursor_sibling(mc, true);
      if (rc == MDBX_NOTFOUND) {
        mc->mc_flags |= C_EOF;
        rc = MDBX_SUCCESS;
      }
    }
    if ((mc->mc_db->md_flags & MDBX_DUPSORT) != 0 &&
        (mc->mc_flags & C_EOF) == 0) {
      MDBX_node *node = page_node(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top]);
      /* If this node has dupdata, it may need to be reinited
       * because its data has moved.
       * If the xcursor was not initd it must be reinited.
       * Else if node points to a subDB, nothing is needed. */
      if (node_flags(node) & F_DUPDATA) {
        if (mc->mc_xcursor->mx_cursor.mc_flags & C_INITIALIZED) {
          if (!(node_flags(node) & F_SUBDATA))
            mc->mc_xcursor->mx_cursor.mc_pg[0] = node_data(node);
        } else {
          rc = mdbx_xcursor_init1(mc, node);
          if (likely(rc != MDBX_SUCCESS))
            mc->mc_xcursor->mx_cursor.mc_flags |= C_DEL;
        }
      }
    }
    mc->mc_flags |= C_DEL;
  }

  if (unlikely(rc))
    mc->mc_txn->mt_flags |= MDBX_TXN_ERROR;
  else if (mdbx_audit_enabled())
    rc = mdbx_cursor_check(mc, false);

  return rc;
}

int mdbx_del(MDBX_txn *txn, MDBX_dbi dbi, MDBX_val *key, MDBX_val *data) {
  int rc = check_txn_rw(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!key))
    return MDBX_EINVAL;

  if (unlikely(!TXN_DBI_EXIST(txn, dbi, DB_USRVALID)))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_flags & (MDBX_RDONLY | MDBX_TXN_BLOCKED)))
    return (txn->mt_flags & MDBX_RDONLY) ? MDBX_EACCESS : MDBX_BAD_TXN;

  return mdbx_del0(txn, dbi, key, data, 0);
}

static int mdbx_del0(MDBX_txn *txn, MDBX_dbi dbi, MDBX_val *key, MDBX_val *data,
                     unsigned flags) {
  MDBX_cursor_couple cx;
  MDBX_cursor_op op;
  MDBX_val rdata;
  int rc, exact = 0;
  DKBUF;

  mdbx_debug("====> delete db %u key [%s], data [%s]", dbi, DKEY(key),
             DVAL(data));

  rc = mdbx_cursor_init(&cx.outer, txn, dbi);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (data) {
    op = MDBX_GET_BOTH;
    rdata = *data;
    data = &rdata;
  } else {
    op = MDBX_SET;
    flags |= MDBX_NODUPDATA;
  }
  rc = mdbx_cursor_set(&cx.outer, key, data, op, &exact);
  if (likely(rc == MDBX_SUCCESS)) {
    /* let mdbx_page_split know about this cursor if needed:
     * delete will trigger a rebalance; if it needs to move
     * a node from one page to another, it will have to
     * update the parent's separator key(s). If the new sepkey
     * is larger than the current one, the parent page may
     * run out of space, triggering a split. We need this
     * cursor to be consistent until the end of the rebalance. */
    cx.outer.mc_next = txn->mt_cursors[dbi];
    txn->mt_cursors[dbi] = &cx.outer;
    rc = mdbx_cursor_del(&cx.outer, flags);
    txn->mt_cursors[dbi] = cx.outer.mc_next;
  }
  return rc;
}

/* Split a page and insert a new node.
 * Set MDBX_TXN_ERROR on failure.
 * [in,out] mc Cursor pointing to the page and desired insertion index.
 * The cursor will be updated to point to the actual page and index where
 * the node got inserted after the split.
 * [in] newkey The key for the newly inserted node.
 * [in] newdata The data for the newly inserted node.
 * [in] newpgno The page number, if the new node is a branch node.
 * [in] nflags The NODE_ADD_FLAGS for the new node.
 * Returns 0 on success, non-zero on failure. */
static int mdbx_page_split(MDBX_cursor *mc, const MDBX_val *newkey,
                           MDBX_val *newdata, pgno_t newpgno, unsigned nflags) {
  unsigned flags;
  int rc = MDBX_SUCCESS, foliage = 0, did_split = 0;
  pgno_t pgno = 0;
  unsigned i, ptop;
  MDBX_env *env = mc->mc_txn->mt_env;
  MDBX_node *node;
  MDBX_val sepkey, rkey, xdata;
  MDBX_page *copy = NULL;
  MDBX_page *rp, *pp;
  MDBX_cursor mn;
  DKBUF;

  MDBX_page *mp = mc->mc_pg[mc->mc_top];
  unsigned newindx = mc->mc_ki[mc->mc_top];
  unsigned nkeys = page_numkeys(mp);
  if (mdbx_audit_enabled()) {
    rc = mdbx_cursor_check(mc, true);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }

  mdbx_debug("-----> splitting %s page %" PRIaPGNO
             " and adding [%s] at index %i/%i",
             IS_LEAF(mp) ? "leaf" : "branch", mp->mp_pgno, DKEY(newkey),
             mc->mc_ki[mc->mc_top], nkeys);

  /* Create a right sibling. */
  if ((rc = mdbx_page_new(mc, mp->mp_flags, 1, &rp)))
    return rc;
  rp->mp_leaf2_ksize = mp->mp_leaf2_ksize;
  mdbx_debug("new right sibling: page %" PRIaPGNO, rp->mp_pgno);

  /* Usually when splitting the root page, the cursor
   * height is 1. But when called from mdbx_update_key,
   * the cursor height may be greater because it walks
   * up the stack while finding the branch slot to update. */
  if (mc->mc_top < 1) {
    if ((rc = mdbx_page_new(mc, P_BRANCH, 1, &pp)))
      goto done;
    /* shift current top to make room for new parent */
    mdbx_cassert(mc, mc->mc_snum < 2 && mc->mc_db->md_depth > 0);
    mc->mc_pg[2] = mc->mc_pg[1];
    mc->mc_ki[2] = mc->mc_ki[1];
    mc->mc_pg[1] = mc->mc_pg[0];
    mc->mc_ki[1] = mc->mc_ki[0];
    mc->mc_pg[0] = pp;
    mc->mc_ki[0] = 0;
    mc->mc_db->md_root = pp->mp_pgno;
    mdbx_debug("root split! new root = %" PRIaPGNO, pp->mp_pgno);
    foliage = mc->mc_db->md_depth++;

    /* Add left (implicit) pointer. */
    if (unlikely((rc = mdbx_node_add_branch(mc, 0, NULL, mp->mp_pgno)) !=
                 MDBX_SUCCESS)) {
      /* undo the pre-push */
      mc->mc_pg[0] = mc->mc_pg[1];
      mc->mc_ki[0] = mc->mc_ki[1];
      mc->mc_db->md_root = mp->mp_pgno;
      mc->mc_db->md_depth--;
      goto done;
    }
    mc->mc_snum++;
    mc->mc_top++;
    ptop = 0;
  } else {
    ptop = mc->mc_top - 1;
    mdbx_debug("parent branch page is %" PRIaPGNO, mc->mc_pg[ptop]->mp_pgno);
  }

  mdbx_cursor_copy(mc, &mn);
  mn.mc_xcursor = NULL;
  mn.mc_pg[mn.mc_top] = rp;
  mn.mc_ki[mn.mc_top] = 0;
  mn.mc_ki[ptop] = mc->mc_ki[ptop] + 1;

  unsigned split_indx;
  if (nflags & MDBX_APPEND) {
    mn.mc_ki[mn.mc_top] = 0;
    sepkey = *newkey;
    split_indx = newindx;
    nkeys = 0;
  } else {
    split_indx = (nkeys + 1) / 2;
    if (IS_LEAF2(rp)) {
      char *split, *ins;
      int x;
      unsigned lsize, rsize, ksize;
      /* Move half of the keys to the right sibling */
      x = mc->mc_ki[mc->mc_top] - split_indx;
      ksize = mc->mc_db->md_xsize;
      split = page_leaf2key(mp, split_indx, ksize);
      rsize = (nkeys - split_indx) * ksize;
      lsize = (nkeys - split_indx) * sizeof(indx_t);
      mdbx_cassert(mc, mp->mp_lower >= lsize);
      mp->mp_lower -= (indx_t)lsize;
      mdbx_cassert(mc, rp->mp_lower + lsize <= UINT16_MAX);
      rp->mp_lower += (indx_t)lsize;
      mdbx_cassert(mc, mp->mp_upper + rsize - lsize <= UINT16_MAX);
      mp->mp_upper += (indx_t)(rsize - lsize);
      mdbx_cassert(mc, rp->mp_upper >= rsize - lsize);
      rp->mp_upper -= (indx_t)(rsize - lsize);
      sepkey.iov_len = ksize;
      if (newindx == split_indx) {
        sepkey.iov_base = newkey->iov_base;
      } else {
        sepkey.iov_base = split;
      }
      if (x < 0) {
        mdbx_cassert(mc, ksize >= sizeof(indx_t));
        ins = page_leaf2key(mp, mc->mc_ki[mc->mc_top], ksize);
        memcpy(rp->mp_ptrs, split, rsize);
        sepkey.iov_base = rp->mp_ptrs;
        memmove(ins + ksize, ins, (split_indx - mc->mc_ki[mc->mc_top]) * ksize);
        memcpy(ins, newkey->iov_base, ksize);
        mdbx_cassert(mc, UINT16_MAX - mp->mp_lower >= (int)sizeof(indx_t));
        mp->mp_lower += sizeof(indx_t);
        mdbx_cassert(mc, mp->mp_upper >= ksize - sizeof(indx_t));
        mp->mp_upper -= (indx_t)(ksize - sizeof(indx_t));
      } else {
        if (x)
          memcpy(rp->mp_ptrs, split, x * ksize);
        ins = page_leaf2key(rp, x, ksize);
        memcpy(ins, newkey->iov_base, ksize);
        memcpy(ins + ksize, split + x * ksize, rsize - x * ksize);
        mdbx_cassert(mc, UINT16_MAX - rp->mp_lower >= (int)sizeof(indx_t));
        rp->mp_lower += sizeof(indx_t);
        mdbx_cassert(mc, rp->mp_upper >= ksize - sizeof(indx_t));
        rp->mp_upper -= (indx_t)(ksize - sizeof(indx_t));
        mdbx_cassert(mc, x <= UINT16_MAX);
        mc->mc_ki[mc->mc_top] = (indx_t)x;
      }
    } else {
      size_t psize, nsize, k;
      /* Maximum free space in an empty page */
      const unsigned pmax = page_space(env);
      nsize = IS_LEAF(mp) ? leaf_size(env, newkey, newdata)
                          : branch_size(env, newkey);

      /* grab a page to hold a temporary copy */
      copy = mdbx_page_malloc(mc->mc_txn, 1);
      if (unlikely(copy == NULL)) {
        rc = MDBX_ENOMEM;
        goto done;
      }
      copy->mp_pgno = mp->mp_pgno;
      copy->mp_flags = mp->mp_flags;
      copy->mp_lower = 0;
      copy->mp_upper = (indx_t)page_space(env);

      /* prepare to insert */
      for (unsigned j = i = 0; i < nkeys; i++) {
        if (i == newindx)
          copy->mp_ptrs[j++] = 0;
        copy->mp_ptrs[j++] = mp->mp_ptrs[i];
      }

      /* When items are relatively large the split point needs
       * to be checked, because being off-by-one will make the
       * difference between success or failure in mdbx_node_add.
       *
       * It's also relevant if a page happens to be laid out
       * such that one half of its nodes are all "small" and
       * the other half of its nodes are "large." If the new
       * item is also "large" and falls on the half with
       * "large" nodes, it also may not fit.
       *
       * As a final tweak, if the new item goes on the last
       * spot on the page (and thus, onto the new page), bias
       * the split so the new page is emptier than the old page.
       * This yields better packing during sequential inserts.
       */
      int dir;
      if (nkeys < 32 || nsize > pmax / 16 || newindx >= nkeys) {
        /* Find split point */
        psize = 0;
        if (newindx <= split_indx || newindx >= nkeys) {
          i = 0;
          dir = 1;
          k = (newindx >= nkeys) ? nkeys : split_indx + 1 + IS_LEAF(mp);
        } else {
          i = nkeys;
          dir = -1;
          k = split_indx - 1;
        }
        for (; i != k; i += dir) {
          if (i == newindx) {
            psize += nsize;
            node = NULL;
          } else {
            node = (MDBX_node *)((char *)mp + copy->mp_ptrs[i] + PAGEHDRSZ);
            psize += NODESIZE + node_ks(node) + sizeof(indx_t);
            if (IS_LEAF(mp)) {
              if (F_ISSET(node_flags(node), F_BIGDATA))
                psize += sizeof(pgno_t);
              else
                psize += node_ds(node);
            }
            psize = EVEN(psize);
          }
          if (psize > pmax || i == k - dir) {
            split_indx = i + (dir < 0);
            break;
          }
        }
      }
      if (split_indx == newindx) {
        sepkey.iov_len = newkey->iov_len;
        sepkey.iov_base = newkey->iov_base;
      } else {
        node =
            (MDBX_node *)((char *)mp + copy->mp_ptrs[split_indx] + PAGEHDRSZ);
        sepkey.iov_len = node_ks(node);
        sepkey.iov_base = node_key(node);
      }
    }
  }

  mdbx_debug("separator is %d [%s]", split_indx, DKEY(&sepkey));
  if (mdbx_audit_enabled()) {
    rc = mdbx_cursor_check(mc, true);
    if (unlikely(rc != MDBX_SUCCESS))
      goto done;
    rc = mdbx_cursor_check(&mn, true);
    if (unlikely(rc != MDBX_SUCCESS))
      goto done;
  }

  /* Copy separator key to the parent. */
  if (page_room(mn.mc_pg[ptop]) < branch_size(env, &sepkey)) {
    const int snum = mc->mc_snum;
    const int depth = mc->mc_db->md_depth;
    mn.mc_snum--;
    mn.mc_top--;
    did_split = 1;
    /* We want other splits to find mn when doing fixups */
    WITH_CURSOR_TRACKING(
        mn, rc = mdbx_page_split(&mn, &sepkey, NULL, rp->mp_pgno, 0));
    if (unlikely(rc != MDBX_SUCCESS))
      goto done;
    mdbx_cassert(mc, mc->mc_snum - snum == mc->mc_db->md_depth - depth);
    if (mdbx_audit_enabled()) {
      rc = mdbx_cursor_check(mc, true);
      if (unlikely(rc != MDBX_SUCCESS))
        goto done;
    }

    /* root split? */
    ptop += mc->mc_snum - snum;

    /* Right page might now have changed parent.
     * Check if left page also changed parent. */
    if (mn.mc_pg[ptop] != mc->mc_pg[ptop] &&
        mc->mc_ki[ptop] >= page_numkeys(mc->mc_pg[ptop])) {
      for (i = 0; i < ptop; i++) {
        mc->mc_pg[i] = mn.mc_pg[i];
        mc->mc_ki[i] = mn.mc_ki[i];
      }
      mc->mc_pg[ptop] = mn.mc_pg[ptop];
      if (mn.mc_ki[ptop]) {
        mc->mc_ki[ptop] = mn.mc_ki[ptop] - 1;
      } else {
        /* find right page's left sibling */
        mc->mc_ki[ptop] = mn.mc_ki[ptop];
        rc = mdbx_cursor_sibling(mc, false);
      }
    }
  } else {
    mn.mc_top--;
    rc = mdbx_node_add_branch(&mn, mn.mc_ki[ptop], &sepkey, rp->mp_pgno);
    mn.mc_top++;
  }
  if (unlikely(rc != MDBX_SUCCESS)) {
    if (rc == MDBX_NOTFOUND) /* improper mdbx_cursor_sibling() result */ {
      mdbx_error("unexpected %s", "MDBX_NOTFOUND");
      rc = MDBX_PROBLEM;
    }
    goto done;
  }

  if (nflags & MDBX_APPEND) {
    mc->mc_pg[mc->mc_top] = rp;
    mc->mc_ki[mc->mc_top] = 0;
    switch (PAGETYPE(rp)) {
    case P_BRANCH: {
      mdbx_cassert(mc, (nflags & (F_BIGDATA | F_SUBDATA | F_DUPDATA)) == 0);
      mdbx_cassert(mc, newpgno != 0 && newpgno != P_INVALID);
      rc = mdbx_node_add_branch(mc, 0, newkey, newpgno);
    } break;
    case P_LEAF: {
      mdbx_cassert(mc, newpgno == 0 || newpgno == P_INVALID);
      rc = mdbx_node_add_leaf(mc, 0, newkey, newdata, nflags);
    } break;
    case P_LEAF | P_LEAF2: {
      mdbx_cassert(mc, (nflags & (F_BIGDATA | F_SUBDATA | F_DUPDATA)) == 0);
      mdbx_cassert(mc, newpgno == 0 || newpgno == P_INVALID);
      rc = mdbx_node_add_leaf2(mc, 0, newkey);
    } break;
    default:
      rc = MDBX_CORRUPTED;
    }
    if (rc)
      goto done;
    for (i = 0; i < mc->mc_top; i++)
      mc->mc_ki[i] = mn.mc_ki[i];
  } else if (!IS_LEAF2(mp)) {
    /* Move nodes */
    mc->mc_pg[mc->mc_top] = rp;
    i = split_indx;
    indx_t n = 0;
    do {
      MDBX_val *rdata = NULL;
      if (i == newindx) {
        rkey.iov_base = newkey->iov_base;
        rkey.iov_len = newkey->iov_len;
        if (IS_LEAF(mp)) {
          rdata = newdata;
        } else
          pgno = newpgno;
        flags = nflags;
        /* Update index for the new key. */
        mc->mc_ki[mc->mc_top] = n;
      } else {
        node = (MDBX_node *)((char *)mp + copy->mp_ptrs[i] + PAGEHDRSZ);
        rkey.iov_base = node_key(node);
        rkey.iov_len = node_ks(node);
        if (IS_LEAF(mp)) {
          xdata.iov_base = node_data(node);
          xdata.iov_len = node_ds(node);
          rdata = &xdata;
        } else
          pgno = node_pgno(node);
        flags = node_flags(node);
      }

      switch (PAGETYPE(rp)) {
      case P_BRANCH: {
        mdbx_cassert(mc, 0 == (uint16_t)flags);
        if (n == 0) {
          /* First branch index doesn't need key data. */
          rkey.iov_len = 0;
        }
        rc = mdbx_node_add_branch(mc, n, &rkey, pgno);
      } break;
      case P_LEAF: {
        mdbx_cassert(mc, pgno == 0);
        mdbx_cassert(mc, rdata != NULL);
        rc = mdbx_node_add_leaf(mc, n, &rkey, rdata, flags);
      } break;
      /* case P_LEAF | P_LEAF2: {
        mdbx_cassert(mc, (nflags & (F_BIGDATA | F_SUBDATA | F_DUPDATA)) == 0);
        mdbx_cassert(mc, gno == 0);
        rc = mdbx_node_add_leaf2(mc, n, &rkey);
      } break; */
      default:
        rc = MDBX_CORRUPTED;
      }
      if (rc)
        goto done;

      if (i == nkeys) {
        i = 0;
        n = 0;
        mc->mc_pg[mc->mc_top] = copy;
      } else {
        i++;
        n++;
      }
    } while (i != split_indx);

    nkeys = page_numkeys(copy);
    for (i = 0; i < nkeys; i++)
      mp->mp_ptrs[i] = copy->mp_ptrs[i];
    mp->mp_lower = copy->mp_lower;
    mp->mp_upper = copy->mp_upper;
    memcpy(page_node(mp, nkeys - 1), page_node(copy, nkeys - 1),
           env->me_psize - copy->mp_upper - PAGEHDRSZ);

    /* reset back to original page */
    if (newindx < split_indx) {
      mc->mc_pg[mc->mc_top] = mp;
    } else {
      mc->mc_pg[mc->mc_top] = rp;
      mc->mc_ki[ptop]++;
      /* Make sure mc_ki is still valid. */
      if (mn.mc_pg[ptop] != mc->mc_pg[ptop] &&
          mc->mc_ki[ptop] >= page_numkeys(mc->mc_pg[ptop])) {
        for (i = 0; i <= ptop; i++) {
          mc->mc_pg[i] = mn.mc_pg[i];
          mc->mc_ki[i] = mn.mc_ki[i];
        }
      }
    }
    if (nflags & MDBX_RESERVE) {
      node = page_node(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top]);
      if (!(node_flags(node) & F_BIGDATA))
        newdata->iov_base = node_data(node);
    }
  } else {
    if (newindx >= split_indx) {
      mc->mc_pg[mc->mc_top] = rp;
      mc->mc_ki[ptop]++;
      /* Make sure mc_ki is still valid. */
      if (mn.mc_pg[ptop] != mc->mc_pg[ptop] &&
          mc->mc_ki[ptop] >= page_numkeys(mc->mc_pg[ptop])) {
        for (i = 0; i <= ptop; i++) {
          mc->mc_pg[i] = mn.mc_pg[i];
          mc->mc_ki[i] = mn.mc_ki[i];
        }
      }
    }
  }

  {
    /* Adjust other cursors pointing to mp */
    MDBX_cursor *m2, *m3;
    MDBX_dbi dbi = mc->mc_dbi;
    nkeys = page_numkeys(mp);

    for (m2 = mc->mc_txn->mt_cursors[dbi]; m2; m2 = m2->mc_next) {
      m3 = (mc->mc_flags & C_SUB) ? &m2->mc_xcursor->mx_cursor : m2;
      if (m3 == mc)
        continue;
      if (!(m2->mc_flags & m3->mc_flags & C_INITIALIZED))
        continue;
      if (foliage) {
        int k;
        /* sub cursors may be on different DB */
        if (m3->mc_pg[0] != mp)
          continue;
        /* root split */
        for (k = foliage; k >= 0; k--) {
          m3->mc_ki[k + 1] = m3->mc_ki[k];
          m3->mc_pg[k + 1] = m3->mc_pg[k];
        }
        m3->mc_ki[0] = (m3->mc_ki[0] >= nkeys) ? 1 : 0;
        m3->mc_pg[0] = mc->mc_pg[0];
        m3->mc_snum++;
        m3->mc_top++;
      }
      if (m3->mc_top >= mc->mc_top && m3->mc_pg[mc->mc_top] == mp) {
        if (m3->mc_ki[mc->mc_top] >= newindx && !(nflags & MDBX_SPLIT_REPLACE))
          m3->mc_ki[mc->mc_top]++;
        if (m3->mc_ki[mc->mc_top] >= nkeys) {
          m3->mc_pg[mc->mc_top] = rp;
          mdbx_cassert(mc, m3->mc_ki[mc->mc_top] >= nkeys);
          m3->mc_ki[mc->mc_top] -= (indx_t)nkeys;
          for (i = 0; i < mc->mc_top; i++) {
            m3->mc_ki[i] = mn.mc_ki[i];
            m3->mc_pg[i] = mn.mc_pg[i];
          }
        }
      } else if (!did_split && m3->mc_top >= ptop &&
                 m3->mc_pg[ptop] == mc->mc_pg[ptop] &&
                 m3->mc_ki[ptop] >= mc->mc_ki[ptop]) {
        m3->mc_ki[ptop]++;
      }
      if (XCURSOR_INITED(m3) && IS_LEAF(mp))
        XCURSOR_REFRESH(m3, m3->mc_pg[mc->mc_top], m3->mc_ki[mc->mc_top]);
    }
  }
  mdbx_debug("mp left: %d, rp left: %d", page_room(mp), page_room(rp));

done:
  if (copy) /* tmp page */
    mdbx_dpage_free(env, copy, 1);
  if (unlikely(rc))
    mc->mc_txn->mt_flags |= MDBX_TXN_ERROR;
  return rc;
}

int mdbx_put(MDBX_txn *txn, MDBX_dbi dbi, MDBX_val *key, MDBX_val *data,
             unsigned flags) {
  int rc = check_txn_rw(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!key || !data))
    return MDBX_EINVAL;

  if (unlikely(!TXN_DBI_EXIST(txn, dbi, DB_USRVALID)))
    return MDBX_EINVAL;

  if (unlikely(flags & ~(MDBX_NOOVERWRITE | MDBX_NODUPDATA | MDBX_RESERVE |
                         MDBX_APPEND | MDBX_APPENDDUP | MDBX_CURRENT)))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_flags & (MDBX_RDONLY | MDBX_TXN_BLOCKED)))
    return (txn->mt_flags & MDBX_RDONLY) ? MDBX_EACCESS : MDBX_BAD_TXN;

  MDBX_cursor_couple cx;
  rc = mdbx_cursor_init(&cx.outer, txn, dbi);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;
  cx.outer.mc_next = txn->mt_cursors[dbi];
  txn->mt_cursors[dbi] = &cx.outer;

  /* LY: support for update (explicit overwrite) */
  if (flags & MDBX_CURRENT) {
    rc = mdbx_cursor_get(&cx.outer, key, NULL, MDBX_SET);
    if (likely(rc == MDBX_SUCCESS) &&
        (txn->mt_dbs[dbi].md_flags & MDBX_DUPSORT)) {
      /* LY: allows update (explicit overwrite) only for unique keys */
      MDBX_node *node = page_node(cx.outer.mc_pg[cx.outer.mc_top],
                                  cx.outer.mc_ki[cx.outer.mc_top]);
      if (F_ISSET(node_flags(node), F_DUPDATA)) {
        mdbx_tassert(txn, XCURSOR_INITED(&cx.outer) &&
                              cx.outer.mc_xcursor->mx_db.md_entries > 1);
        rc = MDBX_EMULTIVAL;
      }
    }
  }

  if (likely(rc == MDBX_SUCCESS))
    rc = mdbx_cursor_put(&cx.outer, key, data, flags);
  txn->mt_cursors[dbi] = cx.outer.mc_next;

  return rc;
}

/**** COPYING *****************************************************************/

#ifndef MDBX_WBUF
#define MDBX_WBUF ((size_t)1024 * 1024)
#endif
#define MDBX_EOF 0x10 /* mdbx_env_copyfd1() is done reading */

/* State needed for a double-buffering compacting copy. */
typedef struct mdbx_copy {
  MDBX_env *mc_env;
  MDBX_txn *mc_txn;
  mdbx_condmutex_t mc_condmutex;
  uint8_t *mc_wbuf[2];
  uint8_t *mc_over[2];
  size_t mc_wlen[2];
  size_t mc_olen[2];
  mdbx_filehandle_t mc_fd;
  volatile int mc_error;
  pgno_t mc_next_pgno;
  short mc_toggle; /* Buffer number in provider */
  short mc_new;    /* (0-2 buffers to write) | (MDBX_EOF at end) */
  /* Error code.  Never cleared if set.  Both threads can set nonzero
   * to fail the copy.  Not mutex-protected, MDBX expects atomic int. */
} mdbx_copy;

/* Dedicated writer thread for compacting copy. */
static THREAD_RESULT __cold THREAD_CALL mdbx_env_copythr(void *arg) {
  mdbx_copy *my = arg;
  uint8_t *ptr;
  int toggle = 0;
  int rc;

  mdbx_condmutex_lock(&my->mc_condmutex);
  while (!my->mc_error) {
    while (!my->mc_new)
      mdbx_condmutex_wait(&my->mc_condmutex);
    if (my->mc_new == 0 + MDBX_EOF) /* 0 buffers, just EOF */
      break;
    size_t wsize = my->mc_wlen[toggle];
    ptr = my->mc_wbuf[toggle];
  again:
    if (wsize > 0 && !my->mc_error) {
      rc = mdbx_write(my->mc_fd, ptr, wsize);
      if (rc != MDBX_SUCCESS) {
        my->mc_error = rc;
        break;
      }
    }

    /* If there's an overflow page tail, write it too */
    if (my->mc_olen[toggle]) {
      wsize = my->mc_olen[toggle];
      ptr = my->mc_over[toggle];
      my->mc_olen[toggle] = 0;
      goto again;
    }
    my->mc_wlen[toggle] = 0;
    toggle ^= 1;
    /* Return the empty buffer to provider */
    my->mc_new--;
    mdbx_condmutex_signal(&my->mc_condmutex);
  }
  mdbx_condmutex_unlock(&my->mc_condmutex);
  return (THREAD_RESULT)0;
}

/* Give buffer and/or MDBX_EOF to writer thread, await unused buffer.
 *
 * [in] my control structure.
 * [in] adjust (1 to hand off 1 buffer) | (MDBX_EOF when ending). */
static int __cold mdbx_env_cthr_toggle(mdbx_copy *my, int adjust) {
  mdbx_condmutex_lock(&my->mc_condmutex);
  my->mc_new += (short)adjust;
  mdbx_condmutex_signal(&my->mc_condmutex);
  while (my->mc_new & 2) /* both buffers in use */
    mdbx_condmutex_wait(&my->mc_condmutex);
  mdbx_condmutex_unlock(&my->mc_condmutex);

  my->mc_toggle ^= (adjust & 1);
  /* Both threads reset mc_wlen, to be safe from threading errors */
  my->mc_wlen[my->mc_toggle] = 0;
  return my->mc_error;
}

/* Depth-first tree traversal for compacting copy.
 * [in] my control structure.
 * [in,out] pg database root.
 * [in] flags includes F_DUPDATA if it is a sorted-duplicate sub-DB. */
static int __cold mdbx_env_cwalk(mdbx_copy *my, pgno_t *pg, int flags) {
  MDBX_cursor mc;
  MDBX_page *mo, *mp, *leaf;
  char *buf, *ptr;
  int rc, toggle;
  unsigned i;

  /* Empty DB, nothing to do */
  if (*pg == P_INVALID)
    return MDBX_SUCCESS;

  memset(&mc, 0, sizeof(mc));
  mc.mc_snum = 1;
  mc.mc_txn = my->mc_txn;

  rc = mdbx_page_get(&mc, *pg, &mc.mc_pg[0], NULL);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;
  rc = mdbx_page_search_root(&mc, NULL, MDBX_PS_FIRST);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  /* Make cursor pages writable */
  buf = ptr = mdbx_malloc(pgno2bytes(my->mc_env, mc.mc_snum));
  if (buf == NULL)
    return MDBX_ENOMEM;

  for (i = 0; i < mc.mc_top; i++) {
    mdbx_page_copy((MDBX_page *)ptr, mc.mc_pg[i], my->mc_env->me_psize);
    mc.mc_pg[i] = (MDBX_page *)ptr;
    ptr += my->mc_env->me_psize;
  }

  /* This is writable space for a leaf page. Usually not needed. */
  leaf = (MDBX_page *)ptr;

  toggle = my->mc_toggle;
  while (mc.mc_snum > 0) {
    unsigned n;
    mp = mc.mc_pg[mc.mc_top];
    n = page_numkeys(mp);

    if (IS_LEAF(mp)) {
      if (!IS_LEAF2(mp) && !(flags & F_DUPDATA)) {
        for (i = 0; i < n; i++) {
          MDBX_node *node = page_node(mp, i);
          if (node_flags(node) & F_BIGDATA) {
            MDBX_page *omp;

            /* Need writable leaf */
            if (mp != leaf) {
              mc.mc_pg[mc.mc_top] = leaf;
              mdbx_page_copy(leaf, mp, my->mc_env->me_psize);
              mp = leaf;
              node = page_node(mp, i);
            }

            const pgno_t pgno = node_largedata_pgno(node);
            poke_pgno(node_data(node), my->mc_next_pgno);
            rc = mdbx_page_get(&mc, pgno, &omp, NULL);
            if (unlikely(rc != MDBX_SUCCESS))
              goto done;
            if (my->mc_wlen[toggle] >= MDBX_WBUF) {
              rc = mdbx_env_cthr_toggle(my, 1);
              if (unlikely(rc != MDBX_SUCCESS))
                goto done;
              toggle = my->mc_toggle;
            }
            mo = (MDBX_page *)(my->mc_wbuf[toggle] + my->mc_wlen[toggle]);
            memcpy(mo, omp, my->mc_env->me_psize);
            mo->mp_pgno = my->mc_next_pgno;
            my->mc_next_pgno += omp->mp_pages;
            my->mc_wlen[toggle] += my->mc_env->me_psize;
            if (omp->mp_pages > 1) {
              my->mc_olen[toggle] = pgno2bytes(my->mc_env, omp->mp_pages - 1);
              my->mc_over[toggle] = (uint8_t *)omp + my->mc_env->me_psize;
              rc = mdbx_env_cthr_toggle(my, 1);
              if (unlikely(rc != MDBX_SUCCESS))
                goto done;
              toggle = my->mc_toggle;
            }
          } else if (node_flags(node) & F_SUBDATA) {
            if (node_ds(node) < sizeof(MDBX_db)) {
              rc = MDBX_CORRUPTED;
              goto done;
            }

            /* Need writable leaf */
            if (mp != leaf) {
              mc.mc_pg[mc.mc_top] = leaf;
              mdbx_page_copy(leaf, mp, my->mc_env->me_psize);
              mp = leaf;
              node = page_node(mp, i);
            }

            MDBX_db db;
            memcpy(&db, node_data(node), sizeof(MDBX_db));
            my->mc_toggle = (short)toggle;
            rc = mdbx_env_cwalk(my, &db.md_root, node_flags(node) & F_DUPDATA);
            if (rc)
              goto done;
            toggle = my->mc_toggle;
            memcpy(node_data(node), &db, sizeof(MDBX_db));
          }
        }
      }
    } else {
      mc.mc_ki[mc.mc_top]++;
      if (mc.mc_ki[mc.mc_top] < n) {
      again:
        rc = mdbx_page_get(&mc, node_pgno(page_node(mp, mc.mc_ki[mc.mc_top])),
                           &mp, NULL);
        if (unlikely(rc != MDBX_SUCCESS))
          goto done;
        mc.mc_top++;
        mc.mc_snum++;
        mc.mc_ki[mc.mc_top] = 0;
        if (IS_BRANCH(mp)) {
          /* Whenever we advance to a sibling branch page,
           * we must proceed all the way down to its first leaf. */
          mdbx_page_copy(mc.mc_pg[mc.mc_top], mp, my->mc_env->me_psize);
          goto again;
        } else
          mc.mc_pg[mc.mc_top] = mp;
        continue;
      }
    }
    if (my->mc_wlen[toggle] >= MDBX_WBUF) {
      rc = mdbx_env_cthr_toggle(my, 1);
      if (unlikely(rc != MDBX_SUCCESS))
        goto done;
      toggle = my->mc_toggle;
    }
    mo = (MDBX_page *)(my->mc_wbuf[toggle] + my->mc_wlen[toggle]);
    mdbx_page_copy(mo, mp, my->mc_env->me_psize);
    mo->mp_pgno = my->mc_next_pgno++;
    my->mc_wlen[toggle] += my->mc_env->me_psize;
    if (mc.mc_top) {
      /* Update parent if there is one */
      node_set_pgno(page_node(mc.mc_pg[mc.mc_top - 1], mc.mc_ki[mc.mc_top - 1]),
                    mo->mp_pgno);
      mdbx_cursor_pop(&mc);
    } else {
      /* Otherwise we're done */
      *pg = mo->mp_pgno;
      break;
    }
  }
done:
  mdbx_free(buf);
  return rc;
}

static __cold void compact_fixup_meta(MDBX_env *env, MDBX_meta *meta) {
  /* Calculate filesize taking in account shrink/growing thresholds */
  if (meta->mm_geo.next > meta->mm_geo.now) {
    const pgno_t aligned = pgno_align2os_pgno(
        env,
        pgno_add(meta->mm_geo.next,
                 meta->mm_geo.grow - meta->mm_geo.next % meta->mm_geo.grow));
    meta->mm_geo.now = aligned;
  } else if (meta->mm_geo.next < meta->mm_geo.now) {
    meta->mm_geo.now = meta->mm_geo.next;
    const pgno_t aligner =
        meta->mm_geo.grow ? meta->mm_geo.grow : meta->mm_geo.shrink;
    const pgno_t aligned = pgno_align2os_pgno(
        env, meta->mm_geo.next + aligner - meta->mm_geo.next % aligner);
    meta->mm_geo.now = aligned;
  }

  if (meta->mm_geo.now < meta->mm_geo.lower)
    meta->mm_geo.now = meta->mm_geo.lower;
  if (meta->mm_geo.now > meta->mm_geo.upper)
    meta->mm_geo.now = meta->mm_geo.upper;

  /* Update signature */
  assert(meta->mm_geo.now >= meta->mm_geo.next);
  meta->mm_datasync_sign = mdbx_meta_sign(meta);
}

/* Copy environment with compaction. */
static int __cold mdbx_env_compact(MDBX_env *env, MDBX_txn *read_txn,
                                   mdbx_filehandle_t fd, uint8_t *buffer,
                                   const bool dest_is_pipe) {
  const size_t meta_bytes = pgno2bytes(env, NUM_METAS);
  uint8_t *const data_buffer =
      buffer + roundup_powerof2(meta_bytes, env->me_os_psize);
  MDBX_meta *const meta = mdbx_init_metas(env, buffer);
  /* copy canary sequenses if present */
  if (read_txn->mt_canary.v) {
    meta->mm_canary = read_txn->mt_canary;
    meta->mm_canary.v = mdbx_meta_txnid_stable(env, meta);
  }

  /* Set metapage 1 with current main DB */
  pgno_t new_root, root = read_txn->mt_dbs[MAIN_DBI].md_root;
  if ((new_root = root) == P_INVALID) {
    /* When the DB is empty, handle it specially to
     * fix any breakage like page leaks from ITS#8174. */
    meta->mm_dbs[MAIN_DBI].md_flags = read_txn->mt_dbs[MAIN_DBI].md_flags;
    compact_fixup_meta(env, meta);
    if (dest_is_pipe) {
      int rc = mdbx_write(fd, buffer, meta_bytes);
      if (rc != MDBX_SUCCESS)
        return rc;
    }
  } else {
    /* Count free pages + GC pages.  Subtract from last_pg
     * to find the new last_pg, which also becomes the new root. */
    pgno_t freecount = 0;
    MDBX_cursor mc;
    MDBX_val key, data;

    int rc = mdbx_cursor_init(&mc, read_txn, FREE_DBI);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    while ((rc = mdbx_cursor_get(&mc, &key, &data, MDBX_NEXT)) == 0)
      freecount += *(pgno_t *)data.iov_base;
    if (unlikely(rc != MDBX_NOTFOUND))
      return rc;

    freecount += read_txn->mt_dbs[FREE_DBI].md_branch_pages +
                 read_txn->mt_dbs[FREE_DBI].md_leaf_pages +
                 read_txn->mt_dbs[FREE_DBI].md_overflow_pages;

    new_root = read_txn->mt_next_pgno - 1 - freecount;
    meta->mm_geo.next = new_root + 1;
    meta->mm_dbs[MAIN_DBI] = read_txn->mt_dbs[MAIN_DBI];
    meta->mm_dbs[MAIN_DBI].md_root = new_root;

    mdbx_copy ctx;
    memset(&ctx, 0, sizeof(ctx));
    rc = mdbx_condmutex_init(&ctx.mc_condmutex);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;

    memset(data_buffer, 0, MDBX_WBUF * 2);
    ctx.mc_wbuf[0] = data_buffer;
    ctx.mc_wbuf[1] = data_buffer + MDBX_WBUF;
    ctx.mc_next_pgno = NUM_METAS;
    ctx.mc_env = env;
    ctx.mc_fd = fd;
    ctx.mc_txn = read_txn;

    mdbx_thread_t thread;
    int thread_err = mdbx_thread_create(&thread, mdbx_env_copythr, &ctx);
    if (likely(thread_err == MDBX_SUCCESS)) {
      if (dest_is_pipe) {
        compact_fixup_meta(env, meta);
        rc = mdbx_write(fd, buffer, meta_bytes);
      }
      if (rc == MDBX_SUCCESS)
        rc = mdbx_env_cwalk(&ctx, &root, 0);
      mdbx_env_cthr_toggle(&ctx, 1 | MDBX_EOF);
      thread_err = mdbx_thread_join(thread);
      mdbx_condmutex_destroy(&ctx.mc_condmutex);
    }
    if (unlikely(thread_err != MDBX_SUCCESS))
      return thread_err;
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    if (unlikely(ctx.mc_error != MDBX_SUCCESS))
      return ctx.mc_error;

    if (dest_is_pipe) {
      if (root != new_root) {
        mdbx_error("post-compactification root %" PRIaPGNO
                   " NE expected %" PRIaPGNO
                   " (source DB corrupted or has a page leak(s))",
                   root, new_root);
        return MDBX_CORRUPTED; /* page leak or corrupt DB */
      }
    } else {
      if (root > new_root) {
        mdbx_error("post-compactification root %" PRIaPGNO
                   " GT expected %" PRIaPGNO " (source DB corrupted)",
                   root, new_root);
        return MDBX_CORRUPTED; /* page leak or corrupt DB */
      }
      if (root < new_root) {
        mdbx_notice("post-compactification root %" PRIaPGNO
                    " LT expected %" PRIaPGNO " (page leak(s) in source DB)",
                    root, new_root);
        /* fixup meta */
        meta->mm_dbs[MAIN_DBI].md_root = root;
        meta->mm_geo.next = root + 1;
      }
      compact_fixup_meta(env, meta);
    }
  }

  /* Extend file if required */
  if (meta->mm_geo.now != meta->mm_geo.next) {
    const size_t whole_size = pgno2bytes(env, meta->mm_geo.now);
    if (!dest_is_pipe)
      return mdbx_ftruncate(fd, whole_size);

    const size_t used_size = pgno2bytes(env, meta->mm_geo.next);
    memset(data_buffer, 0, MDBX_WBUF);
    for (size_t offset = used_size; offset < whole_size;) {
      const size_t chunk =
          (MDBX_WBUF < whole_size - offset) ? MDBX_WBUF : whole_size - offset;
      /* copy to avoit EFAULT in case swapped-out */
      int rc = mdbx_write(fd, data_buffer, chunk);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
      offset += chunk;
    }
  }
  return MDBX_SUCCESS;
}

/* Copy environment as-is. */
static int __cold mdbx_env_copy_asis(MDBX_env *env, MDBX_txn *read_txn,
                                     mdbx_filehandle_t fd, uint8_t *buffer,
                                     const bool dest_is_pipe) {
  /* We must start the actual read txn after blocking writers */
  int rc = mdbx_txn_end(read_txn, MDBX_END_RESET_TMP);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  /* Temporarily block writers until we snapshot the meta pages */
  rc = mdbx_txn_lock(env, false);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  rc = mdbx_txn_renew0(read_txn, MDBX_RDONLY);
  if (unlikely(rc != MDBX_SUCCESS)) {
    mdbx_txn_unlock(env);
    return rc;
  }

  mdbx_jitter4testing(false);
  const size_t meta_bytes = pgno2bytes(env, NUM_METAS);
  /* Make a snapshot of meta-pages,
   * but writing ones after the data was flushed */
  memcpy(buffer, env->me_map, meta_bytes);
  MDBX_meta *const headcopy = /* LY: get pointer to the spanshot copy */
      (MDBX_meta *)(buffer + ((uint8_t *)mdbx_meta_head(env) - env->me_map));
  /* Update signature to steady */
  headcopy->mm_datasync_sign = mdbx_meta_sign(headcopy);
  mdbx_txn_unlock(env);

  /* Copy the data */
  const size_t whole_size = pgno_align2os_bytes(env, read_txn->mt_end_pgno);
  const size_t used_size = pgno2bytes(env, read_txn->mt_next_pgno);
  mdbx_jitter4testing(false);

  if (dest_is_pipe)
    rc = mdbx_write(fd, buffer, meta_bytes);

  uint8_t *const data_buffer =
      buffer + roundup_powerof2(meta_bytes, env->me_os_psize);
  for (size_t offset = meta_bytes; rc == MDBX_SUCCESS && offset < used_size;) {
    if (dest_is_pipe) {
#if defined(__linux__) || defined(__gnu_linux__)
      off_t in_offset = offset;
      const intptr_t written =
          sendfile(fd, env->me_fd, &in_offset, used_size - offset);
      if (unlikely(written <= 0)) {
        rc = written ? errno : MDBX_ENODATA;
        break;
      }
      offset = in_offset;
      continue;
#endif
    } else {
#if __GLIBC_PREREQ(2, 27) && defined(_GNU_SOURCE)
      off_t in_offset = offset, out_offset = offset;
      ssize_t bytes_copied = copy_file_range(
          env->me_fd, &in_offset, fd, &out_offset, used_size - offset, 0);
      if (unlikely(bytes_copied <= 0)) {
        rc = bytes_copied ? errno : MDBX_ENODATA;
        break;
      }
      offset = in_offset;
      continue;
#endif
    }

    /* fallback to portable */
    const size_t chunk =
        (MDBX_WBUF < used_size - offset) ? MDBX_WBUF : used_size - offset;
    /* copy to avoit EFAULT in case swapped-out */
    memcpy(data_buffer, env->me_map + offset, chunk);
    rc = mdbx_write(fd, data_buffer, chunk);
    offset += chunk;
  }

  /* Extend file if required */
  if (likely(rc == MDBX_SUCCESS) && whole_size != used_size) {
    if (!dest_is_pipe)
      rc = mdbx_ftruncate(fd, whole_size);
    else {
      memset(data_buffer, 0, MDBX_WBUF);
      for (size_t offset = used_size;
           rc == MDBX_SUCCESS && offset < whole_size;) {
        const size_t chunk =
            (MDBX_WBUF < whole_size - offset) ? MDBX_WBUF : whole_size - offset;
        /* copy to avoit EFAULT in case swapped-out */
        rc = mdbx_write(fd, data_buffer, chunk);
        offset += chunk;
      }
    }
  }

  return rc;
}

int __cold mdbx_env_copy2fd(MDBX_env *env, mdbx_filehandle_t fd,
                            unsigned flags) {
  if (unlikely(!env))
    return MDBX_EINVAL;

  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

  const int dest_is_pipe = mdbx_is_pipe(fd);
  if (MDBX_IS_ERROR(dest_is_pipe))
    return dest_is_pipe;

  if (!dest_is_pipe) {
    int rc = mdbx_fseek(fd, 0);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }

  const size_t buffer_size =
      pgno_align2os_bytes(env, NUM_METAS) +
      roundup_powerof2(((flags & MDBX_CP_COMPACT) ? MDBX_WBUF * 2 : MDBX_WBUF),
                       env->me_os_psize);

  uint8_t *buffer = NULL;
  int rc = mdbx_memalign_alloc(env->me_os_psize, buffer_size, (void **)&buffer);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  MDBX_txn *read_txn = NULL;
  /* Do the lock/unlock of the reader mutex before starting the
   * write txn. Otherwise other read txns could block writers. */
  rc = mdbx_txn_begin(env, NULL, MDBX_RDONLY, &read_txn);
  if (unlikely(rc != MDBX_SUCCESS)) {
    mdbx_memalign_free(buffer);
    return rc;
  }

  if (!dest_is_pipe) {
    /* Firstly write a stub to meta-pages.
     * Now we sure to incomplete copy will not be used. */
    memset(buffer, -1, pgno2bytes(env, NUM_METAS));
    rc = mdbx_write(fd, buffer, pgno2bytes(env, NUM_METAS));
  }

  if (likely(rc == MDBX_SUCCESS)) {
    memset(buffer, 0, pgno2bytes(env, NUM_METAS));
    rc = (flags & MDBX_CP_COMPACT)
             ? mdbx_env_compact(env, read_txn, fd, buffer, dest_is_pipe)
             : mdbx_env_copy_asis(env, read_txn, fd, buffer, dest_is_pipe);
  }
  mdbx_txn_abort(read_txn);

  if (!dest_is_pipe) {
    if (likely(rc == MDBX_SUCCESS))
      rc = mdbx_filesync(fd, MDBX_SYNC_DATA | MDBX_SYNC_SIZE | MDBX_SYNC_IODQ);

    /* Write actual meta */
    if (likely(rc == MDBX_SUCCESS))
      rc = mdbx_pwrite(fd, buffer, pgno2bytes(env, NUM_METAS), 0);

    if (likely(rc == MDBX_SUCCESS))
      rc = mdbx_filesync(fd, MDBX_SYNC_DATA | MDBX_SYNC_IODQ);
  }

  mdbx_memalign_free(buffer);
  return rc;
}

int __cold mdbx_env_copy(MDBX_env *env, const char *dest_path, unsigned flags) {
  if (unlikely(!env || !dest_path))
    return MDBX_EINVAL;

  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

  char *dxb_pathname;
  mdbx_filehandle_t newfd = INVALID_HANDLE_VALUE;

  if (env->me_flags & MDBX_NOSUBDIR) {
    dxb_pathname = (char *)dest_path;
  } else {
    size_t len = strlen(dest_path);
    len += sizeof(MDBX_DATANAME);
    dxb_pathname = mdbx_malloc(len);
    if (!dxb_pathname)
      return MDBX_ENOMEM;
    sprintf(dxb_pathname, "%s" MDBX_DATANAME, dest_path);
  }

  /* The destination path must exist, but the destination file must not.
   * We don't want the OS to cache the writes, since the source data is
   * already in the OS cache. */
  int rc = mdbx_openfile(dxb_pathname, O_WRONLY | O_CREAT | O_EXCL, 0640,
                         &newfd, true);
  if (rc == MDBX_SUCCESS) {
    if (env->me_psize >= env->me_os_psize) {
#ifdef F_NOCACHE /* __APPLE__ */
      (void)fcntl(newfd, F_NOCACHE, 1);
#elif defined(O_DIRECT) && defined(F_GETFL)
      /* Set O_DIRECT if the file system supports it */
      if ((rc = fcntl(newfd, F_GETFL)) != -1)
        (void)fcntl(newfd, F_SETFL, rc | O_DIRECT);
#endif
    }
    rc = mdbx_env_copy2fd(env, newfd, flags);
  }

  if (newfd != INVALID_HANDLE_VALUE) {
    int err = mdbx_closefile(newfd);
    if (rc == MDBX_SUCCESS && err != rc)
      rc = err;
    if (rc != MDBX_SUCCESS)
      (void)mdbx_removefile(dxb_pathname);
  }

  if (dxb_pathname != dest_path)
    mdbx_free(dxb_pathname);

  return rc;
}

/******************************************************************************/

int __cold mdbx_env_set_flags(MDBX_env *env, unsigned flags, int onoff) {
  if (unlikely(!env))
    return MDBX_EINVAL;

  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(flags & ~CHANGEABLE))
    return MDBX_EPERM;

  if (unlikely(env->me_flags & MDBX_RDONLY))
    return MDBX_EACCESS;

  if (unlikely(env->me_txn0->mt_owner == mdbx_thread_self()))
    return MDBX_BUSY;

  int rc = mdbx_txn_lock(env, false);
  if (unlikely(rc))
    return rc;

  if (onoff)
    env->me_flags |= flags;
  else
    env->me_flags &= ~flags;

  mdbx_txn_unlock(env);
  return MDBX_SUCCESS;
}

int __cold mdbx_env_get_flags(MDBX_env *env, unsigned *arg) {
  if (unlikely(!env || !arg))
    return MDBX_EINVAL;

  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

  *arg = env->me_flags & (CHANGEABLE | CHANGELESS);
  return MDBX_SUCCESS;
}

int __cold mdbx_env_set_userctx(MDBX_env *env, void *ctx) {
  if (unlikely(!env))
    return MDBX_EINVAL;

  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

  env->me_userctx = ctx;
  return MDBX_SUCCESS;
}

void *__cold mdbx_env_get_userctx(MDBX_env *env) {
  return env ? env->me_userctx : NULL;
}

int __cold mdbx_env_set_assert(MDBX_env *env, MDBX_assert_func *func) {
  if (unlikely(!env))
    return MDBX_EINVAL;

  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

#if MDBX_DEBUG
  env->me_assert_func = func;
  return MDBX_SUCCESS;
#else
  (void)func;
  return MDBX_ENOSYS;
#endif
}

int __cold mdbx_env_get_path(MDBX_env *env, const char **arg) {
  if (unlikely(!env || !arg))
    return MDBX_EINVAL;

  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

  *arg = env->me_path;
  return MDBX_SUCCESS;
}

int __cold mdbx_env_get_fd(MDBX_env *env, mdbx_filehandle_t *arg) {
  if (unlikely(!env || !arg))
    return MDBX_EINVAL;

  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

  *arg = env->me_fd;
  return MDBX_SUCCESS;
}

/* Common code for mdbx_dbi_stat() and mdbx_env_stat().
 * [in] env the environment to operate in.
 * [in] db the MDBX_db record containing the stats to return.
 * [out] arg the address of an MDBX_stat structure to receive the stats.
 * Returns 0, this function always succeeds. */
static void mdbx_stat0(const MDBX_env *env, const MDBX_db *db, MDBX_stat *dest,
                       size_t bytes) {
  dest->ms_psize = env->me_psize;
  dest->ms_depth = db->md_depth;
  dest->ms_branch_pages = db->md_branch_pages;
  dest->ms_leaf_pages = db->md_leaf_pages;
  dest->ms_overflow_pages = db->md_overflow_pages;
  dest->ms_entries = db->md_entries;
  if (likely(bytes >=
             offsetof(MDBX_stat, ms_mod_txnid) + sizeof(dest->ms_mod_txnid)))
    dest->ms_mod_txnid = db->md_mod_txnid;
}

int __cold mdbx_env_stat(MDBX_env *env, MDBX_stat *dest, size_t bytes) {
  return mdbx_env_stat_ex(env, NULL, dest, bytes);
}

int __cold mdbx_env_stat_ex(const MDBX_env *env, const MDBX_txn *txn,
                            MDBX_stat *dest, size_t bytes) {
  if (unlikely((env == NULL && txn == NULL) || dest == NULL))
    return MDBX_EINVAL;

  if (txn) {
    int err = check_txn(txn, MDBX_TXN_BLOCKED);
    if (unlikely(err != MDBX_SUCCESS))
      return err;
  }
  if (env) {
    if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
      return MDBX_EBADSIGN;
    if (txn && unlikely(txn->mt_env != env))
      return MDBX_EINVAL;
  }

  const size_t size_before_modtxnid = offsetof(MDBX_stat, ms_mod_txnid);
  if (unlikely(bytes != sizeof(MDBX_stat)) && bytes != size_before_modtxnid)
    return MDBX_EINVAL;

  if (txn) {
    mdbx_stat0(txn->mt_env, &txn->mt_dbs[MAIN_DBI], dest, bytes);
    return MDBX_SUCCESS;
  }

  while (1) {
    const MDBX_meta *const recent_meta = mdbx_meta_head(env);
    const txnid_t txnid = mdbx_meta_txnid_fluid(env, recent_meta);
    mdbx_stat0(env, &recent_meta->mm_dbs[MAIN_DBI], dest, bytes);
    mdbx_compiler_barrier();
    if (likely(txnid == mdbx_meta_txnid_fluid(env, recent_meta) &&
               recent_meta == mdbx_meta_head(env)))
      return MDBX_SUCCESS;
  }
}

int __cold mdbx_env_info(MDBX_env *env, MDBX_envinfo *arg, size_t bytes) {
  return mdbx_env_info_ex(env, NULL, arg, bytes);
}

int __cold mdbx_env_info_ex(const MDBX_env *env, const MDBX_txn *txn,
                            MDBX_envinfo *arg, size_t bytes) {
  if (unlikely((env == NULL && txn == NULL) || arg == NULL))
    return MDBX_EINVAL;

  if (txn) {
    int err = check_txn(txn, MDBX_TXN_BLOCKED);
    if (unlikely(err != MDBX_SUCCESS))
      return err;
  }
  if (env) {
    if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
      return MDBX_EBADSIGN;
    if (txn && unlikely(txn->mt_env != env))
      return MDBX_EINVAL;
  } else {
    env = txn->mt_env;
  }

  const size_t size_before_bootid = offsetof(MDBX_envinfo, mi_bootid);
  if (unlikely(bytes != sizeof(MDBX_envinfo)) && bytes != size_before_bootid)
    return MDBX_EINVAL;

  const MDBX_meta *const meta0 = METAPAGE(env, 0);
  const MDBX_meta *const meta1 = METAPAGE(env, 1);
  const MDBX_meta *const meta2 = METAPAGE(env, 2);
  pgno_t unsynced_pages;
  while (1) {
    if (unlikely(env->me_flags & MDBX_FATAL_ERROR))
      return MDBX_PANIC;

    const MDBX_meta *const recent_meta = mdbx_meta_head(env);
    arg->mi_recent_txnid = mdbx_meta_txnid_fluid(env, recent_meta);
    arg->mi_meta0_txnid = mdbx_meta_txnid_fluid(env, meta0);
    arg->mi_meta0_sign = meta0->mm_datasync_sign;
    arg->mi_meta1_txnid = mdbx_meta_txnid_fluid(env, meta1);
    arg->mi_meta1_sign = meta1->mm_datasync_sign;
    arg->mi_meta2_txnid = mdbx_meta_txnid_fluid(env, meta2);
    arg->mi_meta2_sign = meta2->mm_datasync_sign;

    const MDBX_meta *txn_meta = recent_meta;
    arg->mi_last_pgno = txn_meta->mm_geo.next - 1;
    arg->mi_geo.current = pgno2bytes(env, txn_meta->mm_geo.now);
    if (txn) {
      arg->mi_last_pgno = txn->mt_next_pgno - 1;
      arg->mi_geo.current = pgno2bytes(env, txn->mt_end_pgno);

      const txnid_t wanna_meta_txnid = (txn->mt_flags & MDBX_RDONLY)
                                           ? txn->mt_txnid
                                           : txn->mt_txnid - MDBX_TXNID_STEP;
      txn_meta = (arg->mi_meta0_txnid == wanna_meta_txnid) ? meta0 : txn_meta;
      txn_meta = (arg->mi_meta1_txnid == wanna_meta_txnid) ? meta1 : txn_meta;
      txn_meta = (arg->mi_meta2_txnid == wanna_meta_txnid) ? meta2 : txn_meta;
    }
    arg->mi_geo.lower = pgno2bytes(env, txn_meta->mm_geo.lower);
    arg->mi_geo.upper = pgno2bytes(env, txn_meta->mm_geo.upper);
    arg->mi_geo.shrink = pgno2bytes(env, txn_meta->mm_geo.shrink);
    arg->mi_geo.grow = pgno2bytes(env, txn_meta->mm_geo.grow);
    unsynced_pages = *env->me_unsynced_pages +
                     (*env->me_meta_sync_txnid != (uint32_t)arg->mi_last_pgno);

    arg->mi_mapsize = env->me_dxb_mmap.limit;
    mdbx_compiler_barrier();
    if (likely(arg->mi_meta0_txnid == mdbx_meta_txnid_fluid(env, meta0) &&
               arg->mi_meta0_sign == meta0->mm_datasync_sign &&
               arg->mi_meta1_txnid == mdbx_meta_txnid_fluid(env, meta1) &&
               arg->mi_meta1_sign == meta1->mm_datasync_sign &&
               arg->mi_meta2_txnid == mdbx_meta_txnid_fluid(env, meta2) &&
               arg->mi_meta2_sign == meta2->mm_datasync_sign &&
               recent_meta == mdbx_meta_head(env) &&
               arg->mi_recent_txnid == mdbx_meta_txnid_fluid(env, recent_meta)))
      break;
  }

  arg->mi_maxreaders = env->me_maxreaders;
  arg->mi_numreaders = env->me_lck ? env->me_lck->mti_numreaders : INT32_MAX;
  arg->mi_dxb_pagesize = env->me_psize;
  arg->mi_sys_pagesize = env->me_os_psize;

  const MDBX_lockinfo *const lck = env->me_lck;
  if (likely(bytes > size_before_bootid)) {
    arg->mi_unsync_volume = pgno2bytes(env, unsynced_pages);
    const uint64_t monotime_now = mdbx_osal_monotime();
    arg->mi_since_sync_seconds16dot16 =
        mdbx_osal_monotime_to_16dot16(monotime_now - *env->me_sync_timestamp);
    arg->mi_since_reader_check_seconds16dot16 =
        lck ? mdbx_osal_monotime_to_16dot16(monotime_now -
                                            lck->mti_reader_check_timestamp)
            : 0;
    arg->mi_autosync_threshold = pgno2bytes(env, *env->me_autosync_threshold);
    arg->mi_autosync_period_seconds16dot16 =
        mdbx_osal_monotime_to_16dot16(*env->me_autosync_period);
    arg->mi_bootid[0] = lck ? lck->mti_bootid.x : 0;
    arg->mi_bootid[1] = lck ? lck->mti_bootid.y : 0;
    arg->mi_mode = lck ? lck->mti_envmode : env->me_flags;
  }

  arg->mi_self_latter_reader_txnid = arg->mi_latter_reader_txnid = 0;
  if (lck) {
    arg->mi_self_latter_reader_txnid = arg->mi_latter_reader_txnid =
        arg->mi_recent_txnid;
    for (unsigned i = 0; i < arg->mi_numreaders; ++i) {
      const uint32_t pid = lck->mti_readers[i].mr_pid;
      if (pid) {
        const txnid_t txnid = safe64_read(&lck->mti_readers[i].mr_txnid);
        if (arg->mi_latter_reader_txnid > txnid)
          arg->mi_latter_reader_txnid = txnid;
        if (pid == env->me_pid && arg->mi_self_latter_reader_txnid > txnid)
          arg->mi_self_latter_reader_txnid = txnid;
      }
    }
  }

  return MDBX_SUCCESS;
}

static MDBX_cmp_func *mdbx_default_keycmp(unsigned flags) {
  return (flags & MDBX_REVERSEKEY)
             ? mdbx_cmp_memnr
             : (flags & MDBX_INTEGERKEY) ? mdbx_cmp_int_align2 : mdbx_cmp_memn;
}

static MDBX_cmp_func *mdbx_default_datacmp(unsigned flags) {
  return !(flags & MDBX_DUPSORT)
             ? mdbx_cmp_memn
             : ((flags & MDBX_INTEGERDUP)
                    ? mdbx_cmp_int_unaligned
                    : ((flags & MDBX_REVERSEDUP) ? mdbx_cmp_memnr
                                                 : mdbx_cmp_memn));
}

static int mdbx_dbi_bind(MDBX_txn *txn, const MDBX_dbi dbi, unsigned user_flags,
                         MDBX_cmp_func *keycmp, MDBX_cmp_func *datacmp) {
  /* LY: so, accepting only three cases for the table's flags:
   * 1) user_flags and both comparators are zero
   *    = assume that a by-default mode/flags is requested for reading;
   * 2) user_flags exactly the same
   *    = assume that the target mode/flags are requested properly;
   * 3) user_flags differs, but table is empty and MDBX_CREATE is provided
   *    = assume that a properly create request with custom flags;
   */
  if ((user_flags ^ txn->mt_dbs[dbi].md_flags) & PERSISTENT_FLAGS) {
    /* flags ara differs, check other conditions */
    if (!user_flags && (!keycmp || keycmp == txn->mt_dbxs[dbi].md_cmp) &&
        (!datacmp || datacmp == txn->mt_dbxs[dbi].md_dcmp)) {
      /* no comparators were provided and flags are zero,
       * seems that is case #1 above */
      user_flags = txn->mt_dbs[dbi].md_flags;
    } else if ((user_flags & MDBX_CREATE) && txn->mt_dbs[dbi].md_entries == 0) {
      if (txn->mt_flags & MDBX_RDONLY)
        return /* FIXME: return extended info */ MDBX_EACCESS;
      /* make sure flags changes get committed */
      txn->mt_dbs[dbi].md_flags = user_flags & PERSISTENT_FLAGS;
      txn->mt_flags |= MDBX_TXN_DIRTY;
    } else {
      return /* FIXME: return extended info */ MDBX_INCOMPATIBLE;
    }
  }

  if (!txn->mt_dbxs[dbi].md_cmp || MDBX_DEBUG) {
    if (!keycmp)
      keycmp = mdbx_default_keycmp(user_flags);
    mdbx_tassert(txn, !txn->mt_dbxs[dbi].md_cmp ||
                          txn->mt_dbxs[dbi].md_cmp == keycmp);
    txn->mt_dbxs[dbi].md_cmp = keycmp;
  }

  if (!txn->mt_dbxs[dbi].md_dcmp || MDBX_DEBUG) {
    if (!datacmp)
      datacmp = mdbx_default_datacmp(user_flags);
    mdbx_tassert(txn, !txn->mt_dbxs[dbi].md_dcmp ||
                          txn->mt_dbxs[dbi].md_dcmp == datacmp);
    txn->mt_dbxs[dbi].md_dcmp = datacmp;
  }

  return MDBX_SUCCESS;
}

int mdbx_dbi_open_ex(MDBX_txn *txn, const char *table_name, unsigned user_flags,
                     MDBX_dbi *dbi, MDBX_cmp_func *keycmp,
                     MDBX_cmp_func *datacmp) {
  int rc = check_txn(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!dbi || (user_flags & ~VALID_FLAGS) != 0))
    return MDBX_EINVAL;

  switch (user_flags &
          (MDBX_INTEGERDUP | MDBX_DUPFIXED | MDBX_DUPSORT | MDBX_REVERSEDUP)) {
  default:
    return MDBX_EINVAL;
  case MDBX_DUPSORT:
  case MDBX_DUPSORT | MDBX_REVERSEDUP:
  case MDBX_DUPSORT | MDBX_DUPFIXED:
  case MDBX_DUPSORT | MDBX_DUPFIXED | MDBX_REVERSEDUP:
  case MDBX_DUPSORT | MDBX_DUPFIXED | MDBX_INTEGERDUP:
  case MDBX_DUPSORT | MDBX_DUPFIXED | MDBX_INTEGERDUP | MDBX_REVERSEDUP:
  case 0:
    break;
  }

  /* main table? */
  if (!table_name) {
    *dbi = MAIN_DBI;
    return mdbx_dbi_bind(txn, MAIN_DBI, user_flags, keycmp, datacmp);
  }

  if (txn->mt_dbxs[MAIN_DBI].md_cmp == NULL) {
    txn->mt_dbxs[MAIN_DBI].md_cmp =
        mdbx_default_keycmp(txn->mt_dbs[MAIN_DBI].md_flags);
    txn->mt_dbxs[MAIN_DBI].md_dcmp =
        mdbx_default_datacmp(txn->mt_dbs[MAIN_DBI].md_flags);
  }

  /* Is the DB already open? */
  size_t len = strlen(table_name);
  MDBX_dbi scan, slot;
  for (slot = scan = txn->mt_numdbs; --scan >= CORE_DBS;) {
    if (!txn->mt_dbxs[scan].md_name.iov_len) {
      /* Remember this free slot */
      slot = scan;
      continue;
    }
    if (len == txn->mt_dbxs[scan].md_name.iov_len &&
        !strncmp(table_name, txn->mt_dbxs[scan].md_name.iov_base, len)) {
      *dbi = scan;
      return mdbx_dbi_bind(txn, scan, user_flags, keycmp, datacmp);
    }
  }

  /* Fail, if no free slot and max hit */
  MDBX_env *env = txn->mt_env;
  if (unlikely(slot >= env->me_maxdbs))
    return MDBX_DBS_FULL;

  /* Cannot mix named table with some main-table flags */
  if (unlikely(txn->mt_dbs[MAIN_DBI].md_flags &
               (MDBX_DUPSORT | MDBX_INTEGERKEY)))
    return (user_flags & MDBX_CREATE) ? MDBX_INCOMPATIBLE : MDBX_NOTFOUND;

  /* Find the DB info */
  int exact = 0;
  MDBX_val key, data;
  key.iov_len = len;
  key.iov_base = (void *)table_name;
  MDBX_cursor mc;
  rc = mdbx_cursor_init(&mc, txn, MAIN_DBI);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;
  rc = mdbx_cursor_set(&mc, &key, &data, MDBX_SET, &exact);
  if (unlikely(rc != MDBX_SUCCESS)) {
    if (rc != MDBX_NOTFOUND || !(user_flags & MDBX_CREATE))
      return rc;
  } else {
    /* make sure this is actually a table */
    MDBX_node *node = page_node(mc.mc_pg[mc.mc_top], mc.mc_ki[mc.mc_top]);
    if (unlikely((node_flags(node) & (F_DUPDATA | F_SUBDATA)) != F_SUBDATA))
      return MDBX_INCOMPATIBLE;
    if (unlikely(data.iov_len < sizeof(MDBX_db)))
      return MDBX_CORRUPTED;
  }

  if (rc != MDBX_SUCCESS && unlikely(txn->mt_flags & MDBX_RDONLY))
    return MDBX_EACCESS;

  /* Done here so we cannot fail after creating a new DB */
  char *namedup = mdbx_strdup(table_name);
  if (unlikely(!namedup))
    return MDBX_ENOMEM;

  int err = mdbx_fastmutex_acquire(&env->me_dbi_lock);
  if (unlikely(err != MDBX_SUCCESS)) {
    mdbx_free(namedup);
    return err;
  }

  if (txn->mt_numdbs < env->me_numdbs) {
    for (unsigned i = txn->mt_numdbs; i < env->me_numdbs; ++i) {
      txn->mt_dbflags[i] = 0;
      if (env->me_dbflags[i] & MDBX_VALID) {
        txn->mt_dbs[i].md_flags = env->me_dbflags[i] & PERSISTENT_FLAGS;
        txn->mt_dbflags[i] = DB_VALID | DB_USRVALID | DB_STALE;
        mdbx_tassert(txn, txn->mt_dbxs[i].md_cmp != NULL);
      }
    }
    txn->mt_numdbs = env->me_numdbs;
  }

  for (slot = scan = txn->mt_numdbs; --scan >= CORE_DBS;) {
    if (!txn->mt_dbxs[scan].md_name.iov_len) {
      /* Remember this free slot */
      slot = scan;
      continue;
    }
    if (len == txn->mt_dbxs[scan].md_name.iov_len &&
        !strncmp(table_name, txn->mt_dbxs[scan].md_name.iov_base, len)) {
      *dbi = scan;
      rc = mdbx_dbi_bind(txn, scan, user_flags, keycmp, datacmp);
      goto bailout;
    }
  }

  if (unlikely(slot >= env->me_maxdbs)) {
    rc = MDBX_DBS_FULL;
    goto bailout;
  }

  unsigned dbflag = DB_FRESH | DB_VALID | DB_USRVALID;
  MDBX_db db_dummy;
  if (unlikely(rc)) {
    /* MDBX_NOTFOUND and MDBX_CREATE: Create new DB */
    mdbx_tassert(txn, rc == MDBX_NOTFOUND);
    memset(&db_dummy, 0, sizeof(db_dummy));
    db_dummy.md_root = P_INVALID;
    db_dummy.md_flags = user_flags & PERSISTENT_FLAGS;
    data.iov_len = sizeof(db_dummy);
    data.iov_base = &db_dummy;
    WITH_CURSOR_TRACKING(
        mc,
        rc = mdbx_cursor_put(&mc, &key, &data, F_SUBDATA | MDBX_NOOVERWRITE));

    if (unlikely(rc != MDBX_SUCCESS))
      goto bailout;

    dbflag |= DB_DIRTY | DB_CREAT;
  }

  /* Got info, register DBI in this txn */
  txn->mt_dbxs[slot].md_cmp = nullptr;
  txn->mt_dbxs[slot].md_dcmp = nullptr;
  txn->mt_dbs[slot] = *(MDBX_db *)data.iov_base;
  env->me_dbflags[slot] = 0;
  rc = mdbx_dbi_bind(txn, slot, user_flags, keycmp, datacmp);
  if (unlikely(rc != MDBX_SUCCESS)) {
    mdbx_tassert(txn, (dbflag & DB_CREAT) == 0);
  bailout:
    mdbx_free(namedup);
  } else {
    txn->mt_dbflags[slot] = (uint8_t)dbflag;
    txn->mt_dbxs[slot].md_name.iov_base = namedup;
    txn->mt_dbxs[slot].md_name.iov_len = len;
    txn->mt_numdbs += (slot == txn->mt_numdbs);
    if ((dbflag & DB_CREAT) == 0) {
      env->me_dbflags[slot] = txn->mt_dbs[slot].md_flags | MDBX_VALID;
      mdbx_compiler_barrier();
      if (env->me_numdbs <= slot)
        env->me_numdbs = slot + 1;
    } else {
      env->me_dbiseqs[slot] += 1;
    }
    txn->mt_dbiseqs[slot] = env->me_dbiseqs[slot];
    *dbi = slot;
  }

  mdbx_ensure(env, mdbx_fastmutex_release(&env->me_dbi_lock) == MDBX_SUCCESS);
  return rc;
}

int mdbx_dbi_open(MDBX_txn *txn, const char *table_name, unsigned table_flags,
                  MDBX_dbi *dbi) {
  return mdbx_dbi_open_ex(txn, table_name, table_flags, dbi, nullptr, nullptr);
}

int __cold mdbx_dbi_stat(MDBX_txn *txn, MDBX_dbi dbi, MDBX_stat *dest,
                         size_t bytes) {
  int rc = check_txn(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!dest))
    return MDBX_EINVAL;

  if (unlikely(!TXN_DBI_EXIST(txn, dbi, DB_VALID)))
    return MDBX_EINVAL;

  const size_t size_before_modtxnid = offsetof(MDBX_stat, ms_mod_txnid);
  if (unlikely(bytes != sizeof(MDBX_stat)) && bytes != size_before_modtxnid)
    return MDBX_EINVAL;

  if (unlikely(txn->mt_flags & MDBX_TXN_BLOCKED))
    return MDBX_BAD_TXN;

  if (unlikely(txn->mt_dbflags[dbi] & DB_STALE)) {
    rc = mdbx_fetch_sdb(txn, dbi);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }
  mdbx_stat0(txn->mt_env, &txn->mt_dbs[dbi], dest, bytes);
  return MDBX_SUCCESS;
}

static int mdbx_dbi_close_locked(MDBX_env *env, MDBX_dbi dbi) {
  if (unlikely(dbi < CORE_DBS || dbi >= env->me_maxdbs))
    return MDBX_EINVAL;

  char *ptr = env->me_dbxs[dbi].md_name.iov_base;
  /* If there was no name, this was already closed */
  if (unlikely(!ptr))
    return MDBX_BAD_DBI;

  env->me_dbflags[dbi] = 0;
  env->me_dbxs[dbi].md_name.iov_len = 0;
  mdbx_compiler_barrier();
  env->me_dbiseqs[dbi]++;
  env->me_dbxs[dbi].md_name.iov_base = NULL;
  mdbx_free(ptr);
  return MDBX_SUCCESS;
}

int mdbx_dbi_close(MDBX_env *env, MDBX_dbi dbi) {
  if (unlikely(!env))
    return MDBX_EINVAL;

  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(dbi < CORE_DBS || dbi >= env->me_maxdbs))
    return MDBX_EINVAL;

  int rc = mdbx_fastmutex_acquire(&env->me_dbi_lock);
  if (likely(rc == MDBX_SUCCESS)) {
    rc = mdbx_dbi_close_locked(env, dbi);
    mdbx_ensure(env, mdbx_fastmutex_release(&env->me_dbi_lock) == MDBX_SUCCESS);
  }
  return rc;
}

int mdbx_dbi_flags_ex(MDBX_txn *txn, MDBX_dbi dbi, unsigned *flags,
                      unsigned *state) {
  int rc = check_txn(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!flags || !state))
    return MDBX_EINVAL;

  if (unlikely(!TXN_DBI_EXIST(txn, dbi, DB_VALID)))
    return MDBX_EINVAL;

  *flags = txn->mt_dbs[dbi].md_flags & PERSISTENT_FLAGS;
  *state = txn->mt_dbflags[dbi] & (DB_FRESH | DB_CREAT | DB_DIRTY | DB_STALE);

  return MDBX_SUCCESS;
}

int mdbx_dbi_flags(MDBX_txn *txn, MDBX_dbi dbi, unsigned *flags) {
  unsigned state;
  return mdbx_dbi_flags_ex(txn, dbi, flags, &state);
}

/* Add all the DB's pages to the free list.
 * [in] mc Cursor on the DB to free.
 * [in] subs non-Zero to check for sub-DBs in this DB.
 * Returns 0 on success, non-zero on failure. */
static int mdbx_drop0(MDBX_cursor *mc, int subs) {
  int rc = mdbx_page_search(mc, NULL, MDBX_PS_FIRST);
  if (likely(rc == MDBX_SUCCESS)) {
    MDBX_txn *txn = mc->mc_txn;
    MDBX_cursor mx;
    unsigned i;

    /* DUPSORT sub-DBs have no ovpages/DBs. Omit scanning leaves.
     * This also avoids any P_LEAF2 pages, which have no nodes.
     * Also if the DB doesn't have sub-DBs and has no overflow
     * pages, omit scanning leaves. */
    if ((mc->mc_flags & C_SUB) || (subs | mc->mc_db->md_overflow_pages) == 0)
      mdbx_cursor_pop(mc);

    rc = mdbx_pnl_need(&txn->tw.retired_pages,
                       mc->mc_db->md_branch_pages + mc->mc_db->md_leaf_pages +
                           mc->mc_db->md_overflow_pages);
    if (unlikely(rc))
      goto done;

    mdbx_cursor_copy(mc, &mx);
    while (mc->mc_snum > 0) {
      MDBX_page *mp = mc->mc_pg[mc->mc_top];
      unsigned n = page_numkeys(mp);
      if (IS_LEAF(mp)) {
        for (i = 0; i < n; i++) {
          MDBX_node *node = page_node(mp, i);
          if (node_flags(node) & F_BIGDATA) {
            MDBX_page *omp;
            rc = mdbx_page_get(mc, node_largedata_pgno(node), &omp, NULL);
            if (unlikely(rc))
              goto done;
            mdbx_cassert(mc, IS_OVERFLOW(omp));
            rc = mdbx_page_retire(mc, omp);
            if (unlikely(rc))
              goto done;
            if (!mc->mc_db->md_overflow_pages && !subs)
              break;
          } else if (subs && (node_flags(node) & F_SUBDATA)) {
            rc = mdbx_xcursor_init1(mc, node);
            if (unlikely(rc != MDBX_SUCCESS))
              goto done;
            rc = mdbx_drop0(&mc->mc_xcursor->mx_cursor, 0);
            if (unlikely(rc))
              goto done;
          }
        }
        if (!subs && !mc->mc_db->md_overflow_pages)
          goto pop;
      } else {
        for (i = 0; i < n; i++) {
          /* free it */
          rc = mdbx_retire_pgno(mc, node_pgno(page_node(mp, i)));
          if (unlikely(rc))
            goto done;
        }
      }
      if (!mc->mc_top)
        break;
      mdbx_cassert(mc, i <= UINT16_MAX);
      mc->mc_ki[mc->mc_top] = (indx_t)i;
      rc = mdbx_cursor_sibling(mc, 1);
      if (rc) {
        if (unlikely(rc != MDBX_NOTFOUND))
          goto done;
      /* no more siblings, go back to beginning
       * of previous level. */
      pop:
        mdbx_cursor_pop(mc);
        mc->mc_ki[0] = 0;
        for (i = 1; i < mc->mc_snum; i++) {
          mc->mc_ki[i] = 0;
          mc->mc_pg[i] = mx.mc_pg[i];
        }
      }
    }
    /* free it */
    rc = mdbx_retire_pgno(mc, mc->mc_db->md_root);
  done:
    if (unlikely(rc))
      txn->mt_flags |= MDBX_TXN_ERROR;
  } else if (rc == MDBX_NOTFOUND) {
    rc = MDBX_SUCCESS;
  }
  mc->mc_flags &= ~C_INITIALIZED;
  return rc;
}

int mdbx_drop(MDBX_txn *txn, MDBX_dbi dbi, int del) {
  int rc = check_txn_rw(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(1 < (unsigned)del))
    return MDBX_EINVAL;

  if (unlikely(!TXN_DBI_EXIST(txn, dbi, DB_USRVALID)))
    return MDBX_EINVAL;

  if (unlikely(TXN_DBI_CHANGED(txn, dbi)))
    return MDBX_BAD_DBI;

  MDBX_cursor *mc;
  rc = mdbx_cursor_open(txn, dbi, &mc);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!TXN_DBI_EXIST(txn, dbi, DB_USRVALID))) {
    rc = MDBX_EINVAL;
    goto bailout;
  }

  if (unlikely(TXN_DBI_CHANGED(txn, dbi))) {
    rc = MDBX_BAD_DBI;
    goto bailout;
  }

  rc = mdbx_drop0(mc, mc->mc_db->md_flags & MDBX_DUPSORT);
  /* Invalidate the dropped DB's cursors */
  for (MDBX_cursor *m2 = txn->mt_cursors[dbi]; m2; m2 = m2->mc_next)
    m2->mc_flags &= ~(C_INITIALIZED | C_EOF);
  if (unlikely(rc))
    goto bailout;

  /* Can't delete the main DB */
  if (del && dbi >= CORE_DBS) {
    rc = mdbx_del0(txn, MAIN_DBI, &mc->mc_dbx->md_name, NULL, F_SUBDATA);
    if (likely(rc == MDBX_SUCCESS)) {
      txn->mt_dbflags[dbi] = DB_STALE;
      MDBX_env *env = txn->mt_env;
      rc = mdbx_fastmutex_acquire(&env->me_dbi_lock);
      if (unlikely(rc != MDBX_SUCCESS)) {
        txn->mt_flags |= MDBX_TXN_ERROR;
        goto bailout;
      }
      mdbx_dbi_close_locked(env, dbi);
      mdbx_ensure(env,
                  mdbx_fastmutex_release(&env->me_dbi_lock) == MDBX_SUCCESS);
    } else {
      txn->mt_flags |= MDBX_TXN_ERROR;
    }
  } else {
    /* reset the DB record, mark it dirty */
    txn->mt_dbflags[dbi] |= DB_DIRTY;
    txn->mt_dbs[dbi].md_depth = 0;
    txn->mt_dbs[dbi].md_branch_pages = 0;
    txn->mt_dbs[dbi].md_leaf_pages = 0;
    txn->mt_dbs[dbi].md_overflow_pages = 0;
    txn->mt_dbs[dbi].md_entries = 0;
    txn->mt_dbs[dbi].md_root = P_INVALID;
    txn->mt_dbs[dbi].md_seq = 0;
    txn->mt_flags |= MDBX_TXN_DIRTY;
  }

bailout:
  mdbx_cursor_close(mc);
  return rc;
}

int mdbx_set_compare(MDBX_txn *txn, MDBX_dbi dbi, MDBX_cmp_func *cmp) {
  int rc = check_txn(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!TXN_DBI_EXIST(txn, dbi, DB_USRVALID)))
    return MDBX_EINVAL;

  txn->mt_dbxs[dbi].md_cmp = cmp;
  return MDBX_SUCCESS;
}

int mdbx_set_dupsort(MDBX_txn *txn, MDBX_dbi dbi, MDBX_cmp_func *cmp) {
  int rc = check_txn(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!TXN_DBI_EXIST(txn, dbi, DB_USRVALID)))
    return MDBX_EINVAL;

  txn->mt_dbxs[dbi].md_dcmp = cmp;
  return MDBX_SUCCESS;
}

int __cold mdbx_reader_list(MDBX_env *env, MDBX_reader_list_func *func,
                            void *ctx) {
  if (unlikely(!env || !func))
    return MDBX_EINVAL;

  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

  int rc = MDBX_RESULT_TRUE;
  int serial = 0;
  if (likely(env->me_lck)) {
    const unsigned snap_nreaders = env->me_lck->mti_numreaders;
    for (unsigned i = 0; i < snap_nreaders; i++) {
      const MDBX_reader *r = env->me_lck->mti_readers + i;
    retry_reader:;
      const uint32_t pid = r->mr_pid;
      if (!pid)
        continue;
      txnid_t txnid = safe64_read(&r->mr_txnid);
      const size_t tid = r->mr_tid;
      const pgno_t pages_used = r->mr_snapshot_pages_used;
      const uint64_t reader_pages_retired = r->mr_snapshot_pages_retired;
      mdbx_compiler_barrier();
      if (unlikely(tid != r->mr_tid ||
                   pages_used != r->mr_snapshot_pages_used ||
                   reader_pages_retired != r->mr_snapshot_pages_retired ||
                   txnid != safe64_read(&r->mr_txnid) || pid != r->mr_pid))
        goto retry_reader;

      mdbx_assert(env, txnid > 0);
      if (txnid >= SAFE64_INVALID_THRESHOLD)
        txnid = 0;

      size_t bytes_used = 0;
      size_t bytes_retained = 0;
      uint64_t lag = 0;
      if (txnid) {
      retry_header:;
        const MDBX_meta *const recent_meta = mdbx_meta_head(env);
        const uint64_t head_pages_retired = recent_meta->mm_pages_retired;
        const txnid_t head_txnid = mdbx_meta_txnid_fluid(env, recent_meta);
        mdbx_compiler_barrier();
        if (unlikely(recent_meta != mdbx_meta_head(env) ||
                     head_pages_retired != recent_meta->mm_pages_retired) ||
            head_txnid != mdbx_meta_txnid_fluid(env, recent_meta))
          goto retry_header;

        lag = (head_txnid - txnid) / MDBX_TXNID_STEP;
        bytes_used = pgno2bytes(env, pages_used);
        bytes_retained = (head_pages_retired > reader_pages_retired)
                             ? pgno2bytes(env, (pgno_t)(head_pages_retired -
                                                        reader_pages_retired))
                             : 0;
      }
      rc = func(ctx, ++serial, i, pid, (mdbx_tid_t)tid, txnid, lag, bytes_used,
                bytes_retained);
      if (unlikely(rc != MDBX_SUCCESS))
        break;
    }
  }

  return rc;
}

/* Insert pid into list if not already present.
 * return -1 if already present. */
static int __cold mdbx_pid_insert(uint32_t *ids, uint32_t pid) {
  /* binary search of pid in list */
  unsigned base = 0;
  unsigned cursor = 1;
  int val = 0;
  unsigned n = ids[0];

  while (n > 0) {
    unsigned pivot = n >> 1;
    cursor = base + pivot + 1;
    val = pid - ids[cursor];

    if (val < 0) {
      n = pivot;
    } else if (val > 0) {
      base = cursor;
      n -= pivot + 1;
    } else {
      /* found, so it's a duplicate */
      return -1;
    }
  }

  if (val > 0)
    ++cursor;

  ids[0]++;
  for (n = ids[0]; n > cursor; n--)
    ids[n] = ids[n - 1];
  ids[n] = pid;
  return 0;
}

int __cold mdbx_reader_check(MDBX_env *env, int *dead) {
  if (unlikely(!env))
    return MDBX_EINVAL;

  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

  if (dead)
    *dead = 0;
  return mdbx_reader_check0(env, false, dead);
}

/* Return:
 *  MDBX_RESULT_TRUE - done and mutex recovered
 *  MDBX_SUCCESS     - done
 *  Otherwise errcode. */
int __cold mdbx_reader_check0(MDBX_env *env, int rdt_locked, int *dead) {
  mdbx_assert(env, rdt_locked >= 0);

#if MDBX_TXN_CHECKPID
  if (unlikely(env->me_pid != mdbx_getpid())) {
    env->me_flags |= MDBX_FATAL_ERROR;
    return MDBX_PANIC;
  }
#endif /* MDBX_TXN_CHECKPID */

  MDBX_lockinfo *const lck = env->me_lck;
  if (unlikely(lck == NULL)) {
    /* exclusive mode */
    if (dead)
      *dead = 0;
    return MDBX_SUCCESS;
  }

  lck->mti_reader_check_timestamp = mdbx_osal_monotime();
  const unsigned snap_nreaders = lck->mti_numreaders;
  uint32_t pidsbuf_onstask[142];
  uint32_t *const pids =
      (snap_nreaders < ARRAY_LENGTH(pidsbuf_onstask))
          ? pidsbuf_onstask
          : mdbx_malloc((snap_nreaders + 1) * sizeof(uint32_t));
  if (unlikely(!pids))
    return MDBX_ENOMEM;

  pids[0] = 0;

  int rc = MDBX_SUCCESS, count = 0;
  for (unsigned i = 0; i < snap_nreaders; i++) {
    const uint32_t pid = lck->mti_readers[i].mr_pid;
    if (pid == 0)
      continue /* skip empty */;
    if (pid == env->me_pid)
      continue /* skip self */;
    if (mdbx_pid_insert(pids, pid) != 0)
      continue /* such pid already processed */;

    int err = mdbx_rpid_check(env, pid);
    if (err == MDBX_RESULT_TRUE)
      continue /* reader is live */;

    if (err != MDBX_SUCCESS) {
      rc = err;
      break /* mdbx_rpid_check() failed */;
    }

    /* stale reader found */
    if (!rdt_locked) {
      err = mdbx_rdt_lock(env);
      if (MDBX_IS_ERROR(err)) {
        rc = err;
        break;
      }

      rdt_locked = -1;
      if (err == MDBX_RESULT_TRUE) {
        /* mutex recovered, the mdbx_mutex_failed() checked all readers */
        rc = MDBX_RESULT_TRUE;
        break;
      }

      /* a other process may have clean and reused slot, recheck */
      if (lck->mti_readers[i].mr_pid != pid)
        continue;

      err = mdbx_rpid_check(env, pid);
      if (MDBX_IS_ERROR(err)) {
        rc = err;
        break;
      }

      if (err != MDBX_SUCCESS)
        continue /* the race with other process, slot reused */;
    }

    /* clean it */
    for (unsigned j = i; j < snap_nreaders; j++) {
      if (lck->mti_readers[j].mr_pid == pid) {
        mdbx_debug("clear stale reader pid %" PRIuPTR " txn %" PRIaTXN,
                   (size_t)pid, lck->mti_readers[j].mr_txnid.inconsistent);
        lck->mti_readers[j].mr_pid = 0;
        lck->mti_readers_refresh_flag = true;
        count++;
      }
    }
  }

  if (rdt_locked < 0)
    mdbx_rdt_unlock(env);

  if (pids != pidsbuf_onstask)
    mdbx_free(pids);

  if (dead)
    *dead = count;
  return rc;
}

int __cold mdbx_setup_debug(int loglevel, int flags, MDBX_debug_func *logger) {
  const int rc = mdbx_runtime_flags | (mdbx_loglevel << 16);

#if !MDBX_DEBUG
  (void)loglevel;
#else
  if (loglevel != -1)
    mdbx_loglevel = (uint8_t)loglevel;
#endif

  if (flags != -1) {
#if !MDBX_DEBUG
    flags &= MDBX_DBG_DUMP | MDBX_DBG_LEGACY_MULTIOPEN;
#else
    flags &= MDBX_DBG_ASSERT | MDBX_DBG_AUDIT | MDBX_DBG_JITTER |
             MDBX_DBG_DUMP | MDBX_DBG_LEGACY_MULTIOPEN;
#endif
#if defined(__linux__) || defined(__gnu_linux__)
    if ((mdbx_runtime_flags ^ flags) & MDBX_DBG_DUMP) {
      /* http://man7.org/linux/man-pages/man5/core.5.html */
      const unsigned long dump_bits =
          1 << 3   /* Dump file-backed shared mappings */
          | 1 << 6 /* Dump shared huge pages */
          | 1 << 8 /* Dump shared DAX pages */;
      const int core_filter_fd =
          open("/proc/self/coredump_filter", O_TRUNC | O_RDWR);
      if (core_filter_fd != -1) {
        char buf[32];
        intptr_t bytes = pread(core_filter_fd, buf, sizeof(buf), 0);
        if (bytes > 0 && (size_t)bytes < sizeof(buf)) {
          buf[bytes] = 0;
          const unsigned long present_mask = strtoul(buf, NULL, 16);
          const unsigned long wanna_mask = (flags & MDBX_DBG_DUMP)
                                               ? present_mask | dump_bits
                                               : present_mask & ~dump_bits;
          if (wanna_mask != present_mask) {
            bytes = snprintf(buf, sizeof(buf), "0x%lx\n", wanna_mask);
            if (bytes > 0 && (size_t)bytes < sizeof(buf)) {
              bytes = pwrite(core_filter_fd, buf, bytes, 0);
              (void)bytes;
            }
          }
        }
        close(core_filter_fd);
      }
    }
#endif /* Linux */
    mdbx_runtime_flags = (uint8_t)flags;
  }

  if (-1 != (intptr_t)logger)
    mdbx_debug_logger = logger;
  return rc;
}

static txnid_t __cold mdbx_oomkick(MDBX_env *env, const txnid_t laggard) {
  mdbx_debug("%s", "DB size maxed out");

  int retry;
  for (retry = 0; retry < INT_MAX; ++retry) {
    txnid_t oldest = mdbx_reclaiming_detent(env);
    mdbx_assert(env, oldest < env->me_txn0->mt_txnid);
    mdbx_assert(env, oldest >= laggard);
    mdbx_assert(env, oldest >= *env->me_oldest);
    if (oldest == laggard || unlikely(env->me_lck == NULL /* exclusive mode */))
      return oldest;

    if (MDBX_IS_ERROR(mdbx_reader_check0(env, false, NULL)))
      break;

    MDBX_reader *asleep = nullptr;
    MDBX_lockinfo *const lck = env->me_lck;
    uint64_t oldest_retired = UINT64_MAX;
    const unsigned snap_nreaders = lck->mti_numreaders;
    for (unsigned i = 0; i < snap_nreaders; ++i) {
    retry:
      if (lck->mti_readers[i].mr_pid) {
        /* mdbx_jitter4testing(true); */
        const uint64_t snap_retired =
            lck->mti_readers[i].mr_snapshot_pages_retired;
        const txnid_t snap_txnid = safe64_read(&lck->mti_readers[i].mr_txnid);
        mdbx_memory_barrier();
        if (unlikely(snap_retired !=
                         lck->mti_readers[i].mr_snapshot_pages_retired ||
                     snap_txnid != safe64_read(&lck->mti_readers[i].mr_txnid)))
          goto retry;
        if (oldest > snap_txnid &&
            laggard <= /* ignore pending updates */ snap_txnid) {
          oldest = snap_txnid;
          oldest_retired = snap_retired;
          asleep = &lck->mti_readers[i];
        }
      }
    }

    if (laggard < oldest || !asleep) {
      if (retry && env->me_oom_func) {
        /* LY: notify end of oom-loop */
        const txnid_t gap = oldest - laggard;
        env->me_oom_func(env, 0, 0, laggard,
                         (gap < UINT_MAX) ? (unsigned)gap : UINT_MAX, 0,
                         -retry);
      }
      mdbx_notice("oom-kick: update oldest %" PRIaTXN " -> %" PRIaTXN,
                  *env->me_oldest, oldest);
      mdbx_assert(env, *env->me_oldest <= oldest);
      return *env->me_oldest = oldest;
    }

    if (!env->me_oom_func)
      break;

    uint32_t pid = asleep->mr_pid;
    size_t tid = asleep->mr_tid;
    if (safe64_read(&asleep->mr_txnid) != laggard || pid <= 0)
      continue;

    const MDBX_meta *head_meta = mdbx_meta_head(env);
    const txnid_t gap =
        (mdbx_meta_txnid_stable(env, head_meta) - laggard) / MDBX_TXNID_STEP;
    const uint64_t head_retired = head_meta->mm_pages_retired;
    const size_t space =
        (oldest_retired > head_retired)
            ? pgno2bytes(env, (pgno_t)(oldest_retired - head_retired))
            : 0;
    int rc = env->me_oom_func(env, pid, (mdbx_tid_t)tid, laggard,
                              (gap < UINT_MAX) ? (unsigned)gap : UINT_MAX,
                              space, retry);
    if (rc < 0)
      break;

    if (rc > 0) {
      if (rc == 1) {
        safe64_reset_compare(&asleep->mr_txnid, laggard);
      } else {
        safe64_reset(&asleep->mr_txnid, true);
        asleep->mr_tid = 0;
        asleep->mr_pid = 0;
      }
      lck->mti_readers_refresh_flag = true;
      mdbx_flush_noncoherent_cpu_writeback();
    }
  }

  if (retry && env->me_oom_func) {
    /* LY: notify end of oom-loop */
    env->me_oom_func(env, 0, 0, laggard, 0, 0, -retry);
  }
  return mdbx_find_oldest(env->me_txn);
}

int __cold mdbx_env_set_syncbytes(MDBX_env *env, size_t threshold) {
  if (unlikely(!env))
    return MDBX_EINVAL;

  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(env->me_flags & (MDBX_RDONLY | MDBX_FATAL_ERROR)))
    return MDBX_EACCESS;

  if (unlikely(!env->me_map))
    return MDBX_EPERM;

  *env->me_autosync_threshold = bytes2pgno(env, threshold + env->me_psize - 1);
  if (threshold) {
    int err = mdbx_env_sync_poll(env);
    if (unlikely(MDBX_IS_ERROR(err)))
      return err;
  }
  return MDBX_SUCCESS;
}

int __cold mdbx_env_set_syncperiod(MDBX_env *env, unsigned seconds_16dot16) {
  if (unlikely(!env))
    return MDBX_EINVAL;

  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(env->me_flags & (MDBX_RDONLY | MDBX_FATAL_ERROR)))
    return MDBX_EACCESS;

  if (unlikely(!env->me_map))
    return MDBX_EPERM;

  *env->me_autosync_period = mdbx_osal_16dot16_to_monotime(seconds_16dot16);
  if (seconds_16dot16) {
    int err = mdbx_env_sync_poll(env);
    if (unlikely(MDBX_IS_ERROR(err)))
      return err;
  }
  return MDBX_SUCCESS;
}

int __cold mdbx_env_set_oomfunc(MDBX_env *env, MDBX_oom_func *oomfunc) {
  if (unlikely(!env))
    return MDBX_EINVAL;

  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

  env->me_oom_func = oomfunc;
  return MDBX_SUCCESS;
}

MDBX_oom_func *__cold mdbx_env_get_oomfunc(MDBX_env *env) {
  return likely(env && env->me_signature == MDBX_ME_SIGNATURE)
             ? env->me_oom_func
             : NULL;
}

#ifdef __SANITIZE_THREAD__
/* LY: avoid tsan-trap by me_txn, mm_last_pg and mt_next_pgno */
__attribute__((__no_sanitize_thread__, __noinline__))
#endif
int mdbx_txn_straggler(MDBX_txn *txn, int *percent)
{
  int rc = check_txn(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return (rc > 0) ? -rc : rc;

  MDBX_env *env = txn->mt_env;
  if (unlikely((txn->mt_flags & MDBX_RDONLY) == 0)) {
    if (percent)
      *percent =
          (int)((txn->mt_next_pgno * UINT64_C(100) + txn->mt_end_pgno / 2) /
                txn->mt_end_pgno);
    return 0;
  }

  txnid_t recent;
  MDBX_meta *meta;
  do {
    meta = mdbx_meta_head(env);
    recent = mdbx_meta_txnid_fluid(env, meta);
    if (percent) {
      const pgno_t maxpg = meta->mm_geo.now;
      *percent = (int)((meta->mm_geo.next * UINT64_C(100) + maxpg / 2) / maxpg);
    }
  } while (unlikely(recent != mdbx_meta_txnid_fluid(env, meta)));

  txnid_t lag = (recent - txn->mt_txnid) / MDBX_TXNID_STEP;
  return (lag > INT_MAX) ? INT_MAX : (int)lag;
}

typedef struct mdbx_walk_ctx {
  void *mw_user;
  MDBX_pgvisitor_func *mw_visitor;
  MDBX_cursor mw_cursor;
} mdbx_walk_ctx_t;

/* Depth-first tree traversal. */
static int __cold mdbx_env_walk(mdbx_walk_ctx_t *ctx, const char *dbi,
                                pgno_t pgno, int deep) {
  if (unlikely(pgno == P_INVALID))
    return MDBX_SUCCESS; /* empty db */

  MDBX_page *mp;
  int rc = mdbx_page_get(&ctx->mw_cursor, pgno, &mp, NULL);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  rc = mdbx_page_check(ctx->mw_cursor.mc_txn->mt_env, mp, false);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  const int nkeys = page_numkeys(mp);
  size_t header_size = IS_LEAF2(mp) ? PAGEHDRSZ : PAGEHDRSZ + mp->mp_lower;
  size_t unused_size = page_room(mp);
  size_t payload_size = 0;
  size_t align_bytes = 0;
  MDBX_page_type_t type;

  /* LY: Don't use mask here, e.g bitwise
   * (P_BRANCH|P_LEAF|P_LEAF2|P_META|P_OVERFLOW|P_SUBP).
   * Pages should not me marked dirty/loose or otherwise. */
  switch (mp->mp_flags) {
  case P_BRANCH:
    type = MDBX_page_branch;
    if (unlikely(nkeys < 2))
      return MDBX_CORRUPTED;
    break;
  case P_LEAF:
    type = MDBX_page_leaf;
    break;
  case P_LEAF | P_LEAF2:
    type = MDBX_page_dupfixed_leaf;
    break;
  default:
    return MDBX_CORRUPTED;
  }

  for (int i = 0; i < nkeys;
       align_bytes += ((payload_size + align_bytes) & 1), i++) {
    if (type == MDBX_page_dupfixed_leaf) {
      /* LEAF2 pages have no mp_ptrs[] or node headers */
      payload_size += mp->mp_leaf2_ksize;
      continue;
    }

    MDBX_node *node = page_node(mp, i);
    payload_size += NODESIZE + node_ks(node);

    if (type == MDBX_page_branch) {
      assert(i > 0 || node_ks(node) == 0);
      continue;
    }

    assert(type == MDBX_page_leaf);
    switch (node_flags(node)) {
    case 0 /* usual node */: {
      payload_size += node_ds(node);
    } break;

    case F_BIGDATA /* long data on the large/overflow page */: {
      payload_size += sizeof(pgno_t);

      const pgno_t large_pgno = node_largedata_pgno(node);
      MDBX_page *op;
      rc = mdbx_page_get(&ctx->mw_cursor, large_pgno, &op, NULL);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
      rc = mdbx_page_check(ctx->mw_cursor.mc_txn->mt_env, op, false);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;

      /* LY: Don't use mask here, e.g bitwise
       * (P_BRANCH|P_LEAF|P_LEAF2|P_META|P_OVERFLOW|P_SUBP).
       * Pages should not me marked dirty/loose or otherwise. */
      if (unlikely(P_OVERFLOW != op->mp_flags))
        return MDBX_CORRUPTED;

      const size_t over_header = PAGEHDRSZ;
      const size_t over_payload = node_ds(node);
      const size_t over_unused =
          pgno2bytes(ctx->mw_cursor.mc_txn->mt_env, op->mp_pages) -
          over_payload - over_header;

      rc = ctx->mw_visitor(
          large_pgno, op->mp_pages, ctx->mw_user, deep, dbi,
          pgno2bytes(ctx->mw_cursor.mc_txn->mt_env, op->mp_pages),
          MDBX_page_large, 1, over_payload, over_header, over_unused);
    } break;

    case F_SUBDATA /* sub-db */: {
      const size_t namelen = node_ks(node);
      if (unlikely(namelen == 0 || node_ds(node) < sizeof(MDBX_db)))
        return MDBX_CORRUPTED;
      payload_size += node_ds(node);
    } break;

    case F_SUBDATA | F_DUPDATA /* dupsorted sub-tree */: {
      if (unlikely(node_ds(node) != sizeof(MDBX_db)))
        return MDBX_CORRUPTED;
      payload_size += sizeof(MDBX_db);
    } break;

    case F_DUPDATA /* short sub-page */: {
      if (unlikely(node_ds(node) <= PAGEHDRSZ))
        return MDBX_CORRUPTED;

      MDBX_page *sp = node_data(node);
      const int nsubkeys = page_numkeys(sp);
      size_t subheader_size =
          IS_LEAF2(sp) ? PAGEHDRSZ : PAGEHDRSZ + sp->mp_lower;
      size_t subunused_size = page_room(sp);
      size_t subpayload_size = 0;
      size_t subalign_bytes = 0;
      MDBX_page_type_t subtype;

      switch (sp->mp_flags & ~P_DIRTY /* ignore for sub-pages */) {
      case P_LEAF | P_SUBP:
        subtype = MDBX_subpage_leaf;
        break;
      case P_LEAF | P_LEAF2 | P_SUBP:
        subtype = MDBX_subpage_dupfixed_leaf;
        break;
      default:
        return MDBX_CORRUPTED;
      }

      for (int j = 0; j < nsubkeys;
           subalign_bytes += ((subpayload_size + subalign_bytes) & 1), j++) {

        if (subtype == MDBX_subpage_dupfixed_leaf) {
          /* LEAF2 pages have no mp_ptrs[] or node headers */
          subpayload_size += sp->mp_leaf2_ksize;
        } else {
          assert(subtype == MDBX_subpage_leaf);
          MDBX_node *subnode = page_node(sp, j);
          subpayload_size += NODESIZE + node_ks(subnode) + node_ds(subnode);
          if (unlikely(node_flags(subnode) != 0))
            return MDBX_CORRUPTED;
        }
      }

      rc = ctx->mw_visitor(pgno, 0, ctx->mw_user, deep + 1, dbi, node_ds(node),
                           subtype, nsubkeys, subpayload_size, subheader_size,
                           subunused_size + subalign_bytes);
      header_size += subheader_size;
      unused_size += subunused_size;
      payload_size += subpayload_size;
      align_bytes += subalign_bytes;
    } break;

    default:
      return MDBX_CORRUPTED;
    }

    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }

  rc = ctx->mw_visitor(mp->mp_pgno, 1, ctx->mw_user, deep, dbi,
                       ctx->mw_cursor.mc_txn->mt_env->me_psize, type, nkeys,
                       payload_size, header_size, unused_size + align_bytes);

  if (unlikely(rc != MDBX_SUCCESS))
    return (rc == MDBX_RESULT_TRUE) ? MDBX_SUCCESS : rc;

  for (int i = 0; i < nkeys; i++) {
    if (type == MDBX_page_dupfixed_leaf)
      continue;

    MDBX_node *node = page_node(mp, i);
    if (type == MDBX_page_branch) {
      rc = mdbx_env_walk(ctx, dbi, node_pgno(node), deep + 1);
      if (unlikely(rc != MDBX_SUCCESS)) {
        if (rc != MDBX_RESULT_TRUE)
          return rc;
        break;
      }
      continue;
    }

    assert(type == MDBX_page_leaf);
    MDBX_db db;
    switch (node_flags(node)) {
    default:
      continue;

    case F_SUBDATA /* sub-db */: {
      const size_t namelen = node_ks(node);
      if (unlikely(namelen == 0 || node_ds(node) != sizeof(MDBX_db)))
        return MDBX_CORRUPTED;

      char namebuf_onstask[142];
      char *const name = (namelen < sizeof(namebuf_onstask))
                             ? namebuf_onstask
                             : mdbx_malloc(namelen + 1);
      if (name) {
        memcpy(name, node_key(node), namelen);
        name[namelen] = 0;
        memcpy(&db, node_data(node), sizeof(db));
        rc = mdbx_env_walk(ctx, name, db.md_root, deep + 1);
        if (name != namebuf_onstask)
          mdbx_free(name);
      } else {
        rc = MDBX_ENOMEM;
      }
    } break;

    case F_SUBDATA | F_DUPDATA /* dupsorted sub-tree */:
      if (unlikely(node_ds(node) != sizeof(MDBX_db)))
        return MDBX_CORRUPTED;

      memcpy(&db, node_data(node), sizeof(db));
      rc = mdbx_env_walk(ctx, dbi, db.md_root, deep + 1);
      break;
    }

    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }

  return MDBX_SUCCESS;
}

int __cold mdbx_env_pgwalk(MDBX_txn *txn, MDBX_pgvisitor_func *visitor,
                           void *user) {
  int rc = check_txn(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  mdbx_walk_ctx_t ctx;
  memset(&ctx, 0, sizeof(ctx));
  ctx.mw_cursor.mc_snum = 1;
  ctx.mw_cursor.mc_txn = txn;
  ctx.mw_user = user;
  ctx.mw_visitor = visitor;

  rc = visitor(0, NUM_METAS, user, 0, MDBX_PGWALK_META,
               pgno2bytes(txn->mt_env, NUM_METAS), MDBX_page_meta, NUM_METAS,
               sizeof(MDBX_meta) * NUM_METAS, PAGEHDRSZ * NUM_METAS,
               (txn->mt_env->me_psize - sizeof(MDBX_meta) - PAGEHDRSZ) *
                   NUM_METAS);
  if (!MDBX_IS_ERROR(rc))
    rc = mdbx_env_walk(&ctx, MDBX_PGWALK_GC, txn->mt_dbs[FREE_DBI].md_root, 0);
  if (!MDBX_IS_ERROR(rc))
    rc =
        mdbx_env_walk(&ctx, MDBX_PGWALK_MAIN, txn->mt_dbs[MAIN_DBI].md_root, 0);
  if (!MDBX_IS_ERROR(rc))
    rc = visitor(P_INVALID, 0, user, INT_MIN, NULL, 0, MDBX_page_void, 0, 0, 0,
                 0);
  return rc;
}

int mdbx_canary_put(MDBX_txn *txn, const mdbx_canary *canary) {
  int rc = check_txn_rw(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (likely(canary)) {
    if (txn->mt_canary.x == canary->x && txn->mt_canary.y == canary->y &&
        txn->mt_canary.z == canary->z)
      return MDBX_SUCCESS;
    txn->mt_canary.x = canary->x;
    txn->mt_canary.y = canary->y;
    txn->mt_canary.z = canary->z;
  }
  txn->mt_canary.v = txn->mt_txnid;

  if ((txn->mt_flags & MDBX_TXN_DIRTY) == 0) {
    txn->mt_flags |= MDBX_TXN_DIRTY;
    *txn->mt_env->me_unsynced_pages += 1;
  }
  return MDBX_SUCCESS;
}

int mdbx_canary_get(MDBX_txn *txn, mdbx_canary *canary) {
  int rc = check_txn(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(canary == NULL))
    return MDBX_EINVAL;

  *canary = txn->mt_canary;
  return MDBX_SUCCESS;
}

int mdbx_cursor_on_first(MDBX_cursor *mc) {
  if (unlikely(mc == NULL))
    return MDBX_EINVAL;

  if (unlikely(mc->mc_signature != MDBX_MC_SIGNATURE))
    return MDBX_EBADSIGN;

  if (!(mc->mc_flags & C_INITIALIZED))
    return MDBX_RESULT_FALSE;

  for (unsigned i = 0; i < mc->mc_snum; ++i) {
    if (mc->mc_ki[i])
      return MDBX_RESULT_FALSE;
  }

  return MDBX_RESULT_TRUE;
}

int mdbx_cursor_on_last(MDBX_cursor *mc) {
  if (unlikely(mc == NULL))
    return MDBX_EINVAL;

  if (unlikely(mc->mc_signature != MDBX_MC_SIGNATURE))
    return MDBX_EBADSIGN;

  if (!(mc->mc_flags & C_INITIALIZED))
    return MDBX_RESULT_FALSE;

  for (unsigned i = 0; i < mc->mc_snum; ++i) {
    unsigned nkeys = page_numkeys(mc->mc_pg[i]);
    if (mc->mc_ki[i] < nkeys - 1)
      return MDBX_RESULT_FALSE;
  }

  return MDBX_RESULT_TRUE;
}

int mdbx_cursor_eof(MDBX_cursor *mc) {
  if (unlikely(mc == NULL))
    return MDBX_EINVAL;

  if (unlikely(mc->mc_signature != MDBX_MC_SIGNATURE))
    return MDBX_EBADSIGN;

  if ((mc->mc_flags & C_INITIALIZED) == 0)
    return MDBX_RESULT_TRUE;

  if (mc->mc_snum == 0)
    return MDBX_RESULT_TRUE;

  if ((mc->mc_flags & C_EOF) &&
      mc->mc_ki[mc->mc_top] >= page_numkeys(mc->mc_pg[mc->mc_top]))
    return MDBX_RESULT_TRUE;

  return MDBX_RESULT_FALSE;
}

//------------------------------------------------------------------------------

struct diff_result {
  ptrdiff_t diff;
  int level;
  int root_nkeys;
};

/* calculates: r = x - y */
__hot static int cursor_diff(const MDBX_cursor *const __restrict x,
                             const MDBX_cursor *const __restrict y,
                             struct diff_result *const __restrict r) {
  r->diff = 0;
  r->level = 0;
  r->root_nkeys = 0;

  if (unlikely(y->mc_signature != MDBX_MC_SIGNATURE ||
               x->mc_signature != MDBX_MC_SIGNATURE))
    return MDBX_EBADSIGN;

  int rc = check_txn(x->mc_txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(x->mc_txn != y->mc_txn))
    return MDBX_BAD_TXN;

  if (unlikely(y->mc_dbi != x->mc_dbi))
    return MDBX_EINVAL;

  if (unlikely(!(y->mc_flags & x->mc_flags & C_INITIALIZED)))
    return MDBX_ENODATA;

  while (likely(r->level < y->mc_snum && r->level < x->mc_snum)) {
    if (unlikely(y->mc_pg[r->level] != x->mc_pg[r->level]))
      return MDBX_PROBLEM;

    int nkeys = page_numkeys(y->mc_pg[r->level]);
    assert(nkeys > 0);
    if (r->level == 0)
      r->root_nkeys = nkeys;

    const int limit_ki = nkeys - 1;
    const int x_ki = x->mc_ki[r->level];
    const int y_ki = y->mc_ki[r->level];
    r->diff = ((x_ki < limit_ki) ? x_ki : limit_ki) -
              ((y_ki < limit_ki) ? y_ki : limit_ki);
    if (r->diff == 0) {
      r->level += 1;
      continue;
    }

    while (unlikely(r->diff == 1) &&
           likely(r->level + 1 < y->mc_snum && r->level + 1 < x->mc_snum)) {
      r->level += 1;
      /*   DB'PAGEs: 0------------------>MAX
       *
       *    CURSORs:       y < x
       *  STACK[i ]:         |
       *  STACK[+1]:  ...y++N|0++x...
       */
      nkeys = page_numkeys(y->mc_pg[r->level]);
      r->diff = (nkeys - y->mc_ki[r->level]) + x->mc_ki[r->level];
      assert(r->diff > 0);
    }

    while (unlikely(r->diff == -1) &&
           likely(r->level + 1 < y->mc_snum && r->level + 1 < x->mc_snum)) {
      r->level += 1;
      /*   DB'PAGEs: 0------------------>MAX
       *
       *    CURSORs:       x < y
       *  STACK[i ]:         |
       *  STACK[+1]:  ...x--N|0--y...
       */
      nkeys = page_numkeys(x->mc_pg[r->level]);
      r->diff = -(nkeys - x->mc_ki[r->level]) - y->mc_ki[r->level];
      assert(r->diff < 0);
    }

    return MDBX_SUCCESS;
  }

  r->diff = CMP2INT(x->mc_flags & C_EOF, y->mc_flags & C_EOF);
  return MDBX_SUCCESS;
}

__hot static ptrdiff_t estimate(const MDBX_db *db,
                                struct diff_result *const __restrict dr) {
  /*        root: branch-page    => scale = leaf-factor * branch-factor^(N-1)
   *     level-1: branch-page(s) => scale = leaf-factor * branch-factor^2
   *     level-2: branch-page(s) => scale = leaf-factor * branch-factor
   *     level-N: branch-page(s) => scale = leaf-factor
   *  leaf-level: leaf-page(s)   => scale = 1
   */
  ptrdiff_t btree_power = db->md_depth - 2 - dr->level;
  if (btree_power < 0)
    return dr->diff;

  ptrdiff_t estimated =
      (ptrdiff_t)db->md_entries * dr->diff / (ptrdiff_t)db->md_leaf_pages;
  if (btree_power == 0)
    return estimated;

  if (db->md_depth < 4) {
    assert(dr->level == 0 && btree_power == 1);
    return (ptrdiff_t)db->md_entries * dr->diff / (ptrdiff_t)dr->root_nkeys;
  }

  /* average_branchpage_fillfactor = total(branch_entries) / branch_pages
     total(branch_entries) = leaf_pages + branch_pages - 1 (root page) */
  const size_t log2_fixedpoint = sizeof(size_t) - 1;
  const size_t half = UINT64_C(1) << (log2_fixedpoint - 1);
  const size_t factor =
      ((db->md_leaf_pages + db->md_branch_pages - 1) << log2_fixedpoint) /
      db->md_branch_pages;
  while (1) {
    switch ((size_t)btree_power) {
    default: {
      const size_t square = (factor * factor + half) >> log2_fixedpoint;
      const size_t quad = (square * square + half) >> log2_fixedpoint;
      do {
        estimated = estimated * quad + half;
        estimated >>= log2_fixedpoint;
        btree_power -= 4;
      } while (btree_power >= 4);
      continue;
    }
    case 3:
      estimated = estimated * factor + half;
      estimated >>= log2_fixedpoint;
      __fallthrough /* fall through */;
    case 2:
      estimated = estimated * factor + half;
      estimated >>= log2_fixedpoint;
      __fallthrough /* fall through */;
    case 1:
      estimated = estimated * factor + half;
      estimated >>= log2_fixedpoint;
      __fallthrough /* fall through */;
    case 0:
      if (unlikely(estimated > (ptrdiff_t)db->md_entries))
        return (ptrdiff_t)db->md_entries;
      if (unlikely(estimated < -(ptrdiff_t)db->md_entries))
        return -(ptrdiff_t)db->md_entries;
      return estimated;
    }
  }
}

__hot int mdbx_estimate_distance(const MDBX_cursor *first,
                                 const MDBX_cursor *last,
                                 ptrdiff_t *distance_items) {
  if (unlikely(first == NULL || last == NULL || distance_items == NULL))
    return MDBX_EINVAL;

  *distance_items = 0;
  struct diff_result dr;
  int rc = cursor_diff(last, first, &dr);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(dr.diff == 0) &&
      F_ISSET(first->mc_db->md_flags & first->mc_db->md_flags,
              MDBX_DUPSORT | C_INITIALIZED)) {
    first = &first->mc_xcursor->mx_cursor;
    last = &last->mc_xcursor->mx_cursor;
    rc = cursor_diff(first, last, &dr);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }

  if (likely(dr.diff != 0))
    *distance_items = estimate(first->mc_db, &dr);

  return MDBX_SUCCESS;
}

int mdbx_estimate_move(const MDBX_cursor *cursor, MDBX_val *key, MDBX_val *data,
                       MDBX_cursor_op move_op, ptrdiff_t *distance_items) {
  if (unlikely(cursor == NULL || distance_items == NULL ||
               move_op == MDBX_GET_CURRENT || move_op == MDBX_GET_MULTIPLE))
    return MDBX_EINVAL;

  if (unlikely(cursor->mc_signature != MDBX_MC_SIGNATURE))
    return MDBX_EBADSIGN;

  int rc = check_txn(cursor->mc_txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (!(cursor->mc_flags & C_INITIALIZED))
    return MDBX_ENODATA;

  MDBX_cursor_couple next;
  mdbx_cursor_copy(cursor, &next.outer);
  next.outer.mc_xcursor = NULL;
  if (cursor->mc_db->md_flags & MDBX_DUPSORT) {
    next.outer.mc_xcursor = &next.inner;
    rc = mdbx_xcursor_init0(&next.outer);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    MDBX_xcursor *mx = &container_of(cursor, MDBX_cursor_couple, outer)->inner;
    mdbx_cursor_copy(&mx->mx_cursor, &next.inner.mx_cursor);
  }

  MDBX_val stub = {0, 0};
  if (data == NULL) {
    const unsigned mask =
        1 << MDBX_GET_BOTH | 1 << MDBX_GET_BOTH_RANGE | 1 << MDBX_SET_KEY;
    if (unlikely(mask & (1 << move_op)))
      return MDBX_EINVAL;
    data = &stub;
  }

  if (key == NULL) {
    const unsigned mask = 1 << MDBX_GET_BOTH | 1 << MDBX_GET_BOTH_RANGE |
                          1 << MDBX_SET_KEY | 1 << MDBX_SET |
                          1 << MDBX_SET_RANGE;
    if (unlikely(mask & (1 << move_op)))
      return MDBX_EINVAL;
    key = &stub;
  }

  rc = mdbx_cursor_get(&next.outer, key, data, move_op);
  if (unlikely(rc != MDBX_SUCCESS &&
               (rc != MDBX_NOTFOUND || !(next.outer.mc_flags & C_INITIALIZED))))
    return rc;

  return mdbx_estimate_distance(cursor, &next.outer, distance_items);
}

static int mdbx_is_samedata(const MDBX_val *a, const MDBX_val *b) {
  return a->iov_len == b->iov_len &&
         memcmp(a->iov_base, b->iov_base, a->iov_len) == 0;
}

int mdbx_estimate_range(MDBX_txn *txn, MDBX_dbi dbi, MDBX_val *begin_key,
                        MDBX_val *begin_data, MDBX_val *end_key,
                        MDBX_val *end_data, ptrdiff_t *size_items) {
  int rc = check_txn(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!size_items))
    return MDBX_EINVAL;

  if (unlikely(begin_data && (begin_key == NULL || begin_key == MDBX_EPSILON)))
    return MDBX_EINVAL;

  if (unlikely(end_data && (end_key == NULL || end_key == MDBX_EPSILON)))
    return MDBX_EINVAL;

  if (unlikely(begin_key == MDBX_EPSILON && end_key == MDBX_EPSILON))
    return MDBX_EINVAL;

  if (unlikely(!TXN_DBI_EXIST(txn, dbi, DB_USRVALID)))
    return MDBX_EINVAL;

  MDBX_cursor_couple begin;
  /* LY: first, initialize cursor to refresh a DB in case it have DB_STALE */
  rc = mdbx_cursor_init(&begin.outer, txn, dbi);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(begin.outer.mc_db->md_entries == 0)) {
    *size_items = 0;
    return MDBX_SUCCESS;
  }

  if (!begin_key) {
    if (unlikely(!end_key)) {
      /* LY: FIRST..LAST case */
      *size_items = (ptrdiff_t)begin.outer.mc_db->md_entries;
      return MDBX_SUCCESS;
    }
    MDBX_val stub = {0, 0};
    rc = mdbx_cursor_first(&begin.outer, &stub, &stub);
    if (unlikely(end_key == MDBX_EPSILON)) {
      /* LY: FIRST..+epsilon case */
      return (rc == MDBX_SUCCESS)
                 ? mdbx_cursor_count(&begin.outer, (size_t *)size_items)
                 : rc;
    }
  } else {
    if (unlikely(begin_key == MDBX_EPSILON)) {
      if (end_key == NULL) {
        /* LY: -epsilon..LAST case */
        MDBX_val stub = {0, 0};
        rc = mdbx_cursor_last(&begin.outer, &stub, &stub);
        return (rc == MDBX_SUCCESS)
                   ? mdbx_cursor_count(&begin.outer, (size_t *)size_items)
                   : rc;
      }
      /* LY: -epsilon..value case */
      assert(end_key != MDBX_EPSILON);
      begin_key = end_key;
    } else if (unlikely(end_key == MDBX_EPSILON)) {
      /* LY: value..+epsilon case */
      assert(begin_key != MDBX_EPSILON);
      end_key = begin_key;
    }
    if (end_key && !begin_data && !end_data &&
        (begin_key == end_key || mdbx_is_samedata(begin_key, end_key))) {
      /* LY: single key case */
      int exact = 0;
      rc = mdbx_cursor_set(&begin.outer, begin_key, NULL, MDBX_SET, &exact);
      if (unlikely(rc != MDBX_SUCCESS)) {
        *size_items = 0;
        return (rc == MDBX_NOTFOUND) ? MDBX_SUCCESS : rc;
      }
      *size_items = 1;
      if (begin.outer.mc_xcursor != NULL) {
        MDBX_node *node = page_node(begin.outer.mc_pg[begin.outer.mc_top],
                                    begin.outer.mc_ki[begin.outer.mc_top]);
        if (F_ISSET(node_flags(node), F_DUPDATA)) {
          /* LY: return the number of duplicates for given key */
          mdbx_tassert(txn,
                       begin.outer.mc_xcursor == &begin.inner &&
                           (begin.inner.mx_cursor.mc_flags & C_INITIALIZED));
          *size_items =
              (sizeof(*size_items) >= sizeof(begin.inner.mx_db.md_entries) ||
               begin.inner.mx_db.md_entries <= PTRDIFF_MAX)
                  ? (size_t)begin.inner.mx_db.md_entries
                  : PTRDIFF_MAX;
        }
      }
      return MDBX_SUCCESS;
    } else {
      rc = mdbx_cursor_set(&begin.outer, begin_key, begin_data,
                           begin_data ? MDBX_GET_BOTH_RANGE : MDBX_SET_RANGE,
                           NULL);
    }
  }

  if (unlikely(rc != MDBX_SUCCESS)) {
    if (rc != MDBX_NOTFOUND || !(begin.outer.mc_flags & C_INITIALIZED))
      return rc;
  }

  MDBX_cursor_couple end;
  rc = mdbx_cursor_init(&end.outer, txn, dbi);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;
  if (!end_key) {
    MDBX_val stub = {0, 0};
    rc = mdbx_cursor_last(&end.outer, &stub, &stub);
  } else {
    rc = mdbx_cursor_set(&end.outer, end_key, end_data,
                         end_data ? MDBX_GET_BOTH_RANGE : MDBX_SET_RANGE, NULL);
  }
  if (unlikely(rc != MDBX_SUCCESS)) {
    if (rc != MDBX_NOTFOUND || !(end.outer.mc_flags & C_INITIALIZED))
      return rc;
  }

  rc = mdbx_estimate_distance(&begin.outer, &end.outer, size_items);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;
  assert(*size_items >= -(ptrdiff_t)begin.outer.mc_db->md_entries &&
         *size_items <= (ptrdiff_t)begin.outer.mc_db->md_entries);

#if 0 /* LY: Was decided to returns as-is (i.e. negative) the estimation       \
       * results for an inverted ranges. */

  /* Commit 8ddfd1f34ad7cf7a3c4aa75d2e248ca7e639ed63
     Change-Id: If59eccf7311123ab6384c4b93f9b1fed5a0a10d1 */

  if (*size_items < 0) {
    /* LY: inverted range case */
    *size_items += (ptrdiff_t)begin.outer.mc_db->md_entries;
  } else if (*size_items == 0 && begin_key && end_key) {
    int cmp = begin.outer.mc_dbx->md_cmp(&origin_begin_key, &origin_end_key);
    if (cmp == 0 && (begin.inner.mx_cursor.mc_flags & C_INITIALIZED) &&
        begin_data && end_data)
      cmp = begin.outer.mc_dbx->md_dcmp(&origin_begin_data, &origin_end_data);
    if (cmp > 0) {
      /* LY: inverted range case with empty scope */
      *size_items = (ptrdiff_t)begin.outer.mc_db->md_entries;
    }
  }
  assert(*size_items >= 0 &&
         *size_items <= (ptrdiff_t)begin.outer.mc_db->md_entries);
#endif

  return MDBX_SUCCESS;
}

//------------------------------------------------------------------------------

/* Позволяет обновить или удалить существующую запись с получением
 * в old_data предыдущего значения данных. При этом если new_data равен
 * нулю, то выполняется удаление, иначе обновление/вставка.
 *
 * Текущее значение может находиться в уже измененной (грязной) странице.
 * В этом случае страница будет перезаписана при обновлении, а само старое
 * значение утрачено. Поэтому исходно в old_data должен быть передан
 * дополнительный буфер для копирования старого значения.
 * Если переданный буфер слишком мал, то функция вернет -1, установив
 * old_data->iov_len в соответствующее значение.
 *
 * Для не-уникальных ключей также возможен второй сценарий использования,
 * когда посредством old_data из записей с одинаковым ключом для
 * удаления/обновления выбирается конкретная. Для выбора этого сценария
 * во flags следует одновременно указать MDBX_CURRENT и MDBX_NOOVERWRITE.
 * Именно эта комбинация выбрана, так как она лишена смысла, и этим позволяет
 * идентифицировать запрос такого сценария.
 *
 * Функция может быть замещена соответствующими операциями с курсорами
 * после двух доработок (TODO):
 *  - внешняя аллокация курсоров, в том числе на стеке (без malloc).
 *  - получения статуса страницы по адресу (знать о P_DIRTY).
 */
int mdbx_replace(MDBX_txn *txn, MDBX_dbi dbi, MDBX_val *key, MDBX_val *new_data,
                 MDBX_val *old_data, unsigned flags) {
  int rc = check_txn_rw(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!key || !old_data || old_data == new_data))
    return MDBX_EINVAL;

  if (unlikely(old_data->iov_base == NULL && old_data->iov_len))
    return MDBX_EINVAL;

  if (unlikely(new_data == NULL && !(flags & MDBX_CURRENT)))
    return MDBX_EINVAL;

  if (unlikely(!TXN_DBI_EXIST(txn, dbi, DB_USRVALID)))
    return MDBX_EINVAL;

  if (unlikely(flags & ~(MDBX_NOOVERWRITE | MDBX_NODUPDATA | MDBX_RESERVE |
                         MDBX_APPEND | MDBX_APPENDDUP | MDBX_CURRENT)))
    return MDBX_EINVAL;

  MDBX_cursor_couple cx;
  rc = mdbx_cursor_init(&cx.outer, txn, dbi);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;
  cx.outer.mc_next = txn->mt_cursors[dbi];
  txn->mt_cursors[dbi] = &cx.outer;

  MDBX_val present_key = *key;
  if (F_ISSET(flags, MDBX_CURRENT | MDBX_NOOVERWRITE)) {
    /* в old_data значение для выбора конкретного дубликата */
    if (unlikely(!(txn->mt_dbs[dbi].md_flags & MDBX_DUPSORT))) {
      rc = MDBX_EINVAL;
      goto bailout;
    }

    /* убираем лишний бит, он был признаком запрошенного режима */
    flags -= MDBX_NOOVERWRITE;

    rc = mdbx_cursor_get(&cx.outer, &present_key, old_data, MDBX_GET_BOTH);
    if (rc != MDBX_SUCCESS)
      goto bailout;

    if (new_data) {
      /* обновление конкретного дубликата */
      if (mdbx_is_samedata(old_data, new_data))
        /* если данные совпадают, то ничего делать не надо */
        goto bailout;
    }
  } else {
    /* в old_data буфер для сохранения предыдущего значения */
    if (unlikely(new_data && old_data->iov_base == new_data->iov_base))
      return MDBX_EINVAL;
    MDBX_val present_data;
    rc = mdbx_cursor_get(&cx.outer, &present_key, &present_data, MDBX_SET_KEY);
    if (unlikely(rc != MDBX_SUCCESS)) {
      old_data->iov_base = NULL;
      old_data->iov_len = 0;
      if (rc != MDBX_NOTFOUND || (flags & MDBX_CURRENT))
        goto bailout;
    } else if (flags & MDBX_NOOVERWRITE) {
      rc = MDBX_KEYEXIST;
      *old_data = present_data;
      goto bailout;
    } else {
      MDBX_page *page = cx.outer.mc_pg[cx.outer.mc_top];
      if (txn->mt_dbs[dbi].md_flags & MDBX_DUPSORT) {
        if (flags & MDBX_CURRENT) {
          /* для не-уникальных ключей позволяем update/delete только если ключ
           * один */
          MDBX_node *node = page_node(page, cx.outer.mc_ki[cx.outer.mc_top]);
          if (F_ISSET(node_flags(node), F_DUPDATA)) {
            mdbx_tassert(txn, XCURSOR_INITED(&cx.outer) &&
                                  cx.outer.mc_xcursor->mx_db.md_entries > 1);
            if (cx.outer.mc_xcursor->mx_db.md_entries > 1) {
              rc = MDBX_EMULTIVAL;
              goto bailout;
            }
          }
          /* если данные совпадают, то ничего делать не надо */
          if (new_data && mdbx_is_samedata(&present_data, new_data)) {
            *old_data = *new_data;
            goto bailout;
          }
          /* В оригинальной LMDB флажок MDBX_CURRENT здесь приведет
           * к замене данных без учета MDBX_DUPSORT сортировки,
           * но здесь это в любом случае допустимо, так как мы
           * проверили что для ключа есть только одно значение. */
        } else if ((flags & MDBX_NODUPDATA) &&
                   mdbx_is_samedata(&present_data, new_data)) {
          /* если данные совпадают и установлен MDBX_NODUPDATA */
          rc = MDBX_KEYEXIST;
          goto bailout;
        }
      } else {
        /* если данные совпадают, то ничего делать не надо */
        if (new_data && mdbx_is_samedata(&present_data, new_data)) {
          *old_data = *new_data;
          goto bailout;
        }
        flags |= MDBX_CURRENT;
      }

      if (IS_DIRTY(page)) {
        if (unlikely(old_data->iov_len < present_data.iov_len)) {
          old_data->iov_base = NULL;
          old_data->iov_len = present_data.iov_len;
          rc = MDBX_RESULT_TRUE;
          goto bailout;
        }
        memcpy(old_data->iov_base, present_data.iov_base, present_data.iov_len);
        old_data->iov_len = present_data.iov_len;
      } else {
        *old_data = present_data;
      }
    }
  }

  if (likely(new_data))
    rc = mdbx_cursor_put(&cx.outer, key, new_data, flags);
  else
    rc = mdbx_cursor_del(&cx.outer, 0);

bailout:
  txn->mt_cursors[dbi] = cx.outer.mc_next;
  return rc;
}

/* Функция сообщает находится ли указанный адрес в "грязной" странице у
 * заданной пишущей транзакции. В конечном счете это позволяет избавиться от
 * лишнего копирования данных из НЕ-грязных страниц.
 *
 * "Грязные" страницы - это те, которые уже были изменены в ходе пишущей
 * транзакции. Соответственно, какие-либо дальнейшие изменения могут привести
 * к перезаписи таких страниц. Поэтому все функции, выполняющие изменения, в
 * качестве аргументов НЕ должны получать указатели на данные в таких
 * страницах. В свою очередь "НЕ грязные" страницы перед модификацией будут
 * скопированы.
 *
 * Другими словами, данные из "грязных" страниц должны быть либо скопированы
 * перед передачей в качестве аргументов для дальнейших модификаций, либо
 * отвергнуты на стадии проверки корректности аргументов.
 *
 * Таким образом, функция позволяет как избавиться от лишнего копирования,
 * так и выполнить более полную проверку аргументов.
 *
 * ВАЖНО: Передаваемый указатель должен указывать на начало данных. Только
 * так гарантируется что актуальный заголовок страницы будет физически
 * расположен в той-же странице памяти, в том числе для многостраничных
 * P_OVERFLOW страниц с длинными данными. */
int mdbx_is_dirty(const MDBX_txn *txn, const void *ptr) {
  int rc = check_txn(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (txn->mt_flags & MDBX_RDONLY)
    return MDBX_RESULT_FALSE;

  const MDBX_env *env = txn->mt_env;
  const ptrdiff_t offset = (uint8_t *)ptr - env->me_map;
  if (offset >= 0) {
    const pgno_t pgno = bytes2pgno(env, offset);
    if (likely(pgno < txn->mt_next_pgno)) {
      const MDBX_page *page = pgno2page(env, pgno);
      if (unlikely(page->mp_pgno != pgno)) {
        /* The ptr pointed into middle of a large page,
         * not to the beginning of a data. */
        return MDBX_EINVAL;
      }
      if (unlikely(page->mp_flags & (P_DIRTY | P_LOOSE | P_KEEP)))
        return MDBX_RESULT_TRUE;
      if (likely(txn->tw.spill_pages == nullptr))
        return MDBX_RESULT_FALSE;
      return mdbx_pnl_exist(txn->tw.spill_pages, pgno << 1) ? MDBX_RESULT_TRUE
                                                            : MDBX_RESULT_FALSE;
    }
    if ((size_t)offset < env->me_dxb_mmap.limit) {
      /* Указатель адресует что-то в пределах mmap, но за границей
       * распределенных страниц. Такое может случится если mdbx_is_dirty()
       * вызывает после операции, в ходе которой гразная страница попала
       * в loose и затем была возвращена в нераспределенное пространство. */
      return MDBX_RESULT_TRUE;
    }
  }

  /* Страница вне используемого mmap-диапазона, т.е. либо в функцию был
   * передан некорректный адрес, либо адрес в теневой странице, которая была
   * выделена посредством malloc().
   *
   * Для WRITE_MAP режима такая страница однозначно "не грязная",
   * а для режимов без WRITE_MAP следует просматривать списки dirty
   * и spilled страниц у каких-либо транзакций (в том числе дочерних).
   *
   * Поэтому для WRITE_MAP возвращаем false, а для остальных режимов
   * всегда true. Такая логика имеет ряд преимуществ:
   *  - не тратим время на просмотр списков;
   *  - результат всегда безопасен (может быть ложно-положительным,
   *    но не ложно-отрицательным);
   *  - результат не зависит от вложенности транзакций и от относительного
   *    положения переданной транзакции в этой рекурсии. */
  return (env->me_flags & MDBX_WRITEMAP) ? MDBX_RESULT_FALSE : MDBX_RESULT_TRUE;
}

int mdbx_dbi_sequence(MDBX_txn *txn, MDBX_dbi dbi, uint64_t *result,
                      uint64_t increment) {
  int rc = check_txn(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!TXN_DBI_EXIST(txn, dbi, DB_USRVALID)))
    return MDBX_EINVAL;

  if (unlikely(TXN_DBI_CHANGED(txn, dbi)))
    return MDBX_BAD_DBI;

  if (unlikely(txn->mt_dbflags[dbi] & DB_STALE)) {
    rc = mdbx_fetch_sdb(txn, dbi);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }

  MDBX_db *dbs = &txn->mt_dbs[dbi];
  if (likely(result))
    *result = dbs->md_seq;

  if (likely(increment > 0)) {
    if (unlikely(txn->mt_flags & MDBX_RDONLY))
      return MDBX_EACCESS;

    uint64_t new = dbs->md_seq + increment;
    if (unlikely(new < increment))
      return MDBX_RESULT_TRUE;

    mdbx_tassert(txn, new > dbs->md_seq);
    dbs->md_seq = new;
    txn->mt_flags |= MDBX_TXN_DIRTY;
    txn->mt_dbflags[dbi] |= DB_DIRTY;
  }

  return MDBX_SUCCESS;
}

/*----------------------------------------------------------------------------*/

__cold intptr_t mdbx_limits_keysize_max(intptr_t pagesize) {
  if (pagesize < 1)
    pagesize = (intptr_t)mdbx_syspagesize();
  else if (unlikely(pagesize < (intptr_t)MIN_PAGESIZE ||
                    pagesize > (intptr_t)MAX_PAGESIZE ||
                    !is_powerof2((size_t)pagesize)))
    return (MDBX_EINVAL > 0) ? -MDBX_EINVAL : MDBX_EINVAL;

  return mdbx_maxkey(mdbx_nodemax(pagesize));
}

__cold intptr_t mdbx_limits_dbsize_min(intptr_t pagesize) {
  if (pagesize < 1)
    pagesize = (intptr_t)mdbx_syspagesize();
  else if (unlikely(pagesize < (intptr_t)MIN_PAGESIZE ||
                    pagesize > (intptr_t)MAX_PAGESIZE ||
                    !is_powerof2((size_t)pagesize)))
    return (MDBX_EINVAL > 0) ? -MDBX_EINVAL : MDBX_EINVAL;

  return MIN_PAGENO * pagesize;
}

__cold intptr_t mdbx_limits_dbsize_max(intptr_t pagesize) {
  if (pagesize < 1)
    pagesize = (intptr_t)mdbx_syspagesize();
  else if (unlikely(pagesize < (intptr_t)MIN_PAGESIZE ||
                    pagesize > (intptr_t)MAX_PAGESIZE ||
                    !is_powerof2((size_t)pagesize)))
    return (MDBX_EINVAL > 0) ? -MDBX_EINVAL : MDBX_EINVAL;

  const uint64_t limit = MAX_PAGENO * (uint64_t)pagesize;
  return (limit < (intptr_t)MAX_MAPSIZE) ? (intptr_t)limit
                                         : (intptr_t)MAX_MAPSIZE;
}

__cold intptr_t mdbx_limits_txnsize_max(intptr_t pagesize) {
  if (pagesize < 1)
    pagesize = (intptr_t)mdbx_syspagesize();
  else if (unlikely(pagesize < (intptr_t)MIN_PAGESIZE ||
                    pagesize > (intptr_t)MAX_PAGESIZE ||
                    !is_powerof2((size_t)pagesize)))
    return (MDBX_EINVAL > 0) ? -MDBX_EINVAL : MDBX_EINVAL;

  return pagesize * (MDBX_DPL_TXNFULL - 1);
}

/*** Attribute support functions for Nexenta **********************************/
#ifdef MDBX_NEXENTA_ATTRS

static __inline int mdbx_attr_peek(MDBX_val *data, mdbx_attr_t *attrptr) {
  if (unlikely(data->iov_len < sizeof(mdbx_attr_t)))
    return MDBX_INCOMPATIBLE;

  if (likely(attrptr != NULL))
    *attrptr = *(mdbx_attr_t *)data->iov_base;
  data->iov_len -= sizeof(mdbx_attr_t);
  data->iov_base =
      likely(data->iov_len > 0) ? ((mdbx_attr_t *)data->iov_base) + 1 : NULL;

  return MDBX_SUCCESS;
}

static __inline int mdbx_attr_poke(MDBX_val *reserved, MDBX_val *data,
                                   mdbx_attr_t attr, unsigned flags) {
  mdbx_attr_t *space = reserved->iov_base;
  if (flags & MDBX_RESERVE) {
    if (likely(data != NULL)) {
      data->iov_base = data->iov_len ? space + 1 : NULL;
    }
  } else {
    *space = attr;
    if (likely(data != NULL)) {
      memcpy(space + 1, data->iov_base, data->iov_len);
    }
  }

  return MDBX_SUCCESS;
}

int mdbx_cursor_get_attr(MDBX_cursor *mc, MDBX_val *key, MDBX_val *data,
                         mdbx_attr_t *attrptr, MDBX_cursor_op op) {
  int rc = mdbx_cursor_get(mc, key, data, op);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  return mdbx_attr_peek(data, attrptr);
}

int mdbx_get_attr(MDBX_txn *txn, MDBX_dbi dbi, MDBX_val *key, MDBX_val *data,
                  uint64_t *attrptr) {
  int rc = mdbx_get(txn, dbi, key, data);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  return mdbx_attr_peek(data, attrptr);
}

int mdbx_put_attr(MDBX_txn *txn, MDBX_dbi dbi, MDBX_val *key, MDBX_val *data,
                  mdbx_attr_t attr, unsigned flags) {
  MDBX_val reserve;
  reserve.iov_base = NULL;
  reserve.iov_len = (data ? data->iov_len : 0) + sizeof(mdbx_attr_t);

  int rc = mdbx_put(txn, dbi, key, &reserve, flags | MDBX_RESERVE);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  return mdbx_attr_poke(&reserve, data, attr, flags);
}

int mdbx_cursor_put_attr(MDBX_cursor *cursor, MDBX_val *key, MDBX_val *data,
                         mdbx_attr_t attr, unsigned flags) {
  MDBX_val reserve;
  reserve.iov_base = NULL;
  reserve.iov_len = (data ? data->iov_len : 0) + sizeof(mdbx_attr_t);

  int rc = mdbx_cursor_put(cursor, key, &reserve, flags | MDBX_RESERVE);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  return mdbx_attr_poke(&reserve, data, attr, flags);
}

int mdbx_set_attr(MDBX_txn *txn, MDBX_dbi dbi, MDBX_val *key, MDBX_val *data,
                  mdbx_attr_t attr) {
  if (unlikely(!key || !txn))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_signature != MDBX_MT_SIGNATURE))
    return MDBX_VERSION_MISMATCH;

  if (unlikely(!TXN_DBI_EXIST(txn, dbi, DB_USRVALID)))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_flags & (MDBX_RDONLY | MDBX_TXN_BLOCKED)))
    return (txn->mt_flags & MDBX_RDONLY) ? MDBX_EACCESS : MDBX_BAD_TXN;

  MDBX_cursor_couple cx;
  MDBX_val old_data;
  int rc = mdbx_cursor_init(&cx.outer, txn, dbi);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;
  rc = mdbx_cursor_set(&cx.outer, key, &old_data, MDBX_SET, NULL);
  if (unlikely(rc != MDBX_SUCCESS)) {
    if (rc == MDBX_NOTFOUND && data) {
      cx.outer.mc_next = txn->mt_cursors[dbi];
      txn->mt_cursors[dbi] = &cx.outer;
      rc = mdbx_cursor_put_attr(&cx.outer, key, data, attr, 0);
      txn->mt_cursors[dbi] = cx.outer.mc_next;
    }
    return rc;
  }

  mdbx_attr_t old_attr = 0;
  rc = mdbx_attr_peek(&old_data, &old_attr);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (old_attr == attr && (!data || (data->iov_len == old_data.iov_len &&
                                     memcmp(data->iov_base, old_data.iov_base,
                                            old_data.iov_len) == 0)))
    return MDBX_SUCCESS;

  cx.outer.mc_next = txn->mt_cursors[dbi];
  txn->mt_cursors[dbi] = &cx.outer;
  rc = mdbx_cursor_put_attr(&cx.outer, key, data ? data : &old_data, attr,
                            MDBX_CURRENT);
  txn->mt_cursors[dbi] = cx.outer.mc_next;
  return rc;
}
#endif /* MDBX_NEXENTA_ATTRS */

/******************************************************************************/
/* *INDENT-OFF* */
/* clang-format off */

__dll_export
#ifdef __attribute_used__
    __attribute_used__
#elif defined(__GNUC__) || __has_attribute(__used__)
    __attribute__((__used__))
#endif
#ifdef __attribute_externally_visible__
        __attribute_externally_visible__
#elif (defined(__GNUC__) && !defined(__clang__)) ||                            \
    __has_attribute(__externally_visible__)
    __attribute__((__externally_visible__))
#endif
    const mdbx_build_info mdbx_build = {
#ifdef MDBX_BUILD_TIMESTAMP
    MDBX_BUILD_TIMESTAMP
#else
    __DATE__ " " __TIME__
#endif /* MDBX_BUILD_TIMESTAMP */

    ,
#ifdef MDBX_BUILD_TARGET
    MDBX_BUILD_TARGET
#else
  #if defined(__ANDROID__)
    "Android"
  #elif defined(__linux__) || defined(__gnu_linux__)
    "Linux"
  #elif defined(EMSCRIPTEN) || defined(__EMSCRIPTEN__)
    "webassembly"
  #elif defined(__CYGWIN__)
    "CYGWIN"
  #elif defined(_WIN64) || defined(_WIN32) || defined(__TOS_WIN__) \
      || defined(__WINDOWS__)
    "Windows"
  #elif defined(__APPLE__)
    #if (defined(TARGET_OS_IPHONE) && TARGET_OS_IPHONE) \
      || (defined(TARGET_IPHONE_SIMULATOR) && TARGET_IPHONE_SIMULATOR)
      "iOS"
    #else
      "MacOS"
    #endif
  #elif defined(__FreeBSD__)
    "FreeBSD"
  #elif defined(__DragonFly__)
    "DragonFlyBSD"
  #elif defined(__NetBSD__) || defined(__NETBSD__)
    "NetBSD"
  #elif defined(__OpenBSD__)
    "OpenBSD"
  #elif defined(__bsdi__)
    "UnixBSDI"
  #elif defined(__MACH__)
    "MACH"
  #elif (defined(_HPUX_SOURCE) || defined(__hpux) || defined(__HP_aCC))
    "HPUX"
  #elif defined(_AIX)
    "AIX"
  #elif defined(__sun) && defined(__SVR4)
    "Solaris"
  #elif defined(__BSD__) || defined(BSD)
    "UnixBSD"
  #elif defined(__unix__) || defined(UNIX) || defined(__unix) \
      || defined(__UNIX) || defined(__UNIX__)
    "UNIX"
  #elif defined(_POSIX_VERSION)
    "POSIX" STRINGIFY(_POSIX_VERSION)
  #else
    "UnknownOS"
  #endif /* Target OS */

    "-"

  #if defined(__amd64__)
    "AMD64"
  #elif defined(__ia32__)
    "IA32"
  #elif defined(__e2k__) || defined(__elbrus__)
    "Elbrus"
  #elif defined(__alpha__) || defined(__alpha) || defined(_M_ALPHA)
    "Alpha"
  #elif defined(__aarch64__) || defined(_M_ARM64)
    "ARM64"
  #elif defined(__arm__) || defined(__thumb__) || defined(__TARGET_ARCH_ARM) \
      || defined(__TARGET_ARCH_THUMB) || defined(_ARM) || defined(_M_ARM) \
      || defined(_M_ARMT) || defined(__arm)
    "ARM"
  #elif defined(__mips64) || defined(__mips64__) || (defined(__mips) && (__mips >= 64))
    "MIPS64"
  #elif if defined(__mips__) || defined(__mips) || defined(_R4000) || defined(__MIPS__)
    "MIPS"
  #elif defined(__hppa64__) || defined(__HPPA64__) || defined(__hppa64)
    "PARISC64"
  #elif defined(__hppa__) || defined(__HPPA__) || defined(__hppa)
    "PARISC"
  #elif defined(__ia64__) || defined(__ia64) || defined(_IA64) \
      || defined(__IA64__) || defined(_M_IA64) || defined(__itanium__)
    "Itanium"
  #elif defined(__powerpc64__) || defined(__ppc64__) || defined(__ppc64) \
      || defined(__powerpc64) || defined(_ARCH_PPC64)
    "PowerPC64"
  #elif defined(__powerpc__) || defined(__ppc__) || defined(__powerpc) \
      || defined(__ppc) || defined(_ARCH_PPC) || defined(__PPC__) || defined(__POWERPC__)
    "PowerPC"
  #elif defined(__sparc64__) || defined(__sparc64)
    "SPARC64"
  #elif defined(__sparc__) || defined(__sparc)
    "SPARC"
  #elif defined(__s390__) || defined(__s390) || defined(__zarch__) || defined(__zarch)
    "S390"
  #else
    "UnknownARCH"
  #endif
#endif /* MDBX_BUILD_TARGET */

#ifdef MDBX_BUILD_CONFIG
# if defined(_MSC_VER)
#   pragma message("Configuration-depended MDBX_BUILD_CONFIG: " MDBX_BUILD_CONFIG)
# endif
    "-" MDBX_BUILD_CONFIG
#endif /* MDBX_BUILD_CONFIG */
    ,
    "MDBX_DEBUG=" STRINGIFY(MDBX_DEBUG)
#ifdef MDBX_LOGLEVEL_BUILD
    " MDBX_LOGLEVEL_BUILD=" STRINGIFY(MDBX_LOGLEVEL_BUILD)
#endif /* MDBX_LOGLEVEL_BUILD */
    " MDBX_WORDBITS=" STRINGIFY(MDBX_WORDBITS)
    " BYTE_ORDER="
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    "LITTLE_ENDIAN"
#elif __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
    "BIG_ENDIAN"
#else
    #error "FIXME: Unsupported byte order"
#endif /* __BYTE_ORDER__ */
    " MDBX_TXN_CHECKPID=" MDBX_TXN_CHECKPID_CONFIG
    " MDBX_TXN_CHECKOWNER=" MDBX_TXN_CHECKOWNER_CONFIG
    " MDBX_64BIT_ATOMIC=" MDBX_64BIT_ATOMIC_CONFIG
    " MDBX_64BIT_CAS=" MDBX_64BIT_CAS_CONFIG
    " MDBX_TRUST_RTC=" MDBX_TRUST_RTC_CONFIG
#ifdef __SANITIZE_ADDRESS__
    " SANITIZE_ADDRESS=YES"
#endif /* __SANITIZE_ADDRESS__ */
#ifdef MDBX_USE_VALGRIND
    " MDBX_USE_VALGRIND=YES"
#endif /* MDBX_USE_VALGRIND */
#ifdef _GNU_SOURCE
    " _GNU_SOURCE=YES"
#else
    " _GNU_SOURCE=NO"
#endif /* _GNU_SOURCE */
#ifdef __APPLE__
    " MDBX_OSX_SPEED_INSTEADOF_DURABILITY=" STRINGIFY(MDBX_OSX_SPEED_INSTEADOF_DURABILITY)
#endif /* MacOS */
#if defined(_WIN32) || defined(_WIN64)
    " MDBX_AVOID_CRT=" STRINGIFY(MDBX_AVOID_CRT)
    " MDBX_CONFIG_MANUAL_TLS_CALLBACK=" STRINGIFY(MDBX_CONFIG_MANUAL_TLS_CALLBACK)
    " MDBX_BUILD_SHARED_LIBRARY=" STRINGIFY(MDBX_BUILD_SHARED_LIBRARY)
    " WINVER=" STRINGIFY(WINVER)
#else /* Windows */
    " MDBX_USE_ROBUST=" MDBX_USE_ROBUST_CONFIG
    " MDBX_USE_OFDLOCKS=" MDBX_USE_OFDLOCKS_CONFIG
#endif /* !Windows */
#ifdef MDBX_OSAL_LOCK
    " MDBX_OSAL_LOCK=" STRINGIFY(MDBX_OSAL_LOCK)
#endif
    " MDBX_CACHELINE_SIZE=" STRINGIFY(MDBX_CACHELINE_SIZE)
    " MDBX_CPU_WRITEBACK_IS_COHERENT=" STRINGIFY(MDBX_CPU_WRITEBACK_IS_COHERENT)
    " MDBX_UNALIGNED_OK=" STRINGIFY(MDBX_UNALIGNED_OK)
    " MDBX_PNL_ASCENDING=" STRINGIFY(MDBX_PNL_ASCENDING)
    ,
#ifdef MDBX_BUILD_COMPILER
    MDBX_BUILD_COMPILER
#else
  #ifdef __INTEL_COMPILER
    "Intel C/C++ " STRINGIFY(__INTEL_COMPILER)
  #elsif defined(__apple_build_version__)
    "Apple clang " STRINGIFY(__apple_build_version__)
  #elif defined(__ibmxl__)
    "IBM clang C " STRINGIFY(__ibmxl_version__) "." STRINGIFY(__ibmxl_release__)
    "." STRINGIFY(__ibmxl_modification__) "." STRINGIFY(__ibmxl_ptf_fix_level__)
  #elif defined(__clang__)
    "clang " STRINGIFY(__clang_version__)
  #elif defined(__MINGW64__)
    "MINGW-64 " STRINGIFY(__MINGW64_MAJOR_VERSION) "." STRINGIFY(__MINGW64_MINOR_VERSION)
  #elif defined(__MINGW32__)
    "MINGW-32 " STRINGIFY(__MINGW32_MAJOR_VERSION) "." STRINGIFY(__MINGW32_MINOR_VERSION)
  #elif defined(__IBMC__)
    "IBM C " STRINGIFY(__IBMC__)
  #elif defined(__GNUC__)
    "GNU C/C++ "
    #ifdef __VERSION__
      __VERSION__
    #else
      STRINGIFY(__GNUC__) "." STRINGIFY(__GNUC_MINOR__) "." STRINGIFY(__GNUC_PATCHLEVEL__)
    #endif
  #elif defined(_MSC_VER)
    "MSVC " STRINGIFY(_MSC_FULL_VER) "-" STRINGIFY(_MSC_BUILD)
  #else
    "Unknown compiler"
  #endif
#endif /* MDBX_BUILD_COMPILER */
    ,
#ifdef MDBX_BUILD_FLAGS
    MDBX_BUILD_FLAGS
#endif /* MDBX_BUILD_FLAGS */
#ifdef MDBX_BUILD_FLAGS_CONFIG
    MDBX_BUILD_FLAGS_CONFIG
#endif /* MDBX_BUILD_FLAGS_CONFIG */
};

#ifdef __SANITIZE_ADDRESS__
LIBMDBX_API __attribute__((__weak__)) const char *__asan_default_options() {
  return "symbolize=1:allow_addr2line=1:"
#ifdef _DEBUG
         "debug=1:"
#endif /* _DEBUG */
         "report_globals=1:"
         "replace_str=1:replace_intrin=1:"
         "malloc_context_size=9:"
         "detect_leaks=1:"
         "check_printf=1:"
         "detect_deadlocks=1:"
#ifndef LTO_ENABLED
         "check_initialization_order=1:"
#endif
         "detect_stack_use_after_return=1:"
         "intercept_tls_get_addr=1:"
         "decorate_proc_maps=1:"
         "abort_on_error=1";
}
#endif /* __SANITIZE_ADDRESS__ */

/* *INDENT-ON* */
/* clang-format on */
