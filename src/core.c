/*
 * Copyright 2015-2024 Leonid Yuriev <leo@yuriev.ru>.
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
 * Internal inline functions */

MDBX_NOTHROW_CONST_FUNCTION static size_t branchless_abs(intptr_t value) {
  assert(value > INT_MIN);
  const size_t expanded_sign =
      (size_t)(value >> (sizeof(value) * CHAR_BIT - 1));
  return ((size_t)value + expanded_sign) ^ expanded_sign;
}

/* Pack/Unpack 16-bit values for Grow step & Shrink threshold */
MDBX_NOTHROW_CONST_FUNCTION static __inline pgno_t me2v(size_t m, size_t e) {
  assert(m < 2048 && e < 8);
  return (pgno_t)(32768 + ((m + 1) << (e + 8)));
}

MDBX_NOTHROW_CONST_FUNCTION static __inline uint16_t v2me(size_t v, size_t e) {
  assert(v > (e ? me2v(2047, e - 1) : 32768));
  assert(v <= me2v(2047, e));
  size_t m = (v - 32768 + ((size_t)1 << (e + 8)) - 1) >> (e + 8);
  m -= m > 0;
  assert(m < 2048 && e < 8);
  // f e d c b a 9 8 7 6 5 4 3 2 1 0
  // 1 e e e m m m m m m m m m m m 1
  const uint16_t pv = (uint16_t)(0x8001 + (e << 12) + (m << 1));
  assert(pv != 65535);
  return pv;
}

/* Convert 16-bit packed (exponential quantized) value to number of pages */
MDBX_NOTHROW_CONST_FUNCTION static pgno_t pv2pages(uint16_t pv) {
  if ((pv & 0x8001) != 0x8001)
    return pv;
  if (pv == 65535)
    return 65536;
  // f e d c b a 9 8 7 6 5 4 3 2 1 0
  // 1 e e e m m m m m m m m m m m 1
  return me2v((pv >> 1) & 2047, (pv >> 12) & 7);
}

/* Convert number of pages to 16-bit packed (exponential quantized) value */
MDBX_NOTHROW_CONST_FUNCTION static uint16_t pages2pv(size_t pages) {
  if (pages < 32769 || (pages < 65536 && (pages & 1) == 0))
    return (uint16_t)pages;
  if (pages <= me2v(2047, 0))
    return v2me(pages, 0);
  if (pages <= me2v(2047, 1))
    return v2me(pages, 1);
  if (pages <= me2v(2047, 2))
    return v2me(pages, 2);
  if (pages <= me2v(2047, 3))
    return v2me(pages, 3);
  if (pages <= me2v(2047, 4))
    return v2me(pages, 4);
  if (pages <= me2v(2047, 5))
    return v2me(pages, 5);
  if (pages <= me2v(2047, 6))
    return v2me(pages, 6);
  return (pages < me2v(2046, 7)) ? v2me(pages, 7) : 65533;
}

/*------------------------------------------------------------------------------
 * Unaligned access */

MDBX_MAYBE_UNUSED MDBX_NOTHROW_CONST_FUNCTION static __always_inline size_t
field_alignment(size_t alignment_baseline, size_t field_offset) {
  size_t merge = alignment_baseline | (size_t)field_offset;
  return merge & -(int)merge;
}

/* read-thunk for UB-sanitizer */
MDBX_NOTHROW_PURE_FUNCTION static __always_inline uint8_t
peek_u8(const uint8_t *const __restrict ptr) {
  return *ptr;
}

/* write-thunk for UB-sanitizer */
static __always_inline void poke_u8(uint8_t *const __restrict ptr,
                                    const uint8_t v) {
  *ptr = v;
}

MDBX_NOTHROW_PURE_FUNCTION static __always_inline uint16_t
unaligned_peek_u16(const size_t expected_alignment, const void *const ptr) {
  assert((uintptr_t)ptr % expected_alignment == 0);
  if (MDBX_UNALIGNED_OK >= 2 || (expected_alignment % sizeof(uint16_t)) == 0)
    return *(const uint16_t *)ptr;
  else {
#if defined(__unaligned) || defined(_M_ARM) || defined(_M_ARM64) ||            \
    defined(_M_X64) || defined(_M_IA64)
    return *(const __unaligned uint16_t *)ptr;
#else
    uint16_t v;
    memcpy(&v, ptr, sizeof(v));
    return v;
#endif /* _MSC_VER || __unaligned */
  }
}

static __always_inline void unaligned_poke_u16(const size_t expected_alignment,
                                               void *const __restrict ptr,
                                               const uint16_t v) {
  assert((uintptr_t)ptr % expected_alignment == 0);
  if (MDBX_UNALIGNED_OK >= 2 || (expected_alignment % sizeof(v)) == 0)
    *(uint16_t *)ptr = v;
  else {
#if defined(__unaligned) || defined(_M_ARM) || defined(_M_ARM64) ||            \
    defined(_M_X64) || defined(_M_IA64)
    *((uint16_t __unaligned *)ptr) = v;
#else
    memcpy(ptr, &v, sizeof(v));
#endif /* _MSC_VER || __unaligned */
  }
}

MDBX_NOTHROW_PURE_FUNCTION static __always_inline uint32_t unaligned_peek_u32(
    const size_t expected_alignment, const void *const __restrict ptr) {
  assert((uintptr_t)ptr % expected_alignment == 0);
  if (MDBX_UNALIGNED_OK >= 4 || (expected_alignment % sizeof(uint32_t)) == 0)
    return *(const uint32_t *)ptr;
  else if ((expected_alignment % sizeof(uint16_t)) == 0) {
    const uint16_t lo =
        ((const uint16_t *)ptr)[__BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__];
    const uint16_t hi =
        ((const uint16_t *)ptr)[__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__];
    return lo | (uint32_t)hi << 16;
  } else {
#if defined(__unaligned) || defined(_M_ARM) || defined(_M_ARM64) ||            \
    defined(_M_X64) || defined(_M_IA64)
    return *(const __unaligned uint32_t *)ptr;
#else
    uint32_t v;
    memcpy(&v, ptr, sizeof(v));
    return v;
#endif /* _MSC_VER || __unaligned */
  }
}

static __always_inline void unaligned_poke_u32(const size_t expected_alignment,
                                               void *const __restrict ptr,
                                               const uint32_t v) {
  assert((uintptr_t)ptr % expected_alignment == 0);
  if (MDBX_UNALIGNED_OK >= 4 || (expected_alignment % sizeof(v)) == 0)
    *(uint32_t *)ptr = v;
  else if ((expected_alignment % sizeof(uint16_t)) == 0) {
    ((uint16_t *)ptr)[__BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__] = (uint16_t)v;
    ((uint16_t *)ptr)[__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__] =
        (uint16_t)(v >> 16);
  } else {
#if defined(__unaligned) || defined(_M_ARM) || defined(_M_ARM64) ||            \
    defined(_M_X64) || defined(_M_IA64)
    *((uint32_t __unaligned *)ptr) = v;
#else
    memcpy(ptr, &v, sizeof(v));
#endif /* _MSC_VER || __unaligned */
  }
}

MDBX_NOTHROW_PURE_FUNCTION static __always_inline uint64_t unaligned_peek_u64(
    const size_t expected_alignment, const void *const __restrict ptr) {
  assert((uintptr_t)ptr % expected_alignment == 0);
  if (MDBX_UNALIGNED_OK >= 8 || (expected_alignment % sizeof(uint64_t)) == 0)
    return *(const uint64_t *)ptr;
  else if ((expected_alignment % sizeof(uint32_t)) == 0) {
    const uint32_t lo =
        ((const uint32_t *)ptr)[__BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__];
    const uint32_t hi =
        ((const uint32_t *)ptr)[__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__];
    return lo | (uint64_t)hi << 32;
  } else {
#if defined(__unaligned) || defined(_M_ARM) || defined(_M_ARM64) ||            \
    defined(_M_X64) || defined(_M_IA64)
    return *(const __unaligned uint64_t *)ptr;
#else
    uint64_t v;
    memcpy(&v, ptr, sizeof(v));
    return v;
#endif /* _MSC_VER || __unaligned */
  }
}

static __always_inline uint64_t
unaligned_peek_u64_volatile(const size_t expected_alignment,
                            const volatile void *const __restrict ptr) {
  assert((uintptr_t)ptr % expected_alignment == 0);
  assert(expected_alignment % sizeof(uint32_t) == 0);
  if (MDBX_UNALIGNED_OK >= 8 || (expected_alignment % sizeof(uint64_t)) == 0)
    return *(const volatile uint64_t *)ptr;
  else {
#if defined(__unaligned) || defined(_M_ARM) || defined(_M_ARM64) ||            \
    defined(_M_X64) || defined(_M_IA64)
    return *(const volatile __unaligned uint64_t *)ptr;
#else
    const uint32_t lo = ((const volatile uint32_t *)
                             ptr)[__BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__];
    const uint32_t hi = ((const volatile uint32_t *)
                             ptr)[__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__];
    return lo | (uint64_t)hi << 32;
#endif /* _MSC_VER || __unaligned */
  }
}

static __always_inline void unaligned_poke_u64(const size_t expected_alignment,
                                               void *const __restrict ptr,
                                               const uint64_t v) {
  assert((uintptr_t)ptr % expected_alignment == 0);
  if (MDBX_UNALIGNED_OK >= 8 || (expected_alignment % sizeof(v)) == 0)
    *(uint64_t *)ptr = v;
  else if ((expected_alignment % sizeof(uint32_t)) == 0) {
    ((uint32_t *)ptr)[__BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__] = (uint32_t)v;
    ((uint32_t *)ptr)[__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__] =
        (uint32_t)(v >> 32);
  } else {
#if defined(__unaligned) || defined(_M_ARM) || defined(_M_ARM64) ||            \
    defined(_M_X64) || defined(_M_IA64)
    *((uint64_t __unaligned *)ptr) = v;
#else
    memcpy(ptr, &v, sizeof(v));
#endif /* _MSC_VER || __unaligned */
  }
}

#define UNALIGNED_PEEK_8(ptr, struct, field)                                   \
  peek_u8(ptr_disp(ptr, offsetof(struct, field)))
#define UNALIGNED_POKE_8(ptr, struct, field, value)                            \
  poke_u8(ptr_disp(ptr, offsetof(struct, field)), value)

#define UNALIGNED_PEEK_16(ptr, struct, field)                                  \
  unaligned_peek_u16(1, ptr_disp(ptr, offsetof(struct, field)))
#define UNALIGNED_POKE_16(ptr, struct, field, value)                           \
  unaligned_poke_u16(1, ptr_disp(ptr, offsetof(struct, field)), value)

#define UNALIGNED_PEEK_32(ptr, struct, field)                                  \
  unaligned_peek_u32(1, ptr_disp(ptr, offsetof(struct, field)))
#define UNALIGNED_POKE_32(ptr, struct, field, value)                           \
  unaligned_poke_u32(1, ptr_disp(ptr, offsetof(struct, field)), value)

#define UNALIGNED_PEEK_64(ptr, struct, field)                                  \
  unaligned_peek_u64(1, ptr_disp(ptr, offsetof(struct, field)))
#define UNALIGNED_POKE_64(ptr, struct, field, value)                           \
  unaligned_poke_u64(1, ptr_disp(ptr, offsetof(struct, field)), value)

/* Get the page number pointed to by a branch node */
MDBX_NOTHROW_PURE_FUNCTION static __always_inline pgno_t
node_pgno(const MDBX_node *const __restrict node) {
  pgno_t pgno = UNALIGNED_PEEK_32(node, MDBX_node, mn_pgno32);
  if (sizeof(pgno) > 4)
    pgno |= ((uint64_t)UNALIGNED_PEEK_8(node, MDBX_node, mn_extra)) << 32;
  return pgno;
}

/* Set the page number in a branch node */
static __always_inline void node_set_pgno(MDBX_node *const __restrict node,
                                          pgno_t pgno) {
  assert(pgno >= MIN_PAGENO && pgno <= MAX_PAGENO);

  UNALIGNED_POKE_32(node, MDBX_node, mn_pgno32, (uint32_t)pgno);
  if (sizeof(pgno) > 4)
    UNALIGNED_POKE_8(node, MDBX_node, mn_extra,
                     (uint8_t)((uint64_t)pgno >> 32));
}

/* Get the size of the data in a leaf node */
MDBX_NOTHROW_PURE_FUNCTION static __always_inline size_t
node_ds(const MDBX_node *const __restrict node) {
  return UNALIGNED_PEEK_32(node, MDBX_node, mn_dsize);
}

/* Set the size of the data for a leaf node */
static __always_inline void node_set_ds(MDBX_node *const __restrict node,
                                        size_t size) {
  assert(size < INT_MAX);
  UNALIGNED_POKE_32(node, MDBX_node, mn_dsize, (uint32_t)size);
}

/* The size of a key in a node */
MDBX_NOTHROW_PURE_FUNCTION static __always_inline size_t
node_ks(const MDBX_node *const __restrict node) {
  return UNALIGNED_PEEK_16(node, MDBX_node, mn_ksize);
}

/* Set the size of the key for a leaf node */
static __always_inline void node_set_ks(MDBX_node *const __restrict node,
                                        size_t size) {
  assert(size < INT16_MAX);
  UNALIGNED_POKE_16(node, MDBX_node, mn_ksize, (uint16_t)size);
}

MDBX_NOTHROW_PURE_FUNCTION static __always_inline uint8_t
node_flags(const MDBX_node *const __restrict node) {
  return UNALIGNED_PEEK_8(node, MDBX_node, mn_flags);
}

static __always_inline void node_set_flags(MDBX_node *const __restrict node,
                                           uint8_t flags) {
  UNALIGNED_POKE_8(node, MDBX_node, mn_flags, flags);
}

/* Size of the node header, excluding dynamic data at the end */
#define NODESIZE offsetof(MDBX_node, mn_data)

/* Address of the key for the node */
MDBX_NOTHROW_PURE_FUNCTION static __always_inline void *
node_key(const MDBX_node *const __restrict node) {
  return ptr_disp(node, NODESIZE);
}

/* Address of the data for a node */
MDBX_NOTHROW_PURE_FUNCTION static __always_inline void *
node_data(const MDBX_node *const __restrict node) {
  return ptr_disp(node_key(node), node_ks(node));
}

/* Size of a node in a leaf page with a given key and data.
 * This is node header plus key plus data size. */
MDBX_NOTHROW_CONST_FUNCTION static __always_inline size_t
node_size_len(const size_t key_len, const size_t value_len) {
  return NODESIZE + EVEN(key_len + value_len);
}
MDBX_NOTHROW_PURE_FUNCTION static __always_inline size_t
node_size(const MDBX_val *key, const MDBX_val *value) {
  return node_size_len(key ? key->iov_len : 0, value ? value->iov_len : 0);
}

MDBX_NOTHROW_PURE_FUNCTION static __always_inline pgno_t
peek_pgno(const void *const __restrict ptr) {
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

static __always_inline void poke_pgno(void *const __restrict ptr,
                                      const pgno_t pgno) {
  if (sizeof(pgno) == sizeof(uint32_t))
    unaligned_poke_u32(1, ptr, pgno);
  else if (sizeof(pgno) == sizeof(uint64_t))
    unaligned_poke_u64(1, ptr, pgno);
  else
    memcpy(ptr, &pgno, sizeof(pgno));
}

MDBX_NOTHROW_PURE_FUNCTION static __always_inline pgno_t
node_largedata_pgno(const MDBX_node *const __restrict node) {
  assert(node_flags(node) & F_BIGDATA);
  return peek_pgno(node_data(node));
}

/*------------------------------------------------------------------------------
 * Nodes, Keys & Values length limitation factors:
 *
 * BRANCH_NODE_MAX
 *   Branch-page must contain at least two nodes, within each a key and a child
 *   page number. But page can't be split if it contains less that 4 keys,
 *   i.e. a page should not overflow before adding the fourth key. Therefore,
 *   at least 3 branch-node should fit in the single branch-page. Further, the
 *   first node of a branch-page doesn't contain a key, i.e. the first node
 *   is always require space just for itself. Thus:
 *       PAGEROOM = pagesize - page_hdr_len;
 *       BRANCH_NODE_MAX = even_floor(
 *         (PAGEROOM - sizeof(indx_t) - NODESIZE) / (3 - 1) - sizeof(indx_t));
 *       KEYLEN_MAX = BRANCH_NODE_MAX - node_hdr_len;
 *
 * LEAF_NODE_MAX
 *   Leaf-node must fit into single leaf-page, where a value could be placed on
 *   a large/overflow page. However, may require to insert a nearly page-sized
 *   node between two large nodes are already fill-up a page. In this case the
 *   page must be split to two if some pair of nodes fits on one page, or
 *   otherwise the page should be split to the THREE with a single node
 *   per each of ones. Such 1-into-3 page splitting is costly and complex since
 *   requires TWO insertion into the parent page, that could lead to split it
 *   and so on up to the root. Therefore double-splitting is avoided here and
 *   the maximum node size is half of a leaf page space:
 *       LEAF_NODE_MAX = even_floor(PAGEROOM / 2 - sizeof(indx_t));
 *       DATALEN_NO_OVERFLOW = LEAF_NODE_MAX - NODESIZE - KEYLEN_MAX;
 *
 *  - SubDatabase-node must fit into one leaf-page:
 *       SUBDB_NAME_MAX = LEAF_NODE_MAX - node_hdr_len - sizeof(MDBX_db);
 *
 *  - Dupsort values itself are a keys in a dupsort-subdb and couldn't be longer
 *    than the KEYLEN_MAX. But dupsort node must not great than LEAF_NODE_MAX,
 *    since dupsort value couldn't be placed on a large/overflow page:
 *       DUPSORT_DATALEN_MAX = min(KEYLEN_MAX,
 *                                 max(DATALEN_NO_OVERFLOW, sizeof(MDBX_db));
 */

#define PAGEROOM(pagesize) ((pagesize) - PAGEHDRSZ)
#define EVEN_FLOOR(n) ((n) & ~(size_t)1)
#define BRANCH_NODE_MAX(pagesize)                                              \
  (EVEN_FLOOR((PAGEROOM(pagesize) - sizeof(indx_t) - NODESIZE) / (3 - 1) -     \
              sizeof(indx_t)))
#define LEAF_NODE_MAX(pagesize)                                                \
  (EVEN_FLOOR(PAGEROOM(pagesize) / 2) - sizeof(indx_t))
#define MAX_GC1OVPAGE(pagesize) (PAGEROOM(pagesize) / sizeof(pgno_t) - 1)

static __inline size_t keysize_max(size_t pagesize, MDBX_db_flags_t flags) {
  assert(pagesize >= MIN_PAGESIZE && pagesize <= MAX_PAGESIZE &&
         is_powerof2(pagesize));
  STATIC_ASSERT(BRANCH_NODE_MAX(MIN_PAGESIZE) - NODESIZE >= 8);
  if (flags & MDBX_INTEGERKEY)
    return 8 /* sizeof(uint64_t) */;

  const intptr_t max_branch_key = BRANCH_NODE_MAX(pagesize) - NODESIZE;
  STATIC_ASSERT(LEAF_NODE_MAX(MIN_PAGESIZE) - NODESIZE -
                    /* sizeof(uint64) as a key */ 8 >
                sizeof(MDBX_db));
  if (flags &
      (MDBX_DUPSORT | MDBX_DUPFIXED | MDBX_REVERSEDUP | MDBX_INTEGERDUP)) {
    const intptr_t max_dupsort_leaf_key =
        LEAF_NODE_MAX(pagesize) - NODESIZE - sizeof(MDBX_db);
    return (max_branch_key < max_dupsort_leaf_key) ? max_branch_key
                                                   : max_dupsort_leaf_key;
  }
  return max_branch_key;
}

static __inline size_t valsize_max(size_t pagesize, MDBX_db_flags_t flags) {
  assert(pagesize >= MIN_PAGESIZE && pagesize <= MAX_PAGESIZE &&
         is_powerof2(pagesize));

  if (flags & MDBX_INTEGERDUP)
    return 8 /* sizeof(uint64_t) */;

  if (flags & (MDBX_DUPSORT | MDBX_DUPFIXED | MDBX_REVERSEDUP))
    return keysize_max(pagesize, 0);

  const unsigned page_ln2 = log2n_powerof2(pagesize);
  const size_t hard = 0x7FF00000ul;
  const size_t hard_pages = hard >> page_ln2;
  STATIC_ASSERT(MDBX_PGL_LIMIT <= MAX_PAGENO);
  const size_t pages_limit = MDBX_PGL_LIMIT / 4;
  const size_t limit =
      (hard_pages < pages_limit) ? hard : (pages_limit << page_ln2);
  return (limit < MAX_MAPSIZE / 2) ? limit : MAX_MAPSIZE / 2;
}

__cold int mdbx_env_get_maxkeysize(const MDBX_env *env) {
  return mdbx_env_get_maxkeysize_ex(env, MDBX_DUPSORT);
}

__cold int mdbx_env_get_maxkeysize_ex(const MDBX_env *env,
                                      MDBX_db_flags_t flags) {
  if (unlikely(!env || env->me_signature.weak != MDBX_ME_SIGNATURE))
    return -1;

  return (int)mdbx_limits_keysize_max((intptr_t)env->me_psize, flags);
}

size_t mdbx_default_pagesize(void) {
  size_t pagesize = osal_syspagesize();
  ENSURE(nullptr, is_powerof2(pagesize));
  pagesize = (pagesize >= MIN_PAGESIZE) ? pagesize : MIN_PAGESIZE;
  pagesize = (pagesize <= MAX_PAGESIZE) ? pagesize : MAX_PAGESIZE;
  return pagesize;
}

__cold intptr_t mdbx_limits_keysize_max(intptr_t pagesize,
                                        MDBX_db_flags_t flags) {
  if (pagesize < 1)
    pagesize = (intptr_t)mdbx_default_pagesize();
  if (unlikely(pagesize < (intptr_t)MIN_PAGESIZE ||
               pagesize > (intptr_t)MAX_PAGESIZE ||
               !is_powerof2((size_t)pagesize)))
    return -1;

  return keysize_max(pagesize, flags);
}

__cold int mdbx_env_get_maxvalsize_ex(const MDBX_env *env,
                                      MDBX_db_flags_t flags) {
  if (unlikely(!env || env->me_signature.weak != MDBX_ME_SIGNATURE))
    return -1;

  return (int)mdbx_limits_valsize_max((intptr_t)env->me_psize, flags);
}

__cold intptr_t mdbx_limits_valsize_max(intptr_t pagesize,
                                        MDBX_db_flags_t flags) {
  if (pagesize < 1)
    pagesize = (intptr_t)mdbx_default_pagesize();
  if (unlikely(pagesize < (intptr_t)MIN_PAGESIZE ||
               pagesize > (intptr_t)MAX_PAGESIZE ||
               !is_powerof2((size_t)pagesize)))
    return -1;

  return valsize_max(pagesize, flags);
}

__cold intptr_t mdbx_limits_pairsize4page_max(intptr_t pagesize,
                                              MDBX_db_flags_t flags) {
  if (pagesize < 1)
    pagesize = (intptr_t)mdbx_default_pagesize();
  if (unlikely(pagesize < (intptr_t)MIN_PAGESIZE ||
               pagesize > (intptr_t)MAX_PAGESIZE ||
               !is_powerof2((size_t)pagesize)))
    return -1;

  if (flags &
      (MDBX_DUPSORT | MDBX_DUPFIXED | MDBX_INTEGERDUP | MDBX_REVERSEDUP))
    return BRANCH_NODE_MAX(pagesize) - NODESIZE;

  return LEAF_NODE_MAX(pagesize) - NODESIZE;
}

__cold int mdbx_env_get_pairsize4page_max(const MDBX_env *env,
                                          MDBX_db_flags_t flags) {
  if (unlikely(!env || env->me_signature.weak != MDBX_ME_SIGNATURE))
    return -1;

  return (int)mdbx_limits_pairsize4page_max((intptr_t)env->me_psize, flags);
}

__cold intptr_t mdbx_limits_valsize4page_max(intptr_t pagesize,
                                             MDBX_db_flags_t flags) {
  if (pagesize < 1)
    pagesize = (intptr_t)mdbx_default_pagesize();
  if (unlikely(pagesize < (intptr_t)MIN_PAGESIZE ||
               pagesize > (intptr_t)MAX_PAGESIZE ||
               !is_powerof2((size_t)pagesize)))
    return -1;

  if (flags &
      (MDBX_DUPSORT | MDBX_DUPFIXED | MDBX_INTEGERDUP | MDBX_REVERSEDUP))
    return valsize_max(pagesize, flags);

  return PAGEROOM(pagesize);
}

__cold int mdbx_env_get_valsize4page_max(const MDBX_env *env,
                                         MDBX_db_flags_t flags) {
  if (unlikely(!env || env->me_signature.weak != MDBX_ME_SIGNATURE))
    return -1;

  return (int)mdbx_limits_valsize4page_max((intptr_t)env->me_psize, flags);
}

/* Calculate the size of a leaf node.
 *
 * The size depends on the environment's page size; if a data item
 * is too large it will be put onto an large/overflow page and the node
 * size will only include the key and not the data. Sizes are always
 * rounded up to an even number of bytes, to guarantee 2-byte alignment
 * of the MDBX_node headers. */
MDBX_NOTHROW_PURE_FUNCTION static __always_inline size_t
leaf_size(const MDBX_env *env, const MDBX_val *key, const MDBX_val *data) {
  size_t node_bytes = node_size(key, data);
  if (node_bytes > env->me_leaf_nodemax) {
    /* put on large/overflow page */
    node_bytes = node_size_len(key->iov_len, 0) + sizeof(pgno_t);
  }

  return node_bytes + sizeof(indx_t);
}

/* Calculate the size of a branch node.
 *
 * The size should depend on the environment's page size but since
 * we currently don't support spilling large keys onto large/overflow
 * pages, it's simply the size of the MDBX_node header plus the
 * size of the key. Sizes are always rounded up to an even number
 * of bytes, to guarantee 2-byte alignment of the MDBX_node headers.
 *
 * [in] env The environment handle.
 * [in] key The key for the node.
 *
 * Returns The number of bytes needed to store the node. */
MDBX_NOTHROW_PURE_FUNCTION static __always_inline size_t
branch_size(const MDBX_env *env, const MDBX_val *key) {
  /* Size of a node in a branch page with a given key.
   * This is just the node header plus the key, there is no data. */
  size_t node_bytes = node_size(key, nullptr);
  if (unlikely(node_bytes > env->me_branch_nodemax)) {
    /* put on large/overflow page */
    /* not implemented */
    mdbx_panic("node_size(key) %zu > %u branch_nodemax", node_bytes,
               env->me_branch_nodemax);
    node_bytes = node_size(key, nullptr) + sizeof(pgno_t);
  }

  return node_bytes + sizeof(indx_t);
}

MDBX_NOTHROW_CONST_FUNCTION static __always_inline uint16_t
flags_db2sub(uint16_t db_flags) {
  uint16_t sub_flags = db_flags & MDBX_DUPFIXED;

  /* MDBX_INTEGERDUP => MDBX_INTEGERKEY */
#define SHIFT_INTEGERDUP_TO_INTEGERKEY 2
  STATIC_ASSERT((MDBX_INTEGERDUP >> SHIFT_INTEGERDUP_TO_INTEGERKEY) ==
                MDBX_INTEGERKEY);
  sub_flags |= (db_flags & MDBX_INTEGERDUP) >> SHIFT_INTEGERDUP_TO_INTEGERKEY;

  /* MDBX_REVERSEDUP => MDBX_REVERSEKEY */
#define SHIFT_REVERSEDUP_TO_REVERSEKEY 5
  STATIC_ASSERT((MDBX_REVERSEDUP >> SHIFT_REVERSEDUP_TO_REVERSEKEY) ==
                MDBX_REVERSEKEY);
  sub_flags |= (db_flags & MDBX_REVERSEDUP) >> SHIFT_REVERSEDUP_TO_REVERSEKEY;

  return sub_flags;
}

/*----------------------------------------------------------------------------*/

MDBX_NOTHROW_PURE_FUNCTION static __always_inline size_t
pgno2bytes(const MDBX_env *env, size_t pgno) {
  eASSERT(env, (1u << env->me_psize2log) == env->me_psize);
  return ((size_t)pgno) << env->me_psize2log;
}

MDBX_NOTHROW_PURE_FUNCTION static __always_inline MDBX_page *
pgno2page(const MDBX_env *env, size_t pgno) {
  return ptr_disp(env->me_map, pgno2bytes(env, pgno));
}

MDBX_NOTHROW_PURE_FUNCTION static __always_inline pgno_t
bytes2pgno(const MDBX_env *env, size_t bytes) {
  eASSERT(env, (env->me_psize >> env->me_psize2log) == 1);
  return (pgno_t)(bytes >> env->me_psize2log);
}

MDBX_NOTHROW_PURE_FUNCTION static size_t
pgno_align2os_bytes(const MDBX_env *env, size_t pgno) {
  return ceil_powerof2(pgno2bytes(env, pgno), env->me_os_psize);
}

MDBX_NOTHROW_PURE_FUNCTION static pgno_t pgno_align2os_pgno(const MDBX_env *env,
                                                            size_t pgno) {
  return bytes2pgno(env, pgno_align2os_bytes(env, pgno));
}

MDBX_NOTHROW_PURE_FUNCTION static size_t
bytes_align2os_bytes(const MDBX_env *env, size_t bytes) {
  return ceil_powerof2(ceil_powerof2(bytes, env->me_psize), env->me_os_psize);
}

/* Address of first usable data byte in a page, after the header */
MDBX_NOTHROW_PURE_FUNCTION static __always_inline void *
page_data(const MDBX_page *mp) {
  return ptr_disp(mp, PAGEHDRSZ);
}

MDBX_NOTHROW_PURE_FUNCTION static __always_inline const MDBX_page *
data_page(const void *data) {
  return container_of(data, MDBX_page, mp_ptrs);
}

MDBX_NOTHROW_PURE_FUNCTION static __always_inline MDBX_meta *
page_meta(MDBX_page *mp) {
  return (MDBX_meta *)page_data(mp);
}

/* Number of nodes on a page */
MDBX_NOTHROW_PURE_FUNCTION static __always_inline size_t
page_numkeys(const MDBX_page *mp) {
  return mp->mp_lower >> 1;
}

/* The amount of space remaining in the page */
MDBX_NOTHROW_PURE_FUNCTION static __always_inline size_t
page_room(const MDBX_page *mp) {
  return mp->mp_upper - mp->mp_lower;
}

/* Maximum free space in an empty page */
MDBX_NOTHROW_PURE_FUNCTION static __always_inline size_t
page_space(const MDBX_env *env) {
  STATIC_ASSERT(PAGEHDRSZ % 2 == 0);
  return env->me_psize - PAGEHDRSZ;
}

MDBX_NOTHROW_PURE_FUNCTION static __always_inline size_t
page_used(const MDBX_env *env, const MDBX_page *mp) {
  return page_space(env) - page_room(mp);
}

/* The percentage of space used in the page, in a percents. */
MDBX_MAYBE_UNUSED MDBX_NOTHROW_PURE_FUNCTION static __inline double
page_fill(const MDBX_env *env, const MDBX_page *mp) {
  return page_used(env, mp) * 100.0 / page_space(env);
}

/* The number of large/overflow pages needed to store the given size. */
MDBX_NOTHROW_PURE_FUNCTION static __always_inline pgno_t
number_of_ovpages(const MDBX_env *env, size_t bytes) {
  return bytes2pgno(env, PAGEHDRSZ - 1 + bytes) + 1;
}

__cold static const char *pagetype_caption(const uint8_t type,
                                           char buf4unknown[16]) {
  switch (type) {
  case P_BRANCH:
    return "branch";
  case P_LEAF:
    return "leaf";
  case P_LEAF | P_SUBP:
    return "subleaf";
  case P_LEAF | P_LEAF2:
    return "dupfixed-leaf";
  case P_LEAF | P_LEAF2 | P_SUBP:
    return "dupfixed-subleaf";
  case P_LEAF | P_LEAF2 | P_SUBP | P_LEGACY_DIRTY:
    return "dupfixed-subleaf.legacy-dirty";
  case P_OVERFLOW:
    return "large";
  default:
    snprintf(buf4unknown, 16, "unknown_0x%x", type);
    return buf4unknown;
  }
}

__cold static int MDBX_PRINTF_ARGS(2, 3)
    bad_page(const MDBX_page *mp, const char *fmt, ...) {
  if (LOG_ENABLED(MDBX_LOG_ERROR)) {
    static const MDBX_page *prev;
    if (prev != mp) {
      char buf4unknown[16];
      prev = mp;
      debug_log(MDBX_LOG_ERROR, "badpage", 0,
                "corrupted %s-page #%u, mod-txnid %" PRIaTXN "\n",
                pagetype_caption(PAGETYPE_WHOLE(mp), buf4unknown), mp->mp_pgno,
                mp->mp_txnid);
    }

    va_list args;
    va_start(args, fmt);
    debug_log_va(MDBX_LOG_ERROR, "badpage", 0, fmt, args);
    va_end(args);
  }
  return MDBX_CORRUPTED;
}

__cold static void MDBX_PRINTF_ARGS(2, 3)
    poor_page(const MDBX_page *mp, const char *fmt, ...) {
  if (LOG_ENABLED(MDBX_LOG_NOTICE)) {
    static const MDBX_page *prev;
    if (prev != mp) {
      char buf4unknown[16];
      prev = mp;
      debug_log(MDBX_LOG_NOTICE, "poorpage", 0,
                "suboptimal %s-page #%u, mod-txnid %" PRIaTXN "\n",
                pagetype_caption(PAGETYPE_WHOLE(mp), buf4unknown), mp->mp_pgno,
                mp->mp_txnid);
    }

    va_list args;
    va_start(args, fmt);
    debug_log_va(MDBX_LOG_NOTICE, "poorpage", 0, fmt, args);
    va_end(args);
  }
}

/* Address of node i in page p */
MDBX_NOTHROW_PURE_FUNCTION static __always_inline MDBX_node *
page_node(const MDBX_page *mp, size_t i) {
  assert(PAGETYPE_COMPAT(mp) == P_LEAF || PAGETYPE_WHOLE(mp) == P_BRANCH);
  assert(page_numkeys(mp) > i);
  assert(mp->mp_ptrs[i] % 2 == 0);
  return ptr_disp(mp, mp->mp_ptrs[i] + PAGEHDRSZ);
}

/* The address of a key in a LEAF2 page.
 * LEAF2 pages are used for MDBX_DUPFIXED sorted-duplicate sub-DBs.
 * There are no node headers, keys are stored contiguously. */
MDBX_NOTHROW_PURE_FUNCTION static __always_inline void *
page_leaf2key(const MDBX_page *mp, size_t i, size_t keysize) {
  assert(PAGETYPE_COMPAT(mp) == (P_LEAF | P_LEAF2));
  assert(mp->mp_leaf2_ksize == keysize);
  (void)keysize;
  return ptr_disp(mp, PAGEHDRSZ + i * mp->mp_leaf2_ksize);
}

/* Set the node's key into keyptr. */
static __always_inline void get_key(const MDBX_node *node, MDBX_val *keyptr) {
  keyptr->iov_len = node_ks(node);
  keyptr->iov_base = node_key(node);
}

/* Set the node's key into keyptr, if requested. */
static __always_inline void
get_key_optional(const MDBX_node *node, MDBX_val *keyptr /* __may_null */) {
  if (keyptr)
    get_key(node, keyptr);
}

/*------------------------------------------------------------------------------
 * safe read/write volatile 64-bit fields on 32-bit architectures. */

#ifndef atomic_store64
MDBX_MAYBE_UNUSED static __always_inline uint64_t
atomic_store64(MDBX_atomic_uint64_t *p, const uint64_t value,
               enum MDBX_memory_order order) {
  STATIC_ASSERT(sizeof(MDBX_atomic_uint64_t) == 8);
#if MDBX_64BIT_ATOMIC
#if __GNUC_PREREQ(11, 0)
  STATIC_ASSERT(__alignof__(MDBX_atomic_uint64_t) >= sizeof(uint64_t));
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
        atomic_load64(const volatile MDBX_atomic_uint64_t *p,
                      enum MDBX_memory_order order) {
  STATIC_ASSERT(sizeof(MDBX_atomic_uint64_t) == 8);
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
  value |= atomic_load32(&p->low, (order == mo_Relaxed) ? mo_Relaxed
                                                        : mo_AcquireRelease);
  jitter4testing(true);
  for (;;) {
    osal_compiler_barrier();
    uint64_t again = (uint64_t)atomic_load32(&p->high, order) << 32;
    jitter4testing(true);
    again |= atomic_load32(&p->low, (order == mo_Relaxed) ? mo_Relaxed
                                                          : mo_AcquireRelease);
    jitter4testing(true);
    if (likely(value == again))
      return value;
    value = again;
  }
#endif /* !MDBX_64BIT_ATOMIC */
}
#endif /* atomic_load64 */

static __always_inline void atomic_yield(void) {
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
#elif defined(__aarch64__) || (defined(__ARM_ARCH) && __ARM_ARCH > 6) ||       \
    defined(__ARM_ARCH_6K__)
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
#elif defined(__linux__) || defined(__gnu_linux__) || defined(_UNIX03_SOURCE)
  sched_yield();
#elif (defined(_GNU_SOURCE) && __GLIBC_PREREQ(2, 1)) || defined(_OPEN_THREADS)
  pthread_yield();
#endif
}

#if MDBX_64BIT_CAS
static __always_inline bool atomic_cas64(MDBX_atomic_uint64_t *p, uint64_t c,
                                         uint64_t v) {
#ifdef MDBX_HAVE_C11ATOMICS
  STATIC_ASSERT(sizeof(long long) >= sizeof(uint64_t));
  assert(atomic_is_lock_free(MDBX_c11a_rw(uint64_t, p)));
  return atomic_compare_exchange_strong(MDBX_c11a_rw(uint64_t, p), &c, v);
#elif defined(__GNUC__) || defined(__clang__)
  return __sync_bool_compare_and_swap(&p->weak, c, v);
#elif defined(_MSC_VER)
  return c == (uint64_t)_InterlockedCompareExchange64(
                  (volatile __int64 *)&p->weak, v, c);
#elif defined(__APPLE__)
  return OSAtomicCompareAndSwap64Barrier(c, v, &p->weak);
#else
#error FIXME: Unsupported compiler
#endif
}
#endif /* MDBX_64BIT_CAS */

static __always_inline bool atomic_cas32(MDBX_atomic_uint32_t *p, uint32_t c,
                                         uint32_t v) {
#ifdef MDBX_HAVE_C11ATOMICS
  STATIC_ASSERT(sizeof(int) >= sizeof(uint32_t));
  assert(atomic_is_lock_free(MDBX_c11a_rw(uint32_t, p)));
  return atomic_compare_exchange_strong(MDBX_c11a_rw(uint32_t, p), &c, v);
#elif defined(__GNUC__) || defined(__clang__)
  return __sync_bool_compare_and_swap(&p->weak, c, v);
#elif defined(_MSC_VER)
  STATIC_ASSERT(sizeof(volatile long) == sizeof(volatile uint32_t));
  return c ==
         (uint32_t)_InterlockedCompareExchange((volatile long *)&p->weak, v, c);
#elif defined(__APPLE__)
  return OSAtomicCompareAndSwap32Barrier(c, v, &p->weak);
#else
#error FIXME: Unsupported compiler
#endif
}

static __always_inline uint32_t atomic_add32(MDBX_atomic_uint32_t *p,
                                             uint32_t v) {
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

static __always_inline uint64_t safe64_txnid_next(uint64_t txnid) {
  txnid += xMDBX_TXNID_STEP;
#if !MDBX_64BIT_CAS
  /* avoid overflow of low-part in safe64_reset() */
  txnid += (UINT32_MAX == (uint32_t)txnid);
#endif
  return txnid;
}

/* Atomically make target value >= SAFE64_INVALID_THRESHOLD */
static __always_inline void safe64_reset(MDBX_atomic_uint64_t *p,
                                         bool single_writer) {
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

static __always_inline bool safe64_reset_compare(MDBX_atomic_uint64_t *p,
                                                 txnid_t compare) {
  /* LY: This function is used to reset `mr_txnid` from hsr-handler in case
   *     the asynchronously cancellation of read transaction. Therefore,
   *     there may be a collision between the cleanup performed here and
   *     asynchronous termination and restarting of the read transaction
   *     in another process/thread. In general we MUST NOT reset the `mr_txnid`
   *     if a new transaction was started (i.e. if `mr_txnid` was changed). */
#if MDBX_64BIT_CAS
  bool rc = atomic_cas64(p, compare, UINT64_MAX);
#else
  /* LY: There is no gold ratio here since shared mutex is too costly,
   *     in such way we must acquire/release it for every update of mr_txnid,
   *     i.e. twice for each read transaction). */
  bool rc = false;
  if (likely(atomic_load32(&p->low, mo_AcquireRelease) == (uint32_t)compare &&
             atomic_cas32(&p->high, (uint32_t)(compare >> 32), UINT32_MAX))) {
    if (unlikely(atomic_load32(&p->low, mo_AcquireRelease) !=
                 (uint32_t)compare))
      atomic_cas32(&p->high, UINT32_MAX, (uint32_t)(compare >> 32));
    else
      rc = true;
  }
#endif /* MDBX_64BIT_CAS */
  jitter4testing(true);
  return rc;
}

static __always_inline void safe64_write(MDBX_atomic_uint64_t *p,
                                         const uint64_t v) {
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

static __always_inline uint64_t safe64_read(const MDBX_atomic_uint64_t *p) {
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
 safe64_is_valid_ptr(const MDBX_atomic_uint64_t *p) {
#if MDBX_64BIT_ATOMIC
  return atomic_load64(p, mo_AcquireRelease) < SAFE64_INVALID_THRESHOLD;
#else
  return atomic_load32(&p->high, mo_AcquireRelease) != UINT32_MAX;
#endif /* MDBX_64BIT_ATOMIC */
}
#endif /* unused for now */

/* non-atomic write with safety for reading a half-updated value */
static __always_inline void safe64_update(MDBX_atomic_uint64_t *p,
                                          const uint64_t v) {
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
    safe64_inc(MDBX_atomic_uint64_t *p, const uint64_t v) {
  assert(v > 0);
  safe64_update(p, safe64_read(p) + v);
}

/*----------------------------------------------------------------------------*/
/* rthc (tls keys and destructors) */

typedef struct rthc_entry_t {
  MDBX_reader *begin;
  MDBX_reader *end;
  osal_thread_key_t thr_tls_key;
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

static pthread_mutex_t lcklist_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t rthc_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t rthc_cond = PTHREAD_COND_INITIALIZER;
static osal_thread_key_t rthc_key;
static MDBX_atomic_uint32_t rthc_pending;

static __inline uint64_t rthc_signature(const void *addr, uint8_t kind) {
  uint64_t salt = osal_thread_self() * UINT64_C(0xA2F0EEC059629A17) ^
                  UINT64_C(0x01E07C6FDB596497) * (uintptr_t)(addr);
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
  return salt << 8 | kind;
#elif __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
  return (uint64_t)kind << 56 | salt >> 8;
#else
#error "FIXME: Unsupported byte order"
#endif /* __BYTE_ORDER__ */
}

#define MDBX_THREAD_RTHC_REGISTERED(addr) rthc_signature(addr, 0x0D)
#define MDBX_THREAD_RTHC_COUNTED(addr) rthc_signature(addr, 0xC0)
static __thread uint64_t rthc_thread_state
#if __has_attribute(tls_model) &&                                              \
    (defined(__PIC__) || defined(__pic__) || MDBX_BUILD_SHARED_LIBRARY)
    __attribute__((tls_model("local-dynamic")))
#endif
    ;

#if defined(__APPLE__) && defined(__SANITIZE_ADDRESS__) &&                     \
    !defined(MDBX_ATTRIBUTE_NO_SANITIZE_ADDRESS)
/* Avoid ASAN-trap due the target TLS-variable feed by Darwin's tlv_free() */
#define MDBX_ATTRIBUTE_NO_SANITIZE_ADDRESS                                     \
  __attribute__((__no_sanitize_address__, __noinline__))
#else
#define MDBX_ATTRIBUTE_NO_SANITIZE_ADDRESS __inline
#endif

MDBX_ATTRIBUTE_NO_SANITIZE_ADDRESS static uint64_t rthc_read(const void *rthc) {
  return *(volatile uint64_t *)rthc;
}

MDBX_ATTRIBUTE_NO_SANITIZE_ADDRESS static uint64_t
rthc_compare_and_clean(const void *rthc, const uint64_t signature) {
#if MDBX_64BIT_CAS
  return atomic_cas64((MDBX_atomic_uint64_t *)rthc, signature, 0);
#elif __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
  return atomic_cas32((MDBX_atomic_uint32_t *)rthc, (uint32_t)signature, 0);
#elif __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
  return atomic_cas32((MDBX_atomic_uint32_t *)rthc, (uint32_t)(signature >> 32),
                      0);
#else
#error "FIXME: Unsupported byte order"
#endif
}

static __inline int rthc_atexit(void (*dtor)(void *), void *obj,
                                void *dso_symbol) {
#ifndef MDBX_HAVE_CXA_THREAD_ATEXIT_IMPL
#if defined(LIBCXXABI_HAS_CXA_THREAD_ATEXIT_IMPL) ||                           \
    defined(HAVE___CXA_THREAD_ATEXIT_IMPL) || __GLIBC_PREREQ(2, 18) ||         \
    defined(BIONIC)
#define MDBX_HAVE_CXA_THREAD_ATEXIT_IMPL 1
#else
#define MDBX_HAVE_CXA_THREAD_ATEXIT_IMPL 0
#endif
#endif /* MDBX_HAVE_CXA_THREAD_ATEXIT_IMPL */

#ifndef MDBX_HAVE_CXA_THREAD_ATEXIT
#if defined(LIBCXXABI_HAS_CXA_THREAD_ATEXIT) ||                                \
    defined(HAVE___CXA_THREAD_ATEXIT)
#define MDBX_HAVE_CXA_THREAD_ATEXIT 1
#elif !MDBX_HAVE_CXA_THREAD_ATEXIT_IMPL &&                                     \
    (defined(__linux__) || defined(__gnu_linux__))
#define MDBX_HAVE_CXA_THREAD_ATEXIT 1
#else
#define MDBX_HAVE_CXA_THREAD_ATEXIT 0
#endif
#endif /* MDBX_HAVE_CXA_THREAD_ATEXIT */

  int rc = MDBX_ENOSYS;
#if MDBX_HAVE_CXA_THREAD_ATEXIT_IMPL && !MDBX_HAVE_CXA_THREAD_ATEXIT
#define __cxa_thread_atexit __cxa_thread_atexit_impl
#endif
#if MDBX_HAVE_CXA_THREAD_ATEXIT || defined(__cxa_thread_atexit)
  extern int __cxa_thread_atexit(void (*dtor)(void *), void *obj,
                                 void *dso_symbol) MDBX_WEAK_IMPORT_ATTRIBUTE;
  if (&__cxa_thread_atexit)
    rc = __cxa_thread_atexit(dtor, obj, dso_symbol);
#elif defined(__APPLE__) || defined(_DARWIN_C_SOURCE)
  extern void _tlv_atexit(void (*termfunc)(void *objAddr), void *objAddr)
      MDBX_WEAK_IMPORT_ATTRIBUTE;
  if (&_tlv_atexit) {
    (void)dso_symbol;
    _tlv_atexit(dtor, obj);
    rc = 0;
  }
#else
  (void)dtor;
  (void)obj;
  (void)dso_symbol;
#endif
  return rc;
}

__cold static void workaround_glibc_bug21031(void) {
  /* Workaround for https://sourceware.org/bugzilla/show_bug.cgi?id=21031
   *
   * Due race between pthread_key_delete() and __nptl_deallocate_tsd()
   * The destructor(s) of thread-local-storage object(s) may be running
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
  ENSURE(nullptr, osal_pthread_mutex_lock(&rthc_mutex) == 0);
#endif
}

static __inline void rthc_unlock(void) {
#if defined(_WIN32) || defined(_WIN64)
  LeaveCriticalSection(&rthc_critical_section);
#else
  ENSURE(nullptr, pthread_mutex_unlock(&rthc_mutex) == 0);
#endif
}

static __inline int thread_key_create(osal_thread_key_t *key) {
  int rc;
#if defined(_WIN32) || defined(_WIN64)
  *key = TlsAlloc();
  rc = (*key != TLS_OUT_OF_INDEXES) ? MDBX_SUCCESS : GetLastError();
#else
  rc = pthread_key_create(key, nullptr);
#endif
  TRACE("&key = %p, value %" PRIuPTR ", rc %d", __Wpedantic_format_voidptr(key),
        (uintptr_t)*key, rc);
  return rc;
}

static __inline void thread_key_delete(osal_thread_key_t key) {
  TRACE("key = %" PRIuPTR, (uintptr_t)key);
#if defined(_WIN32) || defined(_WIN64)
  ENSURE(nullptr, TlsFree(key));
#else
  ENSURE(nullptr, pthread_key_delete(key) == 0);
  workaround_glibc_bug21031();
#endif
}

static __inline void *thread_rthc_get(osal_thread_key_t key) {
#if defined(_WIN32) || defined(_WIN64)
  return TlsGetValue(key);
#else
  return pthread_getspecific(key);
#endif
}

static void thread_rthc_set(osal_thread_key_t key, const void *value) {
#if defined(_WIN32) || defined(_WIN64)
  ENSURE(nullptr, TlsSetValue(key, (void *)value));
#else
  const uint64_t sign_registered =
      MDBX_THREAD_RTHC_REGISTERED(&rthc_thread_state);
  const uint64_t sign_counted = MDBX_THREAD_RTHC_COUNTED(&rthc_thread_state);
  if (value && unlikely(rthc_thread_state != sign_registered &&
                        rthc_thread_state != sign_counted)) {
    rthc_thread_state = sign_registered;
    TRACE("thread registered 0x%" PRIxPTR, osal_thread_self());
    if (rthc_atexit(thread_dtor, &rthc_thread_state,
                    (void *)&mdbx_version /* dso_anchor */)) {
      ENSURE(nullptr, pthread_setspecific(rthc_key, &rthc_thread_state) == 0);
      rthc_thread_state = sign_counted;
      const unsigned count_before = atomic_add32(&rthc_pending, 1);
      ENSURE(nullptr, count_before < INT_MAX);
      NOTICE("fallback to pthreads' tsd, key %" PRIuPTR ", count %u",
             (uintptr_t)rthc_key, count_before);
      (void)count_before;
    }
  }
  ENSURE(nullptr, pthread_setspecific(key, value) == 0);
#endif
}

/* dtor called for thread, i.e. for all mdbx's environment objects */
__cold void thread_dtor(void *rthc) {
  rthc_lock();
  TRACE(">> pid %d, thread 0x%" PRIxPTR ", rthc %p", osal_getpid(),
        osal_thread_self(), rthc);

  const uint32_t self_pid = osal_getpid();
  for (size_t i = 0; i < rthc_count; ++i) {
    const osal_thread_key_t key = rthc_table[i].thr_tls_key;
    MDBX_reader *const reader = thread_rthc_get(key);
    if (reader < rthc_table[i].begin || reader >= rthc_table[i].end)
      continue;
#if !defined(_WIN32) && !defined(_WIN64)
    if (pthread_setspecific(key, nullptr) != 0) {
      TRACE("== thread 0x%" PRIxPTR
            ", rthc %p: ignore race with tsd-key deletion",
            osal_thread_self(), __Wpedantic_format_voidptr(reader));
      continue /* ignore race with tsd-key deletion by mdbx_env_close() */;
    }
#endif

    TRACE("== thread 0x%" PRIxPTR
          ", rthc %p, [%zi], %p ... %p (%+i), rtch-pid %i, "
          "current-pid %i",
          osal_thread_self(), __Wpedantic_format_voidptr(reader), i,
          __Wpedantic_format_voidptr(rthc_table[i].begin),
          __Wpedantic_format_voidptr(rthc_table[i].end),
          (int)(reader - rthc_table[i].begin), reader->mr_pid.weak, self_pid);
    if (atomic_load32(&reader->mr_pid, mo_Relaxed) == self_pid) {
      TRACE("==== thread 0x%" PRIxPTR ", rthc %p, cleanup", osal_thread_self(),
            __Wpedantic_format_voidptr(reader));
      (void)atomic_cas32(&reader->mr_pid, self_pid, 0);
    }
  }

#if defined(_WIN32) || defined(_WIN64)
  TRACE("<< thread 0x%" PRIxPTR ", rthc %p", osal_thread_self(), rthc);
  rthc_unlock();
#else
  const uint64_t sign_registered = MDBX_THREAD_RTHC_REGISTERED(rthc);
  const uint64_t sign_counted = MDBX_THREAD_RTHC_COUNTED(rthc);
  const uint64_t state = rthc_read(rthc);
  if (state == sign_registered &&
      rthc_compare_and_clean(rthc, sign_registered)) {
    TRACE("== thread 0x%" PRIxPTR
          ", rthc %p, pid %d, self-status %s (0x%08" PRIx64 ")",
          osal_thread_self(), rthc, osal_getpid(), "registered", state);
  } else if (state == sign_counted &&
             rthc_compare_and_clean(rthc, sign_counted)) {
    TRACE("== thread 0x%" PRIxPTR
          ", rthc %p, pid %d, self-status %s (0x%08" PRIx64 ")",
          osal_thread_self(), rthc, osal_getpid(), "counted", state);
    ENSURE(nullptr, atomic_sub32(&rthc_pending, 1) > 0);
  } else {
    WARNING("thread 0x%" PRIxPTR
            ", rthc %p, pid %d, self-status %s (0x%08" PRIx64 ")",
            osal_thread_self(), rthc, osal_getpid(), "wrong", state);
  }

  if (atomic_load32(&rthc_pending, mo_AcquireRelease) == 0) {
    TRACE("== thread 0x%" PRIxPTR ", rthc %p, pid %d, wake", osal_thread_self(),
          rthc, osal_getpid());
    ENSURE(nullptr, pthread_cond_broadcast(&rthc_cond) == 0);
  }

  TRACE("<< thread 0x%" PRIxPTR ", rthc %p", osal_thread_self(), rthc);
  /* Allow tail call optimization, i.e. gcc should generate the jmp instruction
   * instead of a call for pthread_mutex_unlock() and therefore CPU could not
   * return to current DSO's code section, which may be unloaded immediately
   * after the mutex got released. */
  pthread_mutex_unlock(&rthc_mutex);
#endif
}

MDBX_EXCLUDE_FOR_GPROF
__cold void global_dtor(void) {
  TRACE(">> pid %d", osal_getpid());

  rthc_lock();
#if !defined(_WIN32) && !defined(_WIN64)
  uint64_t *rthc = pthread_getspecific(rthc_key);
  TRACE("== thread 0x%" PRIxPTR ", rthc %p, pid %d, self-status 0x%08" PRIx64
        ", left %d",
        osal_thread_self(), __Wpedantic_format_voidptr(rthc), osal_getpid(),
        rthc ? rthc_read(rthc) : ~UINT64_C(0),
        atomic_load32(&rthc_pending, mo_Relaxed));
  if (rthc) {
    const uint64_t sign_registered = MDBX_THREAD_RTHC_REGISTERED(rthc);
    const uint64_t sign_counted = MDBX_THREAD_RTHC_COUNTED(rthc);
    const uint64_t state = rthc_read(rthc);
    if (state == sign_registered &&
        rthc_compare_and_clean(rthc, sign_registered)) {
      TRACE("== thread 0x%" PRIxPTR
            ", rthc %p, pid %d, self-status %s (0x%08" PRIx64 ")",
            osal_thread_self(), __Wpedantic_format_voidptr(rthc), osal_getpid(),
            "registered", state);
    } else if (state == sign_counted &&
               rthc_compare_and_clean(rthc, sign_counted)) {
      TRACE("== thread 0x%" PRIxPTR
            ", rthc %p, pid %d, self-status %s (0x%08" PRIx64 ")",
            osal_thread_self(), __Wpedantic_format_voidptr(rthc), osal_getpid(),
            "counted", state);
      ENSURE(nullptr, atomic_sub32(&rthc_pending, 1) > 0);
    } else {
      WARNING("thread 0x%" PRIxPTR
              ", rthc %p, pid %d, self-status %s (0x%08" PRIx64 ")",
              osal_thread_self(), __Wpedantic_format_voidptr(rthc),
              osal_getpid(), "wrong", state);
    }
  }

  struct timespec abstime;
  ENSURE(nullptr, clock_gettime(CLOCK_REALTIME, &abstime) == 0);
  abstime.tv_nsec += 1000000000l / 10;
  if (abstime.tv_nsec >= 1000000000l) {
    abstime.tv_nsec -= 1000000000l;
    abstime.tv_sec += 1;
  }
#if MDBX_DEBUG > 0
  abstime.tv_sec += 600;
#endif

  for (unsigned left;
       (left = atomic_load32(&rthc_pending, mo_AcquireRelease)) > 0;) {
    NOTICE("tls-cleanup: pid %d, pending %u, wait for...", osal_getpid(), left);
    const int rc = pthread_cond_timedwait(&rthc_cond, &rthc_mutex, &abstime);
    if (rc && rc != EINTR)
      break;
  }
  thread_key_delete(rthc_key);
#endif

  const uint32_t self_pid = osal_getpid();
  for (size_t i = 0; i < rthc_count; ++i) {
    const osal_thread_key_t key = rthc_table[i].thr_tls_key;
    thread_key_delete(key);
    for (MDBX_reader *rthc = rthc_table[i].begin; rthc < rthc_table[i].end;
         ++rthc) {
      TRACE("== [%zi] = key %" PRIuPTR ", %p ... %p, rthc %p (%+i), "
            "rthc-pid %i, current-pid %i",
            i, (uintptr_t)key, __Wpedantic_format_voidptr(rthc_table[i].begin),
            __Wpedantic_format_voidptr(rthc_table[i].end),
            __Wpedantic_format_voidptr(rthc), (int)(rthc - rthc_table[i].begin),
            rthc->mr_pid.weak, self_pid);
      if (atomic_load32(&rthc->mr_pid, mo_Relaxed) == self_pid) {
        atomic_store32(&rthc->mr_pid, 0, mo_AcquireRelease);
        TRACE("== cleanup %p", __Wpedantic_format_voidptr(rthc));
      }
    }
  }

  rthc_limit = rthc_count = 0;
  if (rthc_table != rthc_table_static)
    osal_free(rthc_table);
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

  osal_dtor();
  TRACE("<< pid %d\n", osal_getpid());
}

__cold int rthc_alloc(osal_thread_key_t *pkey, MDBX_reader *begin,
                      MDBX_reader *end) {
  assert(pkey != NULL);
#ifndef NDEBUG
  *pkey = (osal_thread_key_t)0xBADBADBAD;
#endif /* NDEBUG */

  rthc_lock();
  TRACE(">> rthc_count %u, rthc_limit %u", rthc_count, rthc_limit);
  int rc;
  if (rthc_count == rthc_limit) {
    rthc_entry_t *new_table =
        osal_realloc((rthc_table == rthc_table_static) ? nullptr : rthc_table,
                     sizeof(rthc_entry_t) * rthc_limit * 2);
    if (new_table == nullptr) {
      rc = MDBX_ENOMEM;
      goto bailout;
    }
    if (rthc_table == rthc_table_static)
      memcpy(new_table, rthc_table, sizeof(rthc_entry_t) * rthc_limit);
    rthc_table = new_table;
    rthc_limit *= 2;
  }

  rc = thread_key_create(&rthc_table[rthc_count].thr_tls_key);
  if (rc != MDBX_SUCCESS)
    goto bailout;

  *pkey = rthc_table[rthc_count].thr_tls_key;
  TRACE("== [%i] = key %" PRIuPTR ", %p ... %p", rthc_count, (uintptr_t)*pkey,
        __Wpedantic_format_voidptr(begin), __Wpedantic_format_voidptr(end));

  rthc_table[rthc_count].begin = begin;
  rthc_table[rthc_count].end = end;
  ++rthc_count;
  TRACE("<< key %" PRIuPTR ", rthc_count %u, rthc_limit %u", (uintptr_t)*pkey,
        rthc_count, rthc_limit);
  rthc_unlock();
  return MDBX_SUCCESS;

bailout:
  rthc_unlock();
  return rc;
}

__cold void rthc_remove(const osal_thread_key_t key) {
  thread_key_delete(key);
  rthc_lock();
  TRACE(">> key %zu, rthc_count %u, rthc_limit %u", (uintptr_t)key, rthc_count,
        rthc_limit);

  for (size_t i = 0; i < rthc_count; ++i) {
    if (key == rthc_table[i].thr_tls_key) {
      const uint32_t self_pid = osal_getpid();
      TRACE("== [%zi], %p ...%p, current-pid %d", i,
            __Wpedantic_format_voidptr(rthc_table[i].begin),
            __Wpedantic_format_voidptr(rthc_table[i].end), self_pid);

      for (MDBX_reader *rthc = rthc_table[i].begin; rthc < rthc_table[i].end;
           ++rthc) {
        if (atomic_load32(&rthc->mr_pid, mo_Relaxed) == self_pid) {
          atomic_store32(&rthc->mr_pid, 0, mo_AcquireRelease);
          TRACE("== cleanup %p", __Wpedantic_format_voidptr(rthc));
        }
      }
      if (--rthc_count > 0)
        rthc_table[i] = rthc_table[rthc_count];
      else if (rthc_table != rthc_table_static) {
        osal_free(rthc_table);
        rthc_table = rthc_table_static;
        rthc_limit = RTHC_INITIAL_LIMIT;
      }
      break;
    }
  }

  TRACE("<< key %zu, rthc_count %u, rthc_limit %u", (size_t)key, rthc_count,
        rthc_limit);
  rthc_unlock();
}

//------------------------------------------------------------------------------

#define RTHC_ENVLIST_END ((MDBX_env *)((uintptr_t)50459))
static MDBX_env *inprocess_lcklist_head = RTHC_ENVLIST_END;

static __inline void lcklist_lock(void) {
#if defined(_WIN32) || defined(_WIN64)
  EnterCriticalSection(&lcklist_critical_section);
#else
  ENSURE(nullptr, osal_pthread_mutex_lock(&lcklist_mutex) == 0);
#endif
}

static __inline void lcklist_unlock(void) {
#if defined(_WIN32) || defined(_WIN64)
  LeaveCriticalSection(&lcklist_critical_section);
#else
  ENSURE(nullptr, pthread_mutex_unlock(&lcklist_mutex) == 0);
#endif
}

MDBX_NOTHROW_CONST_FUNCTION static uint64_t rrxmrrxmsx_0(uint64_t v) {
  /* Pelle Evensen's mixer, https://bit.ly/2HOfynt */
  v ^= (v << 39 | v >> 25) ^ (v << 14 | v >> 50);
  v *= UINT64_C(0xA24BAED4963EE407);
  v ^= (v << 40 | v >> 24) ^ (v << 15 | v >> 49);
  v *= UINT64_C(0x9FB21C651E98DF25);
  return v ^ v >> 28;
}

static int uniq_peek(const osal_mmap_t *pending, osal_mmap_t *scan) {
  int rc;
  uint64_t bait;
  MDBX_lockinfo *const pending_lck = pending->lck;
  MDBX_lockinfo *const scan_lck = scan->lck;
  if (pending_lck) {
    bait = atomic_load64(&pending_lck->mti_bait_uniqueness, mo_AcquireRelease);
    rc = MDBX_SUCCESS;
  } else {
    bait = 0 /* hush MSVC warning */;
    rc = osal_msync(scan, 0, sizeof(MDBX_lockinfo), MDBX_SYNC_DATA);
    if (rc == MDBX_SUCCESS)
      rc = osal_pread(pending->fd, &bait, sizeof(scan_lck->mti_bait_uniqueness),
                      offsetof(MDBX_lockinfo, mti_bait_uniqueness));
  }
  if (likely(rc == MDBX_SUCCESS) &&
      bait == atomic_load64(&scan_lck->mti_bait_uniqueness, mo_AcquireRelease))
    rc = MDBX_RESULT_TRUE;

  TRACE("uniq-peek: %s, bait 0x%016" PRIx64 ",%s rc %d",
        pending_lck ? "mem" : "file", bait,
        (rc == MDBX_RESULT_TRUE) ? " found," : (rc ? " FAILED," : ""), rc);
  return rc;
}

static int uniq_poke(const osal_mmap_t *pending, osal_mmap_t *scan,
                     uint64_t *abra) {
  if (*abra == 0) {
    const uintptr_t tid = osal_thread_self();
    uintptr_t uit = 0;
    memcpy(&uit, &tid, (sizeof(tid) < sizeof(uit)) ? sizeof(tid) : sizeof(uit));
    *abra = rrxmrrxmsx_0(osal_monotime() + UINT64_C(5873865991930747) * uit);
  }
  const uint64_t cadabra =
      rrxmrrxmsx_0(*abra + UINT64_C(7680760450171793) * (unsigned)osal_getpid())
          << 24 |
      *abra >> 40;
  MDBX_lockinfo *const scan_lck = scan->lck;
  atomic_store64(&scan_lck->mti_bait_uniqueness, cadabra, mo_AcquireRelease);
  *abra = *abra * UINT64_C(6364136223846793005) + 1;
  return uniq_peek(pending, scan);
}

__cold static int uniq_check(const osal_mmap_t *pending, MDBX_env **found) {
  *found = nullptr;
  uint64_t salt = 0;
  for (MDBX_env *scan = inprocess_lcklist_head; scan != RTHC_ENVLIST_END;
       scan = scan->me_lcklist_next) {
    MDBX_lockinfo *const scan_lck = scan->me_lck_mmap.lck;
    int err = atomic_load64(&scan_lck->mti_bait_uniqueness, mo_AcquireRelease)
                  ? uniq_peek(pending, &scan->me_lck_mmap)
                  : uniq_poke(pending, &scan->me_lck_mmap, &salt);
    if (err == MDBX_ENODATA) {
      uint64_t length = 0;
      if (likely(osal_filesize(pending->fd, &length) == MDBX_SUCCESS &&
                 length == 0)) {
        /* LY: skip checking since LCK-file is empty, i.e. just created. */
        DEBUG("uniq-probe: %s", "unique (new/empty lck)");
        return MDBX_RESULT_TRUE;
      }
    }
    if (err == MDBX_RESULT_TRUE)
      err = uniq_poke(pending, &scan->me_lck_mmap, &salt);
    if (err == MDBX_RESULT_TRUE) {
      (void)osal_msync(&scan->me_lck_mmap, 0, sizeof(MDBX_lockinfo),
                       MDBX_SYNC_KICK);
      err = uniq_poke(pending, &scan->me_lck_mmap, &salt);
    }
    if (err == MDBX_RESULT_TRUE) {
      err = uniq_poke(pending, &scan->me_lck_mmap, &salt);
      *found = scan;
      DEBUG("uniq-probe: found %p", __Wpedantic_format_voidptr(*found));
      return MDBX_RESULT_FALSE;
    }
    if (unlikely(err != MDBX_SUCCESS)) {
      DEBUG("uniq-probe: failed rc %d", err);
      return err;
    }
  }

  DEBUG("uniq-probe: %s", "unique");
  return MDBX_RESULT_TRUE;
}

static int lcklist_detach_locked(MDBX_env *env) {
  MDBX_env *inprocess_neighbor = nullptr;
  int rc = MDBX_SUCCESS;
  if (env->me_lcklist_next != nullptr) {
    ENSURE(env, env->me_lcklist_next != nullptr);
    ENSURE(env, inprocess_lcklist_head != RTHC_ENVLIST_END);
    for (MDBX_env **ptr = &inprocess_lcklist_head; *ptr != RTHC_ENVLIST_END;
         ptr = &(*ptr)->me_lcklist_next) {
      if (*ptr == env) {
        *ptr = env->me_lcklist_next;
        env->me_lcklist_next = nullptr;
        break;
      }
    }
    ENSURE(env, env->me_lcklist_next == nullptr);
  }

  rc = likely(osal_getpid() == env->me_pid)
           ? uniq_check(&env->me_lck_mmap, &inprocess_neighbor)
           : MDBX_PANIC;
  if (!inprocess_neighbor && env->me_live_reader)
    (void)osal_rpid_clear(env);
  if (!MDBX_IS_ERROR(rc))
    rc = osal_lck_destroy(env, inprocess_neighbor);
  return rc;
}

/*------------------------------------------------------------------------------
 * LY: State of the art quicksort-based sorting, with internal stack
 * and network-sort for small chunks.
 * Thanks to John M. Gamble for the http://pages.ripco.net/~jgamble/nw.html */

#if MDBX_HAVE_CMOV
#define SORT_CMP_SWAP(TYPE, CMP, a, b)                                         \
  do {                                                                         \
    const TYPE swap_tmp = (a);                                                 \
    const bool swap_cmp = expect_with_probability(CMP(swap_tmp, b), 0, .5);    \
    (a) = swap_cmp ? swap_tmp : b;                                             \
    (b) = swap_cmp ? b : swap_tmp;                                             \
  } while (0)
#else
#define SORT_CMP_SWAP(TYPE, CMP, a, b)                                         \
  do                                                                           \
    if (expect_with_probability(!CMP(a, b), 0, .5)) {                          \
      const TYPE swap_tmp = (a);                                               \
      (a) = (b);                                                               \
      (b) = swap_tmp;                                                          \
    }                                                                          \
  while (0)
#endif

//  3 comparators, 3 parallel operations
//  o-----^--^--o
//        |  |
//  o--^--|--v--o
//     |  |
//  o--v--v-----o
//
//  [[1,2]]
//  [[0,2]]
//  [[0,1]]
#define SORT_NETWORK_3(TYPE, CMP, begin)                                       \
  do {                                                                         \
    SORT_CMP_SWAP(TYPE, CMP, begin[1], begin[2]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[0], begin[2]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[0], begin[1]);                              \
  } while (0)

//  5 comparators, 3 parallel operations
//  o--^--^--------o
//     |  |
//  o--v--|--^--^--o
//        |  |  |
//  o--^--v--|--v--o
//     |     |
//  o--v-----v-----o
//
//  [[0,1],[2,3]]
//  [[0,2],[1,3]]
//  [[1,2]]
#define SORT_NETWORK_4(TYPE, CMP, begin)                                       \
  do {                                                                         \
    SORT_CMP_SWAP(TYPE, CMP, begin[0], begin[1]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[2], begin[3]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[0], begin[2]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[1], begin[3]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[1], begin[2]);                              \
  } while (0)

//  9 comparators, 5 parallel operations
//  o--^--^-----^-----------o
//     |  |     |
//  o--|--|--^--v-----^--^--o
//     |  |  |        |  |
//  o--|--v--|--^--^--|--v--o
//     |     |  |  |  |
//  o--|-----v--|--v--|--^--o
//     |        |     |  |
//  o--v--------v-----v--v--o
//
//  [[0,4],[1,3]]
//  [[0,2]]
//  [[2,4],[0,1]]
//  [[2,3],[1,4]]
//  [[1,2],[3,4]]
#define SORT_NETWORK_5(TYPE, CMP, begin)                                       \
  do {                                                                         \
    SORT_CMP_SWAP(TYPE, CMP, begin[0], begin[4]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[1], begin[3]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[0], begin[2]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[2], begin[4]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[0], begin[1]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[2], begin[3]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[1], begin[4]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[1], begin[2]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[3], begin[4]);                              \
  } while (0)

//  12 comparators, 6 parallel operations
//  o-----^--^--^-----------------o
//        |  |  |
//  o--^--|--v--|--^--------^-----o
//     |  |     |  |        |
//  o--v--v-----|--|--^--^--|--^--o
//              |  |  |  |  |  |
//  o-----^--^--v--|--|--|--v--v--o
//        |  |     |  |  |
//  o--^--|--v-----v--|--v--------o
//     |  |           |
//  o--v--v-----------v-----------o
//
//  [[1,2],[4,5]]
//  [[0,2],[3,5]]
//  [[0,1],[3,4],[2,5]]
//  [[0,3],[1,4]]
//  [[2,4],[1,3]]
//  [[2,3]]
#define SORT_NETWORK_6(TYPE, CMP, begin)                                       \
  do {                                                                         \
    SORT_CMP_SWAP(TYPE, CMP, begin[1], begin[2]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[4], begin[5]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[0], begin[2]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[3], begin[5]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[0], begin[1]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[3], begin[4]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[2], begin[5]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[0], begin[3]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[1], begin[4]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[2], begin[4]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[1], begin[3]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[2], begin[3]);                              \
  } while (0)

//  16 comparators, 6 parallel operations
//  o--^--------^-----^-----------------o
//     |        |     |
//  o--|--^-----|--^--v--------^--^-----o
//     |  |     |  |           |  |
//  o--|--|--^--v--|--^-----^--|--v-----o
//     |  |  |     |  |     |  |
//  o--|--|--|-----v--|--^--v--|--^--^--o
//     |  |  |        |  |     |  |  |
//  o--v--|--|--^-----v--|--^--v--|--v--o
//        |  |  |        |  |     |
//  o-----v--|--|--------v--v-----|--^--o
//           |  |                 |  |
//  o--------v--v-----------------v--v--o
//
//  [[0,4],[1,5],[2,6]]
//  [[0,2],[1,3],[4,6]]
//  [[2,4],[3,5],[0,1]]
//  [[2,3],[4,5]]
//  [[1,4],[3,6]]
//  [[1,2],[3,4],[5,6]]
#define SORT_NETWORK_7(TYPE, CMP, begin)                                       \
  do {                                                                         \
    SORT_CMP_SWAP(TYPE, CMP, begin[0], begin[4]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[1], begin[5]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[2], begin[6]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[0], begin[2]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[1], begin[3]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[4], begin[6]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[2], begin[4]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[3], begin[5]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[0], begin[1]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[2], begin[3]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[4], begin[5]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[1], begin[4]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[3], begin[6]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[1], begin[2]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[3], begin[4]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[5], begin[6]);                              \
  } while (0)

//  19 comparators, 6 parallel operations
//  o--^--------^-----^-----------------o
//     |        |     |
//  o--|--^-----|--^--v--------^--^-----o
//     |  |     |  |           |  |
//  o--|--|--^--v--|--^-----^--|--v-----o
//     |  |  |     |  |     |  |
//  o--|--|--|--^--v--|--^--v--|--^--^--o
//     |  |  |  |     |  |     |  |  |
//  o--v--|--|--|--^--v--|--^--v--|--v--o
//        |  |  |  |     |  |     |
//  o-----v--|--|--|--^--v--v-----|--^--o
//           |  |  |  |           |  |
//  o--------v--|--v--|--^--------v--v--o
//              |     |  |
//  o-----------v-----v--v--------------o
//
//  [[0,4],[1,5],[2,6],[3,7]]
//  [[0,2],[1,3],[4,6],[5,7]]
//  [[2,4],[3,5],[0,1],[6,7]]
//  [[2,3],[4,5]]
//  [[1,4],[3,6]]
//  [[1,2],[3,4],[5,6]]
#define SORT_NETWORK_8(TYPE, CMP, begin)                                       \
  do {                                                                         \
    SORT_CMP_SWAP(TYPE, CMP, begin[0], begin[4]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[1], begin[5]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[2], begin[6]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[3], begin[7]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[0], begin[2]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[1], begin[3]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[4], begin[6]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[5], begin[7]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[2], begin[4]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[3], begin[5]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[0], begin[1]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[6], begin[7]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[2], begin[3]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[4], begin[5]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[1], begin[4]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[3], begin[6]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[1], begin[2]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[3], begin[4]);                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[5], begin[6]);                              \
  } while (0)

#define SORT_INNER(TYPE, CMP, begin, end, len)                                 \
  switch (len) {                                                               \
  default:                                                                     \
    assert(false);                                                             \
    __unreachable();                                                           \
  case 0:                                                                      \
  case 1:                                                                      \
    break;                                                                     \
  case 2:                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[0], begin[1]);                              \
    break;                                                                     \
  case 3:                                                                      \
    SORT_NETWORK_3(TYPE, CMP, begin);                                          \
    break;                                                                     \
  case 4:                                                                      \
    SORT_NETWORK_4(TYPE, CMP, begin);                                          \
    break;                                                                     \
  case 5:                                                                      \
    SORT_NETWORK_5(TYPE, CMP, begin);                                          \
    break;                                                                     \
  case 6:                                                                      \
    SORT_NETWORK_6(TYPE, CMP, begin);                                          \
    break;                                                                     \
  case 7:                                                                      \
    SORT_NETWORK_7(TYPE, CMP, begin);                                          \
    break;                                                                     \
  case 8:                                                                      \
    SORT_NETWORK_8(TYPE, CMP, begin);                                          \
    break;                                                                     \
  }

#define SORT_SWAP(TYPE, a, b)                                                  \
  do {                                                                         \
    const TYPE swap_tmp = (a);                                                 \
    (a) = (b);                                                                 \
    (b) = swap_tmp;                                                            \
  } while (0)

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

#define SORT_IMPL(NAME, EXPECT_LOW_CARDINALITY_OR_PRESORTED, TYPE, CMP)        \
                                                                               \
  static __inline bool NAME##_is_sorted(const TYPE *first, const TYPE *last) { \
    while (++first <= last)                                                    \
      if (expect_with_probability(CMP(first[0], first[-1]), 1, .1))            \
        return false;                                                          \
    return true;                                                               \
  }                                                                            \
                                                                               \
  typedef struct {                                                             \
    TYPE *lo, *hi;                                                             \
  } NAME##_stack;                                                              \
                                                                               \
  __hot static void NAME(TYPE *const __restrict begin,                         \
                         TYPE *const __restrict end) {                         \
    NAME##_stack stack[sizeof(size_t) * CHAR_BIT], *__restrict top = stack;    \
                                                                               \
    TYPE *__restrict hi = end - 1;                                             \
    TYPE *__restrict lo = begin;                                               \
    while (true) {                                                             \
      const ptrdiff_t len = hi - lo;                                           \
      if (len < 8) {                                                           \
        SORT_INNER(TYPE, CMP, lo, hi + 1, len + 1);                            \
        if (unlikely(top == stack))                                            \
          break;                                                               \
        SORT_POP(lo, hi);                                                      \
        continue;                                                              \
      }                                                                        \
                                                                               \
      TYPE *__restrict mid = lo + (len >> 1);                                  \
      SORT_CMP_SWAP(TYPE, CMP, *lo, *mid);                                     \
      SORT_CMP_SWAP(TYPE, CMP, *mid, *hi);                                     \
      SORT_CMP_SWAP(TYPE, CMP, *lo, *mid);                                     \
                                                                               \
      TYPE *right = hi - 1;                                                    \
      TYPE *left = lo + 1;                                                     \
      while (1) {                                                              \
        while (expect_with_probability(CMP(*left, *mid), 0, .5))               \
          ++left;                                                              \
        while (expect_with_probability(CMP(*mid, *right), 0, .5))              \
          --right;                                                             \
        if (unlikely(left > right)) {                                          \
          if (EXPECT_LOW_CARDINALITY_OR_PRESORTED) {                           \
            if (NAME##_is_sorted(lo, right))                                   \
              lo = right + 1;                                                  \
            if (NAME##_is_sorted(left, hi))                                    \
              hi = left;                                                       \
          }                                                                    \
          break;                                                               \
        }                                                                      \
        SORT_SWAP(TYPE, *left, *right);                                        \
        mid = (mid == left) ? right : (mid == right) ? left : mid;             \
        ++left;                                                                \
        --right;                                                               \
      }                                                                        \
                                                                               \
      if (right - lo > hi - left) {                                            \
        SORT_PUSH(lo, right);                                                  \
        lo = left;                                                             \
      } else {                                                                 \
        SORT_PUSH(left, hi);                                                   \
        hi = right;                                                            \
      }                                                                        \
    }                                                                          \
                                                                               \
    if (AUDIT_ENABLED()) {                                                     \
      for (TYPE *scan = begin + 1; scan < end; ++scan)                         \
        assert(CMP(scan[-1], scan[0]));                                        \
    }                                                                          \
  }

/*------------------------------------------------------------------------------
 * LY: radix sort for large chunks */

#define RADIXSORT_IMPL(NAME, TYPE, EXTRACT_KEY, BUFFER_PREALLOCATED, END_GAP)  \
                                                                               \
  __hot static bool NAME##_radixsort(TYPE *const begin, const size_t length) { \
    TYPE *tmp;                                                                 \
    if (BUFFER_PREALLOCATED) {                                                 \
      tmp = begin + length + END_GAP;                                          \
      /* memset(tmp, 0xDeadBeef, sizeof(TYPE) * length); */                    \
    } else {                                                                   \
      tmp = osal_malloc(sizeof(TYPE) * length);                                \
      if (unlikely(!tmp))                                                      \
        return false;                                                          \
    }                                                                          \
                                                                               \
    size_t key_shift = 0, key_diff_mask;                                       \
    do {                                                                       \
      struct {                                                                 \
        pgno_t a[256], b[256];                                                 \
      } counters;                                                              \
      memset(&counters, 0, sizeof(counters));                                  \
                                                                               \
      key_diff_mask = 0;                                                       \
      size_t prev_key = EXTRACT_KEY(begin) >> key_shift;                       \
      TYPE *r = begin, *end = begin + length;                                  \
      do {                                                                     \
        const size_t key = EXTRACT_KEY(r) >> key_shift;                        \
        counters.a[key & 255]++;                                               \
        counters.b[(key >> 8) & 255]++;                                        \
        key_diff_mask |= prev_key ^ key;                                       \
        prev_key = key;                                                        \
      } while (++r != end);                                                    \
                                                                               \
      pgno_t ta = 0, tb = 0;                                                   \
      for (size_t i = 0; i < 256; ++i) {                                       \
        const pgno_t ia = counters.a[i];                                       \
        counters.a[i] = ta;                                                    \
        ta += ia;                                                              \
        const pgno_t ib = counters.b[i];                                       \
        counters.b[i] = tb;                                                    \
        tb += ib;                                                              \
      }                                                                        \
                                                                               \
      r = begin;                                                               \
      do {                                                                     \
        const size_t key = EXTRACT_KEY(r) >> key_shift;                        \
        tmp[counters.a[key & 255]++] = *r;                                     \
      } while (++r != end);                                                    \
                                                                               \
      if (unlikely(key_diff_mask < 256)) {                                     \
        memcpy(begin, tmp, ptr_dist(end, begin));                              \
        break;                                                                 \
      }                                                                        \
      end = (r = tmp) + length;                                                \
      do {                                                                     \
        const size_t key = EXTRACT_KEY(r) >> key_shift;                        \
        begin[counters.b[(key >> 8) & 255]++] = *r;                            \
      } while (++r != end);                                                    \
                                                                               \
      key_shift += 16;                                                         \
    } while (key_diff_mask >> 16);                                             \
                                                                               \
    if (!(BUFFER_PREALLOCATED))                                                \
      osal_free(tmp);                                                          \
    return true;                                                               \
  }

/*------------------------------------------------------------------------------
 * LY: Binary search */

#if defined(__clang__) && __clang_major__ > 4 && defined(__ia32__)
#define WORKAROUND_FOR_CLANG_OPTIMIZER_BUG(size, flag)                         \
  do                                                                           \
    __asm __volatile(""                                                        \
                     : "+r"(size)                                              \
                     : "r" /* the `b` constraint is more suitable here, but    \
                              cause CLANG to allocate and push/pop an one more \
                              register, so using the `r` which avoids this. */ \
                     (flag));                                                  \
  while (0)
#else
#define WORKAROUND_FOR_CLANG_OPTIMIZER_BUG(size, flag)                         \
  do {                                                                         \
    /* nope for non-clang or non-x86 */;                                       \
  } while (0)
#endif /* Workaround for CLANG */

#define BINARY_SEARCH_STEP(TYPE_LIST, CMP, it, size, key)                      \
  do {                                                                         \
  } while (0)

/* *INDENT-OFF* */
/* clang-format off */
#define SEARCH_IMPL(NAME, TYPE_LIST, TYPE_ARG, CMP)                            \
  static __always_inline const TYPE_LIST *NAME(                                \
      const TYPE_LIST *it, size_t length, const TYPE_ARG item) {               \
    const TYPE_LIST *const begin = it, *const end = begin + length;            \
                                                                               \
    if (MDBX_HAVE_CMOV)                                                        \
      do {                                                                     \
        /* Адаптивно-упрощенный шаг двоичного поиска:                          \
         *  - без переходов при наличии cmov или аналога;                      \
         *  - допускает лишние итерации;                                       \
         *  - но ищет пока size > 2, что требует дозавершения поиска           \
         *    среди остающихся 0-1-2 элементов. */                             \
        const TYPE_LIST *const middle = it + (length >> 1);                    \
        length = (length + 1) >> 1;                                            \
        const bool flag = expect_with_probability(CMP(*middle, item), 0, .5);  \
        WORKAROUND_FOR_CLANG_OPTIMIZER_BUG(length, flag);                      \
        it = flag ? middle : it;                                               \
      } while (length > 2);                                                    \
    else                                                                       \
      while (length > 2) {                                                     \
        /* Вариант с использованием условного перехода. Основное отличие в     \
         * том, что при "не равно" (true от компаратора) переход делается на 1 \
         * ближе к концу массива. Алгоритмически это верно и обеспечивает      \
         * чуть-чуть более быструю сходимость, но зато требует больше          \
         * вычислений при true от компаратора. Также ВАЖНО(!) не допускается   \
         * спекулятивное выполнение при size == 0. */                          \
        const TYPE_LIST *const middle = it + (length >> 1);                    \
        length = (length + 1) >> 1;                                            \
        const bool flag = expect_with_probability(CMP(*middle, item), 0, .5);  \
        if (flag) {                                                            \
          it = middle + 1;                                                     \
          length -= 1;                                                         \
        }                                                                      \
      }                                                                        \
    it += length > 1 && expect_with_probability(CMP(*it, item), 0, .5);        \
    it += length > 0 && expect_with_probability(CMP(*it, item), 0, .5);        \
                                                                               \
    if (AUDIT_ENABLED()) {                                                     \
      for (const TYPE_LIST *scan = begin; scan < it; ++scan)                   \
        assert(CMP(*scan, item));                                              \
      for (const TYPE_LIST *scan = it; scan < end; ++scan)                     \
        assert(!CMP(*scan, item));                                             \
      (void)begin, (void)end;                                                  \
    }                                                                          \
                                                                               \
    return it;                                                                 \
  }
/* *INDENT-ON* */
/* clang-format on */

/*----------------------------------------------------------------------------*/

static __always_inline size_t pnl_size2bytes(size_t size) {
  assert(size > 0 && size <= MDBX_PGL_LIMIT);
#if MDBX_PNL_PREALLOC_FOR_RADIXSORT
  size += size;
#endif /* MDBX_PNL_PREALLOC_FOR_RADIXSORT */
  STATIC_ASSERT(MDBX_ASSUME_MALLOC_OVERHEAD +
                    (MDBX_PGL_LIMIT * (MDBX_PNL_PREALLOC_FOR_RADIXSORT + 1) +
                     MDBX_PNL_GRANULATE + 3) *
                        sizeof(pgno_t) <
                SIZE_MAX / 4 * 3);
  size_t bytes =
      ceil_powerof2(MDBX_ASSUME_MALLOC_OVERHEAD + sizeof(pgno_t) * (size + 3),
                    MDBX_PNL_GRANULATE * sizeof(pgno_t)) -
      MDBX_ASSUME_MALLOC_OVERHEAD;
  return bytes;
}

static __always_inline pgno_t pnl_bytes2size(const size_t bytes) {
  size_t size = bytes / sizeof(pgno_t);
  assert(size > 3 && size <= MDBX_PGL_LIMIT + /* alignment gap */ 65536);
  size -= 3;
#if MDBX_PNL_PREALLOC_FOR_RADIXSORT
  size >>= 1;
#endif /* MDBX_PNL_PREALLOC_FOR_RADIXSORT */
  return (pgno_t)size;
}

static MDBX_PNL pnl_alloc(size_t size) {
  size_t bytes = pnl_size2bytes(size);
  MDBX_PNL pl = osal_malloc(bytes);
  if (likely(pl)) {
#if __GLIBC_PREREQ(2, 12) || defined(__FreeBSD__) || defined(malloc_usable_size)
    bytes = malloc_usable_size(pl);
#endif /* malloc_usable_size */
    pl[0] = pnl_bytes2size(bytes);
    assert(pl[0] >= size);
    pl += 1;
    *pl = 0;
  }
  return pl;
}

static void pnl_free(MDBX_PNL pl) {
  if (likely(pl))
    osal_free(pl - 1);
}

/* Shrink the PNL to the default size if it has grown larger */
static void pnl_shrink(MDBX_PNL *ppl) {
  assert(pnl_bytes2size(pnl_size2bytes(MDBX_PNL_INITIAL)) >= MDBX_PNL_INITIAL &&
         pnl_bytes2size(pnl_size2bytes(MDBX_PNL_INITIAL)) <
             MDBX_PNL_INITIAL * 3 / 2);
  assert(MDBX_PNL_GETSIZE(*ppl) <= MDBX_PGL_LIMIT &&
         MDBX_PNL_ALLOCLEN(*ppl) >= MDBX_PNL_GETSIZE(*ppl));
  MDBX_PNL_SETSIZE(*ppl, 0);
  if (unlikely(MDBX_PNL_ALLOCLEN(*ppl) >
               MDBX_PNL_INITIAL * (MDBX_PNL_PREALLOC_FOR_RADIXSORT ? 8 : 4) -
                   MDBX_CACHELINE_SIZE / sizeof(pgno_t))) {
    size_t bytes = pnl_size2bytes(MDBX_PNL_INITIAL * 2);
    MDBX_PNL pl = osal_realloc(*ppl - 1, bytes);
    if (likely(pl)) {
#if __GLIBC_PREREQ(2, 12) || defined(__FreeBSD__) || defined(malloc_usable_size)
      bytes = malloc_usable_size(pl);
#endif /* malloc_usable_size */
      *pl = pnl_bytes2size(bytes);
      *ppl = pl + 1;
    }
  }
}

/* Grow the PNL to the size growed to at least given size */
static int pnl_reserve(MDBX_PNL *ppl, const size_t wanna) {
  const size_t allocated = MDBX_PNL_ALLOCLEN(*ppl);
  assert(MDBX_PNL_GETSIZE(*ppl) <= MDBX_PGL_LIMIT &&
         MDBX_PNL_ALLOCLEN(*ppl) >= MDBX_PNL_GETSIZE(*ppl));
  if (likely(allocated >= wanna))
    return MDBX_SUCCESS;

  if (unlikely(wanna > /* paranoia */ MDBX_PGL_LIMIT)) {
    ERROR("PNL too long (%zu > %zu)", wanna, (size_t)MDBX_PGL_LIMIT);
    return MDBX_TXN_FULL;
  }

  const size_t size = (wanna + wanna - allocated < MDBX_PGL_LIMIT)
                          ? wanna + wanna - allocated
                          : MDBX_PGL_LIMIT;
  size_t bytes = pnl_size2bytes(size);
  MDBX_PNL pl = osal_realloc(*ppl - 1, bytes);
  if (likely(pl)) {
#if __GLIBC_PREREQ(2, 12) || defined(__FreeBSD__) || defined(malloc_usable_size)
    bytes = malloc_usable_size(pl);
#endif /* malloc_usable_size */
    *pl = pnl_bytes2size(bytes);
    assert(*pl >= wanna);
    *ppl = pl + 1;
    return MDBX_SUCCESS;
  }
  return MDBX_ENOMEM;
}

/* Make room for num additional elements in an PNL */
static __always_inline int __must_check_result pnl_need(MDBX_PNL *ppl,
                                                        size_t num) {
  assert(MDBX_PNL_GETSIZE(*ppl) <= MDBX_PGL_LIMIT &&
         MDBX_PNL_ALLOCLEN(*ppl) >= MDBX_PNL_GETSIZE(*ppl));
  assert(num <= MDBX_PGL_LIMIT);
  const size_t wanna = MDBX_PNL_GETSIZE(*ppl) + num;
  return likely(MDBX_PNL_ALLOCLEN(*ppl) >= wanna) ? MDBX_SUCCESS
                                                  : pnl_reserve(ppl, wanna);
}

static __always_inline void pnl_xappend(MDBX_PNL pl, pgno_t pgno) {
  assert(MDBX_PNL_GETSIZE(pl) < MDBX_PNL_ALLOCLEN(pl));
  if (AUDIT_ENABLED()) {
    for (size_t i = MDBX_PNL_GETSIZE(pl); i > 0; --i)
      assert(pgno != pl[i]);
  }
  *pl += 1;
  MDBX_PNL_LAST(pl) = pgno;
}

/* Append an pgno range onto an unsorted PNL */
__always_inline static int __must_check_result pnl_append_range(bool spilled,
                                                                MDBX_PNL *ppl,
                                                                pgno_t pgno,
                                                                size_t n) {
  assert(n > 0);
  int rc = pnl_need(ppl, n);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  const MDBX_PNL pnl = *ppl;
#if MDBX_PNL_ASCENDING
  size_t w = MDBX_PNL_GETSIZE(pnl);
  do {
    pnl[++w] = pgno;
    pgno += spilled ? 2 : 1;
  } while (--n);
  MDBX_PNL_SETSIZE(pnl, w);
#else
  size_t w = MDBX_PNL_GETSIZE(pnl) + n;
  MDBX_PNL_SETSIZE(pnl, w);
  do {
    pnl[w--] = pgno;
    pgno += spilled ? 2 : 1;
  } while (--n);
#endif

  return MDBX_SUCCESS;
}

/* Append an pgno range into the sorted PNL */
__hot static int __must_check_result pnl_insert_range(MDBX_PNL *ppl,
                                                      pgno_t pgno, size_t n) {
  assert(n > 0);
  int rc = pnl_need(ppl, n);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  const MDBX_PNL pnl = *ppl;
  size_t r = MDBX_PNL_GETSIZE(pnl), w = r + n;
  MDBX_PNL_SETSIZE(pnl, w);
  while (r && MDBX_PNL_DISORDERED(pnl[r], pgno))
    pnl[w--] = pnl[r--];

  for (pgno_t fill = MDBX_PNL_ASCENDING ? pgno + n : pgno; w > r; --w)
    pnl[w] = MDBX_PNL_ASCENDING ? --fill : fill++;

  return MDBX_SUCCESS;
}

__hot static bool pnl_check(const pgno_t *pl, const size_t limit) {
  assert(limit >= MIN_PAGENO - MDBX_ENABLE_REFUND);
  if (likely(MDBX_PNL_GETSIZE(pl))) {
    if (unlikely(MDBX_PNL_GETSIZE(pl) > MDBX_PGL_LIMIT))
      return false;
    if (unlikely(MDBX_PNL_LEAST(pl) < MIN_PAGENO))
      return false;
    if (unlikely(MDBX_PNL_MOST(pl) >= limit))
      return false;

    if ((!MDBX_DISABLE_VALIDATION || AUDIT_ENABLED()) &&
        likely(MDBX_PNL_GETSIZE(pl) > 1)) {
      const pgno_t *scan = MDBX_PNL_BEGIN(pl);
      const pgno_t *const end = MDBX_PNL_END(pl);
      pgno_t prev = *scan++;
      do {
        if (unlikely(!MDBX_PNL_ORDERED(prev, *scan)))
          return false;
        prev = *scan;
      } while (likely(++scan != end));
    }
  }
  return true;
}

static __always_inline bool pnl_check_allocated(const pgno_t *pl,
                                                const size_t limit) {
  return pl == nullptr || (MDBX_PNL_ALLOCLEN(pl) >= MDBX_PNL_GETSIZE(pl) &&
                           pnl_check(pl, limit));
}

static __always_inline void
pnl_merge_inner(pgno_t *__restrict dst, const pgno_t *__restrict src_a,
                const pgno_t *__restrict src_b,
                const pgno_t *__restrict const src_b_detent) {
  do {
#if MDBX_HAVE_CMOV
    const bool flag = MDBX_PNL_ORDERED(*src_b, *src_a);
#if defined(__LCC__) || __CLANG_PREREQ(13, 0)
    // lcc 1.26: 13ШК (подготовка и первая итерация) + 7ШК (цикл), БЕЗ loop-mode
    // gcc>=7: cmp+jmp с возвратом в тело цикла (WTF?)
    // gcc<=6: cmov×3
    // clang<=12: cmov×3
    // clang>=13: cmov, set+add/sub
    *dst = flag ? *src_a-- : *src_b--;
#else
    // gcc: cmov, cmp+set+add/sub
    // clang<=5: cmov×2, set+add/sub
    // clang>=6: cmov, set+add/sub
    *dst = flag ? *src_a : *src_b;
    src_b += (ptrdiff_t)flag - 1;
    src_a -= flag;
#endif
    --dst;
#else  /* MDBX_HAVE_CMOV */
    while (MDBX_PNL_ORDERED(*src_b, *src_a))
      *dst-- = *src_a--;
    *dst-- = *src_b--;
#endif /* !MDBX_HAVE_CMOV */
  } while (likely(src_b > src_b_detent));
}

/* Merge a PNL onto a PNL. The destination PNL must be big enough */
__hot static size_t pnl_merge(MDBX_PNL dst, const MDBX_PNL src) {
  assert(pnl_check_allocated(dst, MAX_PAGENO + 1));
  assert(pnl_check(src, MAX_PAGENO + 1));
  const size_t src_len = MDBX_PNL_GETSIZE(src);
  const size_t dst_len = MDBX_PNL_GETSIZE(dst);
  size_t total = dst_len;
  assert(MDBX_PNL_ALLOCLEN(dst) >= total);
  if (likely(src_len > 0)) {
    total += src_len;
    if (!MDBX_DEBUG && total < (MDBX_HAVE_CMOV ? 21 : 12))
      goto avoid_call_libc_for_short_cases;
    if (dst_len == 0 ||
        MDBX_PNL_ORDERED(MDBX_PNL_LAST(dst), MDBX_PNL_FIRST(src)))
      memcpy(MDBX_PNL_END(dst), MDBX_PNL_BEGIN(src), src_len * sizeof(pgno_t));
    else if (MDBX_PNL_ORDERED(MDBX_PNL_LAST(src), MDBX_PNL_FIRST(dst))) {
      memmove(MDBX_PNL_BEGIN(dst) + src_len, MDBX_PNL_BEGIN(dst),
              dst_len * sizeof(pgno_t));
      memcpy(MDBX_PNL_BEGIN(dst), MDBX_PNL_BEGIN(src),
             src_len * sizeof(pgno_t));
    } else {
    avoid_call_libc_for_short_cases:
      dst[0] = /* the detent */ (MDBX_PNL_ASCENDING ? 0 : P_INVALID);
      pnl_merge_inner(dst + total, dst + dst_len, src + src_len, src);
    }
    MDBX_PNL_SETSIZE(dst, total);
  }
  assert(pnl_check_allocated(dst, MAX_PAGENO + 1));
  return total;
}

static void spill_remove(MDBX_txn *txn, size_t idx, size_t npages) {
  tASSERT(txn, idx > 0 && idx <= MDBX_PNL_GETSIZE(txn->tw.spilled.list) &&
                   txn->tw.spilled.least_removed > 0);
  txn->tw.spilled.least_removed = (idx < txn->tw.spilled.least_removed)
                                      ? idx
                                      : txn->tw.spilled.least_removed;
  txn->tw.spilled.list[idx] |= 1;
  MDBX_PNL_SETSIZE(txn->tw.spilled.list,
                   MDBX_PNL_GETSIZE(txn->tw.spilled.list) -
                       (idx == MDBX_PNL_GETSIZE(txn->tw.spilled.list)));

  while (unlikely(npages > 1)) {
    const pgno_t pgno = (txn->tw.spilled.list[idx] >> 1) + 1;
    if (MDBX_PNL_ASCENDING) {
      if (++idx > MDBX_PNL_GETSIZE(txn->tw.spilled.list) ||
          (txn->tw.spilled.list[idx] >> 1) != pgno)
        return;
    } else {
      if (--idx < 1 || (txn->tw.spilled.list[idx] >> 1) != pgno)
        return;
      txn->tw.spilled.least_removed = (idx < txn->tw.spilled.least_removed)
                                          ? idx
                                          : txn->tw.spilled.least_removed;
    }
    txn->tw.spilled.list[idx] |= 1;
    MDBX_PNL_SETSIZE(txn->tw.spilled.list,
                     MDBX_PNL_GETSIZE(txn->tw.spilled.list) -
                         (idx == MDBX_PNL_GETSIZE(txn->tw.spilled.list)));
    --npages;
  }
}

static MDBX_PNL spill_purge(MDBX_txn *txn) {
  tASSERT(txn, txn->tw.spilled.least_removed > 0);
  const MDBX_PNL sl = txn->tw.spilled.list;
  if (txn->tw.spilled.least_removed != INT_MAX) {
    size_t len = MDBX_PNL_GETSIZE(sl), r, w;
    for (w = r = txn->tw.spilled.least_removed; r <= len; ++r) {
      sl[w] = sl[r];
      w += 1 - (sl[r] & 1);
    }
    for (size_t i = 1; i < w; ++i)
      tASSERT(txn, (sl[i] & 1) == 0);
    MDBX_PNL_SETSIZE(sl, w - 1);
    txn->tw.spilled.least_removed = INT_MAX;
  } else {
    for (size_t i = 1; i <= MDBX_PNL_GETSIZE(sl); ++i)
      tASSERT(txn, (sl[i] & 1) == 0);
  }
  return sl;
}

#if MDBX_PNL_ASCENDING
#define MDBX_PNL_EXTRACT_KEY(ptr) (*(ptr))
#else
#define MDBX_PNL_EXTRACT_KEY(ptr) (P_INVALID - *(ptr))
#endif
RADIXSORT_IMPL(pgno, pgno_t, MDBX_PNL_EXTRACT_KEY,
               MDBX_PNL_PREALLOC_FOR_RADIXSORT, 0)

SORT_IMPL(pgno_sort, false, pgno_t, MDBX_PNL_ORDERED)

__hot __noinline static void pnl_sort_nochk(MDBX_PNL pnl) {
  if (likely(MDBX_PNL_GETSIZE(pnl) < MDBX_RADIXSORT_THRESHOLD) ||
      unlikely(!pgno_radixsort(&MDBX_PNL_FIRST(pnl), MDBX_PNL_GETSIZE(pnl))))
    pgno_sort(MDBX_PNL_BEGIN(pnl), MDBX_PNL_END(pnl));
}

static __inline void pnl_sort(MDBX_PNL pnl, size_t limit4check) {
  pnl_sort_nochk(pnl);
  assert(pnl_check(pnl, limit4check));
  (void)limit4check;
}

/* Search for an pgno in an PNL.
 * Returns The index of the first item greater than or equal to pgno. */
SEARCH_IMPL(pgno_bsearch, pgno_t, pgno_t, MDBX_PNL_ORDERED)

__hot __noinline static size_t pnl_search_nochk(const MDBX_PNL pnl,
                                                pgno_t pgno) {
  const pgno_t *begin = MDBX_PNL_BEGIN(pnl);
  const pgno_t *it = pgno_bsearch(begin, MDBX_PNL_GETSIZE(pnl), pgno);
  const pgno_t *end = begin + MDBX_PNL_GETSIZE(pnl);
  assert(it >= begin && it <= end);
  if (it != begin)
    assert(MDBX_PNL_ORDERED(it[-1], pgno));
  if (it != end)
    assert(!MDBX_PNL_ORDERED(it[0], pgno));
  return it - begin + 1;
}

static __inline size_t pnl_search(const MDBX_PNL pnl, pgno_t pgno,
                                  size_t limit) {
  assert(pnl_check_allocated(pnl, limit));
  if (MDBX_HAVE_CMOV) {
    /* cmov-ускоренный бинарный поиск может читать (но не использовать) один
     * элемент за концом данных, этот элемент в пределах выделенного участка
     * памяти, но не инициализирован. */
    VALGRIND_MAKE_MEM_DEFINED(MDBX_PNL_END(pnl), sizeof(pgno_t));
  }
  assert(pgno < limit);
  (void)limit;
  size_t n = pnl_search_nochk(pnl, pgno);
  if (MDBX_HAVE_CMOV) {
    VALGRIND_MAKE_MEM_UNDEFINED(MDBX_PNL_END(pnl), sizeof(pgno_t));
  }
  return n;
}

static __inline size_t search_spilled(const MDBX_txn *txn, pgno_t pgno) {
  tASSERT(txn, (txn->mt_flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);
  const MDBX_PNL pnl = txn->tw.spilled.list;
  if (likely(!pnl))
    return 0;
  pgno <<= 1;
  size_t n = pnl_search(pnl, pgno, (size_t)MAX_PAGENO + MAX_PAGENO + 1);
  return (n <= MDBX_PNL_GETSIZE(pnl) && pnl[n] == pgno) ? n : 0;
}

static __inline bool intersect_spilled(const MDBX_txn *txn, pgno_t pgno,
                                       size_t npages) {
  const MDBX_PNL pnl = txn->tw.spilled.list;
  if (likely(!pnl))
    return false;
  const size_t len = MDBX_PNL_GETSIZE(pnl);
  if (LOG_ENABLED(MDBX_LOG_EXTRA)) {
    DEBUG_EXTRA("PNL len %zu [", len);
    for (size_t i = 1; i <= len; ++i)
      DEBUG_EXTRA_PRINT(" %li", (pnl[i] & 1) ? -(long)(pnl[i] >> 1)
                                             : (long)(pnl[i] >> 1));
    DEBUG_EXTRA_PRINT("%s\n", "]");
  }
  const pgno_t spilled_range_begin = pgno << 1;
  const pgno_t spilled_range_last = ((pgno + (pgno_t)npages) << 1) - 1;
#if MDBX_PNL_ASCENDING
  const size_t n =
      pnl_search(pnl, spilled_range_begin, (size_t)(MAX_PAGENO + 1) << 1);
  assert(n &&
         (n == MDBX_PNL_GETSIZE(pnl) + 1 || spilled_range_begin <= pnl[n]));
  const bool rc = n <= MDBX_PNL_GETSIZE(pnl) && pnl[n] <= spilled_range_last;
#else
  const size_t n =
      pnl_search(pnl, spilled_range_last, (size_t)MAX_PAGENO + MAX_PAGENO + 1);
  assert(n && (n == MDBX_PNL_GETSIZE(pnl) + 1 || spilled_range_last >= pnl[n]));
  const bool rc = n <= MDBX_PNL_GETSIZE(pnl) && pnl[n] >= spilled_range_begin;
#endif
  if (ASSERT_ENABLED()) {
    bool check = false;
    for (size_t i = 0; i < npages; ++i)
      check |= search_spilled(txn, (pgno_t)(pgno + i)) != 0;
    assert(check == rc);
  }
  return rc;
}

/*----------------------------------------------------------------------------*/

static __always_inline size_t txl_size2bytes(const size_t size) {
  assert(size > 0 && size <= MDBX_TXL_MAX * 2);
  size_t bytes =
      ceil_powerof2(MDBX_ASSUME_MALLOC_OVERHEAD + sizeof(txnid_t) * (size + 2),
                    MDBX_TXL_GRANULATE * sizeof(txnid_t)) -
      MDBX_ASSUME_MALLOC_OVERHEAD;
  return bytes;
}

static __always_inline size_t txl_bytes2size(const size_t bytes) {
  size_t size = bytes / sizeof(txnid_t);
  assert(size > 2 && size <= MDBX_TXL_MAX * 2);
  return size - 2;
}

static MDBX_TXL txl_alloc(void) {
  size_t bytes = txl_size2bytes(MDBX_TXL_INITIAL);
  MDBX_TXL tl = osal_malloc(bytes);
  if (likely(tl)) {
#if __GLIBC_PREREQ(2, 12) || defined(__FreeBSD__) || defined(malloc_usable_size)
    bytes = malloc_usable_size(tl);
#endif /* malloc_usable_size */
    tl[0] = txl_bytes2size(bytes);
    assert(tl[0] >= MDBX_TXL_INITIAL);
    tl += 1;
    *tl = 0;
  }
  return tl;
}

static void txl_free(MDBX_TXL tl) {
  if (likely(tl))
    osal_free(tl - 1);
}

static int txl_reserve(MDBX_TXL *ptl, const size_t wanna) {
  const size_t allocated = (size_t)MDBX_PNL_ALLOCLEN(*ptl);
  assert(MDBX_PNL_GETSIZE(*ptl) <= MDBX_TXL_MAX &&
         MDBX_PNL_ALLOCLEN(*ptl) >= MDBX_PNL_GETSIZE(*ptl));
  if (likely(allocated >= wanna))
    return MDBX_SUCCESS;

  if (unlikely(wanna > /* paranoia */ MDBX_TXL_MAX)) {
    ERROR("TXL too long (%zu > %zu)", wanna, (size_t)MDBX_TXL_MAX);
    return MDBX_TXN_FULL;
  }

  const size_t size = (wanna + wanna - allocated < MDBX_TXL_MAX)
                          ? wanna + wanna - allocated
                          : MDBX_TXL_MAX;
  size_t bytes = txl_size2bytes(size);
  MDBX_TXL tl = osal_realloc(*ptl - 1, bytes);
  if (likely(tl)) {
#if __GLIBC_PREREQ(2, 12) || defined(__FreeBSD__) || defined(malloc_usable_size)
    bytes = malloc_usable_size(tl);
#endif /* malloc_usable_size */
    *tl = txl_bytes2size(bytes);
    assert(*tl >= wanna);
    *ptl = tl + 1;
    return MDBX_SUCCESS;
  }
  return MDBX_ENOMEM;
}

static __always_inline int __must_check_result txl_need(MDBX_TXL *ptl,
                                                        size_t num) {
  assert(MDBX_PNL_GETSIZE(*ptl) <= MDBX_TXL_MAX &&
         MDBX_PNL_ALLOCLEN(*ptl) >= MDBX_PNL_GETSIZE(*ptl));
  assert(num <= MDBX_PGL_LIMIT);
  const size_t wanna = (size_t)MDBX_PNL_GETSIZE(*ptl) + num;
  return likely(MDBX_PNL_ALLOCLEN(*ptl) >= wanna) ? MDBX_SUCCESS
                                                  : txl_reserve(ptl, wanna);
}

static __always_inline void txl_xappend(MDBX_TXL tl, txnid_t id) {
  assert(MDBX_PNL_GETSIZE(tl) < MDBX_PNL_ALLOCLEN(tl));
  tl[0] += 1;
  MDBX_PNL_LAST(tl) = id;
}

#define TXNID_SORT_CMP(first, last) ((first) > (last))
SORT_IMPL(txnid_sort, false, txnid_t, TXNID_SORT_CMP)
static void txl_sort(MDBX_TXL tl) {
  txnid_sort(MDBX_PNL_BEGIN(tl), MDBX_PNL_END(tl));
}

static int __must_check_result txl_append(MDBX_TXL *ptl, txnid_t id) {
  if (unlikely(MDBX_PNL_GETSIZE(*ptl) == MDBX_PNL_ALLOCLEN(*ptl))) {
    int rc = txl_need(ptl, MDBX_TXL_GRANULATE);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }
  txl_xappend(*ptl, id);
  return MDBX_SUCCESS;
}

/*----------------------------------------------------------------------------*/

#define MDBX_DPL_GAP_MERGESORT 16
#define MDBX_DPL_GAP_EDGING 2
#define MDBX_DPL_RESERVE_GAP (MDBX_DPL_GAP_MERGESORT + MDBX_DPL_GAP_EDGING)

static __always_inline size_t dpl_size2bytes(ptrdiff_t size) {
  assert(size > CURSOR_STACK && (size_t)size <= MDBX_PGL_LIMIT);
#if MDBX_DPL_PREALLOC_FOR_RADIXSORT
  size += size;
#endif /* MDBX_DPL_PREALLOC_FOR_RADIXSORT */
  STATIC_ASSERT(MDBX_ASSUME_MALLOC_OVERHEAD + sizeof(MDBX_dpl) +
                    (MDBX_PGL_LIMIT * (MDBX_DPL_PREALLOC_FOR_RADIXSORT + 1) +
                     MDBX_DPL_RESERVE_GAP) *
                        sizeof(MDBX_dp) +
                    MDBX_PNL_GRANULATE * sizeof(void *) * 2 <
                SIZE_MAX / 4 * 3);
  size_t bytes =
      ceil_powerof2(MDBX_ASSUME_MALLOC_OVERHEAD + sizeof(MDBX_dpl) +
                        ((size_t)size + MDBX_DPL_RESERVE_GAP) * sizeof(MDBX_dp),
                    MDBX_PNL_GRANULATE * sizeof(void *) * 2) -
      MDBX_ASSUME_MALLOC_OVERHEAD;
  return bytes;
}

static __always_inline size_t dpl_bytes2size(const ptrdiff_t bytes) {
  size_t size = (bytes - sizeof(MDBX_dpl)) / sizeof(MDBX_dp);
  size -= MDBX_DPL_RESERVE_GAP;
#if MDBX_DPL_PREALLOC_FOR_RADIXSORT
  size >>= 1;
#endif /* MDBX_DPL_PREALLOC_FOR_RADIXSORT */
  assert(size > CURSOR_STACK && size <= MDBX_PGL_LIMIT + MDBX_PNL_GRANULATE);
  return size;
}

static __always_inline size_t dpl_setlen(MDBX_dpl *dl, size_t len) {
  static const MDBX_page dpl_stub_pageE = {INVALID_TXNID,
                                           0,
                                           P_BAD,
                                           {0},
                                           /* pgno */ ~(pgno_t)0};
  assert(dpl_stub_pageE.mp_flags == P_BAD &&
         dpl_stub_pageE.mp_pgno == P_INVALID);
  dl->length = len;
  dl->items[len + 1].ptr = (MDBX_page *)&dpl_stub_pageE;
  dl->items[len + 1].pgno = P_INVALID;
  dl->items[len + 1].npages = 1;
  return len;
}

static __always_inline void dpl_clear(MDBX_dpl *dl) {
  static const MDBX_page dpl_stub_pageB = {INVALID_TXNID,
                                           0,
                                           P_BAD,
                                           {0},
                                           /* pgno */ 0};
  assert(dpl_stub_pageB.mp_flags == P_BAD && dpl_stub_pageB.mp_pgno == 0);
  dl->sorted = dpl_setlen(dl, 0);
  dl->pages_including_loose = 0;
  dl->items[0].ptr = (MDBX_page *)&dpl_stub_pageB;
  dl->items[0].pgno = 0;
  dl->items[0].npages = 1;
  assert(dl->items[0].pgno == 0 && dl->items[dl->length + 1].pgno == P_INVALID);
}

static void dpl_free(MDBX_txn *txn) {
  if (likely(txn->tw.dirtylist)) {
    osal_free(txn->tw.dirtylist);
    txn->tw.dirtylist = NULL;
  }
}

static MDBX_dpl *dpl_reserve(MDBX_txn *txn, size_t size) {
  tASSERT(txn, (txn->mt_flags & MDBX_TXN_RDONLY) == 0);
  tASSERT(txn, (txn->mt_flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);

  size_t bytes =
      dpl_size2bytes((size < MDBX_PGL_LIMIT) ? size : MDBX_PGL_LIMIT);
  MDBX_dpl *const dl = osal_realloc(txn->tw.dirtylist, bytes);
  if (likely(dl)) {
#if __GLIBC_PREREQ(2, 12) || defined(__FreeBSD__) || defined(malloc_usable_size)
    bytes = malloc_usable_size(dl);
#endif /* malloc_usable_size */
    dl->detent = dpl_bytes2size(bytes);
    tASSERT(txn, txn->tw.dirtylist == NULL || dl->length <= dl->detent);
    txn->tw.dirtylist = dl;
  }
  return dl;
}

static int dpl_alloc(MDBX_txn *txn) {
  tASSERT(txn, (txn->mt_flags & MDBX_TXN_RDONLY) == 0);
  tASSERT(txn, (txn->mt_flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);

  const size_t wanna = (txn->mt_env->me_options.dp_initial < txn->mt_geo.upper)
                           ? txn->mt_env->me_options.dp_initial
                           : txn->mt_geo.upper;
#if MDBX_FORCE_ASSERTIONS || MDBX_DEBUG
  if (txn->tw.dirtylist)
    /* обнуляем чтобы не сработал ассерт внутри dpl_reserve() */
    txn->tw.dirtylist->sorted = txn->tw.dirtylist->length = 0;
#endif /* asertions enabled */
  if (unlikely(!txn->tw.dirtylist || txn->tw.dirtylist->detent < wanna ||
               txn->tw.dirtylist->detent > wanna + wanna) &&
      unlikely(!dpl_reserve(txn, wanna)))
    return MDBX_ENOMEM;

  dpl_clear(txn->tw.dirtylist);
  return MDBX_SUCCESS;
}

#define MDBX_DPL_EXTRACT_KEY(ptr) ((ptr)->pgno)
RADIXSORT_IMPL(dpl, MDBX_dp, MDBX_DPL_EXTRACT_KEY,
               MDBX_DPL_PREALLOC_FOR_RADIXSORT, 1)

#define DP_SORT_CMP(first, last) ((first).pgno < (last).pgno)
SORT_IMPL(dp_sort, false, MDBX_dp, DP_SORT_CMP)

__hot __noinline static MDBX_dpl *dpl_sort_slowpath(const MDBX_txn *txn) {
  tASSERT(txn, (txn->mt_flags & MDBX_TXN_RDONLY) == 0);
  tASSERT(txn, (txn->mt_flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);

  MDBX_dpl *dl = txn->tw.dirtylist;
  assert(dl->items[0].pgno == 0 && dl->items[dl->length + 1].pgno == P_INVALID);
  const size_t unsorted = dl->length - dl->sorted;
  if (likely(unsorted < MDBX_RADIXSORT_THRESHOLD) ||
      unlikely(!dpl_radixsort(dl->items + 1, dl->length))) {
    if (dl->sorted > unsorted / 4 + 4 &&
        (MDBX_DPL_PREALLOC_FOR_RADIXSORT ||
         dl->length + unsorted < dl->detent + MDBX_DPL_GAP_MERGESORT)) {
      MDBX_dp *const sorted_begin = dl->items + 1;
      MDBX_dp *const sorted_end = sorted_begin + dl->sorted;
      MDBX_dp *const end =
          dl->items + (MDBX_DPL_PREALLOC_FOR_RADIXSORT
                           ? dl->length + dl->length + 1
                           : dl->detent + MDBX_DPL_RESERVE_GAP);
      MDBX_dp *const tmp = end - unsorted;
      assert(dl->items + dl->length + 1 < tmp);
      /* copy unsorted to the end of allocated space and sort it */
      memcpy(tmp, sorted_end, unsorted * sizeof(MDBX_dp));
      dp_sort(tmp, tmp + unsorted);
      /* merge two parts from end to begin */
      MDBX_dp *__restrict w = dl->items + dl->length;
      MDBX_dp *__restrict l = dl->items + dl->sorted;
      MDBX_dp *__restrict r = end - 1;
      do {
        const bool cmp = expect_with_probability(l->pgno > r->pgno, 0, .5);
#if defined(__LCC__) || __CLANG_PREREQ(13, 0) || !MDBX_HAVE_CMOV
        *w = cmp ? *l-- : *r--;
#else
        *w = cmp ? *l : *r;
        l -= cmp;
        r += (ptrdiff_t)cmp - 1;
#endif
      } while (likely(--w > l));
      assert(r == tmp - 1);
      assert(dl->items[0].pgno == 0 &&
             dl->items[dl->length + 1].pgno == P_INVALID);
      if (ASSERT_ENABLED())
        for (size_t i = 0; i <= dl->length; ++i)
          assert(dl->items[i].pgno < dl->items[i + 1].pgno);
    } else {
      dp_sort(dl->items + 1, dl->items + dl->length + 1);
      assert(dl->items[0].pgno == 0 &&
             dl->items[dl->length + 1].pgno == P_INVALID);
    }
  } else {
    assert(dl->items[0].pgno == 0 &&
           dl->items[dl->length + 1].pgno == P_INVALID);
  }
  dl->sorted = dl->length;
  return dl;
}

static __always_inline MDBX_dpl *dpl_sort(const MDBX_txn *txn) {
  tASSERT(txn, (txn->mt_flags & MDBX_TXN_RDONLY) == 0);
  tASSERT(txn, (txn->mt_flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);

  MDBX_dpl *dl = txn->tw.dirtylist;
  assert(dl->length <= MDBX_PGL_LIMIT);
  assert(dl->sorted <= dl->length);
  assert(dl->items[0].pgno == 0 && dl->items[dl->length + 1].pgno == P_INVALID);
  return likely(dl->sorted == dl->length) ? dl : dpl_sort_slowpath(txn);
}

/* Returns the index of the first dirty-page whose pgno
 * member is greater than or equal to id. */
#define DP_SEARCH_CMP(dp, id) ((dp).pgno < (id))
SEARCH_IMPL(dp_bsearch, MDBX_dp, pgno_t, DP_SEARCH_CMP)

__hot __noinline static size_t dpl_search(const MDBX_txn *txn, pgno_t pgno) {
  tASSERT(txn, (txn->mt_flags & MDBX_TXN_RDONLY) == 0);
  tASSERT(txn, (txn->mt_flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);

  MDBX_dpl *dl = txn->tw.dirtylist;
  assert(dl->items[0].pgno == 0 && dl->items[dl->length + 1].pgno == P_INVALID);
  if (AUDIT_ENABLED()) {
    for (const MDBX_dp *ptr = dl->items + dl->sorted; --ptr > dl->items;) {
      assert(ptr[0].pgno < ptr[1].pgno);
      assert(ptr[0].pgno >= NUM_METAS);
    }
  }

  switch (dl->length - dl->sorted) {
  default:
    /* sort a whole */
    dpl_sort_slowpath(txn);
    break;
  case 0:
    /* whole sorted cases */
    break;

#define LINEAR_SEARCH_CASE(N)                                                  \
  case N:                                                                      \
    if (dl->items[dl->length - N + 1].pgno == pgno)                            \
      return dl->length - N + 1;                                               \
    __fallthrough

    /* use linear scan until the threshold */
    LINEAR_SEARCH_CASE(7); /* fall through */
    LINEAR_SEARCH_CASE(6); /* fall through */
    LINEAR_SEARCH_CASE(5); /* fall through */
    LINEAR_SEARCH_CASE(4); /* fall through */
    LINEAR_SEARCH_CASE(3); /* fall through */
    LINEAR_SEARCH_CASE(2); /* fall through */
  case 1:
    if (dl->items[dl->length].pgno == pgno)
      return dl->length;
    /* continue bsearch on the sorted part */
    break;
  }
  return dp_bsearch(dl->items + 1, dl->sorted, pgno) - dl->items;
}

MDBX_NOTHROW_PURE_FUNCTION static __inline unsigned
dpl_npages(const MDBX_dpl *dl, size_t i) {
  assert(0 <= (intptr_t)i && i <= dl->length);
  unsigned n = dl->items[i].npages;
  assert(n == (IS_OVERFLOW(dl->items[i].ptr) ? dl->items[i].ptr->mp_pages : 1));
  return n;
}

MDBX_NOTHROW_PURE_FUNCTION static __inline pgno_t
dpl_endpgno(const MDBX_dpl *dl, size_t i) {
  return dpl_npages(dl, i) + dl->items[i].pgno;
}

static __inline bool dpl_intersect(const MDBX_txn *txn, pgno_t pgno,
                                   size_t npages) {
  tASSERT(txn, (txn->mt_flags & MDBX_TXN_RDONLY) == 0);
  tASSERT(txn, (txn->mt_flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);

  MDBX_dpl *dl = txn->tw.dirtylist;
  assert(dl->sorted == dl->length);
  assert(dl->items[0].pgno == 0 && dl->items[dl->length + 1].pgno == P_INVALID);
  size_t const n = dpl_search(txn, pgno);
  assert(n >= 1 && n <= dl->length + 1);
  assert(pgno <= dl->items[n].pgno);
  assert(pgno > dl->items[n - 1].pgno);
  const bool rc =
      /* intersection with founded */ pgno + npages > dl->items[n].pgno ||
      /* intersection with prev */ dpl_endpgno(dl, n - 1) > pgno;
  if (ASSERT_ENABLED()) {
    bool check = false;
    for (size_t i = 1; i <= dl->length; ++i) {
      const MDBX_page *const dp = dl->items[i].ptr;
      if (!(dp->mp_pgno /* begin */ >= /* end */ pgno + npages ||
            dpl_endpgno(dl, i) /* end */ <= /* begin */ pgno))
        check |= true;
    }
    assert(check == rc);
  }
  return rc;
}

MDBX_NOTHROW_PURE_FUNCTION static __always_inline size_t
dpl_exist(const MDBX_txn *txn, pgno_t pgno) {
  tASSERT(txn, (txn->mt_flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);
  MDBX_dpl *dl = txn->tw.dirtylist;
  size_t i = dpl_search(txn, pgno);
  assert((int)i > 0);
  return (dl->items[i].pgno == pgno) ? i : 0;
}

MDBX_MAYBE_UNUSED static const MDBX_page *debug_dpl_find(const MDBX_txn *txn,
                                                         const pgno_t pgno) {
  tASSERT(txn, (txn->mt_flags & MDBX_TXN_RDONLY) == 0);
  const MDBX_dpl *dl = txn->tw.dirtylist;
  if (dl) {
    tASSERT(txn, (txn->mt_flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);
    assert(dl->items[0].pgno == 0 &&
           dl->items[dl->length + 1].pgno == P_INVALID);
    for (size_t i = dl->length; i > dl->sorted; --i)
      if (dl->items[i].pgno == pgno)
        return dl->items[i].ptr;

    if (dl->sorted) {
      const size_t i = dp_bsearch(dl->items + 1, dl->sorted, pgno) - dl->items;
      if (dl->items[i].pgno == pgno)
        return dl->items[i].ptr;
    }
  } else {
    tASSERT(txn, (txn->mt_flags & MDBX_WRITEMAP) != 0 && !MDBX_AVOID_MSYNC);
  }
  return nullptr;
}

static void dpl_remove_ex(const MDBX_txn *txn, size_t i, size_t npages) {
  tASSERT(txn, (txn->mt_flags & MDBX_TXN_RDONLY) == 0);
  tASSERT(txn, (txn->mt_flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);

  MDBX_dpl *dl = txn->tw.dirtylist;
  assert((intptr_t)i > 0 && i <= dl->length);
  assert(dl->items[0].pgno == 0 && dl->items[dl->length + 1].pgno == P_INVALID);
  dl->pages_including_loose -= npages;
  dl->sorted -= dl->sorted >= i;
  dl->length -= 1;
  memmove(dl->items + i, dl->items + i + 1,
          (dl->length - i + 2) * sizeof(dl->items[0]));
  assert(dl->items[0].pgno == 0 && dl->items[dl->length + 1].pgno == P_INVALID);
}

static void dpl_remove(const MDBX_txn *txn, size_t i) {
  dpl_remove_ex(txn, i, dpl_npages(txn->tw.dirtylist, i));
}

static __noinline void txn_lru_reduce(MDBX_txn *txn) {
  NOTICE("lru-reduce %u -> %u", txn->tw.dirtylru, txn->tw.dirtylru >> 1);
  tASSERT(txn, (txn->mt_flags & (MDBX_TXN_RDONLY | MDBX_WRITEMAP)) == 0);
  do {
    txn->tw.dirtylru >>= 1;
    MDBX_dpl *dl = txn->tw.dirtylist;
    for (size_t i = 1; i <= dl->length; ++i) {
      size_t *const ptr =
          ptr_disp(dl->items[i].ptr, -(ptrdiff_t)sizeof(size_t));
      *ptr >>= 1;
    }
    txn = txn->mt_parent;
  } while (txn);
}

MDBX_NOTHROW_PURE_FUNCTION static __inline uint32_t dpl_age(const MDBX_txn *txn,
                                                            size_t i) {
  tASSERT(txn, (txn->mt_flags & (MDBX_TXN_RDONLY | MDBX_WRITEMAP)) == 0);
  const MDBX_dpl *dl = txn->tw.dirtylist;
  assert((intptr_t)i > 0 && i <= dl->length);
  size_t *const ptr = ptr_disp(dl->items[i].ptr, -(ptrdiff_t)sizeof(size_t));
  return txn->tw.dirtylru - (uint32_t)*ptr;
}

static __inline uint32_t txn_lru_turn(MDBX_txn *txn) {
  txn->tw.dirtylru += 1;
  if (unlikely(txn->tw.dirtylru > UINT32_MAX / 3) &&
      (txn->mt_flags & MDBX_WRITEMAP) == 0)
    txn_lru_reduce(txn);
  return txn->tw.dirtylru;
}

static __always_inline int __must_check_result dpl_append(MDBX_txn *txn,
                                                          pgno_t pgno,
                                                          MDBX_page *page,
                                                          size_t npages) {
  tASSERT(txn, (txn->mt_flags & MDBX_TXN_RDONLY) == 0);
  tASSERT(txn, (txn->mt_flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);
  const MDBX_dp dp = {page, pgno, (pgno_t)npages};
  if ((txn->mt_flags & MDBX_WRITEMAP) == 0) {
    size_t *const ptr = ptr_disp(page, -(ptrdiff_t)sizeof(size_t));
    *ptr = txn->tw.dirtylru;
  }

  MDBX_dpl *dl = txn->tw.dirtylist;
  tASSERT(txn, dl->length <= MDBX_PGL_LIMIT + MDBX_PNL_GRANULATE);
  tASSERT(txn, dl->items[0].pgno == 0 &&
                   dl->items[dl->length + 1].pgno == P_INVALID);
  if (AUDIT_ENABLED()) {
    for (size_t i = dl->length; i > 0; --i) {
      assert(dl->items[i].pgno != dp.pgno);
      if (unlikely(dl->items[i].pgno == dp.pgno)) {
        ERROR("Page %u already exist in the DPL at %zu", dp.pgno, i);
        return MDBX_PROBLEM;
      }
    }
  }

  if (unlikely(dl->length == dl->detent)) {
    if (unlikely(dl->detent >= MDBX_PGL_LIMIT)) {
      ERROR("DPL is full (MDBX_PGL_LIMIT %zu)", MDBX_PGL_LIMIT);
      return MDBX_TXN_FULL;
    }
    const size_t size = (dl->detent < MDBX_PNL_INITIAL * 42)
                            ? dl->detent + dl->detent
                            : dl->detent + dl->detent / 2;
    dl = dpl_reserve(txn, size);
    if (unlikely(!dl))
      return MDBX_ENOMEM;
    tASSERT(txn, dl->length < dl->detent);
  }

  /* Сортировка нужна для быстрого поиска, используем несколько тактик:
   *  1) Сохраняем упорядоченность при естественной вставке в нужном порядке.
   *  2) Добавляем в не-сортированный хвост, который сортируем и сливаем
   *     с отсортированной головой по необходимости, а пока хвост короткий
   *     ищем в нём сканированием, избегая большой пересортировки.
   *  3) Если не-сортированный хвост короткий, а добавляемый элемент близок
   *     к концу отсортированной головы, то выгоднее сразу вставить элемент
   *     в нужное место.
   *
   * Алгоритмически:
   *  - добавлять в не-сортированный хвост следует только если вставка сильно
   *    дорогая, т.е. если целевая позиция элемента сильно далека от конца;
   *  - для быстрой проверки достаточно сравнить добавляемый элемент с отстоящим
   *    от конца на максимально-приемлемое расстояние;
   *  - если список короче, либо элемент в этой позиции меньше вставляемого,
   *    то следует перемещать элементы и вставлять в отсортированную голову;
   *  - если не-сортированный хвост длиннее, либо элемент в этой позиции больше,
   *    то следует добавлять в не-сортированный хвост. */

  dl->pages_including_loose += npages;
  MDBX_dp *i = dl->items + dl->length;

#define MDBX_DPL_INSERTION_THRESHOLD 42
  const ptrdiff_t pivot = (ptrdiff_t)dl->length - MDBX_DPL_INSERTION_THRESHOLD;
#if MDBX_HAVE_CMOV
  const pgno_t pivot_pgno =
      dl->items[(dl->length < MDBX_DPL_INSERTION_THRESHOLD)
                    ? 0
                    : dl->length - MDBX_DPL_INSERTION_THRESHOLD]
          .pgno;
#endif /* MDBX_HAVE_CMOV */

  /* copy the stub beyond the end */
  i[2] = i[1];
  dl->length += 1;

  if (likely(pivot <= (ptrdiff_t)dl->sorted) &&
#if MDBX_HAVE_CMOV
      pivot_pgno < dp.pgno) {
#else
      (pivot <= 0 || dl->items[pivot].pgno < dp.pgno)) {
#endif /* MDBX_HAVE_CMOV */
    dl->sorted += 1;

    /* сдвигаем несортированный хвост */
    while (i >= dl->items + dl->sorted) {
#if !defined(__GNUC__) /* пытаемся избежать вызова memmove() */
      i[1] = *i;
#elif MDBX_WORDBITS == 64 &&                                                   \
    (defined(__SIZEOF_INT128__) ||                                             \
     (defined(_INTEGRAL_MAX_BITS) && _INTEGRAL_MAX_BITS >= 128))
      STATIC_ASSERT(sizeof(MDBX_dp) == sizeof(__uint128_t));
      ((__uint128_t *)i)[1] = *(volatile __uint128_t *)i;
#else
    i[1].ptr = i->ptr;
    i[1].pgno = i->pgno;
    i[1].npages = i->npages;
#endif
      --i;
    }
    /* ищем нужную позицию сдвигая отсортированные элементы */
    while (i->pgno > pgno) {
      tASSERT(txn, i > dl->items);
      i[1] = *i;
      --i;
    }
    tASSERT(txn, i->pgno < dp.pgno);
  }

  i[1] = dp;
  assert(dl->items[0].pgno == 0 && dl->items[dl->length + 1].pgno == P_INVALID);
  assert(dl->sorted <= dl->length);
  return MDBX_SUCCESS;
}

/*----------------------------------------------------------------------------*/

uint8_t runtime_flags = MDBX_RUNTIME_FLAGS_INIT;
uint8_t loglevel = MDBX_LOG_FATAL;
MDBX_debug_func *debug_logger;

static __must_check_result __inline int page_retire(MDBX_cursor *mc,
                                                    MDBX_page *mp);

static int __must_check_result page_dirty(MDBX_txn *txn, MDBX_page *mp,
                                          size_t npages);
typedef struct page_result {
  MDBX_page *page;
  int err;
} pgr_t;

static txnid_t kick_longlived_readers(MDBX_env *env, const txnid_t laggard);

static pgr_t page_new(MDBX_cursor *mc, const unsigned flags);
static pgr_t page_new_large(MDBX_cursor *mc, const size_t npages);
static int page_touch(MDBX_cursor *mc);
static int cursor_touch(MDBX_cursor *const mc, const MDBX_val *key,
                        const MDBX_val *data);

#define MDBX_END_NAMES                                                         \
  {"committed", "empty-commit", "abort",          "reset",                     \
   "reset-tmp", "fail-begin",   "fail-beginchild"}
enum {
  /* txn_end operation number, for logging */
  MDBX_END_COMMITTED,
  MDBX_END_PURE_COMMIT,
  MDBX_END_ABORT,
  MDBX_END_RESET,
  MDBX_END_RESET_TMP,
  MDBX_END_FAIL_BEGIN,
  MDBX_END_FAIL_BEGINCHILD
};
#define MDBX_END_OPMASK 0x0F  /* mask for txn_end() operation number */
#define MDBX_END_UPDATE 0x10  /* update env state (DBIs) */
#define MDBX_END_FREE 0x20    /* free txn unless it is MDBX_env.me_txn0 */
#define MDBX_END_EOTDONE 0x40 /* txn's cursors already closed */
#define MDBX_END_SLOT 0x80    /* release any reader slot if MDBX_NOTLS */
static int txn_end(MDBX_txn *txn, const unsigned mode);

static __always_inline pgr_t page_get_inline(const uint16_t ILL,
                                             const MDBX_cursor *const mc,
                                             const pgno_t pgno,
                                             const txnid_t front);

static pgr_t page_get_any(const MDBX_cursor *const mc, const pgno_t pgno,
                          const txnid_t front) {
  return page_get_inline(P_ILL_BITS, mc, pgno, front);
}

__hot static pgr_t page_get_three(const MDBX_cursor *const mc,
                                  const pgno_t pgno, const txnid_t front) {
  return page_get_inline(P_ILL_BITS | P_OVERFLOW, mc, pgno, front);
}

static pgr_t page_get_large(const MDBX_cursor *const mc, const pgno_t pgno,
                            const txnid_t front) {
  return page_get_inline(P_ILL_BITS | P_BRANCH | P_LEAF | P_LEAF2, mc, pgno,
                         front);
}

static __always_inline int __must_check_result page_get(const MDBX_cursor *mc,
                                                        const pgno_t pgno,
                                                        MDBX_page **mp,
                                                        const txnid_t front) {
  pgr_t ret = page_get_three(mc, pgno, front);
  *mp = ret.page;
  return ret.err;
}

static int __must_check_result page_search_root(MDBX_cursor *mc,
                                                const MDBX_val *key, int flags);

#define MDBX_PS_MODIFY 1
#define MDBX_PS_ROOTONLY 2
#define MDBX_PS_FIRST 4
#define MDBX_PS_LAST 8
static int __must_check_result page_search(MDBX_cursor *mc, const MDBX_val *key,
                                           int flags);
static int __must_check_result page_merge(MDBX_cursor *csrc, MDBX_cursor *cdst);

#define MDBX_SPLIT_REPLACE MDBX_APPENDDUP /* newkey is not new */
static int __must_check_result page_split(MDBX_cursor *mc,
                                          const MDBX_val *const newkey,
                                          MDBX_val *const newdata,
                                          pgno_t newpgno, const unsigned naf);

static int coherency_timeout(uint64_t *timestamp, intptr_t pgno,
                             const MDBX_env *env);
static int __must_check_result validate_meta_copy(MDBX_env *env,
                                                  const MDBX_meta *meta,
                                                  MDBX_meta *dest);
static int __must_check_result override_meta(MDBX_env *env, size_t target,
                                             txnid_t txnid,
                                             const MDBX_meta *shape);
static int __must_check_result read_header(MDBX_env *env, MDBX_meta *meta,
                                           const int lck_exclusive,
                                           const mdbx_mode_t mode_bits);
static int __must_check_result sync_locked(MDBX_env *env, unsigned flags,
                                           MDBX_meta *const pending,
                                           meta_troika_t *const troika);
static int env_close(MDBX_env *env);

struct node_result {
  MDBX_node *node;
  bool exact;
};

static struct node_result node_search(MDBX_cursor *mc, const MDBX_val *key);

static int __must_check_result node_add_branch(MDBX_cursor *mc, size_t indx,
                                               const MDBX_val *key,
                                               pgno_t pgno);
static int __must_check_result node_add_leaf(MDBX_cursor *mc, size_t indx,
                                             const MDBX_val *key,
                                             MDBX_val *data, unsigned flags);
static int __must_check_result node_add_leaf2(MDBX_cursor *mc, size_t indx,
                                              const MDBX_val *key);

static void node_del(MDBX_cursor *mc, size_t ksize);
static MDBX_node *node_shrink(MDBX_page *mp, size_t indx, MDBX_node *node);
static int __must_check_result node_move(MDBX_cursor *csrc, MDBX_cursor *cdst,
                                         bool fromleft);
static int __must_check_result node_read(MDBX_cursor *mc, const MDBX_node *leaf,
                                         MDBX_val *data, const MDBX_page *mp);
static int __must_check_result rebalance(MDBX_cursor *mc);
static int __must_check_result update_key(MDBX_cursor *mc, const MDBX_val *key);

static void cursor_pop(MDBX_cursor *mc);
static int __must_check_result cursor_push(MDBX_cursor *mc, MDBX_page *mp);

static int __must_check_result audit_ex(MDBX_txn *txn, size_t retired_stored,
                                        bool dont_filter_gc);

static int __must_check_result page_check(const MDBX_cursor *const mc,
                                          const MDBX_page *const mp);
static int __must_check_result cursor_check(const MDBX_cursor *mc);
static int __must_check_result cursor_get(MDBX_cursor *mc, MDBX_val *key,
                                          MDBX_val *data, MDBX_cursor_op op);
static int __must_check_result cursor_put_checklen(MDBX_cursor *mc,
                                                   const MDBX_val *key,
                                                   MDBX_val *data,
                                                   unsigned flags);
static int __must_check_result cursor_put_nochecklen(MDBX_cursor *mc,
                                                     const MDBX_val *key,
                                                     MDBX_val *data,
                                                     unsigned flags);
static int __must_check_result cursor_check_updating(MDBX_cursor *mc);
static int __must_check_result cursor_del(MDBX_cursor *mc,
                                          MDBX_put_flags_t flags);
static int __must_check_result delete(MDBX_txn *txn, MDBX_dbi dbi,
                                      const MDBX_val *key, const MDBX_val *data,
                                      unsigned flags);
#define SIBLING_LEFT 0
#define SIBLING_RIGHT 2
static int __must_check_result cursor_sibling(MDBX_cursor *mc, int dir);
static int __must_check_result cursor_next(MDBX_cursor *mc, MDBX_val *key,
                                           MDBX_val *data, MDBX_cursor_op op);
static int __must_check_result cursor_prev(MDBX_cursor *mc, MDBX_val *key,
                                           MDBX_val *data, MDBX_cursor_op op);
struct cursor_set_result {
  int err;
  bool exact;
};

static struct cursor_set_result cursor_set(MDBX_cursor *mc, MDBX_val *key,
                                           MDBX_val *data, MDBX_cursor_op op);
static int __must_check_result cursor_first(MDBX_cursor *mc, MDBX_val *key,
                                            MDBX_val *data);
static int __must_check_result cursor_last(MDBX_cursor *mc, MDBX_val *key,
                                           MDBX_val *data);

static int __must_check_result cursor_init(MDBX_cursor *mc, const MDBX_txn *txn,
                                           size_t dbi);
static int __must_check_result cursor_xinit0(MDBX_cursor *mc);
static int __must_check_result cursor_xinit1(MDBX_cursor *mc, MDBX_node *node,
                                             const MDBX_page *mp);
static int __must_check_result cursor_xinit2(MDBX_cursor *mc,
                                             MDBX_xcursor *src_mx,
                                             bool new_dupdata);
static void cursor_copy(const MDBX_cursor *csrc, MDBX_cursor *cdst);

static int __must_check_result drop_tree(MDBX_cursor *mc,
                                         const bool may_have_subDBs);
static int __must_check_result fetch_sdb(MDBX_txn *txn, size_t dbi);
static int __must_check_result setup_dbx(MDBX_dbx *const dbx,
                                         const MDBX_db *const db,
                                         const unsigned pagesize);

static __inline MDBX_cmp_func *get_default_keycmp(MDBX_db_flags_t flags);
static __inline MDBX_cmp_func *get_default_datacmp(MDBX_db_flags_t flags);

__cold const char *mdbx_liberr2str(int errnum) {
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
      "MDBX_DBS_FULL: Too many DBI-handles (maxdbs reached)",
      "MDBX_READERS_FULL: Too many readers (maxreaders reached)",
      NULL /* MDBX_TLS_FULL (-30789): unused in MDBX */,
      "MDBX_TXN_FULL: Transaction has too many dirty pages,"
      " i.e transaction is too big",
      "MDBX_CURSOR_FULL: Cursor stack limit reachedn - this usually indicates"
      " corruption, i.e branch-pages loop",
      "MDBX_PAGE_FULL: Internal error - Page has no more space",
      "MDBX_UNABLE_EXTEND_MAPSIZE: Database engine was unable to extend"
      " mapping, e.g. since address space is unavailable or busy,"
      " or Operation system not supported such operations",
      "MDBX_INCOMPATIBLE: Environment or database is not compatible"
      " with the requested operation or the specified flags",
      "MDBX_BAD_RSLOT: Invalid reuse of reader locktable slot,"
      " e.g. read-transaction already run for current thread",
      "MDBX_BAD_TXN: Transaction is not valid for requested operation,"
      " e.g. had errored and be must aborted, has a child, or is invalid",
      "MDBX_BAD_VALSIZE: Invalid size or alignment of key or data"
      " for target database, either invalid subDB name",
      "MDBX_BAD_DBI: The specified DBI-handle is invalid"
      " or changed by another thread/transaction",
      "MDBX_PROBLEM: Unexpected internal error, transaction should be aborted",
      "MDBX_BUSY: Another write transaction is running,"
      " or environment is already used while opening with MDBX_EXCLUSIVE flag",
  };

  if (errnum >= MDBX_KEYEXIST && errnum <= MDBX_BUSY) {
    int i = errnum - MDBX_KEYEXIST;
    return tbl[i];
  }

  switch (errnum) {
  case MDBX_SUCCESS:
    return "MDBX_SUCCESS: Successful";
  case MDBX_EMULTIVAL:
    return "MDBX_EMULTIVAL: The specified key has"
           " more than one associated value";
  case MDBX_EBADSIGN:
    return "MDBX_EBADSIGN: Wrong signature of a runtime object(s),"
           " e.g. memory corruption or double-free";
  case MDBX_WANNA_RECOVERY:
    return "MDBX_WANNA_RECOVERY: Database should be recovered,"
           " but this could NOT be done automatically for now"
           " since it opened in read-only mode";
  case MDBX_EKEYMISMATCH:
    return "MDBX_EKEYMISMATCH: The given key value is mismatched to the"
           " current cursor position";
  case MDBX_TOO_LARGE:
    return "MDBX_TOO_LARGE: Database is too large for current system,"
           " e.g. could NOT be mapped into RAM";
  case MDBX_THREAD_MISMATCH:
    return "MDBX_THREAD_MISMATCH: A thread has attempted to use a not"
           " owned object, e.g. a transaction that started by another thread";
  case MDBX_TXN_OVERLAPPING:
    return "MDBX_TXN_OVERLAPPING: Overlapping read and write transactions for"
           " the current thread";
  case MDBX_DUPLICATED_CLK:
    return "MDBX_DUPLICATED_CLK: Alternative/Duplicate LCK-file is exists, "
           "please keep one and remove unused other";
  default:
    return NULL;
  }
}

__cold const char *mdbx_strerror_r(int errnum, char *buf, size_t buflen) {
  const char *msg = mdbx_liberr2str(errnum);
  if (!msg && buflen > 0 && buflen < INT_MAX) {
#if defined(_WIN32) || defined(_WIN64)
    DWORD size = FormatMessageA(
        FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, NULL,
        errnum, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), buf, (DWORD)buflen,
        NULL);
    while (size && buf[size - 1] <= ' ')
      --size;
    buf[size] = 0;
    return size ? buf : "FormatMessageA(FORMAT_MESSAGE_FROM_SYSTEM) failed";
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

__cold const char *mdbx_strerror(int errnum) {
#if defined(_WIN32) || defined(_WIN64)
  static char buf[1024];
  return mdbx_strerror_r(errnum, buf, sizeof(buf));
#else
  const char *msg = mdbx_liberr2str(errnum);
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
  const char *msg = mdbx_liberr2str(errnum);
  if (!msg && buflen > 0 && buflen < INT_MAX) {
    DWORD size = FormatMessageA(
        FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, NULL,
        errnum, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), buf, (DWORD)buflen,
        NULL);
    while (size && buf[size - 1] <= ' ')
      --size;
    buf[size] = 0;
    if (!size)
      msg = "FormatMessageA(FORMAT_MESSAGE_FROM_SYSTEM) failed";
    else if (!CharToOemBuffA(buf, buf, size))
      msg = "CharToOemBuffA() failed";
    else
      msg = buf;
  }
  return msg;
}

const char *mdbx_strerror_ANSI2OEM(int errnum) {
  static char buf[1024];
  return mdbx_strerror_r_ANSI2OEM(errnum, buf, sizeof(buf));
}
#endif /* Bit of madness for Windows */

__cold void debug_log_va(int level, const char *function, int line,
                         const char *fmt, va_list args) {
  if (debug_logger)
    debug_logger(level, function, line, fmt, args);
  else {
#if defined(_WIN32) || defined(_WIN64)
    if (IsDebuggerPresent()) {
      int prefix_len = 0;
      char *prefix = nullptr;
      if (function && line > 0)
        prefix_len = osal_asprintf(&prefix, "%s:%d ", function, line);
      else if (function)
        prefix_len = osal_asprintf(&prefix, "%s: ", function);
      else if (line > 0)
        prefix_len = osal_asprintf(&prefix, "%d: ", line);
      if (prefix_len > 0 && prefix) {
        OutputDebugStringA(prefix);
        osal_free(prefix);
      }
      char *msg = nullptr;
      int msg_len = osal_vasprintf(&msg, fmt, args);
      if (msg_len > 0 && msg) {
        OutputDebugStringA(msg);
        osal_free(msg);
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
}

__cold void debug_log(int level, const char *function, int line,
                      const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);
  debug_log_va(level, function, line, fmt, args);
  va_end(args);
}

/* Dump a val in ascii or hexadecimal. */
const char *mdbx_dump_val(const MDBX_val *val, char *const buf,
                          const size_t bufsize) {
  if (!val)
    return "<null>";
  if (!val->iov_len)
    return "<empty>";
  if (!buf || bufsize < 4)
    return nullptr;

  if (!val->iov_base) {
    int len = snprintf(buf, bufsize, "<nullptr.%zu>", val->iov_len);
    assert(len > 0 && (size_t)len < bufsize);
    (void)len;
    return buf;
  }

  bool is_ascii = true;
  const uint8_t *const data = val->iov_base;
  for (size_t i = 0; i < val->iov_len; i++)
    if (data[i] < ' ' || data[i] > '~') {
      is_ascii = false;
      break;
    }

  if (is_ascii) {
    int len =
        snprintf(buf, bufsize, "%.*s",
                 (val->iov_len > INT_MAX) ? INT_MAX : (int)val->iov_len, data);
    assert(len > 0 && (size_t)len < bufsize);
    (void)len;
  } else {
    char *const detent = buf + bufsize - 2;
    char *ptr = buf;
    *ptr++ = '<';
    for (size_t i = 0; i < val->iov_len && ptr < detent; i++) {
      const char hex[16] = {'0', '1', '2', '3', '4', '5', '6', '7',
                            '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
      *ptr++ = hex[data[i] >> 4];
      *ptr++ = hex[data[i] & 15];
    }
    if (ptr < detent)
      *ptr++ = '>';
    *ptr = '\0';
  }
  return buf;
}

/*------------------------------------------------------------------------------
 LY: debug stuff */

static const char *leafnode_type(MDBX_node *n) {
  static const char *const tp[2][2] = {{"", ": DB"},
                                       {": sub-page", ": sub-DB"}};
  return (node_flags(n) & F_BIGDATA)
             ? ": large page"
             : tp[!!(node_flags(n) & F_DUPDATA)][!!(node_flags(n) & F_SUBDATA)];
}

/* Display all the keys in the page. */
MDBX_MAYBE_UNUSED static void page_list(MDBX_page *mp) {
  pgno_t pgno = mp->mp_pgno;
  const char *type;
  MDBX_node *node;
  size_t i, nkeys, nsize, total = 0;
  MDBX_val key;
  DKBUF;

  switch (PAGETYPE_WHOLE(mp)) {
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
    VERBOSE("Overflow page %" PRIaPGNO " pages %u\n", pgno, mp->mp_pages);
    return;
  case P_META:
    VERBOSE("Meta-page %" PRIaPGNO " txnid %" PRIu64 "\n", pgno,
            unaligned_peek_u64(4, page_meta(mp)->mm_txnid_a));
    return;
  default:
    VERBOSE("Bad page %" PRIaPGNO " flags 0x%X\n", pgno, mp->mp_flags);
    return;
  }

  nkeys = page_numkeys(mp);
  VERBOSE("%s %" PRIaPGNO " numkeys %zu\n", type, pgno, nkeys);

  for (i = 0; i < nkeys; i++) {
    if (IS_LEAF2(mp)) { /* LEAF2 pages have no mp_ptrs[] or node headers */
      key.iov_len = nsize = mp->mp_leaf2_ksize;
      key.iov_base = page_leaf2key(mp, i, nsize);
      total += nsize;
      VERBOSE("key %zu: nsize %zu, %s\n", i, nsize, DKEY(&key));
      continue;
    }
    node = page_node(mp, i);
    key.iov_len = node_ks(node);
    key.iov_base = node->mn_data;
    nsize = NODESIZE + key.iov_len;
    if (IS_BRANCH(mp)) {
      VERBOSE("key %zu: page %" PRIaPGNO ", %s\n", i, node_pgno(node),
              DKEY(&key));
      total += nsize;
    } else {
      if (node_flags(node) & F_BIGDATA)
        nsize += sizeof(pgno_t);
      else
        nsize += node_ds(node);
      total += nsize;
      nsize += sizeof(indx_t);
      VERBOSE("key %zu: nsize %zu, %s%s\n", i, nsize, DKEY(&key),
              leafnode_type(node));
    }
    total = EVEN(total);
  }
  VERBOSE("Total: header %zu + contents %zu + unused %zu\n",
          IS_LEAF2(mp) ? PAGEHDRSZ : PAGEHDRSZ + mp->mp_lower, total,
          page_room(mp));
}

/*----------------------------------------------------------------------------*/

/* Check if there is an initialized xcursor, so XCURSOR_REFRESH() is proper */
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

MDBX_MAYBE_UNUSED static bool cursor_is_tracked(const MDBX_cursor *mc) {
  for (MDBX_cursor *scan = mc->mc_txn->mt_cursors[mc->mc_dbi]; scan;
       scan = scan->mc_next)
    if (mc == ((mc->mc_flags & C_SUB) ? &scan->mc_xcursor->mx_cursor : scan))
      return true;
  return false;
}

/* Perform act while tracking temporary cursor mn */
#define WITH_CURSOR_TRACKING(mn, act)                                          \
  do {                                                                         \
    cASSERT(&(mn),                                                             \
            mn.mc_txn->mt_cursors != NULL /* must be not rdonly txt */);       \
    cASSERT(&(mn), !cursor_is_tracked(&(mn)));                                 \
    MDBX_cursor mc_dummy;                                                      \
    MDBX_cursor **tracking_head = &(mn).mc_txn->mt_cursors[mn.mc_dbi];         \
    MDBX_cursor *tracked = &(mn);                                              \
    if ((mn).mc_flags & C_SUB) {                                               \
      mc_dummy.mc_flags = C_INITIALIZED;                                       \
      mc_dummy.mc_top = 0;                                                     \
      mc_dummy.mc_snum = 0;                                                    \
      mc_dummy.mc_xcursor = (MDBX_xcursor *)&(mn);                             \
      tracked = &mc_dummy;                                                     \
    }                                                                          \
    tracked->mc_next = *tracking_head;                                         \
    *tracking_head = tracked;                                                  \
    {                                                                          \
      act;                                                                     \
    }                                                                          \
    *tracking_head = tracked->mc_next;                                         \
  } while (0)

int mdbx_cmp(const MDBX_txn *txn, MDBX_dbi dbi, const MDBX_val *a,
             const MDBX_val *b) {
  eASSERT(NULL, txn->mt_signature == MDBX_MT_SIGNATURE);
  return txn->mt_dbxs[dbi].md_cmp(a, b);
}

int mdbx_dcmp(const MDBX_txn *txn, MDBX_dbi dbi, const MDBX_val *a,
              const MDBX_val *b) {
  eASSERT(NULL, txn->mt_signature == MDBX_MT_SIGNATURE);
  return txn->mt_dbxs[dbi].md_dcmp(a, b);
}

/* Allocate memory for a page.
 * Re-use old malloc'ed pages first for singletons, otherwise just malloc.
 * Set MDBX_TXN_ERROR on failure. */
static MDBX_page *page_malloc(MDBX_txn *txn, size_t num) {
  MDBX_env *env = txn->mt_env;
  MDBX_page *np = env->me_dp_reserve;
  size_t size = env->me_psize;
  if (likely(num == 1 && np)) {
    eASSERT(env, env->me_dp_reserve_len > 0);
    MDBX_ASAN_UNPOISON_MEMORY_REGION(np, size);
    VALGRIND_MEMPOOL_ALLOC(env, ptr_disp(np, -(ptrdiff_t)sizeof(size_t)),
                           size + sizeof(size_t));
    VALGRIND_MAKE_MEM_DEFINED(&mp_next(np), sizeof(MDBX_page *));
    env->me_dp_reserve = mp_next(np);
    env->me_dp_reserve_len -= 1;
  } else {
    size = pgno2bytes(env, num);
    void *const ptr = osal_malloc(size + sizeof(size_t));
    if (unlikely(!ptr)) {
      txn->mt_flags |= MDBX_TXN_ERROR;
      return nullptr;
    }
    VALGRIND_MEMPOOL_ALLOC(env, ptr, size + sizeof(size_t));
    np = ptr_disp(ptr, sizeof(size_t));
  }

  if ((env->me_flags & MDBX_NOMEMINIT) == 0) {
    /* For a single page alloc, we init everything after the page header.
     * For multi-page, we init the final page; if the caller needed that
     * many pages they will be filling in at least up to the last page. */
    size_t skip = PAGEHDRSZ;
    if (num > 1)
      skip += pgno2bytes(env, num - 1);
    memset(ptr_disp(np, skip), 0, size - skip);
  }
#if MDBX_DEBUG
  np->mp_pgno = 0;
#endif
  VALGRIND_MAKE_MEM_UNDEFINED(np, size);
  np->mp_flags = 0;
  np->mp_pages = (pgno_t)num;
  return np;
}

/* Free a shadow dirty page */
static void dpage_free(MDBX_env *env, MDBX_page *dp, size_t npages) {
  VALGRIND_MAKE_MEM_UNDEFINED(dp, pgno2bytes(env, npages));
  MDBX_ASAN_UNPOISON_MEMORY_REGION(dp, pgno2bytes(env, npages));
  if (unlikely(env->me_flags & MDBX_PAGEPERTURB))
    memset(dp, -1, pgno2bytes(env, npages));
  if (npages == 1 &&
      env->me_dp_reserve_len < env->me_options.dp_reserve_limit) {
    MDBX_ASAN_POISON_MEMORY_REGION(dp, env->me_psize);
    MDBX_ASAN_UNPOISON_MEMORY_REGION(&mp_next(dp), sizeof(MDBX_page *));
    mp_next(dp) = env->me_dp_reserve;
    VALGRIND_MEMPOOL_FREE(env, ptr_disp(dp, -(ptrdiff_t)sizeof(size_t)));
    env->me_dp_reserve = dp;
    env->me_dp_reserve_len += 1;
  } else {
    /* large pages just get freed directly */
    void *const ptr = ptr_disp(dp, -(ptrdiff_t)sizeof(size_t));
    VALGRIND_MEMPOOL_FREE(env, ptr);
    osal_free(ptr);
  }
}

/* Return all dirty pages to dpage list */
static void dlist_free(MDBX_txn *txn) {
  tASSERT(txn, (txn->mt_flags & (MDBX_TXN_RDONLY | MDBX_WRITEMAP)) == 0);
  MDBX_env *env = txn->mt_env;
  MDBX_dpl *const dl = txn->tw.dirtylist;

  for (size_t i = 1; i <= dl->length; i++)
    dpage_free(env, dl->items[i].ptr, dpl_npages(dl, i));

  dpl_clear(dl);
}

static __always_inline MDBX_db *outer_db(MDBX_cursor *mc) {
  cASSERT(mc, (mc->mc_flags & C_SUB) != 0);
  MDBX_xcursor *mx = container_of(mc->mc_db, MDBX_xcursor, mx_db);
  MDBX_cursor_couple *couple = container_of(mx, MDBX_cursor_couple, inner);
  cASSERT(mc, mc->mc_db == &couple->outer.mc_xcursor->mx_db);
  cASSERT(mc, mc->mc_dbx == &couple->outer.mc_xcursor->mx_dbx);
  return couple->outer.mc_db;
}

MDBX_MAYBE_UNUSED __cold static bool dirtylist_check(MDBX_txn *txn) {
  tASSERT(txn, (txn->mt_flags & MDBX_TXN_RDONLY) == 0);
  const MDBX_dpl *const dl = txn->tw.dirtylist;
  if (!dl) {
    tASSERT(txn, (txn->mt_flags & MDBX_WRITEMAP) != 0 && !MDBX_AVOID_MSYNC);
    return true;
  }
  tASSERT(txn, (txn->mt_flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);

  assert(dl->items[0].pgno == 0 && dl->items[dl->length + 1].pgno == P_INVALID);
  tASSERT(txn, txn->tw.dirtyroom + dl->length ==
                   (txn->mt_parent ? txn->mt_parent->tw.dirtyroom
                                   : txn->mt_env->me_options.dp_limit));

  if (!AUDIT_ENABLED())
    return true;

  size_t loose = 0, pages = 0;
  for (size_t i = dl->length; i > 0; --i) {
    const MDBX_page *const dp = dl->items[i].ptr;
    if (!dp)
      continue;

    tASSERT(txn, dp->mp_pgno == dl->items[i].pgno);
    if (unlikely(dp->mp_pgno != dl->items[i].pgno))
      return false;

    if ((txn->mt_flags & MDBX_WRITEMAP) == 0) {
      const uint32_t age = dpl_age(txn, i);
      tASSERT(txn, age < UINT32_MAX / 3);
      if (unlikely(age > UINT32_MAX / 3))
        return false;
    }

    tASSERT(txn, dp->mp_flags == P_LOOSE || IS_MODIFIABLE(txn, dp));
    if (dp->mp_flags == P_LOOSE) {
      loose += 1;
    } else if (unlikely(!IS_MODIFIABLE(txn, dp)))
      return false;

    const unsigned num = dpl_npages(dl, i);
    pages += num;
    tASSERT(txn, txn->mt_next_pgno >= dp->mp_pgno + num);
    if (unlikely(txn->mt_next_pgno < dp->mp_pgno + num))
      return false;

    if (i < dl->sorted) {
      tASSERT(txn, dl->items[i + 1].pgno >= dp->mp_pgno + num);
      if (unlikely(dl->items[i + 1].pgno < dp->mp_pgno + num))
        return false;
    }

    const size_t rpa =
        pnl_search(txn->tw.relist, dp->mp_pgno, txn->mt_next_pgno);
    tASSERT(txn, rpa > MDBX_PNL_GETSIZE(txn->tw.relist) ||
                     txn->tw.relist[rpa] != dp->mp_pgno);
    if (rpa <= MDBX_PNL_GETSIZE(txn->tw.relist) &&
        unlikely(txn->tw.relist[rpa] == dp->mp_pgno))
      return false;
    if (num > 1) {
      const size_t rpb =
          pnl_search(txn->tw.relist, dp->mp_pgno + num - 1, txn->mt_next_pgno);
      tASSERT(txn, rpa == rpb);
      if (unlikely(rpa != rpb))
        return false;
    }
  }

  tASSERT(txn, loose == txn->tw.loose_count);
  if (unlikely(loose != txn->tw.loose_count))
    return false;

  tASSERT(txn, pages == dl->pages_including_loose);
  if (unlikely(pages != dl->pages_including_loose))
    return false;

  for (size_t i = 1; i <= MDBX_PNL_GETSIZE(txn->tw.retired_pages); ++i) {
    const MDBX_page *const dp = debug_dpl_find(txn, txn->tw.retired_pages[i]);
    tASSERT(txn, !dp);
    if (unlikely(dp))
      return false;
  }

  return true;
}

#if MDBX_ENABLE_REFUND
static void refund_reclaimed(MDBX_txn *txn) {
  /* Scanning in descend order */
  pgno_t next_pgno = txn->mt_next_pgno;
  const MDBX_PNL pnl = txn->tw.relist;
  tASSERT(txn, MDBX_PNL_GETSIZE(pnl) && MDBX_PNL_MOST(pnl) == next_pgno - 1);
#if MDBX_PNL_ASCENDING
  size_t i = MDBX_PNL_GETSIZE(pnl);
  tASSERT(txn, pnl[i] == next_pgno - 1);
  while (--next_pgno, --i > 0 && pnl[i] == next_pgno - 1)
    ;
  MDBX_PNL_SETSIZE(pnl, i);
#else
  size_t i = 1;
  tASSERT(txn, pnl[i] == next_pgno - 1);
  size_t len = MDBX_PNL_GETSIZE(pnl);
  while (--next_pgno, ++i <= len && pnl[i] == next_pgno - 1)
    ;
  MDBX_PNL_SETSIZE(pnl, len -= i - 1);
  for (size_t move = 0; move < len; ++move)
    pnl[1 + move] = pnl[i + move];
#endif
  VERBOSE("refunded %" PRIaPGNO " pages: %" PRIaPGNO " -> %" PRIaPGNO,
          txn->mt_next_pgno - next_pgno, txn->mt_next_pgno, next_pgno);
  txn->mt_next_pgno = next_pgno;
  tASSERT(txn, pnl_check_allocated(txn->tw.relist, txn->mt_next_pgno - 1));
}

static void refund_loose(MDBX_txn *txn) {
  tASSERT(txn, txn->tw.loose_pages != nullptr);
  tASSERT(txn, txn->tw.loose_count > 0);

  MDBX_dpl *const dl = txn->tw.dirtylist;
  if (dl) {
    tASSERT(txn, dl->length >= txn->tw.loose_count);
    tASSERT(txn, (txn->mt_flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);
  } else {
    tASSERT(txn, (txn->mt_flags & MDBX_WRITEMAP) != 0 && !MDBX_AVOID_MSYNC);
  }

  pgno_t onstack[MDBX_CACHELINE_SIZE * 8 / sizeof(pgno_t)];
  MDBX_PNL suitable = onstack;

  if (!dl || dl->length - dl->sorted > txn->tw.loose_count) {
    /* Dirty list is useless since unsorted. */
    if (pnl_bytes2size(sizeof(onstack)) < txn->tw.loose_count) {
      suitable = pnl_alloc(txn->tw.loose_count);
      if (unlikely(!suitable))
        return /* this is not a reason for transaction fail */;
    }

    /* Collect loose-pages which may be refunded. */
    tASSERT(txn, txn->mt_next_pgno >= MIN_PAGENO + txn->tw.loose_count);
    pgno_t most = MIN_PAGENO;
    size_t w = 0;
    for (const MDBX_page *lp = txn->tw.loose_pages; lp; lp = mp_next(lp)) {
      tASSERT(txn, lp->mp_flags == P_LOOSE);
      tASSERT(txn, txn->mt_next_pgno > lp->mp_pgno);
      if (likely(txn->mt_next_pgno - txn->tw.loose_count <= lp->mp_pgno)) {
        tASSERT(txn,
                w < ((suitable == onstack) ? pnl_bytes2size(sizeof(onstack))
                                           : MDBX_PNL_ALLOCLEN(suitable)));
        suitable[++w] = lp->mp_pgno;
        most = (lp->mp_pgno > most) ? lp->mp_pgno : most;
      }
      MDBX_ASAN_UNPOISON_MEMORY_REGION(&mp_next(lp), sizeof(MDBX_page *));
      VALGRIND_MAKE_MEM_DEFINED(&mp_next(lp), sizeof(MDBX_page *));
    }

    if (most + 1 == txn->mt_next_pgno) {
      /* Sort suitable list and refund pages at the tail. */
      MDBX_PNL_SETSIZE(suitable, w);
      pnl_sort(suitable, MAX_PAGENO + 1);

      /* Scanning in descend order */
      const intptr_t step = MDBX_PNL_ASCENDING ? -1 : 1;
      const intptr_t begin =
          MDBX_PNL_ASCENDING ? MDBX_PNL_GETSIZE(suitable) : 1;
      const intptr_t end =
          MDBX_PNL_ASCENDING ? 0 : MDBX_PNL_GETSIZE(suitable) + 1;
      tASSERT(txn, suitable[begin] >= suitable[end - step]);
      tASSERT(txn, most == suitable[begin]);

      for (intptr_t i = begin + step; i != end; i += step) {
        if (suitable[i] != most - 1)
          break;
        most -= 1;
      }
      const size_t refunded = txn->mt_next_pgno - most;
      DEBUG("refund-suitable %zu pages %" PRIaPGNO " -> %" PRIaPGNO, refunded,
            most, txn->mt_next_pgno);
      txn->mt_next_pgno = most;
      txn->tw.loose_count -= refunded;
      if (dl) {
        txn->tw.dirtyroom += refunded;
        dl->pages_including_loose -= refunded;
        assert(txn->tw.dirtyroom <= txn->mt_env->me_options.dp_limit);

        /* Filter-out dirty list */
        size_t r = 0;
        w = 0;
        if (dl->sorted) {
          do {
            if (dl->items[++r].pgno < most) {
              if (++w != r)
                dl->items[w] = dl->items[r];
            }
          } while (r < dl->sorted);
          dl->sorted = w;
        }
        while (r < dl->length) {
          if (dl->items[++r].pgno < most) {
            if (++w != r)
              dl->items[w] = dl->items[r];
          }
        }
        dpl_setlen(dl, w);
        tASSERT(txn, txn->tw.dirtyroom + txn->tw.dirtylist->length ==
                         (txn->mt_parent ? txn->mt_parent->tw.dirtyroom
                                         : txn->mt_env->me_options.dp_limit));
      }
      goto unlink_loose;
    }
  } else {
    /* Dirtylist is mostly sorted, just refund loose pages at the end. */
    dpl_sort(txn);
    tASSERT(txn,
            dl->length < 2 || dl->items[1].pgno < dl->items[dl->length].pgno);
    tASSERT(txn, dl->sorted == dl->length);

    /* Scan dirtylist tail-forward and cutoff suitable pages. */
    size_t n;
    for (n = dl->length; dl->items[n].pgno == txn->mt_next_pgno - 1 &&
                         dl->items[n].ptr->mp_flags == P_LOOSE;
         --n) {
      tASSERT(txn, n > 0);
      MDBX_page *dp = dl->items[n].ptr;
      DEBUG("refund-sorted page %" PRIaPGNO, dp->mp_pgno);
      tASSERT(txn, dp->mp_pgno == dl->items[n].pgno);
      txn->mt_next_pgno -= 1;
    }
    dpl_setlen(dl, n);

    if (dl->sorted != dl->length) {
      const size_t refunded = dl->sorted - dl->length;
      dl->sorted = dl->length;
      txn->tw.loose_count -= refunded;
      txn->tw.dirtyroom += refunded;
      dl->pages_including_loose -= refunded;
      tASSERT(txn, txn->tw.dirtyroom + txn->tw.dirtylist->length ==
                       (txn->mt_parent ? txn->mt_parent->tw.dirtyroom
                                       : txn->mt_env->me_options.dp_limit));

      /* Filter-out loose chain & dispose refunded pages. */
    unlink_loose:
      for (MDBX_page **link = &txn->tw.loose_pages; *link;) {
        MDBX_page *dp = *link;
        tASSERT(txn, dp->mp_flags == P_LOOSE);
        MDBX_ASAN_UNPOISON_MEMORY_REGION(&mp_next(dp), sizeof(MDBX_page *));
        VALGRIND_MAKE_MEM_DEFINED(&mp_next(dp), sizeof(MDBX_page *));
        if (txn->mt_next_pgno > dp->mp_pgno) {
          link = &mp_next(dp);
        } else {
          *link = mp_next(dp);
          if ((txn->mt_flags & MDBX_WRITEMAP) == 0)
            dpage_free(txn->mt_env, dp, 1);
        }
      }
    }
  }

  tASSERT(txn, dirtylist_check(txn));
  if (suitable != onstack)
    pnl_free(suitable);
  txn->tw.loose_refund_wl = txn->mt_next_pgno;
}

static bool txn_refund(MDBX_txn *txn) {
  const pgno_t before = txn->mt_next_pgno;

  if (txn->tw.loose_pages && txn->tw.loose_refund_wl > txn->mt_next_pgno)
    refund_loose(txn);

  while (true) {
    if (MDBX_PNL_GETSIZE(txn->tw.relist) == 0 ||
        MDBX_PNL_MOST(txn->tw.relist) != txn->mt_next_pgno - 1)
      break;

    refund_reclaimed(txn);
    if (!txn->tw.loose_pages || txn->tw.loose_refund_wl <= txn->mt_next_pgno)
      break;

    const pgno_t memo = txn->mt_next_pgno;
    refund_loose(txn);
    if (memo == txn->mt_next_pgno)
      break;
  }

  if (before == txn->mt_next_pgno)
    return false;

  if (txn->tw.spilled.list)
    /* Squash deleted pagenums if we refunded any */
    spill_purge(txn);

  return true;
}
#else  /* MDBX_ENABLE_REFUND */
static __inline bool txn_refund(MDBX_txn *txn) {
  (void)txn;
  /* No online auto-compactification. */
  return false;
}
#endif /* MDBX_ENABLE_REFUND */

__cold static void kill_page(MDBX_txn *txn, MDBX_page *mp, pgno_t pgno,
                             size_t npages) {
  MDBX_env *const env = txn->mt_env;
  DEBUG("kill %zu page(s) %" PRIaPGNO, npages, pgno);
  eASSERT(env, pgno >= NUM_METAS && npages);
  if (!IS_FROZEN(txn, mp)) {
    const size_t bytes = pgno2bytes(env, npages);
    memset(mp, -1, bytes);
    mp->mp_pgno = pgno;
    if ((txn->mt_flags & MDBX_WRITEMAP) == 0)
      osal_pwrite(env->me_lazy_fd, mp, bytes, pgno2bytes(env, pgno));
  } else {
    struct iovec iov[MDBX_AUXILARY_IOV_MAX];
    iov[0].iov_len = env->me_psize;
    iov[0].iov_base = ptr_disp(env->me_pbuf, env->me_psize);
    size_t iov_off = pgno2bytes(env, pgno), n = 1;
    while (--npages) {
      iov[n] = iov[0];
      if (++n == MDBX_AUXILARY_IOV_MAX) {
        osal_pwritev(env->me_lazy_fd, iov, MDBX_AUXILARY_IOV_MAX, iov_off);
        iov_off += pgno2bytes(env, MDBX_AUXILARY_IOV_MAX);
        n = 0;
      }
    }
    osal_pwritev(env->me_lazy_fd, iov, n, iov_off);
  }
}

/* Remove page from dirty list, etc */
static __inline void page_wash(MDBX_txn *txn, size_t di, MDBX_page *const mp,
                               const size_t npages) {
  tASSERT(txn, (txn->mt_flags & MDBX_TXN_RDONLY) == 0);
  mp->mp_txnid = INVALID_TXNID;
  mp->mp_flags = P_BAD;

  if (txn->tw.dirtylist) {
    tASSERT(txn, (txn->mt_flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);
    tASSERT(txn,
            MDBX_AVOID_MSYNC || (di && txn->tw.dirtylist->items[di].ptr == mp));
    if (!MDBX_AVOID_MSYNC || di) {
      dpl_remove_ex(txn, di, npages);
      txn->tw.dirtyroom++;
      tASSERT(txn, txn->tw.dirtyroom + txn->tw.dirtylist->length ==
                       (txn->mt_parent ? txn->mt_parent->tw.dirtyroom
                                       : txn->mt_env->me_options.dp_limit));
      if (!MDBX_AVOID_MSYNC || !(txn->mt_flags & MDBX_WRITEMAP)) {
        dpage_free(txn->mt_env, mp, npages);
        return;
      }
    }
  } else {
    tASSERT(txn, (txn->mt_flags & MDBX_WRITEMAP) && !MDBX_AVOID_MSYNC && !di);
    txn->tw.writemap_dirty_npages -= (txn->tw.writemap_dirty_npages > npages)
                                         ? npages
                                         : txn->tw.writemap_dirty_npages;
  }
  VALGRIND_MAKE_MEM_UNDEFINED(mp, PAGEHDRSZ);
  VALGRIND_MAKE_MEM_NOACCESS(page_data(mp),
                             pgno2bytes(txn->mt_env, npages) - PAGEHDRSZ);
  MDBX_ASAN_POISON_MEMORY_REGION(page_data(mp),
                                 pgno2bytes(txn->mt_env, npages) - PAGEHDRSZ);
}

static __inline bool suitable4loose(const MDBX_txn *txn, pgno_t pgno) {
  /* TODO:
   *  1) при включенной "экономии последовательностей" проверить, что
   *     страница не примыкает к какой-либо из уже находящийся в reclaimed.
   *  2) стоит подумать над тем, чтобы при большом loose-списке отбрасывать
         половину в reclaimed. */
  return txn->tw.loose_count < txn->mt_env->me_options.dp_loose_limit &&
         (!MDBX_ENABLE_REFUND ||
          /* skip pages near to the end in favor of compactification */
          txn->mt_next_pgno > pgno + txn->mt_env->me_options.dp_loose_limit ||
          txn->mt_next_pgno <= txn->mt_env->me_options.dp_loose_limit);
}

/* Retire, loosen or free a single page.
 *
 * For dirty pages, saves single pages to a list for future reuse in this same
 * txn. It has been pulled from the GC and already resides on the dirty list,
 * but has been deleted. Use these pages first before pulling again from the GC.
 *
 * If the page wasn't dirtied in this txn, just add it
 * to this txn's free list. */
static int page_retire_ex(MDBX_cursor *mc, const pgno_t pgno,
                          MDBX_page *mp /* maybe null */,
                          unsigned pageflags /* maybe unknown/zero */) {
  int rc;
  MDBX_txn *const txn = mc->mc_txn;
  tASSERT(txn, !mp || (mp->mp_pgno == pgno && mp->mp_flags == pageflags));

  /* During deleting entire subtrees, it is reasonable and possible to avoid
   * reading leaf pages, i.e. significantly reduce hard page-faults & IOPs:
   *  - mp is null, i.e. the page has not yet been read;
   *  - pagetype is known and the P_LEAF bit is set;
   *  - we can determine the page status via scanning the lists
   *    of dirty and spilled pages.
   *
   *  On the other hand, this could be suboptimal for WRITEMAP mode, since
   *  requires support the list of dirty pages and avoid explicit spilling.
   *  So for flexibility and avoid extra internal dependencies we just
   *  fallback to reading if dirty list was not allocated yet. */
  size_t di = 0, si = 0, npages = 1;
  enum page_status {
    unknown,
    frozen,
    spilled,
    shadowed,
    modifable
  } status = unknown;

  if (unlikely(!mp)) {
    if (ASSERT_ENABLED() && pageflags) {
      pgr_t check;
      check = page_get_any(mc, pgno, txn->mt_front);
      if (unlikely(check.err != MDBX_SUCCESS))
        return check.err;
      tASSERT(txn,
              (check.page->mp_flags & ~P_SPILLED) == (pageflags & ~P_FROZEN));
      tASSERT(txn, !(pageflags & P_FROZEN) || IS_FROZEN(txn, check.page));
    }
    if (pageflags & P_FROZEN) {
      status = frozen;
      if (ASSERT_ENABLED()) {
        for (MDBX_txn *scan = txn; scan; scan = scan->mt_parent) {
          tASSERT(txn, !txn->tw.spilled.list || !search_spilled(scan, pgno));
          tASSERT(txn, !scan->tw.dirtylist || !debug_dpl_find(scan, pgno));
        }
      }
      goto status_done;
    } else if (pageflags && txn->tw.dirtylist) {
      if ((di = dpl_exist(txn, pgno)) != 0) {
        mp = txn->tw.dirtylist->items[di].ptr;
        tASSERT(txn, IS_MODIFIABLE(txn, mp));
        status = modifable;
        goto status_done;
      }
      if ((si = search_spilled(txn, pgno)) != 0) {
        status = spilled;
        goto status_done;
      }
      for (MDBX_txn *parent = txn->mt_parent; parent;
           parent = parent->mt_parent) {
        if (dpl_exist(parent, pgno)) {
          status = shadowed;
          goto status_done;
        }
        if (search_spilled(parent, pgno)) {
          status = spilled;
          goto status_done;
        }
      }
      status = frozen;
      goto status_done;
    }

    pgr_t pg = page_get_any(mc, pgno, txn->mt_front);
    if (unlikely(pg.err != MDBX_SUCCESS))
      return pg.err;
    mp = pg.page;
    tASSERT(txn, !pageflags || mp->mp_flags == pageflags);
    pageflags = mp->mp_flags;
  }

  if (IS_FROZEN(txn, mp)) {
    status = frozen;
    tASSERT(txn, !IS_MODIFIABLE(txn, mp));
    tASSERT(txn, !IS_SPILLED(txn, mp));
    tASSERT(txn, !IS_SHADOWED(txn, mp));
    tASSERT(txn, !debug_dpl_find(txn, pgno));
    tASSERT(txn, !txn->tw.spilled.list || !search_spilled(txn, pgno));
  } else if (IS_MODIFIABLE(txn, mp)) {
    status = modifable;
    if (txn->tw.dirtylist)
      di = dpl_exist(txn, pgno);
    tASSERT(txn, (txn->mt_flags & MDBX_WRITEMAP) || !IS_SPILLED(txn, mp));
    tASSERT(txn, !txn->tw.spilled.list || !search_spilled(txn, pgno));
  } else if (IS_SHADOWED(txn, mp)) {
    status = shadowed;
    tASSERT(txn, !txn->tw.spilled.list || !search_spilled(txn, pgno));
    tASSERT(txn, !debug_dpl_find(txn, pgno));
  } else {
    tASSERT(txn, IS_SPILLED(txn, mp));
    status = spilled;
    si = search_spilled(txn, pgno);
    tASSERT(txn, !debug_dpl_find(txn, pgno));
  }

status_done:
  if (likely((pageflags & P_OVERFLOW) == 0)) {
    STATIC_ASSERT(P_BRANCH == 1);
    const bool is_branch = pageflags & P_BRANCH;
    if (unlikely(mc->mc_flags & C_SUB)) {
      MDBX_db *outer = outer_db(mc);
      cASSERT(mc, !is_branch || outer->md_branch_pages > 0);
      outer->md_branch_pages -= is_branch;
      cASSERT(mc, is_branch || outer->md_leaf_pages > 0);
      outer->md_leaf_pages -= 1 - is_branch;
    }
    cASSERT(mc, !is_branch || mc->mc_db->md_branch_pages > 0);
    mc->mc_db->md_branch_pages -= is_branch;
    cASSERT(mc, (pageflags & P_LEAF) == 0 || mc->mc_db->md_leaf_pages > 0);
    mc->mc_db->md_leaf_pages -= (pageflags & P_LEAF) != 0;
  } else {
    npages = mp->mp_pages;
    cASSERT(mc, mc->mc_db->md_overflow_pages >= npages);
    mc->mc_db->md_overflow_pages -= (pgno_t)npages;
  }

  if (status == frozen) {
  retire:
    DEBUG("retire %zu page %" PRIaPGNO, npages, pgno);
    rc = pnl_append_range(false, &txn->tw.retired_pages, pgno, npages);
    tASSERT(txn, dirtylist_check(txn));
    return rc;
  }

  /* Возврат страниц в нераспределенный "хвост" БД.
   * Содержимое страниц не уничтожается, а для вложенных транзакций граница
   * нераспределенного "хвоста" БД сдвигается только при их коммите. */
  if (MDBX_ENABLE_REFUND && unlikely(pgno + npages == txn->mt_next_pgno)) {
    const char *kind = nullptr;
    if (status == modifable) {
      /* Страница испачкана в этой транзакции, но до этого могла быть
       * аллоцирована, испачкана и пролита в одной из родительских транзакций.
       * Её МОЖНО вытолкнуть в нераспределенный хвост. */
      kind = "dirty";
      /* Remove from dirty list */
      page_wash(txn, di, mp, npages);
    } else if (si) {
      /* Страница пролита в этой транзакции, т.е. она аллоцирована
       * и запачкана в этой или одной из родительских транзакций.
       * Её МОЖНО вытолкнуть в нераспределенный хвост. */
      kind = "spilled";
      tASSERT(txn, status == spilled);
      spill_remove(txn, si, npages);
    } else {
      /* Страница аллоцирована, запачкана и возможно пролита в одной
       * из родительских транзакций.
       * Её МОЖНО вытолкнуть в нераспределенный хвост. */
      kind = "parent's";
      if (ASSERT_ENABLED() && mp) {
        kind = nullptr;
        for (MDBX_txn *parent = txn->mt_parent; parent;
             parent = parent->mt_parent) {
          if (search_spilled(parent, pgno)) {
            kind = "parent-spilled";
            tASSERT(txn, status == spilled);
            break;
          }
          if (mp == debug_dpl_find(parent, pgno)) {
            kind = "parent-dirty";
            tASSERT(txn, status == shadowed);
            break;
          }
        }
        tASSERT(txn, kind != nullptr);
      }
      tASSERT(txn, status == spilled || status == shadowed);
    }
    DEBUG("refunded %zu %s page %" PRIaPGNO, npages, kind, pgno);
    txn->mt_next_pgno = pgno;
    txn_refund(txn);
    return MDBX_SUCCESS;
  }

  if (status == modifable) {
    /* Dirty page from this transaction */
    /* If suitable we can reuse it through loose list */
    if (likely(npages == 1 && suitable4loose(txn, pgno)) &&
        (di || !txn->tw.dirtylist)) {
      DEBUG("loosen dirty page %" PRIaPGNO, pgno);
      if (MDBX_DEBUG != 0 || unlikely(txn->mt_env->me_flags & MDBX_PAGEPERTURB))
        memset(page_data(mp), -1, txn->mt_env->me_psize - PAGEHDRSZ);
      mp->mp_txnid = INVALID_TXNID;
      mp->mp_flags = P_LOOSE;
      mp_next(mp) = txn->tw.loose_pages;
      txn->tw.loose_pages = mp;
      txn->tw.loose_count++;
#if MDBX_ENABLE_REFUND
      txn->tw.loose_refund_wl = (pgno + 2 > txn->tw.loose_refund_wl)
                                    ? pgno + 2
                                    : txn->tw.loose_refund_wl;
#endif /* MDBX_ENABLE_REFUND */
      VALGRIND_MAKE_MEM_NOACCESS(page_data(mp),
                                 txn->mt_env->me_psize - PAGEHDRSZ);
      MDBX_ASAN_POISON_MEMORY_REGION(page_data(mp),
                                     txn->mt_env->me_psize - PAGEHDRSZ);
      return MDBX_SUCCESS;
    }

#if !MDBX_DEBUG && !defined(MDBX_USE_VALGRIND) && !defined(__SANITIZE_ADDRESS__)
    if (unlikely(txn->mt_env->me_flags & MDBX_PAGEPERTURB))
#endif
    {
      /* Страница могла быть изменена в одной из родительских транзакций,
       * в том числе, позже выгружена и затем снова загружена и изменена.
       * В обоих случаях её нельзя затирать на диске и помечать недоступной
       * в asan и/или valgrind */
      for (MDBX_txn *parent = txn->mt_parent;
           parent && (parent->mt_flags & MDBX_TXN_SPILLS);
           parent = parent->mt_parent) {
        if (intersect_spilled(parent, pgno, npages))
          goto skip_invalidate;
        if (dpl_intersect(parent, pgno, npages))
          goto skip_invalidate;
      }

#if defined(MDBX_USE_VALGRIND) || defined(__SANITIZE_ADDRESS__)
      if (MDBX_DEBUG != 0 || unlikely(txn->mt_env->me_flags & MDBX_PAGEPERTURB))
#endif
        kill_page(txn, mp, pgno, npages);
      if ((txn->mt_flags & MDBX_WRITEMAP) == 0) {
        VALGRIND_MAKE_MEM_NOACCESS(page_data(pgno2page(txn->mt_env, pgno)),
                                   pgno2bytes(txn->mt_env, npages) - PAGEHDRSZ);
        MDBX_ASAN_POISON_MEMORY_REGION(page_data(pgno2page(txn->mt_env, pgno)),
                                       pgno2bytes(txn->mt_env, npages) -
                                           PAGEHDRSZ);
      }
    }
  skip_invalidate:

    /* wash dirty page */
    page_wash(txn, di, mp, npages);

  reclaim:
    DEBUG("reclaim %zu %s page %" PRIaPGNO, npages, "dirty", pgno);
    rc = pnl_insert_range(&txn->tw.relist, pgno, npages);
    tASSERT(txn, pnl_check_allocated(txn->tw.relist,
                                     txn->mt_next_pgno - MDBX_ENABLE_REFUND));
    tASSERT(txn, dirtylist_check(txn));
    return rc;
  }

  if (si) {
    /* Page ws spilled in this txn */
    spill_remove(txn, si, npages);
    /* Страница могла быть выделена и затем пролита в этой транзакции,
     * тогда её необходимо поместить в reclaimed-список.
     * Либо она могла быть выделена в одной из родительских транзакций и затем
     * пролита в этой транзакции, тогда её необходимо поместить в
     * retired-список для последующей фильтрации при коммите. */
    for (MDBX_txn *parent = txn->mt_parent; parent;
         parent = parent->mt_parent) {
      if (dpl_exist(parent, pgno))
        goto retire;
    }
    /* Страница точно была выделена в этой транзакции
     * и теперь может быть использована повторно. */
    goto reclaim;
  }

  if (status == shadowed) {
    /* Dirty page MUST BE a clone from (one of) parent transaction(s). */
    if (ASSERT_ENABLED()) {
      const MDBX_page *parent_dp = nullptr;
      /* Check parent(s)'s dirty lists. */
      for (MDBX_txn *parent = txn->mt_parent; parent && !parent_dp;
           parent = parent->mt_parent) {
        tASSERT(txn, !search_spilled(parent, pgno));
        parent_dp = debug_dpl_find(parent, pgno);
      }
      tASSERT(txn, parent_dp && (!mp || parent_dp == mp));
    }
    /* Страница была выделена в родительской транзакции и теперь может быть
     * использована повторно, но только внутри этой транзакции, либо дочерних.
     */
    goto reclaim;
  }

  /* Страница может входить в доступный читателям MVCC-снимок, либо же она
   * могла быть выделена, а затем пролита в одной из родительских
   * транзакций. Поэтому пока помещаем её в retired-список, который будет
   * фильтроваться относительно dirty- и spilled-списков родительских
   * транзакций при коммите дочерних транзакций, либо же будет записан
   * в GC в неизменном виде. */
  goto retire;
}

static __inline int page_retire(MDBX_cursor *mc, MDBX_page *mp) {
  return page_retire_ex(mc, mp->mp_pgno, mp, mp->mp_flags);
}

typedef struct iov_ctx {
  MDBX_env *env;
  osal_ioring_t *ior;
  mdbx_filehandle_t fd;
  int err;
#ifndef MDBX_NEED_WRITTEN_RANGE
#define MDBX_NEED_WRITTEN_RANGE 1
#endif /* MDBX_NEED_WRITTEN_RANGE */
#if MDBX_NEED_WRITTEN_RANGE
  pgno_t flush_begin;
  pgno_t flush_end;
#endif /* MDBX_NEED_WRITTEN_RANGE */
  uint64_t coherency_timestamp;
} iov_ctx_t;

__must_check_result static int iov_init(MDBX_txn *const txn, iov_ctx_t *ctx,
                                        size_t items, size_t npages,
                                        mdbx_filehandle_t fd,
                                        bool check_coherence) {
  ctx->env = txn->mt_env;
  ctx->ior = &txn->mt_env->me_ioring;
  ctx->fd = fd;
  ctx->coherency_timestamp =
      (check_coherence || txn->mt_env->me_lck->mti_pgop_stat.incoherence.weak)
          ? 0
          : UINT64_MAX /* не выполнять сверку */;
  ctx->err = osal_ioring_prepare(ctx->ior, items,
                                 pgno_align2os_bytes(txn->mt_env, npages));
  if (likely(ctx->err == MDBX_SUCCESS)) {
#if MDBX_NEED_WRITTEN_RANGE
    ctx->flush_begin = MAX_PAGENO;
    ctx->flush_end = MIN_PAGENO;
#endif /* MDBX_NEED_WRITTEN_RANGE */
    osal_ioring_reset(ctx->ior);
  }
  return ctx->err;
}

static inline bool iov_empty(const iov_ctx_t *ctx) {
  return osal_ioring_used(ctx->ior) == 0;
}

static void iov_callback4dirtypages(iov_ctx_t *ctx, size_t offset, void *data,
                                    size_t bytes) {
  MDBX_env *const env = ctx->env;
  eASSERT(env, (env->me_flags & MDBX_WRITEMAP) == 0);

  MDBX_page *wp = (MDBX_page *)data;
  eASSERT(env, wp->mp_pgno == bytes2pgno(env, offset));
  eASSERT(env, bytes2pgno(env, bytes) >= (IS_OVERFLOW(wp) ? wp->mp_pages : 1u));
  eASSERT(env, (wp->mp_flags & P_ILL_BITS) == 0);

  if (likely(ctx->err == MDBX_SUCCESS)) {
    const MDBX_page *const rp = ptr_disp(env->me_map, offset);
    VALGRIND_MAKE_MEM_DEFINED(rp, bytes);
    MDBX_ASAN_UNPOISON_MEMORY_REGION(rp, bytes);
    osal_flush_incoherent_mmap(rp, bytes, env->me_os_psize);
    /* check with timeout as the workaround
     * for https://libmdbx.dqdkfa.ru/dead-github/issues/269
     *
     * Проблема проявляется только при неупорядоченности: если записанная
     * последней мета-страница "обгоняет" ранее записанные, т.е. когда
     * записанное в файл позже становится видимым в отображении раньше,
     * чем записанное ранее.
     *
     * Исходно здесь всегда выполнялась полная сверка. Это давало полную
     * гарантию защиты от проявления проблемы, но порождало накладные расходы.
     * В некоторых сценариях наблюдалось снижение производительности до 10-15%,
     * а в синтетических тестах до 30%. Конечно никто не вникал в причины,
     * а просто останавливался на мнении "libmdbx не быстрее LMDB",
     * например: https://clck.ru/3386er
     *
     * Поэтому после серии экспериментов и тестов реализовано следующее:
     * 0. Посредством опции сборки MDBX_FORCE_CHECK_MMAP_COHERENCY=1
     *    можно включить полную сверку после записи.
     *    Остальные пункты являются взвешенным компромиссом между полной
     *    гарантией обнаружения проблемы и бесполезными затратами на системах
     *    без этого недостатка.
     * 1. При старте транзакций проверяется соответствие выбранной мета-страницы
     *    корневым страницам b-tree проверяется. Эта проверка показала себя
     *    достаточной без сверки после записи. При обнаружении "некогерентности"
     *    эти случаи подсчитываются, а при их ненулевом счетчике выполняется
     *    полная сверка. Таким образом, произойдет переключение в режим полной
     *    сверки, если показавшая себя достаточной проверка заметит проявление
     *    проблемы хоты-бы раз.
     * 2. Сверка не выполняется при фиксации транзакции, так как:
     *    - при наличии проблемы "не-когерентности" (при отложенном копировании
     *      или обновлении PTE, после возврата из write-syscall), проверка
     *      в этом процессе не гарантирует актуальность данных в другом
     *      процессе, который может запустить транзакцию сразу после коммита;
     *    - сверка только последнего блока позволяет почти восстановить
     *      производительность в больших транзакциях, но одновременно размывает
     *      уверенность в отсутствии сбоев, чем обесценивает всю затею;
     *    - после записи данных будет записана мета-страница, соответствие
     *      которой корневым страницам b-tree проверяется при старте
     *      транзакций, и только эта проверка показала себя достаточной;
     * 3. При спиллинге производится полная сверка записанных страниц. Тут был
     *    соблазн сверять не полностью, а например начало и конец каждого блока.
     *    Но при спиллинге возможна ситуация повторного вытеснения страниц, в
     *    том числе large/overflow. При этом возникает риск прочитать в текущей
     *    транзакции старую версию страницы, до повторной записи. В этом случае
     *    могут возникать крайне редкие невоспроизводимые ошибки. С учетом того
     *    что спиллинг выполняет крайне редко, решено отказаться от экономии
     *    в пользу надежности. */
#ifndef MDBX_FORCE_CHECK_MMAP_COHERENCY
#define MDBX_FORCE_CHECK_MMAP_COHERENCY 0
#endif /* MDBX_FORCE_CHECK_MMAP_COHERENCY */
    if ((MDBX_FORCE_CHECK_MMAP_COHERENCY ||
         ctx->coherency_timestamp != UINT64_MAX) &&
        unlikely(memcmp(wp, rp, bytes))) {
      ctx->coherency_timestamp = 0;
      env->me_lck->mti_pgop_stat.incoherence.weak =
          (env->me_lck->mti_pgop_stat.incoherence.weak >= INT32_MAX)
              ? INT32_MAX
              : env->me_lck->mti_pgop_stat.incoherence.weak + 1;
      WARNING("catch delayed/non-arrived page %" PRIaPGNO " %s", wp->mp_pgno,
              "(workaround for incoherent flaw of unified page/buffer cache)");
      do
        if (coherency_timeout(&ctx->coherency_timestamp, wp->mp_pgno, env) !=
            MDBX_RESULT_TRUE) {
          ctx->err = MDBX_PROBLEM;
          break;
        }
      while (unlikely(memcmp(wp, rp, bytes)));
    }
  }

  if (likely(bytes == env->me_psize))
    dpage_free(env, wp, 1);
  else {
    do {
      eASSERT(env, wp->mp_pgno == bytes2pgno(env, offset));
      eASSERT(env, (wp->mp_flags & P_ILL_BITS) == 0);
      size_t npages = IS_OVERFLOW(wp) ? wp->mp_pages : 1u;
      size_t chunk = pgno2bytes(env, npages);
      eASSERT(env, bytes >= chunk);
      MDBX_page *next = ptr_disp(wp, chunk);
      dpage_free(env, wp, npages);
      wp = next;
      offset += chunk;
      bytes -= chunk;
    } while (bytes);
  }
}

static void iov_complete(iov_ctx_t *ctx) {
  if ((ctx->env->me_flags & MDBX_WRITEMAP) == 0)
    osal_ioring_walk(ctx->ior, ctx, iov_callback4dirtypages);
  osal_ioring_reset(ctx->ior);
}

__must_check_result static int iov_write(iov_ctx_t *ctx) {
  eASSERT(ctx->env, !iov_empty(ctx));
  osal_ioring_write_result_t r = osal_ioring_write(ctx->ior, ctx->fd);
#if MDBX_ENABLE_PGOP_STAT
  ctx->env->me_lck->mti_pgop_stat.wops.weak += r.wops;
#endif /* MDBX_ENABLE_PGOP_STAT */
  ctx->err = r.err;
  if (unlikely(ctx->err != MDBX_SUCCESS))
    ERROR("Write error: %s", mdbx_strerror(ctx->err));
  iov_complete(ctx);
  return ctx->err;
}

__must_check_result static int iov_page(MDBX_txn *txn, iov_ctx_t *ctx,
                                        MDBX_page *dp, size_t npages) {
  MDBX_env *const env = txn->mt_env;
  tASSERT(txn, ctx->err == MDBX_SUCCESS);
  tASSERT(txn, dp->mp_pgno >= MIN_PAGENO && dp->mp_pgno < txn->mt_next_pgno);
  tASSERT(txn, IS_MODIFIABLE(txn, dp));
  tASSERT(txn, !(dp->mp_flags & ~(P_BRANCH | P_LEAF | P_LEAF2 | P_OVERFLOW)));

  if (IS_SHADOWED(txn, dp)) {
    tASSERT(txn, !(txn->mt_flags & MDBX_WRITEMAP));
    dp->mp_txnid = txn->mt_txnid;
    tASSERT(txn, IS_SPILLED(txn, dp));
#if MDBX_AVOID_MSYNC
  doit:;
#endif /* MDBX_AVOID_MSYNC */
    int err = osal_ioring_add(ctx->ior, pgno2bytes(env, dp->mp_pgno), dp,
                              pgno2bytes(env, npages));
    if (unlikely(err != MDBX_SUCCESS)) {
      ctx->err = err;
      if (unlikely(err != MDBX_RESULT_TRUE)) {
        iov_complete(ctx);
        return err;
      }
      err = iov_write(ctx);
      tASSERT(txn, iov_empty(ctx));
      if (likely(err == MDBX_SUCCESS)) {
        err = osal_ioring_add(ctx->ior, pgno2bytes(env, dp->mp_pgno), dp,
                              pgno2bytes(env, npages));
        if (unlikely(err != MDBX_SUCCESS)) {
          iov_complete(ctx);
          return ctx->err = err;
        }
      }
      tASSERT(txn, ctx->err == MDBX_SUCCESS);
    }
  } else {
    tASSERT(txn, txn->mt_flags & MDBX_WRITEMAP);
#if MDBX_AVOID_MSYNC
    goto doit;
#endif /* MDBX_AVOID_MSYNC */
  }

#if MDBX_NEED_WRITTEN_RANGE
  ctx->flush_begin =
      (ctx->flush_begin < dp->mp_pgno) ? ctx->flush_begin : dp->mp_pgno;
  ctx->flush_end = (ctx->flush_end > dp->mp_pgno + (pgno_t)npages)
                       ? ctx->flush_end
                       : dp->mp_pgno + (pgno_t)npages;
#endif /* MDBX_NEED_WRITTEN_RANGE */
  return MDBX_SUCCESS;
}

static int spill_page(MDBX_txn *txn, iov_ctx_t *ctx, MDBX_page *dp,
                      const size_t npages) {
  tASSERT(txn, !(txn->mt_flags & MDBX_WRITEMAP));
#if MDBX_ENABLE_PGOP_STAT
  txn->mt_env->me_lck->mti_pgop_stat.spill.weak += npages;
#endif /* MDBX_ENABLE_PGOP_STAT */
  const pgno_t pgno = dp->mp_pgno;
  int err = iov_page(txn, ctx, dp, npages);
  if (likely(err == MDBX_SUCCESS))
    err = pnl_append_range(true, &txn->tw.spilled.list, pgno << 1, npages);
  return err;
}

/* Set unspillable LRU-label for dirty pages watched by txn.
 * Returns the number of pages marked as unspillable. */
static size_t cursor_keep(const MDBX_txn *const txn, const MDBX_cursor *mc) {
  tASSERT(txn, (txn->mt_flags & (MDBX_TXN_RDONLY | MDBX_WRITEMAP)) == 0);
  size_t keep = 0;
  while ((mc->mc_flags & C_INITIALIZED) && mc->mc_snum) {
    tASSERT(txn, mc->mc_top == mc->mc_snum - 1);
    const MDBX_page *mp;
    size_t i = 0;
    do {
      mp = mc->mc_pg[i];
      tASSERT(txn, !IS_SUBP(mp));
      if (IS_MODIFIABLE(txn, mp)) {
        size_t const n = dpl_search(txn, mp->mp_pgno);
        if (txn->tw.dirtylist->items[n].pgno == mp->mp_pgno &&
            /* не считаем дважды */ dpl_age(txn, n)) {
          size_t *const ptr = ptr_disp(txn->tw.dirtylist->items[n].ptr,
                                       -(ptrdiff_t)sizeof(size_t));
          *ptr = txn->tw.dirtylru;
          tASSERT(txn, dpl_age(txn, n) == 0);
          ++keep;
        }
      }
    } while (++i < mc->mc_snum);

    tASSERT(txn, IS_LEAF(mp));
    if (!mc->mc_xcursor || mc->mc_ki[mc->mc_top] >= page_numkeys(mp))
      break;
    if (!(node_flags(page_node(mp, mc->mc_ki[mc->mc_top])) & F_SUBDATA))
      break;
    mc = &mc->mc_xcursor->mx_cursor;
  }
  return keep;
}

static size_t txn_keep(MDBX_txn *txn, MDBX_cursor *m0) {
  tASSERT(txn, (txn->mt_flags & (MDBX_TXN_RDONLY | MDBX_WRITEMAP)) == 0);
  txn_lru_turn(txn);
  size_t keep = m0 ? cursor_keep(txn, m0) : 0;
  for (size_t i = FREE_DBI; i < txn->mt_numdbs; ++i)
    if (F_ISSET(txn->mt_dbistate[i], DBI_DIRTY | DBI_VALID) &&
        txn->mt_dbs[i].md_root != P_INVALID)
      for (MDBX_cursor *mc = txn->mt_cursors[i]; mc; mc = mc->mc_next)
        if (mc != m0)
          keep += cursor_keep(txn, mc);
  return keep;
}

/* Returns the spilling priority (0..255) for a dirty page:
 *      0 = should be spilled;
 *    ...
 *  > 255 = must not be spilled. */
MDBX_NOTHROW_PURE_FUNCTION static unsigned
spill_prio(const MDBX_txn *txn, const size_t i, const uint32_t reciprocal) {
  MDBX_dpl *const dl = txn->tw.dirtylist;
  const uint32_t age = dpl_age(txn, i);
  const size_t npages = dpl_npages(dl, i);
  const pgno_t pgno = dl->items[i].pgno;
  if (age == 0) {
    DEBUG("skip %s %zu page %" PRIaPGNO, "keep", npages, pgno);
    return 256;
  }

  MDBX_page *const dp = dl->items[i].ptr;
  if (dp->mp_flags & (P_LOOSE | P_SPILLED)) {
    DEBUG("skip %s %zu page %" PRIaPGNO,
          (dp->mp_flags & P_LOOSE) ? "loose" : "parent-spilled", npages, pgno);
    return 256;
  }

  /* Can't spill twice,
   * make sure it's not already in a parent's spill list(s). */
  MDBX_txn *parent = txn->mt_parent;
  if (parent && (parent->mt_flags & MDBX_TXN_SPILLS)) {
    do
      if (intersect_spilled(parent, pgno, npages)) {
        DEBUG("skip-2 parent-spilled %zu page %" PRIaPGNO, npages, pgno);
        dp->mp_flags |= P_SPILLED;
        return 256;
      }
    while ((parent = parent->mt_parent) != nullptr);
  }

  tASSERT(txn, age * (uint64_t)reciprocal < UINT32_MAX);
  unsigned prio = age * reciprocal >> 24;
  tASSERT(txn, prio < 256);
  if (likely(npages == 1))
    return prio = 256 - prio;

  /* make a large/overflow pages be likely to spill */
  size_t factor = npages | npages >> 1;
  factor |= factor >> 2;
  factor |= factor >> 4;
  factor |= factor >> 8;
  factor |= factor >> 16;
  factor = (size_t)prio * log2n_powerof2(factor + 1) + /* golden ratio */ 157;
  factor = (factor < 256) ? 255 - factor : 0;
  tASSERT(txn, factor < 256 && factor < (256 - prio));
  return prio = (unsigned)factor;
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
 * the same way as for a txn commit, e.g. their dirty status is cleared.
 * If the txn never references them again, they can be left alone.
 * If the txn only reads them, they can be used without any fuss.
 * If the txn writes them again, they can be dirtied immediately without
 * going thru all of the work of page_touch(). Such references are
 * handled by page_unspill().
 *
 * Also note, we never spill DB root pages, nor pages of active cursors,
 * because we'll need these back again soon anyway. And in nested txns,
 * we can't spill a page in a child txn if it was already spilled in a
 * parent txn. That would alter the parent txns' data even though
 * the child hasn't committed yet, and we'd have no way to undo it if
 * the child aborted. */
__cold static int txn_spill_slowpath(MDBX_txn *const txn, MDBX_cursor *const m0,
                                     const intptr_t wanna_spill_entries,
                                     const intptr_t wanna_spill_npages,
                                     const size_t need);

static __inline int txn_spill(MDBX_txn *const txn, MDBX_cursor *const m0,
                              const size_t need) {
  tASSERT(txn, (txn->mt_flags & MDBX_TXN_RDONLY) == 0);
  tASSERT(txn, !m0 || cursor_is_tracked(m0));

  const intptr_t wanna_spill_entries =
      txn->tw.dirtylist ? (need - txn->tw.dirtyroom - txn->tw.loose_count) : 0;
  const intptr_t wanna_spill_npages =
      need +
      (txn->tw.dirtylist ? txn->tw.dirtylist->pages_including_loose
                         : txn->tw.writemap_dirty_npages) -
      txn->tw.loose_count - txn->mt_env->me_options.dp_limit;

  /* production mode */
  if (likely(wanna_spill_npages < 1 && wanna_spill_entries < 1)
#if xMDBX_DEBUG_SPILLING == 1
      /* debug mode: always try to spill if xMDBX_DEBUG_SPILLING == 1 */
      && txn->mt_txnid % 23 > 11
#endif
  )
    return MDBX_SUCCESS;

  return txn_spill_slowpath(txn, m0, wanna_spill_entries, wanna_spill_npages,
                            need);
}

static size_t spill_gate(const MDBX_env *env, intptr_t part,
                         const size_t total) {
  const intptr_t spill_min =
      env->me_options.spill_min_denominator
          ? (total + env->me_options.spill_min_denominator - 1) /
                env->me_options.spill_min_denominator
          : 1;
  const intptr_t spill_max =
      total - (env->me_options.spill_max_denominator
                   ? total / env->me_options.spill_max_denominator
                   : 0);
  part = (part < spill_max) ? part : spill_max;
  part = (part > spill_min) ? part : spill_min;
  eASSERT(env, part >= 0 && (size_t)part <= total);
  return (size_t)part;
}

__cold static int txn_spill_slowpath(MDBX_txn *const txn, MDBX_cursor *const m0,
                                     const intptr_t wanna_spill_entries,
                                     const intptr_t wanna_spill_npages,
                                     const size_t need) {
  tASSERT(txn, (txn->mt_flags & MDBX_TXN_RDONLY) == 0);

  int rc = MDBX_SUCCESS;
  if (unlikely(txn->tw.loose_count >=
               (txn->tw.dirtylist ? txn->tw.dirtylist->pages_including_loose
                                  : txn->tw.writemap_dirty_npages)))
    goto done;

  const size_t dirty_entries =
      txn->tw.dirtylist ? (txn->tw.dirtylist->length - txn->tw.loose_count) : 1;
  const size_t dirty_npages =
      (txn->tw.dirtylist ? txn->tw.dirtylist->pages_including_loose
                         : txn->tw.writemap_dirty_npages) -
      txn->tw.loose_count;
  const size_t need_spill_entries =
      spill_gate(txn->mt_env, wanna_spill_entries, dirty_entries);
  const size_t need_spill_npages =
      spill_gate(txn->mt_env, wanna_spill_npages, dirty_npages);

  const size_t need_spill = (need_spill_entries > need_spill_npages)
                                ? need_spill_entries
                                : need_spill_npages;
  if (!need_spill)
    goto done;

  if (txn->mt_flags & MDBX_WRITEMAP) {
    NOTICE("%s-spilling %zu dirty-entries, %zu dirty-npages", "msync",
           dirty_entries, dirty_npages);
    const MDBX_env *env = txn->mt_env;
    tASSERT(txn, txn->tw.spilled.list == nullptr);
    rc =
        osal_msync(&txn->mt_env->me_dxb_mmap, 0,
                   pgno_align2os_bytes(env, txn->mt_next_pgno), MDBX_SYNC_KICK);
    if (unlikely(rc != MDBX_SUCCESS))
      goto bailout;
#if MDBX_AVOID_MSYNC
    MDBX_ANALYSIS_ASSUME(txn->tw.dirtylist != nullptr);
    tASSERT(txn, dirtylist_check(txn));
    env->me_lck->mti_unsynced_pages.weak +=
        txn->tw.dirtylist->pages_including_loose - txn->tw.loose_count;
    dpl_clear(txn->tw.dirtylist);
    txn->tw.dirtyroom = env->me_options.dp_limit - txn->tw.loose_count;
    for (MDBX_page *lp = txn->tw.loose_pages; lp != nullptr; lp = mp_next(lp)) {
      tASSERT(txn, lp->mp_flags == P_LOOSE);
      rc = dpl_append(txn, lp->mp_pgno, lp, 1);
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;
      MDBX_ASAN_UNPOISON_MEMORY_REGION(&mp_next(lp), sizeof(MDBX_page *));
      VALGRIND_MAKE_MEM_DEFINED(&mp_next(lp), sizeof(MDBX_page *));
    }
    tASSERT(txn, dirtylist_check(txn));
#else
    tASSERT(txn, txn->tw.dirtylist == nullptr);
    env->me_lck->mti_unsynced_pages.weak += txn->tw.writemap_dirty_npages;
    txn->tw.writemap_spilled_npages += txn->tw.writemap_dirty_npages;
    txn->tw.writemap_dirty_npages = 0;
#endif /* MDBX_AVOID_MSYNC */
    goto done;
  }

  NOTICE("%s-spilling %zu dirty-entries, %zu dirty-npages", "write",
         need_spill_entries, need_spill_npages);
  MDBX_ANALYSIS_ASSUME(txn->tw.dirtylist != nullptr);
  tASSERT(txn, txn->tw.dirtylist->length - txn->tw.loose_count >= 1);
  tASSERT(txn, txn->tw.dirtylist->pages_including_loose - txn->tw.loose_count >=
                   need_spill_npages);
  if (!txn->tw.spilled.list) {
    txn->tw.spilled.least_removed = INT_MAX;
    txn->tw.spilled.list = pnl_alloc(need_spill);
    if (unlikely(!txn->tw.spilled.list)) {
      rc = MDBX_ENOMEM;
    bailout:
      txn->mt_flags |= MDBX_TXN_ERROR;
      return rc;
    }
  } else {
    /* purge deleted slots */
    spill_purge(txn);
    rc = pnl_reserve(&txn->tw.spilled.list, need_spill);
    (void)rc /* ignore since the resulting list may be shorter
     and pnl_append() will increase pnl on demand */
        ;
  }

  /* Сортируем чтобы запись на диск была полее последовательна */
  MDBX_dpl *const dl = dpl_sort(txn);

  /* Preserve pages which may soon be dirtied again */
  const size_t unspillable = txn_keep(txn, m0);
  if (unspillable + txn->tw.loose_count >= dl->length) {
#if xMDBX_DEBUG_SPILLING == 1 /* avoid false failure in debug mode  */
    if (likely(txn->tw.dirtyroom + txn->tw.loose_count >= need))
      return MDBX_SUCCESS;
#endif /* xMDBX_DEBUG_SPILLING */
    ERROR("all %zu dirty pages are unspillable since referenced "
          "by a cursor(s), use fewer cursors or increase "
          "MDBX_opt_txn_dp_limit",
          unspillable);
    goto done;
  }

  /* Подзадача: Вытолкнуть часть страниц на диск в соответствии с LRU,
   * но при этом учесть важные поправки:
   *  - лучше выталкивать старые large/overflow страницы, так будет освобождено
   *    больше памяти, а также так как они (в текущем понимании) гораздо реже
   *    повторно изменяются;
   *  - при прочих равных лучше выталкивать смежные страницы, так будет
   *    меньше I/O операций;
   *  - желательно потратить на это меньше времени чем std::partial_sort_copy;
   *
   * Решение:
   *  - Квантуем весь диапазон lru-меток до 256 значений и задействуем один
   *    проход 8-битного radix-sort. В результате получаем 256 уровней
   *    "свежести", в том числе значение lru-метки, старее которой страницы
   *    должны быть выгружены;
   *  - Двигаемся последовательно в сторону увеличения номеров страниц
   *    и выталкиваем страницы с lru-меткой старее отсекающего значения,
   *    пока не вытолкнем достаточно;
   *  - Встречая страницы смежные с выталкиваемыми для уменьшения кол-ва
   *    I/O операций выталкиваем и их, если они попадают в первую половину
   *    между выталкиваемыми и самыми свежими lru-метками;
   *  - дополнительно при сортировке умышленно старим large/overflow страницы,
   *    тем самым повышая их шансы на выталкивание. */

  /* get min/max of LRU-labels */
  uint32_t age_max = 0;
  for (size_t i = 1; i <= dl->length; ++i) {
    const uint32_t age = dpl_age(txn, i);
    age_max = (age_max >= age) ? age_max : age;
  }

  VERBOSE("lru-head %u, age-max %u", txn->tw.dirtylru, age_max);

  /* half of 8-bit radix-sort */
  pgno_t radix_entries[256], radix_npages[256];
  memset(&radix_entries, 0, sizeof(radix_entries));
  memset(&radix_npages, 0, sizeof(radix_npages));
  size_t spillable_entries = 0, spillable_npages = 0;
  const uint32_t reciprocal = (UINT32_C(255) << 24) / (age_max + 1);
  for (size_t i = 1; i <= dl->length; ++i) {
    const unsigned prio = spill_prio(txn, i, reciprocal);
    size_t *const ptr = ptr_disp(dl->items[i].ptr, -(ptrdiff_t)sizeof(size_t));
    TRACE("page %" PRIaPGNO
          ", lru %zu, is_multi %c, npages %u, age %u of %u, prio %u",
          dl->items[i].pgno, *ptr, (dl->items[i].npages > 1) ? 'Y' : 'N',
          dpl_npages(dl, i), dpl_age(txn, i), age_max, prio);
    if (prio < 256) {
      radix_entries[prio] += 1;
      spillable_entries += 1;
      const pgno_t npages = dpl_npages(dl, i);
      radix_npages[prio] += npages;
      spillable_npages += npages;
    }
  }

  tASSERT(txn, spillable_npages >= spillable_entries);
  pgno_t spilled_entries = 0, spilled_npages = 0;
  if (likely(spillable_entries > 0)) {
    size_t prio2spill = 0, prio2adjacent = 128,
           amount_entries = radix_entries[0], amount_npages = radix_npages[0];
    for (size_t i = 1; i < 256; i++) {
      if (amount_entries < need_spill_entries ||
          amount_npages < need_spill_npages) {
        prio2spill = i;
        prio2adjacent = i + (257 - i) / 2;
        amount_entries += radix_entries[i];
        amount_npages += radix_npages[i];
      } else if (amount_entries + amount_entries <
                     spillable_entries + need_spill_entries
                 /* РАВНОЗНАЧНО: amount - need_spill < spillable - amount */
                 || amount_npages + amount_npages <
                        spillable_npages + need_spill_npages) {
        prio2adjacent = i;
        amount_entries += radix_entries[i];
        amount_npages += radix_npages[i];
      } else
        break;
    }

    VERBOSE("prio2spill %zu, prio2adjacent %zu, spillable %zu/%zu,"
            " wanna-spill %zu/%zu, amount %zu/%zu",
            prio2spill, prio2adjacent, spillable_entries, spillable_npages,
            need_spill_entries, need_spill_npages, amount_entries,
            amount_npages);
    tASSERT(txn, prio2spill < prio2adjacent && prio2adjacent <= 256);

    iov_ctx_t ctx;
    rc =
        iov_init(txn, &ctx, amount_entries, amount_npages,
#if defined(_WIN32) || defined(_WIN64)
                 txn->mt_env->me_overlapped_fd ? txn->mt_env->me_overlapped_fd :
#endif
                                               txn->mt_env->me_lazy_fd,
                 true);
    if (unlikely(rc != MDBX_SUCCESS))
      goto bailout;

    size_t r = 0, w = 0;
    pgno_t last = 0;
    while (r < dl->length && (spilled_entries < need_spill_entries ||
                              spilled_npages < need_spill_npages)) {
      dl->items[++w] = dl->items[++r];
      unsigned prio = spill_prio(txn, w, reciprocal);
      if (prio > prio2spill &&
          (prio >= prio2adjacent || last != dl->items[w].pgno))
        continue;

      const size_t e = w;
      last = dpl_endpgno(dl, w);
      while (--w && dpl_endpgno(dl, w) == dl->items[w + 1].pgno &&
             spill_prio(txn, w, reciprocal) < prio2adjacent)
        ;

      for (size_t i = w; ++i <= e;) {
        const unsigned npages = dpl_npages(dl, i);
        prio = spill_prio(txn, i, reciprocal);
        DEBUG("%sspill[%zu] %u page %" PRIaPGNO " (age %d, prio %u)",
              (prio > prio2spill) ? "co-" : "", i, npages, dl->items[i].pgno,
              dpl_age(txn, i), prio);
        tASSERT(txn, prio < 256);
        ++spilled_entries;
        spilled_npages += npages;
        rc = spill_page(txn, &ctx, dl->items[i].ptr, npages);
        if (unlikely(rc != MDBX_SUCCESS))
          goto failed;
      }
    }

    VERBOSE("spilled entries %u, spilled npages %u", spilled_entries,
            spilled_npages);
    tASSERT(txn, spillable_entries == 0 || spilled_entries > 0);
    tASSERT(txn, spilled_npages >= spilled_entries);

  failed:
    while (r < dl->length)
      dl->items[++w] = dl->items[++r];
    tASSERT(txn, r - w == spilled_entries || rc != MDBX_SUCCESS);

    dl->sorted = dpl_setlen(dl, w);
    txn->tw.dirtyroom += spilled_entries;
    txn->tw.dirtylist->pages_including_loose -= spilled_npages;
    tASSERT(txn, dirtylist_check(txn));

    if (!iov_empty(&ctx)) {
      tASSERT(txn, rc == MDBX_SUCCESS);
      rc = iov_write(&ctx);
    }
    if (unlikely(rc != MDBX_SUCCESS))
      goto bailout;

    txn->mt_env->me_lck->mti_unsynced_pages.weak += spilled_npages;
    pnl_sort(txn->tw.spilled.list, (size_t)txn->mt_next_pgno << 1);
    txn->mt_flags |= MDBX_TXN_SPILLS;
    NOTICE("spilled %u dirty-entries, %u dirty-npages, now have %zu dirty-room",
           spilled_entries, spilled_npages, txn->tw.dirtyroom);
  } else {
    tASSERT(txn, rc == MDBX_SUCCESS);
    for (size_t i = 1; i <= dl->length; ++i) {
      MDBX_page *dp = dl->items[i].ptr;
      VERBOSE(
          "unspillable[%zu]: pgno %u, npages %u, flags 0x%04X, age %u, prio %u",
          i, dp->mp_pgno, dpl_npages(dl, i), dp->mp_flags, dpl_age(txn, i),
          spill_prio(txn, i, reciprocal));
    }
  }

#if xMDBX_DEBUG_SPILLING == 2
  if (txn->tw.loose_count + txn->tw.dirtyroom <= need / 2 + 1)
    ERROR("dirty-list length: before %zu, after %zu, parent %zi, loose %zu; "
          "needed %zu, spillable %zu; "
          "spilled %u dirty-entries, now have %zu dirty-room",
          dl->length + spilled_entries, dl->length,
          (txn->mt_parent && txn->mt_parent->tw.dirtylist)
              ? (intptr_t)txn->mt_parent->tw.dirtylist->length
              : -1,
          txn->tw.loose_count, need, spillable_entries, spilled_entries,
          txn->tw.dirtyroom);
  ENSURE(txn->mt_env, txn->tw.loose_count + txn->tw.dirtyroom > need / 2);
#endif /* xMDBX_DEBUG_SPILLING */

done:
  return likely(txn->tw.dirtyroom + txn->tw.loose_count >
                ((need > CURSOR_STACK) ? CURSOR_STACK : need))
             ? MDBX_SUCCESS
             : MDBX_TXN_FULL;
}

/*----------------------------------------------------------------------------*/

static bool meta_bootid_match(const MDBX_meta *meta) {
  return memcmp(&meta->mm_bootid, &bootid, 16) == 0 &&
         (bootid.x | bootid.y) != 0;
}

static bool meta_weak_acceptable(const MDBX_env *env, const MDBX_meta *meta,
                                 const int lck_exclusive) {
  return lck_exclusive
             ? /* exclusive lock */ meta_bootid_match(meta)
             : /* db already opened */ env->me_lck_mmap.lck &&
                   (env->me_lck_mmap.lck->mti_envmode.weak & MDBX_RDONLY) == 0;
}

#define METAPAGE(env, n) page_meta(pgno2page(env, n))
#define METAPAGE_END(env) METAPAGE(env, NUM_METAS)

MDBX_NOTHROW_PURE_FUNCTION static txnid_t
constmeta_txnid(const MDBX_meta *meta) {
  const txnid_t a = unaligned_peek_u64(4, &meta->mm_txnid_a);
  const txnid_t b = unaligned_peek_u64(4, &meta->mm_txnid_b);
  return likely(a == b) ? a : 0;
}

typedef struct {
  uint64_t txnid;
  size_t is_steady;
} meta_snap_t;

static __always_inline txnid_t
atomic_load_txnid(const volatile MDBX_atomic_uint32_t *ptr) {
#if (defined(__amd64__) || defined(__e2k__)) && !defined(ENABLE_UBSAN) &&      \
    MDBX_UNALIGNED_OK >= 8
  return atomic_load64((const volatile MDBX_atomic_uint64_t *)ptr,
                       mo_AcquireRelease);
#else
  const uint32_t l = atomic_load32(
      &ptr[__BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__], mo_AcquireRelease);
  const uint32_t h = atomic_load32(
      &ptr[__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__], mo_AcquireRelease);
  return (uint64_t)h << 32 | l;
#endif
}

static __inline meta_snap_t meta_snap(const volatile MDBX_meta *meta) {
  txnid_t txnid = atomic_load_txnid(meta->mm_txnid_a);
  jitter4testing(true);
  size_t is_steady = META_IS_STEADY(meta) && txnid >= MIN_TXNID;
  jitter4testing(true);
  if (unlikely(txnid != atomic_load_txnid(meta->mm_txnid_b)))
    txnid = is_steady = 0;
  meta_snap_t r = {txnid, is_steady};
  return r;
}

static __inline txnid_t meta_txnid(const volatile MDBX_meta *meta) {
  return meta_snap(meta).txnid;
}

static __inline void meta_update_begin(const MDBX_env *env, MDBX_meta *meta,
                                       txnid_t txnid) {
  eASSERT(env, meta >= METAPAGE(env, 0) && meta < METAPAGE_END(env));
  eASSERT(env, unaligned_peek_u64(4, meta->mm_txnid_a) < txnid &&
                   unaligned_peek_u64(4, meta->mm_txnid_b) < txnid);
  (void)env;
#if (defined(__amd64__) || defined(__e2k__)) && !defined(ENABLE_UBSAN) &&      \
    MDBX_UNALIGNED_OK >= 8
  atomic_store64((MDBX_atomic_uint64_t *)&meta->mm_txnid_b, 0,
                 mo_AcquireRelease);
  atomic_store64((MDBX_atomic_uint64_t *)&meta->mm_txnid_a, txnid,
                 mo_AcquireRelease);
#else
  atomic_store32(&meta->mm_txnid_b[__BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__],
                 0, mo_AcquireRelease);
  atomic_store32(&meta->mm_txnid_b[__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__],
                 0, mo_AcquireRelease);
  atomic_store32(&meta->mm_txnid_a[__BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__],
                 (uint32_t)txnid, mo_AcquireRelease);
  atomic_store32(&meta->mm_txnid_a[__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__],
                 (uint32_t)(txnid >> 32), mo_AcquireRelease);
#endif
}

static __inline void meta_update_end(const MDBX_env *env, MDBX_meta *meta,
                                     txnid_t txnid) {
  eASSERT(env, meta >= METAPAGE(env, 0) && meta < METAPAGE_END(env));
  eASSERT(env, unaligned_peek_u64(4, meta->mm_txnid_a) == txnid);
  eASSERT(env, unaligned_peek_u64(4, meta->mm_txnid_b) < txnid);
  (void)env;
  jitter4testing(true);
  memcpy(&meta->mm_bootid, &bootid, 16);
#if (defined(__amd64__) || defined(__e2k__)) && !defined(ENABLE_UBSAN) &&      \
    MDBX_UNALIGNED_OK >= 8
  atomic_store64((MDBX_atomic_uint64_t *)&meta->mm_txnid_b, txnid,
                 mo_AcquireRelease);
#else
  atomic_store32(&meta->mm_txnid_b[__BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__],
                 (uint32_t)txnid, mo_AcquireRelease);
  atomic_store32(&meta->mm_txnid_b[__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__],
                 (uint32_t)(txnid >> 32), mo_AcquireRelease);
#endif
}

static __inline void meta_set_txnid(const MDBX_env *env, MDBX_meta *meta,
                                    const txnid_t txnid) {
  eASSERT(env,
          !env->me_map || meta < METAPAGE(env, 0) || meta >= METAPAGE_END(env));
  (void)env;
  /* update inconsistently since this function used ONLY for filling meta-image
   * for writing, but not the actual meta-page */
  memcpy(&meta->mm_bootid, &bootid, 16);
  unaligned_poke_u64(4, meta->mm_txnid_a, txnid);
  unaligned_poke_u64(4, meta->mm_txnid_b, txnid);
}

static __inline uint64_t meta_sign(const MDBX_meta *meta) {
  uint64_t sign = MDBX_DATASIGN_NONE;
#if 0 /* TODO */
  sign = hippeus_hash64(...);
#else
  (void)meta;
#endif
  /* LY: newer returns MDBX_DATASIGN_NONE or MDBX_DATASIGN_WEAK */
  return (sign > MDBX_DATASIGN_WEAK) ? sign : ~sign;
}

typedef struct {
  txnid_t txnid;
  union {
    const volatile MDBX_meta *ptr_v;
    const MDBX_meta *ptr_c;
  };
  size_t is_steady;
} meta_ptr_t;

static meta_ptr_t meta_ptr(const MDBX_env *env, unsigned n) {
  eASSERT(env, n < NUM_METAS);
  meta_ptr_t r;
  meta_snap_t snap = meta_snap(r.ptr_v = METAPAGE(env, n));
  r.txnid = snap.txnid;
  r.is_steady = snap.is_steady;
  return r;
}

static __always_inline uint8_t meta_cmp2int(txnid_t a, txnid_t b, uint8_t s) {
  return unlikely(a == b) ? 1 * s : (a > b) ? 2 * s : 0 * s;
}

static __always_inline uint8_t meta_cmp2recent(uint8_t ab_cmp2int,
                                               bool a_steady, bool b_steady) {
  assert(ab_cmp2int < 3 /* && a_steady< 2 && b_steady < 2 */);
  return ab_cmp2int > 1 || (ab_cmp2int == 1 && a_steady > b_steady);
}

static __always_inline uint8_t meta_cmp2steady(uint8_t ab_cmp2int,
                                               bool a_steady, bool b_steady) {
  assert(ab_cmp2int < 3 /* && a_steady< 2 && b_steady < 2 */);
  return a_steady > b_steady || (a_steady == b_steady && ab_cmp2int > 1);
}

static __inline bool meta_choice_recent(txnid_t a_txnid, bool a_steady,
                                        txnid_t b_txnid, bool b_steady) {
  return meta_cmp2recent(meta_cmp2int(a_txnid, b_txnid, 1), a_steady, b_steady);
}

static __inline bool meta_choice_steady(txnid_t a_txnid, bool a_steady,
                                        txnid_t b_txnid, bool b_steady) {
  return meta_cmp2steady(meta_cmp2int(a_txnid, b_txnid, 1), a_steady, b_steady);
}

MDBX_MAYBE_UNUSED static uint8_t meta_cmp2pack(uint8_t c01, uint8_t c02,
                                               uint8_t c12, bool s0, bool s1,
                                               bool s2) {
  assert(c01 < 3 && c02 < 3 && c12 < 3);
  /* assert(s0 < 2 && s1 < 2 && s2 < 2); */
  const uint8_t recent = meta_cmp2recent(c01, s0, s1)
                             ? (meta_cmp2recent(c02, s0, s2) ? 0 : 2)
                             : (meta_cmp2recent(c12, s1, s2) ? 1 : 2);
  const uint8_t prefer_steady = meta_cmp2steady(c01, s0, s1)
                                    ? (meta_cmp2steady(c02, s0, s2) ? 0 : 2)
                                    : (meta_cmp2steady(c12, s1, s2) ? 1 : 2);

  uint8_t tail;
  if (recent == 0)
    tail = meta_cmp2steady(c12, s1, s2) ? 2 : 1;
  else if (recent == 1)
    tail = meta_cmp2steady(c02, s0, s2) ? 2 : 0;
  else
    tail = meta_cmp2steady(c01, s0, s1) ? 1 : 0;

  const bool valid =
      c01 != 1 || s0 != s1 || c02 != 1 || s0 != s2 || c12 != 1 || s1 != s2;
  const bool strict = (c01 != 1 || s0 != s1) && (c02 != 1 || s0 != s2) &&
                      (c12 != 1 || s1 != s2);
  return tail | recent << 2 | prefer_steady << 4 | strict << 6 | valid << 7;
}

static __inline void meta_troika_unpack(meta_troika_t *troika,
                                        const uint8_t packed) {
  troika->recent = (packed >> 2) & 3;
  troika->prefer_steady = (packed >> 4) & 3;
  troika->tail_and_flags = packed & 0xC3;
#if MDBX_WORDBITS > 32 /* Workaround for false-positives from Valgrind */
  troika->unused_pad = 0;
#endif
}

static const uint8_t troika_fsm_map[2 * 2 * 2 * 3 * 3 * 3] = {
    232, 201, 216, 216, 232, 233, 232, 232, 168, 201, 216, 152, 168, 233, 232,
    168, 233, 201, 216, 201, 233, 233, 232, 233, 168, 201, 152, 216, 232, 169,
    232, 168, 168, 193, 152, 152, 168, 169, 232, 168, 169, 193, 152, 194, 233,
    169, 232, 169, 232, 201, 216, 216, 232, 201, 232, 232, 168, 193, 216, 152,
    168, 193, 232, 168, 193, 193, 210, 194, 225, 193, 225, 193, 168, 137, 212,
    214, 232, 233, 168, 168, 168, 137, 212, 150, 168, 233, 168, 168, 169, 137,
    216, 201, 233, 233, 168, 169, 168, 137, 148, 214, 232, 169, 168, 168, 40,
    129, 148, 150, 168, 169, 168, 40,  169, 129, 152, 194, 233, 169, 168, 169,
    168, 137, 214, 214, 232, 201, 168, 168, 168, 129, 214, 150, 168, 193, 168,
    168, 129, 129, 210, 194, 225, 193, 161, 129, 212, 198, 212, 214, 228, 228,
    212, 212, 148, 201, 212, 150, 164, 233, 212, 148, 233, 201, 216, 201, 233,
    233, 216, 233, 148, 198, 148, 214, 228, 164, 212, 148, 148, 194, 148, 150,
    164, 169, 212, 148, 169, 194, 152, 194, 233, 169, 216, 169, 214, 198, 214,
    214, 228, 198, 212, 214, 150, 194, 214, 150, 164, 193, 212, 150, 194, 194,
    210, 194, 225, 193, 210, 194};

__hot static meta_troika_t meta_tap(const MDBX_env *env) {
  meta_snap_t snap;
  meta_troika_t troika;
  snap = meta_snap(METAPAGE(env, 0));
  troika.txnid[0] = snap.txnid;
  troika.fsm = (uint8_t)snap.is_steady << 0;
  snap = meta_snap(METAPAGE(env, 1));
  troika.txnid[1] = snap.txnid;
  troika.fsm += (uint8_t)snap.is_steady << 1;
  troika.fsm += meta_cmp2int(troika.txnid[0], troika.txnid[1], 8);
  snap = meta_snap(METAPAGE(env, 2));
  troika.txnid[2] = snap.txnid;
  troika.fsm += (uint8_t)snap.is_steady << 2;
  troika.fsm += meta_cmp2int(troika.txnid[0], troika.txnid[2], 8 * 3);
  troika.fsm += meta_cmp2int(troika.txnid[1], troika.txnid[2], 8 * 3 * 3);

  meta_troika_unpack(&troika, troika_fsm_map[troika.fsm]);
  return troika;
}

static txnid_t recent_committed_txnid(const MDBX_env *env) {
  const txnid_t m0 = meta_txnid(METAPAGE(env, 0));
  const txnid_t m1 = meta_txnid(METAPAGE(env, 1));
  const txnid_t m2 = meta_txnid(METAPAGE(env, 2));
  return (m0 > m1) ? ((m0 > m2) ? m0 : m2) : ((m1 > m2) ? m1 : m2);
}

static __inline bool meta_eq(const meta_troika_t *troika, size_t a, size_t b) {
  assert(a < NUM_METAS && b < NUM_METAS);
  return troika->txnid[a] == troika->txnid[b] &&
         (((troika->fsm >> a) ^ (troika->fsm >> b)) & 1) == 0 &&
         troika->txnid[a];
}

static unsigned meta_eq_mask(const meta_troika_t *troika) {
  return meta_eq(troika, 0, 1) | meta_eq(troika, 1, 2) << 1 |
         meta_eq(troika, 2, 0) << 2;
}

__hot static bool meta_should_retry(const MDBX_env *env,
                                    meta_troika_t *troika) {
  const meta_troika_t prev = *troika;
  *troika = meta_tap(env);
  return prev.fsm != troika->fsm || prev.txnid[0] != troika->txnid[0] ||
         prev.txnid[1] != troika->txnid[1] || prev.txnid[2] != troika->txnid[2];
}

static __always_inline meta_ptr_t meta_recent(const MDBX_env *env,
                                              const meta_troika_t *troika) {
  meta_ptr_t r;
  r.txnid = troika->txnid[troika->recent];
  r.ptr_v = METAPAGE(env, troika->recent);
  r.is_steady = (troika->fsm >> troika->recent) & 1;
  return r;
}

static __always_inline meta_ptr_t
meta_prefer_steady(const MDBX_env *env, const meta_troika_t *troika) {
  meta_ptr_t r;
  r.txnid = troika->txnid[troika->prefer_steady];
  r.ptr_v = METAPAGE(env, troika->prefer_steady);
  r.is_steady = (troika->fsm >> troika->prefer_steady) & 1;
  return r;
}

static __always_inline meta_ptr_t meta_tail(const MDBX_env *env,
                                            const meta_troika_t *troika) {
  const uint8_t tail = troika->tail_and_flags & 3;
  MDBX_ANALYSIS_ASSUME(tail < NUM_METAS);
  meta_ptr_t r;
  r.txnid = troika->txnid[tail];
  r.ptr_v = METAPAGE(env, tail);
  r.is_steady = (troika->fsm >> tail) & 1;
  return r;
}

static const char *durable_caption(const volatile MDBX_meta *const meta) {
  if (META_IS_STEADY(meta))
    return (unaligned_peek_u64_volatile(4, meta->mm_sign) ==
            meta_sign((const MDBX_meta *)meta))
               ? "Steady"
               : "Tainted";
  return "Weak";
}

__cold static void meta_troika_dump(const MDBX_env *env,
                                    const meta_troika_t *troika) {
  const meta_ptr_t recent = meta_recent(env, troika);
  const meta_ptr_t prefer_steady = meta_prefer_steady(env, troika);
  const meta_ptr_t tail = meta_tail(env, troika);
  NOTICE("%" PRIaTXN ".%c:%" PRIaTXN ".%c:%" PRIaTXN ".%c, fsm=0x%02x, "
         "head=%d-%" PRIaTXN ".%c, "
         "base=%d-%" PRIaTXN ".%c, "
         "tail=%d-%" PRIaTXN ".%c, "
         "valid %c, strict %c",
         troika->txnid[0], (troika->fsm & 1) ? 's' : 'w', troika->txnid[1],
         (troika->fsm & 2) ? 's' : 'w', troika->txnid[2],
         (troika->fsm & 4) ? 's' : 'w', troika->fsm, troika->recent,
         recent.txnid, recent.is_steady ? 's' : 'w', troika->prefer_steady,
         prefer_steady.txnid, prefer_steady.is_steady ? 's' : 'w',
         troika->tail_and_flags % NUM_METAS, tail.txnid,
         tail.is_steady ? 's' : 'w', TROIKA_VALID(troika) ? 'Y' : 'N',
         TROIKA_STRICT_VALID(troika) ? 'Y' : 'N');
}

/*----------------------------------------------------------------------------*/

static __inline MDBX_CONST_FUNCTION MDBX_lockinfo *
lckless_stub(const MDBX_env *env) {
  uintptr_t stub = (uintptr_t)&env->x_lckless_stub;
  /* align to avoid false-positive alarm from UndefinedBehaviorSanitizer */
  stub = (stub + MDBX_CACHELINE_SIZE - 1) & ~(MDBX_CACHELINE_SIZE - 1);
  return (MDBX_lockinfo *)stub;
}

/* Find oldest txnid still referenced. */
static txnid_t find_oldest_reader(MDBX_env *const env, const txnid_t steady) {
  const uint32_t nothing_changed = MDBX_STRING_TETRAD("None");
  eASSERT(env, steady <= env->me_txn0->mt_txnid);

  MDBX_lockinfo *const lck = env->me_lck_mmap.lck;
  if (unlikely(lck == NULL /* exclusive without-lck mode */)) {
    eASSERT(env, env->me_lck == lckless_stub(env));
    env->me_lck->mti_readers_refresh_flag.weak = nothing_changed;
    return env->me_lck->mti_oldest_reader.weak = steady;
  }

  const txnid_t prev_oldest =
      atomic_load64(&lck->mti_oldest_reader, mo_AcquireRelease);
  eASSERT(env, steady >= prev_oldest);

  txnid_t new_oldest = prev_oldest;
  while (nothing_changed !=
         atomic_load32(&lck->mti_readers_refresh_flag, mo_AcquireRelease)) {
    lck->mti_readers_refresh_flag.weak = nothing_changed;
    jitter4testing(false);
    const size_t snap_nreaders =
        atomic_load32(&lck->mti_numreaders, mo_AcquireRelease);
    new_oldest = steady;

    for (size_t i = 0; i < snap_nreaders; ++i) {
      const uint32_t pid =
          atomic_load32(&lck->mti_readers[i].mr_pid, mo_AcquireRelease);
      if (!pid)
        continue;
      jitter4testing(true);

      const txnid_t rtxn = safe64_read(&lck->mti_readers[i].mr_txnid);
      if (unlikely(rtxn < prev_oldest)) {
        if (unlikely(nothing_changed ==
                     atomic_load32(&lck->mti_readers_refresh_flag,
                                   mo_AcquireRelease)) &&
            safe64_reset_compare(&lck->mti_readers[i].mr_txnid, rtxn)) {
          NOTICE("kick stuck reader[%zu of %zu].pid_%u %" PRIaTXN
                 " < prev-oldest %" PRIaTXN ", steady-txn %" PRIaTXN,
                 i, snap_nreaders, pid, rtxn, prev_oldest, steady);
        }
        continue;
      }

      if (rtxn < new_oldest) {
        new_oldest = rtxn;
        if (!MDBX_DEBUG && !MDBX_FORCE_ASSERTIONS && new_oldest == prev_oldest)
          break;
      }
    }
  }

  if (new_oldest != prev_oldest) {
    VERBOSE("update oldest %" PRIaTXN " -> %" PRIaTXN, prev_oldest, new_oldest);
    eASSERT(env, new_oldest >= lck->mti_oldest_reader.weak);
    atomic_store64(&lck->mti_oldest_reader, new_oldest, mo_Relaxed);
  }
  return new_oldest;
}

static txnid_t txn_oldest_reader(const MDBX_txn *const txn) {
  return find_oldest_reader(txn->mt_env,
                            txn->tw.troika.txnid[txn->tw.troika.prefer_steady]);
}

/* Find largest mvcc-snapshot still referenced. */
static pgno_t find_largest_snapshot(const MDBX_env *env,
                                    pgno_t last_used_page) {
  MDBX_lockinfo *const lck = env->me_lck_mmap.lck;
  if (likely(lck != NULL /* check for exclusive without-lck mode */)) {
  retry:;
    const size_t snap_nreaders =
        atomic_load32(&lck->mti_numreaders, mo_AcquireRelease);
    for (size_t i = 0; i < snap_nreaders; ++i) {
      if (atomic_load32(&lck->mti_readers[i].mr_pid, mo_AcquireRelease)) {
        /* jitter4testing(true); */
        const pgno_t snap_pages = atomic_load32(
            &lck->mti_readers[i].mr_snapshot_pages_used, mo_Relaxed);
        const txnid_t snap_txnid = safe64_read(&lck->mti_readers[i].mr_txnid);
        if (unlikely(
                snap_pages !=
                    atomic_load32(&lck->mti_readers[i].mr_snapshot_pages_used,
                                  mo_AcquireRelease) ||
                snap_txnid != safe64_read(&lck->mti_readers[i].mr_txnid)))
          goto retry;
        if (last_used_page < snap_pages && snap_txnid <= env->me_txn0->mt_txnid)
          last_used_page = snap_pages;
      }
    }
  }

  return last_used_page;
}

/* Add a page to the txn's dirty list */
__hot static int __must_check_result page_dirty(MDBX_txn *txn, MDBX_page *mp,
                                                size_t npages) {
  tASSERT(txn, (txn->mt_flags & MDBX_TXN_RDONLY) == 0);
  mp->mp_txnid = txn->mt_front;
  if (!txn->tw.dirtylist) {
    tASSERT(txn, (txn->mt_flags & MDBX_WRITEMAP) != 0 && !MDBX_AVOID_MSYNC);
    txn->tw.writemap_dirty_npages += npages;
    tASSERT(txn, txn->tw.spilled.list == nullptr);
    return MDBX_SUCCESS;
  }
  tASSERT(txn, (txn->mt_flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);

#if xMDBX_DEBUG_SPILLING == 2
  txn->mt_env->debug_dirtied_act += 1;
  ENSURE(txn->mt_env,
         txn->mt_env->debug_dirtied_act < txn->mt_env->debug_dirtied_est);
  ENSURE(txn->mt_env, txn->tw.dirtyroom + txn->tw.loose_count > 0);
#endif /* xMDBX_DEBUG_SPILLING == 2 */

  int rc;
  if (unlikely(txn->tw.dirtyroom == 0)) {
    if (txn->tw.loose_count) {
      MDBX_page *lp = txn->tw.loose_pages;
      DEBUG("purge-and-reclaim loose page %" PRIaPGNO, lp->mp_pgno);
      rc = pnl_insert_range(&txn->tw.relist, lp->mp_pgno, 1);
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;
      size_t di = dpl_search(txn, lp->mp_pgno);
      tASSERT(txn, txn->tw.dirtylist->items[di].ptr == lp);
      dpl_remove(txn, di);
      MDBX_ASAN_UNPOISON_MEMORY_REGION(&mp_next(lp), sizeof(MDBX_page *));
      VALGRIND_MAKE_MEM_DEFINED(&mp_next(lp), sizeof(MDBX_page *));
      txn->tw.loose_pages = mp_next(lp);
      txn->tw.loose_count--;
      txn->tw.dirtyroom++;
      if (!MDBX_AVOID_MSYNC || !(txn->mt_flags & MDBX_WRITEMAP))
        dpage_free(txn->mt_env, lp, 1);
    } else {
      ERROR("Dirtyroom is depleted, DPL length %zu", txn->tw.dirtylist->length);
      if (!MDBX_AVOID_MSYNC || !(txn->mt_flags & MDBX_WRITEMAP))
        dpage_free(txn->mt_env, mp, npages);
      return MDBX_TXN_FULL;
    }
  }

  rc = dpl_append(txn, mp->mp_pgno, mp, npages);
  if (unlikely(rc != MDBX_SUCCESS)) {
  bailout:
    txn->mt_flags |= MDBX_TXN_ERROR;
    return rc;
  }
  txn->tw.dirtyroom--;
  tASSERT(txn, dirtylist_check(txn));
  return MDBX_SUCCESS;
}

static void mincore_clean_cache(const MDBX_env *const env) {
  memset(env->me_lck->mti_mincore_cache.begin, -1,
         sizeof(env->me_lck->mti_mincore_cache.begin));
}

#if !(defined(_WIN32) || defined(_WIN64))
MDBX_MAYBE_UNUSED static __always_inline int ignore_enosys(int err) {
#ifdef ENOSYS
  if (err == ENOSYS)
    return MDBX_RESULT_TRUE;
#endif /* ENOSYS */
#ifdef ENOIMPL
  if (err == ENOIMPL)
    return MDBX_RESULT_TRUE;
#endif /* ENOIMPL */
#ifdef ENOTSUP
  if (err == ENOTSUP)
    return MDBX_RESULT_TRUE;
#endif /* ENOTSUP */
#ifdef ENOSUPP
  if (err == ENOSUPP)
    return MDBX_RESULT_TRUE;
#endif /* ENOSUPP */
#ifdef EOPNOTSUPP
  if (err == EOPNOTSUPP)
    return MDBX_RESULT_TRUE;
#endif /* EOPNOTSUPP */
  if (err == EAGAIN)
    return MDBX_RESULT_TRUE;
  return err;
}
#endif /* defined(_WIN32) || defined(_WIN64) */

#if MDBX_ENABLE_MADVISE
/* Turn on/off readahead. It's harmful when the DB is larger than RAM. */
__cold static int set_readahead(const MDBX_env *env, const pgno_t edge,
                                const bool enable, const bool force_whole) {
  eASSERT(env, edge >= NUM_METAS && edge <= MAX_PAGENO + 1);
  eASSERT(env, (enable & 1) == (enable != 0));
  const bool toggle = force_whole ||
                      ((enable ^ env->me_lck->mti_readahead_anchor) & 1) ||
                      !env->me_lck->mti_readahead_anchor;
  const pgno_t prev_edge = env->me_lck->mti_readahead_anchor >> 1;
  const size_t limit = env->me_dxb_mmap.limit;
  size_t offset =
      toggle ? 0
             : pgno_align2os_bytes(env, (prev_edge < edge) ? prev_edge : edge);
  offset = (offset < limit) ? offset : limit;

  size_t length =
      pgno_align2os_bytes(env, (prev_edge < edge) ? edge : prev_edge);
  length = (length < limit) ? length : limit;
  length -= offset;

  eASSERT(env, 0 <= (intptr_t)length);
  if (length == 0)
    return MDBX_SUCCESS;

  NOTICE("readahead %s %u..%u", enable ? "ON" : "OFF", bytes2pgno(env, offset),
         bytes2pgno(env, offset + length));

#if defined(F_RDAHEAD)
  if (toggle && unlikely(fcntl(env->me_lazy_fd, F_RDAHEAD, enable) == -1))
    return errno;
#endif /* F_RDAHEAD */

  int err;
  void *const ptr = ptr_disp(env->me_map, offset);
  if (enable) {
#if defined(MADV_NORMAL)
    err =
        madvise(ptr, length, MADV_NORMAL) ? ignore_enosys(errno) : MDBX_SUCCESS;
    if (unlikely(MDBX_IS_ERROR(err)))
      return err;
#elif defined(POSIX_MADV_NORMAL)
    err = ignore_enosys(posix_madvise(ptr, length, POSIX_MADV_NORMAL));
    if (unlikely(MDBX_IS_ERROR(err)))
      return err;
#elif defined(POSIX_FADV_NORMAL) && defined(POSIX_FADV_WILLNEED)
    err = ignore_enosys(
        posix_fadvise(env->me_lazy_fd, offset, length, POSIX_FADV_NORMAL));
    if (unlikely(MDBX_IS_ERROR(err)))
      return err;
#elif defined(_WIN32) || defined(_WIN64)
    /* no madvise on Windows */
#else
#warning "FIXME"
#endif
    if (toggle) {
      /* NOTE: Seems there is a bug in the Mach/Darwin/OSX kernel,
       * because MADV_WILLNEED with offset != 0 may cause SIGBUS
       * on following access to the hinted region.
       * 19.6.0 Darwin Kernel Version 19.6.0: Tue Jan 12 22:13:05 PST 2021;
       * root:xnu-6153.141.16~1/RELEASE_X86_64 x86_64 */
#if defined(F_RDADVISE)
      struct radvisory hint;
      hint.ra_offset = offset;
      hint.ra_count =
          unlikely(length > INT_MAX && sizeof(length) > sizeof(hint.ra_count))
              ? INT_MAX
              : (int)length;
      (void)/* Ignore ENOTTY for DB on the ram-disk and so on */ fcntl(
          env->me_lazy_fd, F_RDADVISE, &hint);
#elif defined(MADV_WILLNEED)
      err = madvise(ptr, length, MADV_WILLNEED) ? ignore_enosys(errno)
                                                : MDBX_SUCCESS;
      if (unlikely(MDBX_IS_ERROR(err)))
        return err;
#elif defined(POSIX_MADV_WILLNEED)
      err = ignore_enosys(posix_madvise(ptr, length, POSIX_MADV_WILLNEED));
      if (unlikely(MDBX_IS_ERROR(err)))
        return err;
#elif defined(_WIN32) || defined(_WIN64)
      if (mdbx_PrefetchVirtualMemory) {
        WIN32_MEMORY_RANGE_ENTRY hint;
        hint.VirtualAddress = ptr;
        hint.NumberOfBytes = length;
        (void)mdbx_PrefetchVirtualMemory(GetCurrentProcess(), 1, &hint, 0);
      }
#elif defined(POSIX_FADV_WILLNEED)
      err = ignore_enosys(
          posix_fadvise(env->me_lazy_fd, offset, length, POSIX_FADV_WILLNEED));
      if (unlikely(MDBX_IS_ERROR(err)))
        return err;
#else
#warning "FIXME"
#endif
    }
  } else {
    mincore_clean_cache(env);
#if defined(MADV_RANDOM)
    err =
        madvise(ptr, length, MADV_RANDOM) ? ignore_enosys(errno) : MDBX_SUCCESS;
    if (unlikely(MDBX_IS_ERROR(err)))
      return err;
#elif defined(POSIX_MADV_RANDOM)
    err = ignore_enosys(posix_madvise(ptr, length, POSIX_MADV_RANDOM));
    if (unlikely(MDBX_IS_ERROR(err)))
      return err;
#elif defined(POSIX_FADV_RANDOM)
    err = ignore_enosys(
        posix_fadvise(env->me_lazy_fd, offset, length, POSIX_FADV_RANDOM));
    if (unlikely(MDBX_IS_ERROR(err)))
      return err;
#elif defined(_WIN32) || defined(_WIN64)
    /* no madvise on Windows */
#else
#warning "FIXME"
#endif /* MADV_RANDOM */
  }

  env->me_lck->mti_readahead_anchor = (enable & 1) + (edge << 1);
  err = MDBX_SUCCESS;
  return err;
}
#endif /* MDBX_ENABLE_MADVISE */

__cold static void update_mlcnt(const MDBX_env *env,
                                const pgno_t new_aligned_mlocked_pgno,
                                const bool lock_not_release) {
  for (;;) {
    const pgno_t mlock_pgno_before =
        atomic_load32(&env->me_mlocked_pgno, mo_AcquireRelease);
    eASSERT(env,
            pgno_align2os_pgno(env, mlock_pgno_before) == mlock_pgno_before);
    eASSERT(env, pgno_align2os_pgno(env, new_aligned_mlocked_pgno) ==
                     new_aligned_mlocked_pgno);
    if (lock_not_release ? (mlock_pgno_before >= new_aligned_mlocked_pgno)
                         : (mlock_pgno_before <= new_aligned_mlocked_pgno))
      break;
    if (likely(atomic_cas32(&((MDBX_env *)env)->me_mlocked_pgno,
                            mlock_pgno_before, new_aligned_mlocked_pgno)))
      for (;;) {
        MDBX_atomic_uint32_t *const mlcnt = env->me_lck->mti_mlcnt;
        const int32_t snap_locked = atomic_load32(mlcnt + 0, mo_Relaxed);
        const int32_t snap_unlocked = atomic_load32(mlcnt + 1, mo_Relaxed);
        if (mlock_pgno_before == 0 && (snap_locked - snap_unlocked) < INT_MAX) {
          eASSERT(env, lock_not_release);
          if (unlikely(!atomic_cas32(mlcnt + 0, snap_locked, snap_locked + 1)))
            continue;
        }
        if (new_aligned_mlocked_pgno == 0 &&
            (snap_locked - snap_unlocked) > 0) {
          eASSERT(env, !lock_not_release);
          if (unlikely(
                  !atomic_cas32(mlcnt + 1, snap_unlocked, snap_unlocked + 1)))
            continue;
        }
        NOTICE("%s-pages %u..%u, mlocked-process(es) %u -> %u",
               lock_not_release ? "lock" : "unlock",
               lock_not_release ? mlock_pgno_before : new_aligned_mlocked_pgno,
               lock_not_release ? new_aligned_mlocked_pgno : mlock_pgno_before,
               snap_locked - snap_unlocked,
               atomic_load32(mlcnt + 0, mo_Relaxed) -
                   atomic_load32(mlcnt + 1, mo_Relaxed));
        return;
      }
  }
}

__cold static void munlock_after(const MDBX_env *env, const pgno_t aligned_pgno,
                                 const size_t end_bytes) {
  if (atomic_load32(&env->me_mlocked_pgno, mo_AcquireRelease) > aligned_pgno) {
    int err = MDBX_ENOSYS;
    const size_t munlock_begin = pgno2bytes(env, aligned_pgno);
    const size_t munlock_size = end_bytes - munlock_begin;
    eASSERT(env, end_bytes % env->me_os_psize == 0 &&
                     munlock_begin % env->me_os_psize == 0 &&
                     munlock_size % env->me_os_psize == 0);
#if defined(_WIN32) || defined(_WIN64)
    err = VirtualUnlock(ptr_disp(env->me_map, munlock_begin), munlock_size)
              ? MDBX_SUCCESS
              : (int)GetLastError();
    if (err == ERROR_NOT_LOCKED)
      err = MDBX_SUCCESS;
#elif defined(_POSIX_MEMLOCK_RANGE)
    err = munlock(ptr_disp(env->me_map, munlock_begin), munlock_size)
              ? errno
              : MDBX_SUCCESS;
#endif
    if (likely(err == MDBX_SUCCESS))
      update_mlcnt(env, aligned_pgno, false);
    else {
#if defined(_WIN32) || defined(_WIN64)
      WARNING("VirtualUnlock(%zu, %zu) error %d", munlock_begin, munlock_size,
              err);
#else
      WARNING("munlock(%zu, %zu) error %d", munlock_begin, munlock_size, err);
#endif
    }
  }
}

__cold static void munlock_all(const MDBX_env *env) {
  munlock_after(env, 0, bytes_align2os_bytes(env, env->me_dxb_mmap.current));
}

__cold static unsigned default_rp_augment_limit(const MDBX_env *env) {
  /* default rp_augment_limit = npages / 3 */
  const size_t augment = env->me_dbgeo.now / 3 >> env->me_psize2log;
  eASSERT(env, augment < MDBX_PGL_LIMIT);
  return pnl_bytes2size(pnl_size2bytes(
      (augment > MDBX_PNL_INITIAL) ? augment : MDBX_PNL_INITIAL));
}

static bool default_prefault_write(const MDBX_env *env) {
  return !MDBX_MMAP_INCOHERENT_FILE_WRITE && !env->me_incore &&
         (env->me_flags & (MDBX_WRITEMAP | MDBX_RDONLY)) == MDBX_WRITEMAP;
}

static void adjust_defaults(MDBX_env *env) {
  if (!env->me_options.flags.non_auto.rp_augment_limit)
    env->me_options.rp_augment_limit = default_rp_augment_limit(env);
  if (!env->me_options.flags.non_auto.prefault_write)
    env->me_options.prefault_write = default_prefault_write(env);

  const size_t basis = env->me_dbgeo.now;
  /* TODO: use options? */
  const unsigned factor = 9;
  size_t threshold = (basis < ((size_t)65536 << factor))
                         ? 65536 /* minimal threshold */
                     : (basis > (MEGABYTE * 4 << factor))
                         ? MEGABYTE * 4 /* maximal threshold */
                         : basis >> factor;
  threshold = (threshold < env->me_dbgeo.shrink || !env->me_dbgeo.shrink)
                  ? threshold
                  : env->me_dbgeo.shrink;

  env->me_madv_threshold =
      bytes2pgno(env, bytes_align2os_bytes(env, threshold));
}

enum resize_mode { implicit_grow, impilict_shrink, explicit_resize };

__cold static int dxb_resize(MDBX_env *const env, const pgno_t used_pgno,
                             const pgno_t size_pgno, pgno_t limit_pgno,
                             const enum resize_mode mode) {
  /* Acquire guard to avoid collision between read and write txns
   * around me_dbgeo and me_dxb_mmap */
#if defined(_WIN32) || defined(_WIN64)
  osal_srwlock_AcquireExclusive(&env->me_remap_guard);
  int rc = MDBX_SUCCESS;
  mdbx_handle_array_t *suspended = NULL;
  mdbx_handle_array_t array_onstack;
#else
  int rc = osal_fastmutex_acquire(&env->me_remap_guard);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;
#endif

  const size_t prev_size = env->me_dxb_mmap.current;
  const size_t prev_limit = env->me_dxb_mmap.limit;
  const pgno_t prev_limit_pgno = bytes2pgno(env, prev_limit);
  eASSERT(env, limit_pgno >= size_pgno);
  eASSERT(env, size_pgno >= used_pgno);
  if (mode < explicit_resize && size_pgno <= prev_limit_pgno) {
    /* The actual mapsize may be less since the geo.upper may be changed
     * by other process. Avoids remapping until it necessary. */
    limit_pgno = prev_limit_pgno;
  }
  const size_t limit_bytes = pgno_align2os_bytes(env, limit_pgno);
  const size_t size_bytes = pgno_align2os_bytes(env, size_pgno);
#if MDBX_ENABLE_MADVISE || defined(MDBX_USE_VALGRIND)
  const void *const prev_map = env->me_dxb_mmap.base;
#endif /* MDBX_ENABLE_MADVISE || MDBX_USE_VALGRIND */

  VERBOSE("resize/%d datafile/mapping: "
          "present %" PRIuPTR " -> %" PRIuPTR ", "
          "limit %" PRIuPTR " -> %" PRIuPTR,
          mode, prev_size, size_bytes, prev_limit, limit_bytes);

  eASSERT(env, limit_bytes >= size_bytes);
  eASSERT(env, bytes2pgno(env, size_bytes) >= size_pgno);
  eASSERT(env, bytes2pgno(env, limit_bytes) >= limit_pgno);

  unsigned mresize_flags =
      env->me_flags & (MDBX_RDONLY | MDBX_WRITEMAP | MDBX_UTTERLY_NOSYNC);
  if (mode >= impilict_shrink)
    mresize_flags |= MDBX_SHRINK_ALLOWED;

  if (limit_bytes == env->me_dxb_mmap.limit &&
      size_bytes == env->me_dxb_mmap.current &&
      size_bytes == env->me_dxb_mmap.filesize)
    goto bailout;

#if defined(_WIN32) || defined(_WIN64)
  if ((env->me_flags & MDBX_NOTLS) == 0 &&
      ((size_bytes < env->me_dxb_mmap.current && mode > implicit_grow) ||
       limit_bytes != env->me_dxb_mmap.limit)) {
    /* 1) Windows allows only extending a read-write section, but not a
     *    corresponding mapped view. Therefore in other cases we must suspend
     *    the local threads for safe remap.
     * 2) At least on Windows 10 1803 the entire mapped section is unavailable
     *    for short time during NtExtendSection() or VirtualAlloc() execution.
     * 3) Under Wine runtime environment on Linux a section extending is not
     *    supported.
     *
     * THEREFORE LOCAL THREADS SUSPENDING IS ALWAYS REQUIRED! */
    array_onstack.limit = ARRAY_LENGTH(array_onstack.handles);
    array_onstack.count = 0;
    suspended = &array_onstack;
    rc = osal_suspend_threads_before_remap(env, &suspended);
    if (rc != MDBX_SUCCESS) {
      ERROR("failed suspend-for-remap: errcode %d", rc);
      goto bailout;
    }
    mresize_flags |= (mode < explicit_resize)
                         ? MDBX_MRESIZE_MAY_UNMAP
                         : MDBX_MRESIZE_MAY_UNMAP | MDBX_MRESIZE_MAY_MOVE;
  }
#else  /* Windows */
  MDBX_lockinfo *const lck = env->me_lck_mmap.lck;
  if (mode == explicit_resize && limit_bytes != env->me_dxb_mmap.limit &&
      !(env->me_flags & MDBX_NOTLS)) {
    mresize_flags |= MDBX_MRESIZE_MAY_UNMAP | MDBX_MRESIZE_MAY_MOVE;
    if (lck) {
      int err = osal_rdt_lock(env) /* lock readers table until remap done */;
      if (unlikely(MDBX_IS_ERROR(err))) {
        rc = err;
        goto bailout;
      }

      /* looking for readers from this process */
      const size_t snap_nreaders =
          atomic_load32(&lck->mti_numreaders, mo_AcquireRelease);
      eASSERT(env, mode == explicit_resize);
      for (size_t i = 0; i < snap_nreaders; ++i) {
        if (lck->mti_readers[i].mr_pid.weak == env->me_pid &&
            lck->mti_readers[i].mr_tid.weak != osal_thread_self()) {
          /* the base address of the mapping can't be changed since
           * the other reader thread from this process exists. */
          osal_rdt_unlock(env);
          mresize_flags &= ~(MDBX_MRESIZE_MAY_UNMAP | MDBX_MRESIZE_MAY_MOVE);
          break;
        }
      }
    }
  }
#endif /* ! Windows */

  const pgno_t aligned_munlock_pgno =
      (mresize_flags & (MDBX_MRESIZE_MAY_UNMAP | MDBX_MRESIZE_MAY_MOVE))
          ? 0
          : bytes2pgno(env, size_bytes);
  if (mresize_flags & (MDBX_MRESIZE_MAY_UNMAP | MDBX_MRESIZE_MAY_MOVE)) {
    mincore_clean_cache(env);
    if ((env->me_flags & MDBX_WRITEMAP) &&
        env->me_lck->mti_unsynced_pages.weak) {
#if MDBX_ENABLE_PGOP_STAT
      env->me_lck->mti_pgop_stat.msync.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
      rc = osal_msync(&env->me_dxb_mmap, 0, pgno_align2os_bytes(env, used_pgno),
                      MDBX_SYNC_NONE);
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;
    }
  }
  munlock_after(env, aligned_munlock_pgno, size_bytes);

#if MDBX_ENABLE_MADVISE
  if (size_bytes < prev_size && mode > implicit_grow) {
    NOTICE("resize-MADV_%s %u..%u",
           (env->me_flags & MDBX_WRITEMAP) ? "REMOVE" : "DONTNEED", size_pgno,
           bytes2pgno(env, prev_size));
    const uint32_t munlocks_before =
        atomic_load32(&env->me_lck->mti_mlcnt[1], mo_Relaxed);
    rc = MDBX_RESULT_TRUE;
#if defined(MADV_REMOVE)
    if (env->me_flags & MDBX_WRITEMAP)
      rc = madvise(ptr_disp(env->me_map, size_bytes), prev_size - size_bytes,
                   MADV_REMOVE)
               ? ignore_enosys(errno)
               : MDBX_SUCCESS;
#endif /* MADV_REMOVE */
#if defined(MADV_DONTNEED)
    if (rc == MDBX_RESULT_TRUE)
      rc = madvise(ptr_disp(env->me_map, size_bytes), prev_size - size_bytes,
                   MADV_DONTNEED)
               ? ignore_enosys(errno)
               : MDBX_SUCCESS;
#elif defined(POSIX_MADV_DONTNEED)
    if (rc == MDBX_RESULT_TRUE)
      rc = ignore_enosys(posix_madvise(ptr_disp(env->me_map, size_bytes),
                                       prev_size - size_bytes,
                                       POSIX_MADV_DONTNEED));
#elif defined(POSIX_FADV_DONTNEED)
    if (rc == MDBX_RESULT_TRUE)
      rc = ignore_enosys(posix_fadvise(env->me_lazy_fd, size_bytes,
                                       prev_size - size_bytes,
                                       POSIX_FADV_DONTNEED));
#endif /* MADV_DONTNEED */
    if (unlikely(MDBX_IS_ERROR(rc))) {
      const uint32_t mlocks_after =
          atomic_load32(&env->me_lck->mti_mlcnt[0], mo_Relaxed);
      if (rc == MDBX_EINVAL) {
        const int severity =
            (mlocks_after - munlocks_before) ? MDBX_LOG_NOTICE : MDBX_LOG_WARN;
        if (LOG_ENABLED(severity))
          debug_log(severity, __func__, __LINE__,
                    "%s-madvise: ignore EINVAL (%d) since some pages maybe "
                    "locked (%u/%u mlcnt-processes)",
                    "resize", rc, mlocks_after, munlocks_before);
      } else {
        ERROR("%s-madvise(%s, %zu, +%zu), %u/%u mlcnt-processes, err %d",
              "mresize", "DONTNEED", size_bytes, prev_size - size_bytes,
              mlocks_after, munlocks_before, rc);
        goto bailout;
      }
    } else
      env->me_lck->mti_discarded_tail.weak = size_pgno;
  }
#endif /* MDBX_ENABLE_MADVISE */

  rc = osal_mresize(mresize_flags, &env->me_dxb_mmap, size_bytes, limit_bytes);
  eASSERT(env, env->me_dxb_mmap.limit >= env->me_dxb_mmap.current);

#if MDBX_ENABLE_MADVISE
  if (rc == MDBX_SUCCESS) {
    eASSERT(env, limit_bytes == env->me_dxb_mmap.limit);
    eASSERT(env, size_bytes <= env->me_dxb_mmap.filesize);
    if (mode == explicit_resize)
      eASSERT(env, size_bytes == env->me_dxb_mmap.current);
    else
      eASSERT(env, size_bytes <= env->me_dxb_mmap.current);
    env->me_lck->mti_discarded_tail.weak = size_pgno;
    const bool readahead =
        !(env->me_flags & MDBX_NORDAHEAD) &&
        mdbx_is_readahead_reasonable(size_bytes, -(intptr_t)prev_size);
    const bool force = limit_bytes != prev_limit ||
                       env->me_dxb_mmap.base != prev_map
#if defined(_WIN32) || defined(_WIN64)
                       || prev_size > size_bytes
#endif /* Windows */
        ;
    rc = set_readahead(env, size_pgno, readahead, force);
  }
#endif /* MDBX_ENABLE_MADVISE */

bailout:
  if (rc == MDBX_SUCCESS) {
    eASSERT(env, env->me_dxb_mmap.limit >= env->me_dxb_mmap.current);
    eASSERT(env, limit_bytes == env->me_dxb_mmap.limit);
    eASSERT(env, size_bytes <= env->me_dxb_mmap.filesize);
    if (mode == explicit_resize)
      eASSERT(env, size_bytes == env->me_dxb_mmap.current);
    else
      eASSERT(env, size_bytes <= env->me_dxb_mmap.current);
    /* update env-geo to avoid influences */
    env->me_dbgeo.now = env->me_dxb_mmap.current;
    env->me_dbgeo.upper = env->me_dxb_mmap.limit;
    adjust_defaults(env);
#ifdef MDBX_USE_VALGRIND
    if (prev_limit != env->me_dxb_mmap.limit || prev_map != env->me_map) {
      VALGRIND_DISCARD(env->me_valgrind_handle);
      env->me_valgrind_handle = 0;
      if (env->me_dxb_mmap.limit)
        env->me_valgrind_handle =
            VALGRIND_CREATE_BLOCK(env->me_map, env->me_dxb_mmap.limit, "mdbx");
    }
#endif /* MDBX_USE_VALGRIND */
  } else {
    if (rc != MDBX_UNABLE_EXTEND_MAPSIZE && rc != MDBX_EPERM) {
      ERROR("failed resize datafile/mapping: "
            "present %" PRIuPTR " -> %" PRIuPTR ", "
            "limit %" PRIuPTR " -> %" PRIuPTR ", errcode %d",
            prev_size, size_bytes, prev_limit, limit_bytes, rc);
    } else {
      WARNING("unable resize datafile/mapping: "
              "present %" PRIuPTR " -> %" PRIuPTR ", "
              "limit %" PRIuPTR " -> %" PRIuPTR ", errcode %d",
              prev_size, size_bytes, prev_limit, limit_bytes, rc);
      eASSERT(env, env->me_dxb_mmap.limit >= env->me_dxb_mmap.current);
    }
    if (!env->me_dxb_mmap.base) {
      env->me_flags |= MDBX_FATAL_ERROR;
      if (env->me_txn)
        env->me_txn->mt_flags |= MDBX_TXN_ERROR;
      rc = MDBX_PANIC;
    }
  }

#if defined(_WIN32) || defined(_WIN64)
  int err = MDBX_SUCCESS;
  osal_srwlock_ReleaseExclusive(&env->me_remap_guard);
  if (suspended) {
    err = osal_resume_threads_after_remap(suspended);
    if (suspended != &array_onstack)
      osal_free(suspended);
  }
#else
  if (env->me_lck_mmap.lck &&
      (mresize_flags & (MDBX_MRESIZE_MAY_UNMAP | MDBX_MRESIZE_MAY_MOVE)) != 0)
    osal_rdt_unlock(env);
  int err = osal_fastmutex_release(&env->me_remap_guard);
#endif /* Windows */
  if (err != MDBX_SUCCESS) {
    FATAL("failed resume-after-remap: errcode %d", err);
    return MDBX_PANIC;
  }
  return rc;
}

static int meta_unsteady(int err, MDBX_env *env, const txnid_t early_than,
                         const pgno_t pgno) {
  MDBX_meta *const meta = METAPAGE(env, pgno);
  const txnid_t txnid = constmeta_txnid(meta);
  if (unlikely(err != MDBX_SUCCESS) || !META_IS_STEADY(meta) ||
      !(txnid < early_than))
    return err;

  WARNING("wipe txn #%" PRIaTXN ", meta %" PRIaPGNO, txnid, pgno);
  const uint64_t wipe = MDBX_DATASIGN_NONE;
  const void *ptr = &wipe;
  size_t bytes = sizeof(meta->mm_sign),
         offset = ptr_dist(&meta->mm_sign, env->me_map);
  if (env->me_flags & MDBX_WRITEMAP) {
    unaligned_poke_u64(4, meta->mm_sign, wipe);
    osal_flush_incoherent_cpu_writeback();
    if (!MDBX_AVOID_MSYNC) {
      err =
          osal_msync(&env->me_dxb_mmap, 0, pgno_align2os_bytes(env, NUM_METAS),
                     MDBX_SYNC_DATA | MDBX_SYNC_IODQ);
#if MDBX_ENABLE_PGOP_STAT
      env->me_lck->mti_pgop_stat.msync.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
      return err;
    }
    ptr = data_page(meta);
    offset = ptr_dist(ptr, env->me_map);
    bytes = env->me_psize;
  }

#if MDBX_ENABLE_PGOP_STAT
  env->me_lck->mti_pgop_stat.wops.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
  err = osal_pwrite(env->me_fd4meta, ptr, bytes, offset);
  if (likely(err == MDBX_SUCCESS) && env->me_fd4meta == env->me_lazy_fd) {
    err = osal_fsync(env->me_lazy_fd, MDBX_SYNC_DATA | MDBX_SYNC_IODQ);
#if MDBX_ENABLE_PGOP_STAT
    env->me_lck->mti_pgop_stat.fsync.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
  }
  return err;
}

__cold static int wipe_steady(MDBX_txn *txn, txnid_t last_steady) {
  MDBX_env *const env = txn->mt_env;
  int err = MDBX_SUCCESS;

  /* early than last_steady */
  err = meta_unsteady(err, env, last_steady, 0);
  err = meta_unsteady(err, env, last_steady, 1);
  err = meta_unsteady(err, env, last_steady, 2);

  /* the last_steady */
  err = meta_unsteady(err, env, last_steady + 1, 0);
  err = meta_unsteady(err, env, last_steady + 1, 1);
  err = meta_unsteady(err, env, last_steady + 1, 2);

  osal_flush_incoherent_mmap(env->me_map, pgno2bytes(env, NUM_METAS),
                             env->me_os_psize);

  /* force oldest refresh */
  atomic_store32(&env->me_lck->mti_readers_refresh_flag, true, mo_Relaxed);

  tASSERT(txn, (txn->mt_flags & MDBX_TXN_RDONLY) == 0);
  txn->tw.troika = meta_tap(env);
  for (MDBX_txn *scan = txn->mt_env->me_txn0; scan; scan = scan->mt_child)
    if (scan != txn)
      scan->tw.troika = txn->tw.troika;
  return err;
}

//------------------------------------------------------------------------------

MDBX_MAYBE_UNUSED __hot static pgno_t *
scan4seq_fallback(pgno_t *range, const size_t len, const size_t seq) {
  assert(seq > 0 && len > seq);
#if MDBX_PNL_ASCENDING
  assert(range[-1] == len);
  const pgno_t *const detent = range + len - seq;
  const ptrdiff_t offset = (ptrdiff_t)seq;
  const pgno_t target = (pgno_t)offset;
  if (likely(len > seq + 3)) {
    do {
      const pgno_t diff0 = range[offset + 0] - range[0];
      const pgno_t diff1 = range[offset + 1] - range[1];
      const pgno_t diff2 = range[offset + 2] - range[2];
      const pgno_t diff3 = range[offset + 3] - range[3];
      if (diff0 == target)
        return range + 0;
      if (diff1 == target)
        return range + 1;
      if (diff2 == target)
        return range + 2;
      if (diff3 == target)
        return range + 3;
      range += 4;
    } while (range + 3 < detent);
    if (range == detent)
      return nullptr;
  }
  do
    if (range[offset] - *range == target)
      return range;
  while (++range < detent);
#else
  assert(range[-(ptrdiff_t)len] == len);
  const pgno_t *const detent = range - len + seq;
  const ptrdiff_t offset = -(ptrdiff_t)seq;
  const pgno_t target = (pgno_t)offset;
  if (likely(len > seq + 3)) {
    do {
      const pgno_t diff0 = range[-0] - range[offset - 0];
      const pgno_t diff1 = range[-1] - range[offset - 1];
      const pgno_t diff2 = range[-2] - range[offset - 2];
      const pgno_t diff3 = range[-3] - range[offset - 3];
      /* Смысл вычислений до ветвлений в том, чтобы позволить компилятору
       * загружать и вычислять все значения параллельно. */
      if (diff0 == target)
        return range - 0;
      if (diff1 == target)
        return range - 1;
      if (diff2 == target)
        return range - 2;
      if (diff3 == target)
        return range - 3;
      range -= 4;
    } while (range > detent + 3);
    if (range == detent)
      return nullptr;
  }
  do
    if (*range - range[offset] == target)
      return range;
  while (--range > detent);
#endif /* MDBX_PNL sort-order */
  return nullptr;
}

MDBX_MAYBE_UNUSED static const pgno_t *scan4range_checker(const MDBX_PNL pnl,
                                                          const size_t seq) {
  size_t begin = MDBX_PNL_ASCENDING ? 1 : MDBX_PNL_GETSIZE(pnl);
#if MDBX_PNL_ASCENDING
  while (seq <= MDBX_PNL_GETSIZE(pnl) - begin) {
    if (pnl[begin + seq] - pnl[begin] == seq)
      return pnl + begin;
    ++begin;
  }
#else
  while (begin > seq) {
    if (pnl[begin - seq] - pnl[begin] == seq)
      return pnl + begin;
    --begin;
  }
#endif /* MDBX_PNL sort-order */
  return nullptr;
}

#if defined(_MSC_VER) && !defined(__builtin_clz) &&                            \
    !__has_builtin(__builtin_clz)
MDBX_MAYBE_UNUSED static __always_inline size_t __builtin_clz(uint32_t value) {
  unsigned long index;
  _BitScanReverse(&index, value);
  return 31 - index;
}
#endif /* _MSC_VER */

#if defined(_MSC_VER) && !defined(__builtin_clzl) &&                           \
    !__has_builtin(__builtin_clzl)
MDBX_MAYBE_UNUSED static __always_inline size_t __builtin_clzl(size_t value) {
  unsigned long index;
#ifdef _WIN64
  assert(sizeof(value) == 8);
  _BitScanReverse64(&index, value);
  return 63 - index;
#else
  assert(sizeof(value) == 4);
  _BitScanReverse(&index, value);
  return 31 - index;
#endif
}
#endif /* _MSC_VER */

#if !MDBX_PNL_ASCENDING

#if !defined(MDBX_ATTRIBUTE_TARGET) &&                                         \
    (__has_attribute(__target__) || __GNUC_PREREQ(5, 0))
#define MDBX_ATTRIBUTE_TARGET(target) __attribute__((__target__(target)))
#endif /* MDBX_ATTRIBUTE_TARGET */

#ifndef MDBX_GCC_FASTMATH_i686_SIMD_WORKAROUND
/* Workaround for GCC's bug with `-m32 -march=i686 -Ofast`
 * gcc/i686-buildroot-linux-gnu/12.2.0/include/xmmintrin.h:814:1:
 *     error: inlining failed in call to 'always_inline' '_mm_movemask_ps':
 *            target specific option mismatch */
#if !defined(__FAST_MATH__) || !__FAST_MATH__ || !defined(__GNUC__) ||         \
    defined(__e2k__) || defined(__clang__) || defined(__amd64__) ||            \
    defined(__SSE2__)
#define MDBX_GCC_FASTMATH_i686_SIMD_WORKAROUND 0
#else
#define MDBX_GCC_FASTMATH_i686_SIMD_WORKAROUND 1
#endif
#endif /* MDBX_GCC_FASTMATH_i686_SIMD_WORKAROUND */

#if defined(__SSE2__) && defined(__SSE__)
#define MDBX_ATTRIBUTE_TARGET_SSE2 /* nope */
#elif (defined(_M_IX86_FP) && _M_IX86_FP >= 2) || defined(__amd64__)
#define __SSE2__
#define MDBX_ATTRIBUTE_TARGET_SSE2 /* nope */
#elif defined(MDBX_ATTRIBUTE_TARGET) && defined(__ia32__) &&                   \
    !MDBX_GCC_FASTMATH_i686_SIMD_WORKAROUND
#define MDBX_ATTRIBUTE_TARGET_SSE2 MDBX_ATTRIBUTE_TARGET("sse,sse2")
#endif /* __SSE2__ */

#if defined(__AVX2__)
#define MDBX_ATTRIBUTE_TARGET_AVX2 /* nope */
#elif defined(MDBX_ATTRIBUTE_TARGET) && defined(__ia32__) &&                   \
    !MDBX_GCC_FASTMATH_i686_SIMD_WORKAROUND
#define MDBX_ATTRIBUTE_TARGET_AVX2 MDBX_ATTRIBUTE_TARGET("sse,sse2,avx,avx2")
#endif /* __AVX2__ */

#if defined(MDBX_ATTRIBUTE_TARGET_AVX2)
#if defined(__AVX512BW__)
#define MDBX_ATTRIBUTE_TARGET_AVX512BW /* nope */
#elif defined(MDBX_ATTRIBUTE_TARGET) && defined(__ia32__) &&                   \
    !MDBX_GCC_FASTMATH_i686_SIMD_WORKAROUND &&                                 \
    (__GNUC_PREREQ(6, 0) || __CLANG_PREREQ(5, 0))
#define MDBX_ATTRIBUTE_TARGET_AVX512BW                                         \
  MDBX_ATTRIBUTE_TARGET("sse,sse2,avx,avx2,avx512bw")
#endif /* __AVX512BW__ */
#endif /* MDBX_ATTRIBUTE_TARGET_AVX2 for MDBX_ATTRIBUTE_TARGET_AVX512BW */

#ifdef MDBX_ATTRIBUTE_TARGET_SSE2
MDBX_ATTRIBUTE_TARGET_SSE2 static __always_inline unsigned
diffcmp2mask_sse2(const pgno_t *const ptr, const ptrdiff_t offset,
                  const __m128i pattern) {
  const __m128i f = _mm_loadu_si128((const __m128i *)ptr);
  const __m128i l = _mm_loadu_si128((const __m128i *)(ptr + offset));
  const __m128i cmp = _mm_cmpeq_epi32(_mm_sub_epi32(f, l), pattern);
  return _mm_movemask_ps(*(const __m128 *)&cmp);
}

MDBX_MAYBE_UNUSED __hot MDBX_ATTRIBUTE_TARGET_SSE2 static pgno_t *
scan4seq_sse2(pgno_t *range, const size_t len, const size_t seq) {
  assert(seq > 0 && len > seq);
#if MDBX_PNL_ASCENDING
#error "FIXME: Not implemented"
#endif /* MDBX_PNL_ASCENDING */
  assert(range[-(ptrdiff_t)len] == len);
  pgno_t *const detent = range - len + seq;
  const ptrdiff_t offset = -(ptrdiff_t)seq;
  const pgno_t target = (pgno_t)offset;
  const __m128i pattern = _mm_set1_epi32(target);
  uint8_t mask;
  if (likely(len > seq + 3)) {
    do {
      mask = (uint8_t)diffcmp2mask_sse2(range - 3, offset, pattern);
      if (mask) {
#ifndef __SANITIZE_ADDRESS__
      found:
#endif /* __SANITIZE_ADDRESS__ */
        return range + 28 - __builtin_clz(mask);
      }
      range -= 4;
    } while (range > detent + 3);
    if (range == detent)
      return nullptr;
  }

  /* Далее происходит чтение от 4 до 12 лишних байт, которые могут быть не
   * только за пределами региона выделенного под PNL, но и пересекать границу
   * страницы памяти. Что может приводить как к ошибкам ASAN, так и к падению.
   * Поэтому проверяем смещение на странице, а с ASAN всегда страхуемся. */
#ifndef __SANITIZE_ADDRESS__
  const unsigned on_page_safe_mask = 0xff0 /* enough for '-15' bytes offset */;
  if (likely(on_page_safe_mask & (uintptr_t)(range + offset)) &&
      !RUNNING_ON_VALGRIND) {
    const unsigned extra = (unsigned)(detent + 4 - range);
    assert(extra > 0 && extra < 4);
    mask = 0xF << extra;
    mask &= diffcmp2mask_sse2(range - 3, offset, pattern);
    if (mask)
      goto found;
    return nullptr;
  }
#endif /* __SANITIZE_ADDRESS__ */
  do
    if (*range - range[offset] == target)
      return range;
  while (--range != detent);
  return nullptr;
}
#endif /* MDBX_ATTRIBUTE_TARGET_SSE2 */

#ifdef MDBX_ATTRIBUTE_TARGET_AVX2
MDBX_ATTRIBUTE_TARGET_AVX2 static __always_inline unsigned
diffcmp2mask_avx2(const pgno_t *const ptr, const ptrdiff_t offset,
                  const __m256i pattern) {
  const __m256i f = _mm256_loadu_si256((const __m256i *)ptr);
  const __m256i l = _mm256_loadu_si256((const __m256i *)(ptr + offset));
  const __m256i cmp = _mm256_cmpeq_epi32(_mm256_sub_epi32(f, l), pattern);
  return _mm256_movemask_ps(*(const __m256 *)&cmp);
}

MDBX_ATTRIBUTE_TARGET_AVX2 static __always_inline unsigned
diffcmp2mask_sse2avx(const pgno_t *const ptr, const ptrdiff_t offset,
                     const __m128i pattern) {
  const __m128i f = _mm_loadu_si128((const __m128i *)ptr);
  const __m128i l = _mm_loadu_si128((const __m128i *)(ptr + offset));
  const __m128i cmp = _mm_cmpeq_epi32(_mm_sub_epi32(f, l), pattern);
  return _mm_movemask_ps(*(const __m128 *)&cmp);
}

MDBX_MAYBE_UNUSED __hot MDBX_ATTRIBUTE_TARGET_AVX2 static pgno_t *
scan4seq_avx2(pgno_t *range, const size_t len, const size_t seq) {
  assert(seq > 0 && len > seq);
#if MDBX_PNL_ASCENDING
#error "FIXME: Not implemented"
#endif /* MDBX_PNL_ASCENDING */
  assert(range[-(ptrdiff_t)len] == len);
  pgno_t *const detent = range - len + seq;
  const ptrdiff_t offset = -(ptrdiff_t)seq;
  const pgno_t target = (pgno_t)offset;
  const __m256i pattern = _mm256_set1_epi32(target);
  uint8_t mask;
  if (likely(len > seq + 7)) {
    do {
      mask = (uint8_t)diffcmp2mask_avx2(range - 7, offset, pattern);
      if (mask) {
#ifndef __SANITIZE_ADDRESS__
      found:
#endif /* __SANITIZE_ADDRESS__ */
        return range + 24 - __builtin_clz(mask);
      }
      range -= 8;
    } while (range > detent + 7);
    if (range == detent)
      return nullptr;
  }

  /* Далее происходит чтение от 4 до 28 лишних байт, которые могут быть не
   * только за пределами региона выделенного под PNL, но и пересекать границу
   * страницы памяти. Что может приводить как к ошибкам ASAN, так и к падению.
   * Поэтому проверяем смещение на странице, а с ASAN всегда страхуемся. */
#ifndef __SANITIZE_ADDRESS__
  const unsigned on_page_safe_mask = 0xfe0 /* enough for '-31' bytes offset */;
  if (likely(on_page_safe_mask & (uintptr_t)(range + offset)) &&
      !RUNNING_ON_VALGRIND) {
    const unsigned extra = (unsigned)(detent + 8 - range);
    assert(extra > 0 && extra < 8);
    mask = 0xFF << extra;
    mask &= diffcmp2mask_avx2(range - 7, offset, pattern);
    if (mask)
      goto found;
    return nullptr;
  }
#endif /* __SANITIZE_ADDRESS__ */
  if (range - 3 > detent) {
    mask = diffcmp2mask_sse2avx(range - 3, offset, *(const __m128i *)&pattern);
    if (mask)
      return range + 28 - __builtin_clz(mask);
    range -= 4;
  }
  while (range > detent) {
    if (*range - range[offset] == target)
      return range;
    --range;
  }
  return nullptr;
}
#endif /* MDBX_ATTRIBUTE_TARGET_AVX2 */

#ifdef MDBX_ATTRIBUTE_TARGET_AVX512BW
MDBX_ATTRIBUTE_TARGET_AVX512BW static __always_inline unsigned
diffcmp2mask_avx512bw(const pgno_t *const ptr, const ptrdiff_t offset,
                      const __m512i pattern) {
  const __m512i f = _mm512_loadu_si512((const __m512i *)ptr);
  const __m512i l = _mm512_loadu_si512((const __m512i *)(ptr + offset));
  return _mm512_cmpeq_epi32_mask(_mm512_sub_epi32(f, l), pattern);
}

MDBX_MAYBE_UNUSED __hot MDBX_ATTRIBUTE_TARGET_AVX512BW static pgno_t *
scan4seq_avx512bw(pgno_t *range, const size_t len, const size_t seq) {
  assert(seq > 0 && len > seq);
#if MDBX_PNL_ASCENDING
#error "FIXME: Not implemented"
#endif /* MDBX_PNL_ASCENDING */
  assert(range[-(ptrdiff_t)len] == len);
  pgno_t *const detent = range - len + seq;
  const ptrdiff_t offset = -(ptrdiff_t)seq;
  const pgno_t target = (pgno_t)offset;
  const __m512i pattern = _mm512_set1_epi32(target);
  unsigned mask;
  if (likely(len > seq + 15)) {
    do {
      mask = diffcmp2mask_avx512bw(range - 15, offset, pattern);
      if (mask) {
#ifndef __SANITIZE_ADDRESS__
      found:
#endif /* __SANITIZE_ADDRESS__ */
        return range + 16 - __builtin_clz(mask);
      }
      range -= 16;
    } while (range > detent + 15);
    if (range == detent)
      return nullptr;
  }

  /* Далее происходит чтение от 4 до 60 лишних байт, которые могут быть не
   * только за пределами региона выделенного под PNL, но и пересекать границу
   * страницы памяти. Что может приводить как к ошибкам ASAN, так и к падению.
   * Поэтому проверяем смещение на странице, а с ASAN всегда страхуемся. */
#ifndef __SANITIZE_ADDRESS__
  const unsigned on_page_safe_mask = 0xfc0 /* enough for '-63' bytes offset */;
  if (likely(on_page_safe_mask & (uintptr_t)(range + offset)) &&
      !RUNNING_ON_VALGRIND) {
    const unsigned extra = (unsigned)(detent + 16 - range);
    assert(extra > 0 && extra < 16);
    mask = 0xFFFF << extra;
    mask &= diffcmp2mask_avx512bw(range - 15, offset, pattern);
    if (mask)
      goto found;
    return nullptr;
  }
#endif /* __SANITIZE_ADDRESS__ */
  if (range - 7 > detent) {
    mask = diffcmp2mask_avx2(range - 7, offset, *(const __m256i *)&pattern);
    if (mask)
      return range + 24 - __builtin_clz(mask);
    range -= 8;
  }
  if (range - 3 > detent) {
    mask = diffcmp2mask_sse2avx(range - 3, offset, *(const __m128i *)&pattern);
    if (mask)
      return range + 28 - __builtin_clz(mask);
    range -= 4;
  }
  while (range > detent) {
    if (*range - range[offset] == target)
      return range;
    --range;
  }
  return nullptr;
}
#endif /* MDBX_ATTRIBUTE_TARGET_AVX512BW */

#if (defined(__ARM_NEON) || defined(__ARM_NEON__)) &&                          \
    (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
static __always_inline size_t diffcmp2mask_neon(const pgno_t *const ptr,
                                                const ptrdiff_t offset,
                                                const uint32x4_t pattern) {
  const uint32x4_t f = vld1q_u32(ptr);
  const uint32x4_t l = vld1q_u32(ptr + offset);
  const uint16x4_t cmp = vmovn_u32(vceqq_u32(vsubq_u32(f, l), pattern));
  if (sizeof(size_t) > 7)
    return vget_lane_u64(vreinterpret_u64_u16(cmp), 0);
  else
    return vget_lane_u32(vreinterpret_u32_u8(vmovn_u16(vcombine_u16(cmp, cmp))),
                         0);
}

__hot static pgno_t *scan4seq_neon(pgno_t *range, const size_t len,
                                   const size_t seq) {
  assert(seq > 0 && len > seq);
#if MDBX_PNL_ASCENDING
#error "FIXME: Not implemented"
#endif /* MDBX_PNL_ASCENDING */
  assert(range[-(ptrdiff_t)len] == len);
  pgno_t *const detent = range - len + seq;
  const ptrdiff_t offset = -(ptrdiff_t)seq;
  const pgno_t target = (pgno_t)offset;
  const uint32x4_t pattern = vmovq_n_u32(target);
  size_t mask;
  if (likely(len > seq + 3)) {
    do {
      mask = diffcmp2mask_neon(range - 3, offset, pattern);
      if (mask) {
#ifndef __SANITIZE_ADDRESS__
      found:
#endif /* __SANITIZE_ADDRESS__ */
        return ptr_disp(range, -(__builtin_clzl(mask) >> sizeof(size_t) / 4));
      }
      range -= 4;
    } while (range > detent + 3);
    if (range == detent)
      return nullptr;
  }

  /* Далее происходит чтение от 4 до 12 лишних байт, которые могут быть не
   * только за пределами региона выделенного под PNL, но и пересекать границу
   * страницы памяти. Что может приводить как к ошибкам ASAN, так и к падению.
   * Поэтому проверяем смещение на странице, а с ASAN всегда страхуемся. */
#ifndef __SANITIZE_ADDRESS__
  const unsigned on_page_safe_mask = 0xff0 /* enough for '-15' bytes offset */;
  if (likely(on_page_safe_mask & (uintptr_t)(range + offset)) &&
      !RUNNING_ON_VALGRIND) {
    const unsigned extra = (unsigned)(detent + 4 - range);
    assert(extra > 0 && extra < 4);
    mask = (~(size_t)0) << (extra * sizeof(size_t) * 2);
    mask &= diffcmp2mask_neon(range - 3, offset, pattern);
    if (mask)
      goto found;
    return nullptr;
  }
#endif /* __SANITIZE_ADDRESS__ */
  do
    if (*range - range[offset] == target)
      return range;
  while (--range != detent);
  return nullptr;
}
#endif /* __ARM_NEON || __ARM_NEON__ */

#if defined(__AVX512BW__) && defined(MDBX_ATTRIBUTE_TARGET_AVX512BW)
#define scan4seq_default scan4seq_avx512bw
#define scan4seq_impl scan4seq_default
#elif defined(__AVX2__) && defined(MDBX_ATTRIBUTE_TARGET_AVX2)
#define scan4seq_default scan4seq_avx2
#elif defined(__SSE2__) && defined(MDBX_ATTRIBUTE_TARGET_SSE2)
#define scan4seq_default scan4seq_sse2
#elif (defined(__ARM_NEON) || defined(__ARM_NEON__)) &&                        \
    (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
#define scan4seq_default scan4seq_neon
/* Choosing of another variants should be added here. */
#endif /* scan4seq_default */

#endif /* MDBX_PNL_ASCENDING */

#ifndef scan4seq_default
#define scan4seq_default scan4seq_fallback
#endif /* scan4seq_default */

#ifdef scan4seq_impl
/* The scan4seq_impl() is the best or no alternatives */
#elif !MDBX_HAVE_BUILTIN_CPU_SUPPORTS
/* The scan4seq_default() will be used since no cpu-features detection support
 * from compiler. Please don't ask to implement cpuid-based detection and don't
 * make such PRs. */
#define scan4seq_impl scan4seq_default
#else
/* Selecting the most appropriate implementation at runtime,
 * depending on the available CPU features. */
static pgno_t *scan4seq_resolver(pgno_t *range, const size_t len,
                                 const size_t seq);
static pgno_t *(*scan4seq_impl)(pgno_t *range, const size_t len,
                                const size_t seq) = scan4seq_resolver;

static pgno_t *scan4seq_resolver(pgno_t *range, const size_t len,
                                 const size_t seq) {
  pgno_t *(*choice)(pgno_t *range, const size_t len, const size_t seq) =
      nullptr;
#if __has_builtin(__builtin_cpu_init) || defined(__BUILTIN_CPU_INIT__) ||      \
    __GNUC_PREREQ(4, 8)
  __builtin_cpu_init();
#endif /* __builtin_cpu_init() */
#ifdef MDBX_ATTRIBUTE_TARGET_SSE2
  if (__builtin_cpu_supports("sse2"))
    choice = scan4seq_sse2;
#endif /* MDBX_ATTRIBUTE_TARGET_SSE2 */
#ifdef MDBX_ATTRIBUTE_TARGET_AVX2
  if (__builtin_cpu_supports("avx2"))
    choice = scan4seq_avx2;
#endif /* MDBX_ATTRIBUTE_TARGET_AVX2 */
#ifdef MDBX_ATTRIBUTE_TARGET_AVX512BW
  if (__builtin_cpu_supports("avx512bw"))
    choice = scan4seq_avx512bw;
#endif /* MDBX_ATTRIBUTE_TARGET_AVX512BW */
  /* Choosing of another variants should be added here. */
  scan4seq_impl = choice ? choice : scan4seq_default;
  return scan4seq_impl(range, len, seq);
}
#endif /* scan4seq_impl */

//------------------------------------------------------------------------------

/* Allocate page numbers and memory for writing.  Maintain mt_last_reclaimed,
 * mt_relist and mt_next_pgno.  Set MDBX_TXN_ERROR on failure.
 *
 * If there are free pages available from older transactions, they
 * are re-used first. Otherwise allocate a new page at mt_next_pgno.
 * Do not modify the GC, just merge GC records into mt_relist
 * and move mt_last_reclaimed to say which records were consumed.  Only this
 * function can create mt_relist and move
 * mt_last_reclaimed/mt_next_pgno.
 *
 * [in] mc    cursor A cursor handle identifying the transaction and
 *            database for which we are allocating.
 * [in] num   the number of pages to allocate.
 *
 * Returns 0 on success, non-zero on failure.*/

#define MDBX_ALLOC_DEFAULT 0
#define MDBX_ALLOC_RESERVE 1
#define MDBX_ALLOC_UNIMPORTANT 2
#define MDBX_ALLOC_COALESCE 4    /* внутреннее состояние */
#define MDBX_ALLOC_SHOULD_SCAN 8 /* внутреннее состояние */
#define MDBX_ALLOC_LIFO 16       /* внутреннее состояние */

static __inline bool is_gc_usable(MDBX_txn *txn, const MDBX_cursor *mc,
                                  const uint8_t flags) {
  /* If txn is updating the GC, then the retired-list cannot play catch-up with
   * itself by growing while trying to save it. */
  if (mc->mc_dbi == FREE_DBI && !(flags & MDBX_ALLOC_RESERVE) &&
      !(mc->mc_flags & C_GCU))
    return false;

  /* avoid search inside empty tree and while tree is updating,
     https://libmdbx.dqdkfa.ru/dead-github/issues/31 */
  if (unlikely(txn->mt_dbs[FREE_DBI].md_entries == 0)) {
    txn->mt_flags |= MDBX_TXN_DRAINED_GC;
    return false;
  }

  return true;
}

__hot static bool is_already_reclaimed(const MDBX_txn *txn, txnid_t id) {
  const size_t len = MDBX_PNL_GETSIZE(txn->tw.lifo_reclaimed);
  for (size_t i = 1; i <= len; ++i)
    if (txn->tw.lifo_reclaimed[i] == id)
      return true;
  return false;
}

__hot static pgno_t relist_get_single(MDBX_txn *txn) {
  const size_t len = MDBX_PNL_GETSIZE(txn->tw.relist);
  assert(len > 0);
  pgno_t *target = MDBX_PNL_EDGE(txn->tw.relist);
  const ptrdiff_t dir = MDBX_PNL_ASCENDING ? 1 : -1;

  /* Есть ТРИ потенциально выигрышные, но противо-направленные тактики:
   *
   * 1. Стараться использовать страницы с наименьшими номерами. Так обмен с
   * диском будет более кучным, а у страниц ближе к концу БД будет больше шансов
   * попасть под авто-компактификацию. Частично эта тактика уже реализована, но
   * для её эффективности требуется явно приоритезировать выделение страниц:
   *   - поддерживать два relist, для ближних и для дальних страниц;
   *   - использовать страницы из дальнего списка, если первый пуст,
   *     а второй слишком большой, либо при пустой GC.
   *
   * 2. Стараться выделять страницы последовательно. Так записываемые на диск
   * регионы будут линейными, что принципиально ускоряет запись на HDD.
   * Одновременно, в среднем это не повлияет на чтение, точнее говоря, если
   * порядок чтения не совпадает с порядком изменения (иначе говоря, если
   * чтение не коррелирует с обновлениями и/или вставками) то не повлияет, иначе
   * может ускорить. Однако, последовательности в среднем достаточно редки.
   * Поэтому для эффективности требуется аккумулировать и поддерживать в ОЗУ
   * огромные списки страниц, а затем сохранять их обратно в БД. Текущий формат
   * БД (без сжатых битовых карт) для этого крайне не удачен. Поэтому эта
   * тактика не имеет шансов быть успешной без смены формата БД (Mithril).
   *
   * 3. Стараться экономить последовательности страниц. Это позволяет избегать
   * лишнего чтения/поиска в GC при более-менее постоянном размещении и/или
   * обновлении данных требующих более одной страницы. Проблема в том, что без
   * информации от приложения библиотека не может знать насколько
   * востребованными будут последовательности в ближайшей перспективе, а
   * экономия последовательностей "на всякий случай" не только затратна
   * сама-по-себе, но и работает во вред (добавляет хаоса).
   *
   * Поэтому:
   *  - в TODO добавляется разделение relist на «ближние» и «дальние» страницы,
   *    с последующей реализацией первой тактики;
   *  - преимущественное использование последовательностей отправляется
   *    в MithrilDB как составляющая "HDD frendly" feature;
   *  - реализованная в 3757eb72f7c6b46862f8f17881ac88e8cecc1979 экономия
   *    последовательностей отключается через MDBX_ENABLE_SAVING_SEQUENCES=0.
   *
   * В качестве альтернативы для безусловной «экономии» последовательностей,
   * в следующих версиях libmdbx, вероятно, будет предложено
   * API для взаимодействия с GC:
   *  - получение размера GC, включая гистограммы размеров последовательностей
   *    и близости к концу БД;
   *  - включение формирования "линейного запаса" для последующего использования
   *    в рамках текущей транзакции;
   *  - намеренная загрузка GC в память для коагуляции и "выпрямления";
   *  - намеренное копирование данных из страниц в конце БД для последующего
   *    из освобождения, т.е. контролируемая компактификация по запросу. */

#ifndef MDBX_ENABLE_SAVING_SEQUENCES
#define MDBX_ENABLE_SAVING_SEQUENCES 0
#endif
  if (MDBX_ENABLE_SAVING_SEQUENCES && unlikely(target[dir] == *target + 1) &&
      len > 2) {
    /* Пытаемся пропускать последовательности при наличии одиночных элементов.
     * TODO: необходимо кэшировать пропускаемые последовательности
     * чтобы не сканировать список сначала при каждом выделении. */
    pgno_t *scan = target + dir + dir;
    size_t left = len;
    do {
      if (likely(scan[-dir] != *scan - 1 && *scan + 1 != scan[dir])) {
#if MDBX_PNL_ASCENDING
        target = scan;
        break;
#else
        /* вырезаем элемент с перемещением хвоста */
        const pgno_t pgno = *scan;
        MDBX_PNL_SETSIZE(txn->tw.relist, len - 1);
        while (++scan <= target)
          scan[-1] = *scan;
        return pgno;
#endif
      }
      scan += dir;
    } while (--left > 2);
  }

  const pgno_t pgno = *target;
#if MDBX_PNL_ASCENDING
  /* вырезаем элемент с перемещением хвоста */
  MDBX_PNL_SETSIZE(txn->tw.relist, len - 1);
  for (const pgno_t *const end = txn->tw.relist + len - 1; target <= end;
       ++target)
    *target = target[1];
#else
  /* перемещать хвост не нужно, просто усекам список */
  MDBX_PNL_SETSIZE(txn->tw.relist, len - 1);
#endif
  return pgno;
}

__hot static pgno_t relist_get_sequence(MDBX_txn *txn, const size_t num,
                                        uint8_t flags) {
  const size_t len = MDBX_PNL_GETSIZE(txn->tw.relist);
  pgno_t *edge = MDBX_PNL_EDGE(txn->tw.relist);
  assert(len >= num && num > 1);
  const size_t seq = num - 1;
#if !MDBX_PNL_ASCENDING
  if (edge[-(ptrdiff_t)seq] - *edge == seq) {
    if (unlikely(flags & MDBX_ALLOC_RESERVE))
      return P_INVALID;
    assert(edge == scan4range_checker(txn->tw.relist, seq));
    /* перемещать хвост не нужно, просто усекам список */
    MDBX_PNL_SETSIZE(txn->tw.relist, len - num);
    return *edge;
  }
#endif
  pgno_t *target = scan4seq_impl(edge, len, seq);
  assert(target == scan4range_checker(txn->tw.relist, seq));
  if (target) {
    if (unlikely(flags & MDBX_ALLOC_RESERVE))
      return P_INVALID;
    const pgno_t pgno = *target;
    /* вырезаем найденную последовательность с перемещением хвоста */
    MDBX_PNL_SETSIZE(txn->tw.relist, len - num);
#if MDBX_PNL_ASCENDING
    for (const pgno_t *const end = txn->tw.relist + len - num; target <= end;
         ++target)
      *target = target[num];
#else
    for (const pgno_t *const end = txn->tw.relist + len; ++target <= end;)
      target[-(ptrdiff_t)num] = *target;
#endif
    return pgno;
  }
  return 0;
}

#if MDBX_ENABLE_MINCORE
static __inline bool bit_tas(uint64_t *field, char bit) {
  const uint64_t m = UINT64_C(1) << bit;
  const bool r = (*field & m) != 0;
  *field |= m;
  return r;
}

static bool mincore_fetch(MDBX_env *const env, const size_t unit_begin) {
  MDBX_lockinfo *const lck = env->me_lck;
  for (size_t i = 1; i < ARRAY_LENGTH(lck->mti_mincore_cache.begin); ++i) {
    const ptrdiff_t dist = unit_begin - lck->mti_mincore_cache.begin[i];
    if (likely(dist >= 0 && dist < 64)) {
      const pgno_t tmp_begin = lck->mti_mincore_cache.begin[i];
      const uint64_t tmp_mask = lck->mti_mincore_cache.mask[i];
      do {
        lck->mti_mincore_cache.begin[i] = lck->mti_mincore_cache.begin[i - 1];
        lck->mti_mincore_cache.mask[i] = lck->mti_mincore_cache.mask[i - 1];
      } while (--i);
      lck->mti_mincore_cache.begin[0] = tmp_begin;
      lck->mti_mincore_cache.mask[0] = tmp_mask;
      return bit_tas(lck->mti_mincore_cache.mask, (char)dist);
    }
  }

  size_t pages = 64;
  unsigned unit_log = sys_pagesize_ln2;
  unsigned shift = 0;
  if (env->me_psize > env->me_os_psize) {
    unit_log = env->me_psize2log;
    shift = env->me_psize2log - sys_pagesize_ln2;
    pages <<= shift;
  }

  const size_t offset = unit_begin << unit_log;
  size_t length = pages << sys_pagesize_ln2;
  if (offset + length > env->me_dxb_mmap.current) {
    length = env->me_dxb_mmap.current - offset;
    pages = length >> sys_pagesize_ln2;
  }

#if MDBX_ENABLE_PGOP_STAT
  env->me_lck->mti_pgop_stat.mincore.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
  uint8_t *const vector = alloca(pages);
  if (unlikely(mincore(ptr_disp(env->me_dxb_mmap.base, offset), length,
                       (void *)vector))) {
    NOTICE("mincore(+%zu, %zu), err %d", offset, length, errno);
    return false;
  }

  for (size_t i = 1; i < ARRAY_LENGTH(lck->mti_mincore_cache.begin); ++i) {
    lck->mti_mincore_cache.begin[i] = lck->mti_mincore_cache.begin[i - 1];
    lck->mti_mincore_cache.mask[i] = lck->mti_mincore_cache.mask[i - 1];
  }
  lck->mti_mincore_cache.begin[0] = unit_begin;

  uint64_t mask = 0;
#ifdef MINCORE_INCORE
  STATIC_ASSERT(MINCORE_INCORE == 1);
#endif
  for (size_t i = 0; i < pages; ++i) {
    uint64_t bit = (vector[i] & 1) == 0;
    bit <<= i >> shift;
    mask |= bit;
  }

  lck->mti_mincore_cache.mask[0] = ~mask;
  return bit_tas(lck->mti_mincore_cache.mask, 0);
}
#endif /* MDBX_ENABLE_MINCORE */

MDBX_MAYBE_UNUSED static __inline bool mincore_probe(MDBX_env *const env,
                                                     const pgno_t pgno) {
#if MDBX_ENABLE_MINCORE
  const size_t offset_aligned =
      floor_powerof2(pgno2bytes(env, pgno), env->me_os_psize);
  const unsigned unit_log2 = (env->me_psize2log > sys_pagesize_ln2)
                                 ? env->me_psize2log
                                 : sys_pagesize_ln2;
  const size_t unit_begin = offset_aligned >> unit_log2;
  eASSERT(env, (unit_begin << unit_log2) == offset_aligned);
  const ptrdiff_t dist = unit_begin - env->me_lck->mti_mincore_cache.begin[0];
  if (likely(dist >= 0 && dist < 64))
    return bit_tas(env->me_lck->mti_mincore_cache.mask, (char)dist);
  return mincore_fetch(env, unit_begin);
#else
  (void)env;
  (void)pgno;
  return false;
#endif /* MDBX_ENABLE_MINCORE */
}

static __inline pgr_t page_alloc_finalize(MDBX_env *const env,
                                          MDBX_txn *const txn,
                                          const MDBX_cursor *const mc,
                                          const pgno_t pgno, const size_t num) {
#if MDBX_ENABLE_PROFGC
  size_t majflt_before;
  const uint64_t cputime_before = osal_cputime(&majflt_before);
  profgc_stat_t *const prof = (mc->mc_dbi == FREE_DBI)
                                  ? &env->me_lck->mti_pgop_stat.gc_prof.self
                                  : &env->me_lck->mti_pgop_stat.gc_prof.work;
#else
  (void)mc;
#endif /* MDBX_ENABLE_PROFGC */
  ENSURE(env, pgno >= NUM_METAS);

  pgr_t ret;
  bool need_clean = (env->me_flags & MDBX_PAGEPERTURB) != 0;
  if (env->me_flags & MDBX_WRITEMAP) {
    ret.page = pgno2page(env, pgno);
    MDBX_ASAN_UNPOISON_MEMORY_REGION(ret.page, pgno2bytes(env, num));
    VALGRIND_MAKE_MEM_UNDEFINED(ret.page, pgno2bytes(env, num));

    /* Содержимое выделенной страницы не нужно, но если страница отсутствует
     * в ОЗУ (что весьма вероятно), то любое обращение к ней приведет
     * к page-fault:
     *  - прерыванию по отсутствию страницы;
     *  - переключение контекста в режим ядра с засыпанием процесса;
     *  - чтение страницы с диска;
     *  - обновление PTE и пробуждением процесса;
     *  - переключение контекста по доступности ЦПУ.
     *
     * Пытаемся минимизировать накладные расходы записывая страницу, что при
     * наличии unified page cache приведет к появлению страницы в ОЗУ без чтения
     * с диска. При этом запись на диск должна быть отложена адекватным ядром,
     * так как страница отображена в память в режиме чтения-записи и следом в
     * неё пишет ЦПУ. */

    /* В случае если страница в памяти процесса, то излишняя запись может быть
     * достаточно дорогой. Кроме системного вызова и копирования данных, в особо
     * одаренных ОС при этом могут включаться файловая система, выделяться
     * временная страница, пополняться очереди асинхронного выполнения,
     * обновляться PTE с последующей генерацией page-fault и чтением данных из
     * грязной I/O очереди. Из-за этого штраф за лишнюю запись может быть
     * сравним с избегаемым ненужным чтением. */
    if (env->me_prefault_write) {
      void *const pattern = ptr_disp(
          env->me_pbuf, need_clean ? env->me_psize : env->me_psize * 2);
      size_t file_offset = pgno2bytes(env, pgno);
      if (likely(num == 1)) {
        if (!mincore_probe(env, pgno)) {
          osal_pwrite(env->me_lazy_fd, pattern, env->me_psize, file_offset);
#if MDBX_ENABLE_PGOP_STAT
          env->me_lck->mti_pgop_stat.prefault.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
          need_clean = false;
        }
      } else {
        struct iovec iov[MDBX_AUXILARY_IOV_MAX];
        size_t n = 0, cleared = 0;
        for (size_t i = 0; i < num; ++i) {
          if (!mincore_probe(env, pgno + (pgno_t)i)) {
            ++cleared;
            iov[n].iov_len = env->me_psize;
            iov[n].iov_base = pattern;
            if (unlikely(++n == MDBX_AUXILARY_IOV_MAX)) {
              osal_pwritev(env->me_lazy_fd, iov, MDBX_AUXILARY_IOV_MAX,
                           file_offset);
#if MDBX_ENABLE_PGOP_STAT
              env->me_lck->mti_pgop_stat.prefault.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
              file_offset += pgno2bytes(env, MDBX_AUXILARY_IOV_MAX);
              n = 0;
            }
          }
        }
        if (likely(n > 0)) {
          osal_pwritev(env->me_lazy_fd, iov, n, file_offset);
#if MDBX_ENABLE_PGOP_STAT
          env->me_lck->mti_pgop_stat.prefault.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
        }
        if (cleared == num)
          need_clean = false;
      }
    }
  } else {
    ret.page = page_malloc(txn, num);
    if (unlikely(!ret.page)) {
      ret.err = MDBX_ENOMEM;
      goto bailout;
    }
  }

  if (unlikely(need_clean))
    memset(ret.page, -1, pgno2bytes(env, num));

  VALGRIND_MAKE_MEM_UNDEFINED(ret.page, pgno2bytes(env, num));
  ret.page->mp_pgno = pgno;
  ret.page->mp_leaf2_ksize = 0;
  ret.page->mp_flags = 0;
  if ((ASSERT_ENABLED() || AUDIT_ENABLED()) && num > 1) {
    ret.page->mp_pages = (pgno_t)num;
    ret.page->mp_flags = P_OVERFLOW;
  }

  ret.err = page_dirty(txn, ret.page, (pgno_t)num);
bailout:
  tASSERT(txn, pnl_check_allocated(txn->tw.relist,
                                   txn->mt_next_pgno - MDBX_ENABLE_REFUND));
#if MDBX_ENABLE_PROFGC
  size_t majflt_after;
  prof->xtime_cpu += osal_cputime(&majflt_after) - cputime_before;
  prof->majflt += (uint32_t)(majflt_after - majflt_before);
#endif /* MDBX_ENABLE_PROFGC */
  return ret;
}

static pgr_t page_alloc_slowpath(const MDBX_cursor *const mc, const size_t num,
                                 uint8_t flags) {
#if MDBX_ENABLE_PROFGC
  const uint64_t monotime_before = osal_monotime();
#endif /* MDBX_ENABLE_PROFGC */

  pgr_t ret;
  MDBX_txn *const txn = mc->mc_txn;
  MDBX_env *const env = txn->mt_env;
#if MDBX_ENABLE_PROFGC
  profgc_stat_t *const prof = (mc->mc_dbi == FREE_DBI)
                                  ? &env->me_lck->mti_pgop_stat.gc_prof.self
                                  : &env->me_lck->mti_pgop_stat.gc_prof.work;
  prof->spe_counter += 1;
#endif /* MDBX_ENABLE_PROFGC */

  eASSERT(env, num > 0 || (flags & MDBX_ALLOC_RESERVE));
  eASSERT(env, pnl_check_allocated(txn->tw.relist,
                                   txn->mt_next_pgno - MDBX_ENABLE_REFUND));

  pgno_t pgno = 0;
  size_t newnext;
  if (num > 1) {
#if MDBX_ENABLE_PROFGC
    prof->xpages += 1;
#endif /* MDBX_ENABLE_PROFGC */
    if (MDBX_PNL_GETSIZE(txn->tw.relist) >= num) {
      eASSERT(env, MDBX_PNL_LAST(txn->tw.relist) < txn->mt_next_pgno &&
                       MDBX_PNL_FIRST(txn->tw.relist) < txn->mt_next_pgno);
      pgno = relist_get_sequence(txn, num, flags);
      if (likely(pgno))
        goto done;
    }
  } else {
    eASSERT(env, num == 0 || MDBX_PNL_GETSIZE(txn->tw.relist) == 0);
    eASSERT(env, !(flags & MDBX_ALLOC_RESERVE) || num == 0);
  }

  //---------------------------------------------------------------------------

  if (unlikely(!is_gc_usable(txn, mc, flags))) {
    eASSERT(env, (txn->mt_flags & MDBX_TXN_DRAINED_GC) || num > 1);
    goto no_gc;
  }

  eASSERT(env, (flags & (MDBX_ALLOC_COALESCE | MDBX_ALLOC_LIFO |
                         MDBX_ALLOC_SHOULD_SCAN)) == 0);
  flags += (env->me_flags & MDBX_LIFORECLAIM) ? MDBX_ALLOC_LIFO : 0;

  if (/* Не коагулируем записи при подготовке резерва для обновления GC.
       * Иначе попытка увеличить резерв может приводить к необходимости ещё
       * большего резерва из-за увеличения списка переработанных страниц. */
      (flags & MDBX_ALLOC_RESERVE) == 0) {
    if (txn->mt_dbs[FREE_DBI].md_branch_pages &&
        MDBX_PNL_GETSIZE(txn->tw.relist) < env->me_maxgc_ov1page / 2)
      flags += MDBX_ALLOC_COALESCE;
  }

  MDBX_cursor *const gc = ptr_disp(env->me_txn0, sizeof(MDBX_txn));
  eASSERT(env, mc != gc && gc->mc_next == nullptr);
  gc->mc_txn = txn;
  gc->mc_flags = 0;

  env->me_prefault_write = env->me_options.prefault_write;
  if (env->me_prefault_write) {
    /* Проверка посредством minicore() существенно снижает затраты, но в
     * простейших случаях (тривиальный бенчмарк) интегральная производительность
     * становится вдвое меньше. А на платформах без mincore() и с проблемной
     * подсистемой виртуальной памяти ситуация может быть многократно хуже.
     * Поэтому избегаем затрат в ситуациях когда prefault-write скорее всего не
     * нужна. */
    const bool readahead_enabled = env->me_lck->mti_readahead_anchor & 1;
    const pgno_t readahead_edge = env->me_lck->mti_readahead_anchor >> 1;
    if (/* Не суетимся если GC почти пустая и БД маленькая */
        (txn->mt_dbs[FREE_DBI].md_branch_pages == 0 &&
         txn->mt_geo.now < 1234) ||
        /* Не суетимся если страница в зоне включенного упреждающего чтения */
        (readahead_enabled && pgno + num < readahead_edge))
      env->me_prefault_write = false;
  }

retry_gc_refresh_oldest:;
  txnid_t oldest = txn_oldest_reader(txn);
retry_gc_have_oldest:
  if (unlikely(oldest >= txn->mt_txnid)) {
    ERROR("unexpected/invalid oldest-readed txnid %" PRIaTXN
          " for current-txnid %" PRIaTXN,
          oldest, txn->mt_txnid);
    ret.err = MDBX_PROBLEM;
    goto fail;
  }
  const txnid_t detent = oldest + 1;

  txnid_t id = 0;
  MDBX_cursor_op op = MDBX_FIRST;
  if (flags & MDBX_ALLOC_LIFO) {
    if (!txn->tw.lifo_reclaimed) {
      txn->tw.lifo_reclaimed = txl_alloc();
      if (unlikely(!txn->tw.lifo_reclaimed)) {
        ret.err = MDBX_ENOMEM;
        goto fail;
      }
    }
    /* Begin lookup backward from oldest reader */
    id = detent - 1;
    op = MDBX_SET_RANGE;
  } else if (txn->tw.last_reclaimed) {
    /* Continue lookup forward from last-reclaimed */
    id = txn->tw.last_reclaimed + 1;
    if (id >= detent)
      goto depleted_gc;
    op = MDBX_SET_RANGE;
  }

next_gc:;
  MDBX_val key;
  key.iov_base = &id;
  key.iov_len = sizeof(id);

#if MDBX_ENABLE_PROFGC
  prof->rsteps += 1;
#endif /* MDBX_ENABLE_PROFGC */

  /* Seek first/next GC record */
  ret.err = cursor_get(gc, &key, NULL, op);
  if (unlikely(ret.err != MDBX_SUCCESS)) {
    if (unlikely(ret.err != MDBX_NOTFOUND))
      goto fail;
    if ((flags & MDBX_ALLOC_LIFO) && op == MDBX_SET_RANGE) {
      op = MDBX_PREV;
      goto next_gc;
    }
    goto depleted_gc;
  }
  if (unlikely(key.iov_len != sizeof(txnid_t))) {
    ret.err = MDBX_CORRUPTED;
    goto fail;
  }
  id = unaligned_peek_u64(4, key.iov_base);
  if (flags & MDBX_ALLOC_LIFO) {
    op = MDBX_PREV;
    if (id >= detent || is_already_reclaimed(txn, id))
      goto next_gc;
  } else {
    op = MDBX_NEXT;
    if (unlikely(id >= detent))
      goto depleted_gc;
  }
  txn->mt_flags &= ~MDBX_TXN_DRAINED_GC;

  /* Reading next GC record */
  MDBX_val data;
  MDBX_page *const mp = gc->mc_pg[gc->mc_top];
  if (unlikely((ret.err = node_read(gc, page_node(mp, gc->mc_ki[gc->mc_top]),
                                    &data, mp)) != MDBX_SUCCESS))
    goto fail;

  pgno_t *gc_pnl = (pgno_t *)data.iov_base;
  if (unlikely(data.iov_len % sizeof(pgno_t) ||
               data.iov_len < MDBX_PNL_SIZEOF(gc_pnl) ||
               !pnl_check(gc_pnl, txn->mt_next_pgno))) {
    ret.err = MDBX_CORRUPTED;
    goto fail;
  }

  const size_t gc_len = MDBX_PNL_GETSIZE(gc_pnl);
  TRACE("gc-read: id #%" PRIaTXN " len %zu, re-list will %zu ", id, gc_len,
        gc_len + MDBX_PNL_GETSIZE(txn->tw.relist));

  if (unlikely(gc_len + MDBX_PNL_GETSIZE(txn->tw.relist) >=
               env->me_maxgc_ov1page)) {
    /* Don't try to coalesce too much. */
    if (flags & MDBX_ALLOC_SHOULD_SCAN) {
      eASSERT(env, flags & MDBX_ALLOC_COALESCE);
      eASSERT(env, !(flags & MDBX_ALLOC_RESERVE));
      eASSERT(env, num > 0);
#if MDBX_ENABLE_PROFGC
      env->me_lck->mti_pgop_stat.gc_prof.coalescences += 1;
#endif /* MDBX_ENABLE_PROFGC */
      TRACE("clear %s %s", "MDBX_ALLOC_COALESCE", "since got threshold");
      if (MDBX_PNL_GETSIZE(txn->tw.relist) >= num) {
        eASSERT(env, MDBX_PNL_LAST(txn->tw.relist) < txn->mt_next_pgno &&
                         MDBX_PNL_FIRST(txn->tw.relist) < txn->mt_next_pgno);
        if (likely(num == 1)) {
          pgno = relist_get_single(txn);
          goto done;
        }
        pgno = relist_get_sequence(txn, num, flags);
        if (likely(pgno))
          goto done;
      }
      flags -= MDBX_ALLOC_COALESCE | MDBX_ALLOC_SHOULD_SCAN;
    }
    if (unlikely(/* list is too long already */ MDBX_PNL_GETSIZE(
                     txn->tw.relist) >= env->me_options.rp_augment_limit) &&
        ((/* not a slot-request from gc-update */ num &&
          /* have enough unallocated space */ txn->mt_geo.upper >=
              txn->mt_next_pgno + num) ||
         gc_len + MDBX_PNL_GETSIZE(txn->tw.relist) >= MDBX_PGL_LIMIT)) {
      /* Stop reclaiming to avoid large/overflow the page list. This is a rare
       * case while search for a continuously multi-page region in a
       * large database, see https://libmdbx.dqdkfa.ru/dead-github/issues/123 */
      NOTICE("stop reclaiming %s: %zu (current) + %zu "
             "(chunk) -> %zu, rp_augment_limit %u",
             likely(gc_len + MDBX_PNL_GETSIZE(txn->tw.relist) < MDBX_PGL_LIMIT)
                 ? "since rp_augment_limit was reached"
                 : "to avoid PNL overflow",
             MDBX_PNL_GETSIZE(txn->tw.relist), gc_len,
             gc_len + MDBX_PNL_GETSIZE(txn->tw.relist),
             env->me_options.rp_augment_limit);
      goto depleted_gc;
    }
  }

  /* Remember ID of readed GC record */
  txn->tw.last_reclaimed = id;
  if (flags & MDBX_ALLOC_LIFO) {
    ret.err = txl_append(&txn->tw.lifo_reclaimed, id);
    if (unlikely(ret.err != MDBX_SUCCESS))
      goto fail;
  }

  /* Append PNL from GC record to tw.relist */
  ret.err = pnl_need(&txn->tw.relist, gc_len);
  if (unlikely(ret.err != MDBX_SUCCESS))
    goto fail;

  if (LOG_ENABLED(MDBX_LOG_EXTRA)) {
    DEBUG_EXTRA("readed GC-pnl txn %" PRIaTXN " root %" PRIaPGNO
                " len %zu, PNL",
                id, txn->mt_dbs[FREE_DBI].md_root, gc_len);
    for (size_t i = gc_len; i; i--)
      DEBUG_EXTRA_PRINT(" %" PRIaPGNO, gc_pnl[i]);
    DEBUG_EXTRA_PRINT(", next_pgno %u\n", txn->mt_next_pgno);
  }

  /* Merge in descending sorted order */
  pnl_merge(txn->tw.relist, gc_pnl);
  flags |= MDBX_ALLOC_SHOULD_SCAN;
  if (AUDIT_ENABLED()) {
    if (unlikely(!pnl_check(txn->tw.relist, txn->mt_next_pgno))) {
      ret.err = MDBX_CORRUPTED;
      goto fail;
    }
  } else {
    eASSERT(env, pnl_check_allocated(txn->tw.relist, txn->mt_next_pgno));
  }
  eASSERT(env, dirtylist_check(txn));

  eASSERT(env, MDBX_PNL_GETSIZE(txn->tw.relist) == 0 ||
                   MDBX_PNL_MOST(txn->tw.relist) < txn->mt_next_pgno);
  if (MDBX_ENABLE_REFUND && MDBX_PNL_GETSIZE(txn->tw.relist) &&
      unlikely(MDBX_PNL_MOST(txn->tw.relist) == txn->mt_next_pgno - 1)) {
    /* Refund suitable pages into "unallocated" space */
    txn_refund(txn);
  }
  eASSERT(env, pnl_check_allocated(txn->tw.relist,
                                   txn->mt_next_pgno - MDBX_ENABLE_REFUND));

  /* Done for a kick-reclaim mode, actually no page needed */
  if (unlikely(num == 0)) {
    eASSERT(env, ret.err == MDBX_SUCCESS);
    TRACE("%s: last id #%" PRIaTXN ", re-len %zu", "early-exit for slot", id,
          MDBX_PNL_GETSIZE(txn->tw.relist));
    goto early_exit;
  }

  /* TODO: delete reclaimed records */

  eASSERT(env, op == MDBX_PREV || op == MDBX_NEXT);
  if (flags & MDBX_ALLOC_COALESCE) {
    TRACE("%s: last id #%" PRIaTXN ", re-len %zu", "coalesce-continue", id,
          MDBX_PNL_GETSIZE(txn->tw.relist));
    goto next_gc;
  }

scan:
  eASSERT(env, flags & MDBX_ALLOC_SHOULD_SCAN);
  eASSERT(env, num > 0);
  if (MDBX_PNL_GETSIZE(txn->tw.relist) >= num) {
    eASSERT(env, MDBX_PNL_LAST(txn->tw.relist) < txn->mt_next_pgno &&
                     MDBX_PNL_FIRST(txn->tw.relist) < txn->mt_next_pgno);
    if (likely(num == 1)) {
      eASSERT(env, !(flags & MDBX_ALLOC_RESERVE));
      pgno = relist_get_single(txn);
      goto done;
    }
    pgno = relist_get_sequence(txn, num, flags);
    if (likely(pgno))
      goto done;
  }
  flags -= MDBX_ALLOC_SHOULD_SCAN;
  if (ret.err == MDBX_SUCCESS) {
    TRACE("%s: last id #%" PRIaTXN ", re-len %zu", "continue-search", id,
          MDBX_PNL_GETSIZE(txn->tw.relist));
    goto next_gc;
  }

depleted_gc:
  TRACE("%s: last id #%" PRIaTXN ", re-len %zu", "gc-depleted", id,
        MDBX_PNL_GETSIZE(txn->tw.relist));
  ret.err = MDBX_NOTFOUND;
  if (flags & MDBX_ALLOC_SHOULD_SCAN)
    goto scan;
  txn->mt_flags |= MDBX_TXN_DRAINED_GC;

  //-------------------------------------------------------------------------

  /* There is no suitable pages in the GC and to be able to allocate
   * we should CHOICE one of:
   *  - make a new steady checkpoint if reclaiming was stopped by
   *    the last steady-sync, or wipe it in the MDBX_UTTERLY_NOSYNC mode;
   *  - kick lagging reader(s) if reclaiming was stopped by ones of it.
   *  - extend the database file. */

  /* Will use new pages from the map if nothing is suitable in the GC. */
  newnext = txn->mt_next_pgno + num;

  /* Does reclaiming stopped at the last steady point? */
  const meta_ptr_t recent = meta_recent(env, &txn->tw.troika);
  const meta_ptr_t prefer_steady = meta_prefer_steady(env, &txn->tw.troika);
  if (recent.ptr_c != prefer_steady.ptr_c && prefer_steady.is_steady &&
      detent == prefer_steady.txnid + 1) {
    DEBUG("gc-kick-steady: recent %" PRIaTXN "-%s, steady %" PRIaTXN
          "-%s, detent %" PRIaTXN,
          recent.txnid, durable_caption(recent.ptr_c), prefer_steady.txnid,
          durable_caption(prefer_steady.ptr_c), detent);
    const pgno_t autosync_threshold =
        atomic_load32(&env->me_lck->mti_autosync_threshold, mo_Relaxed);
    const uint64_t autosync_period =
        atomic_load64(&env->me_lck->mti_autosync_period, mo_Relaxed);
    uint64_t eoos_timestamp;
    /* wipe the last steady-point if one of:
     *  - UTTERLY_NOSYNC mode AND auto-sync threshold is NOT specified
     *  - UTTERLY_NOSYNC mode AND free space at steady-point is exhausted
     * otherwise, make a new steady-point if one of:
     *  - auto-sync threshold is specified and reached;
     *  - upper limit of database size is reached;
     *  - database is full (with the current file size)
     *       AND auto-sync threshold it NOT specified */
    if (F_ISSET(env->me_flags, MDBX_UTTERLY_NOSYNC) &&
        ((autosync_threshold | autosync_period) == 0 ||
         newnext >= prefer_steady.ptr_c->mm_geo.now)) {
      /* wipe steady checkpoint in MDBX_UTTERLY_NOSYNC mode
       * without any auto-sync threshold(s). */
#if MDBX_ENABLE_PROFGC
      env->me_lck->mti_pgop_stat.gc_prof.wipes += 1;
#endif /* MDBX_ENABLE_PROFGC */
      ret.err = wipe_steady(txn, detent);
      DEBUG("gc-wipe-steady, rc %d", ret.err);
      if (unlikely(ret.err != MDBX_SUCCESS))
        goto fail;
      eASSERT(env, prefer_steady.ptr_c !=
                       meta_prefer_steady(env, &txn->tw.troika).ptr_c);
      goto retry_gc_refresh_oldest;
    }
    if ((autosync_threshold &&
         atomic_load64(&env->me_lck->mti_unsynced_pages, mo_Relaxed) >=
             autosync_threshold) ||
        (autosync_period &&
         (eoos_timestamp =
              atomic_load64(&env->me_lck->mti_eoos_timestamp, mo_Relaxed)) &&
         osal_monotime() - eoos_timestamp >= autosync_period) ||
        newnext >= txn->mt_geo.upper ||
        ((num == 0 || newnext >= txn->mt_end_pgno) &&
         (autosync_threshold | autosync_period) == 0)) {
      /* make steady checkpoint. */
#if MDBX_ENABLE_PROFGC
      env->me_lck->mti_pgop_stat.gc_prof.flushes += 1;
#endif /* MDBX_ENABLE_PROFGC */
      MDBX_meta meta = *recent.ptr_c;
      ret.err = sync_locked(env, env->me_flags & MDBX_WRITEMAP, &meta,
                            &txn->tw.troika);
      DEBUG("gc-make-steady, rc %d", ret.err);
      eASSERT(env, ret.err != MDBX_RESULT_TRUE);
      if (unlikely(ret.err != MDBX_SUCCESS))
        goto fail;
      eASSERT(env, prefer_steady.ptr_c !=
                       meta_prefer_steady(env, &txn->tw.troika).ptr_c);
      goto retry_gc_refresh_oldest;
    }
  }

  if (unlikely(true == atomic_load32(&env->me_lck->mti_readers_refresh_flag,
                                     mo_AcquireRelease))) {
    oldest = txn_oldest_reader(txn);
    if (oldest >= detent)
      goto retry_gc_have_oldest;
  }

  /* Avoid kick lagging reader(s) if is enough unallocated space
   * at the end of database file. */
  if (!(flags & MDBX_ALLOC_RESERVE) && newnext <= txn->mt_end_pgno) {
    eASSERT(env, pgno == 0);
    goto done;
  }

  if (oldest < txn->mt_txnid - xMDBX_TXNID_STEP) {
    oldest = kick_longlived_readers(env, oldest);
    if (oldest >= detent)
      goto retry_gc_have_oldest;
  }

  //---------------------------------------------------------------------------

no_gc:
  eASSERT(env, pgno == 0);
#ifndef MDBX_ENABLE_BACKLOG_DEPLETED
#define MDBX_ENABLE_BACKLOG_DEPLETED 0
#endif /* MDBX_ENABLE_BACKLOG_DEPLETED*/
  if (MDBX_ENABLE_BACKLOG_DEPLETED &&
      unlikely(!(txn->mt_flags & MDBX_TXN_DRAINED_GC))) {
    ret.err = MDBX_BACKLOG_DEPLETED;
    goto fail;
  }
  if (flags & MDBX_ALLOC_RESERVE) {
    ret.err = MDBX_NOTFOUND;
    goto fail;
  }

  /* Will use new pages from the map if nothing is suitable in the GC. */
  newnext = txn->mt_next_pgno + num;
  if (newnext <= txn->mt_end_pgno)
    goto done;

  if (newnext > txn->mt_geo.upper || !txn->mt_geo.grow_pv) {
    NOTICE("gc-alloc: next %zu > upper %" PRIaPGNO, newnext, txn->mt_geo.upper);
    ret.err = MDBX_MAP_FULL;
    goto fail;
  }

  eASSERT(env, newnext > txn->mt_end_pgno);
  const size_t grow_step = pv2pages(txn->mt_geo.grow_pv);
  size_t aligned = pgno_align2os_pgno(
      env, (pgno_t)(newnext + grow_step - newnext % grow_step));

  if (aligned > txn->mt_geo.upper)
    aligned = txn->mt_geo.upper;
  eASSERT(env, aligned >= newnext);

  VERBOSE("try growth datafile to %zu pages (+%zu)", aligned,
          aligned - txn->mt_end_pgno);
  ret.err = dxb_resize(env, txn->mt_next_pgno, (pgno_t)aligned,
                       txn->mt_geo.upper, implicit_grow);
  if (ret.err != MDBX_SUCCESS) {
    ERROR("unable growth datafile to %zu pages (+%zu), errcode %d", aligned,
          aligned - txn->mt_end_pgno, ret.err);
    goto fail;
  }
  env->me_txn->mt_end_pgno = (pgno_t)aligned;
  eASSERT(env, pgno == 0);

  //---------------------------------------------------------------------------

done:
  ret.err = MDBX_SUCCESS;
  if (likely((flags & MDBX_ALLOC_RESERVE) == 0)) {
    if (pgno) {
      eASSERT(env, pgno + num <= txn->mt_next_pgno && pgno >= NUM_METAS);
      eASSERT(env, pnl_check_allocated(txn->tw.relist,
                                       txn->mt_next_pgno - MDBX_ENABLE_REFUND));
    } else {
      pgno = txn->mt_next_pgno;
      txn->mt_next_pgno += (pgno_t)num;
      eASSERT(env, txn->mt_next_pgno <= txn->mt_end_pgno);
      eASSERT(env, pgno >= NUM_METAS && pgno + num <= txn->mt_next_pgno);
    }

    ret = page_alloc_finalize(env, txn, mc, pgno, num);
    if (unlikely(ret.err != MDBX_SUCCESS)) {
    fail:
      eASSERT(env, ret.err != MDBX_SUCCESS);
      eASSERT(env, pnl_check_allocated(txn->tw.relist,
                                       txn->mt_next_pgno - MDBX_ENABLE_REFUND));
      int level;
      const char *what;
      if (flags & MDBX_ALLOC_RESERVE) {
        level =
            (flags & MDBX_ALLOC_UNIMPORTANT) ? MDBX_LOG_DEBUG : MDBX_LOG_NOTICE;
        what = num ? "reserve-pages" : "fetch-slot";
      } else {
        txn->mt_flags |= MDBX_TXN_ERROR;
        level = MDBX_LOG_ERROR;
        what = "pages";
      }
      if (LOG_ENABLED(level))
        debug_log(level, __func__, __LINE__,
                  "unable alloc %zu %s, alloc-flags 0x%x, err %d, txn-flags "
                  "0x%x, re-list-len %zu, loose-count %zu, gc: height %u, "
                  "branch %zu, leaf %zu, large %zu, entries %zu\n",
                  num, what, flags, ret.err, txn->mt_flags,
                  MDBX_PNL_GETSIZE(txn->tw.relist), txn->tw.loose_count,
                  txn->mt_dbs[FREE_DBI].md_depth,
                  (size_t)txn->mt_dbs[FREE_DBI].md_branch_pages,
                  (size_t)txn->mt_dbs[FREE_DBI].md_leaf_pages,
                  (size_t)txn->mt_dbs[FREE_DBI].md_overflow_pages,
                  (size_t)txn->mt_dbs[FREE_DBI].md_entries);
      ret.page = NULL;
    }
  } else {
  early_exit:
    DEBUG("return NULL for %zu pages for ALLOC_%s, rc %d", num,
          num ? "RESERVE" : "SLOT", ret.err);
    ret.page = NULL;
  }

#if MDBX_ENABLE_PROFGC
  prof->rtime_monotonic += osal_monotime() - monotime_before;
#endif /* MDBX_ENABLE_PROFGC */
  return ret;
}

__hot static pgr_t page_alloc(const MDBX_cursor *const mc) {
  MDBX_txn *const txn = mc->mc_txn;
  tASSERT(txn, mc->mc_txn->mt_flags & MDBX_TXN_DIRTY);
  tASSERT(txn, F_ISSET(txn->mt_dbistate[mc->mc_dbi], DBI_DIRTY | DBI_VALID));

  /* If there are any loose pages, just use them */
  while (likely(txn->tw.loose_pages)) {
#if MDBX_ENABLE_REFUND
    if (unlikely(txn->tw.loose_refund_wl > txn->mt_next_pgno)) {
      txn_refund(txn);
      if (!txn->tw.loose_pages)
        break;
    }
#endif /* MDBX_ENABLE_REFUND */

    MDBX_page *lp = txn->tw.loose_pages;
    MDBX_ASAN_UNPOISON_MEMORY_REGION(lp, txn->mt_env->me_psize);
    VALGRIND_MAKE_MEM_DEFINED(&mp_next(lp), sizeof(MDBX_page *));
    txn->tw.loose_pages = mp_next(lp);
    txn->tw.loose_count--;
    DEBUG_EXTRA("db %d use loose page %" PRIaPGNO, DDBI(mc), lp->mp_pgno);
    tASSERT(txn, lp->mp_pgno < txn->mt_next_pgno);
    tASSERT(txn, lp->mp_pgno >= NUM_METAS);
    VALGRIND_MAKE_MEM_UNDEFINED(page_data(lp), page_space(txn->mt_env));
    lp->mp_txnid = txn->mt_front;
    pgr_t ret = {lp, MDBX_SUCCESS};
    return ret;
  }

  if (likely(MDBX_PNL_GETSIZE(txn->tw.relist) > 0))
    return page_alloc_finalize(txn->mt_env, txn, mc, relist_get_single(txn), 1);

  return page_alloc_slowpath(mc, 1, MDBX_ALLOC_DEFAULT);
}

/* Copy the used portions of a page. */
__hot static void page_copy(MDBX_page *const dst, const MDBX_page *const src,
                            const size_t size) {
  STATIC_ASSERT(UINT16_MAX > MAX_PAGESIZE - PAGEHDRSZ);
  STATIC_ASSERT(MIN_PAGESIZE > PAGEHDRSZ + NODESIZE * 4);
  void *copy_dst = dst;
  const void *copy_src = src;
  size_t copy_len = size;
  if (src->mp_flags & P_LEAF2) {
    copy_len = PAGEHDRSZ + src->mp_leaf2_ksize * page_numkeys(src);
    if (unlikely(copy_len > size))
      goto bailout;
  }
  if ((src->mp_flags & (P_LEAF2 | P_OVERFLOW)) == 0) {
    size_t upper = src->mp_upper, lower = src->mp_lower;
    intptr_t unused = upper - lower;
    /* If page isn't full, just copy the used portion. Adjust
     * alignment so memcpy may copy words instead of bytes. */
    if (unused > MDBX_CACHELINE_SIZE * 3) {
      lower = ceil_powerof2(lower + PAGEHDRSZ, sizeof(void *));
      upper = floor_powerof2(upper + PAGEHDRSZ, sizeof(void *));
      if (unlikely(upper > copy_len))
        goto bailout;
      memcpy(copy_dst, copy_src, lower);
      copy_dst = ptr_disp(copy_dst, upper);
      copy_src = ptr_disp(copy_src, upper);
      copy_len -= upper;
    }
  }
  memcpy(copy_dst, copy_src, copy_len);
  return;

bailout:
  if (src->mp_flags & P_LEAF2)
    bad_page(src, "%s addr %p, n-keys %zu, ksize %u",
             "invalid/corrupted source page", __Wpedantic_format_voidptr(src),
             page_numkeys(src), src->mp_leaf2_ksize);
  else
    bad_page(src, "%s addr %p, upper %u", "invalid/corrupted source page",
             __Wpedantic_format_voidptr(src), src->mp_upper);
  memset(dst, -1, size);
}

/* Pull a page off the txn's spill list, if present.
 *
 * If a page being referenced was spilled to disk in this txn, bring
 * it back and make it dirty/writable again. */
static pgr_t __must_check_result page_unspill(MDBX_txn *const txn,
                                              const MDBX_page *const mp) {
  VERBOSE("unspill page %" PRIaPGNO, mp->mp_pgno);
  tASSERT(txn, (txn->mt_flags & MDBX_WRITEMAP) == 0);
  tASSERT(txn, IS_SPILLED(txn, mp));
  const MDBX_txn *scan = txn;
  pgr_t ret;
  do {
    tASSERT(txn, (scan->mt_flags & MDBX_TXN_SPILLS) != 0);
    const size_t si = search_spilled(scan, mp->mp_pgno);
    if (!si)
      continue;
    const unsigned npages = IS_OVERFLOW(mp) ? mp->mp_pages : 1;
    ret.page = page_malloc(txn, npages);
    if (unlikely(!ret.page)) {
      ret.err = MDBX_ENOMEM;
      return ret;
    }
    page_copy(ret.page, mp, pgno2bytes(txn->mt_env, npages));
    if (scan == txn) {
      /* If in current txn, this page is no longer spilled.
       * If it happens to be the last page, truncate the spill list.
       * Otherwise mark it as deleted by setting the LSB. */
      spill_remove(txn, si, npages);
    } /* otherwise, if belonging to a parent txn, the
       * page remains spilled until child commits */

    ret.err = page_dirty(txn, ret.page, npages);
    if (unlikely(ret.err != MDBX_SUCCESS))
      return ret;
#if MDBX_ENABLE_PGOP_STAT
    txn->mt_env->me_lck->mti_pgop_stat.unspill.weak += npages;
#endif /* MDBX_ENABLE_PGOP_STAT */
    ret.page->mp_flags |= (scan == txn) ? 0 : P_SPILLED;
    ret.err = MDBX_SUCCESS;
    return ret;
  } while (likely((scan = scan->mt_parent) != nullptr &&
                  (scan->mt_flags & MDBX_TXN_SPILLS) != 0));
  ERROR("Page %" PRIaPGNO " mod-txnid %" PRIaTXN
        " not found in the spill-list(s), current txn %" PRIaTXN
        " front %" PRIaTXN ", root txn %" PRIaTXN " front %" PRIaTXN,
        mp->mp_pgno, mp->mp_txnid, txn->mt_txnid, txn->mt_front,
        txn->mt_env->me_txn0->mt_txnid, txn->mt_env->me_txn0->mt_front);
  ret.err = MDBX_PROBLEM;
  ret.page = NULL;
  return ret;
}

/* Touch a page: make it dirty and re-insert into tree with updated pgno.
 * Set MDBX_TXN_ERROR on failure.
 *
 * [in] mc  cursor pointing to the page to be touched
 *
 * Returns 0 on success, non-zero on failure. */
__hot static int page_touch(MDBX_cursor *mc) {
  const MDBX_page *const mp = mc->mc_pg[mc->mc_top];
  MDBX_page *np;
  MDBX_txn *txn = mc->mc_txn;
  int rc;

  tASSERT(txn, mc->mc_txn->mt_flags & MDBX_TXN_DIRTY);
  tASSERT(txn, F_ISSET(*mc->mc_dbistate, DBI_DIRTY | DBI_VALID));
  tASSERT(txn, !IS_OVERFLOW(mp));
  if (ASSERT_ENABLED()) {
    if (mc->mc_flags & C_SUB) {
      MDBX_xcursor *mx = container_of(mc->mc_db, MDBX_xcursor, mx_db);
      MDBX_cursor_couple *couple = container_of(mx, MDBX_cursor_couple, inner);
      tASSERT(txn, mc->mc_db == &couple->outer.mc_xcursor->mx_db);
      tASSERT(txn, mc->mc_dbx == &couple->outer.mc_xcursor->mx_dbx);
      tASSERT(txn, *couple->outer.mc_dbistate & DBI_DIRTY);
    }
    tASSERT(txn, dirtylist_check(txn));
  }

  if (IS_MODIFIABLE(txn, mp)) {
    if (!txn->tw.dirtylist) {
      tASSERT(txn, (txn->mt_flags & MDBX_WRITEMAP) && !MDBX_AVOID_MSYNC);
      return MDBX_SUCCESS;
    }
    if (IS_SUBP(mp))
      return MDBX_SUCCESS;
    tASSERT(txn, (txn->mt_flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);
    const size_t n = dpl_search(txn, mp->mp_pgno);
    if (MDBX_AVOID_MSYNC &&
        unlikely(txn->tw.dirtylist->items[n].pgno != mp->mp_pgno)) {
      tASSERT(txn, (txn->mt_flags & MDBX_WRITEMAP));
      tASSERT(txn, n > 0 && n <= txn->tw.dirtylist->length + 1);
      VERBOSE("unspill page %" PRIaPGNO, mp->mp_pgno);
      np = (MDBX_page *)mp;
#if MDBX_ENABLE_PGOP_STAT
      txn->mt_env->me_lck->mti_pgop_stat.unspill.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
      return page_dirty(txn, np, 1);
    }
    tASSERT(txn, n > 0 && n <= txn->tw.dirtylist->length);
    tASSERT(txn, txn->tw.dirtylist->items[n].pgno == mp->mp_pgno &&
                     txn->tw.dirtylist->items[n].ptr == mp);
    if (!MDBX_AVOID_MSYNC || (txn->mt_flags & MDBX_WRITEMAP) == 0) {
      size_t *const ptr =
          ptr_disp(txn->tw.dirtylist->items[n].ptr, -(ptrdiff_t)sizeof(size_t));
      *ptr = txn->tw.dirtylru;
    }
    return MDBX_SUCCESS;
  }
  if (IS_SUBP(mp)) {
    np = (MDBX_page *)mp;
    np->mp_txnid = txn->mt_front;
    return MDBX_SUCCESS;
  }
  tASSERT(txn, !IS_OVERFLOW(mp) && !IS_SUBP(mp));

  if (IS_FROZEN(txn, mp)) {
    /* CoW the page */
    rc = pnl_need(&txn->tw.retired_pages, 1);
    if (unlikely(rc != MDBX_SUCCESS))
      goto fail;
    const pgr_t par = page_alloc(mc);
    rc = par.err;
    np = par.page;
    if (unlikely(rc != MDBX_SUCCESS))
      goto fail;

    const pgno_t pgno = np->mp_pgno;
    DEBUG("touched db %d page %" PRIaPGNO " -> %" PRIaPGNO, DDBI(mc),
          mp->mp_pgno, pgno);
    tASSERT(txn, mp->mp_pgno != pgno);
    pnl_xappend(txn->tw.retired_pages, mp->mp_pgno);
    /* Update the parent page, if any, to point to the new page */
    if (mc->mc_top) {
      MDBX_page *parent = mc->mc_pg[mc->mc_top - 1];
      MDBX_node *node = page_node(parent, mc->mc_ki[mc->mc_top - 1]);
      node_set_pgno(node, pgno);
    } else {
      mc->mc_db->md_root = pgno;
    }

#if MDBX_ENABLE_PGOP_STAT
    txn->mt_env->me_lck->mti_pgop_stat.cow.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
    page_copy(np, mp, txn->mt_env->me_psize);
    np->mp_pgno = pgno;
    np->mp_txnid = txn->mt_front;
  } else if (IS_SPILLED(txn, mp)) {
    pgr_t pur = page_unspill(txn, mp);
    np = pur.page;
    rc = pur.err;
    if (likely(rc == MDBX_SUCCESS)) {
      tASSERT(txn, np != nullptr);
      goto done;
    }
    goto fail;
  } else {
    if (unlikely(!txn->mt_parent)) {
      ERROR("Unexpected not frozen/modifiable/spilled but shadowed %s "
            "page %" PRIaPGNO " mod-txnid %" PRIaTXN ","
            " without parent transaction, current txn %" PRIaTXN
            " front %" PRIaTXN,
            IS_BRANCH(mp) ? "branch" : "leaf", mp->mp_pgno, mp->mp_txnid,
            mc->mc_txn->mt_txnid, mc->mc_txn->mt_front);
      rc = MDBX_PROBLEM;
      goto fail;
    }

    DEBUG("clone db %d page %" PRIaPGNO, DDBI(mc), mp->mp_pgno);
    tASSERT(txn,
            txn->tw.dirtylist->length <= MDBX_PGL_LIMIT + MDBX_PNL_GRANULATE);
    /* No - copy it */
    np = page_malloc(txn, 1);
    if (unlikely(!np)) {
      rc = MDBX_ENOMEM;
      goto fail;
    }
    page_copy(np, mp, txn->mt_env->me_psize);

    /* insert a clone of parent's dirty page, so don't touch dirtyroom */
    rc = page_dirty(txn, np, 1);
    if (unlikely(rc != MDBX_SUCCESS))
      goto fail;

#if MDBX_ENABLE_PGOP_STAT
    txn->mt_env->me_lck->mti_pgop_stat.clone.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
  }

done:
  /* Adjust cursors pointing to mp */
  mc->mc_pg[mc->mc_top] = np;
  MDBX_cursor *m2 = txn->mt_cursors[mc->mc_dbi];
  if (mc->mc_flags & C_SUB) {
    for (; m2; m2 = m2->mc_next) {
      MDBX_cursor *m3 = &m2->mc_xcursor->mx_cursor;
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

static int meta_sync(const MDBX_env *env, const meta_ptr_t head) {
  eASSERT(env, atomic_load32(&env->me_lck->mti_meta_sync_txnid, mo_Relaxed) !=
                   (uint32_t)head.txnid);
  /* Функция может вызываться (в том числе) при (env->me_flags &
   * MDBX_NOMETASYNC) == 0 и env->me_fd4meta == env->me_dsync_fd, например если
   * предыдущая транзакция была выполненна с флагом MDBX_NOMETASYNC. */

  int rc = MDBX_RESULT_TRUE;
  if (env->me_flags & MDBX_WRITEMAP) {
    if (!MDBX_AVOID_MSYNC) {
      rc = osal_msync(&env->me_dxb_mmap, 0, pgno_align2os_bytes(env, NUM_METAS),
                      MDBX_SYNC_DATA | MDBX_SYNC_IODQ);
#if MDBX_ENABLE_PGOP_STAT
      env->me_lck->mti_pgop_stat.msync.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
    } else {
#if MDBX_ENABLE_PGOP_ST
      env->me_lck->mti_pgop_stat.wops.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
      const MDBX_page *page = data_page(head.ptr_c);
      rc = osal_pwrite(env->me_fd4meta, page, env->me_psize,
                       ptr_dist(page, env->me_map));

      if (likely(rc == MDBX_SUCCESS) && env->me_fd4meta == env->me_lazy_fd) {
        rc = osal_fsync(env->me_lazy_fd, MDBX_SYNC_DATA | MDBX_SYNC_IODQ);
#if MDBX_ENABLE_PGOP_STAT
        env->me_lck->mti_pgop_stat.fsync.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
      }
    }
  } else {
    rc = osal_fsync(env->me_lazy_fd, MDBX_SYNC_DATA | MDBX_SYNC_IODQ);
#if MDBX_ENABLE_PGOP_STAT
    env->me_lck->mti_pgop_stat.fsync.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
  }

  if (likely(rc == MDBX_SUCCESS))
    env->me_lck->mti_meta_sync_txnid.weak = (uint32_t)head.txnid;
  return rc;
}

__cold static int env_sync(MDBX_env *env, bool force, bool nonblock) {
  bool locked = false;
  int rc = MDBX_RESULT_TRUE /* means "nothing to sync" */;

retry:;
  unsigned flags = env->me_flags & ~(MDBX_NOMETASYNC | MDBX_SHRINK_ALLOWED);
  if (unlikely((flags & (MDBX_RDONLY | MDBX_FATAL_ERROR | MDBX_ENV_ACTIVE)) !=
               MDBX_ENV_ACTIVE)) {
    rc = MDBX_EACCESS;
    if (!(flags & MDBX_ENV_ACTIVE))
      rc = MDBX_EPERM;
    if (flags & MDBX_FATAL_ERROR)
      rc = MDBX_PANIC;
    goto bailout;
  }

  const bool inside_txn = (env->me_txn0->mt_owner == osal_thread_self());
  const meta_troika_t troika =
      (inside_txn | locked) ? env->me_txn0->tw.troika : meta_tap(env);
  const meta_ptr_t head = meta_recent(env, &troika);
  const uint64_t unsynced_pages =
      atomic_load64(&env->me_lck->mti_unsynced_pages, mo_Relaxed);
  if (unsynced_pages == 0) {
    const uint32_t synched_meta_txnid_u32 =
        atomic_load32(&env->me_lck->mti_meta_sync_txnid, mo_Relaxed);
    if (synched_meta_txnid_u32 == (uint32_t)head.txnid && head.is_steady)
      goto bailout;
  }

  if (!inside_txn && locked && (env->me_flags & MDBX_WRITEMAP) &&
      unlikely(head.ptr_c->mm_geo.next >
               bytes2pgno(env, env->me_dxb_mmap.current))) {

    if (unlikely(env->me_stuck_meta >= 0) &&
        troika.recent != (uint8_t)env->me_stuck_meta) {
      NOTICE("skip %s since wagering meta-page (%u) is mispatch the recent "
             "meta-page (%u)",
             "sync datafile", env->me_stuck_meta, troika.recent);
      rc = MDBX_RESULT_TRUE;
    } else {
      rc = dxb_resize(env, head.ptr_c->mm_geo.next, head.ptr_c->mm_geo.now,
                      head.ptr_c->mm_geo.upper, implicit_grow);
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;
    }
  }

  const size_t autosync_threshold =
      atomic_load32(&env->me_lck->mti_autosync_threshold, mo_Relaxed);
  const uint64_t autosync_period =
      atomic_load64(&env->me_lck->mti_autosync_period, mo_Relaxed);
  uint64_t eoos_timestamp;
  if (force || (autosync_threshold && unsynced_pages >= autosync_threshold) ||
      (autosync_period &&
       (eoos_timestamp =
            atomic_load64(&env->me_lck->mti_eoos_timestamp, mo_Relaxed)) &&
       osal_monotime() - eoos_timestamp >= autosync_period))
    flags &= MDBX_WRITEMAP /* clear flags for full steady sync */;

  if (!inside_txn) {
    if (!locked) {
#if MDBX_ENABLE_PGOP_STAT
      unsigned wops = 0;
#endif /* MDBX_ENABLE_PGOP_STAT */

      int err;
      /* pre-sync to avoid latency for writer */
      if (unsynced_pages > /* FIXME: define threshold */ 42 &&
          (flags & MDBX_SAFE_NOSYNC) == 0) {
        eASSERT(env, ((flags ^ env->me_flags) & MDBX_WRITEMAP) == 0);
        if (flags & MDBX_WRITEMAP) {
          /* Acquire guard to avoid collision with remap */
#if defined(_WIN32) || defined(_WIN64)
          osal_srwlock_AcquireShared(&env->me_remap_guard);
#else
          err = osal_fastmutex_acquire(&env->me_remap_guard);
          if (unlikely(err != MDBX_SUCCESS))
            return err;
#endif
          const size_t usedbytes =
              pgno_align2os_bytes(env, head.ptr_c->mm_geo.next);
          err = osal_msync(&env->me_dxb_mmap, 0, usedbytes, MDBX_SYNC_DATA);
#if defined(_WIN32) || defined(_WIN64)
          osal_srwlock_ReleaseShared(&env->me_remap_guard);
#else
          int unlock_err = osal_fastmutex_release(&env->me_remap_guard);
          if (unlikely(unlock_err != MDBX_SUCCESS) && err == MDBX_SUCCESS)
            err = unlock_err;
#endif
        } else
          err = osal_fsync(env->me_lazy_fd, MDBX_SYNC_DATA);

        if (unlikely(err != MDBX_SUCCESS))
          return err;

#if MDBX_ENABLE_PGOP_STAT
        wops = 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
        /* pre-sync done */
        rc = MDBX_SUCCESS /* means "some data was synced" */;
      }

      err = mdbx_txn_lock(env, nonblock);
      if (unlikely(err != MDBX_SUCCESS))
        return err;

      locked = true;
#if MDBX_ENABLE_PGOP_STAT
      env->me_lck->mti_pgop_stat.wops.weak += wops;
#endif /* MDBX_ENABLE_PGOP_STAT */
      env->me_txn0->tw.troika = meta_tap(env);
      eASSERT(env, !env->me_txn && !env->me_txn0->mt_child);
      goto retry;
    }
    eASSERT(env, head.txnid == recent_committed_txnid(env));
    env->me_txn0->mt_txnid = head.txnid;
    txn_oldest_reader(env->me_txn0);
    flags |= MDBX_SHRINK_ALLOWED;
  }

  eASSERT(env, inside_txn || locked);
  eASSERT(env, !inside_txn || (flags & MDBX_SHRINK_ALLOWED) == 0);

  if (!head.is_steady && unlikely(env->me_stuck_meta >= 0) &&
      troika.recent != (uint8_t)env->me_stuck_meta) {
    NOTICE("skip %s since wagering meta-page (%u) is mispatch the recent "
           "meta-page (%u)",
           "sync datafile", env->me_stuck_meta, troika.recent);
    rc = MDBX_RESULT_TRUE;
    goto bailout;
  }
  if (!head.is_steady || ((flags & MDBX_SAFE_NOSYNC) == 0 && unsynced_pages)) {
    DEBUG("meta-head %" PRIaPGNO ", %s, sync_pending %" PRIu64,
          data_page(head.ptr_c)->mp_pgno, durable_caption(head.ptr_c),
          unsynced_pages);
    MDBX_meta meta = *head.ptr_c;
    rc = sync_locked(env, flags, &meta, &env->me_txn0->tw.troika);
    if (unlikely(rc != MDBX_SUCCESS))
      goto bailout;
  }

  /* LY: sync meta-pages if MDBX_NOMETASYNC enabled
   *     and someone was not synced above. */
  if (atomic_load32(&env->me_lck->mti_meta_sync_txnid, mo_Relaxed) !=
      (uint32_t)head.txnid)
    rc = meta_sync(env, head);

bailout:
  if (locked)
    mdbx_txn_unlock(env);
  return rc;
}

static __inline int check_env(const MDBX_env *env, const bool wanna_active) {
  if (unlikely(!env))
    return MDBX_EINVAL;

  if (unlikely(env->me_signature.weak != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(env->me_flags & MDBX_FATAL_ERROR))
    return MDBX_PANIC;

  if (wanna_active) {
#if MDBX_ENV_CHECKPID
    if (unlikely(env->me_pid != osal_getpid()) && env->me_pid) {
      ((MDBX_env *)env)->me_flags |= MDBX_FATAL_ERROR;
      return MDBX_PANIC;
    }
#endif /* MDBX_ENV_CHECKPID */
    if (unlikely((env->me_flags & MDBX_ENV_ACTIVE) == 0))
      return MDBX_EPERM;
    eASSERT(env, env->me_map != nullptr);
  }

  return MDBX_SUCCESS;
}

__cold int mdbx_env_sync_ex(MDBX_env *env, bool force, bool nonblock) {
  int rc = check_env(env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  return env_sync(env, force, nonblock);
}

/* Back up parent txn's cursors, then grab the originals for tracking */
static int cursor_shadow(MDBX_txn *parent, MDBX_txn *nested) {
  tASSERT(parent, parent->mt_cursors[FREE_DBI] == nullptr);
  nested->mt_cursors[FREE_DBI] = nullptr;
  for (int i = parent->mt_numdbs; --i > FREE_DBI;) {
    nested->mt_cursors[i] = NULL;
    MDBX_cursor *mc = parent->mt_cursors[i];
    if (mc != NULL) {
      size_t size = mc->mc_xcursor ? sizeof(MDBX_cursor) + sizeof(MDBX_xcursor)
                                   : sizeof(MDBX_cursor);
      for (MDBX_cursor *bk; mc; mc = bk->mc_next) {
        bk = mc;
        if (mc->mc_signature != MDBX_MC_LIVE)
          continue;
        bk = osal_malloc(size);
        if (unlikely(!bk))
          return MDBX_ENOMEM;
#if MDBX_DEBUG
        memset(bk, 0xCD, size);
        VALGRIND_MAKE_MEM_UNDEFINED(bk, size);
#endif /* MDBX_DEBUG */
        *bk = *mc;
        mc->mc_backup = bk;
        /* Kill pointers into src to reduce abuse: The
         * user may not use mc until dst ends. But we need a valid
         * txn pointer here for cursor fixups to keep working. */
        mc->mc_txn = nested;
        mc->mc_db = &nested->mt_dbs[i];
        mc->mc_dbistate = &nested->mt_dbistate[i];
        MDBX_xcursor *mx = mc->mc_xcursor;
        if (mx != NULL) {
          *(MDBX_xcursor *)(bk + 1) = *mx;
          mx->mx_cursor.mc_txn = mc->mc_txn;
          mx->mx_cursor.mc_dbistate = mc->mc_dbistate;
        }
        mc->mc_next = nested->mt_cursors[i];
        nested->mt_cursors[i] = mc;
      }
    }
  }
  return MDBX_SUCCESS;
}

/* Close this txn's cursors, give parent txn's cursors back to parent.
 *
 * [in] txn     the transaction handle.
 * [in] merge   true to keep changes to parent cursors, false to revert.
 *
 * Returns 0 on success, non-zero on failure. */
static void cursors_eot(MDBX_txn *txn, const bool merge) {
  tASSERT(txn, txn->mt_cursors[FREE_DBI] == nullptr);
  for (intptr_t i = txn->mt_numdbs; --i > FREE_DBI;) {
    MDBX_cursor *mc = txn->mt_cursors[i];
    if (!mc)
      continue;
    txn->mt_cursors[i] = nullptr;
    do {
      const unsigned stage = mc->mc_signature;
      MDBX_cursor *const next = mc->mc_next;
      MDBX_cursor *const bk = mc->mc_backup;
      ENSURE(txn->mt_env,
             stage == MDBX_MC_LIVE || (stage == MDBX_MC_WAIT4EOT && bk));
      cASSERT(mc, mc->mc_dbi == (MDBX_dbi)i);
      if (bk) {
        MDBX_xcursor *mx = mc->mc_xcursor;
        tASSERT(txn, txn->mt_parent != NULL);
        /* Zap: Using uninitialized memory '*mc->mc_backup'. */
        MDBX_SUPPRESS_GOOFY_MSVC_ANALYZER(6001);
        ENSURE(txn->mt_env, bk->mc_signature == MDBX_MC_LIVE);
        tASSERT(txn, mx == bk->mc_xcursor);
        if (merge) {
          /* Restore pointers to parent txn */
          mc->mc_next = bk->mc_next;
          mc->mc_backup = bk->mc_backup;
          mc->mc_txn = bk->mc_txn;
          mc->mc_db = bk->mc_db;
          mc->mc_dbistate = bk->mc_dbistate;
          if (mx) {
            mx->mx_cursor.mc_txn = mc->mc_txn;
            mx->mx_cursor.mc_dbistate = mc->mc_dbistate;
          }
        } else {
          /* Restore from backup, i.e. rollback/abort nested txn */
          *mc = *bk;
          if (mx)
            *mx = *(MDBX_xcursor *)(bk + 1);
        }
        bk->mc_signature = 0;
        osal_free(bk);
        if (stage == MDBX_MC_WAIT4EOT /* Cursor was closed by user */)
          mc->mc_signature = stage /* Promote closed state to parent txn */;
      } else {
        ENSURE(txn->mt_env, stage == MDBX_MC_LIVE);
        mc->mc_signature = MDBX_MC_READY4CLOSE /* Cursor may be reused */;
        mc->mc_flags = 0 /* reset C_UNTRACK */;
      }
      mc = next;
    } while (mc);
  }
}

#if defined(MDBX_USE_VALGRIND) || defined(__SANITIZE_ADDRESS__)
/* Find largest mvcc-snapshot still referenced by this process. */
static pgno_t find_largest_this(MDBX_env *env, pgno_t largest) {
  MDBX_lockinfo *const lck = env->me_lck_mmap.lck;
  if (likely(lck != NULL /* exclusive mode */)) {
    const size_t snap_nreaders =
        atomic_load32(&lck->mti_numreaders, mo_AcquireRelease);
    for (size_t i = 0; i < snap_nreaders; ++i) {
    retry:
      if (atomic_load32(&lck->mti_readers[i].mr_pid, mo_AcquireRelease) ==
          env->me_pid) {
        /* jitter4testing(true); */
        const pgno_t snap_pages = atomic_load32(
            &lck->mti_readers[i].mr_snapshot_pages_used, mo_Relaxed);
        const txnid_t snap_txnid = safe64_read(&lck->mti_readers[i].mr_txnid);
        if (unlikely(
                snap_pages !=
                    atomic_load32(&lck->mti_readers[i].mr_snapshot_pages_used,
                                  mo_AcquireRelease) ||
                snap_txnid != safe64_read(&lck->mti_readers[i].mr_txnid)))
          goto retry;
        if (largest < snap_pages &&
            atomic_load64(&lck->mti_oldest_reader, mo_AcquireRelease) <=
                /* ignore pending updates */ snap_txnid &&
            snap_txnid <= MAX_TXNID)
          largest = snap_pages;
      }
    }
  }
  return largest;
}

static void txn_valgrind(MDBX_env *env, MDBX_txn *txn) {
#if !defined(__SANITIZE_ADDRESS__)
  if (!RUNNING_ON_VALGRIND)
    return;
#endif

  if (txn) { /* transaction start */
    if (env->me_poison_edge < txn->mt_next_pgno)
      env->me_poison_edge = txn->mt_next_pgno;
    VALGRIND_MAKE_MEM_DEFINED(env->me_map, pgno2bytes(env, txn->mt_next_pgno));
    MDBX_ASAN_UNPOISON_MEMORY_REGION(env->me_map,
                                     pgno2bytes(env, txn->mt_next_pgno));
    /* don't touch more, it should be already poisoned */
  } else { /* transaction end */
    bool should_unlock = false;
    pgno_t last = MAX_PAGENO + 1;
    if (env->me_txn0 && env->me_txn0->mt_owner == osal_thread_self()) {
      /* inside write-txn */
      last = meta_recent(env, &env->me_txn0->tw.troika).ptr_v->mm_geo.next;
    } else if (env->me_flags & MDBX_RDONLY) {
      /* read-only mode, no write-txn, no wlock mutex */
      last = NUM_METAS;
    } else if (mdbx_txn_lock(env, true) == MDBX_SUCCESS) {
      /* no write-txn */
      last = NUM_METAS;
      should_unlock = true;
    } else {
      /* write txn is running, therefore shouldn't poison any memory range */
      return;
    }

    last = find_largest_this(env, last);
    const pgno_t edge = env->me_poison_edge;
    if (edge > last) {
      eASSERT(env, last >= NUM_METAS);
      env->me_poison_edge = last;
      VALGRIND_MAKE_MEM_NOACCESS(ptr_disp(env->me_map, pgno2bytes(env, last)),
                                 pgno2bytes(env, edge - last));
      MDBX_ASAN_POISON_MEMORY_REGION(
          ptr_disp(env->me_map, pgno2bytes(env, last)),
          pgno2bytes(env, edge - last));
    }
    if (should_unlock)
      mdbx_txn_unlock(env);
  }
}
#endif /* MDBX_USE_VALGRIND || __SANITIZE_ADDRESS__ */

typedef struct {
  int err;
  MDBX_reader *rslot;
} bind_rslot_result;

static bind_rslot_result bind_rslot(MDBX_env *env, const uintptr_t tid) {
  eASSERT(env, env->me_lck_mmap.lck);
  eASSERT(env, env->me_lck->mti_magic_and_version == MDBX_LOCK_MAGIC);
  eASSERT(env, env->me_lck->mti_os_and_format == MDBX_LOCK_FORMAT);

  bind_rslot_result result = {osal_rdt_lock(env), nullptr};
  if (unlikely(MDBX_IS_ERROR(result.err)))
    return result;
  if (unlikely(env->me_flags & MDBX_FATAL_ERROR)) {
    osal_rdt_unlock(env);
    result.err = MDBX_PANIC;
    return result;
  }
  if (unlikely(!env->me_map)) {
    osal_rdt_unlock(env);
    result.err = MDBX_EPERM;
    return result;
  }

  if (unlikely(env->me_live_reader != env->me_pid)) {
    result.err = osal_rpid_set(env);
    if (unlikely(result.err != MDBX_SUCCESS)) {
      osal_rdt_unlock(env);
      return result;
    }
    env->me_live_reader = env->me_pid;
  }

  result.err = MDBX_SUCCESS;
  size_t slot, nreaders;
  while (1) {
    nreaders = env->me_lck->mti_numreaders.weak;
    for (slot = 0; slot < nreaders; slot++)
      if (!atomic_load32(&env->me_lck->mti_readers[slot].mr_pid,
                         mo_AcquireRelease))
        break;

    if (likely(slot < env->me_maxreaders))
      break;

    result.err = cleanup_dead_readers(env, true, NULL);
    if (result.err != MDBX_RESULT_TRUE) {
      osal_rdt_unlock(env);
      result.err =
          (result.err == MDBX_SUCCESS) ? MDBX_READERS_FULL : result.err;
      return result;
    }
  }

  result.rslot = &env->me_lck->mti_readers[slot];
  /* Claim the reader slot, carefully since other code
   * uses the reader table un-mutexed: First reset the
   * slot, next publish it in lck->mti_numreaders.  After
   * that, it is safe for mdbx_env_close() to touch it.
   * When it will be closed, we can finally claim it. */
  atomic_store32(&result.rslot->mr_pid, 0, mo_AcquireRelease);
  safe64_reset(&result.rslot->mr_txnid, true);
  if (slot == nreaders)
    env->me_lck->mti_numreaders.weak = (uint32_t)++nreaders;
  result.rslot->mr_tid.weak = (env->me_flags & MDBX_NOTLS) ? 0 : tid;
  atomic_store32(&result.rslot->mr_pid, env->me_pid, mo_AcquireRelease);
  osal_rdt_unlock(env);

  if (likely(env->me_flags & MDBX_ENV_TXKEY)) {
    eASSERT(env, env->me_live_reader == env->me_pid);
    thread_rthc_set(env->me_txkey, result.rslot);
  }
  return result;
}

__cold int mdbx_thread_register(const MDBX_env *env) {
  int rc = check_env(env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!env->me_lck_mmap.lck))
    return (env->me_flags & MDBX_EXCLUSIVE) ? MDBX_EINVAL : MDBX_EPERM;

  if (unlikely((env->me_flags & MDBX_ENV_TXKEY) == 0)) {
    eASSERT(env, !env->me_lck_mmap.lck || (env->me_flags & MDBX_NOTLS));
    return MDBX_EINVAL /* MDBX_NOTLS mode */;
  }

  eASSERT(env, (env->me_flags & (MDBX_NOTLS | MDBX_ENV_TXKEY |
                                 MDBX_EXCLUSIVE)) == MDBX_ENV_TXKEY);
  MDBX_reader *r = thread_rthc_get(env->me_txkey);
  if (unlikely(r != NULL)) {
    eASSERT(env, r->mr_pid.weak == env->me_pid);
    eASSERT(env, r->mr_tid.weak == osal_thread_self());
    if (unlikely(r->mr_pid.weak != env->me_pid))
      return MDBX_BAD_RSLOT;
    return MDBX_RESULT_TRUE /* already registered */;
  }

  const uintptr_t tid = osal_thread_self();
  if (env->me_txn0 && unlikely(env->me_txn0->mt_owner == tid))
    return MDBX_TXN_OVERLAPPING;
  return bind_rslot((MDBX_env *)env, tid).err;
}

__cold int mdbx_thread_unregister(const MDBX_env *env) {
  int rc = check_env(env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!env->me_lck_mmap.lck))
    return MDBX_RESULT_TRUE;

  if (unlikely((env->me_flags & MDBX_ENV_TXKEY) == 0)) {
    eASSERT(env, !env->me_lck_mmap.lck || (env->me_flags & MDBX_NOTLS));
    return MDBX_RESULT_TRUE /* MDBX_NOTLS mode */;
  }

  eASSERT(env, (env->me_flags & (MDBX_NOTLS | MDBX_ENV_TXKEY |
                                 MDBX_EXCLUSIVE)) == MDBX_ENV_TXKEY);
  MDBX_reader *r = thread_rthc_get(env->me_txkey);
  if (unlikely(r == NULL))
    return MDBX_RESULT_TRUE /* not registered */;

  eASSERT(env, r->mr_pid.weak == env->me_pid);
  eASSERT(env, r->mr_tid.weak == osal_thread_self());
  if (unlikely(r->mr_pid.weak != env->me_pid ||
               r->mr_tid.weak != osal_thread_self()))
    return MDBX_BAD_RSLOT;

  eASSERT(env, r->mr_txnid.weak >= SAFE64_INVALID_THRESHOLD);
  if (unlikely(r->mr_txnid.weak < SAFE64_INVALID_THRESHOLD))
    return MDBX_BUSY /* transaction is still active */;

  atomic_store32(&r->mr_pid, 0, mo_Relaxed);
  atomic_store32(&env->me_lck->mti_readers_refresh_flag, true,
                 mo_AcquireRelease);
  thread_rthc_set(env->me_txkey, nullptr);
  return MDBX_SUCCESS;
}

/* check against https://libmdbx.dqdkfa.ru/dead-github/issues/269 */
static bool coherency_check(const MDBX_env *env, const txnid_t txnid,
                            const volatile MDBX_db *dbs,
                            const volatile MDBX_meta *meta, bool report) {
  const txnid_t freedb_mod_txnid = dbs[FREE_DBI].md_mod_txnid;
  const txnid_t maindb_mod_txnid = dbs[MAIN_DBI].md_mod_txnid;
  const pgno_t last_pgno = meta->mm_geo.now;

  const pgno_t freedb_root_pgno = dbs[FREE_DBI].md_root;
  const MDBX_page *freedb_root = (env->me_map && freedb_root_pgno < last_pgno)
                                     ? pgno2page(env, freedb_root_pgno)
                                     : nullptr;

  const pgno_t maindb_root_pgno = dbs[MAIN_DBI].md_root;
  const MDBX_page *maindb_root = (env->me_map && maindb_root_pgno < last_pgno)
                                     ? pgno2page(env, maindb_root_pgno)
                                     : nullptr;
  const uint64_t magic_and_version =
      unaligned_peek_u64_volatile(4, &meta->mm_magic_and_version);

  bool ok = true;
  if (freedb_root_pgno != P_INVALID &&
      unlikely(freedb_root_pgno >= last_pgno)) {
    if (report)
      WARNING(
          "catch invalid %sdb root %" PRIaPGNO " for meta_txnid %" PRIaTXN
          " %s",
          "free", freedb_root_pgno, txnid,
          (env->me_stuck_meta < 0)
              ? "(workaround for incoherent flaw of unified page/buffer cache)"
              : "(wagering meta)");
    ok = false;
  }
  if (maindb_root_pgno != P_INVALID &&
      unlikely(maindb_root_pgno >= last_pgno)) {
    if (report)
      WARNING(
          "catch invalid %sdb root %" PRIaPGNO " for meta_txnid %" PRIaTXN
          " %s",
          "main", maindb_root_pgno, txnid,
          (env->me_stuck_meta < 0)
              ? "(workaround for incoherent flaw of unified page/buffer cache)"
              : "(wagering meta)");
    ok = false;
  }
  if (unlikely(txnid < freedb_mod_txnid ||
               (!freedb_mod_txnid && freedb_root &&
                likely(magic_and_version == MDBX_DATA_MAGIC)))) {
    if (report)
      WARNING(
          "catch invalid %sdb.mod_txnid %" PRIaTXN " for meta_txnid %" PRIaTXN
          " %s",
          "free", freedb_mod_txnid, txnid,
          (env->me_stuck_meta < 0)
              ? "(workaround for incoherent flaw of unified page/buffer cache)"
              : "(wagering meta)");
    ok = false;
  }
  if (unlikely(txnid < maindb_mod_txnid ||
               (!maindb_mod_txnid && maindb_root &&
                likely(magic_and_version == MDBX_DATA_MAGIC)))) {
    if (report)
      WARNING(
          "catch invalid %sdb.mod_txnid %" PRIaTXN " for meta_txnid %" PRIaTXN
          " %s",
          "main", maindb_mod_txnid, txnid,
          (env->me_stuck_meta < 0)
              ? "(workaround for incoherent flaw of unified page/buffer cache)"
              : "(wagering meta)");
    ok = false;
  }
  if (likely(freedb_root && freedb_mod_txnid &&
             (size_t)ptr_dist(env->me_dxb_mmap.base, freedb_root) <
                 env->me_dxb_mmap.limit)) {
    VALGRIND_MAKE_MEM_DEFINED(freedb_root, sizeof(freedb_root->mp_txnid));
    MDBX_ASAN_UNPOISON_MEMORY_REGION(freedb_root,
                                     sizeof(freedb_root->mp_txnid));
    const txnid_t root_txnid = freedb_root->mp_txnid;
    if (unlikely(root_txnid != freedb_mod_txnid)) {
      if (report)
        WARNING("catch invalid root_page %" PRIaPGNO " mod_txnid %" PRIaTXN
                " for %sdb.mod_txnid %" PRIaTXN " %s",
                freedb_root_pgno, root_txnid, "free", freedb_mod_txnid,
                (env->me_stuck_meta < 0) ? "(workaround for incoherent flaw of "
                                           "unified page/buffer cache)"
                                         : "(wagering meta)");
      ok = false;
    }
  }
  if (likely(maindb_root && maindb_mod_txnid &&
             (size_t)ptr_dist(env->me_dxb_mmap.base, maindb_root) <
                 env->me_dxb_mmap.limit)) {
    VALGRIND_MAKE_MEM_DEFINED(maindb_root, sizeof(maindb_root->mp_txnid));
    MDBX_ASAN_UNPOISON_MEMORY_REGION(maindb_root,
                                     sizeof(maindb_root->mp_txnid));
    const txnid_t root_txnid = maindb_root->mp_txnid;
    if (unlikely(root_txnid != maindb_mod_txnid)) {
      if (report)
        WARNING("catch invalid root_page %" PRIaPGNO " mod_txnid %" PRIaTXN
                " for %sdb.mod_txnid %" PRIaTXN " %s",
                maindb_root_pgno, root_txnid, "main", maindb_mod_txnid,
                (env->me_stuck_meta < 0) ? "(workaround for incoherent flaw of "
                                           "unified page/buffer cache)"
                                         : "(wagering meta)");
      ok = false;
    }
  }
  if (unlikely(!ok) && report)
    env->me_lck->mti_pgop_stat.incoherence.weak =
        (env->me_lck->mti_pgop_stat.incoherence.weak >= INT32_MAX)
            ? INT32_MAX
            : env->me_lck->mti_pgop_stat.incoherence.weak + 1;
  return ok;
}

__cold static int coherency_timeout(uint64_t *timestamp, intptr_t pgno,
                                    const MDBX_env *env) {
  if (likely(timestamp && *timestamp == 0))
    *timestamp = osal_monotime();
  else if (unlikely(!timestamp || osal_monotime() - *timestamp >
                                      osal_16dot16_to_monotime(65536 / 10))) {
    if (pgno >= 0 && pgno != env->me_stuck_meta)
      ERROR("bailout waiting for %" PRIuSIZE " page arrival %s", pgno,
            "(workaround for incoherent flaw of unified page/buffer cache)");
    else if (env->me_stuck_meta < 0)
      ERROR("bailout waiting for valid snapshot (%s)",
            "workaround for incoherent flaw of unified page/buffer cache");
    return MDBX_PROBLEM;
  }

  osal_memory_fence(mo_AcquireRelease, true);
#if defined(_WIN32) || defined(_WIN64)
  SwitchToThread();
#elif defined(__linux__) || defined(__gnu_linux__) || defined(_UNIX03_SOURCE)
  sched_yield();
#elif (defined(_GNU_SOURCE) && __GLIBC_PREREQ(2, 1)) || defined(_OPEN_THREADS)
  pthread_yield();
#else
  usleep(42);
#endif
  return MDBX_RESULT_TRUE;
}

/* check with timeout as the workaround
 * for https://libmdbx.dqdkfa.ru/dead-github/issues/269 */
__hot static int coherency_check_head(MDBX_txn *txn, const meta_ptr_t head,
                                      uint64_t *timestamp) {
  /* Copy the DB info and flags */
  txn->mt_geo = head.ptr_v->mm_geo;
  memcpy(txn->mt_dbs, head.ptr_c->mm_dbs, CORE_DBS * sizeof(MDBX_db));
  txn->mt_canary = head.ptr_v->mm_canary;

  if (unlikely(!coherency_check(txn->mt_env, head.txnid, txn->mt_dbs,
                                head.ptr_v, *timestamp == 0)))
    return coherency_timeout(timestamp, -1, txn->mt_env);
  return MDBX_SUCCESS;
}

static int coherency_check_written(const MDBX_env *env, const txnid_t txnid,
                                   const volatile MDBX_meta *meta,
                                   const intptr_t pgno, uint64_t *timestamp) {
  const bool report = !(timestamp && *timestamp);
  const txnid_t head_txnid = meta_txnid(meta);
  if (unlikely(head_txnid < MIN_TXNID || head_txnid < txnid)) {
    if (report) {
      env->me_lck->mti_pgop_stat.incoherence.weak =
          (env->me_lck->mti_pgop_stat.incoherence.weak >= INT32_MAX)
              ? INT32_MAX
              : env->me_lck->mti_pgop_stat.incoherence.weak + 1;
      WARNING("catch %s txnid %" PRIaTXN " for meta_%" PRIaPGNO " %s",
              (head_txnid < MIN_TXNID) ? "invalid" : "unexpected", head_txnid,
              bytes2pgno(env, ptr_dist(meta, env->me_map)),
              "(workaround for incoherent flaw of unified page/buffer cache)");
    }
    return coherency_timeout(timestamp, pgno, env);
  }
  if (unlikely(!coherency_check(env, head_txnid, meta->mm_dbs, meta, report)))
    return coherency_timeout(timestamp, pgno, env);
  return MDBX_SUCCESS;
}

static bool check_meta_coherency(const MDBX_env *env,
                                 const volatile MDBX_meta *meta, bool report) {
  uint64_t timestamp = 0;
  return coherency_check_written(env, 0, meta, -1,
                                 report ? &timestamp : nullptr) == MDBX_SUCCESS;
}

/* Common code for mdbx_txn_begin() and mdbx_txn_renew(). */
static int txn_renew(MDBX_txn *txn, const unsigned flags) {
  MDBX_env *env = txn->mt_env;
  int rc;

#if MDBX_ENV_CHECKPID
  if (unlikely(env->me_pid != osal_getpid())) {
    env->me_flags |= MDBX_FATAL_ERROR;
    return MDBX_PANIC;
  }
#endif /* MDBX_ENV_CHECKPID */

  STATIC_ASSERT(sizeof(MDBX_reader) == 32);
#if MDBX_LOCKING > 0
  STATIC_ASSERT(offsetof(MDBX_lockinfo, mti_wlock) % MDBX_CACHELINE_SIZE == 0);
  STATIC_ASSERT(offsetof(MDBX_lockinfo, mti_rlock) % MDBX_CACHELINE_SIZE == 0);
#else
  STATIC_ASSERT(
      offsetof(MDBX_lockinfo, mti_oldest_reader) % MDBX_CACHELINE_SIZE == 0);
  STATIC_ASSERT(offsetof(MDBX_lockinfo, mti_numreaders) % MDBX_CACHELINE_SIZE ==
                0);
#endif /* MDBX_LOCKING */
  STATIC_ASSERT(offsetof(MDBX_lockinfo, mti_readers) % MDBX_CACHELINE_SIZE ==
                0);

  const uintptr_t tid = osal_thread_self();
  if (flags & MDBX_TXN_RDONLY) {
    eASSERT(env, (flags & ~(MDBX_TXN_RO_BEGIN_FLAGS | MDBX_WRITEMAP)) == 0);
    txn->mt_flags =
        MDBX_TXN_RDONLY | (env->me_flags & (MDBX_NOTLS | MDBX_WRITEMAP));
    MDBX_reader *r = txn->to.reader;
    STATIC_ASSERT(sizeof(uintptr_t) <= sizeof(r->mr_tid));
    if (likely(env->me_flags & MDBX_ENV_TXKEY)) {
      eASSERT(env, !(env->me_flags & MDBX_NOTLS));
      r = thread_rthc_get(env->me_txkey);
      if (likely(r)) {
        if (unlikely(!r->mr_pid.weak) &&
            (runtime_flags & MDBX_DBG_LEGACY_MULTIOPEN)) {
          thread_rthc_set(env->me_txkey, nullptr);
          r = nullptr;
        } else {
          eASSERT(env, r->mr_pid.weak == env->me_pid);
          eASSERT(env, r->mr_tid.weak == osal_thread_self());
        }
      }
    } else {
      eASSERT(env, !env->me_lck_mmap.lck || (env->me_flags & MDBX_NOTLS));
    }

    if (likely(r)) {
      if (unlikely(r->mr_pid.weak != env->me_pid ||
                   r->mr_txnid.weak < SAFE64_INVALID_THRESHOLD))
        return MDBX_BAD_RSLOT;
    } else if (env->me_lck_mmap.lck) {
      bind_rslot_result brs = bind_rslot(env, tid);
      if (unlikely(brs.err != MDBX_SUCCESS))
        return brs.err;
      r = brs.rslot;
    }
    txn->to.reader = r;
    if (flags & (MDBX_TXN_RDONLY_PREPARE - MDBX_TXN_RDONLY)) {
      eASSERT(env, txn->mt_txnid == 0);
      eASSERT(env, txn->mt_owner == 0);
      eASSERT(env, txn->mt_numdbs == 0);
      if (likely(r)) {
        eASSERT(env, r->mr_snapshot_pages_used.weak == 0);
        eASSERT(env, r->mr_txnid.weak >= SAFE64_INVALID_THRESHOLD);
        atomic_store32(&r->mr_snapshot_pages_used, 0, mo_Relaxed);
      }
      txn->mt_flags = MDBX_TXN_RDONLY | MDBX_TXN_FINISHED;
      return MDBX_SUCCESS;
    }

    /* Seek & fetch the last meta */
    uint64_t timestamp = 0;
    size_t loop = 0;
    meta_troika_t troika = meta_tap(env);
    while (1) {
      const meta_ptr_t head =
          likely(env->me_stuck_meta < 0)
              ? /* regular */ meta_recent(env, &troika)
              : /* recovery mode */ meta_ptr(env, env->me_stuck_meta);
      if (likely(r)) {
        safe64_reset(&r->mr_txnid, false);
        atomic_store32(&r->mr_snapshot_pages_used, head.ptr_v->mm_geo.next,
                       mo_Relaxed);
        atomic_store64(
            &r->mr_snapshot_pages_retired,
            unaligned_peek_u64_volatile(4, head.ptr_v->mm_pages_retired),
            mo_Relaxed);
        safe64_write(&r->mr_txnid, head.txnid);
        eASSERT(env, r->mr_pid.weak == osal_getpid());
        eASSERT(env,
                r->mr_tid.weak ==
                    ((env->me_flags & MDBX_NOTLS) ? 0 : osal_thread_self()));
        eASSERT(env, r->mr_txnid.weak == head.txnid ||
                         (r->mr_txnid.weak >= SAFE64_INVALID_THRESHOLD &&
                          head.txnid < env->me_lck->mti_oldest_reader.weak));
        atomic_store32(&env->me_lck->mti_readers_refresh_flag, true,
                       mo_AcquireRelease);
      } else {
        /* exclusive mode without lck */
        eASSERT(env, !env->me_lck_mmap.lck && env->me_lck == lckless_stub(env));
      }
      jitter4testing(true);

      /* Snap the state from current meta-head */
      txn->mt_txnid = head.txnid;
      if (likely(env->me_stuck_meta < 0) &&
          unlikely(meta_should_retry(env, &troika) ||
                   head.txnid < atomic_load64(&env->me_lck->mti_oldest_reader,
                                              mo_AcquireRelease))) {
        if (unlikely(++loop > 42)) {
          ERROR("bailout waiting for valid snapshot (%s)",
                "metapages are too volatile");
          rc = MDBX_PROBLEM;
          txn->mt_txnid = INVALID_TXNID;
          if (likely(r))
            safe64_reset(&r->mr_txnid, false);
          goto bailout;
        }
        timestamp = 0;
        continue;
      }

      rc = coherency_check_head(txn, head, &timestamp);
      jitter4testing(false);
      if (likely(rc == MDBX_SUCCESS))
        break;

      if (unlikely(rc != MDBX_RESULT_TRUE)) {
        txn->mt_txnid = INVALID_TXNID;
        if (likely(r))
          safe64_reset(&r->mr_txnid, false);
        goto bailout;
      }
    }

    if (unlikely(txn->mt_txnid < MIN_TXNID || txn->mt_txnid > MAX_TXNID)) {
      ERROR("%s", "environment corrupted by died writer, must shutdown!");
      if (likely(r))
        safe64_reset(&r->mr_txnid, false);
      txn->mt_txnid = INVALID_TXNID;
      rc = MDBX_CORRUPTED;
      goto bailout;
    }
    eASSERT(env, txn->mt_txnid >= env->me_lck->mti_oldest_reader.weak);
    txn->mt_dbxs = env->me_dbxs; /* mostly static anyway */
    ENSURE(env, txn->mt_txnid >=
                    /* paranoia is appropriate here */ env->me_lck
                        ->mti_oldest_reader.weak);
    txn->mt_numdbs = env->me_numdbs;
  } else {
    eASSERT(env, (flags & ~(MDBX_TXN_RW_BEGIN_FLAGS | MDBX_TXN_SPILLS |
                            MDBX_WRITEMAP)) == 0);
    if (unlikely(txn->mt_owner == tid ||
                 /* not recovery mode */ env->me_stuck_meta >= 0))
      return MDBX_BUSY;
    MDBX_lockinfo *const lck = env->me_lck_mmap.lck;
    if (lck && (env->me_flags & MDBX_NOTLS) == 0 &&
        (runtime_flags & MDBX_DBG_LEGACY_OVERLAP) == 0) {
      const size_t snap_nreaders =
          atomic_load32(&lck->mti_numreaders, mo_AcquireRelease);
      for (size_t i = 0; i < snap_nreaders; ++i) {
        if (atomic_load32(&lck->mti_readers[i].mr_pid, mo_Relaxed) ==
                env->me_pid &&
            unlikely(atomic_load64(&lck->mti_readers[i].mr_tid, mo_Relaxed) ==
                     tid)) {
          const txnid_t txnid = safe64_read(&lck->mti_readers[i].mr_txnid);
          if (txnid >= MIN_TXNID && txnid <= MAX_TXNID)
            return MDBX_TXN_OVERLAPPING;
        }
      }
    }

    /* Not yet touching txn == env->me_txn0, it may be active */
    jitter4testing(false);
    rc = mdbx_txn_lock(env, !!(flags & MDBX_TXN_TRY));
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

    txn->tw.troika = meta_tap(env);
    const meta_ptr_t head = meta_recent(env, &txn->tw.troika);
    uint64_t timestamp = 0;
    while ("workaround for https://libmdbx.dqdkfa.ru/dead-github/issues/269") {
      rc = coherency_check_head(txn, head, &timestamp);
      if (likely(rc == MDBX_SUCCESS))
        break;
      if (unlikely(rc != MDBX_RESULT_TRUE))
        goto bailout;
    }
    eASSERT(env, meta_txnid(head.ptr_v) == head.txnid);
    txn->mt_txnid = safe64_txnid_next(head.txnid);
    if (unlikely(txn->mt_txnid > MAX_TXNID)) {
      rc = MDBX_TXN_FULL;
      ERROR("txnid overflow, raise %d", rc);
      goto bailout;
    }

    txn->mt_flags = flags;
    txn->mt_child = NULL;
    txn->tw.loose_pages = NULL;
    txn->tw.loose_count = 0;
#if MDBX_ENABLE_REFUND
    txn->tw.loose_refund_wl = 0;
#endif /* MDBX_ENABLE_REFUND */
    MDBX_PNL_SETSIZE(txn->tw.retired_pages, 0);
    txn->tw.spilled.list = NULL;
    txn->tw.spilled.least_removed = 0;
    txn->tw.last_reclaimed = 0;
    if (txn->tw.lifo_reclaimed)
      MDBX_PNL_SETSIZE(txn->tw.lifo_reclaimed, 0);
    env->me_txn = txn;
    txn->mt_numdbs = env->me_numdbs;
    memcpy(txn->mt_dbiseqs, env->me_dbiseqs, txn->mt_numdbs * sizeof(unsigned));

    if ((txn->mt_flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC) {
      rc = dpl_alloc(txn);
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;
      txn->tw.dirtyroom = txn->mt_env->me_options.dp_limit;
      txn->tw.dirtylru = MDBX_DEBUG ? UINT32_MAX / 3 - 42 : 0;
    } else {
      tASSERT(txn, txn->tw.dirtylist == nullptr);
      txn->tw.dirtylist = nullptr;
      txn->tw.dirtyroom = MAX_PAGENO;
      txn->tw.dirtylru = 0;
    }
    eASSERT(env, txn->tw.writemap_dirty_npages == 0);
    eASSERT(env, txn->tw.writemap_spilled_npages == 0);
  }

  /* Setup db info */
  osal_compiler_barrier();
  memset(txn->mt_cursors, 0, sizeof(MDBX_cursor *) * txn->mt_numdbs);
  for (size_t i = CORE_DBS; i < txn->mt_numdbs; i++) {
    const unsigned db_flags = env->me_dbflags[i];
    txn->mt_dbs[i].md_flags = db_flags & DB_PERSISTENT_FLAGS;
    txn->mt_dbistate[i] =
        (db_flags & DB_VALID) ? DBI_VALID | DBI_USRVALID | DBI_STALE : 0;
  }
  txn->mt_dbistate[MAIN_DBI] = DBI_VALID | DBI_USRVALID;
  rc =
      setup_dbx(&txn->mt_dbxs[MAIN_DBI], &txn->mt_dbs[MAIN_DBI], env->me_psize);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout;
  txn->mt_dbistate[FREE_DBI] = DBI_VALID;
  txn->mt_front =
      txn->mt_txnid + ((flags & (MDBX_WRITEMAP | MDBX_RDONLY)) == 0);

  if (unlikely(env->me_flags & MDBX_FATAL_ERROR)) {
    WARNING("%s", "environment had fatal error, must shutdown!");
    rc = MDBX_PANIC;
  } else {
    const size_t size_bytes = pgno2bytes(env, txn->mt_end_pgno);
    const size_t used_bytes = pgno2bytes(env, txn->mt_next_pgno);
    const size_t required_bytes =
        (txn->mt_flags & MDBX_TXN_RDONLY) ? used_bytes : size_bytes;
    eASSERT(env, env->me_dxb_mmap.limit >= env->me_dxb_mmap.current);
    if (unlikely(required_bytes > env->me_dxb_mmap.current)) {
      /* Размер БД (для пишущих транзакций) или используемых данных (для
       * читающих транзакций) больше предыдущего/текущего размера внутри
       * процесса, увеличиваем. Сюда также попадает случай увеличения верхней
       * границы размера БД и отображения. В читающих транзакциях нельзя
       * изменять размер файла, который может быть больше необходимого этой
       * транзакции. */
      if (txn->mt_geo.upper > MAX_PAGENO + 1 ||
          bytes2pgno(env, pgno2bytes(env, txn->mt_geo.upper)) !=
              txn->mt_geo.upper) {
        rc = MDBX_UNABLE_EXTEND_MAPSIZE;
        goto bailout;
      }
      rc = dxb_resize(env, txn->mt_next_pgno, txn->mt_end_pgno,
                      txn->mt_geo.upper, implicit_grow);
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;
      eASSERT(env, env->me_dxb_mmap.limit >= env->me_dxb_mmap.current);
    } else if (unlikely(size_bytes < env->me_dxb_mmap.current)) {
      /* Размер БД меньше предыдущего/текущего размера внутри процесса, можно
       * уменьшить, но всё сложнее:
       *  - размер файла согласован со всеми читаемыми снимками на момент
       *    коммита последней транзакции;
       *  - в читающей транзакции размер файла может быть больше и него нельзя
       *    изменять, в том числе менять madvise (меньша размера файла нельзя,
       *    а за размером нет смысла).
       *  - в пишущей транзакции уменьшать размер файла можно только после
       *    проверки размера читаемых снимков, но в этом нет смысла, так как
       *    это будет сделано при фиксации транзакции.
       *
       *  В сухом остатке, можно только установить dxb_mmap.current равным
       *  размеру файла, а это проще сделать без вызова dxb_resize() и усложения
       *  внутренней логики.
       *
       *  В этой тактике есть недостаток: если пишущите транзакции не регулярны,
       *  и при завершении такой транзакции файл БД остаётся не-уменьшеным из-за
       *  читающих транзакций использующих предыдущие снимки. */
#if defined(_WIN32) || defined(_WIN64)
      osal_srwlock_AcquireShared(&env->me_remap_guard);
#else
      rc = osal_fastmutex_acquire(&env->me_remap_guard);
#endif
      if (likely(rc == MDBX_SUCCESS)) {
        eASSERT(env, env->me_dxb_mmap.limit >= env->me_dxb_mmap.current);
        rc = osal_filesize(env->me_dxb_mmap.fd, &env->me_dxb_mmap.filesize);
        if (likely(rc == MDBX_SUCCESS)) {
          eASSERT(env, env->me_dxb_mmap.filesize >= required_bytes);
          if (env->me_dxb_mmap.current > env->me_dxb_mmap.filesize)
            env->me_dxb_mmap.current =
                (env->me_dxb_mmap.limit < env->me_dxb_mmap.filesize)
                    ? env->me_dxb_mmap.limit
                    : (size_t)env->me_dxb_mmap.filesize;
        }
#if defined(_WIN32) || defined(_WIN64)
        osal_srwlock_ReleaseShared(&env->me_remap_guard);
#else
        int err = osal_fastmutex_release(&env->me_remap_guard);
        if (unlikely(err) && likely(rc == MDBX_SUCCESS))
          rc = err;
#endif
      }
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;
    }
    eASSERT(env,
            pgno2bytes(env, txn->mt_next_pgno) <= env->me_dxb_mmap.current);
    eASSERT(env, env->me_dxb_mmap.limit >= env->me_dxb_mmap.current);
    if (txn->mt_flags & MDBX_TXN_RDONLY) {
#if defined(_WIN32) || defined(_WIN64)
      if (((used_bytes > env->me_dbgeo.lower && env->me_dbgeo.shrink) ||
           (mdbx_RunningUnderWine() &&
            /* under Wine acquisition of remap_guard is always required,
             * since Wine don't support section extending,
             * i.e. in both cases unmap+map are required. */
            used_bytes < env->me_dbgeo.upper && env->me_dbgeo.grow)) &&
          /* avoid recursive use SRW */ (txn->mt_flags & MDBX_NOTLS) == 0) {
        txn->mt_flags |= MDBX_SHRINK_ALLOWED;
        osal_srwlock_AcquireShared(&env->me_remap_guard);
      }
#endif /* Windows */
    } else {
      if (unlikely((txn->mt_dbs[FREE_DBI].md_flags & DB_PERSISTENT_FLAGS) !=
                   MDBX_INTEGERKEY)) {
        ERROR("unexpected/invalid db-flags 0x%x for %s",
              txn->mt_dbs[FREE_DBI].md_flags, "GC/FreeDB");
        rc = MDBX_INCOMPATIBLE;
        goto bailout;
      }

      tASSERT(txn, txn == env->me_txn0);
      MDBX_cursor *const gc = ptr_disp(txn, sizeof(MDBX_txn));
      rc = cursor_init(gc, txn, FREE_DBI);
      if (rc != MDBX_SUCCESS)
        goto bailout;
    }
#if defined(MDBX_USE_VALGRIND) || defined(__SANITIZE_ADDRESS__)
    txn_valgrind(env, txn);
#endif /* MDBX_USE_VALGRIND || __SANITIZE_ADDRESS__ */
    txn->mt_owner = tid;
    return MDBX_SUCCESS;
  }
bailout:
  tASSERT(txn, rc != MDBX_SUCCESS);
  txn_end(txn, MDBX_END_SLOT | MDBX_END_EOTDONE | MDBX_END_FAIL_BEGIN);
  return rc;
}

static __always_inline int check_txn(const MDBX_txn *txn, int bad_bits) {
  if (unlikely(!txn))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_signature != MDBX_MT_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(txn->mt_flags & bad_bits))
    return MDBX_BAD_TXN;

  tASSERT(txn, (txn->mt_flags & MDBX_TXN_FINISHED) ||
                   (txn->mt_flags & MDBX_NOTLS) ==
                       ((txn->mt_flags & MDBX_TXN_RDONLY)
                            ? txn->mt_env->me_flags & MDBX_NOTLS
                            : 0));
#if MDBX_TXN_CHECKOWNER
  STATIC_ASSERT(MDBX_NOTLS > MDBX_TXN_FINISHED + MDBX_TXN_RDONLY);
  if (unlikely(txn->mt_owner != osal_thread_self()) &&
      (txn->mt_flags & (MDBX_NOTLS | MDBX_TXN_FINISHED | MDBX_TXN_RDONLY)) <
          (MDBX_TXN_FINISHED | MDBX_TXN_RDONLY))
    return txn->mt_owner ? MDBX_THREAD_MISMATCH : MDBX_BAD_TXN;
#endif /* MDBX_TXN_CHECKOWNER */

  if (bad_bits && unlikely(!txn->mt_env->me_map))
    return MDBX_EPERM;

  return MDBX_SUCCESS;
}

static __always_inline int check_txn_rw(const MDBX_txn *txn, int bad_bits) {
  int err = check_txn(txn, bad_bits);
  if (unlikely(err))
    return err;

  if (unlikely(txn->mt_flags & MDBX_TXN_RDONLY))
    return MDBX_EACCESS;

  return MDBX_SUCCESS;
}

int mdbx_txn_renew(MDBX_txn *txn) {
  if (unlikely(!txn))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_signature != MDBX_MT_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely((txn->mt_flags & MDBX_TXN_RDONLY) == 0))
    return MDBX_EINVAL;

  int rc;
  if (unlikely(txn->mt_owner != 0 || !(txn->mt_flags & MDBX_TXN_FINISHED))) {
    rc = mdbx_txn_reset(txn);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }

  rc = txn_renew(txn, MDBX_TXN_RDONLY);
  if (rc == MDBX_SUCCESS) {
    tASSERT(txn, txn->mt_owner == osal_thread_self());
    DEBUG("renew txn %" PRIaTXN "%c %p on env %p, root page %" PRIaPGNO
          "/%" PRIaPGNO,
          txn->mt_txnid, (txn->mt_flags & MDBX_TXN_RDONLY) ? 'r' : 'w',
          (void *)txn, (void *)txn->mt_env, txn->mt_dbs[MAIN_DBI].md_root,
          txn->mt_dbs[FREE_DBI].md_root);
  }
  return rc;
}

int mdbx_txn_set_userctx(MDBX_txn *txn, void *ctx) {
  int rc = check_txn(txn, MDBX_TXN_FINISHED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  txn->mt_userctx = ctx;
  return MDBX_SUCCESS;
}

void *mdbx_txn_get_userctx(const MDBX_txn *txn) {
  return check_txn(txn, MDBX_TXN_FINISHED) ? nullptr : txn->mt_userctx;
}

int mdbx_txn_begin_ex(MDBX_env *env, MDBX_txn *parent, MDBX_txn_flags_t flags,
                      MDBX_txn **ret, void *context) {
  if (unlikely(!ret))
    return MDBX_EINVAL;
  *ret = NULL;

  if (unlikely((flags & ~MDBX_TXN_RW_BEGIN_FLAGS) &&
               (parent || (flags & ~MDBX_TXN_RO_BEGIN_FLAGS))))
    return MDBX_EINVAL;

  int rc = check_env(env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(env->me_flags & MDBX_RDONLY &
               ~flags)) /* write txn in RDONLY env */
    return MDBX_EACCESS;

  flags |= env->me_flags & MDBX_WRITEMAP;

  MDBX_txn *txn = nullptr;
  if (parent) {
    /* Nested transactions: Max 1 child, write txns only, no writemap */
    rc = check_txn_rw(parent,
                      MDBX_TXN_RDONLY | MDBX_WRITEMAP | MDBX_TXN_BLOCKED);
    if (unlikely(rc != MDBX_SUCCESS)) {
      if (rc == MDBX_BAD_TXN &&
          (parent->mt_flags & (MDBX_TXN_RDONLY | MDBX_TXN_BLOCKED)) == 0) {
        ERROR("%s mode is incompatible with nested transactions",
              "MDBX_WRITEMAP");
        rc = MDBX_INCOMPATIBLE;
      }
      return rc;
    }

    if (env->me_options.spill_parent4child_denominator) {
      /* Spill dirty-pages of parent to provide dirtyroom for child txn */
      rc = txn_spill(parent, nullptr,
                     parent->tw.dirtylist->length /
                         env->me_options.spill_parent4child_denominator);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
    }
    tASSERT(parent, audit_ex(parent, 0, false) == 0);

    flags |= parent->mt_flags & (MDBX_TXN_RW_BEGIN_FLAGS | MDBX_TXN_SPILLS);
  } else if (flags & MDBX_TXN_RDONLY) {
    if (env->me_txn0 &&
        unlikely(env->me_txn0->mt_owner == osal_thread_self()) &&
        (runtime_flags & MDBX_DBG_LEGACY_OVERLAP) == 0)
      return MDBX_TXN_OVERLAPPING;
  } else {
    /* Reuse preallocated write txn. However, do not touch it until
     * txn_renew() succeeds, since it currently may be active. */
    txn = env->me_txn0;
    goto renew;
  }

  const size_t base = (flags & MDBX_TXN_RDONLY)
                          ? sizeof(MDBX_txn) - sizeof(txn->tw) + sizeof(txn->to)
                          : sizeof(MDBX_txn);
  const size_t size =
      base + env->me_maxdbs * (sizeof(MDBX_db) + sizeof(MDBX_cursor *) + 1);
  txn = osal_malloc(size);
  if (unlikely(txn == nullptr)) {
    DEBUG("calloc: %s", "failed");
    return MDBX_ENOMEM;
  }
#if MDBX_DEBUG
  memset(txn, 0xCD, size);
  VALGRIND_MAKE_MEM_UNDEFINED(txn, size);
#endif /* MDBX_DEBUG */
  MDBX_ANALYSIS_ASSUME(size > base);
  memset(txn, 0,
         (MDBX_GOOFY_MSVC_STATIC_ANALYZER && base > size) ? size : base);
  txn->mt_dbs = ptr_disp(txn, base);
  txn->mt_cursors = ptr_disp(txn->mt_dbs, sizeof(MDBX_db) * env->me_maxdbs);
#if MDBX_DEBUG
  txn->mt_cursors[FREE_DBI] = nullptr; /* avoid SIGSEGV in an assertion later */
#endif                                 /* MDBX_DEBUG */
  txn->mt_dbistate = ptr_disp(txn, size - env->me_maxdbs);
  txn->mt_dbxs = env->me_dbxs; /* static */
  txn->mt_flags = flags;
  txn->mt_env = env;

  if (parent) {
    tASSERT(parent, dirtylist_check(parent));
    txn->mt_dbiseqs = parent->mt_dbiseqs;
    txn->mt_geo = parent->mt_geo;
    rc = dpl_alloc(txn);
    if (likely(rc == MDBX_SUCCESS)) {
      const size_t len =
          MDBX_PNL_GETSIZE(parent->tw.relist) + parent->tw.loose_count;
      txn->tw.relist =
          pnl_alloc((len > MDBX_PNL_INITIAL) ? len : MDBX_PNL_INITIAL);
      if (unlikely(!txn->tw.relist))
        rc = MDBX_ENOMEM;
    }
    if (unlikely(rc != MDBX_SUCCESS)) {
    nested_failed:
      pnl_free(txn->tw.relist);
      dpl_free(txn);
      osal_free(txn);
      return rc;
    }

    /* Move loose pages to reclaimed list */
    if (parent->tw.loose_count) {
      do {
        MDBX_page *lp = parent->tw.loose_pages;
        tASSERT(parent, lp->mp_flags == P_LOOSE);
        rc = pnl_insert_range(&parent->tw.relist, lp->mp_pgno, 1);
        if (unlikely(rc != MDBX_SUCCESS))
          goto nested_failed;
        MDBX_ASAN_UNPOISON_MEMORY_REGION(&mp_next(lp), sizeof(MDBX_page *));
        VALGRIND_MAKE_MEM_DEFINED(&mp_next(lp), sizeof(MDBX_page *));
        parent->tw.loose_pages = mp_next(lp);
        /* Remove from dirty list */
        page_wash(parent, dpl_exist(parent, lp->mp_pgno), lp, 1);
      } while (parent->tw.loose_pages);
      parent->tw.loose_count = 0;
#if MDBX_ENABLE_REFUND
      parent->tw.loose_refund_wl = 0;
#endif /* MDBX_ENABLE_REFUND */
      tASSERT(parent, dirtylist_check(parent));
    }
    txn->tw.dirtyroom = parent->tw.dirtyroom;
    txn->tw.dirtylru = parent->tw.dirtylru;

    dpl_sort(parent);
    if (parent->tw.spilled.list)
      spill_purge(parent);

    tASSERT(txn, MDBX_PNL_ALLOCLEN(txn->tw.relist) >=
                     MDBX_PNL_GETSIZE(parent->tw.relist));
    memcpy(txn->tw.relist, parent->tw.relist,
           MDBX_PNL_SIZEOF(parent->tw.relist));
    eASSERT(env, pnl_check_allocated(
                     txn->tw.relist,
                     (txn->mt_next_pgno /* LY: intentional assignment here,
                                               only for assertion */
                      = parent->mt_next_pgno) -
                         MDBX_ENABLE_REFUND));

    txn->tw.last_reclaimed = parent->tw.last_reclaimed;
    if (parent->tw.lifo_reclaimed) {
      txn->tw.lifo_reclaimed = parent->tw.lifo_reclaimed;
      parent->tw.lifo_reclaimed =
          (void *)(intptr_t)MDBX_PNL_GETSIZE(parent->tw.lifo_reclaimed);
    }

    txn->tw.retired_pages = parent->tw.retired_pages;
    parent->tw.retired_pages =
        (void *)(intptr_t)MDBX_PNL_GETSIZE(parent->tw.retired_pages);

    txn->mt_txnid = parent->mt_txnid;
    txn->mt_front = parent->mt_front + 1;
#if MDBX_ENABLE_REFUND
    txn->tw.loose_refund_wl = 0;
#endif /* MDBX_ENABLE_REFUND */
    txn->mt_canary = parent->mt_canary;
    parent->mt_flags |= MDBX_TXN_HAS_CHILD;
    parent->mt_child = txn;
    txn->mt_parent = parent;
    txn->mt_numdbs = parent->mt_numdbs;
    txn->mt_owner = parent->mt_owner;
    memcpy(txn->mt_dbs, parent->mt_dbs, txn->mt_numdbs * sizeof(MDBX_db));
    txn->tw.troika = parent->tw.troika;
    /* Copy parent's mt_dbistate, but clear DB_NEW */
    for (size_t i = 0; i < txn->mt_numdbs; i++)
      txn->mt_dbistate[i] =
          parent->mt_dbistate[i] & ~(DBI_FRESH | DBI_CREAT | DBI_DIRTY);
    tASSERT(parent,
            parent->tw.dirtyroom + parent->tw.dirtylist->length ==
                (parent->mt_parent ? parent->mt_parent->tw.dirtyroom
                                   : parent->mt_env->me_options.dp_limit));
    tASSERT(txn, txn->tw.dirtyroom + txn->tw.dirtylist->length ==
                     (txn->mt_parent ? txn->mt_parent->tw.dirtyroom
                                     : txn->mt_env->me_options.dp_limit));
    env->me_txn = txn;
    rc = cursor_shadow(parent, txn);
    if (AUDIT_ENABLED() && ASSERT_ENABLED()) {
      txn->mt_signature = MDBX_MT_SIGNATURE;
      tASSERT(txn, audit_ex(txn, 0, false) == 0);
    }
    if (unlikely(rc != MDBX_SUCCESS))
      txn_end(txn, MDBX_END_FAIL_BEGINCHILD);
  } else { /* MDBX_TXN_RDONLY */
    txn->mt_dbiseqs = env->me_dbiseqs;
  renew:
    rc = txn_renew(txn, flags);
  }

  if (unlikely(rc != MDBX_SUCCESS)) {
    if (txn != env->me_txn0)
      osal_free(txn);
  } else {
    if (flags & (MDBX_TXN_RDONLY_PREPARE - MDBX_TXN_RDONLY))
      eASSERT(env, txn->mt_flags == (MDBX_TXN_RDONLY | MDBX_TXN_FINISHED));
    else if (flags & MDBX_TXN_RDONLY)
      eASSERT(env, (txn->mt_flags &
                    ~(MDBX_NOTLS | MDBX_TXN_RDONLY | MDBX_WRITEMAP |
                      /* Win32: SRWL flag */ MDBX_SHRINK_ALLOWED)) == 0);
    else {
      eASSERT(env, (txn->mt_flags &
                    ~(MDBX_WRITEMAP | MDBX_SHRINK_ALLOWED | MDBX_NOMETASYNC |
                      MDBX_SAFE_NOSYNC | MDBX_TXN_SPILLS)) == 0);
      assert(!txn->tw.spilled.list && !txn->tw.spilled.least_removed);
    }
    txn->mt_signature = MDBX_MT_SIGNATURE;
    txn->mt_userctx = context;
    *ret = txn;
    DEBUG("begin txn %" PRIaTXN "%c %p on env %p, root page %" PRIaPGNO
          "/%" PRIaPGNO,
          txn->mt_txnid, (flags & MDBX_TXN_RDONLY) ? 'r' : 'w', (void *)txn,
          (void *)env, txn->mt_dbs[MAIN_DBI].md_root,
          txn->mt_dbs[FREE_DBI].md_root);
  }

  return rc;
}

int mdbx_txn_info(const MDBX_txn *txn, MDBX_txn_info *info, bool scan_rlt) {
  int rc = check_txn(txn, MDBX_TXN_FINISHED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!info))
    return MDBX_EINVAL;

  MDBX_env *const env = txn->mt_env;
#if MDBX_ENV_CHECKPID
  if (unlikely(env->me_pid != osal_getpid())) {
    env->me_flags |= MDBX_FATAL_ERROR;
    return MDBX_PANIC;
  }
#endif /* MDBX_ENV_CHECKPID */

  info->txn_id = txn->mt_txnid;
  info->txn_space_used = pgno2bytes(env, txn->mt_geo.next);

  if (txn->mt_flags & MDBX_TXN_RDONLY) {
    meta_ptr_t head;
    uint64_t head_retired;
    meta_troika_t troika = meta_tap(env);
    do {
      /* fetch info from volatile head */
      head = meta_recent(env, &troika);
      head_retired =
          unaligned_peek_u64_volatile(4, head.ptr_v->mm_pages_retired);
      info->txn_space_limit_soft = pgno2bytes(env, head.ptr_v->mm_geo.now);
      info->txn_space_limit_hard = pgno2bytes(env, head.ptr_v->mm_geo.upper);
      info->txn_space_leftover =
          pgno2bytes(env, head.ptr_v->mm_geo.now - head.ptr_v->mm_geo.next);
    } while (unlikely(meta_should_retry(env, &troika)));

    info->txn_reader_lag = head.txnid - info->txn_id;
    info->txn_space_dirty = info->txn_space_retired = 0;
    uint64_t reader_snapshot_pages_retired;
    if (txn->to.reader &&
        head_retired >
            (reader_snapshot_pages_retired = atomic_load64(
                 &txn->to.reader->mr_snapshot_pages_retired, mo_Relaxed))) {
      info->txn_space_dirty = info->txn_space_retired = pgno2bytes(
          env, (pgno_t)(head_retired - reader_snapshot_pages_retired));

      size_t retired_next_reader = 0;
      MDBX_lockinfo *const lck = env->me_lck_mmap.lck;
      if (scan_rlt && info->txn_reader_lag > 1 && lck) {
        /* find next more recent reader */
        txnid_t next_reader = head.txnid;
        const size_t snap_nreaders =
            atomic_load32(&lck->mti_numreaders, mo_AcquireRelease);
        for (size_t i = 0; i < snap_nreaders; ++i) {
        retry:
          if (atomic_load32(&lck->mti_readers[i].mr_pid, mo_AcquireRelease)) {
            jitter4testing(true);
            const txnid_t snap_txnid =
                safe64_read(&lck->mti_readers[i].mr_txnid);
            const uint64_t snap_retired =
                atomic_load64(&lck->mti_readers[i].mr_snapshot_pages_retired,
                              mo_AcquireRelease);
            if (unlikely(snap_retired !=
                         atomic_load64(
                             &lck->mti_readers[i].mr_snapshot_pages_retired,
                             mo_Relaxed)) ||
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
                                atomic_load64(
                                    &txn->to.reader->mr_snapshot_pages_retired,
                                    mo_Relaxed)));
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
        env, txn->mt_child ? (size_t)txn->tw.retired_pages
                           : MDBX_PNL_GETSIZE(txn->tw.retired_pages));
    info->txn_space_leftover = pgno2bytes(env, txn->tw.dirtyroom);
    info->txn_space_dirty = pgno2bytes(
        env, txn->tw.dirtylist ? txn->tw.dirtylist->pages_including_loose
                               : (txn->tw.writemap_dirty_npages +
                                  txn->tw.writemap_spilled_npages));
    info->txn_reader_lag = INT64_MAX;
    MDBX_lockinfo *const lck = env->me_lck_mmap.lck;
    if (scan_rlt && lck) {
      txnid_t oldest_snapshot = txn->mt_txnid;
      const size_t snap_nreaders =
          atomic_load32(&lck->mti_numreaders, mo_AcquireRelease);
      if (snap_nreaders) {
        oldest_snapshot = txn_oldest_reader(txn);
        if (oldest_snapshot == txn->mt_txnid - 1) {
          /* check if there is at least one reader */
          bool exists = false;
          for (size_t i = 0; i < snap_nreaders; ++i) {
            if (atomic_load32(&lck->mti_readers[i].mr_pid, mo_Relaxed) &&
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

MDBX_env *mdbx_txn_env(const MDBX_txn *txn) {
  if (unlikely(!txn || txn->mt_signature != MDBX_MT_SIGNATURE ||
               txn->mt_env->me_signature.weak != MDBX_ME_SIGNATURE))
    return NULL;
  return txn->mt_env;
}

uint64_t mdbx_txn_id(const MDBX_txn *txn) {
  if (unlikely(!txn || txn->mt_signature != MDBX_MT_SIGNATURE))
    return 0;
  return txn->mt_txnid;
}

int mdbx_txn_flags(const MDBX_txn *txn) {
  if (unlikely(!txn || txn->mt_signature != MDBX_MT_SIGNATURE)) {
    assert((-1 & (int)MDBX_TXN_INVALID) != 0);
    return -1;
  }
  assert(0 == (int)(txn->mt_flags & MDBX_TXN_INVALID));
  return txn->mt_flags;
}

/* Check for misused dbi handles */
static __inline bool dbi_changed(const MDBX_txn *txn, size_t dbi) {
  if (txn->mt_dbiseqs == txn->mt_env->me_dbiseqs)
    return false;
  if (likely(
          txn->mt_dbiseqs[dbi].weak ==
          atomic_load32((MDBX_atomic_uint32_t *)&txn->mt_env->me_dbiseqs[dbi],
                        mo_AcquireRelease)))
    return false;
  return true;
}

static __inline unsigned dbi_seq(const MDBX_env *const env, size_t slot) {
  unsigned v = env->me_dbiseqs[slot].weak + 1;
  return v + (v == 0);
}

static void dbi_import_locked(MDBX_txn *txn) {
  const MDBX_env *const env = txn->mt_env;
  size_t n = env->me_numdbs;
  for (size_t i = CORE_DBS; i < n; ++i) {
    if (i >= txn->mt_numdbs) {
      txn->mt_cursors[i] = NULL;
      if (txn->mt_dbiseqs != env->me_dbiseqs)
        txn->mt_dbiseqs[i].weak = 0;
      txn->mt_dbistate[i] = 0;
    }
    if ((dbi_changed(txn, i) &&
         (txn->mt_dbistate[i] & (DBI_CREAT | DBI_DIRTY | DBI_FRESH)) == 0) ||
        ((env->me_dbflags[i] & DB_VALID) &&
         !(txn->mt_dbistate[i] & DBI_VALID))) {
      tASSERT(txn,
              (txn->mt_dbistate[i] & (DBI_CREAT | DBI_DIRTY | DBI_FRESH)) == 0);
      txn->mt_dbiseqs[i] = env->me_dbiseqs[i];
      txn->mt_dbs[i].md_flags = env->me_dbflags[i] & DB_PERSISTENT_FLAGS;
      txn->mt_dbistate[i] = 0;
      if (env->me_dbflags[i] & DB_VALID) {
        txn->mt_dbistate[i] = DBI_VALID | DBI_USRVALID | DBI_STALE;
        tASSERT(txn, txn->mt_dbxs[i].md_cmp != NULL);
        tASSERT(txn, txn->mt_dbxs[i].md_name.iov_base != NULL);
      }
    }
  }
  while (unlikely(n < txn->mt_numdbs))
    if (txn->mt_cursors[txn->mt_numdbs - 1] == NULL &&
        (txn->mt_dbistate[txn->mt_numdbs - 1] & DBI_USRVALID) == 0)
      txn->mt_numdbs -= 1;
    else {
      if ((txn->mt_dbistate[n] & DBI_USRVALID) == 0) {
        if (txn->mt_dbiseqs != env->me_dbiseqs)
          txn->mt_dbiseqs[n].weak = 0;
        txn->mt_dbistate[n] = 0;
      }
      ++n;
    }
  txn->mt_numdbs = (MDBX_dbi)n;
}

/* Import DBI which opened after txn started into context */
__cold static bool dbi_import(MDBX_txn *txn, MDBX_dbi dbi) {
  if (dbi < CORE_DBS ||
      (dbi >= txn->mt_numdbs && dbi >= txn->mt_env->me_numdbs))
    return false;

  ENSURE(txn->mt_env,
         osal_fastmutex_acquire(&txn->mt_env->me_dbi_lock) == MDBX_SUCCESS);
  dbi_import_locked(txn);
  ENSURE(txn->mt_env,
         osal_fastmutex_release(&txn->mt_env->me_dbi_lock) == MDBX_SUCCESS);
  return txn->mt_dbistate[dbi] & DBI_USRVALID;
}

/* Export or close DBI handles opened in this txn. */
static void dbi_update(MDBX_txn *txn, int keep) {
  tASSERT(txn, !txn->mt_parent && txn == txn->mt_env->me_txn0);
  MDBX_dbi n = txn->mt_numdbs;
  if (n) {
    bool locked = false;
    MDBX_env *const env = txn->mt_env;

    for (size_t i = n; --i >= CORE_DBS;) {
      if (likely((txn->mt_dbistate[i] & DBI_CREAT) == 0))
        continue;
      if (!locked) {
        ENSURE(env, osal_fastmutex_acquire(&env->me_dbi_lock) == MDBX_SUCCESS);
        locked = true;
      }
      if (env->me_numdbs <= i ||
          txn->mt_dbiseqs[i].weak != env->me_dbiseqs[i].weak)
        continue /* dbi explicitly closed and/or then re-opened by other txn */;
      if (keep) {
        env->me_dbflags[i] = txn->mt_dbs[i].md_flags | DB_VALID;
      } else {
        const MDBX_val name = env->me_dbxs[i].md_name;
        if (name.iov_base) {
          env->me_dbxs[i].md_name.iov_base = nullptr;
          eASSERT(env, env->me_dbflags[i] == 0);
          atomic_store32(&env->me_dbiseqs[i], dbi_seq(env, i),
                         mo_AcquireRelease);
          env->me_dbxs[i].md_name.iov_len = 0;
          if (name.iov_len)
            osal_free(name.iov_base);
        } else {
          eASSERT(env, name.iov_len == 0);
          eASSERT(env, env->me_dbflags[i] == 0);
        }
      }
    }

    n = env->me_numdbs;
    if (n > CORE_DBS && unlikely(!(env->me_dbflags[n - 1] & DB_VALID))) {
      if (!locked) {
        ENSURE(env, osal_fastmutex_acquire(&env->me_dbi_lock) == MDBX_SUCCESS);
        locked = true;
      }

      n = env->me_numdbs;
      while (n > CORE_DBS && !(env->me_dbflags[n - 1] & DB_VALID))
        --n;
      env->me_numdbs = n;
    }

    if (unlikely(locked))
      ENSURE(env, osal_fastmutex_release(&env->me_dbi_lock) == MDBX_SUCCESS);
  }
}

/* Filter-out pgno list from transaction's dirty-page list */
static void dpl_sift(MDBX_txn *const txn, MDBX_PNL pl, const bool spilled) {
  tASSERT(txn, (txn->mt_flags & MDBX_TXN_RDONLY) == 0);
  tASSERT(txn, (txn->mt_flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);
  if (MDBX_PNL_GETSIZE(pl) && txn->tw.dirtylist->length) {
    tASSERT(txn, pnl_check_allocated(pl, (size_t)txn->mt_next_pgno << spilled));
    MDBX_dpl *dl = dpl_sort(txn);

    /* Scanning in ascend order */
    const intptr_t step = MDBX_PNL_ASCENDING ? 1 : -1;
    const intptr_t begin = MDBX_PNL_ASCENDING ? 1 : MDBX_PNL_GETSIZE(pl);
    const intptr_t end = MDBX_PNL_ASCENDING ? MDBX_PNL_GETSIZE(pl) + 1 : 0;
    tASSERT(txn, pl[begin] <= pl[end - step]);

    size_t w, r = dpl_search(txn, pl[begin] >> spilled);
    tASSERT(txn, dl->sorted == dl->length);
    for (intptr_t i = begin; r <= dl->length;) { /* scan loop */
      assert(i != end);
      tASSERT(txn, !spilled || (pl[i] & 1) == 0);
      pgno_t pl_pgno = pl[i] >> spilled;
      pgno_t dp_pgno = dl->items[r].pgno;
      if (likely(dp_pgno != pl_pgno)) {
        const bool cmp = dp_pgno < pl_pgno;
        r += cmp;
        i += cmp ? 0 : step;
        if (likely(i != end))
          continue;
        return;
      }

      /* update loop */
      unsigned npages;
      w = r;
    remove_dl:
      npages = dpl_npages(dl, r);
      dl->pages_including_loose -= npages;
      if (!MDBX_AVOID_MSYNC || !(txn->mt_flags & MDBX_WRITEMAP))
        dpage_free(txn->mt_env, dl->items[r].ptr, npages);
      ++r;
    next_i:
      i += step;
      if (unlikely(i == end)) {
        while (r <= dl->length)
          dl->items[w++] = dl->items[r++];
      } else {
        while (r <= dl->length) {
          assert(i != end);
          tASSERT(txn, !spilled || (pl[i] & 1) == 0);
          pl_pgno = pl[i] >> spilled;
          dp_pgno = dl->items[r].pgno;
          if (dp_pgno < pl_pgno)
            dl->items[w++] = dl->items[r++];
          else if (dp_pgno > pl_pgno)
            goto next_i;
          else
            goto remove_dl;
        }
      }
      dl->sorted = dpl_setlen(dl, w - 1);
      txn->tw.dirtyroom += r - w;
      tASSERT(txn, txn->tw.dirtyroom + txn->tw.dirtylist->length ==
                       (txn->mt_parent ? txn->mt_parent->tw.dirtyroom
                                       : txn->mt_env->me_options.dp_limit));
      return;
    }
  }
}

/* End a transaction, except successful commit of a nested transaction.
 * May be called twice for readonly txns: First reset it, then abort.
 * [in] txn   the transaction handle to end
 * [in] mode  why and how to end the transaction */
static int txn_end(MDBX_txn *txn, const unsigned mode) {
  MDBX_env *env = txn->mt_env;
  static const char *const names[] = MDBX_END_NAMES;

#if MDBX_ENV_CHECKPID
  if (unlikely(txn->mt_env->me_pid != osal_getpid())) {
    env->me_flags |= MDBX_FATAL_ERROR;
    return MDBX_PANIC;
  }
#endif /* MDBX_ENV_CHECKPID */

  DEBUG("%s txn %" PRIaTXN "%c %p on mdbenv %p, root page %" PRIaPGNO
        "/%" PRIaPGNO,
        names[mode & MDBX_END_OPMASK], txn->mt_txnid,
        (txn->mt_flags & MDBX_TXN_RDONLY) ? 'r' : 'w', (void *)txn, (void *)env,
        txn->mt_dbs[MAIN_DBI].md_root, txn->mt_dbs[FREE_DBI].md_root);

  if (!(mode & MDBX_END_EOTDONE)) /* !(already closed cursors) */
    cursors_eot(txn, false);

  int rc = MDBX_SUCCESS;
  if (txn->mt_flags & MDBX_TXN_RDONLY) {
    if (txn->to.reader) {
      MDBX_reader *slot = txn->to.reader;
      eASSERT(env, slot->mr_pid.weak == env->me_pid);
      if (likely(!(txn->mt_flags & MDBX_TXN_FINISHED))) {
        ENSURE(env, txn->mt_txnid >=
                        /* paranoia is appropriate here */ env->me_lck
                            ->mti_oldest_reader.weak);
        eASSERT(env,
                txn->mt_txnid == slot->mr_txnid.weak &&
                    slot->mr_txnid.weak >= env->me_lck->mti_oldest_reader.weak);
#if defined(MDBX_USE_VALGRIND) || defined(__SANITIZE_ADDRESS__)
        atomic_add32(&env->me_ignore_EDEADLK, 1);
        txn_valgrind(env, nullptr);
        atomic_sub32(&env->me_ignore_EDEADLK, 1);
#endif /* MDBX_USE_VALGRIND || __SANITIZE_ADDRESS__ */
        atomic_store32(&slot->mr_snapshot_pages_used, 0, mo_Relaxed);
        safe64_reset(&slot->mr_txnid, false);
        atomic_store32(&env->me_lck->mti_readers_refresh_flag, true,
                       mo_Relaxed);
      } else {
        eASSERT(env, slot->mr_pid.weak == env->me_pid);
        eASSERT(env, slot->mr_txnid.weak >= SAFE64_INVALID_THRESHOLD);
      }
      if (mode & MDBX_END_SLOT) {
        if ((env->me_flags & MDBX_ENV_TXKEY) == 0)
          atomic_store32(&slot->mr_pid, 0, mo_Relaxed);
        txn->to.reader = NULL;
      }
    }
#if defined(_WIN32) || defined(_WIN64)
    if (txn->mt_flags & MDBX_SHRINK_ALLOWED)
      osal_srwlock_ReleaseShared(&env->me_remap_guard);
#endif
    txn->mt_numdbs = 0; /* prevent further DBI activity */
    txn->mt_flags = MDBX_TXN_RDONLY | MDBX_TXN_FINISHED;
    txn->mt_owner = 0;
  } else if (!(txn->mt_flags & MDBX_TXN_FINISHED)) {
    ENSURE(env, txn->mt_txnid >=
                    /* paranoia is appropriate here */ env->me_lck
                        ->mti_oldest_reader.weak);
#if defined(MDBX_USE_VALGRIND) || defined(__SANITIZE_ADDRESS__)
    if (txn == env->me_txn0)
      txn_valgrind(env, nullptr);
#endif /* MDBX_USE_VALGRIND || __SANITIZE_ADDRESS__ */

    txn->mt_flags = MDBX_TXN_FINISHED;
    txn->mt_owner = 0;
    env->me_txn = txn->mt_parent;
    pnl_free(txn->tw.spilled.list);
    txn->tw.spilled.list = nullptr;
    if (txn == env->me_txn0) {
      eASSERT(env, txn->mt_parent == NULL);
      /* Export or close DBI handles created in this txn */
      dbi_update(txn, mode & MDBX_END_UPDATE);
      pnl_shrink(&txn->tw.retired_pages);
      pnl_shrink(&txn->tw.relist);
      if (!(env->me_flags & MDBX_WRITEMAP))
        dlist_free(txn);
      /* The writer mutex was locked in mdbx_txn_begin. */
      mdbx_txn_unlock(env);
    } else {
      eASSERT(env, txn->mt_parent != NULL);
      MDBX_txn *const parent = txn->mt_parent;
      eASSERT(env, parent->mt_signature == MDBX_MT_SIGNATURE);
      eASSERT(env, parent->mt_child == txn &&
                       (parent->mt_flags & MDBX_TXN_HAS_CHILD) != 0);
      eASSERT(env, pnl_check_allocated(txn->tw.relist,
                                       txn->mt_next_pgno - MDBX_ENABLE_REFUND));
      eASSERT(env, memcmp(&txn->tw.troika, &parent->tw.troika,
                          sizeof(meta_troika_t)) == 0);

      if (txn->tw.lifo_reclaimed) {
        eASSERT(env, MDBX_PNL_GETSIZE(txn->tw.lifo_reclaimed) >=
                         (uintptr_t)parent->tw.lifo_reclaimed);
        MDBX_PNL_SETSIZE(txn->tw.lifo_reclaimed,
                         (uintptr_t)parent->tw.lifo_reclaimed);
        parent->tw.lifo_reclaimed = txn->tw.lifo_reclaimed;
      }

      if (txn->tw.retired_pages) {
        eASSERT(env, MDBX_PNL_GETSIZE(txn->tw.retired_pages) >=
                         (uintptr_t)parent->tw.retired_pages);
        MDBX_PNL_SETSIZE(txn->tw.retired_pages,
                         (uintptr_t)parent->tw.retired_pages);
        parent->tw.retired_pages = txn->tw.retired_pages;
      }

      parent->mt_child = nullptr;
      parent->mt_flags &= ~MDBX_TXN_HAS_CHILD;
      parent->tw.dirtylru = txn->tw.dirtylru;
      tASSERT(parent, dirtylist_check(parent));
      tASSERT(parent, audit_ex(parent, 0, false) == 0);
      dlist_free(txn);
      dpl_free(txn);
      pnl_free(txn->tw.relist);

      if (parent->mt_geo.upper != txn->mt_geo.upper ||
          parent->mt_geo.now != txn->mt_geo.now) {
        /* undo resize performed by child txn */
        rc = dxb_resize(env, parent->mt_next_pgno, parent->mt_geo.now,
                        parent->mt_geo.upper, impilict_shrink);
        if (rc == MDBX_EPERM) {
          /* unable undo resize (it is regular for Windows),
           * therefore promote size changes from child to the parent txn */
          WARNING("unable undo resize performed by child txn, promote to "
                  "the parent (%u->%u, %u->%u)",
                  txn->mt_geo.now, parent->mt_geo.now, txn->mt_geo.upper,
                  parent->mt_geo.upper);
          parent->mt_geo.now = txn->mt_geo.now;
          parent->mt_geo.upper = txn->mt_geo.upper;
          parent->mt_flags |= MDBX_TXN_DIRTY;
          rc = MDBX_SUCCESS;
        } else if (unlikely(rc != MDBX_SUCCESS)) {
          ERROR("error %d while undo resize performed by child txn, fail "
                "the parent",
                rc);
          parent->mt_flags |= MDBX_TXN_ERROR;
          if (!env->me_dxb_mmap.base)
            env->me_flags |= MDBX_FATAL_ERROR;
        }
      }
    }
  }

  eASSERT(env, txn == env->me_txn0 || txn->mt_owner == 0);
  if ((mode & MDBX_END_FREE) != 0 && txn != env->me_txn0) {
    txn->mt_signature = 0;
    osal_free(txn);
  }

  return rc;
}

int mdbx_txn_reset(MDBX_txn *txn) {
  int rc = check_txn(txn, 0);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  /* This call is only valid for read-only txns */
  if (unlikely((txn->mt_flags & MDBX_TXN_RDONLY) == 0))
    return MDBX_EINVAL;

  /* LY: don't close DBI-handles */
  rc = txn_end(txn, MDBX_END_RESET | MDBX_END_UPDATE);
  if (rc == MDBX_SUCCESS) {
    tASSERT(txn, txn->mt_signature == MDBX_MT_SIGNATURE);
    tASSERT(txn, txn->mt_owner == 0);
  }
  return rc;
}

int mdbx_txn_break(MDBX_txn *txn) {
  do {
    int rc = check_txn(txn, 0);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    txn->mt_flags |= MDBX_TXN_ERROR;
    if (txn->mt_flags & MDBX_TXN_RDONLY)
      break;
    txn = txn->mt_child;
  } while (txn);
  return MDBX_SUCCESS;
}

int mdbx_txn_abort(MDBX_txn *txn) {
  int rc = check_txn(txn, 0);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (txn->mt_flags & MDBX_TXN_RDONLY)
    /* LY: don't close DBI-handles */
    return txn_end(txn, MDBX_END_ABORT | MDBX_END_UPDATE | MDBX_END_SLOT |
                            MDBX_END_FREE);

  if (unlikely(txn->mt_flags & MDBX_TXN_FINISHED))
    return MDBX_BAD_TXN;

  if (txn->mt_child)
    mdbx_txn_abort(txn->mt_child);

  tASSERT(txn, (txn->mt_flags & MDBX_TXN_ERROR) || dirtylist_check(txn));
  return txn_end(txn, MDBX_END_ABORT | MDBX_END_SLOT | MDBX_END_FREE);
}

/* Count all the pages in each DB and in the GC and make sure
 * it matches the actual number of pages being used. */
__cold static int audit_ex(MDBX_txn *txn, size_t retired_stored,
                           bool dont_filter_gc) {
  size_t pending = 0;
  if ((txn->mt_flags & MDBX_TXN_RDONLY) == 0)
    pending = txn->tw.loose_count + MDBX_PNL_GETSIZE(txn->tw.relist) +
              (MDBX_PNL_GETSIZE(txn->tw.retired_pages) - retired_stored);

  MDBX_cursor_couple cx;
  int rc = cursor_init(&cx.outer, txn, FREE_DBI);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  size_t gc = 0;
  MDBX_val key, data;
  while ((rc = cursor_get(&cx.outer, &key, &data, MDBX_NEXT)) == 0) {
    if (!dont_filter_gc) {
      if (unlikely(key.iov_len != sizeof(txnid_t)))
        return MDBX_CORRUPTED;
      txnid_t id = unaligned_peek_u64(4, key.iov_base);
      if (txn->tw.lifo_reclaimed) {
        for (size_t i = 1; i <= MDBX_PNL_GETSIZE(txn->tw.lifo_reclaimed); ++i)
          if (id == txn->tw.lifo_reclaimed[i])
            goto skip;
      } else if (id <= txn->tw.last_reclaimed)
        goto skip;
    }

    gc += *(pgno_t *)data.iov_base;
  skip:;
  }
  tASSERT(txn, rc == MDBX_NOTFOUND);

  for (size_t i = FREE_DBI; i < txn->mt_numdbs; i++)
    txn->mt_dbistate[i] &= ~DBI_AUDITED;

  size_t used = NUM_METAS;
  for (size_t i = FREE_DBI; i <= MAIN_DBI; i++) {
    if (!(txn->mt_dbistate[i] & DBI_VALID))
      continue;
    rc = cursor_init(&cx.outer, txn, i);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    txn->mt_dbistate[i] |= DBI_AUDITED;
    if (txn->mt_dbs[i].md_root == P_INVALID)
      continue;
    used += (size_t)txn->mt_dbs[i].md_branch_pages +
            (size_t)txn->mt_dbs[i].md_leaf_pages +
            (size_t)txn->mt_dbs[i].md_overflow_pages;

    if (i != MAIN_DBI)
      continue;
    rc = page_search(&cx.outer, NULL, MDBX_PS_FIRST);
    while (rc == MDBX_SUCCESS) {
      MDBX_page *mp = cx.outer.mc_pg[cx.outer.mc_top];
      for (size_t j = 0; j < page_numkeys(mp); j++) {
        MDBX_node *node = page_node(mp, j);
        if (node_flags(node) == F_SUBDATA) {
          if (unlikely(node_ds(node) != sizeof(MDBX_db)))
            return MDBX_CORRUPTED;
          MDBX_db db_copy, *db;
          memcpy(db = &db_copy, node_data(node), sizeof(db_copy));
          if ((txn->mt_flags & MDBX_TXN_RDONLY) == 0) {
            for (MDBX_dbi k = txn->mt_numdbs; --k > MAIN_DBI;) {
              if ((txn->mt_dbistate[k] & DBI_VALID) &&
                  /* txn->mt_dbxs[k].md_name.iov_base && */
                  node_ks(node) == txn->mt_dbxs[k].md_name.iov_len &&
                  memcmp(node_key(node), txn->mt_dbxs[k].md_name.iov_base,
                         node_ks(node)) == 0) {
                txn->mt_dbistate[k] |= DBI_AUDITED;
                if (!(txn->mt_dbistate[k] & MDBX_DBI_STALE))
                  db = txn->mt_dbs + k;
                break;
              }
            }
          }
          used += (size_t)db->md_branch_pages + (size_t)db->md_leaf_pages +
                  (size_t)db->md_overflow_pages;
        }
      }
      rc = cursor_sibling(&cx.outer, SIBLING_RIGHT);
    }
    tASSERT(txn, rc == MDBX_NOTFOUND);
  }

  for (size_t i = FREE_DBI; i < txn->mt_numdbs; i++) {
    if ((txn->mt_dbistate[i] & (DBI_VALID | DBI_AUDITED | DBI_STALE)) !=
        DBI_VALID)
      continue;
    for (MDBX_txn *t = txn; t; t = t->mt_parent)
      if (F_ISSET(t->mt_dbistate[i], DBI_DIRTY | DBI_CREAT)) {
        used += (size_t)t->mt_dbs[i].md_branch_pages +
                (size_t)t->mt_dbs[i].md_leaf_pages +
                (size_t)t->mt_dbs[i].md_overflow_pages;
        txn->mt_dbistate[i] |= DBI_AUDITED;
        break;
      }
    MDBX_ANALYSIS_ASSUME(txn != nullptr);
    if (!(txn->mt_dbistate[i] & DBI_AUDITED)) {
      WARNING("audit %s@%" PRIaTXN
              ": unable account dbi %zd / \"%*s\", state 0x%02x",
              txn->mt_parent ? "nested-" : "", txn->mt_txnid, i,
              (int)txn->mt_dbxs[i].md_name.iov_len,
              (const char *)txn->mt_dbxs[i].md_name.iov_base,
              txn->mt_dbistate[i]);
    }
  }

  if (pending + gc + used == txn->mt_next_pgno)
    return MDBX_SUCCESS;

  if ((txn->mt_flags & MDBX_TXN_RDONLY) == 0)
    ERROR("audit @%" PRIaTXN ": %zu(pending) = %zu(loose) + "
          "%zu(reclaimed) + %zu(retired-pending) - %zu(retired-stored)",
          txn->mt_txnid, pending, txn->tw.loose_count,
          MDBX_PNL_GETSIZE(txn->tw.relist),
          txn->tw.retired_pages ? MDBX_PNL_GETSIZE(txn->tw.retired_pages) : 0,
          retired_stored);
  ERROR("audit @%" PRIaTXN ": %zu(pending) + %zu"
        "(gc) + %zu(count) = %zu(total) <> %zu"
        "(allocated)",
        txn->mt_txnid, pending, gc, used, pending + gc + used,
        (size_t)txn->mt_next_pgno);
  return MDBX_PROBLEM;
}

typedef struct gc_update_context {
  size_t retired_stored, loop;
  size_t settled, cleaned_slot, reused_slot, filled_slot;
  txnid_t cleaned_id, rid;
  bool lifo, dense;
#if MDBX_ENABLE_BIGFOOT
  txnid_t bigfoot;
#endif /* MDBX_ENABLE_BIGFOOT */
  MDBX_cursor cursor;
} gcu_context_t;

static __inline int gcu_context_init(MDBX_txn *txn, gcu_context_t *ctx) {
  memset(ctx, 0, offsetof(gcu_context_t, cursor));
  ctx->lifo = (txn->mt_env->me_flags & MDBX_LIFORECLAIM) != 0;
#if MDBX_ENABLE_BIGFOOT
  ctx->bigfoot = txn->mt_txnid;
#endif /* MDBX_ENABLE_BIGFOOT */
  return cursor_init(&ctx->cursor, txn, FREE_DBI);
}

static __always_inline size_t gcu_backlog_size(MDBX_txn *txn) {
  return MDBX_PNL_GETSIZE(txn->tw.relist) + txn->tw.loose_count;
}

static int gcu_clean_stored_retired(MDBX_txn *txn, gcu_context_t *ctx) {
  int err = MDBX_SUCCESS;
  if (ctx->retired_stored) {
    MDBX_cursor *const gc = ptr_disp(txn, sizeof(MDBX_txn));
    tASSERT(txn, txn == txn->mt_env->me_txn0 && gc->mc_next == nullptr);
    gc->mc_txn = txn;
    gc->mc_flags = 0;
    gc->mc_next = txn->mt_cursors[FREE_DBI];
    txn->mt_cursors[FREE_DBI] = gc;
    do {
      MDBX_val key, val;
#if MDBX_ENABLE_BIGFOOT
      key.iov_base = &ctx->bigfoot;
#else
      key.iov_base = &txn->mt_txnid;
#endif /* MDBX_ENABLE_BIGFOOT */
      key.iov_len = sizeof(txnid_t);
      const struct cursor_set_result csr = cursor_set(gc, &key, &val, MDBX_SET);
      if (csr.err == MDBX_SUCCESS && csr.exact) {
        ctx->retired_stored = 0;
        err = cursor_del(gc, 0);
        TRACE("== clear-4linear, backlog %zu, err %d", gcu_backlog_size(txn),
              err);
      }
    }
#if MDBX_ENABLE_BIGFOOT
    while (!err && --ctx->bigfoot >= txn->mt_txnid);
#else
    while (0);
#endif /* MDBX_ENABLE_BIGFOOT */
    txn->mt_cursors[FREE_DBI] = gc->mc_next;
    gc->mc_next = nullptr;
  }
  return err;
}

static int gcu_touch(gcu_context_t *ctx) {
  MDBX_val key, val;
  key.iov_base = val.iov_base = nullptr;
  key.iov_len = sizeof(txnid_t);
  val.iov_len = MDBX_PNL_SIZEOF(ctx->cursor.mc_txn->tw.retired_pages);
  ctx->cursor.mc_flags |= C_GCU;
  int err = cursor_touch(&ctx->cursor, &key, &val);
  ctx->cursor.mc_flags -= C_GCU;
  return err;
}

/* Prepare a backlog of pages to modify GC itself, while reclaiming is
 * prohibited. It should be enough to prevent search in page_alloc_slowpath()
 * during a deleting, when GC tree is unbalanced. */
static int gcu_prepare_backlog(MDBX_txn *txn, gcu_context_t *ctx) {
  const size_t for_cow = txn->mt_dbs[FREE_DBI].md_depth;
  const size_t for_rebalance = for_cow + 1 +
                               (txn->mt_dbs[FREE_DBI].md_depth + 1ul >=
                                txn->mt_dbs[FREE_DBI].md_branch_pages);
  size_t for_split = ctx->retired_stored == 0;

  const intptr_t retired_left =
      MDBX_PNL_SIZEOF(txn->tw.retired_pages) - ctx->retired_stored;
  size_t for_relist = 0;
  if (MDBX_ENABLE_BIGFOOT && retired_left > 0) {
    for_relist = (retired_left + txn->mt_env->me_maxgc_ov1page - 1) /
                 txn->mt_env->me_maxgc_ov1page;
    const size_t per_branch_page = txn->mt_env->me_maxgc_per_branch;
    for (size_t entries = for_relist; entries > 1; for_split += entries)
      entries = (entries + per_branch_page - 1) / per_branch_page;
  } else if (!MDBX_ENABLE_BIGFOOT && retired_left != 0) {
    for_relist =
        number_of_ovpages(txn->mt_env, MDBX_PNL_SIZEOF(txn->tw.retired_pages));
  }

  const size_t for_tree_before_touch = for_cow + for_rebalance + for_split;
  const size_t for_tree_after_touch = for_rebalance + for_split;
  const size_t for_all_before_touch = for_relist + for_tree_before_touch;
  const size_t for_all_after_touch = for_relist + for_tree_after_touch;

  if (likely(for_relist < 2 && gcu_backlog_size(txn) > for_all_before_touch) &&
      (ctx->cursor.mc_snum == 0 ||
       IS_MODIFIABLE(txn, ctx->cursor.mc_pg[ctx->cursor.mc_top])))
    return MDBX_SUCCESS;

  TRACE(">> retired-stored %zu, left %zi, backlog %zu, need %zu (4list %zu, "
        "4split %zu, "
        "4cow %zu, 4tree %zu)",
        ctx->retired_stored, retired_left, gcu_backlog_size(txn),
        for_all_before_touch, for_relist, for_split, for_cow,
        for_tree_before_touch);

  int err = gcu_touch(ctx);
  TRACE("== after-touch, backlog %zu, err %d", gcu_backlog_size(txn), err);

  if (!MDBX_ENABLE_BIGFOOT && unlikely(for_relist > 1) &&
      MDBX_PNL_GETSIZE(txn->tw.retired_pages) != ctx->retired_stored &&
      err == MDBX_SUCCESS) {
    if (unlikely(ctx->retired_stored)) {
      err = gcu_clean_stored_retired(txn, ctx);
      if (unlikely(err != MDBX_SUCCESS))
        return err;
      if (!ctx->retired_stored)
        return /* restart by tail-recursion */ gcu_prepare_backlog(txn, ctx);
    }
    err = page_alloc_slowpath(&ctx->cursor, for_relist, MDBX_ALLOC_RESERVE).err;
    TRACE("== after-4linear, backlog %zu, err %d", gcu_backlog_size(txn), err);
    cASSERT(&ctx->cursor,
            gcu_backlog_size(txn) >= for_relist || err != MDBX_SUCCESS);
  }

  while (gcu_backlog_size(txn) < for_all_after_touch && err == MDBX_SUCCESS)
    err = page_alloc_slowpath(&ctx->cursor, 0,
                              MDBX_ALLOC_RESERVE | MDBX_ALLOC_UNIMPORTANT)
              .err;

  TRACE("<< backlog %zu, err %d, gc: height %u, branch %zu, leaf %zu, large "
        "%zu, entries %zu",
        gcu_backlog_size(txn), err, txn->mt_dbs[FREE_DBI].md_depth,
        (size_t)txn->mt_dbs[FREE_DBI].md_branch_pages,
        (size_t)txn->mt_dbs[FREE_DBI].md_leaf_pages,
        (size_t)txn->mt_dbs[FREE_DBI].md_overflow_pages,
        (size_t)txn->mt_dbs[FREE_DBI].md_entries);
  tASSERT(txn,
          err != MDBX_NOTFOUND || (txn->mt_flags & MDBX_TXN_DRAINED_GC) != 0);
  return (err != MDBX_NOTFOUND) ? err : MDBX_SUCCESS;
}

static __inline void gcu_clean_reserved(MDBX_env *env, MDBX_val pnl) {
#if MDBX_DEBUG && (defined(MDBX_USE_VALGRIND) || defined(__SANITIZE_ADDRESS__))
  /* Для предотвращения предупреждения Valgrind из mdbx_dump_val()
   * вызванное через макрос DVAL_DEBUG() на выходе
   * из cursor_set(MDBX_SET_KEY), которая вызывается ниже внутри update_gc() в
   * цикле очистки и цикле заполнения зарезервированных элементов. */
  memset(pnl.iov_base, 0xBB, pnl.iov_len);
#endif /* MDBX_DEBUG && (MDBX_USE_VALGRIND || __SANITIZE_ADDRESS__) */

  /* PNL is initially empty, zero out at least the length */
  memset(pnl.iov_base, 0, sizeof(pgno_t));
  if ((env->me_flags & (MDBX_WRITEMAP | MDBX_NOMEMINIT)) == 0)
    /* zero out to avoid leaking values from uninitialized malloc'ed memory
     * to the file in non-writemap mode if length of the saving page-list
     * was changed during space reservation. */
    memset(pnl.iov_base, 0, pnl.iov_len);
}

/* Cleanups reclaimed GC (aka freeDB) records, saves the retired-list (aka
 * freelist) of current transaction to GC, puts back into GC leftover of the
 * reclaimed pages with chunking. This recursive changes the reclaimed-list,
 * loose-list and retired-list. Keep trying until it stabilizes.
 *
 * NOTE: This code is a consequence of many iterations of adding crutches (aka
 * "checks and balances") to partially bypass the fundamental design problems
 * inherited from LMDB. So do not try to understand it completely in order to
 * avoid your madness. */
static int update_gc(MDBX_txn *txn, gcu_context_t *ctx) {
  TRACE("\n>>> @%" PRIaTXN, txn->mt_txnid);
  MDBX_env *const env = txn->mt_env;
  const char *const dbg_prefix_mode = ctx->lifo ? "    lifo" : "    fifo";
  (void)dbg_prefix_mode;
  ctx->cursor.mc_next = txn->mt_cursors[FREE_DBI];
  txn->mt_cursors[FREE_DBI] = &ctx->cursor;

  /* txn->tw.relist[] can grow and shrink during this call.
   * txn->tw.last_reclaimed and txn->tw.retired_pages[] can only grow.
   * But page numbers cannot disappear from txn->tw.retired_pages[]. */

retry:
  if (ctx->loop++)
    TRACE("%s", " >> restart");
  int rc = MDBX_SUCCESS;
  tASSERT(txn, pnl_check_allocated(txn->tw.relist,
                                   txn->mt_next_pgno - MDBX_ENABLE_REFUND));
  tASSERT(txn, dirtylist_check(txn));
  if (unlikely(/* paranoia */ ctx->loop > ((MDBX_DEBUG > 0) ? 12 : 42))) {
    ERROR("too more loops %zu, bailout", ctx->loop);
    rc = MDBX_PROBLEM;
    goto bailout;
  }

  if (unlikely(ctx->dense)) {
    rc = gcu_clean_stored_retired(txn, ctx);
    if (unlikely(rc != MDBX_SUCCESS))
      goto bailout;
  }

  ctx->settled = 0;
  ctx->cleaned_slot = 0;
  ctx->reused_slot = 0;
  ctx->filled_slot = ~0u;
  ctx->cleaned_id = 0;
  ctx->rid = txn->tw.last_reclaimed;
  while (true) {
    /* Come back here after each Put() in case retired-list changed */
    TRACE("%s", " >> continue");

    if (ctx->retired_stored != MDBX_PNL_GETSIZE(txn->tw.retired_pages) &&
        (ctx->loop == 1 || ctx->retired_stored > env->me_maxgc_ov1page ||
         MDBX_PNL_GETSIZE(txn->tw.retired_pages) > env->me_maxgc_ov1page)) {
      rc = gcu_prepare_backlog(txn, ctx);
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;
    }

    tASSERT(txn, pnl_check_allocated(txn->tw.relist,
                                     txn->mt_next_pgno - MDBX_ENABLE_REFUND));
    MDBX_val key, data;
    if (ctx->lifo) {
      if (ctx->cleaned_slot < (txn->tw.lifo_reclaimed
                                   ? MDBX_PNL_GETSIZE(txn->tw.lifo_reclaimed)
                                   : 0)) {
        ctx->settled = 0;
        ctx->cleaned_slot = 0;
        ctx->reused_slot = 0;
        ctx->filled_slot = ~0u;
        /* LY: cleanup reclaimed records. */
        do {
          ctx->cleaned_id = txn->tw.lifo_reclaimed[++ctx->cleaned_slot];
          tASSERT(txn,
                  ctx->cleaned_slot > 0 &&
                      ctx->cleaned_id <= env->me_lck->mti_oldest_reader.weak);
          key.iov_base = &ctx->cleaned_id;
          key.iov_len = sizeof(ctx->cleaned_id);
          rc = cursor_set(&ctx->cursor, &key, NULL, MDBX_SET).err;
          if (rc == MDBX_NOTFOUND)
            continue;
          if (unlikely(rc != MDBX_SUCCESS))
            goto bailout;
          if (likely(!ctx->dense)) {
            rc = gcu_prepare_backlog(txn, ctx);
            if (unlikely(rc != MDBX_SUCCESS))
              goto bailout;
          }
          tASSERT(txn, ctx->cleaned_id <= env->me_lck->mti_oldest_reader.weak);
          TRACE("%s: cleanup-reclaimed-id [%zu]%" PRIaTXN, dbg_prefix_mode,
                ctx->cleaned_slot, ctx->cleaned_id);
          tASSERT(txn, *txn->mt_cursors == &ctx->cursor);
          rc = cursor_del(&ctx->cursor, 0);
          if (unlikely(rc != MDBX_SUCCESS))
            goto bailout;
        } while (ctx->cleaned_slot < MDBX_PNL_GETSIZE(txn->tw.lifo_reclaimed));
        txl_sort(txn->tw.lifo_reclaimed);
      }
    } else {
      /* Удаляем оставшиеся вынутые из GC записи. */
      while (ctx->cleaned_id <= txn->tw.last_reclaimed) {
        rc = cursor_first(&ctx->cursor, &key, NULL);
        if (rc == MDBX_NOTFOUND)
          break;
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
        if (!MDBX_DISABLE_VALIDATION &&
            unlikely(key.iov_len != sizeof(txnid_t))) {
          rc = MDBX_CORRUPTED;
          goto bailout;
        }
        ctx->rid = ctx->cleaned_id;
        ctx->settled = 0;
        ctx->reused_slot = 0;
        ctx->cleaned_id = unaligned_peek_u64(4, key.iov_base);
        if (ctx->cleaned_id > txn->tw.last_reclaimed)
          break;
        if (likely(!ctx->dense)) {
          rc = gcu_prepare_backlog(txn, ctx);
          if (unlikely(rc != MDBX_SUCCESS))
            goto bailout;
        }
        tASSERT(txn, ctx->cleaned_id <= txn->tw.last_reclaimed);
        tASSERT(txn, ctx->cleaned_id <= env->me_lck->mti_oldest_reader.weak);
        TRACE("%s: cleanup-reclaimed-id %" PRIaTXN, dbg_prefix_mode,
              ctx->cleaned_id);
        tASSERT(txn, *txn->mt_cursors == &ctx->cursor);
        rc = cursor_del(&ctx->cursor, 0);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
      }
    }

    tASSERT(txn, pnl_check_allocated(txn->tw.relist,
                                     txn->mt_next_pgno - MDBX_ENABLE_REFUND));
    tASSERT(txn, dirtylist_check(txn));
    if (AUDIT_ENABLED()) {
      rc = audit_ex(txn, ctx->retired_stored, false);
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;
    }

    /* return suitable into unallocated space */
    if (txn_refund(txn)) {
      tASSERT(txn, pnl_check_allocated(txn->tw.relist,
                                       txn->mt_next_pgno - MDBX_ENABLE_REFUND));
      if (AUDIT_ENABLED()) {
        rc = audit_ex(txn, ctx->retired_stored, false);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
      }
    }

    /* handle loose pages - put ones into the reclaimed- or retired-list */
    if (txn->tw.loose_pages) {
      tASSERT(txn, txn->tw.loose_count > 0);
      /* Return loose page numbers to tw.relist,
       * though usually none are left at this point.
       * The pages themselves remain in dirtylist. */
      if (unlikely(!txn->tw.lifo_reclaimed && txn->tw.last_reclaimed < 1)) {
        TRACE("%s: try allocate gc-slot for %zu loose-pages", dbg_prefix_mode,
              txn->tw.loose_count);
        rc = page_alloc_slowpath(&ctx->cursor, 0, MDBX_ALLOC_RESERVE).err;
        if (rc == MDBX_SUCCESS) {
          TRACE("%s: retry since gc-slot for %zu loose-pages available",
                dbg_prefix_mode, txn->tw.loose_count);
          continue;
        }

        /* Put loose page numbers in tw.retired_pages,
         * since unable to return them to tw.relist. */
        if (unlikely((rc = pnl_need(&txn->tw.retired_pages,
                                    txn->tw.loose_count)) != 0))
          goto bailout;
        for (MDBX_page *lp = txn->tw.loose_pages; lp; lp = mp_next(lp)) {
          pnl_xappend(txn->tw.retired_pages, lp->mp_pgno);
          MDBX_ASAN_UNPOISON_MEMORY_REGION(&mp_next(lp), sizeof(MDBX_page *));
          VALGRIND_MAKE_MEM_DEFINED(&mp_next(lp), sizeof(MDBX_page *));
        }
        TRACE("%s: append %zu loose-pages to retired-pages", dbg_prefix_mode,
              txn->tw.loose_count);
      } else {
        /* Room for loose pages + temp PNL with same */
        rc = pnl_need(&txn->tw.relist, 2 * txn->tw.loose_count + 2);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
        MDBX_PNL loose = txn->tw.relist + MDBX_PNL_ALLOCLEN(txn->tw.relist) -
                         txn->tw.loose_count - 1;
        size_t count = 0;
        for (MDBX_page *lp = txn->tw.loose_pages; lp; lp = mp_next(lp)) {
          tASSERT(txn, lp->mp_flags == P_LOOSE);
          loose[++count] = lp->mp_pgno;
          MDBX_ASAN_UNPOISON_MEMORY_REGION(&mp_next(lp), sizeof(MDBX_page *));
          VALGRIND_MAKE_MEM_DEFINED(&mp_next(lp), sizeof(MDBX_page *));
        }
        tASSERT(txn, count == txn->tw.loose_count);
        MDBX_PNL_SETSIZE(loose, count);
        pnl_sort(loose, txn->mt_next_pgno);
        pnl_merge(txn->tw.relist, loose);
        TRACE("%s: append %zu loose-pages to reclaimed-pages", dbg_prefix_mode,
              txn->tw.loose_count);
      }

      /* filter-out list of dirty-pages from loose-pages */
      MDBX_dpl *const dl = txn->tw.dirtylist;
      if (dl) {
        tASSERT(txn, (txn->mt_flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);
        tASSERT(txn, dl->sorted <= dl->length);
        size_t w = 0, sorted_out = 0;
        for (size_t r = w; ++r <= dl->length;) {
          MDBX_page *dp = dl->items[r].ptr;
          tASSERT(txn, dp->mp_flags == P_LOOSE || IS_MODIFIABLE(txn, dp));
          tASSERT(txn, dpl_endpgno(dl, r) <= txn->mt_next_pgno);
          if ((dp->mp_flags & P_LOOSE) == 0) {
            if (++w != r)
              dl->items[w] = dl->items[r];
          } else {
            tASSERT(txn, dp->mp_flags == P_LOOSE);
            sorted_out += dl->sorted >= r;
            if (!MDBX_AVOID_MSYNC || !(env->me_flags & MDBX_WRITEMAP)) {
              tASSERT(txn, (txn->mt_flags & MDBX_WRITEMAP) == 0);
              dpage_free(env, dp, 1);
            }
          }
        }
        TRACE("%s: filtered-out loose-pages from %zu -> %zu dirty-pages",
              dbg_prefix_mode, dl->length, w);
        tASSERT(txn, txn->tw.loose_count == dl->length - w);
        dl->sorted -= sorted_out;
        tASSERT(txn, dl->sorted <= w);
        dpl_setlen(dl, w);
        dl->pages_including_loose -= txn->tw.loose_count;
        txn->tw.dirtyroom += txn->tw.loose_count;
        tASSERT(txn, txn->tw.dirtyroom + txn->tw.dirtylist->length ==
                         (txn->mt_parent ? txn->mt_parent->tw.dirtyroom
                                         : txn->mt_env->me_options.dp_limit));
      } else {
        tASSERT(txn, (txn->mt_flags & MDBX_WRITEMAP) != 0 && !MDBX_AVOID_MSYNC);
      }
      txn->tw.loose_pages = NULL;
      txn->tw.loose_count = 0;
#if MDBX_ENABLE_REFUND
      txn->tw.loose_refund_wl = 0;
#endif /* MDBX_ENABLE_REFUND */
    }

    const size_t amount = MDBX_PNL_GETSIZE(txn->tw.relist);
    /* handle retired-list - store ones into single gc-record */
    if (ctx->retired_stored < MDBX_PNL_GETSIZE(txn->tw.retired_pages)) {
      if (unlikely(!ctx->retired_stored)) {
        /* Make sure last page of GC is touched and on retired-list */
        rc = cursor_last(&ctx->cursor, nullptr, nullptr);
        if (likely(rc == MDBX_SUCCESS))
          rc = gcu_touch(ctx);
        if (unlikely(rc != MDBX_SUCCESS) && rc != MDBX_NOTFOUND)
          goto bailout;
      }

#if MDBX_ENABLE_BIGFOOT
      size_t retired_pages_before;
      do {
        if (ctx->bigfoot > txn->mt_txnid) {
          rc = gcu_clean_stored_retired(txn, ctx);
          if (unlikely(rc != MDBX_SUCCESS))
            goto bailout;
          tASSERT(txn, ctx->bigfoot <= txn->mt_txnid);
        }

        retired_pages_before = MDBX_PNL_GETSIZE(txn->tw.retired_pages);
        rc = gcu_prepare_backlog(txn, ctx);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
        if (retired_pages_before != MDBX_PNL_GETSIZE(txn->tw.retired_pages)) {
          TRACE("%s: retired-list changed (%zu -> %zu), retry", dbg_prefix_mode,
                retired_pages_before, MDBX_PNL_GETSIZE(txn->tw.retired_pages));
          break;
        }

        pnl_sort(txn->tw.retired_pages, txn->mt_next_pgno);
        ctx->retired_stored = 0;
        ctx->bigfoot = txn->mt_txnid;
        do {
          if (ctx->retired_stored) {
            rc = gcu_prepare_backlog(txn, ctx);
            if (unlikely(rc != MDBX_SUCCESS))
              goto bailout;
            if (ctx->retired_stored >=
                MDBX_PNL_GETSIZE(txn->tw.retired_pages)) {
              TRACE("%s: retired-list changed (%zu -> %zu), retry",
                    dbg_prefix_mode, retired_pages_before,
                    MDBX_PNL_GETSIZE(txn->tw.retired_pages));
              break;
            }
          }
          key.iov_len = sizeof(txnid_t);
          key.iov_base = &ctx->bigfoot;
          const size_t left =
              MDBX_PNL_GETSIZE(txn->tw.retired_pages) - ctx->retired_stored;
          const size_t chunk =
              (left > env->me_maxgc_ov1page && ctx->bigfoot < MAX_TXNID)
                  ? env->me_maxgc_ov1page
                  : left;
          data.iov_len = (chunk + 1) * sizeof(pgno_t);
          rc = cursor_put_nochecklen(&ctx->cursor, &key, &data, MDBX_RESERVE);
          if (unlikely(rc != MDBX_SUCCESS))
            goto bailout;

#if MDBX_DEBUG && (defined(MDBX_USE_VALGRIND) || defined(__SANITIZE_ADDRESS__))
          /* Для предотвращения предупреждения Valgrind из mdbx_dump_val()
           * вызванное через макрос DVAL_DEBUG() на выходе
           * из cursor_set(MDBX_SET_KEY), которая вызывается как выше в цикле
           * очистки, так и ниже в цикле заполнения зарезервированных элементов.
           */
          memset(data.iov_base, 0xBB, data.iov_len);
#endif /* MDBX_DEBUG && (MDBX_USE_VALGRIND || __SANITIZE_ADDRESS__) */

          if (retired_pages_before == MDBX_PNL_GETSIZE(txn->tw.retired_pages)) {
            const size_t at = (ctx->lifo == MDBX_PNL_ASCENDING)
                                  ? left - chunk
                                  : ctx->retired_stored;
            pgno_t *const begin = txn->tw.retired_pages + at;
            /* MDBX_PNL_ASCENDING == false && LIFO == false:
             *  - the larger pgno is at the beginning of retired list
             *    and should be placed with the larger txnid.
             * MDBX_PNL_ASCENDING == true && LIFO == true:
             *  - the larger pgno is at the ending of retired list
             *    and should be placed with the smaller txnid.
             */
            const pgno_t save = *begin;
            *begin = (pgno_t)chunk;
            memcpy(data.iov_base, begin, data.iov_len);
            *begin = save;
            TRACE("%s: put-retired/bigfoot @ %" PRIaTXN
                  " (slice #%u) #%zu [%zu..%zu] of %zu",
                  dbg_prefix_mode, ctx->bigfoot,
                  (unsigned)(ctx->bigfoot - txn->mt_txnid), chunk, at,
                  at + chunk, retired_pages_before);
          }
          ctx->retired_stored += chunk;
        } while (ctx->retired_stored <
                     MDBX_PNL_GETSIZE(txn->tw.retired_pages) &&
                 (++ctx->bigfoot, true));
      } while (retired_pages_before != MDBX_PNL_GETSIZE(txn->tw.retired_pages));
#else
      /* Write to last page of GC */
      key.iov_len = sizeof(txnid_t);
      key.iov_base = &txn->mt_txnid;
      do {
        gcu_prepare_backlog(txn, ctx);
        data.iov_len = MDBX_PNL_SIZEOF(txn->tw.retired_pages);
        rc = cursor_put_nochecklen(&ctx->cursor, &key, &data, MDBX_RESERVE);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;

#if MDBX_DEBUG && (defined(MDBX_USE_VALGRIND) || defined(__SANITIZE_ADDRESS__))
        /* Для предотвращения предупреждения Valgrind из mdbx_dump_val()
         * вызванное через макрос DVAL_DEBUG() на выходе
         * из cursor_set(MDBX_SET_KEY), которая вызывается как выше в цикле
         * очистки, так и ниже в цикле заполнения зарезервированных элементов.
         */
        memset(data.iov_base, 0xBB, data.iov_len);
#endif /* MDBX_DEBUG && (MDBX_USE_VALGRIND || __SANITIZE_ADDRESS__) */

        /* Retry if tw.retired_pages[] grew during the Put() */
      } while (data.iov_len < MDBX_PNL_SIZEOF(txn->tw.retired_pages));

      ctx->retired_stored = MDBX_PNL_GETSIZE(txn->tw.retired_pages);
      pnl_sort(txn->tw.retired_pages, txn->mt_next_pgno);
      eASSERT(env, data.iov_len == MDBX_PNL_SIZEOF(txn->tw.retired_pages));
      memcpy(data.iov_base, txn->tw.retired_pages, data.iov_len);

      TRACE("%s: put-retired #%zu @ %" PRIaTXN, dbg_prefix_mode,
            ctx->retired_stored, txn->mt_txnid);
#endif /* MDBX_ENABLE_BIGFOOT */
      if (LOG_ENABLED(MDBX_LOG_EXTRA)) {
        size_t i = ctx->retired_stored;
        DEBUG_EXTRA("txn %" PRIaTXN " root %" PRIaPGNO " num %zu, retired-PNL",
                    txn->mt_txnid, txn->mt_dbs[FREE_DBI].md_root, i);
        for (; i; i--)
          DEBUG_EXTRA_PRINT(" %" PRIaPGNO, txn->tw.retired_pages[i]);
        DEBUG_EXTRA_PRINT("%s\n", ".");
      }
      if (unlikely(amount != MDBX_PNL_GETSIZE(txn->tw.relist) &&
                   ctx->settled)) {
        TRACE("%s: reclaimed-list changed %zu -> %zu, retry", dbg_prefix_mode,
              amount, MDBX_PNL_GETSIZE(txn->tw.relist));
        goto retry /* rare case, but avoids GC fragmentation
                                and one cycle. */
            ;
      }
      continue;
    }

    /* handle reclaimed and lost pages - merge and store both into gc */
    tASSERT(txn, pnl_check_allocated(txn->tw.relist,
                                     txn->mt_next_pgno - MDBX_ENABLE_REFUND));
    tASSERT(txn, txn->tw.loose_count == 0);

    TRACE("%s", " >> reserving");
    if (AUDIT_ENABLED()) {
      rc = audit_ex(txn, ctx->retired_stored, false);
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;
    }
    const size_t left = amount - ctx->settled;
    TRACE("%s: amount %zu, settled %zd, left %zd, lifo-reclaimed-slots %zu, "
          "reused-gc-slots %zu",
          dbg_prefix_mode, amount, ctx->settled, left,
          txn->tw.lifo_reclaimed ? MDBX_PNL_GETSIZE(txn->tw.lifo_reclaimed) : 0,
          ctx->reused_slot);
    if (0 >= (intptr_t)left)
      break;

    const size_t prefer_max_scatter = MDBX_ENABLE_BIGFOOT ? MDBX_TXL_MAX : 257;
    txnid_t reservation_gc_id;
    if (ctx->lifo) {
      if (txn->tw.lifo_reclaimed == nullptr) {
        txn->tw.lifo_reclaimed = txl_alloc();
        if (unlikely(!txn->tw.lifo_reclaimed)) {
          rc = MDBX_ENOMEM;
          goto bailout;
        }
      }
      if (MDBX_PNL_GETSIZE(txn->tw.lifo_reclaimed) < prefer_max_scatter &&
          left > (MDBX_PNL_GETSIZE(txn->tw.lifo_reclaimed) - ctx->reused_slot) *
                     env->me_maxgc_ov1page &&
          !ctx->dense) {
        /* Hужен свободный для для сохранения списка страниц. */
        bool need_cleanup = false;
        txnid_t snap_oldest = 0;
      retry_rid:
        do {
          rc = page_alloc_slowpath(&ctx->cursor, 0, MDBX_ALLOC_RESERVE).err;
          snap_oldest = env->me_lck->mti_oldest_reader.weak;
          if (likely(rc == MDBX_SUCCESS)) {
            TRACE("%s: took @%" PRIaTXN " from GC", dbg_prefix_mode,
                  MDBX_PNL_LAST(txn->tw.lifo_reclaimed));
            need_cleanup = true;
          }
        } while (
            rc == MDBX_SUCCESS &&
            MDBX_PNL_GETSIZE(txn->tw.lifo_reclaimed) < prefer_max_scatter &&
            left >
                (MDBX_PNL_GETSIZE(txn->tw.lifo_reclaimed) - ctx->reused_slot) *
                    env->me_maxgc_ov1page);

        if (likely(rc == MDBX_SUCCESS)) {
          TRACE("%s: got enough from GC.", dbg_prefix_mode);
          continue;
        } else if (unlikely(rc != MDBX_NOTFOUND))
          /* LY: some troubles... */
          goto bailout;

        if (MDBX_PNL_GETSIZE(txn->tw.lifo_reclaimed)) {
          if (need_cleanup) {
            txl_sort(txn->tw.lifo_reclaimed);
            ctx->cleaned_slot = 0;
          }
          ctx->rid = MDBX_PNL_LAST(txn->tw.lifo_reclaimed);
        } else {
          tASSERT(txn, txn->tw.last_reclaimed == 0);
          if (unlikely(txn_oldest_reader(txn) != snap_oldest))
            /* should retry page_alloc_slowpath()
             * if the oldest reader changes since the last attempt */
            goto retry_rid;
          /* no reclaimable GC entries,
           * therefore no entries with ID < mdbx_find_oldest(txn) */
          txn->tw.last_reclaimed = ctx->rid = snap_oldest;
          TRACE("%s: none recycled yet, set rid to @%" PRIaTXN, dbg_prefix_mode,
                ctx->rid);
        }

        /* В GC нет годных к переработке записей,
         * будем использовать свободные id в обратном порядке. */
        while (MDBX_PNL_GETSIZE(txn->tw.lifo_reclaimed) < prefer_max_scatter &&
               left > (MDBX_PNL_GETSIZE(txn->tw.lifo_reclaimed) -
                       ctx->reused_slot) *
                          env->me_maxgc_ov1page) {
          if (unlikely(ctx->rid <= MIN_TXNID)) {
            if (unlikely(MDBX_PNL_GETSIZE(txn->tw.lifo_reclaimed) <=
                         ctx->reused_slot)) {
              VERBOSE("** restart: reserve depleted (reused_gc_slot %zu >= "
                      "lifo_reclaimed %zu" PRIaTXN,
                      ctx->reused_slot,
                      MDBX_PNL_GETSIZE(txn->tw.lifo_reclaimed));
              goto retry;
            }
            break;
          }

          tASSERT(txn, ctx->rid >= MIN_TXNID && ctx->rid <= MAX_TXNID);
          ctx->rid -= 1;
          key.iov_base = &ctx->rid;
          key.iov_len = sizeof(ctx->rid);
          rc = cursor_set(&ctx->cursor, &key, &data, MDBX_SET_KEY).err;
          if (unlikely(rc == MDBX_SUCCESS)) {
            DEBUG("%s: GC's id %" PRIaTXN " is present, going to first",
                  dbg_prefix_mode, ctx->rid);
            rc = cursor_first(&ctx->cursor, &key, nullptr);
            if (unlikely(rc != MDBX_SUCCESS ||
                         key.iov_len != sizeof(txnid_t))) {
              rc = MDBX_CORRUPTED;
              goto bailout;
            }
            const txnid_t gc_first = unaligned_peek_u64(4, key.iov_base);
            if (gc_first <= MIN_TXNID) {
              DEBUG("%s: no free GC's id(s) less than %" PRIaTXN
                    " (going dense-mode)",
                    dbg_prefix_mode, ctx->rid);
              ctx->dense = true;
              break;
            }
            ctx->rid = gc_first - 1;
          }

          eASSERT(env, !ctx->dense);
          rc = txl_append(&txn->tw.lifo_reclaimed, ctx->rid);
          if (unlikely(rc != MDBX_SUCCESS))
            goto bailout;

          if (ctx->reused_slot)
            /* rare case, but it is better to clear and re-create GC entries
             * with less fragmentation. */
            need_cleanup = true;
          else
            ctx->cleaned_slot +=
                1 /* mark cleanup is not needed for added slot. */;

          TRACE("%s: append @%" PRIaTXN
                " to lifo-reclaimed, cleaned-gc-slot = %zu",
                dbg_prefix_mode, ctx->rid, ctx->cleaned_slot);
        }

        if (need_cleanup || ctx->dense) {
          if (ctx->cleaned_slot) {
            TRACE("%s: restart to clear and re-create GC entries",
                  dbg_prefix_mode);
            goto retry;
          }
          continue;
        }
      }

      const size_t i =
          MDBX_PNL_GETSIZE(txn->tw.lifo_reclaimed) - ctx->reused_slot;
      tASSERT(txn, i > 0 && i <= MDBX_PNL_GETSIZE(txn->tw.lifo_reclaimed));
      reservation_gc_id = txn->tw.lifo_reclaimed[i];
      TRACE("%s: take @%" PRIaTXN " from lifo-reclaimed[%zu]", dbg_prefix_mode,
            reservation_gc_id, i);
    } else {
      tASSERT(txn, txn->tw.lifo_reclaimed == NULL);
      if (unlikely(ctx->rid == 0)) {
        ctx->rid = txn_oldest_reader(txn);
        rc = cursor_first(&ctx->cursor, &key, nullptr);
        if (likely(rc == MDBX_SUCCESS)) {
          if (unlikely(key.iov_len != sizeof(txnid_t))) {
            rc = MDBX_CORRUPTED;
            goto bailout;
          }
          const txnid_t gc_first = unaligned_peek_u64(4, key.iov_base);
          if (ctx->rid >= gc_first)
            ctx->rid = gc_first - 1;
          if (unlikely(ctx->rid == 0)) {
            ERROR("%s", "** no GC tail-space to store (going dense-mode)");
            ctx->dense = true;
            goto retry;
          }
        } else if (rc != MDBX_NOTFOUND)
          goto bailout;
        txn->tw.last_reclaimed = ctx->rid;
        ctx->cleaned_id = ctx->rid + 1;
      }
      reservation_gc_id = ctx->rid--;
      TRACE("%s: take @%" PRIaTXN " from head-gc-id", dbg_prefix_mode,
            reservation_gc_id);
    }
    ++ctx->reused_slot;

    size_t chunk = left;
    if (unlikely(chunk > env->me_maxgc_ov1page)) {
      const size_t avail_gc_slots =
          txn->tw.lifo_reclaimed
              ? MDBX_PNL_GETSIZE(txn->tw.lifo_reclaimed) - ctx->reused_slot + 1
          : (ctx->rid < INT16_MAX) ? (size_t)ctx->rid
                                   : INT16_MAX;
      if (avail_gc_slots > 1) {
#if MDBX_ENABLE_BIGFOOT
        chunk = (chunk < env->me_maxgc_ov1page * (size_t)2)
                    ? chunk / 2
                    : env->me_maxgc_ov1page;
#else
        if (chunk < env->me_maxgc_ov1page * 2)
          chunk /= 2;
        else {
          const size_t threshold =
              env->me_maxgc_ov1page * ((avail_gc_slots < prefer_max_scatter)
                                           ? avail_gc_slots
                                           : prefer_max_scatter);
          if (left < threshold)
            chunk = env->me_maxgc_ov1page;
          else {
            const size_t tail = left - threshold + env->me_maxgc_ov1page + 1;
            size_t span = 1;
            size_t avail = ((pgno2bytes(env, span) - PAGEHDRSZ) /
                            sizeof(pgno_t)) /* - 1 + span */;
            if (tail > avail) {
              for (size_t i = amount - span; i > 0; --i) {
                if (MDBX_PNL_ASCENDING ? (txn->tw.relist[i] + span)
                                       : (txn->tw.relist[i] - span) ==
                                             txn->tw.relist[i + span]) {
                  span += 1;
                  avail =
                      ((pgno2bytes(env, span) - PAGEHDRSZ) / sizeof(pgno_t)) -
                      1 + span;
                  if (avail >= tail)
                    break;
                }
              }
            }

            chunk = (avail >= tail) ? tail - span
                    : (avail_gc_slots > 3 &&
                       ctx->reused_slot < prefer_max_scatter - 3)
                        ? avail - span
                        : tail;
          }
        }
#endif /* MDBX_ENABLE_BIGFOOT */
      }
    }
    tASSERT(txn, chunk > 0);

    TRACE("%s: gc_rid %" PRIaTXN ", reused_gc_slot %zu, reservation-id "
          "%" PRIaTXN,
          dbg_prefix_mode, ctx->rid, ctx->reused_slot, reservation_gc_id);

    TRACE("%s: chunk %zu, gc-per-ovpage %u", dbg_prefix_mode, chunk,
          env->me_maxgc_ov1page);

    tASSERT(txn, reservation_gc_id <= env->me_lck->mti_oldest_reader.weak);
    if (unlikely(
            reservation_gc_id < MIN_TXNID ||
            reservation_gc_id >
                atomic_load64(&env->me_lck->mti_oldest_reader, mo_Relaxed))) {
      ERROR("** internal error (reservation_gc_id %" PRIaTXN ")",
            reservation_gc_id);
      rc = MDBX_PROBLEM;
      goto bailout;
    }

    key.iov_len = sizeof(reservation_gc_id);
    key.iov_base = &reservation_gc_id;
    data.iov_len = (chunk + 1) * sizeof(pgno_t);
    TRACE("%s: reserve %zu [%zu...%zu) @%" PRIaTXN, dbg_prefix_mode, chunk,
          ctx->settled + 1, ctx->settled + chunk + 1, reservation_gc_id);
    gcu_prepare_backlog(txn, ctx);
    rc = cursor_put_nochecklen(&ctx->cursor, &key, &data,
                               MDBX_RESERVE | MDBX_NOOVERWRITE);
    tASSERT(txn, pnl_check_allocated(txn->tw.relist,
                                     txn->mt_next_pgno - MDBX_ENABLE_REFUND));
    if (unlikely(rc != MDBX_SUCCESS))
      goto bailout;

    gcu_clean_reserved(env, data);
    ctx->settled += chunk;
    TRACE("%s: settled %zu (+%zu), continue", dbg_prefix_mode, ctx->settled,
          chunk);

    if (txn->tw.lifo_reclaimed &&
        unlikely(amount < MDBX_PNL_GETSIZE(txn->tw.relist)) &&
        (ctx->loop < 5 ||
         MDBX_PNL_GETSIZE(txn->tw.relist) - amount > env->me_maxgc_ov1page)) {
      NOTICE("** restart: reclaimed-list growth %zu -> %zu", amount,
             MDBX_PNL_GETSIZE(txn->tw.relist));
      goto retry;
    }

    continue;
  }

  tASSERT(txn,
          ctx->cleaned_slot == (txn->tw.lifo_reclaimed
                                    ? MDBX_PNL_GETSIZE(txn->tw.lifo_reclaimed)
                                    : 0));

  TRACE("%s", " >> filling");
  /* Fill in the reserved records */
  ctx->filled_slot =
      txn->tw.lifo_reclaimed
          ? MDBX_PNL_GETSIZE(txn->tw.lifo_reclaimed) - ctx->reused_slot
          : ctx->reused_slot;
  rc = MDBX_SUCCESS;
  tASSERT(txn, pnl_check_allocated(txn->tw.relist,
                                   txn->mt_next_pgno - MDBX_ENABLE_REFUND));
  tASSERT(txn, dirtylist_check(txn));
  if (MDBX_PNL_GETSIZE(txn->tw.relist)) {
    MDBX_val key, data;
    key.iov_len = data.iov_len = 0; /* avoid MSVC warning */
    key.iov_base = data.iov_base = NULL;

    const size_t amount = MDBX_PNL_GETSIZE(txn->tw.relist);
    size_t left = amount;
    if (txn->tw.lifo_reclaimed == nullptr) {
      tASSERT(txn, ctx->lifo == 0);
      rc = cursor_first(&ctx->cursor, &key, &data);
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;
    } else {
      tASSERT(txn, ctx->lifo != 0);
    }

    while (true) {
      txnid_t fill_gc_id;
      TRACE("%s: left %zu of %zu", dbg_prefix_mode, left,
            MDBX_PNL_GETSIZE(txn->tw.relist));
      if (txn->tw.lifo_reclaimed == nullptr) {
        tASSERT(txn, ctx->lifo == 0);
        fill_gc_id = unaligned_peek_u64(4, key.iov_base);
        if (ctx->filled_slot-- == 0 || fill_gc_id > txn->tw.last_reclaimed) {
          VERBOSE(
              "** restart: reserve depleted (filled_slot %zu, fill_id %" PRIaTXN
              " > last_reclaimed %" PRIaTXN,
              ctx->filled_slot, fill_gc_id, txn->tw.last_reclaimed);
          goto retry;
        }
      } else {
        tASSERT(txn, ctx->lifo != 0);
        if (++ctx->filled_slot > MDBX_PNL_GETSIZE(txn->tw.lifo_reclaimed)) {
          VERBOSE("** restart: reserve depleted (filled_gc_slot %zu > "
                  "lifo_reclaimed %zu" PRIaTXN,
                  ctx->filled_slot, MDBX_PNL_GETSIZE(txn->tw.lifo_reclaimed));
          goto retry;
        }
        fill_gc_id = txn->tw.lifo_reclaimed[ctx->filled_slot];
        TRACE("%s: seek-reservation @%" PRIaTXN " at lifo_reclaimed[%zu]",
              dbg_prefix_mode, fill_gc_id, ctx->filled_slot);
        key.iov_base = &fill_gc_id;
        key.iov_len = sizeof(fill_gc_id);
        rc = cursor_set(&ctx->cursor, &key, &data, MDBX_SET_KEY).err;
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
      }
      tASSERT(txn, ctx->cleaned_slot ==
                       (txn->tw.lifo_reclaimed
                            ? MDBX_PNL_GETSIZE(txn->tw.lifo_reclaimed)
                            : 0));
      tASSERT(txn, fill_gc_id > 0 &&
                       fill_gc_id <= env->me_lck->mti_oldest_reader.weak);
      key.iov_base = &fill_gc_id;
      key.iov_len = sizeof(fill_gc_id);

      tASSERT(txn, data.iov_len >= sizeof(pgno_t) * 2);
      size_t chunk = data.iov_len / sizeof(pgno_t) - 1;
      if (unlikely(chunk > left)) {
        TRACE("%s: chunk %zu > left %zu, @%" PRIaTXN, dbg_prefix_mode, chunk,
              left, fill_gc_id);
        if ((ctx->loop < 5 && chunk - left > ctx->loop / 2) ||
            chunk - left > env->me_maxgc_ov1page) {
          data.iov_len = (left + 1) * sizeof(pgno_t);
        }
        chunk = left;
      }
      rc = cursor_put_nochecklen(&ctx->cursor, &key, &data,
                                 MDBX_CURRENT | MDBX_RESERVE);
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;
      gcu_clean_reserved(env, data);

      if (unlikely(txn->tw.loose_count ||
                   amount != MDBX_PNL_GETSIZE(txn->tw.relist))) {
        NOTICE("** restart: reclaimed-list growth (%zu -> %zu, loose +%zu)",
               amount, MDBX_PNL_GETSIZE(txn->tw.relist), txn->tw.loose_count);
        goto retry;
      }
      if (unlikely(txn->tw.lifo_reclaimed
                       ? ctx->cleaned_slot <
                             MDBX_PNL_GETSIZE(txn->tw.lifo_reclaimed)
                       : ctx->cleaned_id < txn->tw.last_reclaimed)) {
        NOTICE("%s", "** restart: reclaimed-slots changed");
        goto retry;
      }
      if (unlikely(ctx->retired_stored !=
                   MDBX_PNL_GETSIZE(txn->tw.retired_pages))) {
        tASSERT(txn,
                ctx->retired_stored < MDBX_PNL_GETSIZE(txn->tw.retired_pages));
        NOTICE("** restart: retired-list growth (%zu -> %zu)",
               ctx->retired_stored, MDBX_PNL_GETSIZE(txn->tw.retired_pages));
        goto retry;
      }

      pgno_t *dst = data.iov_base;
      *dst++ = (pgno_t)chunk;
      pgno_t *src = MDBX_PNL_BEGIN(txn->tw.relist) + left - chunk;
      memcpy(dst, src, chunk * sizeof(pgno_t));
      pgno_t *from = src, *to = src + chunk;
      TRACE("%s: fill %zu [ %zu:%" PRIaPGNO "...%zu:%" PRIaPGNO "] @%" PRIaTXN,
            dbg_prefix_mode, chunk, from - txn->tw.relist, from[0],
            to - txn->tw.relist, to[-1], fill_gc_id);

      left -= chunk;
      if (AUDIT_ENABLED()) {
        rc = audit_ex(txn, ctx->retired_stored + amount - left, true);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
      }
      if (left == 0) {
        rc = MDBX_SUCCESS;
        break;
      }

      if (txn->tw.lifo_reclaimed == nullptr) {
        tASSERT(txn, ctx->lifo == 0);
        rc = cursor_next(&ctx->cursor, &key, &data, MDBX_NEXT);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
      } else {
        tASSERT(txn, ctx->lifo != 0);
      }
    }
  }

  tASSERT(txn, rc == MDBX_SUCCESS);
  if (unlikely(txn->tw.loose_count != 0)) {
    NOTICE("** restart: got %zu loose pages", txn->tw.loose_count);
    goto retry;
  }
  if (unlikely(ctx->filled_slot !=
               (txn->tw.lifo_reclaimed
                    ? MDBX_PNL_GETSIZE(txn->tw.lifo_reclaimed)
                    : 0))) {

    const bool will_retry = ctx->loop < 9;
    NOTICE("** %s: reserve excess (filled-slot %zu, loop %zu)",
           will_retry ? "restart" : "ignore", ctx->filled_slot, ctx->loop);
    if (will_retry)
      goto retry;
  }

  tASSERT(txn,
          txn->tw.lifo_reclaimed == NULL ||
              ctx->cleaned_slot == MDBX_PNL_GETSIZE(txn->tw.lifo_reclaimed));

bailout:
  txn->mt_cursors[FREE_DBI] = ctx->cursor.mc_next;

  MDBX_PNL_SETSIZE(txn->tw.relist, 0);
#if MDBX_ENABLE_PROFGC
  env->me_lck->mti_pgop_stat.gc_prof.wloops += (uint32_t)ctx->loop;
#endif /* MDBX_ENABLE_PROFGC */
  TRACE("<<< %zu loops, rc = %d", ctx->loop, rc);
  return rc;
}

static int txn_write(MDBX_txn *txn, iov_ctx_t *ctx) {
  tASSERT(txn, (txn->mt_flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);
  MDBX_dpl *const dl = dpl_sort(txn);
  int rc = MDBX_SUCCESS;
  size_t r, w, total_npages = 0;
  for (w = 0, r = 1; r <= dl->length; ++r) {
    MDBX_page *dp = dl->items[r].ptr;
    if (dp->mp_flags & P_LOOSE) {
      dl->items[++w] = dl->items[r];
      continue;
    }
    unsigned npages = dpl_npages(dl, r);
    total_npages += npages;
    rc = iov_page(txn, ctx, dp, npages);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }

  if (!iov_empty(ctx)) {
    tASSERT(txn, rc == MDBX_SUCCESS);
    rc = iov_write(ctx);
  }

  if (likely(rc == MDBX_SUCCESS) && ctx->fd == txn->mt_env->me_lazy_fd) {
    txn->mt_env->me_lck->mti_unsynced_pages.weak += total_npages;
    if (!txn->mt_env->me_lck->mti_eoos_timestamp.weak)
      txn->mt_env->me_lck->mti_eoos_timestamp.weak = osal_monotime();
  }

  txn->tw.dirtylist->pages_including_loose -= total_npages;
  while (r <= dl->length)
    dl->items[++w] = dl->items[r++];

  dl->sorted = dpl_setlen(dl, w);
  txn->tw.dirtyroom += r - 1 - w;
  tASSERT(txn, txn->tw.dirtyroom + txn->tw.dirtylist->length ==
                   (txn->mt_parent ? txn->mt_parent->tw.dirtyroom
                                   : txn->mt_env->me_options.dp_limit));
  tASSERT(txn, txn->tw.dirtylist->length == txn->tw.loose_count);
  tASSERT(txn, txn->tw.dirtylist->pages_including_loose == txn->tw.loose_count);
  return rc;
}

/* Check txn and dbi arguments to a function */
static __always_inline bool check_dbi(const MDBX_txn *txn, MDBX_dbi dbi,
                                      unsigned validity) {
  if (likely(dbi < txn->mt_numdbs)) {
    if (likely(!dbi_changed(txn, dbi))) {
      if (likely(txn->mt_dbistate[dbi] & validity))
        return true;
      if (likely(dbi < CORE_DBS ||
                 (txn->mt_env->me_dbflags[dbi] & DB_VALID) == 0))
        return false;
    }
  }
  return dbi_import((MDBX_txn *)txn, dbi);
}

/* Merge child txn into parent */
static __inline void txn_merge(MDBX_txn *const parent, MDBX_txn *const txn,
                               const size_t parent_retired_len) {
  tASSERT(txn, (txn->mt_flags & MDBX_WRITEMAP) == 0);
  MDBX_dpl *const src = dpl_sort(txn);

  /* Remove refunded pages from parent's dirty list */
  MDBX_dpl *const dst = dpl_sort(parent);
  if (MDBX_ENABLE_REFUND) {
    size_t n = dst->length;
    while (n && dst->items[n].pgno >= parent->mt_next_pgno) {
      const unsigned npages = dpl_npages(dst, n);
      dpage_free(txn->mt_env, dst->items[n].ptr, npages);
      --n;
    }
    parent->tw.dirtyroom += dst->sorted - n;
    dst->sorted = dpl_setlen(dst, n);
    tASSERT(parent,
            parent->tw.dirtyroom + parent->tw.dirtylist->length ==
                (parent->mt_parent ? parent->mt_parent->tw.dirtyroom
                                   : parent->mt_env->me_options.dp_limit));
  }

  /* Remove reclaimed pages from parent's dirty list */
  const MDBX_PNL reclaimed_list = parent->tw.relist;
  dpl_sift(parent, reclaimed_list, false);

  /* Move retired pages from parent's dirty & spilled list to reclaimed */
  size_t r, w, d, s, l;
  for (r = w = parent_retired_len;
       ++r <= MDBX_PNL_GETSIZE(parent->tw.retired_pages);) {
    const pgno_t pgno = parent->tw.retired_pages[r];
    const size_t di = dpl_exist(parent, pgno);
    const size_t si = !di ? search_spilled(parent, pgno) : 0;
    unsigned npages;
    const char *kind;
    if (di) {
      MDBX_page *dp = dst->items[di].ptr;
      tASSERT(parent, (dp->mp_flags & ~(P_LEAF | P_LEAF2 | P_BRANCH |
                                        P_OVERFLOW | P_SPILLED)) == 0);
      npages = dpl_npages(dst, di);
      page_wash(parent, di, dp, npages);
      kind = "dirty";
      l = 1;
      if (unlikely(npages > l)) {
        /* OVERFLOW-страница могла быть переиспользована по частям. Тогда
         * в retired-списке может быть только начало последовательности,
         * а остаток растащен по dirty, spilled и reclaimed спискам. Поэтому
         * переносим в reclaimed с проверкой на обрыв последовательности.
         * В любом случае, все осколки будут учтены и отфильтрованы, т.е. если
         * страница была разбита на части, то важно удалить dirty-элемент,
         * а все осколки будут учтены отдельно. */

        /* Список retired страниц не сортирован, но для ускорения сортировки
         * дополняется в соответствии с MDBX_PNL_ASCENDING */
#if MDBX_PNL_ASCENDING
        const size_t len = MDBX_PNL_GETSIZE(parent->tw.retired_pages);
        while (r < len && parent->tw.retired_pages[r + 1] == pgno + l) {
          ++r;
          if (++l == npages)
            break;
        }
#else
        while (w > parent_retired_len &&
               parent->tw.retired_pages[w - 1] == pgno + l) {
          --w;
          if (++l == npages)
            break;
        }
#endif
      }
    } else if (unlikely(si)) {
      l = npages = 1;
      spill_remove(parent, si, 1);
      kind = "spilled";
    } else {
      parent->tw.retired_pages[++w] = pgno;
      continue;
    }

    DEBUG("reclaim retired parent's %u -> %zu %s page %" PRIaPGNO, npages, l,
          kind, pgno);
    int err = pnl_insert_range(&parent->tw.relist, pgno, l);
    ENSURE(txn->mt_env, err == MDBX_SUCCESS);
  }
  MDBX_PNL_SETSIZE(parent->tw.retired_pages, w);

  /* Filter-out parent spill list */
  if (parent->tw.spilled.list &&
      MDBX_PNL_GETSIZE(parent->tw.spilled.list) > 0) {
    const MDBX_PNL sl = spill_purge(parent);
    size_t len = MDBX_PNL_GETSIZE(sl);
    if (len) {
      /* Remove refunded pages from parent's spill list */
      if (MDBX_ENABLE_REFUND &&
          MDBX_PNL_MOST(sl) >= (parent->mt_next_pgno << 1)) {
#if MDBX_PNL_ASCENDING
        size_t i = MDBX_PNL_GETSIZE(sl);
        assert(MDBX_PNL_MOST(sl) == MDBX_PNL_LAST(sl));
        do {
          if ((sl[i] & 1) == 0)
            DEBUG("refund parent's spilled page %" PRIaPGNO, sl[i] >> 1);
          i -= 1;
        } while (i && sl[i] >= (parent->mt_next_pgno << 1));
        MDBX_PNL_SETSIZE(sl, i);
#else
        assert(MDBX_PNL_MOST(sl) == MDBX_PNL_FIRST(sl));
        size_t i = 0;
        do {
          ++i;
          if ((sl[i] & 1) == 0)
            DEBUG("refund parent's spilled page %" PRIaPGNO, sl[i] >> 1);
        } while (i < len && sl[i + 1] >= (parent->mt_next_pgno << 1));
        MDBX_PNL_SETSIZE(sl, len -= i);
        memmove(sl + 1, sl + 1 + i, len * sizeof(sl[0]));
#endif
      }
      tASSERT(txn, pnl_check_allocated(sl, (size_t)parent->mt_next_pgno << 1));

      /* Remove reclaimed pages from parent's spill list */
      s = MDBX_PNL_GETSIZE(sl), r = MDBX_PNL_GETSIZE(reclaimed_list);
      /* Scanning from end to begin */
      while (s && r) {
        if (sl[s] & 1) {
          --s;
          continue;
        }
        const pgno_t spilled_pgno = sl[s] >> 1;
        const pgno_t reclaimed_pgno = reclaimed_list[r];
        if (reclaimed_pgno != spilled_pgno) {
          const bool cmp = MDBX_PNL_ORDERED(spilled_pgno, reclaimed_pgno);
          s -= !cmp;
          r -= cmp;
        } else {
          DEBUG("remove reclaimed parent's spilled page %" PRIaPGNO,
                reclaimed_pgno);
          spill_remove(parent, s, 1);
          --s;
          --r;
        }
      }

      /* Remove anything in our dirty list from parent's spill list */
      /* Scanning spill list in descend order */
      const intptr_t step = MDBX_PNL_ASCENDING ? -1 : 1;
      s = MDBX_PNL_ASCENDING ? MDBX_PNL_GETSIZE(sl) : 1;
      d = src->length;
      while (d && (MDBX_PNL_ASCENDING ? s > 0 : s <= MDBX_PNL_GETSIZE(sl))) {
        if (sl[s] & 1) {
          s += step;
          continue;
        }
        const pgno_t spilled_pgno = sl[s] >> 1;
        const pgno_t dirty_pgno_form = src->items[d].pgno;
        const unsigned npages = dpl_npages(src, d);
        const pgno_t dirty_pgno_to = dirty_pgno_form + npages;
        if (dirty_pgno_form > spilled_pgno) {
          --d;
          continue;
        }
        if (dirty_pgno_to <= spilled_pgno) {
          s += step;
          continue;
        }

        DEBUG("remove dirtied parent's spilled %u page %" PRIaPGNO, npages,
              dirty_pgno_form);
        spill_remove(parent, s, 1);
        s += step;
      }

      /* Squash deleted pagenums if we deleted any */
      spill_purge(parent);
    }
  }

  /* Remove anything in our spill list from parent's dirty list */
  if (txn->tw.spilled.list) {
    tASSERT(txn, pnl_check_allocated(txn->tw.spilled.list,
                                     (size_t)parent->mt_next_pgno << 1));
    dpl_sift(parent, txn->tw.spilled.list, true);
    tASSERT(parent,
            parent->tw.dirtyroom + parent->tw.dirtylist->length ==
                (parent->mt_parent ? parent->mt_parent->tw.dirtyroom
                                   : parent->mt_env->me_options.dp_limit));
  }

  /* Find length of merging our dirty list with parent's and release
   * filter-out pages */
  for (l = 0, d = dst->length, s = src->length; d > 0 && s > 0;) {
    MDBX_page *sp = src->items[s].ptr;
    tASSERT(parent, (sp->mp_flags & ~(P_LEAF | P_LEAF2 | P_BRANCH | P_OVERFLOW |
                                      P_LOOSE | P_SPILLED)) == 0);
    const unsigned s_npages = dpl_npages(src, s);
    const pgno_t s_pgno = src->items[s].pgno;

    MDBX_page *dp = dst->items[d].ptr;
    tASSERT(parent, (dp->mp_flags & ~(P_LEAF | P_LEAF2 | P_BRANCH | P_OVERFLOW |
                                      P_SPILLED)) == 0);
    const unsigned d_npages = dpl_npages(dst, d);
    const pgno_t d_pgno = dst->items[d].pgno;

    if (d_pgno >= s_pgno + s_npages) {
      --d;
      ++l;
    } else if (d_pgno + d_npages <= s_pgno) {
      if (sp->mp_flags != P_LOOSE) {
        sp->mp_txnid = parent->mt_front;
        sp->mp_flags &= ~P_SPILLED;
      }
      --s;
      ++l;
    } else {
      dst->items[d--].ptr = nullptr;
      dpage_free(txn->mt_env, dp, d_npages);
    }
  }
  assert(dst->sorted == dst->length);
  tASSERT(parent, dst->detent >= l + d + s);
  dst->sorted = l + d + s; /* the merged length */

  while (s > 0) {
    MDBX_page *sp = src->items[s].ptr;
    tASSERT(parent, (sp->mp_flags & ~(P_LEAF | P_LEAF2 | P_BRANCH | P_OVERFLOW |
                                      P_LOOSE | P_SPILLED)) == 0);
    if (sp->mp_flags != P_LOOSE) {
      sp->mp_txnid = parent->mt_front;
      sp->mp_flags &= ~P_SPILLED;
    }
    --s;
  }

  /* Merge our dirty list into parent's, i.e. merge(dst, src) -> dst */
  if (dst->sorted >= dst->length) {
    /* from end to begin with dst extending */
    for (l = dst->sorted, s = src->length, d = dst->length; s > 0 && d > 0;) {
      if (unlikely(l <= d)) {
        /* squash to get a gap of free space for merge */
        for (r = w = 1; r <= d; ++r)
          if (dst->items[r].ptr) {
            if (w != r) {
              dst->items[w] = dst->items[r];
              dst->items[r].ptr = nullptr;
            }
            ++w;
          }
        VERBOSE("squash to begin for extending-merge %zu -> %zu", d, w - 1);
        d = w - 1;
        continue;
      }
      assert(l > d);
      if (dst->items[d].ptr) {
        dst->items[l--] = (dst->items[d].pgno > src->items[s].pgno)
                              ? dst->items[d--]
                              : src->items[s--];
      } else
        --d;
    }
    if (s > 0) {
      assert(l == s);
      while (d > 0) {
        assert(dst->items[d].ptr == nullptr);
        --d;
      }
      do {
        assert(l > 0);
        dst->items[l--] = src->items[s--];
      } while (s > 0);
    } else {
      assert(l == d);
      while (l > 0) {
        assert(dst->items[l].ptr != nullptr);
        --l;
      }
    }
  } else {
    /* from begin to end with shrinking (a lot of new large/overflow pages) */
    for (l = s = d = 1; s <= src->length && d <= dst->length;) {
      if (unlikely(l >= d)) {
        /* squash to get a gap of free space for merge */
        for (r = w = dst->length; r >= d; --r)
          if (dst->items[r].ptr) {
            if (w != r) {
              dst->items[w] = dst->items[r];
              dst->items[r].ptr = nullptr;
            }
            --w;
          }
        VERBOSE("squash to end for shrinking-merge %zu -> %zu", d, w + 1);
        d = w + 1;
        continue;
      }
      assert(l < d);
      if (dst->items[d].ptr) {
        dst->items[l++] = (dst->items[d].pgno < src->items[s].pgno)
                              ? dst->items[d++]
                              : src->items[s++];
      } else
        ++d;
    }
    if (s <= src->length) {
      assert(dst->sorted - l == src->length - s);
      while (d <= dst->length) {
        assert(dst->items[d].ptr == nullptr);
        --d;
      }
      do {
        assert(l <= dst->sorted);
        dst->items[l++] = src->items[s++];
      } while (s <= src->length);
    } else {
      assert(dst->sorted - l == dst->length - d);
      while (l <= dst->sorted) {
        assert(l <= d && d <= dst->length && dst->items[d].ptr);
        dst->items[l++] = dst->items[d++];
      }
    }
  }
  parent->tw.dirtyroom -= dst->sorted - dst->length;
  assert(parent->tw.dirtyroom <= parent->mt_env->me_options.dp_limit);
  dpl_setlen(dst, dst->sorted);
  parent->tw.dirtylru = txn->tw.dirtylru;

  /* В текущем понимании выгоднее пересчитать кол-во страниц,
   * чем подмешивать лишние ветвления и вычисления в циклы выше. */
  dst->pages_including_loose = 0;
  for (r = 1; r <= dst->length; ++r)
    dst->pages_including_loose += dpl_npages(dst, r);

  tASSERT(parent, dirtylist_check(parent));
  dpl_free(txn);

  if (txn->tw.spilled.list) {
    if (parent->tw.spilled.list) {
      /* Must not fail since space was preserved above. */
      pnl_merge(parent->tw.spilled.list, txn->tw.spilled.list);
      pnl_free(txn->tw.spilled.list);
    } else {
      parent->tw.spilled.list = txn->tw.spilled.list;
      parent->tw.spilled.least_removed = txn->tw.spilled.least_removed;
    }
    tASSERT(parent, dirtylist_check(parent));
  }

  parent->mt_flags &= ~MDBX_TXN_HAS_CHILD;
  if (parent->tw.spilled.list) {
    assert(pnl_check_allocated(parent->tw.spilled.list,
                               (size_t)parent->mt_next_pgno << 1));
    if (MDBX_PNL_GETSIZE(parent->tw.spilled.list))
      parent->mt_flags |= MDBX_TXN_SPILLS;
  }
}

static void take_gcprof(MDBX_txn *txn, MDBX_commit_latency *latency) {
  MDBX_env *const env = txn->mt_env;
  if (MDBX_ENABLE_PROFGC) {
    pgop_stat_t *const ptr = &env->me_lck->mti_pgop_stat;
    latency->gc_prof.work_counter = ptr->gc_prof.work.spe_counter;
    latency->gc_prof.work_rtime_monotonic =
        osal_monotime_to_16dot16(ptr->gc_prof.work.rtime_monotonic);
    latency->gc_prof.work_xtime_cpu =
        osal_monotime_to_16dot16(ptr->gc_prof.work.xtime_cpu);
    latency->gc_prof.work_rsteps = ptr->gc_prof.work.rsteps;
    latency->gc_prof.work_xpages = ptr->gc_prof.work.xpages;
    latency->gc_prof.work_majflt = ptr->gc_prof.work.majflt;

    latency->gc_prof.self_counter = ptr->gc_prof.self.spe_counter;
    latency->gc_prof.self_rtime_monotonic =
        osal_monotime_to_16dot16(ptr->gc_prof.self.rtime_monotonic);
    latency->gc_prof.self_xtime_cpu =
        osal_monotime_to_16dot16(ptr->gc_prof.self.xtime_cpu);
    latency->gc_prof.self_rsteps = ptr->gc_prof.self.rsteps;
    latency->gc_prof.self_xpages = ptr->gc_prof.self.xpages;
    latency->gc_prof.self_majflt = ptr->gc_prof.self.majflt;

    latency->gc_prof.wloops = ptr->gc_prof.wloops;
    latency->gc_prof.coalescences = ptr->gc_prof.coalescences;
    latency->gc_prof.wipes = ptr->gc_prof.wipes;
    latency->gc_prof.flushes = ptr->gc_prof.flushes;
    latency->gc_prof.kicks = ptr->gc_prof.kicks;
    if (txn == env->me_txn0)
      memset(&ptr->gc_prof, 0, sizeof(ptr->gc_prof));
  } else
    memset(&latency->gc_prof, 0, sizeof(latency->gc_prof));
}

int mdbx_txn_commit_ex(MDBX_txn *txn, MDBX_commit_latency *latency) {
  STATIC_ASSERT(MDBX_TXN_FINISHED ==
                MDBX_TXN_BLOCKED - MDBX_TXN_HAS_CHILD - MDBX_TXN_ERROR);
  const uint64_t ts_0 = latency ? osal_monotime() : 0;
  uint64_t ts_1 = 0, ts_2 = 0, ts_3 = 0, ts_4 = 0, ts_5 = 0, gc_cputime = 0;

  int rc = check_txn(txn, MDBX_TXN_FINISHED);
  if (unlikely(rc != MDBX_SUCCESS)) {
    if (latency)
      memset(latency, 0, sizeof(*latency));
    return rc;
  }

  MDBX_env *const env = txn->mt_env;
#if MDBX_ENV_CHECKPID
  if (unlikely(env->me_pid != osal_getpid())) {
    env->me_flags |= MDBX_FATAL_ERROR;
    if (latency)
      memset(latency, 0, sizeof(*latency));
    return MDBX_PANIC;
  }
#endif /* MDBX_ENV_CHECKPID */

  if (unlikely(txn->mt_flags & MDBX_TXN_ERROR)) {
    rc = MDBX_RESULT_TRUE;
    goto fail;
  }

  /* txn_end() mode for a commit which writes nothing */
  unsigned end_mode =
      MDBX_END_PURE_COMMIT | MDBX_END_UPDATE | MDBX_END_SLOT | MDBX_END_FREE;
  if (unlikely(txn->mt_flags & MDBX_TXN_RDONLY))
    goto done;

  if (txn->mt_child) {
    rc = mdbx_txn_commit_ex(txn->mt_child, NULL);
    tASSERT(txn, txn->mt_child == NULL);
    if (unlikely(rc != MDBX_SUCCESS))
      goto fail;
  }

  if (unlikely(txn != env->me_txn)) {
    DEBUG("%s", "attempt to commit unknown transaction");
    rc = MDBX_EINVAL;
    goto fail;
  }

  if (txn->mt_parent) {
    tASSERT(txn, audit_ex(txn, 0, false) == 0);
    eASSERT(env, txn != env->me_txn0);
    MDBX_txn *const parent = txn->mt_parent;
    eASSERT(env, parent->mt_signature == MDBX_MT_SIGNATURE);
    eASSERT(env, parent->mt_child == txn &&
                     (parent->mt_flags & MDBX_TXN_HAS_CHILD) != 0);
    eASSERT(env, dirtylist_check(txn));

    if (txn->tw.dirtylist->length == 0 && !(txn->mt_flags & MDBX_TXN_DIRTY) &&
        parent->mt_numdbs == txn->mt_numdbs) {
      for (int i = txn->mt_numdbs; --i >= 0;) {
        tASSERT(txn, (txn->mt_dbistate[i] & DBI_DIRTY) == 0);
        if ((txn->mt_dbistate[i] & DBI_STALE) &&
            !(parent->mt_dbistate[i] & DBI_STALE))
          tASSERT(txn, memcmp(&parent->mt_dbs[i], &txn->mt_dbs[i],
                              sizeof(MDBX_db)) == 0);
      }

      tASSERT(txn, memcmp(&parent->mt_geo, &txn->mt_geo,
                          sizeof(parent->mt_geo)) == 0);
      tASSERT(txn, memcmp(&parent->mt_canary, &txn->mt_canary,
                          sizeof(parent->mt_canary)) == 0);
      tASSERT(txn, !txn->tw.spilled.list ||
                       MDBX_PNL_GETSIZE(txn->tw.spilled.list) == 0);
      tASSERT(txn, txn->tw.loose_count == 0);

      /* fast completion of pure nested transaction */
      end_mode = MDBX_END_PURE_COMMIT | MDBX_END_SLOT | MDBX_END_FREE;
      goto done;
    }

    /* Preserve space for spill list to avoid parent's state corruption
     * if allocation fails. */
    const size_t parent_retired_len = (uintptr_t)parent->tw.retired_pages;
    tASSERT(txn, parent_retired_len <= MDBX_PNL_GETSIZE(txn->tw.retired_pages));
    const size_t retired_delta =
        MDBX_PNL_GETSIZE(txn->tw.retired_pages) - parent_retired_len;
    if (retired_delta) {
      rc = pnl_need(&txn->tw.relist, retired_delta);
      if (unlikely(rc != MDBX_SUCCESS))
        goto fail;
    }

    if (txn->tw.spilled.list) {
      if (parent->tw.spilled.list) {
        rc = pnl_need(&parent->tw.spilled.list,
                      MDBX_PNL_GETSIZE(txn->tw.spilled.list));
        if (unlikely(rc != MDBX_SUCCESS))
          goto fail;
      }
      spill_purge(txn);
    }

    if (unlikely(txn->tw.dirtylist->length + parent->tw.dirtylist->length >
                     parent->tw.dirtylist->detent &&
                 !dpl_reserve(parent, txn->tw.dirtylist->length +
                                          parent->tw.dirtylist->length))) {
      rc = MDBX_ENOMEM;
      goto fail;
    }

    //-------------------------------------------------------------------------

    parent->tw.lifo_reclaimed = txn->tw.lifo_reclaimed;
    txn->tw.lifo_reclaimed = NULL;

    parent->tw.retired_pages = txn->tw.retired_pages;
    txn->tw.retired_pages = NULL;

    pnl_free(parent->tw.relist);
    parent->tw.relist = txn->tw.relist;
    txn->tw.relist = NULL;
    parent->tw.last_reclaimed = txn->tw.last_reclaimed;

    parent->mt_geo = txn->mt_geo;
    parent->mt_canary = txn->mt_canary;
    parent->mt_flags |= txn->mt_flags & MDBX_TXN_DIRTY;

    /* Move loose pages to parent */
#if MDBX_ENABLE_REFUND
    parent->tw.loose_refund_wl = txn->tw.loose_refund_wl;
#endif /* MDBX_ENABLE_REFUND */
    parent->tw.loose_count = txn->tw.loose_count;
    parent->tw.loose_pages = txn->tw.loose_pages;

    /* Merge our cursors into parent's and close them */
    cursors_eot(txn, true);
    end_mode |= MDBX_END_EOTDONE;

    /* Update parent's DBs array */
    memcpy(parent->mt_dbs, txn->mt_dbs, txn->mt_numdbs * sizeof(MDBX_db));
    parent->mt_numdbs = txn->mt_numdbs;
    for (size_t i = 0; i < txn->mt_numdbs; i++) {
      /* preserve parent's status */
      const uint8_t state =
          txn->mt_dbistate[i] |
          (parent->mt_dbistate[i] & (DBI_CREAT | DBI_FRESH | DBI_DIRTY));
      DEBUG("dbi %zu dbi-state %s 0x%02x -> 0x%02x", i,
            (parent->mt_dbistate[i] != state) ? "update" : "still",
            parent->mt_dbistate[i], state);
      parent->mt_dbistate[i] = state;
    }

    if (latency) {
      ts_1 = osal_monotime();
      ts_2 = /* no gc-update */ ts_1;
      ts_3 = /* no audit */ ts_2;
      ts_4 = /* no write */ ts_3;
      ts_5 = /* no sync */ ts_4;
    }
    txn_merge(parent, txn, parent_retired_len);
    env->me_txn = parent;
    parent->mt_child = NULL;
    tASSERT(parent, dirtylist_check(parent));

#if MDBX_ENABLE_REFUND
    txn_refund(parent);
    if (ASSERT_ENABLED()) {
      /* Check parent's loose pages not suitable for refund */
      for (MDBX_page *lp = parent->tw.loose_pages; lp; lp = mp_next(lp)) {
        tASSERT(parent, lp->mp_pgno < parent->tw.loose_refund_wl &&
                            lp->mp_pgno + 1 < parent->mt_next_pgno);
        MDBX_ASAN_UNPOISON_MEMORY_REGION(&mp_next(lp), sizeof(MDBX_page *));
        VALGRIND_MAKE_MEM_DEFINED(&mp_next(lp), sizeof(MDBX_page *));
      }
      /* Check parent's reclaimed pages not suitable for refund */
      if (MDBX_PNL_GETSIZE(parent->tw.relist))
        tASSERT(parent,
                MDBX_PNL_MOST(parent->tw.relist) + 1 < parent->mt_next_pgno);
    }
#endif /* MDBX_ENABLE_REFUND */

    txn->mt_signature = 0;
    osal_free(txn);
    tASSERT(parent, audit_ex(parent, 0, false) == 0);
    rc = MDBX_SUCCESS;
    goto provide_latency;
  }

  if (!txn->tw.dirtylist) {
    tASSERT(txn, (txn->mt_flags & MDBX_WRITEMAP) != 0 && !MDBX_AVOID_MSYNC);
  } else {
    tASSERT(txn, (txn->mt_flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);
    tASSERT(txn, txn->tw.dirtyroom + txn->tw.dirtylist->length ==
                     (txn->mt_parent ? txn->mt_parent->tw.dirtyroom
                                     : txn->mt_env->me_options.dp_limit));
  }
  cursors_eot(txn, false);
  end_mode |= MDBX_END_EOTDONE;

  if ((!txn->tw.dirtylist || txn->tw.dirtylist->length == 0) &&
      (txn->mt_flags & (MDBX_TXN_DIRTY | MDBX_TXN_SPILLS)) == 0) {
    for (intptr_t i = txn->mt_numdbs; --i >= 0;)
      tASSERT(txn, (txn->mt_dbistate[i] & DBI_DIRTY) == 0);
#if defined(MDBX_NOSUCCESS_EMPTY_COMMIT) && MDBX_NOSUCCESS_EMPTY_COMMIT
    rc = txn_end(txn, end_mode);
    if (unlikely(rc != MDBX_SUCCESS))
      goto fail;
    rc = MDBX_RESULT_TRUE;
    goto provide_latency;
#else
    goto done;
#endif /* MDBX_NOSUCCESS_EMPTY_COMMIT */
  }

  DEBUG("committing txn %" PRIaTXN " %p on mdbenv %p, root page %" PRIaPGNO
        "/%" PRIaPGNO,
        txn->mt_txnid, (void *)txn, (void *)env, txn->mt_dbs[MAIN_DBI].md_root,
        txn->mt_dbs[FREE_DBI].md_root);

  /* Update DB root pointers */
  if (txn->mt_numdbs > CORE_DBS) {
    MDBX_cursor_couple couple;
    MDBX_val data;
    data.iov_len = sizeof(MDBX_db);

    rc = cursor_init(&couple.outer, txn, MAIN_DBI);
    if (unlikely(rc != MDBX_SUCCESS))
      goto fail;
    for (MDBX_dbi i = CORE_DBS; i < txn->mt_numdbs; i++) {
      if (txn->mt_dbistate[i] & DBI_DIRTY) {
        MDBX_db *db = &txn->mt_dbs[i];
        DEBUG("update main's entry for sub-db %u, mod_txnid %" PRIaTXN
              " -> %" PRIaTXN,
              i, db->md_mod_txnid, txn->mt_txnid);
        /* Может быть mod_txnid > front после коммита вложенных тразакций */
        db->md_mod_txnid = txn->mt_txnid;
        data.iov_base = db;
        WITH_CURSOR_TRACKING(
            couple.outer,
            rc = cursor_put_nochecklen(&couple.outer, &txn->mt_dbxs[i].md_name,
                                       &data, F_SUBDATA));
        if (unlikely(rc != MDBX_SUCCESS))
          goto fail;
      }
    }
  }

  ts_1 = latency ? osal_monotime() : 0;

  gcu_context_t gcu_ctx;
  gc_cputime = latency ? osal_cputime(nullptr) : 0;
  rc = gcu_context_init(txn, &gcu_ctx);
  if (unlikely(rc != MDBX_SUCCESS))
    goto fail;
  rc = update_gc(txn, &gcu_ctx);
  gc_cputime = latency ? osal_cputime(nullptr) - gc_cputime : 0;
  if (unlikely(rc != MDBX_SUCCESS))
    goto fail;

  tASSERT(txn, txn->tw.loose_count == 0);
  txn->mt_dbs[FREE_DBI].md_mod_txnid = (txn->mt_dbistate[FREE_DBI] & DBI_DIRTY)
                                           ? txn->mt_txnid
                                           : txn->mt_dbs[FREE_DBI].md_mod_txnid;

  txn->mt_dbs[MAIN_DBI].md_mod_txnid = (txn->mt_dbistate[MAIN_DBI] & DBI_DIRTY)
                                           ? txn->mt_txnid
                                           : txn->mt_dbs[MAIN_DBI].md_mod_txnid;

  ts_2 = latency ? osal_monotime() : 0;
  ts_3 = ts_2;
  if (AUDIT_ENABLED()) {
    rc = audit_ex(txn, MDBX_PNL_GETSIZE(txn->tw.retired_pages), true);
    ts_3 = osal_monotime();
    if (unlikely(rc != MDBX_SUCCESS))
      goto fail;
  }

  bool need_flush_for_nometasync = false;
  const meta_ptr_t head = meta_recent(env, &txn->tw.troika);
  const uint32_t meta_sync_txnid =
      atomic_load32(&env->me_lck->mti_meta_sync_txnid, mo_Relaxed);
  /* sync prev meta */
  if (head.is_steady && meta_sync_txnid != (uint32_t)head.txnid) {
    /* Исправление унаследованного от LMDB недочета:
     *
     * Всё хорошо, если все процессы работающие с БД не используют WRITEMAP.
     * Тогда мета-страница (обновленная, но не сброшенная на диск) будет
     * сохранена в результате fdatasync() при записи данных этой транзакции.
     *
     * Всё хорошо, если все процессы работающие с БД используют WRITEMAP
     * без MDBX_AVOID_MSYNC.
     * Тогда мета-страница (обновленная, но не сброшенная на диск) будет
     * сохранена в результате msync() при записи данных этой транзакции.
     *
     * Если же в процессах работающих с БД используется оба метода, как sync()
     * в режиме MDBX_WRITEMAP, так и записи через файловый дескриптор, то
     * становится невозможным обеспечить фиксацию на диске мета-страницы
     * предыдущей транзакции и данных текущей транзакции, за счет одной
     * sync-операцией выполняемой после записи данных текущей транзакции.
     * Соответственно, требуется явно обновлять мета-страницу, что полностью
     * уничтожает выгоду от NOMETASYNC. */
    const uint32_t txnid_dist =
        ((txn->mt_flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC)
            ? MDBX_NOMETASYNC_LAZY_FD
            : MDBX_NOMETASYNC_LAZY_WRITEMAP;
    /* Смысл "магии" в том, чтобы избежать отдельного вызова fdatasync()
     * или msync() для гарантированной фиксации на диске мета-страницы,
     * которая была "лениво" отправлена на запись в предыдущей транзакции,
     * но не сброшена на диск из-за активного режима MDBX_NOMETASYNC. */
    if (
#if defined(_WIN32) || defined(_WIN64)
        !env->me_overlapped_fd &&
#endif
        meta_sync_txnid == (uint32_t)head.txnid - txnid_dist)
      need_flush_for_nometasync = true;
    else {
      rc = meta_sync(env, head);
      if (unlikely(rc != MDBX_SUCCESS)) {
        ERROR("txn-%s: error %d", "presync-meta", rc);
        goto fail;
      }
    }
  }

  if (txn->tw.dirtylist) {
    tASSERT(txn, (txn->mt_flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);
    tASSERT(txn, txn->tw.loose_count == 0);

    mdbx_filehandle_t fd =
#if defined(_WIN32) || defined(_WIN64)
        env->me_overlapped_fd ? env->me_overlapped_fd : env->me_lazy_fd;
    (void)need_flush_for_nometasync;
#else
#define MDBX_WRITETHROUGH_THRESHOLD_DEFAULT 2
        (need_flush_for_nometasync ||
         env->me_dsync_fd == INVALID_HANDLE_VALUE ||
         txn->tw.dirtylist->length > env->me_options.writethrough_threshold ||
         atomic_load64(&env->me_lck->mti_unsynced_pages, mo_Relaxed))
            ? env->me_lazy_fd
            : env->me_dsync_fd;
#endif /* Windows */

    iov_ctx_t write_ctx;
    rc = iov_init(txn, &write_ctx, txn->tw.dirtylist->length,
                  txn->tw.dirtylist->pages_including_loose, fd, false);
    if (unlikely(rc != MDBX_SUCCESS)) {
      ERROR("txn-%s: error %d", "iov-init", rc);
      goto fail;
    }

    rc = txn_write(txn, &write_ctx);
    if (unlikely(rc != MDBX_SUCCESS)) {
      ERROR("txn-%s: error %d", "write", rc);
      goto fail;
    }
  } else {
    tASSERT(txn, (txn->mt_flags & MDBX_WRITEMAP) != 0 && !MDBX_AVOID_MSYNC);
    env->me_lck->mti_unsynced_pages.weak += txn->tw.writemap_dirty_npages;
    if (!env->me_lck->mti_eoos_timestamp.weak)
      env->me_lck->mti_eoos_timestamp.weak = osal_monotime();
  }

  /* TODO: use ctx.flush_begin & ctx.flush_end for range-sync */
  ts_4 = latency ? osal_monotime() : 0;

  MDBX_meta meta;
  memcpy(meta.mm_magic_and_version, head.ptr_c->mm_magic_and_version, 8);
  meta.mm_extra_flags = head.ptr_c->mm_extra_flags;
  meta.mm_validator_id = head.ptr_c->mm_validator_id;
  meta.mm_extra_pagehdr = head.ptr_c->mm_extra_pagehdr;
  unaligned_poke_u64(4, meta.mm_pages_retired,
                     unaligned_peek_u64(4, head.ptr_c->mm_pages_retired) +
                         MDBX_PNL_GETSIZE(txn->tw.retired_pages));
  meta.mm_geo = txn->mt_geo;
  meta.mm_dbs[FREE_DBI] = txn->mt_dbs[FREE_DBI];
  meta.mm_dbs[MAIN_DBI] = txn->mt_dbs[MAIN_DBI];
  meta.mm_canary = txn->mt_canary;

  txnid_t commit_txnid = txn->mt_txnid;
#if MDBX_ENABLE_BIGFOOT
  if (gcu_ctx.bigfoot > txn->mt_txnid) {
    commit_txnid = gcu_ctx.bigfoot;
    TRACE("use @%" PRIaTXN " (+%zu) for commit bigfoot-txn", commit_txnid,
          (size_t)(commit_txnid - txn->mt_txnid));
  }
#endif
  meta.unsafe_sign = MDBX_DATASIGN_NONE;
  meta_set_txnid(env, &meta, commit_txnid);

  rc = sync_locked(env, env->me_flags | txn->mt_flags | MDBX_SHRINK_ALLOWED,
                   &meta, &txn->tw.troika);

  ts_5 = latency ? osal_monotime() : 0;
  if (unlikely(rc != MDBX_SUCCESS)) {
    env->me_flags |= MDBX_FATAL_ERROR;
    ERROR("txn-%s: error %d", "sync", rc);
    goto fail;
  }

  end_mode = MDBX_END_COMMITTED | MDBX_END_UPDATE | MDBX_END_EOTDONE;

done:
  if (latency)
    take_gcprof(txn, latency);
  rc = txn_end(txn, end_mode);

provide_latency:
  if (latency) {
    latency->preparation = ts_1 ? osal_monotime_to_16dot16(ts_1 - ts_0) : 0;
    latency->gc_wallclock =
        (ts_2 > ts_1) ? osal_monotime_to_16dot16(ts_2 - ts_1) : 0;
    latency->gc_cputime = gc_cputime ? osal_monotime_to_16dot16(gc_cputime) : 0;
    latency->audit = (ts_3 > ts_2) ? osal_monotime_to_16dot16(ts_3 - ts_2) : 0;
    latency->write = (ts_4 > ts_3) ? osal_monotime_to_16dot16(ts_4 - ts_3) : 0;
    latency->sync = (ts_5 > ts_4) ? osal_monotime_to_16dot16(ts_5 - ts_4) : 0;
    const uint64_t ts_6 = osal_monotime();
    latency->ending = ts_5 ? osal_monotime_to_16dot16(ts_6 - ts_5) : 0;
    latency->whole = osal_monotime_to_16dot16_noUnderflow(ts_6 - ts_0);
  }
  return rc;

fail:
  txn->mt_flags |= MDBX_TXN_ERROR;
  if (latency)
    take_gcprof(txn, latency);
  mdbx_txn_abort(txn);
  goto provide_latency;
}

static __always_inline int cmp_int_inline(const size_t expected_alignment,
                                          const MDBX_val *a,
                                          const MDBX_val *b) {
  if (likely(a->iov_len == b->iov_len)) {
    if (sizeof(size_t) > 7 && likely(a->iov_len == 8))
      return CMP2INT(unaligned_peek_u64(expected_alignment, a->iov_base),
                     unaligned_peek_u64(expected_alignment, b->iov_base));
    if (likely(a->iov_len == 4))
      return CMP2INT(unaligned_peek_u32(expected_alignment, a->iov_base),
                     unaligned_peek_u32(expected_alignment, b->iov_base));
    if (sizeof(size_t) < 8 && likely(a->iov_len == 8))
      return CMP2INT(unaligned_peek_u64(expected_alignment, a->iov_base),
                     unaligned_peek_u64(expected_alignment, b->iov_base));
  }
  ERROR("mismatch and/or invalid size %p.%zu/%p.%zu for INTEGERKEY/INTEGERDUP",
        a->iov_base, a->iov_len, b->iov_base, b->iov_len);
  return 0;
}

__hot static int cmp_int_unaligned(const MDBX_val *a, const MDBX_val *b) {
  return cmp_int_inline(1, a, b);
}

/* Compare two items pointing at 2-byte aligned unsigned int's. */
#if MDBX_UNALIGNED_OK < 2 ||                                                   \
    (MDBX_DEBUG || MDBX_FORCE_ASSERTIONS || !defined(NDEBUG))
__hot static int cmp_int_align2(const MDBX_val *a, const MDBX_val *b) {
  return cmp_int_inline(2, a, b);
}
#else
#define cmp_int_align2 cmp_int_unaligned
#endif /* !MDBX_UNALIGNED_OK || debug */

/* Compare two items pointing at aligned unsigned int's. */
#if MDBX_UNALIGNED_OK < 4 ||                                                   \
    (MDBX_DEBUG || MDBX_FORCE_ASSERTIONS || !defined(NDEBUG))
__hot static int cmp_int_align4(const MDBX_val *a, const MDBX_val *b) {
  return cmp_int_inline(4, a, b);
}
#else
#define cmp_int_align4 cmp_int_unaligned
#endif /* !MDBX_UNALIGNED_OK || debug */

/* Compare two items lexically */
__hot static int cmp_lexical(const MDBX_val *a, const MDBX_val *b) {
  if (a->iov_len == b->iov_len)
    return a->iov_len ? memcmp(a->iov_base, b->iov_base, a->iov_len) : 0;

  const int diff_len = (a->iov_len < b->iov_len) ? -1 : 1;
  const size_t shortest = (a->iov_len < b->iov_len) ? a->iov_len : b->iov_len;
  int diff_data = shortest ? memcmp(a->iov_base, b->iov_base, shortest) : 0;
  return likely(diff_data) ? diff_data : diff_len;
}

MDBX_NOTHROW_PURE_FUNCTION static __always_inline unsigned
tail3le(const uint8_t *p, size_t l) {
  STATIC_ASSERT(sizeof(unsigned) > 2);
  // 1: 0 0 0
  // 2: 0 1 1
  // 3: 0 1 2
  return p[0] | p[l >> 1] << 8 | p[l - 1] << 16;
}

/* Compare two items in reverse byte order */
__hot static int cmp_reverse(const MDBX_val *a, const MDBX_val *b) {
  size_t left = (a->iov_len < b->iov_len) ? a->iov_len : b->iov_len;
  if (likely(left)) {
    const uint8_t *pa = ptr_disp(a->iov_base, a->iov_len);
    const uint8_t *pb = ptr_disp(b->iov_base, b->iov_len);
    while (left >= sizeof(size_t)) {
      pa -= sizeof(size_t);
      pb -= sizeof(size_t);
      left -= sizeof(size_t);
      STATIC_ASSERT(sizeof(size_t) == 4 || sizeof(size_t) == 8);
      if (sizeof(size_t) == 4) {
        uint32_t xa = unaligned_peek_u32(1, pa);
        uint32_t xb = unaligned_peek_u32(1, pb);
#if __BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__
        xa = osal_bswap32(xa);
        xb = osal_bswap32(xb);
#endif /* __BYTE_ORDER__ != __ORDER_BIG_ENDIAN__ */
        if (xa != xb)
          return (xa < xb) ? -1 : 1;
      } else {
        uint64_t xa = unaligned_peek_u64(1, pa);
        uint64_t xb = unaligned_peek_u64(1, pb);
#if __BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__
        xa = osal_bswap64(xa);
        xb = osal_bswap64(xb);
#endif /* __BYTE_ORDER__ != __ORDER_BIG_ENDIAN__ */
        if (xa != xb)
          return (xa < xb) ? -1 : 1;
      }
    }
    if (sizeof(size_t) == 8 && left >= 4) {
      pa -= 4;
      pb -= 4;
      left -= 4;
      uint32_t xa = unaligned_peek_u32(1, pa);
      uint32_t xb = unaligned_peek_u32(1, pb);
#if __BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__
      xa = osal_bswap32(xa);
      xb = osal_bswap32(xb);
#endif /* __BYTE_ORDER__ != __ORDER_BIG_ENDIAN__ */
      if (xa != xb)
        return (xa < xb) ? -1 : 1;
    }
    if (left) {
      unsigned xa = tail3le(pa - left, left);
      unsigned xb = tail3le(pb - left, left);
      if (xa != xb)
        return (xa < xb) ? -1 : 1;
    }
  }
  return CMP2INT(a->iov_len, b->iov_len);
}

/* Fast non-lexically comparator */
__hot static int cmp_lenfast(const MDBX_val *a, const MDBX_val *b) {
  int diff = CMP2INT(a->iov_len, b->iov_len);
  return (likely(diff) || a->iov_len == 0)
             ? diff
             : memcmp(a->iov_base, b->iov_base, a->iov_len);
}

__hot static bool eq_fast_slowpath(const uint8_t *a, const uint8_t *b,
                                   size_t l) {
  if (likely(l > 3)) {
    if (MDBX_UNALIGNED_OK >= 4 && likely(l < 9))
      return ((unaligned_peek_u32(1, a) - unaligned_peek_u32(1, b)) |
              (unaligned_peek_u32(1, a + l - 4) -
               unaligned_peek_u32(1, b + l - 4))) == 0;
    if (MDBX_UNALIGNED_OK >= 8 && sizeof(size_t) > 7 && likely(l < 17))
      return ((unaligned_peek_u64(1, a) - unaligned_peek_u64(1, b)) |
              (unaligned_peek_u64(1, a + l - 8) -
               unaligned_peek_u64(1, b + l - 8))) == 0;
    return memcmp(a, b, l) == 0;
  }
  if (likely(l))
    return tail3le(a, l) == tail3le(b, l);
  return true;
}

static __always_inline bool eq_fast(const MDBX_val *a, const MDBX_val *b) {
  return unlikely(a->iov_len == b->iov_len) &&
         eq_fast_slowpath(a->iov_base, b->iov_base, a->iov_len);
}

static int validate_meta(MDBX_env *env, MDBX_meta *const meta,
                         const MDBX_page *const page,
                         const unsigned meta_number, unsigned *guess_pagesize) {
  const uint64_t magic_and_version =
      unaligned_peek_u64(4, &meta->mm_magic_and_version);
  if (unlikely(magic_and_version != MDBX_DATA_MAGIC &&
               magic_and_version != MDBX_DATA_MAGIC_LEGACY_COMPAT &&
               magic_and_version != MDBX_DATA_MAGIC_LEGACY_DEVEL)) {
    ERROR("meta[%u] has invalid magic/version %" PRIx64, meta_number,
          magic_and_version);
    return ((magic_and_version >> 8) != MDBX_MAGIC) ? MDBX_INVALID
                                                    : MDBX_VERSION_MISMATCH;
  }

  if (unlikely(page->mp_pgno != meta_number)) {
    ERROR("meta[%u] has invalid pageno %" PRIaPGNO, meta_number, page->mp_pgno);
    return MDBX_INVALID;
  }

  if (unlikely(page->mp_flags != P_META)) {
    ERROR("page #%u not a meta-page", meta_number);
    return MDBX_INVALID;
  }

  /* LY: check pagesize */
  if (unlikely(!is_powerof2(meta->mm_psize) || meta->mm_psize < MIN_PAGESIZE ||
               meta->mm_psize > MAX_PAGESIZE)) {
    WARNING("meta[%u] has invalid pagesize (%u), skip it", meta_number,
            meta->mm_psize);
    return is_powerof2(meta->mm_psize) ? MDBX_VERSION_MISMATCH : MDBX_INVALID;
  }

  if (guess_pagesize && *guess_pagesize != meta->mm_psize) {
    *guess_pagesize = meta->mm_psize;
    VERBOSE("meta[%u] took pagesize %u", meta_number, meta->mm_psize);
  }

  const txnid_t txnid = unaligned_peek_u64(4, &meta->mm_txnid_a);
  if (unlikely(txnid != unaligned_peek_u64(4, &meta->mm_txnid_b))) {
    WARNING("meta[%u] not completely updated, skip it", meta_number);
    return MDBX_RESULT_TRUE;
  }

  if (unlikely(meta->mm_extra_flags != 0)) {
    WARNING("meta[%u] has unsupported %s 0x%x, skip it", meta_number,
            "extra-flags", meta->mm_extra_flags);
    return MDBX_RESULT_TRUE;
  }
  if (unlikely(meta->mm_validator_id != 0)) {
    WARNING("meta[%u] has unsupported %s 0x%x, skip it", meta_number,
            "validator-id", meta->mm_validator_id);
    return MDBX_RESULT_TRUE;
  }
  if (unlikely(meta->mm_extra_pagehdr != 0)) {
    WARNING("meta[%u] has unsupported %s 0x%x, skip it", meta_number,
            "extra-pageheader", meta->mm_extra_pagehdr);
    return MDBX_RESULT_TRUE;
  }

  /* LY: check signature as a checksum */
  if (META_IS_STEADY(meta) &&
      unlikely(unaligned_peek_u64(4, &meta->mm_sign) != meta_sign(meta))) {
    WARNING("meta[%u] has invalid steady-checksum (0x%" PRIx64 " != 0x%" PRIx64
            "), skip it",
            meta_number, unaligned_peek_u64(4, &meta->mm_sign),
            meta_sign(meta));
    return MDBX_RESULT_TRUE;
  }

  DEBUG("checking meta%" PRIaPGNO " = root %" PRIaPGNO "/%" PRIaPGNO
        ", geo %" PRIaPGNO "/%" PRIaPGNO "-%" PRIaPGNO "/%" PRIaPGNO
        " +%u -%u, txn_id %" PRIaTXN ", %s",
        page->mp_pgno, meta->mm_dbs[MAIN_DBI].md_root,
        meta->mm_dbs[FREE_DBI].md_root, meta->mm_geo.lower, meta->mm_geo.next,
        meta->mm_geo.now, meta->mm_geo.upper, pv2pages(meta->mm_geo.grow_pv),
        pv2pages(meta->mm_geo.shrink_pv), txnid, durable_caption(meta));

  if (unlikely(txnid < MIN_TXNID || txnid > MAX_TXNID)) {
    WARNING("meta[%u] has invalid txnid %" PRIaTXN ", skip it", meta_number,
            txnid);
    return MDBX_RESULT_TRUE;
  }

  /* LY: check min-pages value */
  if (unlikely(meta->mm_geo.lower < MIN_PAGENO ||
               meta->mm_geo.lower > MAX_PAGENO + 1)) {
    WARNING("meta[%u] has invalid min-pages (%" PRIaPGNO "), skip it",
            meta_number, meta->mm_geo.lower);
    return MDBX_INVALID;
  }

  /* LY: check max-pages value */
  if (unlikely(meta->mm_geo.upper < MIN_PAGENO ||
               meta->mm_geo.upper > MAX_PAGENO + 1 ||
               meta->mm_geo.upper < meta->mm_geo.lower)) {
    WARNING("meta[%u] has invalid max-pages (%" PRIaPGNO "), skip it",
            meta_number, meta->mm_geo.upper);
    return MDBX_INVALID;
  }

  /* LY: check last_pgno */
  if (unlikely(meta->mm_geo.next < MIN_PAGENO ||
               meta->mm_geo.next - 1 > MAX_PAGENO)) {
    WARNING("meta[%u] has invalid next-pageno (%" PRIaPGNO "), skip it",
            meta_number, meta->mm_geo.next);
    return MDBX_CORRUPTED;
  }

  /* LY: check filesize & used_bytes */
  const uint64_t used_bytes = meta->mm_geo.next * (uint64_t)meta->mm_psize;
  if (unlikely(used_bytes > env->me_dxb_mmap.filesize)) {
    /* Here could be a race with DB-shrinking performed by other process */
    int err = osal_filesize(env->me_lazy_fd, &env->me_dxb_mmap.filesize);
    if (unlikely(err != MDBX_SUCCESS))
      return err;
    if (unlikely(used_bytes > env->me_dxb_mmap.filesize)) {
      WARNING("meta[%u] used-bytes (%" PRIu64 ") beyond filesize (%" PRIu64
              "), skip it",
              meta_number, used_bytes, env->me_dxb_mmap.filesize);
      return MDBX_CORRUPTED;
    }
  }
  if (unlikely(meta->mm_geo.next - 1 > MAX_PAGENO ||
               used_bytes > MAX_MAPSIZE)) {
    WARNING("meta[%u] has too large used-space (%" PRIu64 "), skip it",
            meta_number, used_bytes);
    return MDBX_TOO_LARGE;
  }

  /* LY: check mapsize limits */
  pgno_t geo_lower = meta->mm_geo.lower;
  uint64_t mapsize_min = geo_lower * (uint64_t)meta->mm_psize;
  STATIC_ASSERT(MAX_MAPSIZE < PTRDIFF_MAX - MAX_PAGESIZE);
  STATIC_ASSERT(MIN_MAPSIZE < MAX_MAPSIZE);
  STATIC_ASSERT((uint64_t)(MAX_PAGENO + 1) * MIN_PAGESIZE % (4ul << 20) == 0);
  if (unlikely(mapsize_min < MIN_MAPSIZE || mapsize_min > MAX_MAPSIZE)) {
    if (MAX_MAPSIZE != MAX_MAPSIZE64 && mapsize_min > MAX_MAPSIZE &&
        mapsize_min <= MAX_MAPSIZE64) {
      eASSERT(env,
              meta->mm_geo.next - 1 <= MAX_PAGENO && used_bytes <= MAX_MAPSIZE);
      WARNING("meta[%u] has too large min-mapsize (%" PRIu64 "), "
              "but size of used space still acceptable (%" PRIu64 ")",
              meta_number, mapsize_min, used_bytes);
      geo_lower = (pgno_t)((mapsize_min = MAX_MAPSIZE) / meta->mm_psize);
      if (geo_lower > MAX_PAGENO + 1) {
        geo_lower = MAX_PAGENO + 1;
        mapsize_min = geo_lower * (uint64_t)meta->mm_psize;
      }
      WARNING("meta[%u] consider get-%s pageno is %" PRIaPGNO
              " instead of wrong %" PRIaPGNO
              ", will be corrected on next commit(s)",
              meta_number, "lower", geo_lower, meta->mm_geo.lower);
      meta->mm_geo.lower = geo_lower;
    } else {
      WARNING("meta[%u] has invalid min-mapsize (%" PRIu64 "), skip it",
              meta_number, mapsize_min);
      return MDBX_VERSION_MISMATCH;
    }
  }

  pgno_t geo_upper = meta->mm_geo.upper;
  uint64_t mapsize_max = geo_upper * (uint64_t)meta->mm_psize;
  STATIC_ASSERT(MIN_MAPSIZE < MAX_MAPSIZE);
  if (unlikely(mapsize_max > MAX_MAPSIZE ||
               (MAX_PAGENO + 1) <
                   ceil_powerof2((size_t)mapsize_max, env->me_os_psize) /
                       (size_t)meta->mm_psize)) {
    if (mapsize_max > MAX_MAPSIZE64) {
      WARNING("meta[%u] has invalid max-mapsize (%" PRIu64 "), skip it",
              meta_number, mapsize_max);
      return MDBX_VERSION_MISMATCH;
    }
    /* allow to open large DB from a 32-bit environment */
    eASSERT(env,
            meta->mm_geo.next - 1 <= MAX_PAGENO && used_bytes <= MAX_MAPSIZE);
    WARNING("meta[%u] has too large max-mapsize (%" PRIu64 "), "
            "but size of used space still acceptable (%" PRIu64 ")",
            meta_number, mapsize_max, used_bytes);
    geo_upper = (pgno_t)((mapsize_max = MAX_MAPSIZE) / meta->mm_psize);
    if (geo_upper > MAX_PAGENO + 1) {
      geo_upper = MAX_PAGENO + 1;
      mapsize_max = geo_upper * (uint64_t)meta->mm_psize;
    }
    WARNING("meta[%u] consider get-%s pageno is %" PRIaPGNO
            " instead of wrong %" PRIaPGNO
            ", will be corrected on next commit(s)",
            meta_number, "upper", geo_upper, meta->mm_geo.upper);
    meta->mm_geo.upper = geo_upper;
  }

  /* LY: check and silently put mm_geo.now into [geo.lower...geo.upper].
   *
   * Copy-with-compaction by previous version of libmdbx could produce DB-file
   * less than meta.geo.lower bound, in case actual filling is low or no data
   * at all. This is not a problem as there is no damage or loss of data.
   * Therefore it is better not to consider such situation as an error, but
   * silently correct it. */
  pgno_t geo_now = meta->mm_geo.now;
  if (geo_now < geo_lower)
    geo_now = geo_lower;
  if (geo_now > geo_upper && meta->mm_geo.next <= geo_upper)
    geo_now = geo_upper;

  if (unlikely(meta->mm_geo.next > geo_now)) {
    WARNING("meta[%u] next-pageno (%" PRIaPGNO
            ") is beyond end-pgno (%" PRIaPGNO "), skip it",
            meta_number, meta->mm_geo.next, geo_now);
    return MDBX_CORRUPTED;
  }
  if (meta->mm_geo.now != geo_now) {
    WARNING("meta[%u] consider geo-%s pageno is %" PRIaPGNO
            " instead of wrong %" PRIaPGNO
            ", will be corrected on next commit(s)",
            meta_number, "now", geo_now, meta->mm_geo.now);
    meta->mm_geo.now = geo_now;
  }

  /* GC */
  if (meta->mm_dbs[FREE_DBI].md_root == P_INVALID) {
    if (unlikely(meta->mm_dbs[FREE_DBI].md_branch_pages ||
                 meta->mm_dbs[FREE_DBI].md_depth ||
                 meta->mm_dbs[FREE_DBI].md_entries ||
                 meta->mm_dbs[FREE_DBI].md_leaf_pages ||
                 meta->mm_dbs[FREE_DBI].md_overflow_pages)) {
      WARNING("meta[%u] has false-empty %s, skip it", meta_number, "GC/FreeDB");
      return MDBX_CORRUPTED;
    }
  } else if (unlikely(meta->mm_dbs[FREE_DBI].md_root >= meta->mm_geo.next)) {
    WARNING("meta[%u] has invalid %s-root %" PRIaPGNO ", skip it", meta_number,
            "GC/FreeDB", meta->mm_dbs[FREE_DBI].md_root);
    return MDBX_CORRUPTED;
  }

  /* MainDB */
  if (meta->mm_dbs[MAIN_DBI].md_root == P_INVALID) {
    if (unlikely(meta->mm_dbs[MAIN_DBI].md_branch_pages ||
                 meta->mm_dbs[MAIN_DBI].md_depth ||
                 meta->mm_dbs[MAIN_DBI].md_entries ||
                 meta->mm_dbs[MAIN_DBI].md_leaf_pages ||
                 meta->mm_dbs[MAIN_DBI].md_overflow_pages)) {
      WARNING("meta[%u] has false-empty %s", meta_number, "MainDB");
      return MDBX_CORRUPTED;
    }
  } else if (unlikely(meta->mm_dbs[MAIN_DBI].md_root >= meta->mm_geo.next)) {
    WARNING("meta[%u] has invalid %s-root %" PRIaPGNO ", skip it", meta_number,
            "MainDB", meta->mm_dbs[MAIN_DBI].md_root);
    return MDBX_CORRUPTED;
  }

  if (unlikely(meta->mm_dbs[FREE_DBI].md_mod_txnid > txnid)) {
    WARNING("meta[%u] has wrong md_mod_txnid %" PRIaTXN " for %s, skip it",
            meta_number, meta->mm_dbs[FREE_DBI].md_mod_txnid, "GC/FreeDB");
    return MDBX_CORRUPTED;
  }

  if (unlikely(meta->mm_dbs[MAIN_DBI].md_mod_txnid > txnid)) {
    WARNING("meta[%u] has wrong md_mod_txnid %" PRIaTXN " for %s, skip it",
            meta_number, meta->mm_dbs[MAIN_DBI].md_mod_txnid, "MainDB");
    return MDBX_CORRUPTED;
  }

  if (unlikely((meta->mm_dbs[FREE_DBI].md_flags & DB_PERSISTENT_FLAGS) !=
               MDBX_INTEGERKEY)) {
    WARNING("meta[%u] has unexpected/invalid db-flags 0x%x for %s", meta_number,
            meta->mm_dbs[FREE_DBI].md_flags, "GC/FreeDB");
    return MDBX_INCOMPATIBLE;
  }

  return MDBX_SUCCESS;
}

static int validate_meta_copy(MDBX_env *env, const MDBX_meta *meta,
                              MDBX_meta *dest) {
  *dest = *meta;
  return validate_meta(env, dest, data_page(meta),
                       bytes2pgno(env, ptr_dist(meta, env->me_map)), nullptr);
}

/* Read the environment parameters of a DB environment
 * before mapping it into memory. */
__cold static int read_header(MDBX_env *env, MDBX_meta *dest,
                              const int lck_exclusive,
                              const mdbx_mode_t mode_bits) {
  memset(dest, 0, sizeof(MDBX_meta));
  int rc = osal_filesize(env->me_lazy_fd, &env->me_dxb_mmap.filesize);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  unaligned_poke_u64(4, dest->mm_sign, MDBX_DATASIGN_WEAK);
  rc = MDBX_CORRUPTED;

  /* Read twice all meta pages so we can find the latest one. */
  unsigned loop_limit = NUM_METAS * 2;
  /* We don't know the page size on first time. So, just guess it. */
  unsigned guess_pagesize = 0;
  for (unsigned loop_count = 0; loop_count < loop_limit; ++loop_count) {
    const unsigned meta_number = loop_count % NUM_METAS;
    const unsigned offset = (guess_pagesize             ? guess_pagesize
                             : (loop_count > NUM_METAS) ? env->me_psize
                                                        : env->me_os_psize) *
                            meta_number;

    char buffer[MIN_PAGESIZE];
    unsigned retryleft = 42;
    while (1) {
      TRACE("reading meta[%d]: offset %u, bytes %u, retry-left %u", meta_number,
            offset, MIN_PAGESIZE, retryleft);
      int err = osal_pread(env->me_lazy_fd, buffer, MIN_PAGESIZE, offset);
      if (err == MDBX_ENODATA && offset == 0 && loop_count == 0 &&
          env->me_dxb_mmap.filesize == 0 &&
          mode_bits /* non-zero for DB creation */ != 0) {
        NOTICE("read meta: empty file (%d, %s)", err, mdbx_strerror(err));
        return err;
      }
#if defined(_WIN32) || defined(_WIN64)
      if (err == ERROR_LOCK_VIOLATION) {
        SleepEx(0, true);
        err = osal_pread(env->me_lazy_fd, buffer, MIN_PAGESIZE, offset);
        if (err == ERROR_LOCK_VIOLATION && --retryleft) {
          WARNING("read meta[%u,%u]: %i, %s", offset, MIN_PAGESIZE, err,
                  mdbx_strerror(err));
          continue;
        }
      }
#endif /* Windows */
      if (err != MDBX_SUCCESS) {
        ERROR("read meta[%u,%u]: %i, %s", offset, MIN_PAGESIZE, err,
              mdbx_strerror(err));
        return err;
      }

      char again[MIN_PAGESIZE];
      err = osal_pread(env->me_lazy_fd, again, MIN_PAGESIZE, offset);
#if defined(_WIN32) || defined(_WIN64)
      if (err == ERROR_LOCK_VIOLATION) {
        SleepEx(0, true);
        err = osal_pread(env->me_lazy_fd, again, MIN_PAGESIZE, offset);
        if (err == ERROR_LOCK_VIOLATION && --retryleft) {
          WARNING("read meta[%u,%u]: %i, %s", offset, MIN_PAGESIZE, err,
                  mdbx_strerror(err));
          continue;
        }
      }
#endif /* Windows */
      if (err != MDBX_SUCCESS) {
        ERROR("read meta[%u,%u]: %i, %s", offset, MIN_PAGESIZE, err,
              mdbx_strerror(err));
        return err;
      }

      if (memcmp(buffer, again, MIN_PAGESIZE) == 0 || --retryleft == 0)
        break;

      VERBOSE("meta[%u] was updated, re-read it", meta_number);
    }

    if (!retryleft) {
      ERROR("meta[%u] is too volatile, skip it", meta_number);
      continue;
    }

    MDBX_page *const page = (MDBX_page *)buffer;
    MDBX_meta *const meta = page_meta(page);
    rc = validate_meta(env, meta, page, meta_number, &guess_pagesize);
    if (rc != MDBX_SUCCESS)
      continue;

    bool latch;
    if (env->me_stuck_meta >= 0)
      latch = (meta_number == (unsigned)env->me_stuck_meta);
    else if (meta_bootid_match(meta))
      latch = meta_choice_recent(
          meta->unsafe_txnid, SIGN_IS_STEADY(meta->unsafe_sign),
          dest->unsafe_txnid, SIGN_IS_STEADY(dest->unsafe_sign));
    else
      latch = meta_choice_steady(
          meta->unsafe_txnid, SIGN_IS_STEADY(meta->unsafe_sign),
          dest->unsafe_txnid, SIGN_IS_STEADY(dest->unsafe_sign));
    if (latch) {
      *dest = *meta;
      if (!lck_exclusive && !META_IS_STEADY(dest))
        loop_limit += 1; /* LY: should re-read to hush race with update */
      VERBOSE("latch meta[%u]", meta_number);
    }
  }

  if (dest->mm_psize == 0 ||
      (env->me_stuck_meta < 0 &&
       !(META_IS_STEADY(dest) ||
         meta_weak_acceptable(env, dest, lck_exclusive)))) {
    ERROR("%s", "no usable meta-pages, database is corrupted");
    if (rc == MDBX_SUCCESS) {
      /* TODO: try to restore the database by fully checking b-tree structure
       * for the each meta page, if the corresponding option was given */
      return MDBX_CORRUPTED;
    }
    return rc;
  }

  return MDBX_SUCCESS;
}

__cold static MDBX_page *meta_model(const MDBX_env *env, MDBX_page *model,
                                    size_t num) {
  ENSURE(env, is_powerof2(env->me_psize));
  ENSURE(env, env->me_psize >= MIN_PAGESIZE);
  ENSURE(env, env->me_psize <= MAX_PAGESIZE);
  ENSURE(env, env->me_dbgeo.lower >= MIN_MAPSIZE);
  ENSURE(env, env->me_dbgeo.upper <= MAX_MAPSIZE);
  ENSURE(env, env->me_dbgeo.now >= env->me_dbgeo.lower);
  ENSURE(env, env->me_dbgeo.now <= env->me_dbgeo.upper);

  memset(model, 0, env->me_psize);
  model->mp_pgno = (pgno_t)num;
  model->mp_flags = P_META;
  MDBX_meta *const model_meta = page_meta(model);
  unaligned_poke_u64(4, model_meta->mm_magic_and_version, MDBX_DATA_MAGIC);

  model_meta->mm_geo.lower = bytes2pgno(env, env->me_dbgeo.lower);
  model_meta->mm_geo.upper = bytes2pgno(env, env->me_dbgeo.upper);
  model_meta->mm_geo.grow_pv = pages2pv(bytes2pgno(env, env->me_dbgeo.grow));
  model_meta->mm_geo.shrink_pv =
      pages2pv(bytes2pgno(env, env->me_dbgeo.shrink));
  model_meta->mm_geo.now = bytes2pgno(env, env->me_dbgeo.now);
  model_meta->mm_geo.next = NUM_METAS;

  ENSURE(env, model_meta->mm_geo.lower >= MIN_PAGENO);
  ENSURE(env, model_meta->mm_geo.upper <= MAX_PAGENO + 1);
  ENSURE(env, model_meta->mm_geo.now >= model_meta->mm_geo.lower);
  ENSURE(env, model_meta->mm_geo.now <= model_meta->mm_geo.upper);
  ENSURE(env, model_meta->mm_geo.next >= MIN_PAGENO);
  ENSURE(env, model_meta->mm_geo.next <= model_meta->mm_geo.now);
  ENSURE(env, model_meta->mm_geo.grow_pv ==
                  pages2pv(pv2pages(model_meta->mm_geo.grow_pv)));
  ENSURE(env, model_meta->mm_geo.shrink_pv ==
                  pages2pv(pv2pages(model_meta->mm_geo.shrink_pv)));

  model_meta->mm_psize = env->me_psize;
  model_meta->mm_dbs[FREE_DBI].md_flags = MDBX_INTEGERKEY;
  model_meta->mm_dbs[FREE_DBI].md_root = P_INVALID;
  model_meta->mm_dbs[MAIN_DBI].md_root = P_INVALID;
  meta_set_txnid(env, model_meta, MIN_TXNID + num);
  unaligned_poke_u64(4, model_meta->mm_sign, meta_sign(model_meta));
  eASSERT(env, check_meta_coherency(env, model_meta, true));
  return ptr_disp(model, env->me_psize);
}

/* Fill in most of the zeroed meta-pages for an empty database environment.
 * Return pointer to recently (head) meta-page. */
__cold static MDBX_meta *init_metas(const MDBX_env *env, void *buffer) {
  MDBX_page *page0 = (MDBX_page *)buffer;
  MDBX_page *page1 = meta_model(env, page0, 0);
  MDBX_page *page2 = meta_model(env, page1, 1);
  meta_model(env, page2, 2);
  return page_meta(page2);
}

static int sync_locked(MDBX_env *env, unsigned flags, MDBX_meta *const pending,
                       meta_troika_t *const troika) {
  eASSERT(env, ((env->me_flags ^ flags) & MDBX_WRITEMAP) == 0);
  const MDBX_meta *const meta0 = METAPAGE(env, 0);
  const MDBX_meta *const meta1 = METAPAGE(env, 1);
  const MDBX_meta *const meta2 = METAPAGE(env, 2);
  const meta_ptr_t head = meta_recent(env, troika);
  int rc;

  eASSERT(env,
          pending < METAPAGE(env, 0) || pending > METAPAGE(env, NUM_METAS));
  eASSERT(env, (env->me_flags & (MDBX_RDONLY | MDBX_FATAL_ERROR)) == 0);
  eASSERT(env, pending->mm_geo.next <= pending->mm_geo.now);

  if (flags & MDBX_SAFE_NOSYNC) {
    /* Check auto-sync conditions */
    const pgno_t autosync_threshold =
        atomic_load32(&env->me_lck->mti_autosync_threshold, mo_Relaxed);
    const uint64_t autosync_period =
        atomic_load64(&env->me_lck->mti_autosync_period, mo_Relaxed);
    uint64_t eoos_timestamp;
    if ((autosync_threshold &&
         atomic_load64(&env->me_lck->mti_unsynced_pages, mo_Relaxed) >=
             autosync_threshold) ||
        (autosync_period &&
         (eoos_timestamp =
              atomic_load64(&env->me_lck->mti_eoos_timestamp, mo_Relaxed)) &&
         osal_monotime() - eoos_timestamp >= autosync_period))
      flags &= MDBX_WRITEMAP | MDBX_SHRINK_ALLOWED; /* force steady */
  }

  pgno_t shrink = 0;
  if (flags & MDBX_SHRINK_ALLOWED) {
    const size_t prev_discarded_pgno =
        atomic_load32(&env->me_lck->mti_discarded_tail, mo_Relaxed);
    if (prev_discarded_pgno < pending->mm_geo.next)
      env->me_lck->mti_discarded_tail.weak = pending->mm_geo.next;
    else if (prev_discarded_pgno >=
             pending->mm_geo.next + env->me_madv_threshold) {
      /* LY: check conditions to discard unused pages */
      const pgno_t largest_pgno = find_largest_snapshot(
          env, (head.ptr_c->mm_geo.next > pending->mm_geo.next)
                   ? head.ptr_c->mm_geo.next
                   : pending->mm_geo.next);
      eASSERT(env, largest_pgno >= NUM_METAS);

#if defined(MDBX_USE_VALGRIND) || defined(__SANITIZE_ADDRESS__)
      const pgno_t edge = env->me_poison_edge;
      if (edge > largest_pgno) {
        env->me_poison_edge = largest_pgno;
        VALGRIND_MAKE_MEM_NOACCESS(
            ptr_disp(env->me_map, pgno2bytes(env, largest_pgno)),
            pgno2bytes(env, edge - largest_pgno));
        MDBX_ASAN_POISON_MEMORY_REGION(
            ptr_disp(env->me_map, pgno2bytes(env, largest_pgno)),
            pgno2bytes(env, edge - largest_pgno));
      }
#endif /* MDBX_USE_VALGRIND || __SANITIZE_ADDRESS__ */

#if MDBX_ENABLE_MADVISE &&                                                     \
    (defined(MADV_DONTNEED) || defined(POSIX_MADV_DONTNEED))
      const size_t discard_edge_pgno = pgno_align2os_pgno(env, largest_pgno);
      if (prev_discarded_pgno >= discard_edge_pgno + env->me_madv_threshold) {
        const size_t prev_discarded_bytes =
            pgno_align2os_bytes(env, prev_discarded_pgno);
        const size_t discard_edge_bytes = pgno2bytes(env, discard_edge_pgno);
        /* из-за выравнивания prev_discarded_bytes и discard_edge_bytes
         * могут быть равны */
        if (prev_discarded_bytes > discard_edge_bytes) {
          NOTICE("shrink-MADV_%s %zu..%zu", "DONTNEED", discard_edge_pgno,
                 prev_discarded_pgno);
          munlock_after(env, discard_edge_pgno,
                        bytes_align2os_bytes(env, env->me_dxb_mmap.current));
          const uint32_t munlocks_before =
              atomic_load32(&env->me_lck->mti_mlcnt[1], mo_Relaxed);
#if defined(MADV_DONTNEED)
          int advise = MADV_DONTNEED;
#if defined(MADV_FREE) &&                                                      \
    0 /* MADV_FREE works for only anonymous vma at the moment */
          if ((env->me_flags & MDBX_WRITEMAP) &&
              linux_kernel_version > 0x04050000)
            advise = MADV_FREE;
#endif /* MADV_FREE */
          int err = madvise(ptr_disp(env->me_map, discard_edge_bytes),
                            prev_discarded_bytes - discard_edge_bytes, advise)
                        ? ignore_enosys(errno)
                        : MDBX_SUCCESS;
#else
          int err = ignore_enosys(posix_madvise(
              ptr_disp(env->me_map, discard_edge_bytes),
              prev_discarded_bytes - discard_edge_bytes, POSIX_MADV_DONTNEED));
#endif
          if (unlikely(MDBX_IS_ERROR(err))) {
            const uint32_t mlocks_after =
                atomic_load32(&env->me_lck->mti_mlcnt[0], mo_Relaxed);
            if (err == MDBX_EINVAL) {
              const int severity = (mlocks_after - munlocks_before)
                                       ? MDBX_LOG_NOTICE
                                       : MDBX_LOG_WARN;
              if (LOG_ENABLED(severity))
                debug_log(
                    severity, __func__, __LINE__,
                    "%s-madvise: ignore EINVAL (%d) since some pages maybe "
                    "locked (%u/%u mlcnt-processes)",
                    "shrink", err, mlocks_after, munlocks_before);
            } else {
              ERROR("%s-madvise(%s, %zu, +%zu), %u/%u mlcnt-processes, err %d",
                    "shrink", "DONTNEED", discard_edge_bytes,
                    prev_discarded_bytes - discard_edge_bytes, mlocks_after,
                    munlocks_before, err);
              return err;
            }
          } else
            env->me_lck->mti_discarded_tail.weak = discard_edge_pgno;
        }
      }
#endif /* MDBX_ENABLE_MADVISE && (MADV_DONTNEED || POSIX_MADV_DONTNEED) */

      /* LY: check conditions to shrink datafile */
      const pgno_t backlog_gap = 3 + pending->mm_dbs[FREE_DBI].md_depth * 3;
      pgno_t shrink_step = 0;
      if (pending->mm_geo.shrink_pv &&
          pending->mm_geo.now - pending->mm_geo.next >
              (shrink_step = pv2pages(pending->mm_geo.shrink_pv)) +
                  backlog_gap) {
        if (pending->mm_geo.now > largest_pgno &&
            pending->mm_geo.now - largest_pgno > shrink_step + backlog_gap) {
          const pgno_t aligner =
              pending->mm_geo.grow_pv
                  ? /* grow_step */ pv2pages(pending->mm_geo.grow_pv)
                  : shrink_step;
          const pgno_t with_backlog_gap = largest_pgno + backlog_gap;
          const pgno_t aligned =
              pgno_align2os_pgno(env, (size_t)with_backlog_gap + aligner -
                                          with_backlog_gap % aligner);
          const pgno_t bottom = (aligned > pending->mm_geo.lower)
                                    ? aligned
                                    : pending->mm_geo.lower;
          if (pending->mm_geo.now > bottom) {
            if (TROIKA_HAVE_STEADY(troika))
              /* force steady, but only if steady-checkpoint is present */
              flags &= MDBX_WRITEMAP | MDBX_SHRINK_ALLOWED;
            shrink = pending->mm_geo.now - bottom;
            pending->mm_geo.now = bottom;
            if (unlikely(head.txnid == pending->unsafe_txnid)) {
              const txnid_t txnid = safe64_txnid_next(pending->unsafe_txnid);
              NOTICE("force-forward pending-txn %" PRIaTXN " -> %" PRIaTXN,
                     pending->unsafe_txnid, txnid);
              ENSURE(env, !env->me_txn0 ||
                              (env->me_txn0->mt_owner != osal_thread_self() &&
                               !env->me_txn));
              if (unlikely(txnid > MAX_TXNID)) {
                rc = MDBX_TXN_FULL;
                ERROR("txnid overflow, raise %d", rc);
                goto fail;
              }
              meta_set_txnid(env, pending, txnid);
              eASSERT(env, check_meta_coherency(env, pending, true));
            }
          }
        }
      }
    }
  }

  /* LY: step#1 - sync previously written/updated data-pages */
  rc = MDBX_RESULT_FALSE /* carry steady */;
  if (atomic_load64(&env->me_lck->mti_unsynced_pages, mo_Relaxed)) {
    eASSERT(env, ((flags ^ env->me_flags) & MDBX_WRITEMAP) == 0);
    enum osal_syncmode_bits mode_bits = MDBX_SYNC_NONE;
    unsigned sync_op = 0;
    if ((flags & MDBX_SAFE_NOSYNC) == 0) {
      sync_op = 1;
      mode_bits = MDBX_SYNC_DATA;
      if (pending->mm_geo.next >
          meta_prefer_steady(env, troika).ptr_c->mm_geo.now)
        mode_bits |= MDBX_SYNC_SIZE;
      if (flags & MDBX_NOMETASYNC)
        mode_bits |= MDBX_SYNC_IODQ;
    } else if (unlikely(env->me_incore))
      goto skip_incore_sync;
    if (flags & MDBX_WRITEMAP) {
#if MDBX_ENABLE_PGOP_STAT
      env->me_lck->mti_pgop_stat.msync.weak += sync_op;
#else
      (void)sync_op;
#endif /* MDBX_ENABLE_PGOP_STAT */
      rc =
          osal_msync(&env->me_dxb_mmap, 0,
                     pgno_align2os_bytes(env, pending->mm_geo.next), mode_bits);
    } else {
#if MDBX_ENABLE_PGOP_STAT
      env->me_lck->mti_pgop_stat.fsync.weak += sync_op;
#else
      (void)sync_op;
#endif /* MDBX_ENABLE_PGOP_STAT */
      rc = osal_fsync(env->me_lazy_fd, mode_bits);
    }
    if (unlikely(rc != MDBX_SUCCESS))
      goto fail;
    rc = (flags & MDBX_SAFE_NOSYNC) ? MDBX_RESULT_TRUE /* carry non-steady */
                                    : MDBX_RESULT_FALSE /* carry steady */;
  }
  eASSERT(env, check_meta_coherency(env, pending, true));

  /* Steady or Weak */
  if (rc == MDBX_RESULT_FALSE /* carry steady */) {
    unaligned_poke_u64(4, pending->mm_sign, meta_sign(pending));
    atomic_store64(&env->me_lck->mti_eoos_timestamp, 0, mo_Relaxed);
    atomic_store64(&env->me_lck->mti_unsynced_pages, 0, mo_Relaxed);
  } else {
    assert(rc == MDBX_RESULT_TRUE /* carry non-steady */);
  skip_incore_sync:
    eASSERT(env, env->me_lck->mti_unsynced_pages.weak > 0);
    /* Может быть нулевым если unsynced_pages > 0 в результате спиллинга.
     * eASSERT(env, env->me_lck->mti_eoos_timestamp.weak != 0); */
    unaligned_poke_u64(4, pending->mm_sign, MDBX_DATASIGN_WEAK);
  }

  const bool legal4overwrite =
      head.txnid == pending->unsafe_txnid &&
      memcmp(&head.ptr_c->mm_dbs, &pending->mm_dbs, sizeof(pending->mm_dbs)) ==
          0 &&
      memcmp(&head.ptr_c->mm_canary, &pending->mm_canary,
             sizeof(pending->mm_canary)) == 0 &&
      memcmp(&head.ptr_c->mm_geo, &pending->mm_geo, sizeof(pending->mm_geo)) ==
          0;
  MDBX_meta *target = nullptr;
  if (head.txnid == pending->unsafe_txnid) {
    ENSURE(env, legal4overwrite);
    if (!head.is_steady && META_IS_STEADY(pending))
      target = (MDBX_meta *)head.ptr_c;
    else {
      WARNING("%s", "skip update meta");
      return MDBX_SUCCESS;
    }
  } else {
    const unsigned troika_tail = troika->tail_and_flags & 3;
    ENSURE(env, troika_tail < NUM_METAS && troika_tail != troika->recent &&
                    troika_tail != troika->prefer_steady);
    target = (MDBX_meta *)meta_tail(env, troika).ptr_c;
  }

  /* LY: step#2 - update meta-page. */
  DEBUG("writing meta%" PRIaPGNO " = root %" PRIaPGNO "/%" PRIaPGNO
        ", geo %" PRIaPGNO "/%" PRIaPGNO "-%" PRIaPGNO "/%" PRIaPGNO
        " +%u -%u, txn_id %" PRIaTXN ", %s",
        data_page(target)->mp_pgno, pending->mm_dbs[MAIN_DBI].md_root,
        pending->mm_dbs[FREE_DBI].md_root, pending->mm_geo.lower,
        pending->mm_geo.next, pending->mm_geo.now, pending->mm_geo.upper,
        pv2pages(pending->mm_geo.grow_pv), pv2pages(pending->mm_geo.shrink_pv),
        pending->unsafe_txnid, durable_caption(pending));

  DEBUG("meta0: %s, %s, txn_id %" PRIaTXN ", root %" PRIaPGNO "/%" PRIaPGNO,
        (meta0 == head.ptr_c) ? "head"
        : (meta0 == target)   ? "tail"
                              : "stay",
        durable_caption(meta0), constmeta_txnid(meta0),
        meta0->mm_dbs[MAIN_DBI].md_root, meta0->mm_dbs[FREE_DBI].md_root);
  DEBUG("meta1: %s, %s, txn_id %" PRIaTXN ", root %" PRIaPGNO "/%" PRIaPGNO,
        (meta1 == head.ptr_c) ? "head"
        : (meta1 == target)   ? "tail"
                              : "stay",
        durable_caption(meta1), constmeta_txnid(meta1),
        meta1->mm_dbs[MAIN_DBI].md_root, meta1->mm_dbs[FREE_DBI].md_root);
  DEBUG("meta2: %s, %s, txn_id %" PRIaTXN ", root %" PRIaPGNO "/%" PRIaPGNO,
        (meta2 == head.ptr_c) ? "head"
        : (meta2 == target)   ? "tail"
                              : "stay",
        durable_caption(meta2), constmeta_txnid(meta2),
        meta2->mm_dbs[MAIN_DBI].md_root, meta2->mm_dbs[FREE_DBI].md_root);

  eASSERT(env, pending->unsafe_txnid != constmeta_txnid(meta0) ||
                   (META_IS_STEADY(pending) && !META_IS_STEADY(meta0)));
  eASSERT(env, pending->unsafe_txnid != constmeta_txnid(meta1) ||
                   (META_IS_STEADY(pending) && !META_IS_STEADY(meta1)));
  eASSERT(env, pending->unsafe_txnid != constmeta_txnid(meta2) ||
                   (META_IS_STEADY(pending) && !META_IS_STEADY(meta2)));

  eASSERT(env, ((env->me_flags ^ flags) & MDBX_WRITEMAP) == 0);
  ENSURE(env, target == head.ptr_c ||
                  constmeta_txnid(target) < pending->unsafe_txnid);
  if (flags & MDBX_WRITEMAP) {
    jitter4testing(true);
    if (likely(target != head.ptr_c)) {
      /* LY: 'invalidate' the meta. */
      meta_update_begin(env, target, pending->unsafe_txnid);
      unaligned_poke_u64(4, target->mm_sign, MDBX_DATASIGN_WEAK);
#ifndef NDEBUG
      /* debug: provoke failure to catch a violators, but don't touch mm_psize
       * to allow readers catch actual pagesize. */
      void *provoke_begin = &target->mm_dbs[FREE_DBI].md_root;
      void *provoke_end = &target->mm_sign;
      memset(provoke_begin, 0xCC, ptr_dist(provoke_end, provoke_begin));
      jitter4testing(false);
#endif

      /* LY: update info */
      target->mm_geo = pending->mm_geo;
      target->mm_dbs[FREE_DBI] = pending->mm_dbs[FREE_DBI];
      target->mm_dbs[MAIN_DBI] = pending->mm_dbs[MAIN_DBI];
      target->mm_canary = pending->mm_canary;
      memcpy(target->mm_pages_retired, pending->mm_pages_retired, 8);
      jitter4testing(true);

      /* LY: 'commit' the meta */
      meta_update_end(env, target, unaligned_peek_u64(4, pending->mm_txnid_b));
      jitter4testing(true);
      eASSERT(env, check_meta_coherency(env, target, true));
    } else {
      /* dangerous case (target == head), only mm_sign could
       * me updated, check assertions once again */
      eASSERT(env,
              legal4overwrite && !head.is_steady && META_IS_STEADY(pending));
    }
    memcpy(target->mm_sign, pending->mm_sign, 8);
    osal_flush_incoherent_cpu_writeback();
    jitter4testing(true);
    if (!env->me_incore) {
      if (!MDBX_AVOID_MSYNC) {
        /* sync meta-pages */
#if MDBX_ENABLE_PGOP_STAT
        env->me_lck->mti_pgop_stat.msync.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
        rc = osal_msync(
            &env->me_dxb_mmap, 0, pgno_align2os_bytes(env, NUM_METAS),
            (flags & MDBX_NOMETASYNC) ? MDBX_SYNC_NONE
                                      : MDBX_SYNC_DATA | MDBX_SYNC_IODQ);
      } else {
#if MDBX_ENABLE_PGOP_STAT
        env->me_lck->mti_pgop_stat.wops.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
        const MDBX_page *page = data_page(target);
        rc = osal_pwrite(env->me_fd4meta, page, env->me_psize,
                         ptr_dist(page, env->me_map));
        if (likely(rc == MDBX_SUCCESS)) {
          osal_flush_incoherent_mmap(target, sizeof(MDBX_meta),
                                     env->me_os_psize);
          if ((flags & MDBX_NOMETASYNC) == 0 &&
              env->me_fd4meta == env->me_lazy_fd) {
#if MDBX_ENABLE_PGOP_STAT
            env->me_lck->mti_pgop_stat.fsync.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
            rc = osal_fsync(env->me_lazy_fd, MDBX_SYNC_DATA | MDBX_SYNC_IODQ);
          }
        }
      }
      if (unlikely(rc != MDBX_SUCCESS))
        goto fail;
    }
  } else {
#if MDBX_ENABLE_PGOP_STAT
    env->me_lck->mti_pgop_stat.wops.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
    const MDBX_meta undo_meta = *target;
    rc = osal_pwrite(env->me_fd4meta, pending, sizeof(MDBX_meta),
                     ptr_dist(target, env->me_map));
    if (unlikely(rc != MDBX_SUCCESS)) {
    undo:
      DEBUG("%s", "write failed, disk error?");
      /* On a failure, the pagecache still contains the new data.
       * Try write some old data back, to prevent it from being used. */
      osal_pwrite(env->me_fd4meta, &undo_meta, sizeof(MDBX_meta),
                  ptr_dist(target, env->me_map));
      goto fail;
    }
    osal_flush_incoherent_mmap(target, sizeof(MDBX_meta), env->me_os_psize);
    /* sync meta-pages */
    if ((flags & MDBX_NOMETASYNC) == 0 && env->me_fd4meta == env->me_lazy_fd &&
        !env->me_incore) {
#if MDBX_ENABLE_PGOP_STAT
      env->me_lck->mti_pgop_stat.fsync.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
      rc = osal_fsync(env->me_lazy_fd, MDBX_SYNC_DATA | MDBX_SYNC_IODQ);
      if (rc != MDBX_SUCCESS)
        goto undo;
    }
  }

  uint64_t timestamp = 0;
  while ("workaround for https://libmdbx.dqdkfa.ru/dead-github/issues/269") {
    rc = coherency_check_written(env, pending->unsafe_txnid, target,
                                 bytes2pgno(env, ptr_dist(target, env->me_map)),
                                 &timestamp);
    if (likely(rc == MDBX_SUCCESS))
      break;
    if (unlikely(rc != MDBX_RESULT_TRUE))
      goto fail;
  }

  const uint32_t sync_txnid_dist =
      ((flags & MDBX_NOMETASYNC) == 0) ? 0
      : ((flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC)
          ? MDBX_NOMETASYNC_LAZY_FD
          : MDBX_NOMETASYNC_LAZY_WRITEMAP;
  env->me_lck->mti_meta_sync_txnid.weak =
      pending->mm_txnid_a[__BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__].weak -
      sync_txnid_dist;

  *troika = meta_tap(env);
  for (MDBX_txn *txn = env->me_txn0; txn; txn = txn->mt_child)
    if (troika != &txn->tw.troika)
      txn->tw.troika = *troika;

  /* LY: shrink datafile if needed */
  if (unlikely(shrink)) {
    VERBOSE("shrink to %" PRIaPGNO " pages (-%" PRIaPGNO ")",
            pending->mm_geo.now, shrink);
    rc = dxb_resize(env, pending->mm_geo.next, pending->mm_geo.now,
                    pending->mm_geo.upper, impilict_shrink);
    if (rc != MDBX_SUCCESS && rc != MDBX_EPERM)
      goto fail;
    eASSERT(env, check_meta_coherency(env, target, true));
  }

  MDBX_lockinfo *const lck = env->me_lck_mmap.lck;
  if (likely(lck))
    /* toggle oldest refresh */
    atomic_store32(&lck->mti_readers_refresh_flag, false, mo_Relaxed);

  return MDBX_SUCCESS;

fail:
  env->me_flags |= MDBX_FATAL_ERROR;
  return rc;
}

static void recalculate_merge_threshold(MDBX_env *env) {
  const size_t bytes = page_space(env);
  env->me_merge_threshold =
      (uint16_t)(bytes -
                 (bytes * env->me_options.merge_threshold_16dot16_percent >>
                  16));
  env->me_merge_threshold_gc =
      (uint16_t)(bytes -
                 ((env->me_options.merge_threshold_16dot16_percent > 19005)
                      ? bytes / 3 /* 33 % */
                      : bytes / 4 /* 25 % */));
}

__cold static void setup_pagesize(MDBX_env *env, const size_t pagesize) {
  STATIC_ASSERT(PTRDIFF_MAX > MAX_MAPSIZE);
  STATIC_ASSERT(MIN_PAGESIZE > sizeof(MDBX_page) + sizeof(MDBX_meta));
  ENSURE(env, is_powerof2(pagesize));
  ENSURE(env, pagesize >= MIN_PAGESIZE);
  ENSURE(env, pagesize <= MAX_PAGESIZE);
  env->me_psize = (unsigned)pagesize;
  if (env->me_pbuf) {
    osal_memalign_free(env->me_pbuf);
    env->me_pbuf = nullptr;
  }

  STATIC_ASSERT(MAX_GC1OVPAGE(MIN_PAGESIZE) > 4);
  STATIC_ASSERT(MAX_GC1OVPAGE(MAX_PAGESIZE) < MDBX_PGL_LIMIT);
  const intptr_t maxgc_ov1page = (pagesize - PAGEHDRSZ) / sizeof(pgno_t) - 1;
  ENSURE(env,
         maxgc_ov1page > 42 && maxgc_ov1page < (intptr_t)MDBX_PGL_LIMIT / 4);
  env->me_maxgc_ov1page = (unsigned)maxgc_ov1page;
  env->me_maxgc_per_branch =
      (unsigned)((pagesize - PAGEHDRSZ) /
                 (sizeof(indx_t) + sizeof(MDBX_node) + sizeof(txnid_t)));

  STATIC_ASSERT(LEAF_NODE_MAX(MIN_PAGESIZE) > sizeof(MDBX_db) + NODESIZE + 42);
  STATIC_ASSERT(LEAF_NODE_MAX(MAX_PAGESIZE) < UINT16_MAX);
  STATIC_ASSERT(LEAF_NODE_MAX(MIN_PAGESIZE) >= BRANCH_NODE_MAX(MIN_PAGESIZE));
  STATIC_ASSERT(BRANCH_NODE_MAX(MAX_PAGESIZE) > NODESIZE + 42);
  STATIC_ASSERT(BRANCH_NODE_MAX(MAX_PAGESIZE) < UINT16_MAX);
  const intptr_t branch_nodemax = BRANCH_NODE_MAX(pagesize);
  const intptr_t leaf_nodemax = LEAF_NODE_MAX(pagesize);
  ENSURE(env, branch_nodemax > (intptr_t)(NODESIZE + 42) &&
                  branch_nodemax % 2 == 0 &&
                  leaf_nodemax > (intptr_t)(sizeof(MDBX_db) + NODESIZE + 42) &&
                  leaf_nodemax >= branch_nodemax &&
                  leaf_nodemax < (int)UINT16_MAX && leaf_nodemax % 2 == 0);
  env->me_leaf_nodemax = (uint16_t)leaf_nodemax;
  env->me_branch_nodemax = (uint16_t)branch_nodemax;
  env->me_psize2log = (uint8_t)log2n_powerof2(pagesize);
  eASSERT(env, pgno2bytes(env, 1) == pagesize);
  eASSERT(env, bytes2pgno(env, pagesize + pagesize) == 2);
  recalculate_merge_threshold(env);

  /* TODO: recalculate me_subpage_xyz values from MDBX_opt_subpage_xyz.  */
  env->me_subpage_limit = env->me_leaf_nodemax - NODESIZE;
  env->me_subpage_room_threshold = 0;
  env->me_subpage_reserve_prereq = env->me_leaf_nodemax;
  env->me_subpage_reserve_limit = env->me_subpage_limit / 42;
  eASSERT(env,
          env->me_subpage_reserve_prereq >
              env->me_subpage_room_threshold + env->me_subpage_reserve_limit);
  eASSERT(env, env->me_leaf_nodemax >= env->me_subpage_limit + NODESIZE);

  const pgno_t max_pgno = bytes2pgno(env, MAX_MAPSIZE);
  if (!env->me_options.flags.non_auto.dp_limit) {
    /* auto-setup dp_limit by "The42" ;-) */
    intptr_t total_ram_pages, avail_ram_pages;
    int err = mdbx_get_sysraminfo(nullptr, &total_ram_pages, &avail_ram_pages);
    if (unlikely(err != MDBX_SUCCESS))
      ERROR("mdbx_get_sysraminfo(), rc %d", err);
    else {
      size_t reasonable_dpl_limit =
          (size_t)(total_ram_pages + avail_ram_pages) / 42;
      if (pagesize > env->me_os_psize)
        reasonable_dpl_limit /= pagesize / env->me_os_psize;
      else if (pagesize < env->me_os_psize)
        reasonable_dpl_limit *= env->me_os_psize / pagesize;
      reasonable_dpl_limit = (reasonable_dpl_limit < MDBX_PGL_LIMIT)
                                 ? reasonable_dpl_limit
                                 : MDBX_PGL_LIMIT;
      reasonable_dpl_limit = (reasonable_dpl_limit > CURSOR_STACK * 4)
                                 ? reasonable_dpl_limit
                                 : CURSOR_STACK * 4;
      env->me_options.dp_limit = (unsigned)reasonable_dpl_limit;
    }
  }
  if (env->me_options.dp_limit > max_pgno - NUM_METAS)
    env->me_options.dp_limit = max_pgno - NUM_METAS;
  if (env->me_options.dp_initial > env->me_options.dp_limit)
    env->me_options.dp_initial = env->me_options.dp_limit;
}

__cold int mdbx_env_create(MDBX_env **penv) {
  if (unlikely(!penv))
    return MDBX_EINVAL;
  *penv = nullptr;

#ifdef MDBX_HAVE_C11ATOMICS
  if (unlikely(!atomic_is_lock_free((const volatile uint32_t *)penv))) {
    ERROR("lock-free atomic ops for %u-bit types is required", 32);
    return MDBX_INCOMPATIBLE;
  }
#if MDBX_64BIT_ATOMIC
  if (unlikely(!atomic_is_lock_free((const volatile uint64_t *)penv))) {
    ERROR("lock-free atomic ops for %u-bit types is required", 64);
    return MDBX_INCOMPATIBLE;
  }
#endif /* MDBX_64BIT_ATOMIC */
#endif /* MDBX_HAVE_C11ATOMICS */

  const size_t os_psize = osal_syspagesize();
  if (unlikely(!is_powerof2(os_psize) || os_psize < MIN_PAGESIZE)) {
    ERROR("unsuitable system pagesize %" PRIuPTR, os_psize);
    return MDBX_INCOMPATIBLE;
  }

#if defined(__linux__) || defined(__gnu_linux__)
  if (unlikely(linux_kernel_version < 0x04000000)) {
    /* 2022-09-01: Прошло уже больше двух после окончания какой-либо поддержки
     * самого "долгоиграющего" ядра 3.16.85 ветки 3.x */
    ERROR("too old linux kernel %u.%u.%u.%u, the >= 4.0.0 is required",
          linux_kernel_version >> 24, (linux_kernel_version >> 16) & 255,
          (linux_kernel_version >> 8) & 255, linux_kernel_version & 255);
    return MDBX_INCOMPATIBLE;
  }
#endif /* Linux */

  MDBX_env *env = osal_calloc(1, sizeof(MDBX_env));
  if (unlikely(!env))
    return MDBX_ENOMEM;

  env->me_maxreaders = DEFAULT_READERS;
  env->me_maxdbs = env->me_numdbs = CORE_DBS;
  env->me_lazy_fd = env->me_dsync_fd = env->me_fd4meta = env->me_lfd =
      INVALID_HANDLE_VALUE;
  env->me_pid = osal_getpid();
  env->me_stuck_meta = -1;

  env->me_options.rp_augment_limit = MDBX_PNL_INITIAL;
  env->me_options.dp_reserve_limit = MDBX_PNL_INITIAL;
  env->me_options.dp_initial = MDBX_PNL_INITIAL;
  env->me_options.spill_max_denominator = 8;
  env->me_options.spill_min_denominator = 8;
  env->me_options.spill_parent4child_denominator = 0;
  env->me_options.dp_loose_limit = 64;
  env->me_options.merge_threshold_16dot16_percent = 65536 / 4 /* 25% */;

#if !(defined(_WIN32) || defined(_WIN64))
  env->me_options.writethrough_threshold =
#if defined(__linux__) || defined(__gnu_linux__)
      mdbx_RunningOnWSL1 ? MAX_PAGENO :
#endif /* Linux */
                         MDBX_WRITETHROUGH_THRESHOLD_DEFAULT;
#endif /* Windows */

  env->me_os_psize = (unsigned)os_psize;
  setup_pagesize(env, (env->me_os_psize < MAX_PAGESIZE) ? env->me_os_psize
                                                        : MAX_PAGESIZE);

  int rc = osal_fastmutex_init(&env->me_dbi_lock);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout;

#if defined(_WIN32) || defined(_WIN64)
  osal_srwlock_Init(&env->me_remap_guard);
  InitializeCriticalSection(&env->me_windowsbug_lock);
#else
  rc = osal_fastmutex_init(&env->me_remap_guard);
  if (unlikely(rc != MDBX_SUCCESS)) {
    osal_fastmutex_destroy(&env->me_dbi_lock);
    goto bailout;
  }

#if MDBX_LOCKING > MDBX_LOCKING_SYSV
  MDBX_lockinfo *const stub = lckless_stub(env);
  rc = osal_ipclock_stub(&stub->mti_wlock);
#endif /* MDBX_LOCKING */
  if (unlikely(rc != MDBX_SUCCESS)) {
    osal_fastmutex_destroy(&env->me_remap_guard);
    osal_fastmutex_destroy(&env->me_dbi_lock);
    goto bailout;
  }
#endif /* Windows */

  VALGRIND_CREATE_MEMPOOL(env, 0, 0);
  env->me_signature.weak = MDBX_ME_SIGNATURE;
  *penv = env;
  return MDBX_SUCCESS;

bailout:
  osal_free(env);
  return rc;
}

__cold static intptr_t get_reasonable_db_maxsize(intptr_t *cached_result) {
  if (*cached_result == 0) {
    intptr_t pagesize, total_ram_pages;
    if (unlikely(mdbx_get_sysraminfo(&pagesize, &total_ram_pages, nullptr) !=
                 MDBX_SUCCESS))
      return *cached_result = MAX_MAPSIZE32 /* the 32-bit limit is good enough
                                               for fallback */
          ;

    if (unlikely((size_t)total_ram_pages * 2 > MAX_MAPSIZE / (size_t)pagesize))
      return *cached_result = MAX_MAPSIZE;
    assert(MAX_MAPSIZE >= (size_t)(total_ram_pages * pagesize * 2));

    /* Suggesting should not be more than golden ratio of the size of RAM. */
    *cached_result = (intptr_t)((size_t)total_ram_pages * 207 >> 7) * pagesize;

    /* Round to the nearest human-readable granulation. */
    for (size_t unit = MEGABYTE; unit; unit <<= 5) {
      const size_t floor = floor_powerof2(*cached_result, unit);
      const size_t ceil = ceil_powerof2(*cached_result, unit);
      const size_t threshold = (size_t)*cached_result >> 4;
      const bool down =
          *cached_result - floor < ceil - *cached_result || ceil > MAX_MAPSIZE;
      if (threshold < (down ? *cached_result - floor : ceil - *cached_result))
        break;
      *cached_result = down ? floor : ceil;
    }
  }
  return *cached_result;
}

__cold int mdbx_env_set_geometry(MDBX_env *env, intptr_t size_lower,
                                 intptr_t size_now, intptr_t size_upper,
                                 intptr_t growth_step,
                                 intptr_t shrink_threshold, intptr_t pagesize) {
  int rc = check_env(env, false);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  const bool inside_txn =
      (env->me_txn0 && env->me_txn0->mt_owner == osal_thread_self());

#if MDBX_DEBUG
  if (growth_step < 0) {
    growth_step = 1;
    if (shrink_threshold < 0)
      shrink_threshold = 1;
  }
#endif /* MDBX_DEBUG */

  intptr_t reasonable_maxsize = 0;
  bool need_unlock = false;
  if (env->me_map) {
    /* env already mapped */
    if (unlikely(env->me_flags & MDBX_RDONLY))
      return MDBX_EACCESS;

    if (!inside_txn) {
      int err = mdbx_txn_lock(env, false);
      if (unlikely(err != MDBX_SUCCESS))
        return err;
      need_unlock = true;
      env->me_txn0->tw.troika = meta_tap(env);
      eASSERT(env, !env->me_txn && !env->me_txn0->mt_child);
      env->me_txn0->mt_txnid =
          env->me_txn0->tw.troika.txnid[env->me_txn0->tw.troika.recent];
      txn_oldest_reader(env->me_txn0);
    }

    /* get untouched params from current TXN or DB */
    if (pagesize <= 0 || pagesize >= INT_MAX)
      pagesize = env->me_psize;
    const MDBX_geo *const geo =
        inside_txn ? &env->me_txn->mt_geo
                   : &meta_recent(env, &env->me_txn0->tw.troika).ptr_c->mm_geo;
    if (size_lower < 0)
      size_lower = pgno2bytes(env, geo->lower);
    if (size_now < 0)
      size_now = pgno2bytes(env, geo->now);
    if (size_upper < 0)
      size_upper = pgno2bytes(env, geo->upper);
    if (growth_step < 0)
      growth_step = pgno2bytes(env, pv2pages(geo->grow_pv));
    if (shrink_threshold < 0)
      shrink_threshold = pgno2bytes(env, pv2pages(geo->shrink_pv));

    if (pagesize != (intptr_t)env->me_psize) {
      rc = MDBX_EINVAL;
      goto bailout;
    }
    const size_t usedbytes =
        pgno2bytes(env, find_largest_snapshot(env, geo->next));
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

    /* is requested some auto-value for pagesize ? */
    if (pagesize >= INT_MAX /* maximal */)
      pagesize = MAX_PAGESIZE;
    else if (pagesize <= 0) {
      if (pagesize < 0 /* default */) {
        pagesize = env->me_os_psize;
        if ((uintptr_t)pagesize > MAX_PAGESIZE)
          pagesize = MAX_PAGESIZE;
        eASSERT(env, (uintptr_t)pagesize >= MIN_PAGESIZE);
      } else if (pagesize == 0 /* minimal */)
        pagesize = MIN_PAGESIZE;

      /* choose pagesize */
      intptr_t max_size = (size_now > size_lower) ? size_now : size_lower;
      max_size = (size_upper > max_size) ? size_upper : max_size;
      if (max_size < 0 /* default */)
        max_size = DEFAULT_MAPSIZE;
      else if (max_size == 0 /* minimal */)
        max_size = MIN_MAPSIZE;
      else if (max_size >= (intptr_t)MAX_MAPSIZE /* maximal */)
        max_size = get_reasonable_db_maxsize(&reasonable_maxsize);

      while (max_size > pagesize * (int64_t)(MAX_PAGENO + 1) &&
             pagesize < MAX_PAGESIZE)
        pagesize <<= 1;
    }
  }

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
  if (size_lower >= INTPTR_MAX) {
    size_lower = get_reasonable_db_maxsize(&reasonable_maxsize);
    if ((size_t)size_lower / pagesize > MAX_PAGENO + 1)
      size_lower = pagesize * (MAX_PAGENO + 1);
  }

  if (size_now <= 0) {
    size_now = size_lower;
    if (size_upper >= size_lower && size_now > size_upper)
      size_now = size_upper;
  }
  if (size_now >= INTPTR_MAX) {
    size_now = get_reasonable_db_maxsize(&reasonable_maxsize);
    if ((size_t)size_now / pagesize > MAX_PAGENO + 1)
      size_now = pagesize * (MAX_PAGENO + 1);
  }

  if (size_upper <= 0) {
    if (size_now >= get_reasonable_db_maxsize(&reasonable_maxsize) / 2)
      size_upper = get_reasonable_db_maxsize(&reasonable_maxsize);
    else if (MAX_MAPSIZE != MAX_MAPSIZE32 &&
             (size_t)size_now >= MAX_MAPSIZE32 / 2 &&
             (size_t)size_now <= MAX_MAPSIZE32 / 4 * 3)
      size_upper = MAX_MAPSIZE32;
    else {
      size_upper = size_now + size_now;
      if ((size_t)size_upper < DEFAULT_MAPSIZE * 2)
        size_upper = DEFAULT_MAPSIZE * 2;
    }
    if ((size_t)size_upper / pagesize > (MAX_PAGENO + 1))
      size_upper = pagesize * (MAX_PAGENO + 1);
  } else if (size_upper >= INTPTR_MAX) {
    size_upper = get_reasonable_db_maxsize(&reasonable_maxsize);
    if ((size_t)size_upper / pagesize > MAX_PAGENO + 1)
      size_upper = pagesize * (MAX_PAGENO + 1);
  }

  if (unlikely(size_lower < (intptr_t)MIN_MAPSIZE || size_lower > size_upper)) {
    rc = MDBX_EINVAL;
    goto bailout;
  }

  if ((uint64_t)size_lower / pagesize < MIN_PAGENO) {
    size_lower = pagesize * MIN_PAGENO;
    if (unlikely(size_lower > size_upper)) {
      rc = MDBX_EINVAL;
      goto bailout;
    }
    if (size_now < size_lower)
      size_now = size_lower;
  }

  if (unlikely((size_t)size_upper > MAX_MAPSIZE ||
               (uint64_t)size_upper / pagesize > MAX_PAGENO + 1)) {
    rc = MDBX_TOO_LARGE;
    goto bailout;
  }

  const size_t unit = (env->me_os_psize > (size_t)pagesize) ? env->me_os_psize
                                                            : (size_t)pagesize;
  size_lower = ceil_powerof2(size_lower, unit);
  size_upper = ceil_powerof2(size_upper, unit);
  size_now = ceil_powerof2(size_now, unit);

  /* LY: подбираем значение size_upper:
   *  - кратное размеру страницы
   *  - без нарушения MAX_MAPSIZE и MAX_PAGENO */
  while (unlikely((size_t)size_upper > MAX_MAPSIZE ||
                  (uint64_t)size_upper / pagesize > MAX_PAGENO + 1)) {
    if ((size_t)size_upper < unit + MIN_MAPSIZE ||
        (size_t)size_upper < (size_t)pagesize * (MIN_PAGENO + 1)) {
      /* паранойа на случай переполнения при невероятных значениях */
      rc = MDBX_EINVAL;
      goto bailout;
    }
    size_upper -= unit;
    if ((size_t)size_upper < (size_t)size_lower)
      size_lower = size_upper;
  }
  eASSERT(env, (size_upper - size_lower) % env->me_os_psize == 0);

  if (size_now < size_lower)
    size_now = size_lower;
  if (size_now > size_upper)
    size_now = size_upper;

  if (growth_step < 0) {
    growth_step = ((size_t)(size_upper - size_lower)) / 42;
    if (growth_step > size_lower && size_lower < (intptr_t)MEGABYTE)
      growth_step = size_lower;
    if (growth_step < 65536)
      growth_step = 65536;
    if ((size_t)growth_step > MAX_MAPSIZE / 64)
      growth_step = MAX_MAPSIZE / 64;
  }
  if (growth_step == 0 && shrink_threshold > 0)
    growth_step = 1;
  growth_step = ceil_powerof2(growth_step, unit);

  if (shrink_threshold < 0)
    shrink_threshold = growth_step + growth_step;
  shrink_threshold = ceil_powerof2(shrink_threshold, unit);

  //----------------------------------------------------------------------------

  if (!env->me_map) {
    /* save user's geo-params for future open/create */
    if (pagesize != (intptr_t)env->me_psize)
      setup_pagesize(env, pagesize);
    env->me_dbgeo.lower = size_lower;
    env->me_dbgeo.now = size_now;
    env->me_dbgeo.upper = size_upper;
    env->me_dbgeo.grow =
        pgno2bytes(env, pv2pages(pages2pv(bytes2pgno(env, growth_step))));
    env->me_dbgeo.shrink =
        pgno2bytes(env, pv2pages(pages2pv(bytes2pgno(env, shrink_threshold))));
    adjust_defaults(env);

    ENSURE(env, env->me_dbgeo.lower >= MIN_MAPSIZE);
    ENSURE(env, env->me_dbgeo.lower / (unsigned)pagesize >= MIN_PAGENO);
    ENSURE(env, env->me_dbgeo.lower % (unsigned)pagesize == 0);
    ENSURE(env, env->me_dbgeo.lower % env->me_os_psize == 0);

    ENSURE(env, env->me_dbgeo.upper <= MAX_MAPSIZE);
    ENSURE(env, env->me_dbgeo.upper / (unsigned)pagesize <= MAX_PAGENO + 1);
    ENSURE(env, env->me_dbgeo.upper % (unsigned)pagesize == 0);
    ENSURE(env, env->me_dbgeo.upper % env->me_os_psize == 0);

    ENSURE(env, env->me_dbgeo.now >= env->me_dbgeo.lower);
    ENSURE(env, env->me_dbgeo.now <= env->me_dbgeo.upper);
    ENSURE(env, env->me_dbgeo.now % (unsigned)pagesize == 0);
    ENSURE(env, env->me_dbgeo.now % env->me_os_psize == 0);

    ENSURE(env, env->me_dbgeo.grow % (unsigned)pagesize == 0);
    ENSURE(env, env->me_dbgeo.grow % env->me_os_psize == 0);
    ENSURE(env, env->me_dbgeo.shrink % (unsigned)pagesize == 0);
    ENSURE(env, env->me_dbgeo.shrink % env->me_os_psize == 0);

    rc = MDBX_SUCCESS;
  } else {
    /* apply new params to opened environment */
    ENSURE(env, pagesize == (intptr_t)env->me_psize);
    MDBX_meta meta;
    memset(&meta, 0, sizeof(meta));
    if (!inside_txn) {
      eASSERT(env, need_unlock);
      const meta_ptr_t head = meta_recent(env, &env->me_txn0->tw.troika);

      uint64_t timestamp = 0;
      while ("workaround for "
             "https://libmdbx.dqdkfa.ru/dead-github/issues/269") {
        rc = coherency_check_head(env->me_txn0, head, &timestamp);
        if (likely(rc == MDBX_SUCCESS))
          break;
        if (unlikely(rc != MDBX_RESULT_TRUE))
          goto bailout;
      }
      meta = *head.ptr_c;
      const txnid_t txnid = safe64_txnid_next(head.txnid);
      if (unlikely(txnid > MAX_TXNID)) {
        rc = MDBX_TXN_FULL;
        ERROR("txnid overflow, raise %d", rc);
        goto bailout;
      }
      meta_set_txnid(env, &meta, txnid);
    }

    const MDBX_geo *const current_geo =
        &(env->me_txn ? env->me_txn : env->me_txn0)->mt_geo;
    /* update env-geo to avoid influences */
    env->me_dbgeo.now = pgno2bytes(env, current_geo->now);
    env->me_dbgeo.lower = pgno2bytes(env, current_geo->lower);
    env->me_dbgeo.upper = pgno2bytes(env, current_geo->upper);
    env->me_dbgeo.grow = pgno2bytes(env, pv2pages(current_geo->grow_pv));
    env->me_dbgeo.shrink = pgno2bytes(env, pv2pages(current_geo->shrink_pv));

    MDBX_geo new_geo;
    new_geo.lower = bytes2pgno(env, size_lower);
    new_geo.now = bytes2pgno(env, size_now);
    new_geo.upper = bytes2pgno(env, size_upper);
    new_geo.grow_pv = pages2pv(bytes2pgno(env, growth_step));
    new_geo.shrink_pv = pages2pv(bytes2pgno(env, shrink_threshold));
    new_geo.next = current_geo->next;

    ENSURE(env, pgno_align2os_bytes(env, new_geo.lower) == (size_t)size_lower);
    ENSURE(env, pgno_align2os_bytes(env, new_geo.upper) == (size_t)size_upper);
    ENSURE(env, pgno_align2os_bytes(env, new_geo.now) == (size_t)size_now);
    ENSURE(env, new_geo.grow_pv == pages2pv(pv2pages(new_geo.grow_pv)));
    ENSURE(env, new_geo.shrink_pv == pages2pv(pv2pages(new_geo.shrink_pv)));

    ENSURE(env, (size_t)size_lower >= MIN_MAPSIZE);
    ENSURE(env, new_geo.lower >= MIN_PAGENO);
    ENSURE(env, (size_t)size_upper <= MAX_MAPSIZE);
    ENSURE(env, new_geo.upper <= MAX_PAGENO + 1);
    ENSURE(env, new_geo.now >= new_geo.next);
    ENSURE(env, new_geo.upper >= new_geo.now);
    ENSURE(env, new_geo.now >= new_geo.lower);

    if (memcmp(current_geo, &new_geo, sizeof(MDBX_geo)) != 0) {
#if defined(_WIN32) || defined(_WIN64)
      /* Was DB shrinking disabled before and now it will be enabled? */
      if (new_geo.lower < new_geo.upper && new_geo.shrink_pv &&
          !(current_geo->lower < current_geo->upper &&
            current_geo->shrink_pv)) {
        if (!env->me_lck_mmap.lck) {
          rc = MDBX_EPERM;
          goto bailout;
        }
        int err = osal_rdt_lock(env);
        if (unlikely(MDBX_IS_ERROR(err))) {
          rc = err;
          goto bailout;
        }

        /* Check if there are any reading threads that do not use the SRWL */
        const size_t CurrentTid = GetCurrentThreadId();
        const MDBX_reader *const begin = env->me_lck_mmap.lck->mti_readers;
        const MDBX_reader *const end =
            begin + atomic_load32(&env->me_lck_mmap.lck->mti_numreaders,
                                  mo_AcquireRelease);
        for (const MDBX_reader *reader = begin; reader < end; ++reader) {
          if (reader->mr_pid.weak == env->me_pid && reader->mr_tid.weak &&
              reader->mr_tid.weak != CurrentTid) {
            /* At least one thread may don't use SRWL */
            rc = MDBX_EPERM;
            break;
          }
        }

        osal_rdt_unlock(env);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
      }
#endif

      if (new_geo.now != current_geo->now ||
          new_geo.upper != current_geo->upper) {
        rc = dxb_resize(env, current_geo->next, new_geo.now, new_geo.upper,
                        explicit_resize);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
      }
      if (inside_txn) {
        env->me_txn->mt_geo = new_geo;
        env->me_txn->mt_flags |= MDBX_TXN_DIRTY;
      } else {
        meta.mm_geo = new_geo;
        rc = sync_locked(env, env->me_flags, &meta, &env->me_txn0->tw.troika);
        if (likely(rc == MDBX_SUCCESS)) {
          env->me_dbgeo.now = pgno2bytes(env, new_geo.now = meta.mm_geo.now);
          env->me_dbgeo.upper =
              pgno2bytes(env, new_geo.upper = meta.mm_geo.upper);
        }
      }
    }
    if (likely(rc == MDBX_SUCCESS)) {
      /* update env-geo to avoid influences */
      eASSERT(env, env->me_dbgeo.now == pgno2bytes(env, new_geo.now));
      env->me_dbgeo.lower = pgno2bytes(env, new_geo.lower);
      eASSERT(env, env->me_dbgeo.upper == pgno2bytes(env, new_geo.upper));
      env->me_dbgeo.grow = pgno2bytes(env, pv2pages(new_geo.grow_pv));
      env->me_dbgeo.shrink = pgno2bytes(env, pv2pages(new_geo.shrink_pv));
    }
  }

bailout:
  if (need_unlock)
    mdbx_txn_unlock(env);
  return rc;
}

__cold static int alloc_page_buf(MDBX_env *env) {
  return env->me_pbuf ? MDBX_SUCCESS
                      : osal_memalign_alloc(env->me_os_psize,
                                            env->me_psize * (size_t)NUM_METAS,
                                            &env->me_pbuf);
}

/* Further setup required for opening an MDBX environment */
__cold static int setup_dxb(MDBX_env *env, const int lck_rc,
                            const mdbx_mode_t mode_bits) {
  MDBX_meta header;
  int rc = MDBX_RESULT_FALSE;
  int err = read_header(env, &header, lck_rc, mode_bits);
  if (unlikely(err != MDBX_SUCCESS)) {
    if (lck_rc != /* lck exclusive */ MDBX_RESULT_TRUE || err != MDBX_ENODATA ||
        (env->me_flags & MDBX_RDONLY) != 0 ||
        /* recovery mode */ env->me_stuck_meta >= 0)
      return err;

    DEBUG("%s", "create new database");
    rc = /* new database */ MDBX_RESULT_TRUE;

    if (!env->me_dbgeo.now) {
      /* set defaults if not configured */
      err = mdbx_env_set_geometry(env, 0, -1, DEFAULT_MAPSIZE, -1, -1, -1);
      if (unlikely(err != MDBX_SUCCESS))
        return err;
    }

    err = alloc_page_buf(env);
    if (unlikely(err != MDBX_SUCCESS))
      return err;

    header = *init_metas(env, env->me_pbuf);
    err = osal_pwrite(env->me_lazy_fd, env->me_pbuf,
                      env->me_psize * (size_t)NUM_METAS, 0);
    if (unlikely(err != MDBX_SUCCESS))
      return err;

    err = osal_ftruncate(env->me_lazy_fd, env->me_dxb_mmap.filesize =
                                              env->me_dxb_mmap.current =
                                                  env->me_dbgeo.now);
    if (unlikely(err != MDBX_SUCCESS))
      return err;

#ifndef NDEBUG /* just for checking */
    err = read_header(env, &header, lck_rc, mode_bits);
    if (unlikely(err != MDBX_SUCCESS))
      return err;
#endif
  }

  VERBOSE("header: root %" PRIaPGNO "/%" PRIaPGNO ", geo %" PRIaPGNO
          "/%" PRIaPGNO "-%" PRIaPGNO "/%" PRIaPGNO " +%u -%u, txn_id %" PRIaTXN
          ", %s",
          header.mm_dbs[MAIN_DBI].md_root, header.mm_dbs[FREE_DBI].md_root,
          header.mm_geo.lower, header.mm_geo.next, header.mm_geo.now,
          header.mm_geo.upper, pv2pages(header.mm_geo.grow_pv),
          pv2pages(header.mm_geo.shrink_pv),
          unaligned_peek_u64(4, header.mm_txnid_a), durable_caption(&header));

  if (env->me_psize != header.mm_psize)
    setup_pagesize(env, header.mm_psize);
  const size_t used_bytes = pgno2bytes(env, header.mm_geo.next);
  const size_t used_aligned2os_bytes =
      ceil_powerof2(used_bytes, env->me_os_psize);
  if ((env->me_flags & MDBX_RDONLY) /* readonly */
      || lck_rc != MDBX_RESULT_TRUE /* not exclusive */
      || /* recovery mode */ env->me_stuck_meta >= 0) {
    /* use present params from db */
    const size_t pagesize = header.mm_psize;
    err = mdbx_env_set_geometry(
        env, header.mm_geo.lower * pagesize, header.mm_geo.now * pagesize,
        header.mm_geo.upper * pagesize,
        pv2pages(header.mm_geo.grow_pv) * pagesize,
        pv2pages(header.mm_geo.shrink_pv) * pagesize, header.mm_psize);
    if (unlikely(err != MDBX_SUCCESS)) {
      ERROR("%s: err %d", "could not apply geometry from db", err);
      return (err == MDBX_EINVAL) ? MDBX_INCOMPATIBLE : err;
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
            pgno2bytes(env, header.mm_geo.upper) ||
        bytes_align2os_bytes(env, env->me_dbgeo.lower) !=
            pgno2bytes(env, header.mm_geo.lower) ||
        bytes_align2os_bytes(env, env->me_dbgeo.shrink) !=
            pgno2bytes(env, pv2pages(header.mm_geo.shrink_pv)) ||
        bytes_align2os_bytes(env, env->me_dbgeo.grow) !=
            pgno2bytes(env, pv2pages(header.mm_geo.grow_pv))) {

      if (env->me_dbgeo.shrink && env->me_dbgeo.now > used_bytes)
        /* pre-shrink if enabled */
        env->me_dbgeo.now = used_bytes + env->me_dbgeo.shrink -
                            used_bytes % env->me_dbgeo.shrink;

      err = mdbx_env_set_geometry(env, env->me_dbgeo.lower, env->me_dbgeo.now,
                                  env->me_dbgeo.upper, env->me_dbgeo.grow,
                                  env->me_dbgeo.shrink, header.mm_psize);
      if (unlikely(err != MDBX_SUCCESS)) {
        ERROR("%s: err %d", "could not apply preconfigured db-geometry", err);
        return (err == MDBX_EINVAL) ? MDBX_INCOMPATIBLE : err;
      }

      /* update meta fields */
      header.mm_geo.now = bytes2pgno(env, env->me_dbgeo.now);
      header.mm_geo.lower = bytes2pgno(env, env->me_dbgeo.lower);
      header.mm_geo.upper = bytes2pgno(env, env->me_dbgeo.upper);
      header.mm_geo.grow_pv = pages2pv(bytes2pgno(env, env->me_dbgeo.grow));
      header.mm_geo.shrink_pv = pages2pv(bytes2pgno(env, env->me_dbgeo.shrink));

      VERBOSE("amended: root %" PRIaPGNO "/%" PRIaPGNO ", geo %" PRIaPGNO
              "/%" PRIaPGNO "-%" PRIaPGNO "/%" PRIaPGNO
              " +%u -%u, txn_id %" PRIaTXN ", %s",
              header.mm_dbs[MAIN_DBI].md_root, header.mm_dbs[FREE_DBI].md_root,
              header.mm_geo.lower, header.mm_geo.next, header.mm_geo.now,
              header.mm_geo.upper, pv2pages(header.mm_geo.grow_pv),
              pv2pages(header.mm_geo.shrink_pv),
              unaligned_peek_u64(4, header.mm_txnid_a),
              durable_caption(&header));
    } else {
      /* fetch back 'now/current' size, since it was ignored during comparison
       * and may differ. */
      env->me_dbgeo.now = pgno_align2os_bytes(env, header.mm_geo.now);
    }
    ENSURE(env, header.mm_geo.now >= header.mm_geo.next);
  } else {
    /* geo-params are not pre-configured by user,
     * get current values from the meta. */
    env->me_dbgeo.now = pgno2bytes(env, header.mm_geo.now);
    env->me_dbgeo.lower = pgno2bytes(env, header.mm_geo.lower);
    env->me_dbgeo.upper = pgno2bytes(env, header.mm_geo.upper);
    env->me_dbgeo.grow = pgno2bytes(env, pv2pages(header.mm_geo.grow_pv));
    env->me_dbgeo.shrink = pgno2bytes(env, pv2pages(header.mm_geo.shrink_pv));
  }

  ENSURE(env, pgno_align2os_bytes(env, header.mm_geo.now) == env->me_dbgeo.now);
  ENSURE(env, env->me_dbgeo.now >= used_bytes);
  const uint64_t filesize_before = env->me_dxb_mmap.filesize;
  if (unlikely(filesize_before != env->me_dbgeo.now)) {
    if (lck_rc != /* lck exclusive */ MDBX_RESULT_TRUE) {
      VERBOSE("filesize mismatch (expect %" PRIuPTR "b/%" PRIaPGNO
              "p, have %" PRIu64 "b/%" PRIaPGNO "p), "
              "assume other process working",
              env->me_dbgeo.now, bytes2pgno(env, env->me_dbgeo.now),
              filesize_before, bytes2pgno(env, (size_t)filesize_before));
    } else {
      WARNING("filesize mismatch (expect %" PRIuSIZE "b/%" PRIaPGNO
              "p, have %" PRIu64 "b/%" PRIaPGNO "p)",
              env->me_dbgeo.now, bytes2pgno(env, env->me_dbgeo.now),
              filesize_before, bytes2pgno(env, (size_t)filesize_before));
      if (filesize_before < used_bytes) {
        ERROR("last-page beyond end-of-file (last %" PRIaPGNO
              ", have %" PRIaPGNO ")",
              header.mm_geo.next, bytes2pgno(env, (size_t)filesize_before));
        return MDBX_CORRUPTED;
      }

      if (env->me_flags & MDBX_RDONLY) {
        if (filesize_before & (env->me_os_psize - 1)) {
          ERROR("%s", "filesize should be rounded-up to system page");
          return MDBX_WANNA_RECOVERY;
        }
        WARNING("%s", "ignore filesize mismatch in readonly-mode");
      } else {
        VERBOSE("will resize datafile to %" PRIuSIZE " bytes, %" PRIaPGNO
                " pages",
                env->me_dbgeo.now, bytes2pgno(env, env->me_dbgeo.now));
      }
    }
  }

  VERBOSE("current boot-id %" PRIx64 "-%" PRIx64 " (%savailable)", bootid.x,
          bootid.y, (bootid.x | bootid.y) ? "" : "not-");

#if MDBX_ENABLE_MADVISE
  /* calculate readahead hint before mmap with zero redundant pages */
  const bool readahead =
      !(env->me_flags & MDBX_NORDAHEAD) &&
      mdbx_is_readahead_reasonable(used_bytes, 0) == MDBX_RESULT_TRUE;
#endif /* MDBX_ENABLE_MADVISE */

  err = osal_mmap(
      env->me_flags, &env->me_dxb_mmap, env->me_dbgeo.now, env->me_dbgeo.upper,
      (lck_rc && env->me_stuck_meta < 0) ? MMAP_OPTION_TRUNCATE : 0);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

#if MDBX_ENABLE_MADVISE
#if defined(MADV_DONTDUMP)
  err = madvise(env->me_map, env->me_dxb_mmap.limit, MADV_DONTDUMP)
            ? ignore_enosys(errno)
            : MDBX_SUCCESS;
  if (unlikely(MDBX_IS_ERROR(err)))
    return err;
#endif /* MADV_DONTDUMP */
#if defined(MADV_DODUMP)
  if (runtime_flags & MDBX_DBG_DUMP) {
    const size_t meta_length_aligned2os = pgno_align2os_bytes(env, NUM_METAS);
    err = madvise(env->me_map, meta_length_aligned2os, MADV_DODUMP)
              ? ignore_enosys(errno)
              : MDBX_SUCCESS;
    if (unlikely(MDBX_IS_ERROR(err)))
      return err;
  }
#endif /* MADV_DODUMP */
#endif /* MDBX_ENABLE_MADVISE */

#ifdef MDBX_USE_VALGRIND
  env->me_valgrind_handle =
      VALGRIND_CREATE_BLOCK(env->me_map, env->me_dxb_mmap.limit, "mdbx");
#endif /* MDBX_USE_VALGRIND */

  eASSERT(env, used_bytes >= pgno2bytes(env, NUM_METAS) &&
                   used_bytes <= env->me_dxb_mmap.limit);
#if defined(MDBX_USE_VALGRIND) || defined(__SANITIZE_ADDRESS__)
  if (env->me_dxb_mmap.filesize > used_bytes &&
      env->me_dxb_mmap.filesize < env->me_dxb_mmap.limit) {
    VALGRIND_MAKE_MEM_NOACCESS(ptr_disp(env->me_map, used_bytes),
                               env->me_dxb_mmap.filesize - used_bytes);
    MDBX_ASAN_POISON_MEMORY_REGION(ptr_disp(env->me_map, used_bytes),
                                   env->me_dxb_mmap.filesize - used_bytes);
  }
  env->me_poison_edge =
      bytes2pgno(env, (env->me_dxb_mmap.filesize < env->me_dxb_mmap.limit)
                          ? env->me_dxb_mmap.filesize
                          : env->me_dxb_mmap.limit);
#endif /* MDBX_USE_VALGRIND || __SANITIZE_ADDRESS__ */

  meta_troika_t troika = meta_tap(env);
#if MDBX_DEBUG
  meta_troika_dump(env, &troika);
#endif
  eASSERT(env, !env->me_txn && !env->me_txn0);
  //-------------------------------- validate/rollback head & steady meta-pages
  if (unlikely(env->me_stuck_meta >= 0)) {
    /* recovery mode */
    MDBX_meta clone;
    MDBX_meta const *const target = METAPAGE(env, env->me_stuck_meta);
    err = validate_meta_copy(env, target, &clone);
    if (unlikely(err != MDBX_SUCCESS)) {
      ERROR("target meta[%u] is corrupted",
            bytes2pgno(env, ptr_dist(data_page(target), env->me_map)));
      meta_troika_dump(env, &troika);
      return MDBX_CORRUPTED;
    }
  } else /* not recovery mode */
    while (1) {
      const unsigned meta_clash_mask = meta_eq_mask(&troika);
      if (unlikely(meta_clash_mask)) {
        ERROR("meta-pages are clashed: mask 0x%d", meta_clash_mask);
        meta_troika_dump(env, &troika);
        return MDBX_CORRUPTED;
      }

      if (lck_rc != /* lck exclusive */ MDBX_RESULT_TRUE) {
        /* non-exclusive mode,
         * meta-pages should be validated by a first process opened the DB */
        if (troika.recent == troika.prefer_steady)
          break;

        if (!env->me_lck_mmap.lck) {
          /* LY: without-lck (read-only) mode, so it is impossible that other
           * process made weak checkpoint. */
          ERROR("%s", "without-lck, unable recovery/rollback");
          meta_troika_dump(env, &troika);
          return MDBX_WANNA_RECOVERY;
        }

        /* LY: assume just have a collision with other running process,
         *     or someone make a weak checkpoint */
        VERBOSE("%s", "assume collision or online weak checkpoint");
        break;
      }
      eASSERT(env, lck_rc == MDBX_RESULT_TRUE);
      /* exclusive mode */

      const meta_ptr_t recent = meta_recent(env, &troika);
      const meta_ptr_t prefer_steady = meta_prefer_steady(env, &troika);
      MDBX_meta clone;
      if (prefer_steady.is_steady) {
        err = validate_meta_copy(env, prefer_steady.ptr_c, &clone);
        if (unlikely(err != MDBX_SUCCESS)) {
          ERROR("meta[%u] with %s txnid %" PRIaTXN " is corrupted, %s needed",
                bytes2pgno(env, ptr_dist(prefer_steady.ptr_c, env->me_map)),
                "steady", prefer_steady.txnid, "manual recovery");
          meta_troika_dump(env, &troika);
          return MDBX_CORRUPTED;
        }
        if (prefer_steady.ptr_c == recent.ptr_c)
          break;
      }

      const pgno_t pgno = bytes2pgno(env, ptr_dist(recent.ptr_c, env->me_map));
      const bool last_valid =
          validate_meta_copy(env, recent.ptr_c, &clone) == MDBX_SUCCESS;
      eASSERT(env,
              !prefer_steady.is_steady || recent.txnid != prefer_steady.txnid);
      if (unlikely(!last_valid)) {
        if (unlikely(!prefer_steady.is_steady)) {
          ERROR("%s for open or automatic rollback, %s",
                "there are no suitable meta-pages",
                "manual recovery is required");
          meta_troika_dump(env, &troika);
          return MDBX_CORRUPTED;
        }
        WARNING("meta[%u] with last txnid %" PRIaTXN
                " is corrupted, rollback needed",
                pgno, recent.txnid);
        meta_troika_dump(env, &troika);
        goto purge_meta_head;
      }

      if (meta_bootid_match(recent.ptr_c)) {
        if (env->me_flags & MDBX_RDONLY) {
          ERROR("%s, but boot-id(%016" PRIx64 "-%016" PRIx64 ") is MATCH: "
                "rollback NOT needed, steady-sync NEEDED%s",
                "opening after an unclean shutdown", bootid.x, bootid.y,
                ", but unable in read-only mode");
          meta_troika_dump(env, &troika);
          return MDBX_WANNA_RECOVERY;
        }
        WARNING("%s, but boot-id(%016" PRIx64 "-%016" PRIx64 ") is MATCH: "
                "rollback NOT needed, steady-sync NEEDED%s",
                "opening after an unclean shutdown", bootid.x, bootid.y, "");
        header = clone;
        env->me_lck->mti_unsynced_pages.weak = header.mm_geo.next;
        if (!env->me_lck->mti_eoos_timestamp.weak)
          env->me_lck->mti_eoos_timestamp.weak = osal_monotime();
        break;
      }
      if (unlikely(!prefer_steady.is_steady)) {
        ERROR("%s, but %s for automatic rollback: %s",
              "opening after an unclean shutdown",
              "there are no suitable meta-pages",
              "manual recovery is required");
        meta_troika_dump(env, &troika);
        return MDBX_CORRUPTED;
      }
      if (env->me_flags & MDBX_RDONLY) {
        ERROR("%s and rollback needed: (from head %" PRIaTXN
              " to steady %" PRIaTXN ")%s",
              "opening after an unclean shutdown", recent.txnid,
              prefer_steady.txnid, ", but unable in read-only mode");
        meta_troika_dump(env, &troika);
        return MDBX_WANNA_RECOVERY;
      }

    purge_meta_head:
      NOTICE("%s and doing automatic rollback: "
             "purge%s meta[%u] with%s txnid %" PRIaTXN,
             "opening after an unclean shutdown", last_valid ? "" : " invalid",
             pgno, last_valid ? " weak" : "", recent.txnid);
      meta_troika_dump(env, &troika);
      ENSURE(env, prefer_steady.is_steady);
      err = override_meta(env, pgno, 0,
                          last_valid ? recent.ptr_c : prefer_steady.ptr_c);
      if (err) {
        ERROR("rollback: overwrite meta[%u] with txnid %" PRIaTXN ", error %d",
              pgno, recent.txnid, err);
        return err;
      }
      troika = meta_tap(env);
      ENSURE(env, 0 == meta_txnid(recent.ptr_v));
      ENSURE(env, 0 == meta_eq_mask(&troika));
    }

  if (lck_rc == /* lck exclusive */ MDBX_RESULT_TRUE) {
    //-------------------------------------------------- shrink DB & update geo
    /* re-check size after mmap */
    if ((env->me_dxb_mmap.current & (env->me_os_psize - 1)) != 0 ||
        env->me_dxb_mmap.current < used_bytes) {
      ERROR("unacceptable/unexpected datafile size %" PRIuPTR,
            env->me_dxb_mmap.current);
      return MDBX_PROBLEM;
    }
    if (env->me_dxb_mmap.current != env->me_dbgeo.now) {
      header.mm_geo.now = bytes2pgno(env, env->me_dxb_mmap.current);
      NOTICE("need update meta-geo to filesize %" PRIuPTR " bytes, %" PRIaPGNO
             " pages",
             env->me_dxb_mmap.current, header.mm_geo.now);
    }

    const meta_ptr_t recent = meta_recent(env, &troika);
    if (/* не учитываем различия в geo.next */
        header.mm_geo.grow_pv != recent.ptr_c->mm_geo.grow_pv ||
        header.mm_geo.shrink_pv != recent.ptr_c->mm_geo.shrink_pv ||
        header.mm_geo.lower != recent.ptr_c->mm_geo.lower ||
        header.mm_geo.upper != recent.ptr_c->mm_geo.upper ||
        header.mm_geo.now != recent.ptr_c->mm_geo.now) {
      if ((env->me_flags & MDBX_RDONLY) != 0 ||
          /* recovery mode */ env->me_stuck_meta >= 0) {
        WARNING("skipped update meta.geo in %s mode: from l%" PRIaPGNO
                "-n%" PRIaPGNO "-u%" PRIaPGNO "/s%u-g%u, to l%" PRIaPGNO
                "-n%" PRIaPGNO "-u%" PRIaPGNO "/s%u-g%u",
                (env->me_stuck_meta < 0) ? "read-only" : "recovery",
                recent.ptr_c->mm_geo.lower, recent.ptr_c->mm_geo.now,
                recent.ptr_c->mm_geo.upper,
                pv2pages(recent.ptr_c->mm_geo.shrink_pv),
                pv2pages(recent.ptr_c->mm_geo.grow_pv), header.mm_geo.lower,
                header.mm_geo.now, header.mm_geo.upper,
                pv2pages(header.mm_geo.shrink_pv),
                pv2pages(header.mm_geo.grow_pv));
      } else {
        const txnid_t next_txnid = safe64_txnid_next(recent.txnid);
        if (unlikely(next_txnid > MAX_TXNID)) {
          ERROR("txnid overflow, raise %d", MDBX_TXN_FULL);
          return MDBX_TXN_FULL;
        }
        NOTICE("updating meta.geo: "
               "from l%" PRIaPGNO "-n%" PRIaPGNO "-u%" PRIaPGNO
               "/s%u-g%u (txn#%" PRIaTXN "), "
               "to l%" PRIaPGNO "-n%" PRIaPGNO "-u%" PRIaPGNO
               "/s%u-g%u (txn#%" PRIaTXN ")",
               recent.ptr_c->mm_geo.lower, recent.ptr_c->mm_geo.now,
               recent.ptr_c->mm_geo.upper,
               pv2pages(recent.ptr_c->mm_geo.shrink_pv),
               pv2pages(recent.ptr_c->mm_geo.grow_pv), recent.txnid,
               header.mm_geo.lower, header.mm_geo.now, header.mm_geo.upper,
               pv2pages(header.mm_geo.shrink_pv),
               pv2pages(header.mm_geo.grow_pv), next_txnid);

        ENSURE(env, header.unsafe_txnid == recent.txnid);
        meta_set_txnid(env, &header, next_txnid);
        err = sync_locked(env, env->me_flags | MDBX_SHRINK_ALLOWED, &header,
                          &troika);
        if (err) {
          ERROR("error %d, while updating meta.geo: "
                "from l%" PRIaPGNO "-n%" PRIaPGNO "-u%" PRIaPGNO
                "/s%u-g%u (txn#%" PRIaTXN "), "
                "to l%" PRIaPGNO "-n%" PRIaPGNO "-u%" PRIaPGNO
                "/s%u-g%u (txn#%" PRIaTXN ")",
                err, recent.ptr_c->mm_geo.lower, recent.ptr_c->mm_geo.now,
                recent.ptr_c->mm_geo.upper,
                pv2pages(recent.ptr_c->mm_geo.shrink_pv),
                pv2pages(recent.ptr_c->mm_geo.grow_pv), recent.txnid,
                header.mm_geo.lower, header.mm_geo.now, header.mm_geo.upper,
                pv2pages(header.mm_geo.shrink_pv),
                pv2pages(header.mm_geo.grow_pv), header.unsafe_txnid);
          return err;
        }
      }
    }

    atomic_store32(&env->me_lck->mti_discarded_tail,
                   bytes2pgno(env, used_aligned2os_bytes), mo_Relaxed);

    if ((env->me_flags & MDBX_RDONLY) == 0 && env->me_stuck_meta < 0 &&
        (runtime_flags & MDBX_DBG_DONT_UPGRADE) == 0) {
      for (int n = 0; n < NUM_METAS; ++n) {
        MDBX_meta *const meta = METAPAGE(env, n);
        if (unlikely(unaligned_peek_u64(4, &meta->mm_magic_and_version) !=
                     MDBX_DATA_MAGIC)) {
          const txnid_t txnid = constmeta_txnid(meta);
          NOTICE("%s %s"
                 "meta[%u], txnid %" PRIaTXN,
                 "updating db-format signature for",
                 META_IS_STEADY(meta) ? "stead-" : "weak-", n, txnid);
          err = override_meta(env, n, txnid, meta);
          if (unlikely(err != MDBX_SUCCESS) &&
              /* Just ignore the MDBX_PROBLEM error, since here it is
               * returned only in case of the attempt to upgrade an obsolete
               * meta-page that is invalid for current state of a DB,
               * e.g. after shrinking DB file */
              err != MDBX_PROBLEM) {
            ERROR("%s meta[%u], txnid %" PRIaTXN ", error %d",
                  "updating db-format signature for", n, txnid, err);
            return err;
          }
          troika = meta_tap(env);
        }
      }
    }
  } /* lck exclusive, lck_rc == MDBX_RESULT_TRUE */

  //---------------------------------------------------- setup madvise/readahead
#if MDBX_ENABLE_MADVISE
  if (used_aligned2os_bytes < env->me_dxb_mmap.current) {
#if defined(MADV_REMOVE)
    if (lck_rc && (env->me_flags & MDBX_WRITEMAP) != 0 &&
        /* not recovery mode */ env->me_stuck_meta < 0) {
      NOTICE("open-MADV_%s %u..%u", "REMOVE (deallocate file space)",
             env->me_lck->mti_discarded_tail.weak,
             bytes2pgno(env, env->me_dxb_mmap.current));
      err =
          madvise(ptr_disp(env->me_map, used_aligned2os_bytes),
                  env->me_dxb_mmap.current - used_aligned2os_bytes, MADV_REMOVE)
              ? ignore_enosys(errno)
              : MDBX_SUCCESS;
      if (unlikely(MDBX_IS_ERROR(err)))
        return err;
    }
#endif /* MADV_REMOVE */
#if defined(MADV_DONTNEED)
    NOTICE("open-MADV_%s %u..%u", "DONTNEED",
           env->me_lck->mti_discarded_tail.weak,
           bytes2pgno(env, env->me_dxb_mmap.current));
    err =
        madvise(ptr_disp(env->me_map, used_aligned2os_bytes),
                env->me_dxb_mmap.current - used_aligned2os_bytes, MADV_DONTNEED)
            ? ignore_enosys(errno)
            : MDBX_SUCCESS;
    if (unlikely(MDBX_IS_ERROR(err)))
      return err;
#elif defined(POSIX_MADV_DONTNEED)
    err = ignore_enosys(posix_madvise(
        ptr_disp(env->me_map, used_aligned2os_bytes),
        env->me_dxb_mmap.current - used_aligned2os_bytes, POSIX_MADV_DONTNEED));
    if (unlikely(MDBX_IS_ERROR(err)))
      return err;
#elif defined(POSIX_FADV_DONTNEED)
    err = ignore_enosys(posix_fadvise(
        env->me_lazy_fd, used_aligned2os_bytes,
        env->me_dxb_mmap.current - used_aligned2os_bytes, POSIX_FADV_DONTNEED));
    if (unlikely(MDBX_IS_ERROR(err)))
      return err;
#endif /* MADV_DONTNEED */
  }

  err = set_readahead(env, bytes2pgno(env, used_bytes), readahead, true);
  if (unlikely(err != MDBX_SUCCESS))
    return err;
#endif /* MDBX_ENABLE_MADVISE */

  return rc;
}

/******************************************************************************/

/* Open and/or initialize the lock region for the environment. */
__cold static int setup_lck(MDBX_env *env, pathchar_t *lck_pathname,
                            mdbx_mode_t mode) {
  eASSERT(env, env->me_lazy_fd != INVALID_HANDLE_VALUE);
  eASSERT(env, env->me_lfd == INVALID_HANDLE_VALUE);

  int err = osal_openfile(MDBX_OPEN_LCK, env, lck_pathname, &env->me_lfd, mode);
  if (err != MDBX_SUCCESS) {
    switch (err) {
    default:
      return err;
    case MDBX_ENOFILE:
    case MDBX_EACCESS:
    case MDBX_EPERM:
      if (!F_ISSET(env->me_flags, MDBX_RDONLY | MDBX_EXCLUSIVE))
        return err;
      break;
    case MDBX_EROFS:
      if ((env->me_flags & MDBX_RDONLY) == 0)
        return err;
      break;
    }

    if (err != MDBX_ENOFILE) {
      /* ENSURE the file system is read-only */
      err = osal_check_fs_rdonly(env->me_lazy_fd, lck_pathname, err);
      if (err != MDBX_SUCCESS &&
          /* ignore ERROR_NOT_SUPPORTED for exclusive mode */
          !(err == MDBX_ENOSYS && (env->me_flags & MDBX_EXCLUSIVE)))
        return err;
    }

    /* LY: without-lck mode (e.g. exclusive or on read-only filesystem) */
    /* beginning of a locked section ---------------------------------------- */
    lcklist_lock();
    eASSERT(env, env->me_lcklist_next == nullptr);
    env->me_lfd = INVALID_HANDLE_VALUE;
    const int rc = osal_lck_seize(env);
    if (MDBX_IS_ERROR(rc)) {
      /* Calling lcklist_detach_locked() is required to restore POSIX-filelock
       * and this job will be done by env_close(). */
      lcklist_unlock();
      return rc;
    }
    /* insert into inprocess lck-list */
    env->me_lcklist_next = inprocess_lcklist_head;
    inprocess_lcklist_head = env;
    lcklist_unlock();
    /* end of a locked section ---------------------------------------------- */

    env->me_lck = lckless_stub(env);
    env->me_maxreaders = UINT_MAX;
    DEBUG("lck-setup:%s%s%s", " lck-less",
          (env->me_flags & MDBX_RDONLY) ? " readonly" : "",
          (rc == MDBX_RESULT_TRUE) ? " exclusive" : " cooperative");
    return rc;
  }

  /* beginning of a locked section ------------------------------------------ */
  lcklist_lock();
  eASSERT(env, env->me_lcklist_next == nullptr);

  /* Try to get exclusive lock. If we succeed, then
   * nobody is using the lock region and we should initialize it. */
  err = osal_lck_seize(env);
  if (MDBX_IS_ERROR(err)) {
  bailout:
    /* Calling lcklist_detach_locked() is required to restore POSIX-filelock
     * and this job will be done by env_close(). */
    lcklist_unlock();
    return err;
  }

  MDBX_env *inprocess_neighbor = nullptr;
  if (err == MDBX_RESULT_TRUE) {
    err = uniq_check(&env->me_lck_mmap, &inprocess_neighbor);
    if (MDBX_IS_ERROR(err))
      goto bailout;
    if (inprocess_neighbor &&
        ((runtime_flags & MDBX_DBG_LEGACY_MULTIOPEN) == 0 ||
         (inprocess_neighbor->me_flags & MDBX_EXCLUSIVE) != 0)) {
      err = MDBX_BUSY;
      goto bailout;
    }
  }
  const int lck_seize_rc = err;

  DEBUG("lck-setup:%s%s%s", " with-lck",
        (env->me_flags & MDBX_RDONLY) ? " readonly" : "",
        (lck_seize_rc == MDBX_RESULT_TRUE) ? " exclusive" : " cooperative");

  uint64_t size = 0;
  err = osal_filesize(env->me_lfd, &size);
  if (unlikely(err != MDBX_SUCCESS))
    goto bailout;

  if (lck_seize_rc == MDBX_RESULT_TRUE) {
    size = ceil_powerof2(env->me_maxreaders * sizeof(MDBX_reader) +
                             sizeof(MDBX_lockinfo),
                         env->me_os_psize);
    jitter4testing(false);
  } else {
    if (env->me_flags & MDBX_EXCLUSIVE) {
      err = MDBX_BUSY;
      goto bailout;
    }
    if (size > INT_MAX || (size & (env->me_os_psize - 1)) != 0 ||
        size < env->me_os_psize) {
      ERROR("lck-file has invalid size %" PRIu64 " bytes", size);
      err = MDBX_PROBLEM;
      goto bailout;
    }
  }

  const size_t maxreaders =
      ((size_t)size - sizeof(MDBX_lockinfo)) / sizeof(MDBX_reader);
  if (maxreaders < 4) {
    ERROR("lck-size too small (up to %" PRIuPTR " readers)", maxreaders);
    err = MDBX_PROBLEM;
    goto bailout;
  }
  env->me_maxreaders = (maxreaders <= MDBX_READERS_LIMIT)
                           ? (unsigned)maxreaders
                           : (unsigned)MDBX_READERS_LIMIT;

  err = osal_mmap((env->me_flags & MDBX_EXCLUSIVE) | MDBX_WRITEMAP,
                  &env->me_lck_mmap, (size_t)size, (size_t)size,
                  lck_seize_rc ? MMAP_OPTION_TRUNCATE | MMAP_OPTION_SEMAPHORE
                               : MMAP_OPTION_SEMAPHORE);
  if (unlikely(err != MDBX_SUCCESS))
    goto bailout;

#if MDBX_ENABLE_MADVISE
#ifdef MADV_DODUMP
  err = madvise(env->me_lck_mmap.lck, size, MADV_DODUMP) ? ignore_enosys(errno)
                                                         : MDBX_SUCCESS;
  if (unlikely(MDBX_IS_ERROR(err)))
    goto bailout;
#endif /* MADV_DODUMP */

#ifdef MADV_WILLNEED
  err = madvise(env->me_lck_mmap.lck, size, MADV_WILLNEED)
            ? ignore_enosys(errno)
            : MDBX_SUCCESS;
  if (unlikely(MDBX_IS_ERROR(err)))
    goto bailout;
#elif defined(POSIX_MADV_WILLNEED)
  err = ignore_enosys(
      posix_madvise(env->me_lck_mmap.lck, size, POSIX_MADV_WILLNEED));
  if (unlikely(MDBX_IS_ERROR(err)))
    goto bailout;
#endif /* MADV_WILLNEED */
#endif /* MDBX_ENABLE_MADVISE */

  struct MDBX_lockinfo *const lck = env->me_lck_mmap.lck;
  if (lck_seize_rc == MDBX_RESULT_TRUE) {
    /* LY: exclusive mode, check and reset lck content */
    memset(lck, 0, (size_t)size);
    jitter4testing(false);
    lck->mti_magic_and_version = MDBX_LOCK_MAGIC;
    lck->mti_os_and_format = MDBX_LOCK_FORMAT;
#if MDBX_ENABLE_PGOP_STAT
    lck->mti_pgop_stat.wops.weak = 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
    err = osal_msync(&env->me_lck_mmap, 0, (size_t)size,
                     MDBX_SYNC_DATA | MDBX_SYNC_SIZE);
    if (unlikely(err != MDBX_SUCCESS)) {
      ERROR("initial-%s for lck-file failed, err %d", "msync/fsync", err);
      goto bailout;
    }
  } else {
    if (lck->mti_magic_and_version != MDBX_LOCK_MAGIC) {
      const bool invalid = (lck->mti_magic_and_version >> 8) != MDBX_MAGIC;
      ERROR("lock region has %s",
            invalid
                ? "invalid magic"
                : "incompatible version (only applications with nearly or the "
                  "same versions of libmdbx can share the same database)");
      err = invalid ? MDBX_INVALID : MDBX_VERSION_MISMATCH;
      goto bailout;
    }
    if (lck->mti_os_and_format != MDBX_LOCK_FORMAT) {
      ERROR("lock region has os/format signature 0x%" PRIx32
            ", expected 0x%" PRIx32,
            lck->mti_os_and_format, MDBX_LOCK_FORMAT);
      err = MDBX_VERSION_MISMATCH;
      goto bailout;
    }
  }

  err = osal_lck_init(env, inprocess_neighbor, lck_seize_rc);
  if (MDBX_IS_ERROR(err))
    goto bailout;

  ENSURE(env, env->me_lcklist_next == nullptr);
  /* insert into inprocess lck-list */
  env->me_lcklist_next = inprocess_lcklist_head;
  inprocess_lcklist_head = env;
  lcklist_unlock();
  /* end of a locked section ------------------------------------------------ */

  eASSERT(env, !MDBX_IS_ERROR(lck_seize_rc));
  env->me_lck = lck;
  return lck_seize_rc;
}

__cold int mdbx_is_readahead_reasonable(size_t volume, intptr_t redundancy) {
  if (volume <= 1024 * 1024 * 4ul)
    return MDBX_RESULT_TRUE;

  intptr_t pagesize, total_ram_pages;
  int err = mdbx_get_sysraminfo(&pagesize, &total_ram_pages, nullptr);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  const int log2page = log2n_powerof2(pagesize);
  const intptr_t volume_pages = (volume + pagesize - 1) >> log2page;
  const intptr_t redundancy_pages =
      (redundancy < 0) ? -(intptr_t)((-redundancy + pagesize - 1) >> log2page)
                       : (intptr_t)(redundancy + pagesize - 1) >> log2page;
  if (volume_pages >= total_ram_pages ||
      volume_pages + redundancy_pages >= total_ram_pages)
    return MDBX_RESULT_FALSE;

  intptr_t avail_ram_pages;
  err = mdbx_get_sysraminfo(nullptr, nullptr, &avail_ram_pages);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  return (volume_pages + redundancy_pages >= avail_ram_pages)
             ? MDBX_RESULT_FALSE
             : MDBX_RESULT_TRUE;
}

/* Merge sync flags */
static uint32_t merge_sync_flags(const uint32_t a, const uint32_t b) {
  uint32_t r = a | b;

  /* avoid false MDBX_UTTERLY_NOSYNC */
  if (F_ISSET(r, MDBX_UTTERLY_NOSYNC) && !F_ISSET(a, MDBX_UTTERLY_NOSYNC) &&
      !F_ISSET(b, MDBX_UTTERLY_NOSYNC))
    r = (r - MDBX_UTTERLY_NOSYNC) | MDBX_SAFE_NOSYNC;

  /* convert MDBX_DEPRECATED_MAPASYNC to MDBX_SAFE_NOSYNC */
  if ((r & (MDBX_WRITEMAP | MDBX_DEPRECATED_MAPASYNC)) ==
          (MDBX_WRITEMAP | MDBX_DEPRECATED_MAPASYNC) &&
      !F_ISSET(r, MDBX_UTTERLY_NOSYNC))
    r = (r - MDBX_DEPRECATED_MAPASYNC) | MDBX_SAFE_NOSYNC;

  /* force MDBX_NOMETASYNC if NOSYNC enabled */
  if (r & (MDBX_SAFE_NOSYNC | MDBX_UTTERLY_NOSYNC))
    r |= MDBX_NOMETASYNC;

  assert(!(F_ISSET(r, MDBX_UTTERLY_NOSYNC) &&
           !F_ISSET(a, MDBX_UTTERLY_NOSYNC) &&
           !F_ISSET(b, MDBX_UTTERLY_NOSYNC)));
  return r;
}

__cold static int __must_check_result override_meta(MDBX_env *env,
                                                    size_t target,
                                                    txnid_t txnid,
                                                    const MDBX_meta *shape) {
  int rc = alloc_page_buf(env);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;
  MDBX_page *const page = env->me_pbuf;
  meta_model(env, page, target);
  MDBX_meta *const model = page_meta(page);
  meta_set_txnid(env, model, txnid);
  if (txnid)
    eASSERT(env, check_meta_coherency(env, model, true));
  if (shape) {
    if (txnid && unlikely(!check_meta_coherency(env, shape, false))) {
      ERROR("bailout overriding meta-%zu since model failed "
            "freedb/maindb %s-check for txnid #%" PRIaTXN,
            target, "pre", constmeta_txnid(shape));
      return MDBX_PROBLEM;
    }
    if (runtime_flags & MDBX_DBG_DONT_UPGRADE)
      memcpy(&model->mm_magic_and_version, &shape->mm_magic_and_version,
             sizeof(model->mm_magic_and_version));
    model->mm_extra_flags = shape->mm_extra_flags;
    model->mm_validator_id = shape->mm_validator_id;
    model->mm_extra_pagehdr = shape->mm_extra_pagehdr;
    memcpy(&model->mm_geo, &shape->mm_geo, sizeof(model->mm_geo));
    memcpy(&model->mm_dbs, &shape->mm_dbs, sizeof(model->mm_dbs));
    memcpy(&model->mm_canary, &shape->mm_canary, sizeof(model->mm_canary));
    memcpy(&model->mm_pages_retired, &shape->mm_pages_retired,
           sizeof(model->mm_pages_retired));
    if (txnid) {
      if ((!model->mm_dbs[FREE_DBI].md_mod_txnid &&
           model->mm_dbs[FREE_DBI].md_root != P_INVALID) ||
          (!model->mm_dbs[MAIN_DBI].md_mod_txnid &&
           model->mm_dbs[MAIN_DBI].md_root != P_INVALID))
        memcpy(&model->mm_magic_and_version, &shape->mm_magic_and_version,
               sizeof(model->mm_magic_and_version));
      if (unlikely(!check_meta_coherency(env, model, false))) {
        ERROR("bailout overriding meta-%zu since model failed "
              "freedb/maindb %s-check for txnid #%" PRIaTXN,
              target, "post", txnid);
        return MDBX_PROBLEM;
      }
    }
  }
  unaligned_poke_u64(4, model->mm_sign, meta_sign(model));
  rc = validate_meta(env, model, page, (pgno_t)target, nullptr);
  if (unlikely(MDBX_IS_ERROR(rc)))
    return MDBX_PROBLEM;

  if (shape && memcmp(model, shape, sizeof(MDBX_meta)) == 0) {
    NOTICE("skip overriding meta-%zu since no changes "
           "for txnid #%" PRIaTXN,
           target, txnid);
    return MDBX_SUCCESS;
  }

  if (env->me_flags & MDBX_WRITEMAP) {
#if MDBX_ENABLE_PGOP_STAT
    env->me_lck->mti_pgop_stat.msync.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
    rc = osal_msync(&env->me_dxb_mmap, 0,
                    pgno_align2os_bytes(env, model->mm_geo.next),
                    MDBX_SYNC_DATA | MDBX_SYNC_IODQ);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    /* override_meta() called only while current process have exclusive
     * lock of a DB file. So meta-page could be updated directly without
     * clearing consistency flag by mdbx_meta_update_begin() */
    memcpy(pgno2page(env, target), page, env->me_psize);
    osal_flush_incoherent_cpu_writeback();
#if MDBX_ENABLE_PGOP_STAT
    env->me_lck->mti_pgop_stat.msync.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
    rc = osal_msync(&env->me_dxb_mmap, 0, pgno_align2os_bytes(env, target + 1),
                    MDBX_SYNC_DATA | MDBX_SYNC_IODQ);
  } else {
#if MDBX_ENABLE_PGOP_STAT
    env->me_lck->mti_pgop_stat.wops.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
    rc = osal_pwrite(env->me_fd4meta, page, env->me_psize,
                     pgno2bytes(env, target));
    if (rc == MDBX_SUCCESS && env->me_fd4meta == env->me_lazy_fd) {
#if MDBX_ENABLE_PGOP_STAT
      env->me_lck->mti_pgop_stat.fsync.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
      rc = osal_fsync(env->me_lazy_fd, MDBX_SYNC_DATA | MDBX_SYNC_IODQ);
    }
    osal_flush_incoherent_mmap(env->me_map, pgno2bytes(env, NUM_METAS),
                               env->me_os_psize);
  }
  eASSERT(env, (!env->me_txn && !env->me_txn0) ||
                   (env->me_stuck_meta == (int)target &&
                    (env->me_flags & (MDBX_EXCLUSIVE | MDBX_RDONLY)) ==
                        MDBX_EXCLUSIVE));
  return rc;
}

__cold int mdbx_env_turn_for_recovery(MDBX_env *env, unsigned target) {
  if (unlikely(target >= NUM_METAS))
    return MDBX_EINVAL;
  int rc = check_env(env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely((env->me_flags & (MDBX_EXCLUSIVE | MDBX_RDONLY)) !=
               MDBX_EXCLUSIVE))
    return MDBX_EPERM;

  const MDBX_meta *const target_meta = METAPAGE(env, target);
  txnid_t new_txnid = constmeta_txnid(target_meta);
  if (new_txnid < MIN_TXNID)
    new_txnid = MIN_TXNID;
  for (unsigned n = 0; n < NUM_METAS; ++n) {
    if (n == target)
      continue;
    MDBX_page *const page = pgno2page(env, n);
    MDBX_meta meta = *page_meta(page);
    if (validate_meta(env, &meta, page, n, nullptr) != MDBX_SUCCESS) {
      int err = override_meta(env, n, 0, nullptr);
      if (unlikely(err != MDBX_SUCCESS))
        return err;
    } else {
      txnid_t txnid = constmeta_txnid(&meta);
      if (new_txnid <= txnid)
        new_txnid = safe64_txnid_next(txnid);
    }
  }

  if (unlikely(new_txnid > MAX_TXNID)) {
    ERROR("txnid overflow, raise %d", MDBX_TXN_FULL);
    return MDBX_TXN_FULL;
  }
  return override_meta(env, target, new_txnid, target_meta);
}

__cold int mdbx_env_open_for_recovery(MDBX_env *env, const char *pathname,
                                      unsigned target_meta, bool writeable) {
#if defined(_WIN32) || defined(_WIN64)
  wchar_t *pathnameW = nullptr;
  int rc = osal_mb2w(pathname, &pathnameW);
  if (likely(rc == MDBX_SUCCESS)) {
    rc = mdbx_env_open_for_recoveryW(env, pathnameW, target_meta, writeable);
    osal_free(pathnameW);
  }
  return rc;
}

__cold int mdbx_env_open_for_recoveryW(MDBX_env *env, const wchar_t *pathname,
                                       unsigned target_meta, bool writeable) {
#endif /* Windows */

  if (unlikely(target_meta >= NUM_METAS))
    return MDBX_EINVAL;
  int rc = check_env(env, false);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;
  if (unlikely(env->me_map))
    return MDBX_EPERM;

  env->me_stuck_meta = (int8_t)target_meta;
  return
#if defined(_WIN32) || defined(_WIN64)
      mdbx_env_openW
#else
      mdbx_env_open
#endif /* Windows */
      (env, pathname, writeable ? MDBX_EXCLUSIVE : MDBX_EXCLUSIVE | MDBX_RDONLY,
       0);
}

typedef struct {
  void *buffer_for_free;
  pathchar_t *lck, *dxb;
  size_t ent_len;
} MDBX_handle_env_pathname;

__cold static int check_alternative_lck_absent(const pathchar_t *lck_pathname) {
  int err = osal_fileexists(lck_pathname);
  if (unlikely(err != MDBX_RESULT_FALSE)) {
    if (err == MDBX_RESULT_TRUE)
      err = MDBX_DUPLICATED_CLK;
    ERROR("Alternative/Duplicate LCK-file '%" MDBX_PRIsPATH "' error %d",
          lck_pathname, err);
  }
  return err;
}

__cold static int handle_env_pathname(MDBX_handle_env_pathname *ctx,
                                      const pathchar_t *pathname,
                                      MDBX_env_flags_t *flags,
                                      const mdbx_mode_t mode) {
  memset(ctx, 0, sizeof(*ctx));
  if (unlikely(!pathname || !*pathname))
    return MDBX_EINVAL;

  int rc;
#if defined(_WIN32) || defined(_WIN64)
  const DWORD dwAttrib = GetFileAttributesW(pathname);
  if (dwAttrib == INVALID_FILE_ATTRIBUTES) {
    rc = GetLastError();
    if (rc != MDBX_ENOFILE)
      return rc;
    if (mode == 0 || (*flags & MDBX_RDONLY) != 0)
      /* can't open existing */
      return rc;

    /* auto-create directory if requested */
    if ((*flags & MDBX_NOSUBDIR) == 0 && !CreateDirectoryW(pathname, nullptr)) {
      rc = GetLastError();
      if (rc != ERROR_ALREADY_EXISTS)
        return rc;
    }
  } else {
    /* ignore passed MDBX_NOSUBDIR flag and set it automatically */
    *flags |= MDBX_NOSUBDIR;
    if (dwAttrib & FILE_ATTRIBUTE_DIRECTORY)
      *flags -= MDBX_NOSUBDIR;
  }
#else
  struct stat st;
  if (stat(pathname, &st) != 0) {
    rc = errno;
    if (rc != MDBX_ENOFILE)
      return rc;
    if (mode == 0 || (*flags & MDBX_RDONLY) != 0)
      /* can't open non-existing */
      return rc /* MDBX_ENOFILE */;

    /* auto-create directory if requested */
    const mdbx_mode_t dir_mode =
        (/* inherit read/write permissions for group and others */ mode &
         (S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH)) |
        /* always add read/write/search for owner */ S_IRWXU |
        ((mode & S_IRGRP) ? /* +search if readable by group */ S_IXGRP : 0) |
        ((mode & S_IROTH) ? /* +search if readable by others */ S_IXOTH : 0);
    if ((*flags & MDBX_NOSUBDIR) == 0 && mkdir(pathname, dir_mode)) {
      rc = errno;
      if (rc != EEXIST)
        return rc;
    }
  } else {
    /* ignore passed MDBX_NOSUBDIR flag and set it automatically */
    *flags |= MDBX_NOSUBDIR;
    if (S_ISDIR(st.st_mode))
      *flags -= MDBX_NOSUBDIR;
  }
#endif

  static const pathchar_t dxb_name[] = MDBX_DATANAME;
  static const pathchar_t lck_name[] = MDBX_LOCKNAME;
  static const pathchar_t lock_suffix[] = MDBX_LOCK_SUFFIX;

#if defined(_WIN32) || defined(_WIN64)
  assert(dxb_name[0] == '\\' && lck_name[0] == '\\');
  const size_t pathname_len = wcslen(pathname);
#else
  assert(dxb_name[0] == '/' && lck_name[0] == '/');
  const size_t pathname_len = strlen(pathname);
#endif
  assert(!osal_isdirsep(lock_suffix[0]));
  ctx->ent_len = pathname_len;
  static const size_t dxb_name_len = ARRAY_LENGTH(dxb_name) - 1;
  if (*flags & MDBX_NOSUBDIR) {
    if (ctx->ent_len > dxb_name_len &&
        osal_pathequal(pathname + ctx->ent_len - dxb_name_len, dxb_name,
                       dxb_name_len)) {
      *flags -= MDBX_NOSUBDIR;
      ctx->ent_len -= dxb_name_len;
    } else if (ctx->ent_len == dxb_name_len - 1 && osal_isdirsep(dxb_name[0]) &&
               osal_isdirsep(lck_name[0]) &&
               osal_pathequal(pathname + ctx->ent_len - dxb_name_len + 1,
                              dxb_name + 1, dxb_name_len - 1)) {
      *flags -= MDBX_NOSUBDIR;
      ctx->ent_len -= dxb_name_len - 1;
    }
  }

  const size_t suflen_with_NOSUBDIR = sizeof(lock_suffix) + sizeof(pathchar_t);
  const size_t suflen_without_NOSUBDIR = sizeof(lck_name) + sizeof(dxb_name);
  const size_t enogh4any = (suflen_with_NOSUBDIR > suflen_without_NOSUBDIR)
                               ? suflen_with_NOSUBDIR
                               : suflen_without_NOSUBDIR;
  const size_t bytes_needed = sizeof(pathchar_t) * ctx->ent_len * 2 + enogh4any;
  ctx->buffer_for_free = osal_malloc(bytes_needed);
  if (!ctx->buffer_for_free)
    return MDBX_ENOMEM;

  ctx->dxb = ctx->buffer_for_free;
  ctx->lck = ctx->dxb + ctx->ent_len + dxb_name_len + 1;
  pathchar_t *const buf = ctx->buffer_for_free;
  rc = MDBX_SUCCESS;
  if (ctx->ent_len) {
    memcpy(buf + /* shutting up goofy MSVC static analyzer */ 0, pathname,
           sizeof(pathchar_t) * pathname_len);
    if (*flags & MDBX_NOSUBDIR) {
      const pathchar_t *const lck_ext =
          osal_fileext(lck_name, ARRAY_LENGTH(lck_name));
      if (lck_ext) {
        pathchar_t *pathname_ext = osal_fileext(buf, pathname_len);
        memcpy(pathname_ext ? pathname_ext : buf + pathname_len, lck_ext,
               sizeof(pathchar_t) * (ARRAY_END(lck_name) - lck_ext));
        rc = check_alternative_lck_absent(buf);
      }
    } else {
      memcpy(buf + ctx->ent_len, dxb_name, sizeof(dxb_name));
      memcpy(buf + ctx->ent_len + dxb_name_len, lock_suffix,
             sizeof(lock_suffix));
      rc = check_alternative_lck_absent(buf);
    }

    memcpy(ctx->dxb + /* shutting up goofy MSVC static analyzer */ 0, pathname,
           sizeof(pathchar_t) * (ctx->ent_len + 1));
    memcpy(ctx->lck, pathname, sizeof(pathchar_t) * ctx->ent_len);
    if (*flags & MDBX_NOSUBDIR) {
      memcpy(ctx->lck + ctx->ent_len, lock_suffix, sizeof(lock_suffix));
    } else {
      memcpy(ctx->dxb + ctx->ent_len, dxb_name, sizeof(dxb_name));
      memcpy(ctx->lck + ctx->ent_len, lck_name, sizeof(lck_name));
    }
  } else {
    assert(!(*flags & MDBX_NOSUBDIR));
    memcpy(buf + /* shutting up goofy MSVC static analyzer */ 0, dxb_name + 1,
           sizeof(dxb_name) - sizeof(pathchar_t));
    memcpy(buf + dxb_name_len - 1, lock_suffix, sizeof(lock_suffix));
    rc = check_alternative_lck_absent(buf);

    memcpy(ctx->dxb + /* shutting up goofy MSVC static analyzer */ 0,
           dxb_name + 1, sizeof(dxb_name) - sizeof(pathchar_t));
    memcpy(ctx->lck, lck_name + 1, sizeof(lck_name) - sizeof(pathchar_t));
  }
  return rc;
}

__cold int mdbx_env_delete(const char *pathname, MDBX_env_delete_mode_t mode) {
#if defined(_WIN32) || defined(_WIN64)
  wchar_t *pathnameW = nullptr;
  int rc = osal_mb2w(pathname, &pathnameW);
  if (likely(rc == MDBX_SUCCESS)) {
    rc = mdbx_env_deleteW(pathnameW, mode);
    osal_free(pathnameW);
  }
  return rc;
}

__cold int mdbx_env_deleteW(const wchar_t *pathname,
                            MDBX_env_delete_mode_t mode) {
#endif /* Windows */

  switch (mode) {
  default:
    return MDBX_EINVAL;
  case MDBX_ENV_JUST_DELETE:
  case MDBX_ENV_ENSURE_UNUSED:
  case MDBX_ENV_WAIT_FOR_UNUSED:
    break;
  }

#ifdef __e2k__ /* https://bugs.mcst.ru/bugzilla/show_bug.cgi?id=6011 */
  MDBX_env *const dummy_env = alloca(sizeof(MDBX_env));
#else
  MDBX_env dummy_env_silo, *const dummy_env = &dummy_env_silo;
#endif
  memset(dummy_env, 0, sizeof(*dummy_env));
  dummy_env->me_flags =
      (mode == MDBX_ENV_ENSURE_UNUSED) ? MDBX_EXCLUSIVE : MDBX_ENV_DEFAULTS;
  dummy_env->me_os_psize = (unsigned)osal_syspagesize();
  dummy_env->me_psize = (unsigned)mdbx_default_pagesize();
  dummy_env->me_pathname = (pathchar_t *)pathname;

  MDBX_handle_env_pathname env_pathname;
  STATIC_ASSERT(sizeof(dummy_env->me_flags) == sizeof(MDBX_env_flags_t));
  int rc = MDBX_RESULT_TRUE,
      err = handle_env_pathname(&env_pathname, pathname,
                                (MDBX_env_flags_t *)&dummy_env->me_flags, 0);
  if (likely(err == MDBX_SUCCESS)) {
    mdbx_filehandle_t clk_handle = INVALID_HANDLE_VALUE,
                      dxb_handle = INVALID_HANDLE_VALUE;
    if (mode > MDBX_ENV_JUST_DELETE) {
      err = osal_openfile(MDBX_OPEN_DELETE, dummy_env, env_pathname.dxb,
                          &dxb_handle, 0);
      err = (err == MDBX_ENOFILE) ? MDBX_SUCCESS : err;
      if (err == MDBX_SUCCESS) {
        err = osal_openfile(MDBX_OPEN_DELETE, dummy_env, env_pathname.lck,
                            &clk_handle, 0);
        err = (err == MDBX_ENOFILE) ? MDBX_SUCCESS : err;
      }
      if (err == MDBX_SUCCESS && clk_handle != INVALID_HANDLE_VALUE)
        err = osal_lockfile(clk_handle, mode == MDBX_ENV_WAIT_FOR_UNUSED);
      if (err == MDBX_SUCCESS && dxb_handle != INVALID_HANDLE_VALUE)
        err = osal_lockfile(dxb_handle, mode == MDBX_ENV_WAIT_FOR_UNUSED);
    }

    if (err == MDBX_SUCCESS) {
      err = osal_removefile(env_pathname.dxb);
      if (err == MDBX_SUCCESS)
        rc = MDBX_SUCCESS;
      else if (err == MDBX_ENOFILE)
        err = MDBX_SUCCESS;
    }

    if (err == MDBX_SUCCESS) {
      err = osal_removefile(env_pathname.lck);
      if (err == MDBX_SUCCESS)
        rc = MDBX_SUCCESS;
      else if (err == MDBX_ENOFILE)
        err = MDBX_SUCCESS;
    }

    if (err == MDBX_SUCCESS && !(dummy_env->me_flags & MDBX_NOSUBDIR) &&
        (/* pathname != "." */ pathname[0] != '.' || pathname[1] != 0) &&
        (/* pathname != ".." */ pathname[0] != '.' || pathname[1] != '.' ||
         pathname[2] != 0)) {
      err = osal_removedirectory(pathname);
      if (err == MDBX_SUCCESS)
        rc = MDBX_SUCCESS;
      else if (err == MDBX_ENOFILE)
        err = MDBX_SUCCESS;
    }

    if (dxb_handle != INVALID_HANDLE_VALUE)
      osal_closefile(dxb_handle);
    if (clk_handle != INVALID_HANDLE_VALUE)
      osal_closefile(clk_handle);
  } else if (err == MDBX_ENOFILE)
    err = MDBX_SUCCESS;

  osal_free(env_pathname.buffer_for_free);
  return (err == MDBX_SUCCESS) ? rc : err;
}

__cold int mdbx_env_open(MDBX_env *env, const char *pathname,
                         MDBX_env_flags_t flags, mdbx_mode_t mode) {
#if defined(_WIN32) || defined(_WIN64)
  wchar_t *pathnameW = nullptr;
  int rc = osal_mb2w(pathname, &pathnameW);
  if (likely(rc == MDBX_SUCCESS)) {
    rc = mdbx_env_openW(env, pathnameW, flags, mode);
    osal_free(pathnameW);
    if (rc == MDBX_SUCCESS)
      /* force to make cache of the multi-byte pathname representation */
      mdbx_env_get_path(env, &pathname);
  }
  return rc;
}

__cold int mdbx_env_openW(MDBX_env *env, const wchar_t *pathname,
                          MDBX_env_flags_t flags, mdbx_mode_t mode) {
#endif /* Windows */

  int rc = check_env(env, false);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(flags & ~ENV_USABLE_FLAGS))
    return MDBX_EINVAL;

  if (unlikely(env->me_lazy_fd != INVALID_HANDLE_VALUE ||
               (env->me_flags & MDBX_ENV_ACTIVE) != 0 || env->me_map))
    return MDBX_EPERM;

  /* Pickup previously mdbx_env_set_flags(),
   * but avoid MDBX_UTTERLY_NOSYNC by disjunction */
  const uint32_t saved_me_flags = env->me_flags;
  flags = merge_sync_flags(flags | MDBX_DEPRECATED_COALESCE, env->me_flags);

  if (flags & MDBX_RDONLY) {
    /* Silently ignore irrelevant flags when we're only getting read access */
    flags &= ~(MDBX_WRITEMAP | MDBX_DEPRECATED_MAPASYNC | MDBX_SAFE_NOSYNC |
               MDBX_NOMETASYNC | MDBX_DEPRECATED_COALESCE | MDBX_LIFORECLAIM |
               MDBX_NOMEMINIT | MDBX_ACCEDE);
    mode = 0;
  } else {
#if MDBX_MMAP_INCOHERENT_FILE_WRITE
    /* Temporary `workaround` for OpenBSD kernel's flaw.
     * See https://libmdbx.dqdkfa.ru/dead-github/issues/67 */
    if ((flags & MDBX_WRITEMAP) == 0) {
      if (flags & MDBX_ACCEDE)
        flags |= MDBX_WRITEMAP;
      else {
        debug_log(MDBX_LOG_ERROR, __func__, __LINE__,
                  "System (i.e. OpenBSD) requires MDBX_WRITEMAP because "
                  "of an internal flaw(s) in a file/buffer/page cache.\n");
        return 42 /* ENOPROTOOPT */;
      }
    }
#endif /* MDBX_MMAP_INCOHERENT_FILE_WRITE */
  }

  MDBX_handle_env_pathname env_pathname;
  rc = handle_env_pathname(&env_pathname, pathname, &flags, mode);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout;

  env->me_flags = (flags & ~MDBX_FATAL_ERROR) | MDBX_ENV_ACTIVE;
  env->me_pathname = osal_calloc(env_pathname.ent_len + 1, sizeof(pathchar_t));
  env->me_dbxs = osal_calloc(env->me_maxdbs, sizeof(MDBX_dbx));
  env->me_dbflags = osal_calloc(env->me_maxdbs, sizeof(env->me_dbflags[0]));
  env->me_dbiseqs = osal_calloc(env->me_maxdbs, sizeof(env->me_dbiseqs[0]));
  if (!(env->me_dbxs && env->me_pathname && env->me_dbflags &&
        env->me_dbiseqs)) {
    rc = MDBX_ENOMEM;
    goto bailout;
  }
  memcpy(env->me_pathname, env_pathname.dxb,
         env_pathname.ent_len * sizeof(pathchar_t));
  env->me_dbxs[FREE_DBI].md_cmp = cmp_int_align4; /* aligned MDBX_INTEGERKEY */
  env->me_dbxs[FREE_DBI].md_dcmp = cmp_lenfast;
  env->me_dbxs[FREE_DBI].md_klen_max = env->me_dbxs[FREE_DBI].md_klen_min = 8;
  env->me_dbxs[FREE_DBI].md_vlen_min = 4;
  env->me_dbxs[FREE_DBI].md_vlen_max =
      mdbx_env_get_maxvalsize_ex(env, MDBX_INTEGERKEY);

  /* Использование O_DSYNC или FILE_FLAG_WRITE_THROUGH:
   *
   *   0) Если размер страниц БД меньше системной страницы ОЗУ, то ядру ОС
   *      придется чаще обновлять страницы в unified page cache.
   *
   *      Однако, O_DSYNC не предполагает отключение unified page cache,
   *      поэтому подобные затруднения будем считать проблемой ОС и/или
   *      ожидаемым пенальти из-за использования мелких страниц БД.
   *
   *   1) В режиме MDBX_SYNC_DURABLE - O_DSYNC для записи как данных,
   *      так и мета-страниц. Однако, на Linux отказ от O_DSYNC с последующим
   *      fdatasync() может быть выгоднее при использовании HDD, так как
   *      позволяет io-scheduler переупорядочить запись с учетом актуального
   *      расположения файла БД на носителе.
   *
   *   2) В режиме MDBX_NOMETASYNC - O_DSYNC можно использовать для данных,
   *      но в этом может не быть смысла, так как fdatasync() всё равно
   *      требуется для гарантии фиксации мета после предыдущей транзакции.
   *
   *      В итоге на нормальных системах (не Windows) есть два варианта:
   *       - при возможности O_DIRECT и/или io_ring для данных, скорее всего,
   *         есть смысл вызвать fdatasync() перед записью данных, а затем
   *         использовать O_DSYNC;
   *       - не использовать O_DSYNC и вызывать fdatasync() после записи данных.
   *
   *      На Windows же следует минимизировать использование FlushFileBuffers()
   *      из-за проблем с производительностью. Поэтому на Windows в режиме
   *      MDBX_NOMETASYNC:
   *       - мета обновляется через дескриптор без FILE_FLAG_WRITE_THROUGH;
   *       - перед началом записи данных вызывается FlushFileBuffers(), если
   *         mti_meta_sync_txnid не совпадает с последней записанной мета;
   *       - данные записываются через дескриптор с FILE_FLAG_WRITE_THROUGH.
   *
   *   3) В режиме MDBX_SAFE_NOSYNC - O_DSYNC нет смысла использовать, пока не
   *      будет реализована возможность полностью асинхронной "догоняющей"
   *      записи в выделенном процессе-сервере с io-ring очередями внутри.
   *
   * -----
   *
   * Использование O_DIRECT или FILE_FLAG_NO_BUFFERING:
   *
   *   Назначение этих флагов в отключении файлового дескриптора от
   *   unified page cache, т.е. от отображенных в память данных в случае
   *   libmdbx.
   *
   *   Поэтому, использование direct i/o в libmdbx без MDBX_WRITEMAP лишено
   *   смысла и контр-продуктивно, ибо так мы провоцируем ядро ОС на
   *   не-когерентность отображения в память с содержимым файла на носителе,
   *   либо требуем дополнительных проверок и действий направленных на
   *   фактическое отключение O_DIRECT для отображенных в память данных.
   *
   *   В режиме MDBX_WRITEMAP когерентность отображенных данных обеспечивается
   *   физически. Поэтому использование direct i/o может иметь смысл, если у
   *   ядра ОС есть какие-то проблемы с msync(), в том числе с
   *   производительностью:
   *    - использование io_ring или gather-write может быть дешевле, чем
   *      просмотр PTE ядром и запись измененных/грязных;
   *    - но проблема в том, что записываемые из user mode страницы либо не
   *      будут помечены чистыми (и соответственно будут записаны ядром
   *      еще раз), либо ядру необходимо искать и чистить PTE при получении
   *      запроса на запись.
   *
   *   Поэтому O_DIRECT или FILE_FLAG_NO_BUFFERING используется:
   *    - только в режиме MDBX_SYNC_DURABLE с MDBX_WRITEMAP;
   *    - когда me_psize >= me_os_psize;
   *    - опция сборки MDBX_AVOID_MSYNC != 0, которая по-умолчанию включена
   *      только на Windows (см ниже).
   *
   * -----
   *
   * Использование FILE_FLAG_OVERLAPPED на Windows:
   *
   * У Windows очень плохо с I/O (за исключением прямых постраничных
   * scatter/gather, которые работают в обход проблемного unified page
   * cache и поэтому почти бесполезны в libmdbx).
   *
   * При этом всё еще хуже при использовании FlushFileBuffers(), что также
   * требуется после FlushViewOfFile() в режиме MDBX_WRITEMAP. Поэтому
   * на Windows вместо FlushViewOfFile() и FlushFileBuffers() следует
   * использовать запись через дескриптор с FILE_FLAG_WRITE_THROUGH.
   *
   * В свою очередь, запись с FILE_FLAG_WRITE_THROUGH дешевле/быстрее
   * при использовании FILE_FLAG_OVERLAPPED. В результате, на Windows
   * в durable-режимах запись данных всегда в overlapped-режиме,
   * при этом для записи мета требуется отдельный не-overlapped дескриптор.
   */

  rc = osal_openfile((flags & MDBX_RDONLY) ? MDBX_OPEN_DXB_READ
                                           : MDBX_OPEN_DXB_LAZY,
                     env, env_pathname.dxb, &env->me_lazy_fd, mode);
  if (rc != MDBX_SUCCESS)
    goto bailout;

#if MDBX_LOCKING == MDBX_LOCKING_SYSV
  env->me_sysv_ipc.key = ftok(env_pathname.dxb, 42);
  if (env->me_sysv_ipc.key == -1) {
    rc = errno;
    goto bailout;
  }
#endif /* MDBX_LOCKING */

  /* Set the position in files outside of the data to avoid corruption
   * due to erroneous use of file descriptors in the application code. */
  const uint64_t safe_parking_lot_offset = UINT64_C(0x7fffFFFF80000000);
  osal_fseek(env->me_lazy_fd, safe_parking_lot_offset);

  env->me_fd4meta = env->me_lazy_fd;
#if defined(_WIN32) || defined(_WIN64)
  eASSERT(env, env->me_overlapped_fd == 0);
  bool ior_direct = false;
  if (!(flags &
        (MDBX_RDONLY | MDBX_SAFE_NOSYNC | MDBX_NOMETASYNC | MDBX_EXCLUSIVE))) {
    if (MDBX_AVOID_MSYNC && (flags & MDBX_WRITEMAP)) {
      /* Запрошен режим MDBX_SYNC_DURABLE | MDBX_WRITEMAP при активной опции
       * MDBX_AVOID_MSYNC.
       *
       * 1) В этой комбинации наиболее выгодно использовать WriteFileGather(),
       * но для этого необходимо открыть файл с флагом FILE_FLAG_NO_BUFFERING и
       * после обеспечивать выравнивание адресов и размера данных на границу
       * системной страницы, что в свою очередь возможно если размер страницы БД
       * не меньше размера системной страницы ОЗУ. Поэтому для открытия файла в
       * нужном режиме требуется знать размер страницы БД.
       *
       * 2) Кроме этого, в Windows запись в заблокированный регион файла
       * возможно только через тот-же дескриптор. Поэтому изначальный захват
       * блокировок посредством osal_lck_seize(), захват/освобождение блокировок
       * во время пишущих транзакций и запись данных должны выполнятся через
       * один дескриптор.
       *
       * Таким образом, требуется прочитать волатильный заголовок БД, чтобы
       * узнать размер страницы, чтобы открыть дескриптор файла в режиме нужном
       * для записи данных, чтобы использовать именно этот дескриптор для
       * изначального захвата блокировок. */
      MDBX_meta header;
      uint64_t dxb_filesize;
      int err = read_header(env, &header, MDBX_SUCCESS, true);
      if ((err == MDBX_SUCCESS && header.mm_psize >= env->me_os_psize) ||
          (err == MDBX_ENODATA && mode && env->me_psize >= env->me_os_psize &&
           osal_filesize(env->me_lazy_fd, &dxb_filesize) == MDBX_SUCCESS &&
           dxb_filesize == 0))
        /* Может быть коллизия, если два процесса пытаются одновременно создать
         * БД с разным размером страницы, который у одного меньше системной
         * страницы, а у другого НЕ меньше. Эта допустимая, но очень странная
         * ситуация. Поэтому считаем её ошибочной и не пытаемся разрешить. */
        ior_direct = true;
    }

    rc = osal_openfile(ior_direct ? MDBX_OPEN_DXB_OVERLAPPED_DIRECT
                                  : MDBX_OPEN_DXB_OVERLAPPED,
                       env, env_pathname.dxb, &env->me_overlapped_fd, 0);
    if (rc != MDBX_SUCCESS)
      goto bailout;
    env->me_data_lock_event = CreateEventW(nullptr, true, false, nullptr);
    if (!env->me_data_lock_event) {
      rc = (int)GetLastError();
      goto bailout;
    }
    osal_fseek(env->me_overlapped_fd, safe_parking_lot_offset);
  }
#else
  if (mode == 0) {
    /* pickup mode for lck-file */
    struct stat st;
    if (fstat(env->me_lazy_fd, &st)) {
      rc = errno;
      goto bailout;
    }
    mode = st.st_mode;
  }
  mode = (/* inherit read permissions for group and others */ mode &
          (S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH)) |
         /* always add read/write for owner */ S_IRUSR | S_IWUSR |
         ((mode & S_IRGRP) ? /* +write if readable by group */ S_IWGRP : 0) |
         ((mode & S_IROTH) ? /* +write if readable by others */ S_IWOTH : 0);
#endif /* !Windows */
  const int lck_rc = setup_lck(env, env_pathname.lck, mode);
  if (MDBX_IS_ERROR(lck_rc)) {
    rc = lck_rc;
    goto bailout;
  }
  osal_fseek(env->me_lfd, safe_parking_lot_offset);

  eASSERT(env, env->me_dsync_fd == INVALID_HANDLE_VALUE);
  if (!(flags & (MDBX_RDONLY | MDBX_SAFE_NOSYNC | MDBX_DEPRECATED_MAPASYNC
#if defined(_WIN32) || defined(_WIN64)
                 | MDBX_EXCLUSIVE
#endif /* !Windows */
                 ))) {
    rc = osal_openfile(MDBX_OPEN_DXB_DSYNC, env, env_pathname.dxb,
                       &env->me_dsync_fd, 0);
    if (MDBX_IS_ERROR(rc))
      goto bailout;
    if (env->me_dsync_fd != INVALID_HANDLE_VALUE) {
      if ((flags & MDBX_NOMETASYNC) == 0)
        env->me_fd4meta = env->me_dsync_fd;
      osal_fseek(env->me_dsync_fd, safe_parking_lot_offset);
    }
  }

  const MDBX_env_flags_t lazy_flags =
      MDBX_SAFE_NOSYNC | MDBX_UTTERLY_NOSYNC | MDBX_NOMETASYNC;
  const MDBX_env_flags_t mode_flags = lazy_flags | MDBX_LIFORECLAIM |
                                      MDBX_NORDAHEAD | MDBX_RDONLY |
                                      MDBX_WRITEMAP;

  MDBX_lockinfo *const lck = env->me_lck_mmap.lck;
  if (lck && lck_rc != MDBX_RESULT_TRUE && (env->me_flags & MDBX_RDONLY) == 0) {
    MDBX_env_flags_t snap_flags;
    while ((snap_flags = atomic_load32(&lck->mti_envmode, mo_AcquireRelease)) ==
           MDBX_RDONLY) {
      if (atomic_cas32(&lck->mti_envmode, MDBX_RDONLY,
                       (snap_flags = (env->me_flags & mode_flags)))) {
        /* The case:
         *  - let's assume that for some reason the DB file is smaller
         *    than it should be according to the geometry,
         *    but not smaller than the last page used;
         *  - the first process that opens the database (lck_rc == RESULT_TRUE)
         *    does this in readonly mode and therefore cannot bring
         *    the file size back to normal;
         *  - some next process (lck_rc != RESULT_TRUE) opens the DB in
         *    read-write mode and now is here.
         *
         * FIXME: Should we re-check and set the size of DB-file right here? */
        break;
      }
      atomic_yield();
    }

    if (env->me_flags & MDBX_ACCEDE) {
      /* Pickup current mode-flags (MDBX_LIFORECLAIM, MDBX_NORDAHEAD, etc). */
      const MDBX_env_flags_t diff =
          (snap_flags ^ env->me_flags) &
          ((snap_flags & lazy_flags) ? mode_flags
                                     : mode_flags & ~MDBX_WRITEMAP);
      env->me_flags ^= diff;
      NOTICE("accede mode-flags: 0x%X, 0x%X -> 0x%X", diff,
             env->me_flags ^ diff, env->me_flags);
    }

    /* Ранее упущенный не очевидный момент: При работе БД в режимах
     * не-синхронной/отложенной фиксации на диске, все процессы-писатели должны
     * иметь одинаковый режим MDBX_WRITEMAP.
     *
     * В противном случае, сброс на диск следует выполнять дважды: сначала
     * msync(), затем fdatasync(). При этом msync() не обязан отрабатывать
     * в процессах без MDBX_WRITEMAP, так как файл в память отображен только
     * для чтения. Поэтому, в общем случае, различия по MDBX_WRITEMAP не
     * позволяют выполнить фиксацию данных на диск, после их изменения в другом
     * процессе.
     *
     * В режиме MDBX_UTTERLY_NOSYNC позволять совместную работу с MDBX_WRITEMAP
     * также не следует, поскольку никакой процесс (в том числе последний) не
     * может гарантированно сбросить данные на диск, а следовательно не должен
     * помечать какую-либо транзакцию как steady.
     *
     * В результате, требуется либо запретить совместную работу процессам с
     * разным MDBX_WRITEMAP в режиме отложенной записи, либо отслеживать такое
     * смешивание и блокировать steady-пометки - что контрпродуктивно. */
    const MDBX_env_flags_t rigorous_flags =
        (snap_flags & lazy_flags)
            ? MDBX_SAFE_NOSYNC | MDBX_UTTERLY_NOSYNC | MDBX_WRITEMAP
            : MDBX_SAFE_NOSYNC | MDBX_UTTERLY_NOSYNC;
    const MDBX_env_flags_t rigorous_diff =
        (snap_flags ^ env->me_flags) & rigorous_flags;
    if (rigorous_diff) {
      ERROR("current mode/flags 0x%X incompatible with requested 0x%X, "
            "rigorous diff 0x%X",
            env->me_flags, snap_flags, rigorous_diff);
      rc = MDBX_INCOMPATIBLE;
      goto bailout;
    }
  }

  mincore_clean_cache(env);
  const int dxb_rc = setup_dxb(env, lck_rc, mode);
  if (MDBX_IS_ERROR(dxb_rc)) {
    rc = dxb_rc;
    goto bailout;
  }

  rc = osal_check_fs_incore(env->me_lazy_fd);
  env->me_incore = false;
  if (rc == MDBX_RESULT_TRUE) {
    env->me_incore = true;
    NOTICE("%s", "in-core database");
    rc = MDBX_SUCCESS;
  } else if (unlikely(rc != MDBX_SUCCESS)) {
    ERROR("check_fs_incore(), err %d", rc);
    goto bailout;
  }

  if (unlikely(/* recovery mode */ env->me_stuck_meta >= 0) &&
      (lck_rc != /* exclusive */ MDBX_RESULT_TRUE ||
       (flags & MDBX_EXCLUSIVE) == 0)) {
    ERROR("%s", "recovery requires exclusive mode");
    rc = MDBX_BUSY;
    goto bailout;
  }

  DEBUG("opened dbenv %p", (void *)env);
  if (!lck || lck_rc == MDBX_RESULT_TRUE) {
    env->me_lck->mti_envmode.weak = env->me_flags & mode_flags;
    env->me_lck->mti_meta_sync_txnid.weak =
        (uint32_t)recent_committed_txnid(env);
    env->me_lck->mti_reader_check_timestamp.weak = osal_monotime();
  }
  if (lck) {
    if (lck_rc == MDBX_RESULT_TRUE) {
      rc = osal_lck_downgrade(env);
      DEBUG("lck-downgrade-%s: rc %i",
            (env->me_flags & MDBX_EXCLUSIVE) ? "partial" : "full", rc);
      if (rc != MDBX_SUCCESS)
        goto bailout;
    } else {
      rc = cleanup_dead_readers(env, false, NULL);
      if (MDBX_IS_ERROR(rc))
        goto bailout;
    }

    if ((env->me_flags & MDBX_NOTLS) == 0) {
      rc = rthc_alloc(&env->me_txkey, &lck->mti_readers[0],
                      &lck->mti_readers[env->me_maxreaders]);
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;
      env->me_flags |= MDBX_ENV_TXKEY;
    }
  }

  if ((flags & MDBX_RDONLY) == 0) {
    const size_t tsize = sizeof(MDBX_txn) + sizeof(MDBX_cursor),
                 size = tsize + env->me_maxdbs *
                                    (sizeof(MDBX_db) + sizeof(MDBX_cursor *) +
                                     sizeof(MDBX_atomic_uint32_t) + 1);
    rc = alloc_page_buf(env);
    if (rc == MDBX_SUCCESS) {
      memset(env->me_pbuf, -1, env->me_psize * (size_t)2);
      memset(ptr_disp(env->me_pbuf, env->me_psize * (size_t)2), 0,
             env->me_psize);
      MDBX_txn *txn = osal_calloc(1, size);
      if (txn) {
        txn->mt_dbs = ptr_disp(txn, tsize);
        txn->mt_cursors =
            ptr_disp(txn->mt_dbs, sizeof(MDBX_db) * env->me_maxdbs);
        txn->mt_dbiseqs =
            ptr_disp(txn->mt_cursors, sizeof(MDBX_cursor *) * env->me_maxdbs);
        txn->mt_dbistate = ptr_disp(
            txn->mt_dbiseqs, sizeof(MDBX_atomic_uint32_t) * env->me_maxdbs);
        txn->mt_env = env;
        txn->mt_dbxs = env->me_dbxs;
        txn->mt_flags = MDBX_TXN_FINISHED;
        env->me_txn0 = txn;
        txn->tw.retired_pages = pnl_alloc(MDBX_PNL_INITIAL);
        txn->tw.relist = pnl_alloc(MDBX_PNL_INITIAL);
        if (unlikely(!txn->tw.retired_pages || !txn->tw.relist))
          rc = MDBX_ENOMEM;
      } else
        rc = MDBX_ENOMEM;
    }
    if (rc == MDBX_SUCCESS)
      rc = osal_ioring_create(&env->me_ioring
#if defined(_WIN32) || defined(_WIN64)
                              ,
                              ior_direct, env->me_overlapped_fd
#endif /* Windows */
      );
    if (rc == MDBX_SUCCESS)
      adjust_defaults(env);
  }

#if MDBX_DEBUG
  if (rc == MDBX_SUCCESS) {
    const meta_troika_t troika = meta_tap(env);
    const meta_ptr_t head = meta_recent(env, &troika);
    const MDBX_db *db = &head.ptr_c->mm_dbs[MAIN_DBI];

    DEBUG("opened database version %u, pagesize %u",
          (uint8_t)unaligned_peek_u64(4, head.ptr_c->mm_magic_and_version),
          env->me_psize);
    DEBUG("using meta page %" PRIaPGNO ", txn %" PRIaTXN,
          data_page(head.ptr_c)->mp_pgno, head.txnid);
    DEBUG("depth: %u", db->md_depth);
    DEBUG("entries: %" PRIu64, db->md_entries);
    DEBUG("branch pages: %" PRIaPGNO, db->md_branch_pages);
    DEBUG("leaf pages: %" PRIaPGNO, db->md_leaf_pages);
    DEBUG("large/overflow pages: %" PRIaPGNO, db->md_overflow_pages);
    DEBUG("root: %" PRIaPGNO, db->md_root);
    DEBUG("schema_altered: %" PRIaTXN, db->md_mod_txnid);
  }
#endif

bailout:
  if (rc != MDBX_SUCCESS) {
    rc = env_close(env) ? MDBX_PANIC : rc;
    env->me_flags =
        saved_me_flags | ((rc != MDBX_PANIC) ? 0 : MDBX_FATAL_ERROR);
  } else {
#if defined(MDBX_USE_VALGRIND) || defined(__SANITIZE_ADDRESS__)
    txn_valgrind(env, nullptr);
#endif /* MDBX_USE_VALGRIND || __SANITIZE_ADDRESS__ */
  }
  osal_free(env_pathname.buffer_for_free);
  return rc;
}

/* Destroy resources from mdbx_env_open(), clear our readers & DBIs */
__cold static int env_close(MDBX_env *env) {
  const unsigned flags = env->me_flags;
  if (!(flags & MDBX_ENV_ACTIVE)) {
    ENSURE(env, env->me_lcklist_next == nullptr);
    return MDBX_SUCCESS;
  }

  env->me_flags &= ~ENV_INTERNAL_FLAGS;
  if (flags & MDBX_ENV_TXKEY) {
    rthc_remove(env->me_txkey);
    env->me_txkey = (osal_thread_key_t)0;
  }

  munlock_all(env);
  if (!(env->me_flags & MDBX_RDONLY))
    osal_ioring_destroy(&env->me_ioring);

  lcklist_lock();
  const int rc = lcklist_detach_locked(env);
  lcklist_unlock();

  env->me_lck = nullptr;
  if (env->me_lck_mmap.lck)
    osal_munmap(&env->me_lck_mmap);

  if (env->me_map) {
    osal_munmap(&env->me_dxb_mmap);
#ifdef MDBX_USE_VALGRIND
    VALGRIND_DISCARD(env->me_valgrind_handle);
    env->me_valgrind_handle = -1;
#endif
  }

#if defined(_WIN32) || defined(_WIN64)
  eASSERT(env, !env->me_overlapped_fd ||
                   env->me_overlapped_fd == INVALID_HANDLE_VALUE);
  if (env->me_data_lock_event != INVALID_HANDLE_VALUE) {
    CloseHandle(env->me_data_lock_event);
    env->me_data_lock_event = INVALID_HANDLE_VALUE;
  }
#endif /* Windows */

  if (env->me_dsync_fd != INVALID_HANDLE_VALUE) {
    (void)osal_closefile(env->me_dsync_fd);
    env->me_dsync_fd = INVALID_HANDLE_VALUE;
  }

  if (env->me_lazy_fd != INVALID_HANDLE_VALUE) {
    (void)osal_closefile(env->me_lazy_fd);
    env->me_lazy_fd = INVALID_HANDLE_VALUE;
  }

  if (env->me_lfd != INVALID_HANDLE_VALUE) {
    (void)osal_closefile(env->me_lfd);
    env->me_lfd = INVALID_HANDLE_VALUE;
  }

  if (env->me_dbxs) {
    for (size_t i = CORE_DBS; i < env->me_numdbs; ++i)
      if (env->me_dbxs[i].md_name.iov_len)
        osal_free(env->me_dbxs[i].md_name.iov_base);
    osal_free(env->me_dbxs);
    env->me_numdbs = CORE_DBS;
    env->me_dbxs = nullptr;
  }
  if (env->me_pbuf) {
    osal_memalign_free(env->me_pbuf);
    env->me_pbuf = nullptr;
  }
  if (env->me_dbiseqs) {
    osal_free(env->me_dbiseqs);
    env->me_dbiseqs = nullptr;
  }
  if (env->me_dbflags) {
    osal_free(env->me_dbflags);
    env->me_dbflags = nullptr;
  }
  if (env->me_pathname) {
    osal_free(env->me_pathname);
    env->me_pathname = nullptr;
  }
#if defined(_WIN32) || defined(_WIN64)
  if (env->me_pathname_char) {
    osal_free(env->me_pathname_char);
    env->me_pathname_char = nullptr;
  }
#endif /* Windows */
  if (env->me_txn0) {
    dpl_free(env->me_txn0);
    txl_free(env->me_txn0->tw.lifo_reclaimed);
    pnl_free(env->me_txn0->tw.retired_pages);
    pnl_free(env->me_txn0->tw.spilled.list);
    pnl_free(env->me_txn0->tw.relist);
    osal_free(env->me_txn0);
    env->me_txn0 = nullptr;
  }
  env->me_stuck_meta = -1;
  return rc;
}

__cold int mdbx_env_close_ex(MDBX_env *env, bool dont_sync) {
  MDBX_page *dp;
  int rc = MDBX_SUCCESS;

  if (unlikely(!env))
    return MDBX_EINVAL;

  if (unlikely(env->me_signature.weak != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

#if MDBX_ENV_CHECKPID || !(defined(_WIN32) || defined(_WIN64))
  /* Check the PID even if MDBX_ENV_CHECKPID=0 on non-Windows
   * platforms (i.e. where fork() is available).
   * This is required to legitimize a call after fork()
   * from a child process, that should be allowed to free resources. */
  if (unlikely(env->me_pid != osal_getpid()))
    env->me_flags |= MDBX_FATAL_ERROR;
#endif /* MDBX_ENV_CHECKPID */

  if (env->me_map && (env->me_flags & (MDBX_RDONLY | MDBX_FATAL_ERROR)) == 0 &&
      env->me_txn0) {
    if (env->me_txn0->mt_owner && env->me_txn0->mt_owner != osal_thread_self())
      return MDBX_BUSY;
  } else
    dont_sync = true;

  if (!atomic_cas32(&env->me_signature, MDBX_ME_SIGNATURE, 0))
    return MDBX_EBADSIGN;

  if (!dont_sync) {
#if defined(_WIN32) || defined(_WIN64)
    /* On windows, without blocking is impossible to determine whether another
     * process is running a writing transaction or not.
     * Because in the "owner died" condition kernel don't release
     * file lock immediately. */
    rc = env_sync(env, true, false);
    rc = (rc == MDBX_RESULT_TRUE) ? MDBX_SUCCESS : rc;
#else
    struct stat st;
    if (unlikely(fstat(env->me_lazy_fd, &st)))
      rc = errno;
    else if (st.st_nlink > 0 /* don't sync deleted files */) {
      rc = env_sync(env, true, true);
      rc = (rc == MDBX_BUSY || rc == EAGAIN || rc == EACCES || rc == EBUSY ||
            rc == EWOULDBLOCK || rc == MDBX_RESULT_TRUE)
               ? MDBX_SUCCESS
               : rc;
    }
#endif /* Windows */
  }

  eASSERT(env, env->me_signature.weak == 0);
  rc = env_close(env) ? MDBX_PANIC : rc;
  ENSURE(env, osal_fastmutex_destroy(&env->me_dbi_lock) == MDBX_SUCCESS);
#if defined(_WIN32) || defined(_WIN64)
  /* me_remap_guard don't have destructor (Slim Reader/Writer Lock) */
  DeleteCriticalSection(&env->me_windowsbug_lock);
#else
  ENSURE(env, osal_fastmutex_destroy(&env->me_remap_guard) == MDBX_SUCCESS);
#endif /* Windows */

#if MDBX_LOCKING > MDBX_LOCKING_SYSV
  MDBX_lockinfo *const stub = lckless_stub(env);
  ENSURE(env, osal_ipclock_destroy(&stub->mti_wlock) == 0);
#endif /* MDBX_LOCKING */

  while ((dp = env->me_dp_reserve) != NULL) {
    MDBX_ASAN_UNPOISON_MEMORY_REGION(dp, env->me_psize);
    VALGRIND_MAKE_MEM_DEFINED(&mp_next(dp), sizeof(MDBX_page *));
    env->me_dp_reserve = mp_next(dp);
    void *const ptr = ptr_disp(dp, -(ptrdiff_t)sizeof(size_t));
    osal_free(ptr);
  }
  VALGRIND_DESTROY_MEMPOOL(env);
  ENSURE(env, env->me_lcklist_next == nullptr);
  env->me_pid = 0;
  osal_free(env);

  return rc;
}

/* Search for key within a page, using binary search.
 * Returns the smallest entry larger or equal to the key.
 * Updates the cursor index with the index of the found entry.
 * If no entry larger or equal to the key is found, returns NULL. */
__hot static struct node_result node_search(MDBX_cursor *mc,
                                            const MDBX_val *key) {
  MDBX_page *mp = mc->mc_pg[mc->mc_top];
  const intptr_t nkeys = page_numkeys(mp);
  DKBUF_DEBUG;

  DEBUG("searching %zu keys in %s %spage %" PRIaPGNO, nkeys,
        IS_LEAF(mp) ? "leaf" : "branch", IS_SUBP(mp) ? "sub-" : "",
        mp->mp_pgno);

  struct node_result ret;
  ret.exact = false;
  STATIC_ASSERT(P_BRANCH == 1);
  intptr_t low = mp->mp_flags & P_BRANCH;
  intptr_t high = nkeys - 1;
  if (unlikely(high < low)) {
    mc->mc_ki[mc->mc_top] = 0;
    ret.node = NULL;
    return ret;
  }

  intptr_t i;
  MDBX_cmp_func *cmp = mc->mc_dbx->md_cmp;
  MDBX_val nodekey;
  if (unlikely(IS_LEAF2(mp))) {
    cASSERT(mc, mp->mp_leaf2_ksize == mc->mc_db->md_xsize);
    nodekey.iov_len = mp->mp_leaf2_ksize;
    do {
      i = (low + high) >> 1;
      nodekey.iov_base = page_leaf2key(mp, i, nodekey.iov_len);
      cASSERT(mc, ptr_disp(mp, mc->mc_txn->mt_env->me_psize) >=
                      ptr_disp(nodekey.iov_base, nodekey.iov_len));
      int cr = cmp(key, &nodekey);
      DEBUG("found leaf index %zu [%s], rc = %i", i, DKEY_DEBUG(&nodekey), cr);
      if (cr > 0)
        /* Found entry is less than the key. */
        /* Skip to get the smallest entry larger than key. */
        low = ++i;
      else if (cr < 0)
        high = i - 1;
      else {
        ret.exact = true;
        break;
      }
    } while (likely(low <= high));

    /* store the key index */
    mc->mc_ki[mc->mc_top] = (indx_t)i;
    ret.node = (i < nkeys)
                   ? /* fake for LEAF2 */ (MDBX_node *)(intptr_t)-1
                   : /* There is no entry larger or equal to the key. */ NULL;
    return ret;
  }

  if (IS_BRANCH(mp) && cmp == cmp_int_align2)
    /* Branch pages have no data, so if using integer keys,
     * alignment is guaranteed. Use faster cmp_int_align4(). */
    cmp = cmp_int_align4;

  MDBX_node *node;
  do {
    i = (low + high) >> 1;
    node = page_node(mp, i);
    nodekey.iov_len = node_ks(node);
    nodekey.iov_base = node_key(node);
    cASSERT(mc, ptr_disp(mp, mc->mc_txn->mt_env->me_psize) >=
                    ptr_disp(nodekey.iov_base, nodekey.iov_len));
    int cr = cmp(key, &nodekey);
    if (IS_LEAF(mp))
      DEBUG("found leaf index %zu [%s], rc = %i", i, DKEY_DEBUG(&nodekey), cr);
    else
      DEBUG("found branch index %zu [%s -> %" PRIaPGNO "], rc = %i", i,
            DKEY_DEBUG(&nodekey), node_pgno(node), cr);
    if (cr > 0)
      /* Found entry is less than the key. */
      /* Skip to get the smallest entry larger than key. */
      low = ++i;
    else if (cr < 0)
      high = i - 1;
    else {
      ret.exact = true;
      break;
    }
  } while (likely(low <= high));

  /* store the key index */
  mc->mc_ki[mc->mc_top] = (indx_t)i;
  ret.node = (i < nkeys)
                 ? page_node(mp, i)
                 : /* There is no entry larger or equal to the key. */ NULL;
  return ret;
}

/* Pop a page off the top of the cursor's stack. */
static __inline void cursor_pop(MDBX_cursor *mc) {
  if (likely(mc->mc_snum)) {
    DEBUG("popped page %" PRIaPGNO " off db %d cursor %p",
          mc->mc_pg[mc->mc_top]->mp_pgno, DDBI(mc), (void *)mc);
    if (likely(--mc->mc_snum)) {
      mc->mc_top--;
    } else {
      mc->mc_flags &= ~C_INITIALIZED;
    }
  }
}

/* Push a page onto the top of the cursor's stack.
 * Set MDBX_TXN_ERROR on failure. */
static __inline int cursor_push(MDBX_cursor *mc, MDBX_page *mp) {
  DEBUG("pushing page %" PRIaPGNO " on db %d cursor %p", mp->mp_pgno, DDBI(mc),
        (void *)mc);

  if (unlikely(mc->mc_snum >= CURSOR_STACK)) {
    mc->mc_txn->mt_flags |= MDBX_TXN_ERROR;
    return MDBX_CURSOR_FULL;
  }

  mc->mc_top = mc->mc_snum++;
  mc->mc_pg[mc->mc_top] = mp;
  mc->mc_ki[mc->mc_top] = 0;
  return MDBX_SUCCESS;
}

__hot static __always_inline int page_get_checker_lite(const uint16_t ILL,
                                                       const MDBX_page *page,
                                                       MDBX_txn *const txn,
                                                       const txnid_t front) {
  if (unlikely(page->mp_flags & ILL)) {
    if (ILL == P_ILL_BITS || (page->mp_flags & P_ILL_BITS))
      return bad_page(page, "invalid page's flags (%u)\n", page->mp_flags);
    else if (ILL & P_OVERFLOW) {
      assert((ILL & (P_BRANCH | P_LEAF | P_LEAF2)) == 0);
      assert(page->mp_flags & (P_BRANCH | P_LEAF | P_LEAF2));
      return bad_page(page, "unexpected %s instead of %s (%u)\n",
                      "large/overflow", "branch/leaf/leaf2", page->mp_flags);
    } else if (ILL & (P_BRANCH | P_LEAF | P_LEAF2)) {
      assert((ILL & P_BRANCH) && (ILL & P_LEAF) && (ILL & P_LEAF2));
      assert(page->mp_flags & (P_BRANCH | P_LEAF | P_LEAF2));
      return bad_page(page, "unexpected %s instead of %s (%u)\n",
                      "branch/leaf/leaf2", "large/overflow", page->mp_flags);
    } else {
      assert(false);
    }
  }

  if (unlikely(page->mp_txnid > front) &&
      unlikely(page->mp_txnid > txn->mt_front || front < txn->mt_txnid))
    return bad_page(
        page,
        "invalid page' txnid (%" PRIaTXN ") for %s' txnid (%" PRIaTXN ")\n",
        page->mp_txnid,
        (front == txn->mt_front && front != txn->mt_txnid) ? "front-txn"
                                                           : "parent-page",
        front);

  if (((ILL & P_OVERFLOW) || !IS_OVERFLOW(page)) &&
      (ILL & (P_BRANCH | P_LEAF | P_LEAF2)) == 0) {
    /* Контроль четности page->mp_upper тут либо приводит к ложным ошибкам,
     * либо слишком дорог по количеству операций. Заковырка в том, что mp_upper
     * может быть нечетным на LEAF2-страницах, при нечетном количестве элементов
     * нечетной длины. Поэтому четность page->mp_upper здесь не проверяется, но
     * соответствующие полные проверки есть в page_check(). */
    if (unlikely(page->mp_upper < page->mp_lower || (page->mp_lower & 1) ||
                 PAGEHDRSZ + page->mp_upper > txn->mt_env->me_psize))
      return bad_page(page,
                      "invalid page' lower(%u)/upper(%u) with limit %zu\n",
                      page->mp_lower, page->mp_upper, page_space(txn->mt_env));

  } else if ((ILL & P_OVERFLOW) == 0) {
    const pgno_t npages = page->mp_pages;
    if (unlikely(npages < 1) || unlikely(npages >= MAX_PAGENO / 2))
      return bad_page(page, "invalid n-pages (%u) for large-page\n", npages);
    if (unlikely(page->mp_pgno + npages > txn->mt_next_pgno))
      return bad_page(
          page,
          "end of large-page beyond (%u) allocated space (%u next-pgno)\n",
          page->mp_pgno + npages, txn->mt_next_pgno);
  } else {
    assert(false);
  }
  return MDBX_SUCCESS;
}

__cold static __noinline pgr_t
page_get_checker_full(const uint16_t ILL, MDBX_page *page,
                      const MDBX_cursor *const mc, const txnid_t front) {
  pgr_t r = {page, page_get_checker_lite(ILL, page, mc->mc_txn, front)};
  if (likely(r.err == MDBX_SUCCESS))
    r.err = page_check(mc, page);
  if (unlikely(r.err != MDBX_SUCCESS))
    mc->mc_txn->mt_flags |= MDBX_TXN_ERROR;
  return r;
}

__hot static __always_inline pgr_t page_get_inline(const uint16_t ILL,
                                                   const MDBX_cursor *const mc,
                                                   const pgno_t pgno,
                                                   const txnid_t front) {
  MDBX_txn *const txn = mc->mc_txn;
  tASSERT(txn, front <= txn->mt_front);

  pgr_t r;
  if (unlikely(pgno >= txn->mt_next_pgno)) {
    ERROR("page #%" PRIaPGNO " beyond next-pgno", pgno);
    r.page = nullptr;
    r.err = MDBX_PAGE_NOTFOUND;
  bailout:
    txn->mt_flags |= MDBX_TXN_ERROR;
    return r;
  }

  eASSERT(txn->mt_env,
          ((txn->mt_flags ^ txn->mt_env->me_flags) & MDBX_WRITEMAP) == 0);
  r.page = pgno2page(txn->mt_env, pgno);
  if ((txn->mt_flags & (MDBX_TXN_RDONLY | MDBX_WRITEMAP)) == 0) {
    const MDBX_txn *spiller = txn;
    do {
      /* Spilled pages were dirtied in this txn and flushed
       * because the dirty list got full. Bring this page
       * back in from the map (but don't unspill it here,
       * leave that unless page_touch happens again). */
      if (unlikely(spiller->mt_flags & MDBX_TXN_SPILLS) &&
          search_spilled(spiller, pgno))
        break;

      const size_t i = dpl_search(spiller, pgno);
      tASSERT(txn, (intptr_t)i > 0);
      if (spiller->tw.dirtylist->items[i].pgno == pgno) {
        r.page = spiller->tw.dirtylist->items[i].ptr;
        break;
      }

      spiller = spiller->mt_parent;
    } while (spiller);
  }

  if (unlikely(r.page->mp_pgno != pgno)) {
    r.err = bad_page(
        r.page, "pgno mismatch (%" PRIaPGNO ") != expected (%" PRIaPGNO ")\n",
        r.page->mp_pgno, pgno);
    goto bailout;
  }

  if (unlikely(mc->mc_checking & CC_PAGECHECK))
    return page_get_checker_full(ILL, r.page, mc, front);

#if MDBX_DISABLE_VALIDATION
  r.err = MDBX_SUCCESS;
#else
  r.err = page_get_checker_lite(ILL, r.page, txn, front);
  if (unlikely(r.err != MDBX_SUCCESS))
    goto bailout;
#endif /* MDBX_DISABLE_VALIDATION */
  return r;
}

/* Finish mdbx_page_search() / mdbx_page_search_lowest().
 * The cursor is at the root page, set up the rest of it. */
__hot __noinline static int page_search_root(MDBX_cursor *mc,
                                             const MDBX_val *key, int flags) {
  MDBX_page *mp = mc->mc_pg[mc->mc_top];
  int rc;
  DKBUF_DEBUG;

  while (IS_BRANCH(mp)) {
    MDBX_node *node;
    intptr_t i;

    DEBUG("branch page %" PRIaPGNO " has %zu keys", mp->mp_pgno,
          page_numkeys(mp));
    /* Don't assert on branch pages in the GC. We can get here
     * while in the process of rebalancing a GC branch page; we must
     * let that proceed. ITS#8336 */
    cASSERT(mc, !mc->mc_dbi || page_numkeys(mp) > 1);
    DEBUG("found index 0 to page %" PRIaPGNO, node_pgno(page_node(mp, 0)));

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
      const struct node_result nsr = node_search(mc, key);
      if (likely(nsr.node))
        i = mc->mc_ki[mc->mc_top] + (intptr_t)nsr.exact - 1;
      else
        i = page_numkeys(mp) - 1;
      DEBUG("following index %zu for key [%s]", i, DKEY_DEBUG(key));
    }

    cASSERT(mc, i >= 0 && i < (int)page_numkeys(mp));
    node = page_node(mp, i);

    rc = page_get(mc, node_pgno(node), &mp, mp->mp_txnid);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;

    mc->mc_ki[mc->mc_top] = (indx_t)i;
    if (unlikely(rc = cursor_push(mc, mp)))
      return rc;

  ready:
    if (flags & MDBX_PS_MODIFY) {
      rc = page_touch(mc);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
      mp = mc->mc_pg[mc->mc_top];
    }
  }

  if (!MDBX_DISABLE_VALIDATION && unlikely(!CHECK_LEAF_TYPE(mc, mp))) {
    ERROR("unexpected leaf-page #%" PRIaPGNO " type 0x%x seen by cursor",
          mp->mp_pgno, mp->mp_flags);
    return MDBX_CORRUPTED;
  }

  DEBUG("found leaf page %" PRIaPGNO " for key [%s]", mp->mp_pgno,
        DKEY_DEBUG(key));
  mc->mc_flags |= C_INITIALIZED;
  mc->mc_flags &= ~C_EOF;

  return MDBX_SUCCESS;
}

static int setup_dbx(MDBX_dbx *const dbx, const MDBX_db *const db,
                     const unsigned pagesize) {
  if (unlikely(!dbx->md_cmp)) {
    dbx->md_cmp = get_default_keycmp(db->md_flags);
    dbx->md_dcmp = get_default_datacmp(db->md_flags);
  }

  dbx->md_klen_min =
      (db->md_flags & MDBX_INTEGERKEY) ? 4 /* sizeof(uint32_t) */ : 0;
  dbx->md_klen_max = keysize_max(pagesize, db->md_flags);
  assert(dbx->md_klen_max != (unsigned)-1);

  dbx->md_vlen_min = (db->md_flags & MDBX_INTEGERDUP)
                         ? 4 /* sizeof(uint32_t) */
                         : ((db->md_flags & MDBX_DUPFIXED) ? 1 : 0);
  dbx->md_vlen_max = valsize_max(pagesize, db->md_flags);
  assert(dbx->md_vlen_max != (size_t)-1);

  if ((db->md_flags & (MDBX_DUPFIXED | MDBX_INTEGERDUP)) != 0 && db->md_xsize) {
    if (!MDBX_DISABLE_VALIDATION && unlikely(db->md_xsize < dbx->md_vlen_min ||
                                             db->md_xsize > dbx->md_vlen_max)) {
      ERROR("db.md_xsize (%u) <> min/max value-length (%zu/%zu)", db->md_xsize,
            dbx->md_vlen_min, dbx->md_vlen_max);
      return MDBX_CORRUPTED;
    }
    dbx->md_vlen_min = dbx->md_vlen_max = db->md_xsize;
  }
  return MDBX_SUCCESS;
}

static int fetch_sdb(MDBX_txn *txn, size_t dbi) {
  MDBX_cursor_couple couple;
  if (unlikely(dbi_changed(txn, dbi))) {
    NOTICE("dbi %zu was changed for txn %" PRIaTXN, dbi, txn->mt_txnid);
    return MDBX_BAD_DBI;
  }
  int rc = cursor_init(&couple.outer, txn, MAIN_DBI);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  MDBX_dbx *const dbx = &txn->mt_dbxs[dbi];
  rc = page_search(&couple.outer, &dbx->md_name, 0);
  if (unlikely(rc != MDBX_SUCCESS)) {
  notfound:
    NOTICE("dbi %zu refs to inaccessible subDB `%*s` for txn %" PRIaTXN
           " (err %d)",
           dbi, (int)dbx->md_name.iov_len, (const char *)dbx->md_name.iov_base,
           txn->mt_txnid, rc);
    return (rc == MDBX_NOTFOUND) ? MDBX_BAD_DBI : rc;
  }

  MDBX_val data;
  struct node_result nsr = node_search(&couple.outer, &dbx->md_name);
  if (unlikely(!nsr.exact)) {
    rc = MDBX_NOTFOUND;
    goto notfound;
  }
  if (unlikely((node_flags(nsr.node) & (F_DUPDATA | F_SUBDATA)) != F_SUBDATA)) {
    NOTICE("dbi %zu refs to not a named subDB `%*s` for txn %" PRIaTXN " (%s)",
           dbi, (int)dbx->md_name.iov_len, (const char *)dbx->md_name.iov_base,
           txn->mt_txnid, "wrong flags");
    return MDBX_INCOMPATIBLE; /* not a named DB */
  }

  rc = node_read(&couple.outer, nsr.node, &data,
                 couple.outer.mc_pg[couple.outer.mc_top]);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(data.iov_len != sizeof(MDBX_db))) {
    NOTICE("dbi %zu refs to not a named subDB `%*s` for txn %" PRIaTXN " (%s)",
           dbi, (int)dbx->md_name.iov_len, (const char *)dbx->md_name.iov_base,
           txn->mt_txnid, "wrong rec-size");
    return MDBX_INCOMPATIBLE; /* not a named DB */
  }

  uint16_t md_flags = UNALIGNED_PEEK_16(data.iov_base, MDBX_db, md_flags);
  /* The txn may not know this DBI, or another process may
   * have dropped and recreated the DB with other flags. */
  MDBX_db *const db = &txn->mt_dbs[dbi];
  if (unlikely((db->md_flags & DB_PERSISTENT_FLAGS) != md_flags)) {
    NOTICE("dbi %zu refs to the re-created subDB `%*s` for txn %" PRIaTXN
           " with different flags (present 0x%X != wanna 0x%X)",
           dbi, (int)dbx->md_name.iov_len, (const char *)dbx->md_name.iov_base,
           txn->mt_txnid, db->md_flags & DB_PERSISTENT_FLAGS, md_flags);
    return MDBX_INCOMPATIBLE;
  }

  memcpy(db, data.iov_base, sizeof(MDBX_db));
#if !MDBX_DISABLE_VALIDATION
  const txnid_t pp_txnid = couple.outer.mc_pg[couple.outer.mc_top]->mp_txnid;
  tASSERT(txn, txn->mt_front >= pp_txnid);
  if (unlikely(db->md_mod_txnid > pp_txnid)) {
    ERROR("db.md_mod_txnid (%" PRIaTXN ") > page-txnid (%" PRIaTXN ")",
          db->md_mod_txnid, pp_txnid);
    return MDBX_CORRUPTED;
  }
#endif /* !MDBX_DISABLE_VALIDATION */
  rc = setup_dbx(dbx, db, txn->mt_env->me_psize);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  txn->mt_dbistate[dbi] &= ~DBI_STALE;
  return MDBX_SUCCESS;
}

/* Search for the lowest key under the current branch page.
 * This just bypasses a numkeys check in the current page
 * before calling mdbx_page_search_root(), because the callers
 * are all in situations where the current page is known to
 * be underfilled. */
__hot static int page_search_lowest(MDBX_cursor *mc) {
  MDBX_page *mp = mc->mc_pg[mc->mc_top];
  cASSERT(mc, IS_BRANCH(mp));
  MDBX_node *node = page_node(mp, 0);

  int rc = page_get(mc, node_pgno(node), &mp, mp->mp_txnid);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  mc->mc_ki[mc->mc_top] = 0;
  if (unlikely(rc = cursor_push(mc, mp)))
    return rc;
  return page_search_root(mc, NULL, MDBX_PS_FIRST);
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
__hot static int page_search(MDBX_cursor *mc, const MDBX_val *key, int flags) {
  int rc;
  pgno_t root;

  /* Make sure the txn is still viable, then find the root from
   * the txn's db table and set it as the root of the cursor's stack. */
  if (unlikely(mc->mc_txn->mt_flags & MDBX_TXN_BLOCKED)) {
    DEBUG("%s", "transaction has failed, must abort");
    return MDBX_BAD_TXN;
  }

  /* Make sure we're using an up-to-date root */
  if (unlikely(*mc->mc_dbistate & DBI_STALE)) {
    rc = fetch_sdb(mc->mc_txn, mc->mc_dbi);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }
  root = mc->mc_db->md_root;

  if (unlikely(root == P_INVALID)) { /* Tree is empty. */
    DEBUG("%s", "tree is empty");
    return MDBX_NOTFOUND;
  }

  cASSERT(mc, root >= NUM_METAS);
  if (!mc->mc_snum || !(mc->mc_flags & C_INITIALIZED) ||
      mc->mc_pg[0]->mp_pgno != root) {
    txnid_t pp_txnid = mc->mc_db->md_mod_txnid;
    pp_txnid = /* mc->mc_db->md_mod_txnid maybe zero in a legacy DB */ pp_txnid
                   ? pp_txnid
                   : mc->mc_txn->mt_txnid;
    if ((mc->mc_txn->mt_flags & MDBX_TXN_RDONLY) == 0) {
      MDBX_txn *scan = mc->mc_txn;
      do
        if ((scan->mt_flags & MDBX_TXN_DIRTY) &&
            (mc->mc_dbi == MAIN_DBI ||
             (scan->mt_dbistate[mc->mc_dbi] & DBI_DIRTY))) {
          /* После коммита вложенных тразакций может быть mod_txnid > front */
          pp_txnid = scan->mt_front;
          break;
        }
      while (unlikely((scan = scan->mt_parent) != nullptr));
    }
    if (unlikely((rc = page_get(mc, root, &mc->mc_pg[0], pp_txnid)) != 0))
      return rc;
  }

  mc->mc_snum = 1;
  mc->mc_top = 0;

  DEBUG("db %d root page %" PRIaPGNO " has flags 0x%X", DDBI(mc), root,
        mc->mc_pg[0]->mp_flags);

  if (flags & MDBX_PS_MODIFY) {
    if (unlikely(rc = page_touch(mc)))
      return rc;
  }

  if (flags & MDBX_PS_ROOTONLY)
    return MDBX_SUCCESS;

  return page_search_root(mc, key, flags);
}

/* Read large/overflow node data. */
static __noinline int node_read_bigdata(MDBX_cursor *mc, const MDBX_node *node,
                                        MDBX_val *data, const MDBX_page *mp) {
  cASSERT(mc, node_flags(node) == F_BIGDATA && data->iov_len == node_ds(node));

  pgr_t lp = page_get_large(mc, node_largedata_pgno(node), mp->mp_txnid);
  if (unlikely((lp.err != MDBX_SUCCESS))) {
    DEBUG("read large/overflow page %" PRIaPGNO " failed",
          node_largedata_pgno(node));
    return lp.err;
  }

  cASSERT(mc, PAGETYPE_WHOLE(lp.page) == P_OVERFLOW);
  data->iov_base = page_data(lp.page);
  if (!MDBX_DISABLE_VALIDATION) {
    const MDBX_env *env = mc->mc_txn->mt_env;
    const size_t dsize = data->iov_len;
    const unsigned npages = number_of_ovpages(env, dsize);
    if (unlikely(lp.page->mp_pages < npages))
      return bad_page(lp.page,
                      "too less n-pages %u for bigdata-node (%zu bytes)",
                      lp.page->mp_pages, dsize);
  }
  return MDBX_SUCCESS;
}

/* Return the data associated with a given node. */
static __always_inline int node_read(MDBX_cursor *mc, const MDBX_node *node,
                                     MDBX_val *data, const MDBX_page *mp) {
  data->iov_len = node_ds(node);
  data->iov_base = node_data(node);
  if (likely(node_flags(node) != F_BIGDATA))
    return MDBX_SUCCESS;
  return node_read_bigdata(mc, node, data, mp);
}

int mdbx_get(const MDBX_txn *txn, MDBX_dbi dbi, const MDBX_val *key,
             MDBX_val *data) {
  DKBUF_DEBUG;
  DEBUG("===> get db %u key [%s]", dbi, DKEY_DEBUG(key));

  int rc = check_txn(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!key || !data))
    return MDBX_EINVAL;

  if (unlikely(!check_dbi(txn, dbi, DBI_USRVALID)))
    return MDBX_BAD_DBI;

  MDBX_cursor_couple cx;
  rc = cursor_init(&cx.outer, txn, dbi);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  return cursor_set(&cx.outer, (MDBX_val *)key, data, MDBX_SET).err;
}

int mdbx_get_equal_or_great(const MDBX_txn *txn, MDBX_dbi dbi, MDBX_val *key,
                            MDBX_val *data) {
  int rc = check_txn(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!key || !data))
    return MDBX_EINVAL;

  if (unlikely(!check_dbi(txn, dbi, DBI_USRVALID)))
    return MDBX_BAD_DBI;

  if (unlikely(txn->mt_flags & MDBX_TXN_BLOCKED))
    return MDBX_BAD_TXN;

  MDBX_cursor_couple cx;
  rc = cursor_init(&cx.outer, txn, dbi);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  return cursor_get(&cx.outer, key, data, MDBX_SET_LOWERBOUND);
}

int mdbx_get_ex(const MDBX_txn *txn, MDBX_dbi dbi, MDBX_val *key,
                MDBX_val *data, size_t *values_count) {
  DKBUF_DEBUG;
  DEBUG("===> get db %u key [%s]", dbi, DKEY_DEBUG(key));

  int rc = check_txn(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!key || !data))
    return MDBX_EINVAL;

  if (unlikely(!check_dbi(txn, dbi, DBI_USRVALID)))
    return MDBX_BAD_DBI;

  MDBX_cursor_couple cx;
  rc = cursor_init(&cx.outer, txn, dbi);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  rc = cursor_set(&cx.outer, key, data, MDBX_SET_KEY).err;
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
      if (node_flags(node) & F_DUPDATA) {
        // coverity[uninit_use : FALSE]
        tASSERT(txn, cx.outer.mc_xcursor == &cx.inner &&
                         (cx.inner.mx_cursor.mc_flags & C_INITIALIZED));
        // coverity[uninit_use : FALSE]
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
 * [in] mc    The cursor for this operation.
 * [in] dir   SIBLING_LEFT or SIBLING_RIGHT.
 *
 * Returns 0 on success, non-zero on failure. */
static int cursor_sibling(MDBX_cursor *mc, int dir) {
  int rc;
  MDBX_node *node;
  MDBX_page *mp;
  assert(dir == SIBLING_LEFT || dir == SIBLING_RIGHT);

  if (unlikely(mc->mc_snum < 2))
    return MDBX_NOTFOUND; /* root has no siblings */

  cursor_pop(mc);
  DEBUG("parent page is page %" PRIaPGNO ", index %u",
        mc->mc_pg[mc->mc_top]->mp_pgno, mc->mc_ki[mc->mc_top]);

  if ((dir == SIBLING_RIGHT) ? (mc->mc_ki[mc->mc_top] + (size_t)1 >=
                                page_numkeys(mc->mc_pg[mc->mc_top]))
                             : (mc->mc_ki[mc->mc_top] == 0)) {
    DEBUG("no more keys aside, moving to next %s sibling",
          dir ? "right" : "left");
    if (unlikely((rc = cursor_sibling(mc, dir)) != MDBX_SUCCESS)) {
      /* undo cursor_pop before returning */
      mc->mc_top++;
      mc->mc_snum++;
      return rc;
    }
  } else {
    assert((dir - 1) == -1 || (dir - 1) == 1);
    mc->mc_ki[mc->mc_top] += (indx_t)(dir - 1);
    DEBUG("just moving to %s index key %u",
          (dir == SIBLING_RIGHT) ? "right" : "left", mc->mc_ki[mc->mc_top]);
  }
  cASSERT(mc, IS_BRANCH(mc->mc_pg[mc->mc_top]));

  node = page_node(mp = mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top]);
  rc = page_get(mc, node_pgno(node), &mp, mp->mp_txnid);
  if (unlikely(rc != MDBX_SUCCESS)) {
    /* mc will be inconsistent if caller does mc_snum++ as above */
    mc->mc_flags &= ~(C_INITIALIZED | C_EOF);
    return rc;
  }

  rc = cursor_push(mc, mp);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  mc->mc_ki[mc->mc_top] =
      (dir == SIBLING_LEFT) ? (indx_t)page_numkeys(mp) - 1 : 0;
  return MDBX_SUCCESS;
}

/* Move the cursor to the next data item. */
static int cursor_next(MDBX_cursor *mc, MDBX_val *key, MDBX_val *data,
                       MDBX_cursor_op op) {
  MDBX_page *mp;
  MDBX_node *node;
  int rc;

  if (unlikely(mc->mc_flags & C_DEL) && op == MDBX_NEXT_DUP)
    return MDBX_NOTFOUND;

  if (unlikely(!(mc->mc_flags & C_INITIALIZED)))
    return cursor_first(mc, key, data);

  mp = mc->mc_pg[mc->mc_top];
  if (unlikely(mc->mc_flags & C_EOF)) {
    if (mc->mc_ki[mc->mc_top] + (size_t)1 >= page_numkeys(mp))
      return MDBX_NOTFOUND;
    mc->mc_flags ^= C_EOF;
  }

  if (mc->mc_db->md_flags & MDBX_DUPSORT) {
    node = page_node(mp, mc->mc_ki[mc->mc_top]);
    if (node_flags(node) & F_DUPDATA) {
      if (op == MDBX_NEXT || op == MDBX_NEXT_DUP) {
        rc = cursor_next(&mc->mc_xcursor->mx_cursor, data, NULL, MDBX_NEXT);
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

  DEBUG("cursor_next: top page is %" PRIaPGNO " in cursor %p", mp->mp_pgno,
        (void *)mc);
  if (mc->mc_flags & C_DEL) {
    mc->mc_flags ^= C_DEL;
    goto skip;
  }

  intptr_t ki = mc->mc_ki[mc->mc_top];
  mc->mc_ki[mc->mc_top] = (indx_t)++ki;
  const intptr_t numkeys = page_numkeys(mp);
  if (unlikely(ki >= numkeys)) {
    DEBUG("%s", "=====> move to next sibling page");
    mc->mc_ki[mc->mc_top] = (indx_t)(numkeys - 1);
    rc = cursor_sibling(mc, SIBLING_RIGHT);
    if (unlikely(rc != MDBX_SUCCESS)) {
      mc->mc_flags |= C_EOF;
      return rc;
    }
    mp = mc->mc_pg[mc->mc_top];
    DEBUG("next page is %" PRIaPGNO ", key index %u", mp->mp_pgno,
          mc->mc_ki[mc->mc_top]);
  }

skip:
  DEBUG("==> cursor points to page %" PRIaPGNO " with %zu keys, key index %u",
        mp->mp_pgno, page_numkeys(mp), mc->mc_ki[mc->mc_top]);

  if (!MDBX_DISABLE_VALIDATION && unlikely(!CHECK_LEAF_TYPE(mc, mp))) {
    ERROR("unexpected leaf-page #%" PRIaPGNO " type 0x%x seen by cursor",
          mp->mp_pgno, mp->mp_flags);
    return MDBX_CORRUPTED;
  }

  if (IS_LEAF2(mp)) {
    if (likely(key)) {
      key->iov_len = mc->mc_db->md_xsize;
      key->iov_base = page_leaf2key(mp, mc->mc_ki[mc->mc_top], key->iov_len);
    }
    return MDBX_SUCCESS;
  }

  node = page_node(mp, mc->mc_ki[mc->mc_top]);
  if (node_flags(node) & F_DUPDATA) {
    rc = cursor_xinit1(mc, node, mp);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    rc = cursor_first(&mc->mc_xcursor->mx_cursor, data, NULL);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  } else if (likely(data)) {
    rc = node_read(mc, node, data, mp);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }

  get_key_optional(node, key);
  return MDBX_SUCCESS;
}

/* Move the cursor to the previous data item. */
static int cursor_prev(MDBX_cursor *mc, MDBX_val *key, MDBX_val *data,
                       MDBX_cursor_op op) {
  MDBX_page *mp;
  MDBX_node *node;
  int rc;

  if (unlikely(mc->mc_flags & C_DEL) && op == MDBX_PREV_DUP)
    return MDBX_NOTFOUND;

  if (unlikely(!(mc->mc_flags & C_INITIALIZED))) {
    rc = cursor_last(mc, key, data);
    if (unlikely(rc))
      return rc;
    mc->mc_ki[mc->mc_top]++;
  }

  mp = mc->mc_pg[mc->mc_top];
  if ((mc->mc_db->md_flags & MDBX_DUPSORT) &&
      mc->mc_ki[mc->mc_top] < page_numkeys(mp)) {
    node = page_node(mp, mc->mc_ki[mc->mc_top]);
    if (node_flags(node) & F_DUPDATA) {
      if (op == MDBX_PREV || op == MDBX_PREV_DUP) {
        rc = cursor_prev(&mc->mc_xcursor->mx_cursor, data, NULL, MDBX_PREV);
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

  DEBUG("cursor_prev: top page is %" PRIaPGNO " in cursor %p", mp->mp_pgno,
        (void *)mc);

  mc->mc_flags &= ~(C_EOF | C_DEL);

  int ki = mc->mc_ki[mc->mc_top];
  mc->mc_ki[mc->mc_top] = (indx_t)--ki;
  if (unlikely(ki < 0)) {
    mc->mc_ki[mc->mc_top] = 0;
    DEBUG("%s", "=====> move to prev sibling page");
    if ((rc = cursor_sibling(mc, SIBLING_LEFT)) != MDBX_SUCCESS)
      return rc;
    mp = mc->mc_pg[mc->mc_top];
    DEBUG("prev page is %" PRIaPGNO ", key index %u", mp->mp_pgno,
          mc->mc_ki[mc->mc_top]);
  }
  DEBUG("==> cursor points to page %" PRIaPGNO " with %zu keys, key index %u",
        mp->mp_pgno, page_numkeys(mp), mc->mc_ki[mc->mc_top]);

  if (!MDBX_DISABLE_VALIDATION && unlikely(!CHECK_LEAF_TYPE(mc, mp))) {
    ERROR("unexpected leaf-page #%" PRIaPGNO " type 0x%x seen by cursor",
          mp->mp_pgno, mp->mp_flags);
    return MDBX_CORRUPTED;
  }

  if (IS_LEAF2(mp)) {
    if (likely(key)) {
      key->iov_len = mc->mc_db->md_xsize;
      key->iov_base = page_leaf2key(mp, mc->mc_ki[mc->mc_top], key->iov_len);
    }
    return MDBX_SUCCESS;
  }

  node = page_node(mp, mc->mc_ki[mc->mc_top]);

  if (node_flags(node) & F_DUPDATA) {
    rc = cursor_xinit1(mc, node, mp);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    rc = cursor_last(&mc->mc_xcursor->mx_cursor, data, NULL);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  } else if (likely(data)) {
    rc = node_read(mc, node, data, mp);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }

  get_key_optional(node, key);
  return MDBX_SUCCESS;
}

/* Set the cursor on a specific data item. */
__hot static struct cursor_set_result
cursor_set(MDBX_cursor *mc, MDBX_val *key, MDBX_val *data, MDBX_cursor_op op) {
  MDBX_page *mp;
  MDBX_node *node = NULL;
  DKBUF_DEBUG;

  struct cursor_set_result ret;
  ret.exact = false;
  if (unlikely(key->iov_len < mc->mc_dbx->md_klen_min ||
               (key->iov_len > mc->mc_dbx->md_klen_max &&
                (mc->mc_dbx->md_klen_min == mc->mc_dbx->md_klen_max ||
                 MDBX_DEBUG || MDBX_FORCE_ASSERTIONS)))) {
    cASSERT(mc, !"Invalid key-size");
    ret.err = MDBX_BAD_VALSIZE;
    return ret;
  }

  MDBX_val aligned_key = *key;
  uint64_t aligned_keybytes;
  if (mc->mc_db->md_flags & MDBX_INTEGERKEY) {
    switch (aligned_key.iov_len) {
    default:
      cASSERT(mc, !"key-size is invalid for MDBX_INTEGERKEY");
      ret.err = MDBX_BAD_VALSIZE;
      return ret;
    case 4:
      if (unlikely(3 & (uintptr_t)aligned_key.iov_base))
        /* copy instead of return error to avoid break compatibility */
        aligned_key.iov_base =
            memcpy(&aligned_keybytes, aligned_key.iov_base, 4);
      break;
    case 8:
      if (unlikely(7 & (uintptr_t)aligned_key.iov_base))
        /* copy instead of return error to avoid break compatibility */
        aligned_key.iov_base =
            memcpy(&aligned_keybytes, aligned_key.iov_base, 8);
      break;
    }
  }

  if (mc->mc_xcursor)
    mc->mc_xcursor->mx_cursor.mc_flags &= ~(C_INITIALIZED | C_EOF);

  /* See if we're already on the right page */
  if (mc->mc_flags & C_INITIALIZED) {
    MDBX_val nodekey;

    cASSERT(mc, IS_LEAF(mc->mc_pg[mc->mc_top]));
    mp = mc->mc_pg[mc->mc_top];
    if (unlikely(!page_numkeys(mp))) {
      mc->mc_ki[mc->mc_top] = 0;
      mc->mc_flags |= C_EOF;
      ret.err = MDBX_NOTFOUND;
      return ret;
    }
    if (IS_LEAF2(mp)) {
      nodekey.iov_len = mc->mc_db->md_xsize;
      nodekey.iov_base = page_leaf2key(mp, 0, nodekey.iov_len);
    } else {
      node = page_node(mp, 0);
      get_key(node, &nodekey);
    }
    int cmp = mc->mc_dbx->md_cmp(&aligned_key, &nodekey);
    if (unlikely(cmp == 0)) {
      /* Probably happens rarely, but first node on the page
       * was the one we wanted. */
      mc->mc_ki[mc->mc_top] = 0;
      ret.exact = true;
      cASSERT(mc, mc->mc_ki[mc->mc_top] < page_numkeys(mc->mc_pg[mc->mc_top]) ||
                      (mc->mc_flags & C_EOF));
      goto got_node;
    }
    if (cmp > 0) {
      const size_t nkeys = page_numkeys(mp);
      if (nkeys > 1) {
        if (IS_LEAF2(mp)) {
          nodekey.iov_base = page_leaf2key(mp, nkeys - 1, nodekey.iov_len);
        } else {
          node = page_node(mp, nkeys - 1);
          get_key(node, &nodekey);
        }
        cmp = mc->mc_dbx->md_cmp(&aligned_key, &nodekey);
        if (cmp == 0) {
          /* last node was the one we wanted */
          cASSERT(mc, nkeys >= 1 && nkeys <= UINT16_MAX + 1);
          mc->mc_ki[mc->mc_top] = (indx_t)(nkeys - 1);
          ret.exact = true;
          cASSERT(mc,
                  mc->mc_ki[mc->mc_top] < page_numkeys(mc->mc_pg[mc->mc_top]) ||
                      (mc->mc_flags & C_EOF));
          goto got_node;
        }
        if (cmp < 0) {
          if (mc->mc_ki[mc->mc_top] < page_numkeys(mp)) {
            /* This is definitely the right page, skip search_page */
            if (IS_LEAF2(mp)) {
              nodekey.iov_base =
                  page_leaf2key(mp, mc->mc_ki[mc->mc_top], nodekey.iov_len);
            } else {
              node = page_node(mp, mc->mc_ki[mc->mc_top]);
              get_key(node, &nodekey);
            }
            cmp = mc->mc_dbx->md_cmp(&aligned_key, &nodekey);
            if (cmp == 0) {
              /* current node was the one we wanted */
              ret.exact = true;
              cASSERT(mc, mc->mc_ki[mc->mc_top] <
                                  page_numkeys(mc->mc_pg[mc->mc_top]) ||
                              (mc->mc_flags & C_EOF));
              goto got_node;
            }
          }
          mc->mc_flags &= ~C_EOF;
          goto search_node;
        }
      }
      /* If any parents have right-sibs, search.
       * Otherwise, there's nothing further. */
      size_t i;
      for (i = 0; i < mc->mc_top; i++)
        if (mc->mc_ki[i] < page_numkeys(mc->mc_pg[i]) - 1)
          break;
      if (i == mc->mc_top) {
        /* There are no other pages */
        cASSERT(mc, nkeys <= UINT16_MAX);
        mc->mc_ki[mc->mc_top] = (uint16_t)nkeys;
        mc->mc_flags |= C_EOF;
        ret.err = MDBX_NOTFOUND;
        return ret;
      }
    }
    if (!mc->mc_top) {
      /* There are no other pages */
      mc->mc_ki[mc->mc_top] = 0;
      if (op == MDBX_SET_RANGE)
        goto got_node;

      cASSERT(mc, mc->mc_ki[mc->mc_top] < page_numkeys(mc->mc_pg[mc->mc_top]) ||
                      (mc->mc_flags & C_EOF));
      ret.err = MDBX_NOTFOUND;
      return ret;
    }
  } else {
    mc->mc_pg[0] = nullptr;
  }

  ret.err = page_search(mc, &aligned_key, 0);
  if (unlikely(ret.err != MDBX_SUCCESS))
    return ret;

  mp = mc->mc_pg[mc->mc_top];
  MDBX_ANALYSIS_ASSUME(mp != nullptr);
  cASSERT(mc, IS_LEAF(mp));

search_node:;
  struct node_result nsr = node_search(mc, &aligned_key);
  node = nsr.node;
  ret.exact = nsr.exact;
  if (!ret.exact) {
    if (op != MDBX_SET_RANGE) {
      /* MDBX_SET specified and not an exact match. */
      if (unlikely(mc->mc_ki[mc->mc_top] >=
                   page_numkeys(mc->mc_pg[mc->mc_top])))
        mc->mc_flags |= C_EOF;
      ret.err = MDBX_NOTFOUND;
      return ret;
    }

    if (node == NULL) {
      DEBUG("%s", "===> inexact leaf not found, goto sibling");
      ret.err = cursor_sibling(mc, SIBLING_RIGHT);
      if (unlikely(ret.err != MDBX_SUCCESS)) {
        mc->mc_flags |= C_EOF;
        return ret; /* no entries matched */
      }
      mp = mc->mc_pg[mc->mc_top];
      cASSERT(mc, IS_LEAF(mp));
      if (!IS_LEAF2(mp))
        node = page_node(mp, 0);
    }
  }
  cASSERT(mc, mc->mc_ki[mc->mc_top] < page_numkeys(mc->mc_pg[mc->mc_top]) ||
                  (mc->mc_flags & C_EOF));

got_node:
  mc->mc_flags |= C_INITIALIZED;
  mc->mc_flags &= ~C_EOF;

  if (!MDBX_DISABLE_VALIDATION && unlikely(!CHECK_LEAF_TYPE(mc, mp))) {
    ERROR("unexpected leaf-page #%" PRIaPGNO " type 0x%x seen by cursor",
          mp->mp_pgno, mp->mp_flags);
    ret.err = MDBX_CORRUPTED;
    return ret;
  }

  if (IS_LEAF2(mp)) {
    if (op == MDBX_SET_RANGE || op == MDBX_SET_KEY) {
      key->iov_len = mc->mc_db->md_xsize;
      key->iov_base = page_leaf2key(mp, mc->mc_ki[mc->mc_top], key->iov_len);
    }
    ret.err = MDBX_SUCCESS;
    return ret;
  }

  if (node_flags(node) & F_DUPDATA) {
    ret.err = cursor_xinit1(mc, node, mp);
    if (unlikely(ret.err != MDBX_SUCCESS))
      return ret;
    if (op == MDBX_SET || op == MDBX_SET_KEY || op == MDBX_SET_RANGE) {
      MDBX_ANALYSIS_ASSUME(mc->mc_xcursor != nullptr);
      ret.err = cursor_first(&mc->mc_xcursor->mx_cursor, data, NULL);
      if (unlikely(ret.err != MDBX_SUCCESS))
        return ret;
    } else {
      MDBX_ANALYSIS_ASSUME(mc->mc_xcursor != nullptr);
      ret = cursor_set(&mc->mc_xcursor->mx_cursor, data, NULL, MDBX_SET_RANGE);
      if (unlikely(ret.err != MDBX_SUCCESS))
        return ret;
      if (op == MDBX_GET_BOTH && !ret.exact) {
        ret.err = MDBX_NOTFOUND;
        return ret;
      }
    }
  } else if (likely(data)) {
    if (op == MDBX_GET_BOTH || op == MDBX_GET_BOTH_RANGE) {
      if (unlikely(data->iov_len < mc->mc_dbx->md_vlen_min ||
                   data->iov_len > mc->mc_dbx->md_vlen_max)) {
        cASSERT(mc, !"Invalid data-size");
        ret.err = MDBX_BAD_VALSIZE;
        return ret;
      }
      MDBX_val aligned_data = *data;
      uint64_t aligned_databytes;
      if (mc->mc_db->md_flags & MDBX_INTEGERDUP) {
        switch (aligned_data.iov_len) {
        default:
          cASSERT(mc, !"data-size is invalid for MDBX_INTEGERDUP");
          ret.err = MDBX_BAD_VALSIZE;
          return ret;
        case 4:
          if (unlikely(3 & (uintptr_t)aligned_data.iov_base))
            /* copy instead of return error to avoid break compatibility */
            aligned_data.iov_base =
                memcpy(&aligned_databytes, aligned_data.iov_base, 4);
          break;
        case 8:
          if (unlikely(7 & (uintptr_t)aligned_data.iov_base))
            /* copy instead of return error to avoid break compatibility */
            aligned_data.iov_base =
                memcpy(&aligned_databytes, aligned_data.iov_base, 8);
          break;
        }
      }
      MDBX_val actual_data;
      ret.err = node_read(mc, node, &actual_data, mc->mc_pg[mc->mc_top]);
      if (unlikely(ret.err != MDBX_SUCCESS))
        return ret;
      const int cmp = mc->mc_dbx->md_dcmp(&aligned_data, &actual_data);
      if (cmp) {
        cASSERT(mc,
                mc->mc_ki[mc->mc_top] < page_numkeys(mc->mc_pg[mc->mc_top]) ||
                    (mc->mc_flags & C_EOF));
        if (op != MDBX_GET_BOTH_RANGE || cmp > 0) {
          ret.err = MDBX_NOTFOUND;
          return ret;
        }
      }
      *data = actual_data;
    } else {
      ret.err = node_read(mc, node, data, mc->mc_pg[mc->mc_top]);
      if (unlikely(ret.err != MDBX_SUCCESS))
        return ret;
    }
  }

  /* The key already matches in all other cases */
  if (op == MDBX_SET_RANGE || op == MDBX_SET_KEY)
    get_key_optional(node, key);

  DEBUG("==> cursor placed on key [%s], data [%s]", DKEY_DEBUG(key),
        DVAL_DEBUG(data));
  ret.err = MDBX_SUCCESS;
  return ret;
}

/* Move the cursor to the first item in the database. */
static int cursor_first(MDBX_cursor *mc, MDBX_val *key, MDBX_val *data) {
  int rc;

  if (mc->mc_xcursor)
    mc->mc_xcursor->mx_cursor.mc_flags &= ~(C_INITIALIZED | C_EOF);

  if (!(mc->mc_flags & C_INITIALIZED) || mc->mc_top) {
    rc = page_search(mc, NULL, MDBX_PS_FIRST);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }

  const MDBX_page *mp = mc->mc_pg[mc->mc_top];
  if (!MDBX_DISABLE_VALIDATION && unlikely(!CHECK_LEAF_TYPE(mc, mp))) {
    ERROR("unexpected leaf-page #%" PRIaPGNO " type 0x%x seen by cursor",
          mp->mp_pgno, mp->mp_flags);
    return MDBX_CORRUPTED;
  }

  mc->mc_flags |= C_INITIALIZED;
  mc->mc_flags &= ~C_EOF;
  mc->mc_ki[mc->mc_top] = 0;

  if (IS_LEAF2(mp)) {
    if (likely(key)) {
      key->iov_len = mc->mc_db->md_xsize;
      key->iov_base = page_leaf2key(mp, 0, key->iov_len);
    }
    return MDBX_SUCCESS;
  }

  MDBX_node *node = page_node(mp, 0);
  if (node_flags(node) & F_DUPDATA) {
    rc = cursor_xinit1(mc, node, mp);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    MDBX_ANALYSIS_ASSUME(mc->mc_xcursor != nullptr);
    rc = cursor_first(&mc->mc_xcursor->mx_cursor, data, NULL);
    if (unlikely(rc))
      return rc;
  } else if (likely(data)) {
    rc = node_read(mc, node, data, mp);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }

  get_key_optional(node, key);
  return MDBX_SUCCESS;
}

/* Move the cursor to the last item in the database. */
static int cursor_last(MDBX_cursor *mc, MDBX_val *key, MDBX_val *data) {
  int rc;

  if (mc->mc_xcursor)
    mc->mc_xcursor->mx_cursor.mc_flags &= ~(C_INITIALIZED | C_EOF);

  if (!(mc->mc_flags & C_INITIALIZED) || mc->mc_top) {
    rc = page_search(mc, NULL, MDBX_PS_LAST);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }

  const MDBX_page *mp = mc->mc_pg[mc->mc_top];
  if (!MDBX_DISABLE_VALIDATION && unlikely(!CHECK_LEAF_TYPE(mc, mp))) {
    ERROR("unexpected leaf-page #%" PRIaPGNO " type 0x%x seen by cursor",
          mp->mp_pgno, mp->mp_flags);
    return MDBX_CORRUPTED;
  }

  mc->mc_ki[mc->mc_top] = (indx_t)page_numkeys(mp) - 1;
  mc->mc_flags |= C_INITIALIZED | C_EOF;

  if (IS_LEAF2(mp)) {
    if (likely(key)) {
      key->iov_len = mc->mc_db->md_xsize;
      key->iov_base = page_leaf2key(mp, mc->mc_ki[mc->mc_top], key->iov_len);
    }
    return MDBX_SUCCESS;
  }

  MDBX_node *node = page_node(mp, mc->mc_ki[mc->mc_top]);
  if (node_flags(node) & F_DUPDATA) {
    rc = cursor_xinit1(mc, node, mp);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    MDBX_ANALYSIS_ASSUME(mc->mc_xcursor != nullptr);
    rc = cursor_last(&mc->mc_xcursor->mx_cursor, data, NULL);
    if (unlikely(rc))
      return rc;
  } else if (likely(data)) {
    rc = node_read(mc, node, data, mp);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }

  get_key_optional(node, key);
  return MDBX_SUCCESS;
}

static __hot int cursor_get(MDBX_cursor *mc, MDBX_val *key, MDBX_val *data,
                            MDBX_cursor_op op) {
  int (*mfunc)(MDBX_cursor *mc, MDBX_val *key, MDBX_val *data);
  int rc;

  switch (op) {
  case MDBX_GET_CURRENT: {
    if (unlikely(!(mc->mc_flags & C_INITIALIZED)))
      return MDBX_ENODATA;
    const MDBX_page *mp = mc->mc_pg[mc->mc_top];
    if (!MDBX_DISABLE_VALIDATION && unlikely(!CHECK_LEAF_TYPE(mc, mp))) {
      ERROR("unexpected leaf-page #%" PRIaPGNO " type 0x%x seen by cursor",
            mp->mp_pgno, mp->mp_flags);
      return MDBX_CORRUPTED;
    }
    const size_t nkeys = page_numkeys(mp);
    if (unlikely(mc->mc_ki[mc->mc_top] >= nkeys)) {
      cASSERT(mc, nkeys <= UINT16_MAX);
      if (mc->mc_flags & C_EOF)
        return MDBX_ENODATA;
      mc->mc_ki[mc->mc_top] = (uint16_t)nkeys;
      mc->mc_flags |= C_EOF;
      return MDBX_NOTFOUND;
    }
    cASSERT(mc, nkeys > 0);

    rc = MDBX_SUCCESS;
    if (IS_LEAF2(mp)) {
      key->iov_len = mc->mc_db->md_xsize;
      key->iov_base = page_leaf2key(mp, mc->mc_ki[mc->mc_top], key->iov_len);
    } else {
      MDBX_node *node = page_node(mp, mc->mc_ki[mc->mc_top]);
      get_key_optional(node, key);
      if (data) {
        if (node_flags(node) & F_DUPDATA) {
          if (unlikely(!(mc->mc_xcursor->mx_cursor.mc_flags & C_INITIALIZED))) {
            rc = cursor_xinit1(mc, node, mp);
            if (unlikely(rc != MDBX_SUCCESS))
              return rc;
            rc = cursor_first(&mc->mc_xcursor->mx_cursor, data, NULL);
            if (unlikely(rc))
              return rc;
          } else {
            rc = cursor_get(&mc->mc_xcursor->mx_cursor, data, NULL,
                            MDBX_GET_CURRENT);
            if (unlikely(rc))
              return rc;
          }
        } else {
          rc = node_read(mc, node, data, mp);
          if (unlikely(rc))
            return rc;
        }
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
    rc = cursor_set(mc, key, data, op).err;
    if (mc->mc_flags & C_INITIALIZED) {
      cASSERT(mc, mc->mc_snum > 0 && mc->mc_top < mc->mc_snum);
      cASSERT(mc, mc->mc_ki[mc->mc_top] < page_numkeys(mc->mc_pg[mc->mc_top]) ||
                      (mc->mc_flags & C_EOF));
    }
    break;
  case MDBX_GET_MULTIPLE:
    if (unlikely(!data))
      return MDBX_EINVAL;
    if (unlikely((mc->mc_db->md_flags & MDBX_DUPFIXED) == 0))
      return MDBX_INCOMPATIBLE;
    if ((mc->mc_flags & C_INITIALIZED) == 0) {
      if (unlikely(!key))
        return MDBX_EINVAL;
      rc = cursor_set(mc, key, data, MDBX_SET).err;
      if (unlikely(rc != MDBX_SUCCESS))
        break;
    }
    rc = MDBX_SUCCESS;
    if (unlikely(C_INITIALIZED != (mc->mc_xcursor->mx_cursor.mc_flags &
                                   (C_INITIALIZED | C_EOF))))
      break;
    goto fetch_multiple;
  case MDBX_NEXT_MULTIPLE:
    if (unlikely(!data))
      return MDBX_EINVAL;
    if (unlikely(!(mc->mc_db->md_flags & MDBX_DUPFIXED)))
      return MDBX_INCOMPATIBLE;
    rc = cursor_next(mc, key, data, MDBX_NEXT_DUP);
    if (rc == MDBX_SUCCESS) {
      if (mc->mc_xcursor->mx_cursor.mc_flags & C_INITIALIZED) {
      fetch_multiple:;
        MDBX_cursor *mx = &mc->mc_xcursor->mx_cursor;
        data->iov_len =
            page_numkeys(mx->mc_pg[mx->mc_top]) * mx->mc_db->md_xsize;
        data->iov_base = page_data(mx->mc_pg[mx->mc_top]);
        mx->mc_ki[mx->mc_top] = (indx_t)page_numkeys(mx->mc_pg[mx->mc_top]) - 1;
      }
    }
    break;
  case MDBX_PREV_MULTIPLE:
    if (unlikely(!data))
      return MDBX_EINVAL;
    if (!(mc->mc_db->md_flags & MDBX_DUPFIXED))
      return MDBX_INCOMPATIBLE;
    rc = MDBX_SUCCESS;
    if ((mc->mc_flags & C_INITIALIZED) == 0)
      rc = cursor_last(mc, key, data);
    if (rc == MDBX_SUCCESS) {
      MDBX_cursor *mx = &mc->mc_xcursor->mx_cursor;
      rc = MDBX_NOTFOUND;
      if (mx->mc_flags & C_INITIALIZED) {
        rc = cursor_sibling(mx, SIBLING_LEFT);
        if (rc == MDBX_SUCCESS)
          goto fetch_multiple;
      }
    }
    break;
  case MDBX_NEXT:
  case MDBX_NEXT_DUP:
  case MDBX_NEXT_NODUP:
    rc = cursor_next(mc, key, data, op);
    break;
  case MDBX_PREV:
  case MDBX_PREV_DUP:
  case MDBX_PREV_NODUP:
    rc = cursor_prev(mc, key, data, op);
    break;
  case MDBX_FIRST:
    rc = cursor_first(mc, key, data);
    break;
  case MDBX_FIRST_DUP:
    mfunc = cursor_first;
  move:
    if (unlikely(data == NULL || !(mc->mc_flags & C_INITIALIZED)))
      return MDBX_EINVAL;
    if (unlikely(mc->mc_xcursor == NULL))
      return MDBX_INCOMPATIBLE;
    if (mc->mc_ki[mc->mc_top] >= page_numkeys(mc->mc_pg[mc->mc_top])) {
      mc->mc_ki[mc->mc_top] = (indx_t)page_numkeys(mc->mc_pg[mc->mc_top]);
      mc->mc_flags |= C_EOF;
      return MDBX_NOTFOUND;
    } else {
      MDBX_node *node = page_node(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top]);
      if (!(node_flags(node) & F_DUPDATA)) {
        get_key_optional(node, key);
        rc = node_read(mc, node, data, mc->mc_pg[mc->mc_top]);
        break;
      }
    }
    if (unlikely(!(mc->mc_xcursor->mx_cursor.mc_flags & C_INITIALIZED)))
      return MDBX_EINVAL;
    rc = mfunc(&mc->mc_xcursor->mx_cursor, data, NULL);
    break;
  case MDBX_LAST:
    rc = cursor_last(mc, key, data);
    break;
  case MDBX_LAST_DUP:
    mfunc = cursor_last;
    goto move;
  case MDBX_SET_UPPERBOUND: /* mostly same as MDBX_SET_LOWERBOUND */
  case MDBX_SET_LOWERBOUND: {
    if (unlikely(key == NULL || data == NULL))
      return MDBX_EINVAL;
    MDBX_val save_data = *data;
    struct cursor_set_result csr = cursor_set(mc, key, data, MDBX_SET_RANGE);
    rc = csr.err;
    if (rc == MDBX_SUCCESS && csr.exact && mc->mc_xcursor) {
      mc->mc_flags &= ~C_DEL;
      csr.exact = false;
      if (!save_data.iov_base && (mc->mc_db->md_flags & MDBX_DUPFIXED)) {
        /* Avoiding search nested dupfixed hive if no data provided.
         * This is changes the semantic of MDBX_SET_LOWERBOUND but avoid
         * returning MDBX_BAD_VALSIZE. */
      } else if (mc->mc_xcursor->mx_cursor.mc_flags & C_INITIALIZED) {
        *data = save_data;
        csr =
            cursor_set(&mc->mc_xcursor->mx_cursor, data, NULL, MDBX_SET_RANGE);
        rc = csr.err;
        if (rc == MDBX_NOTFOUND) {
          cASSERT(mc, !csr.exact);
          rc = cursor_next(mc, key, data, MDBX_NEXT_NODUP);
        }
      } else {
        int cmp = mc->mc_dbx->md_dcmp(&save_data, data);
        csr.exact = (cmp == 0);
        if (cmp > 0)
          rc = cursor_next(mc, key, data, MDBX_NEXT_NODUP);
      }
    }
    if (rc == MDBX_SUCCESS && !csr.exact)
      rc = MDBX_RESULT_TRUE;
    if (unlikely(op == MDBX_SET_UPPERBOUND)) {
      /* minor fixups for MDBX_SET_UPPERBOUND */
      if (rc == MDBX_RESULT_TRUE)
        /* already at great-than by MDBX_SET_LOWERBOUND */
        rc = MDBX_SUCCESS;
      else if (rc == MDBX_SUCCESS)
        /* exactly match, going next */
        rc = cursor_next(mc, key, data, MDBX_NEXT);
    }
    break;
  }
  default:
    DEBUG("unhandled/unimplemented cursor operation %u", op);
    return MDBX_EINVAL;
  }

  mc->mc_flags &= ~C_DEL;
  return rc;
}

int mdbx_cursor_get(MDBX_cursor *mc, MDBX_val *key, MDBX_val *data,
                    MDBX_cursor_op op) {
  if (unlikely(mc == NULL))
    return MDBX_EINVAL;

  if (unlikely(mc->mc_signature != MDBX_MC_LIVE))
    return (mc->mc_signature == MDBX_MC_READY4CLOSE) ? MDBX_EINVAL
                                                     : MDBX_EBADSIGN;

  int rc = check_txn(mc->mc_txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  return cursor_get(mc, key, data, op);
}

static int cursor_first_batch(MDBX_cursor *mc) {
  if (!(mc->mc_flags & C_INITIALIZED) || mc->mc_top) {
    int err = page_search(mc, NULL, MDBX_PS_FIRST);
    if (unlikely(err != MDBX_SUCCESS))
      return err;
  }
  cASSERT(mc, IS_LEAF(mc->mc_pg[mc->mc_top]));

  mc->mc_flags |= C_INITIALIZED;
  mc->mc_flags &= ~C_EOF;
  mc->mc_ki[mc->mc_top] = 0;
  return MDBX_SUCCESS;
}

static int cursor_next_batch(MDBX_cursor *mc) {
  if (unlikely(!(mc->mc_flags & C_INITIALIZED)))
    return cursor_first_batch(mc);

  MDBX_page *mp = mc->mc_pg[mc->mc_top];
  if (unlikely(mc->mc_flags & C_EOF)) {
    if ((size_t)mc->mc_ki[mc->mc_top] + 1 >= page_numkeys(mp))
      return MDBX_NOTFOUND;
    mc->mc_flags ^= C_EOF;
  }

  intptr_t ki = mc->mc_ki[mc->mc_top];
  mc->mc_ki[mc->mc_top] = (indx_t)++ki;
  const intptr_t numkeys = page_numkeys(mp);
  if (likely(ki >= numkeys)) {
    DEBUG("%s", "=====> move to next sibling page");
    mc->mc_ki[mc->mc_top] = (indx_t)(numkeys - 1);
    int err = cursor_sibling(mc, SIBLING_RIGHT);
    if (unlikely(err != MDBX_SUCCESS)) {
      mc->mc_flags |= C_EOF;
      return err;
    }
    mp = mc->mc_pg[mc->mc_top];
    DEBUG("next page is %" PRIaPGNO ", key index %u", mp->mp_pgno,
          mc->mc_ki[mc->mc_top]);
    if (!MDBX_DISABLE_VALIDATION && unlikely(!CHECK_LEAF_TYPE(mc, mp))) {
      ERROR("unexpected leaf-page #%" PRIaPGNO " type 0x%x seen by cursor",
            mp->mp_pgno, mp->mp_flags);
      return MDBX_CORRUPTED;
    }
  }
  return MDBX_SUCCESS;
}

int mdbx_cursor_get_batch(MDBX_cursor *mc, size_t *count, MDBX_val *pairs,
                          size_t limit, MDBX_cursor_op op) {
  if (unlikely(mc == NULL || count == NULL || limit < 4))
    return MDBX_EINVAL;

  if (unlikely(mc->mc_signature != MDBX_MC_LIVE))
    return (mc->mc_signature == MDBX_MC_READY4CLOSE) ? MDBX_EINVAL
                                                     : MDBX_EBADSIGN;

  int rc = check_txn(mc->mc_txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(mc->mc_db->md_flags & MDBX_DUPSORT))
    return MDBX_INCOMPATIBLE /* must be a non-dupsort subDB */;

  switch (op) {
  case MDBX_FIRST:
    rc = cursor_first_batch(mc);
    break;
  case MDBX_NEXT:
    rc = cursor_next_batch(mc);
    break;
  case MDBX_GET_CURRENT:
    rc = likely(mc->mc_flags & C_INITIALIZED) ? MDBX_SUCCESS : MDBX_ENODATA;
    break;
  default:
    DEBUG("unhandled/unimplemented cursor operation %u", op);
    rc = MDBX_EINVAL;
    break;
  }

  if (unlikely(rc != MDBX_SUCCESS)) {
    *count = 0;
    return rc;
  }

  const MDBX_page *const mp = mc->mc_pg[mc->mc_top];
  if (!MDBX_DISABLE_VALIDATION && unlikely(!CHECK_LEAF_TYPE(mc, mp))) {
    ERROR("unexpected leaf-page #%" PRIaPGNO " type 0x%x seen by cursor",
          mp->mp_pgno, mp->mp_flags);
    return MDBX_CORRUPTED;
  }
  const size_t nkeys = page_numkeys(mp);
  size_t i = mc->mc_ki[mc->mc_top], n = 0;
  if (unlikely(i >= nkeys)) {
    cASSERT(mc, op == MDBX_GET_CURRENT);
    cASSERT(mc, mdbx_cursor_on_last(mc) == MDBX_RESULT_TRUE);
    *count = 0;
    if (mc->mc_flags & C_EOF) {
      cASSERT(mc, mdbx_cursor_on_last(mc) == MDBX_RESULT_TRUE);
      return MDBX_ENODATA;
    }
    if (mdbx_cursor_on_last(mc) != MDBX_RESULT_TRUE)
      return MDBX_EINVAL /* again MDBX_GET_CURRENT after MDBX_GET_CURRENT */;
    mc->mc_flags |= C_EOF;
    return MDBX_NOTFOUND;
  }

  do {
    if (unlikely(n + 2 > limit)) {
      rc = MDBX_RESULT_TRUE;
      break;
    }
    const MDBX_node *leaf = page_node(mp, i);
    get_key(leaf, &pairs[n]);
    rc = node_read(mc, leaf, &pairs[n + 1], mp);
    if (unlikely(rc != MDBX_SUCCESS))
      break;
    n += 2;
  } while (++i < nkeys);

  mc->mc_ki[mc->mc_top] = (indx_t)i;
  *count = n;
  return rc;
}

static int touch_dbi(MDBX_cursor *mc) {
  cASSERT(mc, (*mc->mc_dbistate & DBI_DIRTY) == 0);
  *mc->mc_dbistate |= DBI_DIRTY;
  mc->mc_txn->mt_flags |= MDBX_TXN_DIRTY;
  if (mc->mc_dbi >= CORE_DBS) {
    /* Touch DB record of named DB */
    MDBX_cursor_couple cx;
    int rc = cursor_init(&cx.outer, mc->mc_txn, MAIN_DBI);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    mc->mc_txn->mt_dbistate[MAIN_DBI] |= DBI_DIRTY;
    rc = page_search(&cx.outer, &mc->mc_dbx->md_name, MDBX_PS_MODIFY);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }
  return MDBX_SUCCESS;
}

static __hot int cursor_touch(MDBX_cursor *const mc, const MDBX_val *key,
                              const MDBX_val *data) {
  cASSERT(mc, (mc->mc_txn->mt_flags & MDBX_TXN_RDONLY) == 0);
  cASSERT(mc, (mc->mc_flags & C_INITIALIZED) || mc->mc_snum == 0);
  cASSERT(mc, cursor_is_tracked(mc));

  if ((mc->mc_flags & C_SUB) == 0) {
    MDBX_txn *const txn = mc->mc_txn;
    txn_lru_turn(txn);

    if (unlikely((*mc->mc_dbistate & DBI_DIRTY) == 0)) {
      int err = touch_dbi(mc);
      if (unlikely(err != MDBX_SUCCESS))
        return err;
    }

    /* Estimate how much space this operation will take: */
    /* 1) Max b-tree height, reasonable enough with including dups' sub-tree */
    size_t need = CURSOR_STACK + 3;
    /* 2) GC/FreeDB for any payload */
    if (mc->mc_dbi > FREE_DBI) {
      need += txn->mt_dbs[FREE_DBI].md_depth + (size_t)3;
      /* 3) Named DBs also dirty the main DB */
      if (mc->mc_dbi > MAIN_DBI)
        need += txn->mt_dbs[MAIN_DBI].md_depth + (size_t)3;
    }
#if xMDBX_DEBUG_SPILLING != 2
    /* production mode */
    /* 4) Double the page chain estimation
     * for extensively splitting, rebalance and merging */
    need += need;
    /* 5) Factor the key+data which to be put in */
    need += bytes2pgno(txn->mt_env, node_size(key, data)) + (size_t)1;
#else
    /* debug mode */
    (void)key;
    (void)data;
    txn->mt_env->debug_dirtied_est = ++need;
    txn->mt_env->debug_dirtied_act = 0;
#endif /* xMDBX_DEBUG_SPILLING == 2 */

    int err = txn_spill(txn, mc, need);
    if (unlikely(err != MDBX_SUCCESS))
      return err;
  }

  int rc = MDBX_SUCCESS;
  if (likely(mc->mc_snum)) {
    mc->mc_top = 0;
    do {
      rc = page_touch(mc);
      if (unlikely(rc != MDBX_SUCCESS))
        break;
      mc->mc_top += 1;
    } while (mc->mc_top < mc->mc_snum);
    mc->mc_top = mc->mc_snum - 1;
  }
  return rc;
}

static size_t leaf2_reserve(const MDBX_env *const env, size_t host_page_room,
                            size_t subpage_len, size_t item_len) {
  eASSERT(env, (subpage_len & 1) == 0);
  eASSERT(env,
          env->me_subpage_reserve_prereq > env->me_subpage_room_threshold +
                                               env->me_subpage_reserve_limit &&
              env->me_leaf_nodemax >= env->me_subpage_limit + NODESIZE);
  size_t reserve = 0;
  for (size_t n = 0;
       n < 5 && reserve + item_len <= env->me_subpage_reserve_limit &&
       EVEN(subpage_len + item_len) <= env->me_subpage_limit &&
       host_page_room >=
           env->me_subpage_reserve_prereq + EVEN(subpage_len + item_len);
       ++n) {
    subpage_len += item_len;
    reserve += item_len;
  }
  return reserve + (subpage_len & 1);
}

static __hot int cursor_put_nochecklen(MDBX_cursor *mc, const MDBX_val *key,
                                       MDBX_val *data, unsigned flags) {
  int err;
  DKBUF_DEBUG;
  MDBX_env *const env = mc->mc_txn->mt_env;
  if (LOG_ENABLED(MDBX_LOG_DEBUG) && (flags & MDBX_RESERVE))
    data->iov_base = nullptr;
  DEBUG("==> put db %d key [%s], size %" PRIuPTR ", data [%s] size %" PRIuPTR,
        DDBI(mc), DKEY_DEBUG(key), key->iov_len, DVAL_DEBUG(data),
        data->iov_len);

  if ((flags & MDBX_CURRENT) != 0 && (mc->mc_flags & C_SUB) == 0) {
    if (unlikely(flags & (MDBX_APPEND | MDBX_NOOVERWRITE)))
      return MDBX_EINVAL;
    /* Опция MDBX_CURRENT означает, что запрошено обновление текущей записи,
     * на которой сейчас стоит курсор. Проверяем что переданный ключ совпадает
     * со значением в текущей позиции курсора.
     * Здесь проще вызвать cursor_get(), так как для обслуживания таблиц
     * с MDBX_DUPSORT также требуется текущий размер данных. */
    MDBX_val current_key, current_data;
    err = cursor_get(mc, &current_key, &current_data, MDBX_GET_CURRENT);
    if (unlikely(err != MDBX_SUCCESS))
      return err;
    if (mc->mc_dbx->md_cmp(key, &current_key) != 0)
      return MDBX_EKEYMISMATCH;

    if (unlikely((flags & MDBX_MULTIPLE)))
      goto drop_current;

    if (mc->mc_db->md_flags & MDBX_DUPSORT) {
      MDBX_node *node = page_node(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top]);
      if (node_flags(node) & F_DUPDATA) {
        cASSERT(mc, mc->mc_xcursor != NULL &&
                        (mc->mc_xcursor->mx_cursor.mc_flags & C_INITIALIZED));
        /* Если за ключом более одного значения, либо если размер данных
         * отличается, то вместо обновления требуется удаление и
         * последующая вставка. */
        if (mc->mc_xcursor->mx_db.md_entries > 1 ||
            current_data.iov_len != data->iov_len) {
        drop_current:
          err = cursor_del(mc, flags & MDBX_ALLDUPS);
          if (unlikely(err != MDBX_SUCCESS))
            return err;
          flags -= MDBX_CURRENT;
          goto skip_check_samedata;
        }
      } else if (unlikely(node_size(key, data) > env->me_leaf_nodemax)) {
        err = cursor_del(mc, 0);
        if (unlikely(err != MDBX_SUCCESS))
          return err;
        flags -= MDBX_CURRENT;
        goto skip_check_samedata;
      }
    }
    if (!(flags & MDBX_RESERVE) &&
        unlikely(cmp_lenfast(&current_data, data) == 0))
      return MDBX_SUCCESS /* the same data, nothing to update */;
  skip_check_samedata:;
  }

  int rc = MDBX_SUCCESS;
  if (mc->mc_db->md_root == P_INVALID) {
    /* new database, cursor has nothing to point to */
    mc->mc_snum = 0;
    mc->mc_top = 0;
    mc->mc_flags &= ~C_INITIALIZED;
    rc = MDBX_NO_ROOT;
  } else if ((flags & MDBX_CURRENT) == 0) {
    bool exact = false;
    MDBX_val last_key, old_data;
    if ((flags & MDBX_APPEND) && mc->mc_db->md_entries > 0) {
      rc = cursor_last(mc, &last_key, &old_data);
      if (likely(rc == MDBX_SUCCESS)) {
        const int cmp = mc->mc_dbx->md_cmp(key, &last_key);
        if (likely(cmp > 0)) {
          mc->mc_ki[mc->mc_top]++; /* step forward for appending */
          rc = MDBX_NOTFOUND;
        } else if (unlikely(cmp != 0)) {
          /* new-key < last-key */
          return MDBX_EKEYMISMATCH;
        } else {
          rc = MDBX_SUCCESS;
          exact = true;
        }
      }
    } else {
      struct cursor_set_result csr =
          /* olddata may not be updated in case LEAF2-page of dupfixed-subDB */
          cursor_set(mc, (MDBX_val *)key, &old_data, MDBX_SET);
      rc = csr.err;
      exact = csr.exact;
    }
    if (likely(rc == MDBX_SUCCESS)) {
      if (exact) {
        if (unlikely(flags & MDBX_NOOVERWRITE)) {
          DEBUG("duplicate key [%s]", DKEY_DEBUG(key));
          *data = old_data;
          return MDBX_KEYEXIST;
        }
        if (unlikely(mc->mc_flags & C_SUB)) {
          /* nested subtree of DUPSORT-database with the same key,
           * nothing to update */
          eASSERT(env, data->iov_len == 0 &&
                           (old_data.iov_len == 0 ||
                            /* olddata may not be updated in case LEAF2-page
                               of dupfixed-subDB */
                            (mc->mc_db->md_flags & MDBX_DUPFIXED)));
          return MDBX_SUCCESS;
        }
        if (unlikely(flags & MDBX_ALLDUPS) && mc->mc_xcursor &&
            (mc->mc_xcursor->mx_cursor.mc_flags & C_INITIALIZED)) {
          err = cursor_del(mc, MDBX_ALLDUPS);
          if (unlikely(err != MDBX_SUCCESS))
            return err;
          flags -= MDBX_ALLDUPS;
          rc = mc->mc_snum ? MDBX_NOTFOUND : MDBX_NO_ROOT;
          exact = false;
        } else if (!(flags & (MDBX_RESERVE | MDBX_MULTIPLE))) {
          /* checking for early exit without dirtying pages */
          if (unlikely(eq_fast(data, &old_data))) {
            cASSERT(mc, mc->mc_dbx->md_dcmp(data, &old_data) == 0);
            if (mc->mc_xcursor) {
              if (flags & MDBX_NODUPDATA)
                return MDBX_KEYEXIST;
              if (flags & MDBX_APPENDDUP)
                return MDBX_EKEYMISMATCH;
            }
            /* the same data, nothing to update */
            return MDBX_SUCCESS;
          }
          cASSERT(mc, mc->mc_dbx->md_dcmp(data, &old_data) != 0);
        }
      }
    } else if (unlikely(rc != MDBX_NOTFOUND))
      return rc;
  }

  mc->mc_flags &= ~C_DEL;
  MDBX_val xdata, *ref_data = data;
  size_t *batch_dupfixed_done = nullptr, batch_dupfixed_given = 0;
  if (unlikely(flags & MDBX_MULTIPLE)) {
    batch_dupfixed_given = data[1].iov_len;
    batch_dupfixed_done = &data[1].iov_len;
    *batch_dupfixed_done = 0;
  }

  /* Cursor is positioned, check for room in the dirty list */
  err = cursor_touch(mc, key, ref_data);
  if (unlikely(err))
    return err;

  if (unlikely(rc == MDBX_NO_ROOT)) {
    /* new database, write a root leaf page */
    DEBUG("%s", "allocating new root leaf page");
    pgr_t npr = page_new(mc, P_LEAF);
    if (unlikely(npr.err != MDBX_SUCCESS))
      return npr.err;
    npr.err = cursor_push(mc, npr.page);
    if (unlikely(npr.err != MDBX_SUCCESS))
      return npr.err;
    mc->mc_db->md_root = npr.page->mp_pgno;
    mc->mc_db->md_depth++;
    if (mc->mc_db->md_flags & MDBX_INTEGERKEY) {
      assert(key->iov_len >= mc->mc_dbx->md_klen_min &&
             key->iov_len <= mc->mc_dbx->md_klen_max);
      mc->mc_dbx->md_klen_min = mc->mc_dbx->md_klen_max = key->iov_len;
    }
    if (mc->mc_db->md_flags & (MDBX_INTEGERDUP | MDBX_DUPFIXED)) {
      assert(data->iov_len >= mc->mc_dbx->md_vlen_min &&
             data->iov_len <= mc->mc_dbx->md_vlen_max);
      assert(mc->mc_xcursor != NULL);
      mc->mc_db->md_xsize = mc->mc_xcursor->mx_db.md_xsize =
          (unsigned)(mc->mc_dbx->md_vlen_min = mc->mc_dbx->md_vlen_max =
                         mc->mc_xcursor->mx_dbx.md_klen_min =
                             mc->mc_xcursor->mx_dbx.md_klen_max =
                                 data->iov_len);
      if (mc->mc_flags & C_SUB)
        npr.page->mp_flags |= P_LEAF2;
    }
    mc->mc_flags |= C_INITIALIZED;
  }

  MDBX_val old_singledup, old_data;
  MDBX_db nested_dupdb;
  MDBX_page *sub_root = nullptr;
  bool insert_key, insert_data;
  uint16_t fp_flags = P_LEAF;
  MDBX_page *fp = env->me_pbuf;
  fp->mp_txnid = mc->mc_txn->mt_front;
  insert_key = insert_data = (rc != MDBX_SUCCESS);
  old_singledup.iov_base = nullptr;
  if (insert_key) {
    /* The key does not exist */
    DEBUG("inserting key at index %i", mc->mc_ki[mc->mc_top]);
    if ((mc->mc_db->md_flags & MDBX_DUPSORT) &&
        node_size(key, data) > env->me_leaf_nodemax) {
      /* Too big for a node, insert in sub-DB.  Set up an empty
       * "old sub-page" for convert_to_subtree to expand to a full page. */
      fp->mp_leaf2_ksize =
          (mc->mc_db->md_flags & MDBX_DUPFIXED) ? (uint16_t)data->iov_len : 0;
      fp->mp_lower = fp->mp_upper = 0;
      old_data.iov_len = PAGEHDRSZ;
      goto convert_to_subtree;
    }
  } else {
    /* there's only a key anyway, so this is a no-op */
    if (IS_LEAF2(mc->mc_pg[mc->mc_top])) {
      size_t ksize = mc->mc_db->md_xsize;
      if (unlikely(key->iov_len != ksize))
        return MDBX_BAD_VALSIZE;
      void *ptr =
          page_leaf2key(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top], ksize);
      memcpy(ptr, key->iov_base, ksize);
    fix_parent:
      /* if overwriting slot 0 of leaf, need to
       * update branch key if there is a parent page */
      if (mc->mc_top && !mc->mc_ki[mc->mc_top]) {
        size_t dtop = 1;
        mc->mc_top--;
        /* slot 0 is always an empty key, find real slot */
        while (mc->mc_top && !mc->mc_ki[mc->mc_top]) {
          mc->mc_top--;
          dtop++;
        }
        err = MDBX_SUCCESS;
        if (mc->mc_ki[mc->mc_top])
          err = update_key(mc, key);
        cASSERT(mc, mc->mc_top + dtop < UINT16_MAX);
        mc->mc_top += (uint8_t)dtop;
        if (unlikely(err != MDBX_SUCCESS))
          return err;
      }

      if (AUDIT_ENABLED()) {
        err = cursor_check(mc);
        if (unlikely(err != MDBX_SUCCESS))
          return err;
      }
      return MDBX_SUCCESS;
    }

  more:;
    if (AUDIT_ENABLED()) {
      err = cursor_check(mc);
      if (unlikely(err != MDBX_SUCCESS))
        return err;
    }
    MDBX_node *const node =
        page_node(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top]);

    /* Large/Overflow page overwrites need special handling */
    if (unlikely(node_flags(node) & F_BIGDATA)) {
      const size_t dpages = (node_size(key, data) > env->me_leaf_nodemax)
                                ? number_of_ovpages(env, data->iov_len)
                                : 0;

      const pgno_t pgno = node_largedata_pgno(node);
      pgr_t lp = page_get_large(mc, pgno, mc->mc_pg[mc->mc_top]->mp_txnid);
      if (unlikely(lp.err != MDBX_SUCCESS))
        return lp.err;
      cASSERT(mc, PAGETYPE_WHOLE(lp.page) == P_OVERFLOW);

      /* Is the ov page from this txn (or a parent) and big enough? */
      const size_t ovpages = lp.page->mp_pages;
      const size_t extra_threshold =
          (mc->mc_dbi == FREE_DBI)
              ? 1
              : /* LY: add configurable threshold to keep reserve space */ 0;
      if (!IS_FROZEN(mc->mc_txn, lp.page) && ovpages >= dpages &&
          ovpages <= dpages + extra_threshold) {
        /* yes, overwrite it. */
        if (!IS_MODIFIABLE(mc->mc_txn, lp.page)) {
          if (IS_SPILLED(mc->mc_txn, lp.page)) {
            lp = /* TODO: avoid search and get txn & spill-index from
                     page_result */
                page_unspill(mc->mc_txn, lp.page);
            if (unlikely(lp.err))
              return lp.err;
          } else {
            if (unlikely(!mc->mc_txn->mt_parent)) {
              ERROR("Unexpected not frozen/modifiable/spilled but shadowed %s "
                    "page %" PRIaPGNO " mod-txnid %" PRIaTXN ","
                    " without parent transaction, current txn %" PRIaTXN
                    " front %" PRIaTXN,
                    "overflow/large", pgno, lp.page->mp_txnid,
                    mc->mc_txn->mt_txnid, mc->mc_txn->mt_front);
              return MDBX_PROBLEM;
            }

            /* It is writable only in a parent txn */
            MDBX_page *np = page_malloc(mc->mc_txn, ovpages);
            if (unlikely(!np))
              return MDBX_ENOMEM;

            memcpy(np, lp.page, PAGEHDRSZ); /* Copy header of page */
            err = page_dirty(mc->mc_txn, lp.page = np, ovpages);
            if (unlikely(err != MDBX_SUCCESS))
              return err;

#if MDBX_ENABLE_PGOP_STAT
            mc->mc_txn->mt_env->me_lck->mti_pgop_stat.clone.weak += ovpages;
#endif /* MDBX_ENABLE_PGOP_STAT */
            cASSERT(mc, dirtylist_check(mc->mc_txn));
          }
        }
        node_set_ds(node, data->iov_len);
        if (flags & MDBX_RESERVE)
          data->iov_base = page_data(lp.page);
        else
          memcpy(page_data(lp.page), data->iov_base, data->iov_len);

        if (AUDIT_ENABLED()) {
          err = cursor_check(mc);
          if (unlikely(err != MDBX_SUCCESS))
            return err;
        }
        return MDBX_SUCCESS;
      }

      if ((err = page_retire(mc, lp.page)) != MDBX_SUCCESS)
        return err;
    } else {
      old_data.iov_len = node_ds(node);
      old_data.iov_base = node_data(node);
      cASSERT(mc, ptr_disp(old_data.iov_base, old_data.iov_len) <=
                      ptr_disp(mc->mc_pg[mc->mc_top], env->me_psize));

      /* DB has dups? */
      if (mc->mc_db->md_flags & MDBX_DUPSORT) {
        /* Prepare (sub-)page/sub-DB to accept the new item, if needed.
         * fp: old sub-page or a header faking it.
         * mp: new (sub-)page.
         * xdata: node data with new sub-page or sub-DB. */
        size_t growth = 0; /* growth in page size.*/
        MDBX_page *mp = fp = xdata.iov_base = env->me_pbuf;
        mp->mp_pgno = mc->mc_pg[mc->mc_top]->mp_pgno;

        /* Was a single item before, must convert now */
        if (!(node_flags(node) & F_DUPDATA)) {
          /* does data match? */
          if (flags & MDBX_APPENDDUP) {
            const int cmp = mc->mc_dbx->md_dcmp(data, &old_data);
            cASSERT(mc, cmp != 0 || eq_fast(data, &old_data));
            if (unlikely(cmp <= 0))
              return MDBX_EKEYMISMATCH;
          } else if (eq_fast(data, &old_data)) {
            cASSERT(mc, mc->mc_dbx->md_dcmp(data, &old_data) == 0);
            if (flags & MDBX_NODUPDATA)
              return MDBX_KEYEXIST;
            /* data is match exactly byte-to-byte, nothing to update */
            rc = MDBX_SUCCESS;
            if (unlikely(batch_dupfixed_done))
              goto batch_dupfixed_continue;
            return rc;
          }

          /* Just overwrite the current item */
          if (flags & MDBX_CURRENT) {
            cASSERT(mc, node_size(key, data) <= env->me_leaf_nodemax);
            goto current;
          }

          /* Back up original data item */
          memcpy(old_singledup.iov_base = fp + 1, old_data.iov_base,
                 old_singledup.iov_len = old_data.iov_len);

          /* Make sub-page header for the dup items, with dummy body */
          fp->mp_flags = P_LEAF | P_SUBP;
          fp->mp_lower = 0;
          xdata.iov_len = PAGEHDRSZ + old_data.iov_len + data->iov_len;
          if (mc->mc_db->md_flags & MDBX_DUPFIXED) {
            fp->mp_flags |= P_LEAF2;
            fp->mp_leaf2_ksize = (uint16_t)data->iov_len;
            /* Будем создавать LEAF2-страницу, как минимум с двумя элементами.
             * При коротких значениях и наличии свободного места можно сделать
             * некоторое резервирование места, чтобы при последующих добавлениях
             * не сразу расширять созданную под-страницу.
             * Резервирование в целом сомнительно (см ниже), но может сработать
             * в плюс (а если в минус то несущественный) при коротких ключах. */
            xdata.iov_len += leaf2_reserve(
                env, page_room(mc->mc_pg[mc->mc_top]) + old_data.iov_len,
                xdata.iov_len, data->iov_len);
            cASSERT(mc, (xdata.iov_len & 1) == 0);
          } else {
            xdata.iov_len += 2 * (sizeof(indx_t) + NODESIZE) +
                             (old_data.iov_len & 1) + (data->iov_len & 1);
          }
          cASSERT(mc, (xdata.iov_len & 1) == 0);
          fp->mp_upper = (uint16_t)(xdata.iov_len - PAGEHDRSZ);
          old_data.iov_len = xdata.iov_len; /* pretend olddata is fp */
        } else if (node_flags(node) & F_SUBDATA) {
          /* Data is on sub-DB, just store it */
          flags |= F_DUPDATA | F_SUBDATA;
          goto dupsort_put;
        } else {
          /* Data is on sub-page */
          fp = old_data.iov_base;
          switch (flags) {
          default:
            growth = IS_LEAF2(fp) ? fp->mp_leaf2_ksize
                                  : (node_size(data, nullptr) + sizeof(indx_t));
            if (page_room(fp) >= growth) {
              /* На текущей под-странице есть место для добавления элемента.
               * Оптимальнее продолжить использовать эту страницу, ибо
               * добавление вложенного дерева увеличит WAF на одну страницу. */
              goto continue_subpage;
            }
            /* На текущей под-странице нет места для еще одного элемента.
             * Можно либо увеличить эту под-страницу, либо вынести куст
             * значений во вложенное дерево.
             *
             * Продолжать использовать текущую под-страницу возможно
             * только пока и если размер после добавления элемента будет
             * меньше me_leaf_nodemax. Соответственно, при превышении
             * просто сразу переходим на вложенное дерево. */
            xdata.iov_len = old_data.iov_len + (growth += growth & 1);
            if (xdata.iov_len > env->me_subpage_limit)
              goto convert_to_subtree;

            /* Можно либо увеличить под-страницу, в том числе с некоторым
             * запасом, либо перейти на вложенное поддерево.
             *
             * Резервирование места на под-странице представляется сомнительным:
             *  - Резервирование увеличит рыхлость страниц, в том числе
             *    вероятность разделения основной/гнездовой страницы;
             *  - Сложно предсказать полезный размер резервирования,
             *    особенно для не-MDBX_DUPFIXED;
             *  - Наличие резерва позволяет съекономить только на перемещении
             *    части элементов основной/гнездовой страницы при последующих
             *    добавлениях в нее элементов. Причем после первого изменения
             *    размера под-страницы, её тело будет примыкать
             *    к неиспользуемому месту на основной/гнездовой странице,
             *    поэтому последующие последовательные добавления потребуют
             *    только передвижения в mp_ptrs[].
             *
             * Соответственно, более важным/определяющим представляется
             * своевременный переход к вложеному дереву, но тут достаточно
             * сложный конфликт интересов:
             *  - При склонности к переходу к вложенным деревьям, суммарно
             *    в БД будет большее кол-во более рыхлых страниц. Это увеличит
             *    WAF, а также RAF при последовательных чтениях большой БД.
             *    Однако, при коротких ключах и большом кол-ве
             *    дубликатов/мультизначений, плотность ключей в листовых
             *    страницах основного дерева будет выше. Соответственно, будет
             *    пропорционально меньше branch-страниц. Поэтому будет выше
             *    вероятность оседания/не-вымывания страниц основного дерева из
             *    LRU-кэша, а также попадания в write-back кэш при записи.
             *  - Наоботот, при склонности к использованию под-страниц, будут
             *    наблюдаться обратные эффекты. Плюс некоторые накладные расходы
             *    на лишнее копирование данных под-страниц в сценариях
             *    нескольких обонвлений дубликатов одного куста в одной
             *    транзакции.
             *
             * Суммарно наиболее рациональным представляется такая тактика:
             *  - Вводим три порога subpage_limit, subpage_room_threshold
             *    и subpage_reserve_prereq, которые могут быть
             *    заданы/скорректированы пользователем в ‰ от me_leaf_nodemax;
             *  - Используем под-страницу пока её размер меньше subpage_limit
             *    и на основной/гнездовой странице не-менее
             *    subpage_room_threshold свободного места;
             *  - Резервируем место только для 1-3 коротких dupfixed-элементов,
             *    расширяя размер под-страницы на размер кэш-линии ЦПУ, но
             *    только если на странице не менее subpage_reserve_prereq
             *    свободного места.
             *  - По-умолчанию устанавливаем:
             *     subpage_limit = me_leaf_nodemax (1000‰);
             *     subpage_room_threshold = 0;
             *     subpage_reserve_prereq = me_leaf_nodemax (1000‰).
             */
            if (IS_LEAF2(fp))
              growth += leaf2_reserve(
                  env, page_room(mc->mc_pg[mc->mc_top]) + old_data.iov_len,
                  xdata.iov_len, data->iov_len);
            break;

          case MDBX_CURRENT | MDBX_NODUPDATA:
          case MDBX_CURRENT:
          continue_subpage:
            fp->mp_txnid = mc->mc_txn->mt_front;
            fp->mp_pgno = mp->mp_pgno;
            mc->mc_xcursor->mx_cursor.mc_pg[0] = fp;
            flags |= F_DUPDATA;
            goto dupsort_put;
          }
          xdata.iov_len = old_data.iov_len + growth;
          cASSERT(mc, (xdata.iov_len & 1) == 0);
        }

        fp_flags = fp->mp_flags;
        if (xdata.iov_len > env->me_subpage_limit ||
            node_size_len(node_ks(node), xdata.iov_len) >
                env->me_leaf_nodemax ||
            (env->me_subpage_room_threshold &&
             page_room(mc->mc_pg[mc->mc_top]) +
                     node_size_len(node_ks(node), old_data.iov_len) <
                 env->me_subpage_room_threshold +
                     node_size_len(node_ks(node), xdata.iov_len))) {
          /* Too big for a sub-page, convert to sub-DB */
        convert_to_subtree:
          fp_flags &= ~P_SUBP;
          nested_dupdb.md_xsize = 0;
          nested_dupdb.md_flags = flags_db2sub(mc->mc_db->md_flags);
          if (mc->mc_db->md_flags & MDBX_DUPFIXED) {
            fp_flags |= P_LEAF2;
            nested_dupdb.md_xsize = fp->mp_leaf2_ksize;
          }
          nested_dupdb.md_depth = 1;
          nested_dupdb.md_branch_pages = 0;
          nested_dupdb.md_leaf_pages = 1;
          nested_dupdb.md_overflow_pages = 0;
          nested_dupdb.md_entries = page_numkeys(fp);
          xdata.iov_len = sizeof(nested_dupdb);
          xdata.iov_base = &nested_dupdb;
          const pgr_t par = page_alloc(mc);
          mp = par.page;
          if (unlikely(par.err != MDBX_SUCCESS))
            return par.err;
          mc->mc_db->md_leaf_pages += 1;
          cASSERT(mc, env->me_psize > old_data.iov_len);
          growth = env->me_psize - (unsigned)old_data.iov_len;
          cASSERT(mc, (growth & 1) == 0);
          flags |= F_DUPDATA | F_SUBDATA;
          nested_dupdb.md_root = mp->mp_pgno;
          nested_dupdb.md_seq = 0;
          nested_dupdb.md_mod_txnid = mc->mc_txn->mt_txnid;
          sub_root = mp;
        }
        if (mp != fp) {
          mp->mp_flags = fp_flags;
          mp->mp_txnid = mc->mc_txn->mt_front;
          mp->mp_leaf2_ksize = fp->mp_leaf2_ksize;
          mp->mp_lower = fp->mp_lower;
          cASSERT(mc, fp->mp_upper + growth < UINT16_MAX);
          mp->mp_upper = fp->mp_upper + (indx_t)growth;
          if (unlikely(fp_flags & P_LEAF2)) {
            memcpy(page_data(mp), page_data(fp),
                   page_numkeys(fp) * fp->mp_leaf2_ksize);
            cASSERT(mc,
                    (((mp->mp_leaf2_ksize & page_numkeys(mp)) ^ mp->mp_upper) &
                     1) == 0);
          } else {
            cASSERT(mc, (mp->mp_upper & 1) == 0);
            memcpy(ptr_disp(mp, mp->mp_upper + PAGEHDRSZ),
                   ptr_disp(fp, fp->mp_upper + PAGEHDRSZ),
                   old_data.iov_len - fp->mp_upper - PAGEHDRSZ);
            memcpy(mp->mp_ptrs, fp->mp_ptrs,
                   page_numkeys(fp) * sizeof(mp->mp_ptrs[0]));
            for (size_t i = 0; i < page_numkeys(fp); i++) {
              cASSERT(mc, mp->mp_ptrs[i] + growth <= UINT16_MAX);
              mp->mp_ptrs[i] += (indx_t)growth;
            }
          }
        }

        if (!insert_key)
          node_del(mc, 0);
        ref_data = &xdata;
        flags |= F_DUPDATA;
        goto insert_node;
      }

      /* MDBX passes F_SUBDATA in 'flags' to write a DB record */
      if (unlikely((node_flags(node) ^ flags) & F_SUBDATA))
        return MDBX_INCOMPATIBLE;

    current:
      if (data->iov_len == old_data.iov_len) {
        cASSERT(mc, EVEN(key->iov_len) == EVEN(node_ks(node)));
        /* same size, just replace it. Note that we could
         * also reuse this node if the new data is smaller,
         * but instead we opt to shrink the node in that case. */
        if (flags & MDBX_RESERVE)
          data->iov_base = old_data.iov_base;
        else if (!(mc->mc_flags & C_SUB))
          memcpy(old_data.iov_base, data->iov_base, data->iov_len);
        else {
          cASSERT(mc, page_numkeys(mc->mc_pg[mc->mc_top]) == 1);
          cASSERT(mc, PAGETYPE_COMPAT(mc->mc_pg[mc->mc_top]) == P_LEAF);
          cASSERT(mc, node_ds(node) == 0);
          cASSERT(mc, node_flags(node) == 0);
          cASSERT(mc, key->iov_len < UINT16_MAX);
          node_set_ks(node, key->iov_len);
          memcpy(node_key(node), key->iov_base, key->iov_len);
          cASSERT(mc, ptr_disp(node_key(node), node_ds(node)) <
                          ptr_disp(mc->mc_pg[mc->mc_top], env->me_psize));
          goto fix_parent;
        }

        if (AUDIT_ENABLED()) {
          err = cursor_check(mc);
          if (unlikely(err != MDBX_SUCCESS))
            return err;
        }
        return MDBX_SUCCESS;
      }
    }
    node_del(mc, 0);
  }

  ref_data = data;

insert_node:;
  const unsigned naf = flags & NODE_ADD_FLAGS;
  size_t nsize = IS_LEAF2(mc->mc_pg[mc->mc_top])
                     ? key->iov_len
                     : leaf_size(env, key, ref_data);
  if (page_room(mc->mc_pg[mc->mc_top]) < nsize) {
    rc = page_split(mc, key, ref_data, P_INVALID,
                    insert_key ? naf : naf | MDBX_SPLIT_REPLACE);
    if (rc == MDBX_SUCCESS && AUDIT_ENABLED())
      rc = insert_key ? cursor_check(mc) : cursor_check_updating(mc);
  } else {
    /* There is room already in this leaf page. */
    if (IS_LEAF2(mc->mc_pg[mc->mc_top])) {
      cASSERT(mc, !(naf & (F_BIGDATA | F_SUBDATA | F_DUPDATA)) &&
                      ref_data->iov_len == 0);
      rc = node_add_leaf2(mc, mc->mc_ki[mc->mc_top], key);
    } else
      rc = node_add_leaf(mc, mc->mc_ki[mc->mc_top], key, ref_data, naf);
    if (likely(rc == 0)) {
      /* Adjust other cursors pointing to mp */
      const MDBX_dbi dbi = mc->mc_dbi;
      const size_t top = mc->mc_top;
      MDBX_page *const mp = mc->mc_pg[top];
      for (MDBX_cursor *m2 = mc->mc_txn->mt_cursors[dbi]; m2;
           m2 = m2->mc_next) {
        MDBX_cursor *m3 =
            (mc->mc_flags & C_SUB) ? &m2->mc_xcursor->mx_cursor : m2;
        if (m3 == mc || m3->mc_snum < mc->mc_snum || m3->mc_pg[top] != mp)
          continue;
        if (m3->mc_ki[top] >= mc->mc_ki[top])
          m3->mc_ki[top] += insert_key;
        if (XCURSOR_INITED(m3))
          XCURSOR_REFRESH(m3, mp, m3->mc_ki[top]);
      }
    }
  }

  if (likely(rc == MDBX_SUCCESS)) {
    /* Now store the actual data in the child DB. Note that we're
     * storing the user data in the keys field, so there are strict
     * size limits on dupdata. The actual data fields of the child
     * DB are all zero size. */
    if (flags & F_DUPDATA) {
      MDBX_val empty;
    dupsort_put:
      empty.iov_len = 0;
      empty.iov_base = nullptr;
      MDBX_node *node = page_node(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top]);
#define SHIFT_MDBX_NODUPDATA_TO_MDBX_NOOVERWRITE 1
      STATIC_ASSERT(
          (MDBX_NODUPDATA >> SHIFT_MDBX_NODUPDATA_TO_MDBX_NOOVERWRITE) ==
          MDBX_NOOVERWRITE);
      unsigned xflags =
          MDBX_CURRENT | ((flags & MDBX_NODUPDATA) >>
                          SHIFT_MDBX_NODUPDATA_TO_MDBX_NOOVERWRITE);
      if ((flags & MDBX_CURRENT) == 0) {
        xflags -= MDBX_CURRENT;
        err = cursor_xinit1(mc, node, mc->mc_pg[mc->mc_top]);
        if (unlikely(err != MDBX_SUCCESS))
          return err;
      }
      if (sub_root)
        mc->mc_xcursor->mx_cursor.mc_pg[0] = sub_root;
      /* converted, write the original data first */
      if (old_singledup.iov_base) {
        rc = cursor_put_nochecklen(&mc->mc_xcursor->mx_cursor, &old_singledup,
                                   &empty, xflags);
        if (unlikely(rc))
          goto dupsort_error;
      }
      if (!(node_flags(node) & F_SUBDATA) || sub_root) {
        /* Adjust other cursors pointing to mp */
        MDBX_xcursor *const mx = mc->mc_xcursor;
        const size_t top = mc->mc_top;
        MDBX_page *const mp = mc->mc_pg[top];
        const intptr_t nkeys = page_numkeys(mp);

        for (MDBX_cursor *m2 = mc->mc_txn->mt_cursors[mc->mc_dbi]; m2;
             m2 = m2->mc_next) {
          if (m2 == mc || m2->mc_snum < mc->mc_snum)
            continue;
          if (!(m2->mc_flags & C_INITIALIZED))
            continue;
          if (m2->mc_pg[top] == mp) {
            if (m2->mc_ki[top] == mc->mc_ki[top]) {
              err = cursor_xinit2(m2, mx, old_singledup.iov_base != nullptr);
              if (unlikely(err != MDBX_SUCCESS))
                return err;
            } else if (!insert_key && m2->mc_ki[top] < nkeys) {
              XCURSOR_REFRESH(m2, mp, m2->mc_ki[top]);
            }
          }
        }
      }
      cASSERT(mc, mc->mc_xcursor->mx_db.md_entries < PTRDIFF_MAX);
      const size_t probe = (size_t)mc->mc_xcursor->mx_db.md_entries;
#define SHIFT_MDBX_APPENDDUP_TO_MDBX_APPEND 1
      STATIC_ASSERT((MDBX_APPENDDUP >> SHIFT_MDBX_APPENDDUP_TO_MDBX_APPEND) ==
                    MDBX_APPEND);
      xflags |= (flags & MDBX_APPENDDUP) >> SHIFT_MDBX_APPENDDUP_TO_MDBX_APPEND;
      rc = cursor_put_nochecklen(&mc->mc_xcursor->mx_cursor, data, &empty,
                                 xflags);
      if (flags & F_SUBDATA) {
        void *db = node_data(node);
        mc->mc_xcursor->mx_db.md_mod_txnid = mc->mc_txn->mt_txnid;
        memcpy(db, &mc->mc_xcursor->mx_db, sizeof(MDBX_db));
      }
      insert_data = (probe != (size_t)mc->mc_xcursor->mx_db.md_entries);
    }
    /* Increment count unless we just replaced an existing item. */
    if (insert_data)
      mc->mc_db->md_entries++;
    if (insert_key) {
      if (unlikely(rc != MDBX_SUCCESS))
        goto dupsort_error;
      /* If we succeeded and the key didn't exist before,
       * make sure the cursor is marked valid. */
      mc->mc_flags |= C_INITIALIZED;
    }
    if (likely(rc == MDBX_SUCCESS)) {
      if (unlikely(batch_dupfixed_done)) {
      batch_dupfixed_continue:
        /* let caller know how many succeeded, if any */
        if ((*batch_dupfixed_done += 1) < batch_dupfixed_given) {
          data[0].iov_base = ptr_disp(data[0].iov_base, data[0].iov_len);
          insert_key = insert_data = false;
          old_singledup.iov_base = nullptr;
          goto more;
        }
      }
      if (AUDIT_ENABLED())
        rc = cursor_check(mc);
    }
    return rc;

  dupsort_error:
    if (unlikely(rc == MDBX_KEYEXIST)) {
      /* should not happen, we deleted that item */
      ERROR("Unexpected %i error while put to nested dupsort's hive", rc);
      rc = MDBX_PROBLEM;
    }
  }
  mc->mc_txn->mt_flags |= MDBX_TXN_ERROR;
  return rc;
}

static __hot int cursor_put_checklen(MDBX_cursor *mc, const MDBX_val *key,
                                     MDBX_val *data, unsigned flags) {
  cASSERT(mc, (mc->mc_flags & C_SUB) == 0);
  uint64_t aligned_keybytes, aligned_databytes;
  MDBX_val aligned_key, aligned_data;
  if (unlikely(key->iov_len < mc->mc_dbx->md_klen_min ||
               key->iov_len > mc->mc_dbx->md_klen_max)) {
    cASSERT(mc, !"Invalid key-size");
    return MDBX_BAD_VALSIZE;
  }
  if (unlikely(data->iov_len < mc->mc_dbx->md_vlen_min ||
               data->iov_len > mc->mc_dbx->md_vlen_max)) {
    cASSERT(mc, !"Invalid data-size");
    return MDBX_BAD_VALSIZE;
  }

  if (mc->mc_db->md_flags & MDBX_INTEGERKEY) {
    switch (key->iov_len) {
    default:
      cASSERT(mc, !"key-size is invalid for MDBX_INTEGERKEY");
      return MDBX_BAD_VALSIZE;
    case 4:
      if (unlikely(3 & (uintptr_t)key->iov_base)) {
        /* copy instead of return error to avoid break compatibility */
        aligned_key.iov_base =
            memcpy(&aligned_keybytes, key->iov_base, aligned_key.iov_len = 4);
        key = &aligned_key;
      }
      break;
    case 8:
      if (unlikely(7 & (uintptr_t)key->iov_base)) {
        /* copy instead of return error to avoid break compatibility */
        aligned_key.iov_base =
            memcpy(&aligned_keybytes, key->iov_base, aligned_key.iov_len = 8);
        key = &aligned_key;
      }
      break;
    }
  }
  if (mc->mc_db->md_flags & MDBX_INTEGERDUP) {
    switch (data->iov_len) {
    default:
      cASSERT(mc, !"data-size is invalid for MDBX_INTEGERKEY");
      return MDBX_BAD_VALSIZE;
    case 4:
      if (unlikely(3 & (uintptr_t)data->iov_base)) {
        if (unlikely(flags & MDBX_MULTIPLE))
          return MDBX_BAD_VALSIZE;
        /* copy instead of return error to avoid break compatibility */
        aligned_data.iov_base = memcpy(&aligned_databytes, data->iov_base,
                                       aligned_data.iov_len = 4);
        data = &aligned_data;
      }
      break;
    case 8:
      if (unlikely(7 & (uintptr_t)data->iov_base)) {
        if (unlikely(flags & MDBX_MULTIPLE))
          return MDBX_BAD_VALSIZE;
        /* copy instead of return error to avoid break compatibility */
        aligned_data.iov_base = memcpy(&aligned_databytes, data->iov_base,
                                       aligned_data.iov_len = 8);
        data = &aligned_data;
      }
      break;
    }
  }
  return cursor_put_nochecklen(mc, key, data, flags);
}

int mdbx_cursor_put(MDBX_cursor *mc, const MDBX_val *key, MDBX_val *data,
                    MDBX_put_flags_t flags) {
  if (unlikely(mc == NULL || key == NULL || data == NULL))
    return MDBX_EINVAL;

  if (unlikely(mc->mc_signature != MDBX_MC_LIVE))
    return (mc->mc_signature == MDBX_MC_READY4CLOSE) ? MDBX_EINVAL
                                                     : MDBX_EBADSIGN;

  int rc = check_txn_rw(mc->mc_txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(dbi_changed(mc->mc_txn, mc->mc_dbi)))
    return MDBX_BAD_DBI;

  cASSERT(mc, cursor_is_tracked(mc));

  /* Check this first so counter will always be zero on any early failures. */
  if (unlikely(flags & MDBX_MULTIPLE)) {
    if (unlikely(flags & MDBX_RESERVE))
      return MDBX_EINVAL;
    if (unlikely(!(mc->mc_db->md_flags & MDBX_DUPFIXED)))
      return MDBX_INCOMPATIBLE;
    const size_t dcount = data[1].iov_len;
    if (unlikely(dcount < 2 || data->iov_len == 0))
      return MDBX_BAD_VALSIZE;
    if (unlikely(mc->mc_db->md_xsize != data->iov_len) && mc->mc_db->md_xsize)
      return MDBX_BAD_VALSIZE;
    if (unlikely(dcount > MAX_MAPSIZE / 2 /
                              (BRANCH_NODE_MAX(MAX_PAGESIZE) - NODESIZE))) {
      /* checking for multiplication overflow */
      if (unlikely(dcount > MAX_MAPSIZE / 2 / data->iov_len))
        return MDBX_TOO_LARGE;
    }
  }

  if (flags & MDBX_RESERVE) {
    if (unlikely(mc->mc_db->md_flags & (MDBX_DUPSORT | MDBX_REVERSEDUP |
                                        MDBX_INTEGERDUP | MDBX_DUPFIXED)))
      return MDBX_INCOMPATIBLE;
    data->iov_base = nullptr;
  }

  if (unlikely(mc->mc_txn->mt_flags & (MDBX_TXN_RDONLY | MDBX_TXN_BLOCKED)))
    return (mc->mc_txn->mt_flags & MDBX_TXN_RDONLY) ? MDBX_EACCESS
                                                    : MDBX_BAD_TXN;

  return cursor_put_checklen(mc, key, data, flags);
}

int mdbx_cursor_del(MDBX_cursor *mc, MDBX_put_flags_t flags) {
  if (unlikely(!mc))
    return MDBX_EINVAL;

  if (unlikely(mc->mc_signature != MDBX_MC_LIVE))
    return (mc->mc_signature == MDBX_MC_READY4CLOSE) ? MDBX_EINVAL
                                                     : MDBX_EBADSIGN;

  int rc = check_txn_rw(mc->mc_txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(dbi_changed(mc->mc_txn, mc->mc_dbi)))
    return MDBX_BAD_DBI;

  if (unlikely(!(mc->mc_flags & C_INITIALIZED)))
    return MDBX_ENODATA;

  if (unlikely(mc->mc_ki[mc->mc_top] >= page_numkeys(mc->mc_pg[mc->mc_top])))
    return MDBX_NOTFOUND;

  return cursor_del(mc, flags);
}

static __hot int cursor_del(MDBX_cursor *mc, MDBX_put_flags_t flags) {
  cASSERT(mc, mc->mc_flags & C_INITIALIZED);
  cASSERT(mc, mc->mc_ki[mc->mc_top] < page_numkeys(mc->mc_pg[mc->mc_top]));

  int rc = cursor_touch(mc, nullptr, nullptr);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  MDBX_page *mp = mc->mc_pg[mc->mc_top];
  cASSERT(mc, IS_MODIFIABLE(mc->mc_txn, mp));
  if (!MDBX_DISABLE_VALIDATION && unlikely(!CHECK_LEAF_TYPE(mc, mp))) {
    ERROR("unexpected leaf-page #%" PRIaPGNO " type 0x%x seen by cursor",
          mp->mp_pgno, mp->mp_flags);
    return MDBX_CORRUPTED;
  }
  if (IS_LEAF2(mp))
    goto del_key;

  MDBX_node *node = page_node(mp, mc->mc_ki[mc->mc_top]);
  if (node_flags(node) & F_DUPDATA) {
    if (flags & (MDBX_ALLDUPS | /* for compatibility */ MDBX_NODUPDATA)) {
      /* will subtract the final entry later */
      mc->mc_db->md_entries -= mc->mc_xcursor->mx_db.md_entries - 1;
      mc->mc_xcursor->mx_cursor.mc_flags &= ~C_INITIALIZED;
    } else {
      if (!(node_flags(node) & F_SUBDATA))
        mc->mc_xcursor->mx_cursor.mc_pg[0] = node_data(node);
      rc = cursor_del(&mc->mc_xcursor->mx_cursor, 0);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
      /* If sub-DB still has entries, we're done */
      if (mc->mc_xcursor->mx_db.md_entries) {
        if (node_flags(node) & F_SUBDATA) {
          /* update subDB info */
          mc->mc_xcursor->mx_db.md_mod_txnid = mc->mc_txn->mt_txnid;
          memcpy(node_data(node), &mc->mc_xcursor->mx_db, sizeof(MDBX_db));
        } else {
          /* shrink sub-page */
          node = node_shrink(mp, mc->mc_ki[mc->mc_top], node);
          mc->mc_xcursor->mx_cursor.mc_pg[0] = node_data(node);
          /* fix other sub-DB cursors pointed at sub-pages on this page */
          for (MDBX_cursor *m2 = mc->mc_txn->mt_cursors[mc->mc_dbi]; m2;
               m2 = m2->mc_next) {
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
        cASSERT(mc, mc->mc_db->md_entries > 0 && mc->mc_db->md_depth > 0 &&
                        mc->mc_db->md_root != P_INVALID);
        return rc;
      } else {
        mc->mc_xcursor->mx_cursor.mc_flags &= ~C_INITIALIZED;
      }
      /* otherwise fall thru and delete the sub-DB */
    }

    if (node_flags(node) & F_SUBDATA) {
      /* add all the child DB's pages to the free list */
      rc = drop_tree(&mc->mc_xcursor->mx_cursor, false);
      if (unlikely(rc))
        goto fail;
    }
  }
  /* MDBX passes F_SUBDATA in 'flags' to delete a DB record */
  else if (unlikely((node_flags(node) ^ flags) & F_SUBDATA))
    return MDBX_INCOMPATIBLE;

  /* add large/overflow pages to free list */
  if (node_flags(node) & F_BIGDATA) {
    pgr_t lp = page_get_large(mc, node_largedata_pgno(node), mp->mp_txnid);
    if (unlikely((rc = lp.err) || (rc = page_retire(mc, lp.page))))
      goto fail;
  }

del_key:
  mc->mc_db->md_entries--;
  const MDBX_dbi dbi = mc->mc_dbi;
  indx_t ki = mc->mc_ki[mc->mc_top];
  mp = mc->mc_pg[mc->mc_top];
  cASSERT(mc, IS_LEAF(mp));
  node_del(mc, mc->mc_db->md_xsize);

  /* Adjust other cursors pointing to mp */
  for (MDBX_cursor *m2 = mc->mc_txn->mt_cursors[dbi]; m2; m2 = m2->mc_next) {
    MDBX_cursor *m3 = (mc->mc_flags & C_SUB) ? &m2->mc_xcursor->mx_cursor : m2;
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

  rc = rebalance(mc);
  if (unlikely(rc != MDBX_SUCCESS))
    goto fail;

  if (unlikely(!mc->mc_snum)) {
    /* DB is totally empty now, just bail out.
     * Other cursors adjustments were already done
     * by rebalance and aren't needed here. */
    cASSERT(mc, mc->mc_db->md_entries == 0 && mc->mc_db->md_depth == 0 &&
                    mc->mc_db->md_root == P_INVALID);
    mc->mc_flags |= C_EOF;
    return MDBX_SUCCESS;
  }

  ki = mc->mc_ki[mc->mc_top];
  mp = mc->mc_pg[mc->mc_top];
  cASSERT(mc, IS_LEAF(mc->mc_pg[mc->mc_top]));
  size_t nkeys = page_numkeys(mp);
  cASSERT(mc, (mc->mc_db->md_entries > 0 && nkeys > 0) ||
                  ((mc->mc_flags & C_SUB) && mc->mc_db->md_entries == 0 &&
                   nkeys == 0));

  /* Adjust this and other cursors pointing to mp */
  for (MDBX_cursor *m2 = mc->mc_txn->mt_cursors[dbi]; m2; m2 = m2->mc_next) {
    MDBX_cursor *m3 = (mc->mc_flags & C_SUB) ? &m2->mc_xcursor->mx_cursor : m2;
    if (!(m2->mc_flags & m3->mc_flags & C_INITIALIZED))
      continue;
    if (m3->mc_snum < mc->mc_snum)
      continue;
    if (m3->mc_pg[mc->mc_top] == mp) {
      /* if m3 points past last node in page, find next sibling */
      if (m3->mc_ki[mc->mc_top] >= nkeys) {
        rc = cursor_sibling(m3, SIBLING_RIGHT);
        if (rc == MDBX_NOTFOUND) {
          m3->mc_flags |= C_EOF;
          rc = MDBX_SUCCESS;
          continue;
        }
        if (unlikely(rc != MDBX_SUCCESS))
          goto fail;
      }
      if (m3->mc_ki[mc->mc_top] >= ki ||
          /* moved to right sibling */ m3->mc_pg[mc->mc_top] != mp) {
        if (m3->mc_xcursor && !(m3->mc_flags & C_EOF)) {
          node = page_node(m3->mc_pg[m3->mc_top], m3->mc_ki[m3->mc_top]);
          /* If this node has dupdata, it may need to be reinited
           * because its data has moved.
           * If the xcursor was not inited it must be reinited.
           * Else if node points to a subDB, nothing is needed. */
          if (node_flags(node) & F_DUPDATA) {
            if (m3->mc_xcursor->mx_cursor.mc_flags & C_INITIALIZED) {
              if (!(node_flags(node) & F_SUBDATA))
                m3->mc_xcursor->mx_cursor.mc_pg[0] = node_data(node);
            } else {
              rc = cursor_xinit1(m3, node, m3->mc_pg[m3->mc_top]);
              if (unlikely(rc != MDBX_SUCCESS))
                goto fail;
              rc = cursor_first(&m3->mc_xcursor->mx_cursor, NULL, NULL);
              if (unlikely(rc != MDBX_SUCCESS))
                goto fail;
            }
          }
          m3->mc_xcursor->mx_cursor.mc_flags |= C_DEL;
        }
        m3->mc_flags |= C_DEL;
      }
    }
  }

  cASSERT(mc, rc == MDBX_SUCCESS);
  if (AUDIT_ENABLED())
    rc = cursor_check(mc);
  return rc;

fail:
  mc->mc_txn->mt_flags |= MDBX_TXN_ERROR;
  return rc;
}

/* Allocate and initialize new pages for a database.
 * Set MDBX_TXN_ERROR on failure. */
static pgr_t page_new(MDBX_cursor *mc, const unsigned flags) {
  cASSERT(mc, (flags & P_OVERFLOW) == 0);
  pgr_t ret = page_alloc(mc);
  if (unlikely(ret.err != MDBX_SUCCESS))
    return ret;

  DEBUG("db %u allocated new page %" PRIaPGNO, mc->mc_dbi, ret.page->mp_pgno);
  ret.page->mp_flags = (uint16_t)flags;
  cASSERT(mc, *mc->mc_dbistate & DBI_DIRTY);
  cASSERT(mc, mc->mc_txn->mt_flags & MDBX_TXN_DIRTY);
#if MDBX_ENABLE_PGOP_STAT
  mc->mc_txn->mt_env->me_lck->mti_pgop_stat.newly.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */

  STATIC_ASSERT(P_BRANCH == 1);
  const unsigned is_branch = flags & P_BRANCH;

  ret.page->mp_lower = 0;
  ret.page->mp_upper = (indx_t)(mc->mc_txn->mt_env->me_psize - PAGEHDRSZ);
  mc->mc_db->md_branch_pages += is_branch;
  mc->mc_db->md_leaf_pages += 1 - is_branch;
  if (unlikely(mc->mc_flags & C_SUB)) {
    MDBX_db *outer = outer_db(mc);
    outer->md_branch_pages += is_branch;
    outer->md_leaf_pages += 1 - is_branch;
  }
  return ret;
}

static pgr_t page_new_large(MDBX_cursor *mc, const size_t npages) {
  pgr_t ret = likely(npages == 1)
                  ? page_alloc(mc)
                  : page_alloc_slowpath(mc, npages, MDBX_ALLOC_DEFAULT);
  if (unlikely(ret.err != MDBX_SUCCESS))
    return ret;

  DEBUG("db %u allocated new large-page %" PRIaPGNO ", num %zu", mc->mc_dbi,
        ret.page->mp_pgno, npages);
  ret.page->mp_flags = P_OVERFLOW;
  cASSERT(mc, *mc->mc_dbistate & DBI_DIRTY);
  cASSERT(mc, mc->mc_txn->mt_flags & MDBX_TXN_DIRTY);
#if MDBX_ENABLE_PGOP_STAT
  mc->mc_txn->mt_env->me_lck->mti_pgop_stat.newly.weak += npages;
#endif /* MDBX_ENABLE_PGOP_STAT */

  mc->mc_db->md_overflow_pages += (pgno_t)npages;
  ret.page->mp_pages = (pgno_t)npages;
  cASSERT(mc, !(mc->mc_flags & C_SUB));
  return ret;
}

__hot static int __must_check_result node_add_leaf2(MDBX_cursor *mc,
                                                    size_t indx,
                                                    const MDBX_val *key) {
  MDBX_page *mp = mc->mc_pg[mc->mc_top];
  MDBX_ANALYSIS_ASSUME(key != nullptr);
  DKBUF_DEBUG;
  DEBUG("add to leaf2-%spage %" PRIaPGNO " index %zi, "
        " key size %" PRIuPTR " [%s]",
        IS_SUBP(mp) ? "sub-" : "", mp->mp_pgno, indx, key ? key->iov_len : 0,
        DKEY_DEBUG(key));

  cASSERT(mc, key);
  cASSERT(mc, PAGETYPE_COMPAT(mp) == (P_LEAF | P_LEAF2));
  const size_t ksize = mc->mc_db->md_xsize;
  cASSERT(mc, ksize == key->iov_len);
  const size_t nkeys = page_numkeys(mp);
  cASSERT(mc, (((ksize & page_numkeys(mp)) ^ mp->mp_upper) & 1) == 0);

  /* Just using these for counting */
  const intptr_t lower = mp->mp_lower + sizeof(indx_t);
  const intptr_t upper = mp->mp_upper - (ksize - sizeof(indx_t));
  if (unlikely(lower > upper)) {
    mc->mc_txn->mt_flags |= MDBX_TXN_ERROR;
    return MDBX_PAGE_FULL;
  }
  mp->mp_lower = (indx_t)lower;
  mp->mp_upper = (indx_t)upper;

  void *const ptr = page_leaf2key(mp, indx, ksize);
  cASSERT(mc, nkeys >= indx);
  const size_t diff = nkeys - indx;
  if (likely(diff > 0))
    /* Move higher keys up one slot. */
    memmove(ptr_disp(ptr, ksize), ptr, diff * ksize);
  /* insert new key */
  memcpy(ptr, key->iov_base, ksize);

  cASSERT(mc, (((ksize & page_numkeys(mp)) ^ mp->mp_upper) & 1) == 0);
  return MDBX_SUCCESS;
}

static int __must_check_result node_add_branch(MDBX_cursor *mc, size_t indx,
                                               const MDBX_val *key,
                                               pgno_t pgno) {
  MDBX_page *mp = mc->mc_pg[mc->mc_top];
  DKBUF_DEBUG;
  DEBUG("add to branch-%spage %" PRIaPGNO " index %zi, node-pgno %" PRIaPGNO
        " key size %" PRIuPTR " [%s]",
        IS_SUBP(mp) ? "sub-" : "", mp->mp_pgno, indx, pgno,
        key ? key->iov_len : 0, DKEY_DEBUG(key));

  cASSERT(mc, PAGETYPE_WHOLE(mp) == P_BRANCH);
  STATIC_ASSERT(NODESIZE % 2 == 0);

  /* Move higher pointers up one slot. */
  const size_t nkeys = page_numkeys(mp);
  cASSERT(mc, nkeys >= indx);
  for (size_t i = nkeys; i > indx; --i)
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

__hot static int __must_check_result node_add_leaf(MDBX_cursor *mc, size_t indx,
                                                   const MDBX_val *key,
                                                   MDBX_val *data,
                                                   unsigned flags) {
  MDBX_ANALYSIS_ASSUME(key != nullptr);
  MDBX_ANALYSIS_ASSUME(data != nullptr);
  MDBX_page *mp = mc->mc_pg[mc->mc_top];
  DKBUF_DEBUG;
  DEBUG("add to leaf-%spage %" PRIaPGNO " index %zi, data size %" PRIuPTR
        " key size %" PRIuPTR " [%s]",
        IS_SUBP(mp) ? "sub-" : "", mp->mp_pgno, indx, data ? data->iov_len : 0,
        key ? key->iov_len : 0, DKEY_DEBUG(key));
  cASSERT(mc, key != NULL && data != NULL);
  cASSERT(mc, PAGETYPE_COMPAT(mp) == P_LEAF);
  MDBX_page *largepage = NULL;

  size_t node_bytes;
  if (unlikely(flags & F_BIGDATA)) {
    /* Data already on large/overflow page. */
    STATIC_ASSERT(sizeof(pgno_t) % 2 == 0);
    node_bytes =
        node_size_len(key->iov_len, 0) + sizeof(pgno_t) + sizeof(indx_t);
    cASSERT(mc, page_room(mp) >= node_bytes);
  } else if (unlikely(node_size(key, data) >
                      mc->mc_txn->mt_env->me_leaf_nodemax)) {
    /* Put data on large/overflow page. */
    if (unlikely(mc->mc_db->md_flags & MDBX_DUPSORT)) {
      ERROR("Unexpected target %s flags 0x%x for large data-item", "dupsort-db",
            mc->mc_db->md_flags);
      return MDBX_PROBLEM;
    }
    if (unlikely(flags & (F_DUPDATA | F_SUBDATA))) {
      ERROR("Unexpected target %s flags 0x%x for large data-item", "node",
            flags);
      return MDBX_PROBLEM;
    }
    cASSERT(mc, page_room(mp) >= leaf_size(mc->mc_txn->mt_env, key, data));
    const pgno_t ovpages = number_of_ovpages(mc->mc_txn->mt_env, data->iov_len);
    const pgr_t npr = page_new_large(mc, ovpages);
    if (unlikely(npr.err != MDBX_SUCCESS))
      return npr.err;
    largepage = npr.page;
    DEBUG("allocated %u large/overflow page(s) %" PRIaPGNO "for %" PRIuPTR
          " data bytes",
          largepage->mp_pages, largepage->mp_pgno, data->iov_len);
    flags |= F_BIGDATA;
    node_bytes =
        node_size_len(key->iov_len, 0) + sizeof(pgno_t) + sizeof(indx_t);
    cASSERT(mc, node_bytes == leaf_size(mc->mc_txn->mt_env, key, data));
  } else {
    cASSERT(mc, page_room(mp) >= leaf_size(mc->mc_txn->mt_env, key, data));
    node_bytes = node_size(key, data) + sizeof(indx_t);
    cASSERT(mc, node_bytes == leaf_size(mc->mc_txn->mt_env, key, data));
  }

  /* Move higher pointers up one slot. */
  const size_t nkeys = page_numkeys(mp);
  cASSERT(mc, nkeys >= indx);
  for (size_t i = nkeys; i > indx; --i)
    mp->mp_ptrs[i] = mp->mp_ptrs[i - 1];

  /* Adjust free space offsets. */
  const intptr_t lower = mp->mp_lower + sizeof(indx_t);
  const intptr_t upper = mp->mp_upper - (node_bytes - sizeof(indx_t));
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
    if (unlikely(flags & F_BIGDATA)) {
      memcpy(nodedata, data->iov_base, sizeof(pgno_t));
      return MDBX_SUCCESS;
    }
  } else {
    poke_pgno(nodedata, largepage->mp_pgno);
    nodedata = page_data(largepage);
  }
  if (unlikely(flags & MDBX_RESERVE))
    data->iov_base = nodedata;
  else if (likely(nodedata != data->iov_base &&
                  data->iov_len /* to avoid UBSAN traps*/ != 0))
    memcpy(nodedata, data->iov_base, data->iov_len);
  return MDBX_SUCCESS;
}

/* Delete the specified node from a page.
 * [in] mc Cursor pointing to the node to delete.
 * [in] ksize The size of a node. Only used if the page is
 * part of a MDBX_DUPFIXED database. */
__hot static void node_del(MDBX_cursor *mc, size_t ksize) {
  MDBX_page *mp = mc->mc_pg[mc->mc_top];
  const size_t hole = mc->mc_ki[mc->mc_top];
  const size_t nkeys = page_numkeys(mp);

  DEBUG("delete node %zu on %s page %" PRIaPGNO, hole,
        IS_LEAF(mp) ? "leaf" : "branch", mp->mp_pgno);
  cASSERT(mc, hole < nkeys);

  if (IS_LEAF2(mp)) {
    cASSERT(mc, ksize >= sizeof(indx_t));
    size_t diff = nkeys - 1 - hole;
    void *const base = page_leaf2key(mp, hole, ksize);
    if (diff)
      memmove(base, ptr_disp(base, ksize), diff * ksize);
    cASSERT(mc, mp->mp_lower >= sizeof(indx_t));
    mp->mp_lower -= sizeof(indx_t);
    cASSERT(mc, (size_t)UINT16_MAX - mp->mp_upper >= ksize - sizeof(indx_t));
    mp->mp_upper += (indx_t)(ksize - sizeof(indx_t));
    cASSERT(mc, (((ksize & page_numkeys(mp)) ^ mp->mp_upper) & 1) == 0);
    return;
  }

  MDBX_node *node = page_node(mp, hole);
  cASSERT(mc, !IS_BRANCH(mp) || hole || node_ks(node) == 0);
  size_t hole_size = NODESIZE + node_ks(node);
  if (IS_LEAF(mp))
    hole_size +=
        (node_flags(node) & F_BIGDATA) ? sizeof(pgno_t) : node_ds(node);
  hole_size = EVEN(hole_size);

  const indx_t hole_offset = mp->mp_ptrs[hole];
  size_t r, w;
  for (r = w = 0; r < nkeys; r++)
    if (r != hole)
      mp->mp_ptrs[w++] = (mp->mp_ptrs[r] < hole_offset)
                             ? mp->mp_ptrs[r] + (indx_t)hole_size
                             : mp->mp_ptrs[r];

  void *const base = ptr_disp(mp, mp->mp_upper + PAGEHDRSZ);
  memmove(ptr_disp(base, hole_size), base, hole_offset - mp->mp_upper);

  cASSERT(mc, mp->mp_lower >= sizeof(indx_t));
  mp->mp_lower -= sizeof(indx_t);
  cASSERT(mc, (size_t)UINT16_MAX - mp->mp_upper >= hole_size);
  mp->mp_upper += (indx_t)hole_size;

  if (AUDIT_ENABLED()) {
    const uint8_t checking = mc->mc_checking;
    mc->mc_checking |= CC_UPDATING;
    const int page_check_err = page_check(mc, mp);
    mc->mc_checking = checking;
    cASSERT(mc, page_check_err == MDBX_SUCCESS);
  }
}

/* Compact the main page after deleting a node on a subpage.
 * [in] mp The main page to operate on.
 * [in] indx The index of the subpage on the main page. */
static MDBX_node *node_shrink(MDBX_page *mp, size_t indx, MDBX_node *node) {
  assert(node = page_node(mp, indx));
  MDBX_page *sp = (MDBX_page *)node_data(node);
  assert(IS_SUBP(sp) && page_numkeys(sp) > 0);
  const size_t delta =
      EVEN_FLOOR(page_room(sp) /* avoid the node uneven-sized */);
  if (unlikely(delta) == 0)
    return node;

  /* Prepare to shift upward, set len = length(subpage part to shift) */
  size_t nsize = node_ds(node) - delta, len = nsize;
  assert(nsize % 1 == 0);
  if (!IS_LEAF2(sp)) {
    len = PAGEHDRSZ;
    MDBX_page *xp = ptr_disp(sp, delta); /* destination subpage */
    for (intptr_t i = page_numkeys(sp); --i >= 0;) {
      assert(sp->mp_ptrs[i] >= delta);
      xp->mp_ptrs[i] = (indx_t)(sp->mp_ptrs[i] - delta);
    }
  }
  assert(sp->mp_upper >= sp->mp_lower + delta);
  sp->mp_upper -= (indx_t)delta;
  sp->mp_pgno = mp->mp_pgno;
  node_set_ds(node, nsize);

  /* Shift <lower nodes...initial part of subpage> upward */
  void *const base = ptr_disp(mp, mp->mp_upper + PAGEHDRSZ);
  memmove(ptr_disp(base, delta), base, ptr_dist(sp, base) + len);

  const size_t pivot = mp->mp_ptrs[indx];
  for (intptr_t i = page_numkeys(mp); --i >= 0;) {
    if (mp->mp_ptrs[i] <= pivot) {
      assert((size_t)UINT16_MAX - mp->mp_ptrs[i] >= delta);
      mp->mp_ptrs[i] += (indx_t)delta;
    }
  }
  assert((size_t)UINT16_MAX - mp->mp_upper >= delta);
  mp->mp_upper += (indx_t)delta;

  return ptr_disp(node, delta);
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
static int cursor_xinit0(MDBX_cursor *mc) {
  MDBX_xcursor *mx = mc->mc_xcursor;
  if (!MDBX_DISABLE_VALIDATION && unlikely(mx == nullptr)) {
    ERROR("unexpected dupsort-page for non-dupsort db/cursor (dbi %u)",
          mc->mc_dbi);
    return MDBX_CORRUPTED;
  }

  mx->mx_cursor.mc_xcursor = NULL;
  mx->mx_cursor.mc_next = NULL;
  mx->mx_cursor.mc_txn = mc->mc_txn;
  mx->mx_cursor.mc_db = &mx->mx_db;
  mx->mx_cursor.mc_dbx = &mx->mx_dbx;
  mx->mx_cursor.mc_dbi = mc->mc_dbi;
  mx->mx_cursor.mc_dbistate = mc->mc_dbistate;
  mx->mx_cursor.mc_snum = 0;
  mx->mx_cursor.mc_top = 0;
  mx->mx_cursor.mc_flags = C_SUB;
  STATIC_ASSERT(MDBX_DUPFIXED * 2 == P_LEAF2);
  cASSERT(mc, (mc->mc_checking & (P_BRANCH | P_LEAF | P_LEAF2)) == P_LEAF);
  mx->mx_cursor.mc_checking =
      mc->mc_checking + ((mc->mc_db->md_flags & MDBX_DUPFIXED) << 1);
  mx->mx_dbx.md_name.iov_len = 0;
  mx->mx_dbx.md_name.iov_base = NULL;
  mx->mx_dbx.md_cmp = mc->mc_dbx->md_dcmp;
  mx->mx_dbx.md_dcmp = NULL;
  mx->mx_dbx.md_klen_min = INT_MAX;
  mx->mx_dbx.md_vlen_min = mx->mx_dbx.md_klen_max = mx->mx_dbx.md_vlen_max = 0;
  return MDBX_SUCCESS;
}

/* Final setup of a sorted-dups cursor.
 * Sets up the fields that depend on the data from the main cursor.
 * [in] mc The main cursor whose sorted-dups cursor is to be initialized.
 * [in] node The data containing the MDBX_db record for the sorted-dup database.
 */
static int cursor_xinit1(MDBX_cursor *mc, MDBX_node *node,
                         const MDBX_page *mp) {
  MDBX_xcursor *mx = mc->mc_xcursor;
  if (!MDBX_DISABLE_VALIDATION && unlikely(mx == nullptr)) {
    ERROR("unexpected dupsort-page for non-dupsort db/cursor (dbi %u)",
          mc->mc_dbi);
    return MDBX_CORRUPTED;
  }

  const uint8_t flags = node_flags(node);
  switch (flags) {
  default:
    ERROR("invalid node flags %u", flags);
    return MDBX_CORRUPTED;
  case F_DUPDATA | F_SUBDATA:
    if (!MDBX_DISABLE_VALIDATION &&
        unlikely(node_ds(node) != sizeof(MDBX_db))) {
      ERROR("invalid nested-db record size %zu", node_ds(node));
      return MDBX_CORRUPTED;
    }
    memcpy(&mx->mx_db, node_data(node), sizeof(MDBX_db));
    const txnid_t pp_txnid = mp->mp_txnid;
    if (!MDBX_DISABLE_VALIDATION &&
        unlikely(mx->mx_db.md_mod_txnid > pp_txnid)) {
      ERROR("nested-db.md_mod_txnid (%" PRIaTXN ") > page-txnid (%" PRIaTXN ")",
            mx->mx_db.md_mod_txnid, pp_txnid);
      return MDBX_CORRUPTED;
    }
    mx->mx_cursor.mc_pg[0] = 0;
    mx->mx_cursor.mc_snum = 0;
    mx->mx_cursor.mc_top = 0;
    mx->mx_cursor.mc_flags = C_SUB;
    break;
  case F_DUPDATA:
    if (!MDBX_DISABLE_VALIDATION && unlikely(node_ds(node) <= PAGEHDRSZ)) {
      ERROR("invalid nested-page size %zu", node_ds(node));
      return MDBX_CORRUPTED;
    }
    MDBX_page *fp = node_data(node);
    mx->mx_db.md_depth = 1;
    mx->mx_db.md_branch_pages = 0;
    mx->mx_db.md_leaf_pages = 1;
    mx->mx_db.md_overflow_pages = 0;
    mx->mx_db.md_entries = page_numkeys(fp);
    mx->mx_db.md_root = fp->mp_pgno;
    mx->mx_db.md_mod_txnid = mp->mp_txnid;
    mx->mx_cursor.mc_snum = 1;
    mx->mx_cursor.mc_top = 0;
    mx->mx_cursor.mc_flags = C_SUB | C_INITIALIZED;
    mx->mx_cursor.mc_pg[0] = fp;
    mx->mx_cursor.mc_ki[0] = 0;
    mx->mx_db.md_flags = flags_db2sub(mc->mc_db->md_flags);
    mx->mx_db.md_xsize =
        (mc->mc_db->md_flags & MDBX_DUPFIXED) ? fp->mp_leaf2_ksize : 0;
    break;
  }

  if (unlikely(mx->mx_db.md_xsize != mc->mc_db->md_xsize)) {
    if (!MDBX_DISABLE_VALIDATION && unlikely(mc->mc_db->md_xsize != 0)) {
      ERROR("cursor mismatched nested-db md_xsize %u", mc->mc_db->md_xsize);
      return MDBX_CORRUPTED;
    }
    if (!MDBX_DISABLE_VALIDATION &&
        unlikely((mc->mc_db->md_flags & MDBX_DUPFIXED) == 0)) {
      ERROR("mismatched nested-db md_flags %u", mc->mc_db->md_flags);
      return MDBX_CORRUPTED;
    }
    if (!MDBX_DISABLE_VALIDATION &&
        unlikely(mx->mx_db.md_xsize < mc->mc_dbx->md_vlen_min ||
                 mx->mx_db.md_xsize > mc->mc_dbx->md_vlen_max)) {
      ERROR("mismatched nested-db.md_xsize (%u) <> min/max value-length "
            "(%zu/%zu)",
            mx->mx_db.md_xsize, mc->mc_dbx->md_vlen_min,
            mc->mc_dbx->md_vlen_max);
      return MDBX_CORRUPTED;
    }
    mc->mc_db->md_xsize = mx->mx_db.md_xsize;
    mc->mc_dbx->md_vlen_min = mc->mc_dbx->md_vlen_max = mx->mx_db.md_xsize;
  }
  mx->mx_dbx.md_klen_min = mc->mc_dbx->md_vlen_min;
  mx->mx_dbx.md_klen_max = mc->mc_dbx->md_vlen_max;

  DEBUG("Sub-db -%u root page %" PRIaPGNO, mx->mx_cursor.mc_dbi,
        mx->mx_db.md_root);
  return MDBX_SUCCESS;
}

/* Fixup a sorted-dups cursor due to underlying update.
 * Sets up some fields that depend on the data from the main cursor.
 * Almost the same as init1, but skips initialization steps if the
 * xcursor had already been used.
 * [in] mc The main cursor whose sorted-dups cursor is to be fixed up.
 * [in] src_mx The xcursor of an up-to-date cursor.
 * [in] new_dupdata True if converting from a non-F_DUPDATA item. */
static int cursor_xinit2(MDBX_cursor *mc, MDBX_xcursor *src_mx,
                         bool new_dupdata) {
  MDBX_xcursor *mx = mc->mc_xcursor;
  if (!MDBX_DISABLE_VALIDATION && unlikely(mx == nullptr)) {
    ERROR("unexpected dupsort-page for non-dupsort db/cursor (dbi %u)",
          mc->mc_dbi);
    return MDBX_CORRUPTED;
  }

  if (new_dupdata) {
    mx->mx_cursor.mc_snum = 1;
    mx->mx_cursor.mc_top = 0;
    mx->mx_cursor.mc_flags = C_SUB | C_INITIALIZED;
    mx->mx_cursor.mc_ki[0] = 0;
  }

  mx->mx_dbx.md_klen_min = src_mx->mx_dbx.md_klen_min;
  mx->mx_dbx.md_klen_max = src_mx->mx_dbx.md_klen_max;
  mx->mx_dbx.md_cmp = src_mx->mx_dbx.md_cmp;
  mx->mx_db = src_mx->mx_db;
  mx->mx_cursor.mc_pg[0] = src_mx->mx_cursor.mc_pg[0];
  if (mx->mx_cursor.mc_flags & C_INITIALIZED) {
    DEBUG("Sub-db -%u root page %" PRIaPGNO, mx->mx_cursor.mc_dbi,
          mx->mx_db.md_root);
  }
  return MDBX_SUCCESS;
}

static __inline int couple_init(MDBX_cursor_couple *couple, const size_t dbi,
                                const MDBX_txn *const txn, MDBX_db *const db,
                                MDBX_dbx *const dbx, uint8_t *const dbstate) {
  couple->outer.mc_signature = MDBX_MC_LIVE;
  couple->outer.mc_next = NULL;
  couple->outer.mc_backup = NULL;
  couple->outer.mc_dbi = (MDBX_dbi)dbi;
  couple->outer.mc_txn = (MDBX_txn *)txn;
  couple->outer.mc_db = db;
  couple->outer.mc_dbx = dbx;
  couple->outer.mc_dbistate = dbstate;
  couple->outer.mc_snum = 0;
  couple->outer.mc_top = 0;
  couple->outer.mc_pg[0] = 0;
  couple->outer.mc_flags = 0;
  STATIC_ASSERT(CC_BRANCH == P_BRANCH && CC_LEAF == P_LEAF &&
                CC_OVERFLOW == P_OVERFLOW && CC_LEAF2 == P_LEAF2);
  couple->outer.mc_checking =
      (AUDIT_ENABLED() || (txn->mt_env->me_flags & MDBX_VALIDATION))
          ? CC_PAGECHECK | CC_LEAF
          : CC_LEAF;
  couple->outer.mc_ki[0] = 0;
  couple->outer.mc_xcursor = NULL;

  int rc = MDBX_SUCCESS;
  if (unlikely(*couple->outer.mc_dbistate & DBI_STALE)) {
    rc = page_search(&couple->outer, NULL, MDBX_PS_ROOTONLY);
    rc = (rc != MDBX_NOTFOUND) ? rc : MDBX_SUCCESS;
  } else if (unlikely(dbx->md_klen_max == 0)) {
    rc = setup_dbx(dbx, db, txn->mt_env->me_psize);
  }

  if (couple->outer.mc_db->md_flags & MDBX_DUPSORT) {
    couple->inner.mx_cursor.mc_signature = MDBX_MC_LIVE;
    couple->outer.mc_xcursor = &couple->inner;
    rc = cursor_xinit0(&couple->outer);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    couple->inner.mx_dbx.md_klen_min = couple->outer.mc_dbx->md_vlen_min;
    couple->inner.mx_dbx.md_klen_max = couple->outer.mc_dbx->md_vlen_max;
  }
  return rc;
}

/* Initialize a cursor for a given transaction and database. */
static int cursor_init(MDBX_cursor *mc, const MDBX_txn *txn, size_t dbi) {
  STATIC_ASSERT(offsetof(MDBX_cursor_couple, outer) == 0);
  return couple_init(container_of(mc, MDBX_cursor_couple, outer), dbi, txn,
                     &txn->mt_dbs[dbi], &txn->mt_dbxs[dbi],
                     &txn->mt_dbistate[dbi]);
}

MDBX_cursor *mdbx_cursor_create(void *context) {
  MDBX_cursor_couple *couple = osal_calloc(1, sizeof(MDBX_cursor_couple));
  if (unlikely(!couple))
    return nullptr;

  couple->outer.mc_signature = MDBX_MC_READY4CLOSE;
  couple->outer.mc_dbi = UINT_MAX;
  couple->mc_userctx = context;
  return &couple->outer;
}

int mdbx_cursor_set_userctx(MDBX_cursor *mc, void *ctx) {
  if (unlikely(!mc))
    return MDBX_EINVAL;

  if (unlikely(mc->mc_signature != MDBX_MC_READY4CLOSE &&
               mc->mc_signature != MDBX_MC_LIVE))
    return MDBX_EBADSIGN;

  MDBX_cursor_couple *couple = container_of(mc, MDBX_cursor_couple, outer);
  couple->mc_userctx = ctx;
  return MDBX_SUCCESS;
}

void *mdbx_cursor_get_userctx(const MDBX_cursor *mc) {
  if (unlikely(!mc))
    return nullptr;

  if (unlikely(mc->mc_signature != MDBX_MC_READY4CLOSE &&
               mc->mc_signature != MDBX_MC_LIVE))
    return nullptr;

  MDBX_cursor_couple *couple = container_of(mc, MDBX_cursor_couple, outer);
  return couple->mc_userctx;
}

int mdbx_cursor_bind(const MDBX_txn *txn, MDBX_cursor *mc, MDBX_dbi dbi) {
  if (unlikely(!mc))
    return MDBX_EINVAL;

  if (unlikely(mc->mc_signature != MDBX_MC_READY4CLOSE &&
               mc->mc_signature != MDBX_MC_LIVE))
    return MDBX_EBADSIGN;

  int rc = check_txn(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!check_dbi(txn, dbi, DBI_VALID)))
    return MDBX_BAD_DBI;

  if (unlikely(dbi == FREE_DBI && !(txn->mt_flags & MDBX_TXN_RDONLY)))
    return MDBX_EACCESS;

  if (unlikely(mc->mc_backup)) /* Cursor from parent transaction */ {
    cASSERT(mc, mc->mc_signature == MDBX_MC_LIVE);
    if (unlikely(mc->mc_dbi != dbi ||
                 /* paranoia */ mc->mc_signature != MDBX_MC_LIVE ||
                 mc->mc_txn != txn))
      return MDBX_EINVAL;

    assert(mc->mc_db == &txn->mt_dbs[dbi]);
    assert(mc->mc_dbx == &txn->mt_dbxs[dbi]);
    assert(mc->mc_dbi == dbi);
    assert(mc->mc_dbistate == &txn->mt_dbistate[dbi]);
    return likely(mc->mc_dbi == dbi &&
                  /* paranoia */ mc->mc_signature == MDBX_MC_LIVE &&
                  mc->mc_txn == txn)
               ? MDBX_SUCCESS
               : MDBX_EINVAL /* Disallow change DBI in nested transactions */;
  }

  if (mc->mc_signature == MDBX_MC_LIVE) {
    if (unlikely(!mc->mc_txn ||
                 mc->mc_txn->mt_signature != MDBX_MT_SIGNATURE)) {
      ERROR("Wrong cursor's transaction %p 0x%x",
            __Wpedantic_format_voidptr(mc->mc_txn),
            mc->mc_txn ? mc->mc_txn->mt_signature : 0);
      return MDBX_PROBLEM;
    }
    if (mc->mc_flags & C_UNTRACK) {
      MDBX_cursor **prev = &mc->mc_txn->mt_cursors[mc->mc_dbi];
      while (*prev && *prev != mc)
        prev = &(*prev)->mc_next;
      cASSERT(mc, *prev == mc);
      *prev = mc->mc_next;
    }
    mc->mc_signature = MDBX_MC_READY4CLOSE;
    mc->mc_flags = 0;
    mc->mc_dbi = UINT_MAX;
    mc->mc_next = NULL;
    mc->mc_db = NULL;
    mc->mc_dbx = NULL;
    mc->mc_dbistate = NULL;
  }
  cASSERT(mc, !(mc->mc_flags & C_UNTRACK));

  rc = cursor_init(mc, txn, dbi);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  mc->mc_next = txn->mt_cursors[dbi];
  txn->mt_cursors[dbi] = mc;
  mc->mc_flags |= C_UNTRACK;

  return MDBX_SUCCESS;
}

int mdbx_cursor_open(const MDBX_txn *txn, MDBX_dbi dbi, MDBX_cursor **ret) {
  if (unlikely(!ret))
    return MDBX_EINVAL;
  *ret = NULL;

  MDBX_cursor *const mc = mdbx_cursor_create(nullptr);
  if (unlikely(!mc))
    return MDBX_ENOMEM;

  int rc = mdbx_cursor_bind(txn, mc, dbi);
  if (unlikely(rc != MDBX_SUCCESS)) {
    mdbx_cursor_close(mc);
    return rc;
  }

  *ret = mc;
  return MDBX_SUCCESS;
}

int mdbx_cursor_renew(const MDBX_txn *txn, MDBX_cursor *mc) {
  return likely(mc) ? mdbx_cursor_bind(txn, mc, mc->mc_dbi) : MDBX_EINVAL;
}

int mdbx_cursor_copy(const MDBX_cursor *src, MDBX_cursor *dest) {
  if (unlikely(!src))
    return MDBX_EINVAL;
  if (unlikely(src->mc_signature != MDBX_MC_LIVE))
    return (src->mc_signature == MDBX_MC_READY4CLOSE) ? MDBX_EINVAL
                                                      : MDBX_EBADSIGN;

  int rc = mdbx_cursor_bind(src->mc_txn, dest, src->mc_dbi);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  assert(dest->mc_db == src->mc_db);
  assert(dest->mc_dbi == src->mc_dbi);
  assert(dest->mc_dbx == src->mc_dbx);
  assert(dest->mc_dbistate == src->mc_dbistate);
again:
  assert(dest->mc_txn == src->mc_txn);
  dest->mc_flags ^= (dest->mc_flags ^ src->mc_flags) & ~C_UNTRACK;
  dest->mc_top = src->mc_top;
  dest->mc_snum = src->mc_snum;
  for (size_t i = 0; i < src->mc_snum; ++i) {
    dest->mc_ki[i] = src->mc_ki[i];
    dest->mc_pg[i] = src->mc_pg[i];
  }

  if (src->mc_xcursor) {
    dest->mc_xcursor->mx_db = src->mc_xcursor->mx_db;
    dest->mc_xcursor->mx_dbx = src->mc_xcursor->mx_dbx;
    src = &src->mc_xcursor->mx_cursor;
    dest = &dest->mc_xcursor->mx_cursor;
    goto again;
  }

  return MDBX_SUCCESS;
}

void mdbx_cursor_close(MDBX_cursor *mc) {
  if (likely(mc)) {
    ENSURE(NULL, mc->mc_signature == MDBX_MC_LIVE ||
                     mc->mc_signature == MDBX_MC_READY4CLOSE);
    MDBX_txn *const txn = mc->mc_txn;
    if (!mc->mc_backup) {
      mc->mc_txn = NULL;
      /* Unlink from txn, if tracked. */
      if (mc->mc_flags & C_UNTRACK) {
        ENSURE(txn->mt_env, check_txn(txn, 0) == MDBX_SUCCESS);
        MDBX_cursor **prev = &txn->mt_cursors[mc->mc_dbi];
        while (*prev && *prev != mc)
          prev = &(*prev)->mc_next;
        tASSERT(txn, *prev == mc);
        *prev = mc->mc_next;
      }
      mc->mc_signature = 0;
      mc->mc_next = mc;
      osal_free(mc);
    } else {
      /* Cursor closed before nested txn ends */
      tASSERT(txn, mc->mc_signature == MDBX_MC_LIVE);
      ENSURE(txn->mt_env, check_txn_rw(txn, 0) == MDBX_SUCCESS);
      mc->mc_signature = MDBX_MC_WAIT4EOT;
    }
  }
}

MDBX_txn *mdbx_cursor_txn(const MDBX_cursor *mc) {
  if (unlikely(!mc || mc->mc_signature != MDBX_MC_LIVE))
    return NULL;
  MDBX_txn *txn = mc->mc_txn;
  if (unlikely(!txn || txn->mt_signature != MDBX_MT_SIGNATURE))
    return NULL;
  if (unlikely(txn->mt_flags & MDBX_TXN_FINISHED))
    return NULL;
  return txn;
}

MDBX_dbi mdbx_cursor_dbi(const MDBX_cursor *mc) {
  if (unlikely(!mc || mc->mc_signature != MDBX_MC_LIVE))
    return UINT_MAX;
  return mc->mc_dbi;
}

/* Return the count of duplicate data items for the current key */
int mdbx_cursor_count(const MDBX_cursor *mc, size_t *countp) {
  if (unlikely(mc == NULL))
    return MDBX_EINVAL;

  if (unlikely(mc->mc_signature != MDBX_MC_LIVE))
    return (mc->mc_signature == MDBX_MC_READY4CLOSE) ? MDBX_EINVAL
                                                     : MDBX_EBADSIGN;

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
    if (node_flags(node) & F_DUPDATA) {
      cASSERT(mc, mc->mc_xcursor &&
                      (mc->mc_xcursor->mx_cursor.mc_flags & C_INITIALIZED));
      *countp = unlikely(mc->mc_xcursor->mx_db.md_entries > PTRDIFF_MAX)
                    ? PTRDIFF_MAX
                    : (size_t)mc->mc_xcursor->mx_db.md_entries;
    }
  }
  return MDBX_SUCCESS;
}

/* Replace the key for a branch node with a new key.
 * Set MDBX_TXN_ERROR on failure.
 * [in] mc Cursor pointing to the node to operate on.
 * [in] key The new key to use.
 * Returns 0 on success, non-zero on failure. */
static int update_key(MDBX_cursor *mc, const MDBX_val *key) {
  MDBX_page *mp;
  MDBX_node *node;
  size_t len;
  ptrdiff_t delta, ksize, oksize;
  intptr_t ptr, i, nkeys, indx;
  DKBUF_DEBUG;

  cASSERT(mc, cursor_is_tracked(mc));
  indx = mc->mc_ki[mc->mc_top];
  mp = mc->mc_pg[mc->mc_top];
  node = page_node(mp, indx);
  ptr = mp->mp_ptrs[indx];
#if MDBX_DEBUG
  MDBX_val k2;
  k2.iov_base = node_key(node);
  k2.iov_len = node_ks(node);
  DEBUG("update key %zi (offset %zu) [%s] to [%s] on page %" PRIaPGNO, indx,
        ptr, DVAL_DEBUG(&k2), DKEY_DEBUG(key), mp->mp_pgno);
#endif /* MDBX_DEBUG */

  /* Sizes must be 2-byte aligned. */
  ksize = EVEN(key->iov_len);
  oksize = EVEN(node_ks(node));
  delta = ksize - oksize;

  /* Shift node contents if EVEN(key length) changed. */
  if (delta) {
    if (delta > (int)page_room(mp)) {
      /* not enough space left, do a delete and split */
      DEBUG("Not enough room, delta = %zd, splitting...", delta);
      pgno_t pgno = node_pgno(node);
      node_del(mc, 0);
      int err = page_split(mc, key, NULL, pgno, MDBX_SPLIT_REPLACE);
      if (err == MDBX_SUCCESS && AUDIT_ENABLED())
        err = cursor_check_updating(mc);
      return err;
    }

    nkeys = page_numkeys(mp);
    for (i = 0; i < nkeys; i++) {
      if (mp->mp_ptrs[i] <= ptr) {
        cASSERT(mc, mp->mp_ptrs[i] >= delta);
        mp->mp_ptrs[i] -= (indx_t)delta;
      }
    }

    void *const base = ptr_disp(mp, mp->mp_upper + PAGEHDRSZ);
    len = ptr - mp->mp_upper + NODESIZE;
    memmove(ptr_disp(base, -delta), base, len);
    cASSERT(mc, mp->mp_upper >= delta);
    mp->mp_upper -= (indx_t)delta;

    node = page_node(mp, indx);
  }

  /* But even if no shift was needed, update ksize */
  node_set_ks(node, key->iov_len);

  if (likely(key->iov_len /* to avoid UBSAN traps*/ != 0))
    memcpy(node_key(node), key->iov_base, key->iov_len);
  return MDBX_SUCCESS;
}

/* Move a node from csrc to cdst. */
static int node_move(MDBX_cursor *csrc, MDBX_cursor *cdst, bool fromleft) {
  int rc;
  DKBUF_DEBUG;

  MDBX_page *psrc = csrc->mc_pg[csrc->mc_top];
  MDBX_page *pdst = cdst->mc_pg[cdst->mc_top];
  cASSERT(csrc, PAGETYPE_WHOLE(psrc) == PAGETYPE_WHOLE(pdst));
  cASSERT(csrc, csrc->mc_dbi == cdst->mc_dbi);
  cASSERT(csrc, csrc->mc_top == cdst->mc_top);
  if (unlikely(PAGETYPE_WHOLE(psrc) != PAGETYPE_WHOLE(pdst))) {
  bailout:
    ERROR("Wrong or mismatch pages's types (src %d, dst %d) to move node",
          PAGETYPE_WHOLE(psrc), PAGETYPE_WHOLE(pdst));
    csrc->mc_txn->mt_flags |= MDBX_TXN_ERROR;
    return MDBX_PROBLEM;
  }

  MDBX_val key4move;
  switch (PAGETYPE_WHOLE(psrc)) {
  case P_BRANCH: {
    const MDBX_node *srcnode = page_node(psrc, csrc->mc_ki[csrc->mc_top]);
    cASSERT(csrc, node_flags(srcnode) == 0);
    const pgno_t srcpg = node_pgno(srcnode);
    key4move.iov_len = node_ks(srcnode);
    key4move.iov_base = node_key(srcnode);

    if (csrc->mc_ki[csrc->mc_top] == 0) {
      const size_t snum = csrc->mc_snum;
      cASSERT(csrc, snum > 0);
      /* must find the lowest key below src */
      rc = page_search_lowest(csrc);
      MDBX_page *lowest_page = csrc->mc_pg[csrc->mc_top];
      if (unlikely(rc))
        return rc;
      cASSERT(csrc, IS_LEAF(lowest_page));
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
      csrc->mc_snum = (uint8_t)snum;
      csrc->mc_top = (uint8_t)snum - 1;
      csrc->mc_ki[csrc->mc_top] = 0;

      /* paranoia */
      cASSERT(csrc, psrc == csrc->mc_pg[csrc->mc_top]);
      cASSERT(csrc, IS_BRANCH(psrc));
      if (unlikely(!IS_BRANCH(psrc)))
        goto bailout;
    }

    if (cdst->mc_ki[cdst->mc_top] == 0) {
      const size_t snum = cdst->mc_snum;
      cASSERT(csrc, snum > 0);
      MDBX_cursor mn;
      cursor_copy(cdst, &mn);
      /* must find the lowest key below dst */
      rc = page_search_lowest(&mn);
      if (unlikely(rc))
        return rc;
      MDBX_page *const lowest_page = mn.mc_pg[mn.mc_top];
      cASSERT(cdst, IS_LEAF(lowest_page));
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
      mn.mc_snum = (uint8_t)snum;
      mn.mc_top = (uint8_t)snum - 1;
      mn.mc_ki[mn.mc_top] = 0;

      const intptr_t delta =
          EVEN(key.iov_len) - EVEN(node_ks(page_node(mn.mc_pg[mn.mc_top], 0)));
      const intptr_t needed =
          branch_size(cdst->mc_txn->mt_env, &key4move) + delta;
      const intptr_t have = page_room(pdst);
      if (unlikely(needed > have))
        return MDBX_RESULT_TRUE;

      if (unlikely((rc = page_touch(csrc)) || (rc = page_touch(cdst))))
        return rc;
      psrc = csrc->mc_pg[csrc->mc_top];
      pdst = cdst->mc_pg[cdst->mc_top];

      WITH_CURSOR_TRACKING(mn, rc = update_key(&mn, &key));
      if (unlikely(rc))
        return rc;
    } else {
      const size_t needed = branch_size(cdst->mc_txn->mt_env, &key4move);
      const size_t have = page_room(pdst);
      if (unlikely(needed > have))
        return MDBX_RESULT_TRUE;

      if (unlikely((rc = page_touch(csrc)) || (rc = page_touch(cdst))))
        return rc;
      psrc = csrc->mc_pg[csrc->mc_top];
      pdst = cdst->mc_pg[cdst->mc_top];
    }

    DEBUG("moving %s-node %u [%s] on page %" PRIaPGNO
          " to node %u on page %" PRIaPGNO,
          "branch", csrc->mc_ki[csrc->mc_top], DKEY_DEBUG(&key4move),
          psrc->mp_pgno, cdst->mc_ki[cdst->mc_top], pdst->mp_pgno);
    /* Add the node to the destination page. */
    rc = node_add_branch(cdst, cdst->mc_ki[cdst->mc_top], &key4move, srcpg);
  } break;

  case P_LEAF: {
    /* Mark src and dst as dirty. */
    if (unlikely((rc = page_touch(csrc)) || (rc = page_touch(cdst))))
      return rc;
    psrc = csrc->mc_pg[csrc->mc_top];
    pdst = cdst->mc_pg[cdst->mc_top];
    const MDBX_node *srcnode = page_node(psrc, csrc->mc_ki[csrc->mc_top]);
    MDBX_val data;
    data.iov_len = node_ds(srcnode);
    data.iov_base = node_data(srcnode);
    key4move.iov_len = node_ks(srcnode);
    key4move.iov_base = node_key(srcnode);
    DEBUG("moving %s-node %u [%s] on page %" PRIaPGNO
          " to node %u on page %" PRIaPGNO,
          "leaf", csrc->mc_ki[csrc->mc_top], DKEY_DEBUG(&key4move),
          psrc->mp_pgno, cdst->mc_ki[cdst->mc_top], pdst->mp_pgno);
    /* Add the node to the destination page. */
    rc = node_add_leaf(cdst, cdst->mc_ki[cdst->mc_top], &key4move, &data,
                       node_flags(srcnode));
  } break;

  case P_LEAF | P_LEAF2: {
    /* Mark src and dst as dirty. */
    if (unlikely((rc = page_touch(csrc)) || (rc = page_touch(cdst))))
      return rc;
    psrc = csrc->mc_pg[csrc->mc_top];
    pdst = cdst->mc_pg[cdst->mc_top];
    key4move.iov_len = csrc->mc_db->md_xsize;
    key4move.iov_base =
        page_leaf2key(psrc, csrc->mc_ki[csrc->mc_top], key4move.iov_len);
    DEBUG("moving %s-node %u [%s] on page %" PRIaPGNO
          " to node %u on page %" PRIaPGNO,
          "leaf2", csrc->mc_ki[csrc->mc_top], DKEY_DEBUG(&key4move),
          psrc->mp_pgno, cdst->mc_ki[cdst->mc_top], pdst->mp_pgno);
    /* Add the node to the destination page. */
    rc = node_add_leaf2(cdst, cdst->mc_ki[cdst->mc_top], &key4move);
  } break;

  default:
    assert(false);
    goto bailout;
  }

  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  /* Delete the node from the source page. */
  node_del(csrc, key4move.iov_len);

  cASSERT(csrc, psrc == csrc->mc_pg[csrc->mc_top]);
  cASSERT(cdst, pdst == cdst->mc_pg[cdst->mc_top]);
  cASSERT(csrc, PAGETYPE_WHOLE(psrc) == PAGETYPE_WHOLE(pdst));

  {
    /* Adjust other cursors pointing to mp */
    MDBX_cursor *m2, *m3;
    const MDBX_dbi dbi = csrc->mc_dbi;
    cASSERT(csrc, csrc->mc_top == cdst->mc_top);
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
          cASSERT(csrc, csrc->mc_top > 0);
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
            cASSERT(csrc, csrc->mc_top > 0);
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
    cASSERT(csrc, csrc->mc_top > 0);
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
      DEBUG("update separator for source page %" PRIaPGNO " to [%s]",
            psrc->mp_pgno, DKEY_DEBUG(&key));
      MDBX_cursor mn;
      cursor_copy(csrc, &mn);
      cASSERT(csrc, mn.mc_snum > 0);
      mn.mc_snum--;
      mn.mc_top--;
      /* We want rebalance to find mn when doing fixups */
      WITH_CURSOR_TRACKING(mn, rc = update_key(&mn, &key));
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
    }
    if (IS_BRANCH(psrc)) {
      const MDBX_val nullkey = {0, 0};
      const indx_t ix = csrc->mc_ki[csrc->mc_top];
      csrc->mc_ki[csrc->mc_top] = 0;
      rc = update_key(csrc, &nullkey);
      csrc->mc_ki[csrc->mc_top] = ix;
      cASSERT(csrc, rc == MDBX_SUCCESS);
    }
  }

  if (cdst->mc_ki[cdst->mc_top] == 0) {
    cASSERT(cdst, cdst->mc_top > 0);
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
      DEBUG("update separator for destination page %" PRIaPGNO " to [%s]",
            pdst->mp_pgno, DKEY_DEBUG(&key));
      MDBX_cursor mn;
      cursor_copy(cdst, &mn);
      cASSERT(cdst, mn.mc_snum > 0);
      mn.mc_snum--;
      mn.mc_top--;
      /* We want rebalance to find mn when doing fixups */
      WITH_CURSOR_TRACKING(mn, rc = update_key(&mn, &key));
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
    }
    if (IS_BRANCH(pdst)) {
      const MDBX_val nullkey = {0, 0};
      const indx_t ix = cdst->mc_ki[cdst->mc_top];
      cdst->mc_ki[cdst->mc_top] = 0;
      rc = update_key(cdst, &nullkey);
      cdst->mc_ki[cdst->mc_top] = ix;
      cASSERT(cdst, rc == MDBX_SUCCESS);
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
static int page_merge(MDBX_cursor *csrc, MDBX_cursor *cdst) {
  MDBX_val key;
  int rc;

  cASSERT(csrc, csrc != cdst);
  cASSERT(csrc, cursor_is_tracked(csrc));
  cASSERT(cdst, cursor_is_tracked(cdst));
  const MDBX_page *const psrc = csrc->mc_pg[csrc->mc_top];
  MDBX_page *pdst = cdst->mc_pg[cdst->mc_top];
  DEBUG("merging page %" PRIaPGNO " into %" PRIaPGNO, psrc->mp_pgno,
        pdst->mp_pgno);

  cASSERT(csrc, PAGETYPE_WHOLE(psrc) == PAGETYPE_WHOLE(pdst));
  cASSERT(csrc, csrc->mc_dbi == cdst->mc_dbi && csrc->mc_db == cdst->mc_db);
  cASSERT(csrc, csrc->mc_snum > 1); /* can't merge root page */
  cASSERT(cdst, cdst->mc_snum > 1);
  cASSERT(cdst, cdst->mc_snum < cdst->mc_db->md_depth ||
                    IS_LEAF(cdst->mc_pg[cdst->mc_db->md_depth - 1]));
  cASSERT(csrc, csrc->mc_snum < csrc->mc_db->md_depth ||
                    IS_LEAF(csrc->mc_pg[csrc->mc_db->md_depth - 1]));
  const int pagetype = PAGETYPE_WHOLE(psrc);

  /* Move all nodes from src to dst */
  const size_t dst_nkeys = page_numkeys(pdst);
  const size_t src_nkeys = page_numkeys(psrc);
  cASSERT(cdst, dst_nkeys + src_nkeys >= (IS_LEAF(psrc) ? 1u : 2u));
  if (likely(src_nkeys)) {
    size_t j = dst_nkeys;
    if (unlikely(pagetype & P_LEAF2)) {
      /* Mark dst as dirty. */
      rc = page_touch(cdst);
      cASSERT(cdst, rc != MDBX_RESULT_TRUE);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;

      key.iov_len = csrc->mc_db->md_xsize;
      key.iov_base = page_data(psrc);
      size_t i = 0;
      do {
        rc = node_add_leaf2(cdst, j++, &key);
        cASSERT(cdst, rc != MDBX_RESULT_TRUE);
        if (unlikely(rc != MDBX_SUCCESS))
          return rc;
        key.iov_base = ptr_disp(key.iov_base, key.iov_len);
      } while (++i != src_nkeys);
    } else {
      MDBX_node *srcnode = page_node(psrc, 0);
      key.iov_len = node_ks(srcnode);
      key.iov_base = node_key(srcnode);
      if (pagetype & P_BRANCH) {
        MDBX_cursor mn;
        cursor_copy(csrc, &mn);
        /* must find the lowest key below src */
        rc = page_search_lowest(&mn);
        cASSERT(csrc, rc != MDBX_RESULT_TRUE);
        if (unlikely(rc != MDBX_SUCCESS))
          return rc;

        const MDBX_page *mp = mn.mc_pg[mn.mc_top];
        if (likely(!IS_LEAF2(mp))) {
          cASSERT(&mn, IS_LEAF(mp));
          const MDBX_node *lowest = page_node(mp, 0);
          key.iov_len = node_ks(lowest);
          key.iov_base = node_key(lowest);
        } else {
          cASSERT(&mn, mn.mc_top > csrc->mc_top);
          key.iov_len = mp->mp_leaf2_ksize;
          key.iov_base = page_leaf2key(mp, mn.mc_ki[mn.mc_top], key.iov_len);
        }
        cASSERT(&mn, key.iov_len >= csrc->mc_dbx->md_klen_min);
        cASSERT(&mn, key.iov_len <= csrc->mc_dbx->md_klen_max);

        const size_t dst_room = page_room(pdst);
        const size_t src_used = page_used(cdst->mc_txn->mt_env, psrc);
        const size_t space_needed = src_used - node_ks(srcnode) + key.iov_len;
        if (unlikely(space_needed > dst_room))
          return MDBX_RESULT_TRUE;
      }

      /* Mark dst as dirty. */
      rc = page_touch(cdst);
      cASSERT(cdst, rc != MDBX_RESULT_TRUE);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;

      size_t i = 0;
      while (true) {
        if (pagetype & P_LEAF) {
          MDBX_val data;
          data.iov_len = node_ds(srcnode);
          data.iov_base = node_data(srcnode);
          rc = node_add_leaf(cdst, j++, &key, &data, node_flags(srcnode));
        } else {
          cASSERT(csrc, node_flags(srcnode) == 0);
          rc = node_add_branch(cdst, j++, &key, node_pgno(srcnode));
        }
        cASSERT(cdst, rc != MDBX_RESULT_TRUE);
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
    DEBUG("dst page %" PRIaPGNO " now has %zu keys (%.1f%% filled)",
          pdst->mp_pgno, page_numkeys(pdst),
          page_fill(cdst->mc_txn->mt_env, pdst));

    cASSERT(csrc, psrc == csrc->mc_pg[csrc->mc_top]);
    cASSERT(cdst, pdst == cdst->mc_pg[cdst->mc_top]);
  }

  /* Unlink the src page from parent and add to free list. */
  csrc->mc_top--;
  node_del(csrc, 0);
  if (csrc->mc_ki[csrc->mc_top] == 0) {
    const MDBX_val nullkey = {0, 0};
    rc = update_key(csrc, &nullkey);
    cASSERT(csrc, rc != MDBX_RESULT_TRUE);
    if (unlikely(rc != MDBX_SUCCESS)) {
      csrc->mc_top++;
      return rc;
    }
  }
  csrc->mc_top++;

  cASSERT(csrc, psrc == csrc->mc_pg[csrc->mc_top]);
  cASSERT(cdst, pdst == cdst->mc_pg[cdst->mc_top]);

  {
    /* Adjust other cursors pointing to mp */
    MDBX_cursor *m2, *m3;
    const MDBX_dbi dbi = csrc->mc_dbi;
    const size_t top = csrc->mc_top;

    for (m2 = csrc->mc_txn->mt_cursors[dbi]; m2; m2 = m2->mc_next) {
      m3 = (csrc->mc_flags & C_SUB) ? &m2->mc_xcursor->mx_cursor : m2;
      if (m3 == csrc || top >= m3->mc_snum)
        continue;
      if (m3->mc_pg[top] == psrc) {
        m3->mc_pg[top] = pdst;
        cASSERT(m3, dst_nkeys + m3->mc_ki[top] <= UINT16_MAX);
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

  rc = page_retire(csrc, (MDBX_page *)psrc);
  cASSERT(csrc, rc != MDBX_RESULT_TRUE);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  cASSERT(cdst, cdst->mc_db->md_entries > 0);
  cASSERT(cdst, cdst->mc_snum <= cdst->mc_db->md_depth);
  cASSERT(cdst, cdst->mc_top > 0);
  cASSERT(cdst, cdst->mc_snum == cdst->mc_top + 1);
  MDBX_page *const top_page = cdst->mc_pg[cdst->mc_top];
  const indx_t top_indx = cdst->mc_ki[cdst->mc_top];
  const unsigned save_snum = cdst->mc_snum;
  const uint16_t save_depth = cdst->mc_db->md_depth;
  cursor_pop(cdst);
  rc = rebalance(cdst);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  cASSERT(cdst, cdst->mc_db->md_entries > 0);
  cASSERT(cdst, cdst->mc_snum <= cdst->mc_db->md_depth);
  cASSERT(cdst, cdst->mc_snum == cdst->mc_top + 1);

#if MDBX_ENABLE_PGOP_STAT
  cdst->mc_txn->mt_env->me_lck->mti_pgop_stat.merge.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */

  if (IS_LEAF(cdst->mc_pg[cdst->mc_top])) {
    /* LY: don't touch cursor if top-page is a LEAF */
    cASSERT(cdst, IS_LEAF(cdst->mc_pg[cdst->mc_top]) ||
                      PAGETYPE_WHOLE(cdst->mc_pg[cdst->mc_top]) == pagetype);
    return MDBX_SUCCESS;
  }

  cASSERT(cdst, page_numkeys(top_page) == dst_nkeys + src_nkeys);

  if (unlikely(pagetype != PAGETYPE_WHOLE(top_page))) {
    /* LY: LEAF-page becomes BRANCH, unable restore cursor's stack */
    goto bailout;
  }

  if (top_page == cdst->mc_pg[cdst->mc_top]) {
    /* LY: don't touch cursor if prev top-page already on the top */
    cASSERT(cdst, cdst->mc_ki[cdst->mc_top] == top_indx);
    cASSERT(cdst, IS_LEAF(cdst->mc_pg[cdst->mc_top]) ||
                      PAGETYPE_WHOLE(cdst->mc_pg[cdst->mc_top]) == pagetype);
    return MDBX_SUCCESS;
  }

  const int new_snum = save_snum - save_depth + cdst->mc_db->md_depth;
  if (unlikely(new_snum < 1 || new_snum > cdst->mc_db->md_depth)) {
    /* LY: out of range, unable restore cursor's stack */
    goto bailout;
  }

  if (top_page == cdst->mc_pg[new_snum - 1]) {
    cASSERT(cdst, cdst->mc_ki[new_snum - 1] == top_indx);
    /* LY: restore cursor stack */
    cdst->mc_snum = (uint8_t)new_snum;
    cdst->mc_top = (uint8_t)new_snum - 1;
    cASSERT(cdst, cdst->mc_snum < cdst->mc_db->md_depth ||
                      IS_LEAF(cdst->mc_pg[cdst->mc_db->md_depth - 1]));
    cASSERT(cdst, IS_LEAF(cdst->mc_pg[cdst->mc_top]) ||
                      PAGETYPE_WHOLE(cdst->mc_pg[cdst->mc_top]) == pagetype);
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
    cdst->mc_snum = (uint8_t)new_snum;
    cdst->mc_top = (uint8_t)new_snum - 1;
    cASSERT(cdst, cdst->mc_snum < cdst->mc_db->md_depth ||
                      IS_LEAF(cdst->mc_pg[cdst->mc_db->md_depth - 1]));
    cASSERT(cdst, IS_LEAF(cdst->mc_pg[cdst->mc_top]) ||
                      PAGETYPE_WHOLE(cdst->mc_pg[cdst->mc_top]) == pagetype);
    return MDBX_SUCCESS;
  }

bailout:
  /* LY: unable restore cursor's stack */
  cdst->mc_flags &= ~C_INITIALIZED;
  return MDBX_CURSOR_FULL;
}

static void cursor_restore(const MDBX_cursor *csrc, MDBX_cursor *cdst) {
  cASSERT(cdst, cdst->mc_dbi == csrc->mc_dbi);
  cASSERT(cdst, cdst->mc_txn == csrc->mc_txn);
  cASSERT(cdst, cdst->mc_db == csrc->mc_db);
  cASSERT(cdst, cdst->mc_dbx == csrc->mc_dbx);
  cASSERT(cdst, cdst->mc_dbistate == csrc->mc_dbistate);
  cdst->mc_snum = csrc->mc_snum;
  cdst->mc_top = csrc->mc_top;
  cdst->mc_flags = csrc->mc_flags;
  cdst->mc_checking = csrc->mc_checking;

  for (size_t i = 0; i < csrc->mc_snum; i++) {
    cdst->mc_pg[i] = csrc->mc_pg[i];
    cdst->mc_ki[i] = csrc->mc_ki[i];
  }
}

/* Copy the contents of a cursor.
 * [in] csrc The cursor to copy from.
 * [out] cdst The cursor to copy to. */
static void cursor_copy(const MDBX_cursor *csrc, MDBX_cursor *cdst) {
  cASSERT(csrc, csrc->mc_txn->mt_txnid >=
                    csrc->mc_txn->mt_env->me_lck->mti_oldest_reader.weak);
  cdst->mc_dbi = csrc->mc_dbi;
  cdst->mc_next = NULL;
  cdst->mc_backup = NULL;
  cdst->mc_xcursor = NULL;
  cdst->mc_txn = csrc->mc_txn;
  cdst->mc_db = csrc->mc_db;
  cdst->mc_dbx = csrc->mc_dbx;
  cdst->mc_dbistate = csrc->mc_dbistate;
  cursor_restore(csrc, cdst);
}

/* Rebalance the tree after a delete operation.
 * [in] mc Cursor pointing to the page where rebalancing should begin.
 * Returns 0 on success, non-zero on failure. */
static int rebalance(MDBX_cursor *mc) {
  cASSERT(mc, cursor_is_tracked(mc));
  cASSERT(mc, mc->mc_snum > 0);
  cASSERT(mc, mc->mc_snum < mc->mc_db->md_depth ||
                  IS_LEAF(mc->mc_pg[mc->mc_db->md_depth - 1]));
  const int pagetype = PAGETYPE_WHOLE(mc->mc_pg[mc->mc_top]);

  STATIC_ASSERT(P_BRANCH == 1);
  const size_t minkeys = (pagetype & P_BRANCH) + (size_t)1;

  /* Pages emptier than this are candidates for merging. */
  size_t room_threshold = likely(mc->mc_dbi != FREE_DBI)
                              ? mc->mc_txn->mt_env->me_merge_threshold
                              : mc->mc_txn->mt_env->me_merge_threshold_gc;

  const MDBX_page *const tp = mc->mc_pg[mc->mc_top];
  const size_t numkeys = page_numkeys(tp);
  const size_t room = page_room(tp);
  DEBUG("rebalancing %s page %" PRIaPGNO
        " (has %zu keys, full %.1f%%, used %zu, room %zu bytes )",
        (pagetype & P_LEAF) ? "leaf" : "branch", tp->mp_pgno, numkeys,
        page_fill(mc->mc_txn->mt_env, tp), page_used(mc->mc_txn->mt_env, tp),
        room);
  cASSERT(mc, IS_MODIFIABLE(mc->mc_txn, tp));

  if (unlikely(numkeys < minkeys)) {
    DEBUG("page %" PRIaPGNO " must be merged due keys < %zu threshold",
          tp->mp_pgno, minkeys);
  } else if (unlikely(room > room_threshold)) {
    DEBUG("page %" PRIaPGNO " should be merged due room %zu > %zu threshold",
          tp->mp_pgno, room, room_threshold);
  } else {
    DEBUG("no need to rebalance page %" PRIaPGNO ", room %zu < %zu threshold",
          tp->mp_pgno, room, room_threshold);
    cASSERT(mc, mc->mc_db->md_entries > 0);
    return MDBX_SUCCESS;
  }

  int rc;
  if (mc->mc_snum < 2) {
    MDBX_page *const mp = mc->mc_pg[0];
    const size_t nkeys = page_numkeys(mp);
    cASSERT(mc, (mc->mc_db->md_entries == 0) == (nkeys == 0));
    if (IS_SUBP(mp)) {
      DEBUG("%s", "Can't rebalance a subpage, ignoring");
      cASSERT(mc, pagetype & P_LEAF);
      return MDBX_SUCCESS;
    }
    if (nkeys == 0) {
      cASSERT(mc, IS_LEAF(mp));
      DEBUG("%s", "tree is completely empty");
      cASSERT(mc, (*mc->mc_dbistate & DBI_DIRTY) != 0);
      mc->mc_db->md_root = P_INVALID;
      mc->mc_db->md_depth = 0;
      cASSERT(mc, mc->mc_db->md_branch_pages == 0 &&
                      mc->mc_db->md_overflow_pages == 0 &&
                      mc->mc_db->md_leaf_pages == 1);
      /* Adjust cursors pointing to mp */
      for (MDBX_cursor *m2 = mc->mc_txn->mt_cursors[mc->mc_dbi]; m2;
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
      return page_retire(mc, mp);
    }
    if (IS_BRANCH(mp) && nkeys == 1) {
      DEBUG("%s", "collapsing root page!");
      mc->mc_db->md_root = node_pgno(page_node(mp, 0));
      rc = page_get(mc, mc->mc_db->md_root, &mc->mc_pg[0], mp->mp_txnid);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
      mc->mc_db->md_depth--;
      mc->mc_ki[0] = mc->mc_ki[1];
      for (int i = 1; i < mc->mc_db->md_depth; i++) {
        mc->mc_pg[i] = mc->mc_pg[i + 1];
        mc->mc_ki[i] = mc->mc_ki[i + 1];
      }

      /* Adjust other cursors pointing to mp */
      for (MDBX_cursor *m2 = mc->mc_txn->mt_cursors[mc->mc_dbi]; m2;
           m2 = m2->mc_next) {
        MDBX_cursor *m3 =
            (mc->mc_flags & C_SUB) ? &m2->mc_xcursor->mx_cursor : m2;
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
      cASSERT(mc, IS_LEAF(mc->mc_pg[mc->mc_top]) ||
                      PAGETYPE_WHOLE(mc->mc_pg[mc->mc_top]) == pagetype);
      cASSERT(mc, mc->mc_snum < mc->mc_db->md_depth ||
                      IS_LEAF(mc->mc_pg[mc->mc_db->md_depth - 1]));
      return page_retire(mc, mp);
    }
    DEBUG("root page %" PRIaPGNO " doesn't need rebalancing (flags 0x%x)",
          mp->mp_pgno, mp->mp_flags);
    return MDBX_SUCCESS;
  }

  /* The parent (branch page) must have at least 2 pointers,
   * otherwise the tree is invalid. */
  const size_t pre_top = mc->mc_top - 1;
  cASSERT(mc, IS_BRANCH(mc->mc_pg[pre_top]));
  cASSERT(mc, !IS_SUBP(mc->mc_pg[0]));
  cASSERT(mc, page_numkeys(mc->mc_pg[pre_top]) > 1);

  /* Leaf page fill factor is below the threshold.
   * Try to move keys from left or right neighbor, or
   * merge with a neighbor page. */

  /* Find neighbors. */
  MDBX_cursor mn;
  cursor_copy(mc, &mn);

  MDBX_page *left = nullptr, *right = nullptr;
  if (mn.mc_ki[pre_top] > 0) {
    rc = page_get(
        &mn, node_pgno(page_node(mn.mc_pg[pre_top], mn.mc_ki[pre_top] - 1)),
        &left, mc->mc_pg[mc->mc_top]->mp_txnid);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    cASSERT(mc, PAGETYPE_WHOLE(left) == PAGETYPE_WHOLE(mc->mc_pg[mc->mc_top]));
  }
  if (mn.mc_ki[pre_top] + (size_t)1 < page_numkeys(mn.mc_pg[pre_top])) {
    rc = page_get(
        &mn,
        node_pgno(page_node(mn.mc_pg[pre_top], mn.mc_ki[pre_top] + (size_t)1)),
        &right, mc->mc_pg[mc->mc_top]->mp_txnid);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    cASSERT(mc, PAGETYPE_WHOLE(right) == PAGETYPE_WHOLE(mc->mc_pg[mc->mc_top]));
  }
  cASSERT(mc, left || right);

  const size_t ki_top = mc->mc_ki[mc->mc_top];
  const size_t ki_pre_top = mn.mc_ki[pre_top];
  const size_t nkeys = page_numkeys(mn.mc_pg[mn.mc_top]);

  const size_t left_room = left ? page_room(left) : 0;
  const size_t right_room = right ? page_room(right) : 0;
  const size_t left_nkeys = left ? page_numkeys(left) : 0;
  const size_t right_nkeys = right ? page_numkeys(right) : 0;
  bool involve = false;
retry:
  cASSERT(mc, mc->mc_snum > 1);
  if (left_room > room_threshold && left_room >= right_room &&
      (IS_MODIFIABLE(mc->mc_txn, left) || involve)) {
    /* try merge with left */
    cASSERT(mc, left_nkeys >= minkeys);
    mn.mc_pg[mn.mc_top] = left;
    mn.mc_ki[mn.mc_top - 1] = (indx_t)(ki_pre_top - 1);
    mn.mc_ki[mn.mc_top] = (indx_t)(left_nkeys - 1);
    mc->mc_ki[mc->mc_top] = 0;
    const size_t new_ki = ki_top + left_nkeys;
    mn.mc_ki[mn.mc_top] += mc->mc_ki[mn.mc_top] + 1;
    /* We want rebalance to find mn when doing fixups */
    WITH_CURSOR_TRACKING(mn, rc = page_merge(mc, &mn));
    if (likely(rc != MDBX_RESULT_TRUE)) {
      cursor_restore(&mn, mc);
      mc->mc_ki[mc->mc_top] = (indx_t)new_ki;
      cASSERT(mc, rc || page_numkeys(mc->mc_pg[mc->mc_top]) >= minkeys);
      return rc;
    }
  }
  if (right_room > room_threshold &&
      (IS_MODIFIABLE(mc->mc_txn, right) || involve)) {
    /* try merge with right */
    cASSERT(mc, right_nkeys >= minkeys);
    mn.mc_pg[mn.mc_top] = right;
    mn.mc_ki[mn.mc_top - 1] = (indx_t)(ki_pre_top + 1);
    mn.mc_ki[mn.mc_top] = 0;
    mc->mc_ki[mc->mc_top] = (indx_t)nkeys;
    WITH_CURSOR_TRACKING(mn, rc = page_merge(&mn, mc));
    if (likely(rc != MDBX_RESULT_TRUE)) {
      mc->mc_ki[mc->mc_top] = (indx_t)ki_top;
      cASSERT(mc, rc || page_numkeys(mc->mc_pg[mc->mc_top]) >= minkeys);
      return rc;
    }
  }

  if (left_nkeys > minkeys &&
      (right_nkeys <= left_nkeys || right_room >= left_room) &&
      (IS_MODIFIABLE(mc->mc_txn, left) || involve)) {
    /* try move from left */
    mn.mc_pg[mn.mc_top] = left;
    mn.mc_ki[mn.mc_top - 1] = (indx_t)(ki_pre_top - 1);
    mn.mc_ki[mn.mc_top] = (indx_t)(left_nkeys - 1);
    mc->mc_ki[mc->mc_top] = 0;
    WITH_CURSOR_TRACKING(mn, rc = node_move(&mn, mc, true));
    if (likely(rc != MDBX_RESULT_TRUE)) {
      mc->mc_ki[mc->mc_top] = (indx_t)(ki_top + 1);
      cASSERT(mc, rc || page_numkeys(mc->mc_pg[mc->mc_top]) >= minkeys);
      return rc;
    }
  }
  if (right_nkeys > minkeys && (IS_MODIFIABLE(mc->mc_txn, right) || involve)) {
    /* try move from right */
    mn.mc_pg[mn.mc_top] = right;
    mn.mc_ki[mn.mc_top - 1] = (indx_t)(ki_pre_top + 1);
    mn.mc_ki[mn.mc_top] = 0;
    mc->mc_ki[mc->mc_top] = (indx_t)nkeys;
    WITH_CURSOR_TRACKING(mn, rc = node_move(&mn, mc, false));
    if (likely(rc != MDBX_RESULT_TRUE)) {
      mc->mc_ki[mc->mc_top] = (indx_t)ki_top;
      cASSERT(mc, rc || page_numkeys(mc->mc_pg[mc->mc_top]) >= minkeys);
      return rc;
    }
  }

  if (nkeys >= minkeys) {
    mc->mc_ki[mc->mc_top] = (indx_t)ki_top;
    if (AUDIT_ENABLED())
      return cursor_check_updating(mc);
    return MDBX_SUCCESS;
  }

  /* Заглушено в ветке v0.12.x, будет работать в v0.13.1 и далее.
   *
   * if (mc->mc_txn->mt_env->me_options.prefer_waf_insteadof_balance &&
   *    likely(room_threshold > 0)) {
   *  room_threshold = 0;
   *  goto retry;
   * }
   */
  if (likely(!involve) &&
      (likely(mc->mc_dbi != FREE_DBI) || mc->mc_txn->tw.loose_pages ||
       MDBX_PNL_GETSIZE(mc->mc_txn->tw.relist) || (mc->mc_flags & C_GCU) ||
       (mc->mc_txn->mt_flags & MDBX_TXN_DRAINED_GC) || room_threshold)) {
    involve = true;
    goto retry;
  }
  if (likely(room_threshold > 0)) {
    room_threshold = 0;
    goto retry;
  }
  ERROR("Unable to merge/rebalance %s page %" PRIaPGNO
        " (has %zu keys, full %.1f%%, used %zu, room %zu bytes )",
        (pagetype & P_LEAF) ? "leaf" : "branch", tp->mp_pgno, numkeys,
        page_fill(mc->mc_txn->mt_env, tp), page_used(mc->mc_txn->mt_env, tp),
        room);
  return MDBX_PROBLEM;
}

__cold static int page_check(const MDBX_cursor *const mc,
                             const MDBX_page *const mp) {
  DKBUF;
  int rc = MDBX_SUCCESS;
  if (unlikely(mp->mp_pgno < MIN_PAGENO || mp->mp_pgno > MAX_PAGENO))
    rc = bad_page(mp, "invalid pgno (%u)\n", mp->mp_pgno);

  MDBX_env *const env = mc->mc_txn->mt_env;
  const ptrdiff_t offset = ptr_dist(mp, env->me_map);
  unsigned flags_mask = P_ILL_BITS;
  unsigned flags_expected = 0;
  if (offset < 0 ||
      offset > (ptrdiff_t)(pgno2bytes(env, mc->mc_txn->mt_next_pgno) -
                           ((mp->mp_flags & P_SUBP) ? PAGEHDRSZ + 1
                                                    : env->me_psize))) {
    /* should be dirty page without MDBX_WRITEMAP, or a subpage of. */
    flags_mask -= P_SUBP;
    if ((env->me_flags & MDBX_WRITEMAP) != 0 ||
        (!IS_SHADOWED(mc->mc_txn, mp) && !(mp->mp_flags & P_SUBP)))
      rc = bad_page(mp, "invalid page-address %p, offset %zi\n",
                    __Wpedantic_format_voidptr(mp), offset);
  } else if (offset & (env->me_psize - 1))
    flags_expected = P_SUBP;

  if (unlikely((mp->mp_flags & flags_mask) != flags_expected))
    rc = bad_page(mp, "unknown/extra page-flags (have 0x%x, expect 0x%x)\n",
                  mp->mp_flags & flags_mask, flags_expected);

  cASSERT(mc, (mc->mc_checking & CC_LEAF2) == 0 || (mc->mc_flags & C_SUB) != 0);
  const uint8_t type = PAGETYPE_WHOLE(mp);
  switch (type) {
  default:
    return bad_page(mp, "invalid type (%u)\n", type);
  case P_OVERFLOW:
    if (unlikely(mc->mc_flags & C_SUB))
      rc = bad_page(mp, "unexpected %s-page for %s (db-flags 0x%x)\n", "large",
                    "nested dupsort tree", mc->mc_db->md_flags);
    const pgno_t npages = mp->mp_pages;
    if (unlikely(npages < 1 || npages >= MAX_PAGENO / 2))
      rc = bad_page(mp, "invalid n-pages (%u) for large-page\n", npages);
    if (unlikely(mp->mp_pgno + npages > mc->mc_txn->mt_next_pgno))
      rc = bad_page(
          mp, "end of large-page beyond (%u) allocated space (%u next-pgno)\n",
          mp->mp_pgno + npages, mc->mc_txn->mt_next_pgno);
    return rc; //-------------------------- end of large/overflow page handling
  case P_LEAF | P_SUBP:
    if (unlikely(mc->mc_db->md_depth != 1))
      rc = bad_page(mp, "unexpected %s-page for %s (db-flags 0x%x)\n",
                    "leaf-sub", "nested dupsort db", mc->mc_db->md_flags);
    /* fall through */
    __fallthrough;
  case P_LEAF:
    if (unlikely((mc->mc_checking & CC_LEAF2) != 0))
      rc = bad_page(
          mp, "unexpected leaf-page for dupfixed subtree (db-lags 0x%x)\n",
          mc->mc_db->md_flags);
    break;
  case P_LEAF | P_LEAF2 | P_SUBP:
    if (unlikely(mc->mc_db->md_depth != 1))
      rc = bad_page(mp, "unexpected %s-page for %s (db-flags 0x%x)\n",
                    "leaf2-sub", "nested dupsort db", mc->mc_db->md_flags);
    /* fall through */
    __fallthrough;
  case P_LEAF | P_LEAF2:
    if (unlikely((mc->mc_checking & CC_LEAF2) == 0))
      rc = bad_page(
          mp,
          "unexpected leaf2-page for non-dupfixed (sub)tree (db-flags 0x%x)\n",
          mc->mc_db->md_flags);
    break;
  case P_BRANCH:
    break;
  }

  if (unlikely(mp->mp_upper < mp->mp_lower || (mp->mp_lower & 1) ||
               PAGEHDRSZ + mp->mp_upper > env->me_psize))
    rc = bad_page(mp, "invalid page lower(%u)/upper(%u) with limit %zu\n",
                  mp->mp_lower, mp->mp_upper, page_space(env));

  const char *const end_of_page = ptr_disp(mp, env->me_psize);
  const size_t nkeys = page_numkeys(mp);
  STATIC_ASSERT(P_BRANCH == 1);
  if (unlikely(nkeys <= (uint8_t)(mp->mp_flags & P_BRANCH))) {
    if ((!(mc->mc_flags & C_SUB) || mc->mc_db->md_entries) &&
        (!(mc->mc_checking & CC_UPDATING) ||
         !(IS_MODIFIABLE(mc->mc_txn, mp) || (mp->mp_flags & P_SUBP))))
      rc =
          bad_page(mp, "%s-page nkeys (%zu) < %u\n",
                   IS_BRANCH(mp) ? "branch" : "leaf", nkeys, 1 + IS_BRANCH(mp));
  }

  const size_t ksize_max = keysize_max(env->me_psize, 0);
  const size_t leaf2_ksize = mp->mp_leaf2_ksize;
  if (IS_LEAF2(mp)) {
    if (unlikely((mc->mc_flags & C_SUB) == 0 ||
                 (mc->mc_db->md_flags & MDBX_DUPFIXED) == 0))
      rc = bad_page(mp, "unexpected leaf2-page (db-flags 0x%x)\n",
                    mc->mc_db->md_flags);
    else if (unlikely(leaf2_ksize != mc->mc_db->md_xsize))
      rc = bad_page(mp, "invalid leaf2_ksize %zu\n", leaf2_ksize);
    else if (unlikely(((leaf2_ksize & nkeys) ^ mp->mp_upper) & 1))
      rc = bad_page(
          mp, "invalid page upper (%u) for nkeys %zu with leaf2-length %zu\n",
          mp->mp_upper, nkeys, leaf2_ksize);
  } else {
    if (unlikely((mp->mp_upper & 1) || PAGEHDRSZ + mp->mp_upper +
                                               nkeys * sizeof(MDBX_node) +
                                               nkeys - 1 >
                                           env->me_psize))
      rc =
          bad_page(mp, "invalid page upper (%u) for nkeys %zu with limit %zu\n",
                   mp->mp_upper, nkeys, page_space(env));
  }

  MDBX_val here, prev = {0, 0};
  for (size_t i = 0; i < nkeys; ++i) {
    if (IS_LEAF2(mp)) {
      const char *const key = page_leaf2key(mp, i, leaf2_ksize);
      if (unlikely(end_of_page < key + leaf2_ksize)) {
        rc = bad_page(mp, "leaf2-item beyond (%zu) page-end\n",
                      key + leaf2_ksize - end_of_page);
        continue;
      }

      if (unlikely(leaf2_ksize != mc->mc_dbx->md_klen_min)) {
        if (unlikely(leaf2_ksize < mc->mc_dbx->md_klen_min ||
                     leaf2_ksize > mc->mc_dbx->md_klen_max))
          rc = bad_page(
              mp, "leaf2-item size (%zu) <> min/max length (%zu/%zu)\n",
              leaf2_ksize, mc->mc_dbx->md_klen_min, mc->mc_dbx->md_klen_max);
        else
          mc->mc_dbx->md_klen_min = mc->mc_dbx->md_klen_max = leaf2_ksize;
      }
      if ((mc->mc_checking & CC_SKIPORD) == 0) {
        here.iov_base = (void *)key;
        here.iov_len = leaf2_ksize;
        if (prev.iov_base && unlikely(mc->mc_dbx->md_cmp(&prev, &here) >= 0))
          rc = bad_page(mp, "leaf2-item #%zu wrong order (%s >= %s)\n", i,
                        DKEY(&prev), DVAL(&here));
        prev = here;
      }
    } else {
      const MDBX_node *const node = page_node(mp, i);
      const char *const node_end = ptr_disp(node, NODESIZE);
      if (unlikely(node_end > end_of_page)) {
        rc = bad_page(mp, "node[%zu] (%zu) beyond page-end\n", i,
                      node_end - end_of_page);
        continue;
      }
      const size_t ksize = node_ks(node);
      if (unlikely(ksize > ksize_max))
        rc = bad_page(mp, "node[%zu] too long key (%zu)\n", i, ksize);
      const char *const key = node_key(node);
      if (unlikely(end_of_page < key + ksize)) {
        rc = bad_page(mp, "node[%zu] key (%zu) beyond page-end\n", i,
                      key + ksize - end_of_page);
        continue;
      }
      if ((IS_LEAF(mp) || i > 0)) {
        if (unlikely(ksize < mc->mc_dbx->md_klen_min ||
                     ksize > mc->mc_dbx->md_klen_max))
          rc = bad_page(
              mp, "node[%zu] key size (%zu) <> min/max key-length (%zu/%zu)\n",
              i, ksize, mc->mc_dbx->md_klen_min, mc->mc_dbx->md_klen_max);
        if ((mc->mc_checking & CC_SKIPORD) == 0) {
          here.iov_base = (void *)key;
          here.iov_len = ksize;
          if (prev.iov_base && unlikely(mc->mc_dbx->md_cmp(&prev, &here) >= 0))
            rc = bad_page(mp, "node[%zu] key wrong order (%s >= %s)\n", i,
                          DKEY(&prev), DVAL(&here));
          prev = here;
        }
      }
      if (IS_BRANCH(mp)) {
        if ((mc->mc_checking & CC_UPDATING) == 0 && i == 0 &&
            unlikely(ksize != 0))
          rc = bad_page(mp, "branch-node[%zu] wrong 0-node key-length (%zu)\n",
                        i, ksize);
        const pgno_t ref = node_pgno(node);
        if (unlikely(ref < MIN_PAGENO) ||
            (unlikely(ref >= mc->mc_txn->mt_next_pgno) &&
             (unlikely(ref >= mc->mc_txn->mt_geo.now) ||
              !(mc->mc_checking & CC_RETIRING))))
          rc = bad_page(mp, "branch-node[%zu] wrong pgno (%u)\n", i, ref);
        if (unlikely(node_flags(node)))
          rc = bad_page(mp, "branch-node[%zu] wrong flags (%u)\n", i,
                        node_flags(node));
        continue;
      }

      switch (node_flags(node)) {
      default:
        rc =
            bad_page(mp, "invalid node[%zu] flags (%u)\n", i, node_flags(node));
        break;
      case F_BIGDATA /* data on large-page */:
      case 0 /* usual */:
      case F_SUBDATA /* sub-db */:
      case F_SUBDATA | F_DUPDATA /* dupsorted sub-tree */:
      case F_DUPDATA /* short sub-page */:
        break;
      }

      const size_t dsize = node_ds(node);
      const char *const data = node_data(node);
      if (node_flags(node) & F_BIGDATA) {
        if (unlikely(end_of_page < data + sizeof(pgno_t))) {
          rc = bad_page(
              mp, "node-%s(%zu of %zu, %zu bytes) beyond (%zu) page-end\n",
              "bigdata-pgno", i, nkeys, dsize, data + dsize - end_of_page);
          continue;
        }
        if (unlikely(dsize <= mc->mc_dbx->md_vlen_min ||
                     dsize > mc->mc_dbx->md_vlen_max))
          rc = bad_page(
              mp,
              "big-node data size (%zu) <> min/max value-length (%zu/%zu)\n",
              dsize, mc->mc_dbx->md_vlen_min, mc->mc_dbx->md_vlen_max);
        if (unlikely(node_size_len(node_ks(node), dsize) <=
                     mc->mc_txn->mt_env->me_leaf_nodemax) &&
            mc->mc_dbi != FREE_DBI)
          poor_page(mp, "too small data (%zu bytes) for bigdata-node", dsize);

        if ((mc->mc_checking & CC_RETIRING) == 0) {
          const pgr_t lp =
              page_get_large(mc, node_largedata_pgno(node), mp->mp_txnid);
          if (unlikely(lp.err != MDBX_SUCCESS))
            return lp.err;
          cASSERT(mc, PAGETYPE_WHOLE(lp.page) == P_OVERFLOW);
          const unsigned npages = number_of_ovpages(env, dsize);
          if (unlikely(lp.page->mp_pages != npages)) {
            if (lp.page->mp_pages < npages)
              rc = bad_page(lp.page,
                            "too less n-pages %u for bigdata-node (%zu bytes)",
                            lp.page->mp_pages, dsize);
            else if (mc->mc_dbi != FREE_DBI)
              poor_page(lp.page,
                        "extra n-pages %u for bigdata-node (%zu bytes)",
                        lp.page->mp_pages, dsize);
          }
        }
        continue;
      }

      if (unlikely(end_of_page < data + dsize)) {
        rc = bad_page(mp,
                      "node-%s(%zu of %zu, %zu bytes) beyond (%zu) page-end\n",
                      "data", i, nkeys, dsize, data + dsize - end_of_page);
        continue;
      }

      switch (node_flags(node)) {
      default:
        /* wrong, but already handled */
        continue;
      case 0 /* usual */:
        if (unlikely(dsize < mc->mc_dbx->md_vlen_min ||
                     dsize > mc->mc_dbx->md_vlen_max)) {
          rc = bad_page(
              mp, "node-data size (%zu) <> min/max value-length (%zu/%zu)\n",
              dsize, mc->mc_dbx->md_vlen_min, mc->mc_dbx->md_vlen_max);
          continue;
        }
        break;
      case F_SUBDATA /* sub-db */:
        if (unlikely(dsize != sizeof(MDBX_db))) {
          rc = bad_page(mp, "invalid sub-db record size (%zu)\n", dsize);
          continue;
        }
        break;
      case F_SUBDATA | F_DUPDATA /* dupsorted sub-tree */:
        if (unlikely(dsize != sizeof(MDBX_db))) {
          rc = bad_page(mp, "invalid nested-db record size (%zu)\n", dsize);
          continue;
        }
        break;
      case F_DUPDATA /* short sub-page */:
        if (unlikely(dsize <= PAGEHDRSZ)) {
          rc = bad_page(mp, "invalid nested/sub-page record size (%zu)\n",
                        dsize);
          continue;
        } else {
          const MDBX_page *const sp = (MDBX_page *)data;
          switch (sp->mp_flags &
                  /* ignore legacy P_DIRTY flag */ ~P_LEGACY_DIRTY) {
          case P_LEAF | P_SUBP:
          case P_LEAF | P_LEAF2 | P_SUBP:
            break;
          default:
            rc = bad_page(mp, "invalid nested/sub-page flags (0x%02x)\n",
                          sp->mp_flags);
            continue;
          }

          const char *const end_of_subpage = data + dsize;
          const intptr_t nsubkeys = page_numkeys(sp);
          if (unlikely(nsubkeys == 0) && !(mc->mc_checking & CC_UPDATING) &&
              mc->mc_db->md_entries)
            rc = bad_page(mp, "no keys on a %s-page\n",
                          IS_LEAF2(sp) ? "leaf2-sub" : "leaf-sub");

          MDBX_val sub_here, sub_prev = {0, 0};
          for (int j = 0; j < nsubkeys; j++) {
            if (IS_LEAF2(sp)) {
              /* LEAF2 pages have no mp_ptrs[] or node headers */
              const size_t sub_ksize = sp->mp_leaf2_ksize;
              const char *const sub_key = page_leaf2key(sp, j, sub_ksize);
              if (unlikely(end_of_subpage < sub_key + sub_ksize)) {
                rc = bad_page(mp, "nested-leaf2-key beyond (%zu) nested-page\n",
                              sub_key + sub_ksize - end_of_subpage);
                continue;
              }

              if (unlikely(sub_ksize != mc->mc_dbx->md_vlen_min)) {
                if (unlikely(sub_ksize < mc->mc_dbx->md_vlen_min ||
                             sub_ksize > mc->mc_dbx->md_vlen_max))
                  rc = bad_page(mp,
                                "nested-leaf2-key size (%zu) <> min/max "
                                "value-length (%zu/%zu)\n",
                                sub_ksize, mc->mc_dbx->md_vlen_min,
                                mc->mc_dbx->md_vlen_max);
                else
                  mc->mc_dbx->md_vlen_min = mc->mc_dbx->md_vlen_max = sub_ksize;
              }
              if ((mc->mc_checking & CC_SKIPORD) == 0) {
                sub_here.iov_base = (void *)sub_key;
                sub_here.iov_len = sub_ksize;
                if (sub_prev.iov_base &&
                    unlikely(mc->mc_dbx->md_dcmp(&sub_prev, &sub_here) >= 0))
                  rc = bad_page(mp,
                                "nested-leaf2-key #%u wrong order (%s >= %s)\n",
                                j, DKEY(&sub_prev), DVAL(&sub_here));
                sub_prev = sub_here;
              }
            } else {
              const MDBX_node *const sub_node = page_node(sp, j);
              const char *const sub_node_end = ptr_disp(sub_node, NODESIZE);
              if (unlikely(sub_node_end > end_of_subpage)) {
                rc = bad_page(mp, "nested-node beyond (%zu) nested-page\n",
                              end_of_subpage - sub_node_end);
                continue;
              }
              if (unlikely(node_flags(sub_node) != 0))
                rc = bad_page(mp, "nested-node invalid flags (%u)\n",
                              node_flags(sub_node));

              const size_t sub_ksize = node_ks(sub_node);
              const char *const sub_key = node_key(sub_node);
              const size_t sub_dsize = node_ds(sub_node);
              /* char *sub_data = node_data(sub_node); */

              if (unlikely(sub_ksize < mc->mc_dbx->md_vlen_min ||
                           sub_ksize > mc->mc_dbx->md_vlen_max))
                rc = bad_page(mp,
                              "nested-node-key size (%zu) <> min/max "
                              "value-length (%zu/%zu)\n",
                              sub_ksize, mc->mc_dbx->md_vlen_min,
                              mc->mc_dbx->md_vlen_max);
              if ((mc->mc_checking & CC_SKIPORD) == 0) {
                sub_here.iov_base = (void *)sub_key;
                sub_here.iov_len = sub_ksize;
                if (sub_prev.iov_base &&
                    unlikely(mc->mc_dbx->md_dcmp(&sub_prev, &sub_here) >= 0))
                  rc = bad_page(mp,
                                "nested-node-key #%u wrong order (%s >= %s)\n",
                                j, DKEY(&sub_prev), DVAL(&sub_here));
                sub_prev = sub_here;
              }
              if (unlikely(sub_dsize != 0))
                rc = bad_page(mp, "nested-node non-empty data size (%zu)\n",
                              sub_dsize);
              if (unlikely(end_of_subpage < sub_key + sub_ksize))
                rc = bad_page(mp, "nested-node-key beyond (%zu) nested-page\n",
                              sub_key + sub_ksize - end_of_subpage);
            }
          }
        }
        break;
      }
    }
  }
  return rc;
}

__cold static int cursor_check(const MDBX_cursor *mc) {
  if (!mc->mc_txn->tw.dirtylist) {
    cASSERT(mc,
            (mc->mc_txn->mt_flags & MDBX_WRITEMAP) != 0 && !MDBX_AVOID_MSYNC);
  } else {
    cASSERT(mc,
            (mc->mc_txn->mt_flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);
    cASSERT(mc, mc->mc_txn->tw.dirtyroom + mc->mc_txn->tw.dirtylist->length ==
                    (mc->mc_txn->mt_parent
                         ? mc->mc_txn->mt_parent->tw.dirtyroom
                         : mc->mc_txn->mt_env->me_options.dp_limit));
  }
  cASSERT(mc, mc->mc_top == mc->mc_snum - 1 || (mc->mc_checking & CC_UPDATING));
  if (unlikely(mc->mc_top != mc->mc_snum - 1) &&
      (mc->mc_checking & CC_UPDATING) == 0)
    return MDBX_CURSOR_FULL;
  cASSERT(mc, (mc->mc_checking & CC_UPDATING)
                  ? mc->mc_snum <= mc->mc_db->md_depth
                  : mc->mc_snum == mc->mc_db->md_depth);
  if (unlikely((mc->mc_checking & CC_UPDATING)
                   ? mc->mc_snum > mc->mc_db->md_depth
                   : mc->mc_snum != mc->mc_db->md_depth))
    return MDBX_CURSOR_FULL;

  for (int n = 0; n < (int)mc->mc_snum; ++n) {
    MDBX_page *mp = mc->mc_pg[n];
    const size_t nkeys = page_numkeys(mp);
    const bool expect_branch = (n < mc->mc_db->md_depth - 1) ? true : false;
    const bool expect_nested_leaf =
        (n + 1 == mc->mc_db->md_depth - 1) ? true : false;
    const bool branch = IS_BRANCH(mp) ? true : false;
    cASSERT(mc, branch == expect_branch);
    if (unlikely(branch != expect_branch))
      return MDBX_CURSOR_FULL;
    if ((mc->mc_checking & CC_UPDATING) == 0) {
      cASSERT(mc, nkeys > mc->mc_ki[n] || (!branch && nkeys == mc->mc_ki[n] &&
                                           (mc->mc_flags & C_EOF) != 0));
      if (unlikely(nkeys <= mc->mc_ki[n] &&
                   !(!branch && nkeys == mc->mc_ki[n] &&
                     (mc->mc_flags & C_EOF) != 0)))
        return MDBX_CURSOR_FULL;
    } else {
      cASSERT(mc, nkeys + 1 >= mc->mc_ki[n]);
      if (unlikely(nkeys + 1 < mc->mc_ki[n]))
        return MDBX_CURSOR_FULL;
    }

    int err = page_check(mc, mp);
    if (unlikely(err != MDBX_SUCCESS))
      return err;

    for (size_t i = 0; i < nkeys; ++i) {
      if (branch) {
        MDBX_node *node = page_node(mp, i);
        cASSERT(mc, node_flags(node) == 0);
        if (unlikely(node_flags(node) != 0))
          return MDBX_CURSOR_FULL;
        pgno_t pgno = node_pgno(node);
        MDBX_page *np;
        err = page_get(mc, pgno, &np, mp->mp_txnid);
        cASSERT(mc, err == MDBX_SUCCESS);
        if (unlikely(err != MDBX_SUCCESS))
          return err;
        const bool nested_leaf = IS_LEAF(np) ? true : false;
        cASSERT(mc, nested_leaf == expect_nested_leaf);
        if (unlikely(nested_leaf != expect_nested_leaf))
          return MDBX_CURSOR_FULL;
        err = page_check(mc, np);
        if (unlikely(err != MDBX_SUCCESS))
          return err;
      }
    }
  }
  return MDBX_SUCCESS;
}

__cold static int cursor_check_updating(MDBX_cursor *mc) {
  const uint8_t checking = mc->mc_checking;
  mc->mc_checking |= CC_UPDATING;
  const int rc = cursor_check(mc);
  mc->mc_checking = checking;
  return rc;
}

int mdbx_del(MDBX_txn *txn, MDBX_dbi dbi, const MDBX_val *key,
             const MDBX_val *data) {
  int rc = check_txn_rw(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!key))
    return MDBX_EINVAL;

  if (unlikely(!check_dbi(txn, dbi, DBI_USRVALID)))
    return MDBX_BAD_DBI;

  if (unlikely(txn->mt_flags & (MDBX_TXN_RDONLY | MDBX_TXN_BLOCKED)))
    return (txn->mt_flags & MDBX_TXN_RDONLY) ? MDBX_EACCESS : MDBX_BAD_TXN;

  return delete (txn, dbi, key, data, 0);
}

static int delete(MDBX_txn *txn, MDBX_dbi dbi, const MDBX_val *key,
                  const MDBX_val *data, unsigned flags) {
  MDBX_cursor_couple cx;
  MDBX_cursor_op op;
  MDBX_val rdata;
  int rc;
  DKBUF_DEBUG;

  DEBUG("====> delete db %u key [%s], data [%s]", dbi, DKEY_DEBUG(key),
        DVAL_DEBUG(data));

  rc = cursor_init(&cx.outer, txn, dbi);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (data) {
    op = MDBX_GET_BOTH;
    rdata = *data;
    data = &rdata;
  } else {
    op = MDBX_SET;
    flags |= MDBX_ALLDUPS;
  }
  rc = cursor_set(&cx.outer, (MDBX_val *)key, (MDBX_val *)data, op).err;
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
    rc = cursor_del(&cx.outer, flags);
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
 * [in] naf The NODE_ADD_FLAGS for the new node.
 * Returns 0 on success, non-zero on failure. */
static int page_split(MDBX_cursor *mc, const MDBX_val *const newkey,
                      MDBX_val *const newdata, pgno_t newpgno,
                      const unsigned naf) {
  unsigned flags;
  int rc = MDBX_SUCCESS, foliage = 0;
  size_t i, ptop;
  MDBX_env *const env = mc->mc_txn->mt_env;
  MDBX_val rkey, xdata;
  MDBX_page *tmp_ki_copy = NULL;
  DKBUF;

  MDBX_page *const mp = mc->mc_pg[mc->mc_top];
  cASSERT(mc, (mp->mp_flags & P_ILL_BITS) == 0);

  const size_t newindx = mc->mc_ki[mc->mc_top];
  size_t nkeys = page_numkeys(mp);
  if (AUDIT_ENABLED()) {
    rc = cursor_check_updating(mc);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }
  STATIC_ASSERT(P_BRANCH == 1);
  const size_t minkeys = (mp->mp_flags & P_BRANCH) + (size_t)1;

  DEBUG(">> splitting %s-page %" PRIaPGNO
        " and adding %zu+%zu [%s] at %i, nkeys %zi",
        IS_LEAF(mp) ? "leaf" : "branch", mp->mp_pgno, newkey->iov_len,
        newdata ? newdata->iov_len : 0, DKEY_DEBUG(newkey),
        mc->mc_ki[mc->mc_top], nkeys);
  cASSERT(mc, nkeys + 1 >= minkeys * 2);

  /* Create a new sibling page. */
  pgr_t npr = page_new(mc, mp->mp_flags);
  if (unlikely(npr.err != MDBX_SUCCESS))
    return npr.err;
  MDBX_page *const sister = npr.page;
  sister->mp_leaf2_ksize = mp->mp_leaf2_ksize;
  DEBUG("new sibling: page %" PRIaPGNO, sister->mp_pgno);

  /* Usually when splitting the root page, the cursor
   * height is 1. But when called from update_key,
   * the cursor height may be greater because it walks
   * up the stack while finding the branch slot to update. */
  if (mc->mc_top < 1) {
    npr = page_new(mc, P_BRANCH);
    rc = npr.err;
    if (unlikely(rc != MDBX_SUCCESS))
      goto done;
    MDBX_page *const pp = npr.page;
    /* shift current top to make room for new parent */
    cASSERT(mc, mc->mc_snum < 2 && mc->mc_db->md_depth > 0);
#if MDBX_DEBUG
    memset(mc->mc_pg + 3, 0, sizeof(mc->mc_pg) - sizeof(mc->mc_pg[0]) * 3);
    memset(mc->mc_ki + 3, -1, sizeof(mc->mc_ki) - sizeof(mc->mc_ki[0]) * 3);
#endif
    mc->mc_pg[2] = mc->mc_pg[1];
    mc->mc_ki[2] = mc->mc_ki[1];
    mc->mc_pg[1] = mc->mc_pg[0];
    mc->mc_ki[1] = mc->mc_ki[0];
    mc->mc_pg[0] = pp;
    mc->mc_ki[0] = 0;
    mc->mc_db->md_root = pp->mp_pgno;
    DEBUG("root split! new root = %" PRIaPGNO, pp->mp_pgno);
    foliage = mc->mc_db->md_depth++;

    /* Add left (implicit) pointer. */
    rc = node_add_branch(mc, 0, NULL, mp->mp_pgno);
    if (unlikely(rc != MDBX_SUCCESS)) {
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
    if (AUDIT_ENABLED()) {
      rc = cursor_check_updating(mc);
      if (unlikely(rc != MDBX_SUCCESS))
        goto done;
    }
  } else {
    ptop = mc->mc_top - 1;
    DEBUG("parent branch page is %" PRIaPGNO, mc->mc_pg[ptop]->mp_pgno);
  }

  MDBX_cursor mn;
  cursor_copy(mc, &mn);
  mn.mc_pg[mn.mc_top] = sister;
  mn.mc_ki[mn.mc_top] = 0;
  mn.mc_ki[ptop] = mc->mc_ki[ptop] + 1;

  size_t split_indx =
      (newindx < nkeys)
          ? /* split at the middle */ (nkeys + 1) >> 1
          : /* split at the end (i.e. like append-mode ) */ nkeys - minkeys + 1;
  eASSERT(env, split_indx >= minkeys && split_indx <= nkeys - minkeys + 1);

  cASSERT(mc, !IS_BRANCH(mp) || newindx > 0);
  MDBX_val sepkey = {nullptr, 0};
  /* It is reasonable and possible to split the page at the begin */
  if (unlikely(newindx < minkeys)) {
    split_indx = minkeys;
    if (newindx == 0 && !(naf & MDBX_SPLIT_REPLACE)) {
      split_indx = 0;
      /* Checking for ability of splitting by the left-side insertion
       * of a pure page with the new key */
      for (i = 0; i < mc->mc_top; ++i)
        if (mc->mc_ki[i]) {
          get_key(page_node(mc->mc_pg[i], mc->mc_ki[i]), &sepkey);
          if (mc->mc_dbx->md_cmp(newkey, &sepkey) >= 0)
            split_indx = minkeys;
          break;
        }
      if (split_indx == 0) {
        /* Save the current first key which was omitted on the parent branch
         * page and should be updated if the new first entry will be added */
        if (IS_LEAF2(mp)) {
          sepkey.iov_len = mp->mp_leaf2_ksize;
          sepkey.iov_base = page_leaf2key(mp, 0, sepkey.iov_len);
        } else
          get_key(page_node(mp, 0), &sepkey);
        cASSERT(mc, mc->mc_dbx->md_cmp(newkey, &sepkey) < 0);
        /* Avoiding rare complex cases of nested split the parent page(s) */
        if (page_room(mc->mc_pg[ptop]) < branch_size(env, &sepkey))
          split_indx = minkeys;
      }
      if (foliage) {
        TRACE("pure-left: foliage %u, top %i, ptop %zu, split_indx %zi, "
              "minkeys %zi, sepkey %s, parent-room %zu, need4split %zu",
              foliage, mc->mc_top, ptop, split_indx, minkeys,
              DKEY_DEBUG(&sepkey), page_room(mc->mc_pg[ptop]),
              branch_size(env, &sepkey));
        TRACE("pure-left: newkey %s, newdata %s, newindx %zu",
              DKEY_DEBUG(newkey), DVAL_DEBUG(newdata), newindx);
      }
    }
  }

  const bool pure_right = split_indx == nkeys;
  const bool pure_left = split_indx == 0;
  if (unlikely(pure_right)) {
    /* newindx == split_indx == nkeys */
    TRACE("no-split, but add new pure page at the %s", "right/after");
    cASSERT(mc, newindx == nkeys && split_indx == nkeys && minkeys == 1);
    sepkey = *newkey;
  } else if (unlikely(pure_left)) {
    /* newindx == split_indx == 0 */
    TRACE("pure-left: no-split, but add new pure page at the %s",
          "left/before");
    cASSERT(mc, newindx == 0 && split_indx == 0 && minkeys == 1);
    TRACE("pure-left: old-first-key is %s", DKEY_DEBUG(&sepkey));
  } else {
    if (IS_LEAF2(sister)) {
      /* Move half of the keys to the right sibling */
      const intptr_t distance = mc->mc_ki[mc->mc_top] - split_indx;
      size_t ksize = mc->mc_db->md_xsize;
      void *const split = page_leaf2key(mp, split_indx, ksize);
      size_t rsize = (nkeys - split_indx) * ksize;
      size_t lsize = (nkeys - split_indx) * sizeof(indx_t);
      cASSERT(mc, mp->mp_lower >= lsize);
      mp->mp_lower -= (indx_t)lsize;
      cASSERT(mc, sister->mp_lower + lsize <= UINT16_MAX);
      sister->mp_lower += (indx_t)lsize;
      cASSERT(mc, mp->mp_upper + rsize - lsize <= UINT16_MAX);
      mp->mp_upper += (indx_t)(rsize - lsize);
      cASSERT(mc, sister->mp_upper >= rsize - lsize);
      sister->mp_upper -= (indx_t)(rsize - lsize);
      sepkey.iov_len = ksize;
      sepkey.iov_base = (newindx != split_indx) ? split : newkey->iov_base;
      if (distance < 0) {
        cASSERT(mc, ksize >= sizeof(indx_t));
        void *const ins = page_leaf2key(mp, mc->mc_ki[mc->mc_top], ksize);
        memcpy(sister->mp_ptrs, split, rsize);
        sepkey.iov_base = sister->mp_ptrs;
        memmove(ptr_disp(ins, ksize), ins,
                (split_indx - mc->mc_ki[mc->mc_top]) * ksize);
        memcpy(ins, newkey->iov_base, ksize);
        cASSERT(mc, UINT16_MAX - mp->mp_lower >= (int)sizeof(indx_t));
        mp->mp_lower += sizeof(indx_t);
        cASSERT(mc, mp->mp_upper >= ksize - sizeof(indx_t));
        mp->mp_upper -= (indx_t)(ksize - sizeof(indx_t));
        cASSERT(mc, (((ksize & page_numkeys(mp)) ^ mp->mp_upper) & 1) == 0);
      } else {
        memcpy(sister->mp_ptrs, split, distance * ksize);
        void *const ins = page_leaf2key(sister, distance, ksize);
        memcpy(ins, newkey->iov_base, ksize);
        memcpy(ptr_disp(ins, ksize), ptr_disp(split, distance * ksize),
               rsize - distance * ksize);
        cASSERT(mc, UINT16_MAX - sister->mp_lower >= (int)sizeof(indx_t));
        sister->mp_lower += sizeof(indx_t);
        cASSERT(mc, sister->mp_upper >= ksize - sizeof(indx_t));
        sister->mp_upper -= (indx_t)(ksize - sizeof(indx_t));
        cASSERT(mc, distance <= (int)UINT16_MAX);
        mc->mc_ki[mc->mc_top] = (indx_t)distance;
        cASSERT(mc,
                (((ksize & page_numkeys(sister)) ^ sister->mp_upper) & 1) == 0);
      }

      if (AUDIT_ENABLED()) {
        rc = cursor_check_updating(mc);
        if (unlikely(rc != MDBX_SUCCESS))
          goto done;
        rc = cursor_check_updating(&mn);
        if (unlikely(rc != MDBX_SUCCESS))
          goto done;
      }
    } else {
      /* grab a page to hold a temporary copy */
      tmp_ki_copy = page_malloc(mc->mc_txn, 1);
      if (unlikely(tmp_ki_copy == NULL)) {
        rc = MDBX_ENOMEM;
        goto done;
      }

      const size_t max_space = page_space(env);
      const size_t new_size = IS_LEAF(mp) ? leaf_size(env, newkey, newdata)
                                          : branch_size(env, newkey);

      /* prepare to insert */
      for (i = 0; i < newindx; ++i)
        tmp_ki_copy->mp_ptrs[i] = mp->mp_ptrs[i];
      tmp_ki_copy->mp_ptrs[i] = (indx_t)-1;
      while (++i <= nkeys)
        tmp_ki_copy->mp_ptrs[i] = mp->mp_ptrs[i - 1];
      tmp_ki_copy->mp_pgno = mp->mp_pgno;
      tmp_ki_copy->mp_flags = mp->mp_flags;
      tmp_ki_copy->mp_txnid = INVALID_TXNID;
      tmp_ki_copy->mp_lower = 0;
      tmp_ki_copy->mp_upper = (indx_t)max_space;

      /* Добавляемый узел может не поместиться в страницу-половину вместе
       * с количественной половиной узлов из исходной страницы. В худшем случае,
       * в страницу-половину с добавляемым узлом могут попасть самые больше узлы
       * из исходной страницы, а другую половину только узлы с самыми короткими
       * ключами и с пустыми данными. Поэтому, чтобы найти подходящую границу
       * разреза требуется итерировать узлы и считая их объем.
       *
       * Однако, при простом количественном делении (без учета размера ключей
       * и данных) на страницах-половинах будет примерно вдвое меньше узлов.
       * Поэтому добавляемый узел точно поместится, если его размер не больше
       * чем место "освобождающееся" от заголовков узлов, которые переедут
       * в другую страницу-половину. Кроме этого, как минимум по одному байту
       * будет в каждом ключе, в худшем случае кроме одного, который может быть
       * нулевого размера. */

      if (newindx == split_indx && nkeys >= 5) {
        STATIC_ASSERT(P_BRANCH == 1);
        split_indx += mp->mp_flags & P_BRANCH;
      }
      eASSERT(env, split_indx >= minkeys && split_indx <= nkeys + 1 - minkeys);
      const size_t dim_nodes =
          (newindx >= split_indx) ? split_indx : nkeys - split_indx;
      const size_t dim_used = (sizeof(indx_t) + NODESIZE + 1) * dim_nodes;
      if (new_size >= dim_used) {
        /* Search for best acceptable split point */
        i = (newindx < split_indx) ? 0 : nkeys;
        intptr_t dir = (newindx < split_indx) ? 1 : -1;
        size_t before = 0, after = new_size + page_used(env, mp);
        size_t best_split = split_indx;
        size_t best_shift = INT_MAX;

        TRACE("seek separator from %zu, step %zi, default %zu, new-idx %zu, "
              "new-size %zu",
              i, dir, split_indx, newindx, new_size);
        do {
          cASSERT(mc, i <= nkeys);
          size_t size = new_size;
          if (i != newindx) {
            MDBX_node *node = ptr_disp(mp, tmp_ki_copy->mp_ptrs[i] + PAGEHDRSZ);
            size = NODESIZE + node_ks(node) + sizeof(indx_t);
            if (IS_LEAF(mp))
              size += (node_flags(node) & F_BIGDATA) ? sizeof(pgno_t)
                                                     : node_ds(node);
            size = EVEN(size);
          }

          before += size;
          after -= size;
          TRACE("step %zu, size %zu, before %zu, after %zu, max %zu", i, size,
                before, after, max_space);

          if (before <= max_space && after <= max_space) {
            const size_t split = i + (dir > 0);
            if (split >= minkeys && split <= nkeys + 1 - minkeys) {
              const size_t shift = branchless_abs(split_indx - split);
              if (shift >= best_shift)
                break;
              best_shift = shift;
              best_split = split;
              if (!best_shift)
                break;
            }
          }
          i += dir;
        } while (i < nkeys);

        split_indx = best_split;
        TRACE("chosen %zu", split_indx);
      }
      eASSERT(env, split_indx >= minkeys && split_indx <= nkeys + 1 - minkeys);

      sepkey = *newkey;
      if (split_indx != newindx) {
        MDBX_node *node =
            ptr_disp(mp, tmp_ki_copy->mp_ptrs[split_indx] + PAGEHDRSZ);
        sepkey.iov_len = node_ks(node);
        sepkey.iov_base = node_key(node);
      }
    }
  }
  DEBUG("separator is %zd [%s]", split_indx, DKEY_DEBUG(&sepkey));

  bool did_split_parent = false;
  /* Copy separator key to the parent. */
  if (page_room(mn.mc_pg[ptop]) < branch_size(env, &sepkey)) {
    TRACE("need split parent branch-page for key %s", DKEY_DEBUG(&sepkey));
    cASSERT(mc, page_numkeys(mn.mc_pg[ptop]) > 2);
    cASSERT(mc, !pure_left);
    const int snum = mc->mc_snum;
    const int depth = mc->mc_db->md_depth;
    mn.mc_snum--;
    mn.mc_top--;
    did_split_parent = true;
    /* We want other splits to find mn when doing fixups */
    WITH_CURSOR_TRACKING(
        mn, rc = page_split(&mn, &sepkey, NULL, sister->mp_pgno, 0));
    if (unlikely(rc != MDBX_SUCCESS))
      goto done;
    cASSERT(mc, (int)mc->mc_snum - snum == mc->mc_db->md_depth - depth);
    if (AUDIT_ENABLED()) {
      rc = cursor_check_updating(mc);
      if (unlikely(rc != MDBX_SUCCESS))
        goto done;
    }

    /* root split? */
    ptop += mc->mc_snum - (size_t)snum;

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
        rc = cursor_sibling(mc, SIBLING_LEFT);
        if (unlikely(rc != MDBX_SUCCESS)) {
          if (rc == MDBX_NOTFOUND) /* improper mdbx_cursor_sibling() result */ {
            ERROR("unexpected %i error going left sibling", rc);
            rc = MDBX_PROBLEM;
          }
          goto done;
        }
      }
    }
  } else if (unlikely(pure_left)) {
    MDBX_page *ptop_page = mc->mc_pg[ptop];
    TRACE("pure-left: adding to parent page %u node[%u] left-leaf page #%u key "
          "%s",
          ptop_page->mp_pgno, mc->mc_ki[ptop], sister->mp_pgno,
          DKEY(mc->mc_ki[ptop] ? newkey : NULL));
    assert(mc->mc_top == ptop + 1);
    mc->mc_top = (uint8_t)ptop;
    rc = node_add_branch(mc, mc->mc_ki[ptop], mc->mc_ki[ptop] ? newkey : NULL,
                         sister->mp_pgno);
    cASSERT(mc, mp == mc->mc_pg[ptop + 1] && newindx == mc->mc_ki[ptop + 1] &&
                    ptop == mc->mc_top);

    if (likely(rc == MDBX_SUCCESS) && mc->mc_ki[ptop] == 0) {
      MDBX_node *node = page_node(mc->mc_pg[ptop], 1);
      TRACE("pure-left: update prev-first key on parent to %s", DKEY(&sepkey));
      cASSERT(mc, node_ks(node) == 0 && node_pgno(node) == mp->mp_pgno);
      cASSERT(mc, mc->mc_top == ptop && mc->mc_ki[ptop] == 0);
      mc->mc_ki[ptop] = 1;
      rc = update_key(mc, &sepkey);
      cASSERT(mc, mc->mc_top == ptop && mc->mc_ki[ptop] == 1);
      cASSERT(mc, mp == mc->mc_pg[ptop + 1] && newindx == mc->mc_ki[ptop + 1]);
      mc->mc_ki[ptop] = 0;
    } else {
      TRACE("pure-left: no-need-update prev-first key on parent %s",
            DKEY(&sepkey));
    }

    mc->mc_top++;
    if (unlikely(rc != MDBX_SUCCESS))
      goto done;

    MDBX_node *node = page_node(mc->mc_pg[ptop], mc->mc_ki[ptop] + (size_t)1);
    cASSERT(mc, node_pgno(node) == mp->mp_pgno && mc->mc_pg[ptop] == ptop_page);
  } else {
    mn.mc_top--;
    TRACE("add-to-parent the right-entry[%u] for new sibling-page",
          mn.mc_ki[ptop]);
    rc = node_add_branch(&mn, mn.mc_ki[ptop], &sepkey, sister->mp_pgno);
    mn.mc_top++;
    if (unlikely(rc != MDBX_SUCCESS))
      goto done;
  }

  if (unlikely(pure_left | pure_right)) {
    mc->mc_pg[mc->mc_top] = sister;
    mc->mc_ki[mc->mc_top] = 0;
    switch (PAGETYPE_WHOLE(sister)) {
    case P_LEAF: {
      cASSERT(mc, newpgno == 0 || newpgno == P_INVALID);
      rc = node_add_leaf(mc, 0, newkey, newdata, naf);
    } break;
    case P_LEAF | P_LEAF2: {
      cASSERT(mc, (naf & (F_BIGDATA | F_SUBDATA | F_DUPDATA)) == 0);
      cASSERT(mc, newpgno == 0 || newpgno == P_INVALID);
      rc = node_add_leaf2(mc, 0, newkey);
    } break;
    default:
      rc = bad_page(sister, "wrong page-type %u\n", PAGETYPE_WHOLE(sister));
    }
    if (unlikely(rc != MDBX_SUCCESS))
      goto done;

    if (pure_right) {
      for (i = 0; i < mc->mc_top; i++)
        mc->mc_ki[i] = mn.mc_ki[i];
    } else if (mc->mc_ki[mc->mc_top - 1] == 0) {
      for (i = 2; i <= mc->mc_top; ++i)
        if (mc->mc_ki[mc->mc_top - i]) {
          get_key(
              page_node(mc->mc_pg[mc->mc_top - i], mc->mc_ki[mc->mc_top - i]),
              &sepkey);
          if (mc->mc_dbx->md_cmp(newkey, &sepkey) < 0) {
            mc->mc_top -= (uint8_t)i;
            DEBUG("pure-left: update new-first on parent [%i] page %u key %s",
                  mc->mc_ki[mc->mc_top], mc->mc_pg[mc->mc_top]->mp_pgno,
                  DKEY(newkey));
            rc = update_key(mc, newkey);
            mc->mc_top += (uint8_t)i;
            if (unlikely(rc != MDBX_SUCCESS))
              goto done;
          }
          break;
        }
    }
  } else if (tmp_ki_copy) { /* !IS_LEAF2(mp) */
    /* Move nodes */
    mc->mc_pg[mc->mc_top] = sister;
    i = split_indx;
    size_t n = 0;
    do {
      TRACE("i %zu, nkeys %zu => n %zu, rp #%u", i, nkeys, n, sister->mp_pgno);
      pgno_t pgno = 0;
      MDBX_val *rdata = NULL;
      if (i == newindx) {
        rkey = *newkey;
        if (IS_LEAF(mp))
          rdata = newdata;
        else
          pgno = newpgno;
        flags = naf;
        /* Update index for the new key. */
        mc->mc_ki[mc->mc_top] = (indx_t)n;
      } else {
        MDBX_node *node = ptr_disp(mp, tmp_ki_copy->mp_ptrs[i] + PAGEHDRSZ);
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

      switch (PAGETYPE_WHOLE(sister)) {
      case P_BRANCH: {
        cASSERT(mc, 0 == (uint16_t)flags);
        /* First branch index doesn't need key data. */
        rc = node_add_branch(mc, n, n ? &rkey : NULL, pgno);
      } break;
      case P_LEAF: {
        cASSERT(mc, pgno == 0);
        cASSERT(mc, rdata != NULL);
        rc = node_add_leaf(mc, n, &rkey, rdata, flags);
      } break;
      /* case P_LEAF | P_LEAF2: {
        cASSERT(mc, (nflags & (F_BIGDATA | F_SUBDATA | F_DUPDATA)) == 0);
        cASSERT(mc, gno == 0);
        rc = mdbx_node_add_leaf2(mc, n, &rkey);
      } break; */
      default:
        rc = bad_page(sister, "wrong page-type %u\n", PAGETYPE_WHOLE(sister));
      }
      if (unlikely(rc != MDBX_SUCCESS))
        goto done;

      ++n;
      if (++i > nkeys) {
        i = 0;
        n = 0;
        mc->mc_pg[mc->mc_top] = tmp_ki_copy;
        TRACE("switch to mp #%u", tmp_ki_copy->mp_pgno);
      }
    } while (i != split_indx);

    TRACE("i %zu, nkeys %zu, n %zu, pgno #%u", i, nkeys, n,
          mc->mc_pg[mc->mc_top]->mp_pgno);

    nkeys = page_numkeys(tmp_ki_copy);
    for (i = 0; i < nkeys; i++)
      mp->mp_ptrs[i] = tmp_ki_copy->mp_ptrs[i];
    mp->mp_lower = tmp_ki_copy->mp_lower;
    mp->mp_upper = tmp_ki_copy->mp_upper;
    memcpy(page_node(mp, nkeys - 1), page_node(tmp_ki_copy, nkeys - 1),
           env->me_psize - tmp_ki_copy->mp_upper - PAGEHDRSZ);

    /* reset back to original page */
    if (newindx < split_indx) {
      mc->mc_pg[mc->mc_top] = mp;
    } else {
      mc->mc_pg[mc->mc_top] = sister;
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
  } else if (newindx >= split_indx) {
    mc->mc_pg[mc->mc_top] = sister;
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

  /* Adjust other cursors pointing to mp and/or to parent page */
  nkeys = page_numkeys(mp);
  for (MDBX_cursor *m2 = mc->mc_txn->mt_cursors[mc->mc_dbi]; m2;
       m2 = m2->mc_next) {
    MDBX_cursor *m3 = (mc->mc_flags & C_SUB) ? &m2->mc_xcursor->mx_cursor : m2;
    if (m3 == mc)
      continue;
    if (!(m2->mc_flags & m3->mc_flags & C_INITIALIZED))
      continue;
    if (foliage) {
      /* sub cursors may be on different DB */
      if (m3->mc_pg[0] != mp)
        continue;
      /* root split */
      for (int k = foliage; k >= 0; k--) {
        m3->mc_ki[k + 1] = m3->mc_ki[k];
        m3->mc_pg[k + 1] = m3->mc_pg[k];
      }
      m3->mc_ki[0] = m3->mc_ki[0] >= nkeys + pure_left;
      m3->mc_pg[0] = mc->mc_pg[0];
      m3->mc_snum++;
      m3->mc_top++;
    }

    if (m3->mc_top >= mc->mc_top && m3->mc_pg[mc->mc_top] == mp && !pure_left) {
      if (m3->mc_ki[mc->mc_top] >= newindx && !(naf & MDBX_SPLIT_REPLACE))
        m3->mc_ki[mc->mc_top]++;
      if (m3->mc_ki[mc->mc_top] >= nkeys) {
        m3->mc_pg[mc->mc_top] = sister;
        cASSERT(mc, m3->mc_ki[mc->mc_top] >= nkeys);
        m3->mc_ki[mc->mc_top] -= (indx_t)nkeys;
        for (i = 0; i < mc->mc_top; i++) {
          m3->mc_ki[i] = mn.mc_ki[i];
          m3->mc_pg[i] = mn.mc_pg[i];
        }
      }
    } else if (!did_split_parent && m3->mc_top >= ptop &&
               m3->mc_pg[ptop] == mc->mc_pg[ptop] &&
               m3->mc_ki[ptop] >= mc->mc_ki[ptop]) {
      m3->mc_ki[ptop]++; /* also for the `pure-left` case */
    }
    if (XCURSOR_INITED(m3) && IS_LEAF(mp))
      XCURSOR_REFRESH(m3, m3->mc_pg[mc->mc_top], m3->mc_ki[mc->mc_top]);
  }
  TRACE("mp #%u left: %zd, sister #%u left: %zd", mp->mp_pgno, page_room(mp),
        sister->mp_pgno, page_room(sister));

done:
  if (tmp_ki_copy)
    dpage_free(env, tmp_ki_copy, 1);

  if (unlikely(rc != MDBX_SUCCESS))
    mc->mc_txn->mt_flags |= MDBX_TXN_ERROR;
  else {
    if (AUDIT_ENABLED())
      rc = cursor_check_updating(mc);
    if (unlikely(naf & MDBX_RESERVE)) {
      MDBX_node *node = page_node(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top]);
      if (!(node_flags(node) & F_BIGDATA))
        newdata->iov_base = node_data(node);
    }
#if MDBX_ENABLE_PGOP_STAT
    env->me_lck->mti_pgop_stat.split.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
  }

  DEBUG("<< mp #%u, rc %d", mp->mp_pgno, rc);
  return rc;
}

int mdbx_put(MDBX_txn *txn, MDBX_dbi dbi, const MDBX_val *key, MDBX_val *data,
             MDBX_put_flags_t flags) {
  int rc = check_txn_rw(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!key || !data))
    return MDBX_EINVAL;

  if (unlikely(!check_dbi(txn, dbi, DBI_USRVALID)))
    return MDBX_BAD_DBI;

  if (unlikely(flags & ~(MDBX_NOOVERWRITE | MDBX_NODUPDATA | MDBX_ALLDUPS |
                         MDBX_ALLDUPS | MDBX_RESERVE | MDBX_APPEND |
                         MDBX_APPENDDUP | MDBX_CURRENT | MDBX_MULTIPLE)))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_flags & (MDBX_TXN_RDONLY | MDBX_TXN_BLOCKED)))
    return (txn->mt_flags & MDBX_TXN_RDONLY) ? MDBX_EACCESS : MDBX_BAD_TXN;

  MDBX_cursor_couple cx;
  rc = cursor_init(&cx.outer, txn, dbi);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;
  cx.outer.mc_next = txn->mt_cursors[dbi];
  txn->mt_cursors[dbi] = &cx.outer;

  /* LY: support for update (explicit overwrite) */
  if (flags & MDBX_CURRENT) {
    rc = cursor_set(&cx.outer, (MDBX_val *)key, NULL, MDBX_SET).err;
    if (likely(rc == MDBX_SUCCESS) &&
        (txn->mt_dbs[dbi].md_flags & MDBX_DUPSORT) &&
        (flags & MDBX_ALLDUPS) == 0) {
      /* LY: allows update (explicit overwrite) only for unique keys */
      MDBX_node *node = page_node(cx.outer.mc_pg[cx.outer.mc_top],
                                  cx.outer.mc_ki[cx.outer.mc_top]);
      if (node_flags(node) & F_DUPDATA) {
        tASSERT(txn, XCURSOR_INITED(&cx.outer) &&
                         cx.outer.mc_xcursor->mx_db.md_entries > 1);
        rc = MDBX_EMULTIVAL;
        if ((flags & MDBX_NOOVERWRITE) == 0) {
          flags -= MDBX_CURRENT;
          rc = cursor_del(&cx.outer, MDBX_ALLDUPS);
        }
      }
    }
  }

  if (likely(rc == MDBX_SUCCESS))
    rc = cursor_put_checklen(&cx.outer, key, data, flags);
  txn->mt_cursors[dbi] = cx.outer.mc_next;

  return rc;
}

/**** COPYING *****************************************************************/

/* State needed for a double-buffering compacting copy. */
typedef struct mdbx_compacting_ctx {
  MDBX_env *mc_env;
  MDBX_txn *mc_txn;
  osal_condpair_t mc_condpair;
  uint8_t *mc_wbuf[2];
  size_t mc_wlen[2];
  mdbx_filehandle_t mc_fd;
  /* Error code.  Never cleared if set.  Both threads can set nonzero
   * to fail the copy.  Not mutex-protected, MDBX expects atomic int. */
  volatile int mc_error;
  pgno_t mc_next_pgno;
  volatile unsigned mc_head;
  volatile unsigned mc_tail;
} mdbx_compacting_ctx;

/* Dedicated writer thread for compacting copy. */
__cold static THREAD_RESULT THREAD_CALL compacting_write_thread(void *arg) {
  mdbx_compacting_ctx *const ctx = arg;

#if defined(EPIPE) && !(defined(_WIN32) || defined(_WIN64))
  sigset_t sigset;
  sigemptyset(&sigset);
  sigaddset(&sigset, SIGPIPE);
  ctx->mc_error = pthread_sigmask(SIG_BLOCK, &sigset, NULL);
#endif /* EPIPE */

  osal_condpair_lock(&ctx->mc_condpair);
  while (!ctx->mc_error) {
    while (ctx->mc_tail == ctx->mc_head && !ctx->mc_error) {
      int err = osal_condpair_wait(&ctx->mc_condpair, true);
      if (err != MDBX_SUCCESS) {
        ctx->mc_error = err;
        goto bailout;
      }
    }
    const unsigned toggle = ctx->mc_tail & 1;
    size_t wsize = ctx->mc_wlen[toggle];
    if (wsize == 0) {
      ctx->mc_tail += 1;
      break /* EOF */;
    }
    ctx->mc_wlen[toggle] = 0;
    uint8_t *ptr = ctx->mc_wbuf[toggle];
    if (!ctx->mc_error) {
      int err = osal_write(ctx->mc_fd, ptr, wsize);
      if (err != MDBX_SUCCESS) {
#if defined(EPIPE) && !(defined(_WIN32) || defined(_WIN64))
        if (err == EPIPE) {
          /* Collect the pending SIGPIPE,
           * otherwise at least OS X gives it to the process on thread-exit. */
          int unused;
          sigwait(&sigset, &unused);
        }
#endif /* EPIPE */
        ctx->mc_error = err;
        goto bailout;
      }
    }
    ctx->mc_tail += 1;
    osal_condpair_signal(&ctx->mc_condpair, false);
  }
bailout:
  osal_condpair_unlock(&ctx->mc_condpair);
  return (THREAD_RESULT)0;
}

/* Give buffer and/or MDBX_EOF to writer thread, await unused buffer. */
__cold static int compacting_toggle_write_buffers(mdbx_compacting_ctx *ctx) {
  osal_condpair_lock(&ctx->mc_condpair);
  eASSERT(ctx->mc_env, ctx->mc_head - ctx->mc_tail < 2 || ctx->mc_error);
  ctx->mc_head += 1;
  osal_condpair_signal(&ctx->mc_condpair, true);
  while (!ctx->mc_error &&
         ctx->mc_head - ctx->mc_tail == 2 /* both buffers in use */) {
    int err = osal_condpair_wait(&ctx->mc_condpair, false);
    if (err != MDBX_SUCCESS)
      ctx->mc_error = err;
  }
  osal_condpair_unlock(&ctx->mc_condpair);
  return ctx->mc_error;
}

__cold static int compacting_walk_sdb(mdbx_compacting_ctx *ctx, MDBX_db *sdb);

static int compacting_put_bytes(mdbx_compacting_ctx *ctx, const void *src,
                                size_t bytes, pgno_t pgno, pgno_t npages) {
  assert(pgno == 0 || bytes > PAGEHDRSZ);
  while (bytes > 0) {
    const size_t side = ctx->mc_head & 1;
    const size_t left = MDBX_ENVCOPY_WRITEBUF - ctx->mc_wlen[side];
    if (left < (pgno ? PAGEHDRSZ : 1)) {
      int err = compacting_toggle_write_buffers(ctx);
      if (unlikely(err != MDBX_SUCCESS))
        return err;
      continue;
    }
    const size_t chunk = (bytes < left) ? bytes : left;
    void *const dst = ctx->mc_wbuf[side] + ctx->mc_wlen[side];
    if (src) {
      memcpy(dst, src, chunk);
      if (pgno) {
        assert(chunk > PAGEHDRSZ);
        MDBX_page *mp = dst;
        mp->mp_pgno = pgno;
        if (mp->mp_txnid == 0)
          mp->mp_txnid = ctx->mc_txn->mt_txnid;
        if (mp->mp_flags == P_OVERFLOW) {
          assert(bytes <= pgno2bytes(ctx->mc_env, npages));
          mp->mp_pages = npages;
        }
        pgno = 0;
      }
      src = ptr_disp(src, chunk);
    } else
      memset(dst, 0, chunk);
    bytes -= chunk;
    ctx->mc_wlen[side] += chunk;
  }
  return MDBX_SUCCESS;
}

static int compacting_put_page(mdbx_compacting_ctx *ctx, const MDBX_page *mp,
                               const size_t head_bytes, const size_t tail_bytes,
                               const pgno_t npages) {
  if (tail_bytes) {
    assert(head_bytes + tail_bytes <= ctx->mc_env->me_psize);
    assert(npages == 1 &&
           (PAGETYPE_WHOLE(mp) == P_BRANCH || PAGETYPE_WHOLE(mp) == P_LEAF));
  } else {
    assert(head_bytes <= pgno2bytes(ctx->mc_env, npages));
    assert((npages == 1 && PAGETYPE_WHOLE(mp) == (P_LEAF | P_LEAF2)) ||
           PAGETYPE_WHOLE(mp) == P_OVERFLOW);
  }

  const pgno_t pgno = ctx->mc_next_pgno;
  ctx->mc_next_pgno += npages;
  int err = compacting_put_bytes(ctx, mp, head_bytes, pgno, npages);
  if (unlikely(err != MDBX_SUCCESS))
    return err;
  err = compacting_put_bytes(
      ctx, nullptr, pgno2bytes(ctx->mc_env, npages) - (head_bytes + tail_bytes),
      0, 0);
  if (unlikely(err != MDBX_SUCCESS))
    return err;
  return compacting_put_bytes(
      ctx, ptr_disp(mp, ctx->mc_env->me_psize - tail_bytes), tail_bytes, 0, 0);
}

__cold static int compacting_walk_tree(mdbx_compacting_ctx *ctx,
                                       MDBX_cursor *mc, pgno_t *root,
                                       txnid_t parent_txnid) {
  mc->mc_snum = 1;
  int rc = page_get(mc, *root, &mc->mc_pg[0], parent_txnid);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  rc = page_search_root(mc, nullptr, MDBX_PS_FIRST);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  /* Make cursor pages writable */
  void *const buf = osal_malloc(pgno2bytes(ctx->mc_env, mc->mc_snum));
  if (buf == NULL)
    return MDBX_ENOMEM;

  void *ptr = buf;
  for (size_t i = 0; i < mc->mc_top; i++) {
    page_copy(ptr, mc->mc_pg[i], ctx->mc_env->me_psize);
    mc->mc_pg[i] = ptr;
    ptr = ptr_disp(ptr, ctx->mc_env->me_psize);
  }
  /* This is writable space for a leaf page. Usually not needed. */
  MDBX_page *const leaf = ptr;

  while (mc->mc_snum > 0) {
    MDBX_page *mp = mc->mc_pg[mc->mc_top];
    size_t n = page_numkeys(mp);

    if (IS_LEAF(mp)) {
      if (!(mc->mc_flags &
            C_SUB) /* may have nested F_SUBDATA or F_BIGDATA nodes */) {
        for (size_t i = 0; i < n; i++) {
          MDBX_node *node = page_node(mp, i);
          if (node_flags(node) == F_BIGDATA) {
            /* Need writable leaf */
            if (mp != leaf) {
              mc->mc_pg[mc->mc_top] = leaf;
              page_copy(leaf, mp, ctx->mc_env->me_psize);
              mp = leaf;
              node = page_node(mp, i);
            }

            const pgr_t lp =
                page_get_large(mc, node_largedata_pgno(node), mp->mp_txnid);
            if (unlikely((rc = lp.err) != MDBX_SUCCESS))
              goto done;
            const size_t datasize = node_ds(node);
            const pgno_t npages = number_of_ovpages(ctx->mc_env, datasize);
            poke_pgno(node_data(node), ctx->mc_next_pgno);
            rc = compacting_put_page(ctx, lp.page, PAGEHDRSZ + datasize, 0,
                                     npages);
            if (unlikely(rc != MDBX_SUCCESS))
              goto done;
          } else if (node_flags(node) & F_SUBDATA) {
            if (!MDBX_DISABLE_VALIDATION &&
                unlikely(node_ds(node) != sizeof(MDBX_db))) {
              rc = MDBX_CORRUPTED;
              goto done;
            }

            /* Need writable leaf */
            if (mp != leaf) {
              mc->mc_pg[mc->mc_top] = leaf;
              page_copy(leaf, mp, ctx->mc_env->me_psize);
              mp = leaf;
              node = page_node(mp, i);
            }

            MDBX_db *nested = nullptr;
            if (node_flags(node) & F_DUPDATA) {
              rc = cursor_xinit1(mc, node, mp);
              if (likely(rc == MDBX_SUCCESS)) {
                nested = &mc->mc_xcursor->mx_db;
                rc = compacting_walk_tree(ctx, &mc->mc_xcursor->mx_cursor,
                                          &nested->md_root, mp->mp_txnid);
              }
            } else {
              cASSERT(mc, (mc->mc_flags & C_SUB) == 0 && mc->mc_xcursor == 0);
              MDBX_cursor_couple *couple =
                  container_of(mc, MDBX_cursor_couple, outer);
              cASSERT(mc,
                      couple->inner.mx_cursor.mc_signature == ~MDBX_MC_LIVE &&
                          !couple->inner.mx_cursor.mc_flags &&
                          !couple->inner.mx_cursor.mc_db &&
                          !couple->inner.mx_cursor.mc_dbx);
              nested = &couple->inner.mx_db;
              memcpy(nested, node_data(node), sizeof(MDBX_db));
              rc = compacting_walk_sdb(ctx, nested);
            }
            if (unlikely(rc != MDBX_SUCCESS))
              goto done;
            memcpy(node_data(node), nested, sizeof(MDBX_db));
          }
        }
      }
    } else {
      mc->mc_ki[mc->mc_top]++;
      if (mc->mc_ki[mc->mc_top] < n) {
        while (1) {
          const MDBX_node *node = page_node(mp, mc->mc_ki[mc->mc_top]);
          rc = page_get(mc, node_pgno(node), &mp, mp->mp_txnid);
          if (unlikely(rc != MDBX_SUCCESS))
            goto done;
          mc->mc_top++;
          mc->mc_snum++;
          mc->mc_ki[mc->mc_top] = 0;
          if (!IS_BRANCH(mp)) {
            mc->mc_pg[mc->mc_top] = mp;
            break;
          }
          /* Whenever we advance to a sibling branch page,
           * we must proceed all the way down to its first leaf. */
          page_copy(mc->mc_pg[mc->mc_top], mp, ctx->mc_env->me_psize);
        }
        continue;
      }
    }

    const pgno_t pgno = ctx->mc_next_pgno;
    if (likely(!IS_LEAF2(mp))) {
      rc = compacting_put_page(
          ctx, mp, PAGEHDRSZ + mp->mp_lower,
          ctx->mc_env->me_psize - (PAGEHDRSZ + mp->mp_upper), 1);
    } else {
      rc = compacting_put_page(
          ctx, mp, PAGEHDRSZ + page_numkeys(mp) * mp->mp_leaf2_ksize, 0, 1);
    }
    if (unlikely(rc != MDBX_SUCCESS))
      goto done;

    if (mc->mc_top) {
      /* Update parent if there is one */
      node_set_pgno(
          page_node(mc->mc_pg[mc->mc_top - 1], mc->mc_ki[mc->mc_top - 1]),
          pgno);
      cursor_pop(mc);
    } else {
      /* Otherwise we're done */
      *root = pgno;
      break;
    }
  }
done:
  osal_free(buf);
  return rc;
}

__cold static int compacting_walk_sdb(mdbx_compacting_ctx *ctx, MDBX_db *sdb) {
  if (unlikely(sdb->md_root == P_INVALID))
    return MDBX_SUCCESS; /* empty db */

  MDBX_cursor_couple couple;
  memset(&couple, 0, sizeof(couple));
  couple.inner.mx_cursor.mc_signature = ~MDBX_MC_LIVE;
  MDBX_dbx dbx = {.md_klen_min = INT_MAX};
  uint8_t dbistate = DBI_VALID | DBI_AUDITED;
  int rc = couple_init(&couple, ~0u, ctx->mc_txn, sdb, &dbx, &dbistate);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  couple.outer.mc_checking |= CC_SKIPORD | CC_PAGECHECK;
  couple.inner.mx_cursor.mc_checking |= CC_SKIPORD | CC_PAGECHECK;
  if (!sdb->md_mod_txnid)
    sdb->md_mod_txnid = ctx->mc_txn->mt_txnid;
  return compacting_walk_tree(ctx, &couple.outer, &sdb->md_root,
                              sdb->md_mod_txnid);
}

__cold static void compacting_fixup_meta(MDBX_env *env, MDBX_meta *meta) {
  eASSERT(env, meta->mm_dbs[FREE_DBI].md_mod_txnid ||
                   meta->mm_dbs[FREE_DBI].md_root == P_INVALID);
  eASSERT(env, meta->mm_dbs[MAIN_DBI].md_mod_txnid ||
                   meta->mm_dbs[MAIN_DBI].md_root == P_INVALID);

  /* Calculate filesize taking in account shrink/growing thresholds */
  if (meta->mm_geo.next != meta->mm_geo.now) {
    meta->mm_geo.now = meta->mm_geo.next;
    const size_t aligner = pv2pages(
        meta->mm_geo.grow_pv ? meta->mm_geo.grow_pv : meta->mm_geo.shrink_pv);
    if (aligner) {
      const pgno_t aligned = pgno_align2os_pgno(
          env, meta->mm_geo.next + aligner - meta->mm_geo.next % aligner);
      meta->mm_geo.now = aligned;
    }
  }

  if (meta->mm_geo.now < meta->mm_geo.lower)
    meta->mm_geo.now = meta->mm_geo.lower;
  if (meta->mm_geo.now > meta->mm_geo.upper)
    meta->mm_geo.now = meta->mm_geo.upper;

  /* Update signature */
  assert(meta->mm_geo.now >= meta->mm_geo.next);
  unaligned_poke_u64(4, meta->mm_sign, meta_sign(meta));
}

/* Make resizable */
__cold static void meta_make_sizeable(MDBX_meta *meta) {
  meta->mm_geo.lower = MIN_PAGENO;
  if (meta->mm_geo.grow_pv == 0) {
    const pgno_t step = 1 + (meta->mm_geo.upper - meta->mm_geo.lower) / 42;
    meta->mm_geo.grow_pv = pages2pv(step);
  }
  if (meta->mm_geo.shrink_pv == 0) {
    const pgno_t step = pv2pages(meta->mm_geo.grow_pv) << 1;
    meta->mm_geo.shrink_pv = pages2pv(step);
  }
}

/* Copy environment with compaction. */
__cold static int env_compact(MDBX_env *env, MDBX_txn *read_txn,
                              mdbx_filehandle_t fd, uint8_t *buffer,
                              const bool dest_is_pipe,
                              const MDBX_copy_flags_t flags) {
  const size_t meta_bytes = pgno2bytes(env, NUM_METAS);
  uint8_t *const data_buffer =
      buffer + ceil_powerof2(meta_bytes, env->me_os_psize);
  MDBX_meta *const meta = init_metas(env, buffer);
  meta_set_txnid(env, meta, read_txn->mt_txnid);

  if (flags & MDBX_CP_FORCE_DYNAMIC_SIZE)
    meta_make_sizeable(meta);

  /* copy canary sequences if present */
  if (read_txn->mt_canary.v) {
    meta->mm_canary = read_txn->mt_canary;
    meta->mm_canary.v = constmeta_txnid(meta);
  }

  if (read_txn->mt_dbs[MAIN_DBI].md_root == P_INVALID) {
    /* When the DB is empty, handle it specially to
     * fix any breakage like page leaks from ITS#8174. */
    meta->mm_dbs[MAIN_DBI].md_flags = read_txn->mt_dbs[MAIN_DBI].md_flags;
    compacting_fixup_meta(env, meta);
    if (dest_is_pipe) {
      int rc = osal_write(fd, buffer, meta_bytes);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
    }
  } else {
    /* Count free pages + GC pages. */
    MDBX_cursor_couple couple;
    int rc = cursor_init(&couple.outer, read_txn, FREE_DBI);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    pgno_t gc = read_txn->mt_dbs[FREE_DBI].md_branch_pages +
                read_txn->mt_dbs[FREE_DBI].md_leaf_pages +
                read_txn->mt_dbs[FREE_DBI].md_overflow_pages;
    MDBX_val key, data;
    while ((rc = cursor_get(&couple.outer, &key, &data, MDBX_NEXT)) ==
           MDBX_SUCCESS) {
      const MDBX_PNL pnl = data.iov_base;
      if (unlikely(data.iov_len % sizeof(pgno_t) ||
                   data.iov_len < MDBX_PNL_SIZEOF(pnl) ||
                   !(pnl_check(pnl, read_txn->mt_next_pgno))))
        return MDBX_CORRUPTED;
      gc += MDBX_PNL_GETSIZE(pnl);
    }
    if (unlikely(rc != MDBX_NOTFOUND))
      return rc;

    /* Substract GC-pages from mt_next_pgno to find the new mt_next_pgno. */
    meta->mm_geo.next = read_txn->mt_next_pgno - gc;
    /* Set with current main DB */
    meta->mm_dbs[MAIN_DBI] = read_txn->mt_dbs[MAIN_DBI];

    mdbx_compacting_ctx ctx;
    memset(&ctx, 0, sizeof(ctx));
    rc = osal_condpair_init(&ctx.mc_condpair);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;

    memset(data_buffer, 0, 2 * (size_t)MDBX_ENVCOPY_WRITEBUF);
    ctx.mc_wbuf[0] = data_buffer;
    ctx.mc_wbuf[1] = data_buffer + (size_t)MDBX_ENVCOPY_WRITEBUF;
    ctx.mc_next_pgno = NUM_METAS;
    ctx.mc_env = env;
    ctx.mc_fd = fd;
    ctx.mc_txn = read_txn;

    osal_thread_t thread;
    int thread_err = osal_thread_create(&thread, compacting_write_thread, &ctx);
    if (likely(thread_err == MDBX_SUCCESS)) {
      if (dest_is_pipe) {
        if (!meta->mm_dbs[MAIN_DBI].md_mod_txnid)
          meta->mm_dbs[MAIN_DBI].md_mod_txnid = read_txn->mt_txnid;
        compacting_fixup_meta(env, meta);
        rc = osal_write(fd, buffer, meta_bytes);
      }
      if (likely(rc == MDBX_SUCCESS))
        rc = compacting_walk_sdb(&ctx, &meta->mm_dbs[MAIN_DBI]);
      if (ctx.mc_wlen[ctx.mc_head & 1])
        /* toggle to flush non-empty buffers */
        compacting_toggle_write_buffers(&ctx);

      if (likely(rc == MDBX_SUCCESS) &&
          unlikely(meta->mm_geo.next != ctx.mc_next_pgno)) {
        if (ctx.mc_next_pgno > meta->mm_geo.next) {
          ERROR("the source DB %s: post-compactification used pages %" PRIaPGNO
                " %c expected %" PRIaPGNO,
                "has double-used pages or other corruption", ctx.mc_next_pgno,
                '>', meta->mm_geo.next);
          rc = MDBX_CORRUPTED; /* corrupted DB */
        }
        if (ctx.mc_next_pgno < meta->mm_geo.next) {
          WARNING(
              "the source DB %s: post-compactification used pages %" PRIaPGNO
              " %c expected %" PRIaPGNO,
              "has page leak(s)", ctx.mc_next_pgno, '<', meta->mm_geo.next);
          if (dest_is_pipe)
            /* the root within already written meta-pages is wrong */
            rc = MDBX_CORRUPTED;
        }
        /* fixup meta */
        meta->mm_geo.next = ctx.mc_next_pgno;
      }

      /* toggle with empty buffers to exit thread's loop */
      eASSERT(env, (ctx.mc_wlen[ctx.mc_head & 1]) == 0);
      compacting_toggle_write_buffers(&ctx);
      thread_err = osal_thread_join(thread);
      eASSERT(env, (ctx.mc_tail == ctx.mc_head &&
                    ctx.mc_wlen[ctx.mc_head & 1] == 0) ||
                       ctx.mc_error);
      osal_condpair_destroy(&ctx.mc_condpair);
    }
    if (unlikely(thread_err != MDBX_SUCCESS))
      return thread_err;
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    if (unlikely(ctx.mc_error != MDBX_SUCCESS))
      return ctx.mc_error;
    if (!dest_is_pipe)
      compacting_fixup_meta(env, meta);
  }

  /* Extend file if required */
  if (meta->mm_geo.now != meta->mm_geo.next) {
    const size_t whole_size = pgno2bytes(env, meta->mm_geo.now);
    if (!dest_is_pipe)
      return osal_ftruncate(fd, whole_size);

    const size_t used_size = pgno2bytes(env, meta->mm_geo.next);
    memset(data_buffer, 0, (size_t)MDBX_ENVCOPY_WRITEBUF);
    for (size_t offset = used_size; offset < whole_size;) {
      const size_t chunk = ((size_t)MDBX_ENVCOPY_WRITEBUF < whole_size - offset)
                               ? (size_t)MDBX_ENVCOPY_WRITEBUF
                               : whole_size - offset;
      int rc = osal_write(fd, data_buffer, chunk);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
      offset += chunk;
    }
  }
  return MDBX_SUCCESS;
}

/* Copy environment as-is. */
__cold static int env_copy_asis(MDBX_env *env, MDBX_txn *read_txn,
                                mdbx_filehandle_t fd, uint8_t *buffer,
                                const bool dest_is_pipe,
                                const MDBX_copy_flags_t flags) {
  /* We must start the actual read txn after blocking writers */
  int rc = txn_end(read_txn, MDBX_END_RESET_TMP);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  /* Temporarily block writers until we snapshot the meta pages */
  rc = mdbx_txn_lock(env, false);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  rc = txn_renew(read_txn, MDBX_TXN_RDONLY);
  if (unlikely(rc != MDBX_SUCCESS)) {
    mdbx_txn_unlock(env);
    return rc;
  }

  jitter4testing(false);
  const size_t meta_bytes = pgno2bytes(env, NUM_METAS);
  const meta_troika_t troika = meta_tap(env);
  /* Make a snapshot of meta-pages,
   * but writing ones after the data was flushed */
  memcpy(buffer, env->me_map, meta_bytes);
  MDBX_meta *const headcopy = /* LY: get pointer to the snapshot copy */
      ptr_disp(buffer, ptr_dist(meta_recent(env, &troika).ptr_c, env->me_map));
  mdbx_txn_unlock(env);

  if (flags & MDBX_CP_FORCE_DYNAMIC_SIZE)
    meta_make_sizeable(headcopy);
  /* Update signature to steady */
  unaligned_poke_u64(4, headcopy->mm_sign, meta_sign(headcopy));

  /* Copy the data */
  const size_t whole_size = pgno_align2os_bytes(env, read_txn->mt_end_pgno);
  const size_t used_size = pgno2bytes(env, read_txn->mt_next_pgno);
  jitter4testing(false);

  if (dest_is_pipe)
    rc = osal_write(fd, buffer, meta_bytes);

  uint8_t *const data_buffer =
      buffer + ceil_powerof2(meta_bytes, env->me_os_psize);
#if MDBX_USE_COPYFILERANGE
  static bool copyfilerange_unavailable;
  bool not_the_same_filesystem = false;
  struct statfs statfs_info;
  if (fstatfs(fd, &statfs_info) ||
      statfs_info.f_type == /* ECRYPTFS_SUPER_MAGIC */ 0xf15f)
    /* avoid use copyfilerange_unavailable() to ecryptfs due bugs */
    not_the_same_filesystem = true;
#endif /* MDBX_USE_COPYFILERANGE */
  for (size_t offset = meta_bytes; rc == MDBX_SUCCESS && offset < used_size;) {
#if MDBX_USE_SENDFILE
    static bool sendfile_unavailable;
    if (dest_is_pipe && likely(!sendfile_unavailable)) {
      off_t in_offset = offset;
      const ssize_t written =
          sendfile(fd, env->me_lazy_fd, &in_offset, used_size - offset);
      if (likely(written > 0)) {
        offset = in_offset;
        continue;
      }
      rc = MDBX_ENODATA;
      if (written == 0 || ignore_enosys(rc = errno) != MDBX_RESULT_TRUE)
        break;
      sendfile_unavailable = true;
    }
#endif /* MDBX_USE_SENDFILE */

#if MDBX_USE_COPYFILERANGE
    if (!dest_is_pipe && !not_the_same_filesystem &&
        likely(!copyfilerange_unavailable)) {
      off_t in_offset = offset, out_offset = offset;
      ssize_t bytes_copied = copy_file_range(
          env->me_lazy_fd, &in_offset, fd, &out_offset, used_size - offset, 0);
      if (likely(bytes_copied > 0)) {
        offset = in_offset;
        continue;
      }
      rc = MDBX_ENODATA;
      if (bytes_copied == 0)
        break;
      rc = errno;
      if (rc == EXDEV || rc == /* workaround for ecryptfs bug(s),
                                  maybe useful for others FS */
                             EINVAL)
        not_the_same_filesystem = true;
      else if (ignore_enosys(rc) == MDBX_RESULT_TRUE)
        copyfilerange_unavailable = true;
      else
        break;
    }
#endif /* MDBX_USE_COPYFILERANGE */

    /* fallback to portable */
    const size_t chunk = ((size_t)MDBX_ENVCOPY_WRITEBUF < used_size - offset)
                             ? (size_t)MDBX_ENVCOPY_WRITEBUF
                             : used_size - offset;
    /* copy to avoid EFAULT in case swapped-out */
    memcpy(data_buffer, ptr_disp(env->me_map, offset), chunk);
    rc = osal_write(fd, data_buffer, chunk);
    offset += chunk;
  }

  /* Extend file if required */
  if (likely(rc == MDBX_SUCCESS) && whole_size != used_size) {
    if (!dest_is_pipe)
      rc = osal_ftruncate(fd, whole_size);
    else {
      memset(data_buffer, 0, (size_t)MDBX_ENVCOPY_WRITEBUF);
      for (size_t offset = used_size;
           rc == MDBX_SUCCESS && offset < whole_size;) {
        const size_t chunk =
            ((size_t)MDBX_ENVCOPY_WRITEBUF < whole_size - offset)
                ? (size_t)MDBX_ENVCOPY_WRITEBUF
                : whole_size - offset;
        rc = osal_write(fd, data_buffer, chunk);
        offset += chunk;
      }
    }
  }

  return rc;
}

__cold int mdbx_env_copy2fd(MDBX_env *env, mdbx_filehandle_t fd,
                            MDBX_copy_flags_t flags) {
  int rc = check_env(env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  const int dest_is_pipe = osal_is_pipe(fd);
  if (MDBX_IS_ERROR(dest_is_pipe))
    return dest_is_pipe;

  if (!dest_is_pipe) {
    rc = osal_fseek(fd, 0);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }

  const size_t buffer_size =
      pgno_align2os_bytes(env, NUM_METAS) +
      ceil_powerof2(((flags & MDBX_CP_COMPACT)
                         ? 2 * (size_t)MDBX_ENVCOPY_WRITEBUF
                         : (size_t)MDBX_ENVCOPY_WRITEBUF),
                    env->me_os_psize);

  uint8_t *buffer = NULL;
  rc = osal_memalign_alloc(env->me_os_psize, buffer_size, (void **)&buffer);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  MDBX_txn *read_txn = NULL;
  /* Do the lock/unlock of the reader mutex before starting the
   * write txn. Otherwise other read txns could block writers. */
  rc = mdbx_txn_begin(env, NULL, MDBX_TXN_RDONLY, &read_txn);
  if (unlikely(rc != MDBX_SUCCESS)) {
    osal_memalign_free(buffer);
    return rc;
  }

  if (!dest_is_pipe) {
    /* Firstly write a stub to meta-pages.
     * Now we sure to incomplete copy will not be used. */
    memset(buffer, -1, pgno2bytes(env, NUM_METAS));
    rc = osal_write(fd, buffer, pgno2bytes(env, NUM_METAS));
  }

  if (likely(rc == MDBX_SUCCESS)) {
    memset(buffer, 0, pgno2bytes(env, NUM_METAS));
    rc = ((flags & MDBX_CP_COMPACT) ? env_compact : env_copy_asis)(
        env, read_txn, fd, buffer, dest_is_pipe, flags);
  }
  mdbx_txn_abort(read_txn);

  if (!dest_is_pipe) {
    if (likely(rc == MDBX_SUCCESS))
      rc = osal_fsync(fd, MDBX_SYNC_DATA | MDBX_SYNC_SIZE);

    /* Write actual meta */
    if (likely(rc == MDBX_SUCCESS))
      rc = osal_pwrite(fd, buffer, pgno2bytes(env, NUM_METAS), 0);

    if (likely(rc == MDBX_SUCCESS))
      rc = osal_fsync(fd, MDBX_SYNC_DATA | MDBX_SYNC_IODQ);
  }

  osal_memalign_free(buffer);
  return rc;
}

__cold int mdbx_env_copy(MDBX_env *env, const char *dest_path,
                         MDBX_copy_flags_t flags) {
#if defined(_WIN32) || defined(_WIN64)
  wchar_t *dest_pathW = nullptr;
  int rc = osal_mb2w(dest_path, &dest_pathW);
  if (likely(rc == MDBX_SUCCESS)) {
    rc = mdbx_env_copyW(env, dest_pathW, flags);
    osal_free(dest_pathW);
  }
  return rc;
}

__cold int mdbx_env_copyW(MDBX_env *env, const wchar_t *dest_path,
                          MDBX_copy_flags_t flags) {
#endif /* Windows */

  int rc = check_env(env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!dest_path))
    return MDBX_EINVAL;

  /* The destination path must exist, but the destination file must not.
   * We don't want the OS to cache the writes, since the source data is
   * already in the OS cache. */
  mdbx_filehandle_t newfd;
  rc = osal_openfile(MDBX_OPEN_COPY, env, dest_path, &newfd,
#if defined(_WIN32) || defined(_WIN64)
                     (mdbx_mode_t)-1
#else
                     S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP
#endif
  );

#if defined(_WIN32) || defined(_WIN64)
  /* no locking required since the file opened with ShareMode == 0 */
#else
  if (rc == MDBX_SUCCESS) {
    MDBX_STRUCT_FLOCK lock_op;
    memset(&lock_op, 0, sizeof(lock_op));
    lock_op.l_type = F_WRLCK;
    lock_op.l_whence = SEEK_SET;
    lock_op.l_start = 0;
    lock_op.l_len = OFF_T_MAX;
    if (MDBX_FCNTL(newfd, MDBX_F_SETLK, &lock_op)
#if (defined(__linux__) || defined(__gnu_linux__)) && defined(LOCK_EX) &&      \
    (!defined(__ANDROID_API__) || __ANDROID_API__ >= 24)
        || flock(newfd, LOCK_EX | LOCK_NB)
#endif /* Linux */
    )
      rc = errno;
  }
#endif /* Windows / POSIX */

  if (rc == MDBX_SUCCESS)
    rc = mdbx_env_copy2fd(env, newfd, flags);

  if (newfd != INVALID_HANDLE_VALUE) {
    int err = osal_closefile(newfd);
    if (rc == MDBX_SUCCESS && err != rc)
      rc = err;
    if (rc != MDBX_SUCCESS)
      (void)osal_removefile(dest_path);
  }

  return rc;
}

/******************************************************************************/

__cold int mdbx_env_set_flags(MDBX_env *env, MDBX_env_flags_t flags,
                              bool onoff) {
  int rc = check_env(env, false);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(flags &
               ((env->me_flags & MDBX_ENV_ACTIVE) ? ~ENV_CHANGEABLE_FLAGS
                                                  : ~ENV_USABLE_FLAGS)))
    return MDBX_EPERM;

  if (unlikely(env->me_flags & MDBX_RDONLY))
    return MDBX_EACCESS;

  if ((env->me_flags & MDBX_ENV_ACTIVE) &&
      unlikely(env->me_txn0->mt_owner == osal_thread_self()))
    return MDBX_BUSY;

  const bool lock_needed = (env->me_flags & MDBX_ENV_ACTIVE) &&
                           env->me_txn0->mt_owner != osal_thread_self();
  bool should_unlock = false;
  if (lock_needed) {
    rc = mdbx_txn_lock(env, false);
    if (unlikely(rc))
      return rc;
    should_unlock = true;
  }

  if (onoff)
    env->me_flags = merge_sync_flags(env->me_flags, flags);
  else
    env->me_flags &= ~flags;

  if (should_unlock)
    mdbx_txn_unlock(env);
  return MDBX_SUCCESS;
}

__cold int mdbx_env_get_flags(const MDBX_env *env, unsigned *arg) {
  int rc = check_env(env, false);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!arg))
    return MDBX_EINVAL;

  *arg = env->me_flags & ENV_USABLE_FLAGS;
  return MDBX_SUCCESS;
}

__cold int mdbx_env_set_userctx(MDBX_env *env, void *ctx) {
  int rc = check_env(env, false);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  env->me_userctx = ctx;
  return MDBX_SUCCESS;
}

__cold void *mdbx_env_get_userctx(const MDBX_env *env) {
  return env ? env->me_userctx : NULL;
}

__cold int mdbx_env_set_assert(MDBX_env *env, MDBX_assert_func *func) {
  int rc = check_env(env, false);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

#if MDBX_DEBUG
  env->me_assert_func = func;
  return MDBX_SUCCESS;
#else
  (void)func;
  return MDBX_ENOSYS;
#endif
}

#if defined(_WIN32) || defined(_WIN64)
__cold int mdbx_env_get_pathW(const MDBX_env *env, const wchar_t **arg) {
  int rc = check_env(env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!arg))
    return MDBX_EINVAL;

  *arg = env->me_pathname;
  return MDBX_SUCCESS;
}
#endif /* Windows */

__cold int mdbx_env_get_path(const MDBX_env *env, const char **arg) {
  int rc = check_env(env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!arg))
    return MDBX_EINVAL;

#if defined(_WIN32) || defined(_WIN64)
  if (!env->me_pathname_char) {
    *arg = nullptr;
    DWORD flags = /* WC_ERR_INVALID_CHARS */ 0x80;
    size_t mb_len = WideCharToMultiByte(CP_THREAD_ACP, flags, env->me_pathname,
                                        -1, nullptr, 0, nullptr, nullptr);
    rc = mb_len ? MDBX_SUCCESS : (int)GetLastError();
    if (rc == ERROR_INVALID_FLAGS) {
      mb_len = WideCharToMultiByte(CP_THREAD_ACP, flags = 0, env->me_pathname,
                                   -1, nullptr, 0, nullptr, nullptr);
      rc = mb_len ? MDBX_SUCCESS : (int)GetLastError();
    }
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;

    char *const mb_pathname = osal_malloc(mb_len);
    if (!mb_pathname)
      return MDBX_ENOMEM;
    if (mb_len != (size_t)WideCharToMultiByte(CP_THREAD_ACP, flags,
                                              env->me_pathname, -1, mb_pathname,
                                              (int)mb_len, nullptr, nullptr)) {
      rc = (int)GetLastError();
      osal_free(mb_pathname);
      return rc;
    }
    if (env->me_pathname_char ||
        InterlockedCompareExchangePointer(
            (PVOID volatile *)&env->me_pathname_char, mb_pathname, nullptr))
      osal_free(mb_pathname);
  }
  *arg = env->me_pathname_char;
#else
  *arg = env->me_pathname;
#endif /* Windows */
  return MDBX_SUCCESS;
}

__cold int mdbx_env_get_fd(const MDBX_env *env, mdbx_filehandle_t *arg) {
  int rc = check_env(env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!arg))
    return MDBX_EINVAL;

  *arg = env->me_lazy_fd;
  return MDBX_SUCCESS;
}

static void stat_get(const MDBX_db *db, MDBX_stat *st, size_t bytes) {
  st->ms_depth = db->md_depth;
  st->ms_branch_pages = db->md_branch_pages;
  st->ms_leaf_pages = db->md_leaf_pages;
  st->ms_overflow_pages = db->md_overflow_pages;
  st->ms_entries = db->md_entries;
  if (likely(bytes >=
             offsetof(MDBX_stat, ms_mod_txnid) + sizeof(st->ms_mod_txnid)))
    st->ms_mod_txnid = db->md_mod_txnid;
}

static void stat_add(const MDBX_db *db, MDBX_stat *const st,
                     const size_t bytes) {
  st->ms_depth += db->md_depth;
  st->ms_branch_pages += db->md_branch_pages;
  st->ms_leaf_pages += db->md_leaf_pages;
  st->ms_overflow_pages += db->md_overflow_pages;
  st->ms_entries += db->md_entries;
  if (likely(bytes >=
             offsetof(MDBX_stat, ms_mod_txnid) + sizeof(st->ms_mod_txnid)))
    st->ms_mod_txnid = (st->ms_mod_txnid > db->md_mod_txnid) ? st->ms_mod_txnid
                                                             : db->md_mod_txnid;
}

__cold static int stat_acc(const MDBX_txn *txn, MDBX_stat *st, size_t bytes) {
  int err = check_txn(txn, MDBX_TXN_BLOCKED);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  st->ms_psize = txn->mt_env->me_psize;
#if 1
  /* assuming GC is internal and not subject for accounting */
  stat_get(&txn->mt_dbs[MAIN_DBI], st, bytes);
#else
  stat_get(&txn->mt_dbs[FREE_DBI], st, bytes);
  stat_add(&txn->mt_dbs[MAIN_DBI], st, bytes);
#endif

  /* account opened named subDBs */
  for (MDBX_dbi dbi = CORE_DBS; dbi < txn->mt_numdbs; dbi++)
    if ((txn->mt_dbistate[dbi] & (DBI_VALID | DBI_STALE)) == DBI_VALID)
      stat_add(txn->mt_dbs + dbi, st, bytes);

  if (!(txn->mt_dbs[MAIN_DBI].md_flags & (MDBX_DUPSORT | MDBX_INTEGERKEY)) &&
      txn->mt_dbs[MAIN_DBI].md_entries /* TODO: use `md_subs` field */) {
    MDBX_cursor_couple cx;
    err = cursor_init(&cx.outer, (MDBX_txn *)txn, MAIN_DBI);
    if (unlikely(err != MDBX_SUCCESS))
      return err;

    /* scan and account not opened named subDBs */
    err = page_search(&cx.outer, NULL, MDBX_PS_FIRST);
    while (err == MDBX_SUCCESS) {
      const MDBX_page *mp = cx.outer.mc_pg[cx.outer.mc_top];
      for (size_t i = 0; i < page_numkeys(mp); i++) {
        const MDBX_node *node = page_node(mp, i);
        if (node_flags(node) != F_SUBDATA)
          continue;
        if (unlikely(node_ds(node) != sizeof(MDBX_db)))
          return MDBX_CORRUPTED;

        /* skip opened and already accounted */
        for (MDBX_dbi dbi = CORE_DBS; dbi < txn->mt_numdbs; dbi++)
          if ((txn->mt_dbistate[dbi] & (DBI_VALID | DBI_STALE)) == DBI_VALID &&
              node_ks(node) == txn->mt_dbxs[dbi].md_name.iov_len &&
              memcmp(node_key(node), txn->mt_dbxs[dbi].md_name.iov_base,
                     node_ks(node)) == 0) {
            node = NULL;
            break;
          }

        if (node) {
          MDBX_db db;
          memcpy(&db, node_data(node), sizeof(db));
          stat_add(&db, st, bytes);
        }
      }
      err = cursor_sibling(&cx.outer, SIBLING_RIGHT);
    }
    if (unlikely(err != MDBX_NOTFOUND))
      return err;
  }

  return MDBX_SUCCESS;
}

__cold int mdbx_env_stat_ex(const MDBX_env *env, const MDBX_txn *txn,
                            MDBX_stat *dest, size_t bytes) {
  if (unlikely(!dest))
    return MDBX_EINVAL;
  const size_t size_before_modtxnid = offsetof(MDBX_stat, ms_mod_txnid);
  if (unlikely(bytes != sizeof(MDBX_stat)) && bytes != size_before_modtxnid)
    return MDBX_EINVAL;

  if (likely(txn)) {
    if (env && unlikely(txn->mt_env != env))
      return MDBX_EINVAL;
    return stat_acc(txn, dest, bytes);
  }

  int err = check_env(env, true);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  if (env->me_txn0 && env->me_txn0->mt_owner == osal_thread_self())
    /* inside write-txn */
    return stat_acc(env->me_txn, dest, bytes);

  MDBX_txn *tmp_txn;
  err = mdbx_txn_begin((MDBX_env *)env, NULL, MDBX_TXN_RDONLY, &tmp_txn);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  const int rc = stat_acc(tmp_txn, dest, bytes);
  err = mdbx_txn_abort(tmp_txn);
  if (unlikely(err != MDBX_SUCCESS))
    return err;
  return rc;
}

__cold int mdbx_dbi_dupsort_depthmask(const MDBX_txn *txn, MDBX_dbi dbi,
                                      uint32_t *mask) {
  int rc = check_txn(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!mask))
    return MDBX_EINVAL;

  if (unlikely(!check_dbi(txn, dbi, DBI_VALID)))
    return MDBX_BAD_DBI;

  MDBX_cursor_couple cx;
  rc = cursor_init(&cx.outer, txn, dbi);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;
  if ((cx.outer.mc_db->md_flags & MDBX_DUPSORT) == 0)
    return MDBX_RESULT_TRUE;

  MDBX_val key, data;
  rc = cursor_first(&cx.outer, &key, &data);
  *mask = 0;
  while (rc == MDBX_SUCCESS) {
    const MDBX_node *node = page_node(cx.outer.mc_pg[cx.outer.mc_top],
                                      cx.outer.mc_ki[cx.outer.mc_top]);
    const MDBX_db *db = node_data(node);
    const unsigned flags = node_flags(node);
    switch (flags) {
    case F_BIGDATA:
    case 0:
      /* single-value entry, deep = 0 */
      *mask |= 1 << 0;
      break;
    case F_DUPDATA:
      /* single sub-page, deep = 1 */
      *mask |= 1 << 1;
      break;
    case F_DUPDATA | F_SUBDATA:
      /* sub-tree */
      *mask |= 1 << UNALIGNED_PEEK_16(db, MDBX_db, md_depth);
      break;
    default:
      ERROR("wrong node-flags %u", flags);
      return MDBX_CORRUPTED;
    }
    rc = cursor_next(&cx.outer, &key, &data, MDBX_NEXT_NODUP);
  }

  return (rc == MDBX_NOTFOUND) ? MDBX_SUCCESS : rc;
}

__cold static int fetch_envinfo_ex(const MDBX_env *env, const MDBX_txn *txn,
                                   MDBX_envinfo *arg, const size_t bytes) {

  const size_t size_before_bootid = offsetof(MDBX_envinfo, mi_bootid);
  const size_t size_before_pgop_stat = offsetof(MDBX_envinfo, mi_pgop_stat);

  /* is the environment open?
   * (https://libmdbx.dqdkfa.ru/dead-github/issues/171) */
  if (unlikely(!env->me_map)) {
    /* environment not yet opened */
#if 1
    /* default behavior: returns the available info but zeroed the rest */
    memset(arg, 0, bytes);
    arg->mi_geo.lower = env->me_dbgeo.lower;
    arg->mi_geo.upper = env->me_dbgeo.upper;
    arg->mi_geo.shrink = env->me_dbgeo.shrink;
    arg->mi_geo.grow = env->me_dbgeo.grow;
    arg->mi_geo.current = env->me_dbgeo.now;
    arg->mi_maxreaders = env->me_maxreaders;
    arg->mi_dxb_pagesize = env->me_psize;
    arg->mi_sys_pagesize = env->me_os_psize;
    if (likely(bytes > size_before_bootid)) {
      arg->mi_bootid.current.x = bootid.x;
      arg->mi_bootid.current.y = bootid.y;
    }
    return MDBX_SUCCESS;
#else
    /* some users may prefer this behavior: return appropriate error */
    return MDBX_EPERM;
#endif
  }

  const MDBX_meta *const meta0 = METAPAGE(env, 0);
  const MDBX_meta *const meta1 = METAPAGE(env, 1);
  const MDBX_meta *const meta2 = METAPAGE(env, 2);
  if (unlikely(env->me_flags & MDBX_FATAL_ERROR))
    return MDBX_PANIC;

  meta_troika_t holder;
  meta_troika_t const *troika;
  if (txn && !(txn->mt_flags & MDBX_TXN_RDONLY))
    troika = &txn->tw.troika;
  else {
    holder = meta_tap(env);
    troika = &holder;
  }

  const meta_ptr_t head = meta_recent(env, troika);
  arg->mi_recent_txnid = head.txnid;
  arg->mi_meta0_txnid = troika->txnid[0];
  arg->mi_meta0_sign = unaligned_peek_u64(4, meta0->mm_sign);
  arg->mi_meta1_txnid = troika->txnid[1];
  arg->mi_meta1_sign = unaligned_peek_u64(4, meta1->mm_sign);
  arg->mi_meta2_txnid = troika->txnid[2];
  arg->mi_meta2_sign = unaligned_peek_u64(4, meta2->mm_sign);
  if (likely(bytes > size_before_bootid)) {
    memcpy(&arg->mi_bootid.meta0, &meta0->mm_bootid, 16);
    memcpy(&arg->mi_bootid.meta1, &meta1->mm_bootid, 16);
    memcpy(&arg->mi_bootid.meta2, &meta2->mm_bootid, 16);
  }

  const volatile MDBX_meta *txn_meta = head.ptr_v;
  arg->mi_last_pgno = txn_meta->mm_geo.next - 1;
  arg->mi_geo.current = pgno2bytes(env, txn_meta->mm_geo.now);
  if (txn) {
    arg->mi_last_pgno = txn->mt_next_pgno - 1;
    arg->mi_geo.current = pgno2bytes(env, txn->mt_end_pgno);

    const txnid_t wanna_meta_txnid = (txn->mt_flags & MDBX_TXN_RDONLY)
                                         ? txn->mt_txnid
                                         : txn->mt_txnid - xMDBX_TXNID_STEP;
    txn_meta = (arg->mi_meta0_txnid == wanna_meta_txnid) ? meta0 : txn_meta;
    txn_meta = (arg->mi_meta1_txnid == wanna_meta_txnid) ? meta1 : txn_meta;
    txn_meta = (arg->mi_meta2_txnid == wanna_meta_txnid) ? meta2 : txn_meta;
  }
  arg->mi_geo.lower = pgno2bytes(env, txn_meta->mm_geo.lower);
  arg->mi_geo.upper = pgno2bytes(env, txn_meta->mm_geo.upper);
  arg->mi_geo.shrink = pgno2bytes(env, pv2pages(txn_meta->mm_geo.shrink_pv));
  arg->mi_geo.grow = pgno2bytes(env, pv2pages(txn_meta->mm_geo.grow_pv));
  const uint64_t unsynced_pages =
      atomic_load64(&env->me_lck->mti_unsynced_pages, mo_Relaxed) +
      (atomic_load32(&env->me_lck->mti_meta_sync_txnid, mo_Relaxed) !=
       (uint32_t)arg->mi_recent_txnid);

  arg->mi_mapsize = env->me_dxb_mmap.limit;

  const MDBX_lockinfo *const lck = env->me_lck;
  arg->mi_maxreaders = env->me_maxreaders;
  arg->mi_numreaders = env->me_lck_mmap.lck
                           ? atomic_load32(&lck->mti_numreaders, mo_Relaxed)
                           : INT32_MAX;
  arg->mi_dxb_pagesize = env->me_psize;
  arg->mi_sys_pagesize = env->me_os_psize;

  if (likely(bytes > size_before_bootid)) {
    arg->mi_unsync_volume = pgno2bytes(env, (size_t)unsynced_pages);
    const uint64_t monotime_now = osal_monotime();
    uint64_t ts = atomic_load64(&lck->mti_eoos_timestamp, mo_Relaxed);
    arg->mi_since_sync_seconds16dot16 =
        ts ? osal_monotime_to_16dot16_noUnderflow(monotime_now - ts) : 0;
    ts = atomic_load64(&lck->mti_reader_check_timestamp, mo_Relaxed);
    arg->mi_since_reader_check_seconds16dot16 =
        ts ? osal_monotime_to_16dot16_noUnderflow(monotime_now - ts) : 0;
    arg->mi_autosync_threshold = pgno2bytes(
        env, atomic_load32(&lck->mti_autosync_threshold, mo_Relaxed));
    arg->mi_autosync_period_seconds16dot16 =
        osal_monotime_to_16dot16_noUnderflow(
            atomic_load64(&lck->mti_autosync_period, mo_Relaxed));
    arg->mi_bootid.current.x = bootid.x;
    arg->mi_bootid.current.y = bootid.y;
    arg->mi_mode = env->me_lck_mmap.lck ? lck->mti_envmode.weak : env->me_flags;
  }

  if (likely(bytes > size_before_pgop_stat)) {
#if MDBX_ENABLE_PGOP_STAT
    arg->mi_pgop_stat.newly =
        atomic_load64(&lck->mti_pgop_stat.newly, mo_Relaxed);
    arg->mi_pgop_stat.cow = atomic_load64(&lck->mti_pgop_stat.cow, mo_Relaxed);
    arg->mi_pgop_stat.clone =
        atomic_load64(&lck->mti_pgop_stat.clone, mo_Relaxed);
    arg->mi_pgop_stat.split =
        atomic_load64(&lck->mti_pgop_stat.split, mo_Relaxed);
    arg->mi_pgop_stat.merge =
        atomic_load64(&lck->mti_pgop_stat.merge, mo_Relaxed);
    arg->mi_pgop_stat.spill =
        atomic_load64(&lck->mti_pgop_stat.spill, mo_Relaxed);
    arg->mi_pgop_stat.unspill =
        atomic_load64(&lck->mti_pgop_stat.unspill, mo_Relaxed);
    arg->mi_pgop_stat.wops =
        atomic_load64(&lck->mti_pgop_stat.wops, mo_Relaxed);
    arg->mi_pgop_stat.prefault =
        atomic_load64(&lck->mti_pgop_stat.prefault, mo_Relaxed);
    arg->mi_pgop_stat.mincore =
        atomic_load64(&lck->mti_pgop_stat.mincore, mo_Relaxed);
    arg->mi_pgop_stat.msync =
        atomic_load64(&lck->mti_pgop_stat.msync, mo_Relaxed);
    arg->mi_pgop_stat.fsync =
        atomic_load64(&lck->mti_pgop_stat.fsync, mo_Relaxed);
#else
    memset(&arg->mi_pgop_stat, 0, sizeof(arg->mi_pgop_stat));
#endif /* MDBX_ENABLE_PGOP_STAT*/
  }

  arg->mi_self_latter_reader_txnid = arg->mi_latter_reader_txnid =
      arg->mi_recent_txnid;
  if (env->me_lck_mmap.lck) {
    for (size_t i = 0; i < arg->mi_numreaders; ++i) {
      const uint32_t pid =
          atomic_load32(&lck->mti_readers[i].mr_pid, mo_AcquireRelease);
      if (pid) {
        const txnid_t txnid = safe64_read(&lck->mti_readers[i].mr_txnid);
        if (arg->mi_latter_reader_txnid > txnid)
          arg->mi_latter_reader_txnid = txnid;
        if (pid == env->me_pid && arg->mi_self_latter_reader_txnid > txnid)
          arg->mi_self_latter_reader_txnid = txnid;
      }
    }
  }

  osal_compiler_barrier();
  return MDBX_SUCCESS;
}

__cold int mdbx_env_info_ex(const MDBX_env *env, const MDBX_txn *txn,
                            MDBX_envinfo *arg, size_t bytes) {
  if (unlikely((env == NULL && txn == NULL) || arg == NULL))
    return MDBX_EINVAL;

  if (txn) {
    int err = check_txn(txn, MDBX_TXN_BLOCKED - MDBX_TXN_ERROR);
    if (unlikely(err != MDBX_SUCCESS))
      return err;
  }
  if (env) {
    int err = check_env(env, false);
    if (unlikely(err != MDBX_SUCCESS))
      return err;
    if (txn && unlikely(txn->mt_env != env))
      return MDBX_EINVAL;
  } else {
    env = txn->mt_env;
  }

  const size_t size_before_bootid = offsetof(MDBX_envinfo, mi_bootid);
  const size_t size_before_pgop_stat = offsetof(MDBX_envinfo, mi_pgop_stat);
  if (unlikely(bytes != sizeof(MDBX_envinfo)) && bytes != size_before_bootid &&
      bytes != size_before_pgop_stat)
    return MDBX_EINVAL;

  MDBX_envinfo snap;
  int rc = fetch_envinfo_ex(env, txn, &snap, sizeof(snap));
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  while (1) {
    rc = fetch_envinfo_ex(env, txn, arg, bytes);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    snap.mi_since_sync_seconds16dot16 = arg->mi_since_sync_seconds16dot16;
    snap.mi_since_reader_check_seconds16dot16 =
        arg->mi_since_reader_check_seconds16dot16;
    if (likely(memcmp(&snap, arg, bytes) == 0))
      return MDBX_SUCCESS;
    memcpy(&snap, arg, bytes);
  }
}

static __inline MDBX_cmp_func *get_default_keycmp(MDBX_db_flags_t flags) {
  return (flags & MDBX_REVERSEKEY)   ? cmp_reverse
         : (flags & MDBX_INTEGERKEY) ? cmp_int_align2
                                     : cmp_lexical;
}

static __inline MDBX_cmp_func *get_default_datacmp(MDBX_db_flags_t flags) {
  return !(flags & MDBX_DUPSORT)
             ? cmp_lenfast
             : ((flags & MDBX_INTEGERDUP)
                    ? cmp_int_unaligned
                    : ((flags & MDBX_REVERSEDUP) ? cmp_reverse : cmp_lexical));
}

static int dbi_bind(MDBX_txn *txn, const MDBX_dbi dbi, unsigned user_flags,
                    MDBX_cmp_func *keycmp, MDBX_cmp_func *datacmp) {
  /* Accepting only three cases:
   * 1) user_flags and both comparators are zero
   *    = assume that a by-default mode/flags is requested for reading;
   * 2) user_flags exactly the same
   *    = assume that the target mode/flags are requested properly;
   * 3) user_flags differs, but table is empty and MDBX_CREATE is provided
   *    = assume that a properly create request with custom flags;
   */
  if ((user_flags ^ txn->mt_dbs[dbi].md_flags) & DB_PERSISTENT_FLAGS) {
    /* flags are differs, check other conditions */
    if ((!user_flags && (!keycmp || keycmp == txn->mt_dbxs[dbi].md_cmp) &&
         (!datacmp || datacmp == txn->mt_dbxs[dbi].md_dcmp)) ||
        user_flags == MDBX_ACCEDE) {
      /* no comparators were provided and flags are zero,
       * seems that is case #1 above */
      user_flags = txn->mt_dbs[dbi].md_flags;
    } else if ((user_flags & MDBX_CREATE) && txn->mt_dbs[dbi].md_entries == 0) {
      if (txn->mt_flags & MDBX_TXN_RDONLY)
        return /* FIXME: return extended info */ MDBX_EACCESS;
      /* make sure flags changes get committed */
      txn->mt_dbs[dbi].md_flags = user_flags & DB_PERSISTENT_FLAGS;
      txn->mt_flags |= MDBX_TXN_DIRTY;
      /* обнуляем компараторы для установки в соответствии с флагами,
       * либо заданных пользователем */
      txn->mt_dbxs[dbi].md_cmp = nullptr;
      txn->mt_dbxs[dbi].md_dcmp = nullptr;
    } else {
      return /* FIXME: return extended info */ MDBX_INCOMPATIBLE;
    }
  }

  if (!keycmp)
    keycmp = txn->mt_dbxs[dbi].md_cmp ? txn->mt_dbxs[dbi].md_cmp
                                      : get_default_keycmp(user_flags);
  if (txn->mt_dbxs[dbi].md_cmp != keycmp) {
    if (txn->mt_dbxs[dbi].md_cmp)
      return MDBX_EINVAL;
    txn->mt_dbxs[dbi].md_cmp = keycmp;
  }

  if (!datacmp)
    datacmp = txn->mt_dbxs[dbi].md_dcmp ? txn->mt_dbxs[dbi].md_dcmp
                                        : get_default_datacmp(user_flags);
  if (txn->mt_dbxs[dbi].md_dcmp != datacmp) {
    if (txn->mt_dbxs[dbi].md_dcmp)
      return MDBX_EINVAL;
    txn->mt_dbxs[dbi].md_dcmp = datacmp;
  }

  return MDBX_SUCCESS;
}

static int dbi_open(MDBX_txn *txn, const MDBX_val *const table_name,
                    unsigned user_flags, MDBX_dbi *dbi, MDBX_cmp_func *keycmp,
                    MDBX_cmp_func *datacmp) {
  int rc = MDBX_EINVAL;
  if (unlikely(!dbi))
    return rc;

  void *clone = nullptr;
  bool locked = false;
  if (unlikely((user_flags & ~DB_USABLE_FLAGS) != 0)) {
  bailout:
    tASSERT(txn, MDBX_IS_ERROR(rc));
    *dbi = 0;
    if (locked)
      ENSURE(txn->mt_env,
             osal_fastmutex_release(&txn->mt_env->me_dbi_lock) == MDBX_SUCCESS);
    osal_free(clone);
    return rc;
  }

  rc = check_txn(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout;

  if ((user_flags & MDBX_CREATE) && unlikely(txn->mt_flags & MDBX_TXN_RDONLY)) {
    rc = MDBX_EACCESS;
    goto bailout;
  }

  switch (user_flags & (MDBX_INTEGERDUP | MDBX_DUPFIXED | MDBX_DUPSORT |
                        MDBX_REVERSEDUP | MDBX_ACCEDE)) {
  case MDBX_ACCEDE:
    if ((user_flags & MDBX_CREATE) == 0)
      break;
    __fallthrough /* fall through */;
  default:
    rc = MDBX_EINVAL;
    goto bailout;

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
  if (table_name == MDBX_PGWALK_MAIN ||
      table_name->iov_base == MDBX_PGWALK_MAIN) {
    rc = dbi_bind(txn, MAIN_DBI, user_flags, keycmp, datacmp);
    if (unlikely(rc != MDBX_SUCCESS))
      goto bailout;
    *dbi = MAIN_DBI;
    return rc;
  }
  if (table_name == MDBX_PGWALK_GC || table_name->iov_base == MDBX_PGWALK_GC) {
    rc = dbi_bind(txn, FREE_DBI, user_flags, keycmp, datacmp);
    if (unlikely(rc != MDBX_SUCCESS))
      goto bailout;
    *dbi = FREE_DBI;
    return rc;
  }
  if (table_name == MDBX_PGWALK_META ||
      table_name->iov_base == MDBX_PGWALK_META) {
    rc = MDBX_EINVAL;
    goto bailout;
  }

  MDBX_val key = *table_name;
  MDBX_env *const env = txn->mt_env;
  if (key.iov_len > env->me_leaf_nodemax - NODESIZE - sizeof(MDBX_db))
    return MDBX_EINVAL;

  /* Cannot mix named table(s) with DUPSORT flags */
  if (unlikely(txn->mt_dbs[MAIN_DBI].md_flags & MDBX_DUPSORT)) {
    if ((user_flags & MDBX_CREATE) == 0) {
      rc = MDBX_NOTFOUND;
      goto bailout;
    }
    if (txn->mt_dbs[MAIN_DBI].md_leaf_pages || txn->mt_dbxs[MAIN_DBI].md_cmp) {
      /* В MAIN_DBI есть записи либо она уже использовалась. */
      rc = MDBX_INCOMPATIBLE;
      goto bailout;
    }
    /* Пересоздаём MAIN_DBI если там пусто. */
    atomic_store32(&txn->mt_dbiseqs[MAIN_DBI], dbi_seq(env, MAIN_DBI),
                   mo_AcquireRelease);
    tASSERT(txn, txn->mt_dbs[MAIN_DBI].md_depth == 0 &&
                     txn->mt_dbs[MAIN_DBI].md_entries == 0 &&
                     txn->mt_dbs[MAIN_DBI].md_root == P_INVALID);
    txn->mt_dbs[MAIN_DBI].md_flags &= MDBX_REVERSEKEY | MDBX_INTEGERKEY;
    txn->mt_dbistate[MAIN_DBI] |= DBI_DIRTY;
    txn->mt_flags |= MDBX_TXN_DIRTY;
    txn->mt_dbxs[MAIN_DBI].md_cmp =
        get_default_keycmp(txn->mt_dbs[MAIN_DBI].md_flags);
    txn->mt_dbxs[MAIN_DBI].md_dcmp =
        get_default_datacmp(txn->mt_dbs[MAIN_DBI].md_flags);
  }

  tASSERT(txn, txn->mt_dbxs[MAIN_DBI].md_cmp);

  /* Is the DB already open? */
  MDBX_dbi scan, slot;
  for (slot = scan = txn->mt_numdbs; --scan >= CORE_DBS;) {
    if (!txn->mt_dbxs[scan].md_name.iov_base) {
      /* Remember this free slot */
      slot = scan;
      continue;
    }
    if (key.iov_len == txn->mt_dbxs[scan].md_name.iov_len &&
        !memcmp(key.iov_base, txn->mt_dbxs[scan].md_name.iov_base,
                key.iov_len)) {
      rc = dbi_bind(txn, scan, user_flags, keycmp, datacmp);
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;
      *dbi = scan;
      return rc;
    }
  }

  /* Fail, if no free slot and max hit */
  if (unlikely(slot >= env->me_maxdbs)) {
    rc = MDBX_DBS_FULL;
    goto bailout;
  }

  /* Find the DB info */
  MDBX_val data;
  MDBX_cursor_couple couple;
  rc = cursor_init(&couple.outer, txn, MAIN_DBI);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout;
  rc = cursor_set(&couple.outer, &key, &data, MDBX_SET).err;
  if (unlikely(rc != MDBX_SUCCESS)) {
    if (rc != MDBX_NOTFOUND || !(user_flags & MDBX_CREATE))
      goto bailout;
  } else {
    /* make sure this is actually a table */
    MDBX_node *node = page_node(couple.outer.mc_pg[couple.outer.mc_top],
                                couple.outer.mc_ki[couple.outer.mc_top]);
    if (unlikely((node_flags(node) & (F_DUPDATA | F_SUBDATA)) != F_SUBDATA)) {
      rc = MDBX_INCOMPATIBLE;
      goto bailout;
    }
    if (!MDBX_DISABLE_VALIDATION && unlikely(data.iov_len != sizeof(MDBX_db))) {
      rc = MDBX_CORRUPTED;
      goto bailout;
    }
  }

  if (rc != MDBX_SUCCESS && unlikely(txn->mt_flags & MDBX_TXN_RDONLY)) {
    rc = MDBX_EACCESS;
    goto bailout;
  }

  /* Done here so we cannot fail after creating a new DB */
  if (key.iov_len) {
    clone = osal_malloc(key.iov_len);
    if (unlikely(!clone)) {
      rc = MDBX_ENOMEM;
      goto bailout;
    }
    key.iov_base = memcpy(clone, key.iov_base, key.iov_len);
  } else
    key.iov_base = "";

  int err = osal_fastmutex_acquire(&env->me_dbi_lock);
  if (unlikely(err != MDBX_SUCCESS)) {
    rc = err;
    goto bailout;
  }
  locked = true;

  /* Import handles from env */
  dbi_import_locked(txn);

  /* Rescan after mutex acquisition & import handles */
  for (slot = scan = txn->mt_numdbs; --scan >= CORE_DBS;) {
    if (!txn->mt_dbxs[scan].md_name.iov_base) {
      /* Remember this free slot */
      slot = scan;
      continue;
    }
    if (key.iov_len == txn->mt_dbxs[scan].md_name.iov_len &&
        !memcmp(key.iov_base, txn->mt_dbxs[scan].md_name.iov_base,
                key.iov_len)) {
      rc = dbi_bind(txn, scan, user_flags, keycmp, datacmp);
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;
      slot = scan;
      goto done;
    }
  }

  if (unlikely(slot >= env->me_maxdbs)) {
    rc = MDBX_DBS_FULL;
    goto bailout;
  }

  unsigned dbiflags = DBI_FRESH | DBI_VALID | DBI_USRVALID;
  MDBX_db db_dummy;
  if (unlikely(rc)) {
    /* MDBX_NOTFOUND and MDBX_CREATE: Create new DB */
    tASSERT(txn, rc == MDBX_NOTFOUND);
    memset(&db_dummy, 0, sizeof(db_dummy));
    db_dummy.md_root = P_INVALID;
    db_dummy.md_mod_txnid = txn->mt_txnid;
    db_dummy.md_flags = user_flags & DB_PERSISTENT_FLAGS;
    data.iov_len = sizeof(db_dummy);
    data.iov_base = &db_dummy;
    WITH_CURSOR_TRACKING(
        couple.outer, rc = cursor_put_checklen(&couple.outer, &key, &data,
                                               F_SUBDATA | MDBX_NOOVERWRITE));
    if (unlikely(rc != MDBX_SUCCESS))
      goto bailout;

    dbiflags |= DBI_DIRTY | DBI_CREAT;
    txn->mt_flags |= MDBX_TXN_DIRTY;
    tASSERT(txn, (txn->mt_dbistate[MAIN_DBI] & DBI_DIRTY) != 0);
  }

  /* Got info, register DBI in this txn */
  memset(txn->mt_dbxs + slot, 0, sizeof(MDBX_dbx));
  memcpy(&txn->mt_dbs[slot], data.iov_base, sizeof(MDBX_db));
  env->me_dbflags[slot] = 0;
  rc = dbi_bind(txn, slot, user_flags, keycmp, datacmp);
  if (unlikely(rc != MDBX_SUCCESS)) {
    tASSERT(txn, (dbiflags & DBI_CREAT) == 0);
    goto bailout;
  }

  txn->mt_dbistate[slot] = (uint8_t)dbiflags;
  txn->mt_dbxs[slot].md_name = key;
  txn->mt_dbiseqs[slot].weak = env->me_dbiseqs[slot].weak = dbi_seq(env, slot);
  if (!(dbiflags & DBI_CREAT))
    env->me_dbflags[slot] = txn->mt_dbs[slot].md_flags | DB_VALID;
  if (txn->mt_numdbs == slot) {
    txn->mt_cursors[slot] = NULL;
    osal_compiler_barrier();
    txn->mt_numdbs = slot + 1;
  }
  if (env->me_numdbs <= slot) {
    osal_memory_fence(mo_AcquireRelease, true);
    env->me_numdbs = slot + 1;
  }

done:
  *dbi = slot;
  ENSURE(env, osal_fastmutex_release(&env->me_dbi_lock) == MDBX_SUCCESS);
  return MDBX_SUCCESS;
}

static int dbi_open_cstr(MDBX_txn *txn, const char *name_cstr,
                         MDBX_db_flags_t flags, MDBX_dbi *dbi,
                         MDBX_cmp_func *keycmp, MDBX_cmp_func *datacmp) {
  MDBX_val thunk, *name;
  if (name_cstr == MDBX_PGWALK_MAIN || name_cstr == MDBX_PGWALK_GC ||
      name_cstr == MDBX_PGWALK_META)
    name = (void *)name_cstr;
  else {
    thunk.iov_len = strlen(name_cstr);
    thunk.iov_base = (void *)name_cstr;
    name = &thunk;
  }
  return dbi_open(txn, name, flags, dbi, keycmp, datacmp);
}

int mdbx_dbi_open(MDBX_txn *txn, const char *name, MDBX_db_flags_t flags,
                  MDBX_dbi *dbi) {
  return dbi_open_cstr(txn, name, flags, dbi, nullptr, nullptr);
}

int mdbx_dbi_open2(MDBX_txn *txn, const MDBX_val *name, MDBX_db_flags_t flags,
                   MDBX_dbi *dbi) {
  return dbi_open(txn, name, flags, dbi, nullptr, nullptr);
}

int mdbx_dbi_open_ex(MDBX_txn *txn, const char *name, MDBX_db_flags_t flags,
                     MDBX_dbi *dbi, MDBX_cmp_func *keycmp,
                     MDBX_cmp_func *datacmp) {
  return dbi_open_cstr(txn, name, flags, dbi, keycmp, datacmp);
}

int mdbx_dbi_open_ex2(MDBX_txn *txn, const MDBX_val *name,
                      MDBX_db_flags_t flags, MDBX_dbi *dbi,
                      MDBX_cmp_func *keycmp, MDBX_cmp_func *datacmp) {
  return dbi_open(txn, name, flags, dbi, keycmp, datacmp);
}

__cold int mdbx_dbi_stat(const MDBX_txn *txn, MDBX_dbi dbi, MDBX_stat *dest,
                         size_t bytes) {
  int rc = check_txn(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!dest))
    return MDBX_EINVAL;

  if (unlikely(!check_dbi(txn, dbi, DBI_VALID)))
    return MDBX_BAD_DBI;

  const size_t size_before_modtxnid = offsetof(MDBX_stat, ms_mod_txnid);
  if (unlikely(bytes != sizeof(MDBX_stat)) && bytes != size_before_modtxnid)
    return MDBX_EINVAL;

  if (unlikely(txn->mt_flags & MDBX_TXN_BLOCKED))
    return MDBX_BAD_TXN;

  if (unlikely(txn->mt_dbistate[dbi] & DBI_STALE)) {
    rc = fetch_sdb((MDBX_txn *)txn, dbi);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }

  dest->ms_psize = txn->mt_env->me_psize;
  stat_get(&txn->mt_dbs[dbi], dest, bytes);
  return MDBX_SUCCESS;
}

static int dbi_close_locked(MDBX_env *env, MDBX_dbi dbi) {
  eASSERT(env, dbi >= CORE_DBS);
  if (unlikely(dbi >= env->me_numdbs))
    return MDBX_BAD_DBI;

  char *const ptr = env->me_dbxs[dbi].md_name.iov_base;
  /* If there was no name, this was already closed */
  if (unlikely(!ptr))
    return MDBX_BAD_DBI;

  env->me_dbflags[dbi] = 0;
  env->me_dbxs[dbi].md_name.iov_len = 0;
  osal_memory_fence(mo_AcquireRelease, true);
  env->me_dbxs[dbi].md_name.iov_base = NULL;
  osal_free(ptr);

  if (env->me_numdbs == dbi + 1) {
    size_t i = env->me_numdbs;
    do
      --i;
    while (i > CORE_DBS && !env->me_dbxs[i - 1].md_name.iov_base);
    env->me_numdbs = (MDBX_dbi)i;
  }

  return MDBX_SUCCESS;
}

int mdbx_dbi_close(MDBX_env *env, MDBX_dbi dbi) {
  int rc = check_env(env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(dbi < CORE_DBS))
    return (dbi == MAIN_DBI) ? MDBX_SUCCESS : MDBX_BAD_DBI;

  if (unlikely(dbi >= env->me_maxdbs))
    return MDBX_BAD_DBI;

  rc = osal_fastmutex_acquire(&env->me_dbi_lock);
  if (likely(rc == MDBX_SUCCESS)) {
  retry:
    rc = MDBX_BAD_DBI;
    if (dbi < env->me_maxdbs && (env->me_dbflags[dbi] & DB_VALID)) {
      const MDBX_txn *const hazard = env->me_txn;
      osal_compiler_barrier();
      if (env->me_txn0 && (env->me_txn0->mt_flags & MDBX_TXN_FINISHED) == 0) {
        if (env->me_txn0->mt_dbistate[dbi] & (DBI_DIRTY | DBI_CREAT))
          goto bailout_dirty_dbi;
        osal_memory_barrier();
        if (unlikely(hazard != env->me_txn))
          goto retry;
        if (hazard != env->me_txn0 && hazard &&
            (hazard->mt_flags & MDBX_TXN_FINISHED) == 0 &&
            hazard->mt_signature == MDBX_MT_SIGNATURE &&
            (hazard->mt_dbistate[dbi] & (DBI_DIRTY | DBI_CREAT)))
          goto bailout_dirty_dbi;
        osal_compiler_barrier();
        if (unlikely(hazard != env->me_txn))
          goto retry;
      }
      rc = dbi_close_locked(env, dbi);
    }
  bailout_dirty_dbi:
    ENSURE(env, osal_fastmutex_release(&env->me_dbi_lock) == MDBX_SUCCESS);
  }
  return rc;
}

int mdbx_dbi_flags_ex(const MDBX_txn *txn, MDBX_dbi dbi, unsigned *flags,
                      unsigned *state) {
  int rc = check_txn(txn, MDBX_TXN_BLOCKED - MDBX_TXN_ERROR);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!flags || !state))
    return MDBX_EINVAL;

  if (unlikely(!check_dbi(txn, dbi, DBI_VALID)))
    return MDBX_BAD_DBI;

  *flags = txn->mt_dbs[dbi].md_flags & DB_PERSISTENT_FLAGS;
  *state =
      txn->mt_dbistate[dbi] & (DBI_FRESH | DBI_CREAT | DBI_DIRTY | DBI_STALE);

  return MDBX_SUCCESS;
}

static int drop_tree(MDBX_cursor *mc, const bool may_have_subDBs) {
  int rc = page_search(mc, NULL, MDBX_PS_FIRST);
  if (likely(rc == MDBX_SUCCESS)) {
    MDBX_txn *txn = mc->mc_txn;

    /* DUPSORT sub-DBs have no ovpages/DBs. Omit scanning leaves.
     * This also avoids any P_LEAF2 pages, which have no nodes.
     * Also if the DB doesn't have sub-DBs and has no large/overflow
     * pages, omit scanning leaves. */
    if (!(may_have_subDBs | mc->mc_db->md_overflow_pages))
      cursor_pop(mc);

    rc = pnl_need(&txn->tw.retired_pages,
                  (size_t)mc->mc_db->md_branch_pages +
                      (size_t)mc->mc_db->md_leaf_pages +
                      (size_t)mc->mc_db->md_overflow_pages);
    if (unlikely(rc != MDBX_SUCCESS))
      goto bailout;

    MDBX_cursor mx;
    cursor_copy(mc, &mx);
    while (mc->mc_snum > 0) {
      MDBX_page *const mp = mc->mc_pg[mc->mc_top];
      const size_t nkeys = page_numkeys(mp);
      if (IS_LEAF(mp)) {
        cASSERT(mc, mc->mc_snum == mc->mc_db->md_depth);
        for (size_t i = 0; i < nkeys; i++) {
          MDBX_node *node = page_node(mp, i);
          if (node_flags(node) & F_BIGDATA) {
            rc = page_retire_ex(mc, node_largedata_pgno(node), nullptr, 0);
            if (unlikely(rc != MDBX_SUCCESS))
              goto bailout;
            if (!(may_have_subDBs | mc->mc_db->md_overflow_pages))
              goto pop;
          } else if (node_flags(node) & F_SUBDATA) {
            if (unlikely((node_flags(node) & F_DUPDATA) == 0)) {
              rc = /* disallowing implicit subDB deletion */ MDBX_INCOMPATIBLE;
              goto bailout;
            }
            rc = cursor_xinit1(mc, node, mp);
            if (unlikely(rc != MDBX_SUCCESS))
              goto bailout;
            rc = drop_tree(&mc->mc_xcursor->mx_cursor, false);
            if (unlikely(rc != MDBX_SUCCESS))
              goto bailout;
          }
        }
      } else {
        cASSERT(mc, mc->mc_snum < mc->mc_db->md_depth);
        mc->mc_checking |= CC_RETIRING;
        const unsigned pagetype = (IS_FROZEN(txn, mp) ? P_FROZEN : 0) +
                                  ((mc->mc_snum + 1 == mc->mc_db->md_depth)
                                       ? (mc->mc_checking & (P_LEAF | P_LEAF2))
                                       : P_BRANCH);
        for (size_t i = 0; i < nkeys; i++) {
          MDBX_node *node = page_node(mp, i);
          tASSERT(txn, (node_flags(node) &
                        (F_BIGDATA | F_SUBDATA | F_DUPDATA)) == 0);
          const pgno_t pgno = node_pgno(node);
          rc = page_retire_ex(mc, pgno, nullptr, pagetype);
          if (unlikely(rc != MDBX_SUCCESS))
            goto bailout;
        }
        mc->mc_checking -= CC_RETIRING;
      }
      if (!mc->mc_top)
        break;
      cASSERT(mc, nkeys > 0);
      mc->mc_ki[mc->mc_top] = (indx_t)nkeys;
      rc = cursor_sibling(mc, SIBLING_RIGHT);
      if (unlikely(rc != MDBX_SUCCESS)) {
        if (unlikely(rc != MDBX_NOTFOUND))
          goto bailout;
      /* no more siblings, go back to beginning
       * of previous level. */
      pop:
        cursor_pop(mc);
        mc->mc_ki[0] = 0;
        for (size_t i = 1; i < mc->mc_snum; i++) {
          mc->mc_ki[i] = 0;
          mc->mc_pg[i] = mx.mc_pg[i];
        }
      }
    }
    rc = page_retire(mc, mc->mc_pg[0]);
  bailout:
    if (unlikely(rc != MDBX_SUCCESS))
      txn->mt_flags |= MDBX_TXN_ERROR;
  } else if (rc == MDBX_NOTFOUND) {
    rc = MDBX_SUCCESS;
  }
  mc->mc_flags &= ~C_INITIALIZED;
  return rc;
}

int mdbx_drop(MDBX_txn *txn, MDBX_dbi dbi, bool del) {
  int rc = check_txn_rw(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  MDBX_cursor *mc;
  rc = mdbx_cursor_open(txn, dbi, &mc);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  rc = drop_tree(mc,
                 dbi == MAIN_DBI || (mc->mc_db->md_flags & MDBX_DUPSORT) != 0);
  /* Invalidate the dropped DB's cursors */
  for (MDBX_cursor *m2 = txn->mt_cursors[dbi]; m2; m2 = m2->mc_next)
    m2->mc_flags &= ~(C_INITIALIZED | C_EOF);
  if (unlikely(rc))
    goto bailout;

  /* Can't delete the main DB */
  if (del && dbi >= CORE_DBS) {
    rc = delete (txn, MAIN_DBI, &mc->mc_dbx->md_name, NULL, F_SUBDATA);
    if (likely(rc == MDBX_SUCCESS)) {
      tASSERT(txn, txn->mt_dbistate[MAIN_DBI] & DBI_DIRTY);
      tASSERT(txn, txn->mt_flags & MDBX_TXN_DIRTY);
      txn->mt_dbistate[dbi] = DBI_STALE;
      MDBX_env *env = txn->mt_env;
      rc = osal_fastmutex_acquire(&env->me_dbi_lock);
      if (unlikely(rc != MDBX_SUCCESS)) {
        txn->mt_flags |= MDBX_TXN_ERROR;
        goto bailout;
      }
      dbi_close_locked(env, dbi);
      ENSURE(env, osal_fastmutex_release(&env->me_dbi_lock) == MDBX_SUCCESS);
    } else {
      txn->mt_flags |= MDBX_TXN_ERROR;
    }
  } else {
    /* reset the DB record, mark it dirty */
    txn->mt_dbistate[dbi] |= DBI_DIRTY;
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

__cold int mdbx_reader_list(const MDBX_env *env, MDBX_reader_list_func *func,
                            void *ctx) {
  int rc = check_env(env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!func))
    return MDBX_EINVAL;

  rc = MDBX_RESULT_TRUE;
  int serial = 0;
  MDBX_lockinfo *const lck = env->me_lck_mmap.lck;
  if (likely(lck)) {
    const size_t snap_nreaders =
        atomic_load32(&lck->mti_numreaders, mo_AcquireRelease);
    for (size_t i = 0; i < snap_nreaders; i++) {
      const MDBX_reader *r = lck->mti_readers + i;
    retry_reader:;
      const uint32_t pid = atomic_load32(&r->mr_pid, mo_AcquireRelease);
      if (!pid)
        continue;
      txnid_t txnid = safe64_read(&r->mr_txnid);
      const uint64_t tid = atomic_load64(&r->mr_tid, mo_Relaxed);
      const pgno_t pages_used =
          atomic_load32(&r->mr_snapshot_pages_used, mo_Relaxed);
      const uint64_t reader_pages_retired =
          atomic_load64(&r->mr_snapshot_pages_retired, mo_Relaxed);
      if (unlikely(
              txnid != safe64_read(&r->mr_txnid) ||
              pid != atomic_load32(&r->mr_pid, mo_AcquireRelease) ||
              tid != atomic_load64(&r->mr_tid, mo_Relaxed) ||
              pages_used !=
                  atomic_load32(&r->mr_snapshot_pages_used, mo_Relaxed) ||
              reader_pages_retired !=
                  atomic_load64(&r->mr_snapshot_pages_retired, mo_Relaxed)))
        goto retry_reader;

      eASSERT(env, txnid > 0);
      if (txnid >= SAFE64_INVALID_THRESHOLD)
        txnid = 0;

      size_t bytes_used = 0;
      size_t bytes_retained = 0;
      uint64_t lag = 0;
      if (txnid) {
        meta_troika_t troika = meta_tap(env);
      retry_header:;
        const meta_ptr_t head = meta_recent(env, &troika);
        const uint64_t head_pages_retired =
            unaligned_peek_u64_volatile(4, head.ptr_v->mm_pages_retired);
        if (unlikely(meta_should_retry(env, &troika) ||
                     head_pages_retired !=
                         unaligned_peek_u64_volatile(
                             4, head.ptr_v->mm_pages_retired)))
          goto retry_header;

        lag = (head.txnid - txnid) / xMDBX_TXNID_STEP;
        bytes_used = pgno2bytes(env, pages_used);
        bytes_retained = (head_pages_retired > reader_pages_retired)
                             ? pgno2bytes(env, (pgno_t)(head_pages_retired -
                                                        reader_pages_retired))
                             : 0;
      }
      rc = func(ctx, ++serial, (unsigned)i, pid, (mdbx_tid_t)((intptr_t)tid),
                txnid, lag, bytes_used, bytes_retained);
      if (unlikely(rc != MDBX_SUCCESS))
        break;
    }
  }

  return rc;
}

/* Insert pid into list if not already present.
 * return -1 if already present. */
__cold static bool pid_insert(uint32_t *ids, uint32_t pid) {
  /* binary search of pid in list */
  size_t base = 0;
  size_t cursor = 1;
  int val = 0;
  size_t n = ids[0];

  while (n > 0) {
    size_t pivot = n >> 1;
    cursor = base + pivot + 1;
    val = pid - ids[cursor];

    if (val < 0) {
      n = pivot;
    } else if (val > 0) {
      base = cursor;
      n -= pivot + 1;
    } else {
      /* found, so it's a duplicate */
      return false;
    }
  }

  if (val > 0)
    ++cursor;

  ids[0]++;
  for (n = ids[0]; n > cursor; n--)
    ids[n] = ids[n - 1];
  ids[n] = pid;
  return true;
}

__cold int mdbx_reader_check(MDBX_env *env, int *dead) {
  if (dead)
    *dead = 0;
  return cleanup_dead_readers(env, false, dead);
}

/* Return:
 *  MDBX_RESULT_TRUE - done and mutex recovered
 *  MDBX_SUCCESS     - done
 *  Otherwise errcode. */
__cold MDBX_INTERNAL_FUNC int cleanup_dead_readers(MDBX_env *env,
                                                   int rdt_locked, int *dead) {
  int rc = check_env(env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  eASSERT(env, rdt_locked >= 0);
  MDBX_lockinfo *const lck = env->me_lck_mmap.lck;
  if (unlikely(lck == NULL)) {
    /* exclusive mode */
    if (dead)
      *dead = 0;
    return MDBX_SUCCESS;
  }

  const size_t snap_nreaders =
      atomic_load32(&lck->mti_numreaders, mo_AcquireRelease);
  uint32_t pidsbuf_onstask[142];
  uint32_t *const pids =
      (snap_nreaders < ARRAY_LENGTH(pidsbuf_onstask))
          ? pidsbuf_onstask
          : osal_malloc((snap_nreaders + 1) * sizeof(uint32_t));
  if (unlikely(!pids))
    return MDBX_ENOMEM;

  pids[0] = 0;
  int count = 0;
  for (size_t i = 0; i < snap_nreaders; i++) {
    const uint32_t pid =
        atomic_load32(&lck->mti_readers[i].mr_pid, mo_AcquireRelease);
    if (pid == 0)
      continue /* skip empty */;
    if (pid == env->me_pid)
      continue /* skip self */;
    if (!pid_insert(pids, pid))
      continue /* such pid already processed */;

    int err = osal_rpid_check(env, pid);
    if (err == MDBX_RESULT_TRUE)
      continue /* reader is live */;

    if (err != MDBX_SUCCESS) {
      rc = err;
      break /* osal_rpid_check() failed */;
    }

    /* stale reader found */
    if (!rdt_locked) {
      err = osal_rdt_lock(env);
      if (MDBX_IS_ERROR(err)) {
        rc = err;
        break;
      }

      rdt_locked = -1;
      if (err == MDBX_RESULT_TRUE) {
        /* mutex recovered, the mdbx_ipclock_failed() checked all readers */
        rc = MDBX_RESULT_TRUE;
        break;
      }

      /* a other process may have clean and reused slot, recheck */
      if (lck->mti_readers[i].mr_pid.weak != pid)
        continue;

      err = osal_rpid_check(env, pid);
      if (MDBX_IS_ERROR(err)) {
        rc = err;
        break;
      }

      if (err != MDBX_SUCCESS)
        continue /* the race with other process, slot reused */;
    }

    /* clean it */
    for (size_t j = i; j < snap_nreaders; j++) {
      if (lck->mti_readers[j].mr_pid.weak == pid) {
        DEBUG("clear stale reader pid %" PRIuPTR " txn %" PRIaTXN, (size_t)pid,
              lck->mti_readers[j].mr_txnid.weak);
        atomic_store32(&lck->mti_readers[j].mr_pid, 0, mo_Relaxed);
        atomic_store32(&lck->mti_readers_refresh_flag, true, mo_AcquireRelease);
        count++;
      }
    }
  }

  if (likely(!MDBX_IS_ERROR(rc)))
    atomic_store64(&lck->mti_reader_check_timestamp, osal_monotime(),
                   mo_Relaxed);

  if (rdt_locked < 0)
    osal_rdt_unlock(env);

  if (pids != pidsbuf_onstask)
    osal_free(pids);

  if (dead)
    *dead = count;
  return rc;
}

__cold int mdbx_setup_debug(MDBX_log_level_t level, MDBX_debug_flags_t flags,
                            MDBX_debug_func *logger) {
  const int rc = runtime_flags | (loglevel << 16);

  if (level != MDBX_LOG_DONTCHANGE)
    loglevel = (uint8_t)level;

  if (flags != MDBX_DBG_DONTCHANGE) {
    flags &=
#if MDBX_DEBUG
        MDBX_DBG_ASSERT | MDBX_DBG_AUDIT | MDBX_DBG_JITTER |
#endif
        MDBX_DBG_DUMP | MDBX_DBG_LEGACY_MULTIOPEN | MDBX_DBG_LEGACY_OVERLAP |
        MDBX_DBG_DONT_UPGRADE;
    runtime_flags = (uint8_t)flags;
  }

  if (logger != MDBX_LOGGER_DONTCHANGE)
    debug_logger = logger;
  return rc;
}

__cold static txnid_t kick_longlived_readers(MDBX_env *env,
                                             const txnid_t laggard) {
  DEBUG("DB size maxed out by reading #%" PRIaTXN, laggard);
  osal_memory_fence(mo_AcquireRelease, false);
  MDBX_hsr_func *const callback = env->me_hsr_callback;
  txnid_t oldest = 0;
  bool notify_eof_of_loop = false;
  int retry = 0;
  do {
    const txnid_t steady =
        env->me_txn->tw.troika.txnid[env->me_txn->tw.troika.prefer_steady];
    env->me_lck->mti_readers_refresh_flag.weak = /* force refresh */ true;
    oldest = find_oldest_reader(env, steady);
    eASSERT(env, oldest < env->me_txn0->mt_txnid);
    eASSERT(env, oldest >= laggard);
    eASSERT(env, oldest >= env->me_lck->mti_oldest_reader.weak);

    MDBX_lockinfo *const lck = env->me_lck_mmap.lck;
    if (oldest == steady || oldest > laggard || /* without-LCK mode */ !lck)
      break;

    if (MDBX_IS_ERROR(cleanup_dead_readers(env, false, NULL)))
      break;

    if (!callback)
      break;

    MDBX_reader *stucked = nullptr;
    uint64_t hold_retired = 0;
    for (size_t i = 0; i < lck->mti_numreaders.weak; ++i) {
      const uint64_t snap_retired = atomic_load64(
          &lck->mti_readers[i].mr_snapshot_pages_retired, mo_Relaxed);
      const txnid_t rtxn = safe64_read(&lck->mti_readers[i].mr_txnid);
      if (rtxn == laggard &&
          atomic_load32(&lck->mti_readers[i].mr_pid, mo_AcquireRelease)) {
        hold_retired = snap_retired;
        stucked = &lck->mti_readers[i];
      }
    }

    if (!stucked)
      break;

    uint32_t pid = atomic_load32(&stucked->mr_pid, mo_AcquireRelease);
    uint64_t tid = atomic_load64(&stucked->mr_tid, mo_AcquireRelease);
    if (safe64_read(&stucked->mr_txnid) != laggard || !pid ||
        stucked->mr_snapshot_pages_retired.weak != hold_retired)
      continue;

    const meta_ptr_t head = meta_recent(env, &env->me_txn->tw.troika);
    const txnid_t gap = (head.txnid - laggard) / xMDBX_TXNID_STEP;
    const uint64_t head_retired =
        unaligned_peek_u64(4, head.ptr_c->mm_pages_retired);
    const size_t space =
        (head_retired > hold_retired)
            ? pgno2bytes(env, (pgno_t)(head_retired - hold_retired))
            : 0;
    int rc =
        callback(env, env->me_txn, pid, (mdbx_tid_t)((intptr_t)tid), laggard,
                 (gap < UINT_MAX) ? (unsigned)gap : UINT_MAX, space, retry);
    if (rc < 0)
      /* hsr returned error and/or agree MDBX_MAP_FULL error */
      break;

    if (rc > 0) {
      if (rc == 1) {
        /* hsr reported transaction (will be) aborted asynchronous */
        safe64_reset_compare(&stucked->mr_txnid, laggard);
      } else {
        /* hsr reported reader process was killed and slot should be cleared */
        safe64_reset(&stucked->mr_txnid, true);
        atomic_store64(&stucked->mr_tid, 0, mo_Relaxed);
        atomic_store32(&stucked->mr_pid, 0, mo_AcquireRelease);
      }
    } else if (!notify_eof_of_loop) {
#if MDBX_ENABLE_PROFGC
      env->me_lck->mti_pgop_stat.gc_prof.kicks += 1;
#endif /* MDBX_ENABLE_PROFGC */
      notify_eof_of_loop = true;
    }

  } while (++retry < INT_MAX);

  if (notify_eof_of_loop) {
    /* notify end of hsr-loop */
    const txnid_t turn = oldest - laggard;
    if (turn)
      NOTICE("hsr-kick: done turn %" PRIaTXN " -> %" PRIaTXN " +%" PRIaTXN,
             laggard, oldest, turn);
    callback(env, env->me_txn, 0, 0, laggard,
             (turn < UINT_MAX) ? (unsigned)turn : UINT_MAX, 0, -retry);
  }
  return oldest;
}

__cold int mdbx_env_set_hsr(MDBX_env *env, MDBX_hsr_func *hsr) {
  int rc = check_env(env, false);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  env->me_hsr_callback = hsr;
  return MDBX_SUCCESS;
}

__cold MDBX_hsr_func *mdbx_env_get_hsr(const MDBX_env *env) {
  return likely(env && env->me_signature.weak == MDBX_ME_SIGNATURE)
             ? env->me_hsr_callback
             : NULL;
}

#ifdef __SANITIZE_THREAD__
/* LY: avoid tsan-trap by me_txn, mm_last_pg and mt_next_pgno */
__attribute__((__no_sanitize_thread__, __noinline__))
#endif
int mdbx_txn_straggler(const MDBX_txn *txn, int *percent)
{
  int rc = check_txn(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return (rc > 0) ? -rc : rc;

  MDBX_env *env = txn->mt_env;
  if (unlikely((txn->mt_flags & MDBX_TXN_RDONLY) == 0)) {
    if (percent)
      *percent =
          (int)((txn->mt_next_pgno * UINT64_C(100) + txn->mt_end_pgno / 2) /
                txn->mt_end_pgno);
    return 0;
  }

  txnid_t lag;
  meta_troika_t troika = meta_tap(env);
  do {
    const meta_ptr_t head = meta_recent(env, &troika);
    if (percent) {
      const pgno_t maxpg = head.ptr_v->mm_geo.now;
      *percent =
          (int)((head.ptr_v->mm_geo.next * UINT64_C(100) + maxpg / 2) / maxpg);
    }
    lag = (head.txnid - txn->mt_txnid) / xMDBX_TXNID_STEP;
  } while (unlikely(meta_should_retry(env, &troika)));

  return (lag > INT_MAX) ? INT_MAX : (int)lag;
}

typedef struct mdbx_walk_ctx {
  void *mw_user;
  MDBX_pgvisitor_func *mw_visitor;
  MDBX_txn *mw_txn;
  MDBX_cursor *mw_cursor;
  bool mw_dont_check_keys_ordering;
} mdbx_walk_ctx_t;

__cold static int walk_sdb(mdbx_walk_ctx_t *ctx, MDBX_db *const sdb,
                           const MDBX_val *name, int deep);

static MDBX_page_type_t walk_page_type(const MDBX_page *mp) {
  if (mp)
    switch (mp->mp_flags) {
    case P_BRANCH:
      return MDBX_page_branch;
    case P_LEAF:
      return MDBX_page_leaf;
    case P_LEAF | P_LEAF2:
      return MDBX_page_dupfixed_leaf;
    case P_OVERFLOW:
      return MDBX_page_large;
    case P_META:
      return MDBX_page_meta;
    }
  return MDBX_page_broken;
}

/* Depth-first tree traversal. */
__cold static int walk_tree(mdbx_walk_ctx_t *ctx, const pgno_t pgno,
                            const MDBX_val *name, int deep,
                            txnid_t parent_txnid) {
  assert(pgno != P_INVALID);
  MDBX_page *mp = nullptr;
  int err = page_get(ctx->mw_cursor, pgno, &mp, parent_txnid);

  MDBX_page_type_t type = walk_page_type(mp);
  const size_t nentries = mp ? page_numkeys(mp) : 0;
  unsigned npages = 1;
  size_t pagesize = pgno2bytes(ctx->mw_txn->mt_env, npages);
  size_t header_size =
      (mp && !IS_LEAF2(mp)) ? PAGEHDRSZ + mp->mp_lower : PAGEHDRSZ;
  size_t payload_size = 0;
  size_t unused_size =
      (mp ? page_room(mp) : pagesize - header_size) - payload_size;
  size_t align_bytes = 0;

  for (size_t i = 0; err == MDBX_SUCCESS && i < nentries; ++i) {
    if (type == MDBX_page_dupfixed_leaf) {
      /* LEAF2 pages have no mp_ptrs[] or node headers */
      payload_size += mp->mp_leaf2_ksize;
      continue;
    }

    MDBX_node *node = page_node(mp, i);
    const size_t node_key_size = node_ks(node);
    payload_size += NODESIZE + node_key_size;

    if (type == MDBX_page_branch) {
      assert(i > 0 || node_ks(node) == 0);
      align_bytes += node_key_size & 1;
      continue;
    }

    const size_t node_data_size = node_ds(node);
    assert(type == MDBX_page_leaf);
    switch (node_flags(node)) {
    case 0 /* usual node */:
      payload_size += node_data_size;
      align_bytes += (node_key_size + node_data_size) & 1;
      break;

    case F_BIGDATA /* long data on the large/overflow page */: {
      const pgno_t large_pgno = node_largedata_pgno(node);
      const size_t over_payload = node_data_size;
      const size_t over_header = PAGEHDRSZ;
      npages = 1;

      assert(err == MDBX_SUCCESS);
      pgr_t lp = page_get_large(ctx->mw_cursor, large_pgno, mp->mp_txnid);
      err = lp.err;
      if (err == MDBX_SUCCESS) {
        cASSERT(ctx->mw_cursor, PAGETYPE_WHOLE(lp.page) == P_OVERFLOW);
        npages = lp.page->mp_pages;
      }

      pagesize = pgno2bytes(ctx->mw_txn->mt_env, npages);
      const size_t over_unused = pagesize - over_payload - over_header;
      const int rc = ctx->mw_visitor(large_pgno, npages, ctx->mw_user, deep,
                                     name, pagesize, MDBX_page_large, err, 1,
                                     over_payload, over_header, over_unused);
      if (unlikely(rc != MDBX_SUCCESS))
        return (rc == MDBX_RESULT_TRUE) ? MDBX_SUCCESS : rc;
      payload_size += sizeof(pgno_t);
      align_bytes += node_key_size & 1;
    } break;

    case F_SUBDATA /* sub-db */: {
      const size_t namelen = node_key_size;
      if (unlikely(namelen == 0 || node_data_size != sizeof(MDBX_db))) {
        assert(err == MDBX_CORRUPTED);
        err = MDBX_CORRUPTED;
      }
      header_size += node_data_size;
      align_bytes += (node_key_size + node_data_size) & 1;
    } break;

    case F_SUBDATA | F_DUPDATA /* dupsorted sub-tree */:
      if (unlikely(node_data_size != sizeof(MDBX_db))) {
        assert(err == MDBX_CORRUPTED);
        err = MDBX_CORRUPTED;
      }
      header_size += node_data_size;
      align_bytes += (node_key_size + node_data_size) & 1;
      break;

    case F_DUPDATA /* short sub-page */: {
      if (unlikely(node_data_size <= PAGEHDRSZ || (node_data_size & 1))) {
        assert(err == MDBX_CORRUPTED);
        err = MDBX_CORRUPTED;
        break;
      }

      MDBX_page *sp = node_data(node);
      const size_t nsubkeys = page_numkeys(sp);
      size_t subheader_size =
          IS_LEAF2(sp) ? PAGEHDRSZ : PAGEHDRSZ + sp->mp_lower;
      size_t subunused_size = page_room(sp);
      size_t subpayload_size = 0;
      size_t subalign_bytes = 0;
      MDBX_page_type_t subtype;

      switch (sp->mp_flags & /* ignore legacy P_DIRTY flag */ ~P_LEGACY_DIRTY) {
      case P_LEAF | P_SUBP:
        subtype = MDBX_subpage_leaf;
        break;
      case P_LEAF | P_LEAF2 | P_SUBP:
        subtype = MDBX_subpage_dupfixed_leaf;
        break;
      default:
        assert(err == MDBX_CORRUPTED);
        subtype = MDBX_subpage_broken;
        err = MDBX_CORRUPTED;
      }

      for (size_t j = 0; err == MDBX_SUCCESS && j < nsubkeys; ++j) {
        if (subtype == MDBX_subpage_dupfixed_leaf) {
          /* LEAF2 pages have no mp_ptrs[] or node headers */
          subpayload_size += sp->mp_leaf2_ksize;
        } else {
          assert(subtype == MDBX_subpage_leaf);
          const MDBX_node *subnode = page_node(sp, j);
          const size_t subnode_size = node_ks(subnode) + node_ds(subnode);
          subheader_size += NODESIZE;
          subpayload_size += subnode_size;
          subalign_bytes += subnode_size & 1;
          if (unlikely(node_flags(subnode) != 0)) {
            assert(err == MDBX_CORRUPTED);
            err = MDBX_CORRUPTED;
          }
        }
      }

      const int rc =
          ctx->mw_visitor(pgno, 0, ctx->mw_user, deep + 1, name, node_data_size,
                          subtype, err, nsubkeys, subpayload_size,
                          subheader_size, subunused_size + subalign_bytes);
      if (unlikely(rc != MDBX_SUCCESS))
        return (rc == MDBX_RESULT_TRUE) ? MDBX_SUCCESS : rc;
      header_size += subheader_size;
      unused_size += subunused_size;
      payload_size += subpayload_size;
      align_bytes += subalign_bytes + (node_key_size & 1);
    } break;

    default:
      assert(err == MDBX_CORRUPTED);
      err = MDBX_CORRUPTED;
    }
  }

  const int rc = ctx->mw_visitor(
      pgno, 1, ctx->mw_user, deep, name, ctx->mw_txn->mt_env->me_psize, type,
      err, nentries, payload_size, header_size, unused_size + align_bytes);
  if (unlikely(rc != MDBX_SUCCESS))
    return (rc == MDBX_RESULT_TRUE) ? MDBX_SUCCESS : rc;

  for (size_t i = 0; err == MDBX_SUCCESS && i < nentries; ++i) {
    if (type == MDBX_page_dupfixed_leaf)
      continue;

    MDBX_node *node = page_node(mp, i);
    if (type == MDBX_page_branch) {
      assert(err == MDBX_SUCCESS);
      err = walk_tree(ctx, node_pgno(node), name, deep + 1, mp->mp_txnid);
      if (unlikely(err != MDBX_SUCCESS)) {
        if (err == MDBX_RESULT_TRUE)
          break;
        return err;
      }
      continue;
    }

    assert(type == MDBX_page_leaf);
    switch (node_flags(node)) {
    default:
      continue;

    case F_SUBDATA /* sub-db */:
      if (unlikely(node_ds(node) != sizeof(MDBX_db))) {
        assert(err == MDBX_CORRUPTED);
        err = MDBX_CORRUPTED;
      } else {
        MDBX_db db;
        memcpy(&db, node_data(node), sizeof(db));
        const MDBX_val subdb_name = {node_key(node), node_ks(node)};
        assert(err == MDBX_SUCCESS);
        err = walk_sdb(ctx, &db, &subdb_name, deep + 1);
      }
      break;

    case F_SUBDATA | F_DUPDATA /* dupsorted sub-tree */:
      if (unlikely(node_ds(node) != sizeof(MDBX_db) ||
                   ctx->mw_cursor->mc_xcursor == NULL)) {
        assert(err == MDBX_CORRUPTED);
        err = MDBX_CORRUPTED;
      } else {
        MDBX_db db;
        memcpy(&db, node_data(node), sizeof(db));
        assert(ctx->mw_cursor->mc_xcursor ==
               &container_of(ctx->mw_cursor, MDBX_cursor_couple, outer)->inner);
        assert(err == MDBX_SUCCESS);
        err = cursor_xinit1(ctx->mw_cursor, node, mp);
        if (likely(err == MDBX_SUCCESS)) {
          ctx->mw_cursor = &ctx->mw_cursor->mc_xcursor->mx_cursor;
          err = walk_tree(ctx, db.md_root, name, deep + 1, mp->mp_txnid);
          MDBX_xcursor *inner_xcursor =
              container_of(ctx->mw_cursor, MDBX_xcursor, mx_cursor);
          MDBX_cursor_couple *couple =
              container_of(inner_xcursor, MDBX_cursor_couple, inner);
          ctx->mw_cursor = &couple->outer;
        }
      }
      break;
    }
  }

  return MDBX_SUCCESS;
}

__cold static int walk_sdb(mdbx_walk_ctx_t *ctx, MDBX_db *const sdb,
                           const MDBX_val *name, int deep) {
  if (unlikely(sdb->md_root == P_INVALID))
    return MDBX_SUCCESS; /* empty db */

  MDBX_cursor_couple couple;
  MDBX_dbx dbx = {.md_klen_min = INT_MAX};
  uint8_t dbistate = DBI_VALID | DBI_AUDITED;
  int rc = couple_init(&couple, ~0u, ctx->mw_txn, sdb, &dbx, &dbistate);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  couple.outer.mc_checking |= ctx->mw_dont_check_keys_ordering
                                  ? CC_SKIPORD | CC_PAGECHECK
                                  : CC_PAGECHECK;
  couple.inner.mx_cursor.mc_checking |= ctx->mw_dont_check_keys_ordering
                                            ? CC_SKIPORD | CC_PAGECHECK
                                            : CC_PAGECHECK;
  couple.outer.mc_next = ctx->mw_cursor;
  ctx->mw_cursor = &couple.outer;
  rc = walk_tree(ctx, sdb->md_root, name, deep,
                 sdb->md_mod_txnid ? sdb->md_mod_txnid : ctx->mw_txn->mt_txnid);
  ctx->mw_cursor = couple.outer.mc_next;
  return rc;
}

__cold int mdbx_env_pgwalk(MDBX_txn *txn, MDBX_pgvisitor_func *visitor,
                           void *user, bool dont_check_keys_ordering) {
  int rc = check_txn(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  mdbx_walk_ctx_t ctx;
  memset(&ctx, 0, sizeof(ctx));
  ctx.mw_txn = txn;
  ctx.mw_user = user;
  ctx.mw_visitor = visitor;
  ctx.mw_dont_check_keys_ordering = dont_check_keys_ordering;

  rc = visitor(0, NUM_METAS, user, 0, MDBX_PGWALK_META,
               pgno2bytes(txn->mt_env, NUM_METAS), MDBX_page_meta, MDBX_SUCCESS,
               NUM_METAS, sizeof(MDBX_meta) * NUM_METAS, PAGEHDRSZ * NUM_METAS,
               (txn->mt_env->me_psize - sizeof(MDBX_meta) - PAGEHDRSZ) *
                   NUM_METAS);
  if (!MDBX_IS_ERROR(rc))
    rc = walk_sdb(&ctx, &txn->mt_dbs[FREE_DBI], MDBX_PGWALK_GC, 0);
  if (!MDBX_IS_ERROR(rc))
    rc = walk_sdb(&ctx, &txn->mt_dbs[MAIN_DBI], MDBX_PGWALK_MAIN, 0);
  return rc;
}

int mdbx_canary_put(MDBX_txn *txn, const MDBX_canary *canary) {
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
  txn->mt_flags |= MDBX_TXN_DIRTY;

  return MDBX_SUCCESS;
}

int mdbx_canary_get(const MDBX_txn *txn, MDBX_canary *canary) {
  int rc = check_txn(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(canary == NULL))
    return MDBX_EINVAL;

  *canary = txn->mt_canary;
  return MDBX_SUCCESS;
}

int mdbx_cursor_on_first(const MDBX_cursor *mc) {
  if (unlikely(mc == NULL))
    return MDBX_EINVAL;

  if (unlikely(mc->mc_signature != MDBX_MC_LIVE))
    return (mc->mc_signature == MDBX_MC_READY4CLOSE) ? MDBX_EINVAL
                                                     : MDBX_EBADSIGN;

  if (!(mc->mc_flags & C_INITIALIZED))
    return mc->mc_db->md_entries ? MDBX_RESULT_FALSE : MDBX_RESULT_TRUE;

  for (size_t i = 0; i < mc->mc_snum; ++i) {
    if (mc->mc_ki[i])
      return MDBX_RESULT_FALSE;
  }

  return MDBX_RESULT_TRUE;
}

int mdbx_cursor_on_last(const MDBX_cursor *mc) {
  if (unlikely(mc == NULL))
    return MDBX_EINVAL;

  if (unlikely(mc->mc_signature != MDBX_MC_LIVE))
    return (mc->mc_signature == MDBX_MC_READY4CLOSE) ? MDBX_EINVAL
                                                     : MDBX_EBADSIGN;

  if (!(mc->mc_flags & C_INITIALIZED))
    return mc->mc_db->md_entries ? MDBX_RESULT_FALSE : MDBX_RESULT_TRUE;

  for (size_t i = 0; i < mc->mc_snum; ++i) {
    size_t nkeys = page_numkeys(mc->mc_pg[i]);
    if (mc->mc_ki[i] < nkeys - 1)
      return MDBX_RESULT_FALSE;
  }

  return MDBX_RESULT_TRUE;
}

int mdbx_cursor_eof(const MDBX_cursor *mc) {
  if (unlikely(mc == NULL))
    return MDBX_EINVAL;

  if (unlikely(mc->mc_signature != MDBX_MC_LIVE))
    return (mc->mc_signature == MDBX_MC_READY4CLOSE) ? MDBX_EINVAL
                                                     : MDBX_EBADSIGN;

  return ((mc->mc_flags & (C_INITIALIZED | C_EOF)) == C_INITIALIZED &&
          mc->mc_snum &&
          mc->mc_ki[mc->mc_top] < page_numkeys(mc->mc_pg[mc->mc_top]))
             ? MDBX_RESULT_FALSE
             : MDBX_RESULT_TRUE;
}

//------------------------------------------------------------------------------

struct diff_result {
  ptrdiff_t diff;
  size_t level;
  ptrdiff_t root_nkeys;
};

/* calculates: r = x - y */
__hot static int cursor_diff(const MDBX_cursor *const __restrict x,
                             const MDBX_cursor *const __restrict y,
                             struct diff_result *const __restrict r) {
  r->diff = 0;
  r->level = 0;
  r->root_nkeys = 0;

  if (unlikely(x->mc_signature != MDBX_MC_LIVE))
    return (x->mc_signature == MDBX_MC_READY4CLOSE) ? MDBX_EINVAL
                                                    : MDBX_EBADSIGN;

  if (unlikely(y->mc_signature != MDBX_MC_LIVE))
    return (y->mc_signature == MDBX_MC_READY4CLOSE) ? MDBX_EINVAL
                                                    : MDBX_EBADSIGN;

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
    if (unlikely(y->mc_pg[r->level] != x->mc_pg[r->level])) {
      ERROR("Mismatch cursors's pages at %zu level", r->level);
      return MDBX_PROBLEM;
    }

    intptr_t nkeys = page_numkeys(y->mc_pg[r->level]);
    assert(nkeys > 0);
    if (r->level == 0)
      r->root_nkeys = nkeys;

    const intptr_t limit_ki = nkeys - 1;
    const intptr_t x_ki = x->mc_ki[r->level];
    const intptr_t y_ki = y->mc_ki[r->level];
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
  ptrdiff_t btree_power = (ptrdiff_t)db->md_depth - 2 - (ptrdiff_t)dr->level;
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

int mdbx_estimate_distance(const MDBX_cursor *first, const MDBX_cursor *last,
                           ptrdiff_t *distance_items) {
  if (unlikely(first == NULL || last == NULL || distance_items == NULL))
    return MDBX_EINVAL;

  *distance_items = 0;
  struct diff_result dr;
  int rc = cursor_diff(last, first, &dr);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(dr.diff == 0) &&
      F_ISSET(first->mc_db->md_flags & last->mc_db->md_flags,
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

  if (unlikely(cursor->mc_signature != MDBX_MC_LIVE))
    return (cursor->mc_signature == MDBX_MC_READY4CLOSE) ? MDBX_EINVAL
                                                         : MDBX_EBADSIGN;

  int rc = check_txn(cursor->mc_txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (!(cursor->mc_flags & C_INITIALIZED))
    return MDBX_ENODATA;

  MDBX_cursor_couple next;
  cursor_copy(cursor, &next.outer);
  if (cursor->mc_db->md_flags & MDBX_DUPSORT) {
    next.outer.mc_xcursor = &next.inner;
    rc = cursor_xinit0(&next.outer);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    MDBX_xcursor *mx = &container_of(cursor, MDBX_cursor_couple, outer)->inner;
    cursor_copy(&mx->mx_cursor, &next.inner.mx_cursor);
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

  next.outer.mc_signature = MDBX_MC_LIVE;
  rc = cursor_get(&next.outer, key, data, move_op);
  if (unlikely(rc != MDBX_SUCCESS &&
               (rc != MDBX_NOTFOUND || !(next.outer.mc_flags & C_INITIALIZED))))
    return rc;

  return mdbx_estimate_distance(cursor, &next.outer, distance_items);
}

int mdbx_estimate_range(const MDBX_txn *txn, MDBX_dbi dbi,
                        const MDBX_val *begin_key, const MDBX_val *begin_data,
                        const MDBX_val *end_key, const MDBX_val *end_data,
                        ptrdiff_t *size_items) {
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

  if (unlikely(!check_dbi(txn, dbi, DBI_USRVALID)))
    return MDBX_BAD_DBI;

  MDBX_cursor_couple begin;
  /* LY: first, initialize cursor to refresh a DB in case it have DB_STALE */
  rc = cursor_init(&begin.outer, txn, dbi);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(begin.outer.mc_db->md_entries == 0)) {
    *size_items = 0;
    return MDBX_SUCCESS;
  }

  MDBX_val stub;
  if (!begin_key) {
    if (unlikely(!end_key)) {
      /* LY: FIRST..LAST case */
      *size_items = (ptrdiff_t)begin.outer.mc_db->md_entries;
      return MDBX_SUCCESS;
    }
    rc = cursor_first(&begin.outer, &stub, &stub);
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
        rc = cursor_last(&begin.outer, &stub, &stub);
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
        (begin_key == end_key ||
         begin.outer.mc_dbx->md_cmp(begin_key, end_key) == 0)) {
      /* LY: single key case */
      rc = cursor_set(&begin.outer, (MDBX_val *)begin_key, NULL, MDBX_SET).err;
      if (unlikely(rc != MDBX_SUCCESS)) {
        *size_items = 0;
        return (rc == MDBX_NOTFOUND) ? MDBX_SUCCESS : rc;
      }
      *size_items = 1;
      if (begin.outer.mc_xcursor != NULL) {
        MDBX_node *node = page_node(begin.outer.mc_pg[begin.outer.mc_top],
                                    begin.outer.mc_ki[begin.outer.mc_top]);
        if (node_flags(node) & F_DUPDATA) {
          /* LY: return the number of duplicates for given key */
          tASSERT(txn, begin.outer.mc_xcursor == &begin.inner &&
                           (begin.inner.mx_cursor.mc_flags & C_INITIALIZED));
          *size_items =
              (sizeof(*size_items) >= sizeof(begin.inner.mx_db.md_entries) ||
               begin.inner.mx_db.md_entries <= PTRDIFF_MAX)
                  ? (size_t)begin.inner.mx_db.md_entries
                  : PTRDIFF_MAX;
        }
      }
      return MDBX_SUCCESS;
    } else if (begin_data) {
      stub = *begin_data;
      rc = cursor_set(&begin.outer, (MDBX_val *)begin_key, &stub,
                      MDBX_GET_BOTH_RANGE)
               .err;
    } else {
      stub = *begin_key;
      rc = cursor_set(&begin.outer, &stub, nullptr, MDBX_SET_RANGE).err;
    }
  }

  if (unlikely(rc != MDBX_SUCCESS)) {
    if (rc != MDBX_NOTFOUND || !(begin.outer.mc_flags & C_INITIALIZED))
      return rc;
  }

  MDBX_cursor_couple end;
  rc = cursor_init(&end.outer, txn, dbi);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;
  if (!end_key)
    rc = cursor_last(&end.outer, &stub, &stub);
  else if (end_data) {
    stub = *end_data;
    rc = cursor_set(&end.outer, (MDBX_val *)end_key, &stub, MDBX_GET_BOTH_RANGE)
             .err;
  } else {
    stub = *end_key;
    rc = cursor_set(&end.outer, &stub, nullptr, MDBX_SET_RANGE).err;
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
 *  - получения dirty-статуса страницы по адресу (знать о MUTABLE/WRITEABLE).
 */

int mdbx_replace_ex(MDBX_txn *txn, MDBX_dbi dbi, const MDBX_val *key,
                    MDBX_val *new_data, MDBX_val *old_data,
                    MDBX_put_flags_t flags, MDBX_preserve_func preserver,
                    void *preserver_context) {
  int rc = check_txn_rw(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!key || !old_data || old_data == new_data))
    return MDBX_EINVAL;

  if (unlikely(old_data->iov_base == NULL && old_data->iov_len))
    return MDBX_EINVAL;

  if (unlikely(new_data == NULL &&
               (flags & (MDBX_CURRENT | MDBX_RESERVE)) != MDBX_CURRENT))
    return MDBX_EINVAL;

  if (unlikely(!check_dbi(txn, dbi, DBI_USRVALID)))
    return MDBX_BAD_DBI;

  if (unlikely(flags &
               ~(MDBX_NOOVERWRITE | MDBX_NODUPDATA | MDBX_ALLDUPS |
                 MDBX_RESERVE | MDBX_APPEND | MDBX_APPENDDUP | MDBX_CURRENT)))
    return MDBX_EINVAL;

  MDBX_cursor_couple cx;
  rc = cursor_init(&cx.outer, txn, dbi);
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

    rc = cursor_set(&cx.outer, &present_key, old_data, MDBX_GET_BOTH).err;
    if (rc != MDBX_SUCCESS)
      goto bailout;
  } else {
    /* в old_data буфер для сохранения предыдущего значения */
    if (unlikely(new_data && old_data->iov_base == new_data->iov_base))
      return MDBX_EINVAL;
    MDBX_val present_data;
    rc = cursor_set(&cx.outer, &present_key, &present_data, MDBX_SET_KEY).err;
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
          /* disallow update/delete for multi-values */
          MDBX_node *node = page_node(page, cx.outer.mc_ki[cx.outer.mc_top]);
          if (node_flags(node) & F_DUPDATA) {
            tASSERT(txn, XCURSOR_INITED(&cx.outer) &&
                             cx.outer.mc_xcursor->mx_db.md_entries > 1);
            if (cx.outer.mc_xcursor->mx_db.md_entries > 1) {
              rc = MDBX_EMULTIVAL;
              goto bailout;
            }
          }
          /* В оригинальной LMDB флажок MDBX_CURRENT здесь приведет
           * к замене данных без учета MDBX_DUPSORT сортировки,
           * но здесь это в любом случае допустимо, так как мы
           * проверили что для ключа есть только одно значение. */
        }
      }

      if (IS_MODIFIABLE(txn, page)) {
        if (new_data && cmp_lenfast(&present_data, new_data) == 0) {
          /* если данные совпадают, то ничего делать не надо */
          *old_data = *new_data;
          goto bailout;
        }
        rc = preserver ? preserver(preserver_context, old_data,
                                   present_data.iov_base, present_data.iov_len)
                       : MDBX_SUCCESS;
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
      } else {
        *old_data = present_data;
      }
      flags |= MDBX_CURRENT;
    }
  }

  if (likely(new_data))
    rc = cursor_put_checklen(&cx.outer, key, new_data, flags);
  else
    rc = cursor_del(&cx.outer, flags & MDBX_ALLDUPS);

bailout:
  txn->mt_cursors[dbi] = cx.outer.mc_next;
  return rc;
}

static int default_value_preserver(void *context, MDBX_val *target,
                                   const void *src, size_t bytes) {
  (void)context;
  if (unlikely(target->iov_len < bytes)) {
    target->iov_base = nullptr;
    target->iov_len = bytes;
    return MDBX_RESULT_TRUE;
  }
  memcpy(target->iov_base, src, target->iov_len = bytes);
  return MDBX_SUCCESS;
}

int mdbx_replace(MDBX_txn *txn, MDBX_dbi dbi, const MDBX_val *key,
                 MDBX_val *new_data, MDBX_val *old_data,
                 MDBX_put_flags_t flags) {
  return mdbx_replace_ex(txn, dbi, key, new_data, old_data, flags,
                         default_value_preserver, nullptr);
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
 * Таким образом, функция позволяет как избавится от лишнего копирования,
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

  const MDBX_env *env = txn->mt_env;
  const ptrdiff_t offset = ptr_dist(ptr, env->me_map);
  if (offset >= 0) {
    const pgno_t pgno = bytes2pgno(env, offset);
    if (likely(pgno < txn->mt_next_pgno)) {
      const MDBX_page *page = pgno2page(env, pgno);
      if (unlikely(page->mp_pgno != pgno ||
                   (page->mp_flags & P_ILL_BITS) != 0)) {
        /* The ptr pointed into middle of a large page,
         * not to the beginning of a data. */
        return MDBX_EINVAL;
      }
      return ((txn->mt_flags & MDBX_TXN_RDONLY) || !IS_MODIFIABLE(txn, page))
                 ? MDBX_RESULT_FALSE
                 : MDBX_RESULT_TRUE;
    }
    if ((size_t)offset < env->me_dxb_mmap.limit) {
      /* Указатель адресует что-то в пределах mmap, но за границей
       * распределенных страниц. Такое может случится если mdbx_is_dirty()
       * вызывается после операции, в ходе которой грязная страница была
       * возвращена в нераспределенное пространство. */
      return (txn->mt_flags & MDBX_TXN_RDONLY) ? MDBX_EINVAL : MDBX_RESULT_TRUE;
    }
  }

  /* Страница вне используемого mmap-диапазона, т.е. либо в функцию был
   * передан некорректный адрес, либо адрес в теневой странице, которая была
   * выделена посредством malloc().
   *
   * Для режима MDBX_WRITE_MAP режима страница однозначно "не грязная",
   * а для режимов без MDBX_WRITE_MAP однозначно "не чистая". */
  return (txn->mt_flags & (MDBX_WRITEMAP | MDBX_TXN_RDONLY)) ? MDBX_EINVAL
                                                             : MDBX_RESULT_TRUE;
}

int mdbx_dbi_sequence(MDBX_txn *txn, MDBX_dbi dbi, uint64_t *result,
                      uint64_t increment) {
  int rc = check_txn(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!check_dbi(txn, dbi, DBI_USRVALID)))
    return MDBX_BAD_DBI;

  if (unlikely(txn->mt_dbistate[dbi] & DBI_STALE)) {
    rc = fetch_sdb(txn, dbi);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }

  MDBX_db *dbs = &txn->mt_dbs[dbi];
  if (likely(result))
    *result = dbs->md_seq;

  if (likely(increment > 0)) {
    if (unlikely(txn->mt_flags & MDBX_TXN_RDONLY))
      return MDBX_EACCESS;

    uint64_t new = dbs->md_seq + increment;
    if (unlikely(new < increment))
      return MDBX_RESULT_TRUE;

    tASSERT(txn, new > dbs->md_seq);
    dbs->md_seq = new;
    txn->mt_flags |= MDBX_TXN_DIRTY;
    txn->mt_dbistate[dbi] |= DBI_DIRTY;
  }

  return MDBX_SUCCESS;
}

/*----------------------------------------------------------------------------*/

__cold intptr_t mdbx_limits_dbsize_min(intptr_t pagesize) {
  if (pagesize < 1)
    pagesize = (intptr_t)mdbx_default_pagesize();
  else if (unlikely(pagesize < (intptr_t)MIN_PAGESIZE ||
                    pagesize > (intptr_t)MAX_PAGESIZE ||
                    !is_powerof2((size_t)pagesize)))
    return -1;

  return MIN_PAGENO * pagesize;
}

__cold intptr_t mdbx_limits_dbsize_max(intptr_t pagesize) {
  if (pagesize < 1)
    pagesize = (intptr_t)mdbx_default_pagesize();
  else if (unlikely(pagesize < (intptr_t)MIN_PAGESIZE ||
                    pagesize > (intptr_t)MAX_PAGESIZE ||
                    !is_powerof2((size_t)pagesize)))
    return -1;

  STATIC_ASSERT(MAX_MAPSIZE < INTPTR_MAX);
  const uint64_t limit = (1 + (uint64_t)MAX_PAGENO) * pagesize;
  return (limit < MAX_MAPSIZE) ? (intptr_t)limit : (intptr_t)MAX_MAPSIZE;
}

__cold intptr_t mdbx_limits_txnsize_max(intptr_t pagesize) {
  if (pagesize < 1)
    pagesize = (intptr_t)mdbx_default_pagesize();
  else if (unlikely(pagesize < (intptr_t)MIN_PAGESIZE ||
                    pagesize > (intptr_t)MAX_PAGESIZE ||
                    !is_powerof2((size_t)pagesize)))
    return -1;

  STATIC_ASSERT(MAX_MAPSIZE < INTPTR_MAX);
  const uint64_t pgl_limit =
      pagesize * (uint64_t)(MDBX_PGL_LIMIT / MDBX_GOLD_RATIO_DBL);
  const uint64_t map_limit = (uint64_t)(MAX_MAPSIZE / MDBX_GOLD_RATIO_DBL);
  return (pgl_limit < map_limit) ? (intptr_t)pgl_limit : (intptr_t)map_limit;
}

/*** Key-making functions to avoid custom comparators *************************/

static __always_inline double key2double(const int64_t key) {
  union {
    uint64_t u;
    double f;
  } casting;

  casting.u = (key < 0) ? key + UINT64_C(0x8000000000000000)
                        : UINT64_C(0xffffFFFFffffFFFF) - key;
  return casting.f;
}

static __always_inline uint64_t double2key(const double *const ptr) {
  STATIC_ASSERT(sizeof(double) == sizeof(int64_t));
  const int64_t i = *(const int64_t *)ptr;
  const uint64_t u = (i < 0) ? UINT64_C(0xffffFFFFffffFFFF) - i
                             : i + UINT64_C(0x8000000000000000);
  if (ASSERT_ENABLED()) {
    const double f = key2double(u);
    assert(memcmp(&f, ptr, 8) == 0);
  }
  return u;
}

static __always_inline float key2float(const int32_t key) {
  union {
    uint32_t u;
    float f;
  } casting;

  casting.u =
      (key < 0) ? key + UINT32_C(0x80000000) : UINT32_C(0xffffFFFF) - key;
  return casting.f;
}

static __always_inline uint32_t float2key(const float *const ptr) {
  STATIC_ASSERT(sizeof(float) == sizeof(int32_t));
  const int32_t i = *(const int32_t *)ptr;
  const uint32_t u =
      (i < 0) ? UINT32_C(0xffffFFFF) - i : i + UINT32_C(0x80000000);
  if (ASSERT_ENABLED()) {
    const float f = key2float(u);
    assert(memcmp(&f, ptr, 4) == 0);
  }
  return u;
}

uint64_t mdbx_key_from_double(const double ieee754_64bit) {
  return double2key(&ieee754_64bit);
}

uint64_t mdbx_key_from_ptrdouble(const double *const ieee754_64bit) {
  return double2key(ieee754_64bit);
}

uint32_t mdbx_key_from_float(const float ieee754_32bit) {
  return float2key(&ieee754_32bit);
}

uint32_t mdbx_key_from_ptrfloat(const float *const ieee754_32bit) {
  return float2key(ieee754_32bit);
}

#define IEEE754_DOUBLE_MANTISSA_SIZE 52
#define IEEE754_DOUBLE_EXPONENTA_BIAS 0x3FF
#define IEEE754_DOUBLE_EXPONENTA_MAX 0x7FF
#define IEEE754_DOUBLE_IMPLICIT_LEAD UINT64_C(0x0010000000000000)
#define IEEE754_DOUBLE_MANTISSA_MASK UINT64_C(0x000FFFFFFFFFFFFF)
#define IEEE754_DOUBLE_MANTISSA_AMAX UINT64_C(0x001FFFFFFFFFFFFF)

static __inline int clz64(uint64_t value) {
#if __GNUC_PREREQ(4, 1) || __has_builtin(__builtin_clzl)
  if (sizeof(value) == sizeof(int))
    return __builtin_clz(value);
  if (sizeof(value) == sizeof(long))
    return __builtin_clzl(value);
#if (defined(__SIZEOF_LONG_LONG__) && __SIZEOF_LONG_LONG__ == 8) ||            \
    __has_builtin(__builtin_clzll)
  return __builtin_clzll(value);
#endif /* have(long long) && long long == uint64_t */
#endif /* GNU C */

#if defined(_MSC_VER)
  unsigned long index;
#if defined(_M_AMD64) || defined(_M_ARM64) || defined(_M_X64)
  _BitScanReverse64(&index, value);
  return 63 - index;
#else
  if (value > UINT32_MAX) {
    _BitScanReverse(&index, (uint32_t)(value >> 32));
    return 31 - index;
  }
  _BitScanReverse(&index, (uint32_t)value);
  return 63 - index;
#endif
#endif /* MSVC */

  value |= value >> 1;
  value |= value >> 2;
  value |= value >> 4;
  value |= value >> 8;
  value |= value >> 16;
  value |= value >> 32;
  static const uint8_t debruijn_clz64[64] = {
      63, 16, 62, 7,  15, 36, 61, 3,  6,  14, 22, 26, 35, 47, 60, 2,
      9,  5,  28, 11, 13, 21, 42, 19, 25, 31, 34, 40, 46, 52, 59, 1,
      17, 8,  37, 4,  23, 27, 48, 10, 29, 12, 43, 20, 32, 41, 53, 18,
      38, 24, 49, 30, 44, 33, 54, 39, 50, 45, 55, 51, 56, 57, 58, 0};
  return debruijn_clz64[value * UINT64_C(0x03F79D71B4CB0A89) >> 58];
}

static __inline uint64_t round_mantissa(const uint64_t u64, int shift) {
  assert(shift < 0 && u64 > 0);
  shift = -shift;
  const unsigned half = 1 << (shift - 1);
  const unsigned lsb = 1 & (unsigned)(u64 >> shift);
  const unsigned tie2even = 1 ^ lsb;
  return (u64 + half - tie2even) >> shift;
}

uint64_t mdbx_key_from_jsonInteger(const int64_t json_integer) {
  const uint64_t bias = UINT64_C(0x8000000000000000);
  if (json_integer > 0) {
    const uint64_t u64 = json_integer;
    int shift = clz64(u64) - (64 - IEEE754_DOUBLE_MANTISSA_SIZE - 1);
    uint64_t mantissa = u64 << shift;
    if (unlikely(shift < 0)) {
      mantissa = round_mantissa(u64, shift);
      if (mantissa > IEEE754_DOUBLE_MANTISSA_AMAX)
        mantissa = round_mantissa(u64, --shift);
    }

    assert(mantissa >= IEEE754_DOUBLE_IMPLICIT_LEAD &&
           mantissa <= IEEE754_DOUBLE_MANTISSA_AMAX);
    const uint64_t exponent = (uint64_t)IEEE754_DOUBLE_EXPONENTA_BIAS +
                              IEEE754_DOUBLE_MANTISSA_SIZE - shift;
    assert(exponent > 0 && exponent <= IEEE754_DOUBLE_EXPONENTA_MAX);
    const uint64_t key = bias + (exponent << IEEE754_DOUBLE_MANTISSA_SIZE) +
                         (mantissa - IEEE754_DOUBLE_IMPLICIT_LEAD);
#if !defined(_MSC_VER) ||                                                      \
    defined(                                                                   \
        _DEBUG) /* Workaround for MSVC error LNK2019: unresolved external      \
                   symbol __except1 referenced in function __ftol3_except */
    assert(key == mdbx_key_from_double((double)json_integer));
#endif /* Workaround for MSVC */
    return key;
  }

  if (json_integer < 0) {
    const uint64_t u64 = -json_integer;
    int shift = clz64(u64) - (64 - IEEE754_DOUBLE_MANTISSA_SIZE - 1);
    uint64_t mantissa = u64 << shift;
    if (unlikely(shift < 0)) {
      mantissa = round_mantissa(u64, shift);
      if (mantissa > IEEE754_DOUBLE_MANTISSA_AMAX)
        mantissa = round_mantissa(u64, --shift);
    }

    assert(mantissa >= IEEE754_DOUBLE_IMPLICIT_LEAD &&
           mantissa <= IEEE754_DOUBLE_MANTISSA_AMAX);
    const uint64_t exponent = (uint64_t)IEEE754_DOUBLE_EXPONENTA_BIAS +
                              IEEE754_DOUBLE_MANTISSA_SIZE - shift;
    assert(exponent > 0 && exponent <= IEEE754_DOUBLE_EXPONENTA_MAX);
    const uint64_t key = bias - 1 - (exponent << IEEE754_DOUBLE_MANTISSA_SIZE) -
                         (mantissa - IEEE754_DOUBLE_IMPLICIT_LEAD);
#if !defined(_MSC_VER) ||                                                      \
    defined(                                                                   \
        _DEBUG) /* Workaround for MSVC error LNK2019: unresolved external      \
                   symbol __except1 referenced in function __ftol3_except */
    assert(key == mdbx_key_from_double((double)json_integer));
#endif /* Workaround for MSVC */
    return key;
  }

  return bias;
}

int64_t mdbx_jsonInteger_from_key(const MDBX_val v) {
  assert(v.iov_len == 8);
  const uint64_t key = unaligned_peek_u64(2, v.iov_base);
  const uint64_t bias = UINT64_C(0x8000000000000000);
  const uint64_t covalent = (key > bias) ? key - bias : bias - key - 1;
  const int shift = IEEE754_DOUBLE_EXPONENTA_BIAS + 63 -
                    (IEEE754_DOUBLE_EXPONENTA_MAX &
                     (int)(covalent >> IEEE754_DOUBLE_MANTISSA_SIZE));
  if (unlikely(shift < 1))
    return (key < bias) ? INT64_MIN : INT64_MAX;
  if (unlikely(shift > 63))
    return 0;

  const uint64_t unscaled = ((covalent & IEEE754_DOUBLE_MANTISSA_MASK)
                             << (63 - IEEE754_DOUBLE_MANTISSA_SIZE)) +
                            bias;
  const int64_t absolute = unscaled >> shift;
  const int64_t value = (key < bias) ? -absolute : absolute;
  assert(key == mdbx_key_from_jsonInteger(value) ||
         (mdbx_key_from_jsonInteger(value - 1) < key &&
          key < mdbx_key_from_jsonInteger(value + 1)));
  return value;
}

double mdbx_double_from_key(const MDBX_val v) {
  assert(v.iov_len == 8);
  return key2double(unaligned_peek_u64(2, v.iov_base));
}

float mdbx_float_from_key(const MDBX_val v) {
  assert(v.iov_len == 4);
  return key2float(unaligned_peek_u32(2, v.iov_base));
}

int32_t mdbx_int32_from_key(const MDBX_val v) {
  assert(v.iov_len == 4);
  return (int32_t)(unaligned_peek_u32(2, v.iov_base) - UINT32_C(0x80000000));
}

int64_t mdbx_int64_from_key(const MDBX_val v) {
  assert(v.iov_len == 8);
  return (int64_t)(unaligned_peek_u64(2, v.iov_base) -
                   UINT64_C(0x8000000000000000));
}

__cold MDBX_cmp_func *mdbx_get_keycmp(MDBX_db_flags_t flags) {
  return get_default_keycmp(flags);
}

__cold MDBX_cmp_func *mdbx_get_datacmp(MDBX_db_flags_t flags) {
  return get_default_datacmp(flags);
}

__cold int mdbx_env_set_option(MDBX_env *env, const MDBX_option_t option,
                               uint64_t value) {
  int err = check_env(env, false);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  const bool lock_needed = ((env->me_flags & MDBX_ENV_ACTIVE) && env->me_txn0 &&
                            env->me_txn0->mt_owner != osal_thread_self());
  bool should_unlock = false;
  switch (option) {
  case MDBX_opt_sync_bytes:
    if (value == /* default */ UINT64_MAX)
      value = MAX_WRITE;
    if (unlikely(env->me_flags & MDBX_RDONLY))
      return MDBX_EACCESS;
    if (unlikely(!(env->me_flags & MDBX_ENV_ACTIVE)))
      return MDBX_EPERM;
    if (unlikely(value > SIZE_MAX - 65536))
      return MDBX_EINVAL;
    value = bytes2pgno(env, (size_t)value + env->me_psize - 1);
    if ((uint32_t)value != atomic_load32(&env->me_lck->mti_autosync_threshold,
                                         mo_AcquireRelease) &&
        atomic_store32(&env->me_lck->mti_autosync_threshold, (uint32_t)value,
                       mo_Relaxed)
        /* Дергаем sync(force=off) только если задано новое не-нулевое значение
         * и мы вне транзакции */
        && lock_needed) {
      err = env_sync(env, false, false);
      if (err == /* нечего сбрасывать на диск */ MDBX_RESULT_TRUE)
        err = MDBX_SUCCESS;
    }
    break;

  case MDBX_opt_sync_period:
    if (value == /* default */ UINT64_MAX)
      value = 2780315 /* 42.42424 секунды */;
    if (unlikely(env->me_flags & MDBX_RDONLY))
      return MDBX_EACCESS;
    if (unlikely(!(env->me_flags & MDBX_ENV_ACTIVE)))
      return MDBX_EPERM;
    if (unlikely(value > UINT32_MAX))
      return MDBX_EINVAL;
    value = osal_16dot16_to_monotime((uint32_t)value);
    if (value != atomic_load64(&env->me_lck->mti_autosync_period,
                               mo_AcquireRelease) &&
        atomic_store64(&env->me_lck->mti_autosync_period, value, mo_Relaxed)
        /* Дергаем sync(force=off) только если задано новое не-нулевое значение
         * и мы вне транзакции */
        && lock_needed) {
      err = env_sync(env, false, false);
      if (err == /* нечего сбрасывать на диск */ MDBX_RESULT_TRUE)
        err = MDBX_SUCCESS;
    }
    break;

  case MDBX_opt_max_db:
    if (value == /* default */ UINT64_MAX)
      value = 42;
    if (unlikely(value > MDBX_MAX_DBI))
      return MDBX_EINVAL;
    if (unlikely(env->me_map))
      return MDBX_EPERM;
    env->me_maxdbs = (unsigned)value + CORE_DBS;
    break;

  case MDBX_opt_max_readers:
    if (value == /* default */ UINT64_MAX)
      value = MDBX_READERS_LIMIT;
    if (unlikely(value < 1 || value > MDBX_READERS_LIMIT))
      return MDBX_EINVAL;
    if (unlikely(env->me_map))
      return MDBX_EPERM;
    env->me_maxreaders = (unsigned)value;
    break;

  case MDBX_opt_dp_reserve_limit:
    if (value == /* default */ UINT64_MAX)
      value = INT_MAX;
    if (unlikely(value > INT_MAX))
      return MDBX_EINVAL;
    if (env->me_options.dp_reserve_limit != (unsigned)value) {
      if (lock_needed) {
        err = mdbx_txn_lock(env, false);
        if (unlikely(err != MDBX_SUCCESS))
          return err;
        should_unlock = true;
      }
      env->me_options.dp_reserve_limit = (unsigned)value;
      while (env->me_dp_reserve_len > env->me_options.dp_reserve_limit) {
        eASSERT(env, env->me_dp_reserve != NULL);
        MDBX_page *dp = env->me_dp_reserve;
        MDBX_ASAN_UNPOISON_MEMORY_REGION(dp, env->me_psize);
        VALGRIND_MAKE_MEM_DEFINED(&mp_next(dp), sizeof(MDBX_page *));
        env->me_dp_reserve = mp_next(dp);
        void *const ptr = ptr_disp(dp, -(ptrdiff_t)sizeof(size_t));
        osal_free(ptr);
        env->me_dp_reserve_len -= 1;
      }
    }
    break;

  case MDBX_opt_rp_augment_limit:
    if (value == /* default */ UINT64_MAX) {
      env->me_options.flags.non_auto.rp_augment_limit = 0;
      env->me_options.rp_augment_limit = default_rp_augment_limit(env);
    } else if (unlikely(value > MDBX_PGL_LIMIT))
      return MDBX_EINVAL;
    else {
      env->me_options.flags.non_auto.rp_augment_limit = 1;
      env->me_options.rp_augment_limit = (unsigned)value;
    }
    break;

  case MDBX_opt_txn_dp_limit:
  case MDBX_opt_txn_dp_initial:
    if (value == /* default */ UINT64_MAX)
      value = MDBX_PGL_LIMIT;
    if (unlikely(value > MDBX_PGL_LIMIT || value < CURSOR_STACK * 4))
      return MDBX_EINVAL;
    if (unlikely(env->me_flags & MDBX_RDONLY))
      return MDBX_EACCESS;
    if (lock_needed) {
      err = mdbx_txn_lock(env, false);
      if (unlikely(err != MDBX_SUCCESS))
        return err;
      should_unlock = true;
    }
    if (env->me_txn)
      err = MDBX_EPERM /* unable change during transaction */;
    else {
      const pgno_t value32 = (pgno_t)value;
      if (option == MDBX_opt_txn_dp_initial &&
          env->me_options.dp_initial != value32) {
        env->me_options.dp_initial = value32;
        if (env->me_options.dp_limit < value32) {
          env->me_options.dp_limit = value32;
          env->me_options.flags.non_auto.dp_limit = 1;
        }
      }
      if (option == MDBX_opt_txn_dp_limit &&
          env->me_options.dp_limit != value32) {
        env->me_options.dp_limit = value32;
        env->me_options.flags.non_auto.dp_limit = 1;
        if (env->me_options.dp_initial > value32)
          env->me_options.dp_initial = value32;
      }
    }
    break;

  case MDBX_opt_spill_max_denominator:
    if (value == /* default */ UINT64_MAX)
      value = 8;
    if (unlikely(value > 255))
      return MDBX_EINVAL;
    env->me_options.spill_max_denominator = (uint8_t)value;
    break;
  case MDBX_opt_spill_min_denominator:
    if (value == /* default */ UINT64_MAX)
      value = 8;
    if (unlikely(value > 255))
      return MDBX_EINVAL;
    env->me_options.spill_min_denominator = (uint8_t)value;
    break;
  case MDBX_opt_spill_parent4child_denominator:
    if (value == /* default */ UINT64_MAX)
      value = 0;
    if (unlikely(value > 255))
      return MDBX_EINVAL;
    env->me_options.spill_parent4child_denominator = (uint8_t)value;
    break;

  case MDBX_opt_loose_limit:
    if (value == /* default */ UINT64_MAX)
      value = 64;
    if (unlikely(value > 255))
      return MDBX_EINVAL;
    env->me_options.dp_loose_limit = (uint8_t)value;
    break;

  case MDBX_opt_merge_threshold_16dot16_percent:
    if (value == /* default */ UINT64_MAX)
      value = 65536 / 4 /* 25% */;
    if (unlikely(value < 8192 || value > 32768))
      return MDBX_EINVAL;
    env->me_options.merge_threshold_16dot16_percent = (unsigned)value;
    recalculate_merge_threshold(env);
    break;

  case MDBX_opt_writethrough_threshold:
#if defined(_WIN32) || defined(_WIN64)
    /* позволяем "установить" значение по-умолчанию и совпадающее
     * с поведением соответствующим текущей установке MDBX_NOMETASYNC */
    if (value == /* default */ UINT64_MAX &&
        value != ((env->me_flags & MDBX_NOMETASYNC) ? 0 : UINT_MAX))
      err = MDBX_EINVAL;
#else
    if (value == /* default */ UINT64_MAX)
      value = MDBX_WRITETHROUGH_THRESHOLD_DEFAULT;
    if (value != (unsigned)value)
      err = MDBX_EINVAL;
    else
      env->me_options.writethrough_threshold = (unsigned)value;
#endif
    break;

  case MDBX_opt_prefault_write_enable:
    if (value == /* default */ UINT64_MAX) {
      env->me_options.prefault_write = default_prefault_write(env);
      env->me_options.flags.non_auto.prefault_write = false;
    } else if (value > 1)
      err = MDBX_EINVAL;
    else {
      env->me_options.prefault_write = value != 0;
      env->me_options.flags.non_auto.prefault_write = true;
    }
    break;

  default:
    return MDBX_EINVAL;
  }

  if (should_unlock)
    mdbx_txn_unlock(env);
  return err;
}

__cold int mdbx_env_get_option(const MDBX_env *env, const MDBX_option_t option,
                               uint64_t *pvalue) {
  int err = check_env(env, false);
  if (unlikely(err != MDBX_SUCCESS))
    return err;
  if (unlikely(!pvalue))
    return MDBX_EINVAL;

  switch (option) {
  case MDBX_opt_sync_bytes:
    if (unlikely(!(env->me_flags & MDBX_ENV_ACTIVE)))
      return MDBX_EPERM;
    *pvalue = pgno2bytes(
        env, atomic_load32(&env->me_lck->mti_autosync_threshold, mo_Relaxed));
    break;

  case MDBX_opt_sync_period:
    if (unlikely(!(env->me_flags & MDBX_ENV_ACTIVE)))
      return MDBX_EPERM;
    *pvalue = osal_monotime_to_16dot16(
        atomic_load64(&env->me_lck->mti_autosync_period, mo_Relaxed));
    break;

  case MDBX_opt_max_db:
    *pvalue = env->me_maxdbs - CORE_DBS;
    break;

  case MDBX_opt_max_readers:
    *pvalue = env->me_maxreaders;
    break;

  case MDBX_opt_dp_reserve_limit:
    *pvalue = env->me_options.dp_reserve_limit;
    break;

  case MDBX_opt_rp_augment_limit:
    *pvalue = env->me_options.rp_augment_limit;
    break;

  case MDBX_opt_txn_dp_limit:
    *pvalue = env->me_options.dp_limit;
    break;
  case MDBX_opt_txn_dp_initial:
    *pvalue = env->me_options.dp_initial;
    break;

  case MDBX_opt_spill_max_denominator:
    *pvalue = env->me_options.spill_max_denominator;
    break;
  case MDBX_opt_spill_min_denominator:
    *pvalue = env->me_options.spill_min_denominator;
    break;
  case MDBX_opt_spill_parent4child_denominator:
    *pvalue = env->me_options.spill_parent4child_denominator;
    break;

  case MDBX_opt_loose_limit:
    *pvalue = env->me_options.dp_loose_limit;
    break;

  case MDBX_opt_merge_threshold_16dot16_percent:
    *pvalue = env->me_options.merge_threshold_16dot16_percent;
    break;

  case MDBX_opt_writethrough_threshold:
#if defined(_WIN32) || defined(_WIN64)
    *pvalue = (env->me_flags & MDBX_NOMETASYNC) ? 0 : INT_MAX;
#else
    *pvalue = env->me_options.writethrough_threshold;
#endif
    break;

  case MDBX_opt_prefault_write_enable:
    *pvalue = env->me_options.prefault_write;
    break;

  default:
    return MDBX_EINVAL;
  }

  return MDBX_SUCCESS;
}

static size_t estimate_rss(size_t database_bytes) {
  return database_bytes + database_bytes / 64 +
         (512 + MDBX_WORDBITS * 16) * MEGABYTE;
}

__cold int mdbx_env_warmup(const MDBX_env *env, const MDBX_txn *txn,
                           MDBX_warmup_flags_t flags,
                           unsigned timeout_seconds_16dot16) {
  if (unlikely(env == NULL && txn == NULL))
    return MDBX_EINVAL;
  if (unlikely(flags >
               (MDBX_warmup_force | MDBX_warmup_oomsafe | MDBX_warmup_lock |
                MDBX_warmup_touchlimit | MDBX_warmup_release)))
    return MDBX_EINVAL;

  if (txn) {
    int err = check_txn(txn, MDBX_TXN_BLOCKED - MDBX_TXN_ERROR);
    if (unlikely(err != MDBX_SUCCESS))
      return err;
  }
  if (env) {
    int err = check_env(env, false);
    if (unlikely(err != MDBX_SUCCESS))
      return err;
    if (txn && unlikely(txn->mt_env != env))
      return MDBX_EINVAL;
  } else {
    env = txn->mt_env;
  }

  const uint64_t timeout_monotime =
      (timeout_seconds_16dot16 && (flags & MDBX_warmup_force))
          ? osal_monotime() + osal_16dot16_to_monotime(timeout_seconds_16dot16)
          : 0;

  if (flags & MDBX_warmup_release)
    munlock_all(env);

  pgno_t used_pgno;
  if (txn) {
    used_pgno = txn->mt_geo.next;
  } else {
    const meta_troika_t troika = meta_tap(env);
    used_pgno = meta_recent(env, &troika).ptr_v->mm_geo.next;
  }
  const size_t used_range = pgno_align2os_bytes(env, used_pgno);
  const pgno_t mlock_pgno = bytes2pgno(env, used_range);

  int rc = MDBX_SUCCESS;
  if (flags & MDBX_warmup_touchlimit) {
    const size_t estimated_rss = estimate_rss(used_range);
#if defined(_WIN32) || defined(_WIN64)
    SIZE_T current_ws_lower, current_ws_upper;
    if (GetProcessWorkingSetSize(GetCurrentProcess(), &current_ws_lower,
                                 &current_ws_upper) &&
        current_ws_lower < estimated_rss) {
      const SIZE_T ws_lower = estimated_rss;
      const SIZE_T ws_upper =
          (MDBX_WORDBITS == 32 && ws_lower > MEGABYTE * 2048)
              ? ws_lower
              : ws_lower + MDBX_WORDBITS * MEGABYTE * 32;
      if (!SetProcessWorkingSetSize(GetCurrentProcess(), ws_lower, ws_upper)) {
        rc = (int)GetLastError();
        WARNING("SetProcessWorkingSetSize(%zu, %zu) error %d", ws_lower,
                ws_upper, rc);
      }
    }
#endif /* Windows */
#ifdef RLIMIT_RSS
    struct rlimit rss;
    if (getrlimit(RLIMIT_RSS, &rss) == 0 && rss.rlim_cur < estimated_rss) {
      rss.rlim_cur = estimated_rss;
      if (rss.rlim_max < estimated_rss)
        rss.rlim_max = estimated_rss;
      if (setrlimit(RLIMIT_RSS, &rss)) {
        rc = errno;
        WARNING("setrlimit(%s, {%zu, %zu}) error %d", "RLIMIT_RSS",
                (size_t)rss.rlim_cur, (size_t)rss.rlim_max, rc);
      }
    }
#endif /* RLIMIT_RSS */
#ifdef RLIMIT_MEMLOCK
    if (flags & MDBX_warmup_lock) {
      struct rlimit memlock;
      if (getrlimit(RLIMIT_MEMLOCK, &memlock) == 0 &&
          memlock.rlim_cur < estimated_rss) {
        memlock.rlim_cur = estimated_rss;
        if (memlock.rlim_max < estimated_rss)
          memlock.rlim_max = estimated_rss;
        if (setrlimit(RLIMIT_MEMLOCK, &memlock)) {
          rc = errno;
          WARNING("setrlimit(%s, {%zu, %zu}) error %d", "RLIMIT_MEMLOCK",
                  (size_t)memlock.rlim_cur, (size_t)memlock.rlim_max, rc);
        }
      }
    }
#endif /* RLIMIT_MEMLOCK */
    (void)estimated_rss;
  }

#if defined(MLOCK_ONFAULT) &&                                                  \
    ((defined(_GNU_SOURCE) && __GLIBC_PREREQ(2, 27)) ||                        \
     (defined(__ANDROID_API__) && __ANDROID_API__ >= 30)) &&                   \
    (defined(__linux__) || defined(__gnu_linux__))
  if ((flags & MDBX_warmup_lock) != 0 && linux_kernel_version >= 0x04040000 &&
      atomic_load32(&env->me_mlocked_pgno, mo_AcquireRelease) < mlock_pgno) {
    if (mlock2(env->me_map, used_range, MLOCK_ONFAULT)) {
      rc = errno;
      WARNING("mlock2(%zu, %s) error %d", used_range, "MLOCK_ONFAULT", rc);
    } else {
      update_mlcnt(env, mlock_pgno, true);
      rc = MDBX_SUCCESS;
    }
    if (rc != EINVAL)
      flags -= MDBX_warmup_lock;
  }
#endif /* MLOCK_ONFAULT */

  int err = MDBX_ENOSYS;
#if MDBX_ENABLE_MADVISE
  err = set_readahead(env, used_pgno, true, true);
#else
#if defined(_WIN32) || defined(_WIN64)
  if (mdbx_PrefetchVirtualMemory) {
    WIN32_MEMORY_RANGE_ENTRY hint;
    hint.VirtualAddress = env->me_map;
    hint.NumberOfBytes = used_range;
    if (mdbx_PrefetchVirtualMemory(GetCurrentProcess(), 1, &hint, 0))
      err = MDBX_SUCCESS;
    else {
      err = (int)GetLastError();
      ERROR("%s(%zu) error %d", "PrefetchVirtualMemory", used_range, err);
    }
  }
#endif /* Windows */

#if defined(POSIX_MADV_WILLNEED)
  err = posix_madvise(env->me_map, used_range, POSIX_MADV_WILLNEED)
            ? ignore_enosys(errno)
            : MDBX_SUCCESS;
#elif defined(MADV_WILLNEED)
  err = madvise(env->me_map, used_range, MADV_WILLNEED) ? ignore_enosys(errno)
                                                        : MDBX_SUCCESS;
#endif

#if defined(F_RDADVISE)
  if (err) {
    fcntl(env->me_lazy_fd, F_RDAHEAD, true);
    struct radvisory hint;
    hint.ra_offset = 0;
    hint.ra_count = unlikely(used_range > INT_MAX &&
                             sizeof(used_range) > sizeof(hint.ra_count))
                        ? INT_MAX
                        : (int)used_range;
    err = fcntl(env->me_lazy_fd, F_RDADVISE, &hint) ? ignore_enosys(errno)
                                                    : MDBX_SUCCESS;
    if (err == ENOTTY)
      err = MDBX_SUCCESS /* Ignore ENOTTY for DB on the ram-disk */;
  }
#endif /* F_RDADVISE */
#endif /* MDBX_ENABLE_MADVISE */
  if (err != MDBX_SUCCESS && rc == MDBX_SUCCESS)
    rc = err;

  if ((flags & MDBX_warmup_force) != 0 &&
      (rc == MDBX_SUCCESS || rc == MDBX_ENOSYS)) {
    const volatile uint8_t *ptr = env->me_map;
    size_t offset = 0, unused = 42;
#if !(defined(_WIN32) || defined(_WIN64))
    if (flags & MDBX_warmup_oomsafe) {
      const int null_fd = open("/dev/null", O_WRONLY);
      if (unlikely(null_fd < 0))
        rc = errno;
      else {
        struct iovec iov[MDBX_AUXILARY_IOV_MAX];
        for (;;) {
          unsigned i;
          for (i = 0; i < MDBX_AUXILARY_IOV_MAX && offset < used_range; ++i) {
            iov[i].iov_base = (void *)(ptr + offset);
            iov[i].iov_len = 1;
            offset += env->me_os_psize;
          }
          if (unlikely(writev(null_fd, iov, i) < 0)) {
            rc = errno;
            if (rc == EFAULT)
              rc = ENOMEM;
            break;
          }
          if (offset >= used_range) {
            rc = MDBX_SUCCESS;
            break;
          }
          if (timeout_seconds_16dot16 && osal_monotime() > timeout_monotime) {
            rc = MDBX_RESULT_TRUE;
            break;
          }
        }
        close(null_fd);
      }
    } else
#endif /* Windows */
      for (;;) {
        unused += ptr[offset];
        offset += env->me_os_psize;
        if (offset >= used_range) {
          rc = MDBX_SUCCESS;
          break;
        }
        if (timeout_seconds_16dot16 && osal_monotime() > timeout_monotime) {
          rc = MDBX_RESULT_TRUE;
          break;
        }
      }
    (void)unused;
  }

  if ((flags & MDBX_warmup_lock) != 0 &&
      (rc == MDBX_SUCCESS || rc == MDBX_ENOSYS) &&
      atomic_load32(&env->me_mlocked_pgno, mo_AcquireRelease) < mlock_pgno) {
#if defined(_WIN32) || defined(_WIN64)
    if (VirtualLock(env->me_map, used_range)) {
      update_mlcnt(env, mlock_pgno, true);
      rc = MDBX_SUCCESS;
    } else {
      rc = (int)GetLastError();
      WARNING("%s(%zu) error %d", "VirtualLock", used_range, rc);
    }
#elif defined(_POSIX_MEMLOCK_RANGE)
    if (mlock(env->me_map, used_range) == 0) {
      update_mlcnt(env, mlock_pgno, true);
      rc = MDBX_SUCCESS;
    } else {
      rc = errno;
      WARNING("%s(%zu) error %d", "mlock", used_range, rc);
    }
#else
    rc = MDBX_ENOSYS;
#endif
  }

  return rc;
}

__cold void global_ctor(void) {
  osal_ctor();
  rthc_limit = RTHC_INITIAL_LIMIT;
  rthc_table = rthc_table_static;
#if defined(_WIN32) || defined(_WIN64)
  InitializeCriticalSection(&rthc_critical_section);
  InitializeCriticalSection(&lcklist_critical_section);
#else
  ENSURE(nullptr, pthread_key_create(&rthc_key, thread_dtor) == 0);
  TRACE("pid %d, &mdbx_rthc_key = %p, value 0x%x", osal_getpid(),
        __Wpedantic_format_voidptr(&rthc_key), (unsigned)rthc_key);
#endif
  /* checking time conversion, this also avoids racing on 32-bit architectures
   * during storing calculated 64-bit ratio(s) into memory. */
  uint32_t proba = UINT32_MAX;
  while (true) {
    unsigned time_conversion_checkup =
        osal_monotime_to_16dot16(osal_16dot16_to_monotime(proba));
    unsigned one_more = (proba < UINT32_MAX) ? proba + 1 : proba;
    unsigned one_less = (proba > 0) ? proba - 1 : proba;
    ENSURE(nullptr, time_conversion_checkup >= one_less &&
                        time_conversion_checkup <= one_more);
    if (proba == 0)
      break;
    proba >>= 1;
  }

  bootid = osal_bootid();

#if MDBX_DEBUG
  for (size_t i = 0; i < 2 * 2 * 2 * 3 * 3 * 3; ++i) {
    const bool s0 = (i >> 0) & 1;
    const bool s1 = (i >> 1) & 1;
    const bool s2 = (i >> 2) & 1;
    const uint8_t c01 = (i / (8 * 1)) % 3;
    const uint8_t c02 = (i / (8 * 3)) % 3;
    const uint8_t c12 = (i / (8 * 9)) % 3;

    const uint8_t packed = meta_cmp2pack(c01, c02, c12, s0, s1, s2);
    meta_troika_t troika;
    troika.fsm = (uint8_t)i;
    meta_troika_unpack(&troika, packed);

    const uint8_t tail = TROIKA_TAIL(&troika);
    const bool strict = TROIKA_STRICT_VALID(&troika);
    const bool valid = TROIKA_VALID(&troika);

    const uint8_t recent_chk = meta_cmp2recent(c01, s0, s1)
                                   ? (meta_cmp2recent(c02, s0, s2) ? 0 : 2)
                                   : (meta_cmp2recent(c12, s1, s2) ? 1 : 2);
    const uint8_t prefer_steady_chk =
        meta_cmp2steady(c01, s0, s1) ? (meta_cmp2steady(c02, s0, s2) ? 0 : 2)
                                     : (meta_cmp2steady(c12, s1, s2) ? 1 : 2);

    uint8_t tail_chk;
    if (recent_chk == 0)
      tail_chk = meta_cmp2steady(c12, s1, s2) ? 2 : 1;
    else if (recent_chk == 1)
      tail_chk = meta_cmp2steady(c02, s0, s2) ? 2 : 0;
    else
      tail_chk = meta_cmp2steady(c01, s0, s1) ? 1 : 0;

    const bool valid_chk =
        c01 != 1 || s0 != s1 || c02 != 1 || s0 != s2 || c12 != 1 || s1 != s2;
    const bool strict_chk = (c01 != 1 || s0 != s1) && (c02 != 1 || s0 != s2) &&
                            (c12 != 1 || s1 != s2);
    assert(troika.recent == recent_chk);
    assert(troika.prefer_steady == prefer_steady_chk);
    assert(tail == tail_chk);
    assert(valid == valid_chk);
    assert(strict == strict_chk);
    // printf(" %d, ", packed);
    assert(troika_fsm_map[troika.fsm] == packed);
  }
#endif /* MDBX_DEBUG*/

#if 0  /* debug */
  for (size_t i = 0; i < 65536; ++i) {
    size_t pages = pv2pages(i);
    size_t x = pages2pv(pages);
    size_t xp = pv2pages(x);
    if (!(x == i || (x % 2 == 0 && x < 65536)) || pages != xp)
      printf("%u => %zu => %u => %zu\n", i, pages, x, xp);
    assert(pages == xp);
  }
  fflush(stdout);
#endif /* #if 0 */
}

/*------------------------------------------------------------------------------
 * Legacy API */

#ifndef LIBMDBX_NO_EXPORTS_LEGACY_API

LIBMDBX_API int mdbx_txn_begin(MDBX_env *env, MDBX_txn *parent,
                               MDBX_txn_flags_t flags, MDBX_txn **ret) {
  return __inline_mdbx_txn_begin(env, parent, flags, ret);
}

LIBMDBX_API int mdbx_txn_commit(MDBX_txn *txn) {
  return __inline_mdbx_txn_commit(txn);
}

LIBMDBX_API __cold int mdbx_env_stat(const MDBX_env *env, MDBX_stat *stat,
                                     size_t bytes) {
  return __inline_mdbx_env_stat(env, stat, bytes);
}

LIBMDBX_API __cold int mdbx_env_info(const MDBX_env *env, MDBX_envinfo *info,
                                     size_t bytes) {
  return __inline_mdbx_env_info(env, info, bytes);
}

LIBMDBX_API int mdbx_dbi_flags(const MDBX_txn *txn, MDBX_dbi dbi,
                               unsigned *flags) {
  return __inline_mdbx_dbi_flags(txn, dbi, flags);
}

LIBMDBX_API __cold int mdbx_env_sync(MDBX_env *env) {
  return __inline_mdbx_env_sync(env);
}

LIBMDBX_API __cold int mdbx_env_sync_poll(MDBX_env *env) {
  return __inline_mdbx_env_sync_poll(env);
}

LIBMDBX_API __cold int mdbx_env_close(MDBX_env *env) {
  return __inline_mdbx_env_close(env);
}

LIBMDBX_API __cold int mdbx_env_set_mapsize(MDBX_env *env, size_t size) {
  return __inline_mdbx_env_set_mapsize(env, size);
}

LIBMDBX_API __cold int mdbx_env_set_maxdbs(MDBX_env *env, MDBX_dbi dbs) {
  return __inline_mdbx_env_set_maxdbs(env, dbs);
}

LIBMDBX_API __cold int mdbx_env_get_maxdbs(const MDBX_env *env, MDBX_dbi *dbs) {
  return __inline_mdbx_env_get_maxdbs(env, dbs);
}

LIBMDBX_API __cold int mdbx_env_set_maxreaders(MDBX_env *env,
                                               unsigned readers) {
  return __inline_mdbx_env_set_maxreaders(env, readers);
}

LIBMDBX_API __cold int mdbx_env_get_maxreaders(const MDBX_env *env,
                                               unsigned *readers) {
  return __inline_mdbx_env_get_maxreaders(env, readers);
}

LIBMDBX_API __cold int mdbx_env_set_syncbytes(MDBX_env *env, size_t threshold) {
  return __inline_mdbx_env_set_syncbytes(env, threshold);
}

LIBMDBX_API __cold int mdbx_env_get_syncbytes(const MDBX_env *env,
                                              size_t *threshold) {
  return __inline_mdbx_env_get_syncbytes(env, threshold);
}

LIBMDBX_API __cold int mdbx_env_set_syncperiod(MDBX_env *env,
                                               unsigned seconds_16dot16) {
  return __inline_mdbx_env_set_syncperiod(env, seconds_16dot16);
}

LIBMDBX_API __cold int mdbx_env_get_syncperiod(const MDBX_env *env,
                                               unsigned *seconds_16dot16) {
  return __inline_mdbx_env_get_syncperiod(env, seconds_16dot16);
}

LIBMDBX_API __cold MDBX_NOTHROW_CONST_FUNCTION intptr_t
mdbx_limits_pgsize_min(void) {
  return __inline_mdbx_limits_pgsize_min();
}

LIBMDBX_API __cold MDBX_NOTHROW_CONST_FUNCTION intptr_t
mdbx_limits_pgsize_max(void) {
  return __inline_mdbx_limits_pgsize_max();
}

LIBMDBX_API MDBX_NOTHROW_CONST_FUNCTION uint64_t
mdbx_key_from_int64(const int64_t i64) {
  return __inline_mdbx_key_from_int64(i64);
}

LIBMDBX_API MDBX_NOTHROW_CONST_FUNCTION uint32_t
mdbx_key_from_int32(const int32_t i32) {
  return __inline_mdbx_key_from_int32(i32);
}

#endif /* LIBMDBX_NO_EXPORTS_LEGACY_API */

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
    const struct MDBX_build_info mdbx_build = {
#ifdef MDBX_BUILD_TIMESTAMP
    MDBX_BUILD_TIMESTAMP
#else
    "\"" __DATE__ " " __TIME__ "\""
#endif /* MDBX_BUILD_TIMESTAMP */

    ,
#ifdef MDBX_BUILD_TARGET
    MDBX_BUILD_TARGET
#else
  #if defined(__ANDROID_API__)
    "Android" MDBX_STRINGIFY(__ANDROID_API__)
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
  #elif defined(__NetBSD__)
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
    "POSIX" MDBX_STRINGIFY(_POSIX_VERSION)
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
  #elif defined(__mips__) || defined(__mips) || defined(_R4000) || defined(__MIPS__)
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

#ifdef MDBX_BUILD_TYPE
# if defined(_MSC_VER)
#   pragma message("Configuration-depended MDBX_BUILD_TYPE: " MDBX_BUILD_TYPE)
# endif
    "-" MDBX_BUILD_TYPE
#endif /* MDBX_BUILD_TYPE */
    ,
    "MDBX_DEBUG=" MDBX_STRINGIFY(MDBX_DEBUG)
#ifdef ENABLE_GPROF
    " ENABLE_GPROF"
#endif /* ENABLE_GPROF */
    " MDBX_WORDBITS=" MDBX_STRINGIFY(MDBX_WORDBITS)
    " BYTE_ORDER="
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    "LITTLE_ENDIAN"
#elif __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
    "BIG_ENDIAN"
#else
    #error "FIXME: Unsupported byte order"
#endif /* __BYTE_ORDER__ */
    " MDBX_ENABLE_BIGFOOT=" MDBX_STRINGIFY(MDBX_ENABLE_BIGFOOT)
    " MDBX_ENV_CHECKPID=" MDBX_ENV_CHECKPID_CONFIG
    " MDBX_TXN_CHECKOWNER=" MDBX_TXN_CHECKOWNER_CONFIG
    " MDBX_64BIT_ATOMIC=" MDBX_64BIT_ATOMIC_CONFIG
    " MDBX_64BIT_CAS=" MDBX_64BIT_CAS_CONFIG
    " MDBX_TRUST_RTC=" MDBX_TRUST_RTC_CONFIG
    " MDBX_AVOID_MSYNC=" MDBX_STRINGIFY(MDBX_AVOID_MSYNC)
    " MDBX_ENABLE_REFUND=" MDBX_STRINGIFY(MDBX_ENABLE_REFUND)
    " MDBX_ENABLE_MADVISE=" MDBX_STRINGIFY(MDBX_ENABLE_MADVISE)
    " MDBX_ENABLE_MINCORE=" MDBX_STRINGIFY(MDBX_ENABLE_MINCORE)
    " MDBX_ENABLE_PGOP_STAT=" MDBX_STRINGIFY(MDBX_ENABLE_PGOP_STAT)
    " MDBX_ENABLE_PROFGC=" MDBX_STRINGIFY(MDBX_ENABLE_PROFGC)
#if MDBX_DISABLE_VALIDATION
    " MDBX_DISABLE_VALIDATION=YES"
#endif /* MDBX_DISABLE_VALIDATION */
#ifdef __SANITIZE_ADDRESS__
    " SANITIZE_ADDRESS=YES"
#endif /* __SANITIZE_ADDRESS__ */
#ifdef MDBX_USE_VALGRIND
    " MDBX_USE_VALGRIND=YES"
#endif /* MDBX_USE_VALGRIND */
#if MDBX_FORCE_ASSERTIONS
    " MDBX_FORCE_ASSERTIONS=YES"
#endif /* MDBX_FORCE_ASSERTIONS */
#ifdef _GNU_SOURCE
    " _GNU_SOURCE=YES"
#else
    " _GNU_SOURCE=NO"
#endif /* _GNU_SOURCE */
#ifdef __APPLE__
    " MDBX_OSX_SPEED_INSTEADOF_DURABILITY=" MDBX_STRINGIFY(MDBX_OSX_SPEED_INSTEADOF_DURABILITY)
#endif /* MacOS */
#if defined(_WIN32) || defined(_WIN64)
    " MDBX_WITHOUT_MSVC_CRT=" MDBX_STRINGIFY(MDBX_WITHOUT_MSVC_CRT)
    " MDBX_BUILD_SHARED_LIBRARY=" MDBX_STRINGIFY(MDBX_BUILD_SHARED_LIBRARY)
#if !MDBX_BUILD_SHARED_LIBRARY
    " MDBX_MANUAL_MODULE_HANDLER=" MDBX_STRINGIFY(MDBX_MANUAL_MODULE_HANDLER)
#endif
    " WINVER=" MDBX_STRINGIFY(WINVER)
#else /* Windows */
    " MDBX_LOCKING=" MDBX_LOCKING_CONFIG
    " MDBX_USE_OFDLOCKS=" MDBX_USE_OFDLOCKS_CONFIG
#endif /* !Windows */
    " MDBX_CACHELINE_SIZE=" MDBX_STRINGIFY(MDBX_CACHELINE_SIZE)
    " MDBX_CPU_WRITEBACK_INCOHERENT=" MDBX_STRINGIFY(MDBX_CPU_WRITEBACK_INCOHERENT)
    " MDBX_MMAP_INCOHERENT_CPU_CACHE=" MDBX_STRINGIFY(MDBX_MMAP_INCOHERENT_CPU_CACHE)
    " MDBX_MMAP_INCOHERENT_FILE_WRITE=" MDBX_STRINGIFY(MDBX_MMAP_INCOHERENT_FILE_WRITE)
    " MDBX_UNALIGNED_OK=" MDBX_STRINGIFY(MDBX_UNALIGNED_OK)
    " MDBX_PNL_ASCENDING=" MDBX_STRINGIFY(MDBX_PNL_ASCENDING)
    ,
#ifdef MDBX_BUILD_COMPILER
    MDBX_BUILD_COMPILER
#else
  #ifdef __INTEL_COMPILER
    "Intel C/C++ " MDBX_STRINGIFY(__INTEL_COMPILER)
  #elif defined(__apple_build_version__)
    "Apple clang " MDBX_STRINGIFY(__apple_build_version__)
  #elif defined(__ibmxl__)
    "IBM clang C " MDBX_STRINGIFY(__ibmxl_version__) "." MDBX_STRINGIFY(__ibmxl_release__)
    "." MDBX_STRINGIFY(__ibmxl_modification__) "." MDBX_STRINGIFY(__ibmxl_ptf_fix_level__)
  #elif defined(__clang__)
    "clang " MDBX_STRINGIFY(__clang_version__)
  #elif defined(__MINGW64__)
    "MINGW-64 " MDBX_STRINGIFY(__MINGW64_MAJOR_VERSION) "." MDBX_STRINGIFY(__MINGW64_MINOR_VERSION)
  #elif defined(__MINGW32__)
    "MINGW-32 " MDBX_STRINGIFY(__MINGW32_MAJOR_VERSION) "." MDBX_STRINGIFY(__MINGW32_MINOR_VERSION)
  #elif defined(__MINGW__)
    "MINGW " MDBX_STRINGIFY(__MINGW_MAJOR_VERSION) "." MDBX_STRINGIFY(__MINGW_MINOR_VERSION)
  #elif defined(__IBMC__)
    "IBM C " MDBX_STRINGIFY(__IBMC__)
  #elif defined(__GNUC__)
    "GNU C/C++ "
    #ifdef __VERSION__
      __VERSION__
    #else
      MDBX_STRINGIFY(__GNUC__) "." MDBX_STRINGIFY(__GNUC_MINOR__) "." MDBX_STRINGIFY(__GNUC_PATCHLEVEL__)
    #endif
  #elif defined(_MSC_VER)
    "MSVC " MDBX_STRINGIFY(_MSC_FULL_VER) "-" MDBX_STRINGIFY(_MSC_BUILD)
  #else
    "Unknown compiler"
  #endif
#endif /* MDBX_BUILD_COMPILER */
    ,
#ifdef MDBX_BUILD_FLAGS_CONFIG
    MDBX_BUILD_FLAGS_CONFIG
#endif /* MDBX_BUILD_FLAGS_CONFIG */
#ifdef MDBX_BUILD_FLAGS
    MDBX_BUILD_FLAGS
#endif /* MDBX_BUILD_FLAGS */
#if !(defined(MDBX_BUILD_FLAGS_CONFIG) || defined(MDBX_BUILD_FLAGS))
    "undefined (please use correct build script)"
#ifdef _MSC_VER
#pragma message("warning: Build flags undefined. Please use correct build script")
#else
#warning "Build flags undefined. Please use correct build script"
#endif // _MSC_VER
#endif
};

#ifdef __SANITIZE_ADDRESS__
#if !defined(_MSC_VER) || __has_attribute(weak)
LIBMDBX_API __attribute__((__weak__))
#endif
const char *__asan_default_options(void) {
  return "symbolize=1:allow_addr2line=1:"
#if MDBX_DEBUG
         "debug=1:"
         "verbosity=2:"
#endif /* MDBX_DEBUG */
         "log_threads=1:"
         "report_globals=1:"
         "replace_str=1:replace_intrin=1:"
         "malloc_context_size=9:"
#if !defined(__APPLE__)
         "detect_leaks=1:"
#endif
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
