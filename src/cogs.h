/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025

#pragma once

#include "essentials.h"

MDBX_NOTHROW_CONST_FUNCTION MDBX_INTERNAL pgno_t pv2pages(uint16_t pv);

MDBX_NOTHROW_CONST_FUNCTION MDBX_INTERNAL uint16_t pages2pv(size_t pages);

MDBX_MAYBE_UNUSED MDBX_INTERNAL bool pv2pages_verify(void);

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
 *       PAGESPACE = pagesize - page_hdr_len;
 *       BRANCH_NODE_MAX = even_floor(
 *         (PAGESPACE - sizeof(indx_t) - NODESIZE) / (3 - 1) - sizeof(indx_t));
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
 *       LEAF_NODE_MAX = even_floor(PAGESPACE / 2 - sizeof(indx_t));
 *       DATALEN_NO_OVERFLOW = LEAF_NODE_MAX - NODESIZE - KEYLEN_MAX;
 *
 *  - Table-node must fit into one leaf-page:
 *       TABLE_NAME_MAX = LEAF_NODE_MAX - node_hdr_len - sizeof(tree_t);
 *
 *  - Dupsort values itself are a keys in a dupsort-table and couldn't be longer
 *    than the KEYLEN_MAX. But dupsort node must not great than LEAF_NODE_MAX,
 *    since dupsort value couldn't be placed on a large/overflow page:
 *       DUPSORT_DATALEN_MAX = min(KEYLEN_MAX,
 *                                 max(DATALEN_NO_OVERFLOW, sizeof(tree_t));
 */

#define PAGESPACE(pagesize) ((pagesize) - PAGEHDRSZ)

#define BRANCH_NODE_MAX(pagesize)                                                                                      \
  (EVEN_FLOOR((PAGESPACE(pagesize) - sizeof(indx_t) - NODESIZE) / (3 - 1) - sizeof(indx_t)))

#define LEAF_NODE_MAX(pagesize) (EVEN_FLOOR(PAGESPACE(pagesize) / 2) - sizeof(indx_t))

#define MAX_GC1OVPAGE(pagesize) (PAGESPACE(pagesize) / sizeof(pgno_t) - 1)

MDBX_NOTHROW_CONST_FUNCTION static inline size_t keysize_max(size_t pagesize, MDBX_db_flags_t flags) {
  assert(pagesize >= MDBX_MIN_PAGESIZE && pagesize <= MDBX_MAX_PAGESIZE && is_powerof2(pagesize));
  STATIC_ASSERT(BRANCH_NODE_MAX(MDBX_MIN_PAGESIZE) - NODESIZE >= 8);
  if (flags & MDBX_INTEGERKEY)
    return 8 /* sizeof(uint64_t) */;

  const intptr_t max_branch_key = BRANCH_NODE_MAX(pagesize) - NODESIZE;
  STATIC_ASSERT(LEAF_NODE_MAX(MDBX_MIN_PAGESIZE) - NODESIZE -
                    /* sizeof(uint64) as a key */ 8 >
                sizeof(tree_t));
  if (flags & (MDBX_DUPSORT | MDBX_DUPFIXED | MDBX_REVERSEDUP | MDBX_INTEGERDUP)) {
    const intptr_t max_dupsort_leaf_key = LEAF_NODE_MAX(pagesize) - NODESIZE - sizeof(tree_t);
    return (max_branch_key < max_dupsort_leaf_key) ? max_branch_key : max_dupsort_leaf_key;
  }
  return max_branch_key;
}

MDBX_NOTHROW_CONST_FUNCTION static inline size_t env_keysize_max(const MDBX_env *env, MDBX_db_flags_t flags) {
  size_t size_max;
  if (flags & MDBX_INTEGERKEY)
    size_max = 8 /* sizeof(uint64_t) */;
  else {
    const intptr_t max_branch_key = env->branch_nodemax - NODESIZE;
    STATIC_ASSERT(LEAF_NODE_MAX(MDBX_MIN_PAGESIZE) - NODESIZE -
                      /* sizeof(uint64) as a key */ 8 >
                  sizeof(tree_t));
    if (flags & (MDBX_DUPSORT | MDBX_DUPFIXED | MDBX_REVERSEDUP | MDBX_INTEGERDUP)) {
      const intptr_t max_dupsort_leaf_key = env->leaf_nodemax - NODESIZE - sizeof(tree_t);
      size_max = (max_branch_key < max_dupsort_leaf_key) ? max_branch_key : max_dupsort_leaf_key;
    } else
      size_max = max_branch_key;
  }
  eASSERT(env, size_max == keysize_max(env->ps, flags));
  return size_max;
}

MDBX_NOTHROW_CONST_FUNCTION static inline size_t keysize_min(MDBX_db_flags_t flags) {
  return (flags & MDBX_INTEGERKEY) ? 4 /* sizeof(uint32_t) */ : 0;
}

MDBX_NOTHROW_CONST_FUNCTION static inline size_t valsize_min(MDBX_db_flags_t flags) {
  if (flags & MDBX_INTEGERDUP)
    return 4 /* sizeof(uint32_t) */;
  else if (flags & MDBX_DUPFIXED)
    return sizeof(indx_t);
  else
    return 0;
}

MDBX_NOTHROW_CONST_FUNCTION static inline size_t valsize_max(size_t pagesize, MDBX_db_flags_t flags) {
  assert(pagesize >= MDBX_MIN_PAGESIZE && pagesize <= MDBX_MAX_PAGESIZE && is_powerof2(pagesize));

  if (flags & MDBX_INTEGERDUP)
    return 8 /* sizeof(uint64_t) */;

  if (flags & (MDBX_DUPSORT | MDBX_DUPFIXED | MDBX_REVERSEDUP))
    return keysize_max(pagesize, 0);

  const unsigned page_ln2 = log2n_powerof2(pagesize);
  const size_t hard = 0x7FF00000ul;
  const size_t hard_pages = hard >> page_ln2;
  STATIC_ASSERT(PAGELIST_LIMIT <= MAX_PAGENO);
  const size_t pages_limit = PAGELIST_LIMIT / 4;
  const size_t limit = (hard_pages < pages_limit) ? hard : (pages_limit << page_ln2);
  return (limit < MAX_MAPSIZE / 2) ? limit : MAX_MAPSIZE / 2;
}

MDBX_NOTHROW_CONST_FUNCTION static inline size_t env_valsize_max(const MDBX_env *env, MDBX_db_flags_t flags) {
  size_t size_max;
  if (flags & MDBX_INTEGERDUP)
    size_max = 8 /* sizeof(uint64_t) */;
  else if (flags & (MDBX_DUPSORT | MDBX_DUPFIXED | MDBX_REVERSEDUP))
    size_max = env_keysize_max(env, 0);
  else {
    const size_t hard = 0x7FF00000ul;
    const size_t hard_pages = hard >> env->ps2ln;
    STATIC_ASSERT(PAGELIST_LIMIT <= MAX_PAGENO);
    const size_t pages_limit = PAGELIST_LIMIT / 4;
    const size_t limit = (hard_pages < pages_limit) ? hard : (pages_limit << env->ps2ln);
    size_max = (limit < MAX_MAPSIZE / 2) ? limit : MAX_MAPSIZE / 2;
  }
  eASSERT(env, size_max == valsize_max(env->ps, flags));
  return size_max;
}

/*----------------------------------------------------------------------------*/

MDBX_NOTHROW_PURE_FUNCTION static inline size_t leaf_size(const MDBX_env *env, const MDBX_val *key,
                                                          const MDBX_val *data) {
  size_t node_bytes = node_size(key, data);
  if (node_bytes > env->leaf_nodemax)
    /* put on large/overflow page */
    node_bytes = node_size_len(key->iov_len, 0) + sizeof(pgno_t);

  return node_bytes + sizeof(indx_t);
}

MDBX_NOTHROW_PURE_FUNCTION static inline size_t branch_size(const MDBX_env *env, const MDBX_val *key) {
  /* Size of a node in a branch page with a given key.
   * This is just the node header plus the key, there is no data. */
  size_t node_bytes = node_size(key, nullptr);
  if (unlikely(node_bytes > env->branch_nodemax)) {
    /* put on large/overflow page, not implemented */
    mdbx_panic("node_size(key) %zu > %u branch_nodemax", node_bytes, env->branch_nodemax);
    node_bytes = node_size(key, nullptr) + sizeof(pgno_t);
  }

  return node_bytes + sizeof(indx_t);
}

MDBX_NOTHROW_CONST_FUNCTION static inline uint16_t flags_db2sub(uint16_t db_flags) {
  uint16_t sub_flags = db_flags & MDBX_DUPFIXED;

  /* MDBX_INTEGERDUP => MDBX_INTEGERKEY */
#define SHIFT_INTEGERDUP_TO_INTEGERKEY 2
  STATIC_ASSERT((MDBX_INTEGERDUP >> SHIFT_INTEGERDUP_TO_INTEGERKEY) == MDBX_INTEGERKEY);
  sub_flags |= (db_flags & MDBX_INTEGERDUP) >> SHIFT_INTEGERDUP_TO_INTEGERKEY;

  /* MDBX_REVERSEDUP => MDBX_REVERSEKEY */
#define SHIFT_REVERSEDUP_TO_REVERSEKEY 5
  STATIC_ASSERT((MDBX_REVERSEDUP >> SHIFT_REVERSEDUP_TO_REVERSEKEY) == MDBX_REVERSEKEY);
  sub_flags |= (db_flags & MDBX_REVERSEDUP) >> SHIFT_REVERSEDUP_TO_REVERSEKEY;

  return sub_flags;
}

static inline bool check_table_flags(unsigned flags) {
  switch (flags & ~(MDBX_REVERSEKEY | MDBX_INTEGERKEY)) {
  default:
    NOTICE("invalid db-flags 0x%x", flags);
    return false;
  case MDBX_DUPSORT:
  case MDBX_DUPSORT | MDBX_REVERSEDUP:
  case MDBX_DUPSORT | MDBX_DUPFIXED:
  case MDBX_DUPSORT | MDBX_DUPFIXED | MDBX_REVERSEDUP:
  case MDBX_DUPSORT | MDBX_DUPFIXED | MDBX_INTEGERDUP:
  case MDBX_DUPSORT | MDBX_DUPFIXED | MDBX_INTEGERDUP | MDBX_REVERSEDUP:
  case MDBX_DB_DEFAULTS:
    return (flags & (MDBX_REVERSEKEY | MDBX_INTEGERKEY)) != (MDBX_REVERSEKEY | MDBX_INTEGERKEY);
  }
}

static inline int tbl_setup_ifneed(const MDBX_env *env, volatile kvx_t *const kvx, const tree_t *const db) {
  return likely(kvx->clc.v.lmax) ? MDBX_SUCCESS : tbl_setup(env, kvx, db);
}

/*----------------------------------------------------------------------------*/

MDBX_NOTHROW_PURE_FUNCTION static inline size_t pgno2bytes(const MDBX_env *env, size_t pgno) {
  eASSERT(env, (1u << env->ps2ln) == env->ps);
  return ((size_t)pgno) << env->ps2ln;
}

MDBX_NOTHROW_PURE_FUNCTION static inline page_t *pgno2page(const MDBX_env *env, size_t pgno) {
  return ptr_disp(env->dxb_mmap.base, pgno2bytes(env, pgno));
}

MDBX_NOTHROW_PURE_FUNCTION static inline pgno_t bytes2pgno(const MDBX_env *env, size_t bytes) {
  eASSERT(env, (env->ps >> env->ps2ln) == 1);
  return (pgno_t)(bytes >> env->ps2ln);
}

MDBX_NOTHROW_PURE_FUNCTION MDBX_INTERNAL size_t bytes_align2os_bytes(const MDBX_env *env, size_t bytes);

MDBX_NOTHROW_PURE_FUNCTION MDBX_INTERNAL size_t pgno_align2os_bytes(const MDBX_env *env, size_t pgno);

MDBX_NOTHROW_PURE_FUNCTION MDBX_INTERNAL pgno_t pgno_align2os_pgno(const MDBX_env *env, size_t pgno);

MDBX_NOTHROW_PURE_FUNCTION static inline pgno_t largechunk_npages(const MDBX_env *env, size_t bytes) {
  return bytes2pgno(env, PAGEHDRSZ - 1 + bytes) + 1;
}

MDBX_NOTHROW_PURE_FUNCTION static inline MDBX_val get_key(const node_t *node) {
  MDBX_val key;
  key.iov_len = node_ks(node);
  key.iov_base = node_key(node);
  return key;
}

static inline void get_key_optional(const node_t *node, MDBX_val *keyptr /* __may_null */) {
  if (keyptr)
    *keyptr = get_key(node);
}

MDBX_NOTHROW_PURE_FUNCTION static inline void *page_data(const page_t *mp) { return ptr_disp(mp, PAGEHDRSZ); }

MDBX_NOTHROW_PURE_FUNCTION static inline const page_t *data_page(const void *data) {
  return container_of(data, page_t, entries);
}

MDBX_NOTHROW_PURE_FUNCTION static inline meta_t *page_meta(page_t *mp) { return (meta_t *)page_data(mp); }

MDBX_NOTHROW_PURE_FUNCTION static inline size_t page_numkeys(const page_t *mp) { return mp->lower >> 1; }

MDBX_NOTHROW_PURE_FUNCTION static inline size_t page_room(const page_t *mp) { return mp->upper - mp->lower; }

MDBX_NOTHROW_PURE_FUNCTION static inline size_t page_space(const MDBX_env *env) {
  STATIC_ASSERT(PAGEHDRSZ % 2 == 0);
  return env->ps - PAGEHDRSZ;
}

MDBX_NOTHROW_PURE_FUNCTION static inline size_t page_used(const MDBX_env *env, const page_t *mp) {
  return page_space(env) - page_room(mp);
}

/* The percentage of space used in the page, in a percents. */
MDBX_MAYBE_UNUSED MDBX_NOTHROW_PURE_FUNCTION static inline unsigned page_fill_percentum_x10(const MDBX_env *env,
                                                                                            const page_t *mp) {
  const size_t space = page_space(env);
  return (unsigned)((page_used(env, mp) * 1000 + space / 2) / space);
}

MDBX_NOTHROW_PURE_FUNCTION static inline node_t *page_node(const page_t *mp, size_t i) {
  assert(page_type_compat(mp) == P_LEAF || page_type(mp) == P_BRANCH);
  assert(page_numkeys(mp) > i);
  assert(mp->entries[i] % 2 == 0);
  return ptr_disp(mp, mp->entries[i] + PAGEHDRSZ);
}

MDBX_NOTHROW_PURE_FUNCTION static inline void *page_dupfix_ptr(const page_t *mp, size_t i, size_t keysize) {
  assert(page_type_compat(mp) == (P_LEAF | P_DUPFIX) && i == (indx_t)i && mp->dupfix_ksize == keysize);
  (void)keysize;
  return ptr_disp(mp, PAGEHDRSZ + mp->dupfix_ksize * (indx_t)i);
}

MDBX_NOTHROW_PURE_FUNCTION static inline MDBX_val page_dupfix_key(const page_t *mp, size_t i, size_t keysize) {
  MDBX_val r;
  r.iov_base = page_dupfix_ptr(mp, i, keysize);
  r.iov_len = mp->dupfix_ksize;
  return r;
}

/*----------------------------------------------------------------------------*/

MDBX_NOTHROW_PURE_FUNCTION MDBX_INTERNAL int cmp_int_unaligned(const MDBX_val *a, const MDBX_val *b);

#if MDBX_UNALIGNED_OK < 2 || (MDBX_DEBUG || MDBX_FORCE_ASSERTIONS || !defined(NDEBUG))
MDBX_NOTHROW_PURE_FUNCTION MDBX_INTERNAL int
/* Compare two items pointing at 2-byte aligned unsigned int's. */
cmp_int_align2(const MDBX_val *a, const MDBX_val *b);
#else
#define cmp_int_align2 cmp_int_unaligned
#endif /* !MDBX_UNALIGNED_OK || debug */

#if MDBX_UNALIGNED_OK < 4 || (MDBX_DEBUG || MDBX_FORCE_ASSERTIONS || !defined(NDEBUG))
MDBX_NOTHROW_PURE_FUNCTION MDBX_INTERNAL int
/* Compare two items pointing at 4-byte aligned unsigned int's. */
cmp_int_align4(const MDBX_val *a, const MDBX_val *b);
#else
#define cmp_int_align4 cmp_int_unaligned
#endif /* !MDBX_UNALIGNED_OK || debug */

/* Compare two items lexically */
MDBX_NOTHROW_PURE_FUNCTION MDBX_INTERNAL int cmp_lexical(const MDBX_val *a, const MDBX_val *b);

/* Compare two items in reverse byte order */
MDBX_NOTHROW_PURE_FUNCTION MDBX_INTERNAL int cmp_reverse(const MDBX_val *a, const MDBX_val *b);

/* Fast non-lexically comparator */
MDBX_NOTHROW_PURE_FUNCTION MDBX_INTERNAL int cmp_lenfast(const MDBX_val *a, const MDBX_val *b);

MDBX_NOTHROW_PURE_FUNCTION MDBX_INTERNAL bool eq_fast_slowpath(const uint8_t *a, const uint8_t *b, size_t l);

MDBX_NOTHROW_PURE_FUNCTION static inline bool eq_fast(const MDBX_val *a, const MDBX_val *b) {
  return unlikely(a->iov_len == b->iov_len) && eq_fast_slowpath(a->iov_base, b->iov_base, a->iov_len);
}

MDBX_NOTHROW_PURE_FUNCTION MDBX_INTERNAL int cmp_equal_or_greater(const MDBX_val *a, const MDBX_val *b);

MDBX_NOTHROW_PURE_FUNCTION MDBX_INTERNAL int cmp_equal_or_wrong(const MDBX_val *a, const MDBX_val *b);

static inline MDBX_cmp_func *builtin_keycmp(MDBX_db_flags_t flags) {
  return (flags & MDBX_REVERSEKEY) ? cmp_reverse : (flags & MDBX_INTEGERKEY) ? cmp_int_align2 : cmp_lexical;
}

static inline MDBX_cmp_func *builtin_datacmp(MDBX_db_flags_t flags) {
  return !(flags & MDBX_DUPSORT)
             ? cmp_lenfast
             : ((flags & MDBX_INTEGERDUP) ? cmp_int_unaligned
                                          : ((flags & MDBX_REVERSEDUP) ? cmp_reverse : cmp_lexical));
}

/*----------------------------------------------------------------------------*/

MDBX_INTERNAL uint32_t combine_durability_flags(const uint32_t a, const uint32_t b);

MDBX_CONST_FUNCTION static inline lck_t *lckless_stub(const MDBX_env *env) {
  uintptr_t stub = (uintptr_t)&env->lckless_placeholder;
  /* align to avoid false-positive alarm from UndefinedBehaviorSanitizer */
  stub = (stub + MDBX_CACHELINE_SIZE - 1) & ~(MDBX_CACHELINE_SIZE - 1);
  return (lck_t *)stub;
}

#if !(defined(_WIN32) || defined(_WIN64))
MDBX_MAYBE_UNUSED static inline int ignore_enosys(int err) {
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

static inline int check_env(const MDBX_env *env, const bool wanna_active) {
  if (unlikely(!env))
    return MDBX_EINVAL;

  if (unlikely(env->signature.weak != env_signature))
    return MDBX_EBADSIGN;

  if (unlikely(env->flags & ENV_FATAL_ERROR))
    return MDBX_PANIC;

  if (wanna_active) {
#if MDBX_ENV_CHECKPID
    if (unlikely(env->pid != osal_getpid()) && env->pid) {
      ((MDBX_env *)env)->flags |= ENV_FATAL_ERROR;
      return MDBX_PANIC;
    }
#endif /* MDBX_ENV_CHECKPID */
    if (unlikely((env->flags & ENV_ACTIVE) == 0))
      return MDBX_EPERM;
    eASSERT(env, env->dxb_mmap.base != nullptr);
  }

  return MDBX_SUCCESS;
}

static __always_inline int check_txn(const MDBX_txn *txn, int bad_bits) {
  if (unlikely(!txn))
    return MDBX_EINVAL;

  if (unlikely(txn->signature != txn_signature))
    return MDBX_EBADSIGN;

  if (bad_bits) {
    if (unlikely(!txn->env->dxb_mmap.base))
      return MDBX_EPERM;

    if (unlikely(txn->flags & bad_bits)) {
      if ((bad_bits & MDBX_TXN_RDONLY) && unlikely(txn->flags & MDBX_TXN_RDONLY))
        return MDBX_EACCESS;
      if ((bad_bits & MDBX_TXN_PARKED) == 0)
        return MDBX_BAD_TXN;
      return txn_check_badbits_parked(txn, bad_bits);
    }
  }

  tASSERT(txn, (txn->flags & MDBX_TXN_FINISHED) ||
                   (txn->flags & MDBX_NOSTICKYTHREADS) == (txn->env->flags & MDBX_NOSTICKYTHREADS));
#if MDBX_TXN_CHECKOWNER
  if ((txn->flags & (MDBX_NOSTICKYTHREADS | MDBX_TXN_FINISHED)) != MDBX_NOSTICKYTHREADS &&
      !(bad_bits /* abort/reset/txn-break */ == 0 &&
        ((txn->flags & (MDBX_TXN_RDONLY | MDBX_TXN_FINISHED)) == (MDBX_TXN_RDONLY | MDBX_TXN_FINISHED))) &&
      unlikely(txn->owner != osal_thread_self()))
    return txn->owner ? MDBX_THREAD_MISMATCH : MDBX_BAD_TXN;
#endif /* MDBX_TXN_CHECKOWNER */

  return MDBX_SUCCESS;
}

static inline int check_txn_rw(const MDBX_txn *txn, int bad_bits) {
  return check_txn(txn, (bad_bits | MDBX_TXN_RDONLY) & ~MDBX_TXN_PARKED);
}

/*----------------------------------------------------------------------------*/

MDBX_INTERNAL void mincore_clean_cache(const MDBX_env *const env);

MDBX_INTERNAL void update_mlcnt(const MDBX_env *env, const pgno_t new_aligned_mlocked_pgno,
                                const bool lock_not_release);

MDBX_INTERNAL void munlock_after(const MDBX_env *env, const pgno_t aligned_pgno, const size_t end_bytes);

MDBX_INTERNAL void munlock_all(const MDBX_env *env);

/*----------------------------------------------------------------------------*/
/* Cache coherence and mmap invalidation */
#ifndef MDBX_CPU_WRITEBACK_INCOHERENT
#error "The MDBX_CPU_WRITEBACK_INCOHERENT must be defined before"
#elif MDBX_CPU_WRITEBACK_INCOHERENT
#define osal_flush_incoherent_cpu_writeback() osal_memory_barrier()
#else
#define osal_flush_incoherent_cpu_writeback() osal_compiler_barrier()
#endif /* MDBX_CPU_WRITEBACK_INCOHERENT */

MDBX_MAYBE_UNUSED static inline void osal_flush_incoherent_mmap(const void *addr, size_t nbytes,
                                                                const intptr_t pagesize) {
#ifndef MDBX_MMAP_INCOHERENT_FILE_WRITE
#error "The MDBX_MMAP_INCOHERENT_FILE_WRITE must be defined before"
#elif MDBX_MMAP_INCOHERENT_FILE_WRITE
  char *const begin = (char *)(-pagesize & (intptr_t)addr);
  char *const end = (char *)(-pagesize & (intptr_t)((char *)addr + nbytes + pagesize - 1));
  int err = msync(begin, end - begin, MS_SYNC | MS_INVALIDATE) ? errno : 0;
  eASSERT(nullptr, err == 0);
  (void)err;
#else
  (void)pagesize;
#endif /* MDBX_MMAP_INCOHERENT_FILE_WRITE */

#ifndef MDBX_MMAP_INCOHERENT_CPU_CACHE
#error "The MDBX_MMAP_INCOHERENT_CPU_CACHE must be defined before"
#elif MDBX_MMAP_INCOHERENT_CPU_CACHE
#ifdef DCACHE
  /* MIPS has cache coherency issues.
   * Note: for any nbytes >= on-chip cache size, entire is flushed. */
  cacheflush((void *)addr, nbytes, DCACHE);
#else
#error "Oops, cacheflush() not available"
#endif /* DCACHE */
#endif /* MDBX_MMAP_INCOHERENT_CPU_CACHE */

#if !MDBX_MMAP_INCOHERENT_FILE_WRITE && !MDBX_MMAP_INCOHERENT_CPU_CACHE
  (void)addr;
  (void)nbytes;
#endif
}
