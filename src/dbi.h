/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024

#pragma once

#include "essentials.h"

MDBX_MAYBE_UNUSED MDBX_NOTHROW_CONST_FUNCTION MDBX_INTERNAL size_t
dbi_bitmap_ctz_fallback(const MDBX_txn *txn, intptr_t bmi);

#if MDBX_ENABLE_DBI_SPARSE

static inline size_t dbi_bitmap_ctz(const MDBX_txn *txn, intptr_t bmi) {
  tASSERT(txn, bmi > 0);
  STATIC_ASSERT(sizeof(bmi) >= sizeof(txn->dbi_sparse[0]));
#if __GNUC_PREREQ(4, 1) || __has_builtin(__builtin_ctzl)
  if (sizeof(txn->dbi_sparse[0]) <= sizeof(int))
    return __builtin_ctz((int)bmi);
  if (sizeof(txn->dbi_sparse[0]) == sizeof(long))
    return __builtin_ctzl((long)bmi);
#if (defined(__SIZEOF_LONG_LONG__) && __SIZEOF_LONG_LONG__ == 8) ||            \
    __has_builtin(__builtin_ctzll)
  return __builtin_ctzll(bmi);
#endif /* have(long long) && long long == uint64_t */
#endif /* GNU C */

#if defined(_MSC_VER)
  unsigned long index;
  if (sizeof(txn->dbi_sparse[0]) > 4) {
#if defined(_M_AMD64) || defined(_M_ARM64) || defined(_M_X64)
    _BitScanForward64(&index, bmi);
    return index;
#else
    if (bmi > UINT32_MAX) {
      _BitScanForward(&index, (uint32_t)((uint64_t)bmi >> 32));
      return index;
    }
#endif
  }
  _BitScanForward(&index, (uint32_t)bmi);
  return index;
#endif /* MSVC */

  return dbi_bitmap_ctz_fallback(txn, bmi);
}

/* LY: Макрос целенаправленно сделан с одним циклом, чтобы сохранить возможность
 * использования оператора break */
#define TXN_FOREACH_DBI_FROM(TXN, I, FROM)                                     \
  for (size_t bitmap_chunk = CHAR_BIT * sizeof(TXN->dbi_sparse[0]),            \
              bitmap_item = TXN->dbi_sparse[0] >> FROM, I = FROM;              \
       I < TXN->n_dbi; ++I)                                                    \
    if (bitmap_item == 0) {                                                    \
      I = (I - 1) | (bitmap_chunk - 1);                                        \
      bitmap_item = TXN->dbi_sparse[(1 + I) / bitmap_chunk];                   \
      if (!bitmap_item)                                                        \
        I += bitmap_chunk;                                                     \
      continue;                                                                \
    } else if ((bitmap_item & 1) == 0) {                                       \
      size_t bitmap_skip = dbi_bitmap_ctz(txn, bitmap_item);                   \
      bitmap_item >>= bitmap_skip;                                             \
      I += bitmap_skip - 1;                                                    \
      continue;                                                                \
    } else if (bitmap_item >>= 1, TXN->dbi_state[I])

#else

#define TXN_FOREACH_DBI_FROM(TXN, I, SKIP)                                     \
  for (size_t I = SKIP; I < TXN->n_dbi; ++I)                                   \
    if (TXN->dbi_state[I])

#endif /* MDBX_ENABLE_DBI_SPARSE */

#define TXN_FOREACH_DBI_ALL(TXN, I) TXN_FOREACH_DBI_FROM(TXN, I, 0)
#define TXN_FOREACH_DBI_USER(TXN, I) TXN_FOREACH_DBI_FROM(TXN, I, CORE_DBS)

MDBX_INTERNAL int dbi_import(MDBX_txn *txn, const size_t dbi);

struct dbi_snap_result {
  uint32_t sequence;
  unsigned flags;
};
MDBX_INTERNAL struct dbi_snap_result dbi_snap(const MDBX_env *env,
                                              const size_t dbi);

MDBX_INTERNAL int dbi_update(MDBX_txn *txn, int keep);

static inline uint8_t dbi_state(const MDBX_txn *txn, const size_t dbi) {
  STATIC_ASSERT(
      (int)DBI_DIRTY == MDBX_DBI_DIRTY && (int)DBI_STALE == MDBX_DBI_STALE &&
      (int)DBI_FRESH == MDBX_DBI_FRESH && (int)DBI_CREAT == MDBX_DBI_CREAT);

#if MDBX_ENABLE_DBI_SPARSE
  const size_t bitmap_chunk = CHAR_BIT * sizeof(txn->dbi_sparse[0]);
  const size_t bitmap_indx = dbi / bitmap_chunk;
  const size_t bitmap_mask = (size_t)1 << dbi % bitmap_chunk;
  return likely(dbi < txn->n_dbi &&
                (txn->dbi_sparse[bitmap_indx] & bitmap_mask) != 0)
             ? txn->dbi_state[dbi]
             : 0;
#else
  return likely(dbi < txn->n_dbi) ? txn->dbi_state[dbi] : 0;
#endif /* MDBX_ENABLE_DBI_SPARSE */
}

static inline bool dbi_changed(const MDBX_txn *txn, const size_t dbi) {
  const MDBX_env *const env = txn->env;
  eASSERT(env, dbi_state(txn, dbi) & DBI_LINDO);
  const uint32_t snap_seq =
      atomic_load32(&env->dbi_seqs[dbi], mo_AcquireRelease);
  return snap_seq != txn->dbi_seqs[dbi];
}

static inline int dbi_check(const MDBX_txn *txn, const size_t dbi) {
  const uint8_t state = dbi_state(txn, dbi);
  if (likely((state & DBI_LINDO) != 0 && !dbi_changed(txn, dbi)))
    return (state & DBI_VALID) ? MDBX_SUCCESS : MDBX_BAD_DBI;

  /* Медленный путь: ленивая до-инициализацяи и импорт */
  return dbi_import((MDBX_txn *)txn, dbi);
}

static inline uint32_t dbi_seq_next(const MDBX_env *const env, size_t dbi) {
  uint32_t v = atomic_load32(&env->dbi_seqs[dbi], mo_AcquireRelease) + 1;
  return v ? v : 1;
}

MDBX_INTERNAL int dbi_open(MDBX_txn *txn, const MDBX_val *const name,
                           unsigned user_flags, MDBX_dbi *dbi,
                           MDBX_cmp_func *keycmp, MDBX_cmp_func *datacmp);

MDBX_INTERNAL int dbi_bind(MDBX_txn *txn, const size_t dbi, unsigned user_flags,
                           MDBX_cmp_func *keycmp, MDBX_cmp_func *datacmp);

MDBX_INTERNAL const tree_t *dbi_dig(const MDBX_txn *txn, const size_t dbi,
                                    tree_t *fallback);
