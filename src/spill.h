/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025

#pragma once

#include "essentials.h"

MDBX_INTERNAL void spill_remove(MDBX_txn *txn, size_t idx, size_t npages);
MDBX_INTERNAL pnl_t spill_purge(MDBX_txn *txn);
MDBX_INTERNAL int spill_slowpath(MDBX_txn *const txn, MDBX_cursor *const m0, const intptr_t wanna_spill_entries,
                                 const intptr_t wanna_spill_npages, const size_t need);
/*----------------------------------------------------------------------------*/

static inline size_t spill_search(const MDBX_txn *txn, pgno_t pgno) {
  tASSERT(txn, (txn->flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);
  const pnl_t pnl = txn->wr.spilled.list;
  if (likely(!pnl))
    return 0;
  pgno <<= 1;
  size_t n = pnl_search(pnl, pgno, (size_t)MAX_PAGENO + MAX_PAGENO + 1);
  return (n <= pnl_size(pnl) && pnl[n] == pgno) ? n : 0;
}

static inline bool spill_intersect(const MDBX_txn *txn, pgno_t pgno, size_t npages) {
  const pnl_t pnl = txn->wr.spilled.list;
  if (likely(!pnl))
    return false;
  const size_t len = pnl_size(pnl);
  if (LOG_ENABLED(MDBX_LOG_EXTRA)) {
    DEBUG_EXTRA("PNL len %zu [", len);
    for (size_t i = 1; i <= len; ++i)
      DEBUG_EXTRA_PRINT(" %li", (pnl[i] & 1) ? -(long)(pnl[i] >> 1) : (long)(pnl[i] >> 1));
    DEBUG_EXTRA_PRINT("%s\n", "]");
  }
  const pgno_t spilled_range_begin = pgno << 1;
  const pgno_t spilled_range_last = ((pgno + (pgno_t)npages) << 1) - 1;
#if MDBX_PNL_ASCENDING
  const size_t n = pnl_search(pnl, spilled_range_begin, (size_t)(MAX_PAGENO + 1) << 1);
  tASSERT(txn, n && (n == pnl_size(pnl) + 1 || spilled_range_begin <= pnl[n]));
  const bool rc = n <= pnl_size(pnl) && pnl[n] <= spilled_range_last;
#else
  const size_t n = pnl_search(pnl, spilled_range_last, (size_t)MAX_PAGENO + MAX_PAGENO + 1);
  tASSERT(txn, n && (n == pnl_size(pnl) + 1 || spilled_range_last >= pnl[n]));
  const bool rc = n <= pnl_size(pnl) && pnl[n] >= spilled_range_begin;
#endif
  if (ASSERT_ENABLED()) {
    bool check = false;
    for (size_t i = 0; i < npages; ++i)
      check |= spill_search(txn, (pgno_t)(pgno + i)) != 0;
    tASSERT(txn, check == rc);
  }
  return rc;
}

static inline int txn_spill(MDBX_txn *const txn, MDBX_cursor *const m0, const size_t need) {
  tASSERT(txn, (txn->flags & MDBX_TXN_RDONLY) == 0);
  tASSERT(txn, !m0 || cursor_is_tracked(m0));

  const intptr_t wanna_spill_entries = txn->wr.dirtylist ? (need - txn->wr.dirtyroom - txn->wr.loose_count) : 0;
  const intptr_t wanna_spill_npages =
      need + (txn->wr.dirtylist ? txn->wr.dirtylist->pages_including_loose : txn->wr.writemap_dirty_npages) -
      txn->wr.loose_count - txn->env->options.dp_limit;

  /* production mode */
  if (likely(wanna_spill_npages < 1 && wanna_spill_entries < 1)
#if xMDBX_DEBUG_SPILLING == 1
      /* debug mode: always try to spill if xMDBX_DEBUG_SPILLING == 1 */
      && txn->txnid % 23 > 11
#endif
  )
    return MDBX_SUCCESS;

  return spill_slowpath(txn, m0, wanna_spill_entries, wanna_spill_npages, need);
}
