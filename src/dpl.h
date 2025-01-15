/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025

#pragma once

#include "essentials.h"

static inline size_t dpl_setlen(dpl_t *dl, size_t len) {
  static const page_t dpl_stub_pageE = {INVALID_TXNID,
                                        0,
                                        P_BAD,
                                        {0},
                                        /* pgno */ ~(pgno_t)0};
  assert(dpl_stub_pageE.flags == P_BAD && dpl_stub_pageE.pgno == P_INVALID);
  dl->length = len;
  dl->items[len + 1].ptr = (page_t *)&dpl_stub_pageE;
  dl->items[len + 1].pgno = P_INVALID;
  dl->items[len + 1].npages = 1;
  return len;
}

static inline void dpl_clear(dpl_t *dl) {
  static const page_t dpl_stub_pageB = {INVALID_TXNID,
                                        0,
                                        P_BAD,
                                        {0},
                                        /* pgno */ 0};
  assert(dpl_stub_pageB.flags == P_BAD && dpl_stub_pageB.pgno == 0);
  dl->sorted = dpl_setlen(dl, 0);
  dl->pages_including_loose = 0;
  dl->items[0].ptr = (page_t *)&dpl_stub_pageB;
  dl->items[0].pgno = 0;
  dl->items[0].npages = 1;
  assert(dl->items[0].pgno == 0 && dl->items[dl->length + 1].pgno == P_INVALID);
}

MDBX_INTERNAL int __must_check_result dpl_alloc(MDBX_txn *txn);

MDBX_INTERNAL void dpl_free(MDBX_txn *txn);

MDBX_INTERNAL dpl_t *dpl_reserve(MDBX_txn *txn, size_t size);

MDBX_INTERNAL __noinline dpl_t *dpl_sort_slowpath(const MDBX_txn *txn);

static inline dpl_t *dpl_sort(const MDBX_txn *txn) {
  tASSERT(txn, (txn->flags & MDBX_TXN_RDONLY) == 0);
  tASSERT(txn, (txn->flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);

  dpl_t *dl = txn->wr.dirtylist;
  tASSERT(txn, dl->length <= PAGELIST_LIMIT);
  tASSERT(txn, dl->sorted <= dl->length);
  tASSERT(txn, dl->items[0].pgno == 0 && dl->items[dl->length + 1].pgno == P_INVALID);
  return likely(dl->sorted == dl->length) ? dl : dpl_sort_slowpath(txn);
}

MDBX_INTERNAL __noinline size_t dpl_search(const MDBX_txn *txn, pgno_t pgno);

MDBX_MAYBE_UNUSED MDBX_INTERNAL const page_t *debug_dpl_find(const MDBX_txn *txn, const pgno_t pgno);

MDBX_NOTHROW_PURE_FUNCTION static inline unsigned dpl_npages(const dpl_t *dl, size_t i) {
  assert(0 <= (intptr_t)i && i <= dl->length);
  unsigned n = dl->items[i].npages;
  assert(n == (is_largepage(dl->items[i].ptr) ? dl->items[i].ptr->pages : 1));
  return n;
}

MDBX_NOTHROW_PURE_FUNCTION static inline pgno_t dpl_endpgno(const dpl_t *dl, size_t i) {
  return dpl_npages(dl, i) + dl->items[i].pgno;
}

static inline bool dpl_intersect(const MDBX_txn *txn, pgno_t pgno, size_t npages) {
  tASSERT(txn, (txn->flags & MDBX_TXN_RDONLY) == 0);
  tASSERT(txn, (txn->flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);

  dpl_t *dl = txn->wr.dirtylist;
  tASSERT(txn, dl->sorted == dl->length);
  tASSERT(txn, dl->items[0].pgno == 0 && dl->items[dl->length + 1].pgno == P_INVALID);
  size_t const n = dpl_search(txn, pgno);
  tASSERT(txn, n >= 1 && n <= dl->length + 1);
  tASSERT(txn, pgno <= dl->items[n].pgno);
  tASSERT(txn, pgno > dl->items[n - 1].pgno);
  const bool rc =
      /* intersection with founded */ pgno + npages > dl->items[n].pgno ||
      /* intersection with prev */ dpl_endpgno(dl, n - 1) > pgno;
  if (ASSERT_ENABLED()) {
    bool check = false;
    for (size_t i = 1; i <= dl->length; ++i) {
      const page_t *const dp = dl->items[i].ptr;
      if (!(dp->pgno /* begin */ >= /* end */ pgno + npages || dpl_endpgno(dl, i) /* end */ <= /* begin */ pgno))
        check |= true;
    }
    tASSERT(txn, check == rc);
  }
  return rc;
}

MDBX_NOTHROW_PURE_FUNCTION static inline size_t dpl_exist(const MDBX_txn *txn, pgno_t pgno) {
  tASSERT(txn, (txn->flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);
  dpl_t *dl = txn->wr.dirtylist;
  size_t i = dpl_search(txn, pgno);
  tASSERT(txn, (int)i > 0);
  return (dl->items[i].pgno == pgno) ? i : 0;
}

MDBX_INTERNAL void dpl_remove_ex(const MDBX_txn *txn, size_t i, size_t npages);

static inline void dpl_remove(const MDBX_txn *txn, size_t i) {
  dpl_remove_ex(txn, i, dpl_npages(txn->wr.dirtylist, i));
}

MDBX_INTERNAL int __must_check_result dpl_append(MDBX_txn *txn, pgno_t pgno, page_t *page, size_t npages);

MDBX_MAYBE_UNUSED MDBX_INTERNAL bool dpl_check(MDBX_txn *txn);

MDBX_NOTHROW_PURE_FUNCTION static inline uint32_t dpl_age(const MDBX_txn *txn, size_t i) {
  tASSERT(txn, (txn->flags & (MDBX_TXN_RDONLY | MDBX_WRITEMAP)) == 0);
  const dpl_t *dl = txn->wr.dirtylist;
  assert((intptr_t)i > 0 && i <= dl->length);
  size_t *const ptr = ptr_disp(dl->items[i].ptr, -(ptrdiff_t)sizeof(size_t));
  return txn->wr.dirtylru - (uint32_t)*ptr;
}

MDBX_INTERNAL void dpl_lru_reduce(MDBX_txn *txn);

static inline uint32_t dpl_lru_turn(MDBX_txn *txn) {
  txn->wr.dirtylru += 1;
  if (unlikely(txn->wr.dirtylru > UINT32_MAX / 3) && (txn->flags & MDBX_WRITEMAP) == 0)
    dpl_lru_reduce(txn);
  return txn->wr.dirtylru;
}

MDBX_INTERNAL void dpl_sift(MDBX_txn *const txn, pnl_t pl, const bool spilled);

MDBX_INTERNAL void dpl_release_shadows(MDBX_txn *txn);
