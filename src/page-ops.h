/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025

#pragma once

#include "essentials.h"

MDBX_INTERNAL int __must_check_result tree_search_finalize(MDBX_cursor *mc, const MDBX_val *key, int flags);
MDBX_INTERNAL int tree_search_lowest(MDBX_cursor *mc);

enum page_search_flags {
  Z_MODIFY = 1,
  Z_ROOTONLY = 2,
  Z_FIRST = 4,
  Z_LAST = 8,
};
MDBX_INTERNAL int __must_check_result tree_search(MDBX_cursor *mc, const MDBX_val *key, int flags);

#define MDBX_SPLIT_REPLACE MDBX_APPENDDUP /* newkey is not new */
MDBX_INTERNAL int __must_check_result page_split(MDBX_cursor *mc, const MDBX_val *const newkey, MDBX_val *const newdata,
                                                 pgno_t newpgno, const unsigned naf);

/*----------------------------------------------------------------------------*/

MDBX_INTERNAL int MDBX_PRINTF_ARGS(2, 3) bad_page(const page_t *mp, const char *fmt, ...);

MDBX_INTERNAL void MDBX_PRINTF_ARGS(2, 3) poor_page(const page_t *mp, const char *fmt, ...);

MDBX_NOTHROW_PURE_FUNCTION static inline bool is_frozen(const MDBX_txn *txn, const page_t *mp) {
  return mp->txnid < txn->txnid;
}

MDBX_NOTHROW_PURE_FUNCTION static inline bool is_spilled(const MDBX_txn *txn, const page_t *mp) {
  return mp->txnid == txn->txnid;
}

MDBX_NOTHROW_PURE_FUNCTION static inline bool is_shadowed(const MDBX_txn *txn, const page_t *mp) {
  return mp->txnid > txn->txnid;
}

MDBX_MAYBE_UNUSED MDBX_NOTHROW_PURE_FUNCTION static inline bool is_correct(const MDBX_txn *txn, const page_t *mp) {
  return mp->txnid <= txn->front_txnid;
}

MDBX_NOTHROW_PURE_FUNCTION static inline bool is_modifable(const MDBX_txn *txn, const page_t *mp) {
  return mp->txnid == txn->front_txnid;
}

MDBX_INTERNAL int __must_check_result page_check(const MDBX_cursor *const mc, const page_t *const mp);

MDBX_INTERNAL pgr_t page_get_any(const MDBX_cursor *const mc, const pgno_t pgno, const txnid_t front);

MDBX_INTERNAL pgr_t page_get_three(const MDBX_cursor *const mc, const pgno_t pgno, const txnid_t front);

MDBX_INTERNAL pgr_t page_get_large(const MDBX_cursor *const mc, const pgno_t pgno, const txnid_t front);

static inline int __must_check_result page_get(const MDBX_cursor *mc, const pgno_t pgno, page_t **mp,
                                               const txnid_t front) {
  pgr_t ret = page_get_three(mc, pgno, front);
  *mp = ret.page;
  return ret.err;
}

/*----------------------------------------------------------------------------*/

MDBX_INTERNAL int __must_check_result page_dirty(MDBX_txn *txn, page_t *mp, size_t npages);
MDBX_INTERNAL pgr_t page_new(MDBX_cursor *mc, const unsigned flags);
MDBX_INTERNAL pgr_t page_new_large(MDBX_cursor *mc, const size_t npages);
MDBX_INTERNAL int page_touch_modifable(MDBX_txn *txn, const page_t *const mp);
MDBX_INTERNAL int page_touch_unmodifable(MDBX_txn *txn, MDBX_cursor *mc, const page_t *const mp);

static inline int page_touch(MDBX_cursor *mc) {
  page_t *const mp = mc->pg[mc->top];
  MDBX_txn *txn = mc->txn;

  tASSERT(txn, mc->txn->flags & MDBX_TXN_DIRTY);
  tASSERT(txn, F_ISSET(*cursor_dbi_state(mc), DBI_LINDO | DBI_VALID | DBI_DIRTY));
  tASSERT(txn, !is_largepage(mp));
  if (ASSERT_ENABLED()) {
    if (mc->flags & z_inner) {
      subcur_t *mx = container_of(mc->tree, subcur_t, nested_tree);
      cursor_couple_t *couple = container_of(mx, cursor_couple_t, inner);
      tASSERT(txn, mc->tree == &couple->outer.subcur->nested_tree);
      tASSERT(txn, &mc->clc->k == &couple->outer.clc->v);
      tASSERT(txn, *couple->outer.dbi_state & DBI_DIRTY);
    }
    tASSERT(txn, dpl_check(txn));
  }

  if (is_modifable(txn, mp)) {
    if (!txn->wr.dirtylist) {
      tASSERT(txn, (txn->flags & MDBX_WRITEMAP) && !MDBX_AVOID_MSYNC);
      return MDBX_SUCCESS;
    }
    return is_subpage(mp) ? MDBX_SUCCESS : page_touch_modifable(txn, mp);
  }
  return page_touch_unmodifable(txn, mc, mp);
}

MDBX_INTERNAL void page_copy(page_t *const dst, const page_t *const src, const size_t size);
MDBX_INTERNAL pgr_t __must_check_result page_unspill(MDBX_txn *const txn, const page_t *const mp);

MDBX_INTERNAL page_t *page_shadow_alloc(MDBX_txn *txn, size_t num);

MDBX_INTERNAL void page_shadow_release(MDBX_env *env, page_t *dp, size_t npages);

MDBX_INTERNAL int page_retire_ex(MDBX_cursor *mc, const pgno_t pgno, page_t *mp /* maybe null */,
                                 unsigned pageflags /* maybe unknown/zero */);

static inline int page_retire(MDBX_cursor *mc, page_t *mp) { return page_retire_ex(mc, mp->pgno, mp, mp->flags); }

static inline void page_wash(MDBX_txn *txn, size_t di, page_t *const mp, const size_t npages) {
  tASSERT(txn, (txn->flags & MDBX_TXN_RDONLY) == 0);
  mp->txnid = INVALID_TXNID;
  mp->flags = P_BAD;

  if (txn->wr.dirtylist) {
    tASSERT(txn, (txn->flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);
    tASSERT(txn, MDBX_AVOID_MSYNC || (di && txn->wr.dirtylist->items[di].ptr == mp));
    if (!MDBX_AVOID_MSYNC || di) {
      dpl_remove_ex(txn, di, npages);
      txn->wr.dirtyroom++;
      tASSERT(txn, txn->wr.dirtyroom + txn->wr.dirtylist->length ==
                       (txn->parent ? txn->parent->wr.dirtyroom : txn->env->options.dp_limit));
      if (!MDBX_AVOID_MSYNC || !(txn->flags & MDBX_WRITEMAP)) {
        page_shadow_release(txn->env, mp, npages);
        return;
      }
    }
  } else {
    tASSERT(txn, (txn->flags & MDBX_WRITEMAP) && !MDBX_AVOID_MSYNC && !di);
    txn->wr.writemap_dirty_npages -= (txn->wr.writemap_dirty_npages > npages) ? npages : txn->wr.writemap_dirty_npages;
  }
  VALGRIND_MAKE_MEM_UNDEFINED(mp, PAGEHDRSZ);
  VALGRIND_MAKE_MEM_NOACCESS(page2payload(mp), pgno2bytes(txn->env, npages) - PAGEHDRSZ);
  MDBX_ASAN_POISON_MEMORY_REGION(page2payload(mp), pgno2bytes(txn->env, npages) - PAGEHDRSZ);
}

MDBX_INTERNAL size_t page_subleaf2_reserve(const MDBX_env *env, size_t host_page_room, size_t subpage_len,
                                           size_t item_len);

#define page_next(mp) (*(page_t **)ptr_disp((mp)->entries, sizeof(void *) - sizeof(uint32_t)))
