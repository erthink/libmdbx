/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024

#include "internals.h"

typedef struct diff_result {
  ptrdiff_t diff;
  intptr_t level;
  ptrdiff_t root_nkeys;
} diff_t;

/* calculates: r = x - y */
__hot static int cursor_diff(const MDBX_cursor *const __restrict x, const MDBX_cursor *const __restrict y,
                             diff_t *const __restrict r) {
  r->diff = 0;
  r->level = 0;
  r->root_nkeys = 0;

  if (unlikely(x->signature != cur_signature_live))
    return (x->signature == cur_signature_ready4dispose) ? MDBX_EINVAL : MDBX_EBADSIGN;

  if (unlikely(y->signature != cur_signature_live))
    return (y->signature == cur_signature_ready4dispose) ? MDBX_EINVAL : MDBX_EBADSIGN;

  int rc = check_txn(x->txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(x->txn != y->txn))
    return MDBX_BAD_TXN;

  if (unlikely(y->dbi_state != x->dbi_state))
    return MDBX_EINVAL;

  const intptr_t depth = (x->top < y->top) ? x->top : y->top;
  if (unlikely(depth < 0))
    return MDBX_ENODATA;

  r->root_nkeys = page_numkeys(x->pg[0]);
  intptr_t nkeys = r->root_nkeys;
  for (;;) {
    if (unlikely(y->pg[r->level] != x->pg[r->level])) {
      ERROR("Mismatch cursors's pages at %zu level", r->level);
      return MDBX_PROBLEM;
    }
    r->diff = x->ki[r->level] - y->ki[r->level];
    if (r->diff)
      break;
    r->level += 1;
    if (r->level > depth) {
      r->diff = CMP2INT(x->flags & z_eof_hard, y->flags & z_eof_hard);
      return MDBX_SUCCESS;
    }
    nkeys = page_numkeys(x->pg[r->level]);
  }

  while (unlikely(r->diff == 1) && likely(r->level < depth)) {
    r->level += 1;
    /*   DB'PAGEs: 0------------------>MAX
     *
     *    CURSORs:       y < x
     *  STACK[i ]:         |
     *  STACK[+1]:  ...y++N|0++x...
     */
    nkeys = page_numkeys(y->pg[r->level]);
    r->diff = (nkeys - y->ki[r->level]) + x->ki[r->level];
    assert(r->diff > 0);
  }

  while (unlikely(r->diff == -1) && likely(r->level < depth)) {
    r->level += 1;
    /*   DB'PAGEs: 0------------------>MAX
     *
     *    CURSORs:       x < y
     *  STACK[i ]:         |
     *  STACK[+1]:  ...x--N|0--y...
     */
    nkeys = page_numkeys(x->pg[r->level]);
    r->diff = -(nkeys - x->ki[r->level]) - y->ki[r->level];
    assert(r->diff < 0);
  }

  return MDBX_SUCCESS;
}

__hot static ptrdiff_t estimate(const tree_t *tree, diff_t *const __restrict dr) {
  /*        root: branch-page    => scale = leaf-factor * branch-factor^(N-1)
   *     level-1: branch-page(s) => scale = leaf-factor * branch-factor^2
   *     level-2: branch-page(s) => scale = leaf-factor * branch-factor
   *     level-N: branch-page(s) => scale = leaf-factor
   *  leaf-level: leaf-page(s)   => scale = 1
   */
  ptrdiff_t btree_power = (ptrdiff_t)tree->height - 2 - (ptrdiff_t)dr->level;
  if (btree_power < 0)
    return dr->diff;

  ptrdiff_t estimated = (ptrdiff_t)tree->items * dr->diff / (ptrdiff_t)tree->leaf_pages;
  if (btree_power == 0)
    return estimated;

  if (tree->height < 4) {
    assert(dr->level == 0 && btree_power == 1);
    return (ptrdiff_t)tree->items * dr->diff / (ptrdiff_t)dr->root_nkeys;
  }

  /* average_branchpage_fillfactor = total(branch_entries) / branch_pages
     total(branch_entries) = leaf_pages + branch_pages - 1 (root page) */
  const size_t log2_fixedpoint = sizeof(size_t) - 1;
  const size_t half = UINT64_C(1) << (log2_fixedpoint - 1);
  const size_t factor = ((tree->leaf_pages + tree->branch_pages - 1) << log2_fixedpoint) / tree->branch_pages;
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
      if (unlikely(estimated > (ptrdiff_t)tree->items))
        return (ptrdiff_t)tree->items;
      if (unlikely(estimated < -(ptrdiff_t)tree->items))
        return -(ptrdiff_t)tree->items;
      return estimated;
    }
  }
}

__hot int mdbx_estimate_distance(const MDBX_cursor *first, const MDBX_cursor *last, ptrdiff_t *distance_items) {
  if (unlikely(first == nullptr || last == nullptr || distance_items == nullptr))
    return LOG_IFERR(MDBX_EINVAL);

  *distance_items = 0;
  diff_t dr;
  int rc = cursor_diff(last, first, &dr);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  cASSERT(first, dr.diff || inner_pointed(first) == inner_pointed(last));
  if (unlikely(dr.diff == 0) && inner_pointed(first)) {
    first = &first->subcur->cursor;
    last = &last->subcur->cursor;
    rc = cursor_diff(first, last, &dr);
    if (unlikely(rc != MDBX_SUCCESS))
      return LOG_IFERR(rc);
  }

  if (likely(dr.diff != 0))
    *distance_items = estimate(first->tree, &dr);

  return MDBX_SUCCESS;
}

__hot int mdbx_estimate_move(const MDBX_cursor *cursor, MDBX_val *key, MDBX_val *data, MDBX_cursor_op move_op,
                             ptrdiff_t *distance_items) {
  if (unlikely(cursor == nullptr || distance_items == nullptr || move_op == MDBX_GET_CURRENT ||
               move_op == MDBX_GET_MULTIPLE))
    return LOG_IFERR(MDBX_EINVAL);

  if (unlikely(cursor->signature != cur_signature_live))
    return LOG_IFERR((cursor->signature == cur_signature_ready4dispose) ? MDBX_EINVAL : MDBX_EBADSIGN);

  int rc = check_txn(cursor->txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  if (unlikely(!is_pointed(cursor)))
    return LOG_IFERR(MDBX_ENODATA);

  cursor_couple_t next;
  rc = cursor_init(&next.outer, cursor->txn, cursor_dbi(cursor));
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  cursor_cpstk(cursor, &next.outer);
  if (cursor->tree->flags & MDBX_DUPSORT) {
    subcur_t *mx = &container_of(cursor, cursor_couple_t, outer)->inner;
    cursor_cpstk(&mx->cursor, &next.inner.cursor);
  }

  MDBX_val stub_data;
  if (data == nullptr) {
    const unsigned mask = 1 << MDBX_GET_BOTH | 1 << MDBX_GET_BOTH_RANGE | 1 << MDBX_SET_KEY;
    if (unlikely(mask & (1 << move_op)))
      return LOG_IFERR(MDBX_EINVAL);
    stub_data.iov_base = nullptr;
    stub_data.iov_len = 0;
    data = &stub_data;
  }

  MDBX_val stub_key;
  if (key == nullptr) {
    const unsigned mask =
        1 << MDBX_GET_BOTH | 1 << MDBX_GET_BOTH_RANGE | 1 << MDBX_SET_KEY | 1 << MDBX_SET | 1 << MDBX_SET_RANGE;
    if (unlikely(mask & (1 << move_op)))
      return LOG_IFERR(MDBX_EINVAL);
    stub_key.iov_base = nullptr;
    stub_key.iov_len = 0;
    key = &stub_key;
  }

  next.outer.signature = cur_signature_live;
  rc = cursor_ops(&next.outer, key, data, move_op);
  if (unlikely(rc != MDBX_SUCCESS && (rc != MDBX_NOTFOUND || !is_pointed(&next.outer))))
    return LOG_IFERR(rc);

  if (move_op == MDBX_LAST) {
    next.outer.flags |= z_eof_hard;
    next.inner.cursor.flags |= z_eof_hard;
  }
  return mdbx_estimate_distance(cursor, &next.outer, distance_items);
}

__hot int mdbx_estimate_range(const MDBX_txn *txn, MDBX_dbi dbi, const MDBX_val *begin_key, const MDBX_val *begin_data,
                              const MDBX_val *end_key, const MDBX_val *end_data, ptrdiff_t *size_items) {
  int rc = check_txn(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  if (unlikely(!size_items))
    return LOG_IFERR(MDBX_EINVAL);

  if (unlikely(begin_data && (begin_key == nullptr || begin_key == MDBX_EPSILON)))
    return LOG_IFERR(MDBX_EINVAL);

  if (unlikely(end_data && (end_key == nullptr || end_key == MDBX_EPSILON)))
    return LOG_IFERR(MDBX_EINVAL);

  if (unlikely(begin_key == MDBX_EPSILON && end_key == MDBX_EPSILON))
    return LOG_IFERR(MDBX_EINVAL);

  cursor_couple_t begin;
  /* LY: first, initialize cursor to refresh a DB in case it have DB_STALE */
  rc = cursor_init(&begin.outer, txn, dbi);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  if (unlikely(begin.outer.tree->items == 0)) {
    *size_items = 0;
    return MDBX_SUCCESS;
  }

  if (!begin_key) {
    if (unlikely(!end_key)) {
      /* LY: FIRST..LAST case */
      *size_items = (ptrdiff_t)begin.outer.tree->items;
      return MDBX_SUCCESS;
    }
    rc = outer_first(&begin.outer, nullptr, nullptr);
    if (unlikely(end_key == MDBX_EPSILON)) {
      /* LY: FIRST..+epsilon case */
      return LOG_IFERR((rc == MDBX_SUCCESS) ? mdbx_cursor_count(&begin.outer, (size_t *)size_items) : rc);
    }
  } else {
    if (unlikely(begin_key == MDBX_EPSILON)) {
      if (end_key == nullptr) {
        /* LY: -epsilon..LAST case */
        rc = outer_last(&begin.outer, nullptr, nullptr);
        return LOG_IFERR((rc == MDBX_SUCCESS) ? mdbx_cursor_count(&begin.outer, (size_t *)size_items) : rc);
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
        (begin_key == end_key || begin.outer.clc->k.cmp(begin_key, end_key) == 0)) {
      /* LY: single key case */
      rc = cursor_seek(&begin.outer, (MDBX_val *)begin_key, nullptr, MDBX_SET).err;
      if (unlikely(rc != MDBX_SUCCESS)) {
        *size_items = 0;
        return LOG_IFERR((rc == MDBX_NOTFOUND) ? MDBX_SUCCESS : rc);
      }
      *size_items = 1;
      if (inner_pointed(&begin.outer))
        *size_items = (sizeof(*size_items) >= sizeof(begin.inner.nested_tree.items) ||
                       begin.inner.nested_tree.items <= PTRDIFF_MAX)
                          ? (size_t)begin.inner.nested_tree.items
                          : PTRDIFF_MAX;

      return MDBX_SUCCESS;
    } else {
      MDBX_val proxy_key = *begin_key;
      MDBX_val proxy_data = {nullptr, 0};
      if (begin_data)
        proxy_data = *begin_data;
      rc = LOG_IFERR(cursor_seek(&begin.outer, &proxy_key, &proxy_data, MDBX_SET_LOWERBOUND).err);
    }
  }

  if (unlikely(rc != MDBX_SUCCESS)) {
    if (rc != MDBX_NOTFOUND || !is_pointed(&begin.outer))
      return LOG_IFERR(rc);
  }

  cursor_couple_t end;
  rc = cursor_init(&end.outer, txn, dbi);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);
  if (!end_key) {
    rc = outer_last(&end.outer, nullptr, nullptr);
    end.outer.flags |= z_eof_hard;
    end.inner.cursor.flags |= z_eof_hard;
  } else {
    MDBX_val proxy_key = *end_key;
    MDBX_val proxy_data = {nullptr, 0};
    if (end_data)
      proxy_data = *end_data;
    rc = cursor_seek(&end.outer, &proxy_key, &proxy_data, MDBX_SET_LOWERBOUND).err;
  }
  if (unlikely(rc != MDBX_SUCCESS)) {
    if (rc != MDBX_NOTFOUND || !is_pointed(&end.outer))
      return LOG_IFERR(rc);
  }

  rc = mdbx_estimate_distance(&begin.outer, &end.outer, size_items);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);
  assert(*size_items >= -(ptrdiff_t)begin.outer.tree->items && *size_items <= (ptrdiff_t)begin.outer.tree->items);

#if 0 /* LY: Was decided to returns as-is (i.e. negative) the estimation                                               \
       * results for an inverted ranges. */

  /* Commit 8ddfd1f34ad7cf7a3c4aa75d2e248ca7e639ed63
     Change-Id: If59eccf7311123ab6384c4b93f9b1fed5a0a10d1 */

  if (*size_items < 0) {
    /* LY: inverted range case */
    *size_items += (ptrdiff_t)begin.outer.tree->items;
  } else if (*size_items == 0 && begin_key && end_key) {
    int cmp = begin.outer.kvx->cmp(&origin_begin_key, &origin_end_key);
    if (cmp == 0 && cursor_pointed(begin.inner.cursor.flags) &&
        begin_data && end_data)
      cmp = begin.outer.kvx->v.cmp(&origin_begin_data, &origin_end_data);
    if (cmp > 0) {
      /* LY: inverted range case with empty scope */
      *size_items = (ptrdiff_t)begin.outer.tree->items;
    }
  }
  assert(*size_items >= 0 &&
         *size_items <= (ptrdiff_t)begin.outer.tree->items);
#endif

  return MDBX_SUCCESS;
}
