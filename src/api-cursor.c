/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024

#include "internals.h"

MDBX_cursor *mdbx_cursor_create(void *context) {
  cursor_couple_t *couple = osal_calloc(1, sizeof(cursor_couple_t));
  if (unlikely(!couple))
    return nullptr;

  VALGRIND_MAKE_MEM_UNDEFINED(couple, sizeof(cursor_couple_t));
  couple->outer.signature = cur_signature_ready4dispose;
  couple->outer.next = &couple->outer;
  couple->userctx = context;
  couple->outer.top_and_flags = z_poor_mark;
  couple->inner.cursor.top_and_flags = z_poor_mark | z_inner;
  VALGRIND_MAKE_MEM_DEFINED(&couple->outer.backup, sizeof(couple->outer.backup));
  VALGRIND_MAKE_MEM_DEFINED(&couple->outer.tree, sizeof(couple->outer.tree));
  VALGRIND_MAKE_MEM_DEFINED(&couple->outer.clc, sizeof(couple->outer.clc));
  VALGRIND_MAKE_MEM_DEFINED(&couple->outer.dbi_state, sizeof(couple->outer.dbi_state));
  VALGRIND_MAKE_MEM_DEFINED(&couple->outer.subcur, sizeof(couple->outer.subcur));
  VALGRIND_MAKE_MEM_DEFINED(&couple->outer.txn, sizeof(couple->outer.txn));
  return &couple->outer;
}

int mdbx_cursor_renew(const MDBX_txn *txn, MDBX_cursor *mc) {
  return likely(mc) ? mdbx_cursor_bind(txn, mc, (kvx_t *)mc->clc - txn->env->kvs) : LOG_IFERR(MDBX_EINVAL);
}

int mdbx_cursor_reset(MDBX_cursor *mc) {
  if (unlikely(!mc))
    return LOG_IFERR(MDBX_EINVAL);

  if (unlikely(mc->signature != cur_signature_ready4dispose && mc->signature != cur_signature_live))
    return LOG_IFERR(MDBX_EBADSIGN);

  cursor_couple_t *couple = (cursor_couple_t *)mc;
  couple->outer.top_and_flags = z_poor_mark;
  couple->inner.cursor.top_and_flags = z_poor_mark | z_inner;
  return MDBX_SUCCESS;
}

int mdbx_cursor_bind(const MDBX_txn *txn, MDBX_cursor *mc, MDBX_dbi dbi) {
  if (unlikely(!mc))
    return LOG_IFERR(MDBX_EINVAL);

  if (unlikely(mc->signature != cur_signature_ready4dispose && mc->signature != cur_signature_live))
    return LOG_IFERR(MDBX_EBADSIGN);

  int rc = check_txn(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  rc = dbi_check(txn, dbi);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  if (unlikely(dbi == FREE_DBI && !(txn->flags & MDBX_TXN_RDONLY)))
    return LOG_IFERR(MDBX_EACCESS);

  if (unlikely(mc->backup)) /* Cursor from parent transaction */ {
    cASSERT(mc, mc->signature == cur_signature_live);
    if (unlikely(cursor_dbi(mc) != dbi ||
                 /* paranoia */ mc->signature != cur_signature_live || mc->txn != txn))
      return LOG_IFERR(MDBX_EINVAL);

    cASSERT(mc, mc->tree == &txn->dbs[dbi]);
    cASSERT(mc, mc->clc == &txn->env->kvs[dbi].clc);
    cASSERT(mc, cursor_dbi(mc) == dbi);
    return likely(cursor_dbi(mc) == dbi &&
                  /* paranoia */ mc->signature == cur_signature_live && mc->txn == txn)
               ? MDBX_SUCCESS
               : LOG_IFERR(MDBX_EINVAL) /* Disallow change DBI in nested
                                           transactions */
        ;
  }

  if (mc->signature == cur_signature_live) {
    rc = mdbx_cursor_unbind(mc);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }
  cASSERT(mc, mc->next == mc);

  rc = cursor_init(mc, txn, dbi);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  mc->next = txn->cursors[dbi];
  txn->cursors[dbi] = mc;
  return MDBX_SUCCESS;
}

int mdbx_cursor_unbind(MDBX_cursor *mc) {
  if (unlikely(!mc))
    return LOG_IFERR(MDBX_EINVAL);

  if (unlikely(mc->signature != cur_signature_live))
    return (mc->signature == cur_signature_ready4dispose) ? MDBX_SUCCESS : LOG_IFERR(MDBX_EBADSIGN);

  if (unlikely(mc->backup)) /* Cursor from parent transaction */
    return LOG_IFERR(MDBX_EINVAL);

  eASSERT(nullptr, mc->txn && mc->txn->signature == txn_signature);
  cASSERT(mc, mc->signature == cur_signature_live);
  cASSERT(mc, !mc->backup);
  if (unlikely(!mc->txn || mc->txn->signature != txn_signature)) {
    ERROR("Wrong cursor's transaction %p 0x%x", __Wpedantic_format_voidptr(mc->txn), mc->txn ? mc->txn->signature : 0);
    return LOG_IFERR(MDBX_PROBLEM);
  }
  if (mc->next != mc) {
    const size_t dbi = (kvx_t *)mc->clc - mc->txn->env->kvs;
    cASSERT(mc, cursor_dbi(mc) == dbi);
    cASSERT(mc, dbi < mc->txn->n_dbi);
    if (dbi < mc->txn->n_dbi) {
      MDBX_cursor **prev = &mc->txn->cursors[dbi];
      while (*prev && *prev != mc)
        prev = &(*prev)->next;
      cASSERT(mc, *prev == mc);
      *prev = mc->next;
    }
    mc->next = mc;
  }
  mc->signature = cur_signature_ready4dispose;
  mc->flags = 0;
  return MDBX_SUCCESS;
}

int mdbx_cursor_open(const MDBX_txn *txn, MDBX_dbi dbi, MDBX_cursor **ret) {
  if (unlikely(!ret))
    return LOG_IFERR(MDBX_EINVAL);
  *ret = nullptr;

  MDBX_cursor *const mc = mdbx_cursor_create(nullptr);
  if (unlikely(!mc))
    return LOG_IFERR(MDBX_ENOMEM);

  int rc = mdbx_cursor_bind(txn, mc, dbi);
  if (unlikely(rc != MDBX_SUCCESS)) {
    mdbx_cursor_close(mc);
    return LOG_IFERR(rc);
  }

  *ret = mc;
  return MDBX_SUCCESS;
}

void mdbx_cursor_close(MDBX_cursor *mc) {
  if (likely(mc)) {
    ENSURE(nullptr, mc->signature == cur_signature_live || mc->signature == cur_signature_ready4dispose);
    MDBX_txn *const txn = mc->txn;
    if (!mc->backup) {
      mc->txn = nullptr;
      /* Unlink from txn, if tracked. */
      if (mc->next != mc) {
        ENSURE(txn->env, check_txn(txn, 0) == MDBX_SUCCESS);
        const size_t dbi = (kvx_t *)mc->clc - txn->env->kvs;
        tASSERT(txn, dbi < txn->n_dbi);
        if (dbi < txn->n_dbi) {
          MDBX_cursor **prev = &txn->cursors[dbi];
          while (*prev && *prev != mc)
            prev = &(*prev)->next;
          tASSERT(txn, *prev == mc);
          *prev = mc->next;
        }
        mc->next = mc;
      }
      mc->signature = 0;
      osal_free(mc);
    } else {
      /* Cursor closed before nested txn ends */
      tASSERT(txn, mc->signature == cur_signature_live);
      ENSURE(txn->env, check_txn_rw(txn, 0) == MDBX_SUCCESS);
      mc->signature = cur_signature_wait4eot;
    }
  }
}

int mdbx_cursor_copy(const MDBX_cursor *src, MDBX_cursor *dest) {
  if (unlikely(!src))
    return LOG_IFERR(MDBX_EINVAL);
  if (unlikely(src->signature != cur_signature_live))
    return LOG_IFERR((src->signature == cur_signature_ready4dispose) ? MDBX_EINVAL : MDBX_EBADSIGN);

  int rc = mdbx_cursor_bind(src->txn, dest, cursor_dbi(src));
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  assert(dest->tree == src->tree);
  assert(cursor_dbi(dest) == cursor_dbi(src));
again:
  assert(dest->clc == src->clc);
  assert(dest->txn == src->txn);
  dest->top_and_flags = src->top_and_flags;
  for (intptr_t i = 0; i <= src->top; ++i) {
    dest->ki[i] = src->ki[i];
    dest->pg[i] = src->pg[i];
  }

  if (src->subcur) {
    dest->subcur->nested_tree = src->subcur->nested_tree;
    src = &src->subcur->cursor;
    dest = &dest->subcur->cursor;
    goto again;
  }

  return MDBX_SUCCESS;
}

int mdbx_txn_release_all_cursors(const MDBX_txn *txn, bool unbind) {
  int rc = check_txn(txn, MDBX_TXN_FINISHED | MDBX_TXN_HAS_CHILD);
  if (likely(rc == MDBX_SUCCESS)) {
    TXN_FOREACH_DBI_FROM(txn, i, MAIN_DBI) {
      while (txn->cursors[i]) {
        MDBX_cursor *mc = txn->cursors[i];
        ENSURE(nullptr, mc->signature == cur_signature_live && (mc->next != mc) && !mc->backup);
        rc = likely(rc < INT_MAX) ? rc + 1 : rc;
        txn->cursors[i] = mc->next;
        mc->next = mc;
        if (unbind) {
          mc->signature = cur_signature_ready4dispose;
          mc->flags = 0;
        } else {
          mc->signature = 0;
          osal_free(mc);
        }
      }
    }
  } else {
    eASSERT(nullptr, rc < 0);
    LOG_IFERR(rc);
  }
  return rc;
}

int mdbx_cursor_compare(const MDBX_cursor *l, const MDBX_cursor *r, bool ignore_multival) {
  const int incomparable = INT16_MAX + 1;
  if (unlikely(!l))
    return r ? -incomparable * 9 : 0;
  else if (unlikely(!r))
    return incomparable * 9;

  if (unlikely(l->signature != cur_signature_live))
    return (r->signature == cur_signature_live) ? -incomparable * 8 : 0;
  if (unlikely(r->signature != cur_signature_live))
    return (l->signature == cur_signature_live) ? incomparable * 8 : 0;

  if (unlikely(l->clc != r->clc)) {
    if (l->txn->env != r->txn->env)
      return (l->txn->env > r->txn->env) ? incomparable * 7 : -incomparable * 7;
    if (l->txn->txnid != r->txn->txnid)
      return (l->txn->txnid > r->txn->txnid) ? incomparable * 6 : -incomparable * 6;
    return (l->clc > r->clc) ? incomparable * 5 : -incomparable * 5;
  }
  assert(cursor_dbi(l) == cursor_dbi(r));

  int diff = is_pointed(l) - is_pointed(r);
  if (unlikely(diff))
    return (diff > 0) ? incomparable * 4 : -incomparable * 4;
  if (unlikely(!is_pointed(l)))
    return 0;

  intptr_t detent = (l->top <= r->top) ? l->top : r->top;
  for (intptr_t i = 0; i <= detent; ++i) {
    diff = l->ki[i] - r->ki[i];
    if (diff)
      return diff;
  }
  if (unlikely(l->top != r->top))
    return (l->top > r->top) ? incomparable * 3 : -incomparable * 3;

  assert((l->subcur != nullptr) == (r->subcur != nullptr));
  if (unlikely((l->subcur != nullptr) != (r->subcur != nullptr)))
    return l->subcur ? incomparable * 2 : -incomparable * 2;
  if (ignore_multival || !l->subcur)
    return 0;

#if MDBX_DEBUG
  if (is_pointed(&l->subcur->cursor)) {
    const page_t *mp = l->pg[l->top];
    const node_t *node = page_node(mp, l->ki[l->top]);
    assert(node_flags(node) & N_DUP);
  }
  if (is_pointed(&r->subcur->cursor)) {
    const page_t *mp = r->pg[r->top];
    const node_t *node = page_node(mp, r->ki[r->top]);
    assert(node_flags(node) & N_DUP);
  }
#endif /* MDBX_DEBUG */

  l = &l->subcur->cursor;
  r = &r->subcur->cursor;
  diff = is_pointed(l) - is_pointed(r);
  if (unlikely(diff))
    return (diff > 0) ? incomparable * 2 : -incomparable * 2;
  if (unlikely(!is_pointed(l)))
    return 0;

  detent = (l->top <= r->top) ? l->top : r->top;
  for (intptr_t i = 0; i <= detent; ++i) {
    diff = l->ki[i] - r->ki[i];
    if (diff)
      return diff;
  }
  if (unlikely(l->top != r->top))
    return (l->top > r->top) ? incomparable : -incomparable;

  return (l->flags & z_eof_hard) - (r->flags & z_eof_hard);
}

/* Return the count of duplicate data items for the current key */
int mdbx_cursor_count(const MDBX_cursor *mc, size_t *countp) {
  if (unlikely(mc == nullptr))
    return LOG_IFERR(MDBX_EINVAL);

  if (unlikely(mc->signature != cur_signature_live))
    return LOG_IFERR((mc->signature == cur_signature_ready4dispose) ? MDBX_EINVAL : MDBX_EBADSIGN);

  int rc = check_txn(mc->txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  if (unlikely(countp == nullptr))
    return LOG_IFERR(MDBX_EINVAL);

  if ((*countp = is_filled(mc)) > 0) {
    if (!inner_hollow(mc)) {
      const page_t *mp = mc->pg[mc->top];
      const node_t *node = page_node(mp, mc->ki[mc->top]);
      cASSERT(mc, node_flags(node) & N_DUP);
      *countp =
          unlikely(mc->subcur->nested_tree.items > PTRDIFF_MAX) ? PTRDIFF_MAX : (size_t)mc->subcur->nested_tree.items;
    }
  }
  return MDBX_SUCCESS;
}

int mdbx_cursor_on_first(const MDBX_cursor *mc) {
  if (unlikely(mc == nullptr))
    return LOG_IFERR(MDBX_EINVAL);

  if (unlikely(mc->signature != cur_signature_live))
    return LOG_IFERR((mc->signature == cur_signature_ready4dispose) ? MDBX_EINVAL : MDBX_EBADSIGN);

  for (intptr_t i = 0; i <= mc->top; ++i) {
    if (mc->ki[i])
      return MDBX_RESULT_FALSE;
  }

  return MDBX_RESULT_TRUE;
}

int mdbx_cursor_on_first_dup(const MDBX_cursor *mc) {
  if (unlikely(mc == nullptr))
    return LOG_IFERR(MDBX_EINVAL);

  if (unlikely(mc->signature != cur_signature_live))
    return LOG_IFERR((mc->signature == cur_signature_ready4dispose) ? MDBX_EINVAL : MDBX_EBADSIGN);

  if (is_filled(mc) && mc->subcur) {
    mc = &mc->subcur->cursor;
    for (intptr_t i = 0; i <= mc->top; ++i) {
      if (mc->ki[i])
        return MDBX_RESULT_FALSE;
    }
  }

  return MDBX_RESULT_TRUE;
}

int mdbx_cursor_on_last(const MDBX_cursor *mc) {
  if (unlikely(mc == nullptr))
    return LOG_IFERR(MDBX_EINVAL);

  if (unlikely(mc->signature != cur_signature_live))
    return LOG_IFERR((mc->signature == cur_signature_ready4dispose) ? MDBX_EINVAL : MDBX_EBADSIGN);

  for (intptr_t i = 0; i <= mc->top; ++i) {
    size_t nkeys = page_numkeys(mc->pg[i]);
    if (mc->ki[i] < nkeys - 1)
      return MDBX_RESULT_FALSE;
  }

  return MDBX_RESULT_TRUE;
}

int mdbx_cursor_on_last_dup(const MDBX_cursor *mc) {
  if (unlikely(mc == nullptr))
    return LOG_IFERR(MDBX_EINVAL);

  if (unlikely(mc->signature != cur_signature_live))
    return LOG_IFERR((mc->signature == cur_signature_ready4dispose) ? MDBX_EINVAL : MDBX_EBADSIGN);

  if (is_filled(mc) && mc->subcur) {
    mc = &mc->subcur->cursor;
    for (intptr_t i = 0; i <= mc->top; ++i) {
      size_t nkeys = page_numkeys(mc->pg[i]);
      if (mc->ki[i] < nkeys - 1)
        return MDBX_RESULT_FALSE;
    }
  }

  return MDBX_RESULT_TRUE;
}

int mdbx_cursor_eof(const MDBX_cursor *mc) {
  if (unlikely(mc == nullptr))
    return LOG_IFERR(MDBX_EINVAL);

  if (unlikely(mc->signature != cur_signature_live))
    return LOG_IFERR((mc->signature == cur_signature_ready4dispose) ? MDBX_EINVAL : MDBX_EBADSIGN);

  return is_eof(mc) ? MDBX_RESULT_TRUE : MDBX_RESULT_FALSE;
}

int mdbx_cursor_get(MDBX_cursor *mc, MDBX_val *key, MDBX_val *data, MDBX_cursor_op op) {
  if (unlikely(mc == nullptr))
    return LOG_IFERR(MDBX_EINVAL);

  if (unlikely(mc->signature != cur_signature_live))
    return LOG_IFERR((mc->signature == cur_signature_ready4dispose) ? MDBX_EINVAL : MDBX_EBADSIGN);

  int rc = check_txn(mc->txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  if (unlikely(cursor_dbi_changed(mc)))
    return LOG_IFERR(MDBX_BAD_DBI);

  return LOG_IFERR(cursor_ops(mc, key, data, op));
}

__hot static int scan_confinue(MDBX_cursor *mc, MDBX_predicate_func *predicate, void *context, void *arg, MDBX_val *key,
                               MDBX_val *value, MDBX_cursor_op turn_op) {
  int rc;
  switch (turn_op) {
  case MDBX_NEXT:
  case MDBX_NEXT_NODUP:
    for (;;) {
      rc = predicate(context, key, value, arg);
      if (rc != MDBX_RESULT_FALSE)
        return rc;
      rc = outer_next(mc, key, value, turn_op);
      if (unlikely(rc != MDBX_SUCCESS))
        return (rc == MDBX_NOTFOUND) ? MDBX_RESULT_FALSE : rc;
    }

  case MDBX_PREV:
  case MDBX_PREV_NODUP:
    for (;;) {
      rc = predicate(context, key, value, arg);
      if (rc != MDBX_RESULT_FALSE)
        return rc;
      rc = outer_prev(mc, key, value, turn_op);
      if (unlikely(rc != MDBX_SUCCESS))
        return (rc == MDBX_NOTFOUND) ? MDBX_RESULT_FALSE : rc;
    }

  case MDBX_NEXT_DUP:
    if (mc->subcur)
      for (;;) {
        rc = predicate(context, key, value, arg);
        if (rc != MDBX_RESULT_FALSE)
          return rc;
        rc = inner_next(&mc->subcur->cursor, value);
        if (unlikely(rc != MDBX_SUCCESS))
          return (rc == MDBX_NOTFOUND) ? MDBX_RESULT_FALSE : rc;
      }
    return MDBX_NOTFOUND;

  case MDBX_PREV_DUP:
    if (mc->subcur)
      for (;;) {
        rc = predicate(context, key, value, arg);
        if (rc != MDBX_RESULT_FALSE)
          return rc;
        rc = inner_prev(&mc->subcur->cursor, value);
        if (unlikely(rc != MDBX_SUCCESS))
          return (rc == MDBX_NOTFOUND) ? MDBX_RESULT_FALSE : rc;
      }
    return MDBX_NOTFOUND;

  default:
    for (;;) {
      rc = predicate(context, key, value, arg);
      if (rc != MDBX_RESULT_FALSE)
        return rc;
      rc = cursor_ops(mc, key, value, turn_op);
      if (unlikely(rc != MDBX_SUCCESS))
        return (rc == MDBX_NOTFOUND) ? MDBX_RESULT_FALSE : rc;
    }
  }
}

int mdbx_cursor_scan(MDBX_cursor *mc, MDBX_predicate_func *predicate, void *context, MDBX_cursor_op start_op,
                     MDBX_cursor_op turn_op, void *arg) {
  if (unlikely(!predicate))
    return LOG_IFERR(MDBX_EINVAL);

  const unsigned valid_start_mask = 1 << MDBX_FIRST | 1 << MDBX_FIRST_DUP | 1 << MDBX_LAST | 1 << MDBX_LAST_DUP |
                                    1 << MDBX_GET_CURRENT | 1 << MDBX_GET_MULTIPLE;
  if (unlikely(start_op > 30 || ((1 << start_op) & valid_start_mask) == 0))
    return LOG_IFERR(MDBX_EINVAL);

  const unsigned valid_turn_mask = 1 << MDBX_NEXT | 1 << MDBX_NEXT_DUP | 1 << MDBX_NEXT_NODUP | 1 << MDBX_PREV |
                                   1 << MDBX_PREV_DUP | 1 << MDBX_PREV_NODUP | 1 << MDBX_NEXT_MULTIPLE |
                                   1 << MDBX_PREV_MULTIPLE;
  if (unlikely(turn_op > 30 || ((1 << turn_op) & valid_turn_mask) == 0))
    return LOG_IFERR(MDBX_EINVAL);

  MDBX_val key = {nullptr, 0}, value = {nullptr, 0};
  int rc = mdbx_cursor_get(mc, &key, &value, start_op);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);
  return LOG_IFERR(scan_confinue(mc, predicate, context, arg, &key, &value, turn_op));
}

int mdbx_cursor_scan_from(MDBX_cursor *mc, MDBX_predicate_func *predicate, void *context, MDBX_cursor_op from_op,
                          MDBX_val *key, MDBX_val *value, MDBX_cursor_op turn_op, void *arg) {
  if (unlikely(!predicate || !key))
    return LOG_IFERR(MDBX_EINVAL);

  const unsigned valid_start_mask = 1 << MDBX_GET_BOTH | 1 << MDBX_GET_BOTH_RANGE | 1 << MDBX_SET_KEY |
                                    1 << MDBX_GET_MULTIPLE | 1 << MDBX_SET_LOWERBOUND | 1 << MDBX_SET_UPPERBOUND;
  if (unlikely(from_op < MDBX_TO_KEY_LESSER_THAN && ((1 << from_op) & valid_start_mask) == 0))
    return LOG_IFERR(MDBX_EINVAL);

  const unsigned valid_turn_mask = 1 << MDBX_NEXT | 1 << MDBX_NEXT_DUP | 1 << MDBX_NEXT_NODUP | 1 << MDBX_PREV |
                                   1 << MDBX_PREV_DUP | 1 << MDBX_PREV_NODUP | 1 << MDBX_NEXT_MULTIPLE |
                                   1 << MDBX_PREV_MULTIPLE;
  if (unlikely(turn_op > 30 || ((1 << turn_op) & valid_turn_mask) == 0))
    return LOG_IFERR(MDBX_EINVAL);

  int rc = mdbx_cursor_get(mc, key, value, from_op);
  if (unlikely(MDBX_IS_ERROR(rc)))
    return LOG_IFERR(rc);

  cASSERT(mc, key != nullptr);
  MDBX_val stub;
  if (!value) {
    value = &stub;
    rc = cursor_ops(mc, key, value, MDBX_GET_CURRENT);
    if (unlikely(rc != MDBX_SUCCESS))
      return LOG_IFERR(rc);
  }
  return LOG_IFERR(scan_confinue(mc, predicate, context, arg, key, value, turn_op));
}

int mdbx_cursor_get_batch(MDBX_cursor *mc, size_t *count, MDBX_val *pairs, size_t limit, MDBX_cursor_op op) {
  if (unlikely(!count))
    return LOG_IFERR(MDBX_EINVAL);

  *count = 0;
  if (unlikely(mc == nullptr || limit < 4 || limit > INTPTR_MAX - 2))
    return LOG_IFERR(MDBX_EINVAL);

  if (unlikely(mc->signature != cur_signature_live))
    return LOG_IFERR((mc->signature == cur_signature_ready4dispose) ? MDBX_EINVAL : MDBX_EBADSIGN);

  int rc = check_txn(mc->txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  if (unlikely(cursor_dbi_changed(mc)))
    return LOG_IFERR(MDBX_BAD_DBI);

  if (unlikely(mc->subcur))
    return LOG_IFERR(MDBX_INCOMPATIBLE) /* must be a non-dupsort table */;

  switch (op) {
  case MDBX_NEXT:
    if (unlikely(is_eof(mc)))
      return LOG_IFERR(is_pointed(mc) ? MDBX_NOTFOUND : MDBX_ENODATA);
    break;

  case MDBX_FIRST:
    if (!is_filled(mc)) {
      rc = outer_first(mc, nullptr, nullptr);
      if (unlikely(rc != MDBX_SUCCESS))
        return LOG_IFERR(rc);
    }
    break;

  default:
    DEBUG("unhandled/unimplemented cursor operation %u", op);
    return LOG_IFERR(MDBX_EINVAL);
  }

  const page_t *mp = mc->pg[mc->top];
  size_t nkeys = page_numkeys(mp);
  size_t ki = mc->ki[mc->top];
  size_t n = 0;
  while (n + 2 <= limit) {
    cASSERT(mc, ki < nkeys);
    if (unlikely(ki >= nkeys))
      goto sibling;

    const node_t *leaf = page_node(mp, ki);
    pairs[n] = get_key(leaf);
    rc = node_read(mc, leaf, &pairs[n + 1], mp);
    if (unlikely(rc != MDBX_SUCCESS))
      goto bailout;

    n += 2;
    if (++ki == nkeys) {
    sibling:
      rc = cursor_sibling_right(mc);
      if (rc != MDBX_SUCCESS) {
        if (rc == MDBX_NOTFOUND)
          rc = MDBX_RESULT_TRUE;
        goto bailout;
      }

      mp = mc->pg[mc->top];
      DEBUG("next page is %" PRIaPGNO ", key index %u", mp->pgno, mc->ki[mc->top]);
      if (!MDBX_DISABLE_VALIDATION && unlikely(!check_leaf_type(mc, mp))) {
        ERROR("unexpected leaf-page #%" PRIaPGNO " type 0x%x seen by cursor", mp->pgno, mp->flags);
        rc = MDBX_CORRUPTED;
        goto bailout;
      }
      nkeys = page_numkeys(mp);
      ki = 0;
    }
  }
  mc->ki[mc->top] = (indx_t)ki;

bailout:
  *count = n;
  return LOG_IFERR(rc);
}

/*----------------------------------------------------------------------------*/

int mdbx_cursor_set_userctx(MDBX_cursor *mc, void *ctx) {
  if (unlikely(!mc))
    return LOG_IFERR(MDBX_EINVAL);

  if (unlikely(mc->signature != cur_signature_ready4dispose && mc->signature != cur_signature_live))
    return LOG_IFERR(MDBX_EBADSIGN);

  cursor_couple_t *couple = container_of(mc, cursor_couple_t, outer);
  couple->userctx = ctx;
  return MDBX_SUCCESS;
}

void *mdbx_cursor_get_userctx(const MDBX_cursor *mc) {
  if (unlikely(!mc))
    return nullptr;

  if (unlikely(mc->signature != cur_signature_ready4dispose && mc->signature != cur_signature_live))
    return nullptr;

  cursor_couple_t *couple = container_of(mc, cursor_couple_t, outer);
  return couple->userctx;
}

MDBX_txn *mdbx_cursor_txn(const MDBX_cursor *mc) {
  if (unlikely(!mc || mc->signature != cur_signature_live))
    return nullptr;
  MDBX_txn *txn = mc->txn;
  if (unlikely(!txn || txn->signature != txn_signature))
    return nullptr;
  if (unlikely(txn->flags & MDBX_TXN_FINISHED))
    return nullptr;
  return txn;
}

MDBX_dbi mdbx_cursor_dbi(const MDBX_cursor *mc) {
  if (unlikely(!mc || mc->signature != cur_signature_live))
    return UINT_MAX;
  return cursor_dbi(mc);
}

/*----------------------------------------------------------------------------*/

int mdbx_cursor_put(MDBX_cursor *mc, const MDBX_val *key, MDBX_val *data, MDBX_put_flags_t flags) {
  if (unlikely(mc == nullptr || key == nullptr || data == nullptr))
    return LOG_IFERR(MDBX_EINVAL);

  if (unlikely(mc->signature != cur_signature_live))
    return LOG_IFERR((mc->signature == cur_signature_ready4dispose) ? MDBX_EINVAL : MDBX_EBADSIGN);

  int rc = check_txn_rw(mc->txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  if (unlikely(cursor_dbi_changed(mc)))
    return LOG_IFERR(MDBX_BAD_DBI);

  cASSERT(mc, cursor_is_tracked(mc));

  /* Check this first so counter will always be zero on any early failures. */
  if (unlikely(flags & MDBX_MULTIPLE)) {
    if (unlikely(flags & MDBX_RESERVE))
      return LOG_IFERR(MDBX_EINVAL);
    if (unlikely(!(mc->tree->flags & MDBX_DUPFIXED)))
      return LOG_IFERR(MDBX_INCOMPATIBLE);
    const size_t dcount = data[1].iov_len;
    if (unlikely(dcount < 2 || data->iov_len == 0))
      return LOG_IFERR(MDBX_BAD_VALSIZE);
    if (unlikely(mc->tree->dupfix_size != data->iov_len) && mc->tree->dupfix_size)
      return LOG_IFERR(MDBX_BAD_VALSIZE);
    if (unlikely(dcount > MAX_MAPSIZE / 2 / (BRANCH_NODE_MAX(MDBX_MAX_PAGESIZE) - NODESIZE))) {
      /* checking for multiplication overflow */
      if (unlikely(dcount > MAX_MAPSIZE / 2 / data->iov_len))
        return LOG_IFERR(MDBX_TOO_LARGE);
    }
  }

  if (flags & MDBX_RESERVE) {
    if (unlikely(mc->tree->flags & (MDBX_DUPSORT | MDBX_REVERSEDUP | MDBX_INTEGERDUP | MDBX_DUPFIXED)))
      return LOG_IFERR(MDBX_INCOMPATIBLE);
    data->iov_base = nullptr;
  }

  if (unlikely(mc->txn->flags & (MDBX_TXN_RDONLY | MDBX_TXN_BLOCKED)))
    return LOG_IFERR((mc->txn->flags & MDBX_TXN_RDONLY) ? MDBX_EACCESS : MDBX_BAD_TXN);

  return LOG_IFERR(cursor_put_checklen(mc, key, data, flags));
}

int mdbx_cursor_del(MDBX_cursor *mc, MDBX_put_flags_t flags) {
  if (unlikely(!mc))
    return LOG_IFERR(MDBX_EINVAL);

  if (unlikely(mc->signature != cur_signature_live))
    return LOG_IFERR((mc->signature == cur_signature_ready4dispose) ? MDBX_EINVAL : MDBX_EBADSIGN);

  int rc = check_txn_rw(mc->txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  if (unlikely(cursor_dbi_changed(mc)))
    return LOG_IFERR(MDBX_BAD_DBI);

  return LOG_IFERR(cursor_del(mc, flags));
}

__cold int mdbx_cursor_ignord(MDBX_cursor *mc) {
  if (unlikely(!mc))
    return LOG_IFERR(MDBX_EINVAL);

  if (unlikely(mc->signature != cur_signature_live))
    return LOG_IFERR((mc->signature == cur_signature_ready4dispose) ? MDBX_EINVAL : MDBX_EBADSIGN);

  mc->checking |= z_ignord;
  if (mc->subcur)
    mc->subcur->cursor.checking |= z_ignord;

  return MDBX_SUCCESS;
}
