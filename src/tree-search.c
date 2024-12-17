/// \copyright SPDX-License-Identifier: Apache-2.0
/// \note Please refer to the COPYRIGHT file for explanations license change,
/// credits and acknowledgments.
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024

#include "internals.h"

/* Search for the lowest key under the current branch page.
 * This just bypasses a numkeys check in the current page
 * before calling tree_search_finalize(), because the callers
 * are all in situations where the current page is known to
 * be underfilled. */
__hot int tree_search_lowest(MDBX_cursor *mc) {
  cASSERT(mc, mc->top >= 0);
  page_t *mp = mc->pg[mc->top];
  cASSERT(mc, is_branch(mp));

  node_t *node = page_node(mp, 0);
  int err = page_get(mc, node_pgno(node), &mp, mp->txnid);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  mc->ki[mc->top] = 0;
  err = cursor_push(mc, mp, 0);
  if (unlikely(err != MDBX_SUCCESS))
    return err;
  return tree_search_finalize(mc, nullptr, Z_FIRST);
}

__hot int tree_search(MDBX_cursor *mc, const MDBX_val *key, int flags) {
  int err;
  if (unlikely(mc->txn->flags & MDBX_TXN_BLOCKED)) {
    DEBUG("%s", "transaction has failed, must abort");
    err = MDBX_BAD_TXN;
  bailout:
    be_poor(mc);
    return err;
  }

  const size_t dbi = cursor_dbi(mc);
  if (unlikely(*cursor_dbi_state(mc) & DBI_STALE)) {
    err = tbl_fetch(mc->txn, dbi);
    if (unlikely(err != MDBX_SUCCESS))
      goto bailout;
  }

  const pgno_t root = mc->tree->root;
  if (unlikely(root == P_INVALID)) {
    DEBUG("%s", "tree is empty");
    cASSERT(mc, is_poor(mc));
    return MDBX_NOTFOUND;
  }

  cASSERT(mc, root >= NUM_METAS && root < mc->txn->geo.first_unallocated);
  if (mc->top < 0 || mc->pg[0]->pgno != root) {
    txnid_t pp_txnid = mc->tree->mod_txnid;
    pp_txnid = /* tree->mod_txnid maybe zero in a legacy DB */ pp_txnid ? pp_txnid : mc->txn->txnid;
    if ((mc->txn->flags & MDBX_TXN_RDONLY) == 0) {
      MDBX_txn *scan = mc->txn;
      do
        if ((scan->flags & MDBX_TXN_DIRTY) && (dbi == MAIN_DBI || (scan->dbi_state[dbi] & DBI_DIRTY))) {
          /* После коммита вложенных тразакций может быть mod_txnid > front */
          pp_txnid = scan->front_txnid;
          break;
        }
      while (unlikely((scan = scan->parent) != nullptr));
    }
    err = page_get(mc, root, &mc->pg[0], pp_txnid);
    if (unlikely(err != MDBX_SUCCESS))
      goto bailout;
  }

  mc->top = 0;
  mc->ki[0] = (flags & Z_LAST) ? page_numkeys(mc->pg[0]) - 1 : 0;
  DEBUG("db %d root page %" PRIaPGNO " has flags 0x%X", cursor_dbi_dbg(mc), root, mc->pg[0]->flags);

  if (flags & Z_MODIFY) {
    err = page_touch(mc);
    if (unlikely(err != MDBX_SUCCESS))
      goto bailout;
  }

  if (flags & Z_ROOTONLY)
    return MDBX_SUCCESS;

  return tree_search_finalize(mc, key, flags);
}

__hot __noinline int tree_search_finalize(MDBX_cursor *mc, const MDBX_val *key, int flags) {
  cASSERT(mc, !is_poor(mc));
  DKBUF_DEBUG;
  int err;
  page_t *mp = mc->pg[mc->top];
  intptr_t ki = (flags & Z_FIRST) ? 0 : page_numkeys(mp) - 1;
  while (is_branch(mp)) {
    DEBUG("branch page %" PRIaPGNO " has %zu keys", mp->pgno, page_numkeys(mp));
    cASSERT(mc, page_numkeys(mp) > 1);
    DEBUG("found index 0 to page %" PRIaPGNO, node_pgno(page_node(mp, 0)));

    if ((flags & (Z_FIRST | Z_LAST)) == 0) {
      const struct node_search_result nsr = node_search(mc, key);
      if (likely(nsr.node))
        ki = mc->ki[mc->top] + (intptr_t)nsr.exact - 1;
      DEBUG("following index %zu for key [%s]", ki, DKEY_DEBUG(key));
    }

    err = page_get(mc, node_pgno(page_node(mp, ki)), &mp, mp->txnid);
    if (unlikely(err != MDBX_SUCCESS))
      goto bailout;

    mc->ki[mc->top] = (indx_t)ki;
    ki = (flags & Z_FIRST) ? 0 : page_numkeys(mp) - 1;
    err = cursor_push(mc, mp, ki);
    if (unlikely(err != MDBX_SUCCESS))
      goto bailout;

    if (flags & Z_MODIFY) {
      err = page_touch(mc);
      if (unlikely(err != MDBX_SUCCESS))
        goto bailout;
      mp = mc->pg[mc->top];
    }
  }

  if (!MDBX_DISABLE_VALIDATION && unlikely(!check_leaf_type(mc, mp))) {
    ERROR("unexpected leaf-page #%" PRIaPGNO " type 0x%x seen by cursor", mp->pgno, mp->flags);
    err = MDBX_CORRUPTED;
  bailout:
    be_poor(mc);
    return err;
  }

  DEBUG("found leaf page %" PRIaPGNO " for key [%s]", mp->pgno, DKEY_DEBUG(key));
  /* Логически верно, но (в текущем понимании) нет необходимости.
     Однако, стоит ещё по-проверять/по-тестировать.
     Возможно есть сценарий, в котором очистка флагов всё-таки требуется.

     be_filled(mc); */
  return MDBX_SUCCESS;
}
