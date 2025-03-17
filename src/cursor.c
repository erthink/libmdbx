/// \copyright SPDX-License-Identifier: Apache-2.0
/// \note Please refer to the COPYRIGHT file for explanations license change,
/// credits and acknowledgments.
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025

#include "internals.h"

__cold int cursor_validate(const MDBX_cursor *mc) {
  if (!mc->txn->wr.dirtylist) {
    cASSERT(mc, (mc->txn->flags & MDBX_WRITEMAP) != 0 && !MDBX_AVOID_MSYNC);
  } else {
    cASSERT(mc, (mc->txn->flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);
    cASSERT(mc, mc->txn->wr.dirtyroom + mc->txn->wr.dirtylist->length ==
                    (mc->txn->parent ? mc->txn->parent->wr.dirtyroom : mc->txn->env->options.dp_limit));
  }

  cASSERT(mc, (mc->checking & z_updating) ? mc->top + 1 <= mc->tree->height : mc->top + 1 == mc->tree->height);
  if (unlikely((mc->checking & z_updating) ? mc->top + 1 > mc->tree->height : mc->top + 1 != mc->tree->height))
    return MDBX_CURSOR_FULL;

  if (is_pointed(mc) && (mc->checking & z_updating) == 0) {
    const page_t *mp = mc->pg[mc->top];
    const size_t nkeys = page_numkeys(mp);
    if (!is_hollow(mc)) {
      cASSERT(mc, mc->ki[mc->top] < nkeys);
      if (mc->ki[mc->top] >= nkeys)
        return MDBX_CURSOR_FULL;
    }
    if (inner_pointed(mc)) {
      cASSERT(mc, is_filled(mc));
      if (!is_filled(mc))
        return MDBX_CURSOR_FULL;
    }
  }

  for (intptr_t n = 0; n <= mc->top; ++n) {
    page_t *mp = mc->pg[n];
    const size_t nkeys = page_numkeys(mp);
    const bool expect_branch = (n < mc->tree->height - 1) ? true : false;
    const bool expect_nested_leaf = (n + 1 == mc->tree->height - 1) ? true : false;
    const bool branch = is_branch(mp) ? true : false;
    cASSERT(mc, branch == expect_branch);
    if (unlikely(branch != expect_branch))
      return MDBX_CURSOR_FULL;
    if ((mc->checking & z_updating) == 0) {
      cASSERT(mc, nkeys > mc->ki[n] || (!branch && nkeys == mc->ki[n] && (mc->flags & z_hollow) != 0));
      if (unlikely(nkeys <= mc->ki[n] && !(!branch && nkeys == mc->ki[n] && (mc->flags & z_hollow) != 0)))
        return MDBX_CURSOR_FULL;
    } else {
      cASSERT(mc, nkeys + 1 >= mc->ki[n]);
      if (unlikely(nkeys + 1 < mc->ki[n]))
        return MDBX_CURSOR_FULL;
    }

    int err = page_check(mc, mp);
    if (unlikely(err != MDBX_SUCCESS))
      return err;

    for (size_t i = 0; i < nkeys; ++i) {
      if (branch) {
        node_t *node = page_node(mp, i);
        cASSERT(mc, node_flags(node) == 0);
        if (unlikely(node_flags(node) != 0))
          return MDBX_CURSOR_FULL;
        pgno_t pgno = node_pgno(node);
        page_t *np;
        err = page_get(mc, pgno, &np, mp->txnid);
        cASSERT(mc, err == MDBX_SUCCESS);
        if (unlikely(err != MDBX_SUCCESS))
          return err;
        const bool nested_leaf = is_leaf(np) ? true : false;
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

__cold int cursor_validate_updating(MDBX_cursor *mc) {
  const uint8_t checking = mc->checking;
  mc->checking |= z_updating;
  const int rc = cursor_validate(mc);
  mc->checking = checking;
  return rc;
}

bool cursor_is_tracked(const MDBX_cursor *mc) {
  for (MDBX_cursor *scan = mc->txn->cursors[cursor_dbi(mc)]; scan; scan = scan->next)
    if (mc == ((mc->flags & z_inner) ? &scan->subcur->cursor : scan))
      return true;
  return false;
}

/*----------------------------------------------------------------------------*/

static int touch_dbi(MDBX_cursor *mc) {
  cASSERT(mc, (mc->flags & z_inner) == 0);
  cASSERT(mc, (*cursor_dbi_state(mc) & DBI_DIRTY) == 0);
  *cursor_dbi_state(mc) |= DBI_DIRTY;
  mc->txn->flags |= MDBX_TXN_DIRTY;

  if (!cursor_is_core(mc)) {
    /* Touch DB record of named DB */
    cursor_couple_t cx;
    int rc = dbi_check(mc->txn, MAIN_DBI);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    rc = cursor_init(&cx.outer, mc->txn, MAIN_DBI);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    mc->txn->dbi_state[MAIN_DBI] |= DBI_DIRTY;
    rc = tree_search(&cx.outer, &container_of(mc->clc, kvx_t, clc)->name, Z_MODIFY);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }
  return MDBX_SUCCESS;
}

__hot int cursor_touch(MDBX_cursor *const mc, const MDBX_val *key, const MDBX_val *data) {
  cASSERT(mc, (mc->txn->flags & MDBX_TXN_RDONLY) == 0);
  cASSERT(mc, is_pointed(mc) || mc->tree->height == 0);
  cASSERT(mc, cursor_is_tracked(mc));

  cASSERT(mc, F_ISSET(dbi_state(mc->txn, FREE_DBI), DBI_LINDO | DBI_VALID));
  cASSERT(mc, F_ISSET(dbi_state(mc->txn, MAIN_DBI), DBI_LINDO | DBI_VALID));
  if ((mc->flags & z_inner) == 0) {
    MDBX_txn *const txn = mc->txn;
    dpl_lru_turn(txn);

    if (unlikely((*cursor_dbi_state(mc) & DBI_DIRTY) == 0)) {
      int err = touch_dbi(mc);
      if (unlikely(err != MDBX_SUCCESS))
        return err;
    }

    /* Estimate how much space this operation will take: */
    /* 1) Max b-tree height, reasonable enough with including dups' sub-tree */
    size_t need = CURSOR_STACK_SIZE + 3;
    /* 2) GC/FreeDB for any payload */
    if (!cursor_is_gc(mc)) {
      need += txn->dbs[FREE_DBI].height + (size_t)3;
      /* 3) Named DBs also dirty the main DB */
      if (!cursor_is_main(mc))
        need += txn->dbs[MAIN_DBI].height + (size_t)3;
    }
#if xMDBX_DEBUG_SPILLING != 2
    /* production mode */
    /* 4) Double the page chain estimation
     * for extensively splitting, rebalance and merging */
    need += need;
    /* 5) Factor the key+data which to be put in */
    need += bytes2pgno(txn->env, node_size(key, data)) + (size_t)1;
#else
    /* debug mode */
    (void)key;
    (void)data;
    txn->env->debug_dirtied_est = ++need;
    txn->env->debug_dirtied_act = 0;
#endif /* xMDBX_DEBUG_SPILLING == 2 */

    int err = txn_spill(txn, mc, need);
    if (unlikely(err != MDBX_SUCCESS))
      return err;
  }

  if (likely(is_pointed(mc)) && ((mc->txn->flags & MDBX_TXN_SPILLS) || !is_modifable(mc->txn, mc->pg[mc->top]))) {
    const int8_t top = mc->top;
    mc->top = 0;
    do {
      int err = page_touch(mc);
      if (unlikely(err != MDBX_SUCCESS))
        return err;
      mc->top += 1;
    } while (mc->top <= top);
    mc->top = top;
  }
  return MDBX_SUCCESS;
}

/*----------------------------------------------------------------------------*/

int cursor_shadow(MDBX_cursor *cursor, MDBX_txn *nested, const size_t dbi) {
  tASSERT(nested, cursor->signature == cur_signature_live);
  tASSERT(nested, cursor->txn != nested);
  cASSERT(cursor, cursor->txn->flags & txn_may_have_cursors);
  cASSERT(cursor, dbi == cursor_dbi(cursor));
  tASSERT(nested, dbi > FREE_DBI && dbi < nested->n_dbi);

  const size_t size = cursor->subcur ? sizeof(MDBX_cursor) + sizeof(subcur_t) : sizeof(MDBX_cursor);
  MDBX_cursor *const shadow = osal_malloc(size);
  if (unlikely(!shadow))
    return MDBX_ENOMEM;

#if MDBX_DEBUG
  memset(shadow, 0xCD, size);
  VALGRIND_MAKE_MEM_UNDEFINED(shadow, size);
#endif /* MDBX_DEBUG */
  *shadow = *cursor;
  cursor->backup = shadow;
  cursor->txn = nested;
  cursor->tree = &nested->dbs[dbi];
  cursor->dbi_state = &nested->dbi_state[dbi];
  subcur_t *subcur = cursor->subcur;
  if (subcur) {
    *(subcur_t *)(shadow + 1) = *subcur;
    subcur->cursor.txn = nested;
    subcur->cursor.dbi_state = &nested->dbi_state[dbi];
  }
  return MDBX_SUCCESS;
}

MDBX_cursor *cursor_eot(MDBX_cursor *cursor, MDBX_txn *txn) {
  MDBX_cursor *const next = cursor->next;
  const unsigned stage = cursor->signature;
  MDBX_cursor *const shadow = cursor->backup;
  ENSURE(txn->env, stage == cur_signature_live || (stage == cur_signature_wait4eot && shadow));
  tASSERT(txn, cursor->txn == txn);
  if (shadow) {
    subcur_t *subcur = cursor->subcur;
    tASSERT(txn, txn->parent != nullptr && shadow->txn == txn->parent);
    /* Zap: Using uninitialized memory '*subcur->backup'. */
    MDBX_SUPPRESS_GOOFY_MSVC_ANALYZER(6001);
    ENSURE(txn->env, shadow->signature == cur_signature_live);
    tASSERT(txn, subcur == shadow->subcur);
    if ((txn->flags & MDBX_TXN_ERROR) == 0) {
      /* Update pointers to parent txn */
      cursor->next = shadow->next;
      cursor->backup = shadow->backup;
      cursor->txn = shadow->txn;
      cursor->tree = shadow->tree;
      cursor->dbi_state = shadow->dbi_state;
      if (subcur) {
        subcur->cursor.txn = shadow->txn;
        subcur->cursor.dbi_state = shadow->dbi_state;
      }
    } else {
      /* Restore from backup, i.e. rollback/abort nested txn */
      *cursor = *shadow;
      cursor->signature = stage /* Promote (cur_signature_wait4eot) state to parent txn */;
      if (subcur)
        *subcur = *(subcur_t *)(shadow + 1);
    }
    shadow->signature = 0;
    osal_free(shadow);
  } else {
    ENSURE(cursor->txn->env, stage == cur_signature_live);
    cursor->signature = cur_signature_ready4dispose /* Cursor may be reused */;
    cursor->next = cursor;
    cursor_drown((cursor_couple_t *)cursor);
  }
  return next;
}

/*----------------------------------------------------------------------------*/

static __always_inline int couple_init(cursor_couple_t *couple, const MDBX_txn *const txn, tree_t *const tree,
                                       kvx_t *const kvx, uint8_t *const dbi_state) {

  VALGRIND_MAKE_MEM_UNDEFINED(couple, sizeof(cursor_couple_t));
  tASSERT(txn, F_ISSET(*dbi_state, DBI_VALID | DBI_LINDO));

  couple->outer.signature = cur_signature_live;
  couple->outer.next = &couple->outer;
  couple->outer.backup = nullptr;
  couple->outer.txn = (MDBX_txn *)txn;
  couple->outer.tree = tree;
  couple->outer.clc = &kvx->clc;
  couple->outer.dbi_state = dbi_state;
  couple->outer.top_and_flags = z_fresh_mark;
  STATIC_ASSERT((int)z_branch == P_BRANCH && (int)z_leaf == P_LEAF && (int)z_largepage == P_LARGE &&
                (int)z_dupfix == P_DUPFIX);
  couple->outer.checking = (AUDIT_ENABLED() || (txn->env->flags & MDBX_VALIDATION)) ? z_pagecheck | z_leaf : z_leaf;
  couple->outer.subcur = nullptr;

  if (tree->flags & MDBX_DUPSORT) {
    couple->inner.cursor.signature = cur_signature_live;
    subcur_t *const mx = couple->outer.subcur = &couple->inner;
    mx->cursor.subcur = nullptr;
    mx->cursor.next = &mx->cursor;
    mx->cursor.txn = (MDBX_txn *)txn;
    mx->cursor.tree = &mx->nested_tree;
    mx->cursor.clc = ptr_disp(couple->outer.clc, sizeof(clc_t));
    tASSERT(txn, &mx->cursor.clc->k == &kvx->clc.v);
    mx->cursor.dbi_state = dbi_state;
    mx->cursor.top_and_flags = z_fresh_mark | z_inner;
    STATIC_ASSERT(MDBX_DUPFIXED * 2 == P_DUPFIX);
    mx->cursor.checking = couple->outer.checking + ((tree->flags & MDBX_DUPFIXED) << 1);
  }

  if (unlikely(*dbi_state & DBI_STALE))
    return tbl_fetch(couple->outer.txn, cursor_dbi(&couple->outer));

  return tbl_setup_ifneed(txn->env, kvx, tree);
}

__cold int cursor_init4walk(cursor_couple_t *couple, const MDBX_txn *const txn, tree_t *const tree, kvx_t *const kvx) {
  return couple_init(couple, txn, tree, kvx, txn->dbi_state);
}

int cursor_init(MDBX_cursor *mc, const MDBX_txn *txn, size_t dbi) {
  STATIC_ASSERT(offsetof(cursor_couple_t, outer) == 0);
  int rc = dbi_check(txn, dbi);
  if (likely(rc == MDBX_SUCCESS))
    rc = couple_init(container_of(mc, cursor_couple_t, outer), txn, &txn->dbs[dbi], &txn->env->kvs[dbi],
                     &txn->dbi_state[dbi]);
  return rc;
}

__cold static int unexpected_dupsort(MDBX_cursor *mc) {
  ERROR("unexpected dupsort-page/node for non-dupsort db/cursor (dbi %zu)", cursor_dbi(mc));
  mc->txn->flags |= MDBX_TXN_ERROR;
  be_poor(mc);
  return MDBX_CORRUPTED;
}

int cursor_dupsort_setup(MDBX_cursor *mc, const node_t *node, const page_t *mp) {
  cASSERT(mc, is_pointed(mc));
  subcur_t *mx = mc->subcur;
  if (!MDBX_DISABLE_VALIDATION && unlikely(mx == nullptr))
    return unexpected_dupsort(mc);

  const uint8_t flags = node_flags(node);
  switch (flags) {
  default:
    ERROR("invalid node flags %u", flags);
    goto bailout;
  case N_DUP | N_TREE:
    if (!MDBX_DISABLE_VALIDATION && unlikely(node_ds(node) != sizeof(tree_t))) {
      ERROR("invalid nested-db record size (%zu, expect %zu)", node_ds(node), sizeof(tree_t));
      goto bailout;
    }
    memcpy(&mx->nested_tree, node_data(node), sizeof(tree_t));
    const txnid_t pp_txnid = mp->txnid;
    if (!MDBX_DISABLE_VALIDATION && unlikely(mx->nested_tree.mod_txnid > pp_txnid)) {
      ERROR("nested-db.mod_txnid (%" PRIaTXN ") > page-txnid (%" PRIaTXN ")", mx->nested_tree.mod_txnid, pp_txnid);
      goto bailout;
    }
    mx->cursor.top_and_flags = z_fresh_mark | z_inner;
    break;
  case N_DUP:
    if (!MDBX_DISABLE_VALIDATION && unlikely(node_ds(node) <= PAGEHDRSZ)) {
      ERROR("invalid nested-page size %zu", node_ds(node));
      goto bailout;
    }
    page_t *sp = node_data(node);
    mx->nested_tree.height = 1;
    mx->nested_tree.branch_pages = 0;
    mx->nested_tree.leaf_pages = 1;
    mx->nested_tree.large_pages = 0;
    mx->nested_tree.items = page_numkeys(sp);
    mx->nested_tree.root = 0;
    mx->nested_tree.mod_txnid = mp->txnid;
    mx->cursor.top_and_flags = z_inner;
    mx->cursor.pg[0] = sp;
    mx->cursor.ki[0] = 0;
    mx->nested_tree.flags = flags_db2sub(mc->tree->flags);
    mx->nested_tree.dupfix_size = (mc->tree->flags & MDBX_DUPFIXED) ? sp->dupfix_ksize : 0;
    break;
  }

  if (unlikely(mx->nested_tree.dupfix_size != mc->tree->dupfix_size)) {
    if (!MDBX_DISABLE_VALIDATION && unlikely(mc->tree->dupfix_size != 0)) {
      ERROR("cursor mismatched nested-db dupfix_size %u", mc->tree->dupfix_size);
      goto bailout;
    }
    if (!MDBX_DISABLE_VALIDATION && unlikely((mc->tree->flags & MDBX_DUPFIXED) == 0)) {
      ERROR("mismatched nested-db flags %u", mc->tree->flags);
      goto bailout;
    }
    if (!MDBX_DISABLE_VALIDATION &&
        unlikely(mx->nested_tree.dupfix_size < mc->clc->v.lmin || mx->nested_tree.dupfix_size > mc->clc->v.lmax)) {
      ERROR("mismatched nested-db.dupfix_size (%u) <> min/max value-length "
            "(%zu/%zu)",
            mx->nested_tree.dupfix_size, mc->clc->v.lmin, mc->clc->v.lmax);
      goto bailout;
    }
    mc->tree->dupfix_size = mx->nested_tree.dupfix_size;
    mc->clc->v.lmin = mc->clc->v.lmax = mx->nested_tree.dupfix_size;
    cASSERT(mc, mc->clc->v.lmax >= mc->clc->v.lmin);
  }

  DEBUG("Sub-db dbi -%zu root page %" PRIaPGNO, cursor_dbi(&mx->cursor), mx->nested_tree.root);
  return MDBX_SUCCESS;

bailout:
  mx->cursor.top_and_flags = z_poor_mark | z_inner;
  return MDBX_CORRUPTED;
}

/*----------------------------------------------------------------------------*/

MDBX_cursor *cursor_cpstk(const MDBX_cursor *csrc, MDBX_cursor *cdst) {
  cASSERT(cdst, cdst->txn == csrc->txn);
  cASSERT(cdst, cdst->tree == csrc->tree);
  cASSERT(cdst, cdst->clc == csrc->clc);
  cASSERT(cdst, cdst->dbi_state == csrc->dbi_state);
  cdst->top_and_flags = csrc->top_and_flags;

  for (intptr_t i = 0; i <= csrc->top; i++) {
    cdst->pg[i] = csrc->pg[i];
    cdst->ki[i] = csrc->ki[i];
  }
  return cdst;
}

static __always_inline int sibling(MDBX_cursor *mc, bool right) {
  if (mc->top < 1) {
    /* root has no siblings */
    return MDBX_NOTFOUND;
  }

  cursor_pop(mc);
  DEBUG("parent page is page %" PRIaPGNO ", index %u", mc->pg[mc->top]->pgno, mc->ki[mc->top]);

  int err;
  if (right ? (mc->ki[mc->top] + (size_t)1 >= page_numkeys(mc->pg[mc->top])) : (mc->ki[mc->top] == 0)) {
    DEBUG("no more keys aside, moving to next %s sibling", right ? "right" : "left");
    err = right ? cursor_sibling_right(mc) : cursor_sibling_left(mc);
    if (err != MDBX_SUCCESS) {
      if (likely(err == MDBX_NOTFOUND))
        /* undo cursor_pop before returning */
        mc->top += 1;
      return err;
    }
  } else {
    mc->ki[mc->top] += right ? 1 : -1;
    DEBUG("just moving to %s index key %u", right ? "right" : "left", mc->ki[mc->top]);
  }
  cASSERT(mc, is_branch(mc->pg[mc->top]));

  page_t *mp = mc->pg[mc->top];
  const node_t *node = page_node(mp, mc->ki[mc->top]);
  err = page_get(mc, node_pgno(node), &mp, mp->txnid);
  if (likely(err == MDBX_SUCCESS)) {
    err = cursor_push(mc, mp, right ? 0 : (indx_t)page_numkeys(mp) - 1);
    if (likely(err == MDBX_SUCCESS))
      return err;
  }

  be_poor(mc);
  return err;
}

__hot int cursor_sibling_left(MDBX_cursor *mc) {
  int err = sibling(mc, false);
  if (likely(err != MDBX_NOTFOUND))
    return err;

  cASSERT(mc, mc->top >= 0);
  size_t nkeys = page_numkeys(mc->pg[mc->top]);
  cASSERT(mc, nkeys > 0);
  mc->ki[mc->top] = 0;
  return MDBX_NOTFOUND;
}

__hot int cursor_sibling_right(MDBX_cursor *mc) {
  int err = sibling(mc, true);
  if (likely(err != MDBX_NOTFOUND))
    return err;

  cASSERT(mc, mc->top >= 0);
  size_t nkeys = page_numkeys(mc->pg[mc->top]);
  cASSERT(mc, nkeys > 0);
  mc->ki[mc->top] = (indx_t)nkeys - 1;
  mc->flags = z_eof_soft | z_eof_hard | (mc->flags & z_clear_mask);
  inner_gone(mc);
  return MDBX_NOTFOUND;
}

/*----------------------------------------------------------------------------*/

/* Функция-шаблон: Приземляет курсор на данные в текущей позиции.
 * В том числе, загружает данные во вложенный курсор при его наличии. */
static __always_inline int cursor_bring(const bool inner, const bool tend2first, MDBX_cursor *__restrict mc,
                                        MDBX_val *__restrict key, MDBX_val *__restrict data, bool eof) {
  if (inner) {
    cASSERT(mc, !data && !mc->subcur && (mc->flags & z_inner) != 0);
  } else {
    cASSERT(mc, (mc->flags & z_inner) == 0);
  }

  const page_t *mp = mc->pg[mc->top];
  if (!MDBX_DISABLE_VALIDATION && unlikely(!check_leaf_type(mc, mp))) {
    ERROR("unexpected leaf-page #%" PRIaPGNO " type 0x%x seen by cursor", mp->pgno, mp->flags);
    return MDBX_CORRUPTED;
  }

  const size_t nkeys = page_numkeys(mp);
  cASSERT(mc, nkeys > 0);
  const size_t ki = mc->ki[mc->top];
  cASSERT(mc, nkeys > ki);
  cASSERT(mc, !eof || ki == nkeys - 1);

  if (inner && is_dupfix_leaf(mp)) {
    be_filled(mc);
    if (eof)
      mc->flags |= z_eof_soft;
    if (likely(key))
      *key = page_dupfix_key(mp, ki, mc->tree->dupfix_size);
    return MDBX_SUCCESS;
  }

  const node_t *__restrict node = page_node(mp, ki);
  if (!inner && (node_flags(node) & N_DUP)) {
    int err = cursor_dupsort_setup(mc, node, mp);
    if (unlikely(err != MDBX_SUCCESS))
      return err;
    MDBX_ANALYSIS_ASSUME(mc->subcur != nullptr);
    if (node_flags(node) & N_TREE) {
      err = tend2first ? inner_first(&mc->subcur->cursor, data) : inner_last(&mc->subcur->cursor, data);
      if (unlikely(err != MDBX_SUCCESS))
        return err;
    } else {
      if (!tend2first) {
        mc->subcur->cursor.ki[0] = (indx_t)mc->subcur->nested_tree.items - 1;
        mc->subcur->cursor.flags |= z_eof_soft;
      }
      if (data) {
        const page_t *inner_mp = mc->subcur->cursor.pg[0];
        cASSERT(mc, is_subpage(inner_mp) && is_leaf(inner_mp));
        const size_t inner_ki = mc->subcur->cursor.ki[0];
        if (is_dupfix_leaf(inner_mp))
          *data = page_dupfix_key(inner_mp, inner_ki, mc->tree->dupfix_size);
        else
          *data = get_key(page_node(inner_mp, inner_ki));
      }
    }
    be_filled(mc);
  } else {
    if (!inner)
      inner_gone(mc);
    if (data) {
      int err = node_read(mc, node, data, mp);
      if (unlikely(err != MDBX_SUCCESS))
        return err;
    }
    be_filled(mc);
    if (eof)
      mc->flags |= z_eof_soft;
  }

  get_key_optional(node, key);
  return MDBX_SUCCESS;
}

/* Функция-шаблон: Устанавливает курсор в начало или конец. */
static __always_inline int cursor_brim(const bool inner, const bool tend2first, MDBX_cursor *__restrict mc,
                                       MDBX_val *__restrict key, MDBX_val *__restrict data) {
  if (mc->top != 0) {
    int err = tree_search(mc, nullptr, tend2first ? Z_FIRST : Z_LAST);
    if (unlikely(err != MDBX_SUCCESS))
      return err;
  }
  const size_t nkeys = page_numkeys(mc->pg[mc->top]);
  cASSERT(mc, nkeys > 0);
  mc->ki[mc->top] = tend2first ? 0 : nkeys - 1;
  return cursor_bring(inner, tend2first, mc, key, data, !tend2first);
}

__hot int inner_first(MDBX_cursor *mc, MDBX_val *data) { return cursor_brim(true, true, mc, data, nullptr); }

__hot int inner_last(MDBX_cursor *mc, MDBX_val *data) { return cursor_brim(true, false, mc, data, nullptr); }

__hot int outer_first(MDBX_cursor *mc, MDBX_val *key, MDBX_val *data) {
  return cursor_brim(false, true, mc, key, data);
}

__hot int outer_last(MDBX_cursor *mc, MDBX_val *key, MDBX_val *data) {
  return cursor_brim(false, false, mc, key, data);
}

/*----------------------------------------------------------------------------*/

/* Функция-шаблон: Передвигает курсор на одну позицию.
 * При необходимости управляет вложенным курсором. */
static __always_inline int cursor_step(const bool inner, const bool forward, MDBX_cursor *__restrict mc,
                                       MDBX_val *__restrict key, MDBX_val *__restrict data, MDBX_cursor_op op) {
  if (forward) {
    if (inner)
      cASSERT(mc, op == MDBX_NEXT);
    else
      cASSERT(mc, op == MDBX_NEXT || op == MDBX_NEXT_DUP || op == MDBX_NEXT_NODUP);
  } else {
    if (inner)
      cASSERT(mc, op == MDBX_PREV);
    else
      cASSERT(mc, op == MDBX_PREV || op == MDBX_PREV_DUP || op == MDBX_PREV_NODUP);
  }
  if (inner) {
    cASSERT(mc, !data && !mc->subcur && (mc->flags & z_inner) != 0);
  } else {
    cASSERT(mc, (mc->flags & z_inner) == 0);
  }

  if (unlikely(is_poor(mc))) {
    int state = mc->flags;
    if (state & z_fresh) {
      if (forward)
        return inner ? inner_first(mc, key) : outer_first(mc, key, data);
      else
        return inner ? inner_last(mc, key) : outer_last(mc, key, data);
    }
    mc->flags = inner ? z_inner | z_poor_mark : z_poor_mark;
    return (state & z_after_delete) ? MDBX_NOTFOUND : MDBX_ENODATA;
  }

  const page_t *mp = mc->pg[mc->top];
  const intptr_t nkeys = page_numkeys(mp);
  cASSERT(mc, nkeys > 0);

  intptr_t ki = mc->ki[mc->top];
  const uint8_t state = mc->flags & (z_after_delete | z_hollow | z_eof_hard | z_eof_soft);
  if (likely(state == 0)) {
    cASSERT(mc, ki < nkeys);
    if (!inner && op != (forward ? MDBX_NEXT_NODUP : MDBX_PREV_NODUP)) {
      int err = MDBX_NOTFOUND;
      if (inner_pointed(mc)) {
        err = forward ? inner_next(&mc->subcur->cursor, data) : inner_prev(&mc->subcur->cursor, data);
        if (likely(err == MDBX_SUCCESS)) {
          get_key_optional(page_node(mp, ki), key);
          return MDBX_SUCCESS;
        }
        if (unlikely(err != MDBX_NOTFOUND && err != MDBX_ENODATA)) {
          cASSERT(mc, !inner_pointed(mc));
          return err;
        }
        cASSERT(mc, !forward || (mc->subcur->cursor.flags & z_eof_soft));
      }
      if (op == (forward ? MDBX_NEXT_DUP : MDBX_PREV_DUP))
        return err;
    }
    if (!inner)
      inner_gone(mc);
  } else {
    if (mc->flags & z_hollow) {
      cASSERT(mc, !inner_pointed(mc) || inner_hollow(mc));
      return MDBX_ENODATA;
    }

    if (!inner && op == (forward ? MDBX_NEXT_DUP : MDBX_PREV_DUP))
      return MDBX_NOTFOUND;

    if (forward) {
      if (state & z_after_delete) {
        if (ki < nkeys)
          goto bring;
      } else {
        cASSERT(mc, state & (z_eof_soft | z_eof_hard));
        return MDBX_NOTFOUND;
      }
    } else if (state & z_eof_hard) {
      mc->ki[mc->top] = (indx_t)nkeys - 1;
      goto bring;
    }
  }

  DEBUG("turn-%s: top page was %" PRIaPGNO " in cursor %p, ki %zi of %zi", forward ? "next" : "prev", mp->pgno,
        __Wpedantic_format_voidptr(mc), ki, nkeys);
  if (forward) {
    if (likely(++ki < nkeys))
      mc->ki[mc->top] = (indx_t)ki;
    else {
      DEBUG("%s", "=====> move to next sibling page");
      int err = cursor_sibling_right(mc);
      if (unlikely(err != MDBX_SUCCESS))
        return err;
      mp = mc->pg[mc->top];
      DEBUG("next page is %" PRIaPGNO ", key index %u", mp->pgno, mc->ki[mc->top]);
    }
  } else {
    if (likely(--ki >= 0))
      mc->ki[mc->top] = (indx_t)ki;
    else {
      DEBUG("%s", "=====> move to prev sibling page");
      int err = cursor_sibling_left(mc);
      if (unlikely(err != MDBX_SUCCESS))
        return err;
      mp = mc->pg[mc->top];
      DEBUG("prev page is %" PRIaPGNO ", key index %u", mp->pgno, mc->ki[mc->top]);
    }
  }
  DEBUG("==> cursor points to page %" PRIaPGNO " with %zu keys, key index %u", mp->pgno, page_numkeys(mp),
        mc->ki[mc->top]);

bring:
  return cursor_bring(inner, forward, mc, key, data, false);
}

__hot int inner_next(MDBX_cursor *mc, MDBX_val *data) { return cursor_step(true, true, mc, data, nullptr, MDBX_NEXT); }

__hot int inner_prev(MDBX_cursor *mc, MDBX_val *data) { return cursor_step(true, false, mc, data, nullptr, MDBX_PREV); }

__hot int outer_next(MDBX_cursor *mc, MDBX_val *key, MDBX_val *data, MDBX_cursor_op op) {
  return cursor_step(false, true, mc, key, data, op);
}

__hot int outer_prev(MDBX_cursor *mc, MDBX_val *key, MDBX_val *data, MDBX_cursor_op op) {
  return cursor_step(false, false, mc, key, data, op);
}

/*----------------------------------------------------------------------------*/

__hot int cursor_put(MDBX_cursor *mc, const MDBX_val *key, MDBX_val *data, unsigned flags) {
  int err;
  DKBUF_DEBUG;
  MDBX_env *const env = mc->txn->env;
  if (LOG_ENABLED(MDBX_LOG_DEBUG) && (flags & MDBX_RESERVE))
    data->iov_base = nullptr;
  DEBUG("==> put db %d key [%s], size %" PRIuPTR ", data [%s] size %" PRIuPTR, cursor_dbi_dbg(mc), DKEY_DEBUG(key),
        key->iov_len, DVAL_DEBUG(data), data->iov_len);

  if ((flags & MDBX_CURRENT) != 0 && (mc->flags & z_inner) == 0) {
    if (unlikely(flags & (MDBX_APPEND | MDBX_NOOVERWRITE)))
      return MDBX_EINVAL;
    /* Запрошено обновление текущей записи, на которой сейчас стоит курсор.
     * Проверяем что переданный ключ совпадает со значением в текущей позиции
     * курсора. Здесь проще вызвать cursor_ops(), так как для обслуживания
     * таблиц с MDBX_DUPSORT также требуется текущий размер данных. */
    MDBX_val current_key, current_data;
    err = cursor_ops(mc, &current_key, &current_data, MDBX_GET_CURRENT);
    if (unlikely(err != MDBX_SUCCESS))
      return err;
    if (mc->clc->k.cmp(key, &current_key) != 0)
      return MDBX_EKEYMISMATCH;

    if (unlikely((flags & MDBX_MULTIPLE))) {
      if (unlikely(!mc->subcur))
        return MDBX_EINVAL;
      err = cursor_del(mc, flags & MDBX_ALLDUPS);
      if (unlikely(err != MDBX_SUCCESS))
        return err;
      if (unlikely(data[1].iov_len == 0))
        return MDBX_SUCCESS;
      flags -= MDBX_CURRENT;
      goto skip_check_samedata;
    }

    if (mc->subcur) {
      node_t *node = page_node(mc->pg[mc->top], mc->ki[mc->top]);
      if (node_flags(node) & N_DUP) {
        cASSERT(mc, inner_pointed(mc));
        /* Если за ключом более одного значения, либо если размер данных
         * отличается, то вместо обновления требуется удаление и
         * последующая вставка. */
        if (mc->subcur->nested_tree.items > 1 || current_data.iov_len != data->iov_len) {
          err = cursor_del(mc, flags & MDBX_ALLDUPS);
          if (unlikely(err != MDBX_SUCCESS))
            return err;
          flags -= MDBX_CURRENT;
          goto skip_check_samedata;
        }
      } else if (unlikely(node_size(key, data) > env->leaf_nodemax)) {
        /* Уже есть пара key-value хранящаяся в обычном узле. Новые данные
         * слишком большие для размещения в обычном узле вместе с ключом, но
         * могут быть размещены в вложенном дереве. Удаляем узел со старыми
         * данными, чтобы при помещении новых создать вложенное дерево. */
        err = cursor_del(mc, 0);
        if (unlikely(err != MDBX_SUCCESS))
          return err;
        flags -= MDBX_CURRENT;
        goto skip_check_samedata;
      }
    }
    if (!(flags & MDBX_RESERVE) && unlikely(cmp_lenfast(&current_data, data) == 0))
      return MDBX_SUCCESS /* the same data, nothing to update */;
  skip_check_samedata:;
  }

  int rc = MDBX_SUCCESS;
  if (mc->tree->height == 0) {
    /* new database, cursor has nothing to point to */
    cASSERT(mc, is_poor(mc));
    rc = MDBX_NO_ROOT;
  } else if ((flags & MDBX_CURRENT) == 0) {
    bool exact = false;
    MDBX_val old_data;
    if ((flags & MDBX_APPEND) && mc->tree->items > 0) {
      MDBX_val last_key;
      old_data.iov_base = nullptr;
      old_data.iov_len = 0;
      rc = (mc->flags & z_inner) ? inner_last(mc, &last_key) : outer_last(mc, &last_key, &old_data);
      if (likely(rc == MDBX_SUCCESS)) {
        const int cmp = mc->clc->k.cmp(key, &last_key);
        if (likely(cmp > 0)) {
          mc->ki[mc->top]++; /* step forward for appending */
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
      csr_t csr = cursor_seek(mc, (MDBX_val *)key, &old_data, MDBX_SET);
      rc = csr.err;
      exact = csr.exact;
    }
    if (exact) {
      cASSERT(mc, rc == MDBX_SUCCESS);
      if (unlikely(flags & MDBX_NOOVERWRITE)) {
        DEBUG("duplicate key [%s]", DKEY_DEBUG(key));
        *data = old_data;
        return MDBX_KEYEXIST;
      }
      if (unlikely(mc->flags & z_inner)) {
        /* nested subtree of DUPSORT-database with the same key, nothing to update */
        cASSERT(mc, !"Should not happen since");
        return (flags & MDBX_NODUPDATA) ? MDBX_KEYEXIST : MDBX_SUCCESS;
      }
      if (inner_pointed(mc)) {
        if (unlikely(flags & MDBX_ALLDUPS)) {
          rc = cursor_del(mc, MDBX_ALLDUPS);
          if (unlikely(rc != MDBX_SUCCESS))
            return rc;
          flags -= MDBX_ALLDUPS;
          cASSERT(mc, mc->top + 1 == mc->tree->height);
          rc = (mc->top >= 0) ? MDBX_NOTFOUND : MDBX_NO_ROOT;
        } else if ((flags & (MDBX_RESERVE | MDBX_MULTIPLE)) == 0) {
          old_data = *data;
          csr_t csr = cursor_seek(&mc->subcur->cursor, &old_data, nullptr, MDBX_SET_RANGE);
          if (unlikely(csr.exact)) {
            cASSERT(mc, csr.err == MDBX_SUCCESS);
            if (flags & MDBX_NODUPDATA)
              return MDBX_KEYEXIST;
            if (flags & MDBX_APPENDDUP)
              return MDBX_EKEYMISMATCH;
            /* the same data, nothing to update */
            return MDBX_SUCCESS;
          } else if (csr.err != MDBX_SUCCESS && unlikely(csr.err != MDBX_NOTFOUND)) {
            be_poor(mc);
            return csr.err;
          }
        }
      } else if (!(flags & (MDBX_RESERVE | MDBX_MULTIPLE))) {
        if (unlikely(eq_fast(data, &old_data))) {
          cASSERT(mc, mc->clc->v.cmp(data, &old_data) == 0);
          /* the same data, nothing to update */
          return (mc->subcur && (flags & MDBX_NODUPDATA)) ? MDBX_KEYEXIST : MDBX_SUCCESS;
        }
        cASSERT(mc, mc->clc->v.cmp(data, &old_data) != 0);
      }
    } else if (unlikely(rc != MDBX_NOTFOUND))
      return rc;
  }

  mc->flags &= ~z_after_delete;
  MDBX_val xdata, *ref_data = data;
  size_t *batch_dupfix_done = nullptr, batch_dupfix_given = 0;
  if (unlikely(flags & MDBX_MULTIPLE)) {
    batch_dupfix_given = data[1].iov_len;
    if (unlikely(data[1].iov_len == 0))
      return /* nothing todo */ MDBX_SUCCESS;
    batch_dupfix_done = &data[1].iov_len;
    *batch_dupfix_done = 0;
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
    npr.err = cursor_push(mc, npr.page, 0);
    if (unlikely(npr.err != MDBX_SUCCESS))
      return npr.err;
    mc->tree->root = npr.page->pgno;
    mc->tree->height++;
    if (mc->tree->flags & MDBX_INTEGERKEY) {
      assert(key->iov_len >= mc->clc->k.lmin && key->iov_len <= mc->clc->k.lmax);
      mc->clc->k.lmin = mc->clc->k.lmax = key->iov_len;
    }
    if (mc->tree->flags & (MDBX_INTEGERDUP | MDBX_DUPFIXED)) {
      assert(data->iov_len >= mc->clc->v.lmin && data->iov_len <= mc->clc->v.lmax);
      assert(mc->subcur != nullptr);
      mc->tree->dupfix_size = /* mc->subcur->nested_tree.dupfix_size = */
          (unsigned)(mc->clc->v.lmin = mc->clc->v.lmax = data->iov_len);
      cASSERT(mc, mc->clc->v.lmin == mc->subcur->cursor.clc->k.lmin);
      cASSERT(mc, mc->clc->v.lmax == mc->subcur->cursor.clc->k.lmax);
      if (mc->flags & z_inner)
        npr.page->flags |= P_DUPFIX;
    }
  }

  MDBX_val old_singledup, old_data;
  tree_t nested_dupdb;
  page_t *sub_root = nullptr;
  bool insert_key, insert_data;
  uint16_t fp_flags = P_LEAF;
  page_t *fp = env->page_auxbuf;
  fp->txnid = mc->txn->front_txnid;
  insert_key = insert_data = (rc != MDBX_SUCCESS);
  old_singledup.iov_base = nullptr;
  old_singledup.iov_len = 0;
  if (insert_key) {
    /* The key does not exist */
    DEBUG("inserting key at index %i", mc->ki[mc->top]);
    if (mc->tree->flags & MDBX_DUPSORT) {
      inner_gone(mc);
      if (node_size(key, data) > env->leaf_nodemax) {
        /* Too big for a node, insert in sub-DB.  Set up an empty
         * "old sub-page" for convert_to_subtree to expand to a full page. */
        fp->dupfix_ksize = (mc->tree->flags & MDBX_DUPFIXED) ? (uint16_t)data->iov_len : 0;
        fp->lower = fp->upper = 0;
        old_data.iov_len = PAGEHDRSZ;
        goto convert_to_subtree;
      }
    }
  } else {
    /* there's only a key anyway, so this is a no-op */
    if (is_dupfix_leaf(mc->pg[mc->top])) {
      size_t ksize = mc->tree->dupfix_size;
      if (unlikely(key->iov_len != ksize))
        return MDBX_BAD_VALSIZE;
      void *ptr = page_dupfix_ptr(mc->pg[mc->top], mc->ki[mc->top], ksize);
      memcpy(ptr, key->iov_base, ksize);
    fix_parent:
      /* if overwriting slot 0 of leaf, need to
       * update branch key if there is a parent page */
      if (mc->top && !mc->ki[mc->top]) {
        size_t dtop = 1;
        mc->top--;
        /* slot 0 is always an empty key, find real slot */
        while (mc->top && !mc->ki[mc->top]) {
          mc->top--;
          dtop++;
        }
        err = MDBX_SUCCESS;
        if (mc->ki[mc->top])
          err = tree_propagate_key(mc, key);
        cASSERT(mc, mc->top + dtop < UINT16_MAX);
        mc->top += (uint8_t)dtop;
        if (unlikely(err != MDBX_SUCCESS))
          return err;
      }

      if (AUDIT_ENABLED()) {
        err = cursor_validate(mc);
        if (unlikely(err != MDBX_SUCCESS))
          return err;
      }
      return MDBX_SUCCESS;
    }

  more:
    if (AUDIT_ENABLED()) {
      err = cursor_validate(mc);
      if (unlikely(err != MDBX_SUCCESS))
        return err;
    }
    node_t *const node = page_node(mc->pg[mc->top], mc->ki[mc->top]);

    /* Large/Overflow page overwrites need special handling */
    if (unlikely(node_flags(node) & N_BIG)) {
      const size_t dpages = (node_size(key, data) > env->leaf_nodemax) ? largechunk_npages(env, data->iov_len) : 0;

      const pgno_t pgno = node_largedata_pgno(node);
      pgr_t lp = page_get_large(mc, pgno, mc->pg[mc->top]->txnid);
      if (unlikely(lp.err != MDBX_SUCCESS))
        return lp.err;
      cASSERT(mc, page_type(lp.page) == P_LARGE);

      /* Is the ov page from this txn (or a parent) and big enough? */
      const size_t ovpages = lp.page->pages;
      const size_t extra_threshold =
          (mc->tree == &mc->txn->dbs[FREE_DBI]) ? 1 : /* LY: add configurable threshold to keep reserve space */ 0;
      if (!is_frozen(mc->txn, lp.page) && ovpages >= dpages && ovpages <= dpages + extra_threshold) {
        /* yes, overwrite it. */
        if (!is_modifable(mc->txn, lp.page)) {
          if (is_spilled(mc->txn, lp.page)) {
            lp = /* TODO: avoid search and get txn & spill-index from
                     page_result */
                page_unspill(mc->txn, lp.page);
            if (unlikely(lp.err))
              return lp.err;
          } else {
            if (unlikely(!mc->txn->parent)) {
              ERROR("Unexpected not frozen/modifiable/spilled but shadowed %s "
                    "page %" PRIaPGNO " mod-txnid %" PRIaTXN ","
                    " without parent transaction, current txn %" PRIaTXN " front %" PRIaTXN,
                    "large/overflow", pgno, lp.page->txnid, mc->txn->txnid, mc->txn->front_txnid);
              return MDBX_PROBLEM;
            }

            /* It is writable only in a parent txn */
            page_t *np = page_shadow_alloc(mc->txn, ovpages);
            if (unlikely(!np))
              return MDBX_ENOMEM;

            memcpy(np, lp.page, PAGEHDRSZ); /* Copy header of page */
            err = page_dirty(mc->txn, lp.page = np, ovpages);
            if (unlikely(err != MDBX_SUCCESS))
              return err;

#if MDBX_ENABLE_PGOP_STAT
            mc->txn->env->lck->pgops.clone.weak += ovpages;
#endif /* MDBX_ENABLE_PGOP_STAT */
            cASSERT(mc, dpl_check(mc->txn));
          }
        }
        node_set_ds(node, data->iov_len);
        if (flags & MDBX_RESERVE)
          data->iov_base = page_data(lp.page);
        else
          memcpy(page_data(lp.page), data->iov_base, data->iov_len);

        if (AUDIT_ENABLED()) {
          err = cursor_validate(mc);
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
      cASSERT(mc, ptr_disp(old_data.iov_base, old_data.iov_len) <= ptr_disp(mc->pg[mc->top], env->ps));

      /* DB has dups? */
      if (mc->tree->flags & MDBX_DUPSORT) {
        /* Prepare (sub-)page/sub-DB to accept the new item, if needed.
         * fp: old sub-page or a header faking it.
         * mp: new (sub-)page.
         * xdata: node data with new sub-page or sub-DB. */
        size_t growth = 0; /* growth in page size.*/
        page_t *mp = fp = xdata.iov_base = env->page_auxbuf;
        mp->pgno = mc->pg[mc->top]->pgno;

        /* Was a single item before, must convert now */
        if (!(node_flags(node) & N_DUP)) {
          /* does data match? */
          if (flags & MDBX_APPENDDUP) {
            const int cmp = mc->clc->v.cmp(data, &old_data);
            cASSERT(mc, cmp != 0 || eq_fast(data, &old_data));
            if (unlikely(cmp <= 0))
              return MDBX_EKEYMISMATCH;
          } else if (eq_fast(data, &old_data)) {
            cASSERT(mc, mc->clc->v.cmp(data, &old_data) == 0);
            cASSERT(mc, !"Should not happen since" || batch_dupfix_done);
            if (flags & MDBX_NODUPDATA)
              return MDBX_KEYEXIST;
            /* data is match exactly byte-to-byte, nothing to update */
            rc = MDBX_SUCCESS;
            if (unlikely(batch_dupfix_done))
              goto batch_dupfix_continue;
            return rc;
          }

          /* Just overwrite the current item */
          if (flags & MDBX_CURRENT) {
            cASSERT(mc, node_size(key, data) <= env->leaf_nodemax);
            goto current;
          }

          /* Back up original data item */
          memcpy(old_singledup.iov_base = fp + 1, old_data.iov_base, old_singledup.iov_len = old_data.iov_len);

          /* Make sub-page header for the dup items, with dummy body */
          fp->flags = P_LEAF | P_SUBP;
          fp->lower = 0;
          xdata.iov_len = PAGEHDRSZ + old_data.iov_len + data->iov_len;
          if (mc->tree->flags & MDBX_DUPFIXED) {
            fp->flags |= P_DUPFIX;
            fp->dupfix_ksize = (uint16_t)data->iov_len;
            /* Будем создавать DUPFIX-страницу, как минимум с двумя элементами.
             * При коротких значениях и наличии свободного места можно сделать
             * некоторое резервирование места, чтобы при последующих добавлениях
             * не сразу расширять созданную под-страницу.
             * Резервирование в целом сомнительно (см ниже), но может сработать
             * в плюс (а если в минус то несущественный) при коротких ключах. */
            xdata.iov_len +=
                page_subleaf2_reserve(env, page_room(mc->pg[mc->top]) + old_data.iov_len, xdata.iov_len, data->iov_len);
            cASSERT(mc, (xdata.iov_len & 1) == 0);
          } else {
            xdata.iov_len += 2 * (sizeof(indx_t) + NODESIZE) + (old_data.iov_len & 1) + (data->iov_len & 1);
          }
          cASSERT(mc, (xdata.iov_len & 1) == 0);
          fp->upper = (uint16_t)(xdata.iov_len - PAGEHDRSZ);
          old_data.iov_len = xdata.iov_len; /* pretend olddata is fp */
        } else if (node_flags(node) & N_TREE) {
          /* Data is on sub-DB, just store it */
          flags |= N_DUP | N_TREE;
          goto dupsort_put;
        } else {
          /* Data is on sub-page */
          fp = old_data.iov_base;
          switch (flags) {
          default:
            growth = is_dupfix_leaf(fp) ? fp->dupfix_ksize : (node_size(data, nullptr) + sizeof(indx_t));
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
             * меньше leaf_nodemax. Соответственно, при превышении
             * просто сразу переходим на вложенное дерево. */
            xdata.iov_len = old_data.iov_len + (growth += growth & 1);
            if (xdata.iov_len > env->subpage_limit)
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
             *    только передвижения в entries[].
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
             *    заданы/скорректированы пользователем в ‰ от leaf_nodemax;
             *  - Используем под-страницу пока её размер меньше subpage_limit
             *    и на основной/гнездовой странице не-менее
             *    subpage_room_threshold свободного места;
             *  - Резервируем место только для 1-3 коротких dupfix-элементов,
             *    расширяя размер под-страницы на размер кэш-линии ЦПУ, но
             *    только если на странице не менее subpage_reserve_prereq
             *    свободного места.
             *  - По-умолчанию устанавливаем:
             *     subpage_limit = leaf_nodemax (1000‰);
             *     subpage_room_threshold = 0;
             *     subpage_reserve_prereq = leaf_nodemax (1000‰).
             */
            if (is_dupfix_leaf(fp))
              growth += page_subleaf2_reserve(env, page_room(mc->pg[mc->top]) + old_data.iov_len, xdata.iov_len,
                                              data->iov_len);
            else {
              /* TODO: Если добавить возможность для пользователя задавать
               * min/max размеров ключей/данных, то здесь разумно реализовать
               * тактику резервирования подобную dupfixed. */
            }
            break;

          case MDBX_CURRENT | MDBX_NODUPDATA:
          case MDBX_CURRENT:
          continue_subpage:
            fp->txnid = mc->txn->front_txnid;
            fp->pgno = mp->pgno;
            mc->subcur->cursor.pg[0] = fp;
            flags |= N_DUP;
            goto dupsort_put;
          }
          xdata.iov_len = old_data.iov_len + growth;
          cASSERT(mc, (xdata.iov_len & 1) == 0);
        }

        fp_flags = fp->flags;
        if (xdata.iov_len > env->subpage_limit || node_size_len(node_ks(node), xdata.iov_len) > env->leaf_nodemax ||
            (env->subpage_room_threshold &&
             page_room(mc->pg[mc->top]) + node_size_len(node_ks(node), old_data.iov_len) <
                 env->subpage_room_threshold + node_size_len(node_ks(node), xdata.iov_len))) {
          /* Too big for a sub-page, convert to sub-DB */
        convert_to_subtree:
          fp_flags &= ~P_SUBP;
          nested_dupdb.dupfix_size = 0;
          nested_dupdb.flags = flags_db2sub(mc->tree->flags);
          if (mc->tree->flags & MDBX_DUPFIXED) {
            fp_flags |= P_DUPFIX;
            nested_dupdb.dupfix_size = fp->dupfix_ksize;
          }
          nested_dupdb.height = 1;
          nested_dupdb.branch_pages = 0;
          nested_dupdb.leaf_pages = 1;
          nested_dupdb.large_pages = 0;
          nested_dupdb.items = page_numkeys(fp);
          xdata.iov_len = sizeof(nested_dupdb);
          xdata.iov_base = &nested_dupdb;
          const pgr_t par = gc_alloc_single(mc);
          mp = par.page;
          if (unlikely(par.err != MDBX_SUCCESS))
            return par.err;
          mc->tree->leaf_pages += 1;
          cASSERT(mc, env->ps > old_data.iov_len);
          growth = env->ps - (unsigned)old_data.iov_len;
          cASSERT(mc, (growth & 1) == 0);
          flags |= N_DUP | N_TREE;
          nested_dupdb.root = mp->pgno;
          nested_dupdb.sequence = 0;
          nested_dupdb.mod_txnid = mc->txn->txnid;
          sub_root = mp;
        }
        if (mp != fp) {
          mp->flags = fp_flags;
          mp->txnid = mc->txn->front_txnid;
          mp->dupfix_ksize = fp->dupfix_ksize;
          mp->lower = fp->lower;
          cASSERT(mc, fp->upper + growth < UINT16_MAX);
          mp->upper = fp->upper + (indx_t)growth;
          if (unlikely(fp_flags & P_DUPFIX)) {
            memcpy(page_data(mp), page_data(fp), page_numkeys(fp) * fp->dupfix_ksize);
            cASSERT(mc, (((mp->dupfix_ksize & page_numkeys(mp)) ^ mp->upper) & 1) == 0);
          } else {
            cASSERT(mc, (mp->upper & 1) == 0);
            memcpy(ptr_disp(mp, mp->upper + PAGEHDRSZ), ptr_disp(fp, fp->upper + PAGEHDRSZ),
                   old_data.iov_len - fp->upper - PAGEHDRSZ);
            memcpy(mp->entries, fp->entries, page_numkeys(fp) * sizeof(mp->entries[0]));
            for (size_t i = 0; i < page_numkeys(fp); i++) {
              cASSERT(mc, mp->entries[i] + growth <= UINT16_MAX);
              mp->entries[i] += (indx_t)growth;
            }
          }
        }

        if (!insert_key)
          node_del(mc, 0);
        ref_data = &xdata;
        flags |= N_DUP;
        goto insert_node;
      }

      /* MDBX passes N_TREE in 'flags' to write a DB record */
      if (unlikely((node_flags(node) ^ flags) & N_TREE))
        return MDBX_INCOMPATIBLE;

    current:
      if (data->iov_len == old_data.iov_len) {
        cASSERT(mc, EVEN_CEIL(key->iov_len) == EVEN_CEIL(node_ks(node)));
        /* same size, just replace it. Note that we could
         * also reuse this node if the new data is smaller,
         * but instead we opt to shrink the node in that case. */
        if (flags & MDBX_RESERVE)
          data->iov_base = old_data.iov_base;
        else if (!(mc->flags & z_inner))
          memcpy(old_data.iov_base, data->iov_base, data->iov_len);
        else {
          cASSERT(mc, page_numkeys(mc->pg[mc->top]) == 1);
          cASSERT(mc, page_type_compat(mc->pg[mc->top]) == P_LEAF);
          cASSERT(mc, node_ds(node) == 0);
          cASSERT(mc, node_flags(node) == 0);
          cASSERT(mc, key->iov_len < UINT16_MAX);
          node_set_ks(node, key->iov_len);
          memcpy(node_key(node), key->iov_base, key->iov_len);
          cASSERT(mc, ptr_disp(node_key(node), node_ds(node)) < ptr_disp(mc->pg[mc->top], env->ps));
          goto fix_parent;
        }

        if (AUDIT_ENABLED()) {
          err = cursor_validate(mc);
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
  size_t nsize = is_dupfix_leaf(mc->pg[mc->top]) ? key->iov_len : leaf_size(env, key, ref_data);
  if (page_room(mc->pg[mc->top]) < nsize) {
    rc = page_split(mc, key, ref_data, P_INVALID, insert_key ? naf : naf | MDBX_SPLIT_REPLACE);
    if (rc == MDBX_SUCCESS && AUDIT_ENABLED())
      rc = insert_key ? cursor_validate(mc) : cursor_validate_updating(mc);
  } else {
    /* There is room already in this leaf page. */
    if (is_dupfix_leaf(mc->pg[mc->top])) {
      cASSERT(mc, !(naf & (N_BIG | N_TREE | N_DUP)) && ref_data->iov_len == 0);
      rc = node_add_dupfix(mc, mc->ki[mc->top], key);
    } else
      rc = node_add_leaf(mc, mc->ki[mc->top], key, ref_data, naf);
    if (likely(rc == 0)) {
      /* Adjust other cursors pointing to mp */
      page_t *const mp = mc->pg[mc->top];
      const size_t dbi = cursor_dbi(mc);
      for (MDBX_cursor *m2 = mc->txn->cursors[dbi]; m2; m2 = m2->next) {
        MDBX_cursor *m3 = (mc->flags & z_inner) ? &m2->subcur->cursor : m2;
        if (!is_related(mc, m3) || m3->pg[mc->top] != mp)
          continue;
        if (m3->ki[mc->top] >= mc->ki[mc->top])
          m3->ki[mc->top] += insert_key;
        if (inner_pointed(m3))
          cursor_inner_refresh(m3, mp, m3->ki[mc->top]);
      }
    }
  }

  if (likely(rc == MDBX_SUCCESS)) {
    /* Now store the actual data in the child DB. Note that we're
     * storing the user data in the keys field, so there are strict
     * size limits on dupdata. The actual data fields of the child
     * DB are all zero size. */
    if (flags & N_DUP) {
      MDBX_val empty;
    dupsort_put:
      empty.iov_len = 0;
      empty.iov_base = nullptr;
      node_t *node = page_node(mc->pg[mc->top], mc->ki[mc->top]);
#define SHIFT_MDBX_NODUPDATA_TO_MDBX_NOOVERWRITE 1
      STATIC_ASSERT((MDBX_NODUPDATA >> SHIFT_MDBX_NODUPDATA_TO_MDBX_NOOVERWRITE) == MDBX_NOOVERWRITE);
      unsigned inner_flags = MDBX_CURRENT | ((flags & MDBX_NODUPDATA) >> SHIFT_MDBX_NODUPDATA_TO_MDBX_NOOVERWRITE);
      if ((flags & MDBX_CURRENT) == 0) {
        inner_flags -= MDBX_CURRENT;
        rc = cursor_dupsort_setup(mc, node, mc->pg[mc->top]);
        if (unlikely(rc != MDBX_SUCCESS))
          goto dupsort_error;
      }
      subcur_t *const mx = mc->subcur;
      if (sub_root) {
        cASSERT(mc, mx->nested_tree.height == 1 && mx->nested_tree.root == sub_root->pgno);
        mx->cursor.flags = z_inner;
        mx->cursor.top = 0;
        mx->cursor.pg[0] = sub_root;
        mx->cursor.ki[0] = 0;
      }
      if (old_singledup.iov_base) {
        /* converted, write the original data first */
        if (is_dupfix_leaf(mx->cursor.pg[0]))
          rc = node_add_dupfix(&mx->cursor, 0, &old_singledup);
        else
          rc = node_add_leaf(&mx->cursor, 0, &old_singledup, &empty, 0);
        if (unlikely(rc != MDBX_SUCCESS))
          goto dupsort_error;
        mx->cursor.tree->items = 1;
      }
      if (!(node_flags(node) & N_TREE) || sub_root) {
        page_t *const mp = mc->pg[mc->top];
        const intptr_t nkeys = page_numkeys(mp);
        const size_t dbi = cursor_dbi(mc);

        for (MDBX_cursor *m2 = mc->txn->cursors[dbi]; m2; m2 = m2->next) {
          if (!is_related(mc, m2) || m2->pg[mc->top] != mp)
            continue;
          if (/* пропускаем незаполненные курсоры, иначе получится что у такого
                 курсора будет инициализирован вложенный,
                 что антилогично и бесполезно. */
              is_filled(m2) && m2->ki[mc->top] == mc->ki[mc->top]) {
            cASSERT(m2, m2->subcur->cursor.clc == mx->cursor.clc);
            m2->subcur->nested_tree = mx->nested_tree;
            m2->subcur->cursor.pg[0] = mx->cursor.pg[0];
            if (old_singledup.iov_base) {
              m2->subcur->cursor.top_and_flags = z_inner;
              m2->subcur->cursor.ki[0] = 0;
            }
            DEBUG("Sub-dbi -%zu root page %" PRIaPGNO, cursor_dbi(&m2->subcur->cursor), m2->subcur->nested_tree.root);
          } else if (!insert_key && m2->ki[mc->top] < nkeys)
            cursor_inner_refresh(m2, mp, m2->ki[mc->top]);
        }
      }
      cASSERT(mc, mc->subcur->nested_tree.items < PTRDIFF_MAX);
      const size_t probe = (size_t)mc->subcur->nested_tree.items;
#define SHIFT_MDBX_APPENDDUP_TO_MDBX_APPEND 1
      STATIC_ASSERT((MDBX_APPENDDUP >> SHIFT_MDBX_APPENDDUP_TO_MDBX_APPEND) == MDBX_APPEND);
      inner_flags |= (flags & MDBX_APPENDDUP) >> SHIFT_MDBX_APPENDDUP_TO_MDBX_APPEND;
      rc = cursor_put(&mc->subcur->cursor, data, &empty, inner_flags);
      if (flags & N_TREE) {
        void *db = node_data(node);
        mc->subcur->nested_tree.mod_txnid = mc->txn->txnid;
        memcpy(db, &mc->subcur->nested_tree, sizeof(tree_t));
      }
      insert_data = (probe != (size_t)mc->subcur->nested_tree.items);
    }
    /* Increment count unless we just replaced an existing item. */
    if (insert_data)
      mc->tree->items++;
    if (insert_key) {
      if (unlikely(rc != MDBX_SUCCESS))
        goto dupsort_error;
      /* If we succeeded and the key didn't exist before,
       * make sure the cursor is marked valid. */
      be_filled(mc);
    }
    if (likely(rc == MDBX_SUCCESS)) {
      cASSERT(mc, is_filled(mc));
      if (unlikely(batch_dupfix_done)) {
      batch_dupfix_continue:
        /* let caller know how many succeeded, if any */
        if ((*batch_dupfix_done += 1) < batch_dupfix_given) {
          data[0].iov_base = ptr_disp(data[0].iov_base, data[0].iov_len);
          insert_key = insert_data = false;
          old_singledup.iov_base = nullptr;
          sub_root = nullptr;
          goto more;
        }
      }
      if (AUDIT_ENABLED())
        rc = cursor_validate(mc);
    }
    return rc;

  dupsort_error:
    if (unlikely(rc == MDBX_KEYEXIST)) {
      /* should not happen, we deleted that item */
      ERROR("Unexpected %i error while put to nested dupsort's hive", rc);
      rc = MDBX_PROBLEM;
    }
  }
  mc->txn->flags |= MDBX_TXN_ERROR;
  return rc;
}

int cursor_check_multiple(MDBX_cursor *mc, const MDBX_val *key, MDBX_val *data, unsigned flags) {
  (void)key;
  if (unlikely(flags & MDBX_RESERVE))
    return MDBX_EINVAL;
  if (unlikely(!(mc->tree->flags & MDBX_DUPFIXED)))
    return MDBX_INCOMPATIBLE;
  const size_t number = data[1].iov_len;
  if (unlikely(number > MAX_MAPSIZE / 2 / (BRANCH_NODE_MAX(MDBX_MAX_PAGESIZE) - NODESIZE))) {
    /* checking for multiplication overflow */
    if (unlikely(number > MAX_MAPSIZE / 2 / data->iov_len))
      return MDBX_TOO_LARGE;
  }
  return MDBX_SUCCESS;
}

__hot int cursor_put_checklen(MDBX_cursor *mc, const MDBX_val *key, MDBX_val *data, unsigned flags) {
  cASSERT(mc, (mc->flags & z_inner) == 0);
  if (unlikely(key->iov_len > mc->clc->k.lmax || key->iov_len < mc->clc->k.lmin)) {
    cASSERT(mc, !"Invalid key-size");
    return MDBX_BAD_VALSIZE;
  }
  if (unlikely(data->iov_len > mc->clc->v.lmax || data->iov_len < mc->clc->v.lmin)) {
    cASSERT(mc, !"Invalid data-size");
    return MDBX_BAD_VALSIZE;
  }

  uint64_t aligned_keybytes, aligned_databytes;
  MDBX_val aligned_key, aligned_data;
  if (mc->tree->flags & MDBX_INTEGERKEY) {
    if (key->iov_len == 8) {
      if (unlikely(7 & (uintptr_t)key->iov_base)) {
        /* copy instead of return error to avoid break compatibility */
        aligned_key.iov_base = bcopy_8(&aligned_keybytes, key->iov_base);
        aligned_key.iov_len = key->iov_len;
        key = &aligned_key;
      }
    } else if (key->iov_len == 4) {
      if (unlikely(3 & (uintptr_t)key->iov_base)) {
        /* copy instead of return error to avoid break compatibility */
        aligned_key.iov_base = bcopy_4(&aligned_keybytes, key->iov_base);
        aligned_key.iov_len = key->iov_len;
        key = &aligned_key;
      }
    } else {
      cASSERT(mc, !"key-size is invalid for MDBX_INTEGERKEY");
      return MDBX_BAD_VALSIZE;
    }
  }
  if (mc->tree->flags & MDBX_INTEGERDUP) {
    if (data->iov_len == 8) {
      if (unlikely(7 & (uintptr_t)data->iov_base)) {
        if (unlikely(flags & MDBX_MULTIPLE)) {
          /* LY: использование alignof(uint64_t) тут не подходил из-за ошибок
           * MSVC и некоторых других компиляторов, когда для элементов
           * массивов/векторов обеспечивает выравнивание только на 4-х байтовых
           * границу и одновременно alignof(uint64_t) == 8. */
          if (MDBX_WORDBITS > 32 || (3 & (uintptr_t)data->iov_base) != 0)
            return MDBX_BAD_VALSIZE;
        } else {
          /* copy instead of return error to avoid break compatibility */
          aligned_data.iov_base = bcopy_8(&aligned_databytes, data->iov_base);
          aligned_data.iov_len = data->iov_len;
          data = &aligned_data;
        }
      }
    } else if (data->iov_len == 4) {
      if (unlikely(3 & (uintptr_t)data->iov_base)) {
        if (unlikely(flags & MDBX_MULTIPLE))
          return MDBX_BAD_VALSIZE;
        /* copy instead of return error to avoid break compatibility */
        aligned_data.iov_base = bcopy_4(&aligned_databytes, data->iov_base);
        aligned_data.iov_len = data->iov_len;
        data = &aligned_data;
      }
    } else {
      cASSERT(mc, !"data-size is invalid for MDBX_INTEGERKEY");
      return MDBX_BAD_VALSIZE;
    }
  }
  return cursor_put(mc, key, data, flags);
}

__hot int cursor_del(MDBX_cursor *mc, unsigned flags) {
  if (unlikely(!is_filled(mc)))
    return MDBX_ENODATA;

  int rc = cursor_touch(mc, nullptr, nullptr);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  page_t *mp = mc->pg[mc->top];
  cASSERT(mc, is_modifable(mc->txn, mp));
  if (!MDBX_DISABLE_VALIDATION && unlikely(!check_leaf_type(mc, mp))) {
    ERROR("unexpected leaf-page #%" PRIaPGNO " type 0x%x seen by cursor", mp->pgno, mp->flags);
    return MDBX_CORRUPTED;
  }
  if (is_dupfix_leaf(mp))
    goto del_key;

  node_t *node = page_node(mp, mc->ki[mc->top]);
  if (node_flags(node) & N_DUP) {
    if (flags & (MDBX_ALLDUPS | /* for compatibility */ MDBX_NODUPDATA)) {
      /* will subtract the final entry later */
      mc->tree->items -= mc->subcur->nested_tree.items - 1;
    } else {
      if (!(node_flags(node) & N_TREE)) {
        page_t *sp = node_data(node);
        cASSERT(mc, is_subpage(sp));
        sp->txnid = mp->txnid;
        mc->subcur->cursor.pg[0] = sp;
      }
      rc = cursor_del(&mc->subcur->cursor, 0);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
      /* If sub-DB still has entries, we're done */
      if (mc->subcur->nested_tree.items) {
        if (node_flags(node) & N_TREE) {
          /* update table info */
          mc->subcur->nested_tree.mod_txnid = mc->txn->txnid;
          memcpy(node_data(node), &mc->subcur->nested_tree, sizeof(tree_t));
        } else {
          /* shrink sub-page */
          node = node_shrink(mp, mc->ki[mc->top], node);
          mc->subcur->cursor.pg[0] = node_data(node);
          /* fix other sub-DB cursors pointed at sub-pages on this page */
          for (MDBX_cursor *m2 = mc->txn->cursors[cursor_dbi(mc)]; m2; m2 = m2->next) {
            if (!is_related(mc, m2) || m2->pg[mc->top] != mp)
              continue;
            const node_t *inner = node;
            if (unlikely(m2->ki[mc->top] >= page_numkeys(mp))) {
              m2->flags = z_poor_mark;
              m2->subcur->nested_tree.root = 0;
              m2->subcur->cursor.top_and_flags = z_inner | z_poor_mark;
              continue;
            }
            if (m2->ki[mc->top] != mc->ki[mc->top]) {
              inner = page_node(mp, m2->ki[mc->top]);
              if (node_flags(inner) & N_TREE)
                continue;
            }
            m2->subcur->cursor.pg[0] = node_data(inner);
          }
        }
        mc->tree->items -= 1;
        cASSERT(mc, mc->tree->items > 0 && mc->tree->height > 0 && mc->tree->root != P_INVALID);
        return rc;
      }
      /* otherwise fall thru and delete the sub-DB */
    }

    if ((node_flags(node) & N_TREE) && mc->subcur->cursor.tree->height) {
      /* add all the child DB's pages to the free list */
      rc = tree_drop(&mc->subcur->cursor, false);
      if (unlikely(rc != MDBX_SUCCESS))
        goto fail;
    }
    inner_gone(mc);
  } else {
    cASSERT(mc, !inner_pointed(mc));
    /* MDBX passes N_TREE in 'flags' to delete a DB record */
    if (unlikely((node_flags(node) ^ flags) & N_TREE))
      return MDBX_INCOMPATIBLE;
  }

  /* add large/overflow pages to free list */
  if (node_flags(node) & N_BIG) {
    pgr_t lp = page_get_large(mc, node_largedata_pgno(node), mp->txnid);
    if (unlikely((rc = lp.err) || (rc = page_retire(mc, lp.page))))
      goto fail;
  }

del_key:
  mc->tree->items -= 1;
  const MDBX_dbi dbi = cursor_dbi(mc);
  indx_t ki = mc->ki[mc->top];
  mp = mc->pg[mc->top];
  cASSERT(mc, is_leaf(mp));
  node_del(mc, mc->tree->dupfix_size);

  /* Adjust other cursors pointing to mp */
  for (MDBX_cursor *m2 = mc->txn->cursors[dbi]; m2; m2 = m2->next) {
    MDBX_cursor *m3 = (mc->flags & z_inner) ? &m2->subcur->cursor : m2;
    if (!is_related(mc, m3) || m3->pg[mc->top] != mp)
      continue;
    if (m3->ki[mc->top] == ki) {
      m3->flags |= z_after_delete;
      inner_gone(m3);
    } else {
      m3->ki[mc->top] -= m3->ki[mc->top] > ki;
      if (inner_pointed(m3))
        cursor_inner_refresh(m3, m3->pg[mc->top], m3->ki[mc->top]);
    }
  }

  rc = tree_rebalance(mc);
  if (unlikely(rc != MDBX_SUCCESS))
    goto fail;

  mc->flags |= z_after_delete;
  inner_gone(mc);
  if (unlikely(mc->top < 0)) {
    /* DB is totally empty now, just bail out.
     * Other cursors adjustments were already done
     * by rebalance and aren't needed here. */
    cASSERT(mc, mc->tree->items == 0 && (mc->tree->root == P_INVALID || (is_inner(mc) && !mc->tree->root)) &&
                    mc->flags < 0);
    return MDBX_SUCCESS;
  }

  ki = mc->ki[mc->top];
  mp = mc->pg[mc->top];
  cASSERT(mc, is_leaf(mc->pg[mc->top]));
  size_t nkeys = page_numkeys(mp);
  cASSERT(mc, (mc->tree->items > 0 && nkeys > 0) || ((mc->flags & z_inner) && mc->tree->items == 0 && nkeys == 0));

  /* Adjust this and other cursors pointing to mp */
  const intptr_t top = /* может быть сброшен в -1 */ mc->top;
  for (MDBX_cursor *m2 = mc->txn->cursors[dbi]; m2; m2 = m2->next) {
    MDBX_cursor *m3 = (mc->flags & z_inner) ? &m2->subcur->cursor : m2;
    if (top > m3->top || m3->pg[top] != mp)
      continue;
    /* if m3 points past last node in page, find next sibling */
    if (m3->ki[top] >= nkeys) {
      rc = cursor_sibling_right(m3);
      if (rc == MDBX_NOTFOUND) {
        rc = MDBX_SUCCESS;
        continue;
      }
      if (unlikely(rc != MDBX_SUCCESS))
        goto fail;
    }
    if (/* пропускаем незаполненные курсоры, иначе получится что у такого
           курсора будет инициализирован вложенный,
           что антилогично и бесполезно. */
        is_filled(m3) && m3->subcur &&
        (m3->ki[top] >= ki ||
         /* уже переместились вправо */ m3->pg[top] != mp)) {
      node = page_node(m3->pg[m3->top], m3->ki[m3->top]);
      /* Если это dupsort-узел, то должен быть валидный вложенный курсор. */
      if (node_flags(node) & N_DUP) {
        /* Тут три варианта событий:
         * 1) Вложенный курсор уже инициализирован, у узла есть флаг N_TREE,
         *    соответственно дубликаты вынесены в отдельное дерево с корнем
         *    в отдельной странице = ничего корректировать не требуется.
         * 2) Вложенный курсор уже инициализирован, у узла нет флага N_TREE,
         *    соответственно дубликаты размещены на вложенной sub-странице.
         * 3) Курсор стоял на удалённом элементе, который имел одно значение,
         *    а после удаления переместился на следующий элемент с дубликатами.
         *    В этом случае вложенный курсор не инициализирован и тепеь его
         *    нужно установить на первый дубликат. */
        if (is_pointed(&m3->subcur->cursor)) {
          if ((node_flags(node) & N_TREE) == 0) {
            cASSERT(m3, m3->subcur->cursor.top == 0 && m3->subcur->nested_tree.height == 1);
            m3->subcur->cursor.pg[0] = node_data(node);
          }
        } else {
          rc = cursor_dupsort_setup(m3, node, m3->pg[m3->top]);
          if (unlikely(rc != MDBX_SUCCESS))
            goto fail;
          if (node_flags(node) & N_TREE) {
            rc = inner_first(&m3->subcur->cursor, nullptr);
            if (unlikely(rc != MDBX_SUCCESS))
              goto fail;
          }
        }
      } else
        inner_gone(m3);
    }
  }

  cASSERT(mc, rc == MDBX_SUCCESS);
  if (AUDIT_ENABLED())
    rc = cursor_validate(mc);
  return rc;

fail:
  mc->txn->flags |= MDBX_TXN_ERROR;
  return rc;
}

/*----------------------------------------------------------------------------*/

__hot csr_t cursor_seek(MDBX_cursor *mc, MDBX_val *key, MDBX_val *data, MDBX_cursor_op op) {
  DKBUF_DEBUG;

  csr_t ret;
  ret.exact = false;
  if (unlikely(key->iov_len < mc->clc->k.lmin ||
               (key->iov_len > mc->clc->k.lmax &&
                (mc->clc->k.lmin == mc->clc->k.lmax || MDBX_DEBUG || MDBX_FORCE_ASSERTIONS)))) {
    cASSERT(mc, !"Invalid key-size");
    ret.err = MDBX_BAD_VALSIZE;
    return ret;
  }

  MDBX_val aligned_key = *key;
  uint64_t aligned_key_buf;
  if (mc->tree->flags & MDBX_INTEGERKEY) {
    if (aligned_key.iov_len == 8) {
      if (unlikely(7 & (uintptr_t)aligned_key.iov_base))
        /* copy instead of return error to avoid break compatibility */
        aligned_key.iov_base = bcopy_8(&aligned_key_buf, aligned_key.iov_base);
    } else if (aligned_key.iov_len == 4) {
      if (unlikely(3 & (uintptr_t)aligned_key.iov_base))
        /* copy instead of return error to avoid break compatibility */
        aligned_key.iov_base = bcopy_4(&aligned_key_buf, aligned_key.iov_base);
    } else {
      cASSERT(mc, !"key-size is invalid for MDBX_INTEGERKEY");
      ret.err = MDBX_BAD_VALSIZE;
      return ret;
    }
  }

  page_t *mp;
  node_t *node = nullptr;
  /* See if we're already on the right page */
  if (is_pointed(mc)) {
    mp = mc->pg[mc->top];
    cASSERT(mc, is_leaf(mp));
    const size_t nkeys = page_numkeys(mp);
    if (unlikely(nkeys == 0)) {
      /* при создании первой листовой страницы */
      cASSERT(mc, mc->top == 0 && mc->tree->height == 1 && mc->tree->branch_pages == 0 && mc->tree->leaf_pages == 1 &&
                      mc->ki[0] == 0);
      /* Логически верно, но нет смысла, ибо это мимолетная/временная
       * ситуация до добавления элемента выше по стеку вызовов:
         mc->flags |= z_eof_soft | z_hollow; */
      ret.err = MDBX_NOTFOUND;
      return ret;
    }

    MDBX_val nodekey;
    if (is_dupfix_leaf(mp))
      nodekey = page_dupfix_key(mp, 0, mc->tree->dupfix_size);
    else {
      node = page_node(mp, 0);
      nodekey = get_key(node);
      inner_gone(mc);
    }
    int cmp = mc->clc->k.cmp(&aligned_key, &nodekey);
    if (unlikely(cmp == 0)) {
      /* Probably happens rarely, but first node on the page
       * was the one we wanted. */
      mc->ki[mc->top] = 0;
      ret.exact = true;
      goto got_node;
    }

    if (cmp > 0) {
      /* Искомый ключ больше первого на этой странице,
       * целевая позиция на этой странице либо правее (ближе к концу). */
      if (likely(nkeys > 1)) {
        if (is_dupfix_leaf(mp)) {
          nodekey.iov_base = page_dupfix_ptr(mp, nkeys - 1, nodekey.iov_len);
        } else {
          node = page_node(mp, nkeys - 1);
          nodekey = get_key(node);
        }
        cmp = mc->clc->k.cmp(&aligned_key, &nodekey);
        if (cmp == 0) {
          /* last node was the one we wanted */
          mc->ki[mc->top] = (indx_t)(nkeys - 1);
          ret.exact = true;
          goto got_node;
        }
        if (cmp < 0) {
          /* Искомый ключ между первым и последним на этой страницы,
           * поэтому пропускаем поиск по дереву и продолжаем только на текущей
           * странице. */
          /* Сравниваем с текущей позицией, ибо частным сценарием является такое
           * совпадение, но не делаем проверку если текущая позиция является
           * первой/последний и соответственно такое сравнение было выше. */
          if (mc->ki[mc->top] > 0 && mc->ki[mc->top] < nkeys - 1) {
            if (is_dupfix_leaf(mp)) {
              nodekey.iov_base = page_dupfix_ptr(mp, mc->ki[mc->top], nodekey.iov_len);
            } else {
              node = page_node(mp, mc->ki[mc->top]);
              nodekey = get_key(node);
            }
            cmp = mc->clc->k.cmp(&aligned_key, &nodekey);
            if (cmp == 0) {
              /* current node was the one we wanted */
              ret.exact = true;
              goto got_node;
            }
          }
          goto search_node;
        }
      }

      /* Если в стеке курсора есть страницы справа, то продолжим искать там. */
      cASSERT(mc, mc->tree->height > mc->top);
      for (intptr_t i = 0; i < mc->top; i++)
        if ((size_t)mc->ki[i] + 1 < page_numkeys(mc->pg[i]))
          goto continue_other_pages;

      /* Ключ больше последнего. */
      mc->ki[mc->top] = (indx_t)nkeys;
      if (op < MDBX_SET_RANGE) {
      target_not_found:
        cASSERT(mc, op == MDBX_SET || op == MDBX_SET_KEY || op == MDBX_GET_BOTH || op == MDBX_GET_BOTH_RANGE);
        /* Операция предполагает поиск конкретного ключа, который не найден.
         * Поэтому переводим курсор в неустановленное состояние, но без сброса
         * top, что позволяет работать fastpath при последующем поиске по дереву
         * страниц. */
        mc->flags |= z_hollow;
        if (inner_pointed(mc))
          mc->subcur->cursor.flags |= z_hollow;
        ret.err = MDBX_NOTFOUND;
        return ret;
      }
      cASSERT(mc, op == MDBX_SET_RANGE);
      mc->flags = z_eof_soft | z_eof_hard | (mc->flags & z_clear_mask);
      ret.err = MDBX_NOTFOUND;
      return ret;
    }

    if (mc->top == 0) {
      /* There are no other pages */
      mc->ki[mc->top] = 0;
      if (op >= MDBX_SET_RANGE)
        goto got_node;
      else
        goto target_not_found;
    }
  }
  cASSERT(mc, !inner_pointed(mc));

continue_other_pages:
  ret.err = tree_search(mc, &aligned_key, 0);
  if (unlikely(ret.err != MDBX_SUCCESS))
    return ret;

  cASSERT(mc, is_pointed(mc) && !inner_pointed(mc));
  mp = mc->pg[mc->top];
  MDBX_ANALYSIS_ASSUME(mp != nullptr);
  cASSERT(mc, is_leaf(mp));

search_node:
  cASSERT(mc, is_pointed(mc) && !inner_pointed(mc));
  struct node_search_result nsr = node_search(mc, &aligned_key);
  node = nsr.node;
  ret.exact = nsr.exact;
  if (!ret.exact) {
    if (op < MDBX_SET_RANGE)
      goto target_not_found;

    if (node == nullptr) {
      DEBUG("%s", "===> inexact leaf not found, goto sibling");
      ret.err = cursor_sibling_right(mc);
      if (unlikely(ret.err != MDBX_SUCCESS))
        return ret; /* no entries matched */
      mp = mc->pg[mc->top];
      cASSERT(mc, is_leaf(mp));
      if (!is_dupfix_leaf(mp))
        node = page_node(mp, 0);
    }
  }

got_node:
  cASSERT(mc, is_pointed(mc) && !inner_pointed(mc));
  cASSERT(mc, mc->ki[mc->top] < page_numkeys(mc->pg[mc->top]));
  if (!MDBX_DISABLE_VALIDATION && unlikely(!check_leaf_type(mc, mp))) {
    ERROR("unexpected leaf-page #%" PRIaPGNO " type 0x%x seen by cursor", mp->pgno, mp->flags);
    ret.err = MDBX_CORRUPTED;
    return ret;
  }

  if (is_dupfix_leaf(mp)) {
    if (op >= MDBX_SET_KEY)
      *key = page_dupfix_key(mp, mc->ki[mc->top], mc->tree->dupfix_size);
    be_filled(mc);
    ret.err = MDBX_SUCCESS;
    return ret;
  }

  if (node_flags(node) & N_DUP) {
    ret.err = cursor_dupsort_setup(mc, node, mp);
    if (unlikely(ret.err != MDBX_SUCCESS))
      return ret;
    if (op >= MDBX_SET) {
      MDBX_ANALYSIS_ASSUME(mc->subcur != nullptr);
      if (node_flags(node) & N_TREE) {
        ret.err = inner_first(&mc->subcur->cursor, data);
        if (unlikely(ret.err != MDBX_SUCCESS))
          return ret;
      } else if (data) {
        const page_t *inner_mp = mc->subcur->cursor.pg[0];
        cASSERT(mc, is_subpage(inner_mp) && is_leaf(inner_mp));
        const size_t inner_ki = mc->subcur->cursor.ki[0];
        if (is_dupfix_leaf(inner_mp))
          *data = page_dupfix_key(inner_mp, inner_ki, mc->tree->dupfix_size);
        else
          *data = get_key(page_node(inner_mp, inner_ki));
      }
    } else {
      MDBX_ANALYSIS_ASSUME(mc->subcur != nullptr);
      ret = cursor_seek(&mc->subcur->cursor, data, nullptr, MDBX_SET_RANGE);
      if (unlikely(ret.err != MDBX_SUCCESS)) {
        if (ret.err == MDBX_NOTFOUND && op < MDBX_SET_RANGE)
          goto target_not_found;
        return ret;
      }
      if (op == MDBX_GET_BOTH && !ret.exact)
        goto target_not_found;
    }
  } else if (likely(data)) {
    if (op <= MDBX_GET_BOTH_RANGE) {
      if (unlikely(data->iov_len < mc->clc->v.lmin || data->iov_len > mc->clc->v.lmax)) {
        cASSERT(mc, !"Invalid data-size");
        ret.err = MDBX_BAD_VALSIZE;
        return ret;
      }
      MDBX_val aligned_data = *data;
      uint64_t aligned_databytes;
      if (mc->tree->flags & MDBX_INTEGERDUP) {
        if (aligned_data.iov_len == 8) {
          if (unlikely(7 & (uintptr_t)aligned_data.iov_base))
            /* copy instead of return error to avoid break compatibility */
            aligned_data.iov_base = bcopy_8(&aligned_databytes, aligned_data.iov_base);
        } else if (aligned_data.iov_len == 4) {
          if (unlikely(3 & (uintptr_t)aligned_data.iov_base))
            /* copy instead of return error to avoid break compatibility */
            aligned_data.iov_base = bcopy_4(&aligned_databytes, aligned_data.iov_base);
        } else {
          cASSERT(mc, !"data-size is invalid for MDBX_INTEGERDUP");
          ret.err = MDBX_BAD_VALSIZE;
          return ret;
        }
      }
      MDBX_val actual_data;
      ret.err = node_read(mc, node, &actual_data, mc->pg[mc->top]);
      if (unlikely(ret.err != MDBX_SUCCESS))
        return ret;
      const int cmp = mc->clc->v.cmp(&aligned_data, &actual_data);
      if (cmp) {
        if (op != MDBX_GET_BOTH_RANGE) {
          cASSERT(mc, op == MDBX_GET_BOTH);
          goto target_not_found;
        }
        if (cmp > 0) {
          ret.err = MDBX_NOTFOUND;
          return ret;
        }
      }
      *data = actual_data;
    } else {
      ret.err = node_read(mc, node, data, mc->pg[mc->top]);
      if (unlikely(ret.err != MDBX_SUCCESS))
        return ret;
    }
  }

  /* The key already matches in all other cases */
  if (op >= MDBX_SET_KEY)
    get_key_optional(node, key);

  DEBUG("==> cursor placed on key [%s], data [%s]", DKEY_DEBUG(key), DVAL_DEBUG(data));
  ret.err = MDBX_SUCCESS;
  be_filled(mc);
  return ret;
}

__hot int cursor_ops(MDBX_cursor *mc, MDBX_val *key, MDBX_val *data, const MDBX_cursor_op op) {
  if (op != MDBX_GET_CURRENT)
    DEBUG(">> cursor %p(0x%x), ops %u, key %p, value %p", __Wpedantic_format_voidptr(mc), mc->flags, op,
          __Wpedantic_format_voidptr(key), __Wpedantic_format_voidptr(data));
  int rc;

  switch (op) {
  case MDBX_GET_CURRENT:
    cASSERT(mc, (mc->flags & z_inner) == 0);
    if (unlikely(!is_filled(mc))) {
      if (is_hollow(mc))
        return MDBX_ENODATA;
      if (mc->ki[mc->top] >= page_numkeys(mc->pg[mc->top]))
        return MDBX_NOTFOUND;
    }
    if (mc->flags & z_after_delete)
      return outer_next(mc, key, data, MDBX_NEXT_NODUP);
    else if (inner_pointed(mc) && (mc->subcur->cursor.flags & z_after_delete))
      return outer_next(mc, key, data, MDBX_NEXT_DUP);
    else {
      const page_t *mp = mc->pg[mc->top];
      const node_t *node = page_node(mp, mc->ki[mc->top]);
      get_key_optional(node, key);
      if (!data)
        return MDBX_SUCCESS;
      if (node_flags(node) & N_DUP) {
        if (!MDBX_DISABLE_VALIDATION && unlikely(!mc->subcur))
          return unexpected_dupsort(mc);
        mc = &mc->subcur->cursor;
        if (unlikely(!is_filled(mc))) {
          if (is_hollow(mc))
            return MDBX_ENODATA;
          if (mc->ki[mc->top] >= page_numkeys(mc->pg[mc->top]))
            return MDBX_NOTFOUND;
        }
        mp = mc->pg[mc->top];
        if (is_dupfix_leaf(mp))
          *data = page_dupfix_key(mp, mc->ki[mc->top], mc->tree->dupfix_size);
        else
          *data = get_key(page_node(mp, mc->ki[mc->top]));
        return MDBX_SUCCESS;
      } else {
        cASSERT(mc, !inner_pointed(mc));
        return node_read(mc, node, data, mc->pg[mc->top]);
      }
    }

  case MDBX_GET_BOTH:
  case MDBX_GET_BOTH_RANGE:
    if (unlikely(data == nullptr))
      return MDBX_EINVAL;
    if (unlikely(mc->subcur == nullptr))
      return MDBX_INCOMPATIBLE;
    /* fall through */
    __fallthrough;
  case MDBX_SET:
  case MDBX_SET_KEY:
  case MDBX_SET_RANGE:
    if (unlikely(key == nullptr))
      return MDBX_EINVAL;
    rc = cursor_seek(mc, key, data, op).err;
    if (rc == MDBX_SUCCESS)
      cASSERT(mc, is_filled(mc));
    else if (rc == MDBX_NOTFOUND && mc->tree->items) {
      cASSERT(mc, is_pointed(mc));
      cASSERT(mc, op == MDBX_SET_RANGE || op == MDBX_GET_BOTH_RANGE || is_hollow(mc));
      cASSERT(mc, op == MDBX_GET_BOTH_RANGE || inner_hollow(mc));
    } else
      cASSERT(mc, is_poor(mc) && !is_filled(mc));
    return rc;

  case MDBX_SEEK_AND_GET_MULTIPLE:
    if (unlikely(!key))
      return MDBX_EINVAL;
    rc = cursor_seek(mc, key, data, MDBX_SET).err;
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    __fallthrough /* fall through */;
  case MDBX_GET_MULTIPLE:
    if (unlikely(!data))
      return MDBX_EINVAL;
    if (unlikely((mc->tree->flags & MDBX_DUPFIXED) == 0))
      return MDBX_INCOMPATIBLE;
    if (unlikely(!is_filled(mc)))
      return MDBX_ENODATA;
    if (key) {
      const page_t *mp = mc->pg[mc->top];
      const node_t *node = page_node(mp, mc->ki[mc->top]);
      *key = get_key(node);
    }
    cASSERT(mc, is_filled(mc));
    if (unlikely(!inner_filled(mc))) {
      if (inner_pointed(mc))
        return MDBX_ENODATA;
      const page_t *mp = mc->pg[mc->top];
      const node_t *node = page_node(mp, mc->ki[mc->top]);
      return node_read(mc, node, data, mp);
    }
    goto fetch_multiple;

  case MDBX_NEXT_MULTIPLE:
    if (unlikely(!data))
      return MDBX_EINVAL;
    if (unlikely(mc->subcur == nullptr))
      return MDBX_INCOMPATIBLE;
    rc = outer_next(mc, key, data, MDBX_NEXT_DUP);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    else {
    fetch_multiple:
      cASSERT(mc, is_filled(mc) && inner_filled(mc));
      MDBX_cursor *mx = &mc->subcur->cursor;
      data->iov_len = page_numkeys(mx->pg[mx->top]) * mx->tree->dupfix_size;
      data->iov_base = page_data(mx->pg[mx->top]);
      mx->ki[mx->top] = (indx_t)page_numkeys(mx->pg[mx->top]) - 1;
      return MDBX_SUCCESS;
    }

  case MDBX_PREV_MULTIPLE:
    if (unlikely(!data))
      return MDBX_EINVAL;
    if (unlikely(mc->subcur == nullptr))
      return MDBX_INCOMPATIBLE;
    if (unlikely(!is_filled(mc) || !inner_filled(mc)))
      return MDBX_ENODATA;
    rc = cursor_sibling_left(&mc->subcur->cursor);
    if (likely(rc == MDBX_SUCCESS))
      goto fetch_multiple;
    return rc;

  case MDBX_NEXT_DUP:
  case MDBX_NEXT:
  case MDBX_NEXT_NODUP:
    rc = outer_next(mc, key, data, op);
    mc->flags &= ~z_eof_hard;
    ((cursor_couple_t *)mc)->inner.cursor.flags &= ~z_eof_hard;
    return rc;

  case MDBX_PREV_DUP:
  case MDBX_PREV:
  case MDBX_PREV_NODUP:
    return outer_prev(mc, key, data, op);

  case MDBX_FIRST:
    return outer_first(mc, key, data);
  case MDBX_LAST:
    return outer_last(mc, key, data);

  case MDBX_LAST_DUP:
  case MDBX_FIRST_DUP:
    if (unlikely(data == nullptr))
      return MDBX_EINVAL;
    if (unlikely(!is_filled(mc)))
      return MDBX_ENODATA;
    else {
      node_t *node = page_node(mc->pg[mc->top], mc->ki[mc->top]);
      get_key_optional(node, key);
      if ((node_flags(node) & N_DUP) == 0)
        return node_read(mc, node, data, mc->pg[mc->top]);
      else if (MDBX_DISABLE_VALIDATION || likely(mc->subcur))
        return ((op == MDBX_FIRST_DUP) ? inner_first : inner_last)(&mc->subcur->cursor, data);
      else
        return unexpected_dupsort(mc);
    }
    break;

  case MDBX_SET_UPPERBOUND:
  case MDBX_SET_LOWERBOUND:
    if (unlikely(key == nullptr || data == nullptr))
      return MDBX_EINVAL;
    else {
      MDBX_val save_data = *data;
      csr_t csr = cursor_seek(mc, key, data, MDBX_SET_RANGE);
      rc = csr.err;
      if (rc == MDBX_SUCCESS && csr.exact && mc->subcur) {
        csr.exact = false;
        if (!save_data.iov_base) {
          /* Avoiding search nested dupfix hive if no data provided.
           * This is changes the semantic of MDBX_SET_LOWERBOUND but avoid
           * returning MDBX_BAD_VALSIZE. */
        } else if (is_pointed(&mc->subcur->cursor)) {
          *data = save_data;
          csr = cursor_seek(&mc->subcur->cursor, data, nullptr, MDBX_SET_RANGE);
          rc = csr.err;
          if (rc == MDBX_NOTFOUND) {
            cASSERT(mc, !csr.exact);
            rc = outer_next(mc, key, data, MDBX_NEXT_NODUP);
          }
        } else {
          int cmp = mc->clc->v.cmp(&save_data, data);
          csr.exact = (cmp == 0);
          if (cmp > 0)
            rc = outer_next(mc, key, data, MDBX_NEXT_NODUP);
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
          rc = outer_next(mc, key, data, MDBX_NEXT);
      }
    }
    return rc;

  /* Doubtless API to positioning of the cursor at a specified key. */
  case MDBX_TO_KEY_LESSER_THAN:
  case MDBX_TO_KEY_LESSER_OR_EQUAL:
  case MDBX_TO_KEY_EQUAL:
  case MDBX_TO_KEY_GREATER_OR_EQUAL:
  case MDBX_TO_KEY_GREATER_THAN:
    if (unlikely(key == nullptr))
      return MDBX_EINVAL;
    else {
      csr_t csr = cursor_seek(mc, key, data, MDBX_SET_RANGE);
      rc = csr.err;
      if (csr.exact) {
        cASSERT(mc, csr.err == MDBX_SUCCESS);
        if (op == MDBX_TO_KEY_LESSER_THAN)
          rc = outer_prev(mc, key, data, MDBX_PREV_NODUP);
        else if (op == MDBX_TO_KEY_GREATER_THAN)
          rc = outer_next(mc, key, data, MDBX_NEXT_NODUP);
      } else if (op < MDBX_TO_KEY_EQUAL && (rc == MDBX_NOTFOUND || rc == MDBX_SUCCESS))
        rc = outer_prev(mc, key, data, MDBX_PREV_NODUP);
      else if (op == MDBX_TO_KEY_EQUAL && rc == MDBX_SUCCESS)
        rc = MDBX_NOTFOUND;
    }
    return rc;

  /* Doubtless API to positioning of the cursor at a specified key-value pair
   * for multi-value hives. */
  case MDBX_TO_EXACT_KEY_VALUE_LESSER_THAN:
  case MDBX_TO_EXACT_KEY_VALUE_LESSER_OR_EQUAL:
  case MDBX_TO_EXACT_KEY_VALUE_EQUAL:
  case MDBX_TO_EXACT_KEY_VALUE_GREATER_OR_EQUAL:
  case MDBX_TO_EXACT_KEY_VALUE_GREATER_THAN:
    if (unlikely(key == nullptr || data == nullptr))
      return MDBX_EINVAL;
    else {
      MDBX_val save_data = *data;
      csr_t csr = cursor_seek(mc, key, data, MDBX_SET_KEY);
      rc = csr.err;
      if (rc == MDBX_SUCCESS) {
        cASSERT(mc, csr.exact);
        if (inner_pointed(mc)) {
          MDBX_cursor *const mx = &mc->subcur->cursor;
          csr = cursor_seek(mx, &save_data, nullptr, MDBX_SET_RANGE);
          rc = csr.err;
          if (csr.exact) {
            cASSERT(mc, csr.err == MDBX_SUCCESS);
            if (op == MDBX_TO_EXACT_KEY_VALUE_LESSER_THAN)
              rc = inner_prev(mx, data);
            else if (op == MDBX_TO_EXACT_KEY_VALUE_GREATER_THAN)
              rc = inner_next(mx, data);
          } else if (op < MDBX_TO_EXACT_KEY_VALUE_EQUAL && (rc == MDBX_NOTFOUND || rc == MDBX_SUCCESS))
            rc = inner_prev(mx, data);
          else if (op == MDBX_TO_EXACT_KEY_VALUE_EQUAL && rc == MDBX_SUCCESS)
            rc = MDBX_NOTFOUND;
        } else {
          int cmp = mc->clc->v.cmp(data, &save_data);
          switch (op) {
          default:
            __unreachable();
          case MDBX_TO_EXACT_KEY_VALUE_LESSER_THAN:
            rc = (cmp < 0) ? MDBX_SUCCESS : MDBX_NOTFOUND;
            break;
          case MDBX_TO_EXACT_KEY_VALUE_LESSER_OR_EQUAL:
            rc = (cmp <= 0) ? MDBX_SUCCESS : MDBX_NOTFOUND;
            break;
          case MDBX_TO_EXACT_KEY_VALUE_EQUAL:
            rc = (cmp == 0) ? MDBX_SUCCESS : MDBX_NOTFOUND;
            break;
          case MDBX_TO_EXACT_KEY_VALUE_GREATER_OR_EQUAL:
            rc = (cmp >= 0) ? MDBX_SUCCESS : MDBX_NOTFOUND;
            break;
          case MDBX_TO_EXACT_KEY_VALUE_GREATER_THAN:
            rc = (cmp > 0) ? MDBX_SUCCESS : MDBX_NOTFOUND;
            break;
          }
        }
      }
    }
    return rc;

  case MDBX_TO_PAIR_LESSER_THAN:
  case MDBX_TO_PAIR_LESSER_OR_EQUAL:
  case MDBX_TO_PAIR_EQUAL:
  case MDBX_TO_PAIR_GREATER_OR_EQUAL:
  case MDBX_TO_PAIR_GREATER_THAN:
    if (unlikely(key == nullptr || data == nullptr))
      return MDBX_EINVAL;
    else {
      MDBX_val save_data = *data;
      csr_t csr = cursor_seek(mc, key, data, MDBX_SET_RANGE);
      rc = csr.err;
      if (csr.exact) {
        cASSERT(mc, csr.err == MDBX_SUCCESS);
        if (inner_pointed(mc)) {
          MDBX_cursor *const mx = &mc->subcur->cursor;
          csr = cursor_seek(mx, &save_data, nullptr, MDBX_SET_RANGE);
          rc = csr.err;
          if (csr.exact) {
            cASSERT(mc, csr.err == MDBX_SUCCESS);
            if (op == MDBX_TO_PAIR_LESSER_THAN)
              rc = outer_prev(mc, key, data, MDBX_PREV);
            else if (op == MDBX_TO_PAIR_GREATER_THAN)
              rc = outer_next(mc, key, data, MDBX_NEXT);
          } else if (op < MDBX_TO_PAIR_EQUAL && (rc == MDBX_NOTFOUND || rc == MDBX_SUCCESS))
            rc = outer_prev(mc, key, data, MDBX_PREV);
          else if (op == MDBX_TO_PAIR_EQUAL && rc == MDBX_SUCCESS)
            rc = MDBX_NOTFOUND;
          else if (op > MDBX_TO_PAIR_EQUAL && rc == MDBX_NOTFOUND)
            rc = outer_next(mc, key, data, MDBX_NEXT);
        } else {
          int cmp = mc->clc->v.cmp(data, &save_data);
          switch (op) {
          default:
            __unreachable();
          case MDBX_TO_PAIR_LESSER_THAN:
            if (cmp >= 0)
              rc = outer_prev(mc, key, data, MDBX_PREV);
            break;
          case MDBX_TO_PAIR_LESSER_OR_EQUAL:
            if (cmp > 0)
              rc = outer_prev(mc, key, data, MDBX_PREV);
            break;
          case MDBX_TO_PAIR_EQUAL:
            rc = (cmp == 0) ? MDBX_SUCCESS : MDBX_NOTFOUND;
            break;
          case MDBX_TO_PAIR_GREATER_OR_EQUAL:
            if (cmp < 0)
              rc = outer_next(mc, key, data, MDBX_NEXT);
            break;
          case MDBX_TO_PAIR_GREATER_THAN:
            if (cmp <= 0)
              rc = outer_next(mc, key, data, MDBX_NEXT);
            break;
          }
        }
      } else if (op < MDBX_TO_PAIR_EQUAL && (rc == MDBX_NOTFOUND || rc == MDBX_SUCCESS))
        rc = outer_prev(mc, key, data, MDBX_PREV_NODUP);
      else if (op == MDBX_TO_PAIR_EQUAL && rc == MDBX_SUCCESS)
        rc = MDBX_NOTFOUND;
    }
    return rc;

  default:
    DEBUG("unhandled/unimplemented cursor operation %u", op);
    return MDBX_EINVAL;
  }
}

int cursor_check(const MDBX_cursor *mc, int txn_bad_bits) {
  if (unlikely(mc == nullptr))
    return MDBX_EINVAL;

  if (unlikely(mc->signature != cur_signature_live)) {
    if (mc->signature != cur_signature_ready4dispose)
      return MDBX_EBADSIGN;
    return (txn_bad_bits > MDBX_TXN_FINISHED) ? MDBX_EINVAL : MDBX_SUCCESS;
  }

  /* проверяем что курсор в связном списке для отслеживания, исключение допускается только для read-only операций для
   * служебных/временных курсоров на стеке. */
  MDBX_MAYBE_UNUSED char stack_top[sizeof(void *)];
  cASSERT(mc, cursor_is_tracked(mc) || (!(txn_bad_bits & MDBX_TXN_RDONLY) && stack_top < (char *)mc &&
                                        (char *)mc - stack_top < (ptrdiff_t)globals.sys_pagesize * 4));

  if (txn_bad_bits) {
    int rc = check_txn(mc->txn, txn_bad_bits & ~MDBX_TXN_HAS_CHILD);
    if (unlikely(rc != MDBX_SUCCESS)) {
      cASSERT(mc, rc != MDBX_RESULT_TRUE);
      return rc;
    }

    if (likely((mc->txn->flags & MDBX_TXN_HAS_CHILD) == 0))
      return likely(!cursor_dbi_changed(mc)) ? MDBX_SUCCESS : MDBX_BAD_DBI;

    cASSERT(mc, (mc->txn->flags & MDBX_TXN_RDONLY) == 0 && mc->txn != mc->txn->env->txn && mc->txn->env->txn);
    rc = dbi_check(mc->txn->env->txn, cursor_dbi(mc));
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;

    cASSERT(mc, (mc->txn->flags & MDBX_TXN_RDONLY) == 0 && mc->txn == mc->txn->env->txn);
  }

  return MDBX_SUCCESS;
}
