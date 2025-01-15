/// \copyright SPDX-License-Identifier: Apache-2.0
/// \note Please refer to the COPYRIGHT file for explanations license change,
/// credits and acknowledgments.
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025

#include "internals.h"

static MDBX_cursor *cursor_clone(const MDBX_cursor *csrc, cursor_couple_t *couple) {
  cASSERT(csrc, csrc->txn->txnid >= csrc->txn->env->lck->cached_oldest.weak);
  couple->outer.next = nullptr;
  couple->outer.backup = nullptr;
  couple->outer.subcur = nullptr;
  couple->outer.clc = nullptr;
  couple->outer.txn = csrc->txn;
  couple->outer.dbi_state = csrc->dbi_state;
  couple->outer.checking = z_pagecheck;
  couple->outer.tree = nullptr;
  couple->outer.top_and_flags = 0;

  MDBX_cursor *cdst = &couple->outer;
  if (is_inner(csrc)) {
    couple->inner.cursor.next = nullptr;
    couple->inner.cursor.backup = nullptr;
    couple->inner.cursor.subcur = nullptr;
    couple->inner.cursor.txn = csrc->txn;
    couple->inner.cursor.dbi_state = csrc->dbi_state;
    couple->outer.subcur = &couple->inner;
    cdst = &couple->inner.cursor;
  }

  cdst->checking = csrc->checking;
  cdst->tree = csrc->tree;
  cdst->clc = csrc->clc;
  cursor_cpstk(csrc, cdst);
  return cdst;
}

/*----------------------------------------------------------------------------*/

void recalculate_merge_thresholds(MDBX_env *env) {
  const size_t bytes = page_space(env);
  env->merge_threshold = (uint16_t)(bytes - (bytes * env->options.merge_threshold_16dot16_percent >> 16));
  env->merge_threshold_gc =
      (uint16_t)(bytes - ((env->options.merge_threshold_16dot16_percent > 19005) ? bytes / 3 /* 33 % */
                                                                                 : bytes / 4 /* 25 % */));
}

int tree_drop(MDBX_cursor *mc, const bool may_have_tables) {
  MDBX_txn *txn = mc->txn;
  int rc = tree_search(mc, nullptr, Z_FIRST);
  if (likely(rc == MDBX_SUCCESS)) {
    /* DUPSORT sub-DBs have no large-pages/tables. Omit scanning leaves.
     * This also avoids any P_DUPFIX pages, which have no nodes.
     * Also if the DB doesn't have sub-DBs and has no large/overflow
     * pages, omit scanning leaves. */
    if (!(may_have_tables | mc->tree->large_pages))
      cursor_pop(mc);

    rc = pnl_need(&txn->wr.retired_pages,
                  (size_t)mc->tree->branch_pages + (size_t)mc->tree->leaf_pages + (size_t)mc->tree->large_pages);
    if (unlikely(rc != MDBX_SUCCESS))
      goto bailout;

    page_t *stack[CURSOR_STACK_SIZE];
    for (intptr_t i = 0; i <= mc->top; ++i)
      stack[i] = mc->pg[i];

    while (mc->top >= 0) {
      page_t *const mp = mc->pg[mc->top];
      const size_t nkeys = page_numkeys(mp);
      if (is_leaf(mp)) {
        cASSERT(mc, mc->top + 1 == mc->tree->height);
        for (size_t i = 0; i < nkeys; i++) {
          node_t *node = page_node(mp, i);
          if (node_flags(node) & N_BIG) {
            rc = page_retire_ex(mc, node_largedata_pgno(node), nullptr, 0);
            if (unlikely(rc != MDBX_SUCCESS))
              goto bailout;
            if (!(may_have_tables | mc->tree->large_pages))
              goto pop;
          } else if (node_flags(node) & N_TREE) {
            if (unlikely((node_flags(node) & N_DUP) == 0)) {
              rc = /* disallowing implicit table deletion */ MDBX_INCOMPATIBLE;
              goto bailout;
            }
            rc = cursor_dupsort_setup(mc, node, mp);
            if (unlikely(rc != MDBX_SUCCESS))
              goto bailout;
            rc = tree_drop(&mc->subcur->cursor, false);
            if (unlikely(rc != MDBX_SUCCESS))
              goto bailout;
          }
        }
      } else {
        cASSERT(mc, mc->top + 1 < mc->tree->height);
        mc->checking |= z_retiring;
        const unsigned pagetype = (is_frozen(txn, mp) ? P_FROZEN : 0) +
                                  ((mc->top + 2 == mc->tree->height) ? (mc->checking & (P_LEAF | P_DUPFIX)) : P_BRANCH);
        for (size_t i = 0; i < nkeys; i++) {
          node_t *node = page_node(mp, i);
          tASSERT(txn, (node_flags(node) & (N_BIG | N_TREE | N_DUP)) == 0);
          const pgno_t pgno = node_pgno(node);
          rc = page_retire_ex(mc, pgno, nullptr, pagetype);
          if (unlikely(rc != MDBX_SUCCESS))
            goto bailout;
        }
        mc->checking -= z_retiring;
      }
      if (!mc->top)
        break;
      cASSERT(mc, nkeys > 0);
      mc->ki[mc->top] = (indx_t)nkeys;
      rc = cursor_sibling_right(mc);
      if (unlikely(rc != MDBX_SUCCESS)) {
        if (unlikely(rc != MDBX_NOTFOUND))
          goto bailout;
      /* no more siblings, go back to beginning
       * of previous level. */
      pop:
        cursor_pop(mc);
        mc->ki[0] = 0;
        for (intptr_t i = 1; i <= mc->top; i++) {
          mc->pg[i] = stack[i];
          mc->ki[i] = 0;
        }
      }
    }
    rc = page_retire(mc, mc->pg[0]);
  }

bailout:
  be_poor(mc);
  if (unlikely(rc != MDBX_SUCCESS))
    txn->flags |= MDBX_TXN_ERROR;
  return rc;
}

static int node_move(MDBX_cursor *csrc, MDBX_cursor *cdst, bool fromleft) {
  int rc;
  DKBUF_DEBUG;

  page_t *psrc = csrc->pg[csrc->top];
  page_t *pdst = cdst->pg[cdst->top];
  cASSERT(csrc, page_type(psrc) == page_type(pdst));
  cASSERT(csrc, csrc->tree == cdst->tree);
  cASSERT(csrc, csrc->top == cdst->top);
  if (unlikely(page_type(psrc) != page_type(pdst))) {
  bailout:
    ERROR("Wrong or mismatch pages's types (src %d, dst %d) to move node", page_type(psrc), page_type(pdst));
    csrc->txn->flags |= MDBX_TXN_ERROR;
    return MDBX_PROBLEM;
  }

  MDBX_val key4move;
  switch (page_type(psrc)) {
  case P_BRANCH: {
    const node_t *srcnode = page_node(psrc, csrc->ki[csrc->top]);
    cASSERT(csrc, node_flags(srcnode) == 0);
    const pgno_t srcpg = node_pgno(srcnode);
    key4move.iov_len = node_ks(srcnode);
    key4move.iov_base = node_key(srcnode);

    if (csrc->ki[csrc->top] == 0) {
      const int8_t top = csrc->top;
      cASSERT(csrc, top >= 0);
      /* must find the lowest key below src */
      rc = tree_search_lowest(csrc);
      page_t *lowest_page = csrc->pg[csrc->top];
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
      cASSERT(csrc, is_leaf(lowest_page));
      if (unlikely(!is_leaf(lowest_page)))
        goto bailout;
      if (is_dupfix_leaf(lowest_page))
        key4move = page_dupfix_key(lowest_page, 0, csrc->tree->dupfix_size);
      else {
        const node_t *lowest_node = page_node(lowest_page, 0);
        key4move.iov_len = node_ks(lowest_node);
        key4move.iov_base = node_key(lowest_node);
      }

      /* restore cursor after mdbx_page_search_lowest() */
      csrc->top = top;
      csrc->ki[csrc->top] = 0;

      /* paranoia */
      cASSERT(csrc, psrc == csrc->pg[csrc->top]);
      cASSERT(csrc, is_branch(psrc));
      if (unlikely(!is_branch(psrc)))
        goto bailout;
    }

    if (cdst->ki[cdst->top] == 0) {
      cursor_couple_t couple;
      MDBX_cursor *const mn = cursor_clone(cdst, &couple);
      const int8_t top = cdst->top;
      cASSERT(csrc, top >= 0);

      /* must find the lowest key below dst */
      rc = tree_search_lowest(mn);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
      page_t *const lowest_page = mn->pg[mn->top];
      cASSERT(cdst, is_leaf(lowest_page));
      if (unlikely(!is_leaf(lowest_page)))
        goto bailout;
      MDBX_val key;
      if (is_dupfix_leaf(lowest_page))
        key = page_dupfix_key(lowest_page, 0, mn->tree->dupfix_size);
      else {
        node_t *lowest_node = page_node(lowest_page, 0);
        key.iov_len = node_ks(lowest_node);
        key.iov_base = node_key(lowest_node);
      }

      /* restore cursor after mdbx_page_search_lowest() */
      mn->top = top;
      mn->ki[mn->top] = 0;

      const intptr_t delta = EVEN_CEIL(key.iov_len) - EVEN_CEIL(node_ks(page_node(mn->pg[mn->top], 0)));
      const intptr_t needed = branch_size(cdst->txn->env, &key4move) + delta;
      const intptr_t have = page_room(pdst);
      if (unlikely(needed > have))
        return MDBX_RESULT_TRUE;

      if (unlikely((rc = page_touch(csrc)) || (rc = page_touch(cdst))))
        return rc;
      psrc = csrc->pg[csrc->top];
      pdst = cdst->pg[cdst->top];

      couple.outer.next = mn->txn->cursors[cursor_dbi(mn)];
      mn->txn->cursors[cursor_dbi(mn)] = &couple.outer;
      rc = tree_propagate_key(mn, &key);
      mn->txn->cursors[cursor_dbi(mn)] = couple.outer.next;
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
    } else {
      const size_t needed = branch_size(cdst->txn->env, &key4move);
      const size_t have = page_room(pdst);
      if (unlikely(needed > have))
        return MDBX_RESULT_TRUE;

      if (unlikely((rc = page_touch(csrc)) || (rc = page_touch(cdst))))
        return rc;
      psrc = csrc->pg[csrc->top];
      pdst = cdst->pg[cdst->top];
    }

    DEBUG("moving %s-node %u [%s] on page %" PRIaPGNO " to node %u on page %" PRIaPGNO, "branch", csrc->ki[csrc->top],
          DKEY_DEBUG(&key4move), psrc->pgno, cdst->ki[cdst->top], pdst->pgno);
    /* Add the node to the destination page. */
    rc = node_add_branch(cdst, cdst->ki[cdst->top], &key4move, srcpg);
  } break;

  case P_LEAF: {
    /* Mark src and dst as dirty. */
    if (unlikely((rc = page_touch(csrc)) || (rc = page_touch(cdst))))
      return rc;
    psrc = csrc->pg[csrc->top];
    pdst = cdst->pg[cdst->top];
    const node_t *srcnode = page_node(psrc, csrc->ki[csrc->top]);
    MDBX_val data;
    data.iov_len = node_ds(srcnode);
    data.iov_base = node_data(srcnode);
    key4move.iov_len = node_ks(srcnode);
    key4move.iov_base = node_key(srcnode);
    DEBUG("moving %s-node %u [%s] on page %" PRIaPGNO " to node %u on page %" PRIaPGNO, "leaf", csrc->ki[csrc->top],
          DKEY_DEBUG(&key4move), psrc->pgno, cdst->ki[cdst->top], pdst->pgno);
    /* Add the node to the destination page. */
    rc = node_add_leaf(cdst, cdst->ki[cdst->top], &key4move, &data, node_flags(srcnode));
  } break;

  case P_LEAF | P_DUPFIX: {
    /* Mark src and dst as dirty. */
    if (unlikely((rc = page_touch(csrc)) || (rc = page_touch(cdst))))
      return rc;
    psrc = csrc->pg[csrc->top];
    pdst = cdst->pg[cdst->top];
    key4move = page_dupfix_key(psrc, csrc->ki[csrc->top], csrc->tree->dupfix_size);
    DEBUG("moving %s-node %u [%s] on page %" PRIaPGNO " to node %u on page %" PRIaPGNO, "leaf2", csrc->ki[csrc->top],
          DKEY_DEBUG(&key4move), psrc->pgno, cdst->ki[cdst->top], pdst->pgno);
    /* Add the node to the destination page. */
    rc = node_add_dupfix(cdst, cdst->ki[cdst->top], &key4move);
  } break;

  default:
    assert(false);
    goto bailout;
  }

  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  /* Delete the node from the source page. */
  node_del(csrc, key4move.iov_len);

  cASSERT(csrc, psrc == csrc->pg[csrc->top]);
  cASSERT(cdst, pdst == cdst->pg[cdst->top]);
  cASSERT(csrc, page_type(psrc) == page_type(pdst));

  /* csrc курсор тут всегда временный, на стеке внутри tree_rebalance(),
   * и его нет необходимости корректировать. */
  {
    /* Adjust other cursors pointing to mp */
    MDBX_cursor *m2, *m3;
    const size_t dbi = cursor_dbi(csrc);
    cASSERT(csrc, csrc->top == cdst->top);
    if (fromleft) {
      /* Перемещаем с левой страницы нв правую, нужно сдвинуть ki на +1 */
      for (m2 = csrc->txn->cursors[dbi]; m2; m2 = m2->next) {
        m3 = (csrc->flags & z_inner) ? &m2->subcur->cursor : m2;
        if (!is_related(csrc, m3))
          continue;

        if (m3 != cdst && m3->pg[csrc->top] == pdst && m3->ki[csrc->top] >= cdst->ki[csrc->top]) {
          m3->ki[csrc->top] += 1;
        }

        if (/* m3 != csrc && */ m3->pg[csrc->top] == psrc && m3->ki[csrc->top] == csrc->ki[csrc->top]) {
          m3->pg[csrc->top] = pdst;
          m3->ki[csrc->top] = cdst->ki[cdst->top];
          cASSERT(csrc, csrc->top > 0);
          m3->ki[csrc->top - 1] += 1;
        }

        if (is_leaf(psrc) && inner_pointed(m3)) {
          cASSERT(csrc, csrc->top == m3->top);
          size_t nkeys = page_numkeys(m3->pg[csrc->top]);
          if (likely(nkeys > m3->ki[csrc->top]))
            cursor_inner_refresh(m3, m3->pg[csrc->top], m3->ki[csrc->top]);
        }
      }
    } else {
      /* Перемещаем с правой страницы на левую, нужно сдвинуть ki на -1 */
      for (m2 = csrc->txn->cursors[dbi]; m2; m2 = m2->next) {
        m3 = (csrc->flags & z_inner) ? &m2->subcur->cursor : m2;
        if (!is_related(csrc, m3))
          continue;
        if (m3->pg[csrc->top] == psrc) {
          if (!m3->ki[csrc->top]) {
            m3->pg[csrc->top] = pdst;
            m3->ki[csrc->top] = cdst->ki[cdst->top];
            cASSERT(csrc, csrc->top > 0 && m3->ki[csrc->top - 1] > 0);
            m3->ki[csrc->top - 1] -= 1;
          } else
            m3->ki[csrc->top] -= 1;

          if (is_leaf(psrc) && inner_pointed(m3)) {
            cASSERT(csrc, csrc->top == m3->top);
            size_t nkeys = page_numkeys(m3->pg[csrc->top]);
            if (likely(nkeys > m3->ki[csrc->top]))
              cursor_inner_refresh(m3, m3->pg[csrc->top], m3->ki[csrc->top]);
          }
        }
      }
    }
  }

  /* Update the parent separators. */
  if (csrc->ki[csrc->top] == 0) {
    cASSERT(csrc, csrc->top > 0);
    if (csrc->ki[csrc->top - 1] != 0) {
      MDBX_val key;
      if (is_dupfix_leaf(psrc))
        key = page_dupfix_key(psrc, 0, csrc->tree->dupfix_size);
      else {
        node_t *srcnode = page_node(psrc, 0);
        key.iov_len = node_ks(srcnode);
        key.iov_base = node_key(srcnode);
      }
      DEBUG("update separator for source page %" PRIaPGNO " to [%s]", psrc->pgno, DKEY_DEBUG(&key));

      cursor_couple_t couple;
      MDBX_cursor *const mn = cursor_clone(csrc, &couple);
      cASSERT(csrc, mn->top > 0);
      mn->top -= 1;

      couple.outer.next = mn->txn->cursors[cursor_dbi(mn)];
      mn->txn->cursors[cursor_dbi(mn)] = &couple.outer;
      rc = tree_propagate_key(mn, &key);
      mn->txn->cursors[cursor_dbi(mn)] = couple.outer.next;
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
    }
    if (is_branch(psrc)) {
      const MDBX_val nullkey = {0, 0};
      const indx_t ix = csrc->ki[csrc->top];
      csrc->ki[csrc->top] = 0;
      rc = tree_propagate_key(csrc, &nullkey);
      csrc->ki[csrc->top] = ix;
      cASSERT(csrc, rc == MDBX_SUCCESS);
    }
  }

  if (cdst->ki[cdst->top] == 0) {
    cASSERT(cdst, cdst->top > 0);
    if (cdst->ki[cdst->top - 1] != 0) {
      MDBX_val key;
      if (is_dupfix_leaf(pdst))
        key = page_dupfix_key(pdst, 0, cdst->tree->dupfix_size);
      else {
        node_t *srcnode = page_node(pdst, 0);
        key.iov_len = node_ks(srcnode);
        key.iov_base = node_key(srcnode);
      }
      DEBUG("update separator for destination page %" PRIaPGNO " to [%s]", pdst->pgno, DKEY_DEBUG(&key));
      cursor_couple_t couple;
      MDBX_cursor *const mn = cursor_clone(cdst, &couple);
      cASSERT(cdst, mn->top > 0);
      mn->top -= 1;

      couple.outer.next = mn->txn->cursors[cursor_dbi(mn)];
      mn->txn->cursors[cursor_dbi(mn)] = &couple.outer;
      rc = tree_propagate_key(mn, &key);
      mn->txn->cursors[cursor_dbi(mn)] = couple.outer.next;
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
    }
    if (is_branch(pdst)) {
      const MDBX_val nullkey = {0, 0};
      const indx_t ix = cdst->ki[cdst->top];
      cdst->ki[cdst->top] = 0;
      rc = tree_propagate_key(cdst, &nullkey);
      cdst->ki[cdst->top] = ix;
      cASSERT(cdst, rc == MDBX_SUCCESS);
    }
  }

  return MDBX_SUCCESS;
}

static int page_merge(MDBX_cursor *csrc, MDBX_cursor *cdst) {
  MDBX_val key;
  int rc;

  cASSERT(csrc, csrc != cdst);
  cASSERT(csrc, cursor_is_tracked(csrc));
  cASSERT(cdst, cursor_is_tracked(cdst));
  const page_t *const psrc = csrc->pg[csrc->top];
  page_t *pdst = cdst->pg[cdst->top];
  DEBUG("merging page %" PRIaPGNO " into %" PRIaPGNO, psrc->pgno, pdst->pgno);

  cASSERT(csrc, page_type(psrc) == page_type(pdst));
  cASSERT(csrc, csrc->clc == cdst->clc && csrc->tree == cdst->tree);
  cASSERT(csrc, csrc->top > 0); /* can't merge root page */
  cASSERT(cdst, cdst->top > 0);
  cASSERT(cdst, cdst->top + 1 < cdst->tree->height || is_leaf(cdst->pg[cdst->tree->height - 1]));
  cASSERT(csrc, csrc->top + 1 < csrc->tree->height || is_leaf(csrc->pg[csrc->tree->height - 1]));
  cASSERT(cdst,
          csrc->txn->env->options.prefer_waf_insteadof_balance || page_room(pdst) >= page_used(cdst->txn->env, psrc));
  const int pagetype = page_type(psrc);

  /* Move all nodes from src to dst */
  const size_t dst_nkeys = page_numkeys(pdst);
  const size_t src_nkeys = page_numkeys(psrc);
  cASSERT(cdst, dst_nkeys + src_nkeys >= (is_leaf(psrc) ? 1u : 2u));
  if (likely(src_nkeys)) {
    size_t ii = dst_nkeys;
    if (unlikely(pagetype & P_DUPFIX)) {
      /* Mark dst as dirty. */
      rc = page_touch(cdst);
      cASSERT(cdst, rc != MDBX_RESULT_TRUE);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;

      key.iov_len = csrc->tree->dupfix_size;
      key.iov_base = page_data(psrc);
      size_t i = 0;
      do {
        rc = node_add_dupfix(cdst, ii++, &key);
        cASSERT(cdst, rc != MDBX_RESULT_TRUE);
        if (unlikely(rc != MDBX_SUCCESS))
          return rc;
        key.iov_base = ptr_disp(key.iov_base, key.iov_len);
      } while (++i != src_nkeys);
    } else {
      node_t *srcnode = page_node(psrc, 0);
      key.iov_len = node_ks(srcnode);
      key.iov_base = node_key(srcnode);
      if (pagetype & P_BRANCH) {
        cursor_couple_t couple;
        MDBX_cursor *const mn = cursor_clone(csrc, &couple);

        /* must find the lowest key below src */
        rc = tree_search_lowest(mn);
        cASSERT(csrc, rc != MDBX_RESULT_TRUE);
        if (unlikely(rc != MDBX_SUCCESS))
          return rc;

        const page_t *mp = mn->pg[mn->top];
        if (likely(!is_dupfix_leaf(mp))) {
          cASSERT(mn, is_leaf(mp));
          const node_t *lowest = page_node(mp, 0);
          key.iov_len = node_ks(lowest);
          key.iov_base = node_key(lowest);
        } else {
          cASSERT(mn, mn->top > csrc->top);
          key = page_dupfix_key(mp, mn->ki[mn->top], csrc->tree->dupfix_size);
        }
        cASSERT(mn, key.iov_len >= csrc->clc->k.lmin);
        cASSERT(mn, key.iov_len <= csrc->clc->k.lmax);

        const size_t dst_room = page_room(pdst);
        const size_t src_used = page_used(cdst->txn->env, psrc);
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
          rc = node_add_leaf(cdst, ii++, &key, &data, node_flags(srcnode));
        } else {
          cASSERT(csrc, node_flags(srcnode) == 0);
          rc = node_add_branch(cdst, ii++, &key, node_pgno(srcnode));
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

    pdst = cdst->pg[cdst->top];
    DEBUG("dst page %" PRIaPGNO " now has %zu keys (%u.%u%% filled)", pdst->pgno, page_numkeys(pdst),
          page_fill_percentum_x10(cdst->txn->env, pdst) / 10, page_fill_percentum_x10(cdst->txn->env, pdst) % 10);

    cASSERT(csrc, psrc == csrc->pg[csrc->top]);
    cASSERT(cdst, pdst == cdst->pg[cdst->top]);
  }

  /* Unlink the src page from parent and add to free list. */
  csrc->top -= 1;
  node_del(csrc, 0);
  if (csrc->ki[csrc->top] == 0) {
    const MDBX_val nullkey = {0, 0};
    rc = tree_propagate_key(csrc, &nullkey);
    cASSERT(csrc, rc != MDBX_RESULT_TRUE);
    if (unlikely(rc != MDBX_SUCCESS)) {
      csrc->top += 1;
      return rc;
    }
  }
  csrc->top += 1;

  cASSERT(csrc, psrc == csrc->pg[csrc->top]);
  cASSERT(cdst, pdst == cdst->pg[cdst->top]);

  {
    /* Adjust other cursors pointing to mp */
    MDBX_cursor *m2, *m3;
    const size_t dbi = cursor_dbi(csrc);
    for (m2 = csrc->txn->cursors[dbi]; m2; m2 = m2->next) {
      m3 = (csrc->flags & z_inner) ? &m2->subcur->cursor : m2;
      if (!is_related(csrc, m3))
        continue;
      if (m3->pg[csrc->top] == psrc) {
        m3->pg[csrc->top] = pdst;
        m3->ki[csrc->top] += (indx_t)dst_nkeys;
        m3->ki[csrc->top - 1] = cdst->ki[csrc->top - 1];
      } else if (m3->pg[csrc->top - 1] == csrc->pg[csrc->top - 1] && m3->ki[csrc->top - 1] > csrc->ki[csrc->top - 1]) {
        cASSERT(m3, m3->ki[csrc->top - 1] > 0 && m3->ki[csrc->top - 1] <= page_numkeys(m3->pg[csrc->top - 1]));
        m3->ki[csrc->top - 1] -= 1;
      }

      if (is_leaf(psrc) && inner_pointed(m3)) {
        cASSERT(csrc, csrc->top == m3->top);
        size_t nkeys = page_numkeys(m3->pg[csrc->top]);
        if (likely(nkeys > m3->ki[csrc->top]))
          cursor_inner_refresh(m3, m3->pg[csrc->top], m3->ki[csrc->top]);
      }
    }
  }

  rc = page_retire(csrc, (page_t *)psrc);
  cASSERT(csrc, rc != MDBX_RESULT_TRUE);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  cASSERT(cdst, cdst->tree->items > 0);
  cASSERT(cdst, cdst->top + 1 <= cdst->tree->height);
  cASSERT(cdst, cdst->top > 0);
  page_t *const top_page = cdst->pg[cdst->top];
  const indx_t top_indx = cdst->ki[cdst->top];
  const int save_top = cdst->top;
  const uint16_t save_height = cdst->tree->height;
  cursor_pop(cdst);
  rc = tree_rebalance(cdst);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  cASSERT(cdst, cdst->tree->items > 0);
  cASSERT(cdst, cdst->top + 1 <= cdst->tree->height);

#if MDBX_ENABLE_PGOP_STAT
  cdst->txn->env->lck->pgops.merge.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */

  if (is_leaf(cdst->pg[cdst->top])) {
    /* LY: don't touch cursor if top-page is a LEAF */
    cASSERT(cdst, is_leaf(cdst->pg[cdst->top]) || page_type(cdst->pg[cdst->top]) == pagetype);
    return MDBX_SUCCESS;
  }

  cASSERT(cdst, page_numkeys(top_page) == dst_nkeys + src_nkeys);

  if (unlikely(pagetype != page_type(top_page))) {
    /* LY: LEAF-page becomes BRANCH, unable restore cursor's stack */
    goto bailout;
  }

  if (top_page == cdst->pg[cdst->top]) {
    /* LY: don't touch cursor if prev top-page already on the top */
    cASSERT(cdst, cdst->ki[cdst->top] == top_indx);
    cASSERT(cdst, is_leaf(cdst->pg[cdst->top]) || page_type(cdst->pg[cdst->top]) == pagetype);
    return MDBX_SUCCESS;
  }

  const int new_top = save_top - save_height + cdst->tree->height;
  if (unlikely(new_top < 0 || new_top >= cdst->tree->height)) {
    /* LY: out of range, unable restore cursor's stack */
    goto bailout;
  }

  if (top_page == cdst->pg[new_top]) {
    cASSERT(cdst, cdst->ki[new_top] == top_indx);
    /* LY: restore cursor stack */
    cdst->top = (int8_t)new_top;
    cASSERT(cdst, cdst->top + 1 < cdst->tree->height || is_leaf(cdst->pg[cdst->tree->height - 1]));
    cASSERT(cdst, is_leaf(cdst->pg[cdst->top]) || page_type(cdst->pg[cdst->top]) == pagetype);
    return MDBX_SUCCESS;
  }

  page_t *const stub_page = (page_t *)(~(uintptr_t)top_page);
  const indx_t stub_indx = top_indx;
  if (save_height > cdst->tree->height && ((cdst->pg[save_top] == top_page && cdst->ki[save_top] == top_indx) ||
                                           (cdst->pg[save_top] == stub_page && cdst->ki[save_top] == stub_indx))) {
    /* LY: restore cursor stack */
    cdst->pg[new_top] = top_page;
    cdst->ki[new_top] = top_indx;
#if MDBX_DEBUG
    cdst->pg[new_top + 1] = nullptr;
    cdst->ki[new_top + 1] = INT16_MAX;
#endif
    cdst->top = (int8_t)new_top;
    cASSERT(cdst, cdst->top + 1 < cdst->tree->height || is_leaf(cdst->pg[cdst->tree->height - 1]));
    cASSERT(cdst, is_leaf(cdst->pg[cdst->top]) || page_type(cdst->pg[cdst->top]) == pagetype);
    return MDBX_SUCCESS;
  }

bailout:
  /* LY: unable restore cursor's stack */
  be_poor(cdst);
  return MDBX_CURSOR_FULL;
}

int tree_rebalance(MDBX_cursor *mc) {
  cASSERT(mc, cursor_is_tracked(mc));
  cASSERT(mc, mc->top >= 0);
  cASSERT(mc, mc->top + 1 < mc->tree->height || is_leaf(mc->pg[mc->tree->height - 1]));
  const page_t *const tp = mc->pg[mc->top];
  const uint8_t pagetype = page_type(tp);

  STATIC_ASSERT(P_BRANCH == 1);
  const size_t minkeys = (pagetype & P_BRANCH) + (size_t)1;

  /* Pages emptier than this are candidates for merging. */
  size_t room_threshold =
      likely(mc->tree != &mc->txn->dbs[FREE_DBI]) ? mc->txn->env->merge_threshold : mc->txn->env->merge_threshold_gc;

  const size_t numkeys = page_numkeys(tp);
  const size_t room = page_room(tp);
  DEBUG("rebalancing %s page %" PRIaPGNO " (has %zu keys, fill %u.%u%%, used %zu, room %zu bytes)",
        is_leaf(tp) ? "leaf" : "branch", tp->pgno, numkeys, page_fill_percentum_x10(mc->txn->env, tp) / 10,
        page_fill_percentum_x10(mc->txn->env, tp) % 10, page_used(mc->txn->env, tp), room);
  cASSERT(mc, is_modifable(mc->txn, tp));

  if (unlikely(numkeys < minkeys)) {
    DEBUG("page %" PRIaPGNO " must be merged due keys < %zu threshold", tp->pgno, minkeys);
  } else if (unlikely(room > room_threshold)) {
    DEBUG("page %" PRIaPGNO " should be merged due room %zu > %zu threshold", tp->pgno, room, room_threshold);
  } else {
    DEBUG("no need to rebalance page %" PRIaPGNO ", room %zu < %zu threshold", tp->pgno, room, room_threshold);
    cASSERT(mc, mc->tree->items > 0);
    return MDBX_SUCCESS;
  }

  int rc;
  if (mc->top == 0) {
    page_t *const mp = mc->pg[0];
    const size_t nkeys = page_numkeys(mp);
    cASSERT(mc, (mc->tree->items == 0) == (nkeys == 0));
    if (nkeys == 0) {
      DEBUG("%s", "tree is completely empty");
      cASSERT(mc, is_leaf(mp));
      cASSERT(mc, (*cursor_dbi_state(mc) & DBI_DIRTY) != 0);
      cASSERT(mc, mc->tree->branch_pages == 0 && mc->tree->large_pages == 0 && mc->tree->leaf_pages == 1);
      /* Adjust cursors pointing to mp */
      for (MDBX_cursor *m2 = mc->txn->cursors[cursor_dbi(mc)]; m2; m2 = m2->next) {
        MDBX_cursor *m3 = (mc->flags & z_inner) ? &m2->subcur->cursor : m2;
        if (!is_poor(m3) && m3->pg[0] == mp) {
          be_poor(m3);
          m3->flags |= z_after_delete;
        }
      }
      if (is_subpage(mp)) {
        return MDBX_SUCCESS;
      } else {
        mc->tree->root = P_INVALID;
        mc->tree->height = 0;
        return page_retire(mc, mp);
      }
    }
    if (is_subpage(mp)) {
      DEBUG("%s", "Can't rebalance a subpage, ignoring");
      cASSERT(mc, is_leaf(tp));
      return MDBX_SUCCESS;
    }
    if (is_branch(mp) && nkeys == 1) {
      DEBUG("%s", "collapsing root page!");
      mc->tree->root = node_pgno(page_node(mp, 0));
      rc = page_get(mc, mc->tree->root, &mc->pg[0], mp->txnid);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
      mc->tree->height--;
      mc->ki[0] = mc->ki[1];
      for (intptr_t i = 1; i < mc->tree->height; i++) {
        mc->pg[i] = mc->pg[i + 1];
        mc->ki[i] = mc->ki[i + 1];
      }

      /* Adjust other cursors pointing to mp */
      for (MDBX_cursor *m2 = mc->txn->cursors[cursor_dbi(mc)]; m2; m2 = m2->next) {
        MDBX_cursor *m3 = (mc->flags & z_inner) ? &m2->subcur->cursor : m2;
        if (is_related(mc, m3) && m3->pg[0] == mp) {
          for (intptr_t i = 0; i < mc->tree->height; i++) {
            m3->pg[i] = m3->pg[i + 1];
            m3->ki[i] = m3->ki[i + 1];
          }
          m3->top -= 1;
        }
      }
      cASSERT(mc, is_leaf(mc->pg[mc->top]) || page_type(mc->pg[mc->top]) == pagetype);
      cASSERT(mc, mc->top + 1 < mc->tree->height || is_leaf(mc->pg[mc->tree->height - 1]));
      return page_retire(mc, mp);
    }
    DEBUG("root page %" PRIaPGNO " doesn't need rebalancing (flags 0x%x)", mp->pgno, mp->flags);
    return MDBX_SUCCESS;
  }

  /* The parent (branch page) must have at least 2 pointers,
   * otherwise the tree is invalid. */
  const size_t pre_top = mc->top - 1;
  cASSERT(mc, is_branch(mc->pg[pre_top]));
  cASSERT(mc, !is_subpage(mc->pg[0]));
  cASSERT(mc, page_numkeys(mc->pg[pre_top]) > 1);

  /* Leaf page fill factor is below the threshold.
   * Try to move keys from left or right neighbor, or
   * merge with a neighbor page. */

  /* Find neighbors. */
  cursor_couple_t couple;
  MDBX_cursor *const mn = cursor_clone(mc, &couple);

  page_t *left = nullptr, *right = nullptr;
  if (mn->ki[pre_top] > 0) {
    rc = page_get(mn, node_pgno(page_node(mn->pg[pre_top], mn->ki[pre_top] - 1)), &left, mc->pg[mc->top]->txnid);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    cASSERT(mc, page_type(left) == page_type(mc->pg[mc->top]));
  }
  if (mn->ki[pre_top] + (size_t)1 < page_numkeys(mn->pg[pre_top])) {
    rc = page_get(mn, node_pgno(page_node(mn->pg[pre_top], mn->ki[pre_top] + (size_t)1)), &right,
                  mc->pg[mc->top]->txnid);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    cASSERT(mc, page_type(right) == page_type(mc->pg[mc->top]));
  }
  cASSERT(mc, left || right);

  const size_t ki_top = mc->ki[mc->top];
  const size_t ki_pre_top = mn->ki[pre_top];
  const size_t nkeys = page_numkeys(mn->pg[mn->top]);

  const size_t left_room = left ? page_room(left) : 0;
  const size_t right_room = right ? page_room(right) : 0;
  const size_t left_nkeys = left ? page_numkeys(left) : 0;
  const size_t right_nkeys = right ? page_numkeys(right) : 0;
  bool involve = !(left && right);
retry:
  cASSERT(mc, mc->top > 0);
  if (left_room > room_threshold && left_room >= right_room && (is_modifable(mc->txn, left) || involve)) {
    /* try merge with left */
    cASSERT(mc, left_nkeys >= minkeys);
    mn->pg[mn->top] = left;
    mn->ki[mn->top - 1] = (indx_t)(ki_pre_top - 1);
    mn->ki[mn->top] = (indx_t)(left_nkeys - 1);
    mc->ki[mc->top] = 0;
    const size_t new_ki = ki_top + left_nkeys;
    mn->ki[mn->top] += mc->ki[mn->top] + 1;
    couple.outer.next = mn->txn->cursors[cursor_dbi(mn)];
    mn->txn->cursors[cursor_dbi(mn)] = &couple.outer;
    rc = page_merge(mc, mn);
    mn->txn->cursors[cursor_dbi(mn)] = couple.outer.next;
    if (likely(rc != MDBX_RESULT_TRUE)) {
      cursor_cpstk(mn, mc);
      mc->ki[mc->top] = (indx_t)new_ki;
      cASSERT(mc, rc || page_numkeys(mc->pg[mc->top]) >= minkeys);
      return rc;
    }
  }
  if (right_room > room_threshold && (is_modifable(mc->txn, right) || involve)) {
    /* try merge with right */
    cASSERT(mc, right_nkeys >= minkeys);
    mn->pg[mn->top] = right;
    mn->ki[mn->top - 1] = (indx_t)(ki_pre_top + 1);
    mn->ki[mn->top] = 0;
    mc->ki[mc->top] = (indx_t)nkeys;
    couple.outer.next = mn->txn->cursors[cursor_dbi(mn)];
    mn->txn->cursors[cursor_dbi(mn)] = &couple.outer;
    rc = page_merge(mn, mc);
    mn->txn->cursors[cursor_dbi(mn)] = couple.outer.next;
    if (likely(rc != MDBX_RESULT_TRUE)) {
      mc->ki[mc->top] = (indx_t)ki_top;
      cASSERT(mc, rc || page_numkeys(mc->pg[mc->top]) >= minkeys);
      return rc;
    }
  }

  if (left_nkeys > minkeys && (right_nkeys <= left_nkeys || right_room >= left_room) &&
      (is_modifable(mc->txn, left) || involve)) {
    /* try move from left */
    mn->pg[mn->top] = left;
    mn->ki[mn->top - 1] = (indx_t)(ki_pre_top - 1);
    mn->ki[mn->top] = (indx_t)(left_nkeys - 1);
    mc->ki[mc->top] = 0;
    couple.outer.next = mn->txn->cursors[cursor_dbi(mn)];
    mn->txn->cursors[cursor_dbi(mn)] = &couple.outer;
    rc = node_move(mn, mc, true);
    mn->txn->cursors[cursor_dbi(mn)] = couple.outer.next;
    if (likely(rc != MDBX_RESULT_TRUE)) {
      mc->ki[mc->top] = (indx_t)(ki_top + 1);
      cASSERT(mc, rc || page_numkeys(mc->pg[mc->top]) >= minkeys);
      return rc;
    }
  }
  if (right_nkeys > minkeys && (is_modifable(mc->txn, right) || involve)) {
    /* try move from right */
    mn->pg[mn->top] = right;
    mn->ki[mn->top - 1] = (indx_t)(ki_pre_top + 1);
    mn->ki[mn->top] = 0;
    mc->ki[mc->top] = (indx_t)nkeys;
    couple.outer.next = mn->txn->cursors[cursor_dbi(mn)];
    mn->txn->cursors[cursor_dbi(mn)] = &couple.outer;
    rc = node_move(mn, mc, false);
    mn->txn->cursors[cursor_dbi(mn)] = couple.outer.next;
    if (likely(rc != MDBX_RESULT_TRUE)) {
      mc->ki[mc->top] = (indx_t)ki_top;
      cASSERT(mc, rc || page_numkeys(mc->pg[mc->top]) >= minkeys);
      return rc;
    }
  }

  if (nkeys >= minkeys) {
    mc->ki[mc->top] = (indx_t)ki_top;
    if (AUDIT_ENABLED())
      return cursor_check_updating(mc);
    return MDBX_SUCCESS;
  }

  if (mc->txn->env->options.prefer_waf_insteadof_balance && likely(room_threshold > 0)) {
    room_threshold = 0;
    goto retry;
  }
  if (likely(!involve) &&
      (likely(mc->tree != &mc->txn->dbs[FREE_DBI]) || mc->txn->wr.loose_pages || MDBX_PNL_GETSIZE(mc->txn->wr.repnl) ||
       (mc->flags & z_gcu_preparation) || (mc->txn->flags & txn_gc_drained) || room_threshold)) {
    involve = true;
    goto retry;
  }
  if (likely(room_threshold > 0)) {
    room_threshold = 0;
    goto retry;
  }

  ERROR("Unable to merge/rebalance %s page %" PRIaPGNO " (has %zu keys, fill %u.%u%%, used %zu, room %zu bytes)",
        is_leaf(tp) ? "leaf" : "branch", tp->pgno, numkeys, page_fill_percentum_x10(mc->txn->env, tp) / 10,
        page_fill_percentum_x10(mc->txn->env, tp) % 10, page_used(mc->txn->env, tp), room);
  return MDBX_PROBLEM;
}

int page_split(MDBX_cursor *mc, const MDBX_val *const newkey, MDBX_val *const newdata, pgno_t newpgno,
               const unsigned naf) {
  unsigned flags;
  int rc = MDBX_SUCCESS, foliage = 0;
  MDBX_env *const env = mc->txn->env;
  MDBX_val rkey, xdata;
  page_t *tmp_ki_copy = nullptr;
  DKBUF;

  page_t *const mp = mc->pg[mc->top];
  cASSERT(mc, (mp->flags & P_ILL_BITS) == 0);

  const size_t newindx = mc->ki[mc->top];
  size_t nkeys = page_numkeys(mp);
  if (AUDIT_ENABLED()) {
    rc = cursor_check_updating(mc);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }
  STATIC_ASSERT(P_BRANCH == 1);
  const size_t minkeys = (mp->flags & P_BRANCH) + (size_t)1;

  DEBUG(">> splitting %s-page %" PRIaPGNO " and adding %zu+%zu [%s] at %i, nkeys %zi", is_leaf(mp) ? "leaf" : "branch",
        mp->pgno, newkey->iov_len, newdata ? newdata->iov_len : 0, DKEY_DEBUG(newkey), mc->ki[mc->top], nkeys);
  cASSERT(mc, nkeys + 1 >= minkeys * 2);

  /* Create a new sibling page. */
  pgr_t npr = page_new(mc, mp->flags);
  if (unlikely(npr.err != MDBX_SUCCESS))
    return npr.err;
  page_t *const sister = npr.page;
  sister->dupfix_ksize = mp->dupfix_ksize;
  DEBUG("new sibling: page %" PRIaPGNO, sister->pgno);

  /* Usually when splitting the root page, the cursor
   * height is 1. But when called from tree_propagate_key,
   * the cursor height may be greater because it walks
   * up the stack while finding the branch slot to update. */
  intptr_t prev_top = mc->top - 1;
  if (mc->top == 0) {
    npr = page_new(mc, P_BRANCH);
    rc = npr.err;
    if (unlikely(rc != MDBX_SUCCESS))
      goto done;
    page_t *const pp = npr.page;
    /* shift current top to make room for new parent */
    cASSERT(mc, mc->tree->height > 0);
#if MDBX_DEBUG
    memset(mc->pg + 3, 0, sizeof(mc->pg) - sizeof(mc->pg[0]) * 3);
    memset(mc->ki + 3, -1, sizeof(mc->ki) - sizeof(mc->ki[0]) * 3);
#endif
    mc->pg[2] = mc->pg[1];
    mc->ki[2] = mc->ki[1];
    mc->pg[1] = mc->pg[0];
    mc->ki[1] = mc->ki[0];
    mc->pg[0] = pp;
    mc->ki[0] = 0;
    mc->tree->root = pp->pgno;
    DEBUG("root split! new root = %" PRIaPGNO, pp->pgno);
    foliage = mc->tree->height++;

    /* Add left (implicit) pointer. */
    rc = node_add_branch(mc, 0, nullptr, mp->pgno);
    if (unlikely(rc != MDBX_SUCCESS)) {
      /* undo the pre-push */
      mc->pg[0] = mc->pg[1];
      mc->ki[0] = mc->ki[1];
      mc->tree->root = mp->pgno;
      mc->tree->height--;
      goto done;
    }
    mc->top = 1;
    prev_top = 0;
    if (AUDIT_ENABLED()) {
      rc = cursor_check_updating(mc);
      if (unlikely(rc != MDBX_SUCCESS))
        goto done;
    }
  } else {
    DEBUG("parent branch page is %" PRIaPGNO, mc->pg[prev_top]->pgno);
  }

  cursor_couple_t couple;
  MDBX_cursor *const mn = cursor_clone(mc, &couple);
  mn->pg[mn->top] = sister;
  mn->ki[mn->top] = 0;
  mn->ki[prev_top] = mc->ki[prev_top] + 1;

  size_t split_indx = (newindx < nkeys) ? /* split at the middle */ (nkeys + 1) >> 1
                                        : /* split at the end (i.e. like append-mode ) */ nkeys - minkeys + 1;
  eASSERT(env, split_indx >= minkeys && split_indx <= nkeys - minkeys + 1);

  cASSERT(mc, !is_branch(mp) || newindx > 0);
  MDBX_val sepkey = {nullptr, 0};
  /* It is reasonable and possible to split the page at the begin */
  if (unlikely(newindx < minkeys)) {
    split_indx = minkeys;
    if (newindx == 0 && !(naf & MDBX_SPLIT_REPLACE)) {
      split_indx = 0;
      /* Checking for ability of splitting by the left-side insertion
       * of a pure page with the new key */
      for (intptr_t i = 0; i < mc->top; ++i)
        if (mc->ki[i]) {
          sepkey = get_key(page_node(mc->pg[i], mc->ki[i]));
          if (mc->clc->k.cmp(newkey, &sepkey) >= 0)
            split_indx = minkeys;
          break;
        }
      if (split_indx == 0) {
        /* Save the current first key which was omitted on the parent branch
         * page and should be updated if the new first entry will be added */
        if (is_dupfix_leaf(mp))
          sepkey = page_dupfix_key(mp, 0, mc->tree->dupfix_size);
        else
          sepkey = get_key(page_node(mp, 0));
        cASSERT(mc, mc->clc->k.cmp(newkey, &sepkey) < 0);
        /* Avoiding rare complex cases of nested split the parent page(s) */
        if (page_room(mc->pg[prev_top]) < branch_size(env, &sepkey))
          split_indx = minkeys;
      }
      if (foliage) {
        TRACE("pure-left: foliage %u, top %i, ptop %zu, split_indx %zi, "
              "minkeys %zi, sepkey %s, parent-room %zu, need4split %zu",
              foliage, mc->top, prev_top, split_indx, minkeys, DKEY_DEBUG(&sepkey), page_room(mc->pg[prev_top]),
              branch_size(env, &sepkey));
        TRACE("pure-left: newkey %s, newdata %s, newindx %zu", DKEY_DEBUG(newkey), DVAL_DEBUG(newdata), newindx);
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
    TRACE("pure-left: no-split, but add new pure page at the %s", "left/before");
    cASSERT(mc, newindx == 0 && split_indx == 0 && minkeys == 1);
    TRACE("pure-left: old-first-key is %s", DKEY_DEBUG(&sepkey));
  } else {
    if (is_dupfix_leaf(sister)) {
      /* Move half of the keys to the right sibling */
      const intptr_t distance = mc->ki[mc->top] - split_indx;
      size_t ksize = mc->tree->dupfix_size;
      void *const split = page_dupfix_ptr(mp, split_indx, ksize);
      size_t rsize = (nkeys - split_indx) * ksize;
      size_t lsize = (nkeys - split_indx) * sizeof(indx_t);
      cASSERT(mc, mp->lower >= lsize);
      mp->lower -= (indx_t)lsize;
      cASSERT(mc, sister->lower + lsize <= UINT16_MAX);
      sister->lower += (indx_t)lsize;
      cASSERT(mc, mp->upper + rsize - lsize <= UINT16_MAX);
      mp->upper += (indx_t)(rsize - lsize);
      cASSERT(mc, sister->upper >= rsize - lsize);
      sister->upper -= (indx_t)(rsize - lsize);
      sepkey.iov_len = ksize;
      sepkey.iov_base = (newindx != split_indx) ? split : newkey->iov_base;
      if (distance < 0) {
        cASSERT(mc, ksize >= sizeof(indx_t));
        void *const ins = page_dupfix_ptr(mp, mc->ki[mc->top], ksize);
        memcpy(sister->entries, split, rsize);
        sepkey.iov_base = sister->entries;
        memmove(ptr_disp(ins, ksize), ins, (split_indx - mc->ki[mc->top]) * ksize);
        memcpy(ins, newkey->iov_base, ksize);
        cASSERT(mc, UINT16_MAX - mp->lower >= (int)sizeof(indx_t));
        mp->lower += sizeof(indx_t);
        cASSERT(mc, mp->upper >= ksize - sizeof(indx_t));
        mp->upper -= (indx_t)(ksize - sizeof(indx_t));
        cASSERT(mc, (((ksize & page_numkeys(mp)) ^ mp->upper) & 1) == 0);
      } else {
        memcpy(sister->entries, split, distance * ksize);
        void *const ins = page_dupfix_ptr(sister, distance, ksize);
        memcpy(ins, newkey->iov_base, ksize);
        memcpy(ptr_disp(ins, ksize), ptr_disp(split, distance * ksize), rsize - distance * ksize);
        cASSERT(mc, UINT16_MAX - sister->lower >= (int)sizeof(indx_t));
        sister->lower += sizeof(indx_t);
        cASSERT(mc, sister->upper >= ksize - sizeof(indx_t));
        sister->upper -= (indx_t)(ksize - sizeof(indx_t));
        cASSERT(mc, distance <= (int)UINT16_MAX);
        mc->ki[mc->top] = (indx_t)distance;
        cASSERT(mc, (((ksize & page_numkeys(sister)) ^ sister->upper) & 1) == 0);
      }

      if (AUDIT_ENABLED()) {
        rc = cursor_check_updating(mc);
        if (unlikely(rc != MDBX_SUCCESS))
          goto done;
        rc = cursor_check_updating(mn);
        if (unlikely(rc != MDBX_SUCCESS))
          goto done;
      }
    } else {
      /* grab a page to hold a temporary copy */
      tmp_ki_copy = page_shadow_alloc(mc->txn, 1);
      if (unlikely(tmp_ki_copy == nullptr)) {
        rc = MDBX_ENOMEM;
        goto done;
      }

      const size_t max_space = page_space(env);
      const size_t new_size = is_leaf(mp) ? leaf_size(env, newkey, newdata) : branch_size(env, newkey);

      /* prepare to insert */
      size_t i = 0;
      while (i < newindx) {
        tmp_ki_copy->entries[i] = mp->entries[i];
        ++i;
      }
      tmp_ki_copy->entries[i] = (indx_t)-1;
      while (++i <= nkeys)
        tmp_ki_copy->entries[i] = mp->entries[i - 1];
      tmp_ki_copy->pgno = mp->pgno;
      tmp_ki_copy->flags = mp->flags;
      tmp_ki_copy->txnid = INVALID_TXNID;
      tmp_ki_copy->lower = 0;
      tmp_ki_copy->upper = (indx_t)max_space;

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
        split_indx += mp->flags & P_BRANCH;
      }
      eASSERT(env, split_indx >= minkeys && split_indx <= nkeys + 1 - minkeys);
      const size_t dim_nodes = (newindx >= split_indx) ? split_indx : nkeys - split_indx;
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
            node_t *node = ptr_disp(mp, tmp_ki_copy->entries[i] + PAGEHDRSZ);
            size = NODESIZE + node_ks(node) + sizeof(indx_t);
            if (is_leaf(mp))
              size += (node_flags(node) & N_BIG) ? sizeof(pgno_t) : node_ds(node);
            size = EVEN_CEIL(size);
          }

          before += size;
          after -= size;
          TRACE("step %zu, size %zu, before %zu, after %zu, max %zu", i, size, before, after, max_space);

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
        node_t *node = ptr_disp(mp, tmp_ki_copy->entries[split_indx] + PAGEHDRSZ);
        sepkey.iov_len = node_ks(node);
        sepkey.iov_base = node_key(node);
      }
    }
  }
  DEBUG("separator is %zd [%s]", split_indx, DKEY_DEBUG(&sepkey));

  bool did_split_parent = false;
  /* Copy separator key to the parent. */
  if (page_room(mn->pg[prev_top]) < branch_size(env, &sepkey)) {
    TRACE("need split parent branch-page for key %s", DKEY_DEBUG(&sepkey));
    cASSERT(mc, page_numkeys(mn->pg[prev_top]) > 2);
    cASSERT(mc, !pure_left);
    const int top = mc->top;
    const int height = mc->tree->height;
    mn->top -= 1;
    did_split_parent = true;
    couple.outer.next = mn->txn->cursors[cursor_dbi(mn)];
    mn->txn->cursors[cursor_dbi(mn)] = &couple.outer;
    rc = page_split(mn, &sepkey, nullptr, sister->pgno, 0);
    mn->txn->cursors[cursor_dbi(mn)] = couple.outer.next;
    if (unlikely(rc != MDBX_SUCCESS))
      goto done;
    cASSERT(mc, mc->top - top == mc->tree->height - height);
    if (AUDIT_ENABLED()) {
      rc = cursor_check_updating(mc);
      if (unlikely(rc != MDBX_SUCCESS))
        goto done;
    }

    /* root split? */
    prev_top += mc->top - top;

    /* Right page might now have changed parent.
     * Check if left page also changed parent. */
    if (mn->pg[prev_top] != mc->pg[prev_top] && mc->ki[prev_top] >= page_numkeys(mc->pg[prev_top])) {
      for (intptr_t i = 0; i < prev_top; i++) {
        mc->pg[i] = mn->pg[i];
        mc->ki[i] = mn->ki[i];
      }
      mc->pg[prev_top] = mn->pg[prev_top];
      if (mn->ki[prev_top]) {
        mc->ki[prev_top] = mn->ki[prev_top] - 1;
      } else {
        /* find right page's left sibling */
        mc->ki[prev_top] = mn->ki[prev_top];
        rc = cursor_sibling_left(mc);
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
    page_t *ptop_page = mc->pg[prev_top];
    TRACE("pure-left: adding to parent page %u node[%u] left-leaf page #%u key "
          "%s",
          ptop_page->pgno, mc->ki[prev_top], sister->pgno, DKEY(mc->ki[prev_top] ? newkey : nullptr));
    assert(mc->top == prev_top + 1);
    mc->top = (uint8_t)prev_top;
    rc = node_add_branch(mc, mc->ki[prev_top], mc->ki[prev_top] ? newkey : nullptr, sister->pgno);
    cASSERT(mc, mp == mc->pg[prev_top + 1] && newindx == mc->ki[prev_top + 1] && prev_top == mc->top);

    if (likely(rc == MDBX_SUCCESS) && mc->ki[prev_top] == 0) {
      node_t *node = page_node(mc->pg[prev_top], 1);
      TRACE("pure-left: update prev-first key on parent to %s", DKEY(&sepkey));
      cASSERT(mc, node_ks(node) == 0 && node_pgno(node) == mp->pgno);
      cASSERT(mc, mc->top == prev_top && mc->ki[prev_top] == 0);
      mc->ki[prev_top] = 1;
      rc = tree_propagate_key(mc, &sepkey);
      cASSERT(mc, mc->top == prev_top && mc->ki[prev_top] == 1);
      cASSERT(mc, mp == mc->pg[prev_top + 1] && newindx == mc->ki[prev_top + 1]);
      mc->ki[prev_top] = 0;
    } else {
      TRACE("pure-left: no-need-update prev-first key on parent %s", DKEY(&sepkey));
    }

    mc->top++;
    if (unlikely(rc != MDBX_SUCCESS))
      goto done;

    node_t *node = page_node(mc->pg[prev_top], mc->ki[prev_top] + (size_t)1);
    cASSERT(mc, node_pgno(node) == mp->pgno && mc->pg[prev_top] == ptop_page);
  } else {
    mn->top -= 1;
    TRACE("add-to-parent the right-entry[%u] for new sibling-page", mn->ki[prev_top]);
    rc = node_add_branch(mn, mn->ki[prev_top], &sepkey, sister->pgno);
    mn->top += 1;
    if (unlikely(rc != MDBX_SUCCESS))
      goto done;
  }

  if (unlikely(pure_left | pure_right)) {
    mc->pg[mc->top] = sister;
    mc->ki[mc->top] = 0;
    switch (page_type(sister)) {
    case P_LEAF: {
      cASSERT(mc, newpgno == 0 || newpgno == P_INVALID);
      rc = node_add_leaf(mc, 0, newkey, newdata, naf);
    } break;
    case P_LEAF | P_DUPFIX: {
      cASSERT(mc, (naf & (N_BIG | N_TREE | N_DUP)) == 0);
      cASSERT(mc, newpgno == 0 || newpgno == P_INVALID);
      rc = node_add_dupfix(mc, 0, newkey);
    } break;
    default:
      rc = bad_page(sister, "wrong page-type %u\n", page_type(sister));
    }
    if (unlikely(rc != MDBX_SUCCESS))
      goto done;

    if (pure_right) {
      for (intptr_t i = 0; i < mc->top; i++)
        mc->ki[i] = mn->ki[i];
    } else if (mc->ki[mc->top - 1] == 0) {
      for (intptr_t i = 2; i <= mc->top; ++i)
        if (mc->ki[mc->top - i]) {
          sepkey = get_key(page_node(mc->pg[mc->top - i], mc->ki[mc->top - i]));
          if (mc->clc->k.cmp(newkey, &sepkey) < 0) {
            mc->top -= (int8_t)i;
            DEBUG("pure-left: update new-first on parent [%i] page %u key %s", mc->ki[mc->top], mc->pg[mc->top]->pgno,
                  DKEY(newkey));
            rc = tree_propagate_key(mc, newkey);
            mc->top += (int8_t)i;
            if (unlikely(rc != MDBX_SUCCESS))
              goto done;
          }
          break;
        }
    }
  } else if (tmp_ki_copy) { /* !is_dupfix_leaf(mp) */
    /* Move nodes */
    mc->pg[mc->top] = sister;
    size_t n = 0, ii = split_indx;
    do {
      TRACE("i %zu, nkeys %zu => n %zu, rp #%u", ii, nkeys, n, sister->pgno);
      pgno_t pgno = 0;
      MDBX_val *rdata = nullptr;
      if (ii == newindx) {
        rkey = *newkey;
        if (is_leaf(mp))
          rdata = newdata;
        else
          pgno = newpgno;
        flags = naf;
        /* Update index for the new key. */
        mc->ki[mc->top] = (indx_t)n;
      } else {
        node_t *node = ptr_disp(mp, tmp_ki_copy->entries[ii] + PAGEHDRSZ);
        rkey.iov_base = node_key(node);
        rkey.iov_len = node_ks(node);
        if (is_leaf(mp)) {
          xdata.iov_base = node_data(node);
          xdata.iov_len = node_ds(node);
          rdata = &xdata;
        } else
          pgno = node_pgno(node);
        flags = node_flags(node);
      }

      switch (page_type(sister)) {
      case P_BRANCH: {
        cASSERT(mc, 0 == (uint16_t)flags);
        /* First branch index doesn't need key data. */
        rc = node_add_branch(mc, n, n ? &rkey : nullptr, pgno);
      } break;
      case P_LEAF: {
        cASSERT(mc, pgno == 0);
        cASSERT(mc, rdata != nullptr);
        rc = node_add_leaf(mc, n, &rkey, rdata, flags);
      } break;
      /* case P_LEAF | P_DUPFIX: {
        cASSERT(mc, (nflags & (N_BIG | N_TREE | N_DUP)) == 0);
        cASSERT(mc, gno == 0);
        rc = mdbx_node_add_dupfix(mc, n, &rkey);
      } break; */
      default:
        rc = bad_page(sister, "wrong page-type %u\n", page_type(sister));
      }
      if (unlikely(rc != MDBX_SUCCESS))
        goto done;

      ++n;
      if (++ii > nkeys) {
        ii = 0;
        n = 0;
        mc->pg[mc->top] = tmp_ki_copy;
        TRACE("switch to mp #%u", tmp_ki_copy->pgno);
      }
    } while (ii != split_indx);

    TRACE("ii %zu, nkeys %zu, n %zu, pgno #%u", ii, nkeys, n, mc->pg[mc->top]->pgno);

    nkeys = page_numkeys(tmp_ki_copy);
    for (size_t i = 0; i < nkeys; i++)
      mp->entries[i] = tmp_ki_copy->entries[i];
    mp->lower = tmp_ki_copy->lower;
    mp->upper = tmp_ki_copy->upper;
    memcpy(page_node(mp, nkeys - 1), page_node(tmp_ki_copy, nkeys - 1), env->ps - tmp_ki_copy->upper - PAGEHDRSZ);

    /* reset back to original page */
    if (newindx < split_indx) {
      mc->pg[mc->top] = mp;
    } else {
      mc->pg[mc->top] = sister;
      mc->ki[prev_top]++;
      /* Make sure ki is still valid. */
      if (mn->pg[prev_top] != mc->pg[prev_top] && mc->ki[prev_top] >= page_numkeys(mc->pg[prev_top])) {
        for (intptr_t i = 0; i <= prev_top; i++) {
          mc->pg[i] = mn->pg[i];
          mc->ki[i] = mn->ki[i];
        }
      }
    }
  } else if (newindx >= split_indx) {
    mc->pg[mc->top] = sister;
    mc->ki[prev_top]++;
    /* Make sure ki is still valid. */
    if (mn->pg[prev_top] != mc->pg[prev_top] && mc->ki[prev_top] >= page_numkeys(mc->pg[prev_top])) {
      for (intptr_t i = 0; i <= prev_top; i++) {
        mc->pg[i] = mn->pg[i];
        mc->ki[i] = mn->ki[i];
      }
    }
  }

  /* Adjust other cursors pointing to mp and/or to parent page */
  nkeys = page_numkeys(mp);
  for (MDBX_cursor *m2 = mc->txn->cursors[cursor_dbi(mc)]; m2; m2 = m2->next) {
    MDBX_cursor *m3 = (mc->flags & z_inner) ? &m2->subcur->cursor : m2;
    if (!is_pointed(m3) || m3 == mc)
      continue;
    if (foliage) {
      /* sub cursors may be on different DB */
      if (m3->pg[0] != mp)
        continue;
      /* root split */
      for (intptr_t k = foliage; k >= 0; k--) {
        m3->ki[k + 1] = m3->ki[k];
        m3->pg[k + 1] = m3->pg[k];
      }
      m3->ki[0] = m3->ki[0] >= nkeys + pure_left;
      m3->pg[0] = mc->pg[0];
      m3->top += 1;
    }

    if (m3->top >= mc->top && m3->pg[mc->top] == mp && !pure_left) {
      if (m3->ki[mc->top] >= newindx)
        m3->ki[mc->top] += !(naf & MDBX_SPLIT_REPLACE);
      if (m3->ki[mc->top] >= nkeys) {
        m3->pg[mc->top] = sister;
        cASSERT(mc, m3->ki[mc->top] >= nkeys);
        m3->ki[mc->top] -= (indx_t)nkeys;
        for (intptr_t i = 0; i < mc->top; i++) {
          m3->ki[i] = mn->ki[i];
          m3->pg[i] = mn->pg[i];
        }
      }
    } else if (!did_split_parent && m3->top >= prev_top && m3->pg[prev_top] == mc->pg[prev_top] &&
               m3->ki[prev_top] >= mc->ki[prev_top]) {
      m3->ki[prev_top]++; /* also for the `pure-left` case */
    }
    if (inner_pointed(m3) && is_leaf(mp))
      cursor_inner_refresh(m3, m3->pg[mc->top], m3->ki[mc->top]);
  }
  TRACE("mp #%u left: %zd, sister #%u left: %zd", mp->pgno, page_room(mp), sister->pgno, page_room(sister));

done:
  if (tmp_ki_copy)
    page_shadow_release(env, tmp_ki_copy, 1);

  if (unlikely(rc != MDBX_SUCCESS))
    mc->txn->flags |= MDBX_TXN_ERROR;
  else {
    if (AUDIT_ENABLED())
      rc = cursor_check_updating(mc);
    if (unlikely(naf & MDBX_RESERVE)) {
      node_t *node = page_node(mc->pg[mc->top], mc->ki[mc->top]);
      if (!(node_flags(node) & N_BIG))
        newdata->iov_base = node_data(node);
    }
#if MDBX_ENABLE_PGOP_STAT
    env->lck->pgops.split.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
  }

  DEBUG("<< mp #%u, rc %d", mp->pgno, rc);
  return rc;
}

int tree_propagate_key(MDBX_cursor *mc, const MDBX_val *key) {
  page_t *mp;
  node_t *node;
  size_t len;
  ptrdiff_t delta, ksize, oksize;
  intptr_t ptr, i, nkeys, indx;
  DKBUF_DEBUG;

  cASSERT(mc, cursor_is_tracked(mc));
  indx = mc->ki[mc->top];
  mp = mc->pg[mc->top];
  node = page_node(mp, indx);
  ptr = mp->entries[indx];
#if MDBX_DEBUG
  MDBX_val k2;
  k2.iov_base = node_key(node);
  k2.iov_len = node_ks(node);
  DEBUG("update key %zi (offset %zu) [%s] to [%s] on page %" PRIaPGNO, indx, ptr, DVAL_DEBUG(&k2), DKEY_DEBUG(key),
        mp->pgno);
#endif /* MDBX_DEBUG */

  /* Sizes must be 2-byte aligned. */
  ksize = EVEN_CEIL(key->iov_len);
  oksize = EVEN_CEIL(node_ks(node));
  delta = ksize - oksize;

  /* Shift node contents if EVEN_CEIL(key length) changed. */
  if (delta) {
    if (delta > (int)page_room(mp)) {
      /* not enough space left, do a delete and split */
      DEBUG("Not enough room, delta = %zd, splitting...", delta);
      pgno_t pgno = node_pgno(node);
      node_del(mc, 0);
      int err = page_split(mc, key, nullptr, pgno, MDBX_SPLIT_REPLACE);
      if (err == MDBX_SUCCESS && AUDIT_ENABLED())
        err = cursor_check_updating(mc);
      return err;
    }

    nkeys = page_numkeys(mp);
    for (i = 0; i < nkeys; i++) {
      if (mp->entries[i] <= ptr) {
        cASSERT(mc, mp->entries[i] >= delta);
        mp->entries[i] -= (indx_t)delta;
      }
    }

    void *const base = ptr_disp(mp, mp->upper + PAGEHDRSZ);
    len = ptr - mp->upper + NODESIZE;
    memmove(ptr_disp(base, -delta), base, len);
    cASSERT(mc, mp->upper >= delta);
    mp->upper -= (indx_t)delta;

    node = page_node(mp, indx);
  }

  /* But even if no shift was needed, update ksize */
  node_set_ks(node, key->iov_len);

  if (likely(key->iov_len /* to avoid UBSAN traps*/ != 0))
    memcpy(node_key(node), key->iov_base, key->iov_len);
  return MDBX_SUCCESS;
}
