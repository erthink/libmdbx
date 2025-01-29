/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025

#include "internals.h"

static inline tree_t *outer_tree(MDBX_cursor *mc) {
  cASSERT(mc, (mc->flags & z_inner) != 0);
  subcur_t *mx = container_of(mc->tree, subcur_t, nested_tree);
  cursor_couple_t *couple = container_of(mx, cursor_couple_t, inner);
  cASSERT(mc, mc->tree == &couple->outer.subcur->nested_tree);
  cASSERT(mc, &mc->clc->k == &couple->outer.clc->v);
  return couple->outer.tree;
}

pgr_t page_new(MDBX_cursor *mc, const unsigned flags) {
  cASSERT(mc, (flags & P_LARGE) == 0);
  pgr_t ret = gc_alloc_single(mc);
  if (unlikely(ret.err != MDBX_SUCCESS))
    return ret;

  DEBUG("db %zu allocated new page %" PRIaPGNO, cursor_dbi(mc), ret.page->pgno);
  ret.page->flags = (uint16_t)flags;
  cASSERT(mc, *cursor_dbi_state(mc) & DBI_DIRTY);
  cASSERT(mc, mc->txn->flags & MDBX_TXN_DIRTY);
#if MDBX_ENABLE_PGOP_STAT
  mc->txn->env->lck->pgops.newly.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */

  STATIC_ASSERT(P_BRANCH == 1);
  const unsigned is_branch = flags & P_BRANCH;

  ret.page->lower = 0;
  ret.page->upper = (indx_t)(mc->txn->env->ps - PAGEHDRSZ);
  mc->tree->branch_pages += is_branch;
  mc->tree->leaf_pages += 1 - is_branch;
  if (unlikely(mc->flags & z_inner)) {
    tree_t *outer = outer_tree(mc);
    outer->branch_pages += is_branch;
    outer->leaf_pages += 1 - is_branch;
  }
  return ret;
}

pgr_t page_new_large(MDBX_cursor *mc, const size_t npages) {
  pgr_t ret = likely(npages == 1) ? gc_alloc_single(mc) : gc_alloc_ex(mc, npages, ALLOC_DEFAULT);
  if (unlikely(ret.err != MDBX_SUCCESS))
    return ret;

  DEBUG("dbi %zu allocated new large-page %" PRIaPGNO ", num %zu", cursor_dbi(mc), ret.page->pgno, npages);
  ret.page->flags = P_LARGE;
  cASSERT(mc, *cursor_dbi_state(mc) & DBI_DIRTY);
  cASSERT(mc, mc->txn->flags & MDBX_TXN_DIRTY);
#if MDBX_ENABLE_PGOP_STAT
  mc->txn->env->lck->pgops.newly.weak += npages;
#endif /* MDBX_ENABLE_PGOP_STAT */

  mc->tree->large_pages += (pgno_t)npages;
  ret.page->pages = (pgno_t)npages;
  cASSERT(mc, !(mc->flags & z_inner));
  return ret;
}

__hot void page_copy(page_t *const dst, const page_t *const src, const size_t size) {
  STATIC_ASSERT(UINT16_MAX > MDBX_MAX_PAGESIZE - PAGEHDRSZ);
  STATIC_ASSERT(MDBX_MIN_PAGESIZE > PAGEHDRSZ + NODESIZE * 4);
  void *copy_dst = dst;
  const void *copy_src = src;
  size_t copy_len = size;
  if (src->flags & P_DUPFIX) {
    copy_len = PAGEHDRSZ + src->dupfix_ksize * page_numkeys(src);
    if (unlikely(copy_len > size))
      goto bailout;
  } else if ((src->flags & P_LARGE) == 0) {
    size_t upper = src->upper, lower = src->lower;
    intptr_t unused = upper - lower;
    /* If page isn't full, just copy the used portion. Adjust
     * alignment so memcpy may copy words instead of bytes. */
    if (unused > MDBX_CACHELINE_SIZE * 3) {
      lower = ceil_powerof2(lower + PAGEHDRSZ, sizeof(void *));
      upper = floor_powerof2(upper + PAGEHDRSZ, sizeof(void *));
      if (unlikely(upper > copy_len))
        goto bailout;
      memcpy(copy_dst, copy_src, lower);
      copy_dst = ptr_disp(copy_dst, upper);
      copy_src = ptr_disp(copy_src, upper);
      copy_len -= upper;
    }
  }
  memcpy(copy_dst, copy_src, copy_len);
  return;

bailout:
  if (src->flags & P_DUPFIX)
    bad_page(src, "%s addr %p, n-keys %zu, ksize %u", "invalid/corrupted source page", __Wpedantic_format_voidptr(src),
             page_numkeys(src), src->dupfix_ksize);
  else
    bad_page(src, "%s addr %p, upper %u", "invalid/corrupted source page", __Wpedantic_format_voidptr(src), src->upper);
  memset(dst, -1, size);
}

__cold pgr_t __must_check_result page_unspill(MDBX_txn *const txn, const page_t *const mp) {
  VERBOSE("unspill page %" PRIaPGNO, mp->pgno);
  tASSERT(txn, (txn->flags & MDBX_WRITEMAP) == 0);
  tASSERT(txn, is_spilled(txn, mp));
  const MDBX_txn *scan = txn;
  pgr_t ret;
  do {
    tASSERT(txn, (scan->flags & MDBX_TXN_SPILLS) != 0);
    const size_t si = spill_search(scan, mp->pgno);
    if (!si)
      continue;
    const unsigned npages = is_largepage(mp) ? mp->pages : 1;
    ret.page = page_shadow_alloc(txn, npages);
    if (unlikely(!ret.page)) {
      ret.err = MDBX_ENOMEM;
      return ret;
    }
    page_copy(ret.page, mp, pgno2bytes(txn->env, npages));
    if (scan == txn) {
      /* If in current txn, this page is no longer spilled.
       * If it happens to be the last page, truncate the spill list.
       * Otherwise mark it as deleted by setting the LSB. */
      spill_remove(txn, si, npages);
    } /* otherwise, if belonging to a parent txn, the
       * page remains spilled until child commits */

    ret.err = page_dirty(txn, ret.page, npages);
    if (unlikely(ret.err != MDBX_SUCCESS))
      return ret;
#if MDBX_ENABLE_PGOP_STAT
    txn->env->lck->pgops.unspill.weak += npages;
#endif /* MDBX_ENABLE_PGOP_STAT */
    ret.page->flags |= (scan == txn) ? 0 : P_SPILLED;
    ret.err = MDBX_SUCCESS;
    return ret;
  } while (likely((scan = scan->parent) != nullptr && (scan->flags & MDBX_TXN_SPILLS) != 0));
  ERROR("Page %" PRIaPGNO " mod-txnid %" PRIaTXN " not found in the spill-list(s), current txn %" PRIaTXN
        " front %" PRIaTXN ", root txn %" PRIaTXN " front %" PRIaTXN,
        mp->pgno, mp->txnid, txn->txnid, txn->front_txnid, txn->env->basal_txn->txnid,
        txn->env->basal_txn->front_txnid);
  ret.err = MDBX_PROBLEM;
  ret.page = nullptr;
  return ret;
}

__hot int page_touch_modifable(MDBX_txn *txn, const page_t *const mp) {
  tASSERT(txn, is_modifable(txn, mp) && txn->wr.dirtylist);
  tASSERT(txn, !is_largepage(mp) && !is_subpage(mp));
  tASSERT(txn, (txn->flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);

  const size_t n = dpl_search(txn, mp->pgno);
  if (MDBX_AVOID_MSYNC && unlikely(txn->wr.dirtylist->items[n].pgno != mp->pgno)) {
    tASSERT(txn, (txn->flags & MDBX_WRITEMAP));
    tASSERT(txn, n > 0 && n <= txn->wr.dirtylist->length + 1);
    VERBOSE("unspill page %" PRIaPGNO, mp->pgno);
#if MDBX_ENABLE_PGOP_STAT
    txn->env->lck->pgops.unspill.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
    return page_dirty(txn, (page_t *)mp, 1);
  }

  tASSERT(txn, n > 0 && n <= txn->wr.dirtylist->length);
  tASSERT(txn, txn->wr.dirtylist->items[n].pgno == mp->pgno && txn->wr.dirtylist->items[n].ptr == mp);
  if (!MDBX_AVOID_MSYNC || (txn->flags & MDBX_WRITEMAP) == 0) {
    size_t *const ptr = ptr_disp(txn->wr.dirtylist->items[n].ptr, -(ptrdiff_t)sizeof(size_t));
    *ptr = txn->wr.dirtylru;
  }
  return MDBX_SUCCESS;
}

__hot int page_touch_unmodifable(MDBX_txn *txn, MDBX_cursor *mc, const page_t *const mp) {
  tASSERT(txn, !is_modifable(txn, mp) && !is_largepage(mp));
  if (is_subpage(mp)) {
    ((page_t *)mp)->txnid = txn->front_txnid;
    return MDBX_SUCCESS;
  }

  int rc;
  page_t *np;
  if (is_frozen(txn, mp)) {
    /* CoW the page */
    rc = pnl_need(&txn->wr.retired_pages, 1);
    if (unlikely(rc != MDBX_SUCCESS))
      goto fail;
    const pgr_t par = gc_alloc_single(mc);
    rc = par.err;
    np = par.page;
    if (unlikely(rc != MDBX_SUCCESS))
      goto fail;

    const pgno_t pgno = np->pgno;
    DEBUG("touched db %d page %" PRIaPGNO " -> %" PRIaPGNO, cursor_dbi_dbg(mc), mp->pgno, pgno);
    tASSERT(txn, mp->pgno != pgno);
    pnl_append_prereserved(txn->wr.retired_pages, mp->pgno);
    /* Update the parent page, if any, to point to the new page */
    if (likely(mc->top)) {
      page_t *parent = mc->pg[mc->top - 1];
      node_t *node = page_node(parent, mc->ki[mc->top - 1]);
      node_set_pgno(node, pgno);
    } else {
      mc->tree->root = pgno;
    }

#if MDBX_ENABLE_PGOP_STAT
    txn->env->lck->pgops.cow.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
    page_copy(np, mp, txn->env->ps);
    np->pgno = pgno;
    np->txnid = txn->front_txnid;
  } else if (is_spilled(txn, mp)) {
    pgr_t pur = page_unspill(txn, mp);
    np = pur.page;
    rc = pur.err;
    if (likely(rc == MDBX_SUCCESS)) {
      tASSERT(txn, np != nullptr);
      goto done;
    }
    goto fail;
  } else {
    if (unlikely(!txn->parent)) {
      ERROR("Unexpected not frozen/modifiable/spilled but shadowed %s "
            "page %" PRIaPGNO " mod-txnid %" PRIaTXN ","
            " without parent transaction, current txn %" PRIaTXN " front %" PRIaTXN,
            is_branch(mp) ? "branch" : "leaf", mp->pgno, mp->txnid, mc->txn->txnid, mc->txn->front_txnid);
      rc = MDBX_PROBLEM;
      goto fail;
    }

    DEBUG("clone db %d page %" PRIaPGNO, cursor_dbi_dbg(mc), mp->pgno);
    tASSERT(txn, txn->wr.dirtylist->length <= PAGELIST_LIMIT + MDBX_PNL_GRANULATE);
    /* No - copy it */
    np = page_shadow_alloc(txn, 1);
    if (unlikely(!np)) {
      rc = MDBX_ENOMEM;
      goto fail;
    }
    page_copy(np, mp, txn->env->ps);

    /* insert a clone of parent's dirty page, so don't touch dirtyroom */
    rc = page_dirty(txn, np, 1);
    if (unlikely(rc != MDBX_SUCCESS))
      goto fail;

#if MDBX_ENABLE_PGOP_STAT
    txn->env->lck->pgops.clone.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
  }

done:
  /* Adjust cursors pointing to mp */
  mc->pg[mc->top] = np;
  MDBX_cursor *m2 = txn->cursors[cursor_dbi(mc)];
  if (mc->flags & z_inner) {
    for (; m2; m2 = m2->next) {
      MDBX_cursor *m3 = &m2->subcur->cursor;
      if (m3->top < mc->top)
        continue;
      if (m3->pg[mc->top] == mp)
        m3->pg[mc->top] = np;
    }
  } else {
    for (; m2; m2 = m2->next) {
      if (m2->top < mc->top)
        continue;
      if (m2->pg[mc->top] == mp) {
        m2->pg[mc->top] = np;
        if (is_leaf(np) && inner_pointed(m2))
          cursor_inner_refresh(m2, np, m2->ki[mc->top]);
      }
    }
  }
  return MDBX_SUCCESS;

fail:
  txn->flags |= MDBX_TXN_ERROR;
  return rc;
}

page_t *page_shadow_alloc(MDBX_txn *txn, size_t num) {
  MDBX_env *env = txn->env;
  page_t *np = env->shadow_reserve;
  size_t size = env->ps;
  if (likely(num == 1 && np)) {
    eASSERT(env, env->shadow_reserve_len > 0);
    MDBX_ASAN_UNPOISON_MEMORY_REGION(np, size);
    VALGRIND_MEMPOOL_ALLOC(env, ptr_disp(np, -(ptrdiff_t)sizeof(size_t)), size + sizeof(size_t));
    VALGRIND_MAKE_MEM_DEFINED(&page_next(np), sizeof(page_t *));
    env->shadow_reserve = page_next(np);
    env->shadow_reserve_len -= 1;
  } else {
    size = pgno2bytes(env, num);
    void *const ptr = osal_malloc(size + sizeof(size_t));
    if (unlikely(!ptr)) {
      txn->flags |= MDBX_TXN_ERROR;
      return nullptr;
    }
    VALGRIND_MEMPOOL_ALLOC(env, ptr, size + sizeof(size_t));
    np = ptr_disp(ptr, sizeof(size_t));
  }

  if ((env->flags & MDBX_NOMEMINIT) == 0) {
    /* For a single page alloc, we init everything after the page header.
     * For multi-page, we init the final page; if the caller needed that
     * many pages they will be filling in at least up to the last page. */
    size_t skip = PAGEHDRSZ;
    if (num > 1)
      skip += pgno2bytes(env, num - 1);
    memset(ptr_disp(np, skip), 0, size - skip);
  }
#if MDBX_DEBUG
  np->pgno = 0;
#endif
  VALGRIND_MAKE_MEM_UNDEFINED(np, size);
  np->flags = 0;
  np->pages = (pgno_t)num;
  return np;
}

void page_shadow_release(MDBX_env *env, page_t *dp, size_t npages) {
  VALGRIND_MAKE_MEM_UNDEFINED(dp, pgno2bytes(env, npages));
  MDBX_ASAN_UNPOISON_MEMORY_REGION(dp, pgno2bytes(env, npages));
  if (unlikely(env->flags & MDBX_PAGEPERTURB))
    memset(dp, -1, pgno2bytes(env, npages));
  if (likely(npages == 1 && env->shadow_reserve_len < env->options.dp_reserve_limit)) {
    MDBX_ASAN_POISON_MEMORY_REGION(dp, env->ps);
    MDBX_ASAN_UNPOISON_MEMORY_REGION(&page_next(dp), sizeof(page_t *));
    page_next(dp) = env->shadow_reserve;
    VALGRIND_MEMPOOL_FREE(env, ptr_disp(dp, -(ptrdiff_t)sizeof(size_t)));
    env->shadow_reserve = dp;
    env->shadow_reserve_len += 1;
  } else {
    /* large pages just get freed directly */
    void *const ptr = ptr_disp(dp, -(ptrdiff_t)sizeof(size_t));
    VALGRIND_MEMPOOL_FREE(env, ptr);
    osal_free(ptr);
  }
}

__cold static void page_kill(MDBX_txn *txn, page_t *mp, pgno_t pgno, size_t npages) {
  MDBX_env *const env = txn->env;
  DEBUG("kill %zu page(s) %" PRIaPGNO, npages, pgno);
  eASSERT(env, pgno >= NUM_METAS && npages);
  if (!is_frozen(txn, mp)) {
    const size_t bytes = pgno2bytes(env, npages);
    memset(mp, -1, bytes);
    mp->pgno = pgno;
    if ((txn->flags & MDBX_WRITEMAP) == 0)
      osal_pwrite(env->lazy_fd, mp, bytes, pgno2bytes(env, pgno));
  } else {
    struct iovec iov[MDBX_AUXILARY_IOV_MAX];
    iov[0].iov_len = env->ps;
    iov[0].iov_base = ptr_disp(env->page_auxbuf, env->ps);
    size_t iov_off = pgno2bytes(env, pgno), n = 1;
    while (--npages) {
      iov[n] = iov[0];
      if (++n == MDBX_AUXILARY_IOV_MAX) {
        osal_pwritev(env->lazy_fd, iov, MDBX_AUXILARY_IOV_MAX, iov_off);
        iov_off += pgno2bytes(env, MDBX_AUXILARY_IOV_MAX);
        n = 0;
      }
    }
    osal_pwritev(env->lazy_fd, iov, n, iov_off);
  }
}

static inline bool suitable4loose(const MDBX_txn *txn, pgno_t pgno) {
  /* TODO:
   *  1) при включенной "экономии последовательностей" проверить, что
   *     страница не примыкает к какой-либо из уже находящийся в reclaimed.
   *  2) стоит подумать над тем, чтобы при большом loose-списке отбрасывать
         половину в reclaimed. */
  return txn->wr.loose_count < txn->env->options.dp_loose_limit &&
         (!MDBX_ENABLE_REFUND ||
          /* skip pages near to the end in favor of compactification */
          txn->geo.first_unallocated > pgno + txn->env->options.dp_loose_limit ||
          txn->geo.first_unallocated <= txn->env->options.dp_loose_limit);
}

/* Retire, loosen or free a single page.
 *
 * For dirty pages, saves single pages to a list for future reuse in this same
 * txn. It has been pulled from the GC and already resides on the dirty list,
 * but has been deleted. Use these pages first before pulling again from the GC.
 *
 * If the page wasn't dirtied in this txn, just add it
 * to this txn's free list. */
int page_retire_ex(MDBX_cursor *mc, const pgno_t pgno, page_t *mp /* maybe null */,
                   unsigned pageflags /* maybe unknown/zero */) {
  int rc;
  MDBX_txn *const txn = mc->txn;
  tASSERT(txn, !mp || (mp->pgno == pgno && mp->flags == pageflags));

  /* During deleting entire subtrees, it is reasonable and possible to avoid
   * reading leaf pages, i.e. significantly reduce hard page-faults & IOPs:
   *  - mp is null, i.e. the page has not yet been read;
   *  - pagetype is known and the P_LEAF bit is set;
   *  - we can determine the page status via scanning the lists
   *    of dirty and spilled pages.
   *
   *  On the other hand, this could be suboptimal for WRITEMAP mode, since
   *  requires support the list of dirty pages and avoid explicit spilling.
   *  So for flexibility and avoid extra internal dependencies we just
   *  fallback to reading if dirty list was not allocated yet. */
  size_t di = 0, si = 0, npages = 1;
  enum page_status { unknown, frozen, spilled, shadowed, modifable } status = unknown;

  if (unlikely(!mp)) {
    if (ASSERT_ENABLED() && pageflags) {
      pgr_t check;
      check = page_get_any(mc, pgno, txn->front_txnid);
      if (unlikely(check.err != MDBX_SUCCESS))
        return check.err;
      tASSERT(txn, ((unsigned)check.page->flags & ~P_SPILLED) == (pageflags & ~P_FROZEN));
      tASSERT(txn, !(pageflags & P_FROZEN) || is_frozen(txn, check.page));
    }
    if (pageflags & P_FROZEN) {
      status = frozen;
      if (ASSERT_ENABLED()) {
        for (MDBX_txn *scan = txn; scan; scan = scan->parent) {
          tASSERT(txn, !txn->wr.spilled.list || !spill_search(scan, pgno));
          tASSERT(txn, !scan->wr.dirtylist || !debug_dpl_find(scan, pgno));
        }
      }
      goto status_done;
    } else if (pageflags && txn->wr.dirtylist) {
      if ((di = dpl_exist(txn, pgno)) != 0) {
        mp = txn->wr.dirtylist->items[di].ptr;
        tASSERT(txn, is_modifable(txn, mp));
        status = modifable;
        goto status_done;
      }
      if ((si = spill_search(txn, pgno)) != 0) {
        status = spilled;
        goto status_done;
      }
      for (MDBX_txn *parent = txn->parent; parent; parent = parent->parent) {
        if (dpl_exist(parent, pgno)) {
          status = shadowed;
          goto status_done;
        }
        if (spill_search(parent, pgno)) {
          status = spilled;
          goto status_done;
        }
      }
      status = frozen;
      goto status_done;
    }

    pgr_t pg = page_get_any(mc, pgno, txn->front_txnid);
    if (unlikely(pg.err != MDBX_SUCCESS))
      return pg.err;
    mp = pg.page;
    tASSERT(txn, !pageflags || mp->flags == pageflags);
    pageflags = mp->flags;
  }

  if (is_frozen(txn, mp)) {
    status = frozen;
    tASSERT(txn, !is_modifable(txn, mp));
    tASSERT(txn, !is_spilled(txn, mp));
    tASSERT(txn, !is_shadowed(txn, mp));
    tASSERT(txn, !debug_dpl_find(txn, pgno));
    tASSERT(txn, !txn->wr.spilled.list || !spill_search(txn, pgno));
  } else if (is_modifable(txn, mp)) {
    status = modifable;
    if (txn->wr.dirtylist)
      di = dpl_exist(txn, pgno);
    tASSERT(txn, (txn->flags & MDBX_WRITEMAP) || !is_spilled(txn, mp));
    tASSERT(txn, !txn->wr.spilled.list || !spill_search(txn, pgno));
  } else if (is_shadowed(txn, mp)) {
    status = shadowed;
    tASSERT(txn, !txn->wr.spilled.list || !spill_search(txn, pgno));
    tASSERT(txn, !debug_dpl_find(txn, pgno));
  } else {
    tASSERT(txn, is_spilled(txn, mp));
    status = spilled;
    si = spill_search(txn, pgno);
    tASSERT(txn, !debug_dpl_find(txn, pgno));
  }

status_done:
  if (likely((pageflags & P_LARGE) == 0)) {
    STATIC_ASSERT(P_BRANCH == 1);
    const bool is_branch = pageflags & P_BRANCH;
    cASSERT(mc, ((pageflags & P_LEAF) == 0) == is_branch);
    if (unlikely(mc->flags & z_inner)) {
      tree_t *outer = outer_tree(mc);
      cASSERT(mc, !is_branch || outer->branch_pages > 0);
      outer->branch_pages -= is_branch;
      cASSERT(mc, is_branch || outer->leaf_pages > 0);
      outer->leaf_pages -= 1 - is_branch;
    }
    cASSERT(mc, !is_branch || mc->tree->branch_pages > 0);
    mc->tree->branch_pages -= is_branch;
    cASSERT(mc, is_branch || mc->tree->leaf_pages > 0);
    mc->tree->leaf_pages -= 1 - is_branch;
  } else {
    npages = mp->pages;
    cASSERT(mc, mc->tree->large_pages >= npages);
    mc->tree->large_pages -= (pgno_t)npages;
  }

  if (status == frozen) {
  retire:
    DEBUG("retire %zu page %" PRIaPGNO, npages, pgno);
    rc = pnl_append_span(&txn->wr.retired_pages, pgno, npages);
    tASSERT(txn, dpl_check(txn));
    return rc;
  }

  /* Возврат страниц в нераспределенный "хвост" БД.
   * Содержимое страниц не уничтожается, а для вложенных транзакций граница
   * нераспределенного "хвоста" БД сдвигается только при их коммите. */
  if (MDBX_ENABLE_REFUND && unlikely(pgno + npages == txn->geo.first_unallocated)) {
    const char *kind = nullptr;
    if (status == modifable) {
      /* Страница испачкана в этой транзакции, но до этого могла быть
       * аллоцирована, испачкана и пролита в одной из родительских транзакций.
       * Её МОЖНО вытолкнуть в нераспределенный хвост. */
      kind = "dirty";
      /* Remove from dirty list */
      page_wash(txn, di, mp, npages);
    } else if (si) {
      /* Страница пролита в этой транзакции, т.е. она аллоцирована
       * и запачкана в этой или одной из родительских транзакций.
       * Её МОЖНО вытолкнуть в нераспределенный хвост. */
      kind = "spilled";
      tASSERT(txn, status == spilled);
      spill_remove(txn, si, npages);
    } else {
      /* Страница аллоцирована, запачкана и возможно пролита в одной
       * из родительских транзакций.
       * Её МОЖНО вытолкнуть в нераспределенный хвост. */
      kind = "parent's";
      if (ASSERT_ENABLED() && mp) {
        kind = nullptr;
        for (MDBX_txn *parent = txn->parent; parent; parent = parent->parent) {
          if (spill_search(parent, pgno)) {
            kind = "parent-spilled";
            tASSERT(txn, status == spilled);
            break;
          }
          if (mp == debug_dpl_find(parent, pgno)) {
            kind = "parent-dirty";
            tASSERT(txn, status == shadowed);
            break;
          }
        }
        tASSERT(txn, kind != nullptr);
      }
      tASSERT(txn, status == spilled || status == shadowed);
    }
    DEBUG("refunded %zu %s page %" PRIaPGNO, npages, kind, pgno);
    txn->geo.first_unallocated = pgno;
    txn_refund(txn);
    return MDBX_SUCCESS;
  }

  if (status == modifable) {
    /* Dirty page from this transaction */
    /* If suitable we can reuse it through loose list */
    if (likely(npages == 1 && suitable4loose(txn, pgno)) && (di || !txn->wr.dirtylist)) {
      DEBUG("loosen dirty page %" PRIaPGNO, pgno);
      if (MDBX_DEBUG != 0 || unlikely(txn->env->flags & MDBX_PAGEPERTURB))
        memset(page_data(mp), -1, txn->env->ps - PAGEHDRSZ);
      mp->txnid = INVALID_TXNID;
      mp->flags = P_LOOSE;
      page_next(mp) = txn->wr.loose_pages;
      txn->wr.loose_pages = mp;
      txn->wr.loose_count++;
#if MDBX_ENABLE_REFUND
      txn->wr.loose_refund_wl = (pgno + 2 > txn->wr.loose_refund_wl) ? pgno + 2 : txn->wr.loose_refund_wl;
#endif /* MDBX_ENABLE_REFUND */
      VALGRIND_MAKE_MEM_NOACCESS(page_data(mp), txn->env->ps - PAGEHDRSZ);
      MDBX_ASAN_POISON_MEMORY_REGION(page_data(mp), txn->env->ps - PAGEHDRSZ);
      return MDBX_SUCCESS;
    }

#if !MDBX_DEBUG && !defined(ENABLE_MEMCHECK) && !defined(__SANITIZE_ADDRESS__)
    if (unlikely(txn->env->flags & MDBX_PAGEPERTURB))
#endif
    {
      /* Страница могла быть изменена в одной из родительских транзакций,
       * в том числе, позже выгружена и затем снова загружена и изменена.
       * В обоих случаях её нельзя затирать на диске и помечать недоступной
       * в asan и/или valgrind */
      for (MDBX_txn *parent = txn->parent; parent && (parent->flags & MDBX_TXN_SPILLS); parent = parent->parent) {
        if (spill_intersect(parent, pgno, npages))
          goto skip_invalidate;
        if (dpl_intersect(parent, pgno, npages))
          goto skip_invalidate;
      }

#if defined(ENABLE_MEMCHECK) || defined(__SANITIZE_ADDRESS__)
      if (MDBX_DEBUG != 0 || unlikely(txn->env->flags & MDBX_PAGEPERTURB))
#endif
        page_kill(txn, mp, pgno, npages);
      if ((txn->flags & MDBX_WRITEMAP) == 0) {
        VALGRIND_MAKE_MEM_NOACCESS(page_data(pgno2page(txn->env, pgno)), pgno2bytes(txn->env, npages) - PAGEHDRSZ);
        MDBX_ASAN_POISON_MEMORY_REGION(page_data(pgno2page(txn->env, pgno)), pgno2bytes(txn->env, npages) - PAGEHDRSZ);
      }
    }
  skip_invalidate:

    /* wash dirty page */
    page_wash(txn, di, mp, npages);

  reclaim:
    DEBUG("reclaim %zu %s page %" PRIaPGNO, npages, "dirty", pgno);
    rc = pnl_insert_span(&txn->wr.repnl, pgno, npages);
    tASSERT(txn, pnl_check_allocated(txn->wr.repnl, txn->geo.first_unallocated - MDBX_ENABLE_REFUND));
    tASSERT(txn, dpl_check(txn));
    return rc;
  }

  if (si) {
    /* Page ws spilled in this txn */
    spill_remove(txn, si, npages);
    /* Страница могла быть выделена и затем пролита в этой транзакции,
     * тогда её необходимо поместить в reclaimed-список.
     * Либо она могла быть выделена в одной из родительских транзакций и затем
     * пролита в этой транзакции, тогда её необходимо поместить в
     * retired-список для последующей фильтрации при коммите. */
    for (MDBX_txn *parent = txn->parent; parent; parent = parent->parent) {
      if (dpl_exist(parent, pgno))
        goto retire;
    }
    /* Страница точно была выделена в этой транзакции
     * и теперь может быть использована повторно. */
    goto reclaim;
  }

  if (status == shadowed) {
    /* Dirty page MUST BE a clone from (one of) parent transaction(s). */
    if (ASSERT_ENABLED()) {
      const page_t *parent_dp = nullptr;
      /* Check parent(s)'s dirty lists. */
      for (MDBX_txn *parent = txn->parent; parent && !parent_dp; parent = parent->parent) {
        tASSERT(txn, !spill_search(parent, pgno));
        parent_dp = debug_dpl_find(parent, pgno);
      }
      tASSERT(txn, parent_dp && (!mp || parent_dp == mp));
    }
    /* Страница была выделена в родительской транзакции и теперь может быть
     * использована повторно, но только внутри этой транзакции, либо дочерних.
     */
    goto reclaim;
  }

  /* Страница может входить в доступный читателям MVCC-снимок, либо же она
   * могла быть выделена, а затем пролита в одной из родительских
   * транзакций. Поэтому пока помещаем её в retired-список, который будет
   * фильтроваться относительно dirty- и spilled-списков родительских
   * транзакций при коммите дочерних транзакций, либо же будет записан
   * в GC в неизменном виде. */
  goto retire;
}

__hot int __must_check_result page_dirty(MDBX_txn *txn, page_t *mp, size_t npages) {
  tASSERT(txn, (txn->flags & MDBX_TXN_RDONLY) == 0);
  mp->txnid = txn->front_txnid;
  if (!txn->wr.dirtylist) {
    tASSERT(txn, (txn->flags & MDBX_WRITEMAP) != 0 && !MDBX_AVOID_MSYNC);
    txn->wr.writemap_dirty_npages += npages;
    tASSERT(txn, txn->wr.spilled.list == nullptr);
    return MDBX_SUCCESS;
  }
  tASSERT(txn, (txn->flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);

#if xMDBX_DEBUG_SPILLING == 2
  txn->env->debug_dirtied_act += 1;
  ENSURE(txn->env, txn->env->debug_dirtied_act < txn->env->debug_dirtied_est);
  ENSURE(txn->env, txn->wr.dirtyroom + txn->wr.loose_count > 0);
#endif /* xMDBX_DEBUG_SPILLING == 2 */

  int rc;
  if (unlikely(txn->wr.dirtyroom == 0)) {
    if (txn->wr.loose_count) {
      page_t *lp = txn->wr.loose_pages;
      DEBUG("purge-and-reclaim loose page %" PRIaPGNO, lp->pgno);
      rc = pnl_insert_span(&txn->wr.repnl, lp->pgno, 1);
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;
      size_t di = dpl_search(txn, lp->pgno);
      tASSERT(txn, txn->wr.dirtylist->items[di].ptr == lp);
      dpl_remove(txn, di);
      MDBX_ASAN_UNPOISON_MEMORY_REGION(&page_next(lp), sizeof(page_t *));
      VALGRIND_MAKE_MEM_DEFINED(&page_next(lp), sizeof(page_t *));
      txn->wr.loose_pages = page_next(lp);
      txn->wr.loose_count--;
      txn->wr.dirtyroom++;
      if (!MDBX_AVOID_MSYNC || !(txn->flags & MDBX_WRITEMAP))
        page_shadow_release(txn->env, lp, 1);
    } else {
      ERROR("Dirtyroom is depleted, DPL length %zu", txn->wr.dirtylist->length);
      if (!MDBX_AVOID_MSYNC || !(txn->flags & MDBX_WRITEMAP))
        page_shadow_release(txn->env, mp, npages);
      return MDBX_TXN_FULL;
    }
  }

  rc = dpl_append(txn, mp->pgno, mp, npages);
  if (unlikely(rc != MDBX_SUCCESS)) {
  bailout:
    txn->flags |= MDBX_TXN_ERROR;
    return rc;
  }
  txn->wr.dirtyroom--;
  tASSERT(txn, dpl_check(txn));
  return MDBX_SUCCESS;
}

void recalculate_subpage_thresholds(MDBX_env *env) {
  size_t whole = env->leaf_nodemax - NODESIZE;
  env->subpage_limit = (whole * env->options.subpage.limit + 32767) >> 16;
  whole = env->subpage_limit;
  env->subpage_reserve_limit = (whole * env->options.subpage.reserve_limit + 32767) >> 16;
  eASSERT(env, env->leaf_nodemax >= env->subpage_limit + NODESIZE);
  eASSERT(env, env->subpage_limit >= env->subpage_reserve_limit);

  whole = env->leaf_nodemax;
  env->subpage_room_threshold = (whole * env->options.subpage.room_threshold + 32767) >> 16;
  env->subpage_reserve_prereq = (whole * env->options.subpage.reserve_prereq + 32767) >> 16;
  if (env->subpage_room_threshold + env->subpage_reserve_limit > (intptr_t)page_space(env))
    env->subpage_reserve_prereq = page_space(env);
  else if (env->subpage_reserve_prereq < env->subpage_room_threshold + env->subpage_reserve_limit)
    env->subpage_reserve_prereq = env->subpage_room_threshold + env->subpage_reserve_limit;
  eASSERT(env, env->subpage_reserve_prereq >= env->subpage_room_threshold + env->subpage_reserve_limit);
}

size_t page_subleaf2_reserve(const MDBX_env *env, size_t host_page_room, size_t subpage_len, size_t item_len) {
  eASSERT(env, (subpage_len & 1) == 0);
  eASSERT(env, env->leaf_nodemax >= env->subpage_limit + NODESIZE);
  size_t reserve = 0;
  for (size_t n = 0; n < 5 && reserve + item_len <= env->subpage_reserve_limit &&
                     EVEN_CEIL(subpage_len + item_len) <= env->subpage_limit &&
                     host_page_room >= env->subpage_reserve_prereq + EVEN_CEIL(subpage_len + item_len);
       ++n) {
    subpage_len += item_len;
    reserve += item_len;
  }
  return reserve + (subpage_len & 1);
}
