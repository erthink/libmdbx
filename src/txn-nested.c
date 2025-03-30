/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025

#include "internals.h"

/* Merge pageset of the nested txn into parent */
static void txn_merge(MDBX_txn *const parent, MDBX_txn *const txn, const size_t parent_retired_len) {
  tASSERT(txn, (txn->flags & MDBX_WRITEMAP) == 0);
  dpl_t *const src = dpl_sort(txn);

  /* Remove refunded pages from parent's dirty list */
  dpl_t *const dst = dpl_sort(parent);
  if (MDBX_ENABLE_REFUND) {
    size_t n = dst->length;
    while (n && dst->items[n].pgno >= parent->geo.first_unallocated) {
      const unsigned npages = dpl_npages(dst, n);
      page_shadow_release(txn->env, dst->items[n].ptr, npages);
      --n;
    }
    parent->wr.dirtyroom += dst->sorted - n;
    dst->sorted = dpl_setlen(dst, n);
    tASSERT(parent, parent->wr.dirtyroom + parent->wr.dirtylist->length ==
                        (parent->parent ? parent->parent->wr.dirtyroom : parent->env->options.dp_limit));
  }

  /* Remove reclaimed pages from parent's dirty list */
  const pnl_t reclaimed_list = parent->wr.repnl;
  dpl_sift(parent, reclaimed_list, false);

  /* Move retired pages from parent's dirty & spilled list to reclaimed */
  size_t r, w, d, s, l;
  for (r = w = parent_retired_len; ++r <= MDBX_PNL_GETSIZE(parent->wr.retired_pages);) {
    const pgno_t pgno = parent->wr.retired_pages[r];
    const size_t di = dpl_exist(parent, pgno);
    const size_t si = !di ? spill_search(parent, pgno) : 0;
    unsigned npages;
    const char *kind;
    if (di) {
      page_t *dp = dst->items[di].ptr;
      tASSERT(parent, (dp->flags & ~(P_LEAF | P_DUPFIX | P_BRANCH | P_LARGE | P_SPILLED)) == 0);
      npages = dpl_npages(dst, di);
      page_wash(parent, di, dp, npages);
      kind = "dirty";
      l = 1;
      if (unlikely(npages > l)) {
        /* OVERFLOW-страница могла быть переиспользована по частям. Тогда
         * в retired-списке может быть только начало последовательности,
         * а остаток растащен по dirty, spilled и reclaimed спискам. Поэтому
         * переносим в reclaimed с проверкой на обрыв последовательности.
         * В любом случае, все осколки будут учтены и отфильтрованы, т.е. если
         * страница была разбита на части, то важно удалить dirty-элемент,
         * а все осколки будут учтены отдельно. */

        /* Список retired страниц не сортирован, но для ускорения сортировки
         * дополняется в соответствии с MDBX_PNL_ASCENDING */
#if MDBX_PNL_ASCENDING
        const size_t len = MDBX_PNL_GETSIZE(parent->wr.retired_pages);
        while (r < len && parent->wr.retired_pages[r + 1] == pgno + l) {
          ++r;
          if (++l == npages)
            break;
        }
#else
        while (w > parent_retired_len && parent->wr.retired_pages[w - 1] == pgno + l) {
          --w;
          if (++l == npages)
            break;
        }
#endif
      }
    } else if (unlikely(si)) {
      l = npages = 1;
      spill_remove(parent, si, 1);
      kind = "spilled";
    } else {
      parent->wr.retired_pages[++w] = pgno;
      continue;
    }

    DEBUG("reclaim retired parent's %u -> %zu %s page %" PRIaPGNO, npages, l, kind, pgno);
    int err = pnl_insert_span(&parent->wr.repnl, pgno, l);
    ENSURE(txn->env, err == MDBX_SUCCESS);
  }
  MDBX_PNL_SETSIZE(parent->wr.retired_pages, w);

  /* Filter-out parent spill list */
  if (parent->wr.spilled.list && MDBX_PNL_GETSIZE(parent->wr.spilled.list) > 0) {
    const pnl_t sl = spill_purge(parent);
    size_t len = MDBX_PNL_GETSIZE(sl);
    if (len) {
      /* Remove refunded pages from parent's spill list */
      if (MDBX_ENABLE_REFUND && MDBX_PNL_MOST(sl) >= (parent->geo.first_unallocated << 1)) {
#if MDBX_PNL_ASCENDING
        size_t i = MDBX_PNL_GETSIZE(sl);
        assert(MDBX_PNL_MOST(sl) == MDBX_PNL_LAST(sl));
        do {
          if ((sl[i] & 1) == 0)
            DEBUG("refund parent's spilled page %" PRIaPGNO, sl[i] >> 1);
          i -= 1;
        } while (i && sl[i] >= (parent->geo.first_unallocated << 1));
        MDBX_PNL_SETSIZE(sl, i);
#else
        assert(MDBX_PNL_MOST(sl) == MDBX_PNL_FIRST(sl));
        size_t i = 0;
        do {
          ++i;
          if ((sl[i] & 1) == 0)
            DEBUG("refund parent's spilled page %" PRIaPGNO, sl[i] >> 1);
        } while (i < len && sl[i + 1] >= (parent->geo.first_unallocated << 1));
        MDBX_PNL_SETSIZE(sl, len -= i);
        memmove(sl + 1, sl + 1 + i, len * sizeof(sl[0]));
#endif
      }
      tASSERT(txn, pnl_check_allocated(sl, (size_t)parent->geo.first_unallocated << 1));

      /* Remove reclaimed pages from parent's spill list */
      s = MDBX_PNL_GETSIZE(sl), r = MDBX_PNL_GETSIZE(reclaimed_list);
      /* Scanning from end to begin */
      while (s && r) {
        if (sl[s] & 1) {
          --s;
          continue;
        }
        const pgno_t spilled_pgno = sl[s] >> 1;
        const pgno_t reclaimed_pgno = reclaimed_list[r];
        if (reclaimed_pgno != spilled_pgno) {
          const bool cmp = MDBX_PNL_ORDERED(spilled_pgno, reclaimed_pgno);
          s -= !cmp;
          r -= cmp;
        } else {
          DEBUG("remove reclaimed parent's spilled page %" PRIaPGNO, reclaimed_pgno);
          spill_remove(parent, s, 1);
          --s;
          --r;
        }
      }

      /* Remove anything in our dirty list from parent's spill list */
      /* Scanning spill list in descend order */
      const intptr_t step = MDBX_PNL_ASCENDING ? -1 : 1;
      s = MDBX_PNL_ASCENDING ? MDBX_PNL_GETSIZE(sl) : 1;
      d = src->length;
      while (d && (MDBX_PNL_ASCENDING ? s > 0 : s <= MDBX_PNL_GETSIZE(sl))) {
        if (sl[s] & 1) {
          s += step;
          continue;
        }
        const pgno_t spilled_pgno = sl[s] >> 1;
        const pgno_t dirty_pgno_form = src->items[d].pgno;
        const unsigned npages = dpl_npages(src, d);
        const pgno_t dirty_pgno_to = dirty_pgno_form + npages;
        if (dirty_pgno_form > spilled_pgno) {
          --d;
          continue;
        }
        if (dirty_pgno_to <= spilled_pgno) {
          s += step;
          continue;
        }

        DEBUG("remove dirtied parent's spilled %u page %" PRIaPGNO, npages, dirty_pgno_form);
        spill_remove(parent, s, 1);
        s += step;
      }

      /* Squash deleted pagenums if we deleted any */
      spill_purge(parent);
    }
  }

  /* Remove anything in our spill list from parent's dirty list */
  if (txn->wr.spilled.list) {
    tASSERT(txn, pnl_check_allocated(txn->wr.spilled.list, (size_t)parent->geo.first_unallocated << 1));
    dpl_sift(parent, txn->wr.spilled.list, true);
    tASSERT(parent, parent->wr.dirtyroom + parent->wr.dirtylist->length ==
                        (parent->parent ? parent->parent->wr.dirtyroom : parent->env->options.dp_limit));
  }

  /* Find length of merging our dirty list with parent's and release
   * filter-out pages */
  for (l = 0, d = dst->length, s = src->length; d > 0 && s > 0;) {
    page_t *sp = src->items[s].ptr;
    tASSERT(parent, (sp->flags & ~(P_LEAF | P_DUPFIX | P_BRANCH | P_LARGE | P_LOOSE | P_SPILLED)) == 0);
    const unsigned s_npages = dpl_npages(src, s);
    const pgno_t s_pgno = src->items[s].pgno;

    page_t *dp = dst->items[d].ptr;
    tASSERT(parent, (dp->flags & ~(P_LEAF | P_DUPFIX | P_BRANCH | P_LARGE | P_SPILLED)) == 0);
    const unsigned d_npages = dpl_npages(dst, d);
    const pgno_t d_pgno = dst->items[d].pgno;

    if (d_pgno >= s_pgno + s_npages) {
      --d;
      ++l;
    } else if (d_pgno + d_npages <= s_pgno) {
      if (sp->flags != P_LOOSE) {
        sp->txnid = parent->front_txnid;
        sp->flags &= ~P_SPILLED;
      }
      --s;
      ++l;
    } else {
      dst->items[d--].ptr = nullptr;
      page_shadow_release(txn->env, dp, d_npages);
    }
  }
  assert(dst->sorted == dst->length);
  tASSERT(parent, dst->detent >= l + d + s);
  dst->sorted = l + d + s; /* the merged length */

  while (s > 0) {
    page_t *sp = src->items[s].ptr;
    tASSERT(parent, (sp->flags & ~(P_LEAF | P_DUPFIX | P_BRANCH | P_LARGE | P_LOOSE | P_SPILLED)) == 0);
    if (sp->flags != P_LOOSE) {
      sp->txnid = parent->front_txnid;
      sp->flags &= ~P_SPILLED;
    }
    --s;
  }

  /* Merge our dirty list into parent's, i.e. merge(dst, src) -> dst */
  if (dst->sorted >= dst->length) {
    /* from end to begin with dst extending */
    for (l = dst->sorted, s = src->length, d = dst->length; s > 0 && d > 0;) {
      if (unlikely(l <= d)) {
        /* squash to get a gap of free space for merge */
        for (r = w = 1; r <= d; ++r)
          if (dst->items[r].ptr) {
            if (w != r) {
              dst->items[w] = dst->items[r];
              dst->items[r].ptr = nullptr;
            }
            ++w;
          }
        VERBOSE("squash to begin for extending-merge %zu -> %zu", d, w - 1);
        d = w - 1;
        continue;
      }
      assert(l > d);
      if (dst->items[d].ptr) {
        dst->items[l--] = (dst->items[d].pgno > src->items[s].pgno) ? dst->items[d--] : src->items[s--];
      } else
        --d;
    }
    if (s > 0) {
      assert(l == s);
      while (d > 0) {
        assert(dst->items[d].ptr == nullptr);
        --d;
      }
      do {
        assert(l > 0);
        dst->items[l--] = src->items[s--];
      } while (s > 0);
    } else {
      assert(l == d);
      while (l > 0) {
        assert(dst->items[l].ptr != nullptr);
        --l;
      }
    }
  } else {
    /* from begin to end with shrinking (a lot of new large/overflow pages) */
    for (l = s = d = 1; s <= src->length && d <= dst->length;) {
      if (unlikely(l >= d)) {
        /* squash to get a gap of free space for merge */
        for (r = w = dst->length; r >= d; --r)
          if (dst->items[r].ptr) {
            if (w != r) {
              dst->items[w] = dst->items[r];
              dst->items[r].ptr = nullptr;
            }
            --w;
          }
        VERBOSE("squash to end for shrinking-merge %zu -> %zu", d, w + 1);
        d = w + 1;
        continue;
      }
      assert(l < d);
      if (dst->items[d].ptr) {
        dst->items[l++] = (dst->items[d].pgno < src->items[s].pgno) ? dst->items[d++] : src->items[s++];
      } else
        ++d;
    }
    if (s <= src->length) {
      assert(dst->sorted - l == src->length - s);
      while (d <= dst->length) {
        assert(dst->items[d].ptr == nullptr);
        --d;
      }
      do {
        assert(l <= dst->sorted);
        dst->items[l++] = src->items[s++];
      } while (s <= src->length);
    } else {
      assert(dst->sorted - l == dst->length - d);
      while (l <= dst->sorted) {
        assert(l <= d && d <= dst->length && dst->items[d].ptr);
        dst->items[l++] = dst->items[d++];
      }
    }
  }
  parent->wr.dirtyroom -= dst->sorted - dst->length;
  assert(parent->wr.dirtyroom <= parent->env->options.dp_limit);
  dpl_setlen(dst, dst->sorted);
  parent->wr.dirtylru = txn->wr.dirtylru;

  /* В текущем понимании выгоднее пересчитать кол-во страниц,
   * чем подмешивать лишние ветвления и вычисления в циклы выше. */
  dst->pages_including_loose = 0;
  for (r = 1; r <= dst->length; ++r)
    dst->pages_including_loose += dpl_npages(dst, r);

  tASSERT(parent, dpl_check(parent));
  dpl_free(txn);

  if (txn->wr.spilled.list) {
    if (parent->wr.spilled.list) {
      /* Must not fail since space was preserved above. */
      pnl_merge(parent->wr.spilled.list, txn->wr.spilled.list);
      pnl_free(txn->wr.spilled.list);
    } else {
      parent->wr.spilled.list = txn->wr.spilled.list;
      parent->wr.spilled.least_removed = txn->wr.spilled.least_removed;
    }
    tASSERT(parent, dpl_check(parent));
  }

  parent->flags &= ~MDBX_TXN_HAS_CHILD;
  if (parent->wr.spilled.list) {
    assert(pnl_check_allocated(parent->wr.spilled.list, (size_t)parent->geo.first_unallocated << 1));
    if (MDBX_PNL_GETSIZE(parent->wr.spilled.list))
      parent->flags |= MDBX_TXN_SPILLS;
  }
}

int txn_nested_create(MDBX_txn *parent, const MDBX_txn_flags_t flags) {
  if (parent->env->options.spill_parent4child_denominator) {
    /* Spill dirty-pages of parent to provide dirtyroom for child txn */
    int err =
        txn_spill(parent, nullptr, parent->wr.dirtylist->length / parent->env->options.spill_parent4child_denominator);
    if (unlikely(err != MDBX_SUCCESS))
      return LOG_IFERR(err);
  }
  tASSERT(parent, audit_ex(parent, 0, false) == 0);

  MDBX_txn *const txn = txn_alloc(flags, parent->env);
  if (unlikely(!txn))
    return LOG_IFERR(MDBX_ENOMEM);

  tASSERT(parent, dpl_check(parent));
#if MDBX_ENABLE_DBI_SPARSE
  txn->dbi_sparse = parent->dbi_sparse;
#endif /* MDBX_ENABLE_DBI_SPARSE */
  txn->dbi_seqs = parent->dbi_seqs;
  txn->geo = parent->geo;
  int err = dpl_alloc(txn);
  if (likely(err == MDBX_SUCCESS)) {
    const size_t len = MDBX_PNL_GETSIZE(parent->wr.repnl) + parent->wr.loose_count;
    txn->wr.repnl = pnl_alloc((len > MDBX_PNL_INITIAL) ? len : MDBX_PNL_INITIAL);
    if (unlikely(!txn->wr.repnl))
      err = MDBX_ENOMEM;
  }
  if (unlikely(err != MDBX_SUCCESS)) {
  failed:
    pnl_free(txn->wr.repnl);
    dpl_free(txn);
    osal_free(txn);
    return LOG_IFERR(err);
  }

  /* Move loose pages to reclaimed list */
  if (parent->wr.loose_count) {
    do {
      page_t *lp = parent->wr.loose_pages;
      tASSERT(parent, lp->flags == P_LOOSE);
      err = pnl_insert_span(&parent->wr.repnl, lp->pgno, 1);
      if (unlikely(err != MDBX_SUCCESS))
        goto failed;
      MDBX_ASAN_UNPOISON_MEMORY_REGION(&page_next(lp), sizeof(page_t *));
      VALGRIND_MAKE_MEM_DEFINED(&page_next(lp), sizeof(page_t *));
      parent->wr.loose_pages = page_next(lp);
      /* Remove from dirty list */
      page_wash(parent, dpl_exist(parent, lp->pgno), lp, 1);
    } while (parent->wr.loose_pages);
    parent->wr.loose_count = 0;
#if MDBX_ENABLE_REFUND
    parent->wr.loose_refund_wl = 0;
#endif /* MDBX_ENABLE_REFUND */
    tASSERT(parent, dpl_check(parent));
  }
  txn->wr.dirtyroom = parent->wr.dirtyroom;
  txn->wr.dirtylru = parent->wr.dirtylru;

  dpl_sort(parent);
  if (parent->wr.spilled.list)
    spill_purge(parent);

  tASSERT(txn, MDBX_PNL_ALLOCLEN(txn->wr.repnl) >= MDBX_PNL_GETSIZE(parent->wr.repnl));
  memcpy(txn->wr.repnl, parent->wr.repnl, MDBX_PNL_SIZEOF(parent->wr.repnl));
  tASSERT(txn, pnl_check_allocated(txn->wr.repnl, (txn->geo.first_unallocated /* LY: intentional assignment
                                                                             here, only for assertion */
                                                   = parent->geo.first_unallocated) -
                                                      MDBX_ENABLE_REFUND));

  txn->wr.gc.time_acc = parent->wr.gc.time_acc;
  txn->wr.gc.last_reclaimed = parent->wr.gc.last_reclaimed;
  if (parent->wr.gc.retxl) {
    txn->wr.gc.retxl = parent->wr.gc.retxl;
    parent->wr.gc.retxl = (void *)(intptr_t)MDBX_PNL_GETSIZE(parent->wr.gc.retxl);
  }

  txn->wr.retired_pages = parent->wr.retired_pages;
  parent->wr.retired_pages = (void *)(intptr_t)MDBX_PNL_GETSIZE(parent->wr.retired_pages);

  txn->txnid = parent->txnid;
  txn->front_txnid = parent->front_txnid + 1;
#if MDBX_ENABLE_REFUND
  txn->wr.loose_refund_wl = 0;
#endif /* MDBX_ENABLE_REFUND */
  txn->canary = parent->canary;
  parent->flags |= MDBX_TXN_HAS_CHILD;
  parent->nested = txn;
  txn->parent = parent;
  txn->owner = parent->owner;
  txn->wr.troika = parent->wr.troika;

  txn->cursors[FREE_DBI] = nullptr;
  txn->cursors[MAIN_DBI] = nullptr;
  txn->dbi_state[FREE_DBI] = parent->dbi_state[FREE_DBI] & ~(DBI_FRESH | DBI_CREAT | DBI_DIRTY);
  txn->dbi_state[MAIN_DBI] = parent->dbi_state[MAIN_DBI] & ~(DBI_FRESH | DBI_CREAT | DBI_DIRTY);
  memset(txn->dbi_state + CORE_DBS, 0, (txn->n_dbi = parent->n_dbi) - CORE_DBS);
  memcpy(txn->dbs, parent->dbs, sizeof(txn->dbs[0]) * CORE_DBS);

  tASSERT(parent, parent->wr.dirtyroom + parent->wr.dirtylist->length ==
                      (parent->parent ? parent->parent->wr.dirtyroom : parent->env->options.dp_limit));
  tASSERT(txn, txn->wr.dirtyroom + txn->wr.dirtylist->length ==
                   (txn->parent ? txn->parent->wr.dirtyroom : txn->env->options.dp_limit));
  parent->env->txn = txn;
  tASSERT(parent, parent->cursors[FREE_DBI] == nullptr);
  return txn_shadow_cursors(parent, MAIN_DBI);
}

void txn_nested_abort(MDBX_txn *nested) {
  MDBX_txn *const parent = nested->parent;
  tASSERT(nested, !(nested->flags & txn_may_have_cursors));
  nested->signature = 0;
  nested->owner = 0;

  if (nested->wr.gc.retxl) {
    tASSERT(parent, MDBX_PNL_GETSIZE(nested->wr.gc.retxl) >= (uintptr_t)parent->wr.gc.retxl);
    MDBX_PNL_SETSIZE(nested->wr.gc.retxl, (uintptr_t)parent->wr.gc.retxl);
    parent->wr.gc.retxl = nested->wr.gc.retxl;
  }

  if (nested->wr.retired_pages) {
    tASSERT(parent, MDBX_PNL_GETSIZE(nested->wr.retired_pages) >= (uintptr_t)parent->wr.retired_pages);
    MDBX_PNL_SETSIZE(nested->wr.retired_pages, (uintptr_t)parent->wr.retired_pages);
    parent->wr.retired_pages = nested->wr.retired_pages;
  }

  parent->wr.dirtylru = nested->wr.dirtylru;
  parent->nested = nullptr;
  parent->flags &= ~MDBX_TXN_HAS_CHILD;
  tASSERT(parent, dpl_check(parent));
  tASSERT(parent, audit_ex(parent, 0, false) == 0);
  dpl_release_shadows(nested);
  dpl_free(nested);
  pnl_free(nested->wr.repnl);
  osal_free(nested);
}

int txn_nested_join(MDBX_txn *txn, struct commit_timestamp *ts) {
  MDBX_env *const env = txn->env;
  MDBX_txn *const parent = txn->parent;
  tASSERT(txn, audit_ex(txn, 0, false) == 0);
  eASSERT(env, txn != env->basal_txn);
  eASSERT(env, parent->signature == txn_signature);
  eASSERT(env, parent->nested == txn && (parent->flags & MDBX_TXN_HAS_CHILD) != 0);
  eASSERT(env, dpl_check(txn));

  if (txn->wr.dirtylist->length == 0 && !(txn->flags & MDBX_TXN_DIRTY) && parent->n_dbi == txn->n_dbi) {
    VERBOSE("fast-complete pure nested txn %" PRIaTXN, txn->txnid);

    tASSERT(txn, memcmp(&parent->geo, &txn->geo, sizeof(parent->geo)) == 0);
    tASSERT(txn, memcmp(&parent->canary, &txn->canary, sizeof(parent->canary)) == 0);
    tASSERT(txn, !txn->wr.spilled.list || MDBX_PNL_GETSIZE(txn->wr.spilled.list) == 0);
    tASSERT(txn, txn->wr.loose_count == 0);

    /* Update parent's DBs array */
    eASSERT(env, parent->n_dbi == txn->n_dbi);
    TXN_FOREACH_DBI_ALL(txn, dbi) {
      tASSERT(txn, (txn->dbi_state[dbi] & (DBI_CREAT | DBI_DIRTY)) == 0);
      if (txn->dbi_state[dbi] & DBI_FRESH) {
        parent->dbs[dbi] = txn->dbs[dbi];
        /* preserve parent's status */
        const uint8_t state = txn->dbi_state[dbi] | DBI_FRESH;
        DEBUG("dbi %zu dbi-state %s 0x%02x -> 0x%02x", dbi, (parent->dbi_state[dbi] != state) ? "update" : "still",
              parent->dbi_state[dbi], state);
        parent->dbi_state[dbi] = state;
      }
    }
    return txn_end(txn, TXN_END_PURE_COMMIT | TXN_END_SLOT | TXN_END_FREE);
  }

  /* Preserve space for spill list to avoid parent's state corruption
   * if allocation fails. */
  const size_t parent_retired_len = (uintptr_t)parent->wr.retired_pages;
  tASSERT(txn, parent_retired_len <= MDBX_PNL_GETSIZE(txn->wr.retired_pages));
  const size_t retired_delta = MDBX_PNL_GETSIZE(txn->wr.retired_pages) - parent_retired_len;
  if (retired_delta) {
    int err = pnl_need(&txn->wr.repnl, retired_delta);
    if (unlikely(err != MDBX_SUCCESS))
      return err;
  }

  if (txn->wr.spilled.list) {
    if (parent->wr.spilled.list) {
      int err = pnl_need(&parent->wr.spilled.list, MDBX_PNL_GETSIZE(txn->wr.spilled.list));
      if (unlikely(err != MDBX_SUCCESS))
        return err;
    }
    spill_purge(txn);
  }

  if (unlikely(txn->wr.dirtylist->length + parent->wr.dirtylist->length > parent->wr.dirtylist->detent &&
               !dpl_reserve(parent, txn->wr.dirtylist->length + parent->wr.dirtylist->length))) {
    return MDBX_ENOMEM;
  }

  //-------------------------------------------------------------------------

  parent->wr.gc.retxl = txn->wr.gc.retxl;
  txn->wr.gc.retxl = nullptr;

  parent->wr.retired_pages = txn->wr.retired_pages;
  txn->wr.retired_pages = nullptr;

  pnl_free(parent->wr.repnl);
  parent->wr.repnl = txn->wr.repnl;
  txn->wr.repnl = nullptr;
  parent->wr.gc.time_acc = txn->wr.gc.time_acc;
  parent->wr.gc.last_reclaimed = txn->wr.gc.last_reclaimed;

  parent->geo = txn->geo;
  parent->canary = txn->canary;
  parent->flags |= txn->flags & MDBX_TXN_DIRTY;

  /* Move loose pages to parent */
#if MDBX_ENABLE_REFUND
  parent->wr.loose_refund_wl = txn->wr.loose_refund_wl;
#endif /* MDBX_ENABLE_REFUND */
  parent->wr.loose_count = txn->wr.loose_count;
  parent->wr.loose_pages = txn->wr.loose_pages;

  if (txn->flags & txn_may_have_cursors)
    /* Merge our cursors into parent's and close them */
    txn_done_cursors(txn);

  /* Update parent's DBs array */
  eASSERT(env, parent->n_dbi == txn->n_dbi);
  TXN_FOREACH_DBI_ALL(txn, dbi) {
    if (txn->dbi_state[dbi] & (DBI_CREAT | DBI_FRESH | DBI_DIRTY)) {
      parent->dbs[dbi] = txn->dbs[dbi];
      /* preserve parent's status */
      const uint8_t state = txn->dbi_state[dbi] | (parent->dbi_state[dbi] & (DBI_CREAT | DBI_FRESH | DBI_DIRTY));
      DEBUG("dbi %zu dbi-state %s 0x%02x -> 0x%02x", dbi, (parent->dbi_state[dbi] != state) ? "update" : "still",
            parent->dbi_state[dbi], state);
      parent->dbi_state[dbi] = state;
    } else {
      eASSERT(env, txn->dbi_state[dbi] == (parent->dbi_state[dbi] & ~(DBI_FRESH | DBI_CREAT | DBI_DIRTY)));
    }
  }

  if (ts) {
    ts->prep = osal_monotime();
    ts->gc = /* no gc-update */ ts->prep;
    ts->audit = /* no audit */ ts->gc;
    ts->write = /* no write */ ts->audit;
    ts->sync = /* no sync */ ts->write;
  }
  txn_merge(parent, txn, parent_retired_len);
  env->txn = parent;
  parent->nested = nullptr;
  parent->flags &= ~MDBX_TXN_HAS_CHILD;
  tASSERT(parent, dpl_check(parent));

#if MDBX_ENABLE_REFUND
  txn_refund(parent);
  if (ASSERT_ENABLED()) {
    /* Check parent's loose pages not suitable for refund */
    for (page_t *lp = parent->wr.loose_pages; lp; lp = page_next(lp)) {
      tASSERT(parent, lp->pgno < parent->wr.loose_refund_wl && lp->pgno + 1 < parent->geo.first_unallocated);
      MDBX_ASAN_UNPOISON_MEMORY_REGION(&page_next(lp), sizeof(page_t *));
      VALGRIND_MAKE_MEM_DEFINED(&page_next(lp), sizeof(page_t *));
    }
    /* Check parent's reclaimed pages not suitable for refund */
    if (MDBX_PNL_GETSIZE(parent->wr.repnl))
      tASSERT(parent, MDBX_PNL_MOST(parent->wr.repnl) + 1 < parent->geo.first_unallocated);
  }
#endif /* MDBX_ENABLE_REFUND */

  txn->signature = 0;
  osal_free(txn);
  tASSERT(parent, audit_ex(parent, 0, false) == 0);
  return MDBX_SUCCESS;
}
