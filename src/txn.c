/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024

#include "internals.h"

__hot txnid_t txn_snapshot_oldest(const MDBX_txn *const txn) {
  return mvcc_shapshot_oldest(txn->env, txn->tw.troika.txnid[txn->tw.troika.prefer_steady]);
}

void txn_done_cursors(MDBX_txn *txn) {
  tASSERT(txn, txn->flags & txn_may_have_cursors);
  tASSERT(txn, txn->cursors[FREE_DBI] == nullptr);

  TXN_FOREACH_DBI_FROM(txn, i, /* skip FREE_DBI */ 1) {
    MDBX_cursor *cursor = txn->cursors[i];
    if (cursor) {
      txn->cursors[i] = nullptr;
      do {
        MDBX_cursor *const next = cursor->next;
        cursor_eot(cursor);
        cursor = next;
      } while (cursor);
    }
  }
  txn->flags &= ~txn_may_have_cursors;
}

int txn_shadow_cursors(const MDBX_txn *parent, const size_t dbi) {
  tASSERT(parent, dbi > FREE_DBI && dbi < parent->n_dbi);
  MDBX_cursor *cursor = parent->cursors[dbi];
  if (!cursor)
    return MDBX_SUCCESS;

  MDBX_txn *const txn = parent->nested;
  tASSERT(parent, parent->flags & txn_may_have_cursors);
  MDBX_cursor *next = nullptr;
  do {
    next = cursor->next;
    if (cursor->signature != cur_signature_live)
      continue;
    tASSERT(parent, cursor->txn == parent && dbi == cursor_dbi(cursor));

    int err = cursor_shadow(cursor, txn, dbi);
    if (unlikely(err != MDBX_SUCCESS)) {
      /* не получилось забекапить курсоры */
      txn->dbi_state[dbi] = DBI_OLDEN | DBI_LINDO | DBI_STALE;
      txn->flags |= MDBX_TXN_ERROR;
      return err;
    }
    cursor->next = txn->cursors[dbi];
    txn->cursors[dbi] = cursor;
    txn->flags |= txn_may_have_cursors;
  } while ((cursor = next) != nullptr);
  return MDBX_SUCCESS;
}

static int txn_write(MDBX_txn *txn, iov_ctx_t *ctx) {
  tASSERT(txn, (txn->flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);
  dpl_t *const dl = dpl_sort(txn);
  int rc = MDBX_SUCCESS;
  size_t r, w, total_npages = 0;
  for (w = 0, r = 1; r <= dl->length; ++r) {
    page_t *dp = dl->items[r].ptr;
    if (dp->flags & P_LOOSE) {
      dl->items[++w] = dl->items[r];
      continue;
    }
    unsigned npages = dpl_npages(dl, r);
    total_npages += npages;
    rc = iov_page(txn, ctx, dp, npages);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }

  if (!iov_empty(ctx)) {
    tASSERT(txn, rc == MDBX_SUCCESS);
    rc = iov_write(ctx);
  }

  if (likely(rc == MDBX_SUCCESS) && ctx->fd == txn->env->lazy_fd) {
    txn->env->lck->unsynced_pages.weak += total_npages;
    if (!txn->env->lck->eoos_timestamp.weak)
      txn->env->lck->eoos_timestamp.weak = osal_monotime();
  }

  txn->tw.dirtylist->pages_including_loose -= total_npages;
  while (r <= dl->length)
    dl->items[++w] = dl->items[r++];

  dl->sorted = dpl_setlen(dl, w);
  txn->tw.dirtyroom += r - 1 - w;
  tASSERT(txn, txn->tw.dirtyroom + txn->tw.dirtylist->length ==
                   (txn->parent ? txn->parent->tw.dirtyroom : txn->env->options.dp_limit));
  tASSERT(txn, txn->tw.dirtylist->length == txn->tw.loose_count);
  tASSERT(txn, txn->tw.dirtylist->pages_including_loose == txn->tw.loose_count);
  return rc;
}

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
    parent->tw.dirtyroom += dst->sorted - n;
    dst->sorted = dpl_setlen(dst, n);
    tASSERT(parent, parent->tw.dirtyroom + parent->tw.dirtylist->length ==
                        (parent->parent ? parent->parent->tw.dirtyroom : parent->env->options.dp_limit));
  }

  /* Remove reclaimed pages from parent's dirty list */
  const pnl_t reclaimed_list = parent->tw.repnl;
  dpl_sift(parent, reclaimed_list, false);

  /* Move retired pages from parent's dirty & spilled list to reclaimed */
  size_t r, w, d, s, l;
  for (r = w = parent_retired_len; ++r <= MDBX_PNL_GETSIZE(parent->tw.retired_pages);) {
    const pgno_t pgno = parent->tw.retired_pages[r];
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
        const size_t len = MDBX_PNL_GETSIZE(parent->tw.retired_pages);
        while (r < len && parent->tw.retired_pages[r + 1] == pgno + l) {
          ++r;
          if (++l == npages)
            break;
        }
#else
        while (w > parent_retired_len && parent->tw.retired_pages[w - 1] == pgno + l) {
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
      parent->tw.retired_pages[++w] = pgno;
      continue;
    }

    DEBUG("reclaim retired parent's %u -> %zu %s page %" PRIaPGNO, npages, l, kind, pgno);
    int err = pnl_insert_span(&parent->tw.repnl, pgno, l);
    ENSURE(txn->env, err == MDBX_SUCCESS);
  }
  MDBX_PNL_SETSIZE(parent->tw.retired_pages, w);

  /* Filter-out parent spill list */
  if (parent->tw.spilled.list && MDBX_PNL_GETSIZE(parent->tw.spilled.list) > 0) {
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
  if (txn->tw.spilled.list) {
    tASSERT(txn, pnl_check_allocated(txn->tw.spilled.list, (size_t)parent->geo.first_unallocated << 1));
    dpl_sift(parent, txn->tw.spilled.list, true);
    tASSERT(parent, parent->tw.dirtyroom + parent->tw.dirtylist->length ==
                        (parent->parent ? parent->parent->tw.dirtyroom : parent->env->options.dp_limit));
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
  parent->tw.dirtyroom -= dst->sorted - dst->length;
  assert(parent->tw.dirtyroom <= parent->env->options.dp_limit);
  dpl_setlen(dst, dst->sorted);
  parent->tw.dirtylru = txn->tw.dirtylru;

  /* В текущем понимании выгоднее пересчитать кол-во страниц,
   * чем подмешивать лишние ветвления и вычисления в циклы выше. */
  dst->pages_including_loose = 0;
  for (r = 1; r <= dst->length; ++r)
    dst->pages_including_loose += dpl_npages(dst, r);

  tASSERT(parent, dpl_check(parent));
  dpl_free(txn);

  if (txn->tw.spilled.list) {
    if (parent->tw.spilled.list) {
      /* Must not fail since space was preserved above. */
      pnl_merge(parent->tw.spilled.list, txn->tw.spilled.list);
      pnl_free(txn->tw.spilled.list);
    } else {
      parent->tw.spilled.list = txn->tw.spilled.list;
      parent->tw.spilled.least_removed = txn->tw.spilled.least_removed;
    }
    tASSERT(parent, dpl_check(parent));
  }

  parent->flags &= ~MDBX_TXN_HAS_CHILD;
  if (parent->tw.spilled.list) {
    assert(pnl_check_allocated(parent->tw.spilled.list, (size_t)parent->geo.first_unallocated << 1));
    if (MDBX_PNL_GETSIZE(parent->tw.spilled.list))
      parent->flags |= MDBX_TXN_SPILLS;
  }
}

int txn_abort(MDBX_txn *txn) {
  if (txn->flags & MDBX_TXN_RDONLY)
    /* LY: don't close DBI-handles */
    return txn_end(txn, TXN_END_ABORT | TXN_END_UPDATE | TXN_END_SLOT | TXN_END_FREE);

  if (unlikely(txn->flags & MDBX_TXN_FINISHED))
    return MDBX_BAD_TXN;

  if (txn->nested)
    txn_abort(txn->nested);

  tASSERT(txn, (txn->flags & MDBX_TXN_ERROR) || dpl_check(txn));
  return txn_end(txn, TXN_END_ABORT | TXN_END_SLOT | TXN_END_FREE);
}

int txn_renew(MDBX_txn *txn, unsigned flags) {
  MDBX_env *const env = txn->env;
  int rc;

  flags |= env->flags & (MDBX_NOSTICKYTHREADS | MDBX_WRITEMAP);
  if (flags & MDBX_TXN_RDONLY) {
    eASSERT(env, (flags & ~(txn_ro_begin_flags | MDBX_WRITEMAP | MDBX_NOSTICKYTHREADS)) == 0);
    txn->flags = flags;
    reader_slot_t *r = txn->to.reader;
    STATIC_ASSERT(sizeof(uintptr_t) <= sizeof(r->tid));
    if (likely(env->flags & ENV_TXKEY)) {
      eASSERT(env, !(env->flags & MDBX_NOSTICKYTHREADS));
      r = thread_rthc_get(env->me_txkey);
      if (likely(r)) {
        if (unlikely(!r->pid.weak) && (globals.runtime_flags & MDBX_DBG_LEGACY_MULTIOPEN)) {
          thread_rthc_set(env->me_txkey, nullptr);
          r = nullptr;
        } else {
          eASSERT(env, r->pid.weak == env->pid);
          eASSERT(env, r->tid.weak == osal_thread_self());
        }
      }
    } else {
      eASSERT(env, !env->lck_mmap.lck || (env->flags & MDBX_NOSTICKYTHREADS));
    }

    if (likely(r)) {
      if (unlikely(r->pid.weak != env->pid || r->txnid.weak < SAFE64_INVALID_THRESHOLD))
        return MDBX_BAD_RSLOT;
    } else if (env->lck_mmap.lck) {
      bsr_t brs = mvcc_bind_slot(env);
      if (unlikely(brs.err != MDBX_SUCCESS))
        return brs.err;
      r = brs.rslot;
    }
    txn->to.reader = r;
    STATIC_ASSERT(MDBX_TXN_RDONLY_PREPARE > MDBX_TXN_RDONLY);
    if (flags & (MDBX_TXN_RDONLY_PREPARE - MDBX_TXN_RDONLY)) {
      eASSERT(env, txn->txnid == 0);
      eASSERT(env, txn->owner == 0);
      eASSERT(env, txn->n_dbi == 0);
      if (likely(r)) {
        eASSERT(env, r->snapshot_pages_used.weak == 0);
        eASSERT(env, r->txnid.weak >= SAFE64_INVALID_THRESHOLD);
        atomic_store32(&r->snapshot_pages_used, 0, mo_Relaxed);
      }
      txn->flags = MDBX_TXN_RDONLY | MDBX_TXN_FINISHED;
      return MDBX_SUCCESS;
    }
    txn->owner = likely(r) ? (uintptr_t)r->tid.weak : ((env->flags & MDBX_NOSTICKYTHREADS) ? 0 : osal_thread_self());
    if ((env->flags & MDBX_NOSTICKYTHREADS) == 0 && env->txn && unlikely(env->basal_txn->owner == txn->owner) &&
        (globals.runtime_flags & MDBX_DBG_LEGACY_OVERLAP) == 0)
      return MDBX_TXN_OVERLAPPING;

    /* Seek & fetch the last meta */
    uint64_t timestamp = 0;
    size_t loop = 0;
    troika_t troika = meta_tap(env);
    while (1) {
      const meta_ptr_t head = likely(env->stuck_meta < 0) ? /* regular */ meta_recent(env, &troika)
                                                          : /* recovery mode */ meta_ptr(env, env->stuck_meta);
      if (likely(r != nullptr)) {
        safe64_reset(&r->txnid, true);
        atomic_store32(&r->snapshot_pages_used, head.ptr_v->geometry.first_unallocated, mo_Relaxed);
        atomic_store64(&r->snapshot_pages_retired, unaligned_peek_u64_volatile(4, head.ptr_v->pages_retired),
                       mo_Relaxed);
        safe64_write(&r->txnid, head.txnid);
        eASSERT(env, r->pid.weak == osal_getpid());
        eASSERT(env, r->tid.weak == ((env->flags & MDBX_NOSTICKYTHREADS) ? 0 : osal_thread_self()));
        eASSERT(env, r->txnid.weak == head.txnid ||
                         (r->txnid.weak >= SAFE64_INVALID_THRESHOLD && head.txnid < env->lck->cached_oldest.weak));
        atomic_store32(&env->lck->rdt_refresh_flag, true, mo_AcquireRelease);
      } else {
        /* exclusive mode without lck */
        eASSERT(env, !env->lck_mmap.lck && env->lck == lckless_stub(env));
      }
      jitter4testing(true);

      if (unlikely(meta_should_retry(env, &troika))) {
      retry:
        if (likely(++loop < 42)) {
          timestamp = 0;
          continue;
        }
        ERROR("bailout waiting for valid snapshot (%s)", "meta-pages are too volatile");
        rc = MDBX_PROBLEM;
        goto read_failed;
      }

      /* Snap the state from current meta-head */
      rc = coherency_fetch_head(txn, head, &timestamp);
      jitter4testing(false);
      if (unlikely(rc != MDBX_SUCCESS)) {
        if (rc == MDBX_RESULT_TRUE)
          goto retry;
        else
          goto read_failed;
      }

      const uint64_t snap_oldest = atomic_load64(&env->lck->cached_oldest, mo_AcquireRelease);
      if (unlikely(txn->txnid < snap_oldest)) {
        if (env->stuck_meta < 0)
          goto retry;
        ERROR("target meta-page %i is referenced to an obsolete MVCC-snapshot "
              "%" PRIaTXN " < cached-oldest %" PRIaTXN,
              env->stuck_meta, txn->txnid, snap_oldest);
        rc = MDBX_MVCC_RETARDED;
        goto read_failed;
      }

      if (likely(r != nullptr) && unlikely(txn->txnid != atomic_load64(&r->txnid, mo_Relaxed)))
        goto retry;
      break;
    }

    if (unlikely(txn->txnid < MIN_TXNID || txn->txnid > MAX_TXNID)) {
      ERROR("%s", "environment corrupted by died writer, must shutdown!");
      rc = MDBX_CORRUPTED;
    read_failed:
      txn->txnid = INVALID_TXNID;
      if (likely(r != nullptr))
        safe64_reset(&r->txnid, true);
      goto bailout;
    }

    tASSERT(txn, rc == MDBX_SUCCESS);
    ENSURE(env, txn->txnid >=
                    /* paranoia is appropriate here */ env->lck->cached_oldest.weak);
    tASSERT(txn, txn->dbs[FREE_DBI].flags == MDBX_INTEGERKEY);
    tASSERT(txn, check_table_flags(txn->dbs[MAIN_DBI].flags));
  } else {
    eASSERT(env, (flags & ~(txn_rw_begin_flags | MDBX_TXN_SPILLS | MDBX_WRITEMAP | MDBX_NOSTICKYTHREADS)) == 0);
    const uintptr_t tid = osal_thread_self();
    if (unlikely(txn->owner == tid ||
                 /* not recovery mode */ env->stuck_meta >= 0))
      return MDBX_BUSY;
    lck_t *const lck = env->lck_mmap.lck;
    if (lck && (env->flags & MDBX_NOSTICKYTHREADS) == 0 && (globals.runtime_flags & MDBX_DBG_LEGACY_OVERLAP) == 0) {
      const size_t snap_nreaders = atomic_load32(&lck->rdt_length, mo_AcquireRelease);
      for (size_t i = 0; i < snap_nreaders; ++i) {
        if (atomic_load32(&lck->rdt[i].pid, mo_Relaxed) == env->pid &&
            unlikely(atomic_load64(&lck->rdt[i].tid, mo_Relaxed) == tid)) {
          const txnid_t txnid = safe64_read(&lck->rdt[i].txnid);
          if (txnid >= MIN_TXNID && txnid <= MAX_TXNID)
            return MDBX_TXN_OVERLAPPING;
        }
      }
    }

    /* Not yet touching txn == env->basal_txn, it may be active */
    jitter4testing(false);
    rc = lck_txn_lock(env, !!(flags & MDBX_TXN_TRY));
    if (unlikely(rc))
      return rc;
    if (unlikely(env->flags & ENV_FATAL_ERROR)) {
      lck_txn_unlock(env);
      return MDBX_PANIC;
    }
#if defined(_WIN32) || defined(_WIN64)
    if (unlikely(!env->dxb_mmap.base)) {
      lck_txn_unlock(env);
      return MDBX_EPERM;
    }
#endif /* Windows */

    txn->tw.troika = meta_tap(env);
    const meta_ptr_t head = meta_recent(env, &txn->tw.troika);
    uint64_t timestamp = 0;
    while ("workaround for https://libmdbx.dqdkfa.ru/dead-github/issues/269") {
      rc = coherency_fetch_head(txn, head, &timestamp);
      if (likely(rc == MDBX_SUCCESS))
        break;
      if (unlikely(rc != MDBX_RESULT_TRUE))
        goto bailout;
    }
    eASSERT(env, meta_txnid(head.ptr_v) == txn->txnid);
    txn->txnid = safe64_txnid_next(txn->txnid);
    if (unlikely(txn->txnid > MAX_TXNID)) {
      rc = MDBX_TXN_FULL;
      ERROR("txnid overflow, raise %d", rc);
      goto bailout;
    }

    tASSERT(txn, txn->dbs[FREE_DBI].flags == MDBX_INTEGERKEY);
    tASSERT(txn, check_table_flags(txn->dbs[MAIN_DBI].flags));
    txn->flags = flags;
    txn->nested = nullptr;
    txn->tw.loose_pages = nullptr;
    txn->tw.loose_count = 0;
#if MDBX_ENABLE_REFUND
    txn->tw.loose_refund_wl = 0;
#endif /* MDBX_ENABLE_REFUND */
    MDBX_PNL_SETSIZE(txn->tw.retired_pages, 0);
    txn->tw.spilled.list = nullptr;
    txn->tw.spilled.least_removed = 0;
    txn->tw.gc.time_acc = 0;
    txn->tw.gc.last_reclaimed = 0;
    if (txn->tw.gc.retxl)
      MDBX_PNL_SETSIZE(txn->tw.gc.retxl, 0);
    env->txn = txn;
  }

  txn->front_txnid = txn->txnid + ((flags & (MDBX_WRITEMAP | MDBX_RDONLY)) == 0);

  /* Setup db info */
  tASSERT(txn, txn->dbs[FREE_DBI].flags == MDBX_INTEGERKEY);
  tASSERT(txn, check_table_flags(txn->dbs[MAIN_DBI].flags));
  VALGRIND_MAKE_MEM_UNDEFINED(txn->dbi_state, env->max_dbi);
#if MDBX_ENABLE_DBI_SPARSE
  txn->n_dbi = CORE_DBS;
  VALGRIND_MAKE_MEM_UNDEFINED(txn->dbi_sparse,
                              ceil_powerof2(env->max_dbi, CHAR_BIT * sizeof(txn->dbi_sparse[0])) / CHAR_BIT);
  txn->dbi_sparse[0] = (1 << CORE_DBS) - 1;
#else
  txn->n_dbi = (env->n_dbi < 8) ? env->n_dbi : 8;
  if (txn->n_dbi > CORE_DBS)
    memset(txn->dbi_state + CORE_DBS, 0, txn->n_dbi - CORE_DBS);
#endif /* MDBX_ENABLE_DBI_SPARSE */
  txn->dbi_state[FREE_DBI] = DBI_LINDO | DBI_VALID;
  txn->dbi_state[MAIN_DBI] = DBI_LINDO | DBI_VALID;
  txn->cursors[FREE_DBI] = nullptr;
  txn->cursors[MAIN_DBI] = nullptr;
  txn->dbi_seqs[FREE_DBI] = 0;
  txn->dbi_seqs[MAIN_DBI] = atomic_load32(&env->dbi_seqs[MAIN_DBI], mo_AcquireRelease);

  if (unlikely(env->dbs_flags[MAIN_DBI] != (DB_VALID | txn->dbs[MAIN_DBI].flags))) {
    const bool need_txn_lock = env->basal_txn && env->basal_txn->owner != osal_thread_self();
    bool should_unlock = false;
    if (need_txn_lock) {
      rc = lck_txn_lock(env, true);
      if (rc == MDBX_SUCCESS)
        should_unlock = true;
      else if (rc != MDBX_BUSY && rc != MDBX_EDEADLK)
        goto bailout;
    }
    rc = osal_fastmutex_acquire(&env->dbi_lock);
    if (likely(rc == MDBX_SUCCESS)) {
      uint32_t seq = dbi_seq_next(env, MAIN_DBI);
      /* проверяем повторно после захвата блокировки */
      if (env->dbs_flags[MAIN_DBI] != (DB_VALID | txn->dbs[MAIN_DBI].flags)) {
        if (!need_txn_lock || should_unlock ||
            /* если нет активной пишущей транзакции,
             * то следующая будет ждать на dbi_lock */
            !env->txn) {
          if (env->dbs_flags[MAIN_DBI] != 0 || MDBX_DEBUG)
            NOTICE("renew MainDB for %s-txn %" PRIaTXN " since db-flags changes 0x%x -> 0x%x",
                   (txn->flags & MDBX_TXN_RDONLY) ? "ro" : "rw", txn->txnid, env->dbs_flags[MAIN_DBI] & ~DB_VALID,
                   txn->dbs[MAIN_DBI].flags);
          env->dbs_flags[MAIN_DBI] = DB_POISON;
          atomic_store32(&env->dbi_seqs[MAIN_DBI], seq, mo_AcquireRelease);
          rc = tbl_setup(env, &env->kvs[MAIN_DBI], &txn->dbs[MAIN_DBI]);
          if (likely(rc == MDBX_SUCCESS)) {
            seq = dbi_seq_next(env, MAIN_DBI);
            env->dbs_flags[MAIN_DBI] = DB_VALID | txn->dbs[MAIN_DBI].flags;
            txn->dbi_seqs[MAIN_DBI] = atomic_store32(&env->dbi_seqs[MAIN_DBI], seq, mo_AcquireRelease);
          }
        } else {
          ERROR("MainDB db-flags changes 0x%x -> 0x%x ahead of read-txn "
                "%" PRIaTXN,
                txn->dbs[MAIN_DBI].flags, env->dbs_flags[MAIN_DBI] & ~DB_VALID, txn->txnid);
          rc = MDBX_INCOMPATIBLE;
        }
      }
      ENSURE(env, osal_fastmutex_release(&env->dbi_lock) == MDBX_SUCCESS);
    } else {
      DEBUG("dbi_lock failed, err %d", rc);
    }
    if (should_unlock)
      lck_txn_unlock(env);
    if (unlikely(rc != MDBX_SUCCESS))
      goto bailout;
  }

  if (unlikely(txn->dbs[FREE_DBI].flags != MDBX_INTEGERKEY)) {
    ERROR("unexpected/invalid db-flags 0x%x for %s", txn->dbs[FREE_DBI].flags, "GC/FreeDB");
    rc = MDBX_INCOMPATIBLE;
    goto bailout;
  }

  tASSERT(txn, txn->dbs[FREE_DBI].flags == MDBX_INTEGERKEY);
  tASSERT(txn, check_table_flags(txn->dbs[MAIN_DBI].flags));
  if (unlikely(env->flags & ENV_FATAL_ERROR)) {
    WARNING("%s", "environment had fatal error, must shutdown!");
    rc = MDBX_PANIC;
  } else {
    const size_t size_bytes = pgno2bytes(env, txn->geo.end_pgno);
    const size_t used_bytes = pgno2bytes(env, txn->geo.first_unallocated);
    const size_t required_bytes = (txn->flags & MDBX_TXN_RDONLY) ? used_bytes : size_bytes;
    eASSERT(env, env->dxb_mmap.limit >= env->dxb_mmap.current);
    if (unlikely(required_bytes > env->dxb_mmap.current)) {
      /* Размер БД (для пишущих транзакций) или используемых данных (для
       * читающих транзакций) больше предыдущего/текущего размера внутри
       * процесса, увеличиваем. Сюда также попадает случай увеличения верхней
       * границы размера БД и отображения. В читающих транзакциях нельзя
       * изменять размер файла, который может быть больше необходимого этой
       * транзакции. */
      if (txn->geo.upper > MAX_PAGENO + 1 || bytes2pgno(env, pgno2bytes(env, txn->geo.upper)) != txn->geo.upper) {
        rc = MDBX_UNABLE_EXTEND_MAPSIZE;
        goto bailout;
      }
      rc = dxb_resize(env, txn->geo.first_unallocated, txn->geo.end_pgno, txn->geo.upper, implicit_grow);
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;
      eASSERT(env, env->dxb_mmap.limit >= env->dxb_mmap.current);
    } else if (unlikely(size_bytes < env->dxb_mmap.current)) {
      /* Размер БД меньше предыдущего/текущего размера внутри процесса, можно
       * уменьшить, но всё сложнее:
       *  - размер файла согласован со всеми читаемыми снимками на момент
       *    коммита последней транзакции;
       *  - в читающей транзакции размер файла может быть больше и него нельзя
       *    изменять, в том числе менять madvise (меньша размера файла нельзя,
       *    а за размером нет смысла).
       *  - в пишущей транзакции уменьшать размер файла можно только после
       *    проверки размера читаемых снимков, но в этом нет смысла, так как
       *    это будет сделано при фиксации транзакции.
       *
       *  В сухом остатке, можно только установить dxb_mmap.current равным
       *  размеру файла, а это проще сделать без вызова dxb_resize() и усложения
       *  внутренней логики.
       *
       *  В этой тактике есть недостаток: если пишущите транзакции не регулярны,
       *  и при завершении такой транзакции файл БД остаётся не-уменьшеным из-за
       *  читающих транзакций использующих предыдущие снимки. */
#if defined(_WIN32) || defined(_WIN64)
      imports.srwl_AcquireShared(&env->remap_guard);
#else
      rc = osal_fastmutex_acquire(&env->remap_guard);
#endif
      if (likely(rc == MDBX_SUCCESS)) {
        eASSERT(env, env->dxb_mmap.limit >= env->dxb_mmap.current);
        rc = osal_filesize(env->dxb_mmap.fd, &env->dxb_mmap.filesize);
        if (likely(rc == MDBX_SUCCESS)) {
          eASSERT(env, env->dxb_mmap.filesize >= required_bytes);
          if (env->dxb_mmap.current > env->dxb_mmap.filesize)
            env->dxb_mmap.current =
                (env->dxb_mmap.limit < env->dxb_mmap.filesize) ? env->dxb_mmap.limit : (size_t)env->dxb_mmap.filesize;
        }
#if defined(_WIN32) || defined(_WIN64)
        imports.srwl_ReleaseShared(&env->remap_guard);
#else
        int err = osal_fastmutex_release(&env->remap_guard);
        if (unlikely(err) && likely(rc == MDBX_SUCCESS))
          rc = err;
#endif
      }
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;
    }
    eASSERT(env, pgno2bytes(env, txn->geo.first_unallocated) <= env->dxb_mmap.current);
    eASSERT(env, env->dxb_mmap.limit >= env->dxb_mmap.current);
    if (txn->flags & MDBX_TXN_RDONLY) {
#if defined(_WIN32) || defined(_WIN64)
      if (((used_bytes > env->geo_in_bytes.lower && env->geo_in_bytes.shrink) ||
           (globals.running_under_Wine &&
            /* under Wine acquisition of remap_guard is always required,
             * since Wine don't support section extending,
             * i.e. in both cases unmap+map are required. */
            used_bytes < env->geo_in_bytes.upper && env->geo_in_bytes.grow)) &&
          /* avoid recursive use SRW */ (txn->flags & MDBX_NOSTICKYTHREADS) == 0) {
        txn->flags |= txn_shrink_allowed;
        imports.srwl_AcquireShared(&env->remap_guard);
      }
#endif /* Windows */
    } else {
      tASSERT(txn, txn == env->basal_txn);

      if (env->options.need_dp_limit_adjust)
        env_options_adjust_dp_limit(env);
      if ((txn->flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC) {
        rc = dpl_alloc(txn);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
        txn->tw.dirtyroom = txn->env->options.dp_limit;
        txn->tw.dirtylru = MDBX_DEBUG ? UINT32_MAX / 3 - 42 : 0;
      } else {
        tASSERT(txn, txn->tw.dirtylist == nullptr);
        txn->tw.dirtylist = nullptr;
        txn->tw.dirtyroom = MAX_PAGENO;
        txn->tw.dirtylru = 0;
      }
      eASSERT(env, txn->tw.writemap_dirty_npages == 0);
      eASSERT(env, txn->tw.writemap_spilled_npages == 0);

      MDBX_cursor *const gc = ptr_disp(txn, sizeof(MDBX_txn));
      rc = cursor_init(gc, txn, FREE_DBI);
      if (rc != MDBX_SUCCESS)
        goto bailout;
    }
    dxb_sanitize_tail(env, txn);
    return MDBX_SUCCESS;
  }
bailout:
  tASSERT(txn, rc != MDBX_SUCCESS);
  txn_end(txn, TXN_END_SLOT | TXN_END_FAIL_BEGIN);
  return rc;
}

static int txn_ro_end(MDBX_txn *txn, unsigned mode) {
  MDBX_env *const env = txn->env;
  txn->n_dbi = 0; /* prevent further DBI activity */
  if (txn->to.reader) {
    reader_slot_t *slot = txn->to.reader;
    if (unlikely(!env->lck))
      txn->to.reader = nullptr;
    else {
      eASSERT(env, slot->pid.weak == env->pid);
      if (likely((txn->flags & MDBX_TXN_FINISHED) == 0)) {
        if (likely((txn->flags & MDBX_TXN_PARKED) == 0)) {
          ENSURE(env, txn->txnid >=
                          /* paranoia is appropriate here */ env->lck->cached_oldest.weak);
          eASSERT(env, txn->txnid == slot->txnid.weak && slot->txnid.weak >= env->lck->cached_oldest.weak);
        } else {
          if ((mode & TXN_END_OPMASK) != TXN_END_OUSTED && safe64_read(&slot->tid) == MDBX_TID_TXN_OUSTED)
            mode = (mode & ~TXN_END_OPMASK) | TXN_END_OUSTED;
          do {
            safe64_reset(&slot->txnid, false);
            atomic_store64(&slot->tid, txn->owner, mo_AcquireRelease);
            atomic_yield();
          } while (
              unlikely(safe64_read(&slot->txnid) < SAFE64_INVALID_THRESHOLD || safe64_read(&slot->tid) != txn->owner));
        }
        dxb_sanitize_tail(env, nullptr);
        atomic_store32(&slot->snapshot_pages_used, 0, mo_Relaxed);
        safe64_reset(&slot->txnid, true);
        atomic_store32(&env->lck->rdt_refresh_flag, true, mo_Relaxed);
      } else {
        eASSERT(env, slot->pid.weak == env->pid);
        eASSERT(env, slot->txnid.weak >= SAFE64_INVALID_THRESHOLD);
      }
      if (mode & TXN_END_SLOT) {
        if ((env->flags & ENV_TXKEY) == 0)
          atomic_store32(&slot->pid, 0, mo_Relaxed);
        txn->to.reader = nullptr;
      }
    }
  }
#if defined(_WIN32) || defined(_WIN64)
  if (txn->flags & txn_shrink_allowed)
    imports.srwl_ReleaseShared(&env->remap_guard);
#endif
  txn->flags = ((mode & TXN_END_OPMASK) != TXN_END_OUSTED) ? MDBX_TXN_RDONLY | MDBX_TXN_FINISHED
                                                           : MDBX_TXN_RDONLY | MDBX_TXN_FINISHED | MDBX_TXN_OUSTED;
  txn->owner = 0;
  if (mode & TXN_END_FREE) {
    txn->signature = 0;
    osal_free(txn);
  }
  return MDBX_SUCCESS;
}

int txn_end(MDBX_txn *txn, unsigned mode) {
  static const char *const names[] = TXN_END_NAMES;
  DEBUG("%s txn %" PRIaTXN "%c-0x%X %p  on env %p, root page %" PRIaPGNO "/%" PRIaPGNO, names[mode & TXN_END_OPMASK],
        txn->txnid, (txn->flags & MDBX_TXN_RDONLY) ? 'r' : 'w', txn->flags, (void *)txn, (void *)txn->env,
        txn->dbs[MAIN_DBI].root, txn->dbs[FREE_DBI].root);

  tASSERT(txn, txn->signature == txn_signature && !txn->nested && !(txn->flags & MDBX_TXN_HAS_CHILD));
  if (txn->flags & txn_may_have_cursors) {
    txn->flags |= /* avoid merge cursors' state */ MDBX_TXN_ERROR;
    txn_done_cursors(txn);
  }

  MDBX_env *const env = txn->env;
  MDBX_txn *const parent = txn->parent;
  if (txn == env->basal_txn) {
    tASSERT(txn, !parent && !(txn->flags & MDBX_TXN_RDONLY));
    tASSERT(txn, (txn->flags & MDBX_TXN_FINISHED) == 0 && txn->owner);
    if (unlikely(txn->flags & MDBX_TXN_FINISHED))
      return MDBX_SUCCESS;

    ENSURE(env, txn->txnid >= /* paranoia is appropriate here */ env->lck->cached_oldest.weak);
    dxb_sanitize_tail(env, nullptr);

    txn->flags = MDBX_TXN_FINISHED;
    env->txn = nullptr;
    pnl_free(txn->tw.spilled.list);
    txn->tw.spilled.list = nullptr;

    eASSERT(env, txn->parent == nullptr);
    pnl_shrink(&txn->tw.retired_pages);
    pnl_shrink(&txn->tw.repnl);
    if (!(env->flags & MDBX_WRITEMAP))
      dpl_release_shadows(txn);

    /* Export or close DBI handles created in this txn */
    int err = dbi_update(txn, (mode & TXN_END_UPDATE) != 0);
    if (unlikely(err != MDBX_SUCCESS)) {
      ERROR("unexpected error %d during export the state of dbi-handles to env", err);
      err = MDBX_PROBLEM;
    }

    /* The writer mutex was locked in mdbx_txn_begin. */
    lck_txn_unlock(env);
    return err;
  }

  if (txn->flags & MDBX_TXN_RDONLY) {
    tASSERT(txn, txn != env->txn && !parent);
    return txn_ro_end(txn, mode);
  }

  if (unlikely(!parent || txn != env->txn || parent->signature != txn_signature || parent->nested != txn ||
               !(parent->flags & MDBX_TXN_HAS_CHILD) || txn == env->basal_txn)) {
    ERROR("parent txn %p is invalid or mismatch for nested txn %p", (void *)parent, (void *)txn);
    return MDBX_PROBLEM;
  }
  tASSERT(txn, pnl_check_allocated(txn->tw.repnl, txn->geo.first_unallocated - MDBX_ENABLE_REFUND));
  tASSERT(txn, memcmp(&txn->tw.troika, &parent->tw.troika, sizeof(troika_t)) == 0);
  tASSERT(txn, mode & TXN_END_FREE);
  env->txn = parent;
  const pgno_t nested_now = txn->geo.now, nested_upper = txn->geo.upper;
  txn_nested_abort(txn);

  if (unlikely(parent->geo.upper != nested_upper || parent->geo.now != nested_now) &&
      !(parent->flags & MDBX_TXN_ERROR) && !(env->flags & ENV_FATAL_ERROR)) {
    /* undo resize performed by nested txn */
    int err = dxb_resize(env, parent->geo.first_unallocated, parent->geo.now, parent->geo.upper, impilict_shrink);
    if (err == MDBX_EPERM) {
      /* unable undo resize (it is regular for Windows),
       * therefore promote size changes from nested to the parent txn */
      WARNING("unable undo resize performed by nested txn, promote to "
              "the parent (%u->%u, %u->%u)",
              nested_now, parent->geo.now, nested_upper, parent->geo.upper);
      parent->geo.now = nested_now;
      parent->flags |= MDBX_TXN_DIRTY;
    } else if (unlikely(err != MDBX_SUCCESS)) {
      ERROR("error %d while undo resize performed by nested txn, fail the parent", err);
      mdbx_txn_break(env->basal_txn);
      parent->flags |= MDBX_TXN_ERROR;
      if (!env->dxb_mmap.base)
        env->flags |= ENV_FATAL_ERROR;
      return err;
    }
  }
  return MDBX_SUCCESS;
}

int txn_check_badbits_parked(const MDBX_txn *txn, int bad_bits) {
  tASSERT(txn, (bad_bits & MDBX_TXN_PARKED) && (txn->flags & bad_bits));
  /* Здесь осознано заложено отличие в поведении припаркованных транзакций:
   *  - некоторые функции (например mdbx_env_info_ex()), допускают
   *    использование поломанных транзакций (с флагом MDBX_TXN_ERROR), но
   *    не могут работать с припаркованными транзакциями (требуют распарковки).
   *  - но при распарковке поломанные транзакции завершаются.
   *  - получается что транзакцию можно припарковать, потом поломать вызвав
   *    mdbx_txn_break(), но далее любое её использование приведет к завершению
   *    при распарковке. */
  if ((txn->flags & (bad_bits | MDBX_TXN_AUTOUNPARK)) != (MDBX_TXN_PARKED | MDBX_TXN_AUTOUNPARK))
    return LOG_IFERR(MDBX_BAD_TXN);

  tASSERT(txn, bad_bits == MDBX_TXN_BLOCKED || bad_bits == MDBX_TXN_BLOCKED - MDBX_TXN_ERROR);
  return mdbx_txn_unpark((MDBX_txn *)txn, false);
}

int txn_park(MDBX_txn *txn, bool autounpark) {
  reader_slot_t *const rslot = txn->to.reader;
  tASSERT(txn, (txn->flags & (MDBX_TXN_FINISHED | MDBX_TXN_RDONLY | MDBX_TXN_PARKED)) == MDBX_TXN_RDONLY);
  tASSERT(txn, txn->to.reader->tid.weak < MDBX_TID_TXN_OUSTED);
  if (unlikely((txn->flags & (MDBX_TXN_FINISHED | MDBX_TXN_RDONLY | MDBX_TXN_PARKED)) != MDBX_TXN_RDONLY))
    return MDBX_BAD_TXN;

  const uint32_t pid = atomic_load32(&rslot->pid, mo_Relaxed);
  const uint64_t tid = atomic_load64(&rslot->tid, mo_Relaxed);
  const uint64_t txnid = atomic_load64(&rslot->txnid, mo_Relaxed);
  if (unlikely(pid != txn->env->pid)) {
    ERROR("unexpected pid %u%s%u", pid, " != must ", txn->env->pid);
    return MDBX_PROBLEM;
  }
  if (unlikely(tid != txn->owner || txnid != txn->txnid)) {
    ERROR("unexpected thread-id 0x%" PRIx64 "%s0x%0zx"
          " and/or txn-id %" PRIaTXN "%s%" PRIaTXN,
          tid, " != must ", txn->owner, txnid, " != must ", txn->txnid);
    return MDBX_BAD_RSLOT;
  }

  atomic_store64(&rslot->tid, MDBX_TID_TXN_PARKED, mo_AcquireRelease);
  atomic_store32(&txn->env->lck->rdt_refresh_flag, true, mo_Relaxed);
  txn->flags += autounpark ? MDBX_TXN_PARKED | MDBX_TXN_AUTOUNPARK : MDBX_TXN_PARKED;
  return MDBX_SUCCESS;
}

int txn_unpark(MDBX_txn *txn) {
  if (unlikely((txn->flags & (MDBX_TXN_FINISHED | MDBX_TXN_HAS_CHILD | MDBX_TXN_RDONLY | MDBX_TXN_PARKED)) !=
               (MDBX_TXN_RDONLY | MDBX_TXN_PARKED)))
    return MDBX_BAD_TXN;

  for (reader_slot_t *const rslot = txn->to.reader; rslot; atomic_yield()) {
    const uint32_t pid = atomic_load32(&rslot->pid, mo_Relaxed);
    uint64_t tid = safe64_read(&rslot->tid);
    uint64_t txnid = safe64_read(&rslot->txnid);
    if (unlikely(pid != txn->env->pid)) {
      ERROR("unexpected pid %u%s%u", pid, " != expected ", txn->env->pid);
      return MDBX_PROBLEM;
    }
    if (unlikely(tid == MDBX_TID_TXN_OUSTED || txnid >= SAFE64_INVALID_THRESHOLD))
      break;
    if (unlikely(tid != MDBX_TID_TXN_PARKED || txnid != txn->txnid)) {
      ERROR("unexpected thread-id 0x%" PRIx64 "%s0x%" PRIx64 " and/or txn-id %" PRIaTXN "%s%" PRIaTXN, tid, " != must ",
            MDBX_TID_TXN_OUSTED, txnid, " != must ", txn->txnid);
      break;
    }
    if (unlikely((txn->flags & MDBX_TXN_ERROR)))
      break;

#if MDBX_64BIT_CAS
    if (unlikely(!atomic_cas64(&rslot->tid, MDBX_TID_TXN_PARKED, txn->owner)))
      continue;
#else
    atomic_store32(&rslot->tid.high, (uint32_t)((uint64_t)txn->owner >> 32), mo_Relaxed);
    if (unlikely(!atomic_cas32(&rslot->tid.low, (uint32_t)MDBX_TID_TXN_PARKED, (uint32_t)txn->owner))) {
      atomic_store32(&rslot->tid.high, (uint32_t)(MDBX_TID_TXN_PARKED >> 32), mo_AcquireRelease);
      continue;
    }
#endif
    txnid = safe64_read(&rslot->txnid);
    tid = safe64_read(&rslot->tid);
    if (unlikely(txnid != txn->txnid || tid != txn->owner)) {
      ERROR("unexpected thread-id 0x%" PRIx64 "%s0x%zx"
            " and/or txn-id %" PRIaTXN "%s%" PRIaTXN,
            tid, " != must ", txn->owner, txnid, " != must ", txn->txnid);
      break;
    }
    txn->flags &= ~(MDBX_TXN_PARKED | MDBX_TXN_AUTOUNPARK);
    return MDBX_SUCCESS;
  }

  int err = txn_end(txn, TXN_END_OUSTED | TXN_END_RESET | TXN_END_UPDATE);
  return err ? err : MDBX_OUSTED;
}

MDBX_txn *txn_basal_create(const size_t max_dbi) {
  MDBX_txn *txn = nullptr;
  const intptr_t bitmap_bytes =
#if MDBX_ENABLE_DBI_SPARSE
      ceil_powerof2(max_dbi, CHAR_BIT * sizeof(txn->dbi_sparse[0])) / CHAR_BIT;
#else
      0;
#endif /* MDBX_ENABLE_DBI_SPARSE */
  const size_t base = sizeof(MDBX_txn) + /* GC cursor */ sizeof(cursor_couple_t);
  const size_t size =
      base + bitmap_bytes +
      max_dbi * (sizeof(txn->dbs[0]) + sizeof(txn->cursors[0]) + sizeof(txn->dbi_seqs[0]) + sizeof(txn->dbi_state[0]));

  txn = osal_calloc(1, size);
  if (unlikely(!txn))
    return txn;

  txn->dbs = ptr_disp(txn, base);
  txn->cursors = ptr_disp(txn->dbs, max_dbi * sizeof(txn->dbs[0]));
  txn->dbi_seqs = ptr_disp(txn->cursors, max_dbi * sizeof(txn->cursors[0]));
  txn->dbi_state = ptr_disp(txn, size - max_dbi * sizeof(txn->dbi_state[0]));
#if MDBX_ENABLE_DBI_SPARSE
  txn->dbi_sparse = ptr_disp(txn->dbi_state, -bitmap_bytes);
#endif /* MDBX_ENABLE_DBI_SPARSE */
  txn->flags = MDBX_TXN_FINISHED;
  txn->tw.retired_pages = pnl_alloc(MDBX_PNL_INITIAL);
  txn->tw.repnl = pnl_alloc(MDBX_PNL_INITIAL);
  if (unlikely(!txn->tw.retired_pages || !txn->tw.repnl)) {
    txn_basal_destroy(txn);
    txn = nullptr;
  }

  return txn;
}

void txn_basal_destroy(MDBX_txn *txn) {
  dpl_free(txn);
  txl_free(txn->tw.gc.retxl);
  pnl_free(txn->tw.retired_pages);
  pnl_free(txn->tw.spilled.list);
  pnl_free(txn->tw.repnl);
  osal_free(txn);
}

MDBX_txn *txn_alloc(const MDBX_txn_flags_t flags, MDBX_env *env) {
  MDBX_txn *txn = nullptr;
  const intptr_t bitmap_bytes =
#if MDBX_ENABLE_DBI_SPARSE
      ceil_powerof2(env->max_dbi, CHAR_BIT * sizeof(txn->dbi_sparse[0])) / CHAR_BIT;
#else
      0;
#endif /* MDBX_ENABLE_DBI_SPARSE */
  STATIC_ASSERT(sizeof(txn->tw) > sizeof(txn->to));
  const size_t base =
      (flags & MDBX_TXN_RDONLY) ? sizeof(MDBX_txn) - sizeof(txn->tw) + sizeof(txn->to) : sizeof(MDBX_txn);
  const size_t size = base +
                      ((flags & MDBX_TXN_RDONLY) ? (size_t)bitmap_bytes + env->max_dbi * sizeof(txn->dbi_seqs[0]) : 0) +
                      env->max_dbi * (sizeof(txn->dbs[0]) + sizeof(txn->cursors[0]) + sizeof(txn->dbi_state[0]));
  txn = osal_malloc(size);
  if (unlikely(!txn))
    return txn;
#if MDBX_DEBUG
  memset(txn, 0xCD, size);
  VALGRIND_MAKE_MEM_UNDEFINED(txn, size);
#endif /* MDBX_DEBUG */
  MDBX_ANALYSIS_ASSUME(size > base);
  memset(txn, 0, (MDBX_GOOFY_MSVC_STATIC_ANALYZER && base > size) ? size : base);
  txn->dbs = ptr_disp(txn, base);
  txn->cursors = ptr_disp(txn->dbs, env->max_dbi * sizeof(txn->dbs[0]));
#if MDBX_DEBUG
  txn->cursors[FREE_DBI] = nullptr; /* avoid SIGSEGV in an assertion later */
#endif
  txn->dbi_state = ptr_disp(txn, size - env->max_dbi * sizeof(txn->dbi_state[0]));
  txn->flags = flags;
  txn->env = env;

  if (flags & MDBX_TXN_RDONLY) {
    txn->dbi_seqs = ptr_disp(txn->cursors, env->max_dbi * sizeof(txn->cursors[0]));
#if MDBX_ENABLE_DBI_SPARSE
    txn->dbi_sparse = ptr_disp(txn->dbi_state, -bitmap_bytes);
#endif /* MDBX_ENABLE_DBI_SPARSE */
  }
  return txn;
}

int txn_nested_create(MDBX_txn *parent, const MDBX_txn_flags_t flags) {
  if (parent->env->options.spill_parent4child_denominator) {
    /* Spill dirty-pages of parent to provide dirtyroom for child txn */
    int err =
        txn_spill(parent, nullptr, parent->tw.dirtylist->length / parent->env->options.spill_parent4child_denominator);
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
    const size_t len = MDBX_PNL_GETSIZE(parent->tw.repnl) + parent->tw.loose_count;
    txn->tw.repnl = pnl_alloc((len > MDBX_PNL_INITIAL) ? len : MDBX_PNL_INITIAL);
    if (unlikely(!txn->tw.repnl))
      err = MDBX_ENOMEM;
  }
  if (unlikely(err != MDBX_SUCCESS)) {
  failed:
    pnl_free(txn->tw.repnl);
    dpl_free(txn);
    osal_free(txn);
    return LOG_IFERR(err);
  }

  /* Move loose pages to reclaimed list */
  if (parent->tw.loose_count) {
    do {
      page_t *lp = parent->tw.loose_pages;
      tASSERT(parent, lp->flags == P_LOOSE);
      err = pnl_insert_span(&parent->tw.repnl, lp->pgno, 1);
      if (unlikely(err != MDBX_SUCCESS))
        goto failed;
      MDBX_ASAN_UNPOISON_MEMORY_REGION(&page_next(lp), sizeof(page_t *));
      VALGRIND_MAKE_MEM_DEFINED(&page_next(lp), sizeof(page_t *));
      parent->tw.loose_pages = page_next(lp);
      /* Remove from dirty list */
      page_wash(parent, dpl_exist(parent, lp->pgno), lp, 1);
    } while (parent->tw.loose_pages);
    parent->tw.loose_count = 0;
#if MDBX_ENABLE_REFUND
    parent->tw.loose_refund_wl = 0;
#endif /* MDBX_ENABLE_REFUND */
    tASSERT(parent, dpl_check(parent));
  }
  txn->tw.dirtyroom = parent->tw.dirtyroom;
  txn->tw.dirtylru = parent->tw.dirtylru;

  dpl_sort(parent);
  if (parent->tw.spilled.list)
    spill_purge(parent);

  tASSERT(txn, MDBX_PNL_ALLOCLEN(txn->tw.repnl) >= MDBX_PNL_GETSIZE(parent->tw.repnl));
  memcpy(txn->tw.repnl, parent->tw.repnl, MDBX_PNL_SIZEOF(parent->tw.repnl));
  tASSERT(txn, pnl_check_allocated(txn->tw.repnl, (txn->geo.first_unallocated /* LY: intentional assignment
                                                                             here, only for assertion */
                                                   = parent->geo.first_unallocated) -
                                                      MDBX_ENABLE_REFUND));

  txn->tw.gc.time_acc = parent->tw.gc.time_acc;
  txn->tw.gc.last_reclaimed = parent->tw.gc.last_reclaimed;
  if (parent->tw.gc.retxl) {
    txn->tw.gc.retxl = parent->tw.gc.retxl;
    parent->tw.gc.retxl = (void *)(intptr_t)MDBX_PNL_GETSIZE(parent->tw.gc.retxl);
  }

  txn->tw.retired_pages = parent->tw.retired_pages;
  parent->tw.retired_pages = (void *)(intptr_t)MDBX_PNL_GETSIZE(parent->tw.retired_pages);

  txn->txnid = parent->txnid;
  txn->front_txnid = parent->front_txnid + 1;
#if MDBX_ENABLE_REFUND
  txn->tw.loose_refund_wl = 0;
#endif /* MDBX_ENABLE_REFUND */
  txn->canary = parent->canary;
  parent->flags |= MDBX_TXN_HAS_CHILD;
  parent->nested = txn;
  txn->parent = parent;
  txn->owner = parent->owner;
  txn->tw.troika = parent->tw.troika;

  txn->cursors[FREE_DBI] = nullptr;
  txn->cursors[MAIN_DBI] = nullptr;
  txn->dbi_state[FREE_DBI] = parent->dbi_state[FREE_DBI] & ~(DBI_FRESH | DBI_CREAT | DBI_DIRTY);
  txn->dbi_state[MAIN_DBI] = parent->dbi_state[MAIN_DBI] & ~(DBI_FRESH | DBI_CREAT | DBI_DIRTY);
  memset(txn->dbi_state + CORE_DBS, 0, (txn->n_dbi = parent->n_dbi) - CORE_DBS);
  memcpy(txn->dbs, parent->dbs, sizeof(txn->dbs[0]) * CORE_DBS);

  tASSERT(parent, parent->tw.dirtyroom + parent->tw.dirtylist->length ==
                      (parent->parent ? parent->parent->tw.dirtyroom : parent->env->options.dp_limit));
  tASSERT(txn, txn->tw.dirtyroom + txn->tw.dirtylist->length ==
                   (txn->parent ? txn->parent->tw.dirtyroom : txn->env->options.dp_limit));
  parent->env->txn = txn;
  tASSERT(parent, parent->cursors[FREE_DBI] == nullptr);
  return txn_shadow_cursors(parent, MAIN_DBI);
}

void txn_nested_abort(MDBX_txn *nested) {
  MDBX_txn *const parent = nested->parent;
  nested->signature = 0;
  nested->owner = 0;

  if (nested->tw.gc.retxl) {
    tASSERT(parent, MDBX_PNL_GETSIZE(nested->tw.gc.retxl) >= (uintptr_t)parent->tw.gc.retxl);
    MDBX_PNL_SETSIZE(nested->tw.gc.retxl, (uintptr_t)parent->tw.gc.retxl);
    parent->tw.gc.retxl = nested->tw.gc.retxl;
  }

  if (nested->tw.retired_pages) {
    tASSERT(parent, MDBX_PNL_GETSIZE(nested->tw.retired_pages) >= (uintptr_t)parent->tw.retired_pages);
    MDBX_PNL_SETSIZE(nested->tw.retired_pages, (uintptr_t)parent->tw.retired_pages);
    parent->tw.retired_pages = nested->tw.retired_pages;
  }

  parent->tw.dirtylru = nested->tw.dirtylru;
  parent->nested = nullptr;
  parent->flags &= ~MDBX_TXN_HAS_CHILD;
  tASSERT(parent, dpl_check(parent));
  tASSERT(parent, audit_ex(parent, 0, false) == 0);
  dpl_release_shadows(nested);
  dpl_free(nested);
  pnl_free(nested->tw.repnl);
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

  if (txn->tw.dirtylist->length == 0 && !(txn->flags & MDBX_TXN_DIRTY) && parent->n_dbi == txn->n_dbi) {
    TXN_FOREACH_DBI_ALL(txn, i) {
      tASSERT(txn, (txn->dbi_state[i] & DBI_DIRTY) == 0);
      if ((txn->dbi_state[i] & DBI_STALE) && !(parent->dbi_state[i] & DBI_STALE))
        tASSERT(txn, memcmp(&parent->dbs[i], &txn->dbs[i], sizeof(tree_t)) == 0);
    }

    tASSERT(txn, memcmp(&parent->geo, &txn->geo, sizeof(parent->geo)) == 0);
    tASSERT(txn, memcmp(&parent->canary, &txn->canary, sizeof(parent->canary)) == 0);
    tASSERT(txn, !txn->tw.spilled.list || MDBX_PNL_GETSIZE(txn->tw.spilled.list) == 0);
    tASSERT(txn, txn->tw.loose_count == 0);

    VERBOSE("fast-complete pure nested txn %" PRIaTXN, txn->txnid);
    return MDBX_RESULT_TRUE;
  }

  /* Preserve space for spill list to avoid parent's state corruption
   * if allocation fails. */
  const size_t parent_retired_len = (uintptr_t)parent->tw.retired_pages;
  tASSERT(txn, parent_retired_len <= MDBX_PNL_GETSIZE(txn->tw.retired_pages));
  const size_t retired_delta = MDBX_PNL_GETSIZE(txn->tw.retired_pages) - parent_retired_len;
  if (retired_delta) {
    int err = pnl_need(&txn->tw.repnl, retired_delta);
    if (unlikely(err != MDBX_SUCCESS))
      return err;
  }

  if (txn->tw.spilled.list) {
    if (parent->tw.spilled.list) {
      int err = pnl_need(&parent->tw.spilled.list, MDBX_PNL_GETSIZE(txn->tw.spilled.list));
      if (unlikely(err != MDBX_SUCCESS))
        return err;
    }
    spill_purge(txn);
  }

  if (unlikely(txn->tw.dirtylist->length + parent->tw.dirtylist->length > parent->tw.dirtylist->detent &&
               !dpl_reserve(parent, txn->tw.dirtylist->length + parent->tw.dirtylist->length))) {
    return MDBX_ENOMEM;
  }

  //-------------------------------------------------------------------------

  parent->tw.gc.retxl = txn->tw.gc.retxl;
  txn->tw.gc.retxl = nullptr;

  parent->tw.retired_pages = txn->tw.retired_pages;
  txn->tw.retired_pages = nullptr;

  pnl_free(parent->tw.repnl);
  parent->tw.repnl = txn->tw.repnl;
  txn->tw.repnl = nullptr;
  parent->tw.gc.time_acc = txn->tw.gc.time_acc;
  parent->tw.gc.last_reclaimed = txn->tw.gc.last_reclaimed;

  parent->geo = txn->geo;
  parent->canary = txn->canary;
  parent->flags |= txn->flags & MDBX_TXN_DIRTY;

  /* Move loose pages to parent */
#if MDBX_ENABLE_REFUND
  parent->tw.loose_refund_wl = txn->tw.loose_refund_wl;
#endif /* MDBX_ENABLE_REFUND */
  parent->tw.loose_count = txn->tw.loose_count;
  parent->tw.loose_pages = txn->tw.loose_pages;

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
  tASSERT(parent, dpl_check(parent));

#if MDBX_ENABLE_REFUND
  txn_refund(parent);
  if (ASSERT_ENABLED()) {
    /* Check parent's loose pages not suitable for refund */
    for (page_t *lp = parent->tw.loose_pages; lp; lp = page_next(lp)) {
      tASSERT(parent, lp->pgno < parent->tw.loose_refund_wl && lp->pgno + 1 < parent->geo.first_unallocated);
      MDBX_ASAN_UNPOISON_MEMORY_REGION(&page_next(lp), sizeof(page_t *));
      VALGRIND_MAKE_MEM_DEFINED(&page_next(lp), sizeof(page_t *));
    }
    /* Check parent's reclaimed pages not suitable for refund */
    if (MDBX_PNL_GETSIZE(parent->tw.repnl))
      tASSERT(parent, MDBX_PNL_MOST(parent->tw.repnl) + 1 < parent->geo.first_unallocated);
  }
#endif /* MDBX_ENABLE_REFUND */

  txn->signature = 0;
  osal_free(txn);
  tASSERT(parent, audit_ex(parent, 0, false) == 0);
  return MDBX_SUCCESS;
}

int txn_basal_commit(MDBX_txn *txn, struct commit_timestamp *ts) {
  MDBX_env *const env = txn->env;
  tASSERT(txn, txn == env->basal_txn && !txn->parent && !txn->nested);
  if (!txn->tw.dirtylist) {
    tASSERT(txn, (txn->flags & MDBX_WRITEMAP) != 0 && !MDBX_AVOID_MSYNC);
  } else {
    tASSERT(txn, (txn->flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);
    tASSERT(txn, txn->tw.dirtyroom + txn->tw.dirtylist->length == env->options.dp_limit);
  }

  if (txn->flags & txn_may_have_cursors)
    txn_done_cursors(txn);

  if ((!txn->tw.dirtylist || txn->tw.dirtylist->length == 0) &&
      (txn->flags & (MDBX_TXN_DIRTY | MDBX_TXN_SPILLS)) == 0) {
    TXN_FOREACH_DBI_ALL(txn, i) { tASSERT(txn, !(txn->dbi_state[i] & DBI_DIRTY)); }
    /* fast completion of pure transaction */
    return MDBX_RESULT_TRUE;
  }

  DEBUG("committing txn %" PRIaTXN " %p on env %p, root page %" PRIaPGNO "/%" PRIaPGNO, txn->txnid, (void *)txn,
        (void *)env, txn->dbs[MAIN_DBI].root, txn->dbs[FREE_DBI].root);

  if (txn->n_dbi > CORE_DBS) {
    /* Update table root pointers */
    cursor_couple_t cx;
    int err = cursor_init(&cx.outer, txn, MAIN_DBI);
    if (unlikely(err != MDBX_SUCCESS))
      return err;
    cx.outer.next = txn->cursors[MAIN_DBI];
    txn->cursors[MAIN_DBI] = &cx.outer;
    TXN_FOREACH_DBI_USER(txn, i) {
      if ((txn->dbi_state[i] & DBI_DIRTY) == 0)
        continue;
      tree_t *const db = &txn->dbs[i];
      DEBUG("update main's entry for sub-db %zu, mod_txnid %" PRIaTXN " -> %" PRIaTXN, i, db->mod_txnid, txn->txnid);
      /* Может быть mod_txnid > front после коммита вложенных тразакций */
      db->mod_txnid = txn->txnid;
      MDBX_val data = {db, sizeof(tree_t)};
      err = cursor_put(&cx.outer, &env->kvs[i].name, &data, N_TREE);
      if (unlikely(err != MDBX_SUCCESS)) {
        txn->cursors[MAIN_DBI] = cx.outer.next;
        return err;
      }
    }
    txn->cursors[MAIN_DBI] = cx.outer.next;
  }

  if (ts) {
    ts->prep = osal_monotime();
    ts->gc_cpu = osal_cputime(nullptr);
  }

  gcu_t gcu_ctx;
  int rc = gc_update_init(txn, &gcu_ctx);
  if (likely(rc == MDBX_SUCCESS))
    rc = gc_update(txn, &gcu_ctx);
  if (ts)
    ts->gc_cpu = osal_cputime(nullptr) - ts->gc_cpu;
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  tASSERT(txn, txn->tw.loose_count == 0);
  txn->dbs[FREE_DBI].mod_txnid = (txn->dbi_state[FREE_DBI] & DBI_DIRTY) ? txn->txnid : txn->dbs[FREE_DBI].mod_txnid;
  txn->dbs[MAIN_DBI].mod_txnid = (txn->dbi_state[MAIN_DBI] & DBI_DIRTY) ? txn->txnid : txn->dbs[MAIN_DBI].mod_txnid;

  if (ts) {
    ts->gc = osal_monotime();
    ts->audit = ts->gc;
  }
  if (AUDIT_ENABLED()) {
    rc = audit_ex(txn, MDBX_PNL_GETSIZE(txn->tw.retired_pages), true);
    if (ts)
      ts->audit = osal_monotime();
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }

  bool need_flush_for_nometasync = false;
  const meta_ptr_t head = meta_recent(env, &txn->tw.troika);
  const uint32_t meta_sync_txnid = atomic_load32(&env->lck->meta_sync_txnid, mo_Relaxed);
  /* sync prev meta */
  if (head.is_steady && meta_sync_txnid != (uint32_t)head.txnid) {
    /* Исправление унаследованного от LMDB недочета:
     *
     * Всё хорошо, если все процессы работающие с БД не используют WRITEMAP.
     * Тогда мета-страница (обновленная, но не сброшенная на диск) будет
     * сохранена в результате fdatasync() при записи данных этой транзакции.
     *
     * Всё хорошо, если все процессы работающие с БД используют WRITEMAP
     * без MDBX_AVOID_MSYNC.
     * Тогда мета-страница (обновленная, но не сброшенная на диск) будет
     * сохранена в результате msync() при записи данных этой транзакции.
     *
     * Если же в процессах работающих с БД используется оба метода, как sync()
     * в режиме MDBX_WRITEMAP, так и записи через файловый дескриптор, то
     * становится невозможным обеспечить фиксацию на диске мета-страницы
     * предыдущей транзакции и данных текущей транзакции, за счет одной
     * sync-операцией выполняемой после записи данных текущей транзакции.
     * Соответственно, требуется явно обновлять мета-страницу, что полностью
     * уничтожает выгоду от NOMETASYNC. */
    const uint32_t txnid_dist = ((txn->flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC) ? MDBX_NOMETASYNC_LAZY_FD
                                                                                        : MDBX_NOMETASYNC_LAZY_WRITEMAP;
    /* Смысл "магии" в том, чтобы избежать отдельного вызова fdatasync()
     * или msync() для гарантированной фиксации на диске мета-страницы,
     * которая была "лениво" отправлена на запись в предыдущей транзакции,
     * но не сброшена на диск из-за активного режима MDBX_NOMETASYNC. */
    if (
#if defined(_WIN32) || defined(_WIN64)
        !env->ioring.overlapped_fd &&
#endif
        meta_sync_txnid == (uint32_t)head.txnid - txnid_dist)
      need_flush_for_nometasync = true;
    else {
      rc = meta_sync(env, head);
      if (unlikely(rc != MDBX_SUCCESS)) {
        ERROR("txn-%s: error %d", "presync-meta", rc);
        return rc;
      }
    }
  }

  if (txn->tw.dirtylist) {
    tASSERT(txn, (txn->flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);
    tASSERT(txn, txn->tw.loose_count == 0);

    mdbx_filehandle_t fd =
#if defined(_WIN32) || defined(_WIN64)
        env->ioring.overlapped_fd ? env->ioring.overlapped_fd : env->lazy_fd;
    (void)need_flush_for_nometasync;
#else
        (need_flush_for_nometasync || env->dsync_fd == INVALID_HANDLE_VALUE ||
         txn->tw.dirtylist->length > env->options.writethrough_threshold ||
         atomic_load64(&env->lck->unsynced_pages, mo_Relaxed))
            ? env->lazy_fd
            : env->dsync_fd;
#endif /* Windows */

    iov_ctx_t write_ctx;
    rc = iov_init(txn, &write_ctx, txn->tw.dirtylist->length, txn->tw.dirtylist->pages_including_loose, fd, false);
    if (unlikely(rc != MDBX_SUCCESS)) {
      ERROR("txn-%s: error %d", "iov-init", rc);
      return rc;
    }

    rc = txn_write(txn, &write_ctx);
    if (unlikely(rc != MDBX_SUCCESS)) {
      ERROR("txn-%s: error %d", "write", rc);
      return rc;
    }
  } else {
    tASSERT(txn, (txn->flags & MDBX_WRITEMAP) != 0 && !MDBX_AVOID_MSYNC);
    env->lck->unsynced_pages.weak += txn->tw.writemap_dirty_npages;
    if (!env->lck->eoos_timestamp.weak)
      env->lck->eoos_timestamp.weak = osal_monotime();
  }

  /* TODO: use ctx.flush_begin & ctx.flush_end for range-sync */
  if (ts)
    ts->write = osal_monotime();

  meta_t meta;
  memcpy(meta.magic_and_version, head.ptr_c->magic_and_version, 8);
  meta.reserve16 = head.ptr_c->reserve16;
  meta.validator_id = head.ptr_c->validator_id;
  meta.extra_pagehdr = head.ptr_c->extra_pagehdr;
  unaligned_poke_u64(4, meta.pages_retired,
                     unaligned_peek_u64(4, head.ptr_c->pages_retired) + MDBX_PNL_GETSIZE(txn->tw.retired_pages));
  meta.geometry = txn->geo;
  meta.trees.gc = txn->dbs[FREE_DBI];
  meta.trees.main = txn->dbs[MAIN_DBI];
  meta.canary = txn->canary;
  memcpy(&meta.dxbid, &head.ptr_c->dxbid, sizeof(meta.dxbid));

  txnid_t commit_txnid = txn->txnid;
#if MDBX_ENABLE_BIGFOOT
  if (gcu_ctx.bigfoot > txn->txnid) {
    commit_txnid = gcu_ctx.bigfoot;
    TRACE("use @%" PRIaTXN " (+%zu) for commit bigfoot-txn", commit_txnid, (size_t)(commit_txnid - txn->txnid));
  }
#endif
  meta.unsafe_sign = DATASIGN_NONE;
  meta_set_txnid(env, &meta, commit_txnid);

  rc = dxb_sync_locked(env, env->flags | txn->flags | txn_shrink_allowed, &meta, &txn->tw.troika);

  if (ts)
    ts->sync = osal_monotime();
  if (unlikely(rc != MDBX_SUCCESS)) {
    env->flags |= ENV_FATAL_ERROR;
    ERROR("txn-%s: error %d", "sync", rc);
    return rc;
  }

  return MDBX_SUCCESS;
}
