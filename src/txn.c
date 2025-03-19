/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025

#include "internals.h"

__hot txnid_t txn_snapshot_oldest(const MDBX_txn *const txn) {
  return mvcc_shapshot_oldest(txn->env, txn->tw.troika.txnid[txn->tw.troika.prefer_steady]);
}

void txn_done_cursors(MDBX_txn *txn, const bool merge) {
  tASSERT(txn, txn->cursors[FREE_DBI] == nullptr);
  TXN_FOREACH_DBI_FROM(txn, i, /* skip FREE_DBI */ 1) {
    MDBX_cursor *mc = txn->cursors[i];
    if (mc) {
      txn->cursors[i] = nullptr;
      do
        mc = cursor_eot(mc, txn, merge);
      while (mc);
    }
  }
}

int txn_write(MDBX_txn *txn, iov_ctx_t *ctx) {
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

/* Merge child txn into parent */
void txn_merge(MDBX_txn *const parent, MDBX_txn *const txn, const size_t parent_retired_len) {
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

void txn_take_gcprof(const MDBX_txn *txn, MDBX_commit_latency *latency) {
  MDBX_env *const env = txn->env;
  if (MDBX_ENABLE_PROFGC) {
    pgop_stat_t *const ptr = &env->lck->pgops;
    latency->gc_prof.work_counter = ptr->gc_prof.work.spe_counter;
    latency->gc_prof.work_rtime_monotonic = osal_monotime_to_16dot16(ptr->gc_prof.work.rtime_monotonic);
    latency->gc_prof.work_xtime_cpu = osal_monotime_to_16dot16(ptr->gc_prof.work.xtime_cpu);
    latency->gc_prof.work_rsteps = ptr->gc_prof.work.rsteps;
    latency->gc_prof.work_xpages = ptr->gc_prof.work.xpages;
    latency->gc_prof.work_majflt = ptr->gc_prof.work.majflt;

    latency->gc_prof.self_counter = ptr->gc_prof.self.spe_counter;
    latency->gc_prof.self_rtime_monotonic = osal_monotime_to_16dot16(ptr->gc_prof.self.rtime_monotonic);
    latency->gc_prof.self_xtime_cpu = osal_monotime_to_16dot16(ptr->gc_prof.self.xtime_cpu);
    latency->gc_prof.self_rsteps = ptr->gc_prof.self.rsteps;
    latency->gc_prof.self_xpages = ptr->gc_prof.self.xpages;
    latency->gc_prof.self_majflt = ptr->gc_prof.self.majflt;

    latency->gc_prof.wloops = ptr->gc_prof.wloops;
    latency->gc_prof.coalescences = ptr->gc_prof.coalescences;
    latency->gc_prof.wipes = ptr->gc_prof.wipes;
    latency->gc_prof.flushes = ptr->gc_prof.flushes;
    latency->gc_prof.kicks = ptr->gc_prof.kicks;

    latency->gc_prof.pnl_merge_work.time = osal_monotime_to_16dot16(ptr->gc_prof.work.pnl_merge.time);
    latency->gc_prof.pnl_merge_work.calls = ptr->gc_prof.work.pnl_merge.calls;
    latency->gc_prof.pnl_merge_work.volume = ptr->gc_prof.work.pnl_merge.volume;
    latency->gc_prof.pnl_merge_self.time = osal_monotime_to_16dot16(ptr->gc_prof.self.pnl_merge.time);
    latency->gc_prof.pnl_merge_self.calls = ptr->gc_prof.self.pnl_merge.calls;
    latency->gc_prof.pnl_merge_self.volume = ptr->gc_prof.self.pnl_merge.volume;

    if (txn == env->basal_txn)
      memset(&ptr->gc_prof, 0, sizeof(ptr->gc_prof));
  } else
    memset(&latency->gc_prof, 0, sizeof(latency->gc_prof));
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

#if MDBX_ENV_CHECKPID
  if (unlikely(env->pid != osal_getpid())) {
    env->flags |= ENV_FATAL_ERROR;
    return MDBX_PANIC;
  }
#endif /* MDBX_ENV_CHECKPID */

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
  txn_end(txn, TXN_END_SLOT | TXN_END_EOTDONE | TXN_END_FAIL_BEGIN);
  return rc;
}

int txn_end(MDBX_txn *txn, unsigned mode) {
  MDBX_env *env = txn->env;
  static const char *const names[] = TXN_END_NAMES;

  DEBUG("%s txn %" PRIaTXN "%c-0x%X %p  on env %p, root page %" PRIaPGNO "/%" PRIaPGNO, names[mode & TXN_END_OPMASK],
        txn->txnid, (txn->flags & MDBX_TXN_RDONLY) ? 'r' : 'w', txn->flags, (void *)txn, (void *)env,
        txn->dbs[MAIN_DBI].root, txn->dbs[FREE_DBI].root);

  if (!(mode & TXN_END_EOTDONE)) /* !(already closed cursors) */
    txn_done_cursors(txn, false);

  int rc = MDBX_SUCCESS;
  if (txn->flags & MDBX_TXN_RDONLY) {
    if (txn->to.reader) {
      reader_slot_t *slot = txn->to.reader;
      eASSERT(env, slot->pid.weak == env->pid);
      if (likely(!(txn->flags & MDBX_TXN_FINISHED))) {
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
#if defined(_WIN32) || defined(_WIN64)
    if (txn->flags & txn_shrink_allowed)
      imports.srwl_ReleaseShared(&env->remap_guard);
#endif
    txn->n_dbi = 0; /* prevent further DBI activity */
    txn->flags = ((mode & TXN_END_OPMASK) != TXN_END_OUSTED) ? MDBX_TXN_RDONLY | MDBX_TXN_FINISHED
                                                             : MDBX_TXN_RDONLY | MDBX_TXN_FINISHED | MDBX_TXN_OUSTED;
    txn->owner = 0;
  } else if (!(txn->flags & MDBX_TXN_FINISHED)) {
    ENSURE(env, txn->txnid >=
                    /* paranoia is appropriate here */ env->lck->cached_oldest.weak);
    if (txn == env->basal_txn)
      dxb_sanitize_tail(env, nullptr);

    txn->flags = MDBX_TXN_FINISHED;
    env->txn = txn->parent;
    pnl_free(txn->tw.spilled.list);
    txn->tw.spilled.list = nullptr;
    if (txn == env->basal_txn) {
      eASSERT(env, txn->parent == nullptr);
      /* Export or close DBI handles created in this txn */
      rc = dbi_update(txn, mode & TXN_END_UPDATE);
      pnl_shrink(&txn->tw.retired_pages);
      pnl_shrink(&txn->tw.repnl);
      if (!(env->flags & MDBX_WRITEMAP))
        dpl_release_shadows(txn);
      /* The writer mutex was locked in mdbx_txn_begin. */
      lck_txn_unlock(env);
    } else {
      eASSERT(env, txn->parent != nullptr);
      MDBX_txn *const parent = txn->parent;
      eASSERT(env, parent->signature == txn_signature);
      eASSERT(env, parent->nested == txn && (parent->flags & MDBX_TXN_HAS_CHILD) != 0);
      eASSERT(env, pnl_check_allocated(txn->tw.repnl, txn->geo.first_unallocated - MDBX_ENABLE_REFUND));
      eASSERT(env, memcmp(&txn->tw.troika, &parent->tw.troika, sizeof(troika_t)) == 0);

      txn->owner = 0;
      if (txn->tw.gc.retxl) {
        eASSERT(env, MDBX_PNL_GETSIZE(txn->tw.gc.retxl) >= (uintptr_t)parent->tw.gc.retxl);
        MDBX_PNL_SETSIZE(txn->tw.gc.retxl, (uintptr_t)parent->tw.gc.retxl);
        parent->tw.gc.retxl = txn->tw.gc.retxl;
      }

      if (txn->tw.retired_pages) {
        eASSERT(env, MDBX_PNL_GETSIZE(txn->tw.retired_pages) >= (uintptr_t)parent->tw.retired_pages);
        MDBX_PNL_SETSIZE(txn->tw.retired_pages, (uintptr_t)parent->tw.retired_pages);
        parent->tw.retired_pages = txn->tw.retired_pages;
      }

      parent->nested = nullptr;
      parent->flags &= ~MDBX_TXN_HAS_CHILD;
      parent->tw.dirtylru = txn->tw.dirtylru;
      tASSERT(parent, dpl_check(parent));
      tASSERT(parent, audit_ex(parent, 0, false) == 0);
      dpl_release_shadows(txn);
      dpl_free(txn);
      pnl_free(txn->tw.repnl);

      if (parent->geo.upper != txn->geo.upper || parent->geo.now != txn->geo.now) {
        /* undo resize performed by child txn */
        rc = dxb_resize(env, parent->geo.first_unallocated, parent->geo.now, parent->geo.upper, impilict_shrink);
        if (rc == MDBX_EPERM) {
          /* unable undo resize (it is regular for Windows),
           * therefore promote size changes from child to the parent txn */
          WARNING("unable undo resize performed by child txn, promote to "
                  "the parent (%u->%u, %u->%u)",
                  txn->geo.now, parent->geo.now, txn->geo.upper, parent->geo.upper);
          parent->geo.now = txn->geo.now;
          parent->geo.upper = txn->geo.upper;
          parent->flags |= MDBX_TXN_DIRTY;
          rc = MDBX_SUCCESS;
        } else if (unlikely(rc != MDBX_SUCCESS)) {
          ERROR("error %d while undo resize performed by child txn, fail "
                "the parent",
                rc);
          parent->flags |= MDBX_TXN_ERROR;
          if (!env->dxb_mmap.base)
            env->flags |= ENV_FATAL_ERROR;
        }
      }
    }
  }

  eASSERT(env, txn == env->basal_txn || txn->owner == 0);
  if ((mode & TXN_END_FREE) != 0 && txn != env->basal_txn) {
    txn->signature = 0;
    osal_free(txn);
  }

  return rc;
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
   *    при распарковке.
   *
   * Поэтому для припаркованных транзакций возвращается ошибка если не-включена
   * авто-распарковка, либо есть другие плохие биты. */
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
