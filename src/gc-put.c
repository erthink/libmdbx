/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024

#include "internals.h"

MDBX_NOTHROW_PURE_FUNCTION static bool is_lifo(const MDBX_txn *txn) {
  return (txn->env->flags & MDBX_LIFORECLAIM) != 0;
}

MDBX_MAYBE_UNUSED static inline const char *dbg_prefix(const gcu_t *ctx) {
  return is_lifo(ctx->cursor.txn) ? "    lifo" : "    fifo";
}

static inline size_t backlog_size(MDBX_txn *txn) { return MDBX_PNL_GETSIZE(txn->tw.relist) + txn->tw.loose_count; }

static int clean_stored_retired(MDBX_txn *txn, gcu_t *ctx) {
  int err = MDBX_SUCCESS;
  if (ctx->retired_stored) {
    MDBX_cursor *const gc = ptr_disp(txn, sizeof(MDBX_txn));
    tASSERT(txn, txn == txn->env->basal_txn && gc->next == gc);
    gc->txn = txn;
    gc->dbi_state = txn->dbi_state;
    gc->top_and_flags = z_fresh_mark;
    gc->next = txn->cursors[FREE_DBI];
    txn->cursors[FREE_DBI] = gc;
    do {
      MDBX_val key, val;
#if MDBX_ENABLE_BIGFOOT
      key.iov_base = &ctx->bigfoot;
#else
      key.iov_base = &txn->txnid;
#endif /* MDBX_ENABLE_BIGFOOT */
      key.iov_len = sizeof(txnid_t);
      const csr_t csr = cursor_seek(gc, &key, &val, MDBX_SET);
      if (csr.err == MDBX_SUCCESS && csr.exact) {
        ctx->retired_stored = 0;
        err = cursor_del(gc, 0);
        TRACE("== clear-4linear, backlog %zu, err %d", backlog_size(txn), err);
      } else
        err = (csr.err == MDBX_NOTFOUND) ? MDBX_SUCCESS : csr.err;
    }
#if MDBX_ENABLE_BIGFOOT
    while (!err && --ctx->bigfoot >= txn->txnid);
#else
    while (0);
#endif /* MDBX_ENABLE_BIGFOOT */
    txn->cursors[FREE_DBI] = gc->next;
    gc->next = gc;
  }
  return err;
}

static int touch_gc(gcu_t *ctx) {
  tASSERT(ctx->cursor.txn, is_pointed(&ctx->cursor) || ctx->cursor.txn->dbs[FREE_DBI].leaf_pages == 0);
  MDBX_val key, val;
  key.iov_base = val.iov_base = nullptr;
  key.iov_len = sizeof(txnid_t);
  val.iov_len = MDBX_PNL_SIZEOF(ctx->cursor.txn->tw.retired_pages);
  ctx->cursor.flags |= z_gcu_preparation;
  int err = cursor_touch(&ctx->cursor, &key, &val);
  ctx->cursor.flags -= z_gcu_preparation;
  return err;
}

/* Prepare a backlog of pages to modify GC itself, while reclaiming is
 * prohibited. It should be enough to prevent search in gc_alloc_ex()
 * during a deleting, when GC tree is unbalanced. */
static int prepare_backlog(MDBX_txn *txn, gcu_t *ctx) {
  const size_t for_cow = txn->dbs[FREE_DBI].height;
  const size_t for_rebalance = for_cow + 1 + (txn->dbs[FREE_DBI].height + 1ul >= txn->dbs[FREE_DBI].branch_pages);
  size_t for_split = ctx->retired_stored == 0;
  tASSERT(txn, is_pointed(&ctx->cursor) || txn->dbs[FREE_DBI].leaf_pages == 0);

  const intptr_t retired_left = MDBX_PNL_SIZEOF(txn->tw.retired_pages) - ctx->retired_stored;
  size_t for_relist = 0;
  if (MDBX_ENABLE_BIGFOOT && retired_left > 0) {
    for_relist = (retired_left + txn->env->maxgc_large1page - 1) / txn->env->maxgc_large1page;
    const size_t per_branch_page = txn->env->maxgc_per_branch;
    for (size_t entries = for_relist; entries > 1; for_split += entries)
      entries = (entries + per_branch_page - 1) / per_branch_page;
  } else if (!MDBX_ENABLE_BIGFOOT && retired_left != 0) {
    for_relist = largechunk_npages(txn->env, MDBX_PNL_SIZEOF(txn->tw.retired_pages));
  }

  const size_t for_tree_before_touch = for_cow + for_rebalance + for_split;
  const size_t for_tree_after_touch = for_rebalance + for_split;
  const size_t for_all_before_touch = for_relist + for_tree_before_touch;
  const size_t for_all_after_touch = for_relist + for_tree_after_touch;

  if (likely(for_relist < 2 && backlog_size(txn) > for_all_before_touch) &&
      (ctx->cursor.top < 0 || is_modifable(txn, ctx->cursor.pg[ctx->cursor.top])))
    return MDBX_SUCCESS;

  TRACE(">> retired-stored %zu, left %zi, backlog %zu, need %zu (4list %zu, "
        "4split %zu, "
        "4cow %zu, 4tree %zu)",
        ctx->retired_stored, retired_left, backlog_size(txn), for_all_before_touch, for_relist, for_split, for_cow,
        for_tree_before_touch);

  int err = touch_gc(ctx);
  TRACE("== after-touch, backlog %zu, err %d", backlog_size(txn), err);

  if (!MDBX_ENABLE_BIGFOOT && unlikely(for_relist > 1) &&
      MDBX_PNL_GETSIZE(txn->tw.retired_pages) != ctx->retired_stored && err == MDBX_SUCCESS) {
    if (unlikely(ctx->retired_stored)) {
      err = clean_stored_retired(txn, ctx);
      if (unlikely(err != MDBX_SUCCESS))
        return err;
      if (!ctx->retired_stored)
        return /* restart by tail-recursion */ prepare_backlog(txn, ctx);
    }
    err = gc_alloc_ex(&ctx->cursor, for_relist, ALLOC_RESERVE).err;
    TRACE("== after-4linear, backlog %zu, err %d", backlog_size(txn), err);
    cASSERT(&ctx->cursor, backlog_size(txn) >= for_relist || err != MDBX_SUCCESS);
  }

  while (backlog_size(txn) < for_all_after_touch && err == MDBX_SUCCESS)
    err = gc_alloc_ex(&ctx->cursor, 0, ALLOC_RESERVE | ALLOC_UNIMPORTANT).err;

  TRACE("<< backlog %zu, err %d, gc: height %u, branch %zu, leaf %zu, large "
        "%zu, entries %zu",
        backlog_size(txn), err, txn->dbs[FREE_DBI].height, (size_t)txn->dbs[FREE_DBI].branch_pages,
        (size_t)txn->dbs[FREE_DBI].leaf_pages, (size_t)txn->dbs[FREE_DBI].large_pages,
        (size_t)txn->dbs[FREE_DBI].items);
  tASSERT(txn, err != MDBX_NOTFOUND || (txn->flags & txn_gc_drained) != 0);
  return (err != MDBX_NOTFOUND) ? err : MDBX_SUCCESS;
}

static inline void zeroize_reserved(const MDBX_env *env, MDBX_val pnl) {
#if MDBX_DEBUG && (defined(ENABLE_MEMCHECK) || defined(__SANITIZE_ADDRESS__))
  /* Для предотвращения предупреждения Valgrind из mdbx_dump_val()
   * вызванное через макрос DVAL_DEBUG() на выходе
   * из cursor_seek(MDBX_SET_KEY), которая вызывается ниже внутри gc_update() в
   * цикле очистки и цикле заполнения зарезервированных элементов. */
  memset(pnl.iov_base, 0xBB, pnl.iov_len);
#endif /* MDBX_DEBUG && (ENABLE_MEMCHECK || __SANITIZE_ADDRESS__) */

  /* PNL is initially empty, zero out at least the length */
  memset(pnl.iov_base, 0, sizeof(pgno_t));
  if ((env->flags & (MDBX_WRITEMAP | MDBX_NOMEMINIT)) == 0)
    /* zero out to avoid leaking values from uninitialized malloc'ed memory
     * to the file in non-writemap mode if length of the saving page-list
     * was changed during space reservation. */
    memset(pnl.iov_base, 0, pnl.iov_len);
}

static int gcu_loose(MDBX_txn *txn, gcu_t *ctx) {
  tASSERT(txn, txn->tw.loose_count > 0);
  /* Return loose page numbers to tw.relist,
   * though usually none are left at this point.
   * The pages themselves remain in dirtylist. */
  if (unlikely(!txn->tw.gc.reclaimed && txn->tw.gc.last_reclaimed < 1)) {
    TRACE("%s: try allocate gc-slot for %zu loose-pages", dbg_prefix(ctx), txn->tw.loose_count);
    int err = gc_alloc_ex(&ctx->cursor, 0, ALLOC_RESERVE).err;
    if (err == MDBX_SUCCESS) {
      TRACE("%s: retry since gc-slot for %zu loose-pages available", dbg_prefix(ctx), txn->tw.loose_count);
      return MDBX_RESULT_TRUE;
    }

    /* Put loose page numbers in tw.retired_pages,
     * since unable to return ones to tw.relist. */
    err = pnl_need(&txn->tw.retired_pages, txn->tw.loose_count);
    if (unlikely(err != MDBX_SUCCESS))
      return err;
    for (page_t *lp = txn->tw.loose_pages; lp; lp = page_next(lp)) {
      pnl_append_prereserved(txn->tw.retired_pages, lp->pgno);
      MDBX_ASAN_UNPOISON_MEMORY_REGION(&page_next(lp), sizeof(page_t *));
      VALGRIND_MAKE_MEM_DEFINED(&page_next(lp), sizeof(page_t *));
    }
    TRACE("%s: append %zu loose-pages to retired-pages", dbg_prefix(ctx), txn->tw.loose_count);
  } else {
    /* Room for loose pages + temp PNL with same */
    int err = pnl_need(&txn->tw.relist, 2 * txn->tw.loose_count + 2);
    if (unlikely(err != MDBX_SUCCESS))
      return err;
    pnl_t loose = txn->tw.relist + MDBX_PNL_ALLOCLEN(txn->tw.relist) - txn->tw.loose_count - 1;
    size_t count = 0;
    for (page_t *lp = txn->tw.loose_pages; lp; lp = page_next(lp)) {
      tASSERT(txn, lp->flags == P_LOOSE);
      loose[++count] = lp->pgno;
      MDBX_ASAN_UNPOISON_MEMORY_REGION(&page_next(lp), sizeof(page_t *));
      VALGRIND_MAKE_MEM_DEFINED(&page_next(lp), sizeof(page_t *));
    }
    tASSERT(txn, count == txn->tw.loose_count);
    MDBX_PNL_SETSIZE(loose, count);
    pnl_sort(loose, txn->geo.first_unallocated);
    pnl_merge(txn->tw.relist, loose);
    TRACE("%s: append %zu loose-pages to reclaimed-pages", dbg_prefix(ctx), txn->tw.loose_count);
  }

  /* filter-out list of dirty-pages from loose-pages */
  dpl_t *const dl = txn->tw.dirtylist;
  if (dl) {
    tASSERT(txn, (txn->flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);
    tASSERT(txn, dl->sorted <= dl->length);
    size_t w = 0, sorted_out = 0;
    for (size_t r = w; ++r <= dl->length;) {
      page_t *dp = dl->items[r].ptr;
      tASSERT(txn, dp->flags == P_LOOSE || is_modifable(txn, dp));
      tASSERT(txn, dpl_endpgno(dl, r) <= txn->geo.first_unallocated);
      if ((dp->flags & P_LOOSE) == 0) {
        if (++w != r)
          dl->items[w] = dl->items[r];
      } else {
        tASSERT(txn, dp->flags == P_LOOSE);
        sorted_out += dl->sorted >= r;
        if (!MDBX_AVOID_MSYNC || !(txn->flags & MDBX_WRITEMAP))
          page_shadow_release(txn->env, dp, 1);
      }
    }
    TRACE("%s: filtered-out loose-pages from %zu -> %zu dirty-pages", dbg_prefix(ctx), dl->length, w);
    tASSERT(txn, txn->tw.loose_count == dl->length - w);
    dl->sorted -= sorted_out;
    tASSERT(txn, dl->sorted <= w);
    dpl_setlen(dl, w);
    dl->pages_including_loose -= txn->tw.loose_count;
    txn->tw.dirtyroom += txn->tw.loose_count;
    tASSERT(txn, txn->tw.dirtyroom + txn->tw.dirtylist->length ==
                     (txn->parent ? txn->parent->tw.dirtyroom : txn->env->options.dp_limit));
  } else {
    tASSERT(txn, (txn->flags & MDBX_WRITEMAP) != 0 && !MDBX_AVOID_MSYNC);
  }
  txn->tw.loose_pages = nullptr;
  txn->tw.loose_count = 0;
#if MDBX_ENABLE_REFUND
  txn->tw.loose_refund_wl = 0;
#endif /* MDBX_ENABLE_REFUND */
  return MDBX_SUCCESS;
}

static int gcu_retired(MDBX_txn *txn, gcu_t *ctx) {
  int err;
  if (unlikely(!ctx->retired_stored)) {
    /* Make sure last page of GC is touched and on retired-list */
    err = outer_last(&ctx->cursor, nullptr, nullptr);
    if (likely(err == MDBX_SUCCESS))
      err = touch_gc(ctx);
    if (unlikely(err != MDBX_SUCCESS) && err != MDBX_NOTFOUND)
      return err;
  }

  MDBX_val key, data;
#if MDBX_ENABLE_BIGFOOT
  size_t retired_pages_before;
  do {
    if (ctx->bigfoot > txn->txnid) {
      err = clean_stored_retired(txn, ctx);
      if (unlikely(err != MDBX_SUCCESS))
        return err;
      tASSERT(txn, ctx->bigfoot <= txn->txnid);
    }

    retired_pages_before = MDBX_PNL_GETSIZE(txn->tw.retired_pages);
    err = prepare_backlog(txn, ctx);
    if (unlikely(err != MDBX_SUCCESS))
      return err;
    if (retired_pages_before != MDBX_PNL_GETSIZE(txn->tw.retired_pages)) {
      TRACE("%s: retired-list changed (%zu -> %zu), retry", dbg_prefix(ctx), retired_pages_before,
            MDBX_PNL_GETSIZE(txn->tw.retired_pages));
      break;
    }

    pnl_sort(txn->tw.retired_pages, txn->geo.first_unallocated);
    ctx->retired_stored = 0;
    ctx->bigfoot = txn->txnid;
    do {
      if (ctx->retired_stored) {
        err = prepare_backlog(txn, ctx);
        if (unlikely(err != MDBX_SUCCESS))
          return err;
        if (ctx->retired_stored >= MDBX_PNL_GETSIZE(txn->tw.retired_pages)) {
          TRACE("%s: retired-list changed (%zu -> %zu), retry", dbg_prefix(ctx), retired_pages_before,
                MDBX_PNL_GETSIZE(txn->tw.retired_pages));
          break;
        }
      }
      key.iov_len = sizeof(txnid_t);
      key.iov_base = &ctx->bigfoot;
      const size_t left = MDBX_PNL_GETSIZE(txn->tw.retired_pages) - ctx->retired_stored;
      const size_t chunk =
          (left > txn->env->maxgc_large1page && ctx->bigfoot < MAX_TXNID) ? txn->env->maxgc_large1page : left;
      data.iov_len = (chunk + 1) * sizeof(pgno_t);
      err = cursor_put(&ctx->cursor, &key, &data, MDBX_RESERVE);
      if (unlikely(err != MDBX_SUCCESS))
        return err;

#if MDBX_DEBUG && (defined(ENABLE_MEMCHECK) || defined(__SANITIZE_ADDRESS__))
      /* Для предотвращения предупреждения Valgrind из mdbx_dump_val()
       * вызванное через макрос DVAL_DEBUG() на выходе
       * из cursor_seek(MDBX_SET_KEY), которая вызывается как выше в цикле
       * очистки, так и ниже в цикле заполнения зарезервированных элементов.
       */
      memset(data.iov_base, 0xBB, data.iov_len);
#endif /* MDBX_DEBUG && (ENABLE_MEMCHECK || __SANITIZE_ADDRESS__) */

      if (retired_pages_before == MDBX_PNL_GETSIZE(txn->tw.retired_pages)) {
        const size_t at = (is_lifo(txn) == MDBX_PNL_ASCENDING) ? left - chunk : ctx->retired_stored;
        pgno_t *const begin = txn->tw.retired_pages + at;
        /* MDBX_PNL_ASCENDING == false && LIFO == false:
         *  - the larger pgno is at the beginning of retired list
         *    and should be placed with the larger txnid.
         * MDBX_PNL_ASCENDING == true && LIFO == true:
         *  - the larger pgno is at the ending of retired list
         *    and should be placed with the smaller txnid. */
        const pgno_t save = *begin;
        *begin = (pgno_t)chunk;
        memcpy(data.iov_base, begin, data.iov_len);
        *begin = save;
        TRACE("%s: put-retired/bigfoot @ %" PRIaTXN " (slice #%u) #%zu [%zu..%zu] of %zu", dbg_prefix(ctx),
              ctx->bigfoot, (unsigned)(ctx->bigfoot - txn->txnid), chunk, at, at + chunk, retired_pages_before);
      }
      ctx->retired_stored += chunk;
    } while (ctx->retired_stored < MDBX_PNL_GETSIZE(txn->tw.retired_pages) && (++ctx->bigfoot, true));
  } while (retired_pages_before != MDBX_PNL_GETSIZE(txn->tw.retired_pages));
#else
  /* Write to last page of GC */
  key.iov_len = sizeof(txnid_t);
  key.iov_base = &txn->txnid;
  do {
    prepare_backlog(txn, ctx);
    data.iov_len = MDBX_PNL_SIZEOF(txn->tw.retired_pages);
    err = cursor_put(&ctx->cursor, &key, &data, MDBX_RESERVE);
    if (unlikely(err != MDBX_SUCCESS))
      return err;

#if MDBX_DEBUG && (defined(ENABLE_MEMCHECK) || defined(__SANITIZE_ADDRESS__))
    /* Для предотвращения предупреждения Valgrind из mdbx_dump_val()
     * вызванное через макрос DVAL_DEBUG() на выходе
     * из cursor_seek(MDBX_SET_KEY), которая вызывается как выше в цикле
     * очистки, так и ниже в цикле заполнения зарезервированных элементов. */
    memset(data.iov_base, 0xBB, data.iov_len);
#endif /* MDBX_DEBUG && (ENABLE_MEMCHECK || __SANITIZE_ADDRESS__) */

    /* Retry if tw.retired_pages[] grew during the Put() */
  } while (data.iov_len < MDBX_PNL_SIZEOF(txn->tw.retired_pages));

  ctx->retired_stored = MDBX_PNL_GETSIZE(txn->tw.retired_pages);
  pnl_sort(txn->tw.retired_pages, txn->geo.first_unallocated);
  tASSERT(txn, data.iov_len == MDBX_PNL_SIZEOF(txn->tw.retired_pages));
  memcpy(data.iov_base, txn->tw.retired_pages, data.iov_len);

  TRACE("%s: put-retired #%zu @ %" PRIaTXN, dbg_prefix(ctx), ctx->retired_stored, txn->txnid);
#endif /* MDBX_ENABLE_BIGFOOT */
  if (LOG_ENABLED(MDBX_LOG_EXTRA)) {
    size_t i = ctx->retired_stored;
    DEBUG_EXTRA("txn %" PRIaTXN " root %" PRIaPGNO " num %zu, retired-PNL", txn->txnid, txn->dbs[FREE_DBI].root, i);
    for (; i; i--)
      DEBUG_EXTRA_PRINT(" %" PRIaPGNO, txn->tw.retired_pages[i]);
    DEBUG_EXTRA_PRINT("%s\n", ".");
  }
  return MDBX_SUCCESS;
}

typedef struct gcu_rid_result {
  int err;
  txnid_t rid;
} rid_t;

static rid_t get_rid_for_reclaimed(MDBX_txn *txn, gcu_t *ctx, const size_t left) {
  rid_t r;
  if (is_lifo(txn)) {
    if (txn->tw.gc.reclaimed == nullptr) {
      txn->tw.gc.reclaimed = txl_alloc();
      if (unlikely(!txn->tw.gc.reclaimed)) {
        r.err = MDBX_ENOMEM;
        goto return_error;
      }
    }
    if (MDBX_PNL_GETSIZE(txn->tw.gc.reclaimed) < txl_max &&
        left > (MDBX_PNL_GETSIZE(txn->tw.gc.reclaimed) - ctx->reused_slot) * txn->env->maxgc_large1page &&
        !ctx->dense) {
      /* Hужен свободный для для сохранения списка страниц. */
      bool need_cleanup = false;
      txnid_t snap_oldest = 0;
    retry_rid:
      do {
        r.err = gc_alloc_ex(&ctx->cursor, 0, ALLOC_RESERVE).err;
        snap_oldest = txn->env->lck->cached_oldest.weak;
        if (likely(r.err == MDBX_SUCCESS)) {
          TRACE("%s: took @%" PRIaTXN " from GC", dbg_prefix(ctx), MDBX_PNL_LAST(txn->tw.gc.reclaimed));
          need_cleanup = true;
        }
      } while (r.err == MDBX_SUCCESS && MDBX_PNL_GETSIZE(txn->tw.gc.reclaimed) < txl_max &&
               left > (MDBX_PNL_GETSIZE(txn->tw.gc.reclaimed) - ctx->reused_slot) * txn->env->maxgc_large1page);

      if (likely(r.err == MDBX_SUCCESS)) {
        TRACE("%s: got enough from GC.", dbg_prefix(ctx));
        goto return_continue;
      } else if (unlikely(r.err != MDBX_NOTFOUND))
        /* LY: some troubles... */
        goto return_error;

      if (MDBX_PNL_GETSIZE(txn->tw.gc.reclaimed)) {
        if (need_cleanup) {
          txl_sort(txn->tw.gc.reclaimed);
          ctx->cleaned_slot = 0;
        }
        ctx->rid = MDBX_PNL_LAST(txn->tw.gc.reclaimed);
      } else {
        tASSERT(txn, txn->tw.gc.last_reclaimed == 0);
        if (unlikely(txn_snapshot_oldest(txn) != snap_oldest))
          /* should retry gc_alloc_ex()
           * if the oldest reader changes since the last attempt */
          goto retry_rid;
        /* no reclaimable GC entries,
         * therefore no entries with ID < mdbx_find_oldest(txn) */
        txn->tw.gc.last_reclaimed = ctx->rid = snap_oldest;
        TRACE("%s: none recycled yet, set rid to @%" PRIaTXN, dbg_prefix(ctx), ctx->rid);
      }

      /* В GC нет годных к переработке записей,
       * будем использовать свободные id в обратном порядке. */
      while (MDBX_PNL_GETSIZE(txn->tw.gc.reclaimed) < txl_max &&
             left > (MDBX_PNL_GETSIZE(txn->tw.gc.reclaimed) - ctx->reused_slot) * txn->env->maxgc_large1page) {
        if (unlikely(ctx->rid <= MIN_TXNID)) {
          ctx->dense = true;
          if (unlikely(MDBX_PNL_GETSIZE(txn->tw.gc.reclaimed) <= ctx->reused_slot)) {
            NOTICE("** restart: reserve depleted (reused_gc_slot %zu >= "
                   "gc.reclaimed %zu)",
                   ctx->reused_slot, MDBX_PNL_GETSIZE(txn->tw.gc.reclaimed));
            goto return_restart;
          }
          break;
        }

        tASSERT(txn, ctx->rid >= MIN_TXNID && ctx->rid <= MAX_TXNID);
        ctx->rid -= 1;
        MDBX_val key = {&ctx->rid, sizeof(ctx->rid)}, data;
        r.err = cursor_seek(&ctx->cursor, &key, &data, MDBX_SET_KEY).err;
        if (unlikely(r.err == MDBX_SUCCESS)) {
          DEBUG("%s: GC's id %" PRIaTXN " is present, going to first", dbg_prefix(ctx), ctx->rid);
          r.err = outer_first(&ctx->cursor, &key, nullptr);
          if (unlikely(r.err != MDBX_SUCCESS || key.iov_len != sizeof(txnid_t))) {
            ERROR("%s/%d: %s %u", "MDBX_CORRUPTED", MDBX_CORRUPTED, "invalid GC-key size", (unsigned)key.iov_len);
            r.err = MDBX_CORRUPTED;
            goto return_error;
          }
          const txnid_t gc_first = unaligned_peek_u64(4, key.iov_base);
          if (unlikely(gc_first <= INITIAL_TXNID)) {
            NOTICE("%s: no free GC's id(s) less than %" PRIaTXN " (going dense-mode)", dbg_prefix(ctx), ctx->rid);
            ctx->dense = true;
            goto return_restart;
          }
          ctx->rid = gc_first - 1;
        }

        tASSERT(txn, !ctx->dense);
        r.err = txl_append(&txn->tw.gc.reclaimed, ctx->rid);
        if (unlikely(r.err != MDBX_SUCCESS))
          goto return_error;

        if (ctx->reused_slot)
          /* rare case, but it is better to clear and re-create GC entries
           * with less fragmentation. */
          need_cleanup = true;
        else
          ctx->cleaned_slot += 1 /* mark cleanup is not needed for added slot. */;

        TRACE("%s: append @%" PRIaTXN " to lifo-reclaimed, cleaned-gc-slot = %zu", dbg_prefix(ctx), ctx->rid,
              ctx->cleaned_slot);
      }

      if (need_cleanup) {
        if (ctx->cleaned_slot) {
          TRACE("%s: restart to clear and re-create GC entries", dbg_prefix(ctx));
          goto return_restart;
        }
        goto return_continue;
      }
    }

    const size_t i = MDBX_PNL_GETSIZE(txn->tw.gc.reclaimed) - ctx->reused_slot;
    tASSERT(txn, i > 0 && i <= MDBX_PNL_GETSIZE(txn->tw.gc.reclaimed));
    r.rid = txn->tw.gc.reclaimed[i];
    TRACE("%s: take @%" PRIaTXN " from lifo-reclaimed[%zu]", dbg_prefix(ctx), r.rid, i);
  } else {
    tASSERT(txn, txn->tw.gc.reclaimed == nullptr);
    if (unlikely(ctx->rid == 0)) {
      ctx->rid = txn_snapshot_oldest(txn);
      MDBX_val key;
      r.err = outer_first(&ctx->cursor, &key, nullptr);
      if (likely(r.err == MDBX_SUCCESS)) {
        if (unlikely(key.iov_len != sizeof(txnid_t))) {
          ERROR("%s/%d: %s %u", "MDBX_CORRUPTED", MDBX_CORRUPTED, "invalid GC-key size", (unsigned)key.iov_len);
          r.err = MDBX_CORRUPTED;
          goto return_error;
        }
        const txnid_t gc_first = unaligned_peek_u64(4, key.iov_base);
        if (ctx->rid >= gc_first && gc_first)
          ctx->rid = gc_first - 1;
        if (unlikely(ctx->rid <= MIN_TXNID)) {
          ERROR("%s", "** no GC tail-space to store (going dense-mode)");
          ctx->dense = true;
          goto return_restart;
        }
      } else if (r.err != MDBX_NOTFOUND) {
        r.rid = 0;
        return r;
      }
      txn->tw.gc.last_reclaimed = ctx->rid;
      ctx->cleaned_id = ctx->rid + 1;
    }
    r.rid = ctx->rid--;
    TRACE("%s: take @%" PRIaTXN " from GC", dbg_prefix(ctx), r.rid);
  }
  ++ctx->reused_slot;
  r.err = MDBX_SUCCESS;
  return r;

return_continue:
  r.err = MDBX_SUCCESS;
  r.rid = 0;
  return r;

return_restart:
  r.err = MDBX_RESULT_TRUE;
  r.rid = 0;
  return r;

return_error:
  tASSERT(txn, r.err != MDBX_SUCCESS);
  r.rid = 0;
  return r;
}

/* Cleanups reclaimed GC (aka freeDB) records, saves the retired-list (aka
 * freelist) of current transaction to GC, puts back into GC leftover of the
 * reclaimed pages with chunking. This recursive changes the reclaimed-list,
 * loose-list and retired-list. Keep trying until it stabilizes.
 *
 * NOTE: This code is a consequence of many iterations of adding crutches (aka
 * "checks and balances") to partially bypass the fundamental design problems
 * inherited from LMDB. So do not try to understand it completely in order to
 * avoid your madness. */
int gc_update(MDBX_txn *txn, gcu_t *ctx) {
  TRACE("\n>>> @%" PRIaTXN, txn->txnid);
  MDBX_env *const env = txn->env;
  ctx->cursor.next = txn->cursors[FREE_DBI];
  txn->cursors[FREE_DBI] = &ctx->cursor;
  int rc;

  /* txn->tw.relist[] can grow and shrink during this call.
   * txn->tw.gc.last_reclaimed and txn->tw.retired_pages[] can only grow.
   * But page numbers cannot disappear from txn->tw.retired_pages[]. */
retry_clean_adj:
  ctx->reserve_adj = 0;
retry:
  ctx->loop += !(ctx->prev_first_unallocated > txn->geo.first_unallocated);
  TRACE(">> restart, loop %u", ctx->loop);

  tASSERT(txn, pnl_check_allocated(txn->tw.relist, txn->geo.first_unallocated - MDBX_ENABLE_REFUND));
  tASSERT(txn, dpl_check(txn));
  if (unlikely(/* paranoia */ ctx->loop > ((MDBX_DEBUG > 0) ? 12 : 42))) {
    ERROR("txn #%" PRIaTXN " too more loops %u, bailout", txn->txnid, ctx->loop);
    rc = MDBX_PROBLEM;
    goto bailout;
  }

  if (unlikely(ctx->dense || ctx->prev_first_unallocated > txn->geo.first_unallocated)) {
    rc = clean_stored_retired(txn, ctx);
    if (unlikely(rc != MDBX_SUCCESS))
      goto bailout;
  }

  ctx->prev_first_unallocated = txn->geo.first_unallocated;
  rc = MDBX_SUCCESS;
  ctx->reserved = 0;
  ctx->cleaned_slot = 0;
  ctx->reused_slot = 0;
  ctx->amount = 0;
  ctx->fill_idx = ~0u;
  ctx->cleaned_id = 0;
  ctx->rid = txn->tw.gc.last_reclaimed;
  while (true) {
    /* Come back here after each Put() in case retired-list changed */
    TRACE("%s", " >> continue");

    tASSERT(txn, pnl_check_allocated(txn->tw.relist, txn->geo.first_unallocated - MDBX_ENABLE_REFUND));
    MDBX_val key, data;
    if (is_lifo(txn)) {
      if (ctx->cleaned_slot < (txn->tw.gc.reclaimed ? MDBX_PNL_GETSIZE(txn->tw.gc.reclaimed) : 0)) {
        ctx->reserved = 0;
        ctx->cleaned_slot = 0;
        ctx->reused_slot = 0;
        ctx->fill_idx = ~0u;
        /* LY: cleanup reclaimed records. */
        do {
          ctx->cleaned_id = txn->tw.gc.reclaimed[++ctx->cleaned_slot];
          tASSERT(txn, ctx->cleaned_slot > 0 && ctx->cleaned_id <= env->lck->cached_oldest.weak);
          key.iov_base = &ctx->cleaned_id;
          key.iov_len = sizeof(ctx->cleaned_id);
          rc = cursor_seek(&ctx->cursor, &key, nullptr, MDBX_SET).err;
          if (rc == MDBX_NOTFOUND)
            continue;
          if (unlikely(rc != MDBX_SUCCESS))
            goto bailout;
          rc = prepare_backlog(txn, ctx);
          if (unlikely(rc != MDBX_SUCCESS))
            goto bailout;
          tASSERT(txn, ctx->cleaned_id <= env->lck->cached_oldest.weak);
          TRACE("%s: cleanup-reclaimed-id [%zu]%" PRIaTXN, dbg_prefix(ctx), ctx->cleaned_slot, ctx->cleaned_id);
          tASSERT(txn, *txn->cursors == &ctx->cursor);
          rc = cursor_del(&ctx->cursor, 0);
          if (unlikely(rc != MDBX_SUCCESS))
            goto bailout;
        } while (ctx->cleaned_slot < MDBX_PNL_GETSIZE(txn->tw.gc.reclaimed));
        txl_sort(txn->tw.gc.reclaimed);
      }
    } else {
      /* Удаляем оставшиеся вынутые из GC записи. */
      while (txn->tw.gc.last_reclaimed && ctx->cleaned_id <= txn->tw.gc.last_reclaimed) {
        rc = outer_first(&ctx->cursor, &key, nullptr);
        if (rc == MDBX_NOTFOUND) {
          ctx->cleaned_id = txn->tw.gc.last_reclaimed + 1;
          ctx->rid = txn->tw.gc.last_reclaimed;
          ctx->reserved = 0;
          ctx->reused_slot = 0;
          break;
        }
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
        if (!MDBX_DISABLE_VALIDATION && unlikely(key.iov_len != sizeof(txnid_t))) {
          ERROR("%s/%d: %s %u", "MDBX_CORRUPTED", MDBX_CORRUPTED, "invalid GC-key size", (unsigned)key.iov_len);
          rc = MDBX_CORRUPTED;
          goto bailout;
        }
        if (ctx->rid != ctx->cleaned_id) {
          ctx->rid = ctx->cleaned_id;
          ctx->reserved = 0;
          ctx->reused_slot = 0;
        }
        ctx->cleaned_id = unaligned_peek_u64(4, key.iov_base);
        if (ctx->cleaned_id > txn->tw.gc.last_reclaimed)
          break;
        rc = prepare_backlog(txn, ctx);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
        tASSERT(txn, ctx->cleaned_id <= txn->tw.gc.last_reclaimed);
        tASSERT(txn, ctx->cleaned_id <= env->lck->cached_oldest.weak);
        TRACE("%s: cleanup-reclaimed-id %" PRIaTXN, dbg_prefix(ctx), ctx->cleaned_id);
        tASSERT(txn, *txn->cursors == &ctx->cursor);
        rc = cursor_del(&ctx->cursor, 0);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
      }
    }

    tASSERT(txn, pnl_check_allocated(txn->tw.relist, txn->geo.first_unallocated - MDBX_ENABLE_REFUND));
    tASSERT(txn, dpl_check(txn));
    if (AUDIT_ENABLED()) {
      rc = audit_ex(txn, ctx->retired_stored, false);
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;
    }

    /* return suitable into unallocated space */
    if (txn_refund(txn)) {
      tASSERT(txn, pnl_check_allocated(txn->tw.relist, txn->geo.first_unallocated - MDBX_ENABLE_REFUND));
      if (AUDIT_ENABLED()) {
        rc = audit_ex(txn, ctx->retired_stored, false);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
      }
    }

    if (txn->tw.loose_pages) {
      /* put loose pages into the reclaimed- or retired-list */
      rc = gcu_loose(txn, ctx);
      if (unlikely(rc != MDBX_SUCCESS)) {
        if (rc == MDBX_RESULT_TRUE)
          continue;
        goto bailout;
      }
      tASSERT(txn, txn->tw.loose_pages == 0);
    }

    if (unlikely(ctx->reserved > MDBX_PNL_GETSIZE(txn->tw.relist)) &&
        (ctx->loop < 5 || ctx->reserved - MDBX_PNL_GETSIZE(txn->tw.relist) > env->maxgc_large1page / 2)) {
      TRACE("%s: reclaimed-list changed %zu -> %zu, retry", dbg_prefix(ctx), ctx->amount,
            MDBX_PNL_GETSIZE(txn->tw.relist));
      ctx->reserve_adj += ctx->reserved - MDBX_PNL_GETSIZE(txn->tw.relist);
      goto retry;
    }
    ctx->amount = MDBX_PNL_GETSIZE(txn->tw.relist);

    if (ctx->retired_stored < MDBX_PNL_GETSIZE(txn->tw.retired_pages)) {
      /* store retired-list into GC */
      rc = gcu_retired(txn, ctx);
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;
      continue;
    }

    tASSERT(txn, pnl_check_allocated(txn->tw.relist, txn->geo.first_unallocated - MDBX_ENABLE_REFUND));
    tASSERT(txn, txn->tw.loose_count == 0);

    TRACE("%s", " >> reserving");
    if (AUDIT_ENABLED()) {
      rc = audit_ex(txn, ctx->retired_stored, false);
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;
    }
    const size_t left = ctx->amount - ctx->reserved - ctx->reserve_adj;
    TRACE("%s: amount %zu, reserved %zd, reserve_adj %zu, left %zd, "
          "lifo-reclaimed-slots %zu, "
          "reused-gc-slots %zu",
          dbg_prefix(ctx), ctx->amount, ctx->reserved, ctx->reserve_adj, left,
          txn->tw.gc.reclaimed ? MDBX_PNL_GETSIZE(txn->tw.gc.reclaimed) : 0, ctx->reused_slot);
    if (0 >= (intptr_t)left)
      break;

    const rid_t rid_result = get_rid_for_reclaimed(txn, ctx, left);
    if (unlikely(!rid_result.rid)) {
      rc = rid_result.err;
      if (likely(rc == MDBX_SUCCESS))
        continue;
      if (likely(rc == MDBX_RESULT_TRUE))
        goto retry;
      goto bailout;
    }
    tASSERT(txn, rid_result.err == MDBX_SUCCESS);
    const txnid_t reservation_gc_id = rid_result.rid;

    size_t chunk = left;
    if (unlikely(left > env->maxgc_large1page)) {
      const size_t avail_gc_slots = txn->tw.gc.reclaimed ? MDBX_PNL_GETSIZE(txn->tw.gc.reclaimed) - ctx->reused_slot + 1
                                    : (ctx->rid < INT16_MAX) ? (size_t)ctx->rid
                                                             : INT16_MAX;
      if (likely(avail_gc_slots > 1)) {
#if MDBX_ENABLE_BIGFOOT
        chunk = env->maxgc_large1page;
        if (avail_gc_slots < INT16_MAX && unlikely(left > env->maxgc_large1page * avail_gc_slots))
          /* TODO: Можно смотреть последовательности какой длины есть в relist
           *       и пробовать нарезать куски соответствующего размера.
           *       Смысл в том, чтобы не дробить последовательности страниц,
           *       а использовать целиком. */
          chunk = env->maxgc_large1page + left / (env->maxgc_large1page * avail_gc_slots) * env->maxgc_large1page;
#else
        if (chunk < env->maxgc_large1page * 2)
          chunk /= 2;
        else {
          const size_t prefer_max_scatter = 257;
          const size_t threshold =
              env->maxgc_large1page * ((avail_gc_slots < prefer_max_scatter) ? avail_gc_slots : prefer_max_scatter);
          if (left < threshold)
            chunk = env->maxgc_large1page;
          else {
            const size_t tail = left - threshold + env->maxgc_large1page + 1;
            size_t span = 1;
            size_t avail = ((pgno2bytes(env, span) - PAGEHDRSZ) / sizeof(pgno_t)) /* - 1 + span */;
            if (tail > avail) {
              for (size_t i = ctx->amount - span; i > 0; --i) {
                if (MDBX_PNL_ASCENDING ? (txn->tw.relist[i] + span)
                                       : (txn->tw.relist[i] - span) == txn->tw.relist[i + span]) {
                  span += 1;
                  avail = ((pgno2bytes(env, span) - PAGEHDRSZ) / sizeof(pgno_t)) - 1 + span;
                  if (avail >= tail)
                    break;
                }
              }
            }

            chunk = (avail >= tail)                                                     ? tail - span
                    : (avail_gc_slots > 3 && ctx->reused_slot < prefer_max_scatter - 3) ? avail - span
                                                                                        : tail;
          }
        }
#endif /* MDBX_ENABLE_BIGFOOT */
      }
    }
    tASSERT(txn, chunk > 0);

    TRACE("%s: gc_rid %" PRIaTXN ", reused_gc_slot %zu, reservation-id "
          "%" PRIaTXN,
          dbg_prefix(ctx), ctx->rid, ctx->reused_slot, reservation_gc_id);

    TRACE("%s: chunk %zu, gc-per-ovpage %u", dbg_prefix(ctx), chunk, env->maxgc_large1page);

    tASSERT(txn, reservation_gc_id <= env->lck->cached_oldest.weak);
    if (unlikely(reservation_gc_id < MIN_TXNID ||
                 reservation_gc_id > atomic_load64(&env->lck->cached_oldest, mo_Relaxed))) {
      ERROR("** internal error (reservation_gc_id %" PRIaTXN ")", reservation_gc_id);
      rc = MDBX_PROBLEM;
      goto bailout;
    }

    tASSERT(txn, reservation_gc_id >= MIN_TXNID && reservation_gc_id <= MAX_TXNID);
    key.iov_len = sizeof(reservation_gc_id);
    key.iov_base = (void *)&reservation_gc_id;
    data.iov_len = (chunk + 1) * sizeof(pgno_t);
    TRACE("%s: reserve %zu [%zu...%zu) @%" PRIaTXN, dbg_prefix(ctx), chunk, ctx->reserved + 1,
          ctx->reserved + chunk + 1, reservation_gc_id);
    prepare_backlog(txn, ctx);
    rc = cursor_put(&ctx->cursor, &key, &data, MDBX_RESERVE | MDBX_NOOVERWRITE);
    tASSERT(txn, pnl_check_allocated(txn->tw.relist, txn->geo.first_unallocated - MDBX_ENABLE_REFUND));
    if (unlikely(rc != MDBX_SUCCESS))
      goto bailout;

    zeroize_reserved(env, data);
    ctx->reserved += chunk;
    TRACE("%s: reserved %zu (+%zu), continue", dbg_prefix(ctx), ctx->reserved, chunk);

    continue;
  }

  tASSERT(txn, ctx->cleaned_slot == (txn->tw.gc.reclaimed ? MDBX_PNL_GETSIZE(txn->tw.gc.reclaimed) : 0));

  TRACE("%s", " >> filling");
  /* Fill in the reserved records */
  size_t excess_slots = 0;
  ctx->fill_idx = txn->tw.gc.reclaimed ? MDBX_PNL_GETSIZE(txn->tw.gc.reclaimed) - ctx->reused_slot : ctx->reused_slot;
  rc = MDBX_SUCCESS;
  tASSERT(txn, pnl_check_allocated(txn->tw.relist, txn->geo.first_unallocated - MDBX_ENABLE_REFUND));
  tASSERT(txn, dpl_check(txn));
  if (ctx->amount) {
    MDBX_val key, data;
    key.iov_len = data.iov_len = 0;
    key.iov_base = data.iov_base = nullptr;

    size_t left = ctx->amount, excess = 0;
    if (txn->tw.gc.reclaimed == nullptr) {
      tASSERT(txn, is_lifo(txn) == 0);
      rc = outer_first(&ctx->cursor, &key, &data);
      if (unlikely(rc != MDBX_SUCCESS)) {
        if (rc != MDBX_NOTFOUND)
          goto bailout;
      }
    } else {
      tASSERT(txn, is_lifo(txn) != 0);
    }

    while (true) {
      txnid_t fill_gc_id;
      TRACE("%s: left %zu of %zu", dbg_prefix(ctx), left, MDBX_PNL_GETSIZE(txn->tw.relist));
      if (txn->tw.gc.reclaimed == nullptr) {
        tASSERT(txn, is_lifo(txn) == 0);
        fill_gc_id = key.iov_base ? unaligned_peek_u64(4, key.iov_base) : MIN_TXNID;
        if (ctx->fill_idx == 0 || fill_gc_id > txn->tw.gc.last_reclaimed) {
          if (!left)
            break;
          NOTICE("** restart: reserve depleted (fill_idx %zu, fill_id %" PRIaTXN " > last_reclaimed %" PRIaTXN
                 ", left %zu",
                 ctx->fill_idx, fill_gc_id, txn->tw.gc.last_reclaimed, left);
          ctx->reserve_adj = (ctx->reserve_adj > left) ? ctx->reserve_adj - left : 0;
          goto retry;
        }
        ctx->fill_idx -= 1;
      } else {
        tASSERT(txn, is_lifo(txn) != 0);
        if (ctx->fill_idx >= MDBX_PNL_GETSIZE(txn->tw.gc.reclaimed)) {
          if (!left)
            break;
          NOTICE("** restart: reserve depleted (fill_idx %zu >= "
                 "gc.reclaimed %zu, left %zu",
                 ctx->fill_idx, MDBX_PNL_GETSIZE(txn->tw.gc.reclaimed), left);
          ctx->reserve_adj = (ctx->reserve_adj > left) ? ctx->reserve_adj - left : 0;
          goto retry;
        }
        ctx->fill_idx += 1;
        fill_gc_id = txn->tw.gc.reclaimed[ctx->fill_idx];
        TRACE("%s: seek-reservation @%" PRIaTXN " at gc.reclaimed[%zu]", dbg_prefix(ctx), fill_gc_id, ctx->fill_idx);
        key.iov_base = &fill_gc_id;
        key.iov_len = sizeof(fill_gc_id);
        rc = cursor_seek(&ctx->cursor, &key, &data, MDBX_SET_KEY).err;
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
      }
      tASSERT(txn, ctx->cleaned_slot == (txn->tw.gc.reclaimed ? MDBX_PNL_GETSIZE(txn->tw.gc.reclaimed) : 0));
      tASSERT(txn, fill_gc_id > 0 && fill_gc_id <= env->lck->cached_oldest.weak);
      key.iov_base = &fill_gc_id;
      key.iov_len = sizeof(fill_gc_id);

      tASSERT(txn, data.iov_len >= sizeof(pgno_t) * 2);
      size_t chunk = data.iov_len / sizeof(pgno_t) - 1;
      if (unlikely(chunk > left)) {
        const size_t delta = chunk - left;
        excess += delta;
        TRACE("%s: chunk %zu > left %zu, @%" PRIaTXN, dbg_prefix(ctx), chunk, left, fill_gc_id);
        if (!left) {
          excess_slots += 1;
          goto next;
        }
        if ((ctx->loop < 5 && delta > (ctx->loop / 2)) || delta > env->maxgc_large1page)
          data.iov_len = (left + 1) * sizeof(pgno_t);
        chunk = left;
      }
      rc = cursor_put(&ctx->cursor, &key, &data, MDBX_CURRENT | MDBX_RESERVE);
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;
      zeroize_reserved(env, data);

      if (unlikely(txn->tw.loose_count || ctx->amount != MDBX_PNL_GETSIZE(txn->tw.relist))) {
        NOTICE("** restart: reclaimed-list changed (%zu -> %zu, loose +%zu)", ctx->amount,
               MDBX_PNL_GETSIZE(txn->tw.relist), txn->tw.loose_count);
        if (ctx->loop < 5 || (ctx->loop > 10 && (ctx->loop & 1)))
          goto retry_clean_adj;
        goto retry;
      }

      if (unlikely(txn->tw.gc.reclaimed ? ctx->cleaned_slot < MDBX_PNL_GETSIZE(txn->tw.gc.reclaimed)
                                        : ctx->cleaned_id < txn->tw.gc.last_reclaimed)) {
        NOTICE("%s", "** restart: reclaimed-slots changed");
        goto retry;
      }
      if (unlikely(ctx->retired_stored != MDBX_PNL_GETSIZE(txn->tw.retired_pages))) {
        tASSERT(txn, ctx->retired_stored < MDBX_PNL_GETSIZE(txn->tw.retired_pages));
        NOTICE("** restart: retired-list growth (%zu -> %zu)", ctx->retired_stored,
               MDBX_PNL_GETSIZE(txn->tw.retired_pages));
        goto retry;
      }

      pgno_t *dst = data.iov_base;
      *dst++ = (pgno_t)chunk;
      pgno_t *src = MDBX_PNL_BEGIN(txn->tw.relist) + left - chunk;
      memcpy(dst, src, chunk * sizeof(pgno_t));
      pgno_t *from = src, *to = src + chunk;
      TRACE("%s: fill %zu [ %zu:%" PRIaPGNO "...%zu:%" PRIaPGNO "] @%" PRIaTXN, dbg_prefix(ctx), chunk,
            from - txn->tw.relist, from[0], to - txn->tw.relist, to[-1], fill_gc_id);

      left -= chunk;
      if (AUDIT_ENABLED()) {
        rc = audit_ex(txn, ctx->retired_stored + ctx->amount - left, true);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
      }

    next:

      if (txn->tw.gc.reclaimed == nullptr) {
        tASSERT(txn, is_lifo(txn) == 0);
        rc = outer_next(&ctx->cursor, &key, &data, MDBX_NEXT);
        if (unlikely(rc != MDBX_SUCCESS)) {
          if (rc == MDBX_NOTFOUND && !left) {
            rc = MDBX_SUCCESS;
            break;
          }
          goto bailout;
        }
      } else {
        tASSERT(txn, is_lifo(txn) != 0);
      }
    }

    if (excess) {
      size_t n = excess, adj = excess;
      while (n >= env->maxgc_large1page)
        adj -= n /= env->maxgc_large1page;
      ctx->reserve_adj += adj;
      TRACE("%s: extra %zu reserved space, adj +%zu (%zu)", dbg_prefix(ctx), excess, adj, ctx->reserve_adj);
    }
  }

  tASSERT(txn, rc == MDBX_SUCCESS);
  if (unlikely(txn->tw.loose_count != 0 || ctx->amount != MDBX_PNL_GETSIZE(txn->tw.relist))) {
    NOTICE("** restart: got %zu loose pages (reclaimed-list %zu -> %zu)", txn->tw.loose_count, ctx->amount,
           MDBX_PNL_GETSIZE(txn->tw.relist));
    goto retry;
  }

  if (unlikely(excess_slots)) {
    const bool will_retry = ctx->loop < 5 || excess_slots > 1;
    NOTICE("** %s: reserve excess (excess-slots %zu, filled-slot %zu, adj %zu, "
           "loop %u)",
           will_retry ? "restart" : "ignore", excess_slots, ctx->fill_idx, ctx->reserve_adj, ctx->loop);
    if (will_retry)
      goto retry;
  }

  tASSERT(txn, txn->tw.gc.reclaimed == nullptr || ctx->cleaned_slot == MDBX_PNL_GETSIZE(txn->tw.gc.reclaimed));

bailout:
  txn->cursors[FREE_DBI] = ctx->cursor.next;

  MDBX_PNL_SETSIZE(txn->tw.relist, 0);
#if MDBX_ENABLE_PROFGC
  env->lck->pgops.gc_prof.wloops += (uint32_t)ctx->loop;
#endif /* MDBX_ENABLE_PROFGC */
  TRACE("<<< %u loops, rc = %d", ctx->loop, rc);
  return rc;
}
