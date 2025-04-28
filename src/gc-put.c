/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2025

#include "internals.h"

int gc_put_init(MDBX_txn *txn, gcu_t *ctx) {
  memset(ctx, 0, offsetof(gcu_t, ready4reuse));
  /* Размер куска помещающийся на одну отдельную "overflow" страницу, но с небольшим запасом сводобного места. */
  ctx->goodchunk = txn->env->maxgc_large1page - (txn->env->maxgc_large1page >> 4);
  rkl_init(&ctx->ready4reuse);
  rkl_init(&ctx->sequel);
#if MDBX_ENABLE_BIGFOOT
  ctx->bigfoot = txn->txnid;
#endif /* MDBX_ENABLE_BIGFOOT */
  return cursor_init(&ctx->cursor, txn, FREE_DBI);
}

void gc_put_destroy(gcu_t *ctx) {
  rkl_destroy(&ctx->ready4reuse);
  rkl_destroy(&ctx->sequel);
}

static size_t gc_chunk_pages(const MDBX_txn *txn, const size_t chunk) {
  return largechunk_npages(txn->env, gc_chunk_bytes(chunk));
}

static int gc_peekid(const MDBX_val *key, txnid_t *id) {
  if (likely(key->iov_len == sizeof(txnid_t))) {
    *id = unaligned_peek_u64(4, key->iov_base);
    return MDBX_SUCCESS;
  }
  ERROR("%s/%d: %s", "MDBX_CORRUPTED", MDBX_CORRUPTED, "invalid GC key-length");
  return MDBX_CORRUPTED;
}

#if MDBX_DEBUG_GCU
#pragma push_macro("LOG_ENABLED")
#undef LOG_ENABLED
#define LOG_ENABLED(LVL)                                                                                               \
  unlikely(MDBX_DEBUG_GCU > 2 || (ctx->loop > 1 && (MDBX_DEBUG_GCU > 1 || LVL < MDBX_LOG_EXTRA)) ||                    \
           LVL <= globals.loglevel)
#endif /* MDBX_DEBUG_GCU */

MDBX_NOTHROW_PURE_FUNCTION static bool is_lifo(const MDBX_txn *txn) {
  return (txn->env->flags & MDBX_LIFORECLAIM) != 0;
}

MDBX_NOTHROW_PURE_FUNCTION MDBX_MAYBE_UNUSED static inline const char *dbg_prefix(const gcu_t *ctx) {
  return is_lifo(ctx->cursor.txn) ? "    lifo" : "    fifo";
}

MDBX_MAYBE_UNUSED static void dbg_id(gcu_t *ctx, txnid_t id) {
#if MDBX_DEBUG_GCU
  if (ctx->dbg.prev) {
    if (ctx->dbg.prev != id - 1) {
      if (ctx->dbg.n)
        DEBUG_EXTRA_PRINT("-%" PRIaTXN, ctx->dbg.prev);
      if (id)
        DEBUG_EXTRA_PRINT(" %" PRIaTXN, id);
      ctx->dbg.n = 0;
    } else
      ctx->dbg.n += 1;
  } else {
    DEBUG_EXTRA_PRINT(" %" PRIaTXN, id);
    ctx->dbg.n = 0;
  }
  ctx->dbg.prev = id;
#else
  (void)ctx;
  (void)id;
#endif /* MDBX_DEBUG_GCU */
}

MDBX_MAYBE_UNUSED static void dbg_dump_ids(gcu_t *ctx) {
#if MDBX_DEBUG_GCU
  if (LOG_ENABLED(MDBX_LOG_EXTRA)) {
    DEBUG_EXTRA("%s", "GC:");
    if (ctx->cursor.tree->items) {
      cursor_couple_t couple;
      MDBX_val key;
      int err = cursor_init(&couple.outer, ctx->cursor.txn, FREE_DBI);
      if (err != MDBX_SUCCESS)
        ERROR("%s(), %d", "cursor_init", err);
      else
        err = outer_first(&couple.outer, &key, nullptr);

      txnid_t id;
      while (err == MDBX_SUCCESS) {
        err = gc_peekid(&key, &id);
        if (unlikely(err == MDBX_SUCCESS)) {
          dbg_id(ctx, id);
          if (id >= couple.outer.txn->env->gc.detent)
            break;
          err = outer_next(&couple.outer, &key, nullptr, MDBX_NEXT);
        }
      }
      dbg_id(ctx, 0);
      DEBUG_EXTRA_PRINT("%s\n", (id >= couple.outer.txn->env->gc.detent) ? "..." : "");
    } else
      DEBUG_EXTRA_PRINT("%s\n", " empty");

    DEBUG_EXTRA("%s", "ready4reuse:");
    if (rkl_empty(&ctx->ready4reuse))
      DEBUG_EXTRA_PRINT("%s\n", " empty");
    else {
      rkl_iter_t i = rkl_iterator(&ctx->ready4reuse, false);
      txnid_t id = rkl_turn(&i, false);
      while (id) {
        dbg_id(ctx, id);
        id = rkl_turn(&i, false);
      }
      dbg_id(ctx, 0);
      DEBUG_EXTRA_PRINT("%s\n", "");
    }

    DEBUG_EXTRA("%s", "comeback:");
    if (rkl_empty(&ctx->cursor.txn->wr.gc.comeback))
      DEBUG_EXTRA_PRINT("%s\n", " empty");
    else {
      rkl_iter_t i = rkl_iterator(&ctx->cursor.txn->wr.gc.comeback, false);
      txnid_t id = rkl_turn(&i, false);
      while (id) {
        dbg_id(ctx, id);
        id = rkl_turn(&i, false);
      }
      dbg_id(ctx, 0);
      DEBUG_EXTRA_PRINT("%s\n", "");
    }
  }
#else
  (void)ctx;
#endif /* MDBX_DEBUG_GCU */
}

MDBX_MAYBE_UNUSED MDBX_NOTHROW_PURE_FUNCTION static inline size_t gc_reclaimed_maxspan_chunk(MDBX_txn *txn,
                                                                                             gcu_t *ctx) {
  (void)ctx;
  /* Функция вычисляет размер куска списка возвращаемых/неиспользованных в GC страниц, который можно разместить
   * в последовательности смежных страниц, которая возможно есть в самом этом списке.
   *
   * С одной стороны, такое размещение позволяет обойтись меньшим кол-вом слотов при возврате страниц,
   * уменьшить кол-во итераций при резервировании, а также не дробить саму последовательность.
   *
   * Однако, при последующей переработке такая тактика допустима только при не-отложенной очистке GC. Иначе
   * последовательность смежных страниц останется в GC до завершения переработавшей эту запись транзакции. А затем, в
   * ходе обновления GC при фиксации транзакции, будет раздроблена или снова возвращена в GC. Таким образом, без
   * не-отложенной очистки, такая тактика способствует миграции последовательностей страниц внутрь структуры самой GC
   * делая его более громоздкой.
   *
   * Поэтому пока используем только при нехватке слотов. */
  const size_t maxspan = pnl_maxspan(txn->wr.repnl);
  tASSERT(txn, maxspan > 0);
  size_t start_lp = txn->env->maxgc_large1page /* начало набольшой страницы, с заголовком */;
  size_t tail_lp =
      ((maxspan - 1) << txn->env->ps2ln) / sizeof(txnid_t) /* продолжение большой страницы, без заголовка */;
  size_t pages4span = maxspan /* кол-во страниц для размещения*/;
  size_t chunk = start_lp + tail_lp - pages4span;
  TRACE("maxspan %zu, chunk %zu: %zu (start_lp) + %zu (tail_lp) - %zu (pages4span))", maxspan, chunk, start_lp, tail_lp,
        pages4span);
  return chunk;
}

static int gc_clean_stored_retired(MDBX_txn *txn, gcu_t *ctx) {
  int err = MDBX_SUCCESS;
  if (ctx->retired_stored) {
    do {
      MDBX_val key, val;
#if MDBX_ENABLE_BIGFOOT
      key.iov_base = &ctx->bigfoot;
#else
      key.iov_base = &txn->txnid;
#endif /* MDBX_ENABLE_BIGFOOT */
      key.iov_len = sizeof(txnid_t);
      const csr_t csr = cursor_seek(&ctx->cursor, &key, &val, MDBX_SET);
      if (csr.err == MDBX_SUCCESS && csr.exact) {
        ctx->retired_stored = 0;
        err = cursor_del(&ctx->cursor, 0);
        TRACE("== clear-4linear @%" PRIaTXN ", stockpile %zu, err %d", *(txnid_t *)key.iov_base, gc_stockpile(txn),
              err);
      } else
        err = (csr.err == MDBX_NOTFOUND) ? MDBX_SUCCESS : csr.err;
    }
#if MDBX_ENABLE_BIGFOOT
    while (!err && --ctx->bigfoot >= txn->txnid);
#else
    while (0);
#endif /* MDBX_ENABLE_BIGFOOT */
  }
  return err;
}

static int gc_touch(gcu_t *ctx) {
  tASSERT(ctx->cursor.txn, is_pointed(&ctx->cursor) || ctx->cursor.txn->dbs[FREE_DBI].leaf_pages == 0);
  MDBX_val key, val;
  key.iov_base = val.iov_base = nullptr;
  key.iov_len = sizeof(txnid_t);
  val.iov_len = MDBX_PNL_SIZEOF(ctx->cursor.txn->wr.retired_pages);
  ctx->cursor.flags |= z_gcu_preparation;
  int err = cursor_touch(&ctx->cursor, &key, &val);
  ctx->cursor.flags -= z_gcu_preparation;
  return err;
}

static inline int gc_reclaim_slot(MDBX_txn *txn, gcu_t *ctx) {
  (void)txn;
  return gc_alloc_ex(&ctx->cursor, 0, ALLOC_RESERVE | ALLOC_UNIMPORTANT).err;
}

static inline int gc_reserve4retired(MDBX_txn *txn, gcu_t *ctx, size_t sequence_length) {
  (void)txn;
  return gc_alloc_ex(&ctx->cursor, sequence_length, ALLOC_RESERVE | ALLOC_UNIMPORTANT).err;
}

static inline int gc_reserve4stockpile(MDBX_txn *txn, gcu_t *ctx) {
  (void)txn;
  return gc_alloc_ex(&ctx->cursor, 1, ALLOC_RESERVE | ALLOC_UNIMPORTANT).err;
}

static int gc_prepare_stockpile(MDBX_txn *txn, gcu_t *ctx, const size_t for_retired) {
  for (;;) {
    tASSERT(txn, is_pointed(&ctx->cursor) || txn->dbs[FREE_DBI].leaf_pages == 0);

    const size_t for_cow = txn->dbs[FREE_DBI].height;
    const size_t for_rebalance = for_cow + 1 + (txn->dbs[FREE_DBI].height + 1ul >= txn->dbs[FREE_DBI].branch_pages);
    const size_t for_tree_before_touch = for_cow + for_rebalance;
    const size_t for_tree_after_touch = for_rebalance;
    const size_t for_all_before_touch = for_retired + for_tree_before_touch;
    const size_t for_all_after_touch = for_retired + for_tree_after_touch;

    if (likely(for_retired < 2 && gc_stockpile(txn) > for_all_before_touch))
      return MDBX_SUCCESS;

    TRACE(">> retired-stored %zu, retired-left %zi, stockpile %zu, now-need %zu (4list %zu, "
          "4cow %zu, 4tree %zu)",
          ctx->retired_stored, MDBX_PNL_GETSIZE(txn->wr.retired_pages) - ctx->retired_stored, gc_stockpile(txn),
          for_all_before_touch, for_retired, for_cow, for_tree_before_touch);

    int err = gc_touch(ctx);
    TRACE("== after-touch, stockpile %zu, err %d", gc_stockpile(txn), err);

    if (!MDBX_ENABLE_BIGFOOT && unlikely(for_retired > 1) &&
        MDBX_PNL_GETSIZE(txn->wr.retired_pages) != ctx->retired_stored && err == MDBX_SUCCESS) {
      if (unlikely(ctx->retired_stored)) {
        err = gc_clean_stored_retired(txn, ctx);
        if (unlikely(err != MDBX_SUCCESS))
          return err;
        if (!ctx->retired_stored)
          continue;
      }
      err = gc_reserve4retired(txn, ctx, for_retired);
      TRACE("== after-4linear, stockpile %zu, err %d", gc_stockpile(txn), err);
      cASSERT(&ctx->cursor, gc_stockpile(txn) >= for_retired || err != MDBX_SUCCESS);
    }

    while (gc_stockpile(txn) < for_all_after_touch && err == MDBX_SUCCESS)
      err = gc_reserve4stockpile(txn, ctx);

    TRACE("<< stockpile %zu, err %d, gc: height %u, branch %zu, leaf %zu, large "
          "%zu, entries %zu",
          gc_stockpile(txn), err, txn->dbs[FREE_DBI].height, (size_t)txn->dbs[FREE_DBI].branch_pages,
          (size_t)txn->dbs[FREE_DBI].leaf_pages, (size_t)txn->dbs[FREE_DBI].large_pages,
          (size_t)txn->dbs[FREE_DBI].items);
    return (err != MDBX_NOTFOUND) ? err : MDBX_SUCCESS;
  }
}

static int gc_prepare_stockpile4update(MDBX_txn *txn, gcu_t *ctx) { return gc_prepare_stockpile(txn, ctx, 0); }

static int gc_prepare_stockpile4retired(MDBX_txn *txn, gcu_t *ctx) {
  const size_t retired_whole = MDBX_PNL_GETSIZE(txn->wr.retired_pages);
  const intptr_t retired_left = retired_whole - ctx->retired_stored;
  size_t for_retired = 0;
  if (retired_left > 0) {
    if (unlikely(!ctx->retired_stored)) {
      /* Make sure last page of GC is touched and on retired-list */
      int err = outer_last(&ctx->cursor, nullptr, nullptr);
      if (unlikely(err != MDBX_SUCCESS) && err != MDBX_NOTFOUND)
        return err;
      for_retired += 1;
    }
    if (MDBX_ENABLE_BIGFOOT) {
      const size_t per_branch_page = txn->env->maxgc_per_branch;
      for_retired += (retired_left + ctx->goodchunk - 1) / ctx->goodchunk;
      for (size_t entries = for_retired; entries > 1; for_retired += entries)
        entries = (entries + per_branch_page - 1) / per_branch_page;
    } else
      for_retired += largechunk_npages(txn->env, retired_whole);
  }

  return gc_prepare_stockpile(txn, ctx, for_retired);
}

static int gc_merge_loose(MDBX_txn *txn, gcu_t *ctx) {
  tASSERT(txn, txn->wr.loose_count > 0);
  /* Return loose page numbers to wr.repnl, though usually none are left at this point.
   * The pages themselves remain in dirtylist. */
  if (unlikely(!(txn->dbi_state[FREE_DBI] & DBI_DIRTY)) && txn->wr.loose_count < 3 + (unsigned)txn->dbs->height * 2) {
    /* Put loose page numbers in wr.retired_pages, since unreasonable to return ones to wr.repnl. */
    TRACE("%s: merge %zu loose-pages into %s-pages", dbg_prefix(ctx), txn->wr.loose_count, "retired");
    int err = pnl_need(&txn->wr.retired_pages, txn->wr.loose_count);
    if (unlikely(err != MDBX_SUCCESS))
      return err;
    for (page_t *lp = txn->wr.loose_pages; lp; lp = page_next(lp)) {
      pnl_append_prereserved(txn->wr.retired_pages, lp->pgno);
      MDBX_ASAN_UNPOISON_MEMORY_REGION(&page_next(lp), sizeof(page_t *));
      VALGRIND_MAKE_MEM_DEFINED(&page_next(lp), sizeof(page_t *));
    }
  } else {
    /* Room for loose pages + temp PNL with same */
    TRACE("%s: merge %zu loose-pages into %s-pages", dbg_prefix(ctx), txn->wr.loose_count, "reclaimed");
    int err = pnl_need(&txn->wr.repnl, 2 * txn->wr.loose_count + 2);
    if (unlikely(err != MDBX_SUCCESS))
      return err;
    pnl_t loose = txn->wr.repnl + MDBX_PNL_ALLOCLEN(txn->wr.repnl) - txn->wr.loose_count - 1;
    size_t count = 0;
    for (page_t *lp = txn->wr.loose_pages; lp; lp = page_next(lp)) {
      tASSERT(txn, lp->flags == P_LOOSE);
      loose[++count] = lp->pgno;
      MDBX_ASAN_UNPOISON_MEMORY_REGION(&page_next(lp), sizeof(page_t *));
      VALGRIND_MAKE_MEM_DEFINED(&page_next(lp), sizeof(page_t *));
    }
    tASSERT(txn, count == txn->wr.loose_count);
    MDBX_PNL_SETSIZE(loose, count);
    pnl_sort(loose, txn->geo.first_unallocated);
    pnl_merge(txn->wr.repnl, loose);
  }

  /* filter-out list of dirty-pages from loose-pages */
  dpl_t *const dl = txn->wr.dirtylist;
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
    tASSERT(txn, txn->wr.loose_count == dl->length - w);
    dl->sorted -= sorted_out;
    tASSERT(txn, dl->sorted <= w);
    dpl_setlen(dl, w);
    dl->pages_including_loose -= txn->wr.loose_count;
    txn->wr.dirtyroom += txn->wr.loose_count;
    tASSERT(txn, txn->wr.dirtyroom + txn->wr.dirtylist->length ==
                     (txn->parent ? txn->parent->wr.dirtyroom : txn->env->options.dp_limit));
  } else {
    tASSERT(txn, (txn->flags & MDBX_WRITEMAP) != 0 && !MDBX_AVOID_MSYNC);
  }
  txn->wr.loose_pages = nullptr;
  txn->wr.loose_count = 0;
#if MDBX_ENABLE_REFUND
  txn->wr.loose_refund_wl = 0;
#endif /* MDBX_ENABLE_REFUND */
  return MDBX_SUCCESS;
}

static int gc_store_retired(MDBX_txn *txn, gcu_t *ctx) {
  int err;
  MDBX_val key, data;

#if MDBX_ENABLE_BIGFOOT
  size_t retired_before;
  bool should_retry;
  do {
    if (ctx->bigfoot > txn->txnid) {
      err = gc_clean_stored_retired(txn, ctx);
      if (unlikely(err != MDBX_SUCCESS))
        return err;
      tASSERT(txn, ctx->bigfoot <= txn->txnid);
    }

    err = gc_prepare_stockpile4retired(txn, ctx);
    if (unlikely(err != MDBX_SUCCESS))
      return err;

    pnl_sort(txn->wr.retired_pages, txn->geo.first_unallocated);
    retired_before = MDBX_PNL_GETSIZE(txn->wr.retired_pages);
    should_retry = false;
    ctx->retired_stored = 0;
    ctx->bigfoot = txn->txnid;
    do {
      if (ctx->retired_stored) {
        err = gc_prepare_stockpile4retired(txn, ctx);
        if (unlikely(err != MDBX_SUCCESS))
          return err;
      }
      key.iov_len = sizeof(txnid_t);
      key.iov_base = &ctx->bigfoot;
      const size_t left_before = retired_before - ctx->retired_stored;
      const size_t chunk_hi = ((left_before | 3) > ctx->goodchunk && ctx->bigfoot < (MAX_TXNID - UINT32_MAX))
                                  ? ctx->goodchunk
                                  : (left_before | 3);
      data.iov_len = gc_chunk_bytes(chunk_hi);
      err = cursor_put(&ctx->cursor, &key, &data, MDBX_RESERVE);
      if (unlikely(err != MDBX_SUCCESS))
        return err;

#if MDBX_DEBUG && (defined(ENABLE_MEMCHECK) || defined(__SANITIZE_ADDRESS__))
      /* Для предотвращения предупреждения Valgrind из mdbx_dump_val() вызванное через макрос DVAL_DEBUG() на выходе из
       * cursor_seek(MDBX_SET_KEY), которая вызывается как выше в цикле очистки, так и ниже в цикле заполнения
       * зарезервированных элементов. */
      memset(data.iov_base, 0xBB, data.iov_len);
#endif /* MDBX_DEBUG && (ENABLE_MEMCHECK || __SANITIZE_ADDRESS__) */

      const size_t retired_after = MDBX_PNL_GETSIZE(txn->wr.retired_pages);
      const size_t left_after = retired_after - ctx->retired_stored;
      const size_t chunk = (left_after < chunk_hi) ? left_after : chunk_hi;
      should_retry = retired_before != retired_after && chunk < retired_after;
      if (likely(!should_retry)) {
        const size_t at = (is_lifo(txn) == MDBX_PNL_ASCENDING) ? left_before - chunk : ctx->retired_stored;
        pgno_t *const begin = txn->wr.retired_pages + at;
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
              ctx->bigfoot, (unsigned)(ctx->bigfoot - txn->txnid), chunk, at, at + chunk, retired_before);
      }
      ctx->retired_stored += chunk;
    } while (ctx->retired_stored < MDBX_PNL_GETSIZE(txn->wr.retired_pages) && (++ctx->bigfoot, true));
  } while (unlikely(should_retry));
#else
  /* Write to last page of GC */
  key.iov_len = sizeof(txnid_t);
  key.iov_base = &txn->txnid;
  do {
    gc_prepare_stockpile4retired(txn, ctx);
    data.iov_len = MDBX_PNL_SIZEOF(txn->wr.retired_pages);
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

    /* Retry if wr.retired_pages[] grew during the Put() */
  } while (data.iov_len < MDBX_PNL_SIZEOF(txn->wr.retired_pages));

  ctx->retired_stored = MDBX_PNL_GETSIZE(txn->wr.retired_pages);
  pnl_sort(txn->wr.retired_pages, txn->geo.first_unallocated);
  tASSERT(txn, data.iov_len == MDBX_PNL_SIZEOF(txn->wr.retired_pages));
  memcpy(data.iov_base, txn->wr.retired_pages, data.iov_len);

  TRACE("%s: put-retired #%zu @ %" PRIaTXN, dbg_prefix(ctx), ctx->retired_stored, txn->txnid);
#endif /* MDBX_ENABLE_BIGFOOT */
  if (MDBX_DEBUG_GCU < 2 && LOG_ENABLED(MDBX_LOG_EXTRA)) {
    size_t i = ctx->retired_stored;
    DEBUG_EXTRA("txn %" PRIaTXN " root %" PRIaPGNO " num %zu, retired-PNL", txn->txnid, txn->dbs[FREE_DBI].root, i);
    for (; i; i--)
      DEBUG_EXTRA_PRINT(" %" PRIaPGNO, txn->wr.retired_pages[i]);
    DEBUG_EXTRA_PRINT("%s\n", ".");
  }
  return MDBX_SUCCESS;
}

static int gc_remove_rkl(MDBX_txn *txn, gcu_t *ctx, rkl_t *rkl) {
  while (!rkl_empty(rkl)) {
    txnid_t id = rkl_edge(rkl, is_lifo(txn));
    if (ctx->gc_first == id)
      ctx->gc_first = 0;
    tASSERT(txn, id <= txn->env->lck->cached_oldest.weak);
    MDBX_val key = {.iov_base = &id, .iov_len = sizeof(id)};
    int err = cursor_seek(&ctx->cursor, &key, nullptr, MDBX_SET).err;
    tASSERT(txn, id == rkl_edge(rkl, is_lifo(txn)));
    if (err == MDBX_NOTFOUND) {
      err = rkl_push(&ctx->ready4reuse, rkl_pop(rkl, is_lifo(txn)), false);
      WARNING("unexpected %s for gc-id %" PRIaTXN ", ignore and continue, push-err %d", "MDBX_NOTFOUND", id, err);
      if (unlikely(MDBX_IS_ERROR(err)))
        return err;
      continue;
    }
    if (unlikely(err != MDBX_SUCCESS))
      return err;

    err = gc_prepare_stockpile4update(txn, ctx);
    if (unlikely(err != MDBX_SUCCESS))
      return err;
    if (unlikely(id != rkl_edge(rkl, is_lifo(txn)))) {
      TRACE("id %" PRIaTXN " not at edge, continue", id);
      continue;
    }
    err = cursor_del(&ctx->cursor, 0);
    if (unlikely(err != MDBX_SUCCESS))
      return err;
    ENSURE(txn->env, id == rkl_pop(rkl, is_lifo(txn)));
    tASSERT(txn, id <= txn->env->lck->cached_oldest.weak);
    err = rkl_push(&ctx->ready4reuse, id, false);
    if (unlikely(err != MDBX_SUCCESS))
      return err;
    TRACE("id %" PRIaTXN " cleared and moved to ready4reuse", id);
  }
  return MDBX_SUCCESS;
}

static inline int gc_clear_reclaimed(MDBX_txn *txn, gcu_t *ctx) {
  return gc_remove_rkl(txn, ctx, &txn->wr.gc.reclaimed);
}

static inline int gc_clear_returned(MDBX_txn *txn, gcu_t *ctx) {
  ctx->return_reserved_lo = 0;
  ctx->return_reserved_hi = 0;
  return gc_remove_rkl(txn, ctx, &txn->wr.gc.comeback);
}

static int gc_push_sequel(MDBX_txn *txn, gcu_t *ctx, txnid_t id) {
  tASSERT(txn, id > 0 && id < txn->env->gc.detent);
  tASSERT(txn, !rkl_contain(&txn->wr.gc.comeback, id) && !rkl_contain(&ctx->ready4reuse, id));
  TRACE("id %" PRIaTXN ", return-left %zi", id, ctx->return_left);
  int err = rkl_push(&ctx->sequel, id, false);
  if (unlikely(err != MDBX_SUCCESS)) {
    if (err == MDBX_RESULT_TRUE) {
      ERROR("%s/%d: %s", "MDBX_PROBLEM", MDBX_PROBLEM, "unexpected duplicate(s) during rkl-push");
      err = MDBX_PROBLEM;
    }
    return err;
  }
  ctx->return_left -= ctx->goodchunk;
  return (ctx->return_left <= 0) ? MDBX_RESULT_TRUE : MDBX_RESULT_FALSE;
}

/* Строит гистограмму длин последовательностей соседствующих/примыкающих страниц */
static void gc_dense_hist(MDBX_txn *txn, gcu_t *ctx) {
  memset(&ctx->dense_histogram, 0, sizeof(ctx->dense_histogram));
  size_t seqlen = 0, seqmax = 1;
  for (size_t i = 2; i <= MDBX_PNL_GETSIZE(txn->wr.repnl); ++i) {
    seqlen += 1;
    if (seqlen == ARRAY_LENGTH(ctx->dense_histogram.array) ||
        !MDBX_PNL_CONTIGUOUS(txn->wr.repnl[i - 1], txn->wr.repnl[i], 1)) {
      ctx->dense_histogram.array[seqlen - 1] += 1;
      seqmax = (seqmax >= seqlen) ? seqmax : seqlen;
      seqlen = 0;
    }
  }
  ctx->dense_histogram.array[seqlen] += 1;
  ctx->dense_histogram.end = (unsigned)((seqmax > seqlen) ? seqmax : seqlen + 1);
}

/* Оптимальным решением является использование всех доступных слотов/идентификаторов, при максимальном использовании
 * последовательностей длиной ближе к целевой средней длине, необходимой для размещения всех возвращаемых страниц.
 *
 * Если последовательностей нужной или большей длины хватает, то достаточно просто выполнить соответствующую нарезку.
 * Иначе поиск решения можно рассматривать как необходимую замену в наборе коротких (в том числе нулевых)
 * последовательностей/кусков более длинными. Сложность в том, что нужно учитывать возможность разбиения/деления длинных
 * последовательностей на несколько более коротких.
 *
 * Поэтому алгоритмически поиск решения выглядит как попытка сначала нарезать N кусков по L, а в случае неуспеха
 * попробовать комбинации X=1..N-1 кусков по L+1 и Y=N-X кусков длины L и меньше (при достижении объёма/суммы),
 * затем комбинаций X=1..N-1 кусков по L+2, Y=0..N-X кусков длины L-1 и Z=N-(X+Y) кусков длины L и меньше:
 *  - a=0..(V/(L+1)) кусков L+1, плюс хвост N-a длиной до L включительно;
 *  - b=0..(V/(L+2)) кусков L+2, a=0..(V/(L+1)) кусков L+1, плюс хвост N-b-a длиной до L включительно;
 *  - c=0..(V/(L+3)) кусков L+3, b=0..(V/(L+2)) кусков L+2, a=0..(V/(L+1)) кусков L+1,
 *    плюс хвост N-c-b-a длиной до L включительно;
 *  - и т.д.
 *
 * 1. начинаем с максимальной длины из гистограммы и спускаемся до L
 *    для каждого уровня начинаем с 0 кусков и одного из условий:
 *     - исчерпание/отсутствие кусков нужной длины,
 *     - либо до достижения объёма (что должно привести к возврату решения);
 * 2. сначала спускаемся рекурсивно вглубь, затем идем двоичным поиском 0 --> hi на каждом уровне;
 * 3. поиск/выделение кусков и восстановление/откат:
 *     - каждое выделение может быть дробным, т.е. образовывать осколки меньшего размера, которые могут использовать
 *       последующие шаги;
 *     - на каждом уровне нужна своя актуальная гистограмма;
 *     - два варианта: локальная копия гистограммы, либо "дельта" для отката
 *     - с "дельтой" очень много возни, в том числе условных переходов,
 *       поэтому проще делать локальные копии и целиком их откатывать.
 *
 * Максимальная потребность памяти на стеке sizeof(pgno_t)*L*L, где L = максимальная длина последовательности
 * учитываемой в гистограмме. Для L=31 получается порядка 4 Кб на стеке, что представляется допустимым, а отслеживание
 * более длинных последовательностей не представляется рациональным.
 *
 * Сложность можно оценить как O(H*N*log(N)), либо как O(H*V*log(N)), где:
 *  - H = высота гистограммы,
 *  - N = количество имеющихся слотов/идентификаторов,
 *  - V = объем/количество не помещающихся номеров страниц. */

typedef struct sr_state {
  unsigned left_slots;
  pgno_t left_volume;
  gc_dense_histogram_t hist;
} sr_state_t;

/* Пытается отъесть n кусков длиной len, двигаясь по гистограмме от больших элементов к меньшим. */
static bool consume_stack(sr_state_t *const st, const size_t len, size_t n) {
  assert(len > 1 && n > 0);
  while (st->hist.end >= len) {
    if (st->hist.array[st->hist.end - 1] < 1)
      st->hist.end -= 1;
    else {
      if (st->hist.end > len)
        st->hist.array[st->hist.end - len - 1] += 1;
      st->hist.array[st->hist.end - 1] -= 1;
      if (--n == 0)
        return true;
    }
  }
  return false;
}

typedef struct sr_context {
  /* расход страниц / ёмкость кусков */
  pgno_t first_page, other_pages;
  /* длина последовательностей смежных страниц, при нарезке на куски соответствующей длины, имеющихся
   * слотов/идентификаторов хватит для размещения возвращаемых страниц. Нарезать куски большего размера, есть смысл
   * только если недостаточно последовательностей такой длины (с учетом более длинных, в том числе кратно длиннее). */
  pgno_t factor;
  /* результирующее решение */
  gc_dense_histogram_t *solution;
} sr_context_t;

/* Пытается покрыть остаток объёма и слотов, кусками длиной не более factor,
 * двигаясь по гистограмме от больших элементов к меньшим. */
static bool consume_remaining(const sr_context_t *const ct, sr_state_t *const st, size_t len) {
  pgno_t *const solution = ct->solution->array;
  while (len > ct->factor)
    solution[--len] = 0;
  solution[len - 1] = 0;
  if (unlikely(0 >= (int)st->left_volume))
    goto done;

  size_t per_chunk = ct->first_page + ct->other_pages * (len - 1);
  while (st->hist.end > 0 && st->left_slots > 0) {
    if (st->hist.array[st->hist.end - 1]) {
      solution[len - 1] += 1;
      if (st->hist.end > len)
        st->hist.array[st->hist.end - len - 1] += 1;
      st->hist.array[st->hist.end - 1] -= 1;
      st->left_slots -= 1;
      st->left_volume -= per_chunk;
      if (0 >= (int)st->left_volume) {
      done:
        while (--len)
          solution[len - 1] = 0;
        return true;
      }
    } else {
      st->hist.end -= 1;
      if (len > st->hist.end) {
        assert(len == st->hist.end + 1);
        len = st->hist.end;
        per_chunk -= ct->other_pages;
        solution[len - 1] = 0;
      }
    }
  }
  return false;
}

/* Поиск оптимального решения путем жадного бинарного деления и рекурсивного спуска по уже посчитанной гистограмме. */
static bool solve_recursive(const sr_context_t *const ct, sr_state_t *const st, size_t len) {
  assert(st->left_slots >= 1);
  size_t per_chunk = ct->first_page + ct->other_pages * (len - 1);
  if (len > ct->factor && st->left_slots > 1 && st->left_volume > per_chunk) {
    unsigned lo = 0, hi = st->left_slots - 1, n = lo;
    do {
      sr_state_t local = *st;
      if (n) {
        if (!consume_stack(&local, len, n)) {
          hi = n - 1;
          n = (hi + lo) / 2;
          continue;
        }
        assert(local.left_slots > n);
        local.left_slots -= n;
        local.left_volume = (local.left_volume > n * per_chunk) ? local.left_volume - n * per_chunk : 0;
      }
      if (!solve_recursive(ct, &local, len - 1)) {
        lo = n + 1;
      } else if (n > lo && n < hi) {
        hi = n;
      } else {
        ct->solution->array[len - 1] = n;
        *st = local;
        return true;
      }
      n = (hi + lo + 1) / 2;
    } while (hi >= lo);
    return false;
  }

  return consume_remaining(ct, st, len);
}

static int gc_dense_solve(MDBX_txn *txn, gcu_t *ctx, gc_dense_histogram_t *const solution) {
  sr_state_t st = {
      .left_slots = rkl_len(&ctx->ready4reuse), .left_volume = ctx->return_left, .hist = ctx->dense_histogram};
  assert(st.left_slots > 0 && st.left_volume > 0 && MDBX_PNL_GETSIZE(txn->wr.repnl) > 0);
  if (unlikely(!st.left_slots || !st.left_volume)) {
    ERROR("%s/%d: %s", "MDBX_PROBLEM", MDBX_PROBLEM, "recursive-solving preconditions violated");
    return MDBX_PROBLEM;
  }

  const sr_context_t ct = {.factor = gc_chunk_pages(txn, (st.left_volume + st.left_slots - 1) / st.left_slots),
                           .first_page = /* на первой странице */ txn->env->maxgc_large1page +
                                         /* сама страница также будет израсходована */ 1,
                           .other_pages = /* на второй и последующих страницах */ txn->env->ps / sizeof(pgno_t) +
                                          /* каждая страница также будет израсходована */ 1,
                           .solution = solution};

  memset(solution, 0, sizeof(*solution));
  if (solve_recursive(&ct, &st, st.hist.end)) {
    const pgno_t *end = ARRAY_END(solution->array);
    while (end > solution->array && end[-1] == 0)
      --end;
    solution->end = (unsigned)(end - solution->array);

    /* проверяем решение */
    size_t items = 0, volume = 0;
    for (size_t i = 0, chunk = ct.first_page; i < solution->end; ++i) {
      items += solution->array[i];
      volume += solution->array[i] * chunk;
      chunk += ct.other_pages;
    }

    if (unlikely(volume < (size_t)ctx->return_left || items > rkl_len(&ctx->ready4reuse))) {
      assert(!"recursive-solving failure");
      ERROR("%s/%d: %s", "MDBX_PROBLEM", MDBX_PROBLEM, "recursive-solving failure");
      return MDBX_PROBLEM;
    }
    return MDBX_RESULT_TRUE;
  }

  /* решение НЕ найдено */
  return MDBX_RESULT_FALSE;
}

// int gc_solve_test(MDBX_txn *txn, gcu_t *ctx) {
//   gc_dense_histogram_t r;
//   gc_dense_histogram_t *const solution = &r;
//
//   sr_state_t st = {.left_slots = 5,
//                    .left_volume = 8463,
//                    .hist = {.end = 31, .array = {6493, 705, 120, 14, 2, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0,
//                                                  0,    0,   0,   0,  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4}}};
//   assert(st.left_slots > 0 && st.left_volume > 0 && MDBX_PNL_GETSIZE(txn->wr.repnl) > 0);
//   if (unlikely(!st.left_slots || !st.left_volume)) {
//     ERROR("%s/%d: %s", "MDBX_PROBLEM", MDBX_PROBLEM, "recursive-solving preconditions violated");
//     return MDBX_PROBLEM;
//   }
//
//   const sr_context_t ct = {.factor = gc_chunk_pages(txn, (st.left_volume + st.left_slots - 1) / st.left_slots),
//                            .first_page = /* на первой странице */ txn->env->maxgc_large1page +
//                                          /* сама страница также будет израсходована */ 1,
//                            .other_pages = /* на второй и последующих страницах */ txn->env->ps / sizeof(pgno_t) +
//                                           /* каждая страница также будет израсходована */ 1,
//                            .solution = solution};
//
//   memset(solution, 0, sizeof(*solution));
//   if (solve_recursive(&ct, &st, st.hist.end)) {
//     const pgno_t *end = ARRAY_END(solution->array);
//     while (end > solution->array && end[-1] == 0)
//       --end;
//     solution->end = (unsigned)(end - solution->array);
//
//     /* проверяем решение */
//     size_t items = 0, volume = 0;
//     for (size_t i = 0, chunk = ct.first_page; i < solution->end; ++i) {
//       items += solution->array[i];
//       volume += solution->array[i] * chunk;
//       chunk += ct.other_pages;
//     }
//
//     if (unlikely(volume < (size_t)ctx->return_left || items > rkl_len(&ctx->ready4reuse))) {
//       assert(!"recursive-solving failure");
//       ERROR("%s/%d: %s", "MDBX_PROBLEM", MDBX_PROBLEM, "recursive-solving failure");
//       return MDBX_PROBLEM;
//     }
//     return MDBX_RESULT_TRUE;
//   }
//
//   /* решение НЕ найдено */
//   return MDBX_RESULT_FALSE;
// }

/* Ищем свободные/неиспользуемые id в GC, чтобы затем использовать эти идентификаторы для возврата неиспользованных
 * остатков номеров страниц ранее изъятых из GC.
 *
 * Нехватка идентификаторов достаточно редкая ситуация, так как возвращается страниц обычно не более чем было извлечено.
 * Однако, больше идентификаторов может потребоваться в следующих ситуациях:
 *
 *  - ранее с БД работала старая версия libmdbx без поддержки BigFoot и поэтому были переработано очень длинные записи,
 *    возврат остатков которых потребует нарезки на несколько кусков.
 *
 *  - ранее было зафиксировано несколько транзакций, которые помещали в GC retired-списки близкие к максимальному
 *    размеру помещающемуся на одну листовую страницу, после чего текущая транзакция переработала все эти записи,
 *    но по совокупности операций эти страницы оказались лишними и теперь при возврате потребуют больше слотов
 *    из-за обеспечения резерва свободного места при нарезке на множество кусков.
 *
 * Таким образом, потребность в поиске возникает редко и в большинстве случаев необходимо найти 1-2 свободных
 * слота/идентификатора. Если же требуется много слотов, то нет смысла экономить на поиске. */
static int gc_search_holes(MDBX_txn *txn, gcu_t *ctx) {
  tASSERT(txn, ctx->return_left > 0 && txn->env->gc.detent);
  tASSERT(txn, rkl_empty(&txn->wr.gc.reclaimed));
  if (!ctx->gc_first) {
    ctx->gc_first = txn->env->gc.detent;
    if (txn->dbs[FREE_DBI].items) {
      MDBX_val key;
      int err = outer_first(&ctx->cursor, &key, nullptr);
      if (unlikely(err != MDBX_SUCCESS))
        return err;
      err = gc_peekid(&key, &ctx->gc_first);
      if (unlikely(err != MDBX_SUCCESS))
        return err;
    }
  }

  /* В LIFO режиме требуется поиск в направлении от новых к старым записям с углублением в потенциально
   * рыхлую/неоднородную структуру, с последующим её заполнением возвращаемыми страницами. */

  /* В FIFO режиме, поиск внутри GC может быть полезным при нелинейной переработке (которая пока не реализована),
   * когда будет переработан один из следующих MVCC-снимков без переработки предыдущего. Необходимая для этого
   * независимость (отсутствие пересечения) снимков по набору retired-страниц может сложиться при последовательности
   * пишущих транзакций изменяющих данные в структурно одних и тех же страницах b-tree.
   *
   * Однако, в текущем понимании, это крайне редкая ситуация, при которой также весьма вероятно наличие свободного
   * интервала в начале GC, а поэтому вероятность выигрыша от дополнительного поиска вперед стремится к нулю. Кроме
   * этого, из-за мизерной вероятности ситуаций в которых такой поиск будет работать, его крайне сложно тестировать
   * -- требуется разработка отдельного теста, который может быть достаточно хрупким, так как любая доработка
   * основного кода может требовать изменений/подстройки сценария теста.
   *
   * Поэтому пока, до появления явной необходимости и/или пользы, решено отказаться от поиска свободных слотов вглубь GC
   * в направлении от старых записей к новым, в том числе в режиме FIFO. */

  dbg_dump_ids(ctx);
  const intptr_t tail_space =
      ((ctx->gc_first > UINT16_MAX) ? UINT16_MAX : (unsigned)ctx->gc_first - 1) * ctx->goodchunk;
  const txnid_t reasonable_deep =
      txn->env->maxgc_per_branch +
      2 * (txn->env->gc.detent - txnid_min(rkl_lowest(&ctx->ready4reuse), rkl_lowest(&txn->wr.gc.comeback)));
  const txnid_t scan_threshold = (txn->env->gc.detent > reasonable_deep) ? txn->env->gc.detent - reasonable_deep : 0;

  txnid_t scan_hi = txn->env->gc.detent, scan_lo = INVALID_TXNID;
  if (!is_lifo(txn) && ctx->gc_first < txn->env->gc.detent &&
      txn->env->gc.detent - ctx->gc_first < ctx->cursor.tree->items) {
    scan_hi = ctx->gc_first;
    scan_lo = 0;
  }

  rkl_iter_t iter_ready4reuse, iter_comeback;
  rkl_find(&ctx->ready4reuse, scan_hi, &iter_ready4reuse);
  rkl_find(&txn->wr.gc.comeback, scan_hi, &iter_comeback);
  rkl_hole_t hole_ready4reuse = rkl_hole(&iter_ready4reuse, true);
  rkl_hole_t hole_comeback = rkl_hole(&iter_comeback, true);
  txnid_t begin, end;
  /* Ищем свободные id в GC в направлении от конца (новых записей) к началу (старым записям). */
  do {
    TRACE("hole-ready4reuse %" PRIaTXN "..%" PRIaTXN ", hole-comeback %" PRIaTXN "..%" PRIaTXN ", scan-range %" PRIaTXN
          "..%" PRIaTXN,
          hole_ready4reuse.begin, hole_ready4reuse.end, hole_comeback.begin, hole_comeback.end, scan_lo, scan_hi);
    MDBX_val key;
    int err;
    end = txnid_min(scan_hi, txnid_min(hole_ready4reuse.end, hole_comeback.end));
    if (hole_comeback.begin >= end) {
      hole_comeback = rkl_hole(&iter_comeback, true);
      TRACE("turn-comeback %" PRIaTXN "..%" PRIaTXN, hole_comeback.begin, hole_comeback.end);
    } else if (hole_ready4reuse.begin >= end) {
      hole_ready4reuse = rkl_hole(&iter_ready4reuse, true);
      TRACE("turn-ready4reuse %" PRIaTXN "..%" PRIaTXN, hole_ready4reuse.begin, hole_ready4reuse.end);
    } else if (scan_lo >= end) {
      TRACE("turn-scan from %" PRIaTXN "..%" PRIaTXN, scan_lo, scan_hi);
      scan_hi = scan_lo - 1;
      if (scan_lo - end > 4) {
        scan_lo = end - 1;
        key.iov_base = &scan_lo;
        key.iov_len = sizeof(scan_lo);
        const csr_t csr = cursor_seek(&ctx->cursor, &key, nullptr, MDBX_SET_RANGE);
        if (csr.err != MDBX_NOTFOUND) {
          if (unlikely(csr.err != MDBX_SUCCESS))
            return csr.err;
        }
        scan_hi = end - csr.exact;
      }
      goto scan;
    } else {
      begin = txnid_max(scan_lo, txnid_max(hole_ready4reuse.begin, hole_comeback.begin));
      tASSERT(txn, begin <= scan_hi && begin > 0);
      while (--end >= begin) {
        err = gc_push_sequel(txn, ctx, end);
        tASSERT(txn, (ctx->return_left > 0) == (err != MDBX_RESULT_TRUE));
        if (err != MDBX_SUCCESS) {
          return err;
        }
      }
      if (MIN_TXNID >= begin)
        break;
      if (begin == hole_comeback.begin) {
        hole_comeback = rkl_hole(&iter_comeback, true);
        TRACE("pull-comeback %" PRIaTXN "..%" PRIaTXN, hole_comeback.begin, hole_comeback.end);
      }
      if (begin == hole_ready4reuse.begin) {
        hole_ready4reuse = rkl_hole(&iter_ready4reuse, true);
        TRACE("pull-ready4reuse %" PRIaTXN "..%" PRIaTXN, hole_ready4reuse.begin, hole_ready4reuse.end);
      }
      if (begin == scan_lo) {
        TRACE("pull-scan from %" PRIaTXN "..%" PRIaTXN, scan_lo, scan_hi);
        do {
          scan_hi = scan_lo - 1;
        scan:
          if (scan_hi < scan_threshold && tail_space >= ctx->return_left) {
            /* Искать глубже нет смысла, ибо в начале GC есть достаточно свободных идентификаторов. */
            TRACE("stop-scan %s", "threshold");
            scan_lo = 0;
            scan_hi = ctx->gc_first;
            break;
          }
          err = outer_prev(&ctx->cursor, &key, nullptr, MDBX_PREV);
          if (err == MDBX_NOTFOUND) {
            /* больше нет записей ближе к началу GC, все значения id свободны */
            TRACE("stop-scan %s", "eof");
            scan_lo = 0;
            break;
          }
          if (unlikely(err != MDBX_SUCCESS))
            return err;
          err = gc_peekid(&key, &scan_lo);
          if (unlikely(err != MDBX_SUCCESS))
            return err;
          TRACE("scan: peek %" PRIaTXN, scan_lo);
          scan_lo += 1;
        } while (scan_lo >= scan_hi);
        TRACE("scan-range %" PRIaTXN "..%" PRIaTXN, scan_lo, scan_hi);
      }
    }
  } while (end > MIN_TXNID);
  return MDBX_SUCCESS;
}

static inline int gc_reserve4return(MDBX_txn *txn, gcu_t *ctx, const size_t chunk_lo, const size_t chunk_hi) {
  txnid_t reservation_id = rkl_pop(&ctx->ready4reuse, true);
  TRACE("%s: slots-ready4reuse-left %zu, reservation-id %" PRIaTXN, dbg_prefix(ctx), rkl_len(&ctx->ready4reuse),
        reservation_id);
  tASSERT(txn, reservation_id >= MIN_TXNID && reservation_id < txn->txnid);
  tASSERT(txn, reservation_id <= txn->env->lck->cached_oldest.weak);
  if (unlikely(reservation_id < MIN_TXNID ||
               reservation_id > atomic_load64(&txn->env->lck->cached_oldest, mo_Relaxed))) {
    ERROR("** internal error (reservation gc-id %" PRIaTXN ")", reservation_id);
    return MDBX_PROBLEM;
  }

  int err = rkl_push(&txn->wr.gc.comeback, reservation_id, false);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  MDBX_val key = {.iov_base = &reservation_id, .iov_len = sizeof(reservation_id)};
  MDBX_val data = {.iov_base = nullptr, .iov_len = gc_chunk_bytes(chunk_hi)};
  TRACE("%s: reserved +%zu...+%zu [%zu...%zu), err %d", dbg_prefix(ctx), chunk_lo, chunk_hi,
        ctx->return_reserved_lo + 1, ctx->return_reserved_hi + chunk_hi + 1, err);
  gc_prepare_stockpile4update(txn, ctx);
  err = cursor_put(&ctx->cursor, &key, &data, MDBX_RESERVE | MDBX_NOOVERWRITE);
  tASSERT(txn, pnl_check_allocated(txn->wr.repnl, txn->geo.first_unallocated - MDBX_ENABLE_REFUND));
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  memset(data.iov_base, 0, data.iov_len);
  ctx->return_reserved_lo += chunk_lo;
  ctx->return_reserved_hi += chunk_hi;
  if (unlikely(!rkl_empty(&txn->wr.gc.reclaimed))) {
    NOTICE("%s: restart since %zu slot(s) reclaimed (reserved %zu...%zu of %zu)", dbg_prefix(ctx),
           rkl_len(&txn->wr.gc.reclaimed), ctx->return_reserved_lo, ctx->return_reserved_hi,
           MDBX_PNL_GETSIZE(txn->wr.repnl));
    return MDBX_RESULT_TRUE;
  }

  return MDBX_SUCCESS;
}

static size_t dense_chunk_outlay(const MDBX_txn *txn, const size_t chunk) {
  size_t need_span = gc_chunk_pages(txn, chunk);
  return gc_repnl_has_span(txn, need_span) ? need_span : 0;
}

static size_t dense_adjust_chunk(const MDBX_txn *txn, const size_t chunk) {
  size_t adjusted = chunk;
  if (chunk > txn->env->maxgc_large1page) {
    size_t hi = chunk + 1, lo = chunk - gc_chunk_pages(txn, chunk) - 1;
    while (lo < hi) {
      adjusted = (hi + lo) / 2;
      size_t probe = chunk - dense_chunk_outlay(txn, adjusted);
      if (probe > adjusted)
        lo = adjusted + 1;
      else if (probe < adjusted)
        hi = adjusted - 1;
      else
        break;
    }
  }
  return adjusted;
}

static size_t dense_adjust_amount(const MDBX_txn *const txn, size_t amount) {
  const size_t gap = 2 + txn->dbs[FREE_DBI].height;
  const size_t snubber = txn->env->ps / sizeof(pgno_t) / 2;
  return ((amount + gap < txn->env->maxgc_large1page) ? txn->env->maxgc_large1page : amount + snubber);
}

static int gc_handle_dense(MDBX_txn *txn, gcu_t *ctx, size_t left_min, size_t left_max) {
  /* Крайне маловероятная ситуация, в текущем понимании не возможная при нормальной/ожидаемой работе всех
   * актуальных версий движка. Тем не менее, сюда мы можем попасть при использовании БД с содержимым GC
   * оставшимся после старых версий и/или при выключенном BigFoot. Тогда в GC могут быть записи огромного
   * размера, при возврате которых мы получаем так много кусков, что в GC не хватает свободных/неиспользуемых
   * идентификаторов от прошлых транзакций.
   *
   * Дальше три возможности:
   * 1. Искать в GC доступные для переработки записи короче maxgc_large1page. Это малоэффективный путь,
   *    так как если мы уже попали в текущую ситуацию, то маловероятно что в GC есть такие записи и что запаса
   *    места хватит. Поэтому оставляем этот путь в качестве предпоследнего варианта.
   * 2. Попытаться запихнуть остаток одним куском, который может быть многократно больше maxgc_large1page,
   *    т.е. потребуется несколько последовательных свободных страниц, из-за чего может произойти загрузка всей
   *    GC и т.д. Это плохой путь, который можно использовать только в качестве последнего шанса.
   * 3. Искать смежные страницы среди возвращаемых и сохранять куски помещающиеся в такие последовательности.
   *
   *  Поэтому комбинируем все три варианта 3+1+2:
   *  - Вычисляем среднюю целевую длину куска в large/overflow страницах, при нарезке на которые имеющихся
   *    слотов/идентификаторов хватит для размещения возвращаемых страниц.
   *  - В идеале ищем в wr.repnl последовательности смежных страниц длиной от ⌊целевой длины куска⌋
   *    до ⌈целевой длины куска⌉ и выполняем резервирование кусками помещающимися в эти последовательности.
   *    Теоретически, вероятность (а следовательно и количество) последовательностей экспоненциально уменьшается
   *    с увеличением длины. На практике, в основном, это будут пары и тройки страниц, но также и длинные
   *    последовательности, которые образуются в исходных больших транзакциях (порождающий большие
   *    retired-списки), особенно при выделении новых страниц. При этом использование длинных
   *    последовательностей чревато повторением проблем при переработке созданных сейчас записей.
   *  - Поэтому оптимальное решение выглядит как поиск набора последовательностей, мощность которого равна
   *    количеству доступных слотов/идентификаторов, а длины последовательностей минимальны, но достаточны для
   *    размещения всех возвращаемых страниц. */

  int err = MDBX_RESULT_FALSE;
  if (!rkl_empty(&ctx->ready4reuse)) {
    gc_dense_hist(txn, ctx);
    gc_dense_histogram_t solution;
    if (ctx->loop == 1 || ctx->loop % 3 == 0)
      left_max = dense_adjust_amount(txn, left_max);
    ctx->return_left = left_max;
    err = gc_dense_solve(txn, ctx, &solution);
    if (err == MDBX_RESULT_FALSE /* решение НЕ найдено */ && left_max != left_min) {
      if (ctx->loop == 1 || ctx->loop % 3 == 0)
        left_min = dense_adjust_amount(txn, left_min);
      if (left_max != left_min) {
        ctx->return_left = left_min;
        err = gc_dense_solve(txn, ctx, &solution);
      }
    }
    if (err == MDBX_RESULT_TRUE /* решение найдено */) {
      for (size_t i = solution.end; i > 0; --i)
        for (pgno_t n = 0; n < solution.array[i - 1]; ++n) {
          size_t span = i;
          size_t chunk_hi = txn->env->maxgc_large1page + txn->env->ps / sizeof(pgno_t) * (span - 1);
          if (chunk_hi > left_max) {
            chunk_hi = left_max;
            span = gc_chunk_pages(txn, chunk_hi);
          }
          size_t chunk_lo = chunk_hi - txn->env->maxgc_large1page + ctx->goodchunk;
          TRACE("%s: dense-chunk (seq-len %zu, %d of %d) %zu...%zu, gc-per-ovpage %u", dbg_prefix(ctx), i, n + 1,
                solution.array[i - 1], chunk_lo, chunk_hi, txn->env->maxgc_large1page);
          size_t amount = MDBX_PNL_GETSIZE(txn->wr.repnl);
          err = gc_reserve4return(txn, ctx, chunk_lo, chunk_hi);
          if (unlikely(err != MDBX_SUCCESS))
            return err;

          const size_t now = MDBX_PNL_GETSIZE(txn->wr.repnl);
          if (span < amount - now - txn->dbs[FREE_DBI].height || span > amount - now + txn->dbs[FREE_DBI].height)
            TRACE("dense-%s-reservation: miss %zu (expected) != %zi (got)", "solve", span, amount - now);
          amount = now;
          if (ctx->return_reserved_hi >= amount)
            return MDBX_SUCCESS;
          left_max = dense_adjust_amount(txn, amount) - ctx->return_reserved_lo;
        }
    }
  } else if (rkl_len(&txn->wr.gc.comeback)) {
    NOTICE("%s: restart since %zu slot(s) comemack non-dense (reserved %zu...%zu of %zu)", dbg_prefix(ctx),
           rkl_len(&txn->wr.gc.comeback), ctx->return_reserved_lo, ctx->return_reserved_hi,
           MDBX_PNL_GETSIZE(txn->wr.repnl));
    return /* повтор цикла */ MDBX_RESULT_TRUE;
  }

  if (err == MDBX_RESULT_FALSE /* решение НЕ найдено, либо нет идентификаторов */) {
    if (ctx->return_left > txn->env->maxgc_large1page) {
      err = gc_reclaim_slot(txn, ctx);
      if (err == MDBX_NOTFOUND)
        err = gc_reserve4retired(txn, ctx, gc_chunk_pages(txn, dense_adjust_chunk(txn, ctx->return_left)));
      if (err != MDBX_NOTFOUND && err != MDBX_SUCCESS)
        return err;
    }

    const size_t per_page = txn->env->ps / sizeof(pgno_t);
    size_t amount = MDBX_PNL_GETSIZE(txn->wr.repnl);
    do {
      if (rkl_empty(&ctx->ready4reuse)) {
        NOTICE("%s: restart since no slot(s) available (reserved %zu...%zu of %zu)", dbg_prefix(ctx),
               ctx->return_reserved_lo, ctx->return_reserved_hi, amount);
        return MDBX_RESULT_TRUE;
      }
      const size_t left = dense_adjust_amount(txn, amount) - ctx->return_reserved_hi;
      const size_t slots = rkl_len(&ctx->ready4reuse);
      const size_t base = (left + slots - 1) / slots;
      const size_t adjusted = dense_adjust_chunk(txn, base);
      TRACE("dense-reservation: reserved %zu...%zu of %zu, left %zu slot(s) and %zu pnl, step: %zu base,"
            " %zu adjusted",
            ctx->return_reserved_lo, ctx->return_reserved_hi, amount, slots, left, base, adjusted);
      const size_t chunk_hi =
          (adjusted > txn->env->maxgc_large1page)
              ? txn->env->maxgc_large1page + ceil_powerof2(adjusted - txn->env->maxgc_large1page, per_page)
              : txn->env->maxgc_large1page;
      const size_t chunk_lo =
          (adjusted > txn->env->maxgc_large1page)
              ? txn->env->maxgc_large1page + floor_powerof2(adjusted - txn->env->maxgc_large1page, per_page)
              : adjusted;
      err = gc_reserve4return(txn, ctx, chunk_lo, chunk_hi);
      if (unlikely(err != MDBX_SUCCESS))
        return err;
      const size_t now = MDBX_PNL_GETSIZE(txn->wr.repnl);
      if (base - adjusted + txn->dbs[FREE_DBI].height < amount - now ||
          base - adjusted > amount - now + txn->dbs[FREE_DBI].height)
        TRACE("dense-%s-reservation: miss %zu (expected) != %zi (got)", "unsolve", base - adjusted, amount - now);
      amount = now;
    } while (ctx->return_reserved_hi < amount);
  }

  if (unlikely(err != MDBX_SUCCESS))
    ERROR("unable provide IDs and/or to fit returned PNL (%zd+%zd pages, %zd+%zd slots), err %d", ctx->retired_stored,
          MDBX_PNL_GETSIZE(txn->wr.repnl), rkl_len(&txn->wr.gc.comeback), rkl_len(&ctx->ready4reuse), err);
  return err;
}

/* Выполняет один шаг резервирования записей для возврата в GC страниц (их номеров), оставшихся после
 * переработки GC и последующего использования в транзакции. */
static int gc_rerere(MDBX_txn *txn, gcu_t *ctx) {
  /* При резервировании часть оставшихся страниц может быть использована до полного исчерпания остатка,
   * что также может привести к переработке дополнительных записей GC. Таким образом, на каждой итерации
   * ситуация может существенно меняться, в том числе может потребоваться очистка резерва и повтор всего цикла.
   *
   * Кроме этого, теоретически в GC могут быть очень большие записи (созданные старыми версиями движка и/или
   * при выключенной опции MDBX_ENABLE_BIGFOOT), которые при возврате будут нарезаться на более мелкие куски.
   * В этом случае возвращаемых записей будет больше чем было переработано, поэтому потребуются дополнительные
   * идентификаторы/слоты отсутствующие в GC. */

  // gc_solve_test(txn, ctx);

  tASSERT(txn, rkl_empty(&txn->wr.gc.reclaimed));
  const size_t amount = MDBX_PNL_GETSIZE(txn->wr.repnl);
  if (ctx->return_reserved_hi >= amount) {
    if (unlikely(ctx->dense)) {
      ctx->dense = false;
      NOTICE("%s: out of dense-mode (amount %zu, reserved %zu..%zu)", dbg_prefix(ctx), amount, ctx->return_reserved_lo,
             ctx->return_reserved_hi);
    }
    if (unlikely(amount ? (amount + txn->env->maxgc_large1page < ctx->return_reserved_lo)
                        : (ctx->return_reserved_hi > 3))) {
      /* после резервирования было израсходованно слишком много страниц и получилось слишком много резерва */
      TRACE("%s: reclaimed-list %zu < reversed %zu..%zu, retry", dbg_prefix(ctx), amount, ctx->return_reserved_lo,
            ctx->return_reserved_hi);
      return MDBX_RESULT_TRUE;
    }
    /* резерва достаточно, ничего делать не надо */
    return MDBX_SUCCESS;
  }

  const size_t left_min = amount - ctx->return_reserved_hi;
  const size_t left_max = amount - ctx->return_reserved_lo;
  if (likely(left_min < txn->env->maxgc_large1page && !rkl_empty(&ctx->ready4reuse))) {
    /* Есть хотя-бы один слот и весь остаток списка номеров страниц помещается в один кусок.
     * Это самая частая ситуация, просто продолжаем. */
  } else {
    if (likely(rkl_len(&ctx->ready4reuse) * ctx->goodchunk >= left_max)) {
      /* Слотов хватает, основная задача делить на куски так, чтобы изменение (уменьшение) кол-ва возвращаемых страниц в
       * процессе резервирования записей в GC не потребовало менять резервирование, т.е. удалять и повторять всё снова.
       */
    } else {
      /* Слотов нет, либо не хватает для нарезки возвращаемых страниц кусками по goodchunk */
      ctx->return_left = left_max;
      int err = gc_search_holes(txn, ctx);
      tASSERT(txn, (ctx->return_left <= 0) == (err == MDBX_RESULT_TRUE));
      if (unlikely(MDBX_IS_ERROR(err)))
        return err;

      if (!rkl_empty(&ctx->sequel)) {
        err = rkl_merge(&ctx->ready4reuse, &ctx->sequel, false);
        if (unlikely(err != MDBX_SUCCESS)) {
          if (err == MDBX_RESULT_TRUE) {
            ERROR("%s/%d: %s", "MDBX_PROBLEM", MDBX_PROBLEM, "unexpected duplicate(s) during rkl-merge");
            err = MDBX_PROBLEM;
          }
          return err;
        }
        rkl_clear(&ctx->sequel);
      }

      if (unlikely(ctx->return_left > 0)) {
        /* Делаем переоценку баланса для кусков предельного размера (по maxgc_large1page, вместо goodchunk). */
        const intptr_t dense_unfit = left_min - rkl_len(&ctx->ready4reuse) * txn->env->maxgc_large1page;
        if (dense_unfit > 0) {
          /* Имеющихся идентификаторов НЕ хватит,
           * даже если если их использовать для кусков размером maxgc_large1page вместо goodchunk. */
          if (!ctx->dense) {
            NOTICE("%s: enter to dense-mode (amount %zu, reserved %zu..%zu, slots/ids %zu, left %zu..%zu, unfit %zu)",
                   dbg_prefix(ctx), amount, ctx->return_reserved_lo, ctx->return_reserved_hi,
                   rkl_len(&ctx->ready4reuse), left_min, left_max, dense_unfit);
            ctx->dense = true;
          }
          return gc_handle_dense(txn, ctx, left_min, left_max);
        }
      }
      tASSERT(txn, rkl_empty(&txn->wr.gc.reclaimed));
    }
  }

  /* Максимальный размер куска, который помещается на листовой странице, без выноса на отдельную "overflow" страницу. */
  const size_t chunk_inpage = (txn->env->leaf_nodemax - NODESIZE - sizeof(txnid_t)) / sizeof(pgno_t) - 1;

  /* Размер куска помещающийся на одну отдельную "overflow" страницу, но с небольшим запасом сводобного места. */
  const size_t chunk_good = ctx->goodchunk;

  /* Учитываем резервирование по минимальному размеру кусков (chunk_lo), но резервируем слоты с некоторым запасом
   * (chunk_hi). При этом предполагая что каждый слот может быть заполнен от chunk_lo до chunk_hi, что обеспечивает
   * хорошую амортизацию изменения размера списка возвращаемых страниц, как из-за расходов на создаваемые записи, так и
   * из-за переработки GC. */
  const size_t chunk_lo = (left_min < chunk_inpage) ? left_min : chunk_good;
  /* Куски размером больше chunk_inpage и до maxgc_large1page включительно требуют одной "overflow" страницы.
   * Соответственно требуют одинаковых затрат на обслуживание, а диапазон между chunk_good и maxgc_large1page
   * амортизирует изменения кол-ва списка возвращаемых страниц.
   *
   * Выравниваем размер коротких кусков на 4 (т.е. до 3, с учетом нулевого элемента с длиной),
   * а длинных кусков до maxgc_large1page */
  const size_t chunk_hi = (((left_max + 1) | 3) > chunk_inpage) ? txn->env->maxgc_large1page : ((left_max + 1) | 3);

  TRACE("%s: chunk %zu...%zu, gc-per-ovpage %u", dbg_prefix(ctx), chunk_lo, chunk_hi, txn->env->maxgc_large1page);
  tASSERT(txn, chunk_lo > 0 && chunk_lo <= chunk_hi && chunk_hi > 1);
  return gc_reserve4return(txn, ctx, chunk_lo, chunk_hi);
}

/* Заполняет зарезервированные записи номерами возвращаемых в GC страниц. */
static int gc_fill_returned(MDBX_txn *txn, gcu_t *ctx) {
  tASSERT(txn, pnl_check_allocated(txn->wr.repnl, txn->geo.first_unallocated - MDBX_ENABLE_REFUND));
  tASSERT(txn, dpl_check(txn));

  /* Уже есть набор зарезервированных записей GC, id которых собраны в txn->wr.gc.comeback. При этом текущее
   * кол-вол возвращаемых страниц (оставшихся после расходов на резервирование) точно помещается в
   * эти записи и скорее всего с некоторым запасом. Иначе, если резерва недостаточно или избыток
   * резерва неприемлемо велик, то нет другого способа как удалить все созданные записи и повторить
   * всё ещё раз, и дальше этот путь здесь не рассматривается.
   *
   * В большинстве случаев, при резервировании записей переработка GC происходить не будет. Поэтому
   * размер резервированных записей кроме последней будет равен gc_largechunk_preferred_size(),
   * а последней округлённому/выровненному остатку страниц. Однако, в общем случае, может существенно
   * колебаться как размер записей, так и "баланс" отклонения от среднего.
   *
   * Если считать что резерва достаточно и имеющийся избыток допустим, то задача заполнения сводится
   * к распределению излишков резерва по записям с учётом их размера, а далее просто к записи данных.
   * При этом желательно обойтись без каких-то сложных операций типа деления и т.п. */
  const size_t amount = MDBX_PNL_GETSIZE(txn->wr.repnl);
  tASSERT(txn, amount > 0 && amount <= ctx->return_reserved_hi && !rkl_empty(&txn->wr.gc.comeback));
  const size_t slots = rkl_len(&txn->wr.gc.comeback);
  if (likely(slots == 1)) {
    /* самый простой и частый случай */
    txnid_t id = rkl_lowest(&txn->wr.gc.comeback);
    MDBX_val key = {.iov_base = &id, .iov_len = sizeof(id)};
    MDBX_val data = {.iov_base = nullptr, .iov_len = 0};
    int err = cursor_seek(&ctx->cursor, &key, &data, MDBX_SET_KEY).err;
    if (likely(err == MDBX_SUCCESS)) {
      pgno_t *const from = MDBX_PNL_BEGIN(txn->wr.repnl), *const to = MDBX_PNL_END(txn->wr.repnl);
      TRACE("%s: fill %zu [ %zu:%" PRIaPGNO "...%zu:%" PRIaPGNO "] @%" PRIaTXN " (%s)", dbg_prefix(ctx),
            MDBX_PNL_GETSIZE(txn->wr.repnl), from - txn->wr.repnl, from[0], to - txn->wr.repnl, to[-1], id, "at-once");
      tASSERT(txn, data.iov_len >= gc_chunk_bytes(MDBX_PNL_GETSIZE(txn->wr.repnl)));
      if (unlikely(data.iov_len - gc_chunk_bytes(MDBX_PNL_GETSIZE(txn->wr.repnl)) >= txn->env->ps * 2)) {
        NOTICE("too long %s-comeback-reserve @%" PRIaTXN ", have %zu bytes, need %zu bytes", "single", id, data.iov_len,
               gc_chunk_bytes(MDBX_PNL_GETSIZE(txn->wr.repnl)));
        return MDBX_RESULT_TRUE;
      }
      /* coverity[var_deref_model] */
      memcpy(data.iov_base, txn->wr.repnl, gc_chunk_bytes(MDBX_PNL_GETSIZE(txn->wr.repnl)));
    }
    return err;
  }

  rkl_iter_t iter = rkl_iterator(&txn->wr.gc.comeback, is_lifo(txn));
  size_t surplus = ctx->return_reserved_hi - amount, stored = 0;
  const size_t scale = 32 - ceil_log2n(ctx->return_reserved_hi), half4rounding = (1 << scale) / 2 - 1;
  tASSERT(txn, scale > 3 && scale < 32);
  const size_t factor = (surplus << scale) / ctx->return_reserved_hi;
  TRACE("%s: amount %zu, slots %zu, surplus %zu (%zu..%zu), factor %.5f (sharp %.7f)", dbg_prefix(ctx), amount, slots,
        surplus, ctx->return_reserved_lo, ctx->return_reserved_hi, factor / (double)(1 << scale),
        surplus / (double)ctx->return_reserved_lo);
  do {
    const size_t left = amount - stored;
    tASSERT(txn, left > 0 && left <= amount);
    txnid_t id = rkl_turn(&iter, is_lifo(txn));
    if (unlikely(!id)) {
      ERROR("reserve depleted (used %zu slots, left %zu loop %u)", rkl_len(&txn->wr.gc.comeback), left, ctx->loop);
      return MDBX_PROBLEM;
    }
    MDBX_val key = {.iov_base = &id, .iov_len = sizeof(id)};
    MDBX_val data = {.iov_base = nullptr, .iov_len = 0};
    const int err = cursor_seek(&ctx->cursor, &key, &data, MDBX_SET_KEY).err;
    if (unlikely(err != MDBX_SUCCESS))
      return err;

    tASSERT(txn, data.iov_len >= sizeof(pgno_t) * 2);
    const size_t chunk_hi = data.iov_len / sizeof(pgno_t) - 1;
    tASSERT(txn, chunk_hi >= 2);
    size_t chunk = left;
    if (chunk > chunk_hi) {
      chunk = chunk_hi;
      const size_t left_slots = rkl_left(&iter, is_lifo(txn));
      if (surplus && left_slots) {
        /* Единственный путь выполнения (набор условий) когда нужно распределять избыток резерва. */
        size_t hole = (chunk_hi * factor + half4rounding) >> scale;
        tASSERT(txn, hole < chunk_hi && hole <= surplus);
        chunk = chunk_hi - hole;
        tASSERT(txn, chunk > 0 && chunk <= chunk_hi);
        const intptr_t estimate_balance =
            (((left + surplus - chunk_hi) * factor + half4rounding) >> scale) - (surplus - hole);
        if (MDBX_HAVE_CMOV || estimate_balance) {
          chunk -= estimate_balance < 0 && chunk > 1;
          chunk += estimate_balance > 0 && hole > 0 && surplus > hole;
        }
      }
      tASSERT(txn, chunk <= chunk_hi && surplus >= chunk_hi - chunk && chunk <= left);
      surplus -= chunk_hi - chunk;
    }

    pgno_t *const dst = data.iov_base;
    pgno_t *const src = MDBX_PNL_BEGIN(txn->wr.repnl) + left - chunk;
    pgno_t *const from = src, *const to = src + chunk;
    TRACE("%s: fill +%zu (surplus %zu) [ %zu:%" PRIaPGNO "...%zu:%" PRIaPGNO "] @%" PRIaTXN " (%s)", dbg_prefix(ctx),
          chunk, chunk_hi - chunk, from - txn->wr.repnl, from[0], to - txn->wr.repnl, to[-1], id, "series");
    TRACE("%s: left %zu, surplus %zu, slots %zu", dbg_prefix(ctx), amount - (stored + chunk), surplus,
          rkl_left(&iter, is_lifo(txn)));
    tASSERT(txn, chunk > 0 && chunk <= chunk_hi && chunk <= left);
    if (unlikely(data.iov_len - gc_chunk_bytes(chunk) >= txn->env->ps)) {
      NOTICE("too long %s-comeback-reserve @%" PRIaTXN ", have %zu bytes, need %zu bytes", "multi", id, data.iov_len,
             gc_chunk_bytes(chunk));
      return MDBX_RESULT_TRUE;
    }

    /* coverity[var_deref_op] */
    *dst = (pgno_t)chunk;
    memcpy(dst + 1, src, chunk * sizeof(pgno_t));
    stored += chunk;
  } while (stored < amount);
  return MDBX_SUCCESS;
}

int gc_update(MDBX_txn *txn, gcu_t *ctx) {
  TRACE("\n>>> @%" PRIaTXN, txn->txnid);
  MDBX_env *const env = txn->env;
  ctx->cursor.next = txn->cursors[FREE_DBI];
  txn->cursors[FREE_DBI] = &ctx->cursor;
  int err;

  if (unlikely(!txn->env->gc.detent))
    txn_gc_detent(txn);

  if (AUDIT_ENABLED()) {
    err = audit_ex(txn, 0, false);
    if (unlikely(err != MDBX_SUCCESS))
      goto bailout;
  }

  /* The txn->wr.repnl[] can grow and shrink during this call.
   * The txn->wr.gc.reclaimed[] can grow, then migrate into ctx->ready4reuse and later to txn->wr.gc.comeback[].
   * But page numbers cannot disappear from txn->wr.retired_pages[]. */
retry:
  ctx->loop += !(ctx->prev_first_unallocated > txn->geo.first_unallocated);
  TRACE(">> %sstart, loop %u, gc: txn-rkl %zu, detent %" PRIaTXN, (ctx->loop > 1) ? "re" : "", ctx->loop,
        rkl_len(&txn->wr.gc.reclaimed), txn->env->gc.detent);

  tASSERT(txn, pnl_check_allocated(txn->wr.repnl, txn->geo.first_unallocated - MDBX_ENABLE_REFUND));
  tASSERT(txn, dpl_check(txn));
  if (unlikely(/* paranoia */ ctx->loop > ((MDBX_DEBUG > 0) ? 12 : 42))) {
    ERROR("txn #%" PRIaTXN " too more loops %u, bailout", txn->txnid, ctx->loop);
    err = MDBX_PROBLEM;
    goto bailout;
  }

  if (unlikely(ctx->prev_first_unallocated > txn->geo.first_unallocated)) {
    err = gc_clean_stored_retired(txn, ctx);
    if (unlikely(err != MDBX_SUCCESS))
      goto bailout;
  }

  ctx->prev_first_unallocated = txn->geo.first_unallocated;
  err = gc_clear_returned(txn, ctx);
  if (unlikely(err != MDBX_SUCCESS))
    goto bailout;

  while (true) {
    /* come back here after each put() in case retired-list changed */
    TRACE("%s", " >> continue");

    tASSERT(txn, pnl_check_allocated(txn->wr.repnl, txn->geo.first_unallocated - MDBX_ENABLE_REFUND));
    err = gc_clear_reclaimed(txn, ctx);
    if (unlikely(err != MDBX_SUCCESS))
      goto bailout;

    tASSERT(txn, pnl_check_allocated(txn->wr.repnl, txn->geo.first_unallocated - MDBX_ENABLE_REFUND));
    tASSERT(txn, dpl_check(txn));
    if (AUDIT_ENABLED()) {
      err = audit_ex(txn, ctx->retired_stored, false);
      if (unlikely(err != MDBX_SUCCESS))
        goto bailout;
    }

    /* return suitable into unallocated space */
    if (txn_refund(txn)) {
      tASSERT(txn, pnl_check_allocated(txn->wr.repnl, txn->geo.first_unallocated - MDBX_ENABLE_REFUND));
      if (AUDIT_ENABLED()) {
        err = audit_ex(txn, ctx->retired_stored, false);
        if (unlikely(err != MDBX_SUCCESS))
          goto bailout;
      }
    }

    if (txn->wr.loose_pages) {
      /* merge loose pages into the reclaimed- either retired-list */
      err = gc_merge_loose(txn, ctx);
      if (unlikely(err != MDBX_SUCCESS)) {
        if (err == MDBX_RESULT_TRUE)
          continue;
        goto bailout;
      }
      tASSERT(txn, txn->wr.loose_pages == 0);
    }

    if (ctx->retired_stored < MDBX_PNL_GETSIZE(txn->wr.retired_pages)) {
      /* store retired-list into GC */
      err = gc_store_retired(txn, ctx);
      if (unlikely(err != MDBX_SUCCESS))
        goto bailout;
      continue;
    }

    tASSERT(txn, pnl_check_allocated(txn->wr.repnl, txn->geo.first_unallocated - MDBX_ENABLE_REFUND));
    tASSERT(txn, txn->wr.loose_count == 0);
    if (AUDIT_ENABLED()) {
      err = audit_ex(txn, ctx->retired_stored, false);
      if (unlikely(err != MDBX_SUCCESS))
        goto bailout;
    }

    if (unlikely(MDBX_PNL_GETSIZE(txn->wr.repnl) + env->maxgc_large1page <= ctx->return_reserved_lo) && !ctx->dense) {
      /* после резервирования было израсходованно слишком много страниц и получилось слишком много резерва */
      TRACE("%s: reclaimed-list %zu < reversed %zu, retry", dbg_prefix(ctx), MDBX_PNL_GETSIZE(txn->wr.repnl),
            ctx->return_reserved_lo);
      goto retry;
    }

    if (ctx->return_reserved_hi < MDBX_PNL_GETSIZE(txn->wr.repnl)) {
      /* верхней границы резерва НЕ хватает, продолжаем резервирование */
      TRACE(">> %s, %zu...%zu, %s %zu", "reserving", ctx->return_reserved_lo, ctx->return_reserved_hi, "return-left",
            MDBX_PNL_GETSIZE(txn->wr.repnl) - ctx->return_reserved_hi);
      err = gc_rerere(txn, ctx);
      if (unlikely(err != MDBX_SUCCESS)) {
        if (err == MDBX_RESULT_TRUE)
          goto retry;
        goto bailout;
      }
      continue;
    }

    if (MDBX_PNL_GETSIZE(txn->wr.repnl) > 0) {
      TRACE(">> %s, %s %zu -> %zu...%zu", "filling", "return-reserved", MDBX_PNL_GETSIZE(txn->wr.repnl),
            ctx->return_reserved_lo, ctx->return_reserved_hi);
      err = gc_fill_returned(txn, ctx);
      if (unlikely(err != MDBX_SUCCESS)) {
        if (err == MDBX_RESULT_TRUE)
          goto retry;
        goto bailout;
      }
    }
    break;
  }

  tASSERT(txn, err == MDBX_SUCCESS);
  if (AUDIT_ENABLED()) {
    err = audit_ex(txn, ctx->retired_stored + MDBX_PNL_GETSIZE(txn->wr.repnl), true);
    if (unlikely(err != MDBX_SUCCESS))
      goto bailout;
  }
  if (unlikely(txn->wr.loose_count > 0)) {
    DEBUG("** restart: got %zu loose pages", txn->wr.loose_count);
    goto retry;
  }

bailout:
  txn->cursors[FREE_DBI] = ctx->cursor.next;

  MDBX_PNL_SETSIZE(txn->wr.repnl, 0);
#if MDBX_ENABLE_PROFGC
  env->lck->pgops.gc_prof.wloops += (uint32_t)ctx->loop;
#endif /* MDBX_ENABLE_PROFGC */
  TRACE("<<< %u loops, rc = %d\n", ctx->loop, err);
  return err;
}

#if MDBX_DEBUG_GCU
#pragma pop_macro("LOG_ENABLED")
#endif /* MDBX_DEBUG_GCU */
