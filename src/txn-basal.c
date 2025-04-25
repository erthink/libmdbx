/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025

#include "internals.h"

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

  txn->wr.dirtylist->pages_including_loose -= total_npages;
  while (r <= dl->length)
    dl->items[++w] = dl->items[r++];

  dl->sorted = dpl_setlen(dl, w);
  txn->wr.dirtyroom += r - 1 - w;
  tASSERT(txn, txn->wr.dirtyroom + txn->wr.dirtylist->length ==
                   (txn->parent ? txn->parent->wr.dirtyroom : txn->env->options.dp_limit));
  tASSERT(txn, txn->wr.dirtylist->length == txn->wr.loose_count);
  tASSERT(txn, txn->wr.dirtylist->pages_including_loose == txn->wr.loose_count);
  return rc;
}

__cold MDBX_txn *txn_basal_create(const size_t max_dbi) {
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

  rkl_init(&txn->wr.gc.reclaimed);
  rkl_init(&txn->wr.gc.comeback);
  txn->dbs = ptr_disp(txn, base);
  txn->cursors = ptr_disp(txn->dbs, max_dbi * sizeof(txn->dbs[0]));
  txn->dbi_seqs = ptr_disp(txn->cursors, max_dbi * sizeof(txn->cursors[0]));
  txn->dbi_state = ptr_disp(txn, size - max_dbi * sizeof(txn->dbi_state[0]));
#if MDBX_ENABLE_DBI_SPARSE
  txn->dbi_sparse = ptr_disp(txn->dbi_state, -bitmap_bytes);
#endif /* MDBX_ENABLE_DBI_SPARSE */
  txn->flags = MDBX_TXN_FINISHED;
  txn->wr.retired_pages = pnl_alloc(MDBX_PNL_INITIAL);
  txn->wr.repnl = pnl_alloc(MDBX_PNL_INITIAL);
  if (unlikely(!txn->wr.retired_pages || !txn->wr.repnl)) {
    txn_basal_destroy(txn);
    txn = nullptr;
  }

  return txn;
}

__cold void txn_basal_destroy(MDBX_txn *txn) {
  dpl_free(txn);
  rkl_destroy(&txn->wr.gc.reclaimed);
  rkl_destroy(&txn->wr.gc.comeback);
  pnl_free(txn->wr.retired_pages);
  pnl_free(txn->wr.spilled.list);
  pnl_free(txn->wr.repnl);
  osal_free(txn);
}

int txn_basal_start(MDBX_txn *txn, unsigned flags) {
  MDBX_env *const env = txn->env;

  txn->wr.troika = meta_tap(env);
  const meta_ptr_t head = meta_recent(env, &txn->wr.troika);
  uint64_t timestamp = 0;
  /* coverity[array_null] */
  while ("workaround for https://libmdbx.dqdkfa.ru/dead-github/issues/269") {
    int err = coherency_fetch_head(txn, head, &timestamp);
    if (likely(err == MDBX_SUCCESS))
      break;
    if (unlikely(err != MDBX_RESULT_TRUE))
      return err;
  }
  eASSERT(env, meta_txnid(head.ptr_v) == txn->txnid);
  txn->txnid = safe64_txnid_next(txn->txnid);
  if (unlikely(txn->txnid > MAX_TXNID)) {
    ERROR("txnid overflow, raise %d", MDBX_TXN_FULL);
    return MDBX_TXN_FULL;
  }

  tASSERT(txn, txn->dbs[FREE_DBI].flags == MDBX_INTEGERKEY);
  tASSERT(txn, check_table_flags(txn->dbs[MAIN_DBI].flags));
  txn->flags = flags;
  txn->nested = nullptr;
  txn->wr.loose_pages = nullptr;
  txn->wr.loose_count = 0;
#if MDBX_ENABLE_REFUND
  txn->wr.loose_refund_wl = 0;
#endif /* MDBX_ENABLE_REFUND */
  MDBX_PNL_SETSIZE(txn->wr.retired_pages, 0);
  txn->wr.spilled.list = nullptr;
  txn->wr.spilled.least_removed = 0;
  txn->wr.gc.spent = 0;
  tASSERT(txn, rkl_empty(&txn->wr.gc.reclaimed));
  txn->env->gc.detent = 0;
  env->txn = txn;

  return MDBX_SUCCESS;
}

int txn_basal_end(MDBX_txn *txn, unsigned mode) {
  MDBX_env *const env = txn->env;
  tASSERT(txn, (txn->flags & (MDBX_TXN_FINISHED | txn_may_have_cursors)) == 0 && txn->owner);
  ENSURE(env, txn->txnid >= /* paranoia is appropriate here */ env->lck->cached_oldest.weak);
  dxb_sanitize_tail(env, nullptr);

  txn->flags = MDBX_TXN_FINISHED;
  env->txn = nullptr;
  pnl_free(txn->wr.spilled.list);
  txn->wr.spilled.list = nullptr;
  rkl_clear_and_shrink(&txn->wr.gc.reclaimed);
  rkl_clear_and_shrink(&txn->wr.gc.comeback);

  eASSERT(env, txn->parent == nullptr);
  pnl_shrink(&txn->wr.retired_pages);
  pnl_shrink(&txn->wr.repnl);
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

int txn_basal_commit(MDBX_txn *txn, struct commit_timestamp *ts) {
  MDBX_env *const env = txn->env;
  tASSERT(txn, txn == env->basal_txn && !txn->parent && !txn->nested);
  if (!txn->wr.dirtylist) {
    tASSERT(txn, (txn->flags & MDBX_WRITEMAP) != 0 && !MDBX_AVOID_MSYNC);
  } else {
    tASSERT(txn, (txn->flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);
    tASSERT(txn, txn->wr.dirtyroom + txn->wr.dirtylist->length == env->options.dp_limit);
  }

  if (txn->flags & txn_may_have_cursors)
    txn_done_cursors(txn);

  bool need_flush_for_nometasync = false;
  const meta_ptr_t head = meta_recent(env, &txn->wr.troika);
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
      int err = meta_sync(env, head);
      if (unlikely(err != MDBX_SUCCESS)) {
        ERROR("txn-%s: error %d", "presync-meta", err);
        return err;
      }
    }
  }

  if ((!txn->wr.dirtylist || txn->wr.dirtylist->length == 0) &&
      (txn->flags & (MDBX_TXN_DIRTY | MDBX_TXN_SPILLS | MDBX_TXN_NOSYNC | MDBX_TXN_NOMETASYNC)) == 0 &&
      !need_flush_for_nometasync && !head.is_steady && !AUDIT_ENABLED()) {
    TXN_FOREACH_DBI_ALL(txn, i) { tASSERT(txn, !(txn->dbi_state[i] & DBI_DIRTY)); }
    /* fast completion of pure transaction */
    return MDBX_NOSUCCESS_PURE_COMMIT ? MDBX_RESULT_TRUE : MDBX_SUCCESS;
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
  int rc = gc_put_init(txn, &gcu_ctx);
  if (likely(rc == MDBX_SUCCESS))
    rc = gc_update(txn, &gcu_ctx);

#if MDBX_ENABLE_BIGFOOT
  const txnid_t commit_txnid = gcu_ctx.bigfoot;
  if (commit_txnid > txn->txnid)
    TRACE("use @%" PRIaTXN " (+%zu) for commit bigfoot-txn", commit_txnid, (size_t)(commit_txnid - txn->txnid));
#else
  const txnid_t commit_txnid = txn->txnid;
#endif
  gc_put_destroy(&gcu_ctx);

  if (ts)
    ts->gc_cpu = osal_cputime(nullptr) - ts->gc_cpu;
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  tASSERT(txn, txn->wr.loose_count == 0);
  txn->dbs[FREE_DBI].mod_txnid = (txn->dbi_state[FREE_DBI] & DBI_DIRTY) ? txn->txnid : txn->dbs[FREE_DBI].mod_txnid;
  txn->dbs[MAIN_DBI].mod_txnid = (txn->dbi_state[MAIN_DBI] & DBI_DIRTY) ? txn->txnid : txn->dbs[MAIN_DBI].mod_txnid;

  if (ts) {
    ts->gc = osal_monotime();
    ts->audit = ts->gc;
  }
  if (AUDIT_ENABLED()) {
    rc = audit_ex(txn, MDBX_PNL_GETSIZE(txn->wr.retired_pages), true);
    if (ts)
      ts->audit = osal_monotime();
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }

  if (txn->wr.dirtylist) {
    tASSERT(txn, (txn->flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);
    tASSERT(txn, txn->wr.loose_count == 0);

    mdbx_filehandle_t fd =
#if defined(_WIN32) || defined(_WIN64)
        env->ioring.overlapped_fd ? env->ioring.overlapped_fd : env->lazy_fd;
    (void)need_flush_for_nometasync;
#else
        (need_flush_for_nometasync || env->dsync_fd == INVALID_HANDLE_VALUE ||
         txn->wr.dirtylist->length > env->options.writethrough_threshold ||
         atomic_load64(&env->lck->unsynced_pages, mo_Relaxed))
            ? env->lazy_fd
            : env->dsync_fd;
#endif /* Windows */

    iov_ctx_t write_ctx;
    rc = iov_init(txn, &write_ctx, txn->wr.dirtylist->length, txn->wr.dirtylist->pages_including_loose, fd, false);
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
    env->lck->unsynced_pages.weak += txn->wr.writemap_dirty_npages;
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
                     unaligned_peek_u64(4, head.ptr_c->pages_retired) + MDBX_PNL_GETSIZE(txn->wr.retired_pages));
  meta.geometry = txn->geo;
  meta.trees.gc = txn->dbs[FREE_DBI];
  meta.trees.main = txn->dbs[MAIN_DBI];
  meta.canary = txn->canary;
  memcpy(&meta.dxbid, &head.ptr_c->dxbid, sizeof(meta.dxbid));

  meta.unsafe_sign = DATASIGN_NONE;
  meta_set_txnid(env, &meta, commit_txnid);

  rc = dxb_sync_locked(env, env->flags | txn->flags | txn_shrink_allowed, &meta, &txn->wr.troika);

  if (ts)
    ts->sync = osal_monotime();
  if (unlikely(rc != MDBX_SUCCESS)) {
    env->flags |= ENV_FATAL_ERROR;
    ERROR("txn-%s: error %d", "sync", rc);
    return rc;
  }

  return MDBX_SUCCESS;
}
