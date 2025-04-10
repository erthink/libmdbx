/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025

#include "internals.h"

#ifdef __SANITIZE_THREAD__
/* LY: avoid tsan-trap by txn, mm_last_pg and geo.first_unallocated */
__attribute__((__no_sanitize_thread__, __noinline__))
#endif
int mdbx_txn_straggler(const MDBX_txn *txn, int *percent)
{
  int rc = check_txn(txn, MDBX_TXN_BLOCKED - MDBX_TXN_PARKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR((rc > 0) ? -rc : rc);

  MDBX_env *env = txn->env;
  if (unlikely((txn->flags & MDBX_TXN_RDONLY) == 0)) {
    if (percent)
      *percent = (int)((txn->geo.first_unallocated * UINT64_C(100) + txn->geo.end_pgno / 2) / txn->geo.end_pgno);
    return 0;
  }

  txnid_t lag;
  troika_t troika = meta_tap(env);
  do {
    const meta_ptr_t head = meta_recent(env, &troika);
    if (percent) {
      const pgno_t maxpg = head.ptr_v->geometry.now;
      *percent = (int)((head.ptr_v->geometry.first_unallocated * UINT64_C(100) + maxpg / 2) / maxpg);
    }
    lag = (head.txnid - txn->txnid) / xMDBX_TXNID_STEP;
  } while (unlikely(meta_should_retry(env, &troika)));

  return (lag > INT_MAX) ? INT_MAX : (int)lag;
}

MDBX_env *mdbx_txn_env(const MDBX_txn *txn) {
  if (unlikely(!txn || txn->signature != txn_signature || txn->env->signature.weak != env_signature))
    return nullptr;
  return txn->env;
}

uint64_t mdbx_txn_id(const MDBX_txn *txn) {
  if (unlikely(!txn || txn->signature != txn_signature))
    return 0;
  return txn->txnid;
}

MDBX_txn_flags_t mdbx_txn_flags(const MDBX_txn *txn) {
  STATIC_ASSERT(
      (MDBX_TXN_INVALID & (MDBX_TXN_FINISHED | MDBX_TXN_ERROR | MDBX_TXN_DIRTY | MDBX_TXN_SPILLS | MDBX_TXN_HAS_CHILD |
                           txn_gc_drained | txn_shrink_allowed | txn_rw_begin_flags | txn_ro_begin_flags)) == 0);
  if (unlikely(!txn || txn->signature != txn_signature))
    return MDBX_TXN_INVALID;
  assert(0 == (int)(txn->flags & MDBX_TXN_INVALID));

  MDBX_txn_flags_t flags = txn->flags;
  if (F_ISSET(flags, MDBX_TXN_PARKED | MDBX_TXN_RDONLY) && txn->to.reader &&
      safe64_read(&txn->to.reader->tid) == MDBX_TID_TXN_OUSTED)
    flags |= MDBX_TXN_OUSTED;
  return flags;
}

int mdbx_txn_reset(MDBX_txn *txn) {
  int rc = check_txn(txn, 0);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  /* This call is only valid for read-only txns */
  if (unlikely((txn->flags & MDBX_TXN_RDONLY) == 0))
    return LOG_IFERR(MDBX_EINVAL);

  /* LY: don't close DBI-handles */
  rc = txn_end(txn, TXN_END_RESET | TXN_END_UPDATE);
  if (rc == MDBX_SUCCESS) {
    tASSERT(txn, txn->signature == txn_signature);
    tASSERT(txn, txn->owner == 0);
  }
  return LOG_IFERR(rc);
}

int mdbx_txn_break(MDBX_txn *txn) {
  do {
    int rc = check_txn(txn, 0);
    if (unlikely(rc != MDBX_SUCCESS))
      return LOG_IFERR(rc);
    txn->flags |= MDBX_TXN_ERROR;
    if (txn->flags & MDBX_TXN_RDONLY)
      break;
    txn = txn->nested;
  } while (txn);
  return MDBX_SUCCESS;
}

int mdbx_txn_abort(MDBX_txn *txn) {
  int rc = check_txn(txn, 0);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  rc = check_env(txn->env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

#if MDBX_TXN_CHECKOWNER
  if ((txn->flags & (MDBX_TXN_RDONLY | MDBX_NOSTICKYTHREADS)) == MDBX_NOSTICKYTHREADS &&
      unlikely(txn->owner != osal_thread_self())) {
    mdbx_txn_break(txn);
    return LOG_IFERR(MDBX_THREAD_MISMATCH);
  }
#endif /* MDBX_TXN_CHECKOWNER */

  return LOG_IFERR(txn_abort(txn));
}

int mdbx_txn_park(MDBX_txn *txn, bool autounpark) {
  STATIC_ASSERT(MDBX_TXN_BLOCKED > MDBX_TXN_ERROR);
  int rc = check_txn(txn, MDBX_TXN_BLOCKED - MDBX_TXN_ERROR);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);
  if (unlikely((txn->flags & MDBX_TXN_RDONLY) == 0))
    return LOG_IFERR(MDBX_TXN_INVALID);

  if (unlikely((txn->flags & MDBX_TXN_ERROR))) {
    rc = txn_end(txn, TXN_END_RESET | TXN_END_UPDATE);
    return LOG_IFERR(rc ? rc : MDBX_OUSTED);
  }

  return LOG_IFERR(txn_park(txn, autounpark));
}

int mdbx_txn_unpark(MDBX_txn *txn, bool restart_if_ousted) {
  STATIC_ASSERT(MDBX_TXN_BLOCKED > MDBX_TXN_PARKED + MDBX_TXN_ERROR);
  int rc = check_txn(txn, MDBX_TXN_BLOCKED - MDBX_TXN_PARKED - MDBX_TXN_ERROR);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);
  if (unlikely(!F_ISSET(txn->flags, MDBX_TXN_RDONLY | MDBX_TXN_PARKED)))
    return MDBX_SUCCESS;

  rc = txn_unpark(txn);
  if (likely(rc != MDBX_OUSTED) || !restart_if_ousted)
    return LOG_IFERR(rc);

  tASSERT(txn, txn->flags & MDBX_TXN_FINISHED);
  rc = txn_renew(txn, MDBX_TXN_RDONLY);
  return (rc == MDBX_SUCCESS) ? MDBX_RESULT_TRUE : LOG_IFERR(rc);
}

int mdbx_txn_renew(MDBX_txn *txn) {
  if (unlikely(!txn))
    return LOG_IFERR(MDBX_EINVAL);

  if (unlikely(txn->signature != txn_signature))
    return LOG_IFERR(MDBX_EBADSIGN);

  if (unlikely((txn->flags & MDBX_TXN_RDONLY) == 0))
    return LOG_IFERR(MDBX_EINVAL);

  if (unlikely(txn->owner != 0 || !(txn->flags & MDBX_TXN_FINISHED))) {
    int rc = mdbx_txn_reset(txn);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }

  int rc = txn_renew(txn, MDBX_TXN_RDONLY);
  if (rc == MDBX_SUCCESS) {
    tASSERT(txn, txn->owner == (txn->flags & MDBX_NOSTICKYTHREADS) ? 0 : osal_thread_self());
    DEBUG("renew txn %" PRIaTXN "%c %p on env %p, root page %" PRIaPGNO "/%" PRIaPGNO, txn->txnid,
          (txn->flags & MDBX_TXN_RDONLY) ? 'r' : 'w', (void *)txn, (void *)txn->env, txn->dbs[MAIN_DBI].root,
          txn->dbs[FREE_DBI].root);
  }
  return LOG_IFERR(rc);
}

int mdbx_txn_set_userctx(MDBX_txn *txn, void *ctx) {
  int rc = check_txn(txn, MDBX_TXN_FINISHED);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  txn->userctx = ctx;
  return MDBX_SUCCESS;
}

void *mdbx_txn_get_userctx(const MDBX_txn *txn) { return check_txn(txn, MDBX_TXN_FINISHED) ? nullptr : txn->userctx; }

int mdbx_txn_begin_ex(MDBX_env *env, MDBX_txn *parent, MDBX_txn_flags_t flags, MDBX_txn **ret, void *context) {
  if (unlikely(!ret))
    return LOG_IFERR(MDBX_EINVAL);
  *ret = nullptr;

  if (unlikely((flags & ~txn_rw_begin_flags) && (parent || (flags & ~txn_ro_begin_flags))))
    return LOG_IFERR(MDBX_EINVAL);

  int rc = check_env(env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  if (unlikely(env->flags & MDBX_RDONLY & ~flags)) /* write txn in RDONLY env */
    return LOG_IFERR(MDBX_EACCESS);

  MDBX_txn *txn = nullptr;
  if (parent) {
    /* Nested transactions: Max 1 child, write txns only, no writemap */
    rc = check_txn(parent, MDBX_TXN_BLOCKED - MDBX_TXN_PARKED);
    if (unlikely(rc != MDBX_SUCCESS))
      return LOG_IFERR(rc);

    if (unlikely(parent->flags & (MDBX_TXN_RDONLY | MDBX_WRITEMAP))) {
      rc = MDBX_BAD_TXN;
      if ((parent->flags & MDBX_TXN_RDONLY) == 0) {
        ERROR("%s mode is incompatible with nested transactions", "MDBX_WRITEMAP");
        rc = MDBX_INCOMPATIBLE;
      }
      return LOG_IFERR(rc);
    }

    if (env->options.spill_parent4child_denominator) {
      /* Spill dirty-pages of parent to provide dirtyroom for child txn */
      rc = txn_spill(parent, nullptr, parent->tw.dirtylist->length / env->options.spill_parent4child_denominator);
      if (unlikely(rc != MDBX_SUCCESS))
        return LOG_IFERR(rc);
    }
    tASSERT(parent, audit_ex(parent, 0, false) == 0);

    flags |= parent->flags & (txn_rw_begin_flags | MDBX_TXN_SPILLS | MDBX_NOSTICKYTHREADS | MDBX_WRITEMAP);
  } else if ((flags & MDBX_TXN_RDONLY) == 0) {
    /* Reuse preallocated write txn. However, do not touch it until
     * txn_renew() succeeds, since it currently may be active. */
    txn = env->basal_txn;
    goto renew;
  }

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
  if (unlikely(txn == nullptr))
    return LOG_IFERR(MDBX_ENOMEM);
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

  if (parent) {
    tASSERT(parent, dpl_check(parent));
#if MDBX_ENABLE_DBI_SPARSE
    txn->dbi_sparse = parent->dbi_sparse;
#endif /* MDBX_ENABLE_DBI_SPARSE */
    txn->dbi_seqs = parent->dbi_seqs;
    txn->geo = parent->geo;
    rc = dpl_alloc(txn);
    if (likely(rc == MDBX_SUCCESS)) {
      const size_t len = MDBX_PNL_GETSIZE(parent->tw.repnl) + parent->tw.loose_count;
      txn->tw.repnl = pnl_alloc((len > MDBX_PNL_INITIAL) ? len : MDBX_PNL_INITIAL);
      if (unlikely(!txn->tw.repnl))
        rc = MDBX_ENOMEM;
    }
    if (unlikely(rc != MDBX_SUCCESS)) {
    nested_failed:
      pnl_free(txn->tw.repnl);
      dpl_free(txn);
      osal_free(txn);
      return LOG_IFERR(rc);
    }

    /* Move loose pages to reclaimed list */
    if (parent->tw.loose_count) {
      do {
        page_t *lp = parent->tw.loose_pages;
        tASSERT(parent, lp->flags == P_LOOSE);
        rc = pnl_insert_span(&parent->tw.repnl, lp->pgno, 1);
        if (unlikely(rc != MDBX_SUCCESS))
          goto nested_failed;
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
    eASSERT(env, pnl_check_allocated(txn->tw.repnl, (txn->geo.first_unallocated /* LY: intentional assignment
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
    env->txn = txn;
    tASSERT(parent, parent->cursors[FREE_DBI] == nullptr);
    rc = parent->cursors[MAIN_DBI] ? cursor_shadow(parent->cursors[MAIN_DBI], txn, MAIN_DBI) : MDBX_SUCCESS;
    if (AUDIT_ENABLED() && ASSERT_ENABLED()) {
      txn->signature = txn_signature;
      tASSERT(txn, audit_ex(txn, 0, false) == 0);
    }
    if (unlikely(rc != MDBX_SUCCESS))
      txn_end(txn, TXN_END_FAIL_BEGINCHILD);
  } else { /* MDBX_TXN_RDONLY */
    txn->dbi_seqs = ptr_disp(txn->cursors, env->max_dbi * sizeof(txn->cursors[0]));
#if MDBX_ENABLE_DBI_SPARSE
    txn->dbi_sparse = ptr_disp(txn->dbi_state, -bitmap_bytes);
#endif /* MDBX_ENABLE_DBI_SPARSE */
  renew:
    rc = txn_renew(txn, flags);
  }

  if (unlikely(rc != MDBX_SUCCESS)) {
    if (txn != env->basal_txn)
      osal_free(txn);
  } else {
    if (flags & (MDBX_TXN_RDONLY_PREPARE - MDBX_TXN_RDONLY))
      eASSERT(env, txn->flags == (MDBX_TXN_RDONLY | MDBX_TXN_FINISHED));
    else if (flags & MDBX_TXN_RDONLY)
      eASSERT(env, (txn->flags & ~(MDBX_NOSTICKYTHREADS | MDBX_TXN_RDONLY | MDBX_WRITEMAP |
                                   /* Win32: SRWL flag */ txn_shrink_allowed)) == 0);
    else {
      eASSERT(env, (txn->flags & ~(MDBX_NOSTICKYTHREADS | MDBX_WRITEMAP | txn_shrink_allowed | MDBX_NOMETASYNC |
                                   MDBX_SAFE_NOSYNC | MDBX_TXN_SPILLS)) == 0);
      assert(!txn->tw.spilled.list && !txn->tw.spilled.least_removed);
    }
    txn->signature = txn_signature;
    txn->userctx = context;
    *ret = txn;
    DEBUG("begin txn %" PRIaTXN "%c %p on env %p, root page %" PRIaPGNO "/%" PRIaPGNO, txn->txnid,
          (flags & MDBX_TXN_RDONLY) ? 'r' : 'w', (void *)txn, (void *)env, txn->dbs[MAIN_DBI].root,
          txn->dbs[FREE_DBI].root);
  }

  return LOG_IFERR(rc);
}

int mdbx_txn_commit_ex(MDBX_txn *txn, MDBX_commit_latency *latency) {
  STATIC_ASSERT(MDBX_TXN_FINISHED == MDBX_TXN_BLOCKED - MDBX_TXN_HAS_CHILD - MDBX_TXN_ERROR - MDBX_TXN_PARKED);
  const uint64_t ts_0 = latency ? osal_monotime() : 0;
  uint64_t ts_1 = 0, ts_2 = 0, ts_3 = 0, ts_4 = 0, ts_5 = 0, gc_cputime = 0;

  /* txn_end() mode for a commit which writes nothing */
  unsigned end_mode = TXN_END_PURE_COMMIT | TXN_END_UPDATE | TXN_END_SLOT | TXN_END_FREE;

  int rc = check_txn(txn, MDBX_TXN_FINISHED);
  if (unlikely(rc != MDBX_SUCCESS)) {
    if (rc == MDBX_BAD_TXN && (txn->flags & MDBX_TXN_RDONLY)) {
      rc = MDBX_RESULT_TRUE;
      goto fail;
    }
  bailout:
    if (latency)
      memset(latency, 0, sizeof(*latency));
    return LOG_IFERR(rc);
  }

  MDBX_env *const env = txn->env;
  if (MDBX_ENV_CHECKPID && unlikely(env->pid != osal_getpid())) {
    env->flags |= ENV_FATAL_ERROR;
    rc = MDBX_PANIC;
    goto bailout;
  }

  if (unlikely(txn->flags & MDBX_TXN_RDONLY)) {
    if (txn->flags & MDBX_TXN_ERROR) {
      rc = MDBX_RESULT_TRUE;
      goto fail;
    }
    goto done;
  }

#if MDBX_TXN_CHECKOWNER
  if ((txn->flags & MDBX_NOSTICKYTHREADS) && txn == env->basal_txn && unlikely(txn->owner != osal_thread_self())) {
    txn->flags |= MDBX_TXN_ERROR;
    rc = MDBX_THREAD_MISMATCH;
    return LOG_IFERR(rc);
  }
#endif /* MDBX_TXN_CHECKOWNER */

  if (unlikely(txn->flags & MDBX_TXN_ERROR)) {
    rc = MDBX_RESULT_TRUE;
    goto fail;
  }

  if (txn->nested) {
    rc = mdbx_txn_commit_ex(txn->nested, nullptr);
    tASSERT(txn, txn->nested == nullptr);
    if (unlikely(rc != MDBX_SUCCESS))
      goto fail;
  }

  if (unlikely(txn != env->txn)) {
    DEBUG("%s", "attempt to commit unknown transaction");
    rc = MDBX_EINVAL;
    goto fail;
  }

  if (txn->parent) {
    tASSERT(txn, audit_ex(txn, 0, false) == 0);
    eASSERT(env, txn != env->basal_txn);
    MDBX_txn *const parent = txn->parent;
    eASSERT(env, parent->signature == txn_signature);
    eASSERT(env, parent->nested == txn && (parent->flags & MDBX_TXN_HAS_CHILD) != 0);
    eASSERT(env, dpl_check(txn));

    if (txn->tw.dirtylist->length == 0 && !(txn->flags & MDBX_TXN_DIRTY) && parent->n_dbi == txn->n_dbi) {
      /* fast completion of pure nested transaction */
      VERBOSE("fast-complete pure nested txn %" PRIaTXN, txn->txnid);

      tASSERT(txn, memcmp(&parent->geo, &txn->geo, sizeof(parent->geo)) == 0);
      tASSERT(txn, memcmp(&parent->canary, &txn->canary, sizeof(parent->canary)) == 0);
      tASSERT(txn, !txn->tw.spilled.list || MDBX_PNL_GETSIZE(txn->tw.spilled.list) == 0);
      tASSERT(txn, txn->tw.loose_count == 0);

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
      txn_done_cursors(txn, true);
      end_mode = TXN_END_PURE_COMMIT | TXN_END_SLOT | TXN_END_FREE | TXN_END_EOTDONE;
      goto done;
    }

    /* Preserve space for spill list to avoid parent's state corruption
     * if allocation fails. */
    const size_t parent_retired_len = (uintptr_t)parent->tw.retired_pages;
    tASSERT(txn, parent_retired_len <= MDBX_PNL_GETSIZE(txn->tw.retired_pages));
    const size_t retired_delta = MDBX_PNL_GETSIZE(txn->tw.retired_pages) - parent_retired_len;
    if (retired_delta) {
      rc = pnl_need(&txn->tw.repnl, retired_delta);
      if (unlikely(rc != MDBX_SUCCESS))
        goto fail;
    }

    if (txn->tw.spilled.list) {
      if (parent->tw.spilled.list) {
        rc = pnl_need(&parent->tw.spilled.list, MDBX_PNL_GETSIZE(txn->tw.spilled.list));
        if (unlikely(rc != MDBX_SUCCESS))
          goto fail;
      }
      spill_purge(txn);
    }

    if (unlikely(txn->tw.dirtylist->length + parent->tw.dirtylist->length > parent->tw.dirtylist->detent &&
                 !dpl_reserve(parent, txn->tw.dirtylist->length + parent->tw.dirtylist->length))) {
      rc = MDBX_ENOMEM;
      goto fail;
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

    /* Merge our cursors into parent's and close them */
    txn_done_cursors(txn, true);
    end_mode |= TXN_END_EOTDONE;

    /* Update parent's DBs array */
    eASSERT(env, parent->n_dbi == txn->n_dbi);
    TXN_FOREACH_DBI_ALL(txn, dbi) {
      if (txn->dbi_state[dbi] != (parent->dbi_state[dbi] & ~(DBI_FRESH | DBI_CREAT | DBI_DIRTY))) {
        eASSERT(env, (txn->dbi_state[dbi] & (DBI_CREAT | DBI_FRESH | DBI_DIRTY)) != 0 ||
                         (txn->dbi_state[dbi] | DBI_STALE) ==
                             (parent->dbi_state[dbi] & ~(DBI_FRESH | DBI_CREAT | DBI_DIRTY)));
        parent->dbs[dbi] = txn->dbs[dbi];
        /* preserve parent's status */
        const uint8_t state = txn->dbi_state[dbi] | (parent->dbi_state[dbi] & (DBI_CREAT | DBI_FRESH | DBI_DIRTY));
        DEBUG("dbi %zu dbi-state %s 0x%02x -> 0x%02x", dbi, (parent->dbi_state[dbi] != state) ? "update" : "still",
              parent->dbi_state[dbi], state);
        parent->dbi_state[dbi] = state;
      }
    }

    if (latency) {
      ts_1 = osal_monotime();
      ts_2 = /* no gc-update */ ts_1;
      ts_3 = /* no audit */ ts_2;
      ts_4 = /* no write */ ts_3;
      ts_5 = /* no sync */ ts_4;
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
    rc = MDBX_SUCCESS;
    goto provide_latency;
  }

  if (!txn->tw.dirtylist) {
    tASSERT(txn, (txn->flags & MDBX_WRITEMAP) != 0 && !MDBX_AVOID_MSYNC);
  } else {
    tASSERT(txn, (txn->flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);
    tASSERT(txn, txn->tw.dirtyroom + txn->tw.dirtylist->length ==
                     (txn->parent ? txn->parent->tw.dirtyroom : env->options.dp_limit));
  }
  txn_done_cursors(txn, false);
  end_mode |= TXN_END_EOTDONE;

  if ((!txn->tw.dirtylist || txn->tw.dirtylist->length == 0) &&
      (txn->flags & (MDBX_TXN_DIRTY | MDBX_TXN_SPILLS)) == 0) {
    TXN_FOREACH_DBI_ALL(txn, i) { tASSERT(txn, !(txn->dbi_state[i] & DBI_DIRTY)); }
#if defined(MDBX_NOSUCCESS_EMPTY_COMMIT) && MDBX_NOSUCCESS_EMPTY_COMMIT
    rc = txn_end(txn, end_mode);
    if (unlikely(rc != MDBX_SUCCESS))
      goto fail;
    rc = MDBX_RESULT_TRUE;
    goto provide_latency;
#else
    goto done;
#endif /* MDBX_NOSUCCESS_EMPTY_COMMIT */
  }

  DEBUG("committing txn %" PRIaTXN " %p on env %p, root page %" PRIaPGNO "/%" PRIaPGNO, txn->txnid, (void *)txn,
        (void *)env, txn->dbs[MAIN_DBI].root, txn->dbs[FREE_DBI].root);

  if (txn->n_dbi > CORE_DBS) {
    /* Update table root pointers */
    cursor_couple_t cx;
    rc = cursor_init(&cx.outer, txn, MAIN_DBI);
    if (unlikely(rc != MDBX_SUCCESS))
      goto fail;
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
      rc = cursor_put(&cx.outer, &env->kvs[i].name, &data, N_TREE);
      if (unlikely(rc != MDBX_SUCCESS)) {
        txn->cursors[MAIN_DBI] = cx.outer.next;
        goto fail;
      }
    }
    txn->cursors[MAIN_DBI] = cx.outer.next;
  }

  ts_1 = latency ? osal_monotime() : 0;

  gcu_t gcu_ctx;
  gc_cputime = latency ? osal_cputime(nullptr) : 0;
  rc = gc_update_init(txn, &gcu_ctx);
  if (unlikely(rc != MDBX_SUCCESS))
    goto fail;
  rc = gc_update(txn, &gcu_ctx);
  gc_cputime = latency ? osal_cputime(nullptr) - gc_cputime : 0;
  if (unlikely(rc != MDBX_SUCCESS))
    goto fail;

  tASSERT(txn, txn->tw.loose_count == 0);
  txn->dbs[FREE_DBI].mod_txnid = (txn->dbi_state[FREE_DBI] & DBI_DIRTY) ? txn->txnid : txn->dbs[FREE_DBI].mod_txnid;

  txn->dbs[MAIN_DBI].mod_txnid = (txn->dbi_state[MAIN_DBI] & DBI_DIRTY) ? txn->txnid : txn->dbs[MAIN_DBI].mod_txnid;

  ts_2 = latency ? osal_monotime() : 0;
  ts_3 = ts_2;
  if (AUDIT_ENABLED()) {
    rc = audit_ex(txn, MDBX_PNL_GETSIZE(txn->tw.retired_pages), true);
    ts_3 = osal_monotime();
    if (unlikely(rc != MDBX_SUCCESS))
      goto fail;
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
        goto fail;
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
      goto fail;
    }

    rc = txn_write(txn, &write_ctx);
    if (unlikely(rc != MDBX_SUCCESS)) {
      ERROR("txn-%s: error %d", "write", rc);
      goto fail;
    }
  } else {
    tASSERT(txn, (txn->flags & MDBX_WRITEMAP) != 0 && !MDBX_AVOID_MSYNC);
    env->lck->unsynced_pages.weak += txn->tw.writemap_dirty_npages;
    if (!env->lck->eoos_timestamp.weak)
      env->lck->eoos_timestamp.weak = osal_monotime();
  }

  /* TODO: use ctx.flush_begin & ctx.flush_end for range-sync */
  ts_4 = latency ? osal_monotime() : 0;

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

  ts_5 = latency ? osal_monotime() : 0;
  if (unlikely(rc != MDBX_SUCCESS)) {
    env->flags |= ENV_FATAL_ERROR;
    ERROR("txn-%s: error %d", "sync", rc);
    goto fail;
  }

  end_mode = TXN_END_COMMITTED | TXN_END_UPDATE | TXN_END_EOTDONE;

done:
  if (latency)
    txn_take_gcprof(txn, latency);
  rc = txn_end(txn, end_mode);

provide_latency:
  if (latency) {
    latency->preparation = ts_1 ? osal_monotime_to_16dot16(ts_1 - ts_0) : 0;
    latency->gc_wallclock = (ts_2 > ts_1) ? osal_monotime_to_16dot16(ts_2 - ts_1) : 0;
    latency->gc_cputime = gc_cputime ? osal_monotime_to_16dot16(gc_cputime) : 0;
    latency->audit = (ts_3 > ts_2) ? osal_monotime_to_16dot16(ts_3 - ts_2) : 0;
    latency->write = (ts_4 > ts_3) ? osal_monotime_to_16dot16(ts_4 - ts_3) : 0;
    latency->sync = (ts_5 > ts_4) ? osal_monotime_to_16dot16(ts_5 - ts_4) : 0;
    const uint64_t ts_6 = osal_monotime();
    latency->ending = ts_5 ? osal_monotime_to_16dot16(ts_6 - ts_5) : 0;
    latency->whole = osal_monotime_to_16dot16_noUnderflow(ts_6 - ts_0);
  }
  return LOG_IFERR(rc);

fail:
  txn->flags |= MDBX_TXN_ERROR;
  if (latency)
    txn_take_gcprof(txn, latency);
  txn_abort(txn);
  goto provide_latency;
}

int mdbx_txn_info(const MDBX_txn *txn, MDBX_txn_info *info, bool scan_rlt) {
  int rc = check_txn(txn, MDBX_TXN_FINISHED);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  if (unlikely(!info))
    return LOG_IFERR(MDBX_EINVAL);

  MDBX_env *const env = txn->env;
#if MDBX_ENV_CHECKPID
  if (unlikely(env->pid != osal_getpid())) {
    env->flags |= ENV_FATAL_ERROR;
    return LOG_IFERR(MDBX_PANIC);
  }
#endif /* MDBX_ENV_CHECKPID */

  info->txn_id = txn->txnid;
  info->txn_space_used = pgno2bytes(env, txn->geo.first_unallocated);

  if (txn->flags & MDBX_TXN_RDONLY) {
    meta_ptr_t head;
    uint64_t head_retired;
    troika_t troika = meta_tap(env);
    do {
      /* fetch info from volatile head */
      head = meta_recent(env, &troika);
      head_retired = unaligned_peek_u64_volatile(4, head.ptr_v->pages_retired);
      info->txn_space_limit_soft = pgno2bytes(env, head.ptr_v->geometry.now);
      info->txn_space_limit_hard = pgno2bytes(env, head.ptr_v->geometry.upper);
      info->txn_space_leftover = pgno2bytes(env, head.ptr_v->geometry.now - head.ptr_v->geometry.first_unallocated);
    } while (unlikely(meta_should_retry(env, &troika)));

    info->txn_reader_lag = head.txnid - info->txn_id;
    info->txn_space_dirty = info->txn_space_retired = 0;
    uint64_t reader_snapshot_pages_retired = 0;
    if (txn->to.reader &&
        ((txn->flags & MDBX_TXN_PARKED) == 0 || safe64_read(&txn->to.reader->tid) != MDBX_TID_TXN_OUSTED) &&
        head_retired >
            (reader_snapshot_pages_retired = atomic_load64(&txn->to.reader->snapshot_pages_retired, mo_Relaxed))) {
      info->txn_space_dirty = info->txn_space_retired =
          pgno2bytes(env, (pgno_t)(head_retired - reader_snapshot_pages_retired));

      size_t retired_next_reader = 0;
      lck_t *const lck = env->lck_mmap.lck;
      if (scan_rlt && info->txn_reader_lag > 1 && lck) {
        /* find next more recent reader */
        txnid_t next_reader = head.txnid;
        const size_t snap_nreaders = atomic_load32(&lck->rdt_length, mo_AcquireRelease);
        for (size_t i = 0; i < snap_nreaders; ++i) {
        retry:
          if (atomic_load32(&lck->rdt[i].pid, mo_AcquireRelease)) {
            jitter4testing(true);
            const uint64_t snap_tid = safe64_read(&lck->rdt[i].tid);
            const txnid_t snap_txnid = safe64_read(&lck->rdt[i].txnid);
            const uint64_t snap_retired = atomic_load64(&lck->rdt[i].snapshot_pages_retired, mo_AcquireRelease);
            if (unlikely(snap_retired != atomic_load64(&lck->rdt[i].snapshot_pages_retired, mo_Relaxed)) ||
                snap_txnid != safe64_read(&lck->rdt[i].txnid) || snap_tid != safe64_read(&lck->rdt[i].tid))
              goto retry;
            if (snap_txnid <= txn->txnid) {
              retired_next_reader = 0;
              break;
            }
            if (snap_txnid < next_reader && snap_tid >= MDBX_TID_TXN_OUSTED) {
              next_reader = snap_txnid;
              retired_next_reader = pgno2bytes(
                  env, (pgno_t)(snap_retired - atomic_load64(&txn->to.reader->snapshot_pages_retired, mo_Relaxed)));
            }
          }
        }
      }
      info->txn_space_dirty = retired_next_reader;
    }
  } else {
    info->txn_space_limit_soft = pgno2bytes(env, txn->geo.now);
    info->txn_space_limit_hard = pgno2bytes(env, txn->geo.upper);
    info->txn_space_retired =
        pgno2bytes(env, txn->nested ? (size_t)txn->tw.retired_pages : MDBX_PNL_GETSIZE(txn->tw.retired_pages));
    info->txn_space_leftover = pgno2bytes(env, txn->tw.dirtyroom);
    info->txn_space_dirty =
        pgno2bytes(env, txn->tw.dirtylist ? txn->tw.dirtylist->pages_including_loose
                                          : (txn->tw.writemap_dirty_npages + txn->tw.writemap_spilled_npages));
    info->txn_reader_lag = INT64_MAX;
    lck_t *const lck = env->lck_mmap.lck;
    if (scan_rlt && lck) {
      txnid_t oldest_snapshot = txn->txnid;
      const size_t snap_nreaders = atomic_load32(&lck->rdt_length, mo_AcquireRelease);
      if (snap_nreaders) {
        oldest_snapshot = txn_snapshot_oldest(txn);
        if (oldest_snapshot == txn->txnid - 1) {
          /* check if there is at least one reader */
          bool exists = false;
          for (size_t i = 0; i < snap_nreaders; ++i) {
            if (atomic_load32(&lck->rdt[i].pid, mo_Relaxed) && txn->txnid > safe64_read(&lck->rdt[i].txnid)) {
              exists = true;
              break;
            }
          }
          oldest_snapshot += !exists;
        }
      }
      info->txn_reader_lag = txn->txnid - oldest_snapshot;
    }
  }

  return MDBX_SUCCESS;
}
