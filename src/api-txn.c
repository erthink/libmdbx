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
  if (likely(rc == MDBX_SUCCESS))
    rc = check_env(txn->env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR((rc > 0) ? -rc : rc);

  if (unlikely((txn->flags & MDBX_TXN_RDONLY) == 0)) {
    if (percent)
      *percent = (int)((txn->geo.first_unallocated * UINT64_C(100) + txn->geo.end_pgno / 2) / txn->geo.end_pgno);
    return 0;
  }

  txnid_t lag;
  troika_t troika = meta_tap(txn->env);
  do {
    const meta_ptr_t head = meta_recent(txn->env, &troika);
    if (percent) {
      const pgno_t maxpg = head.ptr_v->geometry.now;
      *percent = (int)((head.ptr_v->geometry.first_unallocated * UINT64_C(100) + maxpg / 2) / maxpg);
    }
    lag = (head.txnid - txn->txnid) / xMDBX_TXNID_STEP;
  } while (unlikely(meta_should_retry(txn->env, &troika)));

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
  if (F_ISSET(flags, MDBX_TXN_PARKED | MDBX_TXN_RDONLY) && txn->ro.slot &&
      safe64_read(&txn->ro.slot->tid) == MDBX_TID_TXN_OUSTED)
    flags |= MDBX_TXN_OUSTED;
  return flags;
}

int mdbx_txn_reset(MDBX_txn *txn) {
  int rc = check_txn(txn, 0);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  rc = check_env(txn->env, false);
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

  rc = check_env(txn->env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  if (unlikely((txn->flags & MDBX_TXN_RDONLY) == 0))
    return LOG_IFERR(MDBX_TXN_INVALID);

  if (unlikely((txn->flags & MDBX_TXN_ERROR))) {
    rc = txn_end(txn, TXN_END_RESET | TXN_END_UPDATE);
    return LOG_IFERR(rc ? rc : MDBX_OUSTED);
  }

  return LOG_IFERR(txn_ro_park(txn, autounpark));
}

int mdbx_txn_unpark(MDBX_txn *txn, bool restart_if_ousted) {
  STATIC_ASSERT(MDBX_TXN_BLOCKED > MDBX_TXN_PARKED + MDBX_TXN_ERROR);
  int rc = check_txn(txn, MDBX_TXN_BLOCKED - MDBX_TXN_PARKED - MDBX_TXN_ERROR);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  rc = check_env(txn->env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  if (unlikely(!F_ISSET(txn->flags, MDBX_TXN_RDONLY | MDBX_TXN_PARKED)))
    return MDBX_SUCCESS;

  rc = txn_ro_unpark(txn);
  if (likely(rc != MDBX_OUSTED) || !restart_if_ousted)
    return LOG_IFERR(rc);

  tASSERT(txn, txn->flags & MDBX_TXN_FINISHED);
  rc = txn_renew(txn, MDBX_TXN_RDONLY);
  return (rc == MDBX_SUCCESS) ? MDBX_RESULT_TRUE : LOG_IFERR(rc);
}

int mdbx_txn_renew(MDBX_txn *txn) {
  int rc = check_txn(txn, 0);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  rc = check_env(txn->env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  if (unlikely((txn->flags & MDBX_TXN_RDONLY) == 0))
    return LOG_IFERR(MDBX_EINVAL);

  if (unlikely(txn->owner != 0 || !(txn->flags & MDBX_TXN_FINISHED))) {
    rc = mdbx_txn_reset(txn);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }

  rc = txn_renew(txn, MDBX_TXN_RDONLY);
  if (rc == MDBX_SUCCESS) {
    tASSERT(txn, txn->owner == (txn->flags & MDBX_NOSTICKYTHREADS) ? 0 : osal_thread_self());
    DEBUG("renew txn %" PRIaTXN "%c %p on env %p, root page %" PRIaPGNO "/%" PRIaPGNO, txn->txnid,
          (txn->flags & MDBX_TXN_RDONLY) ? 'r' : 'w', (void *)txn, (void *)txn->env, txn->dbs[MAIN_DBI].root,
          txn->dbs[FREE_DBI].root);
  }
  return LOG_IFERR(rc);
}

int mdbx_txn_set_userctx(MDBX_txn *txn, void *ctx) {
  int rc = check_txn(txn, 0);
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

  /* Reuse preallocated write txn. However, do not touch it until
   * txn_renew() succeeds, since it currently may be active. */
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
    if (unlikely(parent->env != env))
      return LOG_IFERR(MDBX_BAD_TXN);

    flags |= parent->flags & (txn_rw_begin_flags | MDBX_TXN_SPILLS | MDBX_NOSTICKYTHREADS | MDBX_WRITEMAP);
    rc = txn_nested_create(parent, flags);
    txn = parent->nested;
    if (unlikely(rc != MDBX_SUCCESS)) {
      int err = txn_end(txn, TXN_END_FAIL_BEGIN_NESTED);
      return err ? err : rc;
    }
    if (AUDIT_ENABLED() && ASSERT_ENABLED()) {
      txn->signature = txn_signature;
      tASSERT(txn, audit_ex(txn, 0, false) == 0);
    }
  } else {
    txn = env->basal_txn;
    if (flags & MDBX_TXN_RDONLY) {
      txn = txn_alloc(flags, env);
      if (unlikely(!txn))
        return LOG_IFERR(MDBX_ENOMEM);
    }
    rc = txn_renew(txn, flags);
    if (unlikely(rc != MDBX_SUCCESS)) {
      if (txn != env->basal_txn)
        osal_free(txn);
      return LOG_IFERR(rc);
    }
  }

  if (flags & (MDBX_TXN_RDONLY_PREPARE - MDBX_TXN_RDONLY))
    eASSERT(env, txn->flags == (MDBX_TXN_RDONLY | MDBX_TXN_FINISHED));
  else if (flags & MDBX_TXN_RDONLY)
    eASSERT(env, (txn->flags & ~(MDBX_NOSTICKYTHREADS | MDBX_TXN_RDONLY | MDBX_WRITEMAP |
                                 /* Win32: SRWL flag */ txn_shrink_allowed)) == 0);
  else {
    eASSERT(env, (txn->flags & ~(MDBX_NOSTICKYTHREADS | MDBX_WRITEMAP | txn_shrink_allowed | txn_may_have_cursors |
                                 MDBX_NOMETASYNC | MDBX_SAFE_NOSYNC | MDBX_TXN_SPILLS)) == 0);
    assert(!txn->wr.spilled.list && !txn->wr.spilled.least_removed);
  }
  txn->signature = txn_signature;
  txn->userctx = context;
  *ret = txn;
  DEBUG("begin txn %" PRIaTXN "%c %p on env %p, root page %" PRIaPGNO "/%" PRIaPGNO, txn->txnid,
        (flags & MDBX_TXN_RDONLY) ? 'r' : 'w', (void *)txn, (void *)env, txn->dbs[MAIN_DBI].root,
        txn->dbs[FREE_DBI].root);
  return MDBX_SUCCESS;
}

static void latency_gcprof(MDBX_commit_latency *latency, const MDBX_txn *txn) {
  MDBX_env *const env = txn->env;
  if (latency && likely(env->lck) && MDBX_ENABLE_PROFGC) {
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
  }
}

static void latency_init(MDBX_commit_latency *latency, struct commit_timestamp *ts) {
  ts->start = 0;
  ts->gc_cpu = 0;
  if (latency) {
    ts->start = osal_monotime();
    memset(latency, 0, sizeof(*latency));
  }
  ts->prep = ts->gc = ts->audit = ts->write = ts->sync = ts->start;
}

static void latency_done(MDBX_commit_latency *latency, struct commit_timestamp *ts) {
  if (latency) {
    latency->preparation = (ts->prep > ts->start) ? osal_monotime_to_16dot16(ts->prep - ts->start) : 0;
    latency->gc_wallclock = (ts->gc > ts->prep) ? osal_monotime_to_16dot16(ts->gc - ts->prep) : 0;
    latency->gc_cputime = ts->gc_cpu ? osal_monotime_to_16dot16(ts->gc_cpu) : 0;
    latency->audit = (ts->audit > ts->gc) ? osal_monotime_to_16dot16(ts->audit - ts->gc) : 0;
    latency->write = (ts->write > ts->audit) ? osal_monotime_to_16dot16(ts->write - ts->audit) : 0;
    latency->sync = (ts->sync > ts->write) ? osal_monotime_to_16dot16(ts->sync - ts->write) : 0;
    const uint64_t ts_end = osal_monotime();
    latency->ending = (ts_end > ts->sync) ? osal_monotime_to_16dot16(ts_end - ts->sync) : 0;
    latency->whole = osal_monotime_to_16dot16_noUnderflow(ts_end - ts->start);
  }
}

int mdbx_txn_commit_ex(MDBX_txn *txn, MDBX_commit_latency *latency) {
  STATIC_ASSERT(MDBX_TXN_FINISHED == MDBX_TXN_BLOCKED - MDBX_TXN_HAS_CHILD - MDBX_TXN_ERROR - MDBX_TXN_PARKED);

  struct commit_timestamp ts;
  latency_init(latency, &ts);

  int rc = check_txn(txn, MDBX_TXN_FINISHED);
  if (unlikely(rc != MDBX_SUCCESS)) {
    if (rc == MDBX_BAD_TXN && F_ISSET(txn->flags, MDBX_TXN_FINISHED | MDBX_TXN_RDONLY)) {
      rc = MDBX_RESULT_TRUE;
      goto fail;
    }
    return LOG_IFERR(rc);
  }

  MDBX_env *const env = txn->env;
  if (MDBX_ENV_CHECKPID && unlikely(env->pid != osal_getpid())) {
    env->flags |= ENV_FATAL_ERROR;
    rc = MDBX_PANIC;
    return LOG_IFERR(rc);
  }

  if (txn->flags & MDBX_TXN_RDONLY) {
    if (unlikely(txn->parent || (txn->flags & MDBX_TXN_HAS_CHILD) || txn == env->txn || txn == env->basal_txn)) {
      ERROR("attempt to commit %s txn %p", "strange read-only", (void *)txn);
      return MDBX_PROBLEM;
    }
    latency_gcprof(latency, txn);
    rc = (txn->flags & MDBX_TXN_ERROR) ? MDBX_RESULT_TRUE : MDBX_SUCCESS;
    txn_end(txn, TXN_END_PURE_COMMIT | TXN_END_UPDATE | TXN_END_SLOT | TXN_END_FREE);
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
  fail:
    latency_gcprof(latency, txn);
    int err = txn_abort(txn);
    if (unlikely(err != MDBX_SUCCESS))
      rc = err;
    goto done;
  }

  if (txn->nested) {
    rc = mdbx_txn_commit_ex(txn->nested, nullptr);
    tASSERT(txn, txn->nested == nullptr);
    if (unlikely(rc != MDBX_SUCCESS))
      goto fail;
  }

  if (unlikely(txn != env->txn)) {
    ERROR("attempt to commit %s txn %p", "unknown", (void *)txn);
    return MDBX_EINVAL;
  }

  if (txn->parent) {
    if (unlikely(txn->parent->nested != txn || txn->parent->env != env)) {
      ERROR("attempt to commit %s txn %p", "strange nested", (void *)txn);
      return MDBX_PROBLEM;
    }

    latency_gcprof(latency, txn);
    rc = txn_nested_join(txn, latency ? &ts : nullptr);
    goto done;
  }

  rc = txn_basal_commit(txn, latency ? &ts : nullptr);
  latency_gcprof(latency, txn);
  int end = TXN_END_COMMITTED | TXN_END_UPDATE;
  if (unlikely(rc != MDBX_SUCCESS)) {
    end = TXN_END_ABORT;
    if (rc == MDBX_RESULT_TRUE) {
      end = TXN_END_PURE_COMMIT | TXN_END_UPDATE;
      rc = MDBX_NOSUCCESS_PURE_COMMIT ? MDBX_RESULT_TRUE : MDBX_SUCCESS;
    }
  }
  int err = txn_end(txn, end);
  if (unlikely(err != MDBX_SUCCESS))
    rc = err;

done:
  latency_done(latency, &ts);
  return LOG_IFERR(rc);
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
    if (txn->ro.slot &&
        ((txn->flags & MDBX_TXN_PARKED) == 0 || safe64_read(&txn->ro.slot->tid) != MDBX_TID_TXN_OUSTED) &&
        head_retired >
            (reader_snapshot_pages_retired = atomic_load64(&txn->ro.slot->snapshot_pages_retired, mo_Relaxed))) {
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
                  env, (pgno_t)(snap_retired - atomic_load64(&txn->ro.slot->snapshot_pages_retired, mo_Relaxed)));
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
        pgno2bytes(env, txn->nested ? (size_t)txn->wr.retired_pages : pnl_size(txn->wr.retired_pages));
    info->txn_space_leftover = pgno2bytes(env, txn->wr.dirtyroom);
    info->txn_space_dirty =
        pgno2bytes(env, txn->wr.dirtylist ? txn->wr.dirtylist->pages_including_loose
                                          : (txn->wr.writemap_dirty_npages + txn->wr.writemap_spilled_npages));
    info->txn_reader_lag = INT64_MAX;
    lck_t *const lck = env->lck_mmap.lck;
    if (scan_rlt && lck) {
      txnid_t oldest_reading = txn->txnid;
      const size_t snap_nreaders = atomic_load32(&lck->rdt_length, mo_AcquireRelease);
      if (snap_nreaders) {
        txn_gc_detent(txn);
        oldest_reading = txn->env->gc.detent;
        if (oldest_reading == txn->wr.troika.txnid[txn->wr.troika.recent]) {
          /* Если самый старый используемый снимок является предыдущим, т.е. непосредственно предшествующим текущей
           * транзакции, то просматриваем таблицу читателей чтобы выяснить действительно ли снимок используется
           * читателями. */
          oldest_reading = txn->txnid;
          for (size_t i = 0; i < snap_nreaders; ++i) {
            if (atomic_load32(&lck->rdt[i].pid, mo_Relaxed) && txn->env->gc.detent == safe64_read(&lck->rdt[i].txnid)) {
              oldest_reading = txn->env->gc.detent;
              break;
            }
          }
        }
      }
      info->txn_reader_lag = txn->txnid - oldest_reading;
    }
  }

  return MDBX_SUCCESS;
}
