/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024

#include "internals.h"

bsr_t mvcc_bind_slot(MDBX_env *env) {
  eASSERT(env, env->lck_mmap.lck);
  eASSERT(env, env->lck->magic_and_version == MDBX_LOCK_MAGIC);
  eASSERT(env, env->lck->os_and_format == MDBX_LOCK_FORMAT);

  bsr_t result = {lck_rdt_lock(env), nullptr};
  if (unlikely(MDBX_IS_ERROR(result.err)))
    return result;
  if (unlikely(env->flags & ENV_FATAL_ERROR)) {
    lck_rdt_unlock(env);
    result.err = MDBX_PANIC;
    return result;
  }
  if (unlikely(!env->dxb_mmap.base)) {
    lck_rdt_unlock(env);
    result.err = MDBX_EPERM;
    return result;
  }

  if (unlikely(env->registered_reader_pid != env->pid)) {
    result.err = lck_rpid_set(env);
    if (unlikely(result.err != MDBX_SUCCESS)) {
      lck_rdt_unlock(env);
      return result;
    }
    env->registered_reader_pid = env->pid;
  }

  result.err = MDBX_SUCCESS;
  size_t slot, nreaders;
  while (1) {
    nreaders = env->lck->rdt_length.weak;
    for (slot = 0; slot < nreaders; slot++)
      if (!atomic_load32(&env->lck->rdt[slot].pid, mo_AcquireRelease))
        break;

    if (likely(slot < env->max_readers))
      break;

    result.err = mvcc_cleanup_dead(env, true, nullptr);
    if (result.err != MDBX_RESULT_TRUE) {
      lck_rdt_unlock(env);
      result.err =
          (result.err == MDBX_SUCCESS) ? MDBX_READERS_FULL : result.err;
      return result;
    }
  }

  result.rslot = &env->lck->rdt[slot];
  /* Claim the reader slot, carefully since other code
   * uses the reader table un-mutexed: First reset the
   * slot, next publish it in lck->rdt_length.  After
   * that, it is safe for mdbx_env_close() to touch it.
   * When it will be closed, we can finally claim it. */
  atomic_store32(&result.rslot->pid, 0, mo_AcquireRelease);
  safe64_reset(&result.rslot->txnid, true);
  if (slot == nreaders)
    env->lck->rdt_length.weak = (uint32_t)++nreaders;
  result.rslot->tid.weak =
      (env->flags & MDBX_NOSTICKYTHREADS) ? 0 : osal_thread_self();
  atomic_store32(&result.rslot->pid, env->pid, mo_AcquireRelease);
  lck_rdt_unlock(env);

  if (likely(env->flags & ENV_TXKEY)) {
    eASSERT(env, env->registered_reader_pid == env->pid);
    thread_rthc_set(env->me_txkey, result.rslot);
  }
  return result;
}

__hot txnid_t mvcc_shapshot_oldest(MDBX_env *const env, const txnid_t steady) {
  const uint32_t nothing_changed = MDBX_STRING_TETRAD("None");
  eASSERT(env, steady <= env->basal_txn->txnid);

  lck_t *const lck = env->lck_mmap.lck;
  if (unlikely(lck == nullptr /* exclusive without-lck mode */)) {
    eASSERT(env, env->lck == lckless_stub(env));
    env->lck->rdt_refresh_flag.weak = nothing_changed;
    return env->lck->cached_oldest.weak = steady;
  }

  const txnid_t prev_oldest =
      atomic_load64(&lck->cached_oldest, mo_AcquireRelease);
  eASSERT(env, steady >= prev_oldest);

  txnid_t new_oldest = prev_oldest;
  while (nothing_changed !=
         atomic_load32(&lck->rdt_refresh_flag, mo_AcquireRelease)) {
    lck->rdt_refresh_flag.weak = nothing_changed;
    jitter4testing(false);
    const size_t snap_nreaders =
        atomic_load32(&lck->rdt_length, mo_AcquireRelease);
    new_oldest = steady;

    for (size_t i = 0; i < snap_nreaders; ++i) {
      const uint32_t pid = atomic_load32(&lck->rdt[i].pid, mo_AcquireRelease);
      if (!pid)
        continue;
      jitter4testing(true);

      const txnid_t rtxn = safe64_read(&lck->rdt[i].txnid);
      if (unlikely(rtxn < prev_oldest)) {
        if (unlikely(nothing_changed == atomic_load32(&lck->rdt_refresh_flag,
                                                      mo_AcquireRelease)) &&
            safe64_reset_compare(&lck->rdt[i].txnid, rtxn)) {
          NOTICE("kick stuck reader[%zu of %zu].pid_%u %" PRIaTXN
                 " < prev-oldest %" PRIaTXN ", steady-txn %" PRIaTXN,
                 i, snap_nreaders, pid, rtxn, prev_oldest, steady);
        }
        continue;
      }

      if (rtxn < new_oldest) {
        new_oldest = rtxn;
        if (!MDBX_DEBUG && !MDBX_FORCE_ASSERTIONS && new_oldest == prev_oldest)
          break;
      }
    }
  }

  if (new_oldest != prev_oldest) {
    VERBOSE("update oldest %" PRIaTXN " -> %" PRIaTXN, prev_oldest, new_oldest);
    eASSERT(env, new_oldest >= lck->cached_oldest.weak);
    atomic_store64(&lck->cached_oldest, new_oldest, mo_Relaxed);
  }
  return new_oldest;
}

pgno_t mvcc_snapshot_largest(const MDBX_env *env, pgno_t last_used_page) {
  lck_t *const lck = env->lck_mmap.lck;
  if (likely(lck != nullptr /* check for exclusive without-lck mode */)) {
  retry:;
    const size_t snap_nreaders =
        atomic_load32(&lck->rdt_length, mo_AcquireRelease);
    for (size_t i = 0; i < snap_nreaders; ++i) {
      if (atomic_load32(&lck->rdt[i].pid, mo_AcquireRelease)) {
        /* jitter4testing(true); */
        const pgno_t snap_pages =
            atomic_load32(&lck->rdt[i].snapshot_pages_used, mo_Relaxed);
        const txnid_t snap_txnid = safe64_read(&lck->rdt[i].txnid);
        if (unlikely(snap_pages !=
                         atomic_load32(&lck->rdt[i].snapshot_pages_used,
                                       mo_AcquireRelease) ||
                     snap_txnid != safe64_read(&lck->rdt[i].txnid)))
          goto retry;
        if (last_used_page < snap_pages && snap_txnid <= env->basal_txn->txnid)
          last_used_page = snap_pages;
      }
    }
  }

  return last_used_page;
}

/* Find largest mvcc-snapshot still referenced by this process. */
pgno_t mvcc_largest_this(MDBX_env *env, pgno_t largest) {
  lck_t *const lck = env->lck_mmap.lck;
  if (likely(lck != nullptr /* exclusive mode */)) {
    const size_t snap_nreaders =
        atomic_load32(&lck->rdt_length, mo_AcquireRelease);
    for (size_t i = 0; i < snap_nreaders; ++i) {
    retry:
      if (atomic_load32(&lck->rdt[i].pid, mo_AcquireRelease) == env->pid) {
        /* jitter4testing(true); */
        const pgno_t snap_pages =
            atomic_load32(&lck->rdt[i].snapshot_pages_used, mo_Relaxed);
        const txnid_t snap_txnid = safe64_read(&lck->rdt[i].txnid);
        if (unlikely(snap_pages !=
                         atomic_load32(&lck->rdt[i].snapshot_pages_used,
                                       mo_AcquireRelease) ||
                     snap_txnid != safe64_read(&lck->rdt[i].txnid)))
          goto retry;
        if (largest < snap_pages &&
            atomic_load64(&lck->cached_oldest, mo_AcquireRelease) <=
                /* ignore pending updates */ snap_txnid &&
            snap_txnid <= MAX_TXNID)
          largest = snap_pages;
      }
    }
  }
  return largest;
}

static bool pid_insert(uint32_t *list, uint32_t pid) {
  /* binary search of pid in list */
  size_t base = 0;
  size_t cursor = 1;
  int32_t val = 0;
  size_t n = /* length */ list[0];

  while (n > 0) {
    size_t pivot = n >> 1;
    cursor = base + pivot + 1;
    val = pid - list[cursor];

    if (val < 0) {
      n = pivot;
    } else if (val > 0) {
      base = cursor;
      n -= pivot + 1;
    } else {
      /* found, so it's a duplicate */
      return false;
    }
  }

  if (val > 0)
    ++cursor;

  list[0]++;
  for (n = list[0]; n > cursor; n--)
    list[n] = list[n - 1];
  list[n] = pid;
  return true;
}

__cold MDBX_INTERNAL int mvcc_cleanup_dead(MDBX_env *env, int rdt_locked,
                                           int *dead) {
  int rc = check_env(env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  eASSERT(env, rdt_locked >= 0);
  lck_t *const lck = env->lck_mmap.lck;
  if (unlikely(lck == nullptr)) {
    /* exclusive mode */
    if (dead)
      *dead = 0;
    return MDBX_SUCCESS;
  }

  const size_t snap_nreaders =
      atomic_load32(&lck->rdt_length, mo_AcquireRelease);
  uint32_t pidsbuf_onstask[142];
  uint32_t *const pids =
      (snap_nreaders < ARRAY_LENGTH(pidsbuf_onstask))
          ? pidsbuf_onstask
          : osal_malloc((snap_nreaders + 1) * sizeof(uint32_t));
  if (unlikely(!pids))
    return MDBX_ENOMEM;

  pids[0] = 0;
  int count = 0;
  for (size_t i = 0; i < snap_nreaders; i++) {
    const uint32_t pid = atomic_load32(&lck->rdt[i].pid, mo_AcquireRelease);
    if (pid == 0)
      continue /* skip empty */;
    if (pid == env->pid)
      continue /* skip self */;
    if (!pid_insert(pids, pid))
      continue /* such pid already processed */;

    int err = lck_rpid_check(env, pid);
    if (err == MDBX_RESULT_TRUE)
      continue /* reader is live */;

    if (err != MDBX_SUCCESS) {
      rc = err;
      break /* lck_rpid_check() failed */;
    }

    /* stale reader found */
    if (!rdt_locked) {
      err = lck_rdt_lock(env);
      if (MDBX_IS_ERROR(err)) {
        rc = err;
        break;
      }

      rdt_locked = -1;
      if (err == MDBX_RESULT_TRUE) {
        /* mutex recovered, the mdbx_ipclock_failed() checked all readers */
        rc = MDBX_RESULT_TRUE;
        break;
      }

      /* a other process may have clean and reused slot, recheck */
      if (lck->rdt[i].pid.weak != pid)
        continue;

      err = lck_rpid_check(env, pid);
      if (MDBX_IS_ERROR(err)) {
        rc = err;
        break;
      }

      if (err != MDBX_SUCCESS)
        continue /* the race with other process, slot reused */;
    }

    /* clean it */
    for (size_t ii = i; ii < snap_nreaders; ii++) {
      if (lck->rdt[ii].pid.weak == pid) {
        DEBUG("clear stale reader pid %" PRIuPTR " txn %" PRIaTXN, (size_t)pid,
              lck->rdt[ii].txnid.weak);
        atomic_store32(&lck->rdt[ii].pid, 0, mo_Relaxed);
        atomic_store32(&lck->rdt_refresh_flag, true, mo_AcquireRelease);
        count++;
      }
    }
  }

  if (likely(!MDBX_IS_ERROR(rc)))
    atomic_store64(&lck->readers_check_timestamp, osal_monotime(), mo_Relaxed);

  if (rdt_locked < 0)
    lck_rdt_unlock(env);

  if (pids != pidsbuf_onstask)
    osal_free(pids);

  if (dead)
    *dead = count;
  return rc;
}

int txn_park(MDBX_txn *txn, bool autounpark) {
  reader_slot_t *const rslot = txn->to.reader;
  tASSERT(txn, (txn->flags & (MDBX_TXN_FINISHED | MDBX_TXN_RDONLY |
                              MDBX_TXN_PARKED)) == MDBX_TXN_RDONLY);
  tASSERT(txn, txn->to.reader->tid.weak < MDBX_TID_TXN_OUSTED);
  if (unlikely((txn->flags & (MDBX_TXN_FINISHED | MDBX_TXN_RDONLY |
                              MDBX_TXN_PARKED)) != MDBX_TXN_RDONLY))
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
  txn->flags +=
      autounpark ? MDBX_TXN_PARKED | MDBX_TXN_AUTOUNPARK : MDBX_TXN_PARKED;
  return MDBX_SUCCESS;
}

int txn_unpark(MDBX_txn *txn) {
  if (unlikely((txn->flags & (MDBX_TXN_FINISHED | MDBX_TXN_HAS_CHILD |
                              MDBX_TXN_RDONLY | MDBX_TXN_PARKED)) !=
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
    if (unlikely(tid == MDBX_TID_TXN_OUSTED ||
                 txnid >= SAFE64_INVALID_THRESHOLD))
      break;
    if (unlikely(tid != MDBX_TID_TXN_PARKED || txnid != txn->txnid)) {
      ERROR("unexpected thread-id 0x%" PRIx64 "%s0x%" PRIx64
            " and/or txn-id %" PRIaTXN "%s%" PRIaTXN,
            tid, " != must ", MDBX_TID_TXN_OUSTED, txnid, " != must ",
            txn->txnid);
      break;
    }
    if (unlikely((txn->flags & MDBX_TXN_ERROR)))
      break;

#if MDBX_64BIT_CAS
    if (unlikely(!atomic_cas64(&rslot->tid, MDBX_TID_TXN_PARKED, txn->owner)))
      continue;
#else
    atomic_store32(&rslot->tid.high, (uint32_t)((uint64_t)txn->owner >> 32),
                   mo_Relaxed);
    if (unlikely(!atomic_cas32(&rslot->tid.low, (uint32_t)MDBX_TID_TXN_PARKED,
                               (uint32_t)txn->owner))) {
      atomic_store32(&rslot->tid.high, (uint32_t)(MDBX_TID_TXN_PARKED >> 32),
                     mo_AcquireRelease);
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

__cold txnid_t mvcc_kick_laggards(MDBX_env *env, const txnid_t straggler) {
  DEBUG("DB size maxed out by reading #%" PRIaTXN, straggler);
  osal_memory_fence(mo_AcquireRelease, false);
  MDBX_hsr_func *const callback = env->hsr_callback;
  txnid_t oldest = 0;
  bool notify_eof_of_loop = false;
  int retry = 0;
  do {
    const txnid_t steady =
        env->txn->tw.troika.txnid[env->txn->tw.troika.prefer_steady];
    env->lck->rdt_refresh_flag.weak = /* force refresh */ true;
    oldest = mvcc_shapshot_oldest(env, steady);
    eASSERT(env, oldest < env->basal_txn->txnid);
    eASSERT(env, oldest >= straggler);
    eASSERT(env, oldest >= env->lck->cached_oldest.weak);

    lck_t *const lck = env->lck_mmap.lck;
    if (oldest == steady || oldest > straggler || /* without-LCK mode */ !lck)
      break;

    if (MDBX_IS_ERROR(mvcc_cleanup_dead(env, false, nullptr)))
      break;

    reader_slot_t *stucked = nullptr;
    uint64_t hold_retired = 0;
    for (size_t i = 0; i < lck->rdt_length.weak; ++i) {
      uint32_t pid;
      reader_slot_t *const rslot = &lck->rdt[i];
      txnid_t rtxn = safe64_read(&rslot->txnid);
    retry:
      if (rtxn == straggler &&
          (pid = atomic_load32(&rslot->pid, mo_AcquireRelease)) != 0) {
        const uint64_t tid = safe64_read(&rslot->tid);
        if (tid == MDBX_TID_TXN_PARKED) {
          /* Читающая транзакция была помечена владельцем как "припаркованная",
           * т.е. подлежащая асинхронному прерыванию, либо восстановлению
           * по активности читателя.
           *
           * Если первый CAS(slot->tid) будет успешным, то
           * safe64_reset_compare() безопасно очистит txnid, либо откажется
           * из-за того что читатель сбросил и/или перезапустил транзакцию.
           * При этом читатеть может не заметить вытестения, если приступит
           * к завершению транзакции. Все эти исходы нас устраивют.
           *
           * Если первый CAS(slot->tid) будет НЕ успешным, то значит читатеть
           * восстановил транзакцию, либо завершил её, либо даже освободил слот.
           */
          bool ousted =
#if MDBX_64BIT_CAS
              atomic_cas64(&rslot->tid, MDBX_TID_TXN_PARKED,
                           MDBX_TID_TXN_OUSTED);
#else
              atomic_cas32(&rslot->tid.low, (uint32_t)MDBX_TID_TXN_PARKED,
                           (uint32_t)MDBX_TID_TXN_OUSTED);
#endif
          if (likely(ousted)) {
            ousted = safe64_reset_compare(&rslot->txnid, rtxn);
            NOTICE("ousted-%s parked read-txn %" PRIaTXN
                   ", pid %u, tid 0x%" PRIx64,
                   ousted ? "complete" : "half", rtxn, pid, tid);
            eASSERT(env, ousted || safe64_read(&rslot->txnid) > straggler);
            continue;
          }
          rtxn = safe64_read(&rslot->txnid);
          goto retry;
        }
        hold_retired =
            atomic_load64(&lck->rdt[i].snapshot_pages_retired, mo_Relaxed);
        stucked = rslot;
      }
    }

    if (!callback || !stucked)
      break;

    uint32_t pid = atomic_load32(&stucked->pid, mo_AcquireRelease);
    uint64_t tid = safe64_read(&stucked->tid);
    if (safe64_read(&stucked->txnid) != straggler || !pid)
      continue;

    const meta_ptr_t head = meta_recent(env, &env->txn->tw.troika);
    const txnid_t gap = (head.txnid - straggler) / xMDBX_TXNID_STEP;
    const uint64_t head_retired =
        unaligned_peek_u64(4, head.ptr_c->pages_retired);
    const size_t space =
        (head_retired > hold_retired)
            ? pgno2bytes(env, (pgno_t)(head_retired - hold_retired))
            : 0;
    int rc =
        callback(env, env->txn, pid, (mdbx_tid_t)((intptr_t)tid), straggler,
                 (gap < UINT_MAX) ? (unsigned)gap : UINT_MAX, space, retry);
    if (rc < 0)
      /* hsr returned error and/or agree MDBX_MAP_FULL error */
      break;

    if (rc > 0) {
      if (rc == 1) {
        /* hsr reported transaction (will be) aborted asynchronous */
        safe64_reset_compare(&stucked->txnid, straggler);
      } else {
        /* hsr reported reader process was killed and slot should be cleared */
        safe64_reset(&stucked->txnid, true);
        atomic_store64(&stucked->tid, 0, mo_Relaxed);
        atomic_store32(&stucked->pid, 0, mo_AcquireRelease);
      }
    } else if (!notify_eof_of_loop) {
#if MDBX_ENABLE_PROFGC
      env->lck->pgops.gc_prof.kicks += 1;
#endif /* MDBX_ENABLE_PROFGC */
      notify_eof_of_loop = true;
    }

  } while (++retry < INT_MAX);

  if (notify_eof_of_loop) {
    /* notify end of hsr-loop */
    const txnid_t turn = oldest - straggler;
    if (turn)
      NOTICE("hsr-kick: done turn %" PRIaTXN " -> %" PRIaTXN " +%" PRIaTXN,
             straggler, oldest, turn);
    callback(env, env->txn, 0, 0, straggler,
             (turn < UINT_MAX) ? (unsigned)turn : UINT_MAX, 0, -retry);
  }
  return oldest;
}

/*----------------------------------------------------------------------------*/

__cold int mdbx_thread_register(const MDBX_env *env) {
  int rc = check_env(env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!env->lck_mmap.lck))
    return (env->flags & MDBX_EXCLUSIVE) ? MDBX_EINVAL : MDBX_EPERM;

  if (unlikely((env->flags & ENV_TXKEY) == 0)) {
    eASSERT(env, env->flags & MDBX_NOSTICKYTHREADS);
    return MDBX_EINVAL /* MDBX_NOSTICKYTHREADS mode */;
  }

  eASSERT(env, (env->flags & (MDBX_NOSTICKYTHREADS | ENV_TXKEY)) == ENV_TXKEY);
  reader_slot_t *r = thread_rthc_get(env->me_txkey);
  if (unlikely(r != nullptr)) {
    eASSERT(env, r->pid.weak == env->pid);
    eASSERT(env, r->tid.weak == osal_thread_self());
    if (unlikely(r->pid.weak != env->pid))
      return MDBX_BAD_RSLOT;
    return MDBX_RESULT_TRUE /* already registered */;
  }

  return mvcc_bind_slot((MDBX_env *)env).err;
}

__cold int mdbx_thread_unregister(const MDBX_env *env) {
  int rc = check_env(env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(!env->lck_mmap.lck))
    return MDBX_RESULT_TRUE;

  if (unlikely((env->flags & ENV_TXKEY) == 0)) {
    eASSERT(env, env->flags & MDBX_NOSTICKYTHREADS);
    return MDBX_RESULT_TRUE /* MDBX_NOSTICKYTHREADS mode */;
  }

  eASSERT(env, (env->flags & (MDBX_NOSTICKYTHREADS | ENV_TXKEY)) == ENV_TXKEY);
  reader_slot_t *r = thread_rthc_get(env->me_txkey);
  if (unlikely(r == nullptr))
    return MDBX_RESULT_TRUE /* not registered */;

  eASSERT(env, r->pid.weak == env->pid);
  eASSERT(env, r->tid.weak == osal_thread_self());
  if (unlikely(r->pid.weak != env->pid || r->tid.weak != osal_thread_self()))
    return MDBX_BAD_RSLOT;

  eASSERT(env, r->txnid.weak >= SAFE64_INVALID_THRESHOLD);
  if (unlikely(r->txnid.weak < SAFE64_INVALID_THRESHOLD))
    return MDBX_BUSY /* transaction is still active */;

  atomic_store32(&r->pid, 0, mo_Relaxed);
  atomic_store32(&env->lck->rdt_refresh_flag, true, mo_AcquireRelease);
  thread_rthc_set(env->me_txkey, nullptr);
  return MDBX_SUCCESS;
}
