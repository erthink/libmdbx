/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025

#include "internals.h"

static inline int txn_ro_rslot(MDBX_txn *txn) {
  reader_slot_t *slot = txn->ro.slot;
  STATIC_ASSERT(sizeof(uintptr_t) <= sizeof(slot->tid));
  if (likely(slot)) {
    if (likely(slot->pid.weak == txn->env->pid && slot->txnid.weak >= SAFE64_INVALID_THRESHOLD)) {
      tASSERT(txn, slot->pid.weak == osal_getpid());
      tASSERT(txn, slot->tid.weak == ((txn->env->flags & MDBX_NOSTICKYTHREADS) ? 0 : osal_thread_self()));
      return MDBX_SUCCESS;
    }
    return MDBX_BAD_RSLOT;
  }

  if (unlikely(!txn->env->lck_mmap.lck))
    return MDBX_SUCCESS;

  MDBX_env *const env = txn->env;
  if (env->flags & ENV_TXKEY) {
    eASSERT(env, !(env->flags & MDBX_NOSTICKYTHREADS));
    slot = thread_rthc_get(env->me_txkey);
    if (likely(slot)) {
      if (likely(slot->pid.weak == env->pid && slot->txnid.weak >= SAFE64_INVALID_THRESHOLD)) {
        tASSERT(txn, slot->pid.weak == osal_getpid());
        tASSERT(txn, slot->tid.weak == ((env->flags & MDBX_NOSTICKYTHREADS) ? 0 : osal_thread_self()));
        txn->ro.slot = slot;
        return MDBX_SUCCESS;
      }
      if (unlikely(slot->pid.weak) || !(globals.runtime_flags & MDBX_DBG_LEGACY_MULTIOPEN))
        return MDBX_BAD_RSLOT;
      thread_rthc_set(env->me_txkey, nullptr);
    }
  } else {
    eASSERT(env, (env->flags & MDBX_NOSTICKYTHREADS));
  }

  bsr_t brs = mvcc_bind_slot(env);
  if (likely(brs.err == MDBX_SUCCESS)) {
    tASSERT(txn, brs.slot->pid.weak == osal_getpid());
    tASSERT(txn, brs.slot->tid.weak == ((env->flags & MDBX_NOSTICKYTHREADS) ? 0 : osal_thread_self()));
  }
  txn->ro.slot = brs.slot;
  return brs.err;
}

static inline int txn_ro_seize(MDBX_txn *txn) {
  /* Seek & fetch the last meta */
  troika_t troika = meta_tap(txn->env);
  uint64_t timestamp = 0;
  size_t loop = 0;
  do {
    MDBX_env *const env = txn->env;
    const meta_ptr_t head = likely(env->stuck_meta < 0) ? /* regular */ meta_recent(env, &troika)
                                                        : /* recovery mode */ meta_ptr(env, env->stuck_meta);
    reader_slot_t *const r = txn->ro.slot;
    if (likely(r != nullptr)) {
      safe64_reset(&r->txnid, true);
      atomic_store32(&r->snapshot_pages_used, head.ptr_v->geometry.first_unallocated, mo_Relaxed);
      atomic_store64(&r->snapshot_pages_retired, unaligned_peek_u64_volatile(4, head.ptr_v->pages_retired), mo_Relaxed);
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
      timestamp = 0;
      continue;
    }

    /* Snap the state from current meta-head */
    int err = coherency_fetch_head(txn, head, &timestamp);
    jitter4testing(false);
    if (unlikely(err != MDBX_SUCCESS)) {
      if (err != MDBX_RESULT_TRUE)
        return err;
      continue;
    }

    const uint64_t snap_oldest = atomic_load64(&env->lck->cached_oldest, mo_AcquireRelease);
    if (unlikely(txn->txnid < snap_oldest)) {
      if (env->stuck_meta >= 0) {
        ERROR("target meta-page %i is referenced to an obsolete MVCC-snapshot "
              "%" PRIaTXN " < cached-oldest %" PRIaTXN,
              env->stuck_meta, txn->txnid, snap_oldest);
        return MDBX_MVCC_RETARDED;
      }
      continue;
    }

    if (!r || likely(txn->txnid == atomic_load64(&r->txnid, mo_Relaxed)))
      return MDBX_SUCCESS;

  } while (likely(++loop < 42));

  ERROR("bailout waiting for valid snapshot (%s)", "meta-pages are too volatile");
  return MDBX_PROBLEM;
}

int txn_ro_start(MDBX_txn *txn, unsigned flags) {
  MDBX_env *const env = txn->env;
  eASSERT(env, flags & MDBX_TXN_RDONLY);
  eASSERT(env, (flags & ~(txn_ro_begin_flags | MDBX_WRITEMAP | MDBX_NOSTICKYTHREADS)) == 0);
  txn->flags = flags;

  int err = txn_ro_rslot(txn);
  if (unlikely(err != MDBX_SUCCESS))
    goto bailout;

  STATIC_ASSERT(MDBX_TXN_RDONLY_PREPARE > MDBX_TXN_RDONLY);
  reader_slot_t *r = txn->ro.slot;
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
      (globals.runtime_flags & MDBX_DBG_LEGACY_OVERLAP) == 0) {
    err = MDBX_TXN_OVERLAPPING;
    goto bailout;
  }

  err = txn_ro_seize(txn);
  if (unlikely(err != MDBX_SUCCESS))
    goto bailout;

  if (unlikely(txn->txnid < MIN_TXNID || txn->txnid > MAX_TXNID)) {
    ERROR("%s", "environment corrupted by died writer, must shutdown!");
    err = MDBX_CORRUPTED;
    goto bailout;
  }

  return MDBX_SUCCESS;

bailout:
  tASSERT(txn, err != MDBX_SUCCESS);
  txn->txnid = INVALID_TXNID;
  if (likely(txn->ro.slot))
    safe64_reset(&txn->ro.slot->txnid, true);
  return err;
}

int txn_ro_end(MDBX_txn *txn, unsigned mode) {
  MDBX_env *const env = txn->env;
  tASSERT(txn, (txn->flags & txn_may_have_cursors) == 0);
  txn->n_dbi = 0; /* prevent further DBI activity */
  if (txn->ro.slot) {
    reader_slot_t *slot = txn->ro.slot;
    if (unlikely(!env->lck))
      txn->ro.slot = nullptr;
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
        txn->ro.slot = nullptr;
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

int txn_ro_park(MDBX_txn *txn, bool autounpark) {
  reader_slot_t *const rslot = txn->ro.slot;
  tASSERT(txn, (txn->flags & (MDBX_TXN_FINISHED | MDBX_TXN_RDONLY | MDBX_TXN_PARKED)) == MDBX_TXN_RDONLY);
  tASSERT(txn, txn->ro.slot->tid.weak < MDBX_TID_TXN_OUSTED);
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

int txn_ro_unpark(MDBX_txn *txn) {
  if (unlikely((txn->flags & (MDBX_TXN_FINISHED | MDBX_TXN_HAS_CHILD | MDBX_TXN_RDONLY | MDBX_TXN_PARKED)) !=
               (MDBX_TXN_RDONLY | MDBX_TXN_PARKED)))
    return MDBX_BAD_TXN;

  for (reader_slot_t *const rslot = txn->ro.slot; rslot; atomic_yield()) {
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
