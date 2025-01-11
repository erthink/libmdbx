/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024

#include "internals.h"

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

int txn_ro_end(MDBX_txn *txn, unsigned mode) {
  MDBX_env *const env = txn->env;
  tASSERT(txn, (txn->flags & txn_may_have_cursors) == 0);
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
