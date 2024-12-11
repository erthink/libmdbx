/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024

#include "internals.h"

/*------------------------------------------------------------------------------
 * Readers API */

__cold int mdbx_reader_list(const MDBX_env *env, MDBX_reader_list_func *func, void *ctx) {
  int rc = check_env(env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  if (unlikely(!func))
    return LOG_IFERR(MDBX_EINVAL);

  rc = MDBX_RESULT_TRUE;
  int serial = 0;
  lck_t *const lck = env->lck_mmap.lck;
  if (likely(lck)) {
    const size_t snap_nreaders = atomic_load32(&lck->rdt_length, mo_AcquireRelease);
    for (size_t i = 0; i < snap_nreaders; i++) {
      const reader_slot_t *r = lck->rdt + i;
    retry_reader:;
      const uint32_t pid = atomic_load32(&r->pid, mo_AcquireRelease);
      if (!pid)
        continue;
      txnid_t txnid = safe64_read(&r->txnid);
      const uint64_t tid = atomic_load64(&r->tid, mo_Relaxed);
      const pgno_t pages_used = atomic_load32(&r->snapshot_pages_used, mo_Relaxed);
      const uint64_t reader_pages_retired = atomic_load64(&r->snapshot_pages_retired, mo_Relaxed);
      if (unlikely(txnid != safe64_read(&r->txnid) || pid != atomic_load32(&r->pid, mo_AcquireRelease) ||
                   tid != atomic_load64(&r->tid, mo_Relaxed) ||
                   pages_used != atomic_load32(&r->snapshot_pages_used, mo_Relaxed) ||
                   reader_pages_retired != atomic_load64(&r->snapshot_pages_retired, mo_Relaxed)))
        goto retry_reader;

      eASSERT(env, txnid > 0);
      if (txnid >= SAFE64_INVALID_THRESHOLD)
        txnid = 0;

      size_t bytes_used = 0;
      size_t bytes_retained = 0;
      uint64_t lag = 0;
      if (txnid) {
        troika_t troika = meta_tap(env);
      retry_header:;
        const meta_ptr_t head = meta_recent(env, &troika);
        const uint64_t head_pages_retired = unaligned_peek_u64_volatile(4, head.ptr_v->pages_retired);
        if (unlikely(meta_should_retry(env, &troika) ||
                     head_pages_retired != unaligned_peek_u64_volatile(4, head.ptr_v->pages_retired)))
          goto retry_header;

        lag = (head.txnid - txnid) / xMDBX_TXNID_STEP;
        bytes_used = pgno2bytes(env, pages_used);
        bytes_retained = (head_pages_retired > reader_pages_retired)
                             ? pgno2bytes(env, (pgno_t)(head_pages_retired - reader_pages_retired))
                             : 0;
      }
      rc = func(ctx, ++serial, (unsigned)i, pid, (mdbx_tid_t)((intptr_t)tid), txnid, lag, bytes_used, bytes_retained);
      if (unlikely(rc != MDBX_SUCCESS))
        break;
    }
  }

  return LOG_IFERR(rc);
}

__cold int mdbx_reader_check(MDBX_env *env, int *dead) {
  if (dead)
    *dead = 0;
  return LOG_IFERR(mvcc_cleanup_dead(env, false, dead));
}

/*------------------------------------------------------------------------------
 * Locking API */

int mdbx_txn_lock(MDBX_env *env, bool dont_wait) {
  int rc = check_env(env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  if (unlikely(env->flags & MDBX_RDONLY))
    return LOG_IFERR(MDBX_EACCESS);
  if (unlikely(env->basal_txn->owner || (env->basal_txn->flags & MDBX_TXN_FINISHED) == 0))
    return LOG_IFERR(MDBX_BUSY);

  return LOG_IFERR(lck_txn_lock(env, dont_wait));
}

int mdbx_txn_unlock(MDBX_env *env) {
  int rc = check_env(env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  if (unlikely(env->flags & MDBX_RDONLY))
    return LOG_IFERR(MDBX_EACCESS);
  if (unlikely(env->basal_txn->owner != osal_thread_self()))
    return LOG_IFERR(MDBX_THREAD_MISMATCH);
  if (unlikely((env->basal_txn->flags & MDBX_TXN_FINISHED) == 0))
    return LOG_IFERR(MDBX_BUSY);

  lck_txn_unlock(env);
  return MDBX_SUCCESS;
}
