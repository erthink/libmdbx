/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024

#include "internals.h"

/* check against https://libmdbx.dqdkfa.ru/dead-github/issues/269 */
static bool coherency_check(const MDBX_env *env, const txnid_t txnid, const volatile tree_t *trees,
                            const volatile meta_t *meta, bool report) {
  const txnid_t freedb_mod_txnid = trees[FREE_DBI].mod_txnid;
  const txnid_t maindb_mod_txnid = trees[MAIN_DBI].mod_txnid;
  const pgno_t last_pgno = meta->geometry.now;

  const pgno_t freedb_root_pgno = trees[FREE_DBI].root;
  const page_t *freedb_root =
      (env->dxb_mmap.base && freedb_root_pgno < last_pgno) ? pgno2page(env, freedb_root_pgno) : nullptr;

  const pgno_t maindb_root_pgno = trees[MAIN_DBI].root;
  const page_t *maindb_root =
      (env->dxb_mmap.base && maindb_root_pgno < last_pgno) ? pgno2page(env, maindb_root_pgno) : nullptr;
  const uint64_t magic_and_version = unaligned_peek_u64_volatile(4, &meta->magic_and_version);

  bool ok = true;
  if (freedb_root_pgno != P_INVALID && unlikely(freedb_root_pgno >= last_pgno)) {
    if (report)
      WARNING("catch invalid %s-db root %" PRIaPGNO " for meta_txnid %" PRIaTXN " %s", "free", freedb_root_pgno, txnid,
              (env->stuck_meta < 0) ? "(workaround for incoherent flaw of unified page/buffer cache)"
                                    : "(wagering meta)");
    ok = false;
  }
  if (maindb_root_pgno != P_INVALID && unlikely(maindb_root_pgno >= last_pgno)) {
    if (report)
      WARNING("catch invalid %s-db root %" PRIaPGNO " for meta_txnid %" PRIaTXN " %s", "main", maindb_root_pgno, txnid,
              (env->stuck_meta < 0) ? "(workaround for incoherent flaw of unified page/buffer cache)"
                                    : "(wagering meta)");
    ok = false;
  }
  if (unlikely(txnid < freedb_mod_txnid ||
               (!freedb_mod_txnid && freedb_root && likely(magic_and_version == MDBX_DATA_MAGIC)))) {
    if (report)
      WARNING(
          "catch invalid %s-db.mod_txnid %" PRIaTXN " for meta_txnid %" PRIaTXN " %s", "free", freedb_mod_txnid, txnid,
          (env->stuck_meta < 0) ? "(workaround for incoherent flaw of unified page/buffer cache)" : "(wagering meta)");
    ok = false;
  }
  if (unlikely(txnid < maindb_mod_txnid ||
               (!maindb_mod_txnid && maindb_root && likely(magic_and_version == MDBX_DATA_MAGIC)))) {
    if (report)
      WARNING(
          "catch invalid %s-db.mod_txnid %" PRIaTXN " for meta_txnid %" PRIaTXN " %s", "main", maindb_mod_txnid, txnid,
          (env->stuck_meta < 0) ? "(workaround for incoherent flaw of unified page/buffer cache)" : "(wagering meta)");
    ok = false;
  }

  /* Проверяем отметки внутри корневых страниц только если сами страницы
   * в пределах текущего отображения. Иначе возможны SIGSEGV до переноса
   * вызова coherency_check_head() после dxb_resize() внутри txn_renew(). */
  if (likely(freedb_root && freedb_mod_txnid &&
             (size_t)ptr_dist(env->dxb_mmap.base, freedb_root) < env->dxb_mmap.limit)) {
    VALGRIND_MAKE_MEM_DEFINED(freedb_root, sizeof(freedb_root->txnid));
    MDBX_ASAN_UNPOISON_MEMORY_REGION(freedb_root, sizeof(freedb_root->txnid));
    const txnid_t root_txnid = freedb_root->txnid;
    if (unlikely(root_txnid != freedb_mod_txnid)) {
      if (report)
        WARNING("catch invalid root_page %" PRIaPGNO " mod_txnid %" PRIaTXN " for %s-db.mod_txnid %" PRIaTXN " %s",
                freedb_root_pgno, root_txnid, "free", freedb_mod_txnid,
                (env->stuck_meta < 0) ? "(workaround for incoherent flaw of "
                                        "unified page/buffer cache)"
                                      : "(wagering meta)");
      ok = false;
    }
  }
  if (likely(maindb_root && maindb_mod_txnid &&
             (size_t)ptr_dist(env->dxb_mmap.base, maindb_root) < env->dxb_mmap.limit)) {
    VALGRIND_MAKE_MEM_DEFINED(maindb_root, sizeof(maindb_root->txnid));
    MDBX_ASAN_UNPOISON_MEMORY_REGION(maindb_root, sizeof(maindb_root->txnid));
    const txnid_t root_txnid = maindb_root->txnid;
    if (unlikely(root_txnid != maindb_mod_txnid)) {
      if (report)
        WARNING("catch invalid root_page %" PRIaPGNO " mod_txnid %" PRIaTXN " for %s-db.mod_txnid %" PRIaTXN " %s",
                maindb_root_pgno, root_txnid, "main", maindb_mod_txnid,
                (env->stuck_meta < 0) ? "(workaround for incoherent flaw of "
                                        "unified page/buffer cache)"
                                      : "(wagering meta)");
      ok = false;
    }
  }
  if (unlikely(!ok) && report)
    env->lck->pgops.incoherence.weak =
        (env->lck->pgops.incoherence.weak >= INT32_MAX) ? INT32_MAX : env->lck->pgops.incoherence.weak + 1;
  return ok;
}

__cold int coherency_timeout(uint64_t *timestamp, intptr_t pgno, const MDBX_env *env) {
  if (likely(timestamp && *timestamp == 0))
    *timestamp = osal_monotime();
  else if (unlikely(!timestamp || osal_monotime() - *timestamp > osal_16dot16_to_monotime(65536 / 10))) {
    if (pgno >= 0 && pgno != env->stuck_meta)
      ERROR("bailout waiting for %" PRIuSIZE " page arrival %s", pgno,
            "(workaround for incoherent flaw of unified page/buffer cache)");
    else if (env->stuck_meta < 0)
      ERROR("bailout waiting for valid snapshot (%s)", "workaround for incoherent flaw of unified page/buffer cache");
    return MDBX_PROBLEM;
  }

  osal_memory_fence(mo_AcquireRelease, true);
#if defined(_WIN32) || defined(_WIN64)
  SwitchToThread();
#elif defined(__linux__) || defined(__gnu_linux__) || defined(_UNIX03_SOURCE)
  sched_yield();
#elif (defined(_GNU_SOURCE) && __GLIBC_PREREQ(2, 1)) || defined(_OPEN_THREADS)
  pthread_yield();
#else
  usleep(42);
#endif
  return MDBX_RESULT_TRUE;
}

/* check with timeout as the workaround
 * for https://libmdbx.dqdkfa.ru/dead-github/issues/269 */
__hot int coherency_fetch_head(MDBX_txn *txn, const meta_ptr_t head, uint64_t *timestamp) {
  /* Copy the DB info and flags */
  txn->txnid = head.txnid;
  txn->geo = head.ptr_c->geometry;
  memcpy(txn->dbs, &head.ptr_c->trees, sizeof(head.ptr_c->trees));
  STATIC_ASSERT(sizeof(head.ptr_c->trees) == CORE_DBS * sizeof(tree_t));
  VALGRIND_MAKE_MEM_UNDEFINED(txn->dbs + CORE_DBS, txn->env->max_dbi - CORE_DBS);
  txn->canary = head.ptr_c->canary;

  if (unlikely(!coherency_check(txn->env, head.txnid, txn->dbs, head.ptr_v, *timestamp == 0) ||
               txn->txnid != meta_txnid(head.ptr_v)))
    return coherency_timeout(timestamp, -1, txn->env);

  if (unlikely(txn->dbs[FREE_DBI].flags != MDBX_INTEGERKEY)) {
    if ((txn->dbs[FREE_DBI].flags & DB_PERSISTENT_FLAGS) != MDBX_INTEGERKEY ||
        unaligned_peek_u64(4, &head.ptr_c->magic_and_version) == MDBX_DATA_MAGIC) {
      ERROR("unexpected/invalid db-flags 0x%x for %s", txn->dbs[FREE_DBI].flags, "GC/FreeDB");
      return MDBX_INCOMPATIBLE;
    }
    txn->dbs[FREE_DBI].flags &= DB_PERSISTENT_FLAGS;
  }
  tASSERT(txn, txn->dbs[FREE_DBI].flags == MDBX_INTEGERKEY);
  tASSERT(txn, check_table_flags(txn->dbs[MAIN_DBI].flags));
  return MDBX_SUCCESS;
}

int coherency_check_written(const MDBX_env *env, const txnid_t txnid, const volatile meta_t *meta, const intptr_t pgno,
                            uint64_t *timestamp) {
  const bool report = !(timestamp && *timestamp);
  const txnid_t head_txnid = meta_txnid(meta);
  if (likely(head_txnid >= MIN_TXNID && head_txnid >= txnid)) {
    if (likely(coherency_check(env, head_txnid, &meta->trees.gc, meta, report))) {
      eASSERT(env, meta->trees.gc.flags == MDBX_INTEGERKEY);
      eASSERT(env, check_table_flags(meta->trees.main.flags));
      return MDBX_SUCCESS;
    }
  } else if (report) {
    env->lck->pgops.incoherence.weak =
        (env->lck->pgops.incoherence.weak >= INT32_MAX) ? INT32_MAX : env->lck->pgops.incoherence.weak + 1;
    WARNING("catch %s txnid %" PRIaTXN " for meta_%" PRIaPGNO " %s",
            (head_txnid < MIN_TXNID) ? "invalid" : "unexpected", head_txnid,
            bytes2pgno(env, ptr_dist(meta, env->dxb_mmap.base)),
            "(workaround for incoherent flaw of unified page/buffer cache)");
  }
  return coherency_timeout(timestamp, pgno, env);
}

bool coherency_check_meta(const MDBX_env *env, const volatile meta_t *meta, bool report) {
  uint64_t timestamp = 0;
  return coherency_check_written(env, 0, meta, -1, report ? &timestamp : nullptr) == MDBX_SUCCESS;
}
