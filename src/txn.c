/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025

#include "internals.h"

__hot bool txn_gc_detent(const MDBX_txn *const txn) {
  const txnid_t detent = mvcc_shapshot_oldest(txn->env, txn->wr.troika.txnid[txn->wr.troika.prefer_steady]);
  if (likely(detent == txn->env->gc.detent))
    return false;

  txn->env->gc.detent = detent;
  return true;
}

void txn_done_cursors(MDBX_txn *txn) {
  tASSERT(txn, txn->flags & txn_may_have_cursors);

  TXN_FOREACH_DBI_ALL(txn, i) {
    MDBX_cursor *cursor = txn->cursors[i];
    if (cursor) {
      txn->cursors[i] = nullptr;
      do
        cursor = cursor_eot(cursor, txn);
      while (cursor);
    }
  }
  txn->flags &= ~txn_may_have_cursors;
}

int txn_shadow_cursors(const MDBX_txn *parent, const size_t dbi) {
  tASSERT(parent, dbi < parent->n_dbi);
  MDBX_cursor *cursor = parent->cursors[dbi];
  if (!cursor)
    return MDBX_SUCCESS;

  MDBX_txn *const txn = parent->nested;
  tASSERT(parent, parent->flags & txn_may_have_cursors);
  MDBX_cursor *next = nullptr;
  do {
    next = cursor->next;
    if (cursor->signature != cur_signature_live) {
      ENSURE(parent->env, cursor->signature == cur_signature_wait4eot);
      continue;
    }
    tASSERT(parent, cursor->txn == parent && dbi == cursor_dbi(cursor));

    int err = cursor_shadow(cursor, txn, dbi);
    if (unlikely(err != MDBX_SUCCESS)) {
      /* не получилось забекапить курсоры */
      txn->dbi_state[dbi] = DBI_OLDEN | DBI_LINDO | DBI_STALE;
      txn->flags |= MDBX_TXN_ERROR;
      return err;
    }
    cursor->next = txn->cursors[dbi];
    txn->cursors[dbi] = cursor;
    txn->flags |= txn_may_have_cursors;
  } while ((cursor = next) != nullptr);
  return MDBX_SUCCESS;
}

int txn_abort(MDBX_txn *txn) {
  if (txn->flags & MDBX_TXN_RDONLY)
    /* LY: don't close DBI-handles */
    return txn_end(txn, TXN_END_ABORT | TXN_END_UPDATE | TXN_END_SLOT | TXN_END_FREE);

  if (unlikely(txn->flags & MDBX_TXN_FINISHED))
    return MDBX_BAD_TXN;

  if (txn->nested)
    txn_abort(txn->nested);

  tASSERT(txn, (txn->flags & MDBX_TXN_ERROR) || dpl_check(txn));
  txn->flags |= /* avoid merge cursors' state */ MDBX_TXN_ERROR;
  return txn_end(txn, TXN_END_ABORT | TXN_END_SLOT | TXN_END_FREE);
}

static bool txn_check_overlapped(lck_t *const lck, const uint32_t pid, const uintptr_t tid) {
  const size_t snap_nreaders = atomic_load32(&lck->rdt_length, mo_AcquireRelease);
  for (size_t i = 0; i < snap_nreaders; ++i) {
    if (atomic_load32(&lck->rdt[i].pid, mo_Relaxed) == pid &&
        unlikely(atomic_load64(&lck->rdt[i].tid, mo_Relaxed) == tid)) {
      const txnid_t txnid = safe64_read(&lck->rdt[i].txnid);
      if (txnid >= MIN_TXNID && txnid <= MAX_TXNID)
        return true;
    }
  }
  return false;
}

int txn_renew(MDBX_txn *txn, unsigned flags) {
  MDBX_env *const env = txn->env;
  int rc;

  flags |= env->flags & (MDBX_NOSTICKYTHREADS | MDBX_WRITEMAP);
  if (flags & MDBX_TXN_RDONLY) {
    rc = txn_ro_start(txn, flags);
    if (unlikely(rc != MDBX_SUCCESS))
      goto bailout;
    ENSURE(env, txn->txnid >=
                    /* paranoia is appropriate here */ env->lck->cached_oldest.weak);
    tASSERT(txn, txn->dbs[FREE_DBI].flags == MDBX_INTEGERKEY);
    tASSERT(txn, check_table_flags(txn->dbs[MAIN_DBI].flags));
  } else {
    eASSERT(env, (flags & ~(txn_rw_begin_flags | MDBX_TXN_SPILLS | MDBX_WRITEMAP | MDBX_NOSTICKYTHREADS)) == 0);
    const uintptr_t tid = osal_thread_self();
    if (unlikely(txn->owner == tid ||
                 /* not recovery mode */ env->stuck_meta >= 0))
      return MDBX_BUSY;
    lck_t *const lck = env->lck_mmap.lck;
    if (lck && !(env->flags & MDBX_NOSTICKYTHREADS) && !(globals.runtime_flags & MDBX_DBG_LEGACY_OVERLAP) &&
        txn_check_overlapped(lck, env->pid, tid))
      return MDBX_TXN_OVERLAPPING;

    /* Not yet touching txn == env->basal_txn, it may be active */
    jitter4testing(false);
    rc = lck_txn_lock(env, !!(flags & MDBX_TXN_TRY));
    if (unlikely(rc))
      return rc;
    if (unlikely(env->flags & ENV_FATAL_ERROR)) {
      lck_txn_unlock(env);
      return MDBX_PANIC;
    }
#if defined(_WIN32) || defined(_WIN64)
    if (unlikely(!env->dxb_mmap.base)) {
      lck_txn_unlock(env);
      return MDBX_EPERM;
    }
#endif /* Windows */

    rc = txn_basal_start(txn, flags);
    if (unlikely(rc != MDBX_SUCCESS))
      goto bailout;
  }

  txn->front_txnid = txn->txnid + ((flags & (MDBX_WRITEMAP | MDBX_RDONLY)) == 0);

  /* Setup db info */
  tASSERT(txn, txn->dbs[FREE_DBI].flags == MDBX_INTEGERKEY);
  tASSERT(txn, check_table_flags(txn->dbs[MAIN_DBI].flags));
  VALGRIND_MAKE_MEM_UNDEFINED(txn->dbi_state, env->max_dbi);
#if MDBX_ENABLE_DBI_SPARSE
  txn->n_dbi = CORE_DBS;
  VALGRIND_MAKE_MEM_UNDEFINED(txn->dbi_sparse,
                              ceil_powerof2(env->max_dbi, CHAR_BIT * sizeof(txn->dbi_sparse[0])) / CHAR_BIT);
  txn->dbi_sparse[0] = (1 << CORE_DBS) - 1;
#else
  txn->n_dbi = (env->n_dbi < 8) ? env->n_dbi : 8;
  if (txn->n_dbi > CORE_DBS)
    memset(txn->dbi_state + CORE_DBS, 0, txn->n_dbi - CORE_DBS);
#endif /* MDBX_ENABLE_DBI_SPARSE */
  txn->dbi_state[FREE_DBI] = DBI_LINDO | DBI_VALID;
  txn->dbi_state[MAIN_DBI] = DBI_LINDO | DBI_VALID;
  txn->cursors[FREE_DBI] = nullptr;
  txn->cursors[MAIN_DBI] = nullptr;
  txn->dbi_seqs[FREE_DBI] = 0;
  txn->dbi_seqs[MAIN_DBI] = atomic_load32(&env->dbi_seqs[MAIN_DBI], mo_AcquireRelease);

  if (unlikely(env->dbs_flags[MAIN_DBI] != (DB_VALID | txn->dbs[MAIN_DBI].flags))) {
    const bool need_txn_lock = env->basal_txn && env->basal_txn->owner != osal_thread_self();
    bool should_unlock = false;
    if (need_txn_lock) {
      rc = lck_txn_lock(env, true);
      if (rc == MDBX_SUCCESS)
        should_unlock = true;
      else if (rc != MDBX_BUSY && rc != MDBX_EDEADLK)
        goto bailout;
    }
    rc = osal_fastmutex_acquire(&env->dbi_lock);
    if (likely(rc == MDBX_SUCCESS)) {
      uint32_t seq = dbi_seq_next(env, MAIN_DBI);
      /* проверяем повторно после захвата блокировки */
      if (env->dbs_flags[MAIN_DBI] != (DB_VALID | txn->dbs[MAIN_DBI].flags)) {
        if (!need_txn_lock || should_unlock ||
            /* если нет активной пишущей транзакции,
             * то следующая будет ждать на dbi_lock */
            !env->txn) {
          if (env->dbs_flags[MAIN_DBI] != 0 || MDBX_DEBUG)
            NOTICE("renew MainDB for %s-txn %" PRIaTXN " since db-flags changes 0x%x -> 0x%x",
                   (txn->flags & MDBX_TXN_RDONLY) ? "ro" : "rw", txn->txnid, env->dbs_flags[MAIN_DBI] & ~DB_VALID,
                   txn->dbs[MAIN_DBI].flags);
          env->dbs_flags[MAIN_DBI] = DB_POISON;
          atomic_store32(&env->dbi_seqs[MAIN_DBI], seq, mo_AcquireRelease);
          rc = tbl_setup(env, &env->kvs[MAIN_DBI], &txn->dbs[MAIN_DBI]);
          if (likely(rc == MDBX_SUCCESS)) {
            seq = dbi_seq_next(env, MAIN_DBI);
            env->dbs_flags[MAIN_DBI] = DB_VALID | txn->dbs[MAIN_DBI].flags;
            txn->dbi_seqs[MAIN_DBI] = atomic_store32(&env->dbi_seqs[MAIN_DBI], seq, mo_AcquireRelease);
          }
        } else {
          ERROR("MainDB db-flags changes 0x%x -> 0x%x ahead of read-txn "
                "%" PRIaTXN,
                txn->dbs[MAIN_DBI].flags, env->dbs_flags[MAIN_DBI] & ~DB_VALID, txn->txnid);
          rc = MDBX_INCOMPATIBLE;
        }
      }
      ENSURE(env, osal_fastmutex_release(&env->dbi_lock) == MDBX_SUCCESS);
    } else {
      DEBUG("dbi_lock failed, err %d", rc);
    }
    if (should_unlock)
      lck_txn_unlock(env);
    if (unlikely(rc != MDBX_SUCCESS))
      goto bailout;
  }

  if (unlikely(txn->dbs[FREE_DBI].flags != MDBX_INTEGERKEY)) {
    ERROR("unexpected/invalid db-flags 0x%x for %s", txn->dbs[FREE_DBI].flags, "GC/FreeDB");
    rc = MDBX_INCOMPATIBLE;
    goto bailout;
  }

  tASSERT(txn, txn->dbs[FREE_DBI].flags == MDBX_INTEGERKEY);
  tASSERT(txn, check_table_flags(txn->dbs[MAIN_DBI].flags));
  if (unlikely(env->flags & ENV_FATAL_ERROR)) {
    WARNING("%s", "environment had fatal error, must shutdown!");
    rc = MDBX_PANIC;
  } else {
    const size_t size_bytes = pgno2bytes(env, txn->geo.end_pgno);
    const size_t used_bytes = pgno2bytes(env, txn->geo.first_unallocated);
    const size_t required_bytes = (txn->flags & MDBX_TXN_RDONLY) ? used_bytes : size_bytes;
    eASSERT(env, env->dxb_mmap.limit >= env->dxb_mmap.current);
    if (unlikely(required_bytes > env->dxb_mmap.current)) {
      /* Размер БД (для пишущих транзакций) или используемых данных (для
       * читающих транзакций) больше предыдущего/текущего размера внутри
       * процесса, увеличиваем. Сюда также попадает случай увеличения верхней
       * границы размера БД и отображения. В читающих транзакциях нельзя
       * изменять размер файла, который может быть больше необходимого этой
       * транзакции. */
      if (txn->geo.upper > MAX_PAGENO + 1 || bytes2pgno(env, pgno2bytes(env, txn->geo.upper)) != txn->geo.upper) {
        rc = MDBX_UNABLE_EXTEND_MAPSIZE;
        goto bailout;
      }
      rc = dxb_resize(env, txn->geo.first_unallocated, txn->geo.end_pgno, txn->geo.upper, implicit_grow);
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;
      eASSERT(env, env->dxb_mmap.limit >= env->dxb_mmap.current);
    } else if (unlikely(size_bytes < env->dxb_mmap.current)) {
      /* Размер БД меньше предыдущего/текущего размера внутри процесса, можно
       * уменьшить, но всё сложнее:
       *  - размер файла согласован со всеми читаемыми снимками на момент
       *    коммита последней транзакции;
       *  - в читающей транзакции размер файла может быть больше и него нельзя
       *    изменять, в том числе менять madvise (меньша размера файла нельзя,
       *    а за размером нет смысла).
       *  - в пишущей транзакции уменьшать размер файла можно только после
       *    проверки размера читаемых снимков, но в этом нет смысла, так как
       *    это будет сделано при фиксации транзакции.
       *
       *  В сухом остатке, можно только установить dxb_mmap.current равным
       *  размеру файла, а это проще сделать без вызова dxb_resize() и усложения
       *  внутренней логики.
       *
       *  В этой тактике есть недостаток: если пишущите транзакции не регулярны,
       *  и при завершении такой транзакции файл БД остаётся не-уменьшеным из-за
       *  читающих транзакций использующих предыдущие снимки. */
#if defined(_WIN32) || defined(_WIN64)
      imports.srwl_AcquireShared(&env->remap_guard);
#else
      rc = osal_fastmutex_acquire(&env->remap_guard);
#endif
      if (likely(rc == MDBX_SUCCESS)) {
        eASSERT(env, env->dxb_mmap.limit >= env->dxb_mmap.current);
        rc = osal_filesize(env->dxb_mmap.fd, &env->dxb_mmap.filesize);
        if (likely(rc == MDBX_SUCCESS)) {
          eASSERT(env, env->dxb_mmap.filesize >= required_bytes);
          if (env->dxb_mmap.current > env->dxb_mmap.filesize)
            env->dxb_mmap.current =
                (env->dxb_mmap.limit < env->dxb_mmap.filesize) ? env->dxb_mmap.limit : (size_t)env->dxb_mmap.filesize;
        }
#if defined(_WIN32) || defined(_WIN64)
        imports.srwl_ReleaseShared(&env->remap_guard);
#else
        int err = osal_fastmutex_release(&env->remap_guard);
        if (unlikely(err) && likely(rc == MDBX_SUCCESS))
          rc = err;
#endif
      }
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;
    }
    eASSERT(env, pgno2bytes(env, txn->geo.first_unallocated) <= env->dxb_mmap.current);
    eASSERT(env, env->dxb_mmap.limit >= env->dxb_mmap.current);
    if (txn->flags & MDBX_TXN_RDONLY) {
#if defined(_WIN32) || defined(_WIN64)
      if (((used_bytes > env->geo_in_bytes.lower && env->geo_in_bytes.shrink) ||
           (globals.running_under_Wine &&
            /* under Wine acquisition of remap_guard is always required,
             * since Wine don't support section extending,
             * i.e. in both cases unmap+map are required. */
            used_bytes < env->geo_in_bytes.upper && env->geo_in_bytes.grow)) &&
          /* avoid recursive use SRW */ (txn->flags & MDBX_NOSTICKYTHREADS) == 0) {
        txn->flags |= txn_shrink_allowed;
        imports.srwl_AcquireShared(&env->remap_guard);
      }
#endif /* Windows */
    } else {
      tASSERT(txn, txn == env->basal_txn);

      if (env->options.need_dp_limit_adjust)
        env_options_adjust_dp_limit(env);
      if ((txn->flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC) {
        rc = dpl_alloc(txn);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
        txn->wr.dirtyroom = txn->env->options.dp_limit;
        txn->wr.dirtylru = MDBX_DEBUG ? UINT32_MAX / 3 - 42 : 0;
      } else {
        tASSERT(txn, txn->wr.dirtylist == nullptr);
        txn->wr.dirtylist = nullptr;
        txn->wr.dirtyroom = MAX_PAGENO;
        txn->wr.dirtylru = 0;
      }
      eASSERT(env, txn->wr.writemap_dirty_npages == 0);
      eASSERT(env, txn->wr.writemap_spilled_npages == 0);

      MDBX_cursor *const gc = ptr_disp(txn, sizeof(MDBX_txn));
      rc = cursor_init(gc, txn, FREE_DBI);
      if (rc != MDBX_SUCCESS)
        goto bailout;
      tASSERT(txn, txn->cursors[FREE_DBI] == nullptr);
    }
    dxb_sanitize_tail(env, txn);
    return MDBX_SUCCESS;
  }
bailout:
  tASSERT(txn, rc != MDBX_SUCCESS);
  txn_end(txn, TXN_END_SLOT | TXN_END_FAIL_BEGIN);
  return rc;
}

int txn_end(MDBX_txn *txn, unsigned mode) {
  static const char *const names[] = TXN_END_NAMES;
  DEBUG("%s txn %" PRIaTXN "%c-0x%X %p  on env %p, root page %" PRIaPGNO "/%" PRIaPGNO, names[mode & TXN_END_OPMASK],
        txn->txnid, (txn->flags & MDBX_TXN_RDONLY) ? 'r' : 'w', txn->flags, (void *)txn, (void *)txn->env,
        txn->dbs[MAIN_DBI].root, txn->dbs[FREE_DBI].root);

  tASSERT(txn, /* txn->signature == txn_signature && */ !txn->nested && !(txn->flags & MDBX_TXN_HAS_CHILD));
  if (txn->flags & txn_may_have_cursors)
    txn_done_cursors(txn);

  MDBX_env *const env = txn->env;
  MDBX_txn *const parent = txn->parent;
  if (txn == env->basal_txn) {
    tASSERT(txn, !parent && !(txn->flags & (MDBX_TXN_RDONLY | MDBX_TXN_FINISHED)) && txn->owner);
    return txn_basal_end(txn, mode);
  }

  if (txn->flags & MDBX_TXN_RDONLY) {
    tASSERT(txn, txn != env->txn && !parent);
    return txn_ro_end(txn, mode);
  }

  if (unlikely(!parent || txn != env->txn || parent->signature != txn_signature || parent->nested != txn ||
               !(parent->flags & MDBX_TXN_HAS_CHILD) || txn == env->basal_txn)) {
    ERROR("parent txn %p is invalid or mismatch for nested txn %p", (void *)parent, (void *)txn);
    return MDBX_PROBLEM;
  }
  tASSERT(txn, pnl_check_allocated(txn->wr.repnl, txn->geo.first_unallocated - MDBX_ENABLE_REFUND));
  tASSERT(txn, memcmp(&txn->wr.troika, &parent->wr.troika, sizeof(troika_t)) == 0);
  tASSERT(txn, mode & TXN_END_FREE);
  tASSERT(parent, parent->flags & MDBX_TXN_HAS_CHILD);
  env->txn = parent;
  parent->nested = nullptr;
  parent->flags -= MDBX_TXN_HAS_CHILD;
  const pgno_t nested_now = txn->geo.now, nested_upper = txn->geo.upper;
  txn_nested_abort(txn);

  if (unlikely(parent->geo.upper != nested_upper || parent->geo.now != nested_now) &&
      !(parent->flags & MDBX_TXN_ERROR) && !(env->flags & ENV_FATAL_ERROR)) {
    /* undo resize performed by nested txn */
    int err = dxb_resize(env, parent->geo.first_unallocated, parent->geo.now, parent->geo.upper, impilict_shrink);
    if (err == MDBX_EPERM) {
      /* unable undo resize (it is regular for Windows),
       * therefore promote size changes from nested to the parent txn */
      WARNING("unable undo resize performed by nested txn, promote to "
              "the parent (%u->%u, %u->%u)",
              nested_now, parent->geo.now, nested_upper, parent->geo.upper);
      parent->geo.now = nested_now;
      parent->flags |= MDBX_TXN_DIRTY;
    } else if (unlikely(err != MDBX_SUCCESS)) {
      ERROR("error %d while undo resize performed by nested txn, fail the parent", err);
      mdbx_txn_break(env->basal_txn);
      parent->flags |= MDBX_TXN_ERROR;
      if (!env->dxb_mmap.base)
        env->flags |= ENV_FATAL_ERROR;
      return err;
    }
  }
  return MDBX_SUCCESS;
}

int txn_check_badbits_parked(const MDBX_txn *txn, int bad_bits) {
  tASSERT(txn, (bad_bits & MDBX_TXN_PARKED) && (txn->flags & bad_bits));
  /* Здесь осознано заложено отличие в поведении припаркованных транзакций:
   *  - некоторые функции (например mdbx_env_info_ex()), допускают
   *    использование поломанных транзакций (с флагом MDBX_TXN_ERROR), но
   *    не могут работать с припаркованными транзакциями (требуют распарковки).
   *  - но при распарковке поломанные транзакции завершаются.
   *  - получается что транзакцию можно припарковать, потом поломать вызвав
   *    mdbx_txn_break(), но далее любое её использование приведет к завершению
   *    при распарковке.
   *
   * Поэтому для припаркованных транзакций возвращается ошибка если не-включена
   * авто-распарковка, либо есть другие плохие биты. */
  if ((txn->flags & (bad_bits | MDBX_TXN_AUTOUNPARK)) != (MDBX_TXN_PARKED | MDBX_TXN_AUTOUNPARK))
    return LOG_IFERR(MDBX_BAD_TXN);

  tASSERT(txn, bad_bits == MDBX_TXN_BLOCKED || bad_bits == MDBX_TXN_BLOCKED - MDBX_TXN_ERROR);
  return mdbx_txn_unpark((MDBX_txn *)txn, false);
}

MDBX_txn *txn_alloc(const MDBX_txn_flags_t flags, MDBX_env *env) {
  MDBX_txn *txn = nullptr;
  const intptr_t bitmap_bytes =
#if MDBX_ENABLE_DBI_SPARSE
      ceil_powerof2(env->max_dbi, CHAR_BIT * sizeof(txn->dbi_sparse[0])) / CHAR_BIT;
#else
      0;
#endif /* MDBX_ENABLE_DBI_SPARSE */
  STATIC_ASSERT(sizeof(txn->wr) > sizeof(txn->ro));
  const size_t base =
      (flags & MDBX_TXN_RDONLY) ? sizeof(MDBX_txn) - sizeof(txn->wr) + sizeof(txn->ro) : sizeof(MDBX_txn);
  const size_t size = base +
                      ((flags & MDBX_TXN_RDONLY) ? (size_t)bitmap_bytes + env->max_dbi * sizeof(txn->dbi_seqs[0]) : 0) +
                      env->max_dbi * (sizeof(txn->dbs[0]) + sizeof(txn->cursors[0]) + sizeof(txn->dbi_state[0]));
  txn = osal_malloc(size);
  if (unlikely(!txn))
    return txn;
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

  if (flags & MDBX_TXN_RDONLY) {
    txn->dbi_seqs = ptr_disp(txn->cursors, env->max_dbi * sizeof(txn->cursors[0]));
#if MDBX_ENABLE_DBI_SPARSE
    txn->dbi_sparse = ptr_disp(txn->dbi_state, -bitmap_bytes);
#endif /* MDBX_ENABLE_DBI_SPARSE */
  }
  return txn;
}
