/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024

#include "internals.h"

#if MDBX_ENABLE_DBI_SPARSE
size_t dbi_bitmap_ctz_fallback(const MDBX_txn *txn, intptr_t bmi) {
  tASSERT(txn, bmi > 0);
  bmi &= -bmi;
  if (sizeof(txn->dbi_sparse[0]) > 4) {
    static const uint8_t debruijn_ctz64[64] = {0,  1,  2,  53, 3,  7,  54, 27, 4,  38, 41, 8,  34, 55, 48, 28,
                                               62, 5,  39, 46, 44, 42, 22, 9,  24, 35, 59, 56, 49, 18, 29, 11,
                                               63, 52, 6,  26, 37, 40, 33, 47, 61, 45, 43, 21, 23, 58, 17, 10,
                                               51, 25, 36, 32, 60, 20, 57, 16, 50, 31, 19, 15, 30, 14, 13, 12};
    return debruijn_ctz64[(UINT64_C(0x022FDD63CC95386D) * (uint64_t)bmi) >> 58];
  } else {
    static const uint8_t debruijn_ctz32[32] = {0,  1,  28, 2,  29, 14, 24, 3, 30, 22, 20, 15, 25, 17, 4,  8,
                                               31, 27, 13, 23, 21, 19, 16, 7, 26, 12, 18, 6,  11, 5,  10, 9};
    return debruijn_ctz32[(UINT32_C(0x077CB531) * (uint32_t)bmi) >> 27];
  }
}
#endif /* MDBX_ENABLE_DBI_SPARSE */

struct dbi_snap_result dbi_snap(const MDBX_env *env, const size_t dbi) {
  eASSERT(env, dbi < env->n_dbi);
  struct dbi_snap_result r;
  uint32_t snap = atomic_load32(&env->dbi_seqs[dbi], mo_AcquireRelease);
  do {
    r.sequence = snap;
    r.flags = env->dbs_flags[dbi];
    snap = atomic_load32(&env->dbi_seqs[dbi], mo_AcquireRelease);
  } while (unlikely(snap != r.sequence));
  return r;
}

__noinline int dbi_import(MDBX_txn *txn, const size_t dbi) {
  const MDBX_env *const env = txn->env;
  if (dbi >= env->n_dbi || !env->dbs_flags[dbi])
    return MDBX_BAD_DBI;

#if MDBX_ENABLE_DBI_SPARSE
  const size_t bitmap_chunk = CHAR_BIT * sizeof(txn->dbi_sparse[0]);
  const size_t bitmap_indx = dbi / bitmap_chunk;
  const size_t bitmap_mask = (size_t)1 << dbi % bitmap_chunk;
  if (dbi >= txn->n_dbi) {
    for (size_t i = (txn->n_dbi + bitmap_chunk - 1) / bitmap_chunk; bitmap_indx >= i; ++i)
      txn->dbi_sparse[i] = 0;
    eASSERT(env, (txn->dbi_sparse[bitmap_indx] & bitmap_mask) == 0);
    MDBX_txn *scan = txn;
    do {
      eASSERT(env, scan->dbi_sparse == txn->dbi_sparse);
      eASSERT(env, scan->n_dbi < dbi + 1);
      scan->n_dbi = (unsigned)dbi + 1;
      scan->dbi_state[dbi] = 0;
      scan = scan->parent;
    } while (scan /* && scan->dbi_sparse == txn->dbi_sparse */);
    txn->dbi_sparse[bitmap_indx] |= bitmap_mask;
    goto lindo;
  }
  if ((txn->dbi_sparse[bitmap_indx] & bitmap_mask) == 0) {
    MDBX_txn *scan = txn;
    do {
      eASSERT(env, scan->dbi_sparse == txn->dbi_sparse);
      eASSERT(env, scan->n_dbi == txn->n_dbi);
      scan->dbi_state[dbi] = 0;
      scan = scan->parent;
    } while (scan /* && scan->dbi_sparse == txn->dbi_sparse */);
    txn->dbi_sparse[bitmap_indx] |= bitmap_mask;
    goto lindo;
  }
#else
  if (dbi >= txn->n_dbi) {
    size_t i = txn->n_dbi;
    do
      txn->dbi_state[i] = 0;
    while (dbi >= ++i);
    txn->n_dbi = i;
    goto lindo;
  }
#endif /* MDBX_ENABLE_DBI_SPARSE */

  if (!txn->dbi_state[dbi]) {
  lindo:
    /* dbi-слот еще не инициализирован в транзакции, а хендл не использовался */
    txn->cursors[dbi] = nullptr;
    MDBX_txn *const parent = txn->parent;
    if (parent) {
      /* вложенная пишущая транзакция */
      int rc = dbi_check(parent, dbi);
      /* копируем состояние table очищая new-флаги. */
      eASSERT(env, txn->dbi_seqs == parent->dbi_seqs);
      txn->dbi_state[dbi] = parent->dbi_state[dbi] & ~(DBI_FRESH | DBI_CREAT | DBI_DIRTY);
      if (likely(rc == MDBX_SUCCESS)) {
        txn->dbs[dbi] = parent->dbs[dbi];
        if (parent->cursors[dbi]) {
          rc = cursor_shadow(parent->cursors[dbi], txn, dbi);
          if (unlikely(rc != MDBX_SUCCESS)) {
            /* не получилось забекапить курсоры */
            txn->dbi_state[dbi] = DBI_OLDEN | DBI_LINDO | DBI_STALE;
            txn->flags |= MDBX_TXN_ERROR;
          }
        }
      }
      return rc;
    }
    txn->dbi_seqs[dbi] = 0;
    txn->dbi_state[dbi] = DBI_LINDO;
  } else {
    eASSERT(env, txn->dbi_seqs[dbi] != env->dbi_seqs[dbi].weak);
    if (unlikely((txn->dbi_state[dbi] & (DBI_VALID | DBI_OLDEN)) || txn->cursors[dbi])) {
      /* хендл уже использовался в транзакции, но был закрыт или переоткрыт,
       * либо при явном пере-открытии хендла есть висячие курсоры */
      eASSERT(env, (txn->dbi_state[dbi] & DBI_STALE) == 0);
      txn->dbi_seqs[dbi] = env->dbi_seqs[dbi].weak;
      txn->dbi_state[dbi] = DBI_OLDEN | DBI_LINDO;
      return txn->cursors[dbi] ? MDBX_DANGLING_DBI : MDBX_BAD_DBI;
    }
  }

  /* хендл не использовался в транзакции, либо явно пере-отрывается при
   * отсутствии висячих курсоров */
  eASSERT(env, (txn->dbi_state[dbi] & DBI_LINDO) && !txn->cursors[dbi]);

  /* читаем актуальные флаги и sequence */
  struct dbi_snap_result snap = dbi_snap(env, dbi);
  txn->dbi_seqs[dbi] = snap.sequence;
  if (snap.flags & DB_VALID) {
    txn->dbs[dbi].flags = snap.flags & DB_PERSISTENT_FLAGS;
    txn->dbi_state[dbi] = DBI_LINDO | DBI_VALID | DBI_STALE;
    return MDBX_SUCCESS;
  }
  return MDBX_BAD_DBI;
}

static int defer_and_release(MDBX_env *const env, defer_free_item_t *const chain) {
  size_t length = 0;
  defer_free_item_t *obsolete_chain = nullptr;
#if MDBX_ENABLE_DBI_LOCKFREE
  const uint64_t now = osal_monotime();
  defer_free_item_t **scan = &env->defer_free;
  if (env->defer_free) {
    const uint64_t threshold_1second = osal_16dot16_to_monotime(1 * 65536);
    do {
      defer_free_item_t *item = *scan;
      if (now - item->timestamp < threshold_1second) {
        scan = &item->next;
        length += 1;
      } else {
        *scan = item->next;
        item->next = obsolete_chain;
        obsolete_chain = item;
      }
    } while (*scan);
  }

  eASSERT(env, *scan == nullptr);
  if (chain) {
    defer_free_item_t *item = chain;
    do {
      item->timestamp = now;
      item = item->next;
    } while (item);
    *scan = chain;
  }
#else  /* MDBX_ENABLE_DBI_LOCKFREE */
  obsolete_chain = chain;
#endif /* MDBX_ENABLE_DBI_LOCKFREE */

  ENSURE(env, osal_fastmutex_release(&env->dbi_lock) == MDBX_SUCCESS);
  if (length > 42) {
#if defined(_WIN32) || defined(_WIN64)
    SwitchToThread();
#else
    sched_yield();
#endif /* Windows */
  }
  while (obsolete_chain) {
    defer_free_item_t *item = obsolete_chain;
    obsolete_chain = obsolete_chain->next;
    osal_free(item);
  }
  return chain ? MDBX_SUCCESS : MDBX_BAD_DBI;
}

/* Export or close DBI handles opened in this txn. */
int dbi_update(MDBX_txn *txn, int keep) {
  MDBX_env *const env = txn->env;
  tASSERT(txn, !txn->parent && txn == env->basal_txn);
  bool locked = false;
  defer_free_item_t *defer_chain = nullptr;
  TXN_FOREACH_DBI_USER(txn, dbi) {
    if (likely((txn->dbi_state[dbi] & DBI_CREAT) == 0))
      continue;
    if (!locked) {
      int err = osal_fastmutex_acquire(&env->dbi_lock);
      if (unlikely(err != MDBX_SUCCESS))
        return err;
      locked = true;
      if (dbi >= env->n_dbi)
        /* хендл был закрыт из другого потока пока захватывали блокировку */
        continue;
    }
    tASSERT(txn, dbi < env->n_dbi);
    if (keep) {
      env->dbs_flags[dbi] = txn->dbs[dbi].flags | DB_VALID;
    } else {
      uint32_t seq = dbi_seq_next(env, dbi);
      defer_free_item_t *item = env->kvs[dbi].name.iov_base;
      if (item) {
        env->dbs_flags[dbi] = 0;
        env->kvs[dbi].name.iov_len = 0;
        env->kvs[dbi].name.iov_base = nullptr;
        atomic_store32(&env->dbi_seqs[dbi], seq, mo_AcquireRelease);
        osal_flush_incoherent_cpu_writeback();
        item->next = defer_chain;
        defer_chain = item;
      } else {
        eASSERT(env, env->kvs[dbi].name.iov_len == 0);
        eASSERT(env, env->dbs_flags[dbi] == 0);
      }
    }
  }

  if (locked) {
    size_t i = env->n_dbi;
    while ((env->dbs_flags[i - 1] & DB_VALID) == 0) {
      --i;
      eASSERT(env, i >= CORE_DBS);
      eASSERT(env, !env->dbs_flags[i] && !env->kvs[i].name.iov_len && !env->kvs[i].name.iov_base);
    }
    env->n_dbi = (unsigned)i;
    defer_and_release(env, defer_chain);
  }
  return MDBX_SUCCESS;
}

int dbi_bind(MDBX_txn *txn, const size_t dbi, unsigned user_flags, MDBX_cmp_func *keycmp, MDBX_cmp_func *datacmp) {
  const MDBX_env *const env = txn->env;
  eASSERT(env, dbi < txn->n_dbi && dbi < env->n_dbi);
  eASSERT(env, dbi_state(txn, dbi) & DBI_LINDO);
  eASSERT(env, env->dbs_flags[dbi] != DB_POISON);
  if ((env->dbs_flags[dbi] & DB_VALID) == 0) {
    eASSERT(env, !env->kvs[dbi].clc.k.cmp && !env->kvs[dbi].clc.v.cmp && !env->kvs[dbi].name.iov_len &&
                     !env->kvs[dbi].name.iov_base && !env->kvs[dbi].clc.k.lmax && !env->kvs[dbi].clc.k.lmin &&
                     !env->kvs[dbi].clc.v.lmax && !env->kvs[dbi].clc.v.lmin);
  } else {
    eASSERT(env, !(txn->dbi_state[dbi] & DBI_VALID) || (txn->dbs[dbi].flags | DB_VALID) == env->dbs_flags[dbi]);
    eASSERT(env, env->kvs[dbi].name.iov_base || dbi < CORE_DBS);
  }

  /* Если dbi уже использовался, то корректными считаем четыре варианта:
   * 1) user_flags равны MDBX_DB_ACCEDE
   *   = предполагаем что пользователь открывает существующую table,
   *     при этом код проверки не позволит установить другие компараторы.
   * 2) user_flags нулевые, а оба компаратора пустые/нулевые или равны текущим
   *   = предполагаем что пользователь открывает существующую table
   *     старым способом с нулевыми с флагами по-умолчанию.
   * 3) user_flags совпадают, а компараторы не заданы или те же
   *    = предполагаем что пользователь открывает table указывая все параметры;
   * 4) user_flags отличаются, но table пустая и задан флаг MDBX_CREATE
   *    = предполагаем что пользователь пересоздает table;
   */
  if ((user_flags & ~MDBX_CREATE) != (unsigned)(env->dbs_flags[dbi] & DB_PERSISTENT_FLAGS)) {
    /* flags are differs, check other conditions */
    if ((!user_flags && (!keycmp || keycmp == env->kvs[dbi].clc.k.cmp) &&
         (!datacmp || datacmp == env->kvs[dbi].clc.v.cmp)) ||
        user_flags == MDBX_DB_ACCEDE) {
      user_flags = env->dbs_flags[dbi] & DB_PERSISTENT_FLAGS;
    } else if ((user_flags & MDBX_CREATE) == 0)
      return /* FIXME: return extended info */ MDBX_INCOMPATIBLE;
    else {
      if (txn->dbi_state[dbi] & DBI_STALE) {
        eASSERT(env, env->dbs_flags[dbi] & DB_VALID);
        int err = tbl_fetch(txn, dbi);
        if (unlikely(err == MDBX_SUCCESS))
          return err;
      }
      eASSERT(env, ((env->dbs_flags[dbi] ^ txn->dbs[dbi].flags) & DB_PERSISTENT_FLAGS) == 0);
      eASSERT(env, (txn->dbi_state[dbi] & (DBI_LINDO | DBI_VALID | DBI_STALE)) == (DBI_LINDO | DBI_VALID));
      if (unlikely(txn->dbs[dbi].leaf_pages))
        return /* FIXME: return extended info */ MDBX_INCOMPATIBLE;

      /* Пересоздаём table если там пусто */
      if (unlikely(txn->cursors[dbi]))
        return MDBX_DANGLING_DBI;
      env->dbs_flags[dbi] = DB_POISON;
      atomic_store32(&env->dbi_seqs[dbi], dbi_seq_next(env, dbi), mo_AcquireRelease);

      const uint32_t seq = dbi_seq_next(env, dbi);
      const uint16_t db_flags = user_flags & DB_PERSISTENT_FLAGS;
      eASSERT(env, txn->dbs[dbi].height == 0 && txn->dbs[dbi].items == 0 && txn->dbs[dbi].root == P_INVALID);
      env->kvs[dbi].clc.k.cmp = keycmp ? keycmp : builtin_keycmp(user_flags);
      env->kvs[dbi].clc.v.cmp = datacmp ? datacmp : builtin_datacmp(user_flags);
      txn->dbs[dbi].flags = db_flags;
      txn->dbs[dbi].dupfix_size = 0;
      if (unlikely(tbl_setup(env, &env->kvs[dbi], &txn->dbs[dbi]))) {
        txn->dbi_state[dbi] = DBI_LINDO;
        txn->flags |= MDBX_TXN_ERROR;
        return MDBX_PROBLEM;
      }

      env->dbs_flags[dbi] = db_flags | DB_VALID;
      atomic_store32(&env->dbi_seqs[dbi], seq, mo_AcquireRelease);
      txn->dbi_seqs[dbi] = seq;
      txn->dbi_state[dbi] = DBI_LINDO | DBI_VALID | DBI_CREAT | DBI_DIRTY;
      txn->flags |= MDBX_TXN_DIRTY;
    }
  }

  if (!keycmp)
    keycmp = (env->dbs_flags[dbi] & DB_VALID) ? env->kvs[dbi].clc.k.cmp : builtin_keycmp(user_flags);
  if (env->kvs[dbi].clc.k.cmp != keycmp) {
    if (env->dbs_flags[dbi] & DB_VALID)
      return MDBX_EINVAL;
    env->kvs[dbi].clc.k.cmp = keycmp;
  }

  if (!datacmp)
    datacmp = (env->dbs_flags[dbi] & DB_VALID) ? env->kvs[dbi].clc.v.cmp : builtin_datacmp(user_flags);
  if (env->kvs[dbi].clc.v.cmp != datacmp) {
    if (env->dbs_flags[dbi] & DB_VALID)
      return MDBX_EINVAL;
    env->kvs[dbi].clc.v.cmp = datacmp;
  }

  return MDBX_SUCCESS;
}

static inline size_t dbi_namelen(const MDBX_val name) {
  return (name.iov_len > sizeof(defer_free_item_t)) ? name.iov_len : sizeof(defer_free_item_t);
}

static int dbi_open_locked(MDBX_txn *txn, unsigned user_flags, MDBX_dbi *dbi, MDBX_cmp_func *keycmp,
                           MDBX_cmp_func *datacmp, MDBX_val name) {
  MDBX_env *const env = txn->env;

  /* Cannot mix named table(s) with DUPSORT flags */
  tASSERT(txn, (txn->dbi_state[MAIN_DBI] & (DBI_LINDO | DBI_VALID | DBI_STALE)) == (DBI_LINDO | DBI_VALID));
  if (unlikely(txn->dbs[MAIN_DBI].flags & MDBX_DUPSORT)) {
    if (unlikely((user_flags & MDBX_CREATE) == 0))
      return MDBX_NOTFOUND;
    if (unlikely(txn->dbs[MAIN_DBI].leaf_pages))
      /* В MainDB есть записи, либо она уже использовалась. */
      return MDBX_INCOMPATIBLE;

    /* Пересоздаём MainDB когда там пусто. */
    tASSERT(txn,
            txn->dbs[MAIN_DBI].height == 0 && txn->dbs[MAIN_DBI].items == 0 && txn->dbs[MAIN_DBI].root == P_INVALID);
    if (unlikely(txn->cursors[MAIN_DBI]))
      return MDBX_DANGLING_DBI;
    env->dbs_flags[MAIN_DBI] = DB_POISON;
    atomic_store32(&env->dbi_seqs[MAIN_DBI], dbi_seq_next(env, MAIN_DBI), mo_AcquireRelease);

    const uint32_t seq = dbi_seq_next(env, MAIN_DBI);
    const uint16_t main_flags = txn->dbs[MAIN_DBI].flags & (MDBX_REVERSEKEY | MDBX_INTEGERKEY);
    env->kvs[MAIN_DBI].clc.k.cmp = builtin_keycmp(main_flags);
    env->kvs[MAIN_DBI].clc.v.cmp = builtin_datacmp(main_flags);
    txn->dbs[MAIN_DBI].flags = main_flags;
    txn->dbs[MAIN_DBI].dupfix_size = 0;
    int err = tbl_setup(env, &env->kvs[MAIN_DBI], &txn->dbs[MAIN_DBI]);
    if (unlikely(err != MDBX_SUCCESS)) {
      txn->dbi_state[MAIN_DBI] = DBI_LINDO;
      txn->flags |= MDBX_TXN_ERROR;
      env->flags |= ENV_FATAL_ERROR;
      return err;
    }
    env->dbs_flags[MAIN_DBI] = main_flags | DB_VALID;
    txn->dbi_seqs[MAIN_DBI] = atomic_store32(&env->dbi_seqs[MAIN_DBI], seq, mo_AcquireRelease);
    txn->dbi_state[MAIN_DBI] |= DBI_DIRTY;
    txn->flags |= MDBX_TXN_DIRTY;
  }

  tASSERT(txn, env->kvs[MAIN_DBI].clc.k.cmp);

  /* Is the DB already open? */
  size_t slot = env->n_dbi;
  for (size_t scan = CORE_DBS; scan < env->n_dbi; ++scan) {
    if ((env->dbs_flags[scan] & DB_VALID) == 0) {
      /* Remember this free slot */
      slot = (slot < scan) ? slot : scan;
      continue;
    }
    if (!env->kvs[MAIN_DBI].clc.k.cmp(&name, &env->kvs[scan].name)) {
      slot = scan;
      int err = dbi_check(txn, slot);
      if (err == MDBX_BAD_DBI && txn->dbi_state[slot] == (DBI_OLDEN | DBI_LINDO)) {
        /* хендл использовался, стал невалидным,
         * но теперь явно пере-открывается в этой транзакци */
        eASSERT(env, !txn->cursors[slot]);
        txn->dbi_state[slot] = DBI_LINDO;
        err = dbi_check(txn, slot);
      }
      if (err == MDBX_SUCCESS) {
        err = dbi_bind(txn, slot, user_flags, keycmp, datacmp);
        if (likely(err == MDBX_SUCCESS)) {
          goto done;
        }
      }
      return err;
    }
  }

  /* Fail, if no free slot and max hit */
  if (unlikely(slot >= env->max_dbi))
    return MDBX_DBS_FULL;

  if (env->n_dbi == slot)
    eASSERT(env, !env->dbs_flags[slot] && !env->kvs[slot].name.iov_len && !env->kvs[slot].name.iov_base);

  env->dbs_flags[slot] = DB_POISON;
  atomic_store32(&env->dbi_seqs[slot], dbi_seq_next(env, slot), mo_AcquireRelease);
  memset(&env->kvs[slot], 0, sizeof(env->kvs[slot]));
  if (env->n_dbi == slot)
    env->n_dbi = (unsigned)slot + 1;
  eASSERT(env, slot < env->n_dbi);

  int err = dbi_check(txn, slot);
  eASSERT(env, err == MDBX_BAD_DBI);
  if (err != MDBX_BAD_DBI)
    return MDBX_PROBLEM;

  /* Find the DB info */
  MDBX_val body;
  cursor_couple_t cx;
  int rc = cursor_init(&cx.outer, txn, MAIN_DBI);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;
  rc = cursor_seek(&cx.outer, &name, &body, MDBX_SET).err;
  if (unlikely(rc != MDBX_SUCCESS)) {
    if (rc != MDBX_NOTFOUND || !(user_flags & MDBX_CREATE))
      return rc;
  } else {
    /* make sure this is actually a table */
    node_t *node = page_node(cx.outer.pg[cx.outer.top], cx.outer.ki[cx.outer.top]);
    if (unlikely((node_flags(node) & (N_DUP | N_TREE)) != N_TREE))
      return MDBX_INCOMPATIBLE;
    if (!MDBX_DISABLE_VALIDATION && unlikely(body.iov_len != sizeof(tree_t))) {
      ERROR("%s/%d: %s %zu", "MDBX_CORRUPTED", MDBX_CORRUPTED, "invalid table node size", body.iov_len);
      return MDBX_CORRUPTED;
    }
    memcpy(&txn->dbs[slot], body.iov_base, sizeof(tree_t));
  }

  /* Done here so we cannot fail after creating a new DB */
  defer_free_item_t *const clone = osal_malloc(dbi_namelen(name));
  if (unlikely(!clone))
    return MDBX_ENOMEM;
  memcpy(clone, name.iov_base, name.iov_len);
  name.iov_base = clone;

  uint8_t dbi_state = DBI_LINDO | DBI_VALID | DBI_FRESH;
  if (unlikely(rc)) {
    /* MDBX_NOTFOUND and MDBX_CREATE: Create new DB */
    tASSERT(txn, rc == MDBX_NOTFOUND);
    body.iov_base = memset(&txn->dbs[slot], 0, body.iov_len = sizeof(tree_t));
    txn->dbs[slot].root = P_INVALID;
    txn->dbs[slot].mod_txnid = txn->txnid;
    txn->dbs[slot].flags = user_flags & DB_PERSISTENT_FLAGS;
    cx.outer.next = txn->cursors[MAIN_DBI];
    txn->cursors[MAIN_DBI] = &cx.outer;
    rc = cursor_put_checklen(&cx.outer, &name, &body, N_TREE | MDBX_NOOVERWRITE);
    txn->cursors[MAIN_DBI] = cx.outer.next;
    if (unlikely(rc != MDBX_SUCCESS))
      goto bailout;

    dbi_state |= DBI_DIRTY | DBI_CREAT;
    txn->flags |= MDBX_TXN_DIRTY;
    tASSERT(txn, (txn->dbi_state[MAIN_DBI] & DBI_DIRTY) != 0);
  }

  /* Got info, register DBI in this txn */
  const uint32_t seq = dbi_seq_next(env, slot);
  eASSERT(env, env->dbs_flags[slot] == DB_POISON && !txn->cursors[slot] &&
                   (txn->dbi_state[slot] & (DBI_LINDO | DBI_VALID)) == DBI_LINDO);
  txn->dbi_state[slot] = dbi_state;
  memcpy(&txn->dbs[slot], body.iov_base, sizeof(txn->dbs[slot]));
  env->dbs_flags[slot] = txn->dbs[slot].flags;
  rc = dbi_bind(txn, slot, user_flags, keycmp, datacmp);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout;

  env->kvs[slot].name = name;
  env->dbs_flags[slot] = txn->dbs[slot].flags | DB_VALID;
  txn->dbi_seqs[slot] = atomic_store32(&env->dbi_seqs[slot], seq, mo_AcquireRelease);

done:
  *dbi = (MDBX_dbi)slot;
  tASSERT(txn, slot < txn->n_dbi && (env->dbs_flags[slot] & DB_VALID) != 0);
  eASSERT(env, dbi_check(txn, slot) == MDBX_SUCCESS);
  return MDBX_SUCCESS;

bailout:
  eASSERT(env, !txn->cursors[slot] && !env->kvs[slot].name.iov_len && !env->kvs[slot].name.iov_base);
  txn->dbi_state[slot] &= DBI_LINDO | DBI_OLDEN;
  env->dbs_flags[slot] = 0;
  osal_free(clone);
  if (slot + 1 == env->n_dbi)
    txn->n_dbi = env->n_dbi = (unsigned)slot;
  return rc;
}

int dbi_open(MDBX_txn *txn, const MDBX_val *const name, unsigned user_flags, MDBX_dbi *dbi, MDBX_cmp_func *keycmp,
             MDBX_cmp_func *datacmp) {
  if (unlikely(!dbi))
    return MDBX_EINVAL;
  *dbi = 0;

  if (user_flags != MDBX_ACCEDE && unlikely(!check_table_flags(user_flags & ~MDBX_CREATE)))
    return MDBX_EINVAL;

  int rc = check_txn(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if ((user_flags & MDBX_CREATE) && unlikely(txn->flags & MDBX_TXN_RDONLY))
    return MDBX_EACCESS;

  /* main table? */
  if (unlikely(name == MDBX_CHK_MAIN || name->iov_base == MDBX_CHK_MAIN)) {
    rc = dbi_bind(txn, MAIN_DBI, user_flags, keycmp, datacmp);
    if (likely(rc == MDBX_SUCCESS))
      *dbi = MAIN_DBI;
    return rc;
  }
  if (unlikely(name == MDBX_CHK_GC || name->iov_base == MDBX_CHK_GC)) {
    rc = dbi_bind(txn, FREE_DBI, user_flags, keycmp, datacmp);
    if (likely(rc == MDBX_SUCCESS))
      *dbi = FREE_DBI;
    return rc;
  }
  if (unlikely(name == MDBX_CHK_META || name->iov_base == MDBX_CHK_META))
    return MDBX_EINVAL;
  if (unlikely(name->iov_len > txn->env->leaf_nodemax - NODESIZE - sizeof(tree_t)))
    return MDBX_EINVAL;

#if MDBX_ENABLE_DBI_LOCKFREE
  /* Is the DB already open? */
  const MDBX_env *const env = txn->env;
  size_t free_slot = env->n_dbi;
  for (size_t i = CORE_DBS; i < env->n_dbi; ++i) {
  retry:
    if ((env->dbs_flags[i] & DB_VALID) == 0) {
      free_slot = i;
      continue;
    }

    const uint32_t snap_seq = atomic_load32(&env->dbi_seqs[i], mo_AcquireRelease);
    const uint16_t snap_flags = env->dbs_flags[i];
    const MDBX_val snap_name = env->kvs[i].name;
    if (user_flags != MDBX_ACCEDE &&
        (((user_flags ^ snap_flags) & DB_PERSISTENT_FLAGS) || (keycmp && keycmp != env->kvs[i].clc.k.cmp) ||
         (datacmp && datacmp != env->kvs[i].clc.v.cmp)))
      continue;
    const uint32_t main_seq = atomic_load32(&env->dbi_seqs[MAIN_DBI], mo_AcquireRelease);
    MDBX_cmp_func *const snap_cmp = env->kvs[MAIN_DBI].clc.k.cmp;
    if (unlikely(!(snap_flags & DB_VALID) || !snap_name.iov_base || !snap_name.iov_len || !snap_cmp))
      continue;

    const bool name_match = snap_cmp(&snap_name, name) == 0;
    osal_flush_incoherent_cpu_writeback();
    if (unlikely(snap_seq != atomic_load32(&env->dbi_seqs[i], mo_AcquireRelease) ||
                 main_seq != atomic_load32(&env->dbi_seqs[MAIN_DBI], mo_AcquireRelease) ||
                 snap_flags != env->dbs_flags[i] || snap_name.iov_base != env->kvs[i].name.iov_base ||
                 snap_name.iov_len != env->kvs[i].name.iov_len))
      goto retry;
    if (name_match) {
      rc = dbi_check(txn, i);
      if (rc == MDBX_BAD_DBI && txn->dbi_state[i] == (DBI_OLDEN | DBI_LINDO)) {
        /* хендл использовался, стал невалидным,
         * но теперь явно пере-открывается в этой транзакци */
        eASSERT(env, !txn->cursors[i]);
        txn->dbi_state[i] = DBI_LINDO;
        rc = dbi_check(txn, i);
      }
      if (likely(rc == MDBX_SUCCESS)) {
        rc = dbi_bind(txn, i, user_flags, keycmp, datacmp);
        if (likely(rc == MDBX_SUCCESS))
          *dbi = (MDBX_dbi)i;
      }
      return rc;
    }
  }

  /* Fail, if no free slot and max hit */
  if (unlikely(free_slot >= env->max_dbi))
    return MDBX_DBS_FULL;
#endif /* MDBX_ENABLE_DBI_LOCKFREE */

  rc = osal_fastmutex_acquire(&txn->env->dbi_lock);
  if (likely(rc == MDBX_SUCCESS)) {
    rc = dbi_open_locked(txn, user_flags, dbi, keycmp, datacmp, *name);
    ENSURE(txn->env, osal_fastmutex_release(&txn->env->dbi_lock) == MDBX_SUCCESS);
  }
  return rc;
}

static int dbi_open_cstr(MDBX_txn *txn, const char *name_cstr, MDBX_db_flags_t flags, MDBX_dbi *dbi,
                         MDBX_cmp_func *keycmp, MDBX_cmp_func *datacmp) {
  MDBX_val thunk, *name;
  if (name_cstr == MDBX_CHK_MAIN || name_cstr == MDBX_CHK_GC || name_cstr == MDBX_CHK_META)
    name = (void *)name_cstr;
  else {
    thunk.iov_len = strlen(name_cstr);
    thunk.iov_base = (void *)name_cstr;
    name = &thunk;
  }
  return dbi_open(txn, name, flags, dbi, keycmp, datacmp);
}

struct dbi_rename_result {
  defer_free_item_t *defer;
  int err;
};

__cold static struct dbi_rename_result dbi_rename_locked(MDBX_txn *txn, MDBX_dbi dbi, MDBX_val new_name) {
  struct dbi_rename_result pair;
  pair.defer = nullptr;
  pair.err = dbi_check(txn, dbi);
  if (unlikely(pair.err != MDBX_SUCCESS))
    return pair;

  MDBX_env *const env = txn->env;
  MDBX_val old_name = env->kvs[dbi].name;
  if (env->kvs[MAIN_DBI].clc.k.cmp(&new_name, &old_name) == 0 && MDBX_DEBUG == 0)
    return pair;

  cursor_couple_t cx;
  pair.err = cursor_init(&cx.outer, txn, MAIN_DBI);
  if (unlikely(pair.err != MDBX_SUCCESS))
    return pair;
  pair.err = cursor_seek(&cx.outer, &new_name, nullptr, MDBX_SET).err;
  if (unlikely(pair.err != MDBX_NOTFOUND)) {
    pair.err = (pair.err == MDBX_SUCCESS) ? MDBX_KEYEXIST : pair.err;
    return pair;
  }

  pair.defer = osal_malloc(dbi_namelen(new_name));
  if (unlikely(!pair.defer)) {
    pair.err = MDBX_ENOMEM;
    return pair;
  }
  new_name.iov_base = memcpy(pair.defer, new_name.iov_base, new_name.iov_len);

  cx.outer.next = txn->cursors[MAIN_DBI];
  txn->cursors[MAIN_DBI] = &cx.outer;

  MDBX_val data = {&txn->dbs[dbi], sizeof(tree_t)};
  pair.err = cursor_put_checklen(&cx.outer, &new_name, &data, N_TREE | MDBX_NOOVERWRITE);
  if (likely(pair.err == MDBX_SUCCESS)) {
    pair.err = cursor_seek(&cx.outer, &old_name, nullptr, MDBX_SET).err;
    if (likely(pair.err == MDBX_SUCCESS))
      pair.err = cursor_del(&cx.outer, N_TREE);
    if (likely(pair.err == MDBX_SUCCESS)) {
      pair.defer = env->kvs[dbi].name.iov_base;
      env->kvs[dbi].name = new_name;
    } else
      txn->flags |= MDBX_TXN_ERROR;
  }

  txn->cursors[MAIN_DBI] = cx.outer.next;
  return pair;
}

static defer_free_item_t *dbi_close_locked(MDBX_env *env, MDBX_dbi dbi) {
  eASSERT(env, dbi >= CORE_DBS);
  if (unlikely(dbi >= env->n_dbi))
    return nullptr;

  const uint32_t seq = dbi_seq_next(env, dbi);
  defer_free_item_t *defer_item = env->kvs[dbi].name.iov_base;
  if (likely(defer_item)) {
    env->dbs_flags[dbi] = 0;
    env->kvs[dbi].name.iov_len = 0;
    env->kvs[dbi].name.iov_base = nullptr;
    atomic_store32(&env->dbi_seqs[dbi], seq, mo_AcquireRelease);
    osal_flush_incoherent_cpu_writeback();
    defer_item->next = nullptr;

    if (env->n_dbi == dbi + 1) {
      size_t i = env->n_dbi;
      do {
        --i;
        eASSERT(env, i >= CORE_DBS);
        eASSERT(env, !env->dbs_flags[i] && !env->kvs[i].name.iov_len && !env->kvs[i].name.iov_base);
      } while (i > CORE_DBS && !env->kvs[i - 1].name.iov_base);
      env->n_dbi = (unsigned)i;
    }
  }

  return defer_item;
}

/*----------------------------------------------------------------------------*/
/* API */

int mdbx_dbi_open(MDBX_txn *txn, const char *name, MDBX_db_flags_t flags, MDBX_dbi *dbi) {
  return LOG_IFERR(dbi_open_cstr(txn, name, flags, dbi, nullptr, nullptr));
}

int mdbx_dbi_open2(MDBX_txn *txn, const MDBX_val *name, MDBX_db_flags_t flags, MDBX_dbi *dbi) {
  return LOG_IFERR(dbi_open(txn, name, flags, dbi, nullptr, nullptr));
}

int mdbx_dbi_open_ex(MDBX_txn *txn, const char *name, MDBX_db_flags_t flags, MDBX_dbi *dbi, MDBX_cmp_func *keycmp,
                     MDBX_cmp_func *datacmp) {
  return LOG_IFERR(dbi_open_cstr(txn, name, flags, dbi, keycmp, datacmp));
}

int mdbx_dbi_open_ex2(MDBX_txn *txn, const MDBX_val *name, MDBX_db_flags_t flags, MDBX_dbi *dbi, MDBX_cmp_func *keycmp,
                      MDBX_cmp_func *datacmp) {
  return LOG_IFERR(dbi_open(txn, name, flags, dbi, keycmp, datacmp));
}

__cold int mdbx_drop(MDBX_txn *txn, MDBX_dbi dbi, bool del) {
  int rc = check_txn_rw(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  cursor_couple_t cx;
  rc = cursor_init(&cx.outer, txn, dbi);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  if (txn->dbs[dbi].height) {
    cx.outer.next = txn->cursors[dbi];
    txn->cursors[dbi] = &cx.outer;
    rc = tree_drop(&cx.outer, dbi == MAIN_DBI || (cx.outer.tree->flags & MDBX_DUPSORT));
    txn->cursors[dbi] = cx.outer.next;
    if (unlikely(rc != MDBX_SUCCESS))
      return LOG_IFERR(rc);
  }

  /* Invalidate the dropped DB's cursors */
  for (MDBX_cursor *mc = txn->cursors[dbi]; mc; mc = mc->next)
    be_poor(mc);

  if (!del || dbi < CORE_DBS) {
    /* reset the DB record, mark it dirty */
    txn->dbi_state[dbi] |= DBI_DIRTY;
    txn->dbs[dbi].height = 0;
    txn->dbs[dbi].branch_pages = 0;
    txn->dbs[dbi].leaf_pages = 0;
    txn->dbs[dbi].large_pages = 0;
    txn->dbs[dbi].items = 0;
    txn->dbs[dbi].root = P_INVALID;
    txn->dbs[dbi].sequence = 0;
    /* txn->dbs[dbi].mod_txnid = txn->txnid; */
    txn->flags |= MDBX_TXN_DIRTY;
    return MDBX_SUCCESS;
  }

  MDBX_env *const env = txn->env;
  MDBX_val name = env->kvs[dbi].name;
  rc = cursor_init(&cx.outer, txn, MAIN_DBI);
  if (likely(rc == MDBX_SUCCESS)) {
    rc = cursor_seek(&cx.outer, &name, nullptr, MDBX_SET).err;
    if (likely(rc == MDBX_SUCCESS)) {
      cx.outer.next = txn->cursors[MAIN_DBI];
      txn->cursors[MAIN_DBI] = &cx.outer;
      rc = cursor_del(&cx.outer, N_TREE);
      txn->cursors[MAIN_DBI] = cx.outer.next;
      if (likely(rc == MDBX_SUCCESS)) {
        tASSERT(txn, txn->dbi_state[MAIN_DBI] & DBI_DIRTY);
        tASSERT(txn, txn->flags & MDBX_TXN_DIRTY);
        txn->dbi_state[dbi] = DBI_LINDO | DBI_OLDEN;
        rc = osal_fastmutex_acquire(&env->dbi_lock);
        if (likely(rc == MDBX_SUCCESS))
          return LOG_IFERR(defer_and_release(env, dbi_close_locked(env, dbi)));
      }
    }
  }
  txn->flags |= MDBX_TXN_ERROR;
  return LOG_IFERR(rc);
}

__cold int mdbx_dbi_rename(MDBX_txn *txn, MDBX_dbi dbi, const char *name_cstr) {
  MDBX_val thunk, *name;
  if (name_cstr == MDBX_CHK_MAIN || name_cstr == MDBX_CHK_GC || name_cstr == MDBX_CHK_META)
    name = (void *)name_cstr;
  else {
    thunk.iov_len = strlen(name_cstr);
    thunk.iov_base = (void *)name_cstr;
    name = &thunk;
  }
  return LOG_IFERR(mdbx_dbi_rename2(txn, dbi, name));
}

int mdbx_dbi_close(MDBX_env *env, MDBX_dbi dbi) {
  int rc = check_env(env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  if (unlikely(dbi < CORE_DBS))
    return (dbi == MAIN_DBI) ? MDBX_SUCCESS : LOG_IFERR(MDBX_BAD_DBI);

  if (unlikely(dbi >= env->max_dbi))
    return LOG_IFERR(MDBX_BAD_DBI);

  if (unlikely(dbi < CORE_DBS || dbi >= env->max_dbi))
    return LOG_IFERR(MDBX_BAD_DBI);

  rc = osal_fastmutex_acquire(&env->dbi_lock);
  if (likely(rc == MDBX_SUCCESS && dbi < env->n_dbi)) {
  retry:
    if (env->basal_txn && (env->dbs_flags[dbi] & DB_VALID) && (env->basal_txn->flags & MDBX_TXN_FINISHED) == 0) {
      /* LY: Опасный код, так как env->txn может быть изменено в другом потоке.
       * К сожалению тут нет надежного решения и может быть падение при неверном
       * использовании API (вызове mdbx_dbi_close конкурентно с завершением
       * пишущей транзакции).
       *
       * Для минимизации вероятности падения сначала проверяем dbi-флаги
       * в basal_txn, а уже после в env->txn. Таким образом, падение может быть
       * только при коллизии с завершением вложенной транзакции.
       *
       * Альтернативно можно попробовать выполнять обновление/put записи в
       * mainDb соответствующей таблице закрываемого хендла. Семантически это
       * верный путь, но проблема в текущем API, в котором исторически dbi-хендл
       * живет и закрывается вне транзакции. Причем проблема не только в том,
       * что нет указателя на текущую пишущую транзакцию, а в том что
       * пользователь точно не ожидает что закрытие хендла приведет к
       * скрытой/непрозрачной активности внутри транзакции потенциально
       * выполняемой в другом потоке. Другими словами, проблема может быть
       * только при неверном использовании API и если пользователь это
       * допускает, то точно не будет ожидать скрытых действий внутри
       * транзакции, и поэтому этот путь потенциально более опасен. */
      const MDBX_txn *const hazard = env->txn;
      osal_compiler_barrier();
      if ((dbi_state(env->basal_txn, dbi) & (DBI_LINDO | DBI_DIRTY | DBI_CREAT)) > DBI_LINDO) {
      bailout_dirty_dbi:
        osal_fastmutex_release(&env->dbi_lock);
        return LOG_IFERR(MDBX_DANGLING_DBI);
      }
      osal_memory_barrier();
      if (unlikely(hazard != env->txn))
        goto retry;
      if (hazard != env->basal_txn && hazard && (hazard->flags & MDBX_TXN_FINISHED) == 0 &&
          hazard->signature == txn_signature &&
          (dbi_state(hazard, dbi) & (DBI_LINDO | DBI_DIRTY | DBI_CREAT)) > DBI_LINDO)
        goto bailout_dirty_dbi;
      osal_compiler_barrier();
      if (unlikely(hazard != env->txn))
        goto retry;
    }
    rc = defer_and_release(env, dbi_close_locked(env, dbi));
  }
  return LOG_IFERR(rc);
}

int mdbx_dbi_flags_ex(const MDBX_txn *txn, MDBX_dbi dbi, unsigned *flags, unsigned *state) {
  if (unlikely(!flags || !state))
    return LOG_IFERR(MDBX_EINVAL);

  int rc = check_txn(txn, MDBX_TXN_BLOCKED - MDBX_TXN_ERROR - MDBX_TXN_PARKED);
  if (unlikely(rc != MDBX_SUCCESS)) {
    *flags = 0;
    *state = 0;
    return LOG_IFERR(rc);
  }

  rc = dbi_check(txn, dbi);
  if (unlikely(rc != MDBX_SUCCESS)) {
    *flags = 0;
    *state = 0;
    return LOG_IFERR(rc);
  }

  *flags = txn->dbs[dbi].flags & DB_PERSISTENT_FLAGS;
  *state = txn->dbi_state[dbi] & (DBI_FRESH | DBI_CREAT | DBI_DIRTY | DBI_STALE);
  return MDBX_SUCCESS;
}

__cold int mdbx_dbi_rename2(MDBX_txn *txn, MDBX_dbi dbi, const MDBX_val *new_name) {
  int rc = check_txn_rw(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  if (unlikely(new_name == MDBX_CHK_MAIN || new_name->iov_base == MDBX_CHK_MAIN || new_name == MDBX_CHK_GC ||
               new_name->iov_base == MDBX_CHK_GC || new_name == MDBX_CHK_META || new_name->iov_base == MDBX_CHK_META))
    return LOG_IFERR(MDBX_EINVAL);

  if (unlikely(dbi < CORE_DBS))
    return LOG_IFERR(MDBX_EINVAL);
  rc = dbi_check(txn, dbi);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  rc = osal_fastmutex_acquire(&txn->env->dbi_lock);
  if (likely(rc == MDBX_SUCCESS)) {
    struct dbi_rename_result pair = dbi_rename_locked(txn, dbi, *new_name);
    if (pair.defer)
      pair.defer->next = nullptr;
    defer_and_release(txn->env, pair.defer);
    rc = pair.err;
  }
  return LOG_IFERR(rc);
}

static void stat_get(const tree_t *db, MDBX_stat *st, size_t bytes) {
  st->ms_depth = db->height;
  st->ms_branch_pages = db->branch_pages;
  st->ms_leaf_pages = db->leaf_pages;
  st->ms_overflow_pages = db->large_pages;
  st->ms_entries = db->items;
  if (likely(bytes >= offsetof(MDBX_stat, ms_mod_txnid) + sizeof(st->ms_mod_txnid)))
    st->ms_mod_txnid = db->mod_txnid;
}

__cold int mdbx_dbi_stat(const MDBX_txn *txn, MDBX_dbi dbi, MDBX_stat *dest, size_t bytes) {
  if (unlikely(!dest))
    return LOG_IFERR(MDBX_EINVAL);

  int rc = check_txn(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout;

  rc = dbi_check(txn, dbi);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout;

  const size_t size_before_modtxnid = offsetof(MDBX_stat, ms_mod_txnid);
  if (unlikely(bytes != sizeof(MDBX_stat)) && bytes != size_before_modtxnid) {
    rc = MDBX_EINVAL;
    goto bailout;
  }

  if (unlikely(txn->flags & MDBX_TXN_BLOCKED)) {
    rc = MDBX_BAD_TXN;
    goto bailout;
  }

  if (unlikely(txn->dbi_state[dbi] & DBI_STALE)) {
    rc = tbl_fetch((MDBX_txn *)txn, dbi);
    if (unlikely(rc != MDBX_SUCCESS))
      goto bailout;
  }

  dest->ms_psize = txn->env->ps;
  stat_get(&txn->dbs[dbi], dest, bytes);
  return MDBX_SUCCESS;

bailout:
  memset(dest, 0, bytes);
  return LOG_IFERR(rc);
}

__cold const tree_t *dbi_dig(const MDBX_txn *txn, const size_t dbi, tree_t *fallback) {
  const MDBX_txn *dig = txn;
  do {
    tASSERT(txn, txn->n_dbi == dig->n_dbi);
    const uint8_t state = dbi_state(dig, dbi);
    if (state & DBI_LINDO)
      switch (state & (DBI_VALID | DBI_STALE | DBI_OLDEN)) {
      case DBI_VALID:
      case DBI_OLDEN:
        return dig->dbs + dbi;
      case 0:
        return nullptr;
      case DBI_VALID | DBI_STALE:
      case DBI_OLDEN | DBI_STALE:
        break;
      default:
        tASSERT(txn, !!"unexpected dig->dbi_state[dbi]");
      }
    dig = dig->parent;
  } while (dig);
  return fallback;
}

__cold int mdbx_enumerate_tables(const MDBX_txn *txn, MDBX_table_enum_func *func, void *ctx) {
  if (unlikely(!func))
    return LOG_IFERR(MDBX_EINVAL);

  int rc = check_txn(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  cursor_couple_t cx;
  rc = cursor_init(&cx.outer, txn, MAIN_DBI);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  cx.outer.next = txn->cursors[MAIN_DBI];
  txn->cursors[MAIN_DBI] = &cx.outer;
  for (rc = outer_first(&cx.outer, nullptr, nullptr); rc == MDBX_SUCCESS;
       rc = outer_next(&cx.outer, nullptr, nullptr, MDBX_NEXT_NODUP)) {
    node_t *node = page_node(cx.outer.pg[cx.outer.top], cx.outer.ki[cx.outer.top]);
    if (node_flags(node) != N_TREE)
      continue;
    if (unlikely(node_ds(node) != sizeof(tree_t))) {
      ERROR("%s/%d: %s %u", "MDBX_CORRUPTED", MDBX_CORRUPTED, "invalid dupsort sub-tree node size",
            (unsigned)node_ds(node));
      rc = MDBX_CORRUPTED;
      break;
    }

    tree_t reside;
    const tree_t *tree = memcpy(&reside, node_data(node), sizeof(reside));
    const MDBX_val name = {node_key(node), node_ks(node)};
    const MDBX_env *const env = txn->env;
    MDBX_dbi dbi = 0;
    for (size_t i = CORE_DBS; i < env->n_dbi; ++i) {
      if (i >= txn->n_dbi || !(env->dbs_flags[i] & DB_VALID))
        continue;
      if (env->kvs[MAIN_DBI].clc.k.cmp(&name, &env->kvs[i].name))
        continue;

      tree = dbi_dig(txn, i, &reside);
      dbi = (MDBX_dbi)i;
      break;
    }

    MDBX_stat stat;
    stat_get(tree, &stat, sizeof(stat));
    rc = func(ctx, txn, &name, tree->flags, &stat, dbi);
    if (rc != MDBX_SUCCESS)
      goto bailout;
  }
  rc = (rc == MDBX_NOTFOUND) ? MDBX_SUCCESS : rc;

bailout:
  txn->cursors[MAIN_DBI] = cx.outer.next;
  return LOG_IFERR(rc);
}
