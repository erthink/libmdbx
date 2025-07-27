/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025

#include "internals.h"

int mdbx_dbi_open2(MDBX_txn *txn, const MDBX_val *name, MDBX_db_flags_t flags, MDBX_dbi *dbi) {
  return LOG_IFERR(dbi_open(txn, name, flags, dbi, nullptr, nullptr));
}

int mdbx_dbi_open_ex2(MDBX_txn *txn, const MDBX_val *name, MDBX_db_flags_t flags, MDBX_dbi *dbi, MDBX_cmp_func *keycmp,
                      MDBX_cmp_func *datacmp) {
  return LOG_IFERR(dbi_open(txn, name, flags, dbi, keycmp, datacmp));
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

int mdbx_dbi_open(MDBX_txn *txn, const char *name, MDBX_db_flags_t flags, MDBX_dbi *dbi) {
  return LOG_IFERR(dbi_open_cstr(txn, name, flags, dbi, nullptr, nullptr));
}

int mdbx_dbi_open_ex(MDBX_txn *txn, const char *name, MDBX_db_flags_t flags, MDBX_dbi *dbi, MDBX_cmp_func *keycmp,
                     MDBX_cmp_func *datacmp) {
  return LOG_IFERR(dbi_open_cstr(txn, name, flags, dbi, keycmp, datacmp));
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
          return LOG_IFERR(dbi_close_release(env, dbi));
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
  return mdbx_dbi_rename2(txn, dbi, name);
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
    dbi_defer_release(txn->env, pair.defer);
    rc = pair.err;
  }
  return LOG_IFERR(rc);
}

int mdbx_dbi_close(MDBX_env *env, MDBX_dbi dbi) {
  int rc = check_env(env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  if (unlikely(dbi < CORE_DBS))
    return (dbi == MAIN_DBI) ? MDBX_SUCCESS : LOG_IFERR(MDBX_BAD_DBI);

  if (unlikely(dbi >= env->n_dbi))
    return LOG_IFERR(MDBX_BAD_DBI);

  rc = osal_fastmutex_acquire(&env->dbi_lock);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  if (unlikely(dbi >= env->n_dbi)) {
    rc = MDBX_BAD_DBI;
  bailout:
    osal_fastmutex_release(&env->dbi_lock);
    return LOG_IFERR(rc);
  }

  while (env->basal_txn && (env->dbs_flags[dbi] & DB_VALID) && (env->basal_txn->flags & MDBX_TXN_FINISHED) == 0) {
    /* LY: Опасный код, так как env->txn может быть изменено в другом потоке.
     * К сожалению тут нет надежного решения и может быть падение при неверном
     * использовании API (вызове mdbx_dbi_close конкурентно с завершением
     * пишущей транзакции).
     *
     * Для минимизации вероятности падения сначала проверяем dbi-флаги
     * в basal_txn, а уже после в env->txn. Таким образом, падение может быть
     * только при коллизии с завершением вложенной транзакции.
     *
     * Альтернативно можно попробовать выполнять обновление/put строки в
     * MainDB соответствующей таблице закрываемого хендла. Семантически это
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
      rc = MDBX_DANGLING_DBI;
      goto bailout;
    }
    osal_memory_barrier();
    if (unlikely(hazard != env->txn))
      continue;
    if (hazard != env->basal_txn && hazard && (hazard->flags & MDBX_TXN_FINISHED) == 0 &&
        hazard->signature == txn_signature &&
        (dbi_state(hazard, dbi) & (DBI_LINDO | DBI_DIRTY | DBI_CREAT)) > DBI_LINDO) {
      rc = MDBX_DANGLING_DBI;
      goto bailout;
    }
    osal_compiler_barrier();
    if (likely(hazard == env->txn))
      break;
  }
  rc = dbi_close_release(env, dbi);
  return LOG_IFERR(rc);
}

int mdbx_dbi_flags_ex(const MDBX_txn *txn, MDBX_dbi dbi, unsigned *flags, unsigned *state) {
  int rc = check_txn(txn, MDBX_TXN_BLOCKED - MDBX_TXN_ERROR - MDBX_TXN_PARKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  rc = dbi_check(txn, dbi);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  if (unlikely(!flags || !state))
    return LOG_IFERR(MDBX_EINVAL);

  *flags = txn->dbs[dbi].flags & DB_PERSISTENT_FLAGS;
  *state = txn->dbi_state[dbi] & (DBI_FRESH | DBI_CREAT | DBI_DIRTY | DBI_STALE);
  return MDBX_SUCCESS;
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
  int rc = check_txn(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  rc = dbi_check(txn, dbi);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  if (unlikely(txn->flags & MDBX_TXN_BLOCKED))
    return LOG_IFERR(MDBX_BAD_TXN);

  if (unlikely(txn->dbi_state[dbi] & DBI_STALE)) {
    rc = tbl_fetch((MDBX_txn *)txn, dbi);
    if (unlikely(rc != MDBX_SUCCESS))
      return LOG_IFERR(rc);
  }

  if (unlikely(!dest))
    return LOG_IFERR(MDBX_EINVAL);

  const size_t size_before_modtxnid = offsetof(MDBX_stat, ms_mod_txnid);
  if (unlikely(bytes != sizeof(MDBX_stat)) && bytes != size_before_modtxnid)
    return LOG_IFERR(MDBX_EINVAL);

  dest->ms_psize = txn->env->ps;
  stat_get(&txn->dbs[dbi], dest, bytes);
  return MDBX_SUCCESS;
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
