/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024

#include "internals.h"

__cold static tree_t *audit_db_dig(const MDBX_txn *txn, const size_t dbi,
                                   tree_t *fallback) {
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

static size_t audit_db_used(const tree_t *db) {
  return db ? (size_t)db->branch_pages + (size_t)db->leaf_pages +
                  (size_t)db->large_pages
            : 0;
}

__cold static int audit_ex_locked(MDBX_txn *txn, size_t retired_stored,
                                  bool dont_filter_gc) {
  const MDBX_env *const env = txn->env;
  size_t pending = 0;
  if ((txn->flags & MDBX_TXN_RDONLY) == 0)
    pending = txn->tw.loose_count + MDBX_PNL_GETSIZE(txn->tw.relist) +
              (MDBX_PNL_GETSIZE(txn->tw.retired_pages) - retired_stored);

  cursor_couple_t cx;
  int rc = cursor_init(&cx.outer, txn, FREE_DBI);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  size_t gc = 0;
  MDBX_val key, data;
  rc = outer_first(&cx.outer, &key, &data);
  while (rc == MDBX_SUCCESS) {
    if (!dont_filter_gc) {
      if (unlikely(key.iov_len != sizeof(txnid_t))) {
        ERROR("%s/%d: %s %u", "MDBX_CORRUPTED", MDBX_CORRUPTED,
              "invalid GC-key size", (unsigned)key.iov_len);
        return MDBX_CORRUPTED;
      }
      txnid_t id = unaligned_peek_u64(4, key.iov_base);
      if (txn->tw.gc.reclaimed) {
        for (size_t i = 1; i <= MDBX_PNL_GETSIZE(txn->tw.gc.reclaimed); ++i)
          if (id == txn->tw.gc.reclaimed[i])
            goto skip;
      } else if (id <= txn->tw.gc.last_reclaimed)
        goto skip;
    }
    gc += *(pgno_t *)data.iov_base;
  skip:
    rc = outer_next(&cx.outer, &key, &data, MDBX_NEXT);
  }
  tASSERT(txn, rc == MDBX_NOTFOUND);

  const size_t done_bitmap_size = (txn->n_dbi + CHAR_BIT - 1) / CHAR_BIT;
  uint8_t *const done_bitmap = alloca(done_bitmap_size);
  memset(done_bitmap, 0, done_bitmap_size);
  if (txn->parent) {
    tASSERT(txn, txn->n_dbi == txn->parent->n_dbi &&
                     txn->n_dbi == txn->env->txn->n_dbi);
#if MDBX_ENABLE_DBI_SPARSE
    tASSERT(txn, txn->dbi_sparse == txn->parent->dbi_sparse &&
                     txn->dbi_sparse == txn->env->txn->dbi_sparse);
#endif /* MDBX_ENABLE_DBI_SPARSE */
  }

  size_t used = NUM_METAS +
                audit_db_used(audit_db_dig(txn, FREE_DBI, nullptr)) +
                audit_db_used(audit_db_dig(txn, MAIN_DBI, nullptr));
  rc = cursor_init(&cx.outer, txn, MAIN_DBI);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  rc = tree_search(&cx.outer, nullptr, Z_FIRST);
  while (rc == MDBX_SUCCESS) {
    page_t *mp = cx.outer.pg[cx.outer.top];
    for (size_t k = 0; k < page_numkeys(mp); k++) {
      node_t *node = page_node(mp, k);
      if (node_flags(node) != N_SUBDATA)
        continue;
      if (unlikely(node_ds(node) != sizeof(tree_t))) {
        ERROR("%s/%d: %s %u", "MDBX_CORRUPTED", MDBX_CORRUPTED,
              "invalid dupsort sub-tree node size", (unsigned)node_ds(node));
        return MDBX_CORRUPTED;
      }

      tree_t reside;
      const tree_t *db = memcpy(&reside, node_data(node), sizeof(reside));
      const MDBX_val name = {node_key(node), node_ks(node)};
      for (size_t dbi = CORE_DBS; dbi < env->n_dbi; ++dbi) {
        if (dbi >= txn->n_dbi || !(env->dbs_flags[dbi] & DB_VALID))
          continue;
        if (env->kvs[MAIN_DBI].clc.k.cmp(&name, &env->kvs[dbi].name))
          continue;

        done_bitmap[dbi / CHAR_BIT] |= 1 << dbi % CHAR_BIT;
        db = audit_db_dig(txn, dbi, &reside);
        break;
      }
      used += audit_db_used(db);
    }
    rc = cursor_sibling_right(&cx.outer);
  }
  tASSERT(txn, rc == MDBX_NOTFOUND);

  for (size_t dbi = CORE_DBS; dbi < txn->n_dbi; ++dbi) {
    if (done_bitmap[dbi / CHAR_BIT] & (1 << dbi % CHAR_BIT))
      continue;
    const tree_t *db = audit_db_dig(txn, dbi, nullptr);
    if (db)
      used += audit_db_used(db);
    else if (dbi_state(txn, dbi))
      WARNING("audit %s@%" PRIaTXN
              ": unable account dbi %zd / \"%*s\", state 0x%02x",
              txn->parent ? "nested-" : "", txn->txnid, dbi,
              (int)env->kvs[dbi].name.iov_len,
              (const char *)env->kvs[dbi].name.iov_base, dbi_state(txn, dbi));
  }

  if (pending + gc + used == txn->geo.first_unallocated)
    return MDBX_SUCCESS;

  if ((txn->flags & MDBX_TXN_RDONLY) == 0)
    ERROR("audit @%" PRIaTXN ": %zu(pending) = %zu(loose) + "
          "%zu(reclaimed) + %zu(retired-pending) - %zu(retired-stored)",
          txn->txnid, pending, txn->tw.loose_count,
          MDBX_PNL_GETSIZE(txn->tw.relist),
          txn->tw.retired_pages ? MDBX_PNL_GETSIZE(txn->tw.retired_pages) : 0,
          retired_stored);
  ERROR("audit @%" PRIaTXN ": %zu(pending) + %zu"
        "(gc) + %zu(count) = %zu(total) <> %zu"
        "(allocated)",
        txn->txnid, pending, gc, used, pending + gc + used,
        (size_t)txn->geo.first_unallocated);
  return MDBX_PROBLEM;
}

__cold int audit_ex(MDBX_txn *txn, size_t retired_stored, bool dont_filter_gc) {
  MDBX_env *const env = txn->env;
  int rc = osal_fastmutex_acquire(&env->dbi_lock);
  if (likely(rc == MDBX_SUCCESS)) {
    rc = audit_ex_locked(txn, retired_stored, dont_filter_gc);
    ENSURE(txn->env, osal_fastmutex_release(&env->dbi_lock) == MDBX_SUCCESS);
  }
  return rc;
}
