/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025

#include "internals.h"

struct audit_ctx {
  size_t used;
  uint8_t *const done_bitmap;
};

static int audit_dbi(void *ctx, const MDBX_txn *txn, const MDBX_val *name, MDBX_db_flags_t flags,
                     const struct MDBX_stat *stat, MDBX_dbi dbi) {
  struct audit_ctx *audit_ctx = ctx;
  (void)name;
  (void)txn;
  (void)flags;
  audit_ctx->used += (size_t)stat->ms_branch_pages + (size_t)stat->ms_leaf_pages + (size_t)stat->ms_overflow_pages;
  if (dbi)
    audit_ctx->done_bitmap[dbi / CHAR_BIT] |= 1 << dbi % CHAR_BIT;
  return MDBX_SUCCESS;
}

static size_t audit_db_used(const tree_t *db) {
  return db ? (size_t)db->branch_pages + (size_t)db->leaf_pages + (size_t)db->large_pages : 0;
}

__cold static int audit_ex_locked(MDBX_txn *txn, const size_t retired_stored, const bool dont_filter_gc) {
  const MDBX_env *const env = txn->env;
  tASSERT(txn, (txn->flags & MDBX_TXN_RDONLY) == 0);
  const size_t pending =
      txn->wr.loose_count + pnl_size(txn->wr.repnl) + (pnl_size(txn->wr.retired_pages) - retired_stored);

  cursor_couple_t cx;
  int rc = cursor_init(&cx.outer, txn, FREE_DBI);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  size_t gc = 0;
  MDBX_val key, data;
  rc = outer_first(&cx.outer, &key, &data);
  while (rc == MDBX_SUCCESS) {
    if (unlikely(key.iov_len != sizeof(txnid_t))) {
      ERROR("%s/%d: %s %u", "MDBX_CORRUPTED", MDBX_CORRUPTED, "invalid GC-key size", (unsigned)key.iov_len);
      return MDBX_CORRUPTED;
    }
    const txnid_t id = unaligned_peek_u64(4, key.iov_base);
    const size_t len = *(pgno_t *)data.iov_base;
    const bool acc = dont_filter_gc || !gc_is_reclaimed(txn, id);
    TRACE("%s id %" PRIaTXN " len %zu", acc ? "acc" : "skip", id, len);
    if (acc)
      gc += len;
    rc = outer_next(&cx.outer, &key, &data, MDBX_NEXT);
  }
  tASSERT(txn, rc == MDBX_NOTFOUND);

  const size_t done_bitmap_size = (txn->n_dbi + CHAR_BIT - 1) / CHAR_BIT;
  if (txn->parent) {
    tASSERT(txn, txn->n_dbi == txn->parent->n_dbi && txn->n_dbi == txn->env->txn->n_dbi);
#if MDBX_ENABLE_DBI_SPARSE
    tASSERT(txn, txn->dbi_sparse == txn->parent->dbi_sparse && txn->dbi_sparse == txn->env->txn->dbi_sparse);
#endif /* MDBX_ENABLE_DBI_SPARSE */
  }

  struct audit_ctx ctx = {0, alloca(done_bitmap_size)};
  memset(ctx.done_bitmap, 0, done_bitmap_size);
  ctx.used =
      NUM_METAS + audit_db_used(dbi_dig(txn, FREE_DBI, nullptr)) + audit_db_used(dbi_dig(txn, MAIN_DBI, nullptr));

  rc = mdbx_enumerate_tables(txn, audit_dbi, &ctx);
  tASSERT(txn, rc == MDBX_SUCCESS);

  for (size_t dbi = CORE_DBS; dbi < txn->n_dbi; ++dbi) {
    if (ctx.done_bitmap[dbi / CHAR_BIT] & (1 << dbi % CHAR_BIT))
      continue;
    const tree_t *db = dbi_dig(txn, dbi, nullptr);
    if (db)
      ctx.used += audit_db_used(db);
    else if (dbi_state(txn, dbi))
      WARNING("audit %s@%" PRIaTXN ": unable account dbi %zd / \"%.*s\", state 0x%02x", txn->parent ? "nested-" : "",
              txn->txnid, dbi, (int)env->kvs[dbi].name.iov_len, (const char *)env->kvs[dbi].name.iov_base,
              dbi_state(txn, dbi));
  }

  if (pending + gc + ctx.used == txn->geo.first_unallocated)
    return MDBX_SUCCESS;

  if ((txn->flags & MDBX_TXN_RDONLY) == 0)
    ERROR("audit @%" PRIaTXN ": %zu(pending) = %zu(loose) + "
          "%zu(reclaimed) + %zu(retired-pending) - %zu(retired-stored)",
          txn->txnid, pending, txn->wr.loose_count, pnl_size(txn->wr.repnl),
          txn->wr.retired_pages ? pnl_size(txn->wr.retired_pages) : 0, retired_stored);
  ERROR("audit @%" PRIaTXN ": %zu(pending) + %zu"
        "(gc) + %zu(count) = %zu(total) <> %zu"
        "(allocated)",
        txn->txnid, pending, gc, ctx.used, pending + gc + ctx.used, (size_t)txn->geo.first_unallocated);
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
