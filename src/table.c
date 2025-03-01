/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025

#include "internals.h"

int tbl_setup(const MDBX_env *env, kvx_t *const kvx, const tree_t *const db) {
  if (unlikely(!check_table_flags(db->flags))) {
    ERROR("incompatible or invalid db.flags (0x%x) ", db->flags);
    return MDBX_INCOMPATIBLE;
  }
  if (unlikely(!kvx->clc.k.cmp)) {
    kvx->clc.k.cmp = builtin_keycmp(db->flags);
    kvx->clc.v.cmp = builtin_datacmp(db->flags);
  }

  kvx->clc.k.lmin = keysize_min(db->flags);
  kvx->clc.k.lmax = env_keysize_max(env, db->flags);
  kvx->clc.v.lmin = valsize_min(db->flags);
  kvx->clc.v.lmax = env_valsize_max(env, db->flags);

  if ((db->flags & (MDBX_DUPFIXED | MDBX_INTEGERDUP)) != 0 && db->dupfix_size) {
    if (!MDBX_DISABLE_VALIDATION && unlikely(db->dupfix_size < kvx->clc.v.lmin || db->dupfix_size > kvx->clc.v.lmax)) {
      ERROR("db.dupfix_size (%u) <> min/max value-length (%zu/%zu)", db->dupfix_size, kvx->clc.v.lmin, kvx->clc.v.lmax);
      return MDBX_CORRUPTED;
    }
    kvx->clc.v.lmin = kvx->clc.v.lmax = db->dupfix_size;
  }
  return MDBX_SUCCESS;
}

int tbl_fetch(MDBX_txn *txn, size_t dbi) {
  cursor_couple_t couple;
  int rc = cursor_init(&couple.outer, txn, MAIN_DBI);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  kvx_t *const kvx = &txn->env->kvs[dbi];
  rc = tree_search(&couple.outer, &kvx->name, 0);
  if (unlikely(rc != MDBX_SUCCESS)) {
  bailout:
    NOTICE("dbi %zu refs to inaccessible table `%.*s` for txn %" PRIaTXN " (err %d)", dbi, (int)kvx->name.iov_len,
           (const char *)kvx->name.iov_base, txn->txnid, rc);
    return (rc == MDBX_NOTFOUND) ? MDBX_BAD_DBI : rc;
  }

  MDBX_val data;
  struct node_search_result nsr = node_search(&couple.outer, &kvx->name);
  if (unlikely(!nsr.exact)) {
    rc = MDBX_NOTFOUND;
    goto bailout;
  }
  if (unlikely((node_flags(nsr.node) & (N_DUP | N_TREE)) != N_TREE)) {
    NOTICE("dbi %zu refs to not a named table `%.*s` for txn %" PRIaTXN " (%s)", dbi, (int)kvx->name.iov_len,
           (const char *)kvx->name.iov_base, txn->txnid, "wrong flags");
    return MDBX_INCOMPATIBLE; /* not a named DB */
  }

  rc = node_read(&couple.outer, nsr.node, &data, couple.outer.pg[couple.outer.top]);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(data.iov_len != sizeof(tree_t))) {
    NOTICE("dbi %zu refs to not a named table `%.*s` for txn %" PRIaTXN " (%s)", dbi, (int)kvx->name.iov_len,
           (const char *)kvx->name.iov_base, txn->txnid, "wrong rec-size");
    return MDBX_INCOMPATIBLE; /* not a named DB */
  }

  uint16_t flags = UNALIGNED_PEEK_16(data.iov_base, tree_t, flags);
  /* The txn may not know this DBI, or another process may
   * have dropped and recreated the DB with other flags. */
  tree_t *const db = &txn->dbs[dbi];
  if (unlikely((db->flags & DB_PERSISTENT_FLAGS) != flags)) {
    NOTICE("dbi %zu refs to the re-created table `%.*s` for txn %" PRIaTXN
           " with different flags (present 0x%X != wanna 0x%X)",
           dbi, (int)kvx->name.iov_len, (const char *)kvx->name.iov_base, txn->txnid, db->flags & DB_PERSISTENT_FLAGS,
           flags);
    return MDBX_INCOMPATIBLE;
  }

  memcpy(db, data.iov_base, sizeof(tree_t));
#if !MDBX_DISABLE_VALIDATION
  const txnid_t pp_txnid = couple.outer.pg[couple.outer.top]->txnid;
  tASSERT(txn, txn->front_txnid >= pp_txnid);
  if (unlikely(db->mod_txnid > pp_txnid)) {
    ERROR("db.mod_txnid (%" PRIaTXN ") > page-txnid (%" PRIaTXN ")", db->mod_txnid, pp_txnid);
    return MDBX_CORRUPTED;
  }
#endif /* !MDBX_DISABLE_VALIDATION */
  rc = tbl_setup(txn->env, kvx, db);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  txn->dbi_state[dbi] &= ~DBI_STALE;
  return MDBX_SUCCESS;
}
