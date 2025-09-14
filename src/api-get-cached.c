/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2025

#include "internals.h"

static MDBX_cache_result_t cache_get(const MDBX_txn *txn, MDBX_dbi dbi, const MDBX_val *key, MDBX_val *data,
                                     MDBX_cache_entry *entry);

static inline bool is_outside_dxb(const MDBX_txn *txn, const void *ptr) {
  const MDBX_env *env = txn->env;
  const ptrdiff_t offset = ptr_dist(ptr, env->dxb_mmap.base);
  return offset < 0 || (size_t)offset >= pgno2bytes(env, txn->geo.first_unallocated);
}

static inline bool is_not_commited(const MDBX_txn *txn, const page_t *mp) {
  tASSERT(txn, mp >= (const page_t *)txn->env->dxb_mmap.base &&
                   mp < (const page_t *)(ptr_disp(txn->env->dxb_mmap.base,
                                                  pgno2bytes(txn->env, txn->geo.first_unallocated))));
  return mp->txnid >= txn_basis_snapshot(txn);
}

MDBX_MAYBE_UNUSED static inline bool is_inside_dxb_and_commited(const MDBX_txn *txn, const void *ptr) {
  return !is_outside_dxb(txn, ptr) && !is_not_commited(txn, ptr2page(txn->env, ptr));
}

static inline MDBX_cache_result_t cache_result(int err, MDBX_cache_status_t status) {
  MDBX_cache_result_t result = {.errcode = err, .status = status};
  return result;
}

static inline MDBX_cache_result_t cache_error(int err) {
  assert(err != MDBX_SUCCESS && err != MDBX_RESULT_TRUE);
  return cache_result(err, MDBX_CACHE_ERROR);
}

static inline MDBX_cache_result_t cache_fallback(const MDBX_txn *txn, MDBX_dbi dbi, const MDBX_val *key, MDBX_val *data,
                                                 MDBX_cache_status_t status) {
  MDBX_cache_entry stub = {.last_confirmed_txnid = 0, .trunk_txnid = 0};
  MDBX_cache_result_t result = cache_get(txn, dbi, key, data, &stub);
  if (result.status > MDBX_CACHE_DIRTY)
    result.status = status;
  return result;
}

__hot static MDBX_cache_result_t cache_get(const MDBX_txn *txn, MDBX_dbi dbi, const MDBX_val *key, MDBX_val *data,
                                           MDBX_cache_entry *entry) {
  DKBUF_DEBUG;
  DEBUG("===> cached-get dbi %u, key [%s], entry %p (trunk %" PRIaTXN ", last_confirmed %" PRIaTXN
        ", data offset %zu, len %u)",
        dbi, DKEY_DEBUG(key), entry, entry->trunk_txnid, entry->last_confirmed_txnid, entry->offset, entry->length);

  if (unlikely(entry->trunk_txnid > entry->last_confirmed_txnid))
    return cache_error(LOG_IFERR(MDBX_INVALID));

  int err = check_txn(txn, MDBX_TXN_BLOCKED);
  if (unlikely(err != MDBX_SUCCESS))
    return cache_error(LOG_IFERR(err));

  tASSERT(txn, entry->offset || entry->length == 0);
  tASSERT(txn, entry->last_confirmed_txnid <= MAX_TXNID);
  if (unlikely(txn->txnid < entry->trunk_txnid))
    /* the used/read MVCC-snapshot is behind the cached MVCC-range */
    return cache_fallback(txn, dbi, key, data, MDBX_CACHE_BEHIND);

  if (likely(txn->txnid <= entry->last_confirmed_txnid)) {
    /* cache hit fast-path */
    data->iov_base = entry->offset ? ptr_disp(txn->env->dxb_mmap.base, entry->offset) : 0;
    data->iov_len = entry->length;
    tASSERT(txn, (!entry->offset && !entry->length) || is_inside_dxb_and_commited(txn, data->iov_base));
    return cache_result(data->iov_base ? MDBX_SUCCESS : MDBX_NOTFOUND, MDBX_CACHE_HIT);
  }

  err = dbi_check(txn, dbi);
  if (unlikely(err != MDBX_SUCCESS))
    return cache_error(LOG_IFERR(err));

  const uint64_t committed_snapshot_txnid = txn_basis_snapshot(txn);
  txnid_t trunk_txnid = txn->front_txnid;
  if (unlikely(txn->dbi_state[dbi] & DBI_STALE)) {
    err = tbl_fetch((MDBX_txn *)txn, dbi);
    if (unlikely(err != MDBX_SUCCESS)) {
      if (err == MDBX_NOTFOUND) {
        /* the corresponding table has been deleted */
      not_found:
        data->iov_base = nullptr;
        data->iov_len = 0;
        MDBX_cache_status_t status = MDBX_CACHE_DIRTY;
        if (trunk_txnid <= committed_snapshot_txnid) {
          status = MDBX_CACHE_CONFIRMED;
          if (entry->offset) {
            status = MDBX_CACHE_REFRESHED;
            tASSERT(txn, trunk_txnid > entry->trunk_txnid);
            entry->offset = 0;
            entry->length = 0;
            entry->trunk_txnid = trunk_txnid;
          }
          entry->last_confirmed_txnid = committed_snapshot_txnid;
        }
        return cache_result(err, status);
      }
      return cache_error(LOG_IFERR(err));
    }
  }

  if (txn->dbs[dbi].mod_txnid /* tree->mod_txnid maybe zero in a legacy DB */)
    trunk_txnid = txn->dbs[dbi].mod_txnid;
  if ((txn->flags & MDBX_TXN_RDONLY) == 0) {
    const MDBX_txn *scan = txn;
    do
      if ((scan->flags & MDBX_TXN_DIRTY) && (dbi == MAIN_DBI || (scan->dbi_state[dbi] & DBI_DIRTY))) {
        /* После коммита вложенных тразакций может быть mod_txnid > front */
        trunk_txnid = scan->front_txnid;
        break;
      }
    while (unlikely((scan = scan->parent) != nullptr));
  }

  if (trunk_txnid <= entry->last_confirmed_txnid) {
    tASSERT(txn, (txn->dbi_state[dbi] & DBI_DIRTY) == 0);
  cache_confirmed:
    tASSERT(txn, trunk_txnid < committed_snapshot_txnid && trunk_txnid <= entry->last_confirmed_txnid);
    tASSERT(txn, trunk_txnid == entry->trunk_txnid);
    data->iov_base = entry->offset ? ptr_disp(txn->env->dxb_mmap.base, entry->offset) : 0;
    data->iov_len = entry->length;
    tASSERT(txn, (!entry->offset && !entry->length) || is_inside_dxb_and_commited(txn, data->iov_base));
    entry->last_confirmed_txnid = committed_snapshot_txnid;
    return cache_result(data->iov_base ? MDBX_SUCCESS : MDBX_NOTFOUND, MDBX_CACHE_CONFIRMED);
  }

  if (unlikely(txn->dbs[dbi].root == P_INVALID)) {
    /* the corresponding table is empty now */
    goto not_found;
  }

  cursor_couple_t cx;
  err = cursor_init(&cx.outer, txn, dbi);
  if (unlikely(err != MDBX_SUCCESS))
    return cache_error(LOG_IFERR(err));

  alignkey_t aligned;
  err = check_key(&cx.outer, key, &aligned);
  if (unlikely(err != MDBX_SUCCESS))
    return cache_error(LOG_IFERR(err));

  cx.outer.top = 0;
  cx.outer.ki[0] = 0;
  err = page_get(&cx.outer, txn->dbs[dbi].root, &cx.outer.pg[0], trunk_txnid);
  if (unlikely(err != MDBX_SUCCESS))
    return cache_error(LOG_IFERR(err));

  page_t *mp = cx.outer.pg[0];
  if ((trunk_txnid = mp->txnid) <= entry->last_confirmed_txnid)
    goto cache_confirmed;

  intptr_t ki = page_numkeys(mp) - 1;
  while (is_branch(mp)) {
    const struct node_search_result nsr = node_search(&cx.outer, key);
    if (likely(nsr.node))
      ki = cx.outer.ki[cx.outer.top] + (intptr_t)nsr.exact - 1;
    err = page_get(&cx.outer, node_pgno(page_node(mp, ki)), &mp, trunk_txnid);
    if (unlikely(err != MDBX_SUCCESS))
      return cache_error(LOG_IFERR(err));

    if ((trunk_txnid = mp->txnid) <= entry->last_confirmed_txnid)
      goto cache_confirmed;

    ki = page_numkeys(mp) - 1;
    err = cursor_push(&cx.outer, mp, ki);
    if (unlikely(err != MDBX_SUCCESS))
      return cache_error(LOG_IFERR(err));
  }

  if (!MDBX_DISABLE_VALIDATION && unlikely(!check_leaf_type(&cx.outer, mp))) {
    ERROR("unexpected leaf-page #%" PRIaPGNO " type 0x%x seen by cursor", mp->pgno, mp->flags);
    err = MDBX_CORRUPTED;
    return cache_error(LOG_IFERR(err));
  }

  struct node_search_result nsr = node_search(&cx.outer, &aligned.key);
  if (!nsr.exact)
    goto not_found;

  if (unlikely(node_flags(nsr.node) & N_DUP)) {
    /* TODO: It is possible to implement support, but need to think through the usage scenarios */
    err = MDBX_EMULTIVAL;
    return cache_error(LOG_IFERR(err));
  }

  err = node_read(&cx.outer, nsr.node, data, mp);
  if (unlikely(err != MDBX_SUCCESS))
    return cache_error(LOG_IFERR(err));

  if (trunk_txnid > committed_snapshot_txnid) {
    tASSERT(txn, trunk_txnid > entry->last_confirmed_txnid && trunk_txnid > entry->trunk_txnid);
    return cache_result(MDBX_SUCCESS, MDBX_CACHE_DIRTY);
  }

  tASSERT(txn, is_inside_dxb_and_commited(txn, data->iov_base));
  tASSERT(txn, trunk_txnid <= committed_snapshot_txnid && trunk_txnid > entry->last_confirmed_txnid &&
                   trunk_txnid > entry->trunk_txnid);
  entry->offset = ptr_dist(data->iov_base, txn->env->dxb_mmap.base);
  entry->length = (uint32_t)data->iov_len;
  entry->trunk_txnid = trunk_txnid;
  entry->last_confirmed_txnid = committed_snapshot_txnid;
  return cache_result(MDBX_SUCCESS, MDBX_CACHE_REFRESHED);
}

/*----------------------------------------------------------------------------*/

__hot MDBX_cache_result_t mdbx_cache_get(const MDBX_txn *txn, MDBX_dbi dbi, const MDBX_val *key, MDBX_val *data,
                                         volatile MDBX_cache_entry *entry) {

  if (unlikely(!key || !data || !entry))
    return cache_error(LOG_IFERR(MDBX_EINVAL));

  MDBX_cache_entry local = *entry;
  while (true) {
    MDBX_cache_entry again;
    again.last_confirmed_txnid = safe64_read((mdbx_atomic_uint64_t *)&entry->last_confirmed_txnid);
    if (unlikely(again.last_confirmed_txnid > MAX_TXNID)) {
      atomic_yield();
      again.last_confirmed_txnid = safe64_read((mdbx_atomic_uint64_t *)&entry->last_confirmed_txnid);
      if (unlikely(again.last_confirmed_txnid > MAX_TXNID)) {
        atomic_yield();
        again.last_confirmed_txnid = safe64_read((mdbx_atomic_uint64_t *)&entry->last_confirmed_txnid);
        if (unlikely(again.last_confirmed_txnid > MAX_TXNID))
          return cache_fallback(txn, dbi, key, data, MDBX_CACHE_RACE);
      }
    }

    again.trunk_txnid = entry->trunk_txnid;
    again.offset = entry->offset;
    again.length = entry->length;
    if (local.last_confirmed_txnid == again.last_confirmed_txnid && local.trunk_txnid == again.trunk_txnid &&
        local.offset == again.offset && local.length == again.length)
      break;

    local = again;
    atomic_yield();
  }

  MDBX_cache_result_t result = cache_get(txn, dbi, key, data, &local);
  if (result.status > MDBX_CACHE_HIT) {
    tASSERT(txn, local.last_confirmed_txnid < MAX_TXNID && local.trunk_txnid <= local.last_confirmed_txnid &&
                     local.trunk_txnid > 0);
    while (true) {
      const txnid_t snap = safe64_read((mdbx_atomic_uint64_t *)&entry->last_confirmed_txnid);
      if (snap >= local.last_confirmed_txnid) {
        result.status = MDBX_CACHE_RACE;
        break;
      }

      if (likely(safe64_reset_compare((mdbx_atomic_uint64_t *)&entry->last_confirmed_txnid, snap))) {
        entry->trunk_txnid = 0;
        osal_compiler_barrier();
        entry->offset = local.offset;
        entry->length = local.length;
        entry->trunk_txnid = local.trunk_txnid;
        safe64_write((mdbx_atomic_uint64_t *)&entry->last_confirmed_txnid, local.last_confirmed_txnid);
        break;
      }

      atomic_yield();
    }
  }
  return result;
}

__hot MDBX_cache_result_t mdbx_cache_get_SingleThreaded(const MDBX_txn *txn, MDBX_dbi dbi, const MDBX_val *key,
                                                        MDBX_val *data, MDBX_cache_entry *entry) {
  if (unlikely(!key || !data || !entry))
    return cache_error(LOG_IFERR(MDBX_EINVAL));

  return cache_get(txn, dbi, key, data, entry);
}

LIBMDBX_API void mdbx_cache_init(MDBX_cache_entry *entry) { __inline_mdbx_cache_init(entry); }
