/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025

#pragma once

#include "essentials.h"

/* Internal prototypes */

/* audit.c */
MDBX_INTERNAL int audit_ex(MDBX_txn *txn, size_t retired_stored, bool dont_filter_gc);

/* mvcc-readers.c */
MDBX_INTERNAL bsr_t mvcc_bind_slot(MDBX_env *env);
MDBX_MAYBE_UNUSED MDBX_INTERNAL pgno_t mvcc_largest_this(MDBX_env *env, pgno_t largest);
MDBX_INTERNAL txnid_t mvcc_shapshot_oldest(MDBX_env *const env, const txnid_t steady);
MDBX_INTERNAL pgno_t mvcc_snapshot_largest(const MDBX_env *env, pgno_t last_used_page);
MDBX_INTERNAL int mvcc_cleanup_dead(MDBX_env *env, int rlocked, int *dead);
MDBX_INTERNAL bool mvcc_kick_laggards(MDBX_env *env, const txnid_t laggard);

/* dxb.c */
MDBX_INTERNAL int dxb_setup(MDBX_env *env, const int lck_rc, const mdbx_mode_t mode_bits);
MDBX_INTERNAL int __must_check_result dxb_read_header(MDBX_env *env, meta_t *meta, const int lck_exclusive,
                                                      const mdbx_mode_t mode_bits);
enum resize_mode { implicit_grow, impilict_shrink, explicit_resize };
MDBX_INTERNAL int __must_check_result dxb_resize(MDBX_env *const env, const pgno_t used_pgno, const pgno_t size_pgno,
                                                 pgno_t limit_pgno, const enum resize_mode mode);
MDBX_INTERNAL int dxb_set_readahead(const MDBX_env *env, const pgno_t edge, const bool enable, const bool force_whole);
MDBX_INTERNAL int __must_check_result dxb_sync_locked(MDBX_env *env, unsigned flags, meta_t *const pending,
                                                      troika_t *const troika);
#if defined(ENABLE_MEMCHECK) || defined(__SANITIZE_ADDRESS__)
MDBX_INTERNAL void dxb_sanitize_tail(MDBX_env *env, MDBX_txn *txn);
#else
static inline void dxb_sanitize_tail(MDBX_env *env, MDBX_txn *txn) {
  (void)env;
  (void)txn;
}
#endif /* ENABLE_MEMCHECK || __SANITIZE_ADDRESS__ */

/* txn.c */
#define TXN_END_NAMES                                                                                                  \
  {"committed", "pure-commit", "abort", "reset", "fail-begin", "fail-begin-nested", "ousted", nullptr}
enum {
  /* txn_end operation number, for logging */
  TXN_END_COMMITTED /* 0 */,
  TXN_END_PURE_COMMIT /* 1 */,
  TXN_END_ABORT /* 2 */,
  TXN_END_RESET /* 3 */,
  TXN_END_FAIL_BEGIN /* 4 */,
  TXN_END_FAIL_BEGIN_NESTED /* 5 */,
  TXN_END_OUSTED /* 6 */,

  TXN_END_OPMASK = 0x07 /* mask for txn_end() operation number */,
  TXN_END_UPDATE = 0x10 /* update env state (DBIs) */,
  TXN_END_FREE = 0x20 /* free txn unless it is env.basal_txn */,
  TXN_END_SLOT = 0x40 /* release any reader slot if NOSTICKYTHREADS */
};

struct commit_timestamp {
  uint64_t start, prep, gc, audit, write, sync, gc_cpu;
};

MDBX_INTERNAL bool txn_refund(MDBX_txn *txn);
MDBX_INTERNAL bool txn_gc_detent(const MDBX_txn *const txn);
MDBX_INTERNAL int txn_check_badbits_parked(const MDBX_txn *txn, int bad_bits);
MDBX_INTERNAL void txn_done_cursors(MDBX_txn *txn);
MDBX_INTERNAL int txn_shadow_cursors(const MDBX_txn *parent, const size_t dbi);

MDBX_INTERNAL MDBX_txn *txn_alloc(const MDBX_txn_flags_t flags, MDBX_env *env);
MDBX_INTERNAL int txn_abort(MDBX_txn *txn);
MDBX_INTERNAL int txn_renew(MDBX_txn *txn, unsigned flags);
MDBX_INTERNAL int txn_end(MDBX_txn *txn, unsigned mode);

MDBX_INTERNAL int txn_nested_create(MDBX_txn *parent, const MDBX_txn_flags_t flags);
MDBX_INTERNAL void txn_nested_abort(MDBX_txn *nested);
MDBX_INTERNAL int txn_nested_join(MDBX_txn *txn, struct commit_timestamp *ts);

MDBX_INTERNAL MDBX_txn *txn_basal_create(const size_t max_dbi);
MDBX_INTERNAL void txn_basal_destroy(MDBX_txn *txn);
MDBX_INTERNAL int txn_basal_start(MDBX_txn *txn, unsigned flags);
MDBX_INTERNAL int txn_basal_commit(MDBX_txn *txn, struct commit_timestamp *ts);
MDBX_INTERNAL int txn_basal_end(MDBX_txn *txn, unsigned mode);

MDBX_INTERNAL int txn_ro_park(MDBX_txn *txn, bool autounpark);
MDBX_INTERNAL int txn_ro_unpark(MDBX_txn *txn);
MDBX_INTERNAL int txn_ro_start(MDBX_txn *txn, unsigned flags);
MDBX_INTERNAL int txn_ro_end(MDBX_txn *txn, unsigned mode);

/* env.c */
MDBX_INTERNAL int env_open(MDBX_env *env, mdbx_mode_t mode);
MDBX_INTERNAL int env_info(const MDBX_env *env, const MDBX_txn *txn, MDBX_envinfo *out, size_t bytes, troika_t *troika);
MDBX_INTERNAL int env_sync(MDBX_env *env, bool force, bool nonblock);
MDBX_INTERNAL int env_close(MDBX_env *env, bool resurrect_after_fork);
MDBX_INTERNAL MDBX_txn *env_owned_wrtxn(const MDBX_env *env);
MDBX_INTERNAL int __must_check_result env_page_auxbuffer(MDBX_env *env);
MDBX_INTERNAL unsigned env_setup_pagesize(MDBX_env *env, const size_t pagesize);

/* api-opt.c */
MDBX_INTERNAL void env_options_init(MDBX_env *env);
MDBX_INTERNAL void env_options_adjust_defaults(MDBX_env *env);
MDBX_INTERNAL void env_options_adjust_dp_limit(MDBX_env *env);
MDBX_INTERNAL pgno_t default_dp_limit(const MDBX_env *env);

/* tree.c */
MDBX_INTERNAL int tree_drop(MDBX_cursor *mc, const bool may_have_tables);
MDBX_INTERNAL int __must_check_result tree_rebalance(MDBX_cursor *mc);
MDBX_INTERNAL int __must_check_result tree_propagate_key(MDBX_cursor *mc, const MDBX_val *key);
MDBX_INTERNAL void recalculate_merge_thresholds(MDBX_env *env);
MDBX_INTERNAL void recalculate_subpage_thresholds(MDBX_env *env);

/* table.c */
MDBX_INTERNAL int __must_check_result tbl_fetch(MDBX_txn *txn, size_t dbi);
MDBX_INTERNAL int __must_check_result tbl_setup(const MDBX_env *env, volatile kvx_t *const kvx, const tree_t *const db);

/* coherency.c */
MDBX_INTERNAL bool coherency_check_meta(const MDBX_env *env, const volatile meta_t *meta, bool report);
MDBX_INTERNAL int coherency_fetch_head(MDBX_txn *txn, const meta_ptr_t head, uint64_t *timestamp);
MDBX_INTERNAL int coherency_check_written(const MDBX_env *env, const txnid_t txnid, const volatile meta_t *meta,
                                          const intptr_t pgno, uint64_t *timestamp);
MDBX_INTERNAL int coherency_timeout(uint64_t *timestamp, intptr_t pgno, const MDBX_env *env);
