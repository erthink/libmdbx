/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024

#pragma once

#include "essentials.h"

/* Internal prototypes */

/* audit.c */
MDBX_INTERNAL int audit_ex(MDBX_txn *txn, size_t retired_stored,
                           bool dont_filter_gc);

/* mvcc-readers.c */
MDBX_INTERNAL bsr_t mvcc_bind_slot(MDBX_env *env);
MDBX_MAYBE_UNUSED MDBX_INTERNAL pgno_t mvcc_largest_this(MDBX_env *env,
                                                         pgno_t largest);
MDBX_INTERNAL txnid_t mvcc_shapshot_oldest(MDBX_env *const env,
                                           const txnid_t steady);
MDBX_INTERNAL pgno_t mvcc_snapshot_largest(const MDBX_env *env,
                                           pgno_t last_used_page);
MDBX_INTERNAL txnid_t mvcc_kick_laggards(MDBX_env *env,
                                         const txnid_t straggler);
MDBX_INTERNAL int mvcc_cleanup_dead(MDBX_env *env, int rlocked, int *dead);
MDBX_INTERNAL txnid_t mvcc_kick_laggards(MDBX_env *env, const txnid_t laggard);

/* dxb.c */
MDBX_INTERNAL int dxb_setup(MDBX_env *env, const int lck_rc,
                            const mdbx_mode_t mode_bits);
MDBX_INTERNAL int __must_check_result
dxb_read_header(MDBX_env *env, meta_t *meta, const int lck_exclusive,
                const mdbx_mode_t mode_bits);
enum resize_mode { implicit_grow, impilict_shrink, explicit_resize };
MDBX_INTERNAL int __must_check_result dxb_resize(MDBX_env *const env,
                                                 const pgno_t used_pgno,
                                                 const pgno_t size_pgno,
                                                 pgno_t limit_pgno,
                                                 const enum resize_mode mode);
MDBX_INTERNAL int dxb_set_readahead(const MDBX_env *env, const pgno_t edge,
                                    const bool enable, const bool force_whole);
MDBX_INTERNAL int __must_check_result dxb_sync_locked(MDBX_env *env,
                                                      unsigned flags,
                                                      meta_t *const pending,
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
MDBX_INTERNAL bool txn_refund(MDBX_txn *txn);
MDBX_INTERNAL txnid_t txn_snapshot_oldest(const MDBX_txn *const txn);
MDBX_INTERNAL int txn_abort(MDBX_txn *txn);
MDBX_INTERNAL int txn_renew(MDBX_txn *txn, unsigned flags);
MDBX_INTERNAL int txn_park(MDBX_txn *txn, bool autounpark);
MDBX_INTERNAL int txn_unpark(MDBX_txn *txn);
MDBX_INTERNAL int txn_check_badbits_parked(const MDBX_txn *txn, int bad_bits);

#define TXN_END_NAMES                                                          \
  {"committed", "empty-commit", "abort",           "reset",                    \
   "reset-tmp", "fail-begin",   "fail-beginchild", "ousted"}
enum {
  /* txn_end operation number, for logging */
  TXN_END_COMMITTED,
  TXN_END_PURE_COMMIT,
  TXN_END_ABORT,
  TXN_END_RESET,
  TXN_END_RESET_TMP,
  TXN_END_FAIL_BEGIN,
  TXN_END_FAIL_BEGINCHILD,
  TXN_END_OUSTED,

  TXN_END_OPMASK = 0x0F /* mask for txn_end() operation number */,
  TXN_END_UPDATE = 0x10 /* update env state (DBIs) */,
  TXN_END_FREE = 0x20 /* free txn unless it is env.basal_txn */,
  TXN_END_EOTDONE = 0x40 /* txn's cursors already closed */,
  TXN_END_SLOT = 0x80 /* release any reader slot if NOSTICKYTHREADS */
};
MDBX_INTERNAL int txn_end(MDBX_txn *txn, unsigned mode);
MDBX_INTERNAL int txn_write(MDBX_txn *txn, iov_ctx_t *ctx);

/* env.c */
MDBX_INTERNAL int env_open(MDBX_env *env, mdbx_mode_t mode);
MDBX_INTERNAL int env_info(const MDBX_env *env, const MDBX_txn *txn,
                           MDBX_envinfo *out, size_t bytes, troika_t *troika);
MDBX_INTERNAL int env_sync(MDBX_env *env, bool force, bool nonblock);
MDBX_INTERNAL int env_close(MDBX_env *env, bool resurrect_after_fork);
MDBX_INTERNAL bool env_txn0_owned(const MDBX_env *env);
MDBX_INTERNAL void env_options_init(MDBX_env *env);
MDBX_INTERNAL void env_options_adjust_defaults(MDBX_env *env);
MDBX_INTERNAL int __must_check_result env_page_auxbuffer(MDBX_env *env);
MDBX_INTERNAL unsigned env_setup_pagesize(MDBX_env *env, const size_t pagesize);

/* tree.c */
MDBX_INTERNAL int tree_drop(MDBX_cursor *mc, const bool may_have_tables);
MDBX_INTERNAL int __must_check_result tree_rebalance(MDBX_cursor *mc);
MDBX_INTERNAL int __must_check_result tree_propagate_key(MDBX_cursor *mc,
                                                         const MDBX_val *key);
MDBX_INTERNAL void recalculate_merge_thresholds(MDBX_env *env);
MDBX_INTERNAL void recalculate_subpage_thresholds(MDBX_env *env);

/* table.c */
MDBX_INTERNAL int __must_check_result tbl_fetch(MDBX_txn *txn, size_t dbi);
MDBX_INTERNAL int __must_check_result tbl_setup(const MDBX_env *env,
                                                kvx_t *const kvx,
                                                const tree_t *const db);

/* coherency.c */
MDBX_INTERNAL bool coherency_check_meta(const MDBX_env *env,
                                        const volatile meta_t *meta,
                                        bool report);
MDBX_INTERNAL int coherency_fetch_head(MDBX_txn *txn, const meta_ptr_t head,
                                       uint64_t *timestamp);
MDBX_INTERNAL int coherency_check_written(const MDBX_env *env,
                                          const txnid_t txnid,
                                          const volatile meta_t *meta,
                                          const intptr_t pgno,
                                          uint64_t *timestamp);
MDBX_INTERNAL int coherency_timeout(uint64_t *timestamp, intptr_t pgno,
                                    const MDBX_env *env);
