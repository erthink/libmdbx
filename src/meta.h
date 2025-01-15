/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025

#pragma once

#include "essentials.h"

static inline uint64_t meta_sign_calculate(const meta_t *meta) {
  uint64_t sign = DATASIGN_NONE;
#if 0 /* TODO */
  sign = hippeus_hash64(...);
#else
  (void)meta;
#endif
  /* LY: newer returns DATASIGN_NONE or DATASIGN_WEAK */
  return (sign > DATASIGN_WEAK) ? sign : ~sign;
}

static inline uint64_t meta_sign_get(const volatile meta_t *meta) { return unaligned_peek_u64_volatile(4, meta->sign); }

static inline void meta_sign_as_steady(meta_t *meta) { unaligned_poke_u64(4, meta->sign, meta_sign_calculate(meta)); }

static inline bool meta_is_steady(const volatile meta_t *meta) { return SIGN_IS_STEADY(meta_sign_get(meta)); }

MDBX_INTERNAL troika_t meta_tap(const MDBX_env *env);
MDBX_INTERNAL unsigned meta_eq_mask(const troika_t *troika);
MDBX_INTERNAL bool meta_should_retry(const MDBX_env *env, troika_t *troika);
MDBX_MAYBE_UNUSED MDBX_INTERNAL bool troika_verify_fsm(void);

struct meta_ptr {
  txnid_t txnid;
  union {
    const volatile meta_t *ptr_v;
    const meta_t *ptr_c;
  };
  size_t is_steady;
};

MDBX_INTERNAL meta_ptr_t meta_ptr(const MDBX_env *env, unsigned n);
MDBX_INTERNAL txnid_t meta_txnid(const volatile meta_t *meta);
MDBX_INTERNAL txnid_t recent_committed_txnid(const MDBX_env *env);
MDBX_INTERNAL int meta_sync(const MDBX_env *env, const meta_ptr_t head);

MDBX_INTERNAL const char *durable_caption(const meta_t *const meta);
MDBX_INTERNAL void meta_troika_dump(const MDBX_env *env, const troika_t *troika);

#define METAPAGE(env, n) page_meta(pgno2page(env, n))
#define METAPAGE_END(env) METAPAGE(env, NUM_METAS)

static inline meta_ptr_t meta_recent(const MDBX_env *env, const troika_t *troika) {
  meta_ptr_t r;
  r.txnid = troika->txnid[troika->recent];
  r.ptr_v = METAPAGE(env, troika->recent);
  r.is_steady = (troika->fsm >> troika->recent) & 1;
  return r;
}

static inline meta_ptr_t meta_prefer_steady(const MDBX_env *env, const troika_t *troika) {
  meta_ptr_t r;
  r.txnid = troika->txnid[troika->prefer_steady];
  r.ptr_v = METAPAGE(env, troika->prefer_steady);
  r.is_steady = (troika->fsm >> troika->prefer_steady) & 1;
  return r;
}

static inline meta_ptr_t meta_tail(const MDBX_env *env, const troika_t *troika) {
  const uint8_t tail = troika->tail_and_flags & 3;
  MDBX_ANALYSIS_ASSUME(tail < NUM_METAS);
  meta_ptr_t r;
  r.txnid = troika->txnid[tail];
  r.ptr_v = METAPAGE(env, tail);
  r.is_steady = (troika->fsm >> tail) & 1;
  return r;
}

static inline bool meta_is_used(const troika_t *troika, unsigned n) {
  return n == troika->recent || n == troika->prefer_steady;
}

static inline bool meta_bootid_match(const meta_t *meta) {

  return memcmp(&meta->bootid, &globals.bootid, 16) == 0 && (globals.bootid.x | globals.bootid.y) != 0;
}

static inline bool meta_weak_acceptable(const MDBX_env *env, const meta_t *meta, const int lck_exclusive) {
  return lck_exclusive
             ? /* exclusive lock */ meta_bootid_match(meta)
             : /* db already opened */ env->lck_mmap.lck && (env->lck_mmap.lck->envmode.weak & MDBX_RDONLY) == 0;
}

MDBX_NOTHROW_PURE_FUNCTION static inline txnid_t constmeta_txnid(const meta_t *meta) {
  const txnid_t a = unaligned_peek_u64(4, &meta->txnid_a);
  const txnid_t b = unaligned_peek_u64(4, &meta->txnid_b);
  return likely(a == b) ? a : 0;
}

static inline void meta_update_begin(const MDBX_env *env, meta_t *meta, txnid_t txnid) {
  eASSERT(env, meta >= METAPAGE(env, 0) && meta < METAPAGE_END(env));
  eASSERT(env, unaligned_peek_u64(4, meta->txnid_a) < txnid && unaligned_peek_u64(4, meta->txnid_b) < txnid);
  (void)env;
#if (defined(__amd64__) || defined(__e2k__)) && !defined(ENABLE_UBSAN) && MDBX_UNALIGNED_OK >= 8
  atomic_store64((mdbx_atomic_uint64_t *)&meta->txnid_b, 0, mo_AcquireRelease);
  atomic_store64((mdbx_atomic_uint64_t *)&meta->txnid_a, txnid, mo_AcquireRelease);
#else
  atomic_store32(&meta->txnid_b[__BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__], 0, mo_AcquireRelease);
  atomic_store32(&meta->txnid_b[__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__], 0, mo_AcquireRelease);
  atomic_store32(&meta->txnid_a[__BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__], (uint32_t)txnid, mo_AcquireRelease);
  atomic_store32(&meta->txnid_a[__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__], (uint32_t)(txnid >> 32), mo_AcquireRelease);
#endif
}

static inline void meta_update_end(const MDBX_env *env, meta_t *meta, txnid_t txnid) {
  eASSERT(env, meta >= METAPAGE(env, 0) && meta < METAPAGE_END(env));
  eASSERT(env, unaligned_peek_u64(4, meta->txnid_a) == txnid);
  eASSERT(env, unaligned_peek_u64(4, meta->txnid_b) < txnid);
  (void)env;
  jitter4testing(true);
  memcpy(&meta->bootid, &globals.bootid, 16);
#if (defined(__amd64__) || defined(__e2k__)) && !defined(ENABLE_UBSAN) && MDBX_UNALIGNED_OK >= 8
  atomic_store64((mdbx_atomic_uint64_t *)&meta->txnid_b, txnid, mo_AcquireRelease);
#else
  atomic_store32(&meta->txnid_b[__BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__], (uint32_t)txnid, mo_AcquireRelease);
  atomic_store32(&meta->txnid_b[__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__], (uint32_t)(txnid >> 32), mo_AcquireRelease);
#endif
}

static inline void meta_set_txnid(const MDBX_env *env, meta_t *meta, const txnid_t txnid) {
  eASSERT(env, !env->dxb_mmap.base || meta < METAPAGE(env, 0) || meta >= METAPAGE_END(env));
  (void)env;
  /* update inconsistently since this function used ONLY for filling meta-image
   * for writing, but not the actual meta-page */
  memcpy(&meta->bootid, &globals.bootid, 16);
  unaligned_poke_u64(4, meta->txnid_a, txnid);
  unaligned_poke_u64(4, meta->txnid_b, txnid);
}

static inline uint8_t meta_cmp2int(txnid_t a, txnid_t b, uint8_t s) {
  return unlikely(a == b) ? 1 * s : (a > b) ? 2 * s : 0 * s;
}

static inline uint8_t meta_cmp2recent(uint8_t ab_cmp2int, bool a_steady, bool b_steady) {
  assert(ab_cmp2int < 3 /* && a_steady< 2 && b_steady < 2 */);
  return ab_cmp2int > 1 || (ab_cmp2int == 1 && a_steady > b_steady);
}

static inline uint8_t meta_cmp2steady(uint8_t ab_cmp2int, bool a_steady, bool b_steady) {
  assert(ab_cmp2int < 3 /* && a_steady< 2 && b_steady < 2 */);
  return a_steady > b_steady || (a_steady == b_steady && ab_cmp2int > 1);
}

static inline bool meta_choice_recent(txnid_t a_txnid, bool a_steady, txnid_t b_txnid, bool b_steady) {
  return meta_cmp2recent(meta_cmp2int(a_txnid, b_txnid, 1), a_steady, b_steady);
}

static inline bool meta_choice_steady(txnid_t a_txnid, bool a_steady, txnid_t b_txnid, bool b_steady) {
  return meta_cmp2steady(meta_cmp2int(a_txnid, b_txnid, 1), a_steady, b_steady);
}

MDBX_INTERNAL meta_t *meta_init_triplet(const MDBX_env *env, void *buffer);

MDBX_INTERNAL int meta_validate(MDBX_env *env, meta_t *const meta, const page_t *const page, const unsigned meta_number,
                                unsigned *guess_pagesize);

MDBX_INTERNAL int __must_check_result meta_validate_copy(MDBX_env *env, const meta_t *meta, meta_t *dest);

MDBX_INTERNAL int __must_check_result meta_override(MDBX_env *env, size_t target, txnid_t txnid, const meta_t *shape);

MDBX_INTERNAL int meta_wipe_steady(MDBX_env *env, txnid_t inclusive_upto);
