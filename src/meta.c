/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024

#include "internals.h"

typedef struct meta_snap {
  uint64_t txnid;
  size_t is_steady;
} meta_snap_t;

static inline txnid_t fetch_txnid(const volatile mdbx_atomic_uint32_t *ptr) {
#if (defined(__amd64__) || defined(__e2k__)) && !defined(ENABLE_UBSAN) &&      \
    MDBX_UNALIGNED_OK >= 8
  return atomic_load64((const volatile mdbx_atomic_uint64_t *)ptr,
                       mo_AcquireRelease);
#else
  const uint32_t l = atomic_load32(
      &ptr[__BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__], mo_AcquireRelease);
  const uint32_t h = atomic_load32(
      &ptr[__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__], mo_AcquireRelease);
  return (uint64_t)h << 32 | l;
#endif
}

static inline meta_snap_t meta_snap(const volatile meta_t *meta) {
  txnid_t txnid = fetch_txnid(meta->txnid_a);
  jitter4testing(true);
  size_t is_steady = meta_is_steady(meta) && txnid >= MIN_TXNID;
  jitter4testing(true);
  if (unlikely(txnid != fetch_txnid(meta->txnid_b)))
    txnid = is_steady = 0;
  meta_snap_t r = {txnid, is_steady};
  return r;
}

txnid_t meta_txnid(const volatile meta_t *meta) {
  return meta_snap(meta).txnid;
}

meta_ptr_t meta_ptr(const MDBX_env *env, unsigned n) {
  eASSERT(env, n < NUM_METAS);
  meta_ptr_t r;
  meta_snap_t snap = meta_snap(r.ptr_v = METAPAGE(env, n));
  r.txnid = snap.txnid;
  r.is_steady = snap.is_steady;
  return r;
}

static uint8_t meta_cmp2pack(uint8_t c01, uint8_t c02, uint8_t c12, bool s0,
                             bool s1, bool s2) {
  assert(c01 < 3 && c02 < 3 && c12 < 3);
  /* assert(s0 < 2 && s1 < 2 && s2 < 2); */
  const uint8_t recent = meta_cmp2recent(c01, s0, s1)
                             ? (meta_cmp2recent(c02, s0, s2) ? 0 : 2)
                             : (meta_cmp2recent(c12, s1, s2) ? 1 : 2);
  const uint8_t prefer_steady = meta_cmp2steady(c01, s0, s1)
                                    ? (meta_cmp2steady(c02, s0, s2) ? 0 : 2)
                                    : (meta_cmp2steady(c12, s1, s2) ? 1 : 2);

  uint8_t tail;
  if (recent == 0)
    tail = meta_cmp2steady(c12, s1, s2) ? 2 : 1;
  else if (recent == 1)
    tail = meta_cmp2steady(c02, s0, s2) ? 2 : 0;
  else
    tail = meta_cmp2steady(c01, s0, s1) ? 1 : 0;

  const bool valid =
      c01 != 1 || s0 != s1 || c02 != 1 || s0 != s2 || c12 != 1 || s1 != s2;
  const bool strict = (c01 != 1 || s0 != s1) && (c02 != 1 || s0 != s2) &&
                      (c12 != 1 || s1 != s2);
  return tail | recent << 2 | prefer_steady << 4 | strict << 6 | valid << 7;
}

static inline void meta_troika_unpack(troika_t *troika, const uint8_t packed) {
  troika->recent = (packed >> 2) & 3;
  troika->prefer_steady = (packed >> 4) & 3;
  troika->tail_and_flags = packed & 0xC3;
#if MDBX_WORDBITS > 32 /* Workaround for false-positives from Valgrind */
  troika->unused_pad = 0;
#endif
}

static const uint8_t troika_fsm_map[2 * 2 * 2 * 3 * 3 * 3] = {
    232, 201, 216, 216, 232, 233, 232, 232, 168, 201, 216, 152, 168, 233, 232,
    168, 233, 201, 216, 201, 233, 233, 232, 233, 168, 201, 152, 216, 232, 169,
    232, 168, 168, 193, 152, 152, 168, 169, 232, 168, 169, 193, 152, 194, 233,
    169, 232, 169, 232, 201, 216, 216, 232, 201, 232, 232, 168, 193, 216, 152,
    168, 193, 232, 168, 193, 193, 210, 194, 225, 193, 225, 193, 168, 137, 212,
    214, 232, 233, 168, 168, 168, 137, 212, 150, 168, 233, 168, 168, 169, 137,
    216, 201, 233, 233, 168, 169, 168, 137, 148, 214, 232, 169, 168, 168, 40,
    129, 148, 150, 168, 169, 168, 40,  169, 129, 152, 194, 233, 169, 168, 169,
    168, 137, 214, 214, 232, 201, 168, 168, 168, 129, 214, 150, 168, 193, 168,
    168, 129, 129, 210, 194, 225, 193, 161, 129, 212, 198, 212, 214, 228, 228,
    212, 212, 148, 201, 212, 150, 164, 233, 212, 148, 233, 201, 216, 201, 233,
    233, 216, 233, 148, 198, 148, 214, 228, 164, 212, 148, 148, 194, 148, 150,
    164, 169, 212, 148, 169, 194, 152, 194, 233, 169, 216, 169, 214, 198, 214,
    214, 228, 198, 212, 214, 150, 194, 214, 150, 164, 193, 212, 150, 194, 194,
    210, 194, 225, 193, 210, 194};

__cold bool troika_verify_fsm(void) {
  bool ok = true;
  for (size_t i = 0; i < 2 * 2 * 2 * 3 * 3 * 3; ++i) {
    const bool s0 = (i >> 0) & 1;
    const bool s1 = (i >> 1) & 1;
    const bool s2 = (i >> 2) & 1;
    const uint8_t c01 = (i / (8 * 1)) % 3;
    const uint8_t c02 = (i / (8 * 3)) % 3;
    const uint8_t c12 = (i / (8 * 9)) % 3;

    const uint8_t packed = meta_cmp2pack(c01, c02, c12, s0, s1, s2);
    troika_t troika;
    troika.fsm = (uint8_t)i;
    meta_troika_unpack(&troika, packed);

    const uint8_t tail = TROIKA_TAIL(&troika);
    const bool strict = TROIKA_STRICT_VALID(&troika);
    const bool valid = TROIKA_VALID(&troika);

    const uint8_t recent_chk = meta_cmp2recent(c01, s0, s1)
                                   ? (meta_cmp2recent(c02, s0, s2) ? 0 : 2)
                                   : (meta_cmp2recent(c12, s1, s2) ? 1 : 2);
    const uint8_t prefer_steady_chk =
        meta_cmp2steady(c01, s0, s1) ? (meta_cmp2steady(c02, s0, s2) ? 0 : 2)
                                     : (meta_cmp2steady(c12, s1, s2) ? 1 : 2);

    uint8_t tail_chk;
    if (recent_chk == 0)
      tail_chk = meta_cmp2steady(c12, s1, s2) ? 2 : 1;
    else if (recent_chk == 1)
      tail_chk = meta_cmp2steady(c02, s0, s2) ? 2 : 0;
    else
      tail_chk = meta_cmp2steady(c01, s0, s1) ? 1 : 0;

    const bool valid_chk =
        c01 != 1 || s0 != s1 || c02 != 1 || s0 != s2 || c12 != 1 || s1 != s2;
    const bool strict_chk = (c01 != 1 || s0 != s1) && (c02 != 1 || s0 != s2) &&
                            (c12 != 1 || s1 != s2);
    assert(troika.recent == recent_chk);
    assert(troika.prefer_steady == prefer_steady_chk);
    assert(tail == tail_chk);
    assert(valid == valid_chk);
    assert(strict == strict_chk);
    assert(troika_fsm_map[troika.fsm] == packed);
    if (troika.recent != recent_chk ||
        troika.prefer_steady != prefer_steady_chk || tail != tail_chk ||
        valid != valid_chk || strict != strict_chk ||
        troika_fsm_map[troika.fsm] != packed) {
      ok = false;
    }
  }
  return ok;
}

__hot troika_t meta_tap(const MDBX_env *env) {
  meta_snap_t snap;
  troika_t troika;
  snap = meta_snap(METAPAGE(env, 0));
  troika.txnid[0] = snap.txnid;
  troika.fsm = (uint8_t)snap.is_steady << 0;
  snap = meta_snap(METAPAGE(env, 1));
  troika.txnid[1] = snap.txnid;
  troika.fsm += (uint8_t)snap.is_steady << 1;
  troika.fsm += meta_cmp2int(troika.txnid[0], troika.txnid[1], 8);
  snap = meta_snap(METAPAGE(env, 2));
  troika.txnid[2] = snap.txnid;
  troika.fsm += (uint8_t)snap.is_steady << 2;
  troika.fsm += meta_cmp2int(troika.txnid[0], troika.txnid[2], 8 * 3);
  troika.fsm += meta_cmp2int(troika.txnid[1], troika.txnid[2], 8 * 3 * 3);

  meta_troika_unpack(&troika, troika_fsm_map[troika.fsm]);
  return troika;
}

txnid_t recent_committed_txnid(const MDBX_env *env) {
  const txnid_t m0 = meta_txnid(METAPAGE(env, 0));
  const txnid_t m1 = meta_txnid(METAPAGE(env, 1));
  const txnid_t m2 = meta_txnid(METAPAGE(env, 2));
  return (m0 > m1) ? ((m0 > m2) ? m0 : m2) : ((m1 > m2) ? m1 : m2);
}

static inline bool meta_eq(const troika_t *troika, size_t a, size_t b) {
  assert(a < NUM_METAS && b < NUM_METAS);
  return troika->txnid[a] == troika->txnid[b] &&
         (((troika->fsm >> a) ^ (troika->fsm >> b)) & 1) == 0 &&
         troika->txnid[a];
}

unsigned meta_eq_mask(const troika_t *troika) {
  return meta_eq(troika, 0, 1) | meta_eq(troika, 1, 2) << 1 |
         meta_eq(troika, 2, 0) << 2;
}

__hot bool meta_should_retry(const MDBX_env *env, troika_t *troika) {
  const troika_t prev = *troika;
  *troika = meta_tap(env);
  return prev.fsm != troika->fsm || prev.txnid[0] != troika->txnid[0] ||
         prev.txnid[1] != troika->txnid[1] || prev.txnid[2] != troika->txnid[2];
}

const char *durable_caption(const meta_t *const meta) {
  if (meta_is_steady(meta))
    return (meta_sign_get(meta) == meta_sign_calculate(meta)) ? "Steady"
                                                              : "Tainted";
  return "Weak";
}

__cold void meta_troika_dump(const MDBX_env *env, const troika_t *troika) {
  const meta_ptr_t recent = meta_recent(env, troika);
  const meta_ptr_t prefer_steady = meta_prefer_steady(env, troika);
  const meta_ptr_t tail = meta_tail(env, troika);
  NOTICE("troika: %" PRIaTXN ".%c:%" PRIaTXN ".%c:%" PRIaTXN ".%c, fsm=0x%02x, "
         "head=%d-%" PRIaTXN ".%c, "
         "base=%d-%" PRIaTXN ".%c, "
         "tail=%d-%" PRIaTXN ".%c, "
         "valid %c, strict %c",
         troika->txnid[0], (troika->fsm & 1) ? 's' : 'w', troika->txnid[1],
         (troika->fsm & 2) ? 's' : 'w', troika->txnid[2],
         (troika->fsm & 4) ? 's' : 'w', troika->fsm, troika->recent,
         recent.txnid, recent.is_steady ? 's' : 'w', troika->prefer_steady,
         prefer_steady.txnid, prefer_steady.is_steady ? 's' : 'w',
         troika->tail_and_flags % NUM_METAS, tail.txnid,
         tail.is_steady ? 's' : 'w', TROIKA_VALID(troika) ? 'Y' : 'N',
         TROIKA_STRICT_VALID(troika) ? 'Y' : 'N');
}

/*----------------------------------------------------------------------------*/

static int meta_unsteady(MDBX_env *env, const txnid_t inclusive_upto,
                         const pgno_t pgno) {
  meta_t *const meta = METAPAGE(env, pgno);
  const txnid_t txnid = constmeta_txnid(meta);
  if (!meta_is_steady(meta) || txnid > inclusive_upto)
    return MDBX_RESULT_FALSE;

  WARNING("wipe txn #%" PRIaTXN ", meta %" PRIaPGNO, txnid, pgno);
  const uint64_t wipe = DATASIGN_NONE;
  const void *ptr = &wipe;
  size_t bytes = sizeof(meta->sign),
         offset = ptr_dist(&meta->sign, env->dxb_mmap.base);
  if (env->flags & MDBX_WRITEMAP) {
    unaligned_poke_u64(4, meta->sign, wipe);
    osal_flush_incoherent_cpu_writeback();
    if (!MDBX_AVOID_MSYNC)
      return MDBX_RESULT_TRUE;
    ptr = data_page(meta);
    offset = ptr_dist(ptr, env->dxb_mmap.base);
    bytes = env->ps;
  }

#if MDBX_ENABLE_PGOP_STAT
  env->lck->pgops.wops.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
  int err = osal_pwrite(env->fd4meta, ptr, bytes, offset);
  return likely(err == MDBX_SUCCESS) ? MDBX_RESULT_TRUE : err;
}

__cold int meta_wipe_steady(MDBX_env *env, txnid_t inclusive_upto) {
  int err = meta_unsteady(env, inclusive_upto, 0);
  if (likely(!MDBX_IS_ERROR(err)))
    err = meta_unsteady(env, inclusive_upto, 1);
  if (likely(!MDBX_IS_ERROR(err)))
    err = meta_unsteady(env, inclusive_upto, 2);

  if (err == MDBX_RESULT_TRUE) {
    err = MDBX_SUCCESS;
    if (!MDBX_AVOID_MSYNC && (env->flags & MDBX_WRITEMAP)) {
      err = osal_msync(&env->dxb_mmap, 0, pgno_align2os_bytes(env, NUM_METAS),
                       MDBX_SYNC_DATA | MDBX_SYNC_IODQ);
#if MDBX_ENABLE_PGOP_STAT
      env->lck->pgops.msync.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
    } else if (env->fd4meta == env->lazy_fd) {
      err = osal_fsync(env->lazy_fd, MDBX_SYNC_DATA | MDBX_SYNC_IODQ);
#if MDBX_ENABLE_PGOP_STAT
      env->lck->pgops.fsync.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
    }
  }

  osal_flush_incoherent_mmap(env->dxb_mmap.base, pgno2bytes(env, NUM_METAS),
                             globals.sys_pagesize);

  /* force oldest refresh */
  atomic_store32(&env->lck->rdt_refresh_flag, true, mo_Relaxed);

  env->basal_txn->tw.troika = meta_tap(env);
  for (MDBX_txn *scan = env->basal_txn->nested; scan; scan = scan->nested)
    scan->tw.troika = env->basal_txn->tw.troika;
  return err;
}

int meta_sync(const MDBX_env *env, const meta_ptr_t head) {
  eASSERT(env, atomic_load32(&env->lck->meta_sync_txnid, mo_Relaxed) !=
                   (uint32_t)head.txnid);
  /* Функция может вызываться (в том числе) при (env->flags &
   * MDBX_NOMETASYNC) == 0 и env->fd4meta == env->dsync_fd, например если
   * предыдущая транзакция была выполненна с флагом MDBX_NOMETASYNC. */

  int rc = MDBX_RESULT_TRUE;
  if (env->flags & MDBX_WRITEMAP) {
    if (!MDBX_AVOID_MSYNC) {
      rc = osal_msync(&env->dxb_mmap, 0, pgno_align2os_bytes(env, NUM_METAS),
                      MDBX_SYNC_DATA | MDBX_SYNC_IODQ);
#if MDBX_ENABLE_PGOP_STAT
      env->lck->pgops.msync.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
    } else {
#if MDBX_ENABLE_PGOP_ST
      env->lck->pgops.wops.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
      const page_t *page = data_page(head.ptr_c);
      rc = osal_pwrite(env->fd4meta, page, env->ps,
                       ptr_dist(page, env->dxb_mmap.base));

      if (likely(rc == MDBX_SUCCESS) && env->fd4meta == env->lazy_fd) {
        rc = osal_fsync(env->lazy_fd, MDBX_SYNC_DATA | MDBX_SYNC_IODQ);
#if MDBX_ENABLE_PGOP_STAT
        env->lck->pgops.fsync.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
      }
    }
  } else {
    rc = osal_fsync(env->lazy_fd, MDBX_SYNC_DATA | MDBX_SYNC_IODQ);
#if MDBX_ENABLE_PGOP_STAT
    env->lck->pgops.fsync.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
  }

  if (likely(rc == MDBX_SUCCESS))
    env->lck->meta_sync_txnid.weak = (uint32_t)head.txnid;
  return rc;
}

__cold static page_t *meta_model(const MDBX_env *env, page_t *model, size_t num,
                                 const bin128_t *guid) {
  ENSURE(env, is_powerof2(env->ps));
  ENSURE(env, env->ps >= MDBX_MIN_PAGESIZE);
  ENSURE(env, env->ps <= MDBX_MAX_PAGESIZE);
  ENSURE(env, env->geo_in_bytes.lower >= MIN_MAPSIZE);
  ENSURE(env, env->geo_in_bytes.upper <= MAX_MAPSIZE);
  ENSURE(env, env->geo_in_bytes.now >= env->geo_in_bytes.lower);
  ENSURE(env, env->geo_in_bytes.now <= env->geo_in_bytes.upper);

  memset(model, 0, env->ps);
  model->pgno = (pgno_t)num;
  model->flags = P_META;
  meta_t *const model_meta = page_meta(model);
  unaligned_poke_u64(4, model_meta->magic_and_version, MDBX_DATA_MAGIC);

  model_meta->geometry.lower = bytes2pgno(env, env->geo_in_bytes.lower);
  model_meta->geometry.upper = bytes2pgno(env, env->geo_in_bytes.upper);
  model_meta->geometry.grow_pv =
      pages2pv(bytes2pgno(env, env->geo_in_bytes.grow));
  model_meta->geometry.shrink_pv =
      pages2pv(bytes2pgno(env, env->geo_in_bytes.shrink));
  model_meta->geometry.now = bytes2pgno(env, env->geo_in_bytes.now);
  model_meta->geometry.first_unallocated = NUM_METAS;

  ENSURE(env, model_meta->geometry.lower >= MIN_PAGENO);
  ENSURE(env, model_meta->geometry.upper <= MAX_PAGENO + 1);
  ENSURE(env, model_meta->geometry.now >= model_meta->geometry.lower);
  ENSURE(env, model_meta->geometry.now <= model_meta->geometry.upper);
  ENSURE(env, model_meta->geometry.first_unallocated >= MIN_PAGENO);
  ENSURE(env,
         model_meta->geometry.first_unallocated <= model_meta->geometry.now);
  ENSURE(env, model_meta->geometry.grow_pv ==
                  pages2pv(pv2pages(model_meta->geometry.grow_pv)));
  ENSURE(env, model_meta->geometry.shrink_pv ==
                  pages2pv(pv2pages(model_meta->geometry.shrink_pv)));

  model_meta->pagesize = env->ps;
  model_meta->trees.gc.flags = MDBX_INTEGERKEY;
  model_meta->trees.gc.root = P_INVALID;
  model_meta->trees.main.root = P_INVALID;
  memcpy(&model_meta->dxbid, guid, sizeof(model_meta->dxbid));
  meta_set_txnid(env, model_meta, MIN_TXNID + num);
  unaligned_poke_u64(4, model_meta->sign, meta_sign_calculate(model_meta));
  eASSERT(env, coherency_check_meta(env, model_meta, true));
  return ptr_disp(model, env->ps);
}

__cold meta_t *meta_init_triplet(const MDBX_env *env, void *buffer) {
  const bin128_t guid = osal_guid(env);
  page_t *page0 = (page_t *)buffer;
  page_t *page1 = meta_model(env, page0, 0, &guid);
  page_t *page2 = meta_model(env, page1, 1, &guid);
  meta_model(env, page2, 2, &guid);
  return page_meta(page2);
}

__cold int __must_check_result meta_override(MDBX_env *env, size_t target,
                                             txnid_t txnid,
                                             const meta_t *shape) {
  page_t *const page = env->page_auxbuf;
  meta_model(env, page, target,
             &((target == 0 && shape) ? shape : METAPAGE(env, 0))->dxbid);
  meta_t *const model = page_meta(page);
  meta_set_txnid(env, model, txnid);
  if (txnid)
    eASSERT(env, coherency_check_meta(env, model, true));
  if (shape) {
    if (txnid && unlikely(!coherency_check_meta(env, shape, false))) {
      ERROR("bailout overriding meta-%zu since model failed "
            "FreeDB/MainDB %s-check for txnid #%" PRIaTXN,
            target, "pre", constmeta_txnid(shape));
      return MDBX_PROBLEM;
    }
    if (globals.runtime_flags & MDBX_DBG_DONT_UPGRADE)
      memcpy(&model->magic_and_version, &shape->magic_and_version,
             sizeof(model->magic_and_version));
    model->reserve16 = shape->reserve16;
    model->validator_id = shape->validator_id;
    model->extra_pagehdr = shape->extra_pagehdr;
    memcpy(&model->geometry, &shape->geometry, sizeof(model->geometry));
    memcpy(&model->trees, &shape->trees, sizeof(model->trees));
    memcpy(&model->canary, &shape->canary, sizeof(model->canary));
    memcpy(&model->pages_retired, &shape->pages_retired,
           sizeof(model->pages_retired));
    if (txnid) {
      if ((!model->trees.gc.mod_txnid && model->trees.gc.root != P_INVALID) ||
          (!model->trees.main.mod_txnid && model->trees.main.root != P_INVALID))
        memcpy(&model->magic_and_version, &shape->magic_and_version,
               sizeof(model->magic_and_version));
      if (unlikely(!coherency_check_meta(env, model, false))) {
        ERROR("bailout overriding meta-%zu since model failed "
              "FreeDB/MainDB %s-check for txnid #%" PRIaTXN,
              target, "post", txnid);
        return MDBX_PROBLEM;
      }
    }
  }

  if (target == 0 && (model->dxbid.x | model->dxbid.y) == 0) {
    const bin128_t guid = osal_guid(env);
    memcpy(&model->dxbid, &guid, sizeof(model->dxbid));
  }

  meta_sign_as_steady(model);
  int rc = meta_validate(env, model, page, (pgno_t)target, nullptr);
  if (unlikely(MDBX_IS_ERROR(rc)))
    return MDBX_PROBLEM;

  if (shape && memcmp(model, shape, sizeof(meta_t)) == 0) {
    NOTICE("skip overriding meta-%zu since no changes "
           "for txnid #%" PRIaTXN,
           target, txnid);
    return MDBX_SUCCESS;
  }

  if (env->flags & MDBX_WRITEMAP) {
#if MDBX_ENABLE_PGOP_STAT
    env->lck->pgops.msync.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
    rc = osal_msync(&env->dxb_mmap, 0,
                    pgno_align2os_bytes(env, model->geometry.first_unallocated),
                    MDBX_SYNC_DATA | MDBX_SYNC_IODQ);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    /* meta_override() called only while current process have exclusive
     * lock of a DB file. So meta-page could be updated directly without
     * clearing consistency flag by mdbx_meta_update_begin() */
    memcpy(pgno2page(env, target), page, env->ps);
    osal_flush_incoherent_cpu_writeback();
#if MDBX_ENABLE_PGOP_STAT
    env->lck->pgops.msync.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
    rc = osal_msync(&env->dxb_mmap, 0, pgno_align2os_bytes(env, target + 1),
                    MDBX_SYNC_DATA | MDBX_SYNC_IODQ);
  } else {
#if MDBX_ENABLE_PGOP_STAT
    env->lck->pgops.wops.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
    rc = osal_pwrite(env->fd4meta, page, env->ps, pgno2bytes(env, target));
    if (rc == MDBX_SUCCESS && env->fd4meta == env->lazy_fd) {
#if MDBX_ENABLE_PGOP_STAT
      env->lck->pgops.fsync.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
      rc = osal_fsync(env->lazy_fd, MDBX_SYNC_DATA | MDBX_SYNC_IODQ);
    }
    osal_flush_incoherent_mmap(env->dxb_mmap.base, pgno2bytes(env, NUM_METAS),
                               globals.sys_pagesize);
  }
  eASSERT(env, (!env->txn && (env->flags & ENV_ACTIVE) == 0) ||
                   (env->stuck_meta == (int)target &&
                    (env->flags & (MDBX_EXCLUSIVE | MDBX_RDONLY)) ==
                        MDBX_EXCLUSIVE));
  return rc;
}

__cold int meta_validate(MDBX_env *env, meta_t *const meta,
                         const page_t *const page, const unsigned meta_number,
                         unsigned *guess_pagesize) {
  const uint64_t magic_and_version =
      unaligned_peek_u64(4, &meta->magic_and_version);
  if (unlikely(magic_and_version != MDBX_DATA_MAGIC &&
               magic_and_version != MDBX_DATA_MAGIC_LEGACY_COMPAT &&
               magic_and_version != MDBX_DATA_MAGIC_LEGACY_DEVEL)) {
    ERROR("meta[%u] has invalid magic/version %" PRIx64, meta_number,
          magic_and_version);
    return ((magic_and_version >> 8) != MDBX_MAGIC) ? MDBX_INVALID
                                                    : MDBX_VERSION_MISMATCH;
  }

  if (unlikely(page->pgno != meta_number)) {
    ERROR("meta[%u] has invalid pageno %" PRIaPGNO, meta_number, page->pgno);
    return MDBX_INVALID;
  }

  if (unlikely(page->flags != P_META)) {
    ERROR("page #%u not a meta-page", meta_number);
    return MDBX_INVALID;
  }

  if (unlikely(!is_powerof2(meta->pagesize) ||
               meta->pagesize < MDBX_MIN_PAGESIZE ||
               meta->pagesize > MDBX_MAX_PAGESIZE)) {
    WARNING("meta[%u] has invalid pagesize (%u), skip it", meta_number,
            meta->pagesize);
    return is_powerof2(meta->pagesize) ? MDBX_VERSION_MISMATCH : MDBX_INVALID;
  }

  if (guess_pagesize && *guess_pagesize != meta->pagesize) {
    *guess_pagesize = meta->pagesize;
    VERBOSE("meta[%u] took pagesize %u", meta_number, meta->pagesize);
  }

  const txnid_t txnid = unaligned_peek_u64(4, &meta->txnid_a);
  if (unlikely(txnid != unaligned_peek_u64(4, &meta->txnid_b))) {
    WARNING("meta[%u] not completely updated, skip it", meta_number);
    return MDBX_RESULT_TRUE;
  }

  /* LY: check signature as a checksum */
  const uint64_t sign = meta_sign_get(meta);
  const uint64_t sign_stready = meta_sign_calculate(meta);
  if (SIGN_IS_STEADY(sign) && unlikely(sign != sign_stready)) {
    WARNING("meta[%u] has invalid steady-checksum (0x%" PRIx64 " != 0x%" PRIx64
            "), skip it",
            meta_number, sign, sign_stready);
    return MDBX_RESULT_TRUE;
  }

  if (unlikely(meta->trees.gc.flags != MDBX_INTEGERKEY) &&
      ((meta->trees.gc.flags & DB_PERSISTENT_FLAGS) != MDBX_INTEGERKEY ||
       magic_and_version == MDBX_DATA_MAGIC)) {
    WARNING("meta[%u] has invalid %s flags 0x%x, skip it", meta_number,
            "GC/FreeDB", meta->trees.gc.flags);
    return MDBX_INCOMPATIBLE;
  }

  if (unlikely(!check_table_flags(meta->trees.main.flags))) {
    WARNING("meta[%u] has invalid %s flags 0x%x, skip it", meta_number,
            "MainDB", meta->trees.main.flags);
    return MDBX_INCOMPATIBLE;
  }

  DEBUG("checking meta%" PRIaPGNO " = root %" PRIaPGNO "/%" PRIaPGNO
        ", geo %" PRIaPGNO "/%" PRIaPGNO "-%" PRIaPGNO "/%" PRIaPGNO
        " +%u -%u, txn_id %" PRIaTXN ", %s",
        page->pgno, meta->trees.main.root, meta->trees.gc.root,
        meta->geometry.lower, meta->geometry.first_unallocated,
        meta->geometry.now, meta->geometry.upper,
        pv2pages(meta->geometry.grow_pv), pv2pages(meta->geometry.shrink_pv),
        txnid, durable_caption(meta));

  if (unlikely(txnid < MIN_TXNID || txnid > MAX_TXNID)) {
    WARNING("meta[%u] has invalid txnid %" PRIaTXN ", skip it", meta_number,
            txnid);
    return MDBX_RESULT_TRUE;
  }

  if (unlikely(meta->geometry.lower < MIN_PAGENO ||
               meta->geometry.lower > MAX_PAGENO + 1)) {
    WARNING("meta[%u] has invalid min-pages (%" PRIaPGNO "), skip it",
            meta_number, meta->geometry.lower);
    return MDBX_INVALID;
  }

  if (unlikely(meta->geometry.upper < MIN_PAGENO ||
               meta->geometry.upper > MAX_PAGENO + 1 ||
               meta->geometry.upper < meta->geometry.lower)) {
    WARNING("meta[%u] has invalid max-pages (%" PRIaPGNO "), skip it",
            meta_number, meta->geometry.upper);
    return MDBX_INVALID;
  }

  if (unlikely(meta->geometry.first_unallocated < MIN_PAGENO ||
               meta->geometry.first_unallocated - 1 > MAX_PAGENO)) {
    WARNING("meta[%u] has invalid next-pageno (%" PRIaPGNO "), skip it",
            meta_number, meta->geometry.first_unallocated);
    return MDBX_CORRUPTED;
  }

  const uint64_t used_bytes =
      meta->geometry.first_unallocated * (uint64_t)meta->pagesize;
  if (unlikely(used_bytes > env->dxb_mmap.filesize)) {
    /* Here could be a race with DB-shrinking performed by other process */
    int err = osal_filesize(env->lazy_fd, &env->dxb_mmap.filesize);
    if (unlikely(err != MDBX_SUCCESS))
      return err;
    if (unlikely(used_bytes > env->dxb_mmap.filesize)) {
      WARNING("meta[%u] used-bytes (%" PRIu64 ") beyond filesize (%" PRIu64
              "), skip it",
              meta_number, used_bytes, env->dxb_mmap.filesize);
      return MDBX_CORRUPTED;
    }
  }
  if (unlikely(meta->geometry.first_unallocated - 1 > MAX_PAGENO ||
               used_bytes > MAX_MAPSIZE)) {
    WARNING("meta[%u] has too large used-space (%" PRIu64 "), skip it",
            meta_number, used_bytes);
    return MDBX_TOO_LARGE;
  }

  pgno_t geo_lower = meta->geometry.lower;
  uint64_t mapsize_min = geo_lower * (uint64_t)meta->pagesize;
  STATIC_ASSERT(MAX_MAPSIZE < PTRDIFF_MAX - MDBX_MAX_PAGESIZE);
  STATIC_ASSERT(MIN_MAPSIZE < MAX_MAPSIZE);
  STATIC_ASSERT((uint64_t)(MAX_PAGENO + 1) * MDBX_MIN_PAGESIZE % (4ul << 20) ==
                0);
  if (unlikely(mapsize_min < MIN_MAPSIZE || mapsize_min > MAX_MAPSIZE)) {
    if (MAX_MAPSIZE != MAX_MAPSIZE64 && mapsize_min > MAX_MAPSIZE &&
        mapsize_min <= MAX_MAPSIZE64) {
      eASSERT(env, meta->geometry.first_unallocated - 1 <= MAX_PAGENO &&
                       used_bytes <= MAX_MAPSIZE);
      WARNING("meta[%u] has too large min-mapsize (%" PRIu64 "), "
              "but size of used space still acceptable (%" PRIu64 ")",
              meta_number, mapsize_min, used_bytes);
      geo_lower = (pgno_t)((mapsize_min = MAX_MAPSIZE) / meta->pagesize);
      if (geo_lower > MAX_PAGENO + 1) {
        geo_lower = MAX_PAGENO + 1;
        mapsize_min = geo_lower * (uint64_t)meta->pagesize;
      }
      WARNING("meta[%u] consider get-%s pageno is %" PRIaPGNO
              " instead of wrong %" PRIaPGNO
              ", will be corrected on next commit(s)",
              meta_number, "lower", geo_lower, meta->geometry.lower);
      meta->geometry.lower = geo_lower;
    } else {
      WARNING("meta[%u] has invalid min-mapsize (%" PRIu64 "), skip it",
              meta_number, mapsize_min);
      return MDBX_VERSION_MISMATCH;
    }
  }

  pgno_t geo_upper = meta->geometry.upper;
  uint64_t mapsize_max = geo_upper * (uint64_t)meta->pagesize;
  STATIC_ASSERT(MIN_MAPSIZE < MAX_MAPSIZE);
  if (unlikely(mapsize_max > MAX_MAPSIZE ||
               (MAX_PAGENO + 1) <
                   ceil_powerof2((size_t)mapsize_max, globals.sys_pagesize) /
                       (size_t)meta->pagesize)) {
    if (mapsize_max > MAX_MAPSIZE64) {
      WARNING("meta[%u] has invalid max-mapsize (%" PRIu64 "), skip it",
              meta_number, mapsize_max);
      return MDBX_VERSION_MISMATCH;
    }
    /* allow to open large DB from a 32-bit environment */
    eASSERT(env, meta->geometry.first_unallocated - 1 <= MAX_PAGENO &&
                     used_bytes <= MAX_MAPSIZE);
    WARNING("meta[%u] has too large max-mapsize (%" PRIu64 "), "
            "but size of used space still acceptable (%" PRIu64 ")",
            meta_number, mapsize_max, used_bytes);
    geo_upper = (pgno_t)((mapsize_max = MAX_MAPSIZE) / meta->pagesize);
    if (geo_upper > MAX_PAGENO + 1) {
      geo_upper = MAX_PAGENO + 1;
      mapsize_max = geo_upper * (uint64_t)meta->pagesize;
    }
    WARNING("meta[%u] consider get-%s pageno is %" PRIaPGNO
            " instead of wrong %" PRIaPGNO
            ", will be corrected on next commit(s)",
            meta_number, "upper", geo_upper, meta->geometry.upper);
    meta->geometry.upper = geo_upper;
  }

  /* LY: check and silently put geometry.now into [geo.lower...geo.upper].
   *
   * Copy-with-compaction by old version of libmdbx could produce DB-file
   * less than meta.geo.lower bound, in case actual filling is low or no data
   * at all. This is not a problem as there is no damage or loss of data.
   * Therefore it is better not to consider such situation as an error, but
   * silently correct it. */
  pgno_t geo_now = meta->geometry.now;
  if (geo_now < geo_lower)
    geo_now = geo_lower;
  if (geo_now > geo_upper && meta->geometry.first_unallocated <= geo_upper)
    geo_now = geo_upper;

  if (unlikely(meta->geometry.first_unallocated > geo_now)) {
    WARNING("meta[%u] next-pageno (%" PRIaPGNO
            ") is beyond end-pgno (%" PRIaPGNO "), skip it",
            meta_number, meta->geometry.first_unallocated, geo_now);
    return MDBX_CORRUPTED;
  }
  if (meta->geometry.now != geo_now) {
    WARNING("meta[%u] consider geo-%s pageno is %" PRIaPGNO
            " instead of wrong %" PRIaPGNO
            ", will be corrected on next commit(s)",
            meta_number, "now", geo_now, meta->geometry.now);
    meta->geometry.now = geo_now;
  }

  /* GC */
  if (meta->trees.gc.root == P_INVALID) {
    if (unlikely(meta->trees.gc.branch_pages || meta->trees.gc.height ||
                 meta->trees.gc.items || meta->trees.gc.leaf_pages ||
                 meta->trees.gc.large_pages)) {
      WARNING("meta[%u] has false-empty %s, skip it", meta_number, "GC");
      return MDBX_CORRUPTED;
    }
  } else if (unlikely(meta->trees.gc.root >=
                      meta->geometry.first_unallocated)) {
    WARNING("meta[%u] has invalid %s-root %" PRIaPGNO ", skip it", meta_number,
            "GC", meta->trees.gc.root);
    return MDBX_CORRUPTED;
  }

  /* MainDB */
  if (meta->trees.main.root == P_INVALID) {
    if (unlikely(meta->trees.main.branch_pages || meta->trees.main.height ||
                 meta->trees.main.items || meta->trees.main.leaf_pages ||
                 meta->trees.main.large_pages)) {
      WARNING("meta[%u] has false-empty %s", meta_number, "MainDB");
      return MDBX_CORRUPTED;
    }
  } else if (unlikely(meta->trees.main.root >=
                      meta->geometry.first_unallocated)) {
    WARNING("meta[%u] has invalid %s-root %" PRIaPGNO ", skip it", meta_number,
            "MainDB", meta->trees.main.root);
    return MDBX_CORRUPTED;
  }

  if (unlikely(meta->trees.gc.mod_txnid > txnid)) {
    WARNING("meta[%u] has wrong mod_txnid %" PRIaTXN " for %s, skip it",
            meta_number, meta->trees.gc.mod_txnid, "GC");
    return MDBX_CORRUPTED;
  }

  if (unlikely(meta->trees.main.mod_txnid > txnid)) {
    WARNING("meta[%u] has wrong mod_txnid %" PRIaTXN " for %s, skip it",
            meta_number, meta->trees.main.mod_txnid, "MainDB");
    return MDBX_CORRUPTED;
  }

  return MDBX_SUCCESS;
}

__cold int meta_validate_copy(MDBX_env *env, const meta_t *meta, meta_t *dest) {
  *dest = *meta;
  return meta_validate(env, dest, data_page(meta),
                       bytes2pgno(env, ptr_dist(meta, env->dxb_mmap.base)),
                       nullptr);
}
