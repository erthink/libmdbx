/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024

#include "internals.h"

/*------------------------------------------------------------------------------
 * Pack/Unpack 16-bit values for Grow step & Shrink threshold */

MDBX_NOTHROW_CONST_FUNCTION static inline pgno_t me2v(size_t m, size_t e) {
  assert(m < 2048 && e < 8);
  return (pgno_t)(32768 + ((m + 1) << (e + 8)));
}

MDBX_NOTHROW_CONST_FUNCTION static inline uint16_t v2me(size_t v, size_t e) {
  assert(v > (e ? me2v(2047, e - 1) : 32768));
  assert(v <= me2v(2047, e));
  size_t m = (v - 32768 + ((size_t)1 << (e + 8)) - 1) >> (e + 8);
  m -= m > 0;
  assert(m < 2048 && e < 8);
  // f e d c b a 9 8 7 6 5 4 3 2 1 0
  // 1 e e e m m m m m m m m m m m 1
  const uint16_t pv = (uint16_t)(0x8001 + (e << 12) + (m << 1));
  assert(pv != 65535);
  return pv;
}

/* Convert 16-bit packed (exponential quantized) value to number of pages */
pgno_t pv2pages(uint16_t pv) {
  if ((pv & 0x8001) != 0x8001)
    return pv;
  if (pv == 65535)
    return 65536;
  // f e d c b a 9 8 7 6 5 4 3 2 1 0
  // 1 e e e m m m m m m m m m m m 1
  return me2v((pv >> 1) & 2047, (pv >> 12) & 7);
}

/* Convert number of pages to 16-bit packed (exponential quantized) value */
uint16_t pages2pv(size_t pages) {
  if (pages < 32769 || (pages < 65536 && (pages & 1) == 0))
    return (uint16_t)pages;
  if (pages <= me2v(2047, 0))
    return v2me(pages, 0);
  if (pages <= me2v(2047, 1))
    return v2me(pages, 1);
  if (pages <= me2v(2047, 2))
    return v2me(pages, 2);
  if (pages <= me2v(2047, 3))
    return v2me(pages, 3);
  if (pages <= me2v(2047, 4))
    return v2me(pages, 4);
  if (pages <= me2v(2047, 5))
    return v2me(pages, 5);
  if (pages <= me2v(2047, 6))
    return v2me(pages, 6);
  return (pages < me2v(2046, 7)) ? v2me(pages, 7) : 65533;
}

__cold bool pv2pages_verify(void) {
  bool ok = true, dump_translation = false;
  for (size_t i = 0; i < 65536; ++i) {
    size_t pages = pv2pages(i);
    size_t x = pages2pv(pages);
    size_t xp = pv2pages(x);
    if (pages != xp) {
      ERROR("%zu => %zu => %zu => %zu\n", i, pages, x, xp);
      ok = false;
    } else if (dump_translation && !(x == i || (x % 2 == 0 && x < 65536))) {
      DEBUG("%zu => %zu => %zu => %zu\n", i, pages, x, xp);
    }
  }
  return ok;
}

/*----------------------------------------------------------------------------*/

MDBX_NOTHROW_PURE_FUNCTION size_t bytes_align2os_bytes(const MDBX_env *env, size_t bytes) {
  return ceil_powerof2(bytes, (env->ps > globals.sys_pagesize) ? env->ps : globals.sys_pagesize);
}

MDBX_NOTHROW_PURE_FUNCTION size_t pgno_align2os_bytes(const MDBX_env *env, size_t pgno) {
  return ceil_powerof2(pgno2bytes(env, pgno), globals.sys_pagesize);
}

MDBX_NOTHROW_PURE_FUNCTION pgno_t pgno_align2os_pgno(const MDBX_env *env, size_t pgno) {
  return bytes2pgno(env, pgno_align2os_bytes(env, pgno));
}

/*----------------------------------------------------------------------------*/

MDBX_NOTHROW_PURE_FUNCTION static __always_inline int cmp_int_inline(const size_t expected_alignment, const MDBX_val *a,
                                                                     const MDBX_val *b) {
  if (likely(a->iov_len == b->iov_len)) {
    if (sizeof(size_t) > 7 && likely(a->iov_len == 8))
      return CMP2INT(unaligned_peek_u64(expected_alignment, a->iov_base),
                     unaligned_peek_u64(expected_alignment, b->iov_base));
    if (likely(a->iov_len == 4))
      return CMP2INT(unaligned_peek_u32(expected_alignment, a->iov_base),
                     unaligned_peek_u32(expected_alignment, b->iov_base));
    if (sizeof(size_t) < 8 && likely(a->iov_len == 8))
      return CMP2INT(unaligned_peek_u64(expected_alignment, a->iov_base),
                     unaligned_peek_u64(expected_alignment, b->iov_base));
  }
  ERROR("mismatch and/or invalid size %p.%zu/%p.%zu for INTEGERKEY/INTEGERDUP", a->iov_base, a->iov_len, b->iov_base,
        b->iov_len);
  return 0;
}

MDBX_NOTHROW_PURE_FUNCTION __hot int cmp_int_unaligned(const MDBX_val *a, const MDBX_val *b) {
  return cmp_int_inline(1, a, b);
}

#ifndef cmp_int_align2
/* Compare two items pointing at 2-byte aligned unsigned int's. */
MDBX_NOTHROW_PURE_FUNCTION __hot int cmp_int_align2(const MDBX_val *a, const MDBX_val *b) {
  return cmp_int_inline(2, a, b);
}
#endif /* cmp_int_align2 */

#ifndef cmp_int_align4
/* Compare two items pointing at 4-byte aligned unsigned int's. */
MDBX_NOTHROW_PURE_FUNCTION __hot int cmp_int_align4(const MDBX_val *a, const MDBX_val *b) {
  return cmp_int_inline(4, a, b);
}
#endif /* cmp_int_align4 */

/* Compare two items lexically */
MDBX_NOTHROW_PURE_FUNCTION __hot int cmp_lexical(const MDBX_val *a, const MDBX_val *b) {
  if (a->iov_len == b->iov_len)
    return a->iov_len ? memcmp(a->iov_base, b->iov_base, a->iov_len) : 0;

  const int diff_len = (a->iov_len < b->iov_len) ? -1 : 1;
  const size_t shortest = (a->iov_len < b->iov_len) ? a->iov_len : b->iov_len;
  int diff_data = shortest ? memcmp(a->iov_base, b->iov_base, shortest) : 0;
  return likely(diff_data) ? diff_data : diff_len;
}

MDBX_NOTHROW_PURE_FUNCTION static __always_inline unsigned tail3le(const uint8_t *p, size_t l) {
  STATIC_ASSERT(sizeof(unsigned) > 2);
  // 1: 0 0 0
  // 2: 0 1 1
  // 3: 0 1 2
  return p[0] | p[l >> 1] << 8 | p[l - 1] << 16;
}

/* Compare two items in reverse byte order */
MDBX_NOTHROW_PURE_FUNCTION __hot int cmp_reverse(const MDBX_val *a, const MDBX_val *b) {
  size_t left = (a->iov_len < b->iov_len) ? a->iov_len : b->iov_len;
  if (likely(left)) {
    const uint8_t *pa = ptr_disp(a->iov_base, a->iov_len);
    const uint8_t *pb = ptr_disp(b->iov_base, b->iov_len);
    while (left >= sizeof(size_t)) {
      pa -= sizeof(size_t);
      pb -= sizeof(size_t);
      left -= sizeof(size_t);
      STATIC_ASSERT(sizeof(size_t) == 4 || sizeof(size_t) == 8);
      if (sizeof(size_t) == 4) {
        uint32_t xa = unaligned_peek_u32(1, pa);
        uint32_t xb = unaligned_peek_u32(1, pb);
#if __BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__
        xa = osal_bswap32(xa);
        xb = osal_bswap32(xb);
#endif /* __BYTE_ORDER__ != __ORDER_BIG_ENDIAN__ */
        if (xa != xb)
          return (xa < xb) ? -1 : 1;
      } else {
        uint64_t xa = unaligned_peek_u64(1, pa);
        uint64_t xb = unaligned_peek_u64(1, pb);
#if __BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__
        xa = osal_bswap64(xa);
        xb = osal_bswap64(xb);
#endif /* __BYTE_ORDER__ != __ORDER_BIG_ENDIAN__ */
        if (xa != xb)
          return (xa < xb) ? -1 : 1;
      }
    }
    if (sizeof(size_t) == 8 && left >= 4) {
      pa -= 4;
      pb -= 4;
      left -= 4;
      uint32_t xa = unaligned_peek_u32(1, pa);
      uint32_t xb = unaligned_peek_u32(1, pb);
#if __BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__
      xa = osal_bswap32(xa);
      xb = osal_bswap32(xb);
#endif /* __BYTE_ORDER__ != __ORDER_BIG_ENDIAN__ */
      if (xa != xb)
        return (xa < xb) ? -1 : 1;
    }
    if (left) {
      unsigned xa = tail3le(pa - left, left);
      unsigned xb = tail3le(pb - left, left);
      if (xa != xb)
        return (xa < xb) ? -1 : 1;
    }
  }
  return CMP2INT(a->iov_len, b->iov_len);
}

/* Fast non-lexically comparator */
MDBX_NOTHROW_PURE_FUNCTION __hot int cmp_lenfast(const MDBX_val *a, const MDBX_val *b) {
  int diff = CMP2INT(a->iov_len, b->iov_len);
  return (likely(diff) || a->iov_len == 0) ? diff : memcmp(a->iov_base, b->iov_base, a->iov_len);
}

MDBX_NOTHROW_PURE_FUNCTION __hot bool eq_fast_slowpath(const uint8_t *a, const uint8_t *b, size_t l) {
  if (likely(l > 3)) {
    if (MDBX_UNALIGNED_OK >= 4 && likely(l < 9))
      return ((unaligned_peek_u32(1, a) - unaligned_peek_u32(1, b)) |
              (unaligned_peek_u32(1, a + l - 4) - unaligned_peek_u32(1, b + l - 4))) == 0;
    if (MDBX_UNALIGNED_OK >= 8 && sizeof(size_t) > 7 && likely(l < 17))
      return ((unaligned_peek_u64(1, a) - unaligned_peek_u64(1, b)) |
              (unaligned_peek_u64(1, a + l - 8) - unaligned_peek_u64(1, b + l - 8))) == 0;
    return memcmp(a, b, l) == 0;
  }
  if (likely(l))
    return tail3le(a, l) == tail3le(b, l);
  return true;
}

int cmp_equal_or_greater(const MDBX_val *a, const MDBX_val *b) { return eq_fast(a, b) ? 0 : 1; }

int cmp_equal_or_wrong(const MDBX_val *a, const MDBX_val *b) { return eq_fast(a, b) ? 0 : -1; }

/*----------------------------------------------------------------------------*/

__cold void update_mlcnt(const MDBX_env *env, const pgno_t new_aligned_mlocked_pgno, const bool lock_not_release) {
  for (;;) {
    const pgno_t mlock_pgno_before = atomic_load32(&env->mlocked_pgno, mo_AcquireRelease);
    eASSERT(env, pgno_align2os_pgno(env, mlock_pgno_before) == mlock_pgno_before);
    eASSERT(env, pgno_align2os_pgno(env, new_aligned_mlocked_pgno) == new_aligned_mlocked_pgno);
    if (lock_not_release ? (mlock_pgno_before >= new_aligned_mlocked_pgno)
                         : (mlock_pgno_before <= new_aligned_mlocked_pgno))
      break;
    if (likely(atomic_cas32(&((MDBX_env *)env)->mlocked_pgno, mlock_pgno_before, new_aligned_mlocked_pgno)))
      for (;;) {
        mdbx_atomic_uint32_t *const mlcnt = env->lck->mlcnt;
        const int32_t snap_locked = atomic_load32(mlcnt + 0, mo_Relaxed);
        const int32_t snap_unlocked = atomic_load32(mlcnt + 1, mo_Relaxed);
        if (mlock_pgno_before == 0 && (snap_locked - snap_unlocked) < INT_MAX) {
          eASSERT(env, lock_not_release);
          if (unlikely(!atomic_cas32(mlcnt + 0, snap_locked, snap_locked + 1)))
            continue;
        }
        if (new_aligned_mlocked_pgno == 0 && (snap_locked - snap_unlocked) > 0) {
          eASSERT(env, !lock_not_release);
          if (unlikely(!atomic_cas32(mlcnt + 1, snap_unlocked, snap_unlocked + 1)))
            continue;
        }
        NOTICE("%s-pages %u..%u, mlocked-process(es) %u -> %u", lock_not_release ? "lock" : "unlock",
               lock_not_release ? mlock_pgno_before : new_aligned_mlocked_pgno,
               lock_not_release ? new_aligned_mlocked_pgno : mlock_pgno_before, snap_locked - snap_unlocked,
               atomic_load32(mlcnt + 0, mo_Relaxed) - atomic_load32(mlcnt + 1, mo_Relaxed));
        return;
      }
  }
}

__cold void munlock_after(const MDBX_env *env, const pgno_t aligned_pgno, const size_t end_bytes) {
  if (atomic_load32(&env->mlocked_pgno, mo_AcquireRelease) > aligned_pgno) {
    int err = MDBX_ENOSYS;
    const size_t munlock_begin = pgno2bytes(env, aligned_pgno);
    const size_t munlock_size = end_bytes - munlock_begin;
    eASSERT(env, end_bytes % globals.sys_pagesize == 0 && munlock_begin % globals.sys_pagesize == 0 &&
                     munlock_size % globals.sys_pagesize == 0);
#if defined(_WIN32) || defined(_WIN64)
    err = VirtualUnlock(ptr_disp(env->dxb_mmap.base, munlock_begin), munlock_size) ? MDBX_SUCCESS : (int)GetLastError();
    if (err == ERROR_NOT_LOCKED)
      err = MDBX_SUCCESS;
#elif defined(_POSIX_MEMLOCK_RANGE)
    err = munlock(ptr_disp(env->dxb_mmap.base, munlock_begin), munlock_size) ? errno : MDBX_SUCCESS;
#endif
    if (likely(err == MDBX_SUCCESS))
      update_mlcnt(env, aligned_pgno, false);
    else {
#if defined(_WIN32) || defined(_WIN64)
      WARNING("VirtualUnlock(%zu, %zu) error %d", munlock_begin, munlock_size, err);
#else
      WARNING("munlock(%zu, %zu) error %d", munlock_begin, munlock_size, err);
#endif
    }
  }
}

__cold void munlock_all(const MDBX_env *env) {
  munlock_after(env, 0, bytes_align2os_bytes(env, env->dxb_mmap.current));
}

/*----------------------------------------------------------------------------*/

uint32_t combine_durability_flags(const uint32_t a, const uint32_t b) {
  uint32_t r = a | b;

  /* avoid false MDBX_UTTERLY_NOSYNC */
  if (F_ISSET(r, MDBX_UTTERLY_NOSYNC) && !F_ISSET(a, MDBX_UTTERLY_NOSYNC) && !F_ISSET(b, MDBX_UTTERLY_NOSYNC))
    r = (r - MDBX_UTTERLY_NOSYNC) | MDBX_SAFE_NOSYNC;

  /* convert DEPRECATED_MAPASYNC to MDBX_SAFE_NOSYNC */
  if ((r & (MDBX_WRITEMAP | DEPRECATED_MAPASYNC)) == (MDBX_WRITEMAP | DEPRECATED_MAPASYNC) &&
      !F_ISSET(r, MDBX_UTTERLY_NOSYNC))
    r = (r - DEPRECATED_MAPASYNC) | MDBX_SAFE_NOSYNC;

  /* force MDBX_NOMETASYNC if NOSYNC enabled */
  if (r & (MDBX_SAFE_NOSYNC | MDBX_UTTERLY_NOSYNC))
    r |= MDBX_NOMETASYNC;

  assert(!(F_ISSET(r, MDBX_UTTERLY_NOSYNC) && !F_ISSET(a, MDBX_UTTERLY_NOSYNC) && !F_ISSET(b, MDBX_UTTERLY_NOSYNC)));
  return r;
}
