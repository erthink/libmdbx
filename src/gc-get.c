/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025

#include "internals.h"

#if MDBX_USE_MINCORE
/*------------------------------------------------------------------------------
 * Проверка размещения/расположения отображенных страниц БД в ОЗУ (mem-in-core),
 * с кешированием этой информации. */

static inline bool bit_tas(uint64_t *field, char bit) {
  const uint64_t m = UINT64_C(1) << bit;
  const bool r = (*field & m) != 0;
  *field |= m;
  return r;
}

static bool mincore_fetch(MDBX_env *const env, const size_t unit_begin) {
  lck_t *const lck = env->lck;
  for (size_t i = 1; i < ARRAY_LENGTH(lck->mincore_cache.begin); ++i) {
    const ptrdiff_t dist = unit_begin - lck->mincore_cache.begin[i];
    if (likely(dist >= 0 && dist < 64)) {
      const pgno_t tmp_begin = lck->mincore_cache.begin[i];
      const uint64_t tmp_mask = lck->mincore_cache.mask[i];
      do {
        lck->mincore_cache.begin[i] = lck->mincore_cache.begin[i - 1];
        lck->mincore_cache.mask[i] = lck->mincore_cache.mask[i - 1];
      } while (--i);
      lck->mincore_cache.begin[0] = tmp_begin;
      lck->mincore_cache.mask[0] = tmp_mask;
      return bit_tas(lck->mincore_cache.mask, (char)dist);
    }
  }

  size_t pages = 64;
  unsigned unit_log = globals.sys_pagesize_ln2;
  unsigned shift = 0;
  if (env->ps > globals.sys_pagesize) {
    unit_log = env->ps2ln;
    shift = env->ps2ln - globals.sys_pagesize_ln2;
    pages <<= shift;
  }

  const size_t offset = unit_begin << unit_log;
  size_t length = pages << globals.sys_pagesize_ln2;
  if (offset + length > env->dxb_mmap.current) {
    length = env->dxb_mmap.current - offset;
    pages = length >> globals.sys_pagesize_ln2;
  }

#if MDBX_ENABLE_PGOP_STAT
  env->lck->pgops.mincore.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
  uint8_t *const vector = alloca(pages);
  if (unlikely(mincore(ptr_disp(env->dxb_mmap.base, offset), length, (void *)vector))) {
    NOTICE("mincore(+%zu, %zu), err %d", offset, length, errno);
    return false;
  }

  for (size_t i = 1; i < ARRAY_LENGTH(lck->mincore_cache.begin); ++i) {
    lck->mincore_cache.begin[i] = lck->mincore_cache.begin[i - 1];
    lck->mincore_cache.mask[i] = lck->mincore_cache.mask[i - 1];
  }
  lck->mincore_cache.begin[0] = unit_begin;

  uint64_t mask = 0;
#ifdef MINCORE_INCORE
  STATIC_ASSERT(MINCORE_INCORE == 1);
#endif
  for (size_t i = 0; i < pages; ++i) {
    uint64_t bit = (vector[i] & 1) == 0;
    bit <<= i >> shift;
    mask |= bit;
  }

  lck->mincore_cache.mask[0] = ~mask;
  return bit_tas(lck->mincore_cache.mask, 0);
}
#endif /* MDBX_USE_MINCORE */

MDBX_MAYBE_UNUSED static inline bool mincore_probe(MDBX_env *const env, const pgno_t pgno) {
#if MDBX_USE_MINCORE
  const size_t offset_aligned = floor_powerof2(pgno2bytes(env, pgno), globals.sys_pagesize);
  const unsigned unit_log2 = (env->ps2ln > globals.sys_pagesize_ln2) ? env->ps2ln : globals.sys_pagesize_ln2;
  const size_t unit_begin = offset_aligned >> unit_log2;
  eASSERT(env, (unit_begin << unit_log2) == offset_aligned);
  const ptrdiff_t dist = unit_begin - env->lck->mincore_cache.begin[0];
  if (likely(dist >= 0 && dist < 64))
    return bit_tas(env->lck->mincore_cache.mask, (char)dist);
  return mincore_fetch(env, unit_begin);
#else
  (void)env;
  (void)pgno;
  return false;
#endif /* MDBX_USE_MINCORE */
}

/*----------------------------------------------------------------------------*/

MDBX_MAYBE_UNUSED __hot static pgno_t *scan4seq_fallback(pgno_t *range, const size_t len, const size_t seq) {
  assert(seq > 0 && len > seq);
#if MDBX_PNL_ASCENDING
  assert(range[-1] == len);
  const pgno_t *const detent = range + len - seq;
  const ptrdiff_t offset = (ptrdiff_t)seq;
  const pgno_t target = (pgno_t)offset;
  if (likely(len > seq + 3)) {
    do {
      const pgno_t diff0 = range[offset + 0] - range[0];
      const pgno_t diff1 = range[offset + 1] - range[1];
      const pgno_t diff2 = range[offset + 2] - range[2];
      const pgno_t diff3 = range[offset + 3] - range[3];
      if (diff0 == target)
        return range + 0;
      if (diff1 == target)
        return range + 1;
      if (diff2 == target)
        return range + 2;
      if (diff3 == target)
        return range + 3;
      range += 4;
    } while (range + 3 < detent);
    if (range == detent)
      return nullptr;
  }
  do
    if (range[offset] - *range == target)
      return range;
  while (++range < detent);
#else
  assert(range[-(ptrdiff_t)len] == len);
  const pgno_t *const detent = range - len + seq;
  const ptrdiff_t offset = -(ptrdiff_t)seq;
  const pgno_t target = (pgno_t)offset;
  if (likely(len > seq + 3)) {
    do {
      const pgno_t diff0 = range[-0] - range[offset - 0];
      const pgno_t diff1 = range[-1] - range[offset - 1];
      const pgno_t diff2 = range[-2] - range[offset - 2];
      const pgno_t diff3 = range[-3] - range[offset - 3];
      /* Смысл вычислений до ветвлений в том, чтобы позволить компилятору
       * загружать и вычислять все значения параллельно. */
      if (diff0 == target)
        return range - 0;
      if (diff1 == target)
        return range - 1;
      if (diff2 == target)
        return range - 2;
      if (diff3 == target)
        return range - 3;
      range -= 4;
    } while (range > detent + 3);
    if (range == detent)
      return nullptr;
  }
  do
    if (*range - range[offset] == target)
      return range;
  while (--range > detent);
#endif /* pnl_t sort-order */
  return nullptr;
}

MDBX_MAYBE_UNUSED static const pgno_t *scan4range_checker(const pnl_t pnl, const size_t seq) {
  size_t begin = MDBX_PNL_ASCENDING ? 1 : MDBX_PNL_GETSIZE(pnl);
#if MDBX_PNL_ASCENDING
  while (seq <= MDBX_PNL_GETSIZE(pnl) - begin) {
    if (pnl[begin + seq] - pnl[begin] == seq)
      return pnl + begin;
    ++begin;
  }
#else
  while (begin > seq) {
    if (pnl[begin - seq] - pnl[begin] == seq)
      return pnl + begin;
    --begin;
  }
#endif /* pnl_t sort-order */
  return nullptr;
}

#if defined(_MSC_VER) && !defined(__builtin_clz) && !__has_builtin(__builtin_clz)
MDBX_MAYBE_UNUSED static __always_inline size_t __builtin_clz(uint32_t value) {
  unsigned long index;
  _BitScanReverse(&index, value);
  return 31 - index;
}
#endif /* _MSC_VER */

#if defined(_MSC_VER) && !defined(__builtin_clzl) && !__has_builtin(__builtin_clzl)
MDBX_MAYBE_UNUSED static __always_inline size_t __builtin_clzl(size_t value) {
  unsigned long index;
#ifdef _WIN64
  assert(sizeof(value) == 8);
  _BitScanReverse64(&index, value);
  return 63 - index;
#else
  assert(sizeof(value) == 4);
  _BitScanReverse(&index, value);
  return 31 - index;
#endif
}
#endif /* _MSC_VER */

#if !MDBX_PNL_ASCENDING

#if !defined(MDBX_ATTRIBUTE_TARGET) && (__has_attribute(__target__) || __GNUC_PREREQ(5, 0))
#define MDBX_ATTRIBUTE_TARGET(target) __attribute__((__target__(target)))
#endif /* MDBX_ATTRIBUTE_TARGET */

#ifndef MDBX_GCC_FASTMATH_i686_SIMD_WORKAROUND
/* Workaround for GCC's bug with `-m32 -march=i686 -Ofast`
 * gcc/i686-buildroot-linux-gnu/12.2.0/include/xmmintrin.h:814:1:
 *     error: inlining failed in call to 'always_inline' '_mm_movemask_ps':
 *            target specific option mismatch */
#if !defined(__FAST_MATH__) || !__FAST_MATH__ || !defined(__GNUC__) || defined(__e2k__) || defined(__clang__) ||       \
    defined(__amd64__) || defined(__SSE2__)
#define MDBX_GCC_FASTMATH_i686_SIMD_WORKAROUND 0
#else
#define MDBX_GCC_FASTMATH_i686_SIMD_WORKAROUND 1
#endif
#endif /* MDBX_GCC_FASTMATH_i686_SIMD_WORKAROUND */

#if defined(__SSE2__) && defined(__SSE__)
#define MDBX_ATTRIBUTE_TARGET_SSE2 /* nope */
#elif (defined(_M_IX86_FP) && _M_IX86_FP >= 2) || defined(__amd64__)
#define __SSE2__
#define MDBX_ATTRIBUTE_TARGET_SSE2 /* nope */
#elif defined(MDBX_ATTRIBUTE_TARGET) && defined(__ia32__) && !MDBX_GCC_FASTMATH_i686_SIMD_WORKAROUND
#define MDBX_ATTRIBUTE_TARGET_SSE2 MDBX_ATTRIBUTE_TARGET("sse,sse2")
#endif /* __SSE2__ */

#if defined(__AVX2__)
#define MDBX_ATTRIBUTE_TARGET_AVX2 /* nope */
#elif defined(MDBX_ATTRIBUTE_TARGET) && defined(__ia32__) && !MDBX_GCC_FASTMATH_i686_SIMD_WORKAROUND
#define MDBX_ATTRIBUTE_TARGET_AVX2 MDBX_ATTRIBUTE_TARGET("sse,sse2,avx,avx2")
#endif /* __AVX2__ */

#if defined(MDBX_ATTRIBUTE_TARGET_AVX2)
#if defined(__AVX512BW__)
#define MDBX_ATTRIBUTE_TARGET_AVX512BW /* nope */
#elif defined(MDBX_ATTRIBUTE_TARGET) && defined(__ia32__) && !MDBX_GCC_FASTMATH_i686_SIMD_WORKAROUND &&                \
    (__GNUC_PREREQ(6, 0) || __CLANG_PREREQ(5, 0))
#define MDBX_ATTRIBUTE_TARGET_AVX512BW MDBX_ATTRIBUTE_TARGET("sse,sse2,avx,avx2,avx512bw")
#endif /* __AVX512BW__ */
#endif /* MDBX_ATTRIBUTE_TARGET_AVX2 for MDBX_ATTRIBUTE_TARGET_AVX512BW */

#ifdef MDBX_ATTRIBUTE_TARGET_SSE2
MDBX_ATTRIBUTE_TARGET_SSE2 static __always_inline unsigned
diffcmp2mask_sse2(const pgno_t *const ptr, const ptrdiff_t offset, const __m128i pattern) {
  const __m128i f = _mm_loadu_si128((const __m128i *)ptr);
  const __m128i l = _mm_loadu_si128((const __m128i *)(ptr + offset));
  const __m128i cmp = _mm_cmpeq_epi32(_mm_sub_epi32(f, l), pattern);
  return _mm_movemask_ps(*(const __m128 *)&cmp);
}

MDBX_MAYBE_UNUSED __hot MDBX_ATTRIBUTE_TARGET_SSE2 static pgno_t *scan4seq_sse2(pgno_t *range, const size_t len,
                                                                                const size_t seq) {
  assert(seq > 0 && len > seq);
#if MDBX_PNL_ASCENDING
#error "FIXME: Not implemented"
#endif /* MDBX_PNL_ASCENDING */
  assert(range[-(ptrdiff_t)len] == len);
  pgno_t *const detent = range - len + seq;
  const ptrdiff_t offset = -(ptrdiff_t)seq;
  const pgno_t target = (pgno_t)offset;
  const __m128i pattern = _mm_set1_epi32(target);
  uint8_t mask;
  if (likely(len > seq + 3)) {
    do {
      mask = (uint8_t)diffcmp2mask_sse2(range - 3, offset, pattern);
      if (mask) {
#if !defined(ENABLE_MEMCHECK) && !defined(__SANITIZE_ADDRESS__)
      found:
#endif /* !ENABLE_MEMCHECK && !__SANITIZE_ADDRESS__ */
        return range + 28 - __builtin_clz(mask);
      }
      range -= 4;
    } while (range > detent + 3);
    if (range == detent)
      return nullptr;
  }

  /* Далее происходит чтение от 4 до 12 лишних байт, которые могут быть не
   * только за пределами региона выделенного под PNL, но и пересекать границу
   * страницы памяти. Что может приводить как к ошибкам ASAN, так и к падению.
   * Поэтому проверяем смещение на странице, а с ASAN всегда страхуемся. */
#if !defined(ENABLE_MEMCHECK) && !defined(__SANITIZE_ADDRESS__)
  const unsigned on_page_safe_mask = 0xff0 /* enough for '-15' bytes offset */;
  if (likely(on_page_safe_mask & (uintptr_t)(range + offset)) && !RUNNING_ON_VALGRIND) {
    const unsigned extra = (unsigned)(detent + 4 - range);
    assert(extra > 0 && extra < 4);
    mask = 0xF << extra;
    mask &= diffcmp2mask_sse2(range - 3, offset, pattern);
    if (mask)
      goto found;
    return nullptr;
  }
#endif /* !ENABLE_MEMCHECK && !__SANITIZE_ADDRESS__ */
  do
    if (*range - range[offset] == target)
      return range;
  while (--range != detent);
  return nullptr;
}
#endif /* MDBX_ATTRIBUTE_TARGET_SSE2 */

#ifdef MDBX_ATTRIBUTE_TARGET_AVX2
MDBX_ATTRIBUTE_TARGET_AVX2 static __always_inline unsigned
diffcmp2mask_avx2(const pgno_t *const ptr, const ptrdiff_t offset, const __m256i pattern) {
  const __m256i f = _mm256_loadu_si256((const __m256i *)ptr);
  const __m256i l = _mm256_loadu_si256((const __m256i *)(ptr + offset));
  const __m256i cmp = _mm256_cmpeq_epi32(_mm256_sub_epi32(f, l), pattern);
  return _mm256_movemask_ps(*(const __m256 *)&cmp);
}

MDBX_ATTRIBUTE_TARGET_AVX2 static __always_inline unsigned
diffcmp2mask_sse2avx(const pgno_t *const ptr, const ptrdiff_t offset, const __m128i pattern) {
  const __m128i f = _mm_loadu_si128((const __m128i *)ptr);
  const __m128i l = _mm_loadu_si128((const __m128i *)(ptr + offset));
  const __m128i cmp = _mm_cmpeq_epi32(_mm_sub_epi32(f, l), pattern);
  return _mm_movemask_ps(*(const __m128 *)&cmp);
}

MDBX_MAYBE_UNUSED __hot MDBX_ATTRIBUTE_TARGET_AVX2 static pgno_t *scan4seq_avx2(pgno_t *range, const size_t len,
                                                                                const size_t seq) {
  assert(seq > 0 && len > seq);
#if MDBX_PNL_ASCENDING
#error "FIXME: Not implemented"
#endif /* MDBX_PNL_ASCENDING */
  assert(range[-(ptrdiff_t)len] == len);
  pgno_t *const detent = range - len + seq;
  const ptrdiff_t offset = -(ptrdiff_t)seq;
  const pgno_t target = (pgno_t)offset;
  const __m256i pattern = _mm256_set1_epi32(target);
  uint8_t mask;
  if (likely(len > seq + 7)) {
    do {
      mask = (uint8_t)diffcmp2mask_avx2(range - 7, offset, pattern);
      if (mask) {
#if !defined(ENABLE_MEMCHECK) && !defined(__SANITIZE_ADDRESS__)
      found:
#endif /* !ENABLE_MEMCHECK && !__SANITIZE_ADDRESS__ */
        return range + 24 - __builtin_clz(mask);
      }
      range -= 8;
    } while (range > detent + 7);
    if (range == detent)
      return nullptr;
  }

  /* Далее происходит чтение от 4 до 28 лишних байт, которые могут быть не
   * только за пределами региона выделенного под PNL, но и пересекать границу
   * страницы памяти. Что может приводить как к ошибкам ASAN, так и к падению.
   * Поэтому проверяем смещение на странице, а с ASAN всегда страхуемся. */
#if !defined(ENABLE_MEMCHECK) && !defined(__SANITIZE_ADDRESS__)
  const unsigned on_page_safe_mask = 0xfe0 /* enough for '-31' bytes offset */;
  if (likely(on_page_safe_mask & (uintptr_t)(range + offset)) && !RUNNING_ON_VALGRIND) {
    const unsigned extra = (unsigned)(detent + 8 - range);
    assert(extra > 0 && extra < 8);
    mask = 0xFF << extra;
    mask &= diffcmp2mask_avx2(range - 7, offset, pattern);
    if (mask)
      goto found;
    return nullptr;
  }
#endif /* !ENABLE_MEMCHECK && !__SANITIZE_ADDRESS__ */
  if (range - 3 > detent) {
    mask = diffcmp2mask_sse2avx(range - 3, offset, *(const __m128i *)&pattern);
    if (mask)
      return range + 28 - __builtin_clz(mask);
    range -= 4;
  }
  while (range > detent) {
    if (*range - range[offset] == target)
      return range;
    --range;
  }
  return nullptr;
}
#endif /* MDBX_ATTRIBUTE_TARGET_AVX2 */

#ifdef MDBX_ATTRIBUTE_TARGET_AVX512BW
MDBX_ATTRIBUTE_TARGET_AVX512BW static __always_inline unsigned
diffcmp2mask_avx512bw(const pgno_t *const ptr, const ptrdiff_t offset, const __m512i pattern) {
  const __m512i f = _mm512_loadu_si512((const __m512i *)ptr);
  const __m512i l = _mm512_loadu_si512((const __m512i *)(ptr + offset));
  return _mm512_cmpeq_epi32_mask(_mm512_sub_epi32(f, l), pattern);
}

MDBX_MAYBE_UNUSED __hot MDBX_ATTRIBUTE_TARGET_AVX512BW static pgno_t *scan4seq_avx512bw(pgno_t *range, const size_t len,
                                                                                        const size_t seq) {
  assert(seq > 0 && len > seq);
#if MDBX_PNL_ASCENDING
#error "FIXME: Not implemented"
#endif /* MDBX_PNL_ASCENDING */
  assert(range[-(ptrdiff_t)len] == len);
  pgno_t *const detent = range - len + seq;
  const ptrdiff_t offset = -(ptrdiff_t)seq;
  const pgno_t target = (pgno_t)offset;
  const __m512i pattern = _mm512_set1_epi32(target);
  unsigned mask;
  if (likely(len > seq + 15)) {
    do {
      mask = diffcmp2mask_avx512bw(range - 15, offset, pattern);
      if (mask) {
#if !defined(ENABLE_MEMCHECK) && !defined(__SANITIZE_ADDRESS__)
      found:
#endif /* !ENABLE_MEMCHECK && !__SANITIZE_ADDRESS__ */
        return range + 16 - __builtin_clz(mask);
      }
      range -= 16;
    } while (range > detent + 15);
    if (range == detent)
      return nullptr;
  }

  /* Далее происходит чтение от 4 до 60 лишних байт, которые могут быть не
   * только за пределами региона выделенного под PNL, но и пересекать границу
   * страницы памяти. Что может приводить как к ошибкам ASAN, так и к падению.
   * Поэтому проверяем смещение на странице, а с ASAN всегда страхуемся. */
#if !defined(ENABLE_MEMCHECK) && !defined(__SANITIZE_ADDRESS__)
  const unsigned on_page_safe_mask = 0xfc0 /* enough for '-63' bytes offset */;
  if (likely(on_page_safe_mask & (uintptr_t)(range + offset)) && !RUNNING_ON_VALGRIND) {
    const unsigned extra = (unsigned)(detent + 16 - range);
    assert(extra > 0 && extra < 16);
    mask = 0xFFFF << extra;
    mask &= diffcmp2mask_avx512bw(range - 15, offset, pattern);
    if (mask)
      goto found;
    return nullptr;
  }
#endif /* !ENABLE_MEMCHECK && !__SANITIZE_ADDRESS__ */
  if (range - 7 > detent) {
    mask = diffcmp2mask_avx2(range - 7, offset, *(const __m256i *)&pattern);
    if (mask)
      return range + 24 - __builtin_clz(mask);
    range -= 8;
  }
  if (range - 3 > detent) {
    mask = diffcmp2mask_sse2avx(range - 3, offset, *(const __m128i *)&pattern);
    if (mask)
      return range + 28 - __builtin_clz(mask);
    range -= 4;
  }
  while (range > detent) {
    if (*range - range[offset] == target)
      return range;
    --range;
  }
  return nullptr;
}
#endif /* MDBX_ATTRIBUTE_TARGET_AVX512BW */

#if (defined(__ARM_NEON) || defined(__ARM_NEON__)) && (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
static __always_inline size_t diffcmp2mask_neon(const pgno_t *const ptr, const ptrdiff_t offset,
                                                const uint32x4_t pattern) {
  const uint32x4_t f = vld1q_u32(ptr);
  const uint32x4_t l = vld1q_u32(ptr + offset);
  const uint16x4_t cmp = vmovn_u32(vceqq_u32(vsubq_u32(f, l), pattern));
  if (sizeof(size_t) > 7)
    return vget_lane_u64(vreinterpret_u64_u16(cmp), 0);
  else
    return vget_lane_u32(vreinterpret_u32_u8(vmovn_u16(vcombine_u16(cmp, cmp))), 0);
}

__hot static pgno_t *scan4seq_neon(pgno_t *range, const size_t len, const size_t seq) {
  assert(seq > 0 && len > seq);
#if MDBX_PNL_ASCENDING
#error "FIXME: Not implemented"
#endif /* MDBX_PNL_ASCENDING */
  assert(range[-(ptrdiff_t)len] == len);
  pgno_t *const detent = range - len + seq;
  const ptrdiff_t offset = -(ptrdiff_t)seq;
  const pgno_t target = (pgno_t)offset;
  const uint32x4_t pattern = vmovq_n_u32(target);
  size_t mask;
  if (likely(len > seq + 3)) {
    do {
      mask = diffcmp2mask_neon(range - 3, offset, pattern);
      if (mask) {
#if !defined(ENABLE_MEMCHECK) && !defined(__SANITIZE_ADDRESS__)
      found:
#endif /* !ENABLE_MEMCHECK && !__SANITIZE_ADDRESS__ */
        return ptr_disp(range, -(__builtin_clzl(mask) >> sizeof(size_t) / 4));
      }
      range -= 4;
    } while (range > detent + 3);
    if (range == detent)
      return nullptr;
  }

  /* Далее происходит чтение от 4 до 12 лишних байт, которые могут быть не
   * только за пределами региона выделенного под PNL, но и пересекать границу
   * страницы памяти. Что может приводить как к ошибкам ASAN, так и к падению.
   * Поэтому проверяем смещение на странице, а с ASAN всегда страхуемся. */
#if !defined(ENABLE_MEMCHECK) && !defined(__SANITIZE_ADDRESS__)
  const unsigned on_page_safe_mask = 0xff0 /* enough for '-15' bytes offset */;
  if (likely(on_page_safe_mask & (uintptr_t)(range + offset)) && !RUNNING_ON_VALGRIND) {
    const unsigned extra = (unsigned)(detent + 4 - range);
    assert(extra > 0 && extra < 4);
    mask = (~(size_t)0) << (extra * sizeof(size_t) * 2);
    mask &= diffcmp2mask_neon(range - 3, offset, pattern);
    if (mask)
      goto found;
    return nullptr;
  }
#endif /* !ENABLE_MEMCHECK && !__SANITIZE_ADDRESS__ */
  do
    if (*range - range[offset] == target)
      return range;
  while (--range != detent);
  return nullptr;
}
#endif /* __ARM_NEON || __ARM_NEON__ */

#if defined(__AVX512BW__) && defined(MDBX_ATTRIBUTE_TARGET_AVX512BW)
#define scan4seq_default scan4seq_avx512bw
#define scan4seq_impl scan4seq_default
#elif defined(__AVX2__) && defined(MDBX_ATTRIBUTE_TARGET_AVX2)
#define scan4seq_default scan4seq_avx2
#elif defined(__SSE2__) && defined(MDBX_ATTRIBUTE_TARGET_SSE2)
#define scan4seq_default scan4seq_sse2
#elif (defined(__ARM_NEON) || defined(__ARM_NEON__)) && (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
#define scan4seq_default scan4seq_neon
/* Choosing of another variants should be added here. */
#endif /* scan4seq_default */

#endif /* MDBX_PNL_ASCENDING */

#ifndef scan4seq_default
#define scan4seq_default scan4seq_fallback
#endif /* scan4seq_default */

#ifdef scan4seq_impl
/* The scan4seq_impl() is the best or no alternatives */
#elif !MDBX_HAVE_BUILTIN_CPU_SUPPORTS
/* The scan4seq_default() will be used since no cpu-features detection support
 * from compiler. Please don't ask to implement cpuid-based detection and don't
 * make such PRs. */
#define scan4seq_impl scan4seq_default
#else
/* Selecting the most appropriate implementation at runtime,
 * depending on the available CPU features. */
static pgno_t *scan4seq_resolver(pgno_t *range, const size_t len, const size_t seq);
static pgno_t *(*scan4seq_impl)(pgno_t *range, const size_t len, const size_t seq) = scan4seq_resolver;

static pgno_t *scan4seq_resolver(pgno_t *range, const size_t len, const size_t seq) {
  pgno_t *(*choice)(pgno_t *range, const size_t len, const size_t seq) = nullptr;
#if __has_builtin(__builtin_cpu_init) || defined(__BUILTIN_CPU_INIT__) || __GNUC_PREREQ(4, 8)
  __builtin_cpu_init();
#endif /* __builtin_cpu_init() */
#ifdef MDBX_ATTRIBUTE_TARGET_SSE2
  if (__builtin_cpu_supports("sse2"))
    choice = scan4seq_sse2;
#endif /* MDBX_ATTRIBUTE_TARGET_SSE2 */
#ifdef MDBX_ATTRIBUTE_TARGET_AVX2
  if (__builtin_cpu_supports("avx2"))
    choice = scan4seq_avx2;
#endif /* MDBX_ATTRIBUTE_TARGET_AVX2 */
#ifdef MDBX_ATTRIBUTE_TARGET_AVX512BW
  if (__builtin_cpu_supports("avx512bw"))
    choice = scan4seq_avx512bw;
#endif /* MDBX_ATTRIBUTE_TARGET_AVX512BW */
  /* Choosing of another variants should be added here. */
  scan4seq_impl = choice ? choice : scan4seq_default;
  return scan4seq_impl(range, len, seq);
}
#endif /* scan4seq_impl */

/*----------------------------------------------------------------------------*/

#define ALLOC_COALESCE 4    /* внутреннее состояние */
#define ALLOC_SHOULD_SCAN 8 /* внутреннее состояние */
#define ALLOC_LIFO 16       /* внутреннее состояние */

static inline bool is_gc_usable(MDBX_txn *txn, const MDBX_cursor *mc, const uint8_t flags) {
  /* If txn is updating the GC, then the retired-list cannot play catch-up with
   * itself by growing while trying to save it. */
  if (mc->tree == &txn->dbs[FREE_DBI] && !(flags & ALLOC_RESERVE) && !(mc->flags & z_gcu_preparation))
    return false;

  /* avoid search inside empty tree and while tree is updating,
     https://libmdbx.dqdkfa.ru/dead-github/issues/31 */
  if (unlikely(txn->dbs[FREE_DBI].items == 0)) {
    txn->flags |= txn_gc_drained;
    return false;
  }

  return true;
}

static inline bool is_already_reclaimed(const MDBX_txn *txn, txnid_t id) { return txl_contain(txn->tw.gc.retxl, id); }

__hot static pgno_t repnl_get_single(MDBX_txn *txn) {
  const size_t len = MDBX_PNL_GETSIZE(txn->tw.repnl);
  assert(len > 0);
  pgno_t *target = MDBX_PNL_EDGE(txn->tw.repnl);
  const ptrdiff_t dir = MDBX_PNL_ASCENDING ? 1 : -1;

  /* Есть ТРИ потенциально выигрышные, но противо-направленные тактики:
   *
   * 1. Стараться использовать страницы с наименьшими номерами. Так обмен с
   * диском будет более кучным, а у страниц ближе к концу БД будет больше шансов
   * попасть под авто-компактификацию. Частично эта тактика уже реализована, но
   * для её эффективности требуется явно приоритезировать выделение страниц:
   *   - поддерживать два repnl, для ближних и для дальних страниц;
   *   - использовать страницы из дальнего списка, если первый пуст,
   *     а второй слишком большой, либо при пустой GC.
   *
   * 2. Стараться выделять страницы последовательно. Так записываемые на диск
   * регионы будут линейными, что принципиально ускоряет запись на HDD.
   * Одновременно, в среднем это не повлияет на чтение, точнее говоря, если
   * порядок чтения не совпадает с порядком изменения (иначе говоря, если
   * чтение не коррелирует с обновлениями и/или вставками) то не повлияет, иначе
   * может ускорить. Однако, последовательности в среднем достаточно редки.
   * Поэтому для эффективности требуется аккумулировать и поддерживать в ОЗУ
   * огромные списки страниц, а затем сохранять их обратно в БД. Текущий формат
   * БД (без сжатых битовых карт) для этого крайне не удачен. Поэтому эта тактика не
   * имеет шансов быть успешной без смены формата БД (Mithril).
   *
   * 3. Стараться экономить последовательности страниц. Это позволяет избегать
   * лишнего чтения/поиска в GC при более-менее постоянном размещении и/или
   * обновлении данных требующих более одной страницы. Проблема в том, что без
   * информации от приложения библиотека не может знать насколько
   * востребованными будут последовательности в ближайшей перспективе, а
   * экономия последовательностей "на всякий случай" не только затратна
   * сама-по-себе, но и работает во вред (добавляет хаоса).
   *
   * Поэтому:
   *  - в TODO добавляется разделение repnl на «ближние» и «дальние» страницы,
   *    с последующей реализацией первой тактики;
   *  - преимущественное использование последовательностей отправляется
   *    в MithrilDB как составляющая "HDD frendly" feature;
   *  - реализованная в 3757eb72f7c6b46862f8f17881ac88e8cecc1979 экономия
   *    последовательностей отключается через MDBX_ENABLE_SAVING_SEQUENCES=0.
   *
   * В качестве альтернативы для безусловной «экономии» последовательностей,
   * в следующих версиях libmdbx, вероятно, будет предложено
   * API для взаимодействия с GC:
   *  - получение размера GC, включая гистограммы размеров последовательностей
   *    и близости к концу БД;
   *  - включение формирования "линейного запаса" для последующего использования
   *    в рамках текущей транзакции;
   *  - намеренная загрузка GC в память для коагуляции и "выпрямления";
   *  - намеренное копирование данных из страниц в конце БД для последующего
   *    из освобождения, т.е. контролируемая компактификация по запросу. */

#ifndef MDBX_ENABLE_SAVING_SEQUENCES
#define MDBX_ENABLE_SAVING_SEQUENCES 0
#endif
  if (MDBX_ENABLE_SAVING_SEQUENCES && unlikely(target[dir] == *target + 1) && len > 2) {
    /* Пытаемся пропускать последовательности при наличии одиночных элементов.
     * TODO: необходимо кэшировать пропускаемые последовательности
     * чтобы не сканировать список сначала при каждом выделении. */
    pgno_t *scan = target + dir + dir;
    size_t left = len;
    do {
      if (likely(scan[-dir] != *scan - 1 && *scan + 1 != scan[dir])) {
#if MDBX_PNL_ASCENDING
        target = scan;
        break;
#else
        /* вырезаем элемент с перемещением хвоста */
        const pgno_t pgno = *scan;
        MDBX_PNL_SETSIZE(txn->tw.repnl, len - 1);
        while (++scan <= target)
          scan[-1] = *scan;
        return pgno;
#endif
      }
      scan += dir;
    } while (--left > 2);
  }

  const pgno_t pgno = *target;
#if MDBX_PNL_ASCENDING
  /* вырезаем элемент с перемещением хвоста */
  MDBX_PNL_SETSIZE(txn->tw.repnl, len - 1);
  for (const pgno_t *const end = txn->tw.repnl + len - 1; target <= end; ++target)
    *target = target[1];
#else
  /* перемещать хвост не нужно, просто усекам список */
  MDBX_PNL_SETSIZE(txn->tw.repnl, len - 1);
#endif
  return pgno;
}

__hot static pgno_t repnl_get_sequence(MDBX_txn *txn, const size_t num, uint8_t flags) {
  const size_t len = MDBX_PNL_GETSIZE(txn->tw.repnl);
  pgno_t *edge = MDBX_PNL_EDGE(txn->tw.repnl);
  assert(len >= num && num > 1);
  const size_t seq = num - 1;
#if !MDBX_PNL_ASCENDING
  if (edge[-(ptrdiff_t)seq] - *edge == seq) {
    if (unlikely(flags & ALLOC_RESERVE))
      return P_INVALID;
    assert(edge == scan4range_checker(txn->tw.repnl, seq));
    /* перемещать хвост не нужно, просто усекам список */
    MDBX_PNL_SETSIZE(txn->tw.repnl, len - num);
    return *edge;
  }
#endif
  pgno_t *target = scan4seq_impl(edge, len, seq);
  assert(target == scan4range_checker(txn->tw.repnl, seq));
  if (target) {
    if (unlikely(flags & ALLOC_RESERVE))
      return P_INVALID;
    const pgno_t pgno = *target;
    /* вырезаем найденную последовательность с перемещением хвоста */
    MDBX_PNL_SETSIZE(txn->tw.repnl, len - num);
#if MDBX_PNL_ASCENDING
    for (const pgno_t *const end = txn->tw.repnl + len - num; target <= end; ++target)
      *target = target[num];
#else
    for (const pgno_t *const end = txn->tw.repnl + len; ++target <= end;)
      target[-(ptrdiff_t)num] = *target;
#endif
    return pgno;
  }
  return 0;
}

static inline pgr_t page_alloc_finalize(MDBX_env *const env, MDBX_txn *const txn, const MDBX_cursor *const mc,
                                        const pgno_t pgno, const size_t num) {
#if MDBX_ENABLE_PROFGC
  size_t majflt_before;
  const uint64_t cputime_before = osal_cputime(&majflt_before);
  gc_prof_stat_t *const prof =
      (cursor_dbi(mc) == FREE_DBI) ? &env->lck->pgops.gc_prof.self : &env->lck->pgops.gc_prof.work;
#else
  (void)mc;
#endif /* MDBX_ENABLE_PROFGC */
  ENSURE(env, pgno >= NUM_METAS);

  pgr_t ret;
  bool need_clean = (env->flags & MDBX_PAGEPERTURB) != 0;
  if (env->flags & MDBX_WRITEMAP) {
    ret.page = pgno2page(env, pgno);
    MDBX_ASAN_UNPOISON_MEMORY_REGION(ret.page, pgno2bytes(env, num));
    VALGRIND_MAKE_MEM_UNDEFINED(ret.page, pgno2bytes(env, num));

    /* Содержимое выделенной страницы не нужно, но если страница отсутствует
     * в ОЗУ (что весьма вероятно), то любое обращение к ней приведет
     * к page-fault:
     *  - прерыванию по отсутствию страницы;
     *  - переключение контекста в режим ядра с засыпанием процесса;
     *  - чтение страницы с диска;
     *  - обновление PTE и пробуждением процесса;
     *  - переключение контекста по доступности ЦПУ.
     *
     * Пытаемся минимизировать накладные расходы записывая страницу, что при
     * наличии unified page cache приведет к появлению страницы в ОЗУ без чтения
     * с диска. При этом запись на диск должна быть отложена адекватным ядром,
     * так как страница отображена в память в режиме чтения-записи и следом в
     * неё пишет ЦПУ. */

    /* В случае если страница в памяти процесса, то излишняя запись может быть
     * достаточно дорогой. Кроме системного вызова и копирования данных, в особо
     * одаренных ОС при этом могут включаться файловая система, выделяться
     * временная страница, пополняться очереди асинхронного выполнения,
     * обновляться PTE с последующей генерацией page-fault и чтением данных из
     * грязной I/O очереди. Из-за этого штраф за лишнюю запись может быть
     * сравним с избегаемым ненужным чтением. */
    if (txn->tw.prefault_write_activated) {
      void *const pattern = ptr_disp(env->page_auxbuf, need_clean ? env->ps : env->ps * 2);
      size_t file_offset = pgno2bytes(env, pgno);
      if (likely(num == 1)) {
        if (!mincore_probe(env, pgno)) {
          osal_pwrite(env->lazy_fd, pattern, env->ps, file_offset);
#if MDBX_ENABLE_PGOP_STAT
          env->lck->pgops.prefault.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
          need_clean = false;
        }
      } else {
        struct iovec iov[MDBX_AUXILARY_IOV_MAX];
        size_t n = 0, cleared = 0;
        for (size_t i = 0; i < num; ++i) {
          if (!mincore_probe(env, pgno + (pgno_t)i)) {
            ++cleared;
            iov[n].iov_len = env->ps;
            iov[n].iov_base = pattern;
            if (unlikely(++n == MDBX_AUXILARY_IOV_MAX)) {
              osal_pwritev(env->lazy_fd, iov, MDBX_AUXILARY_IOV_MAX, file_offset);
#if MDBX_ENABLE_PGOP_STAT
              env->lck->pgops.prefault.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
              file_offset += pgno2bytes(env, MDBX_AUXILARY_IOV_MAX);
              n = 0;
            }
          }
        }
        if (likely(n > 0)) {
          osal_pwritev(env->lazy_fd, iov, n, file_offset);
#if MDBX_ENABLE_PGOP_STAT
          env->lck->pgops.prefault.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
        }
        if (cleared == num)
          need_clean = false;
      }
    }
  } else {
    ret.page = page_shadow_alloc(txn, num);
    if (unlikely(!ret.page)) {
      ret.err = MDBX_ENOMEM;
      goto bailout;
    }
  }

  if (unlikely(need_clean))
    memset(ret.page, -1, pgno2bytes(env, num));

  VALGRIND_MAKE_MEM_UNDEFINED(ret.page, pgno2bytes(env, num));
  ret.page->pgno = pgno;
  ret.page->dupfix_ksize = 0;
  ret.page->flags = 0;
  if ((ASSERT_ENABLED() || AUDIT_ENABLED()) && num > 1) {
    ret.page->pages = (pgno_t)num;
    ret.page->flags = P_LARGE;
  }

  ret.err = page_dirty(txn, ret.page, (pgno_t)num);
bailout:
  tASSERT(txn, pnl_check_allocated(txn->tw.repnl, txn->geo.first_unallocated - MDBX_ENABLE_REFUND));
#if MDBX_ENABLE_PROFGC
  size_t majflt_after;
  prof->xtime_cpu += osal_cputime(&majflt_after) - cputime_before;
  prof->majflt += (uint32_t)(majflt_after - majflt_before);
#endif /* MDBX_ENABLE_PROFGC */
  return ret;
}

pgr_t gc_alloc_ex(const MDBX_cursor *const mc, const size_t num, uint8_t flags) {
  pgr_t ret;
  MDBX_txn *const txn = mc->txn;
  MDBX_env *const env = txn->env;
#if MDBX_ENABLE_PROFGC
  gc_prof_stat_t *const prof =
      (cursor_dbi(mc) == FREE_DBI) ? &env->lck->pgops.gc_prof.self : &env->lck->pgops.gc_prof.work;
  prof->spe_counter += 1;
#endif /* MDBX_ENABLE_PROFGC */

  eASSERT(env, num > 0 || (flags & ALLOC_RESERVE));
  eASSERT(env, pnl_check_allocated(txn->tw.repnl, txn->geo.first_unallocated - MDBX_ENABLE_REFUND));

  size_t newnext;
  const uint64_t monotime_begin = (MDBX_ENABLE_PROFGC || (num > 1 && env->options.gc_time_limit)) ? osal_monotime() : 0;
  struct monotime_cache now_cache;
  now_cache.expire_countdown = 1 /* старт с 1 позволяет избавиться как от лишних системных вызовов когда
                                    лимит времени задан нулевой или уже исчерпан, так и от подсчета
                                    времени при не-достижении rp_augment_limit */
      ;
  now_cache.value = monotime_begin;
  pgno_t pgno = 0;
  if (num > 1) {
#if MDBX_ENABLE_PROFGC
    prof->xpages += 1;
#endif /* MDBX_ENABLE_PROFGC */
    if (MDBX_PNL_GETSIZE(txn->tw.repnl) >= num) {
      eASSERT(env, MDBX_PNL_LAST(txn->tw.repnl) < txn->geo.first_unallocated &&
                       MDBX_PNL_FIRST(txn->tw.repnl) < txn->geo.first_unallocated);
      pgno = repnl_get_sequence(txn, num, flags);
      if (likely(pgno))
        goto done;
    }
  } else {
    eASSERT(env, num == 0 || MDBX_PNL_GETSIZE(txn->tw.repnl) == 0);
    eASSERT(env, !(flags & ALLOC_RESERVE) || num == 0);
  }

  //---------------------------------------------------------------------------

  if (unlikely(!is_gc_usable(txn, mc, flags)))
    goto no_gc;

  eASSERT(env, (flags & (ALLOC_COALESCE | ALLOC_LIFO | ALLOC_SHOULD_SCAN)) == 0);
  flags += (env->flags & MDBX_LIFORECLAIM) ? ALLOC_LIFO : 0;

  if (/* Не коагулируем записи при подготовке резерва для обновления GC.
       * Иначе попытка увеличить резерв может приводить к необходимости ещё
       * большего резерва из-за увеличения списка переработанных страниц. */
      (flags & ALLOC_RESERVE) == 0) {
    if (txn->dbs[FREE_DBI].branch_pages && MDBX_PNL_GETSIZE(txn->tw.repnl) < env->maxgc_large1page / 2)
      flags += ALLOC_COALESCE;
  }

  MDBX_cursor *const gc = ptr_disp(env->basal_txn, sizeof(MDBX_txn));
  eASSERT(env, mc != gc && gc->next == gc);
  gc->txn = txn;
  gc->dbi_state = txn->dbi_state;
  gc->top_and_flags = z_fresh_mark;

  txn->tw.prefault_write_activated = env->options.prefault_write;
  if (txn->tw.prefault_write_activated) {
    /* Проверка посредством minicore() существенно снижает затраты, но в
     * простейших случаях (тривиальный бенчмарк) интегральная производительность
     * становится вдвое меньше. А на платформах без mincore() и с проблемной
     * подсистемой виртуальной памяти ситуация может быть многократно хуже.
     * Поэтому избегаем затрат в ситуациях когда prefault-write скорее всего не
     * нужна. */
    const bool readahead_enabled = env->lck->readahead_anchor & 1;
    const pgno_t readahead_edge = env->lck->readahead_anchor >> 1;
    if (/* Не суетимся если GC почти пустая и БД маленькая */
        (txn->dbs[FREE_DBI].branch_pages == 0 && txn->geo.now < 1234) ||
        /* Не суетимся если страница в зоне включенного упреждающего чтения */
        (readahead_enabled && pgno + num < readahead_edge))
      txn->tw.prefault_write_activated = false;
  }

retry_gc_refresh_oldest:;
  txnid_t oldest = txn_snapshot_oldest(txn);
retry_gc_have_oldest:
  if (unlikely(oldest >= txn->txnid)) {
    ERROR("unexpected/invalid oldest-readed txnid %" PRIaTXN " for current-txnid %" PRIaTXN, oldest, txn->txnid);
    ret.err = MDBX_PROBLEM;
    goto fail;
  }
  const txnid_t detent = oldest + 1;

  txnid_t id = 0;
  MDBX_cursor_op op = MDBX_FIRST;
  if (flags & ALLOC_LIFO) {
    if (!txn->tw.gc.retxl) {
      txn->tw.gc.retxl = txl_alloc();
      if (unlikely(!txn->tw.gc.retxl)) {
        ret.err = MDBX_ENOMEM;
        goto fail;
      }
    }
    /* Begin lookup backward from oldest reader */
    id = detent - 1;
    op = MDBX_SET_RANGE;
  } else if (txn->tw.gc.last_reclaimed) {
    /* Continue lookup forward from last-reclaimed */
    id = txn->tw.gc.last_reclaimed + 1;
    if (id >= detent)
      goto depleted_gc;
    op = MDBX_SET_RANGE;
  }

next_gc:;
  MDBX_val key;
  key.iov_base = &id;
  key.iov_len = sizeof(id);

#if MDBX_ENABLE_PROFGC
  prof->rsteps += 1;
#endif /* MDBX_ENABLE_PROFGC */

  /* Seek first/next GC record */
  ret.err = cursor_ops(gc, &key, nullptr, op);
  if (unlikely(ret.err != MDBX_SUCCESS)) {
    if (unlikely(ret.err != MDBX_NOTFOUND))
      goto fail;
    if ((flags & ALLOC_LIFO) && op == MDBX_SET_RANGE) {
      op = MDBX_PREV;
      goto next_gc;
    }
    goto depleted_gc;
  }
  if (unlikely(key.iov_len != sizeof(txnid_t))) {
    ERROR("%s/%d: %s", "MDBX_CORRUPTED", MDBX_CORRUPTED, "invalid GC key-length");
    ret.err = MDBX_CORRUPTED;
    goto fail;
  }
  id = unaligned_peek_u64(4, key.iov_base);
  if (flags & ALLOC_LIFO) {
    op = MDBX_PREV;
    if (id >= detent || is_already_reclaimed(txn, id))
      goto next_gc;
  } else {
    op = MDBX_NEXT;
    if (unlikely(id >= detent))
      goto depleted_gc;
  }
  txn->flags &= ~txn_gc_drained;

  /* Reading next GC record */
  MDBX_val data;
  page_t *const mp = gc->pg[gc->top];
  if (unlikely((ret.err = node_read(gc, page_node(mp, gc->ki[gc->top]), &data, mp)) != MDBX_SUCCESS))
    goto fail;

  pgno_t *gc_pnl = (pgno_t *)data.iov_base;
  if (unlikely(data.iov_len % sizeof(pgno_t) || data.iov_len < MDBX_PNL_SIZEOF(gc_pnl) ||
               !pnl_check(gc_pnl, txn->geo.first_unallocated))) {
    ERROR("%s/%d: %s", "MDBX_CORRUPTED", MDBX_CORRUPTED, "invalid GC value-length");
    ret.err = MDBX_CORRUPTED;
    goto fail;
  }

  const size_t gc_len = MDBX_PNL_GETSIZE(gc_pnl);
  TRACE("gc-read: id #%" PRIaTXN " len %zu, re-list will %zu ", id, gc_len, gc_len + MDBX_PNL_GETSIZE(txn->tw.repnl));

  if (unlikely(gc_len + MDBX_PNL_GETSIZE(txn->tw.repnl) >= env->maxgc_large1page)) {
    /* Don't try to coalesce too much. */
    if (flags & ALLOC_SHOULD_SCAN) {
      eASSERT(env, flags & ALLOC_COALESCE);
      eASSERT(env, !(flags & ALLOC_RESERVE));
      eASSERT(env, num > 0);
#if MDBX_ENABLE_PROFGC
      env->lck->pgops.gc_prof.coalescences += 1;
#endif /* MDBX_ENABLE_PROFGC */
      TRACE("clear %s %s", "ALLOC_COALESCE", "since got threshold");
      if (MDBX_PNL_GETSIZE(txn->tw.repnl) >= num) {
        eASSERT(env, MDBX_PNL_LAST(txn->tw.repnl) < txn->geo.first_unallocated &&
                         MDBX_PNL_FIRST(txn->tw.repnl) < txn->geo.first_unallocated);
        if (likely(num == 1)) {
          pgno = repnl_get_single(txn);
          goto done;
        }
        pgno = repnl_get_sequence(txn, num, flags);
        if (likely(pgno))
          goto done;
      }
      flags -= ALLOC_COALESCE | ALLOC_SHOULD_SCAN;
    }
    if (unlikely(/* list is too long already */ MDBX_PNL_GETSIZE(txn->tw.repnl) >= env->options.rp_augment_limit) &&
        ((/* not a slot-request from gc-update */ num &&
          /* have enough unallocated space */ txn->geo.upper >= txn->geo.first_unallocated + num &&
          monotime_since_cached(monotime_begin, &now_cache) + txn->tw.gc.time_acc >= env->options.gc_time_limit) ||
         gc_len + MDBX_PNL_GETSIZE(txn->tw.repnl) >= PAGELIST_LIMIT)) {
      /* Stop reclaiming to avoid large/overflow the page list. This is a rare
       * case while search for a continuously multi-page region in a
       * large database, see https://libmdbx.dqdkfa.ru/dead-github/issues/123 */
      NOTICE("stop reclaiming %s: %zu (current) + %zu "
             "(chunk) -> %zu, rp_augment_limit %u",
             likely(gc_len + MDBX_PNL_GETSIZE(txn->tw.repnl) < PAGELIST_LIMIT) ? "since rp_augment_limit was reached"
                                                                               : "to avoid PNL overflow",
             MDBX_PNL_GETSIZE(txn->tw.repnl), gc_len, gc_len + MDBX_PNL_GETSIZE(txn->tw.repnl),
             env->options.rp_augment_limit);
      goto depleted_gc;
    }
  }

  /* Remember ID of readed GC record */
  txn->tw.gc.last_reclaimed = id;
  if (flags & ALLOC_LIFO) {
    ret.err = txl_append(&txn->tw.gc.retxl, id);
    if (unlikely(ret.err != MDBX_SUCCESS))
      goto fail;
  }

  /* Append PNL from GC record to tw.repnl */
  ret.err = pnl_need(&txn->tw.repnl, gc_len);
  if (unlikely(ret.err != MDBX_SUCCESS))
    goto fail;

  if (LOG_ENABLED(MDBX_LOG_EXTRA)) {
    DEBUG_EXTRA("readed GC-pnl txn %" PRIaTXN " root %" PRIaPGNO " len %zu, PNL", id, txn->dbs[FREE_DBI].root, gc_len);
    for (size_t i = gc_len; i; i--)
      DEBUG_EXTRA_PRINT(" %" PRIaPGNO, gc_pnl[i]);
    DEBUG_EXTRA_PRINT(", first_unallocated %u\n", txn->geo.first_unallocated);
  }

  /* Merge in descending sorted order */
#if MDBX_ENABLE_PROFGC
  const uint64_t merge_begin = osal_monotime();
#endif /* MDBX_ENABLE_PROFGC */
  pnl_merge(txn->tw.repnl, gc_pnl);
#if MDBX_ENABLE_PROFGC
  prof->pnl_merge.calls += 1;
  prof->pnl_merge.volume += MDBX_PNL_GETSIZE(txn->tw.repnl);
  prof->pnl_merge.time += osal_monotime() - merge_begin;
#endif /* MDBX_ENABLE_PROFGC */
  flags |= ALLOC_SHOULD_SCAN;
  if (AUDIT_ENABLED()) {
    if (unlikely(!pnl_check(txn->tw.repnl, txn->geo.first_unallocated))) {
      ERROR("%s/%d: %s", "MDBX_CORRUPTED", MDBX_CORRUPTED, "invalid txn retired-list");
      ret.err = MDBX_CORRUPTED;
      goto fail;
    }
  } else {
    eASSERT(env, pnl_check_allocated(txn->tw.repnl, txn->geo.first_unallocated));
  }
  eASSERT(env, dpl_check(txn));

  eASSERT(env, MDBX_PNL_GETSIZE(txn->tw.repnl) == 0 || MDBX_PNL_MOST(txn->tw.repnl) < txn->geo.first_unallocated);
  if (MDBX_ENABLE_REFUND && MDBX_PNL_GETSIZE(txn->tw.repnl) &&
      unlikely(MDBX_PNL_MOST(txn->tw.repnl) == txn->geo.first_unallocated - 1)) {
    /* Refund suitable pages into "unallocated" space */
    txn_refund(txn);
  }
  eASSERT(env, pnl_check_allocated(txn->tw.repnl, txn->geo.first_unallocated - MDBX_ENABLE_REFUND));

  /* Done for a kick-reclaim mode, actually no page needed */
  if (unlikely(num == 0)) {
    eASSERT(env, ret.err == MDBX_SUCCESS);
    TRACE("%s: last id #%" PRIaTXN ", re-len %zu", "early-exit for slot", id, MDBX_PNL_GETSIZE(txn->tw.repnl));
    goto early_exit;
  }

  /* TODO: delete reclaimed records */

  eASSERT(env, op == MDBX_PREV || op == MDBX_NEXT);
  if (flags & ALLOC_COALESCE) {
    TRACE("%s: last id #%" PRIaTXN ", re-len %zu", "coalesce-continue", id, MDBX_PNL_GETSIZE(txn->tw.repnl));
    goto next_gc;
  }

scan:
  eASSERT(env, flags & ALLOC_SHOULD_SCAN);
  eASSERT(env, num > 0);
  if (MDBX_PNL_GETSIZE(txn->tw.repnl) >= num) {
    eASSERT(env, MDBX_PNL_LAST(txn->tw.repnl) < txn->geo.first_unallocated &&
                     MDBX_PNL_FIRST(txn->tw.repnl) < txn->geo.first_unallocated);
    if (likely(num == 1)) {
      eASSERT(env, !(flags & ALLOC_RESERVE));
      pgno = repnl_get_single(txn);
      goto done;
    }
    pgno = repnl_get_sequence(txn, num, flags);
    if (likely(pgno))
      goto done;
  }
  flags -= ALLOC_SHOULD_SCAN;
  if (ret.err == MDBX_SUCCESS) {
    TRACE("%s: last id #%" PRIaTXN ", re-len %zu", "continue-search", id, MDBX_PNL_GETSIZE(txn->tw.repnl));
    goto next_gc;
  }

depleted_gc:
  TRACE("%s: last id #%" PRIaTXN ", re-len %zu", "gc-depleted", id, MDBX_PNL_GETSIZE(txn->tw.repnl));
  ret.err = MDBX_NOTFOUND;
  if (flags & ALLOC_SHOULD_SCAN)
    goto scan;
  txn->flags |= txn_gc_drained;

  //-------------------------------------------------------------------------

  /* There is no suitable pages in the GC and to be able to allocate
   * we should CHOICE one of:
   *  - make a new steady checkpoint if reclaiming was stopped by
   *    the last steady-sync, or wipe it in the MDBX_UTTERLY_NOSYNC mode;
   *  - kick lagging reader(s) if reclaiming was stopped by ones of it.
   *  - extend the database file. */

  /* Will use new pages from the map if nothing is suitable in the GC. */
  newnext = txn->geo.first_unallocated + num;

  /* Does reclaiming stopped at the last steady point? */
  const meta_ptr_t recent = meta_recent(env, &txn->tw.troika);
  const meta_ptr_t prefer_steady = meta_prefer_steady(env, &txn->tw.troika);
  if (recent.ptr_c != prefer_steady.ptr_c && prefer_steady.is_steady && detent == prefer_steady.txnid + 1) {
    DEBUG("gc-kick-steady: recent %" PRIaTXN "-%s, steady %" PRIaTXN "-%s, detent %" PRIaTXN, recent.txnid,
          durable_caption(recent.ptr_c), prefer_steady.txnid, durable_caption(prefer_steady.ptr_c), detent);
    const pgno_t autosync_threshold = atomic_load32(&env->lck->autosync_threshold, mo_Relaxed);
    const uint64_t autosync_period = atomic_load64(&env->lck->autosync_period, mo_Relaxed);
    uint64_t eoos_timestamp;
    /* wipe the last steady-point if one of:
     *  - UTTERLY_NOSYNC mode AND auto-sync threshold is NOT specified
     *  - UTTERLY_NOSYNC mode AND free space at steady-point is exhausted
     * otherwise, make a new steady-point if one of:
     *  - auto-sync threshold is specified and reached;
     *  - upper limit of database size is reached;
     *  - database is full (with the current file size)
     *       AND auto-sync threshold it NOT specified */
    if (F_ISSET(env->flags, MDBX_UTTERLY_NOSYNC) &&
        ((autosync_threshold | autosync_period) == 0 || newnext >= prefer_steady.ptr_c->geometry.now)) {
      /* wipe steady checkpoint in MDBX_UTTERLY_NOSYNC mode
       * without any auto-sync threshold(s). */
#if MDBX_ENABLE_PROFGC
      env->lck->pgops.gc_prof.wipes += 1;
#endif /* MDBX_ENABLE_PROFGC */
      ret.err = meta_wipe_steady(env, detent);
      DEBUG("gc-wipe-steady, rc %d", ret.err);
      if (unlikely(ret.err != MDBX_SUCCESS))
        goto fail;
      eASSERT(env, prefer_steady.ptr_c != meta_prefer_steady(env, &txn->tw.troika).ptr_c);
      goto retry_gc_refresh_oldest;
    }
    if ((autosync_threshold && atomic_load64(&env->lck->unsynced_pages, mo_Relaxed) >= autosync_threshold) ||
        (autosync_period && (eoos_timestamp = atomic_load64(&env->lck->eoos_timestamp, mo_Relaxed)) &&
         osal_monotime() - eoos_timestamp >= autosync_period) ||
        newnext >= txn->geo.upper ||
        ((num == 0 || newnext >= txn->geo.end_pgno) && (autosync_threshold | autosync_period) == 0)) {
      /* make steady checkpoint. */
#if MDBX_ENABLE_PROFGC
      env->lck->pgops.gc_prof.flushes += 1;
#endif /* MDBX_ENABLE_PROFGC */
      meta_t meta = *recent.ptr_c;
      ret.err = dxb_sync_locked(env, env->flags & MDBX_WRITEMAP, &meta, &txn->tw.troika);
      DEBUG("gc-make-steady, rc %d", ret.err);
      eASSERT(env, ret.err != MDBX_RESULT_TRUE);
      if (unlikely(ret.err != MDBX_SUCCESS))
        goto fail;
      eASSERT(env, prefer_steady.ptr_c != meta_prefer_steady(env, &txn->tw.troika).ptr_c);
      goto retry_gc_refresh_oldest;
    }
  }

  if (unlikely(true == atomic_load32(&env->lck->rdt_refresh_flag, mo_AcquireRelease))) {
    oldest = txn_snapshot_oldest(txn);
    if (oldest >= detent)
      goto retry_gc_have_oldest;
  }

  /* Avoid kick lagging reader(s) if is enough unallocated space
   * at the end of database file. */
  if (!(flags & ALLOC_RESERVE) && newnext <= txn->geo.end_pgno) {
    eASSERT(env, pgno == 0);
    goto done;
  }

  if (oldest < txn->txnid - xMDBX_TXNID_STEP) {
    oldest = mvcc_kick_laggards(env, oldest);
    if (oldest >= detent)
      goto retry_gc_have_oldest;
  }

  //---------------------------------------------------------------------------

no_gc:
  eASSERT(env, pgno == 0);
#ifndef MDBX_ENABLE_BACKLOG_DEPLETED
#define MDBX_ENABLE_BACKLOG_DEPLETED 0
#endif /* MDBX_ENABLE_BACKLOG_DEPLETED*/
  if (MDBX_ENABLE_BACKLOG_DEPLETED && unlikely(!(txn->flags & txn_gc_drained))) {
    ret.err = MDBX_BACKLOG_DEPLETED;
    goto fail;
  }
  if (flags & ALLOC_RESERVE) {
    ret.err = MDBX_NOTFOUND;
    goto fail;
  }

  /* Will use new pages from the map if nothing is suitable in the GC. */
  newnext = txn->geo.first_unallocated + num;
  if (newnext <= txn->geo.end_pgno)
    goto done;

  if (newnext > txn->geo.upper || !txn->geo.grow_pv) {
    NOTICE("gc-alloc: next %zu > upper %" PRIaPGNO, newnext, txn->geo.upper);
    ret.err = MDBX_MAP_FULL;
    goto fail;
  }

  eASSERT(env, newnext > txn->geo.end_pgno);
  const size_t grow_step = pv2pages(txn->geo.grow_pv);
  size_t aligned = pgno_align2os_pgno(env, (pgno_t)(newnext + grow_step - newnext % grow_step));

  if (aligned > txn->geo.upper)
    aligned = txn->geo.upper;
  eASSERT(env, aligned >= newnext);

  VERBOSE("try growth datafile to %zu pages (+%zu)", aligned, aligned - txn->geo.end_pgno);
  ret.err = dxb_resize(env, txn->geo.first_unallocated, (pgno_t)aligned, txn->geo.upper, implicit_grow);
  if (ret.err != MDBX_SUCCESS) {
    ERROR("unable growth datafile to %zu pages (+%zu), errcode %d", aligned, aligned - txn->geo.end_pgno, ret.err);
    goto fail;
  }
  env->txn->geo.end_pgno = (pgno_t)aligned;
  eASSERT(env, pgno == 0);

  //---------------------------------------------------------------------------

done:
  ret.err = MDBX_SUCCESS;
  if (likely((flags & ALLOC_RESERVE) == 0)) {
    if (pgno) {
      eASSERT(env, pgno + num <= txn->geo.first_unallocated && pgno >= NUM_METAS);
      eASSERT(env, pnl_check_allocated(txn->tw.repnl, txn->geo.first_unallocated - MDBX_ENABLE_REFUND));
    } else {
      pgno = txn->geo.first_unallocated;
      txn->geo.first_unallocated += (pgno_t)num;
      eASSERT(env, txn->geo.first_unallocated <= txn->geo.end_pgno);
      eASSERT(env, pgno >= NUM_METAS && pgno + num <= txn->geo.first_unallocated);
    }

    ret = page_alloc_finalize(env, txn, mc, pgno, num);
    if (unlikely(ret.err != MDBX_SUCCESS)) {
    fail:
      eASSERT(env, ret.err != MDBX_SUCCESS);
      eASSERT(env, pnl_check_allocated(txn->tw.repnl, txn->geo.first_unallocated - MDBX_ENABLE_REFUND));
      int level;
      const char *what;
      if (flags & ALLOC_RESERVE) {
        level = (flags & ALLOC_UNIMPORTANT) ? MDBX_LOG_DEBUG : MDBX_LOG_NOTICE;
        what = num ? "reserve-pages" : "fetch-slot";
      } else {
        txn->flags |= MDBX_TXN_ERROR;
        level = MDBX_LOG_ERROR;
        what = "pages";
      }
      if (LOG_ENABLED(level))
        debug_log(level, __func__, __LINE__,
                  "unable alloc %zu %s, alloc-flags 0x%x, err %d, txn-flags "
                  "0x%x, re-list-len %zu, loose-count %zu, gc: height %u, "
                  "branch %zu, leaf %zu, large %zu, entries %zu\n",
                  num, what, flags, ret.err, txn->flags, MDBX_PNL_GETSIZE(txn->tw.repnl), txn->tw.loose_count,
                  txn->dbs[FREE_DBI].height, (size_t)txn->dbs[FREE_DBI].branch_pages,
                  (size_t)txn->dbs[FREE_DBI].leaf_pages, (size_t)txn->dbs[FREE_DBI].large_pages,
                  (size_t)txn->dbs[FREE_DBI].items);
      ret.page = nullptr;
    }
    if (num > 1)
      txn->tw.gc.time_acc += monotime_since_cached(monotime_begin, &now_cache);
  } else {
  early_exit:
    DEBUG("return nullptr for %zu pages for ALLOC_%s, rc %d", num, num ? "RESERVE" : "SLOT", ret.err);
    ret.page = nullptr;
  }

#if MDBX_ENABLE_PROFGC
  prof->rtime_monotonic += osal_monotime() - monotime_begin;
#endif /* MDBX_ENABLE_PROFGC */
  return ret;
}

__hot pgr_t gc_alloc_single(const MDBX_cursor *const mc) {
  MDBX_txn *const txn = mc->txn;
  tASSERT(txn, mc->txn->flags & MDBX_TXN_DIRTY);
  tASSERT(txn, F_ISSET(*cursor_dbi_state(mc), DBI_LINDO | DBI_VALID | DBI_DIRTY));

  /* If there are any loose pages, just use them */
  while (likely(txn->tw.loose_pages)) {
#if MDBX_ENABLE_REFUND
    if (unlikely(txn->tw.loose_refund_wl > txn->geo.first_unallocated)) {
      txn_refund(txn);
      if (!txn->tw.loose_pages)
        break;
    }
#endif /* MDBX_ENABLE_REFUND */

    page_t *lp = txn->tw.loose_pages;
    MDBX_ASAN_UNPOISON_MEMORY_REGION(lp, txn->env->ps);
    VALGRIND_MAKE_MEM_DEFINED(&page_next(lp), sizeof(page_t *));
    txn->tw.loose_pages = page_next(lp);
    txn->tw.loose_count--;
    DEBUG_EXTRA("db %d use loose page %" PRIaPGNO, cursor_dbi_dbg(mc), lp->pgno);
    tASSERT(txn, lp->pgno < txn->geo.first_unallocated);
    tASSERT(txn, lp->pgno >= NUM_METAS);
    VALGRIND_MAKE_MEM_UNDEFINED(page_data(lp), page_space(txn->env));
    lp->txnid = txn->front_txnid;
    pgr_t ret = {lp, MDBX_SUCCESS};
    return ret;
  }

  if (likely(MDBX_PNL_GETSIZE(txn->tw.repnl) > 0))
    return page_alloc_finalize(txn->env, txn, mc, repnl_get_single(txn), 1);

  return gc_alloc_ex(mc, 1, ALLOC_DEFAULT);
}
