/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024

#include "internals.h"

MDBX_INTERNAL pnl_t pnl_alloc(size_t size) {
  size_t bytes = pnl_size2bytes(size);
  pnl_t pnl = osal_malloc(bytes);
  if (likely(pnl)) {
#if __GLIBC_PREREQ(2, 12) || defined(__FreeBSD__) || defined(malloc_usable_size)
    bytes = malloc_usable_size(pnl);
#endif /* malloc_usable_size */
    pnl[0] = pnl_bytes2size(bytes);
    assert(pnl[0] >= size);
    pnl += 1;
    *pnl = 0;
  }
  return pnl;
}

MDBX_INTERNAL void pnl_free(pnl_t pnl) {
  if (likely(pnl))
    osal_free(pnl - 1);
}

MDBX_INTERNAL void pnl_shrink(pnl_t __restrict *__restrict ppnl) {
  assert(pnl_bytes2size(pnl_size2bytes(MDBX_PNL_INITIAL)) >= MDBX_PNL_INITIAL &&
         pnl_bytes2size(pnl_size2bytes(MDBX_PNL_INITIAL)) <
             MDBX_PNL_INITIAL * 3 / 2);
  assert(MDBX_PNL_GETSIZE(*ppnl) <= PAGELIST_LIMIT &&
         MDBX_PNL_ALLOCLEN(*ppnl) >= MDBX_PNL_GETSIZE(*ppnl));
  MDBX_PNL_SETSIZE(*ppnl, 0);
  if (unlikely(MDBX_PNL_ALLOCLEN(*ppnl) >
               MDBX_PNL_INITIAL * (MDBX_PNL_PREALLOC_FOR_RADIXSORT ? 8 : 4) -
                   MDBX_CACHELINE_SIZE / sizeof(pgno_t))) {
    size_t bytes = pnl_size2bytes(MDBX_PNL_INITIAL * 2);
    pnl_t pnl = osal_realloc(*ppnl - 1, bytes);
    if (likely(pnl)) {
#if __GLIBC_PREREQ(2, 12) || defined(__FreeBSD__) || defined(malloc_usable_size)
      bytes = malloc_usable_size(pnl);
#endif /* malloc_usable_size */
      *pnl = pnl_bytes2size(bytes);
      *ppnl = pnl + 1;
    }
  }
}

MDBX_INTERNAL int pnl_reserve(pnl_t __restrict *__restrict ppnl,
                              const size_t wanna) {
  const size_t allocated = MDBX_PNL_ALLOCLEN(*ppnl);
  assert(MDBX_PNL_GETSIZE(*ppnl) <= PAGELIST_LIMIT &&
         MDBX_PNL_ALLOCLEN(*ppnl) >= MDBX_PNL_GETSIZE(*ppnl));
  if (likely(allocated >= wanna))
    return MDBX_SUCCESS;

  if (unlikely(wanna > /* paranoia */ PAGELIST_LIMIT)) {
    ERROR("PNL too long (%zu > %zu)", wanna, (size_t)PAGELIST_LIMIT);
    return MDBX_TXN_FULL;
  }

  const size_t size = (wanna + wanna - allocated < PAGELIST_LIMIT)
                          ? wanna + wanna - allocated
                          : PAGELIST_LIMIT;
  size_t bytes = pnl_size2bytes(size);
  pnl_t pnl = osal_realloc(*ppnl - 1, bytes);
  if (likely(pnl)) {
#if __GLIBC_PREREQ(2, 12) || defined(__FreeBSD__) || defined(malloc_usable_size)
    bytes = malloc_usable_size(pnl);
#endif /* malloc_usable_size */
    *pnl = pnl_bytes2size(bytes);
    assert(*pnl >= wanna);
    *ppnl = pnl + 1;
    return MDBX_SUCCESS;
  }
  return MDBX_ENOMEM;
}

static __always_inline int __must_check_result pnl_append_stepped(
    unsigned step, __restrict pnl_t *ppnl, pgno_t pgno, size_t n) {
  assert(n > 0);
  int rc = pnl_need(ppnl, n);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  const pnl_t pnl = *ppnl;
  if (likely(n == 1)) {
    pnl_append_prereserved(pnl, pgno);
    return MDBX_SUCCESS;
  }

#if MDBX_PNL_ASCENDING
  size_t w = MDBX_PNL_GETSIZE(pnl);
  do {
    pnl[++w] = pgno;
    pgno += step;
  } while (--n);
  MDBX_PNL_SETSIZE(pnl, w);
#else
  size_t w = MDBX_PNL_GETSIZE(pnl) + n;
  MDBX_PNL_SETSIZE(pnl, w);
  do {
    pnl[w--] = pgno;
    pgno += step;
  } while (--n);
#endif
  return MDBX_SUCCESS;
}

__hot MDBX_INTERNAL int __must_check_result
spill_append_span(__restrict pnl_t *ppnl, pgno_t pgno, size_t n) {
  return pnl_append_stepped(2, ppnl, pgno << 1, n);
}

__hot MDBX_INTERNAL int __must_check_result
pnl_append_span(__restrict pnl_t *ppnl, pgno_t pgno, size_t n) {
  return pnl_append_stepped(1, ppnl, pgno, n);
}

__hot MDBX_INTERNAL int __must_check_result
pnl_insert_span(__restrict pnl_t *ppnl, pgno_t pgno, size_t n) {
  assert(n > 0);
  int rc = pnl_need(ppnl, n);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  const pnl_t pnl = *ppnl;
  size_t r = MDBX_PNL_GETSIZE(pnl), w = r + n;
  MDBX_PNL_SETSIZE(pnl, w);
  while (r && MDBX_PNL_DISORDERED(pnl[r], pgno))
    pnl[w--] = pnl[r--];

  for (pgno_t fill = MDBX_PNL_ASCENDING ? pgno + n : pgno; w > r; --w)
    pnl[w] = MDBX_PNL_ASCENDING ? --fill : fill++;

  return MDBX_SUCCESS;
}

__hot __noinline MDBX_INTERNAL bool pnl_check(const const_pnl_t pnl,
                                              const size_t limit) {
  assert(limit >= MIN_PAGENO - MDBX_ENABLE_REFUND);
  if (likely(MDBX_PNL_GETSIZE(pnl))) {
    if (unlikely(MDBX_PNL_GETSIZE(pnl) > PAGELIST_LIMIT))
      return false;
    if (unlikely(MDBX_PNL_LEAST(pnl) < MIN_PAGENO))
      return false;
    if (unlikely(MDBX_PNL_MOST(pnl) >= limit))
      return false;

    if ((!MDBX_DISABLE_VALIDATION || AUDIT_ENABLED()) &&
        likely(MDBX_PNL_GETSIZE(pnl) > 1)) {
      const pgno_t *scan = MDBX_PNL_BEGIN(pnl);
      const pgno_t *const end = MDBX_PNL_END(pnl);
      pgno_t prev = *scan++;
      do {
        if (unlikely(!MDBX_PNL_ORDERED(prev, *scan)))
          return false;
        prev = *scan;
      } while (likely(++scan != end));
    }
  }
  return true;
}

static __always_inline void
pnl_merge_inner(pgno_t *__restrict dst, const pgno_t *__restrict src_a,
                const pgno_t *__restrict src_b,
                const pgno_t *__restrict const src_b_detent) {
  do {
#if MDBX_HAVE_CMOV
    const bool flag = MDBX_PNL_ORDERED(*src_b, *src_a);
#if defined(__LCC__) || __CLANG_PREREQ(13, 0)
    // lcc 1.26: 13ШК (подготовка и первая итерация) + 7ШК (цикл), БЕЗ loop-mode
    // gcc>=7: cmp+jmp с возвратом в тело цикла (WTF?)
    // gcc<=6: cmov×3
    // clang<=12: cmov×3
    // clang>=13: cmov, set+add/sub
    *dst = flag ? *src_a-- : *src_b--;
#else
    // gcc: cmov, cmp+set+add/sub
    // clang<=5: cmov×2, set+add/sub
    // clang>=6: cmov, set+add/sub
    *dst = flag ? *src_a : *src_b;
    src_b += (ptrdiff_t)flag - 1;
    src_a -= flag;
#endif
    --dst;
#else  /* MDBX_HAVE_CMOV */
    while (MDBX_PNL_ORDERED(*src_b, *src_a))
      *dst-- = *src_a--;
    *dst-- = *src_b--;
#endif /* !MDBX_HAVE_CMOV */
  } while (likely(src_b > src_b_detent));
}

__hot MDBX_INTERNAL size_t pnl_merge(pnl_t dst, const pnl_t src) {
  assert(pnl_check_allocated(dst, MAX_PAGENO + 1));
  assert(pnl_check(src, MAX_PAGENO + 1));
  const size_t src_len = MDBX_PNL_GETSIZE(src);
  const size_t dst_len = MDBX_PNL_GETSIZE(dst);
  size_t total = dst_len;
  assert(MDBX_PNL_ALLOCLEN(dst) >= total);
  if (likely(src_len > 0)) {
    total += src_len;
    if (!MDBX_DEBUG && total < (MDBX_HAVE_CMOV ? 21 : 12))
      goto avoid_call_libc_for_short_cases;
    if (dst_len == 0 ||
        MDBX_PNL_ORDERED(MDBX_PNL_LAST(dst), MDBX_PNL_FIRST(src)))
      memcpy(MDBX_PNL_END(dst), MDBX_PNL_BEGIN(src), src_len * sizeof(pgno_t));
    else if (MDBX_PNL_ORDERED(MDBX_PNL_LAST(src), MDBX_PNL_FIRST(dst))) {
      memmove(MDBX_PNL_BEGIN(dst) + src_len, MDBX_PNL_BEGIN(dst),
              dst_len * sizeof(pgno_t));
      memcpy(MDBX_PNL_BEGIN(dst), MDBX_PNL_BEGIN(src),
             src_len * sizeof(pgno_t));
    } else {
    avoid_call_libc_for_short_cases:
      dst[0] = /* the detent */ (MDBX_PNL_ASCENDING ? 0 : P_INVALID);
      pnl_merge_inner(dst + total, dst + dst_len, src + src_len, src);
    }
    MDBX_PNL_SETSIZE(dst, total);
  }
  assert(pnl_check_allocated(dst, MAX_PAGENO + 1));
  return total;
}

#if MDBX_PNL_ASCENDING
#define MDBX_PNL_EXTRACT_KEY(ptr) (*(ptr))
#else
#define MDBX_PNL_EXTRACT_KEY(ptr) (P_INVALID - *(ptr))
#endif
RADIXSORT_IMPL(pgno, pgno_t, MDBX_PNL_EXTRACT_KEY,
               MDBX_PNL_PREALLOC_FOR_RADIXSORT, 0)

SORT_IMPL(pgno_sort, false, pgno_t, MDBX_PNL_ORDERED)

__hot __noinline MDBX_INTERNAL void pnl_sort_nochk(pnl_t pnl) {
  if (likely(MDBX_PNL_GETSIZE(pnl) < MDBX_RADIXSORT_THRESHOLD) ||
      unlikely(!pgno_radixsort(&MDBX_PNL_FIRST(pnl), MDBX_PNL_GETSIZE(pnl))))
    pgno_sort(MDBX_PNL_BEGIN(pnl), MDBX_PNL_END(pnl));
}

SEARCH_IMPL(pgno_bsearch, pgno_t, pgno_t, MDBX_PNL_ORDERED)

__hot __noinline MDBX_INTERNAL size_t pnl_search_nochk(const pnl_t pnl,
                                                       pgno_t pgno) {
  const pgno_t *begin = MDBX_PNL_BEGIN(pnl);
  const pgno_t *it = pgno_bsearch(begin, MDBX_PNL_GETSIZE(pnl), pgno);
  const pgno_t *end = begin + MDBX_PNL_GETSIZE(pnl);
  assert(it >= begin && it <= end);
  if (it != begin)
    assert(MDBX_PNL_ORDERED(it[-1], pgno));
  if (it != end)
    assert(!MDBX_PNL_ORDERED(it[0], pgno));
  return it - begin + 1;
}
