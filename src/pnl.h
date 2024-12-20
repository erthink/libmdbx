/// \copyright SPDX-License-Identifier: Apache-2.0
/// \note Please refer to the COPYRIGHT file for explanations license change,
/// credits and acknowledgments.
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025

#pragma once

#include "essentials.h"

/* An PNL is an Page Number List, a sorted array of IDs.
 *
 * The first element of the array is a counter for how many actual page-numbers
 * are in the list. By default PNLs are sorted in descending order, this allow
 * cut off a page with lowest pgno (at the tail) just truncating the list. The
 * sort order of PNLs is controlled by the MDBX_PNL_ASCENDING build option. */
typedef pgno_t *pnl_t;
typedef const pgno_t *const_pnl_t;

#if MDBX_PNL_ASCENDING
#define MDBX_PNL_ORDERED(first, last) ((first) < (last))
#define MDBX_PNL_DISORDERED(first, last) ((first) >= (last))
#else
#define MDBX_PNL_ORDERED(first, last) ((first) > (last))
#define MDBX_PNL_DISORDERED(first, last) ((first) <= (last))
#endif

#define MDBX_PNL_GRANULATE_LOG2 10
#define MDBX_PNL_GRANULATE (1 << MDBX_PNL_GRANULATE_LOG2)
#define MDBX_PNL_INITIAL (MDBX_PNL_GRANULATE - 2 - MDBX_ASSUME_MALLOC_OVERHEAD / sizeof(pgno_t))

#define MDBX_PNL_ALLOCLEN(pl) ((pl)[-1])
#define MDBX_PNL_GETSIZE(pl) ((size_t)((pl)[0]))
#define MDBX_PNL_SETSIZE(pl, size)                                                                                     \
  do {                                                                                                                 \
    const size_t __size = size;                                                                                        \
    assert(__size < INT_MAX);                                                                                          \
    (pl)[0] = (pgno_t)__size;                                                                                          \
  } while (0)
#define MDBX_PNL_FIRST(pl) ((pl)[1])
#define MDBX_PNL_LAST(pl) ((pl)[MDBX_PNL_GETSIZE(pl)])
#define MDBX_PNL_BEGIN(pl) (&(pl)[1])
#define MDBX_PNL_END(pl) (&(pl)[MDBX_PNL_GETSIZE(pl) + 1])

#if MDBX_PNL_ASCENDING
#define MDBX_PNL_EDGE(pl) ((pl) + 1)
#define MDBX_PNL_LEAST(pl) MDBX_PNL_FIRST(pl)
#define MDBX_PNL_MOST(pl) MDBX_PNL_LAST(pl)
#define MDBX_PNL_CONTIGUOUS(prev, next, span) ((next) - (prev)) == (span))
#else
#define MDBX_PNL_EDGE(pl) ((pl) + MDBX_PNL_GETSIZE(pl))
#define MDBX_PNL_LEAST(pl) MDBX_PNL_LAST(pl)
#define MDBX_PNL_MOST(pl) MDBX_PNL_FIRST(pl)
#define MDBX_PNL_CONTIGUOUS(prev, next, span) (((prev) - (next)) == (span))
#endif

#define MDBX_PNL_SIZEOF(pl) ((MDBX_PNL_GETSIZE(pl) + 1) * sizeof(pgno_t))
#define MDBX_PNL_IS_EMPTY(pl) (MDBX_PNL_GETSIZE(pl) == 0)

MDBX_MAYBE_UNUSED static inline size_t pnl_size2bytes(size_t size) {
  assert(size > 0 && size <= PAGELIST_LIMIT);
#if MDBX_PNL_PREALLOC_FOR_RADIXSORT

  size += size;
#endif /* MDBX_PNL_PREALLOC_FOR_RADIXSORT */
  STATIC_ASSERT(MDBX_ASSUME_MALLOC_OVERHEAD +
                    (PAGELIST_LIMIT * (MDBX_PNL_PREALLOC_FOR_RADIXSORT + 1) + MDBX_PNL_GRANULATE + 3) * sizeof(pgno_t) <
                SIZE_MAX / 4 * 3);
  size_t bytes =
      ceil_powerof2(MDBX_ASSUME_MALLOC_OVERHEAD + sizeof(pgno_t) * (size + 3), MDBX_PNL_GRANULATE * sizeof(pgno_t)) -
      MDBX_ASSUME_MALLOC_OVERHEAD;
  return bytes;
}

MDBX_MAYBE_UNUSED static inline pgno_t pnl_bytes2size(const size_t bytes) {
  size_t size = bytes / sizeof(pgno_t);
  assert(size > 3 && size <= PAGELIST_LIMIT + /* alignment gap */ 65536);
  size -= 3;
#if MDBX_PNL_PREALLOC_FOR_RADIXSORT
  size >>= 1;
#endif /* MDBX_PNL_PREALLOC_FOR_RADIXSORT */
  return (pgno_t)size;
}

MDBX_INTERNAL pnl_t pnl_alloc(size_t size);

MDBX_INTERNAL void pnl_free(pnl_t pnl);

MDBX_MAYBE_UNUSED MDBX_INTERNAL pnl_t pnl_clone(const pnl_t src);

MDBX_INTERNAL int pnl_reserve(pnl_t __restrict *__restrict ppnl, const size_t wanna);

MDBX_MAYBE_UNUSED static inline int __must_check_result pnl_need(pnl_t __restrict *__restrict ppnl, size_t num) {
  assert(MDBX_PNL_GETSIZE(*ppnl) <= PAGELIST_LIMIT && MDBX_PNL_ALLOCLEN(*ppnl) >= MDBX_PNL_GETSIZE(*ppnl));
  assert(num <= PAGELIST_LIMIT);
  const size_t wanna = MDBX_PNL_GETSIZE(*ppnl) + num;
  return likely(MDBX_PNL_ALLOCLEN(*ppnl) >= wanna) ? MDBX_SUCCESS : pnl_reserve(ppnl, wanna);
}

MDBX_MAYBE_UNUSED static inline void pnl_append_prereserved(__restrict pnl_t pnl, pgno_t pgno) {
  assert(MDBX_PNL_GETSIZE(pnl) < MDBX_PNL_ALLOCLEN(pnl));
  if (AUDIT_ENABLED()) {
    for (size_t i = MDBX_PNL_GETSIZE(pnl); i > 0; --i)
      assert(pgno != pnl[i]);
  }
  *pnl += 1;
  MDBX_PNL_LAST(pnl) = pgno;
}

MDBX_INTERNAL void pnl_shrink(pnl_t __restrict *__restrict ppnl);

MDBX_INTERNAL int __must_check_result spill_append_span(__restrict pnl_t *ppnl, pgno_t pgno, size_t n);

MDBX_INTERNAL int __must_check_result pnl_append_span(__restrict pnl_t *ppnl, pgno_t pgno, size_t n);

MDBX_INTERNAL int __must_check_result pnl_insert_span(__restrict pnl_t *ppnl, pgno_t pgno, size_t n);

MDBX_INTERNAL size_t pnl_search_nochk(const pnl_t pnl, pgno_t pgno);

MDBX_INTERNAL void pnl_sort_nochk(pnl_t pnl);

MDBX_INTERNAL bool pnl_check(const const_pnl_t pnl, const size_t limit);

MDBX_MAYBE_UNUSED static inline bool pnl_check_allocated(const const_pnl_t pnl, const size_t limit) {
  return pnl == nullptr || (MDBX_PNL_ALLOCLEN(pnl) >= MDBX_PNL_GETSIZE(pnl) && pnl_check(pnl, limit));
}

MDBX_MAYBE_UNUSED static inline void pnl_sort(pnl_t pnl, size_t limit4check) {
  pnl_sort_nochk(pnl);
  assert(pnl_check(pnl, limit4check));
  (void)limit4check;
}

MDBX_MAYBE_UNUSED static inline size_t pnl_search(const pnl_t pnl, pgno_t pgno, size_t limit) {
  assert(pnl_check_allocated(pnl, limit));
  if (MDBX_HAVE_CMOV) {
    /* cmov-ускоренный бинарный поиск может читать (но не использовать) один
     * элемент за концом данных, этот элемент в пределах выделенного участка
     * памяти, но не инициализирован. */
    VALGRIND_MAKE_MEM_DEFINED(MDBX_PNL_END(pnl), sizeof(pgno_t));
  }
  assert(pgno < limit);
  (void)limit;
  size_t n = pnl_search_nochk(pnl, pgno);
  if (MDBX_HAVE_CMOV) {
    VALGRIND_MAKE_MEM_UNDEFINED(MDBX_PNL_END(pnl), sizeof(pgno_t));
  }
  return n;
}

MDBX_INTERNAL size_t pnl_merge(pnl_t dst, const pnl_t src);

MDBX_MAYBE_UNUSED MDBX_NOTHROW_PURE_FUNCTION MDBX_INTERNAL size_t pnl_maxspan(const pnl_t pnl);
