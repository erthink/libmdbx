/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025

#include "internals.h"

static inline size_t dpl_size2bytes(ptrdiff_t size) {
  assert(size > CURSOR_STACK_SIZE && (size_t)size <= PAGELIST_LIMIT);
#if MDBX_DPL_PREALLOC_FOR_RADIXSORT
  size += size;
#endif /* MDBX_DPL_PREALLOC_FOR_RADIXSORT */
  STATIC_ASSERT(MDBX_ASSUME_MALLOC_OVERHEAD + sizeof(dpl_t) +
                    (PAGELIST_LIMIT * (MDBX_DPL_PREALLOC_FOR_RADIXSORT + 1)) * sizeof(dp_t) +
                    MDBX_PNL_GRANULATE * sizeof(void *) * 2 <
                SIZE_MAX / 4 * 3);
  size_t bytes = ceil_powerof2(MDBX_ASSUME_MALLOC_OVERHEAD + sizeof(dpl_t) + size * sizeof(dp_t),
                               MDBX_PNL_GRANULATE * sizeof(void *) * 2) -
                 MDBX_ASSUME_MALLOC_OVERHEAD;
  return bytes;
}

static inline size_t dpl_bytes2size(const ptrdiff_t bytes) {
  size_t size = (bytes - sizeof(dpl_t)) / sizeof(dp_t);
#if MDBX_DPL_PREALLOC_FOR_RADIXSORT
  size >>= 1;
#endif /* MDBX_DPL_PREALLOC_FOR_RADIXSORT */
  assert(size > CURSOR_STACK_SIZE && size <= PAGELIST_LIMIT + MDBX_PNL_GRANULATE);
  return size;
}

void dpl_free(MDBX_txn *txn) {
  if (likely(txn->wr.dirtylist)) {
    osal_free(txn->wr.dirtylist);
    txn->wr.dirtylist = nullptr;
  }
}

dpl_t *dpl_reserve(MDBX_txn *txn, size_t size) {
  tASSERT(txn, (txn->flags & MDBX_TXN_RDONLY) == 0);
  tASSERT(txn, (txn->flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);

  size_t bytes = dpl_size2bytes((size < PAGELIST_LIMIT) ? size : PAGELIST_LIMIT);
  dpl_t *const dl = osal_realloc(txn->wr.dirtylist, bytes);
  if (likely(dl)) {
#ifdef osal_malloc_usable_size
    bytes = osal_malloc_usable_size(dl);
#endif /* osal_malloc_usable_size */
    dl->detent = dpl_bytes2size(bytes);
    tASSERT(txn, txn->wr.dirtylist == nullptr || dl->length <= dl->detent);
    txn->wr.dirtylist = dl;
  }
  return dl;
}

int dpl_alloc(MDBX_txn *txn) {
  tASSERT(txn, (txn->flags & MDBX_TXN_RDONLY) == 0);
  tASSERT(txn, (txn->flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);

  const size_t wanna = (txn->env->options.dp_initial < txn->geo.upper) ? txn->env->options.dp_initial : txn->geo.upper;
#if MDBX_FORCE_ASSERTIONS || MDBX_DEBUG
  if (txn->wr.dirtylist)
    /* обнуляем чтобы не сработал ассерт внутри dpl_reserve() */
    txn->wr.dirtylist->sorted = txn->wr.dirtylist->length = 0;
#endif /* asertions enabled */
  if (unlikely(!txn->wr.dirtylist || txn->wr.dirtylist->detent < wanna || txn->wr.dirtylist->detent > wanna + wanna) &&
      unlikely(!dpl_reserve(txn, wanna)))
    return MDBX_ENOMEM;

  dpl_clear(txn->wr.dirtylist);
  return MDBX_SUCCESS;
}

#define MDBX_DPL_EXTRACT_KEY(ptr) ((ptr)->pgno)
RADIXSORT_IMPL(dp, dp_t, MDBX_DPL_EXTRACT_KEY, MDBX_DPL_PREALLOC_FOR_RADIXSORT, 1)

#define DP_SORT_CMP(first, last) ((first).pgno < (last).pgno)
SORT_IMPL(dp_sort, false, dp_t, DP_SORT_CMP)

__hot __noinline dpl_t *dpl_sort_slowpath(const MDBX_txn *txn) {
  tASSERT(txn, (txn->flags & MDBX_TXN_RDONLY) == 0);
  tASSERT(txn, (txn->flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);

  dpl_t *dl = txn->wr.dirtylist;
  assert(dl->items[0].pgno == 0 && dl->items[dl->length + 1].pgno == P_INVALID);
  const size_t unsorted = dl->length - dl->sorted;
  if (likely(unsorted < MDBX_RADIXSORT_THRESHOLD) || unlikely(!dp_radixsort(dl->items + 1, dl->length))) {
    if (dl->sorted > unsorted / 4 + 4 &&
        (MDBX_DPL_PREALLOC_FOR_RADIXSORT || dl->length + unsorted < dl->detent + dpl_gap_mergesort)) {
      dp_t *const sorted_begin = dl->items + 1;
      dp_t *const sorted_end = sorted_begin + dl->sorted;
      dp_t *const end =
          dl->items + (MDBX_DPL_PREALLOC_FOR_RADIXSORT ? dl->length + dl->length + 1 : dl->detent + dpl_reserve_gap);
      dp_t *const tmp = end - unsorted;
      assert(dl->items + dl->length + 1 < tmp);
      /* copy unsorted to the end of allocated space and sort it */
      memcpy(tmp, sorted_end, unsorted * sizeof(dp_t));
      dp_sort(tmp, tmp + unsorted);
      /* merge two parts from end to begin */
      dp_t *__restrict w = dl->items + dl->length;
      dp_t *__restrict l = dl->items + dl->sorted;
      dp_t *__restrict r = end - 1;
      do {
        const bool cmp = expect_with_probability(l->pgno > r->pgno, 0, .5);
#if defined(__LCC__) || __CLANG_PREREQ(13, 0) || !MDBX_HAVE_CMOV
        *w = cmp ? *l-- : *r--;
#else
        *w = cmp ? *l : *r;
        l -= cmp;
        r += (ptrdiff_t)cmp - 1;
#endif
      } while (likely(--w > l));
      assert(r == tmp - 1);
      assert(dl->items[0].pgno == 0 && dl->items[dl->length + 1].pgno == P_INVALID);
      if (ASSERT_ENABLED())
        for (size_t i = 0; i <= dl->length; ++i)
          assert(dl->items[i].pgno < dl->items[i + 1].pgno);
    } else {
      dp_sort(dl->items + 1, dl->items + dl->length + 1);
      assert(dl->items[0].pgno == 0 && dl->items[dl->length + 1].pgno == P_INVALID);
    }
  } else {
    assert(dl->items[0].pgno == 0 && dl->items[dl->length + 1].pgno == P_INVALID);
  }
  dl->sorted = dl->length;
  return dl;
}

/* Returns the index of the first dirty-page whose pgno
 * member is greater than or equal to id. */
#define DP_SEARCH_CMP(dp, id) ((dp).pgno < (id))
SEARCH_IMPL(dp_bsearch, dp_t, pgno_t, DP_SEARCH_CMP)

__hot __noinline size_t dpl_search(const MDBX_txn *txn, pgno_t pgno) {
  tASSERT(txn, (txn->flags & MDBX_TXN_RDONLY) == 0);
  tASSERT(txn, (txn->flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);

  dpl_t *dl = txn->wr.dirtylist;
  assert(dl->items[0].pgno == 0 && dl->items[dl->length + 1].pgno == P_INVALID);
  if (AUDIT_ENABLED()) {
    for (const dp_t *ptr = dl->items + dl->sorted; --ptr > dl->items;) {
      assert(ptr[0].pgno < ptr[1].pgno);
      assert(ptr[0].pgno >= NUM_METAS);
    }
  }

  switch (dl->length - dl->sorted) {
  default:
    /* sort a whole */
    dpl_sort_slowpath(txn);
    break;
  case 0:
    /* whole sorted cases */
    break;

#define LINEAR_SEARCH_CASE(N)                                                                                          \
  case N:                                                                                                              \
    if (dl->items[dl->length - N + 1].pgno == pgno)                                                                    \
      return dl->length - N + 1;                                                                                       \
    __fallthrough

    /* use linear scan until the threshold */
    LINEAR_SEARCH_CASE(7); /* fall through */
    LINEAR_SEARCH_CASE(6); /* fall through */
    LINEAR_SEARCH_CASE(5); /* fall through */
    LINEAR_SEARCH_CASE(4); /* fall through */
    LINEAR_SEARCH_CASE(3); /* fall through */
    LINEAR_SEARCH_CASE(2); /* fall through */
  case 1:
    if (dl->items[dl->length].pgno == pgno)
      return dl->length;
    /* continue bsearch on the sorted part */
    break;
  }
  return dp_bsearch(dl->items + 1, dl->sorted, pgno) - dl->items;
}

const page_t *debug_dpl_find(const MDBX_txn *txn, const pgno_t pgno) {
  tASSERT(txn, (txn->flags & MDBX_TXN_RDONLY) == 0);
  const dpl_t *dl = txn->wr.dirtylist;
  if (dl) {
    tASSERT(txn, (txn->flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);
    assert(dl->items[0].pgno == 0 && dl->items[dl->length + 1].pgno == P_INVALID);
    for (size_t i = dl->length; i > dl->sorted; --i)
      if (dl->items[i].pgno == pgno)
        return dl->items[i].ptr;

    if (dl->sorted) {
      const size_t i = dp_bsearch(dl->items + 1, dl->sorted, pgno) - dl->items;
      if (dl->items[i].pgno == pgno)
        return dl->items[i].ptr;
    }
  } else {
    tASSERT(txn, (txn->flags & MDBX_WRITEMAP) != 0 && !MDBX_AVOID_MSYNC);
  }
  return nullptr;
}

void dpl_remove_ex(const MDBX_txn *txn, size_t i, size_t npages) {
  tASSERT(txn, (txn->flags & MDBX_TXN_RDONLY) == 0);
  tASSERT(txn, (txn->flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);

  dpl_t *dl = txn->wr.dirtylist;
  assert((intptr_t)i > 0 && i <= dl->length);
  assert(dl->items[0].pgno == 0 && dl->items[dl->length + 1].pgno == P_INVALID);
  dl->pages_including_loose -= npages;
  dl->sorted -= dl->sorted >= i;
  dl->length -= 1;
  memmove(dl->items + i, dl->items + i + 1, (dl->length - i + 2) * sizeof(dl->items[0]));
  assert(dl->items[0].pgno == 0 && dl->items[dl->length + 1].pgno == P_INVALID);
}

int __must_check_result dpl_append(MDBX_txn *txn, pgno_t pgno, page_t *page, size_t npages) {
  tASSERT(txn, (txn->flags & MDBX_TXN_RDONLY) == 0);
  tASSERT(txn, (txn->flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);
  const dp_t dp = {page, pgno, (pgno_t)npages};
  if ((txn->flags & MDBX_WRITEMAP) == 0) {
    size_t *const ptr = ptr_disp(page, -(ptrdiff_t)sizeof(size_t));
    *ptr = txn->wr.dirtylru;
  }

  dpl_t *dl = txn->wr.dirtylist;
  tASSERT(txn, dl->length <= PAGELIST_LIMIT + MDBX_PNL_GRANULATE);
  tASSERT(txn, dl->items[0].pgno == 0 && dl->items[dl->length + 1].pgno == P_INVALID);
  if (AUDIT_ENABLED()) {
    for (size_t i = dl->length; i > 0; --i) {
      assert(dl->items[i].pgno != dp.pgno);
      if (unlikely(dl->items[i].pgno == dp.pgno)) {
        ERROR("Page %u already exist in the DPL at %zu", dp.pgno, i);
        return MDBX_PROBLEM;
      }
    }
  }

  if (unlikely(dl->length == dl->detent)) {
    if (unlikely(dl->detent >= PAGELIST_LIMIT)) {
      ERROR("DPL is full (PAGELIST_LIMIT %zu)", PAGELIST_LIMIT);
      return MDBX_TXN_FULL;
    }
    const size_t size = (dl->detent < MDBX_PNL_INITIAL * 42) ? dl->detent + dl->detent : dl->detent + dl->detent / 2;
    dl = dpl_reserve(txn, size);
    if (unlikely(!dl))
      return MDBX_ENOMEM;
    tASSERT(txn, dl->length < dl->detent);
  }

  /* Сортировка нужна для быстрого поиска, используем несколько тактик:
   *  1) Сохраняем упорядоченность при естественной вставке в нужном порядке.
   *  2) Добавляем в не-сортированный хвост, который сортируем и сливаем
   *     с отсортированной головой по необходимости, а пока хвост короткий
   *     ищем в нём сканированием, избегая большой пересортировки.
   *  3) Если не-сортированный хвост короткий, а добавляемый элемент близок
   *     к концу отсортированной головы, то выгоднее сразу вставить элемент
   *     в нужное место.
   *
   * Алгоритмически:
   *  - добавлять в не-сортированный хвост следует только если вставка сильно
   *    дорогая, т.е. если целевая позиция элемента сильно далека от конца;
   *  - для быстрой проверки достаточно сравнить добавляемый элемент с отстоящим
   *    от конца на максимально-приемлемое расстояние;
   *  - если список короче, либо элемент в этой позиции меньше вставляемого,
   *    то следует перемещать элементы и вставлять в отсортированную голову;
   *  - если не-сортированный хвост длиннее, либо элемент в этой позиции больше,
   *    то следует добавлять в не-сортированный хвост. */

  dl->pages_including_loose += npages;
  dp_t *i = dl->items + dl->length;

  const ptrdiff_t pivot = (ptrdiff_t)dl->length - dpl_insertion_threshold;
#if MDBX_HAVE_CMOV
  const pgno_t pivot_pgno =
      dl->items[(dl->length < dpl_insertion_threshold) ? 0 : dl->length - dpl_insertion_threshold].pgno;
#endif /* MDBX_HAVE_CMOV */

  /* copy the stub beyond the end */
  i[2] = i[1];
  dl->length += 1;

  if (likely(pivot <= (ptrdiff_t)dl->sorted) &&
#if MDBX_HAVE_CMOV
      pivot_pgno < dp.pgno) {
#else
      (pivot <= 0 || dl->items[pivot].pgno < dp.pgno)) {
#endif /* MDBX_HAVE_CMOV */
    dl->sorted += 1;

    /* сдвигаем несортированный хвост */
    while (i >= dl->items + dl->sorted) {
#if !defined(__GNUC__) /* пытаемся избежать вызова memmove() */
      i[1] = *i;
#elif MDBX_WORDBITS == 64 && (defined(__SIZEOF_INT128__) || (defined(_INTEGRAL_MAX_BITS) && _INTEGRAL_MAX_BITS >= 128))
      STATIC_ASSERT(sizeof(dp) == sizeof(__uint128_t));
      ((__uint128_t *)i)[1] = *(volatile __uint128_t *)i;
#else
    i[1].ptr = i->ptr;
    i[1].pgno = i->pgno;
    i[1].npages = i->npages;
#endif
      --i;
    }
    /* ищем нужную позицию сдвигая отсортированные элементы */
    while (i->pgno > pgno) {
      tASSERT(txn, i > dl->items);
      i[1] = *i;
      --i;
    }
    tASSERT(txn, i->pgno < dp.pgno);
  }

  i[1] = dp;
  assert(dl->items[0].pgno == 0 && dl->items[dl->length + 1].pgno == P_INVALID);
  assert(dl->sorted <= dl->length);
  return MDBX_SUCCESS;
}

__cold bool dpl_check(MDBX_txn *txn) {
  tASSERT(txn, (txn->flags & MDBX_TXN_RDONLY) == 0);
  const dpl_t *const dl = txn->wr.dirtylist;
  if (!dl) {
    tASSERT(txn, (txn->flags & MDBX_WRITEMAP) != 0 && !MDBX_AVOID_MSYNC);
    return true;
  }
  tASSERT(txn, (txn->flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);

  assert(dl->items[0].pgno == 0 && dl->items[dl->length + 1].pgno == P_INVALID);
  tASSERT(txn,
          txn->wr.dirtyroom + dl->length == (txn->parent ? txn->parent->wr.dirtyroom : txn->env->options.dp_limit));

  if (!AUDIT_ENABLED())
    return true;

  size_t loose = 0, pages = 0;
  for (size_t i = dl->length; i > 0; --i) {
    const page_t *const dp = dl->items[i].ptr;
    if (!dp)
      continue;

    tASSERT(txn, dp->pgno == dl->items[i].pgno);
    if (unlikely(dp->pgno != dl->items[i].pgno))
      return false;

    if ((txn->flags & MDBX_WRITEMAP) == 0) {
      const uint32_t age = dpl_age(txn, i);
      tASSERT(txn, age < UINT32_MAX / 3);
      if (unlikely(age > UINT32_MAX / 3))
        return false;
    }

    tASSERT(txn, dp->flags == P_LOOSE || is_modifable(txn, dp));
    if (dp->flags == P_LOOSE) {
      loose += 1;
    } else if (unlikely(!is_modifable(txn, dp)))
      return false;

    const unsigned num = dpl_npages(dl, i);
    pages += num;
    tASSERT(txn, txn->geo.first_unallocated >= dp->pgno + num);
    if (unlikely(txn->geo.first_unallocated < dp->pgno + num))
      return false;

    if (i < dl->sorted) {
      tASSERT(txn, dl->items[i + 1].pgno >= dp->pgno + num);
      if (unlikely(dl->items[i + 1].pgno < dp->pgno + num))
        return false;
    }

    const size_t rpa = pnl_search(txn->wr.repnl, dp->pgno, txn->geo.first_unallocated);
    tASSERT(txn, rpa > MDBX_PNL_GETSIZE(txn->wr.repnl) || txn->wr.repnl[rpa] != dp->pgno);
    if (rpa <= MDBX_PNL_GETSIZE(txn->wr.repnl) && unlikely(txn->wr.repnl[rpa] == dp->pgno))
      return false;
    if (num > 1) {
      const size_t rpb = pnl_search(txn->wr.repnl, dp->pgno + num - 1, txn->geo.first_unallocated);
      tASSERT(txn, rpa == rpb);
      if (unlikely(rpa != rpb))
        return false;
    }
  }

  tASSERT(txn, loose == txn->wr.loose_count);
  if (unlikely(loose != txn->wr.loose_count))
    return false;

  tASSERT(txn, pages == dl->pages_including_loose);
  if (unlikely(pages != dl->pages_including_loose))
    return false;

  for (size_t i = 1; i <= MDBX_PNL_GETSIZE(txn->wr.retired_pages); ++i) {
    const page_t *const dp = debug_dpl_find(txn, txn->wr.retired_pages[i]);
    tASSERT(txn, !dp);
    if (unlikely(dp))
      return false;
  }

  return true;
}

/*----------------------------------------------------------------------------*/

__noinline void dpl_lru_reduce(MDBX_txn *txn) {
  VERBOSE("lru-reduce %u -> %u", txn->wr.dirtylru, txn->wr.dirtylru >> 1);
  tASSERT(txn, (txn->flags & (MDBX_TXN_RDONLY | MDBX_WRITEMAP)) == 0);
  do {
    txn->wr.dirtylru >>= 1;
    dpl_t *dl = txn->wr.dirtylist;
    for (size_t i = 1; i <= dl->length; ++i) {
      size_t *const ptr = ptr_disp(dl->items[i].ptr, -(ptrdiff_t)sizeof(size_t));
      *ptr >>= 1;
    }
    txn = txn->parent;
  } while (txn);
}

void dpl_sift(MDBX_txn *const txn, pnl_t pl, const bool spilled) {
  tASSERT(txn, (txn->flags & MDBX_TXN_RDONLY) == 0);
  tASSERT(txn, (txn->flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC);
  if (MDBX_PNL_GETSIZE(pl) && txn->wr.dirtylist->length) {
    tASSERT(txn, pnl_check_allocated(pl, (size_t)txn->geo.first_unallocated << spilled));
    dpl_t *dl = dpl_sort(txn);

    /* Scanning in ascend order */
    const intptr_t step = MDBX_PNL_ASCENDING ? 1 : -1;
    const intptr_t begin = MDBX_PNL_ASCENDING ? 1 : MDBX_PNL_GETSIZE(pl);
    const intptr_t end = MDBX_PNL_ASCENDING ? MDBX_PNL_GETSIZE(pl) + 1 : 0;
    tASSERT(txn, pl[begin] <= pl[end - step]);

    size_t w, r = dpl_search(txn, pl[begin] >> spilled);
    tASSERT(txn, dl->sorted == dl->length);
    for (intptr_t i = begin; r <= dl->length;) { /* scan loop */
      assert(i != end);
      tASSERT(txn, !spilled || (pl[i] & 1) == 0);
      pgno_t pl_pgno = pl[i] >> spilled;
      pgno_t dp_pgno = dl->items[r].pgno;
      if (likely(dp_pgno != pl_pgno)) {
        const bool cmp = dp_pgno < pl_pgno;
        r += cmp;
        i += cmp ? 0 : step;
        if (likely(i != end))
          continue;
        return;
      }

      /* update loop */
      unsigned npages;
      w = r;
    remove_dl:
      npages = dpl_npages(dl, r);
      dl->pages_including_loose -= npages;
      if (!MDBX_AVOID_MSYNC || !(txn->flags & MDBX_WRITEMAP))
        page_shadow_release(txn->env, dl->items[r].ptr, npages);
      ++r;
    next_i:
      i += step;
      if (unlikely(i == end)) {
        while (r <= dl->length)
          dl->items[w++] = dl->items[r++];
      } else {
        while (r <= dl->length) {
          assert(i != end);
          tASSERT(txn, !spilled || (pl[i] & 1) == 0);
          pl_pgno = pl[i] >> spilled;
          dp_pgno = dl->items[r].pgno;
          if (dp_pgno < pl_pgno)
            dl->items[w++] = dl->items[r++];
          else if (dp_pgno > pl_pgno)
            goto next_i;
          else
            goto remove_dl;
        }
      }
      dl->sorted = dpl_setlen(dl, w - 1);
      txn->wr.dirtyroom += r - w;
      tASSERT(txn, txn->wr.dirtyroom + txn->wr.dirtylist->length ==
                       (txn->parent ? txn->parent->wr.dirtyroom : txn->env->options.dp_limit));
      return;
    }
  }
}

void dpl_release_shadows(MDBX_txn *txn) {
  tASSERT(txn, (txn->flags & (MDBX_TXN_RDONLY | MDBX_WRITEMAP)) == 0);
  MDBX_env *env = txn->env;
  dpl_t *const dl = txn->wr.dirtylist;

  for (size_t i = 1; i <= dl->length; i++)
    page_shadow_release(env, dl->items[i].ptr, dpl_npages(dl, i));

  dpl_clear(dl);
}
