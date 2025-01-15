/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025
///
/// \file sort.h
/// \brief Маркосы реализующие сортировку и двоичный поиск

#pragma once

#define MDBX_RADIXSORT_THRESHOLD 142

/* ---------------------------------------------------------------------------
 * LY: State of the art quicksort-based sorting, with internal stack
 * and network-sort for small chunks.
 * Thanks to John M. Gamble for the http://pages.ripco.net/~jgamble/nw.html */

#if MDBX_HAVE_CMOV
#define SORT_CMP_SWAP(TYPE, CMP, a, b)                                                                                 \
  do {                                                                                                                 \
    const TYPE swap_tmp = (a);                                                                                         \
    const bool swap_cmp = expect_with_probability(CMP(swap_tmp, b), 0, .5);                                            \
    (a) = swap_cmp ? swap_tmp : b;                                                                                     \
    (b) = swap_cmp ? b : swap_tmp;                                                                                     \
  } while (0)
#else
#define SORT_CMP_SWAP(TYPE, CMP, a, b)                                                                                 \
  do                                                                                                                   \
    if (expect_with_probability(!CMP(a, b), 0, .5)) {                                                                  \
      const TYPE swap_tmp = (a);                                                                                       \
      (a) = (b);                                                                                                       \
      (b) = swap_tmp;                                                                                                  \
    }                                                                                                                  \
  while (0)
#endif

//  3 comparators, 3 parallel operations
//  o-----^--^--o
//        |  |
//  o--^--|--v--o
//     |  |
//  o--v--v-----o
//
//  [[1,2]]
//  [[0,2]]
//  [[0,1]]
#define SORT_NETWORK_3(TYPE, CMP, begin)                                                                               \
  do {                                                                                                                 \
    SORT_CMP_SWAP(TYPE, CMP, begin[1], begin[2]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[0], begin[2]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[0], begin[1]);                                                                      \
  } while (0)

//  5 comparators, 3 parallel operations
//  o--^--^--------o
//     |  |
//  o--v--|--^--^--o
//        |  |  |
//  o--^--v--|--v--o
//     |     |
//  o--v-----v-----o
//
//  [[0,1],[2,3]]
//  [[0,2],[1,3]]
//  [[1,2]]
#define SORT_NETWORK_4(TYPE, CMP, begin)                                                                               \
  do {                                                                                                                 \
    SORT_CMP_SWAP(TYPE, CMP, begin[0], begin[1]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[2], begin[3]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[0], begin[2]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[1], begin[3]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[1], begin[2]);                                                                      \
  } while (0)

//  9 comparators, 5 parallel operations
//  o--^--^-----^-----------o
//     |  |     |
//  o--|--|--^--v-----^--^--o
//     |  |  |        |  |
//  o--|--v--|--^--^--|--v--o
//     |     |  |  |  |
//  o--|-----v--|--v--|--^--o
//     |        |     |  |
//  o--v--------v-----v--v--o
//
//  [[0,4],[1,3]]
//  [[0,2]]
//  [[2,4],[0,1]]
//  [[2,3],[1,4]]
//  [[1,2],[3,4]]
#define SORT_NETWORK_5(TYPE, CMP, begin)                                                                               \
  do {                                                                                                                 \
    SORT_CMP_SWAP(TYPE, CMP, begin[0], begin[4]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[1], begin[3]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[0], begin[2]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[2], begin[4]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[0], begin[1]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[2], begin[3]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[1], begin[4]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[1], begin[2]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[3], begin[4]);                                                                      \
  } while (0)

//  12 comparators, 6 parallel operations
//  o-----^--^--^-----------------o
//        |  |  |
//  o--^--|--v--|--^--------^-----o
//     |  |     |  |        |
//  o--v--v-----|--|--^--^--|--^--o
//              |  |  |  |  |  |
//  o-----^--^--v--|--|--|--v--v--o
//        |  |     |  |  |
//  o--^--|--v-----v--|--v--------o
//     |  |           |
//  o--v--v-----------v-----------o
//
//  [[1,2],[4,5]]
//  [[0,2],[3,5]]
//  [[0,1],[3,4],[2,5]]
//  [[0,3],[1,4]]
//  [[2,4],[1,3]]
//  [[2,3]]
#define SORT_NETWORK_6(TYPE, CMP, begin)                                                                               \
  do {                                                                                                                 \
    SORT_CMP_SWAP(TYPE, CMP, begin[1], begin[2]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[4], begin[5]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[0], begin[2]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[3], begin[5]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[0], begin[1]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[3], begin[4]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[2], begin[5]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[0], begin[3]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[1], begin[4]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[2], begin[4]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[1], begin[3]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[2], begin[3]);                                                                      \
  } while (0)

//  16 comparators, 6 parallel operations
//  o--^--------^-----^-----------------o
//     |        |     |
//  o--|--^-----|--^--v--------^--^-----o
//     |  |     |  |           |  |
//  o--|--|--^--v--|--^-----^--|--v-----o
//     |  |  |     |  |     |  |
//  o--|--|--|-----v--|--^--v--|--^--^--o
//     |  |  |        |  |     |  |  |
//  o--v--|--|--^-----v--|--^--v--|--v--o
//        |  |  |        |  |     |
//  o-----v--|--|--------v--v-----|--^--o
//           |  |                 |  |
//  o--------v--v-----------------v--v--o
//
//  [[0,4],[1,5],[2,6]]
//  [[0,2],[1,3],[4,6]]
//  [[2,4],[3,5],[0,1]]
//  [[2,3],[4,5]]
//  [[1,4],[3,6]]
//  [[1,2],[3,4],[5,6]]
#define SORT_NETWORK_7(TYPE, CMP, begin)                                                                               \
  do {                                                                                                                 \
    SORT_CMP_SWAP(TYPE, CMP, begin[0], begin[4]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[1], begin[5]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[2], begin[6]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[0], begin[2]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[1], begin[3]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[4], begin[6]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[2], begin[4]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[3], begin[5]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[0], begin[1]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[2], begin[3]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[4], begin[5]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[1], begin[4]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[3], begin[6]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[1], begin[2]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[3], begin[4]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[5], begin[6]);                                                                      \
  } while (0)

//  19 comparators, 6 parallel operations
//  o--^--------^-----^-----------------o
//     |        |     |
//  o--|--^-----|--^--v--------^--^-----o
//     |  |     |  |           |  |
//  o--|--|--^--v--|--^-----^--|--v-----o
//     |  |  |     |  |     |  |
//  o--|--|--|--^--v--|--^--v--|--^--^--o
//     |  |  |  |     |  |     |  |  |
//  o--v--|--|--|--^--v--|--^--v--|--v--o
//        |  |  |  |     |  |     |
//  o-----v--|--|--|--^--v--v-----|--^--o
//           |  |  |  |           |  |
//  o--------v--|--v--|--^--------v--v--o
//              |     |  |
//  o-----------v-----v--v--------------o
//
//  [[0,4],[1,5],[2,6],[3,7]]
//  [[0,2],[1,3],[4,6],[5,7]]
//  [[2,4],[3,5],[0,1],[6,7]]
//  [[2,3],[4,5]]
//  [[1,4],[3,6]]
//  [[1,2],[3,4],[5,6]]
#define SORT_NETWORK_8(TYPE, CMP, begin)                                                                               \
  do {                                                                                                                 \
    SORT_CMP_SWAP(TYPE, CMP, begin[0], begin[4]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[1], begin[5]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[2], begin[6]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[3], begin[7]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[0], begin[2]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[1], begin[3]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[4], begin[6]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[5], begin[7]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[2], begin[4]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[3], begin[5]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[0], begin[1]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[6], begin[7]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[2], begin[3]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[4], begin[5]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[1], begin[4]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[3], begin[6]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[1], begin[2]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[3], begin[4]);                                                                      \
    SORT_CMP_SWAP(TYPE, CMP, begin[5], begin[6]);                                                                      \
  } while (0)

#define SORT_INNER(TYPE, CMP, begin, end, len)                                                                         \
  switch (len) {                                                                                                       \
  default:                                                                                                             \
    assert(false);                                                                                                     \
    __unreachable();                                                                                                   \
  case 0:                                                                                                              \
  case 1:                                                                                                              \
    break;                                                                                                             \
  case 2:                                                                                                              \
    SORT_CMP_SWAP(TYPE, CMP, begin[0], begin[1]);                                                                      \
    break;                                                                                                             \
  case 3:                                                                                                              \
    SORT_NETWORK_3(TYPE, CMP, begin);                                                                                  \
    break;                                                                                                             \
  case 4:                                                                                                              \
    SORT_NETWORK_4(TYPE, CMP, begin);                                                                                  \
    break;                                                                                                             \
  case 5:                                                                                                              \
    SORT_NETWORK_5(TYPE, CMP, begin);                                                                                  \
    break;                                                                                                             \
  case 6:                                                                                                              \
    SORT_NETWORK_6(TYPE, CMP, begin);                                                                                  \
    break;                                                                                                             \
  case 7:                                                                                                              \
    SORT_NETWORK_7(TYPE, CMP, begin);                                                                                  \
    break;                                                                                                             \
  case 8:                                                                                                              \
    SORT_NETWORK_8(TYPE, CMP, begin);                                                                                  \
    break;                                                                                                             \
  }

#define SORT_SWAP(TYPE, a, b)                                                                                          \
  do {                                                                                                                 \
    const TYPE swap_tmp = (a);                                                                                         \
    (a) = (b);                                                                                                         \
    (b) = swap_tmp;                                                                                                    \
  } while (0)

#define SORT_PUSH(low, high)                                                                                           \
  do {                                                                                                                 \
    top->lo = (low);                                                                                                   \
    top->hi = (high);                                                                                                  \
    ++top;                                                                                                             \
  } while (0)

#define SORT_POP(low, high)                                                                                            \
  do {                                                                                                                 \
    --top;                                                                                                             \
    low = top->lo;                                                                                                     \
    high = top->hi;                                                                                                    \
  } while (0)

#define SORT_IMPL(NAME, EXPECT_LOW_CARDINALITY_OR_PRESORTED, TYPE, CMP)                                                \
                                                                                                                       \
  static inline bool NAME##_is_sorted(const TYPE *first, const TYPE *last) {                                           \
    while (++first <= last)                                                                                            \
      if (expect_with_probability(CMP(first[0], first[-1]), 1, .1))                                                    \
        return false;                                                                                                  \
    return true;                                                                                                       \
  }                                                                                                                    \
                                                                                                                       \
  typedef struct {                                                                                                     \
    TYPE *lo, *hi;                                                                                                     \
  } NAME##_stack;                                                                                                      \
                                                                                                                       \
  __hot static void NAME(TYPE *const __restrict begin, TYPE *const __restrict end) {                                   \
    NAME##_stack stack[sizeof(size_t) * CHAR_BIT], *__restrict top = stack;                                            \
                                                                                                                       \
    TYPE *__restrict hi = end - 1;                                                                                     \
    TYPE *__restrict lo = begin;                                                                                       \
    while (true) {                                                                                                     \
      const ptrdiff_t len = hi - lo;                                                                                   \
      if (len < 8) {                                                                                                   \
        SORT_INNER(TYPE, CMP, lo, hi + 1, len + 1);                                                                    \
        if (unlikely(top == stack))                                                                                    \
          break;                                                                                                       \
        SORT_POP(lo, hi);                                                                                              \
        continue;                                                                                                      \
      }                                                                                                                \
                                                                                                                       \
      TYPE *__restrict mid = lo + (len >> 1);                                                                          \
      SORT_CMP_SWAP(TYPE, CMP, *lo, *mid);                                                                             \
      SORT_CMP_SWAP(TYPE, CMP, *mid, *hi);                                                                             \
      SORT_CMP_SWAP(TYPE, CMP, *lo, *mid);                                                                             \
                                                                                                                       \
      TYPE *right = hi - 1;                                                                                            \
      TYPE *left = lo + 1;                                                                                             \
      while (1) {                                                                                                      \
        while (expect_with_probability(CMP(*left, *mid), 0, .5))                                                       \
          ++left;                                                                                                      \
        while (expect_with_probability(CMP(*mid, *right), 0, .5))                                                      \
          --right;                                                                                                     \
        if (unlikely(left > right)) {                                                                                  \
          if (EXPECT_LOW_CARDINALITY_OR_PRESORTED) {                                                                   \
            if (NAME##_is_sorted(lo, right))                                                                           \
              lo = right + 1;                                                                                          \
            if (NAME##_is_sorted(left, hi))                                                                            \
              hi = left;                                                                                               \
          }                                                                                                            \
          break;                                                                                                       \
        }                                                                                                              \
        SORT_SWAP(TYPE, *left, *right);                                                                                \
        mid = (mid == left) ? right : (mid == right) ? left : mid;                                                     \
        ++left;                                                                                                        \
        --right;                                                                                                       \
      }                                                                                                                \
                                                                                                                       \
      if (right - lo > hi - left) {                                                                                    \
        SORT_PUSH(lo, right);                                                                                          \
        lo = left;                                                                                                     \
      } else {                                                                                                         \
        SORT_PUSH(left, hi);                                                                                           \
        hi = right;                                                                                                    \
      }                                                                                                                \
    }                                                                                                                  \
                                                                                                                       \
    if (AUDIT_ENABLED()) {                                                                                             \
      for (TYPE *scan = begin + 1; scan < end; ++scan)                                                                 \
        assert(CMP(scan[-1], scan[0]));                                                                                \
    }                                                                                                                  \
  }

/*------------------------------------------------------------------------------
 * LY: radix sort for large chunks */

#define RADIXSORT_IMPL(NAME, TYPE, EXTRACT_KEY, BUFFER_PREALLOCATED, END_GAP)                                          \
                                                                                                                       \
  __hot static bool NAME##_radixsort(TYPE *const begin, const size_t length) {                                         \
    TYPE *tmp;                                                                                                         \
    if (BUFFER_PREALLOCATED) {                                                                                         \
      tmp = begin + length + END_GAP;                                                                                  \
      /* memset(tmp, 0xDeadBeef, sizeof(TYPE) * length); */                                                            \
    } else {                                                                                                           \
      tmp = osal_malloc(sizeof(TYPE) * length);                                                                        \
      if (unlikely(!tmp))                                                                                              \
        return false;                                                                                                  \
    }                                                                                                                  \
                                                                                                                       \
    size_t key_shift = 0, key_diff_mask;                                                                               \
    do {                                                                                                               \
      struct {                                                                                                         \
        pgno_t a[256], b[256];                                                                                         \
      } counters;                                                                                                      \
      memset(&counters, 0, sizeof(counters));                                                                          \
                                                                                                                       \
      key_diff_mask = 0;                                                                                               \
      size_t prev_key = EXTRACT_KEY(begin) >> key_shift;                                                               \
      TYPE *r = begin, *end = begin + length;                                                                          \
      do {                                                                                                             \
        const size_t key = EXTRACT_KEY(r) >> key_shift;                                                                \
        counters.a[key & 255]++;                                                                                       \
        counters.b[(key >> 8) & 255]++;                                                                                \
        key_diff_mask |= prev_key ^ key;                                                                               \
        prev_key = key;                                                                                                \
      } while (++r != end);                                                                                            \
                                                                                                                       \
      pgno_t ta = 0, tb = 0;                                                                                           \
      for (size_t i = 0; i < 256; ++i) {                                                                               \
        const pgno_t ia = counters.a[i];                                                                               \
        counters.a[i] = ta;                                                                                            \
        ta += ia;                                                                                                      \
        const pgno_t ib = counters.b[i];                                                                               \
        counters.b[i] = tb;                                                                                            \
        tb += ib;                                                                                                      \
      }                                                                                                                \
                                                                                                                       \
      r = begin;                                                                                                       \
      do {                                                                                                             \
        const size_t key = EXTRACT_KEY(r) >> key_shift;                                                                \
        tmp[counters.a[key & 255]++] = *r;                                                                             \
      } while (++r != end);                                                                                            \
                                                                                                                       \
      if (unlikely(key_diff_mask < 256)) {                                                                             \
        memcpy(begin, tmp, ptr_dist(end, begin));                                                                      \
        break;                                                                                                         \
      }                                                                                                                \
      end = (r = tmp) + length;                                                                                        \
      do {                                                                                                             \
        const size_t key = EXTRACT_KEY(r) >> key_shift;                                                                \
        begin[counters.b[(key >> 8) & 255]++] = *r;                                                                    \
      } while (++r != end);                                                                                            \
                                                                                                                       \
      key_shift += 16;                                                                                                 \
    } while (key_diff_mask >> 16);                                                                                     \
                                                                                                                       \
    if (!(BUFFER_PREALLOCATED))                                                                                        \
      osal_free(tmp);                                                                                                  \
    return true;                                                                                                       \
  }

/*------------------------------------------------------------------------------
 * LY: Binary search */

#if defined(__clang__) && __clang_major__ > 4 && defined(__ia32__)
#define WORKAROUND_FOR_CLANG_OPTIMIZER_BUG(size, flag)                                                                 \
  do                                                                                                                   \
    __asm __volatile(""                                                                                                \
                     : "+r"(size)                                                                                      \
                     : "r" /* the `b` constraint is more suitable here, but                                            \
                              cause CLANG to allocate and push/pop an one more                                         \
                              register, so using the `r` which avoids this. */                                         \
                     (flag));                                                                                          \
  while (0)
#else
#define WORKAROUND_FOR_CLANG_OPTIMIZER_BUG(size, flag)                                                                 \
  do {                                                                                                                 \
    /* nope for non-clang or non-x86 */;                                                                               \
  } while (0)
#endif /* Workaround for CLANG */

/* *INDENT-OFF* */
/* clang-format off */
#define SEARCH_IMPL(NAME, TYPE_LIST, TYPE_ARG, CMP)                            \
  static __always_inline const TYPE_LIST *NAME(                                \
      const TYPE_LIST *it, size_t length, const TYPE_ARG item) {               \
    const TYPE_LIST *const begin = it, *const end = begin + length;            \
                                                                               \
    if (MDBX_HAVE_CMOV)                                                        \
      do {                                                                     \
        /* Адаптивно-упрощенный шаг двоичного поиска:                          \
         *  - без переходов при наличии cmov или аналога;                      \
         *  - допускает лишние итерации;                                       \
         *  - но ищет пока size > 2, что требует дозавершения поиска           \
         *    среди остающихся 0-1-2 элементов. */                             \
        const TYPE_LIST *const middle = it + (length >> 1);                    \
        length = (length + 1) >> 1;                                            \
        const bool flag = expect_with_probability(CMP(*middle, item), 0, .5);  \
        WORKAROUND_FOR_CLANG_OPTIMIZER_BUG(length, flag);                      \
        it = flag ? middle : it;                                               \
      } while (length > 2);                                                    \
    else                                                                       \
      while (length > 2) {                                                     \
        /* Вариант с использованием условного перехода. Основное отличие в     \
         * том, что при "не равно" (true от компаратора) переход делается на 1 \
         * ближе к концу массива. Алгоритмически это верно и обеспечивает      \
         * чуть-чуть более быструю сходимость, но зато требует больше          \
         * вычислений при true от компаратора. Также ВАЖНО(!) не допускается   \
         * спекулятивное выполнение при size == 0. */                          \
        const TYPE_LIST *const middle = it + (length >> 1);                    \
        length = (length + 1) >> 1;                                            \
        const bool flag = expect_with_probability(CMP(*middle, item), 0, .5);  \
        if (flag) {                                                            \
          it = middle + 1;                                                     \
          length -= 1;                                                         \
        }                                                                      \
      }                                                                        \
    it += length > 1 && expect_with_probability(CMP(*it, item), 0, .5);        \
    it += length > 0 && expect_with_probability(CMP(*it, item), 0, .5);        \
                                                                               \
    if (AUDIT_ENABLED()) {                                                     \
      for (const TYPE_LIST *scan = begin; scan < it; ++scan)                   \
        assert(CMP(*scan, item));                                              \
      for (const TYPE_LIST *scan = it; scan < end; ++scan)                     \
        assert(!CMP(*scan, item));                                             \
      (void)begin, (void)end;                                                  \
    }                                                                          \
                                                                               \
    return it;                                                                 \
  }
/* *INDENT-ON* */
/* clang-format on */
