/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2025

#include "internals.h"

static inline size_t rkl_size2bytes(const size_t size) {
  assert(size > 0 && size <= txl_max * 2);
  size_t bytes = ceil_powerof2(MDBX_ASSUME_MALLOC_OVERHEAD + sizeof(txnid_t) * size, txl_granulate * sizeof(txnid_t)) -
                 MDBX_ASSUME_MALLOC_OVERHEAD;
  return bytes;
}

static inline size_t rkl_bytes2size(const size_t bytes) {
  size_t size = bytes / sizeof(txnid_t);
  assert(size > 0 && size <= txl_max * 2);
  return size;
}

void rkl_init(rkl_t *rkl) {
  rkl->list_limit = ARRAY_LENGTH(rkl->inplace);
  rkl->list = rkl->inplace;
  rkl_clear(rkl);
}

void rkl_clear(rkl_t *rkl) {
  rkl->solid_begin = UINT64_MAX;
  rkl->solid_end = 0;
  rkl->list_length = 0;
}

void rkl_destroy(rkl_t *rkl) {
  void *ptr = rkl->list;
  rkl->list = nullptr;
  if (ptr != rkl->inplace)
    osal_free(ptr);
}

static inline bool solid_empty(const rkl_t *rkl) { return !(rkl->solid_begin < rkl->solid_end); }

#define RKL_ORDERED(first, last) ((first) < (last))

SEARCH_IMPL(rkl_bsearch, txnid_t, txnid_t, RKL_ORDERED)

void rkl_destructive_move(rkl_t *src, rkl_t *dst) {
  assert(rkl_check(src));
  dst->solid_begin = src->solid_begin;
  dst->solid_end = src->solid_end;
  dst->list_length = src->list_length;
  if (dst->list != dst->inplace)
    osal_free(dst->list);
  if (src->list != src->inplace) {
    dst->list = src->list;
    dst->list_limit = src->list_limit;
  } else {
    dst->list = dst->inplace;
    dst->list_limit = ARRAY_LENGTH(src->inplace);
    memcpy(dst->inplace, src->list, sizeof(dst->inplace));
  }
  rkl_init(src);
}

static int rkl_resize(rkl_t *rkl, size_t wanna_size) {
  assert(wanna_size > rkl->list_length);
  assert(rkl_check(rkl));
  STATIC_ASSERT(txl_max < INT_MAX / sizeof(txnid_t));
  if (unlikely(wanna_size > txl_max)) {
    ERROR("rkl too long (%zu >= %zu)", wanna_size, (size_t)txl_max);
    return MDBX_TXN_FULL;
  }
  if (unlikely(wanna_size < rkl->list_length)) {
    ERROR("unable shrink rkl to %zu since length is %u", wanna_size, rkl->list_length);
    return MDBX_PROBLEM;
  }

  if (unlikely(wanna_size <= ARRAY_LENGTH(rkl->inplace))) {
    if (rkl->list != rkl->inplace) {
      assert(rkl->list_limit > ARRAY_LENGTH(rkl->inplace) && rkl->list_length <= ARRAY_LENGTH(rkl->inplace));
      memcpy(rkl->inplace, rkl->list, sizeof(rkl->inplace));
      rkl->list_limit = ARRAY_LENGTH(rkl->inplace);
      osal_free(rkl->list);
      rkl->list = rkl->inplace;
    } else {
      assert(rkl->list_limit == ARRAY_LENGTH(rkl->inplace));
    }
    return MDBX_SUCCESS;
  }

  if (wanna_size != rkl->list_limit) {
    size_t bytes = rkl_size2bytes(wanna_size);
    void *ptr = (rkl->list == rkl->inplace) ? osal_malloc(bytes) : osal_realloc(rkl->list, bytes);
    if (unlikely(!ptr))
      return MDBX_ENOMEM;
#ifdef osal_malloc_usable_size
    bytes = osal_malloc_usable_size(ptr);
#endif /* osal_malloc_usable_size */
    rkl->list_limit = rkl_bytes2size(bytes);
    if (rkl->list == rkl->inplace)
      memcpy(ptr, rkl->inplace, sizeof(rkl->inplace));
    rkl->list = ptr;
  }
  return MDBX_SUCCESS;
}

int rkl_copy(const rkl_t *src, rkl_t *dst) {
  assert(rkl_check(src));
  rkl_init(dst);
  if (!rkl_empty(src)) {
    if (dst->list_limit < src->list_length) {
      int err = rkl_resize(dst, src->list_limit);
      if (unlikely(err != MDBX_SUCCESS))
        return err;
    }
    memcpy(dst->list, src->list, sizeof(txnid_t) * src->list_length);
    dst->list_length = src->list_length;
    dst->solid_begin = src->solid_begin;
    dst->solid_end = src->solid_end;
  }
  return MDBX_SUCCESS;
}

size_t rkl_len(const rkl_t *rkl) { return rkl_empty(rkl) ? 0 : rkl->solid_end - rkl->solid_begin + rkl->list_length; }

__hot bool rkl_contain(const rkl_t *rkl, txnid_t id) {
  assert(rkl_check(rkl));
  if (id >= rkl->solid_begin && id < rkl->solid_end)
    return true;
  if (rkl->list_length) {
    const txnid_t *it = rkl_bsearch(rkl->list, rkl->list_length, id);
    const txnid_t *const end = rkl->list + rkl->list_length;
    assert(it >= rkl->list && it <= end);
    if (it != rkl->list)
      assert(RKL_ORDERED(it[-1], id));
    if (it != end) {
      assert(!RKL_ORDERED(it[0], id));
      return *it == id;
    }
  }
  return false;
}

__hot bool rkl_find(const rkl_t *rkl, const txnid_t id, rkl_iter_t *iter) {
  assert(rkl_check(rkl));
  *iter = rkl_iterator(rkl, false);
  if (id >= rkl->solid_begin) {
    if (id < rkl->solid_end) {
      iter->pos = iter->solid_offset + (unsigned)(id - rkl->solid_begin);
      return true;
    }
    iter->pos = (unsigned)(rkl->solid_end - rkl->solid_begin);
  }
  if (rkl->list_length) {
    const txnid_t *it = rkl_bsearch(rkl->list, rkl->list_length, id);
    const txnid_t *const end = rkl->list + rkl->list_length;
    assert(it >= rkl->list && it <= end);
    if (it != rkl->list)
      assert(RKL_ORDERED(it[-1], id));
    iter->pos += (unsigned)(it - rkl->list);
    if (it != end) {
      assert(!RKL_ORDERED(it[0], id));
      return *it == id;
    }
  }
  return false;
}

static inline txnid_t list_remove_first(rkl_t *rkl) {
  assert(rkl->list_length > 0);
  const txnid_t first = rkl->list[0];
  if (--rkl->list_length) {
    /* TODO: Можно подумать о том, чтобы для избавления от memove() добавить headroom или вместо длины и
     * указателя на список использовать три поля: list_begin, list_end и list_buffer. */
    size_t i = 0;
    do
      rkl->list[i] = rkl->list[i + 1];
    while (++i <= rkl->list_length);
  }
  return first;
}

static inline txnid_t after_cut(rkl_t *rkl, const txnid_t out) {
  if (rkl->list_length == 0 && rkl->solid_begin == rkl->solid_end) {
    rkl->solid_end = 0;
    rkl->solid_begin = UINT64_MAX;
  }
  return out;
}

static int extend_solid(rkl_t *rkl, txnid_t solid_begin, txnid_t solid_end, const txnid_t id) {
  if (rkl->list_length) {
    const txnid_t *i = rkl_bsearch(rkl->list, rkl->list_length, id);
    const txnid_t *const end = rkl->list + rkl->list_length;
    /* если начало или конец списка примыкает к непрерывному интервалу,
     * то переносим эти элементы из списка в непрерывный интервал */
    txnid_t *f = (txnid_t *)i;
    while (f > rkl->list && f[-1] >= solid_begin - 1) {
      f -= 1;
      solid_begin -= 1;
      if (unlikely(*f != solid_begin))
        return MDBX_RESULT_TRUE;
    }
    txnid_t *t = (txnid_t *)i;
    while (t < end && *t <= solid_end) {
      if (unlikely(*t != solid_end))
        return MDBX_RESULT_TRUE;
      solid_end += 1;
      t += 1;
    }
    if (f < t) {
      rkl->list_length -= t - f;
      while (t < end)
        *f++ = *t++;
    }
  }

  rkl->solid_begin = solid_begin;
  rkl->solid_end = solid_end;
  assert(rkl_check(rkl));
  return MDBX_SUCCESS;
}

int rkl_push(rkl_t *rkl, const txnid_t id) {
  assert(id >= MIN_TXNID && id < INVALID_TXNID);
  assert(rkl_check(rkl));
  const bool known_continuous = false;

  if (rkl->solid_begin >= rkl->solid_end) {
    /* непрерывный интервал пуст */
    return extend_solid(rkl, id, id + 1, id);
  } else if (id < rkl->solid_begin) {
    if (known_continuous || id + 1 == rkl->solid_begin)
      /* id примыкает к solid_begin */
      return extend_solid(rkl, id, rkl->solid_end, id);
  } else if (id >= rkl->solid_end) {
    if (known_continuous || id == rkl->solid_end)
      /* id примыкает к solid_end */
      return extend_solid(rkl, rkl->solid_begin, id + 1, id);
  } else {
    /* id входит в интервал между solid_begin и solid_end, т.е. подан дубликат */
    return MDBX_RESULT_TRUE;
  }

  if (rkl->list_length == 1 && rkl->solid_end == rkl->solid_begin + 1 &&
      (rkl->list[0] == id + 1 || rkl->list[0] == id - 1)) {
    /* В списке один элемент и добавляемый id примыкает к нему, при этом в непрерывном интервале тоже один элемент.
     * Лучше поменять элементы списка и непрерывного интервала. */
    const txnid_t couple = (rkl->list[0] == id - 1) ? id - 1 : id;
    rkl->list[0] = rkl->solid_begin;
    rkl->solid_begin = couple;
    rkl->solid_end = couple + 2;
    assert(rkl_check(rkl));
    return MDBX_SUCCESS;
  }

  if (unlikely(rkl->list_length == rkl->list_limit)) {
    /* удваиваем размер буфера если закончилось место */
    size_t x2 = (rkl->list_limit + 1) << 1;
    x2 = (x2 > 62) ? x2 : 62;
    x2 = (x2 < txl_max) ? x2 : txl_max;
    x2 = (x2 > rkl->list_length) ? x2 : rkl->list_length + 42;
    int err = rkl_resize(rkl, x2);
    if (unlikely(err != MDBX_SUCCESS))
      return err;
    assert(rkl->list_limit > rkl->list_length);
  }

  size_t i = rkl->list_length;
  /* ищем место для вставки двигаясь от конца к началу списка, сразу переставляя/раздвигая элементы */
  while (i > 0) {
    if (RKL_ORDERED(id, rkl->list[i - 1])) {
      rkl->list[i] = rkl->list[i - 1];
      i -= 1;
      continue;
    }
    if (unlikely(id == rkl->list[i - 1])) {
      while (++i < rkl->list_length)
        rkl->list[i - 1] = rkl->list[i];
      return MDBX_RESULT_TRUE;
    }
    break;
  }

  rkl->list[i] = id;
  rkl->list_length++;
  assert(rkl_check(rkl));

  /* После добавления id в списке могла образоваться длинная последовательность,
   * которую (возможно) стоит обменять с непрерывным интервалом. */
  if (rkl->list_length > (MDBX_DEBUG ? 2 : 16) &&
      ((i > 0 && rkl->list[i - 1] == id - 1) || (i + 1 < rkl->list_length && rkl->list[i + 1] == id + 1))) {
    txnid_t new_solid_begin = id;
    size_t from = i;
    while (from > 0 && rkl->list[from - 1] == new_solid_begin - 1) {
      from -= 1;
      new_solid_begin -= 1;
    }
    txnid_t new_solid_end = id + 1;
    size_t to = i + 1;
    while (to < rkl->list_length && rkl->list[to] == new_solid_end) {
      to += 1;
      new_solid_end += 1;
    }

    const size_t new_solid_len = to - from;
    if (new_solid_len > 3) {
      const size_t old_solid_len = rkl->solid_end - rkl->solid_begin;
      if (new_solid_len > old_solid_len) {
        /* Новая непрерывная последовательность длиннее текущей.
         * Считаем обмен выгодным, если он дешевле пути развития событий с добавлением следующего элемента в список. */
        const size_t old_solid_pos = rkl_bsearch(rkl->list, rkl->list_length, rkl->solid_begin) - rkl->list;
        const size_t swap_cost =
            /* количество элементов списка после изымаемой из списка последовательности,
             * которые нужно переместить */
            rkl->list_length - to +
            /* количество элементов списка после позиции добавляемой в список последовательности,
             * которые нужно переместить */
            ((from > old_solid_pos) ? from - old_solid_pos : 0)
            /* количество элементов списка добавляемой последовательности, которые нужно добавить */
            + old_solid_len;
        /* количество элементов списка, которые нужно переместить для вставки еще-одного/следующего элемента */
        const size_t new_insert_cost = rkl->list_length - i;
        /* coverity[logical_vs_bitwise] */
        if (unlikely(swap_cost < new_insert_cost) || MDBX_DEBUG) {
          /* Изымаемая последовательность длиннее добавляемой, поэтому:
           *  - список станет короче;
           *  - перемещать хвост нужно всегда к началу;
           *  - если начальные элементы потребуется раздвигать,
           *    то места хватит и остающиеся элементы в конце не будут перезаписаны. */
          size_t moved = 0;
          if (from > old_solid_pos) {
            /* добавляемая последовательность ближе к началу, нужно раздвинуть элементы в голове для вставки. */
            moved = from - old_solid_pos;
            do {
              from -= 1;
              rkl->list[from + old_solid_len] = rkl->list[from];
            } while (from > old_solid_pos);
          } else if (from + new_solid_len < old_solid_pos) {
            /* добавляемая последовательность дальше от начала,
             * перемещаем часть элементов из хвоста после изымаемой последовательности */
            do
              rkl->list[from++] = rkl->list[to++];
            while (from < old_solid_pos - new_solid_len);
          }

          /* вставляем последовательноть */
          i = 0;
          do
            rkl->list[from++] = rkl->solid_begin + i++;
          while (i != old_solid_len);

          /* сдвигаем оставшийся хвост */
          while (to < rkl->list_length)
            rkl->list[moved + from++] = rkl->list[to++];

          rkl->list_length = rkl->list_length - new_solid_len + old_solid_len;
          rkl->solid_begin = new_solid_begin;
          rkl->solid_end = new_solid_end;
          assert(rkl_check(rkl));
        }
      }
    }
  }
  return MDBX_SUCCESS;
}

txnid_t rkl_pop(rkl_t *rkl, const bool highest_not_lowest) {
  assert(rkl_check(rkl));

  if (rkl->list_length) {
    assert(rkl->solid_begin <= rkl->solid_end);
    if (highest_not_lowest && (solid_empty(rkl) || rkl->solid_end < rkl->list[rkl->list_length - 1]))
      return after_cut(rkl, rkl->list[rkl->list_length -= 1]);
    if (!highest_not_lowest && (solid_empty(rkl) || rkl->solid_begin > rkl->list[0]))
      return after_cut(rkl, list_remove_first(rkl));
  }

  if (!solid_empty(rkl))
    return after_cut(rkl, highest_not_lowest ? --rkl->solid_end : rkl->solid_begin++);

  assert(rkl_empty(rkl));
  return 0;
}

txnid_t rkl_lowest(const rkl_t *rkl) {
  if (rkl->list_length)
    return (solid_empty(rkl) || rkl->list[0] < rkl->solid_begin) ? rkl->list[0] : rkl->solid_begin;
  return !solid_empty(rkl) ? rkl->solid_begin : INVALID_TXNID;
}

txnid_t rkl_highest(const rkl_t *rkl) {
  if (rkl->list_length)
    return (solid_empty(rkl) || rkl->list[rkl->list_length - 1] >= rkl->solid_end) ? rkl->list[rkl->list_length - 1]
                                                                                   : rkl->solid_end - 1;
  return !solid_empty(rkl) ? rkl->solid_end - 1 : 0;
}

int rkl_merge(const rkl_t *src, rkl_t *dst, bool ignore_duplicates) {
  if (src->list_length) {
    size_t i = src->list_length;
    do {
      int err = rkl_push(dst, src->list[i - 1]);
      if (unlikely(err != MDBX_SUCCESS) && (!ignore_duplicates || err != MDBX_RESULT_TRUE))
        return err;
    } while (--i);
  }

  txnid_t id = src->solid_begin;
  while (id < src->solid_end) {
    int err = rkl_push(dst, id);
    if (unlikely(err != MDBX_SUCCESS) && (!ignore_duplicates || err != MDBX_RESULT_TRUE))
      return err;
    ++id;
  }
  return MDBX_SUCCESS;
}

int rkl_destructive_merge(rkl_t *src, rkl_t *dst, bool ignore_duplicates) {
  int err = rkl_merge(src, dst, ignore_duplicates);
  rkl_destroy(src);
  return err;
}

rkl_iter_t rkl_iterator(const rkl_t *rkl, const bool reverse) {
  rkl_iter_t iter = {.rkl = rkl, .pos = reverse ? rkl_len(rkl) : 0, .solid_offset = 0};
  if (!solid_empty(rkl) && rkl->list_length) {
    const txnid_t *it = rkl_bsearch(rkl->list, rkl->list_length, rkl->solid_begin);
    const txnid_t *const end = rkl->list + rkl->list_length;
    assert(it >= rkl->list && it <= end && (it == end || *it > rkl->solid_begin));
    iter.solid_offset = it - rkl->list;
  }
  return iter;
}

txnid_t rkl_turn(rkl_iter_t *iter, const bool reverse) {
  assert((unsigned)reverse == (unsigned)!!reverse);
  size_t pos = iter->pos - reverse;
  if (unlikely(pos >= rkl_len(iter->rkl)))
    return 0;

  iter->pos = pos + !reverse;
  assert(iter->pos <= rkl_len(iter->rkl));

  const size_t solid_len = iter->rkl->solid_end - iter->rkl->solid_begin;
  if (iter->rkl->list_length) {
    if (pos < iter->solid_offset)
      return iter->rkl->list[pos];
    else if (pos < iter->solid_offset + solid_len)
      return iter->rkl->solid_begin + pos - iter->solid_offset;
    else
      return iter->rkl->list[pos - solid_len];
  }

  assert(pos < solid_len);
  return iter->rkl->solid_begin + pos;
}

size_t rkl_left(rkl_iter_t *iter, const bool reverse) {
  assert(iter->pos <= rkl_len(iter->rkl));
  return reverse ? iter->pos : rkl_len(iter->rkl) - iter->pos;
}

#if 1
#define DEBUG_HOLE(hole)                                                                                               \
  do {                                                                                                                 \
  } while (0)
#else
#define DEBUG_HOLE(hole)                                                                                               \
  do {                                                                                                                 \
    printf("  return-%sward: %d, ", reverse ? "back" : "for", __LINE__);                                               \
    if (hole.begin == hole.end)                                                                                        \
      printf("empty-hole\n");                                                                                          \
    else if (hole.end - hole.begin == 1)                                                                               \
      printf("hole %" PRIaTXN "\n", hole.begin);                                                                       \
    else                                                                                                               \
      printf("hole %" PRIaTXN "-%" PRIaTXN "\n", hole.begin, hole.end - 1);                                            \
    fflush(nullptr);                                                                                                   \
  } while (0)
#endif

rkl_hole_t rkl_hole(rkl_iter_t *iter, const bool reverse) {
  assert((unsigned)reverse == (unsigned)!!reverse);
  rkl_hole_t hole;
  const size_t len = rkl_len(iter->rkl);
  size_t pos = iter->pos;
  if (unlikely(pos >= len)) {
    if (len == 0) {
      hole.begin = 1;
      hole.end = MAX_TXNID;
      iter->pos = 0;
      DEBUG_HOLE(hole);
      return hole;
    } else if (pos == len && reverse) {
      /* шаг назад из позиции на конце rkl */
    } else if (reverse) {
      hole.begin = 1;
      hole.end = 1 /* rkl_lowest(iter->rkl); */;
      iter->pos = 0;
      DEBUG_HOLE(hole);
      return hole;
    } else {
      hole.begin = MAX_TXNID /* rkl_highest(iter->rkl) + 1 */;
      hole.end = MAX_TXNID;
      iter->pos = len;
      DEBUG_HOLE(hole);
      return hole;
    }
  }

  const size_t solid_len = iter->rkl->solid_end - iter->rkl->solid_begin;
  if (iter->rkl->list_length) {
    /* список элементов не пуст */
    txnid_t here, there;
    for (size_t next;; pos = next) {
      next = reverse ? pos - 1 : pos + 1;
      if (pos < iter->solid_offset) {
        /* текущая позиция перед непрерывным интервалом */
        here = iter->rkl->list[pos];
        if (next == iter->solid_offset) {
          /* в следующей позиции начинается непрерывный интерал (при поиске вперед) */
          assert(!reverse);
          hole.begin = here + 1;
          hole.end = iter->rkl->solid_begin;
          next += solid_len;
          assert(hole.begin < hole.end /* зазор обязан быть, иначе это ошибка не-слияния */);
          /* зазор между элементом списка перед сплошным интервалом и началом интервала */
          iter->pos = next - 1;
          DEBUG_HOLE(hole);
          return hole;
        }
        if (next >= len)
          /* уперлись в конец или начало rkl */
          break;
        /* следующая позиция также перед непрерывным интервалом */
        there = iter->rkl->list[next];
      } else if (pos >= iter->solid_offset + solid_len) {
        /* текущая позиция после непрерывного интервала */
        here = (pos < len) ? iter->rkl->list[pos - solid_len] : MAX_TXNID;
        if (next >= len)
          /* уперлись в конец или начало rkl */
          break;
        if (next == iter->solid_offset + solid_len - 1) {
          /* в следующей позиции конец непрерывного интервала (при поиске назад) */
          assert(reverse);
          hole.begin = iter->rkl->solid_end;
          hole.end = here;
          pos = iter->solid_offset;
          assert(hole.begin < hole.end /* зазор обязан быть, иначе это ошибка не-слияния */);
          /* зазор между элементом списка после сплошного интервала и концом интервала */
          iter->pos = pos;
          DEBUG_HOLE(hole);
          return hole;
        }
        /* следующая позиция также после непрерывного интервала */
        there = iter->rkl->list[next - solid_len];
      } else if (reverse) {
        /* текущая позиция внутри непрерывного интервала и поиск назад */
        next = iter->solid_offset - 1;
        here = iter->rkl->solid_begin;
        if (next >= len)
          /* нет элементов списка перед непрерывным интервалом */
          break;
        /* предыдущая позиция перед непрерывным интервалом */
        there = iter->rkl->list[next];
      } else {
        /* текущая позиция внутри непрерывного интервала и поиск вперед */
        next = iter->solid_offset + solid_len;
        here = iter->rkl->solid_end - 1;
        if (next >= len)
          /* нет элементов списка после непрерывного интервала */
          break;
        /* следующая позиция после непрерывного интервала */
        there = iter->rkl->list[next - solid_len];
      }

      hole.begin = (reverse ? there : here) + 1;
      hole.end = reverse ? here : there;
      if (hole.begin < hole.end) {
        /* есть зазор между текущей и следующей позицией */
        iter->pos = next;
        DEBUG_HOLE(hole);
        return hole;
      }
    }

    if (reverse) {
      /* уперлись в начало rkl, возвращаем зазор перед началом rkl */
      hole.begin = 1;
      hole.end = here;
      iter->pos = 0;
      DEBUG_HOLE(hole);
    } else {
      /* уперлись в конец rkl, возвращаем зазор после конца rkl */
      hole.begin = here + 1;
      hole.end = MAX_TXNID;
      iter->pos = len;
      DEBUG_HOLE(hole);
    }
    return hole;
  }

  /* список элементов пуст, но есть непрерывный интервал */
  iter->pos = reverse ? 0 : len;
  if (reverse && pos < len) {
    /* возвращаем зазор перед непрерывным интервалом */
    hole.begin = 1;
    hole.end = iter->rkl->solid_begin;
    DEBUG_HOLE(hole);
  } else {
    /* возвращаем зазор после непрерывного интервала */
    hole.begin = iter->rkl->solid_end;
    hole.end = MAX_TXNID;
    DEBUG_HOLE(hole);
  }
  return hole;
}

bool rkl_check(const rkl_t *rkl) {
  if (!rkl)
    return false;
  if (rkl->list == rkl->inplace && unlikely(rkl->list_limit != ARRAY_LENGTH(rkl->inplace)))
    return false;
  if (unlikely(rkl->list_limit < ARRAY_LENGTH(rkl->inplace)))
    return false;

  if (rkl_empty(rkl))
    return rkl->list_length == 0 && solid_empty(rkl);

  if (rkl->list_length) {
    for (size_t i = 1; i < rkl->list_length; ++i)
      if (unlikely(!RKL_ORDERED(rkl->list[i - 1], rkl->list[i])))
        return false;
    if (!solid_empty(rkl) && rkl->solid_begin - 1 <= rkl->list[rkl->list_length - 1] &&
        rkl->solid_end >= rkl->list[0]) {
      /* непрерывный интервал "плавает" внутри списка, т.е. находится между какими-то соседними значениями */
      const txnid_t *it = rkl_bsearch(rkl->list, rkl->list_length, rkl->solid_begin);
      const txnid_t *const end = rkl->list + rkl->list_length;
      if (it < rkl->list || it > end)
        return false;
      if (it > rkl->list && it[-1] >= rkl->solid_begin)
        return false;
      if (it < end && it[0] <= rkl->solid_end)
        return false;
    }
  }

  return true;
}
