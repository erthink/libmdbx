/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2025

#pragma once

#include "essentials.h"

/* Сортированный набор txnid, использующий внутри комбинацию непрерывного интервала и списка.
 * Обеспечивает хранение id записей при переработке, очистку и обновлении GC, включая возврат остатков переработанных
 * страниц.
 *
 * При переработке GC записи преимущественно выбираются последовательно, но это не гарантируется. В LIFO-режиме
 * переработка и добавление записей в rkl происходит преимущественно в обратном порядке, но из-за завершения читающих
 * транзакций могут быть «скачки» в прямом направлении. В FIFO-режиме записи GC перерабатываются в прямом порядке и при
 * этом линейно, но не обязательно строго последовательно, при этом гарантируется что между добавляемыми в rkl
 * идентификаторами в GC нет записей, т.е. между первой (минимальный id) и последней (максимальный id) в GC нет записей
 * и весь интервал может быть использован для возврата остатков страниц в GC.
 *
 * Таким образом, комбинация линейного интервала и списка (отсортированного в порядке возрастания элементов) является
 * рациональным решением, близким к теоретически оптимальному пределу.
 *
 * Реализация rkl достаточно проста/прозрачная, если не считать неочевидную «магию» обмена непрерывного интервала и
 * образующихся в списке последовательностей. Однако, именно этот автоматически выполняемый без лишних операций обмен
 * оправдывает все накладные расходы. */
typedef struct MDBX_rkl {
  txnid_t solid_begin, solid_end; /* начало и конец непрерывной последовательности solid_begin ... solid_end-1. */
  unsigned list_length;           /* текущая длина списка. */
  unsigned list_limit;    /* размер буфера выделенного под список, равен ARRAY_LENGTH(inplace) когда list == inplace. */
  txnid_t *list;          /* список отдельных элементов в порядке возрастания (наименьший в начале). */
  txnid_t inplace[4 + 8]; /* статический массив для коротких списков, чтобы избавиться от выделения/освобождения памяти
                           * в большинстве случаев. */
} rkl_t;

MDBX_MAYBE_UNUSED MDBX_INTERNAL void rkl_init(rkl_t *rkl);
MDBX_MAYBE_UNUSED MDBX_INTERNAL void rkl_clear(rkl_t *rkl);
static inline void rkl_clear_and_shrink(rkl_t *rkl) { rkl_clear(rkl); /* TODO */ }
MDBX_MAYBE_UNUSED MDBX_INTERNAL void rkl_destroy(rkl_t *rkl);
MDBX_MAYBE_UNUSED MDBX_INTERNAL void rkl_destructive_move(rkl_t *dst, rkl_t *src);
MDBX_MAYBE_UNUSED MDBX_INTERNAL __must_check_result int rkl_copy(const rkl_t *src, rkl_t *dst);
MDBX_MAYBE_UNUSED MDBX_NOTHROW_PURE_FUNCTION static inline bool rkl_empty(const rkl_t *rkl) {
  return rkl->solid_begin > rkl->solid_end;
}
MDBX_MAYBE_UNUSED MDBX_NOTHROW_PURE_FUNCTION MDBX_INTERNAL bool rkl_check(const rkl_t *rkl);
MDBX_MAYBE_UNUSED MDBX_NOTHROW_PURE_FUNCTION MDBX_INTERNAL size_t rkl_len(const rkl_t *rkl);
MDBX_MAYBE_UNUSED MDBX_NOTHROW_PURE_FUNCTION MDBX_INTERNAL txnid_t rkl_lowest(const rkl_t *rkl);
MDBX_MAYBE_UNUSED MDBX_NOTHROW_PURE_FUNCTION MDBX_INTERNAL txnid_t rkl_highest(const rkl_t *rkl);
MDBX_MAYBE_UNUSED MDBX_NOTHROW_PURE_FUNCTION static inline txnid_t rkl_edge(const rkl_t *rkl,
                                                                            const bool highest_not_lowest) {
  return highest_not_lowest ? rkl_highest(rkl) : rkl_lowest(rkl);
}
MDBX_MAYBE_UNUSED MDBX_INTERNAL __must_check_result int rkl_push(rkl_t *rkl, const txnid_t id,
                                                                 const bool known_continuous);
MDBX_MAYBE_UNUSED MDBX_INTERNAL txnid_t rkl_pop(rkl_t *rkl, const bool highest_not_lowest);
MDBX_MAYBE_UNUSED MDBX_INTERNAL __must_check_result int rkl_merge(rkl_t *dst, const rkl_t *src, bool ignore_duplicates);

/* Итератор для rkl.
 * Обеспечивает изоляцию внутреннего устройства rkl от остального кода, чем существенно его упрощает.
 * Фактически именно использованием rkl с итераторами ликвидируется "ребус" исторически образовавшийся в gc-update. */
typedef struct MDBX_rkl_iter {
  const rkl_t *rkl;
  unsigned pos;
  unsigned solid_offset;
} rkl_iter_t;

MDBX_MAYBE_UNUSED MDBX_INTERNAL __must_check_result rkl_iter_t rkl_iterator(const rkl_t *rkl, const bool reverse);
MDBX_MAYBE_UNUSED MDBX_INTERNAL __must_check_result txnid_t rkl_turn(rkl_iter_t *iter, const bool reverse);
MDBX_MAYBE_UNUSED MDBX_NOTHROW_PURE_FUNCTION MDBX_INTERNAL size_t rkl_left(rkl_iter_t *iter, const bool reverse);
MDBX_MAYBE_UNUSED MDBX_INTERNAL bool rkl_find(const rkl_t *rkl, const txnid_t id, rkl_iter_t *iter);
MDBX_MAYBE_UNUSED MDBX_NOTHROW_PURE_FUNCTION __must_check_result MDBX_INTERNAL bool rkl_contain(const rkl_t *rkl,
                                                                                                txnid_t id);

typedef struct MDBX_rkl_hole {
  txnid_t begin;
  txnid_t end;
} rkl_hole_t;
MDBX_MAYBE_UNUSED MDBX_INTERNAL __must_check_result rkl_hole_t rkl_hole(rkl_iter_t *iter, const bool reverse);
