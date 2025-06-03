/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025

#pragma once

#include "essentials.h"

/* Гистограмма решения нарезки фрагментов для ситуации нехватки идентификаторов/слотов. */
typedef struct gc_dense_histogram {
  /* Размер массива одновременно задаёт максимальный размер последовательностей,
   * с которыми решается задача распределения.
   *
   * Использование длинных последовательностей контрпродуктивно, так как такие последовательности будут
   * создавать/воспроизводить/повторять аналогичные затруднения при последующей переработке. Однако,
   * в редких ситуациях это может быть единственным выходом. */
  unsigned end;
  pgno_t array[31];
} gc_dense_histogram_t;

typedef struct gc_update_context {
  unsigned loop;
  unsigned goodchunk;
  bool dense;
  pgno_t prev_first_unallocated;
  size_t retired_stored;
  size_t return_reserved_lo, return_reserved_hi;
  txnid_t gc_first;
  intptr_t return_left;
#ifndef MDBX_DEBUG_GCU
#define MDBX_DEBUG_GCU 0
#endif
#if MDBX_DEBUG_GCU
  struct {
    txnid_t prev;
    unsigned n;
  } dbg;
#endif /* MDBX_DEBUG_GCU */
  rkl_t sequel;
#if MDBX_ENABLE_BIGFOOT
  txnid_t bigfoot;
#endif /* MDBX_ENABLE_BIGFOOT */
  union {
    MDBX_cursor cursor;
    cursor_couple_t couple;
  };
  gc_dense_histogram_t dense_histogram;
} gcu_t;

MDBX_INTERNAL int gc_put_init(MDBX_txn *txn, gcu_t *ctx);
MDBX_INTERNAL void gc_put_destroy(gcu_t *ctx);

#define ALLOC_DEFAULT 0     /* штатное/обычное выделение страниц */
#define ALLOC_UNIMPORTANT 1 /* запрос неважен, невозможность выделения не приведет к ошибке транзакции */
#define ALLOC_RESERVE 2     /* подготовка резерва для обновления GC, без аллокации */
#define ALLOC_COALESCE 4    /* внутреннее состояние/флажок */
#define ALLOC_SHOULD_SCAN 8 /* внутреннее состояние/флажок */
#define ALLOC_LIFO 16       /* внутреннее состояние/флажок */

MDBX_INTERNAL pgr_t gc_alloc_ex(const MDBX_cursor *const mc, const size_t num, uint8_t flags);

MDBX_INTERNAL pgr_t gc_alloc_single(const MDBX_cursor *const mc);
MDBX_INTERNAL int gc_update(MDBX_txn *txn, gcu_t *ctx);

MDBX_NOTHROW_PURE_FUNCTION static inline size_t gc_stockpile(const MDBX_txn *txn) {
  return MDBX_PNL_GETSIZE(txn->wr.repnl) + txn->wr.loose_count;
}

MDBX_NOTHROW_PURE_FUNCTION static inline size_t gc_chunk_bytes(const size_t chunk) {
  return (chunk + 1) * sizeof(pgno_t);
}

MDBX_INTERNAL bool gc_repnl_has_span(const MDBX_txn *txn, const size_t num);

static inline bool gc_is_reclaimed(const MDBX_txn *txn, const txnid_t id) {
  return rkl_contain(&txn->wr.gc.reclaimed, id) || rkl_contain(&txn->wr.gc.comeback, id);
}

static inline txnid_t txnid_min(txnid_t a, txnid_t b) { return (a < b) ? a : b; }

static inline txnid_t txnid_max(txnid_t a, txnid_t b) { return (a > b) ? a : b; }

static inline MDBX_cursor *gc_cursor(MDBX_env *env) { return ptr_disp(env->basal_txn, sizeof(MDBX_txn)); }
