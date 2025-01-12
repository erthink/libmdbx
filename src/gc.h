/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024

#pragma once

#include "essentials.h"

typedef struct gc_update_context {
  unsigned loop;
  pgno_t prev_first_unallocated;
  bool dense;
  size_t reserve_adj;
  size_t retired_stored;
  size_t amount, reserved, cleaned_slot, reused_slot, fill_idx;
  txnid_t cleaned_id, rid;
#if MDBX_ENABLE_BIGFOOT
  txnid_t bigfoot;
#endif /* MDBX_ENABLE_BIGFOOT */
  union {
    MDBX_cursor cursor;
    cursor_couple_t couple;
  };
} gcu_t;

static inline int gc_update_init(MDBX_txn *txn, gcu_t *ctx) {
  memset(ctx, 0, offsetof(gcu_t, cursor));
  ctx->dense = txn->txnid <= MIN_TXNID;
#if MDBX_ENABLE_BIGFOOT
  ctx->bigfoot = txn->txnid;
#endif /* MDBX_ENABLE_BIGFOOT */
  return cursor_init(&ctx->cursor, txn, FREE_DBI);
}

#define ALLOC_DEFAULT 0
#define ALLOC_RESERVE 1
#define ALLOC_UNIMPORTANT 2
MDBX_INTERNAL pgr_t gc_alloc_ex(const MDBX_cursor *const mc, const size_t num, uint8_t flags);

MDBX_INTERNAL pgr_t gc_alloc_single(const MDBX_cursor *const mc);
MDBX_INTERNAL int gc_update(MDBX_txn *txn, gcu_t *ctx);
