/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025

#pragma once

#include "essentials.h"

/* List of txnid */
typedef txnid_t *txl_t;
typedef const txnid_t *const_txl_t;

enum txl_rules {
  txl_granulate = 32,
  txl_initial = txl_granulate - 2 - MDBX_ASSUME_MALLOC_OVERHEAD / sizeof(txnid_t),
  txl_max = (1u << 26) - 2 - MDBX_ASSUME_MALLOC_OVERHEAD / sizeof(txnid_t)
};

MDBX_INTERNAL txl_t txl_alloc(void);

MDBX_INTERNAL void txl_free(txl_t txl);

MDBX_INTERNAL int __must_check_result txl_append(txl_t __restrict *ptxl, txnid_t id);

MDBX_INTERNAL void txl_sort(txl_t txl);

MDBX_INTERNAL bool txl_contain(const txl_t txl, txnid_t id);
