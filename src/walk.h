/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025

#pragma once

#include "essentials.h"

typedef struct walk_tbl {
  MDBX_val name;
  tree_t *internal, *nested;
} walk_tbl_t;

typedef int walk_func(const size_t pgno, const unsigned number, void *const ctx, const int deep,
                      const walk_tbl_t *table, const size_t page_size, const page_type_t page_type,
                      const MDBX_error_t err, const size_t nentries, const size_t payload_bytes,
                      const size_t header_bytes, const size_t unused_bytes);

typedef enum walk_options { dont_check_keys_ordering = 1 } walk_options_t;

MDBX_INTERNAL int walk_pages(MDBX_txn *txn, walk_func *visitor, void *user, walk_options_t options);
