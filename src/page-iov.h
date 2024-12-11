/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024

#pragma once

#include "essentials.h"

#if !(defined(_WIN32) || defined(_WIN64))
#define MDBX_WRITETHROUGH_THRESHOLD_DEFAULT 2
#endif

struct iov_ctx {
  MDBX_env *env;
  osal_ioring_t *ior;
  mdbx_filehandle_t fd;
  int err;
#ifndef MDBX_NEED_WRITTEN_RANGE
#define MDBX_NEED_WRITTEN_RANGE 1
#endif /* MDBX_NEED_WRITTEN_RANGE */
#if MDBX_NEED_WRITTEN_RANGE
  pgno_t flush_begin;
  pgno_t flush_end;
#endif /* MDBX_NEED_WRITTEN_RANGE */
  uint64_t coherency_timestamp;
};

MDBX_INTERNAL __must_check_result int iov_init(MDBX_txn *const txn, iov_ctx_t *ctx, size_t items, size_t npages,
                                               mdbx_filehandle_t fd, bool check_coherence);

static inline bool iov_empty(const iov_ctx_t *ctx) { return osal_ioring_used(ctx->ior) == 0; }

MDBX_INTERNAL __must_check_result int iov_page(MDBX_txn *txn, iov_ctx_t *ctx, page_t *dp, size_t npages);

MDBX_INTERNAL __must_check_result int iov_write(iov_ctx_t *ctx);
