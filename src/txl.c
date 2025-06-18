/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025

#include "internals.h"

static inline size_t txl_size2bytes(const size_t size) {
  assert(size > 0 && size <= txl_max * 2);
  size_t bytes =
      ceil_powerof2(MDBX_ASSUME_MALLOC_OVERHEAD + sizeof(txnid_t) * (size + 2), txl_granulate * sizeof(txnid_t)) -
      MDBX_ASSUME_MALLOC_OVERHEAD;
  return bytes;
}

static inline size_t txl_bytes2size(const size_t bytes) {
  size_t size = bytes / sizeof(txnid_t);
  assert(size > 2 && size <= txl_max * 2);
  return size - 2;
}

txl_t txl_alloc(void) {
  size_t bytes = txl_size2bytes(txl_initial);
  txl_t txl = osal_malloc(bytes);
  if (likely(txl)) {
#ifdef osal_malloc_usable_size
    bytes = osal_malloc_usable_size(txl);
#endif /* osal_malloc_usable_size */
    txl[0] = txl_bytes2size(bytes);
    assert(txl[0] >= txl_initial);
    txl += 1;
    *txl = 0;
  }
  return txl;
}

void txl_free(txl_t txl) {
  if (likely(txl))
    osal_free(txl - 1);
}

static int txl_reserve(txl_t __restrict *__restrict ptxl, const size_t wanna) {
  const size_t allocated = txl_alloclen(*ptxl);
  assert(txl_size(*ptxl) <= txl_max && txl_alloclen(*ptxl) >= txl_size(*ptxl));
  if (likely(allocated >= wanna))
    return MDBX_SUCCESS;

  if (unlikely(wanna > /* paranoia */ txl_max)) {
    ERROR("TXL too long (%zu > %zu)", wanna, (size_t)txl_max);
    return MDBX_TXN_FULL;
  }

  const size_t size = (wanna + wanna - allocated < txl_max) ? wanna + wanna - allocated : txl_max;
  size_t bytes = txl_size2bytes(size);
  txl_t txl = osal_realloc(*ptxl - 1, bytes);
  if (likely(txl)) {
#ifdef osal_malloc_usable_size
    bytes = osal_malloc_usable_size(txl);
#endif /* osal_malloc_usable_size */
    *txl = txl_bytes2size(bytes);
    assert(*txl >= wanna);
    *ptxl = txl + 1;
    return MDBX_SUCCESS;
  }
  return MDBX_ENOMEM;
}

static inline int __must_check_result txl_need(txl_t __restrict *__restrict ptxl, size_t num) {
  assert(txl_size(*ptxl) <= txl_max && txl_alloclen(*ptxl) >= txl_size(*ptxl));
  assert(num <= PAGELIST_LIMIT);
  const size_t wanna = txl_size(*ptxl) + num;
  return likely(txl_alloclen(*ptxl) >= wanna) ? MDBX_SUCCESS : txl_reserve(ptxl, wanna);
}

static inline void txl_append_prereserved(txl_t __restrict txl, txnid_t id) {
  assert(txl_size(txl) < txl_alloclen(txl));
  size_t end = txl[0] += 1;
  txl[end] = id;
}

#define TXNID_SORT_CMP(first, last) ((first) > (last))
SORT_IMPL(txnid_sort, false, txnid_t, TXNID_SORT_CMP)
void txl_sort(txl_t txl) { txnid_sort(txl + 1, txl + txl_size(txl) + 1); }

int __must_check_result txl_append(txl_t __restrict *ptxl, txnid_t id) {
  if (unlikely(txl_size(*ptxl) == txl_alloclen(*ptxl))) {
    int rc = txl_need(ptxl, txl_granulate);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }
  txl_append_prereserved(*ptxl, id);
  return MDBX_SUCCESS;
}

__hot bool txl_contain(const txl_t txl, txnid_t id) {
  const size_t len = txl_size(txl);
  for (size_t i = 1; i <= len; ++i)
    if (txl[i] == id)
      return true;
  return false;
}
