/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024

#pragma once

#include "essentials.h"

/* valid flags for mdbx_node_add() */
#define NODE_ADD_FLAGS (N_DUP | N_TREE | MDBX_RESERVE | MDBX_APPEND)

/* Get the page number pointed to by a branch node */
MDBX_NOTHROW_PURE_FUNCTION static inline pgno_t
node_pgno(const node_t *const __restrict node) {
  pgno_t pgno = UNALIGNED_PEEK_32(node, node_t, child_pgno);
  return pgno;
}

/* Set the page number in a branch node */
static inline void node_set_pgno(node_t *const __restrict node, pgno_t pgno) {
  assert(pgno >= MIN_PAGENO && pgno <= MAX_PAGENO);

  UNALIGNED_POKE_32(node, node_t, child_pgno, (uint32_t)pgno);
}

/* Get the size of the data in a leaf node */
MDBX_NOTHROW_PURE_FUNCTION static inline size_t
node_ds(const node_t *const __restrict node) {
  return UNALIGNED_PEEK_32(node, node_t, dsize);
}

/* Set the size of the data for a leaf node */
static inline void node_set_ds(node_t *const __restrict node, size_t size) {
  assert(size < INT_MAX);
  UNALIGNED_POKE_32(node, node_t, dsize, (uint32_t)size);
}

/* The size of a key in a node */
MDBX_NOTHROW_PURE_FUNCTION static inline size_t
node_ks(const node_t *const __restrict node) {
  return UNALIGNED_PEEK_16(node, node_t, ksize);
}

/* Set the size of the key for a leaf node */
static inline void node_set_ks(node_t *const __restrict node, size_t size) {
  assert(size < INT16_MAX);
  UNALIGNED_POKE_16(node, node_t, ksize, (uint16_t)size);
}

MDBX_NOTHROW_PURE_FUNCTION static inline uint8_t
node_flags(const node_t *const __restrict node) {
  return UNALIGNED_PEEK_8(node, node_t, flags);
}

static inline void node_set_flags(node_t *const __restrict node,
                                  uint8_t flags) {
  UNALIGNED_POKE_8(node, node_t, flags, flags);
}

/* Address of the key for the node */
MDBX_NOTHROW_PURE_FUNCTION static inline void *
node_key(const node_t *const __restrict node) {
  return ptr_disp(node, NODESIZE);
}

/* Address of the data for a node */
MDBX_NOTHROW_PURE_FUNCTION static inline void *
node_data(const node_t *const __restrict node) {
  return ptr_disp(node_key(node), node_ks(node));
}

/* Size of a node in a leaf page with a given key and data.
 * This is node header plus key plus data size. */
MDBX_NOTHROW_CONST_FUNCTION static inline size_t
node_size_len(const size_t key_len, const size_t value_len) {
  return NODESIZE + EVEN_CEIL(key_len + value_len);
}
MDBX_NOTHROW_PURE_FUNCTION static inline size_t
node_size(const MDBX_val *key, const MDBX_val *value) {
  return node_size_len(key ? key->iov_len : 0, value ? value->iov_len : 0);
}

MDBX_NOTHROW_PURE_FUNCTION static inline pgno_t
node_largedata_pgno(const node_t *const __restrict node) {
  assert(node_flags(node) & N_BIG);
  return peek_pgno(node_data(node));
}

MDBX_INTERNAL int __must_check_result node_read_bigdata(MDBX_cursor *mc,
                                                        const node_t *node,
                                                        MDBX_val *data,
                                                        const page_t *mp);

static inline int __must_check_result node_read(MDBX_cursor *mc,
                                                const node_t *node,
                                                MDBX_val *data,
                                                const page_t *mp) {
  data->iov_len = node_ds(node);
  data->iov_base = node_data(node);
  if (likely(node_flags(node) != N_BIG))
    return MDBX_SUCCESS;
  return node_read_bigdata(mc, node, data, mp);
}

/*----------------------------------------------------------------------------*/

MDBX_INTERNAL nsr_t node_search(MDBX_cursor *mc, const MDBX_val *key);

MDBX_INTERNAL int __must_check_result node_add_branch(MDBX_cursor *mc,
                                                      size_t indx,
                                                      const MDBX_val *key,
                                                      pgno_t pgno);

MDBX_INTERNAL int __must_check_result node_add_leaf(MDBX_cursor *mc,
                                                    size_t indx,
                                                    const MDBX_val *key,
                                                    MDBX_val *data,
                                                    unsigned flags);

MDBX_INTERNAL int __must_check_result node_add_dupfix(MDBX_cursor *mc,
                                                      size_t indx,
                                                      const MDBX_val *key);

MDBX_INTERNAL void node_del(MDBX_cursor *mc, size_t ksize);

MDBX_INTERNAL node_t *node_shrink(page_t *mp, size_t indx, node_t *node);
