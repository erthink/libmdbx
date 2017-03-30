/*
 * Copyright 2017 Leonid Yuriev <leo@yuriev.ru>
 * and other libmdbx authors: please see AUTHORS file.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 */

#include "test.h"

namespace keygen {

size_t ffs_fallback(serial_t serial) {
  size_t bit = sizeof(serial_t) * 8 - 1;
  auto mask = (serial_t)1u << bit;
  do {
    if (serial & mask)
      return bit;
    --bit;
  } while (mask >>= 1);
  return 0;
}

void __hot make(const serial_t serial, const params_t &params, result_t &out) {
  assert(out.limit >= params.maxlen);
  assert(params.maxlen >= params.minlen);
  assert(params.maxlen >= length(serial));

  out.value.mv_data = out.bytes;
  out.value.mv_size = params.minlen;

  if (params.flags & (MDB_INTEGERKEY | MDB_INTEGERDUP)) {
    assert(params.maxlen == params.minlen);
    assert(params.minlen == 4 || params.minlen == 8);
    if (is_byteorder_le() || params.minlen == 8)
      out.u64 = serial;
    else
      out.u32 = (uint32_t)serial;
  } else if (params.flags & (MDB_REVERSEKEY | MDB_REVERSEDUP)) {
    if (out.value.mv_size > 8) {
      memset(out.bytes, '\0', out.value.mv_size - 8);
      unaligned::store(out.bytes + out.value.mv_size - 8, htobe64(serial));
    } else {
      out.u64 = htobe64(serial);
      if (out.value.mv_size < 8) {
        out.value.mv_size = std::max(length(serial), out.value.mv_size);
        out.value.mv_data = out.bytes + 8 - out.value.mv_size;
      }
    }
  } else {
    out.u64 = htole64(serial);
    if (out.value.mv_size > 8)
      memset(out.bytes + 8, '\0', out.value.mv_size - 8);
    else
      out.value.mv_size = std::max(length(serial), out.value.mv_size);
  }

  assert(out.value.mv_size >= params.minlen);
  assert(out.value.mv_size <= params.maxlen);
  assert(out.value.mv_size >= length(serial));
  assert(out.value.mv_data >= out.bytes);
  assert((uint8_t *)out.value.mv_data + out.value.mv_size <=
         out.bytes + out.limit);
}

} /* namespace keygen */
