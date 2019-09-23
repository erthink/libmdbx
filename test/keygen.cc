/*
 * Copyright 2017-2019 Leonid Yuriev <leo@yuriev.ru>
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

static inline __pure_function serial_t mask(unsigned bits) {
  assert(bits > 0 && bits <= serial_maxwith);
  return serial_allones >> (serial_maxwith - bits);
}

/* LY: https://en.wikipedia.org/wiki/Injective_function */
serial_t injective(const serial_t serial,
                   const unsigned bits /* at least serial_minwith (8) */,
                   const serial_t salt) {
  assert(bits > serial_minwith && bits <= serial_maxwith);

  /* LY: All these "magic" prime numbers were found
   *     and verified with a bit of brute force. */

  static const uint64_t m[64 - serial_minwith + 1] = {
      /* 8 - 24 */
      113, 157, 397, 653, 1753, 5641, 9697, 23873, 25693, 80833, 105953, 316937,
      309277, 834497, 1499933, 4373441, 10184137,
      /* 25 - 64 */
      10184137, 17279209, 33990377, 67295161, 284404553, 1075238767, 6346721573,
      6924051577, 19204053433, 45840188887, 53625693977, 73447827913,
      141638870249, 745683604649, 1283334050489, 1100828289853, 2201656586197,
      5871903036137, 11238507001417, 45264020802263, 105008404482889,
      81921776907059, 199987980256399, 307207457507641, 946769023178273,
      2420886491930041, 3601632139991929, 11984491914483833, 21805846439714153,
      23171543400565993, 53353226456762893, 155627817337932409,
      227827205384840249, 816509268558278821, 576933057762605689,
      2623957345935638441, 5048241705479929949, 4634245581946485653,
      4613509448041658233, 4952535426879925961};
  static const uint8_t s[64 - serial_minwith + 1] = {
      /* 8 - 24 */
      2, 3, 4, 4, 2, 4, 3, 3, 7, 3, 3, 4, 8, 3, 10, 3, 11,
      /* 25 - 64 */
      11, 9, 9, 9, 11, 10, 5, 14, 11, 16, 14, 12, 13, 16, 19, 10, 10, 21, 7, 20,
      10, 14, 22, 19, 3, 21, 18, 19, 26, 24, 2, 21, 25, 29, 24, 10, 11, 14, 20,
      19};

  const auto mult = m[bits - 8];
  const auto shift = s[bits - 8];
  serial_t result = serial * mult;
  if (salt) {
    const unsigned left = bits / 2;
    const unsigned right = bits - left;
    result = (result << left) | ((result & mask(bits)) >> right);
    result = (result ^ salt) * mult;
  }

  result ^= result << shift;
  result &= mask(bits);
  log_trace("keygen-injective: serial %" PRIu64 "/%u @%" PRIx64 ",%u,%" PRIu64
            " => %" PRIu64 "/%u",
            serial, bits, mult, shift, salt, result, bits);
  return result;
}

void __hot maker::pair(serial_t serial, const buffer &key, buffer &value,
                       serial_t value_age) {
  assert(mapping.width >= serial_minwith && mapping.width <= serial_maxwith);
  assert(mapping.split <= mapping.width);
  assert(mapping.mesh <= mapping.width);
  assert(mapping.rotate <= mapping.width);
  assert(mapping.offset <= mask(mapping.width));
  assert(!(key_essentials.flags &
           ~(MDBX_INTEGERKEY | MDBX_REVERSEKEY | MDBX_DUPSORT)));
  assert(!(value_essentials.flags & ~(MDBX_INTEGERDUP | MDBX_REVERSEDUP)));

  log_trace("keygen-pair: serial %" PRIu64 ", data-age %" PRIu64, serial,
            value_age);

  if (mapping.mesh >= serial_minwith) {
    serial =
        (serial & ~mask(mapping.mesh)) | injective(serial, mapping.mesh, salt);
    log_trace("keygen-pair: mesh@%u => %" PRIu64, mapping.mesh, serial);
  }

  if (mapping.rotate) {
    const unsigned right = mapping.rotate;
    const unsigned left = mapping.width - right;
    serial = (serial << left) | ((serial & mask(mapping.width)) >> right);
    log_trace("keygen-pair: rotate@%u => %" PRIu64 ", 0x%" PRIx64,
              mapping.rotate, serial, serial);
  }

  if (mapping.offset) {
    serial = (serial + mapping.offset) & mask(mapping.width);
    log_trace("keygen-pair: offset@%" PRIu64 " => %" PRIu64, mapping.offset,
              serial);
  }
  if (base) {
    serial += base;
    log_trace("keygen-pair: base@%" PRIu64 " => %" PRIu64, base, serial);
  }

  serial_t key_serial = serial;
  serial_t value_serial = value_age << mapping.split;
  if (mapping.split) {
    if (key_essentials.flags & MDBX_DUPSORT) {
      key_serial >>= mapping.split;
      value_serial += serial & mask(mapping.split);
    } else {
      /* Без MDBX_DUPSORT требуется уникальность ключей, а для этого нельзя
       * отбрасывать какие-либо биты serial после инъективного преобразования.
       * Поэтому key_serial не трогаем, а в value_serial нелинейно вмешиваем
       * запрошенное количество бит из serial */
      value_serial +=
          (serial ^ (serial >> mapping.split)) & mask(mapping.split);
    }

    value_serial |= value_age << mapping.split;
    log_trace("keygen-pair: split@%u => k%" PRIu64 ", v%" PRIu64, mapping.split,
              key_serial, value_serial);
  }

  log_trace("keygen-pair: key %" PRIu64 ", value %" PRIu64, key_serial,
            value_serial);
  mk(key_serial, key_essentials, *key);
  mk(value_serial, value_essentials, *value);

  if (log_enabled(logging::trace)) {
    char dump_key[128], dump_value[128];
    log_trace("keygen-pair: key %s, value %s",
              mdbx_dump_val(&key->value, dump_key, sizeof(dump_key)),
              mdbx_dump_val(&value->value, dump_value, sizeof(dump_value)));
  }
}

void maker::setup(const config::actor_params_pod &actor, unsigned actor_id,
                  unsigned thread_number) {
  key_essentials.flags =
      actor.table_flags & (MDBX_INTEGERKEY | MDBX_REVERSEKEY | MDBX_DUPSORT);
  assert(actor.keylen_min <= UINT8_MAX);
  key_essentials.minlen = (uint8_t)actor.keylen_min;
  assert(actor.keylen_max <= UINT16_MAX);
  key_essentials.maxlen = (uint16_t)actor.keylen_max;

  value_essentials.flags =
      actor.table_flags & (MDBX_INTEGERDUP | MDBX_REVERSEDUP);
  assert(actor.datalen_min <= UINT8_MAX);
  value_essentials.minlen = (uint8_t)actor.datalen_min;
  assert(actor.datalen_max <= UINT16_MAX);
  value_essentials.maxlen = (uint16_t)actor.datalen_max;

  assert(thread_number < 2);
  (void)thread_number;
  mapping = actor.keygen;
  salt = (actor.keygen.seed + actor_id) * UINT64_C(14653293970879851569);

  // FIXME: TODO
  base = 0;
}

void maker::make_ordered() {
  mapping.mesh = 0;
  mapping.rotate = 0;
}

bool maker::is_unordered() const {
  return (mapping.mesh >= serial_minwith || mapping.rotate) != 0;
}

bool maker::increment(serial_t &serial, int delta) const {
  if (serial > mask(mapping.width)) {
    log_extra("keygen-increment: %" PRIu64 " > %" PRIu64 ", overflow", serial,
              mask(mapping.width));
    return false;
  }

  serial_t target = serial + (int64_t)delta;
  if (target > mask(mapping.width) ||
      ((delta > 0) ? target < serial : target > serial)) {
    log_extra("keygen-increment: %" PRIu64 "%-d => %" PRIu64 ", overflow",
              serial, delta, target);
    return false;
  }

  log_extra("keygen-increment: %" PRIu64 "%-d => %" PRIu64 ", continue", serial,
            delta, target);
  serial = target;
  return true;
}

//-----------------------------------------------------------------------------

static size_t length(serial_t serial) {
  size_t n = 0;
  if (serial > UINT32_MAX) {
    n = 4;
    serial >>= 32;
  }
  if (serial > UINT16_MAX) {
    n += 2;
    serial >>= 16;
  }
  if (serial > UINT8_MAX) {
    n += 1;
    serial >>= 8;
  }
  return (serial > 0) ? n + 1 : n;
}

buffer alloc(size_t limit) {
  result *ptr = (result *)malloc(sizeof(result) + limit);
  if (unlikely(ptr == nullptr))
    failure_perror("malloc(keyvalue_buffer)", errno);
  ptr->value.iov_base = ptr->bytes;
  ptr->value.iov_len = 0;
  ptr->limit = limit;
  return buffer(ptr);
}

void __hot maker::mk(const serial_t serial, const essentials &params,
                     result &out) {
  assert(out.limit >= params.maxlen);
  assert(params.maxlen >= params.minlen);
  assert(params.maxlen >= length(serial));

  out.value.iov_base = out.bytes;
  out.value.iov_len =
      (params.maxlen > params.minlen)
          ? params.minlen + serial % (params.maxlen - params.minlen)
          : params.minlen;

  if (params.flags & (MDBX_INTEGERKEY | MDBX_INTEGERDUP)) {
    assert(params.maxlen == params.minlen);
    assert(params.minlen == 4 || params.minlen == 8);
    if (is_byteorder_le() || params.minlen == 8)
      out.u64 = serial;
    else
      out.u32 = (uint32_t)serial;
  } else if (params.flags & (MDBX_REVERSEKEY | MDBX_REVERSEDUP)) {
    if (out.value.iov_len > 8) {
      memset(out.bytes, '\0', out.value.iov_len - 8);
      unaligned::store(out.bytes + out.value.iov_len - 8, htobe64(serial));
    } else {
      out.u64 = htobe64(serial);
      if (out.value.iov_len < 8) {
        out.value.iov_len = std::max(length(serial), out.value.iov_len);
        out.value.iov_base = out.bytes + 8 - out.value.iov_len;
      }
    }
  } else {
    out.u64 = htole64(serial);
    if (out.value.iov_len > 8)
      memset(out.bytes + 8, '\0', out.value.iov_len - 8);
    else
      out.value.iov_len = std::max(length(serial), out.value.iov_len);
  }

  assert(out.value.iov_len >= params.minlen);
  assert(out.value.iov_len <= params.maxlen);
  assert(out.value.iov_len >= length(serial));
  assert(out.value.iov_base >= out.bytes);
  assert((uint8_t *)out.value.iov_base + out.value.iov_len <=
         out.bytes + out.limit);
}

} /* namespace keygen */
