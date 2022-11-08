/*
 * Copyright 2017-2022 Leonid Yuriev <leo@yuriev.ru>
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

#include "test.h++"

namespace keygen {

/* LY: https://en.wikipedia.org/wiki/Injective_function */
serial_t injective(const serial_t serial,
                   const unsigned bits /* at least serial_minwith (8) */,
                   const serial_t salt) {
  assert(bits >= serial_minwith && bits <= serial_maxwith);

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
    result = (result << left) |
             ((result & actor_params::serial_mask(bits)) >> right);
    result = (result ^ salt) * mult;
  }

  result ^= result << shift;
  result &= actor_params::serial_mask(bits);
  log_trace("keygen-injective: serial %" PRIu64 "/%u @%" PRIx64 ",%u,%" PRIu64
            " => %" PRIu64 "/%u",
            serial, bits, mult, shift, salt, result, bits);
  return result;
}

void __hot maker::pair(serial_t serial, const buffer &key, buffer &value,
                       serial_t value_age, const bool keylen_changeable) {
  assert(mapping.width >= serial_minwith && mapping.width <= serial_maxwith);
  assert(mapping.split <= mapping.width);
  assert(mapping.mesh <= mapping.width);
  assert(mapping.rotate <= mapping.width);
  assert(mapping.offset <= actor_params::serial_mask(mapping.width));
  assert(!(key_essentials.flags &
           ~(essentials::prng_fill_flag |
             unsigned(MDBX_INTEGERKEY | MDBX_REVERSEKEY | MDBX_DUPSORT))));
  assert(!(value_essentials.flags &
           ~(essentials::prng_fill_flag |
             unsigned(MDBX_INTEGERDUP | MDBX_REVERSEDUP))));

  log_trace("keygen-pair: serial %" PRIu64 ", data-age %" PRIu64, serial,
            value_age);

  if (mapping.mesh >= serial_minwith) {
    serial = (serial & ~actor_params::serial_mask(mapping.mesh)) |
             injective(serial, mapping.mesh, salt);
    log_trace("keygen-pair: mesh@%u => %" PRIu64, mapping.mesh, serial);
  }

  if (mapping.rotate) {
    const unsigned right = mapping.rotate;
    const unsigned left = mapping.width - right;
    serial = (serial << left) |
             ((serial & actor_params::serial_mask(mapping.width)) >> right);
    log_trace("keygen-pair: rotate@%u => %" PRIu64 ", 0x%" PRIx64,
              mapping.rotate, serial, serial);
  }

  if (mapping.offset) {
    serial =
        (serial + mapping.offset) & actor_params::serial_mask(mapping.width);
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
    if (MDBX_db_flags_t(key_essentials.flags) & MDBX_DUPSORT) {
      key_serial >>= mapping.split;
      value_serial += serial & actor_params::serial_mask(mapping.split);
    } else {
      /* Без MDBX_DUPSORT требуется уникальность ключей, а для этого нельзя
       * отбрасывать какие-либо биты serial после инъективного преобразования.
       * Поэтому key_serial не трогаем, а в value_serial нелинейно вмешиваем
       * запрошенное количество бит из serial */
      value_serial +=
          (serial ^ (serial >> mapping.split) * UINT64_C(57035339200100753)) &
          actor_params::serial_mask(mapping.split);
    }

    value_serial |= value_age << mapping.split;
    log_trace("keygen-pair: split@%u => k%" PRIu64 ", v%" PRIu64, mapping.split,
              key_serial, value_serial);
  }

  log_trace("keygen-pair: key %" PRIu64 ", value %" PRIu64, key_serial,
            value_serial);
  mk_begin(key_serial, key_essentials, *key);
  mk_begin(value_serial, value_essentials, *value);

#if 0 /* unused for now */
  if (key->value.iov_len + value->value.iov_len > pair_maxlen) {
    unsigned extra = key->value.iov_len + value->value.iov_len - pair_maxlen;
    if (keylen_changeable &&
        key->value.iov_len > std::max(8u, (unsigned)key_essentials.minlen)) {
#if defined(__GNUC__) || defined(__clang__)
      const bool coin = __builtin_parityll(serial) != 0;
#else
      const bool coin = INT64_C(0xF2CEECA9989BD96A) * int64_t(serial) < 0;
#endif
      if (coin) {
        const unsigned gap =
            key->value.iov_len - std::max(8u, (unsigned)key_essentials.minlen);
        const unsigned chop = std::min(gap, extra);
        log_trace("keygen-pair: chop %u key-len %u -> %u", chop,
                  (unsigned)key->value.iov_len,
                  (unsigned)key->value.iov_len - chop);
        key->value.iov_len -= chop;
        extra -= chop;
      }
    }
    if (extra && value->value.iov_len >
                     std::max(8u, (unsigned)value_essentials.minlen)) {
      const unsigned gap = value->value.iov_len -
                           std::max(8u, (unsigned)value_essentials.minlen);
      const unsigned chop = std::min(gap, extra);
      log_trace("keygen-pair: chop %u value-len %u -> %u", chop,
                (unsigned)value->value.iov_len,
                (unsigned)value->value.iov_len - chop);
      value->value.iov_len -= chop;
      extra -= chop;
    }
    if (keylen_changeable && extra &&
        key->value.iov_len > std::max(8u, (unsigned)key_essentials.minlen)) {
      const unsigned gap =
          key->value.iov_len - std::max(8u, (unsigned)key_essentials.minlen);
      const unsigned chop = std::min(gap, extra);
      log_trace("keygen-pair: chop %u key-len %u -> %u", chop,
                (unsigned)key->value.iov_len,
                (unsigned)key->value.iov_len - chop);
      key->value.iov_len -= chop;
      extra -= chop;
    }
  }
#else
  (void)keylen_changeable;
#endif /* unused for now */

  mk_continue(key_serial, key_essentials, *key);
  mk_continue(value_serial, value_essentials, *value);
  log_pair(logging::trace, "kv", key, value);
}

void maker::setup(const config::actor_params_pod &actor, unsigned actor_id,
                  unsigned thread_number) {
#if CONSTEXPR_ENUM_FLAGS_OPERATIONS
  static_assert(unsigned(MDBX_INTEGERKEY | MDBX_REVERSEKEY | MDBX_DUPSORT |
                         MDBX_INTEGERDUP | MDBX_REVERSEDUP) < UINT16_MAX,
                "WTF?");
#else
  assert(unsigned(MDBX_INTEGERKEY | MDBX_REVERSEKEY | MDBX_DUPSORT |
                  MDBX_INTEGERDUP | MDBX_REVERSEDUP) < UINT16_MAX);
#endif
  key_essentials.flags = uint16_t(
      actor.table_flags &
      MDBX_db_flags_t(MDBX_INTEGERKEY | MDBX_REVERSEKEY | MDBX_DUPSORT));
  assert(actor.keylen_min <= UINT16_MAX);
  key_essentials.minlen = uint16_t(actor.keylen_min);
  assert(actor.keylen_max <= UINT32_MAX);
  key_essentials.maxlen =
      std::min(uint32_t(actor.keylen_max),
               uint32_t(mdbx_limits_keysize_max(
                   actor.pagesize, MDBX_db_flags_t(key_essentials.flags))));

  value_essentials.flags = uint16_t(
      actor.table_flags & MDBX_db_flags_t(MDBX_INTEGERDUP | MDBX_REVERSEDUP));
  assert(actor.datalen_min <= UINT16_MAX);
  value_essentials.minlen = uint16_t(actor.datalen_min);
  assert(actor.datalen_max <= UINT32_MAX);
  value_essentials.maxlen =
      std::min(uint32_t(actor.datalen_max),
               uint32_t(mdbx_limits_valsize_max(
                   actor.pagesize, MDBX_db_flags_t(key_essentials.flags))));

  if (!actor.keygen.zero_fill) {
    key_essentials.flags |= essentials::prng_fill_flag;
    value_essentials.flags |= essentials::prng_fill_flag;
  }

  (void)thread_number;
  mapping = actor.keygen;
  salt = (actor.keygen.seed + actor_id) * UINT64_C(14653293970879851569);

  base = actor.serial_base();
}

bool maker::is_unordered() const {
  return mapping.rotate ||
         mapping.mesh > ((MDBX_db_flags_t(key_essentials.flags) & MDBX_DUPSORT)
                             ? 0
                             : mapping.split);
}

void maker::seek2end(serial_t &serial) const {
  serial = actor_params::serial_mask(mapping.width) - 1;
}

bool maker::increment(serial_t &serial, int64_t delta) const {
  if (serial > actor_params::serial_mask(mapping.width)) {
    log_extra("keygen-increment: %" PRIu64 " > %" PRIu64 ", overflow", serial,
              actor_params::serial_mask(mapping.width));
    return false;
  }

  serial_t target = serial + delta;
  if (target > actor_params::serial_mask(mapping.width) ||
      ((delta > 0) ? target < serial : target > serial)) {
    log_extra("keygen-increment: %" PRIu64 "%-" PRId64 " => %" PRIu64
              ", overflow",
              serial, delta, target);
    return false;
  }

  log_extra("keygen-increment: %" PRIu64 "%-" PRId64 " => %" PRIu64
            ", continue",
            serial, delta, target);
  serial = target;
  return true;
}

//-----------------------------------------------------------------------------

MDBX_NOTHROW_PURE_FUNCTION static inline unsigned length(serial_t serial) {
#if defined(__clang__) && __clang__ > 8
  unsigned n = 0;
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
#else
  unsigned n = (serial > UINT32_MAX) ? 4 : 0;
  serial = (serial > UINT32_MAX) ? serial >> 32 : serial;

  n += (serial > UINT16_MAX) ? 2 : 0;
  serial = (serial > UINT16_MAX) ? serial >> 16 : serial;

  n += (serial > UINT8_MAX);
  serial = (serial > UINT8_MAX) ? serial >> 8 : serial;
#endif
  return n + (serial > 0);
}

buffer alloc(size_t limit) {
  result *ptr = (result *)malloc(sizeof(result) + limit + 8);
  if (unlikely(ptr == nullptr))
    failure_perror("malloc(keyvalue_buffer)", errno);
  ptr->value.iov_base = ptr->bytes;
  ptr->value.iov_len = 0;
  ptr->limit = limit + 8;
  return buffer(ptr);
}

void __hot maker::mk_begin(const serial_t serial, const essentials &params,
                           result &out) {
  assert(out.limit >= params.maxlen);
  assert(params.maxlen >= params.minlen);
  assert(params.maxlen >= length(serial));

  out.value.iov_len = std::max(unsigned(params.minlen), length(serial));
  const auto variation = params.maxlen - params.minlen;
  if (variation) {
    if (serial % (variation + 1)) {
      auto refix = serial * UINT64_C(48835288005252737);
      refix ^= refix >> 32;
      out.value.iov_len = std::max(
          out.value.iov_len, params.minlen + 1 + size_t(refix) % variation);
    }
  }

  assert(length(serial) <= out.value.iov_len);
  assert(out.value.iov_len >= params.minlen);
  assert(out.value.iov_len <= params.maxlen);
}

void __hot maker::mk_continue(const serial_t serial, const essentials &params,
                              result &out) {
#if CONSTEXPR_ENUM_FLAGS_OPERATIONS
  static_assert(
      (essentials::prng_fill_flag &
       unsigned(MDBX_DUPSORT | MDBX_DUPFIXED | MDBX_INTEGERKEY |
                MDBX_INTEGERDUP | MDBX_REVERSEKEY | MDBX_REVERSEDUP)) == 0,
      "WTF?");
#else
  assert((essentials::prng_fill_flag &
          unsigned(MDBX_DUPSORT | MDBX_DUPFIXED | MDBX_INTEGERKEY |
                   MDBX_INTEGERDUP | MDBX_REVERSEKEY | MDBX_REVERSEDUP)) == 0);
#endif
  assert(length(serial) <= out.value.iov_len);
  out.value.iov_base = out.bytes;
  if (MDBX_db_flags_t(params.flags) & (MDBX_INTEGERKEY | MDBX_INTEGERDUP)) {
    assert(params.maxlen == params.minlen);
    if (MDBX_db_flags_t(params.flags) & (MDBX_INTEGERKEY | MDBX_INTEGERDUP))
      assert(params.minlen == 4 || params.minlen == 8);
    out.u64 = serial;
    if (!is_byteorder_le() && out.value.iov_len != 8)
      out.u32 = uint32_t(serial);
  } else {
    const auto prefix =
        std::max(std::min(unsigned(params.minlen), 8u), length(serial));
    out.u64 = htobe64(serial);
    out.value.iov_base = out.bytes + 8 - prefix;
    if (out.value.iov_len > prefix) {
      if (params.flags & essentials::prng_fill_flag) {
        uint64_t state = serial ^ UINT64_C(0x923ab47b7ee6f6e4);
        prng_fill(state, out.bytes + 8, out.value.iov_len - prefix);
      } else
        memset(out.bytes + 8, '\0', out.value.iov_len - prefix);
    }
    if (unlikely(MDBX_db_flags_t(params.flags) &
                 (MDBX_REVERSEKEY | MDBX_REVERSEDUP)))
      std::reverse((char *)out.value.iov_base,
                   (char *)out.value.iov_base + out.value.iov_len);
  }

  assert(out.value.iov_len >= params.minlen);
  assert(out.value.iov_len <= params.maxlen);
  assert(out.value.iov_len >= length(serial));
  assert(out.value.iov_base >= out.bytes);
  assert((char *)out.value.iov_base + out.value.iov_len <=
         (char *)&out.bytes + out.limit);
}

void log_pair(logging::loglevel level, const char *prefix, const buffer &key,
              buffer &value) {
  if (log_enabled(level)) {
    char dump_key[4096], dump_value[4096];
    logging::output(
        level, "%s-pair: key %s, value %s", prefix,
        mdbx_dump_val(&key->value, dump_key, sizeof(dump_key)),
        mdbx_dump_val(&value->value, dump_value, sizeof(dump_value)));
  }
}

} /* namespace keygen */
