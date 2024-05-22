/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024
/// \copyright SPDX-License-Identifier: Apache-2.0

#include "test.h++"

static const uint64_t primes[64] = {
    /* */
    0, 1, 3, 7, 13, 31, 61, 127, 251, 509, 1021, 2039, 4093, 8191, 16381,
    /* */
    UINT64_C(32749), UINT64_C(65521), UINT64_C(131071), UINT64_C(262139),
    UINT64_C(524287), UINT64_C(1048573), UINT64_C(2097143), UINT64_C(4194301),
    UINT64_C(8388593), UINT64_C(16777213), UINT64_C(33554393),
    UINT64_C(67108859), UINT64_C(134217689), UINT64_C(268435399),
    UINT64_C(536870909), UINT64_C(1073741789), UINT64_C(2147483647),
    UINT64_C(4294967291), UINT64_C(8589934583), UINT64_C(17179869143),
    UINT64_C(34359738337), UINT64_C(68719476731), UINT64_C(137438953447),
    UINT64_C(274877906899), UINT64_C(549755813881), UINT64_C(1099511627689),
    UINT64_C(2199023255531), UINT64_C(4398046511093), UINT64_C(8796093022151),
    UINT64_C(17592186044399), UINT64_C(35184372088777),
    UINT64_C(70368744177643), UINT64_C(140737488355213),
    UINT64_C(281474976710597), UINT64_C(562949953421231),
    UINT64_C(1125899906842597), UINT64_C(2251799813685119),
    UINT64_C(4503599627370449), UINT64_C(9007199254740881),
    UINT64_C(18014398509481951), UINT64_C(36028797018963913),
    UINT64_C(72057594037927931), UINT64_C(144115188075855859),
    UINT64_C(288230376151711717), UINT64_C(576460752303423433),
    UINT64_C(1152921504606846883), UINT64_C(2305843009213693951),
    UINT64_C(4611686018427387847), UINT64_C(9223372036854775783)};

/* static unsigned supid_log2(uint64_t v) {
  unsigned r = 0;
  while (v > 1) {
    v >>= 1;
    r += 1;
  }
  return r;
} */

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

  const auto mask = actor_params::serial_mask(bits);
  const auto mult = m[bits - 8];
  const auto shift = s[bits - 8];
  serial_t result = serial * mult;
  if (salt) {
    const unsigned left = bits / 2;
    const unsigned right = bits - left;
    result = (result << left) | ((result & mask) >> right);
    result = (result ^ salt) * mult;
  }

  result ^= (result & mask) >> shift;
  result &= mask;
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
             unsigned(MDBX_INTEGERDUP | MDBX_REVERSEDUP | MDBX_DUPFIXED))));

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
  serial_t value_serial = (value_age & value_age_mask) << mapping.split;
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

    log_trace("keygen-pair: split@%u => k%" PRIu64 ", v%" PRIu64, mapping.split,
              key_serial, value_serial);
  }

  log_trace("keygen-pair: key %" PRIu64 ", value %" PRIu64, key_serial,
            value_serial);
  key_serial = mk_begin(key_serial, key_essentials, *key);
  value_serial = mk_begin(value_serial, value_essentials, *value);

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

void maker::setup(const config::actor_params_pod &actor,
                  unsigned thread_number) {
#if CONSTEXPR_ENUM_FLAGS_OPERATIONS
  static_assert(unsigned(MDBX_INTEGERKEY | MDBX_REVERSEKEY | MDBX_DUPSORT |
                         MDBX_DUPFIXED | MDBX_INTEGERDUP | MDBX_REVERSEDUP) <
                    UINT16_MAX,
                "WTF?");
#else
  assert(unsigned(MDBX_INTEGERKEY | MDBX_REVERSEKEY | MDBX_DUPSORT |
                  MDBX_DUPFIXED | MDBX_INTEGERDUP | MDBX_REVERSEDUP) <
         UINT16_MAX);
#endif

  key_essentials.flags = uint16_t(
      actor.table_flags &
      MDBX_db_flags_t(MDBX_INTEGERKEY | MDBX_REVERSEKEY | MDBX_DUPSORT));
  assert(actor.keylen_min <= UINT16_MAX);
  key_essentials.minlen = uint16_t(actor.keylen_min);
  assert(actor.keylen_max <= UINT32_MAX);
  key_essentials.maxlen = std::min(
      uint32_t(actor.keylen_max),
      uint32_t(mdbx_limits_keysize_max(actor.pagesize, actor.table_flags)));
  key_essentials.bits = (key_essentials.maxlen < sizeof(serial_t))
                            ? key_essentials.maxlen * CHAR_BIT
                            : sizeof(serial_t) * CHAR_BIT;
  key_essentials.mask = actor_params::serial_mask(key_essentials.bits);
  assert(key_essentials.bits > 63 ||
         key_essentials.mask > primes[key_essentials.bits]);

  value_essentials.flags = uint16_t(
      actor.table_flags &
      MDBX_db_flags_t(MDBX_INTEGERDUP | MDBX_REVERSEDUP | MDBX_DUPFIXED));
  assert(actor.datalen_min <= UINT16_MAX);
  value_essentials.minlen = uint16_t(actor.datalen_min);
  assert(actor.datalen_max <= UINT32_MAX);
  value_essentials.maxlen = std::min(
      uint32_t(actor.datalen_max),
      uint32_t(mdbx_limits_valsize_max(actor.pagesize, actor.table_flags)));
  value_essentials.bits = (value_essentials.maxlen < sizeof(serial_t))
                              ? value_essentials.maxlen * CHAR_BIT
                              : sizeof(serial_t) * CHAR_BIT;
  value_essentials.mask = actor_params::serial_mask(value_essentials.bits);
  assert(value_essentials.bits > 63 ||
         value_essentials.mask > primes[value_essentials.bits]);

  if (!actor.keygen.zero_fill) {
    key_essentials.flags |= essentials::prng_fill_flag;
    value_essentials.flags |= essentials::prng_fill_flag;
  }

  mapping = actor.keygen;
  const auto split = mapping.split;
  while (mapping.split >
             value_essentials.bits - essentials::value_age_minwidth ||
         mapping.split >= mapping.width)
    mapping.split -= 1;
  if (split != mapping.split)
    log_notice("keygen: reduce mapping-split from %u to %u", split,
               mapping.split);

  const auto width = mapping.width;
  while (unsigned((actor.table_flags & MDBX_DUPSORT)
                      ? mapping.width - mapping.split
                      : mapping.width) > key_essentials.bits)
    mapping.width -= 1;
  if (width != mapping.width)
    log_notice("keygen: reduce mapping-width from %u to %u", width,
               mapping.width);

  value_age_bits = value_essentials.bits - mapping.split;
  value_age_mask = actor_params::serial_mask(value_age_bits);
  assert(value_age_bits >= essentials::value_age_minwidth);

  salt = (prng_state ^
          (thread_number * 1575554837) * UINT64_C(59386707711075671)) *
         UINT64_C(14653293970879851569);
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

serial_t __hot maker::mk_begin(serial_t serial, const essentials &params,
                               result &out) {
  assert(out.limit >= params.maxlen);
  assert(params.maxlen >= params.minlen);
  assert(serial <= params.mask);
  if (unlikely(serial > params.mask)) {
#if 1
    serial %= primes[params.bits];
    assert(params.mask > primes[params.bits]);
#else
    const serial_t maxbits = params.maxlen * CHAR_BIT;
    serial ^= (serial >> maxbits / 2) *
              serial_t((sizeof(serial_t) > 4) ? UINT64_C(40719303417517073)
                                              : UINT32_C(3708688457));
    serial &= params.mask;
#endif
    assert(params.maxlen >= length(serial));
  }

  out.value.iov_len = std::max(unsigned(params.minlen), length(serial));
  const auto variation = params.maxlen - params.minlen;
  if (variation) {
    if (serial % (variation + serial_t(1))) {
      auto refix = serial * UINT64_C(48835288005252737);
      refix ^= refix >> 32;
      out.value.iov_len =
          std::max(out.value.iov_len,
                   params.minlen + size_t(1) + size_t(refix) % variation);
    }
  }

  assert(length(serial) <= out.value.iov_len);
  assert(out.value.iov_len >= params.minlen);
  assert(out.value.iov_len <= params.maxlen);
  return serial;
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
