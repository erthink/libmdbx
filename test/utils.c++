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
#include <float.h>
#if defined(HAVE_IEEE754_H) || __has_include(<ieee754.h>)
#include <ieee754.h>
#endif
#if defined(__APPLE__) || defined(__MACH__)
#include <mach/mach_time.h>
#endif /* defined(__APPLE__) || defined(__MACH__) */

std::string format(const char *fmt, ...) {
  va_list ap, ones;
  va_start(ap, fmt);
  va_copy(ones, ap);
#ifdef _MSC_VER
  int needed = _vscprintf(fmt, ap);
#else
  int needed = vsnprintf(nullptr, 0, fmt, ap);
#endif
  assert(needed >= 0);
  va_end(ap);
  std::string result;
  result.reserve((size_t)needed + 1);
  result.resize((size_t)needed, '\0');
  MDBX_MAYBE_UNUSED int actual =
      vsnprintf((char *)result.data(), result.capacity(), fmt, ones);
  assert(actual == needed);
  (void)actual;
  va_end(ones);
  return result;
}

std::string data2hex(const void *ptr, size_t bytes, simple_checksum &checksum) {
  std::string result;
  if (bytes > 0) {
    const uint8_t *data = (const uint8_t *)ptr;
    checksum.push(data, bytes);
    result.reserve(bytes * 2);
    const uint8_t *const end = data + bytes;
    do {
      char h = *data >> 4;
      char l = *data & 15;
      result.push_back((l < 10) ? l + '0' : l - 10 + 'a');
      result.push_back((h < 10) ? h + '0' : h - 10 + 'a');
    } while (++data < end);
  }
  assert(result.size() == bytes * 2);
  return result;
}

bool hex2data(const char *hex_begin, const char *hex_end, void *ptr,
              size_t bytes, simple_checksum &checksum) {
  if (bytes * 2 != (size_t)(hex_end - hex_begin))
    return false;

  uint8_t *data = (uint8_t *)ptr;
  for (const char *hex = hex_begin; hex != hex_end; hex += 2, ++data) {
    unsigned l = hex[0], h = hex[1];

    if (l >= '0' && l <= '9')
      l = l - '0';
    else if (l >= 'A' && l <= 'F')
      l = l - 'A' + 10;
    else if (l >= 'a' && l <= 'f')
      l = l - 'a' + 10;
    else
      return false;

    if (h >= '0' && h <= '9')
      h = h - '0';
    else if (h >= 'A' && h <= 'F')
      h = h - 'A' + 10;
    else if (h >= 'a' && h <= 'f')
      h = h - 'a' + 10;
    else
      return false;

    uint32_t c = l + (h << 4);
    checksum.push(c);
    *data = (uint8_t)c;
  }
  return true;
}

bool is_samedata(const MDBX_val *a, const MDBX_val *b) {
  return a->iov_len == b->iov_len &&
         memcmp(a->iov_base, b->iov_base, a->iov_len) == 0;
}

//-----------------------------------------------------------------------------

uint64_t prng64_white(uint64_t &state) {
  state = prng64_map2_careless(state);
  return bleach64(state);
}

uint32_t prng32(uint64_t &state) {
  return (uint32_t)(prng64_careless(state) >> 32);
}

void prng_fill(uint64_t &state, void *ptr, size_t bytes) {
  uint32_t u32 = prng32(state);

  while (bytes >= 4) {
    memcpy(ptr, &u32, 4);
    ptr = (uint32_t *)ptr + 1;
    bytes -= 4;
    u32 = prng32(state);
  }

  switch (bytes & 3) {
  case 3:
    memcpy(ptr, &u32, 3);
    break;
  case 2:
    memcpy(ptr, &u32, 2);
    break;
  case 1:
    memcpy(ptr, &u32, 1);
    break;
  case 0:
    break;
  }
}

static __thread uint64_t prng_state;

void prng_seed(uint64_t seed) { prng_state = bleach64(seed); }

uint32_t prng32(void) { return prng32(prng_state); }

uint64_t prng64(void) { return prng64_white(prng_state); }

void prng_fill(void *ptr, size_t bytes) { prng_fill(prng_state, ptr, bytes); }

double double_from_lower(uint64_t salt) {
#ifdef IEEE754_DOUBLE_BIAS
  ieee754_double r;
  r.ieee.negative = 0;
  r.ieee.exponent = IEEE754_DOUBLE_BIAS;
  r.ieee.mantissa0 = (unsigned)(salt >> 32);
  r.ieee.mantissa1 = (unsigned)salt;
  return r.d;
#else
  const uint64_t top = (UINT64_C(1) << DBL_MANT_DIG) - 1;
  const double scale = 1.0 / (double)top;
  return (salt & top) * scale;
#endif
}

double double_from_upper(uint64_t salt) {
#ifdef IEEE754_DOUBLE_BIAS
  ieee754_double r;
  r.ieee.negative = 0;
  r.ieee.exponent = IEEE754_DOUBLE_BIAS;
  salt >>= 64 - DBL_MANT_DIG;
  r.ieee.mantissa0 = unsigned(salt >> 32);
  r.ieee.mantissa1 = unsigned(salt);
  return r.d;
#else
  const uint64_t top = (UINT64_C(1) << DBL_MANT_DIG) - 1;
  const double scale = 1.0 / (double)top;
  return (salt >> (64 - DBL_MANT_DIG)) * scale;
#endif
}

bool flipcoin() { return prng32() & 1; }
bool flipcoin_x2() { return (prng32() & 3) == 0; }
bool flipcoin_x3() { return (prng32() & 7) == 0; }
bool flipcoin_x4() { return (prng32() & 15) == 0; }
bool flipcoin_n(unsigned n) {
  return (prng64() & ((UINT64_C(1) << n) - 1)) == 0;
}

bool jitter(unsigned probability_percent) {
  const uint32_t top = UINT32_MAX - UINT32_MAX % 100;
  uint32_t dice, edge = (top) / 100 * probability_percent;
  do
    dice = prng32();
  while (dice >= top);
  return dice < edge;
}

void jitter_delay(bool extra) {
  unsigned dice = prng32() & 3;
  if (dice == 0) {
    log_trace("== jitter.no-delay");
  } else {
    log_trace(">> jitter.delay: dice %u", dice);
    do {
      cpu_relax();
      memory_barrier();
      cpu_relax();
      if (dice > 1) {
        osal_yield();
        cpu_relax();
        if (dice > 2) {
          size_t us =
              prng32() & (extra ? 0xffff /* 656 ms */ : 0x3ff /* 1 ms */);
          log_trace("== jitter.delay: %0.6f", us / 1000000.0);
          osal_udelay(us);
        }
      }
    } while (flipcoin());
    log_trace("<< jitter.delay: dice %u", dice);
  }
}
