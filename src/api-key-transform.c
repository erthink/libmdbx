/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024

#include "internals.h"

static inline double key2double(const int64_t key) {
  union {
    uint64_t u;
    double f;
  } casting;

  casting.u = (key < 0) ? key + UINT64_C(0x8000000000000000) : UINT64_C(0xffffFFFFffffFFFF) - key;
  return casting.f;
}

static inline uint64_t double2key(const double *const ptr) {
  STATIC_ASSERT(sizeof(double) == sizeof(int64_t));
  const int64_t i = *(const int64_t *)ptr;
  const uint64_t u = (i < 0) ? UINT64_C(0xffffFFFFffffFFFF) - i : i + UINT64_C(0x8000000000000000);
  if (ASSERT_ENABLED()) {
    const double f = key2double(u);
    assert(memcmp(&f, ptr, sizeof(double)) == 0);
  }
  return u;
}

static inline float key2float(const int32_t key) {
  union {
    uint32_t u;
    float f;
  } casting;

  casting.u = (key < 0) ? key + UINT32_C(0x80000000) : UINT32_C(0xffffFFFF) - key;
  return casting.f;
}

static inline uint32_t float2key(const float *const ptr) {
  STATIC_ASSERT(sizeof(float) == sizeof(int32_t));
  const int32_t i = *(const int32_t *)ptr;
  const uint32_t u = (i < 0) ? UINT32_C(0xffffFFFF) - i : i + UINT32_C(0x80000000);
  if (ASSERT_ENABLED()) {
    const float f = key2float(u);
    assert(memcmp(&f, ptr, sizeof(float)) == 0);
  }
  return u;
}

uint64_t mdbx_key_from_double(const double ieee754_64bit) { return double2key(&ieee754_64bit); }

uint64_t mdbx_key_from_ptrdouble(const double *const ieee754_64bit) { return double2key(ieee754_64bit); }

uint32_t mdbx_key_from_float(const float ieee754_32bit) { return float2key(&ieee754_32bit); }

uint32_t mdbx_key_from_ptrfloat(const float *const ieee754_32bit) { return float2key(ieee754_32bit); }

#define IEEE754_DOUBLE_MANTISSA_SIZE 52
#define IEEE754_DOUBLE_EXPONENTA_BIAS 0x3FF
#define IEEE754_DOUBLE_EXPONENTA_MAX 0x7FF
#define IEEE754_DOUBLE_IMPLICIT_LEAD UINT64_C(0x0010000000000000)
#define IEEE754_DOUBLE_MANTISSA_MASK UINT64_C(0x000FFFFFFFFFFFFF)
#define IEEE754_DOUBLE_MANTISSA_AMAX UINT64_C(0x001FFFFFFFFFFFFF)

static inline int clz64(uint64_t value) {
#if __GNUC_PREREQ(4, 1) || __has_builtin(__builtin_clzl)
  if (sizeof(value) == sizeof(int))
    return __builtin_clz(value);
  if (sizeof(value) == sizeof(long))
    return __builtin_clzl(value);
#if (defined(__SIZEOF_LONG_LONG__) && __SIZEOF_LONG_LONG__ == 8) || __has_builtin(__builtin_clzll)
  return __builtin_clzll(value);
#endif /* have(long long) && long long == uint64_t */
#endif /* GNU C */

#if defined(_MSC_VER)
  unsigned long index;
#if defined(_M_AMD64) || defined(_M_ARM64) || defined(_M_X64)
  _BitScanReverse64(&index, value);
  return 63 - index;
#else
  if (value > UINT32_MAX) {
    _BitScanReverse(&index, (uint32_t)(value >> 32));
    return 31 - index;
  }
  _BitScanReverse(&index, (uint32_t)value);
  return 63 - index;
#endif
#endif /* MSVC */

  value |= value >> 1;
  value |= value >> 2;
  value |= value >> 4;
  value |= value >> 8;
  value |= value >> 16;
  value |= value >> 32;
  static const uint8_t debruijn_clz64[64] = {63, 16, 62, 7,  15, 36, 61, 3,  6,  14, 22, 26, 35, 47, 60, 2,
                                             9,  5,  28, 11, 13, 21, 42, 19, 25, 31, 34, 40, 46, 52, 59, 1,
                                             17, 8,  37, 4,  23, 27, 48, 10, 29, 12, 43, 20, 32, 41, 53, 18,
                                             38, 24, 49, 30, 44, 33, 54, 39, 50, 45, 55, 51, 56, 57, 58, 0};
  return debruijn_clz64[value * UINT64_C(0x03F79D71B4CB0A89) >> 58];
}

static inline uint64_t round_mantissa(const uint64_t u64, int shift) {
  assert(shift < 0 && u64 > 0);
  shift = -shift;
  const unsigned half = 1 << (shift - 1);
  const unsigned lsb = 1 & (unsigned)(u64 >> shift);
  const unsigned tie2even = 1 ^ lsb;
  return (u64 + half - tie2even) >> shift;
}

uint64_t mdbx_key_from_jsonInteger(const int64_t json_integer) {
  const uint64_t bias = UINT64_C(0x8000000000000000);
  if (json_integer > 0) {
    const uint64_t u64 = json_integer;
    int shift = clz64(u64) - (64 - IEEE754_DOUBLE_MANTISSA_SIZE - 1);
    uint64_t mantissa = u64 << shift;
    if (unlikely(shift < 0)) {
      mantissa = round_mantissa(u64, shift);
      if (mantissa > IEEE754_DOUBLE_MANTISSA_AMAX)
        mantissa = round_mantissa(u64, --shift);
    }

    assert(mantissa >= IEEE754_DOUBLE_IMPLICIT_LEAD && mantissa <= IEEE754_DOUBLE_MANTISSA_AMAX);
    const uint64_t exponent = (uint64_t)IEEE754_DOUBLE_EXPONENTA_BIAS + IEEE754_DOUBLE_MANTISSA_SIZE - shift;
    assert(exponent > 0 && exponent <= IEEE754_DOUBLE_EXPONENTA_MAX);
    const uint64_t key = bias + (exponent << IEEE754_DOUBLE_MANTISSA_SIZE) + (mantissa - IEEE754_DOUBLE_IMPLICIT_LEAD);
#if !defined(_MSC_VER) || defined(_DEBUG) /* Workaround for MSVC error LNK2019: unresolved external                    \
                                             symbol __except1 referenced in function __ftol3_except */
    assert(key == mdbx_key_from_double((double)json_integer));
#endif /* Workaround for MSVC */
    return key;
  }

  if (json_integer < 0) {
    const uint64_t u64 = -json_integer;
    int shift = clz64(u64) - (64 - IEEE754_DOUBLE_MANTISSA_SIZE - 1);
    uint64_t mantissa = u64 << shift;
    if (unlikely(shift < 0)) {
      mantissa = round_mantissa(u64, shift);
      if (mantissa > IEEE754_DOUBLE_MANTISSA_AMAX)
        mantissa = round_mantissa(u64, --shift);
    }

    assert(mantissa >= IEEE754_DOUBLE_IMPLICIT_LEAD && mantissa <= IEEE754_DOUBLE_MANTISSA_AMAX);
    const uint64_t exponent = (uint64_t)IEEE754_DOUBLE_EXPONENTA_BIAS + IEEE754_DOUBLE_MANTISSA_SIZE - shift;
    assert(exponent > 0 && exponent <= IEEE754_DOUBLE_EXPONENTA_MAX);
    const uint64_t key =
        bias - 1 - (exponent << IEEE754_DOUBLE_MANTISSA_SIZE) - (mantissa - IEEE754_DOUBLE_IMPLICIT_LEAD);
#if !defined(_MSC_VER) || defined(_DEBUG) /* Workaround for MSVC error LNK2019: unresolved external                    \
                                             symbol __except1 referenced in function __ftol3_except */
    assert(key == mdbx_key_from_double((double)json_integer));
#endif /* Workaround for MSVC */
    return key;
  }

  return bias;
}

int64_t mdbx_jsonInteger_from_key(const MDBX_val v) {
  assert(v.iov_len == 8);
  const uint64_t key = unaligned_peek_u64(2, v.iov_base);
  const uint64_t bias = UINT64_C(0x8000000000000000);
  const uint64_t covalent = (key > bias) ? key - bias : bias - key - 1;
  const int shift = IEEE754_DOUBLE_EXPONENTA_BIAS + 63 -
                    (IEEE754_DOUBLE_EXPONENTA_MAX & (int)(covalent >> IEEE754_DOUBLE_MANTISSA_SIZE));
  if (unlikely(shift < 1))
    return (key < bias) ? INT64_MIN : INT64_MAX;
  if (unlikely(shift > 63))
    return 0;

  const uint64_t unscaled = ((covalent & IEEE754_DOUBLE_MANTISSA_MASK) << (63 - IEEE754_DOUBLE_MANTISSA_SIZE)) + bias;
  const int64_t absolute = unscaled >> shift;
  const int64_t value = (key < bias) ? -absolute : absolute;
  assert(key == mdbx_key_from_jsonInteger(value) ||
         (mdbx_key_from_jsonInteger(value - 1) < key && key < mdbx_key_from_jsonInteger(value + 1)));
  return value;
}

double mdbx_double_from_key(const MDBX_val v) {
  assert(v.iov_len == 8);
  return key2double(unaligned_peek_u64(2, v.iov_base));
}

float mdbx_float_from_key(const MDBX_val v) {
  assert(v.iov_len == 4);
  return key2float(unaligned_peek_u32(2, v.iov_base));
}

int32_t mdbx_int32_from_key(const MDBX_val v) {
  assert(v.iov_len == 4);
  return (int32_t)(unaligned_peek_u32(2, v.iov_base) - UINT32_C(0x80000000));
}

int64_t mdbx_int64_from_key(const MDBX_val v) {
  assert(v.iov_len == 8);
  return (int64_t)(unaligned_peek_u64(2, v.iov_base) - UINT64_C(0x8000000000000000));
}
