/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025

#include "internals.h"

MDBX_NOTHROW_CONST_FUNCTION MDBX_MAYBE_UNUSED unsigned ceil_log2n(size_t value_uintptr) {
  assert(value_uintptr > 0 && value_uintptr < INT32_MAX);
  value_uintptr -= 1;
  value_uintptr |= value_uintptr >> 1;
  value_uintptr |= value_uintptr >> 2;
  value_uintptr |= value_uintptr >> 4;
  value_uintptr |= value_uintptr >> 8;
  value_uintptr |= value_uintptr >> 16;
  return log2n_powerof2(value_uintptr + 1);
}

MDBX_MAYBE_UNUSED MDBX_NOTHROW_CONST_FUNCTION unsigned log2n_powerof2(size_t value_uintptr) {
  assert(value_uintptr > 0 && value_uintptr < INT32_MAX && is_powerof2(value_uintptr));
  assert((value_uintptr & -(intptr_t)value_uintptr) == value_uintptr);
  const uint32_t value_uint32 = (uint32_t)value_uintptr;
#if __GNUC_PREREQ(4, 1) || __has_builtin(__builtin_ctz)
  STATIC_ASSERT(sizeof(value_uint32) <= sizeof(unsigned));
  return __builtin_ctz(value_uint32);
#elif defined(_MSC_VER)
  unsigned long index;
  STATIC_ASSERT(sizeof(value_uint32) <= sizeof(long));
  _BitScanForward(&index, value_uint32);
  return index;
#else
  static const uint8_t debruijn_ctz32[32] = {0,  1,  28, 2,  29, 14, 24, 3, 30, 22, 20, 15, 25, 17, 4,  8,
                                             31, 27, 13, 23, 21, 19, 16, 7, 26, 12, 18, 6,  11, 5,  10, 9};
  return debruijn_ctz32[(uint32_t)(value_uint32 * 0x077CB531ul) >> 27];
#endif
}

MDBX_NOTHROW_CONST_FUNCTION uint64_t rrxmrrxmsx_0(uint64_t v) {
  /* Pelle Evensen's mixer, https://bit.ly/2HOfynt */
  v ^= (v << 39 | v >> 25) ^ (v << 14 | v >> 50);
  v *= UINT64_C(0xA24BAED4963EE407);
  v ^= (v << 40 | v >> 24) ^ (v << 15 | v >> 49);
  v *= UINT64_C(0x9FB21C651E98DF25);
  return v ^ v >> 28;
}

__cold char *ratio2digits(const uint64_t v, const uint64_t d, ratio2digits_buffer_t *const buffer, int precision) {
  assert(d > 0 && precision < 20);
  char *const dot = buffer->string + 21;
  uint64_t i = v / d, f = v % d, m = d;

  char *tail = dot;
  bool carry = m - f < m / 2;
  if (precision > 0) {
    *tail = '.';
    do {
      while (unlikely(f > UINT64_MAX / 10)) {
        f >>= 1;
        m >>= 1;
      }
      f *= 10;
      assert(tail > buffer->string && tail < ARRAY_END(buffer->string) - 1);
      *++tail = '0' + (char)(f / m);
      f %= m;
    } while (--precision && tail < ARRAY_END(buffer->string) - 1);

    carry = m - f < m / 2;
    for (char *scan = tail; carry && scan > dot; --scan)
      *scan = (carry = *scan == '9') ? '0' : *scan + 1;
  }
  assert(tail > buffer->string && tail < ARRAY_END(buffer->string) - 1);
  *++tail = '\0';

  char *head = dot;
  i += carry;
  while (i > 9) {
    assert(head > buffer->string && head < ARRAY_END(buffer->string));
    *--head = '0' + (char)(i % 10);
    i /= 10;
  }
  assert(head > buffer->string && head < ARRAY_END(buffer->string));
  *--head = '0' + (char)i;

  return head;
}

__cold char *ratio2percent(uint64_t value, uint64_t whole, ratio2digits_buffer_t *buffer) {
  while (unlikely(value > UINT64_MAX / 100)) {
    value >>= 1;
    whole >>= 1;
  }
  const bool rough = whole >= value && (!value || value > whole / 16);
  return ratio2digits(value * 100, whole, buffer, rough ? 1 : 2);
}
