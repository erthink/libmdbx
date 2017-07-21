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

#pragma once
#include "base.h"

#if !defined(__BYTE_ORDER__) || !defined(__ORDER_LITTLE_ENDIAN__) ||           \
    !defined(__ORDER_BIG_ENDIAN__)
#ifndef _MSC_VER
#include <sys/param.h> /* for endianness */
#endif
#if defined(__BYTE_ORDER) && defined(__LITTLE_ENDIAN) && defined(__BIG_ENDIAN)
#define __ORDER_LITTLE_ENDIAN__ __LITTLE_ENDIAN
#define __ORDER_BIG_ENDIAN__ __BIG_ENDIAN
#define __BYTE_ORDER__ __BYTE_ORDER
#else
#define __ORDER_LITTLE_ENDIAN__ 1234
#define __ORDER_BIG_ENDIAN__ 4321
#if defined(__LITTLE_ENDIAN__) || defined(_LITTLE_ENDIAN) ||                   \
    defined(__ARMEL__) || defined(__THUMBEL__) || defined(__AARCH64EL__) ||    \
    defined(__MIPSEL__) || defined(_MIPSEL) || defined(__MIPSEL) ||            \
    defined(__i386) || defined(__x86_64__) || defined(_M_IX86) ||              \
    defined(_M_X64) || defined(i386) || defined(_X86_) || defined(__i386__) || \
    defined(_X86_64_) || defined(_M_ARM) || defined(_M_ARM64) ||               \
    defined(__e2k__)
#define __BYTE_ORDER__ __ORDER_LITTLE_ENDIAN__
#elif defined(__BIG_ENDIAN__) || defined(_BIG_ENDIAN) || defined(__ARMEB__) || \
    defined(__THUMBEB__) || defined(__AARCH64EB__) || defined(__MIPSEB__) ||   \
    defined(_MIPSEB) || defined(__MIPSEB) || defined(_M_IA64)
#define __BYTE_ORDER__ __ORDER_BIG_ENDIAN__
#else
#error __BYTE_ORDER__ should be defined.
#endif
#endif
#endif

#if __BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__ &&                               \
    __BYTE_ORDER__ != __ORDER_BIG_ENDIAN__
#error Unsupported byte order.
#endif

#if __GNUC_PREREQ(4, 4) || defined(__clang__)
#if __GNUC_PREREQ(4, 5) || defined(__clang__)
#define unreachable() __builtin_unreachable()
#endif
#define bswap64(v) __builtin_bswap64(v)
#define bswap32(v) __builtin_bswap32(v)
#if __GNUC_PREREQ(4, 8) || __has_builtin(__builtin_bswap16)
#define bswap16(v) __builtin_bswap16(v)
#endif

#elif defined(_MSC_VER)

#if _MSC_FULL_VER < 190024215
#pragma message(                                                               \
    "It is recommended to use Visual Studio 2015 (MSC 19.0) or newer.")
#endif

#define unreachable() __assume(0)
#define bswap64(v) _byteswap_uint64(v)
#define bswap32(v) _byteswap_ulong(v)
#define bswap16(v) _byteswap_ushort(v)
#define rot64(v, s) _rotr64(v, s)
#define rot32(v, s) _rotr(v, s)

#if defined(_M_ARM64) || defined(_M_X64) || defined(_M_IA64)
#pragma intrinsic(_umul128)
#define mul_64x64_128(a, b, ph) _umul128(a, b, ph)
#pragma intrinsic(__umulh)
#define mul_64x64_high(a, b) __umulh(a, b)
#endif

#if defined(_M_IX86)
#pragma intrinsic(__emulu)
#define mul_32x32_64(a, b) __emulu(a, b)
#elif defined(_M_ARM)
#define mul_32x32_64(a, b) _arm_umull(a, b)
#endif

#endif /* compiler */

#ifndef unreachable
#define unreachable()                                                          \
  do {                                                                         \
  } while (1)
#endif

#ifndef bswap64
#ifdef __bswap_64
#define bswap64(v) __bswap_64(v)
#else
static __inline uint64_t bswap64(uint64_t v) {
  return v << 56 | v >> 56 | ((v << 40) & UINT64_C(0x00ff000000000000)) |
         ((v << 24) & UINT64_C(0x0000ff0000000000)) |
         ((v << 8) & UINT64_C(0x000000ff00000000)) |
         ((v >> 8) & UINT64_C(0x00000000ff0000000)) |
         ((v >> 24) & UINT64_C(0x0000000000ff0000)) |
         ((v >> 40) & UINT64_C(0x000000000000ff00));
}
#endif
#endif /* bswap64 */

#ifndef bswap32
#ifdef __bswap_32
#define bswap32(v) __bswap_32(v)
#else
static __inline uint32_t bswap32(uint32_t v) {
  return v << 24 | v >> 24 | ((v << 8) & UINT32_C(0x00ff0000)) |
         ((v >> 8) & UINT32_C(0x0000ff00));
}
#endif
#endif /* bswap32 */

#ifndef bswap16
#ifdef __bswap_16
#define bswap16(v) __bswap_16(v)
#else
static __inline uint16_t bswap16(uint16_t v) { return v << 8 | v >> 8; }
#endif
#endif /* bswap16 */

#define is_byteorder_le() (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
#define is_byteorder_be() (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)

#ifndef htole16
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#define htobe16(v) bswap16(v)
#define htole16(v) (v)
#define be16toh(v) bswap16(v)
#define le16toh(v) (v)
#else
#define htobe16(v) (v)
#define htole16(v) bswap16(v)
#define be16toh(v) (v)
#define le16toh(v) bswap16(v)
#endif
#endif /* htole16 */

#ifndef htole32
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#define htobe32(v) bswap32(v)
#define htole32(v) (v)
#define be32toh(v) bswap32(v)
#define le32toh(v) (v)
#else
#define htobe32(v) (v)
#define htole32(v) bswap32(v)
#define be32toh(v) (v)
#define le32toh(v) bswap32(v)
#endif
#endif /* htole32 */

#ifndef htole64
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#define htobe64(v) bswap64(v)
#define htole64(v) (v)
#define be64toh(v) bswap64(v)
#define le64toh(v) (v)
#else
#define htobe64(v) (v)
#define htole64(v) bswap_64(v)
#define be64toh(v) (v)
#define le64toh(v) bswap_64(v)
#endif
#endif /* htole64 */

namespace unaligned {

template <typename T> static __inline T load(const void *ptr) {
#if defined(_MSC_VER) &&                                                       \
    (defined(_M_ARM64) || defined(_M_X64) || defined(_M_IA64))
  return *(const T __unaligned *)ptr;
#elif UNALIGNED_OK
  return *(const T *)ptr;
#else
  T local;
#if defined(__GNUC__) || defined(__clang__)
  __builtin_memcpy(&local, (const T *)ptr, sizeof(T));
#else
  memcpy(&local, (const T *)ptr, sizeof(T));
#endif /* __GNUC__ || __clang__ */
  return local;
#endif /* UNALIGNED_OK */
}

template <typename T> static __inline void store(void *ptr, const T &value) {
#if defined(_MSC_VER) &&                                                       \
    (defined(_M_ARM64) || defined(_M_X64) || defined(_M_IA64))
  *((T __unaligned *)ptr) = value;
#elif UNALIGNED_OK
  *(volatile T *)ptr = value;
#else
#if defined(__GNUC__) || defined(__clang__)
  __builtin_memcpy(ptr, &value, sizeof(T));
#else
  memcpy(ptr, &value, sizeof(T));
#endif /* __GNUC__ || __clang__ */
#endif /* UNALIGNED_OK */
}

} /* namespace unaligned */

//-----------------------------------------------------------------------------

#ifndef rot64
static __inline uint64_t rot64(uint64_t v, unsigned s) {
  return (v >> s) | (v << (64 - s));
}
#endif /* rot64 */

#ifndef mul_32x32_64
static __inline uint64_t mul_32x32_64(uint32_t a, uint32_t b) {
  return a * (uint64_t)b;
}
#endif /* mul_32x32_64 */

#ifndef mul_64x64_128

static __inline unsigned add_with_carry(uint64_t *sum, uint64_t addend) {
  *sum += addend;
  return *sum < addend;
}

static __inline uint64_t mul_64x64_128(uint64_t a, uint64_t b, uint64_t *h) {
#if defined(__SIZEOF_INT128__) ||                                              \
    (defined(_INTEGRAL_MAX_BITS) && _INTEGRAL_MAX_BITS >= 128)
  __uint128_t r = (__uint128_t)a * (__uint128_t)b;
  /* modern GCC could nicely optimize this */
  *h = r >> 64;
  return r;
#elif defined(mul_64x64_high)
  *h = mul_64x64_high(a, b);
  return a * b;
#else
  /* performs 64x64 to 128 bit multiplication */
  uint64_t ll = mul_32x32_64((uint32_t)a, (uint32_t)b);
  uint64_t lh = mul_32x32_64(a >> 32, (uint32_t)b);
  uint64_t hl = mul_32x32_64((uint32_t)a, b >> 32);
  *h = mul_32x32_64(a >> 32, b >> 32) + (lh >> 32) + (hl >> 32) +
       add_with_carry(&ll, lh << 32) + add_with_carry(&ll, hl << 32);
  return ll;
#endif
}

#endif /* mul_64x64_128() */

#ifndef mul_64x64_high
static __inline uint64_t mul_64x64_high(uint64_t a, uint64_t b) {
  uint64_t h;
  mul_64x64_128(a, b, &h);
  return h;
}
#endif /* mul_64x64_high */

static __inline bool is_power2(size_t x) { return (x & (x - 1)) == 0; }

static __inline size_t roundup2(size_t value, size_t granularity) {
  assert(is_power2(granularity));
  return (value + granularity - 1) & ~(granularity - 1);
}

//-----------------------------------------------------------------------------

static __inline void memory_barrier(void) {
#if __has_extension(c_atomic) || __has_extension(cxx_atomic)
  __c11_atomic_thread_fence(__ATOMIC_SEQ_CST);
#elif defined(__ATOMIC_SEQ_CST)
  __atomic_thread_fence(__ATOMIC_SEQ_CST);
#elif defined(__clang__) || defined(__GNUC__)
  __sync_synchronize();
#elif defined(_MSC_VER)
  MemoryBarrier();
#elif defined(__INTEL_COMPILER) /* LY: Intel Compiler may mimic GCC and MSC */
#if defined(__ia64__) || defined(__ia64) || defined(_M_IA64)
  __mf();
#elif defined(__i386__) || defined(__x86_64__)
  _mm_mfence();
#else
#error "Unknown target for Intel Compiler, please report to us."
#endif
#elif defined(__SUNPRO_C) || defined(__sun) || defined(sun)
  __machine_rw_barrier();
#elif (defined(_HPUX_SOURCE) || defined(__hpux) || defined(__HP_aCC)) &&       \
    (defined(HP_IA64) || defined(__ia64))
  _Asm_mf();
#elif defined(_AIX) || defined(__ppc__) || defined(__powerpc__) ||             \
    defined(__ppc64__) || defined(__powerpc64__)
  __lwsync();
#else
#error "Could not guess the kind of compiler, please report to us."
#endif
}

static __inline void cpu_relax() {
#if defined(__i386__) || defined(__x86_64__) || defined(_M_IX86) ||            \
    defined(_M_X64)
  _mm_pause();
#elif defined(_WIN32) || defined(_WIN64) || defined(_WINDOWS) ||               \
    defined(YieldProcessor)
  YieldProcessor();
#else
/* nope */
#endif
}

//-----------------------------------------------------------------------------

struct simple_checksum {
  uint64_t value;

  simple_checksum() : value(0) {}

  void push(uint32_t data) {
    value += data * UINT64_C(9386433910765580089) + 1;
    value ^= value >> 41;
  }

  void push(uint64_t data) {
    push((uint32_t)data);
    push((uint32_t)(data >> 32));
  }

  void push(bool data) { push(data ? UINT32_C(0x780E) : UINT32_C(0xFA18E)); }

  void push(const void *ptr, size_t bytes) {
    const uint8_t *data = (const uint8_t *)ptr;
    for (size_t i = 0; i < bytes; ++i)
      push((uint32_t)data[i]);
  }

  void push(const double &data) { push(&data, sizeof(double)); }

  void push(const char *cstr) { push(cstr, strlen(cstr)); }

  void push(const std::string &str) { push(str.data(), str.size()); }

#if defined(_WIN32) || defined(_WIN64) || defined(_WINDOWS)
  void push(const HANDLE &handle) { push(&handle, sizeof(handle)); }
#endif /* _WINDOWS */
};

std::string data2hex(const void *ptr, size_t bytes, simple_checksum &checksum);
bool hex2data(const char *hex_begin, const char *hex_end, void *ptr,
              size_t bytes, simple_checksum &checksum);

std::string format(const char *fmt, ...);

uint64_t entropy_ticks(void);
uint64_t entropy_white(void);
uint64_t prng64_careless(uint64_t &state);
uint64_t prng64_white(uint64_t &state);
uint32_t prng32(uint64_t &state);
void prng_fill(uint64_t &state, void *ptr, size_t bytes);

void prng_seed(uint64_t seed);
uint32_t prng32(void);
uint64_t prng64(void);
void prng_fill(void *ptr, size_t bytes);

bool flipcoin();
bool jitter(unsigned probability_percent);
void jitter_delay(bool extra = false);
