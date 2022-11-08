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

#pragma once
#include "base.h++"

#if !defined(__BYTE_ORDER__) || !defined(__ORDER_LITTLE_ENDIAN__) ||           \
    !defined(__ORDER_BIG_ENDIAN__)
#error __BYTE_ORDER__ should be defined.
#endif

#if __BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__ &&                               \
    __BYTE_ORDER__ != __ORDER_BIG_ENDIAN__
#error Unsupported byte order.
#endif

#if __GNUC_PREREQ(4, 4) || defined(__clang__)
#ifndef bswap64
#define bswap64(v) __builtin_bswap64(v)
#endif
#ifndef bswap32
#define bswap32(v) __builtin_bswap32(v)
#endif
#if (__GNUC_PREREQ(4, 8) || __has_builtin(__builtin_bswap16)) &&               \
    !defined(bswap16)
#define bswap16(v) __builtin_bswap16(v)
#endif

#elif defined(_MSC_VER)

#if _MSC_FULL_VER < 190024215
#pragma message(                                                               \
    "It is recommended to use Visual Studio 2015 (MSC 19.0) or newer.")
#endif

#define bswap64(v) _byteswap_uint64(v)
#define bswap32(v) _byteswap_ulong(v)
#define bswap16(v) _byteswap_ushort(v)
#define rot64(v, s) _rotr64(v, s)
#define rot32(v, s) _rotr(v, s)

#if defined(_M_X64) || defined(_M_IA64)
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
  if (MDBX_UNALIGNED_OK >= sizeof(T))
    return *(const T *)ptr;
  else {
#if defined(__unaligned) || defined(_M_ARM) || defined(_M_ARM64) ||            \
    defined(_M_X64) || defined(_M_IA64)
    return *(const T __unaligned *)ptr;
#else
    T local;
    memcpy(&local, (const T *)ptr, sizeof(T));
    return local;
#endif /* _MSC_VER || __unaligned */
  }
}

template <typename T> static __inline void store(void *ptr, const T &value) {
  if (MDBX_UNALIGNED_OK >= sizeof(T))
    *(T *)ptr = value;
  else {
#if defined(__unaligned) || defined(_M_ARM) || defined(_M_ARM64) ||            \
    defined(_M_X64) || defined(_M_IA64)
    *((T __unaligned *)ptr) = value;
#else
    memcpy(ptr, &value, sizeof(T));
#endif /* _MSC_VER || __unaligned */
  }
}

} /* namespace unaligned */

//-----------------------------------------------------------------------------

#ifndef rot64
static __inline uint64_t rot64(uint64_t v, unsigned s) {
  return (v >> s) | (v << (64 - s));
}
#endif /* rot64 */

static __inline bool is_power2(size_t x) { return (x & (x - 1)) == 0; }

#undef roundup2
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
#elif defined(__ia32__)
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
#if defined(__ia32__)
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
  uint64_t value{0};

  simple_checksum() = default;

  void push(const uint32_t &data) {
    value += data * UINT64_C(9386433910765580089) + 1;
    value ^= value >> 41;
    value *= UINT64_C(0xBD9CACC22C6E9571);
  }

  void push(const uint64_t &data) {
    push((uint32_t)data);
    push((uint32_t)(data >> 32));
  }

  void push(const bool data) {
    push(data ? UINT32_C(0x780E) : UINT32_C(0xFA18E));
  }

  void push(const void *ptr, size_t bytes) {
    const uint8_t *data = (const uint8_t *)ptr;
    for (size_t i = 0; i < bytes; ++i)
      push((uint32_t)data[i]);
  }

  void push(const double &data) { push(&data, sizeof(double)); }
  void push(const char *cstr) { push(cstr, strlen(cstr)); }
  void push(const std::string &str) { push(str.data(), str.size()); }

  void push(unsigned salt, const MDBX_val &val) {
    push(unsigned(val.iov_len));
    push(salt);
    push(val.iov_base, val.iov_len);
  }

#if defined(_WIN32) || defined(_WIN64) || defined(_WINDOWS)
  void push(const HANDLE &handle) { push(&handle, sizeof(handle)); }
#endif /* _WINDOWS */
};

std::string data2hex(const void *ptr, size_t bytes, simple_checksum &checksum);
bool hex2data(const char *hex_begin, const char *hex_end, void *ptr,
              size_t bytes, simple_checksum &checksum);
bool is_samedata(const MDBX_val *a, const MDBX_val *b);
inline bool is_samedata(const MDBX_val &a, const MDBX_val &b) {
  return is_samedata(&a, &b);
}
std::string format(const char *fmt, ...);

static inline uint64_t bleach64(uint64_t v) {
  // Tommy Ettinger, https://www.blogger.com/profile/04953541827437796598
  // http://mostlymangling.blogspot.com/2019/01/better-stronger-mixer-and-test-procedure.html
  v ^= rot64(v, 25) ^ rot64(v, 50);
  v *= UINT64_C(0xA24BAED4963EE407);
  v ^= rot64(v, 24) ^ rot64(v, 49);
  v *= UINT64_C(0x9FB21C651E98DF25);
  return v ^ v >> 28;
}

static inline uint32_t bleach32(uint32_t x) {
  // https://github.com/skeeto/hash-prospector
  // exact bias: 0.17353355999581582
  x ^= x >> 16;
  x *= UINT32_C(0x7feb352d);
  x ^= 0x3027C563 ^ (x >> 15);
  x *= UINT32_C(0x846ca68b);
  x ^= x >> 16;
  return x;
}

static inline uint64_t prng64_map1_careless(uint64_t state) {
  return state * UINT64_C(6364136223846793005) + 1;
}

static inline uint64_t prng64_map2_careless(uint64_t state) {
  return (state + UINT64_C(1442695040888963407)) *
         UINT64_C(6364136223846793005);
}

static inline uint64_t prng64_map1_white(uint64_t state) {
  return bleach64(prng64_map1_careless(state));
}

static inline uint64_t prng64_map2_white(uint64_t state) {
  return bleach64(prng64_map2_careless(state));
}

static inline uint64_t prng64_careless(uint64_t &state) {
  state = prng64_map1_careless(state);
  return state;
}

static inline double u64_to_double1(uint64_t v) {
  union {
    uint64_t u64;
    double d;
  } casting;

  casting.u64 = UINT64_C(0x3ff) << 52 | (v >> 12);
  assert(casting.d >= 1.0 && casting.d < 2.0);
  return casting.d - 1.0;
}

uint64_t prng64_white(uint64_t &state);
uint32_t prng32(uint64_t &state);
void prng_fill(uint64_t &state, void *ptr, size_t bytes);

void prng_seed(uint64_t seed);
uint32_t prng32(void);
uint64_t prng64(void);
void prng_fill(void *ptr, size_t bytes);

bool flipcoin();
bool flipcoin_x2();
bool flipcoin_x3();
bool flipcoin_x4();
bool flipcoin_n(unsigned n);
bool jitter(unsigned probability_percent);
void jitter_delay(bool extra = false);
