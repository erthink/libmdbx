/*
 * Copyright 2017-2018 Leonid Yuriev <leo@yuriev.ru>
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
#include <float.h>
#if defined(HAVE_IEEE754_H) || __has_include(<ieee754.h>)
#include <ieee754.h>
#endif

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
  int actual = vsnprintf((char *)result.data(), result.capacity(), fmt, ones);
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

//-----------------------------------------------------------------------------

/* TODO: replace my 'libmera' fomr t1ha. */
uint64_t entropy_ticks(void) {
#if defined(EMSCRIPTEN)
  return (uint64_t)emscripten_get_now();
#endif /* EMSCRIPTEN */

#if defined(__APPLE__) || defined(__MACH__)
  return mach_absolute_time();
#endif /* defined(__APPLE__) || defined(__MACH__) */

#if defined(__sun__) || defined(__sun)
  return gethrtime();
#endif /* __sun__ */

#if defined(__GNUC__) || defined(__clang__)

#if defined(__ia64__)
  uint64_t ticks;
  __asm __volatile("mov %0=ar.itc" : "=r"(ticks));
  return ticks;
#elif defined(__hppa__)
  uint64_t ticks;
  __asm __volatile("mfctl 16, %0" : "=r"(ticks));
  return ticks;
#elif defined(__s390__)
  uint64_t ticks;
  __asm __volatile("stck 0(%0)" : : "a"(&(ticks)) : "memory", "cc");
  return ticks;
#elif defined(__alpha__)
  uint64_t ticks;
  __asm __volatile("rpcc %0" : "=r"(ticks));
  return ticks;
#elif defined(__sparc__) || defined(__sparc) || defined(__sparc64__) ||        \
    defined(__sparc64) || defined(__sparc_v8plus__) ||                         \
    defined(__sparc_v8plus) || defined(__sparc_v8plusa__) ||                   \
    defined(__sparc_v8plusa) || defined(__sparc_v9__) || defined(__sparc_v9)

  union {
    uint64_t u64;
    struct {
#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
      uint32_t h, l;
#else
      uint32_t l, h;
#endif
    } u32;
  } cycles;

#if defined(__sparc_v8plus__) || defined(__sparc_v8plusa__) ||                 \
    defined(__sparc_v9__) || defined(__sparc_v8plus) ||                        \
    defined(__sparc_v8plusa) || defined(__sparc_v9)

#if UINTPTR_MAX > 0xffffFFFFul || ULONG_MAX > 0xffffFFFFul ||                  \
    defined(__sparc64__) || defined(__sparc64)
  __asm __volatile("rd %%tick, %0" : "=r"(cycles.u64));
#else
  __asm __volatile("rd %%tick, %1; srlx %1, 32, %0"
                   : "=r"(cycles.u32.h), "=r"(cycles.u32.l));
#endif /* __sparc64__ */

#else
  __asm __volatile(".byte 0x83, 0x41, 0x00, 0x00; mov %%g1, %0"
                   : "=r"(cycles.u64)
                   :
                   : "%g1");
#endif /* __sparc8plus__ || __sparc_v9__ */
  return cycles.u64;

#elif (defined(__powerpc64__) || defined(__ppc64__) || defined(__ppc64) ||     \
       defined(__powerpc64))
  uint64_t ticks;
  __asm __volatile("mfspr %0, 268" : "=r"(ticks));
  return ticks;
#elif (defined(__powerpc__) || defined(__ppc__) || defined(__powerpc) ||       \
       defined(__ppc))
#if UINTPTR_MAX > 0xffffFFFFul || ULONG_MAX > 0xffffFFFFul
  uint64_t ticks;
  __asm __volatile("mftb  %0" : "=r"(ticks));
  *now = ticks;
#else
  uint64_t ticks;
  uint32_t low, high_before, high_after;
  __asm __volatile("mftbu %0; mftb  %1; mftbu %2"
                   : "=r"(high_before), "=r"(low), "=r"(high_after));
  ticks = (uint64_t)high_after << 32;
  ticks |= low & /* zeroes if high part has changed */
           ~(high_before - high_after);
#endif
#elif defined(__aarch64__) || (defined(__ARM_ARCH) && __ARM_ARCH > 7)
  uint64_t virtual_timer;
  __asm __volatile("mrs %0, cntvct_el0" : "=r"(virtual_timer));
  return virtual_timer;
#elif defined(__ARM_ARCH) && __ARM_ARCH > 5 && __ARM_ARCH < 8
  unsigned long pmccntr;
  __asm __volatile("mrc p15, 0, %0, c9, c13, 0" : "=r"(pmccntr));
  return pmccntr;
#elif defined(__mips__) || defined(__mips) || defined(_R4000)
  unsigned count;
  __asm __volatile("rdhwr %0, $2" : "=r"(count));
  return count;
#endif /* arch selector */
#endif /* __GNUC__ || __clang__ */

#if defined(__e2k__) || defined(__ia32__)
  return __rdtsc();
#elif defined(_M_ARM)
  return __rdpmccntr64();
#elif defined(_WIN32) || defined(_WIN64) || defined(_WINDOWS)
  LARGE_INTEGER PerformanceCount;
  if (QueryPerformanceCounter(&PerformanceCount))
    return PerformanceCount.QuadPart;
  return GetTickCount64();
#else
  struct timespec ts;
#if defined(CLOCK_MONOTONIC_COARSE)
  clockid_t clock = CLOCK_MONOTONIC_COARSE;
#elif defined(CLOCK_MONOTONIC_RAW)
  clockid_t clock = CLOCK_MONOTONIC_RAW;
#else
  clockid_t clock = CLOCK_MONOTONIC;
#endif
  int rc = clock_gettime(clock, &ts);
  if (unlikely(rc))
    failure_perror("clock_gettime()", rc);

  return (((uint64_t)ts.tv_sec) << 32) + ts.tv_nsec;
#endif
}

//-----------------------------------------------------------------------------

static __inline uint64_t bleach64(uint64_t dirty) {
  return mul_64x64_high(bswap64(dirty), UINT64_C(17048867929148541611));
}

static __inline uint32_t bleach32(uint32_t dirty) {
  return (uint32_t)((bswap32(dirty) * UINT64_C(2175734609)) >> 32);
}

uint64_t prng64_careless(uint64_t &state) {
  state = state * UINT64_C(6364136223846793005) + 1;
  return state;
}

uint64_t prng64_white(uint64_t &state) {
  state = state * UINT64_C(6364136223846793005) + UINT64_C(1442695040888963407);
  return bleach64(state);
}

uint32_t prng32(uint64_t &state) {
  return (uint32_t)(prng64_careless(state) >> 32);
}

void prng_fill(uint64_t &state, void *ptr, size_t bytes) {
  while (bytes >= 4) {
    *((uint32_t *)ptr) = prng32(state);
    ptr = (uint32_t *)ptr + 1;
    bytes -= 4;
  }

  switch (bytes & 3) {
  case 3: {
    uint32_t u32 = prng32(state);
    memcpy(ptr, &u32, 3);
  } break;
  case 2:
    *((uint16_t *)ptr) = (uint16_t)prng32(state);
    break;
  case 1:
    *((uint8_t *)ptr) = (uint8_t)prng32(state);
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

uint64_t entropy_white() { return bleach64(entropy_ticks()); }

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
  r.ieee.mantissa0 = (unsigned)(salt >> 32);
  r.ieee.mantissa1 = (unsigned)salt;
  return r.d;
#else
  const uint64_t top = (UINT64_C(1) << DBL_MANT_DIG) - 1;
  const double scale = 1.0 / (double)top;
  return (salt >> (64 - DBL_MANT_DIG)) * scale;
#endif
}

bool flipcoin() { return bleach32((uint32_t)entropy_ticks()) & 1; }

bool jitter(unsigned probability_percent) {
  const uint32_t top = UINT32_MAX - UINT32_MAX % 100;
  uint32_t dice, edge = (top) / 100 * probability_percent;
  do
    dice = bleach32((uint32_t)entropy_ticks());
  while (dice >= top);
  return dice < edge;
}

void jitter_delay(bool extra) {
  unsigned dice = entropy_white() & 3;
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
          unsigned us = entropy_white() &
                        (extra ? 0xfffff /* 1.05 s */ : 0x3ff /* 1 ms */);
          log_trace("== jitter.delay: %0.6f", us / 1000000.0);
          osal_udelay(us);
        }
      }
    } while (flipcoin());
    log_trace("<< jitter.delay: dice %u", dice);
  }
}
