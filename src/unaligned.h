/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025

#pragma once

/*------------------------------------------------------------------------------
 * Unaligned access */

MDBX_NOTHROW_CONST_FUNCTION MDBX_MAYBE_UNUSED static inline size_t field_alignment(size_t alignment_baseline,
                                                                                   size_t field_offset) {
  size_t merge = alignment_baseline | (size_t)field_offset;
  return merge & -(int)merge;
}

/* read-thunk for UB-sanitizer */
MDBX_NOTHROW_PURE_FUNCTION static inline uint8_t peek_u8(const uint8_t *__restrict ptr) { return *ptr; }

/* write-thunk for UB-sanitizer */
static inline void poke_u8(uint8_t *__restrict ptr, const uint8_t v) { *ptr = v; }

static inline void *bcopy_2(void *__restrict dst, const void *__restrict src) {
  uint8_t *__restrict d = (uint8_t *)dst;
  const uint8_t *__restrict s = (uint8_t *)src;
  d[0] = s[0];
  d[1] = s[1];
  return d;
}

static inline void *bcopy_4(void *const __restrict dst, const void *const __restrict src) {
  uint8_t *__restrict d = (uint8_t *)dst;
  const uint8_t *__restrict s = (uint8_t *)src;
  d[0] = s[0];
  d[1] = s[1];
  d[2] = s[2];
  d[3] = s[3];
  return d;
}

static inline void *bcopy_8(void *const __restrict dst, const void *const __restrict src) {
  uint8_t *__restrict d = (uint8_t *)dst;
  const uint8_t *__restrict s = (uint8_t *)src;
  d[0] = s[0];
  d[1] = s[1];
  d[2] = s[2];
  d[3] = s[3];
  d[4] = s[4];
  d[5] = s[5];
  d[6] = s[6];
  d[7] = s[7];
  return d;
}

MDBX_NOTHROW_PURE_FUNCTION static inline uint16_t unaligned_peek_u16(const size_t expected_alignment,
                                                                     const void *const ptr) {
  assert((uintptr_t)ptr % expected_alignment == 0);
  if (MDBX_UNALIGNED_OK >= 2 || (expected_alignment % sizeof(uint16_t)) == 0)
    return *(const uint16_t *)ptr;
  else {
#if defined(__unaligned) || defined(_M_ARM) || defined(_M_ARM64) || defined(_M_X64) || defined(_M_IA64)
    return *(const __unaligned uint16_t *)ptr;
#else
    uint16_t v;
    bcopy_2((uint8_t *)&v, (const uint8_t *)ptr);
    return v;
#endif /* _MSC_VER || __unaligned */
  }
}

static inline void unaligned_poke_u16(const size_t expected_alignment, void *const __restrict ptr, const uint16_t v) {
  assert((uintptr_t)ptr % expected_alignment == 0);
  if (MDBX_UNALIGNED_OK >= 2 || (expected_alignment % sizeof(v)) == 0)
    *(uint16_t *)ptr = v;
  else {
#if defined(__unaligned) || defined(_M_ARM) || defined(_M_ARM64) || defined(_M_X64) || defined(_M_IA64)
    *((uint16_t __unaligned *)ptr) = v;
#else
    bcopy_2((uint8_t *)ptr, (const uint8_t *)&v);
#endif /* _MSC_VER || __unaligned */
  }
}

MDBX_NOTHROW_PURE_FUNCTION static inline uint32_t unaligned_peek_u32(const size_t expected_alignment,
                                                                     const void *const __restrict ptr) {
  assert((uintptr_t)ptr % expected_alignment == 0);
  if (MDBX_UNALIGNED_OK >= 4 || (expected_alignment % sizeof(uint32_t)) == 0)
    return *(const uint32_t *)ptr;
  else if ((expected_alignment % sizeof(uint16_t)) == 0) {
    const uint16_t lo = ((const uint16_t *)ptr)[__BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__];
    const uint16_t hi = ((const uint16_t *)ptr)[__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__];
    return lo | (uint32_t)hi << 16;
  } else {
#if defined(__unaligned) || defined(_M_ARM) || defined(_M_ARM64) || defined(_M_X64) || defined(_M_IA64)
    return *(const __unaligned uint32_t *)ptr;
#else
    uint32_t v;
    bcopy_4((uint8_t *)&v, (const uint8_t *)ptr);
    return v;
#endif /* _MSC_VER || __unaligned */
  }
}

static inline void unaligned_poke_u32(const size_t expected_alignment, void *const __restrict ptr, const uint32_t v) {
  assert((uintptr_t)ptr % expected_alignment == 0);
  if (MDBX_UNALIGNED_OK >= 4 || (expected_alignment % sizeof(v)) == 0)
    *(uint32_t *)ptr = v;
  else if ((expected_alignment % sizeof(uint16_t)) == 0) {
    ((uint16_t *)ptr)[__BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__] = (uint16_t)v;
    ((uint16_t *)ptr)[__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__] = (uint16_t)(v >> 16);
  } else {
#if defined(__unaligned) || defined(_M_ARM) || defined(_M_ARM64) || defined(_M_X64) || defined(_M_IA64)
    *((uint32_t __unaligned *)ptr) = v;
#else
    bcopy_4((uint8_t *)ptr, (const uint8_t *)&v);
#endif /* _MSC_VER || __unaligned */
  }
}

MDBX_NOTHROW_PURE_FUNCTION static inline uint64_t unaligned_peek_u64(const size_t expected_alignment,
                                                                     const void *const __restrict ptr) {
  assert((uintptr_t)ptr % expected_alignment == 0);
  if (MDBX_UNALIGNED_OK >= 8 || (expected_alignment % sizeof(uint64_t)) == 0)
    return *(const uint64_t *)ptr;
  else if ((expected_alignment % sizeof(uint32_t)) == 0) {
    const uint32_t lo = ((const uint32_t *)ptr)[__BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__];
    const uint32_t hi = ((const uint32_t *)ptr)[__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__];
    return lo | (uint64_t)hi << 32;
  } else {
#if defined(__unaligned) || defined(_M_ARM) || defined(_M_ARM64) || defined(_M_X64) || defined(_M_IA64)
    return *(const __unaligned uint64_t *)ptr;
#else
    uint64_t v;
    bcopy_8((uint8_t *)&v, (const uint8_t *)ptr);
    return v;
#endif /* _MSC_VER || __unaligned */
  }
}

static inline uint64_t unaligned_peek_u64_volatile(const size_t expected_alignment,
                                                   const volatile void *const __restrict ptr) {
  assert((uintptr_t)ptr % expected_alignment == 0);
  assert(expected_alignment % sizeof(uint32_t) == 0);
  if (MDBX_UNALIGNED_OK >= 8 || (expected_alignment % sizeof(uint64_t)) == 0)
    return *(const volatile uint64_t *)ptr;
  else {
#if defined(__unaligned) || defined(_M_ARM) || defined(_M_ARM64) || defined(_M_X64) || defined(_M_IA64)
    return *(const volatile __unaligned uint64_t *)ptr;
#else
    const uint32_t lo = ((const volatile uint32_t *)ptr)[__BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__];
    const uint32_t hi = ((const volatile uint32_t *)ptr)[__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__];
    return lo | (uint64_t)hi << 32;
#endif /* _MSC_VER || __unaligned */
  }
}

static inline void unaligned_poke_u64(const size_t expected_alignment, void *const __restrict ptr, const uint64_t v) {
  assert((uintptr_t)ptr % expected_alignment == 0);
  if (MDBX_UNALIGNED_OK >= 8 || (expected_alignment % sizeof(v)) == 0)
    *(uint64_t *)ptr = v;
  else if ((expected_alignment % sizeof(uint32_t)) == 0) {
    ((uint32_t *)ptr)[__BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__] = (uint32_t)v;
    ((uint32_t *)ptr)[__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__] = (uint32_t)(v >> 32);
  } else {
#if defined(__unaligned) || defined(_M_ARM) || defined(_M_ARM64) || defined(_M_X64) || defined(_M_IA64)
    *((uint64_t __unaligned *)ptr) = v;
#else
    bcopy_8((uint8_t *)ptr, (const uint8_t *)&v);
#endif /* _MSC_VER || __unaligned */
  }
}

#define UNALIGNED_PEEK_8(ptr, struct, field) peek_u8(ptr_disp(ptr, offsetof(struct, field)))
#define UNALIGNED_POKE_8(ptr, struct, field, value) poke_u8(ptr_disp(ptr, offsetof(struct, field)), value)

#define UNALIGNED_PEEK_16(ptr, struct, field) unaligned_peek_u16(1, ptr_disp(ptr, offsetof(struct, field)))
#define UNALIGNED_POKE_16(ptr, struct, field, value)                                                                   \
  unaligned_poke_u16(1, ptr_disp(ptr, offsetof(struct, field)), value)

#define UNALIGNED_PEEK_32(ptr, struct, field) unaligned_peek_u32(1, ptr_disp(ptr, offsetof(struct, field)))
#define UNALIGNED_POKE_32(ptr, struct, field, value)                                                                   \
  unaligned_poke_u32(1, ptr_disp(ptr, offsetof(struct, field)), value)

#define UNALIGNED_PEEK_64(ptr, struct, field) unaligned_peek_u64(1, ptr_disp(ptr, offsetof(struct, field)))
#define UNALIGNED_POKE_64(ptr, struct, field, value)                                                                   \
  unaligned_poke_u64(1, ptr_disp(ptr, offsetof(struct, field)), value)

MDBX_NOTHROW_PURE_FUNCTION static inline pgno_t peek_pgno(const void *const __restrict ptr) {
  if (sizeof(pgno_t) == sizeof(uint32_t))
    return (pgno_t)unaligned_peek_u32(1, ptr);
  else if (sizeof(pgno_t) == sizeof(uint64_t))
    return (pgno_t)unaligned_peek_u64(1, ptr);
  else {
    pgno_t pgno;
    memcpy(&pgno, ptr, sizeof(pgno));
    return pgno;
  }
}

static inline void poke_pgno(void *const __restrict ptr, const pgno_t pgno) {
  if (sizeof(pgno) == sizeof(uint32_t))
    unaligned_poke_u32(1, ptr, pgno);
  else if (sizeof(pgno) == sizeof(uint64_t))
    unaligned_poke_u64(1, ptr, pgno);
  else
    memcpy(ptr, &pgno, sizeof(pgno));
}
