/*
 * Copyright 2015-2022 Leonid Yuriev <leo@yuriev.ru>
 * and other libmdbx authors: please see AUTHORS file.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>. */

#pragma once
#ifdef MDBX_CONFIG_H
#include MDBX_CONFIG_H
#endif

#define LIBMDBX_INTERNALS
#ifdef xMDBX_TOOLS
#define MDBX_DEPRECATED
#endif /* xMDBX_TOOLS */

#ifdef xMDBX_ALLOY
/* Amalgamated build */
#define MDBX_INTERNAL_FUNC static
#define MDBX_INTERNAL_VAR static
#else
/* Non-amalgamated build */
#define MDBX_INTERNAL_FUNC
#define MDBX_INTERNAL_VAR extern
#endif /* xMDBX_ALLOY */

/*----------------------------------------------------------------------------*/

/** Disables using GNU/Linux libc extensions.
 * \ingroup build_option
 * \note This option couldn't be moved to the options.h since dependent
 * control macros/defined should be prepared before include the options.h */
#ifndef MDBX_DISABLE_GNU_SOURCE
#define MDBX_DISABLE_GNU_SOURCE 0
#endif
#if MDBX_DISABLE_GNU_SOURCE
#undef _GNU_SOURCE
#elif (defined(__linux__) || defined(__gnu_linux__)) && !defined(_GNU_SOURCE)
#define _GNU_SOURCE
#endif /* MDBX_DISABLE_GNU_SOURCE */

/* Should be defined before any includes */
#if !defined(_FILE_OFFSET_BITS) && !defined(__ANDROID_API__) &&                \
    !defined(ANDROID)
#define _FILE_OFFSET_BITS 64
#endif

#ifdef __APPLE__
#define _DARWIN_C_SOURCE
#endif

#ifdef _MSC_VER
#if _MSC_FULL_VER < 190024234
/* Actually libmdbx was not tested with compilers older than 19.00.24234 (Visual
 * Studio 2015 Update 3). But you could remove this #error and try to continue
 * at your own risk. In such case please don't rise up an issues related ONLY to
 * old compilers.
 *
 * NOTE:
 *   Unfortunately, there are several different builds of "Visual Studio" that
 *   are called "Visual Studio 2015 Update 3".
 *
 *   The 190024234 is used here because it is minimal version of Visual Studio
 *   that was used for build and testing libmdbx in recent years. Soon this
 *   value will be increased to 19.0.24241.7, since build and testing using
 *   "Visual Studio 2015" will be performed only at https://ci.appveyor.com.
 *
 *   Please ask Microsoft (but not us) for information about version differences
 *   and how to and where you can obtain the latest "Visual Studio 2015" build
 *   with all fixes.
 */
#error                                                                         \
    "At least \"Microsoft C/C++ Compiler\" version 19.00.24234 (Visual Studio 2015 Update 3) is required."
#endif
#ifndef _CRT_SECURE_NO_WARNINGS
#define _CRT_SECURE_NO_WARNINGS
#endif /* _CRT_SECURE_NO_WARNINGS */
#if _MSC_VER > 1800
#pragma warning(disable : 4464) /* relative include path contains '..' */
#endif
#if _MSC_VER > 1913
#pragma warning(disable : 5045) /* Compiler will insert Spectre mitigation...  \
                                 */
#endif
#if _MSC_VER > 1914
#pragma warning(                                                               \
    disable : 5105) /* winbase.h(9531): warning C5105: macro expansion         \
                       producing 'defined' has undefined behavior */
#endif
#pragma warning(disable : 4710) /* 'xyz': function not inlined */
#pragma warning(disable : 4711) /* function 'xyz' selected for automatic       \
                                   inline expansion */
#pragma warning(                                                               \
    disable : 4201) /* nonstandard extension used : nameless struct / union */
#pragma warning(disable : 4702) /* unreachable code */
#pragma warning(disable : 4706) /* assignment within conditional expression */
#pragma warning(disable : 4127) /* conditional expression is constant */
#pragma warning(disable : 4324) /* 'xyz': structure was padded due to          \
                                   alignment specifier */
#pragma warning(disable : 4310) /* cast truncates constant value */
#pragma warning(                                                               \
    disable : 4820) /* bytes padding added after data member for alignment */
#pragma warning(disable : 4548) /* expression before comma has no effect;      \
                                   expected expression with side - effect */
#pragma warning(disable : 4366) /* the result of the unary '&' operator may be \
                                   unaligned */
#pragma warning(disable : 4200) /* nonstandard extension used: zero-sized      \
                                   array in struct/union */
#pragma warning(disable : 4204) /* nonstandard extension used: non-constant    \
                                   aggregate initializer */
#pragma warning(                                                               \
    disable : 4505) /* unreferenced local function has been removed */
#endif              /* _MSC_VER (warnings) */

#if defined(__GNUC__) && __GNUC__ < 9
#pragma GCC diagnostic ignored "-Wattributes"
#endif /* GCC < 9 */

#if (defined(__MINGW__) || defined(__MINGW32__) || defined(__MINGW64__)) &&    \
    !defined(__USE_MINGW_ANSI_STDIO)
#define __USE_MINGW_ANSI_STDIO 1
#endif /* MinGW */

#include "../mdbx.h"
#include "base.h"

#if defined(__GNUC__) && !__GNUC_PREREQ(4, 2)
/* Actually libmdbx was not tested with compilers older than GCC 4.2.
 * But you could ignore this warning at your own risk.
 * In such case please don't rise up an issues related ONLY to old compilers.
 */
#warning "libmdbx required GCC >= 4.2"
#endif

#if defined(__clang__) && !__CLANG_PREREQ(3, 8)
/* Actually libmdbx was not tested with CLANG older than 3.8.
 * But you could ignore this warning at your own risk.
 * In such case please don't rise up an issues related ONLY to old compilers.
 */
#warning "libmdbx required CLANG >= 3.8"
#endif

#if defined(__GLIBC__) && !__GLIBC_PREREQ(2, 12)
/* Actually libmdbx was not tested with something older than glibc 2.12.
 * But you could ignore this warning at your own risk.
 * In such case please don't rise up an issues related ONLY to old systems.
 */
#warning "libmdbx was only tested with GLIBC >= 2.12."
#endif

#ifdef __SANITIZE_THREAD__
#warning                                                                       \
    "libmdbx don't compatible with ThreadSanitizer, you will get a lot of false-positive issues."
#endif /* __SANITIZE_THREAD__ */

#if __has_warning("-Wnested-anon-types")
#if defined(__clang__)
#pragma clang diagnostic ignored "-Wnested-anon-types"
#elif defined(__GNUC__)
#pragma GCC diagnostic ignored "-Wnested-anon-types"
#else
#pragma warning disable "nested-anon-types"
#endif
#endif /* -Wnested-anon-types */

#if __has_warning("-Wconstant-logical-operand")
#if defined(__clang__)
#pragma clang diagnostic ignored "-Wconstant-logical-operand"
#elif defined(__GNUC__)
#pragma GCC diagnostic ignored "-Wconstant-logical-operand"
#else
#pragma warning disable "constant-logical-operand"
#endif
#endif /* -Wconstant-logical-operand */

#if defined(__LCC__) && (__LCC__ <= 121)
/* bug #2798 */
#pragma diag_suppress alignment_reduction_ignored
#elif defined(__ICC)
#pragma warning(disable : 3453 1366)
#elif __has_warning("-Walignment-reduction-ignored")
#if defined(__clang__)
#pragma clang diagnostic ignored "-Walignment-reduction-ignored"
#elif defined(__GNUC__)
#pragma GCC diagnostic ignored "-Walignment-reduction-ignored"
#else
#pragma warning disable "alignment-reduction-ignored"
#endif
#endif /* -Walignment-reduction-ignored */

#ifdef __cplusplus
extern "C" {
#endif

#include "osal.h"

#define mdbx_sourcery_anchor XCONCAT(mdbx_sourcery_, MDBX_BUILD_SOURCERY)
#if defined(xMDBX_TOOLS)
extern LIBMDBX_API const char *const mdbx_sourcery_anchor;
#endif

#include "options.h"

/* Undefine the NDEBUG if debugging is enforced by MDBX_DEBUG */
#if MDBX_DEBUG
#undef NDEBUG
#endif

/*----------------------------------------------------------------------------*/
/* Atomics */

enum MDBX_memory_order {
  mo_Relaxed,
  mo_AcquireRelease,
  mo_SequentialConsistency
};

typedef union {
  volatile uint32_t weak;
#ifdef MDBX_HAVE_C11ATOMICS
  volatile _Atomic uint32_t c11a;
#endif /* MDBX_HAVE_C11ATOMICS */
} MDBX_atomic_uint32_t;

typedef union {
  volatile uint64_t weak;
#if defined(MDBX_HAVE_C11ATOMICS) && (MDBX_64BIT_CAS || MDBX_64BIT_ATOMIC)
  volatile _Atomic uint64_t c11a;
#endif
#if !defined(MDBX_HAVE_C11ATOMICS) || !MDBX_64BIT_CAS || !MDBX_64BIT_ATOMIC
  __anonymous_struct_extension__ struct {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    MDBX_atomic_uint32_t low, high;
#elif __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
    MDBX_atomic_uint32_t high, low;
#else
#error "FIXME: Unsupported byte order"
#endif /* __BYTE_ORDER__ */
  };
#endif
} MDBX_atomic_uint64_t;

#ifdef MDBX_HAVE_C11ATOMICS

/* Crutches for C11 atomic compiler's bugs */
#if defined(__e2k__) && defined(__LCC__) && __LCC__ < /* FIXME */ 127
#define MDBX_c11a_ro(type, ptr) (&(ptr)->weak)
#define MDBX_c11a_rw(type, ptr) (&(ptr)->weak)
#elif defined(__clang__) && __clang__ < 8
#define MDBX_c11a_ro(type, ptr) ((volatile _Atomic(type) *)&(ptr)->c11a)
#define MDBX_c11a_rw(type, ptr) (&(ptr)->c11a)
#else
#define MDBX_c11a_ro(type, ptr) (&(ptr)->c11a)
#define MDBX_c11a_rw(type, ptr) (&(ptr)->c11a)
#endif /* Crutches for C11 atomic compiler's bugs */

#define mo_c11_store(fence)                                                    \
  (((fence) == mo_Relaxed)          ? memory_order_relaxed                     \
   : ((fence) == mo_AcquireRelease) ? memory_order_release                     \
                                    : memory_order_seq_cst)
#define mo_c11_load(fence)                                                     \
  (((fence) == mo_Relaxed)          ? memory_order_relaxed                     \
   : ((fence) == mo_AcquireRelease) ? memory_order_acquire                     \
                                    : memory_order_seq_cst)

#endif /* MDBX_HAVE_C11ATOMICS */

#ifndef __cplusplus

#ifdef MDBX_HAVE_C11ATOMICS
#define mdbx_memory_fence(order, write)                                        \
  atomic_thread_fence((write) ? mo_c11_store(order) : mo_c11_load(order))
#else /* MDBX_HAVE_C11ATOMICS */
#define mdbx_memory_fence(order, write)                                        \
  do {                                                                         \
    mdbx_compiler_barrier();                                                   \
    if (write && order > (MDBX_CPU_WRITEBACK_INCOHERENT ? mo_Relaxed           \
                                                        : mo_AcquireRelease))  \
      mdbx_memory_barrier();                                                   \
  } while (0)
#endif /* MDBX_HAVE_C11ATOMICS */

#if defined(MDBX_HAVE_C11ATOMICS) && defined(__LCC__)
#define atomic_store32(p, value, order)                                        \
  ({                                                                           \
    const uint32_t value_to_store = (value);                                   \
    atomic_store_explicit(MDBX_c11a_rw(uint32_t, p), value_to_store,           \
                          mo_c11_store(order));                                \
    value_to_store;                                                            \
  })
#define atomic_load32(p, order)                                                \
  atomic_load_explicit(MDBX_c11a_ro(uint32_t, p), mo_c11_load(order))
#define atomic_store64(p, value, order)                                        \
  ({                                                                           \
    const uint64_t value_to_store = (value);                                   \
    atomic_store_explicit(MDBX_c11a_rw(uint64_t, p), value_to_store,           \
                          mo_c11_store(order));                                \
    value_to_store;                                                            \
  })
#define atomic_load64(p, order)                                                \
  atomic_load_explicit(MDBX_c11a_ro(uint64_t, p), mo_c11_load(order))
#endif /* LCC && MDBX_HAVE_C11ATOMICS */

#ifndef atomic_store32
MDBX_MAYBE_UNUSED static __always_inline uint32_t
atomic_store32(MDBX_atomic_uint32_t *p, const uint32_t value,
               enum MDBX_memory_order order) {
  STATIC_ASSERT(sizeof(MDBX_atomic_uint32_t) == 4);
#ifdef MDBX_HAVE_C11ATOMICS
  assert(atomic_is_lock_free(MDBX_c11a_rw(uint32_t, p)));
  atomic_store_explicit(MDBX_c11a_rw(uint32_t, p), value, mo_c11_store(order));
#else  /* MDBX_HAVE_C11ATOMICS */
  if (order != mo_Relaxed)
    mdbx_compiler_barrier();
  p->weak = value;
  mdbx_memory_fence(order, true);
#endif /* MDBX_HAVE_C11ATOMICS */
  return value;
}
#endif /* atomic_store32 */

#ifndef atomic_load32
MDBX_MAYBE_UNUSED static __always_inline uint32_t
atomic_load32(const MDBX_atomic_uint32_t *p, enum MDBX_memory_order order) {
  STATIC_ASSERT(sizeof(MDBX_atomic_uint32_t) == 4);
#ifdef MDBX_HAVE_C11ATOMICS
  assert(atomic_is_lock_free(MDBX_c11a_ro(uint32_t, p)));
  return atomic_load_explicit(MDBX_c11a_ro(uint32_t, p), mo_c11_load(order));
#else  /* MDBX_HAVE_C11ATOMICS */
  mdbx_memory_fence(order, false);
  const uint32_t value = p->weak;
  if (order != mo_Relaxed)
    mdbx_compiler_barrier();
  return value;
#endif /* MDBX_HAVE_C11ATOMICS */
}
#endif /* atomic_load32 */

#endif /* !__cplusplus */

/*----------------------------------------------------------------------------*/
/* Basic constants and types */

/* A stamp that identifies a file as an MDBX file.
 * There's nothing special about this value other than that it is easily
 * recognizable, and it will reflect any byte order mismatches. */
#define MDBX_MAGIC UINT64_C(/* 56-bit prime */ 0x59659DBDEF4C11)

/* FROZEN: The version number for a database's datafile format. */
#define MDBX_DATA_VERSION 3
/* The version number for a database's lockfile format. */
#define MDBX_LOCK_VERSION 4

/* handle for the DB used to track free pages. */
#define FREE_DBI 0
/* handle for the default DB. */
#define MAIN_DBI 1
/* Number of DBs in metapage (free and main) - also hardcoded elsewhere */
#define CORE_DBS 2

/* Number of meta pages - also hardcoded elsewhere */
#define NUM_METAS 3

/* A page number in the database.
 *
 * MDBX uses 32 bit for page numbers. This limits database
 * size up to 2^44 bytes, in case of 4K pages. */
typedef uint32_t pgno_t;
typedef MDBX_atomic_uint32_t atomic_pgno_t;
#define PRIaPGNO PRIu32
#define MAX_PAGENO UINT32_C(0x7FFFffff)
#define MIN_PAGENO NUM_METAS

#define SAFE64_INVALID_THRESHOLD UINT64_C(0xffffFFFF00000000)

/* A transaction ID. */
typedef uint64_t txnid_t;
typedef MDBX_atomic_uint64_t atomic_txnid_t;
#define PRIaTXN PRIi64
#define MIN_TXNID UINT64_C(1)
#define MAX_TXNID (SAFE64_INVALID_THRESHOLD - 1)
#define INITIAL_TXNID (MIN_TXNID + NUM_METAS - 1)
#define INVALID_TXNID UINT64_MAX
/* LY: for testing non-atomic 64-bit txnid on 32-bit arches.
 * #define xMDBX_TXNID_STEP (UINT32_MAX / 3) */
#ifndef xMDBX_TXNID_STEP
#if MDBX_64BIT_CAS
#define xMDBX_TXNID_STEP 1u
#else
#define xMDBX_TXNID_STEP 2u
#endif
#endif /* xMDBX_TXNID_STEP */

/* Used for offsets within a single page.
 * Since memory pages are typically 4 or 8KB in size, 12-13 bits,
 * this is plenty. */
typedef uint16_t indx_t;

#define MEGABYTE ((size_t)1 << 20)

/*----------------------------------------------------------------------------*/
/* Core structures for database and shared memory (i.e. format definition) */
#pragma pack(push, 4)

/* Information about a single database in the environment. */
typedef struct MDBX_db {
  uint16_t md_flags;        /* see mdbx_dbi_open */
  uint16_t md_depth;        /* depth of this tree */
  uint32_t md_xsize;        /* key-size for MDBX_DUPFIXED (LEAF2 pages) */
  pgno_t md_root;           /* the root page of this tree */
  pgno_t md_branch_pages;   /* number of internal pages */
  pgno_t md_leaf_pages;     /* number of leaf pages */
  pgno_t md_overflow_pages; /* number of overflow pages */
  uint64_t md_seq;          /* table sequence counter */
  uint64_t md_entries;      /* number of data items */
  uint64_t md_mod_txnid;    /* txnid of last committed modification */
} MDBX_db;

/* database size-related parameters */
typedef struct MDBX_geo {
  uint16_t grow_pv;   /* datafile growth step as a 16-bit packed (exponential
                           quantized) value */
  uint16_t shrink_pv; /* datafile shrink threshold as a 16-bit packed
                           (exponential quantized) value */
  pgno_t lower;       /* minimal size of datafile in pages */
  pgno_t upper;       /* maximal size of datafile in pages */
  pgno_t now;         /* current size of datafile in pages */
  pgno_t next;        /* first unused page in the datafile,
                         but actually the file may be shorter. */
} MDBX_geo;

/* Meta page content.
 * A meta page is the start point for accessing a database snapshot.
 * Pages 0-1 are meta pages. Transaction N writes meta page (N % 2). */
typedef struct MDBX_meta {
  /* Stamp identifying this as an MDBX file.
   * It must be set to MDBX_MAGIC with MDBX_DATA_VERSION. */
  uint32_t mm_magic_and_version[2];

  /* txnid that committed this page, the first of a two-phase-update pair */
  uint32_t mm_txnid_a[2];

  uint16_t mm_extra_flags;  /* extra DB flags, zero (nothing) for now */
  uint8_t mm_validator_id;  /* ID of checksum and page validation method,
                             * zero (nothing) for now */
  uint8_t mm_extra_pagehdr; /* extra bytes in the page header,
                             * zero (nothing) for now */

  MDBX_geo mm_geo; /* database size-related parameters */

  MDBX_db mm_dbs[CORE_DBS]; /* first is free space, 2nd is main db */
                            /* The size of pages used in this DB */
#define mm_psize mm_dbs[FREE_DBI].md_xsize
  MDBX_canary mm_canary;

#define MDBX_DATASIGN_NONE 0u
#define MDBX_DATASIGN_WEAK 1u
#define SIGN_IS_STEADY(sign) ((sign) > MDBX_DATASIGN_WEAK)
#define META_IS_STEADY(meta)                                                   \
  SIGN_IS_STEADY(unaligned_peek_u64_volatile(4, (meta)->mm_datasync_sign))
  uint32_t mm_datasync_sign[2];

  /* txnid that committed this page, the second of a two-phase-update pair */
  uint32_t mm_txnid_b[2];

  /* Number of non-meta pages which were put in GC after COW. May be 0 in case
   * DB was previously handled by libmdbx without corresponding feature.
   * This value in couple with mr_snapshot_pages_retired allows fast estimation
   * of "how much reader is restraining GC recycling". */
  uint32_t mm_pages_retired[2];

  /* The analogue /proc/sys/kernel/random/boot_id or similar to determine
   * whether the system was rebooted after the last use of the database files.
   * If there was no reboot, but there is no need to rollback to the last
   * steady sync point. Zeros mean that no relevant information is available
   * from the system. */
  bin128_t mm_bootid;

} MDBX_meta;

#pragma pack(1)

/* Common header for all page types. The page type depends on mp_flags.
 *
 * P_BRANCH and P_LEAF pages have unsorted 'MDBX_node's at the end, with
 * sorted mp_ptrs[] entries referring to them. Exception: P_LEAF2 pages
 * omit mp_ptrs and pack sorted MDBX_DUPFIXED values after the page header.
 *
 * P_OVERFLOW records occupy one or more contiguous pages where only the
 * first has a page header. They hold the real data of F_BIGDATA nodes.
 *
 * P_SUBP sub-pages are small leaf "pages" with duplicate data.
 * A node with flag F_DUPDATA but not F_SUBDATA contains a sub-page.
 * (Duplicate data can also go in sub-databases, which use normal pages.)
 *
 * P_META pages contain MDBX_meta, the start point of an MDBX snapshot.
 *
 * Each non-metapage up to MDBX_meta.mm_last_pg is reachable exactly once
 * in the snapshot: Either used by a database or listed in a GC record. */
typedef struct MDBX_page {
  union {
#define IS_FROZEN(txn, p) ((p)->mp_txnid < (txn)->mt_txnid)
#define IS_SPILLED(txn, p) ((p)->mp_txnid == (txn)->mt_txnid)
#define IS_SHADOWED(txn, p) ((p)->mp_txnid > (txn)->mt_txnid)
#define IS_VALID(txn, p) ((p)->mp_txnid <= (txn)->mt_front)
#define IS_MODIFIABLE(txn, p) ((p)->mp_txnid == (txn)->mt_front)
    uint64_t mp_txnid;
    struct MDBX_page *mp_next; /* for in-memory list of freed pages */
  };
  uint16_t mp_leaf2_ksize; /* key size if this is a LEAF2 page */
#define P_BRANCH 0x01      /* branch page */
#define P_LEAF 0x02        /* leaf page */
#define P_OVERFLOW 0x04    /* overflow page */
#define P_META 0x08        /* meta page */
#define P_BAD 0x10         /* explicit flag for invalid/bad page */
#define P_LEAF2 0x20       /* for MDBX_DUPFIXED records */
#define P_SUBP 0x40        /* for MDBX_DUPSORT sub-pages */
#define P_SPILLED 0x2000   /* spilled in parent txn */
#define P_LOOSE 0x4000     /* page was dirtied then freed, can be reused */
#define P_FROZEN 0x8000    /* used for retire page with known status */
#define P_ILL_BITS (~(P_BRANCH | P_LEAF | P_LEAF2 | P_OVERFLOW | P_SPILLED))
  uint16_t mp_flags;
  union {
    uint32_t mp_pages; /* number of overflow pages */
    __anonymous_struct_extension__ struct {
      indx_t mp_lower; /* lower bound of free space */
      indx_t mp_upper; /* upper bound of free space */
    };
  };
  pgno_t mp_pgno; /* page number */

#if (defined(__STDC_VERSION__) && __STDC_VERSION__ >= 199901L) ||              \
    (!defined(__cplusplus) && defined(_MSC_VER))
  indx_t mp_ptrs[] /* dynamic size */;
#endif /* C99 */
} MDBX_page;

/* Size of the page header, excluding dynamic data at the end */
#define PAGEHDRSZ ((unsigned)offsetof(MDBX_page, mp_ptrs))

#pragma pack(pop)

#if MDBX_ENABLE_PGOP_STAT
/* Statistics of page operations overall of all (running, completed and aborted)
 * transactions */
typedef struct {
  MDBX_atomic_uint64_t newly;   /* Quantity of a new pages added */
  MDBX_atomic_uint64_t cow;     /* Quantity of pages copied for update */
  MDBX_atomic_uint64_t clone;   /* Quantity of parent's dirty pages clones
                                   for nested transactions */
  MDBX_atomic_uint64_t split;   /* Page splits */
  MDBX_atomic_uint64_t merge;   /* Page merges */
  MDBX_atomic_uint64_t spill;   /* Quantity of spilled dirty pages */
  MDBX_atomic_uint64_t unspill; /* Quantity of unspilled/reloaded pages */
  MDBX_atomic_uint64_t
      wops; /* Number of explicit write operations (not a pages) to a disk */
} MDBX_pgop_stat_t;
#endif /* MDBX_ENABLE_PGOP_STAT */

#if MDBX_LOCKING == MDBX_LOCKING_WIN32FILES
#define MDBX_CLOCK_SIGN UINT32_C(0xF10C)
typedef void mdbx_ipclock_t;
#elif MDBX_LOCKING == MDBX_LOCKING_SYSV

#define MDBX_CLOCK_SIGN UINT32_C(0xF18D)
typedef mdbx_pid_t mdbx_ipclock_t;
#ifndef EOWNERDEAD
#define EOWNERDEAD MDBX_RESULT_TRUE
#endif

#elif MDBX_LOCKING == MDBX_LOCKING_POSIX2001 ||                                \
    MDBX_LOCKING == MDBX_LOCKING_POSIX2008
#define MDBX_CLOCK_SIGN UINT32_C(0x8017)
typedef pthread_mutex_t mdbx_ipclock_t;
#elif MDBX_LOCKING == MDBX_LOCKING_POSIX1988
#define MDBX_CLOCK_SIGN UINT32_C(0xFC29)
typedef sem_t mdbx_ipclock_t;
#else
#error "FIXME"
#endif /* MDBX_LOCKING */

#if MDBX_LOCKING > MDBX_LOCKING_SYSV && !defined(__cplusplus)
MDBX_INTERNAL_FUNC int mdbx_ipclock_stub(mdbx_ipclock_t *ipc);
MDBX_INTERNAL_FUNC int mdbx_ipclock_destroy(mdbx_ipclock_t *ipc);
#endif /* MDBX_LOCKING */

/* Reader Lock Table
 *
 * Readers don't acquire any locks for their data access. Instead, they
 * simply record their transaction ID in the reader table. The reader
 * mutex is needed just to find an empty slot in the reader table. The
 * slot's address is saved in thread-specific data so that subsequent
 * read transactions started by the same thread need no further locking to
 * proceed.
 *
 * If MDBX_NOTLS is set, the slot address is not saved in thread-specific data.
 * No reader table is used if the database is on a read-only filesystem.
 *
 * Since the database uses multi-version concurrency control, readers don't
 * actually need any locking. This table is used to keep track of which
 * readers are using data from which old transactions, so that we'll know
 * when a particular old transaction is no longer in use. Old transactions
 * that have discarded any data pages can then have those pages reclaimed
 * for use by a later write transaction.
 *
 * The lock table is constructed such that reader slots are aligned with the
 * processor's cache line size. Any slot is only ever used by one thread.
 * This alignment guarantees that there will be no contention or cache
 * thrashing as threads update their own slot info, and also eliminates
 * any need for locking when accessing a slot.
 *
 * A writer thread will scan every slot in the table to determine the oldest
 * outstanding reader transaction. Any freed pages older than this will be
 * reclaimed by the writer. The writer doesn't use any locks when scanning
 * this table. This means that there's no guarantee that the writer will
 * see the most up-to-date reader info, but that's not required for correct
 * operation - all we need is to know the upper bound on the oldest reader,
 * we don't care at all about the newest reader. So the only consequence of
 * reading stale information here is that old pages might hang around a
 * while longer before being reclaimed. That's actually good anyway, because
 * the longer we delay reclaiming old pages, the more likely it is that a
 * string of contiguous pages can be found after coalescing old pages from
 * many old transactions together. */

/* The actual reader record, with cacheline padding. */
typedef struct MDBX_reader {
  /* Current Transaction ID when this transaction began, or (txnid_t)-1.
   * Multiple readers that start at the same time will probably have the
   * same ID here. Again, it's not important to exclude them from
   * anything; all we need to know is which version of the DB they
   * started from so we can avoid overwriting any data used in that
   * particular version. */
  MDBX_atomic_uint64_t /* txnid_t */ mr_txnid;

  /* The information we store in a single slot of the reader table.
   * In addition to a transaction ID, we also record the process and
   * thread ID that owns a slot, so that we can detect stale information,
   * e.g. threads or processes that went away without cleaning up.
   *
   * NOTE: We currently don't check for stale records.
   * We simply re-init the table when we know that we're the only process
   * opening the lock file. */

  /* The thread ID of the thread owning this txn. */
  MDBX_atomic_uint64_t mr_tid;

  /* The process ID of the process owning this reader txn. */
  MDBX_atomic_uint32_t mr_pid;

  /* The number of pages used in the reader's MVCC snapshot,
   * i.e. the value of meta->mm_geo.next and txn->mt_next_pgno */
  atomic_pgno_t mr_snapshot_pages_used;
  /* Number of retired pages at the time this reader starts transaction. So,
   * at any time the difference mm_pages_retired - mr_snapshot_pages_retired
   * will give the number of pages which this reader restraining from reuse. */
  MDBX_atomic_uint64_t mr_snapshot_pages_retired;
} MDBX_reader;

/* The header for the reader table (a memory-mapped lock file). */
typedef struct MDBX_lockinfo {
  /* Stamp identifying this as an MDBX file.
   * It must be set to MDBX_MAGIC with MDBX_LOCK_VERSION. */
  uint64_t mti_magic_and_version;

  /* Format of this lock file. Must be set to MDBX_LOCK_FORMAT. */
  uint32_t mti_os_and_format;

  /* Flags which environment was opened. */
  MDBX_atomic_uint32_t mti_envmode;

  /* Threshold of un-synced-with-disk pages for auto-sync feature,
   * zero means no-threshold, i.e. auto-sync is disabled. */
  atomic_pgno_t mti_autosync_threshold;

  /* Low 32-bit of txnid with which meta-pages was synced,
   * i.e. for sync-polling in the MDBX_NOMETASYNC mode. */
  MDBX_atomic_uint32_t mti_meta_sync_txnid;

  /* Period for timed auto-sync feature, i.e. at the every steady checkpoint
   * the mti_unsynced_timeout sets to the current_time + mti_autosync_period.
   * The time value is represented in a suitable system-dependent form, for
   * example clock_gettime(CLOCK_BOOTTIME) or clock_gettime(CLOCK_MONOTONIC).
   * Zero means timed auto-sync is disabled. */
  MDBX_atomic_uint64_t mti_autosync_period;

  /* Marker to distinguish uniqueness of DB/CLK. */
  MDBX_atomic_uint64_t mti_bait_uniqueness;

  MDBX_ALIGNAS(MDBX_CACHELINE_SIZE) /* cacheline ----------------------------*/

#if MDBX_ENABLE_PGOP_STAT
  /* Statistics of costly ops of all (running, completed and aborted)
   * transactions */
  MDBX_pgop_stat_t mti_pgop_stat;
#endif /* MDBX_ENABLE_PGOP_STAT*/

  MDBX_ALIGNAS(MDBX_CACHELINE_SIZE) /* cacheline ----------------------------*/

  /* Write transaction lock. */
#if MDBX_LOCKING > 0
  mdbx_ipclock_t mti_wlock;
#endif /* MDBX_LOCKING > 0 */

  atomic_txnid_t mti_oldest_reader;

  /* Timestamp of the last steady sync. Value is represented in a suitable
   * system-dependent form, for example clock_gettime(CLOCK_BOOTTIME) or
   * clock_gettime(CLOCK_MONOTONIC). */
  MDBX_atomic_uint64_t mti_sync_timestamp;

  /* Number un-synced-with-disk pages for auto-sync feature. */
  atomic_pgno_t mti_unsynced_pages;

  /* Number of page which was discarded last time by madvise(MADV_FREE). */
  atomic_pgno_t mti_discarded_tail;

  /* Timestamp of the last readers check. */
  MDBX_atomic_uint64_t mti_reader_check_timestamp;

  /* Shared anchor for tracking readahead edge and enabled/disabled status. */
  pgno_t mti_readahead_anchor;

  MDBX_ALIGNAS(MDBX_CACHELINE_SIZE) /* cacheline ----------------------------*/

  /* Readeaders registration lock. */
#if MDBX_LOCKING > 0
  mdbx_ipclock_t mti_rlock;
#endif /* MDBX_LOCKING > 0 */

  /* The number of slots that have been used in the reader table.
   * This always records the maximum count, it is not decremented
   * when readers release their slots. */
  MDBX_atomic_uint32_t mti_numreaders;
  MDBX_atomic_uint32_t mti_readers_refresh_flag;

#if (defined(__STDC_VERSION__) && __STDC_VERSION__ >= 199901L) ||              \
    (!defined(__cplusplus) && defined(_MSC_VER))
  MDBX_ALIGNAS(MDBX_CACHELINE_SIZE) /* cacheline ----------------------------*/
  MDBX_reader mti_readers[] /* dynamic size */;
#endif /* C99 */
} MDBX_lockinfo;

/* Lockfile format signature: version, features and field layout */
#define MDBX_LOCK_FORMAT                                                       \
  (MDBX_CLOCK_SIGN * 27733 + (unsigned)sizeof(MDBX_reader) * 13 +              \
   (unsigned)offsetof(MDBX_reader, mr_snapshot_pages_used) * 251 +             \
   (unsigned)offsetof(MDBX_lockinfo, mti_oldest_reader) * 83 +                 \
   (unsigned)offsetof(MDBX_lockinfo, mti_numreaders) * 37 +                    \
   (unsigned)offsetof(MDBX_lockinfo, mti_readers) * 29)

#define MDBX_DATA_MAGIC                                                        \
  ((MDBX_MAGIC << 8) + MDBX_PNL_ASCENDING * 64 + MDBX_DATA_VERSION)

#define MDBX_DATA_MAGIC_LEGACY_COMPAT                                          \
  ((MDBX_MAGIC << 8) + MDBX_PNL_ASCENDING * 64 + 2)

#define MDBX_DATA_MAGIC_LEGACY_DEVEL ((MDBX_MAGIC << 8) + 255)

#define MDBX_LOCK_MAGIC ((MDBX_MAGIC << 8) + MDBX_LOCK_VERSION)

/* The maximum size of a database page.
 *
 * It is 64K, but value-PAGEHDRSZ must fit in MDBX_page.mp_upper.
 *
 * MDBX will use database pages < OS pages if needed.
 * That causes more I/O in write transactions: The OS must
 * know (read) the whole page before writing a partial page.
 *
 * Note that we don't currently support Huge pages. On Linux,
 * regular data files cannot use Huge pages, and in general
 * Huge pages aren't actually pageable. We rely on the OS
 * demand-pager to read our data and page it out when memory
 * pressure from other processes is high. So until OSs have
 * actual paging support for Huge pages, they're not viable. */
#define MAX_PAGESIZE MDBX_MAX_PAGESIZE
#define MIN_PAGESIZE MDBX_MIN_PAGESIZE

#define MIN_MAPSIZE (MIN_PAGESIZE * MIN_PAGENO)
#if defined(_WIN32) || defined(_WIN64)
#define MAX_MAPSIZE32 UINT32_C(0x38000000)
#else
#define MAX_MAPSIZE32 UINT32_C(0x7f000000)
#endif
#define MAX_MAPSIZE64 ((MAX_PAGENO + 1) * (uint64_t)MAX_PAGESIZE)

#if MDBX_WORDBITS >= 64
#define MAX_MAPSIZE MAX_MAPSIZE64
#define MDBX_PGL_LIMIT ((size_t)MAX_PAGENO)
#else
#define MAX_MAPSIZE MAX_MAPSIZE32
#define MDBX_PGL_LIMIT (MAX_MAPSIZE32 / MIN_PAGESIZE)
#endif /* MDBX_WORDBITS */

#define MDBX_READERS_LIMIT 32767
#define MDBX_RADIXSORT_THRESHOLD 333

/*----------------------------------------------------------------------------*/

/* An PNL is an Page Number List, a sorted array of IDs.
 * The first element of the array is a counter for how many actual page-numbers
 * are in the list. By default PNLs are sorted in descending order, this allow
 * cut off a page with lowest pgno (at the tail) just truncating the list. The
 * sort order of PNLs is controlled by the MDBX_PNL_ASCENDING build option. */
typedef pgno_t *MDBX_PNL;

#if MDBX_PNL_ASCENDING
#define MDBX_PNL_ORDERED(first, last) ((first) < (last))
#define MDBX_PNL_DISORDERED(first, last) ((first) >= (last))
#else
#define MDBX_PNL_ORDERED(first, last) ((first) > (last))
#define MDBX_PNL_DISORDERED(first, last) ((first) <= (last))
#endif

/* List of txnid, only for MDBX_txn.tw.lifo_reclaimed */
typedef txnid_t *MDBX_TXL;

/* An Dirty-Page list item is an pgno/pointer pair. */
typedef struct MDBX_dp {
  MDBX_page *ptr;
  pgno_t pgno;
  union {
    unsigned extra;
    __anonymous_struct_extension__ struct {
      unsigned multi : 1;
      unsigned lru : 31;
    };
  };
} MDBX_dp;

/* An DPL (dirty-page list) is a sorted array of MDBX_DPs. */
typedef struct MDBX_dpl {
  unsigned sorted;
  unsigned length;
  unsigned detent; /* allocated size excluding the MDBX_DPL_RESERVE_GAP */
#if (defined(__STDC_VERSION__) && __STDC_VERSION__ >= 199901L) ||              \
    (!defined(__cplusplus) && defined(_MSC_VER))
  MDBX_dp items[] /* dynamic size with holes at zero and after the last */;
#endif
} MDBX_dpl;

/* PNL sizes */
#define MDBX_PNL_GRANULATE 1024
#define MDBX_PNL_INITIAL                                                       \
  (MDBX_PNL_GRANULATE - 2 - MDBX_ASSUME_MALLOC_OVERHEAD / sizeof(pgno_t))

#define MDBX_TXL_GRANULATE 32
#define MDBX_TXL_INITIAL                                                       \
  (MDBX_TXL_GRANULATE - 2 - MDBX_ASSUME_MALLOC_OVERHEAD / sizeof(txnid_t))
#define MDBX_TXL_MAX                                                           \
  ((1u << 17) - 2 - MDBX_ASSUME_MALLOC_OVERHEAD / sizeof(txnid_t))

#define MDBX_PNL_ALLOCLEN(pl) ((pl)[-1])
#define MDBX_PNL_SIZE(pl) ((pl)[0])
#define MDBX_PNL_FIRST(pl) ((pl)[1])
#define MDBX_PNL_LAST(pl) ((pl)[MDBX_PNL_SIZE(pl)])
#define MDBX_PNL_BEGIN(pl) (&(pl)[1])
#define MDBX_PNL_END(pl) (&(pl)[MDBX_PNL_SIZE(pl) + 1])

#if MDBX_PNL_ASCENDING
#define MDBX_PNL_LEAST(pl) MDBX_PNL_FIRST(pl)
#define MDBX_PNL_MOST(pl) MDBX_PNL_LAST(pl)
#else
#define MDBX_PNL_LEAST(pl) MDBX_PNL_LAST(pl)
#define MDBX_PNL_MOST(pl) MDBX_PNL_FIRST(pl)
#endif

#define MDBX_PNL_SIZEOF(pl) ((MDBX_PNL_SIZE(pl) + 1) * sizeof(pgno_t))
#define MDBX_PNL_IS_EMPTY(pl) (MDBX_PNL_SIZE(pl) == 0)

/*----------------------------------------------------------------------------*/
/* Internal structures */

/* Auxiliary DB info.
 * The information here is mostly static/read-only. There is
 * only a single copy of this record in the environment. */
typedef struct MDBX_dbx {
  MDBX_val md_name;                /* name of the database */
  MDBX_cmp_func *md_cmp;           /* function for comparing keys */
  MDBX_cmp_func *md_dcmp;          /* function for comparing data items */
  size_t md_klen_min, md_klen_max; /* min/max key length for the database */
  size_t md_vlen_min,
      md_vlen_max; /* min/max value/data length for the database */
} MDBX_dbx;

/* A database transaction.
 * Every operation requires a transaction handle. */
struct MDBX_txn {
#define MDBX_MT_SIGNATURE UINT32_C(0x93D53A31)
  uint32_t mt_signature;

  /* Transaction Flags */
  /* mdbx_txn_begin() flags */
#define MDBX_TXN_RO_BEGIN_FLAGS (MDBX_TXN_RDONLY | MDBX_TXN_RDONLY_PREPARE)
#define MDBX_TXN_RW_BEGIN_FLAGS                                                \
  (MDBX_TXN_NOMETASYNC | MDBX_TXN_NOSYNC | MDBX_TXN_TRY)
  /* Additional flag for mdbx_sync_locked() */
#define MDBX_SHRINK_ALLOWED UINT32_C(0x40000000)

#define TXN_FLAGS                                                              \
  (MDBX_TXN_FINISHED | MDBX_TXN_ERROR | MDBX_TXN_DIRTY | MDBX_TXN_SPILLS |     \
   MDBX_TXN_HAS_CHILD | MDBX_TXN_INVALID)

#if (TXN_FLAGS & (MDBX_TXN_RW_BEGIN_FLAGS | MDBX_TXN_RO_BEGIN_FLAGS)) ||       \
    ((MDBX_TXN_RW_BEGIN_FLAGS | MDBX_TXN_RO_BEGIN_FLAGS | TXN_FLAGS) &         \
     MDBX_SHRINK_ALLOWED)
#error "Oops, some txn flags overlapped or wrong"
#endif
  uint32_t mt_flags;

  MDBX_txn *mt_parent; /* parent of a nested txn */
  /* Nested txn under this txn, set together with flag MDBX_TXN_HAS_CHILD */
  MDBX_txn *mt_child;
  MDBX_geo mt_geo;
  /* next unallocated page */
#define mt_next_pgno mt_geo.next
  /* corresponding to the current size of datafile */
#define mt_end_pgno mt_geo.now

  /* The ID of this transaction. IDs are integers incrementing from 1.
   * Only committed write transactions increment the ID. If a transaction
   * aborts, the ID may be re-used by the next writer. */
  txnid_t mt_txnid;
  txnid_t mt_front;

  MDBX_env *mt_env; /* the DB environment */
  /* Array of records for each DB known in the environment. */
  MDBX_dbx *mt_dbxs;
  /* Array of MDBX_db records for each known DB */
  MDBX_db *mt_dbs;
  /* Array of sequence numbers for each DB handle */
  unsigned *mt_dbiseqs;

  /* Transaction DBI Flags */
#define DBI_DIRTY MDBX_DBI_DIRTY /* DB was written in this txn */
#define DBI_STALE MDBX_DBI_STALE /* Named-DB record is older than txnID */
#define DBI_FRESH MDBX_DBI_FRESH /* Named-DB handle opened in this txn */
#define DBI_CREAT MDBX_DBI_CREAT /* Named-DB handle created in this txn */
#define DBI_VALID 0x10           /* DB handle is valid, see also DB_VALID */
#define DBI_USRVALID 0x20        /* As DB_VALID, but not set for FREE_DBI */
#define DBI_AUDITED 0x40         /* Internal flag for accounting during audit */
  /* Array of flags for each DB */
  uint8_t *mt_dbistate;
  /* Number of DB records in use, or 0 when the txn is finished.
   * This number only ever increments until the txn finishes; we
   * don't decrement it when individual DB handles are closed. */
  MDBX_dbi mt_numdbs;
  size_t mt_owner; /* thread ID that owns this transaction */
  MDBX_canary mt_canary;
  void *mt_userctx; /* User-settable context */
  MDBX_cursor **mt_cursors;

  union {
    struct {
      /* For read txns: This thread/txn's reader table slot, or NULL. */
      MDBX_reader *reader;
    } to;
    struct {
      /* In write txns, array of cursors for each DB */
      pgno_t *reclaimed_pglist; /* Reclaimed GC pages */
      txnid_t last_reclaimed;   /* ID of last used record */
#if MDBX_ENABLE_REFUND
      pgno_t loose_refund_wl /* FIXME: describe */;
#endif /* MDBX_ENABLE_REFUND */
      /* dirtylist room: Dirty array size - dirty pages visible to this txn.
       * Includes ancestor txns' dirty pages not hidden by other txns'
       * dirty/spilled pages. Thus commit(nested txn) has room to merge
       * dirtylist into mt_parent after freeing hidden mt_parent pages. */
      unsigned dirtyroom;
      /* a sequence to spilling dirty page with LRU policy */
      unsigned dirtylru;
      /* For write txns: Modified pages. Sorted when not MDBX_WRITEMAP. */
      MDBX_dpl *dirtylist;
      /* The list of reclaimed txns from GC */
      MDBX_TXL lifo_reclaimed;
      /* The list of pages that became unused during this transaction. */
      MDBX_PNL retired_pages;
      /* The list of loose pages that became unused and may be reused
       * in this transaction, linked through `mp_next`. */
      MDBX_page *loose_pages;
      /* Number of loose pages (tw.loose_pages) */
      unsigned loose_count;
      /* The sorted list of dirty pages we temporarily wrote to disk
       * because the dirty list was full. page numbers in here are
       * shifted left by 1, deleted slots have the LSB set. */
      MDBX_PNL spill_pages;
      unsigned spill_least_removed;
    } tw;
  };
};

#if MDBX_WORDBITS >= 64
#define CURSOR_STACK 32
#else
#define CURSOR_STACK 24
#endif

struct MDBX_xcursor;

/* Cursors are used for all DB operations.
 * A cursor holds a path of (page pointer, key index) from the DB
 * root to a position in the DB, plus other state. MDBX_DUPSORT
 * cursors include an xcursor to the current data item. Write txns
 * track their cursors and keep them up to date when data moves.
 * Exception: An xcursor's pointer to a P_SUBP page can be stale.
 * (A node with F_DUPDATA but no F_SUBDATA contains a subpage). */
struct MDBX_cursor {
#define MDBX_MC_LIVE UINT32_C(0xFE05D5B1)
#define MDBX_MC_READY4CLOSE UINT32_C(0x2817A047)
#define MDBX_MC_WAIT4EOT UINT32_C(0x90E297A7)
  uint32_t mc_signature;
  /* The database handle this cursor operates on */
  MDBX_dbi mc_dbi;
  /* Next cursor on this DB in this txn */
  MDBX_cursor *mc_next;
  /* Backup of the original cursor if this cursor is a shadow */
  MDBX_cursor *mc_backup;
  /* Context used for databases with MDBX_DUPSORT, otherwise NULL */
  struct MDBX_xcursor *mc_xcursor;
  /* The transaction that owns this cursor */
  MDBX_txn *mc_txn;
  /* The database record for this cursor */
  MDBX_db *mc_db;
  /* The database auxiliary record for this cursor */
  MDBX_dbx *mc_dbx;
  /* The mt_dbistate for this database */
  uint8_t *mc_dbistate;
  unsigned mc_snum; /* number of pushed pages */
  unsigned mc_top;  /* index of top page, normally mc_snum-1 */

  /* Cursor state flags. */
#define C_INITIALIZED 0x01 /* cursor has been initialized and is valid */
#define C_EOF 0x02         /* No more data */
#define C_SUB 0x04         /* Cursor is a sub-cursor */
#define C_DEL 0x08         /* last op was a cursor_del */
#define C_UNTRACK 0x10     /* Un-track cursor when closing */
#define C_RECLAIMING 0x20  /* GC lookup is prohibited */
#define C_GCFREEZE 0x40    /* reclaimed_pglist must not be updated */

  /* Cursor checking flags. */
#define C_COPYING 0x100  /* skip key-value length check (copying simplify) */
#define C_UPDATING 0x200 /* update/rebalance pending */
#define C_RETIRING 0x400 /* refs to child pages may be invalid */
#define C_SKIPORD 0x800  /* don't check keys ordering */

  unsigned mc_flags;              /* see mdbx_cursor */
  MDBX_page *mc_pg[CURSOR_STACK]; /* stack of pushed pages */
  indx_t mc_ki[CURSOR_STACK];     /* stack of page indices */
};

/* Context for sorted-dup records.
 * We could have gone to a fully recursive design, with arbitrarily
 * deep nesting of sub-databases. But for now we only handle these
 * levels - main DB, optional sub-DB, sorted-duplicate DB. */
typedef struct MDBX_xcursor {
  /* A sub-cursor for traversing the Dup DB */
  MDBX_cursor mx_cursor;
  /* The database record for this Dup DB */
  MDBX_db mx_db;
  /* The auxiliary DB record for this Dup DB */
  MDBX_dbx mx_dbx;
} MDBX_xcursor;

typedef struct MDBX_cursor_couple {
  MDBX_cursor outer;
  void *mc_userctx; /* User-settable context */
  MDBX_xcursor inner;
} MDBX_cursor_couple;

/* The database environment. */
struct MDBX_env {
  /* ----------------------------------------------------- mostly static part */
#define MDBX_ME_SIGNATURE UINT32_C(0x9A899641)
  MDBX_atomic_uint32_t me_signature;
  /* Failed to update the meta page. Probably an I/O error. */
#define MDBX_FATAL_ERROR UINT32_C(0x80000000)
  /* Some fields are initialized. */
#define MDBX_ENV_ACTIVE UINT32_C(0x20000000)
  /* me_txkey is set */
#define MDBX_ENV_TXKEY UINT32_C(0x10000000)
  /* Legacy MDBX_MAPASYNC (prior v0.9) */
#define MDBX_DEPRECATED_MAPASYNC UINT32_C(0x100000)
#define ENV_INTERNAL_FLAGS (MDBX_FATAL_ERROR | MDBX_ENV_ACTIVE | MDBX_ENV_TXKEY)
  uint32_t me_flags;
  mdbx_mmap_t me_dxb_mmap; /* The main data file */
#define me_map me_dxb_mmap.dxb
#define me_lazy_fd me_dxb_mmap.fd
  mdbx_filehandle_t me_dsync_fd;
  mdbx_mmap_t me_lck_mmap; /* The lock file */
#define me_lfd me_lck_mmap.fd
  struct MDBX_lockinfo *me_lck;

  unsigned me_psize;        /* DB page size, initialized from me_os_psize */
  unsigned me_leaf_nodemax; /* max size of a leaf-node */
  uint8_t me_psize2log;     /* log2 of DB page size */
  int8_t me_stuck_meta; /* recovery-only: target meta page or less that zero */
  uint16_t me_merge_threshold,
      me_merge_threshold_gc;  /* pages emptier than this are candidates for
                                 merging */
  unsigned me_os_psize;       /* OS page size, from mdbx_syspagesize() */
  unsigned me_maxreaders;     /* size of the reader table */
  MDBX_dbi me_maxdbs;         /* size of the DB table */
  uint32_t me_pid;            /* process ID of this env */
  mdbx_thread_key_t me_txkey; /* thread-key for readers */
  char *me_pathname;          /* path to the DB files */
  void *me_pbuf;              /* scratch area for DUPSORT put() */
  MDBX_txn *me_txn0;          /* preallocated write transaction */

  MDBX_dbx *me_dbxs;    /* array of static DB info */
  uint16_t *me_dbflags; /* array of flags from MDBX_db.md_flags */
  unsigned *me_dbiseqs; /* array of dbi sequence numbers */
  unsigned
      me_maxgc_ov1page;    /* Number of pgno_t fit in a single overflow page */
  uint32_t me_live_reader; /* have liveness lock in reader table */
  void *me_userctx;        /* User-settable context */
  MDBX_hsr_func *me_hsr_callback; /* Callback for kicking laggard readers */

  struct {
    unsigned dp_reserve_limit;
    unsigned rp_augment_limit;
    unsigned dp_limit;
    unsigned dp_initial;
    uint8_t dp_loose_limit;
    uint8_t spill_max_denominator;
    uint8_t spill_min_denominator;
    uint8_t spill_parent4child_denominator;
    unsigned merge_threshold_16dot16_percent;
    union {
      unsigned all;
      /* tracks options with non-auto values but tuned by user */
      struct {
        unsigned dp_limit : 1;
      } non_auto;
    } flags;
  } me_options;

  /* struct me_dbgeo used for accepting db-geo params from user for the new
   * database creation, i.e. when mdbx_env_set_geometry() was called before
   * mdbx_env_open(). */
  struct {
    size_t lower;  /* minimal size of datafile */
    size_t upper;  /* maximal size of datafile */
    size_t now;    /* current size of datafile */
    size_t grow;   /* step to grow datafile */
    size_t shrink; /* threshold to shrink datafile */
  } me_dbgeo;

#if MDBX_LOCKING == MDBX_LOCKING_SYSV
  union {
    key_t key;
    int semid;
  } me_sysv_ipc;
#endif /* MDBX_LOCKING == MDBX_LOCKING_SYSV */

  MDBX_env *me_lcklist_next;

  /* --------------------------------------------------- mostly volatile part */

  MDBX_txn *me_txn; /* current write transaction */
  mdbx_fastmutex_t me_dbi_lock;
  MDBX_dbi me_numdbs; /* number of DBs opened */

  MDBX_page *me_dp_reserve; /* list of malloc'ed blocks for re-use */
  unsigned me_dp_reserve_len;
  /* PNL of pages that became unused in a write txn */
  MDBX_PNL me_retired_pages;

#if defined(_WIN32) || defined(_WIN64)
  MDBX_srwlock me_remap_guard;
  /* Workaround for LockFileEx and WriteFile multithread bug */
  CRITICAL_SECTION me_windowsbug_lock;
#else
  mdbx_fastmutex_t me_remap_guard;
#endif

  /* -------------------------------------------------------------- debugging */

#if MDBX_DEBUG
  MDBX_assert_func *me_assert_func; /*  Callback for assertion failures */
#endif
#ifdef MDBX_USE_VALGRIND
  int me_valgrind_handle;
#endif
#if defined(MDBX_USE_VALGRIND) || defined(__SANITIZE_ADDRESS__)
  pgno_t me_poison_edge;
#endif /* MDBX_USE_VALGRIND || __SANITIZE_ADDRESS__ */

#ifndef xMDBX_DEBUG_SPILLING
#define xMDBX_DEBUG_SPILLING 0
#endif
#if xMDBX_DEBUG_SPILLING == 2
  unsigned debug_dirtied_est, debug_dirtied_act;
#endif /* xMDBX_DEBUG_SPILLING */

  /* ------------------------------------------------- stub for lck-less mode */
  MDBX_atomic_uint64_t
      x_lckless_stub[(sizeof(MDBX_lockinfo) + MDBX_CACHELINE_SIZE - 1) /
                     sizeof(MDBX_atomic_uint64_t)];
};

#ifndef __cplusplus
/*----------------------------------------------------------------------------*/
/* Debug and Logging stuff */

#define MDBX_RUNTIME_FLAGS_INIT                                                \
  ((MDBX_DEBUG) > 0) * MDBX_DBG_ASSERT + ((MDBX_DEBUG) > 1) * MDBX_DBG_AUDIT

extern uint8_t mdbx_runtime_flags;
extern uint8_t mdbx_loglevel;
extern MDBX_debug_func *mdbx_debug_logger;

MDBX_MAYBE_UNUSED static __inline void mdbx_jitter4testing(bool tiny) {
#if MDBX_DEBUG
  if (MDBX_DBG_JITTER & mdbx_runtime_flags)
    mdbx_osal_jitter(tiny);
#else
  (void)tiny;
#endif
}

MDBX_INTERNAL_FUNC void MDBX_PRINTF_ARGS(4, 5)
    mdbx_debug_log(int level, const char *function, int line, const char *fmt,
                   ...) MDBX_PRINTF_ARGS(4, 5);
MDBX_INTERNAL_FUNC void mdbx_debug_log_va(int level, const char *function,
                                          int line, const char *fmt,
                                          va_list args);

#if MDBX_DEBUG
#define mdbx_log_enabled(msg) unlikely(msg <= mdbx_loglevel)
#define mdbx_audit_enabled() unlikely((mdbx_runtime_flags & MDBX_DBG_AUDIT))
#else /* MDBX_DEBUG */
#define mdbx_log_enabled(msg) (msg < MDBX_LOG_VERBOSE && msg <= mdbx_loglevel)
#define mdbx_audit_enabled() (0)
#endif /* MDBX_DEBUG */

#if MDBX_FORCE_ASSERTIONS
#define mdbx_assert_enabled() (1)
#elif MDBX_DEBUG
#define mdbx_assert_enabled() likely((mdbx_runtime_flags & MDBX_DBG_ASSERT))
#else
#define mdbx_assert_enabled() (0)
#endif /* assertions */

#define mdbx_debug_extra(fmt, ...)                                             \
  do {                                                                         \
    if (mdbx_log_enabled(MDBX_LOG_EXTRA))                                      \
      mdbx_debug_log(MDBX_LOG_EXTRA, __func__, __LINE__, fmt, __VA_ARGS__);    \
  } while (0)

#define mdbx_debug_extra_print(fmt, ...)                                       \
  do {                                                                         \
    if (mdbx_log_enabled(MDBX_LOG_EXTRA))                                      \
      mdbx_debug_log(MDBX_LOG_EXTRA, NULL, 0, fmt, __VA_ARGS__);               \
  } while (0)

#define mdbx_trace(fmt, ...)                                                   \
  do {                                                                         \
    if (mdbx_log_enabled(MDBX_LOG_TRACE))                                      \
      mdbx_debug_log(MDBX_LOG_TRACE, __func__, __LINE__, fmt "\n",             \
                     __VA_ARGS__);                                             \
  } while (0)

#define mdbx_debug(fmt, ...)                                                   \
  do {                                                                         \
    if (mdbx_log_enabled(MDBX_LOG_DEBUG))                                      \
      mdbx_debug_log(MDBX_LOG_DEBUG, __func__, __LINE__, fmt "\n",             \
                     __VA_ARGS__);                                             \
  } while (0)

#define mdbx_verbose(fmt, ...)                                                 \
  do {                                                                         \
    if (mdbx_log_enabled(MDBX_LOG_VERBOSE))                                    \
      mdbx_debug_log(MDBX_LOG_VERBOSE, __func__, __LINE__, fmt "\n",           \
                     __VA_ARGS__);                                             \
  } while (0)

#define mdbx_notice(fmt, ...)                                                  \
  do {                                                                         \
    if (mdbx_log_enabled(MDBX_LOG_NOTICE))                                     \
      mdbx_debug_log(MDBX_LOG_NOTICE, __func__, __LINE__, fmt "\n",            \
                     __VA_ARGS__);                                             \
  } while (0)

#define mdbx_warning(fmt, ...)                                                 \
  do {                                                                         \
    if (mdbx_log_enabled(MDBX_LOG_WARN))                                       \
      mdbx_debug_log(MDBX_LOG_WARN, __func__, __LINE__, fmt "\n",              \
                     __VA_ARGS__);                                             \
  } while (0)

#define mdbx_error(fmt, ...)                                                   \
  do {                                                                         \
    if (mdbx_log_enabled(MDBX_LOG_ERROR))                                      \
      mdbx_debug_log(MDBX_LOG_ERROR, __func__, __LINE__, fmt "\n",             \
                     __VA_ARGS__);                                             \
  } while (0)

#define mdbx_fatal(fmt, ...)                                                   \
  mdbx_debug_log(MDBX_LOG_FATAL, __func__, __LINE__, fmt "\n", __VA_ARGS__);

#define mdbx_ensure_msg(env, expr, msg)                                        \
  do {                                                                         \
    if (unlikely(!(expr)))                                                     \
      mdbx_assert_fail(env, msg, __func__, __LINE__);                          \
  } while (0)

#define mdbx_ensure(env, expr) mdbx_ensure_msg(env, expr, #expr)

/* assert(3) variant in environment context */
#define mdbx_assert(env, expr)                                                 \
  do {                                                                         \
    if (mdbx_assert_enabled())                                                 \
      mdbx_ensure(env, expr);                                                  \
  } while (0)

/* assert(3) variant in cursor context */
#define mdbx_cassert(mc, expr) mdbx_assert((mc)->mc_txn->mt_env, expr)

/* assert(3) variant in transaction context */
#define mdbx_tassert(txn, expr) mdbx_assert((txn)->mt_env, expr)

#ifndef xMDBX_TOOLS /* Avoid using internal mdbx_assert() */
#undef assert
#define assert(expr) mdbx_assert(NULL, expr)
#endif

/*----------------------------------------------------------------------------*/
/* Cache coherence and mmap invalidation */

#if MDBX_CPU_WRITEBACK_INCOHERENT
#define mdbx_flush_incoherent_cpu_writeback() mdbx_memory_barrier()
#else
#define mdbx_flush_incoherent_cpu_writeback() mdbx_compiler_barrier()
#endif /* MDBX_CPU_WRITEBACK_INCOHERENT */

MDBX_MAYBE_UNUSED static __inline void
mdbx_flush_incoherent_mmap(void *addr, size_t nbytes, const intptr_t pagesize) {
#if MDBX_MMAP_INCOHERENT_FILE_WRITE
  char *const begin = (char *)(-pagesize & (intptr_t)addr);
  char *const end =
      (char *)(-pagesize & (intptr_t)((char *)addr + nbytes + pagesize - 1));
  int err = msync(begin, end - begin, MS_SYNC | MS_INVALIDATE) ? errno : 0;
  mdbx_assert(nullptr, err == 0);
  (void)err;
#else
  (void)pagesize;
#endif /* MDBX_MMAP_INCOHERENT_FILE_WRITE */

#if MDBX_MMAP_INCOHERENT_CPU_CACHE
#ifdef DCACHE
  /* MIPS has cache coherency issues.
   * Note: for any nbytes >= on-chip cache size, entire is flushed. */
  cacheflush(addr, nbytes, DCACHE);
#else
#error "Oops, cacheflush() not available"
#endif /* DCACHE */
#endif /* MDBX_MMAP_INCOHERENT_CPU_CACHE */

#if !MDBX_MMAP_INCOHERENT_FILE_WRITE && !MDBX_MMAP_INCOHERENT_CPU_CACHE
  (void)addr;
  (void)nbytes;
#endif
}

/*----------------------------------------------------------------------------*/
/* Internal prototypes */

MDBX_INTERNAL_FUNC int mdbx_cleanup_dead_readers(MDBX_env *env, int rlocked,
                                                 int *dead);
MDBX_INTERNAL_FUNC int mdbx_rthc_alloc(mdbx_thread_key_t *key,
                                       MDBX_reader *begin, MDBX_reader *end);
MDBX_INTERNAL_FUNC void mdbx_rthc_remove(const mdbx_thread_key_t key);

MDBX_INTERNAL_FUNC void mdbx_rthc_global_init(void);
MDBX_INTERNAL_FUNC void mdbx_rthc_global_dtor(void);
MDBX_INTERNAL_FUNC void mdbx_rthc_thread_dtor(void *ptr);

#endif /* !__cplusplus */

#define MDBX_IS_ERROR(rc)                                                      \
  ((rc) != MDBX_RESULT_TRUE && (rc) != MDBX_RESULT_FALSE)

/* Internal error codes, not exposed outside libmdbx */
#define MDBX_NO_ROOT (MDBX_LAST_ADDED_ERRCODE + 10)

/* Debugging output value of a cursor DBI: Negative in a sub-cursor. */
#define DDBI(mc)                                                               \
  (((mc)->mc_flags & C_SUB) ? -(int)(mc)->mc_dbi : (int)(mc)->mc_dbi)

/* Key size which fits in a DKBUF (debug key buffer). */
#define DKBUF_MAX 511
#define DKBUF char _kbuf[DKBUF_MAX * 4 + 2]
#define DKEY(x) mdbx_dump_val(x, _kbuf, DKBUF_MAX * 2 + 1)
#define DVAL(x) mdbx_dump_val(x, _kbuf + DKBUF_MAX * 2 + 1, DKBUF_MAX * 2 + 1)

#if MDBX_DEBUG
#define DKBUF_DEBUG DKBUF
#define DKEY_DEBUG(x) DKEY(x)
#define DVAL_DEBUG(x) DVAL(x)
#else
#define DKBUF_DEBUG ((void)(0))
#define DKEY_DEBUG(x) ("-")
#define DVAL_DEBUG(x) ("-")
#endif

/* An invalid page number.
 * Mainly used to denote an empty tree. */
#define P_INVALID (~(pgno_t)0)

/* Test if the flags f are set in a flag word w. */
#define F_ISSET(w, f) (((w) & (f)) == (f))

/* Round n up to an even number. */
#define EVEN(n) (((n) + 1UL) & -2L) /* sign-extending -2 to match n+1U */

/* Default size of memory map.
 * This is certainly too small for any actual applications. Apps should
 * always set the size explicitly using mdbx_env_set_geometry(). */
#define DEFAULT_MAPSIZE MEGABYTE

/* Number of slots in the reader table.
 * This value was chosen somewhat arbitrarily. The 61 is a prime number,
 * and such readers plus a couple mutexes fit into single 4KB page.
 * Applications should set the table size using mdbx_env_set_maxreaders(). */
#define DEFAULT_READERS 61

/* Test if a page is a leaf page */
#define IS_LEAF(p) (((p)->mp_flags & P_LEAF) != 0)
/* Test if a page is a LEAF2 page */
#define IS_LEAF2(p) unlikely(((p)->mp_flags & P_LEAF2) != 0)
/* Test if a page is a branch page */
#define IS_BRANCH(p) (((p)->mp_flags & P_BRANCH) != 0)
/* Test if a page is an overflow page */
#define IS_OVERFLOW(p) unlikely(((p)->mp_flags & P_OVERFLOW) != 0)
/* Test if a page is a sub page */
#define IS_SUBP(p) (((p)->mp_flags & P_SUBP) != 0)

#define PAGETYPE(p) ((p)->mp_flags & (P_BRANCH | P_LEAF | P_LEAF2 | P_OVERFLOW))

/* Header for a single key/data pair within a page.
 * Used in pages of type P_BRANCH and P_LEAF without P_LEAF2.
 * We guarantee 2-byte alignment for 'MDBX_node's.
 *
 * Leaf node flags describe node contents.  F_BIGDATA says the node's
 * data part is the page number of an overflow page with actual data.
 * F_DUPDATA and F_SUBDATA can be combined giving duplicate data in
 * a sub-page/sub-database, and named databases (just F_SUBDATA). */
typedef struct MDBX_node {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
  union {
    uint32_t mn_dsize;
    uint32_t mn_pgno32;
  };
  uint8_t mn_flags; /* see mdbx_node flags */
  uint8_t mn_extra;
  uint16_t mn_ksize; /* key size */
#else
  uint16_t mn_ksize; /* key size */
  uint8_t mn_extra;
  uint8_t mn_flags; /* see mdbx_node flags */
  union {
    uint32_t mn_pgno32;
    uint32_t mn_dsize;
  };
#endif /* __BYTE_ORDER__ */

  /* mdbx_node Flags */
#define F_BIGDATA 0x01 /* data put on overflow page */
#define F_SUBDATA 0x02 /* data is a sub-database */
#define F_DUPDATA 0x04 /* data has duplicates */

  /* valid flags for mdbx_node_add() */
#define NODE_ADD_FLAGS (F_DUPDATA | F_SUBDATA | MDBX_RESERVE | MDBX_APPEND)

#if (defined(__STDC_VERSION__) && __STDC_VERSION__ >= 199901L) ||              \
    (!defined(__cplusplus) && defined(_MSC_VER))
  uint8_t mn_data[] /* key and data are appended here */;
#endif /* C99 */
} MDBX_node;

#define DB_PERSISTENT_FLAGS                                                    \
  (MDBX_REVERSEKEY | MDBX_DUPSORT | MDBX_INTEGERKEY | MDBX_DUPFIXED |          \
   MDBX_INTEGERDUP | MDBX_REVERSEDUP)

/* mdbx_dbi_open() flags */
#define DB_USABLE_FLAGS (DB_PERSISTENT_FLAGS | MDBX_CREATE | MDBX_DB_ACCEDE)

#define DB_VALID 0x8000 /* DB handle is valid, for me_dbflags */
#define DB_INTERNAL_FLAGS DB_VALID

#if DB_INTERNAL_FLAGS & DB_USABLE_FLAGS
#error "Oops, some flags overlapped or wrong"
#endif
#if DB_PERSISTENT_FLAGS & ~DB_USABLE_FLAGS
#error "Oops, some flags overlapped or wrong"
#endif

/* max number of pages to commit in one writev() call */
#define MDBX_COMMIT_PAGES 64
#if defined(IOV_MAX) && IOV_MAX < MDBX_COMMIT_PAGES /* sysconf(_SC_IOV_MAX) */
#undef MDBX_COMMIT_PAGES
#define MDBX_COMMIT_PAGES IOV_MAX
#endif

/*
 *                /
 *                | -1, a < b
 * CMP2INT(a,b) = <  0, a == b
 *                |  1, a > b
 *                \
 */
#ifndef __e2k__
/* LY: fast enough on most systems */
#define CMP2INT(a, b) (((b) > (a)) ? -1 : (a) > (b))
#else
/* LY: more parallelable on VLIW Elbrus */
#define CMP2INT(a, b) (((a) > (b)) - ((b) > (a)))
#endif

/* Do not spill pages to disk if txn is getting full, may fail instead */
#define MDBX_NOSPILL 0x8000

MDBX_MAYBE_UNUSED MDBX_NOTHROW_CONST_FUNCTION static __inline pgno_t
int64pgno(int64_t i64) {
  if (likely(i64 >= (int64_t)MIN_PAGENO && i64 <= (int64_t)MAX_PAGENO + 1))
    return (pgno_t)i64;
  return (i64 < (int64_t)MIN_PAGENO) ? MIN_PAGENO : MAX_PAGENO;
}

MDBX_MAYBE_UNUSED MDBX_NOTHROW_CONST_FUNCTION static __inline pgno_t
pgno_add(size_t base, size_t augend) {
  assert(base <= MAX_PAGENO + 1 && augend < MAX_PAGENO);
  return int64pgno(base + augend);
}

MDBX_MAYBE_UNUSED MDBX_NOTHROW_CONST_FUNCTION static __inline pgno_t
pgno_sub(size_t base, size_t subtrahend) {
  assert(base >= MIN_PAGENO && base <= MAX_PAGENO + 1 &&
         subtrahend < MAX_PAGENO);
  return int64pgno(base - subtrahend);
}

MDBX_MAYBE_UNUSED MDBX_NOTHROW_CONST_FUNCTION static __always_inline bool
is_powerof2(size_t x) {
  return (x & (x - 1)) == 0;
}

MDBX_MAYBE_UNUSED MDBX_NOTHROW_CONST_FUNCTION static __always_inline size_t
floor_powerof2(size_t value, size_t granularity) {
  assert(is_powerof2(granularity));
  return value & ~(granularity - 1);
}

MDBX_MAYBE_UNUSED MDBX_NOTHROW_CONST_FUNCTION static __always_inline size_t
ceil_powerof2(size_t value, size_t granularity) {
  return floor_powerof2(value + granularity - 1, granularity);
}

MDBX_MAYBE_UNUSED MDBX_NOTHROW_CONST_FUNCTION static unsigned
log2n_powerof2(size_t value) {
  assert(value > 0 && value < INT32_MAX && is_powerof2(value));
  assert((value & -(int32_t)value) == value);
#if __GNUC_PREREQ(4, 1) || __has_builtin(__builtin_ctzl)
  return __builtin_ctzl(value);
#elif defined(_MSC_VER)
  unsigned long index;
  _BitScanForward(&index, (unsigned long)value);
  return index;
#else
  static const uint8_t debruijn_ctz32[32] = {
      0,  1,  28, 2,  29, 14, 24, 3, 30, 22, 20, 15, 25, 17, 4,  8,
      31, 27, 13, 23, 21, 19, 16, 7, 26, 12, 18, 6,  11, 5,  10, 9};
  return debruijn_ctz32[(uint32_t)(value * 0x077CB531u) >> 27];
#endif
}

/* Only a subset of the mdbx_env flags can be changed
 * at runtime. Changing other flags requires closing the
 * environment and re-opening it with the new flags. */
#define ENV_CHANGEABLE_FLAGS                                                   \
  (MDBX_SAFE_NOSYNC | MDBX_NOMETASYNC | MDBX_DEPRECATED_MAPASYNC |             \
   MDBX_NOMEMINIT | MDBX_COALESCE | MDBX_PAGEPERTURB | MDBX_ACCEDE)
#define ENV_CHANGELESS_FLAGS                                                   \
  (MDBX_NOSUBDIR | MDBX_RDONLY | MDBX_WRITEMAP | MDBX_NOTLS | MDBX_NORDAHEAD | \
   MDBX_LIFORECLAIM | MDBX_EXCLUSIVE)
#define ENV_USABLE_FLAGS (ENV_CHANGEABLE_FLAGS | ENV_CHANGELESS_FLAGS)

#if !defined(__cplusplus) || CONSTEXPR_ENUM_FLAGS_OPERATIONS
MDBX_MAYBE_UNUSED static void static_checks(void) {
  STATIC_ASSERT_MSG(INT16_MAX - CORE_DBS == MDBX_MAX_DBI,
                    "Oops, MDBX_MAX_DBI or CORE_DBS?");
  STATIC_ASSERT_MSG((unsigned)(MDBX_DB_ACCEDE | MDBX_CREATE) ==
                        ((DB_USABLE_FLAGS | DB_INTERNAL_FLAGS) &
                         (ENV_USABLE_FLAGS | ENV_INTERNAL_FLAGS)),
                    "Oops, some flags overlapped or wrong");
  STATIC_ASSERT_MSG((ENV_INTERNAL_FLAGS & ENV_USABLE_FLAGS) == 0,
                    "Oops, some flags overlapped or wrong");
}
#endif /* Disabled for MSVC 19.0 (VisualStudio 2015) */

#ifdef __cplusplus
}
#endif

#define MDBX_ASAN_POISON_MEMORY_REGION(addr, size)                             \
  do {                                                                         \
    mdbx_trace("POISON_MEMORY_REGION(%p, %zu) at %u", (void *)(addr),          \
               (size_t)(size), __LINE__);                                      \
    ASAN_POISON_MEMORY_REGION(addr, size);                                     \
  } while (0)

#define MDBX_ASAN_UNPOISON_MEMORY_REGION(addr, size)                           \
  do {                                                                         \
    mdbx_trace("UNPOISON_MEMORY_REGION(%p, %zu) at %u", (void *)(addr),        \
               (size_t)(size), __LINE__);                                      \
    ASAN_UNPOISON_MEMORY_REGION(addr, size);                                   \
  } while (0)
