/// \copyright SPDX-License-Identifier: Apache-2.0
/// \note Please refer to the COPYRIGHT file for explanations license change,
/// credits and acknowledgments.
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025

#pragma once

#include "essentials.h"

#pragma pack(push, 4)

/* A stamp that identifies a file as an MDBX file.
 * There's nothing special about this value other than that it is easily
 * recognizable, and it will reflect any byte order mismatches. */
#define MDBX_MAGIC UINT64_C(/* 56-bit prime */ 0x59659DBDEF4C11)

/* FROZEN: The version number for a database's datafile format. */
#define MDBX_DATA_VERSION 3

#define MDBX_DATA_MAGIC ((MDBX_MAGIC << 8) + MDBX_PNL_ASCENDING * 64 + MDBX_DATA_VERSION)
#define MDBX_DATA_MAGIC_LEGACY_COMPAT ((MDBX_MAGIC << 8) + MDBX_PNL_ASCENDING * 64 + 2)
#define MDBX_DATA_MAGIC_LEGACY_DEVEL ((MDBX_MAGIC << 8) + 255)

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
typedef mdbx_atomic_uint32_t atomic_pgno_t;
#define PRIaPGNO PRIu32
#define MAX_PAGENO UINT32_C(0x7FFFffff)
#define MIN_PAGENO NUM_METAS

/* An invalid page number.
 * Mainly used to denote an empty tree. */
#define P_INVALID (~(pgno_t)0)

/* A transaction ID. */
typedef uint64_t txnid_t;
typedef mdbx_atomic_uint64_t atomic_txnid_t;
#define PRIaTXN PRIi64
#define MIN_TXNID UINT64_C(1)
#define MAX_TXNID (SAFE64_INVALID_THRESHOLD - 1)
#define INITIAL_TXNID (MIN_TXNID + NUM_METAS - 1)
#define INVALID_TXNID UINT64_MAX

/* Used for offsets within a single page. */
typedef uint16_t indx_t;

typedef struct tree {
  uint16_t flags;       /* see mdbx_dbi_open */
  uint16_t height;      /* height of this tree */
  uint32_t dupfix_size; /* key-size for MDBX_DUPFIXED (DUPFIX pages) */
  pgno_t root;          /* the root page of this tree */
  pgno_t branch_pages;  /* number of branch pages */
  pgno_t leaf_pages;    /* number of leaf pages */
  pgno_t large_pages;   /* number of large pages */
  uint64_t sequence;    /* table sequence counter */
  uint64_t items;       /* number of data items */
  uint64_t mod_txnid;   /* txnid of last committed modification */
} tree_t;

/* database size-related parameters */
typedef struct geo {
  uint16_t grow_pv;   /* datafile growth step as a 16-bit packed (exponential
                           quantized) value */
  uint16_t shrink_pv; /* datafile shrink threshold as a 16-bit packed
                           (exponential quantized) value */
  pgno_t lower;       /* minimal size of datafile in pages */
  pgno_t upper;       /* maximal size of datafile in pages */
  union {
    pgno_t now; /* current size of datafile in pages */
    pgno_t end_pgno;
  };
  union {
    pgno_t first_unallocated; /* first unused page in the datafile,
                         but actually the file may be shorter. */
    pgno_t next_pgno;
  };
} geo_t;

/* Meta page content.
 * A meta page is the start point for accessing a database snapshot.
 * Pages 0-2 are meta pages. */
typedef struct meta {
  /* Stamp identifying this as an MDBX file.
   * It must be set to MDBX_MAGIC with MDBX_DATA_VERSION. */
  uint32_t magic_and_version[2];

  /* txnid that committed this meta, the first of a two-phase-update pair */
  union {
    mdbx_atomic_uint32_t txnid_a[2];
    uint64_t unsafe_txnid;
  };

  uint16_t reserve16;   /* extra flags, zero (nothing) for now */
  uint8_t validator_id; /* ID of checksum and page validation method,
                         * zero (nothing) for now */
  int8_t extra_pagehdr; /* extra bytes in the page header,
                         * zero (nothing) for now */

  geo_t geometry; /* database size-related parameters */

  union {
    struct {
      tree_t gc, main;
    } trees;
    __anonymous_struct_extension__ struct {
      uint16_t gc_flags;
      uint16_t gc_height;
      uint32_t pagesize;
    };
  };

  MDBX_canary canary;

#define DATASIGN_NONE 0u
#define DATASIGN_WEAK 1u
#define SIGN_IS_STEADY(sign) ((sign) > DATASIGN_WEAK)
  union {
    uint32_t sign[2];
    uint64_t unsafe_sign;
  };

  /* txnid that committed this meta, the second of a two-phase-update pair */
  mdbx_atomic_uint32_t txnid_b[2];

  /* Number of non-meta pages which were put in GC after COW. May be 0 in case
   * DB was previously handled by libmdbx without corresponding feature.
   * This value in couple with reader.snapshot_pages_retired allows fast
   * estimation of "how much reader is restraining GC recycling". */
  uint32_t pages_retired[2];

  /* The analogue /proc/sys/kernel/random/boot_id or similar to determine
   * whether the system was rebooted after the last use of the database files.
   * If there was no reboot, but there is no need to rollback to the last
   * steady sync point. Zeros mean that no relevant information is available
   * from the system. */
  bin128_t bootid;

  /* GUID базы данных, начиная с v0.13.1 */
  bin128_t dxbid;
} meta_t;

#pragma pack(1)

typedef enum page_type {
  P_BRANCH = 0x01u /* branch page */,
  P_LEAF = 0x02u /* leaf page */,
  P_LARGE = 0x04u /* large/overflow page */,
  P_META = 0x08u /* meta page */,
  P_LEGACY_DIRTY = 0x10u /* legacy P_DIRTY flag prior to v0.10 958fd5b9 */,
  P_BAD = P_LEGACY_DIRTY /* explicit flag for invalid/bad page */,
  P_DUPFIX = 0x20u /* for MDBX_DUPFIXED records */,
  P_SUBP = 0x40u /* for MDBX_DUPSORT sub-pages */,
  P_SPILLED = 0x2000u /* spilled in parent txn */,
  P_LOOSE = 0x4000u /* page was dirtied then freed, can be reused */,
  P_FROZEN = 0x8000u /* used for retire page with known status */,
  P_ILL_BITS = (uint16_t)~(P_BRANCH | P_LEAF | P_DUPFIX | P_LARGE | P_SPILLED),

  page_broken = 0,
  page_large = P_LARGE,
  page_branch = P_BRANCH,
  page_leaf = P_LEAF,
  page_dupfix_leaf = P_DUPFIX,
  page_sub_leaf = P_SUBP | P_LEAF,
  page_sub_dupfix_leaf = P_SUBP | P_DUPFIX,
  page_sub_broken = P_SUBP,
} page_type_t;

/* Common header for all page types. The page type depends on flags.
 *
 * P_BRANCH and P_LEAF pages have unsorted 'node_t's at the end, with
 * sorted entries[] entries referring to them. Exception: P_DUPFIX pages
 * omit entries and pack sorted MDBX_DUPFIXED values after the page header.
 *
 * P_LARGE records occupy one or more contiguous pages where only the
 * first has a page header. They hold the real data of N_BIG nodes.
 *
 * P_SUBP sub-pages are small leaf "pages" with duplicate data.
 * A node with flag N_DUP but not N_TREE contains a sub-page.
 * (Duplicate data can also go in tables, which use normal pages.)
 *
 * P_META pages contain meta_t, the start point of an MDBX snapshot.
 *
 * Each non-metapage up to meta_t.mm_last_pg is reachable exactly once
 * in the snapshot: Either used by a database or listed in a GC record. */
typedef struct page {
  uint64_t txnid;        /* txnid which created page, maybe zero in legacy DB */
  uint16_t dupfix_ksize; /* key size if this is a DUPFIX page */
  uint16_t flags;
  union {
    uint32_t pages; /* number of overflow pages */
    __anonymous_struct_extension__ struct {
      indx_t lower; /* lower bound of free space */
      indx_t upper; /* upper bound of free space */
    };
  };
  pgno_t pgno; /* page number */

#if FLEXIBLE_ARRAY_MEMBERS
  indx_t entries[] /* dynamic size */;
#endif /* FLEXIBLE_ARRAY_MEMBERS */
} page_t;

/* Size of the page header, excluding dynamic data at the end */
#define PAGEHDRSZ 20u

/* Header for a single key/data pair within a page.
 * Used in pages of type P_BRANCH and P_LEAF without P_DUPFIX.
 * We guarantee 2-byte alignment for 'node_t's.
 *
 * Leaf node flags describe node contents.  N_BIG says the node's
 * data part is the page number of an overflow page with actual data.
 * N_DUP and N_TREE can be combined giving duplicate data in
 * a sub-page/table, and named databases (just N_TREE). */
typedef struct node {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
  union {
    uint32_t dsize;
    uint32_t child_pgno;
  };
  uint8_t flags; /* see node_flags */
  uint8_t extra;
  uint16_t ksize; /* key size */
#else
  uint16_t ksize; /* key size */
  uint8_t extra;
  uint8_t flags; /* see node_flags */
  union {
    uint32_t child_pgno;
    uint32_t dsize;
  };
#endif /* __BYTE_ORDER__ */

#if FLEXIBLE_ARRAY_MEMBERS
  uint8_t payload[] /* key and data are appended here */;
#endif /* FLEXIBLE_ARRAY_MEMBERS */
} node_t;

/* Size of the node header, excluding dynamic data at the end */
#define NODESIZE 8u

typedef enum node_flags {
  N_BIG = 0x01 /* data put on large page */,
  N_TREE = 0x02 /* data is a b-tree */,
  N_DUP = 0x04 /* data has duplicates */
} node_flags_t;

#pragma pack(pop)

MDBX_MAYBE_UNUSED MDBX_NOTHROW_PURE_FUNCTION static inline uint8_t page_type(const page_t *mp) { return mp->flags; }

MDBX_MAYBE_UNUSED MDBX_NOTHROW_PURE_FUNCTION static inline uint8_t page_type_compat(const page_t *mp) {
  /* Drop legacy P_DIRTY flag for sub-pages for compatilibity,
   * for assertions only. */
  return unlikely(mp->flags & P_SUBP) ? mp->flags & ~(P_SUBP | P_LEGACY_DIRTY) : mp->flags;
}

MDBX_MAYBE_UNUSED MDBX_NOTHROW_PURE_FUNCTION static inline bool is_leaf(const page_t *mp) {
  return (mp->flags & P_LEAF) != 0;
}

MDBX_MAYBE_UNUSED MDBX_NOTHROW_PURE_FUNCTION static inline bool is_dupfix_leaf(const page_t *mp) {
  return (mp->flags & P_DUPFIX) != 0;
}

MDBX_MAYBE_UNUSED MDBX_NOTHROW_PURE_FUNCTION static inline bool is_branch(const page_t *mp) {
  return (mp->flags & P_BRANCH) != 0;
}

MDBX_MAYBE_UNUSED MDBX_NOTHROW_PURE_FUNCTION static inline bool is_largepage(const page_t *mp) {
  return (mp->flags & P_LARGE) != 0;
}

MDBX_MAYBE_UNUSED MDBX_NOTHROW_PURE_FUNCTION static inline bool is_subpage(const page_t *mp) {
  return (mp->flags & P_SUBP) != 0;
}
